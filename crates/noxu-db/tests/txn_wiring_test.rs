//! Regression tests for the Sprint 1 environment/transaction wiring
//! fixes (May 2026 API audit findings F1, F2, F3, F12).
//!
//! Each test in this file is a *behavioural* assertion, not a unit test:
//! it opens a real `Environment`, drives the public surface as a user
//! would, and asserts the documented contract.  Pre-fix, every test in
//! this file would fail.

use noxu_db::{
    Database, DatabaseConfig, DatabaseEntry, Durability, Environment,
    EnvironmentConfig, OperationStatus, TransactionConfig,
};
use std::sync::Arc;
use tempfile::TempDir;

fn open_env(temp_dir: &TempDir, durability: Durability) -> Environment {
    let cfg = EnvironmentConfig::new(temp_dir.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(true)
        .with_durability(durability);
    Environment::open(cfg).unwrap()
}

fn open_db(env: &Environment, name: &str) -> Database {
    env.open_database(
        None,
        name,
        &DatabaseConfig::new().with_allow_create(true),
    )
    .unwrap()
}

// ─── F1: env.close() succeeds after txn.commit() ─────────────────────

#[test]
fn f1_env_close_after_commit_succeeds() {
    let tmp = TempDir::new().unwrap();
    let env = open_env(&tmp, Durability::COMMIT_NO_SYNC);

    let txn = env.begin_transaction(None, None).unwrap();
    txn.commit().expect("commit must succeed");

    // Pre-fix: this returns OperationNotAllowed("Cannot close
    // environment with 1 active transactions").
    env.close().expect("env.close() must succeed after commit");
}

#[test]
fn f1_env_close_after_abort_succeeds() {
    let tmp = TempDir::new().unwrap();
    let env = open_env(&tmp, Durability::COMMIT_NO_SYNC);

    let txn = env.begin_transaction(None, None).unwrap();
    txn.abort().expect("abort must succeed");

    env.close().expect("env.close() must succeed after abort");
}

#[test]
fn f1_env_close_after_many_commits_succeeds() {
    let tmp = TempDir::new().unwrap();
    let env = open_env(&tmp, Durability::COMMIT_NO_SYNC);
    let db = open_db(&env, "f1");

    for i in 0..16 {
        let txn = env.begin_transaction(None, None).unwrap();
        let key = DatabaseEntry::from_data(format!("k{}", i).as_bytes());
        let val = DatabaseEntry::from_data(b"v");
        db.put(Some(&txn), &key, &val).unwrap();
        txn.commit().unwrap();
    }

    db.close().unwrap();
    env.close().expect("env.close() must succeed after many commits");
}

#[test]
fn f1_env_close_with_one_active_txn_still_fails() {
    // Sanity check: only commit/abort prune the registry; an open
    // transaction must still block close().
    let tmp = TempDir::new().unwrap();
    let env = open_env(&tmp, Durability::COMMIT_NO_SYNC);
    let _txn = env.begin_transaction(None, None).unwrap();

    let result = env.close();
    assert!(result.is_err(), "close() must fail with active txn");
}

// ─── F2: read_uncommitted on TransactionConfig is honoured ───────────

#[test]
fn f2_read_uncommitted_sees_uncommitted_writes() {
    // A txn with `with_read_uncommitted(true)` must observe writes from
    // a concurrent uncommitted transaction (dirty read).  Pre-fix the
    // flag was silently dropped by `Environment::begin_transaction`, so
    // the reader took a normal read lock and either blocked on the
    // writer or timed out.
    let tmp = TempDir::new().unwrap();
    let env = Arc::new(open_env(&tmp, Durability::COMMIT_NO_SYNC));
    let db = Arc::new(open_db(&env, "f2"));

    // Seed the key so there is a "before" value to read.
    let key = DatabaseEntry::from_data(b"k");
    let val_before = DatabaseEntry::from_data(b"before");
    db.put(None, &key, &val_before).unwrap();

    // Writer txn: writes a new value but does NOT commit yet.
    let writer_txn = env.begin_transaction(None, None).unwrap();
    let val_after = DatabaseEntry::from_data(b"after");
    db.put(Some(&writer_txn), &key, &val_after).unwrap();

    // Reader txn: read-uncommitted, should see the dirty write.
    let read_cfg = TransactionConfig::new().with_read_uncommitted(true);
    let reader_txn = env.begin_transaction(None, Some(&read_cfg)).unwrap();

    let mut data = DatabaseEntry::new();
    let key_lookup = DatabaseEntry::from_data(b"k");
    let status = db
        .get(Some(&reader_txn), &key_lookup, &mut data)
        .expect("dirty read must not block / error");
    assert_eq!(status, OperationStatus::Success);
    assert_eq!(
        data.get_data(),
        Some(b"after".as_slice()),
        "read-uncommitted txn must see the writer's dirty value"
    );

    reader_txn.commit().unwrap();
    writer_txn.abort().unwrap();
    drop(db);
    Arc::try_unwrap(env).ok().unwrap().close().unwrap();
}

// ─── F3: env-level durability default is honoured on commit ──────────

#[test]
fn f3_env_default_durability_no_sync_skips_fsync() {
    // Open with COMMIT_NO_SYNC; commit a txn with `begin_transaction(None, None)`;
    // assert the WAL fsync count did not increase.  Pre-fix, every commit
    // fsynced because TransactionConfig::default().durability ==
    // COMMIT_SYNC and the env-level durability was never consulted.
    let tmp = TempDir::new().unwrap();
    let env = open_env(&tmp, Durability::COMMIT_NO_SYNC);
    let db = open_db(&env, "f3");

    // Drive at least one auto-commit write so the log is initialised
    // (this fsyncs based on db-level no_sync; we only care about the
    // delta around the explicit-txn commit below).
    {
        let key = DatabaseEntry::from_data(b"warm");
        let val = DatabaseEntry::from_data(b"up");
        db.put(None, &key, &val).unwrap();
    }

    let fsyncs_before = env.stat_fsync_count();

    let txn = env.begin_transaction(None, None).unwrap();
    let key = DatabaseEntry::from_data(b"k");
    let val = DatabaseEntry::from_data(b"v");
    db.put(Some(&txn), &key, &val).unwrap();
    txn.commit().expect("commit must succeed");

    let fsyncs_after = env.stat_fsync_count();
    assert_eq!(
        fsyncs_before,
        fsyncs_after,
        "env-level COMMIT_NO_SYNC must not fsync on commit (delta = {})",
        fsyncs_after - fsyncs_before
    );

    db.close().unwrap();
    env.close().unwrap();
}

#[test]
fn f3_env_default_durability_sync_does_fsync() {
    // Sanity: a COMMIT_SYNC env should fsync on commit.
    let tmp = TempDir::new().unwrap();
    let env = open_env(&tmp, Durability::COMMIT_SYNC);
    let db = open_db(&env, "f3");

    // Warm up the log.
    let warm_key = DatabaseEntry::from_data(b"warm");
    let warm_val = DatabaseEntry::from_data(b"up");
    db.put(None, &warm_key, &warm_val).unwrap();

    let fsyncs_before = env.stat_fsync_count();

    let txn = env.begin_transaction(None, None).unwrap();
    let key = DatabaseEntry::from_data(b"k");
    let val = DatabaseEntry::from_data(b"v");
    db.put(Some(&txn), &key, &val).unwrap();
    txn.commit().unwrap();

    let fsyncs_after = env.stat_fsync_count();
    assert!(
        fsyncs_after > fsyncs_before,
        "COMMIT_SYNC must fsync on commit ({} -> {})",
        fsyncs_before,
        fsyncs_after
    );

    db.close().unwrap();
    env.close().unwrap();
}

#[test]
fn f3_explicit_txn_durability_overrides_env_default() {
    // When the caller supplies a TransactionConfig with an explicit
    // durability, that wins over the env default.
    let tmp = TempDir::new().unwrap();
    let env = open_env(&tmp, Durability::COMMIT_NO_SYNC);
    let db = open_db(&env, "f3");

    // Warm up.
    let warm_key = DatabaseEntry::from_data(b"warm");
    let warm_val = DatabaseEntry::from_data(b"up");
    db.put(None, &warm_key, &warm_val).unwrap();

    let fsyncs_before = env.stat_fsync_count();

    let cfg = TransactionConfig::new().with_durability(Durability::COMMIT_SYNC);
    let txn = env.begin_transaction(None, Some(&cfg)).unwrap();
    let key = DatabaseEntry::from_data(b"k");
    let val = DatabaseEntry::from_data(b"v");
    db.put(Some(&txn), &key, &val).unwrap();
    txn.commit().unwrap();

    let fsyncs_after = env.stat_fsync_count();
    assert!(
        fsyncs_after > fsyncs_before,
        "explicit COMMIT_SYNC must fsync even when env default is COMMIT_NO_SYNC"
    );

    db.close().unwrap();
    env.close().unwrap();
}

// ─── F12: auto-commit writes coordinate with explicit-txn locks ──────

use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

#[test]
fn f12_auto_commit_write_blocks_on_explicit_txn_write_lock() {
    // Begin txn A, A.put(K, V) (write lock held).
    // From a second thread, db.put(None, K, V2) (auto-commit write).
    // The auto-commit write must block until A commits/aborts.
    let tmp = TempDir::new().unwrap();
    let env = Arc::new(open_env(&tmp, Durability::COMMIT_NO_SYNC));
    let db = Arc::new(open_db(&env, "f12"));

    // Seed the key so it has a non-NULL old_lsn — the auto-commit
    // cursor's `lock_write_before_log` takes the write lock against the
    // existing record's LSN, which is what the explicit txn's put just
    // pinned.  (A brand-new insert with no prior version would not
    // coordinate; that is a separate corner case noted as deferred F12
    // follow-up work.)
    let key = DatabaseEntry::from_data(b"k");
    let val0 = DatabaseEntry::from_data(b"v0");
    db.put(None, &key, &val0).unwrap();

    // Writer txn: take the write lock by issuing a put.
    let writer_txn = env.begin_transaction(None, None).unwrap();
    let val1 = DatabaseEntry::from_data(b"v1");
    db.put(Some(&writer_txn), &key, &val1).unwrap();

    // Auto-commit thread tries to write the same key.
    let started = Arc::new(AtomicBool::new(false));
    let finished = Arc::new(AtomicBool::new(false));
    let started_t = Arc::clone(&started);
    let finished_t = Arc::clone(&finished);
    let db_t = Arc::clone(&db);
    let handle = thread::spawn(move || {
        started_t.store(true, Ordering::SeqCst);
        let key = DatabaseEntry::from_data(b"k");
        let val2 = DatabaseEntry::from_data(b"v2");
        db_t.put(None, &key, &val2).unwrap();
        finished_t.store(true, Ordering::SeqCst);
    });

    // Wait for the writer thread to start and try to acquire the lock.
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while !started.load(Ordering::SeqCst)
        && std::time::Instant::now() < deadline
    {
        thread::sleep(Duration::from_millis(5));
    }
    // Give the put() time to actually attempt the lock and block.
    thread::sleep(Duration::from_millis(200));

    assert!(
        !finished.load(Ordering::SeqCst),
        "auto-commit write must block while explicit txn holds the write lock"
    );

    // Commit the writer; the auto-commit write should now proceed.
    writer_txn.commit().unwrap();
    handle.join().expect("auto-commit thread must finish without panic");
    assert!(finished.load(Ordering::SeqCst));

    // Final value is whatever the auto-commit thread wrote.
    let mut data = DatabaseEntry::new();
    let key_lookup = DatabaseEntry::from_data(b"k");
    let status = db.get(None, &key_lookup, &mut data).unwrap();
    assert_eq!(status, OperationStatus::Success);
    assert_eq!(data.get_data(), Some(b"v2".as_slice()));

    drop(db);
    Arc::try_unwrap(env).ok().unwrap().close().unwrap();
}

#[test]
fn f12_auto_commit_does_not_block_on_unrelated_key() {
    // Sanity: auto-commit on a different key must NOT block on an
    // explicit txn's write lock for a different key.
    let tmp = TempDir::new().unwrap();
    let env = Arc::new(open_env(&tmp, Durability::COMMIT_NO_SYNC));
    let db = Arc::new(open_db(&env, "f12b"));

    let k1 = DatabaseEntry::from_data(b"k1");
    let v0 = DatabaseEntry::from_data(b"v0");
    db.put(None, &k1, &v0).unwrap();

    let writer_txn = env.begin_transaction(None, None).unwrap();
    let v1 = DatabaseEntry::from_data(b"v1");
    db.put(Some(&writer_txn), &k1, &v1).unwrap();

    // Different key — must not block.
    let k2 = DatabaseEntry::from_data(b"k2");
    let v2 = DatabaseEntry::from_data(b"v2");
    db.put(None, &k2, &v2)
        .expect("auto-commit on unrelated key must not block");

    writer_txn.commit().unwrap();

    drop(db);
    Arc::try_unwrap(env).ok().unwrap().close().unwrap();
}

#[test]
fn f12_explicit_txn_read_blocks_auto_commit_write() {
    // Belt-and-braces variant of the F12 scenario: an explicit txn
    // takes a Read lock on K, and a concurrent auto-commit write to
    // K must block until the explicit txn releases its read lock.
    let tmp = TempDir::new().unwrap();
    let env = Arc::new(open_env(&tmp, Durability::COMMIT_NO_SYNC));
    let db = Arc::new(open_db(&env, "f12c"));

    // Seed K so a read can land on a real (non-NULL) LSN.
    let key = DatabaseEntry::from_data(b"k");
    let val0 = DatabaseEntry::from_data(b"v0");
    db.put(None, &key, &val0).unwrap();

    // Explicit txn under serializable isolation: read locks are held
    // until commit/abort.
    let cfg = TransactionConfig::new().with_serializable_isolation(true);
    let reader_txn = env.begin_transaction(None, Some(&cfg)).unwrap();
    let mut data = DatabaseEntry::new();
    let key_lookup = DatabaseEntry::from_data(b"k");
    let status = db.get(Some(&reader_txn), &key_lookup, &mut data).unwrap();
    assert_eq!(status, OperationStatus::Success);
    assert_eq!(data.get_data(), Some(b"v0".as_slice()));

    let started = Arc::new(AtomicBool::new(false));
    let finished = Arc::new(AtomicBool::new(false));
    let started_t = Arc::clone(&started);
    let finished_t = Arc::clone(&finished);
    let db_t = Arc::clone(&db);
    let handle = thread::spawn(move || {
        started_t.store(true, Ordering::SeqCst);
        let key = DatabaseEntry::from_data(b"k");
        let val1 = DatabaseEntry::from_data(b"v1");
        db_t.put(None, &key, &val1).unwrap();
        finished_t.store(true, Ordering::SeqCst);
    });

    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while !started.load(Ordering::SeqCst)
        && std::time::Instant::now() < deadline
    {
        thread::sleep(Duration::from_millis(5));
    }
    thread::sleep(Duration::from_millis(200));

    assert!(
        !finished.load(Ordering::SeqCst),
        "auto-commit write must block while explicit txn holds the read lock"
    );

    reader_txn.commit().unwrap();
    handle.join().unwrap();
    assert!(finished.load(Ordering::SeqCst));

    drop(db);
    Arc::try_unwrap(env).ok().unwrap().close().unwrap();
}
