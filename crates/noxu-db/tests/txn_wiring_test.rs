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

// ─── Sprint 6 / Property 4 — Transaction commit/abort visibility oracle ─────
//
// Property: drive a randomised sequence of transaction operations
// (begin / put / commit / abort / auto-commit put) against a real
// `noxu_db::Database` and an oracle that tracks "what would be
// committed".  After every commit the oracle is updated; after every
// abort the oracle reverts to the snapshot at txn-begin.  After every
// commit/abort, a fresh `db.get` for any seen key must return what the
// oracle says.
//
// Catches isolation/visibility bugs and the recently-fixed F1 (txn
// cleanup), F12 (auto-commit isolation), and durability-mismatch bugs.
// This is the multi-step model-test pattern recommended in the hegel
// skill (Tier-1 "Model tests").

mod prop_txn_visibility {
    use super::*;
    use proptest::prelude::*;
    use std::collections::BTreeMap;

    #[derive(Debug, Clone)]
    enum TxnStep {
        // No-op if a txn is already active; otherwise begin one.
        Begin,
        // Put under the active txn if any, else auto-commit put.
        Put { key: Vec<u8>, value: Vec<u8> },
        // Commit the active txn if any (no-op otherwise).
        Commit,
        // Abort the active txn if any (no-op otherwise).
        Abort,
    }

    fn step_strategy() -> impl Strategy<Value = TxnStep> {
        let key_strat = prop::collection::vec(any::<u8>(), 1..=4);
        let val_strat = prop::collection::vec(any::<u8>(), 1..=16);
        prop_oneof![
            // Skew the distribution slightly toward Put so short
            // sequences still build up state to verify.
            1 => Just(TxnStep::Begin),
            4 => (key_strat, val_strat)
                .prop_map(|(key, value)| TxnStep::Put { key, value }),
            2 => Just(TxnStep::Commit),
            1 => Just(TxnStep::Abort),
        ]
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 64,
            .. ProptestConfig::default()
        })]

        #[test]
        fn txn_commit_abort_visibility_oracle(
            steps in prop::collection::vec(step_strategy(), 1..=40),
        ) {
            let tmp = TempDir::new().unwrap();
            let env = open_env(&tmp, Durability::COMMIT_NO_SYNC);
            let db = open_db(&env, "prop_txn_visibility");

            // Oracle: the *committed* state of the database.
            let mut committed: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
            // The active transaction together with the snapshot of
            // `committed` taken at begin, plus the txn-local mutations
            // applied since begin (which collapse onto `committed` on
            // commit and are dropped on abort).
            type ActiveTxn = (noxu_db::Transaction, BTreeMap<Vec<u8>, Vec<u8>>);
            let mut active: Option<ActiveTxn> = None;
            // All keys ever touched, for the post-step visibility sweep.
            let mut all_keys: std::collections::BTreeSet<Vec<u8>> =
                std::collections::BTreeSet::new();

            let verify_committed_visibility = |db: &Database,
                                               committed: &BTreeMap<Vec<u8>, Vec<u8>>,
                                               all_keys: &std::collections::BTreeSet<Vec<u8>>,
                                               step_idx: usize|
             -> Result<(), TestCaseError> {
                for k in all_keys {
                    let mut data = DatabaseEntry::new();
                    let key_e = DatabaseEntry::from_data(k);
                    let status = db.get(None, &key_e, &mut data).unwrap();
                    match (status, committed.get(k)) {
                        (OperationStatus::Success, Some(want)) => {
                            prop_assert_eq!(
                                data.data(), want.as_slice(),
                                "step {}: get({:?}) value mismatch", step_idx, k,
                            );
                        }
                        (OperationStatus::NotFound, None) => { /* agree */ }
                        (s, w) => prop_assert!(
                            false,
                            "step {}: get({:?}) visibility mismatch: db={:?}, oracle={:?}",
                            step_idx, k, s, w,
                        ),
                    }
                }
                Ok(())
            };

            for (i, step) in steps.into_iter().enumerate() {
                match step {
                    TxnStep::Begin => {
                        if active.is_none() {
                            let txn = env.begin_transaction(None, None).unwrap();
                            // Snapshot the committed state; mutations
                            // accumulate here until commit/abort.
                            let snap = committed.clone();
                            active = Some((txn, snap));
                        }
                    }
                    TxnStep::Put { key, value } => {
                        all_keys.insert(key.clone());
                        let key_e = DatabaseEntry::from_data(&key);
                        let val_e = DatabaseEntry::from_data(&value);
                        if let Some((txn, snap)) = active.as_mut() {
                            db.put(Some(txn), &key_e, &val_e).unwrap();
                            snap.insert(key, value);
                        } else {
                            db.put(None, &key_e, &val_e).unwrap();
                            committed.insert(key, value);
                            // Auto-commit: oracle and db should agree
                            // *immediately* after this op.
                            verify_committed_visibility(
                                &db, &committed, &all_keys, i,
                            )?;
                        }
                    }
                    TxnStep::Commit => {
                        if let Some((txn, snap)) = active.take() {
                            txn.commit().unwrap();
                            committed = snap;
                            verify_committed_visibility(
                                &db, &committed, &all_keys, i,
                            )?;
                        }
                    }
                    TxnStep::Abort => {
                        if let Some((txn, _snap)) = active.take() {
                            txn.abort().unwrap();
                            // committed unchanged
                            verify_committed_visibility(
                                &db, &committed, &all_keys, i,
                            )?;
                        }
                    }
                }
            }

            // Drain any still-active txn before final check / close.
            if let Some((txn, _)) = active.take() {
                txn.abort().unwrap();
            }

            // Final visibility sweep.
            verify_committed_visibility(
                &db, &committed, &all_keys, usize::MAX,
            )?;

            // Exercise the F1 fix: env.close() must succeed once all
            // transactions are committed/aborted.
            db.close().unwrap();
            env.close().expect("env.close() must succeed after all txns settled");
        }
    }
}

// ─── F11 / Decision 3B: nested txn rejected with typed Unsupported ────

/// `Environment::begin_transaction(Some(parent), …)` previously dropped the
/// parent on the floor (the parameter was `_parent`).  Decision 3B in
/// `docs/src/internal/v1.5-decisions-2026-05.md` makes that case a typed
/// error so users see a loud, documented failure instead of the silent
/// BDB-JE-shaped behaviour the published mdBook implied.
///
/// The parameter is retained for v1.5 / v1.6 SemVer stability and is
/// scheduled for removal in v2.0.
#[test]
fn f11_nested_transaction_returns_unsupported() {
    use noxu_db::NoxuError;

    let tmp = TempDir::new().unwrap();
    let env = open_env(&tmp, Durability::COMMIT_NO_SYNC);

    let parent = env.begin_transaction(None, None).unwrap();

    let result = env.begin_transaction(Some(&parent), None);
    match result {
        Err(NoxuError::Unsupported(msg)) => {
            assert!(
                msg.contains("nested transactions"),
                "error message should mention nested transactions: {msg}"
            );
            assert!(
                msg.contains("v1.5") && msg.contains("v2.0"),
                "error message should reference v1.5 and v2.0: {msg}"
            );
            assert!(
                msg.contains("None"),
                "error message should tell the caller to pass None: {msg}"
            );
        }
        Ok(_) => {
            panic!("expected NoxuError::Unsupported for nested txn, got Ok")
        }
        Err(other) => panic!(
            "expected NoxuError::Unsupported for nested txn, got: {other:?}"
        ),
    }

    // Parent must still be valid (rejection must not leak any state).
    parent.commit().expect("parent commit must succeed");
    env.close().expect("env.close() must succeed");
}

/// `parent = None` continues to work exactly as before — the rejection is
/// surgical and does not regress the documented happy path.
#[test]
fn f11_nested_transaction_none_still_works() {
    let tmp = TempDir::new().unwrap();
    let env = open_env(&tmp, Durability::COMMIT_NO_SYNC);

    let txn = env.begin_transaction(None, None).unwrap();
    txn.commit().unwrap();

    let cfg = TransactionConfig::new();
    let txn2 = env.begin_transaction(None, Some(&cfg)).unwrap();
    txn2.commit().unwrap();

    env.close().expect("env.close() must succeed");
}
