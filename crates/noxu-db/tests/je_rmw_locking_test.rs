//! C7 — RMW locking core invariant.
//!
//! Faithful to the core guarantee exercised by JE `RMWLockingTest` and the JE
//! `LockMode.RMW` contract: a read performed with `LockMode.RMW` acquires a
//! WRITE lock (not a read lock), so a concurrent writer to the same key is
//! blocked until the RMW reader's transaction commits or aborts.
//!
//! JE `Cursor.get(..., LockMode.RMW)` upgrades the read to a write lock so the
//! subsequent modify cannot deadlock or lose an update. Noxu exposes
//! `LockMode::Rmw` via `Database::get_with_options(ReadOptions::read_modify_write())`.
//!
//! We test under READ-COMMITTED isolation, where a PLAIN read releases its
//! lock immediately (so a concurrent writer would succeed) — this isolates
//! the RMW behaviour: only the RMW write-lock upgrade can block the writer.
//! Under the default serializable isolation a plain read already blocks
//! writers, so it would not distinguish RMW from a normal read.
//!
//! ## LockMode::Rmw write-lock-on-read (JE Cursor.java:5281)
//!
//! An RMW read takes a WRITE lock on the record so a subsequent modify in the
//! same transaction is conflict-free and a concurrent writer blocks at read
//! time. Implemented via `CursorImpl::upgrade_current_to_write_lock`, wired
//! into both `noxu_db::Cursor::get` (on `LockMode::Rmw`) and
//! `Database::get_with_options` (on `ReadOptions::read_modify_write()`).
//!
//! These tests are active (not ignored) and assert the lock-blocking contract:
//! the RMW read blocks a no_wait writer and a concurrent writer until commit.
//! The control test `plain_read_committed_releases_lock_writer_succeeds`
//! confirms a plain read (no RMW) does NOT block, isolating the RMW effect.

use noxu_db::{
    DatabaseConfig, DatabaseEntry, EnvironmentConfig, ReadOptions,
    TransactionConfig,
};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;
use tempfile::TempDir;

fn setup() -> (TempDir, noxu_db::Environment, noxu_db::Database) {
    let dir = TempDir::new().unwrap();
    let env = noxu_db::Environment::open(
        EnvironmentConfig::new(dir.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true),
    )
    .unwrap();
    let db = env
        .open_database(
            None,
            "rmwdb",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap();
    (dir, env, db)
}

fn put_committed(
    env: &noxu_db::Environment,
    db: &noxu_db::Database,
    key: &[u8],
    val: &[u8],
) {
    let txn = env.begin_transaction(None).unwrap();
    db.put_in(
        &txn,
        DatabaseEntry::from_bytes(key),
        DatabaseEntry::from_bytes(val),
    )
    .unwrap();
    txn.commit().unwrap();
}

// ---------------------------------------------------------------------------
// C7.1 — RMW read takes a write lock: no_wait writer conflicts (single thread)
// ---------------------------------------------------------------------------

/// Under read-committed isolation, an RMW read must hold a WRITE lock for the
/// transaction's duration, so a concurrent no_wait writer to the same key must
/// receive a lock conflict (unlike a plain read, which releases its lock).
#[test]
fn rmw_read_holds_write_lock_no_wait_writer_conflicts() {
    let (_dir, env, db) = setup();
    put_committed(&env, &db, b"key", b"v1");

    // Reader under read-committed performs an RMW read and holds.
    let rc = TransactionConfig::read_committed();
    let rmw_txn = env.begin_transaction(Some(&rc)).unwrap();
    let status = db
        .get_with_options(
            Some(&rmw_txn),
            DatabaseEntry::from_bytes(b"key"),
            &ReadOptions::read_modify_write(),
        )
        .unwrap();
    assert!(status.is_some(), "RMW read must find the key");

    // Concurrent no_wait writer to the SAME key must conflict because the RMW
    // read acquired a WRITE lock that is held.
    let no_wait = TransactionConfig::new().with_no_wait(true);
    let writer_txn = env.begin_transaction(Some(&no_wait)).unwrap();
    let write_result = db.put_in(
        &writer_txn,
        DatabaseEntry::from_bytes(b"key"),
        DatabaseEntry::from_bytes(b"v2"),
    );
    assert!(
        write_result.is_err(),
        "no_wait writer must CONFLICT while an RMW reader holds the write \
         lock; got {write_result:?} (RMW LockMode did not acquire a write lock)"
    );
    let _ = writer_txn.abort();

    // After the RMW reader commits, a new writer must succeed.
    rmw_txn.commit().unwrap();
    let writer_txn2 = env.begin_transaction(Some(&no_wait)).unwrap();
    db.put_in(
        &writer_txn2,
        DatabaseEntry::from_bytes(b"key"),
        DatabaseEntry::from_bytes(b"v3"),
    )
    .unwrap();
    writer_txn2.commit().unwrap();
}

/// Control: a PLAIN read under read-committed releases its lock, so the
/// no_wait writer SUCCEEDS. This proves the test above is detecting the RMW
/// write-lock upgrade specifically (not blocked by some always-on read lock).
#[test]
fn plain_read_committed_releases_lock_writer_succeeds() {
    let (_dir, env, db) = setup();
    put_committed(&env, &db, b"key", b"v1");

    let rc = TransactionConfig::read_committed();
    let reader_txn = env.begin_transaction(Some(&rc)).unwrap();
    let mut val = DatabaseEntry::new();
    let _ = db
        .get_into(
            Some(&reader_txn),
            DatabaseEntry::from_bytes(b"key"),
            &mut val,
        )
        .unwrap();

    // Plain read-committed releases the read lock -> no_wait writer succeeds.
    let no_wait = TransactionConfig::new().with_no_wait(true);
    let writer_txn = env.begin_transaction(Some(&no_wait)).unwrap();
    db.put_in(
        &writer_txn,
        DatabaseEntry::from_bytes(b"key"),
        DatabaseEntry::from_bytes(b"v2"),
    )
    .unwrap();
    writer_txn.commit().unwrap();
    reader_txn.commit().unwrap();
}

// ---------------------------------------------------------------------------
// C7.2 — 2-thread blocking: writer blocks until RMW reader commits
// ---------------------------------------------------------------------------

/// Thread A does an RMW read inside a read-committed txn and holds it; thread
/// B's write to the same key must BLOCK until A commits, then proceed. We
/// detect the block by timing: B must not complete until A releases.
#[test]
fn rmw_read_blocks_concurrent_writer_until_commit() {
    let (_dir, env, db) = setup();
    put_committed(&env, &db, b"key", b"v1");

    let env = Arc::new(env);
    let db = Arc::new(db);

    // Barrier: A has taken the RMW lock; B may now attempt its (blocking) write.
    let lock_taken = Arc::new(Barrier::new(2));
    // Shared flag: set true the instant B's write returns.
    let writer_done = Arc::new(std::sync::atomic::AtomicBool::new(false));

    // Thread A: RMW read, signal, hold for a while, then commit.
    let env_a = Arc::clone(&env);
    let db_a = Arc::clone(&db);
    let lt_a = Arc::clone(&lock_taken);
    let wd_a = Arc::clone(&writer_done);
    let a = thread::spawn(move || {
        let rc = TransactionConfig::read_committed();
        let txn = env_a.begin_transaction(Some(&rc)).unwrap();
        let s = db_a
            .get_with_options(
                Some(&txn),
                DatabaseEntry::from_bytes(b"key"),
                &ReadOptions::read_modify_write(),
            )
            .unwrap();
        assert!(s.is_some());

        // Tell B the RMW write lock is held.
        lt_a.wait();

        // Hold the lock. While we sleep, B must be blocked on its write.
        thread::sleep(Duration::from_millis(300));
        // B must NOT have completed its write while we still hold the lock.
        assert!(
            !wd_a.load(std::sync::atomic::Ordering::SeqCst),
            "writer completed before the RMW reader released its write lock \
             (RMW did not block the writer)"
        );

        txn.commit().unwrap();
    });

    // Thread B: wait until A holds the RMW lock, then do a BLOCKING write.
    let env_b = Arc::clone(&env);
    let db_b = Arc::clone(&db);
    let lt_b = Arc::clone(&lock_taken);
    let wd_b = Arc::clone(&writer_done);
    let b = thread::spawn(move || {
        lt_b.wait();
        // Blocking writer (no no_wait): must wait for A to commit.
        let txn = env_b.begin_transaction(None).unwrap();
        let r = db_b.put_in(
            &txn,
            DatabaseEntry::from_bytes(b"key"),
            DatabaseEntry::from_bytes(b"v2"),
        );
        wd_b.store(true, std::sync::atomic::Ordering::SeqCst);
        match r {
            Ok(()) => {
                txn.commit().unwrap();
            }
            other => {
                let _ = txn.abort();
                panic!(
                    "writer should eventually succeed after A commits: {other:?}"
                );
            }
        }
    });

    a.join().unwrap();
    b.join().unwrap();

    // Final value is the writer's, proving the write went through after the
    // RMW reader released.
    let mut val = DatabaseEntry::new();
    let s =
        db.get_into(None, DatabaseEntry::from_bytes(b"key"), &mut val).unwrap();
    assert!(s);
    assert_eq!(val.data_opt(), Some(b"v2" as &[u8]));
}
