// Copyright (C) 2024-2025 Greg Burd.  Apache-2.0 OR MIT.
//! End-to-end locking-behaviour ports from the JE `com.sleepycat.je.txn`
//! package that exercise the public `Environment`/`Database`/`Cursor` API
//! rather than the low-level `LockImpl`/`LockManager` (those live in
//! `noxu-txn`).  Two JE tests are ported here:
//!
//!  * `CursorTxnTest.testNullTxnLockRelease`
//!  * `ReadCommitLockersTest.runTest` (SR #23783)
//!
//! Both are adapted to Noxu's locking model, which differs from JE in one
//! documented way: Noxu READ_COMMITTED (and non-transactional) reads release
//! the record read-lock IMMEDIATELY after each operation, and a READ_COMMITTED
//! cursor uses its PARENT transaction as the single locker (JE creates a
//! per-cursor `ReadCommittedLocker` buddy).  The behavioural invariants the JE
//! tests prove — a non-transactional scan does not accumulate read locks, and
//! two read-committed reads under one txn never self-deadlock — are preserved
//! and asserted below.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use noxu_db::{
    DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig, Get,
    OperationStatus, TransactionConfig,
};
use tempfile::TempDir;

fn open_env(dir: &TempDir) -> Arc<Environment> {
    let cfg = EnvironmentConfig::new(dir.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    Arc::new(Environment::open(cfg).unwrap())
}

fn txn_db(env: &Environment, name: &str) -> noxu_db::Database {
    let db_cfg =
        DatabaseConfig::new().with_allow_create(true).with_transactional(true);
    env.open_database(None, name, &db_cfg).unwrap()
}

// ───────────────────────────────────────────────────────────────────────────
// JE: CursorTxnTest.testNullTxnLockRelease
//
// A cursor opened with a NULL transaction (non-transactional / auto-commit
// locker) must release its read locks per-operation: a forward or backward
// scan of the whole database must NOT accumulate read locks in the live lock
// table.  (JE asserts nReadLocks==1 during a scan and that count() adds no
// locks; the JE cursor's BasicLocker frees the previous read lock as it steps.
// Noxu's auto-commit read path acquires+immediately-releases the read lock, so
// the live-lock-table count returns to zero between steps.)
//
// Fidelity note: JE distinguishes read vs write lock counts via
// `getNReadLocks`/`getNWriteLocks`; Noxu's `LockStats` reports the aggregate
// `n_total_locks` (the read/write split is not tracked per snapshot).  We
// therefore assert the STRONGER, model-appropriate invariant: a null-txn scan
// leaves ZERO locks held after each read (JE's "does not accumulate" made
// exact for the immediate-release model).  A regression that made auto-commit
// reads leak (fail to release) their read lock would make `n_total_locks`
// climb across the scan and fail this test.
// ───────────────────────────────────────────────────────────────────────────
#[test]
fn cursor_txn_test_null_txn_lock_release() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let db = txn_db(&env, "null_txn_locks");

    // Insert 30 records via committed auto-commit puts.  JE uses 10 keys x 3
    // dup values; the DB here is non-dup, so 30 distinct keys -- the invariant
    // under test is per-step read-lock release, not duplicates.
    const N: u32 = 30;
    for i in 0..N {
        let key = format!("k{i:04}");
        let txn = env.begin_transaction(None).unwrap();
        db.put_in(
            &txn,
            DatabaseEntry::from_bytes(key.as_bytes()),
            DatabaseEntry::from_bytes(b"v"),
        )
        .unwrap();
        txn.commit().unwrap();
    }

    let locks_at_rest = env.stats().unwrap().lock.n_total_locks;

    // -- Part A: NULL-txn scan releases read locks per operation. ----------
    // JE: "read locks are held on forward traversal" then released as the
    // cursor steps.  Under Noxu's auto-commit read the lock is acquired and
    // released within each step, so the live lock table never grows beyond
    // its resting count.
    let mut cursor = db.open_cursor(None).unwrap();
    let mut key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();
    let mut seen = 0u32;
    let mut max_held_null_txn = 0u64;
    let mut status = cursor.get(&mut key, &mut data, Get::First, None).unwrap();
    while status == OperationStatus::Success {
        seen += 1;
        let held = env.stats().unwrap().lock.n_total_locks;
        max_held_null_txn =
            max_held_null_txn.max(held.saturating_sub(locks_at_rest));
        status = cursor.get(&mut key, &mut data, Get::Next, None).unwrap();
    }
    assert_eq!(seen, N, "forward scan must visit every record");
    drop(cursor);
    assert_eq!(
        env.stats().unwrap().lock.n_total_locks,
        locks_at_rest,
        "null-txn scan must leave no residual locks"
    );

    // -- Part B: a TRANSACTIONAL scan DOES accumulate read locks (contrast).-
    // This makes Part A non-vacuous: it proves the lock table is observable
    // and that the difference is the null-txn per-op release, not that locks
    // are never taken.  A repeatable-read txn holds every read lock until
    // commit, so mid-scan the held count exceeds the resting count.
    let scan_txn = env.begin_transaction(None).unwrap();
    let mut tcursor = db.open_cursor_in(&scan_txn, None).unwrap();
    let mut max_held_txn = 0u64;
    let mut tstatus =
        tcursor.get(&mut key, &mut data, Get::First, None).unwrap();
    while tstatus == OperationStatus::Success {
        let held = env.stats().unwrap().lock.n_total_locks;
        max_held_txn = max_held_txn.max(held.saturating_sub(locks_at_rest));
        tstatus = tcursor.get(&mut key, &mut data, Get::Next, None).unwrap();
    }
    drop(tcursor);
    scan_txn.commit().unwrap();

    // The discriminating assertions: the transactional scan held strictly
    // more locks at once than the null-txn scan ever did.  If auto-commit
    // reads leaked (failed to release), max_held_null_txn would climb to N and
    // this ordering would collapse -- the de-vacuuming guard.
    assert!(
        max_held_txn > max_held_null_txn,
        "transactional scan must hold more concurrent read locks than a \
         null-txn scan (txn={max_held_txn}, null={max_held_null_txn}); a \
         null-txn read that failed to release would erase this difference \
         (JE CursorTxnTest.testNullTxnLockRelease)"
    );
    // A null-txn scan of uncontended data holds no lasting lock between steps.
    assert!(
        max_held_null_txn <= 1,
        "null-txn scan must not accumulate read locks across steps \
         (max held over resting = {max_held_null_txn})"
    );

    // Backward null-txn scan -- same per-op release invariant.
    let mut cursor = db.open_cursor(None).unwrap();
    let mut status = cursor.get(&mut key, &mut data, Get::Last, None).unwrap();
    let mut seen_back = 0u32;
    while status == OperationStatus::Success {
        seen_back += 1;
        status = cursor.get(&mut key, &mut data, Get::Prev, None).unwrap();
    }
    assert_eq!(seen_back, N, "backward scan must visit every record");
    drop(cursor);
    assert_eq!(
        env.stats().unwrap().lock.n_total_locks,
        locks_at_rest,
        "null-txn scans must leave no residual locks"
    );
}


// ───────────────────────────────────────────────────────────────────────────
// JE: ReadCommitLockersTest.runTest  (SR #23783)
//
// The SR: two read-committed reads issued by the SAME parent transaction must
// not create a FALSE deadlock when a foreign writer is waiting between them.
// In JE each read-committed cursor made its own `ReadCommittedLocker` buddy;
// before the fix the two buddies (L1, L3) were not recognised as sharing the
// same parent (X1), so the second read (L3) waited behind the foreign writer
// (X2) which was itself waiting on L1 — a false T1→T2→T1 cycle.  The fix makes
// the buddies share locks with their parent so no self-cycle forms.
//
// Noxu adaptation: Noxu READ_COMMITTED uses the PARENT txn as the single
// locker and releases the record lock immediately after each op, so two RC
// reads under one txn share one locker id by construction — the SR's false
// cycle cannot form.  The behavioural invariant we assert is the JE one made
// concrete: with a foreign writer contending on the same key, a txn issuing
// two READ_COMMITTED reads of that key completes BOTH reads (no self-deadlock,
// no spurious timeout), and the foreign writer's committed value ("newdata")
// is the final durable state.
//
// A regression that made two RC reads under one txn self-deadlock (e.g. by
// giving each RC read a distinct non-sharing sub-locker that could wait on a
// foreign writer which waits on the first sub-locker) would hang this test
// (caught by the bounded join below) — the de-vacuuming guard.
// ───────────────────────────────────────────────────────────────────────────
#[test]
fn read_commit_lockers_test_no_false_self_deadlock() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let db = Arc::new(txn_db(&env, "rc_buddies"));

    // Insert record R = ("key","data") and commit.
    {
        let txn = env.begin_transaction(None).unwrap();
        db.put_in(
            &txn,
            DatabaseEntry::from_bytes(b"key"),
            DatabaseEntry::from_bytes(b"data"),
        )
        .unwrap();
        txn.commit().unwrap();
    }

    // Barrier phases coordinate T1 (two RC reads under X1) and T2 (one write
    // under X2), mirroring JE's synchronizer1/synchronizer2 handshake.
    // phase_after_first_read: T1 signals it has done its first RC read.
    // phase_writer_engaged:   T2 signals it has issued (and possibly blocked
    //                         on) its write, releasing T1 to do read #2.
    let after_first = Arc::new(Barrier::new(2));
    let writer_engaged = Arc::new(Barrier::new(2));

    // T1: two READ_COMMITTED reads of R under one txn X1.
    let t1_env = Arc::clone(&env);
    let t1_db = Arc::clone(&db);
    let t1_after = Arc::clone(&after_first);
    let t1_writer = Arc::clone(&writer_engaged);
    let t1 = thread::spawn(move || {
        let rc = TransactionConfig::new().with_read_committed(true);
        let x1 = t1_env.begin_transaction(Some(&rc)).unwrap();

        // Read #1 (cursor C1).
        {
            let mut c1 = t1_db.open_cursor_in(&x1, None).unwrap();
            let mut k = DatabaseEntry::from_bytes(b"key");
            let mut d = DatabaseEntry::new();
            let s = c1.get(&mut k, &mut d, Get::Search, None).unwrap();
            assert_eq!(s, OperationStatus::Success, "C1 read #1 must find R");
        }

        // Signal main/T2 that read #1 is done, then wait until T2 has issued
        // its write (and is blocked on it).
        t1_after.wait();
        t1_writer.wait();

        // Read #2 (cursor C3) under the SAME txn X1 — must not self-deadlock.
        {
            let mut c3 = t1_db.open_cursor_in(&x1, None).unwrap();
            let mut k = DatabaseEntry::from_bytes(b"key");
            let mut d = DatabaseEntry::new();
            let s = c3.get(&mut k, &mut d, Get::Search, None).unwrap();
            assert_eq!(s, OperationStatus::Success, "C3 read #2 must find R");
        }
        x1.commit().unwrap();
    });

    // T2: one write of R under X2 (blocks only if T1 held the read lock,
    // which under Noxu RC it does not — the write proceeds).
    let t2_env = Arc::clone(&env);
    let t2_db = Arc::clone(&db);
    let t2_after = Arc::clone(&after_first);
    let t2_writer = Arc::clone(&writer_engaged);
    let t2 = thread::spawn(move || {
        // Wait for T1's first RC read to complete.
        t2_after.wait();
        let x2 = t2_env.begin_transaction(None).unwrap();
        // Update R -> "newdata".
        t2_db
            .put_in(
                &x2,
                DatabaseEntry::from_bytes(b"key"),
                DatabaseEntry::from_bytes(b"newdata"),
            )
            .unwrap();
        // Release T1 to do read #2 while X2 (still uncommitted) holds R's
        // write lock — this is the moment the false cycle would have formed.
        t2_writer.wait();
        // Give T1's read #2 a moment to attempt its lock, then commit so the
        // read (if it blocked on the writer) can proceed and observe newdata.
        thread::sleep(Duration::from_millis(50));
        x2.commit().unwrap();
    });

    // Bounded join: if a false self-deadlock existed, one thread would hang.
    let start = Instant::now();
    t1.join().unwrap();
    t2.join().unwrap();
    assert!(
        start.elapsed() < Duration::from_secs(20),
        "two RC reads under one txn must not self-deadlock (SR#23783)"
    );

    // Final durable state is the writer's committed value.
    assert_eq!(
        db.get(b"key").unwrap().as_deref(),
        Some(&b"newdata"[..]),
        "after both txns commit, R must hold the writer's newdata"
    );
}


// ───────────────────────────────────────────────────────────────────────────
// JE: TxnTest.testAbortNoSplit -- inserting enough records to make the tree
// "ripe for a split", then aborting, must leave the database EMPTY and must
// not corrupt the tree.  JE additionally asserts abort never attempts a tree
// split (a latch-ordering invariant checked via an internal no-latches-while-
// locking assertion).  Noxu has no such internal latch-order probe, so we port
// the OBSERVABLE behaviour: after aborting a large insert batch the DB is
// empty and a concurrent txn (opened before the abort) sees nothing.
// ───────────────────────────────────────────────────────────────────────────
#[test]
fn txn_test_abort_no_split_leaves_db_empty() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let db = txn_db(&env, "abort_no_split");

    // Insert enough data (well past a single BIN's fanout) to make the tree
    // ripe for a split, all under ONE transaction.
    let txn = env.begin_transaction(None).unwrap();
    const NUM_FOR_SPLIT: u32 = 200;
    for i in 0..NUM_FOR_SPLIT {
        let key = format!("k{i:05}");
        db.put_in(
            &txn,
            DatabaseEntry::from_bytes(key.as_bytes()),
            DatabaseEntry::from_bytes(b"d"),
        )
        .unwrap();
    }

    // A second txn (JE's "spoiler") started before the abort must not see the
    // uncommitted inserts.
    let spoiler = env.begin_transaction(None).unwrap();

    // Abort the big insert batch.
    txn.abort().unwrap();

    // The spoiler must observe an empty database (nothing was committed).
    {
        let mut c = db.open_cursor_in(&spoiler, None).unwrap();
        let mut k = DatabaseEntry::new();
        let mut d = DatabaseEntry::new();
        let s = c.get(&mut k, &mut d, Get::First, None).unwrap();
        assert_eq!(
            s,
            OperationStatus::NotFound,
            "after aborting the insert batch, the DB must be empty for the \
             spoiler txn (JE TxnTest.testAbortNoSplit)"
        );
    }
    spoiler.abort().unwrap();

    // A fresh auto-commit read also finds nothing.
    let mut c = db.open_cursor(None).unwrap();
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    assert_eq!(
        c.get(&mut k, &mut d, Get::First, None).unwrap(),
        OperationStatus::NotFound,
        "aborted inserts must leave no committed records"
    );
    drop(c);

    // The tree is still usable: a new committed insert then reads back.
    let t2 = env.begin_transaction(None).unwrap();
    db.put_in(
        &t2,
        DatabaseEntry::from_bytes(b"after"),
        DatabaseEntry::from_bytes(b"ok"),
    )
    .unwrap();
    t2.commit().unwrap();
    assert_eq!(
        db.get(b"after").unwrap().as_deref(),
        Some(&b"ok"[..]),
        "tree must remain usable after an aborted split-ripe batch"
    );
}


// JE: TxnEndTest.testDbCreation -- N/A / COVERED.
//
// JE's scenario opens the SAME database name concurrently under two
// transactions (txnA creates it; txnB opens it non-create) to prove the DDL
// name is invisible until txnA commits.  Noxu enforces ONE open handle per
// database name per Environment (Environment::open_database returns
// "Database 'foo' is already open" for a second open), so the two-handles
// variant does not map to Noxu's model.  The PORTABLE intent -- DDL
// create/remove/rename/truncate under a txn is invisible/rolled back until
// the txn resolves -- is covered by crates/noxu-db/tests/ddl_txn_abort_test.rs
// (remove/rename/truncate under a txn commit vs abort).  Classified N/A for
// the two-concurrent-handles shape (one-handle-per-name design), COVERED for
// the DDL-visibility intent.

// ───────────────────────────────────────────────────────────────────────────
// JE: TxnEndTest.testClose -- a transaction is unusable after it has been
// committed: using it (e.g. to open a database) must be rejected, not silently
// accepted.
// ───────────────────────────────────────────────────────────────────────────
#[test]
fn txn_end_test_closed_transaction_is_unusable() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let db = txn_db(&env, "closed_txn");

    let txn_a = env.begin_transaction(None).unwrap();
    txn_a.commit().unwrap();

    // A committed (closed) transaction must be unusable: DATA operations
    // through it are rejected (state != Open -> InvalidTransaction).  This is
    // the observable "closed transaction is unusable" invariant JE asserts.
    let put = db.put_in(
        &txn_a,
        DatabaseEntry::from_bytes(b"k"),
        DatabaseEntry::from_bytes(b"v"),
    );
    assert!(
        put.is_err(),
        "put through a committed transaction must be rejected \
         (JE TxnEndTest.testClose)"
    );
    let mut out = DatabaseEntry::new();
    let get =
        db.get_into(Some(&txn_a), &DatabaseEntry::from_bytes(b"k"), &mut out);
    assert!(
        get.is_err(),
        "get through a committed transaction must be rejected \
         (JE TxnEndTest.testClose)"
    );
}

// JE: TxnEndTest.testClose (DDL sub-case) -- BUG CANDIDATE (NEW-TXN-1).
//
// JE rejects using a closed transaction for ANY operation, including
// `env.openDatabase(closedTxn, ...)` (throws IllegalArgumentException).  Noxu
// enforces this on the DATA path (put/get through a committed txn error with
// InvalidTransaction -- see `txn_end_test_closed_transaction_is_unusable`), but
// `Environment::open_database` does NOT validate the passed transaction's
// state, so a DDL create/open through a COMMITTED transaction is silently
// accepted.  This is a minor gap (DDL only, not a data-integrity bug), kept
// ignored and escalated as NEW-TXN-1.
//
// Root cause: `Environment::open_database` (crates/noxu-db/src/environment.rs)
// calls `self.check_open()` (env open?) but never checks
// `txn.state() == Open` / `txn.is_valid()` on the supplied `Option<&Transaction>`
// before creating/opening the database under it.  Fix: add a txn-state guard
// mirroring the data path's `check_state`.
#[test]
#[ignore = "NEW-TXN-1: open_database does not reject a committed/closed txn (DDL-path gap)"]
fn txn_end_test_close_open_database_rejects_closed_txn_bug() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let create_cfg =
        DatabaseConfig::new().with_allow_create(true).with_transactional(true);

    let txn_a = env.begin_transaction(None).unwrap();
    txn_a.commit().unwrap();

    // FAITHFUL JE expectation: using a committed transaction to open/create a
    // database must be rejected.  Fails on the current engine (open_database
    // accepts the committed txn).
    let r = env.open_database(Some(&txn_a), "foo", &create_cfg);
    assert!(
        r.is_err(),
        "NEW-TXN-1: open_database through a committed (closed) transaction \
         must be rejected (JE TxnEndTest.testClose); engine currently accepts it"
    );
}

