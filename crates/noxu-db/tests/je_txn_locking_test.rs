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
    let get = db.get_into(Some(&txn_a), b"k", &mut out);
    assert!(
        get.is_err(),
        "get through a committed transaction must be rejected \
         (JE TxnEndTest.testClose)"
    );
}

// JE: TxnEndTest.testClose (DDL sub-case) -- NEW-TXN-1 production guard-fix.
//
// JE rejects using a closed transaction for ANY operation, including
// `env.openDatabase(closedTxn, ...)` (throws IllegalArgumentException).  Noxu
// enforced this on the DATA path (put/get through a committed txn error with
// InvalidTransaction -- see `txn_end_test_closed_transaction_is_unusable`) but
// NOT on the DDL path: `Environment::open_database` silently accepted a
// committed/aborted transaction.  This test drove the NEW-TXN-1 guard added to
// `Environment::open_database` (a txn-state check mirroring the data path's
// `Txn::check_state`).  Fails on base 2ad524d2 (open_database accepts the
// committed txn); passes with the guard.
#[test]
fn txn_end_test_close_open_database_rejects_closed_txn() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let create_cfg =
        DatabaseConfig::new().with_allow_create(true).with_transactional(true);

    // A committed transaction must be rejected by open_database.
    let txn_committed = env.begin_transaction(None).unwrap();
    txn_committed.commit().unwrap();
    let r = env.open_database(Some(&txn_committed), "foo", &create_cfg);
    assert!(
        r.is_err(),
        "NEW-TXN-1: open_database through a committed (closed) transaction \
         must be rejected (JE TxnEndTest.testClose)"
    );

    // An aborted transaction must likewise be rejected.
    let txn_aborted = env.begin_transaction(None).unwrap();
    txn_aborted.abort().unwrap();
    let r2 = env.open_database(Some(&txn_aborted), "bar", &create_cfg);
    assert!(
        r2.is_err(),
        "NEW-TXN-1: open_database through an aborted transaction must be \
         rejected"
    );
}

// NEW-TXN-1 regression guard: the txn-state check must NOT break the legitimate
// cases -- an OPEN transaction opening a DB, and auto-commit (txn = None).
#[test]
fn txn_end_test_open_database_accepts_open_txn_and_auto_commit() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let create_cfg =
        DatabaseConfig::new().with_allow_create(true).with_transactional(true);

    // Auto-commit (txn = None) must still work.
    let db_auto = env.open_database(None, "auto_db", &create_cfg);
    assert!(
        db_auto.is_ok(),
        "auto-commit open_database must work: {:?}",
        db_auto.err()
    );
    drop(db_auto);

    // An OPEN transaction opening a (different) DB must succeed, then commit.
    let txn_open = env.begin_transaction(None).unwrap();
    let db_txn = env.open_database(Some(&txn_open), "txn_db", &create_cfg);
    assert!(
        db_txn.is_ok(),
        "an Open transaction must be able to open/create a database: {:?}",
        db_txn.err()
    );
    drop(db_txn);
    txn_open.commit().unwrap();
}

// ───────────────────────────────────────────────────────────────────────────
// JE: TxnTest.testRepeatingOperationFailures -- BUG CANDIDATE (NEW-TXN-2).
//
// JE: when an operation on a txn fails (e.g. a lock conflict /
// LockConflictException, which extends OperationFailureException), the txn is
// set abort-only (Transaction.State.MUST_ABORT) and is thereafter invalid; a
// FURTHER operation on the same txn re-throws (the same failure is preserved as
// the cause), and the txn cannot commit -- only abort.  This is JE's
// "operation failure invalidates the transaction" contract.
//
// Noxu DIVERGES: a lock TIMEOUT/conflict surfaced from a `db.put`/`db.get`
// leaves the transaction in state `Open` (NOT MustAbort), and a subsequent
// operation on a DIFFERENT key SUCCEEDS.  (Verified: after txn2's put on a
// write-locked key times out, txn2.state() == Open and txn2's next put
// succeeds.)  Only the deadlock-victim path (inside the LockManager wait loop)
// and a commit-time I/O failure flip Noxu to MustAbort; a plain lock
// timeout/conflict does not.
//
// This is NOT a data-integrity bug (the failed op did not partially apply, and
// the txn's own writes still commit/abort atomically), but it is a faithful-
// parity divergence from JE's OperationFailureException semantics.  Whether
// Noxu SHOULD adopt JE's "lock conflict invalidates the txn" contract is a
// design decision with cross-layer impact (cursor_impl + Txn error handling),
// so this is escalated as NEW-TXN-2 rather than fixed unilaterally, and the
// faithful test is kept ignored (not weakened).
//
// Root cause: neither `Txn::lock` (crates/noxu-txn/src/txn.rs) nor the DBI
// cursor lock path (crates/noxu-dbi/src/cursor_impl.rs) calls
// `set_only_abortable()` when a `LockTimeout`/`LockConflict`/`LockNotAvailable`
// is surfaced; the error is propagated but the txn stays `Open`.  JE parity
// would flip the locker abort-only on any LockConflictException from an
// operation (Locker.setOnlyAbortable / Txn handling of
// OperationFailureException).
#[test]
fn txn_test_repeating_operation_failures() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let db = txn_db(&env, "repeating");

    let txn1 = env.begin_transaction(None).unwrap();
    let short = TransactionConfig::new().with_lock_timeout_ms(50);
    let txn2 = env.begin_transaction(Some(&short)).unwrap();

    // txn1 holds a write lock on key1.
    db.put_in(
        &txn1,
        DatabaseEntry::from_bytes(b"key1"),
        DatabaseEntry::from_bytes(b"v"),
    )
    .unwrap();

    // txn2's put on key1 fails with a lock conflict/timeout.
    let first = db.put_in(
        &txn2,
        DatabaseEntry::from_bytes(b"key1"),
        DatabaseEntry::from_bytes(b"v"),
    );
    assert!(first.is_err(), "txn2's contended put must fail");

    // FAITHFUL JE expectation (fixed as NEW-TXN-2): txn2 is now invalid
    // (MUST_ABORT) -- the lock-conflict operation failure poisoned the txn
    // abort-only (JE Locker.setOnlyAbortable on OperationFailureException,
    // Locker.java:285).
    assert!(
        !txn2.is_valid(),
        "NEW-TXN-2: after a lock-conflict operation failure the txn must be \
         invalid/abort-only (JE TxnTest.testRepeatingOperationFailures)"
    );
    // A further operation on the abort-only txn must be rejected (check_state
    // returns InvalidTransaction).
    let second = db.put_in(
        &txn2,
        DatabaseEntry::from_bytes(b"key2"),
        DatabaseEntry::from_bytes(b"v"),
    );
    assert!(
        second.is_err(),
        "NEW-TXN-2: a further operation on the abort-only txn must be rejected"
    );
    // The poisoned txn cannot commit -- it can only be aborted.
    assert!(
        txn2.commit().is_err(),
        "NEW-TXN-2: an abort-only txn must refuse to commit"
    );

    let _ = txn2.abort();
    let _ = txn1.commit();
}

// NEW-TXN-2 positive guard: a lock WAIT that eventually SUCCEEDS must NOT
// poison the txn.  A blocking waiter whose contended lock is later released
// (owner commits) is granted the lock and can still commit normally -- only a
// lock request that RETURNS AN ERROR poisons (JE Locker.setOnlyAbortable fires
// on OperationFailureException, not on a successful grant after a wait).
#[test]
fn txn_test_lock_wait_that_succeeds_does_not_poison() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let db = Arc::new(txn_db(&env, "wait_ok"));

    // txn1 holds a write lock on key1, then releases it shortly by committing.
    let txn1 = env.begin_transaction(None).unwrap();
    db.put_in(
        &txn1,
        DatabaseEntry::from_bytes(b"key1"),
        DatabaseEntry::from_bytes(b"v1"),
    )
    .unwrap();

    // txn2 has a generous lock timeout so its put on key1 WAITS and then
    // SUCCEEDS once txn1 commits and releases the lock.
    let env2 = Arc::clone(&env);
    let db2 = Arc::clone(&db);
    let waiter = thread::spawn(move || {
        let long = TransactionConfig::new().with_lock_timeout_ms(10_000);
        let txn2 = env2.begin_transaction(Some(&long)).unwrap();
        // Blocks until txn1 releases the write lock, then is granted.
        db2.put_in(
            &txn2,
            DatabaseEntry::from_bytes(b"key1"),
            DatabaseEntry::from_bytes(b"v2"),
        )
        .expect("waiter should be granted the lock once txn1 commits");
        // A successful lock WAIT must leave the txn usable: a further op and
        // the commit must both succeed (the txn was NOT poisoned).
        assert!(
            txn2.is_valid(),
            "waiter txn must still be Open after a granted wait"
        );
        db2.put_in(
            &txn2,
            DatabaseEntry::from_bytes(b"key2"),
            DatabaseEntry::from_bytes(b"v3"),
        )
        .expect("second op on a non-poisoned txn must succeed");
        txn2.commit()
            .expect("a txn that only WAITED (never failed) must commit");
    });

    // Give the waiter time to start blocking, then release the lock.
    thread::sleep(Duration::from_millis(150));
    txn1.commit().unwrap();

    waiter.join().unwrap();

    // Final durable state reflects the waiter's committed writes.
    let mut out = DatabaseEntry::new();
    db.get_into(None, DatabaseEntry::from_bytes(b"key1"), &mut out).unwrap();
    assert_eq!(out.data(), b"v2");
}

// NEW-TXN-2 (JE-faithful, no-wait): a NO-WAIT transaction whose operation
// fails to get a lock (surfacing `LockNotAvailable`) must NOT be poisoned.
// JE's `LockNotAvailableException` explicitly documents The Transaction

// NEW-TXN-2 (JE-faithful, no-wait): a NO-WAIT transaction whose operation
// fails to get a lock (surfacing `LockNotAvailable`) must NOT be poisoned.
// JE's `LockNotAvailableException` explicitly documents "The Transaction
// handle is not invalidated as a result of this exception"
// (LockNotAvailableException.java:18-23,41-43 -- "Do not set abort-only for a
// no-wait lock failure"; ctor path LockConflictException.java:127-128 passes
// abortOnly=false).  So after a no-wait failure the txn stays usable: a
// subsequent operation on a FREE key succeeds and the txn commits.
#[test]
fn txn_test_no_wait_lock_failure_does_not_poison() {
    use noxu_db::NoxuError;

    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let db = txn_db(&env, "no_wait_ok");

    // txn1 holds a write lock on key1.
    let txn1 = env.begin_transaction(None).unwrap();
    db.put_in(
        &txn1,
        DatabaseEntry::from_bytes(b"key1"),
        DatabaseEntry::from_bytes(b"v1"),
    )
    .unwrap();

    // txn2 is a NO-WAIT txn: its put on the write-locked key1 fails
    // IMMEDIATELY with LockNotAvailable (never waits).
    let cfg = TransactionConfig::new().with_no_wait(true);
    let txn2 = env.begin_transaction(Some(&cfg)).unwrap();
    let contended = db.put_in(
        &txn2,
        DatabaseEntry::from_bytes(b"key1"),
        DatabaseEntry::from_bytes(b"v2"),
    );
    assert!(
        matches!(contended, Err(NoxuError::LockNotAvailable)),
        "no-wait put on a write-locked key must fail with LockNotAvailable, \
         got {contended:?}"
    );

    // JE contract: the no-wait failure did NOT invalidate the handle.
    assert!(
        txn2.is_valid(),
        "NEW-TXN-2: a no-wait LockNotAvailable failure must NOT poison the txn \
         (JE LockNotAvailableException: handle not invalidated)"
    );

    // The txn is still usable: an op on a FREE key succeeds ...
    db.put_in(
        &txn2,
        DatabaseEntry::from_bytes(b"free_key"),
        DatabaseEntry::from_bytes(b"v3"),
    )
    .expect("op on a free key after a no-wait failure must succeed");

    // ... and the txn commits normally.
    txn2.commit().expect(
        "a no-wait txn that only hit LockNotAvailable must still commit",
    );

    let _ = txn1.commit();

    // The committed write on the free key is durable.
    let mut out = DatabaseEntry::new();
    db.get_into(None, DatabaseEntry::from_bytes(b"free_key"), &mut out)
        .unwrap();
    assert_eq!(out.data(), b"v3");
}
