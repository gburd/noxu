// Copyright (C) 2024-2026 Greg Burd.  Apache-2.0 OR MIT.
//! DELTA-REVIEW witnesses for BUG-BARE-RMW-LOCK-LEAK part 2 (release-on-MOVE,
//! commit 6b1348bd).  Independent reviewer's adversarial probes for the
//! coordinator's two concerns:
//!
//!   #1  A lost-update window on the SAME record (RMW-read A, then
//!       put_current / re-RMW on A).
//!   #2  Placement vs JE: JE releases non-txn locks at the MOVE
//!       (beginMoveCursor / reset) before ANY positioning op, including a
//!       NON-RMW read; this fix releases only at the next RMW write-lock
//!       acquisition.
//!
//! JE references:
//!   LockMode.java:84-86   "both read and write locks are only held while a
//!                          cursor is positioned on the record, and are
//!                          released when the cursor is moved to a different
//!                          record or closed."
//!   Cursor.java:4135      searchExact -> beginMoveCursor(false) -> reset
//!   Cursor.java:2995      putCurrentNoNotify -> beginUseExistingCursor (NO
//!                          releaseNonTxnLocks: putCurrent does NOT move).
//!   Cursor.java:3070      getCurrentNoDups -> beginUseExistingCursor (NO
//!                          release: getCurrent does NOT move).
//!   CursorImpl.java:547   beforeNonStickyOp -> releaseNonTxnLocks (advancing).
//!   CursorImpl.java:597   reset -> releaseNonTxnLocks (search / failed op).
#![cfg(not(noxu_shuttle))]

use noxu_dbi::{
    CursorImpl, DatabaseConfig, DatabaseId, DatabaseImpl, DbType, PutMode,
    SearchMode,
};
use noxu_sync::RwLock;
use noxu_txn::{LockManager, LockType, TxnManager};
use noxu_util::Lsn;
use std::sync::{Arc, Mutex};

fn mk_db() -> Arc<RwLock<DatabaseImpl>> {
    let db = Arc::new(RwLock::new(DatabaseImpl::new(
        DatabaseId::new(1),
        "rmw-delta".into(),
        DbType::User,
        &DatabaseConfig::default(),
    )));
    {
        let tree = db.read();
        let t = tree.get_real_tree().expect("tree");
        t.insert(b"A".to_vec(), b"va".to_vec(), Lsn::new(1, 100)).unwrap();
        t.insert(b"B".to_vec(), b"vb".to_vec(), Lsn::new(1, 200)).unwrap();
    }
    db
}

/// CONCERN #1 (put_current after RMW on the SAME record).
///
/// RMW-read A takes a WRITE lock on A (owner = cursor id).  A following
/// put_current on A must NOT go through the move-release path, so A's write
/// lock is held CONTINUOUSLY from the RMW read through the put — no
/// release/re-acquire window where another locker could slip a write in
/// between (that would be the lost update RMW exists to prevent).  JE:
/// putCurrent -> putCurrentNoNotify -> beginUseExistingCursor
/// (Cursor.java:2995), which does NOT call releaseNonTxnLocks.
///
/// Deterministic witness: a SECOND locker tries a non-blocking WRITE lock on
/// A at the exact instant between the RMW read and the put_current.  If the
/// fix erroneously released A there, the probe would SUCCEED (window open).
/// It must FAIL (A continuously held by the cursor).
#[test]
fn concern1_rmw_then_putcurrent_same_record_holds_lock_continuously() {
    let db = mk_db();
    let lm = Arc::new(LockManager::with_lock_timeout(50));
    let tm = Arc::new(TxnManager::new(lm.clone()));
    let mut cursor = CursorImpl::new(db, 7)
        .with_lock_manager(lm.clone())
        .with_txn_manager(tm);

    // 1. RMW-read A -> WRITE lock on A under the cursor's id (7).
    cursor.search(b"A", None, SearchMode::Set).unwrap();
    cursor.upgrade_current_to_write_lock().unwrap();
    assert_eq!(lm.n_total_locks(), 1, "RMW on A holds exactly one lock");
    let a_lsn = cursor.get_current_lsn();

    // 2. In the window a SECOND locker (id 999) must NOT be able to take A.
    //    A non-blocking WRITE request must be DENIED (A still owned by 7).
    let probe = lm.lock(a_lsn, 999, LockType::Write, true, false);
    assert!(
        probe.is_err(),
        "LOST-UPDATE WINDOW: another locker acquired A's write lock between \
         the RMW read and put_current; A's write lock was released by the \
         move-fix on a SAME-record op.  grant={probe:?}"
    );

    // 3. put_current on A: re-acquires the SAME owner's lock (Existing grant),
    //    performs the write, then releases per-op (release_bare_write_locks)
    //    -- JE: a non-txn DB op releases its lock before returning
    //    (LockMode.java:86-88).  The point proven here is that the move-fix
    //    never ran between the RMW read and the put (step 2's probe was
    //    DENIED): A's write lock was held CONTINUOUSLY across the read..write
    //    span, so no concurrent writer could slip in for a lost update.
    cursor.put(b"A", b"va2", PutMode::Current).unwrap();
    assert_eq!(
        lm.n_total_locks(),
        0,
        "a bare put releases its write lock per-op after the write (JE: a \
         non-txn DB op releases before returning); A was held continuously \
         across the RMW-read..put window (step 2 probe denied)"
    );

    cursor.close().unwrap();
    assert_eq!(lm.n_total_locks(), 0, "nothing left to release");
}

/// CONCERN #1 (two-thread deterministic): the window between an RMW-read of A
/// and the put on A is CLOSED — a concurrent writer cannot slip a write into
/// A there (which would be the lost update RMW exists to prevent).  Then,
/// after the put releases A per-op, the blocked writer DOES acquire A (proving
/// the writer really was contending for A, so the earlier denial is not
/// vacuous).
///
/// Deterministic via a barrier: the writer signals "I am about to request A",
/// the main thread first proves a non-blocking probe is DENIED while it holds
/// A across the read..put span, then releases (put + close) and the writer's
/// blocking request completes.
#[test]
fn concern1_two_thread_writer_blocked_across_rmw_putcurrent() {
    use std::sync::mpsc;
    use std::time::Duration;
    let db = mk_db();
    let lm = Arc::new(LockManager::with_lock_timeout(1000));
    let tm = Arc::new(TxnManager::new(lm.clone()));
    let a_lsn = Lsn::new(1, 100).as_u64();

    let mut cursor = CursorImpl::new(db, 7)
        .with_lock_manager(lm.clone())
        .with_txn_manager(tm);
    cursor.search(b"A", None, SearchMode::Set).unwrap();
    cursor.upgrade_current_to_write_lock().unwrap();

    // Writer thread: BLOCKING write request on A under locker 1234.  It must
    // not complete while the main thread holds A across read..put.
    let lmw = lm;
    let (done_tx, done_rx) = mpsc::channel();
    let w = std::thread::spawn(move || {
        let r = lmw.lock(a_lsn, 1234, LockType::Write, false, false);
        done_tx.send(r.is_ok()).unwrap();
    });

    // While the writer is blocked, the window must stay closed: the writer's
    // request has NOT completed yet (A held by the cursor).
    std::thread::sleep(Duration::from_millis(60));
    assert!(
        done_rx.try_recv().is_err(),
        "LOST UPDATE: writer acquired A while the bare cursor held it across \
         the RMW-read..put window"
    );

    // put on A: re-acquires same owner, writes, releases per-op -> writer
    // unblocks AFTER the put, never during the read..put span.
    cursor.put(b"A", b"va2", PutMode::Current).unwrap();
    cursor.close().unwrap();

    // Non-vacuous: the writer really was contending and now gets A.
    let won = done_rx.recv_timeout(Duration::from_secs(2));
    w.join().unwrap();
    assert_eq!(
        won,
        Ok(true),
        "writer must acquire A only AFTER the bare cursor released it (per-op \
         after the put); got {won:?}"
    );
}

/// CONCERN #1 (re-RMW the SAME record).  RMW-read A, then RMW-read A again.
/// The move-fix's release_non_txn_locks() releases A then re-acquires A under
/// the same owner.  This is the EXACT JE behaviour: a second getSearchKey(A,
/// RMW) is a MOVE (searchExact -> beginMoveCursor(false) -> reset ->
/// releaseNonTxnLocks, Cursor.java:4135) that releases A's lock before
/// re-locking it.  End state: exactly one lock on A.
#[test]
fn concern1_re_rmw_same_record_ends_with_one_lock() {
    let db = mk_db();
    let lm = Arc::new(LockManager::with_lock_timeout(50));
    let tm = Arc::new(TxnManager::new(lm.clone()));
    let mut cursor = CursorImpl::new(db, 7)
        .with_lock_manager(lm.clone())
        .with_txn_manager(tm);

    cursor.search(b"A", None, SearchMode::Set).unwrap();
    cursor.upgrade_current_to_write_lock().unwrap();
    assert_eq!(lm.n_total_locks(), 1);

    cursor.search(b"A", None, SearchMode::Set).unwrap();
    cursor.upgrade_current_to_write_lock().unwrap();
    assert_eq!(
        lm.n_total_locks(),
        1,
        "re-RMW the same record ends holding exactly one lock on A (JE: \
         searchExact releases then re-locks; same owner, idempotent end state)"
    );
    cursor.close().unwrap();
    assert_eq!(lm.n_total_locks(), 0);
}

/// CONCERN #2 (placement deviation): JE releases non-txn locks at the MOVE,
/// before ANY positioning op — including a plain NON-RMW read.  This fix
/// releases only at the next RMW write-lock acquisition.  So after RMW(A)
/// then a plain read of B, Noxu RETAINS A's write lock (released only at the
/// next RMW or at close), whereas JE would have released A at the search/
/// getNext move (beforeNonStickyOp / reset, CursorImpl.java:547/597).
///
/// This witness DOCUMENTS the shipped (post-fix) Noxu behaviour so the delta
/// review can state precisely what deviates.  GREEN asserts the current
/// behaviour; if a later change moves the release to the actual reposition,
/// flip the expected value to 1.
#[test]
fn concern2_rmw_then_plain_read_retains_prior_write_lock() {
    let db = mk_db();
    let lm = Arc::new(LockManager::with_lock_timeout(50));
    let tm = Arc::new(TxnManager::new(lm.clone()));
    let mut cursor = CursorImpl::new(db, 7)
        .with_lock_manager(lm.clone())
        .with_txn_manager(tm);

    cursor.search(b"A", None, SearchMode::Set).unwrap();
    cursor.upgrade_current_to_write_lock().unwrap();
    assert_eq!(lm.n_total_locks(), 1);

    // Plain (non-RMW) read of B: read-lock on B acquired+released per-op; no
    // RMW upgrade => the move-fix does NOT fire.
    cursor.search(b"B", None, SearchMode::Set).unwrap();
    let _ = cursor.get_current().unwrap();

    let total = lm.n_total_locks();
    assert_eq!(
        total, 0,
        "DELTA-REVIEW concern #2 (v2 JE-faithful): after RMW(A) then a plain \
         read of B the bare cursor holds {total} lock(s); the release now \
         fires at the MOVE (search/getNext), so A's write lock is dropped at \
         the reposition -- a bare cursor holds AT MOST the lock on its CURRENT \
         record (B read-locked+released per op => 0).  Matches JE \
         beforeNonStickyOp / reset (CursorImpl.java:547/597)."
    );
    cursor.close().unwrap();
    assert_eq!(
        lm.n_total_locks(),
        0,
        "nothing left; the move already released A"
    );
}

/// CONCERN #3 (getCurrent must NOT release): a plain getCurrent after an RMW
/// read does not move the cursor and must NOT release A's lock
/// (Cursor.java:3070 getCurrentNoDups -> beginUseExistingCursor, no release).
#[test]
fn concern3_getcurrent_after_rmw_does_not_release() {
    let db = mk_db();
    let lm = Arc::new(LockManager::with_lock_timeout(50));
    let tm = Arc::new(TxnManager::new(lm.clone()));
    let mut cursor = CursorImpl::new(db, 7)
        .with_lock_manager(lm.clone())
        .with_txn_manager(tm);

    cursor.search(b"A", None, SearchMode::Set).unwrap();
    cursor.upgrade_current_to_write_lock().unwrap();
    assert_eq!(lm.n_total_locks(), 1);

    let _ = cursor.get_current().unwrap();
    assert_eq!(
        lm.n_total_locks(),
        1,
        "getCurrent does not move the cursor and must retain A's write lock \
         (JE beginUseExistingCursor, no releaseNonTxnLocks)"
    );
    cursor.close().unwrap();
    assert_eq!(lm.n_total_locks(), 0);
}

/// CONCERN #3 (attached-txn must NOT release on move): an RMW read under a Txn
/// must retain its locks across a subsequent RMW move (they belong to the
/// Txn, not the cursor).  The move-fix is a no-op when txn_ref.is_some().
#[test]
fn concern3_attached_txn_rmw_retains_locks_across_move() {
    let db = mk_db();
    let lm = Arc::new(LockManager::with_lock_timeout(50));
    let tm = Arc::new(TxnManager::new(lm.clone()));
    let mut cursor = CursorImpl::new(db, 0)
        .with_lock_manager(lm.clone())
        .with_txn_manager(tm.clone());
    // Give the txn a disjoint id band from the cursor id, as the leak test does.
    tm.set_last_txn_id(cursor.get_id() + 100);
    let txn = Arc::new(Mutex::new(tm.begin_txn()));
    cursor.attach_txn(txn.clone());

    cursor.search(b"A", None, SearchMode::Set).unwrap();
    cursor.upgrade_current_to_write_lock().unwrap();
    let after_a = lm.n_total_locks();
    assert!(after_a >= 1, "txn RMW on A holds >=1 lock");

    // Move to B and RMW: the txn RETAINS A's lock (serializable txn footprint).
    cursor.search(b"B", None, SearchMode::Set).unwrap();
    cursor.upgrade_current_to_write_lock().unwrap();
    let after_b = lm.n_total_locks();
    assert!(
        after_b >= after_a,
        "attached-txn RMW must RETAIN A's lock across a move (the move-fix is a \
         no-op under a Txn); after A={after_a}, after B={after_b}"
    );
    // Cursor close must NOT release the txn's locks.
    cursor.close().unwrap();
    assert_eq!(
        lm.n_total_locks(),
        after_b,
        "cursor close must not release a Txn's locks (they live to commit/abort)"
    );
    // Clean up the txn so TxnManager is left tidy.
    let mut t = txn.lock().unwrap();
    t.abort().unwrap();
    tm.abort_txn(t.id_as_locker());
}
