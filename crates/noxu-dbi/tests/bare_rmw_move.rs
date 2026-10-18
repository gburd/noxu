// Copyright (C) 2024-2026 Greg Burd.  Apache-2.0 OR MIT.
//! BUG-BARE-RMW-LOCK-LEAK release-on-MOVE witness (JE LockMode.java:84-86).
//!
//! JE: for a non-transactional cursor "both read and write locks are only held
//! while a cursor is positioned on the record, and are released when the cursor
//! is moved to a different record or closed" (LockMode.java class javadoc).
//! The mechanism is `CursorImpl.reset()` / close-of-old-cursorImpl on
//! `endMoveCursor` (Cursor.java:5177/5228), both of which call
//! `releaseNonTxnLocks` (CursorImpl.java:602/619).  So a bare JE cursor holds
//! AT MOST the lock on its CURRENT record.
//!
//! This witness: a bare cursor RMW-reads key A (takes a WRITE lock on A), then
//! RMW-reads key B.  Per JE, moving off A must release A's write lock, so the
//! manager holds exactly ONE lock (on B).  If Noxu only releases on close (not
//! on move), the manager holds TWO -- a write lock accumulated on A across the
//! scan: a leak / liveness bug that blocks later writers of A for the whole
//! scan, not just a stale-after-close window.
#![cfg(not(noxu_shuttle))]

use noxu_dbi::{
    CursorImpl, DatabaseConfig, DatabaseId, DatabaseImpl, DbType, SearchMode,
};
use noxu_sync::RwLock;
use noxu_txn::{LockManager, TxnManager};
use noxu_util::Lsn;
use std::sync::Arc;

#[test]
fn bare_rmw_releases_prior_record_lock_on_move() {
    let db = Arc::new(RwLock::new(DatabaseImpl::new(
        DatabaseId::new(1),
        "rmw-move".into(),
        DbType::User,
        &DatabaseConfig::default(),
    )));
    {
        let tree = db.read();
        let t = tree.get_real_tree().expect("tree");
        t.insert(b"A".to_vec(), b"va".to_vec(), Lsn::new(1, 100)).unwrap();
        t.insert(b"B".to_vec(), b"vb".to_vec(), Lsn::new(1, 200)).unwrap();
    }
    let lm = Arc::new(LockManager::with_lock_timeout(50));
    let tm = Arc::new(TxnManager::new(lm.clone()));
    let mut cursor = CursorImpl::new(db, 0)
        .with_lock_manager(lm.clone())
        .with_txn_manager(tm);

    // Position on A and RMW-upgrade -> WRITE lock on A.
    cursor.search(b"A", None, SearchMode::Set).unwrap();
    cursor.upgrade_current_to_write_lock().unwrap();
    assert_eq!(lm.n_total_locks(), 1, "one write lock after RMW on A");

    // Move to B and RMW-upgrade -> WRITE lock on B.
    cursor.search(b"B", None, SearchMode::Set).unwrap();
    cursor.upgrade_current_to_write_lock().unwrap();

    // JE: a non-txn cursor holds AT MOST the lock on its current record.
    // Moving off A should have released A's write lock, leaving exactly one.
    let total = lm.n_total_locks();
    assert_eq!(
        total, 1,
        "JE: a bare cursor holds AT MOST the lock on its CURRENT record \
         (LockMode.java javadoc / CursorImpl.reset); after RMW(A) then RMW(B) \
         the manager holds {total} locks -- A's write lock accumulated across \
         the move (release-on-move missing)"
    );

    cursor.close().unwrap();
    drop(cursor);
    assert_eq!(lm.n_total_locks(), 0, "all released on close");
}
