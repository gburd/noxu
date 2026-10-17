// Copyright (C) 2024-2026 Greg Burd.  Apache-2.0 OR MIT.
//! BUG-BARE-RMW-LOCK-LEAK witness (CursorImpl level).
//!
//! A non-transactional (bare) cursor that upgrades its current record to a
//! WRITE lock via an RMW read (`CursorImpl::upgrade_current_to_write_lock`)
//! must release that lock when it closes or is dropped -- exactly as a BDB-JE
//! non-txn cursor's locker releases its non-transactional locks on `close()`
//! (JE `CursorImpl.close` -> `nonTxnOperationEnd` -> `operationEnd` ->
//! `BasicLocker.releaseNonTxnLocks`, CursorImpl.java:602,
//! BasicLocker.java:198).  Before the fix the WRITE lock leaked in the
//! LockManager and blocked every later writer of that record until timeout
//! (liveness) and pinned the slot against the compressor / cleaner.
#![cfg(not(noxu_shuttle))]

use noxu_dbi::{
    CursorImpl, DatabaseConfig, DatabaseId, DatabaseImpl, DbType, SearchMode,
};
use noxu_sync::RwLock;
use noxu_txn::{LockManager, TxnManager};
use noxu_util::Lsn;
use std::sync::{Arc, Mutex};

fn lsn() -> Lsn {
    Lsn::new(1, 100)
}

fn fixture() -> (CursorImpl, Arc<TxnManager>, Arc<LockManager>) {
    let db = Arc::new(RwLock::new(DatabaseImpl::new(
        DatabaseId::new(1),
        "bare-rmw-leak".into(),
        DbType::User,
        &DatabaseConfig::default(),
    )));
    db.read()
        .get_real_tree()
        .expect("fixture tree")
        .insert(b"key".to_vec(), b"value".to_vec(), lsn())
        .unwrap();
    let lm = Arc::new(LockManager::with_lock_timeout(50));
    let tm = Arc::new(TxnManager::new(lm.clone()));
    let cursor = CursorImpl::new(db, 0)
        .with_lock_manager(lm.clone())
        .with_txn_manager(tm.clone());
    (cursor, tm, lm)
}

/// Shared check: a bare RMW cursor holds exactly one WRITE lock while
/// positioned, and that lock is gone from the manager after close/drop.
fn assert_rmw_cleanup(close: bool) {
    let (mut cursor, tm, lm) = fixture();
    cursor.search(b"key", None, SearchMode::Set).unwrap();
    cursor.upgrade_current_to_write_lock().unwrap();
    // RMW took a WRITE lock owned by the cursor's own id (no txn attached).
    assert_eq!(lm.get_lock_info(lsn().as_u64()), (1, 0));
    assert_eq!(lm.n_total_locks(), 1, "RMW must hold one manager lock");
    if close {
        cursor.close().unwrap();
        cursor.close().unwrap(); // idempotent second close
    }
    drop(cursor);
    assert_eq!(lm.n_total_locks(), 0, "bare RMW leaked after close={close}");
    assert_eq!(tm.n_active_txns(), 0);
}

#[test]
fn cursorimpl_bare_rmw_close_releases_manager_lock() {
    assert_rmw_cleanup(true);
}

#[test]
fn cursorimpl_bare_rmw_drop_releases_manager_lock() {
    assert_rmw_cleanup(false);
}

/// Control: when a transaction IS attached, the RMW WRITE lock belongs to the
/// Txn, not the cursor, and MUST survive cursor close until the txn ends.
#[test]
fn cursorimpl_attached_txn_rmw_survives_cursor_close() {
    let (mut cursor, tm, lm) = fixture();
    tm.set_last_txn_id(cursor.get_id() + 100);
    let txn = Arc::new(Mutex::new(tm.begin_txn()));
    cursor.attach_txn(txn.clone());
    cursor.search(b"key", None, SearchMode::Set).unwrap();
    cursor.upgrade_current_to_write_lock().unwrap();
    cursor.close().unwrap();
    drop(cursor);
    // Txn still owns the WRITE lock after the cursor is gone.
    assert_eq!(tm.n_active_txns(), 1);
    assert_eq!(lm.get_lock_info(lsn().as_u64()), (1, 0));
    let mut txn = txn.lock().unwrap();
    assert_eq!(txn.n_write_locks(), 1);
    txn.abort().unwrap();
    tm.abort_txn(txn.id_as_locker());
    assert_eq!(lm.n_total_locks(), 0);
    assert_eq!(tm.n_active_txns(), 0);
}
