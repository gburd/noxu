// Copyright (C) 2024-2026 Greg Burd.  Apache-2.0 OR MIT.
//! DELTA-REVIEW concern #2 (placement) PUBLIC-API witness.
//!
//! JE releases a non-txn cursor's locks on EVERY move (beginMoveCursor ->
//! reset / beforeNonStickyOp, CursorImpl.java:547/597), before any positioning
//! op INCLUDING a plain non-RMW read.  So a bare JE cursor holds AT MOST the
//! lock on its CURRENT record (LockMode.java:84-86).
//!
//! Witness: a bare cursor RMW-reads A (WRITE lock on A), then does a plain
//! (non-RMW) getNext onto B.  Per JE, moving off A releases A's write lock, so
//! the manager must hold AT MOST one lock (on B's read, which itself releases
//! per-op => zero write locks on A).  If Noxu releases only at the next RMW
//! write-lock acquisition (not at the plain move), A's write lock survives the
//! plain getNext and the manager still shows A locked.
#![cfg(not(noxu_shuttle))]

use noxu_dbi::{
    CursorImpl, DatabaseConfig, DatabaseId, DatabaseImpl, DbType, GetMode,
    SearchMode,
};
use noxu_sync::RwLock;
use noxu_txn::{LockManager, TxnManager};
use noxu_util::Lsn;
use std::sync::Arc;

#[test]
fn concern2_rmw_then_plain_getnext_releases_prior_lock_je() {
    let db = Arc::new(RwLock::new(DatabaseImpl::new(
        DatabaseId::new(1),
        "rmw-c2".into(),
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
    let mut cursor = CursorImpl::new(db, 7)
        .with_lock_manager(lm.clone())
        .with_txn_manager(tm);

    // RMW-read A -> WRITE lock on A.
    cursor.search(b"A", None, SearchMode::Set).unwrap();
    cursor.upgrade_current_to_write_lock().unwrap();
    assert_eq!(lm.n_total_locks(), 1, "one write lock after RMW on A");

    // Plain (non-RMW) getNext -> B.  JE releases A's write lock at this move.
    cursor.retrieve_next(GetMode::Next).unwrap();

    // JE invariant: a bare cursor holds AT MOST the lock on its CURRENT record.
    // B was read with a per-op read lock (released), and A's write lock must be
    // gone => zero manager locks.
    let total = lm.n_total_locks();
    assert_eq!(
        total, 0,
        "JE: a bare cursor holds AT MOST the lock on its CURRENT record \
         (LockMode.java:84-86; CursorImpl.reset/beforeNonStickyOp release on \
         EVERY move).  After RMW(A) then a PLAIN getNext->B the manager holds \
         {total} lock(s) -- A's write lock survived a non-RMW move because the \
         release fires only at the next RMW write-lock acquisition, not at the \
         reposition"
    );

    cursor.close().unwrap();
    assert_eq!(lm.n_total_locks(), 0);
}
