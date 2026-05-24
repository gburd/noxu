//! TxnTest — lifecycle and isolation tests ported to Rust.
//!
//! Covers: transaction lifecycle (begin/commit/abort), state transitions,
//! lock acquisition via Locker trait, durability variants, isolation flags,
//! note_log_entry / has_logged_entries, pre/post commit hooks, undo records,
//! TxnManager stats, cursor registration, importunate flag.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use noxu_txn::{
    Durability, LockManager, LockType, Locker, TxnError, TxnManager, TxnState,
};
use noxu_util::lsn::NULL_LSN;

// ─── helpers ──────────────────────────────────────────────────────────────────

fn lm() -> Arc<LockManager> {
    Arc::new(LockManager::new())
}

// ─── 1. Lifecycle: commit ─────────────────────────────────────────────────────

#[test]
fn txn_initial_state_is_open() {
    let lm = lm();
    let txn = noxu_txn::Txn::new(1, lm);
    assert_eq!(txn.get_state(), TxnState::Open);
}

#[test]
fn txn_commit_transitions_to_committed() {
    let lm = lm();
    let mut txn = noxu_txn::Txn::new(1, lm);
    txn.commit().unwrap();
    assert_eq!(txn.get_state(), TxnState::Committed);
}

#[test]
fn txn_abort_transitions_to_aborted() {
    let lm = lm();
    let mut txn = noxu_txn::Txn::new(1, lm);
    txn.abort().unwrap();
    assert_eq!(txn.get_state(), TxnState::Aborted);
}

#[test]
fn txn_commit_returns_null_lsn_without_log_manager() {
    let lm = lm();
    let mut txn = noxu_txn::Txn::new(1, lm);
    let lsn = txn.commit().unwrap();
    assert_eq!(lsn, NULL_LSN);
}

#[test]
fn txn_abort_returns_null_lsn_without_log_manager() {
    let lm = lm();
    let mut txn = noxu_txn::Txn::new(2, lm);
    let lsn = txn.abort().unwrap();
    assert_eq!(lsn, NULL_LSN);
}

// ─── 2. Double-commit / double-abort guard ────────────────────────────────────

#[test]
fn txn_double_commit_returns_error() {
    let lm = lm();
    let mut txn = noxu_txn::Txn::new(1, lm);
    txn.commit().unwrap();
    let result = txn.commit();
    assert!(result.is_err());
}

#[test]
fn txn_abort_then_abort_is_idempotent() {
    let lm = lm();
    let mut txn = noxu_txn::Txn::new(1, lm);
    txn.abort().unwrap();
    // Second abort should succeed (idempotent).
    txn.abort().unwrap();
    assert_eq!(txn.get_state(), TxnState::Aborted);
}

#[test]
fn txn_commit_after_abort_returns_error() {
    let lm = lm();
    let mut txn = noxu_txn::Txn::new(1, lm);
    txn.abort().unwrap();
    let result = txn.commit();
    assert!(result.is_err());
}

// ─── 3. MustAbort state ───────────────────────────────────────────────────────

#[test]
fn set_only_abortable_blocks_commit() {
    let lm = lm();
    let mut txn = noxu_txn::Txn::new(1, lm);
    txn.set_only_abortable();
    assert_eq!(txn.get_state(), TxnState::MustAbort);
    let result = txn.commit();
    assert!(
        matches!(result, Err(TxnError::InvalidTransaction { .. })),
        "expected InvalidTransaction, got {result:?}"
    );
}

#[test]
fn set_only_abortable_allows_abort() {
    let lm = lm();
    let mut txn = noxu_txn::Txn::new(1, lm);
    txn.set_only_abortable();
    // abort() should succeed even in MustAbort state.
    txn.abort().unwrap();
    assert_eq!(txn.get_state(), TxnState::Aborted);
}

// ─── 4. has_logged_entries / note_log_entry ───────────────────────────────────

#[test]
fn has_logged_entries_false_initially() {
    let lm = lm();
    let txn = noxu_txn::Txn::new(1, lm);
    assert!(!txn.has_logged_entries());
}

#[test]
fn note_log_entry_sets_has_logged_entries() {
    let lm = lm();
    let mut txn = noxu_txn::Txn::new(1, lm);
    txn.note_log_entry(42);
    assert!(txn.has_logged_entries());
    assert_eq!(txn.first_lsn(), 42);
    assert_eq!(txn.last_lsn(), 42);
}

#[test]
fn note_log_entry_updates_last_lsn_only() {
    let lm = lm();
    let mut txn = noxu_txn::Txn::new(1, lm);
    txn.note_log_entry(10);
    txn.note_log_entry(20);
    assert_eq!(
        txn.first_lsn(),
        10,
        "first_lsn must not change after second note"
    );
    assert_eq!(txn.last_lsn(), 20);
}

// ─── 5. Lock acquisition via Locker trait ─────────────────────────────────────

#[test]
fn txn_locker_id_matches_new_id() {
    let lm = lm();
    let txn = noxu_txn::Txn::new(77, lm);
    assert_eq!(txn.id(), 77);
}

#[test]
fn txn_acquire_read_lock_increments_count() {
    let lm = lm();
    let mut txn = noxu_txn::Txn::new(1, lm);
    txn.lock(100, LockType::Read, false).unwrap();
    assert_eq!(txn.n_read_locks(), 1);
    assert_eq!(txn.n_write_locks(), 0);
}

#[test]
fn txn_acquire_write_lock_increments_write_count() {
    let lm = lm();
    let mut txn = noxu_txn::Txn::new(1, lm);
    txn.lock(100, LockType::Write, false).unwrap();
    assert_eq!(txn.n_write_locks(), 1);
    assert_eq!(txn.n_read_locks(), 0);
}

#[test]
fn txn_promote_read_to_write_removes_from_read_set() {
    let lm = lm();
    let mut txn = noxu_txn::Txn::new(1, lm);
    txn.lock(100, LockType::Read, false).unwrap();
    txn.lock(100, LockType::Write, false).unwrap();
    assert_eq!(txn.n_read_locks(), 0, "promotion should remove from read set");
    assert_eq!(txn.n_write_locks(), 1);
}

#[test]
fn txn_n_locks_totals_both_sets() {
    let lm = lm();
    let mut txn = noxu_txn::Txn::new(1, lm);
    txn.lock(1, LockType::Read, false).unwrap();
    txn.lock(2, LockType::Write, false).unwrap();
    assert_eq!(txn.n_locks(), 2);
}

#[test]
fn txn_commit_releases_all_locks() {
    let lm = lm();
    let lm2 = Arc::clone(&lm);
    let mut txn = noxu_txn::Txn::new(1, lm);
    txn.lock(5, LockType::Write, false).unwrap();
    assert_eq!(lm2.n_total_locks(), 1);
    txn.commit().unwrap();
    assert_eq!(lm2.n_total_locks(), 0);
}

#[test]
fn txn_abort_releases_all_locks() {
    let lm = lm();
    let lm2 = Arc::clone(&lm);
    let mut txn = noxu_txn::Txn::new(1, lm);
    txn.lock(5, LockType::Read, false).unwrap();
    txn.lock(6, LockType::Write, false).unwrap();
    assert_eq!(lm2.n_total_locks(), 2);
    txn.abort().unwrap();
    assert_eq!(lm2.n_total_locks(), 0);
}

// ─── 6. Demote write → read ───────────────────────────────────────────────────

#[test]
fn demote_lock_moves_write_to_read() {
    let lm = lm();
    let mut txn = noxu_txn::Txn::new(1, lm);
    txn.lock(10, LockType::Write, false).unwrap();
    txn.demote_lock(10).unwrap();
    assert_eq!(txn.n_write_locks(), 0);
    assert_eq!(txn.n_read_locks(), 1);
}

// ─── 7. Isolation flags ───────────────────────────────────────────────────────

#[test]
fn serializable_isolation_defaults_false() {
    let lm = lm();
    let txn = noxu_txn::Txn::new(1, lm);
    assert!(!txn.is_serializable_isolation());
}

#[test]
fn set_serializable_isolation_true() {
    let lm = lm();
    let mut txn = noxu_txn::Txn::new(1, lm);
    txn.set_serializable_isolation(true);
    assert!(txn.is_serializable_isolation());
}

#[test]
fn read_committed_isolation_defaults_false() {
    let lm = lm();
    let txn = noxu_txn::Txn::new(1, lm);
    assert!(!txn.is_read_committed_isolation());
}

#[test]
fn set_read_committed_isolation_true() {
    let lm = lm();
    let mut txn = noxu_txn::Txn::new(1, lm);
    txn.set_read_committed_isolation(true);
    assert!(txn.is_read_committed_isolation());
}

// ─── 8. Importunate flag ─────────────────────────────────────────────────────

#[test]
fn importunate_defaults_false() {
    let lm = lm();
    let txn = noxu_txn::Txn::new(1, lm);
    assert!(!txn.get_importunate());
}

#[test]
fn set_importunate_true() {
    let lm = lm();
    let mut txn = noxu_txn::Txn::new(1, lm);
    txn.set_importunate(true);
    assert!(txn.get_importunate());
    assert!(txn.is_importunate());
}

// ─── 9. Pre/post commit hooks ─────────────────────────────────────────────────

#[test]
fn pre_commit_hook_fires_on_commit_with_logged_entry() {
    let lm = lm();
    let fired = Arc::new(AtomicBool::new(false));
    let fired_clone = Arc::clone(&fired);
    let mut txn = noxu_txn::Txn::new(1, lm);
    txn.note_log_entry(1); // make has_logged_entries() true
    txn.set_pre_commit_hook(move || {
        fired_clone.store(true, Ordering::Relaxed);
    });
    txn.commit().unwrap();
    assert!(fired.load(Ordering::Relaxed), "pre-commit hook should have fired");
}

#[test]
fn post_commit_hook_fires_on_commit_with_logged_entry() {
    let lm = lm();
    let fired = Arc::new(AtomicBool::new(false));
    let fired_clone = Arc::clone(&fired);
    let mut txn = noxu_txn::Txn::new(1, lm);
    txn.note_log_entry(1);
    txn.set_post_commit_hook(move |_lsn| {
        fired_clone.store(true, Ordering::Relaxed);
    });
    txn.commit().unwrap();
    assert!(
        fired.load(Ordering::Relaxed),
        "post-commit hook should have fired"
    );
}

#[test]
fn pre_commit_hook_does_not_fire_for_read_only_txn() {
    // A txn with no logged entries does not write TxnCommit, so hooks skip.
    let lm = lm();
    let fired = Arc::new(AtomicBool::new(false));
    let fired_clone = Arc::clone(&fired);
    let mut txn = noxu_txn::Txn::new(1, lm);
    // No note_log_entry call → read-only.
    txn.set_pre_commit_hook(move || {
        fired_clone.store(true, Ordering::Relaxed);
    });
    txn.commit().unwrap();
    assert!(
        !fired.load(Ordering::Relaxed),
        "hook must not fire for read-only txn"
    );
}

// ─── 10. Durability variants ──────────────────────────────────────────────────

#[test]
fn commit_sync_succeeds_without_log_manager() {
    let lm = lm();
    let mut txn = noxu_txn::Txn::new(1, lm);
    assert!(txn.commit_with_durability(Durability::CommitSync).is_ok());
}

#[test]
fn commit_write_no_sync_succeeds_without_log_manager() {
    let lm = lm();
    let mut txn = noxu_txn::Txn::new(1, lm);
    assert!(txn.commit_with_durability(Durability::CommitWriteNoSync).is_ok());
}

#[test]
fn commit_no_sync_succeeds_without_log_manager() {
    let lm = lm();
    let mut txn = noxu_txn::Txn::new(1, lm);
    assert!(txn.commit_with_durability(Durability::CommitNoSync).is_ok());
}

// ─── 11. Cursor registration guard ────────────────────────────────────────────

#[test]
fn commit_fails_when_cursors_open() {
    let lm = lm();
    let mut txn = noxu_txn::Txn::new(1, lm);
    txn.register_cursor();
    let result = txn.commit();
    assert!(
        matches!(result, Err(TxnError::InvalidTransaction { .. })),
        "commit with open cursor must fail"
    );
    txn.unregister_cursor();
    txn.commit().unwrap();
}

#[test]
fn cursor_count_tracks_register_unregister() {
    let lm = lm();
    let txn = noxu_txn::Txn::new(1, lm);
    assert_eq!(txn.cursor_count(), 0);
    txn.register_cursor();
    txn.register_cursor();
    assert_eq!(txn.cursor_count(), 2);
    txn.unregister_cursor();
    assert_eq!(txn.cursor_count(), 1);
}

// ─── 12. Undo records ─────────────────────────────────────────────────────────

#[test]
fn abort_collect_undo_empty_for_read_only_txn() {
    let lm = lm();
    let mut txn = noxu_txn::Txn::new(1, lm);
    let records = txn.abort_collect_undo().unwrap();
    assert!(records.is_empty());
}

#[test]
fn set_write_lock_abort_info_populates_undo_on_abort() {
    let lm = lm();
    let mut txn = noxu_txn::Txn::new(1, lm);
    // Acquire write lock first.
    txn.lock(100, LockType::Write, false).unwrap();
    // Set abort info for that lock (simulates a new insert: abort_known_deleted=true).
    txn.set_write_lock_abort_info(
        100, // current lsn
        NULL_LSN.as_u64(),
        None,
        None,
        true, // abort_known_deleted
        1,    // database_id
    );
    txn.abort().unwrap();
    let records = txn.take_undo_records();
    assert_eq!(records.len(), 1);
    assert!(records[0].abort_known_deleted);
    assert_eq!(records[0].database_id, 1);
}

// ─── 13. WriteLockInfo: move_write_lock_to_new_lsn ───────────────────────────

#[test]
fn move_write_lock_migrates_lock_to_new_lsn() {
    let lm = lm();
    let lm2 = Arc::clone(&lm);
    let mut txn = noxu_txn::Txn::new(1, lm);
    txn.lock(50, LockType::Write, false).unwrap();
    txn.move_write_lock_to_new_lsn(50, 99).unwrap();
    // Old lsn should no longer be locked; new lsn should be.
    assert!(lm2.is_owned_write_lock(99, 1), "new lsn should be write-locked");
    assert!(!lm2.is_owned_write_lock(50, 1), "old lsn should be released");
    txn.commit().unwrap();
}

#[test]
fn move_write_lock_no_op_when_no_old_lock() {
    // Calling on an LSN this txn does not hold a write lock for is
    // a no-op and returns Ok — preserves the pre-Result behaviour
    // for callers that don't track which LSNs have moved.
    let lm = lm();
    let mut txn = noxu_txn::Txn::new(1, lm);
    let r = txn.move_write_lock_to_new_lsn(7777, 8888);
    assert!(r.is_ok());
    txn.commit().unwrap();
}

// ─── 14. TxnManager lifecycle ─────────────────────────────────────────────────

#[test]
fn txn_manager_n_active_zero_initially() {
    let lm = lm();
    let mgr = TxnManager::new(lm);
    assert_eq!(mgr.n_active_txns(), 0);
}

#[test]
fn txn_manager_begin_increments_active() {
    let lm = lm();
    let mgr = TxnManager::new(lm);
    let _t1 = mgr.begin_txn();
    let _t2 = mgr.begin_txn();
    assert_eq!(mgr.n_active_txns(), 2);
}

#[test]
fn txn_manager_commit_decrements_active() {
    let lm = lm();
    let mgr = TxnManager::new(lm);
    let t = mgr.begin_txn();
    let id = t.id();
    drop(t);
    mgr.commit_txn(id);
    assert_eq!(mgr.n_active_txns(), 0);
}

#[test]
fn txn_manager_stats_track_begins_commits_aborts() {
    let lm = lm();
    let mgr = TxnManager::new(lm);
    let t1 = mgr.begin_txn();
    let t2 = mgr.begin_txn();
    let id1 = t1.id();
    let id2 = t2.id();
    drop(t1);
    drop(t2);
    mgr.commit_txn(id1);
    mgr.abort_txn(id2);
    let stats = mgr.get_stats();
    assert_eq!(stats.n_begins, 2);
    assert_eq!(stats.n_commits, 1);
    assert_eq!(stats.n_aborts, 1);
    assert_eq!(stats.n_active, 0);
}

#[test]
fn txn_manager_serializable_tracking() {
    let lm = lm();
    let mgr = TxnManager::new(lm);
    assert!(!mgr.are_other_serializable_transactions_active());
    mgr.register_serializable();
    assert!(mgr.are_other_serializable_transactions_active());
    mgr.unregister_serializable();
    assert!(!mgr.are_other_serializable_transactions_active());
}

#[test]
fn txn_manager_set_last_txn_id_advances_next() {
    let lm = lm();
    let mgr = TxnManager::new(lm);
    mgr.set_last_txn_id(1000);
    assert_eq!(mgr.get_last_local_txn_id(), 1000);
    // Next txn should have id > 1000.
    let t = mgr.begin_txn();
    assert!(t.id() > 1000);
}
