//! LockManagerTest — stress tests ported to Rust.
//!
//! Covers: non-blocking acquire, lock demote, lock release,
//! n_total_locks, get_lock_info, get_stats, sharing registry,
//! stealing, lock timeout, concurrent grant/wait.

use std::sync::Arc;
use std::thread;
use std::time::Duration;

use noxu_txn::{LockGrantType, LockManager, LockStats, LockType, TxnError};

// ─── helpers ──────────────────────────────────────────────────────────────────

fn lm() -> Arc<LockManager> {
    Arc::new(LockManager::with_lock_timeout(500))
}

#[allow(dead_code)]
fn lm_no_timeout() -> Arc<LockManager> {
    Arc::new(LockManager::with_lock_timeout(0))
}

fn acquire(lm: &LockManager, lsn: u64, locker: i64, ty: LockType) -> LockGrantType {
    lm.lock(lsn, locker, ty, false, false).unwrap()
}

fn acquire_nb(lm: &LockManager, lsn: u64, locker: i64, ty: LockType) -> Result<LockGrantType, TxnError> {
    lm.lock(lsn, locker, ty, true, false)
}

// ─── 1. Basic grant types ─────────────────────────────────────────────────────

#[test]
fn lock_new_grant_type() {
    let lm = lm();
    assert_eq!(acquire(&lm, 1, 10, LockType::Read), LockGrantType::New);
}

#[test]
fn lock_existing_same_locker_same_type() {
    let lm = lm();
    acquire(&lm, 1, 10, LockType::Read);
    assert_eq!(acquire(&lm, 1, 10, LockType::Read), LockGrantType::Existing);
}

#[test]
fn lock_promotion_read_to_write_solo() {
    let lm = lm();
    acquire(&lm, 1, 10, LockType::Read);
    assert_eq!(acquire(&lm, 1, 10, LockType::Write), LockGrantType::Promotion);
}

#[test]
fn two_readers_both_get_new() {
    let lm = lm();
    assert_eq!(acquire(&lm, 1, 10, LockType::Read), LockGrantType::New);
    assert_eq!(acquire(&lm, 1, 20, LockType::Read), LockGrantType::New);
}

#[test]
fn none_type_returns_none_needed() {
    let lm = lm();
    let r = lm.lock(1, 10, LockType::None, false, false).unwrap();
    assert_eq!(r, LockGrantType::NoneNeeded);
}

// ─── 2. Non-blocking acquire ──────────────────────────────────────────────────

#[test]
fn non_blocking_fails_when_write_held() {
    let lm = lm();
    acquire(&lm, 1, 10, LockType::Write);
    let r = acquire_nb(&lm, 1, 20, LockType::Read);
    assert!(r.is_err(), "should fail non-blocking when write held");
    assert!(matches!(r.unwrap_err(), TxnError::LockNotAvailable { .. }));
}

#[test]
fn non_blocking_succeeds_for_reader_when_no_conflict() {
    let lm = lm();
    acquire(&lm, 1, 10, LockType::Read);
    let r = acquire_nb(&lm, 1, 20, LockType::Read);
    assert_eq!(r.unwrap(), LockGrantType::New);
}

#[test]
fn non_blocking_fails_when_read_held_by_other_and_write_requested() {
    let lm = lm();
    acquire(&lm, 1, 10, LockType::Read);
    let r = acquire_nb(&lm, 1, 20, LockType::Write);
    assert!(matches!(r.unwrap_err(), TxnError::LockNotAvailable { .. }));
}

// ─── 3. Release and n_total_locks ────────────────────────────────────────────

#[test]
fn release_reduces_lock_count() {
    let lm = lm();
    acquire(&lm, 1, 10, LockType::Read);
    acquire(&lm, 2, 10, LockType::Read);
    assert!(lm.n_total_locks() >= 2);
    lm.release(1, 10).unwrap();
    let after = lm.n_total_locks();
    assert!(after < 2, "n_total_locks should decrease after release");
}

#[test]
fn release_unknown_lock_is_ok() {
    let lm = lm();
    // Releasing a lock that was never acquired should not panic.
    let _ = lm.release(999, 10);
}

#[test]
fn n_total_locks_zero_initially() {
    let lm = lm();
    assert_eq!(lm.n_total_locks(), 0);
}

#[test]
fn multiple_locks_accumulate() {
    let lm = lm();
    for i in 1u64..=5 {
        acquire(&lm, i, 10, LockType::Read);
    }
    assert_eq!(lm.n_total_locks(), 5);
}

// ─── 4. Demote ────────────────────────────────────────────────────────────────

#[test]
fn demote_write_to_read_succeeds() {
    let lm = lm();
    acquire(&lm, 1, 10, LockType::Write);
    assert!(lm.demote(1, 10).is_ok());
}

#[test]
fn is_owned_write_lock_false_after_demote() {
    let lm = lm();
    acquire(&lm, 1, 10, LockType::Write);
    lm.demote(1, 10).unwrap();
    assert!(!lm.is_owned_write_lock(1, 10));
}

// ─── 5. Lock stealing ────────────────────────────────────────────────────────

#[test]
fn steal_lock_transfers_ownership() {
    let lm = lm();
    acquire(&lm, 1, 10, LockType::Write);
    // Locker 20 steals the lock from locker 10 (importunate preemption).
    lm.steal_lock(1, 20).unwrap();
    // After preemption, locker 10 no longer holds a write lock.
    assert!(!lm.is_owned_write_lock(1, 10));
}

// ─── 6. get_lock_info (owners, waiters) ──────────────────────────────────────

#[test]
fn get_lock_info_shows_owner() {
    let lm = lm();
    acquire(&lm, 1, 10, LockType::Read);
    let (owners, waiters) = lm.get_lock_info(1);
    assert_eq!(owners, 1);
    assert_eq!(waiters, 0);
}

#[test]
fn get_lock_info_empty_lsn() {
    let lm = lm();
    let (owners, waiters) = lm.get_lock_info(999);
    assert_eq!(owners, 0);
    assert_eq!(waiters, 0);
}

#[test]
fn get_lock_info_two_readers() {
    let lm = lm();
    acquire(&lm, 5, 10, LockType::Read);
    acquire(&lm, 5, 20, LockType::Read);
    let (owners, _waiters) = lm.get_lock_info(5);
    assert_eq!(owners, 2);
}

// ─── 7. get_stats() ──────────────────────────────────────────────────────────

#[test]
fn stats_request_count_increments() {
    let lm = lm();
    acquire(&lm, 1, 10, LockType::Read);
    acquire(&lm, 2, 10, LockType::Read);
    let stats: LockStats = lm.get_stats();
    assert!(stats.lock_requests >= 2);
}

#[test]
fn stats_write_lock_counted() {
    let lm = lm();
    acquire(&lm, 1, 10, LockType::Write);
    let stats = lm.get_stats();
    assert!(stats.lock_requests >= 1);
    assert!(stats.n_total_locks >= 1);
}

#[test]
fn stats_read_lock_counted() {
    let lm = lm();
    acquire(&lm, 1, 10, LockType::Read);
    let stats = lm.get_stats();
    assert!(stats.lock_requests >= 1);
    assert!(stats.n_total_locks >= 1);
}

// ─── 8. Sharing registry ─────────────────────────────────────────────────────

#[test]
fn registered_lockers_are_in_same_group() {
    let lm = lm();
    lm.register_locker_sharing(100, 42);
    lm.register_locker_sharing(200, 42);
    assert!(lm.same_share_group(100, 200));
}

#[test]
fn unregistered_lockers_not_in_same_group() {
    let lm = lm();
    lm.register_locker_sharing(100, 42);
    assert!(!lm.same_share_group(100, 999));
}

#[test]
fn unregister_removes_from_group() {
    let lm = lm();
    lm.register_locker_sharing(100, 42);
    lm.register_locker_sharing(200, 42);
    lm.unregister_locker_sharing(100);
    assert!(!lm.same_share_group(100, 200));
}

// ─── 9. Lock timeout ─────────────────────────────────────────────────────────

#[test]
fn lock_times_out_when_blocked() {
    let lm = Arc::new(LockManager::with_lock_timeout(50)); // 50 ms timeout
    // Locker 10 holds a write lock.
    acquire(&lm, 1, 10, LockType::Write);

    // Locker 20 should time out trying to read.
    let lm2 = Arc::clone(&lm);
    let result = thread::spawn(move || {
        lm2.lock_with_timeout(1, 20, LockType::Read, false, false, 50)
    })
    .join()
    .unwrap();

    assert!(
        matches!(result, Err(TxnError::LockTimeout { .. })),
        "expected LockTimeout, got {result:?}"
    );
}

#[test]
fn set_lock_timeout_takes_effect() {
    let lm = lm();
    lm.set_lock_timeout(200);
    assert_eq!(lm.get_lock_timeout_ms(), 200);
}

// ─── 10. Concurrent grant ────────────────────────────────────────────────────

#[test]
fn concurrent_readers_all_granted() {
    let lm = Arc::new(LockManager::new());
    let handles: Vec<_> = (0..8)
        .map(|i| {
            let lm = Arc::clone(&lm);
            thread::spawn(move || {
                lm.lock(42, i, LockType::Read, false, false).unwrap()
            })
        })
        .collect();
    for h in handles {
        let g = h.join().unwrap();
        assert!(matches!(g, LockGrantType::New | LockGrantType::Existing));
    }
}

#[test]
fn writer_blocked_then_released() {
    let lm = Arc::new(LockManager::new());
    let lm2 = Arc::clone(&lm);

    // Reader holds lock.
    acquire(&lm, 10, 1, LockType::Read);

    // Writer waits in background.
    let writer = thread::spawn(move || {
        lm2.lock_with_timeout(10, 2, LockType::Write, false, false, 2_000)
    });

    // Release the reader after a short delay.
    thread::sleep(Duration::from_millis(20));
    lm.release(10, 1).unwrap();

    let result = writer.join().unwrap();
    assert!(
        result.is_ok(),
        "writer should have been granted after reader released"
    );
}
