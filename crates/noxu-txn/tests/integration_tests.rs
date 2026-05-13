//! Comprehensive integration tests for noxu-txn.
//!
//! Covers:
//!   - Lock conflict matrix: all 25 (held, requested) pairs
//!   - Lock upgrade matrix: all 25 (held, requested) pairs
//!   - Lock upgrade via LockManager: READ->WRITE promotion
//!   - Deadlock detection: 2-locker cycle, 3-locker cycle, no-cycle
//!   - Deadlock victim selection: fewest-locks wins, tie-break by smaller ID
//!   - ThinLock->FullLock mutation when a second locker arrives
//!   - Txn lifecycle: begin, acquire write lock, commit, abort
//!   - WriteLockInfo: create, clone, restore
//!   - Txn::n_locks and set_only_abortable

use hashbrown::{HashMap, HashSet};
use std::sync::Arc;

use noxu_txn::{
    DeadlockDetector, LockConflict, LockGrantType, LockImpl, LockManager,
    LockType, LockUpgrade, Locker, Txn, WriteLockInfo,
};

// ============================================================================
// 1. Lock conflict matrix  --  all 25 (held, requested) pairs
// ============================================================================

#[test]
fn conflict_matrix_read_row() {
    use LockConflict::*;
    use LockType::*;
    assert_eq!(Read.get_conflict(Read), Allow);
    assert_eq!(Read.get_conflict(Write), Block);
    assert_eq!(Read.get_conflict(RangeRead), Allow);
    assert_eq!(Read.get_conflict(RangeWrite), Block);
    assert_eq!(Read.get_conflict(RangeInsert), Allow);
}

#[test]
fn conflict_matrix_write_row() {
    use LockConflict::*;
    use LockType::*;
    assert_eq!(Write.get_conflict(Read), Block);
    assert_eq!(Write.get_conflict(Write), Block);
    assert_eq!(Write.get_conflict(RangeRead), Block);
    assert_eq!(Write.get_conflict(RangeWrite), Block);
    assert_eq!(Write.get_conflict(RangeInsert), Allow);
}

#[test]
fn conflict_matrix_range_read_row() {
    use LockConflict::*;
    use LockType::*;
    assert_eq!(RangeRead.get_conflict(Read), Allow);
    assert_eq!(RangeRead.get_conflict(Write), Block);
    assert_eq!(RangeRead.get_conflict(RangeRead), Allow);
    assert_eq!(RangeRead.get_conflict(RangeWrite), Block);
    assert_eq!(RangeRead.get_conflict(RangeInsert), Block);
}

#[test]
fn conflict_matrix_range_write_row() {
    use LockConflict::*;
    use LockType::*;
    assert_eq!(RangeWrite.get_conflict(Read), Block);
    assert_eq!(RangeWrite.get_conflict(Write), Block);
    assert_eq!(RangeWrite.get_conflict(RangeRead), Block);
    assert_eq!(RangeWrite.get_conflict(RangeWrite), Block);
    assert_eq!(RangeWrite.get_conflict(RangeInsert), Block);
}

#[test]
fn conflict_matrix_range_insert_row() {
    use LockConflict::{Allow, Restart};
    use LockType::*;
    assert_eq!(RangeInsert.get_conflict(Read), Allow);
    assert_eq!(RangeInsert.get_conflict(Write), Allow);
    assert_eq!(RangeInsert.get_conflict(RangeRead), Restart);
    assert_eq!(RangeInsert.get_conflict(RangeWrite), Restart);
    assert_eq!(RangeInsert.get_conflict(RangeInsert), Allow);
}

// ============================================================================
// 2. Lock upgrade matrix  --  all 25 (held, requested) pairs
// ============================================================================

#[test]
fn upgrade_matrix_read_row() {
    use LockType::*;
    use LockUpgrade::*;
    assert_eq!(Read.get_upgrade(Read), Existing);
    assert_eq!(Read.get_upgrade(Write), WritePromote);
    assert_eq!(Read.get_upgrade(RangeRead), RangeReadImmed);
    assert_eq!(Read.get_upgrade(RangeWrite), RangeWritePromote);
    assert_eq!(Read.get_upgrade(RangeInsert), Illegal);
}

#[test]
fn upgrade_matrix_write_row() {
    use LockType::*;
    use LockUpgrade::*;
    assert_eq!(Write.get_upgrade(Read), Existing);
    assert_eq!(Write.get_upgrade(Write), Existing);
    assert_eq!(Write.get_upgrade(RangeRead), RangeWriteImmed);
    assert_eq!(Write.get_upgrade(RangeWrite), RangeWriteImmed);
    assert_eq!(Write.get_upgrade(RangeInsert), Illegal);
}

#[test]
fn upgrade_matrix_range_read_row() {
    use LockType::*;
    use LockUpgrade::*;
    assert_eq!(RangeRead.get_upgrade(Read), Existing);
    assert_eq!(RangeRead.get_upgrade(Write), RangeWritePromote);
    assert_eq!(RangeRead.get_upgrade(RangeRead), Existing);
    assert_eq!(RangeRead.get_upgrade(RangeWrite), RangeWritePromote);
    assert_eq!(RangeRead.get_upgrade(RangeInsert), Illegal);
}

#[test]
fn upgrade_matrix_range_write_row() {
    use LockType::*;
    use LockUpgrade::*;
    assert_eq!(RangeWrite.get_upgrade(Read), Existing);
    assert_eq!(RangeWrite.get_upgrade(Write), Existing);
    assert_eq!(RangeWrite.get_upgrade(RangeRead), Existing);
    assert_eq!(RangeWrite.get_upgrade(RangeWrite), Existing);
    assert_eq!(RangeWrite.get_upgrade(RangeInsert), Illegal);
}

#[test]
fn upgrade_matrix_range_insert_row() {
    use LockType::*;
    use LockUpgrade::*;
    assert_eq!(RangeInsert.get_upgrade(Read), Illegal);
    assert_eq!(RangeInsert.get_upgrade(Write), Illegal);
    assert_eq!(RangeInsert.get_upgrade(RangeRead), Illegal);
    assert_eq!(RangeInsert.get_upgrade(RangeWrite), Illegal);
    assert_eq!(RangeInsert.get_upgrade(RangeInsert), Existing);
}

// ============================================================================
// 3. Lock upgrade via LockManager: READ->WRITE promotion
// ============================================================================

#[test]
fn lock_manager_read_to_write_promotion() {
    let lm = LockManager::new();
    let lsn = 1_000u64;
    let locker = 42i64;

    // Acquire READ lock.
    let g1 = lm.lock(lsn, locker, LockType::Read, false, false).unwrap();
    assert_eq!(g1, LockGrantType::New);
    assert_eq!(lm.get_owned_lock_type(lsn, locker), Some(LockType::Read));

    // Upgrade to WRITE on the same LSN with the same locker: must be Promotion.
    let g2 = lm.lock(lsn, locker, LockType::Write, false, false).unwrap();
    assert_eq!(g2, LockGrantType::Promotion);
    assert_eq!(lm.get_owned_lock_type(lsn, locker), Some(LockType::Write));
    assert!(lm.is_owned_write_lock(lsn, locker));
}

#[test]
fn lock_manager_read_to_range_read_immed() {
    let lm = LockManager::new();
    let lsn = 2_000u64;
    let locker = 1i64;

    lm.lock(lsn, locker, LockType::Read, false, false).unwrap();
    // READ -> RANGE_READ is an immediate upgrade (no promotion wait needed).
    let g = lm.lock(lsn, locker, LockType::RangeRead, false, false).unwrap();
    // Existing is returned for immediate upgrades that do not require a Promotion
    // grant type. The lock is held at the upgraded type.
    assert!(
        g == LockGrantType::Existing || g == LockGrantType::Promotion,
        "unexpected grant: {:?}",
        g
    );
    assert_eq!(lm.get_owned_lock_type(lsn, locker), Some(LockType::RangeRead));
}

#[test]
fn lock_manager_write_blocks_read_other_locker() {
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    let lm = Arc::new(LockManager::new());
    let lsn = 3_000u64;

    // Locker 1 holds WRITE.
    lm.lock(lsn, 1, LockType::Write, false, false).unwrap();

    // Locker 2 tries READ in a separate thread so the test is not blocked.
    // With blocking semantics the call must block until locker 1 releases.
    let lm2 = Arc::clone(&lm);
    let h = thread::spawn(move || {
        lm2.lock_with_timeout(lsn, 2, LockType::Read, false, false, 3000)
    });

    // Allow locker 2 to register as a waiter.
    thread::sleep(Duration::from_millis(50));

    // Verify the lock now has 1 owner and 1 waiter.
    let (owners, waiters) = lm.get_lock_info(lsn);
    assert_eq!(owners, 1, "writer is the only owner");
    assert_eq!(waiters, 1, "reader is queued as waiter");

    // Locker 1 releases; locker 2 should be granted.
    lm.release(lsn, 1).unwrap();

    let g = h.join().unwrap().unwrap();
    assert_eq!(g, LockGrantType::New, "reader granted after writer releases");
}

#[test]
fn lock_manager_two_readers_coexist() {
    let lm = LockManager::new();
    let lsn = 4_000u64;

    let g1 = lm.lock(lsn, 1, LockType::Read, false, false).unwrap();
    let g2 = lm.lock(lsn, 2, LockType::Read, false, false).unwrap();
    assert_eq!(g1, LockGrantType::New);
    assert_eq!(g2, LockGrantType::New);
    let (owners, waiters) = lm.get_lock_info(lsn);
    assert_eq!(owners, 2);
    assert_eq!(waiters, 0);
}

// ============================================================================
// 4. Deadlock detection: 2-locker cycle, 3-locker cycle, no-cycle case
// ============================================================================

#[test]
fn deadlock_two_locker_cycle_detected() {
    // T1 waits for T2, T2 requests lock held by T1 -> deadlock.
    let mut waits_for: HashMap<i64, HashSet<i64>> = HashMap::new();
    waits_for.insert(1, HashSet::from([2]));

    let cycle = DeadlockDetector::detect(2, &[1], &waits_for);
    assert!(cycle.is_some(), "should detect two-locker deadlock");
    let c = cycle.unwrap();
    assert_eq!(c[0], 2);
    assert_eq!(*c.last().unwrap(), 2);
}

#[test]
fn deadlock_three_locker_cycle_detected() {
    // T1->T2->T3->T1 ring.
    let mut waits_for: HashMap<i64, HashSet<i64>> = HashMap::new();
    waits_for.insert(1, HashSet::from([2]));
    waits_for.insert(2, HashSet::from([3]));

    let cycle = DeadlockDetector::detect(3, &[1], &waits_for);
    assert!(cycle.is_some(), "should detect three-locker deadlock");
    let c = cycle.unwrap();
    assert_eq!(c[0], 3);
    assert_eq!(*c.last().unwrap(), 3);
    assert_eq!(c.len(), 4); // 3, 1, 2, 3
}

#[test]
fn deadlock_no_cycle_linear_chain() {
    // T1->T2->T3 (no cycle). New requester T4 requests lock held by T1.
    let mut waits_for: HashMap<i64, HashSet<i64>> = HashMap::new();
    waits_for.insert(1, HashSet::from([2]));
    waits_for.insert(2, HashSet::from([3]));

    let cycle = DeadlockDetector::detect(4, &[1], &waits_for);
    assert!(cycle.is_none(), "should not detect deadlock in linear chain");
}

#[test]
fn deadlock_self_lock_no_cycle() {
    let waits_for: HashMap<i64, HashSet<i64>> = HashMap::new();
    // Requester requests lock it already owns.
    let cycle = DeadlockDetector::detect(1, &[1], &waits_for);
    assert!(cycle.is_none(), "self-lock should not be a deadlock");
}

// ============================================================================
// 5. Deadlock victim selection: fewest locks wins, tie-break by smaller ID
// ============================================================================

#[test]
fn select_victim_fewest_locks() {
    // Cycle: [10, 20, 30] where 20 holds fewest locks.
    let cycle = vec![10i64, 20, 30, 10];
    let mut lock_counts = HashMap::new();
    lock_counts.insert(10, 5usize);
    lock_counts.insert(20, 1usize);
    lock_counts.insert(30, 3usize);

    let victim = DeadlockDetector::select_victim(&cycle, &lock_counts);
    assert_eq!(victim, 20, "locker with fewest locks should be victim");
}

#[test]
fn select_victim_tie_break_by_larger_id() {
    // Cycle: [10, 20] both hold 2 locks.
    // tie-break: youngest transaction = LARGEST locker ID wins.
    // Locker IDs are assigned sequentially; the highest ID is the most
    // recently created ("youngest") transaction.  Aborting the youngest
    // wastes the least accumulated work in the system.
    // `LockManager.selectVictim()`.
    let cycle = vec![10i64, 20, 10];
    let mut lock_counts = HashMap::new();
    lock_counts.insert(10, 2usize);
    lock_counts.insert(20, 2usize);

    let victim = DeadlockDetector::select_victim(&cycle, &lock_counts);
    assert_eq!(victim, 20, "tie broken by largest locker ID (youngest transaction)");
}

#[test]
fn select_victim_missing_count_treated_as_zero() {
    // Locker 5 has no entry in lock_counts; treated as 0.
    let cycle = vec![5i64, 10, 5];
    let mut lock_counts = HashMap::new();
    lock_counts.insert(10, 3usize);
    // 5 not inserted -> count = 0

    let victim = DeadlockDetector::select_victim(&cycle, &lock_counts);
    assert_eq!(victim, 5, "locker with implicit 0 locks should be victim");
}

#[test]
fn select_victim_single_locker_in_cycle() {
    let cycle = vec![99i64, 99];
    let lock_counts = HashMap::new();
    let victim = DeadlockDetector::select_victim(&cycle, &lock_counts);
    assert_eq!(victim, 99);
}

// ============================================================================
// 6. ThinLock -> FullLock mutation: second locker forces mutation
// ============================================================================

#[test]
fn thin_to_full_mutation_on_second_locker() {
    let lm = LockManager::new();
    let lsn = 5_000u64;

    // First locker: starts as thin lock.
    lm.lock(lsn, 1, LockType::Read, false, false).unwrap();
    // The lock table entry is thin; we can confirm indirectly via lock info.
    let (owners1, _) = lm.get_lock_info(lsn);
    assert_eq!(owners1, 1);

    // Second locker: thin must mutate to full to support two owners.
    lm.lock(lsn, 2, LockType::Read, false, false).unwrap();
    let (owners2, _) = lm.get_lock_info(lsn);
    assert_eq!(owners2, 2, "full lock should hold two readers");
}

#[test]
fn thin_to_full_mutation_preserves_first_owner() {
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    let lm = Arc::new(LockManager::new());
    let lsn = 5_001u64;

    // Locker 10 holds WRITE; this causes the thin lock to hold one owner.
    lm.lock(lsn, 10, LockType::Write, false, false).unwrap();

    // Locker 20 requests READ in a separate thread: it must block because
    // of the conflicting write lock.  The thin lock mutates to a full lock
    // and 20 is queued as a waiter.
    let lm2 = Arc::clone(&lm);
    let h = thread::spawn(move || {
        lm2.lock_with_timeout(lsn, 20, LockType::Read, false, false, 3000)
    });

    // Allow locker 20 to register as a waiter.
    thread::sleep(Duration::from_millis(50));

    // While 10 still holds the write lock, verify the lock state.
    assert!(
        lm.is_owned_write_lock(lsn, 10),
        "original owner must still hold write lock"
    );
    let (owners, waiters) = lm.get_lock_info(lsn);
    assert_eq!(owners, 1, "only one owner (the writer)");
    assert_eq!(waiters, 1, "reader should be queued as waiter");

    // Release so the thread can finish.
    lm.release(lsn, 10).unwrap();
    h.join().unwrap().unwrap();
}

#[test]
fn lock_impl_thin_single_owner_new_grant() {
    let mut lock = LockImpl::new();
    let r = lock.lock(LockType::Read, 1, false, false);
    assert_eq!(r.grant_type, LockGrantType::New);
    assert_eq!(lock.n_owners(), 1);
    assert_eq!(lock.n_waiters(), 0);
}

#[test]
fn lock_impl_second_reader_new_grant() {
    let mut lock = LockImpl::new();
    lock.lock(LockType::Read, 1, false, false);
    let r = lock.lock(LockType::Read, 2, false, false);
    assert_eq!(r.grant_type, LockGrantType::New);
    assert_eq!(lock.n_owners(), 2);
}

#[test]
fn lock_impl_same_locker_existing() {
    let mut lock = LockImpl::new();
    lock.lock(LockType::Read, 1, false, false);
    let r = lock.lock(LockType::Read, 1, false, false);
    assert_eq!(r.grant_type, LockGrantType::Existing);
    assert_eq!(lock.n_owners(), 1);
}

#[test]
fn lock_impl_read_to_write_single_owner_promotion() {
    let mut lock = LockImpl::new();
    lock.lock(LockType::Read, 1, false, false);
    let r = lock.lock(LockType::Write, 1, false, false);
    assert_eq!(r.grant_type, LockGrantType::Promotion);
    assert!(lock.is_owned_write_lock(1));
    assert_eq!(lock.n_owners(), 1);
}

#[test]
fn lock_impl_read_to_write_with_conflicting_reader_wait_promotion() {
    let mut lock = LockImpl::new();
    lock.lock(LockType::Read, 1, false, false);
    lock.lock(LockType::Read, 2, false, false); // two readers
    // Locker 1 tries to upgrade to write: blocked by locker 2's read.
    let r = lock.lock(LockType::Write, 1, false, false);
    assert_eq!(r.grant_type, LockGrantType::WaitPromotion);
    assert_eq!(lock.n_waiters(), 1);
}

#[test]
fn lock_impl_write_blocks_reader_non_blocking_denied() {
    let mut lock = LockImpl::new();
    lock.lock(LockType::Write, 1, false, false);
    let r = lock.lock(LockType::Read, 2, true, false); // non-blocking
    assert_eq!(r.grant_type, LockGrantType::Denied);
}

#[test]
fn lock_impl_release_promotes_waiter() {
    let mut lock = LockImpl::new();
    lock.lock(LockType::Write, 1, false, false);
    lock.lock(LockType::Read, 2, false, false); // waits
    let notified = lock.release(1).unwrap();
    assert_eq!(notified, vec![2]);
    assert_eq!(lock.n_owners(), 1);
    assert_eq!(lock.n_waiters(), 0);
}

// ============================================================================
// 7. Txn lifecycle: begin, acquire write lock, commit, abort
// ============================================================================

fn make_txn(id: i64) -> Txn {
    let lm = Arc::new(LockManager::new());
    Txn::new(id, lm)
}

#[test]
fn txn_begin_state_is_open() {
    let txn = make_txn(1);
    assert!(txn.is_open());
}

#[test]
fn txn_acquire_write_lock_and_commit() {
    let mut txn = make_txn(1);
    let result = txn.lock(100, LockType::Write, false).unwrap();
    assert_eq!(result.grant, LockGrantType::New);
    assert!(result.write_lock_info.is_some());
    assert_eq!(txn.n_write_locks(), 1);
    assert_eq!(txn.n_locks(), 1);

    txn.commit().unwrap();
    assert_eq!(txn.n_write_locks(), 0);
    assert_eq!(txn.n_locks(), 0);
    assert!(!txn.is_open());
}

#[test]
fn txn_acquire_write_lock_and_abort() {
    let mut txn = make_txn(2);
    txn.lock(200, LockType::Write, false).unwrap();
    txn.lock(201, LockType::Read, false).unwrap();
    assert_eq!(txn.n_locks(), 2);

    txn.abort().unwrap();
    assert_eq!(txn.n_locks(), 0);
    assert!(!txn.is_open());
}

#[test]
fn txn_n_locks_counts_read_and_write() {
    let mut txn = make_txn(3);
    txn.lock(300, LockType::Write, false).unwrap();
    txn.lock(301, LockType::Read, false).unwrap();
    txn.lock(302, LockType::Write, false).unwrap();
    assert_eq!(txn.n_write_locks(), 2);
    assert_eq!(txn.n_read_locks(), 1);
    assert_eq!(txn.n_locks(), 3);
}

#[test]
fn txn_set_only_abortable_blocks_further_ops() {
    let mut txn = make_txn(4);
    txn.set_only_abortable();
    // MustAbort is still "valid" (Locker::is_open returns true for MustAbort),
    // but check_state() rejects operations while in MustAbort state.
    let err = txn.lock(400, LockType::Write, false);
    assert!(err.is_err(), "lock should fail in MustAbort state");
    // Can still abort.
    txn.abort().unwrap();
    assert!(!txn.is_open());
}

#[test]
fn txn_ops_fail_after_commit() {
    let mut txn = make_txn(5);
    txn.commit().unwrap();
    let err = txn.lock(500, LockType::Write, false);
    assert!(err.is_err());
}

#[test]
fn txn_ops_fail_after_abort() {
    let mut txn = make_txn(6);
    txn.abort().unwrap();
    let err = txn.lock(600, LockType::Write, false);
    assert!(err.is_err());
}

#[test]
fn txn_owns_write_lock_after_acquisition() {
    let mut txn = make_txn(7);
    txn.lock(700, LockType::Write, false).unwrap();
    assert!(txn.owns_write_lock(700));
    assert!(!txn.owns_write_lock(701));
}

#[test]
fn txn_close_aborts_open_transaction() {
    let mut txn = make_txn(8);
    txn.lock(800, LockType::Write, false).unwrap();
    txn.close();
    assert!(!txn.is_open());
    assert_eq!(txn.n_write_locks(), 0);
}

// ============================================================================
// 8. WriteLockInfo: create, clone, restore
// ============================================================================

#[test]
fn write_lock_info_new_has_null_abort_lsn() {
    let wli = WriteLockInfo::new();
    assert!(wli.is_null_abort_lsn());
    assert!(wli.never_locked);
    assert_eq!(wli.abort_vlsn, -1);
}

#[test]
fn write_lock_info_clone_is_independent() {
    let mut wli = WriteLockInfo::new();
    wli.abort_lsn = 9999;
    wli.abort_key = Some(vec![1, 2, 3]);

    let cloned = wli;
    assert_eq!(cloned.abort_lsn, 9999);
    assert_eq!(cloned.abort_key, Some(vec![1, 2, 3]));
}

#[test]
fn write_lock_info_copy_all_info_restores_state() {
    let mut src = WriteLockInfo::new();
    src.abort_lsn = 42;
    src.abort_known_deleted = true;
    src.abort_key = Some(b"key".to_vec());
    src.abort_data = Some(b"data".to_vec());
    src.abort_vlsn = 77;
    src.abort_log_size = 128;
    src.abort_expiration = 3600;
    src.abort_expiration_in_hours = true;
    src.never_locked = false;

    let mut dst = WriteLockInfo::new();
    dst.copy_all_info(&src);

    assert_eq!(dst.abort_lsn, 42);
    assert!(dst.abort_known_deleted);
    assert_eq!(dst.abort_key, Some(b"key".to_vec()));
    assert_eq!(dst.abort_data, Some(b"data".to_vec()));
    assert_eq!(dst.abort_vlsn, 77);
    assert_eq!(dst.abort_log_size, 128);
    assert_eq!(dst.abort_expiration, 3600);
    assert!(dst.abort_expiration_in_hours);
    assert!(!dst.never_locked);
}

#[test]
fn write_lock_info_set_abort_info() {
    let mut wli = WriteLockInfo::new();
    wli.set_abort_info(12345, Some(b"k".to_vec()), None, 10, 64, true, 0, false);
    assert_eq!(wli.abort_lsn, 12345);
    assert!(wli.abort_known_deleted);
    assert_eq!(wli.abort_key, Some(b"k".to_vec()));
    assert!(wli.abort_data.is_none());
    assert!(!wli.is_null_abort_lsn());
}

// ============================================================================
// 9. Lock upgrade through LockManager: additional upgrade paths
// ============================================================================

#[test]
fn lock_manager_write_upgrade_to_range_write_immed() {
    let lm = LockManager::new();
    let lsn = 6_000u64;
    let locker = 1i64;

    lm.lock(lsn, locker, LockType::Write, false, false).unwrap();
    // Write -> RangeWrite is an immediate upgrade (RangeWriteImmed).
    let g = lm.lock(lsn, locker, LockType::RangeWrite, false, false).unwrap();
    assert!(
        g == LockGrantType::Existing || g == LockGrantType::Promotion,
        "unexpected grant for Write->RangeWrite: {:?}",
        g
    );
    assert_eq!(lm.get_owned_lock_type(lsn, locker), Some(LockType::RangeWrite));
}

#[test]
fn lock_manager_range_write_covers_all_weaker_requests() {
    let lm = LockManager::new();
    let lsn = 7_000u64;
    let locker = 1i64;

    lm.lock(lsn, locker, LockType::RangeWrite, false, false).unwrap();
    for req in [LockType::Read, LockType::Write, LockType::RangeRead, LockType::RangeWrite] {
        let g = lm.lock(lsn, locker, req, false, false).unwrap();
        assert_eq!(
            g,
            LockGrantType::Existing,
            "RangeWrite should cover request {:?}",
            req
        );
    }
}

// ============================================================================
// 10. Multiple txns competing for the same lock
// ============================================================================

#[test]
fn two_txns_shared_lock_manager_read_write_conflict() {
    use std::thread;
    use std::time::Duration;

    let lm = Arc::new(LockManager::new());
    let mut t1 = Txn::new(1, lm.clone());

    // T1 acquires write lock.
    let r1 = t1.lock(1000, LockType::Write, false).unwrap();
    assert_eq!(r1.grant, LockGrantType::New);
    assert_eq!(lm.n_total_locks(), 1);

    // T2 tries read from a separate thread: it must block because T1 holds
    // a conflicting write lock.  With blocking semantics, the call will
    // wait until T1 releases the lock.
    let lm2 = lm.clone();
    let h = thread::spawn(move || {
        let mut t2 = Txn::new(2, lm2.clone());
        // Use a generous timeout so this does not flake.
        let r2 = lm2
            .lock_with_timeout(1000, 2, LockType::Read, false, false, 5000)
            .unwrap();
        assert_eq!(r2, LockGrantType::New, "T2 granted after T1 releases");
        // T2 must release the lock it holds.
        lm2.release(1000, 2).unwrap();
        t2.abort().unwrap();
    });

    // Allow T2 to register as a waiter.
    thread::sleep(Duration::from_millis(50));

    // The lock should have 1 owner (T1) and 1 waiter (T2).
    let (owners, waiters) = lm.get_lock_info(1000);
    assert_eq!(owners, 1, "T1 is the only owner");
    assert_eq!(waiters, 1, "T2 is queued as waiter");

    // T1 commits, releasing its write lock.  T2 wakes up and becomes owner.
    t1.commit().unwrap();

    // Wait for T2 thread to finish.
    h.join().unwrap();

    // After both transactions are done the lock table must be empty.
    assert_eq!(lm.n_total_locks(), 0, "lock table empty after both txns end");
}
