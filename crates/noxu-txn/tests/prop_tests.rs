//! Property-based tests for noxu-txn using Hegel / hegeltest.

use hashbrown::{HashMap, HashSet};
use hegel::generators;

use noxu_txn::{DeadlockDetector, LockConflict, LockType, LockUpgrade};

/// Generator producing one of the 5 main lock types used in the
/// conflict/upgrade matrices.
#[hegel::composite]
fn lock_type(tc: hegel::TestCase) -> LockType {
    tc.draw(hegel::one_of!(
        generators::just(LockType::Read),
        generators::just(LockType::Write),
        generators::just(LockType::RangeRead),
        generators::just(LockType::RangeWrite),
        generators::just(LockType::RangeInsert),
    ))
}

/// Generator for the 4 non-insert lock types (the symmetric part of the
/// matrix).
#[hegel::composite]
fn symmetric_lock_type(tc: hegel::TestCase) -> LockType {
    tc.draw(hegel::one_of!(
        generators::just(LockType::Read),
        generators::just(LockType::Write),
        generators::just(LockType::RangeRead),
        generators::just(LockType::RangeWrite),
    ))
}

/// Generator for the 2 write lock types.
#[hegel::composite]
fn write_lock_type(tc: hegel::TestCase) -> LockType {
    tc.draw(hegel::one_of!(
        generators::just(LockType::Write),
        generators::just(LockType::RangeWrite),
    ))
}

// ============================================================================
// 1. Lock conflict matrix symmetry for compatible types
//    For the 5 main lock types, conflict(a, b) and conflict(b, a) should
//    share the same "blocking" quality when the matrix is symmetric.
//    Note: The conflict matrix is NOT fully symmetric (RangeInsert row
//    differs from the RangeInsert column), so we test a weaker property:
//    Allow is symmetric among Read/Write/RangeRead/RangeWrite.
// ============================================================================

#[hegel::test]
fn lock_conflict_symmetry_among_non_insert_types(tc: hegel::TestCase) {
    let a = tc.draw(symmetric_lock_type());
    let b = tc.draw(symmetric_lock_type());
    let ab = a.get_conflict(b);
    let ba = b.get_conflict(a);
    // Among Read/Write/RangeRead/RangeWrite the conflict matrix is symmetric
    assert_eq!(
        ab, ba,
        "conflict({a:?}, {b:?}) = {ab:?} but conflict({b:?}, {a:?}) = {ba:?}"
    );
}

// ============================================================================
// 2. Lock upgrade: upgrade(a, a) == Existing (upgrading to same type is identity)
// ============================================================================

#[hegel::test]
fn lock_upgrade_identity(tc: hegel::TestCase) {
    let a = tc.draw(lock_type());
    let upgrade = a.get_upgrade(a);
    // For the 5 main types, upgrading a lock to the same type should yield Existing
    assert_eq!(
        upgrade,
        LockUpgrade::Existing,
        "get_upgrade({a:?}, {a:?}) = {upgrade:?}, expected Existing"
    );
}

// ============================================================================
// 3. Lock type write check: Write and RangeWrite are write locks, others are not
// ============================================================================

#[hegel::test]
fn lock_write_classification(tc: hegel::TestCase) {
    let lt = tc.draw(lock_type());
    let is_write = lt.is_write_lock();
    match lt {
        LockType::Write | LockType::RangeWrite => {
            assert!(is_write, "{lt:?} should be a write lock");
        }
        _ => {
            assert!(!is_write, "{lt:?} should NOT be a write lock");
        }
    }
}

// ============================================================================
// 4. Deadlock: chain of N locks with no cycle -> no deadlock detected
//    Build a linear chain: T1 waits for T2, T2 waits for T3, ..., T(n-1) waits for Tn.
//    Then check that T(n+1) requesting a lock held by T1 has no deadlock
//    (since T(n+1) is not in the chain).
// ============================================================================

#[hegel::test]
fn no_deadlock_in_linear_chain(tc: hegel::TestCase) {
    let chain_len = tc.draw(generators::integers::<u32>().min_value(2).max_value(19));
    let mut waits_for: HashMap<i64, HashSet<i64>> = HashMap::new();

    // Build chain: 1 -> 2 -> 3 -> ... -> chain_len
    for i in 1..chain_len as i64 {
        waits_for.insert(i, HashSet::from([i + 1]));
    }

    // A new transaction (chain_len + 1) requests a lock held by T1.
    // Since chain_len+1 is NOT in the chain, there should be no deadlock.
    let requester = chain_len as i64 + 1;
    let result = DeadlockDetector::detect(requester, &[1], &waits_for);
    assert!(
        result.is_none(),
        "Expected no deadlock for requester {requester} but got cycle: {result:?}"
    );
}

// ============================================================================
// 5. Deadlock: ring of N locks -> deadlock detected
//    Build a ring: T1 waits for T2, T2 waits for T3, ..., T(n-1) waits for Tn.
//    Then Tn requests a lock held by T1 -> cycle detected.
// ============================================================================

#[hegel::test]
fn deadlock_in_ring(tc: hegel::TestCase) {
    let ring_size = tc.draw(generators::integers::<u32>().min_value(2).max_value(19));
    let mut waits_for: HashMap<i64, HashSet<i64>> = HashMap::new();

    // Build chain: 1 -> 2 -> 3 -> ... -> (ring_size - 1) -> ring_size
    for i in 1..ring_size as i64 {
        waits_for.insert(i, HashSet::from([i + 1]));
    }

    // ring_size requests a lock held by T1 -> completes the ring
    let requester = ring_size as i64;
    let result = DeadlockDetector::detect(requester, &[1], &waits_for);
    assert!(
        result.is_some(),
        "Expected deadlock for ring of size {ring_size} but none detected"
    );

    // Verify the cycle includes the requester
    let cycle = result.unwrap();
    assert_eq!(
        cycle[0], requester,
        "Cycle should start with requester {requester}, got {cycle:?}"
    );
    assert_eq!(
        *cycle.last().unwrap(),
        requester,
        "Cycle should end with requester {requester}, got {cycle:?}"
    );
    // The cycle length should be ring_size + 1 (requester appears at start and end)
    assert_eq!(
        cycle.len(),
        ring_size as usize + 1,
        "Cycle length should be {} but got {} for cycle {cycle:?}",
        ring_size + 1,
        cycle.len()
    );
}

// ============================================================================
// Additional property: conflict matrix has no Allow for Write-vs-Write
// ============================================================================

#[hegel::test]
fn write_locks_always_conflict_with_each_other(tc: hegel::TestCase) {
    let a = tc.draw(write_lock_type());
    let b = tc.draw(write_lock_type());
    let conflict = a.get_conflict(b);
    assert_eq!(
        conflict,
        LockConflict::Block,
        "conflict({a:?}, {b:?}) should be Block but got {conflict:?}"
    );
}
