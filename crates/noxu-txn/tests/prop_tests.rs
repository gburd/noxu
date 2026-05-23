//! Property-based tests for noxu-txn using proptest.

use hashbrown::{HashMap, HashSet};
use proptest::prelude::*;

use noxu_txn::{DeadlockDetector, LockConflict, LockType, LockUpgrade};

/// Strategy to produce one of the 5 main lock types used in the conflict/upgrade matrices.
fn lock_type_strategy() -> impl Strategy<Value = LockType> {
    prop_oneof![
        Just(LockType::Read),
        Just(LockType::Write),
        Just(LockType::RangeRead),
        Just(LockType::RangeWrite),
        Just(LockType::RangeInsert),
    ]
}

// ============================================================================
// 1. Lock conflict matrix symmetry for compatible types
//    For the 5 main lock types, conflict(a, b) and conflict(b, a) should
//    share the same "blocking" quality when the matrix is symmetric.
//    Note: The conflict matrix is NOT fully symmetric (RangeInsert row
//    differs from the RangeInsert column), so we test a weaker property:
//    Allow is symmetric among Read/Write/RangeRead/RangeWrite.
// ============================================================================

/// Strategy for the 4 non-insert lock types (the symmetric part of the matrix).
fn symmetric_lock_type_strategy() -> impl Strategy<Value = LockType> {
    prop_oneof![
        Just(LockType::Read),
        Just(LockType::Write),
        Just(LockType::RangeRead),
        Just(LockType::RangeWrite),
    ]
}

proptest! {
    #[test]
    fn lock_conflict_symmetry_among_non_insert_types(
        a in symmetric_lock_type_strategy(),
        b in symmetric_lock_type_strategy()
    ) {
        let ab = a.get_conflict(b);
        let ba = b.get_conflict(a);
        // Among Read/Write/RangeRead/RangeWrite the conflict matrix is symmetric
        prop_assert_eq!(
            ab, ba,
            "conflict({:?}, {:?}) = {:?} but conflict({:?}, {:?}) = {:?}",
            a, b, ab, b, a, ba
        );
    }
}

// ============================================================================
// 2. Lock upgrade: upgrade(a, a) == Existing (upgrading to same type is identity)
// ============================================================================

proptest! {
    #[test]
    fn lock_upgrade_identity(a in lock_type_strategy()) {
        let upgrade = a.get_upgrade(a);
        // For the 5 main types, upgrading a lock to the same type should yield Existing
        prop_assert_eq!(
            upgrade,
            LockUpgrade::Existing,
            "get_upgrade({:?}, {:?}) = {:?}, expected Existing",
            a, a, upgrade
        );
    }
}

// ============================================================================
// 3. Lock type write check: Write and RangeWrite are write locks, others are not
// ============================================================================

proptest! {
    #[test]
    fn lock_write_classification(lt in lock_type_strategy()) {
        let is_write = lt.is_write_lock();
        match lt {
            LockType::Write | LockType::RangeWrite => {
                prop_assert!(is_write, "{:?} should be a write lock", lt);
            }
            _ => {
                prop_assert!(!is_write, "{:?} should NOT be a write lock", lt);
            }
        }
    }
}

// ============================================================================
// 4. Deadlock: chain of N locks with no cycle -> no deadlock detected
//    Build a linear chain: T1 waits for T2, T2 waits for T3, ..., T(n-1) waits for Tn.
//    Then check that T(n+1) requesting a lock held by T1 has no deadlock
//    (since T(n+1) is not in the chain).
// ============================================================================

proptest! {
    #[test]
    fn no_deadlock_in_linear_chain(chain_len in 2u32..20) {
        let mut waits_for: HashMap<i64, HashSet<i64>> = HashMap::new();

        // Build chain: 1 -> 2 -> 3 -> ... -> chain_len
        for i in 1..chain_len as i64 {
            waits_for.insert(i, HashSet::from([i + 1]));
        }

        // A new transaction (chain_len + 1) requests a lock held by T1.
        // Since chain_len+1 is NOT in the chain, there should be no deadlock.
        let requester = chain_len as i64 + 1;
        let result = DeadlockDetector::detect(requester, &[1], &waits_for);
        prop_assert!(
            result.is_none(),
            "Expected no deadlock for requester {} but got cycle: {:?}",
            requester,
            result
        );
    }
}

// ============================================================================
// 5. Deadlock: ring of N locks -> deadlock detected
//    Build a ring: T1 waits for T2, T2 waits for T3, ..., T(n-1) waits for Tn.
//    Then Tn requests a lock held by T1 -> cycle detected.
// ============================================================================

proptest! {
    #[test]
    fn deadlock_in_ring(ring_size in 2u32..20) {
        let mut waits_for: HashMap<i64, HashSet<i64>> = HashMap::new();

        // Build chain: 1 -> 2 -> 3 -> ... -> (ring_size - 1) -> ring_size
        for i in 1..ring_size as i64 {
            waits_for.insert(i, HashSet::from([i + 1]));
        }

        // ring_size requests a lock held by T1 -> completes the ring
        let requester = ring_size as i64;
        let result = DeadlockDetector::detect(requester, &[1], &waits_for);
        prop_assert!(
            result.is_some(),
            "Expected deadlock for ring of size {} but none detected",
            ring_size
        );

        // Verify the cycle includes the requester
        let cycle = result.unwrap();
        prop_assert_eq!(
            cycle[0], requester,
            "Cycle should start with requester {}, got {:?}",
            requester, cycle
        );
        prop_assert_eq!(
            *cycle.last().unwrap(), requester,
            "Cycle should end with requester {}, got {:?}",
            requester, cycle
        );
        // The cycle length should be ring_size + 1 (requester appears at start and end)
        prop_assert_eq!(
            cycle.len(),
            ring_size as usize + 1,
            "Cycle length should be {} but got {} for cycle {:?}",
            ring_size + 1, cycle.len(), cycle
        );
    }
}

// ============================================================================
// Additional property: conflict matrix has no Allow for Write-vs-Write
// ============================================================================

proptest! {
    #[test]
    fn write_locks_always_conflict_with_each_other(
        a in prop_oneof![Just(LockType::Write), Just(LockType::RangeWrite)],
        b in prop_oneof![Just(LockType::Write), Just(LockType::RangeWrite)]
    ) {
        let conflict = a.get_conflict(b);
        prop_assert_eq!(
            conflict,
            LockConflict::Block,
            "conflict({:?}, {:?}) should be Block but got {:?}",
            a, b, conflict
        );
    }
}
