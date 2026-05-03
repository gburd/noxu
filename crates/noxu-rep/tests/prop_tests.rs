//! Property-based tests for noxu-rep using proptest.

use noxu_rep::elections::Proposal;
use noxu_rep::vlsn::VlsnRange;
use proptest::prelude::*;

proptest! {
    // 1. Proposal ordering: proposals with higher VLSN always win
    //    (regardless of other fields).
    #[test]
    fn prop_higher_vlsn_wins(
        vlsn_a in 0u64..u64::MAX,
        vlsn_b in 0u64..u64::MAX,
        prio_a: u32,
        prio_b: u32,
        term_a: u64,
        term_b: u64,
        name_a in "[a-z]{1,8}",
        name_b in "[a-z]{1,8}",
    ) {
        prop_assume!(vlsn_a != vlsn_b);

        let pa = Proposal::with_timestamp(name_a, vlsn_a, prio_a, term_a, 0);
        let pb = Proposal::with_timestamp(name_b, vlsn_b, prio_b, term_b, 0);

        if vlsn_a > vlsn_b {
            prop_assert!(pa.is_better_than(&pb));
        } else {
            prop_assert!(pb.is_better_than(&pa));
        }
    }

    // Additional: Proposal ordering is total and antisymmetric.
    #[test]
    fn prop_proposal_ordering_antisymmetric(
        vlsn_a: u64,
        vlsn_b: u64,
        prio_a: u32,
        prio_b: u32,
        term_a: u64,
        term_b: u64,
        name_a in "[a-z]{1,8}",
        name_b in "[a-z]{1,8}",
    ) {
        let pa = Proposal::with_timestamp(name_a, vlsn_a, prio_a, term_a, 0);
        let pb = Proposal::with_timestamp(name_b, vlsn_b, prio_b, term_b, 0);

        // At most one can be "better" (antisymmetric), or they are equal.
        let a_better = pa.is_better_than(&pb);
        let b_better = pb.is_better_than(&pa);
        prop_assert!(!(a_better && b_better), "Both cannot be better than each other");
    }

    // 2. VlsnRange: first <= last always holds after extend operations.
    #[test]
    fn prop_vlsn_range_first_le_last(
        vlsns in prop::collection::vec(1u64..10000u64, 1..50),
    ) {
        let mut range = VlsnRange::new();
        for v in &vlsns {
            range.extend(*v);
        }

        // After extending, the range should not be empty.
        prop_assert!(!range.is_empty());
        // first <= last must always hold.
        prop_assert!(range.first() <= range.last());
        // first should be the min of all extended values.
        let expected_first = *vlsns.iter().min().unwrap();
        let expected_last = *vlsns.iter().max().unwrap();
        prop_assert_eq!(range.first(), expected_first);
        prop_assert_eq!(range.last(), expected_last);
    }

    // Additional: VlsnRange with_range always satisfies first <= last.
    #[test]
    fn prop_vlsn_range_with_range_valid(first in 1u64..10000u64, delta in 0u64..10000u64) {
        let last = first.saturating_add(delta);
        let range = VlsnRange::with_range(first, last);
        prop_assert!(range.first() <= range.last());
        prop_assert_eq!(range.len(), last - first + 1);
    }

    // Additional: VlsnRange contains is correct after extend.
    #[test]
    fn prop_vlsn_range_contains(
        vlsns in prop::collection::vec(1u64..10000u64, 1..20),
    ) {
        let mut range = VlsnRange::new();
        for v in &vlsns {
            range.extend(*v);
        }

        // Every extended value should be contained.
        for v in &vlsns {
            prop_assert!(range.contains(*v));
        }
    }

    // Additional: VlsnRange merge preserves first <= last.
    #[test]
    fn prop_vlsn_range_merge_valid(
        first_a in 1u64..5000u64,
        delta_a in 0u64..5000u64,
        first_b in 1u64..5000u64,
        delta_b in 0u64..5000u64,
    ) {
        let last_a = first_a.saturating_add(delta_a);
        let last_b = first_b.saturating_add(delta_b);
        let mut range_a = VlsnRange::with_range(first_a, last_a);
        let range_b = VlsnRange::with_range(first_b, last_b);

        range_a.merge(&range_b);

        prop_assert!(range_a.first() <= range_a.last());
        prop_assert!(range_a.first() <= first_a.min(first_b));
        prop_assert!(range_a.last() >= last_a.max(last_b));
    }
}
