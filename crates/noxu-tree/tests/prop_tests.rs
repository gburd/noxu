//! Property-based tests for noxu-tree using proptest.

use proptest::prelude::*;
use std::cmp::Ordering;

use noxu_tree::in_node::{BIN_LEVEL, EXACT_MATCH, INSERT_SUCCESS, InNode};
use noxu_tree::key::{
    compare_keys, compare_unsigned_bytes, create_key_prefix,
    get_key_prefix_length,
};
use noxu_util::{Lsn, NULL_LSN};

// ============================================================================
// 1. Key comparison is consistent: compare(a,b) is opposite of compare(b,a)
// ============================================================================

proptest! {
    #[test]
    fn key_comparison_antisymmetry(a in prop::collection::vec(any::<u8>(), 0..64),
                                    b in prop::collection::vec(any::<u8>(), 0..64)) {
        let ab = compare_unsigned_bytes(&a, &b);
        let ba = compare_unsigned_bytes(&b, &a);
        prop_assert_eq!(ab, ba.reverse());
    }

    #[test]
    fn key_comparison_with_comparator_antisymmetry(
        a in prop::collection::vec(any::<u8>(), 0..64),
        b in prop::collection::vec(any::<u8>(), 0..64)
    ) {
        let ab = compare_keys(&a, &b, None);
        let ba = compare_keys(&b, &a, None);
        prop_assert_eq!(ab, ba.reverse());
    }
}

// ============================================================================
// 2. Key comparison is transitive: if a<b and b<c then a<c
// ============================================================================

proptest! {
    #[test]
    fn key_comparison_transitivity(
        a in prop::collection::vec(any::<u8>(), 0..32),
        b in prop::collection::vec(any::<u8>(), 0..32),
        c in prop::collection::vec(any::<u8>(), 0..32)
    ) {
        let ab = compare_unsigned_bytes(&a, &b);
        let bc = compare_unsigned_bytes(&b, &c);
        let ac = compare_unsigned_bytes(&a, &c);

        if ab == Ordering::Less && bc == Ordering::Less {
            prop_assert_eq!(ac, Ordering::Less);
        }
        if ab == Ordering::Greater && bc == Ordering::Greater {
            prop_assert_eq!(ac, Ordering::Greater);
        }
        if ab == Ordering::Equal && bc == Ordering::Equal {
            prop_assert_eq!(ac, Ordering::Equal);
        }
    }
}

// ============================================================================
// 3. Key prefix: the prefix of two keys is a prefix of both keys, and
//    the prefix length is consistent with create_key_prefix.
// ============================================================================

proptest! {
    #[test]
    fn key_prefix_is_prefix_of_both(
        a in prop::collection::vec(any::<u8>(), 0..64),
        b in prop::collection::vec(any::<u8>(), 0..64)
    ) {
        let prefix_len = get_key_prefix_length(&a, &b);

        // The prefix length must not exceed either key length
        prop_assert!(prefix_len <= a.len());
        prop_assert!(prefix_len <= b.len());

        // The first prefix_len bytes must be identical
        prop_assert_eq!(&a[..prefix_len], &b[..prefix_len]);

        // If prefix_len < min(a.len(), b.len()), the bytes at prefix_len must differ
        if prefix_len < a.len().min(b.len()) {
            prop_assert_ne!(a[prefix_len], b[prefix_len]);
        }
    }

    #[test]
    fn create_key_prefix_roundtrip(
        a in prop::collection::vec(any::<u8>(), 0..64),
        b in prop::collection::vec(any::<u8>(), 0..64)
    ) {
        let prefix_len = get_key_prefix_length(&a, &b);
        let prefix = create_key_prefix(&a, &b);

        match prefix {
            Some(p) => {
                prop_assert!(prefix_len > 0);
                prop_assert_eq!(p.len(), prefix_len);
                // The prefix must be a prefix of both keys
                prop_assert_eq!(&a[..prefix_len], p.as_slice());
                prop_assert_eq!(&b[..prefix_len], p.as_slice());
            }
            None => {
                prop_assert_eq!(prefix_len, 0);
            }
        }
    }

    #[test]
    fn key_prefix_of_identical_keys(a in prop::collection::vec(any::<u8>(), 0..64)) {
        let prefix_len = get_key_prefix_length(&a, &a);
        prop_assert_eq!(prefix_len, a.len());
    }
}

// ============================================================================
// 4. InNode insert then search finds key: insert N keys, all findable
// ============================================================================

/// Generate a set of distinct keys for insertion.
fn distinct_keys_strategy(
    max_count: usize,
) -> impl Strategy<Value = Vec<Vec<u8>>> {
    prop::collection::hash_set(
        prop::collection::vec(any::<u8>(), 1..32),
        1..=max_count,
    )
    .prop_map(|set| set.into_iter().collect::<Vec<_>>())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn in_node_insert_then_find(keys in distinct_keys_strategy(50)) {
        let max_entries = keys.len() + 10; // ensure capacity
        let mut node = InNode::new(1, BIN_LEVEL, max_entries);

        // Insert all keys
        for key in &keys {
            let result = node.insert_entry(
                key.clone(),
                NULL_LSN,
                0,
            );
            prop_assert!(result.is_ok(), "insert failed for key {:?}: {:?}", key, result);
            let idx = result.unwrap();
            prop_assert_ne!(idx & INSERT_SUCCESS, 0, "INSERT_SUCCESS flag not set");
        }

        prop_assert_eq!(node.n_entries(), keys.len());

        // Search for each key using exact match with indicate_if_duplicate
        for key in &keys {
            let idx = node.find_entry(key, true, true);
            prop_assert!(
                idx >= 0 && (idx & EXACT_MATCH) != 0,
                "key {:?} not found, find_entry returned {}",
                key,
                idx
            );
        }
    }

    #[test]
    fn in_node_maintains_sorted_order(keys in distinct_keys_strategy(50)) {
        let max_entries = keys.len() + 10;
        let mut node = InNode::new(1, BIN_LEVEL, max_entries);

        for key in &keys {
            node.insert_entry(key.clone(), NULL_LSN, 0).unwrap();
        }

        // Verify all keys are in sorted order
        for i in 1..node.n_entries() {
            let prev = node.get_key(i - 1);
            let curr = node.get_key(i);
            prop_assert!(
                prev < curr,
                "keys not sorted at index {}: {:?} >= {:?}",
                i, prev, curr
            );
        }
    }

    #[test]
    fn in_node_insert_duplicate_returns_existing_index(
        key in prop::collection::vec(any::<u8>(), 1..32)
    ) {
        let mut node = InNode::new(1, BIN_LEVEL, 128);
        let first = node.insert_entry(key.clone(), NULL_LSN, 0).unwrap();
        prop_assert_ne!(first & INSERT_SUCCESS, 0);

        let second = node.insert_entry(key, Lsn::from_u64(999), 0).unwrap();
        // Second insert of same key should NOT have INSERT_SUCCESS set
        prop_assert_eq!(second & INSERT_SUCCESS, 0, "duplicate insert should not set INSERT_SUCCESS");
    }

    #[test]
    fn in_node_serialization_roundtrip(keys in distinct_keys_strategy(20)) {
        let max_entries = keys.len() + 10;
        let mut node = InNode::new(42, BIN_LEVEL, max_entries);
        node.set_node_id(1234);
        node.set_identifier_key(b"id".to_vec());

        for key in &keys {
            node.insert_entry(key.clone(), NULL_LSN, 0).unwrap();
        }

        let mut buf = Vec::with_capacity(node.log_size());
        node.write_to_log(&mut buf);

        let restored = InNode::read_from_log(&buf, BIN_LEVEL).unwrap();
        prop_assert_eq!(restored.n_entries(), node.n_entries());
        prop_assert_eq!(restored.node_id(), 1234);

        for i in 0..node.n_entries() {
            prop_assert_eq!(restored.get_key(i), node.get_key(i));
        }
    }
}
