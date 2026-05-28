//! Property-based tests for noxu-tree using proptest.

use proptest::prelude::*;
use std::cmp::Ordering;

use noxu_tree::bin::Bin;
use noxu_tree::delta_info::DeltaInfo;
use noxu_tree::entry_states::{DIRTY_BIT, SlotState};
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

// ============================================================================
// 5. DeltaInfo encode/decode round-trip (Wave 11-E).
//
// For any DeltaInfo with arbitrary key, lsn, and state byte, write_to_log
// followed by read_from_log must reproduce the same DeltaInfo and consume
// exactly log_size() bytes.
// ============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    #[test]
    fn delta_info_roundtrip(
        key in prop::collection::vec(any::<u8>(), 0..256),
        lsn_raw: u64,
        state_byte: u8,
    ) {
        let lsn = Lsn::from_u64(lsn_raw);
        let state = SlotState::from_byte(state_byte);
        let original = DeltaInfo::new(key.clone(), lsn, state);

        let mut buf = Vec::new();
        original.write_to_log(&mut buf);

        prop_assert_eq!(buf.len(), original.log_size());

        let (decoded, consumed) =
            DeltaInfo::read_from_log(&buf).expect("decode must succeed");
        prop_assert_eq!(consumed, buf.len());
        prop_assert_eq!(decoded.key, key);
        prop_assert_eq!(decoded.lsn, lsn);
        prop_assert_eq!(decoded.state.as_byte(), state_byte);
    }

    /// Encoding is deterministic: encoding the same DeltaInfo twice produces
    /// the same bytes.  Catches accidental nondeterminism (HashMap iteration,
    /// uninitialized padding, etc.) in the encoder.
    #[test]
    fn delta_info_encode_deterministic(
        key in prop::collection::vec(any::<u8>(), 0..128),
        lsn_raw: u64,
        state_byte: u8,
    ) {
        let lsn = Lsn::from_u64(lsn_raw);
        let state = SlotState::from_byte(state_byte);
        let info = DeltaInfo::new(key, lsn, state);
        let mut buf1 = Vec::new();
        let mut buf2 = Vec::new();
        info.write_to_log(&mut buf1);
        info.write_to_log(&mut buf2);
        prop_assert_eq!(buf1, buf2);
    }

    /// Read-then-write reproduces the original byte sequence (reverse direction
    /// of the round-trip).  For any byte sequence that successfully decodes,
    /// re-encoding must produce a byte-identical prefix of the original.
    #[test]
    fn delta_info_read_then_write_idempotent(
        key in prop::collection::vec(any::<u8>(), 0..128),
        lsn_raw: u64,
        state_byte: u8,
    ) {
        let original = DeltaInfo::new(
            key,
            Lsn::from_u64(lsn_raw),
            SlotState::from_byte(state_byte),
        );
        let mut buf1 = Vec::new();
        original.write_to_log(&mut buf1);

        let (decoded, _) = DeltaInfo::read_from_log(&buf1).unwrap();
        let mut buf2 = Vec::new();
        decoded.write_to_log(&mut buf2);

        prop_assert_eq!(buf1, buf2);
    }
}

// ============================================================================
// 6. BIN-delta apply round-trip (Wave 11-E).
//
// Property: for any sequence of dirty-slot updates applied to a full BIN, the
//   sequence (full -> mutate_to_bin_delta -> mutate_to_full_bin) reconstitutes
//   a BIN whose visible (key, lsn, state) tuples equal the brute-force
//   "apply each update directly to the full BIN" oracle.
//
// Property: applying the empty delta to a full BIN is a no-op (any full BIN
//   with no dirty slots cannot mutate to a delta — `can_mutate_to_bin_delta`
//   returns false; the precondition is documented).
// ============================================================================

/// Generate a small set of distinct keys for BIN-delta tests.
fn bin_keys_strategy(max: usize) -> impl Strategy<Value = Vec<Vec<u8>>> {
    prop::collection::hash_set(
        prop::collection::vec(any::<u8>(), 1..16),
        1..=max,
    )
    .prop_map(|set| {
        let mut v: Vec<Vec<u8>> = set.into_iter().collect();
        v.sort(); // deterministic order so shrinking is stable
        v
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]

    /// Property: building a BIN-delta from a set of dirty (key, lsn, state)
    /// slots and merging it into a base BIN produces the same result as
    /// calling `apply_delta_slot` directly on the base for each entry.
    ///
    /// This is the core invariant `IN.applyDelta` /
    /// `mutate_to_full_bin` is supposed to preserve.
    #[test]
    fn bin_delta_full_roundtrip(
        keys in bin_keys_strategy(16),
        update_count in 1usize..6,
        update_seed in any::<u64>(),
    ) {
        let n = keys.len();
        prop_assume!(n >= 2);

        let max_entries = (n + 8).max(16);
        // Build base BIN.
        let mut base = Bin::new(1, max_entries);
        for (i, k) in keys.iter().enumerate() {
            base.insert_entry(k.clone(), Lsn::from_u64(100 + i as u64), 0, None)
                .unwrap();
        }

        // Pick `update_count` keys (with replacement) to update.  Use the seed
        // to derive an arbitrary mapping deterministically.
        let mut updates: Vec<(Vec<u8>, Lsn)> = Vec::new();
        let mut s = update_seed;
        for _ in 0..update_count {
            // Cheap LCG to derive an index.
            s = s.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            let idx = (s as usize) % n;
            let new_lsn = Lsn::from_u64(1_000_000 + s % 1_000_000);
            updates.push((keys[idx].clone(), new_lsn));
        }

        // Path A — oracle: apply each update to a clone via apply_delta_slot.
        let mut oracle = Bin::new(1, max_entries);
        for (i, k) in keys.iter().enumerate() {
            oracle.insert_entry(k.clone(), Lsn::from_u64(100 + i as u64), 0, None)
                .unwrap();
        }
        for (k, lsn) in &updates {
            oracle.apply_delta_slot(k.clone(), *lsn, DIRTY_BIT, None);
        }

        // Path B — under test: build a delta-shaped BIN containing exactly the
        // updates, mark it as a delta, and merge into a fresh base.
        let mut merge_target = Bin::new(1, max_entries);
        for (i, k) in keys.iter().enumerate() {
            merge_target.insert_entry(k.clone(), Lsn::from_u64(100 + i as u64), 0, None)
                .unwrap();
        }
        let mut delta = Bin::new(1, max_entries);
        // Deduplicate by key, keeping the *last* lsn for each key (matches the
        // semantics of multiple in-flight dirty writes to the same slot).
        let mut last: std::collections::BTreeMap<Vec<u8>, Lsn> =
            std::collections::BTreeMap::new();
        for (k, lsn) in &updates {
            last.insert(k.clone(), *lsn);
        }
        for (k, lsn) in &last {
            delta.insert_entry(k.clone(), *lsn, DIRTY_BIT, None).unwrap();
        }
        delta.set_bin_delta(true);
        delta.mutate_to_full_bin(&mut merge_target, false);
        prop_assert!(!delta.is_bin_delta(), "delta must become full BIN after merge");

        // Both paths must agree on the visible (key, lsn) mapping.
        prop_assert_eq!(
            delta.get_n_entries(),
            oracle.get_n_entries(),
            "entry counts disagree",
        );
        for k in &keys {
            let i_a = oracle.find_entry(k, false, true);
            let i_b = delta.find_entry(k, false, true);
            prop_assert!(i_a >= 0 && (i_a & EXACT_MATCH) != 0,
                "oracle missing key {:?}", k);
            prop_assert!(i_b >= 0 && (i_b & EXACT_MATCH) != 0,
                "delta-merged missing key {:?}", k);
            let lsn_a = oracle.get_lsn((i_a & 0xFFFF) as usize);
            let lsn_b = delta.get_lsn((i_b & 0xFFFF) as usize);
            prop_assert_eq!(lsn_a, lsn_b,
                "key {:?}: oracle lsn {:?} != merged lsn {:?}", k, lsn_a, lsn_b);
        }
    }

    /// Property: `apply_delta_slot` is idempotent for a fixed (key, lsn,
    /// state) tuple.  Applying the same delta twice produces the same BIN
    /// state as applying it once.
    #[test]
    fn bin_apply_delta_slot_idempotent(
        keys in bin_keys_strategy(8),
        update_key in prop::collection::vec(any::<u8>(), 1..16),
        new_lsn_raw in 1u64..1_000_000u64,
        state_byte: u8,
    ) {
        let mut bin = Bin::new(1, 64);
        for (i, k) in keys.iter().enumerate() {
            bin.insert_entry(k.clone(), Lsn::from_u64(100 + i as u64), 0, None)
                .unwrap();
        }
        let new_lsn = Lsn::from_u64(new_lsn_raw + 10_000_000);
        bin.apply_delta_slot(update_key.clone(), new_lsn, state_byte, None);
        let n_after_first = bin.get_n_entries();

        let idx1 = bin.find_entry(&update_key, false, true);
        prop_assert!(idx1 >= 0);
        let lsn1 = bin.get_lsn((idx1 & 0xFFFF) as usize);
        let state1 = bin.get_state((idx1 & 0xFFFF) as usize);

        bin.apply_delta_slot(update_key.clone(), new_lsn, state_byte, None);
        prop_assert_eq!(bin.get_n_entries(), n_after_first);
        let idx2 = bin.find_entry(&update_key, false, true);
        prop_assert_eq!(idx1 & 0xFFFF, idx2 & 0xFFFF);
        prop_assert_eq!(bin.get_lsn((idx2 & 0xFFFF) as usize), lsn1);
        prop_assert_eq!(bin.get_state((idx2 & 0xFFFF) as usize), state1);
    }

    /// Property: `apply_delta_slot` updates an existing key in-place rather
    /// than inserting a duplicate.  The number of entries grows by at most
    /// 1 per `apply_delta_slot` call (and by 0 if the key already existed).
    #[test]
    fn bin_apply_delta_slot_no_duplicates(
        keys in bin_keys_strategy(8),
        updates in prop::collection::vec(
            (prop::collection::vec(any::<u8>(), 1..16), 1u64..1_000_000u64),
            1..8,
        ),
    ) {
        let mut bin = Bin::new(1, 64);
        for (i, k) in keys.iter().enumerate() {
            bin.insert_entry(k.clone(), Lsn::from_u64(100 + i as u64), 0, None)
                .unwrap();
        }
        let initial_n = bin.get_n_entries();
        let mut unique_new_keys: std::collections::HashSet<Vec<u8>> =
            std::collections::HashSet::new();
        for (uk, lsn) in &updates {
            if !keys.contains(uk) {
                unique_new_keys.insert(uk.clone());
            }
            bin.apply_delta_slot(uk.clone(), Lsn::from_u64(*lsn), 0, None);
        }
        let final_n = bin.get_n_entries();
        prop_assert_eq!(
            final_n,
            initial_n + unique_new_keys.len(),
            "n_entries must grow only by the number of *new* keys",
        );
        // Every key (initial + new) is findable exactly once.
        for k in keys.iter().chain(unique_new_keys.iter()) {
            let idx = bin.find_entry(k, false, true);
            prop_assert!(idx >= 0, "key {:?} not found", k);
            prop_assert!((idx & EXACT_MATCH) != 0, "key {:?} not exact", k);
        }
    }

    /// Property: after `apply_delta_slot`, the slot for the updated key has
    /// exactly the supplied LSN and state byte (modulo internal masking — we
    /// compare full bytes here because the impl preserves them).
    #[test]
    fn bin_apply_delta_slot_writes_lsn_and_state(
        update_key in prop::collection::vec(any::<u8>(), 1..16),
        new_lsn_raw in 1u64..1_000_000u64,
        state_byte: u8,
    ) {
        let mut bin = Bin::new(1, 64);
        // Pre-populate with a different key so the BIN is not empty.
        let other_key = vec![0xFF];
        prop_assume!(other_key != update_key);
        bin.insert_entry(other_key, Lsn::from_u64(50), 0, None).unwrap();

        let new_lsn = Lsn::from_u64(new_lsn_raw + 100_000);
        bin.apply_delta_slot(update_key.clone(), new_lsn, state_byte, None);
        let idx = bin.find_entry(&update_key, false, true);
        prop_assert!(idx >= 0);
        let slot = (idx & 0xFFFF) as usize;
        prop_assert_eq!(bin.get_lsn(slot), new_lsn);
        prop_assert_eq!(bin.get_state(slot), state_byte);
    }
}

// ============================================================================
// 7. Suppress dead-code lints introduced by the helper imports above.
// ============================================================================

#[allow(dead_code)]
fn _force_use_of_imports() {
    // Reference each newly-imported symbol so a future refactor that drops
    // them surfaces as a compile error rather than a silent test deletion.
    let _ = Ordering::Equal;
    let _ = NULL_LSN;
    let _ = compare_keys;
}
