//! Property-based tests for noxu-tree using Hegel / hegeltest.
//!
//! NOTE: the former sections 4 (InNode insert/find) and 6 (BIN-delta apply
//! round-trip) tested the faithful `bin::Bin` / `in_node::InNode`
//! transliterations, which were a shelved parallel implementation beside the
//! runtime `tree::BinStub` / `tree::InNodeStub`.  Those modules were deleted
//! (T-1); the runtime stub is now pinned to a JE-faithful oracle by
//! `tests/bin_stub_conformance.rs`.  The key-comparison and DeltaInfo
//! properties below exercise live modules (`key`, `delta_info`) and remain.

use hegel::generators;
use std::cmp::Ordering;

use noxu_tree::delta_info::DeltaInfo;
use noxu_tree::entry_states::SlotState;
use noxu_tree::key::{
    compare_keys, compare_unsigned_bytes, create_key_prefix,
    get_key_prefix_length,
};
use noxu_util::Lsn;

// ============================================================================
// 1. Key comparison is consistent: compare(a,b) is opposite of compare(b,a)
// ============================================================================

#[hegel::test]
fn key_comparison_antisymmetry(tc: hegel::TestCase) {
    let a = tc.draw(generators::binary().max_size(63));
    let b = tc.draw(generators::binary().max_size(63));
    let ab = compare_unsigned_bytes(&a, &b);
    let ba = compare_unsigned_bytes(&b, &a);
    assert_eq!(ab, ba.reverse());
}

#[hegel::test]
fn key_comparison_with_comparator_antisymmetry(tc: hegel::TestCase) {
    let a = tc.draw(generators::binary().max_size(63));
    let b = tc.draw(generators::binary().max_size(63));
    let ab = compare_keys(&a, &b, None);
    let ba = compare_keys(&b, &a, None);
    assert_eq!(ab, ba.reverse());
}

// ============================================================================
// 2. Key comparison is transitive: if a<b and b<c then a<c
// ============================================================================

#[hegel::test]
fn key_comparison_transitivity(tc: hegel::TestCase) {
    let a = tc.draw(generators::binary().max_size(31));
    let b = tc.draw(generators::binary().max_size(31));
    let c = tc.draw(generators::binary().max_size(31));

    let ab = compare_unsigned_bytes(&a, &b);
    let bc = compare_unsigned_bytes(&b, &c);
    let ac = compare_unsigned_bytes(&a, &c);

    if ab == Ordering::Less && bc == Ordering::Less {
        assert_eq!(ac, Ordering::Less);
    }
    if ab == Ordering::Greater && bc == Ordering::Greater {
        assert_eq!(ac, Ordering::Greater);
    }
    if ab == Ordering::Equal && bc == Ordering::Equal {
        assert_eq!(ac, Ordering::Equal);
    }
}

// ============================================================================
// 3. Key prefix: the prefix of two keys is a prefix of both keys, and
//    the prefix length is consistent with create_key_prefix.
// ============================================================================

#[hegel::test]
fn key_prefix_is_prefix_of_both(tc: hegel::TestCase) {
    let a = tc.draw(generators::binary().max_size(63));
    let b = tc.draw(generators::binary().max_size(63));
    let prefix_len = get_key_prefix_length(&a, &b);

    // The prefix length must not exceed either key length
    assert!(prefix_len <= a.len());
    assert!(prefix_len <= b.len());

    // The first prefix_len bytes must be identical
    assert_eq!(&a[..prefix_len], &b[..prefix_len]);

    // If prefix_len < min(a.len(), b.len()), the bytes at prefix_len must differ
    if prefix_len < a.len().min(b.len()) {
        assert_ne!(a[prefix_len], b[prefix_len]);
    }
}

#[hegel::test]
fn create_key_prefix_roundtrip(tc: hegel::TestCase) {
    let a = tc.draw(generators::binary().max_size(63));
    let b = tc.draw(generators::binary().max_size(63));
    let prefix_len = get_key_prefix_length(&a, &b);
    let prefix = create_key_prefix(&a, &b);

    match prefix {
        Some(p) => {
            assert!(prefix_len > 0);
            assert_eq!(p.len(), prefix_len);
            // The prefix must be a prefix of both keys
            assert_eq!(&a[..prefix_len], p.as_slice());
            assert_eq!(&b[..prefix_len], p.as_slice());
        }
        None => {
            assert_eq!(prefix_len, 0);
        }
    }
}

#[hegel::test]
fn key_prefix_of_identical_keys(tc: hegel::TestCase) {
    let a = tc.draw(generators::binary().max_size(63));
    let prefix_len = get_key_prefix_length(&a, &a);
    assert_eq!(prefix_len, a.len());
}

// ============================================================================
// 4. DeltaInfo encode/decode round-trip (Wave 11-E).
//
// For any DeltaInfo with arbitrary key, lsn, and state byte, write_to_log
// followed by read_from_log must reproduce the same DeltaInfo and consume
// exactly log_size() bytes.
// ============================================================================

#[hegel::test(test_cases = 128)]
fn delta_info_roundtrip(tc: hegel::TestCase) {
    let key = tc.draw(generators::binary().max_size(255));
    let lsn_raw = tc.draw(generators::integers::<u64>());
    let state_byte = tc.draw(generators::integers::<u8>());

    let lsn = Lsn::from_u64(lsn_raw);
    let state = SlotState::from_byte(state_byte);
    let original = DeltaInfo::new(key.clone(), lsn, state);

    let mut buf = Vec::new();
    original.write_to_log(&mut buf);

    assert_eq!(buf.len(), original.log_size());

    let (decoded, consumed) =
        DeltaInfo::read_from_log(&buf).expect("decode must succeed");
    assert_eq!(consumed, buf.len());
    assert_eq!(decoded.key, key);
    assert_eq!(decoded.lsn, lsn);
    assert_eq!(decoded.state.as_byte(), state_byte);
}

/// Encoding is deterministic: encoding the same DeltaInfo twice produces
/// the same bytes.  Catches accidental nondeterminism (HashMap iteration,
/// uninitialized padding, etc.) in the encoder.
#[hegel::test(test_cases = 128)]
fn delta_info_encode_deterministic(tc: hegel::TestCase) {
    let key = tc.draw(generators::binary().max_size(127));
    let lsn_raw = tc.draw(generators::integers::<u64>());
    let state_byte = tc.draw(generators::integers::<u8>());

    let lsn = Lsn::from_u64(lsn_raw);
    let state = SlotState::from_byte(state_byte);
    let info = DeltaInfo::new(key, lsn, state);
    let mut buf1 = Vec::new();
    let mut buf2 = Vec::new();
    info.write_to_log(&mut buf1);
    info.write_to_log(&mut buf2);
    assert_eq!(buf1, buf2);
}

/// Read-then-write reproduces the original byte sequence (reverse direction
/// of the round-trip).  For any byte sequence that successfully decodes,
/// re-encoding must produce a byte-identical prefix of the original.
#[hegel::test(test_cases = 128)]
fn delta_info_read_then_write_idempotent(tc: hegel::TestCase) {
    let key = tc.draw(generators::binary().max_size(127));
    let lsn_raw = tc.draw(generators::integers::<u64>());
    let state_byte = tc.draw(generators::integers::<u8>());

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

    assert_eq!(buf1, buf2);
}
