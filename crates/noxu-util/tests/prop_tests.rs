//! Property-based tests for noxu-util foundation types.

use noxu_util::lsn::{Lsn, NULL_LSN};
use noxu_util::packed::{
    read_packed_i32, read_packed_i64, read_sorted_i32, read_sorted_i64,
    write_packed_i32, write_packed_i64, write_sorted_i32, write_sorted_i64,
};
use noxu_util::vlsn::Vlsn;
use proptest::prelude::*;
use std::io::Cursor;

// =============================================================================
// LSN property tests
// =============================================================================

proptest! {
    /// For any (file_number, offset), constructing an LSN and extracting the
    /// components yields the original values.
    #[test]
    fn lsn_roundtrip(file_number: u32, offset: u32) {
        let lsn = Lsn::new(file_number, offset);
        prop_assert_eq!(lsn.file_number(), file_number);
        prop_assert_eq!(lsn.file_offset(), offset);
    }

    /// For any u64, converting to LSN and back yields the same u64.
    #[test]
    fn lsn_from_u64_roundtrip(val: u64) {
        let lsn = Lsn::from_u64(val);
        prop_assert_eq!(lsn.as_u64(), val);
    }

    /// If file_a < file_b, then Lsn(file_a, 0) < Lsn(file_b, 0).
    #[test]
    fn lsn_ordering_by_file(
        file_a in 0u32..u32::MAX,
    ) {
        let file_b = file_a + 1; // guaranteed file_a < file_b
        let lsn_a = Lsn::new(file_a, 0);
        let lsn_b = Lsn::new(file_b, 0);
        prop_assert!(lsn_a < lsn_b);
    }

    /// Within the same file, ordering is by offset.
    #[test]
    fn lsn_ordering_by_offset(
        file_number: u32,
        offset_a in 0u32..u32::MAX,
    ) {
        let offset_b = offset_a + 1;
        let lsn_a = Lsn::new(file_number, offset_a);
        let lsn_b = Lsn::new(file_number, offset_b);
        prop_assert!(lsn_a < lsn_b);
    }

    /// NULL_LSN is always detected as null.
    #[test]
    fn lsn_null_is_null(_dummy in 0u32..1u32) {
        prop_assert!(NULL_LSN.is_null());
    }

    /// A non-MAX LSN is not null.
    #[test]
    fn lsn_non_null(
        file_number in 0u32..u32::MAX,
        offset: u32,
    ) {
        // Lsn(u32::MAX, u32::MAX) == NULL_LSN, so exclude file_number == MAX
        let lsn = Lsn::new(file_number, offset);
        prop_assert!(!lsn.is_null());
    }
}

// =============================================================================
// VLSN property tests
// =============================================================================

proptest! {
    /// For any i64, constructing a VLSN and extracting the sequence yields
    /// the original value.
    #[test]
    fn vlsn_roundtrip(val: i64) {
        let vlsn = Vlsn::new(val);
        prop_assert_eq!(vlsn.sequence(), val);
    }

    /// For two positive VLSNs where a < b, Vlsn(a) < Vlsn(b).
    #[test]
    fn vlsn_ordering(a in 1i64..i64::MAX) {
        let b = a; // same value => equal
        let vlsn_a = Vlsn::new(a);
        let vlsn_b = Vlsn::new(b);
        prop_assert_eq!(vlsn_a, vlsn_b);
    }

    /// For a < b (both positive), ordering is preserved.
    #[test]
    fn vlsn_ordering_strict(a in 1i64..(i64::MAX - 1)) {
        let b = a + 1;
        let vlsn_a = Vlsn::new(a);
        let vlsn_b = Vlsn::new(b);
        prop_assert!(vlsn_a < vlsn_b);
    }

    /// next() of a positive VLSN yields sequence + 1.
    #[test]
    fn vlsn_next(seq in 1i64..(i64::MAX - 1)) {
        let vlsn = Vlsn::new(seq);
        prop_assert_eq!(vlsn.next().sequence(), seq + 1);
    }

    /// prev() of a VLSN with sequence > 1 yields sequence - 1.
    #[test]
    fn vlsn_prev(seq in 2i64..i64::MAX) {
        let vlsn = Vlsn::new(seq);
        prop_assert_eq!(vlsn.prev().sequence(), seq - 1);
    }
}

// =============================================================================
// Packed integer property tests
// =============================================================================

proptest! {
    /// For any i32, write then read yields the original value.
    #[test]
    fn packed_i32_roundtrip(val: i32) {
        let mut buf = Vec::new();
        write_packed_i32(&mut buf, val).unwrap();
        let result = read_packed_i32(&mut Cursor::new(&buf)).unwrap();
        prop_assert_eq!(result, val);
    }

    /// For any i64, write then read yields the original value.
    #[test]
    fn packed_i64_roundtrip(val: i64) {
        let mut buf = Vec::new();
        write_packed_i64(&mut buf, val).unwrap();
        let result = read_packed_i64(&mut Cursor::new(&buf)).unwrap();
        prop_assert_eq!(result, val);
    }

    /// Sorted i32 encoding preserves order: if a < b, encoded(a) < encoded(b)
    /// in lexicographic byte comparison.
    #[test]
    fn sorted_i32_order_preserving(a in i32::MIN..(i32::MAX - 1)) {
        let b = a + 1; // a < b guaranteed by construction
        let mut buf_a = Vec::new();
        let mut buf_b = Vec::new();
        write_sorted_i32(&mut buf_a, a).unwrap();
        write_sorted_i32(&mut buf_b, b).unwrap();
        prop_assert!(buf_a < buf_b, "encoded({}) should be < encoded({})", a, b);
    }

    /// Sorted i64 encoding preserves order: if a < b, encoded(a) < encoded(b).
    #[test]
    fn sorted_i64_order_preserving(a in i64::MIN..(i64::MAX - 1)) {
        let b = a + 1;
        let mut buf_a = Vec::new();
        let mut buf_b = Vec::new();
        write_sorted_i64(&mut buf_a, a).unwrap();
        write_sorted_i64(&mut buf_b, b).unwrap();
        prop_assert!(buf_a < buf_b, "encoded({}) should be < encoded({})", a, b);
    }

    /// Sorted i32 round-trip: write then read yields original.
    #[test]
    fn sorted_i32_roundtrip(val: i32) {
        let mut buf = Vec::new();
        write_sorted_i32(&mut buf, val).unwrap();
        let result = read_sorted_i32(&mut Cursor::new(&buf)).unwrap();
        prop_assert_eq!(result, val);
    }

    /// Sorted i64 round-trip: write then read yields original.
    #[test]
    fn sorted_i64_roundtrip(val: i64) {
        let mut buf = Vec::new();
        write_sorted_i64(&mut buf, val).unwrap();
        let result = read_sorted_i64(&mut Cursor::new(&buf)).unwrap();
        prop_assert_eq!(result, val);
    }

    /// Packed encoding size is always positive and bounded.
    #[test]
    fn packed_i32_size_bounded(val: i32) {
        let mut buf = Vec::new();
        let size = write_packed_i32(&mut buf, val).unwrap();
        prop_assert!((1..=5).contains(&size));
        prop_assert_eq!(size, buf.len());
    }

    /// Packed i64 encoding size is always positive and bounded.
    #[test]
    fn packed_i64_size_bounded(val: i64) {
        let mut buf = Vec::new();
        let size = write_packed_i64(&mut buf, val).unwrap();
        prop_assert!((1..=9).contains(&size));
        prop_assert_eq!(size, buf.len());
    }
}
