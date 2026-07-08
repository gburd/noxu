//! Property-based tests for noxu-util foundation types (Hegel / hegeltest).

use hegel::generators;
use noxu_util::lsn::{Lsn, NULL_LSN};
use noxu_util::packed::{
    read_packed_i32, read_packed_i64, read_sorted_i32, read_sorted_i64,
    write_packed_i32, write_packed_i64, write_sorted_i32, write_sorted_i64,
};
use noxu_util::vlsn::Vlsn;
use std::io::Cursor;

// =============================================================================
// LSN property tests
// =============================================================================

/// For any (file_number, offset), constructing an LSN and extracting the
/// components yields the original values.
#[hegel::test]
fn lsn_roundtrip(tc: hegel::TestCase) {
    let file_number = tc.draw(generators::integers::<u32>());
    let offset = tc.draw(generators::integers::<u32>());
    let lsn = Lsn::new(file_number, offset);
    assert_eq!(lsn.file_number(), file_number);
    assert_eq!(lsn.file_offset(), offset);
}

/// For any u64, converting to LSN and back yields the same u64.
#[hegel::test]
fn lsn_from_u64_roundtrip(tc: hegel::TestCase) {
    let val = tc.draw(generators::integers::<u64>());
    let lsn = Lsn::from_u64(val);
    assert_eq!(lsn.as_u64(), val);
}

/// If file_a < file_b, then Lsn(file_a, 0) < Lsn(file_b, 0).
#[hegel::test]
fn lsn_ordering_by_file(tc: hegel::TestCase) {
    let file_a = tc.draw(generators::integers::<u32>().max_value(u32::MAX - 1));
    let file_b = file_a + 1; // guaranteed file_a < file_b
    let lsn_a = Lsn::new(file_a, 0);
    let lsn_b = Lsn::new(file_b, 0);
    assert!(lsn_a < lsn_b);
}

/// Within the same file, ordering is by offset.
#[hegel::test]
fn lsn_ordering_by_offset(tc: hegel::TestCase) {
    let file_number = tc.draw(generators::integers::<u32>());
    let offset_a = tc.draw(generators::integers::<u32>().max_value(u32::MAX - 1));
    let offset_b = offset_a + 1;
    let lsn_a = Lsn::new(file_number, offset_a);
    let lsn_b = Lsn::new(file_number, offset_b);
    assert!(lsn_a < lsn_b);
}

/// NULL_LSN is always detected as null.
#[test]
fn lsn_null_is_null() {
    assert!(NULL_LSN.is_null());
}

/// A non-MAX LSN is not null.
#[hegel::test]
fn lsn_non_null(tc: hegel::TestCase) {
    // Lsn(u32::MAX, u32::MAX) == NULL_LSN, so exclude file_number == MAX.
    let file_number = tc.draw(generators::integers::<u32>().max_value(u32::MAX - 1));
    let offset = tc.draw(generators::integers::<u32>());
    let lsn = Lsn::new(file_number, offset);
    assert!(!lsn.is_null());
}

// =============================================================================
// VLSN property tests
// =============================================================================

/// For any i64, constructing a VLSN and extracting the sequence yields
/// the original value.
#[hegel::test]
fn vlsn_roundtrip(tc: hegel::TestCase) {
    let val = tc.draw(generators::integers::<i64>());
    let vlsn = Vlsn::new(val);
    assert_eq!(vlsn.sequence(), val);
}

/// A VLSN equals another VLSN built from the same sequence.
#[hegel::test]
fn vlsn_ordering(tc: hegel::TestCase) {
    let a = tc.draw(generators::integers::<i64>().min_value(1));
    let vlsn_a = Vlsn::new(a);
    let vlsn_b = Vlsn::new(a);
    assert_eq!(vlsn_a, vlsn_b);
}

/// For a < b (both positive), ordering is preserved.
#[hegel::test]
fn vlsn_ordering_strict(tc: hegel::TestCase) {
    let a = tc.draw(generators::integers::<i64>().min_value(1).max_value(i64::MAX - 1));
    let b = a + 1;
    let vlsn_a = Vlsn::new(a);
    let vlsn_b = Vlsn::new(b);
    assert!(vlsn_a < vlsn_b);
}

/// next() of a positive VLSN yields sequence + 1.
#[hegel::test]
fn vlsn_next(tc: hegel::TestCase) {
    let seq = tc.draw(generators::integers::<i64>().min_value(1).max_value(i64::MAX - 1));
    let vlsn = Vlsn::new(seq);
    assert_eq!(vlsn.next().sequence(), seq + 1);
}

/// prev() of a VLSN with sequence > 1 yields sequence - 1.
#[hegel::test]
fn vlsn_prev(tc: hegel::TestCase) {
    let seq = tc.draw(generators::integers::<i64>().min_value(2));
    let vlsn = Vlsn::new(seq);
    assert_eq!(vlsn.prev().sequence(), seq - 1);
}

// =============================================================================
// Packed integer property tests
// =============================================================================

/// For any i32, write then read yields the original value.
#[hegel::test]
fn packed_i32_roundtrip(tc: hegel::TestCase) {
    let val = tc.draw(generators::integers::<i32>());
    let mut buf = Vec::new();
    write_packed_i32(&mut buf, val).unwrap();
    let result = read_packed_i32(&mut Cursor::new(&buf)).unwrap();
    assert_eq!(result, val);
}

/// For any i64, write then read yields the original value.
#[hegel::test]
fn packed_i64_roundtrip(tc: hegel::TestCase) {
    let val = tc.draw(generators::integers::<i64>());
    let mut buf = Vec::new();
    write_packed_i64(&mut buf, val).unwrap();
    let result = read_packed_i64(&mut Cursor::new(&buf)).unwrap();
    assert_eq!(result, val);
}

/// Sorted i32 encoding preserves order: if a < b, encoded(a) < encoded(b)
/// in lexicographic byte comparison.
#[hegel::test]
fn sorted_i32_order_preserving(tc: hegel::TestCase) {
    let a = tc.draw(generators::integers::<i32>().max_value(i32::MAX - 1));
    let b = a + 1; // a < b guaranteed by construction
    let mut buf_a = Vec::new();
    let mut buf_b = Vec::new();
    write_sorted_i32(&mut buf_a, a).unwrap();
    write_sorted_i32(&mut buf_b, b).unwrap();
    assert!(buf_a < buf_b, "encoded({a}) should be < encoded({b})");
}

/// Sorted i64 encoding preserves order: if a < b, encoded(a) < encoded(b).
#[hegel::test]
fn sorted_i64_order_preserving(tc: hegel::TestCase) {
    let a = tc.draw(generators::integers::<i64>().max_value(i64::MAX - 1));
    let b = a + 1;
    let mut buf_a = Vec::new();
    let mut buf_b = Vec::new();
    write_sorted_i64(&mut buf_a, a).unwrap();
    write_sorted_i64(&mut buf_b, b).unwrap();
    assert!(buf_a < buf_b, "encoded({a}) should be < encoded({b})");
}

/// Sorted i32 round-trip: write then read yields original.
#[hegel::test]
fn sorted_i32_roundtrip(tc: hegel::TestCase) {
    let val = tc.draw(generators::integers::<i32>());
    let mut buf = Vec::new();
    write_sorted_i32(&mut buf, val).unwrap();
    let result = read_sorted_i32(&mut Cursor::new(&buf)).unwrap();
    assert_eq!(result, val);
}

/// Sorted i64 round-trip: write then read yields original.
#[hegel::test]
fn sorted_i64_roundtrip(tc: hegel::TestCase) {
    let val = tc.draw(generators::integers::<i64>());
    let mut buf = Vec::new();
    write_sorted_i64(&mut buf, val).unwrap();
    let result = read_sorted_i64(&mut Cursor::new(&buf)).unwrap();
    assert_eq!(result, val);
}

/// Packed encoding size is always positive and bounded.
#[hegel::test]
fn packed_i32_size_bounded(tc: hegel::TestCase) {
    let val = tc.draw(generators::integers::<i32>());
    let mut buf = Vec::new();
    let size = write_packed_i32(&mut buf, val).unwrap();
    assert!((1..=5).contains(&size));
    assert_eq!(size, buf.len());
}

/// Packed i64 encoding size is always positive and bounded.
#[hegel::test]
fn packed_i64_size_bounded(tc: hegel::TestCase) {
    let val = tc.draw(generators::integers::<i64>());
    let mut buf = Vec::new();
    let size = write_packed_i64(&mut buf, val).unwrap();
    assert!((1..=9).contains(&size));
    assert_eq!(size, buf.len());
}
