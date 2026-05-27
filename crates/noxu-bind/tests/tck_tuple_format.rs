//! JE TCK port: tuple format & ordering tests.
//!
//! Ports invariants from JE
//! `com.sleepycat.bind.tuple.test.TupleFormatTest` and
//! `com.sleepycat.bind.tuple.test.TupleOrderingTest` onto Noxu's
//! `TupleInput` / `TupleOutput`.
//!
//! Notes on adaptation
//!
//! - JE `writeString` writes UTF-8 followed by a single 0x00 terminator.
//!   Noxu's `write_string` escapes embedded `0x00` bytes as `[0x00, 0x01]`
//!   and terminates with `[0x00, 0x00]` (two bytes).  This is documented
//!   on `TupleOutput::write_string`, so the JE wire-size assertions
//!   (`val.length() + 1`) do not apply to noxu and are intentionally
//!   omitted; the round-trip and ordering invariants are preserved.
//! - JE supports a "null string" marker.  Noxu's API takes `&str` and
//!   does not, so JE's `testNullString` is omitted.
//! - JE `writeFloat` / `writeDouble` use the *non-sorted* IEEE-754
//!   ordering, in which only non-negative values are ordered; noxu's
//!   `write_float` / `write_double` match that contract.  Sortable
//!   ordering across the full range uses `write_sorted_float` /
//!   `write_sorted_double`.
//! - All assertions check value-level invariants (round-trip, monotone
//!   byte ordering of the encoded form).  Wire-format byte-level
//!   assertions are noxu-specific and live in unit tests in
//!   `tuple_output.rs` and `tuple_input.rs`.

use noxu_bind::{TupleInput, TupleOutput};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Asserts that `prev < next` lexicographically as unsigned bytes.
/// Mirrors JE TupleOrderingTest's `check(int)` helper.
fn assert_lt_bytes(prev: &[u8], next: &[u8], idx: usize) {
    assert!(
        prev < next,
        "encoded tuple at idx {idx} is not strictly greater than the \
         previous one\n  prev = {prev:02x?}\n  next = {next:02x?}",
    );
}

/// Encode each value with `write_value`, assert that successive encodings
/// are strictly monotone in unsigned-byte lexicographic order.
fn check_monotone<T: Copy, F: FnMut(&mut TupleOutput, T)>(
    data: &[T],
    mut write_value: F,
) {
    let mut prev: Option<Vec<u8>> = None;
    for (i, &v) in data.iter().enumerate() {
        let mut out = TupleOutput::new();
        write_value(&mut out, v);
        let next = out.as_bytes().to_vec();
        if let Some(prev) = &prev {
            assert_lt_bytes(prev, &next, i);
        }
        prev = Some(next);
    }
}

// ---------------------------------------------------------------------------
// Round-trip tests (port of TupleFormatTest)
// ---------------------------------------------------------------------------

/// Port of `TupleFormatTest.testString`: round-trip "", "a", "abc", and
/// confirm that multiple strings concatenated by repeated `write_string`
/// can be read back in order with no bytes left over.
#[test]
fn tck_tuple_format_test_string() {
    for s in ["", "a", "abc"] {
        let mut out = TupleOutput::new();
        out.write_string(s);
        let mut input = TupleInput::new(out.as_bytes());
        assert_eq!(s, input.read_string().unwrap());
        assert_eq!(0, input.available());
    }

    // Two strings.
    let mut out = TupleOutput::new();
    out.write_string("abc");
    out.write_string("defg");
    let mut input = TupleInput::new(out.as_bytes());
    assert_eq!("abc", input.read_string().unwrap());
    assert_eq!("defg", input.read_string().unwrap());
    assert_eq!(0, input.available());

    // Three strings.
    let mut out = TupleOutput::new();
    out.write_string("abc");
    out.write_string("defg");
    out.write_string("hijkl");
    let mut input = TupleInput::new(out.as_bytes());
    assert_eq!("abc", input.read_string().unwrap());
    assert_eq!("defg", input.read_string().unwrap());
    assert_eq!("hijkl", input.read_string().unwrap());
    assert_eq!(0, input.available());
}

/// Port of `TupleFormatTest.testBoolean`: round-trip of true/false in
/// 1/2/3-element tuples; each boolean occupies 1 byte.
#[test]
fn tck_tuple_format_test_boolean() {
    for v in [true, false] {
        let mut out = TupleOutput::new();
        out.write_bool(v);
        assert_eq!(1, out.len());
        let mut input = TupleInput::new(out.as_bytes());
        assert_eq!(v, input.read_bool().unwrap());
        assert_eq!(0, input.available());
    }

    let mut out = TupleOutput::new();
    out.write_bool(true);
    out.write_bool(false);
    out.write_bool(true);
    assert_eq!(3, out.len());
    let mut input = TupleInput::new(out.as_bytes());
    assert!(input.read_bool().unwrap());
    assert!(!input.read_bool().unwrap());
    assert!(input.read_bool().unwrap());
    assert_eq!(0, input.available());
}

/// Port of `TupleFormatTest.testInt`: round-trip every interesting i32
/// boundary value.  Each i32 occupies 4 bytes.
#[test]
fn tck_tuple_format_test_int() {
    let data: &[i32] = &[
        i32::MIN,
        i32::MIN + 1,
        i16::MIN as i32,
        i16::MIN as i32 + 1,
        i8::MIN as i32,
        i8::MIN as i32 + 1,
        -1,
        0,
        1,
        i8::MAX as i32 - 1,
        i8::MAX as i32,
        i16::MAX as i32 - 1,
        i16::MAX as i32,
        i32::MAX - 1,
        i32::MAX,
    ];
    for &v in data {
        let mut out = TupleOutput::new();
        out.write_i32(v);
        assert_eq!(4, out.len());
        let mut input = TupleInput::new(out.as_bytes());
        assert_eq!(v, input.read_i32().unwrap());
        assert_eq!(0, input.available());
    }
}

/// Port of `TupleFormatTest.testLong`: same as testInt but for i64.
/// Each i64 occupies 8 bytes.
#[test]
fn tck_tuple_format_test_long() {
    let data: &[i64] = &[
        i64::MIN,
        i64::MIN + 1,
        i32::MIN as i64,
        i32::MIN as i64 + 1,
        i16::MIN as i64,
        -1,
        0,
        1,
        i16::MAX as i64,
        i32::MAX as i64,
        i32::MAX as i64 + 1,
        i64::MAX - 1,
        i64::MAX,
    ];
    for &v in data {
        let mut out = TupleOutput::new();
        out.write_i64(v);
        assert_eq!(8, out.len());
        let mut input = TupleInput::new(out.as_bytes());
        assert_eq!(v, input.read_i64().unwrap());
        assert_eq!(0, input.available());
    }
}

/// Port of `TupleFormatTest.testPackedInt` & `testPackedLong`: round-trip
/// for the variable-length packed encoding.  The size of the encoding is
/// not asserted (it depends on magnitude), only that decode(encode) == v
/// for every boundary value.
#[test]
fn tck_tuple_format_test_packed_int_and_long() {
    let int_data: &[i32] =
        &[i32::MIN, -1, 0, 1, 0x7F, 0x80, 0x3FFF, 0x4000, i32::MAX];
    for &v in int_data {
        let mut out = TupleOutput::new();
        out.write_packed_int(v);
        let mut input = TupleInput::new(out.as_bytes());
        assert_eq!(v, input.read_packed_int().unwrap());
        assert_eq!(0, input.available());
    }

    let long_data: &[i64] =
        &[i64::MIN, i32::MIN as i64, -1, 0, 1, i32::MAX as i64, i64::MAX];
    for &v in long_data {
        let mut out = TupleOutput::new();
        out.write_packed_long(v);
        let mut input = TupleInput::new(out.as_bytes());
        assert_eq!(v, input.read_packed_long().unwrap());
        assert_eq!(0, input.available());
    }
}

// ---------------------------------------------------------------------------
// Ordering tests (port of TupleOrderingTest)
// ---------------------------------------------------------------------------

/// Port of `TupleOrderingTest.testString`: encoded strings sort
/// lexicographically by Unicode order when written via `write_string`.
/// Strings containing `\u{0000}` are excluded because noxu's escape
/// sequence (`\0` -> `[0x00 0x01]`) means a `\0`-containing string sorts
/// strictly *after* the same string without the trailing `\0` rather
/// than before it; the ordering is still total and consistent, but the
/// JE-specific data set is not directly comparable for those cases.
#[test]
fn tck_tuple_ordering_test_string() {
    let data: &[&str] = &[
        "",
        "\u{0001}",
        "\u{0002}",
        "A",
        "a",
        "ab",
        "b",
        "bb",
        "bba",
        "c",
        "c\u{0001}",
        "d",
        "\u{007F}",
        "\u{00FF}",
    ];
    check_monotone(data, |out, s| out.write_string(s));
}

/// Port of `TupleOrderingTest.testBoolean`: false < true.
#[test]
fn tck_tuple_ordering_test_boolean() {
    check_monotone(&[false, true], |out, v| out.write_bool(v));
}

/// Port of `TupleOrderingTest.testUnsignedByte`/`UnsignedShort`/
/// `UnsignedInt`: unsigned big-endian writes preserve numeric order.
#[test]
fn tck_tuple_ordering_test_unsigned() {
    let bytes: &[u8] = &[0, 1, 0x7F, 0xFF];
    check_monotone(bytes, |out, v| out.write_u8(v));

    let shorts: &[u16] = &[0, 1, 0xFE, 0xFF, 0x800, 0x7FFF, 0xFFFF];
    check_monotone(shorts, |out, v| out.write_u16(v));

    let ints: &[u32] = &[
        0, 1, 0xFE, 0xFF, 0x800, 0x7FFF, 0xFFFF, 0x80000, 0x7FFFFFFF,
        0x80000000, 0xFFFFFFFF,
    ];
    check_monotone(ints, |out, v| out.write_u32(v));
}

/// Port of `TupleOrderingTest.testByte` / `testShort` / `testInt` /
/// `testLong`: signed integers, when written via `write_iN`, sort by
/// numeric value.  Noxu (like JE) flips the high bit at the byte level
/// so that negative values sort before non-negative ones.
#[test]
fn tck_tuple_ordering_test_signed() {
    let bytes: &[i8] = &[i8::MIN, i8::MIN + 1, -1, 0, 1, i8::MAX - 1, i8::MAX];
    check_monotone(bytes, |out, v| out.write_i8(v));

    let shorts: &[i16] = &[
        i16::MIN,
        i16::MIN + 1,
        i8::MIN as i16,
        i8::MIN as i16 + 1,
        -1,
        0,
        1,
        i8::MAX as i16 - 1,
        i8::MAX as i16,
        i16::MAX - 1,
        i16::MAX,
    ];
    check_monotone(shorts, |out, v| out.write_i16(v));

    let ints: &[i32] = &[
        i32::MIN,
        i32::MIN + 1,
        i16::MIN as i32,
        i16::MIN as i32 + 1,
        -1,
        0,
        1,
        i16::MAX as i32,
        i32::MAX - 1,
        i32::MAX,
    ];
    check_monotone(ints, |out, v| out.write_i32(v));

    let longs: &[i64] = &[
        i64::MIN,
        i64::MIN + 1,
        i32::MIN as i64,
        -1,
        0,
        1,
        i32::MAX as i64,
        i64::MAX - 1,
        i64::MAX,
    ];
    check_monotone(longs, |out, v| out.write_i64(v));
}

/// Port of `TupleOrderingTest.testFloat` / `testDouble`: only the
/// non-negative subset of IEEE-754 sorts deterministically when
/// written via `write_float` / `write_double`.  NaN is excluded
/// (`NaN < NaN` is meaningless; JE's test happens to work because
/// the bit pattern of one specific NaN sorts above +Inf, but that
/// is implementation-specific noise rather than an invariant).
#[test]
fn tck_tuple_ordering_test_float_double_nonneg() {
    let floats: &[f32] = &[
        0.0,
        f32::MIN_POSITIVE,
        2.0 * f32::MIN_POSITIVE,
        0.01,
        0.99,
        1.0,
        1.99,
        i8::MAX as f32,
        i16::MAX as f32,
        i32::MAX as f32,
        f32::MAX,
        f32::INFINITY,
    ];
    check_monotone(floats, |out, v| out.write_float(v));

    let doubles: &[f64] = &[
        0.0,
        f64::MIN_POSITIVE,
        2.0 * f64::MIN_POSITIVE,
        0.001,
        0.999,
        1.0,
        1.999,
        i32::MAX as f64,
        f32::MAX as f64,
        f64::MAX,
        f64::INFINITY,
    ];
    check_monotone(doubles, |out, v| out.write_double(v));
}

/// Port of `TupleOrderingTest.testSortedFloat` / `testSortedDouble`:
/// across the full IEEE-754 range (negatives included), `write_sorted_*`
/// produces a monotone byte encoding.  NaN is excluded as above.
#[test]
fn tck_tuple_ordering_test_sorted_float_double() {
    let floats: &[f32] = &[
        f32::NEG_INFINITY,
        -f32::MAX,
        i32::MIN as f32,
        i16::MIN as f32,
        i8::MIN as f32,
        -1.99,
        -1.0,
        -0.99,
        -0.01,
        -2.0 * f32::MIN_POSITIVE,
        -f32::MIN_POSITIVE,
        0.0,
        f32::MIN_POSITIVE,
        2.0 * f32::MIN_POSITIVE,
        0.01,
        0.99,
        1.0,
        1.99,
        i8::MAX as f32,
        i16::MAX as f32,
        i32::MAX as f32,
        f32::MAX,
        f32::INFINITY,
    ];
    check_monotone(floats, |out, v| out.write_sorted_float(v));

    let doubles: &[f64] = &[
        f64::NEG_INFINITY,
        -f64::MAX,
        -(f32::MAX as f64),
        i64::MIN as f64,
        i32::MIN as f64,
        i16::MIN as f64,
        i8::MIN as f64,
        -1.999,
        -1.0,
        -0.999,
        -0.001,
        -2.0 * f64::MIN_POSITIVE,
        -f64::MIN_POSITIVE,
        0.0,
        f64::MIN_POSITIVE,
        2.0 * f64::MIN_POSITIVE,
        0.001,
        0.999,
        1.0,
        1.999,
        i32::MAX as f64,
        f32::MAX as f64,
        f64::MAX,
        f64::INFINITY,
    ];
    check_monotone(doubles, |out, v| out.write_sorted_double(v));
}

/// Port of `TupleOrderingTest.testSortedPackedInt` /
/// `testSortedPackedLong`: the sorted packed varint encoding is
/// monotone across negative and non-negative values.
#[test]
fn tck_tuple_ordering_test_sorted_packed_int_long() {
    let ints: &[i32] = &[
        i32::MIN,
        i16::MIN as i32,
        i8::MIN as i32,
        -1,
        0,
        1,
        i8::MAX as i32,
        i16::MAX as i32,
        i32::MAX,
    ];
    check_monotone(ints, |out, v| out.write_sorted_packed_int(v));

    let longs: &[i64] = &[
        i64::MIN,
        i32::MIN as i64,
        i16::MIN as i64,
        -1,
        0,
        1,
        i16::MAX as i64,
        i32::MAX as i64,
        i64::MAX,
    ];
    check_monotone(longs, |out, v| out.write_sorted_packed_long(v));
}
