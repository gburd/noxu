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

// ---------------------------------------------------------------------------
// Round-trip ports added in wave 9-C (TupleFormatTest extras)
// ---------------------------------------------------------------------------

/// Port of `TupleFormatTest.testChars`: write a sequence of u16 chars
/// via repeated `write_char` and read them back via `read_char`.  JE
/// uses `writeChars(String)`/`readChars(int)` operating on UTF-16
/// code units; Noxu exposes only the per-char primitive, so we loop
/// to assert the same invariant.
#[test]
fn tck_tuple_format_test_chars() {
    fn round_trip_chars(s: &str) {
        let chars: Vec<u16> = s.encode_utf16().collect();
        let mut out = TupleOutput::new();
        for &c in &chars {
            out.write_char(c);
        }
        // Each char occupies 2 bytes.
        assert_eq!(2 * chars.len(), out.len());
        let mut input = TupleInput::new(out.as_bytes());
        for &c in &chars {
            assert_eq!(c, input.read_char().unwrap());
        }
        assert_eq!(0, input.available());
    }

    round_trip_chars("");
    round_trip_chars("a");
    round_trip_chars("abc");

    // Multi-string concatenation, JE testChars: 7 chars total = 14 bytes.
    let mut out = TupleOutput::new();
    for c in "abc".encode_utf16() {
        out.write_char(c);
    }
    for c in "defg".encode_utf16() {
        out.write_char(c);
    }
    assert_eq!(7 * 2, out.len());
    let mut input = TupleInput::new(out.as_bytes());
    let mut buf = String::new();
    for _ in 0..3 {
        buf.push(char::from_u32(input.read_char().unwrap() as u32).unwrap());
    }
    assert_eq!("abc", buf);
    let mut buf = String::new();
    for _ in 0..4 {
        buf.push(char::from_u32(input.read_char().unwrap() as u32).unwrap());
    }
    assert_eq!("defg", buf);
    assert_eq!(0, input.available());
}

/// Port of `TupleFormatTest.testBytes`: `write_bytes` writes the raw
/// bytes (no length prefix) and `read_bytes(n)` consumes exactly `n`
/// bytes.  Multi-write/multi-read concatenates without delimiters.
#[test]
fn tck_tuple_format_test_bytes() {
    fn round_trip(val: &[u8]) {
        let mut out = TupleOutput::new();
        out.write_bytes(val);
        assert_eq!(val.len(), out.len());
        let mut input = TupleInput::new(out.as_bytes());
        let got = input.read_bytes(val.len()).unwrap();
        assert_eq!(val, got.as_slice());
        assert_eq!(0, input.available());
    }

    round_trip(&[]);
    round_trip(b"a");
    round_trip(b"abc");
    // Top-bit byte values, JE's 0x7F00..=0xFFFF, projected to bytes
    // by JE's `writeBytes(char[])` (high byte discarded).
    round_trip(&[0x00, 0xFF, 0x00, 0xFF]);

    // Multi-write concatenation.
    let mut out = TupleOutput::new();
    out.write_bytes(b"abc");
    out.write_bytes(b"defg");
    assert_eq!(7, out.len());
    let mut input = TupleInput::new(out.as_bytes());
    assert_eq!(b"abc".to_vec(), input.read_bytes(3).unwrap());
    assert_eq!(b"defg".to_vec(), input.read_bytes(4).unwrap());
    assert_eq!(0, input.available());

    let mut out = TupleOutput::new();
    out.write_bytes(b"abc");
    out.write_bytes(b"defg");
    out.write_bytes(b"hijkl");
    assert_eq!(12, out.len());
    let mut input = TupleInput::new(out.as_bytes());
    assert_eq!(b"abc".to_vec(), input.read_bytes(3).unwrap());
    assert_eq!(b"defg".to_vec(), input.read_bytes(4).unwrap());
    assert_eq!(b"hijkl".to_vec(), input.read_bytes(5).unwrap());
    assert_eq!(0, input.available());
}

/// Port of `TupleFormatTest.testByte`: round-trip every interesting i8
/// boundary value.  Each i8 occupies 1 byte.
#[test]
fn tck_tuple_format_test_byte() {
    let data: &[i8] = &[
        i8::MIN,
        i8::MIN + 1,
        -1,
        0,
        1,
        i8::MAX - 1,
        i8::MAX,
        0x7F,
        -0x80, // 0x80 wraps in i8
    ];
    for &v in data {
        let mut out = TupleOutput::new();
        out.write_i8(v);
        assert_eq!(1, out.len());
        let mut input = TupleInput::new(out.as_bytes());
        assert_eq!(v, input.read_i8().unwrap());
        assert_eq!(0, input.available());
    }

    // Three-byte concatenation.
    let mut out = TupleOutput::new();
    out.write_i8(0);
    out.write_i8(1);
    out.write_i8(-1);
    assert_eq!(3, out.len());
    let mut input = TupleInput::new(out.as_bytes());
    assert_eq!(0, input.read_i8().unwrap());
    assert_eq!(1, input.read_i8().unwrap());
    assert_eq!(-1, input.read_i8().unwrap());
    assert_eq!(0, input.available());
}

/// Port of `TupleFormatTest.testShort`: round-trip every interesting i16
/// boundary value.  Each i16 occupies 2 bytes.
#[test]
fn tck_tuple_format_test_short() {
    let data: &[i16] = &[
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
    for &v in data {
        let mut out = TupleOutput::new();
        out.write_i16(v);
        assert_eq!(2, out.len());
        let mut input = TupleInput::new(out.as_bytes());
        assert_eq!(v, input.read_i16().unwrap());
        assert_eq!(0, input.available());
    }

    // Three-short concatenation.
    let mut out = TupleOutput::new();
    out.write_i16(0);
    out.write_i16(1);
    out.write_i16(-1);
    assert_eq!(3 * 2, out.len());
    let mut input = TupleInput::new(out.as_bytes());
    assert_eq!(0, input.read_i16().unwrap());
    assert_eq!(1, input.read_i16().unwrap());
    assert_eq!(-1, input.read_i16().unwrap());
    assert_eq!(0, input.available());
}

/// Port of `TupleFormatTest.testFloat`: round-trip f32 values including
/// NaN, +/-Inf, and signed zero across every interesting boundary.
/// Each f32 occupies 4 bytes.  Note that for `write_float` (unsorted
/// IEEE-754), the byte ordering does not match numeric ordering for
/// negatives; only round-trip is asserted here.
#[test]
fn tck_tuple_format_test_float() {
    let data: &[f32] = &[
        0.0,
        1.0,
        -1.0,
        0.1,
        -0.1,
        f32::NEG_INFINITY,
        f32::INFINITY,
        i16::MAX as f32,
        i16::MIN as f32,
        i32::MAX as f32,
        i32::MIN as f32,
        f32::MAX,
        f32::MIN_POSITIVE,
        -f32::MIN_POSITIVE,
        -f32::MAX,
    ];
    for &v in data {
        let mut out = TupleOutput::new();
        out.write_float(v);
        assert_eq!(4, out.len());
        let mut input = TupleInput::new(out.as_bytes());
        let got = input.read_float().unwrap();
        assert_eq!(v.to_bits(), got.to_bits());
        assert_eq!(0, input.available());
    }

    // NaN round-trip preserves NaN-ness.
    let mut out = TupleOutput::new();
    out.write_float(f32::NAN);
    let mut input = TupleInput::new(out.as_bytes());
    assert!(input.read_float().unwrap().is_nan());

    // Three-float concatenation.
    let mut out = TupleOutput::new();
    out.write_float(0.0);
    out.write_float(1.0);
    out.write_float(-1.0);
    assert_eq!(3 * 4, out.len());
    let mut input = TupleInput::new(out.as_bytes());
    assert_eq!(0.0, input.read_float().unwrap());
    assert_eq!(1.0, input.read_float().unwrap());
    assert_eq!(-1.0, input.read_float().unwrap());
    assert_eq!(0, input.available());
}

/// Port of `TupleFormatTest.testDouble`: as testFloat but for f64.
/// Each f64 occupies 8 bytes.
#[test]
fn tck_tuple_format_test_double() {
    let data: &[f64] = &[
        0.0,
        1.0,
        -1.0,
        0.1,
        -0.1,
        f64::NEG_INFINITY,
        f64::INFINITY,
        i32::MAX as f64,
        i32::MIN as f64,
        i64::MAX as f64,
        i64::MIN as f64,
        f32::MAX as f64,
        f64::MAX,
        f64::MIN_POSITIVE,
        -f64::MIN_POSITIVE,
        -f64::MAX,
    ];
    for &v in data {
        let mut out = TupleOutput::new();
        out.write_double(v);
        assert_eq!(8, out.len());
        let mut input = TupleInput::new(out.as_bytes());
        let got = input.read_double().unwrap();
        assert_eq!(v.to_bits(), got.to_bits());
        assert_eq!(0, input.available());
    }

    // NaN round-trip preserves NaN-ness.
    let mut out = TupleOutput::new();
    out.write_double(f64::NAN);
    let mut input = TupleInput::new(out.as_bytes());
    assert!(input.read_double().unwrap().is_nan());

    // Three-double concatenation.
    let mut out = TupleOutput::new();
    out.write_double(0.0);
    out.write_double(1.0);
    out.write_double(-1.0);
    assert_eq!(3 * 8, out.len());
    let mut input = TupleInput::new(out.as_bytes());
    assert_eq!(0.0, input.read_double().unwrap());
    assert_eq!(1.0, input.read_double().unwrap());
    assert_eq!(-1.0, input.read_double().unwrap());
    assert_eq!(0, input.available());
}

/// Port of `TupleFormatTest.testSortedFloat`: round-trip f32 via the
/// sorted encoding (`write_sorted_float`/`read_sorted_float`).  Each
/// value occupies 4 bytes.
#[test]
fn tck_tuple_format_test_sorted_float() {
    let data: &[f32] = &[
        0.0,
        1.0,
        -1.0,
        0.1,
        -0.1,
        f32::NEG_INFINITY,
        f32::INFINITY,
        i16::MAX as f32,
        i16::MIN as f32,
        i32::MAX as f32,
        i32::MIN as f32,
        f32::MAX,
        -f32::MAX,
        f32::MIN_POSITIVE,
        -f32::MIN_POSITIVE,
    ];
    for &v in data {
        let mut out = TupleOutput::new();
        out.write_sorted_float(v);
        assert_eq!(4, out.len());
        let mut input = TupleInput::new(out.as_bytes());
        let got = input.read_sorted_float().unwrap();
        assert_eq!(v.to_bits(), got.to_bits());
        assert_eq!(0, input.available());
    }

    // NaN round-trips as NaN.
    let mut out = TupleOutput::new();
    out.write_sorted_float(f32::NAN);
    let mut input = TupleInput::new(out.as_bytes());
    assert!(input.read_sorted_float().unwrap().is_nan());
}

/// Port of `TupleFormatTest.testSortedDouble`: round-trip f64 via the
/// sorted encoding.  Each value occupies 8 bytes.
#[test]
fn tck_tuple_format_test_sorted_double() {
    let data: &[f64] = &[
        0.0,
        1.0,
        -1.0,
        0.1,
        -0.1,
        f64::NEG_INFINITY,
        f64::INFINITY,
        i32::MAX as f64,
        i32::MIN as f64,
        i64::MAX as f64,
        i64::MIN as f64,
        f32::MAX as f64,
        f64::MAX,
        -f64::MAX,
        f64::MIN_POSITIVE,
        -f64::MIN_POSITIVE,
    ];
    for &v in data {
        let mut out = TupleOutput::new();
        out.write_sorted_double(v);
        assert_eq!(8, out.len());
        let mut input = TupleInput::new(out.as_bytes());
        let got = input.read_sorted_double().unwrap();
        assert_eq!(v.to_bits(), got.to_bits());
        assert_eq!(0, input.available());
    }

    // NaN round-trips as NaN.
    let mut out = TupleOutput::new();
    out.write_sorted_double(f64::NAN);
    let mut input = TupleInput::new(out.as_bytes());
    assert!(input.read_sorted_double().unwrap().is_nan());
}

/// Port of `TupleFormatTest.testSortedPackedInt`: round-trip i32
/// boundaries via the sorted-packed varint encoding.  The encoded size
/// varies with magnitude; only decode(encode) == v is asserted.
#[test]
fn tck_tuple_format_test_sorted_packed_int() {
    let data: &[i32] = &[
        i32::MIN,
        i32::MIN + 1,
        i16::MIN as i32,
        i8::MIN as i32,
        -1,
        0,
        1,
        i8::MAX as i32,
        i16::MAX as i32,
        i32::MAX - 1,
        i32::MAX,
    ];
    for &v in data {
        let mut out = TupleOutput::new();
        out.write_sorted_packed_int(v);
        let mut input = TupleInput::new(out.as_bytes());
        assert_eq!(v, input.read_sorted_packed_int().unwrap());
        assert_eq!(0, input.available());
    }
}

/// Port of `TupleFormatTest.testSortedPackedLong`: round-trip i64
/// boundaries via the sorted-packed varint encoding.
#[test]
fn tck_tuple_format_test_sorted_packed_long() {
    let data: &[i64] = &[
        i64::MIN,
        i64::MIN + 1,
        i32::MIN as i64,
        i16::MIN as i64,
        -1,
        0,
        1,
        i16::MAX as i64,
        i32::MAX as i64,
        i64::MAX - 1,
        i64::MAX,
    ];
    for &v in data {
        let mut out = TupleOutput::new();
        out.write_sorted_packed_long(v);
        let mut input = TupleInput::new(out.as_bytes());
        assert_eq!(v, input.read_sorted_packed_long().unwrap());
        assert_eq!(0, input.available());
    }
}

/// Port of `TupleFormatTest.testUnsignedByte`: round-trip u8 via
/// `write_u8`/`read_u8`.  Each value occupies 1 byte.
#[test]
fn tck_tuple_format_test_unsigned_byte() {
    for v in [0u8, 1, 254, 255] {
        let mut out = TupleOutput::new();
        out.write_u8(v);
        assert_eq!(1, out.len());
        let mut input = TupleInput::new(out.as_bytes());
        assert_eq!(v, input.read_u8().unwrap());
        assert_eq!(0, input.available());
    }

    let mut out = TupleOutput::new();
    out.write_u8(0);
    out.write_u8(1);
    out.write_u8(255);
    assert_eq!(3, out.len());
    let mut input = TupleInput::new(out.as_bytes());
    assert_eq!(0, input.read_u8().unwrap());
    assert_eq!(1, input.read_u8().unwrap());
    assert_eq!(255, input.read_u8().unwrap());
    assert_eq!(0, input.available());
}

/// Port of `TupleFormatTest.testUnsignedShort`: round-trip u16 via
/// `write_u16`/`read_u16`.  Each value occupies 2 bytes.
#[test]
fn tck_tuple_format_test_unsigned_short() {
    let data: &[u16] = &[
        0,
        1,
        255,
        256,
        257,
        i16::MAX as u16 - 1,
        i16::MAX as u16,
        i16::MAX as u16 + 1,
        u16::MAX - 1,
        u16::MAX,
    ];
    for &v in data {
        let mut out = TupleOutput::new();
        out.write_u16(v);
        assert_eq!(2, out.len());
        let mut input = TupleInput::new(out.as_bytes());
        assert_eq!(v, input.read_u16().unwrap());
        assert_eq!(0, input.available());
    }

    let mut out = TupleOutput::new();
    out.write_u16(0);
    out.write_u16(1);
    out.write_u16(u16::MAX);
    assert_eq!(6, out.len());
    let mut input = TupleInput::new(out.as_bytes());
    assert_eq!(0, input.read_u16().unwrap());
    assert_eq!(1, input.read_u16().unwrap());
    assert_eq!(u16::MAX, input.read_u16().unwrap());
    assert_eq!(0, input.available());
}

/// Port of `TupleFormatTest.testUnsignedInt`: round-trip u32 via
/// `write_u32`/`read_u32`.  Each value occupies 4 bytes.
#[test]
fn tck_tuple_format_test_unsigned_int() {
    let data: &[u32] = &[
        0,
        1,
        255,
        256,
        257,
        i16::MAX as u32 - 1,
        i16::MAX as u32,
        i16::MAX as u32 + 1,
        i32::MAX as u32 - 1,
        i32::MAX as u32,
        i32::MAX as u32 + 1,
        u32::MAX - 1,
        u32::MAX,
    ];
    for &v in data {
        let mut out = TupleOutput::new();
        out.write_u32(v);
        assert_eq!(4, out.len());
        let mut input = TupleInput::new(out.as_bytes());
        assert_eq!(v, input.read_u32().unwrap());
        assert_eq!(0, input.available());
    }
}

// ---------------------------------------------------------------------------
// TupleOrderingTest extras (wave 9-C)
// ---------------------------------------------------------------------------

/// Port of `TupleOrderingTest.testChars`: per-char writes (16-bit BE)
/// produce a monotone-byte ordering by lexicographic char order.
/// Adapted: noxu has only `write_char` (single u16); we loop instead
/// of using JE's `writeChars(char[])`.
#[test]
fn tck_tuple_ordering_test_chars() {
    let data: &[&[u16]] = &[
        &[],
        &[0],
        &[b'a' as u16],
        &[b'a' as u16, 0],
        &[b'a' as u16, b'b' as u16],
        &[b'b' as u16],
        &[b'b' as u16, b'b' as u16],
        &[0x7F],
        &[0x7F, 0],
        &[0xFF],
        &[0xFF, 0],
    ];
    let mut prev: Option<Vec<u8>> = None;
    for (i, &v) in data.iter().enumerate() {
        let mut out = TupleOutput::new();
        for &c in v {
            out.write_char(c);
        }
        let next = out.as_bytes().to_vec();
        if let Some(prev) = &prev {
            assert_lt_bytes(prev, &next, i);
        }
        prev = Some(next);
    }
}

/// Port of `TupleOrderingTest.testBytes`: writing raw bytes preserves
/// lexicographic byte ordering.
#[test]
fn tck_tuple_ordering_test_bytes() {
    let data: &[&[u8]] =
        &[&[], &[0], b"a", &[b'a', 0], b"ab", b"b", b"bb", &[0x7F], &[0xFF]];
    let mut prev: Option<Vec<u8>> = None;
    for (i, &v) in data.iter().enumerate() {
        let mut out = TupleOutput::new();
        out.write_bytes(v);
        let next = out.as_bytes().to_vec();
        if let Some(prev) = &prev {
            assert_lt_bytes(prev, &next, i);
        }
        prev = Some(next);
    }
}

/// Port of `TupleOrderingTest.testPackedIntAndLong`: documents the JE
/// contract that the *unsorted* packed encoding sorts correctly only
/// across small non-negative integers (0..=630).  Full-range monotone
/// ordering requires `write_sorted_packed_*`.
#[test]
fn tck_tuple_ordering_test_packed_int_and_long() {
    let mut prev: Option<Vec<u8>> = None;
    for i in 0..=630i32 {
        let mut out = TupleOutput::new();
        out.write_packed_int(i);
        let next = out.as_bytes().to_vec();
        if let Some(prev) = &prev {
            assert_lt_bytes(prev, &next, i as usize);
        }
        prev = Some(next);
    }

    let mut prev: Option<Vec<u8>> = None;
    for i in 0..=630i64 {
        let mut out = TupleOutput::new();
        out.write_packed_long(i);
        let next = out.as_bytes().to_vec();
        if let Some(prev) = &prev {
            assert_lt_bytes(prev, &next, i as usize);
        }
        prev = Some(next);
    }
}

// ---------------------------------------------------------------------------
// TupleBindingTest: primitive object<->entry round-trip (wave 9-C)
// ---------------------------------------------------------------------------

/// Port of `TupleBindingTest.testPrimitiveBindings`: every primitive
/// binding round-trips an object via `object_to_entry` /
/// `entry_to_object`, with the expected byte size.
///
/// Adapted: noxu has no BigInteger / BigDecimal bindings (OUT-OF-SCOPE).
/// The Java-`null` paths (`StringBinding.stringToEntry(null,...)`) are
/// also absent because noxu's `StringBinding` round-trips a `String`
/// without a null-marker.  Boolean, byte (u8), short (i16), int (i32),
/// long (i64), float, double, packed int/long, sorted float/double,
/// sorted packed int/long, char, and string are all exercised.
#[test]
fn tck_tuple_binding_test_primitive_bindings() {
    use noxu_bind::{
        BoolBinding, ByteBinding, CharBinding, DoubleBinding, EntryBinding,
        FloatBinding, IntBinding, LongBinding, PackedIntBinding,
        PackedLongBinding, ShortBinding, SortedDoubleBinding,
        SortedFloatBinding, SortedPackedIntBinding, SortedPackedLongBinding,
        StringBinding,
    };
    use noxu_db::DatabaseEntry;

    fn rt<B, T>(binding: &B, value: T, expected_size: usize)
    where
        B: EntryBinding<T>,
        T: PartialEq + std::fmt::Debug + Clone,
    {
        let mut entry = DatabaseEntry::new();
        binding.object_to_entry(&value, &mut entry).unwrap();
        assert_eq!(
            expected_size,
            entry.data().len(),
            "unexpected encoded size for {value:?}",
        );
        let got = binding.entry_to_object(&entry).unwrap();
        assert_eq!(value, got);
    }

    // String: "abc" -> 3 bytes payload + 2-byte terminator (noxu).
    rt(&StringBinding::new(), "abc".to_string(), 3 + 2);

    // Char (u16) -> 2 bytes BE.
    rt(&CharBinding::new(), b'a' as u16, 2);

    // Bool -> 1 byte.
    rt(&BoolBinding::new(), true, 1);
    rt(&BoolBinding::new(), false, 1);

    // Byte (u8) -> 1 byte.
    rt(&ByteBinding::new(), 123u8, 1);

    // Short (i16) -> 2 bytes.
    rt(&ShortBinding::new(), 123i16, 2);

    // Int (i32) -> 4 bytes.
    rt(&IntBinding::new(), 123i32, 4);

    // Long (i64) -> 8 bytes.
    rt(&LongBinding::new(), 123i64, 8);

    // Float -> 4 bytes.
    rt(&FloatBinding::new(), 123.123f32, 4);

    // Double -> 8 bytes.
    rt(&DoubleBinding::new(), 123.123f64, 8);

    // Sorted float/double -> 4/8 bytes.
    rt(&SortedFloatBinding::new(), 123.123f32, 4);
    rt(&SortedDoubleBinding::new(), 123.123f64, 8);

    // Packed int/long: actual encoding is variable-length.  Just
    // round-trip and assert decode(encode) == value.
    let int_b = PackedIntBinding::new();
    let long_b = PackedLongBinding::new();
    let mut e = DatabaseEntry::new();
    int_b.object_to_entry(&1234i32, &mut e).unwrap();
    assert_eq!(1234i32, int_b.entry_to_object(&e).unwrap());
    long_b.object_to_entry(&1234i64, &mut e).unwrap();
    assert_eq!(1234i64, long_b.entry_to_object(&e).unwrap());

    // Sorted packed int/long similarly.
    let sint_b = SortedPackedIntBinding::new();
    let slong_b = SortedPackedLongBinding::new();
    sint_b.object_to_entry(&1234i32, &mut e).unwrap();
    assert_eq!(1234i32, sint_b.entry_to_object(&e).unwrap());
    slong_b.object_to_entry(&1234i64, &mut e).unwrap();
    assert_eq!(1234i64, slong_b.entry_to_object(&e).unwrap());
}

// ---------------------------------------------------------------------------
// TupleBindingTest: TupleInputBinding-equivalent round-trip (wave 10-A)
// ---------------------------------------------------------------------------

/// Port of `TupleBindingTest.testTupleInputBinding`.
///
/// JE's `TupleInputBinding` is an `EntryBinding<TupleInput>` that copies
/// the input's underlying bytes into the entry verbatim, and on the way
/// back wraps the entry's bytes in a fresh `TupleInput`.
///
/// Noxu has no `TupleInputBinding` type, but `TupleInput::new(entry.data())`
/// is the same operation.  This test asserts the same invariant: writing a
/// string into a `TupleOutput`, copying its bytes into a `DatabaseEntry`,
/// and reading them back via `TupleInput` round-trips with no bytes left
/// over.  In noxu, `"abc"` encodes as 3 payload bytes + 2-byte terminator.
#[test]
fn tck_tuple_binding_test_tuple_input_binding() {
    use noxu_db::DatabaseEntry;

    let mut out = TupleOutput::new();
    out.write_string("abc");
    let mut entry = DatabaseEntry::new();
    entry.set_data_vec(out.into_vec());
    // JE asserts buffer.getSize() == 4 (3 + 1-byte terminator).
    // Noxu's terminator is 2 bytes — 3 + 2 = 5.
    assert_eq!(5, entry.data().len());

    // entryToObject equivalent: TupleInput wrapping the entry's bytes.
    let mut input = TupleInput::new(entry.data());
    assert_eq!("abc", input.read_string().unwrap());
    assert_eq!(0, input.available());
}

// ---------------------------------------------------------------------------
// TupleBindingTest: TupleMarshalledBinding-equivalent round-trip (wave 10-A)
// ---------------------------------------------------------------------------

/// A test analogue of JE's `MarshalledObject`: a value that knows how to
/// marshal itself into a `TupleOutput` and unmarshal from a `TupleInput`.
#[derive(Debug, PartialEq, Eq, Default, Clone)]
struct MarshalledData {
    data: String,
    index_key1: String,
    index_key2: String,
}

impl MarshalledData {
    fn marshal_data(&self, out: &mut TupleOutput) {
        out.write_string(&self.data);
        out.write_string(&self.index_key1);
        out.write_string(&self.index_key2);
    }

    fn unmarshal_data(input: &mut TupleInput) -> Self {
        let data = input.read_string().unwrap();
        let index_key1 = input.read_string().unwrap();
        let index_key2 = input.read_string().unwrap();
        Self { data, index_key1, index_key2 }
    }

    /// Expected encoded size: each string contributes len + 2 bytes
    /// (3 components encoded back-to-back).
    fn expected_data_length(&self) -> usize {
        self.data.len()
            + 2
            + self.index_key1.len()
            + 2
            + self.index_key2.len()
            + 2
    }
}

/// Port of `TupleBindingTest.testTupleMarshalledBinding`.
///
/// Builds a `TupleBinding<MarshalledData>` whose `object_to_tuple` /
/// `tuple_to_object` delegate to the marshalled value's own marshal /
/// unmarshal methods.  Asserts:
///   1. encoded size matches `expected_data_length`,
///   2. round-trip preserves the `data` field (and the rest).
#[test]
fn tck_tuple_binding_test_tuple_marshalled_binding() {
    use noxu_bind::{EntryBinding, TupleBinding};
    use noxu_db::DatabaseEntry;

    struct MarshalledBinding;
    impl EntryBinding<MarshalledData> for MarshalledBinding {
        fn entry_to_object(
            &self,
            entry: &DatabaseEntry,
        ) -> noxu_bind::Result<MarshalledData> {
            let mut input = TupleInput::new(entry.data());
            self.tuple_to_object(&mut input)
        }
        fn object_to_entry(
            &self,
            object: &MarshalledData,
            entry: &mut DatabaseEntry,
        ) -> noxu_bind::Result<()> {
            let mut out = TupleOutput::new();
            self.object_to_tuple(object, &mut out)?;
            entry.set_data_vec(out.into_vec());
            Ok(())
        }
    }
    impl TupleBinding<MarshalledData> for MarshalledBinding {
        fn tuple_to_object(
            &self,
            input: &mut TupleInput,
        ) -> noxu_bind::Result<MarshalledData> {
            Ok(MarshalledData::unmarshal_data(input))
        }
        fn object_to_tuple(
            &self,
            object: &MarshalledData,
            output: &mut TupleOutput,
        ) -> noxu_bind::Result<()> {
            object.marshal_data(output);
            Ok(())
        }
    }

    let val = MarshalledData {
        data: "abc".to_string(),
        index_key1: String::new(),
        index_key2: String::new(),
    };
    let binding = MarshalledBinding;
    let mut entry = DatabaseEntry::new();
    binding.object_to_entry(&val, &mut entry).unwrap();
    assert_eq!(val.expected_data_length(), entry.data().len());

    let got = binding.entry_to_object(&entry).unwrap();
    assert_eq!("abc", got.data);
    assert_eq!(val, got);
}

// ---------------------------------------------------------------------------
// TupleBindingTest: TupleTupleMarshalledBinding-equivalent (wave 10-A)
// ---------------------------------------------------------------------------

/// Marshalled entity with a separate primary key field.  Mirrors JE's
/// `MarshalledObject` when used through `TupleTupleMarshalledBinding`.
#[derive(Debug, PartialEq, Eq, Default, Clone)]
struct MarshalledEntity {
    data: String,
    primary_key: String,
    index_key1: String,
    index_key2: String,
}

impl MarshalledEntity {
    fn expected_data_length(&self) -> usize {
        self.data.len()
            + 2
            + self.index_key1.len()
            + 2
            + self.index_key2.len()
            + 2
    }
    fn expected_key_length(&self) -> usize {
        self.primary_key.len() + 2
    }
}

/// Port of `TupleBindingTest.testTupleTupleMarshalledBinding`.
///
/// Defines an `EntityBinding<MarshalledEntity>` that splits the value across
/// a `key` entry (primary key) and a `data` entry (data + index keys).
/// Asserts:
///   1. encoded data size matches `expected_data_length`,
///   2. encoded key size matches `expected_key_length`,
///   3. round-trip preserves all four fields.
#[test]
fn tck_tuple_binding_test_tuple_tuple_marshalled_binding() {
    use noxu_bind::EntityBinding;
    use noxu_db::DatabaseEntry;

    struct MarshalledEntityBinding;
    impl EntityBinding<MarshalledEntity> for MarshalledEntityBinding {
        fn entry_to_object(
            &self,
            key: &DatabaseEntry,
            data: &DatabaseEntry,
        ) -> noxu_bind::Result<MarshalledEntity> {
            let mut k = TupleInput::new(key.data());
            let mut d = TupleInput::new(data.data());
            let primary_key = k.read_string().unwrap();
            let dat = d.read_string().unwrap();
            let i1 = d.read_string().unwrap();
            let i2 = d.read_string().unwrap();
            Ok(MarshalledEntity {
                data: dat,
                primary_key,
                index_key1: i1,
                index_key2: i2,
            })
        }
        fn object_to_key(
            &self,
            object: &MarshalledEntity,
            key: &mut DatabaseEntry,
        ) -> noxu_bind::Result<()> {
            let mut out = TupleOutput::new();
            out.write_string(&object.primary_key);
            key.set_data_vec(out.into_vec());
            Ok(())
        }
        fn object_to_data(
            &self,
            object: &MarshalledEntity,
            data: &mut DatabaseEntry,
        ) -> noxu_bind::Result<()> {
            let mut out = TupleOutput::new();
            out.write_string(&object.data);
            out.write_string(&object.index_key1);
            out.write_string(&object.index_key2);
            data.set_data_vec(out.into_vec());
            Ok(())
        }
    }

    let val = MarshalledEntity {
        data: "abc".to_string(),
        primary_key: "primary".to_string(),
        index_key1: "index1".to_string(),
        index_key2: "index2".to_string(),
    };
    let binding = MarshalledEntityBinding;
    let mut key_entry = DatabaseEntry::new();
    let mut data_entry = DatabaseEntry::new();
    binding.object_to_data(&val, &mut data_entry).unwrap();
    assert_eq!(val.expected_data_length(), data_entry.data().len());
    binding.object_to_key(&val, &mut key_entry).unwrap();
    assert_eq!(val.expected_key_length(), key_entry.data().len());

    let got = binding.entry_to_object(&key_entry, &data_entry).unwrap();
    assert_eq!("abc", got.data);
    assert_eq!("primary", got.primary_key);
    assert_eq!("index1", got.index_key1);
    assert_eq!("index2", got.index_key2);
    assert_eq!(val, got);
}
