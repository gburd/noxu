//! Sort-preserving key encoding trait for database keys.
//!
//! Types that implement `SortKey` can be encoded to bytes such that the
//! lexicographic byte order of the encoded form exactly matches the natural
//! ordering of the original values. This is the required property for B-tree
//! keys: byte-wise comparison of serialized keys must agree with `Ord` on the
//! original values.
//!
//! ## Encoding rules
//!
//! | Rust type  | Width    | Encoding                                             |
//! |------------|----------|------------------------------------------------------|
//! | `bool`     | 1 byte   | `false` → 0x00, `true` → 0x01                       |
//! | `u8`       | 1 byte   | raw value                                            |
//! | `i8`       | 1 byte   | value XOR 0x80 (sign-bit flip)                       |
//! | `u16`      | 2 bytes  | big-endian                                           |
//! | `i16`      | 2 bytes  | big-endian, sign-bit flipped                         |
//! | `u32`      | 4 bytes  | big-endian                                           |
//! | `i32`      | 4 bytes  | big-endian, sign-bit flipped                         |
//! | `u64`      | 8 bytes  | big-endian                                           |
//! | `i64`      | 8 bytes  | big-endian, sign-bit flipped                         |
//! | `f32`      | 4 bytes  | IEEE 754 with sign-conditional bit-flip              |
//! | `f64`      | 8 bytes  | IEEE 754 with sign-conditional bit-flip              |
//! | `String`   | variable | UTF-8 bytes, null-escaped, two-byte `[0x00,0x00]` terminator |
//! | `Vec<u8>`  | variable | raw bytes, null-escaped, two-byte `[0x00,0x00]` terminator   |
//!
//! The `f32`/`f64` encoding uses the same IEEE 754 sign-bit manipulation as
//! `TupleOutput::write_sorted_float`/`write_sorted_double`:
//! - Negative values: all bits XOR'd (sort before positive)
//! - Positive values: only the sign bit XOR'd (sort after negative)
//!
//! Variable-length types (`String`, `Vec<u8>`) use null-byte escaping so that
//! composite keys containing multiple variable-length fields remain sortable
//! and self-delimiting: each embedded `0x00` byte is written as `[0x00, 0x01]`
//! and the field is terminated with `[0x00, 0x00]`.

use crate::error::{BindError, Result};
use crate::tuple::tuple_input::TupleInput;
use crate::tuple::tuple_output::TupleOutput;

/// Trait for types whose values can be encoded to and decoded from a
/// sort-preserving byte representation.
///
/// The encoded bytes must satisfy: for any two values `a` and `b` of the same
/// type, `encode(a) < encode(b)` (lexicographically) if and only if `a < b`.
///
/// Implementations must be consistent with `Ord` for the type.
pub trait SortKey: Sized {
    /// Writes the sort-preserving encoding of `self` into `output`.
    fn encode_sort_key(&self, output: &mut TupleOutput);

    /// Reads a value from `input` that was written by `encode_sort_key`.
    fn decode_sort_key(input: &mut TupleInput) -> Result<Self>;
}

impl SortKey for bool {
    fn encode_sort_key(&self, output: &mut TupleOutput) {
        output.write_bool(*self);
    }
    fn decode_sort_key(input: &mut TupleInput) -> Result<Self> {
        input.read_bool()
    }
}

impl SortKey for u8 {
    fn encode_sort_key(&self, output: &mut TupleOutput) {
        output.write_u8(*self);
    }
    fn decode_sort_key(input: &mut TupleInput) -> Result<Self> {
        input.read_u8()
    }
}

impl SortKey for i8 {
    fn encode_sort_key(&self, output: &mut TupleOutput) {
        output.write_i8(*self);
    }
    fn decode_sort_key(input: &mut TupleInput) -> Result<Self> {
        input.read_i8()
    }
}

impl SortKey for u16 {
    fn encode_sort_key(&self, output: &mut TupleOutput) {
        output.write_u16(*self);
    }
    fn decode_sort_key(input: &mut TupleInput) -> Result<Self> {
        input.read_u16()
    }
}

impl SortKey for i16 {
    fn encode_sort_key(&self, output: &mut TupleOutput) {
        output.write_i16(*self);
    }
    fn decode_sort_key(input: &mut TupleInput) -> Result<Self> {
        input.read_i16()
    }
}

impl SortKey for u32 {
    fn encode_sort_key(&self, output: &mut TupleOutput) {
        output.write_u32(*self);
    }
    fn decode_sort_key(input: &mut TupleInput) -> Result<Self> {
        input.read_u32()
    }
}

impl SortKey for i32 {
    fn encode_sort_key(&self, output: &mut TupleOutput) {
        output.write_i32(*self);
    }
    fn decode_sort_key(input: &mut TupleInput) -> Result<Self> {
        input.read_i32()
    }
}

impl SortKey for u64 {
    fn encode_sort_key(&self, output: &mut TupleOutput) {
        output.write_u64(*self);
    }
    fn decode_sort_key(input: &mut TupleInput) -> Result<Self> {
        input.read_u64()
    }
}

impl SortKey for i64 {
    fn encode_sort_key(&self, output: &mut TupleOutput) {
        output.write_i64(*self);
    }
    fn decode_sort_key(input: &mut TupleInput) -> Result<Self> {
        input.read_i64()
    }
}

/// `f32` keys use the sort-preserving IEEE 754 encoding: negative values have
/// all bits flipped; positive values have only the sign bit flipped. This
/// ensures the full float range sorts correctly as unsigned bytes.
impl SortKey for f32 {
    fn encode_sort_key(&self, output: &mut TupleOutput) {
        output.write_sorted_float(*self);
    }
    fn decode_sort_key(input: &mut TupleInput) -> Result<Self> {
        input.read_sorted_float()
    }
}

/// `f64` keys use the sort-preserving IEEE 754 encoding: negative values have
/// all bits flipped; positive values have only the sign bit flipped.
impl SortKey for f64 {
    fn encode_sort_key(&self, output: &mut TupleOutput) {
        output.write_sorted_double(*self);
    }
    fn decode_sort_key(input: &mut TupleInput) -> Result<Self> {
        input.read_sorted_double()
    }
}

/// `String` keys are encoded as null-escaped UTF-8 followed by `[0x00, 0x00]`.
/// Embedded `0x00` bytes are escaped as `[0x00, 0x01]`. Lexicographic byte
/// order of encoded strings matches lexicographic string order.
impl SortKey for String {
    fn encode_sort_key(&self, output: &mut TupleOutput) {
        output.write_string(self.as_str());
    }
    fn decode_sort_key(input: &mut TupleInput) -> Result<Self> {
        input.read_string()
    }
}

/// `Vec<u8>` keys are encoded as null-escaped raw bytes followed by `[0x00,
/// 0x00]`. Embedded `0x00` bytes are escaped as `[0x00, 0x01]`.
/// Lexicographic byte order of encoded `Vec<u8>` values matches
/// lexicographic byte-slice order.
impl SortKey for Vec<u8> {
    fn encode_sort_key(&self, output: &mut TupleOutput) {
        // Null-escape the bytes, then write the two-byte terminator.
        for &b in self.iter() {
            if b == 0x00 {
                output.write_bytes(&[0x00, 0x01]);
            } else {
                output.write_bytes(&[b]);
            }
        }
        output.write_bytes(&[0x00, 0x00]);
    }
    fn decode_sort_key(input: &mut TupleInput) -> Result<Self> {
        let mut decoded: Vec<u8> = Vec::new();
        loop {
            let buf = input.get_buffer();
            let off = input.get_offset();
            if off >= buf.len() {
                return Err(BindError::InvalidData(
                    "no null terminator found for Vec<u8> key".to_string(),
                ));
            }
            let b = buf[off];
            input.skip(1)?;
            if b == 0x00 {
                let buf2 = input.get_buffer();
                let off2 = input.get_offset();
                if off2 >= buf2.len() {
                    return Err(BindError::InvalidData(
                        "truncated null escape in Vec<u8> key".to_string(),
                    ));
                }
                let next = buf2[off2];
                input.skip(1)?;
                if next == 0x00 {
                    break;
                } else if next == 0x01 {
                    decoded.push(0x00);
                } else {
                    return Err(BindError::InvalidData(format!(
                        "invalid null escape byte 0x{:02x} in Vec<u8> key",
                        next
                    )));
                }
            } else {
                decoded.push(b);
            }
        }
        Ok(decoded)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode<T: SortKey>(val: &T) -> Vec<u8> {
        let mut out = TupleOutput::new();
        val.encode_sort_key(&mut out);
        out.into_vec()
    }

    fn decode<T: SortKey>(bytes: &[u8]) -> T {
        let mut inp = TupleInput::new(bytes);
        T::decode_sort_key(&mut inp).unwrap()
    }

    fn round_trip<T: SortKey + PartialEq + std::fmt::Debug>(val: T) -> T {
        let encoded = encode(&val);
        decode(&encoded)
    }

    // --- round-trip tests ---

    #[test]
    fn test_bool_round_trip() {
        assert_eq!(round_trip(false), false);
        assert_eq!(round_trip(true), true);
    }

    #[test]
    fn test_u8_round_trip() { assert_eq!(round_trip(0u8), 0); assert_eq!(round_trip(255u8), 255); }
    #[test]
    fn test_i8_round_trip() { assert_eq!(round_trip(i8::MIN), i8::MIN); assert_eq!(round_trip(0i8), 0); assert_eq!(round_trip(i8::MAX), i8::MAX); }
    #[test]
    fn test_u16_round_trip() { assert_eq!(round_trip(0u16), 0); assert_eq!(round_trip(u16::MAX), u16::MAX); }
    #[test]
    fn test_i16_round_trip() { assert_eq!(round_trip(i16::MIN), i16::MIN); assert_eq!(round_trip(0i16), 0); assert_eq!(round_trip(i16::MAX), i16::MAX); }
    #[test]
    fn test_u32_round_trip() { assert_eq!(round_trip(0u32), 0); assert_eq!(round_trip(u32::MAX), u32::MAX); }
    #[test]
    fn test_i32_round_trip() { assert_eq!(round_trip(i32::MIN), i32::MIN); assert_eq!(round_trip(0i32), 0); assert_eq!(round_trip(i32::MAX), i32::MAX); }
    #[test]
    fn test_u64_round_trip() { assert_eq!(round_trip(0u64), 0); assert_eq!(round_trip(u64::MAX), u64::MAX); }
    #[test]
    fn test_i64_round_trip() { assert_eq!(round_trip(i64::MIN), i64::MIN); assert_eq!(round_trip(0i64), 0); assert_eq!(round_trip(i64::MAX), i64::MAX); }
    #[test]
    fn test_f32_round_trip() {
        for &v in &[0.0f32, 1.5, -1.5, f32::MAX, f32::MIN] {
            assert_eq!(round_trip(v).to_bits(), v.to_bits());
        }
        assert!(round_trip(f32::NAN).is_nan());
    }
    #[test]
    fn test_f64_round_trip() {
        for &v in &[0.0f64, 1.5, -1.5, f64::MAX, f64::MIN] {
            assert_eq!(round_trip(v).to_bits(), v.to_bits());
        }
        assert!(round_trip(f64::NAN).is_nan());
    }
    #[test]
    fn test_string_round_trip() {
        assert_eq!(round_trip("".to_string()), "");
        assert_eq!(round_trip("hello".to_string()), "hello");
        let with_null = "a\x00b".to_string();
        assert_eq!(round_trip(with_null.clone()), with_null);
    }
    #[test]
    fn test_vec_u8_round_trip() {
        assert_eq!(round_trip(vec![]), Vec::<u8>::new());
        assert_eq!(round_trip(vec![1u8, 2, 3]), vec![1, 2, 3]);
        assert_eq!(round_trip(vec![0x00u8, 0x01, 0x00]), vec![0x00, 0x01, 0x00]);
    }

    // --- sort-order tests ---

    fn assert_order<T: SortKey>(lesser: T, greater: T) {
        assert!(
            encode(&lesser) < encode(&greater),
            "expected encode({:?}) < encode({:?})",
            encode(&lesser),
            encode(&greater)
        );
    }

    #[test]
    fn test_sort_order_u64() {
        for (a, b) in [(0u64, 1), (1, 10), (100, 1000), (u64::MAX - 1, u64::MAX)] {
            assert_order(a, b);
        }
    }
    #[test]
    fn test_sort_order_i64() {
        let vals = [i64::MIN, -1000i64, -1, 0, 1, 1000, i64::MAX];
        for w in vals.windows(2) { assert_order(w[0], w[1]); }
    }
    #[test]
    fn test_sort_order_u32() {
        for (a, b) in [(0u32, 1), (1, 100), (u32::MAX - 1, u32::MAX)] {
            assert_order(a, b);
        }
    }
    #[test]
    fn test_sort_order_i32() {
        let vals = [i32::MIN, -1i32, 0, 1, i32::MAX];
        for w in vals.windows(2) { assert_order(w[0], w[1]); }
    }
    #[test]
    fn test_sort_order_string() {
        assert_order("a".to_string(), "b".to_string());
        assert_order("a".to_string(), "aa".to_string());
        assert_order("abc".to_string(), "abd".to_string());
    }
    #[test]
    fn test_sort_order_vec_u8() {
        assert_order(vec![0x01u8], vec![0x02u8]);
        assert_order(vec![0x01u8], vec![0x01u8, 0x00]);
        assert_order(vec![0xFEu8], vec![0xFFu8]);
    }
    #[test]
    fn test_sort_order_f64() {
        let vals = [f64::NEG_INFINITY, -1.0f64, 0.0, 1.0, f64::INFINITY];
        for w in vals.windows(2) { assert_order(w[0], w[1]); }
    }
    #[test]
    fn test_sort_order_i8() {
        let vals = [i8::MIN, -1i8, 0, 1, i8::MAX];
        for w in vals.windows(2) { assert_order(w[0], w[1]); }
    }
    #[test]
    fn test_sort_order_bool() {
        assert_order(false, true);
    }
}
