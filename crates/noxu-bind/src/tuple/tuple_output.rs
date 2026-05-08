//! TupleOutput: writes primitive types to a growable buffer using sortable encodings.
//!

use noxu_db::DatabaseEntry;
use noxu_util::packed::{write_sorted_i32, write_sorted_i64};

/// A writer for tuple-encoded byte data.
///
/// Writes primitive values to a growable byte buffer using the same encoding formats
/// as `TupleOutput`. Signed integers use big-endian with the sign bit
/// flipped for sortable ordering. Floats use IEEE 754 with bit manipulation
/// for sortable ordering.
///
/// 
#[derive(Debug, Clone)]
pub struct TupleOutput {
    buf: Vec<u8>,
}

impl TupleOutput {
    /// Creates a new empty `TupleOutput`.
    pub fn new() -> Self {
        Self { buf: Vec::with_capacity(32) }
    }

    /// Creates a new `TupleOutput` with the given initial capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self { buf: Vec::with_capacity(capacity) }
    }

    /// Returns the written bytes as a vector.
    pub fn to_vec(&self) -> Vec<u8> {
        self.buf.clone()
    }

    /// Consumes self and returns the written bytes.
    pub fn into_vec(self) -> Vec<u8> {
        self.buf
    }

    /// Returns the current length of written data.
    pub fn len(&self) -> usize {
        self.buf.len()
    }

    /// Returns true if no data has been written.
    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    /// Converts the written data to a `DatabaseEntry`.
    pub fn to_database_entry(&self) -> DatabaseEntry {
        DatabaseEntry::from_vec(self.buf.clone())
    }

    /// Returns a reference to the internal buffer.
    pub fn as_bytes(&self) -> &[u8] {
        &self.buf
    }

    /// Writes a boolean (one byte) value. True is stored as 1, false as 0.
    ///
    /// Values can be read using `TupleInput::read_bool`.
    pub fn write_bool(&mut self, val: bool) {
        self.buf.push(if val { 1 } else { 0 });
    }

    /// Writes an unsigned byte value directly.
    ///
    /// Values can be read using `TupleInput::read_u8`.
    pub fn write_u8(&mut self, val: u8) {
        self.buf.push(val);
    }

    /// Writes a signed byte value with sign bit flipped for sort order.
    ///
    /// Values can be read using `TupleInput::read_i8`.
    pub fn write_i8(&mut self, val: i8) {
        self.buf.push((val as u8) ^ 0x80);
    }

    /// Writes an unsigned short (two byte, big-endian) value.
    ///
    /// Values can be read using `TupleInput::read_u16`.
    pub fn write_u16(&mut self, val: u16) {
        self.buf.push((val >> 8) as u8);
        self.buf.push(val as u8);
    }

    /// Writes a signed short (two byte) value with sign bit flipped for sort order.
    ///
    /// Values can be read using `TupleInput::read_i16`.
    pub fn write_i16(&mut self, val: i16) {
        let encoded = (val as u16) ^ 0x8000;
        self.buf.push((encoded >> 8) as u8);
        self.buf.push(encoded as u8);
    }

    /// Writes an unsigned int (four byte, big-endian) value.
    ///
    /// Values can be read using `TupleInput::read_u32`.
    pub fn write_u32(&mut self, val: u32) {
        self.buf.push((val >> 24) as u8);
        self.buf.push((val >> 16) as u8);
        self.buf.push((val >> 8) as u8);
        self.buf.push(val as u8);
    }

    /// Writes a signed int (four byte) value with sign bit flipped for sort order.
    ///
    /// Values can be read using `TupleInput::read_i32`.
    pub fn write_i32(&mut self, val: i32) {
        let encoded = (val as u32) ^ 0x80000000;
        self.buf.push((encoded >> 24) as u8);
        self.buf.push((encoded >> 16) as u8);
        self.buf.push((encoded >> 8) as u8);
        self.buf.push(encoded as u8);
    }

    /// Writes an unsigned long (eight byte, big-endian) value.
    ///
    /// Values can be read using `TupleInput::read_u64`.
    pub fn write_u64(&mut self, val: u64) {
        self.buf.push((val >> 56) as u8);
        self.buf.push((val >> 48) as u8);
        self.buf.push((val >> 40) as u8);
        self.buf.push((val >> 32) as u8);
        self.buf.push((val >> 24) as u8);
        self.buf.push((val >> 16) as u8);
        self.buf.push((val >> 8) as u8);
        self.buf.push(val as u8);
    }

    /// Writes a signed long (eight byte) value with sign bit flipped for sort order.
    ///
    /// Values can be read using `TupleInput::read_i64`.
    pub fn write_i64(&mut self, val: i64) {
        let encoded = (val as u64) ^ 0x8000000000000000;
        self.buf.push((encoded >> 56) as u8);
        self.buf.push((encoded >> 48) as u8);
        self.buf.push((encoded >> 40) as u8);
        self.buf.push((encoded >> 32) as u8);
        self.buf.push((encoded >> 24) as u8);
        self.buf.push((encoded >> 16) as u8);
        self.buf.push((encoded >> 8) as u8);
        self.buf.push(encoded as u8);
    }

    /// Writes an unsorted float (four byte) value as raw IEEE 754 big-endian bits.
    ///
    /// This does NOT produce sortable byte ordering. Use `write_sorted_float`
    /// for keys that need to sort correctly.
    ///
    /// Values can be read using `TupleInput::read_float`.
    pub fn write_float(&mut self, val: f32) {
        let bits = val.to_bits();
        self.write_u32(bits);
    }

    /// Writes an unsorted double (eight byte) value as raw IEEE 754 big-endian bits.
    ///
    /// This does NOT produce sortable byte ordering. Use `write_sorted_double`
    /// for keys that need to sort correctly.
    ///
    /// Values can be read using `TupleInput::read_double`.
    pub fn write_double(&mut self, val: f64) {
        let bits = val.to_bits();
        self.write_u64(bits);
    }

    /// Writes a sorted float (four byte) value using sign-bit manipulation.
    ///
    /// The encoding ensures that the byte representation sorts in the same
    /// order as the float values:
    /// - If negative (sign bit set): XOR all bits
    /// - If positive (sign bit clear): XOR only the sign bit
    ///
    /// Values can be read using `TupleInput::read_sorted_float`.
    pub fn write_sorted_float(&mut self, val: f32) {
        let bits = val.to_bits() as i32;
        let encoded = if bits < 0 {
            (bits as u32) ^ 0xFFFFFFFF
        } else {
            (bits as u32) ^ 0x80000000
        };
        self.write_u32(encoded);
    }

    /// Writes a sorted double (eight byte) value using sign-bit manipulation.
    ///
    /// The encoding ensures that the byte representation sorts in the same
    /// order as the double values:
    /// - If negative (sign bit set): XOR all bits
    /// - If positive (sign bit clear): XOR only the sign bit
    ///
    /// Values can be read using `TupleInput::read_sorted_double`.
    pub fn write_sorted_double(&mut self, val: f64) {
        let bits = val.to_bits() as i64;
        let encoded = if bits < 0 {
            (bits as u64) ^ 0xFFFFFFFFFFFFFFFF
        } else {
            (bits as u64) ^ 0x8000000000000000
        };
        self.write_u64(encoded);
    }

    /// Writes a packed (variable-length) i32 value.
    ///
    /// Values in [-119, 119] are stored in a single byte. Larger values use
    /// 2-5 bytes with the first byte encoding the sign and byte count.
    ///
    /// This is an unsorted encoding  -  it is compact but the byte representation
    /// does NOT sort in the same order as the integer values.
    ///
    /// 
    ///
    /// Values can be read using `TupleInput::read_packed_int`.
    pub fn write_packed_int(&mut self, value: i32) {
        if (-119..=119).contains(&value) {
            self.buf.push(value as u8);
            return;
        }

        let negative = value < -119;
        // Use i64 intermediate to avoid overflow when value == i32::MIN
        let adjusted: u32 = if negative {
            (-(value as i64) - 119) as u32
        } else {
            (value - 119) as u32
        };

        // Determine byte length needed
        let byte_len: u32 = if adjusted & 0xFFFFFF00 == 0 {
            1
        } else if adjusted & 0xFFFF0000 == 0 {
            2
        } else if adjusted & 0xFF000000 == 0 {
            3
        } else {
            4
        };

        // Write header byte
        let header: u8 = if negative {
            (-(119i16 + byte_len as i16)) as u8
        } else {
            (119 + byte_len) as u8
        };
        self.buf.push(header);

        // Write value bytes in little-endian order (matching JE)
        self.buf.push(adjusted as u8);
        if byte_len > 1 {
            self.buf.push((adjusted >> 8) as u8);
            if byte_len > 2 {
                self.buf.push((adjusted >> 16) as u8);
                if byte_len > 3 {
                    self.buf.push((adjusted >> 24) as u8);
                }
            }
        }
    }

    /// Writes a packed (variable-length) i64 value.
    ///
    /// Values in [-119, 119] are stored in a single byte. Larger values use
    /// 2-9 bytes with the first byte encoding the sign and byte count.
    ///
    /// This is an unsorted encoding  -  it is compact but the byte representation
    /// does NOT sort in the same order as the integer values.
    ///
    /// 
    ///
    /// Values can be read using `TupleInput::read_packed_long`.
    pub fn write_packed_long(&mut self, value: i64) {
        if (-119..=119).contains(&value) {
            self.buf.push(value as u8);
            return;
        }

        let negative = value < -119;
        // Use i128 intermediate to avoid overflow when value == i64::MIN
        let adjusted: u64 = if negative {
            (-(value as i128) - 119) as u64
        } else {
            (value - 119) as u64
        };

        // Determine byte length needed
        let byte_len = if adjusted & 0xFFFFFFFFFFFFFF00 == 0 {
            1
        } else if adjusted & 0xFFFFFFFFFFFF0000 == 0 {
            2
        } else if adjusted & 0xFFFFFFFFFF000000 == 0 {
            3
        } else if adjusted & 0xFFFFFFFF00000000 == 0 {
            4
        } else if adjusted & 0xFFFFFF0000000000 == 0 {
            5
        } else if adjusted & 0xFFFF000000000000 == 0 {
            6
        } else if adjusted & 0xFF00000000000000 == 0 {
            7
        } else {
            8
        };

        // Write header byte
        let header: u8 = if negative {
            (-(119i16 + byte_len as i16)) as u8
        } else {
            (119 + byte_len) as u8
        };
        self.buf.push(header);

        // Write value bytes in little-endian order (matching JE)
        self.buf.push(adjusted as u8);
        if byte_len > 1 {
            self.buf.push((adjusted >> 8) as u8);
            if byte_len > 2 {
                self.buf.push((adjusted >> 16) as u8);
                if byte_len > 3 {
                    self.buf.push((adjusted >> 24) as u8);
                    if byte_len > 4 {
                        self.buf.push((adjusted >> 32) as u8);
                        if byte_len > 5 {
                            self.buf.push((adjusted >> 40) as u8);
                            if byte_len > 6 {
                                self.buf.push((adjusted >> 48) as u8);
                                if byte_len > 7 {
                                    self.buf.push((adjusted >> 56) as u8);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /// Writes a null-escaped UTF-8 string using null-byte escape format.
    ///
    /// Each 0x00 byte in the string is escaped as the two-byte sequence
    /// [0x00, 0x01], and the string is terminated with [0x00, 0x00].
    /// This allows strings containing embedded null bytes to round-trip
    /// correctly and preserves lexicographic sort order.
    ///
    /// Values can be read using `TupleInput::read_string`.
    pub fn write_string(&mut self, val: &str) {
        for &b in val.as_bytes() {
            if b == 0x00 {
                self.buf.push(0x00);
                self.buf.push(0x01);
            } else {
                self.buf.push(b);
            }
        }
        // Null terminator: two-byte sequence [0x00, 0x00]
        self.buf.push(0x00);
        self.buf.push(0x00);
    }

    /// Writes raw bytes to the buffer without any framing.
    ///
    /// The caller must know the length when reading back.
    ///
    /// Values can be read using `TupleInput::read_bytes`.
    pub fn write_bytes(&mut self, data: &[u8]) {
        self.buf.extend_from_slice(data);
    }

    /// Writes a sorted packed (variable-length, order-preserving) i32 value.
    ///
    /// Values in [-119, 120] are stored in a single byte as `(value + 127)`.
    /// Larger positive values use 2-5 bytes with first byte `0xF7 + N`.
    /// Smaller negative values use 2-5 bytes with first byte `0x08 - N`.
    ///
    /// The byte representation sorts in the same order as the integer values,
    /// making this suitable for database keys. This is distinct from
    /// `write_packed_int`, which is compact but NOT sortable.
    ///
    /// / `TupleOutput.writeSortedPackedInt()`.
    ///
    /// Values can be read using `TupleInput::read_sorted_packed_int`.
    pub fn write_sorted_packed_int(&mut self, val: i32) {
        write_sorted_i32(&mut self.buf, val)
            .expect("write_sorted_i32 to Vec is infallible");
    }

    /// Writes a sorted packed (variable-length, order-preserving) i64 value.
    ///
    /// Values in [-119, 120] are stored in a single byte as `(value + 127)`.
    /// Larger positive values use 2-9 bytes with first byte `0xF7 + N`.
    /// Smaller negative values use 2-9 bytes with first byte `0x08 - N`.
    ///
    /// The byte representation sorts in the same order as the integer values,
    /// making this suitable for database keys. This is distinct from
    /// `write_packed_long`, which is compact but NOT sortable.
    ///
    /// / `TupleOutput.writeSortedPackedLong()`.
    ///
    /// Values can be read using `TupleInput::read_sorted_packed_long`.
    pub fn write_sorted_packed_long(&mut self, val: i64) {
        write_sorted_i64(&mut self.buf, val)
            .expect("write_sorted_i64 to Vec is infallible");
    }

    /// Writes a Java `char` (16-bit Unicode code point) as two big-endian bytes.
    ///
    /// The encoding is identical to an unsigned big-endian u16: the high byte
    /// first, then the low byte. This matches Java's `DataOutputStream.writeChar`.
    ///
    /// 
    ///
    /// Values can be read using `TupleInput::read_char`.
    pub fn write_char(&mut self, val: u16) {
        self.buf.push((val >> 8) as u8);
        self.buf.push(val as u8);
    }

    /// Resets the output, clearing all written data.
    pub fn reset(&mut self) {
        self.buf.clear();
    }
}

impl Default for TupleOutput {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let out = TupleOutput::new();
        assert!(out.is_empty());
        assert_eq!(out.len(), 0);
    }

    #[test]
    fn test_write_bool() {
        let mut out = TupleOutput::new();
        out.write_bool(true);
        out.write_bool(false);
        assert_eq!(out.to_vec(), vec![1, 0]);
    }

    #[test]
    fn test_write_i32_sort_order() {
        // Verify that encoded i32 values sort correctly as bytes
        let values = [i32::MIN, -1000, -1, 0, 1, 1000, i32::MAX];
        let encoded: Vec<Vec<u8>> = values
            .iter()
            .map(|&v| {
                let mut out = TupleOutput::new();
                out.write_i32(v);
                out.to_vec()
            })
            .collect();

        for i in 0..encoded.len() - 1 {
            assert!(
                encoded[i] < encoded[i + 1],
                "sort order violated: {} (encoded {:?}) should be < {} (encoded {:?})",
                values[i],
                encoded[i],
                values[i + 1],
                encoded[i + 1]
            );
        }
    }

    #[test]
    fn test_write_i64_sort_order() {
        let values = [i64::MIN, -1000, -1, 0, 1, 1000, i64::MAX];
        let encoded: Vec<Vec<u8>> = values
            .iter()
            .map(|&v| {
                let mut out = TupleOutput::new();
                out.write_i64(v);
                out.to_vec()
            })
            .collect();

        for i in 0..encoded.len() - 1 {
            assert!(
                encoded[i] < encoded[i + 1],
                "sort order violated: {} should be < {}",
                values[i],
                values[i + 1]
            );
        }
    }

    #[test]
    fn test_write_sorted_float_sort_order() {
        let values = [f32::MIN, -1.0, -0.5, 0.0, 0.5, 1.0, f32::MAX];
        let encoded: Vec<Vec<u8>> = values
            .iter()
            .map(|&v| {
                let mut out = TupleOutput::new();
                out.write_sorted_float(v);
                out.to_vec()
            })
            .collect();

        for i in 0..encoded.len() - 1 {
            assert!(
                encoded[i] < encoded[i + 1],
                "sort order violated: {} (encoded {:?}) should be < {} (encoded {:?})",
                values[i],
                encoded[i],
                values[i + 1],
                encoded[i + 1]
            );
        }
    }

    #[test]
    fn test_write_sorted_double_sort_order() {
        let values = [f64::MIN, -1.0, -0.5, 0.0, 0.5, 1.0, f64::MAX];
        let encoded: Vec<Vec<u8>> = values
            .iter()
            .map(|&v| {
                let mut out = TupleOutput::new();
                out.write_sorted_double(v);
                out.to_vec()
            })
            .collect();

        for i in 0..encoded.len() - 1 {
            assert!(
                encoded[i] < encoded[i + 1],
                "sort order violated: {} should be < {}",
                values[i],
                values[i + 1]
            );
        }
    }

    #[test]
    fn test_to_database_entry() {
        let mut out = TupleOutput::new();
        out.write_i32(42);
        let entry = out.to_database_entry();
        assert_eq!(entry.data().len(), 4);
    }

    #[test]
    fn test_reset() {
        let mut out = TupleOutput::new();
        out.write_i32(42);
        assert_eq!(out.len(), 4);
        out.reset();
        assert!(out.is_empty());
    }

    #[test]
    fn test_packed_int_single_byte() {
        // Values -119..=119 should be a single byte
        for v in -119..=119i32 {
            let mut out = TupleOutput::new();
            out.write_packed_int(v);
            assert_eq!(out.len(), 1, "value {} should be 1 byte", v);
        }
    }

    #[test]
    fn test_packed_int_multi_byte() {
        let mut out = TupleOutput::new();
        out.write_packed_int(120);
        assert!(out.len() > 1);

        let mut out = TupleOutput::new();
        out.write_packed_int(-120);
        assert!(out.len() > 1);
    }

    // -----------------------------------------------------------------------
    // Ported from TupleFormatTest: exact byte sizes per type
    // -----------------------------------------------------------------------

    /// TupleFormatTest: bool is 1 byte.
    #[test]
    fn test_format_bool_size() {
        let mut out = TupleOutput::new();
        out.write_bool(true);
        assert_eq!(out.len(), 1);
        out.write_bool(false);
        assert_eq!(out.len(), 2);
    }

    /// TupleFormatTest: u8 / i8 are 1 byte.
    #[test]
    fn test_format_byte_sizes() {
        let mut out = TupleOutput::new();
        out.write_u8(123);
        assert_eq!(out.len(), 1);
        let mut out = TupleOutput::new();
        out.write_i8(-1);
        assert_eq!(out.len(), 1);
    }

    /// TupleFormatTest: u16 / i16 are 2 bytes.
    #[test]
    fn test_format_short_sizes() {
        let mut out = TupleOutput::new();
        out.write_u16(0xFFFF);
        assert_eq!(out.len(), 2);
        let mut out = TupleOutput::new();
        out.write_i16(-1);
        assert_eq!(out.len(), 2);
    }

    /// TupleFormatTest: u32 / i32 are 4 bytes.
    #[test]
    fn test_format_int_sizes() {
        let mut out = TupleOutput::new();
        out.write_u32(0xFFFF_FFFF);
        assert_eq!(out.len(), 4);
        let mut out = TupleOutput::new();
        out.write_i32(123);
        assert_eq!(out.len(), 4);
    }

    /// TupleFormatTest: u64 / i64 are 8 bytes.
    #[test]
    fn test_format_long_sizes() {
        let mut out = TupleOutput::new();
        out.write_u64(123);
        assert_eq!(out.len(), 8);
        let mut out = TupleOutput::new();
        out.write_i64(123);
        assert_eq!(out.len(), 8);
    }

    /// TupleFormatTest: f32 is 4 bytes (both sorted and unsorted).
    #[test]
    fn test_format_float_sizes() {
        let mut out = TupleOutput::new();
        out.write_float(123.123);
        assert_eq!(out.len(), 4);
        let mut out = TupleOutput::new();
        out.write_sorted_float(123.123);
        assert_eq!(out.len(), 4);
    }

    /// TupleFormatTest: f64 is 8 bytes (both sorted and unsorted).
    #[test]
    fn test_format_double_sizes() {
        let mut out = TupleOutput::new();
        out.write_double(123.123);
        assert_eq!(out.len(), 8);
        let mut out = TupleOutput::new();
        out.write_sorted_double(123.123);
        assert_eq!(out.len(), 8);
    }

    /// TupleFormatTest: char (write_char) is 2 bytes.
    #[test]
    fn test_format_char_size() {
        let mut out = TupleOutput::new();
        out.write_char(b'a' as u16);
        assert_eq!(out.len(), 2);
    }

    /// TupleFormatTest: null-terminated string "abc" is len+1 = 4 bytes.
    #[test]
    fn test_format_string_size() {
        let mut out = TupleOutput::new();
        out.write_string("abc");
        // 3 chars + 2-byte null terminator [0x00, 0x00] = 5 bytes
        // But writes each char as 1 byte then a 1-byte null terminator.
        // Our format writes UTF-8 bytes + [0x00, 0x00] terminator.
        // "abc" → 3 bytes + 2 bytes terminator = 5 bytes total.
        // However TupleFormatTest expects 4 for "abc", because uses
        // a SINGLE null byte terminator for ASCII-range strings.
        // Our Rust implementation uses a 2-byte null terminator to allow
        // embedded nulls, so "abc" → 5 bytes.
        assert_eq!(out.len(), 5); // "abc" + [0x00, 0x00]
    }

    /// TupleFormatTest: empty string is just the terminator.
    #[test]
    fn test_format_empty_string_size() {
        let mut out = TupleOutput::new();
        out.write_string("");
        assert_eq!(out.len(), 2); // [0x00, 0x00] terminator only
    }

    /// TupleFormatTest: multiple strings written sequentially accumulate size.
    #[test]
    fn test_format_multi_string_size() {
        let mut out = TupleOutput::new();
        out.write_string("abc");  // 3 + 2 = 5
        out.write_string("defg"); // 4 + 2 = 6 → total 11
        assert_eq!(out.len(), 11);
    }

    /// TupleFormatTest: three booleans accumulate correctly.
    #[test]
    fn test_format_multi_bool_size() {
        let mut out = TupleOutput::new();
        out.write_bool(true);
        out.write_bool(false);
        out.write_bool(true);
        assert_eq!(out.len(), 3);
    }

    /// TupleFormatTest: three i32 values accumulate to 3*4 bytes.
    #[test]
    fn test_format_multi_int_size() {
        let mut out = TupleOutput::new();
        out.write_i32(0);
        out.write_i32(1);
        out.write_i32(-1);
        assert_eq!(out.len(), 12);
    }

    /// TupleFormatTest: three i64 values accumulate to 3*8 bytes.
    #[test]
    fn test_format_multi_long_size() {
        let mut out = TupleOutput::new();
        out.write_i64(0);
        out.write_i64(1);
        out.write_i64(-1);
        assert_eq!(out.len(), 24);
    }

    /// TupleFormatTest: three f32 values accumulate to 3*4 bytes.
    #[test]
    fn test_format_multi_float_size() {
        let mut out = TupleOutput::new();
        out.write_float(0.0);
        out.write_float(1.0);
        out.write_float(-1.0);
        assert_eq!(out.len(), 12);
    }

    /// TupleFormatTest: three f64 values accumulate to 3*8 bytes.
    #[test]
    fn test_format_multi_double_size() {
        let mut out = TupleOutput::new();
        out.write_double(0.0);
        out.write_double(1.0);
        out.write_double(-1.0);
        assert_eq!(out.len(), 24);
    }

    // -----------------------------------------------------------------------
    // Ported from TupleOrderingTest: ordering checks for all types
    // -----------------------------------------------------------------------

    /// Helper: encode a single value to bytes.
    fn encode_i8(v: i8) -> Vec<u8> {
        let mut out = TupleOutput::new();
        out.write_i8(v);
        out.to_vec()
    }
    fn encode_i16(v: i16) -> Vec<u8> {
        let mut out = TupleOutput::new();
        out.write_i16(v);
        out.to_vec()
    }
    fn encode_i32(v: i32) -> Vec<u8> {
        let mut out = TupleOutput::new();
        out.write_i32(v);
        out.to_vec()
    }
    fn encode_i64(v: i64) -> Vec<u8> {
        let mut out = TupleOutput::new();
        out.write_i64(v);
        out.to_vec()
    }
    fn encode_u8(v: u8) -> Vec<u8> {
        let mut out = TupleOutput::new();
        out.write_u8(v);
        out.to_vec()
    }
    fn encode_u16(v: u16) -> Vec<u8> {
        let mut out = TupleOutput::new();
        out.write_u16(v);
        out.to_vec()
    }
    fn encode_u32(v: u32) -> Vec<u8> {
        let mut out = TupleOutput::new();
        out.write_u32(v);
        out.to_vec()
    }
    fn encode_f32_sorted(v: f32) -> Vec<u8> {
        let mut out = TupleOutput::new();
        out.write_sorted_float(v);
        out.to_vec()
    }
    fn encode_f64_sorted(v: f64) -> Vec<u8> {
        let mut out = TupleOutput::new();
        out.write_sorted_double(v);
        out.to_vec()
    }
    fn encode_f32(v: f32) -> Vec<u8> {
        let mut out = TupleOutput::new();
        out.write_float(v);
        out.to_vec()
    }
    fn encode_f64(v: f64) -> Vec<u8> {
        let mut out = TupleOutput::new();
        out.write_double(v);
        out.to_vec()
    }

    /// TupleOrderingTest.testByte: signed byte full ordering.
    #[test]
    fn test_ordering_i8_full_boundary() {
        let data: &[i8] = &[
            i8::MIN, i8::MIN + 1, -1, 0, 1, i8::MAX - 1, i8::MAX,
        ];
        for i in 0..data.len() - 1 {
            assert!(
                encode_i8(data[i]) < encode_i8(data[i + 1]),
                "i8 ordering violated: {} should be < {}",
                data[i], data[i + 1]
            );
        }
    }

    /// TupleOrderingTest.testShort: signed short full ordering.
    #[test]
    fn test_ordering_i16_full_boundary() {
        let data: &[i16] = &[
            i16::MIN, i16::MIN + 1,
            i8::MIN as i16, i8::MIN as i16 + 1,
            -1, 0, 1,
            i8::MAX as i16 - 1, i8::MAX as i16,
            i16::MAX - 1, i16::MAX,
        ];
        for i in 0..data.len() - 1 {
            assert!(
                encode_i16(data[i]) < encode_i16(data[i + 1]),
                "i16 ordering violated: {} should be < {}",
                data[i], data[i + 1]
            );
        }
    }

    /// TupleOrderingTest.testInt: signed int full ordering.
    #[test]
    fn test_ordering_i32_full_boundary() {
        let data: &[i32] = &[
            i32::MIN, i32::MIN + 1,
            i16::MIN as i32, i16::MIN as i32 + 1,
            i8::MIN as i32, i8::MIN as i32 + 1,
            -1, 0, 1,
            i8::MAX as i32 - 1, i8::MAX as i32,
            i16::MAX as i32 - 1, i16::MAX as i32,
            i32::MAX - 1, i32::MAX,
        ];
        for i in 0..data.len() - 1 {
            assert!(
                encode_i32(data[i]) < encode_i32(data[i + 1]),
                "i32 ordering violated: {} should be < {}",
                data[i], data[i + 1]
            );
        }
    }

    /// TupleOrderingTest.testLong: signed long full ordering.
    #[test]
    fn test_ordering_i64_full_boundary() {
        let data: &[i64] = &[
            i64::MIN, i64::MIN + 1,
            i32::MIN as i64, i32::MIN as i64 + 1,
            i16::MIN as i64, i16::MIN as i64 + 1,
            i8::MIN as i64, i8::MIN as i64 + 1,
            -1, 0, 1,
            i8::MAX as i64 - 1, i8::MAX as i64,
            i16::MAX as i64 - 1, i16::MAX as i64,
            i32::MAX as i64 - 1, i32::MAX as i64,
            i64::MAX - 1, i64::MAX,
        ];
        for i in 0..data.len() - 1 {
            assert!(
                encode_i64(data[i]) < encode_i64(data[i + 1]),
                "i64 ordering violated: {} should be < {}",
                data[i], data[i + 1]
            );
        }
    }

    /// TupleOrderingTest.testUnsignedByte: unsigned byte ordering.
    #[test]
    fn test_ordering_u8_full() {
        let data: &[u8] = &[0, 1, 0x7F, 0xFF];
        for i in 0..data.len() - 1 {
            assert!(
                encode_u8(data[i]) < encode_u8(data[i + 1]),
                "u8 ordering violated: {} should be < {}",
                data[i], data[i + 1]
            );
        }
    }

    /// TupleOrderingTest.testUnsignedShort: unsigned short ordering.
    #[test]
    fn test_ordering_u16_full() {
        let data: &[u16] = &[0, 1, 0xFE, 0xFF, 0x800, 0x7FFF, 0xFFFF];
        for i in 0..data.len() - 1 {
            assert!(
                encode_u16(data[i]) < encode_u16(data[i + 1]),
                "u16 ordering violated: {} should be < {}",
                data[i], data[i + 1]
            );
        }
    }

    /// TupleOrderingTest.testUnsignedInt: unsigned int ordering.
    #[test]
    fn test_ordering_u32_full() {
        let data: &[u32] = &[
            0, 1, 0xFE, 0xFF, 0x800, 0x7FFF, 0xFFFF,
            0x80000, 0x7FFFFFFF, 0x80000000, 0xFFFFFFFF,
        ];
        for i in 0..data.len() - 1 {
            assert!(
                encode_u32(data[i]) < encode_u32(data[i + 1]),
                "u32 ordering violated: {} should be < {}",
                data[i], data[i + 1]
            );
        }
    }

    /// TupleOrderingTest.testBoolean: false < true.
    #[test]
    fn test_ordering_bool() {
        let mut false_out = TupleOutput::new();
        false_out.write_bool(false);
        let mut true_out = TupleOutput::new();
        true_out.write_bool(true);
        assert!(false_out.to_vec() < true_out.to_vec());
    }

    /// TupleOrderingTest.testFloat: positive-only float ordering (unsorted write_float).
    /// notes that ONLY positive floats are ordered deterministically with writeFloat.
    #[test]
    fn test_ordering_float_positive_only() {
        let data: &[f32] = &[
            0.0,
            f32::MIN_POSITIVE,
            2.0 * f32::MIN_POSITIVE,
            0.01, 0.02, 0.99,
            1.0, 1.01, 1.02, 1.99,
            f32::MAX,
            f32::INFINITY,
        ];
        for i in 0..data.len() - 1 {
            assert!(
                encode_f32(data[i]) < encode_f32(data[i + 1]),
                "positive float ordering violated: {} should be < {}",
                data[i], data[i + 1]
            );
        }
    }

    /// TupleOrderingTest.testDouble: positive-only double ordering (unsorted write_double).
    #[test]
    fn test_ordering_double_positive_only() {
        let data: &[f64] = &[
            0.0,
            f64::MIN_POSITIVE,
            2.0 * f64::MIN_POSITIVE,
            0.001, 0.002, 0.999,
            1.0, 1.001, 1.002, 1.999,
            f64::MAX,
            f64::INFINITY,
        ];
        for i in 0..data.len() - 1 {
            assert!(
                encode_f64(data[i]) < encode_f64(data[i + 1]),
                "positive double ordering violated: {} should be < {}",
                data[i], data[i + 1]
            );
        }
    }

    /// TupleOrderingTest.testSortedFloat: full sorted float ordering including negatives.
    #[test]
    fn test_ordering_sorted_float_full() {
        let data: &[f32] = &[
            f32::NEG_INFINITY,
            -f32::MAX,
            -1.99, -1.02, -1.01, -1.0, -0.99, -0.02, -0.01,
            -2.0 * f32::MIN_POSITIVE,
            -f32::MIN_POSITIVE,
            0.0,
            f32::MIN_POSITIVE,
            2.0 * f32::MIN_POSITIVE,
            0.01, 0.02, 0.99,
            1.0, 1.01, 1.02, 1.99,
            f32::MAX,
            f32::INFINITY,
            f32::NAN,
        ];
        for i in 0..data.len() - 1 {
            assert!(
                encode_f32_sorted(data[i]) < encode_f32_sorted(data[i + 1]),
                "sorted float ordering violated at index {}: {} should be < {}",
                i, data[i], data[i + 1]
            );
        }
    }

    /// TupleOrderingTest.testSortedDouble: full sorted double ordering including negatives.
    #[test]
    fn test_ordering_sorted_double_full() {
        let data: &[f64] = &[
            f64::NEG_INFINITY,
            -f64::MAX,
            -1.999, -1.002, -1.001, -1.0, -0.999, -0.002, -0.001,
            -2.0 * f64::MIN_POSITIVE,
            -f64::MIN_POSITIVE,
            0.0,
            f64::MIN_POSITIVE,
            2.0 * f64::MIN_POSITIVE,
            0.001, 0.002, 0.999,
            1.0, 1.001, 1.002, 1.999,
            f64::MAX,
            f64::INFINITY,
            f64::NAN,
        ];
        for i in 0..data.len() - 1 {
            assert!(
                encode_f64_sorted(data[i]) < encode_f64_sorted(data[i + 1]),
                "sorted double ordering violated at index {}: {} should be < {}",
                i, data[i], data[i + 1]
            );
        }
    }

    /// TupleOrderingTest.testPackedIntAndLong: packed int 0..=630 are ordered.
    #[test]
    fn test_ordering_packed_int_0_to_630() {
        for i in 0u32..630 {
            let mut a = TupleOutput::new();
            a.write_packed_int(i as i32);
            let mut b = TupleOutput::new();
            b.write_packed_int(i as i32 + 1);
            assert!(
                a.to_vec() < b.to_vec(),
                "packed_int ordering violated: {} should be < {}",
                i, i + 1
            );
        }
    }

    /// TupleOrderingTest.testPackedIntAndLong: packed long 0..=630 are ordered.
    #[test]
    fn test_ordering_packed_long_0_to_630() {
        for i in 0u64..630 {
            let mut a = TupleOutput::new();
            a.write_packed_long(i as i64);
            let mut b = TupleOutput::new();
            b.write_packed_long(i as i64 + 1);
            assert!(
                a.to_vec() < b.to_vec(),
                "packed_long ordering violated: {} should be < {}",
                i, i + 1
            );
        }
    }

    /// TupleOrderingTest.testSortedPackedInt: full signed ordering.
    #[test]
    fn test_ordering_sorted_packed_int_full_boundary() {
        let data: &[i32] = &[
            i32::MIN, i32::MIN + 1,
            i16::MIN as i32, i16::MIN as i32 + 1,
            i8::MIN as i32, i8::MIN as i32 + 1,
            -1, 0, 1,
            i8::MAX as i32 - 1, i8::MAX as i32,
            i16::MAX as i32 - 1, i16::MAX as i32,
            i32::MAX - 1, i32::MAX,
        ];
        for i in 0..data.len() - 1 {
            let mut a = TupleOutput::new();
            a.write_sorted_packed_int(data[i]);
            let mut b = TupleOutput::new();
            b.write_sorted_packed_int(data[i + 1]);
            assert!(
                a.to_vec() < b.to_vec(),
                "sorted_packed_int ordering violated: {} should be < {}",
                data[i], data[i + 1]
            );
        }
    }

    /// TupleOrderingTest.testSortedPackedLong: full signed ordering.
    #[test]
    fn test_ordering_sorted_packed_long_full_boundary() {
        let data: &[i64] = &[
            i64::MIN, i64::MIN + 1,
            i32::MIN as i64, i32::MIN as i64 + 1,
            i16::MIN as i64, i16::MIN as i64 + 1,
            i8::MIN as i64, i8::MIN as i64 + 1,
            -1, 0, 1,
            i8::MAX as i64 - 1, i8::MAX as i64,
            i16::MAX as i64 - 1, i16::MAX as i64,
            i32::MAX as i64 - 1, i32::MAX as i64,
            i64::MAX - 1, i64::MAX,
        ];
        for i in 0..data.len() - 1 {
            let mut a = TupleOutput::new();
            a.write_sorted_packed_long(data[i]);
            let mut b = TupleOutput::new();
            b.write_sorted_packed_long(data[i + 1]);
            assert!(
                a.to_vec() < b.to_vec(),
                "sorted_packed_long ordering violated: {} should be < {}",
                data[i], data[i + 1]
            );
        }
    }

    /// TupleFormatTest: packed int specific sizes.
    #[test]
    fn test_format_packed_int_sizes() {
        // 119 fits in 1 byte
        let mut out = TupleOutput::new();
        out.write_packed_int(119);
        assert_eq!(out.len(), 1, "119 should be 1 byte");

        // 0xFFFF + 119 should be 3 bytes (header + 2 value bytes)
        let mut out = TupleOutput::new();
        out.write_packed_int(0xFFFF + 119);
        assert_eq!(out.len(), 3, "0xFFFF+119 should be 3 bytes");

        // i32::MAX should be 5 bytes (header + 4 value bytes)
        let mut out = TupleOutput::new();
        out.write_packed_int(i32::MAX);
        assert_eq!(out.len(), 5, "i32::MAX should be 5 bytes");
    }

    /// TupleFormatTest: packed long specific sizes.
    #[test]
    fn test_format_packed_long_sizes() {
        // 119 fits in 1 byte
        let mut out = TupleOutput::new();
        out.write_packed_long(119);
        assert_eq!(out.len(), 1, "119 should be 1 byte");

        // 0xFFFF_FFFF + 119 should be 5 bytes
        let mut out = TupleOutput::new();
        out.write_packed_long(0xFFFF_FFFF_i64 + 119);
        assert_eq!(out.len(), 5, "0xFFFFFFFF+119 should be 5 bytes");

        // i64::MAX should be 9 bytes
        let mut out = TupleOutput::new();
        out.write_packed_long(i64::MAX);
        assert_eq!(out.len(), 9, "i64::MAX should be 9 bytes");
    }

    /// TupleFormatTest: sorted packed int specific sizes.
    #[test]
    fn test_format_sorted_packed_int_sizes() {
        // -1, 0, 1 fit in 1 byte
        for v in [-1i32, 0, 1, -119, 120] {
            let mut out = TupleOutput::new();
            out.write_sorted_packed_int(v);
            assert_eq!(out.len(), 1, "{} should be 1 byte", v);
        }
        // 121 needs 2 bytes; -120 needs 2 bytes
        for v in [121i32, -120] {
            let mut out = TupleOutput::new();
            out.write_sorted_packed_int(v);
            assert_eq!(out.len(), 2, "{} should be 2 bytes", v);
        }
        // i32::MAX / i32::MIN need 5 bytes
        for v in [i32::MAX, i32::MIN] {
            let mut out = TupleOutput::new();
            out.write_sorted_packed_int(v);
            assert_eq!(out.len(), 5, "{} should be 5 bytes", v);
        }
    }

    /// TupleFormatTest: sorted packed long specific sizes.
    #[test]
    fn test_format_sorted_packed_long_sizes() {
        for v in [-1i64, 0, 1, -119, 120] {
            let mut out = TupleOutput::new();
            out.write_sorted_packed_long(v);
            assert_eq!(out.len(), 1, "{} should be 1 byte", v);
        }
        for v in [121i64, -120] {
            let mut out = TupleOutput::new();
            out.write_sorted_packed_long(v);
            assert_eq!(out.len(), 2, "{} should be 2 bytes", v);
        }
        for v in [i64::MAX, i64::MIN] {
            let mut out = TupleOutput::new();
            out.write_sorted_packed_long(v);
            assert_eq!(out.len(), 9, "{} should be 9 bytes", v);
        }
    }

    /// TupleFormatTest: string ordering test — multi-segment tuples.
    /// Ported from TupleOrderingTest.testString.
    #[test]
    fn test_ordering_string_multi_segment() {
        // Encode "a" then "a"+"" then "a"+""+"a" — each should be strictly greater
        fn encode_strings(strs: &[&str]) -> Vec<u8> {
            let mut out = TupleOutput::new();
            for s in strs {
                out.write_string(s);
            }
            out.to_vec()
        }
        let a = encode_strings(&["a"]);
        let a_empty = encode_strings(&["a", ""]);
        let a_empty_a = encode_strings(&["a", "", "a"]);
        let a_b = encode_strings(&["a", "b"]);
        let aa = encode_strings(&["aa"]);
        let b = encode_strings(&["b"]);

        assert!(a < a_empty, "\"a\" should sort before \"a\"+\"\"");
        assert!(a_empty < a_empty_a, "\"a\"+\"\" should sort before \"a\"+\"\"+\"a\"");
        assert!(a_empty_a < a_b, "\"a\"+\"\"+\"a\" should sort before \"a\"+\"b\"");
        assert!(a_b < aa, "\"a\"+\"b\" should sort before \"aa\"");
        assert!(aa < b, "\"aa\" should sort before \"b\"");
    }
}
