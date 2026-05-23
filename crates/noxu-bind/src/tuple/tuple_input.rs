//! TupleInput: reads primitive types from a byte buffer using sortable encodings.
//!

use crate::error::{BindError, Result};
use bytes::Bytes;

/// A reader for tuple-encoded byte data.
///
/// Reads primitive values from a byte buffer using the same encoding formats
/// as `TupleInput`. Signed integers use big-endian with the sign bit
/// flipped for sortable ordering. Floats use IEEE 754 with bit manipulation
/// for sortable ordering.
///
/// Internally uses `bytes::Bytes` so that `clone()` is O(1) and `from_vec`
/// is zero-copy.
///
///
#[derive(Debug, Clone)]
pub struct TupleInput {
    buf: Bytes,
    off: usize,
}

impl TupleInput {
    /// Creates a new `TupleInput` from a byte slice (copies the slice).
    pub fn new(data: &[u8]) -> Self {
        Self { buf: Bytes::copy_from_slice(data), off: 0 }
    }

    /// Creates a new `TupleInput` from a byte vector — zero-copy.
    pub fn from_vec(data: Vec<u8>) -> Self {
        Self { buf: Bytes::from(data), off: 0 }
    }

    /// Creates a new `TupleInput` from existing `Bytes` — zero-copy.
    pub fn from_bytes(data: Bytes) -> Self {
        Self { buf: data, off: 0 }
    }

    /// Returns the number of bytes remaining to be read.
    pub fn available(&self) -> usize {
        self.buf.len().saturating_sub(self.off)
    }

    /// Returns the current read offset.
    pub fn get_offset(&self) -> usize {
        self.off
    }

    /// Sets the read offset.
    pub fn set_offset(&mut self, offset: usize) {
        self.off = offset;
    }

    /// Returns a reference to the underlying buffer.
    pub fn get_buffer(&self) -> &[u8] {
        &self.buf
    }

    /// Reads a single byte from the buffer.
    fn read_fast(&mut self) -> Result<u8> {
        if self.off >= self.buf.len() {
            return Err(BindError::BufferUnderflow { needed: 1, available: 0 });
        }
        let b = self.buf[self.off];
        self.off += 1;
        Ok(b)
    }

    /// Reads a boolean (one byte) value. Non-zero is `true`.
    ///
    /// Reads values written by `TupleOutput::write_bool`.
    pub fn read_bool(&mut self) -> Result<bool> {
        Ok(self.read_fast()? != 0)
    }

    /// Reads an unsigned byte value.
    ///
    /// Reads values written by `TupleOutput::write_u8`.
    pub fn read_u8(&mut self) -> Result<u8> {
        self.read_fast()
    }

    /// Reads a signed byte (one byte) value with sign bit flipped for sort order.
    ///
    /// Reads values written by `TupleOutput::write_i8`.
    pub fn read_i8(&mut self) -> Result<i8> {
        let b = self.read_fast()?;
        Ok((b ^ 0x80) as i8)
    }

    /// Reads an unsigned short (two byte, big-endian) value.
    ///
    /// Reads values written by `TupleOutput::write_u16`.
    pub fn read_u16(&mut self) -> Result<u16> {
        let c1 = self.read_fast()? as u16;
        let c2 = self.read_fast()? as u16;
        Ok((c1 << 8) | c2)
    }

    /// Reads a signed short (two byte) value with sign bit flipped for sort order.
    ///
    /// Reads values written by `TupleOutput::write_i16`.
    pub fn read_i16(&mut self) -> Result<i16> {
        let v = self.read_u16()?;
        Ok((v ^ 0x8000) as i16)
    }

    /// Reads an unsigned int (four byte, big-endian) value.
    ///
    /// Reads values written by `TupleOutput::write_u32`.
    pub fn read_u32(&mut self) -> Result<u32> {
        let c1 = self.read_fast()? as u32;
        let c2 = self.read_fast()? as u32;
        let c3 = self.read_fast()? as u32;
        let c4 = self.read_fast()? as u32;
        Ok((c1 << 24) | (c2 << 16) | (c3 << 8) | c4)
    }

    /// Reads a signed int (four byte) value with sign bit flipped for sort order.
    ///
    /// Reads values written by `TupleOutput::write_i32`.
    pub fn read_i32(&mut self) -> Result<i32> {
        let v = self.read_u32()?;
        Ok((v ^ 0x80000000) as i32)
    }

    /// Reads an unsigned long (eight byte, big-endian) value.
    ///
    /// Reads values written by `TupleOutput::write_u64`.
    pub fn read_u64(&mut self) -> Result<u64> {
        let c1 = self.read_fast()? as u64;
        let c2 = self.read_fast()? as u64;
        let c3 = self.read_fast()? as u64;
        let c4 = self.read_fast()? as u64;
        let c5 = self.read_fast()? as u64;
        let c6 = self.read_fast()? as u64;
        let c7 = self.read_fast()? as u64;
        let c8 = self.read_fast()? as u64;
        Ok((c1 << 56)
            | (c2 << 48)
            | (c3 << 40)
            | (c4 << 32)
            | (c5 << 24)
            | (c6 << 16)
            | (c7 << 8)
            | c8)
    }

    /// Reads a signed long (eight byte) value with sign bit flipped for sort order.
    ///
    /// Reads values written by `TupleOutput::write_i64`.
    pub fn read_i64(&mut self) -> Result<i64> {
        let v = self.read_u64()?;
        Ok((v ^ 0x8000000000000000) as i64)
    }

    /// Reads an unsorted float (four byte) value from the buffer.
    ///
    /// The float is stored as raw IEEE 754 bits in big-endian order.
    /// This does NOT produce sortable byte ordering.
    ///
    /// Reads values written by `TupleOutput::write_float`.
    pub fn read_float(&mut self) -> Result<f32> {
        let bits = self.read_u32()?;
        Ok(f32::from_bits(bits))
    }

    /// Reads an unsorted double (eight byte) value from the buffer.
    ///
    /// The double is stored as raw IEEE 754 bits in big-endian order.
    /// This does NOT produce sortable byte ordering.
    ///
    /// Reads values written by `TupleOutput::write_double`.
    pub fn read_double(&mut self) -> Result<f64> {
        let bits = self.read_u64()?;
        Ok(f64::from_bits(bits))
    }

    /// Reads a sorted float (four byte) value from the buffer.
    ///
    /// Uses sign-bit manipulation to produce sortable byte ordering:
    /// - Positive floats: sign bit flipped (0x80000000 XOR)
    /// - Negative floats: all bits flipped (0xFFFFFFFF XOR)
    ///
    /// Reads values written by `TupleOutput::write_sorted_float`.
    pub fn read_sorted_float(&mut self) -> Result<f32> {
        let encoded = self.read_u32()?;
        // If the high bit is set after reading, it was originally positive
        // (sign bit was flipped to 1), so XOR with 0x80000000.
        // If high bit is clear, it was originally negative (all bits flipped),
        // so XOR with 0xFFFFFFFF.
        let bits = if encoded & 0x80000000 != 0 {
            encoded ^ 0x80000000
        } else {
            encoded ^ 0xFFFFFFFF
        };
        Ok(f32::from_bits(bits))
    }

    /// Reads a sorted double (eight byte) value from the buffer.
    ///
    /// Uses sign-bit manipulation to produce sortable byte ordering:
    /// - Positive doubles: sign bit flipped (0x8000000000000000 XOR)
    /// - Negative doubles: all bits flipped (0xFFFFFFFFFFFFFFFF XOR)
    ///
    /// Reads values written by `TupleOutput::write_sorted_double`.
    pub fn read_sorted_double(&mut self) -> Result<f64> {
        let encoded = self.read_u64()?;
        let bits = if encoded & 0x8000000000000000 != 0 {
            encoded ^ 0x8000000000000000
        } else {
            encoded ^ 0xFFFFFFFFFFFFFFFF
        };
        Ok(f64::from_bits(bits))
    }

    /// Reads a packed (variable-length) i32 value.
    ///
    /// This is an unsorted variable-length encoding where values in [-119, 119]
    /// are stored in a single byte. Larger values use 2-5 bytes.
    ///
    ///
    ///
    /// Reads values written by `TupleOutput::write_packed_int`.
    pub fn read_packed_int(&mut self) -> Result<i32> {
        let b1 = self.read_fast()? as i8;

        let (negative, byte_len) = if b1 < -119 {
            (true, ((-b1) as usize) - 119)
        } else if b1 > 119 {
            (false, (b1 as usize) - 119)
        } else {
            return Ok(b1 as i32);
        };

        let mut value: u32 = self.read_fast()? as u32;
        if byte_len > 1 {
            value |= (self.read_fast()? as u32) << 8;
            if byte_len > 2 {
                value |= (self.read_fast()? as u32) << 16;
                if byte_len > 3 {
                    value |= (self.read_fast()? as u32) << 24;
                }
            }
        }

        if negative {
            Ok(-(value as i32) - 119)
        } else {
            Ok((value as i32) + 119)
        }
    }

    /// Reads a packed (variable-length) i64 value.
    ///
    /// This is an unsorted variable-length encoding where values in [-119, 119]
    /// are stored in a single byte. Larger values use 2-9 bytes.
    ///
    ///
    ///
    /// Reads values written by `TupleOutput::write_packed_long`.
    pub fn read_packed_long(&mut self) -> Result<i64> {
        let b1 = self.read_fast()? as i8;

        let (negative, byte_len) = if b1 < -119 {
            (true, ((-b1) as usize) - 119)
        } else if b1 > 119 {
            (false, (b1 as usize) - 119)
        } else {
            return Ok(b1 as i64);
        };

        let mut value: u64 = self.read_fast()? as u64;
        if byte_len > 1 {
            value |= (self.read_fast()? as u64) << 8;
            if byte_len > 2 {
                value |= (self.read_fast()? as u64) << 16;
                if byte_len > 3 {
                    value |= (self.read_fast()? as u64) << 24;
                    if byte_len > 4 {
                        value |= (self.read_fast()? as u64) << 32;
                        if byte_len > 5 {
                            value |= (self.read_fast()? as u64) << 40;
                            if byte_len > 6 {
                                value |= (self.read_fast()? as u64) << 48;
                                if byte_len > 7 {
                                    value |= (self.read_fast()? as u64) << 56;
                                }
                            }
                        }
                    }
                }
            }
        }

        if negative {
            Ok(-(value as i64) - 119)
        } else {
            Ok((value as i64) + 119)
        }
    }

    /// Reads a sorted packed (variable-length, order-preserving) i32 value.
    ///
    /// Decodes the format written by `TupleOutput::write_sorted_packed_int`.
    ///
    /// Single-byte range [-119, 120]: first byte in `[0x08, 0xF7]`, stored as
    /// `(value + 127)`.  Negative multi-byte: first byte `< 0x08`, meaning
    /// `(0x08 - b1)` big-endian value bytes follow; value = `raw - 119`.
    /// Positive multi-byte: first byte `> 0xF7`, meaning `(b1 - 0xF7)` big-endian
    /// value bytes follow; value = `raw + 121`.
    ///
    ///
    pub fn read_sorted_packed_int(&mut self) -> Result<i32> {
        let b1 = self.read_fast()?;
        if b1 < 0x08 {
            // Negative: (0x08 - b1) additional big-endian bytes
            let n = (0x08 - b1) as usize;
            let mut raw: u32 = 0xFFFFFFFF;
            for _ in 0..n {
                raw = (raw << 8) | (self.read_fast()? as u32);
            }
            Ok(raw as i32 - 119)
        } else if b1 > 0xF7 {
            // Positive: (b1 - 0xF7) additional big-endian bytes
            let n = (b1 - 0xF7) as usize;
            let mut raw: u32 = 0;
            for _ in 0..n {
                raw = (raw << 8) | (self.read_fast()? as u32);
            }
            Ok(raw as i32 + 121)
        } else {
            Ok(b1 as i32 - 127)
        }
    }

    /// Reads a sorted packed (variable-length, order-preserving) i64 value.
    ///
    /// Decodes the format written by `TupleOutput::write_sorted_packed_long`.
    /// Uses the same header-byte scheme as `read_sorted_packed_int`, extended
    /// to up to 8 value bytes.
    ///
    ///
    pub fn read_sorted_packed_long(&mut self) -> Result<i64> {
        let b1 = self.read_fast()?;
        if b1 < 0x08 {
            let n = (0x08 - b1) as usize;
            let mut raw: u64 = 0xFFFFFFFFFFFFFFFF;
            for _ in 0..n {
                raw = (raw << 8) | (self.read_fast()? as u64);
            }
            Ok(raw as i64 - 119)
        } else if b1 > 0xF7 {
            let n = (b1 - 0xF7) as usize;
            let mut raw: u64 = 0;
            for _ in 0..n {
                raw = (raw << 8) | (self.read_fast()? as u64);
            }
            Ok(raw as i64 + 121)
        } else {
            Ok(b1 as i64 - 127)
        }
    }

    /// Reads a Java `char` (16-bit Unicode code point) stored as two big-endian bytes.
    ///
    /// Reads values written by `TupleOutput::write_char`.
    pub fn read_char(&mut self) -> Result<u16> {
        self.read_u16()
    }

    /// Reads a null-escaped UTF-8 string from the buffer.
    ///
    /// Scans for the two-byte terminator [0x00, 0x00], unescaping any
    /// [0x00, 0x01] sequences back to a single 0x00 byte. This is the
    /// inverse of `TupleOutput::write_string`.
    pub fn read_string(&mut self) -> Result<String> {
        let mut decoded: Vec<u8> = Vec::new();
        loop {
            if self.off >= self.buf.len() {
                return Err(BindError::InvalidData(
                    "no null terminator found for string".to_string(),
                ));
            }
            let b = self.buf[self.off];
            self.off += 1;
            if b == 0x00 {
                // Peek at next byte to distinguish terminator from escape
                if self.off >= self.buf.len() {
                    return Err(BindError::InvalidData(
                        "truncated null escape sequence in string".to_string(),
                    ));
                }
                let next = self.buf[self.off];
                self.off += 1;
                if next == 0x00 {
                    // [0x00, 0x00] is the end-of-string terminator
                    break;
                } else if next == 0x01 {
                    // [0x00, 0x01] is an escaped null byte
                    decoded.push(0x00);
                } else {
                    return Err(BindError::InvalidData(format!(
                        "invalid null escape byte 0x{:02x} in string",
                        next
                    )));
                }
            } else {
                decoded.push(b);
            }
        }
        String::from_utf8(decoded).map_err(|e| {
            BindError::StringEncoding(format!("invalid UTF-8: {}", e))
        })
    }

    /// Reads the specified number of raw bytes from the buffer.
    ///
    /// Reads values written by `TupleOutput::write_bytes`.
    pub fn read_bytes(&mut self, len: usize) -> Result<Vec<u8>> {
        if self.available() < len {
            return Err(BindError::BufferUnderflow {
                needed: len,
                available: self.available(),
            });
        }
        let bytes = self.buf[self.off..self.off + len].to_vec();
        self.off += len;
        Ok(bytes)
    }

    /// Skips the specified number of bytes.
    pub fn skip(&mut self, count: usize) -> Result<()> {
        if self.available() < count {
            return Err(BindError::BufferUnderflow {
                needed: count,
                available: self.available(),
            });
        }
        self.off += count;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tuple::TupleOutput;

    #[test]
    fn test_bool_round_trip() {
        let mut out = TupleOutput::new();
        out.write_bool(true);
        out.write_bool(false);
        let mut input = TupleInput::new(&out.to_vec());
        assert!(input.read_bool().unwrap());
        assert!(!input.read_bool().unwrap());
    }

    #[test]
    fn test_i8_round_trip() {
        let mut out = TupleOutput::new();
        out.write_i8(-128);
        out.write_i8(0);
        out.write_i8(127);
        let mut input = TupleInput::new(&out.to_vec());
        assert_eq!(input.read_i8().unwrap(), -128);
        assert_eq!(input.read_i8().unwrap(), 0);
        assert_eq!(input.read_i8().unwrap(), 127);
    }

    #[test]
    fn test_i16_round_trip() {
        let mut out = TupleOutput::new();
        out.write_i16(i16::MIN);
        out.write_i16(0);
        out.write_i16(i16::MAX);
        let mut input = TupleInput::new(&out.to_vec());
        assert_eq!(input.read_i16().unwrap(), i16::MIN);
        assert_eq!(input.read_i16().unwrap(), 0);
        assert_eq!(input.read_i16().unwrap(), i16::MAX);
    }

    #[test]
    fn test_i32_round_trip() {
        let mut out = TupleOutput::new();
        out.write_i32(i32::MIN);
        out.write_i32(-1);
        out.write_i32(0);
        out.write_i32(1);
        out.write_i32(i32::MAX);
        let mut input = TupleInput::new(&out.to_vec());
        assert_eq!(input.read_i32().unwrap(), i32::MIN);
        assert_eq!(input.read_i32().unwrap(), -1);
        assert_eq!(input.read_i32().unwrap(), 0);
        assert_eq!(input.read_i32().unwrap(), 1);
        assert_eq!(input.read_i32().unwrap(), i32::MAX);
    }

    #[test]
    fn test_i64_round_trip() {
        let mut out = TupleOutput::new();
        out.write_i64(i64::MIN);
        out.write_i64(-1);
        out.write_i64(0);
        out.write_i64(1);
        out.write_i64(i64::MAX);
        let mut input = TupleInput::new(&out.to_vec());
        assert_eq!(input.read_i64().unwrap(), i64::MIN);
        assert_eq!(input.read_i64().unwrap(), -1);
        assert_eq!(input.read_i64().unwrap(), 0);
        assert_eq!(input.read_i64().unwrap(), 1);
        assert_eq!(input.read_i64().unwrap(), i64::MAX);
    }

    #[test]
    fn test_u8_round_trip() {
        let mut out = TupleOutput::new();
        out.write_u8(0);
        out.write_u8(128);
        out.write_u8(255);
        let mut input = TupleInput::new(&out.to_vec());
        assert_eq!(input.read_u8().unwrap(), 0);
        assert_eq!(input.read_u8().unwrap(), 128);
        assert_eq!(input.read_u8().unwrap(), 255);
    }

    #[test]
    fn test_float_round_trip() {
        let mut out = TupleOutput::new();
        out.write_float(0.0);
        out.write_float(1.5);
        out.write_float(-1.5);
        out.write_float(f32::MAX);
        out.write_float(f32::MIN);
        let mut input = TupleInput::new(&out.to_vec());
        assert_eq!(input.read_float().unwrap(), 0.0);
        assert_eq!(input.read_float().unwrap(), 1.5);
        assert_eq!(input.read_float().unwrap(), -1.5);
        assert_eq!(input.read_float().unwrap(), f32::MAX);
        assert_eq!(input.read_float().unwrap(), f32::MIN);
    }

    #[test]
    fn test_double_round_trip() {
        let mut out = TupleOutput::new();
        out.write_double(0.0);
        out.write_double(1.5);
        out.write_double(-1.5);
        out.write_double(f64::MAX);
        out.write_double(f64::MIN);
        let mut input = TupleInput::new(&out.to_vec());
        assert_eq!(input.read_double().unwrap(), 0.0);
        assert_eq!(input.read_double().unwrap(), 1.5);
        assert_eq!(input.read_double().unwrap(), -1.5);
        assert_eq!(input.read_double().unwrap(), f64::MAX);
        assert_eq!(input.read_double().unwrap(), f64::MIN);
    }

    #[test]
    fn test_sorted_float_round_trip() {
        let mut out = TupleOutput::new();
        out.write_sorted_float(-1.0);
        out.write_sorted_float(0.0);
        out.write_sorted_float(1.0);
        out.write_sorted_float(f32::MAX);
        out.write_sorted_float(f32::MIN);
        let mut input = TupleInput::new(&out.to_vec());
        assert_eq!(input.read_sorted_float().unwrap(), -1.0);
        assert_eq!(input.read_sorted_float().unwrap(), 0.0);
        assert_eq!(input.read_sorted_float().unwrap(), 1.0);
        assert_eq!(input.read_sorted_float().unwrap(), f32::MAX);
        assert_eq!(input.read_sorted_float().unwrap(), f32::MIN);
    }

    #[test]
    fn test_sorted_double_round_trip() {
        let mut out = TupleOutput::new();
        out.write_sorted_double(-1.0);
        out.write_sorted_double(0.0);
        out.write_sorted_double(1.0);
        out.write_sorted_double(f64::MAX);
        out.write_sorted_double(f64::MIN);
        let mut input = TupleInput::new(&out.to_vec());
        assert_eq!(input.read_sorted_double().unwrap(), -1.0);
        assert_eq!(input.read_sorted_double().unwrap(), 0.0);
        assert_eq!(input.read_sorted_double().unwrap(), 1.0);
        assert_eq!(input.read_sorted_double().unwrap(), f64::MAX);
        assert_eq!(input.read_sorted_double().unwrap(), f64::MIN);
    }

    #[test]
    fn test_packed_int_round_trip() {
        let mut out = TupleOutput::new();
        let values = [
            0,
            1,
            -1,
            119,
            -119,
            120,
            -120,
            255,
            -256,
            1000,
            -1000,
            i32::MAX,
            i32::MIN,
            65535,
            -65536,
        ];
        for &v in &values {
            out.write_packed_int(v);
        }
        let mut input = TupleInput::new(&out.to_vec());
        for &v in &values {
            assert_eq!(
                input.read_packed_int().unwrap(),
                v,
                "failed for value {}",
                v
            );
        }
    }

    #[test]
    fn test_packed_long_round_trip() {
        let mut out = TupleOutput::new();
        let values: &[i64] = &[
            0,
            1,
            -1,
            119,
            -119,
            120,
            -120,
            1000,
            -1000,
            i32::MAX as i64,
            i32::MIN as i64,
            i64::MAX,
            i64::MIN,
        ];
        for &v in values {
            out.write_packed_long(v);
        }
        let mut input = TupleInput::new(&out.to_vec());
        for &v in values {
            assert_eq!(
                input.read_packed_long().unwrap(),
                v,
                "failed for value {}",
                v
            );
        }
    }

    #[test]
    fn test_string_round_trip() {
        let mut out = TupleOutput::new();
        out.write_string("hello");
        out.write_string("");
        out.write_string("world");
        let mut input = TupleInput::new(&out.to_vec());
        assert_eq!(input.read_string().unwrap(), "hello");
        assert_eq!(input.read_string().unwrap(), "");
        assert_eq!(input.read_string().unwrap(), "world");
    }

    #[test]
    fn test_bytes_round_trip() {
        let mut out = TupleOutput::new();
        out.write_bytes(&[1, 2, 3, 4, 5]);
        let mut input = TupleInput::new(&out.to_vec());
        assert_eq!(input.read_bytes(5).unwrap(), vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_buffer_underflow() {
        let mut input = TupleInput::new(&[]);
        assert!(input.read_i32().is_err());
    }

    #[test]
    fn test_available() {
        let mut input = TupleInput::new(&[1, 2, 3, 4]);
        assert_eq!(input.available(), 4);
        input.read_u8().unwrap();
        assert_eq!(input.available(), 3);
    }

    // -----------------------------------------------------------------------
    // Ported from TupleFormatTest: read-side correctness
    // -----------------------------------------------------------------------

    /// TupleFormatTest: available decrements correctly after each read.
    #[test]
    fn test_available_tracks_reads() {
        let mut out = TupleOutput::new();
        out.write_bool(true);
        out.write_i8(-1);
        out.write_i16(1000);
        out.write_i32(123);
        out.write_i64(456);
        // sizes: 1 + 1 + 2 + 4 + 8 = 16
        let mut inp = TupleInput::new(&out.to_vec());
        assert_eq!(inp.available(), 16);
        inp.read_bool().unwrap();
        assert_eq!(inp.available(), 15);
        inp.read_i8().unwrap();
        assert_eq!(inp.available(), 14);
        inp.read_i16().unwrap();
        assert_eq!(inp.available(), 12);
        inp.read_i32().unwrap();
        assert_eq!(inp.available(), 8);
        inp.read_i64().unwrap();
        assert_eq!(inp.available(), 0);
    }

    /// TupleFormatTest: multi-type interleaving round-trip.
    #[test]
    fn test_multi_type_interleave_round_trip() {
        let mut out = TupleOutput::new();
        out.write_string("abc");
        out.write_i32(42);
        out.write_bool(true);
        out.write_double(-1.5_f64);
        out.write_string("xyz");

        let mut inp = TupleInput::new(&out.to_vec());
        assert_eq!(inp.read_string().unwrap(), "abc");
        assert_eq!(inp.read_i32().unwrap(), 42);
        assert!(inp.read_bool().unwrap());
        assert_eq!(inp.read_double().unwrap(), -1.5_f64);
        assert_eq!(inp.read_string().unwrap(), "xyz");
        assert_eq!(inp.available(), 0);
    }

    /// TupleFormatTest: skip advances the offset correctly.
    #[test]
    fn test_skip() {
        let mut out = TupleOutput::new();
        out.write_i32(111);
        out.write_i32(222);
        out.write_i32(333);
        let mut inp = TupleInput::new(&out.to_vec());
        inp.skip(4).unwrap(); // skip the first i32
        assert_eq!(inp.read_i32().unwrap(), 222);
        assert_eq!(inp.available(), 4);
        assert_eq!(inp.read_i32().unwrap(), 333);
        assert_eq!(inp.available(), 0);
    }

    /// TupleFormatTest: skip past end returns an error.
    #[test]
    fn test_skip_past_end_error() {
        let mut inp = TupleInput::new(&[1, 2, 3]);
        assert!(inp.skip(4).is_err());
    }

    /// TupleFormatTest: set_offset / get_offset acts as mark+reset.
    #[test]
    fn test_set_get_offset_mark_reset() {
        let mut out = TupleOutput::new();
        out.write_i32(10);
        out.write_i32(20);
        let mut inp = TupleInput::new(&out.to_vec());
        assert_eq!(inp.get_offset(), 0);
        inp.read_i32().unwrap();
        let mark = inp.get_offset(); // 4
        assert_eq!(inp.read_i32().unwrap(), 20);
        // reset back to mark
        inp.set_offset(mark);
        assert_eq!(inp.read_i32().unwrap(), 20);
        assert_eq!(inp.available(), 0);
    }

    /// TupleFormatTest: get_buffer returns the original data.
    #[test]
    fn test_get_buffer() {
        let data = vec![1u8, 2, 3, 4];
        let inp = TupleInput::new(&data);
        assert_eq!(inp.get_buffer(), data.as_slice());
    }

    /// TupleFormatTest: bool sequence read/write.
    #[test]
    fn test_bool_sequence_read() {
        let mut out = TupleOutput::new();
        out.write_bool(true);
        out.write_bool(false);
        out.write_bool(true);
        assert_eq!(out.len(), 3);
        let mut inp = TupleInput::new(&out.to_vec());
        assert!(inp.read_bool().unwrap());
        assert!(!inp.read_bool().unwrap());
        assert!(inp.read_bool().unwrap());
        assert_eq!(inp.available(), 0);
    }

    /// TupleFormatTest: i16 sequence read/write.
    #[test]
    fn test_short_sequence_read() {
        let mut out = TupleOutput::new();
        out.write_i16(0);
        out.write_i16(1);
        out.write_i16(-1);
        assert_eq!(out.len(), 6);
        let mut inp = TupleInput::new(&out.to_vec());
        assert_eq!(inp.read_i16().unwrap(), 0);
        assert_eq!(inp.read_i16().unwrap(), 1);
        assert_eq!(inp.read_i16().unwrap(), -1);
        assert_eq!(inp.available(), 0);
    }

    /// TupleFormatTest: i32 sequence read/write.
    #[test]
    fn test_int_sequence_read() {
        let mut out = TupleOutput::new();
        out.write_i32(0);
        out.write_i32(1);
        out.write_i32(-1);
        assert_eq!(out.len(), 12);
        let mut inp = TupleInput::new(&out.to_vec());
        assert_eq!(inp.read_i32().unwrap(), 0);
        assert_eq!(inp.read_i32().unwrap(), 1);
        assert_eq!(inp.read_i32().unwrap(), -1);
        assert_eq!(inp.available(), 0);
    }

    /// TupleFormatTest: i64 sequence read/write.
    #[test]
    fn test_long_sequence_read() {
        let mut out = TupleOutput::new();
        out.write_i64(0);
        out.write_i64(1);
        out.write_i64(-1);
        assert_eq!(out.len(), 24);
        let mut inp = TupleInput::new(&out.to_vec());
        assert_eq!(inp.read_i64().unwrap(), 0);
        assert_eq!(inp.read_i64().unwrap(), 1);
        assert_eq!(inp.read_i64().unwrap(), -1);
        assert_eq!(inp.available(), 0);
    }

    /// TupleFormatTest: f32 sequence read/write.
    #[test]
    fn test_float_sequence_read() {
        let mut out = TupleOutput::new();
        out.write_float(0.0);
        out.write_float(1.0);
        out.write_float(-1.0);
        assert_eq!(out.len(), 12);
        let mut inp = TupleInput::new(&out.to_vec());
        assert_eq!(inp.read_float().unwrap(), 0.0f32);
        assert_eq!(inp.read_float().unwrap(), 1.0f32);
        assert_eq!(inp.read_float().unwrap(), -1.0f32);
        assert_eq!(inp.available(), 0);
    }

    /// TupleFormatTest: f64 sequence read/write.
    #[test]
    fn test_double_sequence_read() {
        let mut out = TupleOutput::new();
        out.write_double(0.0);
        out.write_double(1.0);
        out.write_double(-1.0);
        assert_eq!(out.len(), 24);
        let mut inp = TupleInput::new(&out.to_vec());
        assert_eq!(inp.read_double().unwrap(), 0.0f64);
        assert_eq!(inp.read_double().unwrap(), 1.0f64);
        assert_eq!(inp.read_double().unwrap(), -1.0f64);
        assert_eq!(inp.available(), 0);
    }

    /// TupleFormatTest: sorted float sequence read/write.
    #[test]
    fn test_sorted_float_sequence_read() {
        let mut out = TupleOutput::new();
        out.write_sorted_float(0.0);
        out.write_sorted_float(1.0);
        out.write_sorted_float(-1.0);
        assert_eq!(out.len(), 12);
        let mut inp = TupleInput::new(&out.to_vec());
        assert_eq!(inp.read_sorted_float().unwrap(), 0.0f32);
        assert_eq!(inp.read_sorted_float().unwrap(), 1.0f32);
        assert_eq!(inp.read_sorted_float().unwrap(), -1.0f32);
        assert_eq!(inp.available(), 0);
    }

    /// TupleFormatTest: sorted double sequence read/write.
    #[test]
    fn test_sorted_double_sequence_read() {
        let mut out = TupleOutput::new();
        out.write_sorted_double(0.0);
        out.write_sorted_double(1.0);
        out.write_sorted_double(-1.0);
        assert_eq!(out.len(), 24);
        let mut inp = TupleInput::new(&out.to_vec());
        assert_eq!(inp.read_sorted_double().unwrap(), 0.0f64);
        assert_eq!(inp.read_sorted_double().unwrap(), 1.0f64);
        assert_eq!(inp.read_sorted_double().unwrap(), -1.0f64);
        assert_eq!(inp.available(), 0);
    }

    /// TupleFormatTest: NaN round-trips through unsorted float.
    #[test]
    fn test_float_nan_round_trip() {
        let mut out = TupleOutput::new();
        out.write_float(f32::NAN);
        let mut inp = TupleInput::new(&out.to_vec());
        assert!(inp.read_float().unwrap().is_nan());
    }

    /// TupleFormatTest: NaN round-trips through unsorted double.
    #[test]
    fn test_double_nan_round_trip() {
        let mut out = TupleOutput::new();
        out.write_double(f64::NAN);
        let mut inp = TupleInput::new(&out.to_vec());
        assert!(inp.read_double().unwrap().is_nan());
    }

    /// TupleFormatTest: NaN round-trips through sorted float.
    #[test]
    fn test_sorted_float_nan_round_trip() {
        let mut out = TupleOutput::new();
        out.write_sorted_float(f32::NAN);
        let mut inp = TupleInput::new(&out.to_vec());
        assert!(inp.read_sorted_float().unwrap().is_nan());
    }

    /// TupleFormatTest: NaN round-trips through sorted double.
    #[test]
    fn test_sorted_double_nan_round_trip() {
        let mut out = TupleOutput::new();
        out.write_sorted_double(f64::NAN);
        let mut inp = TupleInput::new(&out.to_vec());
        assert!(inp.read_sorted_double().unwrap().is_nan());
    }

    /// TupleFormatTest: infinity round-trips through sorted float.
    #[test]
    fn test_sorted_float_infinity_round_trip() {
        let mut out = TupleOutput::new();
        out.write_sorted_float(f32::INFINITY);
        out.write_sorted_float(f32::NEG_INFINITY);
        let mut inp = TupleInput::new(&out.to_vec());
        assert_eq!(inp.read_sorted_float().unwrap(), f32::INFINITY);
        assert_eq!(inp.read_sorted_float().unwrap(), f32::NEG_INFINITY);
        assert_eq!(inp.available(), 0);
    }

    /// TupleFormatTest: infinity round-trips through sorted double.
    #[test]
    fn test_sorted_double_infinity_round_trip() {
        let mut out = TupleOutput::new();
        out.write_sorted_double(f64::INFINITY);
        out.write_sorted_double(f64::NEG_INFINITY);
        let mut inp = TupleInput::new(&out.to_vec());
        assert_eq!(inp.read_sorted_double().unwrap(), f64::INFINITY);
        assert_eq!(inp.read_sorted_double().unwrap(), f64::NEG_INFINITY);
        assert_eq!(inp.available(), 0);
    }

    /// TupleFormatTest: packed int 119/0xFFFF+119/MAX sizes and round-trip.
    #[test]
    fn test_packed_int_specific_sizes_round_trip() {
        let cases: &[(i32, usize)] =
            &[(119, 1), (0xFFFF + 119, 3), (i32::MAX, 5)];
        for &(val, expected_size) in cases {
            let mut out = TupleOutput::new();
            out.write_packed_int(val);
            assert_eq!(out.len(), expected_size, "packed_int {} size", val);
            let mut inp = TupleInput::new(&out.to_vec());
            assert_eq!(inp.read_packed_int().unwrap(), val);
        }
    }

    /// TupleFormatTest: packed long 119/0xFFFFFFFF+119/MAX sizes and round-trip.
    #[test]
    fn test_packed_long_specific_sizes_round_trip() {
        let cases: &[(i64, usize)] =
            &[(119, 1), (0xFFFF_FFFF_i64 + 119, 5), (i64::MAX, 9)];
        for &(val, expected_size) in cases {
            let mut out = TupleOutput::new();
            out.write_packed_long(val);
            assert_eq!(out.len(), expected_size, "packed_long {} size", val);
            let mut inp = TupleInput::new(&out.to_vec());
            assert_eq!(inp.read_packed_long().unwrap(), val);
        }
    }

    /// TupleFormatTest: sorted packed int sizes and round-trip.
    #[test]
    fn test_sorted_packed_int_specific_sizes_round_trip() {
        let cases: &[(i32, usize)] = &[
            (-1, 1),
            (0, 1),
            (1, 1),
            (-119, 1),
            (120, 1),
            (121, 2),
            (-120, 2),
            (i32::MAX, 5),
            (i32::MIN, 5),
        ];
        for &(val, expected_size) in cases {
            let mut out = TupleOutput::new();
            out.write_sorted_packed_int(val);
            assert_eq!(
                out.len(),
                expected_size,
                "sorted_packed_int {} size",
                val
            );
            let mut inp = TupleInput::new(&out.to_vec());
            assert_eq!(inp.read_sorted_packed_int().unwrap(), val);
        }
    }

    /// TupleFormatTest: sorted packed long sizes and round-trip.
    #[test]
    fn test_sorted_packed_long_specific_sizes_round_trip() {
        let cases: &[(i64, usize)] = &[
            (-1, 1),
            (0, 1),
            (1, 1),
            (-119, 1),
            (120, 1),
            (121, 2),
            (-120, 2),
            (i64::MAX, 9),
            (i64::MIN, 9),
        ];
        for &(val, expected_size) in cases {
            let mut out = TupleOutput::new();
            out.write_sorted_packed_long(val);
            assert_eq!(
                out.len(),
                expected_size,
                "sorted_packed_long {} size",
                val
            );
            let mut inp = TupleInput::new(&out.to_vec());
            assert_eq!(inp.read_sorted_packed_long().unwrap(), val);
        }
    }

    /// TupleFormatTest: u8 sequence read/write including boundary values.
    #[test]
    fn test_unsigned_byte_sequence_read() {
        let mut out = TupleOutput::new();
        out.write_u8(0);
        out.write_u8(1);
        out.write_u8(255);
        assert_eq!(out.len(), 3);
        let mut inp = TupleInput::new(&out.to_vec());
        assert_eq!(inp.read_u8().unwrap(), 0);
        assert_eq!(inp.read_u8().unwrap(), 1);
        assert_eq!(inp.read_u8().unwrap(), 255);
        assert_eq!(inp.available(), 0);
    }

    /// TupleFormatTest: u16 sequence read/write including boundary values.
    #[test]
    fn test_unsigned_short_sequence_read() {
        let mut out = TupleOutput::new();
        out.write_u16(0);
        out.write_u16(1);
        out.write_u16(0xFFFF);
        assert_eq!(out.len(), 6);
        let mut inp = TupleInput::new(&out.to_vec());
        assert_eq!(inp.read_u16().unwrap(), 0);
        assert_eq!(inp.read_u16().unwrap(), 1);
        assert_eq!(inp.read_u16().unwrap(), 0xFFFF);
        assert_eq!(inp.available(), 0);
    }

    /// TupleFormatTest: u32 sequence read/write including boundary values.
    #[test]
    fn test_unsigned_int_sequence_read() {
        let mut out = TupleOutput::new();
        out.write_u32(0);
        out.write_u32(1);
        out.write_u32(0xFFFF_FFFF);
        assert_eq!(out.len(), 12);
        let mut inp = TupleInput::new(&out.to_vec());
        assert_eq!(inp.read_u32().unwrap(), 0);
        assert_eq!(inp.read_u32().unwrap(), 1);
        assert_eq!(inp.read_u32().unwrap(), 0xFFFF_FFFF);
        assert_eq!(inp.available(), 0);
    }

    /// TupleFormatTest: null string then int interleaved.
    /// TupleFormatTest.testNullString writes null then int and back.
    /// In Rust we model "null string" as Option<String>, but the tuple
    /// format doesn't have a built-in null. We test the Rust equivalent:
    /// an empty string followed by an i32, verifying the i32 is readable.
    #[test]
    fn test_string_then_int_interleaved() {
        let mut out = TupleOutput::new();
        out.write_string("abc");
        out.write_i32(123);
        // "abc" = 3 + 2 bytes terminator = 5; i32 = 4 bytes → total 9
        assert_eq!(out.len(), 9);
        let mut inp = TupleInput::new(&out.to_vec());
        assert_eq!(inp.read_string().unwrap(), "abc");
        assert_eq!(inp.available(), 4);
        assert_eq!(inp.read_i32().unwrap(), 123);
        assert_eq!(inp.available(), 0);
    }

    /// TupleFormatTest: char round-trips as 2 bytes.
    #[test]
    fn test_char_size_and_round_trip() {
        let mut out = TupleOutput::new();
        out.write_char(b'a' as u16);
        assert_eq!(out.len(), 2);
        let mut inp = TupleInput::new(&out.to_vec());
        assert_eq!(inp.read_char().unwrap(), b'a' as u16);
        assert_eq!(inp.available(), 0);
    }

    /// TupleFormatTest: from_vec constructor works the same as new.
    #[test]
    fn test_from_vec_constructor() {
        let data = vec![0x80u8, 0x00, 0x00, 0x2B]; // write_i32(43)
        let mut inp1 = TupleInput::from_vec(data.clone());
        let mut inp2 = TupleInput::new(&data);
        assert_eq!(inp1.read_i32().unwrap(), inp2.read_i32().unwrap());
    }

    // -----------------------------------------------------------------------
    // Ported from TupleBindingTest: edge values for all numeric types
    // -----------------------------------------------------------------------

    /// TupleBindingTest: i8 edge values MIN, -1, 0, 1, MAX.
    #[test]
    fn test_i8_edge_values() {
        for &v in &[i8::MIN, -1i8, 0, 1, i8::MAX] {
            let mut out = TupleOutput::new();
            out.write_i8(v);
            assert_eq!(out.len(), 1, "i8 {} should be 1 byte", v);
            let mut inp = TupleInput::new(&out.to_vec());
            assert_eq!(inp.read_i8().unwrap(), v);
        }
    }

    /// TupleBindingTest: i16 edge values MIN, -1, 0, 1, MAX.
    #[test]
    fn test_i16_edge_values() {
        for &v in &[i16::MIN, -1i16, 0, 1, i16::MAX] {
            let mut out = TupleOutput::new();
            out.write_i16(v);
            assert_eq!(out.len(), 2, "i16 {} should be 2 bytes", v);
            let mut inp = TupleInput::new(&out.to_vec());
            assert_eq!(inp.read_i16().unwrap(), v);
        }
    }

    /// TupleBindingTest: i32 edge values MIN, MAX, and all wrapping boundary arithmetic.
    #[test]
    fn test_i32_wrapping_values() {
        // TupleFormatTest.testInt exercises values like MAX+1 which wrap.
        // i32::MAX+1 wraps to i32::MIN in two's complement.
        let cases: &[(i32, i32)] = &[
            (i32::MAX.wrapping_add(1), i32::MIN), // wraps
            (i32::MIN.wrapping_sub(1), i32::MAX), // wraps
        ];
        for &(input_val, expected) in cases {
            let mut out = TupleOutput::new();
            out.write_i32(input_val);
            let mut inp = TupleInput::new(&out.to_vec());
            assert_eq!(inp.read_i32().unwrap(), expected);
        }
    }

    /// TupleBindingTest: i64 edge values MIN, MAX, and wrapping boundary arithmetic.
    #[test]
    fn test_i64_wrapping_values() {
        let cases: &[(i64, i64)] = &[
            (i64::MAX.wrapping_add(1), i64::MIN),
            (i64::MIN.wrapping_sub(1), i64::MAX),
        ];
        for &(input_val, expected) in cases {
            let mut out = TupleOutput::new();
            out.write_i64(input_val);
            let mut inp = TupleInput::new(&out.to_vec());
            assert_eq!(inp.read_i64().unwrap(), expected);
        }
    }

    /// TupleBindingTest: float special values (NaN, infinity, min, max).
    #[test]
    fn test_float_special_values_round_trip() {
        let special: &[f32] = &[
            f32::NAN,
            f32::INFINITY,
            f32::NEG_INFINITY,
            f32::MAX,
            f32::MIN,
            f32::MIN_POSITIVE,
            0.0,
            -0.0,
            1.0,
            -1.0,
        ];
        for &v in special {
            let mut out = TupleOutput::new();
            out.write_float(v);
            assert_eq!(out.len(), 4);
            let mut inp = TupleInput::new(&out.to_vec());
            let got = inp.read_float().unwrap();
            if v.is_nan() {
                assert!(got.is_nan(), "NaN should round-trip as NaN");
            } else {
                assert_eq!(
                    got.to_bits(),
                    v.to_bits(),
                    "float {} should round-trip",
                    v
                );
            }
        }
    }

    /// TupleBindingTest: double special values (NaN, infinity, min, max).
    #[test]
    fn test_double_special_values_round_trip() {
        let special: &[f64] = &[
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
            f64::MAX,
            f64::MIN,
            f64::MIN_POSITIVE,
            0.0,
            -0.0,
            1.0,
            -1.0,
        ];
        for &v in special {
            let mut out = TupleOutput::new();
            out.write_double(v);
            assert_eq!(out.len(), 8);
            let mut inp = TupleInput::new(&out.to_vec());
            let got = inp.read_double().unwrap();
            if v.is_nan() {
                assert!(got.is_nan(), "NaN should round-trip as NaN");
            } else {
                assert_eq!(
                    got.to_bits(),
                    v.to_bits(),
                    "double {} should round-trip",
                    v
                );
            }
        }
    }

    /// TupleBindingTest: sorted float special values.
    #[test]
    fn test_sorted_float_special_values_round_trip() {
        let special: &[f32] = &[
            f32::NAN,
            f32::INFINITY,
            f32::NEG_INFINITY,
            f32::MAX,
            f32::MIN,
            f32::MIN_POSITIVE,
            0.0,
            -0.0,
            1.0,
            -1.0,
            123.123,
        ];
        for &v in special {
            let mut out = TupleOutput::new();
            out.write_sorted_float(v);
            assert_eq!(out.len(), 4);
            let mut inp = TupleInput::new(&out.to_vec());
            let got = inp.read_sorted_float().unwrap();
            if v.is_nan() {
                assert!(got.is_nan(), "NaN should round-trip");
            } else {
                assert_eq!(
                    got.to_bits(),
                    v.to_bits(),
                    "sorted float {} should round-trip",
                    v
                );
            }
        }
    }

    /// TupleBindingTest: sorted double special values.
    #[test]
    fn test_sorted_double_special_values_round_trip() {
        let special: &[f64] = &[
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
            f64::MAX,
            f64::MIN,
            f64::MIN_POSITIVE,
            0.0,
            -0.0,
            1.0,
            -1.0,
            123.123,
        ];
        for &v in special {
            let mut out = TupleOutput::new();
            out.write_sorted_double(v);
            assert_eq!(out.len(), 8);
            let mut inp = TupleInput::new(&out.to_vec());
            let got = inp.read_sorted_double().unwrap();
            if v.is_nan() {
                assert!(got.is_nan(), "NaN should round-trip");
            } else {
                assert_eq!(
                    got.to_bits(),
                    v.to_bits(),
                    "sorted double {} should round-trip",
                    v
                );
            }
        }
    }
}
