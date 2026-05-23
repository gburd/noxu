//! Utility functions for marshalling data to/from the log.
//!
//!
//! Provides helpers for reading and writing common data types in the log
//! format using little-endian byte order.

use crate::error::Result;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io::{Read, Write};

/// Size constants for fixed-width types.
pub const SHORT_BYTES: usize = 2;
pub const INT_BYTES: usize = 4;
pub const LONG_BYTES: usize = 8;
pub const UNSIGNED_INT_BYTES: usize = 4;

/// Zero-length byte array constant for memory efficiency.
pub const ZERO_LENGTH_BYTE_ARRAY: &[u8] = &[];

// ============================================================================
// Integer writing/reading (fixed-width)
// ============================================================================

/// Writes an i16 to the output.
pub fn write_i16(w: &mut impl Write, val: i16) -> Result<()> {
    Ok(w.write_i16::<LittleEndian>(val)?)
}

/// Reads an i16 from the input.
pub fn read_i16(r: &mut impl Read) -> Result<i16> {
    Ok(r.read_i16::<LittleEndian>()?)
}

/// Writes an i32 to the output.
pub fn write_i32(w: &mut impl Write, val: i32) -> Result<()> {
    Ok(w.write_i32::<LittleEndian>(val)?)
}

/// Reads an i32 from the input.
pub fn read_i32(r: &mut impl Read) -> Result<i32> {
    Ok(r.read_i32::<LittleEndian>()?)
}

/// Writes an i64 to the output.
pub fn write_i64(w: &mut impl Write, val: i64) -> Result<()> {
    Ok(w.write_i64::<LittleEndian>(val)?)
}

/// Reads an i64 from the input.
pub fn read_i64(r: &mut impl Read) -> Result<i64> {
    Ok(r.read_i64::<LittleEndian>()?)
}

/// Writes a u32 as an unsigned int (4 bytes).
pub fn write_u32(w: &mut impl Write, val: u32) -> Result<()> {
    Ok(w.write_u32::<LittleEndian>(val)?)
}

/// Reads a u32 from the input.
pub fn read_u32(r: &mut impl Read) -> Result<u32> {
    Ok(r.read_u32::<LittleEndian>()?)
}

// ============================================================================
// Packed integer writing/reading (variable-width)
// ============================================================================

/// Writes a packed i32 to the output.
pub fn write_packed_i32(w: &mut impl Write, val: i32) -> Result<usize> {
    Ok(noxu_util::packed::write_packed_i32(w, val)?)
}

/// Reads a packed i32 from the input.
pub fn read_packed_i32(r: &mut impl Read) -> Result<i32> {
    Ok(noxu_util::packed::read_packed_i32(r)?)
}

/// Returns the size needed to encode a packed i32.
pub fn packed_i32_size(val: i32) -> usize {
    noxu_util::packed::packed_i32_size(val)
}

/// Writes a packed i64 to the output.
pub fn write_packed_i64(w: &mut impl Write, val: i64) -> Result<usize> {
    Ok(noxu_util::packed::write_packed_i64(w, val)?)
}

/// Reads a packed i64 from the input.
pub fn read_packed_i64(r: &mut impl Read) -> Result<i64> {
    Ok(noxu_util::packed::read_packed_i64(r)?)
}

/// Returns the size needed to encode a packed i64.
pub fn packed_i64_size(val: i64) -> usize {
    noxu_util::packed::packed_i64_size(val)
}

// ============================================================================
// Byte array writing/reading
// ============================================================================

/// Writes a byte array to the output with a length prefix.
///
/// Format: packed i32 length, followed by the bytes.
/// A null array is encoded as length -1.
pub fn write_byte_array(
    w: &mut impl Write,
    data: Option<&[u8]>,
) -> Result<usize> {
    match data {
        None => write_packed_i32(w, -1),
        Some(bytes) => {
            let len = bytes.len() as i32;
            let size1 = write_packed_i32(w, len)?;
            w.write_all(bytes)?;
            Ok(size1 + bytes.len())
        }
    }
}

/// Reads a byte array from the input with a length prefix.
pub fn read_byte_array(r: &mut impl Read) -> Result<Option<Vec<u8>>> {
    let len = read_packed_i32(r)?;
    if len < 0 {
        return Ok(None);
    }
    if len == 0 {
        return Ok(Some(Vec::new()));
    }

    let mut buf = vec![0u8; len as usize];
    r.read_exact(&mut buf)?;
    Ok(Some(buf))
}

/// Returns the size needed to encode a byte array.
pub fn byte_array_size(data: Option<&[u8]>) -> usize {
    match data {
        None => packed_i32_size(-1),
        Some(bytes) => packed_i32_size(bytes.len() as i32) + bytes.len(),
    }
}

/// Writes bytes without a length prefix.
pub fn write_bytes_no_length(w: &mut impl Write, data: &[u8]) -> Result<()> {
    Ok(w.write_all(data)?)
}

/// Reads a fixed number of bytes without a length prefix.
pub fn read_bytes_no_length(r: &mut impl Read, len: usize) -> Result<Vec<u8>> {
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf)?;
    Ok(buf)
}

// ============================================================================
// String writing/reading
// ============================================================================

/// Writes a UTF-8 string to the output with a length prefix.
///
/// Format: byte array with UTF-8 encoding.
pub fn write_string(w: &mut impl Write, s: Option<&str>) -> Result<usize> {
    match s {
        None => write_byte_array(w, None),
        Some(text) => write_byte_array(w, Some(text.as_bytes())),
    }
}

/// Reads a UTF-8 string from the input with a length prefix.
pub fn read_string(r: &mut impl Read) -> Result<Option<String>> {
    match read_byte_array(r)? {
        None => Ok(None),
        Some(bytes) => {
            let s = String::from_utf8(bytes).map_err(|e| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, e)
            })?;
            Ok(Some(s))
        }
    }
}

/// Returns the size needed to encode a string.
pub fn string_size(s: Option<&str>) -> usize {
    match s {
        None => packed_i32_size(-1),
        Some(text) => byte_array_size(Some(text.as_bytes())),
    }
}

// ============================================================================
// Boolean writing/reading
// ============================================================================

/// Writes a boolean to the output (1 byte).
pub fn write_bool(w: &mut impl Write, val: bool) -> Result<()> {
    Ok(w.write_u8(if val { 1 } else { 0 })?)
}

/// Reads a boolean from the input (1 byte).
pub fn read_bool(r: &mut impl Read) -> Result<bool> {
    Ok(r.read_u8()? != 0)
}

/// Returns the size needed to encode a boolean (always 1).
pub const fn bool_size() -> usize {
    1
}

// ============================================================================
// Timestamp writing/reading (milliseconds since epoch as packed i64)
// ============================================================================

/// Writes a timestamp (milliseconds since Unix epoch) as a packed i64.
pub fn write_timestamp(w: &mut impl Write, millis: i64) -> Result<usize> {
    write_packed_i64(w, millis)
}

/// Reads a timestamp as a packed i64.
pub fn read_timestamp(r: &mut impl Read) -> Result<i64> {
    read_packed_i64(r)
}

/// Returns the size needed to encode a timestamp.
pub fn timestamp_size(millis: i64) -> usize {
    packed_i64_size(millis)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_i32_roundtrip() {
        let values = [0, 1, -1, i32::MAX, i32::MIN, 12345, -67890];
        for &val in &values {
            let mut buf = Vec::new();
            write_i32(&mut buf, val).unwrap();
            assert_eq!(buf.len(), INT_BYTES);

            let result = read_i32(&mut Cursor::new(&buf)).unwrap();
            assert_eq!(val, result);
        }
    }

    #[test]
    fn test_packed_i32_roundtrip() {
        let values = [0, 1, -1, 100, -100, 1000, -1000, i32::MAX, i32::MIN];
        for &val in &values {
            let mut buf = Vec::new();
            let size = write_packed_i32(&mut buf, val).unwrap();
            assert_eq!(size, buf.len());
            assert_eq!(size, packed_i32_size(val));

            let result = read_packed_i32(&mut Cursor::new(&buf)).unwrap();
            assert_eq!(val, result);
        }
    }

    #[test]
    fn test_byte_array_roundtrip() {
        // Non-empty array
        let data = b"Hello, Noxu!";
        let mut buf = Vec::new();
        write_byte_array(&mut buf, Some(data)).unwrap();
        let result = read_byte_array(&mut Cursor::new(&buf)).unwrap();
        assert_eq!(result, Some(data.to_vec()));

        // Empty array
        let mut buf = Vec::new();
        write_byte_array(&mut buf, Some(&[])).unwrap();
        let result = read_byte_array(&mut Cursor::new(&buf)).unwrap();
        assert_eq!(result, Some(Vec::new()));

        // Null array
        let mut buf = Vec::new();
        write_byte_array(&mut buf, None).unwrap();
        let result = read_byte_array(&mut Cursor::new(&buf)).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_string_roundtrip() {
        let test_cases =
            [Some("Hello, World!"), Some(""), Some("UTF-8: (ok) (crab)"), None];

        for &text in &test_cases {
            let mut buf = Vec::new();
            write_string(&mut buf, text).unwrap();
            let result = read_string(&mut Cursor::new(&buf)).unwrap();
            assert_eq!(result.as_deref(), text);
        }
    }

    #[test]
    fn test_bool_roundtrip() {
        for &val in &[true, false] {
            let mut buf = Vec::new();
            write_bool(&mut buf, val).unwrap();
            assert_eq!(buf.len(), 1);

            let result = read_bool(&mut Cursor::new(&buf)).unwrap();
            assert_eq!(val, result);
        }
    }

    #[test]
    fn test_timestamp() {
        let now = 1234567890123i64;
        let mut buf = Vec::new();
        write_timestamp(&mut buf, now).unwrap();
        let result = read_timestamp(&mut Cursor::new(&buf)).unwrap();
        assert_eq!(now, result);
    }

    #[test]
    fn test_i16_roundtrip() {
        let values: &[i16] = &[0, 1, -1, i16::MAX, i16::MIN, 1000, -1000];
        for &val in values {
            let mut buf = Vec::new();
            write_i16(&mut buf, val).unwrap();
            assert_eq!(buf.len(), SHORT_BYTES);
            let result = read_i16(&mut Cursor::new(&buf)).unwrap();
            assert_eq!(val, result);
        }
    }

    #[test]
    fn test_i64_roundtrip() {
        let values: &[i64] =
            &[0, 1, -1, i64::MAX, i64::MIN, 1_234_567_890_123, -9_876_543_210];
        for &val in values {
            let mut buf = Vec::new();
            write_i64(&mut buf, val).unwrap();
            assert_eq!(buf.len(), LONG_BYTES);
            let result = read_i64(&mut Cursor::new(&buf)).unwrap();
            assert_eq!(val, result);
        }
    }

    #[test]
    fn test_u32_roundtrip() {
        let values: &[u32] = &[0, 1, u32::MAX, 0x1234_5678, 999_999];
        for &val in values {
            let mut buf = Vec::new();
            write_u32(&mut buf, val).unwrap();
            assert_eq!(buf.len(), UNSIGNED_INT_BYTES);
            let result = read_u32(&mut Cursor::new(&buf)).unwrap();
            assert_eq!(val, result);
        }
    }

    #[test]
    fn test_packed_i32_size_tiers() {
        // Each size tier boundary: 1, 2, 3, 4, 5 bytes.
        // noxu_util::packed uses the same encoding as the:
        //   1 byte : -119..=119
        //   2 bytes: +/-120..
        //   3 bytes: ...
        //   4 bytes: ...
        //   5 bytes: i32::MIN / i32::MAX
        let cases: &[(i32, usize)] = &[
            (0, 1),
            (119, 1),
            (-119, 1),
            (120, 2),
            (-120, 2),
            (i32::MAX, 5),
            (i32::MIN, 5),
        ];
        for &(val, expected_size) in cases {
            assert_eq!(
                packed_i32_size(val),
                expected_size,
                "packed_i32_size({}) expected {}",
                val,
                expected_size
            );

            let mut buf = Vec::new();
            let written = write_packed_i32(&mut buf, val).unwrap();
            assert_eq!(written, expected_size);
            assert_eq!(buf.len(), expected_size);

            let decoded = read_packed_i32(&mut Cursor::new(&buf)).unwrap();
            assert_eq!(decoded, val);
        }
    }

    #[test]
    fn test_packed_i64_roundtrip() {
        let values: &[i64] = &[
            0,
            1,
            -1,
            119,
            -119,
            120,
            -120,
            i64::MAX,
            i64::MIN,
            1_000_000_000_000,
        ];
        for &val in values {
            let mut buf = Vec::new();
            let written = write_packed_i64(&mut buf, val).unwrap();
            assert_eq!(written, packed_i64_size(val));
            assert_eq!(buf.len(), packed_i64_size(val));

            let decoded = read_packed_i64(&mut Cursor::new(&buf)).unwrap();
            assert_eq!(decoded, val, "packed i64 roundtrip failed for {}", val);
        }
    }

    #[test]
    fn test_byte_array_large() {
        let data: Vec<u8> = (0u8..=255).cycle().take(1024).collect();
        let mut buf = Vec::new();
        write_byte_array(&mut buf, Some(&data)).unwrap();
        let result = read_byte_array(&mut Cursor::new(&buf)).unwrap();
        assert_eq!(result.unwrap(), data);
    }

    #[test]
    fn test_string_empty_and_null() {
        // empty string (Some("")) and null (None) must be distinguishable
        let mut buf_empty = Vec::new();
        write_string(&mut buf_empty, Some("")).unwrap();
        let result_empty = read_string(&mut Cursor::new(&buf_empty)).unwrap();
        assert_eq!(result_empty, Some(String::new()));

        let mut buf_null = Vec::new();
        write_string(&mut buf_null, None).unwrap();
        let result_null = read_string(&mut Cursor::new(&buf_null)).unwrap();
        assert_eq!(result_null, None);

        // The two encodings must differ.
        assert_ne!(buf_empty, buf_null);
    }

    #[test]
    fn test_write_read_bytes_no_length() {
        let data = b"raw bytes no length";
        let mut buf = Vec::new();
        write_bytes_no_length(&mut buf, data).unwrap();
        assert_eq!(buf, data);

        let read_back =
            read_bytes_no_length(&mut Cursor::new(&buf), data.len()).unwrap();
        assert_eq!(read_back, data);
    }

    #[test]
    fn test_bool_both_values() {
        for val in [true, false] {
            let mut buf = Vec::new();
            write_bool(&mut buf, val).unwrap();
            assert_eq!(buf.len(), bool_size());
            let decoded = read_bool(&mut Cursor::new(&buf)).unwrap();
            assert_eq!(decoded, val);
        }
    }
}
