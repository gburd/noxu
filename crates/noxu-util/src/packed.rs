//! Packed integer encoding utilities.
//!
//! Port of `com.sleepycat.util.PackedInteger` and sorted number encoding
//! from `com.sleepycat.bind.tuple`.
//!
//! Provides variable-length encoding for integers that is compact for small
//! values and preserves sort order when used as keys.
//!
//! # Packed integer format
//!
//! Values in [-119, 119] are stored in a single byte.
//!
//! For values outside that range, the first byte encodes the sign and the
//! number of following value bytes:
//!
//! - Positive: first byte = 119 + N (where N is 1..=4 for i32, 1..=8 for i64)
//! - Negative: first byte = -(119 + N)  (as signed byte)
//!
//! The value bytes are stored in little-endian order as an unsigned magnitude.
//! On read: positive value = magnitude + 119; negative value = -(magnitude + 119).
//!
//! # Sorted (order-preserving) format
//!
//! Values in [-119, 120] are stored in a single byte as (value + 127).
//!
//! For values outside that range the first byte encodes the length, and the
//! remaining bytes are stored big-endian so that byte-wise comparison preserves
//! integer ordering:
//!
//! - Positive (value > 120): first byte = 0xF7 + N; value bytes store (value - 121)
//! - Negative (value < -119): first byte = 0x08 - N; value bytes store (value + 119)
//!
//! This is a faithful port of the JE PackedInteger class.

use std::io::{self, Read, Write};

// ---------------------------------------------------------------------------
// Packed i32
// ---------------------------------------------------------------------------

/// Returns the number of bytes needed to encode a packed i32.
pub fn packed_i32_size(val: i32) -> usize {
    if (-119..=119).contains(&val) {
        return 1;
    }
    let mag = if val < -119 {
        val.wrapping_neg().wrapping_sub(119) as u32
    } else {
        (val - 119) as u32
    };
    if mag & 0xFFFFFF00 == 0 { 2 }
    else if mag & 0xFFFF0000 == 0 { 3 }
    else if mag & 0xFF000000 == 0 { 4 }
    else { 5 }
}

/// Writes a packed (variable-length) i32 to the output.
///
/// Returns the number of bytes written (1..=5).
pub fn write_packed_i32(w: &mut impl Write, val: i32) -> io::Result<usize> {
    if (-119..=119).contains(&val) {
        w.write_all(&[val as i8 as u8])?;
        return Ok(1);
    }

    let negative = val < -119;
    let mag: u32 = if negative {
        val.wrapping_neg().wrapping_sub(119) as u32
    } else {
        (val - 119) as u32
    };

    // Write value bytes in little-endian (least significant first)
    let n: u8 = if mag & 0xFFFFFF00 == 0 { 1 }
        else if mag & 0xFFFF0000 == 0 { 2 }
        else if mag & 0xFF000000 == 0 { 3 }
        else { 4 };

    let prefix: u8 = if negative {
        (-(119i16 + n as i16)) as i8 as u8
    } else {
        119 + n
    };

    w.write_all(&[prefix])?;
    for i in 0..n {
        w.write_all(&[((mag >> (i * 8)) & 0xFF) as u8])?;
    }
    Ok(1 + n as usize)
}

/// Reads a packed (variable-length) i32 from the input.
pub fn read_packed_i32(r: &mut impl Read) -> io::Result<i32> {
    let mut buf1 = [0u8; 1];
    r.read_exact(&mut buf1)?;
    let b1 = buf1[0] as i8;

    if (-119..=119).contains(&b1) {
        return Ok(b1 as i32);
    }

    let (negative, n) = if b1 < -119 {
        (true, ((-b1) - 119) as usize)
    } else {
        (false, (b1 - 119) as usize)
    };

    let mut mag: u32 = 0;
    for i in 0..n {
        let mut byte = [0u8; 1];
        r.read_exact(&mut byte)?;
        mag |= (byte[0] as u32) << (i * 8);
    }

    if negative {
        Ok(-(mag as i32) - 119)
    } else {
        Ok(mag as i32 + 119)
    }
}

// ---------------------------------------------------------------------------
// Packed i64
// ---------------------------------------------------------------------------

/// Returns the number of bytes needed to encode a packed i64.
pub fn packed_i64_size(val: i64) -> usize {
    if (-119..=119).contains(&val) {
        return 1;
    }
    let mag = if val < -119 {
        val.wrapping_neg().wrapping_sub(119) as u64
    } else {
        (val - 119) as u64
    };
    if mag & 0xFFFFFFFFFFFFFF00 == 0 { 2 }
    else if mag & 0xFFFFFFFFFFFF0000 == 0 { 3 }
    else if mag & 0xFFFFFFFFFF000000 == 0 { 4 }
    else if mag & 0xFFFFFFFF00000000 == 0 { 5 }
    else if mag & 0xFFFFFF0000000000 == 0 { 6 }
    else if mag & 0xFFFF000000000000 == 0 { 7 }
    else if mag & 0xFF00000000000000 == 0 { 8 }
    else { 9 }
}

/// Writes a packed (variable-length) i64 to the output.
///
/// Returns the number of bytes written (1..=9).
pub fn write_packed_i64(w: &mut impl Write, val: i64) -> io::Result<usize> {
    if (-119..=119).contains(&val) {
        w.write_all(&[val as i8 as u8])?;
        return Ok(1);
    }

    let negative = val < -119;
    let mag: u64 = if negative {
        val.wrapping_neg().wrapping_sub(119) as u64
    } else {
        (val - 119) as u64
    };

    let n: u8 = if mag & 0xFFFFFFFFFFFFFF00 == 0 { 1 }
        else if mag & 0xFFFFFFFFFFFF0000 == 0 { 2 }
        else if mag & 0xFFFFFFFFFF000000 == 0 { 3 }
        else if mag & 0xFFFFFFFF00000000 == 0 { 4 }
        else if mag & 0xFFFFFF0000000000 == 0 { 5 }
        else if mag & 0xFFFF000000000000 == 0 { 6 }
        else if mag & 0xFF00000000000000 == 0 { 7 }
        else { 8 };

    let prefix: u8 = if negative {
        (-(119i16 + n as i16)) as i8 as u8
    } else {
        119 + n
    };

    w.write_all(&[prefix])?;
    for i in 0..n {
        w.write_all(&[((mag >> (i * 8)) & 0xFF) as u8])?;
    }
    Ok(1 + n as usize)
}

/// Reads a packed (variable-length) i64 from the input.
pub fn read_packed_i64(r: &mut impl Read) -> io::Result<i64> {
    let mut buf1 = [0u8; 1];
    r.read_exact(&mut buf1)?;
    let b1 = buf1[0] as i8;

    if (-119..=119).contains(&b1) {
        return Ok(b1 as i64);
    }

    let (negative, n) = if b1 < -119 {
        (true, ((-b1) - 119) as usize)
    } else {
        (false, (b1 - 119) as usize)
    };

    let mut mag: u64 = 0;
    for i in 0..n {
        let mut byte = [0u8; 1];
        r.read_exact(&mut byte)?;
        mag |= (byte[0] as u64) << (i * 8);
    }

    if negative {
        Ok(-(mag as i64) - 119)
    } else {
        Ok(mag as i64 + 119)
    }
}

// ---------------------------------------------------------------------------
// Sorted (order-preserving) integers
//
// These use a different encoding from the packed format above: values
// are stored big-endian, and the first byte is chosen so that
// byte-wise comparison preserves the integer ordering.
//
// Single-byte range: [-119, 120] stored as (value + 127).
// Positive (value > 120): first byte = 0xF7 + N, value bytes = (value - 121) big-endian.
// Negative (value < -119): first byte = 0x08 - N, value bytes = (value + 119) big-endian.
// ---------------------------------------------------------------------------

/// Writes a sorted (order-preserving) i32 to the output.
pub fn write_sorted_i32(w: &mut impl Write, val: i32) -> io::Result<()> {
    if (-119..=120).contains(&val) {
        w.write_all(&[(val + 127) as u8])?;
        return Ok(());
    }

    if val > 120 {
        let adj = (val - 121) as u32;
        let n = if adj & 0xFF000000 != 0 { 4u8 }
            else if adj & 0xFFFF0000 != 0 { 3 }
            else if adj & 0xFFFFFF00 != 0 { 2 }
            else { 1 };
        w.write_all(&[0xF7 + n])?;
        for i in (0..n).rev() {
            w.write_all(&[((adj >> (i * 8)) & 0xFF) as u8])?;
        }
    } else {
        // val < -119
        let adj = val + 119; // negative, e.g. -120+119 = -1
        let uadj = adj as u32;        // two's complement
        let n = if (uadj | 0x00FFFFFF) != 0xFFFFFFFF { 4u8 }
            else if (uadj | 0x0000FFFF) != 0xFFFFFFFF { 3 }
            else if (uadj | 0x000000FF) != 0xFFFFFFFF { 2 }
            else { 1 };
        w.write_all(&[(0x08u8).wrapping_sub(n)])?;
        for i in (0..n).rev() {
            w.write_all(&[((uadj >> (i * 8)) & 0xFF) as u8])?;
        }
    }
    Ok(())
}

/// Reads a sorted (order-preserving) i32 from the input.
pub fn read_sorted_i32(r: &mut impl Read) -> io::Result<i32> {
    let mut buf1 = [0u8; 1];
    r.read_exact(&mut buf1)?;
    let b1 = buf1[0];

    if b1 < 0x08 {
        // Negative, n extra bytes
        let n = (0x08 - b1) as usize;
        let mut raw: u32 = 0xFFFFFFFF;
        for _ in 0..n {
            let mut byte = [0u8; 1];
            r.read_exact(&mut byte)?;
            raw = (raw << 8) | byte[0] as u32;
        }
        Ok(raw as i32 - 119)
    } else if b1 > 0xF7 {
        // Positive, n extra bytes
        let n = (b1 - 0xF7) as usize;
        let mut raw: u32 = 0;
        for _ in 0..n {
            let mut byte = [0u8; 1];
            r.read_exact(&mut byte)?;
            raw = (raw << 8) | byte[0] as u32;
        }
        Ok(raw as i32 + 121)
    } else {
        Ok(b1 as i32 - 127)
    }
}

/// Writes a sorted (order-preserving) i64 to the output.
pub fn write_sorted_i64(w: &mut impl Write, val: i64) -> io::Result<()> {
    if (-119..=120).contains(&val) {
        w.write_all(&[(val + 127) as u8])?;
        return Ok(());
    }

    if val > 120 {
        let adj = (val - 121) as u64;
        let n = if adj & 0xFF00000000000000 != 0 { 8u8 }
            else if adj & 0xFFFF000000000000 != 0 { 7 }
            else if adj & 0xFFFFFF0000000000 != 0 { 6 }
            else if adj & 0xFFFFFFFF00000000 != 0 { 5 }
            else if adj & 0xFFFFFFFFFF000000 != 0 { 4 }
            else if adj & 0xFFFFFFFFFFFF0000 != 0 { 3 }
            else if adj & 0xFFFFFFFFFFFFFF00 != 0 { 2 }
            else { 1 };
        w.write_all(&[0xF7 + n])?;
        for i in (0..n).rev() {
            w.write_all(&[((adj >> (i * 8)) & 0xFF) as u8])?;
        }
    } else {
        // val < -119
        let adj = val + 119;
        let uadj = adj as u64;
        let n = if (uadj | 0x00FFFFFFFFFFFFFF) != 0xFFFFFFFFFFFFFFFF { 8u8 }
            else if (uadj | 0x0000FFFFFFFFFFFF) != 0xFFFFFFFFFFFFFFFF { 7 }
            else if (uadj | 0x000000FFFFFFFFFF) != 0xFFFFFFFFFFFFFFFF { 6 }
            else if (uadj | 0x00000000FFFFFFFF) != 0xFFFFFFFFFFFFFFFF { 5 }
            else if (uadj | 0x0000000000FFFFFF) != 0xFFFFFFFFFFFFFFFF { 4 }
            else if (uadj | 0x000000000000FFFF) != 0xFFFFFFFFFFFFFFFF { 3 }
            else if (uadj | 0x00000000000000FF) != 0xFFFFFFFFFFFFFFFF { 2 }
            else { 1 };
        w.write_all(&[(0x08u8).wrapping_sub(n)])?;
        for i in (0..n).rev() {
            w.write_all(&[((uadj >> (i * 8)) & 0xFF) as u8])?;
        }
    }
    Ok(())
}

/// Reads a sorted (order-preserving) i64 from the input.
pub fn read_sorted_i64(r: &mut impl Read) -> io::Result<i64> {
    let mut buf1 = [0u8; 1];
    r.read_exact(&mut buf1)?;
    let b1 = buf1[0];

    if b1 < 0x08 {
        let n = (0x08 - b1) as usize;
        let mut raw: u64 = 0xFFFFFFFFFFFFFFFF;
        for _ in 0..n {
            let mut byte = [0u8; 1];
            r.read_exact(&mut byte)?;
            raw = (raw << 8) | byte[0] as u64;
        }
        Ok(raw as i64 - 119)
    } else if b1 > 0xF7 {
        let n = (b1 - 0xF7) as usize;
        let mut raw: u64 = 0;
        for _ in 0..n {
            let mut byte = [0u8; 1];
            r.read_exact(&mut byte)?;
            raw = (raw << 8) | byte[0] as u64;
        }
        Ok(raw as i64 + 121)
    } else {
        Ok(b1 as i64 - 127)
    }
}

/// Writes a sorted (order-preserving) f32 to the output.
///
/// Encodes IEEE 754 floats such that the byte-wise comparison preserves
/// the natural float ordering (including NaN handling).
pub fn write_sorted_f32(w: &mut impl Write, val: f32) -> io::Result<()> {
    let bits = val.to_bits();
    // If sign bit is set, flip all bits; otherwise flip only the sign bit.
    let encoded =
        if bits & 0x8000_0000 != 0 { !bits } else { bits ^ 0x8000_0000 };
    let bytes = encoded.to_be_bytes();
    w.write_all(&bytes)
}

/// Reads a sorted (order-preserving) f32 from the input.
pub fn read_sorted_f32(r: &mut impl Read) -> io::Result<f32> {
    let mut buf = [0u8; 4];
    r.read_exact(&mut buf)?;
    let encoded = u32::from_be_bytes(buf);
    let bits = if encoded & 0x8000_0000 != 0 {
        encoded ^ 0x8000_0000
    } else {
        !encoded
    };
    Ok(f32::from_bits(bits))
}

/// Writes a sorted (order-preserving) f64 to the output.
pub fn write_sorted_f64(w: &mut impl Write, val: f64) -> io::Result<()> {
    let bits = val.to_bits();
    let encoded = if bits & 0x8000_0000_0000_0000 != 0 {
        !bits
    } else {
        bits ^ 0x8000_0000_0000_0000
    };
    let bytes = encoded.to_be_bytes();
    w.write_all(&bytes)
}

/// Reads a sorted (order-preserving) f64 from the input.
pub fn read_sorted_f64(r: &mut impl Read) -> io::Result<f64> {
    let mut buf = [0u8; 8];
    r.read_exact(&mut buf)?;
    let encoded = u64::from_be_bytes(buf);
    let bits = if encoded & 0x8000_0000_0000_0000 != 0 {
        encoded ^ 0x8000_0000_0000_0000
    } else {
        !encoded
    };
    Ok(f64::from_bits(bits))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn encode_i32(val: i32) -> Vec<u8> {
        let mut buf = Vec::new();
        write_packed_i32(&mut buf, val).unwrap();
        buf
    }

    fn decode_i32(buf: &[u8]) -> i32 {
        read_packed_i32(&mut Cursor::new(buf)).unwrap()
    }

    fn encode_i64(val: i64) -> Vec<u8> {
        let mut buf = Vec::new();
        write_packed_i64(&mut buf, val).unwrap();
        buf
    }

    fn decode_i64(buf: &[u8]) -> i64 {
        read_packed_i64(&mut Cursor::new(buf)).unwrap()
    }

    fn encode_si32(val: i32) -> Vec<u8> {
        let mut buf = Vec::new();
        write_sorted_i32(&mut buf, val).unwrap();
        buf
    }

    fn decode_si32(buf: &[u8]) -> i32 {
        read_sorted_i32(&mut Cursor::new(buf)).unwrap()
    }

    fn encode_si64(val: i64) -> Vec<u8> {
        let mut buf = Vec::new();
        write_sorted_i64(&mut buf, val).unwrap();
        buf
    }

    fn decode_si64(buf: &[u8]) -> i64 {
        read_sorted_i64(&mut Cursor::new(buf)).unwrap()
    }

    // -----------------------------------------------------------------------
    // Packed i32 -- all 5 size tiers
    // -----------------------------------------------------------------------

    #[test]
    fn test_packed_i32_size_tier1_single_byte() {
        // Values in [-119, 119] occupy exactly 1 byte
        for val in [-119i32, -1, 0, 1, 119] {
            let buf = encode_i32(val);
            assert_eq!(buf.len(), 1, "expected 1 byte for {}", val);
            assert_eq!(packed_i32_size(val), 1);
            assert_eq!(decode_i32(&buf), val);
        }
    }

    #[test]
    fn test_packed_i32_size_tier2_two_bytes() {
        // Positive: 120..=374 (119+1 .. 119+255)
        for val in [120i32, 374, 256, -120, -374] {
            let buf = encode_i32(val);
            assert_eq!(buf.len(), 2, "expected 2 bytes for {}", val);
            assert_eq!(packed_i32_size(val), 2);
            assert_eq!(decode_i32(&buf), val);
        }
    }

    #[test]
    fn test_packed_i32_size_tier3_three_bytes() {
        // Positive: 375..=65654 (119+256 .. 119+65535)
        for val in [375i32, 65654, -375, -65654] {
            let buf = encode_i32(val);
            assert_eq!(buf.len(), 3, "expected 3 bytes for {}", val);
            assert_eq!(packed_i32_size(val), 3);
            assert_eq!(decode_i32(&buf), val);
        }
    }

    #[test]
    fn test_packed_i32_size_tier4_four_bytes() {
        // Positive: 65655..=16777334 (119+65536 .. 119+16777215)
        for val in [65655i32, 16_777_334, -65655, -16_777_334] {
            let buf = encode_i32(val);
            assert_eq!(buf.len(), 4, "expected 4 bytes for {}", val);
            assert_eq!(packed_i32_size(val), 4);
            assert_eq!(decode_i32(&buf), val);
        }
    }

    #[test]
    fn test_packed_i32_size_tier5_five_bytes() {
        for val in [i32::MAX, i32::MIN, 16_777_335, -16_777_335] {
            let buf = encode_i32(val);
            assert_eq!(buf.len(), 5, "expected 5 bytes for {}", val);
            assert_eq!(packed_i32_size(val), 5);
            assert_eq!(decode_i32(&buf), val);
        }
    }

    #[test]
    fn test_packed_i32_roundtrip() {
        let values = [
            0, 1, -1, 119, -119, 120, -120, 127, -128, 256,
            -256, 65535, -65536, i32::MAX, i32::MIN,
        ];
        for &val in &values {
            assert_eq!(decode_i32(&encode_i32(val)), val, "roundtrip failed for {}", val);
        }
    }

    // Port of JE's testIntRange for boundary values
    #[test]
    fn test_packed_i32_boundary_values() {
        let v119 = 119i32;
        let max1 = 0xFF_i32;

        // Tier 1 boundaries
        assert_eq!(packed_i32_size(-v119), 1);
        assert_eq!(packed_i32_size(v119), 1);

        // Tier 2 boundaries
        assert_eq!(packed_i32_size(-max1 - v119), 2);
        assert_eq!(packed_i32_size(-1 - v119), 2);
        assert_eq!(packed_i32_size(1 + v119), 2);
        assert_eq!(packed_i32_size(max1 + v119), 2);

        // Roundtrip all boundary values
        for &val in &[-max1 - v119, -1 - v119, 1 + v119, max1 + v119] {
            assert_eq!(decode_i32(&encode_i32(val)), val);
        }
    }

    // -----------------------------------------------------------------------
    // Packed i64 -- all 9 size tiers
    // -----------------------------------------------------------------------

    #[test]
    fn test_packed_i64_size_tier1() {
        for val in [-119i64, 0, 119] {
            assert_eq!(packed_i64_size(val), 1);
            assert_eq!(decode_i64(&encode_i64(val)), val);
        }
    }

    #[test]
    fn test_packed_i64_size_tier2() {
        for val in [120i64, 374, -120, -374] {
            let buf = encode_i64(val);
            assert_eq!(buf.len(), 2, "expected 2 bytes for {}", val);
            assert_eq!(decode_i64(&buf), val);
        }
    }

    #[test]
    fn test_packed_i64_size_tier3() {
        for val in [375i64, 65654, -375, -65654] {
            let buf = encode_i64(val);
            assert_eq!(buf.len(), 3, "expected 3 bytes for {}", val);
            assert_eq!(decode_i64(&buf), val);
        }
    }

    #[test]
    fn test_packed_i64_size_tier4() {
        for val in [65655i64, 16_777_334, -65655, -16_777_334] {
            let buf = encode_i64(val);
            assert_eq!(buf.len(), 4, "expected 4 bytes for {}", val);
            assert_eq!(decode_i64(&buf), val);
        }
    }

    #[test]
    fn test_packed_i64_size_tier5() {
        for val in [16_777_335i64, 4_294_967_414, -16_777_335, -4_294_967_414] {
            let buf = encode_i64(val);
            assert_eq!(buf.len(), 5, "expected 5 bytes for {}", val);
            assert_eq!(decode_i64(&buf), val);
        }
    }

    #[test]
    fn test_packed_i64_size_tier6() {
        let start = 4_294_967_414i64 + 1;
        let end   = 119i64 + 0xFF_FFFF_FFFFi64; // 119 + 2^40-1 = max 5-byte magnitude
        for val in [start, end, -start, -end] {
            let buf = encode_i64(val);
            assert_eq!(buf.len(), 6, "expected 6 bytes for {}", val);
            assert_eq!(decode_i64(&buf), val);
        }
    }

    #[test]
    fn test_packed_i64_size_tier9() {
        for val in [i64::MAX, i64::MIN] {
            let buf = encode_i64(val);
            assert_eq!(buf.len(), 9, "expected 9 bytes for {}", val);
            assert_eq!(decode_i64(&buf), val);
        }
    }

    #[test]
    fn test_packed_i64_roundtrip() {
        let values = [0i64, 1, -1, 119, -119, 120, i32::MAX as i64,
                      i32::MIN as i64, i64::MAX, i64::MIN];
        for &val in &values {
            assert_eq!(decode_i64(&encode_i64(val)), val, "roundtrip failed for {}", val);
        }
    }

    // -----------------------------------------------------------------------
    // Sorted i32
    // -----------------------------------------------------------------------

    #[test]
    fn test_sorted_i32_roundtrip() {
        let values = [i32::MIN, -1000, -120, -119, -1, 0, 1, 120, 121, 1000, i32::MAX];
        for &val in &values {
            let decoded = decode_si32(&encode_si32(val));
            assert_eq!(val, decoded, "roundtrip failed for {}", val);
        }
    }

    #[test]
    fn test_sorted_i32_ordering() {
        let values: Vec<i32> = vec![i32::MIN, -1000, -120, -119, -1, 0, 1, 120, 121, 1000, i32::MAX];
        let encoded: Vec<Vec<u8>> = values.iter().map(|&v| encode_si32(v)).collect();
        for i in 0..encoded.len() - 1 {
            assert!(
                encoded[i] < encoded[i + 1],
                "sort order violated: encoded({}) >= encoded({})",
                values[i], values[i + 1]
            );
        }
    }

    #[test]
    fn test_sorted_i32_single_byte_range() {
        // [-119, 120] should all encode to 1 byte
        for val in [-119i32, 0, 120] {
            let buf = encode_si32(val);
            assert_eq!(buf.len(), 1, "expected 1 byte for {}", val);
        }
    }

    // -----------------------------------------------------------------------
    // Sorted i64
    // -----------------------------------------------------------------------

    #[test]
    fn test_sorted_i64_roundtrip() {
        let values: Vec<i64> = vec![i64::MIN, -1000, -120, -119, -1, 0, 1, 120, 121, 1000, i64::MAX];
        for &val in &values {
            let decoded = decode_si64(&encode_si64(val));
            assert_eq!(val, decoded, "roundtrip failed for {}", val);
        }
    }

    #[test]
    fn test_sorted_i64_ordering() {
        let values: Vec<i64> = vec![i64::MIN, -1_000_000_000_000i64, -1000, -119, 0, 120, 1000, 1_000_000_000_000i64, i64::MAX];
        let encoded: Vec<Vec<u8>> = values.iter().map(|&v| encode_si64(v)).collect();
        for i in 0..encoded.len() - 1 {
            assert!(
                encoded[i] < encoded[i + 1],
                "sort order violated: encoded({}) >= encoded({})",
                values[i], values[i + 1]
            );
        }
    }

    // -----------------------------------------------------------------------
    // Sorted f32 / f64
    // -----------------------------------------------------------------------

    #[test]
    fn test_sorted_f32_roundtrip() {
        let values = [f32::NEG_INFINITY, -1.5, -0.0, 0.0, 1.5, f32::INFINITY];
        for &val in &values {
            let mut buf = Vec::new();
            write_sorted_f32(&mut buf, val).unwrap();
            let result = read_sorted_f32(&mut Cursor::new(&buf)).unwrap();
            assert_eq!(val.to_bits(), result.to_bits());
        }
    }

    #[test]
    fn test_sorted_f32_ordering() {
        let values = [f32::NEG_INFINITY, -1.0, -0.0, 0.0, 1.0, f32::INFINITY];
        let encoded: Vec<Vec<u8>> = values.iter().map(|&v| {
            let mut buf = Vec::new();
            write_sorted_f32(&mut buf, v).unwrap();
            buf
        }).collect();
        for i in 0..encoded.len() - 1 {
            assert!(
                encoded[i] <= encoded[i + 1],
                "sort order violated at index {} ({} vs {})",
                i, values[i], values[i + 1]
            );
        }
    }

    #[test]
    fn test_sorted_f64_roundtrip() {
        let values = [f64::NEG_INFINITY, -1.5, 0.0, 1.5, f64::INFINITY];
        for &val in &values {
            let mut buf = Vec::new();
            write_sorted_f64(&mut buf, val).unwrap();
            let result = read_sorted_f64(&mut Cursor::new(&buf)).unwrap();
            assert_eq!(val, result);
        }
    }

    #[test]
    fn test_sorted_f64_ordering() {
        let values = [f64::NEG_INFINITY, -1.0, -0.0, 0.0, 1.0, f64::INFINITY];
        let mut encoded: Vec<Vec<u8>> = Vec::new();
        for &val in &values {
            let mut buf = Vec::new();
            write_sorted_f64(&mut buf, val).unwrap();
            encoded.push(buf);
        }
        for i in 0..encoded.len() - 1 {
            assert!(
                encoded[i] <= encoded[i + 1],
                "sort order violated at index {} ({} vs {})",
                i,
                values[i],
                values[i + 1]
            );
        }
    }

    // -----------------------------------------------------------------------
    // Cross-format: packed vs sorted are DIFFERENT formats
    // -----------------------------------------------------------------------
    #[test]
    fn test_packed_and_sorted_are_distinct_formats() {
        // Encoding 200 (a 2-byte packed value) in both formats should differ
        let packed = encode_i32(200);
        let sorted = encode_si32(200);
        assert_ne!(packed, sorted, "packed and sorted should use different encodings");
    }

    // -----------------------------------------------------------------------
    // Port of JE PackedIntegerTest.testIntRange / testLongRange
    //
    // For each size tier, verify:
    //   1. write_packed produces exactly `expected_bytes` bytes for every value
    //      in the range.
    //   2. packed_i32_size / packed_i64_size returns `expected_bytes`.
    //   3. read_packed(write_packed(v)) == v (round-trip).
    // -----------------------------------------------------------------------

    /// Encode every i32 in [first, last] into a single buffer, then read them
    /// back and assert: each encoded length == `expected_bytes` and the decoded
    /// value equals the original.
    fn check_i32_range(first: i32, last: i32, expected_bytes: usize) {
        // Write pass
        let mut buf: Vec<u8> = Vec::new();
        for i in first..=last {
            let before = buf.len();
            write_packed_i32(&mut buf, i).unwrap();
            let written = buf.len() - before;
            assert_eq!(
                written, expected_bytes,
                "write length wrong for i32 value {i}: got {written}, expected {expected_bytes}"
            );
            assert_eq!(
                packed_i32_size(i),
                expected_bytes,
                "size() wrong for i32 value {i}"
            );
        }
        // Read pass
        let mut cur = std::io::Cursor::new(&buf);
        for i in first..=last {
            let start = cur.position();
            let got = read_packed_i32(&mut cur).unwrap();
            let read_len = (cur.position() - start) as usize;
            assert_eq!(
                read_len, expected_bytes,
                "read length wrong for i32 value {i}: got {read_len}, expected {expected_bytes}"
            );
            assert_eq!(got, i, "round-trip failed for i32 value {i}");
        }
    }

    fn check_i64_range(first: i64, last: i64, expected_bytes: usize) {
        let mut buf: Vec<u8> = Vec::new();
        for i in first..=last {
            let before = buf.len();
            write_packed_i64(&mut buf, i).unwrap();
            let written = buf.len() - before;
            assert_eq!(
                written, expected_bytes,
                "write length wrong for i64 value {i}: got {written}, expected {expected_bytes}"
            );
            assert_eq!(
                packed_i64_size(i),
                expected_bytes,
                "size() wrong for i64 value {i}"
            );
        }
        let mut cur = std::io::Cursor::new(&buf);
        for i in first..=last {
            let start = cur.position();
            let got = read_packed_i64(&mut cur).unwrap();
            let read_len = (cur.position() - start) as usize;
            assert_eq!(
                read_len, expected_bytes,
                "read length wrong for i64 value {i}: got {read_len}, expected {expected_bytes}"
            );
            assert_eq!(got, i, "round-trip failed for i64 value {i}");
        }
    }

    // JE constants
    const V119: i64 = 119;
    const MAX_1: i64 = 0xFF;
    const MAX_2: i64 = 0xFFFF;
    const MAX_3: i64 = 0xFF_FFFF;
    const MAX_4: i64 = 0xFFFF_FFFF;
    const MAX_5: i64 = 0xFF_FFFF_FFFF;
    const MAX_6: i64 = 0xFFFF_FFFF_FFFF;
    const MAX_7: i64 = 0xFF_FFFF_FFFF_FFFF;

    // --- i32 range tests (ported from JE testIntRange) ---

    #[test]
    fn test_je_int_range_tier1() {
        check_i32_range(-V119 as i32, V119 as i32, 1);
    }

    #[test]
    fn test_je_int_range_tier2_neg() {
        // [-MAX_1-119, -1-119]  =>  2 bytes
        let first = (-MAX_1 - V119) as i32;
        let last = (-1 - V119) as i32;
        check_i32_range(first, last, 2);
    }

    #[test]
    fn test_je_int_range_tier2_pos() {
        // [1+119, MAX_1+119]  =>  2 bytes
        let first = (1 + V119) as i32;
        let last = (MAX_1 + V119) as i32;
        check_i32_range(first, last, 2);
    }

    #[test]
    fn test_je_int_range_tier3_neg_low() {
        let first = (-MAX_2 - V119) as i32;
        let last = (-MAX_2 + 99) as i32;
        check_i32_range(first, last, 3);
    }

    #[test]
    fn test_je_int_range_tier3_neg_high() {
        let first = (-MAX_1 - V119 - 99) as i32;
        let last = (-MAX_1 - V119 - 1) as i32;
        check_i32_range(first, last, 3);
    }

    #[test]
    fn test_je_int_range_tier3_pos_low() {
        let first = (MAX_1 + V119 + 1) as i32;
        let last = (MAX_1 + V119 + 99) as i32;
        check_i32_range(first, last, 3);
    }

    #[test]
    fn test_je_int_range_tier3_pos_high() {
        let first = (MAX_2 - 99) as i32;
        let last = (MAX_2 + V119) as i32;
        check_i32_range(first, last, 3);
    }

    #[test]
    fn test_je_int_range_tier4_neg_low() {
        let first = (-MAX_3 - V119) as i32;
        let last = (-MAX_3 + 99) as i32;
        check_i32_range(first, last, 4);
    }

    #[test]
    fn test_je_int_range_tier4_neg_high() {
        let first = (-MAX_2 - V119 - 99) as i32;
        let last = (-MAX_2 - V119 - 1) as i32;
        check_i32_range(first, last, 4);
    }

    #[test]
    fn test_je_int_range_tier4_pos_low() {
        let first = (MAX_2 + V119 + 1) as i32;
        let last = (MAX_2 + V119 + 99) as i32;
        check_i32_range(first, last, 4);
    }

    #[test]
    fn test_je_int_range_tier4_pos_high() {
        let first = (MAX_3 - 99) as i32;
        let last = (MAX_3 + V119) as i32;
        check_i32_range(first, last, 4);
    }

    #[test]
    fn test_je_int_range_tier5_min() {
        check_i32_range(i32::MIN, i32::MIN + 99, 5);
    }

    #[test]
    fn test_je_int_range_tier5_max() {
        check_i32_range(i32::MAX - 99, i32::MAX, 5);
    }

    // --- i64 range tests (ported from JE testLongRange) ---

    #[test]
    fn test_je_long_range_tier1() {
        check_i64_range(-V119, V119, 1);
    }

    #[test]
    fn test_je_long_range_tier2_neg() {
        check_i64_range(-MAX_1 - V119, -1 - V119, 2);
    }

    #[test]
    fn test_je_long_range_tier2_pos() {
        check_i64_range(1 + V119, MAX_1 + V119, 2);
    }

    #[test]
    fn test_je_long_range_tier3_neg_low() {
        check_i64_range(-MAX_2 - V119, -MAX_2 + 99, 3);
    }

    #[test]
    fn test_je_long_range_tier3_neg_high() {
        check_i64_range(-MAX_1 - V119 - 99, -MAX_1 - V119 - 1, 3);
    }

    #[test]
    fn test_je_long_range_tier3_pos_low() {
        check_i64_range(MAX_1 + V119 + 1, MAX_1 + V119 + 99, 3);
    }

    #[test]
    fn test_je_long_range_tier3_pos_high() {
        check_i64_range(MAX_2 - 99, MAX_2 + V119, 3);
    }

    #[test]
    fn test_je_long_range_tier4_neg_low() {
        check_i64_range(-MAX_3 - V119, -MAX_3 + 99, 4);
    }

    #[test]
    fn test_je_long_range_tier4_neg_high() {
        check_i64_range(-MAX_2 - V119 - 99, -MAX_2 - V119 - 1, 4);
    }

    #[test]
    fn test_je_long_range_tier4_pos_low() {
        check_i64_range(MAX_2 + V119 + 1, MAX_2 + V119 + 99, 4);
    }

    #[test]
    fn test_je_long_range_tier4_pos_high() {
        check_i64_range(MAX_3 - 99, MAX_3 + V119, 4);
    }

    #[test]
    fn test_je_long_range_tier5_neg_low() {
        check_i64_range(-MAX_4 - V119, -MAX_4 + 99, 5);
    }

    #[test]
    fn test_je_long_range_tier5_neg_high() {
        check_i64_range(-MAX_3 - V119 - 99, -MAX_3 - V119 - 1, 5);
    }

    #[test]
    fn test_je_long_range_tier5_pos_low() {
        check_i64_range(MAX_3 + V119 + 1, MAX_3 + V119 + 99, 5);
    }

    #[test]
    fn test_je_long_range_tier5_pos_high() {
        check_i64_range(MAX_4 - 99, MAX_4 + V119, 5);
    }

    #[test]
    fn test_je_long_range_tier6_neg_low() {
        check_i64_range(-MAX_5 - V119, -MAX_5 + 99, 6);
    }

    #[test]
    fn test_je_long_range_tier6_neg_high() {
        check_i64_range(-MAX_4 - V119 - 99, -MAX_4 - V119 - 1, 6);
    }

    #[test]
    fn test_je_long_range_tier6_pos_low() {
        check_i64_range(MAX_4 + V119 + 1, MAX_4 + V119 + 99, 6);
    }

    #[test]
    fn test_je_long_range_tier6_pos_high() {
        check_i64_range(MAX_5 - 99, MAX_5 + V119, 6);
    }

    #[test]
    fn test_je_long_range_tier7_neg_low() {
        check_i64_range(-MAX_6 - V119, -MAX_6 + 99, 7);
    }

    #[test]
    fn test_je_long_range_tier7_neg_high() {
        check_i64_range(-MAX_5 - V119 - 99, -MAX_5 - V119 - 1, 7);
    }

    #[test]
    fn test_je_long_range_tier7_pos_low() {
        check_i64_range(MAX_5 + V119 + 1, MAX_5 + V119 + 99, 7);
    }

    #[test]
    fn test_je_long_range_tier7_pos_high() {
        check_i64_range(MAX_6 - 99, MAX_6 + V119, 7);
    }

    #[test]
    fn test_je_long_range_tier8_neg_low() {
        check_i64_range(-MAX_7 - V119, -MAX_7 + 99, 8);
    }

    #[test]
    fn test_je_long_range_tier8_neg_high() {
        check_i64_range(-MAX_6 - V119 - 99, -MAX_6 - V119 - 1, 8);
    }

    #[test]
    fn test_je_long_range_tier8_pos_low() {
        check_i64_range(MAX_6 + V119 + 1, MAX_6 + V119 + 99, 8);
    }

    #[test]
    fn test_je_long_range_tier8_pos_high() {
        check_i64_range(MAX_7 - 99, MAX_7 + V119, 8);
    }

    #[test]
    fn test_je_long_range_tier9_min() {
        check_i64_range(i64::MIN, i64::MIN + 99, 9);
    }

    #[test]
    fn test_je_long_range_tier9_max() {
        // JE uses Long.MAX_VALUE - 1 as the upper bound (exclusive of MAX)
        check_i64_range(i64::MAX - 99, i64::MAX - 1, 9);
    }

    // -----------------------------------------------------------------------
    // Port of JE testSortOrder: sorted-encoded bytes must compare in the
    // same order as the integer values (the crucial correctness invariant).
    //
    // NOTE: The *packed* format (write_packed_i32/i64) uses little-endian
    // encoding and does NOT preserve byte-wise sort order.  Only the *sorted*
    // format (write_sorted_i32/i64) is sort-order preserving.  JE's
    // testSortOrder exercises the sorted format, so we do the same here with
    // a broader set of boundary values than the existing sorted ordering tests.
    // -----------------------------------------------------------------------

    #[test]
    fn test_sorted_i32_sort_order_full_tiers() {
        // One representative from each size tier in sorted encoding,
        // spanning all boundary values.
        let mut values: Vec<i32> = vec![
            i32::MIN,
            i32::MIN + 1,
            -1_000_000,
            -65_654,
            -374,
            -120,
            -119,
            -1,
            0,
            1,
            119,
            120,
            121,
            374,
            65_654,
            1_000_000,
            i32::MAX - 1,
            i32::MAX,
        ];
        values.sort();
        values.dedup();

        let encoded: Vec<Vec<u8>> = values.iter().map(|&v| encode_si32(v)).collect();

        for i in 0..encoded.len() - 1 {
            assert!(
                encoded[i] < encoded[i + 1],
                "sorted i32 sort-order violated: encode({}) >= encode({})",
                values[i],
                values[i + 1]
            );
        }
    }

    #[test]
    fn test_sorted_i64_sort_order_full_tiers() {
        let mut values: Vec<i64> = vec![
            i64::MIN,
            i64::MIN + 1,
            -(MAX_7 + V119),
            -(MAX_6 + V119),
            -(MAX_5 + V119),
            -(MAX_4 + V119),
            -(MAX_3 + V119),
            -(MAX_2 + V119),
            -(MAX_1 + V119),
            -V119,
            -1,
            0,
            1,
            V119,
            MAX_1 + V119,
            MAX_2 + V119,
            MAX_3 + V119,
            MAX_4 + V119,
            MAX_5 + V119,
            MAX_6 + V119,
            MAX_7 + V119,
            i64::MAX - 1,
            i64::MAX,
        ];
        values.sort();
        values.dedup();

        let encoded: Vec<Vec<u8>> = values.iter().map(|&v| encode_si64(v)).collect();

        for i in 0..encoded.len() - 1 {
            assert!(
                encoded[i] < encoded[i + 1],
                "sorted i64 sort-order violated: encode({}) >= encode({})",
                values[i],
                values[i + 1]
            );
        }
    }

    // -----------------------------------------------------------------------
    // Port of JE testIntArray: array encode/decode round-trip
    // Encode multiple values sequentially into a buffer, then decode them
    // back in order and verify each value.
    // -----------------------------------------------------------------------

    #[test]
    fn test_packed_i32_array_roundtrip() {
        let values: Vec<i32> = vec![
            0, 1, -1, 119, -119, 120, -120, 374, -374, 375, -375,
            65_654, -65_654, 65_655, -65_655, i32::MAX, i32::MIN,
        ];

        // Encode all values into one buffer
        let mut buf: Vec<u8> = Vec::new();
        let mut sizes: Vec<usize> = Vec::new();
        for &v in &values {
            let before = buf.len();
            write_packed_i32(&mut buf, v).unwrap();
            sizes.push(buf.len() - before);
        }

        // Verify declared sizes match actual write sizes
        for (i, (&v, &sz)) in values.iter().zip(sizes.iter()).enumerate() {
            assert_eq!(
                packed_i32_size(v),
                sz,
                "size mismatch at index {i} for value {v}"
            );
        }

        // Decode all values from the buffer
        let mut cur = std::io::Cursor::new(&buf);
        for (i, &expected) in values.iter().enumerate() {
            let got = read_packed_i32(&mut cur).unwrap();
            assert_eq!(got, expected, "array decode mismatch at index {i}");
        }
    }

    #[test]
    fn test_packed_i64_array_roundtrip() {
        let values: Vec<i64> = vec![
            0,
            1,
            -1,
            119,
            -119,
            120,
            -120,
            MAX_1 + V119,
            -(MAX_1 + V119),
            MAX_2 + V119,
            -(MAX_2 + V119),
            MAX_3 + V119,
            -(MAX_3 + V119),
            MAX_4 + V119,
            -(MAX_4 + V119),
            i64::MAX,
            i64::MIN,
        ];

        let mut buf: Vec<u8> = Vec::new();
        let mut sizes: Vec<usize> = Vec::new();
        for &v in &values {
            let before = buf.len();
            write_packed_i64(&mut buf, v).unwrap();
            sizes.push(buf.len() - before);
        }

        for (i, (&v, &sz)) in values.iter().zip(sizes.iter()).enumerate() {
            assert_eq!(
                packed_i64_size(v),
                sz,
                "size mismatch at index {i} for value {v}"
            );
        }

        let mut cur = std::io::Cursor::new(&buf);
        for (i, &expected) in values.iter().enumerate() {
            let got = read_packed_i64(&mut cur).unwrap();
            assert_eq!(got, expected, "array decode mismatch at index {i}");
        }
    }
}
