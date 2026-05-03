//! Simple binary serializer for entity persistence.
//!
//! Provides a callback-based `EntitySerializer` implementation and
//! helper types (`FieldEncoder`/`FieldDecoder`) for common binary
//! encoding patterns. This is suitable for testing and simple
//! applications that do not need a schema evolution mechanism.

use crate::entity::Entity;
use crate::entity_serializer::EntitySerializer;
use crate::error::{PersistError, Result};

/// A simple binary serializer that delegates to user-provided closures.
///
/// This is the easiest way to get an `EntitySerializer` without writing
/// a full struct implementation. The user supplies serialize and
/// deserialize functions, typically using `FieldEncoder`/`FieldDecoder`
/// for the byte-level encoding.
///
/// # Example
///
/// ```
/// use noxu_persist::entity::Entity;
/// use noxu_persist::simple_serializer::{SimpleSerializer, FieldEncoder, FieldDecoder};
/// use noxu_persist::entity_serializer::EntitySerializer;
///
/// #[derive(Debug, Clone, PartialEq)]
/// struct Item { id: u64, name: String }
///
/// impl Entity for Item {
///     type PrimaryKey = u64;
///     fn primary_key(&self) -> &u64 { &self.id }
///     fn entity_name() -> &'static str { "Item" }
/// }
///
/// let ser = SimpleSerializer::new(
///     |item: &Item| {
///         let mut enc = FieldEncoder::new();
///         enc.write_u64(item.id);
///         enc.write_string(&item.name);
///         Ok(enc.finish())
///     },
///     |bytes| {
///         let mut dec = FieldDecoder::new(bytes);
///         Ok(Item {
///             id: dec.read_u64()?,
///             name: dec.read_string()?,
///         })
///     },
/// );
///
/// let item = Item { id: 1, name: "test".into() };
/// let bytes = ser.serialize(&item).unwrap();
/// let decoded = ser.deserialize(&bytes).unwrap();
/// assert_eq!(item, decoded);
/// ```
pub struct SimpleSerializer<E: Entity> {
    serialize_fn: Box<dyn Fn(&E) -> Result<Vec<u8>> + Send + Sync>,
    deserialize_fn: Box<dyn Fn(&[u8]) -> Result<E> + Send + Sync>,
}

impl<E: Entity> SimpleSerializer<E> {
    /// Creates a new `SimpleSerializer` with the given closures.
    ///
    /// # Arguments
    /// * `serialize` - A function that encodes an entity to bytes.
    /// * `deserialize` - A function that decodes an entity from bytes.
    pub fn new<S, D>(serialize: S, deserialize: D) -> Self
    where
        S: Fn(&E) -> Result<Vec<u8>> + Send + Sync + 'static,
        D: Fn(&[u8]) -> Result<E> + Send + Sync + 'static,
    {
        Self {
            serialize_fn: Box::new(serialize),
            deserialize_fn: Box::new(deserialize),
        }
    }
}

impl<E: Entity> EntitySerializer<E> for SimpleSerializer<E> {
    fn serialize(&self, entity: &E) -> Result<Vec<u8>> {
        (self.serialize_fn)(entity)
    }

    fn deserialize(&self, bytes: &[u8]) -> Result<E> {
        (self.deserialize_fn)(bytes)
    }
}

// ---------------------------------------------------------------------------
// Free-standing encoding helpers
// ---------------------------------------------------------------------------

/// Encode a `u64` as 8 big-endian bytes.
pub fn encode_u64(value: u64) -> [u8; 8] {
    value.to_be_bytes()
}

/// Decode a `u64` from big-endian bytes.
///
/// # Errors
/// Returns `PersistError::SerializationError` if `bytes` is shorter than 8.
pub fn decode_u64(bytes: &[u8]) -> Result<u64> {
    if bytes.len() < 8 {
        return Err(PersistError::SerializationError(format!(
            "expected at least 8 bytes for u64, got {}",
            bytes.len()
        )));
    }
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&bytes[..8]);
    Ok(u64::from_be_bytes(buf))
}

/// Encode a string as a 4-byte big-endian length prefix followed by UTF-8 bytes.
pub fn encode_string(s: &str) -> Vec<u8> {
    let len = s.len() as u32;
    let mut buf = Vec::with_capacity(4 + s.len());
    buf.extend_from_slice(&len.to_be_bytes());
    buf.extend_from_slice(s.as_bytes());
    buf
}

/// Decode a length-prefixed string starting at `*offset` in `bytes`.
///
/// On success, `*offset` is advanced past the decoded string.
///
/// # Errors
/// Returns `PersistError::SerializationError` on truncated data or invalid UTF-8.
pub fn decode_string(bytes: &[u8], offset: &mut usize) -> Result<String> {
    if *offset + 4 > bytes.len() {
        return Err(PersistError::SerializationError(
            "not enough bytes for string length prefix".to_string(),
        ));
    }
    let mut len_buf = [0u8; 4];
    len_buf.copy_from_slice(&bytes[*offset..*offset + 4]);
    let len = u32::from_be_bytes(len_buf) as usize;
    *offset += 4;

    if *offset + len > bytes.len() {
        return Err(PersistError::SerializationError(format!(
            "expected {} bytes for string, only {} available",
            len,
            bytes.len() - *offset
        )));
    }
    let s = String::from_utf8(bytes[*offset..*offset + len].to_vec()).map_err(
        |e| {
            PersistError::SerializationError(format!(
                "invalid UTF-8 in string: {}",
                e
            ))
        },
    )?;
    *offset += len;
    Ok(s)
}

// ---------------------------------------------------------------------------
// FieldEncoder / FieldDecoder
// ---------------------------------------------------------------------------

/// A sequential field encoder that appends typed values to a byte buffer.
///
/// Each `write_*` method appends a value in a format that the corresponding
/// `FieldDecoder::read_*` method can reconstruct. Variable-length values
/// (strings, byte slices) are length-prefixed with a 4-byte big-endian u32.
///
/// # Example
///
/// ```
/// use noxu_persist::simple_serializer::{FieldEncoder, FieldDecoder};
///
/// let mut enc = FieldEncoder::new();
/// enc.write_u64(42);
/// enc.write_string("hello");
/// enc.write_bool(true);
/// let bytes = enc.finish();
///
/// let mut dec = FieldDecoder::new(&bytes);
/// assert_eq!(dec.read_u64().unwrap(), 42);
/// assert_eq!(dec.read_string().unwrap(), "hello");
/// assert_eq!(dec.read_bool().unwrap(), true);
/// ```
#[derive(Debug, Clone)]
pub struct FieldEncoder {
    buf: Vec<u8>,
}

impl Default for FieldEncoder {
    fn default() -> Self {
        Self::new()
    }
}

impl FieldEncoder {
    /// Creates a new, empty encoder.
    pub fn new() -> Self {
        Self { buf: Vec::new() }
    }

    /// Creates a new encoder with the given initial capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self { buf: Vec::with_capacity(capacity) }
    }

    /// Writes a `u8`.
    pub fn write_u8(&mut self, v: u8) {
        self.buf.push(v);
    }

    /// Writes a `u16` in big-endian byte order.
    pub fn write_u16(&mut self, v: u16) {
        self.buf.extend_from_slice(&v.to_be_bytes());
    }

    /// Writes a `u32` in big-endian byte order.
    pub fn write_u32(&mut self, v: u32) {
        self.buf.extend_from_slice(&v.to_be_bytes());
    }

    /// Writes a `u64` in big-endian byte order.
    pub fn write_u64(&mut self, v: u64) {
        self.buf.extend_from_slice(&v.to_be_bytes());
    }

    /// Writes an `i8`.
    pub fn write_i8(&mut self, v: i8) {
        self.buf.push(v as u8);
    }

    /// Writes an `i16` in big-endian byte order.
    pub fn write_i16(&mut self, v: i16) {
        self.buf.extend_from_slice(&v.to_be_bytes());
    }

    /// Writes an `i32` in big-endian byte order.
    pub fn write_i32(&mut self, v: i32) {
        self.buf.extend_from_slice(&v.to_be_bytes());
    }

    /// Writes an `i64` in big-endian byte order.
    pub fn write_i64(&mut self, v: i64) {
        self.buf.extend_from_slice(&v.to_be_bytes());
    }

    /// Writes a `f32` in big-endian byte order.
    pub fn write_f32(&mut self, v: f32) {
        self.buf.extend_from_slice(&v.to_be_bytes());
    }

    /// Writes a `f64` in big-endian byte order.
    pub fn write_f64(&mut self, v: f64) {
        self.buf.extend_from_slice(&v.to_be_bytes());
    }

    /// Writes a `bool` as a single byte (`1` for true, `0` for false).
    pub fn write_bool(&mut self, v: bool) {
        self.buf.push(if v { 1 } else { 0 });
    }

    /// Writes a length-prefixed UTF-8 string.
    pub fn write_string(&mut self, s: &str) {
        let len = s.len() as u32;
        self.buf.extend_from_slice(&len.to_be_bytes());
        self.buf.extend_from_slice(s.as_bytes());
    }

    /// Writes a length-prefixed byte slice.
    pub fn write_bytes(&mut self, b: &[u8]) {
        let len = b.len() as u32;
        self.buf.extend_from_slice(&len.to_be_bytes());
        self.buf.extend_from_slice(b);
    }

    /// Writes an optional string. Encodes a leading `bool` tag followed by
    /// the string value when `Some`.
    pub fn write_option_string(&mut self, s: &Option<String>) {
        match s {
            Some(val) => {
                self.write_bool(true);
                self.write_string(val);
            }
            None => {
                self.write_bool(false);
            }
        }
    }

    /// Writes an optional `u64`. Encodes a leading `bool` tag followed by
    /// the value when `Some`.
    pub fn write_option_u64(&mut self, v: &Option<u64>) {
        match v {
            Some(val) => {
                self.write_bool(true);
                self.write_u64(*val);
            }
            None => {
                self.write_bool(false);
            }
        }
    }

    /// Consumes the encoder and returns the accumulated byte buffer.
    pub fn finish(self) -> Vec<u8> {
        self.buf
    }

    /// Returns the number of bytes written so far.
    pub fn len(&self) -> usize {
        self.buf.len()
    }

    /// Returns `true` if no bytes have been written.
    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }
}

/// A sequential field decoder that reads typed values from a byte buffer.
///
/// Each `read_*` method advances an internal offset. The methods return
/// `PersistError::SerializationError` if there are not enough bytes
/// remaining.
pub struct FieldDecoder<'a> {
    data: &'a [u8],
    offset: usize,
}

impl<'a> FieldDecoder<'a> {
    /// Creates a new decoder positioned at the start of `data`.
    pub fn new(data: &'a [u8]) -> Self {
        Self { data, offset: 0 }
    }

    /// Returns the current byte offset within the buffer.
    pub fn position(&self) -> usize {
        self.offset
    }

    /// Returns the number of bytes remaining to be read.
    pub fn remaining(&self) -> usize {
        self.data.len().saturating_sub(self.offset)
    }

    /// Returns `true` if all bytes have been consumed.
    pub fn is_exhausted(&self) -> bool {
        self.offset >= self.data.len()
    }

    // --- helpers ---

    fn need(&self, n: usize) -> Result<()> {
        if self.offset + n > self.data.len() {
            return Err(PersistError::SerializationError(format!(
                "need {} bytes at offset {}, but only {} available",
                n,
                self.offset,
                self.data.len() - self.offset
            )));
        }
        Ok(())
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        self.need(n)?;
        let slice = &self.data[self.offset..self.offset + n];
        self.offset += n;
        Ok(slice)
    }

    // --- public readers ---

    /// Reads a `u8`.
    pub fn read_u8(&mut self) -> Result<u8> {
        let b = self.take(1)?;
        Ok(b[0])
    }

    /// Reads a `u16` in big-endian byte order.
    pub fn read_u16(&mut self) -> Result<u16> {
        let b = self.take(2)?;
        Ok(u16::from_be_bytes([b[0], b[1]]))
    }

    /// Reads a `u32` in big-endian byte order.
    pub fn read_u32(&mut self) -> Result<u32> {
        let b = self.take(4)?;
        Ok(u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }

    /// Reads a `u64` in big-endian byte order.
    pub fn read_u64(&mut self) -> Result<u64> {
        let b = self.take(8)?;
        let mut arr = [0u8; 8];
        arr.copy_from_slice(b);
        Ok(u64::from_be_bytes(arr))
    }

    /// Reads an `i8`.
    pub fn read_i8(&mut self) -> Result<i8> {
        let b = self.take(1)?;
        Ok(b[0] as i8)
    }

    /// Reads an `i16` in big-endian byte order.
    pub fn read_i16(&mut self) -> Result<i16> {
        let b = self.take(2)?;
        Ok(i16::from_be_bytes([b[0], b[1]]))
    }

    /// Reads an `i32` in big-endian byte order.
    pub fn read_i32(&mut self) -> Result<i32> {
        let b = self.take(4)?;
        Ok(i32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }

    /// Reads an `i64` in big-endian byte order.
    pub fn read_i64(&mut self) -> Result<i64> {
        let b = self.take(8)?;
        let mut arr = [0u8; 8];
        arr.copy_from_slice(b);
        Ok(i64::from_be_bytes(arr))
    }

    /// Reads an `f32` in big-endian byte order.
    pub fn read_f32(&mut self) -> Result<f32> {
        let b = self.take(4)?;
        Ok(f32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }

    /// Reads an `f64` in big-endian byte order.
    pub fn read_f64(&mut self) -> Result<f64> {
        let b = self.take(8)?;
        let mut arr = [0u8; 8];
        arr.copy_from_slice(b);
        Ok(f64::from_be_bytes(arr))
    }

    /// Reads a `bool` encoded as a single byte.
    pub fn read_bool(&mut self) -> Result<bool> {
        let b = self.take(1)?;
        Ok(b[0] != 0)
    }

    /// Reads a length-prefixed UTF-8 string.
    pub fn read_string(&mut self) -> Result<String> {
        let len = self.read_u32()? as usize;
        let b = self.take(len)?;
        String::from_utf8(b.to_vec()).map_err(|e| {
            PersistError::SerializationError(format!(
                "invalid UTF-8 in string field: {}",
                e
            ))
        })
    }

    /// Reads a length-prefixed byte slice.
    pub fn read_bytes(&mut self) -> Result<Vec<u8>> {
        let len = self.read_u32()? as usize;
        let b = self.take(len)?;
        Ok(b.to_vec())
    }

    /// Reads an optional string (bool tag + optional value).
    pub fn read_option_string(&mut self) -> Result<Option<String>> {
        let present = self.read_bool()?;
        if present { Ok(Some(self.read_string()?)) } else { Ok(None) }
    }

    /// Reads an optional `u64` (bool tag + optional value).
    pub fn read_option_u64(&mut self) -> Result<Option<u64>> {
        let present = self.read_bool()?;
        if present { Ok(Some(self.read_u64()?)) } else { Ok(None) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entity::Entity;

    // ------------------------------------------------------------------
    // Free-standing helpers
    // ------------------------------------------------------------------

    #[test]
    fn test_encode_decode_u64() {
        let encoded = encode_u64(42);
        let decoded = decode_u64(&encoded).unwrap();
        assert_eq!(decoded, 42);
    }

    #[test]
    fn test_encode_decode_u64_zero() {
        let encoded = encode_u64(0);
        let decoded = decode_u64(&encoded).unwrap();
        assert_eq!(decoded, 0);
    }

    #[test]
    fn test_encode_decode_u64_max() {
        let encoded = encode_u64(u64::MAX);
        let decoded = decode_u64(&encoded).unwrap();
        assert_eq!(decoded, u64::MAX);
    }

    #[test]
    fn test_decode_u64_too_short() {
        assert!(decode_u64(&[1, 2]).is_err());
    }

    #[test]
    fn test_encode_decode_string() {
        let encoded = encode_string("hello");
        let mut offset = 0;
        let decoded = decode_string(&encoded, &mut offset).unwrap();
        assert_eq!(decoded, "hello");
        assert_eq!(offset, encoded.len());
    }

    #[test]
    fn test_encode_decode_string_empty() {
        let encoded = encode_string("");
        let mut offset = 0;
        let decoded = decode_string(&encoded, &mut offset).unwrap();
        assert_eq!(decoded, "");
    }

    #[test]
    fn test_encode_decode_string_unicode() {
        let original = "caf\u{00E9} \u{1F600}";
        let encoded = encode_string(original);
        let mut offset = 0;
        let decoded = decode_string(&encoded, &mut offset).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn test_decode_string_truncated_length() {
        let mut offset = 0;
        assert!(decode_string(&[0, 0], &mut offset).is_err());
    }

    #[test]
    fn test_decode_string_truncated_payload() {
        // length says 100 but only 2 bytes available
        let data = [0u8, 0, 0, 100, 65, 66];
        let mut offset = 0;
        assert!(decode_string(&data, &mut offset).is_err());
    }

    // ------------------------------------------------------------------
    // FieldEncoder / FieldDecoder round-trips
    // ------------------------------------------------------------------

    #[test]
    fn test_field_u8_round_trip() {
        let mut enc = FieldEncoder::new();
        enc.write_u8(0);
        enc.write_u8(127);
        enc.write_u8(255);
        let bytes = enc.finish();
        let mut dec = FieldDecoder::new(&bytes);
        assert_eq!(dec.read_u8().unwrap(), 0);
        assert_eq!(dec.read_u8().unwrap(), 127);
        assert_eq!(dec.read_u8().unwrap(), 255);
        assert!(dec.is_exhausted());
    }

    #[test]
    fn test_field_u16_round_trip() {
        let mut enc = FieldEncoder::new();
        enc.write_u16(0);
        enc.write_u16(1000);
        enc.write_u16(u16::MAX);
        let bytes = enc.finish();
        let mut dec = FieldDecoder::new(&bytes);
        assert_eq!(dec.read_u16().unwrap(), 0);
        assert_eq!(dec.read_u16().unwrap(), 1000);
        assert_eq!(dec.read_u16().unwrap(), u16::MAX);
    }

    #[test]
    fn test_field_u32_round_trip() {
        let mut enc = FieldEncoder::new();
        enc.write_u32(0);
        enc.write_u32(123456);
        enc.write_u32(u32::MAX);
        let bytes = enc.finish();
        let mut dec = FieldDecoder::new(&bytes);
        assert_eq!(dec.read_u32().unwrap(), 0);
        assert_eq!(dec.read_u32().unwrap(), 123456);
        assert_eq!(dec.read_u32().unwrap(), u32::MAX);
    }

    #[test]
    fn test_field_u64_round_trip() {
        let mut enc = FieldEncoder::new();
        enc.write_u64(0);
        enc.write_u64(42);
        enc.write_u64(u64::MAX);
        let bytes = enc.finish();
        let mut dec = FieldDecoder::new(&bytes);
        assert_eq!(dec.read_u64().unwrap(), 0);
        assert_eq!(dec.read_u64().unwrap(), 42);
        assert_eq!(dec.read_u64().unwrap(), u64::MAX);
    }

    #[test]
    fn test_field_i8_round_trip() {
        let mut enc = FieldEncoder::new();
        enc.write_i8(-128);
        enc.write_i8(0);
        enc.write_i8(127);
        let bytes = enc.finish();
        let mut dec = FieldDecoder::new(&bytes);
        assert_eq!(dec.read_i8().unwrap(), -128);
        assert_eq!(dec.read_i8().unwrap(), 0);
        assert_eq!(dec.read_i8().unwrap(), 127);
    }

    #[test]
    fn test_field_i16_round_trip() {
        let mut enc = FieldEncoder::new();
        enc.write_i16(i16::MIN);
        enc.write_i16(0);
        enc.write_i16(i16::MAX);
        let bytes = enc.finish();
        let mut dec = FieldDecoder::new(&bytes);
        assert_eq!(dec.read_i16().unwrap(), i16::MIN);
        assert_eq!(dec.read_i16().unwrap(), 0);
        assert_eq!(dec.read_i16().unwrap(), i16::MAX);
    }

    #[test]
    fn test_field_i32_round_trip() {
        let mut enc = FieldEncoder::new();
        enc.write_i32(i32::MIN);
        enc.write_i32(-1);
        enc.write_i32(i32::MAX);
        let bytes = enc.finish();
        let mut dec = FieldDecoder::new(&bytes);
        assert_eq!(dec.read_i32().unwrap(), i32::MIN);
        assert_eq!(dec.read_i32().unwrap(), -1);
        assert_eq!(dec.read_i32().unwrap(), i32::MAX);
    }

    #[test]
    fn test_field_i64_round_trip() {
        let mut enc = FieldEncoder::new();
        enc.write_i64(i64::MIN);
        enc.write_i64(0);
        enc.write_i64(i64::MAX);
        let bytes = enc.finish();
        let mut dec = FieldDecoder::new(&bytes);
        assert_eq!(dec.read_i64().unwrap(), i64::MIN);
        assert_eq!(dec.read_i64().unwrap(), 0);
        assert_eq!(dec.read_i64().unwrap(), i64::MAX);
    }

    #[test]
    fn test_field_f32_round_trip() {
        let mut enc = FieldEncoder::new();
        enc.write_f32(0.0);
        enc.write_f32(std::f32::consts::PI);
        enc.write_f32(-1.5);
        let bytes = enc.finish();
        let mut dec = FieldDecoder::new(&bytes);
        assert_eq!(dec.read_f32().unwrap(), 0.0);
        assert_eq!(dec.read_f32().unwrap(), std::f32::consts::PI);
        assert_eq!(dec.read_f32().unwrap(), -1.5);
    }

    #[test]
    fn test_field_f64_round_trip() {
        let mut enc = FieldEncoder::new();
        enc.write_f64(0.0);
        enc.write_f64(std::f64::consts::E);
        enc.write_f64(-99.99);
        let bytes = enc.finish();
        let mut dec = FieldDecoder::new(&bytes);
        assert_eq!(dec.read_f64().unwrap(), 0.0);
        assert_eq!(dec.read_f64().unwrap(), std::f64::consts::E);
        assert_eq!(dec.read_f64().unwrap(), -99.99);
    }

    #[test]
    fn test_field_bool_round_trip() {
        let mut enc = FieldEncoder::new();
        enc.write_bool(true);
        enc.write_bool(false);
        let bytes = enc.finish();
        let mut dec = FieldDecoder::new(&bytes);
        assert!(dec.read_bool().unwrap());
        assert!(!dec.read_bool().unwrap());
    }

    #[test]
    fn test_field_string_round_trip() {
        let mut enc = FieldEncoder::new();
        enc.write_string("hello world");
        enc.write_string("");
        enc.write_string("\u{1F600}");
        let bytes = enc.finish();
        let mut dec = FieldDecoder::new(&bytes);
        assert_eq!(dec.read_string().unwrap(), "hello world");
        assert_eq!(dec.read_string().unwrap(), "");
        assert_eq!(dec.read_string().unwrap(), "\u{1F600}");
    }

    #[test]
    fn test_field_bytes_round_trip() {
        let mut enc = FieldEncoder::new();
        enc.write_bytes(&[1, 2, 3, 4, 5]);
        enc.write_bytes(&[]);
        let bytes = enc.finish();
        let mut dec = FieldDecoder::new(&bytes);
        assert_eq!(dec.read_bytes().unwrap(), vec![1, 2, 3, 4, 5]);
        assert_eq!(dec.read_bytes().unwrap(), Vec::<u8>::new());
    }

    #[test]
    fn test_field_option_string_round_trip() {
        let mut enc = FieldEncoder::new();
        enc.write_option_string(&Some("present".to_string()));
        enc.write_option_string(&None);
        let bytes = enc.finish();
        let mut dec = FieldDecoder::new(&bytes);
        assert_eq!(
            dec.read_option_string().unwrap(),
            Some("present".to_string())
        );
        assert_eq!(dec.read_option_string().unwrap(), None);
    }

    #[test]
    fn test_field_option_u64_round_trip() {
        let mut enc = FieldEncoder::new();
        enc.write_option_u64(&Some(999));
        enc.write_option_u64(&None);
        let bytes = enc.finish();
        let mut dec = FieldDecoder::new(&bytes);
        assert_eq!(dec.read_option_u64().unwrap(), Some(999));
        assert_eq!(dec.read_option_u64().unwrap(), None);
    }

    #[test]
    fn test_field_mixed_types() {
        let mut enc = FieldEncoder::new();
        enc.write_u64(1);
        enc.write_string("test");
        enc.write_bool(true);
        enc.write_i32(-42);
        enc.write_bytes(&[0xDE, 0xAD]);
        enc.write_option_string(&Some("opt".to_string()));

        let bytes = enc.finish();
        let mut dec = FieldDecoder::new(&bytes);

        assert_eq!(dec.read_u64().unwrap(), 1);
        assert_eq!(dec.read_string().unwrap(), "test");
        assert!(dec.read_bool().unwrap());
        assert_eq!(dec.read_i32().unwrap(), -42);
        assert_eq!(dec.read_bytes().unwrap(), vec![0xDE, 0xAD]);
        assert_eq!(dec.read_option_string().unwrap(), Some("opt".to_string()));
        assert!(dec.is_exhausted());
    }

    #[test]
    fn test_decoder_remaining() {
        let mut enc = FieldEncoder::new();
        enc.write_u32(1);
        let bytes = enc.finish();
        let mut dec = FieldDecoder::new(&bytes);
        assert_eq!(dec.remaining(), 4);
        assert_eq!(dec.position(), 0);
        dec.read_u32().unwrap();
        assert_eq!(dec.remaining(), 0);
        assert_eq!(dec.position(), 4);
    }

    #[test]
    fn test_decoder_read_past_end() {
        let dec_data = [0u8; 2];
        let mut dec = FieldDecoder::new(&dec_data);
        assert!(dec.read_u64().is_err());
    }

    #[test]
    fn test_encoder_len_and_empty() {
        let mut enc = FieldEncoder::new();
        assert!(enc.is_empty());
        assert_eq!(enc.len(), 0);
        enc.write_u8(1);
        assert!(!enc.is_empty());
        assert_eq!(enc.len(), 1);
    }

    #[test]
    fn test_encoder_with_capacity() {
        let enc = FieldEncoder::with_capacity(1024);
        assert!(enc.is_empty());
    }

    // ------------------------------------------------------------------
    // SimpleSerializer with an entity
    // ------------------------------------------------------------------

    #[derive(Debug, Clone, PartialEq)]
    struct TestItem {
        id: u64,
        name: String,
        active: bool,
    }

    impl Entity for TestItem {
        type PrimaryKey = u64;
        fn primary_key(&self) -> &u64 {
            &self.id
        }
        fn entity_name() -> &'static str {
            "TestItem"
        }
    }

    fn make_test_serializer() -> SimpleSerializer<TestItem> {
        SimpleSerializer::new(
            |item: &TestItem| {
                let mut enc = FieldEncoder::new();
                enc.write_u64(item.id);
                enc.write_string(&item.name);
                enc.write_bool(item.active);
                Ok(enc.finish())
            },
            |bytes| {
                let mut dec = FieldDecoder::new(bytes);
                Ok(TestItem {
                    id: dec.read_u64()?,
                    name: dec.read_string()?,
                    active: dec.read_bool()?,
                })
            },
        )
    }

    #[test]
    fn test_simple_serializer_round_trip() {
        let ser = make_test_serializer();
        let item = TestItem { id: 42, name: "hello".into(), active: true };
        let bytes = ser.serialize(&item).unwrap();
        let decoded = ser.deserialize(&bytes).unwrap();
        assert_eq!(item, decoded);
    }

    #[test]
    fn test_simple_serializer_empty_name() {
        let ser = make_test_serializer();
        let item = TestItem { id: 0, name: String::new(), active: false };
        let bytes = ser.serialize(&item).unwrap();
        let decoded = ser.deserialize(&bytes).unwrap();
        assert_eq!(item, decoded);
    }

    #[test]
    fn test_simple_serializer_via_trait() {
        let ser: Box<dyn EntitySerializer<TestItem>> =
            Box::new(make_test_serializer());
        let item =
            TestItem { id: 99, name: "trait object".into(), active: true };
        let bytes = ser.serialize(&item).unwrap();
        let decoded = ser.deserialize(&bytes).unwrap();
        assert_eq!(item, decoded);
    }
}
