//! Entity and key traits for the persistence layer.
//!
//! Model`. In Rust, these are expressed as traits
//! that users implement for their types. Derive macros can be added later
//! in a separate proc-macro crate.

use crate::error::{PersistError, Result};

/// Trait for types that can be stored as entities in the persistence layer.
///
/// This is the Rust equivalent of `@Entity` annotation. Types implementing
/// this trait can be stored in and retrieved from an `EntityStore` via a
/// `PrimaryIndex`.
///
///
///
/// # Example
///
/// ```
/// use noxu_persist::{Entity, PrimaryKey};
///
/// struct User {
///     id: u64,
///     name: String,
///     email: String,
/// }
///
/// impl Entity for User {
///     type PrimaryKey = u64;
///
///     fn primary_key(&self) -> &u64 {
///         &self.id
///     }
///
///     fn entity_name() -> &'static str {
///         "User"
///     }
/// }
/// ```
pub trait Entity: Sized {
    /// The primary key type for this entity.
    type PrimaryKey: PrimaryKey;

    /// Returns a reference to the primary key of this entity.
    fn primary_key(&self) -> &Self::PrimaryKey;

    /// Returns the entity class name, used for database naming within an
    /// `EntityStore`. Each entity type should return a unique, stable name.
    fn entity_name() -> &'static str;

    /// Returns the current schema version of this entity class.
    ///
    /// This is the version that newly written records will be tagged with on
    /// disk.  The default is `0` so existing entity definitions need no
    /// changes.  Bump this whenever you change the on-disk shape of the
    /// entity (e.g. add / remove / rename fields, or change the way an
    /// existing field is serialized) and supply matching
    /// [`crate::evolve::Mutations`] via
    /// [`crate::store_config::StoreConfig::with_mutations`] so that older
    /// records can be read or rewritten on store open.
    ///
    /// Per-record class versions are persisted in a 2-byte BE
    /// prefix on every entity record (see
    /// [`crate::evolve::envelope`]).
    fn class_version() -> u16 {
        0
    }
}

/// Trait for types that can serve as primary keys.
///
/// This is the Rust equivalent of `@PrimaryKey` annotation. Primary key
/// types must be serializable to and from bytes, and must support equality
/// comparison and hashing for use in indexes.
///
///
pub trait PrimaryKey: Clone + Eq + std::hash::Hash {
    /// Encodes this key to a byte vector.
    fn to_bytes(&self) -> Vec<u8>;

    /// Decodes a key from a byte slice.
    ///
    /// # Errors
    /// Returns `PersistError::SerializationError` if the bytes cannot be decoded.
    fn from_bytes(bytes: &[u8]) -> Result<Self>;

    /// Encodes this key as **order-preserving, self-delimiting** bytes for use
    /// as one field of a composite (multi-field) key.
    ///
    /// Composite keys concatenate each field's `to_sortable_bytes()` with no
    /// length prefix, so byte-lexicographic order of the concatenation equals
    /// the logical tuple order `(field0, field1, ...)`. This is the same
    /// approach as JE's tuple format (`com.sleepycat.bind.tuple.TupleOutput`),
    /// where each field is written with an order-preserving, self-delimiting
    /// encoding so plain concatenation sorts correctly.
    ///
    /// The default impl returns `to_bytes()`, which is correct for the
    /// fixed-width, order-preserving numeric encodings (big-endian unsigned,
    /// sign-flipped big-endian signed). Variable-length types (`String`,
    /// `Vec<u8>`) override this with a terminated, escaped encoding.
    fn to_sortable_bytes(&self) -> Vec<u8> {
        self.to_bytes()
    }

    /// Decodes a key written by [`to_sortable_bytes`](Self::to_sortable_bytes)
    /// from the front of `bytes`, returning the decoded value and the number
    /// of bytes consumed (so the next field starts at `&bytes[consumed..]`).
    ///
    /// The default impl consumes the whole slice via `from_bytes` and reports
    /// `bytes.len()` consumed. Fixed-width types whose `from_bytes` requires
    /// an exact length MUST override this to consume only their own width;
    /// see the numeric impls below.
    ///
    /// # Errors
    /// Returns `PersistError::SerializationError` if the bytes cannot be decoded.
    fn from_sortable_bytes(bytes: &[u8]) -> Result<(Self, usize)> {
        Ok((Self::from_bytes(bytes)?, bytes.len()))
    }
}

/// Order-preserving, self-delimiting encoding for a variable-length byte
/// string used as a composite-key field.
///
/// Mirrors JE's `TupleOutput.writeString`, which writes the (modified-UTF-8)
/// bytes followed by a `0x00` terminator (`TupleOutput.java:126`,
/// `writeFast(0)`). Modified-UTF-8 never emits `0x00`, so the terminator is
/// unambiguous there. Rust `String`/`Vec<u8>` can contain `0x00`, so we
/// additionally escape any `0x00` data byte as `0x00 0x01` and terminate with
/// `0x00 0x00`. Because `0x01 > 0x00`, a shorter string that is a prefix of a
/// longer one terminates earlier and therefore sorts first, and no embedded
/// byte can forge the terminator — the encoding is order-preserving.
fn encode_sortable_byte_string(data: &[u8], out: &mut Vec<u8>) {
    for &b in data {
        if b == 0x00 {
            out.push(0x00);
            out.push(0x01);
        } else {
            out.push(b);
        }
    }
    out.push(0x00);
    out.push(0x00);
}

/// Inverse of [`encode_sortable_byte_string`]: decodes one terminated,
/// escaped byte string from the front of `bytes`, returning the raw bytes and
/// the number of input bytes consumed (including the terminator).
fn decode_sortable_byte_string(bytes: &[u8]) -> Result<(Vec<u8>, usize)> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b == 0x00 {
            match bytes.get(i + 1) {
                Some(0x00) => return Ok((out, i + 2)), // terminator
                Some(0x01) => {
                    out.push(0x00);
                    i += 2;
                }
                _ => {
                    return Err(PersistError::SerializationError(
                        "invalid escape sequence decoding sortable byte string"
                            .into(),
                    ));
                }
            }
        } else {
            out.push(b);
            i += 1;
        }
    }
    Err(PersistError::SerializationError(
        "unterminated sortable byte string".into(),
    ))
}

// --- PrimaryKey implementations for common types ---

impl PrimaryKey for u64 {
    fn to_bytes(&self) -> Vec<u8> {
        self.to_be_bytes().to_vec()
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != 8 {
            return Err(PersistError::SerializationError(format!(
                "expected 8 bytes for u64, got {}",
                bytes.len()
            )));
        }
        let mut buf = [0u8; 8];
        buf.copy_from_slice(bytes);
        Ok(u64::from_be_bytes(buf))
    }

    fn from_sortable_bytes(bytes: &[u8]) -> Result<(Self, usize)> {
        if bytes.len() < 8 {
            return Err(PersistError::SerializationError(format!(
                "expected 8 bytes for u64, got {}",
                bytes.len()
            )));
        }
        Ok((Self::from_bytes(&bytes[..8])?, 8))
    }
}

impl PrimaryKey for i64 {
    /// Encodes as big-endian with sign bit flipped so that negative values
    /// sort before positive values in byte-lexicographic order.
    /// i64::MIN -> 0x00 00 00 00 00 00 00 00 (smallest)
    /// -1       -> 0x7f ff ff ff ff ff ff ff
    /// 0        -> 0x80 00 00 00 00 00 00 00
    /// i64::MAX -> 0xff ff ff ff ff ff ff ff (largest)
    fn to_bytes(&self) -> Vec<u8> {
        let sortable = (*self as u64) ^ 0x8000_0000_0000_0000u64;
        sortable.to_be_bytes().to_vec()
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != 8 {
            return Err(PersistError::SerializationError(format!(
                "expected 8 bytes for i64, got {}",
                bytes.len()
            )));
        }
        let mut buf = [0u8; 8];
        buf.copy_from_slice(bytes);
        let sortable = u64::from_be_bytes(buf);
        Ok((sortable ^ 0x8000_0000_0000_0000u64) as i64)
    }

    fn from_sortable_bytes(bytes: &[u8]) -> Result<(Self, usize)> {
        if bytes.len() < 8 {
            return Err(PersistError::SerializationError(format!(
                "expected 8 bytes for i64, got {}",
                bytes.len()
            )));
        }
        Ok((Self::from_bytes(&bytes[..8])?, 8))
    }
}

impl PrimaryKey for u32 {
    fn to_bytes(&self) -> Vec<u8> {
        self.to_be_bytes().to_vec()
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != 4 {
            return Err(PersistError::SerializationError(format!(
                "expected 4 bytes for u32, got {}",
                bytes.len()
            )));
        }
        let mut buf = [0u8; 4];
        buf.copy_from_slice(bytes);
        Ok(u32::from_be_bytes(buf))
    }

    fn from_sortable_bytes(bytes: &[u8]) -> Result<(Self, usize)> {
        if bytes.len() < 4 {
            return Err(PersistError::SerializationError(format!(
                "expected 4 bytes for u32, got {}",
                bytes.len()
            )));
        }
        Ok((Self::from_bytes(&bytes[..4])?, 4))
    }
}

impl PrimaryKey for i32 {
    /// Encodes as big-endian with sign bit flipped so that negative values
    /// sort before positive values in byte-lexicographic order.
    /// i32::MIN -> 0x00 00 00 00 (smallest)
    /// -1       -> 0x7f ff ff ff
    /// 0        -> 0x80 00 00 00
    /// i32::MAX -> 0xff ff ff ff (largest)
    fn to_bytes(&self) -> Vec<u8> {
        let sortable = (*self as u32) ^ 0x8000_0000u32;
        sortable.to_be_bytes().to_vec()
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != 4 {
            return Err(PersistError::SerializationError(format!(
                "expected 4 bytes for i32, got {}",
                bytes.len()
            )));
        }
        let mut buf = [0u8; 4];
        buf.copy_from_slice(bytes);
        let sortable = u32::from_be_bytes(buf);
        Ok((sortable ^ 0x8000_0000u32) as i32)
    }

    fn from_sortable_bytes(bytes: &[u8]) -> Result<(Self, usize)> {
        if bytes.len() < 4 {
            return Err(PersistError::SerializationError(format!(
                "expected 4 bytes for i32, got {}",
                bytes.len()
            )));
        }
        Ok((Self::from_bytes(&bytes[..4])?, 4))
    }
}

impl PrimaryKey for String {
    fn to_bytes(&self) -> Vec<u8> {
        self.as_bytes().to_vec()
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        String::from_utf8(bytes.to_vec()).map_err(|e| {
            PersistError::SerializationError(format!(
                "invalid UTF-8 for String key: {}",
                e
            ))
        })
    }

    /// Order-preserving, self-delimiting encoding for composite keys.
    /// Mirrors JE's `TupleOutput.writeString` (UTF-8 bytes + `0x00`
    /// terminator, `TupleOutput.java:126`); we escape embedded `0x00` so the
    /// scheme is total over arbitrary `String` contents.
    fn to_sortable_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.len() + 2);
        encode_sortable_byte_string(self.as_bytes(), &mut out);
        out
    }

    fn from_sortable_bytes(bytes: &[u8]) -> Result<(Self, usize)> {
        let (raw, consumed) = decode_sortable_byte_string(bytes)?;
        let s = String::from_utf8(raw).map_err(|e| {
            PersistError::SerializationError(format!(
                "invalid UTF-8 for String key: {}",
                e
            ))
        })?;
        Ok((s, consumed))
    }
}

impl PrimaryKey for Vec<u8> {
    fn to_bytes(&self) -> Vec<u8> {
        self.clone()
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        Ok(bytes.to_vec())
    }

    /// Order-preserving, self-delimiting encoding for composite keys
    /// (same scheme as `String`; see [`encode_sortable_byte_string`]).
    fn to_sortable_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.len() + 2);
        encode_sortable_byte_string(self, &mut out);
        out
    }

    fn from_sortable_bytes(bytes: &[u8]) -> Result<(Self, usize)> {
        decode_sortable_byte_string(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_u64_round_trip() {
        let val: u64 = 42;
        let bytes = val.to_bytes();
        assert_eq!(bytes.len(), 8);
        let decoded = u64::from_bytes(&bytes).unwrap();
        assert_eq!(val, decoded);
    }

    #[test]
    fn test_u64_zero() {
        let val: u64 = 0;
        let bytes = val.to_bytes();
        let decoded = u64::from_bytes(&bytes).unwrap();
        assert_eq!(val, decoded);
    }

    #[test]
    fn test_u64_max() {
        let val: u64 = u64::MAX;
        let bytes = val.to_bytes();
        let decoded = u64::from_bytes(&bytes).unwrap();
        assert_eq!(val, decoded);
    }

    #[test]
    fn test_u64_wrong_length() {
        let result = u64::from_bytes(&[1, 2, 3]);
        assert!(result.is_err());
    }

    #[test]
    fn test_i64_round_trip() {
        let val: i64 = -42;
        let bytes = val.to_bytes();
        let decoded = i64::from_bytes(&bytes).unwrap();
        assert_eq!(val, decoded);
    }

    #[test]
    fn test_i64_negative() {
        let val: i64 = i64::MIN;
        let bytes = val.to_bytes();
        let decoded = i64::from_bytes(&bytes).unwrap();
        assert_eq!(val, decoded);
    }

    #[test]
    fn test_i64_wrong_length() {
        let result = i64::from_bytes(&[1]);
        assert!(result.is_err());
    }

    #[test]
    fn test_u32_round_trip() {
        let val: u32 = 12345;
        let bytes = val.to_bytes();
        assert_eq!(bytes.len(), 4);
        let decoded = u32::from_bytes(&bytes).unwrap();
        assert_eq!(val, decoded);
    }

    #[test]
    fn test_u32_wrong_length() {
        let result = u32::from_bytes(&[1, 2]);
        assert!(result.is_err());
    }

    #[test]
    fn test_i32_round_trip() {
        let val: i32 = -999;
        let bytes = val.to_bytes();
        let decoded = i32::from_bytes(&bytes).unwrap();
        assert_eq!(val, decoded);
    }

    #[test]
    fn test_i32_wrong_length() {
        let result = i32::from_bytes(&[]);
        assert!(result.is_err());
    }

    #[test]
    fn test_string_round_trip() {
        let val = String::from("hello world");
        let bytes = val.to_bytes();
        let decoded = String::from_bytes(&bytes).unwrap();
        assert_eq!(val, decoded);
    }

    #[test]
    fn test_string_empty() {
        let val = String::from("");
        let bytes = val.to_bytes();
        let decoded = String::from_bytes(&bytes).unwrap();
        assert_eq!(val, decoded);
    }

    #[test]
    fn test_string_unicode() {
        let val = String::from("hello \u{1F600} world");
        let bytes = val.to_bytes();
        let decoded = String::from_bytes(&bytes).unwrap();
        assert_eq!(val, decoded);
    }

    #[test]
    fn test_string_invalid_utf8() {
        let result = String::from_bytes(&[0xFF, 0xFE]);
        assert!(result.is_err());
    }

    #[test]
    fn test_vec_u8_round_trip() {
        let val: Vec<u8> = vec![1, 2, 3, 4, 5];
        let bytes = val.to_bytes();
        let decoded = Vec::<u8>::from_bytes(&bytes).unwrap();
        assert_eq!(val, decoded);
    }

    #[test]
    fn test_vec_u8_empty() {
        let val: Vec<u8> = vec![];
        let bytes = val.to_bytes();
        let decoded = Vec::<u8>::from_bytes(&bytes).unwrap();
        assert_eq!(val, decoded);
    }

    // Test that Entity trait works with a concrete type
    #[derive(Clone, Debug, PartialEq)]
    struct TestEntity {
        id: u64,
        name: String,
    }

    impl Entity for TestEntity {
        type PrimaryKey = u64;

        fn primary_key(&self) -> &u64 {
            &self.id
        }

        fn entity_name() -> &'static str {
            "TestEntity"
        }
    }

    #[test]
    fn test_entity_primary_key() {
        let entity = TestEntity { id: 42, name: "test".to_string() };
        assert_eq!(*entity.primary_key(), 42);
    }

    #[test]
    fn test_entity_name() {
        assert_eq!(TestEntity::entity_name(), "TestEntity");
    }

    // Test u64 key ordering (big-endian preserves sort order)
    #[test]
    fn test_u64_byte_ordering() {
        let a: u64 = 1;
        let b: u64 = 256;
        let bytes_a = a.to_bytes();
        let bytes_b = b.to_bytes();
        assert!(bytes_a < bytes_b);
    }

    #[test]
    fn test_u32_byte_ordering() {
        let a: u32 = 100;
        let b: u32 = 200;
        let bytes_a = a.to_bytes();
        let bytes_b = b.to_bytes();
        assert!(bytes_a < bytes_b);
    }

    // --- i32 signed sort order tests ---

    #[test]
    fn test_i32_min_sorts_before_max() {
        let bytes_min = i32::MIN.to_bytes();
        let bytes_max = i32::MAX.to_bytes();
        assert!(bytes_min < bytes_max, "i32::MIN should sort before i32::MAX");
    }

    #[test]
    fn test_i32_negative_one_sorts_before_zero() {
        let bytes_neg = (-1i32).to_bytes();
        let bytes_zero = 0i32.to_bytes();
        assert!(bytes_neg < bytes_zero, "-1 should sort before 0");
    }

    #[test]
    fn test_i32_sort_order_sequence() {
        let values: Vec<i32> = vec![i32::MIN, -1000, -1, 0, 1, 1000, i32::MAX];
        let encoded: Vec<Vec<u8>> =
            values.iter().map(|v| v.to_bytes()).collect();
        for i in 0..encoded.len() - 1 {
            assert!(
                encoded[i] < encoded[i + 1],
                "i32 sort order: {} (encoded {:?}) should be < {} (encoded {:?})",
                values[i],
                encoded[i],
                values[i + 1],
                encoded[i + 1]
            );
        }
    }

    #[test]
    fn test_i32_round_trip_with_sort_encoding() {
        for val in [i32::MIN, -1, 0, 1, i32::MAX] {
            let bytes = val.to_bytes();
            let decoded = i32::from_bytes(&bytes).unwrap();
            assert_eq!(val, decoded, "i32 round-trip failed for {}", val);
        }
    }

    // --- i64 signed sort order tests ---

    #[test]
    fn test_i64_min_sorts_before_max() {
        let bytes_min = i64::MIN.to_bytes();
        let bytes_max = i64::MAX.to_bytes();
        assert!(bytes_min < bytes_max, "i64::MIN should sort before i64::MAX");
    }

    #[test]
    fn test_i64_negative_one_sorts_before_zero() {
        let bytes_neg = (-1i64).to_bytes();
        let bytes_zero = 0i64.to_bytes();
        assert!(bytes_neg < bytes_zero, "-1i64 should sort before 0");
    }

    #[test]
    fn test_i64_sort_order_sequence() {
        let values: Vec<i64> = vec![i64::MIN, -1000, -1, 0, 1, 1000, i64::MAX];
        let encoded: Vec<Vec<u8>> =
            values.iter().map(|v| v.to_bytes()).collect();
        for i in 0..encoded.len() - 1 {
            assert!(
                encoded[i] < encoded[i + 1],
                "i64 sort order: {} should be < {}",
                values[i],
                values[i + 1]
            );
        }
    }

    #[test]
    fn test_i64_round_trip_with_sort_encoding() {
        for val in [i64::MIN, -1, 0, 1, i64::MAX] {
            let bytes = val.to_bytes();
            let decoded = i64::from_bytes(&bytes).unwrap();
            assert_eq!(val, decoded, "i64 round-trip failed for {}", val);
        }
    }

    // Verify the exact byte encoding values for i32
    #[test]
    fn test_i32_encoding_known_values() {
        // i32::MIN -> 0x00000000
        assert_eq!(i32::MIN.to_bytes(), vec![0x00, 0x00, 0x00, 0x00]);
        // -1 -> 0x7fffffff
        assert_eq!((-1i32).to_bytes(), vec![0x7f, 0xff, 0xff, 0xff]);
        // 0 -> 0x80000000
        assert_eq!(0i32.to_bytes(), vec![0x80, 0x00, 0x00, 0x00]);
        // i32::MAX -> 0xffffffff
        assert_eq!(i32::MAX.to_bytes(), vec![0xff, 0xff, 0xff, 0xff]);
    }

    // Verify the exact byte encoding values for i64
    #[test]
    fn test_i64_encoding_known_values() {
        // i64::MIN -> 0x0000000000000000
        assert_eq!(
            i64::MIN.to_bytes(),
            vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]
        );
        // 0 -> 0x8000000000000000
        assert_eq!(
            0i64.to_bytes(),
            vec![0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]
        );
        // i64::MAX -> 0xffffffffffffffff
        assert_eq!(
            i64::MAX.to_bytes(),
            vec![0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]
        );
    }

    // --- to_sortable_bytes / from_sortable_bytes (PERSIST-COMP-1) ---

    /// Round-trip a single field via the sortable encoding, then assert the
    /// reported consumed length equals the encoded length when the slice has
    /// no trailing bytes.
    fn rt_one<T: PrimaryKey + std::fmt::Debug>(v: T) {
        let enc = v.to_sortable_bytes();
        let (dec, consumed) = T::from_sortable_bytes(&enc).unwrap();
        assert_eq!(v, dec, "round-trip value mismatch");
        assert_eq!(consumed, enc.len(), "consumed != encoded length");
    }

    #[test]
    fn sortable_round_trip_each_type() {
        rt_one(0u64);
        rt_one(u64::MAX);
        rt_one(-1i64);
        rt_one(i64::MIN);
        rt_one(0u32);
        rt_one(u32::MAX);
        rt_one(-7i32);
        rt_one(i32::MIN);
        rt_one(String::from("hello"));
        rt_one(String::new());
        rt_one(vec![1u8, 2, 3]);
        rt_one(Vec::<u8>::new());
    }

    /// Embedded 0x00 bytes must round-trip via the escape (0x00 -> 0x00 0x01)
    /// and the 0x00 0x00 terminator.
    #[test]
    fn sortable_round_trip_embedded_nulls() {
        let v = vec![0u8, 1, 0, 0, 2, 255];
        rt_one(v);
        // A String cannot hold a NUL in the middle as easily, but it can:
        rt_one(String::from("a\0b\0"));
    }

    /// Two fields concatenated must decode sequentially, each reporting its
    /// own consumed length so the next field starts at the right offset.
    #[test]
    fn sortable_sequential_decode_two_fields() {
        let s = String::from("region");
        let n = 42u32;
        let mut buf = s.to_sortable_bytes();
        buf.extend_from_slice(&n.to_sortable_bytes());
        let (s2, c0) = String::from_sortable_bytes(&buf).unwrap();
        let (n2, c1) = u32::from_sortable_bytes(&buf[c0..]).unwrap();
        assert_eq!(s, s2);
        assert_eq!(n, n2);
        assert_eq!(c0 + c1, buf.len());
    }

    /// The sortable encoding is order-preserving for variable-length strings:
    /// a shorter prefix sorts before a longer string sharing that prefix, and
    /// length differences never invert logical order (the PERSIST-COMP-1 bug).
    #[test]
    fn sortable_string_order_preserving() {
        let mut inputs = vec!["", "a", "aa", "aaa", "ab", "b", "ba"];
        let logical = inputs.clone();
        let mut encoded: Vec<(Vec<u8>, &str)> = inputs
            .drain(..)
            .map(|s| (String::from(s).to_sortable_bytes(), s))
            .collect();
        encoded.sort();
        let by_bytes: Vec<&str> = encoded.iter().map(|(_, s)| *s).collect();
        assert_eq!(by_bytes, logical, "byte order must equal logical order");
    }

    /// A field with embedded 0x00 must still sort correctly relative to one
    /// without — the escape (0x00 0x01) keeps a real 0x00 below any 0x01+ byte
    /// while staying above the 0x00 0x00 terminator.
    #[test]
    fn sortable_bytes_order_with_null() {
        let a = vec![0u8].to_sortable_bytes(); // [0x00] data
        let empty = Vec::<u8>::new().to_sortable_bytes();
        let b = vec![1u8].to_sortable_bytes();
        // "" < [0x00] < [0x01]
        assert!(empty < a);
        assert!(a < b);
    }

    #[test]
    fn sortable_unterminated_string_errors() {
        // No 0x00 0x00 terminator anywhere -> error, not panic.
        let r = String::from_sortable_bytes(b"abc");
        assert!(matches!(r, Err(PersistError::SerializationError(_))));
    }
}
