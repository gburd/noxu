//! On-disk record envelope for entity persistence (Wave 2C-2).
//!
//! Every entity record stored by [`crate::primary_index::PrimaryIndex`] is
//! wrapped in a small envelope that records the class version the payload
//! was written under, plus the entity class name (tag).  The shape is:
//!
//! ```text
//! [2-byte class_version BE]
//! [1-byte entity_class_tag_len]
//! [entity_class_tag bytes]    (UTF-8, length = tag_len, max 255 bytes)
//! [payload bytes]             (whatever the user EntitySerializer emitted)
//! ```
//!
//! **Why a class tag?**  The tag lets us validate at decode time that we
//! are reading a record written by the same entity class — useful when an
//! [`crate::evolve::Renamer`] has been registered, since the on-disk tag
//! reveals the *old* name and we can transparently dispatch to the new
//! class.  It also catches "wrong serializer" misuse where the user has
//! pointed two different entity types at the same database name.
//!
//! **BREAKING (vs. pre-v1.6 entity stores):** before Wave 2C-2 entity
//! records stored only the user payload.  Pre-v1.6 entity stores
//! cannot be read by Wave 2C-2 builds and vice versa.  See the
//! migration guide in `docs/src/getting-started/migrating.md` for the
//! recommended dump-and-reload procedure.
//!
//! This envelope sits **above** the binding layer's `SerdeBinding`
//! 2-byte version header (Wave 2B): the binding-layer header lives in
//! the *payload* bytes that this envelope wraps.  The two layers are
//! independent and address different evolution concerns —
//! `SerdeBinding` evolves the binary encoding of a single binding,
//! while this envelope evolves entity classes (whole records).

use crate::error::{PersistError, Result};

/// Maximum length of the entity class tag in bytes.
///
/// Constrained to `u8::MAX = 255` because the tag length is encoded as a
/// single byte.  All practical entity class names fit comfortably (the
/// JE convention of fully-qualified package + class names rarely exceeds
/// 100 bytes).
pub const MAX_CLASS_TAG_LEN: usize = u8::MAX as usize;

/// Fixed-size header part: 2 bytes for class version + 1 byte for tag length.
const HEADER_FIXED_LEN: usize = 3;

/// Encodes a class version + class tag + payload into a single byte vector.
///
/// # Errors
///
/// * [`PersistError::SerializationError`] if `class_tag` is empty or longer
///   than [`MAX_CLASS_TAG_LEN`].
pub fn encode(
    class_version: u16,
    class_tag: &str,
    payload: &[u8],
) -> Result<Vec<u8>> {
    let tag_bytes = class_tag.as_bytes();
    if tag_bytes.is_empty() {
        return Err(PersistError::SerializationError(
            "entity class tag must not be empty".to_string(),
        ));
    }
    if tag_bytes.len() > MAX_CLASS_TAG_LEN {
        return Err(PersistError::SerializationError(format!(
            "entity class tag too long: {} bytes (max {})",
            tag_bytes.len(),
            MAX_CLASS_TAG_LEN,
        )));
    }

    let mut out =
        Vec::with_capacity(HEADER_FIXED_LEN + tag_bytes.len() + payload.len());
    out.extend_from_slice(&class_version.to_be_bytes());
    out.push(tag_bytes.len() as u8);
    out.extend_from_slice(tag_bytes);
    out.extend_from_slice(payload);
    Ok(out)
}

/// A decoded envelope returned by [`decode`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedRecord<'a> {
    /// The class version this record was written under.
    pub class_version: u16,
    /// The entity class tag (typically `Entity::entity_name()`).
    pub class_tag: &'a str,
    /// The user payload bytes (input to `EntitySerializer::deserialize`).
    pub payload: &'a [u8],
}

/// Decodes a record envelope.
///
/// Returns the class version, class tag, and payload as borrows into
/// `bytes` (no allocation).
///
/// # Errors
///
/// * [`PersistError::SerializationError`] if `bytes` is too short to
///   contain a valid envelope, or the embedded tag is not valid UTF-8.
pub fn decode(bytes: &[u8]) -> Result<DecodedRecord<'_>> {
    if bytes.len() < HEADER_FIXED_LEN {
        return Err(PersistError::SerializationError(format!(
            "record too short for entity envelope: {} bytes (need >= {})",
            bytes.len(),
            HEADER_FIXED_LEN,
        )));
    }
    let class_version = u16::from_be_bytes([bytes[0], bytes[1]]);
    let tag_len = bytes[2] as usize;
    let tag_start = HEADER_FIXED_LEN;
    let tag_end = tag_start + tag_len;
    if bytes.len() < tag_end {
        return Err(PersistError::SerializationError(format!(
            "record too short for tag of length {}: {} bytes",
            tag_len,
            bytes.len(),
        )));
    }
    let class_tag =
        std::str::from_utf8(&bytes[tag_start..tag_end]).map_err(|e| {
            PersistError::SerializationError(format!(
                "entity class tag is not valid UTF-8: {}",
                e
            ))
        })?;
    let payload = &bytes[tag_end..];
    Ok(DecodedRecord { class_version, class_tag, payload })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_simple() {
        let bytes = encode(0, "User", b"hello").unwrap();
        let dec = decode(&bytes).unwrap();
        assert_eq!(dec.class_version, 0);
        assert_eq!(dec.class_tag, "User");
        assert_eq!(dec.payload, b"hello");
    }

    #[test]
    fn round_trip_high_version() {
        let bytes = encode(0xABCD, "com.example.Foo", b"").unwrap();
        let dec = decode(&bytes).unwrap();
        assert_eq!(dec.class_version, 0xABCD);
        assert_eq!(dec.class_tag, "com.example.Foo");
        assert_eq!(dec.payload, b"");
    }

    #[test]
    fn round_trip_empty_payload() {
        let bytes = encode(7, "X", b"").unwrap();
        let dec = decode(&bytes).unwrap();
        assert_eq!(dec.class_version, 7);
        assert_eq!(dec.class_tag, "X");
        assert!(dec.payload.is_empty());
    }

    #[test]
    fn empty_tag_rejected() {
        let r = encode(0, "", b"x");
        assert!(r.is_err());
    }

    #[test]
    fn oversized_tag_rejected() {
        let big = "a".repeat(MAX_CLASS_TAG_LEN + 1);
        let r = encode(0, &big, b"x");
        assert!(r.is_err());
    }

    #[test]
    fn decode_truncated_header() {
        assert!(decode(&[]).is_err());
        assert!(decode(&[0]).is_err());
        assert!(decode(&[0, 0]).is_err());
    }

    #[test]
    fn decode_truncated_tag() {
        // class_version=0, tag_len=10, but only 2 tag bytes follow.
        let mut buf = vec![0u8, 0, 10];
        buf.extend_from_slice(b"AB");
        assert!(decode(&buf).is_err());
    }

    #[test]
    fn decode_invalid_utf8_tag() {
        // class_version=0, tag_len=2, tag = 0xFF 0xFE (invalid UTF-8)
        let buf = vec![0u8, 0, 2, 0xFF, 0xFE, 0x00];
        assert!(decode(&buf).is_err());
    }

    #[test]
    fn header_fixed_layout() {
        // Encode and check the byte layout exactly so we lock the on-disk shape.
        let bytes = encode(0x0102, "X", b"yz").unwrap();
        // [0x01, 0x02]  class_version BE
        // [0x01]        tag_len = 1
        // ['X']         tag
        // ['y', 'z']    payload
        assert_eq!(bytes, vec![0x01, 0x02, 0x01, b'X', b'y', b'z']);
    }

    #[test]
    fn payload_bytes_preserved_with_arbitrary_bytes() {
        let payload = (0u8..=255).collect::<Vec<u8>>();
        let bytes = encode(42, "Bin", &payload).unwrap();
        let dec = decode(&bytes).unwrap();
        assert_eq!(dec.payload, payload.as_slice());
    }

    #[test]
    fn unicode_tag_round_trip() {
        let bytes = encode(0, "naïve.Class", b"x").unwrap();
        let dec = decode(&bytes).unwrap();
        assert_eq!(dec.class_tag, "naïve.Class");
    }
}
