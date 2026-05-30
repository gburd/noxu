//! Serde-based entry binding for Noxu DB.
//!
//! Replaces Java serialization with Rust's serde framework using a
//! compact binary format implemented in [`super::simple_serial`].
//!
//! ## Wire format (v1.5)
//!
//! Every payload produced by `SerdeBinding::object_to_entry` begins
//! with a 2-byte header followed by the [`super::simple_serial`] body:
//!
//! ```text
//! +--------+---------+----------------+
//! | 0xCB   |   0x01  |  simple_serial |
//! | magic  | version |    payload     |
//! +--------+---------+----------------+
//! ```
//!
//! On decode, `entry_to_object` validates both bytes and returns
//! [`crate::bind::BindError::VersionMismatch`] if either is wrong, rather
//! than silently producing a wrong-shaped value.  This is **not** full
//! schema evolution (it cannot tolerate added/removed/reordered struct
//! fields), but it stops silent corruption and gives an unambiguous,
//! typed error when on-disk data and the running binary disagree.
//!
//! ## Breaking change vs. earlier 1.5 release candidates
//!
//! Data written by `SerdeBinding` in pre-3C builds did **not** carry
//! the 2-byte header.  Records produced by older builds will fail to
//! decode under v1.5 with `BindError::VersionMismatch { found_magic:
//! <whatever the first byte happened to be>, ... }`.  See
//! `docs/src/getting-started/bindings.md` for the migration guidance.
//!
//! ## Required dependencies (to be added to Cargo.toml)
//!
//! ```toml
//! serde = { version = "1", features = ["derive"] }
//! ```

use std::marker::PhantomData;

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::db::DatabaseEntry;

use crate::bind::Result;
use crate::bind::entry_binding::EntryBinding;
use crate::bind::serial::simple_serial;

/// Magic byte identifying a `SerdeBinding`-encoded payload.  Picked to
/// be stable across releases; bumping it would be a breaking on-disk
/// change.
pub const SERDE_BINDING_MAGIC: u8 = 0xCB;

/// Wire-format version emitted by this build.  Bump in lock-step with
/// any incompatible change to the [`super::simple_serial`] format.
pub const SERDE_BINDING_VERSION: u8 = 0x01;

/// Length of the version header prefixed to every encoded entry.
pub const SERDE_BINDING_HEADER_LEN: usize = 2;

/// Binding that uses a compact binary format via serde for serialization.
///
/// Any type implementing `Serialize + DeserializeOwned` can be stored in and
/// retrieved from database entries using this binding.  Each entry is
/// prefixed with a 2-byte header; see the module docs for the format.
///
/// # Schema management caveat
///
/// `SerdeBinding` does **not** carry a per-record schema descriptor.
/// The 2-byte header guards against decoding records produced by an
/// incompatible *wire format* (`BindError::VersionMismatch`), but it
/// cannot detect changes in the *Rust struct* being serialised:
///
/// * Adding a struct field, removing a struct field, or reordering
///   fields **silently corrupts** records written by an earlier
///   build of the same binary — the deserializer walks the same
///   field list, in the same order, with no field tags to anchor it.
///
/// JE's `SerialBinding` solved the same problem with a
/// `StoredClassCatalog` keyed off a per-record class id; this crate
/// has no equivalent today.  Two concrete mitigations:
///
/// 1. Keep the on-disk struct stable and add fields only via
///    `Option<T>`-typed wrappers under a new top-level enum variant
///    that you can match on at the application layer.
/// 2. For schemas that need to evolve, use the DPL
///    (`noxu-persist`) which has explicit `@KeyField` /
///    `@SecondaryKey` annotation-driven evolution; see
///    `docs/src/collections/entity-persistence.md`.
///
/// # Examples
///
/// ```ignore
/// use serde::{Serialize, Deserialize};
/// use crate::bind::serial::serde_binding::SerdeBinding;
/// use crate::bind::entry_binding::EntryBinding;
/// use crate::db::DatabaseEntry;
///
/// #[derive(Serialize, Deserialize, Debug, PartialEq)]
/// struct Person {
///     name: String,
///     age: u32,
/// }
///
/// let binding = SerdeBinding::<Person>::new();
/// let person = Person { name: "Alice".into(), age: 30 };
///
/// let mut entry = DatabaseEntry::new();
/// binding.object_to_entry(&person, &mut entry).unwrap();
///
/// let decoded = binding.entry_to_object(&entry).unwrap();
/// assert_eq!(decoded, person);
/// ```
pub struct SerdeBinding<T> {
    _phantom: PhantomData<T>,
}

impl<T> SerdeBinding<T> {
    /// Creates a new serde-based binding.
    pub fn new() -> Self {
        Self { _phantom: PhantomData }
    }
}

impl<T> Default for SerdeBinding<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Clone for SerdeBinding<T> {
    fn clone(&self) -> Self {
        Self::new()
    }
}

impl<T> std::fmt::Debug for SerdeBinding<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SerdeBinding")
            .field("type", &std::any::type_name::<T>())
            .finish()
    }
}

impl<T: Serialize + DeserializeOwned> EntryBinding<T> for SerdeBinding<T> {
    fn entry_to_object(&self, entry: &DatabaseEntry) -> Result<T> {
        let data = entry.data();
        if data.len() < SERDE_BINDING_HEADER_LEN {
            return Err(crate::bind::BindError::VersionMismatch {
                expected_magic: SERDE_BINDING_MAGIC,
                expected_version: SERDE_BINDING_VERSION,
                found_magic: data.first().copied().unwrap_or(0),
                found_version: data.get(1).copied().unwrap_or(0),
            });
        }
        if data[0] != SERDE_BINDING_MAGIC || data[1] != SERDE_BINDING_VERSION {
            return Err(crate::bind::BindError::VersionMismatch {
                expected_magic: SERDE_BINDING_MAGIC,
                expected_version: SERDE_BINDING_VERSION,
                found_magic: data[0],
                found_version: data[1],
            });
        }
        simple_serial::from_bytes(&data[SERDE_BINDING_HEADER_LEN..])
    }

    fn object_to_entry(
        &self,
        object: &T,
        entry: &mut DatabaseEntry,
    ) -> Result<()> {
        let body = simple_serial::to_bytes(object)?;
        let mut bytes =
            Vec::with_capacity(body.len() + SERDE_BINDING_HEADER_LEN);
        bytes.push(SERDE_BINDING_MAGIC);
        bytes.push(SERDE_BINDING_VERSION);
        bytes.extend_from_slice(&body);
        entry.set_data_vec(bytes);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[test]
    fn test_u32_round_trip() {
        let binding = SerdeBinding::<u32>::new();
        let mut entry = DatabaseEntry::new();
        binding.object_to_entry(&42u32, &mut entry).unwrap();
        assert_eq!(binding.entry_to_object(&entry).unwrap(), 42u32);
    }

    #[test]
    fn test_string_round_trip() {
        let binding = SerdeBinding::<String>::new();
        let mut entry = DatabaseEntry::new();
        let s = "hello world".to_string();
        binding.object_to_entry(&s, &mut entry).unwrap();
        assert_eq!(binding.entry_to_object(&entry).unwrap(), s);
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct TestRecord {
        id: u64,
        name: String,
        active: bool,
    }

    #[test]
    fn test_struct_round_trip() {
        let binding = SerdeBinding::<TestRecord>::new();
        let record =
            TestRecord { id: 12345, name: "test".to_string(), active: true };
        let mut entry = DatabaseEntry::new();
        binding.object_to_entry(&record, &mut entry).unwrap();
        assert_eq!(binding.entry_to_object(&entry).unwrap(), record);
    }

    #[test]
    fn test_vec_round_trip() {
        let binding = SerdeBinding::<Vec<u32>>::new();
        let v = vec![1, 2, 3, 4, 5];
        let mut entry = DatabaseEntry::new();
        binding.object_to_entry(&v, &mut entry).unwrap();
        assert_eq!(binding.entry_to_object(&entry).unwrap(), v);
    }

    #[test]
    fn test_option_round_trip() {
        let binding = SerdeBinding::<Option<String>>::new();
        let mut entry = DatabaseEntry::new();

        binding.object_to_entry(&Some("yes".to_string()), &mut entry).unwrap();
        assert_eq!(
            binding.entry_to_object(&entry).unwrap(),
            Some("yes".to_string())
        );

        binding.object_to_entry(&None, &mut entry).unwrap();
        assert_eq!(binding.entry_to_object(&entry).unwrap(), None);
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    enum Status {
        Active,
        Inactive,
        Pending(String),
    }

    #[test]
    fn test_enum_round_trip() {
        let binding = SerdeBinding::<Status>::new();
        let mut entry = DatabaseEntry::new();

        binding.object_to_entry(&Status::Active, &mut entry).unwrap();
        assert_eq!(binding.entry_to_object(&entry).unwrap(), Status::Active);

        binding
            .object_to_entry(&Status::Pending("review".to_string()), &mut entry)
            .unwrap();
        assert_eq!(
            binding.entry_to_object(&entry).unwrap(),
            Status::Pending("review".to_string())
        );
    }

    #[test]
    fn test_default() {
        let binding = SerdeBinding::<u32>::default();
        let mut entry = DatabaseEntry::new();
        binding.object_to_entry(&7u32, &mut entry).unwrap();
        assert_eq!(binding.entry_to_object(&entry).unwrap(), 7u32);
    }

    #[test]
    fn test_clone() {
        let binding = SerdeBinding::<u32>::new();
        let cloned = binding.clone();
        let mut entry = DatabaseEntry::new();
        binding.object_to_entry(&99u32, &mut entry).unwrap();
        assert_eq!(cloned.entry_to_object(&entry).unwrap(), 99u32);
    }

    #[test]
    fn test_debug() {
        let binding = SerdeBinding::<u32>::new();
        let debug = format!("{:?}", binding);
        assert!(debug.contains("SerdeBinding"));
    }

    #[test]
    fn test_empty_entry_error() {
        let binding = SerdeBinding::<u32>::new();
        let entry = DatabaseEntry::new();
        // Empty entry has no data  -  should fail on deserialization
        assert!(binding.entry_to_object(&entry).is_err());
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Nested {
        inner: TestRecord,
        tags: Vec<String>,
    }

    #[test]
    fn test_nested_struct_round_trip() {
        let binding = SerdeBinding::<Nested>::new();
        let nested = Nested {
            inner: TestRecord {
                id: 1,
                name: "nested".to_string(),
                active: false,
            },
            tags: vec!["a".to_string(), "b".to_string()],
        };
        let mut entry = DatabaseEntry::new();
        binding.object_to_entry(&nested, &mut entry).unwrap();
        assert_eq!(binding.entry_to_object(&entry).unwrap(), nested);
    }

    #[test]
    fn test_tuple_round_trip() {
        let binding = SerdeBinding::<(u32, String, bool)>::new();
        let val = (42u32, "hello".to_string(), true);
        let mut entry = DatabaseEntry::new();
        binding.object_to_entry(&val, &mut entry).unwrap();
        assert_eq!(binding.entry_to_object(&entry).unwrap(), val);
    }

    #[test]
    fn test_entry_data_is_set() {
        let binding = SerdeBinding::<u32>::new();
        let mut entry = DatabaseEntry::new();
        assert!(entry.is_empty());
        binding.object_to_entry(&42u32, &mut entry).unwrap();
        assert!(!entry.is_empty());
        assert!(entry.get_data().is_some());
    }

    // ----- Sprint 3C version-prefix tests (audit finding #19) ----------------

    /// The wire format must begin with the documented 2-byte header.
    /// If this assertion fails the on-disk format has drifted and the
    /// version constant must be bumped.
    #[test]
    fn test_encoded_payload_starts_with_version_header() {
        let binding = SerdeBinding::<u32>::new();
        let mut entry = DatabaseEntry::new();
        binding.object_to_entry(&42u32, &mut entry).unwrap();

        let bytes = entry.get_data().unwrap();
        assert!(
            bytes.len() >= SERDE_BINDING_HEADER_LEN,
            "encoded entry must include the 2-byte header",
        );
        assert_eq!(bytes[0], SERDE_BINDING_MAGIC);
        assert_eq!(bytes[1], SERDE_BINDING_VERSION);
        // body is a 4-byte big-endian u32 (= 42).
        assert_eq!(&bytes[2..], &[0, 0, 0, 42]);
    }

    /// An entry written by a pre-3C build (no header) must surface as
    /// `BindError::VersionMismatch` rather than panicking, returning
    /// `InvalidData`, or producing a wrong-shaped value.
    #[test]
    fn test_decode_unprefixed_payload_returns_version_mismatch() {
        // Pre-3C bytes: a bare big-endian u32 with no header.
        let entry = DatabaseEntry::from_bytes(&[0, 0, 0, 42]);
        let binding = SerdeBinding::<u32>::new();

        let err = binding
            .entry_to_object(&entry)
            .expect_err("unprefixed payload must fail to decode");
        match err {
            crate::bind::BindError::VersionMismatch {
                expected_magic,
                expected_version,
                found_magic,
                found_version,
            } => {
                assert_eq!(expected_magic, SERDE_BINDING_MAGIC);
                assert_eq!(expected_version, SERDE_BINDING_VERSION);
                // The pre-3C u32 starts with 0x00 0x00 — neither matches
                // the magic, so the error reports those bytes verbatim.
                assert_eq!(found_magic, 0x00);
                assert_eq!(found_version, 0x00);
            }
            other => panic!("expected VersionMismatch, got {:?}", other),
        }
    }

    /// A short entry (less than 2 bytes total) must also surface as
    /// `VersionMismatch` rather than `BufferUnderflow`.
    #[test]
    fn test_decode_short_payload_returns_version_mismatch() {
        let binding = SerdeBinding::<u32>::new();

        for short in &[&[][..], &[SERDE_BINDING_MAGIC][..]] {
            let entry = DatabaseEntry::from_bytes(short);
            let err = binding
                .entry_to_object(&entry)
                .expect_err("short payload must fail to decode");
            assert!(
                matches!(err, crate::bind::BindError::VersionMismatch { .. }),
                "short payload (len={}) must fail with VersionMismatch, got {:?}",
                short.len(),
                err,
            );
        }
    }

    /// A header with the right magic but the wrong version must be
    /// rejected with `VersionMismatch`, even if the trailing body
    /// would otherwise round-trip cleanly.
    #[test]
    fn test_decode_wrong_version_returns_version_mismatch() {
        let mut bytes = vec![SERDE_BINDING_MAGIC, 0xFF];
        bytes.extend_from_slice(&42u32.to_be_bytes());
        let entry = DatabaseEntry::from_bytes(&bytes);
        let binding = SerdeBinding::<u32>::new();

        let err = binding
            .entry_to_object(&entry)
            .expect_err("wrong-version payload must fail to decode");
        match err {
            crate::bind::BindError::VersionMismatch {
                found_magic,
                found_version,
                ..
            } => {
                assert_eq!(found_magic, SERDE_BINDING_MAGIC);
                assert_eq!(found_version, 0xFF);
            }
            other => panic!("expected VersionMismatch, got {:?}", other),
        }
    }

    /// `VersionMismatch` formats with both expected and found bytes so
    /// users see exactly what is wrong.
    #[test]
    fn test_version_mismatch_display() {
        let err = crate::bind::BindError::VersionMismatch {
            expected_magic: 0xCB,
            expected_version: 0x01,
            found_magic: 0x00,
            found_version: 0x00,
        };
        let s = err.to_string();
        assert!(s.contains("0xCB"), "display must include expected magic: {s}");
        assert!(
            s.contains("0x01"),
            "display must include expected version: {s}"
        );
        assert!(
            s.contains("version mismatch"),
            "display must name the failure: {s}"
        );
    }
}
