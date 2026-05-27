//! JE TCK port: SerialBinding tests.
//!
//! Ports invariants from JE
//! `com.sleepycat.bind.serial.test.SerialBindingTest` onto noxu's
//! `SerdeBinding` / `TupleSerdeBinding`.
//!
//! Mapping JE -> noxu:
//!
//! | JE                              | Noxu                              |
//! |---------------------------------|-----------------------------------|
//! | `ClassCatalog`                  | implicit (type is parameter `T`) |
//! | `SerialBinding<T>`              | `SerdeBinding<T>`                 |
//! | `SerialSerialBinding`           | (n/a; serde-only) `EntityBinding` |
//! | `TupleSerialMarshalledBinding`  | `TupleSerdeBinding<K, V>`         |
//!
//! Noxu does not carry an external class catalog because it does not
//! need Java's per-class serialization metadata: the type is a generic
//! parameter on the binding.  The 2-byte version header (added in
//! Sprint 3C, see `SerdeBinding`'s module docs) is what guards
//! against decoding a payload written with a different wire format.
//! See `tck_serde_version_header` below.

use noxu_bind::{EntityBinding, EntryBinding, SerdeBinding, TupleSerdeBinding};
use noxu_db::DatabaseEntry;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Primitive bindings round-trip — port of SerialBindingTest.testPrimitiveBindings
// ---------------------------------------------------------------------------

fn primitive_round_trip<T>(val: T)
where
    T: Serialize + serde::de::DeserializeOwned + PartialEq + std::fmt::Debug,
{
    let binding = SerdeBinding::<T>::new();
    let mut buf = DatabaseEntry::new();
    binding.object_to_entry(&val, &mut buf).unwrap();
    assert!(
        !buf.data().is_empty(),
        "encoded entry must contain the version header at minimum"
    );
    let val2 = binding.entry_to_object(&buf).unwrap();
    assert_eq!(val, val2);
}

#[test]
fn tck_serial_primitive_bindings() {
    // String
    primitive_round_trip("abc".to_string());
    // Char (mapped to u32 via serde::serialize_char)
    primitive_round_trip('a');
    // Boolean
    primitive_round_trip(true);
    primitive_round_trip(false);
    // Integer types: i8, i16, i32, i64
    primitive_round_trip(123_i8);
    primitive_round_trip(123_i16);
    primitive_round_trip(123_i32);
    primitive_round_trip(123_i64);
    // Floating point
    primitive_round_trip(123.123_f32);
    primitive_round_trip(123.123_f64);
}

// ---------------------------------------------------------------------------
// "Null object" handling — port of SerialBindingTest.testNullObjects
// ---------------------------------------------------------------------------
//
// In Java, `SerialBinding(null-class)` permits `objectToEntry(null, buffer)`
// and the encoded entry has nonzero size.  In Rust the analogue of a
// null reference is `Option<T>::None`; encoding it must yield a
// non-empty entry (header + tag for None) and round-trip back to None.

#[test]
fn tck_serial_null_objects() {
    let binding = SerdeBinding::<Option<String>>::new();
    let mut buf = DatabaseEntry::new();
    binding.object_to_entry(&None, &mut buf).unwrap();
    assert!(
        !buf.data().is_empty(),
        "encoded None must include the version header (and the None tag)"
    );
    let result = binding.entry_to_object(&buf).unwrap();
    assert_eq!(None, result);
}

// ---------------------------------------------------------------------------
// SerialSerialBinding analogue — port of testSerialSerialBinding
// ---------------------------------------------------------------------------
//
// JE's SerialSerialBinding pairs a key SerialBinding with a value
// SerialBinding.  In noxu, both halves of an entity-binding pair use
// the same SerdeBinding<T> mechanism; this test combines two
// SerdeBindings to encode a key / value pair via `EntryBinding`.

#[test]
fn tck_serial_serial_binding_pair_round_trip() {
    let key_binding = SerdeBinding::<String>::new();
    let value_binding = SerdeBinding::<String>::new();

    let key = "key#value?indexKey".to_string();
    let value = "the-value".to_string();

    let mut key_buf = DatabaseEntry::new();
    let mut val_buf = DatabaseEntry::new();
    key_binding.object_to_entry(&key, &mut key_buf).unwrap();
    value_binding.object_to_entry(&value, &mut val_buf).unwrap();
    assert!(!key_buf.data().is_empty());
    assert!(!val_buf.data().is_empty());

    assert_eq!(key, key_binding.entry_to_object(&key_buf).unwrap());
    assert_eq!(value, value_binding.entry_to_object(&val_buf).unwrap());
}

// ---------------------------------------------------------------------------
// TupleSerial(Marshalled)Binding — port of testTupleSerialMarshalledBinding
// ---------------------------------------------------------------------------
//
// JE's TupleSerialMarshalledBinding extracts a tuple-encoded key from
// an entity whose data half is serial-encoded.  Noxu's
// `TupleSerdeBinding<K, V>` does the same: tuple key + serde data.
// Round-trip through `EntityBinding` (`object_to_key`,
// `object_to_data`, `entry_to_object`) must reconstitute the entity.

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Person {
    /// Tuple-encoded key, derived from `name` for sorting.
    name: String,
    age: u32,
}

#[test]
fn tck_tuple_serial_marshalled_binding_round_trip() {
    let binding = TupleSerdeBinding::<String, Person>::new(
        |p: &Person| p.name.clone(),
        |_k, v| v,
    );

    let original = Person { name: "Alice".to_string(), age: 30 };
    let mut key_buf = DatabaseEntry::new();
    let mut data_buf = DatabaseEntry::new();
    binding.object_to_key(&original, &mut key_buf).unwrap();
    binding.object_to_data(&original, &mut data_buf).unwrap();
    assert!(!key_buf.data().is_empty());
    assert!(!data_buf.data().is_empty());

    let decoded = binding.entry_to_object(&key_buf, &data_buf).unwrap();
    assert_eq!(original, decoded);
}

// ---------------------------------------------------------------------------
// Buffer size / overhead — port of testBufferSize / testBufferOverride
// ---------------------------------------------------------------------------
//
// JE asserts that the *initial* buffer size used by SerialBinding is a
// configurable parameter (default 100, override via `setSerialBufferSize`).
// Noxu does not expose buffer-size tuning at the binding level (the
// encoder owns its own Vec growth).  What *is* a stable invariant
// across both implementations is that each encoded entry includes a
// fixed-size header that takes constant overhead independent of the
// payload, and that two encodings of the same payload produce
// byte-identical output.

#[test]
fn tck_serial_buffer_overhead_is_constant() {
    let binding = SerdeBinding::<u32>::new();

    // The 2-byte version header dominates small payloads; encoding an
    // empty struct alongside `u32` lets us confirm the header is fixed
    // and identical regardless of payload value.
    let mut buf_small = DatabaseEntry::new();
    let mut buf_big = DatabaseEntry::new();
    binding.object_to_entry(&0u32, &mut buf_small).unwrap();
    binding.object_to_entry(&u32::MAX, &mut buf_big).unwrap();

    // Both payloads use the same 2-byte header.
    assert_eq!(buf_small.data()[..2], buf_big.data()[..2]);
    // Magic byte / version stable across builds.
    assert_eq!(buf_small.data()[0], 0xCB); // SERDE_BINDING_MAGIC
    assert_eq!(buf_small.data()[1], 0x01); // SERDE_BINDING_VERSION
}

#[test]
fn tck_serial_encoding_is_deterministic() {
    // JE's testBufferSize implicitly relies on encode being deterministic
    // (otherwise the size invariants couldn't hold).  Make this an
    // explicit invariant for noxu: encoding the same value twice via
    // independent `SerdeBinding` instances yields byte-identical output.
    let value = ("hello".to_string(), 42u64, true);

    let b1 = SerdeBinding::<(String, u64, bool)>::new();
    let b2 = SerdeBinding::<(String, u64, bool)>::new();
    let mut e1 = DatabaseEntry::new();
    let mut e2 = DatabaseEntry::new();
    b1.object_to_entry(&value, &mut e1).unwrap();
    b2.object_to_entry(&value, &mut e2).unwrap();
    assert_eq!(e1.data(), e2.data());
}

// ---------------------------------------------------------------------------
// Version-header guard — equivalent of "testClassloaderOverride" guarantees
// ---------------------------------------------------------------------------
//
// JE's classloader override prevents accidentally deserialising a
// payload using a class loaded from the wrong place; the noxu
// equivalent is the magic+version header that fails fast when an
// older or foreign payload is fed to a binding.

#[test]
fn tck_serde_version_header_rejects_missing_header() {
    // A payload that is too short to even contain the header must
    // fail with a typed error rather than producing a garbage value.
    let mut entry = DatabaseEntry::new();
    entry.set_data_vec(vec![]); // empty
    let binding = SerdeBinding::<u32>::new();
    let err = binding.entry_to_object(&entry).unwrap_err();
    assert!(
        matches!(err, noxu_bind::BindError::VersionMismatch { .. }),
        "expected VersionMismatch on empty payload, got {err:?}",
    );
}

#[test]
fn tck_serde_version_header_rejects_wrong_magic() {
    let mut entry = DatabaseEntry::new();
    // Bytes that look like an old, header-less payload would have.
    entry.set_data_vec(vec![0x00, 0x01, 0x02, 0x03]);
    let binding = SerdeBinding::<u32>::new();
    let err = binding.entry_to_object(&entry).unwrap_err();
    assert!(
        matches!(err, noxu_bind::BindError::VersionMismatch { found_magic: 0x00, .. }),
        "expected VersionMismatch with found_magic=0x00, got {err:?}",
    );
}

#[test]
fn tck_serde_version_header_rejects_wrong_version() {
    let mut entry = DatabaseEntry::new();
    // Right magic, wrong version.
    entry.set_data_vec(vec![0xCB, 0xFF, 0x00]);
    let binding = SerdeBinding::<u32>::new();
    let err = binding.entry_to_object(&entry).unwrap_err();
    assert!(
        matches!(err, noxu_bind::BindError::VersionMismatch { found_magic: 0xCB, found_version: 0xFF, .. }),
        "expected VersionMismatch with found_magic=0xCB found_version=0xFF, got {err:?}",
    );
}
