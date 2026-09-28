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
//! The 2-byte header (see `SerdeBinding`'s module docs) is what guards
//! against decoding a payload written with a different wire format.
//! See `tck_serde_version_header` below.
//!
//! ## `collections.test.serial` package: N/A / COVERED-CITED map
//!
//! The four JE `@Test` methods in
//! `com.sleepycat.collections.test.serial` all sit on top of Java Object
//! Serialization + `StoredClassCatalog`, which is a documented intentional
//! deviation (Noxu is serde-based; there is no `ObjectOutputStream`, no
//! `ObjectStreamClass`, no external class-descriptor catalog).  The split of
//! Java-serialization MECHANISM (N/A) vs. catalog/factory PURPOSE
//! (COVERED-CITED) is:
//!
//! - `StoredClassCatalogTestInit.runTest` -> **N/A (Java serialization
//!   mechanism)**: serializes a `java.io.Serializable` `TestSerial` graph
//!   through `SerialBinding` and asserts the `StoredClassCatalog` stored the
//!   `ObjectStreamClass` so records reference it by numeric class ID.  Noxu's
//!   serde binding compiles the type in as a generic parameter, so this
//!   compression catalog is structurally absent.  (Noxu-persist's
//!   `ClassCatalog` stores *class versions keyed by name* for schema
//!   evolution -- a different purpose, not the serialization-metadata dedup
//!   catalog JE tests here.)
//! - `StoredClassCatalogTest.runTest` -> **N/A (Java serialization
//!   mechanism)**: reads objects written with the *old* class format and
//!   writes with the *new* format across a `serialVersionUID` change, and
//!   calls `catalog.getClassID(ObjectStreamClass)`.  This is Java's
//!   serialization class-evolution machinery; Noxu has no `serialVersionUID`
//!   / `ObjectStreamClass`.
//! - `CatalogCornerCaseTest.testReadOnlyEmptyCatalog` -> **COVERED-CITED**:
//!   the portable corner-case PURPOSE (a read-only catalog with no on-disk
//!   backing cannot be written) is exercised in noxu-persist
//!   `evolve::catalog` test `read_only_empty_catalog_rejects_writes`.
//! - `TupleSerialFactoryTest.runTest` -> **COVERED-CITED**: the tuple-key
//!   plus value binding produced by the factory is `TupleSerdeBinding`
//!   (round-trip and key extraction in `tuple_serde_binding.rs`), and the
//!   factory test's foreign-key CASCADE body is covered by noxu-collections
//!   `test_foreign_key_delete_cascade_pattern` and noxu-db
//!   `secondary_decisions_test.rs`.  The Java-serial VALUE mechanism is N/A.

//! ## `bind.serial.test` package (`SerialBindingTest`): N/A / COVERED-CITED map
//!
//! `SerialBindingTest` has **7** `@Test` methods, all sitting on top of JE's
//! `SerialBinding` = `java.io.ObjectOutputStream`/`ObjectInputStream` over a
//! `Serializable` object graph, deduped by a `StoredClassCatalog` keyed on
//! `ObjectStreamClass` descriptors.  The Java-object-serialization MECHANISM
//! (`ObjectOutputStream`, `StoredClassCatalog`, `serialVersionUID`,
//! `FastOutputStream` buffer tuning, classloader override) is a documented
//! intentional deviation with no Rust analog -- see `tp-je-serializecompat.md`
//! (`SerializeReadObjectsTest` = all-N/A, `serialVersionUID`/`ObjectInputStream`
//! deviation) and `tp-collections-serial.md` (`StoredClassCatalog` structurally
//! absent).  The general binding PURPOSE (bind a value type to bytes, round-trip,
//! null/absent value, key+value entity binding, tuple-key + value entity binding)
//! IS portable and is covered on noxu's `SerdeBinding`/`TupleSerdeBinding`:
//!
//! - `SerialBindingTest.testPrimitiveBindings` -> **COVERED-CITED**: value
//!   round-trip for each primitive (String, char, bool, i8/i16/i32/i64, f32/f64)
//!   in `tck_serial_primitive_bindings`.  JE's wrong-class `IllegalArgumentException`
//!   sub-check is **N/A**: in Rust the base type is a compile-time generic
//!   parameter `T`, so feeding a wrong-typed value is a *compile* error, not a
//!   runtime `IllegalArgumentException` -- there is no runtime type mismatch to test.
//! - `SerialBindingTest.testNullObjects` -> **COVERED-CITED**: the "null value"
//!   PURPOSE maps to `Option<T>::None`; encoding None yields a non-empty entry
//!   that round-trips to None in `tck_serial_null_objects`.  (JE's `SerialBinding`
//!   with a `null` base class serialising a Java `null` reference is the
//!   `ObjectOutputStream` mechanism; the value-absence purpose is what ports.)
//! - `SerialBindingTest.testSerialSerialBinding` -> **COVERED-CITED**: the
//!   key-binding + value-binding entity pair PURPOSE, in
//!   `tck_serial_serial_binding_pair_round_trip`.  (`SerialSerialBinding` itself
//!   -- two `SerialBinding`s -- is the serialization mechanism; the two-binding
//!   entity round-trip is what ports.)
//! - `SerialBindingTest.testTupleSerialMarshalledBinding` -> **COVERED-CITED**:
//!   tuple-encoded key + serde-encoded value entity binding = `TupleSerdeBinding`,
//!   round-trip in `tck_tuple_serial_marshalled_binding_round_trip`, which also
//!   asserts the deterministic key-length invariant (JE:
//!   `MarshalledObject.expectedKeyLength() == primaryKey.length() + 1`; noxu's
//!   sort-preserving tuple string terminator is two bytes, so `len + 2` -- the
//!   one-vs-two-byte terminator is the documented tuple-format deviation recorded
//!   in `tck_tuple_format.rs`).
//! - `SerialBindingTest.testBufferSize` -> **N/A (Java-serialization mechanism)**
//!   for the buffer-tuning surface (`FastOutputStream.DEFAULT_INIT_SIZE`,
//!   `setSerialBufferSize`, `getSerialOutput`) -- noxu's encoder owns its own
//!   `Vec` growth and exposes no binding-level buffer size.  The portable residue
//!   (fixed constant-size header overhead + deterministic encoding) is asserted in
//!   `tck_serial_buffer_overhead_is_constant` / `tck_serial_encoding_is_deterministic`.
//! - `SerialBindingTest.testBufferOverride` -> **N/A (Java-serialization
//!   mechanism)**: overriding `getSerialOutput` to supply a cached
//!   `FastOutputStream` is `ObjectOutputStream`-plumbing with no noxu analog.
//!   Covered residue: same deterministic-encoding invariant above.
//! - `SerialBindingTest.testClassloaderOverride` -> **N/A (Java reflection /
//!   classloading mechanism)**: overriding `SerialBinding.getClassLoader` so
//!   `ObjectInputStream.resolveClass` uses a custom `ClassLoader` is pure JVM
//!   class resolution.  Rust monomorphises `T`; there is no classloader.  The
//!   analogous "fail fast on a payload that cannot be decoded by this binding"
//!   guarantee is the magic+version header, asserted in
//!   `tck_serde_version_header_*`.
//!
//! Count: JE @Test in `SerialBindingTest` = 7; COVERED-CITED = 4
//! (`testPrimitiveBindings`, `testNullObjects`, `testSerialSerialBinding`,
//! `testTupleSerialMarshalledBinding`); N/A = 3 (`testBufferSize`,
//! `testBufferOverride`, `testClassloaderOverride`).  4 + 3 = 7.

use noxu_bind::{EntityBinding, EntryBinding, SerdeBinding, TupleSerdeBinding};
use noxu_db::DatabaseEntry;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// JE: SerialBindingTest.testPrimitiveBindings — COVERED-CITED (value round-trip).
// Primitive bindings round-trip.
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
// JE: SerialBindingTest.testNullObjects — COVERED-CITED (Option::None value absence).
// "Null object" handling.
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
// JE: SerialBindingTest.testSerialSerialBinding — COVERED-CITED (key+value entity pair).
// SerialSerialBinding analogue.
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
// JE: SerialBindingTest.testTupleSerialMarshalledBinding — COVERED-CITED (tuple key + serde value).
// TupleSerial(Marshalled)Binding.
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

    // JE `MarshalledObject.expectedKeyLength()` returns `primaryKey.length() + 1`
    // (JE `TupleOutput.writeString` appends a single 0x00 terminator).  Noxu's
    // sort-preserving tuple string terminator is two bytes (`[0x00, 0x00]`, for
    // null-escaping / correct byte-order comparison — the documented tuple-format
    // deviation recorded in `tck_tuple_format.rs`), so the faithful analogue of
    // JE's `assertEquals(val.expectedKeyLength(), keyBuffer.getSize())` is
    // `key.len() + 2` for a primary key with no embedded 0x00 bytes.
    let name = "Alice".to_string();
    let original = Person { name: name.clone(), age: 30 };
    let mut key_buf = DatabaseEntry::new();
    let mut data_buf = DatabaseEntry::new();
    binding.object_to_key(&original, &mut key_buf).unwrap();
    binding.object_to_data(&original, &mut data_buf).unwrap();
    assert!(!key_buf.data().is_empty());
    assert!(!data_buf.data().is_empty());
    // Deterministic key length: UTF-8 payload + 2-byte terminator (noxu deviation
    // from JE's 1-byte terminator).  De-vacuums the round-trip: a regression in
    // the tuple key encoding (dropped/extra terminator) would fail here.
    assert_eq!(
        key_buf.data().len(),
        name.len() + 2,
        "tuple string key = UTF-8 payload + 2-byte terminator"
    );

    let decoded = binding.entry_to_object(&key_buf, &data_buf).unwrap();
    assert_eq!(original, decoded);
}

// ---------------------------------------------------------------------------
// JE: SerialBindingTest.testBufferSize / testBufferOverride — buffer-tuning MECHANISM is
// N/A (FastOutputStream / setSerialBufferSize; noxu encoder owns its Vec growth);
// the portable residue (constant-size header overhead + deterministic encoding).
// Buffer size / overhead.
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
// JE: SerialBindingTest.testClassloaderOverride — classloader/reflection MECHANISM is
// N/A (no JVM ClassLoader in Rust; T is monomorphised); the analogous
// "fail fast on an undecodable payload" guarantee is the magic+version header.
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
        matches!(
            err,
            noxu_bind::BindError::VersionMismatch { found_magic: 0x00, .. }
        ),
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
        matches!(
            err,
            noxu_bind::BindError::VersionMismatch {
                found_magic: 0xCB,
                found_version: 0xFF,
                ..
            }
        ),
        "expected VersionMismatch with found_magic=0xCB found_version=0xFF, got {err:?}",
    );
}
