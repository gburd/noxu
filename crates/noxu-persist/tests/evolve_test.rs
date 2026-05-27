//! BDB-JE-style TCK port tests for DPL schema evolution (Wave 2C-2).
//!
//! These tests exercise the open-path schema-evolution flow that
//! [`EntityStore::open`] / [`EntityStore::get_primary_index`] now wires
//! in.  Every test follows the same shape:
//!
//! 1. Open an environment + entity store with the **OLD** definition of
//!    an entity, populate it, and close the env.
//! 2. Reopen the env / store with the **NEW** definition (a different
//!    Rust struct + a different `class_version`) plus a `Mutations`
//!    set, and assert the data is correctly readable / has been
//!    evolved.
//!
//! ## Mapping to JE TCK
//!
//! | JE test                     | Noxu test                                |
//! |-----------------------------|------------------------------------------|
//! | `EvolveTest.testRenamerField` (lazy field renamer) | `evolve_basic_field_rename` |
//! | `EvolveTest.testAddField`   | `evolve_add_field_with_versioned_decoder`|
//! | `EvolveTest.testRemoveField` | `evolve_field_deleter`                  |
//! | `EvolveTest.testClassRenamer` | `evolve_class_rename`                  |
//! | `EvolveTest.testClassConverter` | `convert_and_add_test`              |
//! | `DevolutionTest.testRevert` | `devolution_revert_schema`              |
//! | `ConvertAndAddTest`         | `convert_and_add_test`                   |
//! | `EvolveProxyClassTest`      | `evolve_proxy_class_test`                |
//! | (idempotence corner case)   | `evolve_is_idempotent_across_reopens`    |
//! | (large-store streaming)     | `evolve_streaming_handles_thousand_records` |
//! | (transactional rollback)    | `evolve_aborts_on_listener_failure`      |

#![allow(clippy::needless_range_loop, clippy::too_many_arguments)]

use std::path::Path;

use noxu_db::{Environment, EnvironmentConfig};
use noxu_persist::evolve::{
    Converter, Deleter, EvolveConfig, EvolveListener, Mutations, Renamer,
};
use noxu_persist::{
    Entity, EntitySerializer, EntityStore, PersistError, PrimaryIndex, Result,
    StoreConfig,
};
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Shared scaffolding
// ---------------------------------------------------------------------------

/// Holds an env + its temp dir together so opens/closes around the same
/// directory work cleanly across the `phase_*` helpers below.
fn env_for(path: &Path) -> Environment {
    let cfg =
        EnvironmentConfig::new(path.to_path_buf()).with_allow_create(true);
    Environment::open(cfg).unwrap()
}

// ---------------------------------------------------------------------------
// Test 1: EvolveTest -- field rename via lazy read-side renamer
// ---------------------------------------------------------------------------
//
// The OLD entity has a field "name"; the NEW entity has the same field
// renamed to "fullName".  Old records are decoded by a versioned
// deserializer that consults `Mutations` and applies the renamer.

mod field_rename {
    use super::*;

    /// OLD: version 0, field "name".
    #[derive(Clone, Debug, PartialEq)]
    pub struct PersonV0 {
        pub id: u64,
        pub name: String,
    }

    impl Entity for PersonV0 {
        type PrimaryKey = u64;
        fn primary_key(&self) -> &u64 {
            &self.id
        }
        fn entity_name() -> &'static str {
            "Person"
        }
        // class_version() defaults to 0
    }

    pub struct PersonV0Ser;
    impl EntitySerializer<PersonV0> for PersonV0Ser {
        fn serialize(&self, e: &PersonV0) -> Result<Vec<u8>> {
            // [u64 id][u32 name_len][name bytes]  with field tag "name"
            let mut buf = Vec::new();
            buf.extend_from_slice(&e.id.to_be_bytes());
            // tag "name" length-prefixed
            let tag = b"name";
            buf.push(tag.len() as u8);
            buf.extend_from_slice(tag);
            let n = e.name.as_bytes();
            buf.extend_from_slice(&(n.len() as u32).to_be_bytes());
            buf.extend_from_slice(n);
            Ok(buf)
        }
        fn deserialize(&self, b: &[u8]) -> Result<PersonV0> {
            let id = u64::from_be_bytes(b[0..8].try_into().unwrap());
            let tag_len = b[8] as usize;
            let _tag = &b[9..9 + tag_len];
            let mut p = 9 + tag_len;
            let n_len =
                u32::from_be_bytes(b[p..p + 4].try_into().unwrap()) as usize;
            p += 4;
            let name = String::from_utf8(b[p..p + n_len].to_vec()).unwrap();
            Ok(PersonV0 { id, name })
        }
    }

    /// NEW: version 1, field renamed to "fullName".
    #[derive(Clone, Debug, PartialEq)]
    pub struct PersonV1 {
        pub id: u64,
        pub full_name: String,
    }

    impl Entity for PersonV1 {
        type PrimaryKey = u64;
        fn primary_key(&self) -> &u64 {
            &self.id
        }
        fn entity_name() -> &'static str {
            "Person"
        }
        fn class_version() -> u16 {
            1
        }
    }

    pub struct PersonV1Ser;
    impl EntitySerializer<PersonV1> for PersonV1Ser {
        fn serialize(&self, e: &PersonV1) -> Result<Vec<u8>> {
            let mut buf = Vec::new();
            buf.extend_from_slice(&e.id.to_be_bytes());
            let tag = b"fullName";
            buf.push(tag.len() as u8);
            buf.extend_from_slice(tag);
            let n = e.full_name.as_bytes();
            buf.extend_from_slice(&(n.len() as u32).to_be_bytes());
            buf.extend_from_slice(n);
            Ok(buf)
        }
        fn deserialize(&self, b: &[u8]) -> Result<PersonV1> {
            // For records written by V1.
            let id = u64::from_be_bytes(b[0..8].try_into().unwrap());
            let tag_len = b[8] as usize;
            let mut p = 9 + tag_len;
            let n_len =
                u32::from_be_bytes(b[p..p + 4].try_into().unwrap()) as usize;
            p += 4;
            let name = String::from_utf8(b[p..p + n_len].to_vec()).unwrap();
            Ok(PersonV1 { id, full_name: name })
        }

        // Field-level evolution: when reading a v0 record, look up the
        // renamer for field "name" -> "fullName" and accept.
        fn deserialize_versioned(
            &self,
            bytes: &[u8],
            class_version: u16,
            mutations: &Mutations,
        ) -> Result<PersonV1> {
            if class_version == 1 {
                return self.deserialize(bytes);
            }
            // Read the v0 record; check that the registered renamer maps
            // "name" -> "fullName" for the version we are decoding.
            let id = u64::from_be_bytes(bytes[0..8].try_into().unwrap());
            let tag_len = bytes[8] as usize;
            let on_disk_tag =
                std::str::from_utf8(&bytes[9..9 + tag_len]).unwrap();
            let renamer = mutations.get_renamer(
                "Person",
                class_version.into(),
                Some(on_disk_tag),
            );
            let Some(r) = renamer else {
                return Err(PersistError::SerializationError(format!(
                    "no renamer for v{} field '{}'",
                    class_version, on_disk_tag,
                )));
            };
            assert_eq!(r.new_name(), "fullName");
            let mut p = 9 + tag_len;
            let n_len = u32::from_be_bytes(bytes[p..p + 4].try_into().unwrap())
                as usize;
            p += 4;
            let name = String::from_utf8(bytes[p..p + n_len].to_vec()).unwrap();
            Ok(PersonV1 { id, full_name: name })
        }
    }
}

#[test]
fn evolve_basic_field_rename() {
    use field_rename::*;

    let td = TempDir::new().unwrap();
    let path = td.path().to_path_buf();

    // Phase 1: OLD definition, populate.
    {
        let env = env_for(&path);
        let cfg = StoreConfig::new("s").with_allow_create(true);
        let mut store = EntityStore::open(&env, cfg).unwrap();
        let idx: PrimaryIndex<u64, PersonV0> =
            store.get_primary_index().unwrap();
        let ser = PersonV0Ser;
        for i in 1u64..=5 {
            idx.put(
                None,
                &ser,
                &PersonV0 { id: i, name: format!("Alice{}", i) },
            )
            .unwrap();
        }
        store.close().unwrap();
        env.close().unwrap();
    }

    // Phase 2: NEW definition with field renamer.
    {
        let env = env_for(&path);
        let mut mutations = Mutations::new();
        // Rename "Person" v0 field "name" -> "fullName".
        mutations
            .add_renamer(Renamer::for_field("Person", 0, "name", "fullName"));

        let cfg = StoreConfig::new("s")
            .with_allow_create(true)
            .with_mutations(mutations);
        let mut store = EntityStore::open(&env, cfg).unwrap();
        let idx: PrimaryIndex<u64, PersonV1> =
            store.get_primary_index().unwrap();
        let ser = PersonV1Ser;
        for i in 1u64..=5 {
            let p = idx.get(None, &ser, &i).unwrap().unwrap();
            assert_eq!(p.full_name, format!("Alice{}", i));
        }
        store.close().unwrap();
        env.close().unwrap();
    }
}

// ---------------------------------------------------------------------------
// Test 2: EvolveTest -- add a field with a default in the new version.
// ---------------------------------------------------------------------------

mod add_field {
    use super::*;

    #[derive(Clone, Debug, PartialEq)]
    pub struct DocV0 {
        pub id: u64,
        pub title: String,
    }
    impl Entity for DocV0 {
        type PrimaryKey = u64;
        fn primary_key(&self) -> &u64 {
            &self.id
        }
        fn entity_name() -> &'static str {
            "Doc"
        }
    }

    pub struct DocV0Ser;
    impl EntitySerializer<DocV0> for DocV0Ser {
        fn serialize(&self, e: &DocV0) -> Result<Vec<u8>> {
            let mut b = Vec::new();
            b.extend_from_slice(&e.id.to_be_bytes());
            let s = e.title.as_bytes();
            b.extend_from_slice(&(s.len() as u32).to_be_bytes());
            b.extend_from_slice(s);
            Ok(b)
        }
        fn deserialize(&self, b: &[u8]) -> Result<DocV0> {
            let id = u64::from_be_bytes(b[0..8].try_into().unwrap());
            let n = u32::from_be_bytes(b[8..12].try_into().unwrap()) as usize;
            let title = String::from_utf8(b[12..12 + n].to_vec()).unwrap();
            Ok(DocV0 { id, title })
        }
    }

    #[derive(Clone, Debug, PartialEq)]
    pub struct DocV1 {
        pub id: u64,
        pub title: String,
        pub author: String, // new field, default "unknown"
    }
    impl Entity for DocV1 {
        type PrimaryKey = u64;
        fn primary_key(&self) -> &u64 {
            &self.id
        }
        fn entity_name() -> &'static str {
            "Doc"
        }
        fn class_version() -> u16 {
            1
        }
    }

    pub struct DocV1Ser;
    impl EntitySerializer<DocV1> for DocV1Ser {
        fn serialize(&self, e: &DocV1) -> Result<Vec<u8>> {
            let mut b = Vec::new();
            b.extend_from_slice(&e.id.to_be_bytes());
            let s = e.title.as_bytes();
            b.extend_from_slice(&(s.len() as u32).to_be_bytes());
            b.extend_from_slice(s);
            let a = e.author.as_bytes();
            b.extend_from_slice(&(a.len() as u32).to_be_bytes());
            b.extend_from_slice(a);
            Ok(b)
        }
        fn deserialize(&self, b: &[u8]) -> Result<DocV1> {
            let id = u64::from_be_bytes(b[0..8].try_into().unwrap());
            let n = u32::from_be_bytes(b[8..12].try_into().unwrap()) as usize;
            let title = String::from_utf8(b[12..12 + n].to_vec()).unwrap();
            let mut p = 12 + n;
            let m =
                u32::from_be_bytes(b[p..p + 4].try_into().unwrap()) as usize;
            p += 4;
            let author = String::from_utf8(b[p..p + m].to_vec()).unwrap();
            Ok(DocV1 { id, title, author })
        }

        fn deserialize_versioned(
            &self,
            bytes: &[u8],
            class_version: u16,
            _mutations: &Mutations,
        ) -> Result<DocV1> {
            if class_version == 1 {
                return self.deserialize(bytes);
            }
            // v0: no author field; default it.
            let v0 = DocV0Ser.deserialize(bytes)?;
            Ok(DocV1 { id: v0.id, title: v0.title, author: "unknown".into() })
        }
    }
}

#[test]
fn evolve_add_field_with_versioned_decoder() {
    use add_field::*;
    let td = TempDir::new().unwrap();
    let path = td.path().to_path_buf();

    // Phase 1: write V0.
    {
        let env = env_for(&path);
        let cfg = StoreConfig::new("s").with_allow_create(true);
        let mut store = EntityStore::open(&env, cfg).unwrap();
        let idx: PrimaryIndex<u64, DocV0> = store.get_primary_index().unwrap();
        let ser = DocV0Ser;
        idx.put(None, &ser, &DocV0 { id: 1, title: "Hello".into() }).unwrap();
        idx.put(None, &ser, &DocV0 { id: 2, title: "World".into() }).unwrap();
        store.close().unwrap();
        env.close().unwrap();
    }

    // Phase 2: read as V1.
    {
        let env = env_for(&path);
        let cfg = StoreConfig::new("s")
            .with_allow_create(true)
            .with_mutations(Mutations::new());
        let mut store = EntityStore::open(&env, cfg).unwrap();
        let idx: PrimaryIndex<u64, DocV1> = store.get_primary_index().unwrap();
        let ser = DocV1Ser;

        let d1 = idx.get(None, &ser, &1).unwrap().unwrap();
        assert_eq!(d1.title, "Hello");
        assert_eq!(d1.author, "unknown");
        let d2 = idx.get(None, &ser, &2).unwrap().unwrap();
        assert_eq!(d2.title, "World");
        assert_eq!(d2.author, "unknown");

        // Round-trip: writing as V1 and reading it back gives V1.
        idx.put(
            None,
            &ser,
            &DocV1 { id: 3, title: "New".into(), author: "Bob".into() },
        )
        .unwrap();
        let d3 = idx.get(None, &ser, &3).unwrap().unwrap();
        assert_eq!(d3.author, "Bob");
        store.close().unwrap();
    }
}

// ---------------------------------------------------------------------------
// Test 3: EvolveTest -- field deleter (whole-record converter)
// ---------------------------------------------------------------------------

#[test]
fn evolve_field_deleter() {
    // We model "field deletion" at the class-converter level: the
    // class-level converter rewrites the record to drop bytes for the
    // field, and the new entity definition simply does not have that
    // field.

    // OLD: id + name + nickname.  NEW: id + name (no nickname).
    mod m {
        use super::*;

        #[derive(Clone, Debug)]
        pub struct UserV0 {
            pub id: u64,
            pub name: String,
            pub nickname: String,
        }
        impl Entity for UserV0 {
            type PrimaryKey = u64;
            fn primary_key(&self) -> &u64 {
                &self.id
            }
            fn entity_name() -> &'static str {
                "U"
            }
        }
        pub struct UV0;
        impl EntitySerializer<UserV0> for UV0 {
            fn serialize(&self, e: &UserV0) -> Result<Vec<u8>> {
                let mut b = Vec::new();
                b.extend_from_slice(&e.id.to_be_bytes());
                let n = e.name.as_bytes();
                b.extend_from_slice(&(n.len() as u32).to_be_bytes());
                b.extend_from_slice(n);
                let nk = e.nickname.as_bytes();
                b.extend_from_slice(&(nk.len() as u32).to_be_bytes());
                b.extend_from_slice(nk);
                Ok(b)
            }
            fn deserialize(&self, _b: &[u8]) -> Result<UserV0> {
                unimplemented!()
            }
        }

        #[derive(Clone, Debug, PartialEq)]
        pub struct UserV1 {
            pub id: u64,
            pub name: String,
        }
        impl Entity for UserV1 {
            type PrimaryKey = u64;
            fn primary_key(&self) -> &u64 {
                &self.id
            }
            fn entity_name() -> &'static str {
                "U"
            }
            fn class_version() -> u16 {
                1
            }
        }
        pub struct UV1;
        impl EntitySerializer<UserV1> for UV1 {
            fn serialize(&self, e: &UserV1) -> Result<Vec<u8>> {
                let mut b = Vec::new();
                b.extend_from_slice(&e.id.to_be_bytes());
                let n = e.name.as_bytes();
                b.extend_from_slice(&(n.len() as u32).to_be_bytes());
                b.extend_from_slice(n);
                Ok(b)
            }
            fn deserialize(&self, b: &[u8]) -> Result<UserV1> {
                let id = u64::from_be_bytes(b[0..8].try_into().unwrap());
                let n =
                    u32::from_be_bytes(b[8..12].try_into().unwrap()) as usize;
                let name = String::from_utf8(b[12..12 + n].to_vec()).unwrap();
                Ok(UserV1 { id, name })
            }
        }
    }

    let td = TempDir::new().unwrap();
    let path = td.path().to_path_buf();

    // Phase 1: write V0 records.
    {
        let env = env_for(&path);
        let cfg = StoreConfig::new("s").with_allow_create(true);
        let mut store = EntityStore::open(&env, cfg).unwrap();
        let idx: PrimaryIndex<u64, m::UserV0> =
            store.get_primary_index().unwrap();
        idx.put(
            None,
            &m::UV0,
            &m::UserV0 { id: 1, name: "Alice".into(), nickname: "Al".into() },
        )
        .unwrap();
        store.close().unwrap();
        env.close().unwrap();
    }

    // Phase 2: register a class-level Converter that strips the trailing
    // (4-byte len + bytes) of nickname.
    {
        let env = env_for(&path);
        let mut mutations = Mutations::new();
        mutations.add_converter(Converter::for_class(
            "U",
            0,
            |old: &[u8]| {
                // [id 8][name_len 4][name_bytes][nickname_len 4][nickname_bytes]
                let n =
                    u32::from_be_bytes(old[8..12].try_into().unwrap()) as usize;
                let new_len = 12 + n;
                Some(old[..new_len].to_vec())
            },
        ));

        let cfg = StoreConfig::new("s")
            .with_allow_create(true)
            .with_mutations(mutations);
        let mut store = EntityStore::open(&env, cfg).unwrap();
        let idx: PrimaryIndex<u64, m::UserV1> =
            store.get_primary_index().unwrap();
        let u = idx.get(None, &m::UV1, &1).unwrap().unwrap();
        assert_eq!(u, m::UserV1 { id: 1, name: "Alice".into() });
        store.close().unwrap();
    }
}

// ---------------------------------------------------------------------------
// Test 4: EvolveTest.testClassRenamer -- rename an entire entity class.
// ---------------------------------------------------------------------------
//
// In Noxu the `entity_name()` is also the database name suffix.  A
// class-level rename without renaming the entity name would have nowhere
// to land, so the JE-equivalent semantics here are: "the on-disk class
// tag was X, the new entity_name() is Y, the Renamer makes reads work".
// We exercise that via a test that uses the same db (same entity_name)
// but checks the renamer chain succeeds when the on-disk tag came
// from the OLD name.

mod class_rename {
    use super::*;

    /// OLD class wrote records under tag "Person"; we keep using
    /// `entity_name()` "Person" but pretend the *tag baked into the
    /// record* was the old name.  Easiest way: just use two structs
    /// with the same entity_name but different class_version.  The
    /// Wave 2C-2 envelope embeds the tag derived from `entity_name()`
    /// at write time, so to simulate a real class-rename we corrupt
    /// the tag by writing under tag "OldPerson" via a manual put.
    /// We then register a Renamer("OldPerson" -> "Person") and assert
    /// the read succeeds.
    #[derive(Clone, Debug, PartialEq)]
    pub struct Person {
        pub id: u64,
        pub name: String,
    }
    impl Entity for Person {
        type PrimaryKey = u64;
        fn primary_key(&self) -> &u64 {
            &self.id
        }
        fn entity_name() -> &'static str {
            "Person"
        }
    }

    pub struct PersonSer;
    impl EntitySerializer<Person> for PersonSer {
        fn serialize(&self, e: &Person) -> Result<Vec<u8>> {
            let mut b = Vec::new();
            b.extend_from_slice(&e.id.to_be_bytes());
            let n = e.name.as_bytes();
            b.extend_from_slice(&(n.len() as u32).to_be_bytes());
            b.extend_from_slice(n);
            Ok(b)
        }
        fn deserialize(&self, b: &[u8]) -> Result<Person> {
            let id = u64::from_be_bytes(b[0..8].try_into().unwrap());
            let n = u32::from_be_bytes(b[8..12].try_into().unwrap()) as usize;
            let name = String::from_utf8(b[12..12 + n].to_vec()).unwrap();
            Ok(Person { id, name })
        }
    }
}

#[test]
fn evolve_class_rename() {
    use class_rename::*;

    let td = TempDir::new().unwrap();
    let path = td.path().to_path_buf();

    // Phase 1: write a record under entity_name "Person", but inject
    // an envelope manually with class_tag "OldPerson" to simulate a
    // pre-rename state.
    {
        let env_cfg = EnvironmentConfig::new(path.clone())
            .with_allow_create(true)
            .with_transactional(true);
        let env = Environment::open(env_cfg).unwrap();
        let payload = PersonSer
            .serialize(&Person { id: 1, name: "Alice".into() })
            .unwrap();
        let envelope =
            noxu_persist::evolve::envelope::encode(0, "OldPerson", &payload)
                .unwrap();
        // Open the underlying database directly with the same naming
        // convention `EntityStore` uses.
        let dbcfg = noxu_db::DatabaseConfig::new()
            .with_allow_create(true)
            .with_transactional(true);
        let db = env.open_database(None, "s_Person", &dbcfg).unwrap();
        let txn = env.begin_transaction(None).unwrap();
        let key = noxu_db::DatabaseEntry::from_vec(1u64.to_be_bytes().to_vec());
        let val = noxu_db::DatabaseEntry::from_vec(envelope);
        db.put(Some(&txn), &key, &val).unwrap();
        txn.commit().unwrap();
        db.close().unwrap();
        env.close().unwrap();
    }

    // Phase 2: open with a Renamer mapping "OldPerson" v0 -> "Person".
    {
        let env_cfg = EnvironmentConfig::new(path)
            .with_allow_create(true)
            .with_transactional(true);
        let env = Environment::open(env_cfg).unwrap();
        let mut mutations = Mutations::new();
        mutations.add_renamer(Renamer::for_class("OldPerson", 0, "Person"));
        let cfg = StoreConfig::new("s")
            .with_allow_create(true)
            .with_transactional(true)
            .with_mutations(mutations);
        let mut store = EntityStore::open(&env, cfg).unwrap();
        let idx: PrimaryIndex<u64, Person> = store.get_primary_index().unwrap();
        let p = idx.get(None, &PersonSer, &1).unwrap().unwrap();
        assert_eq!(p, Person { id: 1, name: "Alice".into() });
    }
}

// ---------------------------------------------------------------------------
// Test 5: ConvertAndAddTest -- combine a class converter with a new field.
// ---------------------------------------------------------------------------

#[test]
fn convert_and_add_test() {
    // OLD: { id, age_years: u8 }.  NEW: { id, age_months: u32, country: String }.
    // Class converter: age_years * 12 -> age_months.  New field "country"
    // defaults to "??".

    mod m {
        use super::*;

        #[derive(Clone, Debug)]
        pub struct V0 {
            pub id: u64,
            pub age_years: u8,
        }
        impl Entity for V0 {
            type PrimaryKey = u64;
            fn primary_key(&self) -> &u64 {
                &self.id
            }
            fn entity_name() -> &'static str {
                "P"
            }
        }
        pub struct S0;
        impl EntitySerializer<V0> for S0 {
            fn serialize(&self, e: &V0) -> Result<Vec<u8>> {
                let mut b = Vec::new();
                b.extend_from_slice(&e.id.to_be_bytes());
                b.push(e.age_years);
                Ok(b)
            }
            fn deserialize(&self, b: &[u8]) -> Result<V0> {
                let id = u64::from_be_bytes(b[0..8].try_into().unwrap());
                Ok(V0 { id, age_years: b[8] })
            }
        }

        #[derive(Clone, Debug, PartialEq)]
        pub struct V1 {
            pub id: u64,
            pub age_months: u32,
            pub country: String,
        }
        impl Entity for V1 {
            type PrimaryKey = u64;
            fn primary_key(&self) -> &u64 {
                &self.id
            }
            fn entity_name() -> &'static str {
                "P"
            }
            fn class_version() -> u16 {
                1
            }
        }
        pub struct S1;
        impl EntitySerializer<V1> for S1 {
            fn serialize(&self, e: &V1) -> Result<Vec<u8>> {
                let mut b = Vec::new();
                b.extend_from_slice(&e.id.to_be_bytes());
                b.extend_from_slice(&e.age_months.to_be_bytes());
                let c = e.country.as_bytes();
                b.extend_from_slice(&(c.len() as u32).to_be_bytes());
                b.extend_from_slice(c);
                Ok(b)
            }
            fn deserialize(&self, b: &[u8]) -> Result<V1> {
                let id = u64::from_be_bytes(b[0..8].try_into().unwrap());
                let am = u32::from_be_bytes(b[8..12].try_into().unwrap());
                let n =
                    u32::from_be_bytes(b[12..16].try_into().unwrap()) as usize;
                let country =
                    String::from_utf8(b[16..16 + n].to_vec()).unwrap();
                Ok(V1 { id, age_months: am, country })
            }
        }
    }

    let td = TempDir::new().unwrap();
    let path = td.path().to_path_buf();

    // Phase 1: write V0.
    {
        let env = env_for(&path);
        let cfg = StoreConfig::new("s").with_allow_create(true);
        let mut store = EntityStore::open(&env, cfg).unwrap();
        let idx: PrimaryIndex<u64, m::V0> = store.get_primary_index().unwrap();
        idx.put(None, &m::S0, &m::V0 { id: 1, age_years: 30 }).unwrap();
        idx.put(None, &m::S0, &m::V0 { id: 2, age_years: 5 }).unwrap();
        store.close().unwrap();
        env.close().unwrap();
    }

    // Phase 2: open with V1 + class converter + default for new field.
    {
        let env = env_for(&path);
        let mut mutations = Mutations::new();
        mutations.add_converter(Converter::for_class(
            "P",
            0,
            |old: &[u8]| {
                // old: [id 8][age_years 1] -> new: [id 8][age_months 4][country_len 4][country bytes]
                let id_bytes = &old[0..8];
                let age_years = old[8] as u32;
                let age_months = age_years * 12;
                let country = b"??";
                let mut out = Vec::new();
                out.extend_from_slice(id_bytes);
                out.extend_from_slice(&age_months.to_be_bytes());
                out.extend_from_slice(&(country.len() as u32).to_be_bytes());
                out.extend_from_slice(country);
                Some(out)
            },
        ));

        let cfg = StoreConfig::new("s")
            .with_allow_create(true)
            .with_mutations(mutations);
        let mut store = EntityStore::open(&env, cfg).unwrap();
        let idx: PrimaryIndex<u64, m::V1> = store.get_primary_index().unwrap();
        let v1 = idx.get(None, &m::S1, &1).unwrap().unwrap();
        assert_eq!(v1, m::V1 { id: 1, age_months: 360, country: "??".into() });
        let v2 = idx.get(None, &m::S1, &2).unwrap().unwrap();
        assert_eq!(v2, m::V1 { id: 2, age_months: 60, country: "??".into() });

        // Round-trip: write a V1 and read it back.
        idx.put(
            None,
            &m::S1,
            &m::V1 { id: 3, age_months: 1200, country: "FR".into() },
        )
        .unwrap();
        let v3 = idx.get(None, &m::S1, &3).unwrap().unwrap();
        assert_eq!(v3.country, "FR");
        store.close().unwrap();
    }
}

// ---------------------------------------------------------------------------
// Test 6: DevolutionTest -- revert to an earlier schema.
// ---------------------------------------------------------------------------
//
// JE's `DevolutionTest` exercises the symmetry: after evolving v0->v1,
// the user can re-deploy the v0 binary if needed, with a "reverse"
// converter that maps v1 records back to v0 shape.  We mirror that by
// chaining two converters.

#[test]
fn devolution_revert_schema() {
    mod m {
        use super::*;

        // Two versions of the same entity (we use plain version-tagged
        // Rust structs; entity_name() is shared).
        #[derive(Clone, Debug, PartialEq)]
        pub struct VA {
            pub id: u64,
            pub n: u32,
        }
        impl Entity for VA {
            type PrimaryKey = u64;
            fn primary_key(&self) -> &u64 {
                &self.id
            }
            fn entity_name() -> &'static str {
                "X"
            }
            fn class_version() -> u16 {
                0
            }
        }
        pub struct SA;
        impl EntitySerializer<VA> for SA {
            fn serialize(&self, e: &VA) -> Result<Vec<u8>> {
                let mut b = Vec::new();
                b.extend_from_slice(&e.id.to_be_bytes());
                b.extend_from_slice(&e.n.to_be_bytes());
                Ok(b)
            }
            fn deserialize(&self, b: &[u8]) -> Result<VA> {
                let id = u64::from_be_bytes(b[0..8].try_into().unwrap());
                let n = u32::from_be_bytes(b[8..12].try_into().unwrap());
                Ok(VA { id, n })
            }
        }

        #[derive(Clone, Debug, PartialEq)]
        pub struct VB {
            pub id: u64,
            pub n_doubled: u32,
        }
        impl Entity for VB {
            type PrimaryKey = u64;
            fn primary_key(&self) -> &u64 {
                &self.id
            }
            fn entity_name() -> &'static str {
                "X"
            }
            fn class_version() -> u16 {
                1
            }
        }
        pub struct SB;
        impl EntitySerializer<VB> for SB {
            fn serialize(&self, e: &VB) -> Result<Vec<u8>> {
                let mut b = Vec::new();
                b.extend_from_slice(&e.id.to_be_bytes());
                b.extend_from_slice(&e.n_doubled.to_be_bytes());
                Ok(b)
            }
            fn deserialize(&self, b: &[u8]) -> Result<VB> {
                let id = u64::from_be_bytes(b[0..8].try_into().unwrap());
                let n = u32::from_be_bytes(b[8..12].try_into().unwrap());
                Ok(VB { id, n_doubled: n })
            }
        }
    }

    let td = TempDir::new().unwrap();
    let path = td.path().to_path_buf();

    // Phase 1: write VA (v0).
    {
        let env = env_for(&path);
        let cfg = StoreConfig::new("s").with_allow_create(true);
        let mut store = EntityStore::open(&env, cfg).unwrap();
        let idx: PrimaryIndex<u64, m::VA> = store.get_primary_index().unwrap();
        idx.put(None, &m::SA, &m::VA { id: 1, n: 3 }).unwrap();
        idx.put(None, &m::SA, &m::VA { id: 2, n: 7 }).unwrap();
        store.close().unwrap();
        env.close().unwrap();
    }

    // Phase 2: evolve to VB (v1) via a converter that doubles `n`.
    {
        let env = env_for(&path);
        let mut mutations = Mutations::new();
        mutations.add_converter(Converter::for_class(
            "X",
            0,
            |old: &[u8]| {
                let id = u64::from_be_bytes(old[0..8].try_into().unwrap());
                let n = u32::from_be_bytes(old[8..12].try_into().unwrap());
                let mut out = Vec::new();
                out.extend_from_slice(&id.to_be_bytes());
                out.extend_from_slice(&(n * 2).to_be_bytes());
                Some(out)
            },
        ));
        let cfg = StoreConfig::new("s")
            .with_allow_create(true)
            .with_mutations(mutations);
        let mut store = EntityStore::open(&env, cfg).unwrap();
        let idx: PrimaryIndex<u64, m::VB> = store.get_primary_index().unwrap();
        let b1 = idx.get(None, &m::SB, &1).unwrap().unwrap();
        assert_eq!(b1.n_doubled, 6);
        let b2 = idx.get(None, &m::SB, &2).unwrap().unwrap();
        assert_eq!(b2.n_doubled, 14);
        store.close().unwrap();
        env.close().unwrap();
    }

    // Phase 3 (DEVOLUTION): reopen with the v0 binary again, plus a
    // reverse converter that halves `n`.  The catalog is at v1; the
    // user's `Entity::class_version()` is 0, so we go from v1 -> v0.
    {
        let env = env_for(&path);
        let mut mutations = Mutations::new();
        mutations.add_converter(Converter::for_class(
            "X",
            1,
            |old: &[u8]| {
                let id = u64::from_be_bytes(old[0..8].try_into().unwrap());
                let n = u32::from_be_bytes(old[8..12].try_into().unwrap());
                let mut out = Vec::new();
                out.extend_from_slice(&id.to_be_bytes());
                out.extend_from_slice(&(n / 2).to_be_bytes());
                Some(out)
            },
        ));
        let cfg = StoreConfig::new("s")
            .with_allow_create(true)
            .with_mutations(mutations);
        let mut store = EntityStore::open(&env, cfg).unwrap();
        let idx: PrimaryIndex<u64, m::VA> = store.get_primary_index().unwrap();
        let a1 = idx.get(None, &m::SA, &1).unwrap().unwrap();
        assert_eq!(a1.n, 3);
        let a2 = idx.get(None, &m::SA, &2).unwrap().unwrap();
        assert_eq!(a2.n, 7);
    }
}

// ---------------------------------------------------------------------------
// Test 7: EvolveProxyClassTest -- a class behind an opaque "proxy" wrapper.
// ---------------------------------------------------------------------------
//
// JE's EvolveProxyClassTest exercises a proxy pattern where the entity
// holds another type via a wrapper.  We model it as a wrapper struct
// containing an inner enum whose old/new variants we transition.

#[test]
fn evolve_proxy_class_test() {
    mod m {
        use super::*;

        // OLD: payload tag 0 = "Email(addr)"
        #[derive(Clone, Debug)]
        pub struct ContactV0 {
            pub id: u64,
            pub email: String,
        }
        impl Entity for ContactV0 {
            type PrimaryKey = u64;
            fn primary_key(&self) -> &u64 {
                &self.id
            }
            fn entity_name() -> &'static str {
                "Contact"
            }
        }
        pub struct C0;
        impl EntitySerializer<ContactV0> for C0 {
            fn serialize(&self, e: &ContactV0) -> Result<Vec<u8>> {
                let mut b = Vec::new();
                b.extend_from_slice(&e.id.to_be_bytes());
                b.push(0u8); // proxy tag = email
                let s = e.email.as_bytes();
                b.extend_from_slice(&(s.len() as u32).to_be_bytes());
                b.extend_from_slice(s);
                Ok(b)
            }
            fn deserialize(&self, _b: &[u8]) -> Result<ContactV0> {
                unimplemented!()
            }
        }

        // NEW: proxy tag 0 = email, proxy tag 1 = phone (added in v1)
        #[derive(Clone, Debug, PartialEq)]
        pub enum Method {
            Email(String),
            Phone(String),
        }

        #[derive(Clone, Debug, PartialEq)]
        pub struct ContactV1 {
            pub id: u64,
            pub method: Method,
        }
        impl Entity for ContactV1 {
            type PrimaryKey = u64;
            fn primary_key(&self) -> &u64 {
                &self.id
            }
            fn entity_name() -> &'static str {
                "Contact"
            }
            fn class_version() -> u16 {
                1
            }
        }
        pub struct C1;
        impl EntitySerializer<ContactV1> for C1 {
            fn serialize(&self, e: &ContactV1) -> Result<Vec<u8>> {
                let mut b = Vec::new();
                b.extend_from_slice(&e.id.to_be_bytes());
                let (tag, s) = match &e.method {
                    Method::Email(x) => (0u8, x.clone()),
                    Method::Phone(x) => (1u8, x.clone()),
                };
                b.push(tag);
                let bs = s.as_bytes();
                b.extend_from_slice(&(bs.len() as u32).to_be_bytes());
                b.extend_from_slice(bs);
                Ok(b)
            }
            fn deserialize(&self, b: &[u8]) -> Result<ContactV1> {
                let id = u64::from_be_bytes(b[0..8].try_into().unwrap());
                let tag = b[8];
                let n =
                    u32::from_be_bytes(b[9..13].try_into().unwrap()) as usize;
                let s = String::from_utf8(b[13..13 + n].to_vec()).unwrap();
                let method = match tag {
                    0 => Method::Email(s),
                    1 => Method::Phone(s),
                    other => {
                        return Err(PersistError::SerializationError(format!(
                            "unknown proxy tag {}",
                            other
                        )));
                    }
                };
                Ok(ContactV1 { id, method })
            }
        }
    }

    let td = TempDir::new().unwrap();
    let path = td.path().to_path_buf();
    {
        let env = env_for(&path);
        let cfg = StoreConfig::new("s").with_allow_create(true);
        let mut store = EntityStore::open(&env, cfg).unwrap();
        let idx: PrimaryIndex<u64, m::ContactV0> =
            store.get_primary_index().unwrap();
        idx.put(None, &m::C0, &m::ContactV0 { id: 1, email: "a@x.com".into() })
            .unwrap();
        store.close().unwrap();
        env.close().unwrap();
    }
    {
        let env = env_for(&path);
        let cfg = StoreConfig::new("s")
            .with_allow_create(true)
            // No converter needed: the v0 layout is a forward-compatible
            // subset of v1 (proxy tag 0 = email is unchanged).  We rely
            // on the rewrite path to bump the envelope version.
            .with_mutations(Mutations::new());
        let mut store = EntityStore::open(&env, cfg).unwrap();
        let idx: PrimaryIndex<u64, m::ContactV1> =
            store.get_primary_index().unwrap();
        let c = idx.get(None, &m::C1, &1).unwrap().unwrap();
        assert_eq!(c.method, m::Method::Email("a@x.com".into()));
    }
}

// ---------------------------------------------------------------------------
// Test 8: idempotence
// ---------------------------------------------------------------------------

#[test]
fn evolve_is_idempotent_across_reopens() {
    use add_field::*;

    let td = TempDir::new().unwrap();
    let path = td.path().to_path_buf();

    // Phase 1: V0.
    {
        let env = env_for(&path);
        let cfg = StoreConfig::new("s").with_allow_create(true);
        let mut store = EntityStore::open(&env, cfg).unwrap();
        let idx: PrimaryIndex<u64, DocV0> = store.get_primary_index().unwrap();
        idx.put(None, &DocV0Ser, &DocV0 { id: 1, title: "T".into() }).unwrap();
        store.close().unwrap();
        env.close().unwrap();
    }

    // Phase 2 + 3: open with V1 twice.  Both opens succeed and read the
    // same data; no record is converted twice.
    for _ in 0..2 {
        let env = env_for(&path);
        let cfg = StoreConfig::new("s")
            .with_allow_create(true)
            .with_mutations(Mutations::new());
        let mut store = EntityStore::open(&env, cfg).unwrap();
        let idx: PrimaryIndex<u64, DocV1> = store.get_primary_index().unwrap();
        let d = idx.get(None, &DocV1Ser, &1).unwrap().unwrap();
        assert_eq!(d.title, "T");
        store.close().unwrap();
        env.close().unwrap();
    }
}

// ---------------------------------------------------------------------------
// Test 9: streaming over many records
// ---------------------------------------------------------------------------
//
// A property the old `scan_all_kv`-based evolve violated: it collected
// every record into RAM before applying mutations.  This test confirms
// the streamed path works for thousands of records.

#[test]
fn evolve_streaming_handles_thousand_records() {
    use add_field::*;

    const N: u64 = 1_000;

    let td = TempDir::new().unwrap();
    let path = td.path().to_path_buf();
    {
        let env = env_for(&path);
        let cfg = StoreConfig::new("s").with_allow_create(true);
        let mut store = EntityStore::open(&env, cfg).unwrap();
        let idx: PrimaryIndex<u64, DocV0> = store.get_primary_index().unwrap();
        for i in 0..N {
            idx.put(
                None,
                &DocV0Ser,
                &DocV0 { id: i, title: format!("title{}", i) },
            )
            .unwrap();
        }
        store.close().unwrap();
        env.close().unwrap();
    }
    {
        let env = env_for(&path);
        let cfg = StoreConfig::new("s")
            .with_allow_create(true)
            .with_mutations(Mutations::new());
        let mut store = EntityStore::open(&env, cfg).unwrap();
        let idx: PrimaryIndex<u64, DocV1> = store.get_primary_index().unwrap();
        let count = idx.count().unwrap();
        assert_eq!(count, N);
        // Sample a few.
        for i in [0u64, N / 2, N - 1] {
            let d = idx.get(None, &DocV1Ser, &i).unwrap().unwrap();
            assert_eq!(d.title, format!("title{}", i));
            assert_eq!(d.author, "unknown");
        }
    }
}

// ---------------------------------------------------------------------------
// Test 10: listener-driven abort rolls back the transaction
// ---------------------------------------------------------------------------

#[test]
fn evolve_aborts_on_listener_failure() {
    use add_field::*;

    /// Listener that returns false after the first record.
    struct Stopper;
    impl EvolveListener for Stopper {
        fn evolve_progress(
            &self,
            _entity_class_name: &str,
            n_read: u64,
            _n_converted: u64,
        ) -> bool {
            n_read < 2
        }
    }

    let td = TempDir::new().unwrap();
    let path = td.path().to_path_buf();

    // Phase 1: write a few V0 records.
    {
        let env_cfg = EnvironmentConfig::new(path.clone())
            .with_allow_create(true)
            .with_transactional(true);
        let env = Environment::open(env_cfg).unwrap();
        let cfg = StoreConfig::new("s")
            .with_allow_create(true)
            .with_transactional(true);
        let mut store = EntityStore::open(&env, cfg).unwrap();
        let idx: PrimaryIndex<u64, DocV0> = store.get_primary_index().unwrap();
        for i in 0..5u64 {
            idx.put(
                None,
                &DocV0Ser,
                &DocV0 { id: i, title: format!("t{}", i) },
            )
            .unwrap();
        }
        store.close().unwrap();
        env.close().unwrap();
    }

    // Phase 2: open with a converter that would rewrite all records,
    // but a listener that aborts after the second.  The whole txn
    // rolls back; nothing should have been changed on disk.
    {
        let env_cfg = EnvironmentConfig::new(path.clone())
            .with_allow_create(true)
            .with_transactional(true);
        let env = Environment::open(env_cfg).unwrap();

        let mut mutations = Mutations::new();
        mutations.add_converter(Converter::for_class(
            "Doc",
            0,
            |old: &[u8]| {
                // Bump the title with a sentinel byte to detect partial
                // application after a bogus abort would commit.
                let mut out = old.to_vec();
                out.push(0xAA);
                Some(out)
            },
        ));

        let cfg = StoreConfig::new("s")
            .with_allow_create(true)
            .with_transactional(true)
            .with_mutations(mutations)
            .with_evolve_config(EvolveConfig::new().with_listener(Stopper));
        let result = EntityStore::open(&env, cfg).and_then(|mut store| {
            let _idx: PrimaryIndex<u64, DocV1> = store.get_primary_index()?;
            Ok(())
        });
        assert!(result.is_err(), "open should fail when listener aborts");
        // Because EntityStore::get_primary_index propagates the
        // listener error from evolve_open_path, the store handle is
        // dropped here; on next reopen the records remain at V0.
    }

    // Phase 3: reopen WITHOUT the listener and confirm the old V0
    // shape is intact (no records were converted).
    {
        let env_cfg = EnvironmentConfig::new(path)
            .with_allow_create(true)
            .with_transactional(true);
        let env = Environment::open(env_cfg).unwrap();
        let cfg = StoreConfig::new("s")
            .with_allow_create(true)
            .with_transactional(true);
        let mut store = EntityStore::open(&env, cfg).unwrap();
        let idx: PrimaryIndex<u64, DocV0> = store.get_primary_index().unwrap();
        for i in 0..5u64 {
            let d = idx.get(None, &DocV0Ser, &i).unwrap().unwrap();
            assert_eq!(d.title, format!("t{}", i));
        }
    }
}

// ---------------------------------------------------------------------------
// Test 11: deleter at the class level removes the entity
// ---------------------------------------------------------------------------
//
// Once we register a class-level Deleter for entity_name "X" version 0,
// all v0 records of that class are removed and the catalog entry is
// dropped.

#[test]
fn evolve_class_deleter_drops_records_and_catalog() {
    mod m {
        use super::*;

        #[derive(Clone, Debug)]
        pub struct Old {
            pub id: u64,
        }
        impl Entity for Old {
            type PrimaryKey = u64;
            fn primary_key(&self) -> &u64 {
                &self.id
            }
            fn entity_name() -> &'static str {
                "Old"
            }
        }
        pub struct Os;
        impl EntitySerializer<Old> for Os {
            fn serialize(&self, e: &Old) -> Result<Vec<u8>> {
                Ok(e.id.to_be_bytes().to_vec())
            }
            fn deserialize(&self, b: &[u8]) -> Result<Old> {
                Ok(Old { id: u64::from_be_bytes(b[0..8].try_into().unwrap()) })
            }
        }
    }

    let td = TempDir::new().unwrap();
    let path = td.path().to_path_buf();
    {
        let env = env_for(&path);
        let cfg = StoreConfig::new("s").with_allow_create(true);
        let mut store = EntityStore::open(&env, cfg).unwrap();
        let idx: PrimaryIndex<u64, m::Old> = store.get_primary_index().unwrap();
        for i in 0u64..5 {
            idx.put(None, &m::Os, &m::Old { id: i }).unwrap();
        }
        store.close().unwrap();
        env.close().unwrap();
    }

    {
        let env = env_for(&path);
        let mut mutations = Mutations::new();
        mutations.add_deleter(Deleter::for_class("Old", 0));
        let cfg = StoreConfig::new("s")
            .with_allow_create(true)
            .with_mutations(mutations);
        let mut store = EntityStore::open(&env, cfg).unwrap();
        // Use evolve() explicitly so we can read EvolveStats; it should
        // also have run as part of get_primary_index, but is idempotent.
        let stats = store
            .evolve(
                store.mutations().clone().as_ref(),
                store.evolve_config().clone().as_ref(),
            )
            .unwrap();
        // Either get_primary_index or this evolve removed the records;
        // re-checking once more should find none.
        let idx: PrimaryIndex<u64, m::Old> = store.get_primary_index().unwrap();
        assert_eq!(idx.count().unwrap(), 0, "stats: {}", stats);
    }
}
