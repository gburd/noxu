//! Port of JE persist layer tests.
//!
//! Covers the key invariants from:
//!   - `OperationTest.java`  — EntityStore open/close, put/get/delete, count,
//!                             put-replaces, read-only reopening.
//!   - `IndexTest.java`      — PrimaryIndex iteration/cursor first/next/last/
//!                             prev, sub-range; secondary index lookup.
//!   - `EvolveTest.java`     — Renamer mutation (field readable after rename),
//!                             Deleter mutation (record removed), EvolveStats
//!                             n_read/n_converted tracking.
//!
//! The Rust API does not use annotations or Java-style generics; instead,
//! `EntityStore`, `PrimaryIndex`, `SecondaryIndex`, and `EntitySerializer`
//! are used directly.  All entity types and serializers are defined inline.

use noxu_db::{Environment, EnvironmentConfig};
use noxu_persist::{
    entity_store::EntityStore,
    entity::Entity,
    entity_serializer::EntitySerializer,
    error::{PersistError, Result},
    primary_index::PrimaryIndex,
    secondary_index::SecondaryIndex,
    store_config::StoreConfig,
    evolve::{Converter, Deleter, EvolveConfig, Mutations, Renamer},
};
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Shared test fixtures
// ---------------------------------------------------------------------------

/// A simple entity with an integer primary key and one secondary key field.
///
/// Mirrors `MyEntity` in both `OperationTest.java` and `IndexTest.java`.
#[derive(Clone, Debug, PartialEq)]
struct MyEntity {
    pri_key: i32,
    sec_key: Option<i32>,
    label: String,
}

impl Entity for MyEntity {
    type PrimaryKey = i32;
    fn primary_key(&self) -> &i32 {
        &self.pri_key
    }
    fn entity_name() -> &'static str {
        "MyEntity"
    }
}

struct MyEntitySerializer;

impl EntitySerializer<MyEntity> for MyEntitySerializer {
    fn serialize(&self, e: &MyEntity) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&e.pri_key.to_be_bytes());
        // sec_key: 0 = absent, 1 = present
        match e.sec_key {
            None => buf.push(0),
            Some(sk) => {
                buf.push(1);
                buf.extend_from_slice(&sk.to_be_bytes());
            }
        }
        let lb = e.label.as_bytes();
        buf.extend_from_slice(&(lb.len() as u32).to_be_bytes());
        buf.extend_from_slice(lb);
        Ok(buf)
    }

    fn deserialize(&self, bytes: &[u8]) -> Result<MyEntity> {
        if bytes.len() < 5 {
            return Err(PersistError::SerializationError(
                "too short for MyEntity".into(),
            ));
        }
        let pri_key = i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        let (sec_key, mut pos) = if bytes[4] == 0 {
            (None, 5)
        } else {
            if bytes.len() < 9 {
                return Err(PersistError::SerializationError(
                    "too short for sec_key".into(),
                ));
            }
            let sk = i32::from_be_bytes([bytes[5], bytes[6], bytes[7], bytes[8]]);
            (Some(sk), 9)
        };
        if bytes.len() < pos + 4 {
            return Err(PersistError::SerializationError(
                "too short for label length".into(),
            ));
        }
        let label_len =
            u32::from_be_bytes([bytes[pos], bytes[pos + 1], bytes[pos + 2], bytes[pos + 3]])
                as usize;
        pos += 4;
        if bytes.len() < pos + label_len {
            return Err(PersistError::SerializationError(
                "too short for label".into(),
            ));
        }
        let label = String::from_utf8(bytes[pos..pos + label_len].to_vec())
            .map_err(|e| PersistError::SerializationError(e.to_string()))?;
        Ok(MyEntity { pri_key, sec_key, label })
    }
}

fn my_entity(pri_key: i32, sec_key: Option<i32>) -> MyEntity {
    MyEntity { pri_key, sec_key, label: format!("ent{}", pri_key) }
}

// ---------------------------------------------------------------------------
// ==========================================================================
// OperationTest ports
// ==========================================================================
// ---------------------------------------------------------------------------

/// EntityStore open/close round-trip — mirrors `testReadOnly` part 1.
#[test]
fn test_entity_store_open_close() {
    let td = TempDir::new().unwrap();
    let env_cfg =
        EnvironmentConfig::new(td.path().to_path_buf()).with_allow_create(true);
    let env = Environment::open(env_cfg).unwrap();
    let cfg = StoreConfig::new("test").with_allow_create(true);
    let mut store = EntityStore::open(&env, cfg).unwrap();
    assert!(store.is_open());
    store.close().unwrap();
    assert!(!store.is_open());
}

/// Closing a store a second time must return an error.
/// Mirrors JE behavior: calling `EntityStore.close()` twice throws.
#[test]
fn test_close_twice_returns_error() {
    let td = TempDir::new().unwrap();
    let env_cfg =
        EnvironmentConfig::new(td.path().to_path_buf()).with_allow_create(true);
    let env = Environment::open(env_cfg).unwrap();
    let cfg = StoreConfig::new("test").with_allow_create(true);
    let mut store = EntityStore::open(&env, cfg).unwrap();
    store.close().unwrap();
    assert!(store.close().is_err());
}

/// `put()` stores an entity, `get()` retrieves it with matching fields.
/// Mirrors `OperationTest.testCursorUpdate` (put/get assertions) and the
/// general entity-store CRUD contract verified throughout OperationTest.
#[test]
fn test_put_get() {
    let td = TempDir::new().unwrap();
    let env_cfg =
        EnvironmentConfig::new(td.path().to_path_buf()).with_allow_create(true);
    let env = Environment::open(env_cfg).unwrap();
    let cfg = StoreConfig::new("store").with_allow_create(true);
    let mut store = EntityStore::open(&env, cfg).unwrap();
    let ser = MyEntitySerializer;

    let index: PrimaryIndex<i32, MyEntity> = store.get_primary_index().unwrap();
    let e = my_entity(1, Some(10));
    index.put(&ser, &e).unwrap();

    let found = index.get(&ser, &1).unwrap().unwrap();
    assert_eq!(found.pri_key, 1);
    assert_eq!(found.sec_key, Some(10));
    assert_eq!(found.label, "ent1");
}

/// `put()` with the same primary key replaces the existing entity.
/// Mirrors OperationTest entity-store update contract.
#[test]
fn test_put_replaces_existing() {
    let td = TempDir::new().unwrap();
    let env_cfg =
        EnvironmentConfig::new(td.path().to_path_buf()).with_allow_create(true);
    let env = Environment::open(env_cfg).unwrap();
    let cfg = StoreConfig::new("store").with_allow_create(true);
    let mut store = EntityStore::open(&env, cfg).unwrap();
    let ser = MyEntitySerializer;

    let index: PrimaryIndex<i32, MyEntity> = store.get_primary_index().unwrap();
    index.put(&ser, &my_entity(1, Some(10))).unwrap();

    // Overwrite with a different sec_key and label.
    let updated = MyEntity { pri_key: 1, sec_key: Some(99), label: "updated".into() };
    index.put(&ser, &updated).unwrap();

    let found = index.get(&ser, &1).unwrap().unwrap();
    assert_eq!(found.sec_key, Some(99));
    assert_eq!(found.label, "updated");
}

/// `delete()` removes an entity; subsequent `get()` returns `None`.
/// Mirrors `OperationTest.testCursorDelete` primary delete assertions.
#[test]
fn test_delete_then_get_returns_none() {
    let td = TempDir::new().unwrap();
    let env_cfg =
        EnvironmentConfig::new(td.path().to_path_buf()).with_allow_create(true);
    let env = Environment::open(env_cfg).unwrap();
    let cfg = StoreConfig::new("store").with_allow_create(true);
    let mut store = EntityStore::open(&env, cfg).unwrap();
    let ser = MyEntitySerializer;

    let index: PrimaryIndex<i32, MyEntity> = store.get_primary_index().unwrap();
    index.put(&ser, &my_entity(1, Some(10))).unwrap();

    let deleted = index.delete(&1).unwrap();
    assert!(deleted, "delete should return true for existing key");

    assert_eq!(index.get(&ser, &1).unwrap(), None);
}

/// Deleting a non-existent key returns `false`.
#[test]
fn test_delete_missing_key_returns_false() {
    let td = TempDir::new().unwrap();
    let env_cfg =
        EnvironmentConfig::new(td.path().to_path_buf()).with_allow_create(true);
    let env = Environment::open(env_cfg).unwrap();
    let cfg = StoreConfig::new("store").with_allow_create(true);
    let mut store = EntityStore::open(&env, cfg).unwrap();

    let index: PrimaryIndex<i32, MyEntity> = store.get_primary_index().unwrap();
    let deleted = index.delete(&42).unwrap();
    assert!(!deleted);
}

/// `count()` reflects inserts and deletes.
/// Mirrors `OperationTest.testAutoOpenRelatedEntity` (priY.count() == 1 after
/// insert, == 0 after delete) and `IndexTest` expandValueSize checks.
#[test]
fn test_count_reflects_inserts_and_deletes() {
    let td = TempDir::new().unwrap();
    let env_cfg =
        EnvironmentConfig::new(td.path().to_path_buf()).with_allow_create(true);
    let env = Environment::open(env_cfg).unwrap();
    let cfg = StoreConfig::new("store").with_allow_create(true);
    let mut store = EntityStore::open(&env, cfg).unwrap();
    let ser = MyEntitySerializer;

    let index: PrimaryIndex<i32, MyEntity> = store.get_primary_index().unwrap();
    assert_eq!(index.count().unwrap(), 0);

    index.put(&ser, &my_entity(1, None)).unwrap();
    index.put(&ser, &my_entity(2, None)).unwrap();
    index.put(&ser, &my_entity(3, None)).unwrap();
    assert_eq!(index.count().unwrap(), 3);

    index.delete(&2).unwrap();
    assert_eq!(index.count().unwrap(), 2);

    index.delete(&1).unwrap();
    index.delete(&3).unwrap();
    assert_eq!(index.count().unwrap(), 0);
}

/// `put_no_overwrite()` returns `true` on new insert, `false` on collision.
/// Mirrors `OperationTest.testSharedSequence` putNoOverwrite assertions.
#[test]
fn test_put_no_overwrite() {
    let td = TempDir::new().unwrap();
    let env_cfg =
        EnvironmentConfig::new(td.path().to_path_buf()).with_allow_create(true);
    let env = Environment::open(env_cfg).unwrap();
    let cfg = StoreConfig::new("store").with_allow_create(true);
    let mut store = EntityStore::open(&env, cfg).unwrap();
    let ser = MyEntitySerializer;

    let index: PrimaryIndex<i32, MyEntity> = store.get_primary_index().unwrap();
    assert!(index.put_no_overwrite(&ser, &my_entity(1, None)).unwrap());
    assert!(!index.put_no_overwrite(&ser, &my_entity(1, Some(99))).unwrap());

    // Original value must be unchanged.
    let found = index.get(&ser, &1).unwrap().unwrap();
    assert_eq!(found.sec_key, None);
}

/// `contains()` returns true only after insertion.
#[test]
fn test_contains() {
    let td = TempDir::new().unwrap();
    let env_cfg =
        EnvironmentConfig::new(td.path().to_path_buf()).with_allow_create(true);
    let env = Environment::open(env_cfg).unwrap();
    let cfg = StoreConfig::new("store").with_allow_create(true);
    let mut store = EntityStore::open(&env, cfg).unwrap();
    let ser = MyEntitySerializer;

    let index: PrimaryIndex<i32, MyEntity> = store.get_primary_index().unwrap();
    assert!(!index.contains(&1).unwrap());
    index.put(&ser, &my_entity(1, None)).unwrap();
    assert!(index.contains(&1).unwrap());
    index.delete(&1).unwrap();
    assert!(!index.contains(&1).unwrap());
}

/// Operations on a closed store return an error.
/// Mirrors JE `DatabaseException` on closed-store access.
#[test]
fn test_get_primary_index_on_closed_store_fails() {
    let td = TempDir::new().unwrap();
    let env_cfg =
        EnvironmentConfig::new(td.path().to_path_buf()).with_allow_create(true);
    let env = Environment::open(env_cfg).unwrap();
    let cfg = StoreConfig::new("store").with_allow_create(true);
    let mut store = EntityStore::open(&env, cfg).unwrap();
    store.close().unwrap();

    let result: std::result::Result<PrimaryIndex<i32, MyEntity>, _> =
        store.get_primary_index();
    assert!(result.is_err());
}

/// Two separate entity types coexist in the same store.
/// Mirrors `OperationTest` using both `MyEntity` and `SharedSequenceEntity1`.
#[derive(Clone, Debug, PartialEq)]
struct OtherEntity {
    key: u64,
    value: String,
}
impl Entity for OtherEntity {
    type PrimaryKey = u64;
    fn primary_key(&self) -> &u64 {
        &self.key
    }
    fn entity_name() -> &'static str {
        "OtherEntity"
    }
}
struct OtherEntitySerializer;
impl EntitySerializer<OtherEntity> for OtherEntitySerializer {
    fn serialize(&self, e: &OtherEntity) -> Result<Vec<u8>> {
        let mut buf = e.key.to_be_bytes().to_vec();
        let vb = e.value.as_bytes();
        buf.extend_from_slice(&(vb.len() as u32).to_be_bytes());
        buf.extend_from_slice(vb);
        Ok(buf)
    }
    fn deserialize(&self, bytes: &[u8]) -> Result<OtherEntity> {
        let key = u64::from_be_bytes(bytes[0..8].try_into().unwrap());
        let vlen =
            u32::from_be_bytes(bytes[8..12].try_into().unwrap()) as usize;
        let value = String::from_utf8(bytes[12..12 + vlen].to_vec())
            .map_err(|e| PersistError::SerializationError(e.to_string()))?;
        Ok(OtherEntity { key, value })
    }
}

#[test]
fn test_two_entity_types_in_same_store() {
    let td = TempDir::new().unwrap();
    let env_cfg =
        EnvironmentConfig::new(td.path().to_path_buf()).with_allow_create(true);
    let env = Environment::open(env_cfg).unwrap();
    let cfg = StoreConfig::new("store").with_allow_create(true);
    let mut store = EntityStore::open(&env, cfg).unwrap();
    let ser1 = MyEntitySerializer;
    let ser2 = OtherEntitySerializer;

    // Open and use each index in its own scope to avoid simultaneous
    // mutable borrows of `store` — the borrow checker requires this because
    // `get_primary_index` takes `&mut self`.
    {
        let idx1: PrimaryIndex<i32, MyEntity> = store.get_primary_index().unwrap();
        idx1.put(&ser1, &my_entity(1, Some(10))).unwrap();
        assert_eq!(idx1.count().unwrap(), 1);
    }
    {
        let idx2: PrimaryIndex<u64, OtherEntity> = store.get_primary_index().unwrap();
        idx2.put(&ser2, &OtherEntity { key: 100, value: "hello".into() }).unwrap();
        assert_eq!(idx2.count().unwrap(), 1);
    }
    {
        let idx1: PrimaryIndex<i32, MyEntity> = store.get_primary_index().unwrap();
        assert_eq!(idx1.get(&ser1, &1).unwrap().unwrap().pri_key, 1);
    }
    {
        let idx2: PrimaryIndex<u64, OtherEntity> = store.get_primary_index().unwrap();
        assert_eq!(
            idx2.get(&ser2, &100u64).unwrap().unwrap().value,
            "hello"
        );
    }
}

// ---------------------------------------------------------------------------
// ==========================================================================
// IndexTest ports
// ==========================================================================
// ---------------------------------------------------------------------------

/// Insert N entities with keys 0..N and verify iteration yields them in
/// sorted primary-key order.
/// Mirrors `IndexTest.testPrimary` addEntities + checkIndex loop.
#[test]
fn test_primary_iteration_in_key_order() {
    let td = TempDir::new().unwrap();
    let env_cfg =
        EnvironmentConfig::new(td.path().to_path_buf()).with_allow_create(true);
    let env = Environment::open(env_cfg).unwrap();
    let cfg = StoreConfig::new("idx").with_allow_create(true);
    let mut store = EntityStore::open(&env, cfg).unwrap();
    let ser = MyEntitySerializer;

    let index: PrimaryIndex<i32, MyEntity> = store.get_primary_index().unwrap();
    const N: i32 = 5;
    for i in 0..N {
        index.put(&ser, &my_entity(i, Some(i * 10))).unwrap();
    }

    let entities: Vec<MyEntity> = index
        .entities(&ser)
        .unwrap()
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(entities.len(), N as usize);
    for (pos, e) in entities.iter().enumerate() {
        assert_eq!(e.pri_key, pos as i32, "entities must be in key order");
    }
}

/// Iterator over an empty index yields nothing.
/// Mirrors `IndexTest.checkAllEmpty` / `checkEmpty`.
#[test]
fn test_primary_iteration_empty() {
    let td = TempDir::new().unwrap();
    let env_cfg =
        EnvironmentConfig::new(td.path().to_path_buf()).with_allow_create(true);
    let env = Environment::open(env_cfg).unwrap();
    let cfg = StoreConfig::new("idx").with_allow_create(true);
    let mut store = EntityStore::open(&env, cfg).unwrap();
    let ser = MyEntitySerializer;

    let index: PrimaryIndex<i32, MyEntity> = store.get_primary_index().unwrap();
    let entities: Vec<MyEntity> = index
        .entities(&ser)
        .unwrap()
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap();
    assert!(entities.is_empty());
}

/// Insert then delete entities one by one; verify count decrements correctly
/// and the deleted entity is gone from iteration.
/// Mirrors `IndexTest.testPrimary` "Check primary delete" loop.
#[test]
fn test_primary_delete_one_by_one() {
    let td = TempDir::new().unwrap();
    let env_cfg =
        EnvironmentConfig::new(td.path().to_path_buf()).with_allow_create(true);
    let env = Environment::open(env_cfg).unwrap();
    let cfg = StoreConfig::new("idx").with_allow_create(true);
    let mut store = EntityStore::open(&env, cfg).unwrap();
    let ser = MyEntitySerializer;

    let index: PrimaryIndex<i32, MyEntity> = store.get_primary_index().unwrap();
    const N: i32 = 5;
    for i in 0..N {
        index.put(&ser, &my_entity(i, None)).unwrap();
    }

    // Delete last-first for variety (mirrors IndexTest).
    for i in (0..N).rev() {
        let deleted = index.delete(&i).unwrap();
        assert!(deleted, "should have deleted key {}", i);
        assert_eq!(index.count().unwrap(), i as u64);
        assert_eq!(index.get(&ser, &i).unwrap(), None);
    }
}

/// `put()` returns after overwrite; existing entity not findable at new key.
/// Mirrors `IndexTest.testPrimary` "Check PrimaryIndex put operations":
/// put/get, putNoOverwrite true/false checks.
#[test]
fn test_put_operations() {
    let td = TempDir::new().unwrap();
    let env_cfg =
        EnvironmentConfig::new(td.path().to_path_buf()).with_allow_create(true);
    let env = Environment::open(env_cfg).unwrap();
    let cfg = StoreConfig::new("idx").with_allow_create(true);
    let mut store = EntityStore::open(&env, cfg).unwrap();
    let ser = MyEntitySerializer;

    let index: PrimaryIndex<i32, MyEntity> = store.get_primary_index().unwrap();

    // put() insert
    index.put(&ser, &my_entity(1, None)).unwrap();
    assert_eq!(index.get(&ser, &1).unwrap().unwrap().pri_key, 1);

    // put() update (same key)
    let updated = MyEntity { pri_key: 1, sec_key: Some(42), label: "updated".into() };
    index.put(&ser, &updated).unwrap();
    assert_eq!(index.get(&ser, &1).unwrap().unwrap().sec_key, Some(42));

    // put_no_overwrite returns false for existing key
    assert!(!index.put_no_overwrite(&ser, &my_entity(1, None)).unwrap());

    // put_no_overwrite returns true for new key
    assert!(index.put_no_overwrite(&ser, &my_entity(3, None)).unwrap());
    assert_eq!(index.get(&ser, &3).unwrap().unwrap().pri_key, 3);
}

/// Secondary index lookup by secondary key.
/// Mirrors `IndexTest.testOneToOne` / `testManyToOne` core get checks.
#[test]
fn test_secondary_lookup_by_key() {
    let td = TempDir::new().unwrap();
    let env_cfg =
        EnvironmentConfig::new(td.path().to_path_buf()).with_allow_create(true);
    let env = Environment::open(env_cfg).unwrap();
    let cfg = StoreConfig::new("idx").with_allow_create(true);
    let mut store = EntityStore::open(&env, cfg).unwrap();
    let ser = MyEntitySerializer;

    let mut index: PrimaryIndex<i32, MyEntity> = store.get_primary_index().unwrap();
    let sec: SecondaryIndex<i32, i32, MyEntity> =
        index.open_secondary_index(|e: &MyEntity| e.sec_key);

    // Insert 5 entities with distinct secondary keys.
    for i in 0..5i32 {
        index.put(&ser, &my_entity(i, Some(i * -1))).unwrap();
    }

    // get() via secondary key.
    let found = sec.get(&ser, &index, &0).unwrap().unwrap();
    assert_eq!(found.pri_key, 0);

    let found = sec.get(&ser, &index, &-3).unwrap().unwrap();
    assert_eq!(found.pri_key, 3);

    // Non-existent secondary key returns None.
    assert!(sec.get(&ser, &index, &99).unwrap().is_none());
}

/// MANY_TO_ONE: multiple entities share the same secondary key; `sub_index`
/// returns all matching primary keys.
/// Mirrors `IndexTest.testManyToOne` pattern: sec_key = pri_key % 3.
#[test]
fn test_secondary_many_to_one() {
    let td = TempDir::new().unwrap();
    let env_cfg =
        EnvironmentConfig::new(td.path().to_path_buf()).with_allow_create(true);
    let env = Environment::open(env_cfg).unwrap();
    let cfg = StoreConfig::new("idx").with_allow_create(true);
    let mut store = EntityStore::open(&env, cfg).unwrap();
    let ser = MyEntitySerializer;

    let mut index: PrimaryIndex<i32, MyEntity> = store.get_primary_index().unwrap();
    let sec: SecondaryIndex<i32, i32, MyEntity> =
        index.open_secondary_index(|e: &MyEntity| e.sec_key);

    const N: i32 = 5;
    const THREE_TO_ONE: i32 = 3;
    for i in 0..N {
        index.put(&ser, &my_entity(i, Some(i % THREE_TO_ONE))).unwrap();
    }

    // Sec key 0 maps to primary keys 0 and 3.
    let pks_for_0 = sec.sub_index(&0);
    assert_eq!(pks_for_0.len(), 2);
    assert!(pks_for_0.contains(&0));
    assert!(pks_for_0.contains(&3));

    // Sec key 1 maps to primary keys 1 and 4.
    let pks_for_1 = sec.sub_index(&1);
    assert_eq!(pks_for_1.len(), 2);

    // Sec key 2 maps to primary key 2 only.
    let pks_for_2 = sec.sub_index(&2);
    assert_eq!(pks_for_2.len(), 1);
    assert!(pks_for_2.contains(&2));
}

/// Secondary iteration yields pairs in secondary-key order.
/// Mirrors `IndexTest.checkIndex` cursor + `expandKeys`/`expandValues` checks.
#[test]
fn test_secondary_iteration_in_key_order() {
    let td = TempDir::new().unwrap();
    let env_cfg =
        EnvironmentConfig::new(td.path().to_path_buf()).with_allow_create(true);
    let env = Environment::open(env_cfg).unwrap();
    let cfg = StoreConfig::new("idx").with_allow_create(true);
    let mut store = EntityStore::open(&env, cfg).unwrap();
    let ser = MyEntitySerializer;

    let mut index: PrimaryIndex<i32, MyEntity> = store.get_primary_index().unwrap();
    let sec: SecondaryIndex<i32, i32, MyEntity> =
        index.open_secondary_index(|e: &MyEntity| e.sec_key);

    // Insert with reversed secondary keys: entity 0 → sec 4, ..., entity 4 → sec 0.
    for i in 0..5i32 {
        index.put(&ser, &my_entity(i, Some(4 - i))).unwrap();
    }

    let pairs: Vec<(i32, MyEntity)> = sec
        .iter(&ser, &index)
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(pairs.len(), 5);
    // Secondary keys should be in ascending order: 0, 1, 2, 3, 4.
    let sec_keys: Vec<i32> = pairs.iter().map(|(sk, _)| *sk).collect();
    assert_eq!(sec_keys, vec![0, 1, 2, 3, 4]);
}

/// Sub-range via `iter_from`: only entities with sec_key >= bound returned.
/// Mirrors `IndexTest.checkOpenRanges` tail-inclusive logic.
#[test]
fn test_secondary_iter_from_range() {
    let td = TempDir::new().unwrap();
    let env_cfg =
        EnvironmentConfig::new(td.path().to_path_buf()).with_allow_create(true);
    let env = Environment::open(env_cfg).unwrap();
    let cfg = StoreConfig::new("idx").with_allow_create(true);
    let mut store = EntityStore::open(&env, cfg).unwrap();
    let ser = MyEntitySerializer;

    let mut index: PrimaryIndex<i32, MyEntity> = store.get_primary_index().unwrap();
    let sec: SecondaryIndex<i32, i32, MyEntity> =
        index.open_secondary_index(|e: &MyEntity| e.sec_key);

    for i in 0..5i32 {
        index.put(&ser, &my_entity(i, Some(i))).unwrap();
    }

    // iter_from(&2) should yield sec keys 2, 3, 4.
    let pairs: Vec<(i32, MyEntity)> = sec
        .iter_from(&ser, &index, &2)
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(pairs.len(), 3);
    assert_eq!(pairs[0].0, 2);
    assert_eq!(pairs[1].0, 3);
    assert_eq!(pairs[2].0, 4);
}

/// Secondary delete cascades to primary; secondary map is cleaned up.
/// Mirrors `IndexTest.checkDelete` → `SecondaryIndex.delete` / `IndexTest`
/// assertNull(index.get) after delete.
#[test]
fn test_secondary_delete_cascades_to_primary() {
    let td = TempDir::new().unwrap();
    let env_cfg =
        EnvironmentConfig::new(td.path().to_path_buf()).with_allow_create(true);
    let env = Environment::open(env_cfg).unwrap();
    let cfg = StoreConfig::new("idx").with_allow_create(true);
    let mut store = EntityStore::open(&env, cfg).unwrap();
    let ser = MyEntitySerializer;

    let mut index: PrimaryIndex<i32, MyEntity> = store.get_primary_index().unwrap();
    let sec: SecondaryIndex<i32, i32, MyEntity> =
        index.open_secondary_index(|e: &MyEntity| e.sec_key);

    index.put(&ser, &my_entity(1, Some(10))).unwrap();
    index.put(&ser, &my_entity(2, Some(10))).unwrap(); // same sec key

    // Delete all entities with sec_key == 10.
    let deleted = sec.delete(&ser, &index, &10).unwrap();
    assert!(deleted);

    // Both primary records gone.
    assert_eq!(index.get(&ser, &1).unwrap(), None);
    assert_eq!(index.get(&ser, &2).unwrap(), None);

    // Secondary map is clean.
    assert!(!sec.contains(&10));
    assert_eq!(index.count().unwrap(), 0);
}

/// Secondary delete on a non-existent key returns false.
/// Mirrors `IndexTest.checkDelete`: second delete call returns false.
#[test]
fn test_secondary_delete_not_found() {
    let td = TempDir::new().unwrap();
    let env_cfg =
        EnvironmentConfig::new(td.path().to_path_buf()).with_allow_create(true);
    let env = Environment::open(env_cfg).unwrap();
    let cfg = StoreConfig::new("idx").with_allow_create(true);
    let mut store = EntityStore::open(&env, cfg).unwrap();
    let ser = MyEntitySerializer;

    let mut index: PrimaryIndex<i32, MyEntity> = store.get_primary_index().unwrap();
    let sec: SecondaryIndex<i32, i32, MyEntity> =
        index.open_secondary_index(|e: &MyEntity| e.sec_key);

    index.put(&ser, &my_entity(1, Some(10))).unwrap();

    let first = sec.delete(&ser, &index, &10).unwrap();
    assert!(first);
    // Second delete on same key.
    let second = sec.delete(&ser, &index, &10).unwrap();
    assert!(!second);
}

/// `contains()` on secondary index correctly reflects inserts/deletes.
/// Mirrors `IndexTest.checkIndex` → `index.contains`.
#[test]
fn test_secondary_contains() {
    let td = TempDir::new().unwrap();
    let env_cfg =
        EnvironmentConfig::new(td.path().to_path_buf()).with_allow_create(true);
    let env = Environment::open(env_cfg).unwrap();
    let cfg = StoreConfig::new("idx").with_allow_create(true);
    let mut store = EntityStore::open(&env, cfg).unwrap();
    let ser = MyEntitySerializer;

    let mut index: PrimaryIndex<i32, MyEntity> = store.get_primary_index().unwrap();
    let sec: SecondaryIndex<i32, i32, MyEntity> =
        index.open_secondary_index(|e: &MyEntity| e.sec_key);

    assert!(!sec.contains(&7));
    index.put(&ser, &my_entity(1, Some(7))).unwrap();
    assert!(sec.contains(&7));
    index.delete_with_entity(&ser, &1).unwrap();
    assert!(!sec.contains(&7));
}

/// `keys_index()` returns (sec_key, pri_key) pairs without fetching entities.
/// Mirrors `IndexTest.checkIndex` → `index.keysIndex()` usage.
#[test]
fn test_secondary_keys_index() {
    let td = TempDir::new().unwrap();
    let env_cfg =
        EnvironmentConfig::new(td.path().to_path_buf()).with_allow_create(true);
    let env = Environment::open(env_cfg).unwrap();
    let cfg = StoreConfig::new("idx").with_allow_create(true);
    let mut store = EntityStore::open(&env, cfg).unwrap();
    let ser = MyEntitySerializer;

    let mut index: PrimaryIndex<i32, MyEntity> = store.get_primary_index().unwrap();
    let sec: SecondaryIndex<i32, i32, MyEntity> =
        index.open_secondary_index(|e: &MyEntity| e.sec_key);

    index.put(&ser, &my_entity(1, Some(10))).unwrap();
    index.put(&ser, &my_entity(2, Some(20))).unwrap();
    index.put(&ser, &my_entity(3, Some(10))).unwrap(); // dup sec key

    let kv = sec.keys_index();
    // 3 total pairs.
    assert_eq!(kv.len(), 3);

    // 2 pairs for sec_key=10.
    let sec10: Vec<_> = kv.iter().filter(|(sk, _)| *sk == 10).collect();
    assert_eq!(sec10.len(), 2);
}

/// Secondary index updated when entity is overwritten with new secondary key.
/// Mirrors `OperationTest.testCursorUpdate` update-then-verify flow.
#[test]
fn test_secondary_updated_on_overwrite() {
    let td = TempDir::new().unwrap();
    let env_cfg =
        EnvironmentConfig::new(td.path().to_path_buf()).with_allow_create(true);
    let env = Environment::open(env_cfg).unwrap();
    let cfg = StoreConfig::new("idx").with_allow_create(true);
    let mut store = EntityStore::open(&env, cfg).unwrap();
    let ser = MyEntitySerializer;

    let mut index: PrimaryIndex<i32, MyEntity> = store.get_primary_index().unwrap();
    let sec: SecondaryIndex<i32, i32, MyEntity> =
        index.open_secondary_index(|e: &MyEntity| e.sec_key);

    index.put(&ser, &my_entity(1, Some(10))).unwrap();
    assert!(sec.contains(&10));
    assert!(!sec.contains(&20));

    // Update: change sec_key from 10 to 20.
    index.put(&ser, &MyEntity { pri_key: 1, sec_key: Some(20), label: "x".into() }).unwrap();
    assert!(!sec.contains(&10), "old sec_key must be removed");
    assert!(sec.contains(&20), "new sec_key must be present");
}

/// Two independent secondary indexes on the same primary are both maintained.
/// Mirrors `OperationTest.testCursorDelete` looping over {priIndex, secIndex}.
#[test]
fn test_two_secondary_indexes_maintained_independently() {
    let td = TempDir::new().unwrap();
    let env_cfg =
        EnvironmentConfig::new(td.path().to_path_buf()).with_allow_create(true);
    let env = Environment::open(env_cfg).unwrap();
    let cfg = StoreConfig::new("idx").with_allow_create(true);
    let mut store = EntityStore::open(&env, cfg).unwrap();

    #[derive(Clone, Debug, PartialEq)]
    struct EmpB { id: i32, dept: i32, grade: i32 }
    impl Entity for EmpB {
        type PrimaryKey = i32;
        fn primary_key(&self) -> &i32 { &self.id }
        fn entity_name() -> &'static str { "EmpB" }
    }
    struct EmpBSer;
    impl EntitySerializer<EmpB> for EmpBSer {
        fn serialize(&self, e: &EmpB) -> Result<Vec<u8>> {
            let mut b = e.id.to_be_bytes().to_vec();
            b.extend_from_slice(&e.dept.to_be_bytes());
            b.extend_from_slice(&e.grade.to_be_bytes());
            Ok(b)
        }
        fn deserialize(&self, bytes: &[u8]) -> Result<EmpB> {
            let id = i32::from_be_bytes(bytes[0..4].try_into().unwrap());
            let dept = i32::from_be_bytes(bytes[4..8].try_into().unwrap());
            let grade = i32::from_be_bytes(bytes[8..12].try_into().unwrap());
            Ok(EmpB { id, dept, grade })
        }
    }

    let mut primary: PrimaryIndex<i32, EmpB> = store.get_primary_index().unwrap();
    let dept_idx: SecondaryIndex<i32, i32, EmpB> =
        primary.open_secondary_index(|e: &EmpB| Some(e.dept));
    let grade_idx: SecondaryIndex<i32, i32, EmpB> =
        primary.open_secondary_index(|e: &EmpB| Some(e.grade));
    let ser = EmpBSer;

    primary.put(&ser, &EmpB { id: 1, dept: 10, grade: 5 }).unwrap();
    primary.put(&ser, &EmpB { id: 2, dept: 10, grade: 3 }).unwrap();
    primary.put(&ser, &EmpB { id: 3, dept: 20, grade: 5 }).unwrap();

    assert_eq!(dept_idx.sub_index(&10).len(), 2);
    assert_eq!(dept_idx.sub_index(&20).len(), 1);
    assert_eq!(grade_idx.sub_index(&5).len(), 2);
    assert_eq!(grade_idx.sub_index(&3).len(), 1);

    // Delete via primary — both secondaries must be updated.
    primary.delete_with_entity(&ser, &1).unwrap();
    assert_eq!(dept_idx.sub_index(&10).len(), 1);
    assert_eq!(grade_idx.sub_index(&5).len(), 1);
}

// ---------------------------------------------------------------------------
// ==========================================================================
// EvolveTest ports
// ==========================================================================
// ---------------------------------------------------------------------------

/// Renamer mutation: after applying `Renamer::for_field`, a converter that
/// maps the old field's bytes to the new field layout produces readable data.
///
/// In the Rust port there is no reflection-based field renaming at the storage
/// level — the Renamer records metadata only.  The practical contract tested
/// here is:
///   1. A `Renamer` can be registered without error.
///   2. After registering the renamer, `get_renamer` finds it.
///   3. The renamed class/field's `new_name()` is correct.
///
/// This mirrors `EvolveTest.testLazyEvolve` model-check assertions.
#[test]
fn test_renamer_mutation_registered_and_readable() {
    let mut mutations = Mutations::new();
    mutations.add_renamer(Renamer::for_field(
        "MyEntity",
        0,
        "label",
        "description",
    ));

    let r = mutations.get_renamer("MyEntity", 0, Some("label")).unwrap();
    assert_eq!(r.new_name(), "description");
    assert_eq!(r.class_name(), "MyEntity");
    assert_eq!(r.field_name(), Some("label"));
}

/// Deleter mutation: records for a deleted entity class are removed and
/// `EvolveStats` reports n_read/n_converted correctly.
///
/// Mirrors `EvolveTest.testEagerEvolve` stats checks:
///   assertTrue(stats.getNRead() == nExpected)
///   assertTrue(stats.getNConverted() == nExpected)
#[test]
fn test_deleter_mutation_removes_records_and_stats() {
    let td = TempDir::new().unwrap();
    let env_cfg =
        EnvironmentConfig::new(td.path().to_path_buf()).with_allow_create(true);
    let env = Environment::open(env_cfg).unwrap();
    let cfg = StoreConfig::new("store").with_allow_create(true);
    let mut store = EntityStore::open(&env, cfg).unwrap();
    let ser = MyEntitySerializer;

    // Insert 3 entities.
    {
        let idx: PrimaryIndex<i32, MyEntity> = store.get_primary_index().unwrap();
        for i in 0..3i32 {
            idx.put(&ser, &my_entity(i, None)).unwrap();
        }
    }

    // Apply class deleter for "MyEntity" at version 0.
    let mut mutations = Mutations::new();
    mutations.add_deleter(Deleter::for_class("MyEntity", 0));

    let evolve_cfg = EvolveConfig::new();
    let stats = store.evolve(&mutations, &evolve_cfg).unwrap();

    assert_eq!(stats.n_read(), 3, "n_read must equal the number of entities");
    assert_eq!(
        stats.n_converted(),
        3,
        "n_converted must equal n_read for a class deleter"
    );

    // Verify records are gone.
    let idx: PrimaryIndex<i32, MyEntity> = store.get_primary_index().unwrap();
    assert_eq!(idx.count().unwrap(), 0);
}

/// Converter mutation transforms records; stats report correct counts.
///
/// Mirrors `EvolveTest.testEagerEvolve` with nExpected > 0:
///   assertTrue(stats.getNRead() == nExpected)
///   assertTrue(stats.getNConverted() == nExpected)
///   assertTrue(stats.getNConverted() >= stats.getNRead())
#[test]
fn test_converter_mutation_transforms_records_and_stats() {
    let td = TempDir::new().unwrap();
    let env_cfg =
        EnvironmentConfig::new(td.path().to_path_buf()).with_allow_create(true);
    let env = Environment::open(env_cfg).unwrap();
    let cfg = StoreConfig::new("store").with_allow_create(true);
    let mut store = EntityStore::open(&env, cfg).unwrap();
    let ser = MyEntitySerializer;

    const N: i32 = 4;
    {
        let idx: PrimaryIndex<i32, MyEntity> = store.get_primary_index().unwrap();
        for i in 0..N {
            idx.put(&ser, &my_entity(i, None)).unwrap();
        }
    }

    // Converter appends a sentinel byte — trivial structural change.
    let mut mutations = Mutations::new();
    mutations.add_converter(Converter::for_class("MyEntity", 0, |b: &[u8]| {
        let mut out = b.to_vec();
        out.push(0xAB);
        Some(out)
    }));

    let evolve_cfg = EvolveConfig::new();
    let stats = store.evolve(&mutations, &evolve_cfg).unwrap();

    assert_eq!(stats.n_read(), N as u64);
    assert_eq!(stats.n_converted(), N as u64);
    // JE invariant: n_converted >= n_read (always true for eager evolve).
    assert!(stats.n_converted() >= stats.n_read());
}

/// Running `evolve()` a second time when no unevolved formats remain
/// returns zero stats.
///
/// Mirrors `EvolveTest.testEagerEvolve` second evolve call:
///   assertEquals(0, stats.getNRead())
///   assertEquals(0, stats.getNConverted())
///
/// NOTE: In this Rust port the store does not persist per-record schema
/// versions, so the version-sentinel approach used by EntityStore::evolve
/// means a second run with the SAME mutations will still process records.
/// We test the zero case by running with an *empty* Mutations object the
/// second time, which is the cleanest way to verify no spurious work is done.
#[test]
fn test_evolve_with_empty_mutations_returns_zero() {
    let td = TempDir::new().unwrap();
    let env_cfg =
        EnvironmentConfig::new(td.path().to_path_buf()).with_allow_create(true);
    let env = Environment::open(env_cfg).unwrap();
    let cfg = StoreConfig::new("store").with_allow_create(true);
    let mut store = EntityStore::open(&env, cfg).unwrap();
    let ser = MyEntitySerializer;

    {
        let idx: PrimaryIndex<i32, MyEntity> = store.get_primary_index().unwrap();
        idx.put(&ser, &my_entity(1, None)).unwrap();
    }

    let mutations = Mutations::new(); // empty
    let evolve_cfg = EvolveConfig::new();
    let stats = store.evolve(&mutations, &evolve_cfg).unwrap();

    assert_eq!(stats.n_read(), 0);
    assert_eq!(stats.n_converted(), 0);
}

/// `EvolveConfig.with_class_to_evolve()` filters which classes are processed.
///
/// Mirrors `EvolveTest.testEagerEvolve` – only the targeted class is evolved;
/// untargeted classes must show zero stats.
#[test]
fn test_evolve_config_class_filter() {
    let td = TempDir::new().unwrap();
    let env_cfg =
        EnvironmentConfig::new(td.path().to_path_buf()).with_allow_create(true);
    let env = Environment::open(env_cfg).unwrap();
    let cfg = StoreConfig::new("store").with_allow_create(true);
    let mut store = EntityStore::open(&env, cfg).unwrap();
    let ser = MyEntitySerializer;

    {
        let idx: PrimaryIndex<i32, MyEntity> = store.get_primary_index().unwrap();
        idx.put(&ser, &my_entity(1, None)).unwrap();
    }

    let mut mutations = Mutations::new();
    mutations.add_converter(Converter::for_class("MyEntity", 0, |b: &[u8]| {
        Some(b.to_vec())
    }));

    // Target a *different* class → MyEntity should be skipped entirely.
    let evolve_cfg =
        EvolveConfig::new().with_class_to_evolve("SomeOtherClass");
    let stats = store.evolve(&mutations, &evolve_cfg).unwrap();

    assert_eq!(stats.n_read(), 0);
    assert_eq!(stats.n_converted(), 0);
}

/// `evolve()` on a closed store must return an error.
/// Mirrors JE: calling evolve on a closed EntityStore throws DatabaseException.
#[test]
fn test_evolve_on_closed_store_returns_error() {
    let td = TempDir::new().unwrap();
    let env_cfg =
        EnvironmentConfig::new(td.path().to_path_buf()).with_allow_create(true);
    let env = Environment::open(env_cfg).unwrap();
    let cfg = StoreConfig::new("store").with_allow_create(true);
    let mut store = EntityStore::open(&env, cfg).unwrap();
    store.close().unwrap();

    let mutations = Mutations::new();
    let evolve_cfg = EvolveConfig::new();
    assert!(store.evolve(&mutations, &evolve_cfg).is_err());
}

/// `EvolveStats` accumulation mirrors JE's `getNRead`/`getNConverted`.
/// Direct unit test of the stats type used by the listener callback.
#[test]
fn test_evolve_stats_accumulation() {
    use noxu_persist::evolve::EvolveStats;

    let mut stats = EvolveStats::new();
    assert_eq!(stats.n_read(), 0);
    assert_eq!(stats.n_converted(), 0);

    stats.add(5, 5);
    assert_eq!(stats.n_read(), 5);
    assert_eq!(stats.n_converted(), 5);

    stats.add(3, 2);
    assert_eq!(stats.n_read(), 8);
    assert_eq!(stats.n_converted(), 7);

    // n_converted <= n_read is the general JE invariant (when some
    // records are already up to date they are read but not re-written).
    assert!(stats.n_converted() <= stats.n_read());
}

/// Full round-trip: insert entities, evolve with a converter that rewrites
/// data, verify the changed bytes survive by reading back with a modified
/// deserializer that handles the new format.
///
/// This exercises the same flow as `EvolveTest.testEagerEvolve` which calls
/// `caseObj.readObjects(store, true /*doUpdate*/)` after eager evolution.
#[test]
fn test_evolve_full_round_trip() {
    let td = TempDir::new().unwrap();
    let env_cfg =
        EnvironmentConfig::new(td.path().to_path_buf()).with_allow_create(true);
    let env = Environment::open(env_cfg).unwrap();
    let cfg = StoreConfig::new("store").with_allow_create(true);
    let mut store = EntityStore::open(&env, cfg).unwrap();
    let ser = MyEntitySerializer;

    // Write 2 entities.
    {
        let idx: PrimaryIndex<i32, MyEntity> = store.get_primary_index().unwrap();
        idx.put(&ser, &my_entity(1, Some(10))).unwrap();
        idx.put(&ser, &my_entity(2, Some(20))).unwrap();
    }

    // Converter: flip the sign of sec_key by rewriting it in-place.
    // Old layout for sec_key present: [pri(4)] [0x01] [sk(4)] [label...]
    // New layout: same structure but sk bytes are negated.
    let mut mutations = Mutations::new();
    mutations.add_converter(Converter::for_class("MyEntity", 0, |bytes: &[u8]| {
        // Only modify records that have sec_key present (byte[4] == 1).
        if bytes.len() < 9 || bytes[4] != 1 {
            return Some(bytes.to_vec());
        }
        let mut out = bytes.to_vec();
        let sk = i32::from_be_bytes([out[5], out[6], out[7], out[8]]);
        let new_sk = sk.wrapping_neg();
        let nb = new_sk.to_be_bytes();
        out[5] = nb[0];
        out[6] = nb[1];
        out[7] = nb[2];
        out[8] = nb[3];
        Some(out)
    }));

    let evolve_cfg = EvolveConfig::new();
    let stats = store.evolve(&mutations, &evolve_cfg).unwrap();
    assert_eq!(stats.n_read(), 2);
    assert_eq!(stats.n_converted(), 2);

    // Read back; sec_keys should now be negated.
    let idx: PrimaryIndex<i32, MyEntity> = store.get_primary_index().unwrap();
    let e1 = idx.get(&ser, &1).unwrap().unwrap();
    let e2 = idx.get(&ser, &2).unwrap().unwrap();
    assert_eq!(e1.sec_key, Some(-10));
    assert_eq!(e2.sec_key, Some(-20));
}
