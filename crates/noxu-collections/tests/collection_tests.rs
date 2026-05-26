//! BDB-JE collection-test ports against the v1.6 typed `noxu-collections`
//! API surface.
//!
//! These tests preserve the spirit of the original `_/je`
//! `CollectionTest`, `ForeignKeyTest`, `NullValueTest`, and
//! `TestSR15721` cases.  Wave 2B (v1.6) breaks the v1.5 `&[u8]`-keyed
//! shape, so byte-keyed tests now use `ByteArrayBinding` for both
//! keys and values; tests that depended on removed APIs
//! (`register_key`, `register_keys`, `known_keys`) have been dropped
//! since the typed iterators walk the database directly via a cursor.
//!
//! Reference:
//!   - `_/je/test/.../CollectionTest.java`
//!   - `_/je/test/.../ForeignKeyTest.java`
//!   - `_/je/test/.../NullValueTest.java`
//!   - `_/je/test/.../TestSR15721.java`

use noxu_bind::{ByteArrayBinding, IntBinding, StringBinding};
use noxu_collections::{
    CollectionError, StoredKeySet, StoredList, StoredMap, StoredSortedMap,
    StoredValueSet, TransactionRunner,
};
use noxu_db::{Database, DatabaseConfig, Environment, EnvironmentConfig};
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

type ByteMap<'db> = StoredMap<'db, Vec<u8>, Vec<u8>, ByteArrayBinding, ByteArrayBinding>;
type ByteSortedMap<'db> =
    StoredSortedMap<'db, Vec<u8>, Vec<u8>, ByteArrayBinding, ByteArrayBinding>;
type ByteKeySet<'db> = StoredKeySet<'db, Vec<u8>, ByteArrayBinding>;
type ByteValueSet<'db> = StoredValueSet<'db, Vec<u8>, ByteArrayBinding>;
type ByteList<'db> = StoredList<'db, Vec<u8>, ByteArrayBinding>;

fn setup_env_and_db() -> (TempDir, Environment, Database) {
    let td = TempDir::new().unwrap();
    let env = Environment::open(
        EnvironmentConfig::new(td.path().to_path_buf()).with_allow_create(true),
    )
    .unwrap();
    let db = env
        .open_database(
            None,
            "testdb",
            &DatabaseConfig::new().with_allow_create(true),
        )
        .unwrap();
    (td, env, db)
}

fn setup_transactional_env_and_db() -> (TempDir, Environment, Database) {
    let td = TempDir::new().unwrap();
    let env = Environment::open(
        EnvironmentConfig::new(td.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true),
    )
    .unwrap();
    let db = env
        .open_database(
            None,
            "testdb",
            &DatabaseConfig::new().with_allow_create(true),
        )
        .unwrap();
    (td, env, db)
}

fn key_bytes(n: u64) -> Vec<u8> {
    n.to_be_bytes().to_vec()
}

fn key_u64(bytes: &[u8]) -> u64 {
    let mut arr = [0u8; 8];
    arr.copy_from_slice(&bytes[..8]);
    u64::from_be_bytes(arr)
}

fn populate_map_range(map: &ByteMap<'_>, begin: u64, end: u64) {
    for i in begin..=end {
        map.put(None, &key_bytes(i), &key_bytes(i)).unwrap();
    }
}

fn make_byte_map(db: &Database) -> ByteMap<'_> {
    StoredMap::new(db, ByteArrayBinding, ByteArrayBinding)
}

fn make_byte_sorted_map(db: &Database) -> ByteSortedMap<'_> {
    StoredSortedMap::new(db, ByteArrayBinding, ByteArrayBinding)
}

// ---------------------------------------------------------------------------
// CollectionTest: StoredMap basics
// ---------------------------------------------------------------------------

#[test]
fn test_stored_map_put_get_roundtrip() {
    let (_td, _env, db) = setup_env_and_db();
    let map = make_byte_map(&db);

    for i in 1u64..=6 {
        let old = map.put(None, &key_bytes(i), &key_bytes(i)).unwrap();
        assert!(old.is_none(), "first put should return None for key {i}");
    }

    for i in 1u64..=6 {
        let val = map.get(None, &key_bytes(i)).unwrap();
        assert_eq!(val, Some(key_bytes(i)));
    }
}

#[test]
fn test_stored_map_put_overwrite_returns_old() {
    let (_td, _env, db) = setup_env_and_db();
    let map = make_byte_map(&db);
    map.put(None, &b"k1".to_vec(), &b"v1".to_vec()).unwrap();
    let old = map.put(None, &b"k1".to_vec(), &b"v2".to_vec()).unwrap();
    assert_eq!(old, Some(b"v1".to_vec()));
    assert_eq!(map.get(None, &b"k1".to_vec()).unwrap(), Some(b"v2".to_vec()));
}

#[test]
fn test_stored_map_remove_then_get_none() {
    let (_td, _env, db) = setup_env_and_db();
    let map = make_byte_map(&db);
    map.put(None, &b"key".to_vec(), &b"val".to_vec()).unwrap();
    let removed = map.remove(None, &b"key".to_vec()).unwrap();
    assert_eq!(removed, Some(b"val".to_vec()));
    assert!(map.get(None, &b"key".to_vec()).unwrap().is_none());
}

#[test]
fn test_stored_map_get_absent_key() {
    let (_td, _env, db) = setup_env_and_db();
    let map = make_byte_map(&db);
    assert!(map.get(None, &b"absent".to_vec()).unwrap().is_none());
}

#[test]
fn test_stored_map_remove_absent_key() {
    let (_td, _env, db) = setup_env_and_db();
    let map = make_byte_map(&db);
    assert!(map.remove(None, &b"absent".to_vec()).unwrap().is_none());
}

#[test]
fn test_stored_map_contains_key() {
    let (_td, _env, db) = setup_env_and_db();
    let map = make_byte_map(&db);

    assert!(!map.contains_key(None, &b"k".to_vec()).unwrap());
    map.put(None, &b"k".to_vec(), &b"v".to_vec()).unwrap();
    assert!(map.contains_key(None, &b"k".to_vec()).unwrap());
    map.remove(None, &b"k".to_vec()).unwrap();
    assert!(!map.contains_key(None, &b"k".to_vec()).unwrap());
}

#[test]
fn test_stored_map_len() {
    let (_td, _env, db) = setup_env_and_db();
    let map = make_byte_map(&db);

    assert_eq!(map.len(None).unwrap(), 0);
    assert!(map.is_empty(None).unwrap());

    for i in 1u64..=6 {
        map.put(None, &key_bytes(i), &key_bytes(i)).unwrap();
    }
    assert_eq!(map.len(None).unwrap(), 6);
    assert!(!map.is_empty(None).unwrap());
}

#[test]
fn test_stored_map_clear() {
    let (_td, _env, db) = setup_env_and_db();
    let map = make_byte_map(&db);
    populate_map_range(&map, 1, 6);
    assert_eq!(map.len(None).unwrap(), 6);

    map.clear(None).unwrap();
    assert_eq!(map.len(None).unwrap(), 0);
    assert!(map.is_empty(None).unwrap());
    assert!(map.get(None, &key_bytes(1)).unwrap().is_none());
}

#[test]
fn test_stored_map_read_only_rejects_writes() {
    let (_td, _env, db) = setup_env_and_db();
    let rw = make_byte_map(&db);
    rw.put(None, &b"k".to_vec(), &b"v".to_vec()).unwrap();

    let ro = StoredMap::new_read_only(&db, ByteArrayBinding, ByteArrayBinding);
    assert!(matches!(
        ro.put(None, &b"k2".to_vec(), &b"v".to_vec()),
        Err(CollectionError::ReadOnly),
    ));
    assert!(matches!(
        ro.remove(None, &b"k".to_vec()),
        Err(CollectionError::ReadOnly),
    ));
    assert!(matches!(ro.clear(None), Err(CollectionError::ReadOnly)));
    // read still works
    assert_eq!(ro.get(None, &b"k".to_vec()).unwrap(), Some(b"v".to_vec()));
}

// ---------------------------------------------------------------------------
// CollectionTest: StoredMap iteration
// ---------------------------------------------------------------------------

#[test]
fn test_stored_map_iter_sorted_order() {
    let (_td, _env, db) = setup_env_and_db();
    let map = make_byte_map(&db);
    map.put(None, &b"cherry".to_vec(), &b"3".to_vec()).unwrap();
    map.put(None, &b"apple".to_vec(), &b"1".to_vec()).unwrap();
    map.put(None, &b"banana".to_vec(), &b"2".to_vec()).unwrap();

    let items: Vec<_> = map.iter(None).unwrap().map(Result::unwrap).collect();
    assert_eq!(items.len(), 3);
    assert_eq!(items[0].0, b"apple");
    assert_eq!(items[1].0, b"banana");
    assert_eq!(items[2].0, b"cherry");
}

#[test]
fn test_stored_map_keys_sorted() {
    let (_td, _env, db) = setup_env_and_db();
    let map = make_byte_map(&db);
    map.put(None, &b"c".to_vec(), &b"3".to_vec()).unwrap();
    map.put(None, &b"a".to_vec(), &b"1".to_vec()).unwrap();
    map.put(None, &b"b".to_vec(), &b"2".to_vec()).unwrap();

    let keys: Vec<_> = map.keys(None).unwrap().map(Result::unwrap).collect();
    assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
}

#[test]
fn test_stored_map_values_sorted_by_key() {
    let (_td, _env, db) = setup_env_and_db();
    let map = make_byte_map(&db);
    map.put(None, &b"c".to_vec(), &b"val_c".to_vec()).unwrap();
    map.put(None, &b"a".to_vec(), &b"val_a".to_vec()).unwrap();
    map.put(None, &b"b".to_vec(), &b"val_b".to_vec()).unwrap();

    let vals: Vec<_> = map.values(None).unwrap().map(Result::unwrap).collect();
    assert_eq!(
        vals,
        vec![b"val_a".to_vec(), b"val_b".to_vec(), b"val_c".to_vec()],
    );
}

#[test]
fn test_stored_map_iter_empty() {
    let (_td, _env, db) = setup_env_and_db();
    let map = make_byte_map(&db);
    assert_eq!(map.iter(None).unwrap().count(), 0);
    assert_eq!(map.keys(None).unwrap().count(), 0);
    assert_eq!(map.values(None).unwrap().count(), 0);
}

#[test]
fn test_stored_map_iter_after_partial_remove() {
    let (_td, _env, db) = setup_env_and_db();
    let map = make_byte_map(&db);
    for i in 1u64..=6 {
        map.put(None, &key_bytes(i), &key_bytes(i)).unwrap();
    }
    for i in [1u64, 3, 5] {
        map.remove(None, &key_bytes(i)).unwrap();
    }
    let keys: Vec<_> = map
        .keys(None)
        .unwrap()
        .map(|r| key_u64(&r.unwrap()))
        .collect();
    assert_eq!(keys, vec![2u64, 4, 6]);
}

// ---------------------------------------------------------------------------
// CollectionTest: StoredSortedMap (headMap / tailMap / subMap)
// ---------------------------------------------------------------------------

#[test]
fn test_sorted_map_first_and_last_key() {
    let (_td, _env, db) = setup_env_and_db();
    let map = make_byte_sorted_map(&db);
    for i in 1u64..=6 {
        map.put(None, &key_bytes(i), &key_bytes(i)).unwrap();
    }
    assert_eq!(map.first_key(None).unwrap(), Some(key_bytes(1)));
    assert_eq!(map.last_key(None).unwrap(), Some(key_bytes(6)));
}

#[test]
fn test_sorted_map_first_last_empty() {
    let (_td, _env, db) = setup_env_and_db();
    let map = make_byte_sorted_map(&db);
    assert_eq!(map.first_key(None).unwrap(), None);
    assert_eq!(map.last_key(None).unwrap(), None);
}

#[test]
fn test_sorted_map_iter_from_tail() {
    let (_td, _env, db) = setup_env_and_db();
    let map = make_byte_sorted_map(&db);
    for i in 1u64..=6 {
        map.put(None, &key_bytes(i), &key_bytes(i)).unwrap();
    }
    let items: Vec<u64> = map
        .iter_from(None, &key_bytes(3))
        .unwrap()
        .map(|r| key_u64(&r.unwrap().0))
        .collect();
    assert_eq!(items, vec![3, 4, 5, 6]);
}

#[test]
fn test_sorted_map_iter_from_beyond_all() {
    let (_td, _env, db) = setup_env_and_db();
    let map = make_byte_sorted_map(&db);
    for i in 1u64..=6 {
        map.put(None, &key_bytes(i), &key_bytes(i)).unwrap();
    }
    let items: Vec<_> = map
        .iter_from(None, &key_bytes(100))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert!(items.is_empty());
}

#[test]
fn test_sorted_map_reverse_iter() {
    let (_td, _env, db) = setup_env_and_db();
    let map = make_byte_sorted_map(&db);
    for i in 1u64..=4 {
        map.put(None, &key_bytes(i), &key_bytes(i)).unwrap();
    }
    let items: Vec<u64> = map
        .iter_reverse(None)
        .unwrap()
        .map(|r| key_u64(&r.unwrap().0))
        .collect();
    assert_eq!(items, vec![4, 3, 2, 1]);
}

#[test]
fn test_sorted_map_first_entry() {
    let (_td, _env, db) = setup_env_and_db();
    let map = make_byte_sorted_map(&db);
    map.put(None, &b"banana".to_vec(), &b"b".to_vec()).unwrap();
    map.put(None, &b"apple".to_vec(), &b"a".to_vec()).unwrap();
    map.put(None, &b"cherry".to_vec(), &b"c".to_vec()).unwrap();

    let entry = map.first_entry(None).unwrap().unwrap();
    assert_eq!(entry.0, b"apple");
    assert_eq!(entry.1, b"a");
}

#[test]
fn test_sorted_map_last_entry() {
    let (_td, _env, db) = setup_env_and_db();
    let map = make_byte_sorted_map(&db);
    map.put(None, &b"banana".to_vec(), &b"b".to_vec()).unwrap();
    map.put(None, &b"apple".to_vec(), &b"a".to_vec()).unwrap();
    map.put(None, &b"cherry".to_vec(), &b"c".to_vec()).unwrap();

    let entry = map.last_entry(None).unwrap().unwrap();
    assert_eq!(entry.0, b"cherry");
    assert_eq!(entry.1, b"c");
}

#[test]
fn test_sorted_map_first_last_after_partial_remove() {
    let (_td, _env, db) = setup_env_and_db();
    let map = make_byte_sorted_map(&db);
    for i in 1u64..=6 {
        map.put(None, &key_bytes(i), &key_bytes(i)).unwrap();
    }
    for i in [1u64, 3, 5] {
        map.remove(None, &key_bytes(i)).unwrap();
    }
    assert_eq!(map.first_key(None).unwrap(), Some(key_bytes(2)));
    assert_eq!(map.last_key(None).unwrap(), Some(key_bytes(6)));
}

// ---------------------------------------------------------------------------
// CollectionTest: StoredSortedMap delegation
// ---------------------------------------------------------------------------

#[test]
fn test_sorted_map_delegates_basic_ops() {
    let (_td, _env, db) = setup_env_and_db();
    let map = make_byte_sorted_map(&db);

    assert!(map.get(None, &b"k".to_vec()).unwrap().is_none());
    map.put(None, &b"k".to_vec(), &b"v".to_vec()).unwrap();
    assert!(map.contains_key(None, &b"k".to_vec()).unwrap());
    assert_eq!(map.get(None, &b"k".to_vec()).unwrap(), Some(b"v".to_vec()));
    assert_eq!(map.len(None).unwrap(), 1);

    let old = map.remove(None, &b"k".to_vec()).unwrap();
    assert_eq!(old, Some(b"v".to_vec()));
    assert!(!map.contains_key(None, &b"k".to_vec()).unwrap());
    assert!(map.is_empty(None).unwrap());
}

#[test]
fn test_sorted_map_as_map() {
    let (_td, _env, db) = setup_env_and_db();
    let sorted = make_byte_sorted_map(&db);
    sorted.put(None, &b"x".to_vec(), &b"y".to_vec()).unwrap();
    let inner = sorted.as_map();
    assert_eq!(
        inner.get(None, &b"x".to_vec()).unwrap(),
        Some(b"y".to_vec()),
    );
}

// ---------------------------------------------------------------------------
// CollectionTest: StoredList
// ---------------------------------------------------------------------------

fn make_byte_list(db: &Database) -> ByteList<'_> {
    StoredList::new(db, ByteArrayBinding)
}

#[test]
fn test_stored_list_push_get() {
    let (_td, _env, db) = setup_env_and_db();
    let list = make_byte_list(&db);

    let idx0 = list.push(None, &b"first".to_vec()).unwrap();
    let idx1 = list.push(None, &b"second".to_vec()).unwrap();
    let idx2 = list.push(None, &b"third".to_vec()).unwrap();
    assert_eq!((idx0, idx1, idx2), (0, 1, 2));

    assert_eq!(list.get(None, 0).unwrap(), Some(b"first".to_vec()));
    assert_eq!(list.get(None, 1).unwrap(), Some(b"second".to_vec()));
    assert_eq!(list.get(None, 2).unwrap(), Some(b"third".to_vec()));
    assert_eq!(list.len(None).unwrap(), 3);
}

#[test]
fn test_stored_list_size_increases() {
    let (_td, _env, db) = setup_env_and_db();
    let list = make_byte_list(&db);

    assert_eq!(list.len(None).unwrap(), 0);
    assert!(list.is_empty(None).unwrap());

    for i in 0u32..6 {
        list.push(None, &i.to_be_bytes().to_vec()).unwrap();
        assert_eq!(list.len(None).unwrap(), (i + 1) as usize);
    }
}

#[test]
fn test_stored_list_remove_compacts() {
    let (_td, _env, db) = setup_env_and_db();
    let list = make_byte_list(&db);

    list.push(None, &b"alpha".to_vec()).unwrap();
    list.push(None, &b"beta".to_vec()).unwrap();
    list.push(None, &b"gamma".to_vec()).unwrap();

    let removed = list.remove(None, 1).unwrap();
    assert_eq!(removed, Some(b"beta".to_vec()));

    // Wave 2B: remove(idx) shifts every higher element down.  The
    // list is now [alpha, gamma] at indices [0, 1].
    assert_eq!(list.get(None, 0).unwrap(), Some(b"alpha".to_vec()));
    assert_eq!(list.get(None, 1).unwrap(), Some(b"gamma".to_vec()));
    assert_eq!(list.get(None, 2).unwrap(), None);
    assert_eq!(list.len(None).unwrap(), 2);
    assert_eq!(list.next_index(), 2);
}

#[test]
fn test_stored_list_remove_nonexistent() {
    let (_td, _env, db) = setup_env_and_db();
    let list = make_byte_list(&db);
    assert_eq!(list.remove(None, 99).unwrap(), None);
}

#[test]
fn test_stored_list_get_nonexistent() {
    let (_td, _env, db) = setup_env_and_db();
    let list = make_byte_list(&db);
    assert!(list.get(None, 0).unwrap().is_none());
    assert!(list.get(None, 100).unwrap().is_none());
}

#[test]
fn test_stored_list_pop() {
    let (_td, _env, db) = setup_env_and_db();
    let list = make_byte_list(&db);

    list.push(None, &b"a".to_vec()).unwrap();
    list.push(None, &b"b".to_vec()).unwrap();
    list.push(None, &b"c".to_vec()).unwrap();
    assert_eq!(list.next_index(), 3);

    assert_eq!(list.pop(None).unwrap(), Some(b"c".to_vec()));
    assert_eq!(list.next_index(), 2);
    assert_eq!(list.len(None).unwrap(), 2);

    assert_eq!(list.pop(None).unwrap(), Some(b"b".to_vec()));
    assert_eq!(list.pop(None).unwrap(), Some(b"a".to_vec()));
    assert_eq!(list.pop(None).unwrap(), None);
    assert!(list.is_empty(None).unwrap());
}

#[test]
fn test_stored_list_index_sort_order() {
    let k0 = StoredList::<Vec<u8>, ByteArrayBinding>::index_to_key(0);
    let k1 = StoredList::<Vec<u8>, ByteArrayBinding>::index_to_key(1);
    let k255 = StoredList::<Vec<u8>, ByteArrayBinding>::index_to_key(255);
    let k256 = StoredList::<Vec<u8>, ByteArrayBinding>::index_to_key(256);
    assert!(k0 < k1);
    assert!(k1 < k255);
    assert!(k255 < k256);
}

#[test]
fn test_stored_list_iteration_order() {
    let (_td, _env, db) = setup_env_and_db();
    let list = make_byte_list(&db);

    let values: Vec<&[u8]> = vec![b"first", b"second", b"third", b"fourth"];
    for v in &values {
        list.push(None, &v.to_vec()).unwrap();
    }
    let items: Vec<Vec<u8>> =
        list.iter(None).unwrap().map(Result::unwrap).collect();
    assert_eq!(
        items,
        vec![
            b"first".to_vec(),
            b"second".to_vec(),
            b"third".to_vec(),
            b"fourth".to_vec(),
        ],
    );
}

#[test]
fn test_stored_list_add_all_remove_all() {
    let (_td, _env, db) = setup_env_and_db();
    let list = make_byte_list(&db);

    for i in 0u32..6 {
        list.push(None, &i.to_be_bytes().to_vec()).unwrap();
    }
    assert_eq!(list.len(None).unwrap(), 6);
    assert!(!list.is_empty(None).unwrap());

    while list.pop(None).unwrap().is_some() {}

    assert!(list.is_empty(None).unwrap());
    assert_eq!(list.len(None).unwrap(), 0);
}

// ---------------------------------------------------------------------------
// CollectionTest: StoredKeySet / StoredValueSet
// ---------------------------------------------------------------------------

#[test]
fn test_stored_key_set_contains_and_iter() {
    let (_td, _env, db) = setup_env_and_db();
    let ks: ByteKeySet<'_> = StoredKeySet::new(&db, ByteArrayBinding);

    assert!(!ks.contains(None, &b"a".to_vec()).unwrap());
    ks.add(None, &b"a".to_vec()).unwrap();
    ks.add(None, &b"b".to_vec()).unwrap();
    ks.add(None, &b"c".to_vec()).unwrap();

    assert!(ks.contains(None, &b"a".to_vec()).unwrap());
    assert!(!ks.contains(None, &b"x".to_vec()).unwrap());

    let keys: Vec<_> = ks.iter(None).unwrap().map(Result::unwrap).collect();
    assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
}

#[test]
fn test_stored_key_set_len_and_is_empty() {
    let (_td, _env, db) = setup_env_and_db();
    let ks: ByteKeySet<'_> = StoredKeySet::new(&db, ByteArrayBinding);
    assert!(ks.is_empty(None).unwrap());

    for i in 0u8..4 {
        ks.add(None, &vec![i]).unwrap();
    }
    assert_eq!(ks.len(None).unwrap(), 4);
    assert!(!ks.is_empty(None).unwrap());
}

#[test]
fn test_stored_value_set_iter_sorted() {
    let (_td, _env, db) = setup_env_and_db();
    let map = make_byte_map(&db);
    map.put(None, &b"c".to_vec(), &b"val_c".to_vec()).unwrap();
    map.put(None, &b"a".to_vec(), &b"val_a".to_vec()).unwrap();
    map.put(None, &b"b".to_vec(), &b"val_b".to_vec()).unwrap();

    let vs: ByteValueSet<'_> = StoredValueSet::new(&db, ByteArrayBinding);
    let vals: Vec<_> = vs.iter(None).unwrap().map(Result::unwrap).collect();
    assert_eq!(
        vals,
        vec![b"val_a".to_vec(), b"val_b".to_vec(), b"val_c".to_vec()],
    );
}

// ---------------------------------------------------------------------------
// TransactionRunner — typed StoredMap composition
// ---------------------------------------------------------------------------

#[test]
fn test_transaction_runner_drives_typed_storedmap() {
    let (_td, env, db) = setup_transactional_env_and_db();
    let runner = TransactionRunner::new(&env);
    let map: StoredMap<'_, i32, String, _, _> =
        StoredMap::new(&db, IntBinding, StringBinding);

    runner
        .run(|txn| {
            map.put(Some(txn), &1, &"alpha".to_string())?;
            map.put(Some(txn), &2, &"beta".to_string())?;
            Ok(())
        })
        .unwrap();

    assert_eq!(map.get(None, &1).unwrap(), Some("alpha".to_string()));
    assert_eq!(map.get(None, &2).unwrap(), Some("beta".to_string()));
}

#[test]
fn test_transaction_runner_aborts_typed_storedmap_on_error() {
    let (_td, env, db) = setup_transactional_env_and_db();
    let runner = TransactionRunner::new(&env);
    let map: StoredMap<'_, i32, String, _, _> =
        StoredMap::new(&db, IntBinding, StringBinding);

    let result: noxu_collections::Result<()> = runner.run(|txn| {
        map.put(Some(txn), &1, &"set".to_string())?;
        Err(CollectionError::IllegalState("rollback".into()))
    });
    assert!(result.is_err());
    // Aborted: nothing was committed.
    assert_eq!(map.get(None, &1).unwrap(), None);
}

#[test]
fn test_transaction_runner_deadlock_retry() {
    let (_td, env, _db) = setup_transactional_env_and_db();
    let runner = TransactionRunner::new(&env).with_max_retries(3);

    let attempts = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
    let attempts2 = attempts.clone();

    let result = runner.run(move |_txn| {
        let n = attempts2.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if n < 2 {
            Err(CollectionError::DatabaseError(
                noxu_db::NoxuError::DeadlockDetected,
            ))
        } else {
            Ok("done")
        }
    });
    assert_eq!(result.unwrap(), "done");
    assert_eq!(attempts.load(std::sync::atomic::Ordering::SeqCst), 3);
}

#[test]
fn test_transaction_runner_retries_exhausted() {
    let (_td, env, _db) = setup_transactional_env_and_db();
    let runner = TransactionRunner::new(&env).with_max_retries(2);

    let result: noxu_collections::Result<()> = runner.run(|_txn| {
        Err(CollectionError::DatabaseError(
            noxu_db::NoxuError::DeadlockDetected,
        ))
    });
    assert!(result.is_err());
}

// ---------------------------------------------------------------------------
// ForeignKeyTest equivalents
// ---------------------------------------------------------------------------

#[test]
fn test_foreign_key_delete_abort_pattern() {
    let (_td, _env, db1) = setup_env_and_db();
    let (_td2, _env2, db2) = setup_env_and_db();

    let store1 = make_byte_map(&db1);
    let store2 = make_byte_map(&db2);

    store1.put(None, &b"pk1".to_vec(), &b"data1".to_vec()).unwrap();
    store2.put(None, &b"pk2".to_vec(), &b"pk1:data2".to_vec()).unwrap();

    // Simulate DELETE_ABORT: refuse to delete pk1 while pk2 references it.
    let fk_still_referenced =
        store2.contains_key(None, &b"pk2".to_vec()).unwrap();
    assert!(fk_still_referenced);

    // Nullify the foreign key in store2 first.
    store2.put(None, &b"pk2".to_vec(), &b":data2".to_vec()).unwrap();

    let removed = store1.remove(None, &b"pk1".to_vec()).unwrap();
    assert_eq!(removed, Some(b"data1".to_vec()));
    assert!(store1.get(None, &b"pk1".to_vec()).unwrap().is_none());
}

#[test]
fn test_foreign_key_delete_cascade_pattern() {
    let (_td, _env, db1) = setup_env_and_db();
    let (_td2, _env2, db2) = setup_env_and_db();

    let store1 = make_byte_map(&db1);
    let store2 = make_byte_map(&db2);

    store1.put(None, &b"pk1".to_vec(), &b"data1".to_vec()).unwrap();
    store2.put(None, &b"pk2".to_vec(), &b"data2".to_vec()).unwrap();

    let referencing_keys: Vec<Vec<u8>> = vec![b"pk2".to_vec()];
    store1.remove(None, &b"pk1".to_vec()).unwrap();
    for rk in &referencing_keys {
        store2.remove(None, rk).unwrap();
    }
    assert!(store1.get(None, &b"pk1".to_vec()).unwrap().is_none());
    assert!(store2.get(None, &b"pk2".to_vec()).unwrap().is_none());
}

// ---------------------------------------------------------------------------
// NullValueTest equivalents
// ---------------------------------------------------------------------------

#[test]
fn test_null_value_store_and_retrieve() {
    let (_td, _env, db) = setup_env_and_db();
    let map = make_byte_map(&db);
    map.put(None, &b"k1".to_vec(), &b"".to_vec()).unwrap();

    let val = map.get(None, &b"k1".to_vec()).unwrap();
    assert!(val.is_some());
    assert_eq!(val.unwrap(), b"".to_vec());
}

#[test]
fn test_null_value_visible_in_values_iter() {
    let (_td, _env, db) = setup_env_and_db();
    let map = make_byte_map(&db);
    map.put(None, &b"k1".to_vec(), &b"".to_vec()).unwrap();
    let vals: Vec<_> = map.values(None).unwrap().map(Result::unwrap).collect();
    assert_eq!(vals, vec![b"".to_vec()]);
}

#[test]
fn test_null_value_remove() {
    let (_td, _env, db) = setup_env_and_db();
    let map = make_byte_map(&db);
    map.put(None, &b"k1".to_vec(), &b"".to_vec()).unwrap();
    let removed = map.remove(None, &b"k1".to_vec()).unwrap();
    assert_eq!(removed, Some(b"".to_vec()));
    assert!(map.get(None, &b"k1".to_vec()).unwrap().is_none());
}

// ---------------------------------------------------------------------------
// TestSR15721 equivalents
// ---------------------------------------------------------------------------

#[test]
fn test_sr15721_two_views_same_data() {
    let (_td, _env, db) = setup_env_and_db();
    let view1 = make_byte_map(&db);
    let view2 = make_byte_map(&db);

    view1.put(None, &b"shared".to_vec(), &b"value".to_vec()).unwrap();
    assert_eq!(
        view2.get(None, &b"shared".to_vec()).unwrap(),
        Some(b"value".to_vec()),
    );
}

// ---------------------------------------------------------------------------
// Additional CollectionTest edge-case ports
// ---------------------------------------------------------------------------

#[test]
fn test_bulk_put_and_verify() {
    let (_td, _env, db) = setup_env_and_db();
    let map = make_byte_map(&db);

    let pairs: Vec<(Vec<u8>, Vec<u8>)> =
        (1u64..=6).map(|i| (key_bytes(i), key_bytes(i))).collect();
    for (k, v) in &pairs {
        map.put(None, k, v).unwrap();
    }
    for (k, v) in &pairs {
        assert_eq!(map.get(None, k).unwrap(), Some(v.clone()));
    }
    assert_eq!(map.len(None).unwrap(), 6);
}

#[test]
fn test_bulk_remove_via_iter() {
    let (_td, _env, db) = setup_env_and_db();
    let map = make_byte_map(&db);
    for i in 1u64..=6 {
        map.put(None, &key_bytes(i), &key_bytes(i)).unwrap();
    }
    // Snapshot the keys via iter, then remove each.
    let keys: Vec<Vec<u8>> =
        map.keys(None).unwrap().map(Result::unwrap).collect();
    for k in &keys {
        map.remove(None, k).unwrap();
    }
    assert!(map.is_empty(None).unwrap());
}

#[test]
fn test_iter_count_matches_len_incremental() {
    let (_td, _env, db) = setup_env_and_db();
    let map = make_byte_map(&db);
    for i in 1u64..=6 {
        map.put(None, &key_bytes(i), &key_bytes(i)).unwrap();
        let iter_count = map.iter(None).unwrap().count();
        let len = map.len(None).unwrap();
        assert_eq!(iter_count, i as usize);
        assert_eq!(len, i as usize);
    }
}

#[test]
fn test_binary_key_value_roundtrip() {
    let (_td, _env, db) = setup_env_and_db();
    let map = make_byte_map(&db);

    let key = vec![0x00u8, 0xFF, 0x80, 0x01, 0xFE];
    let val = vec![0xDEu8, 0xAD, 0xBE, 0xEF, 0x00, 0xFF];
    map.put(None, &key, &val).unwrap();
    let retrieved = map.get(None, &key).unwrap().unwrap();
    assert_eq!(retrieved, val);
}

#[test]
fn test_multiple_overwrites_keep_last() {
    let (_td, _env, db) = setup_env_and_db();
    let map = make_byte_map(&db);
    map.put(None, &b"k".to_vec(), &b"v1".to_vec()).unwrap();
    map.put(None, &b"k".to_vec(), &b"v2".to_vec()).unwrap();
    map.put(None, &b"k".to_vec(), &b"v3".to_vec()).unwrap();
    map.put(None, &b"k".to_vec(), &b"v4".to_vec()).unwrap();
    assert_eq!(map.get(None, &b"k".to_vec()).unwrap(), Some(b"v4".to_vec()));
    assert_eq!(map.len(None).unwrap(), 1);
}
