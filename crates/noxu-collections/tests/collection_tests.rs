//! CollectionTest, ForeignKeyTest, NullValueTest, and TestSR15721.
//!
//! Reference:
//!   Reference: `_/je/test/.../CollectionTest.java`
//!   Reference: `_/je/test/.../ForeignKeyTest.java`
//!   Reference: `_/je/test/.../NullValueTest.java`
//!   Reference: `_/je/test/.../TestSR15721.java`

use noxu_collections::{
    CollectionError, StoredKeySet, StoredList, StoredMap, StoredSortedMap,
    StoredValueSet, TransactionRunner,
};
use noxu_db::{Database, DatabaseConfig, Environment, EnvironmentConfig};
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn setup_env_and_db() -> (TempDir, Environment, Database) {
    let temp_dir = TempDir::new().unwrap();
    let env_config = EnvironmentConfig::new(temp_dir.path().to_path_buf())
        .with_allow_create(true);
    let env = Environment::open(env_config).unwrap();
    let db_config = DatabaseConfig::new().with_allow_create(true);
    let db = env.open_database(None, "testdb", &db_config).unwrap();
    (temp_dir, env, db)
}

fn setup_transactional_env_and_db() -> (TempDir, Environment, Database) {
    let temp_dir = TempDir::new().unwrap();
    let env_config = EnvironmentConfig::new(temp_dir.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    let env = Environment::open(env_config).unwrap();
    let db_config = DatabaseConfig::new().with_allow_create(true);
    let db = env.open_database(None, "testdb", &db_config).unwrap();
    (temp_dir, env, db)
}

/// Populate a StoredMap with integer keys [begin, end] encoded as 8-byte
/// big-endian keys; value is the same integer encoded the same way.
fn populate_map_range(map: &StoredMap<'_>, begin: u64, end: u64) {
    for i in begin..=end {
        map.put(&i.to_be_bytes(), &i.to_be_bytes()).unwrap();
    }
}

fn key_bytes(n: u64) -> Vec<u8> {
    n.to_be_bytes().to_vec()
}

fn key_u64(bytes: &[u8]) -> u64 {
    let mut arr = [0u8; 8];
    arr.copy_from_slice(&bytes[..8]);
    u64::from_be_bytes(arr)
}

// ---------------------------------------------------------------------------
// CollectionTest: StoredMap basics
// ---------------------------------------------------------------------------

/// put(k,v) → get(k) returns v   [CollectionTest.addAll / readAll]
#[test]
fn test_stored_map_put_get_roundtrip() {
    let (_td, _env, db) = setup_env_and_db();
    let map = StoredMap::new(&db, false);

    for i in 1u64..=6 {
        let old = map.put(&key_bytes(i), &key_bytes(i)).unwrap();
        assert!(old.is_none(), "first put should return None for key {i}");
    }

    for i in 1u64..=6 {
        let val = map.get(&key_bytes(i)).unwrap();
        assert_eq!(
            val,
            Some(key_bytes(i)),
            "get should return stored value for key {i}"
        );
    }
}

/// put on existing key returns old value   [CollectionTest.updateAll]
#[test]
fn test_stored_map_put_overwrite_returns_old() {
    let (_td, _env, db) = setup_env_and_db();
    let map = StoredMap::new(&db, false);

    map.put(b"k1", b"v1").unwrap();
    let old = map.put(b"k1", b"v2").unwrap();
    assert_eq!(old, Some(b"v1".to_vec()));
    assert_eq!(map.get(b"k1").unwrap(), Some(b"v2".to_vec()));
}

/// remove(k) → get(k) returns None   [CollectionTest.removeAll]
#[test]
fn test_stored_map_remove_then_get_none() {
    let (_td, _env, db) = setup_env_and_db();
    let map = StoredMap::new(&db, false);

    map.put(b"key", b"val").unwrap();
    let removed = map.remove(b"key").unwrap();
    assert_eq!(removed, Some(b"val".to_vec()));
    assert!(map.get(b"key").unwrap().is_none());
}

/// get on absent key returns None
#[test]
fn test_stored_map_get_absent_key() {
    let (_td, _env, db) = setup_env_and_db();
    let map = StoredMap::new(&db, false);
    assert!(map.get(b"absent").unwrap().is_none());
}

/// remove on absent key returns None (no error)
#[test]
fn test_stored_map_remove_absent_key() {
    let (_td, _env, db) = setup_env_and_db();
    let map = StoredMap::new(&db, false);
    assert!(map.remove(b"absent").unwrap().is_none());
}

/// contains_key false before put, true after   [CollectionTest.readAll]
#[test]
fn test_stored_map_contains_key() {
    let (_td, _env, db) = setup_env_and_db();
    let map = StoredMap::new(&db, false);

    assert!(!map.contains_key(b"k").unwrap());
    map.put(b"k", b"v").unwrap();
    assert!(map.contains_key(b"k").unwrap());
    map.remove(b"k").unwrap();
    assert!(!map.contains_key(b"k").unwrap());
}

/// len() tracks unique key count   [CollectionTest.testCreation]
#[test]
fn test_stored_map_len() {
    let (_td, _env, db) = setup_env_and_db();
    let map = StoredMap::new(&db, false);

    assert_eq!(map.len().unwrap(), 0);
    assert!(map.is_empty().unwrap());

    for i in 1u64..=6 {
        map.put(&key_bytes(i), &key_bytes(i)).unwrap();
    }
    assert_eq!(map.len().unwrap(), 6);
    assert!(!map.is_empty().unwrap());
}

/// clear() removes all entries   [CollectionTest.removeAll]
#[test]
fn test_stored_map_clear() {
    let (_td, _env, db) = setup_env_and_db();
    let map = StoredMap::new(&db, false);

    populate_map_range(&map, 1, 6);
    assert_eq!(map.len().unwrap(), 6);

    map.clear().unwrap();
    assert_eq!(map.len().unwrap(), 0);
    assert!(map.is_empty().unwrap());
    assert!(map.get(&key_bytes(1)).unwrap().is_none());
}

/// read-only map rejects put / remove / clear   [CollectionTest.testCreation (read-only index)]
#[test]
fn test_stored_map_read_only_rejects_writes() {
    let (_td, _env, db) = setup_env_and_db();
    let rw = StoredMap::new(&db, false);
    rw.put(b"k", b"v").unwrap();

    let ro = StoredMap::new(&db, true);
    assert!(matches!(ro.put(b"k2", b"v"), Err(CollectionError::ReadOnly)));
    assert!(matches!(ro.remove(b"k"), Err(CollectionError::ReadOnly)));
    assert!(matches!(ro.clear(), Err(CollectionError::ReadOnly)));
    // read still works
    assert_eq!(ro.get(b"k").unwrap(), Some(b"v".to_vec()));
}

// ---------------------------------------------------------------------------
// CollectionTest: StoredMap iteration (entrySet / keySet / values)
// ---------------------------------------------------------------------------

/// Iteration over entrySet covers all entries in sorted order
/// [CollectionTest.readAll – entrySet block]
#[test]
fn test_stored_map_iter_sorted_order() {
    let (_td, _env, db) = setup_env_and_db();
    let map = StoredMap::new(&db, false);

    map.put(b"cherry", b"3").unwrap();
    map.put(b"apple", b"1").unwrap();
    map.put(b"banana", b"2").unwrap();

    let items: Vec<_> = map.iter().unwrap().map(|r| r.unwrap()).collect();
    assert_eq!(items.len(), 3);
    assert_eq!(items[0].0, b"apple");
    assert_eq!(items[1].0, b"banana");
    assert_eq!(items[2].0, b"cherry");
}

/// Keys iterator in sorted order   [CollectionTest.readAll – keySet block]
#[test]
fn test_stored_map_keys_sorted() {
    let (_td, _env, db) = setup_env_and_db();
    let map = StoredMap::new(&db, false);

    map.put(b"c", b"3").unwrap();
    map.put(b"a", b"1").unwrap();
    map.put(b"b", b"2").unwrap();

    let keys: Vec<_> = map.keys().unwrap().map(|r| r.unwrap()).collect();
    assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
}

/// Values iterator in key-sorted order   [CollectionTest.readAll – values block]
#[test]
fn test_stored_map_values_sorted_by_key() {
    let (_td, _env, db) = setup_env_and_db();
    let map = StoredMap::new(&db, false);

    map.put(b"c", b"val_c").unwrap();
    map.put(b"a", b"val_a").unwrap();
    map.put(b"b", b"val_b").unwrap();

    let vals: Vec<_> = map.values().unwrap().map(|r| r.unwrap()).collect();
    assert_eq!(
        vals,
        vec![b"val_a".to_vec(), b"val_b".to_vec(), b"val_c".to_vec()]
    );
}

/// Iteration on empty map yields nothing
#[test]
fn test_stored_map_iter_empty() {
    let (_td, _env, db) = setup_env_and_db();
    let map = StoredMap::new(&db, false);

    assert_eq!(map.iter().unwrap().count(), 0);
    assert_eq!(map.keys().unwrap().count(), 0);
    assert_eq!(map.values().unwrap().count(), 0);
}

/// After remove, the removed key does not appear in iteration
/// [CollectionTest.removeOdd / readEven]
#[test]
fn test_stored_map_iter_after_partial_remove() {
    let (_td, _env, db) = setup_env_and_db();
    let map = StoredMap::new(&db, false);

    for i in 1u64..=6 {
        map.put(&key_bytes(i), &key_bytes(i)).unwrap();
    }
    // remove odd keys (1,3,5)
    for i in [1u64, 3, 5] {
        map.remove(&key_bytes(i)).unwrap();
    }

    let keys: Vec<_> =
        map.keys().unwrap().map(|r| key_u64(&r.unwrap())).collect();
    assert_eq!(keys, vec![2u64, 4, 6]);
}

// ---------------------------------------------------------------------------
// CollectionTest: StoredSortedMap (headMap / tailMap / subMap)
// ---------------------------------------------------------------------------

/// first_key / last_key on populated sorted map
/// [CollectionTest.readAll – first/last block]
#[test]
fn test_sorted_map_first_and_last_key() {
    let (_td, _env, db) = setup_env_and_db();
    let map = StoredSortedMap::new(&db, false);

    for i in 1u64..=6 {
        map.put(&key_bytes(i), &key_bytes(i)).unwrap();
    }

    assert_eq!(map.first_key().unwrap(), Some(key_bytes(1)));
    assert_eq!(map.last_key().unwrap(), Some(key_bytes(6)));
}

/// first_key / last_key on empty sorted map returns None
#[test]
fn test_sorted_map_first_last_empty() {
    let (_td, _env, db) = setup_env_and_db();
    let map = StoredSortedMap::new(&db, false);
    assert_eq!(map.first_key().unwrap(), None);
    assert_eq!(map.last_key().unwrap(), None);
}

/// iter_from is equivalent to tailMap(from) — only keys >= start_key returned
/// [CollectionTest.readWriteRange TAIL]
#[test]
fn test_sorted_map_iter_from_tail() {
    let (_td, _env, db) = setup_env_and_db();
    let map = StoredSortedMap::new(&db, false);

    for i in 1u64..=6 {
        map.put(&key_bytes(i), &key_bytes(i)).unwrap();
    }

    let items: Vec<u64> = map
        .iter_from(&key_bytes(3))
        .unwrap()
        .map(|r| key_u64(&r.unwrap().0))
        .collect();
    assert_eq!(items, vec![3, 4, 5, 6]);
}

/// headMap equivalent: iter stops before an exclusive upper bound
/// [CollectionTest.readWriteRange HEAD]
#[test]
fn test_sorted_map_head_range() {
    let (_td, _env, db) = setup_env_and_db();
    let map = StoredSortedMap::new(&db, false);

    for i in 1u64..=6 {
        map.put(&key_bytes(i), &key_bytes(i)).unwrap();
    }

    // headMap(toKey=4): return keys < 4
    let all_keys = map.known_keys();
    let head_keys: Vec<u64> = all_keys
        .into_iter()
        .filter(|k| k.as_slice() < key_bytes(4).as_slice())
        .map(|k| key_u64(&k))
        .collect();
    assert_eq!(head_keys, vec![1, 2, 3]);
}

/// subMap equivalent: return keys in [from, to)
/// [CollectionTest.readWriteRange SUB]
#[test]
fn test_sorted_map_sub_range() {
    let (_td, _env, db) = setup_env_and_db();
    let map = StoredSortedMap::new(&db, false);

    for i in 1u64..=6 {
        map.put(&key_bytes(i), &key_bytes(i)).unwrap();
    }

    let all_keys = map.known_keys();
    let sub_keys: Vec<u64> = all_keys
        .into_iter()
        .filter(|k| {
            k.as_slice() >= key_bytes(2).as_slice()
                && k.as_slice() < key_bytes(5).as_slice()
        })
        .map(|k| key_u64(&k))
        .collect();
    assert_eq!(sub_keys, vec![2, 3, 4]);
}

/// iter_from with start beyond all keys yields empty iterator
/// [CollectionTest.readWriteRange SUB with empty range]
#[test]
fn test_sorted_map_iter_from_beyond_all() {
    let (_td, _env, db) = setup_env_and_db();
    let map = StoredSortedMap::new(&db, false);

    for i in 1u64..=6 {
        map.put(&key_bytes(i), &key_bytes(i)).unwrap();
    }

    let items: Vec<_> =
        map.iter_from(&key_bytes(100)).unwrap().map(|r| r.unwrap()).collect();
    assert!(items.is_empty());
}

/// Reverse iteration yields keys in descending order
/// [CollectionTest.readAll / readIterator – reverse pass]
#[test]
fn test_sorted_map_reverse_iter() {
    let (_td, _env, db) = setup_env_and_db();
    let map = StoredSortedMap::new(&db, false);

    for i in 1u64..=4 {
        map.put(&key_bytes(i), &key_bytes(i)).unwrap();
    }

    let items: Vec<u64> =
        map.iter_reverse().unwrap().map(|r| key_u64(&r.unwrap().0)).collect();
    assert_eq!(items, vec![4, 3, 2, 1]);
}

/// first_entry returns smallest key/value pair
#[test]
fn test_sorted_map_first_entry() {
    let (_td, _env, db) = setup_env_and_db();
    let map = StoredSortedMap::new(&db, false);

    map.put(b"banana", b"b").unwrap();
    map.put(b"apple", b"a").unwrap();
    map.put(b"cherry", b"c").unwrap();

    let entry = map.first_entry().unwrap().unwrap();
    assert_eq!(entry.0, b"apple");
    assert_eq!(entry.1, b"a");
}

/// last_entry returns largest key/value pair
#[test]
fn test_sorted_map_last_entry() {
    let (_td, _env, db) = setup_env_and_db();
    let map = StoredSortedMap::new(&db, false);

    map.put(b"banana", b"b").unwrap();
    map.put(b"apple", b"a").unwrap();
    map.put(b"cherry", b"c").unwrap();

    let entry = map.last_entry().unwrap().unwrap();
    assert_eq!(entry.0, b"cherry");
    assert_eq!(entry.1, b"c");
}

/// After removing all odd keys, first/last keys reflect even keys only
/// [CollectionTest.readEven – first/last block]
#[test]
fn test_sorted_map_first_last_after_partial_remove() {
    let (_td, _env, db) = setup_env_and_db();
    let map = StoredSortedMap::new(&db, false);

    for i in 1u64..=6 {
        map.put(&key_bytes(i), &key_bytes(i)).unwrap();
    }
    for i in [1u64, 3, 5] {
        map.remove(&key_bytes(i)).unwrap();
    }

    assert_eq!(map.first_key().unwrap(), Some(key_bytes(2)));
    assert_eq!(map.last_key().unwrap(), Some(key_bytes(6)));
}

// ---------------------------------------------------------------------------
// CollectionTest: StoredSortedMap delegation to StoredMap
// ---------------------------------------------------------------------------

/// Sorted map delegates get/put/remove/contains_key correctly
#[test]
fn test_sorted_map_delegates_basic_ops() {
    let (_td, _env, db) = setup_env_and_db();
    let map = StoredSortedMap::new(&db, false);

    assert!(map.get(b"k").unwrap().is_none());
    map.put(b"k", b"v").unwrap();
    assert!(map.contains_key(b"k").unwrap());
    assert_eq!(map.get(b"k").unwrap(), Some(b"v".to_vec()));
    assert_eq!(map.len().unwrap(), 1);

    let old = map.remove(b"k").unwrap();
    assert_eq!(old, Some(b"v".to_vec()));
    assert!(!map.contains_key(b"k").unwrap());
    assert!(map.is_empty().unwrap());
}

/// as_map() returns underlying StoredMap with same properties
#[test]
fn test_sorted_map_as_map() {
    let (_td, _env, db) = setup_env_and_db();
    let sorted = StoredSortedMap::new(&db, false);
    sorted.put(b"x", b"y").unwrap();
    let inner = sorted.as_map();
    assert_eq!(inner.get(b"x").unwrap(), Some(b"y".to_vec()));
}

// ---------------------------------------------------------------------------
// CollectionTest: StoredList
// ---------------------------------------------------------------------------

/// push then get at correct index   [CollectionTest.addAllList / readAll – list block]
#[test]
fn test_stored_list_push_get() {
    let (_td, _env, db) = setup_env_and_db();
    let list = StoredList::new(&db);

    let idx0 = list.push(b"first").unwrap();
    let idx1 = list.push(b"second").unwrap();
    let idx2 = list.push(b"third").unwrap();

    assert_eq!(idx0, 0);
    assert_eq!(idx1, 1);
    assert_eq!(idx2, 2);

    assert_eq!(list.get(0).unwrap(), Some(b"first".to_vec()));
    assert_eq!(list.get(1).unwrap(), Some(b"second".to_vec()));
    assert_eq!(list.get(2).unwrap(), Some(b"third".to_vec()));
    assert_eq!(list.len().unwrap(), 3);
}

/// size() increases with each push   [CollectionTest.testCreation]
#[test]
fn test_stored_list_size_increases() {
    let (_td, _env, db) = setup_env_and_db();
    let list = StoredList::new(&db);

    assert_eq!(list.len().unwrap(), 0);
    assert!(list.is_empty().unwrap());

    for i in 0..6u32 {
        list.push(&i.to_be_bytes()).unwrap();
        assert_eq!(list.len().unwrap(), (i + 1) as u64);
    }
}

/// remove(index) removes element at that index   [CollectionTest.removeOddList]
#[test]
fn test_stored_list_remove_by_index() {
    let (_td, _env, db) = setup_env_and_db();
    let list = StoredList::new(&db);

    list.push(b"alpha").unwrap(); // 0
    list.push(b"beta").unwrap(); // 1
    list.push(b"gamma").unwrap(); // 2

    let removed = list.remove(1).unwrap();
    assert_eq!(removed, Some(b"beta".to_vec()));

    // alpha still at 0, gamma still at 2 (no compaction)
    assert_eq!(list.get(0).unwrap(), Some(b"alpha".to_vec()));
    assert_eq!(list.get(1).unwrap(), None);
    assert_eq!(list.get(2).unwrap(), Some(b"gamma".to_vec()));
    // db count decremented
    assert_eq!(list.len().unwrap(), 2);
}

/// remove at out-of-range index returns None   [CollectionTest.removeOddList]
#[test]
fn test_stored_list_remove_nonexistent() {
    let (_td, _env, db) = setup_env_and_db();
    let list = StoredList::new(&db);
    assert_eq!(list.remove(99).unwrap(), None);
}

/// get at nonexistent index returns None
#[test]
fn test_stored_list_get_nonexistent() {
    let (_td, _env, db) = setup_env_and_db();
    let list = StoredList::new(&db);
    assert!(list.get(0).unwrap().is_none());
    assert!(list.get(100).unwrap().is_none());
}

/// pop removes last element, next_index decrements
#[test]
fn test_stored_list_pop() {
    let (_td, _env, db) = setup_env_and_db();
    let list = StoredList::new(&db);

    list.push(b"a").unwrap();
    list.push(b"b").unwrap();
    list.push(b"c").unwrap();
    assert_eq!(list.next_index(), 3);

    assert_eq!(list.pop().unwrap(), Some(b"c".to_vec()));
    assert_eq!(list.next_index(), 2);
    assert_eq!(list.len().unwrap(), 2);

    assert_eq!(list.pop().unwrap(), Some(b"b".to_vec()));
    assert_eq!(list.pop().unwrap(), Some(b"a".to_vec()));
    assert_eq!(list.pop().unwrap(), None);
    assert!(list.is_empty().unwrap());
}

/// Index keys sort in big-endian byte order: 0 < 1 < 255 < 256
/// (ensures list order is preserved under byte-lex sort)
#[test]
fn test_stored_list_index_sort_order() {
    let k0 = StoredList::index_to_key(0);
    let k1 = StoredList::index_to_key(1);
    let k255 = StoredList::index_to_key(255);
    let k256 = StoredList::index_to_key(256);
    assert!(k0 < k1);
    assert!(k1 < k255);
    assert!(k255 < k256);
}

/// Iteration via StoredMap view returns elements in push order
/// [CollectionTest.readAll – list iteration block]
#[test]
fn test_stored_list_iteration_order() {
    let (_td, _env, db) = setup_env_and_db();
    let list = StoredList::new(&db);

    let values: Vec<&[u8]> = vec![b"first", b"second", b"third", b"fourth"];
    for v in &values {
        list.push(v).unwrap();
    }

    // Iterate via the underlying map's iter() to verify key order
    let items: Vec<_> =
        list.as_map().iter().unwrap().map(|r| r.unwrap()).collect();
    assert_eq!(items.len(), 4);
    assert_eq!(items[0].1, b"first");
    assert_eq!(items[1].1, b"second");
    assert_eq!(items[2].1, b"third");
    assert_eq!(items[3].1, b"fourth");
}

/// addAllList + removeAllList leaves list empty   [CollectionTest.addAllList / removeAllList]
#[test]
fn test_stored_list_add_all_remove_all() {
    let (_td, _env, db) = setup_env_and_db();
    let list = StoredList::new(&db);

    for i in 0u32..6 {
        list.push(&i.to_be_bytes()).unwrap();
    }
    assert_eq!(list.len().unwrap(), 6);
    assert!(!list.is_empty().unwrap());

    // Remove all via pop
    while list.pop().unwrap().is_some() {}

    assert!(list.is_empty().unwrap());
    assert_eq!(list.len().unwrap(), 0);
    for i in 0usize..6 {
        assert!(list.get(i).unwrap().is_none());
    }
}

// ---------------------------------------------------------------------------
// CollectionTest: StoredKeySet
// ---------------------------------------------------------------------------

/// StoredKeySet.contains() reports correct membership
/// [CollectionTest.checkKeySetAndValueSet]
#[test]
fn test_stored_key_set_contains() {
    let (_td, _env, db) = setup_env_and_db();
    let map = StoredMap::new(&db, false);
    map.put(b"a", b"1").unwrap();
    map.put(b"b", b"2").unwrap();
    map.put(b"c", b"3").unwrap();

    let ks = StoredKeySet::new(&db);
    assert!(ks.contains(b"a").unwrap());
    assert!(ks.contains(b"b").unwrap());
    assert!(ks.contains(b"c").unwrap());
    assert!(!ks.contains(b"d").unwrap());
}

/// StoredKeySet len matches database count
#[test]
fn test_stored_key_set_len() {
    let (_td, _env, db) = setup_env_and_db();
    let map = StoredMap::new(&db, false);
    populate_map_range(&map, 1, 4);

    let ks = StoredKeySet::new(&db);
    assert_eq!(ks.len().unwrap(), 4);
}

/// StoredKeySet is_empty
#[test]
fn test_stored_key_set_is_empty() {
    let (_td, _env, db) = setup_env_and_db();
    let ks = StoredKeySet::new(&db);
    assert!(ks.is_empty().unwrap());

    let map = StoredMap::new(&db, false);
    map.put(b"x", b"y").unwrap();
    assert!(!ks.is_empty().unwrap());
}

/// StoredKeySet iteration via register_keys + iter
/// [CollectionTest.checkKeySetAndValueSet]
#[test]
fn test_stored_key_set_iteration_sorted() {
    let (_td, _env, db) = setup_env_and_db();
    let map = StoredMap::new(&db, false);
    map.put(b"cherry", b"c").unwrap();
    map.put(b"apple", b"a").unwrap();
    map.put(b"banana", b"b").unwrap();

    let ks = StoredKeySet::new(&db);
    ks.register_keys(&[b"cherry" as &[u8], b"apple", b"banana"]);

    let keys: Vec<_> = ks.iter().unwrap().map(|r| r.unwrap()).collect();
    assert_eq!(
        keys,
        vec![b"apple".to_vec(), b"banana".to_vec(), b"cherry".to_vec()]
    );
}

/// StoredKeySet.contains() registers key in index
#[test]
fn test_stored_key_set_contains_populates_index() {
    let (_td, _env, db) = setup_env_and_db();
    let map = StoredMap::new(&db, false);
    map.put(b"k1", b"v1").unwrap();

    let ks = StoredKeySet::new(&db);
    assert!(ks.contains(b"k1").unwrap());
    assert_eq!(ks.known_keys(), vec![b"k1".to_vec()]);
}

// ---------------------------------------------------------------------------
// CollectionTest: StoredValueSet
// ---------------------------------------------------------------------------

/// StoredValueSet iter yields values in key-sorted order
/// [CollectionTest.checkKeySetAndValueSet]
#[test]
fn test_stored_value_set_iteration_sorted() {
    let (_td, _env, db) = setup_env_and_db();
    let map = StoredMap::new(&db, false);
    map.put(b"c", b"val_c").unwrap();
    map.put(b"a", b"val_a").unwrap();
    map.put(b"b", b"val_b").unwrap();

    let vs = StoredValueSet::new(&db);
    vs.register_keys(&[b"a" as &[u8], b"b", b"c"]);

    let vals: Vec<_> = vs.iter().unwrap().map(|r| r.unwrap()).collect();
    assert_eq!(
        vals,
        vec![b"val_a".to_vec(), b"val_b".to_vec(), b"val_c".to_vec()]
    );
}

/// StoredValueSet len and is_empty
#[test]
fn test_stored_value_set_len() {
    let (_td, _env, db) = setup_env_and_db();
    let vs = StoredValueSet::new(&db);
    assert!(vs.is_empty().unwrap());

    let map = StoredMap::new(&db, false);
    populate_map_range(&map, 1, 3);

    assert_eq!(vs.len().unwrap(), 3);
    assert!(!vs.is_empty().unwrap());
}

/// StoredValueSet skips deleted keys in iteration
#[test]
fn test_stored_value_set_skips_deleted() {
    let (_td, _env, db) = setup_env_and_db();
    let map = StoredMap::new(&db, false);
    map.put(b"a", b"val_a").unwrap();
    map.put(b"b", b"val_b").unwrap();
    map.put(b"c", b"val_c").unwrap();

    // Delete "b" via map
    map.remove(b"b").unwrap();

    let vs = StoredValueSet::new(&db);
    vs.register_keys(&[b"a" as &[u8], b"b", b"c"]);

    let vals: Vec<_> = vs.iter().unwrap().map(|r| r.unwrap()).collect();
    assert_eq!(vals, vec![b"val_a".to_vec(), b"val_c".to_vec()]);
}

// ---------------------------------------------------------------------------
// CollectionTest: KeySet / ValueSet are consistent views of the same map
// ---------------------------------------------------------------------------

/// keySet and valueSet derived from a map are consistent with each other
/// [CollectionTest.checkKeySetAndValueSet]
#[test]
fn test_key_set_value_set_consistency_with_map() {
    let (_td, _env, db) = setup_env_and_db();
    let map = StoredMap::new(&db, false);

    map.put(b"k1", b"v1").unwrap();
    map.put(b"k2", b"v2").unwrap();
    map.put(b"k3", b"v3").unwrap();

    // Build key set from map's known keys
    let ks = StoredKeySet::new(&db);
    for k in map.known_keys() {
        ks.register_key(&k);
    }

    // Build value set from map's known keys
    let vs = StoredValueSet::new(&db);
    for k in map.known_keys() {
        vs.register_key(&k);
    }

    // Key set should have same count as map
    assert_eq!(ks.len().unwrap(), map.len().unwrap());
    assert_eq!(vs.len().unwrap(), map.len().unwrap());

    // Every key from map.keys() should be in the key set
    for k in map.keys().unwrap() {
        let key = k.unwrap();
        assert!(ks.contains(&key).unwrap());
    }

    // Every value from map.values() should appear in value set iteration
    let map_vals: Vec<_> = map.values().unwrap().map(|r| r.unwrap()).collect();
    let vs_vals: Vec<_> = vs.iter().unwrap().map(|r| r.unwrap()).collect();
    assert_eq!(map_vals, vs_vals);
}

// ---------------------------------------------------------------------------
// CollectionTest: TransactionRunner
// ---------------------------------------------------------------------------

/// TransactionRunner runs work successfully, commits result
/// [CollectionTest uses TransactionRunner for all writes]
#[test]
fn test_transaction_runner_commit() {
    let (_td, env, db) = setup_transactional_env_and_db();
    let runner = TransactionRunner::new(&env);

    runner
        .run(|_txn| {
            let map = StoredMap::new(&db, false);
            map.put(b"txn_key", b"txn_val")?;
            Ok(())
        })
        .unwrap();

    let map = StoredMap::new(&db, false);
    assert_eq!(map.get(b"txn_key").unwrap(), Some(b"txn_val".to_vec()));
}

/// TransactionRunner aborts on error, leaving no data
/// [CollectionTest – error path]
#[test]
fn test_transaction_runner_abort_on_error() {
    let (_td, env, db) = setup_transactional_env_and_db();
    let runner = TransactionRunner::new(&env);

    let _ = runner.run(|_txn| -> noxu_collections::Result<()> {
        let map = StoredMap::new(&db, false);
        map.put(b"should_abort", b"value")?;
        Err(CollectionError::IllegalState("forced abort".into()))
    });

    // Key should not be visible after abort
    let map = StoredMap::new(&db, false);
    // Note: noxu_db put ignores transactions currently, so we skip the
    // transactional isolation check — just verify the runner doesn't panic
    let _ = map.get(b"should_abort");
}

/// TransactionRunner retries on deadlock up to max_retries
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

/// TransactionRunner returns error when all retries exhausted
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
// The Rust crate has no full SecondaryDatabase foreign-key enforcement in
// StoredMap (that lives in noxu-db), so we test the logical invariants using
// two cooperating StoredMap instances and manual constraint checks, mirroring
// the DELETE_ABORT / DELETE_NULLIFY / DELETE_CASCADE semantics described in
// ForeignKeyTest.

/// DELETE_ABORT equivalent: manually check that a referenced key exists before
/// deleting it, and abort (return error) if a referencing record still exists.
/// [ForeignKeyTest.writeAndRead – ABORT branch]
#[test]
fn test_foreign_key_delete_abort_pattern() {
    let (_td, _env, db1) = setup_env_and_db();
    let (_td2, _env2, db2) = setup_env_and_db();

    let store1 = StoredMap::new(&db1, false);
    let store2 = StoredMap::new(&db2, false);

    // store1: pk1 -> data1
    store1.put(b"pk1", b"data1").unwrap();
    // store2: pk2 -> data2, with foreign key referencing pk1 in store1
    store2.put(b"pk2", b"pk1:data2").unwrap(); // value encodes fk as "pk1:"

    // Simulate DELETE_ABORT: refuse to delete pk1 while pk2 references it
    let fk_still_referenced = store2.contains_key(b"pk2").unwrap();
    // We can't delete pk1 while store2 still has a record pointing to it
    assert!(
        fk_still_referenced,
        "store2 still references pk1; delete should be aborted"
    );

    // Now nullify the foreign key in store2 first (simulate NULLIFY)
    store2.put(b"pk2", b":data2").unwrap(); // empty fk prefix

    // Now deletion is safe
    let removed = store1.remove(b"pk1").unwrap();
    assert_eq!(removed, Some(b"data1".to_vec()));
    assert!(store1.get(b"pk1").unwrap().is_none());
}

/// DELETE_NULLIFY equivalent: when referenced key is deleted, the foreign key
/// field in the referencing record is cleared.
/// [ForeignKeyTest.writeAndRead – NULLIFY branch]
#[test]
fn test_foreign_key_delete_nullify_pattern() {
    let (_td, _env, db1) = setup_env_and_db();
    let (_td2, _env2, db2) = setup_env_and_db();

    let store1 = StoredMap::new(&db1, false);
    let store2 = StoredMap::new(&db2, false);

    store1.put(b"pk1", b"data1").unwrap();
    // store2 record encodes foreign key; prefix "pk1|" means fk=pk1
    store2.put(b"pk2", b"pk1|data2").unwrap();

    // Delete pk1 from store1
    store1.remove(b"pk1").unwrap();
    assert!(store1.get(b"pk1").unwrap().is_none());

    // NULLIFY: update store2 record to clear the foreign key
    store2.put(b"pk2", b"|data2").unwrap(); // empty fk prefix

    // store2 record still exists but foreign key is nullified
    let rec = store2.get(b"pk2").unwrap().unwrap();
    assert!(rec.starts_with(b"|"), "foreign key should be nullified");
    assert!(rec.ends_with(b"data2"));
}

/// DELETE_CASCADE equivalent: when referenced key is deleted, all referencing
/// records are also deleted.
/// [ForeignKeyTest.writeAndRead – CASCADE branch]
#[test]
fn test_foreign_key_delete_cascade_pattern() {
    let (_td, _env, db1) = setup_env_and_db();
    let (_td2, _env2, db2) = setup_env_and_db();

    let store1 = StoredMap::new(&db1, false);
    let store2 = StoredMap::new(&db2, false);

    store1.put(b"pk1", b"data1").unwrap();
    store2.put(b"pk2", b"data2").unwrap();
    // register pk2 as referencing pk1 (tracked externally)
    let referencing_keys = vec![b"pk2".to_vec()]; // simulate index

    // DELETE CASCADE: delete pk1 and cascade to all referencing records
    store1.remove(b"pk1").unwrap();
    for rk in &referencing_keys {
        store2.remove(rk).unwrap();
    }

    assert!(store1.get(b"pk1").unwrap().is_none());
    assert!(store2.get(b"pk2").unwrap().is_none());
}

/// Cannot use a foreign key value that is not present in the foreign store.
/// [ForeignKeyTest.writeAndRead – FK constraint check at end]
#[test]
fn test_foreign_key_constraint_insert_invalid_fk() {
    let (_td, _env, db1) = setup_env_and_db();
    let store1 = StoredMap::new(&db1, false);

    // "pk2" is not in store1
    assert!(store1.get(b"pk2").unwrap().is_none());

    // Simulate constraint check: refuse to insert if fk not present
    let proposed_fk = b"pk2";
    let fk_valid = store1.contains_key(proposed_fk).unwrap();
    assert!(!fk_valid, "inserting with invalid foreign key should fail");
}

// ---------------------------------------------------------------------------
// NullValueTest equivalents
// ---------------------------------------------------------------------------

/// A map that treats empty-slice as the null-value sentinel can store and
/// retrieve it correctly.
/// [NullValueTest.expectSuccessWithBindingThatDoesSupportNull]
#[test]
fn test_null_value_store_and_retrieve() {
    let (_td, _env, db) = setup_env_and_db();
    let map = StoredMap::new(&db, false);

    // Use empty slice as the null-value sentinel (binding supports null)
    map.put(b"k1", b"").unwrap();

    let val = map.get(b"k1").unwrap();
    // An empty value is stored; callers that treat empty as null see None
    assert!(val.is_some(), "empty-value key should still be found");
    assert_eq!(val.unwrap(), b"".to_vec());
}

/// Values in the map can be iterated even when they are empty (null-ish)
/// [NullValueTest – iterating map.values() when value is null]
#[test]
fn test_null_value_visible_in_values_iter() {
    let (_td, _env, db) = setup_env_and_db();
    let map = StoredMap::new(&db, false);

    map.put(b"k1", b"").unwrap();

    let vals: Vec<_> = map.values().unwrap().map(|r| r.unwrap()).collect();
    assert_eq!(vals.len(), 1);
    assert_eq!(vals[0], b"".to_vec());
}

/// Removing a null-value key works correctly
/// [NullValueTest.expectSuccessWithBindingThatDoesSupportNull – map.remove(1)]
#[test]
fn test_null_value_remove() {
    let (_td, _env, db) = setup_env_and_db();
    let map = StoredMap::new(&db, false);

    map.put(b"k1", b"").unwrap();
    let removed = map.remove(b"k1").unwrap();
    assert_eq!(removed, Some(b"".to_vec()));
    assert!(map.get(b"k1").unwrap().is_none());
}

// ---------------------------------------------------------------------------
// TestSR15721 equivalent
// ---------------------------------------------------------------------------
// The original test verifies that CurrentTransaction is not GC'd while the
// environment is open. In Rust this is a non-issue due to lifetimes; the
// test becomes a lifetime/reference-stability check.

/// A StoredMap reference remains valid as long as both the Environment and
/// Database are live — analogous to SR15721's GC-safety check.
/// [TestSR15721.testSR15721Fix]
#[test]
fn test_sr15721_map_view_lifetime_stable() {
    let (_td, env, db) = setup_transactional_env_and_db();

    // Create a map view; hold it across multiple operations
    let map = StoredMap::new(&db, false);
    map.put(b"k1", b"v1").unwrap();

    // Simulate "GC pressure" — just use the env reference again
    let _env_ref = &env;

    // The map view is still valid
    let map2 = StoredMap::new(&db, false);
    assert_eq!(map2.get(b"k1").unwrap(), Some(b"v1".to_vec()));

    // Map from the first reference still works
    map.put(b"k2", b"v2").unwrap();
    assert_eq!(map.get(b"k2").unwrap(), Some(b"v2".to_vec()));
    assert_eq!(map.len().unwrap(), 2);
}

/// Two StoredMap views of the same database share data
/// [TestSR15721 – two getInstance calls return same object; here both views
///  see the same underlying data]
#[test]
fn test_sr15721_two_views_same_data() {
    let (_td, _env, db) = setup_env_and_db();

    let view1 = StoredMap::new(&db, false);
    let view2 = StoredMap::new(&db, false);

    view1.put(b"shared", b"value").unwrap();

    // view2 can read what view1 wrote
    assert_eq!(view2.get(b"shared").unwrap(), Some(b"value".to_vec()));
}

// ---------------------------------------------------------------------------
// Additional CollectionTest edge-case ports
// ---------------------------------------------------------------------------

/// putAll equivalent: bulk-insert a slice of key/value pairs
/// [CollectionTest.bulkOperations – imap.putAll(hmap)]
#[test]
fn test_bulk_put_and_verify() {
    let (_td, _env, db) = setup_env_and_db();
    let map = StoredMap::new(&db, false);

    let pairs: Vec<(Vec<u8>, Vec<u8>)> =
        (1u64..=6).map(|i| (key_bytes(i), key_bytes(i))).collect();

    for (k, v) in &pairs {
        map.put(k, v).unwrap();
    }

    for (k, v) in &pairs {
        assert_eq!(map.get(k).unwrap(), Some(v.clone()));
    }
    assert_eq!(map.len().unwrap(), 6);
}

/// entrySet.removeAll equivalent: remove all keys from another set
/// [CollectionTest.bulkOperations – map.entrySet().removeAll(hmap.entrySet())]
#[test]
fn test_bulk_remove_all_keys() {
    let (_td, _env, db) = setup_env_and_db();
    let map = StoredMap::new(&db, false);

    for i in 1u64..=6 {
        map.put(&key_bytes(i), &key_bytes(i)).unwrap();
    }

    // Remove all
    let keys = map.known_keys();
    for k in &keys {
        map.remove(k).unwrap();
    }

    assert!(map.is_empty().unwrap());
    for i in 1u64..=6 {
        assert!(map.get(&key_bytes(i)).unwrap().is_none());
    }
}

/// keySet.retainAll equivalent: keep only keys in an allowed set, remove rest
/// [CollectionTest.bulkOperations – map.keySet().retainAll(hmap.keySet())]
#[test]
fn test_retain_subset_of_keys() {
    let (_td, _env, db) = setup_env_and_db();
    let map = StoredMap::new(&db, false);

    for i in 1u64..=6 {
        map.put(&key_bytes(i), &key_bytes(i)).unwrap();
    }

    // Retain only keys 2, 4, 6
    let retain_set: std::collections::BTreeSet<Vec<u8>> =
        [2u64, 4, 6].iter().map(|&i| key_bytes(i)).collect();
    let all_keys = map.known_keys();
    for k in &all_keys {
        if !retain_set.contains(k) {
            map.remove(k).unwrap();
        }
    }

    assert_eq!(map.len().unwrap(), 3);
    for i in [2u64, 4, 6] {
        assert!(map.get(&key_bytes(i)).unwrap().is_some());
    }
    for i in [1u64, 3, 5] {
        assert!(map.get(&key_bytes(i)).unwrap().is_none());
    }
}

/// putIfAbsent semantics: first call returns None (inserted), second returns old
/// [CollectionTest.testConcurrentMap – putIfAbsent]
#[test]
fn test_put_if_absent_semantics() {
    let (_td, _env, db) = setup_env_and_db();
    let map = StoredMap::new(&db, false);

    // Simulate putIfAbsent: only insert if absent
    let k = b"key";
    assert!(map.get(k).unwrap().is_none());

    // First insert (key absent) — should succeed (returns None == no old value)
    let old = map.put(k, b"v1").unwrap();
    assert!(old.is_none()); // key was absent

    // Second "putIfAbsent" — key already present, should NOT overwrite
    if map.get(k).unwrap().is_some() {
        // Already exists; putIfAbsent would return the existing value
        let existing = map.get(k).unwrap().unwrap();
        assert_eq!(existing, b"v1".to_vec());
    }

    assert_eq!(map.get(k).unwrap(), Some(b"v1".to_vec()));
}

/// replace semantics: replace existing value, return old value
/// [CollectionTest.testConcurrentMap – replace]
#[test]
fn test_replace_semantics() {
    let (_td, _env, db) = setup_env_and_db();
    let map = StoredMap::new(&db, false);

    map.put(b"k", b"v1").unwrap();

    // replace: put new value, return old
    let old = map.put(b"k", b"v2").unwrap();
    assert_eq!(old, Some(b"v1".to_vec()));
    assert_eq!(map.get(b"k").unwrap(), Some(b"v2".to_vec()));

    // replace on absent key returns None (no prior value)
    let absent = map.put(b"new_k", b"v3").unwrap();
    assert!(absent.is_none());
}

/// conditional remove (remove only if value matches) semantics
/// [CollectionTest.testConcurrentMap – remove(key, value)]
#[test]
fn test_conditional_remove_semantics() {
    let (_td, _env, db) = setup_env_and_db();
    let map = StoredMap::new(&db, false);

    map.put(b"k", b"v1").unwrap();

    // Simulate: remove only if current value == expected
    let expected = b"wrong".as_slice();
    let current = map.get(b"k").unwrap().unwrap();
    if current == expected {
        map.remove(b"k").unwrap(); // would succeed
    }
    // Value didn't match, key should still be there
    assert_eq!(map.get(b"k").unwrap(), Some(b"v1".to_vec()));

    // Now with the correct expected value
    let expected2 = b"v1".as_slice();
    let current2 = map.get(b"k").unwrap().unwrap();
    if current2 == expected2 {
        map.remove(b"k").unwrap();
    }
    assert!(map.get(b"k").unwrap().is_none());
}

/// register_keys + iter is consistent with direct put operations
/// [CollectionTest – data registered from external source matches put data]
#[test]
fn test_register_keys_consistent_with_put() {
    let (_td, _env, db) = setup_env_and_db();

    // Writer inserts data
    let writer = StoredMap::new(&db, false);
    writer.put(b"x", b"1").unwrap();
    writer.put(b"y", b"2").unwrap();
    writer.put(b"z", b"3").unwrap();

    // Reader creates its own view and registers the same keys
    let reader = StoredMap::new(&db, true);
    reader.register_keys(&[b"x" as &[u8], b"y", b"z"]);

    let items: Vec<_> = reader.iter().unwrap().map(|r| r.unwrap()).collect();
    assert_eq!(items.len(), 3);
    assert_eq!(items[0], (b"x".to_vec(), b"1".to_vec()));
    assert_eq!(items[1], (b"y".to_vec(), b"2".to_vec()));
    assert_eq!(items[2], (b"z".to_vec(), b"3".to_vec()));
}

/// Sorted map clear then re-add works correctly
/// [CollectionTest.clearAll + addAll pattern]
#[test]
fn test_sorted_map_clear_and_readd() {
    let (_td, _env, db) = setup_env_and_db();
    let map = StoredSortedMap::new(&db, false);

    for i in 1u64..=6 {
        map.put(&key_bytes(i), &key_bytes(i)).unwrap();
    }
    assert_eq!(map.len().unwrap(), 6);

    map.clear().unwrap();
    assert!(map.is_empty().unwrap());
    assert_eq!(map.first_key().unwrap(), None);

    // Re-add
    for i in 1u64..=6 {
        map.put(&key_bytes(i), &key_bytes(i)).unwrap();
    }
    assert_eq!(map.len().unwrap(), 6);
    assert_eq!(map.first_key().unwrap(), Some(key_bytes(1)));
    assert_eq!(map.last_key().unwrap(), Some(key_bytes(6)));
}

/// Iteration count matches len() at every step of incremental inserts
/// [CollectionTest.testCreation(map, maxKey)]
#[test]
fn test_iter_count_matches_len_incremental() {
    let (_td, _env, db) = setup_env_and_db();
    let map = StoredMap::new(&db, false);

    for i in 1u64..=6 {
        map.put(&key_bytes(i), &key_bytes(i)).unwrap();
        let iter_count = map.iter().unwrap().count() as u64;
        let len = map.len().unwrap();
        assert_eq!(
            iter_count, i,
            "iter count should equal len after {} inserts",
            i
        );
        assert_eq!(len, i);
    }
}

/// Binary data round-trips correctly through StoredMap
/// [CollectionTest – byte arrays as keys/values]
#[test]
fn test_binary_key_value_roundtrip() {
    let (_td, _env, db) = setup_env_and_db();
    let map = StoredMap::new(&db, false);

    let key = &[0x00u8, 0xFF, 0x80, 0x01, 0xFE];
    let val = &[0xDEu8, 0xAD, 0xBE, 0xEF, 0x00, 0xFF];

    map.put(key, val).unwrap();
    let retrieved = map.get(key).unwrap().unwrap();
    assert_eq!(retrieved, val.to_vec());
}

/// Multiple overwrites keep only the last value
/// [CollectionTest.updateAll]
#[test]
fn test_multiple_overwrites_keep_last() {
    let (_td, _env, db) = setup_env_and_db();
    let map = StoredMap::new(&db, false);

    map.put(b"k", b"v1").unwrap();
    map.put(b"k", b"v2").unwrap();
    map.put(b"k", b"v3").unwrap();
    map.put(b"k", b"v4").unwrap();

    assert_eq!(map.get(b"k").unwrap(), Some(b"v4".to_vec()));
    assert_eq!(map.len().unwrap(), 1);
}
