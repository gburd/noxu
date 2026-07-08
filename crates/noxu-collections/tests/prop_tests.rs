//! Property-based tests for the v1.6 typed `noxu-collections` API (Hegel).

use hashbrown::HashMap;
use hegel::generators;
use noxu_bind::ByteArrayBinding;
use noxu_collections::StoredMap;
use noxu_db::{Database, DatabaseConfig, Environment, EnvironmentConfig};
use tempfile::TempDir;

/// Helper: create a temporary environment and database for testing.
fn temp_env_and_db() -> (TempDir, Environment, Database) {
    let temp_dir = TempDir::new().unwrap();
    let env_config = EnvironmentConfig::new(temp_dir.path().to_path_buf())
        .with_allow_create(true);
    let env = Environment::open(env_config).unwrap();
    let db_config =
        DatabaseConfig::new().with_allow_create(true).with_transactional(true);
    let db = env.open_database(None, "testdb", &db_config).unwrap();
    (temp_dir, env, db)
}

// 1. StoredMap put/get: behaves like HashMap for any sequence of
// put operations.
#[hegel::test]
fn prop_stored_map_put_get(tc: hegel::TestCase) {
    let ops: Vec<(Vec<u8>, Vec<u8>)> = tc.draw(
        generators::vecs(generators::tuples!(
            generators::binary().min_size(1).max_size(31),
            generators::binary().max_size(31),
        ))
        .min_size(1)
        .max_size(19),
    );
    let (_td, _env, db) = temp_env_and_db();
    let map: StoredMap<'_, Vec<u8>, Vec<u8>, _, _> =
        StoredMap::new(&db, ByteArrayBinding, ByteArrayBinding);
    let mut expected: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();

    for (key, value) in &ops {
        map.put(None, key, value).unwrap();
        expected.insert(key.clone(), value.clone());
    }

    for (key, expected_value) in &expected {
        let result = map.get(None, key).unwrap();
        assert_eq!(result, Some(expected_value.clone()));
    }
}

// 2. StoredMap remove: after remove, contains_key returns false.
#[hegel::test]
fn prop_stored_map_remove(tc: hegel::TestCase) {
    let key = tc.draw(generators::binary().min_size(1).max_size(31));
    let value = tc.draw(generators::binary().max_size(31));
    let (_td, _env, db) = temp_env_and_db();
    let map: StoredMap<'_, Vec<u8>, Vec<u8>, _, _> =
        StoredMap::new(&db, ByteArrayBinding, ByteArrayBinding);

    map.put(None, &key, &value).unwrap();
    assert!(map.contains_key(None, &key).unwrap());

    map.remove(None, &key).unwrap();
    assert!(!map.contains_key(None, &key).unwrap());
}

// 3. StoredMap len: len matches number of unique keys inserted.
#[hegel::test]
fn prop_stored_map_len(tc: hegel::TestCase) {
    let ops: Vec<(Vec<u8>, Vec<u8>)> = tc.draw(
        generators::vecs(generators::tuples!(
            generators::binary().min_size(1).max_size(15),
            generators::binary().max_size(15),
        ))
        .min_size(1)
        .max_size(19),
    );
    let (_td, _env, db) = temp_env_and_db();
    let map: StoredMap<'_, Vec<u8>, Vec<u8>, _, _> =
        StoredMap::new(&db, ByteArrayBinding, ByteArrayBinding);
    let mut unique_keys: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();

    for (key, value) in &ops {
        map.put(None, key, value).unwrap();
        unique_keys.insert(key.clone(), value.clone());
    }

    let len = map.len(None).unwrap();
    assert_eq!(len, unique_keys.len());
}

// 4. Round-trip via iter() yields exactly the inserted set.
#[hegel::test]
fn prop_stored_map_iter_round_trip(tc: hegel::TestCase) {
    let keys: std::collections::HashSet<Vec<u8>> = tc.draw(
        generators::hashsets(generators::binary().min_size(1).max_size(15))
            .min_size(1)
            .max_size(14),
    );
    let (_td, _env, db) = temp_env_and_db();
    let map: StoredMap<'_, Vec<u8>, Vec<u8>, _, _> =
        StoredMap::new(&db, ByteArrayBinding, ByteArrayBinding);

    for k in &keys {
        map.put(None, k, &b"v".to_vec()).unwrap();
    }

    let collected: HashMap<Vec<u8>, Vec<u8>> =
        map.iter(None).unwrap().map(Result::unwrap).collect();
    assert_eq!(collected.len(), keys.len());
    for k in &keys {
        assert!(collected.contains_key(k));
    }
}
