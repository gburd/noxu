//! Property-based tests for the v1.6 typed `noxu-collections` API.

use hashbrown::HashMap;
use noxu_bind::ByteArrayBinding;
use noxu_collections::StoredMap;
use noxu_db::{Database, DatabaseConfig, Environment, EnvironmentConfig};
use proptest::prelude::*;
use tempfile::TempDir;

/// Helper: create a temporary environment and database for testing.
fn temp_env_and_db() -> (TempDir, Environment, Database) {
    let temp_dir = TempDir::new().unwrap();
    let env_config = EnvironmentConfig::new(temp_dir.path().to_path_buf())
        .with_allow_create(true);
    let env = Environment::open(env_config).unwrap();
    let db_config = DatabaseConfig::new().with_allow_create(true);
    let db = env.open_database(None, "testdb", &db_config).unwrap();
    (temp_dir, env, db)
}

proptest! {
    // 1. StoredMap put/get: behaves like HashMap for any sequence of
    // put operations.
    #[test]
    fn prop_stored_map_put_get(
        ops in prop::collection::vec(
            (
                prop::collection::vec(any::<u8>(), 1..32),
                prop::collection::vec(any::<u8>(), 0..32),
            ),
            1..20,
        ),
    ) {
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
            prop_assert_eq!(result, Some(expected_value.clone()));
        }
    }

    // 2. StoredMap remove: after remove, contains_key returns false.
    #[test]
    fn prop_stored_map_remove(
        key in prop::collection::vec(any::<u8>(), 1..32),
        value in prop::collection::vec(any::<u8>(), 0..32),
    ) {
        let (_td, _env, db) = temp_env_and_db();
        let map: StoredMap<'_, Vec<u8>, Vec<u8>, _, _> =
            StoredMap::new(&db, ByteArrayBinding, ByteArrayBinding);

        map.put(None, &key, &value).unwrap();
        prop_assert!(map.contains_key(None, &key).unwrap());

        map.remove(None, &key).unwrap();
        prop_assert!(!map.contains_key(None, &key).unwrap());
    }

    // 3. StoredMap len: len matches number of unique keys inserted.
    #[test]
    fn prop_stored_map_len(
        ops in prop::collection::vec(
            (
                prop::collection::vec(any::<u8>(), 1..16),
                prop::collection::vec(any::<u8>(), 0..16),
            ),
            1..20,
        ),
    ) {
        let (_td, _env, db) = temp_env_and_db();
        let map: StoredMap<'_, Vec<u8>, Vec<u8>, _, _> =
            StoredMap::new(&db, ByteArrayBinding, ByteArrayBinding);
        let mut unique_keys: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();

        for (key, value) in &ops {
            map.put(None, key, value).unwrap();
            unique_keys.insert(key.clone(), value.clone());
        }

        let len = map.len(None).unwrap();
        prop_assert_eq!(len, unique_keys.len());
    }

    // 4. Round-trip via iter() yields exactly the inserted set.
    #[test]
    fn prop_stored_map_iter_round_trip(
        keys in prop::collection::hash_set(
            prop::collection::vec(any::<u8>(), 1..16),
            1..15,
        ),
    ) {
        let (_td, _env, db) = temp_env_and_db();
        let map: StoredMap<'_, Vec<u8>, Vec<u8>, _, _> =
            StoredMap::new(&db, ByteArrayBinding, ByteArrayBinding);

        for k in &keys {
            map.put(None, k, &b"v".to_vec()).unwrap();
        }

        let collected: HashMap<Vec<u8>, Vec<u8>> =
            map.iter(None).unwrap().map(Result::unwrap).collect();
        prop_assert_eq!(collected.len(), keys.len());
        for k in &keys {
            prop_assert!(collected.contains_key(k));
        }
    }
}
