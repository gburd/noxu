//! Property-based tests for noxu-db using proptest.

use noxu_db::{
    Database, DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig,
    OperationStatus,
};
use proptest::prelude::*;
use tempfile::TempDir;

/// Helper: create a temporary environment and database for testing.
fn temp_env_and_db() -> (TempDir, Environment, Database) {
    let temp_dir = TempDir::new().unwrap();
    let env_config = EnvironmentConfig::new(temp_dir.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    let env = Environment::open(env_config).unwrap();

    let db_config = DatabaseConfig::new().with_allow_create(true);
    let db = env.open_database(None, "testdb", &db_config).unwrap();

    (temp_dir, env, db)
}

proptest! {
    // 1. DatabaseEntry round-trip: for any Vec<u8>, from_data then data() returns the same bytes.
    #[test]
    fn prop_database_entry_round_trip(data: Vec<u8>) {
        let entry = DatabaseEntry::from_data(&data);
        prop_assert_eq!(entry.data(), data.as_slice());
    }

    // 2. DatabaseEntry clone equality: cloned entry has the same data as the original.
    #[test]
    fn prop_database_entry_clone_equality(data: Vec<u8>) {
        let entry = DatabaseEntry::from_data(&data);
        let cloned = entry.clone();
        prop_assert_eq!(entry.get_data(), cloned.get_data());
        prop_assert_eq!(entry, cloned);
    }

    // 3. Put then get: for any key/value bytes, put(key, val) then get(key) returns val.
    #[test]
    fn prop_put_then_get(key: Vec<u8>, value: Vec<u8>) {
        let (_td, _env, db) = temp_env_and_db();

        let key_entry = DatabaseEntry::from_data(&key);
        let val_entry = DatabaseEntry::from_data(&value);

        let status = db.put(None, &key_entry, &val_entry).unwrap();
        prop_assert_eq!(status, OperationStatus::Success);

        let mut retrieved = DatabaseEntry::new();
        let status = db.get(None, &key_entry, &mut retrieved).unwrap();
        prop_assert_eq!(status, OperationStatus::Success);
        prop_assert_eq!(retrieved.data(), value.as_slice());
    }

    // 4. Delete then get: put(key, val), delete(key), get(key) returns NotFound.
    #[test]
    fn prop_delete_then_get(key: Vec<u8>, value: Vec<u8>) {
        let (_td, _env, db) = temp_env_and_db();

        let key_entry = DatabaseEntry::from_data(&key);
        let val_entry = DatabaseEntry::from_data(&value);

        db.put(None, &key_entry, &val_entry).unwrap();

        let status = db.delete(None, &key_entry).unwrap();
        prop_assert_eq!(status, OperationStatus::Success);

        let mut retrieved = DatabaseEntry::new();
        let status = db.get(None, &key_entry, &mut retrieved).unwrap();
        prop_assert_eq!(status, OperationStatus::NotFound);
    }

    // 5. Multiple puts: last put wins  -  put(key, v1), put(key, v2), get(key) returns v2.
    #[test]
    fn prop_last_put_wins(key: Vec<u8>, v1: Vec<u8>, v2: Vec<u8>) {
        let (_td, _env, db) = temp_env_and_db();

        let key_entry = DatabaseEntry::from_data(&key);
        let val1_entry = DatabaseEntry::from_data(&v1);
        let val2_entry = DatabaseEntry::from_data(&v2);

        db.put(None, &key_entry, &val1_entry).unwrap();
        db.put(None, &key_entry, &val2_entry).unwrap();

        let mut retrieved = DatabaseEntry::new();
        let status = db.get(None, &key_entry, &mut retrieved).unwrap();
        prop_assert_eq!(status, OperationStatus::Success);
        prop_assert_eq!(retrieved.data(), v2.as_slice());
    }
}
