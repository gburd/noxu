//! Property-based tests for noxu-db using proptest.

use noxu_db::{
    Database, DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig,
    OperationStatus,
};
use proptest::prelude::*;
use std::collections::BTreeMap;
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

// ============================================================================
// 6. CRUD oracle (Sprint 6, Property 1)
// ----------------------------------------------------------------------------
// Drive a randomised sequence of put/delete/get operations against a real
// `noxu_db::Database` and a `BTreeMap<Vec<u8>, Vec<u8>>` oracle, asserting
// equivalence after every step.  This is the model-test pattern recommended
// in the hegel skill (Tier 1 "Model tests") and is the multi-op cousin of
// the v1.4.3 cursor SearchGte oracle test.  It targets bug classes of the
// shape "put doesn't persist", "delete leaves a stale entry", "get returns
// wrong data" — none of which the deterministic suite has been able to
// uncover on its own.
// ============================================================================

#[derive(Debug, Clone)]
enum CrudOp {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
    Get { key: Vec<u8> },
}

fn crud_op_strategy() -> impl Strategy<Value = CrudOp> {
    // Keys are 1..=8 bytes so the random ops actually collide and exercise
    // overwrite / delete-existing / get-hit code paths.  Values span the full
    // 1..=64-byte size range called out in the sprint plan.
    let key_strat = prop::collection::vec(any::<u8>(), 1..=8);
    let val_strat = prop::collection::vec(any::<u8>(), 1..=64);
    prop_oneof![
        (key_strat.clone(), val_strat)
            .prop_map(|(key, value)| CrudOp::Put { key, value }),
        key_strat.clone().prop_map(|key| CrudOp::Delete { key }),
        key_strat.prop_map(|key| CrudOp::Get { key }),
    ]
}

proptest! {
    // Cap at 64 cases × ~100 ops to stay well under a couple of minutes.
    // Each case opens a fresh environment, so the env-open overhead
    // dominates; tightening cases past 64 buys little.
    #![proptest_config(ProptestConfig {
        cases: 64,
        .. ProptestConfig::default()
    })]

    #[test]
    fn prop_crud_agrees_with_btreemap_oracle(
        ops in prop::collection::vec(crud_op_strategy(), 1..=100),
    ) {
        let (_td, _env, db) = temp_env_and_db();
        let mut oracle: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();

        for (i, op) in ops.into_iter().enumerate() {
            match op {
                CrudOp::Put { key, value } => {
                    let key_e = DatabaseEntry::from_data(&key);
                    let val_e = DatabaseEntry::from_data(&value);
                    let status = db.put(None, &key_e, &val_e).unwrap();
                    prop_assert_eq!(
                        status, OperationStatus::Success,
                        "step {}: put({:?}) returned {:?}", i, key, status
                    );
                    oracle.insert(key, value);
                }
                CrudOp::Delete { key } => {
                    let key_e = DatabaseEntry::from_data(&key);
                    let status = db.delete(None, &key_e).unwrap();
                    let oracle_had = oracle.remove(&key).is_some();
                    let expected = if oracle_had {
                        OperationStatus::Success
                    } else {
                        OperationStatus::NotFound
                    };
                    prop_assert_eq!(
                        status, expected,
                        "step {}: delete({:?}) status mismatch (oracle had={})",
                        i, key, oracle_had,
                    );
                }
                CrudOp::Get { key } => {
                    let key_e = DatabaseEntry::from_data(&key);
                    let mut data = DatabaseEntry::new();
                    let status = db.get(None, &key_e, &mut data).unwrap();
                    match (status, oracle.get(&key)) {
                        (OperationStatus::Success, Some(expected)) => {
                            prop_assert_eq!(
                                data.data(), expected.as_slice(),
                                "step {}: get({:?}) returned wrong value",
                                i, key,
                            );
                        }
                        (OperationStatus::NotFound, None) => { /* agree */ }
                        (s, e) => prop_assert!(
                            false,
                            "step {}: get({:?}) disagreement: db_status={:?}, oracle={:?}",
                            i, key, s, e,
                        ),
                    }
                }
            }
        }

        // Final sweep: every key the oracle thinks is committed must be
        // visible at exactly the recorded value.
        for (k, v) in &oracle {
            let mut data = DatabaseEntry::new();
            let key_e = DatabaseEntry::from_data(k);
            let status = db.get(None, &key_e, &mut data).unwrap();
            prop_assert_eq!(
                status, OperationStatus::Success,
                "final sweep: key {:?} missing from db", k,
            );
            prop_assert_eq!(
                data.data(), v.as_slice(),
                "final sweep: key {:?} has stale value", k,
            );
        }
    }
}
