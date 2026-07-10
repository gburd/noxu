//! Property-based tests for noxu-db (Hegel / hegeltest).

use hegel::generators;
use noxu_db::{
    Database, DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig,
};
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

// 1. DatabaseEntry round-trip: for any Vec<u8>, from_data then data() returns the same bytes.
#[hegel::test]
fn prop_database_entry_round_trip(tc: hegel::TestCase) {
    let data: Vec<u8> = tc.draw(generators::binary());
    let entry = DatabaseEntry::from_data(&data);
    assert_eq!(entry.data(), data.as_slice());
}

// 2. DatabaseEntry clone equality: cloned entry has the same data as the original.
#[hegel::test]
fn prop_database_entry_clone_equality(tc: hegel::TestCase) {
    let data: Vec<u8> = tc.draw(generators::binary());
    let entry = DatabaseEntry::from_data(&data);
    let cloned = entry.clone();
    assert_eq!(entry.data_opt(), cloned.data_opt());
    assert_eq!(entry, cloned);
}

// 3. Put then get: for any key/value bytes, put(key, val) then get(key) returns val.
#[hegel::test]
fn prop_put_then_get(tc: hegel::TestCase) {
    let key: Vec<u8> = tc.draw(generators::binary());
    let value: Vec<u8> = tc.draw(generators::binary());
    let (_td, _env, db) = temp_env_and_db();

    let key_entry = DatabaseEntry::from_data(&key);
    let val_entry = DatabaseEntry::from_data(&value);

    db.put(&key_entry, &val_entry).unwrap();

    let mut retrieved = DatabaseEntry::new();
    let status = db.get_into(None, &key_entry, &mut retrieved).unwrap();
    assert!(status);
    assert_eq!(retrieved.data(), value.as_slice());
}

// 4. Delete then get: put(key, val), delete(key), get(key) returns NotFound.
#[hegel::test]
fn prop_delete_then_get(tc: hegel::TestCase) {
    let key: Vec<u8> = tc.draw(generators::binary());
    let value: Vec<u8> = tc.draw(generators::binary());
    let (_td, _env, db) = temp_env_and_db();

    let key_entry = DatabaseEntry::from_data(&key);
    let val_entry = DatabaseEntry::from_data(&value);

    db.put(&key_entry, &val_entry).unwrap();

    let status = db.delete(&key_entry).unwrap();
    assert!(status);

    let mut retrieved = DatabaseEntry::new();
    let status = db.get_into(None, &key_entry, &mut retrieved).unwrap();
    assert!(!status);
}

// 5. Multiple puts: last put wins  -  put(key, v1), put(key, v2), get(key) returns v2.
#[hegel::test]
fn prop_last_put_wins(tc: hegel::TestCase) {
    let key: Vec<u8> = tc.draw(generators::binary());
    let v1: Vec<u8> = tc.draw(generators::binary());
    let v2: Vec<u8> = tc.draw(generators::binary());
    let (_td, _env, db) = temp_env_and_db();

    let key_entry = DatabaseEntry::from_data(&key);
    let val1_entry = DatabaseEntry::from_data(&v1);
    let val2_entry = DatabaseEntry::from_data(&v2);

    db.put(&key_entry, &val1_entry).unwrap();
    db.put(&key_entry, &val2_entry).unwrap();

    let mut retrieved = DatabaseEntry::new();
    let status = db.get_into(None, &key_entry, &mut retrieved).unwrap();
    assert!(status);
    assert_eq!(retrieved.data(), v2.as_slice());
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

#[hegel::composite]
fn crud_op(tc: hegel::TestCase) -> CrudOp {
    // Keys are 1..=8 bytes so the random ops actually collide and exercise
    // overwrite / delete-existing / get-hit code paths.  Values span the full
    // 1..=64-byte size range called out in the sprint plan.
    let tag = tc.draw(generators::sampled_from(vec!["put", "delete", "get"]));
    let key = tc.draw(generators::binary().min_size(1).max_size(8));
    match tag {
        "put" => {
            let value = tc.draw(generators::binary().min_size(1).max_size(64));
            CrudOp::Put { key, value }
        }
        "delete" => CrudOp::Delete { key },
        _ => CrudOp::Get { key },
    }
}

// Cap at 64 cases × ~100 ops to stay well under a couple of minutes.
// Each case opens a fresh environment, so the env-open overhead
// dominates; tightening cases past 64 buys little.
#[hegel::test(test_cases = 64)]
fn prop_crud_agrees_with_btreemap_oracle(tc: hegel::TestCase) {
    let ops: Vec<CrudOp> =
        tc.draw(generators::vecs(crud_op()).min_size(1).max_size(100));
    let (_td, _env, db) = temp_env_and_db();
    let mut oracle: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();

    for (i, op) in ops.into_iter().enumerate() {
        match op {
            CrudOp::Put { key, value } => {
                let key_e = DatabaseEntry::from_data(&key);
                let val_e = DatabaseEntry::from_data(&value);
                db.put(&key_e, &val_e).unwrap();
                oracle.insert(key, value);
            }
            CrudOp::Delete { key } => {
                let key_e = DatabaseEntry::from_data(&key);
                let status = db.delete(&key_e).unwrap();
                let oracle_had = oracle.remove(&key).is_some();
                assert_eq!(
                    status, oracle_had,
                    "step {}: delete({:?}) status mismatch (oracle had={})",
                    i, key, oracle_had,
                );
            }
            CrudOp::Get { key } => {
                let key_e = DatabaseEntry::from_data(&key);
                let mut data = DatabaseEntry::new();
                let status = db.get_into(None, &key_e, &mut data).unwrap();
                match (status, oracle.get(&key)) {
                    (true, Some(expected)) => {
                        assert_eq!(
                            data.data(),
                            expected.as_slice(),
                            "step {}: get({:?}) returned wrong value",
                            i,
                            key,
                        );
                    }
                    (false, None) => { /* agree */ }
                    (s, e) => panic!(
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
        let status = db.get_into(None, &key_e, &mut data).unwrap();
        assert!(status, "final sweep: key {:?} missing from db", k);
        assert_eq!(
            data.data(),
            v.as_slice(),
            "final sweep: key {:?} has stale value",
            k,
        );
    }
}
