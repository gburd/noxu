#![no_main]

//! Recovery / consistency fuzz test for noxu-db.
//!
//! Generates a random sequence of put/get/delete/cursor operations, executes
//! them against an in-memory noxu-db Environment, then "reopens" the
//! environment by dropping it and creating a new one pointed at the same
//! directory. The test verifies that the data visible after the simulated
//! reopen is consistent with what was committed before the close.
//!
//! The focus is on consistency properties: after open → write → close →
//! reopen, no data should be silently corrupted, truncated, or invented.

use libfuzzer_sys::fuzz_target;

use arbitrary::Arbitrary;
use noxu_db::{
    DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig, Get,
    OperationStatus,
};
use std::collections::BTreeMap;

/// Small key-space so that operations frequently collide on the same keys.
const KEYSPACE: u8 = 24;

/// Maximum value length in bytes.
const MAX_VALUE_LEN: u8 = 48;

/// Operations issued before the simulated "reopen".
#[derive(Debug)]
enum RecoveryOp {
    /// Insert or overwrite a key.
    Put { key: u8, value: Vec<u8> },
    /// Delete a key.
    Delete { key: u8 },
    /// Read a key and verify against the oracle (no-op if key absent).
    Get { key: u8 },
    /// Open a cursor, position at First, and iterate forward up to `steps`.
    CursorForward { steps: u8 },
    /// Open a cursor, position at Last, and iterate backward up to `steps`.
    CursorBackward { steps: u8 },
    /// Search for a key via cursor.
    CursorSearch { key: u8 },
}

fn make_key(k: u8) -> Vec<u8> {
    vec![k]
}

impl<'a> Arbitrary<'a> for RecoveryOp {
    fn arbitrary(
        u: &mut arbitrary::Unstructured<'a>,
    ) -> arbitrary::Result<Self> {
        let choice: u8 = u.int_in_range(0..=5)?;
        match choice {
            0 => {
                let key: u8 = u.int_in_range(0..=KEYSPACE)?;
                let len: u8 = u.int_in_range(0..=MAX_VALUE_LEN)?;
                let value: Vec<u8> = (0..len)
                    .map(|_| u.arbitrary::<u8>())
                    .collect::<arbitrary::Result<Vec<u8>>>()?;
                Ok(RecoveryOp::Put { key, value })
            }
            1 => {
                let key: u8 = u.int_in_range(0..=KEYSPACE)?;
                Ok(RecoveryOp::Delete { key })
            }
            2 => {
                let key: u8 = u.int_in_range(0..=KEYSPACE)?;
                Ok(RecoveryOp::Get { key })
            }
            3 => {
                let steps: u8 = u.int_in_range(0..=16)?;
                Ok(RecoveryOp::CursorForward { steps })
            }
            4 => {
                let steps: u8 = u.int_in_range(0..=16)?;
                Ok(RecoveryOp::CursorBackward { steps })
            }
            _ => {
                let key: u8 = u.int_in_range(0..=KEYSPACE)?;
                Ok(RecoveryOp::CursorSearch { key })
            }
        }
    }
}

/// Open an environment + database at `path`. The directory must already exist.
fn open_env_db(path: &std::path::Path) -> (Environment, noxu_db::Database) {
    let env_config = EnvironmentConfig::new(path.to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    let env = Environment::open(env_config).unwrap();
    let db_config = DatabaseConfig::new().with_allow_create(true);
    let db = env.open_database(None, "fuzz_recovery", &db_config).unwrap();
    (env, db)
}

fuzz_target!(|ops: Vec<RecoveryOp>| {
    // Keep each case fast.
    if ops.len() > 128 {
        return;
    }

    let tmp_dir = tempfile::TempDir::new().unwrap();

    // -----------------------------------------------------------------------
    // Phase 1: open, execute operations, track committed state in oracle.
    // -----------------------------------------------------------------------
    let mut oracle: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();

    {
        let (env, db) = open_env_db(tmp_dir.path());

        for op in &ops {
            match op {
                RecoveryOp::Put { key, value } => {
                    let k = make_key(*key);
                    let key_entry = DatabaseEntry::from_bytes(&k);
                    let val_entry = DatabaseEntry::from_bytes(value);
                    let status = db.put(None, &key_entry, &val_entry).unwrap();
                    assert_eq!(status, OperationStatus::Success);
                    oracle.insert(k, value.clone());
                }

                RecoveryOp::Delete { key } => {
                    let k = make_key(*key);
                    let key_entry = DatabaseEntry::from_bytes(&k);
                    let status = db.delete(None, &key_entry).unwrap();
                    if oracle.remove(&k).is_some() {
                        assert_eq!(status, OperationStatus::Success);
                    } else {
                        assert_eq!(status, OperationStatus::NotFound);
                    }
                }

                RecoveryOp::Get { key } => {
                    let k = make_key(*key);
                    let key_entry = DatabaseEntry::from_bytes(&k);
                    let mut data = DatabaseEntry::new();
                    let status = db.get(None, &key_entry, &mut data).unwrap();
                    match oracle.get(&k) {
                        Some(expected) => {
                            assert_eq!(
                                status,
                                OperationStatus::Success,
                                "Phase 1 get: key {:?} should be present",
                                k
                            );
                            assert_eq!(
                                data.data_opt().unwrap(),
                                expected.as_slice(),
                                "Phase 1 get: value mismatch for key {:?}",
                                k
                            );
                        }
                        None => {
                            assert_eq!(
                                status,
                                OperationStatus::NotFound,
                                "Phase 1 get: key {:?} should be absent",
                                k
                            );
                        }
                    }
                }

                RecoveryOp::CursorForward { steps } => {
                    let mut cursor = db.open_cursor(None, None).unwrap();
                    let mut key_entry = DatabaseEntry::new();
                    let mut data = DatabaseEntry::new();

                    let oracle_values: Vec<Vec<u8>> =
                        oracle.values().cloned().collect();

                    let first_status = cursor
                        .get(&mut key_entry, &mut data, Get::First, None)
                        .unwrap();

                    if oracle.is_empty() {
                        assert_eq!(
                            first_status,
                            OperationStatus::NotFound,
                            "Phase 1 forward: expected empty db"
                        );
                    } else {
                        assert_eq!(
                            first_status,
                            OperationStatus::Success,
                            "Phase 1 forward: expected non-empty db"
                        );
                        let mut idx = 0usize;
                        for _ in 0..*steps {
                            let next_status = cursor
                                .get(&mut key_entry, &mut data, Get::Next, None)
                                .unwrap();
                            if next_status != OperationStatus::Success {
                                break;
                            }
                            idx += 1;
                            if idx >= oracle_values.len() {
                                break;
                            }
                        }
                    }
                    cursor.close().unwrap();
                }

                RecoveryOp::CursorBackward { steps } => {
                    let mut cursor = db.open_cursor(None, None).unwrap();
                    let mut key_entry = DatabaseEntry::new();
                    let mut data = DatabaseEntry::new();

                    let last_status = cursor
                        .get(&mut key_entry, &mut data, Get::Last, None)
                        .unwrap();

                    if oracle.is_empty() {
                        assert_eq!(
                            last_status,
                            OperationStatus::NotFound,
                            "Phase 1 backward: expected empty db"
                        );
                    } else {
                        assert_eq!(
                            last_status,
                            OperationStatus::Success,
                            "Phase 1 backward: expected non-empty db"
                        );
                        for _ in 0..*steps {
                            let prev_status = cursor
                                .get(&mut key_entry, &mut data, Get::Prev, None)
                                .unwrap();
                            if prev_status != OperationStatus::Success {
                                break;
                            }
                        }
                    }
                    cursor.close().unwrap();
                }

                RecoveryOp::CursorSearch { key } => {
                    let k = make_key(*key);
                    let mut cursor = db.open_cursor(None, None).unwrap();
                    let mut key_entry = DatabaseEntry::from_bytes(&k);
                    let mut data = DatabaseEntry::new();
                    let status = cursor
                        .get(&mut key_entry, &mut data, Get::Search, None)
                        .unwrap();
                    if oracle.contains_key(&k) {
                        assert_eq!(
                            status,
                            OperationStatus::Success,
                            "Phase 1 search: key {:?} should be found",
                            k
                        );
                        assert_eq!(
                            data.data_opt().unwrap(),
                            oracle.get(&k).unwrap().as_slice(),
                            "Phase 1 search: value mismatch for key {:?}",
                            k
                        );
                    } else {
                        assert_eq!(
                            status,
                            OperationStatus::NotFound,
                            "Phase 1 search: key {:?} should not be found",
                            k
                        );
                    }
                    cursor.close().unwrap();
                }
            }
        }

        // Close before "reopen".
        db.close().unwrap();
        env.close().unwrap();
    } // env and db dropped here

    // -----------------------------------------------------------------------
    // Phase 2: "reopen" — drop + recreate environment at the same path.
    // Verify that every committed record is still present and correct, and
    // that no extra records have appeared.
    // -----------------------------------------------------------------------
    {
        let (env2, db2) = open_env_db(tmp_dir.path());

        // Every oracle entry must be readable.
        for (k, v) in &oracle {
            let key_entry = DatabaseEntry::from_bytes(k);
            let mut data = DatabaseEntry::new();
            let status = db2.get(None, &key_entry, &mut data).unwrap();
            assert_eq!(
                status,
                OperationStatus::Success,
                "Phase 2 (reopen): committed key {:?} missing",
                k
            );
            assert_eq!(
                data.data_opt().unwrap(),
                v.as_slice(),
                "Phase 2 (reopen): value mismatch for committed key {:?}",
                k
            );
        }

        // Record count must match the oracle.
        let db_count = db2.count().unwrap();
        assert_eq!(
            db_count,
            oracle.len() as u64,
            "Phase 2 (reopen): database count {} != oracle count {}",
            db_count,
            oracle.len()
        );

        // Full cursor scan must match oracle in sorted order.
        let mut cursor = db2.open_cursor(None, None).unwrap();
        let mut key_entry = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();
        let mut oracle_iter = oracle.iter();

        let mut status =
            cursor.get(&mut key_entry, &mut data, Get::First, None).unwrap();
        while status == OperationStatus::Success {
            let (expected_key, expected_val) = oracle_iter.next().expect(
                "Phase 2: database has more records than oracle after reopen",
            );
            assert_eq!(
                data.data_opt().unwrap(),
                expected_val.as_slice(),
                "Phase 2: cursor value mismatch for key {:?}",
                expected_key
            );
            status =
                cursor.get(&mut key_entry, &mut data, Get::Next, None).unwrap();
        }
        assert!(
            oracle_iter.next().is_none(),
            "Phase 2: oracle has more records than database after reopen"
        );

        cursor.close().unwrap();
        db2.close().unwrap();
        env2.close().unwrap();
    }
});
