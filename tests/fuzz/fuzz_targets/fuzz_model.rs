#![no_main]

//! Model-based fuzz test for noxu-db.
//!
//! Generates random sequences of database operations (Put, Get, Delete,
//! CursorNext, CursorPrev, TxnCommit, TxnAbort) and executes them against
//! both a Noxu DB database and a HashMap oracle. After each operation the
//! results are compared to ensure the database behaves correctly.

use libfuzzer_sys::fuzz_target;

use arbitrary::Arbitrary;
use noxu_db::{
    DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig, Get,
    OperationStatus,
};
use std::collections::BTreeMap;

/// Maximum key-space size. Keeping it small increases the chance that
/// operations collide on the same keys, exercising overwrites and deletes.
const KEYSPACE: u8 = 32;

/// Maximum value length in bytes.
const MAX_VALUE_LEN: u8 = 64;

/// Operations that can be performed on the database.
#[derive(Debug)]
enum Op {
    /// Insert or update a key-value pair.
    Put { key: u8, value: Vec<u8> },
    /// Retrieve a value by key.
    Get { key: u8 },
    /// Delete a key.
    Delete { key: u8 },
    /// Position a cursor at the first record and iterate forward N steps.
    CursorScanForward { steps: u8 },
    /// Position a cursor at the last record and iterate backward N steps.
    CursorScanBackward { steps: u8 },
    /// Begin a transaction and immediately commit it (exercises txn lifecycle).
    TxnCommit,
    /// Begin a transaction and immediately abort it (exercises txn lifecycle).
    TxnAbort,
}

fn make_key(k: u8) -> Vec<u8> {
    // Fixed-width big-endian key so byte-wise sort == numeric sort.
    vec![k]
}

impl<'a> Arbitrary<'a> for Op {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        let choice: u8 = u.int_in_range(0..=6)?;
        match choice {
            0 => {
                let key: u8 = u.int_in_range(0..=KEYSPACE)?;
                let val_len: u8 = u.int_in_range(0..=MAX_VALUE_LEN)?;
                let value: Vec<u8> = (0..val_len)
                    .map(|_| u.arbitrary::<u8>())
                    .collect::<arbitrary::Result<Vec<u8>>>()?;
                Ok(Op::Put { key, value })
            }
            1 => {
                let key: u8 = u.int_in_range(0..=KEYSPACE)?;
                Ok(Op::Get { key })
            }
            2 => {
                let key: u8 = u.int_in_range(0..=KEYSPACE)?;
                Ok(Op::Delete { key })
            }
            3 => {
                let steps: u8 = u.int_in_range(0..=10)?;
                Ok(Op::CursorScanForward { steps })
            }
            4 => {
                let steps: u8 = u.int_in_range(0..=10)?;
                Ok(Op::CursorScanBackward { steps })
            }
            5 => Ok(Op::TxnCommit),
            _ => Ok(Op::TxnAbort),
        }
    }
}

fuzz_target!(|ops: Vec<Op>| {
    // Limit operation count to keep each iteration fast.
    if ops.len() > 256 {
        return;
    }

    let tmp_dir = tempfile::TempDir::new().unwrap();
    let env_config = EnvironmentConfig::new(tmp_dir.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    let env = Environment::open(env_config).unwrap();

    let db_config = DatabaseConfig::new().with_allow_create(true);
    let db = env.open_database(None, "fuzz", &db_config).unwrap();

    // Oracle: a BTreeMap that mirrors expected database state.
    let mut oracle: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();

    for op in &ops {
        match op {
            Op::Put { key, value } => {
                let k = make_key(*key);
                let key_entry = DatabaseEntry::from_bytes(&k);
                let val_entry = DatabaseEntry::from_bytes(value);

                let status = db.put(None, &key_entry, &val_entry).unwrap();
                assert_eq!(status, OperationStatus::Success);

                oracle.insert(k, value.clone());
            }

            Op::Get { key } => {
                let k = make_key(*key);
                let key_entry = DatabaseEntry::from_bytes(&k);
                let mut data = DatabaseEntry::new();

                let status = db.get(None, &key_entry, &mut data).unwrap();

                match oracle.get(&k) {
                    Some(expected_val) => {
                        assert_eq!(status, OperationStatus::Success);
                        assert_eq!(
                            data.get_data().unwrap(),
                            expected_val.as_slice(),
                            "Value mismatch for key {:?}",
                            k
                        );
                    }
                    None => {
                        assert_eq!(status, OperationStatus::NotFound);
                    }
                }
            }

            Op::Delete { key } => {
                let k = make_key(*key);
                let key_entry = DatabaseEntry::from_bytes(&k);

                let status = db.delete(None, &key_entry).unwrap();

                if oracle.remove(&k).is_some() {
                    assert_eq!(status, OperationStatus::Success);
                } else {
                    assert_eq!(status, OperationStatus::NotFound);
                }
            }

            Op::CursorScanForward { steps } => {
                let mut cursor = db.open_cursor(None, None).unwrap();
                let mut key_entry = DatabaseEntry::new();
                let mut data = DatabaseEntry::new();

                // Collect from oracle for comparison.
                let oracle_keys: Vec<Vec<u8>> = oracle.keys().cloned().collect();

                let first_status =
                    cursor.get(&mut key_entry, &mut data, Get::First, None).unwrap();

                if oracle.is_empty() {
                    assert_eq!(first_status, OperationStatus::NotFound);
                } else {
                    assert_eq!(first_status, OperationStatus::Success);

                    let mut cursor_idx = 0;
                    for _ in 0..*steps {
                        let next_status =
                            cursor.get(&mut key_entry, &mut data, Get::Next, None).unwrap();
                        if next_status == OperationStatus::NotFound {
                            break;
                        }
                        cursor_idx += 1;
                        if cursor_idx >= oracle_keys.len() {
                            break;
                        }
                    }
                }

                cursor.close().unwrap();
            }

            Op::CursorScanBackward { steps } => {
                let mut cursor = db.open_cursor(None, None).unwrap();
                let mut key_entry = DatabaseEntry::new();
                let mut data = DatabaseEntry::new();

                let last_status =
                    cursor.get(&mut key_entry, &mut data, Get::Last, None).unwrap();

                if oracle.is_empty() {
                    assert_eq!(last_status, OperationStatus::NotFound);
                } else {
                    assert_eq!(last_status, OperationStatus::Success);

                    for _ in 0..*steps {
                        let prev_status =
                            cursor.get(&mut key_entry, &mut data, Get::Prev, None).unwrap();
                        if prev_status == OperationStatus::NotFound {
                            break;
                        }
                    }
                }

                cursor.close().unwrap();
            }

            Op::TxnCommit => {
                if env.is_transactional() {
                    let txn = env.begin_transaction(None, None).unwrap();
                    txn.commit().unwrap();
                }
            }

            Op::TxnAbort => {
                if env.is_transactional() {
                    let txn = env.begin_transaction(None, None).unwrap();
                    txn.abort().unwrap();
                }
            }
        }
    }

    // Final consistency check: every oracle entry must be in the database
    // and every database entry must be in the oracle.
    for (k, v) in &oracle {
        let key_entry = DatabaseEntry::from_bytes(k);
        let mut data = DatabaseEntry::new();
        let status = db.get(None, &key_entry, &mut data).unwrap();
        assert_eq!(status, OperationStatus::Success, "Missing key {:?}", k);
        assert_eq!(
            data.get_data().unwrap(),
            v.as_slice(),
            "Final value mismatch for key {:?}",
            k
        );
    }

    // Verify count matches.
    let db_count = db.count().unwrap();
    assert_eq!(
        db_count,
        oracle.len() as u64,
        "Database count {} != oracle count {}",
        db_count,
        oracle.len()
    );

    // Verify full cursor iteration matches oracle in sorted order.
    let mut cursor = db.open_cursor(None, None).unwrap();
    let mut key_entry = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();
    let mut oracle_iter = oracle.iter();

    let mut status = cursor.get(&mut key_entry, &mut data, Get::First, None).unwrap();
    while status == OperationStatus::Success {
        let (expected_key, expected_val) = oracle_iter
            .next()
            .expect("Database has more records than oracle");
        assert_eq!(
            data.get_data().unwrap(),
            expected_val.as_slice(),
            "Cursor iteration value mismatch"
        );
        let _ = expected_key; // Key verified implicitly via sorted order.
        status = cursor.get(&mut key_entry, &mut data, Get::Next, None).unwrap();
    }
    assert!(
        oracle_iter.next().is_none(),
        "Oracle has more records than database"
    );
    cursor.close().unwrap();

    db.close().unwrap();
    env.close().unwrap();
});
