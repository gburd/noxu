#![no_main]

//! Transaction ACID fuzz test for noxu-db.
//!
//! Generates random sequences of database operations that include explicit
//! transaction management: begin, put/get/delete under a txn, then either
//! commit or abort. A HashMap oracle tracks the committed state and verifies:
//!
//! - Committed transactions are durable: data is readable after commit.
//! - Aborted transactions leave no traces: data written under an aborted txn
//!   is not visible after the abort.
//! - Non-transactional puts (txn = None) are immediately visible.

use libfuzzer_sys::fuzz_target;

use arbitrary::Arbitrary;
use noxu_db::{
    DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig, OperationStatus,
};
use std::collections::HashMap;

/// Maximum key-space size. Keeping it small raises the probability that
/// operations interact on the same keys, exercising overwrites and conflicts.
const KEYSPACE: u8 = 16;

/// Maximum value length in bytes.
const MAX_VALUE_LEN: u8 = 32;

/// Operations the fuzzer can generate.
#[derive(Debug)]
enum TxnOp {
    /// Put a key/value without a transaction (auto-committed).
    PutDirect { key: u8, value: Vec<u8> },
    /// Delete a key without a transaction (auto-committed).
    DeleteDirect { key: u8 },
    /// Get a key without a transaction and verify against oracle.
    GetDirect { key: u8 },
    /// Put a key/value inside a transaction then commit it.
    PutCommit { key: u8, value: Vec<u8> },
    /// Put a key/value inside a transaction then abort it.
    PutAbort { key: u8, value: Vec<u8> },
    /// Delete a key inside a transaction then commit.
    DeleteCommit { key: u8 },
    /// Delete a key inside a transaction then abort.
    DeleteAbort { key: u8 },
    /// Open/commit an empty transaction (exercises txn lifecycle).
    EmptyCommit,
    /// Open/abort an empty transaction (exercises txn lifecycle).
    EmptyAbort,
}

fn make_key(k: u8) -> Vec<u8> {
    // Single-byte key; byte-wise sort equals numeric sort for u8.
    vec![k]
}

impl<'a> Arbitrary<'a> for TxnOp {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        let choice: u8 = u.int_in_range(0..=8)?;
        match choice {
            0 => {
                let key: u8 = u.int_in_range(0..=KEYSPACE)?;
                let len: u8 = u.int_in_range(0..=MAX_VALUE_LEN)?;
                let value: Vec<u8> = (0..len)
                    .map(|_| u.arbitrary::<u8>())
                    .collect::<arbitrary::Result<Vec<u8>>>()?;
                Ok(TxnOp::PutDirect { key, value })
            }
            1 => {
                let key: u8 = u.int_in_range(0..=KEYSPACE)?;
                Ok(TxnOp::DeleteDirect { key })
            }
            2 => {
                let key: u8 = u.int_in_range(0..=KEYSPACE)?;
                Ok(TxnOp::GetDirect { key })
            }
            3 => {
                let key: u8 = u.int_in_range(0..=KEYSPACE)?;
                let len: u8 = u.int_in_range(0..=MAX_VALUE_LEN)?;
                let value: Vec<u8> = (0..len)
                    .map(|_| u.arbitrary::<u8>())
                    .collect::<arbitrary::Result<Vec<u8>>>()?;
                Ok(TxnOp::PutCommit { key, value })
            }
            4 => {
                let key: u8 = u.int_in_range(0..=KEYSPACE)?;
                let len: u8 = u.int_in_range(0..=MAX_VALUE_LEN)?;
                let value: Vec<u8> = (0..len)
                    .map(|_| u.arbitrary::<u8>())
                    .collect::<arbitrary::Result<Vec<u8>>>()?;
                Ok(TxnOp::PutAbort { key, value })
            }
            5 => {
                let key: u8 = u.int_in_range(0..=KEYSPACE)?;
                Ok(TxnOp::DeleteCommit { key })
            }
            6 => {
                let key: u8 = u.int_in_range(0..=KEYSPACE)?;
                Ok(TxnOp::DeleteAbort { key })
            }
            7 => Ok(TxnOp::EmptyCommit),
            _ => Ok(TxnOp::EmptyAbort),
        }
    }
}

fuzz_target!(|ops: Vec<TxnOp>| {
    // Bound iteration count to keep each fuzz case fast.
    if ops.len() > 128 {
        return;
    }

    let tmp_dir = tempfile::TempDir::new().unwrap();
    let env_config = EnvironmentConfig::new(tmp_dir.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    let env = Environment::open(env_config).unwrap();

    let db_config = DatabaseConfig::new().with_allow_create(true);
    let db = env.open_database(None, "fuzz_txn", &db_config).unwrap();

    // Oracle: mirrors only the committed/visible state of the database.
    let mut oracle: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();

    for op in &ops {
        match op {
            // ----------------------------------------------------------------
            // Direct (non-transactional) operations
            // ----------------------------------------------------------------
            TxnOp::PutDirect { key, value } => {
                let k = make_key(*key);
                let key_entry = DatabaseEntry::from_bytes(&k);
                let val_entry = DatabaseEntry::from_bytes(value);
                let status = db.put(None, &key_entry, &val_entry).unwrap();
                assert_eq!(status, OperationStatus::Success);
                oracle.insert(k, value.clone());
            }

            TxnOp::DeleteDirect { key } => {
                let k = make_key(*key);
                let key_entry = DatabaseEntry::from_bytes(&k);
                let status = db.delete(None, &key_entry).unwrap();
                if oracle.remove(&k).is_some() {
                    assert_eq!(status, OperationStatus::Success);
                } else {
                    assert_eq!(status, OperationStatus::NotFound);
                }
            }

            TxnOp::GetDirect { key } => {
                let k = make_key(*key);
                let key_entry = DatabaseEntry::from_bytes(&k);
                let mut data = DatabaseEntry::new();
                let status = db.get(None, &key_entry, &mut data).unwrap();
                match oracle.get(&k) {
                    Some(expected) => {
                        assert_eq!(
                            status,
                            OperationStatus::Success,
                            "Expected key {:?} to be present",
                            k
                        );
                        assert_eq!(
                            data.get_data().unwrap(),
                            expected.as_slice(),
                            "Value mismatch for key {:?}",
                            k
                        );
                    }
                    None => {
                        assert_eq!(
                            status,
                            OperationStatus::NotFound,
                            "Expected key {:?} to be absent",
                            k
                        );
                    }
                }
            }

            // ----------------------------------------------------------------
            // Transactional put then COMMIT — data must be visible afterwards.
            // ----------------------------------------------------------------
            TxnOp::PutCommit { key, value } => {
                let k = make_key(*key);

                if env.is_transactional() {
                    let txn = env.begin_transaction(None, None).unwrap();
                    let key_entry = DatabaseEntry::from_bytes(&k);
                    let val_entry = DatabaseEntry::from_bytes(value);
                    // Write under the transaction.
                    let status = db.put(Some(&txn), &key_entry, &val_entry).unwrap();
                    assert_eq!(status, OperationStatus::Success);
                    txn.commit().unwrap();

                    // After commit the data must be readable.
                    let mut data = DatabaseEntry::new();
                    let read_status = db.get(None, &key_entry, &mut data).unwrap();
                    assert_eq!(
                        read_status,
                        OperationStatus::Success,
                        "Committed put for key {:?} not visible",
                        k
                    );
                    assert_eq!(
                        data.get_data().unwrap(),
                        value.as_slice(),
                        "Committed put value mismatch for key {:?}",
                        k
                    );

                    oracle.insert(k, value.clone());
                } else {
                    // Non-transactional environment: fall back to direct put.
                    let key_entry = DatabaseEntry::from_bytes(&k);
                    let val_entry = DatabaseEntry::from_bytes(value);
                    db.put(None, &key_entry, &val_entry).unwrap();
                    oracle.insert(k, value.clone());
                }
            }

            // ----------------------------------------------------------------
            // Transactional put then ABORT — data must NOT be visible.
            // ----------------------------------------------------------------
            TxnOp::PutAbort { key, value } => {
                let k = make_key(*key);

                if env.is_transactional() {
                    // Remember pre-abort state so we can verify no change.
                    let pre_abort_value = oracle.get(&k).cloned();

                    let txn = env.begin_transaction(None, None).unwrap();
                    let key_entry = DatabaseEntry::from_bytes(&k);
                    let val_entry = DatabaseEntry::from_bytes(value);
                    db.put(Some(&txn), &key_entry, &val_entry).unwrap();
                    txn.abort().unwrap();

                    // After abort the visible state must equal pre-abort state.
                    let mut data = DatabaseEntry::new();
                    let read_status = db.get(None, &key_entry, &mut data).unwrap();
                    match pre_abort_value {
                        Some(ref expected) => {
                            // Key existed before; aborted write must not change it.
                            assert_eq!(
                                read_status,
                                OperationStatus::Success,
                                "Aborted put erased existing key {:?}",
                                k
                            );
                            assert_eq!(
                                data.get_data().unwrap(),
                                expected.as_slice(),
                                "Aborted put changed existing value for key {:?}",
                                k
                            );
                        }
                        None => {
                            // Key did not exist before; aborted write must not create it.
                            assert_eq!(
                                read_status,
                                OperationStatus::NotFound,
                                "Aborted put created key {:?}",
                                k
                            );
                        }
                    }
                    // Oracle unchanged — aborted txn has no effect.
                }
                // Non-transactional environment: skip; abort has no meaning.
            }

            // ----------------------------------------------------------------
            // Transactional delete then COMMIT — key must be gone afterwards.
            // ----------------------------------------------------------------
            TxnOp::DeleteCommit { key } => {
                let k = make_key(*key);

                if env.is_transactional() {
                    let txn = env.begin_transaction(None, None).unwrap();
                    let key_entry = DatabaseEntry::from_bytes(&k);
                    let status = db.delete(Some(&txn), &key_entry).unwrap();

                    if oracle.contains_key(&k) {
                        assert_eq!(status, OperationStatus::Success);
                    } else {
                        assert_eq!(status, OperationStatus::NotFound);
                    }
                    txn.commit().unwrap();

                    // After commit the key must be absent.
                    let mut data = DatabaseEntry::new();
                    let read_status = db.get(None, &key_entry, &mut data).unwrap();
                    assert_eq!(
                        read_status,
                        OperationStatus::NotFound,
                        "Committed delete for key {:?} still visible",
                        k
                    );

                    oracle.remove(&k);
                } else {
                    let key_entry = DatabaseEntry::from_bytes(&k);
                    let status = db.delete(None, &key_entry).unwrap();
                    if oracle.remove(&k).is_some() {
                        assert_eq!(status, OperationStatus::Success);
                    } else {
                        assert_eq!(status, OperationStatus::NotFound);
                    }
                }
            }

            // ----------------------------------------------------------------
            // Transactional delete then ABORT — key visibility must be unchanged.
            // ----------------------------------------------------------------
            TxnOp::DeleteAbort { key } => {
                let k = make_key(*key);

                if env.is_transactional() {
                    let pre_abort_value = oracle.get(&k).cloned();

                    let txn = env.begin_transaction(None, None).unwrap();
                    let key_entry = DatabaseEntry::from_bytes(&k);
                    db.delete(Some(&txn), &key_entry).unwrap();
                    txn.abort().unwrap();

                    // Visibility must be unchanged.
                    let mut data = DatabaseEntry::new();
                    let read_status = db.get(None, &key_entry, &mut data).unwrap();
                    match pre_abort_value {
                        Some(ref expected) => {
                            assert_eq!(
                                read_status,
                                OperationStatus::Success,
                                "Aborted delete removed key {:?}",
                                k
                            );
                            assert_eq!(
                                data.get_data().unwrap(),
                                expected.as_slice(),
                                "Aborted delete changed value for key {:?}",
                                k
                            );
                        }
                        None => {
                            assert_eq!(
                                read_status,
                                OperationStatus::NotFound,
                                "Aborted delete on absent key {:?} created it",
                                k
                            );
                        }
                    }
                    // Oracle unchanged.
                }
            }

            // ----------------------------------------------------------------
            // Empty transactions (lifecycle only)
            // ----------------------------------------------------------------
            TxnOp::EmptyCommit => {
                if env.is_transactional() {
                    let txn = env.begin_transaction(None, None).unwrap();
                    txn.commit().unwrap();
                }
            }

            TxnOp::EmptyAbort => {
                if env.is_transactional() {
                    let txn = env.begin_transaction(None, None).unwrap();
                    txn.abort().unwrap();
                }
            }
        }
    }

    // ------------------------------------------------------------------
    // Final consistency check: oracle must match the database exactly.
    // ------------------------------------------------------------------
    for (k, v) in &oracle {
        let key_entry = DatabaseEntry::from_bytes(k);
        let mut data = DatabaseEntry::new();
        let status = db.get(None, &key_entry, &mut data).unwrap();
        assert_eq!(
            status,
            OperationStatus::Success,
            "Final check: committed key {:?} missing from database",
            k
        );
        assert_eq!(
            data.get_data().unwrap(),
            v.as_slice(),
            "Final check: value mismatch for committed key {:?}",
            k
        );
    }

    let db_count = db.count().unwrap();
    assert_eq!(
        db_count,
        oracle.len() as u64,
        "Final check: database count {} != oracle count {}",
        db_count,
        oracle.len()
    );

    db.close().unwrap();
    env.close().unwrap();
});
