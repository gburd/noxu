#![no_main]

//! Cursor operation fuzz test.
//!
//! Generates random sequences of cursor operations (First, Last, Next, Prev,
//! Search) and verifies:
//! - Cursor always returns keys in sorted order during forward/backward scans.
//! - Search finds keys that exist and returns NotFound for keys that don't.
//! - No panics on any valid cursor operation sequence.

use libfuzzer_sys::fuzz_target;

use arbitrary::Arbitrary;
use noxu_db::{
    DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig, Get,
    OperationStatus,
};
use std::collections::BTreeMap;

const KEYSPACE: u8 = 48;

#[derive(Debug)]
enum CursorOp {
    /// Insert a key-value pair into the database (not a cursor op, but sets up state).
    Insert { key: u8, value: u8 },
    /// Delete a key from the database.
    Remove { key: u8 },
    /// Position cursor at first record.
    First,
    /// Position cursor at last record.
    Last,
    /// Move cursor forward.
    Next,
    /// Move cursor backward.
    Prev,
    /// Search for a specific key.
    Search { key: u8 },
    /// Full forward scan: verify all keys come out in sorted order.
    FullScanForward,
    /// Full backward scan: verify all keys come out in reverse sorted order.
    FullScanBackward,
}

impl<'a> Arbitrary<'a> for CursorOp {
    fn arbitrary(
        u: &mut arbitrary::Unstructured<'a>,
    ) -> arbitrary::Result<Self> {
        let choice: u8 = u.int_in_range(0..=8)?;
        match choice {
            0 => {
                let key: u8 = u.int_in_range(0..=KEYSPACE)?;
                let value: u8 = u.arbitrary()?;
                Ok(CursorOp::Insert { key, value })
            }
            1 => {
                let key: u8 = u.int_in_range(0..=KEYSPACE)?;
                Ok(CursorOp::Remove { key })
            }
            2 => Ok(CursorOp::First),
            3 => Ok(CursorOp::Last),
            4 => Ok(CursorOp::Next),
            5 => Ok(CursorOp::Prev),
            6 => {
                let key: u8 = u.int_in_range(0..=KEYSPACE)?;
                Ok(CursorOp::Search { key })
            }
            7 => Ok(CursorOp::FullScanForward),
            _ => Ok(CursorOp::FullScanBackward),
        }
    }
}

fuzz_target!(|ops: Vec<CursorOp>| {
    if ops.len() > 256 {
        return;
    }

    let tmp_dir = tempfile::TempDir::new().unwrap();
    let env_config = EnvironmentConfig::new(tmp_dir.path().to_path_buf())
        .with_allow_create(true);
    let env = Environment::open(env_config).unwrap();

    let db_config = DatabaseConfig::new().with_allow_create(true);
    let db = env.open_database(None, "fuzz_cursor", &db_config).unwrap();

    let mut oracle: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();

    for op in &ops {
        match op {
            CursorOp::Insert { key, value } => {
                let k = vec![*key];
                let v = vec![*value];
                let key_entry = DatabaseEntry::from_bytes(&k);
                let val_entry = DatabaseEntry::from_bytes(&v);
                db.put(None, &key_entry, &val_entry).unwrap();
                oracle.insert(k, v);
            }

            CursorOp::Remove { key } => {
                let k = vec![*key];
                let key_entry = DatabaseEntry::from_bytes(&k);
                let status = db.delete(None, &key_entry).unwrap();
                if oracle.remove(&k).is_some() {
                    assert_eq!(status, OperationStatus::Success);
                } else {
                    assert_eq!(status, OperationStatus::NotFound);
                }
            }

            CursorOp::First
            | CursorOp::Last
            | CursorOp::Next
            | CursorOp::Prev => {
                // Individual cursor ops: just verify no panic.
                let mut cursor = db.open_cursor(None, None).unwrap();
                let mut key_entry = DatabaseEntry::new();
                let mut data = DatabaseEntry::new();

                let get_type = match op {
                    CursorOp::First => Get::First,
                    CursorOp::Last => Get::Last,
                    CursorOp::Next => Get::Next,
                    CursorOp::Prev => Get::Prev,
                    _ => unreachable!(),
                };

                let _ = cursor.get(&mut key_entry, &mut data, get_type, None);
                cursor.close().unwrap();
            }

            CursorOp::Search { key } => {
                let k = vec![*key];
                let mut cursor = db.open_cursor(None, None).unwrap();
                let mut key_entry = DatabaseEntry::from_bytes(&k);
                let mut data = DatabaseEntry::new();

                let status = cursor
                    .get(&mut key_entry, &mut data, Get::Search, None)
                    .unwrap();

                if oracle.contains_key(&k) {
                    assert_eq!(status, OperationStatus::Success);
                    assert_eq!(
                        data.data_opt().unwrap(),
                        oracle.get(&k).unwrap().as_slice()
                    );
                } else {
                    assert_eq!(status, OperationStatus::NotFound);
                }

                cursor.close().unwrap();
            }

            CursorOp::FullScanForward => {
                let mut cursor = db.open_cursor(None, None).unwrap();
                let mut key_entry = DatabaseEntry::new();
                let mut data = DatabaseEntry::new();

                let mut collected_values: Vec<Vec<u8>> = Vec::new();
                let oracle_values: Vec<Vec<u8>> =
                    oracle.values().cloned().collect();

                let mut status = cursor
                    .get(&mut key_entry, &mut data, Get::First, None)
                    .unwrap();
                while status == OperationStatus::Success {
                    if let Some(d) = data.data_opt() {
                        collected_values.push(d.to_vec());
                    }
                    status = cursor
                        .get(&mut key_entry, &mut data, Get::Next, None)
                        .unwrap();
                }

                assert_eq!(
                    collected_values.len(),
                    oracle_values.len(),
                    "Forward scan count mismatch: got {}, expected {}",
                    collected_values.len(),
                    oracle_values.len()
                );

                // Values should match oracle (which is sorted by key).
                for (i, (got, expected)) in collected_values
                    .iter()
                    .zip(oracle_values.iter())
                    .enumerate()
                {
                    assert_eq!(
                        got, expected,
                        "Forward scan mismatch at position {}",
                        i
                    );
                }

                cursor.close().unwrap();
            }

            CursorOp::FullScanBackward => {
                let mut cursor = db.open_cursor(None, None).unwrap();
                let mut key_entry = DatabaseEntry::new();
                let mut data = DatabaseEntry::new();

                let mut collected_values: Vec<Vec<u8>> = Vec::new();
                let oracle_values_rev: Vec<Vec<u8>> =
                    oracle.values().rev().cloned().collect();

                let mut status = cursor
                    .get(&mut key_entry, &mut data, Get::Last, None)
                    .unwrap();
                while status == OperationStatus::Success {
                    if let Some(d) = data.data_opt() {
                        collected_values.push(d.to_vec());
                    }
                    status = cursor
                        .get(&mut key_entry, &mut data, Get::Prev, None)
                        .unwrap();
                }

                assert_eq!(
                    collected_values.len(),
                    oracle_values_rev.len(),
                    "Backward scan count mismatch: got {}, expected {}",
                    collected_values.len(),
                    oracle_values_rev.len()
                );

                for (i, (got, expected)) in collected_values
                    .iter()
                    .zip(oracle_values_rev.iter())
                    .enumerate()
                {
                    assert_eq!(
                        got, expected,
                        "Backward scan mismatch at position {}",
                        i
                    );
                }

                cursor.close().unwrap();
            }
        }
    }

    db.close().unwrap();
    env.close().unwrap();
});
