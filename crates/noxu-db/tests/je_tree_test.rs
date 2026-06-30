//! JE tree-level invariant ports — split, count, balance, key-prefix.
//!
//! Each test below corresponds to a method in `test/com/sleepycat/je/tree/`.
//! These tests exercise the public-API surface (Database/Cursor) but assert
//! tree-shape invariants (sorted iteration, key-prefix transparency, large
//! key sets surviving splits) that JE asserts at the Tree internal level.

use noxu_db::{
    DatabaseConfig, DatabaseEntry, EnvironmentConfig, Get, OperationStatus,
};
use tempfile::TempDir;

fn open_env_db(
    dir: &TempDir,
    name: &str,
) -> (noxu_db::Environment, noxu_db::Database) {
    let env_cfg = EnvironmentConfig::new(dir.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    let env = noxu_db::Environment::open(env_cfg).unwrap();
    let db_cfg =
        DatabaseConfig::new().with_allow_create(true).with_transactional(true);
    let db = env.open_database(None, name, &db_cfg).unwrap();
    (env, db)
}

// ──────────────────────────────────────────────────────────────────────────────
// SplitTest.test0Split
//
// JE invariant: inserting 16 keys in descending order then 16 in ascending
// order both end up sorted on cursor walk; the splits must preserve order
// invariants.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn split_descending_then_ascending_keys_remain_sorted() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_env_db(&dir, "split_0");

    let txn = env.begin_transaction(None).unwrap();
    // Insert descending: 160, 150, 140, ..., 10
    for i in (10..=160).rev().step_by(10) {
        db.put_in(
            &txn,
            DatabaseEntry::from_bytes(&[i as u8]),
            DatabaseEntry::from_bytes(&[1]),
        )
        .unwrap();
    }
    // Insert ascending: 1, 2, ..., 9
    for i in 1..10u8 {
        db.put_in(
            &txn,
            DatabaseEntry::from_bytes(&[i]),
            DatabaseEntry::from_bytes(&[1]),
        )
        .unwrap();
    }
    txn.commit().unwrap();

    // Cursor walk must yield sorted keys.
    let txn = env.begin_transaction(None).unwrap();
    let mut c = db.open_cursor_in(&txn, None).unwrap();
    let mut prev: Option<u8> = None;
    let mut count = 0usize;
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    let mut s = c.get(&mut k, &mut d, Get::First, None).unwrap();
    while s == OperationStatus::Success {
        let cur = k.get_data().unwrap()[0];
        if let Some(p) = prev {
            assert!(p < cur, "keys must walk in ascending order: {p} < {cur}");
        }
        prev = Some(cur);
        count += 1;
        s = c.get(&mut k, &mut d, Get::Next, None).unwrap();
    }
    assert_eq!(count, 16 + 9);
    drop(c);
    txn.commit().unwrap();
}

// ──────────────────────────────────────────────────────────────────────────────
// TreeTest.testCountAndValidateKeys / testCountAndValidateKeysBackwards
//
// JE invariant: insert N random keys, then walk forward and backward; the
// number of records walked must equal N in both directions, and the keys
// must be sorted.
// ──────────────────────────────────────────────────────────────────────────────

const N_KEYS: u32 = 500;

#[test]
fn tree_count_and_validate_keys_forward() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_env_db(&dir, "count_fwd");

    let txn = env.begin_transaction(None).unwrap();
    // Pseudo-random distinct keys (sorted by hash to scatter).
    let mut keys: Vec<u32> =
        (0..N_KEYS).map(|i| i.wrapping_mul(2_654_435_761)).collect();
    keys.sort();
    keys.dedup();
    let n = keys.len();
    for k in &keys {
        db.put_in(
            &txn,
            DatabaseEntry::from_bytes(&k.to_be_bytes()),
            DatabaseEntry::from_bytes(b"v"),
        )
        .unwrap();
    }
    txn.commit().unwrap();

    let txn = env.begin_transaction(None).unwrap();
    let mut c = db.open_cursor_in(&txn, None).unwrap();
    let mut prev: Option<Vec<u8>> = None;
    let mut count = 0usize;
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    let mut s = c.get(&mut k, &mut d, Get::First, None).unwrap();
    while s == OperationStatus::Success {
        let cur = k.get_data().unwrap().to_vec();
        if let Some(p) = &prev {
            assert!(p < &cur, "forward walk must be sorted");
        }
        prev = Some(cur);
        count += 1;
        s = c.get(&mut k, &mut d, Get::Next, None).unwrap();
    }
    assert_eq!(count, n);
    drop(c);
    txn.commit().unwrap();
}

#[test]
fn tree_count_and_validate_keys_backwards() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_env_db(&dir, "count_bwd");

    let txn = env.begin_transaction(None).unwrap();
    let mut keys: Vec<u32> =
        (0..N_KEYS).map(|i| i.wrapping_mul(2_654_435_761)).collect();
    keys.sort();
    keys.dedup();
    let n = keys.len();
    for k in &keys {
        db.put_in(
            &txn,
            DatabaseEntry::from_bytes(&k.to_be_bytes()),
            DatabaseEntry::from_bytes(b"v"),
        )
        .unwrap();
    }
    txn.commit().unwrap();

    let txn = env.begin_transaction(None).unwrap();
    let mut c = db.open_cursor_in(&txn, None).unwrap();
    let mut prev: Option<Vec<u8>> = None;
    let mut count = 0usize;
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    let mut s = c.get(&mut k, &mut d, Get::Last, None).unwrap();
    while s == OperationStatus::Success {
        let cur = k.get_data().unwrap().to_vec();
        if let Some(p) = &prev {
            assert!(p > &cur, "backward walk must be reverse-sorted");
        }
        prev = Some(cur);
        count += 1;
        s = c.get(&mut k, &mut d, Get::Prev, None).unwrap();
    }
    assert_eq!(count, n);
    drop(c);
    txn.commit().unwrap();
}

// ──────────────────────────────────────────────────────────────────────────────
// TreeTest.testAscendingInsertBalance / testDescendingInsertBalance
//
// JE invariant: ascending and descending insert sequences both produce a
// tree that is fully traversable (forward and backward) with all keys
// visible.  JE additionally asserts the tree depth, but Noxu's
// public API doesn't expose depth — we capture the order/count invariant
// instead.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn tree_ascending_insert_walks_in_order() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_env_db(&dir, "asc_balance");
    let txn = env.begin_transaction(None).unwrap();
    for i in 0..N_KEYS {
        db.put_in(
            &txn,
            DatabaseEntry::from_bytes(&i.to_be_bytes()),
            DatabaseEntry::from_bytes(b""),
        )
        .unwrap();
    }
    txn.commit().unwrap();

    let txn = env.begin_transaction(None).unwrap();
    let mut c = db.open_cursor_in(&txn, None).unwrap();
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    for i in 0..N_KEYS {
        let s = c.get(&mut k, &mut d, Get::Next, None).unwrap();
        assert_eq!(s, OperationStatus::Success);
        let mut a = [0u8; 4];
        a.copy_from_slice(k.get_data().unwrap());
        assert_eq!(u32::from_be_bytes(a), i);
    }
    let s = c.get(&mut k, &mut d, Get::Next, None).unwrap();
    assert_eq!(s, OperationStatus::NotFound);
    drop(c);
    txn.commit().unwrap();
}

#[test]
fn tree_descending_insert_walks_in_order() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_env_db(&dir, "desc_balance");
    let txn = env.begin_transaction(None).unwrap();
    for i in (0..N_KEYS).rev() {
        db.put_in(
            &txn,
            DatabaseEntry::from_bytes(&i.to_be_bytes()),
            DatabaseEntry::from_bytes(b""),
        )
        .unwrap();
    }
    txn.commit().unwrap();

    let txn = env.begin_transaction(None).unwrap();
    let mut c = db.open_cursor_in(&txn, None).unwrap();
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    for i in 0..N_KEYS {
        let s = c.get(&mut k, &mut d, Get::Next, None).unwrap();
        assert_eq!(s, OperationStatus::Success);
        let mut a = [0u8; 4];
        a.copy_from_slice(k.get_data().unwrap());
        assert_eq!(u32::from_be_bytes(a), i);
    }
    drop(c);
    txn.commit().unwrap();
}

// ──────────────────────────────────────────────────────────────────────────────
// KeyPrefixTest.testPrefixBasic (spirit port)
//
// JE invariant: keys with a long shared prefix can be inserted, walked, and
// retrieved correctly — key-prefixing is transparent to the public API.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn key_prefix_basic_long_shared_prefix_round_trip() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_env_db(&dir, "prefix_basic");

    let prefix = b"abcdefghijklmnopqrstuvwxyz0123456789-";
    let mut keys: Vec<Vec<u8>> = Vec::new();
    for i in 0..100u32 {
        let mut k = prefix.to_vec();
        k.extend_from_slice(&i.to_be_bytes());
        keys.push(k);
    }

    let txn = env.begin_transaction(None).unwrap();
    for k in &keys {
        db.put_in(
            &txn,
            DatabaseEntry::from_bytes(k),
            DatabaseEntry::from_bytes(b"v"),
        )
        .unwrap();
    }
    txn.commit().unwrap();

    // Walk: must produce keys in sorted order, count == keys.len().
    let txn = env.begin_transaction(None).unwrap();
    let mut c = db.open_cursor_in(&txn, None).unwrap();
    let mut walked: Vec<Vec<u8>> = Vec::new();
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    let mut s = c.get(&mut k, &mut d, Get::First, None).unwrap();
    while s == OperationStatus::Success {
        walked.push(k.get_data().unwrap().to_vec());
        s = c.get(&mut k, &mut d, Get::Next, None).unwrap();
    }
    let mut sorted = keys.clone();
    sorted.sort();
    assert_eq!(walked, sorted);

    // Random search-by-key works.
    for k in &keys {
        let mut out = DatabaseEntry::new();
        let s = db
            .get_into(Some(&txn), DatabaseEntry::from_bytes(k), &mut out)
            .unwrap();
        assert!(s);
        assert_eq!(out.get_data().unwrap(), b"v");
    }
    drop(c);
    txn.commit().unwrap();
}

// ──────────────────────────────────────────────────────────────────────────────
// KeyPrefixTest.testPrefixManySequential (spirit port)
//
// JE invariant: 1000 sequential u32 keys with a shared prefix all round-trip.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn key_prefix_many_sequential_round_trip() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_env_db(&dir, "prefix_seq");

    let prefix = b"shared-prefix-";
    let txn = env.begin_transaction(None).unwrap();
    for i in 0..1000u32 {
        let mut k = prefix.to_vec();
        k.extend_from_slice(&i.to_be_bytes());
        db.put_in(
            &txn,
            DatabaseEntry::from_bytes(&k),
            DatabaseEntry::from_bytes(&i.to_be_bytes()),
        )
        .unwrap();
    }
    txn.commit().unwrap();

    let txn = env.begin_transaction(None).unwrap();
    let mut c = db.open_cursor_in(&txn, None).unwrap();
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    for i in 0..1000u32 {
        let s = c.get(&mut k, &mut d, Get::Next, None).unwrap();
        assert_eq!(s, OperationStatus::Success);
        let key_bytes = k.get_data().unwrap();
        assert_eq!(&key_bytes[..prefix.len()], prefix);
        let mut a = [0u8; 4];
        a.copy_from_slice(&key_bytes[prefix.len()..]);
        assert_eq!(u32::from_be_bytes(a), i);
    }
    drop(c);
    txn.commit().unwrap();
}
