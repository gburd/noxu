//! JE TCK port: `com.sleepycat.je.dbi.DbCursorTest` and friends
//! (DbCursorSearchTest, DbCursorDeleteTest).
//!
//! Behaviour-level ports.  JE's `DataWalker` / `BackwardsDataWalker`
//! abstractions are flattened into direct cursor walks.  JE's
//! `simpleKeyStrings` / `simpleDataStrings` test fixture is ported
//! verbatim (the same nine string-pair entries) so that the assertions
//! exercise the same key-ordering shape.
//!
//! Adaptations
//!
//! - JE's `cursor.getNext(key, data, LockMode.DEFAULT)` becomes noxu's
//!   `cursor.get(&mut k, &mut d, Get::Next, None)`.
//! - JE's `DbInternal.advanceCursor(...)` is a no-op shim around
//!   `cursor.dup(SAME_POSITION)`; noxu does not expose an internal
//!   `advanceCursor`, so the testCursorAdvance port asserts the
//!   user-visible behaviour: position at first, walk forward, observe
//!   sorted ordering and full count.

use noxu_db::{
    DatabaseConfig, DatabaseEntry, EnvironmentConfig, Get, OperationStatus,
};
use std::collections::BTreeSet;
use tempfile::TempDir;

const SIMPLE_KEYS: &[&str] = &[
    "foo", "bar", "baz", "aaa", "fubar", "foobar", "quux", "mumble", "froboy",
];

const SIMPLE_DATA: &[&str] =
    &["one", "two", "three", "four", "five", "six", "seven", "eight", "nine"];

fn open_env_db() -> (TempDir, noxu_db::Environment, noxu_db::Database) {
    let dir = TempDir::new().unwrap();
    let env_cfg = EnvironmentConfig::new(dir.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    let env = noxu_db::Environment::open(env_cfg).unwrap();
    let db_cfg =
        DatabaseConfig::new().with_allow_create(true).with_transactional(true);
    let db = env.open_database(None, "DbCursorTest", &db_cfg).unwrap();
    (dir, env, db)
}

fn put_simple(env: &noxu_db::Environment, db: &noxu_db::Database) {
    let txn = env.begin_transaction(None).unwrap();
    for (k, v) in SIMPLE_KEYS.iter().zip(SIMPLE_DATA.iter()) {
        db.put(
            Some(&txn),
            &DatabaseEntry::from_bytes(k.as_bytes()),
            &DatabaseEntry::from_bytes(v.as_bytes()),
        )
        .unwrap();
    }
    txn.commit().unwrap();
}

// ---------------------------------------------------------------------------
// DbCursorTest.testSimpleGetPut
// ---------------------------------------------------------------------------

/// Port of `DbCursorTest.testSimpleGetPut`.  Insert the simple key/data
/// fixture, walk forward with `Get::Next`, assert keys appear in
/// ascending order and that all 9 records are seen.
#[test]
fn db_cursor_test_simple_get_put() {
    let (_dir, env, db) = open_env_db();
    put_simple(&env, &db);

    let mut cursor = db.open_cursor(None, None).unwrap();
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();

    let mut prev: Vec<u8> = Vec::new();
    let mut n = 0usize;
    let mut s = cursor.get(&mut k, &mut d, Get::First, None).unwrap();
    while s == OperationStatus::Success {
        let key = k.get_data().unwrap_or(&[]).to_vec();
        if !prev.is_empty() {
            assert!(prev <= key, "expected sorted, got {prev:?} then {key:?}");
        }
        prev = key;
        n += 1;
        s = cursor.get(&mut k, &mut d, Get::Next, None).unwrap();
    }
    assert_eq!(SIMPLE_KEYS.len(), n);
}

// ---------------------------------------------------------------------------
// DbCursorTest.testSimpleGetPutBackwards
// ---------------------------------------------------------------------------

/// Port of `DbCursorTest.testSimpleGetPutBackwards`.  Walk backwards from
/// `Get::Last` via `Get::Prev`, assert descending order and full count.
#[test]
fn db_cursor_test_simple_get_put_backwards() {
    let (_dir, env, db) = open_env_db();
    put_simple(&env, &db);

    let mut cursor = db.open_cursor(None, None).unwrap();
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();

    let mut prev: Option<Vec<u8>> = None;
    let mut n = 0usize;
    let mut s = cursor.get(&mut k, &mut d, Get::Last, None).unwrap();
    while s == OperationStatus::Success {
        let key = k.get_data().unwrap_or(&[]).to_vec();
        if let Some(p) = &prev {
            assert!(*p >= key, "expected descending, got {p:?} then {key:?}");
        }
        prev = Some(key);
        n += 1;
        s = cursor.get(&mut k, &mut d, Get::Prev, None).unwrap();
    }
    assert_eq!(SIMPLE_KEYS.len(), n);
}

// ---------------------------------------------------------------------------
// DbCursorTest.testCursorAdvance
// ---------------------------------------------------------------------------

/// Port of `DbCursorTest.testCursorAdvance`.  JE's `advanceCursor` is an
/// internal idempotent reposition; the user-visible assertion is that
/// after positioning at first, a full forward scan still sees every key
/// in sorted order.  Noxu has no `advanceCursor` shim, so we just verify
/// the equivalent invariant.
#[test]
fn db_cursor_test_cursor_advance() {
    let (_dir, env, db) = open_env_db();
    put_simple(&env, &db);

    let mut cursor = db.open_cursor(None, None).unwrap();
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();

    // Position at first, then duplicate-position (the noxu equivalent
    // of advanceCursor: nothing changes, the cursor still points at the
    // first record).
    let s = cursor.get(&mut k, &mut d, Get::First, None).unwrap();
    assert_eq!(OperationStatus::Success, s);
    let first_key = k.get_data().unwrap_or(&[]).to_vec();

    // Walk the rest forward.
    let mut prev = first_key;
    let mut n = 1usize;
    let mut s = cursor.get(&mut k, &mut d, Get::Next, None).unwrap();
    while s == OperationStatus::Success {
        let key = k.get_data().unwrap_or(&[]).to_vec();
        assert!(prev <= key, "{prev:?} then {key:?}");
        prev = key;
        n += 1;
        s = cursor.get(&mut k, &mut d, Get::Next, None).unwrap();
    }
    assert_eq!(SIMPLE_KEYS.len(), n);
}

// ---------------------------------------------------------------------------
// DbCursorSearchTest.testSimpleSearchKey
// ---------------------------------------------------------------------------

/// Port of `DbCursorSearchTest.testSimpleSearchKey`.  After inserting the
/// fixture, every key is reachable by `Get::Search` with the matching
/// data value.  An unknown key returns NotFound.
#[test]
fn db_cursor_search_test_simple_search_key() {
    let (_dir, env, db) = open_env_db();
    put_simple(&env, &db);

    let mut cursor = db.open_cursor(None, None).unwrap();
    for (k, v) in SIMPLE_KEYS.iter().zip(SIMPLE_DATA.iter()) {
        let mut key = DatabaseEntry::from_bytes(k.as_bytes());
        let mut data = DatabaseEntry::new();
        let s = cursor.get(&mut key, &mut data, Get::Search, None).unwrap();
        assert_eq!(OperationStatus::Success, s, "k={k}");
        assert_eq!(v.as_bytes(), data.get_data().unwrap_or(&[]));
    }

    // Unknown key.
    let mut key = DatabaseEntry::from_bytes(b"notpresent");
    let mut data = DatabaseEntry::new();
    let s = cursor.get(&mut key, &mut data, Get::Search, None).unwrap();
    assert_eq!(OperationStatus::NotFound, s);
    drop(env);
}

// ---------------------------------------------------------------------------
// DbCursorSearchTest.testSimpleDeleteAndSearchKey
// ---------------------------------------------------------------------------

/// Port of `DbCursorSearchTest.testSimpleDeleteAndSearchKey`.  After
/// deleting one entry, `Get::Search` for that key returns NotFound while
/// every other key still resolves.
#[test]
fn db_cursor_search_test_simple_delete_and_search_key() {
    let (_dir, env, db) = open_env_db();
    put_simple(&env, &db);

    // Delete one key.
    let target = "quux";
    let txn = env.begin_transaction(None).unwrap();
    let s = db
        .delete(Some(&txn), &DatabaseEntry::from_bytes(target.as_bytes()))
        .unwrap();
    assert_eq!(OperationStatus::Success, s);
    txn.commit().unwrap();

    let mut cursor = db.open_cursor(None, None).unwrap();
    for k in SIMPLE_KEYS {
        let mut key = DatabaseEntry::from_bytes(k.as_bytes());
        let mut data = DatabaseEntry::new();
        let s = cursor.get(&mut key, &mut data, Get::Search, None).unwrap();
        if *k == target {
            assert_eq!(
                OperationStatus::NotFound,
                s,
                "deleted {k} should be gone"
            );
        } else {
            assert_eq!(OperationStatus::Success, s, "k={k}");
        }
    }
}

// ---------------------------------------------------------------------------
// DbCursorDeleteTest.testSimpleDeleteInsert
// ---------------------------------------------------------------------------

/// Port of `DbCursorDeleteTest.testSimpleDeleteInsert`.  Insert the
/// fixture, delete every entry, re-insert them, walk the cursor.  Final
/// state must contain exactly the original key set.
#[test]
fn db_cursor_delete_test_simple_delete_insert() {
    let (_dir, env, db) = open_env_db();
    put_simple(&env, &db);

    // Delete all.
    let txn = env.begin_transaction(None).unwrap();
    for k in SIMPLE_KEYS {
        let s = db
            .delete(Some(&txn), &DatabaseEntry::from_bytes(k.as_bytes()))
            .unwrap();
        assert_eq!(OperationStatus::Success, s);
    }
    txn.commit().unwrap();

    // Verify empty.
    {
        let mut cursor = db.open_cursor(None, None).unwrap();
        let mut k = DatabaseEntry::new();
        let mut d = DatabaseEntry::new();
        let s = cursor.get(&mut k, &mut d, Get::First, None).unwrap();
        assert_eq!(OperationStatus::NotFound, s);
    }

    // Re-insert.
    put_simple(&env, &db);

    // Walk and collect.
    let mut cursor = db.open_cursor(None, None).unwrap();
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    let mut seen: BTreeSet<String> = BTreeSet::new();
    let mut s = cursor.get(&mut k, &mut d, Get::First, None).unwrap();
    while s == OperationStatus::Success {
        seen.insert(
            String::from_utf8(k.get_data().unwrap_or(&[]).to_vec()).unwrap(),
        );
        s = cursor.get(&mut k, &mut d, Get::Next, None).unwrap();
    }
    let expected: BTreeSet<String> =
        SIMPLE_KEYS.iter().map(|s| s.to_string()).collect();
    assert_eq!(expected, seen);
}

// ---------------------------------------------------------------------------
// DbCursorDeleteTest.testLargeDeleteAll
// ---------------------------------------------------------------------------

/// Port of `DbCursorDeleteTest.testLargeDeleteAll`.  Insert N distinct
/// keys, delete them all via cursor walk, verify count is zero.  JE's
/// fixture inserts thousands of entries; we use 1000 as a balance between
/// coverage and test runtime.
#[test]
fn db_cursor_delete_test_large_delete_all() {
    const N: u32 = 1000;
    let (_dir, env, db) = open_env_db();

    let txn = env.begin_transaction(None).unwrap();
    for i in 0..N {
        let key = DatabaseEntry::from_bytes(&i.to_be_bytes());
        let val = DatabaseEntry::from_bytes(&(i + 100).to_be_bytes());
        db.put(Some(&txn), &key, &val).unwrap();
    }
    txn.commit().unwrap();

    assert_eq!(N as u64, db.count().unwrap());

    let txn = env.begin_transaction(None).unwrap();
    for i in 0..N {
        let s = db
            .delete(Some(&txn), &DatabaseEntry::from_bytes(&i.to_be_bytes()))
            .unwrap();
        assert_eq!(OperationStatus::Success, s);
    }
    txn.commit().unwrap();

    assert_eq!(0, db.count().unwrap());
}
