//! CursorTest — JE JCK cursor API tests ported to Rust.
//!
//! Covers: cursor lifecycle (open/close/state), get operations (First, Last,
//! Next, Prev, Search, SearchGte, Current), put via cursor (Overwrite,
//! NoOverwrite, Current), cursor delete, empty-DB behavior, range scan,
//! cursor count(), key order verification, large-batch iteration.

use noxu_db::cursor::CursorState;
use noxu_db::{DatabaseConfig, DatabaseEntry, EnvironmentConfig, Get, OperationStatus, Put};
use tempfile::TempDir;

// ─── helpers ──────────────────────────────────────────────────────────────────

fn open_env_and_db(dir: &TempDir) -> (noxu_db::Environment, noxu_db::Database) {
    let env_config = EnvironmentConfig::new(dir.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    let env = noxu_db::Environment::open(env_config).unwrap();
    let db_config = DatabaseConfig::new().with_allow_create(true);
    let db = env.open_database(None, "cursor_test_db", &db_config).unwrap();
    (env, db)
}

fn kv(k: &[u8], v: &[u8]) -> (DatabaseEntry, DatabaseEntry) {
    (DatabaseEntry::from_bytes(k), DatabaseEntry::from_bytes(v))
}

fn put_batch(db: &noxu_db::Database, pairs: &[(&[u8], &[u8])]) {
    for (k, v) in pairs {
        let (key, val) = kv(k, v);
        db.put(None, &key, &val).unwrap();
    }
}

// ─── 1. Cursor lifecycle ──────────────────────────────────────────────────────

#[test]
fn cursor_initial_state_not_initialized() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_env_and_db(&dir);
    let cursor = db.open_cursor(None, None).unwrap();
    assert_eq!(cursor.get_state(), CursorState::NotInitialized);
}

#[test]
fn cursor_is_valid_before_positioning() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_env_and_db(&dir);
    let cursor = db.open_cursor(None, None).unwrap();
    assert!(cursor.is_valid());
}

#[test]
fn cursor_is_read_write_by_default() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_env_and_db(&dir);
    let cursor = db.open_cursor(None, None).unwrap();
    assert!(!cursor.is_read_only());
}

#[test]
fn cursor_state_initialized_after_first_get() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_env_and_db(&dir);
    put_batch(&db, &[(b"k", b"v")]);
    let mut cursor = db.open_cursor(None, None).unwrap();
    let mut key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();
    cursor.get(&mut key, &mut data, Get::First, None).unwrap();
    assert_eq!(cursor.get_state(), CursorState::Initialized);
}

#[test]
fn cursor_state_closed_after_close() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_env_and_db(&dir);
    let mut cursor = db.open_cursor(None, None).unwrap();
    cursor.close().unwrap();
    assert_eq!(cursor.get_state(), CursorState::Closed);
}

// ─── 2. Empty-database behavior ───────────────────────────────────────────────

#[test]
fn cursor_first_on_empty_db_returns_not_found() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_env_and_db(&dir);
    let mut cursor = db.open_cursor(None, None).unwrap();
    let mut key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();
    let status = cursor.get(&mut key, &mut data, Get::First, None).unwrap();
    assert_eq!(status, OperationStatus::NotFound);
}

#[test]
fn cursor_last_on_empty_db_returns_not_found() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_env_and_db(&dir);
    let mut cursor = db.open_cursor(None, None).unwrap();
    let mut key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();
    let status = cursor.get(&mut key, &mut data, Get::Last, None).unwrap();
    assert_eq!(status, OperationStatus::NotFound);
}

// ─── 3. Cursor get: First, Last, Next, Prev ───────────────────────────────────

#[test]
fn cursor_first_and_last_single_record() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_env_and_db(&dir);
    put_batch(&db, &[(b"only", b"value")]);

    let mut cursor = db.open_cursor(None, None).unwrap();
    let mut key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();

    let s = cursor.get(&mut key, &mut data, Get::First, None).unwrap();
    assert_eq!(s, OperationStatus::Success);
    assert_eq!(key.data(), b"only");

    let s = cursor.get(&mut key, &mut data, Get::Last, None).unwrap();
    assert_eq!(s, OperationStatus::Success);
    assert_eq!(key.data(), b"only");
}

#[test]
fn cursor_next_at_end_returns_not_found() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_env_and_db(&dir);
    put_batch(&db, &[(b"a", b"1")]);
    let mut cursor = db.open_cursor(None, None).unwrap();
    let mut key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();
    cursor.get(&mut key, &mut data, Get::First, None).unwrap();
    let s = cursor.get(&mut key, &mut data, Get::Next, None).unwrap();
    assert_eq!(s, OperationStatus::NotFound);
}

#[test]
fn cursor_prev_at_beginning_returns_not_found() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_env_and_db(&dir);
    put_batch(&db, &[(b"a", b"1")]);
    let mut cursor = db.open_cursor(None, None).unwrap();
    let mut key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();
    cursor.get(&mut key, &mut data, Get::First, None).unwrap();
    let s = cursor.get(&mut key, &mut data, Get::Prev, None).unwrap();
    assert_eq!(s, OperationStatus::NotFound);
}

#[test]
fn cursor_iterates_all_keys_forward() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_env_and_db(&dir);
    put_batch(&db, &[(b"a", b"1"), (b"b", b"2"), (b"c", b"3")]);

    let mut cursor = db.open_cursor(None, None).unwrap();
    let mut key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();

    let mut collected = Vec::new();
    let mut s = cursor.get(&mut key, &mut data, Get::First, None).unwrap();
    while s == OperationStatus::Success {
        collected.push(key.data().to_vec());
        s = cursor.get(&mut key, &mut data, Get::Next, None).unwrap();
    }
    assert_eq!(collected, vec![b"a", b"b", b"c"]);
}

#[test]
fn cursor_iterates_all_keys_backward() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_env_and_db(&dir);
    put_batch(&db, &[(b"a", b"1"), (b"b", b"2"), (b"c", b"3")]);

    let mut cursor = db.open_cursor(None, None).unwrap();
    let mut key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();

    let mut collected = Vec::new();
    let mut s = cursor.get(&mut key, &mut data, Get::Last, None).unwrap();
    while s == OperationStatus::Success {
        collected.push(key.data().to_vec());
        s = cursor.get(&mut key, &mut data, Get::Prev, None).unwrap();
    }
    assert_eq!(collected, vec![b"c", b"b", b"a"]);
}

// ─── 4. Cursor get: Search and SearchGte ─────────────────────────────────────

#[test]
fn cursor_search_exact_key() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_env_and_db(&dir);
    put_batch(&db, &[(b"aaa", b"v1"), (b"bbb", b"v2"), (b"ccc", b"v3")]);

    let mut cursor = db.open_cursor(None, None).unwrap();
    let mut key = DatabaseEntry::from_bytes(b"bbb");
    let mut data = DatabaseEntry::new();
    let s = cursor.get(&mut key, &mut data, Get::Search, None).unwrap();
    assert_eq!(s, OperationStatus::Success);
    assert_eq!(data.data(), b"v2");
}

#[test]
fn cursor_search_missing_key_returns_not_found() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_env_and_db(&dir);
    put_batch(&db, &[(b"a", b"1"), (b"c", b"3")]);

    let mut cursor = db.open_cursor(None, None).unwrap();
    let mut key = DatabaseEntry::from_bytes(b"b");
    let mut data = DatabaseEntry::new();
    let s = cursor.get(&mut key, &mut data, Get::Search, None).unwrap();
    assert_eq!(s, OperationStatus::NotFound);
}

#[test]
fn cursor_search_gte_positions_at_or_after() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_env_and_db(&dir);
    put_batch(&db, &[(b"aaa", b"v1"), (b"ccc", b"v3"), (b"eee", b"v5")]);

    // Search for "bbb" which doesn't exist → should land on "ccc".
    let mut cursor = db.open_cursor(None, None).unwrap();
    let mut key = DatabaseEntry::from_bytes(b"bbb");
    let mut data = DatabaseEntry::new();
    let s = cursor.get(&mut key, &mut data, Get::SearchGte, None).unwrap();
    assert_eq!(s, OperationStatus::Success);
    assert_eq!(key.data(), b"ccc");
}

#[test]
fn cursor_search_gte_exact_key_matches() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_env_and_db(&dir);
    put_batch(&db, &[(b"a", b"1"), (b"b", b"2")]);

    let mut cursor = db.open_cursor(None, None).unwrap();
    let mut key = DatabaseEntry::from_bytes(b"a");
    let mut data = DatabaseEntry::new();
    let s = cursor.get(&mut key, &mut data, Get::SearchGte, None).unwrap();
    assert_eq!(s, OperationStatus::Success);
    assert_eq!(key.data(), b"a");
}

// ─── 5. Cursor get: Current ───────────────────────────────────────────────────

#[test]
fn cursor_current_returns_current_record() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_env_and_db(&dir);
    put_batch(&db, &[(b"k1", b"v1"), (b"k2", b"v2")]);

    let mut cursor = db.open_cursor(None, None).unwrap();
    let mut key = DatabaseEntry::from_bytes(b"k1");
    let mut data = DatabaseEntry::new();
    cursor.get(&mut key, &mut data, Get::Search, None).unwrap();
    // Now re-read via Current.
    let mut key2 = DatabaseEntry::new();
    let mut data2 = DatabaseEntry::new();
    let s = cursor.get(&mut key2, &mut data2, Get::Current, None).unwrap();
    assert_eq!(s, OperationStatus::Success);
    assert_eq!(key2.data(), b"k1");
    assert_eq!(data2.data(), b"v1");
}

// ─── 6. Cursor put ────────────────────────────────────────────────────────────

#[test]
fn cursor_put_overwrite_inserts_record() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_env_and_db(&dir);
    let mut cursor = db.open_cursor(None, None).unwrap();
    let key = DatabaseEntry::from_bytes(b"new_key");
    let val = DatabaseEntry::from_bytes(b"new_val");
    let s = cursor.put(&key, &val, Put::Overwrite).unwrap();
    assert_eq!(s, OperationStatus::Success);
    cursor.close().unwrap();

    let mut out = DatabaseEntry::new();
    db.get(None, &key, &mut out).unwrap();
    assert_eq!(out.data(), b"new_val");
}

#[test]
fn cursor_put_no_overwrite_returns_key_exists() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_env_and_db(&dir);
    let key = DatabaseEntry::from_bytes(b"k");
    let v1 = DatabaseEntry::from_bytes(b"v1");
    db.put(None, &key, &v1).unwrap();

    let mut cursor = db.open_cursor(None, None).unwrap();
    let v2 = DatabaseEntry::from_bytes(b"v2");
    let s = cursor.put(&key, &v2, Put::NoOverwrite).unwrap();
    assert_eq!(s, OperationStatus::KeyExists);
}

#[test]
fn cursor_put_current_updates_current_record() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_env_and_db(&dir);
    let key = DatabaseEntry::from_bytes(b"k");
    let v1 = DatabaseEntry::from_bytes(b"original");
    db.put(None, &key, &v1).unwrap();

    let mut cursor = db.open_cursor(None, None).unwrap();
    let mut search_key = DatabaseEntry::from_bytes(b"k");
    let mut data = DatabaseEntry::new();
    cursor.get(&mut search_key, &mut data, Get::Search, None).unwrap();

    let new_val = DatabaseEntry::from_bytes(b"updated");
    let s = cursor.put(&key, &new_val, Put::Current).unwrap();
    assert_eq!(s, OperationStatus::Success);
    cursor.close().unwrap();

    let mut out = DatabaseEntry::new();
    db.get(None, &key, &mut out).unwrap();
    assert_eq!(out.data(), b"updated");
}

// ─── 7. Cursor delete ─────────────────────────────────────────────────────────

#[test]
fn cursor_delete_removes_current_record() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_env_and_db(&dir);
    let key = DatabaseEntry::from_bytes(b"to_delete");
    let val = DatabaseEntry::from_bytes(b"v");
    db.put(None, &key, &val).unwrap();

    let mut cursor = db.open_cursor(None, None).unwrap();
    let mut search_key = DatabaseEntry::from_bytes(b"to_delete");
    let mut data = DatabaseEntry::new();
    cursor.get(&mut search_key, &mut data, Get::Search, None).unwrap();
    cursor.delete().unwrap();
    cursor.close().unwrap();

    let mut out = DatabaseEntry::new();
    let s = db.get(None, &key, &mut out).unwrap();
    assert_eq!(s, OperationStatus::NotFound);
}

#[test]
fn cursor_delete_middle_record_leaves_others() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_env_and_db(&dir);
    put_batch(&db, &[(b"a", b"1"), (b"b", b"2"), (b"c", b"3")]);

    let mut cursor = db.open_cursor(None, None).unwrap();
    let mut key = DatabaseEntry::from_bytes(b"b");
    let mut data = DatabaseEntry::new();
    cursor.get(&mut key, &mut data, Get::Search, None).unwrap();
    cursor.delete().unwrap();
    cursor.close().unwrap();

    assert_eq!(db.count().unwrap(), 2);
}

// ─── 8. Cursor count ──────────────────────────────────────────────────────────

#[test]
fn cursor_count_zero_before_positioning() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_env_and_db(&dir);
    let cursor = db.open_cursor(None, None).unwrap();
    assert_eq!(cursor.count().unwrap(), 0);
}

#[test]
fn cursor_count_one_after_positioning() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_env_and_db(&dir);
    put_batch(&db, &[(b"k", b"v")]);
    let mut cursor = db.open_cursor(None, None).unwrap();
    let mut key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();
    cursor.get(&mut key, &mut data, Get::First, None).unwrap();
    assert_eq!(cursor.count().unwrap(), 1);
}

// ─── 9. Large-batch iteration ─────────────────────────────────────────────────

#[test]
fn cursor_iterates_100_records_in_order() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_env_and_db(&dir);

    // Insert 100 zero-padded keys in random order.
    let mut keys: Vec<u32> = (0..100).collect();
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    keys.sort_by_key(|k| {
        let mut h = DefaultHasher::new();
        k.hash(&mut h);
        h.finish()
    });
    for i in &keys {
        let key = format!("{:04}", i);
        let val = format!("{}", i);
        db.put(
            None,
            &DatabaseEntry::from_bytes(key.as_bytes()),
            &DatabaseEntry::from_bytes(val.as_bytes()),
        )
        .unwrap();
    }

    // Iterate forward and verify sorted order.
    let mut cursor = db.open_cursor(None, None).unwrap();
    let mut key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();
    let mut count = 0u32;
    let mut prev_key: Option<Vec<u8>> = None;
    let mut s = cursor.get(&mut key, &mut data, Get::First, None).unwrap();
    while s == OperationStatus::Success {
        if let Some(ref pk) = prev_key {
            assert!(key.data() > pk.as_slice(), "keys must be strictly increasing");
        }
        prev_key = Some(key.data().to_vec());
        count += 1;
        s = cursor.get(&mut key, &mut data, Get::Next, None).unwrap();
    }
    assert_eq!(count, 100);
}

// ─── 10. Key order: sorted lexicographic ─────────────────────────────────────

#[test]
fn cursor_keys_returned_in_lexicographic_order() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_env_and_db(&dir);
    // These are intentionally out of insertion order.
    put_batch(
        &db,
        &[
            (b"zz", b"z"),
            (b"aa", b"a"),
            (b"mm", b"m"),
            (b"bb", b"b"),
            (b"yy", b"y"),
        ],
    );

    let mut cursor = db.open_cursor(None, None).unwrap();
    let mut key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();
    let mut keys: Vec<Vec<u8>> = Vec::new();
    let mut s = cursor.get(&mut key, &mut data, Get::First, None).unwrap();
    while s == OperationStatus::Success {
        keys.push(key.data().to_vec());
        s = cursor.get(&mut key, &mut data, Get::Next, None).unwrap();
    }
    let mut sorted = keys.clone();
    sorted.sort();
    assert_eq!(keys, sorted, "iteration order must be lexicographically sorted");
}
