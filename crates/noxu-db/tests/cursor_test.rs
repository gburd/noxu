//! CursorTest — cursor API tests ported to Rust.
//!
//! Covers: cursor lifecycle (open/close/state), get operations (First, Last,
//! Next, Prev, Search, SearchGte, Current), put via cursor (Overwrite,
//! NoOverwrite, Current), cursor delete, empty-DB behavior, range scan,
//! cursor count(), key order verification, large-batch iteration.

use noxu_db::cursor::CursorState;
use noxu_db::{
    DatabaseConfig, DatabaseEntry, EnvironmentConfig, Get, OperationStatus,
    Put, TransactionConfig,
};
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

/// Like `open_env_and_db` but creates a transactional database, required
/// for cursors that are opened with an explicit `Transaction` argument
/// (JE invariant: txn cursors on non-txn DBs are rejected).
fn open_env_and_txn_db(
    dir: &TempDir,
) -> (noxu_db::Environment, noxu_db::Database) {
    let env_config = EnvironmentConfig::new(dir.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    let env = noxu_db::Environment::open(env_config).unwrap();
    let db_config =
        DatabaseConfig::new().with_allow_create(true).with_transactional(true);
    let db = env.open_database(None, "cursor_txn_test_db", &db_config).unwrap();
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
            assert!(
                key.data() > pk.as_slice(),
                "keys must be strictly increasing"
            );
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
    assert_eq!(
        keys, sorted,
        "iteration order must be lexicographically sorted"
    );
}

// ─── Regression: SearchGte with seed shorter than BIN's key_prefix ─────────────
//
// See `docs/bug-2026-05-25-compress-key-debug-assert-shortprefix-searchgte.md`.
//
// On a many-key tree whose BINs have learned a `key_prefix` longer than the
// search seed (e.g. the seed is a 2-byte tag like `b"K\0"` and the BIN's
// learned prefix is `b"K\0the-bucket\0object-0000…"`), `Cursor::get(SearchGte)`
// previously called `bin.compress_key(seed)` unconditionally, which:
//
//   * `debug_assert!`-panicked in debug builds, and
//   * panicked with a slice out-of-bounds in release builds.
//
// The fix lives in `noxu_dbi::cursor_impl::CursorImpl::find_range_entry`: it
// now compares the seed against the BIN's `key_prefix` and either returns the
// BIN's first entry (seed < key_prefix lex) or `None` (seed > key_prefix lex),
// only delegating to `compress_key` when `seed.starts_with(key_prefix)`.

fn open_env_and_db_named(
    dir: &TempDir,
    name: &str,
) -> (noxu_db::Environment, noxu_db::Database) {
    let env_config = EnvironmentConfig::new(dir.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    let env = noxu_db::Environment::open(env_config).unwrap();
    let db_config = DatabaseConfig::new().with_allow_create(true);
    let db = env.open_database(None, name, &db_config).unwrap();
    (env, db)
}

#[test]
fn cursor_search_gte_short_seed_under_long_prefix_does_not_panic() {
    // Reproduces the panic shape from the bug report: ~1000 keys all
    // sharing a long common prefix (`b"K\0the-bucket\0object-0000"…`),
    // then `SearchGte(b"K\0")` (a 2-byte seed) — strictly shorter than
    // the BIN's learned `key_prefix`.
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_env_and_db_named(&dir, "search_gte_short_seed");

    for i in 0..1000u32 {
        let mut key = Vec::new();
        key.extend_from_slice(b"K\0");
        key.extend_from_slice(b"the-bucket\0");
        key.extend_from_slice(format!("object-{i:08}").as_bytes());
        let value = format!("payload-{i}");
        db.put(
            None,
            &DatabaseEntry::from_bytes(&key),
            &DatabaseEntry::from_bytes(value.as_bytes()),
        )
        .unwrap();
    }

    let mut cursor = db.open_cursor(None, None).unwrap();
    let mut key = DatabaseEntry::from_bytes(b"K\0");
    let mut data = DatabaseEntry::new();
    let s = cursor.get(&mut key, &mut data, Get::SearchGte, None).unwrap();

    assert_eq!(
        s,
        OperationStatus::Success,
        "SearchGte with a short seed under a long-prefix BIN must succeed"
    );
    // The lexicographically smallest inserted key must be returned.
    let want = {
        let mut k = Vec::new();
        k.extend_from_slice(b"K\0");
        k.extend_from_slice(b"the-bucket\0");
        k.extend_from_slice(b"object-00000000");
        k
    };
    assert_eq!(key.data(), want.as_slice());
}

#[test]
fn cursor_search_gte_seed_above_all_keys_returns_not_found() {
    // Companion to the above: seed lex-greater than the BIN's key_prefix
    // (and than every full key in the DB) must return NotFound, not panic.
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_env_and_db_named(&dir, "search_gte_above_all");

    for i in 0..200u32 {
        let mut key = Vec::new();
        key.extend_from_slice(b"K\0");
        key.extend_from_slice(b"the-bucket\0");
        key.extend_from_slice(format!("object-{i:08}").as_bytes());
        db.put(
            None,
            &DatabaseEntry::from_bytes(&key),
            &DatabaseEntry::from_bytes(format!("v{i}").as_bytes()),
        )
        .unwrap();
    }

    let mut cursor = db.open_cursor(None, None).unwrap();
    // `b"L\0"` is lex-greater than every inserted key, which all start
    // with `b"K\0"`.  Pre-fix this would still panic in release on a
    // non-leftmost BIN whose key_prefix differs from the seed.
    let mut key = DatabaseEntry::from_bytes(b"L\0");
    let mut data = DatabaseEntry::new();
    let s = cursor.get(&mut key, &mut data, Get::SearchGte, None).unwrap();
    assert_eq!(s, OperationStatus::NotFound);
}

#[test]
fn cursor_search_gte_seed_below_all_keys_returns_first() {
    // Seed lex-less-than every full key, and shorter than the learned
    // BIN prefix on a many-key tree.  Must return the first key.
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_env_and_db_named(&dir, "search_gte_below_all");

    for i in 0..500u32 {
        let mut key = Vec::new();
        key.extend_from_slice(b"M\0");
        key.extend_from_slice(b"bucket\0");
        key.extend_from_slice(format!("k-{i:06}").as_bytes());
        db.put(
            None,
            &DatabaseEntry::from_bytes(&key),
            &DatabaseEntry::from_bytes(format!("v{i}").as_bytes()),
        )
        .unwrap();
    }

    let mut cursor = db.open_cursor(None, None).unwrap();
    // `b"A"` < every key in the DB; SearchGte must return the smallest.
    let mut key = DatabaseEntry::from_bytes(b"A");
    let mut data = DatabaseEntry::new();
    let s = cursor.get(&mut key, &mut data, Get::SearchGte, None).unwrap();
    assert_eq!(s, OperationStatus::Success);

    let want = {
        let mut k = Vec::new();
        k.extend_from_slice(b"M\0");
        k.extend_from_slice(b"bucket\0");
        k.extend_from_slice(b"k-000000");
        k
    };
    assert_eq!(key.data(), want.as_slice());
}

// ─── Regression: SearchGte must walk to the next BIN on no-match-here ─────────
//
// Before this fix `find_range_entry` only inspected the BIN that
// `find_bin_for_key` chose for the seed.  When that BIN's largest key
// was `< seed` (a common case once the tree has more than one BIN),
// it returned `None` even though a key `>= seed` lived in the next BIN.
//
// The fix calls `Tree::get_next_bin(seed)` on the no-match path; by the
// B+tree separator invariant the first entry of the next BIN is strictly
// greater than `seed`, so a single probe suffices.
//
// Default fanout is 128 entries per BIN (see `noxu-tree::DEFAULT_MAX_ENTRIES`),
// so these tests insert ≥ 256 keys to reliably force the tree into ≥ 2 BINs.

const FANOUT_DOUBLED: u32 = 256;

#[test]
fn cursor_search_gte_walks_to_next_bin_when_chosen_bin_is_below_seed() {
    // Seed sits between two BINs: every key in BIN_L is < seed, every
    // key in BIN_R is > seed.  Pre-fix this returned NotFound.
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_env_and_db_named(&dir, "search_gte_cross_bin");

    // Insert 0..256 as fixed-width keys.  The tree splits into ≥ 2 BINs
    // somewhere in the middle.
    for i in 0..FANOUT_DOUBLED {
        let k = format!("k-{i:08}");
        db.put(
            None,
            &DatabaseEntry::from_bytes(k.as_bytes()),
            &DatabaseEntry::from_bytes(format!("v{i}").as_bytes()),
        )
        .unwrap();
    }

    // `k-00000099a` is between `k-00000099` and `k-00000100`, which by
    // the natural sort straddles the most likely BIN boundary in the
    // middle of the keyspace.  Even if the actual split point shifts,
    // the assertion is that SearchGte returns the first key strictly
    // greater than the seed.
    let mut cursor = db.open_cursor(None, None).unwrap();
    let seed = b"k-00000099a";
    let mut key = DatabaseEntry::from_bytes(seed);
    let mut data = DatabaseEntry::new();
    let s = cursor.get(&mut key, &mut data, Get::SearchGte, None).unwrap();
    assert_eq!(s, OperationStatus::Success);
    assert_eq!(
        key.data(),
        b"k-00000100",
        "SearchGte must return the smallest key strictly greater than \
         the seed, even if that key lives in the next BIN"
    );
}

#[test]
fn cursor_search_gte_past_last_key_returns_not_found_with_many_bins() {
    // Seed is greater than every key in the tree across multiple BINs.
    // get_next_bin returns None at the rightmost BIN, so the cursor
    // correctly reports NotFound (rather than panicking or returning
    // a stale entry).
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_env_and_db_named(&dir, "search_gte_past_last");

    for i in 0..FANOUT_DOUBLED {
        let k = format!("a-{i:08}");
        db.put(
            None,
            &DatabaseEntry::from_bytes(k.as_bytes()),
            &DatabaseEntry::from_bytes(format!("v{i}").as_bytes()),
        )
        .unwrap();
    }

    let mut cursor = db.open_cursor(None, None).unwrap();
    let mut key = DatabaseEntry::from_bytes(b"z-this-is-after-everything");
    let mut data = DatabaseEntry::new();
    let s = cursor.get(&mut key, &mut data, Get::SearchGte, None).unwrap();
    assert_eq!(s, OperationStatus::NotFound);
}

#[test]
fn cursor_search_gte_long_prefix_seed_above_walks_to_next_bin() {
    // Reverse of `cursor_search_gte_short_seed_under_long_prefix_does_not_panic`:
    // seed lex-greater than the chosen BIN's learned `key_prefix` (case 3
    // of `find_range_entry`'s prefix analysis).  Pre-fix this would
    // return NotFound; with the next-BIN walk it must return the first
    // key whose value is `>= seed`.
    let dir = TempDir::new().unwrap();
    let (_env, db) =
        open_env_and_db_named(&dir, "search_gte_long_prefix_above");

    // Two distinct prefix groups so the tree splits into ≥ 2 BINs with
    // distinct learned prefixes.
    for i in 0..FANOUT_DOUBLED {
        let mut k = Vec::new();
        if i < FANOUT_DOUBLED / 2 {
            k.extend_from_slice(b"K\0");
            k.extend_from_slice(b"alpha\0");
        } else {
            k.extend_from_slice(b"K\0");
            k.extend_from_slice(b"omega\0");
        }
        k.extend_from_slice(format!("{i:08}").as_bytes());
        db.put(
            None,
            &DatabaseEntry::from_bytes(&k),
            &DatabaseEntry::from_bytes(format!("v{i}").as_bytes()),
        )
        .unwrap();
    }

    // `K\0gamma…` is lex-greater than `K\0alpha…` (the leftmost BIN's
    // prefix) and lex-less than `K\0omega…` (the rightmost BIN's prefix),
    // which puts it in case 3 against the leftmost BIN.
    let mut cursor = db.open_cursor(None, None).unwrap();
    let seed = b"K\0gamma\0";
    let mut key = DatabaseEntry::from_bytes(seed);
    let mut data = DatabaseEntry::new();
    let s = cursor.get(&mut key, &mut data, Get::SearchGte, None).unwrap();
    assert_eq!(s, OperationStatus::Success);
    let want = {
        let mut k = Vec::new();
        k.extend_from_slice(b"K\0");
        k.extend_from_slice(b"omega\0");
        k.extend_from_slice(format!("{:08}", FANOUT_DOUBLED / 2).as_bytes());
        k
    };
    assert_eq!(
        key.data(),
        want.as_slice(),
        "SearchGte across BIN boundary with case-3 prefix must land on \
         the first omega-prefix key"
    );
}

#[test]
fn cursor_search_gte_in_every_inter_key_gap_agrees_with_get_next() {
    // White-box-ish cross-BIN regression: walk the tree with First+Next
    // (which is known-good and uses get_next_bin internally) to harvest
    // every pair of adjacent keys (K_a, K_b).  For each pair, open a
    // fresh cursor and probe `SearchGte(K_a + b"\\0")`.
    //
    // `K_a + b"\\0"` is strictly greater than `K_a` and (since K_b is
    // K_a's immediate successor in iteration order) strictly less than
    // or equal to K_b, so the answer must be K_b.  Whenever K_a and
    // K_b live in different BINs this exercises the next-BIN-walk path
    // of `find_range_entry`; pre-fix it returns NotFound for those
    // pairs.
    //
    // Inserts ≫ fanout keys to guarantee multiple BIN boundaries are
    // exercised across the iteration.
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_env_and_db_named(&dir, "search_gte_inter_key_gaps");

    const N: u32 = 1024; // 8× default fanout — forces several BINs.
    for i in 0..N {
        let k = format!("k-{i:08}");
        db.put(
            None,
            &DatabaseEntry::from_bytes(k.as_bytes()),
            &DatabaseEntry::from_bytes(format!("v{i}").as_bytes()),
        )
        .unwrap();
    }

    // Harvest the in-order key list via Get::First + Get::Next.
    let mut keys_in_order: Vec<Vec<u8>> = Vec::with_capacity(N as usize);
    {
        let mut cursor = db.open_cursor(None, None).unwrap();
        let mut k = DatabaseEntry::new();
        let mut v = DatabaseEntry::new();
        let mut s = cursor.get(&mut k, &mut v, Get::First, None).unwrap();
        while s == OperationStatus::Success {
            keys_in_order.push(k.data().to_vec());
            s = cursor.get(&mut k, &mut v, Get::Next, None).unwrap();
        }
    }
    assert_eq!(keys_in_order.len(), N as usize);

    // Probe between each pair.
    for pair in keys_in_order.windows(2) {
        let (k_a, k_b) = (&pair[0], &pair[1]);
        let mut probe = k_a.clone();
        probe.push(0); // lex-just-after k_a
        let mut cursor = db.open_cursor(None, None).unwrap();
        let mut key = DatabaseEntry::from_bytes(&probe);
        let mut data = DatabaseEntry::new();
        let s = cursor.get(&mut key, &mut data, Get::SearchGte, None).unwrap();
        assert_eq!(
            s,
            OperationStatus::Success,
            "SearchGte({:?}) returned NotFound; expected next key {:?}",
            probe,
            k_b
        );
        assert_eq!(
            key.data(),
            k_b.as_slice(),
            "SearchGte({:?}) returned wrong next key",
            probe
        );
    }
}

#[test]
fn cursor_search_gte_oracle_brute_force_small_random() {
    // Oracle test: against a small DB with random keys, SearchGte for
    // every interesting probe (every full key, plus k+\\0 and k-\\0
    // perturbations) must agree with the brute-force answer
    // `keys.iter().filter(|k| k >= seed).min()`.
    //
    // This is the test that, had it existed, would have caught both the
    // original short-seed panic and the cross-BIN no-match bug.
    use std::collections::BTreeSet;

    let dir = TempDir::new().unwrap();
    let (_env, db) = open_env_and_db_named(&dir, "search_gte_oracle");

    // Deterministic pseudo-random key set, sized to cross BIN boundaries.
    let mut keys: BTreeSet<Vec<u8>> = BTreeSet::new();
    let mut state: u64 = 0xC0FFEE_DEADBEEF_u64;
    for _ in 0..FANOUT_DOUBLED + 50 {
        // xorshift64
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        // 4..16-byte key derived from the state.
        let len = 4 + (state as usize % 13);
        let bytes: Vec<u8> =
            (0..len).map(|i| ((state >> (i * 4)) & 0xFF) as u8).collect();
        keys.insert(bytes);
    }

    for k in &keys {
        db.put(
            None,
            &DatabaseEntry::from_bytes(k),
            &DatabaseEntry::from_bytes(b"v"),
        )
        .unwrap();
    }

    let mut cursor = db.open_cursor(None, None).unwrap();
    let probes: Vec<Vec<u8>> = {
        let mut p: Vec<Vec<u8>> = Vec::new();
        for k in &keys {
            p.push(k.clone());
            // k with a trailing 0x00 byte appended (lex-just-after k).
            let mut up = k.clone();
            up.push(0);
            p.push(up);
            // k truncated by one byte if possible (lex-just-before k).
            if k.len() > 1 {
                p.push(k[..k.len() - 1].to_vec());
            }
        }
        // Note: empty seed (b"") is intentionally NOT probed.  The public
        // `Cursor::get(SearchGte)` API short-circuits empty keys to
        // `NotFound` (`crates/noxu-db/src/cursor.rs::Get::SearchGte`
        // arm), so it never reaches `find_range_entry` and the oracle
        // would diverge from the API contract.  Lex-greatest probe is
        // a single 0xff byte.
        p.push(b"\xff".to_vec());
        p
    };

    for probe in probes {
        let want = keys.iter().find(|k| k.as_slice() >= probe.as_slice());

        let mut key = DatabaseEntry::from_bytes(&probe);
        let mut data = DatabaseEntry::new();
        let status =
            cursor.get(&mut key, &mut data, Get::SearchGte, None).unwrap();

        match (want, status) {
            (Some(w), OperationStatus::Success) => {
                assert_eq!(
                    key.data(),
                    w.as_slice(),
                    "SearchGte({:02x?}) returned wrong key; oracle expects {:02x?}",
                    probe,
                    w
                );
            }
            (None, OperationStatus::NotFound) => { /* both agree */ }
            (oracle, got) => panic!(
                "SearchGte({:02x?}) disagreement: oracle={:?} got={:?}",
                probe, oracle, got
            ),
        }
    }
}

// ─── Sprint 6 / Property 2 — Cursor full-scan order oracle ───────────────────
//
// Property: for any randomised distinct-key set, walking the database with
// `Get::First` + repeated `Get::Next` must yield the keys in lex-sorted
// order, and walking with `Get::Last` + repeated `Get::Prev` must yield
// them in reverse-sorted order.  Catches bugs of shape "BIN ordering
// wrong", "Next skips records", "iteration loses entries".
//
// Modelled on `cursor_search_gte_oracle_brute_force_small_random`: a
// `BTreeSet<Vec<u8>>` is the oracle, ~256 keys per case is enough to span
// multiple BINs, and proptest's shrinker minimises any failing key set.

mod prop_full_scan_order {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        // 64 cases is plenty: each case inserts up to 256 records into a
        // fresh env, which dominates the runtime; the shrinker still
        // produces a small counterexample on failure.
        #![proptest_config(ProptestConfig {
            cases: 64,
            .. ProptestConfig::default()
        })]

        #[test]
        fn cursor_full_scan_order_oracle_brute_force_small_random(
            keys in prop::collection::btree_set(
                prop::collection::vec(any::<u8>(), 1..=16),
                1..=256,
            ),
        ) {
            let dir = TempDir::new().unwrap();
            let (_env, db) = open_env_and_db_named(&dir, "prop_full_scan");

            for k in &keys {
                db.put(
                    None,
                    &DatabaseEntry::from_bytes(k),
                    &DatabaseEntry::from_bytes(b"v"),
                ).unwrap();
            }

            // Forward scan: Get::First + Get::Next* must be lex-sorted and
            // identical to the BTreeSet ordering.
            let mut cursor = db.open_cursor(None, None).unwrap();
            let mut got_fwd: Vec<Vec<u8>> = Vec::new();
            let mut k = DatabaseEntry::new();
            let mut d = DatabaseEntry::new();
            let mut s = cursor.get(&mut k, &mut d, Get::First, None).unwrap();
            while s == OperationStatus::Success {
                got_fwd.push(k.data().to_vec());
                s = cursor.get(&mut k, &mut d, Get::Next, None).unwrap();
                // Cheap forward-progress check: avoid infinite loops if
                // Next ever stalled on the same key.
                if got_fwd.len() > keys.len() + 1 {
                    prop_assert!(
                        false,
                        "forward scan returned more entries ({}) than were inserted ({})",
                        got_fwd.len(), keys.len(),
                    );
                }
            }
            prop_assert_eq!(s, OperationStatus::NotFound);
            let expected_fwd: Vec<Vec<u8>> = keys.iter().cloned().collect();
            prop_assert_eq!(
                &got_fwd, &expected_fwd,
                "Get::First+Next did not return keys in lex-sorted order",
            );

            // Reverse scan: Get::Last + Get::Prev* must yield the reverse
            // of the same ordering.
            let mut got_rev: Vec<Vec<u8>> = Vec::new();
            let mut s = cursor.get(&mut k, &mut d, Get::Last, None).unwrap();
            while s == OperationStatus::Success {
                got_rev.push(k.data().to_vec());
                s = cursor.get(&mut k, &mut d, Get::Prev, None).unwrap();
                if got_rev.len() > keys.len() + 1 {
                    prop_assert!(
                        false,
                        "reverse scan returned more entries ({}) than were inserted ({})",
                        got_rev.len(), keys.len(),
                    );
                }
            }
            prop_assert_eq!(s, OperationStatus::NotFound);
            let mut expected_rev = expected_fwd;
            expected_rev.reverse();
            prop_assert_eq!(
                &got_rev, &expected_rev,
                "Get::Last+Prev did not return keys in reverse-lex order",
            );
        }
    }
}

// ─── Sprint 1 / Group A — Cursor non-dup hygiene ───────────────────────────
//
// Regression tests for the audit findings tracked in
// the 2026 review:
//
//   * Finding 5 — `Get::NextDup` / `Get::PrevDup` on a non-sorted-dup DB
//     must return `NotFound` rather than silently degenerating into plain
//     `Next` / `Prev`.
//   * Finding 4 — `Get::SearchBoth` on a non-sorted-dup DB must validate the
//     stored data against the user-supplied data, not ignore it.
//   * Finding 3 — `Get::SearchLte` / `Get::FirstDup` / `Get::LastDup` are not
//     yet implemented and must surface a typed `Unsupported` error rather
//     than silently returning `NotFound`.

#[test]
fn cursor_next_dup_on_non_dup_db_returns_not_found() {
    // Audit Finding 5: `Get::NextDup` on a non-sorted-dup DB must NOT
    // advance the cursor to the next *different* key.  BDB-JE semantics:
    // every key has exactly one record on a non-dup DB, so there is no
    // "next duplicate" of the current position.
    //
    // Pre-fix behaviour: returned `Success` with the cursor moved to
    // ("b", "2") because `apply_dup_filter` is gated on `is_sorted_dup()`
    // and is skipped on non-dup DBs, leaving the cursor on the next slot.
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_env_and_db_named(&dir, "next_dup_non_dup");

    db.put(
        None,
        &DatabaseEntry::from_bytes(b"a"),
        &DatabaseEntry::from_bytes(b"1"),
    )
    .unwrap();
    db.put(
        None,
        &DatabaseEntry::from_bytes(b"b"),
        &DatabaseEntry::from_bytes(b"2"),
    )
    .unwrap();

    let mut cursor = db.open_cursor(None, None).unwrap();
    let mut key = DatabaseEntry::from_bytes(b"a");
    let mut data = DatabaseEntry::new();
    let s = cursor.get(&mut key, &mut data, Get::Search, None).unwrap();
    assert_eq!(s, OperationStatus::Success);

    // The bug: pre-fix this returned Success with key == "b".
    let s = cursor.get(&mut key, &mut data, Get::NextDup, None).unwrap();
    assert_eq!(
        s,
        OperationStatus::NotFound,
        "Get::NextDup on non-dup DB must return NotFound; got key={:?}",
        key.get_data()
    );
}

#[test]
fn cursor_prev_dup_on_non_dup_db_returns_not_found() {
    // Companion to the above: `Get::PrevDup` on a non-sorted-dup DB must
    // also return `NotFound` rather than walking to the previous record.
    //
    // We position with `Get::Last` (not `Get::Search`) so the cursor sits
    // on the second slot in the BIN — otherwise the fast-path falls off
    // the left edge naturally and the bug is masked.
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_env_and_db_named(&dir, "prev_dup_non_dup");

    db.put(
        None,
        &DatabaseEntry::from_bytes(b"a"),
        &DatabaseEntry::from_bytes(b"1"),
    )
    .unwrap();
    db.put(
        None,
        &DatabaseEntry::from_bytes(b"b"),
        &DatabaseEntry::from_bytes(b"2"),
    )
    .unwrap();

    let mut cursor = db.open_cursor(None, None).unwrap();
    let mut key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();
    let s = cursor.get(&mut key, &mut data, Get::Last, None).unwrap();
    assert_eq!(s, OperationStatus::Success);
    assert_eq!(key.data(), b"b");

    // The bug: pre-fix this returned Success with key == "a".
    let s = cursor.get(&mut key, &mut data, Get::PrevDup, None).unwrap();
    assert_eq!(
        s,
        OperationStatus::NotFound,
        "Get::PrevDup on non-dup DB must return NotFound; got key={:?}",
        key.get_data()
    );
}

#[test]
fn cursor_search_both_on_non_dup_db_validates_data() {
    // Audit Finding 4: `Get::SearchBoth` on a non-sorted-dup DB must
    // validate the slot's data against the user-supplied data.  Pre-fix
    // the `data` argument was silently dropped and Success was returned
    // for any matching key.
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_env_and_db_named(&dir, "search_both_non_dup");

    db.put(
        None,
        &DatabaseEntry::from_bytes(b"k"),
        &DatabaseEntry::from_bytes(b"stored"),
    )
    .unwrap();

    // Probe with the wrong data.
    let mut cursor = db.open_cursor(None, None).unwrap();
    let mut k = DatabaseEntry::from_bytes(b"k");
    let mut d = DatabaseEntry::from_bytes(b"different");
    let s = cursor.get(&mut k, &mut d, Get::SearchBoth, None).unwrap();
    assert_eq!(
        s,
        OperationStatus::NotFound,
        "SearchBoth on non-dup DB must return NotFound when data mismatches"
    );

    // Probe with the right data — must still succeed.
    let mut k = DatabaseEntry::from_bytes(b"k");
    let mut d = DatabaseEntry::from_bytes(b"stored");
    let s = cursor.get(&mut k, &mut d, Get::SearchBoth, None).unwrap();
    assert_eq!(
        s,
        OperationStatus::Success,
        "SearchBoth on non-dup DB must succeed when data matches"
    );
    assert_eq!(k.data(), b"k");
    assert_eq!(d.data(), b"stored");
}

#[test]
fn cursor_search_both_on_non_dup_db_missing_key_still_not_found() {
    // Sanity: a missing key still returns NotFound regardless of data.
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_env_and_db_named(&dir, "search_both_missing_key");

    db.put(
        None,
        &DatabaseEntry::from_bytes(b"k"),
        &DatabaseEntry::from_bytes(b"v"),
    )
    .unwrap();

    let mut cursor = db.open_cursor(None, None).unwrap();
    let mut k = DatabaseEntry::from_bytes(b"missing");
    let mut d = DatabaseEntry::from_bytes(b"anything");
    let s = cursor.get(&mut k, &mut d, Get::SearchBoth, None).unwrap();
    assert_eq!(s, OperationStatus::NotFound);
}

#[test]
fn cursor_search_lte_returns_unsupported_error() {
    // Audit Finding 3: `Get::SearchLte` is not yet implemented.  Pre-fix
    // it fell through to the wildcard `_ => Ok(NotFound)` arm in
    // `Cursor::get`, silently misleading callers.  It must now return a
    // typed `NoxuError::Unsupported`.
    use noxu_db::NoxuError;

    let dir = TempDir::new().unwrap();
    let (_env, db) = open_env_and_db_named(&dir, "search_lte_unsupported");

    db.put(
        None,
        &DatabaseEntry::from_bytes(b"a"),
        &DatabaseEntry::from_bytes(b"1"),
    )
    .unwrap();

    let mut cursor = db.open_cursor(None, None).unwrap();
    let mut key = DatabaseEntry::from_bytes(b"a");
    let mut data = DatabaseEntry::new();
    let err = cursor
        .get(&mut key, &mut data, Get::SearchLte, None)
        .expect_err("Get::SearchLte must return an Unsupported error");
    match err {
        NoxuError::Unsupported(op) => {
            assert!(
                op.contains("SearchLte"),
                "Unsupported message must name the operation; got {op:?}"
            );
        }
        other => panic!("expected NoxuError::Unsupported, got {other:?}"),
    }
}

#[test]
fn cursor_first_dup_returns_unsupported_error() {
    // Audit Finding 3 — companion to the SearchLte case.
    use noxu_db::NoxuError;

    let dir = TempDir::new().unwrap();
    let (_env, db) = open_env_and_db_named(&dir, "first_dup_unsupported");

    db.put(
        None,
        &DatabaseEntry::from_bytes(b"a"),
        &DatabaseEntry::from_bytes(b"1"),
    )
    .unwrap();

    let mut cursor = db.open_cursor(None, None).unwrap();
    let mut key = DatabaseEntry::from_bytes(b"a");
    let mut data = DatabaseEntry::new();
    cursor.get(&mut key, &mut data, Get::Search, None).unwrap();

    let err = cursor
        .get(&mut key, &mut data, Get::FirstDup, None)
        .expect_err("Get::FirstDup must return an Unsupported error");
    assert!(
        matches!(err, NoxuError::Unsupported(ref op) if op.contains("FirstDup"))
    );
}

#[test]
fn cursor_last_dup_returns_unsupported_error() {
    // Audit Finding 3 — companion to the SearchLte case.
    use noxu_db::NoxuError;

    let dir = TempDir::new().unwrap();
    let (_env, db) = open_env_and_db_named(&dir, "last_dup_unsupported");

    db.put(
        None,
        &DatabaseEntry::from_bytes(b"a"),
        &DatabaseEntry::from_bytes(b"1"),
    )
    .unwrap();

    let mut cursor = db.open_cursor(None, None).unwrap();
    let mut key = DatabaseEntry::from_bytes(b"a");
    let mut data = DatabaseEntry::new();
    cursor.get(&mut key, &mut data, Get::Search, None).unwrap();

    let err = cursor
        .get(&mut key, &mut data, Get::LastDup, None)
        .expect_err("Get::LastDup must return an Unsupported error");
    assert!(
        matches!(err, NoxuError::Unsupported(ref op) if op.contains("LastDup"))
    );
}

// ─── 14. Cursor must participate in the txn passed to open_cursor ─────────────
//
// Regression for API audit 2026-05 cursor finding C1 / #1
// (the 2026 review):
// `Database::open_cursor(Some(&txn), None)` previously bound the txn argument
// as `_txn` and dropped it on the floor.  Cursor writes auto-committed and
// cursor reads bypassed the txn's lock set, silently breaking ACID isolation
// for users following the canonical pattern in
// `docs/src/transactions/cursors.md`.

/// Cursor opened with `Some(&txn)` must participate in that txn: a `put`
/// followed by `txn.abort()` must leave the database unchanged.
///
/// **Pre-fix this test fails** because the cursor was built via
/// `make_cursor()` instead of `make_cursor_for_txn(t)`, so the put was
/// auto-committed and survived the abort.
#[test]
fn cursor_with_txn_put_is_rolled_back_on_abort() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_env_and_txn_db(&dir);

    // Seed nothing — start with an empty database.
    let txn = env.begin_transaction(None).unwrap();
    let mut cursor = db.open_cursor(Some(&txn), None).unwrap();

    let (k, v) = kv(b"rolled-back-key", b"rolled-back-val");
    let s = cursor.put(&k, &v, Put::Overwrite).unwrap();
    assert_eq!(s, OperationStatus::Success);

    // Cursor must be closed before the txn ends.
    cursor.close().unwrap();
    txn.abort().unwrap();

    // Open a fresh, auto-commit cursor and confirm the put did NOT persist.
    let mut probe = db.open_cursor(None, None).unwrap();
    let mut out_k = DatabaseEntry::new();
    let mut out_v = DatabaseEntry::new();
    let status = probe.get(&mut out_k, &mut out_v, Get::First, None).unwrap();
    assert_eq!(
        status,
        OperationStatus::NotFound,
        "cursor.put under an aborted txn must NOT persist; the txn argument \
         to Database::open_cursor was being dropped (audit C1)"
    );
}

/// A second-line check using `db.get`: even when the auto-commit cursor in
/// the assertion above is correct, exercising the public `get` path makes
/// the regression self-evident in a stack trace.
#[test]
fn cursor_with_txn_put_invisible_via_get_after_abort() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_env_and_txn_db(&dir);

    let txn = env.begin_transaction(None).unwrap();
    let mut cursor = db.open_cursor(Some(&txn), None).unwrap();

    let (k, v) = kv(b"k", b"v");
    cursor.put(&k, &v, Put::Overwrite).unwrap();
    cursor.close().unwrap();
    txn.abort().unwrap();

    let mut out = DatabaseEntry::new();
    let status = db.get(None, &k, &mut out).unwrap();
    assert_eq!(
        status,
        OperationStatus::NotFound,
        "value written through a cursor opened with Some(&txn) must vanish \
         on txn.abort() (audit C1)"
    );
}

/// Cursor reads through a txn must take locks in the txn's lock set.
/// We verify this indirectly: writer txn A holds an uncommitted write on a
/// key; reader txn B opens a cursor with `Some(&B)` and a no-wait config and
/// attempts a `Get::Search` on the same key.  Pre-fix the cursor read does
/// not engage the lock manager (because the txn argument is dropped) and
/// the read either spuriously succeeds or returns NotFound without a lock
/// conflict.  Post-fix the read must conflict with A's write lock and
/// return an error under no-wait.
#[test]
fn cursor_with_txn_get_takes_read_lock_via_locker() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_env_and_txn_db(&dir);

    // Seed a committed value so there is something to lock.
    let seed = env.begin_transaction(None).unwrap();
    let (k, v0) = kv(b"locked", b"v0");
    db.put(Some(&seed), &k, &v0).unwrap();
    seed.commit().unwrap();

    // Writer txn A holds an exclusive lock on `locked` (uncommitted).
    let txn_a = env.begin_transaction(None).unwrap();
    let v1 = DatabaseEntry::from_bytes(b"v1");
    db.put(Some(&txn_a), &k, &v1).unwrap();

    // Reader txn B uses a serializable, no-wait config so a lock conflict
    // surfaces as an error rather than blocking forever.
    let no_wait = TransactionConfig::new().with_no_wait(true);
    let txn_b = env.begin_transaction(Some(&no_wait)).unwrap();
    let mut cursor = db.open_cursor(Some(&txn_b), None).unwrap();

    let mut search_k = DatabaseEntry::from_bytes(b"locked");
    let mut out_v = DatabaseEntry::new();
    let read_result = cursor.get(&mut search_k, &mut out_v, Get::Search, None);

    assert!(
        read_result.is_err(),
        "cursor read under txn B with no-wait must conflict with txn A's \
         write lock; pre-fix the txn argument was dropped and the cursor \
         did not consult the lock manager (audit C1). got Ok({:?})",
        read_result.ok()
    );

    cursor.close().unwrap();
    let _ = txn_b.abort();
    txn_a.abort().unwrap();
}

// ─── 15. Secondary cursor must thread its txn / config arguments ──────────────
//
// Regression for API audit 2026-05 secondary-join finding F4
// (the 2026 review):
// `SecondaryDatabase::open_cursor` previously accepted `_txn` and `_config`
// and discarded both, so every secondary cursor ran auto-commit no matter
// what the caller passed.

mod secondary_cursor_txn {
    use noxu_db::{
        CursorConfig, Database, DatabaseConfig, DatabaseEntry, Environment,
        EnvironmentConfig, OperationStatus, SecondaryConfig, SecondaryDatabase,
        SecondaryKeyCreator,
    };
    use noxu_sync::Mutex;
    use std::sync::Arc;
    use tempfile::TempDir;

    /// `sec_key = first byte of data`.
    struct FirstByte;
    impl SecondaryKeyCreator for FirstByte {
        fn create_secondary_key(
            &self,
            _db: &Database,
            _key: &DatabaseEntry,
            data: &DatabaseEntry,
            result: &mut DatabaseEntry,
        ) -> bool {
            if let Some(d) = data.get_data()
                && !d.is_empty()
            {
                result.set_data(&d[..1]);
                return true;
            }
            false
        }
    }

    fn open_pri_sec(
        dir: &TempDir,
    ) -> (Environment, Arc<Mutex<Database>>, SecondaryDatabase) {
        let env = Environment::open(
            EnvironmentConfig::new(dir.path().to_path_buf())
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap();
        let primary_db = env
            .open_database(
                None,
                "pri",
                &DatabaseConfig::new()
                    .with_allow_create(true)
                    .with_transactional(true),
            )
            .unwrap();
        let primary = Arc::new(Mutex::new(primary_db));
        let sec_db = env
            .open_database(
                None,
                "sec",
                &DatabaseConfig::new()
                    .with_allow_create(true)
                    .with_transactional(true)
                    .with_sorted_duplicates(true),
            )
            .unwrap();
        let sec_config = SecondaryConfig::new()
            .with_allow_create(true)
            .with_key_creator(Box::new(FirstByte));
        let secondary =
            SecondaryDatabase::open(Arc::clone(&primary), sec_db, sec_config)
                .unwrap();
        (env, primary, secondary)
    }

    /// Smoke test: `SecondaryDatabase::open_cursor(Some(&txn), Some(&cfg))`
    /// must accept and use both arguments — the inner cursor over the
    /// secondary index now participates in the txn rather than being
    /// auto-commit.  Pre-fix this call accepted `_txn` and `_config` and
    /// silently dropped them; the test exists so a future change that
    /// re-introduces the underscore will trip CI.
    #[test]
    fn sec_open_cursor_threads_txn_and_config() {
        let dir = TempDir::new().unwrap();
        let (env, primary, secondary) = open_pri_sec(&dir);

        // Seed: primary record + matching secondary entry.
        let pk = DatabaseEntry::from_bytes(b"pk1");
        let pv = DatabaseEntry::from_bytes(b"Apple");
        // Auto-hook maintains secondary via primary.put().
        primary.lock().put(None, &pk, &pv).unwrap();

        // Open a secondary cursor under a txn with a non-default config.
        let txn = env.begin_transaction(None).unwrap();
        let cfg = CursorConfig::new();
        let mut cursor = secondary.open_cursor(Some(&txn), Some(&cfg)).unwrap();

        // Iterate to confirm the cursor is functional under the txn.
        let mut sec_key = DatabaseEntry::from_bytes(b"A");
        let mut p_key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();
        let s = cursor.get_search_key(&sec_key, &mut p_key, &mut data).unwrap();
        assert_eq!(s, OperationStatus::Success);
        assert_eq!(p_key.data(), b"pk1");
        assert_eq!(data.data(), b"Apple");

        // Cursor must be closed before the txn ends — same lifecycle rule
        // as the primary cursor.
        cursor.close().unwrap();
        txn.commit().unwrap();

        // And again under abort — the cursor read participated in the
        // (now-aborted) txn but nothing was written, so the secondary
        // entry must still be reachable from a fresh auto-commit cursor.
        let txn2 = env.begin_transaction(None).unwrap();
        let mut cursor2 = secondary.open_cursor(Some(&txn2), None).unwrap();
        sec_key = DatabaseEntry::from_bytes(b"A");
        let s =
            cursor2.get_search_key(&sec_key, &mut p_key, &mut data).unwrap();
        assert_eq!(s, OperationStatus::Success);
        cursor2.close().unwrap();
        txn2.abort().unwrap();

        let mut probe = secondary.open_cursor(None, None).unwrap();
        let s = probe
            .get_search_key(
                &DatabaseEntry::from_bytes(b"A"),
                &mut p_key,
                &mut data,
            )
            .unwrap();
        assert_eq!(s, OperationStatus::Success);
        probe.close().unwrap();
    }

    // NOTE: A deeper test that asserts the secondary cursor's reads engage
    // the supplied txn's lock set is deferred as a follow-up.  As of
    // Sprint 4½, `SecondaryDatabase::update_secondary` /
    // `insert_sec_key` / `delete_sec_key` *do* take a `Transaction`
    // argument and forward it to the inner cursor (closing audit finding
    // F5 for the manual-update path), so a write-conflict probe is now
    // straightforward to author.  The remaining gap is that
    // `SecondaryCursor::delete` itself does not yet thread the cursor's
    // owning txn into the cascading primary-delete + secondary cleanup;
    // wiring that path moves with the v1.6 automatic-association work.
}
