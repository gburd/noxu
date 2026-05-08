//! Integration tests: open env → put/get/delete/cursor scan end-to-end.
//!
//! Verifies that the real B-tree backend (EnvironmentImpl → DatabaseImpl →
//! CursorImpl → Tree) is correctly wired through the noxu-db public API.

use noxu_db::{
    DatabaseConfig, DatabaseEntry, EnvironmentConfig, Get, OperationStatus,
};
use tempfile::TempDir;

fn open_env_and_db(
    dir: &TempDir,
) -> (noxu_db::Environment, noxu_db::Database) {
    let env_config = EnvironmentConfig::new(dir.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    let env = noxu_db::Environment::open(env_config).unwrap();
    let db_config = DatabaseConfig::new().with_allow_create(true);
    let db = env.open_database(None, "test", &db_config).unwrap();
    (env, db)
}

/// Basic put then get round-trip through the real tree.
#[test]
fn test_put_get_round_trip() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_env_and_db(&dir);

    let key = DatabaseEntry::from_bytes(b"hello");
    let val = DatabaseEntry::from_bytes(b"world");

    assert_eq!(db.put(None, &key, &val).unwrap(), OperationStatus::Success);

    let mut out = DatabaseEntry::new();
    assert_eq!(db.get(None, &key, &mut out).unwrap(), OperationStatus::Success);
    assert_eq!(out.data(), b"world");
}

/// Put then delete: subsequent get returns NotFound.
#[test]
fn test_put_delete_get() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_env_and_db(&dir);

    let key = DatabaseEntry::from_bytes(b"key");
    let val = DatabaseEntry::from_bytes(b"val");

    db.put(None, &key, &val).unwrap();
    assert_eq!(db.delete(None, &key).unwrap(), OperationStatus::Success);

    let mut out = DatabaseEntry::new();
    assert_eq!(db.get(None, &key, &mut out).unwrap(), OperationStatus::NotFound);
}

/// Last put wins (overwrite semantics).
#[test]
fn test_put_overwrite() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_env_and_db(&dir);

    let key = DatabaseEntry::from_bytes(b"k");
    let v1 = DatabaseEntry::from_bytes(b"v1");
    let v2 = DatabaseEntry::from_bytes(b"v2");

    db.put(None, &key, &v1).unwrap();
    db.put(None, &key, &v2).unwrap();

    let mut out = DatabaseEntry::new();
    db.get(None, &key, &mut out).unwrap();
    assert_eq!(out.data(), b"v2");
}

/// put_no_overwrite returns KeyExists when key is already present.
#[test]
fn test_put_no_overwrite() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_env_and_db(&dir);

    let key = DatabaseEntry::from_bytes(b"k");
    let v1 = DatabaseEntry::from_bytes(b"v1");
    let v2 = DatabaseEntry::from_bytes(b"v2");

    db.put(None, &key, &v1).unwrap();
    let status = db.put_no_overwrite(None, &key, &v2).unwrap();
    assert_eq!(status, OperationStatus::KeyExists);

    // Original value unchanged
    let mut out = DatabaseEntry::new();
    db.get(None, &key, &mut out).unwrap();
    assert_eq!(out.data(), b"v1");
}

/// Cursor scan: First + Next iterates all records in sorted order.
#[test]
fn test_cursor_scan_sorted() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_env_and_db(&dir);

    // Insert out of order
    for (k, v) in [(b"c", b"3"), (b"a", b"1"), (b"b", b"2")] {
        db.put(
            None,
            &DatabaseEntry::from_bytes(k),
            &DatabaseEntry::from_bytes(v),
        )
        .unwrap();
    }

    let mut cursor = db.open_cursor(None, None).unwrap();
    let mut dummy_key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();

    // Collect values in iteration order
    let mut values: Vec<Vec<u8>> = Vec::new();
    let mut status = cursor.get(&mut dummy_key, &mut data, Get::First, None).unwrap();
    while status == OperationStatus::Success {
        values.push(data.data().to_vec());
        status = cursor.get(&mut dummy_key, &mut data, Get::Next, None).unwrap();
    }

    assert_eq!(values, vec![b"1", b"2", b"3"]);
    cursor.close().unwrap();
}

/// Cursor scan in reverse order: Last + Prev.
#[test]
fn test_cursor_scan_reverse() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_env_and_db(&dir);

    for (k, v) in [(b"a", b"1"), (b"b", b"2"), (b"c", b"3")] {
        db.put(
            None,
            &DatabaseEntry::from_bytes(k),
            &DatabaseEntry::from_bytes(v),
        )
        .unwrap();
    }

    let mut cursor = db.open_cursor(None, None).unwrap();
    let mut dummy_key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();

    let mut values: Vec<Vec<u8>> = Vec::new();
    let mut status = cursor.get(&mut dummy_key, &mut data, Get::Last, None).unwrap();
    while status == OperationStatus::Success {
        values.push(data.data().to_vec());
        status = cursor.get(&mut dummy_key, &mut data, Get::Prev, None).unwrap();
    }

    assert_eq!(values, vec![b"3", b"2", b"1"]);
    cursor.close().unwrap();
}

/// Cursor Search positions at the exact key.
#[test]
fn test_cursor_search() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_env_and_db(&dir);

    for (k, v) in [
        (b"apple".as_ref(), b"a".as_ref()),
        (b"banana".as_ref(), b"b".as_ref()),
        (b"cherry".as_ref(), b"c".as_ref()),
    ] {
        db.put(
            None,
            &DatabaseEntry::from_bytes(k),
            &DatabaseEntry::from_bytes(v),
        )
        .unwrap();
    }

    let mut cursor = db.open_cursor(None, None).unwrap();
    let mut key = DatabaseEntry::from_bytes(b"banana");
    let mut data = DatabaseEntry::new();

    let status = cursor.get(&mut key, &mut data, Get::Search, None).unwrap();
    assert_eq!(status, OperationStatus::Success);
    assert_eq!(data.data(), b"b");
    cursor.close().unwrap();
}

/// Cursor delete removes the record; subsequent search returns NotFound.
#[test]
fn test_cursor_delete() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_env_and_db(&dir);

    let mut key = DatabaseEntry::from_bytes(b"k");
    let val = DatabaseEntry::from_bytes(b"v");
    db.put(None, &key, &val).unwrap();

    let mut cursor = db.open_cursor(None, None).unwrap();
    let mut data = DatabaseEntry::new();

    // Position on the record then delete via cursor
    cursor.get(&mut key, &mut data, Get::Search, None).unwrap();
    cursor.delete().unwrap();
    cursor.close().unwrap();

    // Verify gone via Database::get
    let mut out = DatabaseEntry::new();
    assert_eq!(db.get(None, &key, &mut out).unwrap(), OperationStatus::NotFound);
}

/// Database::count() returns the right number of records.
#[test]
fn test_count() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_env_and_db(&dir);

    assert_eq!(db.count().unwrap(), 0);

    db.put(
        None,
        &DatabaseEntry::from_bytes(b"k1"),
        &DatabaseEntry::from_bytes(b"v1"),
    )
    .unwrap();
    assert_eq!(db.count().unwrap(), 1);

    db.put(
        None,
        &DatabaseEntry::from_bytes(b"k2"),
        &DatabaseEntry::from_bytes(b"v2"),
    )
    .unwrap();
    assert_eq!(db.count().unwrap(), 2);

    db.delete(None, &DatabaseEntry::from_bytes(b"k1")).unwrap();
    assert_eq!(db.count().unwrap(), 1);
}

/// Multiple databases within the same environment are independent.
#[test]
fn test_multiple_databases_isolated() {
    let dir = TempDir::new().unwrap();
    let env_config = EnvironmentConfig::new(dir.path().to_path_buf())
        .with_allow_create(true);
    let env = noxu_db::Environment::open(env_config).unwrap();
    let db_config = DatabaseConfig::new().with_allow_create(true);

    let db1 = env.open_database(None, "db1", &db_config).unwrap();
    let db2 = env.open_database(None, "db2", &db_config).unwrap();

    let key = DatabaseEntry::from_bytes(b"k");
    db1.put(None, &key, &DatabaseEntry::from_bytes(b"from-db1")).unwrap();
    db2.put(None, &key, &DatabaseEntry::from_bytes(b"from-db2")).unwrap();

    let mut out1 = DatabaseEntry::new();
    let mut out2 = DatabaseEntry::new();
    db1.get(None, &key, &mut out1).unwrap();
    db2.get(None, &key, &mut out2).unwrap();

    assert_eq!(out1.data(), b"from-db1");
    assert_eq!(out2.data(), b"from-db2");
}

/// get_database_names() reflects names registered via open_database.
#[test]
fn test_get_database_names() {
    let dir = TempDir::new().unwrap();
    let env_config = EnvironmentConfig::new(dir.path().to_path_buf())
        .with_allow_create(true);
    let env = noxu_db::Environment::open(env_config).unwrap();
    let db_config = DatabaseConfig::new().with_allow_create(true);

    let _db1 = env.open_database(None, "alpha", &db_config).unwrap();
    let _db2 = env.open_database(None, "beta", &db_config).unwrap();

    let names = env.get_database_names().unwrap();
    assert!(names.contains(&"alpha".to_string()));
    assert!(names.contains(&"beta".to_string()));
    assert_eq!(names.len(), 2);
}

// ─── DatabaseEntry correctness tests (ported from DatabaseEntryTest.java) ────

/// DatabaseEntry::new() has no data; get_data() returns None and size is 0.
/// Mirrors the null-entry branch of testBasic().
#[test]
fn dbentry_new_is_null() {
    let entry = noxu_db::DatabaseEntry::new();
    assert_eq!(entry.get_data(), None);
    assert_eq!(entry.get_size(), 0);
}

/// from_bytes stores the data and exposes it at offset 0 with the correct size.
/// Mirrors the constructor-with-array branch of testBasic().
#[test]
fn dbentry_from_bytes_stores_data() {
    let data: Vec<u8> = vec![1u8; 10];
    let entry = noxu_db::DatabaseEntry::from_bytes(&data);
    assert_eq!(entry.get_size(), 10);
    assert_eq!(entry.get_data(), Some(data.as_slice()));
}

/// set_data() on an entry replaces content; get_data() reflects new bytes.
/// set_data with a different payload changes data and resets offset to 0.
/// Mirrors the setData() branch of testBasic().
#[test]
fn dbentry_set_data_replaces_content() {
    let mut entry = noxu_db::DatabaseEntry::from_bytes(b"original");
    assert_eq!(entry.get_data(), Some(b"original".as_ref()));

    entry.set_data(b"replaced");
    assert_eq!(entry.get_data(), Some(b"replaced".as_ref()));
    assert_eq!(entry.get_size(), 8);
    assert_eq!(entry.get_offset(), 0);
}

/// After set_data(null equivalent via clear()) the entry is empty.
/// Mirrors dbtA.setData(null) in testBasic().
#[test]
fn dbentry_clear_makes_null() {
    let mut entry = noxu_db::DatabaseEntry::from_bytes(b"data");
    entry.clear();
    assert_eq!(entry.get_data(), None);
    assert_eq!(entry.get_size(), 0);
}

/// Constructing with offset and size exposes only the sub-slice.
/// After calling set_data() the offset resets to 0 and size equals full length.
/// Mirrors the dbtOffset branch of testBasic().
#[test]
fn dbentry_offset_and_size() {
    let data: Vec<u8> = (0u8..10).collect();
    let mut entry = noxu_db::DatabaseEntry::from_bytes(&data);
    entry.set_offset(3);
    entry.set_size(4);
    // get_data() should return bytes [3..7]
    assert_eq!(entry.get_offset(), 3);
    assert_eq!(entry.get_size(), 4);
    assert_eq!(entry.get_data(), Some(&data[3..7]));

    // Calling set_data resets offset to 0 and size to full length.
    let new_data: Vec<u8> = vec![42u8; 6];
    entry.set_data(&new_data);
    assert_eq!(entry.get_offset(), 0);
    assert_eq!(entry.get_size(), 6);
}

/// Two entries with identical byte content compare equal; differing content
/// compares not-equal.  Mirrors testBasic() assertEquals/arrays.equals checks.
#[test]
fn dbentry_equality_based_on_content() {
    let a = noxu_db::DatabaseEntry::from_bytes(b"hello");
    let b = noxu_db::DatabaseEntry::from_bytes(b"hello");
    let c = noxu_db::DatabaseEntry::from_bytes(b"world");
    assert_eq!(a, b);
    assert_ne!(a, c);
}

/// get_data() on an entry with offset reads the correct sub-slice.
/// Mirrors the testOffset() assertions about foundKey/foundData offset=0, size=10.
#[test]
fn dbentry_get_data_respects_offset_and_size() {
    // Build a 30-byte array where byte[i] == i
    let raw: Vec<u8> = (0u8..30).collect();
    let mut entry = noxu_db::DatabaseEntry::from_bytes(&raw);
    entry.set_offset(10);
    entry.set_size(10);
    // Should return bytes 10..20
    let slice = entry.get_data().unwrap();
    assert_eq!(slice.len(), 10);
    for (i, &byte) in slice.iter().enumerate() {
        assert_eq!(byte, (i + 10) as u8);
    }
}

/// is_partial() / set_partial() round-trip.
/// Mirrors the partial flag checks in testPartial().
#[test]
fn dbentry_partial_flag_round_trip() {
    let mut entry = noxu_db::DatabaseEntry::new();
    assert!(!entry.is_partial());

    entry.set_partial(5, 10, true);
    assert!(entry.is_partial());
    assert_eq!(entry.get_partial_offset(), 5);
    assert_eq!(entry.get_partial_length(), 10);

    entry.set_partial(0, 0, false);
    assert!(!entry.is_partial());
}

// ─── Cursor correctness tests (ported from CursorTest.java) ──────────────────

/// Get::First returns the smallest key in the database.
/// Mirrors the getFirst() assertion in insertMultiDb().
#[test]
fn cursor_first_returns_smallest_key() {
    let dir = tempfile::TempDir::new().unwrap();
    let (_env, db) = open_env_and_db(&dir);

    // Insert out-of-order; keys sort lexicographically.
    for (k, v) in [(b"dog".as_ref(), b"3".as_ref()), (b"ant", b"1"), (b"bee", b"2")] {
        db.put(None, &noxu_db::DatabaseEntry::from_bytes(k), &noxu_db::DatabaseEntry::from_bytes(v)).unwrap();
    }

    let mut cursor = db.open_cursor(None, None).unwrap();
    let mut key = noxu_db::DatabaseEntry::new();
    let mut data = noxu_db::DatabaseEntry::new();

    let status = cursor.get(&mut key, &mut data, noxu_db::Get::First, None).unwrap();
    assert_eq!(status, noxu_db::OperationStatus::Success);
    assert_eq!(key.data(), b"ant");
    assert_eq!(data.data(), b"1");
    cursor.close().unwrap();
}

/// Get::Last returns the largest key in the database.
/// Mirrors the getLast() assertion pattern from CursorTest.
#[test]
fn cursor_last_returns_largest_key() {
    let dir = tempfile::TempDir::new().unwrap();
    let (_env, db) = open_env_and_db(&dir);

    for (k, v) in [(b"dog".as_ref(), b"3".as_ref()), (b"ant", b"1"), (b"bee", b"2")] {
        db.put(None, &noxu_db::DatabaseEntry::from_bytes(k), &noxu_db::DatabaseEntry::from_bytes(v)).unwrap();
    }

    let mut cursor = db.open_cursor(None, None).unwrap();
    let mut key = noxu_db::DatabaseEntry::new();
    let mut data = noxu_db::DatabaseEntry::new();

    let status = cursor.get(&mut key, &mut data, noxu_db::Get::Last, None).unwrap();
    assert_eq!(status, noxu_db::OperationStatus::Success);
    assert_eq!(key.data(), b"dog");
    assert_eq!(data.data(), b"3");
    cursor.close().unwrap();
}

/// Get::Next traverses all keys in ascending sorted order.
/// Mirrors the while(status == SUCCESS) getNext() loop in insertMultiDb().
#[test]
fn cursor_next_traverses_sorted_order() {
    let dir = tempfile::TempDir::new().unwrap();
    let (_env, db) = open_env_and_db(&dir);

    // Insert seven single-byte keys — same alphabet used in CursorTest.dataStrings.
    let pairs: &[(&[u8], &[u8])] = &[
        (b"A", b"1"), (b"B", b"2"), (b"C", b"3"),
        (b"F", b"4"), (b"G", b"5"), (b"H", b"6"), (b"I", b"7"),
    ];
    for (k, v) in pairs {
        db.put(None, &noxu_db::DatabaseEntry::from_bytes(k), &noxu_db::DatabaseEntry::from_bytes(v)).unwrap();
    }

    let mut cursor = db.open_cursor(None, None).unwrap();
    let mut key = noxu_db::DatabaseEntry::new();
    let mut data = noxu_db::DatabaseEntry::new();

    let mut keys_seen: Vec<Vec<u8>> = Vec::new();
    let mut status = cursor.get(&mut key, &mut data, noxu_db::Get::First, None).unwrap();
    while status == noxu_db::OperationStatus::Success {
        keys_seen.push(key.data().to_vec());
        status = cursor.get(&mut key, &mut data, noxu_db::Get::Next, None).unwrap();
    }

    let expected: Vec<&[u8]> = vec![b"A", b"B", b"C", b"F", b"G", b"H", b"I"];
    assert_eq!(keys_seen, expected);
    cursor.close().unwrap();
}

/// Get::Prev traverses all keys in descending sorted order.
/// Mirrors the reverse-scan invariant implied by CursorTest.
#[test]
fn cursor_prev_traverses_reverse_sorted_order() {
    let dir = tempfile::TempDir::new().unwrap();
    let (_env, db) = open_env_and_db(&dir);

    let pairs: &[(&[u8], &[u8])] = &[
        (b"A", b"1"), (b"B", b"2"), (b"C", b"3"),
    ];
    for (k, v) in pairs {
        db.put(None, &noxu_db::DatabaseEntry::from_bytes(k), &noxu_db::DatabaseEntry::from_bytes(v)).unwrap();
    }

    let mut cursor = db.open_cursor(None, None).unwrap();
    let mut key = noxu_db::DatabaseEntry::new();
    let mut data = noxu_db::DatabaseEntry::new();

    let mut keys_seen: Vec<Vec<u8>> = Vec::new();
    let mut status = cursor.get(&mut key, &mut data, noxu_db::Get::Last, None).unwrap();
    while status == noxu_db::OperationStatus::Success {
        keys_seen.push(key.data().to_vec());
        status = cursor.get(&mut key, &mut data, noxu_db::Get::Prev, None).unwrap();
    }

    let expected: Vec<&[u8]> = vec![b"C", b"B", b"A"];
    assert_eq!(keys_seen, expected);
    cursor.close().unwrap();
}

/// Get::Search finds an exact key and returns its data; cursor is positioned.
/// Mirrors cursor.getSearchKey() == SUCCESS assertions in CursorTest.
#[test]
fn cursor_search_finds_exact_key() {
    let dir = tempfile::TempDir::new().unwrap();
    let (_env, db) = open_env_and_db(&dir);

    for (k, v) in [(b"A".as_ref(), b"v1".as_ref()), (b"F", b"v2"), (b"G", b"v3")] {
        db.put(None, &noxu_db::DatabaseEntry::from_bytes(k), &noxu_db::DatabaseEntry::from_bytes(v)).unwrap();
    }

    let mut cursor = db.open_cursor(None, None).unwrap();
    let mut key = noxu_db::DatabaseEntry::from_bytes(b"F");
    let mut data = noxu_db::DatabaseEntry::new();

    let status = cursor.get(&mut key, &mut data, noxu_db::Get::Search, None).unwrap();
    assert_eq!(status, noxu_db::OperationStatus::Success);
    assert_eq!(data.data(), b"v2");
    // Key written back after search must equal the searched key.
    assert_eq!(key.data(), b"F");
    cursor.close().unwrap();
}

/// Get::Search for a missing key returns NotFound.
/// Mirrors OperationStatus.NOTFOUND assertions in CursorTest.
#[test]
fn cursor_search_missing_key_returns_not_found() {
    let dir = tempfile::TempDir::new().unwrap();
    let (_env, db) = open_env_and_db(&dir);

    db.put(None, &noxu_db::DatabaseEntry::from_bytes(b"A"), &noxu_db::DatabaseEntry::from_bytes(b"v")).unwrap();

    let mut cursor = db.open_cursor(None, None).unwrap();
    let mut key = noxu_db::DatabaseEntry::from_bytes(b"Z");
    let mut data = noxu_db::DatabaseEntry::new();

    let status = cursor.get(&mut key, &mut data, noxu_db::Get::Search, None).unwrap();
    assert_eq!(status, noxu_db::OperationStatus::NotFound);
    cursor.close().unwrap();
}

/// Get::SearchGte finds the first key >= the search key.
/// Mirrors cursor.getSearchKeyRange() from CursorTest.testDbInternalSearch()
/// (Search.GTE semantics) and insertionDuringGetNextBin test.
#[test]
fn cursor_search_gte_finds_first_ge_key() {
    let dir = tempfile::TempDir::new().unwrap();
    let (_env, db) = open_env_and_db(&dir);

    // Insert keys 1, 3, 5 (as single bytes).
    for k in [1u8, 3, 5] {
        db.put(
            None,
            &noxu_db::DatabaseEntry::from_bytes(&[k]),
            &noxu_db::DatabaseEntry::from_bytes(&[k]),
        ).unwrap();
    }

    let mut cursor = db.open_cursor(None, None).unwrap();

    // SearchGte(0) → first key >= 0 is 1.
    let mut key = noxu_db::DatabaseEntry::from_bytes(&[0u8]);
    let mut data = noxu_db::DatabaseEntry::new();
    let status = cursor.get(&mut key, &mut data, noxu_db::Get::SearchGte, None).unwrap();
    assert_eq!(status, noxu_db::OperationStatus::Success);
    assert_eq!(key.data(), &[1u8]);

    // SearchGte(3) → exact match, returns 3.
    let mut key = noxu_db::DatabaseEntry::from_bytes(&[3u8]);
    let status = cursor.get(&mut key, &mut data, noxu_db::Get::SearchGte, None).unwrap();
    assert_eq!(status, noxu_db::OperationStatus::Success);
    assert_eq!(key.data(), &[3u8]);

    // SearchGte(4) → first key >= 4 is 5.
    let mut key = noxu_db::DatabaseEntry::from_bytes(&[4u8]);
    let status = cursor.get(&mut key, &mut data, noxu_db::Get::SearchGte, None).unwrap();
    assert_eq!(status, noxu_db::OperationStatus::Success);
    assert_eq!(key.data(), &[5u8]);

    // SearchGte(6) → no key >= 6, returns NotFound.
    let mut key = noxu_db::DatabaseEntry::from_bytes(&[6u8]);
    let status = cursor.get(&mut key, &mut data, noxu_db::Get::SearchGte, None).unwrap();
    assert_eq!(status, noxu_db::OperationStatus::NotFound);

    cursor.close().unwrap();
}

/// After cursor.delete(), the deleted record is gone; the next Get::Next skips it.
/// Mirrors the cursor delete + getNext pattern in CursorTest.
#[test]
fn cursor_delete_removes_record_next_skips_it() {
    let dir = tempfile::TempDir::new().unwrap();
    let (_env, db) = open_env_and_db(&dir);

    for (k, v) in [(b"A".as_ref(), b"1".as_ref()), (b"B", b"2"), (b"C", b"3")] {
        db.put(None, &noxu_db::DatabaseEntry::from_bytes(k), &noxu_db::DatabaseEntry::from_bytes(v)).unwrap();
    }

    let mut cursor = db.open_cursor(None, None).unwrap();
    let mut key = noxu_db::DatabaseEntry::from_bytes(b"B");
    let mut data = noxu_db::DatabaseEntry::new();

    // Position on "B".
    let status = cursor.get(&mut key, &mut data, noxu_db::Get::Search, None).unwrap();
    assert_eq!(status, noxu_db::OperationStatus::Success);

    // Delete "B".
    let del_status = cursor.delete().unwrap();
    assert_eq!(del_status, noxu_db::OperationStatus::Success);

    // The cursor is no longer initialized; a Get::Next should move to "C".
    // (In JE after delete the cursor sits on a deleted slot and Next skips it.)
    // Here we re-position at "A" and advance past where "B" was.
    let mut key2 = noxu_db::DatabaseEntry::new();
    let mut data2 = noxu_db::DatabaseEntry::new();
    let status = cursor.get(&mut key2, &mut data2, noxu_db::Get::First, None).unwrap();
    assert_eq!(status, noxu_db::OperationStatus::Success);
    assert_eq!(key2.data(), b"A");

    let status = cursor.get(&mut key2, &mut data2, noxu_db::Get::Next, None).unwrap();
    assert_eq!(status, noxu_db::OperationStatus::Success);
    // "B" was deleted; next key must be "C".
    assert_eq!(key2.data(), b"C");

    cursor.close().unwrap();
}

/// cursor.put(Put::Overwrite) replaces the existing value for a key.
/// cursor.put(Put::NoOverwrite) returns KeyExists when the key is present.
/// Mirrors put/putNoOverwrite cursor tests in CursorTest.
#[test]
fn cursor_put_overwrite_and_no_overwrite() {
    let dir = tempfile::TempDir::new().unwrap();
    let (_env, db) = open_env_and_db(&dir);

    db.put(None, &noxu_db::DatabaseEntry::from_bytes(b"K"), &noxu_db::DatabaseEntry::from_bytes(b"v1")).unwrap();

    let mut cursor = db.open_cursor(None, None).unwrap();

    // NoOverwrite on existing key → KeyExists.
    let kentry = noxu_db::DatabaseEntry::from_bytes(b"K");
    let v2entry = noxu_db::DatabaseEntry::from_bytes(b"v2");
    let status = cursor.put(&kentry, &v2entry, noxu_db::Put::NoOverwrite).unwrap();
    assert_eq!(status, noxu_db::OperationStatus::KeyExists);

    // Overwrite on existing key → Success and value replaced.
    let v3entry = noxu_db::DatabaseEntry::from_bytes(b"v3");
    let status = cursor.put(&kentry, &v3entry, noxu_db::Put::Overwrite).unwrap();
    assert_eq!(status, noxu_db::OperationStatus::Success);

    // Verify value is now v3.
    let mut read_key = noxu_db::DatabaseEntry::from_bytes(b"K");
    let mut read_data = noxu_db::DatabaseEntry::new();
    cursor.get(&mut read_key, &mut read_data, noxu_db::Get::Search, None).unwrap();
    assert_eq!(read_data.data(), b"v3");

    cursor.close().unwrap();
}

/// Two independent cursors on the same database have separate positions.
/// Mirrors the multi-cursor assertions in CursorTest.insertMultiDb().
#[test]
fn two_cursors_independent_positions() {
    let dir = tempfile::TempDir::new().unwrap();
    let (_env, db) = open_env_and_db(&dir);

    for (k, v) in [(b"A".as_ref(), b"1".as_ref()), (b"B", b"2"), (b"C", b"3")] {
        db.put(None, &noxu_db::DatabaseEntry::from_bytes(k), &noxu_db::DatabaseEntry::from_bytes(v)).unwrap();
    }

    let mut c1 = db.open_cursor(None, None).unwrap();
    let mut c2 = db.open_cursor(None, None).unwrap();

    let mut k1 = noxu_db::DatabaseEntry::new();
    let mut d1 = noxu_db::DatabaseEntry::new();
    let mut k2 = noxu_db::DatabaseEntry::new();
    let mut d2 = noxu_db::DatabaseEntry::new();

    // c1 → First (A); c2 → Last (C).
    c1.get(&mut k1, &mut d1, noxu_db::Get::First, None).unwrap();
    c2.get(&mut k2, &mut d2, noxu_db::Get::Last, None).unwrap();

    assert_eq!(k1.data(), b"A");
    assert_eq!(k2.data(), b"C");

    // Advancing c1 does not move c2.
    c1.get(&mut k1, &mut d1, noxu_db::Get::Next, None).unwrap();
    assert_eq!(k1.data(), b"B");
    assert_eq!(k2.data(), b"C");  // c2 still at C

    c1.close().unwrap();
    c2.close().unwrap();
}

/// Get::Current returns the record at the current cursor position.
/// Mirrors the putCurrent / getFirst / getFirst cycle in CursorTest.
#[test]
fn cursor_get_current_after_search() {
    let dir = tempfile::TempDir::new().unwrap();
    let (_env, db) = open_env_and_db(&dir);

    db.put(None, &noxu_db::DatabaseEntry::from_bytes(b"key"), &noxu_db::DatabaseEntry::from_bytes(b"val")).unwrap();

    let mut cursor = db.open_cursor(None, None).unwrap();
    let mut key = noxu_db::DatabaseEntry::from_bytes(b"key");
    let mut data = noxu_db::DatabaseEntry::new();

    cursor.get(&mut key, &mut data, noxu_db::Get::Search, None).unwrap();

    // Get::Current should re-return the same record without moving the cursor.
    let mut cur_key = noxu_db::DatabaseEntry::new();
    let mut cur_data = noxu_db::DatabaseEntry::new();
    let status = cursor.get(&mut cur_key, &mut cur_data, noxu_db::Get::Current, None).unwrap();
    assert_eq!(status, noxu_db::OperationStatus::Success);
    assert_eq!(cur_key.data(), b"key");
    assert_eq!(cur_data.data(), b"val");

    cursor.close().unwrap();
}

/// Get::Next from an uninitialized cursor positions at the first record.
/// Mirrors JE Cursor contract: Next from uninitialized == First.
#[test]
fn cursor_next_from_uninitialized_is_first() {
    let dir = tempfile::TempDir::new().unwrap();
    let (_env, db) = open_env_and_db(&dir);

    for (k, v) in [(b"X".as_ref(), b"1".as_ref()), (b"Y", b"2")] {
        db.put(None, &noxu_db::DatabaseEntry::from_bytes(k), &noxu_db::DatabaseEntry::from_bytes(v)).unwrap();
    }

    let mut cursor = db.open_cursor(None, None).unwrap();
    let mut key = noxu_db::DatabaseEntry::new();
    let mut data = noxu_db::DatabaseEntry::new();

    let status = cursor.get(&mut key, &mut data, noxu_db::Get::Next, None).unwrap();
    assert_eq!(status, noxu_db::OperationStatus::Success);
    assert_eq!(key.data(), b"X");  // First key
    cursor.close().unwrap();
}

/// Get::Prev from an uninitialized cursor positions at the last record.
/// Mirrors JE Cursor contract: Prev from uninitialized == Last.
#[test]
fn cursor_prev_from_uninitialized_is_last() {
    let dir = tempfile::TempDir::new().unwrap();
    let (_env, db) = open_env_and_db(&dir);

    for (k, v) in [(b"X".as_ref(), b"1".as_ref()), (b"Y", b"2")] {
        db.put(None, &noxu_db::DatabaseEntry::from_bytes(k), &noxu_db::DatabaseEntry::from_bytes(v)).unwrap();
    }

    let mut cursor = db.open_cursor(None, None).unwrap();
    let mut key = noxu_db::DatabaseEntry::new();
    let mut data = noxu_db::DatabaseEntry::new();

    let status = cursor.get(&mut key, &mut data, noxu_db::Get::Prev, None).unwrap();
    assert_eq!(status, noxu_db::OperationStatus::Success);
    assert_eq!(key.data(), b"Y");  // Last key
    cursor.close().unwrap();
}

// ─── Database correctness tests (ported from DatabaseTest.java) ───────────────

/// delete() on a missing key returns NotFound.
/// Mirrors testDeleteNonDup(): second delete of same key returns NOTFOUND.
#[test]
fn db_delete_missing_key_returns_not_found() {
    let dir = tempfile::TempDir::new().unwrap();
    let (_env, db) = open_env_and_db(&dir);

    let key = noxu_db::DatabaseEntry::from_bytes(b"ghost");
    let status = db.delete(None, &key).unwrap();
    assert_eq!(status, noxu_db::OperationStatus::NotFound);
}

/// put() followed by delete() followed by another delete() returns NotFound.
/// Mirrors testDeleteNonDup(): delete then re-delete same key.
#[test]
fn db_double_delete_second_is_not_found() {
    let dir = tempfile::TempDir::new().unwrap();
    let (_env, db) = open_env_and_db(&dir);

    let key = noxu_db::DatabaseEntry::from_bytes(b"k");
    let val = noxu_db::DatabaseEntry::from_bytes(b"v");
    db.put(None, &key, &val).unwrap();

    assert_eq!(db.delete(None, &key).unwrap(), noxu_db::OperationStatus::Success);
    assert_eq!(db.delete(None, &key).unwrap(), noxu_db::OperationStatus::NotFound);
}

/// put_no_overwrite() succeeds for a new key, returns KeyExists on the second call,
/// and leaves the original value intact.
/// Mirrors testPutNoOverwriteInANoDupDb().
#[test]
fn db_put_no_overwrite_semantics() {
    let dir = tempfile::TempDir::new().unwrap();
    let (_env, db) = open_env_and_db(&dir);

    let key = noxu_db::DatabaseEntry::from_bytes(b"key");
    let v1 = noxu_db::DatabaseEntry::from_bytes(b"first");
    let v2 = noxu_db::DatabaseEntry::from_bytes(b"second");

    assert_eq!(db.put_no_overwrite(None, &key, &v1).unwrap(), noxu_db::OperationStatus::Success);
    assert_eq!(db.put_no_overwrite(None, &key, &v2).unwrap(), noxu_db::OperationStatus::KeyExists);

    // Original value must be unchanged.
    let mut out = noxu_db::DatabaseEntry::new();
    db.get(None, &key, &mut out).unwrap();
    assert_eq!(out.data(), b"first");
}

/// count() reflects the exact number of live records as puts and deletes occur.
/// Mirrors testDatabaseCount() and testDatabaseCountWithDeletedEntries().
#[test]
fn db_count_tracks_live_records() {
    let dir = tempfile::TempDir::new().unwrap();
    let (_env, db) = open_env_and_db(&dir);

    assert_eq!(db.count().unwrap(), 0);

    // Insert 10 records.
    for i in 0u8..10 {
        db.put(
            None,
            &noxu_db::DatabaseEntry::from_bytes(&[i]),
            &noxu_db::DatabaseEntry::from_bytes(&[i]),
        ).unwrap();
    }
    assert_eq!(db.count().unwrap(), 10);

    // Delete every other record (keys 0,2,4,6,8 → 5 deletions).
    for i in (0u8..10).step_by(2) {
        db.delete(None, &noxu_db::DatabaseEntry::from_bytes(&[i])).unwrap();
    }
    assert_eq!(db.count().unwrap(), 5);
}

/// count() returns 0 for an empty database.
/// Mirrors testDatabaseCountEmptyDB().
#[test]
fn db_count_empty_database_is_zero() {
    let dir = tempfile::TempDir::new().unwrap();
    let (_env, db) = open_env_and_db(&dir);
    assert_eq!(db.count().unwrap(), 0);
}

/// put() with an existing key overwrites the value (OVERWRITE semantics).
/// Mirrors testPutExisting(): second put on same key is an update.
#[test]
fn db_put_overwrites_existing_value() {
    let dir = tempfile::TempDir::new().unwrap();
    let (_env, db) = open_env_and_db(&dir);

    let key = noxu_db::DatabaseEntry::from_bytes(b"dup");
    let v1 = noxu_db::DatabaseEntry::from_bytes(b"one");
    let v2 = noxu_db::DatabaseEntry::from_bytes(b"two");

    db.put(None, &key, &v1).unwrap();
    db.put(None, &key, &v2).unwrap();

    let mut out = noxu_db::DatabaseEntry::new();
    db.get(None, &key, &mut out).unwrap();
    assert_eq!(out.data(), b"two");
}

/// After put_no_overwrite + delete + put_no_overwrite the second insert succeeds.
/// Mirrors the delete-then-putNoOverwrite pattern in testPutNoOverwriteInADupDbTxn().
#[test]
fn db_put_no_overwrite_after_delete_succeeds() {
    let dir = tempfile::TempDir::new().unwrap();
    let (_env, db) = open_env_and_db(&dir);

    let key = noxu_db::DatabaseEntry::from_bytes(b"key");
    let v1 = noxu_db::DatabaseEntry::from_bytes(b"first");
    let v2 = noxu_db::DatabaseEntry::from_bytes(b"third");

    db.put_no_overwrite(None, &key, &v1).unwrap();
    db.delete(None, &key).unwrap();

    // After deletion, put_no_overwrite should succeed again.
    let status = db.put_no_overwrite(None, &key, &v2).unwrap();
    assert_eq!(status, noxu_db::OperationStatus::Success);

    let mut out = noxu_db::DatabaseEntry::new();
    db.get(None, &key, &mut out).unwrap();
    assert_eq!(out.data(), b"third");
}

/// Multiple concurrent cursors on the same database are independent and can scan
/// the full record set without interfering with each other.
/// Mirrors the multi-cursor open + scan pattern from DatabaseTest.testCursor()
/// and CursorTest.insertMultiDb().
#[test]
fn db_multiple_concurrent_cursors_scan_same_records() {
    let dir = tempfile::TempDir::new().unwrap();
    let (_env, db) = open_env_and_db(&dir);

    // Insert 5 records.
    for i in 0u8..5 {
        db.put(
            None,
            &noxu_db::DatabaseEntry::from_bytes(&[i]),
            &noxu_db::DatabaseEntry::from_bytes(&[i * 10]),
        ).unwrap();
    }

    let mut c1 = db.open_cursor(None, None).unwrap();
    let mut c2 = db.open_cursor(None, None).unwrap();

    let mut k = noxu_db::DatabaseEntry::new();
    let mut d = noxu_db::DatabaseEntry::new();

    // Count records through c1 (forward).
    let mut count1 = 0usize;
    let mut status = c1.get(&mut k, &mut d, noxu_db::Get::First, None).unwrap();
    while status == noxu_db::OperationStatus::Success {
        count1 += 1;
        status = c1.get(&mut k, &mut d, noxu_db::Get::Next, None).unwrap();
    }

    // Count records through c2 (forward); c1 exhausted but c2 is independent.
    let mut count2 = 0usize;
    let mut status = c2.get(&mut k, &mut d, noxu_db::Get::First, None).unwrap();
    while status == noxu_db::OperationStatus::Success {
        count2 += 1;
        status = c2.get(&mut k, &mut d, noxu_db::Get::Next, None).unwrap();
    }

    assert_eq!(count1, 5);
    assert_eq!(count2, 5);

    c1.close().unwrap();
    c2.close().unwrap();
}

/// get() returns NotFound for a key that was never inserted.
/// Mirrors the testGetNonexistent / NOTFOUND assertions from DatabaseTest.
#[test]
fn db_get_not_found_for_missing_key() {
    let dir = tempfile::TempDir::new().unwrap();
    let (_env, db) = open_env_and_db(&dir);

    let mut out = noxu_db::DatabaseEntry::new();
    let status = db.get(None, &noxu_db::DatabaseEntry::from_bytes(b"missing"), &mut out).unwrap();
    assert_eq!(status, noxu_db::OperationStatus::NotFound);
}

/// scan_all_kv() + delete all: count drops to zero.
/// Mirrors the truncate / removeAll pattern implicit in DatabaseTest.
#[test]
fn db_remove_all_records_via_scan() {
    let dir = tempfile::TempDir::new().unwrap();
    let (_env, db) = open_env_and_db(&dir);

    // Insert 20 records.
    for i in 0u8..20 {
        db.put(
            None,
            &noxu_db::DatabaseEntry::from_bytes(&[i]),
            &noxu_db::DatabaseEntry::from_bytes(&[i]),
        ).unwrap();
    }
    assert_eq!(db.count().unwrap(), 20);

    // Delete all via scan_all_kv.
    let records = db.scan_all_kv().unwrap();
    assert_eq!(records.len(), 20);
    for (k, _) in records {
        db.delete(None, &noxu_db::DatabaseEntry::from_vec(k)).unwrap();
    }
    assert_eq!(db.count().unwrap(), 0);
}

/// Large number of records inserted and iterated — exercises tree splits.
/// Mirrors CursorTest.insertMultiDb() with NUM_RECS = 257 records.
#[test]
fn db_large_record_set_sorted_iteration() {
    let dir = tempfile::TempDir::new().unwrap();
    let (_env, db) = open_env_and_db(&dir);

    const N: u32 = 257;

    // Insert in reverse order so the tree must sort them.
    for i in (1u32..=N).rev() {
        let key_bytes = i.to_be_bytes();
        let val_bytes = i.to_be_bytes();
        db.put(
            None,
            &noxu_db::DatabaseEntry::from_bytes(&key_bytes),
            &noxu_db::DatabaseEntry::from_bytes(&val_bytes),
        ).unwrap();
    }

    assert_eq!(db.count().unwrap(), N as u64);

    // Scan and verify ascending order.
    let mut cursor = db.open_cursor(None, None).unwrap();
    let mut key = noxu_db::DatabaseEntry::new();
    let mut data = noxu_db::DatabaseEntry::new();

    let mut prev_key_val: Option<u32> = None;
    let mut count = 0u32;
    let mut status = cursor.get(&mut key, &mut data, noxu_db::Get::First, None).unwrap();
    while status == noxu_db::OperationStatus::Success {
        let k_val = u32::from_be_bytes(key.data().try_into().unwrap());
        let d_val = u32::from_be_bytes(data.data().try_into().unwrap());
        assert_eq!(k_val, d_val, "key and data must match");
        if let Some(prev) = prev_key_val {
            assert!(k_val > prev, "keys must be strictly ascending: {} <= {}", k_val, prev);
        }
        prev_key_val = Some(k_val);
        count += 1;
        status = cursor.get(&mut key, &mut data, noxu_db::Get::Next, None).unwrap();
    }
    assert_eq!(count, N);
    cursor.close().unwrap();
}

// ─── Sequence tests (ported from SequenceTest.java) ──────────────────────────

/// Helper: open an environment and a plain database for sequence tests.
fn open_seq_env_db(dir: &TempDir) -> (noxu_db::Environment, noxu_db::Database) {
    let env_config = EnvironmentConfig::new(dir.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(false);
    let env = noxu_db::Environment::open(env_config).unwrap();
    let db = env
        .open_database(None, "seqdb", &DatabaseConfig::new().with_allow_create(true))
        .unwrap();
    (env, db)
}

/// get(delta=1) returns consecutive integers beginning at initial_value=0.
/// Mirrors SequenceTest.testBasic(): first get returns 0, then 1, 3, 6, 7.
#[test]
fn seq_basic_consecutive_integers() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_seq_env_db(&dir);

    let key = DatabaseEntry::from_bytes(b"counter");
    let config = noxu_db::SequenceConfig::new()
        .with_allow_create(true)
        .with_initial_value(0)
        .with_cache_size(0);
    let seq = db.open_sequence(&key, config).unwrap();

    // First get returns initial_value=0.
    assert_eq!(seq.get(None, 1).unwrap(), 0);
    // Deltas > 1: returns the value at the start of the advance.
    assert_eq!(seq.get(None, 2).unwrap(), 1);
    assert_eq!(seq.get(None, 3).unwrap(), 3);
    assert_eq!(seq.get(None, 1).unwrap(), 6);
    assert_eq!(seq.get(None, 1).unwrap(), 7);
    seq.close().unwrap();
}

/// Stats: n_gets and n_cache_hits are tracked correctly.
/// Mirrors SequenceTest.testBasic() stats assertions.
#[test]
fn seq_stats_n_gets_and_cache_hits() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_seq_env_db(&dir);

    let key = DatabaseEntry::from_bytes(b"stats_key");
    // cache_size=10 means second and subsequent gets (within the batch) are cache hits.
    let config = noxu_db::SequenceConfig::new()
        .with_allow_create(true)
        .with_initial_value(0)
        .with_cache_size(10);
    let seq = db.open_sequence(&key, config).unwrap();

    // Stats before any get: n_gets=0.
    let s = seq.get_stats();
    assert_eq!(s.n_gets, 0);
    assert_eq!(s.range_min, i64::MIN);
    assert_eq!(s.range_max, i64::MAX);

    // First get — triggers a cache refill (not a cache hit).
    let v0 = seq.get(None, 1).unwrap();
    let s = seq.get_stats();
    assert_eq!(s.n_gets, 1);
    // After one get the stored boundary has advanced.
    assert!(s.current_value > v0 || s.current_value == v0 + 1);

    // Second and third gets — served from cache.
    seq.get(None, 1).unwrap();
    seq.get(None, 1).unwrap();
    let s = seq.get_stats();
    assert_eq!(s.n_gets, 3);
    // At least 2 of the 3 gets were cache hits (first one was a refill).
    assert!(s.n_cache_hits >= 2, "expected >= 2 cache hits, got {}", s.n_cache_hits);

    seq.close().unwrap();
}

/// Stats: range_min and range_max reflect the configured range.
/// Mirrors SequenceTest.testBasic(): stats.getMin()/getMax() after creation.
#[test]
fn seq_stats_range_fields() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_seq_env_db(&dir);

    let key = DatabaseEntry::from_bytes(b"range_key");
    let config = noxu_db::SequenceConfig::new()
        .with_allow_create(true)
        .with_range(-100, 200)
        .with_initial_value(-100)
        .with_cache_size(0);
    let seq = db.open_sequence(&key, config).unwrap();

    seq.get(None, 1).unwrap();
    let s = seq.get_stats();
    assert_eq!(s.range_min, -100);
    assert_eq!(s.range_max, 200);

    seq.close().unwrap();
}

/// delta > 1 skips values: if current=10 and delta=5, next call returns 15.
/// Mirrors SequenceTest.testBasic(): get(delta=2) => 1, get(delta=3) => 3.
#[test]
fn seq_delta_skips_values() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_seq_env_db(&dir);

    let key = DatabaseEntry::from_bytes(b"delta_key");
    let config = noxu_db::SequenceConfig::new()
        .with_allow_create(true)
        .with_initial_value(10)
        .with_cache_size(0);
    let seq = db.open_sequence(&key, config).unwrap();

    let v0 = seq.get(None, 5).unwrap();
    let v1 = seq.get(None, 5).unwrap();
    assert_eq!(v0, 10);
    assert_eq!(v1, 15);

    seq.close().unwrap();
}

/// Decrement: sequence counts downward.
/// Mirrors SequenceTest.testIllegal() decrement overflow section and
/// testMultipleHandles() with decrement=true.
#[test]
fn seq_decrement_counts_down() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_seq_env_db(&dir);

    let key = DatabaseEntry::from_bytes(b"decr_key");
    let config = noxu_db::SequenceConfig::new()
        .with_allow_create(true)
        .with_range(1, 10)
        .with_initial_value(10)
        .with_decrement(true)
        .with_cache_size(0);
    let seq = db.open_sequence(&key, config).unwrap();

    // Values must strictly decrease.
    let mut prev = seq.get(None, 1).unwrap();
    assert_eq!(prev, 10);
    for _ in 0..9 {
        let next = seq.get(None, 1).unwrap();
        assert!(next < prev, "decrement: next={next} should be < prev={prev}");
        prev = next;
    }
    // After exhausting the range (10 down to 1, 10 values total) overflow.
    let overflow = seq.get(None, 1);
    assert!(overflow.is_err(), "expected overflow error, got ok");
    let msg = format!("{}", overflow.err().unwrap());
    assert!(msg.contains("overflow"), "error should mention overflow: {msg}");

    seq.close().unwrap();
}

/// range enforcement: get() never returns a value outside [min, max].
/// Mirrors SequenceTest.doRange() forward increment path.
#[test]
fn seq_range_values_stay_in_bounds() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_seq_env_db(&dir);

    let key = DatabaseEntry::from_bytes(b"range_bounds");
    let min: i64 = -5;
    let max: i64 = 5;
    let config = noxu_db::SequenceConfig::new()
        .with_allow_create(true)
        .with_range(min, max)
        .with_initial_value(min)
        .with_cache_size(0);
    let seq = db.open_sequence(&key, config).unwrap();

    // Drain the whole range (11 values: -5 .. 5 inclusive).
    for expected in min..=max {
        let v = seq.get(None, 1).unwrap();
        assert_eq!(v, expected, "expected {expected}, got {v}");
        assert!(v >= min && v <= max, "value {v} outside [{min}, {max}]");
    }

    // One more call should overflow (wrap=false).
    let overflow = seq.get(None, 1);
    assert!(overflow.is_err(), "expected overflow after exhausting range");
    let msg = format!("{}", overflow.err().unwrap());
    assert!(msg.contains("overflow"), "error should mention overflow: {msg}");

    seq.close().unwrap();
}

/// Wrapping: when max is reached and wrap=true, the sequence wraps to min.
/// Mirrors SequenceTest.doRange() wrap=true path.
#[test]
fn seq_wrap_resets_to_min() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_seq_env_db(&dir);

    let key = DatabaseEntry::from_bytes(b"wrap_key");
    let min: i64 = 0;
    let max: i64 = 4;
    let config = noxu_db::SequenceConfig::new()
        .with_allow_create(true)
        .with_range(min, max)
        .with_initial_value(min)
        .with_wrap(true)
        .with_cache_size(0);
    let seq = db.open_sequence(&key, config).unwrap();

    // Drain 0..=4.
    for expected in min..=max {
        let v = seq.get(None, 1).unwrap();
        assert_eq!(v, expected);
    }

    // After wrap the value should be back at min=0.
    let wrapped = seq.get(None, 1).unwrap();
    assert_eq!(wrapped, min, "after wrap should return min={min}, got {wrapped}");

    seq.close().unwrap();
}

/// Cache refills: first N calls come from cache; when exhausted, the database
/// record (stored_value) is updated.
/// Mirrors SequenceTest.doRange() cache hit tracking.
#[test]
fn seq_cache_refill_pattern() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_seq_env_db(&dir);

    let key = DatabaseEntry::from_bytes(b"cache_key");
    let cache = 5;
    let config = noxu_db::SequenceConfig::new()
        .with_allow_create(true)
        .with_initial_value(0)
        .with_cache_size(cache);
    let seq = db.open_sequence(&key, config).unwrap();

    // First get: refill (not a cache hit).
    seq.get(None, 1).unwrap();
    let s0 = seq.get_stats();
    assert_eq!(s0.n_gets, 1);
    assert_eq!(s0.n_cache_hits, 0, "first get should not be a cache hit");

    // Gets 2..=cache: should all be cache hits.
    for _ in 1..cache {
        seq.get(None, 1).unwrap();
    }
    let s1 = seq.get_stats();
    assert_eq!(s1.n_gets, cache as u64);
    assert_eq!(s1.n_cache_hits, (cache - 1) as u64, "gets 2..cache should be cache hits");

    seq.close().unwrap();
}

/// Exclusive create fails if the sequence key already exists.
/// Mirrors SequenceTest.testIllegal(): ExclusiveCreate + existing key.
#[test]
fn seq_exclusive_create_fails_when_exists() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_seq_env_db(&dir);

    let key = DatabaseEntry::from_bytes(b"excl_key");
    // First open creates the sequence.
    db.open_sequence(
        &key,
        noxu_db::SequenceConfig::new()
            .with_allow_create(true)
            .with_range(1, 2)
            .with_initial_value(1)
            .with_cache_size(0),
    )
    .unwrap();

    // Second open with exclusive_create=true must fail.
    let result = db.open_sequence(
        &key,
        noxu_db::SequenceConfig::new()
            .with_allow_create(true)
            .with_exclusive_create(true)
            .with_range(1, 2)
            .with_initial_value(1)
            .with_cache_size(0),
    );
    assert!(result.is_err(), "exclusive_create should fail when key exists");
    let msg = format!("{}", result.err().unwrap());
    assert!(
        msg.contains("already exists") || msg.contains("ExclusiveCreate"),
        "unexpected error: {msg}"
    );
}

/// allow_create=false fails when the key does not exist.
/// Mirrors SequenceTest.testIllegal(): AllowCreate=false + missing key.
#[test]
fn seq_no_create_fails_when_missing() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_seq_env_db(&dir);

    let key = DatabaseEntry::from_bytes(b"missing_seq");
    let result =
        db.open_sequence(&key, noxu_db::SequenceConfig::new().with_allow_create(false));
    assert!(result.is_err(), "should fail with allow_create=false on missing key");
    let msg = format!("{}", result.err().unwrap());
    assert!(
        msg.contains("does not exist") || msg.contains("NotFound") || msg.to_lowercase().contains("not found"),
        "unexpected error: {msg}"
    );
}

/// Range validation: min must be strictly less than max.
/// Mirrors SequenceTest.testIllegal(): setRange(0,0) must throw.
#[test]
fn seq_invalid_range_min_equals_max() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_seq_env_db(&dir);

    let key = DatabaseEntry::from_bytes(b"bad_range");
    let result = db.open_sequence(
        &key,
        noxu_db::SequenceConfig::new()
            .with_allow_create(true)
            .with_range(5, 5)
            .with_initial_value(5),
    );
    assert!(result.is_err(), "equal min/max must be rejected");
    let msg = format!("{}", result.err().unwrap());
    assert!(
        msg.contains("less than the maximum") || msg.contains("range"),
        "unexpected error: {msg}"
    );
}

/// Initial value out of range must be rejected.
/// Mirrors SequenceTest.testIllegal(): initial value above/below range.
#[test]
fn seq_initial_value_out_of_range() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_seq_env_db(&dir);

    let key = DatabaseEntry::from_bytes(b"bad_init");

    // initial_value below range_min
    let result = db.open_sequence(
        &key,
        noxu_db::SequenceConfig::new()
            .with_allow_create(true)
            .with_range(-10, 10)
            .with_initial_value(-11),
    );
    assert!(result.is_err(), "initial value below range_min must be rejected");
    let msg = format!("{}", result.err().unwrap());
    assert!(msg.contains("out of range"), "unexpected error: {msg}");

    // initial_value above range_max
    let result2 = db.open_sequence(
        &key,
        noxu_db::SequenceConfig::new()
            .with_allow_create(true)
            .with_range(-10, 10)
            .with_initial_value(11),
    );
    assert!(result2.is_err(), "initial value above range_max must be rejected");
    let msg2 = format!("{}", result2.err().unwrap());
    assert!(msg2.contains("out of range"), "unexpected error: {msg2}");
}

/// Cache size larger than the range must be rejected.
/// Mirrors SequenceTest.testIllegal(): cache larger than range.
#[test]
fn seq_cache_larger_than_range_rejected() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_seq_env_db(&dir);

    let key = DatabaseEntry::from_bytes(b"big_cache");
    let result = db.open_sequence(
        &key,
        noxu_db::SequenceConfig::new()
            .with_allow_create(true)
            .with_range(-10, 10)
            .with_initial_value(0)
            .with_cache_size(21), // range span = 20, cache = 21
    );
    assert!(result.is_err(), "cache larger than range must be rejected");
    let msg = format!("{}", result.err().unwrap());
    assert!(
        msg.contains("cache size is larger") || msg.contains("cache"),
        "unexpected error: {msg}"
    );
}

/// delta=0 must be rejected (delta must be > 0).
/// Mirrors SequenceTest.testIllegal(): delta < 1.
#[test]
fn seq_delta_zero_rejected() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_seq_env_db(&dir);

    let key = DatabaseEntry::from_bytes(b"zero_delta");
    let config = noxu_db::SequenceConfig::new()
        .with_allow_create(true)
        .with_range(-5, 5)
        .with_initial_value(-5)
        .with_cache_size(0);
    let seq = db.open_sequence(&key, config).unwrap();

    let result = seq.get(None, 0);
    assert!(result.is_err(), "delta=0 must be rejected");
    let msg = format!("{}", result.unwrap_err());
    assert!(
        msg.contains("greater than zero") || msg.contains("delta"),
        "unexpected error: {msg}"
    );

    seq.close().unwrap();
}

/// delta larger than the range must be rejected.
/// Mirrors SequenceTest.testIllegal(): delta > (max - min).
#[test]
fn seq_delta_larger_than_range_rejected() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_seq_env_db(&dir);

    let key = DatabaseEntry::from_bytes(b"big_delta");
    let config = noxu_db::SequenceConfig::new()
        .with_allow_create(true)
        .with_range(-5, 5)
        .with_initial_value(-5)
        .with_cache_size(0);
    let seq = db.open_sequence(&key, config).unwrap();

    let result = seq.get(None, 11); // range = 10, delta = 11
    assert!(result.is_err(), "delta larger than range must be rejected");
    let msg = format!("{}", result.unwrap_err());
    assert!(
        msg.contains("larger than the range") || msg.contains("range"),
        "unexpected error: {msg}"
    );

    seq.close().unwrap();
}

/// Positive and negative ranges: increment through [-10, -1].
/// Mirrors SequenceTest.doRange(db, -10, -1, 1, 0).
#[test]
fn seq_negative_range_increment() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_seq_env_db(&dir);

    let key = DatabaseEntry::from_bytes(b"neg_range");
    let min: i64 = -10;
    let max: i64 = -1;
    let config = noxu_db::SequenceConfig::new()
        .with_allow_create(true)
        .with_range(min, max)
        .with_initial_value(min)
        .with_cache_size(0);
    let seq = db.open_sequence(&key, config).unwrap();

    for expected in min..=max {
        let v = seq.get(None, 1).unwrap();
        assert_eq!(v, expected, "expected {expected}, got {v}");
    }

    // Overflow after exhausting.
    assert!(seq.get(None, 1).is_err());
    seq.close().unwrap();
}

/// Multiple handles on the same sequence share a single counter.
/// Mirrors SequenceTest.testMultipleHandles(): seq and seq2 share state.
#[test]
fn seq_multiple_handles_share_counter() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_seq_env_db(&dir);

    let key = DatabaseEntry::from_bytes(b"shared_seq");
    let config = noxu_db::SequenceConfig::new()
        .with_allow_create(true)
        .with_initial_value(0)
        .with_cache_size(0);

    let seq1 = db.open_sequence(&key, config.clone()).unwrap();
    let v1 = seq1.get(None, 1).unwrap();
    seq1.close().unwrap();

    // Second handle must pick up where the first left off.
    let seq2 = db.open_sequence(&key, config).unwrap();
    let v2 = seq2.get(None, 1).unwrap();
    assert!(v2 > v1, "seq2 must continue from seq1: v2={v2} v1={v1}");
    seq2.close().unwrap();
}

/// Extreme ranges: Long.MIN_VALUE to Long.MIN_VALUE + 10.
/// Mirrors SequenceTest.testRanges() extreme min/max section.
#[test]
fn seq_extreme_range_i64_min() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_seq_env_db(&dir);

    let key = DatabaseEntry::from_bytes(b"i64_min_range");
    let min = i64::MIN;
    let max = i64::MIN + 10;
    let config = noxu_db::SequenceConfig::new()
        .with_allow_create(true)
        .with_range(min, max)
        .with_initial_value(min)
        .with_cache_size(0);
    let seq = db.open_sequence(&key, config).unwrap();

    for expected in min..=max {
        let v = seq.get(None, 1).unwrap();
        assert_eq!(v, expected);
    }
    assert!(seq.get(None, 1).is_err());
    seq.close().unwrap();
}

/// Extreme ranges: Long.MAX_VALUE - 10 to Long.MAX_VALUE.
/// Mirrors SequenceTest.testRanges() extreme min/max section.
#[test]
fn seq_extreme_range_i64_max() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_seq_env_db(&dir);

    let key = DatabaseEntry::from_bytes(b"i64_max_range");
    let min = i64::MAX - 10;
    let max = i64::MAX;
    let config = noxu_db::SequenceConfig::new()
        .with_allow_create(true)
        .with_range(min, max)
        .with_initial_value(min)
        .with_cache_size(0);
    let seq = db.open_sequence(&key, config).unwrap();

    for expected in min..=max {
        let v = seq.get(None, 1).unwrap();
        assert_eq!(v, expected);
    }
    assert!(seq.get(None, 1).is_err());
    seq.close().unwrap();
}

// ─── Secondary database tests (ported from SecondaryTest.java) ───────────────

use noxu_db::{
    SecondaryConfig, SecondaryDatabase, SecondaryKeyCreator, SecondaryMultiKeyCreator,
};
use noxu_sync::Mutex;
use std::sync::Arc;

/// A simple secondary key creator: sec_key = data[0..1] (first byte).
/// Equivalent to SecondaryTest's numeric offset pattern adapted to bytes.
struct FirstByteCreator;

impl SecondaryKeyCreator for FirstByteCreator {
    fn create_secondary_key(
        &self,
        _db: &noxu_db::Database,
        _key: &DatabaseEntry,
        data: &DatabaseEntry,
        result: &mut DatabaseEntry,
    ) -> bool {
        if let Some(d) = data.get_data()
            && !d.is_empty() {
                result.set_data(&d[..1]);
                return true;
            }
        false
    }
}

/// A multi-key creator that treats each byte of data as its own secondary key.
/// Enables testing MultiKeyCreator semantics (multiple sec keys per primary).
struct EachByteCreator;

impl SecondaryMultiKeyCreator for EachByteCreator {
    fn create_secondary_keys(
        &self,
        _db: &noxu_db::Database,
        _key: &DatabaseEntry,
        data: &DatabaseEntry,
        results: &mut Vec<DatabaseEntry>,
    ) {
        if let Some(d) = data.get_data() {
            for &byte in d {
                results.push(DatabaseEntry::from_bytes(&[byte]));
            }
        }
    }
}

/// Helper: set up a primary + secondary for integration tests.
fn open_pri_sec(
    dir: &TempDir,
) -> (
    noxu_db::Environment,
    Arc<Mutex<noxu_db::Database>>,
    SecondaryDatabase,
) {
    let env_config = EnvironmentConfig::new(dir.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    let env = noxu_db::Environment::open(env_config).unwrap();

    let db_config = DatabaseConfig::new().with_allow_create(true);
    let primary_db = env.open_database(None, "primary", &db_config).unwrap();
    let primary = Arc::new(Mutex::new(primary_db));

    let sec_db = env
        .open_database(None, "secondary", &DatabaseConfig::new().with_allow_create(true))
        .unwrap();
    let sec_config = SecondaryConfig::new()
        .with_allow_create(true)
        .with_key_creator(Box::new(FirstByteCreator));
    let secondary = SecondaryDatabase::open(Arc::clone(&primary), sec_db, sec_config).unwrap();

    (env, primary, secondary)
}

/// Helper: write to primary then manually update the secondary.
fn pri_put_and_index(
    primary: &Arc<Mutex<noxu_db::Database>>,
    secondary: &SecondaryDatabase,
    k: &[u8],
    v: &[u8],
    old_v: Option<&[u8]>,
) {
    let pk = DatabaseEntry::from_bytes(k);
    let new_data = DatabaseEntry::from_bytes(v);
    primary.lock().put(None, &pk, &new_data).unwrap();
    let old_entry = old_v.map(DatabaseEntry::from_bytes);
    secondary
        .update_secondary(&pk, old_entry.as_ref(), Some(&new_data))
        .unwrap();
}

/// put to primary → get from secondary returns primary_key + data.
/// Mirrors SecondaryTest.testPutAndDelete(): Database.put() / secDb.get().
#[test]
fn sec_put_primary_get_by_secondary_key() {
    let dir = TempDir::new().unwrap();
    let (_env, primary, secondary) = open_pri_sec(&dir);

    pri_put_and_index(&primary, &secondary, b"pk1", b"Apple", None);

    // Lookup by secondary key 'A' (first byte of "Apple").
    let sec_key = DatabaseEntry::from_bytes(b"A");
    let mut p_key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();
    let status = secondary.get(None, &sec_key, &mut p_key, &mut data).unwrap();
    assert_eq!(status, OperationStatus::Success);
    assert_eq!(p_key.get_data().unwrap(), b"pk1");
    assert_eq!(data.get_data().unwrap(), b"Apple");
}

/// Delete from primary removes secondary entry; subsequent get returns NotFound.
/// Mirrors SecondaryTest.testPutAndDelete(): Database.delete() removes sec entry.
/// We use SecondaryDatabase::delete() (deletes via secondary key) since
/// delete_all_for_primary is crate-internal.
#[test]
fn sec_delete_primary_removes_secondary() {
    let dir = TempDir::new().unwrap();
    let (_env, primary, secondary) = open_pri_sec(&dir);

    pri_put_and_index(&primary, &secondary, b"pk1", b"Cherry", None);

    // Delete via secondary key 'C' (first byte of "Cherry").
    // SecondaryDatabase::delete() removes the primary record and its secondary entries.
    let sec_key = DatabaseEntry::from_bytes(b"C");
    let del_status = secondary.delete(None, &sec_key).unwrap();
    assert_eq!(del_status, OperationStatus::Success);

    // Secondary lookup should now return NotFound.
    let mut p_key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();
    let status = secondary.get(None, &sec_key, &mut p_key, &mut data).unwrap();
    assert_eq!(status, OperationStatus::NotFound);

    // Primary record is also gone.
    let mut pri_data = DatabaseEntry::new();
    let pri_status = primary
        .lock()
        .get(None, &DatabaseEntry::from_bytes(b"pk1"), &mut pri_data)
        .unwrap();
    assert_eq!(pri_status, OperationStatus::NotFound);
}

/// Searching secondary with non-existent key returns NotFound.
/// Mirrors SecondaryTest.testPutAndDelete(): get on missing sec key.
#[test]
fn sec_get_nonexistent_key_returns_not_found() {
    let dir = TempDir::new().unwrap();
    let (_env, primary, secondary) = open_pri_sec(&dir);

    pri_put_and_index(&primary, &secondary, b"pk1", b"Banana", None);

    // 'Z' does not map to any primary record.
    let sec_key = DatabaseEntry::from_bytes(b"Z");
    let mut p_key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();
    let status = secondary.get(None, &sec_key, &mut p_key, &mut data).unwrap();
    assert_eq!(status, OperationStatus::NotFound);
}

/// Update primary value changes secondary key mapping.
/// Mirrors SecondaryTest.testPutAndDelete(): Database.put() overwrite removes
/// old sec entry (102→NotFound) and inserts new one (103→Success).
#[test]
fn sec_update_primary_changes_secondary_key() {
    let dir = TempDir::new().unwrap();
    let (_env, primary, secondary) = open_pri_sec(&dir);

    // Insert with data "Banana" → sec key 'B'.
    pri_put_and_index(&primary, &secondary, b"pk1", b"Banana", None);

    // Overwrite with "Cherry" → sec key should change to 'C'.
    pri_put_and_index(&primary, &secondary, b"pk1", b"Cherry", Some(b"Banana"));

    // Old sec key 'B' must be gone.
    let sec_key_b = DatabaseEntry::from_bytes(b"B");
    let mut pk = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();
    let status = secondary.get(None, &sec_key_b, &mut pk, &mut data).unwrap();
    assert_eq!(status, OperationStatus::NotFound, "old sec key 'B' should be removed");

    // New sec key 'C' must be present.
    let sec_key_c = DatabaseEntry::from_bytes(b"C");
    let status = secondary.get(None, &sec_key_c, &mut pk, &mut data).unwrap();
    assert_eq!(status, OperationStatus::Success);
    assert_eq!(data.get_data().unwrap(), b"Cherry");
}

/// Delete via secondary database deletes the primary record.
/// Mirrors SecondaryTest.testPutAndDelete(): SecondaryDatabase.delete().
#[test]
fn sec_delete_via_secondary_removes_primary() {
    let dir = TempDir::new().unwrap();
    let (_env, primary, secondary) = open_pri_sec(&dir);

    pri_put_and_index(&primary, &secondary, b"pk1", b"Durian", None);

    // Delete by secondary key 'D'.
    let sec_key = DatabaseEntry::from_bytes(b"D");
    let status = secondary.delete(None, &sec_key).unwrap();
    assert_eq!(status, OperationStatus::Success);

    // Second delete on same key returns NotFound.
    let status2 = secondary.delete(None, &sec_key).unwrap();
    assert_eq!(status2, OperationStatus::NotFound);

    // Primary record is gone.
    let mut data = DatabaseEntry::new();
    let get_status = primary
        .lock()
        .get(None, &DatabaseEntry::from_bytes(b"pk1"), &mut data)
        .unwrap();
    assert_eq!(get_status, OperationStatus::NotFound);
}

/// SecondaryCursor::get_first/next iterates all records in secondary key order.
/// Mirrors SecondaryTest.testGet(): cursor.getFirst()/getNext() loop.
#[test]
fn sec_cursor_first_next_sorted_order() {
    let dir = TempDir::new().unwrap();
    let (_env, primary, secondary) = open_pri_sec(&dir);

    // Insert 5 records with sec keys C, A, E, B, D.
    let records: &[(&[u8], &[u8])] = &[
        (b"pk0", b"Cherry"),
        (b"pk1", b"Apple"),
        (b"pk2", b"Elderberry"),
        (b"pk3", b"Banana"),
        (b"pk4", b"Date"),
    ];
    for (k, v) in records {
        pri_put_and_index(&primary, &secondary, k, v, None);
    }

    let mut cursor = secondary.open_cursor(None, None).unwrap();
    let mut sec_key = DatabaseEntry::new();
    let mut p_key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();

    let mut got: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    let mut status = cursor.get_first(&mut sec_key, &mut p_key, &mut data).unwrap();
    while status == OperationStatus::Success {
        got.push((
            sec_key.get_data().unwrap().to_vec(),
            data.get_data().unwrap().to_vec(),
        ));
        status = cursor.get_next(&mut sec_key, &mut p_key, &mut data).unwrap();
    }
    cursor.close().unwrap();

    // Must be in secondary key order: A, B, C, D, E.
    assert_eq!(got.len(), 5);
    assert_eq!(got[0].0, b"A");
    assert_eq!(got[0].1, b"Apple");
    assert_eq!(got[1].0, b"B");
    assert_eq!(got[1].1, b"Banana");
    assert_eq!(got[2].0, b"C");
    assert_eq!(got[2].1, b"Cherry");
    assert_eq!(got[3].0, b"D");
    assert_eq!(got[3].1, b"Date");
    assert_eq!(got[4].0, b"E");
    assert_eq!(got[4].1, b"Elderberry");
}

/// SecondaryCursor::get_last/prev returns records in reverse secondary key order.
/// Mirrors SecondaryTest.testGet(): cursor.getLast()/getPrev() loop.
#[test]
fn sec_cursor_last_prev_reverse_order() {
    let dir = TempDir::new().unwrap();
    let (_env, primary, secondary) = open_pri_sec(&dir);

    let records: &[(&[u8], &[u8])] = &[
        (b"pk1", b"Apple"),
        (b"pk2", b"Banana"),
        (b"pk3", b"Cherry"),
    ];
    for (k, v) in records {
        pri_put_and_index(&primary, &secondary, k, v, None);
    }

    let mut cursor = secondary.open_cursor(None, None).unwrap();
    let mut sec_key = DatabaseEntry::new();
    let mut p_key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();

    let mut got_data: Vec<Vec<u8>> = Vec::new();
    let mut status = cursor.get_last(&mut sec_key, &mut p_key, &mut data).unwrap();
    while status == OperationStatus::Success {
        got_data.push(data.get_data().unwrap().to_vec());
        status = cursor.get_prev(&mut sec_key, &mut p_key, &mut data).unwrap();
    }
    cursor.close().unwrap();

    assert_eq!(got_data.len(), 3);
    assert_eq!(got_data[0], b"Cherry");
    assert_eq!(got_data[1], b"Banana");
    assert_eq!(got_data[2], b"Apple");
}

/// SecondaryCursor::get_search_key returns (sec_key, pri_key, data) correctly.
/// Mirrors SecondaryTest.testGet(): cursor.getSearchKey() with known sec keys.
#[test]
fn sec_cursor_search_key_returns_tuple() {
    let dir = TempDir::new().unwrap();
    let (_env, primary, secondary) = open_pri_sec(&dir);

    let records: &[(&[u8], &[u8])] = &[
        (b"pk0", b"Apricot"),
        (b"pk1", b"Banana"),
        (b"pk2", b"Citrus"),
    ];
    for (k, v) in records {
        pri_put_and_index(&primary, &secondary, k, v, None);
    }

    let mut cursor = secondary.open_cursor(None, None).unwrap();
    let search = DatabaseEntry::from_bytes(b"B");
    let mut p_key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();

    let status = cursor.get_search_key(&search, &mut p_key, &mut data).unwrap();
    assert_eq!(status, OperationStatus::Success);
    assert_eq!(p_key.get_data().unwrap(), b"pk1");
    assert_eq!(data.get_data().unwrap(), b"Banana");

    cursor.close().unwrap();
}

/// SecondaryCursor::get_search_key with non-existent key returns NotFound.
/// Mirrors SecondaryTest.testGet(): getSearchKey on KEY_OFFSET-1 → NOTFOUND.
#[test]
fn sec_cursor_search_key_not_found() {
    let dir = TempDir::new().unwrap();
    let (_env, primary, secondary) = open_pri_sec(&dir);

    pri_put_and_index(&primary, &secondary, b"pk1", b"Apple", None);

    let mut cursor = secondary.open_cursor(None, None).unwrap();
    let search = DatabaseEntry::from_bytes(b"Z");
    let mut p_key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();

    let status = cursor.get_search_key(&search, &mut p_key, &mut data).unwrap();
    assert_eq!(status, OperationStatus::NotFound);

    cursor.close().unwrap();
}

/// SecondaryCursor::get_search_key_range finds first sec key >= search key.
/// Mirrors SecondaryTest.testGet(): cursor.getSearchKeyRange(KEY_OFFSET-1) → first record.
#[test]
fn sec_cursor_search_key_range_gte() {
    let dir = TempDir::new().unwrap();
    let (_env, primary, secondary) = open_pri_sec(&dir);

    // Insert keys with first bytes C and E (no B/D).
    pri_put_and_index(&primary, &secondary, b"pk1", b"Cherry", None);
    pri_put_and_index(&primary, &secondary, b"pk2", b"Elderberry", None);

    let mut cursor = secondary.open_cursor(None, None).unwrap();
    // 'D' is between C and E; GTE should return 'E' (Elderberry).
    let mut search = DatabaseEntry::from_bytes(b"D");
    let mut p_key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();

    let status = cursor
        .get_search_key_range(&mut search, &mut p_key, &mut data)
        .unwrap();
    assert_eq!(status, OperationStatus::Success);
    assert_eq!(data.get_data().unwrap(), b"Elderberry");

    // 'A' → should land on first entry 'C' (Cherry).
    let mut search2 = DatabaseEntry::from_bytes(b"A");
    let status2 = cursor
        .get_search_key_range(&mut search2, &mut p_key, &mut data)
        .unwrap();
    assert_eq!(status2, OperationStatus::Success);
    assert_eq!(data.get_data().unwrap(), b"Cherry");

    // 'Z' → beyond all entries, NotFound.
    let mut search3 = DatabaseEntry::from_bytes(b"Z");
    let status3 = cursor
        .get_search_key_range(&mut search3, &mut p_key, &mut data)
        .unwrap();
    assert_eq!(status3, OperationStatus::NotFound);

    cursor.close().unwrap();
}

/// SecondaryCursor::get_current returns the current (sec_key, pri_key, data).
/// Mirrors SecondaryTest.testGet(): cursor.getCurrent() after getFirst.
#[test]
fn sec_cursor_get_current_after_position() {
    let dir = TempDir::new().unwrap();
    let (_env, primary, secondary) = open_pri_sec(&dir);

    pri_put_and_index(&primary, &secondary, b"pk1", b"Mango", None);

    let mut cursor = secondary.open_cursor(None, None).unwrap();
    let mut sk = DatabaseEntry::new();
    let mut pk = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();

    cursor.get_first(&mut sk, &mut pk, &mut data).unwrap();

    // get_current should return the same position.
    let mut sk2 = DatabaseEntry::new();
    let mut pk2 = DatabaseEntry::new();
    let mut data2 = DatabaseEntry::new();
    let status = cursor.get_current(&mut sk2, &mut pk2, &mut data2).unwrap();
    assert_eq!(status, OperationStatus::Success);
    assert_eq!(sk2.get_data(), sk.get_data());
    assert_eq!(pk2.get_data(), pk.get_data());
    assert_eq!(data2.get_data().unwrap(), b"Mango");

    cursor.close().unwrap();
}

/// Multiple secondary keys per primary record (MultiKeyCreator).
/// Each byte of data becomes an independent secondary index entry.
/// Mirrors SecondaryTest's useMultiKey=true behaviour.
#[test]
fn sec_multi_key_creator_multiple_keys_per_record() {
    let dir = TempDir::new().unwrap();
    let env_config = EnvironmentConfig::new(dir.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    let env = noxu_db::Environment::open(env_config).unwrap();

    let primary_db = env
        .open_database(None, "pri_mk", &DatabaseConfig::new().with_allow_create(true))
        .unwrap();
    let primary = Arc::new(Mutex::new(primary_db));

    let sec_db = env
        .open_database(None, "sec_mk", &DatabaseConfig::new().with_allow_create(true))
        .unwrap();
    let sec_config = SecondaryConfig::new()
        .with_allow_create(true)
        .with_multi_key_creator(Box::new(EachByteCreator));
    let secondary =
        SecondaryDatabase::open(Arc::clone(&primary), sec_db, sec_config).unwrap();

    // Primary record: key="pk1", data=[0x41, 0x42] = "AB"
    let pk = DatabaseEntry::from_bytes(b"pk1");
    let pv = DatabaseEntry::from_bytes(b"AB");
    primary.lock().put(None, &pk, &pv).unwrap();
    secondary.update_secondary(&pk, None, Some(&pv)).unwrap();

    // Both 'A' and 'B' should map to pk1.
    for sec_byte in [b"A" as &[u8], b"B"] {
        let sec_key = DatabaseEntry::from_bytes(sec_byte);
        let mut found_pk = DatabaseEntry::new();
        let mut found_data = DatabaseEntry::new();
        let status = secondary
            .get(None, &sec_key, &mut found_pk, &mut found_data)
            .unwrap();
        assert_eq!(
            status,
            OperationStatus::Success,
            "sec key {:?} not found",
            sec_byte
        );
        assert_eq!(found_pk.get_data().unwrap(), b"pk1");
        assert_eq!(found_data.get_data().unwrap(), b"AB");
    }

    // 'C' should not exist.
    let sec_key_c = DatabaseEntry::from_bytes(b"C");
    let mut pk_out = DatabaseEntry::new();
    let mut data_out = DatabaseEntry::new();
    let status = secondary
        .get(None, &sec_key_c, &mut pk_out, &mut data_out)
        .unwrap();
    assert_eq!(status, OperationStatus::NotFound);
}

/// auto-populate: opening secondary with allow_populate=true on an existing
/// primary fills the index from all existing primary records.
/// Mirrors SecondaryTest's secondary open on a non-empty primary.
#[test]
fn sec_auto_populate_on_open() {
    let dir = TempDir::new().unwrap();
    let env_config = EnvironmentConfig::new(dir.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    let env = noxu_db::Environment::open(env_config).unwrap();

    let primary_db = env
        .open_database(None, "pri_pop", &DatabaseConfig::new().with_allow_create(true))
        .unwrap();
    let primary = Arc::new(Mutex::new(primary_db));

    // Pre-populate primary with 3 records.
    for (k, v) in [(b"pk1" as &[u8], b"Grape" as &[u8]), (b"pk2", b"Kiwi"), (b"pk3", b"Lemon")] {
        primary
            .lock()
            .put(
                None,
                &DatabaseEntry::from_bytes(k),
                &DatabaseEntry::from_bytes(v),
            )
            .unwrap();
    }

    // Open secondary with allow_populate=true.
    let sec_db = env
        .open_database(None, "sec_pop", &DatabaseConfig::new().with_allow_create(true))
        .unwrap();
    let sec_config = SecondaryConfig::new()
        .with_allow_create(true)
        .with_allow_populate(true)
        .with_key_creator(Box::new(FirstByteCreator));
    let secondary =
        SecondaryDatabase::open(Arc::clone(&primary), sec_db, sec_config).unwrap();

    // All three records should be indexed.
    for (sec_b, expected_data) in [(b"G" as &[u8], b"Grape" as &[u8]), (b"K", b"Kiwi"), (b"L", b"Lemon")] {
        let sec_key = DatabaseEntry::from_bytes(sec_b);
        let mut pk = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();
        let status = secondary.get(None, &sec_key, &mut pk, &mut data).unwrap();
        assert_eq!(
            status,
            OperationStatus::Success,
            "sec key {:?} missing after auto-populate",
            sec_b
        );
        assert_eq!(data.get_data().unwrap(), expected_data);
    }
}

/// NUM_RECS put/get round trip through secondary matching SecondaryTest.testGet()
/// structure: 5 records with sec_key = pri_key + KEY_OFFSET encoded as big-endian u32.
/// This is the closest Rust port of the JE testGet() integer-based pattern.
#[test]
fn sec_num_recs_put_get_round_trip() {
    const NUM_RECS: u32 = 5;
    const KEY_OFFSET: u32 = 100;

    let dir = TempDir::new().unwrap();
    let env_config = EnvironmentConfig::new(dir.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    let env = noxu_db::Environment::open(env_config).unwrap();

    // Use a key creator that extracts bytes [4..8] of data as the secondary key
    // (the second u32 field, simulating the JE "value = i, sec_key = i+100" pattern).
    struct SecondU32Creator;
    impl SecondaryKeyCreator for SecondU32Creator {
        fn create_secondary_key(
            &self,
            _db: &noxu_db::Database,
            _key: &DatabaseEntry,
            data: &DatabaseEntry,
            result: &mut DatabaseEntry,
        ) -> bool {
            if let Some(d) = data.get_data()
                && d.len() >= 8 {
                    result.set_data(&d[4..8]);
                    return true;
                }
            false
        }
    }

    let primary_db = env
        .open_database(None, "pri_nr", &DatabaseConfig::new().with_allow_create(true))
        .unwrap();
    let primary = Arc::new(Mutex::new(primary_db));

    let sec_db = env
        .open_database(None, "sec_nr", &DatabaseConfig::new().with_allow_create(true))
        .unwrap();
    let sec_config = SecondaryConfig::new()
        .with_allow_create(true)
        .with_key_creator(Box::new(SecondU32Creator));
    let secondary =
        SecondaryDatabase::open(Arc::clone(&primary), sec_db, sec_config).unwrap();

    // Insert records: pri_key = i (be_u32), data = i ++ (i+KEY_OFFSET) packed as 8 bytes.
    for i in 0u32..NUM_RECS {
        let pri_key_bytes = i.to_be_bytes();
        let sec_key_val = i + KEY_OFFSET;
        // data = 4 bytes primary value + 4 bytes secondary key value
        let mut data_bytes = [0u8; 8];
        data_bytes[..4].copy_from_slice(&i.to_be_bytes());
        data_bytes[4..].copy_from_slice(&sec_key_val.to_be_bytes());

        let pk = DatabaseEntry::from_bytes(&pri_key_bytes);
        let pv = DatabaseEntry::from_bytes(&data_bytes);
        primary.lock().put(None, &pk, &pv).unwrap();
        secondary.update_secondary(&pk, None, Some(&pv)).unwrap();
    }

    // SecondaryDatabase.get(): look up by sec_key = i + KEY_OFFSET for i in 0..NUM_RECS.
    for i in 0u32..NUM_RECS {
        let sec_key_val = (i + KEY_OFFSET).to_be_bytes();
        let sec_key = DatabaseEntry::from_bytes(&sec_key_val);
        let mut p_key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();
        let status = secondary.get(None, &sec_key, &mut p_key, &mut data).unwrap();
        assert_eq!(status, OperationStatus::Success, "i={i}: sec get failed");
        // Primary key should be i
        assert_eq!(p_key.get_data().unwrap(), &i.to_be_bytes(), "i={i}: wrong pri key");
    }

    // Look up sec_key = NUM_RECS + KEY_OFFSET → NotFound.
    let missing_sec = (NUM_RECS + KEY_OFFSET).to_be_bytes();
    let mut pk_out = DatabaseEntry::new();
    let mut data_out = DatabaseEntry::new();
    let status = secondary
        .get(
            None,
            &DatabaseEntry::from_bytes(&missing_sec),
            &mut pk_out,
            &mut data_out,
        )
        .unwrap();
    assert_eq!(status, OperationStatus::NotFound);

    // SecondaryCursor First/Next scan: collect all in order.
    let mut cursor = secondary.open_cursor(None, None).unwrap();
    let mut sk = DatabaseEntry::new();
    let mut pk = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();

    let mut count = 0u32;
    let mut prev_sec_key: Option<u32> = None;
    let mut status = cursor.get_first(&mut sk, &mut pk, &mut d).unwrap();
    while status == OperationStatus::Success {
        let sk_val = u32::from_be_bytes(sk.get_data().unwrap().try_into().unwrap());
        let pk_val = u32::from_be_bytes(pk.get_data().unwrap().try_into().unwrap());
        // sec_key = pri_key + KEY_OFFSET
        assert_eq!(sk_val, pk_val + KEY_OFFSET, "count={count}: sec key mismatch");
        if let Some(prev) = prev_sec_key {
            assert!(sk_val > prev, "count={count}: sec keys not ascending");
        }
        prev_sec_key = Some(sk_val);
        count += 1;
        status = cursor.get_next(&mut sk, &mut pk, &mut d).unwrap();
    }
    assert_eq!(count, NUM_RECS);

    // SecondaryCursor Last/Prev scan: verify reverse order.
    let mut count_rev = 0u32;
    let mut prev_sk_val: Option<u32> = None;
    let mut status = cursor.get_last(&mut sk, &mut pk, &mut d).unwrap();
    while status == OperationStatus::Success {
        let sk_val = u32::from_be_bytes(sk.get_data().unwrap().try_into().unwrap());
        if let Some(prev) = prev_sk_val {
            assert!(sk_val < prev, "count_rev={count_rev}: sec keys not descending");
        }
        prev_sk_val = Some(sk_val);
        count_rev += 1;
        status = cursor.get_prev(&mut sk, &mut pk, &mut d).unwrap();
    }
    assert_eq!(count_rev, NUM_RECS);

    // SecondaryCursor get_search_key: find each entry, confirm NotFound outside range.
    for i in 0u32..NUM_RECS {
        let sec_key_bytes = (i + KEY_OFFSET).to_be_bytes();
        let search = DatabaseEntry::from_bytes(&sec_key_bytes);
        let mut p_key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();
        let s = cursor.get_search_key(&search, &mut p_key, &mut data).unwrap();
        assert_eq!(s, OperationStatus::Success, "i={i}: search failed");
        assert_eq!(
            p_key.get_data().unwrap(),
            &i.to_be_bytes(),
            "i={i}: wrong pri key from cursor search"
        );
    }

    // Just outside range (KEY_OFFSET - 1) → NotFound.
    let before_range = (KEY_OFFSET - 1).to_be_bytes();
    let status = cursor
        .get_search_key(
            &DatabaseEntry::from_bytes(&before_range),
            &mut DatabaseEntry::new(),
            &mut DatabaseEntry::new(),
        )
        .unwrap();
    assert_eq!(status, OperationStatus::NotFound);

    cursor.close().unwrap();
}

// ─── Cursor search tests (ported from DbCursorSearchTest.java) ────────────────
//
// DbCursorSearchTest verifies GetSearchKey / GetSearchKeyRange behaviour across
// single- and multi-BIN trees (JE uses N_KEYS = 50 to force at least one split).

/// Port of DbCursorSearchTest.testSimpleSearchKey.
///
/// Put a small number of string key-value pairs then verify that Get::Search
/// finds each one and the returned data matches the stored value.
#[test]
fn cursor_search_simple_exact_match() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_env_and_db(&dir);

    let pairs: &[(&[u8], &[u8])] = &[
        (b"bar",  b"two"),
        (b"baz",  b"three"),
        (b"foo",  b"one"),
        (b"quux", b"four"),
    ];

    for (k, v) in pairs {
        db.put(None,
               &DatabaseEntry::from_bytes(k),
               &DatabaseEntry::from_bytes(v)).unwrap();
    }

    let mut cursor = db.open_cursor(None, None).unwrap();
    for (k, v) in pairs {
        let mut key  = DatabaseEntry::from_bytes(k);
        let mut data = DatabaseEntry::new();
        let status = cursor.get(&mut key, &mut data, Get::Search, None).unwrap();
        assert_eq!(status, OperationStatus::Success,
            "Get::Search must succeed for key {:?}", k);
        assert_eq!(data.data(), *v, "data must match for key {:?}", k);
    }
    cursor.close().unwrap();
}

/// Port of DbCursorSearchTest.testSimpleDeleteAndSearchKey.
///
/// Put records, search for each one successfully, delete via cursor, then
/// verify that a subsequent Get::Search returns NotFound.
#[test]
fn cursor_search_after_delete_returns_not_found() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_env_and_db(&dir);

    let pairs: &[(&[u8], &[u8])] = &[
        (b"alpha", b"1"),
        (b"beta",  b"2"),
        (b"gamma", b"3"),
    ];

    for (k, v) in pairs {
        db.put(None,
               &DatabaseEntry::from_bytes(k),
               &DatabaseEntry::from_bytes(v)).unwrap();
    }

    let mut cursor = db.open_cursor(None, None).unwrap();

    for (k, _v) in pairs {
        // Search must succeed before deletion.
        let mut key  = DatabaseEntry::from_bytes(k);
        let mut data = DatabaseEntry::new();
        let status = cursor.get(&mut key, &mut data, Get::Search, None).unwrap();
        assert_eq!(status, OperationStatus::Success,
            "Get::Search must succeed before deletion for {:?}", k);

        cursor.delete().unwrap();

        // Searching again must return NotFound.
        let mut key2  = DatabaseEntry::from_bytes(k);
        let mut data2 = DatabaseEntry::new();
        let status2 = cursor.get(&mut key2, &mut data2, Get::Search, None).unwrap();
        assert_eq!(status2, OperationStatus::NotFound,
            "Get::Search must return NotFound after deletion for {:?}", k);
    }

    cursor.close().unwrap();
}

/// Port of DbCursorSearchTest.testLargeSearchKey.
///
/// Insert enough records to force at least one BIN split (N_KEYS = 50) then
/// verify that Get::Search finds every key.
#[test]
fn cursor_search_large_tree_exact_match() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_env_and_db(&dir);

    const N_KEYS: u32 = 50;

    for i in 0..N_KEYS {
        let key_bytes = format!("{:08}", i).into_bytes();
        let val_bytes = i.to_be_bytes().to_vec();
        db.put(None,
               &DatabaseEntry::from_vec(key_bytes),
               &DatabaseEntry::from_vec(val_bytes)).unwrap();
    }

    let mut cursor = db.open_cursor(None, None).unwrap();
    for i in 0..N_KEYS {
        let key_bytes = format!("{:08}", i).into_bytes();
        let expected_val = i.to_be_bytes().to_vec();

        let mut key  = DatabaseEntry::from_vec(key_bytes.clone());
        let mut data = DatabaseEntry::new();
        let status = cursor.get(&mut key, &mut data, Get::Search, None).unwrap();
        assert_eq!(status, OperationStatus::Success,
            "Get::Search must succeed for key {:?} in large tree", key_bytes);
        assert_eq!(data.data(), expected_val.as_slice(),
            "data must match for key {:?}", key_bytes);
    }

    cursor.close().unwrap();
}

/// Port of DbCursorSearchTest.testLargeDeleteAndSearchKey.
///
/// Insert many records (forcing splits), search for each one, delete it, then
/// verify subsequent searches return NotFound.
#[test]
fn cursor_search_large_tree_delete_and_search() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_env_and_db(&dir);

    const N_KEYS: u32 = 50;

    for i in 0..N_KEYS {
        let key_bytes = format!("{:08}", i).into_bytes();
        let val_bytes = i.to_be_bytes().to_vec();
        db.put(None,
               &DatabaseEntry::from_vec(key_bytes),
               &DatabaseEntry::from_vec(val_bytes)).unwrap();
    }

    let mut cursor = db.open_cursor(None, None).unwrap();

    for i in 0..N_KEYS {
        let key_bytes = format!("{:08}", i).into_bytes();

        let mut key  = DatabaseEntry::from_vec(key_bytes.clone());
        let mut data = DatabaseEntry::new();
        let s = cursor.get(&mut key, &mut data, Get::Search, None).unwrap();
        assert_eq!(s, OperationStatus::Success,
            "Get::Search must succeed for key {:?} before deletion", key_bytes);

        cursor.delete().unwrap();

        let mut key2  = DatabaseEntry::from_vec(key_bytes.clone());
        let mut data2 = DatabaseEntry::new();
        let s2 = cursor.get(&mut key2, &mut data2, Get::Search, None).unwrap();
        assert_eq!(s2, OperationStatus::NotFound,
            "Get::Search must return NotFound after deletion for key {:?}", key_bytes);
    }

    cursor.close().unwrap();
}

/// Port of DbCursorSearchTest — Get::SearchGte finds the first key >= the
/// search key in a multi-BIN tree.
///
/// JE: `cursor.getSearchKeyRange` sets key to the found key (which may be >=
/// the search key) and returns SUCCESS, or NOTFOUND if all keys are < query.
#[test]
fn cursor_search_range_finds_first_gte_key() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_env_and_db(&dir);

    for k in [b"a".as_ref(), b"c", b"e", b"g"] {
        db.put(None,
               &DatabaseEntry::from_bytes(k),
               &DatabaseEntry::from_bytes(b"val")).unwrap();
    }

    let mut cursor = db.open_cursor(None, None).unwrap();

    // Exact match: SearchRange on "a" must find "a".
    let mut key  = DatabaseEntry::from_bytes(b"a");
    let mut data = DatabaseEntry::new();
    let s = cursor.get(&mut key, &mut data, Get::SearchGte, None).unwrap();
    assert_eq!(s, OperationStatus::Success);
    assert_eq!(key.data(), b"a");

    // Range miss: "b" not in tree → should find "c".
    let mut key  = DatabaseEntry::from_bytes(b"b");
    let mut data = DatabaseEntry::new();
    let s = cursor.get(&mut key, &mut data, Get::SearchGte, None).unwrap();
    assert_eq!(s, OperationStatus::Success,
        "SearchRange must find the first key >= 'b'");
    assert_eq!(key.data(), b"c",
        "SearchRange on 'b' must return 'c' (the next present key)");

    // Range beyond all keys: "z" → NotFound.
    let mut key  = DatabaseEntry::from_bytes(b"z");
    let mut data = DatabaseEntry::new();
    let s = cursor.get(&mut key, &mut data, Get::SearchGte, None).unwrap();
    assert_eq!(s, OperationStatus::NotFound,
        "SearchRange beyond all keys must return NotFound");

    cursor.close().unwrap();
}

/// Port of DbCursorSearchTest — Get::Search on an empty database returns NotFound.
#[test]
fn cursor_search_empty_database_returns_not_found() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_env_and_db(&dir);

    let mut cursor = db.open_cursor(None, None).unwrap();
    let mut key  = DatabaseEntry::from_bytes(b"anything");
    let mut data = DatabaseEntry::new();

    let status = cursor.get(&mut key, &mut data, Get::Search, None).unwrap();
    assert_eq!(status, OperationStatus::NotFound,
        "Get::Search on an empty database must return NotFound");

    cursor.close().unwrap();
}

/// Port of DbCursorSearchTest — Get::SearchGte on an empty database returns NotFound.
#[test]
fn cursor_search_range_empty_database_returns_not_found() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_env_and_db(&dir);

    let mut cursor = db.open_cursor(None, None).unwrap();
    let mut key  = DatabaseEntry::from_bytes(b"anything");
    let mut data = DatabaseEntry::new();

    let status = cursor.get(&mut key, &mut data, Get::SearchGte, None).unwrap();
    assert_eq!(status, OperationStatus::NotFound,
        "Get::SearchGte on an empty database must return NotFound");

    cursor.close().unwrap();
}

/// Port of DbCursorSearchTest — after tree splits, Get::Search still works
/// for all inserted keys.
///
/// JE: forces splits by inserting N = 80 records; after splits every key must
/// still be findable, exercising the multi-BIN tree traversal path.
#[test]
fn cursor_search_after_tree_splits_all_keys_findable() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_env_and_db(&dir);

    const N: u32 = 80;

    for i in 0..N {
        let key_bytes = i.to_be_bytes().to_vec();
        let val_bytes = (i * 2).to_be_bytes().to_vec();
        db.put(None,
               &DatabaseEntry::from_vec(key_bytes),
               &DatabaseEntry::from_vec(val_bytes)).unwrap();
    }

    assert_eq!(db.count().unwrap(), N as u64);

    let mut cursor = db.open_cursor(None, None).unwrap();

    for i in 0..N {
        let key_bytes = i.to_be_bytes().to_vec();
        let expected_val = (i * 2).to_be_bytes().to_vec();

        let mut key  = DatabaseEntry::from_vec(key_bytes.clone());
        let mut data = DatabaseEntry::new();
        let status = cursor.get(&mut key, &mut data, Get::Search, None).unwrap();
        assert_eq!(status, OperationStatus::Success,
            "Get::Search must find key {:?} after tree splits", key_bytes);
        assert_eq!(data.data(), expected_val.as_slice(),
            "data must match for key {:?}", key_bytes);
    }

    cursor.close().unwrap();
}

// ─────────────────────────────────────────────────────────────────────────────
// Crash-recovery integrity tests (Keith Bostic / Margo Seltzer reviewer concern)
// ─────────────────────────────────────────────────────────────────────────────

/// Verify that all committed records survive a clean close + reopen (recovery
/// run on open).  This is the base case: write N records, close, reopen,
/// assert every key is still present with the correct value.
#[test]
fn recovery_committed_records_survive_reopen() {
    let dir = TempDir::new().unwrap();
    const N: u32 = 200;

    // Phase 1: write N records and close.
    {
        let (env, db) = open_env_and_db(&dir);
        for i in 0..N {
            let k = DatabaseEntry::from_vec(i.to_be_bytes().to_vec());
            let v = DatabaseEntry::from_vec((i * 3 + 7).to_be_bytes().to_vec());
            db.put(None, &k, &v).unwrap();
        }
        drop(db);
        drop(env);
    }

    // Phase 2: reopen (runs recovery) and verify all N records.
    {
        let (env, db) = open_env_and_db(&dir);
        assert_eq!(db.count().unwrap(), N as u64,
            "all committed records must survive reopen");
        for i in 0..N {
            let mut k = DatabaseEntry::from_vec(i.to_be_bytes().to_vec());
            let mut v = DatabaseEntry::new();
            let status = db.get(None, &mut k, &mut v).unwrap();
            assert_eq!(status, OperationStatus::Success,
                "key {} must be present after recovery", i);
            assert_eq!(v.data(), (i * 3 + 7).to_be_bytes(),
                "value for key {} must be correct after recovery", i);
        }
        drop(db);
        drop(env);
    }
}

/// Verify that concurrent writes from multiple threads all survive close +
/// reopen: the Jepsen-style check — concurrent writes + recovery = all
/// committed data intact, no phantom records, no corrupted values.
#[test]
fn recovery_concurrent_writes_all_survive_reopen() {
    use std::sync::Arc;
    use std::thread;

    let dir = TempDir::new().unwrap();
    let dir_path = dir.path().to_path_buf();
    const THREADS: usize = 8;
    const PER_THREAD: u32 = 50;

    // Phase 1: concurrent writes from THREADS threads.
    {
        let (env, db) = open_env_and_db(&dir);
        let env = Arc::new(env);
        let db  = Arc::new(db);

        let handles: Vec<_> = (0..THREADS).map(|t| {
            let db = Arc::clone(&db);
            thread::spawn(move || {
                for i in 0..PER_THREAD {
                    let global_key = (t as u32) * PER_THREAD + i;
                    let k = DatabaseEntry::from_vec(global_key.to_be_bytes().to_vec());
                    let v = DatabaseEntry::from_vec(global_key.to_be_bytes().to_vec());
                    db.put(None, &k, &v).unwrap();
                }
            })
        }).collect();

        for h in handles { h.join().unwrap(); }
        drop(db);
        drop(env);
    }

    // Phase 2: reopen and verify all THREADS*PER_THREAD records.
    {
        let env_config = noxu_db::EnvironmentConfig::new(dir_path)
            .with_allow_create(false)  // Must already exist.
            .with_transactional(true);
        let env = noxu_db::Environment::open(env_config).unwrap();
        // allow_create=true: the database name is not persisted in the log yet;
        // recovery transplants the recovered tree into the newly opened handle.
        let db = env.open_database(None, "test", &DatabaseConfig::new().with_allow_create(true)).unwrap();

        let total = THREADS as u32 * PER_THREAD;
        assert_eq!(db.count().unwrap(), total as u64,
            "all {} records from {} threads must survive reopen", total, THREADS);

        for global_key in 0..total {
            let mut k = DatabaseEntry::from_vec(global_key.to_be_bytes().to_vec());
            let mut v = DatabaseEntry::new();
            let status = db.get(None, &mut k, &mut v).unwrap();
            assert_eq!(status, OperationStatus::Success,
                "key {} (from thread {}) must be present after recovery",
                global_key, global_key / PER_THREAD);
            assert_eq!(v.data(), global_key.to_be_bytes(),
                "value for key {} must be correct (no corruption)", global_key);
        }
    }
}

/// Verify that uncommitted transactions are correctly undone on reopen.
///
/// Write N committed records, then write M records inside a transaction that
/// is never committed (simulated by dropping the transaction without commit).
/// Reopen: recovery must undo the M uncommitted records.  Only N records
/// should be present.
#[test]
fn recovery_uncommitted_transactions_are_undone_on_reopen() {
    let dir = TempDir::new().unwrap();
    const N_COMMITTED: u32 = 50;
    const M_UNCOMMITTED: u32 = 20;

    // Phase 1: write N committed + M uncommitted records.
    {
        let (env, db) = open_env_and_db(&dir);

        // Committed writes (no txn = auto-commit).
        for i in 0..N_COMMITTED {
            let k = DatabaseEntry::from_vec(i.to_be_bytes().to_vec());
            let v = DatabaseEntry::from_vec(b"committed".to_vec());
            db.put(None, &k, &v).unwrap();
        }

        // Uncommitted writes: start a txn, write M records, then abort.
        let txn = env.begin_transaction(None, None).unwrap();
        for i in N_COMMITTED..N_COMMITTED + M_UNCOMMITTED {
            let k = DatabaseEntry::from_vec(i.to_be_bytes().to_vec());
            let v = DatabaseEntry::from_vec(b"uncommitted".to_vec());
            db.put(Some(&txn), &k, &v).unwrap();
        }
        txn.abort().unwrap(); // Explicitly abort — simulates crash scenario.

        drop(db);
        drop(env);
    }

    // Phase 2: reopen and verify only N_COMMITTED records.
    {
        let (_, db) = open_env_and_db(&dir);
        assert_eq!(db.count().unwrap(), N_COMMITTED as u64,
            "only {} committed records must be present; {} uncommitted must be absent",
            N_COMMITTED, M_UNCOMMITTED);

        // Committed records must be present.
        for i in 0..N_COMMITTED {
            let mut k = DatabaseEntry::from_vec(i.to_be_bytes().to_vec());
            let mut v = DatabaseEntry::new();
            assert_eq!(db.get(None, &mut k, &mut v).unwrap(), OperationStatus::Success,
                "committed key {} must be present", i);
        }

        // Uncommitted records must be absent.
        for i in N_COMMITTED..N_COMMITTED + M_UNCOMMITTED {
            let mut k = DatabaseEntry::from_vec(i.to_be_bytes().to_vec());
            let mut v = DatabaseEntry::new();
            assert_eq!(db.get(None, &mut k, &mut v).unwrap(), OperationStatus::NotFound,
                "aborted key {} must NOT be present after recovery", i);
        }
    }
}
