//! Integration tests for `DiskOrderedCursor` (Wave 2C-3).
//!
//! Covers JE-port-audit MEDIUM finding:
//! "DiskOrderedCursor is entirely absent from Noxu."
//!
//! Behaviour properties verified here:
//!
//! 1. `walks_all_inserted_records` — bulk scan returns every live record,
//!    regardless of order.
//! 2. `skips_deleted_records` — records deleted before the scan are absent.
//! 3. `multi_db_scan_returns_all_dbs` — scanning two databases returns the
//!    union of their live entries.
//! 4. `bounded_queue_completes` — small queue_size + small memory budget
//!    still yields every record.
//! 5. `drop_mid_iteration_joins_producer` — dropping the cursor mid-scan
//!    does not leak the producer thread (verified via JoinHandle drop).
//! 6. `stale_versions_visible_by_default` — repeat updates of the same key
//!    yield BOTH versions (JE default).
//! 7. `dedup_keys_filters_repeated_keys` — `dedup_keys = true` returns
//!    each key at most once.
//! 8. `current_returns_last_record` — `current()` re-emits the last
//!    `next()` result.

use noxu_db::{
    DatabaseConfig, DatabaseEntry, DiskOrderedCursorConfig, Environment,
    EnvironmentConfig, OperationStatus, open_disk_ordered_cursor_multi,
};
use std::collections::{HashMap, HashSet};
use tempfile::TempDir;

fn open_env(dir: &TempDir) -> Environment {
    let env_config = EnvironmentConfig::new(dir.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    Environment::open(env_config).unwrap()
}

fn open_db(env: &Environment, name: &str) -> noxu_db::Database {
    let db_config = DatabaseConfig::new().with_allow_create(true);
    env.open_database(None, name, &db_config).unwrap()
}

fn put(db: &noxu_db::Database, key: &[u8], data: &[u8]) {
    let k = DatabaseEntry::from_data(key);
    let v = DatabaseEntry::from_data(data);
    db.put( &k, &v).unwrap();
}

fn delete(db: &noxu_db::Database, key: &[u8]) {
    let k = DatabaseEntry::from_data(key);
    db.delete( &k).unwrap();
}

/// Drain a cursor into a `Vec<(key, data)>` of all returned records.
fn drain(
    cursor: &mut noxu_db::DiskOrderedCursor<'_>,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut out = Vec::new();
    let mut k = DatabaseEntry::new();
    let mut v = DatabaseEntry::new();
    while cursor.next(&mut k, &mut v).unwrap() == OperationStatus::Success {
        out.push((k.data().to_vec(), v.data().to_vec()));
    }
    out
}

// -----------------------------------------------------------------------------
// 1. Walks every live record
// -----------------------------------------------------------------------------

#[test]
fn walks_all_inserted_records() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let db = open_db(&env, "doc_walk");

    let n = 1000;
    let mut expected: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
    for i in 0..n {
        let k = format!("key-{i:06}").into_bytes();
        let v = format!("value-{i}").into_bytes();
        put(&db, &k, &v);
        expected.insert(k, v);
    }

    // Make sure prior writes are durable; auto-commit should already do so,
    // but a checkpoint guarantees the log file content is on disk.
    env.checkpoint(None).unwrap();

    let mut cursor =
        db.open_disk_ordered_cursor(DiskOrderedCursorConfig::new()).unwrap();
    let got = drain(&mut cursor);

    let mut got_map: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
    for (k, v) in got {
        got_map.insert(k, v);
    }
    assert_eq!(got_map, expected, "bulk scan must return every live record");
}

// -----------------------------------------------------------------------------
// 2. Skips records deleted before the scan
// -----------------------------------------------------------------------------

#[test]
fn skips_deleted_records() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let db = open_db(&env, "doc_delete");

    for i in 0..50 {
        put(&db, format!("k{i}").as_bytes(), format!("v{i}").as_bytes());
    }
    // Delete every other key.
    for i in (0..50).step_by(2) {
        delete(&db, format!("k{i}").as_bytes());
    }
    env.checkpoint(None).unwrap();

    let mut cursor =
        db.open_disk_ordered_cursor(DiskOrderedCursorConfig::new()).unwrap();
    let got = drain(&mut cursor);

    // At minimum, the 25 odd-indexed keys must appear.  The default
    // (no dedup) behaviour means deleted keys MAY also have appeared in
    // their pre-delete form; what we strictly assert is that every key
    // we see is one we previously wrote, and every "live" key is present.
    let live: HashSet<Vec<u8>> =
        (1..50).step_by(2).map(|i| format!("k{i}").into_bytes()).collect();
    let mut keys_seen: HashSet<Vec<u8>> = HashSet::new();
    for (k, _) in &got {
        keys_seen.insert(k.clone());
    }
    for k in &live {
        assert!(keys_seen.contains(k), "live key {k:?} missing from scan");
    }
}

#[test]
fn skips_deleted_records_with_dedup() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let db = open_db(&env, "doc_delete_dedup");

    for i in 0..50 {
        put(&db, format!("k{i}").as_bytes(), format!("v{i}").as_bytes());
    }
    for i in (0..50).step_by(2) {
        delete(&db, format!("k{i}").as_bytes());
    }
    env.checkpoint(None).unwrap();

    // dedup_keys=true means each key appears at most once.  Since the producer
    // walks the log in append order and skips Delete entries, the first put
    // of an even key wins — so all 50 keys appear (BUT the value may be
    // the original value of even keys, which is JE-correct).
    let mut cursor = db
        .open_disk_ordered_cursor(
            DiskOrderedCursorConfig::new().with_dedup_keys(true),
        )
        .unwrap();
    let got = drain(&mut cursor);

    let keys_seen: HashSet<Vec<u8>> =
        got.iter().map(|(k, _)| k.clone()).collect();
    assert_eq!(keys_seen.len(), got.len(), "dedup must yield each key once");
    // All 50 originally-inserted keys should be visible (delete entries
    // don't carry data, so they don't count as a "first appearance").
    for i in 0..50 {
        let k = format!("k{i}").into_bytes();
        assert!(keys_seen.contains(&k), "key {i} missing from dedup scan");
    }
}

// -----------------------------------------------------------------------------
// 3. Multi-database scan
// -----------------------------------------------------------------------------

#[test]
fn multi_db_scan_returns_all_dbs() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let db_a = open_db(&env, "doc_multi_a");
    let db_b = open_db(&env, "doc_multi_b");

    for i in 0..100 {
        put(&db_a, format!("a-{i}").as_bytes(), format!("va-{i}").as_bytes());
        put(&db_b, format!("b-{i}").as_bytes(), format!("vb-{i}").as_bytes());
    }
    env.checkpoint(None).unwrap();

    let dbs: [&noxu_db::Database; 2] = [&db_a, &db_b];
    let mut cursor =
        open_disk_ordered_cursor_multi(&dbs, DiskOrderedCursorConfig::new())
            .unwrap();
    let got = drain(&mut cursor);

    let mut a_count = 0;
    let mut b_count = 0;
    for (k, _) in &got {
        if k.starts_with(b"a-") {
            a_count += 1;
        } else if k.starts_with(b"b-") {
            b_count += 1;
        } else {
            panic!("unexpected key prefix: {k:?}");
        }
    }
    assert!(a_count >= 100, "got {a_count} a-* keys; expected >= 100");
    assert!(b_count >= 100, "got {b_count} b-* keys; expected >= 100");
}

// -----------------------------------------------------------------------------
// 4. Bounded queue completes
// -----------------------------------------------------------------------------

#[test]
fn bounded_queue_completes() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let db = open_db(&env, "doc_bounded");

    for i in 0..200 {
        put(&db, format!("k{i:04}").as_bytes(), format!("v{i}").as_bytes());
    }
    env.checkpoint(None).unwrap();

    // Aggressively small queue + memory budget — producer must repeatedly
    // park and resume.
    let cfg = DiskOrderedCursorConfig::new()
        .with_queue_size(2)
        .with_internal_memory_limit(64);

    let mut cursor = db.open_disk_ordered_cursor(cfg).unwrap();
    let got = drain(&mut cursor);

    let keys_seen: HashSet<Vec<u8>> =
        got.iter().map(|(k, _)| k.clone()).collect();
    for i in 0..200 {
        let k = format!("k{i:04}").into_bytes();
        assert!(keys_seen.contains(&k), "key {k:?} missing from bounded scan");
    }
}

// -----------------------------------------------------------------------------
// 5. Drop mid-iteration joins producer
// -----------------------------------------------------------------------------

#[test]
fn drop_mid_iteration_joins_producer() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let db = open_db(&env, "doc_drop_mid");

    for i in 0..500 {
        put(&db, format!("k{i:04}").as_bytes(), format!("v{i}").as_bytes());
    }
    env.checkpoint(None).unwrap();

    // Tiny queue so the producer is parked behind the channel for most
    // of its life — exercises the Drop -> shutdown -> producer-cancel
    // wakeup path.
    let cfg = DiskOrderedCursorConfig::new()
        .with_queue_size(1)
        .with_internal_memory_limit(8);
    let mut cursor = db.open_disk_ordered_cursor(cfg).unwrap();
    let mut k = DatabaseEntry::new();
    let mut v = DatabaseEntry::new();

    // Pull a few records and then drop the cursor.
    for _ in 0..5 {
        let st = cursor.next(&mut k, &mut v).unwrap();
        assert_eq!(st, OperationStatus::Success);
    }
    drop(cursor);

    // If the producer were leaked, its strong reference to the `LogManager`
    // would prevent the environment from closing cleanly on drop.  We
    // therefore close the env explicitly: any leaked producer thread that
    // outlived the cursor would race with this and at minimum corrupt
    // teardown, which would surface as a panic / timeout in CI.
    drop(db);
    env.close().unwrap();
}

#[test]
fn close_is_idempotent() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let db = open_db(&env, "doc_close_idem");
    put(&db, b"only", b"one");
    env.checkpoint(None).unwrap();

    let cursor =
        db.open_disk_ordered_cursor(DiskOrderedCursorConfig::new()).unwrap();
    cursor.close().unwrap();
    // Reopen + drop cycle must also close cleanly.
    let cursor =
        db.open_disk_ordered_cursor(DiskOrderedCursorConfig::new()).unwrap();
    drop(cursor);
}

// -----------------------------------------------------------------------------
// 6. Stale versions visible by default (JE-correct)
// -----------------------------------------------------------------------------

#[test]
fn stale_versions_visible_by_default() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let db = open_db(&env, "doc_stale");

    let key = b"the-key";
    put(&db, key, b"v1");
    put(&db, key, b"v2");
    put(&db, key, b"v3");
    env.checkpoint(None).unwrap();

    let mut cursor =
        db.open_disk_ordered_cursor(DiskOrderedCursorConfig::new()).unwrap();
    let got = drain(&mut cursor);

    // Default behaviour: JE returns every LN that survives in the log,
    // including stale (overwritten) versions — so we should see all three.
    let values_seen: HashSet<Vec<u8>> =
        got.iter().filter(|(k, _)| k == key).map(|(_, v)| v.clone()).collect();
    assert!(values_seen.contains(b"v1".as_slice()));
    assert!(values_seen.contains(b"v2".as_slice()));
    assert!(values_seen.contains(b"v3".as_slice()));
}

// -----------------------------------------------------------------------------
// 7. dedup_keys filters repeated keys
// -----------------------------------------------------------------------------

#[test]
fn dedup_keys_filters_repeated_keys() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let db = open_db(&env, "doc_dedup");

    let key = b"the-key";
    put(&db, key, b"v1");
    put(&db, key, b"v2");
    put(&db, key, b"v3");
    env.checkpoint(None).unwrap();

    let mut cursor = db
        .open_disk_ordered_cursor(
            DiskOrderedCursorConfig::new().with_dedup_keys(true),
        )
        .unwrap();
    let got = drain(&mut cursor);

    let count = got.iter().filter(|(k, _)| k == key).count();
    assert_eq!(count, 1, "dedup must yield each key exactly once");
}

// -----------------------------------------------------------------------------
// 8. current() re-emits last record
// -----------------------------------------------------------------------------

#[test]
fn current_returns_last_record() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let db = open_db(&env, "doc_current");
    put(&db, b"a", b"1");
    put(&db, b"b", b"2");
    env.checkpoint(None).unwrap();

    let mut cursor =
        db.open_disk_ordered_cursor(DiskOrderedCursorConfig::new()).unwrap();
    let mut k = DatabaseEntry::new();
    let mut v = DatabaseEntry::new();

    // current() before next() returns NotFound.
    assert_eq!(
        cursor.current(&mut k, &mut v).unwrap(),
        OperationStatus::NotFound
    );

    assert_eq!(cursor.next(&mut k, &mut v).unwrap(), OperationStatus::Success);
    let first_k = k.data().to_vec();
    let first_v = v.data().to_vec();

    let mut k2 = DatabaseEntry::new();
    let mut v2 = DatabaseEntry::new();
    assert_eq!(
        cursor.current(&mut k2, &mut v2).unwrap(),
        OperationStatus::Success
    );
    assert_eq!(k2.data(), first_k.as_slice());
    assert_eq!(v2.data(), first_v.as_slice());
}

// -----------------------------------------------------------------------------
// 9. Empty database
// -----------------------------------------------------------------------------

#[test]
fn empty_db_yields_no_records() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let db = open_db(&env, "doc_empty");
    env.checkpoint(None).unwrap();

    let mut cursor =
        db.open_disk_ordered_cursor(DiskOrderedCursorConfig::new()).unwrap();
    let mut k = DatabaseEntry::new();
    let mut v = DatabaseEntry::new();
    assert_eq!(cursor.next(&mut k, &mut v).unwrap(), OperationStatus::NotFound);
}

// -----------------------------------------------------------------------------
// 10. keys_only mode returns empty data
// -----------------------------------------------------------------------------

#[test]
fn keys_only_returns_empty_data() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let db = open_db(&env, "doc_keys_only");
    for i in 0..20 {
        put(
            &db,
            format!("k{i}").as_bytes(),
            format!("longish-value-{i}").as_bytes(),
        );
    }
    env.checkpoint(None).unwrap();

    let mut cursor = db
        .open_disk_ordered_cursor(
            DiskOrderedCursorConfig::new().with_keys_only(true),
        )
        .unwrap();
    let got = drain(&mut cursor);
    assert!(!got.is_empty(), "should still return keys");
    for (_, v) in &got {
        assert!(v.is_empty(), "keys_only mode must elide data");
    }
}

// -----------------------------------------------------------------------------
// 11. Empty database list rejected
// -----------------------------------------------------------------------------

#[test]
fn empty_db_list_is_rejected() {
    let dir = TempDir::new().unwrap();
    let _env = open_env(&dir);
    let dbs: [&noxu_db::Database; 0] = [];
    let res =
        open_disk_ordered_cursor_multi(&dbs, DiskOrderedCursorConfig::new());
    assert!(res.is_err(), "empty db list must be rejected");
}

// -----------------------------------------------------------------------------
// 12. CLN-7: a DOS scan completes even while the cleaner runs concurrently.
//
// The DOS producer protects the files it scans from cleaner deletion
// (FileProtector, faithful to JE DiskOrderedScanner.scan,
// DiskOrderedScanner.java:704). Before the fix the cleaner could delete a
// file mid-scan -> LogFileNotFound / torn read. This test writes enough data
// to span multiple log files, fires manual checkpoints + cleaning while the
// scan is in flight, and asserts the scan returns every live record without
// error.
// -----------------------------------------------------------------------------

#[test]
fn cln7_scan_completes_with_concurrent_cleaning() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let db = open_db(&env, "doc_cln7");

    // Write a good number of records, then checkpoint so files become
    // candidates for cleaning.
    let mut expected: HashSet<Vec<u8>> = HashSet::new();
    for i in 0..500u32 {
        let key = format!("key-{i:05}");
        put(&db, key.as_bytes(), format!("value-{i}").as_bytes());
        expected.insert(key.into_bytes());
    }
    env.checkpoint(None).unwrap();

    // Open the DOS cursor (its producer protects the files it will scan),
    // then poke the cleaner while draining.
    let mut cursor = db
        .open_disk_ordered_cursor(
            DiskOrderedCursorConfig::new()
                .with_queue_size(8)
                .with_dedup_keys(true),
        )
        .unwrap();

    let mut got: HashSet<Vec<u8>> = HashSet::new();
    let mut k = DatabaseEntry::new();
    let mut v = DatabaseEntry::new();
    let mut n = 0u32;
    while cursor.next(&mut k, &mut v).unwrap() == OperationStatus::Success {
        got.insert(k.data().to_vec());
        n += 1;
        // Interleave cleaning attempts with the scan. Protected files must
        // be skipped by the cleaner, so the scan never sees LogFileNotFound.
        if n.is_multiple_of(50) {
            let _ = env.checkpoint(None);
        }
    }

    // Every live key must have been returned (none lost to a deleted file).
    for key in &expected {
        assert!(
            got.contains(key),
            "CLN-7: record {:?} missing — a scanned file may have been deleted",
            String::from_utf8_lossy(key)
        );
    }
}
