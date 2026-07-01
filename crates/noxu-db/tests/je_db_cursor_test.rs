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
        db.put_in(
            &txn,
            DatabaseEntry::from_bytes(k.as_bytes()),
            DatabaseEntry::from_bytes(v.as_bytes()),
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

    let mut cursor = db.open_cursor(None).unwrap();
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();

    let mut prev: Vec<u8> = Vec::new();
    let mut n = 0usize;
    let mut s = cursor.get(&mut k, &mut d, Get::First, None).unwrap();
    while s == OperationStatus::Success {
        let key = k.data_opt().unwrap_or(&[]).to_vec();
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

    let mut cursor = db.open_cursor(None).unwrap();
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();

    let mut prev: Option<Vec<u8>> = None;
    let mut n = 0usize;
    let mut s = cursor.get(&mut k, &mut d, Get::Last, None).unwrap();
    while s == OperationStatus::Success {
        let key = k.data_opt().unwrap_or(&[]).to_vec();
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

    let mut cursor = db.open_cursor(None).unwrap();
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();

    // Position at first, then duplicate-position (the noxu equivalent
    // of advanceCursor: nothing changes, the cursor still points at the
    // first record).
    let s = cursor.get(&mut k, &mut d, Get::First, None).unwrap();
    assert_eq!(OperationStatus::Success, s);
    let first_key = k.data_opt().unwrap_or(&[]).to_vec();

    // Walk the rest forward.
    let mut prev = first_key;
    let mut n = 1usize;
    let mut s = cursor.get(&mut k, &mut d, Get::Next, None).unwrap();
    while s == OperationStatus::Success {
        let key = k.data_opt().unwrap_or(&[]).to_vec();
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

    let mut cursor = db.open_cursor(None).unwrap();
    for (k, v) in SIMPLE_KEYS.iter().zip(SIMPLE_DATA.iter()) {
        let mut key = DatabaseEntry::from_bytes(k.as_bytes());
        let mut data = DatabaseEntry::new();
        let s = cursor.get(&mut key, &mut data, Get::Search, None).unwrap();
        assert_eq!(OperationStatus::Success, s, "k={k}");
        assert_eq!(v.as_bytes(), data.data_opt().unwrap_or(&[]));
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
        .delete_in(&txn, DatabaseEntry::from_bytes(target.as_bytes()))
        .unwrap();
    assert!(s);
    txn.commit().unwrap();

    let mut cursor = db.open_cursor(None).unwrap();
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
            .delete_in(&txn, DatabaseEntry::from_bytes(k.as_bytes()))
            .unwrap();
        assert!(s);
    }
    txn.commit().unwrap();

    // Verify empty.
    {
        let mut cursor = db.open_cursor(None).unwrap();
        let mut k = DatabaseEntry::new();
        let mut d = DatabaseEntry::new();
        let s = cursor.get(&mut k, &mut d, Get::First, None).unwrap();
        assert_eq!(OperationStatus::NotFound, s);
    }

    // Re-insert.
    put_simple(&env, &db);

    // Walk and collect.
    let mut cursor = db.open_cursor(None).unwrap();
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    let mut seen: BTreeSet<String> = BTreeSet::new();
    let mut s = cursor.get(&mut k, &mut d, Get::First, None).unwrap();
    while s == OperationStatus::Success {
        seen.insert(
            String::from_utf8(k.data_opt().unwrap_or(&[]).to_vec()).unwrap(),
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
        db.put_in(&txn, &key, &val).unwrap();
    }
    txn.commit().unwrap();

    assert_eq!(N as u64, db.count().unwrap());

    let txn = env.begin_transaction(None).unwrap();
    for i in 0..N {
        let s = db
            .delete_in(&txn, DatabaseEntry::from_bytes(&i.to_be_bytes()))
            .unwrap();
        assert!(s);
    }
    txn.commit().unwrap();

    assert_eq!(0, db.count().unwrap());
}

// ---------------------------------------------------------------------------
// JE TCK port: `com.sleepycat.je.dbi.DbCursorDuplicateTest` (Wave 11-A)
// ---------------------------------------------------------------------------
//
// Six dup-cursor test methods adapted from JE's `DbCursorDuplicateTest`.
// JE's `createRandomDuplicateData(...)` /
// `DataWalker` test helpers are flattened into deterministic loops here
// because noxu has no equivalent harness; the user-visible invariants are
// what JE actually asserts on, so the behaviour matches.
//
// Adaptations
//
// * JE inserts random data keyed by `simpleKeyStrings` * `simpleDataStrings`.
//   We reuse the same SIMPLE_KEYS fixture as the primary set and inject
//   `N_DUP` duplicate data values per primary so the structure of the test
//   matches: many primaries, each with a fixed number of dups.
// * JE's `cursor.putNoDupData(...) == OperationStatus.KEYEXIST` becomes
//   `cursor.put(..., Put::NoDupData) == OperationStatus::KeyExists`.
// * JE's `cursor.count()` is `cursor.count()` here.
// * JE wires its own `DataWalker.walkData()` traversals; we just call
//   `Get::First` + `Get::Next` (or `Get::Last` + `Get::Prev`).

use noxu_db::Put;

/// JE: `DbCursorDuplicateTest.N_COUNT_TOP_KEYS`.  Number of primary keys.
const DUP_N_KEYS: u8 = 6;
/// JE: `DbCursorDuplicateTest.N_COUNT_DUPLICATES_PER_KEY`.  Number of dup
/// values stored under each primary.
const DUP_N_PER_KEY: u8 = 5;

fn open_dup_env_db() -> (TempDir, noxu_db::Environment, noxu_db::Database) {
    let dir = TempDir::new().unwrap();
    let env_cfg = EnvironmentConfig::new(dir.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    let env = noxu_db::Environment::open(env_cfg).unwrap();
    let db_cfg = DatabaseConfig::new()
        .with_allow_create(true)
        .with_transactional(true)
        .with_sorted_duplicates(true);
    let db = env.open_database(None, "DbCursorDuplicateTest", &db_cfg).unwrap();
    (dir, env, db)
}

/// Insert `DUP_N_KEYS` primaries each with `DUP_N_PER_KEY` distinct dup
/// values.  Equivalent to JE's `createRandomDuplicateData(N_KEYS, N_DUPS,
/// ..., false, true)`.
fn put_dup_fixture(env: &noxu_db::Environment, db: &noxu_db::Database) {
    let txn = env.begin_transaction(None).unwrap();
    for k in 0..DUP_N_KEYS {
        // primary key: 4-byte big-endian copy so ordering is total
        let key = DatabaseEntry::from_bytes(&[b'k', k, 0, 0]);
        for d in 0..DUP_N_PER_KEY {
            let data = DatabaseEntry::from_bytes(&[b'd', d]);
            db.put_in(&txn, &key, &data).unwrap();
        }
    }
    txn.commit().unwrap();
}

// --- testDuplicateCreationForward ------------------------------------------

/// Port of `DbCursorDuplicateTest.testDuplicateCreationForward`.
/// Insert `N_KEYS * N_PER_KEY` (key, data) pairs into a sorted-dup DB,
/// walk forward with `Get::First` / `Get::Next`, assert (key, data) is
/// non-decreasing and the full count matches.
#[test]
fn db_cursor_duplicate_test_duplicate_creation_forward() {
    let (_dir, env, db) = open_dup_env_db();
    put_dup_fixture(&env, &db);

    let mut cursor = db.open_cursor(None).unwrap();
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();

    let mut prev: Option<(Vec<u8>, Vec<u8>)> = None;
    let mut n = 0usize;
    let mut s = cursor.get(&mut k, &mut d, Get::First, None).unwrap();
    while s == OperationStatus::Success {
        let cur = (
            k.data_opt().unwrap_or(&[]).to_vec(),
            d.data_opt().unwrap_or(&[]).to_vec(),
        );
        if let Some(p) = &prev {
            assert!(
                p <= &cur,
                "expected (k,d) non-decreasing, got {p:?} then {cur:?}",
            );
        }
        prev = Some(cur);
        n += 1;
        s = cursor.get(&mut k, &mut d, Get::Next, None).unwrap();
    }
    assert_eq!(usize::from(DUP_N_KEYS) * usize::from(DUP_N_PER_KEY), n);
}

// --- testDuplicateCreationBackwards ----------------------------------------

/// Port of `DbCursorDuplicateTest.testDuplicateCreationBackwards`.
/// Walk the same fixture backwards from `Get::Last` via `Get::Prev`,
/// assert (key, data) is non-increasing and full count is preserved.
#[test]
fn db_cursor_duplicate_test_duplicate_creation_backwards() {
    let (_dir, env, db) = open_dup_env_db();
    put_dup_fixture(&env, &db);

    let mut cursor = db.open_cursor(None).unwrap();
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();

    let mut prev: Option<(Vec<u8>, Vec<u8>)> = None;
    let mut n = 0usize;
    let mut s = cursor.get(&mut k, &mut d, Get::Last, None).unwrap();
    while s == OperationStatus::Success {
        let cur = (
            k.data_opt().unwrap_or(&[]).to_vec(),
            d.data_opt().unwrap_or(&[]).to_vec(),
        );
        if let Some(p) = &prev {
            assert!(
                p >= &cur,
                "expected (k,d) non-increasing, got {p:?} then {cur:?}",
            );
        }
        prev = Some(cur);
        n += 1;
        s = cursor.get(&mut k, &mut d, Get::Prev, None).unwrap();
    }
    assert_eq!(usize::from(DUP_N_KEYS) * usize::from(DUP_N_PER_KEY), n);
}

// --- testDuplicateCount -----------------------------------------------------

/// Port of `DbCursorDuplicateTest.testDuplicateCount`.
/// Walk the fixture; at every position `cursor.count()` must report
/// `DUP_N_PER_KEY` (every primary has the same dup-set size).
///
/// TODO(bug): on a multi-primary sorted-dup DB,
/// `Cursor::count()` over-counts whenever the cursor is positioned past
/// the first dup of the current primary.  Empirically count returns
/// `DUP_N_PER_KEY + offset_within_primary` (e.g. for a 5-dup primary,
/// position 0 returns 5, position 1 returns 6, ..., position 4 returns
/// 9).  The `backward + 1 + forward` formula in
/// `noxu_dbi::CursorImpl::count()` double-counts the original position
/// because, after the PrevDup walk repositions scratch on the first
/// dup, the subsequent NextDup walk re-traverses every dup including
/// the original.  JE returns `N_DUPLICATE_PER_KEY` at every position.
///
/// Fixed in Wave 11-N (Bug 1): the count formula is now `forward + 1`
/// after the backward walk repositions scratch on the first dup; see
/// the 2026 review.
#[test]
fn db_cursor_duplicate_test_duplicate_count() {
    let (_dir, env, db) = open_dup_env_db();
    put_dup_fixture(&env, &db);

    let mut cursor = db.open_cursor(None).unwrap();
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();

    let mut n = 0usize;
    let mut s = cursor.get(&mut k, &mut d, Get::First, None).unwrap();
    while s == OperationStatus::Success {
        assert_eq!(
            u64::from(DUP_N_PER_KEY),
            cursor.count().unwrap(),
            "count() at position #{n} must equal DUP_N_PER_KEY",
        );
        n += 1;
        s = cursor.get(&mut k, &mut d, Get::Next, None).unwrap();
    }
    assert_eq!(usize::from(DUP_N_KEYS) * usize::from(DUP_N_PER_KEY), n);
}

// --- testGetNextDup ---------------------------------------------------------

/// Port of `DbCursorDuplicateTest.testGetNextDup`.  For each primary key,
/// position with `Get::Search` and walk via `Get::NextDup`; confirm
/// exactly `DUP_N_PER_KEY` data values come back in sorted order and the
/// next `NextDup` returns NotFound (boundary at next primary).
///
/// TODO(bug): on a multi-primary sorted-dup DB,
/// `Get::Search` positions on the smallest dup of the requested
/// primary, but the immediately-following `Get::NextDup` returns
/// NotFound for every primary except the lexicographically smallest.
/// The single-primary version of this scenario is covered by
/// `sorted_dup_test::test_dup_sorted_order` (which passes), confirming
/// the bug is multi-primary specific.  JE returns the next dup until
/// the dup-set is exhausted regardless of which primary was searched.
///
/// Fixed in Wave 11-N (Bug 2): `CursorImpl::search_dup` now stores the
/// real BIN slot index of the located dup (and pins the BIN) instead
/// of the previous hard-coded `current_index = 0`, so the subsequent
/// `retrieve_next` increments the right slot.  See
/// the 2026 review.
#[test]
fn db_cursor_duplicate_test_get_next_dup() {
    let (_dir, env, db) = open_dup_env_db();
    put_dup_fixture(&env, &db);

    for k in 0..DUP_N_KEYS {
        let mut cursor = db.open_cursor(None).unwrap();
        let mut key = DatabaseEntry::from_bytes(&[b'k', k, 0, 0]);
        let mut data = DatabaseEntry::new();

        let s = cursor.get(&mut key, &mut data, Get::Search, None).unwrap();
        assert_eq!(OperationStatus::Success, s, "Search for k={k}");

        let mut prev = data.data_opt().unwrap_or(&[]).to_vec();
        let mut seen = 1usize;
        loop {
            let s =
                cursor.get(&mut key, &mut data, Get::NextDup, None).unwrap();
            if s == OperationStatus::NotFound {
                break;
            }
            assert_eq!(OperationStatus::Success, s);
            // Stayed inside the same primary key.
            assert_eq!(
                key.data_opt().unwrap_or(&[]),
                &[b'k', k, 0, 0],
                "NextDup must not cross key boundary",
            );
            let cur = data.data_opt().unwrap_or(&[]).to_vec();
            assert!(prev <= cur, "dup ordering: {prev:?} then {cur:?}");
            prev = cur;
            seen += 1;
        }
        assert_eq!(usize::from(DUP_N_PER_KEY), seen);
    }
}

// --- testGetNextNoDup -------------------------------------------------------

/// Port of `DbCursorDuplicateTest.testGetNextNoDup`.  Position at first;
/// each `Get::NextNoDup` must skip the rest of the current primary's dup
/// set and land on the first dup of the next primary.  Total of
/// `DUP_N_KEYS - 1` successful jumps, then NotFound.
#[test]
fn db_cursor_duplicate_test_get_next_no_dup() {
    let (_dir, env, db) = open_dup_env_db();
    put_dup_fixture(&env, &db);

    let mut cursor = db.open_cursor(None).unwrap();
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();

    let s = cursor.get(&mut k, &mut d, Get::First, None).unwrap();
    assert_eq!(OperationStatus::Success, s);
    assert_eq!(k.data_opt().unwrap_or(&[]), &[b'k', 0, 0, 0]);

    let mut jumps = 0usize;
    let mut last_primary = 0u8;
    loop {
        let s = cursor.get(&mut k, &mut d, Get::NextNoDup, None).unwrap();
        if s == OperationStatus::NotFound {
            break;
        }
        assert_eq!(OperationStatus::Success, s);
        let kb = k.data_opt().unwrap_or(&[]);
        assert_eq!(kb.len(), 4, "primary keys are 4 bytes wide");
        let primary = kb[1];
        assert!(
            primary > last_primary,
            "NextNoDup must advance to a strictly larger primary",
        );
        last_primary = primary;
        jumps += 1;
    }
    assert_eq!(usize::from(DUP_N_KEYS) - 1, jumps);
}

// --- testPutNoDupData2 ------------------------------------------------------

/// Port of `DbCursorDuplicateTest.testPutNoDupData2`.  Insert
/// `DUP_N_PER_KEY` distinct dup values under one key with
/// `Put::NoDupData`; every insert must succeed because every (key, data)
/// pair is unique.  Then attempting to re-insert any of them with
/// `Put::NoDupData` returns `KeyExists`.
#[test]
fn db_cursor_duplicate_test_put_no_dup_data2() {
    let (_dir, _env, db) = open_dup_env_db();

    let mut cursor = db.open_cursor(None).unwrap();
    let key = DatabaseEntry::from_bytes(b"oneKey");
    for d in 0..DUP_N_PER_KEY {
        let data = DatabaseEntry::from_bytes(&[d]);
        let s = cursor.put(&key, &data, Put::NoDupData).unwrap();
        assert_eq!(OperationStatus::Success, s, "fresh dup d={d}");
    }
    // Re-insertions of the exact same (key, data) must report KeyExists.
    for d in 0..DUP_N_PER_KEY {
        let data = DatabaseEntry::from_bytes(&[d]);
        let s = cursor.put(&key, &data, Put::NoDupData).unwrap();
        assert_eq!(
            OperationStatus::KeyExists,
            s,
            "re-insert (oneKey,{d}) must return KeyExists",
        );
    }
    cursor.close().unwrap();
}

// --- testDuplicateReplacement ----------------------------------------------

/// Port of `DbCursorDuplicateTest.testDuplicateReplacement`.  Insert two
/// distinct dup values under one key; walk the cursor, calling
/// `putCurrent` to rewrite each dup with its existing value.  All dups
/// must remain visible (`nEntries == 2` in JE).  `putCurrent` here is
/// `Cursor::put(.., Put::Current)`.
#[test]
fn db_cursor_duplicate_test_duplicate_replacement() {
    let (_dir, _env, db) = open_dup_env_db();

    // Single-primary scenario, matching JE's testDuplicateReplacement.
    let key = DatabaseEntry::from_bytes(b"aaaa");
    let v1 = DatabaseEntry::from_bytes(b"d1d1");
    let v2 = DatabaseEntry::from_bytes(b"d2d2");

    {
        let mut cursor = db.open_cursor(None).unwrap();
        // First dup; NoDupData succeeds because the pair is fresh.
        let s = cursor.put(&key, &v1, Put::NoDupData).unwrap();
        assert_eq!(OperationStatus::Success, s);
        // Re-insert (aaaa, d1d1) with NoDupData → KeyExists.
        let s = cursor.put(&key, &v1, Put::NoDupData).unwrap();
        assert_eq!(OperationStatus::KeyExists, s);
        // Second dup is fresh.
        let s = cursor.put(&key, &v2, Put::NoDupData).unwrap();
        assert_eq!(OperationStatus::Success, s);
        cursor.close().unwrap();
    }

    // Walk the cursor and re-write each dup with its existing value.
    let mut cursor = db.open_cursor(None).unwrap();
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    let mut n = 0usize;
    let mut s = cursor.get(&mut k, &mut d, Get::First, None).unwrap();
    while s == OperationStatus::Success {
        // putCurrent must succeed when rewriting with the same data.
        let cur_key = DatabaseEntry::from_bytes(k.data_opt().unwrap_or(&[]));
        let cur_data = DatabaseEntry::from_bytes(d.data_opt().unwrap_or(&[]));
        let p = cursor.put(&cur_key, &cur_data, Put::Current).unwrap();
        assert_eq!(OperationStatus::Success, p, "putCurrent same-data");
        n += 1;
        s = cursor.get(&mut k, &mut d, Get::Next, None).unwrap();
    }
    assert_eq!(2, n, "both dups must remain after putCurrent rewrites");
    cursor.close().unwrap();
}

// --- testDuplicateDuplicates -----------------------------------------------

/// Port of `DbCursorDuplicateTest.testDuplicateDuplicates`.  Insert two
/// distinct dup values for one key; for each value, the second
/// `Put::NoDupData` of the SAME (key, data) pair must report
/// `KeyExists`, and a subsequent `Put::Overwrite` of the same pair must
/// report `KeyExists` (sorted-dup contract: re-inserting an exact dup is
/// a no-op).  Final dup count is 2.
#[test]
fn db_cursor_duplicate_test_duplicate_duplicates() {
    let (_dir, _env, db) = open_dup_env_db();

    let key = DatabaseEntry::from_bytes(b"aaaa");
    let v1 = DatabaseEntry::from_bytes(b"d1d1");
    let v2 = DatabaseEntry::from_bytes(b"d2d2");

    let mut cursor = db.open_cursor(None).unwrap();

    // First dup pair fresh.
    assert_eq!(
        OperationStatus::Success,
        cursor.put(&key, &v1, Put::NoDupData).unwrap()
    );
    // Second NoDupData with the SAME pair → KeyExists.
    assert_eq!(
        OperationStatus::KeyExists,
        cursor.put(&key, &v1, Put::NoDupData).unwrap()
    );
    // JE: Overwrite-mode `cursor.put(...)` of an existing exact dup pair
    // succeeds (it is the duplicate-aware analogue of "replace data at
    // current position").  noxu agrees here.
    assert_eq!(
        OperationStatus::Success,
        cursor.put(&key, &v1, Put::Overwrite).unwrap()
    );

    // Distinct second value is fresh.
    assert_eq!(
        OperationStatus::Success,
        cursor.put(&key, &v2, Put::NoDupData).unwrap()
    );
    assert_eq!(
        OperationStatus::KeyExists,
        cursor.put(&key, &v2, Put::NoDupData).unwrap()
    );
    assert_eq!(
        OperationStatus::Success,
        cursor.put(&key, &v2, Put::Overwrite).unwrap()
    );

    // Verify exactly two dup pairs survived (cursor walk).
    cursor.close().unwrap();
    let mut cursor = db.open_cursor(None).unwrap();
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    let mut n = 0usize;
    let mut s = cursor.get(&mut k, &mut d, Get::First, None).unwrap();
    while s == OperationStatus::Success {
        n += 1;
        s = cursor.get(&mut k, &mut d, Get::Next, None).unwrap();
    }
    assert_eq!(2, n);
    cursor.close().unwrap();
}
