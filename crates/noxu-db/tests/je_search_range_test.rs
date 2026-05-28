//! JE GetSearchBothRangeTest ports — cursor range-search invariants.
//!
//! Each test below corresponds to a method in
//! `test/com/sleepycat/je/GetSearchBothRangeTest.java`.  These exercise the
//! corner cases of `Get::SearchRange` (a.k.a. `getSearchKeyRange`) and
//! `Get::SearchBothRange` on duplicates and singletons.

use noxu_db::{
    DatabaseConfig, DatabaseEntry, EnvironmentConfig, Get, OperationStatus,
};
use tempfile::TempDir;

fn open_env_db(
    dir: &TempDir,
    name: &str,
    dups: bool,
) -> (noxu_db::Environment, noxu_db::Database) {
    let env_cfg = EnvironmentConfig::new(dir.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    let env = noxu_db::Environment::open(env_cfg).unwrap();
    let db_cfg = DatabaseConfig::new()
        .with_allow_create(true)
        .with_transactional(true)
        .with_sorted_duplicates(dups);
    let db = env.open_database(None, name, &db_cfg).unwrap();
    (env, db)
}

fn ikey(i: u32) -> DatabaseEntry {
    DatabaseEntry::from_bytes(&i.to_be_bytes())
}

fn val_u32(e: &DatabaseEntry) -> u32 {
    let bytes = e.get_data().unwrap();
    let mut a = [0u8; 4];
    a.copy_from_slice(bytes);
    u32::from_be_bytes(a)
}

fn put(
    env: &noxu_db::Environment,
    db: &noxu_db::Database,
    key: u32,
    data: u32,
) {
    let txn = env.begin_transaction(None).unwrap();
    db.put(Some(&txn), &ikey(key), &ikey(data)).unwrap();
    txn.commit().unwrap();
}

// ──────────────────────────────────────────────────────────────────────────────
// GetSearchBothRangeTest.testSearchKeyRangeWithDupTree
//
// JE invariant: with sorted-dups, `getSearchKeyRange` for a non-existent key
// must position on the first dup of the next key.  Inserts: (1,1), (1,2),
// (3,1).  Search for key=2 returns SUCCESS with key=3, data=1.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn search_key_range_with_dup_tree_finds_next_key() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_env_db(&dir, "skr_dup", true);
    put(&env, &db, 1, 1);
    put(&env, &db, 1, 2);
    put(&env, &db, 3, 1);

    let txn = env.begin_transaction(None).unwrap();
    let mut c = db.open_cursor(Some(&txn), None).unwrap();
    let mut k = ikey(2);
    let mut d = DatabaseEntry::new();
    let s = c.get(&mut k, &mut d, Get::SearchRange, None).unwrap();
    assert_eq!(s, OperationStatus::Success);
    assert_eq!(val_u32(&k), 3);
    assert_eq!(val_u32(&d), 1);
    drop(c);
    txn.commit().unwrap();
}

// ──────────────────────────────────────────────────────────────────────────────
// GetSearchBothRangeTest.testSearchBothWithNoDupTree
//
// JE invariant: on a sorted-dups db with only one dup chain (just key=1
// with one dup data=1), `getSearchBoth(1, 2)` must return NotFound (the
// (key,data) pair doesn't exist) but `getSearchBoth(1, 1)` must return
// Success.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn search_both_with_no_dup_tree_finds_existing_pair_only() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_env_db(&dir, "sb_no_dup", true);
    put(&env, &db, 1, 1);

    let txn = env.begin_transaction(None).unwrap();
    let mut c = db.open_cursor(Some(&txn), None).unwrap();
    let mut k = ikey(1);
    let mut d = ikey(2);
    let s = c.get(&mut k, &mut d, Get::SearchBoth, None).unwrap();
    assert_eq!(
        s,
        OperationStatus::NotFound,
        "(1, 2) does not exist; SearchBoth must return NotFound"
    );

    let mut k = ikey(1);
    let mut d = ikey(1);
    let s = c.get(&mut k, &mut d, Get::SearchBoth, None).unwrap();
    assert_eq!(s, OperationStatus::Success);
    drop(c);
    txn.commit().unwrap();
}

// ──────────────────────────────────────────────────────────────────────────────
// GetSearchBothRangeTest.testSuccessDup
//
// JE invariant: with sorted-dups, `getSearchBothRange(3, 0)` where key=3
// has dups (3,1), (3,2) must return SUCCESS positioned at (3, 1) (the
// smallest dup ≥ 0).
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn search_both_range_dup_positions_on_first_dup_at_or_after() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_env_db(&dir, "sbr_succ", true);
    put(&env, &db, 1, 1);
    put(&env, &db, 3, 1);
    put(&env, &db, 1, 2);
    put(&env, &db, 3, 2);

    let txn = env.begin_transaction(None).unwrap();
    let mut c = db.open_cursor(Some(&txn), None).unwrap();
    let mut k = ikey(3);
    let mut d = ikey(0);
    let s = c.get(&mut k, &mut d, Get::SearchBothRange, None).unwrap();
    assert_eq!(s, OperationStatus::Success);
    assert_eq!(val_u32(&k), 3);
    assert_eq!(val_u32(&d), 1);
    drop(c);
    txn.commit().unwrap();
}

// ──────────────────────────────────────────────────────────────────────────────
// GetSearchBothRangeTest.testNotFoundDup
//
// JE invariant: with sorted-dups, `getSearchBothRange` for a key that
// does not exist returns NotFound.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn search_both_range_dup_missing_key_returns_not_found() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_env_db(&dir, "sbr_nf", true);
    put(&env, &db, 1, 1);

    let txn = env.begin_transaction(None).unwrap();
    let mut c = db.open_cursor(Some(&txn), None).unwrap();
    let mut k = ikey(99);
    let mut d = ikey(0);
    let s = c.get(&mut k, &mut d, Get::SearchBothRange, None).unwrap();
    assert_eq!(s, OperationStatus::NotFound);
    drop(c);
    txn.commit().unwrap();
}

// ──────────────────────────────────────────────────────────────────────────────
// GetSearchBothRangeTest.testSearchBefore
//
// JE invariant: with sorted-dups, `getSearchBothRange(1, 2)` when only
// (1, 0) exists must return NotFound (no dup ≥ 2 under key 1).
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn search_both_range_dup_data_before_target_returns_not_found() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_env_db(&dir, "sbr_before", true);
    put(&env, &db, 1, 0);

    let txn = env.begin_transaction(None).unwrap();
    let mut c = db.open_cursor(Some(&txn), None).unwrap();
    let mut k = ikey(1);
    let mut d = ikey(2);
    let s = c.get(&mut k, &mut d, Get::SearchBothRange, None).unwrap();
    assert_eq!(s, OperationStatus::NotFound);
    drop(c);
    txn.commit().unwrap();
}

// ──────────────────────────────────────────────────────────────────────────────
// GetSearchBothRangeTest.testSingleDatumBug
//
// JE invariant: with sorted-dups, `getSearchBothRange(1, 2)` when (1, 1)
// and (2, 2) exist must return NotFound (no dup ≥ 2 under key 1; the
// data search must NOT cross key boundaries).
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn search_both_range_does_not_cross_key_boundary() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_env_db(&dir, "sbr_single", true);
    put(&env, &db, 1, 1);
    put(&env, &db, 2, 2);

    let txn = env.begin_transaction(None).unwrap();
    let mut c = db.open_cursor(Some(&txn), None).unwrap();
    let mut k = ikey(1);
    let mut d = ikey(2);
    let s = c.get(&mut k, &mut d, Get::SearchBothRange, None).unwrap();
    assert_eq!(
        s,
        OperationStatus::NotFound,
        "data search must not cross from key=1 into key=2"
    );
    drop(c);
    txn.commit().unwrap();
}
