//! Part 2 acceptance tests — D2: BOTH_RANGE on non-dup DB.
//!
//! JE reference: `Cursor.java search()` converts `BOTH_RANGE → BOTH` (exact
//! key+data match) when the database has no duplicates.  On a non-dup DB,
//! BOTH_RANGE must NOT do a range-on-key search ignoring `data`.
//!
//! Acceptance criteria (audit brief):
//! BOTH_RANGE on non-dup DB with a non-matching data returns NotFound.
//! fail-pre: returned a range hit (the record with the matching key regardless
//! of data).

use noxu_db::{
    DatabaseConfig, DatabaseEntry, EnvironmentConfig, Get, OperationStatus,
};
use tempfile::TempDir;

fn open_env_db(dir: &TempDir) -> (noxu_db::Environment, noxu_db::Database) {
    let cfg = EnvironmentConfig::new(dir.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    let env = noxu_db::Environment::open(cfg).unwrap();
    let dbcfg =
        DatabaseConfig::new().with_allow_create(true).with_transactional(true);
    let db = env.open_database(None, "test", &dbcfg).unwrap();
    (env, db)
}

fn de(s: &[u8]) -> DatabaseEntry {
    DatabaseEntry::from_bytes(s)
}

// ── D2: BOTH_RANGE non-matching data → NotFound ───────────────────────────────
//
// JE Cursor.search() converts BOTH_RANGE to BOTH on a non-dup database.
// The key exists but data doesn't match → NotFound.
#[test]
fn d2_both_range_non_dup_non_matching_data_returns_not_found() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_env_db(&dir);

    db.put(None, &de(b"key1"), &de(b"data1")).unwrap();
    db.put(None, &de(b"key2"), &de(b"data2")).unwrap();

    let mut cursor = db.open_cursor(None, None).unwrap();
    let mut k = de(b"key1");
    let mut d = de(b"WRONG_DATA"); // data does NOT match stored "data1"

    let status =
        cursor.get(&mut k, &mut d, Get::SearchBothRange, None).unwrap();

    assert_eq!(
        status,
        OperationStatus::NotFound,
        "D2: BOTH_RANGE on non-dup DB with wrong data must return NotFound, \
         not a range hit on the key alone"
    );
}

// ── D2: BOTH_RANGE matching key AND data → Success ───────────────────────────
//
// With the exact data, BOTH_RANGE on a non-dup DB acts like BOTH: success.
#[test]
fn d2_both_range_non_dup_exact_data_returns_success() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_env_db(&dir);

    db.put(None, &de(b"key1"), &de(b"data1")).unwrap();

    let mut cursor = db.open_cursor(None, None).unwrap();
    let mut k = de(b"key1");
    let mut d = de(b"data1"); // exact match

    let status =
        cursor.get(&mut k, &mut d, Get::SearchBothRange, None).unwrap();

    assert_eq!(
        status,
        OperationStatus::Success,
        "D2: BOTH_RANGE on non-dup DB with exact data must succeed"
    );
    assert_eq!(k.get_data().unwrap_or(&[]), b"key1");
}

// ── D2: BOTH_RANGE missing key → NotFound ────────────────────────────────────
#[test]
fn d2_both_range_non_dup_missing_key_returns_not_found() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_env_db(&dir);

    db.put(None, &de(b"key2"), &de(b"data2")).unwrap();

    let mut cursor = db.open_cursor(None, None).unwrap();
    let mut k = de(b"key1"); // key1 doesn't exist; key2 does
    let mut d = de(b"data2");

    let status =
        cursor.get(&mut k, &mut d, Get::SearchBothRange, None).unwrap();

    assert_eq!(
        status,
        OperationStatus::NotFound,
        "D2: BOTH_RANGE on non-dup DB with missing key must return NotFound, \
         not range-advance to key2"
    );
}
