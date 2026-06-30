//! Part 3 acceptance tests — D3/D4/D10/D11 status mapping.
//!
//! JE references:
//! - D3: `CursorImpl.deleteCurrentRecord()` returns `KEYEMPTY` when
//!   `getCurrentLN()` finds a PD-flagged/absent slot.
//! - D4: `Cursor.putCurrent()` returns `KEYEMPTY` when the slot is defunct.
//! - D10: `Cursor.search(SET_RANGE)` writes the found key back to the
//!   caller's `DatabaseEntry key` (JE: key is an input/output param).
//! - D11: `Cursor.putNoDupData()` throws `UnsupportedOperationException` on
//!   a non-dup DB.  Noxu maps to `NoxuError::OperationNotAllowed`.

use noxu_db::{
    DatabaseConfig, DatabaseEntry, EnvironmentConfig, Get, OperationStatus, Put,
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

// ── D3: delete on defunct slot → KeyEmpty ────────────────────────────────────
//
// If a cursor is positioned on a slot and another cursor deletes it first,
// delete() on the first cursor must return KeyEmpty, not Success.
#[test]
fn d3_delete_on_defunct_slot_returns_key_empty() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_env_db(&dir);

    db.put(de(b"X"), de(b"v")).unwrap();

    // Open two cursors, both positioned on "X".
    let mut c1 = db.open_cursor(None).unwrap();
    let mut c2 = db.open_cursor(None).unwrap();

    let mut k = de(b"X");
    let mut d = DatabaseEntry::new();
    c1.get(&mut k, &mut d, Get::Search, None).unwrap();
    let mut k2 = de(b"X");
    let mut d2 = DatabaseEntry::new();
    c2.get(&mut k2, &mut d2, Get::Search, None).unwrap();

    // c2 deletes the record first.
    let s2 = c2.delete().unwrap();
    assert_eq!(s2, OperationStatus::Success, "c2 delete must succeed");

    // c1 now tries to delete the already-gone slot.
    let s1 = c1.delete().unwrap();
    assert_eq!(
        s1,
        OperationStatus::KeyEmpty,
        "D3: delete on defunct slot must return KeyEmpty, not Success"
    );
}

// ── D4: putCurrent on defunct slot → KeyEmpty ────────────────────────────────
//
// JE: Cursor.putCurrent() returns KEYEMPTY when the slot is absent.
#[test]
fn d4_put_current_on_defunct_slot_returns_key_empty() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_env_db(&dir);

    db.put(de(b"X"), de(b"v")).unwrap();

    let mut c1 = db.open_cursor(None).unwrap();
    let mut c2 = db.open_cursor(None).unwrap();

    let mut k = de(b"X");
    let mut d = DatabaseEntry::new();
    c1.get(&mut k, &mut d, Get::Search, None).unwrap();
    let mut k2 = de(b"X");
    let mut d2 = DatabaseEntry::new();
    c2.get(&mut k2, &mut d2, Get::Search, None).unwrap();

    // c2 deletes the record.
    c2.delete().unwrap();

    // c1 tries putCurrent on the now-defunct slot.
    let r = c1.put(&de(b"X"), &de(b"new"), Put::Current);
    match r {
        Ok(OperationStatus::KeyEmpty) => {} // correct
        other => panic!(
            "D4: putCurrent on defunct slot must return Ok(KeyEmpty), got: {other:?}"
        ),
    }
}

// ── D10: SearchGte writes back the found key ──────────────────────────────────
//
// JE: Cursor.getSearchKeyRange() writes the found key back to the key param.
#[test]
fn d10_search_gte_writes_back_found_key() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_env_db(&dir);

    db.put(de(b"beta"), de(b"v1")).unwrap();
    db.put(de(b"delta"), de(b"v2")).unwrap();

    let mut cursor = db.open_cursor(None).unwrap();
    let mut k = de(b"alpha"); // search key: between nothing and "beta"
    let mut d = DatabaseEntry::new();

    let s = cursor.get(&mut k, &mut d, Get::SearchGte, None).unwrap();
    assert_eq!(s, OperationStatus::Success);
    assert_eq!(
        k.get_data().unwrap_or(&[]),
        b"beta",
        "D10: SearchGte must write the found key back to the key parameter"
    );
}

// ── D11: putNoDupData on non-dup DB → error ───────────────────────────────────
//
// JE: Cursor.putNoDupData() throws UnsupportedOperationException on non-dup DB.
#[test]
fn d11_put_no_dup_data_on_non_dup_db_errors() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_env_db(&dir);

    let mut cursor = db.open_cursor(None).unwrap();
    let k = de(b"key");
    let d = de(b"data");

    let result = cursor.put(&k, &d, Put::NoDupData);
    assert!(
        result.is_err(),
        "D11: putNoDupData on non-dup DB must return an error, got: {result:?}"
    );
    let msg = result.unwrap_err().to_string();
    assert!(
        msg.contains("non-duplicate"),
        "D11: error message must mention non-duplicate database, got: {msg}"
    );
}
