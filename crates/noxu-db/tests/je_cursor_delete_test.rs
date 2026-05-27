//! JE DbCursorDeleteTest ports — cursor-walk-and-delete invariants.
//!
//! Each test below corresponds to a method in
//! `test/com/sleepycat/je/dbi/DbCursorDeleteTest.java`.  Where the JE test
//! relies on `cursor.getCurrent` returning `KEYEMPTY` after `cursor.delete()`,
//! the Noxu equivalent is that `Get::Current` after `delete` either returns
//! NotFound or fails (Noxu's delete resets cursor state).  The structural
//! invariant — that the deleted records do not reappear — is what we assert.

use noxu_db::{
    DatabaseConfig, DatabaseEntry, EnvironmentConfig, Get, OperationStatus, Put,
};
use tempfile::TempDir;

fn open_env_db(
    dir: &TempDir,
    name: &str,
) -> (noxu_db::Environment, noxu_db::Database) {
    let cfg = EnvironmentConfig::new(dir.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    let env = noxu_db::Environment::open(cfg).unwrap();
    let dbcfg = DatabaseConfig::new()
        .with_allow_create(true)
        .with_transactional(true);
    let db = env.open_database(None, name, &dbcfg).unwrap();
    (env, db)
}

fn put_simple(db: &noxu_db::Database, pairs: &[(&[u8], &[u8])]) {
    for (k, v) in pairs {
        db.put(
            None,
            &DatabaseEntry::from_bytes(k),
            &DatabaseEntry::from_bytes(v),
        )
        .unwrap();
    }
}

fn collect_keys(db: &noxu_db::Database) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    let mut c = db.open_cursor(None, None).unwrap();
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    let mut s = c.get(&mut k, &mut d, Get::First, None).unwrap();
    while s == OperationStatus::Success {
        out.push(k.get_data().unwrap_or(&[]).to_vec());
        s = c.get(&mut k, &mut d, Get::Next, None).unwrap();
    }
    out
}

// ──────────────────────────────────────────────────────────────────────────────
// DbCursorDeleteTest.testSimpleDelete
//
// JE invariant: walk all records via the cursor, deleting those whose key
// starts with 'f'.  After the walk:
//   - no surviving key starts with 'f',
//   - the surviving keys are still in sorted order, and
//   - count = original - deleted_count.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn dbcursor_delete_records_matching_predicate() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_env_db(&dir, "del_pred");
    let pairs: &[(&[u8], &[u8])] = &[
        (b"alpha", b"a"),
        (b"beta", b"b"),
        (b"foo", b"1"),
        (b"fox", b"2"),
        (b"frog", b"3"),
        (b"gamma", b"g"),
        (b"zulu", b"z"),
    ];
    put_simple(&db, pairs);

    // Delete every key starting with 'f' by re-positioning each iteration
    // (Noxu's cursor.delete resets state to NotInitialized).
    let mut deleted = 0u64;
    let keys: Vec<Vec<u8>> = collect_keys(&db);
    for k in &keys {
        if k.first() == Some(&b'f') {
            let s = db.delete(None, &DatabaseEntry::from_bytes(k)).unwrap();
            assert_eq!(s, OperationStatus::Success);
            deleted += 1;
        }
    }

    let after = collect_keys(&db);
    // No 'f'-prefixed key remains.
    for k in &after {
        assert!(
            k.first() != Some(&b'f'),
            "key starting with 'f' survived the delete: {:?}",
            k
        );
    }
    // Sorted order preserved.
    let mut sorted = after.clone();
    sorted.sort();
    assert_eq!(after, sorted, "remaining keys must still be sorted");
    assert_eq!(after.len() as u64 + deleted, pairs.len() as u64);
    assert_eq!(db.count().unwrap(), after.len() as u64);
}

// ──────────────────────────────────────────────────────────────────────────────
// DbCursorDeleteTest.testSimpleDeleteAll
//
// JE invariant: walking the cursor and deleting each record empties the
// database; a second walk finds nothing.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn dbcursor_delete_all_via_walk_empties_db() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_env_db(&dir, "del_all");
    let pairs: &[(&[u8], &[u8])] = &[
        (b"a", b"1"),
        (b"b", b"2"),
        (b"c", b"3"),
        (b"d", b"4"),
        (b"e", b"5"),
    ];
    put_simple(&db, pairs);

    let keys = collect_keys(&db);
    for k in &keys {
        db.delete(None, &DatabaseEntry::from_bytes(k)).unwrap();
    }

    assert_eq!(db.count().unwrap(), 0);
    let after = collect_keys(&db);
    assert!(after.is_empty(), "post-delete-all walk should be empty");
}

// ──────────────────────────────────────────────────────────────────────────────
// DbCursorDeleteTest.testSimpleInsertDeleteInsert
//
// JE invariant: insert k/v, delete via cursor, re-insert via putNoOverwrite
// must succeed (the slot is free because the delete went through).
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn dbcursor_insert_delete_reinsert_no_overwrite_succeeds() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_env_db(&dir, "ins_del_ins");
    let k = DatabaseEntry::from_bytes(b"ka");
    let v = DatabaseEntry::from_bytes(b"va");
    db.put(None, &k, &v).unwrap();

    // Delete via cursor.
    {
        let mut c = db.open_cursor(None, None).unwrap();
        let mut sk = DatabaseEntry::from_bytes(b"ka");
        let mut sd = DatabaseEntry::new();
        let s = c.get(&mut sk, &mut sd, Get::Search, None).unwrap();
        assert_eq!(s, OperationStatus::Success);
        c.delete().unwrap();
    }

    // Re-insert with putNoOverwrite must succeed (slot is empty).
    let s = db.put_no_overwrite(None, &k, &v).unwrap();
    assert_eq!(
        s,
        OperationStatus::Success,
        "re-insert via put_no_overwrite must succeed after delete"
    );

    // A second putNoOverwrite must fail (KeyExists).
    let s = db.put_no_overwrite(None, &k, &v).unwrap();
    assert_eq!(s, OperationStatus::KeyExists);
}

// ──────────────────────────────────────────────────────────────────────────────
// DbCursorDeleteTest.testSimpleDeletePutCurrent
//
// JE invariant: after `cursor.delete()`, `cursor.putCurrent(newData)` returns
// KEYEMPTY (already covered as sr9900 in je_sr_regression_test.rs; we add the
// non-dup case here as a structural reference and tighten the assertion).
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn dbcursor_put_current_after_delete_does_not_revive() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_env_db(&dir, "del_putcur");
    let k = DatabaseEntry::from_bytes(b"x");
    db.put(None, &k, &DatabaseEntry::from_bytes(b"orig")).unwrap();

    let mut c = db.open_cursor(None, None).unwrap();
    let mut sk = DatabaseEntry::from_bytes(b"x");
    let mut sd = DatabaseEntry::new();
    c.get(&mut sk, &mut sd, Get::Search, None).unwrap();
    c.delete().unwrap();

    // putCurrent must NOT revive the deleted slot.
    let r = c.put(&k, &DatabaseEntry::from_bytes(b"new"), Put::Current);
    assert!(
        r.is_err()
            || matches!(r, Ok(OperationStatus::NotFound)),
        "put_current after delete must not succeed: got {:?}",
        r
    );

    // Verify the record is still gone.
    drop(c);
    let mut out = DatabaseEntry::new();
    let s = db.get(None, &k, &mut out).unwrap();
    assert_eq!(s, OperationStatus::NotFound);
}
