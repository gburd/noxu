//! JE SR-numbered regression tests ported to Noxu.
//!
//! Each `sr_NNNN_*` function below corresponds to a `testSRNNNN` method in the
//! Berkeley DB Java Edition (JE) test suite.  The SR numbers are JE's internal
//! bug-tracking identifiers: every one represents a real shipped JE bug, so
//! these tests are high-value regression coverage even when the literal
//! Java-level assertions don't translate directly.
//!
//! When a port is not byte-for-byte equivalent (because Noxu and JE diverge on
//! API shape), we assert the *same invariant* — i.e. would the test catch the
//! same class of regression?  Where the two diverge in an interesting way it
//! is documented inline.
//!
//! See `docs/src/internal/wave-4-b-je-tck-port-priority1.md` for the wave-4-B
//! port narrative and the `je-tck-port-2026-05-enumeration-*.tsv` files for
//! per-test status.

use noxu_db::{
    DatabaseConfig, DatabaseEntry, EnvironmentConfig, Get, OperationStatus, Put,
};
use tempfile::TempDir;

// ─── helpers ──────────────────────────────────────────────────────────────────

fn open_env_and_db(
    dir: &TempDir,
    name: &str,
    sorted_dups: bool,
) -> (noxu_db::Environment, noxu_db::Database) {
    let env_config = EnvironmentConfig::new(dir.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    let env = noxu_db::Environment::open(env_config).unwrap();
    let db_config = DatabaseConfig::new()
        .with_allow_create(true)
        .with_sorted_duplicates(sorted_dups);
    let db = env.open_database(None, name, &db_config).unwrap();
    (env, db)
}

// ──────────────────────────────────────────────────────────────────────────────
// SR9900 — testSR9900 (JE: test/com/sleepycat/je/dbi/DbCursorDuplicateDeleteTest)
//
// JE invariant: after `cursor.delete()`, the cursor is logically positioned on
// a deleted record; `cursor.putCurrent(newData)` must return KEYEMPTY (it must
// not silently re-insert under the deleted slot or panic).
//
// Noxu has no `KeyEmpty` status — instead, `Cursor::put(_, _, Put::Current)`
// requires the cursor to be `Initialized`, and `delete()` resets the state to
// `NotInitialized`.  The same regression therefore surfaces as a `put` error
// (rather than a status code).  The invariant captured: putCurrent after
// delete must NOT succeed.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn sr9900_put_current_after_delete_fails_no_dups() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_env_and_db(&dir, "sr9900", false);
    let txn = env.begin_transaction(None).unwrap();
    let mut c = db.open_cursor(Some(&txn), None).unwrap();

    let k = DatabaseEntry::from_bytes(b"k0");
    let d = DatabaseEntry::from_bytes(b"d0");
    c.put(&k, &d, Put::Overwrite).unwrap();

    // Read current to ensure cursor is positioned.
    let mut rk = DatabaseEntry::new();
    let mut rd = DatabaseEntry::new();
    let s = c.get(&mut rk, &mut rd, Get::Current, None).unwrap();
    assert_eq!(s, OperationStatus::Success);

    c.delete().unwrap();

    // The JE assertion: putCurrent → KEYEMPTY.  In Noxu the cursor state is
    // reset by delete, so put(Put::Current) must fail (any non-Success
    // outcome is acceptable).
    let new_d = DatabaseEntry::from_bytes(b"aaaa");
    let result = c.put(&k, &new_d, Put::Current);
    assert!(
        result.is_err()
            || matches!(result, Ok(OperationStatus::NotFound)),
        "put_current after delete must not succeed: got {:?}",
        result
    );

    drop(c);
    txn.commit().unwrap();
    drop(db);
    drop(env);
}

// ──────────────────────────────────────────────────────────────────────────────
// SR9992 — testSR9992 (JE: test/com/sleepycat/je/dbi/DbCursorDuplicateDeleteTest)
//
// JE invariant: with sorted-duplicates, after inserting several dups under one
// key, then positioning on one and deleting it, putCurrent must return
// KEYEMPTY rather than re-add or corrupt the dup chain.
//
// Same Noxu translation as SR9900: putCurrent must fail after delete.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn sr9992_put_current_after_delete_fails_with_dups() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_env_and_db(&dir, "sr9992", true);
    let txn = env.begin_transaction(None).unwrap();
    let mut c = db.open_cursor(Some(&txn), None).unwrap();

    let k = DatabaseEntry::from_bytes(b"key");
    for i in 1..6u8 {
        let d = DatabaseEntry::from_bytes(&[i]);
        c.put(&k, &d, Put::Overwrite).unwrap();
    }

    let mut rk = DatabaseEntry::new();
    let mut rd = DatabaseEntry::new();
    c.get(&mut rk, &mut rd, Get::Current, None).unwrap();
    c.delete().unwrap();

    let new_d = DatabaseEntry::from_bytes(b"aaaa");
    let result = c.put(&k, &new_d, Put::Current);
    assert!(
        result.is_err()
            || matches!(result, Ok(OperationStatus::NotFound)),
        "put_current after delete (dups) must not succeed: got {:?}",
        result
    );

    drop(c);
    txn.commit().unwrap();
    drop(db);
    drop(env);
}
