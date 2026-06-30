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
//! See the 2026 review for the wave-4-B
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
        .with_transactional(true)
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
    let mut c = db.open_cursor_in(&txn, None).unwrap();

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
        result.is_err() || matches!(result, Ok(OperationStatus::NotFound)),
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
    let mut c = db.open_cursor_in(&txn, None).unwrap();

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
        result.is_err() || matches!(result, Ok(OperationStatus::NotFound)),
        "put_current after delete (dups) must not succeed: got {:?}",
        result
    );

    drop(c);
    txn.commit().unwrap();
    drop(db);
    drop(env);
}

// ──────────────────────────────────────────────────────────────────────────────
// SR9522 — testGetSearchBothNoDuplicatesAllowedSR9522
// (JE: test/com/sleepycat/je/dbi/DbCursorSearchTest)
//
// JE invariant: on a *non-dup* db, `cursor.getSearchBoth(key, data)` must
// return SUCCESS when the (key, data) pair exists, and NotFound otherwise.
// Pre-fix: `getSearchBoth` on a non-dup db returned NotFound even for the
// existing pair.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn sr9522_get_search_both_works_on_non_dup_db() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_env_and_db(&dir, "sr9522", false);
    let txn = env.begin_transaction(None).unwrap();

    // Mirror JE's `simpleKeyStrings` / `simpleDataStrings` (parallel arrays).
    let pairs: &[(&[u8], &[u8])] = &[
        (b"foo", b"one"),
        (b"bar", b"two"),
        (b"baz", b"three"),
        (b"aaa", b"four"),
        (b"fubar", b"five"),
        (b"foobar", b"six"),
        (b"quux", b"seven"),
        (b"mumble", b"eight"),
        (b"froboy", b"nine"),
    ];
    for (k, d) in pairs {
        db.put_in(&txn,
            &DatabaseEntry::from_bytes(k),
            &DatabaseEntry::from_bytes(d))
        .unwrap();
    }

    let mut c = db.open_cursor_in(&txn, None).unwrap();
    // Existing pair: must SUCCEED (the SR9522 regression).
    let mut k = DatabaseEntry::from_bytes(b"bar");
    let mut d = DatabaseEntry::from_bytes(b"two");
    let s = c.get(&mut k, &mut d, Get::SearchBoth, None).unwrap();
    assert_eq!(
        s,
        OperationStatus::Success,
        "getSearchBoth on existing (key,data) in non-dup db must return Success"
    );

    // Non-existent (k, d') pair: must return NotFound.
    let mut k = DatabaseEntry::from_bytes(b"bar");
    let mut d = DatabaseEntry::from_bytes(b"NOT-PRESENT");
    let s = c.get(&mut k, &mut d, Get::SearchBoth, None).unwrap();
    assert_eq!(s, OperationStatus::NotFound);

    drop(c);
    txn.commit().unwrap();
}

// ──────────────────────────────────────────────────────────────────────────────
// SR8984 — testDeletedReplaySR8984
// (JE: test/com/sleepycat/je/dbi/DbCursorDuplicateDeleteTest)
//
// JE invariant: in a single transaction, put(k, d0) → delete → put(k, d1) →
// put(k, d2) (the latter two on a sorted-dup db) — then ABORT.  After abort,
// the database must be empty (`getFirst` returns NotFound).  Pre-fix, the
// aborted re-inserted dups could re-surface when the cursor was repositioned
// during abort-undo.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn sr8984_aborted_delete_then_reinsert_dups_leaves_empty() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_env_and_db(&dir, "sr8984", true);

    let txn = env.begin_transaction(None).unwrap();
    let mut c = db.open_cursor_in(&txn, None).unwrap();

    let k = DatabaseEntry::from_bytes(b"foo");

    // Put d0, then delete via cursor.
    c.put(&k, &DatabaseEntry::from_bytes(b"d0"), Put::Overwrite).unwrap();
    let mut rk = DatabaseEntry::new();
    let mut rd = DatabaseEntry::new();
    c.get(&mut rk, &mut rd, Get::Current, None).unwrap();
    c.delete().unwrap();

    // Re-insert two more dups under the same key.
    for d in [b"d1".as_slice(), b"d2"] {
        c.put(&k, &DatabaseEntry::from_bytes(d), Put::Overwrite).unwrap();
    }
    drop(c);
    txn.abort().unwrap();

    // After abort, db must be empty.
    let txn2 = env.begin_transaction(None).unwrap();
    let mut c = db.open_cursor_in(&txn2, None).unwrap();
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    let s = c.get(&mut k, &mut d, Get::First, None).unwrap();
    assert_eq!(
        s,
        OperationStatus::NotFound,
        "after aborted put+delete+reinsert, db must be empty"
    );
    drop(c);
    txn2.commit().unwrap();
}

// ──────────────────────────────────────────────────────────────────────────────
// SR12068 — testSR12068 (JE: test/com/sleepycat/je/DbHandleLockTest)
//
// JE invariant: closing a Database handle that is the only open handle must
// release the database-handle lock so a subsequent `removeDatabase` (or
// `truncateDatabase`) succeeds without hanging.  Pre-fix the handle lock
// could leak.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn sr12068_db_handle_lock_released_on_close() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_env_and_db(&dir, "sr12068", false);

    // Insert one record so the database is non-empty.
    db.put(
        &DatabaseEntry::from_bytes(b"k"),
        &DatabaseEntry::from_bytes(b"v"))
    .unwrap();
    db.close().unwrap();

    // Now removeDatabase must not hang or report "in use".
    env.remove_database(None, "sr12068").unwrap();

    // Confirm: re-opening (without allow_create) must report NotFound.
    let dbcfg = DatabaseConfig::new().with_transactional(true);
    let result = env.open_database(None, "sr12068", &dbcfg);
    assert!(result.is_err(), "removed db must not be reopenable");
}

// ──────────────────────────────────────────────────────────────────────────────
// SR11297 — test11297 (JE: test/com/sleepycat/je/test/SR11297Test, partial)
//
// JE invariant: after a sequence of inserts + selective deletes that empties
// the first BIN but leaves a record in a later BIN, `cursor.getFirst` must
// find the surviving record.  The original bug:
// `CursorImpl.positionFirstOrLast` returned NotFound when the first BIN was
// empty but a later one was not.
//
// Noxu's tree layout is the same shape (root → BIN list).  We reproduce the
// invariant by: filling enough records to span multiple BINs, deleting the
// first ones, then asserting `Get::First` returns the smallest surviving key.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn sr11297_get_first_after_first_bin_emptied() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_env_and_db(&dir, "sr11297", false);

    // Insert 200 monotonically-ordered keys; this should fill multiple BINs
    // even on a default node-fanout (Noxu defaults to 128 max keys per BIN).
    let txn = env.begin_transaction(None).unwrap();
    for i in 0u32..200 {
        db.put_in(&txn,
            &DatabaseEntry::from_bytes(&i.to_be_bytes()),
            &DatabaseEntry::from_bytes(b"v"))
        .unwrap();
    }
    txn.commit().unwrap();

    // Delete the first 150 keys (more than one full BIN's worth).
    let txn = env.begin_transaction(None).unwrap();
    for i in 0u32..150 {
        db.delete_in(&txn, &DatabaseEntry::from_bytes(&i.to_be_bytes()))
            .unwrap();
    }
    txn.commit().unwrap();

    // getFirst must find key 150.
    let txn = env.begin_transaction(None).unwrap();
    let mut c = db.open_cursor_in(&txn, None).unwrap();
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    let s = c.get(&mut k, &mut d, Get::First, None).unwrap();
    assert_eq!(
        s,
        OperationStatus::Success,
        "getFirst must find a surviving record even after the first BIN was emptied"
    );
    let expected = 150u32.to_be_bytes();
    assert_eq!(k.get_data().unwrap(), &expected[..]);
    drop(c);
    txn.commit().unwrap();
}

// ──────────────────────────────────────────────────────────────────────────────
// SR9885 — testDuplicateDeadlockSR9885
// (JE: test/com/sleepycat/je/dbi/DbCursorDuplicateDeleteTest, simplified port)
//
// JE invariant: two threads concurrently positioning + deleting on the same
// dup chain must not silently corrupt the chain — at most one wins; the
// loser sees a lock-conflict / deadlock.  In this single-threaded port we
// capture the simpler invariant: deleting a positioned dup removes only that
// dup from the chain, and subsequent get_next returns the next dup.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn sr9885_cursor_delete_removes_only_positioned_dup() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_env_and_db(&dir, "sr9885", true);
    let txn = env.begin_transaction(None).unwrap();
    let key = DatabaseEntry::from_bytes(b"k");

    for d in [b"d0".as_slice(), b"d1", b"d2", b"d3"] {
        db.put_in(&txn, &key, &DatabaseEntry::from_bytes(d)).unwrap();
    }

    let mut c = db.open_cursor_in(&txn, None).unwrap();
    let mut k = DatabaseEntry::from_bytes(b"k");
    let mut d = DatabaseEntry::new();
    let s = c.get(&mut k, &mut d, Get::Search, None).unwrap();
    assert_eq!(s, OperationStatus::Success);
    // Position is the first dup (d0).  Delete it.
    c.delete().unwrap();

    // Walk the remaining dups via get_next; we expect d1, d2, d3.
    let mut found: Vec<Vec<u8>> = Vec::new();
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    let mut s = c.get(&mut k, &mut d, Get::First, None).unwrap();
    while s == OperationStatus::Success {
        found.push(d.get_data().unwrap_or(&[]).to_vec());
        s = c.get(&mut k, &mut d, Get::Next, None).unwrap();
    }
    assert_eq!(
        found,
        vec![b"d1".to_vec(), b"d2".to_vec(), b"d3".to_vec()],
        "after positioned-delete, exactly the deleted dup is gone"
    );
    drop(c);
    txn.commit().unwrap();
}

// ──────────────────────────────────────────────────────────────────────────────
// DupSlotReuseTest.testSameTxnAbort
// (JE: test/com/sleepycat/je/DupSlotReuseTest)
//
// JE invariant: in a single txn, put(k, v0) → delete(k) → put(k, v1) →
// abort.  Post-abort the slot must be empty (no resurrected v0 or v1).
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn dup_slot_reuse_same_txn_abort_leaves_empty() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_env_and_db(&dir, "dsr_same", false);

    let txn = env.begin_transaction(None).unwrap();
    let k = DatabaseEntry::from_bytes(b"k");
    db.put_in(&txn, &k, &DatabaseEntry::from_bytes(b"v0")).unwrap();
    db.delete_in(&txn, &k).unwrap();
    db.put_in(&txn, &k, &DatabaseEntry::from_bytes(b"v1")).unwrap();
    txn.abort().unwrap();

    let mut out = DatabaseEntry::new();
    let s = db.get_into(None, &k, &mut out).unwrap();
    assert!(!s,
        "after same-txn put+delete+put+abort, slot must be empty"
    );
}

// ──────────────────────────────────────────────────────────────────────────────
// DupSlotReuseTest.testDiffTxnAbort
//
// JE invariant: txn1 put(k, v0) commits; txn2 delete(k) + put(k, v1) aborts.
// Post-abort the original v0 must still be present.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn dup_slot_reuse_diff_txn_abort_restores_v0() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_env_and_db(&dir, "dsr_diff", false);

    let txn1 = env.begin_transaction(None).unwrap();
    let k = DatabaseEntry::from_bytes(b"k");
    db.put_in(&txn1, &k, &DatabaseEntry::from_bytes(b"v0")).unwrap();
    txn1.commit().unwrap();

    let txn2 = env.begin_transaction(None).unwrap();
    db.delete_in(&txn2, &k).unwrap();
    db.put_in(&txn2, &k, &DatabaseEntry::from_bytes(b"v1")).unwrap();
    txn2.abort().unwrap();

    let mut out = DatabaseEntry::new();
    let s = db.get_into(None, &k, &mut out).unwrap();
    assert!(s);
    assert_eq!(out.get_data().unwrap(), b"v0");
}
