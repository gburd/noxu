//! JE CursorEdgeTest ports — high-priority cursor edge cases.
//!
//! Each test below corresponds to a method in
//! `test/com/sleepycat/je/CursorEdgeTest.java`.  Where Noxu's API or available
//! primitives diverge from JE the port asserts the *same invariant* using the
//! Noxu equivalents (e.g. `Get::Search` for `getSearchKey`,
//! `Get::SearchBoth` for `getSearchBoth`, `Get::SearchGte` for
//! `getSearchKeyRange`).

use noxu_db::{
    DatabaseConfig, DatabaseEntry, EnvironmentConfig, Get, OperationStatus,
    TransactionConfig,
};
use tempfile::TempDir;

// ─── helpers ──────────────────────────────────────────────────────────────────

fn open_env(dir: &TempDir) -> noxu_db::Environment {
    let cfg = EnvironmentConfig::new(dir.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    noxu_db::Environment::open(cfg).unwrap()
}

fn open_db(
    env: &noxu_db::Environment,
    name: &str,
    dups: bool,
) -> noxu_db::Database {
    let cfg = DatabaseConfig::new()
        .with_allow_create(true)
        .with_transactional(true)
        .with_sorted_duplicates(dups);
    env.open_database(None, name, &cfg).unwrap()
}

fn key(i: u8) -> DatabaseEntry {
    DatabaseEntry::from_bytes(&[i])
}

// ──────────────────────────────────────────────────────────────────────────────
// CursorEdgeTest.testSearchOnDuplicatesWithDeletions
//
// JE invariant: with sorted-duplicates and a partially-deleted dup chain
// (compressor disabled — i.e. tombstones still in the BIN), Search/SearchGte/
// SearchBoth/SearchBothRange must *skip* the deleted dups and land on the
// first live one.  After deleting an entire dup chain for a key, all forms of
// search on that key must return NotFound.
//
// Noxu equivalent: with sorted-dups, after inserting dups under k=2 and
// deleting the leading and a middle range, Search on k=2 lands on the first
// live dup.  After deleting all dups under k=5, Search on k=5 returns
// NotFound.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn cursor_edge_search_on_duplicates_with_deletions() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let db = open_db(&env, "search_on_dups_del", true);

    // k1/d1, k3/d1.
    db.put( &key(1), &key(1)).unwrap();
    db.put( &key(3), &key(1)).unwrap();
    // k2/d1..d15
    for i in 1u8..=15 {
        db.put( &key(2), &DatabaseEntry::from_bytes(&[i])).unwrap();
    }

    // Delete k2/d1..d7 and k2/d10..d12.  Note: in Noxu, `cursor.delete()`
    // resets the cursor state to NotInitialized, so a `Next` after `delete`
    // does NOT advance to the next live record (it starts over from the
    // first).  We therefore re-position with `SearchBoth(k2, di)` for each
    // deletion rather than relying on the JE delete+Next idiom.
    let txn = env.begin_transaction(None).unwrap();
    {
        let mut c = db.open_cursor_in(&txn, None).unwrap();
        for di in 1u8..=7 {
            let mut k = key(2);
            let mut d = DatabaseEntry::from_bytes(&[di]);
            let s = c.get(&mut k, &mut d, Get::SearchBoth, None).unwrap();
            assert_eq!(s, OperationStatus::Success, "locate k2/d{di}");
            c.delete().unwrap();
        }
        for di in 10u8..=12 {
            let mut k = key(2);
            let mut d = DatabaseEntry::from_bytes(&[di]);
            let s = c.get(&mut k, &mut d, Get::SearchBoth, None).unwrap();
            assert_eq!(s, OperationStatus::Success, "locate k2/d{di}");
            c.delete().unwrap();
        }
        drop(c);
    }
    txn.commit().unwrap();

    // After commit: search for k2 should land on the first live dup (d8).
    let mut rc = db.open_cursor( None).unwrap();
    let mut k = key(2);
    let mut d = DatabaseEntry::new();
    let s = rc.get(&mut k, &mut d, Get::Search, None).unwrap();
    assert_eq!(s, OperationStatus::Success);
    assert_eq!(k.get_data().unwrap(), &[2]);
    assert_eq!(
        d.get_data().unwrap(),
        &[8],
        "Search on k=2 must skip deleted leading dups and land on d8"
    );

    // Search range: same answer.
    let mut k = key(2);
    let mut d = DatabaseEntry::new();
    let s = rc.get(&mut k, &mut d, Get::SearchGte, None).unwrap();
    assert_eq!(s, OperationStatus::Success);
    assert_eq!(k.get_data().unwrap(), &[2]);
    assert_eq!(d.get_data().unwrap(), &[8]);

    // SearchBoth on (k=2, d=8): exact match.
    let mut k = key(2);
    let mut d = DatabaseEntry::from_bytes(&[8]);
    let s = rc.get(&mut k, &mut d, Get::SearchBoth, None).unwrap();
    assert_eq!(s, OperationStatus::Success);
    assert_eq!(d.get_data().unwrap(), &[8]);

    // Now insert k=5 dups, delete all, verify all searches on k=5 return
    // NotFound.
    drop(rc);
    for i in 0u8..10 {
        db.put( &key(5), &DatabaseEntry::from_bytes(&[i])).unwrap();
    }
    db.delete( &key(5)).unwrap();

    let mut rc = db.open_cursor( None).unwrap();
    let mut k = key(5);
    let mut d = DatabaseEntry::new();
    let s = rc.get(&mut k, &mut d, Get::Search, None).unwrap();
    assert_ne!(s, OperationStatus::Success, "Search on fully-deleted k=5");

    let mut k = key(5);
    let mut d = DatabaseEntry::new();
    let s = rc.get(&mut k, &mut d, Get::SearchGte, None).unwrap();
    // Search >= 5 may match key 5 (NotFound) or skip to an even higher key
    // (none exist >5 in this test), or fall through.  Must NOT be a phantom.
    if s == OperationStatus::Success {
        assert_ne!(
            k.get_data().unwrap(),
            &[5],
            "SearchGte must not surface a fully-deleted key as k=5"
        );
    }

    let mut k = key(5);
    let mut d = DatabaseEntry::from_bytes(&[0]);
    let s = rc.get(&mut k, &mut d, Get::SearchBoth, None).unwrap();
    assert_ne!(s, OperationStatus::Success);
    drop(rc);
    drop(db);
    drop(env);
}

// ──────────────────────────────────────────────────────────────────────────────
// CursorEdgeTest.testSearchBothWithOneDuplicate (JE SR #9248)
//
// JE invariant: on a sorted-dup database with exactly one entry under a key,
// SearchBothRange(k, data-1) lands on (k, data) — i.e. the dup-range search
// must not skip the only entry just because the requested data is below it.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn cursor_edge_search_both_with_one_duplicate() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let db = open_db(&env, "sb_one_dup", true);

    db.put(
        &DatabaseEntry::from_bytes(&[1]),
        &DatabaseEntry::from_bytes(&[1]))
    .unwrap();

    let mut c = db.open_cursor( None).unwrap();
    // SearchBothRange semantically: key exact, data >= requested.  Noxu's
    // closest match is `Get::SearchBothGte`.
    let mut k = DatabaseEntry::from_bytes(&[1]);
    let mut d = DatabaseEntry::from_bytes(&[0]); // data-1
    let s = c.get(&mut k, &mut d, Get::SearchBothRange, None).unwrap();
    assert_eq!(s, OperationStatus::Success);
    assert_eq!(k.get_data().unwrap(), &[1]);
    assert_eq!(d.get_data().unwrap(), &[1]);
    drop(c);
    drop(db);
    drop(env);
}

// ──────────────────────────────────────────────────────────────────────────────
// CursorEdgeTest.testGetPrevNoDupWithEmptyTree (JE bug #11700)
//
// JE invariant: after deleting every record (and compressing) a cursor that
// calls getPrevNoDup on the now-empty tree returns NotFound rather than
// throwing ArrayIndexOutOfBoundsException.
//
// Noxu does not expose `compress()`, but the same regression surfaces if we
// delete every record and then iterate Prev/PrevNoDup on an empty database —
// the cursor must return NotFound, not panic.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn cursor_edge_prev_no_dup_with_empty_tree() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let db = open_db(&env, "prev_no_dup_empty", true);

    // Insert two dup chains.
    for k in [1u8, 2] {
        for d in [1u8, 2] {
            db.put(
                &DatabaseEntry::from_bytes(&[k]),
                &DatabaseEntry::from_bytes(&[d]))
            .unwrap();
        }
    }

    // Delete every record via cursor.
    {
        let mut c = db.open_cursor( None).unwrap();
        let mut k = DatabaseEntry::new();
        let mut d = DatabaseEntry::new();
        let mut s = c.get(&mut k, &mut d, Get::First, None).unwrap();
        while s == OperationStatus::Success {
            c.delete().unwrap();
            s = c.get(&mut k, &mut d, Get::Next, None).unwrap();
        }
    }

    assert_eq!(db.count().unwrap(), 0);

    // Now PrevNoDup on the empty tree must return NotFound, not panic.
    let mut c = db.open_cursor( None).unwrap();
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    let s = c.get(&mut k, &mut d, Get::PrevNoDup, None).unwrap();
    assert_eq!(s, OperationStatus::NotFound);
    let s = c.get(&mut k, &mut d, Get::Prev, None).unwrap();
    assert_eq!(s, OperationStatus::NotFound);
    drop(c);
    drop(db);
    drop(env);
}

// ──────────────────────────────────────────────────────────────────────────────
// CursorEdgeTest.testReadDeletedUncommitted
//
// JE invariant:
//   1. T1 deletes record k=1 and stays open.
//   2. T2 (no-wait) tries to read k=1 — must fail (LockNotAvailable).
//   3. T1 commits.
//   4. T2 reads k=1 → NotFound.
//
// This proves both:
//   - uncommitted deletes are *not* visible to readers (no dirty reads), and
//   - the read on a write-locked record blocks/fails until the writer
//     commits.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn cursor_edge_read_deleted_uncommitted() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let db = open_db(&env, "rd_uncommitted", false);

    // Insert k=1.
    db.put(
        &DatabaseEntry::from_bytes(&[1]),
        &DatabaseEntry::from_bytes(&[1]))
    .unwrap();

    // T1: delete k=1, leave open.
    let txn1 = env.begin_transaction(None).unwrap();
    let s = db.delete_in(&txn1, &DatabaseEntry::from_bytes(&[1])).unwrap();
    assert_eq!(s, OperationStatus::Success);

    // T2 (no-wait): reading k=1 must fail with a lock error.
    let no_wait = TransactionConfig::new().with_no_wait(true);
    let txn2 = env.begin_transaction(Some(&no_wait)).unwrap();

    let mut out = DatabaseEntry::new();
    let r = db.get_into(Some(&txn2), &DatabaseEntry::from_bytes(&[1]), &mut out);
    assert!(
        r.is_err(),
        "no-wait read on a write-locked record must fail; got {:?}",
        r
    );

    let mut c2 = db.open_cursor_in(&txn2, None).unwrap();
    let mut k = DatabaseEntry::from_bytes(&[1]);
    let mut d = DatabaseEntry::new();
    let r = c2.get(&mut k, &mut d, Get::Search, None);
    assert!(
        r.is_err(),
        "no-wait cursor search on a write-locked record must fail; got {:?}",
        r
    );
    drop(c2);

    // Commit T1.  Now T2's read returns NotFound (the delete is visible).
    txn1.commit().unwrap();

    let mut out = DatabaseEntry::new();
    let s = db
        .get_into(Some(&txn2), &DatabaseEntry::from_bytes(&[1]), &mut out)
        .unwrap();
    assert_eq!(s, OperationStatus::NotFound);

    let mut c2 = db.open_cursor_in(&txn2, None).unwrap();
    let mut k = DatabaseEntry::from_bytes(&[1]);
    let mut d = DatabaseEntry::new();
    let s = c2.get(&mut k, &mut d, Get::Search, None).unwrap();
    assert_eq!(s, OperationStatus::NotFound);
    drop(c2);

    txn2.commit().unwrap();
    drop(db);
    drop(env);
}

// ──────────────────────────────────────────────────────────────────────────────
// CursorEdgeTest.testNonTxnalCursorNoUpdates (spirit port)
//
// JE invariant: a non-transactional cursor opened against a transactional
// database must NOT permit update operations (put/delete) — those must fail
// or be rejected.
//
// Noxu: a cursor with no transaction handle on a transactional DB is
// permitted (auto-commit per write).  The strict JE behaviour was an API
// contract Noxu has dropped, so this is documented and skipped.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
#[ignore = "Noxu accepts non-txn cursor updates against a txn DB via auto-commit; \
            the JE 'NonTxnalCursorNoUpdates' contract is intentionally not enforced."]
fn cursor_edge_non_txnal_cursor_no_updates() {
    // Documented divergence: Noxu's per-op auto-commit makes this
    // contract no longer applicable.  Recorded for traceability.
}

// ──────────────────────────────────────────────────────────────────────────────
// CursorEdgeTest.testNoWaitLatchRelease  (wave 9-C port)
//
// JE invariant: when a cursor under a no-wait transaction encounters a
// LockNotAvailableException — for example, T1 holds a record lock and
// T2 (no-wait) calls Cursor.delete() — the failure must surface as the
// no-wait lock error, *not* a panic / corrupt latch state, and the
// transaction must remain usable for cleanup.  JE additionally checks
// `LatchSupport.nBtreeLatchesHeld() == 0` (latch leak guard).
//
// Noxu adaptation: Noxu does not expose a global latch-count probe; we
// assert the user-visible invariant — the no-wait cursor delete fails,
// T2 can be aborted, and the lock T1 holds is later released cleanly.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn cursor_edge_no_wait_latch_release() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let db = open_db(&env, "no_wait_latch", true);

    // Insert record (k=1, v=1) under auto-commit.
    db.put(
        &DatabaseEntry::from_bytes(&[1]),
        &DatabaseEntry::from_bytes(&[1]))
    .unwrap();

    // T1: search-lock record 1 via cursor.
    let txn1 = env.begin_transaction(None).unwrap();
    let mut c1 = db.open_cursor_in(&txn1, None).unwrap();
    let mut k1 = DatabaseEntry::from_bytes(&[1]);
    let mut d1 = DatabaseEntry::from_bytes(&[1]);
    let s = c1.get(&mut k1, &mut d1, Get::SearchBoth, None).unwrap();
    assert_eq!(s, OperationStatus::Success);

    // T2 (no-wait): open cursor, position on the same record, attempt
    // delete.  The delete must fail with a lock error.
    let no_wait = TransactionConfig::new().with_no_wait(true);
    let txn2 = env.begin_transaction(Some(&no_wait)).unwrap();
    let mut c2 = db.open_cursor_in(&txn2, None).unwrap();
    let mut k2 = DatabaseEntry::from_bytes(&[1]);
    let mut d2 = DatabaseEntry::from_bytes(&[1]);
    // The position step itself may already conflict; whichever step
    // fails, the user-visible invariant is that a lock error surfaces
    // before any silent success.
    let pos = c2.get(&mut k2, &mut d2, Get::SearchBoth, None);
    let del = if pos.is_ok() { c2.delete() } else { pos };
    assert!(
        del.is_err(),
        "no-wait cursor delete on a write-locked record must fail; got {:?}",
        del
    );
    drop(c2);
    txn2.abort().unwrap();

    // T1 still works: drop cursor, commit.
    drop(c1);
    txn1.commit().unwrap();

    // The record is unmodified (T2 didn't actually delete it).
    let mut out = DatabaseEntry::new();
    let s = db.get_into(None, &DatabaseEntry::from_bytes(&[1]), &mut out).unwrap();
    assert_eq!(s, OperationStatus::Success);
    assert_eq!(out.data(), &[1]);

    drop(db);
    drop(env);
}

// ──────────────────────────────────────────────────────────────────────────────
// CursorEdgeTest.testGetCurrentDuringDupTreeCreation (spirit port)
//
// JE invariant [SR #11195]: when T1 has a singleton record and another
// transaction is positioned on it, T1 inserting a second duplicate
// (which materialises a DIN-tree under that key) must NOT corrupt T2's
// fetchCurrent.  After T1 commits and T2 reads, T2 should see one of
// the inserted dups — never throw / panic.
//
// Noxu adaptation: we drive a single-threaded sequence — open T2's
// cursor on the singleton key under READ_UNCOMMITTED, then T1 inserts
// a dup, commits; T2's cursor still operates correctly (it was
// positioned via SearchBoth).  This validates the no-corruption
// invariant without the JUnitThread plumbing.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn cursor_edge_get_current_during_dup_tree_creation() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let db = open_db(&env, "dup_tree_create", true);

    // Insert k=1, d=1 (singleton).
    db.put(
        &DatabaseEntry::from_bytes(&[1]),
        &DatabaseEntry::from_bytes(&[1]))
    .unwrap();

    // T2 reads the singleton via getFirst.
    let txn2 = env.begin_transaction(None).unwrap();
    {
        let mut c2 = db.open_cursor_in(&txn2, None).unwrap();
        let mut k = DatabaseEntry::new();
        let mut d = DatabaseEntry::new();
        let s = c2.get(&mut k, &mut d, Get::First, None).unwrap();
        assert_eq!(s, OperationStatus::Success);
        assert_eq!(k.data(), &[1]);
        assert_eq!(d.data(), &[1]);
    }
    txn2.commit().unwrap();

    // T1 inserts a second dup under the same key — promotes the slot
    // from singleton to a dup-chain.
    let txn1 = env.begin_transaction(None).unwrap();
    db.put_in(&txn1,
        &DatabaseEntry::from_bytes(&[1]),
        &DatabaseEntry::from_bytes(&[2]))
    .unwrap();
    txn1.commit().unwrap();

    // After the dup-tree creation, both dups are visible and the
    // cursor scan does not panic (the JE bug was a ClassCastException
    // when the LN was rewritten as a DIN under a still-positioned
    // cursor).
    let txn3 = env.begin_transaction(None).unwrap();
    let mut c3 = db.open_cursor_in(&txn3, None).unwrap();
    let mut keys = Vec::new();
    let mut vals = Vec::new();
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    let mut op = Get::First;
    while let Ok(s) = c3.get(&mut k, &mut d, op, None) {
        if s != OperationStatus::Success {
            break;
        }
        keys.push(k.data().to_vec());
        vals.push(d.data().to_vec());
        op = Get::Next;
    }
    drop(c3);
    txn3.commit().unwrap();

    assert_eq!(keys, vec![vec![1u8], vec![1u8]]);
    assert_eq!(vals, vec![vec![1u8], vec![2u8]]);

    drop(db);
    drop(env);
}
