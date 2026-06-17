//! Part 1 acceptance tests — D1 (delete cursor position) + D5 (insert shift).
//!
//! JE references:
//! - `CursorImpl.adjustCursorsForInsert` (~line 997): increments the index of
//!   every cursor whose `index >= insertIndex` after an in-place slot insert.
//! - `CursorImpl.deleteCurrentRecord()` + `getNext()` PD-flag: keeps the cursor
//!   positioned so that the next `Next`/`Prev` yields the correct record.
//!
//! Acceptance criteria (from the audit brief):
//!
//! (a) Insert-shift — position at key K, insert a key sorting before K,
//!     `Get::Current` still returns K.
//!     fail-pre: returned NotFound/wrong key because current_index was stale.
//!
//! (b) Delete-then-Next forward — position, delete, Next → successor key.
//!     fail-pre: returned NotFound because cursor was reset to NotInitialized.
//!
//! (c) Delete-then-Prev backward — position, delete, Prev → predecessor key.
//!     fail-pre: returned NotFound.

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

// ── (a) Insert-shift: Get::Current after concurrent insert before K ──────────
//
// JE CursorImpl.adjustCursorsForInsert: all cursors with index >= insertIndex
// have their index incremented.  Noxu's lazy re-anchor detects the key mismatch
// at current_index (D5 CC-1 extension) and re-anchors.
#[test]
fn d5_insert_before_positioned_cursor_get_current_still_returns_k() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_env_db(&dir);

    // Insert three keys; position cursor on "bravo".
    db.put(None, &de(b"alpha"), &de(b"1")).unwrap();
    db.put(None, &de(b"bravo"), &de(b"2")).unwrap();
    db.put(None, &de(b"delta"), &de(b"3")).unwrap();

    let mut cursor = db.open_cursor(None, None).unwrap();
    let mut k = de(b"bravo");
    let mut d = DatabaseEntry::new();
    let s = cursor.get(&mut k, &mut d, Get::Search, None).unwrap();
    assert_eq!(s, OperationStatus::Success, "search bravo");

    // Simulate concurrent insert of a key that sorts before "bravo" ("charlie"
    // sorts after "bravo" alphabetically but between "bravo" and "delta"; use
    // "b_sub" which sorts between "alpha" and "bravo" to shift bravo's index).
    // We insert "b_aaa" which sorts before "bravo" but after "alpha", pushing
    // "bravo" from index 1 → 2.
    db.put(None, &de(b"b_aaa"), &de(b"0")).unwrap();

    // Get::Current must still return "bravo" even though its BIN index shifted.
    let mut ck = DatabaseEntry::new();
    let mut cd = DatabaseEntry::new();
    let cs = cursor.get(&mut ck, &mut cd, Get::Current, None).unwrap();
    assert_eq!(cs, OperationStatus::Success, "Get::Current after insert-shift");
    assert_eq!(
        ck.get_data().unwrap_or(&[]),
        b"bravo",
        "D5: cursor must still be on 'bravo' after insert shifted its index"
    );
}

// ── (b) Delete-then-Next (forward) ────────────────────────────────────────────
//
// JE: delete() sets PD flag; next Next() skips it and returns the successor.
// Noxu: delete() sets PendingDeleted; retrieve_next(Next) starts from current_index
// (the gap = former successor slot), not current_index+1.
#[test]
fn d1_delete_then_next_returns_successor() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_env_db(&dir);

    db.put(None, &de(b"A"), &de(b"1")).unwrap();
    db.put(None, &de(b"B"), &de(b"2")).unwrap();
    db.put(None, &de(b"C"), &de(b"3")).unwrap();

    let mut cursor = db.open_cursor(None, None).unwrap();
    let mut k = de(b"B");
    let mut d = DatabaseEntry::new();
    let s = cursor.get(&mut k, &mut d, Get::Search, None).unwrap();
    assert_eq!(s, OperationStatus::Success, "search B");

    // Delete "B".
    let ds = cursor.delete().unwrap();
    assert_eq!(ds, OperationStatus::Success);

    // D1: Next must return "C" (the successor of the deleted slot).
    let mut nk = DatabaseEntry::new();
    let mut nd = DatabaseEntry::new();
    let ns = cursor.get(&mut nk, &mut nd, Get::Next, None).unwrap();
    assert_eq!(
        ns,
        OperationStatus::Success,
        "D1: Next after delete must return successor, not NotFound"
    );
    assert_eq!(
        nk.get_data().unwrap_or(&[]),
        b"C",
        "D1: successor key must be 'C'"
    );
}

// ── (c) Delete-then-Prev (backward) ──────────────────────────────────────────
//
// Deleting the middle entry; Prev from PendingDeleted must return predecessor.
#[test]
fn d1_delete_then_prev_returns_predecessor() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_env_db(&dir);

    db.put(None, &de(b"A"), &de(b"1")).unwrap();
    db.put(None, &de(b"B"), &de(b"2")).unwrap();
    db.put(None, &de(b"C"), &de(b"3")).unwrap();

    let mut cursor = db.open_cursor(None, None).unwrap();
    let mut k = de(b"B");
    let mut d = DatabaseEntry::new();
    cursor.get(&mut k, &mut d, Get::Search, None).unwrap();

    // Delete "B".
    cursor.delete().unwrap();

    // Prev must yield "A".
    let mut pk = DatabaseEntry::new();
    let mut pd = DatabaseEntry::new();
    let ps = cursor.get(&mut pk, &mut pd, Get::Prev, None).unwrap();
    assert_eq!(
        ps,
        OperationStatus::Success,
        "D1: Prev after delete must return predecessor"
    );
    assert_eq!(
        pk.get_data().unwrap_or(&[]),
        b"A",
        "D1: predecessor must be 'A'"
    );
}

// ── (d) Delete last entry, Next returns NotFound ──────────────────────────────
#[test]
fn d1_delete_last_entry_next_returns_not_found() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_env_db(&dir);

    db.put(None, &de(b"A"), &de(b"1")).unwrap();
    db.put(None, &de(b"B"), &de(b"2")).unwrap();

    let mut cursor = db.open_cursor(None, None).unwrap();
    let mut k = de(b"B");
    let mut d = DatabaseEntry::new();
    cursor.get(&mut k, &mut d, Get::Search, None).unwrap();
    cursor.delete().unwrap();

    let mut nk = DatabaseEntry::new();
    let mut nd = DatabaseEntry::new();
    let ns = cursor.get(&mut nk, &mut nd, Get::Next, None).unwrap();
    assert_eq!(ns, OperationStatus::NotFound, "delete last, Next = NotFound");
}

// ── (e) Delete first entry, Prev returns NotFound ────────────────────────────
#[test]
fn d1_delete_first_entry_prev_returns_not_found() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_env_db(&dir);

    db.put(None, &de(b"A"), &de(b"1")).unwrap();
    db.put(None, &de(b"B"), &de(b"2")).unwrap();

    let mut cursor = db.open_cursor(None, None).unwrap();
    let mut k = de(b"A");
    let mut d = DatabaseEntry::new();
    cursor.get(&mut k, &mut d, Get::Search, None).unwrap();
    cursor.delete().unwrap();

    let mut pk = DatabaseEntry::new();
    let mut pd = DatabaseEntry::new();
    let ps = cursor.get(&mut pk, &mut pd, Get::Prev, None).unwrap();
    assert_eq!(ps, OperationStatus::NotFound, "delete first, Prev = NotFound");
}

// ── (f) Full iterate-and-delete walk ─────────────────────────────────────────
//
// Classic JE pattern: position, delete, Next yields the next record.
// Walk all records deleting them one by one; the database must be empty after.
#[test]
fn d1_iterate_and_delete_all_records() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_env_db(&dir);

    let keys: &[&[u8]] = &[b"A", b"B", b"C", b"D", b"E"];
    for k in keys {
        db.put(None, &de(k), &de(b"v")).unwrap();
    }

    let mut cursor = db.open_cursor(None, None).unwrap();
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    let mut status = cursor.get(&mut k, &mut d, Get::First, None).unwrap();
    let mut deleted = 0usize;

    while status == OperationStatus::Success {
        cursor.delete().unwrap();
        deleted += 1;
        // JE idiom: Next after delete yields successor.
        status = cursor.get(&mut k, &mut d, Get::Next, None).unwrap();
    }

    assert_eq!(deleted, keys.len(), "all records must be deleted");
    assert_eq!(db.count().unwrap(), 0, "database must be empty");
}
