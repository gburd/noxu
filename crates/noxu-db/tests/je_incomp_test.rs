//! Test-parity ports for JE `com.sleepycat.je.incomp` (INCompressor package).
//!
//! JE source:
//!   test/com/sleepycat/je/incomp/EmptyBINTest.java   (6 @Test)
//!   test/com/sleepycat/je/incomp/INCompressorTest.java (23 @Test)
//!
//! ## Design deviation (read this before judging fidelity)
//!
//! JE's INCompressor is a **two-phase, daemon-driven** subsystem:
//!
//!   1. `CursorImpl.deleteCurrentRecord()` does NOT remove the slot.  It only
//!      sets the slot's `PendingDeleted` flag; the slot STAYS in the BIN,
//!      still carrying its key, still counted by `bin.getNEntries()`.  That is
//!      why JE tests assert `checkBinEntriesAndCursors(bin, 2, 1)` right after
//!      a delete: the entry count has NOT dropped.
//!   2. Later, the background `INCompressor` daemon (or an explicit
//!      `env.compress()`) drains a queue of `BINReference`s and physically
//!      removes the PendingDeleted slots — but ONLY if no cursor is parked on
//!      the BIN and no txn still locks the slot.  When that empties the BIN,
//!      the daemon prunes the empty BIN from its parent IN.
//!
//! Noxu is **lock-based and single-phase**:
//!
//!   * A committed delete (`Tree::delete` via `CursorImpl::apply_tree_delete`)
//!     PHYSICALLY REMOVES the slot immediately (the lock on the slot's LSN,
//!     not a PendingDeleted flag, is what blocks concurrent readers until the
//!     writer commits/aborts).  `bin.getNEntries()` drops on the delete, not
//!     on a later compress.
//!   * Noxu DOES have a background `noxu-in-compressor` daemon AND an explicit
//!     `env.compress()`, but both only reclaim `known_deleted` *tombstone*
//!     slots.  In Noxu a `known_deleted` tombstone arises only from
//!     BIN-delta reconstitution (an aborted-insert slot preserved across
//!     eviction+refault so it does not resurrect) — NOT from an ordinary
//!     committed delete.  The `known_deleted` reclamation path, the
//!     active-cursor/lock re-check that blocks it, and prefix recompute are
//!     unit-tested directly against `compress_bin` /
//!     `compress_bin_with_lock_check` / `prune_empty_bin` /
//!     `maybe_compress_bin_and_parent` in `noxu-tree/src/tree.rs`
//!     (`test_incompressor_*`, `test_compress_bin_*`, `test_ic1_*`,
//!     `test_ic3_*`), cited against these same JE tests.
//!
//! ### Consequences for what is portable here
//!
//! PORTABLE + ported below (public Environment/Database/Cursor/Transaction):
//!   * EmptyBINTest — scan/search correctness across an empty middle BIN and
//!     across an empty edge BIN (all 6 methods; different scan directions and
//!     start points).  Noxu leaves the emptied BIN in place, so this exercises
//!     the "scan/search must be correct in the face of an empty BIN" invariant
//!     even more directly than JE (where the compressor might race the scan).
//!   * INCompressorTest — the *observable* behaviors that survive the
//!     single-phase model: a committed delete removes the slot and the record
//!     is gone; the tree stays consistent and queryable after delete-all; an
//!     abort/rollback of an insert leaves the record absent; a re-insert after
//!     delete makes the key present again (the "NodeNotEmpty / re-insert
//!     cancels the prune" behavior, observed as: the key is live again).
//!
//! N/A here (recorded, not silently dropped) — see the report:
//!   * `checkINCompQueueSize(n)` — Noxu has no per-BIN `BINReference` queue and
//!     no `getINCompressorQueueSize()`.  Genuine daemon-mechanic deviation.
//!   * `checkBinEntriesAndCursors(bin, N, C)` slot-count timing — asserts the
//!     JE PendingDeleted-then-compress timing (count stays high after delete,
//!     drops on compress).  Noxu drops the count on the delete itself, so the
//!     specific N values do not transfer.  The *semantics* the assertions
//!     protect (delete removes the record; a cursor blocks reclamation; a
//!     non-empty BIN is not pruned) are covered — either by the tree.rs unit
//!     tests (the `known_deleted` reclamation path) or by the public-API
//!     record-presence assertions below.
//!   * BIN-delta variants (`*WithBinDeltas`) — the delta-vs-full logging
//!     decision and `setProhibitNextDelta`/`checkINCompQueueSize` interplay is
//!     JE BIN-delta-logging mechanic; covered at the delta level in
//!     `je_dbi_bin_delta_test.rs`.  The record-level outcome is identical to
//!     the non-delta variant and is asserted here via the non-delta port.
//!   * DBIN/DIN variants (`*Duplicate`, `*DBIN*`) — "DBINs are no longer used"
//!     (JE's own comment); Noxu has no DIN/DBIN node type (sorted dups share
//!     the BIN).  The dup delete-all outcome (record gone, tree consistent) is
//!     the same behavior as the no-dup port and is asserted for dups below.
//!
//! ### Engine finding (escalated, NOT fixed here)
//!
//! `empty_bin_left_by_committed_deletes_is_never_pruned` (ignored) captures a
//! genuine space-reclamation gap: an emptied-by-committed-deletes BIN is never
//! removed from its parent — not by `env.compress()`, not by checkpoint/sync,
//! not by the background daemon — because Noxu's collectors only visit BINs
//! with `known_deleted` slots and a committed delete leaves none.  Correctness
//! holds (records gone, tree queryable); this is compaction debt, not data
//! loss.  A production fix touches `noxu-tree/src/tree.rs`
//! (`collect_bins_with_known_deleted` / the delete path), which is under a
//! REG-CLEANER change freeze — escalated in the report, not patched here.

use noxu_db::{
    DatabaseConfig, DatabaseEntry, EnvironmentConfig, Get, OperationStatus,
    StatsConfig,
};
use tempfile::TempDir;

const NODE_MAX: u32 = 4;

/// Env with all daemons off and a small fanout, matching EmptyBINTest/
/// INCompressorTest (`NODE_MAX=4`, `ENV_RUN_IN_COMPRESSOR=false`).
fn open_env(
    dir: &std::path::Path,
    transactional: bool,
) -> noxu_db::Environment {
    let mut cfg = EnvironmentConfig::new(dir.to_path_buf())
        .with_allow_create(true)
        .with_transactional(transactional);
    cfg.set_run_cleaner(false);
    cfg.set_run_checkpointer(false);
    cfg.set_run_evictor(false);
    cfg.set_run_in_compressor(false);
    cfg.set_node_max_entries(NODE_MAX);
    noxu_db::Environment::open(cfg).unwrap()
}

fn open_db(
    env: &noxu_db::Environment,
    transactional: bool,
) -> noxu_db::Database {
    env.open_database(
        None,
        "testDB",
        &DatabaseConfig::new()
            .with_allow_create(true)
            .with_transactional(transactional),
    )
    .unwrap()
}

fn bin_count(db: &noxu_db::Database) -> u64 {
    db.stats(Some(&StatsConfig::new().with_fast(false)))
        .unwrap()
        .btree
        .bottom_internal_node_count
}

// ===========================================================================
// EmptyBINTest — scan/search correctness in the face of an empty BIN.
//
// JE builds the tree 0..10 (single-byte keys, NODE_MAX=4 → four BINs), empties
// the middle BIN by deleting 5,6,7, then scans/searches across the gap.  The
// six @Test methods vary the scan direction and start point.  In Noxu the
// emptied BIN is left in place (see the deviation note), so this exercises the
// "scan/search must stay correct with an empty BIN present" invariant directly.
// ===========================================================================

/// Build the classic EmptyBINTest tree: keys 0..=10, then delete 5,6,7 so a
/// middle BIN is emptied.  Returns the open env+db (daemons off).
fn build_empty_middle_bin(
    dir: &TempDir,
) -> (noxu_db::Environment, noxu_db::Database) {
    // Compressor ON is what JE uses (ENV_RUN_INCOMPRESSOR=true) so the daemon
    // may race the scan; leaving it off is stricter (the empty BIN is
    // guaranteed present).  We leave it off so the scenario is deterministic.
    let env = open_env(dir.path(), false);
    let db = open_db(&env, false);
    for i in 0u8..11 {
        db.put(
            DatabaseEntry::from_bytes(&[i]),
            DatabaseEntry::from_bytes(&[100]),
        )
        .unwrap();
    }
    for i in 5u8..=7 {
        assert!(db.delete([i]).unwrap(), "delete of key {i} must succeed");
    }
    (env, db)
}

/// Drive a scan and collect the visited key bytes.
fn scan(db: &noxu_db::Database, forward: bool, start: Option<u8>) -> Vec<u8> {
    let mut c = db.open_cursor(None).unwrap();
    let mut k = DatabaseEntry::new();
    let mut v = DatabaseEntry::new();
    let mut out = Vec::new();

    let mut status = match start {
        None => {
            if forward {
                c.get(&mut k, &mut v, Get::First, None).unwrap()
            } else {
                c.get(&mut k, &mut v, Get::Last, None).unwrap()
            }
        }
        Some(s) => {
            k = DatabaseEntry::from_bytes(&[s]);
            // A start key that lands in the deleted range uses a range query
            // (getSearchKeyRange); an exact key uses set.  EmptyBINTest uses
            // getSearchKeyRange for the deleted-range starts.
            if (5..=7).contains(&s) {
                c.get(&mut k, &mut v, Get::SearchGte, None).unwrap()
            } else {
                c.get(&mut k, &mut v, Get::Search, None).unwrap()
            }
        }
    };
    while status == OperationStatus::Success {
        out.push(k.data_opt().unwrap()[0]);
        status = if forward {
            c.get(&mut k, &mut v, Get::Next, None).unwrap()
        } else {
            c.get(&mut k, &mut v, Get::Prev, None).unwrap()
        };
    }
    c.close().unwrap();
    out
}

// JE: EmptyBINTest.testScanFromEndOfFirstBin
// Fwd scan starting at 4 (exact). Expect 4,8,9,10 — must skip the empty BIN.
#[test]
fn empty_bin_scan_from_end_of_first_bin() {
    let dir = TempDir::new().unwrap();
    let (env, db) = build_empty_middle_bin(&dir);
    assert_eq!(scan(&db, true, Some(4)), vec![4, 8, 9, 10]);
    db.close().unwrap();
    env.close().unwrap();
}

// JE: EmptyBINTest.testScanFromLeftSideOfEmptyBin
// Fwd scan starting at 5 (deleted → range). Expect 8,9,10.
#[test]
fn empty_bin_scan_from_left_side_of_empty_bin() {
    let dir = TempDir::new().unwrap();
    let (env, db) = build_empty_middle_bin(&dir);
    assert_eq!(scan(&db, true, Some(5)), vec![8, 9, 10]);
    db.close().unwrap();
    env.close().unwrap();
}

// JE: EmptyBINTest.testScanFromRightSideOfEmptyBin
// Backwards scan starting at 7 (deleted → range). Expect 8,4,3,2,1,0.
#[test]
fn empty_bin_scan_from_right_side_of_empty_bin() {
    let dir = TempDir::new().unwrap();
    let (env, db) = build_empty_middle_bin(&dir);
    // A backwards scan from a deleted key positions at the first key >= 7
    // (getSearchKeyRange lands on 8) and walks backwards: 8,4,3,2,1,0.
    assert_eq!(scan(&db, false, Some(7)), vec![8, 4, 3, 2, 1, 0]);
    db.close().unwrap();
    env.close().unwrap();
}

// JE: EmptyBINTest.testScanFromBeginningOfLastBin
// Backwards scan starting at 8 (exact). Expect 8,4,3,2,1,0.
#[test]
fn empty_bin_scan_from_beginning_of_last_bin() {
    let dir = TempDir::new().unwrap();
    let (env, db) = build_empty_middle_bin(&dir);
    assert_eq!(scan(&db, false, Some(8)), vec![8, 4, 3, 2, 1, 0]);
    db.close().unwrap();
    env.close().unwrap();
}

// JE: EmptyBINTest.testScanForward
// Fwd scan from first. Expect 0,1,2,3,4,8,9,10 (skips empty middle BIN).
#[test]
fn empty_bin_scan_forward_from_first() {
    let dir = TempDir::new().unwrap();
    let (env, db) = build_empty_middle_bin(&dir);
    assert_eq!(scan(&db, true, None), vec![0, 1, 2, 3, 4, 8, 9, 10]);
    db.close().unwrap();
    env.close().unwrap();
}

// JE: EmptyBINTest.testScanBackwards
// Bwd scan from last. Expect 10,9,8,4,3,2,1,0.
#[test]
fn empty_bin_scan_backwards_from_last() {
    let dir = TempDir::new().unwrap();
    let (env, db) = build_empty_middle_bin(&dir);
    assert_eq!(scan(&db, false, None), vec![10, 9, 8, 4, 3, 2, 1, 0]);
    db.close().unwrap();
    env.close().unwrap();
}

// ===========================================================================
// INCompressorTest — portable record-level behaviors via the public API.
//
// The JE slot-count timing (`checkBinEntriesAndCursors`) and queue-size
// (`checkINCompQueueSize`) assertions are N/A (see the module deviation note);
// the record-presence / tree-consistency behaviors they protect are asserted
// here.
// ===========================================================================

/// INCompressorTest `openAndInit`: two committed keys 0,1 in the first BIN,
/// plus enough keys to force a split so there are >= 2 BINs (JE: "we need at
/// least 2 BINs, otherwise empty BINs won't be deleted").
fn open_and_init(
    dir: &TempDir,
    transactional: bool,
) -> (noxu_db::Environment, noxu_db::Database) {
    let env = open_env(dir.path(), transactional);
    let db = open_db(&env, transactional);
    // Fill past one BIN so the tree splits: keys 0..8 (NODE_MAX=4 → >=2 BINs).
    for i in 0u8..8 {
        db.put(
            DatabaseEntry::from_bytes(&[i]),
            DatabaseEntry::from_bytes(&[0]),
        )
        .unwrap();
    }
    assert!(
        bin_count(&db) >= 2,
        "openAndInit must produce >=2 BINs, got {}",
        bin_count(&db)
    );
    (env, db)
}

// JE: INCompressorTest.testDeleteTransactional
// A transactional delete removes the record; a cursor/txn held open does not
// let the record reappear; after commit the record is gone.  (JE asserts the
// slot-count timing; Noxu asserts the record is absent and siblings survive.)
#[test]
fn delete_transactional_removes_record() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_and_init(&dir, true);

    let txn = env.begin_transaction(None).unwrap();
    let mut c = db.open_cursor_in(&txn, None).unwrap();
    let mut k = DatabaseEntry::new();
    let mut v = DatabaseEntry::new();
    assert_eq!(
        c.get(&mut k, &mut v, Get::First, None).unwrap(),
        OperationStatus::Success
    );
    let deleted_key = k.data_opt().unwrap().to_vec();
    assert_eq!(c.delete().unwrap(), OperationStatus::Success);
    // The deleting txn holds the write lock on the slot until commit; a
    // concurrent reader would block (lock-based isolation).  The cursor is now
    // parked at the deleted gap (JE PendingDeleted).
    c.close().unwrap();
    txn.commit().unwrap();
    env.compress().unwrap();

    // Committed: the record is gone; every other key survives.
    assert!(
        db.get(&deleted_key).unwrap().is_none(),
        "committed-deleted key must be absent"
    );
    let mut survivors = 0;
    for i in 0u8..8 {
        if db.get([i]).unwrap().is_some() {
            survivors += 1;
        }
    }
    assert_eq!(survivors, 7, "exactly one key was deleted");
    db.close().unwrap();
    env.close().unwrap();
}

// JE: INCompressorTest.testDeleteNonTransactional
// Same as the transactional case at the record level, auto-commit path.
#[test]
fn delete_non_transactional_removes_record() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_and_init(&dir, false);

    let mut c = db.open_cursor(None).unwrap();
    let mut k = DatabaseEntry::new();
    let mut v = DatabaseEntry::new();
    assert_eq!(
        c.get(&mut k, &mut v, Get::First, None).unwrap(),
        OperationStatus::Success
    );
    let deleted_key = k.data_opt().unwrap().to_vec();
    assert_eq!(c.delete().unwrap(), OperationStatus::Success);
    c.close().unwrap();
    env.compress().unwrap();

    assert!(db.get(&deleted_key).unwrap().is_none());
    assert_eq!((0u8..8).filter(|&i| db.get([i]).unwrap().is_some()).count(), 7);
    db.close().unwrap();
    env.close().unwrap();
}

// JE: INCompressorTest.testDeleteDuplicate
// Delete of one duplicate datum removes only that (key,data) pair; the other
// dups for the key survive.  (Noxu sorted dups share the BIN — no DBIN.)
#[test]
fn delete_duplicate_removes_only_that_datum() {
    let dir = TempDir::new().unwrap();
    let env = open_env(dir.path(), false);
    let db = env
        .open_database(
            None,
            "dups",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_sorted_duplicates(true),
        )
        .unwrap();
    // key 0 has three dups {0,1,2}; keys 1..8 make the tree split.
    for d in 0u8..3 {
        db.put(
            DatabaseEntry::from_bytes(&[0]),
            DatabaseEntry::from_bytes(&[d]),
        )
        .unwrap();
    }
    for i in 1u8..8 {
        db.put(
            DatabaseEntry::from_bytes(&[i]),
            DatabaseEntry::from_bytes(&[0]),
        )
        .unwrap();
    }

    // Delete the first dup of key 0 via a cursor positioned on it.
    let mut c = db.open_cursor(None).unwrap();
    let mut k = DatabaseEntry::from_bytes(&[0]);
    let mut v = DatabaseEntry::from_bytes(&[0]);
    assert_eq!(
        c.get(&mut k, &mut v, Get::SearchBoth, None).unwrap(),
        OperationStatus::Success
    );
    assert_eq!(c.delete().unwrap(), OperationStatus::Success);
    c.close().unwrap();
    env.compress().unwrap();

    // Two dups for key 0 survive: {1,2}.  The deleted pair (0,0) is gone.
    let mut c = db.open_cursor(None).unwrap();
    let mut k = DatabaseEntry::from_bytes(&[0]);
    let mut v = DatabaseEntry::new();
    let mut dups = Vec::new();
    let mut s = c.get(&mut k, &mut v, Get::Search, None).unwrap();
    while s == OperationStatus::Success && k.data_opt().unwrap() == [0] {
        dups.push(v.data_opt().unwrap()[0]);
        s = c.get(&mut k, &mut v, Get::NextDup, None).unwrap();
    }
    c.close().unwrap();
    assert_eq!(dups, vec![1, 2], "only the (0,0) datum must be deleted");
    db.close().unwrap();
    env.close().unwrap();
}

// JE: INCompressorTest.testRemoveEmptyBIN
// Delete BOTH keys in the first BIN.  In JE the empty BIN (and its parent
// slot) is removed by compress().  In Noxu the records are removed but the
// empty BIN is NOT pruned (engine gap — see the ignored test below); this test
// asserts the PORTABLE half: both records are gone and every sibling survives.
#[test]
fn remove_empty_bin_records_gone_and_siblings_survive() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_and_init(&dir, false);

    // Delete the two lexicographically-smallest keys (the first BIN's keys).
    assert!(db.delete([0]).unwrap());
    assert!(db.delete([1]).unwrap());
    env.compress().unwrap();

    assert!(db.get([0]).unwrap().is_none());
    assert!(db.get([1]).unwrap().is_none());
    // Keys 2..8 all survive and remain findable across the (now-sparser) tree.
    for i in 2u8..8 {
        assert!(db.get([i]).unwrap().is_some(), "sibling key {i} must survive");
    }
    // A full forward scan must still visit exactly the survivors, in order.
    assert_eq!(scan(&db, true, None), vec![2, 3, 4, 5, 6, 7]);
    db.close().unwrap();
    env.close().unwrap();
}

// JE: INCompressorTest.testAbortInsert
// Insert a new key in a txn, then abort.  The record must be absent afterward
// (undo removes it); every pre-existing key survives.
#[test]
fn abort_insert_leaves_record_absent() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_and_init(&dir, true);

    let txn = env.begin_transaction(None).unwrap();
    // key 200 is a brand-new key not present in 0..8.
    db.put_in(
        &txn,
        DatabaseEntry::from_bytes(&[200]),
        DatabaseEntry::from_bytes(&[0]),
    )
    .unwrap();
    assert!(
        db.get_in(&txn, DatabaseEntry::from_bytes(&[200])).unwrap().is_some(),
        "inserted key visible inside its txn before abort"
    );
    txn.abort().unwrap();
    env.compress().unwrap();

    assert!(
        db.get([200]).unwrap().is_none(),
        "aborted-insert key must be absent"
    );
    for i in 0u8..8 {
        assert!(
            db.get([i]).unwrap().is_some(),
            "pre-existing key {i} survives"
        );
    }
    db.close().unwrap();
    env.close().unwrap();
}

// JE: INCompressorTest.testRollBackInsert
// Insert in a txn, checkpoint (preserve internal nodes), abort, then reopen to
// run recovery.  The rolled-back record must be absent after recovery; the
// pre-existing keys survive.  (JE asserts the slot-count / compress timing;
// Noxu asserts the recovered record state.)
#[test]
fn rollback_insert_absent_after_recovery() {
    let dir = TempDir::new().unwrap();
    {
        let (env, db) = open_and_init(&dir, true);
        let txn = env.begin_transaction(None).unwrap();
        db.put_in(
            &txn,
            DatabaseEntry::from_bytes(&[200]),
            DatabaseEntry::from_bytes(&[0]),
        )
        .unwrap();
        env.checkpoint(Some(
            &noxu_db::CheckpointConfig::new().with_force(true),
        ))
        .unwrap();
        txn.abort().unwrap();
        db.close().unwrap();
        env.close().unwrap();
    }
    // Reopen → recovery replays the log and undoes the aborted insert.
    let env = open_env(dir.path(), true);
    let db = open_db(&env, true);
    assert!(
        db.get([200]).unwrap().is_none(),
        "rolled-back insert must be absent after recovery"
    );
    for i in 0u8..8 {
        assert!(
            db.get([i]).unwrap().is_some(),
            "committed key {i} must survive recovery"
        );
    }
    db.close().unwrap();
    env.close().unwrap();
}

// JE: INCompressorTest.testRollForwardDelete
// Delete a record (non-txn), checkpoint, reopen to run recovery.  The deleted
// record must stay absent after recovery (the delete is rolled forward); the
// other keys survive.
#[test]
fn rollforward_delete_absent_after_recovery() {
    let dir = TempDir::new().unwrap();
    {
        let (env, db) = open_and_init(&dir, false);
        env.checkpoint(Some(
            &noxu_db::CheckpointConfig::new().with_force(true),
        ))
        .unwrap();
        assert!(db.delete([0]).unwrap());
        db.close().unwrap();
        env.close().unwrap();
    }
    let env = open_env(dir.path(), false);
    let db = open_db(&env, false);
    assert!(
        db.get([0]).unwrap().is_none(),
        "deleted key must stay absent after recovery (roll-forward delete)"
    );
    for i in 1u8..8 {
        assert!(db.get([i]).unwrap().is_some(), "key {i} must survive");
    }
    db.close().unwrap();
    env.close().unwrap();
}

// JE: INCompressorTest.testNodeNotEmpty
// Delete both keys in a BIN (emptying it), then RE-INSERT a key into it before
// pruning.  In JE the re-insert makes pruning hit NodeNotEmptyException and the
// prune is refused (the BIN is live again).  In Noxu the observable outcome is:
// after the re-insert the key is present and findable, and a subsequent
// compress does not remove it.  This is the "re-insert cancels compression"
// behavior at the record level.
#[test]
fn node_not_empty_reinsert_after_delete_keeps_key_live() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_and_init(&dir, false);

    // Empty the first BIN.
    assert!(db.delete([0]).unwrap());
    assert!(db.delete([1]).unwrap());
    assert!(db.get([0]).unwrap().is_none());

    // Re-insert key 0 — the to-be-reclaimed region is live again.
    db.put(DatabaseEntry::from_bytes(&[0]), DatabaseEntry::from_bytes(&[0]))
        .unwrap();
    assert!(db.get([0]).unwrap().is_some(), "re-inserted key must be present");

    // A compress pass must NOT drop the re-inserted live record.
    env.compress().unwrap();
    assert!(
        db.get([0]).unwrap().is_some(),
        "re-inserted key must survive compress (NodeNotEmpty: prune refused)"
    );
    // Full scan: 0 present, 1 gone, 2..8 present.
    assert_eq!(scan(&db, true, None), vec![0, 2, 3, 4, 5, 6, 7]);
    db.close().unwrap();
    env.close().unwrap();
}

// JE: INCompressorTest.testEmptyInitialBINScan
// Delete every key in the FIRST BIN (leaving an empty edge BIN), then position
// a cursor at the first record.  The scan must skip the empty edge BIN and land
// on the first surviving key.  (JE's key value there is 64; Noxu's first
// survivor after emptying keys 0,1 of an 0..8 tree is key 2.)
#[test]
fn empty_initial_bin_scan_positions_on_first_survivor() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_and_init(&dir, false);

    // Empty the first BIN.
    assert!(db.delete([0]).unwrap());
    assert!(db.delete([1]).unwrap());

    // getFirst must skip the empty edge BIN and land on key 2.
    let mut c = db.open_cursor(None).unwrap();
    let mut k = DatabaseEntry::new();
    let mut v = DatabaseEntry::new();
    assert_eq!(
        c.get(&mut k, &mut v, Get::First, None).unwrap(),
        OperationStatus::Success
    );
    assert_eq!(
        k.data_opt().unwrap(),
        [2],
        "getFirst must skip the empty edge BIN and land on the first survivor"
    );
    c.close().unwrap();
    db.close().unwrap();
    env.close().unwrap();
}

// JE: INCompressorTest.testLazyPruning
// After deleting all keys in the first BIN and letting lazy compression run,
// compress() (in JE the daemon) prunes the empty BIN so the parent IN shrinks
// to one entry.  In Noxu the records are gone and the tree stays consistent,
// but the empty BIN is NOT pruned (engine gap — see the ignored test).  This
// port asserts the portable half: after compress the surviving keys form a
// contiguous, correctly-ordered scan and no deleted key resurfaces.
#[test]
fn lazy_pruning_records_gone_tree_consistent() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_and_init(&dir, false);

    assert!(db.delete([0]).unwrap());
    assert!(db.delete([1]).unwrap());
    // Lazy/explicit compression pass.
    env.compress().unwrap();

    assert!(db.get([0]).unwrap().is_none());
    assert!(db.get([1]).unwrap().is_none());
    assert_eq!(scan(&db, true, None), vec![2, 3, 4, 5, 6, 7]);
    // Idempotent: a second compress changes nothing observable.
    env.compress().unwrap();
    assert_eq!(scan(&db, true, None), vec![2, 3, 4, 5, 6, 7]);
    db.close().unwrap();
    env.close().unwrap();
}

// ===========================================================================
// ENGINE BUG CANDIDATE (escalated; ignored so CI stays green).
//
// JE: INCompressorTest.testRemoveEmptyBIN / testLazyPruning core invariant —
// "a BIN emptied by committed deletes is compressed away; the parent IN entry
// for it is removed" (INCompressor.pruneBIN → Tree.delete(idKey)).
//
// CONTROL (the assertion that PASSES, proving the setup is real): after
// deleting all keys in the first BIN, `bin_count` is UNCHANGED — the empty BIN
// is still attached.  If Noxu pruned it, `bin_count` would drop; it does not,
// through EVERY reclamation path (env.compress, checkpoint, sync, daemon —
// verified by probe).
//
// ROOT CAUSE: Noxu physically removes a slot on the committed delete
// (`Tree::delete`), so the emptied BIN has ZERO `known_deleted` slots.  Both
// the `env.compress()` sweep and the `noxu-in-compressor` daemon locate
// candidate BINs via `Tree::collect_bins_with_known_deleted`, which skips a
// BIN with no `known_deleted` slots — so the empty BIN is never handed to
// `prune_empty_bin`.  `maybe_compress_bin_and_parent` (the opportunistic
// prune) is dead in production (only called from unit tests) and no write/
// delete path invokes it.  Result: emptied BINs accumulate and are never
// reclaimed.  Correctness holds (records gone, tree queryable, scans correct);
// this is a SPACE-RECLAMATION / COMPACTION gap, not data loss or corruption.
//
// FIX (production, NOT applied here): make the collector (or the delete path)
// also surface empty BINs to `prune_empty_bin`, e.g. extend
// `collect_bins_with_known_deleted` to also collect `entries.is_empty()` BINs,
// or have `apply_tree_delete` call `maybe_compress_bin_and_parent` on the BIN
// that just went empty.  Both touch `noxu-tree/src/tree.rs`, which is under a
// REG-CLEANER change freeze — escalated in tp-je-incomp.md, not patched here.
//
// When the fix lands: replace the two `bin_count unchanged` assertions with
// `bin_count` DROPPING by one (the pruned first BIN) and remove `#[ignore]`.
// ===========================================================================
// NEW-INCOMP-EMPTY-BIN (FIXED): the empty BIN emptied by committed deletes
// is now pruned from its parent on the compress/checkpoint path.
// `collect_bins_with_known_deleted` also surfaces `entries.is_empty()` BINs
// and `compress_bin_with_lock_check` routes an already-empty BIN through
// `prune_empty_bin_by_id` (noxu-tree/src/tree.rs).  `bin_count` now drops by
// exactly one after reclamation.
#[test]
fn empty_bin_left_by_committed_deletes_is_never_pruned() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_and_init(&dir, false);
    let pre = bin_count(&db);
    assert!(pre >= 2, "setup: need >=2 BINs, got {pre}");

    // Empty the first BIN entirely (its two keys 0,1).
    assert!(db.delete([0]).unwrap());
    assert!(db.delete([1]).unwrap());

    // CONTROL: records are gone (delete worked) — the setup is real.
    assert!(db.get([0]).unwrap().is_none());
    assert!(db.get([1]).unwrap().is_none());

    // Every reclamation path Noxu offers.
    env.compress().unwrap();
    env.checkpoint(Some(&noxu_db::CheckpointConfig::new().with_force(true)))
        .unwrap();
    db.sync().unwrap();
    env.compress().unwrap();

    // JE EXPECTATION (now satisfied after NEW-INCOMP-EMPTY-BIN): the empty
    // BIN was pruned, so the BIN count dropped by exactly one.
    assert_eq!(
        bin_count(&db),
        pre - 1,
        "JE: an emptied BIN must be pruned from its parent (bin_count should \
         drop by 1) so the index space is reclaimed."
    );

    // STRUCTURAL INTEGRITY: pruning must not leave a dangling parent slot,
    // orphan a sibling, or corrupt the tree.  env.verify() must report zero
    // structural errors after the prune.
    let vresult = env.verify(&noxu_db::VerifyConfig::new()).unwrap();
    assert_eq!(
        vresult.error_count(),
        0,
        "env.verify() must report 0 structural errors after pruning the \
         empty BIN, got: {vresult}"
    );

    // SCAN CORRECTNESS: the surviving keys 2..8 must still be readable and
    // returned in order by a full forward scan across the pruned gap.
    assert_eq!(scan(&db, true, None), vec![2, 3, 4, 5, 6, 7]);
    for i in 2u8..8 {
        assert!(db.get([i]).unwrap().is_some(), "survivor {i} must remain");
    }
    assert!(db.get([0]).unwrap().is_none());
    assert!(db.get([1]).unwrap().is_none());

    // IDEMPOTENT: a second reclamation pass changes nothing observable.
    env.compress().unwrap();
    assert_eq!(bin_count(&db), pre - 1);
    assert_eq!(scan(&db, true, None), vec![2, 3, 4, 5, 6, 7]);

    db.close().unwrap();
    env.close().unwrap();
}
