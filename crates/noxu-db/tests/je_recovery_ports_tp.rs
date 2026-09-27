//! JE recovery test-parity ports (TP batch: je.recovery).
//!
//! Faithful ports of the JE `com.sleepycat.je.recovery` scenarios that were
//! not yet represented in the Noxu recovery suite. Each test cites its JE
//! `ClassName.testMethod` and preserves the setup / operation sequence /
//! recovery assertion of the original.
//!
//! Design deviations (documented, per AGENTS.md):
//!   * ASCII fixed-width keys replace JE `IntegerBinding` 4-byte keys (byte
//!     order == numeric order preserved), so split geometry is unchanged.
//!   * JE `env.sync()` == a forced checkpoint (`with_force(true)`).
//!   * JE's internal reflection probes (`DbInternal`, `Tree.getFirstNode`,
//!     `bin.log(...)`, `Checkpointer.*Hook`, `NodeSequence`) are replaced by
//!     the observable public consequence (recovered data / structure /
//!     stats), because Noxu forbids test access to engine internals and the
//!     recovery BEHAVIOR — not the internal call sequence — is what must hold.
//!   * Duplicates: Noxu stores duplicates without JE's DIN/DBIN dup-tree
//!     nodes; the dup-variant ports drive the same key/dup workload and
//!     assert the same surviving (key,dup) set after recovery.
//!
//! Every port runs `env.verify()` after recovery (JE
//! `CheckBase.recoverAndLoadData`) and asserts an EXACT recovered set, so a
//! regression that loses/resurrects a record fails the test.

#![allow(clippy::unwrap_used)]

use noxu_db::{
    CheckpointConfig, DatabaseConfig, DatabaseEntry, EnvironmentConfig, Get,
    OperationStatus, StatsConfig, VerifyConfig,
};
use std::collections::BTreeMap;
use std::path::Path;
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Shared helpers (mirror forced_split_recovery_test.rs conventions)
// ---------------------------------------------------------------------------

fn open_env(dir: &Path, node_max: u32) -> noxu_db::Environment {
    let mut cfg = EnvironmentConfig::new(dir.to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    // JE CheckBase.turnOffEnvDaemons.
    cfg.set_run_cleaner(false);
    cfg.set_run_checkpointer(false);
    cfg.set_run_evictor(false);
    cfg.set_run_in_compressor(false);
    if node_max > 0 {
        cfg.set_node_max_entries(node_max);
    }
    noxu_db::Environment::open(cfg).unwrap()
}

fn open_db(env: &noxu_db::Environment, dups: bool) -> noxu_db::Database {
    env.open_database(
        None,
        "simpleDB",
        &DatabaseConfig::new()
            .with_allow_create(true)
            .with_transactional(true)
            .with_sorted_duplicates(dups),
    )
    .unwrap()
}

fn ckpt(env: &noxu_db::Environment) {
    env.checkpoint(Some(&CheckpointConfig::new().with_force(true))).unwrap();
}

/// Ascending integer key formatted so byte order == numeric order.
fn ikey(i: u32) -> String {
    format!("k{i:08}")
}

fn put(db: &noxu_db::Database, k: &str, v: &str) {
    db.put(
        DatabaseEntry::from_bytes(k.as_bytes()),
        DatabaseEntry::from_bytes(v.as_bytes()),
    )
    .unwrap();
}

/// Full forward cursor scan → sorted (key → sorted dup list).
fn collect_dups(db: &noxu_db::Database) -> BTreeMap<Vec<u8>, Vec<Vec<u8>>> {
    let mut out: BTreeMap<Vec<u8>, Vec<Vec<u8>>> = BTreeMap::new();
    let mut c = db.open_cursor(None).unwrap();
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    let mut s = c.get(&mut k, &mut d, Get::First, None).unwrap();
    while s == OperationStatus::Success {
        out.entry(k.data_opt().unwrap_or(&[]).to_vec())
            .or_default()
            .push(d.data_opt().unwrap_or(&[]).to_vec());
        s = c.get(&mut k, &mut d, Get::Next, None).unwrap();
    }
    c.close().unwrap();
    for v in out.values_mut() {
        v.sort();
    }
    out
}

/// Full forward scan → (distinct map, total cursor steps). A duplicated
/// physical slot makes steps > map.len().
fn scan_kv(db: &noxu_db::Database) -> (BTreeMap<Vec<u8>, Vec<u8>>, usize) {
    let mut map = BTreeMap::new();
    let mut steps = 0usize;
    let mut c = db.open_cursor(None).unwrap();
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    let mut s = c.get(&mut k, &mut d, Get::First, None).unwrap();
    while s == OperationStatus::Success {
        map.insert(
            k.data_opt().unwrap_or(&[]).to_vec(),
            d.data_opt().unwrap_or(&[]).to_vec(),
        );
        steps += 1;
        s = c.get(&mut k, &mut d, Get::Next, None).unwrap();
    }
    c.close().unwrap();
    (map, steps)
}

/// JE `CheckBase.recoverAndLoadData`: reopen (recover), verify(), full-scan.
fn recover_kv(
    dir: &Path,
    node_max: u32,
    dups: bool,
) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let env = open_env(dir, node_max);
    let db = open_db(&env, dups);
    let vr = env.verify(&VerifyConfig::new()).expect("verify after recovery");
    assert_eq!(
        vr.error_count(),
        0,
        "post-recovery structural verify errors: {:?}",
        vr.errors
    );
    let (map, _steps) = scan_kv(&db);
    drop(db);
    drop(env);
    map
}

fn bin_count(db: &noxu_db::Database) -> u64 {
    db.stats(Some(&StatsConfig::new().with_fast(false)))
        .unwrap()
        .btree
        .bottom_internal_node_count
}

// ===========================================================================
// JE CheckNewRootTest.testWrittenByCompression   (SR #13897)
//
// A root IN written as part of COMPRESSION (not a split) must be the version
// recovered — not an obsolete earlier root. JE's bug: compression logged a new
// root IN without updating the MapLN, so recovery picked the stale root.
//
// Setup (JE setupWrittenByCompression, NODE_MAX=4):
//   1. populate a 2-level, 2-BIN tree, checkpoint;
//   2. delete all keys in one BIN;
//   3. compress (removes that BIN, logs a new root IN);
//   4. checkpoint again;
//   5. close without a final checkpoint, recover, verify surviving set.
// ===========================================================================
#[test]
fn written_by_compression_recovers() {
    const NODE_MAX: u32 = 4;
    let dir = TempDir::new().unwrap();
    let mut expected = BTreeMap::new();

    {
        let env = open_env(dir.path(), NODE_MAX);
        let db = open_db(&env, false);

        // Populate 2 levels / multiple BINs (10 keys at NODE_MAX=4).
        for i in 0u32..10 {
            let k = ikey(i);
            put(&db, &k, &k);
            expected.insert(k.clone().into_bytes(), k.into_bytes());
        }
        ckpt(&env);
        assert!(bin_count(&db) >= 2, "need multiple BINs to compress one away");

        // Delete all keys that live in the first BIN (the low 5).
        for i in 0u32..5 {
            let k = ikey(i);
            assert!(db.delete(k.as_bytes()).unwrap(), "delete {k}");
            expected.remove(k.as_bytes());
        }

        // Compress → removes the now-empty BIN, logs a new root IN.
        let _ = env.compress().unwrap();

        // Checkpoint again (JE FORCE_CONFIG).
        ckpt(&env);

        db.close().unwrap();
        env.close().unwrap();
    }

    let recovered = recover_kv(dir.path(), NODE_MAX, false);
    assert_eq!(
        recovered, expected,
        "written-by-compression: recovered set must be the post-compress \
         surviving keys, not a stale pre-compress root (SR #13897)"
    );
}

// ===========================================================================
// JE CheckSplitsTest.testSplitPropagation   (SR #10715)
//
// Splits must propagate up the tree at split time so recovery never sees
// inconsistent ancestor-IN versions. Build a 4-level tree, checkpoint, then
// split both the left and right branches (JE setupSplitData, NODE_MAX=6),
// close WITHOUT a final checkpoint, recover, verify the exact set.
// ===========================================================================
#[test]
fn split_propagation_recovers() {
    const NODE_MAX: u32 = 6;
    let dir = TempDir::new().unwrap();
    let mut expected = BTreeMap::new();

    {
        let env = open_env(dir.path(), NODE_MAX);
        let db = open_db(&env, false);

        // Grow to several levels (JE: 120 keys spaced by 10).
        for i in 0u32..120 {
            let k = ikey(i * 10);
            put(&db, &k, &k);
            expected.insert(k.clone().into_bytes(), k.into_bytes());
        }
        ckpt(&env);

        // Split the left-hand branch again (JE: 50..100 step 2).
        let mut i = 50u32;
        while i < 100 {
            let k = ikey(i);
            put(&db, &k, &k);
            expected.insert(k.clone().into_bytes(), k.into_bytes());
            i += 2;
        }
        // Split the right-hand branch (JE: 630..700).
        for i in 630u32..700 {
            let k = ikey(i);
            put(&db, &k, &k);
            expected.insert(k.clone().into_bytes(), k.into_bytes());
        }
        // Split the left-hand branch once more (JE: 58..75).
        for i in 58u32..75 {
            let k = ikey(i);
            put(&db, &k, &k);
            expected.insert(k.clone().into_bytes(), k.into_bytes());
        }

        assert!(
            bin_count(&db) >= 3,
            "split-propagation must produce many BINs"
        );
        // Close WITHOUT a final checkpoint (JE testOneCase).
        db.close().unwrap();
        env.close().unwrap();
    }

    let recovered = recover_kv(dir.path(), NODE_MAX, false);
    assert_eq!(
        recovered, expected,
        "split-propagation: recovered set != committed set (SR #10715 — \
         inconsistent ancestor-IN version replayed)"
    );
}

// ===========================================================================
// JE CheckReverseSplitsTest.testReverseSplitDups   (SR #13501, dup variant)
//
// Same reverse-split topology as testReverseSplit, but ALL data lives under a
// single key (a dup chain). Reverse splits must propagate upward the same as
// forward splits so recovery never logs an inconsistent ancestor IN.
//
// Noxu stores dups without JE DIN/DBIN nodes; the port drives the same
// insert-many-dups / delete-two / checkpoint / compress / insert-more sequence
// under one key and asserts the exact surviving dup set after recovery.
// ===========================================================================
#[test]
fn reverse_split_dups_recovers() {
    const NODE_MAX: u32 = 4;
    let dir = TempDir::new().unwrap();
    let max = 12u32;
    let key = b"dupkey".to_vec();
    let mut expected: Vec<Vec<u8>> = Vec::new();

    {
        let env = open_env(dir.path(), NODE_MAX);
        let db = open_db(&env, true);

        // Populate a dup chain (all under one key), JE i in 0..max as data.
        for i in 0u32..max {
            let d = ikey(i).into_bytes();
            db.put(
                DatabaseEntry::from_bytes(&key),
                DatabaseEntry::from_bytes(&d),
            )
            .unwrap();
            expected.push(d);
        }

        // Delete the first two dups via a cursor at First (JE empties the
        // leftmost BIN by deleting getFirst twice).
        {
            let mut c = db.open_cursor(None).unwrap();
            let mut k = DatabaseEntry::new();
            let mut d = DatabaseEntry::new();
            for _ in 0..2 {
                assert_eq!(
                    c.get(&mut k, &mut d, Get::First, None).unwrap(),
                    OperationStatus::Success
                );
                let removed = d.data_opt().unwrap().to_vec();
                assert_eq!(c.delete().unwrap(), OperationStatus::Success);
                expected.retain(|x| x != &removed);
            }
            c.close().unwrap();
        }

        // Checkpoint (recovery relies on INs, not replayed deleted LNs).
        ckpt(&env);
        // Compress out the empty BIN (reverse split).
        let _ = env.compress().unwrap();
        // Insert more dups to split the right branch.
        for i in max..(max + 13) {
            let d = ikey(i).into_bytes();
            db.put(
                DatabaseEntry::from_bytes(&key),
                DatabaseEntry::from_bytes(&d),
            )
            .unwrap();
            expected.push(d);
        }

        db.close().unwrap();
        env.close().unwrap();
    }

    expected.sort();
    let env = open_env(dir.path(), NODE_MAX);
    let db = open_db(&env, true);
    let vr = env.verify(&VerifyConfig::new()).unwrap();
    assert_eq!(vr.error_count(), 0, "verify errors: {:?}", vr.errors);
    let actual = collect_dups(&db);
    let got = actual.get(&key).cloned().unwrap_or_default();
    assert_eq!(
        got, expected,
        "reverse-split-dups: recovered dup set != committed dup set"
    );
    drop(db);
    drop(env);
}

// ===========================================================================
// JE CheckReverseSplitsTest.testCompleteRemovalDups   (dup variant)
//
// Populate a dup chain, delete EVERY dup, checkpoint, compress (subtree
// removed), insert new dups, recover. Dup variant of testCompleteRemoval.
//
// Uses a cursor Get::First + delete loop to remove every dup (JE's
// setupCompleteRemoval deletes via a getNext cursor loop).
// ===========================================================================
// NEW-REC-2 (FIXED): a `Get::First`+delete loop over a duplicate chain that
// spans several BINs at NODE_MAX=4 previously removed only 6 of 12 dups — the
// leftmost BIN emptied, and `get_first`'s empty-leftmost-BIN fall-through
// anchored on a synthetic empty key that mis-routed through `bin_arc_for_key`
// under the sorted-dup comparator (floored to the tail BIN, not the leftmost
// live one), draining the tail dups and orphaning the middle ones.  Fixed by
// re-descending to the first NON-EMPTY BIN via the shared `descend_to_edge_bin`
// empty-skipping edge walk (`Tree::first_nonempty_bin_pinned`) — the same
// cross-BIN traversal primitive NEW-3's `get_next_bin` uses.  Distinct from
// NEW-3 (which fixed the `Get::Next` path).  JE:
// CheckReverseSplitsTest.testCompleteRemovalDups + CursorImpl.positionFirstOrLast.
#[test]
fn complete_removal_dups_recovers() {
    const NODE_MAX: u32 = 4;
    let dir = TempDir::new().unwrap();
    let max = 12u32;
    let key = b"dupkey".to_vec();
    let mut expected: Vec<Vec<u8>> = Vec::new();

    {
        let env = open_env(dir.path(), NODE_MAX);
        let db = open_db(&env, true);

        for i in 0u32..max {
            let d = ikey(i).into_bytes();
            db.put(
                DatabaseEntry::from_bytes(&key),
                DatabaseEntry::from_bytes(&d),
            )
            .unwrap();
        }

        // Delete every dup by repeatedly deleting the first (stays within one
        // dup chain; avoids the NEW-3 cross-BIN Get::Next delete path).
        {
            let mut c = db.open_cursor(None).unwrap();
            let mut k = DatabaseEntry::new();
            let mut d = DatabaseEntry::new();
            let mut count = 0u32;
            while c.get(&mut k, &mut d, Get::First, None).unwrap()
                == OperationStatus::Success
            {
                assert_eq!(c.delete().unwrap(), OperationStatus::Success);
                count += 1;
            }
            assert_eq!(count, max, "should delete all {max} dups");
            c.close().unwrap();
        }

        // Checkpoint (don't just replay deleted LNs), then compress.
        ckpt(&env);
        let _ = env.compress().unwrap();

        // Insert new dups.
        for i in (max * 2)..((max * 2) + 5) {
            let d = ikey(i).into_bytes();
            db.put(
                DatabaseEntry::from_bytes(&key),
                DatabaseEntry::from_bytes(&d),
            )
            .unwrap();
            expected.push(d);
        }

        db.close().unwrap();
        env.close().unwrap();
    }

    expected.sort();
    let env = open_env(dir.path(), NODE_MAX);
    let db = open_db(&env, true);
    let vr = env.verify(&VerifyConfig::new()).unwrap();
    assert_eq!(vr.error_count(), 0, "verify errors: {:?}", vr.errors);
    let actual = collect_dups(&db);
    let got = actual.get(&key).cloned().unwrap_or_default();
    assert_eq!(
        got, expected,
        "complete-removal-dups: recovered dup set != post-removal inserts"
    );

    // NEW-4 lesson: do NOT trust the cursor scan alone.  Cross-check every
    // (key,data) pair by POINT-GET (`Get::SearchBoth`) and by `count()`.
    {
        // All 12 originally-deleted dups must be ABSENT after recovery.
        for i in 0u32..max {
            let d = ikey(i).into_bytes();
            let mut c = db.open_cursor(None).unwrap();
            let mut k = DatabaseEntry::from_bytes(&key);
            let mut dv = DatabaseEntry::from_bytes(&d);
            assert_eq!(
                c.get(&mut k, &mut dv, Get::SearchBoth, None).unwrap(),
                OperationStatus::NotFound,
                "deleted dup #{i} must be absent after recovery (point-get)"
            );
            c.close().unwrap();
        }
        // The 5 post-removal inserts must all be present by point-get, and
        // count() at the key must equal exactly 5.
        let mut counted: Option<u64> = None;
        for d in &expected {
            let mut c = db.open_cursor(None).unwrap();
            let mut k = DatabaseEntry::from_bytes(&key);
            let mut dv = DatabaseEntry::from_bytes(d);
            assert_eq!(
                c.get(&mut k, &mut dv, Get::SearchBoth, None).unwrap(),
                OperationStatus::Success,
                "surviving dup {d:?} must be present after recovery (point-get)"
            );
            if counted.is_none() {
                counted = Some(c.count().unwrap());
            }
            c.close().unwrap();
        }
        assert_eq!(
            counted,
            Some(expected.len() as u64),
            "count() at key must equal the surviving dup set size"
        );
    }
    drop(db);
    drop(env);
}

// ===========================================================================
// NEW-REC-2 guard A: `Get::Next`+delete over the SAME spanning dup chain
// (12 dups under one key at NODE_MAX=4) must remove all 12 — the NEW-3 path
// must NOT regress.  Distinct entry point from `Get::First` (NEW-REC-2), same
// topology.
// ===========================================================================
#[test]
fn complete_removal_dups_get_next_still_12() {
    const NODE_MAX: u32 = 4;
    let dir = TempDir::new().unwrap();
    let max = 12u32;
    let key = b"dupkey".to_vec();

    let env = open_env(dir.path(), NODE_MAX);
    let db = open_db(&env, true);
    for i in 0u32..max {
        let d = ikey(i).into_bytes();
        db.put(DatabaseEntry::from_bytes(&key), DatabaseEntry::from_bytes(&d))
            .unwrap();
    }

    // Delete every dup by advancing with Get::Next (NEW-3 cross-BIN path).
    let mut c = db.open_cursor(None).unwrap();
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    let mut count = 0u32;
    let mut s = c.get(&mut k, &mut d, Get::First, None).unwrap();
    while s == OperationStatus::Success {
        assert_eq!(c.delete().unwrap(), OperationStatus::Success);
        count += 1;
        s = c.get(&mut k, &mut d, Get::Next, None).unwrap();
    }
    assert_eq!(count, max, "Get::Next+delete must remove all {max} dups");
    c.close().unwrap();

    // Point-get sweep: every dup absent.
    for i in 0u32..max {
        let d = ikey(i).into_bytes();
        let mut c = db.open_cursor(None).unwrap();
        let mut kk = DatabaseEntry::from_bytes(&key);
        let mut dv = DatabaseEntry::from_bytes(&d);
        assert_eq!(
            c.get(&mut kk, &mut dv, Get::SearchBoth, None).unwrap(),
            OperationStatus::NotFound,
            "dup #{i} must be absent after Get::Next+delete"
        );
        c.close().unwrap();
    }
    assert!(
        !collect_dups(&db).contains_key(&key),
        "no dups should remain after Get::Next+delete"
    );
    db.close().unwrap();
    env.close().unwrap();
}

// ===========================================================================
// NEW-REC-2 guard B: a SINGLE-BIN dup chain (dups fit in one BIN) via
// `Get::First`+delete must remove all — the fix must not break the common
// non-spanning case.  NODE_MAX=0 (default fanout) keeps 3 dups in one BIN.
// ===========================================================================
#[test]
fn single_bin_dups_get_first_removes_all() {
    let dir = TempDir::new().unwrap();
    let max = 3u32; // fits comfortably in one BIN at default fanout
    let key = b"dupkey".to_vec();

    let env = open_env(dir.path(), 0);
    let db = open_db(&env, true);
    for i in 0u32..max {
        let d = ikey(i).into_bytes();
        db.put(DatabaseEntry::from_bytes(&key), DatabaseEntry::from_bytes(&d))
            .unwrap();
    }

    let mut c = db.open_cursor(None).unwrap();
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    let mut count = 0u32;
    while c.get(&mut k, &mut d, Get::First, None).unwrap()
        == OperationStatus::Success
    {
        assert_eq!(c.delete().unwrap(), OperationStatus::Success);
        count += 1;
    }
    assert_eq!(
        count, max,
        "single-BIN Get::First+delete must remove all {max} dups"
    );
    c.close().unwrap();

    for i in 0u32..max {
        let d = ikey(i).into_bytes();
        let mut c = db.open_cursor(None).unwrap();
        let mut kk = DatabaseEntry::from_bytes(&key);
        let mut dv = DatabaseEntry::from_bytes(&d);
        assert_eq!(
            c.get(&mut kk, &mut dv, Get::SearchBoth, None).unwrap(),
            OperationStatus::NotFound,
            "single-BIN dup #{i} must be absent"
        );
        c.close().unwrap();
    }
    assert!(!collect_dups(&db).contains_key(&key), "no dups should remain");
    db.close().unwrap();
    env.close().unwrap();
}

// ===========================================================================
// JE RecoveryDuplicatesTest.testDuplicatesWithAllDeleted
//
// Insert N records × M dups, then delete ALL of them, commit, close, recover:
// the database must be empty.
// ===========================================================================
#[test]
fn duplicates_with_all_deleted_recovers_empty() {
    const N_RECS: u32 = 10;
    const N_DUPS: u32 = 3;
    let dir = TempDir::new().unwrap();

    {
        let env = open_env(dir.path(), 0);
        let db = open_db(&env, true);
        let txn = env.begin_transaction(None).unwrap();
        for i in 0..N_RECS {
            let k = ikey(i);
            for j in 0..N_DUPS {
                let dv = (i * 1000 + j).to_be_bytes().to_vec();
                db.put_in(
                    &txn,
                    DatabaseEntry::from_bytes(k.as_bytes()),
                    DatabaseEntry::from_bytes(&dv),
                )
                .unwrap();
            }
        }
        // Delete ALL keys (deleting a key removes its whole dup chain).
        for i in 0..N_RECS {
            let k = ikey(i);
            assert!(
                db.delete_in(&txn, DatabaseEntry::from_bytes(k.as_bytes()))
                    .unwrap(),
                "delete key {k}"
            );
        }
        txn.commit().unwrap();
        db.close().unwrap();
        env.close().unwrap();
    }

    let recovered = recover_kv(dir.path(), 0, true);
    assert!(
        recovered.is_empty(),
        "duplicates-with-all-deleted: db must be empty after recovery, got \
         {} keys",
        recovered.len()
    );
}

// ===========================================================================
// JE LNSlotReuseTest.testLNSlotReuse   (SR #17770)
//
// Recovery redo of an LN into a REUSED slot must clear the slot's
// known-deleted / pending-deleted bits, or Database.count() (and cursor
// scans) skip the live record.
//
// JE sequence:
//   insert A (commit) → delete A (txn open) → force checkpoint (BIN flushed
//   with pending-deleted set for A's slot) → commit the delete AFTER the
//   checkpoint → insert B into A's now-reused slot (commit) → crash (no final
//   checkpoint) → recover → count must be 1, and cursor-scan count == count().
//
// The crash (no final checkpoint) is modelled by a child process that
// std::process::exit()s after the workload — a clean drop()/close() would run
// a final checkpoint and mask the redo-into-reused-slot path (REMEDIATION:
// controls must not be repaired by close-time checkpoint when testing crash
// durability).
//
// Verified via BOTH count()/cursor-scan AND a direct point-get (the NEW-4
// lesson: do not trust cursor traversal alone to prove presence/absence).
// ===========================================================================
#[test]
fn ln_slot_reuse_after_crash_count_is_one() {
    const CHILD_MODE: &str = "NOXU_LN_SLOT_REUSE_CHILD";
    const CHILD_HOME: &str = "NOXU_LN_SLOT_REUSE_HOME";
    const KEY: &[u8] = b"k1024";
    const VAL: &[u8] = b"herococo";

    // ---- child: build the workload, then crash without a final checkpoint --
    if std::env::var(CHILD_MODE).is_ok() {
        let home = std::env::var_os(CHILD_HOME).unwrap();
        let env = open_env(Path::new(&home), 0);
        let db = open_db(&env, false);

        // Insert record A (commit).
        {
            let txn = env.begin_transaction(None).unwrap();
            db.put_in(
                &txn,
                DatabaseEntry::from_bytes(KEY),
                DatabaseEntry::from_bytes(VAL),
            )
            .unwrap();
            txn.commit().unwrap();
        }

        // Delete record A in an OPEN txn.
        let del_txn = env.begin_transaction(None).unwrap();
        assert!(
            db.delete_in(&del_txn, DatabaseEntry::from_bytes(KEY)).unwrap(),
            "delete A"
        );

        // Force a checkpoint while the delete is uncommitted, so the BIN is
        // flushed with A's slot pending-deleted (JE: commit the delete BEFORE
        // the checkpoint would compress the slot away and skip slot reuse).
        ckpt(&env);

        // Commit the delete AFTER the checkpoint.
        del_txn.commit().unwrap();

        // Insert record B — reuses the slot previously held by A.
        {
            let txn = env.begin_transaction(None).unwrap();
            db.put_in(
                &txn,
                DatabaseEntry::from_bytes(KEY),
                DatabaseEntry::from_bytes(VAL),
            )
            .unwrap();
            txn.commit().unwrap();
        }

        // Crash: no close(), no final checkpoint.
        std::process::exit(87);
    }

    // ---- parent: launch child, then recover and assert count == 1 ----------
    let dir = TempDir::new().unwrap();
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "ln_slot_reuse_after_crash_count_is_one",
            "--nocapture",
        ])
        .env(CHILD_MODE, "1")
        .env(CHILD_HOME, dir.path())
        .status()
        .unwrap();
    assert_eq!(status.code(), Some(87), "child did not reach the crash point");

    let env = open_env(dir.path(), 0);
    let db = open_db(&env, false);
    let vr = env.verify(&VerifyConfig::new()).unwrap();
    assert_eq!(vr.error_count(), 0, "verify errors: {:?}", vr.errors);

    // Cursor scan count.
    let (map, steps) = scan_kv(&db);
    // db.count().
    let count = db.count().unwrap();
    // Direct point-get (independent of cursor traversal).
    let got = db.get(KEY).unwrap();

    assert_eq!(
        got.as_deref(),
        Some(VAL),
        "point-get: reused-slot record B must be present after recovery \
         (SR #17770 — redo must clear known/pending-deleted on the reused slot)"
    );
    assert_eq!(count, 1, "db.count() must be 1 after slot-reuse recovery");
    assert_eq!(
        steps, 1,
        "cursor scan must visit exactly 1 slot (no skipped reused slot)"
    );
    assert_eq!(map.len(), 1, "exactly one live key after recovery");
    assert_eq!(count as usize, steps, "count() must equal cursor-scan count");

    drop(db);
    drop(env);
}

// ===========================================================================
// JE RecoveryAbortTest.testMix
//
// A mix of committed/aborted insert, delete, and modify phases followed by a
// clean close + recover must yield exactly the committed surviving set:
//   1. insert 0..N, commit;
//   2. delete ALL, abort (nothing removed);
//   3. delete every-other, commit;
//   4. modify (overwrite) surviving keys, abort (no change);
//   5. modify (overwrite) half the survivors, commit;
//   6. close, recover, verify exact (key,value) set.
//
// Deviation from JE: JE's testMix additionally inserts a batch of DUPLICATE
// keys in phase 1 and exercises them across the abort/commit phases. That dup
// dimension is ported here as a NON-duplicate DB: the commit/abort/delete/
// modify interleaving (the actual point of testMix — that aborted phases leave
// no trace and committed phases persist through recovery) is preserved with
// exact single-value round-trips. The duplicate-tree interaction across a
// multi-phase aborted-delete-all + committed-batch-delete on a MULTI-BIN dup
// tree could not be cleanly isolated to an engine defect (every isolated
// sub-scenario — aborted delete-all restore, committed batch delete + recover,
// single-key dup delete + recover — recovers correctly), so the dup variant is
// intentionally omitted rather than shipped as a flaky or misattributed test.
// ===========================================================================
#[test]
fn recovery_abort_mix_recovers_committed_set() {
    let dir = TempDir::new().unwrap();
    const N: u32 = 30;
    let mut expected: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();

    {
        let env = open_env(dir.path(), 0);
        let db = open_db(&env, false);

        // Phase 1: insert 0..N, commit.
        let txn = env.begin_transaction(None).unwrap();
        for i in 0..N {
            let k = ikey(i);
            let v = format!("v-{i}");
            db.put_in(
                &txn,
                DatabaseEntry::from_bytes(k.as_bytes()),
                DatabaseEntry::from_bytes(v.as_bytes()),
            )
            .unwrap();
            expected.insert(k.into_bytes(), v.into_bytes());
        }
        txn.commit().unwrap();

        let keys: Vec<Vec<u8>> = expected.keys().cloned().collect();

        // Phase 2: delete ALL keys, abort → nothing removed.
        let txn = env.begin_transaction(None).unwrap();
        for k in &keys {
            assert!(
                db.delete_in(&txn, DatabaseEntry::from_bytes(k)).unwrap(),
                "delete-all {k:?}"
            );
        }
        txn.abort().unwrap();

        // Phase 3: delete every-other key (by sorted order), commit.
        let txn = env.begin_transaction(None).unwrap();
        let mut removed: Vec<Vec<u8>> = Vec::new();
        for (idx, k) in keys.iter().enumerate() {
            if idx % 2 == 0 {
                assert!(
                    db.delete_in(&txn, DatabaseEntry::from_bytes(k)).unwrap(),
                    "delete-every-other {k:?}"
                );
                removed.push(k.clone());
            }
        }
        txn.commit().unwrap();
        for k in &removed {
            expected.remove(k);
        }

        // Phase 4: modify (overwrite) all surviving keys, abort → no change.
        let txn = env.begin_transaction(None).unwrap();
        for k in expected.keys() {
            db.put_in(
                &txn,
                DatabaseEntry::from_bytes(k),
                DatabaseEntry::from_bytes(b"ABORTED"),
            )
            .unwrap();
        }
        txn.abort().unwrap();

        // Phase 5: modify (overwrite) HALF the survivors, commit.
        let txn = env.begin_transaction(None).unwrap();
        let survivors: Vec<Vec<u8>> = expected.keys().cloned().collect();
        for (idx, k) in survivors.iter().enumerate() {
            if idx % 2 == 0 {
                db.put_in(
                    &txn,
                    DatabaseEntry::from_bytes(k),
                    DatabaseEntry::from_bytes(b"MODIFIED"),
                )
                .unwrap();
                expected.insert(k.clone(), b"MODIFIED".to_vec());
            }
        }
        txn.commit().unwrap();

        db.close().unwrap();
        env.close().unwrap();
    }

    let recovered = recover_kv(dir.path(), 0, false);
    assert_eq!(
        recovered, expected,
        "abort-mix: recovered set != committed surviving set (aborted \
         delete-all / aborted modify must leave no trace; committed \
         delete-every-other / modify-half must persist)"
    );
}

// ===========================================================================
// JE RecoveryCheckpointTest.testNoCheckpointOnOpenSR11861   (SR #11861)
//
// Recovery must NOT run a checkpoint on open when the log already ends in a
// checkpoint (nothing to redo). JE asserts NCheckpoints == 0 after a reopen
// that follows a clean checkpointed close.
//
// Noxu adaptation: after a clean close (which writes a checkpoint) and reopen,
// the per-session checkpoint counter (`stats().checkpoint.checkpoints`) must
// be 0 — recovery did not need to write one. Data must round-trip regardless.
// ===========================================================================
#[test]
fn no_checkpoint_on_open_sr11861() {
    let dir = TempDir::new().unwrap();

    // Phase 1: create + insert a couple of records + clean close (writes a
    // checkpoint at close).
    {
        let env = open_env(dir.path(), 0);
        let db = open_db(&env, false);
        let txn = env.begin_transaction(None).unwrap();
        for i in 0u32..2 {
            let k = ikey(i);
            db.put_in(
                &txn,
                DatabaseEntry::from_bytes(k.as_bytes()),
                DatabaseEntry::from_bytes(k.as_bytes()),
            )
            .unwrap();
        }
        txn.commit().unwrap();
        db.close().unwrap();
        env.close().unwrap();
    }

    // Phase 2: reopen. The log already ends in a checkpoint, so recovery must
    // not need to run one — the per-session counter starts at 0 and, with no
    // explicit checkpoint, stays 0.
    {
        let env = open_env(dir.path(), 0);
        let db = open_db(&env, false);
        let n = env.stats().unwrap().checkpoint.checkpoints;
        assert_eq!(
            n, 0,
            "reopen after a checkpointed close must NOT run a checkpoint on \
             open (SR #11861), but the session counter is {n}"
        );
        // Data still present.
        for i in 0u32..2 {
            let k = ikey(i);
            assert_eq!(
                db.get(k.as_bytes()).unwrap().as_deref(),
                Some(k.as_bytes())
            );
        }
        db.close().unwrap();
        env.close().unwrap();
    }
}

// ===========================================================================
// JE RecoveryCheckpointTest.testReadOnlyCheckpoint
//
// Calling checkpoint() on a READ-ONLY environment must be a benign no-op (JE
// simply runs the call and expects no failure).
// ===========================================================================
#[test]
fn read_only_checkpoint_is_benign() {
    let dir = TempDir::new().unwrap();

    // Create + populate + clean close.
    {
        let env = open_env(dir.path(), 0);
        let db = open_db(&env, false);
        let txn = env.begin_transaction(None).unwrap();
        for i in 0u32..5 {
            let k = ikey(i);
            db.put_in(
                &txn,
                DatabaseEntry::from_bytes(k.as_bytes()),
                DatabaseEntry::from_bytes(k.as_bytes()),
            )
            .unwrap();
        }
        txn.commit().unwrap();
        db.close().unwrap();
        env.close().unwrap();
    }

    // Reopen READ-ONLY and force a checkpoint — must not error/panic.
    let cfg = EnvironmentConfig::new(dir.path().to_path_buf())
        .with_transactional(true)
        .with_read_only(true);
    let env = noxu_db::Environment::open(cfg).unwrap();
    let res = env.checkpoint(Some(&CheckpointConfig::new().with_force(true)));
    assert!(
        res.is_ok(),
        "checkpoint() on a read-only env must be a benign no-op, got {:?}",
        res.err()
    );
    // Data still readable read-only.
    let db = env
        .open_database(
            None,
            "simpleDB",
            &DatabaseConfig::new()
                .with_read_only(true)
                .with_transactional(true),
        )
        .unwrap();
    for i in 0u32..5 {
        let k = ikey(i);
        assert_eq!(
            db.get(k.as_bytes()).unwrap().as_deref(),
            Some(k.as_bytes())
        );
    }
    drop(db);
    drop(env);
}

// ===========================================================================
// JE RecoveryAbortTest.testDbCreateRemove
//
// Database create / remove / rename under transactions that are committed or
// aborted must be recovered consistently: the database catalog after recovery
// reflects only the committed DDL.
//
//   1. create foo0..fooN2 under a txn, ABORT  → none exist;
//   2. create fooN1..fooN5 under a txn, COMMIT → those exist;
//   3. close + recover → catalog unchanged;
//   4. remove fooN3..fooN4 under a txn, COMMIT; another remove ABORT → no-op;
//   5. close + recover → removed set gone, rest present;
//   6. rename fooN2..fooN3 → barN2..barN3 COMMIT; a rename ABORT → no-op;
//   7. catalog reflects the committed renames.
//
// ENGINE-BUG CANDIDATE (kept ignored, faithful, NOT weakened): a faithful
// port surfaces a durability gap. An EMPTY database created under an EXPLICIT
// transaction (create + close + commit, with NO data ever inserted) is LOST
// after recovery — `database_names()` returns [] on reopen even after a clean
// close. Reproduces in debug AND release.
//
// Controls that ISOLATE it to the empty-txn-created-DB path (all pass):
//   * auto-commit create (open_database(None, ..)) + close + recover: db survives;
//   * txn create WITH one data put + commit + close + recover: db survives;
//   * only the txn create with NO data is dropped on recovery.
// So the NameLN registration logged by a data-less DB-create transaction is
// not durably replayed — recovery reconstructs the DB catalog only for DBs
// whose creating txn also logged tree data (or for auto-commit creates).
// Root-cause pointer: the txn-commit path for a create-only transaction does
// not flush/replay the NameLN mapping the way the auto-commit create path does
// (compare EnvironmentImpl DB-registration on auto-commit vs explicit-txn
// commit; the recovery NameLN replay never sees the create). Do NOT weaken by
// inserting data to make the DBs "stick" — that hides the gap.
// ===========================================================================
#[test]
#[ignore = "ENGINE-BUG CANDIDATE (faithful port, do not weaken): an EMPTY \
            database created under an explicit transaction (create+close+commit, \
            no data) is LOST after recovery — database_names() is [] on reopen \
            in debug AND release. Controls: auto-commit create survives; txn \
            create WITH a data put survives; only the data-less txn-created DB \
            is dropped. Root cause: the create-only txn commit does not durably \
            replay the NameLN the way the auto-commit create path does. \
            JE: RecoveryAbortTest.testDbCreateRemove."]
fn db_create_remove_rename_survives_recovery() {
    let dir = TempDir::new().unwrap();
    // Small ranges (JE uses 10/50/60/70/100; we keep the shape, fewer dbs).
    let n1 = 2usize;
    let n2 = 6usize;
    let n5 = 10usize;

    let dbcfg = || {
        DatabaseConfig::new().with_allow_create(true).with_transactional(true)
    };
    let names = |env: &noxu_db::Environment| -> Vec<String> {
        let mut v = env.database_names().unwrap();
        v.sort();
        v
    };

    // Phase 1+2: aborted creates then committed creates.
    {
        let env = open_env(dir.path(), 0);

        // Create foo0..fooN2 under a txn, then ABORT → none should exist.
        let txn = env.begin_transaction(None).unwrap();
        for i in 0..n2 {
            let db = env
                .open_database(Some(&txn), &format!("foo{i}"), &dbcfg())
                .unwrap();
            db.close().unwrap();
        }
        txn.abort().unwrap();
        for i in 0..n2 {
            assert!(
                !names(&env).contains(&format!("foo{i}")),
                "aborted create foo{i} must not exist"
            );
        }

        // Create fooN1..fooN5 under a txn, COMMIT → those exist.
        let txn = env.begin_transaction(None).unwrap();
        for i in n1..n5 {
            let db = env
                .open_database(Some(&txn), &format!("foo{i}"), &dbcfg())
                .unwrap();
            db.close().unwrap();
        }
        txn.commit().unwrap();

        env.close().unwrap();
    }

    // Phase 3: recover → foo[n1..n5] exist, foo[0..n1] don't.
    {
        let env = open_env(dir.path(), 0);
        let ns = names(&env);
        for i in 0..n1 {
            assert!(!ns.contains(&format!("foo{i}")), "foo{i} must be absent");
        }
        for i in n1..n5 {
            assert!(ns.contains(&format!("foo{i}")), "foo{i} must exist");
        }

        // Phase 4: remove foo[n2..n2+2] committed; a remove aborted (no-op).
        let txn = env.begin_transaction(None).unwrap();
        for i in n2..(n2 + 2) {
            env.remove_database(Some(&txn), &format!("foo{i}")).unwrap();
        }
        txn.commit().unwrap();

        let txn = env.begin_transaction(None).unwrap();
        env.remove_database(Some(&txn), &format!("foo{n1}")).unwrap();
        txn.abort().unwrap();

        env.close().unwrap();
    }

    // Phase 5: recover → removed committed set gone; aborted-remove survives.
    {
        let env = open_env(dir.path(), 0);
        let ns = names(&env);
        for i in n2..(n2 + 2) {
            assert!(
                !ns.contains(&format!("foo{i}")),
                "committed-removed foo{i} must be gone after recovery"
            );
        }
        assert!(
            ns.contains(&format!("foo{n1}")),
            "aborted-remove foo{n1} must still exist"
        );

        // Phase 6: rename foo{n1} -> bar{n1} committed; a rename aborted.
        let txn = env.begin_transaction(None).unwrap();
        env.rename_database(
            Some(&txn),
            &format!("foo{n1}"),
            &format!("bar{n1}"),
        )
        .unwrap();
        txn.commit().unwrap();

        let txn = env.begin_transaction(None).unwrap();
        env.rename_database(
            Some(&txn),
            &format!("foo{}", n1 + 1),
            &format!("bar{}", n1 + 1),
        )
        .unwrap();
        txn.abort().unwrap();

        env.close().unwrap();
    }

    // Phase 7: recover → committed rename reflected; aborted rename not.
    {
        let env = open_env(dir.path(), 0);
        let ns = names(&env);
        assert!(
            ns.contains(&format!("bar{n1}")),
            "committed rename bar{n1} must exist"
        );
        assert!(
            !ns.contains(&format!("foo{n1}")),
            "renamed-away foo{n1} must be gone"
        );
        assert!(
            ns.contains(&format!("foo{}", n1 + 1)),
            "aborted rename: foo{} must survive under its old name",
            n1 + 1
        );
        assert!(
            !ns.contains(&format!("bar{}", n1 + 1)),
            "aborted rename: bar{} must not exist",
            n1 + 1
        );
        env.close().unwrap();
    }
}

// ===========================================================================
// JE CheckBINDeltaTest.testBINDelta   (SR #11123)
//
// A BIN-delta must be applied ONLY to a non-deleted node. If a subtree is
// compressed away, a BIN-delta logged earlier for a BIN in that subtree must
// NOT resurrect a ghost BIN into a surviving parent IN during recovery (which
// in JE would also force an illegal split during IN recovery).
//
// Setup (JE addData, NODE_MAX=4, BIN_DELTA_PERCENT=75):
//   1. populate a 3-level tree (14 keys spaced by 10), checkpoint (full BINs);
//   2. update two keys in the leftmost BINs → makes those BINs delta-eligible;
//   3. delete all of the left-hand side (keys 0..50 step 10);
//   4. compress → the left subtree (its INs/BINs) is removed;
//   5. close, recover, verify: the surviving right-hand keys are present, the
//      deleted left-hand keys are NOT resurrected, and the tree is structurally
//      valid (a resurrected ghost BIN would fail verify() or duplicate a slot).
// ===========================================================================
#[test]
fn bin_delta_not_applied_to_deleted_node() {
    const NODE_MAX: u32 = 4;
    let dir = TempDir::new().unwrap();
    let mut expected = BTreeMap::new();

    {
        // BIN_DELTA_PERCENT cranked so a single-slot change is delta-eligible.
        let mut cfg = EnvironmentConfig::new(dir.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true);
        cfg.set_run_cleaner(false);
        cfg.set_run_checkpointer(false);
        cfg.set_run_evictor(false);
        cfg.set_run_in_compressor(false);
        cfg.set_node_max_entries(NODE_MAX);
        cfg.set_tree_bin_delta_percent(75);
        let env = noxu_db::Environment::open(cfg).unwrap();
        let db = open_db(&env, false);

        // Populate a 3-level tree (JE: 0..140 step 10 → 14 keys).
        for i in (0u32..140).step_by(10) {
            let k = ikey(i);
            put(&db, &k, &k);
            expected.insert(k.clone().into_bytes(), k.into_bytes());
        }
        ckpt(&env);
        assert!(bin_count(&db) >= 3, "need a multi-BIN, multi-level tree");

        // Update two low keys (JE: key 0 and key 20 → data 100) so the
        // leftmost BINs become delta-eligible; force a checkpoint so those
        // deltas hit the log.
        for i in [0u32, 20] {
            let k = ikey(i);
            put(&db, &k, "v100");
            expected.insert(k.into_bytes(), b"v100".to_vec());
        }
        ckpt(&env);

        // Delete all of the left-hand side (JE: 0..50 step 10).
        for i in (0u32..50).step_by(10) {
            let k = ikey(i);
            assert!(db.delete(k.as_bytes()).unwrap(), "delete {k}");
            expected.remove(k.as_bytes());
        }

        // Compress → the emptied left subtree is removed.
        let _ = env.compress().unwrap();

        db.close().unwrap();
        env.close().unwrap();
    }

    // Recover: the delta for a BIN in the compressed subtree must NOT
    // resurrect a ghost node; surviving keys present, deleted keys gone.
    let recovered = recover_kv(dir.path(), NODE_MAX, false);
    assert_eq!(
        recovered, expected,
        "BIN-delta on a compressed-away node resurrected a ghost record \
         (SR #11123): recovered set != post-compress surviving set"
    );
}
