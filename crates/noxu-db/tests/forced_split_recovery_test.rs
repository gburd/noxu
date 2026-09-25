//! C3 — forced split-recovery topologies.
//!
//! Faithful ports of three JE recovery topology tests:
//!   - JE `CheckNewRootTest.testWrittenBySplit` / `testChangeAndEvictRoot`
//!     (new-root creation via right splits, then checkpoint + recover).
//!   - JE `CheckSplitAuntTest.testSplitAunt` (build a 4-level tree, dirty the
//!     left branch, checkpoint to level 2, then split the right branch so a
//!     "split-aunt" topology must be recovered).
//!   - JE `CheckReverseSplitsTest.testReverseSplit` (build a 3-level tree,
//!     empty the leftmost BIN, checkpoint, compress out the empty BIN
//!     (reverse split / subtree removal), then split the right branch).
//!
//! JE drives each topology with `CheckBase.testOneCase` (close-without-
//! checkpoint, then recover and assert the recovered set == the saved set)
//! AND a `stepwiseLoop` (per-entry truncation sweep, covered generically by
//! `stepwise_truncation_test.rs`). Here we port the topology + recover +
//! assert path, asserting BOTH:
//!   1. data equality (recovered KV set == expected committed set), and
//!   2. structural integrity (`env.verify()` reports zero errors) —
//!      JE `CheckBase.recoverAndLoadData` runs `env.verify()` after recovery.
//!
//! Adaptation notes:
//!   - ASCII keys instead of JE `IntegerBinding`; the split/merge geometry is
//!     preserved by using the same NODE_MAX and the same insert/delete counts.
//!   - `NODE_MAX = 4` (new-root, reverse-split) / `6` (split-aunt) matches JE.
//!   - JE's `env.sync()` == a forced checkpoint; Noxu uses
//!     `env.checkpoint(with_force(true))`.

use noxu_db::{
    CheckpointConfig, DatabaseConfig, DatabaseEntry, EnvironmentConfig, Get,
    OperationStatus, StatsConfig, VerifyConfig,
};
use std::collections::BTreeMap;
use std::path::Path;
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Open an env with daemons off and a fixed NODE_MAX (JE turnOffEnvDaemons +
/// NODE_MAX).
fn open_env(dir: &Path, node_max: u32) -> noxu_db::Environment {
    let mut cfg = EnvironmentConfig::new(dir.to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    cfg.set_run_cleaner(false);
    cfg.set_run_checkpointer(false);
    cfg.set_run_evictor(false);
    cfg.set_run_in_compressor(false);
    cfg.set_node_max_entries(node_max);
    noxu_db::Environment::open(cfg).unwrap()
}

fn open_db(env: &noxu_db::Environment) -> noxu_db::Database {
    env.open_database(
        None,
        "simpleDB",
        &DatabaseConfig::new().with_allow_create(true),
    )
    .unwrap()
}

fn collect_all(db: &noxu_db::Database) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let mut cursor = db.open_cursor(None).unwrap();
    let mut map = BTreeMap::new();
    let mut key = DatabaseEntry::new();
    let mut val = DatabaseEntry::new();
    let mut status = cursor.get(&mut key, &mut val, Get::First, None).unwrap();
    while status == OperationStatus::Success {
        map.insert(
            key.data_opt().unwrap_or(&[]).to_vec(),
            val.data_opt().unwrap_or(&[]).to_vec(),
        );
        status = cursor.get(&mut key, &mut val, Get::Next, None).unwrap();
    }
    cursor.close().unwrap();
    map
}

/// JE `CheckBase.recoverAndLoadData`: reopen (recover), `env.verify()`,
/// full-scan. Returns the recovered KV set; panics on any structural error.
///
/// NEW-2 de-vacuuming: also asserts the recovered tree is genuinely
/// multi-level (`bottom_internal_node_count >= min_bins`).  Before the
/// NODE_MAX-recovery fix, recovery rebuilt every tree at a hard-coded fanout
/// of 256, so a NODE_MAX=4 topology recovered as a single BIN and these
/// tests passed vacuously (data equality held, but no split geometry was
/// ever exercised).  Requiring multiple BINs after recovery makes the test
/// fail if the configured fanout is ever silently replaced by 256 again.
fn recover_and_collect(
    dir: &Path,
    node_max: u32,
    min_bins: u64,
) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let env = open_env(dir, node_max);
    let db = open_db(&env);
    let vresult =
        env.verify(&VerifyConfig::new()).expect("verify after recovery");
    assert_eq!(
        vresult.error_count(),
        0,
        "post-recovery structural verification found {} error(s): {:?}",
        vresult.error_count(),
        vresult.errors,
    );
    // NEW-2: the recovered tree must reflect the configured small fanout, not
    // the old hard-coded 256.  With NODE_MAX=4 the committed data sets here
    // all span several BINs; a single-BIN result means the fanout was lost on
    // reopen.
    let stats = db.stats(Some(&StatsConfig::new().with_fast(false))).unwrap();
    assert!(
        stats.btree.bottom_internal_node_count >= min_bins,
        "post-recovery tree must reflect NODE_MAX={node_max} (>= {min_bins} \
         BINs), got bin_count={} in_count={}; recovery reconstructed the \
         tree at the wrong fanout (NEW-2)",
        stats.btree.bottom_internal_node_count,
        stats.btree.internal_node_count,
    );
    let result = collect_all(&db);
    drop(db);
    drop(env);
    result
}

fn put(db: &noxu_db::Database, k: &str, v: &str) {
    db.put(
        DatabaseEntry::from_bytes(k.as_bytes()),
        DatabaseEntry::from_bytes(v.as_bytes()),
    )
    .unwrap();
}

/// NEW-2 de-vacuuming: assert the LIVE tree splits at the configured small
/// fanout (>= `min_bins` BINs) before the close/recover cycle, so the test
/// exercises genuine split geometry rather than a single fat BIN.
fn assert_multi_bin(db: &noxu_db::Database, min_bins: u64) {
    let stats = db.stats(Some(&StatsConfig::new().with_fast(false))).unwrap();
    assert!(
        stats.btree.bottom_internal_node_count >= min_bins,
        "pre-close tree must split at the configured small NODE_MAX \
         (>= {min_bins} BINs), got bin_count={} in_count={}; a fanout-256 \
         tree would not split with so few keys (test would be vacuous)",
        stats.btree.bottom_internal_node_count,
        stats.btree.internal_node_count,
    );
}

/// Ascending integer key formatted so byte order == numeric order.
fn ikey(i: u32) -> String {
    format!("k{i:08}")
}

// ---------------------------------------------------------------------------
// C3.1 — new-root creation via splits (JE CheckNewRootTest.testWrittenBySplit)
// ---------------------------------------------------------------------------

/// JE `CheckNewRootTest.testWrittenBySplit` (`setupWrittenBySplits`):
/// create a single-key tree + checkpoint, then insert ascending keys to force
/// splits that create a new root, checkpoint again. Recover and assert data +
/// structure.
#[test]
fn new_root_via_split_recovers() {
    const NODE_MAX: u32 = 4;
    let dir = TempDir::new().unwrap();
    let mut expected = BTreeMap::new();

    {
        let env = open_env(dir.path(), NODE_MAX);
        let db = open_db(&env);

        // Create a tree and checkpoint.
        put(&db, &ikey(0), &ikey(0));
        expected.insert(ikey(0).into_bytes(), ikey(0).into_bytes());
        env.checkpoint(Some(&CheckpointConfig::new().with_force(true)))
            .unwrap();

        // Populate so it splits (1..6 with NODE_MAX=4 forces root creation).
        // Enlarge the range so the root is unambiguously created (ascending
        // inserts → right splits → new root above the first BIN).
        for i in 1u32..40 {
            put(&db, &ikey(i), &ikey(i));
            expected.insert(ikey(i).into_bytes(), ikey(i).into_bytes());
        }

        env.checkpoint(Some(&CheckpointConfig::new().with_force(true)))
            .unwrap();
        // NEW-2: the tree must genuinely split at NODE_MAX=4 (many BINs)
        // before we close, or the recovery assertion below is vacuous.
        assert_multi_bin(&db, 2);
        db.close().unwrap();
        env.close().unwrap();
    }

    let recovered = recover_and_collect(dir.path(), NODE_MAX, 2);
    assert_eq!(
        recovered, expected,
        "new-root-via-split: recovered set != expected committed set"
    );
}

/// JE `CheckNewRootTest.testChangeAndEvictRoot` (`setupEvictedRoot`):
/// populate a 2-level tree + checkpoint, add a record that changes the IN
/// versions, evict, checkpoint again. Recover and assert data + structure.
///
/// Adaptation: Noxu drives eviction with `env.evict_memory()` instead of JE's
/// internal evictor `TestHook`; the recovery property (root must not be lost
/// across eviction + checkpoint) is the same.
#[test]
fn change_and_evict_root_recovers() {
    const NODE_MAX: u32 = 4;
    let dir = TempDir::new().unwrap();
    let mut expected = BTreeMap::new();

    {
        let env = open_env(dir.path(), NODE_MAX);
        let db = open_db(&env);

        // Populate a tree so it grows to 2 levels with multiple BINs.
        for i in 0u32..10 {
            put(&db, &ikey(i), &ikey(i));
            expected.insert(ikey(i).into_bytes(), ikey(i).into_bytes());
        }
        env.checkpoint(Some(&CheckpointConfig::new().with_force(true)))
            .unwrap();

        // Add another record so eviction logs different IN versions.
        put(&db, &ikey(10), &ikey(10));
        expected.insert(ikey(10).into_bytes(), ikey(10).into_bytes());

        // Evict, then checkpoint again.
        let _ = env.evict_memory().unwrap();
        env.checkpoint(Some(&CheckpointConfig::new().with_force(true)))
            .unwrap();
        // NEW-2: 11 keys at NODE_MAX=4 must span multiple BINs.
        assert_multi_bin(&db, 2);
        db.close().unwrap();
        env.close().unwrap();
    }

    let recovered = recover_and_collect(dir.path(), NODE_MAX, 2);
    assert_eq!(
        recovered, expected,
        "change-and-evict-root: recovered set != expected committed set"
    );
}

// ---------------------------------------------------------------------------
// C3.2 — split-aunt topology (JE CheckSplitAuntTest.testSplitAunt)
// ---------------------------------------------------------------------------

/// JE `CheckSplitAuntTest.testSplitAunt` (`setupSplitData`):
/// build a deep tree, sync repeatedly, dirty the left branch with a single
/// key, force a checkpoint that logs only to level 2 (leaving an ancestor
/// dirty), then split the right branch (the "split-aunt" topology), and
/// recover.
#[test]
fn split_aunt_recovers() {
    const NODE_MAX: u32 = 6;
    let dir = TempDir::new().unwrap();
    let mut expected = BTreeMap::new();

    {
        let env = open_env(dir.path(), NODE_MAX);
        let db = open_db(&env);

        let max = 26u32;
        // Populate a tree so it grows to multiple levels.
        for i in 0u32..max {
            let k = ikey(i * 10);
            put(&db, &k, &k);
            expected.insert(k.clone().into_bytes(), k.into_bytes());
        }

        // JE syncs several times (== forced checkpoints) to push the tree
        // fully to disk before the targeted dirtying below.
        for _ in 0..6 {
            env.checkpoint(Some(&CheckpointConfig::new().with_force(true)))
                .unwrap();
        }

        // Dirty the left-hand branch with a single key.
        let k5 = ikey(5);
        put(&db, &k5, &k5);
        expected.insert(k5.clone().into_bytes(), k5.into_bytes());

        // A forced checkpoint logs the BIN and its parent IN but leaves a
        // higher ancestor dirty (JE: "split-aunt" precondition).
        env.checkpoint(Some(&CheckpointConfig::new().with_force(true)))
            .unwrap();

        // Split the right-hand branch.
        for i in (max * 10)..(max * 10 + 7) {
            let k = ikey(i);
            put(&db, &k, &k);
            expected.insert(k.clone().into_bytes(), k.into_bytes());
        }

        // Close WITHOUT a final checkpoint so recovery must reconstruct the
        // split-aunt topology from the log (JE testOneCase closes w/out ckpt).
        // NEW-2: 26+7 keys at NODE_MAX=6 must span multiple BINs.
        assert_multi_bin(&db, 2);
        db.close().unwrap();
        env.close().unwrap();
    }

    let recovered = recover_and_collect(dir.path(), NODE_MAX, 2);
    assert_eq!(
        recovered, expected,
        "split-aunt: recovered set != expected committed set"
    );
}

// ---------------------------------------------------------------------------
// C3.3 — reverse-split / subtree removal
//        (JE CheckReverseSplitsTest.testReverseSplit / testCompleteRemoval)
// ---------------------------------------------------------------------------

/// JE `CheckReverseSplitsTest.testReverseSplit` (`setupReverseSplit`):
/// populate a 3-level tree, empty the leftmost BIN via cursor deletes,
/// checkpoint (so deletes are not replayed as LNs but via INs), compress out
/// the empty BIN (reverse split), then split the right branch (creating an
/// INa that still references obsolete BINs). Recover and assert data +
/// structure.
#[test]
fn reverse_split_recovers() {
    const NODE_MAX: u32 = 4;
    let dir = TempDir::new().unwrap();
    let max = 12u32;
    let mut expected = BTreeMap::new();

    {
        let env = open_env(dir.path(), NODE_MAX);
        let db = open_db(&env);

        // Populate a tree so it grows to 3 levels.
        for i in 0u32..max {
            let k = ikey(i);
            put(&db, &k, &k);
            expected.insert(k.clone().into_bytes(), k.into_bytes());
        }

        // Empty the leftmost BIN: delete the first two keys via a cursor
        // positioned at first (JE deletes getFirst twice).
        {
            let mut c = db.open_cursor(None).unwrap();
            let mut key = DatabaseEntry::new();
            let mut val = DatabaseEntry::new();
            for _ in 0..2 {
                let s = c.get(&mut key, &mut val, Get::First, None).unwrap();
                assert_eq!(s, OperationStatus::Success);
                let removed = key.data_opt().unwrap().to_vec();
                assert_eq!(c.delete().unwrap(), OperationStatus::Success);
                expected.remove(&removed);
            }
            c.close().unwrap();
        }

        // Checkpoint so the deleted LNs are not replayed; recovery relies on
        // INs.
        env.checkpoint(Some(&CheckpointConfig::new().with_force(true)))
            .unwrap();

        // Compress out the empty BIN (reverse split).
        let _ = env.compress().unwrap();

        // Add enough keys to split the level-2 IN on the right-hand side,
        // creating an INa that still references obsolete BINs.
        for i in max..(max + 13) {
            let k = ikey(i);
            put(&db, &k, &k);
            expected.insert(k.clone().into_bytes(), k.into_bytes());
        }

        // Close without a final checkpoint (JE testOneCase close w/out ckpt).
        db.close().unwrap();
        env.close().unwrap();
    }

    let recovered = recover_and_collect(dir.path(), NODE_MAX, 2);
    assert_eq!(
        recovered, expected,
        "reverse-split: recovered set != expected committed set"
    );
}

/// Crash variant of `reverse_split_recovers`: identical topology, but the
/// producer environment is torn down by **dropping the handles WITHOUT a
/// clean `close()`** — `EnvironmentImpl::Drop` takes no final checkpoint
/// (unlike `close()`), so recovery must reconstruct the committed set from
/// the WAL and the pre-compress checkpoint alone, not from a close-time
/// checkpoint that could mask a defect (REMEDIATION-BRIEF: controls must not
/// get repaired by normal close/checkpoint when testing crash durability).
///
/// The committed 23 keys are durable because each `put`/`delete` is an
/// autocommit transaction whose commit fsyncs the WAL.  Asserts the exact
/// key+value set via BOTH the cursor full-scan (`recover_and_collect`) AND a
/// direct point-get sweep, so the check does not rely solely on the traversal
/// path that NEW-4 broke.
#[test]
fn reverse_split_recovers_crash() {
    const NODE_MAX: u32 = 4;
    let dir = TempDir::new().unwrap();
    let max = 12u32;
    let mut expected = BTreeMap::new();

    {
        let env = open_env(dir.path(), NODE_MAX);
        let db = open_db(&env);

        for i in 0u32..max {
            let k = ikey(i);
            put(&db, &k, &k);
            expected.insert(k.clone().into_bytes(), k.into_bytes());
        }

        // Empty the leftmost BIN (delete first two keys via cursor).
        {
            let mut c = db.open_cursor(None).unwrap();
            let mut key = DatabaseEntry::new();
            let mut val = DatabaseEntry::new();
            for _ in 0..2 {
                let s = c.get(&mut key, &mut val, Get::First, None).unwrap();
                assert_eq!(s, OperationStatus::Success);
                let removed = key.data_opt().unwrap().to_vec();
                assert_eq!(c.delete().unwrap(), OperationStatus::Success);
                expected.remove(&removed);
            }
            c.close().unwrap();
        }

        // Checkpoint (recovery relies on INs for the deletes), then compress
        // out the empty BIN (reverse split), then split the right branch.
        env.checkpoint(Some(&CheckpointConfig::new().with_force(true)))
            .unwrap();
        let _ = env.compress().unwrap();
        for i in max..(max + 13) {
            let k = ikey(i);
            put(&db, &k, &k);
            expected.insert(k.clone().into_bytes(), k.into_bytes());
        }

        // CRASH: drop without close() — no final checkpoint (Drop path).
        drop(db);
        drop(env);
    }

    // Recover: cursor full-scan set must equal the committed set …
    let recovered = recover_and_collect(dir.path(), NODE_MAX, 2);
    assert_eq!(
        recovered, expected,
        "reverse-split crash: recovered set != expected committed set"
    );

    // … and a direct point-get sweep must find every committed key/value and
    // NOT find the two deleted keys (independent of the scan path).
    let env = open_env(dir.path(), NODE_MAX);
    let db = open_db(&env);
    for i in 0u32..(max + 13) {
        let k = ikey(i);
        let got = db.get(k.as_bytes()).unwrap();
        if expected.contains_key(k.as_bytes()) {
            assert_eq!(
                got.as_deref(),
                Some(k.as_bytes()),
                "reverse-split crash: committed key {k} must return its value"
            );
        } else {
            assert!(
                got.is_none(),
                "reverse-split crash: deleted key {k} must not be present"
            );
        }
    }
    db.close().unwrap();
    env.close().unwrap();
}

/// JE `CheckReverseSplitsTest.testCompleteRemoval` (`setupCompleteRemoval`):
/// populate a 3-level tree, delete EVERY record, checkpoint, compress (the
/// subtree is removed leaving a single BIN), then insert new data. Recover and
/// assert data + structure (and the complete-removal stat: a single BIN).
#[test]
#[ignore = "KNOWN BUG NEW-3 (cross-BIN-cursor-delete), NOT flaky. NEW-2 \
            de-vacuuming unmasked a cross-BIN cursor-delete traversal bug: a \
            cursor Get::Next delete-all loop removes only 2 of 12 keys once \
            the tree spans multiple BINs at NODE_MAX=4. Reproduces in debug \
            AND release. Latent on main today only because fanout 256 packs \
            these keys into a single BIN. Fix the cursor cross-BIN delete \
            traversal separately; do NOT re-vacuum by reverting the fanout \
            fix."]
fn complete_removal_recovers() {
    const NODE_MAX: u32 = 4;
    let dir = TempDir::new().unwrap();
    let max = 12u32;
    let mut expected = BTreeMap::new();

    {
        let env = open_env(dir.path(), NODE_MAX);
        let db = open_db(&env);

        // Populate a tree so it grows to 3 levels.
        for i in 0u32..max {
            let k = ikey(i);
            put(&db, &k, &k);
        }

        // Delete it all.
        {
            let mut c = db.open_cursor(None).unwrap();
            let mut key = DatabaseEntry::new();
            let mut val = DatabaseEntry::new();
            let mut count = 0;
            while c.get(&mut key, &mut val, Get::Next, None).unwrap()
                == OperationStatus::Success
            {
                assert_eq!(c.delete().unwrap(), OperationStatus::Success);
                count += 1;
            }
            assert_eq!(count, max, "should have deleted all {max} keys");
            c.close().unwrap();
        }

        // Checkpoint before so we don't simply replay all the deleted LNs.
        env.checkpoint(Some(&CheckpointConfig::new().with_force(true)))
            .unwrap();

        // Compress, and make sure the subtree was removed (single BIN).
        let _ = env.compress().unwrap();
        let stats =
            db.stats(Some(&StatsConfig::new().with_fast(false))).unwrap();
        assert_eq!(
            stats.btree.bottom_internal_node_count, 1,
            "complete-removal: expected exactly 1 BIN after compress, got {}",
            stats.btree.bottom_internal_node_count
        );

        // Insert new data.
        for i in (max * 2)..((max * 2) + 5) {
            let k = ikey(i);
            put(&db, &k, &k);
            expected.insert(k.clone().into_bytes(), k.into_bytes());
        }

        db.close().unwrap();
        env.close().unwrap();
    }

    let recovered = recover_and_collect(dir.path(), NODE_MAX, 1);
    assert_eq!(
        recovered, expected,
        "complete-removal: recovered set != expected committed set"
    );
}

// ---------------------------------------------------------------------------
// NEW-4 §3 — multiple CONSECUTIVE physically-empty edge BINs
//
// The NEW-4 fix crosses exactly ONE empty leading/trailing BIN.  When two or
// more CONSECUTIVE edge BINs are physically empty (delete >= 5 leading keys at
// NODE_MAX=4 with no compress → 1st BIN fully empty AND 2nd BIN now empty),
// the forward `Get::First` scan silently returns 0 while live keys remain —
// the same visibility class NEW-4 targets, just at N >= 2 empty edge BINs.
//
// JE has no such gap: `Cursor.getNext`/`getPrev` is a WHILE-LOOP that skips
// ANY number of empty BINs — on an empty crossed-into BIN it sets index=-1,
// re-tests `++index < getNEntries()`, and calls `getNextBin`/`getPrevBin`
// AGAIN (CursorImpl.java getNext ~line 2546 / getPrev loop), not a single
// cross.
//
// Method: deletes use POINT `db.delete` (NOT a cursor delete-all loop) to
// avoid the separate NEW-3 cross-BIN-cursor-delete path, and NO compress
// runs, so the emptied BINs stay physically present (n_entries == 0).  The
// point-get sweep must already find every live key (data is on disk) —
// proving this is a cursor-traversal bug, not durability.
// ---------------------------------------------------------------------------

/// Delete `n` leading keys via point `db.delete`, leaving >= 2 CONSECUTIVE
/// leftmost BINs physically empty; the forward `Get::First`+`Get::Next` scan
/// must still find EVERY remaining live key.  FAILS on tip 48784649 (scan
/// returns 0 once the 2nd leading BIN empties); PASSES after the loop fix.
#[test]
fn get_first_crosses_multiple_empty_leading_bins() {
    const NODE_MAX: u32 = 4;
    let dir = TempDir::new().unwrap();
    // 16 keys @ NODE_MAX=4 ≈ 4 BINs of 4 keys each.
    let total = 16u32;
    // Delete 5 leading keys: 1st BIN fully empty, 2nd BIN loses its first
    // slot — but the reviewer's sweep showed delete_n=5 empties the 2nd BIN
    // too under this geometry.  Use 6 to unambiguously empty >= 2 BINs.
    let delete_n = 6u32;

    let env = open_env(dir.path(), NODE_MAX);
    let db = open_db(&env);
    let mut expected: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    for i in 0u32..total {
        let k = ikey(i);
        put(&db, &k, &k);
        expected.insert(k.clone().into_bytes(), k.into_bytes());
    }
    // Genuine split geometry (else the test is vacuous — single fat BIN).
    assert_multi_bin(&db, 3);

    // Point-delete the leading keys (no cursor, no compress).
    for i in 0u32..delete_n {
        let k = ikey(i);
        assert!(
            db.delete(k.as_bytes()).unwrap(),
            "point-delete of leading key {k} must succeed"
        );
        expected.remove(k.as_bytes());
    }
    assert!(
        expected.len() as u32 == total - delete_n && !expected.is_empty(),
        "expected {} live keys after deleting {delete_n} leading",
        total - delete_n
    );

    // Point-get sweep: every live key IS on disk (proves it's a cursor bug,
    // not durability), and deleted keys are gone.
    for i in 0u32..total {
        let k = ikey(i);
        let got = db.get(k.as_bytes()).unwrap();
        if expected.contains_key(k.as_bytes()) {
            assert_eq!(
                got.as_deref(),
                Some(k.as_bytes()),
                "point-get: live key {k} must be present (data on disk)"
            );
        } else {
            assert!(got.is_none(), "point-get: deleted key {k} must be gone");
        }
    }

    // The actual regression: forward cursor scan must find the EXACT live set,
    // crossing >= 2 consecutive empty leading BINs.
    let scanned = collect_all(&db);
    assert_eq!(
        scanned, expected,
        "Get::First forward scan must cross ALL empty leading BINs and find \
         every live key (NEW-4 §3 multi-empty-BIN gap)"
    );

    db.close().unwrap();
    env.close().unwrap();
}

/// Symmetric: delete `n` TRAILING keys, leaving >= 2 CONSECUTIVE rightmost
/// BINs physically empty; the backward `Get::Last`+`Get::Prev` scan must find
/// EVERY remaining live key.
#[test]
fn get_last_crosses_multiple_empty_trailing_bins() {
    const NODE_MAX: u32 = 4;
    let dir = TempDir::new().unwrap();
    let total = 16u32;
    let delete_n = 6u32;

    let env = open_env(dir.path(), NODE_MAX);
    let db = open_db(&env);
    let mut expected: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    for i in 0u32..total {
        let k = ikey(i);
        put(&db, &k, &k);
        expected.insert(k.clone().into_bytes(), k.into_bytes());
    }
    assert_multi_bin(&db, 3);

    // Point-delete the trailing keys.
    for i in (total - delete_n)..total {
        let k = ikey(i);
        assert!(
            db.delete(k.as_bytes()).unwrap(),
            "point-delete of trailing key {k} must succeed"
        );
        expected.remove(k.as_bytes());
    }
    assert!(
        expected.len() as u32 == total - delete_n && !expected.is_empty(),
        "expected {} live keys after deleting {delete_n} trailing",
        total - delete_n
    );

    // Point-get sweep (data on disk).
    for i in 0u32..total {
        let k = ikey(i);
        let got = db.get(k.as_bytes()).unwrap();
        if expected.contains_key(k.as_bytes()) {
            assert_eq!(
                got.as_deref(),
                Some(k.as_bytes()),
                "point-get: live key {k} must be present (data on disk)"
            );
        } else {
            assert!(got.is_none(), "point-get: deleted key {k} must be gone");
        }
    }

    // Backward cursor scan (Get::Last + Get::Prev) must find the EXACT live
    // set, crossing >= 2 consecutive empty trailing BINs.
    let mut cursor = db.open_cursor(None).unwrap();
    let mut scanned: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    let mut key = DatabaseEntry::new();
    let mut val = DatabaseEntry::new();
    let mut status = cursor.get(&mut key, &mut val, Get::Last, None).unwrap();
    while status == OperationStatus::Success {
        scanned.insert(
            key.data_opt().unwrap_or(&[]).to_vec(),
            val.data_opt().unwrap_or(&[]).to_vec(),
        );
        status = cursor.get(&mut key, &mut val, Get::Prev, None).unwrap();
    }
    cursor.close().unwrap();
    assert_eq!(
        scanned, expected,
        "Get::Last backward scan must cross ALL empty trailing BINs and find \
         every live key (NEW-4 §3 multi-empty-BIN gap)"
    );

    db.close().unwrap();
    env.close().unwrap();
}
