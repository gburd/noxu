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
            key.get_data().unwrap_or(&[]).to_vec(),
            val.get_data().unwrap_or(&[]).to_vec(),
        );
        status = cursor.get(&mut key, &mut val, Get::Next, None).unwrap();
    }
    cursor.close().unwrap();
    map
}

/// JE `CheckBase.recoverAndLoadData`: reopen (recover), `env.verify()`,
/// full-scan. Returns the recovered KV set; panics on any structural error.
fn recover_and_collect(
    dir: &Path,
    node_max: u32,
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
        db.close().unwrap();
        env.close().unwrap();
    }

    let recovered = recover_and_collect(dir.path(), NODE_MAX);
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
        db.close().unwrap();
        env.close().unwrap();
    }

    let recovered = recover_and_collect(dir.path(), NODE_MAX);
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
        db.close().unwrap();
        env.close().unwrap();
    }

    let recovered = recover_and_collect(dir.path(), NODE_MAX);
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
                let removed = key.get_data().unwrap().to_vec();
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

    let recovered = recover_and_collect(dir.path(), NODE_MAX);
    assert_eq!(
        recovered, expected,
        "reverse-split: recovered set != expected committed set"
    );
}

/// JE `CheckReverseSplitsTest.testCompleteRemoval` (`setupCompleteRemoval`):
/// populate a 3-level tree, delete EVERY record, checkpoint, compress (the
/// subtree is removed leaving a single BIN), then insert new data. Recover and
/// assert data + structure (and the complete-removal stat: a single BIN).
#[test]
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
            db.get_stats(Some(&StatsConfig::new().with_fast(false))).unwrap();
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

    let recovered = recover_and_collect(dir.path(), NODE_MAX);
    assert_eq!(
        recovered, expected,
        "complete-removal: recovered set != expected committed set"
    );
}
