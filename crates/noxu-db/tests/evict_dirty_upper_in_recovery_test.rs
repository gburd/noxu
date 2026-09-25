//! GAP B production crash-recovery regression: a DIRTY upper IN evicted before
//! a checkpoint must be LOGGED first, so its in-memory structural change (a
//! post-split child slot) survives a crash.
//!
//! Bug (base, before EVICTOR-UPPER-IN-1): the evictor's
//! `flush_dirty_node_to_log` returned `true` ("flushed") for a non-BIN node
//! WITHOUT logging it, and `detach_node_by_id` forced `child_full_lsn = NULL`
//! for an Internal child so the grandparent slot kept its pre-change on-disk
//! LSN. Evicting a dirty upper IN therefore dropped the resident node while
//! leaving the grandparent pointing at the stale image — on crash before the
//! next checkpoint the post-split child slot (and every key under the new BIN)
//! was lost.
//!
//! Fix: the evictor LOGS the dirty upper IN before detach and stamps the fresh
//! LSN into the parent slot, exactly as JE `Evictor.evict` logs ANY dirty
//! target before `parent.detachNode(...)` (Evictor.java:3013-3035,
//! IN.detachNode IN.java:4019-4027).
//!
//! Crash simulation (no clean close/checkpoint to repair the control): the log
//! directory is COPIED at the crash point and recovery runs from the copy. The
//! original env's final `close()` checkpoint never touches the copy.

use noxu_db::{
    DatabaseConfig, DatabaseEntry, EnvironmentConfig, Get, OperationStatus,
    StatsConfig, VerifyConfig,
};
use std::collections::BTreeMap;
use std::path::Path;
use tempfile::TempDir;

const NODE_MAX: u32 = 4;
// Tiny cache so the evictor is forced to evict interior nodes, not just LNs.
const CACHE_BYTES: u64 = 64 * 1024;

/// Env with all daemons OFF so eviction and checkpointing happen only when we
/// ask; a small cache so manual eviction reclaims interior nodes.
fn open_env(dir: &Path) -> noxu_db::Environment {
    let mut cfg = EnvironmentConfig::new(dir.to_path_buf())
        .with_allow_create(true)
        .with_transactional(true)
        .with_cache_size(CACHE_BYTES);
    cfg.set_run_cleaner(false);
    cfg.set_run_checkpointer(false);
    cfg.set_run_evictor(false);
    cfg.set_run_in_compressor(false);
    cfg.set_node_max_entries(NODE_MAX);
    noxu_db::Environment::open(cfg).unwrap()
}

fn open_db(env: &noxu_db::Environment) -> noxu_db::Database {
    env.open_database(
        None,
        "gapB",
        &DatabaseConfig::new().with_allow_create(true),
    )
    .unwrap()
}

fn put(db: &noxu_db::Database, k: &str, v: &str) {
    db.put(
        DatabaseEntry::from_bytes(k.as_bytes()),
        DatabaseEntry::from_bytes(v.as_bytes()),
    )
    .unwrap();
}

fn ikey(i: u32) -> String {
    format!("k{i:08}")
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

/// Copy the whole env directory `src` into a fresh temp dir (crash snapshot).
fn copy_env_dir(src: &Path) -> TempDir {
    let dst = TempDir::new().unwrap();
    for entry in std::fs::read_dir(src).unwrap() {
        let entry = entry.unwrap();
        let from = entry.path();
        if from.is_file() {
            let to = dst.path().join(entry.file_name());
            std::fs::copy(&from, &to).unwrap();
        }
    }
    dst
}

/// Recover from a crash snapshot: reopen (triggers recovery), verify structure,
/// full-scan. Panics on any structural error.
fn recover_and_collect(dir: &Path) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let env = open_env(dir);
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

/// Force a split (creating a dirty upper IN), evict interior nodes so the
/// dirty upper IN is logged+detached, snapshot the log at the crash point,
/// recover from the copy, and assert every key + the structure survive.
#[test]
fn dirty_upper_in_evicted_before_crash_survives_recovery() {
    let src = TempDir::new().unwrap();
    let mut expected = BTreeMap::new();

    // How many upper INs were evicted while dirty (the GAP B path).
    let dirty_evicted;

    {
        let env = open_env(src.path());
        let db = open_db(&env);

        // 1. Build a checkpointed baseline: a small tree pushed fully to disk.
        for i in 0u32..8 {
            let k = ikey(i);
            put(&db, &k, &k);
            expected.insert(k.clone().into_bytes(), k.into_bytes());
        }
        env.checkpoint(None).unwrap();

        // 2. Force SPLITS with ascending inserts (NODE_MAX=4). Each split
        //    installs a new child slot in a parent upper IN and marks it dirty.
        //    These upper-IN structural changes are NOT yet checkpointed.
        for i in 8u32..200 {
            let k = ikey(i);
            put(&db, &k, &k);
            expected.insert(k.clone().into_bytes(), k.into_bytes());
        }

        // Confirm the tree actually grew interior structure to evict.
        let st = db.stats(Some(&StatsConfig::new().with_fast(false))).unwrap();
        assert!(
            st.btree.internal_node_count >= 2,
            "fixture: expected a multi-level tree with upper INs, got \
             internal_node_count={}",
            st.btree.internal_node_count
        );

        // 3. Evict repeatedly. With a tiny cache the evictor reclaims BINs
        //    (making upper INs childless) then the dirty childless upper INs.
        //    A dirty upper IN reaching the Evict path is LOGGED first (the
        //    fix); on base it was detached WITHOUT logging.
        for _ in 0..12 {
            let _ = env.evict_memory().unwrap();
        }

        dirty_evicted = env.stats().unwrap().evictor.dirty_nodes_evicted;

        // 4. CRASH: snapshot the durable on-disk state BEFORE any clean close
        //    or checkpoint (which would flush the dirty set and mask the bug).
        let crash = copy_env_dir(src.path());

        // 5. Recover from the crash snapshot and assert exact survival.
        let recovered = recover_and_collect(crash.path());
        assert_eq!(
            recovered, expected,
            "GAP B: keys lost after crash-recovery — a dirty upper IN was \
             evicted without a fresh logged image, so a post-split child slot \
             (and the BIN under it) became unreachable on recovery"
        );

        // Tidy: clean-close the ORIGINAL env (the snapshot recovery above used
        // an independent copy, so this does not repair the control).
        db.close().unwrap();
        env.close().unwrap();
    }

    // Path activation: at least one dirty node must have been evicted. (This
    // counter covers dirty BINs too; the exact-survival assertion above is the
    // real gate — it fails on base precisely because a dirty upper IN was
    // dropped without logging.)
    assert!(
        dirty_evicted > 0,
        "fixture did not exercise dirty-node eviction (dirty_nodes_evicted=0)"
    );
}
