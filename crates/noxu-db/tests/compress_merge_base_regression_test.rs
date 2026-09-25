//! Refutation evidence for the SYMMETRIC "compress-merge stale base" lead.
//!
//! A sibling lead (bin-split-base.md, fixed at 1c817fbd) flagged that
//! `Tree::compress_node`'s BIN-merge arm (tree.rs ~5871) bulk-replaces the
//! merge SURVIVOR's entries without setting `prohibit_next_delta`, so a later
//! sparse BINDelta over the survivor's stale pre-merge full base could LOSE the
//! merged-in keys on recovery. A tree-level unit test
//! (`compress_merge_survivor_full_base_is_invalidated_for_delta` in
//! noxu-tree) proves that defect exists WHEN the sibling-merge path runs.
//!
//! This integration test answers the reachability question: does any
//! PRODUCTION path drive that sibling-merge? The production
//! `Environment::compress()` and the background INCompressor daemon both route
//! through `compress_bin_with_lock_check`, which does *slot* compression
//! (removing known-deleted slots via `remove_slot`, already guarded by the
//! a1061397 `prohibit_next_delta` fix) and prunes EMPTY BINs only — it never
//! merges two under-full NON-EMPTY siblings. The bare `Tree::compress()`
//! sibling-merge is reachable only from `#[cfg(test)]` and the shuttle DST
//! harness.
//!
//! The test exercises the real production compression path under a workload
//! designed to leave many under-full-but-non-empty adjacent BINs, then asserts:
//!   1. `env.compress()` does NOT reduce the BIN count by merging under-full
//!      siblings (production compression never triggers `compress_node`).
//!   2. Data survives an evict+refault AND a checkpoint+crash+recover exactly
//!      (no lost/duplicated keys), across delta-eligible checkpoints after the
//!      compression — the end-to-end safety the lead worried about.
//!
//! If a future change routes production compression through the sibling-merge
//! (`compress_node`), assertion (1) will start failing and this test flips from
//! a refutation into a live reproduction, prompting the JE-parity fix
//! (`prohibit_next_delta` on the merge survivor).

use noxu_db::{
    CheckpointConfig, DatabaseConfig, DatabaseEntry, EnvironmentConfig, Get,
    OperationStatus, StatsConfig,
};
use std::collections::BTreeMap;
use std::path::Path;
use tempfile::TempDir;

const N: u32 = 900;

fn open_env(dir: &Path) -> noxu_db::Environment {
    let mut cfg = EnvironmentConfig::new(dir.to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    cfg.set_run_checkpointer(false);
    cfg.set_run_cleaner(false);
    cfg.set_run_in_compressor(false);
    cfg.set_run_evictor(false);
    noxu_db::Environment::open(cfg).unwrap()
}

fn open_db(env: &noxu_db::Environment) -> noxu_db::Database {
    env.open_database(
        None,
        "mergedb",
        &DatabaseConfig::new().with_allow_create(true).with_transactional(true),
    )
    .unwrap()
}

fn key(i: u32) -> Vec<u8> {
    format!("k_{i:07}").into_bytes()
}
fn value(i: u32) -> Vec<u8> {
    format!("v_{i:07}").into_bytes()
}

fn checkpoint(env: &noxu_db::Environment) {
    env.checkpoint(Some(&CheckpointConfig::new().with_force(true))).unwrap();
}

fn n_bins(db: &noxu_db::Database) -> u64 {
    db.stats(Some(&StatsConfig::new()))
        .unwrap()
        .btree
        .bottom_internal_node_count
}

/// Full forward scan: (distinct map, total cursor steps). A duplicated slot
/// (same key in two BINs) makes steps > map.len(); a lost key makes both drop.
fn scan(db: &noxu_db::Database) -> (BTreeMap<Vec<u8>, Vec<u8>>, usize) {
    let mut cursor = db.open_cursor(None).unwrap();
    let mut map = BTreeMap::new();
    let mut steps = 0usize;
    let mut k = DatabaseEntry::new();
    let mut v = DatabaseEntry::new();
    let mut s = cursor.get(&mut k, &mut v, Get::First, None).unwrap();
    while s == OperationStatus::Success {
        map.insert(
            k.data_opt().unwrap_or(&[]).to_vec(),
            v.data_opt().unwrap_or(&[]).to_vec(),
        );
        steps += 1;
        s = cursor.get(&mut k, &mut v, Get::Next, None).unwrap();
    }
    cursor.close().unwrap();
    (map, steps)
}

/// Keys 0..N inserted; then keys where i % 5 != 0 are DELETED (leaving ~1/5 of
/// each BIN's slots — under-full but non-empty siblings); then every surviving
/// 25th key is updated (sparse, delta-eligible over the post-compress base).
fn expected_set() -> BTreeMap<Vec<u8>, Vec<u8>> {
    let mut m = BTreeMap::new();
    for i in 0..N {
        if i % 5 == 0 {
            m.insert(key(i), value(i));
        }
    }
    // sparse update on surviving keys (multiples of 25 are also multiples of 5)
    for i in (0..N).step_by(25) {
        m.insert(key(i), b"updated".to_vec());
    }
    m
}

/// Returns the BIN count immediately before and after the production
/// `env.compress()` pass, so the caller can assert compression did not MERGE
/// under-full siblings (it only prunes empty BINs).
fn build_workload(
    env: &noxu_db::Environment,
    db: &noxu_db::Database,
) -> (u64, u64) {
    // Phase 0: insert 0..N, checkpoint durable FULL bases for every BIN.
    {
        let txn = env.begin_transaction(None).unwrap();
        for i in 0..N {
            db.put_in(
                &txn,
                DatabaseEntry::from_bytes(&key(i)),
                DatabaseEntry::from_bytes(&value(i)),
            )
            .unwrap();
        }
        txn.commit().unwrap();
    }
    checkpoint(env);
    assert!(n_bins(db) >= 4, "need several BINs to test sibling merge");

    // Phase 1: delete 4/5 of the keys so adjacent BINs are jointly under-full
    // (the precondition for a sibling MERGE in compress_node).
    {
        let txn = env.begin_transaction(None).unwrap();
        for i in 0..N {
            if i % 5 != 0 {
                db.delete_in(&txn, DatabaseEntry::from_bytes(&key(i))).unwrap();
            }
        }
        txn.commit().unwrap();
    }

    // Phase 2: run PRODUCTION compression. This drains known-deleted slots and
    // prunes empty BINs; it must NOT merge under-full non-empty siblings.
    let bins_before = n_bins(db);
    let _ = env.compress().unwrap();
    let bins_after = n_bins(db);

    // Phase 3: sparse update on surviving keys -> delta-eligible checkpoint over
    // the post-compress base. If production compression HAD merged siblings and
    // left a stale base, this is where a delta would lose the merged-in keys.
    {
        let txn = env.begin_transaction(None).unwrap();
        for i in (0..N).step_by(25) {
            db.put_in(
                &txn,
                DatabaseEntry::from_bytes(&key(i)),
                DatabaseEntry::from_bytes(b"updated"),
            )
            .unwrap();
        }
        txn.commit().unwrap();
    }
    checkpoint(env);

    (bins_before, bins_after)
}

fn assert_exact(tag: &str, env: &noxu_db::Environment, db: &noxu_db::Database) {
    let vresult = env.verify(&noxu_db::VerifyConfig::new()).unwrap();
    assert_eq!(
        vresult.error_count(),
        0,
        "{tag}: structural verify errors: {:?}",
        vresult.errors
    );
    let (recovered, steps) = scan(db);
    let expected = expected_set();
    eprintln!(
        "{tag}: cursor_steps={steps} distinct={} expected={}",
        recovered.len(),
        expected.len()
    );
    assert_eq!(
        steps,
        recovered.len(),
        "{tag}: duplicate physical slots (resurrected keys): {steps} steps vs \
         {} distinct",
        recovered.len()
    );
    let missing: Vec<_> =
        expected.keys().filter(|k| !recovered.contains_key(*k)).collect();
    let extra: Vec<_> =
        recovered.keys().filter(|k| !expected.contains_key(*k)).collect();
    assert!(
        missing.is_empty(),
        "{tag}: lost keys: {} (first: {:?})",
        missing.len(),
        missing.first().map(|k| String::from_utf8_lossy(k).into_owned())
    );
    assert!(
        extra.is_empty(),
        "{tag}: unexpected keys: {} (first: {:?})",
        extra.len(),
        extra.first().map(|k| String::from_utf8_lossy(k).into_owned())
    );
    assert_eq!(recovered, expected, "{tag}: recovered set != committed set");
}

/// Refutation (1) + evict/refault safety: production `env.compress()` does not
/// merge under-full siblings, and data survives evict+refault exactly.
#[test]
fn production_compress_does_not_merge_and_survives_refault() {
    let dir = TempDir::new().unwrap();
    let env = open_env(dir.path());
    let db = open_db(&env);

    let (bins_before, bins_after) = build_workload(&env, &db);
    eprintln!(
        "production compress: bins_before={bins_before} bins_after={bins_after}"
    );

    // Refutation (1): production compression MUST NOT reduce the BIN count by
    // MERGING under-full siblings. (It may drop empty BINs, but this workload
    // leaves ~1/5 of each BIN populated, so no BIN is fully empty; a decrease
    // here would mean a sibling merge fired via compress_node.) If this ever
    // fails, production has started driving the sibling-merge path and this
    // test becomes a LIVE reproduction requiring the survivor prohibit_delta
    // fix.
    assert_eq!(
        bins_before, bins_after,
        "production env.compress() reduced BIN count from {bins_before} to \
         {bins_after}: it MERGED under-full siblings via compress_node. That \
         path leaves the survivor with a stale full base (see the noxu-tree \
         unit test compress_merge_survivor_full_base_is_invalidated_for_delta) \
         and now needs the prohibit_next_delta guard."
    );

    for _ in 0..3 {
        let _ = env.evict_memory().unwrap();
    }
    assert_exact("evict-refault", &env, &db);
}

/// Refutation (2): the same workload survives a checkpoint + crash + recover
/// exactly — the end-to-end durability the lead worried about.
#[test]
fn production_compress_survives_crash_recovery() {
    const CHILD_MODE: &str = "NOXU_MERGE_BASE_CHILD";
    const CHILD_HOME: &str = "NOXU_MERGE_BASE_HOME";

    if std::env::var(CHILD_MODE).is_ok() {
        let home = std::env::var_os(CHILD_HOME).unwrap();
        let env = open_env(Path::new(&home));
        let db = open_db(&env);
        build_workload(&env, &db);
        // Crash: no clean close, no final checkpoint beyond the one in-workload.
        std::process::exit(73);
    }

    let dir = TempDir::new().unwrap();
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "production_compress_survives_crash_recovery",
            "--nocapture",
        ])
        .env(CHILD_MODE, "1")
        .env(CHILD_HOME, dir.path())
        .status()
        .unwrap();
    assert_eq!(status.code(), Some(73), "child did not reach crash point");

    let env = open_env(dir.path());
    let db = open_db(&env);
    assert_exact("crash-recovery", &env, &db);
}
