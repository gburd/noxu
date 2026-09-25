//! Regression: a BIN split must invalidate the left half's full-image base
//! so a later sparse BINDelta cannot resurrect the moved-away right-half keys.
//!
//! `Tree::split_child` (tree.rs ~4292) mutates the left half in place — it
//! installs the left-half entries, marks the node dirty — but leaves
//! `last_full_lsn` pointing at the PRE-split full image (which still holds ALL
//! the original keys, both halves) and does NOT set `prohibit_next_delta`.
//! The right sibling is created with `last_full_lsn == NULL_LSN`, so it is
//! forced to log a full image (safe). The LEFT half is the exposed node.
//!
//! A BINDelta records only dirty slots; it cannot express the *removal* of the
//! moved-away right-half keys. If the left half is later logged as a sparse
//! delta against its stale pre-split `last_full_lsn`, then on refault/recovery
//! the reconstitution (`fetch_node_from_log` / `mutate_to_full_bin`) merges the
//! pre-split full base (all original keys) with the sparse delta BY KEY, and
//! the moved-away right-half keys reappear in the left BIN — duplicated across
//! two BINs.
//!
//! JE avoids this: `IN.splitInternal` (IN.java:4154) logs BOTH modified halves
//! at split time via `optionalLogProvisionalNoCompress` →
//! `logInternal(allowDeltas=false)`, which sets `lastFullLsn = newLsn` +
//! `lastDeltaLsn = NULL` for both halves (IN.java:5545). The left half's full
//! version is advanced to the post-split image before any delta is possible.
//!
//! Probe: default fanout, enough ascending keys to force real BIN splits, a
//! full-image checkpoint (the split BINs' bases), then an UPDATE-ONLY sparse
//! change on already-split left-half keys (NO deletes — a delete would set
//! `prohibit_next_delta` via `remove_slot` and mask the split path). Checkpoint
//! (left BINs may log a delta over their stale base), then exercise the on-disk
//! chain via (A) evict + refault and (B) crash + recover in a child process.
//!
//! Oracle: a full forward cursor scan must visit each key EXACTLY once
//! (`cursor_steps == distinct_keys`); a resurrected moved-away key appears in
//! two BINs and shows up as an extra cursor step. We also assert exact set
//! equality against the true committed set and run structural `env.verify()`.
//! Daemons are OFF; delta-path activation is reported via `delta_in_flush`.

use noxu_db::{
    CheckpointConfig, DatabaseConfig, DatabaseEntry, EnvironmentConfig, Get,
    OperationStatus,
};
use std::collections::BTreeMap;
use std::path::Path;
use tempfile::TempDir;

// Default fanout is 256; recovery currently rebuilds every tree at fanout 256
// regardless of the DB's configured NODE_MAX_ENTRIES, so a small fanout does
// NOT survive a reopen — we must force a genuine split with real key volume.
// 700 ascending keys guarantees several BIN splits under fanout 256.
const N: u32 = 700;

fn open_env_split(dir: &Path) -> noxu_db::Environment {
    let mut cfg = EnvironmentConfig::new(dir.to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    cfg.set_run_checkpointer(false);
    cfg.set_run_cleaner(false);
    cfg.set_run_in_compressor(false);
    cfg.set_run_evictor(false);
    noxu_db::Environment::open(cfg).unwrap()
}

fn open_db_split(env: &noxu_db::Environment) -> noxu_db::Database {
    env.open_database(
        None,
        "splitdb",
        &DatabaseConfig::new().with_allow_create(true).with_transactional(true),
    )
    .unwrap()
}

/// Full forward scan. Returns (distinct map, total cursor steps). A duplicated
/// physical slot (same key in two BINs) makes steps > map.len().
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

fn checkpoint(env: &noxu_db::Environment) {
    env.checkpoint(Some(&CheckpointConfig::new().with_force(true))).unwrap();
}

fn delta_in_flush(env: &noxu_db::Environment) -> u64 {
    env.stats().unwrap().checkpoint.delta_in_flush
}

// Fixed-width so lexical order == numeric order (ascending inserts → splits).
fn key(i: u32) -> Vec<u8> {
    format!("k_{i:07}").into_bytes()
}
fn value(i: u32) -> Vec<u8> {
    format!("v_{i:07}").into_bytes()
}

/// True committed set: all N keys inserted, then every 100th key updated, then
/// (phase 5) every 100th key offset by 50 updated to "updated2".
fn expected_set() -> BTreeMap<Vec<u8>, Vec<u8>> {
    let mut m = BTreeMap::new();
    for i in 0..N {
        m.insert(key(i), value(i));
    }
    for i in (0..N).step_by(100) {
        m.insert(key(i), b"updated".to_vec());
    }
    for i in (50..N).step_by(100) {
        m.insert(key(i), b"updated2".to_vec());
    }
    m
}

/// Build the workload; returns the number of BINDeltas logged at the final
/// checkpoint (delta-path activation evidence).
///
/// Ordering is deliberate: we checkpoint a FULL image of the BIN(s) BEFORE the
/// split, so the pre-split full base is durable, THEN force the split and log
/// a sparse delta on the left half WITHOUT an intervening full-image checkpoint
/// of that left half. That is the only ordering in which a delta could ride on
/// a stale pre-split base.
fn build_workload(env: &noxu_db::Environment, db: &noxu_db::Database) -> u64 {
    // Phase 0: insert JUST BELOW one fanout so the BIN does NOT split yet, and
    // checkpoint a durable FULL image of that (unsplit) BIN. 200 < 256.
    {
        let txn = env.begin_transaction(None).unwrap();
        for i in 0..200 {
            db.put_in(
                &txn,
                DatabaseEntry::from_bytes(&key(i)),
                DatabaseEntry::from_bytes(&value(i)),
            )
            .unwrap();
        }
        txn.commit().unwrap();
    }
    checkpoint(env); // durable pre-split full base (holds keys 0..200)

    // Phase 1: now insert the REST so that BIN splits. The low keys (0..200)
    // whose full base was just written get partitioned across the split.
    {
        let txn = env.begin_transaction(None).unwrap();
        for i in 200..N {
            db.put_in(
                &txn,
                DatabaseEntry::from_bytes(&key(i)),
                DatabaseEntry::from_bytes(&value(i)),
            )
            .unwrap();
        }
        txn.commit().unwrap();
    }

    // Phase 3: UPDATE ONLY (no deletes) a sparse set of LOW keys that were in
    // the pre-split full base and are now in the left half of the split.
    {
        let txn = env.begin_transaction(None).unwrap();
        for i in (0..N).step_by(100) {
            db.put_in(
                &txn,
                DatabaseEntry::from_bytes(&key(i)),
                DatabaseEntry::from_bytes(b"updated"),
            )
            .unwrap();
        }
        txn.commit().unwrap();
    }
    // Phase 4: checkpoint. Left BINs may log a delta over their stale base.
    let before = delta_in_flush(env);
    checkpoint(env);
    let deltas = delta_in_flush(env) - before;
    eprintln!("build_workload: phase-4 deltas={deltas}");

    // Phase 5: after the split's modified halves have been persisted (full
    // images with the fix), a subsequent sparse update MUST once again be
    // delta-eligible — the fix invalidates the stale base for ONE image, it
    // does not permanently disable deltas. Update a different sparse set and
    // checkpoint; assert a delta is chosen and the result still recovers.
    {
        let txn = env.begin_transaction(None).unwrap();
        for i in (50..N).step_by(100) {
            db.put_in(
                &txn,
                DatabaseEntry::from_bytes(&key(i)),
                DatabaseEntry::from_bytes(b"updated2"),
            )
            .unwrap();
        }
        txn.commit().unwrap();
    }
    let before5 = delta_in_flush(env);
    checkpoint(env);
    let deltas5 = delta_in_flush(env) - before5;
    eprintln!("build_workload: phase-5 deltas={deltas5}");
    deltas5
}

fn assert_no_resurrection(
    tag: &str,
    env: &noxu_db::Environment,
    db: &noxu_db::Database,
) {
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

    // A resurrected moved-away key appears in TWO BINs → the forward scan
    // visits it twice → steps exceeds the distinct-key count.
    assert_eq!(
        steps,
        recovered.len(),
        "{tag}: duplicate physical slots (resurrected moved-away keys): \
         cursor visited {steps} slots but only {} distinct keys",
        recovered.len()
    );

    let resurrected: Vec<_> =
        recovered.keys().filter(|k| !expected.contains_key(*k)).collect();
    let missing: Vec<_> =
        expected.keys().filter(|k| !recovered.contains_key(*k)).collect();
    assert!(
        resurrected.is_empty(),
        "{tag}: unexpected keys: {} (first: {:?})",
        resurrected.len(),
        resurrected.first().map(|k| String::from_utf8_lossy(k).into_owned())
    );
    assert!(
        missing.is_empty(),
        "{tag}: lost keys: {} (first: {:?})",
        missing.len(),
        missing.first().map(|k| String::from_utf8_lossy(k).into_owned())
    );
    assert_eq!(recovered, expected, "{tag}: recovered set != committed set");
}

/// Probe A: evict + refault in the same process (split structure preserved).
#[test]
fn split_left_half_delta_survives_evict_refault() {
    let dir = TempDir::new().unwrap();
    let env = open_env_split(dir.path());
    let db = open_db_split(&env);

    let phase5_deltas = build_workload(&env, &db);
    // Regression against over-fixing: after the split's forced full image, a
    // later sparse update MUST be delta-eligible again (the fix invalidates
    // the base for one image, it does not permanently disable deltas).
    assert!(
        phase5_deltas > 0,
        "post-split full image did not restore delta eligibility \
         (phase-5 deltas={phase5_deltas})"
    );

    // Drop resident BINs so a later access must refault them from their
    // delta+base chain.
    for _ in 0..3 {
        let _ = env.evict_memory().unwrap();
    }

    assert_no_resurrection("evict-refault", &env, &db);
}

/// Probe B: crash + recover (child process re-exec; a clean close cannot
/// repair the on-disk chain).
#[test]
fn split_left_half_delta_survives_crash_recovery() {
    const CHILD_MODE: &str = "NOXU_SPLIT_BASE_CHILD";
    const CHILD_HOME: &str = "NOXU_SPLIT_BASE_HOME";

    if std::env::var(CHILD_MODE).is_ok() {
        let home = std::env::var_os(CHILD_HOME).unwrap();
        let env = open_env_split(Path::new(&home));
        let db = open_db_split(&env);
        build_workload(&env, &db);
        // Crash: no close, no final checkpoint.
        std::process::exit(73);
    }

    let dir = TempDir::new().unwrap();
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "split_left_half_delta_survives_crash_recovery",
            "--nocapture",
        ])
        .env(CHILD_MODE, "1")
        .env(CHILD_HOME, dir.path())
        .status()
        .unwrap();
    assert_eq!(status.code(), Some(73), "child did not reach crash point");

    let env = open_env_split(dir.path());
    let db = open_db_split(&env);
    assert_no_resurrection("crash-recovery", &env, &db);
}
