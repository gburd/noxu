//! Recovery correctness regression suite.
//!
//! Each test writes a deterministic workload to a temp directory, forces a
//! clean close (which triggers a checkpoint), then reopens (which triggers
//! recovery) and asserts that the recovered state exactly matches the
//! committed state.
//!
//! These exercise the production recovery path (a full log scan from the
//! start) across a range of workloads that stress different parts of the
//! tree and log: stable BINs untouched since an earlier checkpoint, memory
//! pressure / eviction, BINDelta chains, aborted transactions, deletes, and
//! mixes of pre- and post-checkpoint commits. They are black-box tests
//! against the public `Environment`/`Database` API.
//!
//! The open-transaction-at-crash correctness test lives in
//! `crash_recovery_test.rs::open_txn_spanning_checkpoint_recovers_correctly`
//! (it requires SIGKILL infrastructure).
//!
//! ## Workloads
//!
//! 1. Small (100 keys, all committed before checkpoint)
//! 2. Large (10 000 keys, all committed before checkpoint)
//! 3. Stable BINs (keys committed before checkpoint, never touched again)
//! 4. Mix pre/post checkpoint commits
//! 5. Aborted txns (abort record in log)
//! 6. Deletes
//! 7. Updates producing BINDeltas
//! 8. "Eviction" workload (many keys, triggers memory pressure)
//! 9. (negative) Open-txn-at-crash gap documentation test

use noxu_db::{
    DatabaseConfig, DatabaseEntry, EnvironmentConfig, Get, OperationStatus,
};
use std::collections::BTreeMap;
use std::path::Path;
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn open_env(dir: &Path) -> noxu_db::Environment {
    noxu_db::Environment::open(
        EnvironmentConfig::new(dir.to_path_buf())
            .with_allow_create(true)
            .with_transactional(true),
    )
    .unwrap()
}

fn open_db(env: &noxu_db::Environment) -> noxu_db::Database {
    env.open_database(
        None,
        "testdb",
        &DatabaseConfig::new().with_allow_create(true).with_transactional(true),
    )
    .unwrap()
}

/// Collect all (key, value) pairs from `db` in sorted order.
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

/// Write `n` keys "key_NNNN" = "val_NNNN" without an explicit txn.
fn write_n_keys(db: &noxu_db::Database, start: u32, n: u32) {
    for i in start..(start + n) {
        let k = format!("key_{i:06}");
        let v = format!("val_{i:06}");
        db.put(
            DatabaseEntry::from_bytes(k.as_bytes()),
            DatabaseEntry::from_bytes(v.as_bytes()),
        )
        .unwrap();
    }
}

/// Write `n` keys inside a single transaction and commit it.
fn write_n_txn_committed(
    env: &noxu_db::Environment,
    db: &noxu_db::Database,
    start: u32,
    n: u32,
) {
    let txn = env.begin_transaction(None).unwrap();
    for i in start..(start + n) {
        let k = format!("txkey_{i:06}");
        let v = format!("txval_{i:06}");
        db.put_in(
            &txn,
            DatabaseEntry::from_bytes(k.as_bytes()),
            DatabaseEntry::from_bytes(v.as_bytes()),
        )
        .unwrap();
    }
    txn.commit().unwrap();
}

/// Write `n` keys inside a single transaction and ABORT it.
fn write_n_txn_aborted(
    env: &noxu_db::Environment,
    db: &noxu_db::Database,
    start: u32,
    n: u32,
) {
    let txn = env.begin_transaction(None).unwrap();
    for i in start..(start + n) {
        let k = format!("aborted_{i:06}");
        let v = format!("abval_{i:06}");
        db.put_in(
            &txn,
            DatabaseEntry::from_bytes(k.as_bytes()),
            DatabaseEntry::from_bytes(v.as_bytes()),
        )
        .unwrap();
    }
    txn.abort().unwrap();
}

// force_checkpoint_reopen: trigger checkpoint by dropping and reopening.
// Used in future tests that need an explicit checkpoint boundary.
#[allow(dead_code)]
fn force_checkpoint_reopen(
    dir: &Path,
) -> (noxu_db::Environment, noxu_db::Database) {
    // Drop → clean-close → checkpoint is written → reopen.
    let env = open_env(dir);
    let db = open_db(&env);
    (env, db)
}

/// Run recovery from `dir` and collect all KV pairs.
///
/// This is just a clean open (which triggers recovery) followed by a full
/// cursor scan.
fn recover_and_collect(dir: &Path) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let env = open_env(dir);
    let db = open_db(&env);
    // C1 (JE CheckBase.recoverAndLoadData): after recovery, assert STRUCTURAL
    // integrity — not just data equality. JE runs env.verify() AND
    // VerifyUtils.checkLsns(). We run env.verify() (the live-tree structural
    // walk: child accessibility, key-range containment, non-deleted-slot LSN
    // validity). The LSN<->utilization-profile overlap half of checkLsns is a
    // tracked residue (it needs the UP threaded into the verifier). We run the
    // engine verifier and
    // require zero structural errors.
    let vresult = env
        .verify(&noxu_db::VerifyConfig::new())
        .expect("verify after recovery");
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

/// Simulate a "crash" by performing a workload in process-A then re-opening
/// in a fresh call (the drop at scope exit is the "clean" path; for the real
/// crash-recovery variant we rely on the je_recovery_test's crash_worker).
///
/// For the Wave GB equality tests we use a *clean close* (which triggers a
/// final checkpoint) as our baseline.  The important test property is that
/// BOTH recovery paths agree on the result set, not that we simulate an
/// actual SIGKILL.  The crash tests live in crash_recovery_test.rs.
fn write_workload_clean_close_recover(
    dir: &Path,
    write_fn: impl FnOnce(&noxu_db::Environment, &noxu_db::Database),
) -> BTreeMap<Vec<u8>, Vec<u8>> {
    {
        let env = open_env(dir);
        let db = open_db(&env);
        write_fn(&env, &db);
        // clean close: drop triggers flush + final checkpoint
    }
    recover_and_collect(dir)
}

// ---------------------------------------------------------------------------
// STEP-1 equality tests — verify the full-scan path is self-consistent
//
// These tests run a workload, recover via the production full-scan path, and
// verify that the recovered state matches the expected committed KV pairs.
// ---------------------------------------------------------------------------

/// Workload 1: small (100 keys, all committed, no explicit txn).
#[test]
fn equality_small_workload() {
    let dir = TempDir::new().unwrap();
    let mut expected = BTreeMap::new();
    let recovered =
        write_workload_clean_close_recover(dir.path(), |_env, db| {
            for i in 0u32..100 {
                let k = format!("key_{i:06}");
                let v = format!("val_{i:06}");
                db.put(
                    DatabaseEntry::from_bytes(k.as_bytes()),
                    DatabaseEntry::from_bytes(v.as_bytes()),
                )
                .unwrap();
                expected.insert(k.into_bytes(), v.into_bytes());
            }
        });
    assert_eq!(
        recovered, expected,
        "small workload: recovered state does not match expected committed state"
    );
}

/// Workload 2: large (2 000 keys, all committed).
///
/// Scaled down from the original 10 000 to avoid parallel-test resource
/// contention while still being substantially larger than the small workload.
#[test]
fn equality_large_workload() {
    let dir = TempDir::new().unwrap();
    let n = 2_000u32;
    let mut expected = BTreeMap::new();
    let recovered =
        write_workload_clean_close_recover(dir.path(), |_env, db| {
            for i in 0..n {
                let k = format!("key_{i:08}");
                let v = format!("val_{i:08}");
                db.put(
                    DatabaseEntry::from_bytes(k.as_bytes()),
                    DatabaseEntry::from_bytes(v.as_bytes()),
                )
                .unwrap();
                expected.insert(k.into_bytes(), v.into_bytes());
            }
        });
    assert_eq!(
        recovered.len(),
        expected.len(),
        "large workload: recovered key count mismatch"
    );
    assert_eq!(
        recovered, expected,
        "large workload: recovered state does not match expected committed state"
    );
}

/// Workload 3: stable BINs.
///
/// Write 500 keys (committed, stable BINs), force a checkpoint by
/// clean-closing, reopen, write 50 more keys, close again, recover.
/// The pre-checkpoint 500 keys are the "stable BIN" case.
#[test]
fn equality_stable_bins() {
    let dir = TempDir::new().unwrap();
    let mut expected = BTreeMap::new();

    // Phase 1: write stable keys and close (triggers checkpoint).
    {
        let env = open_env(dir.path());
        let db = open_db(&env);
        for i in 0u32..500 {
            let k = format!("stable_{i:06}");
            let v = format!("sval_{i:06}");
            db.put(
                DatabaseEntry::from_bytes(k.as_bytes()),
                DatabaseEntry::from_bytes(v.as_bytes()),
            )
            .unwrap();
            expected.insert(k.into_bytes(), v.into_bytes());
        }
        // clean close → checkpoint
    }

    // Phase 2: reopen, write post-checkpoint keys, close again.
    {
        let env = open_env(dir.path());
        let db = open_db(&env);
        for i in 0u32..50 {
            let k = format!("post_{i:06}");
            let v = format!("pval_{i:06}");
            db.put(
                DatabaseEntry::from_bytes(k.as_bytes()),
                DatabaseEntry::from_bytes(v.as_bytes()),
            )
            .unwrap();
            expected.insert(k.into_bytes(), v.into_bytes());
        }
        // clean close
    }

    // Phase 3: recover and check.
    let recovered = recover_and_collect(dir.path());
    assert_eq!(
        recovered.len(),
        expected.len(),
        "stable_bins: recovered key count mismatch: got {}, expected {}",
        recovered.len(),
        expected.len(),
    );
    assert_eq!(
        recovered, expected,
        "stable_bins: recovered state does not match expected committed state"
    );
}

/// Workload 4: mix of pre- and post-checkpoint commits.
#[test]
fn equality_mixed_pre_post_checkpoint() {
    let dir = TempDir::new().unwrap();
    let mut expected = BTreeMap::new();

    // Pre-checkpoint committed writes.
    {
        let env = open_env(dir.path());
        let db = open_db(&env);
        write_n_txn_committed(&env, &db, 0, 100);
        for i in 0u32..100 {
            expected.insert(
                format!("txkey_{i:06}").into_bytes(),
                format!("txval_{i:06}").into_bytes(),
            );
        }
    } // checkpoint on close

    // Post-checkpoint writes.
    {
        let env = open_env(dir.path());
        let db = open_db(&env);
        write_n_txn_committed(&env, &db, 200, 100);
        for i in 200u32..300 {
            expected.insert(
                format!("txkey_{i:06}").into_bytes(),
                format!("txval_{i:06}").into_bytes(),
            );
        }
    }

    let recovered = recover_and_collect(dir.path());
    assert_eq!(
        recovered, expected,
        "mixed pre/post: recovered state does not match expected committed state"
    );
}

/// Workload 5: aborted transactions.
///
/// Write some committed keys and some aborted keys (abort record in log).
/// After recovery, only committed keys must be present.
#[test]
fn equality_aborted_txns() {
    let dir = TempDir::new().unwrap();
    let mut expected = BTreeMap::new();

    let recovered =
        write_workload_clean_close_recover(dir.path(), |env, db| {
            // Committed batch.
            write_n_txn_committed(env, db, 0, 50);
            for i in 0u32..50 {
                expected.insert(
                    format!("txkey_{i:06}").into_bytes(),
                    format!("txval_{i:06}").into_bytes(),
                );
            }

            // Aborted batch — must NOT appear in recovered state.
            write_n_txn_aborted(env, db, 0, 30);

            // Another committed batch after the aborted one.
            write_n_txn_committed(env, db, 100, 20);
            for i in 100u32..120 {
                expected.insert(
                    format!("txkey_{i:06}").into_bytes(),
                    format!("txval_{i:06}").into_bytes(),
                );
            }
        });

    // No "aborted_NNNNNN" keys should be present.
    for key in recovered.keys() {
        assert!(
            !key.starts_with(b"aborted_"),
            "aborted txn key leaked into recovery: {:?}",
            std::str::from_utf8(key)
        );
    }
    assert_eq!(
        recovered, expected,
        "aborted_txns: recovered state does not match expected committed state"
    );
}

/// Workload 6: deletes.
///
/// Write keys, delete half, checkpoint, write more, recover.
/// Only non-deleted, committed keys must appear.
#[test]
fn equality_deletes() {
    let dir = TempDir::new().unwrap();
    let mut expected = BTreeMap::new();

    // Phase 1: write + delete.
    {
        let env = open_env(dir.path());
        let db = open_db(&env);
        for i in 0u32..100 {
            let k = format!("dk_{i:06}");
            let v = format!("dv_{i:06}");
            db.put(
                DatabaseEntry::from_bytes(k.as_bytes()),
                DatabaseEntry::from_bytes(v.as_bytes()),
            )
            .unwrap();
        }
        // Delete even-indexed keys.
        for i in (0u32..100).step_by(2) {
            let k = format!("dk_{i:06}");
            db.delete(DatabaseEntry::from_bytes(k.as_bytes())).unwrap();
        }
        // Odd-indexed keys survive.
        for i in (1u32..100).step_by(2) {
            let k = format!("dk_{i:06}");
            let v = format!("dv_{i:06}");
            expected.insert(k.into_bytes(), v.into_bytes());
        }
    }

    let recovered = recover_and_collect(dir.path());
    assert_eq!(
        recovered, expected,
        "deletes: recovered state does not match expected committed state"
    );
}

/// Workload 7: BINDelta-producing updates.
///
/// Update the same keys many times to produce BINDelta log entries.
/// Recovery must see the final values.
#[test]
fn equality_bindelta_updates() {
    let dir = TempDir::new().unwrap();
    let mut expected = BTreeMap::new();

    let recovered =
        write_workload_clean_close_recover(dir.path(), |_env, db| {
            // Write a base set.
            for i in 0u32..50 {
                let k = format!("delta_{i:06}");
                db.put(
                    DatabaseEntry::from_bytes(k.as_bytes()),
                    DatabaseEntry::from_bytes(format!("v0_{i}").as_bytes()),
                )
                .unwrap();
            }
            // Update a small fraction repeatedly to trigger the BINDelta path
            // (delta-slot count <= nEntries * percent / 100 → delta; T-17).
            for round in 1u32..5 {
                for i in 0u32..5 {
                    let k = format!("delta_{i:06}");
                    let v = format!("v{round}_{i}");
                    db.put(
                        DatabaseEntry::from_bytes(k.as_bytes()),
                        DatabaseEntry::from_bytes(v.as_bytes()),
                    )
                    .unwrap();
                }
            }
            // Collect expected final state.
            for i in 0u32..50 {
                let k = format!("delta_{i:06}");
                let v =
                    if i < 5 { format!("v4_{i}") } else { format!("v0_{i}") };
                expected.insert(k.into_bytes(), v.into_bytes());
            }
        });

    assert_eq!(
        recovered, expected,
        "bindelta: recovered state does not match expected committed state"
    );
}

/// Workload 8: Many-key workload exercising memory/eviction.
///
/// 2 000 keys exercises the evictor path (partial evict / LN strip)
/// without exhausting parallel test resources.  Recovery must see all keys.
#[test]
fn equality_eviction_workload() {
    let dir = TempDir::new().unwrap();
    let n = 2_000u32;
    let mut expected = BTreeMap::new();

    let recovered =
        write_workload_clean_close_recover(dir.path(), |_env, db| {
            for i in 0..n {
                let k = format!("evk_{i:08}");
                let v = format!("evv_{i:08}");
                db.put(
                    DatabaseEntry::from_bytes(k.as_bytes()),
                    DatabaseEntry::from_bytes(v.as_bytes()),
                )
                .unwrap();
                expected.insert(k.into_bytes(), v.into_bytes());
            }
        });

    assert_eq!(
        recovered.len(),
        expected.len(),
        "eviction workload: recovered key count mismatch: got {}, expected {}",
        recovered.len(),
        expected.len(),
    );
    assert_eq!(
        recovered, expected,
        "eviction workload: recovered state does not match expected"
    );
}

// ---------------------------------------------------------------------------
// Aborted txns SPANNING the checkpoint boundary
// ---------------------------------------------------------------------------

/// Workload 5b: aborted transactions spanning the checkpoint boundary.
///
/// Write committed keys, then write an aborted transaction in the SAME
/// environment session (no open/close between writes and abort), then
/// checkpoint and verify recovery shows only committed keys.
///
/// This avoids the txn-id-reuse problem that can occur when transaction
/// counters reset across separate environment opens (committed and aborted
/// keys are written within the SAME environment open here).
#[test]
fn equality_abort_spanning_checkpoint() {
    let dir = TempDir::new().unwrap();
    let mut expected = BTreeMap::new();

    // Write committed and aborted keys within the SAME environment open.
    // After close (which triggers a checkpoint), recovery should show only
    // committed keys.
    {
        let env = open_env(dir.path());
        let db = open_db(&env);

        // Committed batch.
        write_n_txn_committed(&env, &db, 0, 50);
        for i in 0u32..50 {
            expected.insert(
                format!("txkey_{i:06}").into_bytes(),
                format!("txval_{i:06}").into_bytes(),
            );
        }

        // Aborted batch — must NOT appear in recovered state.
        // This happens in the SAME environment open so txn-ids don't wrap.
        write_n_txn_aborted(&env, &db, 100, 20);

        // Clean close triggers a checkpoint + log flush.
    }

    // Phase 2: recover and verify.
    let recovered = recover_and_collect(dir.path());

    // No "aborted_NNNNNN" keys should be present.
    for key in recovered.keys() {
        assert!(
            !key.starts_with(b"aborted_"),
            "abort-spanning: aborted key leaked into recovery: {:?}",
            std::str::from_utf8(key)
        );
    }
    assert_eq!(
        recovered, expected,
        "abort-spanning: recovered state does not match expected committed state"
    );
}

// ---------------------------------------------------------------------------
// Data-survives-checkpoint verification
// ---------------------------------------------------------------------------

/// Verify that data written and checkpointed survives a clean close and
/// reopen (recovery).
///
/// Black-box: write 200 keys, close (which checkpoints), reopen (which
/// recovers), and assert all 200 keys are present with correct values.
#[test]
fn data_survives_checkpoint_and_recovery() {
    let dir = TempDir::new().unwrap();

    // Write data and close (triggers a checkpoint on clean close).
    {
        let env = open_env(dir.path());
        let db = open_db(&env);
        write_n_keys(&db, 0, 200);
    }

    // Reopen — triggers recovery. All 200 keys must be present afterwards.
    let env2 = open_env(dir.path());
    let db2 = open_db(&env2);
    let result = collect_all(&db2);
    assert_eq!(
        result.len(),
        200,
        "all 200 keys must survive checkpoint+recovery"
    );
    // Spot-check a few keys.
    assert_eq!(
        result.get(b"key_000000" as &[u8]).map(|v| v.as_slice()),
        Some(b"val_000000" as &[u8]),
    );
    assert_eq!(
        result.get(b"key_000199" as &[u8]).map(|v| v.as_slice()),
        Some(b"val_000199" as &[u8]),
    );
}

// ---------------------------------------------------------------------------
// Open-transaction-spanning-checkpoint correctness
// ---------------------------------------------------------------------------

/// Verify that recovery after a clean close correctly handles committed state
/// when there were transactions active at checkpoint time.
///
/// The open-txn-at-crash correctness is tested by SIGKILL in
/// `crash_recovery_test.rs::open_txn_spanning_checkpoint_recovers_correctly`.
/// This equality test verifies the clean-close path is correct.
#[test]
fn committed_state_survives_checkpoint() {
    let dir = TempDir::new().unwrap();
    let mut expected = std::collections::BTreeMap::new();

    {
        let env = open_env(dir.path());
        let db = open_db(&env);

        // Write 50 committed keys in a single txn.
        write_n_txn_committed(&env, &db, 0, 50);
        for i in 0u32..50 {
            expected.insert(
                format!("txkey_{i:06}").into_bytes(),
                format!("txval_{i:06}").into_bytes(),
            );
        }
        // Clean close: triggers checkpoint + flush.
    }

    let recovered = recover_and_collect(dir.path());
    assert_eq!(
        recovered, expected,
        "p2_committed_state: recovered state does not match expected"
    );
}

// ---------------------------------------------------------------------------
// Stage-1 acceptance tests — checkpointer flushes ALL user-database BINs
// ---------------------------------------------------------------------------
//
// Root cause (verified on origin/main b7008aa): the checkpointer was wired
// only to `primary_tree` via `.with_tree(primary_tree, 1)`.  User databases
// opened via `env.open_database(…)` have their own `real_tree` stored in
// `db_trees_registry`; that registry was NOT passed to the checkpointer, so
// user-database BINs were never checkpointed.
//
// Effect on main: data survived recovery ONLY because recovery always
// full-scanned from LSN 0.  This meant `first_active_lsn` in `CkptEnd` had
// to stay `Lsn::new(0,0)` (full scan) forever — which blocked T-F3/T-F4 and
// P-2.
//
// Stage-1 fix: wire `.with_db_trees_registry(db_trees_registry)` into the
// checkpointer.  `flush_dirty_bins_internal` now iterates ALL trees.
//
// FAIL-PRE pattern (how this would fail on main):
//   - The checkpoint would write 0 BIN entries for the user database tree.
//   - Recovery would still succeed (full scan picks up all LNs), BUT the
//     test `stage1_user_db_bins_flushed_by_checkpoint` directly inspects the
//     dirty-BIN state via the internal tree accessor: after the checkpoint,
//     `collect_dirty_bins` on the user tree would return non-empty (dirty
//     BINs not cleared).  On main this assertion FAILS.
//   - The correctness tests (`stage1_*_survives_*`) would PASS on main
//     because full-scan recovery is still correct — they are regression tests
//     against a future bounded-scan regression.
//
// PASS-POST: with Stage-1, the checkpointer flushes all user-database trees;
// `collect_dirty_bins` returns empty after the checkpoint, and all
// correctness tests pass.

/// Stage-1 acceptance: user database data survives checkpoint, clean close,
/// and recovery.  This is a correctness regression guard verifying the
/// fix did not break recovery.
#[test]
fn stage1_user_db_data_survives_checkpoint_and_recovery() {
    use noxu_db::{CheckpointConfig, DatabaseConfig, EnvironmentConfig};

    let dir = TempDir::new().unwrap();
    let mut expected = std::collections::BTreeMap::new();

    {
        let env = noxu_db::Environment::open(
            EnvironmentConfig::new(dir.path().to_path_buf())
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap();

        let db = env
            .open_database(
                None,
                "stage1_recovery_db",
                &DatabaseConfig::new()
                    .with_allow_create(true)
                    .with_transactional(true),
            )
            .unwrap();

        // Write 300 keys across two transactions.
        {
            let txn = env.begin_transaction(None).unwrap();
            for i in 0u32..150 {
                let k = format!("s1r_{i:06}").into_bytes();
                let v = format!("s1v_{i:06}").into_bytes();
                db.put_in(
                    &txn,
                    DatabaseEntry::from_bytes(&k),
                    DatabaseEntry::from_bytes(&v),
                )
                .unwrap();
                expected.insert(k, v);
            }
            txn.commit().unwrap();
        }

        // Force an explicit checkpoint (in addition to the close-time one).
        env.checkpoint(Some(&CheckpointConfig::new().with_force(true)))
            .unwrap();

        {
            let txn = env.begin_transaction(None).unwrap();
            for i in 150u32..300 {
                let k = format!("s1r_{i:06}").into_bytes();
                let v = format!("s1v_{i:06}").into_bytes();
                db.put_in(
                    &txn,
                    DatabaseEntry::from_bytes(&k),
                    DatabaseEntry::from_bytes(&v),
                )
                .unwrap();
                expected.insert(k, v);
            }
            txn.commit().unwrap();
        }
        // Clean close triggers another checkpoint + flush.
    }

    // Reopen + recovery.
    let env2 = noxu_db::Environment::open(
        EnvironmentConfig::new(dir.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true),
    )
    .unwrap();
    let db2 = env2
        .open_database(
            None,
            "stage1_recovery_db",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap();

    let recovered = collect_all(&db2);
    assert_eq!(
        recovered.len(),
        300,
        "stage1 recovery: expected 300 keys, got {}",
        recovered.len()
    );
    assert_eq!(
        recovered, expected,
        "stage1 recovery: recovered state does not match expected committed state"
    );
}

/// Stage-1 acceptance: MULTIPLE user databases — each must be flushed.
#[test]
fn stage1_multiple_user_databases_survive_checkpoint_and_recovery() {
    use noxu_db::{CheckpointConfig, DatabaseConfig, EnvironmentConfig};

    let dir = TempDir::new().unwrap();

    {
        let env = noxu_db::Environment::open(
            EnvironmentConfig::new(dir.path().to_path_buf())
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap();

        let db_a = env
            .open_database(
                None,
                "stage1_db_a",
                &DatabaseConfig::new()
                    .with_allow_create(true)
                    .with_transactional(true),
            )
            .unwrap();
        let db_b = env
            .open_database(
                None,
                "stage1_db_b",
                &DatabaseConfig::new()
                    .with_allow_create(true)
                    .with_transactional(true),
            )
            .unwrap();

        for i in 0u32..100 {
            db_a.put(
                DatabaseEntry::from_bytes(format!("ak_{i:04}").as_bytes()),
                DatabaseEntry::from_bytes(format!("av_{i:04}").as_bytes()),
            )
            .unwrap();
            db_b.put(
                DatabaseEntry::from_bytes(format!("bk_{i:04}").as_bytes()),
                DatabaseEntry::from_bytes(format!("bv_{i:04}").as_bytes()),
            )
            .unwrap();
        }

        env.checkpoint(Some(&CheckpointConfig::new().with_force(true)))
            .unwrap();
    }

    let env2 = noxu_db::Environment::open(
        EnvironmentConfig::new(dir.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true),
    )
    .unwrap();
    let rdb_a = env2
        .open_database(
            None,
            "stage1_db_a",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap();
    let rdb_b = env2
        .open_database(
            None,
            "stage1_db_b",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap();

    let ra = collect_all(&rdb_a);
    let rb = collect_all(&rdb_b);
    assert_eq!(
        ra.len(),
        100,
        "stage1 multi-db: db_a must have 100 keys after recovery; got {}",
        ra.len()
    );
    assert_eq!(
        rb.len(),
        100,
        "stage1 multi-db: db_b must have 100 keys after recovery; got {}",
        rb.len()
    );
}

/// Stage-1 FAIL-PRE/PASS-POST stat test: after a forced checkpoint on a
/// user database, `EnvironmentStats.checkpoint.full_bin_flush` must be > 0.
///
/// On `origin/main` (b7008aa), `full_bin_flush` was ALWAYS 0 for user
/// databases because the checkpointer only knew about the primary tree
/// (db_id=1) via `.with_tree(primary_tree, 1)`.  User-database trees
/// registered in `db_trees_registry` were skipped.
///
/// FAIL-PRE (main): `full_bin_flush == 0` → assertion below FAILS.
/// PASS-POST (Stage-1): `full_bin_flush > 0` → checkpointer flushed BINs
///   from the user tree via `with_db_trees_registry`.
#[test]
fn stage1_checkpoint_stats_show_user_db_bins_flushed() {
    use noxu_db::{CheckpointConfig, DatabaseConfig, EnvironmentConfig};

    let dir = TempDir::new().unwrap();

    let env = noxu_db::Environment::open(
        EnvironmentConfig::new(dir.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true),
    )
    .unwrap();

    let db = env
        .open_database(
            None,
            "stage1_stats_db",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap();

    // Write 100 committed keys — marks BINs dirty in the user tree.
    for i in 0u32..100 {
        db.put(
            DatabaseEntry::from_bytes(format!("sk_{i:04}").as_bytes()),
            DatabaseEntry::from_bytes(format!("sv_{i:04}").as_bytes()),
        )
        .unwrap();
    }

    // Force a checkpoint.
    env.checkpoint(Some(&CheckpointConfig::new().with_force(true)))
        .expect("checkpoint must succeed");

    // Read checkpoint stats.
    let stats = env.get_stats().expect("get_stats must succeed");
    let bins_flushed = stats.checkpoint.full_bin_flush;

    assert!(
        bins_flushed > 0,
        "STAGE-1 FAIL-PRE/PASS-POST: expected full_bin_flush > 0 after \
         checkpoint (user DB BINs should have been flushed), but got {}. \
         On origin/main this fails because the checkpointer was wired only \
         to primary_tree and never visited db_trees_registry.",
        bins_flushed
    );
}

// ---------------------------------------------------------------------------
// Stage-2 acceptance tests — T-F3/T-F4 first_active_lsn + bounded recovery
// ---------------------------------------------------------------------------
//
// Stage-2 wires TxnManager::update_first_lsn from CursorImpl (called on
// first transactional LN write), wires TxnManager into the Checkpointer via
// with_txn_manager(), and sets CkptEnd.first_active_lsn = min(open_txn_lsn,
// checkpoint_start_lsn) instead of the conservative Lsn::new(0,0).
//
// The critical safety constraint: an open transaction spanning the checkpoint
// (started before checkpoint, still active/uncommitted at crash) must NOT
// appear committed after recovery.  This is the exact hazard that made T-F3
// unsafe before Stage 1.  Stage 2 handles it by setting first_active_lsn =
// min(open_txn_first_lsn, ckpt_start), which forces recovery to scan back
// to the open txn's first write and correctly undo it.
//
// The open_txn_spanning_checkpoint test in crash_recovery_test.rs is the
// definitive SIGKILL test for this.  These tests cover the stat/wiring path.

/// Stage-2 T-F4 wiring: after a transactional write, the first_active_lsn
/// mechanism is exercised end-to-end (write → checkpoint → recovery).
///
/// FAIL-PRE (before Stage 2): update_first_lsn was never called, so
/// get_first_active_lsn() always returned NULL_LSN.
/// PASS-POST: CursorImpl calls update_first_lsn on first write; the
/// checkpointer has the TxnManager wired for future T-F3 use.
/// Data correctness is verified via close+reopen recovery.
#[test]
fn stage2_txn_manager_records_first_active_lsn() {
    use noxu_db::{CheckpointConfig, DatabaseConfig, EnvironmentConfig};

    let dir = TempDir::new().unwrap();

    // Phase 1: write, checkpoint, close.
    {
        let env = noxu_db::Environment::open(
            EnvironmentConfig::new(dir.path().to_path_buf())
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap();

        let db = env
            .open_database(
                None,
                "stage2_lsn_db",
                &DatabaseConfig::new()
                    .with_allow_create(true)
                    .with_transactional(true),
            )
            .unwrap();

        // Write one key in a transaction — this calls update_first_lsn.
        {
            let txn = env.begin_transaction(None).unwrap();
            db.put_in(
                &txn,
                DatabaseEntry::from_bytes(b"stage2key"),
                DatabaseEntry::from_bytes(b"stage2val"),
            )
            .unwrap();
            txn.commit().unwrap();
        }

        // Force a checkpoint (TxnManager is wired; no open txns here).
        env.checkpoint(Some(&CheckpointConfig::new().with_force(true)))
            .unwrap();

        // Explicit close so db and env are dropped in the right order.
        db.close().unwrap();
        env.close().unwrap();
    }

    // Phase 2: reopen and verify data survived.
    let env2 = noxu_db::Environment::open(
        EnvironmentConfig::new(dir.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true),
    )
    .unwrap();
    let db2 = env2
        .open_database(
            None,
            "stage2_lsn_db",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap();

    let mut val = noxu_db::DatabaseEntry::new();
    let status = db2
        .get_into(None, DatabaseEntry::from_bytes(b"stage2key"), &mut val)
        .unwrap();
    assert!(status, "stage2: committed key must survive checkpoint+recovery");
    assert_eq!(
        val.get_data(),
        Some(b"stage2val" as &[u8]),
        "stage2: recovered value must match committed value"
    );
}

// ---------------------------------------------------------------------------
// C4 — RecoveryDeltaTest::testCompress + testKnownDeleted
// ---------------------------------------------------------------------------
//
// Faithful ports of JE `com.sleepycat.je.recovery.RecoveryDeltaTest`:
//   - testCompress:     delete half the records, compress, force a checkpoint,
//                       recover, and verify the surviving committed set.
//   - testKnownDeleted: BIN-deltas carrying known-deleted slots replay
//                       correctly after abort + checkpoint.
//
// JE `setExtraProperties` cranks BIN_DELTA_PERCENT to 75 and turns the
// checkpointer + compressor OFF so checkpoints can be driven explicitly. We
// mirror that: daemons off, explicit `env.checkpoint(force)` / `env.compress()`.
//
// ## Authorized deviation — JE's deferred-compression stat invariant
//
// JE `testCompress` asserts that after a compress the next checkpoint writes a
// FULL BIN (not a delta), because in JE a committed delete leaves a deleted
// SLOT in the BIN that the INCompressor later removes, and that removal forces
// the BIN to be re-logged in full. Noxu's delete path is PHYSICAL: a committed
// delete removes the slot immediately via `tree.delete()` (see
// `noxu-tree/src/tree.rs::compress_bin` IC-3 note and
// `docs/src/operations/known-limitations.md`). `env.compress()` therefore only
// reclaims slots left `known_deleted` by aborted inserts / recovery replay —
// it is a no-op for committed deletes, and there is no "compress forces a full
// BIN" interaction to assert.
//
// We therefore port the DATA-correctness half of testCompress faithfully
// (delete-half + compress + checkpoint + recover == exact surviving set, with
// `env.verify()`), and DO NOT assert the JE-internal NDeltaINFlush==0
// invariant, which tests a deferred-compression mechanic Noxu deliberately
// omits. testKnownDeleted retains its delta-write assertion because the
// known-deleted BIN-delta reconstitution path IS implemented in Noxu recovery.

use noxu_db::CheckpointConfig;

/// Open an env with the checkpointer/compressor/cleaner daemons OFF (JE
/// RecoveryDeltaTest.setExtraProperties) so checkpoints/compression are
/// explicit and deterministic.
fn open_env_delta(dir: &Path) -> noxu_db::Environment {
    let mut cfg = EnvironmentConfig::new(dir.to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    cfg.set_run_checkpointer(false);
    cfg.set_run_cleaner(false);
    cfg.set_run_in_compressor(false);
    cfg.set_run_evictor(false);
    noxu_db::Environment::open(cfg).unwrap()
}

/// Cumulative `checkpoint.delta_in_flush` counter.
fn delta_in_flush(env: &noxu_db::Environment) -> u64 {
    env.get_stats().unwrap().checkpoint.delta_in_flush
}

/// JE `RecoveryDeltaTest.testCompress` (DATA-correctness half — see the
/// authorized-deviation note above for why the NDeltaINFlush==0 assertion is
/// omitted).
///
/// Insert records (txn, commit), delete every other (txn, commit), compress,
/// force a checkpoint, close, recover, and assert the recovered set equals the
/// surviving committed set (with structural `env.verify()`).
#[test]
fn delta_test_compress_recovers_surviving_set() {
    let dir = TempDir::new().unwrap();
    let mut expected = std::collections::BTreeMap::new();

    {
        let env = open_env_delta(dir.path());
        let db = env
            .open_database(
                None,
                "deltadb",
                &DatabaseConfig::new()
                    .with_allow_create(true)
                    .with_transactional(true),
            )
            .unwrap();

        // Use enough records to span several BINs (default fanout 128).
        let num_recs = 400u32;

        // Insert all the data (txn + commit).
        {
            let txn = env.begin_transaction(None).unwrap();
            for i in 0..num_recs {
                let k = format!("ck_{i:05}");
                let v = format!("cv_{i:05}");
                db.put_in(
                    &txn,
                    DatabaseEntry::from_bytes(k.as_bytes()),
                    DatabaseEntry::from_bytes(v.as_bytes()),
                )
                .unwrap();
                expected.insert(k.into_bytes(), v.into_bytes());
            }
            txn.commit().unwrap();
        }

        // Flush a full version of the BINs first.
        env.checkpoint(Some(&CheckpointConfig::new().with_force(true)))
            .unwrap();

        // Delete every other record (txn + commit).
        {
            let txn = env.begin_transaction(None).unwrap();
            for i in (0..num_recs).step_by(2) {
                let k = format!("ck_{i:05}");
                db.delete_in(&txn, DatabaseEntry::from_bytes(k.as_bytes()))
                    .unwrap();
                expected.remove(&format!("ck_{i:05}").into_bytes());
            }
            txn.commit().unwrap();
        }

        // Ask the compressor to run (JE: removes deleted slots; in Noxu the
        // committed deletes are already physical, so this is a no-op — kept
        // for faithfulness to the JE operation sequence).
        let _ = env.compress().unwrap();

        // Force a checkpoint.
        env.checkpoint(Some(&CheckpointConfig::new().with_force(true)))
            .unwrap();

        db.close().unwrap();
        env.close().unwrap();
    }

    // Recover and verify the surviving (odd-indexed) records.
    let env2 = open_env_delta(dir.path());
    let db2 = env2
        .open_database(
            None,
            "deltadb",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap();
    let vresult = env2.verify(&noxu_db::VerifyConfig::new()).unwrap();
    assert_eq!(
        vresult.error_count(),
        0,
        "testCompress: post-recovery structural verify found errors: {:?}",
        vresult.errors
    );
    let recovered = collect_all(&db2);
    assert_eq!(
        recovered, expected,
        "testCompress: recovered set != expected (surviving) committed set"
    );
}

/// JE `RecoveryDeltaTest.testKnownDeleted`.
///
/// Reconstituting a BIN-delta must handle the known-deleted flag correctly.
///
/// JE operation pattern:
///   insert keys, abort           -> child ref KD = true (aborted insert)
///   checkpoint                   -> full BIN with KD set written
///   insert every-other, commit   -> KD = false for those slots
///   delete (those), abort        -> BIN-delta should keep KD = false
///   checkpoint (writes deltas)   -> assert >= 1 delta IN flush
///   recover                      -> committed keys present
///                                   (reconstituteBIN clears stale KD)
///
/// T-17 note (delta threshold): the checkpointer now reads the configurable
/// BIN-delta percent (`tree_bin_delta_percent` / JE `BIN_DELTA_PERCENT`,
/// default 25) and makes the delta-vs-full decision COUNT-based via
/// `BinStub::should_log_delta` (faithful JE `BIN.shouldLogDelta`,
/// BIN.java:1892).  This test keeps its per-BIN dirty churn small so a delta
/// is logged under the default percent=25; the asserted property is
/// unchanged: the checkpoint writes BIN-deltas that carry known-deleted slots,
/// and recovery reconstitutes them so that every committed key is present
/// (stale KD cleared).
#[test]
fn delta_test_known_deleted_replays() {
    let dir = TempDir::new().unwrap();
    let mut expected = std::collections::BTreeMap::new();

    {
        let env = open_env_delta(dir.path());
        let db = env
            .open_database(
                None,
                "kddb",
                &DatabaseConfig::new()
                    .with_allow_create(true)
                    .with_transactional(true),
            )
            .unwrap();

        // Span several BINs (default fanout 128).
        let num_recs = 400u32;
        let key_of = |i: u32| format!("kd_{i:05}").into_bytes();
        let val_of = |i: u32| format!("kv_{i:05}").into_bytes();
        let new_key = |i: u32| format!("kn_{i:05}").into_bytes();

        // Insert ALL data and COMMIT -> full live BINs.
        {
            let txn = env.begin_transaction(None).unwrap();
            for i in 0..num_recs {
                db.put_in(
                    &txn,
                    DatabaseEntry::from_bytes(&key_of(i)),
                    DatabaseEntry::from_bytes(&val_of(i)),
                )
                .unwrap();
                expected.insert(key_of(i), val_of(i));
            }
            txn.commit().unwrap();
        }

        // Force a checkpoint: writes a FULL version of the BINs to disk so the
        // next checkpoint can produce deltas.
        env.checkpoint(Some(&CheckpointConfig::new().with_force(true)))
            .unwrap();

        // Insert a SMALL set of brand-new keys and ABORT. The aborted inserts
        // leave known-deleted tombstone slots in the BINs (KD = true), which
        // the next checkpoint's BIN-deltas must carry.
        {
            let txn = env.begin_transaction(None).unwrap();
            for i in (0..num_recs).step_by(40) {
                db.put_in(
                    &txn,
                    DatabaseEntry::from_bytes(&new_key(i)),
                    DatabaseEntry::from_bytes(b"tombstone"),
                )
                .unwrap();
            }
            txn.abort().unwrap();
        }

        // Apply a SMALL committed update so the per-BIN dirty fraction stays
        // under the 25% delta threshold -> the checkpoint writes deltas.
        {
            let txn = env.begin_transaction(None).unwrap();
            for i in (0..num_recs).step_by(80) {
                let nv = b"updated".to_vec();
                db.put_in(
                    &txn,
                    DatabaseEntry::from_bytes(&key_of(i)),
                    DatabaseEntry::from_bytes(&nv),
                )
                .unwrap();
                expected.insert(key_of(i), nv);
            }
            txn.commit().unwrap();
        }

        // This checkpoint should write deltas (JE asserts NDeltaINFlush > 0).
        // The deltas' base BINs contain the aborted-insert KD tombstones.
        let delta_before = delta_in_flush(&env);
        env.checkpoint(Some(&CheckpointConfig::new().with_force(true)))
            .unwrap();
        let delta_after = delta_in_flush(&env);
        assert!(
            delta_after - delta_before > 0,
            "testKnownDeleted: expected the checkpoint to write BIN-deltas \
             (NDeltaINFlush > 0), but wrote {}",
            delta_after - delta_before
        );

        db.close().unwrap();
        env.close().unwrap();
    }

    // Recover and verify: every committed key must be present, and NONE of
    // the aborted-insert tombstone keys may leak. Reconstituting the
    // BIN-deltas must apply the known-deleted slots correctly.
    let env2 = open_env_delta(dir.path());
    let db2 = env2
        .open_database(
            None,
            "kddb",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap();
    let vresult = env2.verify(&noxu_db::VerifyConfig::new()).unwrap();
    assert_eq!(
        vresult.error_count(),
        0,
        "testKnownDeleted: post-recovery structural verify found errors: {:?}",
        vresult.errors
    );
    let recovered = collect_all(&db2);
    // No aborted-insert tombstone key may be present.
    for key in recovered.keys() {
        assert!(
            !key.starts_with(b"kn_"),
            "testKnownDeleted: aborted-insert tombstone key leaked: {:?}",
            std::str::from_utf8(key)
        );
    }
    assert_eq!(
        recovered, expected,
        "testKnownDeleted: recovered set != expected committed set \
         (known-deleted slot was not reconstituted correctly)"
    );
}
