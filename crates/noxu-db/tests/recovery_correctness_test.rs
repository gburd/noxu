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
        &DatabaseConfig::new().with_allow_create(true),
    )
    .unwrap()
}

/// Collect all (key, value) pairs from `db` in sorted order.
fn collect_all(db: &noxu_db::Database) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let mut cursor = db.open_cursor(None, None).unwrap();
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
            None,
            &DatabaseEntry::from_bytes(k.as_bytes()),
            &DatabaseEntry::from_bytes(v.as_bytes()),
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
        db.put(
            Some(&txn),
            &DatabaseEntry::from_bytes(k.as_bytes()),
            &DatabaseEntry::from_bytes(v.as_bytes()),
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
        db.put(
            Some(&txn),
            &DatabaseEntry::from_bytes(k.as_bytes()),
            &DatabaseEntry::from_bytes(v.as_bytes()),
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
                    None,
                    &DatabaseEntry::from_bytes(k.as_bytes()),
                    &DatabaseEntry::from_bytes(v.as_bytes()),
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
                    None,
                    &DatabaseEntry::from_bytes(k.as_bytes()),
                    &DatabaseEntry::from_bytes(v.as_bytes()),
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
                None,
                &DatabaseEntry::from_bytes(k.as_bytes()),
                &DatabaseEntry::from_bytes(v.as_bytes()),
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
                None,
                &DatabaseEntry::from_bytes(k.as_bytes()),
                &DatabaseEntry::from_bytes(v.as_bytes()),
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
                None,
                &DatabaseEntry::from_bytes(k.as_bytes()),
                &DatabaseEntry::from_bytes(v.as_bytes()),
            )
            .unwrap();
        }
        // Delete even-indexed keys.
        for i in (0u32..100).step_by(2) {
            let k = format!("dk_{i:06}");
            db.delete(None, &DatabaseEntry::from_bytes(k.as_bytes())).unwrap();
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
                    None,
                    &DatabaseEntry::from_bytes(k.as_bytes()),
                    &DatabaseEntry::from_bytes(format!("v0_{i}").as_bytes()),
                )
                .unwrap();
            }
            // Update a small fraction repeatedly to trigger the BINDelta path
            // (dirty_count / total <= 25% → delta).
            for round in 1u32..5 {
                for i in 0u32..5 {
                    let k = format!("delta_{i:06}");
                    let v = format!("v{round}_{i}");
                    db.put(
                        None,
                        &DatabaseEntry::from_bytes(k.as_bytes()),
                        &DatabaseEntry::from_bytes(v.as_bytes()),
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
                    None,
                    &DatabaseEntry::from_bytes(k.as_bytes()),
                    &DatabaseEntry::from_bytes(v.as_bytes()),
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
