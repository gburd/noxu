//! JE `DeferredWriteTest` crash cases — real SIGKILL subprocess ports.
//!
//! JE: `test/com/sleepycat/je/test/DeferredWriteTest.java`.  The distinguishing
//! deferred-write property: records written WITHOUT a sync are LOST after an
//! abnormal (crash) close; records written before the last `db.sync()` survive.
//! JE drives this with `DbInternal.getNonNullEnvImpl(env).abnormalClose()`;
//! Noxu uses the `crash_worker` subprocess + SIGKILL harness (a strictly
//! stronger crash than JE's in-process abnormalClose), the same harness
//! `crash_recovery_test.rs` / `je_recovery_test.rs` use.
//!
//!   - `testRecoverNoSync` / `testCloseOpenNoSync` — mode `deferred_write_nosync`.
//!   - `testRecoverNoSyncEvict` — mode `deferred_write_nosync_evict`.
//!   - `testPreloadNoSync` — mode `deferred_write_nosync` then preload on reopen.

use noxu_db::{DatabaseConfig, DatabaseEntry, EnvironmentConfig};
use std::path::Path;
use std::time::{Duration, Instant};
use tempfile::TempDir;

fn wait_for_flag(dir: &Path, name: &str, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    let flag = dir.join(name);
    while Instant::now() < deadline {
        if flag.exists() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    false
}

fn crash_worker_exe() -> &'static str {
    env!("CARGO_BIN_EXE_crash_worker")
}

/// Spawn the crash worker in `mode`, SIGKILL it after the unsynced batch
/// starts, reopen the env, and assert deferred-write crash semantics:
/// synced records present, unsynced records gone, structure intact.
/// `preload` additionally calls `db.preload()` after reopen (testPreloadNoSync).
fn run_dw_crash(mode: &str, preload: bool) {
    let dir = TempDir::new().unwrap();
    let dir_path = dir.path().to_path_buf();

    let mut child = std::process::Command::new(crash_worker_exe())
        .env("NOXU_CRASH_DIR", &dir_path)
        .env("NOXU_CRASH_MODE", mode)
        .spawn()
        .expect("spawn crash_worker");

    assert!(
        wait_for_flag(&dir_path, "dw_phase1_synced", Duration::from_secs(60)),
        "worker never finished the synced phase"
    );
    assert!(
        wait_for_flag(
            &dir_path,
            "dw_unsynced_started",
            Duration::from_secs(30)
        ),
        "worker never started the unsynced phase"
    );

    // SIGKILL mid-unsynced-batch (strictly stronger than JE abnormalClose).
    child.kill().expect("SIGKILL worker");
    child.wait().expect("wait for killed worker");

    // Reopen -> crash recovery.
    let env = noxu_db::Environment::open(
        EnvironmentConfig::new(dir_path)
            .with_allow_create(true)
            .with_transactional(true),
    )
    .expect("reopen env after crash");
    let dw = env
        .open_database(
            None,
            "dw",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_deferred_write(true),
        )
        .expect("reopen dw db after crash");

    if preload {
        dw.preload(&noxu_db::preload::PreloadConfig::new())
            .expect("preload after crash");
    }

    // Synced batch 0..50 must be present with value "synced".
    let mut missing = 0u32;
    for i in 0u32..50 {
        let mut v = DatabaseEntry::new();
        if dw.get_into(None, i.to_be_bytes(), &mut v).unwrap() {
            assert_eq!(v.data(), b"synced", "synced key {i} wrong value");
        } else {
            missing += 1;
        }
    }
    assert_eq!(
        missing, 0,
        "{missing} SYNCED deferred-write records lost after crash"
    );

    // Unsynced batch 1000..1050 must be GONE (deferred-write, never synced).
    let mut leaked = 0u32;
    for i in 1000u32..1050 {
        let mut v = DatabaseEntry::new();
        if dw.get_into(None, i.to_be_bytes(), &mut v).unwrap() {
            leaked += 1;
        }
    }
    assert_eq!(
        leaked, 0,
        "{leaked} UNSYNCED deferred-write records survived crash (must be lost)"
    );

    // Structural integrity after recovery (JE CheckBase.recoverAndLoadData).
    let vr =
        env.verify(&noxu_db::VerifyConfig::new()).expect("verify after crash");
    assert_eq!(
        vr.error_count(),
        0,
        "post-crash structural verify errors: {:?}",
        vr.errors
    );
}

/// JE `DeferredWriteTest.testRecoverNoSync` / `testCloseOpenNoSync`.
#[test]
fn je_deferred_write_test_test_recover_no_sync() {
    run_dw_crash("deferred_write_nosync", false);
}

/// JE `DeferredWriteTest.testRecoverNoSyncEvict`.
///
/// BUG-DW-SYNC-1 FIXED: deferred-write LNs are now logged (JE `BIN.logDirtyLN` /
/// `IN.logDirtyChildren`) before the BIN image during a sync/checkpoint/eviction
/// flush, so every live slot carries a real LSN and the recovered tree is
/// durable.  The structural verifier now fetches non-resident child BINs from
/// their slot LSN (JE `BtreeVerifier.getINFromFile`) instead of flagging a
/// lazily-fetched child as a null reference.
#[test]
fn je_deferred_write_test_test_recover_no_sync_evict() {
    run_dw_crash("deferred_write_nosync_evict", false);
}

/// JE `DeferredWriteTest.testPreloadNoSync`.
#[test]
fn je_deferred_write_test_test_preload_no_sync() {
    run_dw_crash("deferred_write_nosync", true);
}

/// Dedicated recovery test: a MULTI-LEVEL (root-split) deferred-write tree made
/// durable by db.sync() (no checkpoint) must recover its root + all synced data
/// after a crash — the upper-IN is-root path.  (JE: syncDatabase flushes the
/// whole dirty set bottom-up, root included, whatever its level.)  Uses
/// 512-byte NON-embedded values, exercising the non-embedded deferred-write LN
/// logging path (BUG-DW-SYNC-1).
#[test]
fn je_deferred_write_test_root_split_sync_then_crash() {
    let dir = TempDir::new().unwrap();
    let dir_path = dir.path().to_path_buf();
    let mut child = std::process::Command::new(crash_worker_exe())
        .env("NOXU_CRASH_DIR", &dir_path)
        .env("NOXU_CRASH_MODE", "root_split_sync")
        .spawn()
        .expect("spawn crash_worker");
    assert!(wait_for_flag(
        &dir_path,
        "dw_phase1_synced",
        Duration::from_secs(60)
    ));
    assert!(wait_for_flag(
        &dir_path,
        "dw_unsynced_started",
        Duration::from_secs(30)
    ));
    child.kill().expect("SIGKILL");
    child.wait().expect("wait");

    let env = noxu_db::Environment::open(
        EnvironmentConfig::new(dir_path)
            .with_allow_create(true)
            .with_transactional(true),
    )
    .expect("reopen");
    let dw = env
        .open_database(
            None,
            "dw",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_deferred_write(true),
        )
        .expect("reopen dw");
    // All 300 synced keys present (the split tree + root recovered).
    let mut missing = 0u32;
    for i in 0u32..300 {
        let mut v = DatabaseEntry::new();
        if !dw.get_into(None, i.to_be_bytes(), &mut v).unwrap() {
            missing += 1;
        }
    }
    assert_eq!(
        missing, 0,
        "{missing} synced keys lost from a split deferred-write tree"
    );
    // Unsynced gone.
    let mut leaked = 0u32;
    for i in 1000u32..1050 {
        let mut v = DatabaseEntry::new();
        if dw.get_into(None, i.to_be_bytes(), &mut v).unwrap() {
            leaked += 1;
        }
    }
    assert_eq!(leaked, 0, "{leaked} unsynced keys survived");
    let vr = env.verify(&noxu_db::VerifyConfig::new()).expect("verify");
    assert_eq!(vr.error_count(), 0, "structural verify: {:?}", vr.errors);
}

/// Recovery-correctness witness (BUG-DW-SYNC-1 task 3): a deferred-write DB made
/// durable with `db.sync()` (so its db_id lands in `analysis.dirty_ins`), then
/// REMOVED and the removal committed, then crashed, must STAY REMOVED after
/// recovery.  The "instantiate a tree for every db in dirty_ins" rule must not
/// resurrect a removed database.  JE skips a removed DB's stale logged INs
/// because `dbMapTree.getDb(dbId)` returns null (RecoveryManager.java:1091-1121).
// Depends on BUG-DDL-DURABILITY-1 (A12): a DB removal committed in a txn must
// be fsynced so recovery sees the NameLN-delete; otherwise the removed DB is
// resurrected and this A13 dirty_ins tree-instantiation revives it.  At this
// base a remove-only txn's commit skips the fsync (has_logged_data()==false),
// so the removal is lost on crash and the DB reopens.  The A13 recovery guard
// (gate dirty_ins on recovered_db_names) is in place and correct, but it can
// only drop a removed DB once the removal is durable.  Un-ignored by A12's
// DDL-durability fsync (4a449d51 STOPGAP / e57b92ea DDL part).
#[test]
#[ignore = "needs BUG-DDL-DURABILITY-1 (A12): remove-only txn commit must \
            fsync the NameLN-delete or recovery resurrects the removed DB; \
            A13 recovery guard is in place, un-ignore after A12"]
fn je_deferred_write_test_sync_then_remove_stays_removed() {
    let dir = TempDir::new().unwrap();
    let dir_path = dir.path().to_path_buf();
    let mut child = std::process::Command::new(crash_worker_exe())
        .env("NOXU_CRASH_DIR", &dir_path)
        .env("NOXU_CRASH_MODE", "dw_sync_then_remove")
        .spawn()
        .expect("spawn crash_worker");
    assert!(wait_for_flag(
        &dir_path,
        "dw_unsynced_started",
        Duration::from_secs(60)
    ));
    child.kill().expect("SIGKILL");
    child.wait().expect("wait");

    let env = noxu_db::Environment::open(
        EnvironmentConfig::new(dir_path)
            .with_allow_create(true)
            .with_transactional(true),
    )
    .expect("reopen env after crash");

    // The removed DB must NOT be openable without allow_create (it is gone).
    let reopen = env.open_database(
        None,
        "dw",
        &DatabaseConfig::new().with_deferred_write(true),
    );
    assert!(
        reopen.is_err(),
        "removed deferred-write DB was RESURRECTED by recovery (dirty_ins must \
         not revive a removed database)"
    );

    // The whole environment must still verify cleanly (no stale/orphan tree).
    let vr = env.verify(&noxu_db::VerifyConfig::new()).expect("verify");
    assert_eq!(
        vr.error_count(),
        0,
        "post-crash structural verify errors after remove: {:?}",
        vr.errors
    );
}
