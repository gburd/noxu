// Copyright (C) 2024-2026 Greg Burd.  Apache-2.0 OR MIT.
//! FID-NONTXN-SYNC-FSYNC crash survival — a NON-transactional db's writes are
//! made durable only by `Environment::sync()` (or a checkpoint / flushLog /
//! close), NOT per operation.
//!
//! JE: a non-txn write logs the LN with `flushRequired=false,
//! fsyncRequired=false` (`LN.logInternal`, LN.java:636; `LogParams.java:42,53`)
//! and `ThreadLocker.operationEnd` only releases locks (`BasicLocker.java:
//! 198-247`).  So records written before the last `Environment.sync()`
//! (`Environment.java:1603`, a forced checkpoint) SURVIVE an abnormal close;
//! records written after it are LOST — the same property deferred-write dbs
//! have, but for a plain non-transactional db.
//!
//! Driven by the `crash_worker` subprocess + SIGKILL harness (strictly
//! stronger than JE's in-process `abnormalClose`), crash mode
//! `nontxn_sync_survives`, exactly as `je_deferred_write_crash_test.rs`.
//!
//!   * RED before the fix: every non-txn put fsynced, so the "unsynced" batch
//!     was ALSO durable and survived the crash (the assertion that they are
//!     lost fails — the write was more durable than JE's).
//!   * GREEN after the fix: non-txn puts are not fsynced, so only the
//!     `env.sync()`-ed batch survives; the unsynced batch is lost, matching JE.

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

#[test]
fn nontxn_synced_survive_unsynced_lost_after_crash() {
    let dir = TempDir::new().unwrap();
    let dir_path = dir.path().to_path_buf();

    let mut child = std::process::Command::new(crash_worker_exe())
        .env("NOXU_CRASH_DIR", &dir_path)
        .env("NOXU_CRASH_MODE", "nontxn_sync_survives")
        .spawn()
        .expect("spawn crash_worker");

    assert!(
        wait_for_flag(
            &dir_path,
            "nontxn_phase1_synced",
            Duration::from_secs(60)
        ),
        "worker never finished the env.sync()'d phase",
    );
    assert!(
        wait_for_flag(
            &dir_path,
            "nontxn_unsynced_started",
            Duration::from_secs(30)
        ),
        "worker never started the unsynced phase",
    );

    // SIGKILL mid-unsynced-batch.
    child.kill().expect("SIGKILL worker");
    child.wait().expect("wait for killed worker");

    // Reopen -> crash recovery.
    let env = noxu_db::Environment::open(
        EnvironmentConfig::new(dir_path)
            .with_allow_create(true)
            .with_transactional(true),
    )
    .expect("reopen env after crash");
    let nt = env
        .open_database(
            None,
            "nontxn",
            &DatabaseConfig::new().with_allow_create(true),
        )
        .expect("reopen non-txn db after crash");

    // Synced batch 0..50 must be present ("synced"): env.sync() made them durable.
    let mut missing = 0u32;
    for i in 0u32..50 {
        let mut v = DatabaseEntry::new();
        if nt.get_into(None, i.to_be_bytes(), &mut v).unwrap() {
            assert_eq!(v.data(), b"synced", "synced key {i} wrong value");
        } else {
            missing += 1;
        }
    }
    assert_eq!(
        missing, 0,
        "{missing} env.sync()'d non-txn records lost after crash",
    );

    // Unsynced batch 1000..1050 must be GONE: a non-txn write is not fsynced,
    // so after the last env.sync() these records were never durable.  (RED
    // before the fix, where every non-txn put fsynced them durable.)
    let mut leaked = 0u32;
    for i in 1000u32..1050 {
        let mut v = DatabaseEntry::new();
        if nt.get_into(None, i.to_be_bytes(), &mut v).unwrap() {
            leaked += 1;
        }
    }
    assert_eq!(
        leaked, 0,
        "{leaked} UNSYNCED non-txn records survived crash — a non-txn write \
         must NOT be fsynced per operation (JE ThreadLocker; \
         FID-NONTXN-SYNC-FSYNC)",
    );

    // Structural integrity after recovery.
    let vr =
        env.verify(&noxu_db::VerifyConfig::new()).expect("verify after crash");
    assert_eq!(
        vr.error_count(),
        0,
        "post-crash structural verify errors: {:?}",
        vr.errors
    );
}
