//! R-XARECOVERED FINDING F1 witness (durable recovered rollback), SIGKILL via
//! the crash_worker harness (a `drop` can flush the log buffer and mask the
//! bug; only a real crash is faithful).
//!
//! prepare 8 keys -> SIGKILL -> recover (redo+relock) + xa_rollback-equivalent
//! (undo + write_txn_abort_for_recovered) -> SIGKILL BEFORE any checkpoint ->
//! reopen: the branch MUST be resolved (no in-doubt txn surfaces) and the keys
//! MUST be absent.
//!
//! RED (abort not fsync'd): the branch resurfaces in-doubt / keys reappear.
//! GREEN (abort force-flushed, JE Txn.abort(xid)=abort(true/forceFlush)):
//! branch resolved, keys absent.

use noxu_db::{DatabaseConfig, Environment, EnvironmentConfig};
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

fn run_phase(dir: &Path, mode: &str) {
    let _ = std::fs::remove_file(dir.join("firstlsn_done"));
    let mut child =
        std::process::Command::new(env!("CARGO_BIN_EXE_crash_worker"))
            .env("NOXU_CRASH_DIR", dir)
            .env("NOXU_CRASH_MODE", mode)
            .spawn()
            .expect("spawn crash_worker");
    assert!(
        wait_for_flag(dir, "firstlsn_done", Duration::from_secs(120)),
        "worker never finished (mode {mode})"
    );
    child.kill().expect("SIGKILL");
    child.wait().expect("wait");
}

/// Prepared XA txn -> SIGKILL -> recover + rollback -> SIGKILL before any
/// checkpoint -> reopen: branch resolved, keys absent (durable rollback).
#[test]
fn recovered_prepared_rollback_durable_across_sigkill() {
    let dir = TempDir::new().unwrap();
    let p = dir.path().to_path_buf();

    run_phase(&p, "xarec_prepare"); // crash #1
    run_phase(&p, "xarec_rollback"); // crash #2 (rolled back, pre-checkpoint)

    let env = Environment::open(
        EnvironmentConfig::new(p)
            .with_allow_create(false)
            .with_transactional(true),
    )
    .expect("reopen after the rollback crash");
    let db = env
        .open_database(
            None,
            "test",
            &DatabaseConfig::new().with_transactional(true),
        )
        .expect("db reopen");

    // No in-doubt txn must survive: the TxnAbort must be durable.
    let in_doubt = env.recovered_prepared_txns();
    assert!(
        in_doubt.is_empty(),
        "a recovered prepared xa_rollback must be DURABLE across a crash: the \
         branch resurfaced in-doubt ({} txns) — the resolving TxnAbort was not \
         fsync'd/flushed (JE Txn.abort(xid) force-flushes)",
        in_doubt.len()
    );

    // The rolled-back keys must be absent.
    let txn = env.begin_transaction(None).expect("read txn");
    let mut present = 0u32;
    for i in 0u32..8 {
        if db
            .get_in(&txn, (100_000u32 + i).to_be_bytes())
            .expect("get")
            .is_some()
        {
            present += 1;
        }
    }
    txn.commit().unwrap();
    db.close().unwrap();
    env.close().unwrap();
    assert_eq!(
        present, 0,
        "a durably rolled-back recovered prepared txn must leave 0/8 keys; \
         {present}/8 reappeared (abort not durable -> redo resurrected them)"
    );
}
