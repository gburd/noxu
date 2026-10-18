//! XA-RECOVERED — candidate S1 data-loss witness (two-crash, SIGKILL).
//!
//! A prepared XA transaction that survives a crash is recovered as an in-doubt
//! txn.  Its LNs are NOT redone into the tree (invisible until resolution) and
//! the recovered prepared txn is NOT registered with the TxnManager
//! (`write_txn_commit_for_recovered` / `apply_recovered_prepared_lns` resolve it
//! without a sink-wired `Txn`), so the cleaner's first-active-txn clamp does not
//! protect the prepared LNs' file.  If a cleaner pass reclaims that file between
//! env-open and `xa_commit`, the slot installed by `apply_recovered_prepared_lns`
//! points at `ln.original_lsn` in a CLEANED file.  A SECOND crash before a
//! checkpoint flushes the BIN leaves recovery unable to rebuild the slot from
//! the log -> committed data lost.
//!
//! JE routes a recovered prepared txn through the TxnManager (PreparedTxn extends
//! Txn, registered in allTxns; RecoveryManager redoOneLN -> relock ->
//! txn.addLogInfo(logLsn)), so JE's getFirstActiveLsn covers it and the cleaner
//! clamp protects its files during the resolution window.
//!
//! GREEN = keys survive the second crash (bug absent / fixed).
//! RED   = keys lost after the second crash (S1 data loss).

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
    // Each phase removes the previous phase's done-flag so the next SIGKILL
    // waits on a fresh signal.
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

/// Prepared XA txn -> crash -> recover -> cleaner reclaims the prepared LNs'
/// file -> xa_commit (recovered path) -> crash before checkpoint -> reopen:
/// the 8 committed XA keys must still be present.
#[test]
fn xa_recovered_commit_survives_second_crash() {
    let dir = TempDir::new().unwrap();
    let dir_path = dir.path().to_path_buf();

    // Crash #1: prepare the XA txn, roll files, SIGKILL.
    run_phase(&dir_path, "xarec_prepare");
    // Crash #2: recover, cleaner barrier reclaims the prepared file, resolve
    // (apply + TxnCommit), SIGKILL before any checkpoint.
    run_phase(&dir_path, "xarec_commit");

    // Final reopen: recovery must restore the committed XA keys.
    let env = Environment::open(
        EnvironmentConfig::new(dir_path)
            .with_allow_create(false)
            .with_transactional(true),
    )
    .expect("environment must reopen after the second crash");
    let db = env
        .open_database(
            None,
            "test",
            &DatabaseConfig::new().with_transactional(true),
        )
        .expect("db 'test' must reopen");
    let txn = env.begin_transaction(None).expect("read txn");
    let mut present = 0u32;
    for i in 0u32..8 {
        // Read the SAME range the prepared txn wrote (100_000..) and verify
        // the CORRECT committed value, so a wrong/stale value fails too.
        let want = format!("xa_payload_{i}").into_bytes();
        match db.get_in(&txn, (100_000u32 + i).to_be_bytes()).expect("get") {
            Some(v) if &v[..] == want.as_slice() => present += 1,
            Some(v) => panic!(
                "XA-RECOVERED: key {i} present but WRONG value: got {:?}, \
                 want {:?}",
                v, want
            ),
            None => {}
        }
    }
    txn.commit().unwrap();
    db.close().unwrap();
    env.close().unwrap();
    assert_eq!(
        present, 8,
        "XA-RECOVERED: all 8 committed XA keys must survive the second crash; \
         a lower count means the recovered prepared txn's LNs were in a file \
         reclaimed by the cleaner before xa_commit, and recovery could not \
         rebuild the installed slot (S1 data loss)"
    );
}

/// R-XARECOVERED witness #6: a THIRD crash in the in-doubt window.
/// prepare -> SIGKILL -> recover (redo+relock) + cleaner barrier -> SIGKILL
/// while STILL in-doubt -> recover again -> xa_commit -> SIGKILL before any
/// checkpoint -> reopen: the 8 committed XA keys must still be present.  This
/// exercises a crash AFTER the redo+relock but BEFORE resolution, and another
/// AFTER resolution but BEFORE a checkpoint flushes the TxnCommit's effect.
#[test]
fn xa_recovered_survives_third_crash_in_doubt_window() {
    let dir = TempDir::new().unwrap();
    let dir_path = dir.path().to_path_buf();

    run_phase(&dir_path, "xarec_prepare"); // crash #1
    run_phase(&dir_path, "xarec_recover_only"); // crash #2 (still in-doubt)
    run_phase(&dir_path, "xarec_commit"); // crash #3 (resolved, pre-ckpt)

    let env = Environment::open(
        EnvironmentConfig::new(dir_path)
            .with_allow_create(false)
            .with_transactional(true),
    )
    .expect("environment must reopen after the third crash");
    let db = env
        .open_database(
            None,
            "test",
            &DatabaseConfig::new().with_transactional(true),
        )
        .expect("db 'test' must reopen");
    let txn = env.begin_transaction(None).expect("read txn");
    let mut present = 0u32;
    for i in 0u32..8 {
        let want = format!("xa_payload_{i}").into_bytes();
        match db.get_in(&txn, (100_000u32 + i).to_be_bytes()).expect("get") {
            Some(v) if &v[..] == want.as_slice() => present += 1,
            Some(v) => panic!("key {i} WRONG value: {:?} != {:?}", v, want),
            None => {}
        }
    }
    txn.commit().unwrap();
    db.close().unwrap();
    env.close().unwrap();
    assert_eq!(
        present, 8,
        "XA-RECOVERED: all 8 keys must survive a crash in the in-doubt window \
         followed by resolution and a pre-checkpoint crash"
    );
}
