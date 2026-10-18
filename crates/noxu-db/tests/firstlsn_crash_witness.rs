//! R-FIRSTLSN — candidate S1 data-loss witness (SIGKILL crash recovery).
//!
//! `TxnManager::get_first_active_lsn()` — which the cleaner's
//! first-active-txn-file clamp reads (`noxu-cleaner/src/cleaner.rs:949`) and
//! which bounds the recovery redo/undo scan (`recovery_manager.rs`) — must
//! reflect a transaction's FIRST logged LSN no matter which log write recorded
//! it.  Before the fix it was fed ONLY by `CursorImpl`'s user-LN write path
//! (`TxnManager::update_first_lsn`, `cursor_impl.rs:1403`).  A transactional
//! NameLN write (create/rename/remove DB inside a txn) records its LSN through
//! `Txn::note_log_entry` alone — never `update_first_lsn` — so a txn whose
//! FIRST log entry was a NameLN left the manager-visible first-active LSN at
//! NULL.  The cleaner clamp therefore did not protect the log file holding the
//! OPEN txn's provisional NameLN; nothing else did either (`relog_live_catalog`
//! re-logs only COMMITTED `name_map` entries, not an open txn's `pending_names`
//! entry).  After commit + SIGKILL, recovery cannot find the NameLN and the
//! database is lost.
//!
//! JE: every transactional LN log entry calls `txn.addLogInfo(justLoggedLsn)`
//! in `LNLogEntry.postLogWork` (LNLogEntry.java:~890-898, under the log latch)
//! -> `Txn.addLogInfo` sets `firstLoggedLsn` if NULL (Txn.java:1443-1456) ->
//! `TxnManager.getFirstActiveLsn` (:355-366) reads it.  Noxu's fix makes
//! `Txn::note_log_entry` (the single chokepoint every transactional write
//! funnels through) record the first LSN into the manager's shared `all_txns`
//! sink, exactly as JE's `addLogInfo` does.
//!
//! These are real SIGKILL subprocess ports (crash_worker modes `firstlsn_*`).
//! The worker forces the deletion of the OPEN txn's NameLN file via the
//! two-checkpoint barrier WHILE the txn is open, commits (or aborts), signals
//! `firstlsn_done`, then loops; the parent SIGKILLs it and reopens the env.
//!
//! Scope note: the CREATE-in-txn committed case is the clean, isolated
//! R-FIRSTLSN data-loss witness here (RED on rc2, GREEN with the fix); the
//! remove/rename/truncate NameLN-file protection is proved deterministically
//! and in isolation by `firstlsn_clamp_probe.rs` (file-survival, no recovery
//! confound).  The abort variants prove the fix does not break abort/undo.

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

fn crash_worker_exe() -> &'static str {
    env!("CARGO_BIN_EXE_crash_worker")
}

/// Spawn `mode`, wait for `firstlsn_done`, SIGKILL, reopen, run `check`.
fn run_firstlsn_crash(mode: &str, check: impl FnOnce(&Environment)) {
    let dir = TempDir::new().unwrap();
    let dir_path = dir.path().to_path_buf();

    let mut child = std::process::Command::new(crash_worker_exe())
        .env("NOXU_CRASH_DIR", &dir_path)
        .env("NOXU_CRASH_MODE", mode)
        .spawn()
        .expect("spawn crash_worker");

    assert!(
        wait_for_flag(&dir_path, "firstlsn_done", Duration::from_secs(120)),
        "worker never finished the DDL forcing scenario (mode {mode})"
    );
    child.kill().expect("SIGKILL");
    child.wait().expect("wait");

    let env = Environment::open(
        EnvironmentConfig::new(dir_path)
            .with_allow_create(false)
            .with_transactional(true),
    )
    .expect("environment must reopen after SIGKILL");
    check(&env);
    env.close().unwrap();
}

fn open_existing(
    env: &Environment,
    name: &str,
) -> Result<noxu_db::Database, ()> {
    env.open_database(
        None,
        name,
        &DatabaseConfig::new().with_transactional(true),
    )
    .map_err(|_| ())
}

// ─── create-in-txn: the isolated S1 data-loss witness ───────────────────────

#[test]
fn create_in_txn_committed_survives_crash() {
    run_firstlsn_crash("firstlsn_create_commit", |env| {
        let db = open_existing(env, "victim").expect(
            "committed create-in-txn DB 'victim' must survive the crash — if \
             it is gone, the cleaner deleted the log file holding its NameLNTxn \
             while the txn was open (first-active clamp under-reported)",
        );
        db.close().unwrap();
    });
}

#[test]
fn create_in_txn_aborted_leaves_env_healthy() {
    run_firstlsn_crash("firstlsn_create_abort", |env| {
        assert!(
            open_existing(env, "victim").is_err(),
            "aborted create-in-txn must NOT leave 'victim' present"
        );
        let filler = open_existing(env, "test")
            .expect("filler DB 'test' must survive — env must be healthy");
        filler.close().unwrap();
    });
}

// ─── abort variants: the fix must not break abort/undo ──────────────────────

#[test]
fn remove_in_txn_aborted_db_survives_crash() {
    run_firstlsn_crash("firstlsn_remove_abort", |env| {
        let db = open_existing(env, "victim").expect(
            "aborted remove-in-txn must leave 'victim' intact after crash",
        );
        db.close().unwrap();
    });
}

#[test]
fn rename_in_txn_aborted_old_name_survives_crash() {
    run_firstlsn_crash("firstlsn_rename_abort", |env| {
        let db = open_existing(env, "victim").expect(
            "aborted rename-in-txn must leave the OLD name 'victim' intact",
        );
        db.close().unwrap();
    });
}

#[test]
fn truncate_in_txn_committed_survives_crash() {
    // truncate-in-txn drains the tree through CursorImpl (which already
    // reports its first LSN), so this passes on rc2 too — kept as a
    // regression guard that the fix does not disturb it.
    run_firstlsn_crash("firstlsn_truncate_commit", |env| {
        let db = open_existing(env, "victim").expect(
            "committed truncate-in-txn must leave 'victim' present after crash",
        );
        db.close().unwrap();
    });
}

#[test]
fn truncate_in_txn_aborted_db_survives_crash() {
    run_firstlsn_crash("firstlsn_truncate_abort", |env| {
        let db = open_existing(env, "victim").expect(
            "aborted truncate-in-txn must leave 'victim' intact after crash",
        );
        db.close().unwrap();
    });
}
