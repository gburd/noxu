//! XDB-711 detector — false-positive guard on CLEAN (fixed-writer) logs.
//!
//! The detector ([`noxu_recovery::detect_xdb711_damage`]) must refuse ONLY a
//! log damaged by the 7.11.0/.1 catalog-relog bug — never a legitimate
//! concurrent-DDL + crash history produced by the FIXED (7.11.2) writer. This
//! probe drives the in-tree crash modes that produce exactly the legitimate
//! shapes the detector's exclusions cover — a committed relog of a
//! remove/rename, in-doubt and committed XA DDL, first-LSN create/remove/
//! rename/truncate aborts — SIGKILLs at a quiescent instant, and asserts the
//! reopen NEVER fails closed with `LOG_INTEGRITY`.
//!
//! Each mode is run many times (`XDB_FP_ITERS`, default 50) so a timing- or
//! ordering-sensitive false positive is caught. A single `LOG_INTEGRITY` on
//! any clean reopen fails the test.
//!
//! Complements `xdb711_detect_images.rs` (the 4 damaged images MUST fail
//! closed) — together they bound the detector from both sides.

use noxu_db::{Environment, EnvironmentConfig, EnvironmentFailureReason, NoxuError};
use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant};

fn crash_worker_exe() -> &'static str {
    env!("CARGO_BIN_EXE_crash_worker")
}

fn wait_for_flag(dir: &Path, name: &str, timeout: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if dir.join(name).exists() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    false
}

fn iters() -> usize {
    std::env::var("XDB_FP_ITERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(50)
}

/// Run `mode` once under a crash child, SIGKILL at its ready flag, reopen, and
/// return the open result so the caller can assert it is never a false
/// positive.
fn crash_and_reopen(mode: &str, dir: &Path) -> Result<(), NoxuError> {
    let mut child = Command::new(crash_worker_exe())
        .env("NOXU_CRASH_DIR", dir)
        .env("NOXU_CRASH_MODE", mode)
        .spawn()
        .expect("spawn crash_worker");
    // The relog / firstlsn / xaddl crash modes all signal `firstlsn_done`
    // once they have reached their quiescent (committed/prepared) instant and
    // begin looping until killed.  Wait for that, then SIGKILL; a settle
    // fallback covers any mode that signals a different flag.
    if !wait_for_flag(dir, "firstlsn_done", Duration::from_secs(30)) {
        std::thread::sleep(Duration::from_millis(500));
    }
    let _ = child.kill();
    let _ = child.wait();

    let mut cfg = EnvironmentConfig::new(dir.to_path_buf())
        .with_allow_create(false)
        .with_transactional(true);
    cfg.set_run_cleaner(false);
    cfg.set_run_checkpointer(false);
    cfg.set_run_evictor(false);
    cfg.set_run_in_compressor(false);
    cfg.set_run_verifier(false);
    cfg.set_stats_collect(false);
    Environment::open(cfg).map(|_env| ())
}

fn assert_never_log_integrity(mode: &str) {
    let base = std::env::var_os("TMPDIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(std::env::temp_dir)
        .join(format!("xdb711-fp-{mode}"));
    let _ = std::fs::remove_dir_all(&base);
    let n = iters();
    let mut opened = 0usize;
    let mut benign_err = 0usize;
    for i in 0..n {
        let dir = base.join(format!("i{i}"));
        std::fs::create_dir_all(&dir).unwrap();
        match crash_and_reopen(mode, &dir) {
            Ok(()) => opened += 1,
            Err(NoxuError::EnvironmentFailure {
                reason: EnvironmentFailureReason::LogIntegrity,
                msg,
            }) => {
                panic!(
                    "[false-positive] mode {mode} iter {i}: detector FIRED on \
                     a clean fixed-writer log: {msg}"
                );
            }
            // Any other open outcome (a benign recovery error, a mode that
            // does not leave an openable env) is not a detector false
            // positive — only LOG_INTEGRITY is.
            Err(_) => benign_err += 1,
        }
    }
    eprintln!(
        "mode {mode}: {n} iters — {opened} clean opens, {benign_err} \
         non-integrity outcomes, 0 LOG_INTEGRITY false positives ✓"
    );
    // At least some iterations must have produced an openable env, else the
    // probe is vacuous for this mode.
    assert!(
        opened > 0,
        "mode {mode}: no iteration produced an openable env (probe vacuous)"
    );
}

#[test]
fn relog_remove_commit_never_false_positive() {
    assert_never_log_integrity("relog_remove_commit");
}

#[test]
fn relog_rename_commit_never_false_positive() {
    assert_never_log_integrity("relog_rename_commit");
}

#[test]
fn firstlsn_rename_abort_never_false_positive() {
    assert_never_log_integrity("firstlsn_rename_abort");
}

#[test]
fn firstlsn_remove_abort_never_false_positive() {
    assert_never_log_integrity("firstlsn_remove_abort");
}

#[test]
fn xaddl_rename_prepare_never_false_positive() {
    assert_never_log_integrity("xaddl_rename_prepare");
}

#[test]
fn xaddl_commit_never_false_positive() {
    assert_never_log_integrity("xaddl_commit");
}
