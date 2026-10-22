//! BUG-DDL-DURABILITY-1 — transactional DB remove/rename/truncate must be
//! crash-durable through the commit's own SYNC fsync, exactly like any other
//! transactional write.
//!
//! JE: a transactional DDL op logs its NameLN delete/rename as an ordinary
//! transactional LN *inside the user txn* (`DbTree.doRemoveDb`,
//! `DbTree.java:1182-1206`), records the db in `Txn.deletedDatabases`
//! (`Txn.java:1485-1489`), and lets the normal SYNC commit make it durable
//! (`Txn.logCommitEntry`, `Txn.java:983-986`).  The physical MapLN cleanup is
//! the only post-commit work.  Abort undoes the DDL (the db survives).
//!
//! These are real SIGKILL subprocess ports: the worker performs the txnal DDL
//! op, commits (or aborts) WITHOUT any `db.sync()` and WITHOUT any checkpoint,
//! signals `ddl_done`, then loops; the parent SIGKILLs it.  The ONLY path to
//! durability is therefore the commit fsync — this is what the stopgap
//! (synchronous fsync in `log_name_ln_delete`) stood in for and the
//! JE-faithful fix must now provide.

use noxu_db::{DatabaseConfig, EnvironmentConfig};
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

/// Spawn the crash worker in `mode`, wait for `ddl_done`, SIGKILL it, reopen
/// the env, and hand the reopened env to `check`.
fn run_ddl_crash(mode: &str, check: impl FnOnce(&noxu_db::Environment)) {
    let dir = TempDir::new().unwrap();
    let dir_path = dir.path().to_path_buf();

    let mut child = std::process::Command::new(crash_worker_exe())
        .env("NOXU_CRASH_DIR", &dir_path)
        .env("NOXU_CRASH_MODE", mode)
        .spawn()
        .expect("spawn crash_worker");

    assert!(
        wait_for_flag(&dir_path, "ddl_done", Duration::from_secs(60)),
        "worker never finished the txnal DDL op (mode {mode})"
    );
    child.kill().expect("SIGKILL");
    child.wait().expect("wait");

    let env = noxu_db::Environment::open(
        EnvironmentConfig::new(dir_path)
            .with_allow_create(true)
            .with_transactional(true),
    )
    .expect("reopen env after crash");

    check(&env);

    let vr = env.verify(&noxu_db::VerifyConfig::new()).expect("verify");
    assert_eq!(
        vr.error_count(),
        0,
        "post-crash structural verify errors (mode {mode}): {:?}",
        vr.errors
    );
}

// --------------------------------------------------------------------------
// REMOVE
// --------------------------------------------------------------------------

/// A committed transactional remove must survive a crash with no sync/no
/// checkpoint — the DB must be ABSENT after recovery.
#[test]
fn txnal_remove_commit_is_durable() {
    run_ddl_crash("txnal_remove_commit", |env| {
        let present = env.database_names().unwrap().iter().any(|n| n == "test");
        assert!(
            !present,
            "a committed txnal remove must stay removed after a crash"
        );
        let reopen = env.open_database(
            None,
            "test",
            &DatabaseConfig::new().with_transactional(true),
        );
        assert!(
            reopen.is_err(),
            "removed DB must not be openable without allow_create"
        );
    });
}

/// An aborted transactional remove must leave the DB intact and readable after
/// a crash (the delete was logged under the txn but the txn aborted, so
/// recovery must UNDO the NameLN delete).
#[test]
fn txnal_remove_abort_keeps_db() {
    run_ddl_crash("txnal_remove_abort", |env| {
        let present = env.database_names().unwrap().iter().any(|n| n == "test");
        assert!(
            present,
            "an aborted txnal remove must leave the DB present after a crash"
        );
        let db = env
            .open_database(
                None,
                "test",
                &DatabaseConfig::new().with_transactional(true),
            )
            .expect("reopen the un-removed DB");
        assert_eq!(
            db.count().unwrap(),
            1,
            "the seeded record must survive an aborted remove"
        );
    });
}

// --------------------------------------------------------------------------
// RENAME
// --------------------------------------------------------------------------

/// A committed transactional rename must survive a crash: the new name present
/// (with the record), the old name absent.
#[test]
fn txnal_rename_commit_is_durable() {
    run_ddl_crash("txnal_rename_commit", |env| {
        let names = env.database_names().unwrap();
        assert!(
            names.iter().any(|n| n == "renamed"),
            "committed rename target must exist after a crash: {names:?}"
        );
        assert!(
            !names.iter().any(|n| n == "test"),
            "committed rename source must be gone after a crash: {names:?}"
        );
        let db = env
            .open_database(
                None,
                "renamed",
                &DatabaseConfig::new().with_transactional(true),
            )
            .expect("reopen renamed DB");
        assert_eq!(
            db.count().unwrap(),
            1,
            "the record must survive a committed rename"
        );
    });
}

/// An aborted transactional rename must leave the original name intact and the
/// target absent after a crash.
#[test]
fn txnal_rename_abort_keeps_original() {
    run_ddl_crash("txnal_rename_abort", |env| {
        let names = env.database_names().unwrap();
        assert!(
            names.iter().any(|n| n == "test"),
            "aborted rename must keep the source name: {names:?}"
        );
        assert!(
            !names.iter().any(|n| n == "renamed"),
            "aborted rename must not create the target: {names:?}"
        );
        let db = env
            .open_database(
                None,
                "test",
                &DatabaseConfig::new().with_transactional(true),
            )
            .expect("reopen original DB");
        assert_eq!(db.count().unwrap(), 1, "record survives an aborted rename");
    });
}

// --------------------------------------------------------------------------
// TRUNCATE
// --------------------------------------------------------------------------

/// A committed transactional truncate must survive a crash: the DB exists and
/// is EMPTY after recovery.
#[test]
fn txnal_truncate_commit_is_durable() {
    run_ddl_crash("txnal_truncate_commit", |env| {
        let present = env.database_names().unwrap().iter().any(|n| n == "test");
        assert!(present, "truncated DB must still exist after a crash");
        let db = env
            .open_database(
                None,
                "test",
                &DatabaseConfig::new().with_transactional(true),
            )
            .expect("reopen truncated DB");
        assert_eq!(
            db.count().unwrap(),
            0,
            "a committed truncate must leave the DB empty after a crash"
        );
    });
}

/// An aborted transactional truncate must leave all records intact after a
/// crash.
#[test]
fn txnal_truncate_abort_keeps_records() {
    run_ddl_crash("txnal_truncate_abort", |env| {
        let present = env.database_names().unwrap().iter().any(|n| n == "test");
        assert!(present, "DB must still exist after an aborted truncate");
        let db = env
            .open_database(
                None,
                "test",
                &DatabaseConfig::new().with_transactional(true),
            )
            .expect("reopen DB after aborted truncate");
        assert_eq!(
            db.count().unwrap(),
            20,
            "an aborted truncate must keep every record after a crash"
        );
    });
}

// --------------------------------------------------------------------------
// ENG-FSYNCDROP-RESURRECT-1 (S1) — synthetic, FaultDisk-free deterministic
// witness for the same-id-truncate resurrection (F1).  See the crash_worker
// mode `kd_truncate_resurrect` for the mechanism.  RED on the base (recovered
// "test" holds resurrected records); GREEN once truncate assigns a new id.
// --------------------------------------------------------------------------

/// A committed txnal truncate must leave the db EMPTY after a crash even when
/// the pre-truncate records were checkpointed as known-deleted by an earlier
/// uncommitted (then aborted) delete.  On the base the records resurrect.
#[test]
fn kd_truncate_resurrect_is_empty_after_crash() {
    run_ddl_crash("kd_truncate_resurrect", |env| {
        let present = env.database_names().unwrap().iter().any(|n| n == "test");
        assert!(present, "truncated DB must still exist after a crash");
        let db = env
            .open_database(
                None,
                "test",
                &DatabaseConfig::new().with_transactional(true),
            )
            .expect("reopen truncated DB");
        let n = db.count().unwrap();
        assert_eq!(
            n, 0,
            "a committed truncate must leave the DB empty after a crash — \
             {n} record(s) RESURRECTED (ENG-FSYNCDROP-RESURRECT-1)"
        );
    });
}
