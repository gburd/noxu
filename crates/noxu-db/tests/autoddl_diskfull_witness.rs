//! ENG-AUTODDL-2 witness: an AUTO-COMMIT (txn = None) catalog DDL whose log
//! write hits ENOSPC MUST return Err — never report Ok with the write lost.
//!
//! JE: the auto-txn's log write throws; `runOnce`'s `finally
//! locker.operationEnd(false)` aborts and the exception propagates
//! (Environment.java:1204-1236). Noxu's rc-base drops the error with
//! `let _ = Self::log_name_ln_delete(..)` / `let _ = Self::log_name_ln(..)`
//! in the auto-commit create/remove/rename paths (ENG-AUTODDL-2), so a DDL
//! returns Ok after DiskFull.
//!
//! This witness installs FaultDisk `DiskFull` at the write that the DDL's
//! NameLN append lands on (found adaptively from `write_count`), runs the op,
//! and asserts it returns `Err`. RED on rc-base (returns Ok), GREEN after the
//! fix propagates the log error.
//!
//! faultdisk is process-global; the sub-tests install/uninstall and run
//! single-threaded.
use noxu_db::{DatabaseConfig, Environment, EnvironmentConfig};
use noxu_log::faultdisk::{self, FaultController, FaultKind};
use std::sync::Mutex;

// faultdisk is process-global: serialise the sub-tests in this binary so a
// parallel `cargo test` run cannot have one test's DiskFull controller fire on
// another's writes. (The existing faultdisk tests use the same pattern.)
static GUARD: Mutex<()> = Mutex::new(());

fn cfg(p: &std::path::Path) -> EnvironmentConfig {
    EnvironmentConfig::new(p.to_path_buf())
        .with_allow_create(true)
        .with_transactional(true)
}

fn dbcfg() -> DatabaseConfig {
    DatabaseConfig::new().with_allow_create(true).with_transactional(true)
}

/// Build the pre-state, then install DiskFull at the NEXT write and run `op`;
/// assert it returns Err. The pre-state writes happen with the fault layer
/// off, so the fault fires on the DDL's own append.
fn disk_full_on_ddl(mode: &str) {
    let _serial = GUARD.lock().unwrap_or_else(|p| p.into_inner());
    faultdisk::uninstall();
    let dir = tempfile::tempdir().unwrap();
    let env = Environment::open(cfg(dir.path())).unwrap();
    let db = env.open_database(None, "a", &dbcfg()).unwrap();
    db.put(b"k", b"v").unwrap();
    db.close().unwrap();
    // Force everything so far to disk and quiesce the write counter.
    env.flush_log(true).unwrap();

    // Install DiskFull at the very next posio write — the DDL's first NameLN
    // append (or its DeleteLN for truncate).
    let next = faultdisk::write_count();
    faultdisk::install(FaultController::for_test(FaultKind::DiskFull, next));

    let res: noxu_db::Result<()> = match mode {
        "rename" => env.rename_database(None, "a", "b"),
        "remove" => env.remove_database(None, "a"),
        "truncate" => env.truncate_database(None, "a").map(|_| ()),
        "create" => {
            env.open_database(None, "c", &dbcfg()).and_then(|d| d.close())
        }
        other => panic!("unknown mode {other}"),
    };
    faultdisk::uninstall();

    assert!(
        res.is_err(),
        "ENG-AUTODDL-2: auto {mode} returned Ok after DiskFull (log error dropped): {res:?}"
    );
    env.close().ok();
}

#[test]
fn autoddl_rename_disk_full_returns_err() {
    disk_full_on_ddl("rename");
}

#[test]
fn autoddl_remove_disk_full_returns_err() {
    disk_full_on_ddl("remove");
}

#[test]
fn autoddl_truncate_disk_full_returns_err() {
    disk_full_on_ddl("truncate");
}

#[test]
fn autoddl_create_disk_full_returns_err() {
    disk_full_on_ddl("create");
}
