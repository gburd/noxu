//! R-RELOG-PENDING-DDL — candidate S1 data-loss witness (SIGKILL crash recovery).
//!
//! While a remove/rename transaction `T` is OPEN, the victim's COMMITTED NameLN
//! is still in `name_map` (the physical removal is deferred to `T`'s commit
//! callback: `apply_deferred_remove_database` / `apply_deferred_rename_
//! database`, mirroring JE `Txn.deletedDatabases`).  `relog_live_catalog` runs
//! at the START OF EVERY checkpoint (the checkpointer's `catalog_relog_fn`
//! callback, `environment_impl.rs`) and iterates `name_map`, so WITHOUT the fix
//! it re-logs a fresh **non-transactional** live NameLN for the victim at an LSN
//! *after* `T`'s delete tombstone.  Recovery's latest-NameLN-per-name rule
//! (`recovery_manager.rs`: a non-transactional live NameLN clears any
//! `pending_name_deletes` entry and re-inserts the name) then RESURRECTS the
//! removed / renamed-away name after `T` commits and the process crashes.
//!
//! This is DISTINCT from R-FIRSTLSN (the first-active-txn clamp): the clamp fix
//! (`fix/r-firstlsn`) is applied in this branch's base and these cases are still
//! RED without the R-RELOG fix.
//!
//! JE never re-logs a NameLN for catalog relocation.  Its cleaner migrates a
//! live NameLN via the normal LN-migration path under a non-blocking READ lock
//! on the record (`FileProcessor.processFoundLN`, `FileProcessor.java:1354`);
//! a NameLN write-locked by the pending remove/rename txn is DENIED
//! (`FileProcessor.java:1358`) and deferred to the pending-LN queue (the file
//! is retained, retried later), so the relocation respects the record lock held
//! by the pending DDL txn (`DbTree.doRemoveDb` write-locks + deletes the NameLN
//! under the user locker, `DbTree.java:952/1206/1216`).
//!
//! The Noxu fix (lock-free equivalent, Noxu has no NameLN record lock):
//! `relog_live_catalog` SKIPS a name with a pending remove/rename-away by an
//! active txn, and the DDL op re-logs ONE pre-tombstone live copy whose file the
//! first-active clamp keeps alive while the txn is open.  On COMMIT the tombstone
//! is the latest NameLN (name absent); on ABORT the pre-tombstone live copy is
//! the latest live NameLN (name present).
//!
//! Real SIGKILL subprocess ports (crash_worker modes).  The worker creates +
//! commits the victim (with data), forces a checkpoint, then under an OPEN txn
//! removes/renames it and drives the two-checkpoint deletion barrier with 12
//! forced checkpoints (each re-logs the catalog) WHILE the txn is open, commits,
//! signals `firstlsn_done`, loops; the parent SIGKILLs and reopens.

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

fn run_relog_crash(mode: &str, check: impl FnOnce(&Environment)) {
    let dir = TempDir::new().unwrap();
    let dir_path = dir.path().to_path_buf();
    let mut child =
        std::process::Command::new(env!("CARGO_BIN_EXE_crash_worker"))
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

/// A committed remove-in-txn must leave the removed name ABSENT after a crash.
/// RED without the fix: `relog_live_catalog` re-logs a live NameLN for the
/// still-in-`name_map` victim after the tombstone, so recovery resurrects it.
#[test]
fn remove_in_txn_committed_name_absent_after_crash() {
    run_relog_crash("relog_remove_commit", |env| {
        assert!(
            open_existing(env, "victim").is_err(),
            "R-RELOG: a committed remove-in-txn must leave 'victim' ABSENT — \
             if it is present, relog_live_catalog re-logged a live NameLN after \
             the delete tombstone and recovery resurrected the removed database"
        );
    });
}

/// A committed rename-in-txn must leave the OLD name ABSENT and the NEW name
/// PRESENT WITH ITS DATA after a crash.  RED without the fix: the old name is
/// resurrected (and the new name may be shadowed/lost).
#[test]
fn rename_in_txn_committed_old_absent_new_present_with_data() {
    run_relog_crash("relog_rename_commit", |env| {
        assert!(
            open_existing(env, "victim").is_err(),
            "R-RELOG: a committed rename-in-txn must leave the OLD name \
             'victim' ABSENT — if present, relog_live_catalog resurrected it"
        );
        let nd = open_existing(env, "victim2").expect(
            "R-RELOG: a committed rename-in-txn must leave the NEW name \
             'victim2' PRESENT after the crash",
        );
        // The renamed DB must carry its data (same DatabaseId / tree).
        let txn = env.begin_transaction(None).expect("read txn");
        let mut present = 0u32;
        for i in 0u32..8 {
            if nd.get_in(&txn, i.to_be_bytes()).expect("get").is_some() {
                present += 1;
            }
        }
        txn.commit().unwrap();
        assert_eq!(
            present, 8,
            "R-RELOG: the renamed database 'victim2' must retain all 8 records"
        );
        nd.close().unwrap();
    });
}
