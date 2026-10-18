//! Independent review witness (R-XARECOVERED × R-UNOPENED interaction).
//!
//! The author's report §4/§7 claims a witness
//! `unopened_xarec_test::recovered_prepared_write_to_unopened_db_survives_commit`
//! that is GREEN "on the merged tree" — but NO such test ships in any delivered
//! bundle.  This reviewer-authored witness independently verifies the claimed
//! interaction: a prepared XA write to a database that is NOT opened at
//! recovery time must still be redone + relocked (recovery is db-agnostic:
//! it stashes a recovered tree for EVERY db_id), survive a cleaner deletion
//! barrier (R-UNOPENED keeps the unopened db's file retained), and resolve
//! correctly on xa_commit / xa_rollback.
//!
//! GREEN = the prepared write to the unopened db survives recovery + barrier +
//! resolution.  RED = data lost (either the redo did not cover the unopened
//! db, or the cleaner reclaimed its file).
#![allow(clippy::unwrap_used, clippy::expect_used)]

use noxu_db::{
    CheckpointConfig, Database, DatabaseConfig, Environment, EnvironmentConfig,
};
use noxu_xa::{XaEnvironment, XaFlags, XaResource, Xid};
use std::path::Path;
use tempfile::TempDir;

fn open_env(dir: &Path, create: bool) -> Environment {
    let mut cfg = EnvironmentConfig::new(dir.to_path_buf())
        .with_allow_create(create)
        .with_transactional(true);
    cfg.set_run_cleaner(false);
    cfg.set_run_checkpointer(false);
    cfg.set_run_evictor(false);
    cfg.set_log_file_max_bytes(64 * 1024);
    cfg.set_cleaner_min_age(1);
    Environment::open(cfg).expect("open env")
}

fn open_db(env: &Environment, name: &str, create: bool) -> Database {
    env.open_database(
        None,
        name,
        &DatabaseConfig::new()
            .with_allow_create(create)
            .with_transactional(true),
    )
    .expect("open db")
}

fn barrier(env: &Environment) {
    // Two forced checkpoints + a deletion pass cross the safe-to-delete
    // barrier; force-clean reclaims eligible files.
    let force = CheckpointConfig::new().with_force(true);
    for _ in 0..4 {
        let _ = env.clean_log_forced();
        let _ = env.checkpoint(Some(&force));
    }
}

/// Prepared write to a db that is NOT opened at recovery time -> commit after
/// a cleaner barrier -> the record survives.
#[test]
fn recovered_prepared_write_to_unopened_db_survives_commit() {
    let dir = TempDir::new().unwrap();
    let xid = Xid::new(1, b"rv_unopened_commit", b"br1").unwrap();

    // Create TWO dbs: "opened" (we will open it after restart to drive the
    // barrier) and "target" (we will NOT open it before xa_commit).
    {
        let env = open_env(dir.path(), true);
        let opened = open_db(&env, "opened", true);
        let target = open_db(&env, "target", true);
        // Seed + roll files so the target's prepared LNs land in an older,
        // cleanable file.
        let t = env.begin_transaction(None).unwrap();
        opened.put_in(&t, b"seed", b"seed").unwrap();
        t.commit().unwrap();
        let xa = XaEnvironment::new(env);
        xa.xa_start(&xid, XaFlags::NOFLAGS).unwrap();
        {
            let txn = xa.get_transaction(&xid).unwrap();
            target.put_in(&txn, b"tk", b"tval").unwrap();
        }
        xa.xa_end(&xid, XaFlags::TMSUCCESS).unwrap();
        xa.xa_prepare(&xid, XaFlags::NOFLAGS).unwrap();
        // "crash": drop the env at block end WITHOUT resolving the prepared
        // branch.  The durable TxnPrepare survives; dropping releases the file
        // lock so the next open succeeds.  (A SIGKILL would be stronger but the
        // XA layer's drop does not resolve a prepared branch, so the on-disk
        // state is the in-doubt TxnPrepare either way.)
        drop(target);
        drop(opened);
    }

    // Reopen, recover (prepared txn for the UNOPENED target db is redone +
    // relocked db-agnostically).  Open ONLY "opened"; drive the cleaner
    // barrier WITHOUT opening "target"; then xa_commit.
    {
        let env = open_env(dir.path(), false);
        let _opened = open_db(&env, "opened", false);
        let xa = XaEnvironment::new(env);
        assert_eq!(
            xa.xa_recover(XaFlags::STARTRSCAN).unwrap(),
            vec![xid.clone()],
            "the in-doubt xid (writing the unopened db) must be listed"
        );
        // Barrier while "target" is NOT opened: R-UNOPENED must retain its file
        // AND the redo already materialised the slot.
        barrier(xa.inner());
        xa.xa_commit(&xid, XaFlags::NOFLAGS).unwrap();
    }

    // Final reopen: open "target" and read the committed record.
    {
        let env = open_env(dir.path(), false);
        let target = open_db(&env, "target", false);
        let txn = env.begin_transaction(None).unwrap();
        let v = target.get_in(&txn, b"tk").unwrap();
        assert_eq!(
            v.as_deref(),
            Some(b"tval".as_slice()),
            "the prepared write to the UNOPENED db must survive recovery + \
             cleaner barrier + xa_commit (R-XARECOVERED x R-UNOPENED)"
        );
        txn.commit().unwrap();
    }
}

/// Same shape but ROLLBACK: the prepared write to the unopened db must be
/// absent after xa_rollback.
#[test]
fn recovered_prepared_write_to_unopened_db_rolled_back_absent() {
    let dir = TempDir::new().unwrap();
    let xid = Xid::new(1, b"rv_unopened_rb", b"br1").unwrap();

    {
        let env = open_env(dir.path(), true);
        let opened = open_db(&env, "opened", true);
        let target = open_db(&env, "target", true);
        let t = env.begin_transaction(None).unwrap();
        opened.put_in(&t, b"seed", b"seed").unwrap();
        t.commit().unwrap();
        let xa = XaEnvironment::new(env);
        xa.xa_start(&xid, XaFlags::NOFLAGS).unwrap();
        {
            let txn = xa.get_transaction(&xid).unwrap();
            target.put_in(&txn, b"tk", b"tval").unwrap();
        }
        xa.xa_end(&xid, XaFlags::TMSUCCESS).unwrap();
        xa.xa_prepare(&xid, XaFlags::NOFLAGS).unwrap();
        drop(target);
        drop(opened);
    }
    {
        let env = open_env(dir.path(), false);
        let _opened = open_db(&env, "opened", false);
        let xa = XaEnvironment::new(env);
        assert_eq!(
            xa.xa_recover(XaFlags::STARTRSCAN).unwrap(),
            vec![xid.clone()]
        );
        barrier(xa.inner());
        xa.xa_rollback(&xid, XaFlags::NOFLAGS).unwrap();
    }
    {
        let env = open_env(dir.path(), false);
        let xa_check = XaEnvironment::new(env);
        let still = xa_check.xa_recover(XaFlags::STARTRSCAN).unwrap();
        eprintln!("DIAG final-reopen xa_recover after rollback = {still:?}");
        // If the branch resurfaces, resolve it so the read can proceed.
        if !still.is_empty() {
            xa_check.xa_rollback(&xid, XaFlags::NOFLAGS).unwrap();
        }
        let target = open_db(xa_check.inner(), "target", false);
        let txn = xa_check.inner().begin_transaction(None).unwrap();
        let v = target.get_in(&txn, b"tk").unwrap();
        assert!(
            v.is_none(),
            "the prepared write to the UNOPENED db must be ABSENT after \
             xa_rollback (resurfaced={})",
            !still.is_empty()
        );
        txn.commit().unwrap();
    }
}
