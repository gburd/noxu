//! Independent review witness (R-XARECOVERED FINDING F1): xa_rollback of a
//! RECOVERED prepared (in-doubt) XA branch must be DURABLE.
//!
//! Before this witness's fix, `write_txn_abort_for_recovered`
//! (crates/noxu-db/src/environment.rs) wrote the resolving `TxnAbort` with
//! fsync=false, flush=false, while the commit counterpart
//! `write_txn_commit_for_recovered` used fsync=true, flush=true.  So after
//! `xa_rollback(xid)` returned success, a crash/stop before an independent
//! checkpoint lost the TxnAbort: the next recovery RE-SURFACED the branch
//! in-doubt, REDID its LNs and RE-LOCKED them — a reader of the rolled-back
//! record then blocks (LockTimeout) and `xa_recover` lists the XID again.
//! JE aborts the resurrected PreparedTxn durably; the asymmetry had no JE
//! justification (fidelity: durability ordering).
//!
//! GREEN = after xa_rollback (NO intervening checkpoint), a fresh reopen does
//! NOT resurface the XID and the rolled-back record is absent & readable.
//! RED   = the XID resurfaces / the record is re-locked (abort not durable).
#![allow(clippy::unwrap_used, clippy::expect_used)]

use noxu_db::{
    Database, DatabaseConfig, Environment, EnvironmentConfig, NoxuError,
};
use noxu_xa::{XaEnvironment, XaFlags, XaResource, Xid};
use tempfile::TempDir;

fn open_env(dir: &std::path::Path, create: bool) -> Environment {
    Environment::open(
        EnvironmentConfig::new(dir.to_path_buf())
            .with_allow_create(create)
            .with_transactional(true),
    )
    .unwrap()
}

fn open_db(e: &Environment, name: &str, create: bool) -> Database {
    e.open_database(
        None,
        name,
        &DatabaseConfig::new()
            .with_allow_create(create)
            .with_transactional(true),
    )
    .unwrap()
}

#[test]
fn recovered_prepared_rollback_is_durable_without_checkpoint() {
    let dir = TempDir::new().unwrap();
    let xid = Xid::new(1, b"f1_durable_rb", b"br1").unwrap();

    // Prepare an in-doubt branch that writes "tk" into db "target"; drop.
    {
        let e = open_env(dir.path(), true);
        let o = open_db(&e, "opened", true);
        let t = open_db(&e, "target", true);
        let tx = e.begin_transaction(None).unwrap();
        o.put_in(&tx, b"seed", b"s").unwrap();
        tx.commit().unwrap();
        let xa = XaEnvironment::new(e);
        xa.xa_start(&xid, XaFlags::NOFLAGS).unwrap();
        {
            let txn = xa.get_transaction(&xid).unwrap();
            t.put_in(&txn, b"tk", b"tval").unwrap();
        }
        xa.xa_end(&xid, XaFlags::TMSUCCESS).unwrap();
        xa.xa_prepare(&xid, XaFlags::NOFLAGS).unwrap();
        drop(t);
        drop(o);
    }

    // Recover + xa_rollback.  NO explicit checkpoint afterward: the TxnAbort
    // itself must be durable.  Drop the env (simulates stop after a reported-
    // successful rollback).
    {
        let e = open_env(dir.path(), false);
        let _o = open_db(&e, "opened", false);
        let _t = open_db(&e, "target", false);
        let xa = XaEnvironment::new(e);
        assert_eq!(
            xa.xa_recover(XaFlags::STARTRSCAN).unwrap(),
            vec![xid.clone()]
        );
        xa.xa_rollback(&xid, XaFlags::NOFLAGS).unwrap();
        // NO checkpoint here on purpose.
    }

    // Fresh reopen: the branch must be GONE (durably rolled back) and the
    // record absent & readable (not re-locked by a resurrected prepared txn).
    {
        let e = open_env(dir.path(), false);
        let t = open_db(&e, "target", false);
        let xa = XaEnvironment::new(e);
        let still = xa.xa_recover(XaFlags::STARTRSCAN).unwrap();
        assert!(
            still.is_empty(),
            "xa_rollback must be DURABLE without an intervening checkpoint: \
             the branch resurfaced as in-doubt ({still:?}) — the resolving \
             TxnAbort was not fsync'd/flushed"
        );
        let txn = xa.inner().begin_transaction(None).unwrap();
        let v = t.get_in(&txn, b"tk");
        assert!(
            matches!(&v, Ok(None)),
            "the rolled-back record must be absent & readable (not re-locked \
             by a resurrected prepared txn); got {v:?}"
        );
        assert!(
            !matches!(&v, Err(NoxuError::LockTimeout { .. })),
            "a resurrected prepared txn must not re-lock a rolled-back record"
        );
        txn.commit().ok();
    }
}
