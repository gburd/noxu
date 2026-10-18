//! xa_rep_failclosed_probe — R-REPUSER fail-closed across the XA 2PC flow.
//!
//! Closes the coordinator-review gap: XA uses `begin_transaction` + `put_in` +
//! `prepare` (which logs a TxnPrepare frame) + `commit`. This witnesses that:
//!
//!  1. a `put_in` under an XA branch on a master-wired env is refused at the
//!     write op (before any LN), so `prepare` finds no writes (ReadOnly) and NO
//!     TxnPrepare frame is durable;
//!  2. a pre-existing prepared branch (prepared on a STANDALONE env, "crash",
//!     reopen under a REFUSING coordinator) is refused on `xa_commit`, so NO
//!     TxnCommit frame is written and `xa_recover()` after a second reopen
//!     still lists the branch (unresolved) — the fail-closed answer for
//!     `write_txn_commit_for_recovered`.
//!
//! JE SHAPE (quoted): in BDB-JE a recovered prepared branch commits through
//! `XAEnvironment.commit(xid)` -> `Txn.commit(xid)` (XAEnvironment.java:85-107,
//! Txn.java:730-765), which runs `preLogCommitHook()` whenever
//! `updateLoggedForTxn()` — i.e. a recovered XA commit on a master IS a
//! replicated commit that logs with the MASTER repContext and waits for acks.
//! Noxu cannot ship it in 7.11.x, so the fail-closed stance refuses the
//! recovered commit rather than silently writing a non-replicated TxnCommit.
//!
//! On the base (coordinator not refusing) case (1)'s put returns Ok and the
//! write silently never ships — the S1 defect.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;

use noxu_db::error::NoxuError;
use noxu_db::{
    AckWaitError, AckWaitErrorKind, Database, DatabaseConfig, Environment,
    EnvironmentConfig, ReplicaAckCoordinator, ReplicaAckPolicyKind,
};
use noxu_xa::{
    PrepareResult, XaEnvironment, XaError, XaFlags, XaResource, Xid,
};
use tempfile::TempDir;

/// Coordinator that reports it cannot replicate user writes (the real
/// `noxu_rep::ReplicatedEnvironment` in 7.11.x).
struct RefusingCoord;

impl ReplicaAckCoordinator for RefusingCoord {
    fn await_replica_acks(
        &self,
        _policy: ReplicaAckPolicyKind,
        _timeout: Duration,
    ) -> std::result::Result<u32, AckWaitError> {
        Err(AckWaitError {
            kind: AckWaitErrorKind::Timeout,
            needed: 1,
            received: 0,
        })
    }
    fn refuse_user_write_commit(&self) -> Option<&'static str> {
        Some("R-REPUSER: user writes not replicated in 7.11.x (test)")
    }
}

fn open_xa(dir: &std::path::Path) -> (XaEnvironment, Database) {
    let env = Environment::open(
        EnvironmentConfig::new(dir.to_path_buf())
            .with_allow_create(true)
            .with_transactional(true),
    )
    .unwrap();
    let db = env
        .open_database(
            None,
            "xa_db",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap();
    let xa = XaEnvironment::new(env);
    (xa, db)
}

// ── (1) XA put under a branch on a master-wired env is refused at the op; ──
//        prepare then finds no writes (ReadOnly) and writes NO TxnPrepare.
#[test]
fn xa_put_refused_prepare_readonly_no_prepare_frame() {
    let dir = TempDir::new().unwrap();
    let xid = Xid::new(1, b"xa_failclosed_1", b"br1").unwrap();

    let (xa, db) = open_xa(dir.path());
    // Wire the refusing coordinator on the inner env.
    xa.inner().set_replica_coordinator(Arc::new(RefusingCoord));

    xa.xa_start(&xid, XaFlags::NOFLAGS).unwrap();
    {
        let txn = xa.get_transaction(&xid).unwrap();
        // The write op is refused before any LN.
        match db.put_in(&txn, b"k", b"v") {
            Err(NoxuError::ReplicationNotSupported { reason }) => {
                assert!(!reason.is_empty())
            }
            other => panic!(
                "xa put_in: expected ReplicationNotSupported, got {other:?}"
            ),
        }
    }
    xa.xa_end(&xid, XaFlags::TMSUCCESS).unwrap();

    // No writes logged -> prepare is a read-only no-op; NO TxnPrepare frame.
    let pr = xa.xa_prepare(&xid, XaFlags::NOFLAGS).unwrap();
    assert_eq!(
        pr,
        PrepareResult::ReadOnly,
        "a branch whose only write was refused must prepare ReadOnly (no \
         TxnPrepare frame)"
    );

    let _ = db.close();
    let _ = xa.inner().close();

    // Reopen (fresh XA) and confirm no in-doubt branch surfaced (nothing was
    // ever prepared durably) and the key is absent.
    let (xa2, db2) = open_xa(dir.path());
    let recovered = xa2.xa_recover(XaFlags::STARTRSCAN).unwrap();
    assert!(
        recovered.is_empty(),
        "no TxnPrepare frame should exist; got in-doubt {recovered:?}"
    );
    assert!(db2.get(b"k").unwrap().is_none(), "refused write must be absent");
    let _ = db2.close();
    let _ = xa2.inner().close();
}

// ── (2) write_txn_commit_for_recovered: prepare STANDALONE, crash, reopen ──
//        under a REFUSING coordinator, commit -> refused; no TxnCommit frame;
//        recover() after reopen still lists the branch (unresolved).
#[test]
fn xa_recovered_commit_refused_on_master_wired_env() {
    let dir = TempDir::new().unwrap();
    let xid = Xid::new(1, b"xa_failclosed_2", b"br2").unwrap();
    let key = b"xa_rk";

    // Phase 1: prepare on a STANDALONE env (no coordinator), then "crash".
    {
        let (xa, db) = open_xa(dir.path());
        xa.xa_start(&xid, XaFlags::NOFLAGS).unwrap();
        {
            let txn = xa.get_transaction(&xid).unwrap();
            db.put_in(&txn, key, b"v").unwrap(); // standalone: allowed
        }
        xa.xa_end(&xid, XaFlags::TMSUCCESS).unwrap();
        assert_eq!(
            xa.xa_prepare(&xid, XaFlags::NOFLAGS).unwrap(),
            PrepareResult::Ok
        );
        // drop without commit -> crash with a durable TxnPrepare frame
    }

    // Phase 2: reopen UNDER a refusing coordinator; recover lists the XID;
    // commit must be REFUSED (write_txn_commit_for_recovered gate).
    {
        let (xa, db) = open_xa(dir.path());
        xa.inner().set_replica_coordinator(Arc::new(RefusingCoord));
        let recovered = xa.xa_recover(XaFlags::STARTRSCAN).unwrap();
        assert_eq!(
            recovered,
            vec![xid.clone()],
            "the prepared XID must recover"
        );

        match xa.xa_commit(&xid, XaFlags::NOFLAGS) {
            Err(XaError::Db(NoxuError::ReplicationNotSupported { reason })) => {
                assert!(!reason.is_empty())
            }
            other => panic!(
                "recovered xa_commit on a master-wired env: expected \
                 ReplicationNotSupported, got {other:?}"
            ),
        }
        let _ = db.close();
        let _ = xa.inner().close();
    }

    // Phase 3: reopen again (no coordinator). No TxnCommit frame was written,
    // so the branch is STILL in-doubt and the data is NOT durably committed.
    {
        let (xa, db) = open_xa(dir.path());
        let recovered = xa.xa_recover(XaFlags::STARTRSCAN).unwrap();
        assert_eq!(
            recovered,
            vec![xid.clone()],
            "the branch must still be in-doubt (no TxnCommit was written)"
        );
        // R-XARECOVERED: recovery now redoes AND re-locks a recovered prepared
        // branch's LNs, so while the branch is in-doubt its record is held under
        // the recovered-prepared WRITE lock. A reader therefore BLOCKS on the
        // in-doubt lock and surfaces the typed NoxuError::LockTimeout (owner
        // decision #19 / JE parity), instead of reading the uncommitted value or
        // a stale absence. Positively witness that behaviour before resolving.
        match db.get(key) {
            Err(NoxuError::LockTimeout { .. }) => {}
            other => panic!(
                "an in-doubt recovered-prepared record must block the reader \
                 with a typed LockTimeout (R-XARECOVERED, owner #19), got \
                 {other:?}"
            ),
        }
        // Resolve it now (standalone) so the write lock is released; the branch
        // was never committed (the recovered xa_commit was refused), so after a
        // rollback the data must NOT be durable.
        xa.xa_rollback(&xid, XaFlags::NOFLAGS).unwrap();
        assert!(
            db.get(key).unwrap().is_none(),
            "the recovered-commit was refused and the branch rolled back, \
             so the data must NOT be durable"
        );
        let _ = db.close();
        let _ = xa.inner().close();
    }
}

// ── control: XA on a standalone env (no coordinator) commits durably ──
#[test]
fn xa_standalone_two_phase_commit_unaffected() {
    let dir = TempDir::new().unwrap();
    let xid = Xid::new(1, b"xa_ctrl", b"br").unwrap();
    let key = b"xa_ck";

    let (xa, db) = open_xa(dir.path());
    xa.xa_start(&xid, XaFlags::NOFLAGS).unwrap();
    {
        let txn = xa.get_transaction(&xid).unwrap();
        db.put_in(&txn, key, b"v").unwrap();
    }
    xa.xa_end(&xid, XaFlags::TMSUCCESS).unwrap();
    assert_eq!(
        xa.xa_prepare(&xid, XaFlags::NOFLAGS).unwrap(),
        PrepareResult::Ok
    );
    xa.xa_commit(&xid, XaFlags::NOFLAGS).unwrap();
    assert_eq!(db.get(key).unwrap().as_deref(), Some(&b"v"[..]));
    let _ = db.close();
    let _ = xa.inner().close();
}
