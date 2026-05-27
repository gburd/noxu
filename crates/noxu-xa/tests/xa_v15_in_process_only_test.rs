//! Sprint 3 / Wave 3-2 tests — XA crash-durable two-phase commit.
//!
//! See `docs/src/internal/sprint-3-xa-restriction.md` and
//! `docs/src/internal/wave-3-2-crash-durable-xa.md` for the full
//! design.
//!
//! Wave 3-2 implemented crash-durable XA, removing the v1.5 in-process-
//! only restriction.  These tests now pin down the v2.0 contract:
//!
//! 1. `xa_recover` on a fresh `XaEnvironment` (no prior `xa_prepare`)
//!    returns an empty list — no spurious entries.
//! 2. `xa_commit` of an XID that was prepared in a previous process
//!    (and survived the crash via the WAL `TxnPrepare` frame) **succeeds**
//!    and the prepared writes become visible.
//! 3. `xa_rollback` of such an XID **succeeds** and the prepared writes
//!    are discarded.
//! 4. `xa_forget` of such an XID still succeeds — operators can clear
//!    in-doubt entries without resolving their data.
//! 5. `xa_prepare` auto-detects writes performed via the inner
//!    `Transaction` — a user that forgets to call `mark_write` no longer
//!    silently slides into the read-only optimisation and aborts their
//!    work.
//! 6. A truly read-only branch still returns `PrepareResult::ReadOnly`.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use noxu_db::{
    Database, DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig,
    OperationStatus,
};
use noxu_xa::{
    PrepareResult, XaEnvironment, XaError, XaFlags, XaResource, Xid,
};
use tempfile::TempDir;

fn make_xa_with_log(dir: &std::path::Path) -> (XaEnvironment, Database) {
    let env_config = EnvironmentConfig::new(dir.to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    let env = Environment::open(env_config).unwrap();
    let db = env
        .open_database(
            None,
            "v15_inproc",
            &DatabaseConfig::new().with_allow_create(true),
        )
        .unwrap();
    let xa = XaEnvironment::new(env).with_prepared_log().unwrap();
    (xa, db)
}

fn make_xa_no_log(dir: &std::path::Path) -> (XaEnvironment, Database) {
    let env_config = EnvironmentConfig::new(dir.to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    let env = Environment::open(env_config).unwrap();
    let db = env
        .open_database(
            None,
            "v15_inproc",
            &DatabaseConfig::new().with_allow_create(true),
        )
        .unwrap();
    let xa = XaEnvironment::new(env);
    (xa, db)
}

// ---------------------------------------------------------------------------
// 1. Fresh xa_recover is empty
// ---------------------------------------------------------------------------

#[test]
fn fresh_env_xa_recover_returns_empty_with_log() {
    let dir = TempDir::new().unwrap();
    let (xa, _db) = make_xa_with_log(dir.path());
    let recovered = xa.xa_recover(XaFlags::STARTRSCAN).unwrap();
    assert!(
        recovered.is_empty(),
        "fresh XaEnvironment with prepared log must report no in-doubt XIDs, got {recovered:?}"
    );
}

#[test]
fn fresh_env_xa_recover_returns_empty_without_log() {
    let dir = TempDir::new().unwrap();
    let (xa, _db) = make_xa_no_log(dir.path());
    let recovered = xa.xa_recover(XaFlags::STARTRSCAN).unwrap();
    assert!(recovered.is_empty());
}

// ---------------------------------------------------------------------------
// 2. xa_commit after restart succeeds and prepared writes become visible.
//    Wave 3-2: crash-durable XA replaces the v1.5
//    `CrashDurabilityNotSupported` regression.
// ---------------------------------------------------------------------------

#[test]
fn xa_commit_after_restart_succeeds() {
    let dir = TempDir::new().unwrap();
    let xid = Xid::new(1, b"v2_commit_after_restart", b"br").unwrap();
    let key = b"k_after_restart_commit";

    // Phase 1: prepare and "crash" (drop without committing).
    {
        let (xa, db) = make_xa_with_log(dir.path());
        xa.xa_start(&xid, XaFlags::NOFLAGS).unwrap();
        {
            let txn = xa.get_transaction(&xid).unwrap();
            db.put(
                Some(txn),
                &DatabaseEntry::from_bytes(key),
                &DatabaseEntry::from_bytes(b"v"),
            )
            .unwrap();
        }
        xa.xa_end(&xid, XaFlags::TMSUCCESS).unwrap();
        xa.xa_prepare(&xid, XaFlags::NOFLAGS).unwrap();
        // Drop xa + db + env without committing — simulates a crash.
    }

    // Phase 2: reopen.  The XID is in the WAL; xa_recover must surface it,
    // xa_commit must succeed, and the prepared write must become visible.
    {
        let (xa, db) = make_xa_with_log(dir.path());
        let recovered = xa.xa_recover(XaFlags::STARTRSCAN).unwrap();
        assert!(
            recovered.contains(&xid),
            "recovered XIDs should include the prepared one: {recovered:?}"
        );

        xa.xa_commit(&xid, XaFlags::NOFLAGS).unwrap();

        // After commit, the prepared write is visible.
        let mut val = DatabaseEntry::new();
        let status =
            db.get(None, &DatabaseEntry::from_bytes(key), &mut val).unwrap();
        assert_eq!(status, OperationStatus::Success);
        assert_eq!(val.get_data(), Some(b"v".as_slice()));
    }

    // Phase 3: reopen one more time.  The XID must NOT reappear (it was
    // resolved by the TxnCommit frame written in phase 2).
    {
        let (xa, _db) = make_xa_with_log(dir.path());
        let recovered = xa.xa_recover(XaFlags::STARTRSCAN).unwrap();
        assert!(
            !recovered.contains(&xid),
            "resolved XID should NOT reappear: {recovered:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// 3. xa_rollback after restart succeeds and prepared writes are discarded.
// ---------------------------------------------------------------------------

#[test]
fn xa_rollback_after_restart_succeeds() {
    let dir = TempDir::new().unwrap();
    let xid = Xid::new(1, b"v2_rb_after_restart", b"br").unwrap();
    let key = b"k_after_restart_rb";

    {
        let (xa, db) = make_xa_with_log(dir.path());
        xa.xa_start(&xid, XaFlags::NOFLAGS).unwrap();
        {
            let txn = xa.get_transaction(&xid).unwrap();
            db.put(
                Some(txn),
                &DatabaseEntry::from_bytes(key),
                &DatabaseEntry::from_bytes(b"v"),
            )
            .unwrap();
        }
        xa.xa_end(&xid, XaFlags::TMSUCCESS).unwrap();
        xa.xa_prepare(&xid, XaFlags::NOFLAGS).unwrap();
    }

    {
        let (xa, db) = make_xa_with_log(dir.path());
        let recovered = xa.xa_recover(XaFlags::STARTRSCAN).unwrap();
        assert!(recovered.contains(&xid));

        xa.xa_rollback(&xid, XaFlags::NOFLAGS).unwrap();

        // Prepared write must NOT be visible.
        let mut val = DatabaseEntry::new();
        let status =
            db.get(None, &DatabaseEntry::from_bytes(key), &mut val).unwrap();
        assert_eq!(status, OperationStatus::NotFound);
    }
}

// ---------------------------------------------------------------------------
// 4. xa_commit / xa_rollback of a *truly unknown* XID still returns NotFound.
// ---------------------------------------------------------------------------

#[test]
fn xa_commit_unknown_xid_returns_not_found() {
    let dir = TempDir::new().unwrap();
    let (xa, _db) = make_xa_with_log(dir.path());
    let xid = Xid::new(1, b"never_seen", b"br").unwrap();

    let result = xa.xa_commit(&xid, XaFlags::NOFLAGS);
    assert!(
        matches!(result, Err(XaError::NotFound)),
        "unknown XID must surface as NotFound, got {result:?}"
    );
}

// ---------------------------------------------------------------------------
// 5. xa_forget on a recovered persistent-only XID still succeeds.
// ---------------------------------------------------------------------------

#[test]
fn xa_forget_after_restart_clears_persistent_log() {
    let dir = TempDir::new().unwrap();
    let xid = Xid::new(1, b"v2_forget_after_restart", b"br").unwrap();

    {
        let (xa, db) = make_xa_with_log(dir.path());
        xa.xa_start(&xid, XaFlags::NOFLAGS).unwrap();
        {
            let txn = xa.get_transaction(&xid).unwrap();
            db.put(
                Some(txn),
                &DatabaseEntry::from_bytes(b"k_forget"),
                &DatabaseEntry::from_bytes(b"v"),
            )
            .unwrap();
        }
        xa.xa_end(&xid, XaFlags::TMSUCCESS).unwrap();
        xa.xa_prepare(&xid, XaFlags::NOFLAGS).unwrap();
    }

    {
        let (xa, _db) = make_xa_with_log(dir.path());
        // Confirm it appears as in-doubt.
        let recovered = xa.xa_recover(XaFlags::STARTRSCAN).unwrap();
        assert!(recovered.contains(&xid));

        // Forget should succeed and remove it from the in-doubt list.
        xa.xa_forget(&xid, XaFlags::NOFLAGS).unwrap();
        let recovered = xa.xa_recover(XaFlags::STARTRSCAN).unwrap();
        assert!(!recovered.contains(&xid));

        // Now it really is unknown — commit returns NotFound.
        let result = xa.xa_commit(&xid, XaFlags::NOFLAGS);
        assert!(matches!(result, Err(XaError::NotFound)));
    }
}

// ---------------------------------------------------------------------------
// 6. xa_prepare auto-detects writes (no explicit mark_write needed).
// ---------------------------------------------------------------------------

#[test]
fn xa_prepare_auto_detects_writes_without_mark_write() {
    let dir = TempDir::new().unwrap();
    let (xa, db) = make_xa_no_log(dir.path());
    let xid = Xid::new(1, b"auto_detect_writes", b"br").unwrap();

    xa.xa_start(&xid, XaFlags::NOFLAGS).unwrap();
    {
        let txn = xa.get_transaction(&xid).unwrap();
        db.put(
            Some(txn),
            &DatabaseEntry::from_bytes(b"auto_k"),
            &DatabaseEntry::from_bytes(b"auto_v"),
        )
        .unwrap();
    }
    // Deliberately DO NOT call xa.mark_write(&xid).
    xa.xa_end(&xid, XaFlags::TMSUCCESS).unwrap();

    // Auto-detect: the inner Transaction has logged entries, so prepare
    // must return Ok (NOT ReadOnly), preserving the user's writes for the
    // second phase.
    let result = xa.xa_prepare(&xid, XaFlags::NOFLAGS).unwrap();
    assert_eq!(
        result,
        PrepareResult::Ok,
        "xa_prepare must auto-detect writes when mark_write was not called"
    );

    xa.xa_commit(&xid, XaFlags::NOFLAGS).unwrap();

    // Verify the data really was committed (not silently dropped by the
    // read-only optimisation).
    let mut val = DatabaseEntry::new();
    let status =
        db.get(None, &DatabaseEntry::from_bytes(b"auto_k"), &mut val).unwrap();
    assert_eq!(
        status,
        OperationStatus::Success,
        "auto-detect must preserve writes through prepare+commit"
    );
    assert_eq!(val.get_data(), Some(b"auto_v".as_slice()));
}

// ---------------------------------------------------------------------------
// 7. Truly read-only branch still gets the read-only optimisation.
// ---------------------------------------------------------------------------

#[test]
fn xa_prepare_read_only_branch_still_returns_read_only() {
    let dir = TempDir::new().unwrap();
    let (xa, _db) = make_xa_no_log(dir.path());
    let xid = Xid::new(1, b"true_readonly", b"br").unwrap();

    xa.xa_start(&xid, XaFlags::NOFLAGS).unwrap();
    // No writes performed at all.
    xa.xa_end(&xid, XaFlags::TMSUCCESS).unwrap();

    let result = xa.xa_prepare(&xid, XaFlags::NOFLAGS).unwrap();
    assert_eq!(
        result,
        PrepareResult::ReadOnly,
        "read-only branches must still take the read-only optimisation"
    );
}
