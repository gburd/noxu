//! Sprint 3 regression tests \u2014 v1.5 XA is in-process only.
//!
//! See `docs/src/internal/sprint-3-xa-restriction.md` for the rationale.
//!
//! These tests pin down the v1.5 contract that:
//!
//! 1. `xa_recover` on a fresh `XaEnvironment` (no prior `xa_prepare`) returns
//!    an empty list \u2014 i.e. no spurious entries.
//! 2. `xa_commit` of an XID that survives only in the persistent prepared
//!    log returns the new typed error
//!    [`XaError::CrashDurabilityNotSupported`], not `NotFound` and not a
//!    spurious success.
//! 3. `xa_rollback` of such an XID likewise returns
//!    `CrashDurabilityNotSupported`.
//! 4. `xa_forget` of such an XID still succeeds \u2014 operators must be able
//!    to clear the persistent record without an in-memory branch.
//! 5. `xa_prepare` auto-detects writes performed via the inner
//!    `Transaction` \u2014 a user that forgets to call `mark_write` no longer
//!    silently slides into the read-only optimisation and aborts their
//!    work.
//! 6. A truly read-only branch still returns `PrepareResult::ReadOnly`
//!    (the auto-detect must not produce a false positive on read-only
//!    workloads).

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
// 2. xa_commit after restart returns CrashDurabilityNotSupported
// ---------------------------------------------------------------------------

#[test]
fn xa_commit_after_restart_returns_crash_durability_not_supported() {
    let dir = TempDir::new().unwrap();
    let xid = Xid::new(1, b"sprint3_commit_after_restart", b"br").unwrap();

    // Phase 1: prepare and "crash" (drop without committing).
    {
        let (xa, db) = make_xa_with_log(dir.path());
        xa.xa_start(&xid, XaFlags::NOFLAGS).unwrap();
        {
            let txn = xa.get_transaction(&xid).unwrap();
            db.put(
                Some(txn),
                &DatabaseEntry::from_bytes(b"k_after_restart_commit"),
                &DatabaseEntry::from_bytes(b"v"),
            )
            .unwrap();
        }
        xa.mark_write(&xid).unwrap();
        xa.xa_end(&xid, XaFlags::TMSUCCESS).unwrap();
        xa.xa_prepare(&xid, XaFlags::NOFLAGS).unwrap();
        // Drop xa + db + env without committing \u2014 simulates a crash.
    }

    // Phase 2: reopen. The XID is in the persistent log but the in-memory
    // branch is gone; xa_commit must fail with CrashDurabilityNotSupported.
    {
        let (xa, _db) = make_xa_with_log(dir.path());
        let recovered = xa.xa_recover(XaFlags::STARTRSCAN).unwrap();
        assert!(
            recovered.contains(&xid),
            "recovered XIDs should include the prepared one: {recovered:?}"
        );

        let result = xa.xa_commit(&xid, XaFlags::NOFLAGS);
        assert!(
            matches!(result, Err(XaError::CrashDurabilityNotSupported)),
            "expected CrashDurabilityNotSupported, got {result:?}"
        );

        // The error message must mention v1.5 and xa_forget so the
        // operator knows what to do.
        let msg = format!("{}", result.unwrap_err());
        assert!(msg.contains("v1.5"), "error must mention v1.5: {msg}");
        assert!(
            msg.contains("xa_forget"),
            "error must mention xa_forget: {msg}"
        );
    }
}

// ---------------------------------------------------------------------------
// 3. xa_rollback after restart returns CrashDurabilityNotSupported
// ---------------------------------------------------------------------------

#[test]
fn xa_rollback_after_restart_returns_crash_durability_not_supported() {
    let dir = TempDir::new().unwrap();
    let xid = Xid::new(1, b"sprint3_rb_after_restart", b"br").unwrap();

    {
        let (xa, db) = make_xa_with_log(dir.path());
        xa.xa_start(&xid, XaFlags::NOFLAGS).unwrap();
        {
            let txn = xa.get_transaction(&xid).unwrap();
            db.put(
                Some(txn),
                &DatabaseEntry::from_bytes(b"k_after_restart_rb"),
                &DatabaseEntry::from_bytes(b"v"),
            )
            .unwrap();
        }
        xa.mark_write(&xid).unwrap();
        xa.xa_end(&xid, XaFlags::TMSUCCESS).unwrap();
        xa.xa_prepare(&xid, XaFlags::NOFLAGS).unwrap();
    }

    {
        let (xa, _db) = make_xa_with_log(dir.path());
        let result = xa.xa_rollback(&xid, XaFlags::NOFLAGS);
        assert!(
            matches!(result, Err(XaError::CrashDurabilityNotSupported)),
            "expected CrashDurabilityNotSupported, got {result:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// 4. xa_commit / xa_rollback of a *truly unknown* XID still returns NotFound
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
// 5. xa_forget on a recovered persistent-only XID still succeeds
// ---------------------------------------------------------------------------

#[test]
fn xa_forget_after_restart_clears_persistent_log() {
    let dir = TempDir::new().unwrap();
    let xid = Xid::new(1, b"sprint3_forget_after_restart", b"br").unwrap();

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
        xa.mark_write(&xid).unwrap();
        xa.xa_end(&xid, XaFlags::TMSUCCESS).unwrap();
        xa.xa_prepare(&xid, XaFlags::NOFLAGS).unwrap();
    }

    {
        let (xa, _db) = make_xa_with_log(dir.path());
        // Confirm it appears as in-doubt.
        let recovered = xa.xa_recover(XaFlags::STARTRSCAN).unwrap();
        assert!(recovered.contains(&xid));

        // Forget should succeed and remove it from the persistent log.
        xa.xa_forget(&xid, XaFlags::NOFLAGS).unwrap();
        let recovered = xa.xa_recover(XaFlags::STARTRSCAN).unwrap();
        assert!(!recovered.contains(&xid));

        // Now it really is unknown \u2014 commit returns NotFound, not
        // CrashDurabilityNotSupported.
        let result = xa.xa_commit(&xid, XaFlags::NOFLAGS);
        assert!(matches!(result, Err(XaError::NotFound)));
    }
}

// NOTE: Auto-detect-writes tests are added in the
// `fix(xa): auto-detect writes in xa_prepare` commit.

// ---------------------------------------------------------------------------
// 6. xa_prepare auto-detects writes (no explicit mark_write needed)
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
// 7. Truly read-only branch still gets the read-only optimisation
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
