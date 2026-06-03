//! Wave 3-2 tests: crash-durable XA two-phase commit.
//!
//! Closes audit Critical C5 (`docs/src/internal/api-audit-2026-05-persist-xa.md`):
//!
//! > C5: xa_prepare records the XID in a fsync'd PreparedLog but never tells
//! > the underlying noxu-db::Transaction.  After a crash, recovery rolls the
//! > txn back unconditionally; xa_recover returns prepared XIDs but xa_commit
//! > fails with NotFound because the in-memory branches map is empty.
//! > Two-phase commit non-functional across a crash.
//!
//! These tests pin down the v2.0 contract:
//!
//!   1. prepare → "crash" (drop env) → reopen → xa_recover returns the XID
//!      → xa_commit succeeds → reopen → data IS in the database.
//!
//!   2. Same as (1) but with xa_rollback → data is NOT in the database.
//!
//!   3. Two prepared txns survive a crash, are recovered, one is committed
//!      and the other rolled back.
//!
//!   4. A prepared txn is recovered and the process crashes BEFORE
//!      xa_commit → recovery again must still see the prepared XID.
//!
//!   5. Negative tests for prepare preconditions.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use noxu_db::{
    Database, DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig,
    OperationStatus,
};
use noxu_xa::{
    PrepareResult, XaEnvironment, XaError, XaFlags, XaResource, Xid,
};
use tempfile::TempDir;

fn open_xa(dir: &std::path::Path) -> (XaEnvironment, Database) {
    let env_cfg = EnvironmentConfig::new(dir.to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    let env = Environment::open(env_cfg).unwrap();
    let db = env
        .open_database(
            None,
            "wave3_2_db",
            &DatabaseConfig::new().with_allow_create(true),
        )
        .unwrap();
    let xa = XaEnvironment::new(env);
    (xa, db)
}

// ===========================================================================
// 1. prepare → crash → recover → commit → reopen → data IS visible
// ===========================================================================

#[test]
fn prepare_crash_recover_commit_data_visible() {
    let dir = TempDir::new().unwrap();
    let xid = Xid::new(1, b"w32_commit_visible", b"br1").unwrap();
    let key = b"w32_k1";
    let val = b"w32_v1";

    // Phase 1: prepare and "crash" (drop env without close).
    {
        let (xa, db) = open_xa(dir.path());
        xa.xa_start(&xid, XaFlags::NOFLAGS).unwrap();
        {
            let txn = xa.get_transaction(&xid).unwrap();
            db.put(
                Some(&*txn),
                &DatabaseEntry::from_bytes(key),
                &DatabaseEntry::from_bytes(val),
            )
            .unwrap();
        }
        xa.xa_end(&xid, XaFlags::TMSUCCESS).unwrap();
        let result = xa.xa_prepare(&xid, XaFlags::NOFLAGS).unwrap();
        assert_eq!(result, PrepareResult::Ok);
        // Drop without commit — simulates crash.
    }

    // Phase 2: reopen, recover, commit.
    {
        let (xa, db) = open_xa(dir.path());
        let recovered = xa.xa_recover(XaFlags::STARTRSCAN).unwrap();
        assert_eq!(recovered.len(), 1, "exactly one in-doubt XID");
        assert_eq!(recovered[0], xid);

        xa.xa_commit(&xid, XaFlags::NOFLAGS).unwrap();

        // Verify visible immediately after xa_commit (apply_recovered_prepared_lns
        // replayed the LN into the in-memory tree).
        let mut got = DatabaseEntry::new();
        let status =
            db.get(None, &DatabaseEntry::from_bytes(key), &mut got).unwrap();
        assert_eq!(status, OperationStatus::Success);
        assert_eq!(got.get_data(), Some(val.as_slice()));
    }

    // Phase 3: reopen one more time. Data must be durably committed (TxnCommit
    // frame written by xa_commit), and the XID must NOT reappear in xa_recover.
    {
        let (xa, db) = open_xa(dir.path());
        let recovered = xa.xa_recover(XaFlags::STARTRSCAN).unwrap();
        assert!(recovered.is_empty(), "resolved XID must not reappear");

        let mut got = DatabaseEntry::new();
        let status =
            db.get(None, &DatabaseEntry::from_bytes(key), &mut got).unwrap();
        assert_eq!(
            status,
            OperationStatus::Success,
            "data must survive the second crash because TxnCommit was \
             written durably"
        );
        assert_eq!(got.get_data(), Some(val.as_slice()));
    }
}

// ===========================================================================
// 2. prepare → crash → recover → rollback → reopen → data NOT visible
// ===========================================================================

#[test]
fn prepare_crash_recover_rollback_data_not_visible() {
    let dir = TempDir::new().unwrap();
    let xid = Xid::new(1, b"w32_rollback_not_visible", b"br1").unwrap();
    let key = b"w32_rb_k1";

    {
        let (xa, db) = open_xa(dir.path());
        xa.xa_start(&xid, XaFlags::NOFLAGS).unwrap();
        {
            let txn = xa.get_transaction(&xid).unwrap();
            db.put(
                Some(&*txn),
                &DatabaseEntry::from_bytes(key),
                &DatabaseEntry::from_bytes(b"v"),
            )
            .unwrap();
        }
        xa.xa_end(&xid, XaFlags::TMSUCCESS).unwrap();
        xa.xa_prepare(&xid, XaFlags::NOFLAGS).unwrap();
    }

    {
        let (xa, db) = open_xa(dir.path());
        let recovered = xa.xa_recover(XaFlags::STARTRSCAN).unwrap();
        assert_eq!(recovered, vec![xid.clone()]);

        xa.xa_rollback(&xid, XaFlags::NOFLAGS).unwrap();

        let mut got = DatabaseEntry::new();
        let status =
            db.get(None, &DatabaseEntry::from_bytes(key), &mut got).unwrap();
        assert_eq!(status, OperationStatus::NotFound);
    }

    // Final reopen: durable.
    {
        let (xa, db) = open_xa(dir.path());
        let recovered = xa.xa_recover(XaFlags::STARTRSCAN).unwrap();
        assert!(recovered.is_empty());

        let mut got = DatabaseEntry::new();
        let status =
            db.get(None, &DatabaseEntry::from_bytes(key), &mut got).unwrap();
        assert_eq!(status, OperationStatus::NotFound);
    }
}

// ===========================================================================
// 3. Two prepared txns crash → recover both → commit one, rollback other
// ===========================================================================

#[test]
fn two_prepared_txns_crash_recover_mixed_resolution() {
    let dir = TempDir::new().unwrap();
    let xid_commit = Xid::new(1, b"w32_two_a_commit", b"br_c").unwrap();
    let xid_rollback = Xid::new(1, b"w32_two_b_rollback", b"br_r").unwrap();
    let key_c = b"w32_two_kc";
    let key_r = b"w32_two_kr";

    // Phase 1: prepare both, then "crash".
    {
        let (xa, db) = open_xa(dir.path());

        xa.xa_start(&xid_commit, XaFlags::NOFLAGS).unwrap();
        {
            let txn = xa.get_transaction(&xid_commit).unwrap();
            db.put(
                Some(&*txn),
                &DatabaseEntry::from_bytes(key_c),
                &DatabaseEntry::from_bytes(b"vc"),
            )
            .unwrap();
        }
        xa.xa_end(&xid_commit, XaFlags::TMSUCCESS).unwrap();
        xa.xa_prepare(&xid_commit, XaFlags::NOFLAGS).unwrap();

        xa.xa_start(&xid_rollback, XaFlags::NOFLAGS).unwrap();
        {
            let txn = xa.get_transaction(&xid_rollback).unwrap();
            db.put(
                Some(&*txn),
                &DatabaseEntry::from_bytes(key_r),
                &DatabaseEntry::from_bytes(b"vr"),
            )
            .unwrap();
        }
        xa.xa_end(&xid_rollback, XaFlags::TMSUCCESS).unwrap();
        xa.xa_prepare(&xid_rollback, XaFlags::NOFLAGS).unwrap();
    }

    // Phase 2: recover both, commit one, rollback the other.
    {
        let (xa, db) = open_xa(dir.path());
        let recovered = xa.xa_recover(XaFlags::STARTRSCAN).unwrap();
        assert_eq!(recovered.len(), 2);
        assert!(recovered.contains(&xid_commit));
        assert!(recovered.contains(&xid_rollback));

        xa.xa_commit(&xid_commit, XaFlags::NOFLAGS).unwrap();
        xa.xa_rollback(&xid_rollback, XaFlags::NOFLAGS).unwrap();

        let mut val = DatabaseEntry::new();

        let status_c =
            db.get(None, &DatabaseEntry::from_bytes(key_c), &mut val).unwrap();
        assert_eq!(status_c, OperationStatus::Success);
        assert_eq!(val.get_data(), Some(b"vc".as_slice()));

        let mut val_r = DatabaseEntry::new();
        let status_r = db
            .get(None, &DatabaseEntry::from_bytes(key_r), &mut val_r)
            .unwrap();
        assert_eq!(status_r, OperationStatus::NotFound);
    }

    // Phase 3: durable.
    {
        let (xa, db) = open_xa(dir.path());
        let recovered = xa.xa_recover(XaFlags::STARTRSCAN).unwrap();
        assert!(recovered.is_empty());
        let mut val = DatabaseEntry::new();
        assert_eq!(
            db.get(None, &DatabaseEntry::from_bytes(key_c), &mut val).unwrap(),
            OperationStatus::Success
        );
        let mut val_r = DatabaseEntry::new();
        assert_eq!(
            db.get(None, &DatabaseEntry::from_bytes(key_r), &mut val_r)
                .unwrap(),
            OperationStatus::NotFound
        );
    }
}

// ===========================================================================
// 4. Recovery → second crash before xa_commit → recovery again sees the XID
// ===========================================================================

#[test]
fn double_crash_before_resolution_keeps_xid_in_doubt() {
    let dir = TempDir::new().unwrap();
    let xid = Xid::new(1, b"w32_double_crash", b"br1").unwrap();

    // Phase 1: prepare and crash.
    {
        let (xa, db) = open_xa(dir.path());
        xa.xa_start(&xid, XaFlags::NOFLAGS).unwrap();
        {
            let txn = xa.get_transaction(&xid).unwrap();
            db.put(
                Some(&*txn),
                &DatabaseEntry::from_bytes(b"dc_k"),
                &DatabaseEntry::from_bytes(b"dc_v"),
            )
            .unwrap();
        }
        xa.xa_end(&xid, XaFlags::TMSUCCESS).unwrap();
        xa.xa_prepare(&xid, XaFlags::NOFLAGS).unwrap();
    }

    // Phase 2: reopen, see XID, but crash again BEFORE resolving.
    {
        let (xa, _db) = open_xa(dir.path());
        let recovered = xa.xa_recover(XaFlags::STARTRSCAN).unwrap();
        assert_eq!(recovered, vec![xid.clone()]);
        // Drop without xa_commit / xa_rollback / xa_forget.
    }

    // Phase 3: reopen yet again; XID must STILL be in-doubt because no
    // resolution frame was written.
    {
        let (xa, _db) = open_xa(dir.path());
        let recovered = xa.xa_recover(XaFlags::STARTRSCAN).unwrap();
        assert_eq!(
            recovered,
            vec![xid.clone()],
            "in-doubt XID must persist across multiple crashes \
             until explicitly resolved"
        );
    }
}

// ===========================================================================
// 5. Negative: prepare on a committed transaction is a protocol error
// ===========================================================================

#[test]
fn prepare_after_commit_is_protocol_error() {
    let dir = TempDir::new().unwrap();
    let (xa, db) = open_xa(dir.path());
    let xid = Xid::new(1, b"w32_neg_prepare_after_commit", b"br").unwrap();

    xa.xa_start(&xid, XaFlags::NOFLAGS).unwrap();
    {
        let txn = xa.get_transaction(&xid).unwrap();
        db.put(
            Some(&*txn),
            &DatabaseEntry::from_bytes(b"k"),
            &DatabaseEntry::from_bytes(b"v"),
        )
        .unwrap();
    }
    xa.xa_end(&xid, XaFlags::TMSUCCESS).unwrap();

    // One-phase commit.
    xa.xa_commit(&xid, XaFlags::ONEPHASE).unwrap();

    // Now the branch is gone.  Calling xa_prepare on it must return NotFound.
    let res = xa.xa_prepare(&xid, XaFlags::NOFLAGS);
    assert!(matches!(res, Err(XaError::NotFound)), "got {res:?}");
}

// ===========================================================================
// 5b. Negative: xa_prepare while branch is Active (not yet ended) is protocol
// ===========================================================================

#[test]
fn prepare_before_end_is_protocol_error() {
    let dir = TempDir::new().unwrap();
    let (xa, db) = open_xa(dir.path());
    let xid = Xid::new(1, b"w32_neg_prepare_before_end", b"br").unwrap();

    xa.xa_start(&xid, XaFlags::NOFLAGS).unwrap();
    {
        let txn = xa.get_transaction(&xid).unwrap();
        db.put(
            Some(&*txn),
            &DatabaseEntry::from_bytes(b"k"),
            &DatabaseEntry::from_bytes(b"v"),
        )
        .unwrap();
    }
    // Skip xa_end on purpose.

    let res = xa.xa_prepare(&xid, XaFlags::NOFLAGS);
    assert!(matches!(res, Err(XaError::Protocol(_))), "got {res:?}");

    // Cleanup.
    xa.xa_end(&xid, XaFlags::TMSUCCESS).unwrap();
    xa.xa_rollback(&xid, XaFlags::NOFLAGS).unwrap();
}

// ===========================================================================
// 5c. Negative: starting a new branch under a recovered XID is rejected
// ===========================================================================

#[test]
fn xa_start_on_recovered_xid_is_duplicate() {
    let dir = TempDir::new().unwrap();
    let xid = Xid::new(1, b"w32_neg_start_on_recovered", b"br").unwrap();

    {
        let (xa, db) = open_xa(dir.path());
        xa.xa_start(&xid, XaFlags::NOFLAGS).unwrap();
        {
            let txn = xa.get_transaction(&xid).unwrap();
            db.put(
                Some(&*txn),
                &DatabaseEntry::from_bytes(b"k"),
                &DatabaseEntry::from_bytes(b"v"),
            )
            .unwrap();
        }
        xa.xa_end(&xid, XaFlags::TMSUCCESS).unwrap();
        xa.xa_prepare(&xid, XaFlags::NOFLAGS).unwrap();
    }

    {
        let (xa, _db) = open_xa(dir.path());
        // The XID is recovered but not resolved.  Trying to xa_start a fresh
        // branch under the same XID must be rejected.
        let res = xa.xa_start(&xid, XaFlags::NOFLAGS);
        assert!(matches!(res, Err(XaError::DuplicateXid)), "got {res:?}");

        // Cleanup so we don't leave the dir in an in-doubt state.
        xa.xa_rollback(&xid, XaFlags::NOFLAGS).unwrap();
    }
}

// ===========================================================================
// 6. Recovered branch resolution forgets the XID from xa_recover output
// ===========================================================================

#[test]
fn resolved_xid_disappears_from_xa_recover() {
    let dir = TempDir::new().unwrap();
    let xid_a = Xid::new(1, b"w32_resolve_a", b"br").unwrap();
    let xid_b = Xid::new(1, b"w32_resolve_b", b"br").unwrap();

    {
        let (xa, db) = open_xa(dir.path());
        for (i, x) in [&xid_a, &xid_b].iter().enumerate() {
            xa.xa_start(x, XaFlags::NOFLAGS).unwrap();
            {
                let txn = xa.get_transaction(x).unwrap();
                let key = format!("k{i}");
                db.put(
                    Some(&*txn),
                    &DatabaseEntry::from_bytes(key.as_bytes()),
                    &DatabaseEntry::from_bytes(b"v"),
                )
                .unwrap();
            }
            xa.xa_end(x, XaFlags::TMSUCCESS).unwrap();
            xa.xa_prepare(x, XaFlags::NOFLAGS).unwrap();
        }
    }

    {
        let (xa, _db) = open_xa(dir.path());
        let recovered = xa.xa_recover(XaFlags::STARTRSCAN).unwrap();
        assert_eq!(recovered.len(), 2);

        xa.xa_commit(&xid_a, XaFlags::NOFLAGS).unwrap();

        // After committing one, recover should only show the other.
        let recovered = xa.xa_recover(XaFlags::STARTRSCAN).unwrap();
        assert_eq!(recovered, vec![xid_b.clone()]);

        xa.xa_rollback(&xid_b, XaFlags::NOFLAGS).unwrap();
        let recovered = xa.xa_recover(XaFlags::STARTRSCAN).unwrap();
        assert!(recovered.is_empty());
    }
}
