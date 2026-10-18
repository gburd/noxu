//! Wave 3-2 tests: crash-durable XA two-phase commit.
//!
//! Closes audit Critical C5 (the 2026 review):
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
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
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
            db.put_in(&txn, key, val).unwrap();
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
        let status = db.get_into(None, key, &mut got).unwrap();
        assert!(status);
        assert_eq!(got.data_opt(), Some(val.as_slice()));
    }

    // Phase 3: reopen one more time. Data must be durably committed (TxnCommit
    // frame written by xa_commit), and the XID must NOT reappear in xa_recover.
    {
        let (xa, db) = open_xa(dir.path());
        let recovered = xa.xa_recover(XaFlags::STARTRSCAN).unwrap();
        assert!(recovered.is_empty(), "resolved XID must not reappear");

        let mut got = DatabaseEntry::new();
        let status = db.get_into(None, key, &mut got).unwrap();
        assert!(
            status,
            "data must survive the second crash because TxnCommit was \
             written durably"
        );
        assert_eq!(got.data_opt(), Some(val.as_slice()));
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
            db.put_in(&txn, key, b"v").unwrap();
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
        let status = db.get_into(None, key, &mut got).unwrap();
        assert!(!status);
    }

    // Final reopen: durable.
    {
        let (xa, db) = open_xa(dir.path());
        let recovered = xa.xa_recover(XaFlags::STARTRSCAN).unwrap();
        assert!(recovered.is_empty());

        let mut got = DatabaseEntry::new();
        let status = db.get_into(None, key, &mut got).unwrap();
        assert!(!status);
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
            db.put_in(&txn, key_c, b"vc").unwrap();
        }
        xa.xa_end(&xid_commit, XaFlags::TMSUCCESS).unwrap();
        xa.xa_prepare(&xid_commit, XaFlags::NOFLAGS).unwrap();

        xa.xa_start(&xid_rollback, XaFlags::NOFLAGS).unwrap();
        {
            let txn = xa.get_transaction(&xid_rollback).unwrap();
            db.put_in(&txn, key_r, b"vr").unwrap();
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

        let status_c = db.get_into(None, key_c, &mut val).unwrap();
        assert!(status_c);
        assert_eq!(val.data_opt(), Some(b"vc".as_slice()));

        let mut val_r = DatabaseEntry::new();
        let status_r = db.get_into(None, key_r, &mut val_r).unwrap();
        assert!(!status_r);
    }

    // Phase 3: durable.
    {
        let (xa, db) = open_xa(dir.path());
        let recovered = xa.xa_recover(XaFlags::STARTRSCAN).unwrap();
        assert!(recovered.is_empty());
        let mut val = DatabaseEntry::new();
        assert!(db.get_into(None, key_c, &mut val).unwrap());
        let mut val_r = DatabaseEntry::new();
        assert!(!db.get_into(None, key_r, &mut val_r).unwrap());
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
            db.put_in(&txn, b"dc_k", b"dc_v").unwrap();
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
            vec![xid],
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
        db.put_in(&txn, b"k", b"v").unwrap();
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
        db.put_in(&txn, b"k", b"v").unwrap();
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
            db.put_in(&txn, b"k", b"v").unwrap();
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
                db.put_in(&txn, key.as_bytes(), b"v").unwrap();
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

// ===========================================================================
// R-XARECOVERED witness #2: xa_rollback of a recovered prepared txn RESTORES
// before-images.  A prepared UPDATE of an existing key must restore the old
// value; a prepared DELETE of an existing key must restore the record; a
// prepared INSERT of a new key must leave the key absent.
// ===========================================================================

#[test]
fn recovered_prepared_rollback_restores_before_images() {
    let dir = TempDir::new().unwrap();
    let xid = Xid::new(1, b"w32_rb_beforeimg", b"br1").unwrap();

    // Seed: commit k_upd=old and k_del=present (non-XA).
    {
        let (xa, db) = open_xa(dir.path());
        let txn = xa.inner().begin_transaction(None).unwrap();
        db.put_in(&txn, b"k_upd", b"old_value").unwrap();
        db.put_in(&txn, b"k_del", b"to_be_deleted").unwrap();
        txn.commit().unwrap();
        xa.inner().checkpoint(None).ok();
    }

    // Prepare an XA txn that updates k_upd, deletes k_del, inserts k_ins; crash.
    {
        let (xa, db) = open_xa(dir.path());
        xa.xa_start(&xid, XaFlags::NOFLAGS).unwrap();
        {
            let txn = xa.get_transaction(&xid).unwrap();
            db.put_in(&txn, b"k_upd", b"new_value").unwrap();
            db.delete_in(&txn, b"k_del").unwrap();
            db.put_in(&txn, b"k_ins", b"inserted").unwrap();
        }
        xa.xa_end(&xid, XaFlags::TMSUCCESS).unwrap();
        xa.xa_prepare(&xid, XaFlags::NOFLAGS).unwrap();
        // crash (drop)
    }

    // Reopen, recover, ROLLBACK — before-images must be restored.
    {
        let (xa, db) = open_xa(dir.path());
        let recovered = xa.xa_recover(XaFlags::STARTRSCAN).unwrap();
        assert_eq!(recovered, vec![xid.clone()]);
        xa.xa_rollback(&xid, XaFlags::NOFLAGS).unwrap();

        let mut got = DatabaseEntry::new();
        assert!(db.get_into(None, b"k_upd", &mut got).unwrap());
        assert_eq!(
            got.data_opt(),
            Some(b"old_value".as_slice()),
            "rollback must restore the UPDATE's before-image"
        );
        let mut got = DatabaseEntry::new();
        assert!(
            db.get_into(None, b"k_del", &mut got).unwrap(),
            "rollback must restore the DELETE's record"
        );
        assert_eq!(got.data_opt(), Some(b"to_be_deleted".as_slice()));
        let mut got = DatabaseEntry::new();
        assert!(
            !db.get_into(None, b"k_ins", &mut got).unwrap(),
            "rollback must leave the INSERT's key absent"
        );
    }

    // Final reopen: durable (TxnAbort written; next recovery undoes via log).
    {
        let (xa, db) = open_xa(dir.path());
        assert!(xa.xa_recover(XaFlags::STARTRSCAN).unwrap().is_empty());
        let mut got = DatabaseEntry::new();
        assert!(db.get_into(None, b"k_upd", &mut got).unwrap());
        assert_eq!(got.data_opt(), Some(b"old_value".as_slice()));
        let mut got = DatabaseEntry::new();
        assert!(db.get_into(None, b"k_del", &mut got).unwrap());
        assert_eq!(got.data_opt(), Some(b"to_be_deleted".as_slice()));
        let mut got = DatabaseEntry::new();
        assert!(!db.get_into(None, b"k_ins", &mut got).unwrap());
    }
}

// ===========================================================================
// R-XARECOVERED witness #4: a reader of an IN-DOUBT (recovered prepared,
// unresolved) key BLOCKS (surfaces the typed LockTimeout) because the
// recovered prepared txn holds the WRITE lock (JE isolation).  A reader of
// an UNRELATED key is unaffected.  xa_recover lists the in-doubt XID.
// ===========================================================================

#[test]
fn recovered_prepared_reader_blocks_until_resolution() {
    use noxu_db::NoxuError;
    let dir = TempDir::new().unwrap();
    let xid = Xid::new(1, b"w32_reader_blocks", b"br1").unwrap();

    // Seed an unrelated committed key.
    {
        let (xa, db) = open_xa(dir.path());
        let txn = xa.inner().begin_transaction(None).unwrap();
        db.put_in(&txn, b"other", b"other_val").unwrap();
        txn.commit().unwrap();
    }

    // Prepare an XA txn writing the in-doubt key; crash.
    {
        let (xa, db) = open_xa(dir.path());
        xa.xa_start(&xid, XaFlags::NOFLAGS).unwrap();
        {
            let txn = xa.get_transaction(&xid).unwrap();
            db.put_in(&txn, b"indoubt", b"indoubt_val").unwrap();
        }
        xa.xa_end(&xid, XaFlags::TMSUCCESS).unwrap();
        xa.xa_prepare(&xid, XaFlags::NOFLAGS).unwrap();
    }

    // Reopen with a SHORT lock timeout.  Recovery relocks the in-doubt key.
    let env_cfg = EnvironmentConfig::new(dir.path().to_path_buf())
        .with_allow_create(false)
        .with_transactional(true)
        .with_lock_timeout(300);
    let env = Environment::open(env_cfg).unwrap();
    let db = env
        .open_database(
            None,
            "wave3_2_db",
            &DatabaseConfig::new().with_transactional(true),
        )
        .unwrap();
    let xa = XaEnvironment::new(env);

    // xa_recover lists the in-doubt XID.
    let recovered = xa.xa_recover(XaFlags::STARTRSCAN).unwrap();
    assert_eq!(
        recovered,
        vec![xid.clone()],
        "xa_recover lists the in-doubt xid"
    );

    // A reader of the IN-DOUBT key blocks -> LockTimeout.
    {
        let rtxn = xa.inner().begin_transaction(None).unwrap();
        let res = db.get_in(&rtxn, b"indoubt");
        assert!(
            matches!(res, Err(NoxuError::LockTimeout { .. })),
            "reader of an in-doubt key must block and surface LockTimeout; got {res:?}"
        );
        rtxn.abort().ok();
    }

    // A reader of an UNRELATED key is unaffected.
    {
        let rtxn = xa.inner().begin_transaction(None).unwrap();
        let v = db.get_in(&rtxn, b"other").unwrap();
        assert_eq!(v.as_deref(), Some(b"other_val".as_slice()));
        rtxn.commit().unwrap();
    }

    // After xa_commit, the key is readable (lock released).
    xa.xa_commit(&xid, XaFlags::NOFLAGS).unwrap();
    {
        let rtxn = xa.inner().begin_transaction(None).unwrap();
        let v = db.get_in(&rtxn, b"indoubt").unwrap();
        assert_eq!(v.as_deref(), Some(b"indoubt_val".as_slice()));
        rtxn.commit().unwrap();
    }
}

// ===========================================================================
// R-XARECOVERED witness #3: the first-active-txn clamp holds the WHOLE
// unresolved recovered-prepared window across SEVERAL checkpoints + cleaner
// passes; after resolution there is NO stale pin (first_active_lsn == NULL).
// ===========================================================================

#[test]
fn recovered_prepared_clamp_holds_window_no_stale_pin() {
    let dir = TempDir::new().unwrap();
    let xid = Xid::new(1, b"w32_clamp_window", b"br1").unwrap();

    // Prepare an XA txn (several keys) and crash.
    {
        let (xa, db) = open_xa(dir.path());
        xa.xa_start(&xid, XaFlags::NOFLAGS).unwrap();
        {
            let txn = xa.get_transaction(&xid).unwrap();
            for i in 0u32..8 {
                db.put_in(&txn, (200_000u32 + i).to_be_bytes(), b"clamp_val")
                    .unwrap();
            }
        }
        xa.xa_end(&xid, XaFlags::TMSUCCESS).unwrap();
        xa.xa_prepare(&xid, XaFlags::NOFLAGS).unwrap();
    }

    // Reopen; recovery registers the clamp for the recovered prepared txn.
    let (xa, _db) = open_xa(dir.path());
    assert_eq!(xa.xa_recover(XaFlags::STARTRSCAN).unwrap(), vec![xid.clone()]);

    // The clamp is active (non-NULL) BEFORE resolution.
    let clamp_before = xa
        .inner()
        .first_active_lsn_for_test()
        .expect("the recovered prepared txn must clamp the first-active LSN");

    // Several checkpoints + cleaner passes while the branch is in-doubt.
    for _ in 0..5 {
        xa.inner().clean_log_forced().ok();
        xa.inner()
            .checkpoint(Some(
                &noxu_db::CheckpointConfig::new().with_force(true),
            ))
            .unwrap();
    }

    // Still in-doubt, still clamped (the clamp never advances past the
    // unresolved window regardless of how many checkpoints run).
    assert_eq!(xa.xa_recover(XaFlags::STARTRSCAN).unwrap(), vec![xid.clone()]);
    let clamp_mid = xa
        .inner()
        .first_active_lsn_for_test()
        .expect("clamp must stay held while in-doubt");
    assert_eq!(
        clamp_mid, clamp_before,
        "the clamp must stay pinned at the prepared txn's first LSN"
    );

    // Resolve; the clamp drops to NULL (no stale pin left behind).
    xa.xa_commit(&xid, XaFlags::NOFLAGS).unwrap();
    assert_eq!(
        xa.inner().first_active_lsn_for_test(),
        None,
        "after resolution the clamp must release (no stale pin)"
    );
    // Drop the resolving env before reopening (single-process env lock).
    drop(_db);
    drop(xa);

    // Data present + durable.
    {
        let (xa, db) = open_xa(dir.path());
        assert!(xa.xa_recover(XaFlags::STARTRSCAN).unwrap().is_empty());
        let rtxn = xa.inner().begin_transaction(None).unwrap();
        for i in 0u32..8 {
            let v = db.get_in(&rtxn, (200_000u32 + i).to_be_bytes()).unwrap();
            assert_eq!(v.as_deref(), Some(b"clamp_val".as_slice()));
        }
        rtxn.commit().unwrap();
    }
}

// ===========================================================================
// R-XARECOVERED witness #5 (secondary index): a prepared XA txn that writes
// through a PRIMARY with an attached SECONDARY index logs secondary LNs under
// the SAME txn id, so they are also redone+relocked during recovery.  After
// recover+commit, the secondary lookup resolves to the primary record; after
// recover+rollback, the secondary entry is gone.
// ===========================================================================

mod witness5_secondary {
    use super::*;
    use noxu_db::{SecondaryConfig, SecondaryDatabase, SecondaryKeyCreator};
    use noxu_sync::Mutex;
    use std::sync::Arc;

    // sec key = first byte of the data value.
    struct FirstByteKeyCreator;
    impl SecondaryKeyCreator for FirstByteKeyCreator {
        fn create_secondary_key(
            &self,
            _db: &Database,
            _key: &DatabaseEntry,
            data: &DatabaseEntry,
            result: &mut DatabaseEntry,
        ) -> bool {
            if let Some(d) = data.data_opt() {
                if !d.is_empty() {
                    result.set_data(&[d[0]]);
                    return true;
                }
            }
            false
        }
    }

    fn open_primary_secondary(
        dir: &std::path::Path,
    ) -> (XaEnvironment, Arc<Mutex<Database>>, SecondaryDatabase) {
        let env = Environment::open(
            EnvironmentConfig::new(dir.to_path_buf())
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap();
        let primary = env
            .open_database(
                None,
                "prim",
                &DatabaseConfig::new()
                    .with_allow_create(true)
                    .with_transactional(true),
            )
            .unwrap();
        let primary = Arc::new(Mutex::new(primary));
        let sec_inner = env
            .open_database(
                None,
                "sec",
                &DatabaseConfig::new()
                    .with_allow_create(true)
                    .with_transactional(true)
                    .with_sorted_duplicates(true),
            )
            .unwrap();
        let sec = SecondaryDatabase::open(
            Arc::clone(&primary),
            sec_inner,
            SecondaryConfig::new()
                .with_allow_create(true)
                .with_key_creator(Box::new(FirstByteKeyCreator)),
        )
        .unwrap();
        let xa = XaEnvironment::new(env);
        (xa, primary, sec)
    }

    #[test]
    fn recovered_prepared_commit_updates_secondary() {
        let dir = TempDir::new().unwrap();
        let xid = Xid::new(1, b"w32_sec_commit", b"br1").unwrap();

        // Prepare: write primary key "pk" = data "Avalue" (sec key = 'A').
        {
            let (xa, primary, _sec) = open_primary_secondary(dir.path());
            xa.xa_start(&xid, XaFlags::NOFLAGS).unwrap();
            {
                let txn = xa.get_transaction(&xid).unwrap();
                primary.lock().put_in(&txn, b"pk", b"Avalue").unwrap();
            }
            xa.xa_end(&xid, XaFlags::TMSUCCESS).unwrap();
            xa.xa_prepare(&xid, XaFlags::NOFLAGS).unwrap();
        }

        // Reopen, recover, commit; the secondary must resolve 'A' -> "Avalue".
        {
            let (xa, _primary, sec) = open_primary_secondary(dir.path());
            assert_eq!(
                xa.xa_recover(XaFlags::STARTRSCAN).unwrap(),
                vec![xid.clone()]
            );
            xa.xa_commit(&xid, XaFlags::NOFLAGS).unwrap();

            let mut pk = DatabaseEntry::new();
            let mut pv = DatabaseEntry::new();
            let found = sec.get_into(None, b"A", &mut pk, &mut pv).unwrap();
            assert!(found, "secondary 'A' must resolve after recovered commit");
            assert_eq!(pk.data_opt(), Some(b"pk".as_slice()));
            assert_eq!(pv.data_opt(), Some(b"Avalue".as_slice()));
        }
    }

    #[test]
    fn recovered_prepared_rollback_clears_secondary() {
        let dir = TempDir::new().unwrap();
        let xid = Xid::new(1, b"w32_sec_rollback", b"br1").unwrap();

        {
            let (xa, primary, _sec) = open_primary_secondary(dir.path());
            xa.xa_start(&xid, XaFlags::NOFLAGS).unwrap();
            {
                let txn = xa.get_transaction(&xid).unwrap();
                primary.lock().put_in(&txn, b"pk", b"Bvalue").unwrap();
            }
            xa.xa_end(&xid, XaFlags::TMSUCCESS).unwrap();
            xa.xa_prepare(&xid, XaFlags::NOFLAGS).unwrap();
        }

        {
            let (xa, _primary, sec) = open_primary_secondary(dir.path());
            assert_eq!(
                xa.xa_recover(XaFlags::STARTRSCAN).unwrap(),
                vec![xid.clone()]
            );
            xa.xa_rollback(&xid, XaFlags::NOFLAGS).unwrap();

            let mut pk = DatabaseEntry::new();
            let mut pv = DatabaseEntry::new();
            let found = sec.get_into(None, b"B", &mut pk, &mut pv).unwrap();
            assert!(
                !found,
                "secondary 'B' must be absent after recovered rollback"
            );
        }
    }
}
