//! Independent review witnesses (R-XARECOVERED) — undo correctness gaps the
//! author's suite does not explicitly cover.
//!
//! Added by the INDEPENDENT reviewer (not the author) to probe task-2 claims:
//!  * multiple writes to the SAME key in one prepared txn -> rollback must
//!    restore the ORIGINAL pre-txn committed value (the fix relies on reverse
//!    iteration to the earliest before-image).
//!  * insert-then-delete the same key in one prepared txn -> rollback leaves
//!    the key ABSENT (original state).
//!  * a fresh-insert same key written twice -> rollback leaves it ABSENT.
//!
//! GREEN = undo restored the exact pre-txn state.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use noxu_db::{
    Database, DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig,
};
use noxu_xa::{XaEnvironment, XaFlags, XaResource, Xid};
use tempfile::TempDir;

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
            "undo_db",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap();
    (XaEnvironment::new(env), db)
}

/// A prepared txn updates the SAME existing key THREE times, then crashes.
/// After recover+rollback the key must hold the ORIGINAL pre-txn value, not
/// any of the intermediate uncommitted versions.
#[test]
fn rollback_same_key_multiple_updates_restores_original() {
    let dir = TempDir::new().unwrap();
    let xid = Xid::new(1, b"rv_multi_upd", b"br1").unwrap();

    // Seed k = "ORIGINAL" (committed, non-XA).
    {
        let (xa, db) = open_xa(dir.path());
        let t = xa.inner().begin_transaction(None).unwrap();
        db.put_in(&t, b"k", b"ORIGINAL").unwrap();
        t.commit().unwrap();
        xa.inner().checkpoint(None).ok();
    }

    // Prepared txn writes k three times, then crash (drop without resolving).
    {
        let (xa, db) = open_xa(dir.path());
        xa.xa_start(&xid, XaFlags::NOFLAGS).unwrap();
        {
            let txn = xa.get_transaction(&xid).unwrap();
            db.put_in(&txn, b"k", b"v1").unwrap();
            db.put_in(&txn, b"k", b"v2").unwrap();
            db.put_in(&txn, b"k", b"v3").unwrap();
        }
        xa.xa_end(&xid, XaFlags::TMSUCCESS).unwrap();
        xa.xa_prepare(&xid, XaFlags::NOFLAGS).unwrap();
    }

    // Recover + rollback: k must be the ORIGINAL pre-txn value.
    {
        let (xa, db) = open_xa(dir.path());
        assert_eq!(
            xa.xa_recover(XaFlags::STARTRSCAN).unwrap(),
            vec![xid.clone()]
        );
        xa.xa_rollback(&xid, XaFlags::NOFLAGS).unwrap();
        let mut got = DatabaseEntry::new();
        assert!(db.get_into(None, b"k", &mut got).unwrap());
        assert_eq!(
            got.data_opt(),
            Some(b"ORIGINAL".as_slice()),
            "rollback of a thrice-updated key must restore the ORIGINAL \
             pre-txn value, not an intermediate uncommitted version"
        );
    }

    // Durable across a further reopen (TxnAbort written; next recovery undoes).
    {
        let (xa, db) = open_xa(dir.path());
        assert!(xa.xa_recover(XaFlags::STARTRSCAN).unwrap().is_empty());
        let mut got = DatabaseEntry::new();
        assert!(db.get_into(None, b"k", &mut got).unwrap());
        assert_eq!(got.data_opt(), Some(b"ORIGINAL".as_slice()));
    }
}

/// A prepared txn INSERTS a brand-new key twice (no pre-txn value), then
/// crash.  After recover+rollback the key must be ABSENT.
#[test]
fn rollback_same_key_double_insert_leaves_absent() {
    let dir = TempDir::new().unwrap();
    let xid = Xid::new(1, b"rv_dbl_ins", b"br1").unwrap();

    {
        let (xa, _db) = open_xa(dir.path());
        xa.inner().checkpoint(None).ok();
    }
    {
        let (xa, db) = open_xa(dir.path());
        xa.xa_start(&xid, XaFlags::NOFLAGS).unwrap();
        {
            let txn = xa.get_transaction(&xid).unwrap();
            db.put_in(&txn, b"newk", b"first").unwrap();
            db.put_in(&txn, b"newk", b"second").unwrap();
        }
        xa.xa_end(&xid, XaFlags::TMSUCCESS).unwrap();
        xa.xa_prepare(&xid, XaFlags::NOFLAGS).unwrap();
    }
    {
        let (xa, db) = open_xa(dir.path());
        assert_eq!(
            xa.xa_recover(XaFlags::STARTRSCAN).unwrap(),
            vec![xid.clone()]
        );
        xa.xa_rollback(&xid, XaFlags::NOFLAGS).unwrap();
        let mut got = DatabaseEntry::new();
        assert!(
            !db.get_into(None, b"newk", &mut got).unwrap(),
            "rollback of a doubly-inserted new key must leave it ABSENT"
        );
    }
    {
        let (xa, db) = open_xa(dir.path());
        assert!(xa.xa_recover(XaFlags::STARTRSCAN).unwrap().is_empty());
        let mut got = DatabaseEntry::new();
        assert!(!db.get_into(None, b"newk", &mut got).unwrap());
    }
}

/// A prepared txn UPDATES an existing key then DELETES it (same key, two ops),
/// then crash.  After recover+rollback the key must hold the ORIGINAL value
/// (the delete and the update are both undone to the pre-txn state).
#[test]
fn rollback_same_key_update_then_delete_restores_original() {
    let dir = TempDir::new().unwrap();
    let xid = Xid::new(1, b"rv_upd_del", b"br1").unwrap();

    {
        let (xa, db) = open_xa(dir.path());
        let t = xa.inner().begin_transaction(None).unwrap();
        db.put_in(&t, b"k", b"ORIG").unwrap();
        t.commit().unwrap();
        xa.inner().checkpoint(None).ok();
    }
    {
        let (xa, db) = open_xa(dir.path());
        xa.xa_start(&xid, XaFlags::NOFLAGS).unwrap();
        {
            let txn = xa.get_transaction(&xid).unwrap();
            db.put_in(&txn, b"k", b"changed").unwrap();
            db.delete_in(&txn, b"k").unwrap();
        }
        xa.xa_end(&xid, XaFlags::TMSUCCESS).unwrap();
        xa.xa_prepare(&xid, XaFlags::NOFLAGS).unwrap();
    }
    {
        let (xa, db) = open_xa(dir.path());
        assert_eq!(
            xa.xa_recover(XaFlags::STARTRSCAN).unwrap(),
            vec![xid.clone()]
        );
        xa.xa_rollback(&xid, XaFlags::NOFLAGS).unwrap();
        let mut got = DatabaseEntry::new();
        assert!(
            db.get_into(None, b"k", &mut got).unwrap(),
            "rollback of update+delete must bring the record back"
        );
        assert_eq!(got.data_opt(), Some(b"ORIG".as_slice()));
    }
    {
        let (xa, db) = open_xa(dir.path());
        assert!(xa.xa_recover(XaFlags::STARTRSCAN).unwrap().is_empty());
        let mut got = DatabaseEntry::new();
        assert!(db.get_into(None, b"k", &mut got).unwrap());
        assert_eq!(got.data_opt(), Some(b"ORIG".as_slice()));
    }
}

/// COMMIT counterpart: a prepared txn updates the same key three times, then
/// crash + recover + COMMIT -> the key must hold the LAST value (v3).
#[test]
fn commit_same_key_multiple_updates_keeps_last() {
    let dir = TempDir::new().unwrap();
    let xid = Xid::new(1, b"rv_multi_commit", b"br1").unwrap();

    {
        let (xa, db) = open_xa(dir.path());
        let t = xa.inner().begin_transaction(None).unwrap();
        db.put_in(&t, b"k", b"ORIGINAL").unwrap();
        t.commit().unwrap();
        xa.inner().checkpoint(None).ok();
    }
    {
        let (xa, db) = open_xa(dir.path());
        xa.xa_start(&xid, XaFlags::NOFLAGS).unwrap();
        {
            let txn = xa.get_transaction(&xid).unwrap();
            db.put_in(&txn, b"k", b"v1").unwrap();
            db.put_in(&txn, b"k", b"v2").unwrap();
            db.put_in(&txn, b"k", b"v3").unwrap();
        }
        xa.xa_end(&xid, XaFlags::TMSUCCESS).unwrap();
        xa.xa_prepare(&xid, XaFlags::NOFLAGS).unwrap();
    }
    {
        let (xa, db) = open_xa(dir.path());
        assert_eq!(
            xa.xa_recover(XaFlags::STARTRSCAN).unwrap(),
            vec![xid.clone()]
        );
        xa.xa_commit(&xid, XaFlags::NOFLAGS).unwrap();
        let mut got = DatabaseEntry::new();
        assert!(db.get_into(None, b"k", &mut got).unwrap());
        assert_eq!(
            got.data_opt(),
            Some(b"v3".as_slice()),
            "commit of a thrice-updated key must keep the LAST value"
        );
    }
    {
        let (xa, db) = open_xa(dir.path());
        assert!(xa.xa_recover(XaFlags::STARTRSCAN).unwrap().is_empty());
        let mut got = DatabaseEntry::new();
        assert!(db.get_into(None, b"k", &mut got).unwrap());
        assert_eq!(got.data_opt(), Some(b"v3".as_slice()));
    }
}
