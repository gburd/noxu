//! Regression: a transactional LN must log the record's real abort
//! (before-image) LSN, not the cursor's previous position.
//!
//! JE `LN.logInternal` (LN.java:645) takes `abortLsn` from the
//! `WriteLockInfo`, which `CursorImpl.LockStanding.prepareForUpdate`
//! (CursorImpl.java:3343) seeds with the slot's LSN before the txn's first
//! write to the record, and keeps across later writes by the same txn.
//! Noxu logged `CursorImpl::current_lsn` instead — the LSN of whatever
//! record the cursor was last positioned on (NULL for a fresh cursor).
//! Recovery undo of an in-flight txn then reverted to the wrong version:
//! a committed record updated by the uncommitted txn was DELETED, and an
//! uncommitted insert/update could be "restored" and become visible.
//!
//! Each test checkpoints while the txn is in flight (so the BIN on disk
//! holds the uncommitted LSN and undo must act), forces the log to disk
//! with a later auto-commit, and recovers from a copy of the files taken
//! before the txn resolves (a crash image).
#![cfg(not(noxu_shuttle))]

use noxu_db::{
    CheckpointConfig, Database, DatabaseConfig, DatabaseEntry, Durability,
    Environment, EnvironmentConfig, Put, Transaction,
};
use std::path::Path;

fn env_cfg(p: &Path) -> EnvironmentConfig {
    EnvironmentConfig::new(p.to_path_buf())
        .with_transactional(true)
        .with_allow_create(true)
        .with_durability(Durability::COMMIT_SYNC)
}

fn db_cfg() -> DatabaseConfig {
    DatabaseConfig::new().with_allow_create(true).with_transactional(true)
}

fn entry(b: &[u8]) -> DatabaseEntry {
    DatabaseEntry::from_bytes(b)
}

/// Runs `in_flight` inside a txn that is still open when the crash image is
/// taken, then recovers the image and returns the values of `keys`.
fn crash_with_in_flight_txn(
    in_flight: impl FnOnce(&Database, &Transaction),
    keys: &[&[u8]],
) -> Vec<Option<Vec<u8>>> {
    let dir = tempfile::tempdir().unwrap();
    let img = tempfile::tempdir().unwrap();
    {
        let env = Environment::open(env_cfg(dir.path())).unwrap();
        let db = env.open_database(None, "d", &db_cfg()).unwrap();
        db.put(b"k0", b"committed").unwrap();
        let txn = env.begin_transaction(None).unwrap();
        in_flight(&db, &txn);
        env.checkpoint(Some(&CheckpointConfig::new().with_force(true)))
            .unwrap();
        // A later sync commit flushes the in-flight txn's LNs to the log.
        db.put(b"k9", b"later").unwrap();
        for f in std::fs::read_dir(dir.path()).unwrap() {
            let p = f.unwrap().path();
            if p.is_file() {
                std::fs::copy(&p, img.path().join(p.file_name().unwrap()))
                    .unwrap();
            }
        }
        txn.abort().unwrap();
        db.close().unwrap();
        env.close().unwrap();
    }
    let env = Environment::open(env_cfg(img.path())).unwrap();
    let db = env.open_database(None, "d", &db_cfg()).unwrap();
    let out =
        keys.iter().map(|k| db.get(k).unwrap().map(|b| b.to_vec())).collect();
    db.close().unwrap();
    env.close().unwrap();
    out
}

/// An uncommitted update of a committed record must not delete it.
#[test]
fn uncommitted_update_does_not_delete_committed_record() {
    let got = crash_with_in_flight_txn(
        |db, txn| db.put_in(txn, b"k0", b"UNCOMMITTED").unwrap(),
        &[b"k0", b"k9"],
    );
    assert_eq!(got[0].as_deref(), Some(&b"committed"[..]), "k0 lost");
    assert_eq!(got[1].as_deref(), Some(&b"later"[..]));
}

/// Two uncommitted inserts through one cursor: the second must not be
/// "restored" to the first's version and survive recovery.
#[test]
fn uncommitted_cursor_inserts_are_undone() {
    let got = crash_with_in_flight_txn(
        |db, txn| {
            let mut c = db.open_cursor_in(txn, None).unwrap();
            c.put(&entry(b"k1"), &entry(b"UNC1"), Put::Overwrite).unwrap();
            c.put(&entry(b"k2"), &entry(b"UNC2"), Put::Overwrite).unwrap();
            c.close().unwrap();
        },
        &[b"k0", b"k1", b"k2"],
    );
    assert_eq!(got[0].as_deref(), Some(&b"committed"[..]));
    assert_eq!(got[1], None, "uncommitted k1 visible");
    assert_eq!(got[2], None, "uncommitted k2 visible");
}

/// An insert then an update of a committed record through one cursor: the
/// update's undo must restore the committed value, not leave the
/// uncommitted one.
#[test]
fn uncommitted_cursor_update_after_insert_restores_committed_value() {
    let got = crash_with_in_flight_txn(
        |db, txn| {
            let mut c = db.open_cursor_in(txn, None).unwrap();
            c.put(&entry(b"k1"), &entry(b"UNC1"), Put::Overwrite).unwrap();
            c.put(&entry(b"k0"), &entry(b"UNCOMMITTED"), Put::Overwrite)
                .unwrap();
            c.close().unwrap();
        },
        &[b"k0", b"k1"],
    );
    assert_eq!(got[0].as_deref(), Some(&b"committed"[..]), "k0 wrong");
    assert_eq!(got[1], None, "uncommitted k1 visible");
}

/// Two updates of a committed record in one txn: the second LN must carry
/// the txn's ORIGINAL abort LSN (the committed version), not the first
/// uncommitted version.
#[test]
fn repeated_uncommitted_updates_restore_original_value() {
    let got = crash_with_in_flight_txn(
        |db, txn| {
            db.put_in(txn, b"k0", b"UNC-A").unwrap();
            db.put_in(txn, b"k0", b"UNC-B").unwrap();
        },
        &[b"k0"],
    );
    assert_eq!(got[0].as_deref(), Some(&b"committed"[..]), "k0 wrong");
}
