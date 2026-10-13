//! Crash-recovery undo of an in-flight or aborted transaction must restore
//! the committed before-image of every record the txn wrote.
//!
//! Provenance (imported, reviewer probes; originals and SHA256 in
//! /work/artifacts/freebsd-delete-undo/provenance-*):
//! * freebsd-cursor-sync-review `review_abort_lsn_probe_test.rs`: an
//!   uncommitted DELETE of a committed record (alone, after updates, before a
//!   reinsert, via db API and cursor), checkpoint while in flight, crash
//!   image.  Noxu physically removes the slot on delete, so the checkpointed
//!   BIN has no slot carrying the DeleteLN's LSN and recovery undo skipped
//!   it (JE keeps the slot PD with the deleted LN's LSN: IN.deleteRecord,
//!   IN.java:3765-3798; undo then matches `logLsn == slotLsn`,
//!   RecoveryManager.java:1912-1963).
//! * freebsd-writer-oldlsn-review `review_probe_test.rs` case (c): three
//!   updates in a txn, checkpoint (BIN holds the 3rd version), ABORT, crash
//!   image.  Recovery skipped the undo pass because the txn had an abort
//!   record, so the checkpointed uncommitted version survived.  JE undoLNs
//!   undoes every LN whose txn is neither committed nor prepared, aborted
//!   txns included (RecoveryManager.java:1596-1619).
//!
//! Crash image = copy of the environment files (minus `noxu.lck`) taken
//! while the env is open, after a later sync auto-commit forced the log.
#![cfg(not(noxu_shuttle))]

use noxu_db::{
    CheckpointConfig, Database, DatabaseConfig, DatabaseEntry, Durability,
    Environment, EnvironmentConfig, Get, Put, Transaction,
};
use std::path::Path;

fn env_cfg(p: &Path) -> EnvironmentConfig {
    let mut c = EnvironmentConfig::new(p.to_path_buf())
        .with_transactional(true)
        .with_allow_create(true)
        .with_durability(Durability::COMMIT_SYNC);
    c.set_run_cleaner(false);
    c.set_run_checkpointer(false);
    c.set_run_in_compressor(false);
    c
}

fn db_cfg() -> DatabaseConfig {
    DatabaseConfig::new().with_allow_create(true).with_transactional(true)
}

fn e(b: &[u8]) -> DatabaseEntry {
    DatabaseEntry::from_bytes(b)
}

fn image(dir: &Path) -> tempfile::TempDir {
    let img = tempfile::tempdir().unwrap();
    for f in std::fs::read_dir(dir).unwrap() {
        let f = f.unwrap();
        if f.file_type().unwrap().is_file() && f.file_name() != "noxu.lck" {
            std::fs::copy(f.path(), img.path().join(f.file_name())).unwrap();
        }
    }
    img
}

/// Commits `k0`, runs `f` in a txn, forces a checkpoint while the txn is in
/// flight, optionally aborts, then takes a crash image and recovers it.
/// Returns (in-memory value after abort, recovered value) of `k0`.
fn run(
    abort_before_image: bool,
    f: impl FnOnce(&Database, &Transaction),
) -> (Option<Vec<u8>>, Option<Vec<u8>>) {
    let dir = tempfile::tempdir().unwrap();
    let img;
    let live;
    {
        let env = Environment::open(env_cfg(dir.path())).unwrap();
        let db = env.open_database(None, "d", &db_cfg()).unwrap();
        db.put(b"k0", b"committed").unwrap();
        let txn = env.begin_transaction(None).unwrap();
        f(&db, &txn);
        env.checkpoint(Some(&CheckpointConfig::new().with_force(true)))
            .unwrap();
        if abort_before_image {
            txn.abort().unwrap();
        }
        db.put(b"k9", b"later").unwrap();
        img = image(dir.path());
        if !abort_before_image {
            txn.abort().unwrap();
        }
        live = db.get(b"k0").unwrap().map(|b| b.to_vec());
        db.close().unwrap();
        env.close().unwrap();
    }
    let env = Environment::open(env_cfg(img.path())).unwrap();
    let db = env.open_database(None, "d", &db_cfg()).unwrap();
    let got = db.get(b"k0").unwrap().map(|b| b.to_vec());
    assert_eq!(db.get(b"k9").unwrap().as_deref(), Some(&b"later"[..]));
    db.close().unwrap();
    env.close().unwrap();
    (live, got)
}

fn assert_restored(
    case: &str,
    (live, got): (Option<Vec<u8>>, Option<Vec<u8>>),
) {
    assert_eq!(
        live.as_deref(),
        Some(&b"committed"[..]),
        "{case}: in-memory abort control"
    );
    assert_eq!(
        got.as_deref().map(String::from_utf8_lossy),
        Some("committed".into()),
        "{case}: committed k0 not restored by recovery"
    );
}

fn u2_del_ins_db(db: &Database, t: &Transaction) {
    db.put_in(t, b"k0", b"U1").unwrap();
    db.put_in(t, b"k0", b"U2").unwrap();
    assert!(db.delete_in(t, b"k0").unwrap());
    db.put_in(t, b"k0", b"REINS").unwrap();
}

fn u2_del_ins_cursor(db: &Database, t: &Transaction) {
    let mut c = db.open_cursor_in(t, None).unwrap();
    c.put(&e(b"k0"), &e(b"U1"), Put::Overwrite).unwrap();
    c.put(&e(b"k0"), &e(b"U2"), Put::Current).unwrap();
    c.delete().unwrap();
    c.put(&e(b"k0"), &e(b"REINS"), Put::Overwrite).unwrap();
    c.close().unwrap();
}

fn del_only(db: &Database, t: &Transaction) {
    assert!(db.delete_in(t, b"k0").unwrap());
}

fn del_reins(db: &Database, t: &Transaction) {
    assert!(db.delete_in(t, b"k0").unwrap());
    db.put_in(t, b"k0", b"REINS").unwrap();
}

fn u2_del(db: &Database, t: &Transaction) {
    db.put_in(t, b"k0", b"U1").unwrap();
    db.put_in(t, b"k0", b"U2").unwrap();
    assert!(db.delete_in(t, b"k0").unwrap());
}

fn u3(db: &Database, t: &Transaction) {
    for v in [&b"U1"[..], b"U2", b"U3"] {
        db.put_in(t, b"k0", v).unwrap();
    }
}

fn cursor_search_delete(db: &Database, t: &Transaction) {
    let mut c = db.open_cursor_in(t, None).unwrap();
    let (mut k, mut d) = (e(b"k0"), DatabaseEntry::new());
    c.get(&mut k, &mut d, Get::Search, None).unwrap();
    c.delete().unwrap();
    c.close().unwrap();
}

// ---- txn still open at the crash (no abort record) ----

#[test]
fn in_flight_update2_delete_reinsert_db_api() {
    assert_restored("u2-del-ins/db", run(false, u2_del_ins_db));
}

#[test]
fn in_flight_update2_delete_reinsert_cursor() {
    assert_restored("u2-del-ins/cursor", run(false, u2_del_ins_cursor));
}

#[test]
fn in_flight_delete_only() {
    assert_restored("del-only", run(false, del_only));
}

#[test]
fn in_flight_delete_reinsert() {
    assert_restored("del-reins", run(false, del_reins));
}

#[test]
fn in_flight_update2_delete() {
    assert_restored("u2-del", run(false, u2_del));
}

#[test]
fn in_flight_cursor_search_delete() {
    assert_restored("cursor-search-delete", run(false, cursor_search_delete));
}

/// Control (fixed by 44f832e8): updates only, txn open at crash.
#[test]
fn in_flight_update3() {
    assert_restored("u3/open", run(false, u3));
}

// ---- txn aborted after the checkpoint, before the crash ----

/// Writer-oldlsn review probe (c): the checkpointed BIN holds the 3rd
/// uncommitted version; the abort record must not suppress undo.
#[test]
fn aborted_after_ckpt_update3() {
    assert_restored("u3/aborted", run(true, u3));
}

/// Same as probe (c) with the reviewer's 512-byte and 2-byte values.
#[test]
fn aborted_after_ckpt_update3_large_and_small() {
    let dir = tempfile::tempdir().unwrap();
    let env = Environment::open(env_cfg(dir.path())).unwrap();
    let db = env.open_database(None, "p", &db_cfg()).unwrap();
    let d0 = vec![0x30u8; 512];
    db.put(b"k", &d0).unwrap();
    db.put(b"small", b"s0").unwrap();
    let txn = env.begin_transaction(None).unwrap();
    for b in [0x31u8, 0x32, 0x33] {
        db.put_in(&txn, b"k", vec![b; 512]).unwrap();
        db.put_in(&txn, b"small", vec![b; 2]).unwrap();
    }
    env.checkpoint(Some(&CheckpointConfig::new().with_force(true))).unwrap();
    txn.abort().unwrap();
    assert_eq!(db.get(b"k").unwrap().unwrap().as_ref(), d0.as_slice());
    db.put(b"z", b"z").unwrap();
    let img = image(dir.path());
    let renv = Environment::open(env_cfg(img.path())).unwrap();
    let rdb = renv.open_database(None, "p", &db_cfg()).unwrap();
    let k = rdb.get(b"k").unwrap().map(|b| b.to_vec());
    let s = rdb.get(b"small").unwrap().map(|b| b.to_vec());
    assert_eq!(
        k.as_ref().map(|v| (v.len(), v[0])),
        Some((512, 0x30)),
        "large key must recover to original D0"
    );
    assert_eq!(s.as_deref(), Some(&b"s0"[..]), "small key must recover");
}

/// Delete then abort after the checkpoint: both root causes at once.
#[test]
fn aborted_after_ckpt_delete_only() {
    assert_restored("del-only/aborted", run(true, del_only));
}

#[test]
fn aborted_after_ckpt_update2_delete_reinsert() {
    assert_restored("u2-del-ins/aborted", run(true, u2_del_ins_db));
}
