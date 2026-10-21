// Copyright (C) 2024-2026 Greg Burd.  Apache-2.0 OR MIT.
//! FID-TXN-FIX3A XA-commit witnesses (one-phase and two-phase).
//!
//! An XA branch commit must not release the committing txn's write locks
//! before the commit record is durable -- the same JE `Txn.commit` ordering
//! the core witnesses cover (see noxu-db `fid_txn_fix3a_witness.rs`).  Both XA
//! commit shapes route through `noxu_db::Transaction`: `xa_commit(ONEPHASE)`
//! calls `Transaction::commit()`, and `xa_commit` after `xa_prepare` calls
//! `Transaction::resolved_commit_after_prepare()`.  Both write and fsync the
//! TxnCommit via `write_txn_end()` BEFORE the inner `Txn` releases its write
//! locks, so they are already JE-ordered (the outer layer establishes
//! durability first).  These are permanent regression guards so a future
//! refactor cannot silently reopen the window on the XA path.
//!
//! Deterministic: the committer's fdatasync is held open by `fsync_probe`; a
//! concurrent read-committed reader races the window and must never observe the
//! SYNC-committed value while the committer's fdatasync has not completed.
#![cfg(all(unix, not(noxu_shuttle)))]
#![allow(clippy::unwrap_used)]

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use noxu_db::{
    Database, DatabaseConfig, Durability, Environment, EnvironmentConfig,
    LockMode, Mutex, ReadOptions,
};
use noxu_log::file_handle::fsync_probe;
use noxu_xa::{XaEnvironment, XaFlags, XaResource, Xid};
use tempfile::TempDir;

const V1: &[u8] = b"v1-durable-base-00000000";
const V2: &[u8] = b"v2-committed-not-durable";
const FSYNC_HOLD_US: u64 = 300_000;
const READER_LOCK_TIMEOUT_MS: u64 = 5_000;

/// Serialise the two XA witnesses: they drive the process-global `fsync_probe`.
static PROBE_SERIAL: Mutex<()> = Mutex::new(());

fn open(dir: &TempDir) -> (Arc<XaEnvironment>, Arc<Mutex<Database>>) {
    let cfg = EnvironmentConfig::new(dir.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(true)
        .with_durability(Durability::COMMIT_SYNC)
        .with_lock_timeout(READER_LOCK_TIMEOUT_MS);
    let env = Environment::open(cfg).unwrap();
    let db = env
        .open_database(
            None,
            "xa",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap();
    (Arc::new(XaEnvironment::new(env)), Arc::new(Mutex::new(db)))
}

/// Run one XA commit (via `commit`) and race a read-committed reader into the
/// held-fsync window.  `one_phase` selects ONEPHASE vs prepare+commit.
fn xa_witness(one_phase: bool) -> bool {
    let dir = TempDir::new().unwrap();
    let (xa, db) = open(&dir);

    // Base: k -> V1 durably (its own XA branch).
    {
        let seed = Xid::new(1, b"seed", b"b").unwrap();
        xa.xa_start(&seed, XaFlags::NOFLAGS).unwrap();
        {
            let t = xa.get_transaction(&seed).unwrap();
            db.lock().put_in(&t, b"k", V1).unwrap();
            xa.mark_write(&seed).unwrap();
        }
        xa.xa_end(&seed, XaFlags::TMSUCCESS).unwrap();
        xa.xa_commit(&seed, XaFlags::ONEPHASE).unwrap();
    }

    let saw_nondurable = Arc::new(AtomicBool::new(false));
    let started = Arc::new(AtomicBool::new(false));

    fsync_probe::arm(FSYNC_HOLD_US);

    let xa_c = Arc::clone(&xa);
    let db_c = Arc::clone(&db);
    let s = Arc::clone(&started);
    let committer = std::thread::spawn(move || {
        let xid = Xid::new(2, b"commit", b"b").unwrap();
        xa_c.xa_start(&xid, XaFlags::NOFLAGS).unwrap();
        {
            let t = xa_c.get_transaction(&xid).unwrap();
            db_c.lock().put_in(&t, b"k", V2).unwrap();
            xa_c.mark_write(&xid).unwrap();
        }
        xa_c.xa_end(&xid, XaFlags::TMSUCCESS).unwrap();
        s.store(true, Ordering::SeqCst);
        if one_phase {
            xa_c.xa_commit(&xid, XaFlags::ONEPHASE).unwrap();
        } else {
            xa_c.xa_prepare(&xid, XaFlags::NOFLAGS).unwrap();
            xa_c.xa_commit(&xid, XaFlags::NOFLAGS).unwrap();
        }
    });

    while !started.load(Ordering::SeqCst) {
        std::thread::yield_now();
    }
    std::thread::sleep(Duration::from_millis(40));

    let opts = ReadOptions::new().with_lock_mode(LockMode::ReadCommitted);
    let before = fsync_probe::COMPLETED.load(Ordering::SeqCst);
    let v = {
        let g = db.lock();
        g.get_with_options(None, b"k", &opts).ok().flatten()
    };
    let at = fsync_probe::COMPLETED.load(Ordering::SeqCst);
    if v.as_deref() == Some(V2) && before == 0 && at == 0 {
        saw_nondurable.store(true, Ordering::SeqCst);
    }

    committer.join().unwrap();
    fsync_probe::disarm();
    let observed = saw_nondurable.load(Ordering::SeqCst);
    eprintln!(
        "FIX3A XA {} commit: saw_nondurable_v2={observed}",
        if one_phase { "one-phase" } else { "two-phase" }
    );
    db.lock().close().unwrap();
    observed
}

#[test]
fn xa_one_phase_commit_no_nondurable_read() {
    let _serial = PROBE_SERIAL.lock();
    assert!(
        !xa_witness(true),
        "FID-TXN-FIX3A XA one-phase: a read-committed reader observed the \
         XA-committed V2 before its commit fsync completed."
    );
}

#[test]
fn xa_two_phase_commit_no_nondurable_read() {
    let _serial = PROBE_SERIAL.lock();
    assert!(
        !xa_witness(false),
        "FID-TXN-FIX3A XA two-phase: a read-committed reader observed the \
         XA-committed V2 before its commit fsync completed."
    );
}
