//! F13 — evictor / checkpoint provisional-flag coordination (recovery-race).
//!
//! JE `Evictor.coordinateEvictionWithCheckpoint` →
//! `Checkpointer.coordinateEvictionWithCheckpoint` →
//! `DirtyINMap.coordinateEvictionWithCheckpoint` (DirtyINMap.java:103-164):
//! when the evictor logs a dirty node that is BELOW the in-progress
//! checkpoint's `maxFlushLevel`, it logs it `Provisional.YES` (recovery treats
//! it as provisional until the checkpoint's own non-provisional ancestor makes
//! it durable); otherwise `Provisional.NO`.
//!
//! The provisional-flag DECISION is unit-tested in
//! `noxu_recovery::checkpointer::tests::test_cc4_*`; the post-construction
//! WIRING (the F13 fix: the checkpointer is built after the evictor, so it is
//! installed via `Evictor::set_checkpointer` once the evictor is inside an
//! `Arc`) is unit-tested in `noxu_evictor::evictor::tests::
//! test_f13_set_checkpointer_wires_after_arc`.
//!
//! This end-to-end test proves the property the coordination protects: a
//! workload that races eviction (tiny cache, active evictor) against periodic
//! checkpoints, then clean-closes and reopens, recovers exactly the committed
//! data — no provisional BIN is left stranded without its covering ancestor.

use noxu_db::{
    DatabaseConfig, DatabaseEntry, EnvironmentConfig, Get, OperationStatus,
};
use std::collections::BTreeMap;
use tempfile::TempDir;

const NUM_RECS: u32 = 400;

fn ikey(i: u32) -> DatabaseEntry {
    DatabaseEntry::from_bytes(&i.to_be_bytes())
}
fn ival(i: u32) -> DatabaseEntry {
    // Larger values grow the tree faster (more BINs, more eviction pressure).
    let mut v = Vec::with_capacity(64);
    v.extend_from_slice(&i.to_be_bytes());
    v.resize(64, (i & 0xff) as u8);
    DatabaseEntry::from_bytes(&v)
}

fn open_db(env: &noxu_db::Environment, name: &str) -> noxu_db::Database {
    let cfg =
        DatabaseConfig::new().with_allow_create(true).with_transactional(true);
    env.open_database(None, name, &cfg).unwrap()
}

fn collect_all(db: &noxu_db::Database) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let mut out = BTreeMap::new();
    let mut c = db.open_cursor(None).unwrap();
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    let mut s = c.get(&mut k, &mut d, Get::First, None).unwrap();
    while s == OperationStatus::Success {
        out.insert(
            k.data_opt().unwrap_or(&[]).to_vec(),
            d.data_opt().unwrap_or(&[]).to_vec(),
        );
        s = c.get(&mut k, &mut d, Get::Next, None).unwrap();
    }
    out
}

/// Eviction racing frequent checkpoints must still recover every committed
/// record.  The evictor is wired to the checkpointer (F13), so a dirty BIN
/// evicted below the checkpoint's max flush level is logged provisionally and
/// is subsumed by the checkpoint's non-provisional ancestor; nothing is lost.
#[test]
fn f13_eviction_racing_checkpoints_recovers_all() {
    let dir = TempDir::new().unwrap();
    {
        let cfg = EnvironmentConfig::new(dir.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true)
            // Tiny cache + active evictor => heavy eviction of dirty BINs.
            .with_cache_size(96 * 1024)
            .with_run_evictor(true)
            // Frequent checkpoints => eviction races an in-progress checkpoint.
            .with_run_checkpointer(true)
            .with_checkpointer_bytes_interval(8 * 1024)
            .with_run_cleaner(false);
        let env = noxu_db::Environment::open(cfg).unwrap();
        let db = open_db(&env, "f13");
        for i in 0..NUM_RECS {
            db.put(ikey(i), ival(i)).unwrap();
        }
        // The periodic checkpointer (8 KiB bytes-interval) fires repeatedly
        // during this write burst, so eviction of dirty BINs races an
        // in-progress checkpoint without any manual checkpoint call.
        db.close().unwrap();
        env.close().unwrap();
        // Match the recovery-test convention: drop the handles so the on-disk
        // FileManager lock is released before reopen (close() alone leaves the
        // lock held until the Environment handle drops).
        drop(db);
        drop(env);
    }

    // Reopen (runs recovery) and verify every committed record round-trips.
    let cfg = EnvironmentConfig::new(dir.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    let env = noxu_db::Environment::open(cfg).unwrap();
    let db = open_db(&env, "f13");
    let got = collect_all(&db);
    assert_eq!(
        got.len() as u32,
        NUM_RECS,
        "F13: every record must survive eviction-vs-checkpoint racing + reopen"
    );
    for i in 0..NUM_RECS {
        let k = ikey(i).data_opt().unwrap().to_vec();
        let v = ival(i).data_opt().unwrap().to_vec();
        assert_eq!(
            got.get(&k),
            Some(&v),
            "F13: record {i} missing/wrong after recovery"
        );
    }
    db.close().unwrap();
    env.close().unwrap();
}
