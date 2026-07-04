//! F12 — daemon shutdown ORDER + final-flush-not-dropped on clean close.
//!
//! JE `EnvironmentImpl.close()` (EnvironmentImpl.java:1873 `requestShutdownDaemons`
//! → final `invokeCheckpoint(FORCE)` → :1915 `shutdownDaemons`) shuts daemons
//! down in the order inCompressor → cleaner → checkpointer → evictor, joining
//! the **evictor LAST** so that dirty nodes flushed during the final checkpoint
//! are still evictable/flushable (EnvironmentImpl.java:2352-2354 comment:
//! "The evictors have to be shutdown last because the other daemons might
//! create changes to the memory usage which result in a notify to eviction").
//!
//! Noxu previously joined the evictor FIRST (evictor → checkpointer →
//! inCompressor → cleaner) and ran the final checkpoint AFTER every daemon was
//! already dead — risking dropped final dirty-BIN flushes.  This test proves
//! the F12 property: committed data present at close survives a clean
//! close/reopen even with the periodic checkpointer and evictor daemons
//! DISABLED, so the ONLY thing that can flush the dirty BINs is the final
//! forced checkpoint that `close()` runs while the evictor is still alive.

use noxu_db::{
    DatabaseConfig, DatabaseEntry, EnvironmentConfig, Get, OperationStatus,
};
use std::collections::BTreeMap;
use std::path::Path;
use tempfile::TempDir;

const NUM_RECS: u32 = 200;

/// Open an env with the periodic checkpointer + evictor daemons DISABLED.
///
/// With both off, nothing flushes dirty BINs during runtime; the only flush
/// path is the final forced checkpoint inside `close()`.  If `close()` joined
/// the evictor before that checkpoint (the F12 bug), the final flush of a
/// dirty BIN that needs eviction coordination would be dropped.
fn open_env_no_periodic_flush(dir: &Path) -> noxu_db::Environment {
    let cfg = EnvironmentConfig::new(dir.to_path_buf())
        .with_allow_create(true)
        .with_transactional(true)
        .with_run_checkpointer(false)
        .with_run_evictor(false)
        .with_run_cleaner(false);
    noxu_db::Environment::open(cfg).unwrap()
}

fn open_env_default(dir: &Path) -> noxu_db::Environment {
    let cfg = EnvironmentConfig::new(dir.to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    noxu_db::Environment::open(cfg).unwrap()
}

fn open_db(env: &noxu_db::Environment, name: &str) -> noxu_db::Database {
    let cfg =
        DatabaseConfig::new().with_allow_create(true).with_transactional(true);
    env.open_database(None, name, &cfg).unwrap()
}

fn ikey(i: u32) -> DatabaseEntry {
    DatabaseEntry::from_bytes(&i.to_be_bytes())
}

fn ival(i: u32) -> DatabaseEntry {
    DatabaseEntry::from_bytes(&(i.wrapping_mul(0x9E3779B1)).to_be_bytes())
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

/// F12 core property: a dirty BIN present at clean close is flushed by the
/// final checkpoint (evictor joined last) and is fully present after reopen.
#[test]
fn f12_dirty_bin_flushed_on_clean_close_with_daemons_off() {
    let dir = TempDir::new().unwrap();

    // Phase 1: write committed data with the periodic checkpointer + evictor
    // OFF, then clean-close.  The final forced checkpoint in close() is the
    // only thing that can persist the dirty BINs.
    {
        let env = open_env_no_periodic_flush(dir.path());
        let db = open_db(&env, "f12");
        for i in 0..NUM_RECS {
            db.put(ikey(i), ival(i)).unwrap();
        }
        db.close().unwrap();
        // Clean close: runs the final forced checkpoint with the evictor still
        // alive (F12 ordering), flushing every dirty BIN.
        env.close().unwrap();
    }

    // Phase 2: reopen and verify every committed record round-trips.
    {
        let env = open_env_default(dir.path());
        let db = open_db(&env, "f12");
        let got = collect_all(&db);
        assert_eq!(
            got.len() as u32,
            NUM_RECS,
            "F12: all {} records must survive a clean close (dirty BINs flushed \
             by the final checkpoint that runs before the evictor is joined)",
            NUM_RECS
        );
        for i in 0..NUM_RECS {
            let k = ikey(i).data_opt().unwrap().to_vec();
            let v = ival(i).data_opt().unwrap().to_vec();
            assert_eq!(
                got.get(&k),
                Some(&v),
                "F12: record {i} missing/wrong after clean-close reopen"
            );
        }
        db.close().unwrap();
        env.close().unwrap();
    }
}

/// F12 under eviction pressure: with the evictor ACTIVE and a tiny cache, dirty
/// BINs are being pushed through the evictor's flush path right up to close.
/// The reorder joins the evictor LAST (after the final checkpoint), so no
/// in-flight eviction flush is dropped.  All committed data must survive.
#[test]
fn f12_survives_close_under_eviction_pressure() {
    let dir = TempDir::new().unwrap();
    {
        let cfg = EnvironmentConfig::new(dir.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true)
            // Tiny cache + active evictor: eviction runs concurrently with the
            // writes, so dirty BINs are flushed via the evictor path.  The
            // periodic checkpointer stays OFF so the final close() checkpoint
            // is the durability boundary.
            .with_cache_size(64 * 1024)
            .with_run_evictor(true)
            .with_run_checkpointer(false)
            .with_run_cleaner(false);
        let env = noxu_db::Environment::open(cfg).unwrap();
        let db = open_db(&env, "f12ep");
        for i in 0..NUM_RECS {
            db.put(ikey(i), ival(i)).unwrap();
        }
        db.close().unwrap();
        env.close().unwrap();
    }

    let env = open_env_default(dir.path());
    let db = open_db(&env, "f12ep");
    let got = collect_all(&db);
    assert_eq!(
        got.len() as u32,
        NUM_RECS,
        "F12: all records must survive close under eviction pressure"
    );
    for i in 0..NUM_RECS {
        let k = ikey(i).data_opt().unwrap().to_vec();
        let v = ival(i).data_opt().unwrap().to_vec();
        assert_eq!(
            got.get(&k),
            Some(&v),
            "F12: record {i} lost under pressure"
        );
    }
    db.close().unwrap();
    env.close().unwrap();
}
