// Copyright (C) 2024-2026 Greg Burd.  Apache-2.0 OR MIT.
//! FID-LOCKTIMEOUT-AUTOCOMMIT verification witness.
//!
//! The author's cost-probe caveat claims `EnvironmentConfig::with_lock_timeout`
//! does NOT reach the auto-commit (non-txn / Database::put) locker, so an
//! auto-commit writer that blocks on a contended record lock always times out
//! at the hardcoded 500ms default regardless of the configured timeout.  JE's
//! auto-commit uses the env lock timeout (LockerFactory / Locker.lockTimeout).
//!
//! This test DISCRIMINATES: configure the env lock timeout to 3000ms, hold a
//! record lock under one auto-commit op, and time a second auto-commit op that
//! blocks on it.  If the blocked op returns ~500ms, the config did NOT reach
//! the auto-commit locker (gap CONFIRMED).  If it returns ~3000ms (times out
//! at the configured value) or succeeds after the holder releases, the config
//! DID reach it (no gap).

use noxu_db::{
    Database, DatabaseConfig, Environment, EnvironmentConfig, Durability,
};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;

const CONFIGURED_TIMEOUT_MS: u64 = 3000;

fn env_cfg(p: &std::path::Path) -> EnvironmentConfig {
    EnvironmentConfig::new(p.to_path_buf())
        .with_allow_create(true)
        .with_transactional(true)
        .with_durability(Durability::COMMIT_NO_SYNC)
        .with_lock_timeout(CONFIGURED_TIMEOUT_MS)
}
fn db_cfg() -> DatabaseConfig {
    DatabaseConfig::new()
        .with_allow_create(true)
        .with_transactional(true)
}

#[test]
fn locktimeout_autocommit_reaches_configured_value() {
    let dir = TempDir::new().unwrap();
    let env = Arc::new(Environment::open(env_cfg(dir.path())).unwrap());
    let db: Arc<Database> =
        Arc::new(env.open_database(None, "t", &db_cfg()).unwrap());
    db.put(b"k", b"v0").unwrap();

    // Holder: an EXPLICIT txn that locks k and holds it for 1500ms — longer
    // than the 500ms default, shorter than the 3000ms configured value.  If
    // the auto-commit waiter honours the configured 3000ms, it will BLOCK
    // past 1500ms and then SUCCEED when the holder commits.  If it only
    // honours the 500ms default, it will FAIL at ~500ms.
    let holding = Arc::new(AtomicBool::new(false));
    let release = Arc::new(AtomicBool::new(false));
    let env_h = Arc::clone(&env);
    let db_h = Arc::clone(&db);
    let h = Arc::clone(&holding);
    let r = Arc::clone(&release);
    let holder = std::thread::spawn(move || {
        let txn = env_h.begin_transaction(None).unwrap();
        db_h.put_in(&txn, b"k", b"held").unwrap(); // write-locks k
        h.store(true, Ordering::SeqCst);
        while !r.load(Ordering::SeqCst) {
            std::thread::sleep(Duration::from_millis(10));
        }
        txn.commit().unwrap();
    });

    while !holding.load(Ordering::SeqCst) {
        std::thread::yield_now();
    }

    // Auto-commit writer blocks on k.  Time it.
    let start = Instant::now();
    let res = db.put(b"k", b"auto"); // auto-commit (no txn) -> auto-commit locker
    let waited = start.elapsed();

    // Release the holder regardless.
    release.store(true, Ordering::SeqCst);
    holder.join().unwrap();

    eprintln!(
        "LOCKTIMEOUT-AUTOCOMMIT: configured={}ms waited={}ms result={:?}",
        CONFIGURED_TIMEOUT_MS,
        waited.as_millis(),
        res.as_ref().map(|_| "Ok").map_err(|e| e.to_string()),
    );

    // Discriminator: the holder releases at ~from when we set `release` which
    // is immediately after the put returns.  So the put either:
    //  (a) timed out at ~500ms (gap: did not honour 3000ms), OR
    //  (b) blocked past 1500ms honouring the configured timeout.
    // We assert the auto-commit waiter honoured MORE than the 500ms default —
    // i.e. the configured timeout reached it.  A wait < 800ms with an Err is
    // the gap signature.
    let timed_out_at_default =
        res.is_err() && waited < Duration::from_millis(800);
    assert!(
        !timed_out_at_default,
        "FID-LOCKTIMEOUT-AUTOCOMMIT: the auto-commit locker timed out at \
         ~{}ms (the 500ms default) despite EnvironmentConfig::with_lock_timeout({}) \
         — the configured lock timeout did NOT reach the auto-commit locker. \
         JE's auto-commit uses the env lock timeout.",
        waited.as_millis(),
        CONFIGURED_TIMEOUT_MS
    );

    db.close().unwrap();
    Arc::try_unwrap(env).ok().unwrap().close().unwrap();
}
