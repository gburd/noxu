// Copyright (C) 2024-2026 Greg Burd.  Apache-2.0 OR MIT.
//! FID-LOCKTIMEOUT-711 additional witnesses.
//!
//! Companion to `locktimeout_autocommit` / `locktimeout_explicit` (the
//! reviewer's two witnesses). These cover the other three angles the backport
//! contract calls for:
//!
//!  1. a per-txn override still WINS over the inherited env default;
//!  2. the env-level *txn* timeout is inherited by an auto-commit op (the
//!     38b6d129 "env txn timeout inherited by ALL lockers" angle);
//!  3. a non-txn / bare-cursor lock wait honours the configured env lock
//!     timeout (JE: every Locker starts with envImpl.getLockTimeout()).
//!
//! Timings are small: configured 1500 ms vs the hard-coded 500 ms default,
//! with generous tolerance (the discriminator is "did the waiter give up at
//! ~500 ms or did it honour the configured value"). All `with_durability`
//! COMMIT_NO_SYNC so a put does not fsync between lock acquisition and the
//! blocked waiter's measurement.

#![cfg(all(unix, not(noxu_shuttle)))]

use noxu_db::{
    Database, DatabaseConfig, Durability, Environment, EnvironmentConfig,
    NoxuError,
};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;

fn env_cfg(p: &std::path::Path, lock_ms: u64, txn_ms: u64) -> EnvironmentConfig {
    EnvironmentConfig::new(p.to_path_buf())
        .with_allow_create(true)
        .with_transactional(true)
        .with_durability(Durability::COMMIT_NO_SYNC)
        .with_lock_timeout(lock_ms)
        .with_txn_timeout(txn_ms)
}
fn db_cfg() -> DatabaseConfig {
    DatabaseConfig::new()
        .with_allow_create(true)
        .with_transactional(true)
}

/// (1) A per-txn `Transaction::set_lock_timeout` override (called AFTER
/// `begin_transaction`) reaches the inner `Txn`'s lock wait. JE:
/// `Transaction.setLockTimeout` -> `Locker.setLockTimeout` (Locker.java:210-221).
///
/// This exercises the 3edd93cc forwarding specifically: the outer
/// `Transaction::set_lock_timeout` must forward to the inner `Txn`. On the
/// base it did NOT, so the inner `Txn` kept its hard-coded 500 ms default and
/// the override was silently dropped.
///
/// A holder write-locks k for ~1500 ms (> the 500 ms base default). The waiter
/// is created with `begin_transaction(None)` then overridden to 4000 ms via the
/// outer handle. If the override reaches the inner `Txn` it blocks past the
/// 1500 ms hold and SUCCEEDS; if the override is dropped (base) the inner `Txn`
/// gives up at its 500 ms default and FAILS before the holder releases.
#[test]
fn per_txn_override_beats_env_default() {
    let dir = TempDir::new().unwrap();
    // env lock timeout 100 ms (deliberately SHORT): the override to 4000 ms
    // must win. env txn timeout 0.
    let env = Arc::new(Environment::open(env_cfg(dir.path(), 100, 0)).unwrap());
    let db: Arc<Database> =
        Arc::new(env.open_database(None, "t", &db_cfg()).unwrap());
    db.put(b"k", b"v0").unwrap();

    let holding = Arc::new(AtomicBool::new(false));
    let (eh, dh, h) = (Arc::clone(&env), Arc::clone(&db), Arc::clone(&holding));
    // Holder write-locks k and holds ~1500 ms (> both the 100 ms env value and
    // the 500 ms base Txn default), then commits so an honoured 4000 ms
    // override lets the waiter SUCCEED.
    let holder = std::thread::spawn(move || {
        let txn = eh.begin_transaction(None).unwrap();
        dh.put_in(&txn, b"k", b"held").unwrap();
        h.store(true, Ordering::SeqCst);
        std::thread::sleep(Duration::from_millis(1500));
        txn.commit().unwrap();
    });
    while !holding.load(Ordering::SeqCst) {
        std::thread::yield_now();
    }

    // Create the waiter, THEN override its lock timeout to 4000 ms via the
    // outer-handle API. The override must forward to the inner Txn so the
    // waiter blocks past the 1500 ms hold and succeeds.
    let txn2 = env.begin_transaction(None).unwrap();
    txn2.set_lock_timeout(4000);
    let start = Instant::now();
    let res = db.put_in(&txn2, b"k", b"override");
    let waited = start.elapsed();
    holder.join().unwrap();
    let _ = txn2.abort();

    eprintln!(
        "PER-TXN-OVERRIDE: env_lock=100ms override=4000ms held=1500ms waited={}ms result={:?}",
        waited.as_millis(),
        res.as_ref().map(|_| "Ok").map_err(|e| e.to_string()),
    );
    // Discriminator: an override that reaches the inner Txn lets it wait past
    // the 1500 ms hold (and succeed). A dropped override (base) gives up at the
    // inner Txn's 500 ms default (or the 100 ms env value) with an Err well
    // before 1500 ms.
    let dropped_override =
        res.is_err() && waited < Duration::from_millis(1200);
    assert!(
        !dropped_override,
        "per-txn override (4000ms) was dropped: the waiter gave up at ~{}ms \
         before the 1500ms hold — Transaction::set_lock_timeout did NOT reach \
         the inner Txn's lock wait",
        waited.as_millis()
    );
    db.close().unwrap();
    Arc::try_unwrap(env).ok().unwrap().close().unwrap();
}

/// (2) The env-level TXN timeout is inherited by an AUTO-COMMIT op (no explicit
/// txn). JE Locker ctor: txnTimeoutMillis = envImpl.getTxnTimeout()
/// (Locker.java:136) — every locker, including the auto-commit one. Port of the
/// v8 `je_txn_timeout_test::test_env_txn_timeout_applies_to_auto_commit`.
///
/// Env txn timeout 300 ms, lock timeout 0 (wait forever). An auto-commit write
/// blocked on a write-locked record must surface a TransactionTimeout at 300 ms
/// instead of hanging forever.
#[test]
fn env_txn_timeout_inherited_by_auto_commit() {
    let dir = TempDir::new().unwrap();
    // lock timeout 0 == wait forever; txn timeout 300 ms must still fire.
    let env = Arc::new(Environment::open(env_cfg(dir.path(), 0, 300)).unwrap());
    let db: Arc<Database> =
        Arc::new(env.open_database(None, "t", &db_cfg()).unwrap());
    db.put(b"k", b"v0").unwrap();

    let holding = Arc::new(AtomicBool::new(false));
    let release = Arc::new(AtomicBool::new(false));
    let (eh, dh, h, r) = (
        Arc::clone(&env),
        Arc::clone(&db),
        Arc::clone(&holding),
        Arc::clone(&release),
    );
    let holder = std::thread::spawn(move || {
        let txn = eh.begin_transaction(None).unwrap();
        dh.put_in(&txn, b"k", b"held").unwrap();
        h.store(true, Ordering::SeqCst);
        while !r.load(Ordering::SeqCst) {
            std::thread::sleep(Duration::from_millis(10));
        }
        txn.abort().ok();
    });
    while !holding.load(Ordering::SeqCst) {
        std::thread::yield_now();
    }

    // Auto-commit write (no explicit txn, so lock timeout 0 = forever) must
    // still time out via the inherited env txn timeout (300 ms).
    let start = Instant::now();
    let res = db.put(b"k", b"auto");
    let waited = start.elapsed();
    release.store(true, Ordering::SeqCst);
    holder.join().unwrap();

    eprintln!(
        "ENV-TXN-TIMEOUT-AUTO: txn_timeout=300ms lock_timeout=0 waited={}ms result={:?}",
        waited.as_millis(),
        res.as_ref().map(|_| "Ok").map_err(|e| e.to_string()),
    );
    assert!(
        res.is_err(),
        "auto-commit put must time out via the inherited env txn timeout, got {res:?}"
    );
    assert!(
        matches!(
            res.unwrap_err(),
            NoxuError::TransactionTimeout { timeout_ms, .. } if timeout_ms == 300
        ),
        "auto-commit op must inherit env TransactionTimeout(300); env lock \
         timeout was 0 (forever), so if the env txn timeout was NOT inherited \
         the op would hang"
    );
    // It fired via the 300ms txn timeout, not an accidental 500ms lock default.
    assert!(
        waited < Duration::from_millis(1500),
        "must fire at ~300ms (txn timeout), waited {}ms",
        waited.as_millis()
    );
    db.close().unwrap();
    Arc::try_unwrap(env).ok().unwrap().close().unwrap();
}

/// (3) A non-txn / bare-cursor lock wait honours the configured env lock
/// timeout. JE: a BasicLocker/ThreadLocker (non-txn) starts with
/// envImpl.getLockTimeout() (Locker.java:130/157-159), same as a Txn. In Noxu
/// a bare cursor acquires via `LockManager::lock(..)`, which already uses the
/// LockManager's env-configured default lock timeout.
///
/// Regression guard: unlike the auto-commit *write* path (an auto-commit `Txn`,
/// which WAS broken — see `env_txn_timeout_inherited_by_auto_commit` and the
/// autocommit witness), this bare-cursor *read* path already honoured the env
/// lock timeout on the 7.11.x base (it never read the hard-coded `Txn` default).
/// This test PASSES on the base too; it is kept to prove the fix does NOT
/// regress the non-txn locker path (the task asks us to check it).
///
/// Configure env lock timeout 1500 ms (> the 500 ms default). A holder txn
/// write-locks k for ~900 ms. A bare (non-txn) read cursor positioning on k
/// must BLOCK past the 500 ms default (honouring the 1500 ms configured value)
/// and then SUCCEED once the holder releases. If the configured value did not
/// reach the non-txn locker it would give up at ~500 ms with an Err.
#[test]
fn nontxn_cursor_honours_env_lock_timeout() {
    let dir = TempDir::new().unwrap();
    // env lock timeout 1500 ms; env txn timeout 0.
    let env = Arc::new(Environment::open(env_cfg(dir.path(), 1500, 0)).unwrap());
    let db: Arc<Database> =
        Arc::new(env.open_database(None, "t", &db_cfg()).unwrap());
    db.put(b"k", b"v0").unwrap();

    let holding = Arc::new(AtomicBool::new(false));
    let (eh, dh, h) = (Arc::clone(&env), Arc::clone(&db), Arc::clone(&holding));
    // Holder: explicit txn write-locks k, holds ~900 ms (> 500 default,
    // < 1500 configured), then commits so the waiter can SUCCEED.
    let holder = std::thread::spawn(move || {
        let txn = eh.begin_transaction(None).unwrap();
        dh.put_in(&txn, b"k", b"held").unwrap();
        h.store(true, Ordering::SeqCst);
        std::thread::sleep(Duration::from_millis(900));
        txn.commit().unwrap();
    });
    while !holding.load(Ordering::SeqCst) {
        std::thread::yield_now();
    }

    // Bare (non-txn) read cursor: position on k. Must block past 500 ms and
    // then read the committed value, not time out at the 500 ms default.
    let start = Instant::now();
    let mut cur = db.open_cursor(None).unwrap();
    let res = cur.seek(b"k");
    let waited = start.elapsed();
    holder.join().unwrap();

    eprintln!(
        "NONTXN-CURSOR: env_lock=1500ms held=900ms waited={}ms result={:?}",
        waited.as_millis(),
        res.as_ref().map(|v| v.is_some()).map_err(|e| e.to_string()),
    );
    // Discriminator: a non-txn locker stuck at the 500 ms default would Err
    // at ~500 ms (before the holder releases at ~900 ms). Honouring the
    // configured 1500 ms, it blocks until the holder commits (~900 ms) and
    // then succeeds.
    let timed_out_at_default = res.is_err() && waited < Duration::from_millis(800);
    assert!(
        !timed_out_at_default,
        "the non-txn cursor timed out at ~{}ms (the 500ms default) despite \
         with_lock_timeout(1500) — the configured lock timeout did not reach \
         the non-txn locker",
        waited.as_millis()
    );
    drop(cur);
    db.close().unwrap();
    Arc::try_unwrap(env).ok().unwrap().close().unwrap();
}
