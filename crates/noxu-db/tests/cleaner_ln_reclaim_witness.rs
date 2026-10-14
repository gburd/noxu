//! Witness for BUG-CLEANER-LN-FILE-RECLAIM.
//!
//! A log file must not be reclaimed by the cleaner while a resident BIN slot
//! still references an LN LSN into it.  Under extreme cache pressure
//! (working set >> cache) with concurrent readers + an active cleaner, a
//! point read's LN fetch at a slot's LSN must NEVER hit `LogFileNotFound`
//! (which invalidates the environment).
//!
//! Fail-before on 1bafe2e9: the cleaner deletes `0000000N.ndb` while a
//! resident BIN still points an LN LSN into it; a concurrent `get` fetches
//! that LSN and fails `LogFileNotFound` -> env invalidated -> every
//! subsequent op errors.
//!
//! Teeth: a committed, readable key must remain readable under concurrent
//! cleaning.  Any `Err` from `get` on a committed key, or `!env.is_valid()`,
//! is the bug.
//!
//! Config mirrors `read-race-triage.md`: 1 MiB cache, 40_000 x 400 B records
//! (~16.8x over cache), many reader threads, cleaner + evictor +
//! checkpointer ON, transactional env.  Bounded (fixed op budget, no wall
//! clock) so it is deterministic-ish and runs in CI.

use noxu_db::{
    Database, DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig,
};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use tempfile::TempDir;

fn open_pressure_env(
    dir: &std::path::Path,
    cache_bytes: u64,
) -> (Environment, Database) {
    let mut cfg = EnvironmentConfig::new(dir.to_path_buf());
    cfg.set_allow_create(true);
    cfg.set_transactional(true);
    cfg.set_cache_percent(0); // so set_cache_size takes effect
    cfg.set_cache_size(cache_bytes);
    // All reclamation daemons ON — this is the cleaner<->LN-fetch window.
    cfg.set_run_cleaner(true);
    cfg.set_run_evictor(true);
    cfg.set_run_checkpointer(true);
    cfg.set_run_in_compressor(true);
    // Small log files so many files exist and the cleaner churns constantly,
    // maximising file-reclaim opportunities (triage used the engine default
    // but a smaller file size makes the race fire within a bounded op budget
    // rather than needing an 8 s wall-clock window).
    cfg.set_log_file_max_bytes(256 * 1024);
    // Keep utilization threshold high so the cleaner is eager to reclaim.
    cfg.set_cleaner_min_utilization(90);
    let env = Environment::open(cfg).expect("open env");
    let db = env
        .open_database(
            None,
            "witness",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .expect("open db");
    (env, db)
}

fn fill_batched(env: &Environment, db: &Database, n: usize, val: &[u8]) {
    let mut i = 0usize;
    while i < n {
        let end = (i + 1000).min(n);
        let txn = env.begin_transaction(None).unwrap();
        for j in i..end {
            let k = DatabaseEntry::from_vec(format!("{j:010}").into_bytes());
            db.put_in(&txn, &k, DatabaseEntry::from_bytes(val)).unwrap();
        }
        txn.commit().unwrap();
        i = end;
    }
}

#[test]
fn committed_key_stays_readable_under_concurrent_cleaning() {
    // Allow override via env for the N-times loop; defaults match the triage.
    let cache = std::env::var("RT_CACHE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1_048_576u64);
    let records = std::env::var("RT_RECORDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(40_000usize);
    let value_len = std::env::var("RT_VALUE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(400usize);
    let threads = std::env::var("RT_THREADS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(32usize);
    // Bounded by op budget per thread (safety cap) AND a wall-clock window
    // (the triage used an 8 s window). The window keeps a non-reproduction
    // from grinding the full op budget; the op cap bounds a fast machine.
    let reads_per_thread = std::env::var("RT_READS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5_000_000usize);
    let window_secs = std::env::var("RT_SECONDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8u64);
    // Churn: a background writer keeps rewriting keys so the cleaner has
    // obsolete space to reclaim (this is what creates reclaimable files whose
    // live LNs must be migrated before deletion).
    let churn_rounds = std::env::var("RT_CHURN")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8usize);

    let dir = TempDir::new().unwrap();
    let (env, db) = open_pressure_env(dir.path(), cache);

    let val = vec![0xA5u8; value_len];
    // Preflight: load all keys and verify every one is present single-threaded.
    fill_batched(&env, &db, records, &val);
    for i in 0..records {
        let k = format!("{i:010}");
        let got = db.get(k.as_bytes()).expect("preflight get must not Err");
        assert!(got.is_some(), "preflight: committed key {i} must be present");
    }

    let env = Arc::new(env);
    let db = Arc::new(db);
    let stop = Arc::new(AtomicBool::new(false));
    // First observed failure (any committed-key read that returned Err, or a
    // get that returned None for a key we committed).
    let first_err: Arc<std::sync::Mutex<Option<String>>> =
        Arc::new(std::sync::Mutex::new(None));
    let wrong_none = Arc::new(AtomicU64::new(0));
    let barrier = Arc::new(Barrier::new(threads + 3)); // readers + churn + clean + watchdog

    let mut handles = Vec::new();

    // Watchdog: stop everyone after the wall-clock window elapses.
    {
        let stop = Arc::clone(&stop);
        let barrier = Arc::clone(&barrier);
        handles.push(std::thread::spawn(move || {
            barrier.wait();
            let deadline = std::time::Instant::now()
                + std::time::Duration::from_secs(window_secs);
            while std::time::Instant::now() < deadline {
                if stop.load(Ordering::Relaxed) {
                    return;
                }
                std::thread::sleep(std::time::Duration::from_millis(25));
            }
            stop.store(true, Ordering::Relaxed);
        }));
    }

    // Reader threads: hammer point reads on committed keys.
    for t in 0..threads {
        let db = Arc::clone(&db);
        let env = Arc::clone(&env);
        let stop = Arc::clone(&stop);
        let first_err = Arc::clone(&first_err);
        let wrong_none = Arc::clone(&wrong_none);
        let barrier = Arc::clone(&barrier);
        handles.push(std::thread::spawn(move || {
            barrier.wait();
            let mut key = t;
            for _ in 0..reads_per_thread {
                if stop.load(Ordering::Relaxed) {
                    break;
                }
                key = (key + 2_654_435_761).wrapping_rem(records);
                let k = format!("{key:010}");
                match db.get(k.as_bytes()) {
                    Ok(Some(_)) => {}
                    Ok(None) => {
                        // A committed key reported NotFound: lost read.
                        wrong_none.fetch_add(1, Ordering::Relaxed);
                        let mut g = first_err.lock().unwrap();
                        if g.is_none() {
                            *g = Some(format!(
                                "Ok(None) for committed key {key} (lost read)"
                            ));
                        }
                        stop.store(true, Ordering::Relaxed);
                        break;
                    }
                    Err(e) => {
                        // Transient lock contention with the churn writer is
                        // NOT the bug — it is a retryable LockConflict /
                        // DeadlockDetected / LockTimeout. Only a FATAL env
                        // failure (the LogFileNotFound -> invalidated-env
                        // cleaner<->LN-fetch reclaim violation) is the teeth.
                        use noxu_db::NoxuError as NE;
                        let transient = matches!(
                            e,
                            NE::LockConflict(_)
                                | NE::DeadlockDetected
                                | NE::LockTimeout { .. }
                        );
                        if transient {
                            continue;
                        }
                        // This is the LogFileNotFound -> invalidated env bug.
                        let mut g = first_err.lock().unwrap();
                        if g.is_none() {
                            *g = Some(format!(
                                "Err on committed key {key}: {e} | env_valid={}",
                                env.is_valid()
                            ));
                        }
                        stop.store(true, Ordering::Relaxed);
                        break;
                    }
                }
            }
        }));
    }

    // Churn thread: rewrite keys so the cleaner has obsolete space to reclaim.
    {
        let db = Arc::clone(&db);
        let env = Arc::clone(&env);
        let stop = Arc::clone(&stop);
        let barrier = Arc::clone(&barrier);
        let churn_val = val;
        handles.push(std::thread::spawn(move || {
            barrier.wait();
            let _ = churn_rounds; // kept for env-override compatibility
            while !stop.load(Ordering::Relaxed) {
                // Rewrite in batches so previous versions become obsolete.
                let mut i = 0usize;
                while i < records {
                    if stop.load(Ordering::Relaxed) {
                        break;
                    }
                    let end = (i + 1000).min(records);
                    let txn = match env.begin_transaction(None) {
                        Ok(t) => t,
                        Err(_) => return,
                    };
                    for j in i..end {
                        let k = DatabaseEntry::from_vec(
                            format!("{j:010}").into_bytes(),
                        );
                        if db
                            .put_in(
                                &txn,
                                &k,
                                DatabaseEntry::from_bytes(&churn_val),
                            )
                            .is_err()
                        {
                            let _ = txn.abort();
                            return;
                        }
                    }
                    if txn.commit().is_err() {
                        return;
                    }
                    i = end;
                }
            }
        }));
    }

    // Cleaner driver: force cleaning + checkpoints to accelerate reclaim.
    {
        let env = Arc::clone(&env);
        let stop = Arc::clone(&stop);
        let barrier = Arc::clone(&barrier);
        handles.push(std::thread::spawn(move || {
            barrier.wait();
            while !stop.load(Ordering::Relaxed) {
                if !env.is_valid() {
                    break;
                }
                let _ = env.clean_log();
                let _ = env.checkpoint(None);
                let _ = env.clean_log();
                std::thread::yield_now();
            }
        }));
    }

    for h in handles {
        let _ = h.join();
    }

    // Final quiescent re-scan: every committed key must still be readable and
    // the env must still be valid.
    let env_valid = env.is_valid();
    let err = first_err.lock().unwrap().clone();

    assert!(
        err.is_none(),
        "cleaner<->LN-fetch reclaim violation: {}",
        err.unwrap()
    );
    assert_eq!(
        wrong_none.load(Ordering::Relaxed),
        0,
        "no committed key may ever report NotFound"
    );
    assert!(
        env_valid,
        "env must stay VALID under concurrent cleaning (LogFileNotFound \
         invalidation is the bug)"
    );

    // Post-window quiescent scan (only meaningful if env is still valid).
    for i in 0..records {
        let k = format!("{i:010}");
        let got = db.get(k.as_bytes()).unwrap_or_else(|e| {
            panic!("post-scan get for key {i} must not Err: {e}")
        });
        assert!(
            got.is_some(),
            "post-scan: committed key {i} must still be present"
        );
    }
}
