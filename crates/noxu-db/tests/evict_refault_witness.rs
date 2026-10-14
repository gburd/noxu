//! Per-defect witnesses for BUG-EVICT-REFAULT-DATALOSS.
//!
//! Each targets one of the three cleaner-liveness defects and must be RED on
//! the c1a base (42e55cbb) and GREEN after the A28 commits.  They use a small
//! bounded concurrent driver (a handful of reader threads + a forced
//! evict/checkpoint/clean loop) rather than the 32-thread stress witness, so
//! the evict->clean->refault race fires within a short, bounded window.  The
//! signal is the FINAL quiescent rescan: any committed key missing after the
//! driver stops is durable loss.

use noxu_db::{
    Database, DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig,
};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use tempfile::TempDir;

fn open_env(dir: &std::path::Path, cache: u64) -> Environment {
    let mut cfg = EnvironmentConfig::new(dir.to_path_buf());
    cfg.set_allow_create(true);
    cfg.set_transactional(true);
    cfg.set_cache_percent(0);
    cfg.set_cache_size(cache);
    cfg.set_run_cleaner(true);
    cfg.set_run_evictor(true);
    cfg.set_run_checkpointer(true);
    cfg.set_run_in_compressor(true);
    cfg.set_log_file_max_bytes(64 * 1024);
    cfg.set_cleaner_min_utilization(90);
    Environment::open(cfg).expect("open env")
}

fn open_db(env: &Environment, name: &str) -> Database {
    env.open_database(
        None,
        name,
        &DatabaseConfig::new().with_allow_create(true).with_transactional(true),
    )
    .expect("open db")
}

fn fill(env: &Environment, db: &Database, n: usize, val: &[u8]) {
    let mut i = 0usize;
    while i < n {
        let end = (i + 500).min(n);
        let txn = env.begin_transaction(None).unwrap();
        for j in i..end {
            let k = DatabaseEntry::from_vec(format!("{j:010}").into_bytes());
            db.put_in(&txn, &k, DatabaseEntry::from_bytes(val)).unwrap();
        }
        txn.commit().unwrap();
        i = end;
    }
}

/// Shared driver: `readers` reader threads hammer point gets on committed keys
/// while a driver thread forces evict + checkpoint + clean, for `secs`.  Then
/// quiesce and rescan: returns the number of durably-missing committed keys.
fn run_evict_clean_race(
    env: Environment,
    db: Database,
    n: usize,
    readers: usize,
    secs: u64,
) -> usize {
    let env = Arc::new(env);
    let db = Arc::new(db);
    let stop = Arc::new(AtomicBool::new(false));
    let in_flight = Arc::new(AtomicU64::new(0));
    let barrier = Arc::new(Barrier::new(readers + 3));
    let mut handles = Vec::new();

    for t in 0..readers {
        let (db, stop, barrier, in_flight) = (
            Arc::clone(&db),
            Arc::clone(&stop),
            Arc::clone(&barrier),
            Arc::clone(&in_flight),
        );
        handles.push(std::thread::spawn(move || {
            barrier.wait();
            let mut key = t;
            while !stop.load(Ordering::Relaxed) {
                key = (key + 2_654_435_761).wrapping_rem(n);
                let k = format!("{key:010}");
                if let Ok(None) = db.get(k.as_bytes()) {
                    in_flight.fetch_add(1, Ordering::Relaxed);
                }
            }
        }));
    }
    // Churn writer: rewrite keys so the cleaner has obsolete space to reclaim
    // (this is what creates reclaimable files whose live images must survive,
    // and the BIN-delta chains for the 476 defect).
    {
        let (env, db, stop, barrier) = (
            Arc::clone(&env),
            Arc::clone(&db),
            Arc::clone(&stop),
            Arc::clone(&barrier),
        );
        handles.push(std::thread::spawn(move || {
            barrier.wait();
            let v = vec![0x5Au8; 300];
            while !stop.load(Ordering::Relaxed) {
                let mut i = 0usize;
                while i < n && !stop.load(Ordering::Relaxed) {
                    let end = (i + 500).min(n);
                    let txn = match env.begin_transaction(None) {
                        Ok(t) => t,
                        Err(_) => return,
                    };
                    for j in i..end {
                        let k = DatabaseEntry::from_vec(
                            format!("{j:010}").into_bytes(),
                        );
                        if db
                            .put_in(&txn, &k, DatabaseEntry::from_bytes(&v))
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
    // Evict/clean driver.
    {
        let (env, stop, barrier) =
            (Arc::clone(&env), Arc::clone(&stop), Arc::clone(&barrier));
        handles.push(std::thread::spawn(move || {
            barrier.wait();
            while !stop.load(Ordering::Relaxed) {
                let _ = env.evict_memory();
                let _ = env.checkpoint(None);
                let _ = env.clean_log_forced();
                std::thread::yield_now();
            }
        }));
    }

    barrier.wait();
    std::thread::sleep(std::time::Duration::from_secs(secs));
    stop.store(true, Ordering::Relaxed);
    for h in handles {
        let _ = h.join();
    }

    // Quiescent rescan: count durable losses.
    let mut missing = 0usize;
    let mut first = None;
    for i in 0..n {
        let k = format!("{i:010}");
        match db.get(k.as_bytes()) {
            Ok(Some(_)) => {}
            Ok(None) => {
                // retry once to exclude a transient lock error masquerading
                if let Ok(None) = db.get(k.as_bytes()) {
                    missing += 1;
                    if first.is_none() {
                        first = Some(i);
                    }
                }
            }
            Err(_) => {}
        }
    }
    eprintln!(
        "in_flight_none={} durable_missing={missing} first={:?} env_valid={}",
        in_flight.load(Ordering::Relaxed),
        first,
        env.is_valid()
    );
    missing
}

/// bde3b613 (dominant): the only on-disk image of an EVICTED (non-resident)
/// live BIN must survive cleaning.
#[test]
fn defect_bde_evicted_live_bin_image_survives_clean() {
    let dir = TempDir::new().unwrap();
    let env = open_env(dir.path(), 512 * 1024);
    let db = open_db(&env, "user_bde");
    let n = 20_000usize;
    fill(&env, &db, n, &vec![0xBDu8; 300]);
    env.checkpoint(None).unwrap();
    let missing = run_evict_clean_race(env, db, n, 16, 8);
    assert_eq!(
        missing, 0,
        "{missing} committed keys VANISHED (bde: evicted live BIN image reclaimed)"
    );
}

/// cee317d64c: a NAMED user-DB BIN image must be classified live (owning tree,
/// not primary tree).
#[test]
fn defect_cee317_user_db_bin_live() {
    let dir = TempDir::new().unwrap();
    let env = open_env(dir.path(), 512 * 1024);
    let _primary = open_db(&env, "primary_small");
    let db = open_db(&env, "user_cee");
    let n = 20_000usize;
    fill(&env, &db, n, &vec![0xCEu8; 300]);
    env.checkpoint(None).unwrap();
    let missing = run_evict_clean_race(env, db, n, 16, 8);
    assert_eq!(
        missing, 0,
        "{missing} committed keys VANISHED (cee317: user-DB BIN looked up in primary tree)"
    );
}

/// 476322daaa: a BIN logged as a delta (and the full base a newer delta chains
/// onto) must be classified live.
#[test]
fn defect_476_delta_chain_image_live() {
    let dir = TempDir::new().unwrap();
    let env = open_env(dir.path(), 512 * 1024);
    let db = open_db(&env, "user_476");
    let n = 20_000usize;
    let val = vec![0x47u8; 300];
    fill(&env, &db, n, &val);
    env.checkpoint(None).unwrap();
    // Dirty a few slots per BIN then checkpoint -> BINs logged as deltas.
    let txn = env.begin_transaction(None).unwrap();
    for i in (0..n).step_by(40) {
        let k = DatabaseEntry::from_vec(format!("{i:010}").into_bytes());
        db.put_in(&txn, &k, DatabaseEntry::from_bytes(&val)).unwrap();
    }
    txn.commit().unwrap();
    env.checkpoint(None).unwrap();
    let missing = run_evict_clean_race(env, db, n, 16, 8);
    assert_eq!(
        missing, 0,
        "{missing} committed keys VANISHED (476: delta-chain base reclaimed)"
    );
}
