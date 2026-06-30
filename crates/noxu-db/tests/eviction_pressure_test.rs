//! Eviction-under-pressure tests.
//!
//! These validate the evictor F1+F2 wiring (LRU lists fed from production tree
//! ops; eviction decrements the shared cache_usage counter) and the F8/F10
//! tuning in the regime that matters: a cache SMALLER than the working set, so
//! eviction actually runs. The default 64 MiB cache never evicts at typical
//! test scales, which is why the JE comparison benchmark (64 MiB cache, ~15 MB
//! working set) did not exercise eviction.

use noxu_db::{
    Database, DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig,
    OperationStatus,
};
use tempfile::TempDir;

fn open_small_cache_env(
    dir: &std::path::Path,
    cache_bytes: u64,
) -> (Environment, Database) {
    let mut cfg = EnvironmentConfig::new(dir.to_path_buf());
    cfg.set_allow_create(true);
    cfg.set_transactional(true);
    cfg.set_cache_percent(0); // so set_cache_size takes effect
    cfg.set_cache_size(cache_bytes);
    let env = Environment::open(cfg).expect("open env");
    let db = env
        .open_database(
            None,
            "evict",
            &DatabaseConfig::new().with_allow_create(true),
        )
        .expect("open db");
    (env, db)
}

/// With a cache far smaller than the working set, after inserting many records
/// and running eviction the cache_usage must be bounded (eviction actually
/// reduces it) AND every record must still be readable (correctness preserved).
#[test]
fn eviction_bounds_cache_and_preserves_data() {
    let dir = TempDir::new().unwrap();
    // 2 MiB cache; ~50k records * ~120 B = ~6 MB working set -> must evict.
    let (env, db) = open_small_cache_env(dir.path(), 2 * 1024 * 1024);

    let n = 50_000usize;
    let val = vec![0u8; 100];
    for i in 0..n {
        let k = DatabaseEntry::from_vec(format!("{:010}", i).into_bytes());
        let v = DatabaseEntry::from_bytes(&val);
        db.put( &k, &v).unwrap();
    }

    // Run eviction explicitly (the daemon also runs, but make it deterministic).
    let _ = env.evict_memory().unwrap();

    // F2: eviction must have reduced cache_usage. With a 2 MiB cache and a
    // ~6 MB working set, usage must not be wildly above the budget. We assert
    // it is at least bounded below the full working set (i.e. eviction did
    // something) — a non-evicting (inert) evictor would let usage grow to the
    // full ~6 MB.
    let stats = env.get_stats().unwrap();
    assert!(
        stats.cache_usage < 6 * 1024 * 1024,
        "eviction must bound cache_usage below the full working set; got {} bytes",
        stats.cache_usage
    );

    // Correctness: every record is still readable (eviction must not lose data
    // — evicted nodes are recoverable from the log).
    for i in (0..n).step_by(97) {
        let k = DatabaseEntry::from_vec(format!("{:010}", i).into_bytes());
        let mut out = DatabaseEntry::new();
        let st = db.get_into(None, &k, &mut out).unwrap();
        assert!(st,
            "record {} must survive eviction",
            i
        );
        assert_eq!(out.data(), &val[..], "record {} data intact", i);
    }
}

/// A delete-heavy workload under a small cache must not make cache_usage drift
/// upward unboundedly (F8: delete subtracts key+data+48, not just key+48).
#[test]
fn delete_heavy_does_not_inflate_cache_usage() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_small_cache_env(dir.path(), 4 * 1024 * 1024);

    let val = vec![0u8; 100];
    // Insert then delete the same keys many times. With the F8 leak, each
    // delete would under-subtract by data_len (100B), inflating cache_usage.
    for round in 0..20 {
        for i in 0..2_000usize {
            let k = DatabaseEntry::from_vec(
                format!("r{}-{:08}", round % 2, i).into_bytes(),
            );
            db.put( &k, &DatabaseEntry::from_bytes(&val)).unwrap();
        }
        for i in 0..2_000usize {
            let k = DatabaseEntry::from_vec(
                format!("r{}-{:08}", round % 2, i).into_bytes(),
            );
            let _ = db.delete( &k);
        }
    }
    let _ = env.evict_memory().unwrap();
    let stats = env.get_stats().unwrap();
    // After 40k inserts + 40k deletes of a ~2k-key working set, usage must stay
    // bounded (the live set is small). A data_len leak on delete would make
    // this grow without bound across rounds.
    assert!(
        stats.cache_usage < 8 * 1024 * 1024,
        "delete-heavy churn must not inflate cache_usage (F8); got {} bytes",
        stats.cache_usage
    );
}

/// A full cursor scan over a working set larger than the cache must return the
/// correct data for EVERY record (the scan path must re-hydrate stripped LNs
/// from the log, not return empty data). Validates the scan-path fetchTarget.
#[test]
fn cursor_scan_under_eviction_returns_all_data() {
    use noxu_db::Get;
    let dir = TempDir::new().unwrap();
    let (env, db) = open_small_cache_env(dir.path(), 2 * 1024 * 1024);

    let n = 20_000usize;
    let val = vec![7u8; 80];
    for i in 0..n {
        let k = DatabaseEntry::from_vec(format!("{:010}", i).into_bytes());
        db.put( &k, &DatabaseEntry::from_bytes(&val)).unwrap();
    }
    let _ = env.evict_memory().unwrap();

    // Scan the whole database with a cursor; every record's data must be the
    // full 80-byte value, never empty (which would mean a stripped LN was not
    // re-fetched from the log on the scan path).
    let mut cursor = db.open_cursor( None).unwrap();
    let mut key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();
    let mut count = 0usize;
    let mut st = cursor.get(&mut key, &mut data, Get::First, None).unwrap();
    while st == OperationStatus::Success {
        assert_eq!(
            data.data(),
            &val[..],
            "scanned record {} ({:?}) must have full data, not stripped/empty",
            count,
            String::from_utf8_lossy(key.data())
        );
        count += 1;
        st = cursor.get(&mut key, &mut data, Get::Next, None).unwrap();
    }
    assert_eq!(count, n, "scan must visit every record");
}
