//! `EvictorStats.bin_fetch` / `bin_fetch_miss` must report REAL cache
//! hit/miss counts — the backing counters for the exported
//! `noxu_evictor_cache_hit_ratio` gauge (V22 / B3 / F-10).
//!
//! # The bug
//!
//! `noxu-observe` exports `noxu_evictor_cache_hit_ratio = 1.0 -
//! bin_fetch_miss_ratio()` (export.rs:246). `bin_fetch_miss_ratio()`
//! (noxu-engine env_stats.rs:99) = `bin_fetch_miss / bin_fetch`, returning
//! 0.0 when `bin_fetch == 0`. But `EvictorStats.bin_fetch` /
//! `bin_fetch_miss` had NO production writers — the only stores were the
//! `#[cfg(test)] reset()` and the crate's own unit tests. So `bin_fetch` was
//! ALWAYS 0, the ratio ALWAYS 0.0, and the exported gauge ALWAYS 1.0 (100 %
//! hit rate) regardless of workload or cache pressure. An operator watching a
//! thrashing cache saw a perfect dashboard. Same bug class as the CleanerStats
//! fields fixed in v7.9.1.
//!
//! # What this test asserts
//!
//! Force real eviction (small cache, dataset >> cache), then read the whole
//! key space back so the tree re-faults BINs from the log
//! (`Tree::child_at_or_fetch` slow path). After that:
//!  - `bin_fetch > 0`  — BIN accesses are counted at all,
//!  - `bin_fetch_miss > 0` — the cold re-faults are counted as misses,
//!  - the hit ratio is a PLAUSIBLE value strictly between 0 and 1 — NOT the
//!    fabricated 1.0.
//!
//! FAILS on eda0c208 (counters stuck at 0 -> ratio 1.0). PASSES once the
//! fetch/miss counters are wired into the tree fetch path.

use noxu_db::{DatabaseConfig, Environment, EnvironmentConfig};
use tempfile::TempDir;

#[test]
fn bin_fetch_counters_reflect_real_cache_misses() {
    let dir = TempDir::new().unwrap();

    // 1 MiB cache, ~4 MiB dataset (4x cache): small enough that a full
    // re-read after eviction must re-fault BINs from the log.
    let cache_bytes = 1024 * 1024u64;
    let mut cfg = EnvironmentConfig::new(dir.path().to_path_buf());
    cfg.set_allow_create(true);
    cfg.set_transactional(true);
    cfg.set_cache_percent(0);
    cfg.set_cache_size(cache_bytes);
    let env = Environment::open(cfg).expect("open env");
    let db = env
        .open_database(
            None,
            "hitrate",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .expect("open db");

    let n_records = 4_000usize;
    let value = vec![0xCDu8; 1024]; // 1 KiB values -> ~4 MiB dataset.
    let mut i = 0usize;
    while i < n_records {
        let end = (i + 500).min(n_records);
        let txn = env.begin_transaction(None).unwrap();
        for j in i..end {
            db.put_in(&txn, format!("{:012}", j).into_bytes(), &value).unwrap();
        }
        txn.commit().unwrap();
        i = end;
    }

    // Push the resident set down toward the budget so the read phase faults.
    let _ = env.evict_memory().unwrap();

    // Read the whole key space several times: every cold access re-faults a
    // BIN (a miss); repeat passes over the same keys hit resident BINs.
    for _pass in 0..3 {
        for j in 0..n_records {
            let _ = db.get(format!("{:012}", j).into_bytes()).unwrap();
        }
        // Keep pressure on so subsequent passes still see misses, not a fully
        // warm cache.
        let _ = env.evict_memory().unwrap();
    }

    let stats = env.stats().unwrap();
    let bin_fetch = stats.evictor.bin_fetch;
    let bin_fetch_miss = stats.evictor.bin_fetch_miss;
    let ratio = stats.bin_fetch_miss_ratio();
    let hit_ratio = 1.0 - ratio;

    // (1) BIN accesses must be counted at all. 0 means the stat is unwired
    //     (the fails-on-base condition: bin_fetch is never incremented).
    assert!(
        bin_fetch > 0,
        "bin_fetch is {bin_fetch}; it must count every BIN access. 0 means the \
         counter is unwired and the exported cache-hit-ratio gauge is \
         fabricated (permanently 1.0)."
    );

    // (2) The cold re-faults must be counted as misses.
    assert!(
        bin_fetch_miss > 0,
        "bin_fetch_miss is {bin_fetch_miss} despite a >>cache dataset re-read \
         after forced eviction; the miss counter is unwired."
    );

    // (3) The exported hit ratio must be a PLAUSIBLE value, not the fabricated
    //     1.0. With a 4x-cache dataset re-read under sustained eviction the
    //     real hit ratio is well below 1.0.
    assert!(
        bin_fetch_miss <= bin_fetch,
        "bin_fetch_miss ({bin_fetch_miss}) cannot exceed bin_fetch \
         ({bin_fetch})"
    );
    assert!(
        hit_ratio < 1.0 && hit_ratio >= 0.0,
        "cache hit ratio ({hit_ratio}) must reflect real misses; exactly 1.0 \
         is the fabricated value (bin_fetch={bin_fetch}, \
         bin_fetch_miss={bin_fetch_miss})."
    );

    drop(db);
    env.close().unwrap();
}
