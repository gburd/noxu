//! Root-cause guard for the "stat struct with no production writer" class
//! (V24 / B3-B4). Every evictor field exported by `noxu-observe`
//! (`export.rs::emit`) must be backed by a REAL production writer.
//!
//! The fabricated-metric bug recurred three times — CleanerStats (v7.9.1),
//! `EvictorStats.bin_fetch`/`bin_fetch_miss` (V22), and RepStats (V23) — all
//! the same shape: a stat is defined, exported, and never incremented, so the
//! gauge reads a constant (0, or worse, a misleadingly-healthy 1.0). A grep
//! for `.increment()` call sites is not enough (it passes against a store in a
//! test module); this test drives a real eviction workload and asserts each
//! EXPORTED evictor field actually moves.
//!
//! The exported evictor metrics are (noxu-observe/src/export.rs::emit):
//!   noxu_evictor_runs_total          <- eviction_runs
//!   noxu_evictor_nodes_evicted_total <- nodes_evicted
//!   noxu_evictor_bytes_evicted_total <- bytes_evicted
//!   noxu_evictor_bin_fetch_total     <- bin_fetch
//!   noxu_evictor_bin_fetch_miss_total<- bin_fetch_miss
//!   noxu_evictor_cache_hit_ratio     <- 1 - bin_fetch_miss/bin_fetch
//!   noxu_evictor_lru_size            <- pri1_lru_size + pri2_lru_size
//!
//! A field that stays 0 under a workload that provably exercises eviction and
//! cold re-faults is unwired — the exact failure this guards against.

use noxu_db::{DatabaseConfig, Environment, EnvironmentConfig};
use tempfile::TempDir;

#[test]
fn every_exported_evictor_metric_has_a_real_writer() {
    let dir = TempDir::new().unwrap();

    // 1 MiB cache, ~4 MiB dataset: forces real eviction and cold re-faults.
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
            "sweep",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .expect("open db");

    let n_records = 4_000usize;
    let value = vec![0xEEu8; 1024];
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
    // Force eviction, then read the whole key space several times so both cold
    // re-faults (misses) and warm re-reads (hits) occur.
    let _ = env.evict_memory().unwrap();
    for _pass in 0..4 {
        for j in 0..n_records {
            let _ = db.get(format!("{:012}", j).into_bytes()).unwrap();
        }
    }

    let stats = env.stats().unwrap();
    let e = &stats.evictor;

    // Each closure names the EXPORTED metric and its backing field(s). A 0
    // here means the exported gauge/counter is fabricated.
    let checks: [(&str, u64); 5] = [
        ("noxu_evictor_runs_total (eviction_runs)", e.eviction_runs),
        ("noxu_evictor_nodes_evicted_total (nodes_evicted)", e.nodes_evicted),
        ("noxu_evictor_bytes_evicted_total (bytes_evicted)", e.bytes_evicted),
        ("noxu_evictor_bin_fetch_total (bin_fetch)", e.bin_fetch),
        (
            "noxu_evictor_bin_fetch_miss_total (bin_fetch_miss)",
            e.bin_fetch_miss,
        ),
    ];
    for (metric, value) in checks {
        assert!(
            value > 0,
            "{metric} is 0 under a workload that provably evicts and re-faults \
             -- the exported metric is unwired (fabricated-stat bug class)."
        );
    }

    // lru_size (pri1+pri2) is an instantaneous gauge: the resident-node LRU
    // must be non-empty while the tree holds data.
    assert!(
        e.lru_size > 0,
        "noxu_evictor_lru_size (pri1_lru_size + pri2_lru_size) is 0 while the \
         tree holds {n_records} records -- the LRU-size gauge is unwired."
    );

    // cache_hit_ratio: derived from bin_fetch / bin_fetch_miss. Must be a
    // plausible mix, not the fabricated 1.0 and not a degenerate 0.0.
    let hit_ratio = 1.0 - stats.bin_fetch_miss_ratio();
    assert!(
        hit_ratio > 0.0 && hit_ratio < 1.0,
        "noxu_evictor_cache_hit_ratio ({hit_ratio}) is degenerate; a real \
         hit/miss mix must fall strictly inside (0, 1)."
    );

    drop(db);
    env.close().unwrap();
}
