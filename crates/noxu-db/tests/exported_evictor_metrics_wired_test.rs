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

/// Q1 asymmetry guard (V22/B3): `bin_fetch` and `bin_fetch_miss` must stay
/// consistent on the **non-cursor** descent paths, not only on
/// `Tree::search_with_data` (which `db.get` uses).
///
/// The miss counter is recorded at the single fault site
/// (`Tree::child_at_or_fetch` / `fetch_root_from_log`) shared by every
/// descent; the matching `bin_fetch` count must be recorded at that same
/// fault site (for a faulted BIN) plus at the resident BIN-arrival point (for
/// a hit). `search_with_data` records the arrival count; the cleaner
/// LN-liveness probe (`Cleaner` -> `lookup_parent_bin` -> `Tree::search`)
/// originally did NOT. So a cleaner probe that faulted a cold (evicted) BIN
/// recorded a MISS with no matching FETCH. Once cleaner misses out-run cursor
/// fetches, `bin_fetch_miss > bin_fetch`, `bin_fetch_miss_ratio() > 1.0`, and
/// the exported `noxu_evictor_cache_hit_ratio` (`1.0 - ratio`) goes NEGATIVE
/// -- the same "dashboard lies" defect B3/V22 set out to kill.
///
/// JE has no such asymmetry: `IN.incFetchStats(envImpl, isMiss)`
/// (IN.java:3003) fires on **every** `fetchTarget` -- hit and miss together --
/// including the cleaner's `getParentBINForChildLN` / `tree.search(...)`
/// probes (FileProcessor.java:1140,1507). Fetch and miss are always
/// incremented as a pair at the fault site.
///
/// This test drives the cleaner miss path directly and asserts the invariant
/// `bin_fetch_miss <= bin_fetch` and a strictly-in-`[0, 1]` exported
/// hit_ratio. It FAILS on 515ce63d (cleaner-only misses drive the ratio > 1,
/// hit_ratio < 0) and PASSES once the fetch count is recorded at the shared
/// fault site.
///
/// UN-IGNORED (NEW-7 eviction convergence fix): `Evictor::do_evict` now LOOPS
/// `evict_batch` until the budget is met or no progress is made (JE
/// `Evictor.doEvict` loops `evictBatch`), so bounded manual eviction reaches
/// the phase-2 pri2 drain and FULLY evicts BINs. A fully-evicted (cold) BIN is
/// a `Tree::search` MISS for the cleaner's LN-liveness probe, so
/// `bin_fetch_miss > 0` is met again and this guard is non-vacuous. (History:
/// it was `#[ignore]`d after NEW-2 correctly made per-DB `NODE_MAX_ENTRIES`
/// take effect, which — combined with the single-capped-batch `do_evict` —
/// meant BINs were only LN-stripped and parked resident in pri2, never fully
/// evicted, so the cleaner probe only ever hit resident BINs and
/// `bin_fetch_miss` stayed 0. The NEW-7 loop fix removes that limitation.)
#[test]
fn cleaner_search_miss_path_keeps_hit_ratio_in_range() {
    let dir = TempDir::new().unwrap();

    // Small cache + small log files: a few hundred 1 KiB records span many
    // files and cannot all stay resident, so eviction leaves cold BINs that
    // the cleaner's LN-liveness probe must fault back from the log.
    let mut cfg = EnvironmentConfig::new(dir.path().to_path_buf());
    cfg.set_allow_create(true);
    cfg.set_transactional(true);
    cfg.set_cache_percent(0);
    cfg.set_cache_size(1024 * 1024);
    cfg.set_log_file_max_bytes(64 * 1024);
    // Drive cleaning explicitly (no daemon race) and keep the checkpointer off
    // so the cleaner sees a large obsolete fraction to reclaim.
    cfg.set_run_cleaner(false);
    cfg.set_run_checkpointer(false);
    let env = Environment::open(cfg).expect("open env");
    let db = env
        .open_database(
            None,
            "clean",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .expect("open db");

    let n_records = 4_000usize;
    let value = vec![0xCDu8; 1024];
    // Initial load.
    let mut i = 0usize;
    while i < n_records {
        let end = (i + 300).min(n_records);
        let txn = env.begin_transaction(None).unwrap();
        for j in i..end {
            db.put_in(&txn, format!("{:012}", j).into_bytes(), &value).unwrap();
        }
        txn.commit().unwrap();
        i = end;
    }
    // Churn: overwrite every key several times so a large fraction of the log
    // becomes obsolete and the cleaner has files worth reclaiming (each still
    // holding live LNs whose BINs it must probe).
    for _ in 0..1 {
        let mut i = 0usize;
        while i < n_records {
            let end = (i + 300).min(n_records);
            let txn = env.begin_transaction(None).unwrap();
            for j in i..end {
                db.put_in(&txn, format!("{:012}", j).into_bytes(), &value)
                    .unwrap();
            }
            txn.commit().unwrap();
            i = end;
        }
    }
    env.checkpoint(None).unwrap();

    // Force eviction so the cleaner's Tree::search probes fault COLD BINs from
    // the log (recording misses). Crucially we do NOT warm the cache with
    // db.get first -- the miss traffic here comes from the cleaner's
    // Tree::search path, not the cursor search_with_data path.
    let _ = env.evict_memory().unwrap();

    // Run the cleaner: it walks obsolete files, and for each still-live LN it
    // calls lookup_parent_bin -> Tree::search, faulting the evicted BIN.
    // Repeat a few times to accumulate cleaner-side misses.
    for _ in 0..3 {
        env.clean_log().unwrap();
        let _ = env.evict_memory().unwrap();
    }

    let stats = env.stats().unwrap();
    let e = &stats.evictor;

    // The invariant JE maintains by pairing fetch+miss at the fault site: a
    // BIN reached (fetch) is a superset of a BIN faulted (miss).
    assert!(
        e.bin_fetch_miss <= e.bin_fetch,
        "bin_fetch_miss ({}) exceeds bin_fetch ({}) -- a descent recorded a \
         cache MISS with no matching FETCH. The cleaner's Tree::search probe \
         faults cold BINs but did not count the fetch, so the exported \
         cache-hit ratio is corrupted (JE incFetchStats pairs both).",
        e.bin_fetch_miss,
        e.bin_fetch
    );

    // The exported gauge: noxu_evictor_cache_hit_ratio = 1 - miss/fetch. If
    // miss > fetch this goes NEGATIVE -- the shipped dashboard lie.
    let hit_ratio = 1.0 - stats.bin_fetch_miss_ratio();
    assert!(
        (0.0..=1.0).contains(&hit_ratio),
        "noxu_evictor_cache_hit_ratio ({hit_ratio}) is outside [0, 1] after a \
         cleaner run -- the fetch/miss counters are asymmetric on the \
         Tree::search descent (fabricated-metric bug class B3/V22)."
    );

    // Precondition: the workload actually exercised the cleaner miss path, so
    // this test is not vacuously green. bin_fetch_miss must have moved.
    assert!(
        e.bin_fetch_miss > 0,
        "precondition: the cleaner/eviction workload must record BIN misses; \
         bin_fetch_miss is 0 so the Tree::search miss path was not exercised."
    );

    drop(db);
    env.close().unwrap();
}
