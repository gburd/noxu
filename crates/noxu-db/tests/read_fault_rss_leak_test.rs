//! Regression test for the cold-LN-fault RSS leak (fix/read-fault-rss-leak).
//!
//! # The bug (measured, unbounded)
//!
//! On a PURE READ workload over a dataset larger than the cache, process RSS
//! grew UNBOUNDED and LINEAR instead of plateauing near the cache budget.  Two
//! independent defects combined to cause it:
//!
//!  1. **Uncounted fetched-in LN data (`Tree::fetch_node_from_log`).**  A full
//!     BIN log entry serialises its LN VALUES inline, so a BIN re-fetched on a
//!     cold fault (`child_at_or_fetch`) comes back with tens of KB of resident
//!     LN `data` — but the fetch path only added the node to the eviction
//!     policy (`note_added`) and NEVER charged that resident data to the shared
//!     `cache_usage` counter (JE `IN.postFetchInit` -> `initMemorySize` +
//!     `updateTreeMemoryUsage(+size)`).  The budget signal stayed far below the
//!     true heap, so eviction could not tell it was over budget.
//!
//!  2. **No foreground back-pressure on the READ path (`Database::get_bytes`).**
//!     JE calls `EnvironmentImpl.criticalEviction()` before EVERY cursor op
//!     (reads included, `Cursor.beginMoveCursor`).  Noxu only wired it on the
//!     WRITE path, so a pure-read workload relied solely on the single
//!     background daemon, which cannot keep pace with N reader threads faulting
//!     + re-fetching BINs.
//!
//! A secondary accounting bug (`detach_node_by_id` refusing to evict a
//! re-fetched BIN whose `last_full_lsn` was left NULL, while `node_size_fn`
//! still credited the eviction) was fixed by stamping the fetched-from LSN into
//! the re-fetched BIN (JE `setLastLoggedLsn`).
//!
//! # What this test asserts
//!
//! Load a dataset several times the cache, then run many read-only operations
//! (each a cold-LN fault that re-fetches + repopulates a BIN) and sample
//! process RSS (Linux `/proc/self/statm`).  RSS must PLATEAU near the cache
//! budget + fixed overhead — it must NOT grow linearly with the number of
//! reads.  On the pre-fix (leaking) engine RSS climbs by hundreds of MB across
//! the read loop; after the fix it stays flat.

#![cfg(target_os = "linux")]

use noxu_db::{DatabaseConfig, Environment, EnvironmentConfig};
use tempfile::TempDir;

/// Resident set size of THIS process, in bytes, read from `/proc/self/statm`
/// (field 2 = resident pages).  Linux-only.
fn rss_bytes() -> u64 {
    let statm = std::fs::read_to_string("/proc/self/statm")
        .expect("read /proc/self/statm");
    let resident_pages: u64 = statm
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .expect("parse resident pages");
    let page_size = 4096u64; // Linux default; sufficient for a coarse bound.
    resident_pages * page_size
}

/// Read a `>cache` dataset for many ops; RSS must stay bounded (plateau near
/// the cache budget), not grow linearly with the read count.
///
/// FAILS on origin/main (RSS climbs unbounded on the cold-fault read path).
/// PASSES after the fix (fetched data is budgeted + the read path applies
/// critical-eviction back-pressure, so eviction holds RSS at the cache size).
#[test]
fn read_only_workload_rss_stays_bounded() {
    let dir = TempDir::new().unwrap();

    // 8 MiB cache; ~80k records * ~1 KiB value = ~80 MiB dataset -> 10x
    // cache.  The dataset must GREATLY exceed the cache so that a bounded
    // engine holds RSS at ~cache size while the leak (uncounted fetched-in LN
    // data + no read back-pressure) lets the resident set grow toward the
    // full ~80 MiB dataset -- a clear, large separation.
    let cache_bytes = 8 * 1024 * 1024u64;
    let mut cfg = EnvironmentConfig::new(dir.path().to_path_buf());
    cfg.set_allow_create(true);
    cfg.set_transactional(true);
    cfg.set_cache_percent(0);
    cfg.set_cache_size(cache_bytes);
    let env = Environment::open(cfg).expect("open env");
    let db = env
        .open_database(
            None,
            "leak",
            &DatabaseConfig::new().with_allow_create(true),
        )
        .expect("open db");

    let n_records = 80_000usize;
    let value = vec![0xABu8; 1024]; // 1 KiB values, like the bench.
    for i in 0..n_records {
        db.put(format!("{:012}", i).into_bytes(), &value).unwrap();
    }
    // Force the loaded set down toward the budget before the read phase so the
    // baseline is a warm, budget-sized cache (not the just-written working
    // set).  Take the BASELINE RSS immediately after eviction — before the
    // read loop has a chance to accumulate any leak — so the delta measured
    // below is purely the read-phase growth.
    let _ = env.evict_memory().unwrap();
    let rss_baseline = rss_bytes();

    // Read phase: many point reads across the whole (>>cache) key space.  Each
    // miss faults an LN from the log and re-fetches/repopulates a BIN — the
    // path that leaked.  A bounded engine keeps RSS flat near the cache size;
    // the leak grows the resident set toward the full ~120 MiB dataset.
    let read_key = |i: usize| -> Vec<u8> {
        // Stride through the key space so the working set stays >> cache and
        // most reads are cold faults (not repeat hits).
        let idx = (i.wrapping_mul(7919)) % n_records; // 7919 is prime.
        format!("{:012}", idx).into_bytes()
    };

    // Sustained read run: many reads across the full key space.  A leak grows
    // RSS toward the dataset size (~80 MiB); a bounded engine keeps it flat.
    let sustained = 700_000usize;
    for i in 0..sustained {
        let _ = db.get(read_key(i)).unwrap();
    }
    let rss_after = rss_bytes();

    let growth = rss_after.saturating_sub(rss_baseline);

    // The cache budget is 8 MiB.  A correct engine holds RSS at
    // budget + fixed overhead regardless of how many reads run, so the
    // baseline->after growth over 1.5M reads must be small.  The pre-fix
    // engine leaves fetched-in LN data resident-but-uncounted with no read
    // back-pressure, so the resident set climbs by tens-to-hundreds of MiB
    // toward the full ~120 MiB dataset.
    //
    // Bound: 40 MiB — generous slack over the 8 MiB budget for allocator
    // behaviour and measurement noise, but FAR below the tens-of-MiB the leak
    // produces at this dataset/cache ratio (the resident set climbs toward the
    // full ~80 MiB dataset on the pre-fix engine).
    let limit = 40 * 1024 * 1024u64;
    assert!(
        growth < limit,
        "read-only workload leaked memory: RSS grew {} MiB across {} reads \
         (baseline {} MiB -> after {} MiB); a bounded cache must keep this \
         near zero. Cache budget is {} MiB.",
        growth / (1024 * 1024),
        sustained,
        rss_baseline / (1024 * 1024),
        rss_after / (1024 * 1024),
        cache_bytes / (1024 * 1024),
    );

    // Sanity: the tracked cache_usage counter must also be bounded near the
    // budget (the fix makes the counter honest — it now reflects fetched-in
    // resident LN data, and eviction holds it at budget).
    let usage = env.stats().unwrap().cache_usage;
    assert!(
        usage < 4 * cache_bytes,
        "cache_usage must stay near the budget; got {} MiB (budget {} MiB)",
        usage / (1024 * 1024),
        cache_bytes / (1024 * 1024),
    );

    // Correctness: a sampled read still returns the right data after all the
    // fault/evict churn (evicted+re-fetched LNs must be intact).
    let got = db
        .get(format!("{:012}", 12345usize).into_bytes())
        .unwrap()
        .expect("record must still be readable");
    assert_eq!(got.as_ref(), &value[..], "re-fetched LN data must be intact");
}
