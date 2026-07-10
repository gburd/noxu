//! Local repro for the EC2 read-push #2 finding: a theta~0.99 Zipfian read
//! workload over a cache sized to hold the hot set should hit 70%+, but the
//! production number is ~44%.  This test reproduces the *shape* of the problem
//! locally (skewed reads, hot keys interleaved with cold keys so they SHARE
//! BINs, continuous eviction pressure) and measures the hit rate via the same
//! `n_random_reads` counter the benchmark uses.
//!
//! Two scenarios are measured:
//!   A. hot keys in DEDICATED BINs (spread far apart) — the existing keep-hot
//!      test's regime; keep-hot works here.
//!   B. hot keys INTERLEAVED with cold keys (contiguous hot range) so a hot
//!      BIN also holds cold keys — the production Zipfian regime.  If the
//!      BIN-granular strip is the problem, B faults far more than A.

use noxu_db::{
    Database, DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tempfile::TempDir;

fn open_env(dir: &std::path::Path, cache_bytes: u64) -> (Environment, Database) {
    let mut cfg = EnvironmentConfig::new(dir.to_path_buf());
    cfg.set_allow_create(true);
    cfg.set_transactional(true);
    cfg.set_cache_percent(0);
    cfg.set_cache_size(cache_bytes);
    // Keep the log write-buffer pool tiny so the TREE budget ≈ cache_size
    // (the environment subtracts log_num_buffers * log_buffer_size from the
    // cache to size the tree/evictor arbiter).  Production uses a 4GB cache
    // where the default 3 * 1 MiB log buffers are negligible; a small-cache
    // local repro must shrink them or the tree budget collapses to the 1 MiB
    // floor and the measurement is dominated by that artifact, not the
    // eviction policy under test.
    cfg.set_log_num_buffers(2);
    cfg.set_log_buffer_size(64 * 1024);
    let env = Environment::open(cfg).expect("open env");
    let db = env
        .open_database(
            None,
            "zipf",
            &DatabaseConfig::new().with_allow_create(true),
        )
        .expect("open db");
    (env, db)
}

// Simple splitmix64 PRNG so the repro is deterministic and dependency-free.
fn next_rand(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E3779B97F4A7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
    z ^ (z >> 31)
}

/// Zipfian draw in [0, n) with skew theta (approximate, rejection-free).
/// Uses the standard inverse-CDF approximation (YCSB ZipfianGenerator).
fn zipf(state: &mut u64, n: usize, zetan: f64, theta: f64) -> usize {
    let alpha = 1.0 / (1.0 - theta);
    let eta = (1.0 - (2.0 / n as f64).powf(1.0 - theta))
        / (1.0 - zeta(2, theta) / zetan);
    let u = (next_rand(state) as f64) / (u64::MAX as f64);
    let uz = u * zetan;
    if uz < 1.0 {
        return 0;
    }
    if uz < 1.0 + 0.5f64.powf(theta) {
        return 1;
    }
    let ret = ((eta * u - eta + 1.0).powf(alpha) * n as f64) as usize;
    ret.min(n - 1)
}

fn zeta(n: usize, theta: f64) -> f64 {
    (1..=n).map(|i| 1.0 / (i as f64).powf(theta)).sum()
}

/// Measure the log-fault rate for a skewed read pattern.  `hot_key` maps a
/// draw to a key: scenario B (interleaved) keeps hot keys contiguous, so a hot
/// BIN also holds cold keys.
fn measure_fault_rate(
    dir: &std::path::Path,
    interleaved: bool,
) -> (f64, u64, u64) {
    // 4 MiB cache; working set ~5x the cache (production oversubscription).
    let (env, db) = open_env(dir, 4 * 1024 * 1024);
    let env = Arc::new(env);
    let total_n = 160_000usize; // ~160k * ~120 B = ~19 MB working set (~5x cache)
    let hot_n = 4_000usize; // hot set ~480 KB, fits the cache trivially
    let val = vec![0x5au8; 100];
    for i in 0..total_n {
        let k = DatabaseEntry::from_vec(format!("{:010}", i).into_bytes());
        db.put(&k, DatabaseEntry::from_bytes(&val)).unwrap();
    }
    let _ = env.evict_memory().unwrap();

    let read = |i: usize| {
        let k = DatabaseEntry::from_vec(format!("{:010}", i).into_bytes());
        let mut out = DatabaseEntry::new();
        let _ = db.get_into(None, &k, &mut out).unwrap();
    };

    // Warm the hot set: replay the Zipfian a bit so hot BINs are resident
    // and freshly touched.
    {
        let mut wrng = 0xABCD_EF01_u64;
        let theta = 0.99f64;
        let zetan = zeta(total_n, theta);
        for _ in 0..hot_n * 2 {
            let rank = zipf(&mut wrng, total_n, zetan, theta);
            let key = if interleaved {
                rank
            } else {
                (rank.wrapping_mul(40_009)) % total_n
            };
            read(key);
        }
    }
    let _ = env.evict_memory().unwrap();

    // Background eviction thread: mimic the continuously-running evictor
    // daemon under sustained oversubscription (every batch strips because
    // still_needs_eviction() is always true at ~5x cache).
    let stop = Arc::new(AtomicBool::new(false));
    let evictor_env = Arc::clone(&env);
    let evictor_stop = Arc::clone(&stop);
    let evictor = std::thread::spawn(move || {
        while !evictor_stop.load(Ordering::Relaxed) {
            let _ = evictor_env.evict_memory();
            // Throttle to mimic the throttled background daemon (default
            // wakeup 100ms) rather than a tight spin — a tight spin churns the
            // cache far harder than any real daemon and is not the workload
            // we are modelling.
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
    });

    // Measured phase: a true theta=0.99 Zipfian over the whole key space
    // (matching ycsb_c on EC2) while the background evictor strips.  At
    // theta=0.99, ~80% of draws hit ~20% of keys, so a cache sized to ~20% of
    // the dataset SHOULD hold the hot set and hit 70%+.
    let mut rng = 0x1234_5678_u64;
    let reads = 20_000usize;
    let theta = 0.99f64;
    let zetan = zeta(total_n, theta);
    let before = env.stats().unwrap().log.n_random_reads;
    for _ in 0..reads {
        // Zipfian rank -> key.  For the interleaved layout, rank IS the key
        // (hot ranks are contiguous, sharing BINs).  For dedicated, spread
        // the ranks so hot keys land in distinct BIN regions.
        let rank = zipf(&mut rng, total_n, zetan, theta);
        let key = if interleaved {
            rank
        } else {
            (rank.wrapping_mul(40_009)) % total_n // spread; 40009 is prime
        };
        read(key);
    }
    let after = env.stats().unwrap().log.n_random_reads;
    stop.store(true, Ordering::Relaxed);
    evictor.join().unwrap();
    let st = env.stats().unwrap();
    eprintln!(
        "  [interleaved={}] nodes_targeted={} nodes_stripped={} lns_evicted={} cache_usage={} max_mem~{}",
        interleaved,
        st.evictor.nodes_targeted,
        st.evictor.nodes_stripped,
        st.evictor.lns_evicted,
        st.cache_usage,
        4 * 1024 * 1024,
    );
    let _ = 1024 * 1024; // cache size annotation only
    let faults = after - before;
    let rate = faults as f64 / reads as f64;
    (rate, faults, reads as u64)
}

#[test]
#[ignore = "~8-min full-DB Zipfian hit-rate repro; run explicitly, not a CI gate"]
fn zipfian_hitrate_dedicated_vs_interleaved() {
    let dir_a = TempDir::new().unwrap();
    let (rate_a, faults_a, reads_a) = measure_fault_rate(dir_a.path(), false);
    let dir_b = TempDir::new().unwrap();
    let (rate_b, faults_b, reads_b) = measure_fault_rate(dir_b.path(), true);

    eprintln!(
        "DEDICATED  : fault_rate={:.4} hit_rate={:.4} faults={}/{}",
        rate_a,
        1.0 - rate_a,
        faults_a,
        reads_a
    );
    eprintln!(
        "INTERLEAVED: fault_rate={:.4} hit_rate={:.4} faults={}/{}",
        rate_b,
        1.0 - rate_b,
        faults_b,
        reads_b
    );
    // Not an assertion — this is the measurement harness.  The interesting
    // number is INTERLEAVED's hit rate vs DEDICATED's.
}
