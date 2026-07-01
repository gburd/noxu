//! Cache-eviction policy benchmark under cache pressure.
//!
//! Compares the 5 eviction policies (lru, clock, arc, car, lirs) on three
//! workloads where the working set far exceeds the cache, so eviction fires
//! and the policy's victim choice actually matters:
//!
//!   * `random` — uniform-random point reads over the full keyset.
//!   * `scan` — repeated sequential full-keyset cursor scans (where
//!     scan-resistant policies should beat plain LRU).
//!   * `mixed` — 70% random read / 30% random write.
//!
//! Cache is pinned to 16 MiB; dataset is 80k records x 256 B (~21 MB tree),
//! so eviction is guaranteed. Each (policy x workload) is run 3x; the median
//! ops/s is reported. Must be run on REAL storage (e.g. /scratch), not tmpfs.
//! Durability is COMMIT_NO_SYNC: the policy decides which node to evict, not
//! how durable a commit is — dropping the per-commit fsync exposes the policy
//! signal instead of burying it under fsync latency (writes still hit the log
//! on real disk; evicted nodes are still re-read from disk).
//!
//! Usage:
//!   NOXU_BENCH_DIR=/scratch/evpolicy cargo run --release \
//!       -p noxu-workload-bench --bin evictor_policy_bench

use noxu_db::{
    Database, DatabaseConfig, DatabaseEntry, Durability, Environment,
    EnvironmentConfig, Get, OperationStatus,
};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use std::path::{Path, PathBuf};
use std::time::Instant;

const CACHE_BYTES: u64 = 16 * 1024 * 1024; // 16 MiB — forces pressure.
const N_RECORDS: usize = 80_000; // x 256 B value ~= 21 MB working set >> 16 MiB.
const VALUE_LEN: usize = 256;
const REPEATS: usize = 3;
const OPS_RANDOM: usize = 100_000;
const OPS_MIXED: usize = 50_000;
const SCAN_PASSES: usize = 3;

const POLICIES: [&str; 5] = ["lru", "clock", "arc", "car", "lirs"];
const WORKLOADS: [&str; 3] = ["random", "scan", "mixed"];

fn key(i: usize) -> DatabaseEntry {
    DatabaseEntry::from_vec(format!("{:010}", i).into_bytes())
}

fn open(dir: &Path, algo: &str) -> (Environment, Database) {
    let mut cfg = EnvironmentConfig::new(dir.to_path_buf());
    cfg.set_allow_create(true);
    cfg.set_transactional(true);
    cfg.set_cache_percent(0); // so set_cache_size takes effect
    cfg.set_cache_size(CACHE_BYTES);
    cfg.set_evictor_algorithm(algo);
    // Durability is irrelevant to which node a policy evicts; commit_no_sync
    // removes the per-commit fsync that would otherwise dominate the timing
    // and bury the policy signal. Writes still hit the log on real disk and
    // evicted nodes are still re-read from disk.
    cfg.set_durability(Durability::COMMIT_NO_SYNC);
    let env = Environment::open(cfg).expect("open env");
    // Verify the wiring actually took effect at runtime.
    let runtime = env.evictor_algorithm_name().expect("algo name");
    assert!(
        runtime.eq_ignore_ascii_case(algo),
        "EVICTOR_ALGORITHM wiring failed: requested {algo:?}, runtime {runtime:?}"
    );
    let db = env
        .open_database(
            None,
            "bench",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .expect("open db");
    (env, db)
}

fn populate(db: &Database) {
    let val = vec![0xABu8; VALUE_LEN];
    for i in 0..N_RECORDS {
        db.put(key(i), DatabaseEntry::from_bytes(&val)).unwrap();
    }
}

/// Uniform-random point reads over the full keyset.
fn run_random(db: &Database) -> f64 {
    let mut rng = SmallRng::seed_from_u64(0x5EED);
    let mut out = DatabaseEntry::new();
    let start = Instant::now();
    for _ in 0..OPS_RANDOM {
        let i = rng.gen_range(0..N_RECORDS);
        let st = db.get_into(None, key(i), &mut out).unwrap();
        debug_assert!(st);
    }
    OPS_RANDOM as f64 / start.elapsed().as_secs_f64()
}

/// Repeated sequential full-keyset cursor scans (scan-resistance test).
fn run_scan(db: &Database) -> f64 {
    let start = Instant::now();
    let mut ops = 0usize;
    for _ in 0..SCAN_PASSES {
        let mut cursor = db.open_cursor(None).unwrap();
        // Position at the first key via SearchGte (Get::First is unreliable on
        // this build for a full forward scan; SearchGte from key 0 is the
        // pattern the existing w05_range_scan workload uses).
        let mut k = key(0);
        let mut v = DatabaseEntry::new();
        let mut st = cursor.get(&mut k, &mut v, Get::SearchGte, None).unwrap();
        while st == OperationStatus::Success {
            ops += 1;
            st = cursor.get(&mut k, &mut v, Get::Next, None).unwrap();
        }
        cursor.close().unwrap();
    }
    ops as f64 / start.elapsed().as_secs_f64()
}

/// 70% random read / 30% random write.
fn run_mixed(db: &Database) -> f64 {
    let mut rng = SmallRng::seed_from_u64(0xC0FFEE);
    let val = vec![0xCDu8; VALUE_LEN];
    let mut out = DatabaseEntry::new();
    let start = Instant::now();
    for _ in 0..OPS_MIXED {
        let i = rng.gen_range(0..N_RECORDS);
        if rng.gen_bool(0.30) {
            db.put(key(i), DatabaseEntry::from_bytes(&val)).unwrap();
        } else {
            db.get_into(None, key(i), &mut out).unwrap();
        }
    }
    OPS_MIXED as f64 / start.elapsed().as_secs_f64()
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

fn main() {
    let base = std::env::var("NOXU_BENCH_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            panic!(
                "set NOXU_BENCH_DIR to a directory on REAL storage (e.g. /scratch/evpolicy)"
            )
        });
    std::fs::create_dir_all(&base).expect("create bench base dir");
    eprintln!(
        "evictor_policy_bench: cache={} MiB, records={}, value={} B, repeats={}, dir={}",
        CACHE_BYTES / (1024 * 1024),
        N_RECORDS,
        VALUE_LEN,
        REPEATS,
        base.display()
    );

    // results[policy][workload] = median ops/s
    let mut results = vec![vec![0.0f64; WORKLOADS.len()]; POLICIES.len()];

    for (pi, &policy) in POLICIES.iter().enumerate() {
        for (wi, &workload) in WORKLOADS.iter().enumerate() {
            let mut samples = Vec::with_capacity(REPEATS);
            for rep in 0..REPEATS {
                // Fresh env per repeat so the cache starts cold and the
                // populate cost is identical across policies.
                let dir = base.join(format!("{policy}-{workload}-{rep}"));
                let _ = std::fs::remove_dir_all(&dir);
                std::fs::create_dir_all(&dir).unwrap();
                let (env, db) = open(&dir, policy);
                populate(&db);
                // Checkpoint so dirty BINs/LNs are logged and become
                // strippable, then force an eviction pass before measuring.
                env.checkpoint(None).unwrap();
                let _ = env.evict_memory().unwrap();
                // One-shot sanity check on the first cell: does eviction
                // actually reclaim memory under pressure? If usage stays near
                // the full working set and nodes_evicted stays ~0, the policy
                // choice is INERT end-to-end (every victim is put back) and the
                // ops/s differences below are noise, not policy behaviour.
                // We WARN rather than panic so the table is still produced and
                // the no-op is documented in the run log.
                if pi == 0 && wi == 0 && rep == 0 {
                    let usage = env.cache_usage_bytes().unwrap();
                    let ev = env.stats().unwrap().evictor.nodes_evicted;
                    let working_set = (N_RECORDS * (VALUE_LEN + 16)) as i64;
                    eprintln!(
                        "  [sanity] cache_usage={usage} bytes, nodes_evicted={ev} (cache budget {CACHE_BYTES} bytes, working set ~{working_set} bytes)"
                    );
                    if !(usage < working_set && ev > 0) {
                        eprintln!(
                            "  [WARNING] eviction is NOT reclaiming memory (usage {usage} ~= working set {working_set}, nodes_evicted {ev}). The eviction POLICY is inert end-to-end here: victims are selected but put back, so the ops/s table below does NOT measure policy behaviour. See CHANGELOG / report."
                        );
                    }
                }
                let ops = match workload {
                    "random" => run_random(&db),
                    "scan" => run_scan(&db),
                    "mixed" => run_mixed(&db),
                    _ => unreachable!(),
                };
                samples.push(ops);
                drop(db);
                drop(env);
                let _ = std::fs::remove_dir_all(&dir);
                eprintln!(
                    "  {policy:>5} / {workload:<6} rep {rep}: {ops:>12.0} ops/s"
                );
            }
            results[pi][wi] = median(samples);
        }
    }

    // Markdown table.
    println!(
        "\n## Eviction policy ops/s (median of {REPEATS}, 16 MiB cache, {N_RECORDS} x {VALUE_LEN}B)\n"
    );
    print!("| policy |");
    for w in WORKLOADS {
        print!(" {w} |");
    }
    println!();
    print!("|---|");
    for _ in WORKLOADS {
        print!("---:|");
    }
    println!();
    for (pi, &policy) in POLICIES.iter().enumerate() {
        print!("| {policy} |");
        for cell in &results[pi] {
            print!(" {cell:.0} |");
        }
        println!();
    }

    // Per-workload winner + LRU-relative geomean.
    println!("\n### vs LRU (ratio, >1 = faster than LRU)\n");
    let lru_row = &results[0]; // POLICIES[0] == "lru"
    print!("| policy |");
    for w in WORKLOADS {
        print!(" {w} |");
    }
    println!(" geomean |");
    print!("|---|");
    for _ in WORKLOADS {
        print!("---:|");
    }
    println!("---:|");
    for (pi, &policy) in POLICIES.iter().enumerate() {
        print!("| {policy} |");
        let mut logsum = 0.0f64;
        for (cell, lru) in results[pi].iter().zip(lru_row.iter()) {
            let ratio = cell / lru;
            logsum += ratio.ln();
            print!(" {ratio:.3} |");
        }
        let geo = (logsum / WORKLOADS.len() as f64).exp();
        println!(" {geo:.3} |");
    }
}
