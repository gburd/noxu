//! Saturation benchmark — push Noxu to the limits of the host hardware.
//!
//! Unlike the w01..w13 suite (fixed small scales, ≤16 threads), this driver:
//!   * builds a dataset ~2x RAM (from NOXU_SAT_RECORDS + NOXU_BENCH_VALUE_SIZE),
//!   * gives 90%+ of RAM to the Noxu cache (NOXU_BENCH_CACHE_SIZE),
//!   * drives NOXU_SAT_THREADS concurrent threads (default = num_cpus) for a
//!     fixed wall-clock duration (NOXU_SAT_SECONDS) at several read/write mixes,
//!   * reports aggregate ops/s + p50/p99 latency so we can see scalability and
//!     I/O saturation under heavy multi-core concurrent load.
//!
//! Env knobs:
//!   NOXU_BENCH_DIR         real-NVMe data dir (REQUIRED; refuses tmpfs)
//!   NOXU_BENCH_CACHE_SIZE  cache bytes (e.g. 90% of RAM)
//!   NOXU_BENCH_VALUE_SIZE  value bytes (default 1024)
//!   NOXU_SAT_RECORDS       records to preload (dataset = records * (value+~40))
//!   NOXU_SAT_THREADS       concurrent worker threads (default: all CPUs)
//!   NOXU_SAT_SECONDS       measured seconds per phase (default 60)
//!   NOXU_SAT_LOAD_THREADS  parallel loader threads (default: all CPUs)

use noxu_db::{Database, DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

fn env_usize(k: &str, d: usize) -> usize {
    std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(d)
}

fn key_bytes(i: u64) -> Vec<u8> {
    format!("{:016}", i).into_bytes()
}

fn main() {
    let dir = std::env::var("NOXU_BENCH_DIR")
        .expect("NOXU_BENCH_DIR required (real NVMe path)");
    // Hard guard: never benchmark on tmpfs.
    let fstype = std::process::Command::new("df")
        .args(["-T", &dir])
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .and_then(|s| s.lines().nth(1).map(|l| l.to_string()))
        .unwrap_or_default();
    assert!(
        !fstype.contains("tmpfs"),
        "REFUSING: {dir} is tmpfs ({fstype}) — benchmark must run on real storage"
    );

    let cache: u64 = std::env::var("NOXU_BENCH_CACHE_SIZE")
        .ok()
        .and_then(|v| v.parse().ok())
        .expect("NOXU_BENCH_CACHE_SIZE required");
    let value_size = env_usize("NOXU_BENCH_VALUE_SIZE", 1024);
    let records = env_usize("NOXU_SAT_RECORDS", 100_000_000) as u64;
    let threads = env_usize("NOXU_SAT_THREADS", num_cpus());
    let seconds = env_usize("NOXU_SAT_SECONDS", 60) as u64;
    let load_threads = env_usize("NOXU_SAT_LOAD_THREADS", num_cpus());

    let approx_bytes = records * (value_size as u64 + 40);
    println!("=== Noxu saturation benchmark ===");
    println!("  data dir:   {dir}  (fs: {})", fstype.split_whitespace().nth(1).unwrap_or("?"));
    println!("  cache:      {} GiB", cache / 1024 / 1024 / 1024);
    println!("  records:    {records}  x {value_size}B  ~= {} GiB dataset",
             approx_bytes / 1024 / 1024 / 1024);
    println!("  threads:    {threads} workers, {load_threads} loaders");
    println!("  seconds:    {seconds} per phase");

    let env = Arc::new(
        Environment::open(
            EnvironmentConfig::new(std::path::PathBuf::from(&dir))
                .with_allow_create(true)
                .with_transactional(true)
                .with_cache_size(cache),
        )
        .expect("open env"),
    );
    let db = Arc::new(
        env.open_database(
            None,
            "sat",
            &DatabaseConfig::new().with_allow_create(true).with_transactional(true),
        )
        .expect("open db"),
    );

    // ── Parallel load ─────────────────────────────────────────────────────
    println!("\n-- loading {records} records on {load_threads} threads --");
    let t0 = Instant::now();
    let loaded = Arc::new(AtomicU64::new(0));
    std::thread::scope(|s| {
        let per = records / load_threads as u64;
        for tid in 0..load_threads {
            let db = Arc::clone(&db);
            let loaded = Arc::clone(&loaded);
            let start = tid as u64 * per;
            let end = if tid == load_threads - 1 { records } else { start + per };
            s.spawn(move || {
                let value = vec![0x58u8; value_size];
                let v = DatabaseEntry::from_bytes(&value);
                let mut n = 0u64;
                for i in start..end {
                    let k = DatabaseEntry::from_vec(key_bytes(i));
                    let _ = db.put(&k, &v);
                    n += 1;
                    if n.is_multiple_of(1_000_000) {
                        loaded.fetch_add(1_000_000, Ordering::Relaxed);
                    }
                }
            });
        }
    });
    let load_secs = t0.elapsed().as_secs_f64();
    println!(
        "   loaded in {:.0}s = {:.0} writes/s",
        load_secs,
        records as f64 / load_secs
    );

    // ── Concurrent phases ─────────────────────────────────────────────────
    // (label, read_frac 0..100). At each mix, run `threads` workers for
    // `seconds`, each doing random point ops across the FULL keyspace (so the
    // working set >> cache and eviction/re-fetch is exercised).
    for (label, read_pct) in
        [("100r", 100u32), ("95r5w", 95), ("50r50w", 50), ("0r100w", 0)]
    {
        run_phase(&env, &db, label, read_pct, threads, records, value_size, seconds);
    }

    // ── Transactional phase (multi-op txns under concurrency) ──────────────
    run_txn_phase(&env, &db, threads, records, value_size, seconds);

    println!("\n=== SATURATION DONE ===");
}

#[allow(clippy::too_many_arguments)]
fn run_phase(
    env: &Arc<Environment>,
    db: &Arc<Database>,
    label: &str,
    read_pct: u32,
    threads: usize,
    records: u64,
    value_size: usize,
    seconds: u64,
) {
    let _ = env;
    let barrier = Arc::new(Barrier::new(threads + 1));
    let stop = Arc::new(AtomicBool::new(false));
    let total = Arc::new(AtomicU64::new(0));

    std::thread::scope(|s| {
        let mut lat_handles = Vec::new();
        for tid in 0..threads {
            let db = Arc::clone(db);
            let barrier = Arc::clone(&barrier);
            let stop = Arc::clone(&stop);
            let total = Arc::clone(&total);
            let h = s.spawn(move || -> Vec<u64> {
                let mut rng = SmallRng::seed_from_u64(tid as u64 * 2_654_435_761 + 1);
                let value = vec![0x58u8; value_size];
                let v = DatabaseEntry::from_bytes(&value);
                let mut data = DatabaseEntry::new();
                let mut lats: Vec<u64> = Vec::with_capacity(1 << 16);
                let mut ops = 0u64;
                barrier.wait();
                while !stop.load(Ordering::Relaxed) {
                    let idx = rng.gen_range(0..records);
                    let k = DatabaseEntry::from_vec(key_bytes(idx));
                    let op_t = Instant::now();
                    if rng.gen_range(0..100) < read_pct {
                        let _ = db.get_into(None, &k, &mut data);
                    } else {
                        let _ = db.put(&k, &v);
                    }
                    // Sample latency every 64 ops to keep the vec bounded.
                    if ops.is_multiple_of(64) {
                        lats.push(op_t.elapsed().as_nanos() as u64);
                    }
                    ops += 1;
                }
                total.fetch_add(ops, Ordering::Relaxed);
                lats
            });
            lat_handles.push(h);
        }
        // Controller: release the barrier, sleep, then signal stop.
        barrier.wait();
        let t0 = Instant::now();
        std::thread::sleep(Duration::from_secs(seconds));
        stop.store(true, Ordering::Relaxed);
        let elapsed = t0.elapsed().as_secs_f64();

        let mut all_lats: Vec<u64> = Vec::new();
        for h in lat_handles {
            all_lats.extend(h.join().unwrap_or_default());
        }
        let ops = total.load(Ordering::Relaxed);
        all_lats.sort_unstable();
        let p = |q: f64| -> u64 {
            if all_lats.is_empty() { 0 } else {
                all_lats[((all_lats.len() as f64 * q) as usize).min(all_lats.len() - 1)]
            }
        };
        println!(
            "  {label:>8} {threads:>3}t  {:>12.0} ops/s   p50={:>6}us p99={:>7}us  ({ops} ops / {elapsed:.0}s)",
            ops as f64 / elapsed,
            p(0.50) / 1000,
            p(0.99) / 1000,
        );
    });
}

fn run_txn_phase(
    env: &Arc<Environment>,
    db: &Arc<Database>,
    threads: usize,
    records: u64,
    value_size: usize,
    seconds: u64,
) {
    let barrier = Arc::new(Barrier::new(threads + 1));
    let stop = Arc::new(AtomicBool::new(false));
    let total = Arc::new(AtomicU64::new(0));

    std::thread::scope(|s| {
        for tid in 0..threads {
            let env = Arc::clone(env);
            let db = Arc::clone(db);
            let barrier = Arc::clone(&barrier);
            let stop = Arc::clone(&stop);
            let total = Arc::clone(&total);
            s.spawn(move || {
                let mut rng = SmallRng::seed_from_u64(tid as u64 * 40_503 + 3);
                let value = vec![0x58u8; value_size];
                let v = DatabaseEntry::from_bytes(&value);
                let mut ops = 0u64;
                barrier.wait();
                while !stop.load(Ordering::Relaxed) {
                    // A multi-op transaction: 4 puts across the keyspace.
                    if let Ok(txn) = env.begin_transaction(None) {
                        for _ in 0..4 {
                            let k = DatabaseEntry::from_vec(key_bytes(rng.gen_range(0..records)));
                            let _ = db.put_in(&txn, &k, &v);
                        }
                        let _ = txn.commit();
                        ops += 4;
                    }
                }
                total.fetch_add(ops, Ordering::Relaxed);
            });
        }
        barrier.wait();
        let t0 = Instant::now();
        std::thread::sleep(Duration::from_secs(seconds));
        stop.store(true, Ordering::Relaxed);
        let elapsed = t0.elapsed().as_secs_f64();
        let ops = total.load(Ordering::Relaxed);
        println!(
            "  txn4op {threads:>3}t  {:>12.0} put-ops/s   ({ops} ops / {elapsed:.0}s)",
            ops as f64 / elapsed
        );
    });
}

fn num_cpus() -> usize {
    std::thread::available_parallelism().map(|n| n.get()).unwrap_or(8)
}
