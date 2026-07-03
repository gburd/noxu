//! Write-scaling sweep — measure write throughput as concurrent writers ramp
//! 1, 5, 10, 15, ... up to a max, each for a fixed window, against a warm
//! dataset already larger than cache. Answers: does write throughput scale
//! with writers, or is it fsync-serialized (the ~10 GB/hr ceiling)?
//!
//! Phase 1: load NOXU_SAT_RECORDS records (builds the > cache dataset).
//! Phase 2: ramp writers 1..=MAX step STEP, each running SECS seconds; report
//!          aggregate writes/s at each writer count. Writers do random-key
//!          auto-commit puts across the FULL keyspace (so cache is exercised).
//!
//! Env: NOXU_BENCH_DIR NOXU_BENCH_CACHE_SIZE NOXU_BENCH_VALUE_SIZE
//!      NOXU_SAT_RECORDS  NOXU_SWEEP_MAX(=96) NOXU_SWEEP_STEP(=5) NOXU_SWEEP_SECS(=60)

use noxu_db::{Database, DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

fn envn(k: &str, d: usize) -> usize {
    std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(d)
}
fn key_bytes(i: u64) -> Vec<u8> { format!("{:016}", i).into_bytes() }

fn main() {
    let dir = std::env::var("NOXU_BENCH_DIR").expect("NOXU_BENCH_DIR");
    let fstype = std::process::Command::new("df").args(["-T", &dir]).output().ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .and_then(|s| s.lines().nth(1).map(|l| l.split_whitespace().nth(1).unwrap_or("").to_string()))
        .unwrap_or_default();
    assert!(fstype != "tmpfs", "REFUSING: {dir} is tmpfs");

    let cache: u64 = std::env::var("NOXU_BENCH_CACHE_SIZE").ok().and_then(|v| v.parse().ok()).expect("cache");
    let value_size = envn("NOXU_BENCH_VALUE_SIZE", 1024);
    let records = envn("NOXU_SAT_RECORDS", 40_000_000) as u64;
    let max_w = envn("NOXU_SWEEP_MAX", 96);
    let step = envn("NOXU_SWEEP_STEP", 5);
    let secs = envn("NOXU_SWEEP_SECS", 60) as u64;

    println!("=== Noxu write-scaling sweep ===");
    println!("  dir={dir} (fs {fstype}) cache={}GiB records={records} val={value_size}B",
             cache / 1024 / 1024 / 1024);
    println!("  sweep: 1,{step},{}.. up to {max_w} writers x {secs}s each", step * 2);

    let env = Arc::new(Environment::open(
        EnvironmentConfig::new(std::path::PathBuf::from(&dir))
            .with_allow_create(true).with_transactional(true).with_cache_size(cache),
    ).expect("open"));
    let db = Arc::new(env.open_database(None, "sweep",
        &DatabaseConfig::new().with_allow_create(true).with_transactional(true)).expect("db"));

    // ── Load ──
    println!("\n-- loading {records} records --");
    let t0 = Instant::now();
    std::thread::scope(|s| {
        let lt = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(16);
        let per = records / lt as u64;
        for tid in 0..lt {
            let db = Arc::clone(&db);
            let start = tid as u64 * per;
            let end = if tid == lt - 1 { records } else { start + per };
            s.spawn(move || {
                let v = DatabaseEntry::from_bytes(&vec![0x58u8; value_size]);
                for i in start..end {
                    let _ = db.put(DatabaseEntry::from_vec(key_bytes(i)), &v);
                }
            });
        }
    });
    println!("   loaded in {:.0}s ({:.0} w/s)", t0.elapsed().as_secs_f64(),
             records as f64 / t0.elapsed().as_secs_f64());

    // ── Sweep ──
    println!("\n  writers   writes/s     per-writer-w/s   p50us   p99us");
    let mut counts: Vec<usize> = vec![1];
    let mut c = step;
    while c <= max_w { counts.push(c); c += step; }
    if *counts.last().unwrap() != max_w { counts.push(max_w); }

    for &nw in &counts {
        let (ops, elapsed, p50, p99) = write_phase(&db, nw, records, value_size, secs);
        let wps = ops as f64 / elapsed;
        println!("  {nw:>7}   {wps:>10.0}   {:>14.0}   {:>5} {:>7}",
                 wps / nw as f64, p50 / 1000, p99 / 1000);
    }
    println!("\n=== SWEEP DONE ===");
}

fn write_phase(db: &Arc<Database>, nw: usize, records: u64, value_size: usize, secs: u64)
    -> (u64, f64, u64, u64)
{
    let barrier = Arc::new(Barrier::new(nw + 1));
    let stop = Arc::new(AtomicBool::new(false));
    let total = Arc::new(AtomicU64::new(0));
    std::thread::scope(|s| {
        let mut hs = Vec::new();
        for tid in 0..nw {
            let db = Arc::clone(db);
            let barrier = Arc::clone(&barrier);
            let stop = Arc::clone(&stop);
            let total = Arc::clone(&total);
            hs.push(s.spawn(move || -> Vec<u64> {
                let mut rng = SmallRng::seed_from_u64(tid as u64 * 2_654_435_761 + 1);
                let v = DatabaseEntry::from_bytes(&vec![0x58u8; value_size]);
                let mut lats = Vec::with_capacity(1 << 14);
                let mut ops = 0u64;
                barrier.wait();
                while !stop.load(Ordering::Relaxed) {
                    let k = DatabaseEntry::from_vec(key_bytes(rng.gen_range(0..records)));
                    let t = Instant::now();
                    let _ = db.put(&k, &v);
                    if ops.is_multiple_of(16) { lats.push(t.elapsed().as_nanos() as u64); }
                    ops += 1;
                }
                total.fetch_add(ops, Ordering::Relaxed);
                lats
            }));
        }
        barrier.wait();
        let t0 = Instant::now();
        std::thread::sleep(Duration::from_secs(secs));
        stop.store(true, Ordering::Relaxed);
        let elapsed = t0.elapsed().as_secs_f64();
        let mut lats: Vec<u64> = Vec::new();
        for h in hs { lats.extend(h.join().unwrap_or_default()); }
        lats.sort_unstable();
        let p = |q: f64| if lats.is_empty() { 0 } else { lats[((lats.len() as f64 * q) as usize).min(lats.len()-1)] };
        (total.load(Ordering::Relaxed), elapsed, p(0.5), p(0.99))
    })
}
