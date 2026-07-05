//! Comprehensive multi-profile benchmark for Noxu DB.
//!
//! Exercises a WIDE public-API surface across several named workload profiles,
//! then (with profile=all) runs every profile back to back. Designed for
//! apples-to-apples comparison against the matching JE harness (JeBench.java):
//! identical record counts, value sizes, durability, thread counts, and the
//! same operation mix per profile.
//!
//! Working set is sized by NOXU_BENCH_RECORDS * NOXU_BENCH_VALUE_SIZE; set it
//! to 2-4x the cache (NOXU_BENCH_CACHE_SIZE) so the run is dataset>cache
//! (real I/O), not an all-in-RAM microbench.
//!
//! Env knobs:
//!   NOXU_BENCH_DIR          data dir (MUST be real NVMe, not tmpfs)
//!   NOXU_BENCH_CACHE_SIZE   cache bytes (default 4 GiB)
//!   NOXU_BENCH_VALUE_SIZE   value bytes (default 200)
//!   NOXU_BENCH_RECORDS      dataset record count (default 20_000_000)
//!   NOXU_BENCH_THREADS      worker threads (default: all CPUs)
//!   NOXU_BENCH_SECONDS      measured seconds per profile (default 30)
//!   NOXU_BENCH_DURABILITY   SYNC | WRITE_NO_SYNC | NO_SYNC (default SYNC)
//!   NOXU_BENCH_PROFILE      readonly|read_heavy|balanced|write_heavy|
//!                           txn_multi|cursor_scan|insert_only|all (default all)

use noxu_db::{
    Database, DatabaseConfig, Durability, Environment, EnvironmentConfig,
    Put, TransactionConfig,
};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

fn envp(k: &str, d: u64) -> u64 {
    std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(d)
}
fn cpus() -> usize {
    std::thread::available_parallelism().map(|n| n.get()).unwrap_or(8)
}

fn key_bytes(i: u64) -> [u8; 16] {
    // 16-byte big-endian-ish key so ordered scans are meaningful.
    let mut k = [0u8; 16];
    k[..8].copy_from_slice(&i.to_be_bytes());
    k[8..].copy_from_slice(&(i.wrapping_mul(2654435761)).to_be_bytes());
    k
}

struct Ctx {
    env: Arc<Environment>,
    db: Arc<Database>,
    records: u64,
    value_size: usize,
    threads: usize,
    seconds: u64,
}

/// Result of one measured profile: total ops and elapsed wall time.
fn run_profile(ctx: &Ctx, name: &str) -> (u64, f64) {
    let stop = Arc::new(AtomicBool::new(false));
    let ops = Arc::new(AtomicU64::new(0));
    let start = Instant::now();

    let handles: Vec<_> = (0..ctx.threads)
        .map(|tid| {
            let env = Arc::clone(&ctx.env);
            let db = Arc::clone(&ctx.db);
            let stop = Arc::clone(&stop);
            let ops = Arc::clone(&ops);
            let records = ctx.records;
            let value_size = ctx.value_size;
            let profile = name.to_string();
            std::thread::spawn(move || {
                let mut rng = SmallRng::seed_from_u64(0x51ed ^ tid as u64);
                let value = vec![0x56u8; value_size];
                let mut local = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    for _ in 0..64 {
                        do_op(&profile, &env, &db, &mut rng, records, &value);
                        local += 1;
                    }
                }
                ops.fetch_add(local, Ordering::Relaxed);
            })
        })
        .collect();

    std::thread::sleep(std::time::Duration::from_secs(ctx.seconds));
    stop.store(true, Ordering::Relaxed);
    for h in handles {
        h.join().unwrap();
    }
    (ops.load(Ordering::Relaxed), start.elapsed().as_secs_f64())
}

#[inline]
fn do_op(
    profile: &str,
    env: &Environment,
    db: &Database,
    rng: &mut SmallRng,
    records: u64,
    value: &[u8],
) {
    match profile {
        // 100% point reads (non-txn).
        "readonly" => {
            let k = key_bytes(rng.gen_range(0..records));
            let _ = db.get(k);
        }
        // 95% reads, 5% blind writes (non-txn).
        "read_heavy" => {
            if rng.gen_range(0..100) < 5 {
                let k = key_bytes(rng.gen_range(0..records));
                let _ = db.put(k, value);
            } else {
                let k = key_bytes(rng.gen_range(0..records));
                let _ = db.get(k);
            }
        }
        // 50/50 read/write (non-txn).
        "balanced" => {
            let k = key_bytes(rng.gen_range(0..records));
            if rng.gen_bool(0.5) {
                let _ = db.get(k);
            } else {
                let _ = db.put(k, value);
            }
        }
        // 90% writes / 10% reads (non-txn) — auto-commit dominated.
        "write_heavy" => {
            if rng.gen_range(0..10) < 9 {
                let k = key_bytes(rng.gen_range(0..records));
                let _ = db.put(k, value);
            } else {
                let k = key_bytes(rng.gen_range(0..records));
                let _ = db.get(k);
            }
        }
        // Explicit multi-op transactions: 4 ops (mixed r/w + delete) per txn.
        "txn_multi" => {
            if let Ok(txn) = env.begin_transaction(None) {
                let mut ok = true;
                for j in 0..4 {
                    let k = key_bytes(rng.gen_range(0..records));
                    let r = match j {
                        0 | 2 => db.put_in(&txn, k, value).map(|_| ()),
                        1 => db.get_in(&txn, k).map(|_| ()),
                        _ => db.delete_in(&txn, k).map(|_| ()),
                    };
                    if r.is_err() {
                        ok = false;
                        break;
                    }
                }
                if ok {
                    let _ = txn.commit();
                } else {
                    let _ = txn.abort();
                }
            }
        }
        // Ordered cursor range scan: seek to a random key, read forward N.
        "cursor_scan" => {
            let start = rng.gen_range(0..records);
            if let Ok(mut cur) = db.open_cursor(None) {
                if cur.seek(key_bytes(start)).is_ok() {
                    for _ in 0..32 {
                        match cur.next() {
                            Ok(Some(_)) => {}
                            _ => break,
                        }
                    }
                }
            }
        }
        // Insert-only with put_no_overwrite (distinct fresh keys).
        "insert_only" => {
            // keys above the loaded range so they are always fresh inserts.
            let k = key_bytes(records + rng.gen_range(0..records));
            let _ = db.put_no_overwrite(k, value);
        }
        _ => {}
    }
}

fn load(ctx: &Ctx, load_threads: usize) {
    let per = ctx.records / load_threads as u64;
    let loaded = Arc::new(AtomicU64::new(0));
    // Bulk-load in batched transactions (1000 puts/commit) so the load is
    // fast even under COMMIT_SYNC (one fsync amortized over the batch),
    // rather than one fsync per record. This is a realistic bulk-load path
    // and keeps the load phase from dominating the run; the MEASURED profile
    // phases below still use the configured per-op durability. JeBench loads
    // the same way (batched txns).
    const BATCH: u64 = 1000;
    std::thread::scope(|s| {
        for tid in 0..load_threads {
            let env = Arc::clone(&ctx.env);
            let db = Arc::clone(&ctx.db);
            let loaded = Arc::clone(&loaded);
            let start = tid as u64 * per;
            let end = if tid == load_threads - 1 {
                ctx.records
            } else {
                start + per
            };
            let value_size = ctx.value_size;
            s.spawn(move || {
                let value = vec![0x56u8; value_size];
                let mut i = start;
                while i < end {
                    let batch_end = (i + BATCH).min(end);
                    if let Ok(txn) = env.begin_transaction(None) {
                        let mut ok = true;
                        for j in i..batch_end {
                            if db.put_in(&txn, key_bytes(j), &value).is_err() {
                                ok = false;
                                break;
                            }
                        }
                        if ok {
                            let _ = txn.commit();
                        } else {
                            let _ = txn.abort();
                        }
                    }
                    if i % 10_000_000 < BATCH {
                        loaded.fetch_add(10_000_000, Ordering::Relaxed);
                    }
                    i = batch_end;
                }
            });
        }
    });
    let _ = loaded;
}

fn fstype(dir: &str) -> String {
    std::process::Command::new("df")
        .arg("-T")
        .arg(dir)
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .and_then(|s| s.lines().nth(1).map(|l| l.to_string()))
        .unwrap_or_default()
}

fn main() {
    let dir =
        std::env::var("NOXU_BENCH_DIR").unwrap_or_else(|_| "/tmp/noxu-cmp".into());
    let cache = envp("NOXU_BENCH_CACHE_SIZE", 4 * 1024 * 1024 * 1024);
    let value_size = envp("NOXU_BENCH_VALUE_SIZE", 200) as usize;
    let records = envp("NOXU_BENCH_RECORDS", 20_000_000);
    let threads = envp("NOXU_BENCH_THREADS", cpus() as u64) as usize;
    let seconds = envp("NOXU_BENCH_SECONDS", 30);
    // Loader-thread count: bounded (default 8) because auto-commit writes
    // past ~8 concurrent writers contend heavily on the lock/tree/log path
    // (documented limitation). JeBench uses the same load-thread count for
    // a fair comparison.
    let load_threads =
        envp("NOXU_BENCH_LOAD_THREADS", 8).max(1) as usize;
    let durability =
        std::env::var("NOXU_BENCH_DURABILITY").unwrap_or_else(|_| "SYNC".into());
    let profile = std::env::var("NOXU_BENCH_PROFILE")
        .unwrap_or_else(|_| "all".into());

    // Hard guard: never benchmark on tmpfs (RAM) — it would report fantasy I/O.
    let fst = fstype(&dir);
    if fst.contains("tmpfs") {
        eprintln!("ABORT: {dir} is tmpfs (RAM-backed); use real NVMe. df: {fst}");
        std::process::exit(2);
    }

    let _ = std::fs::create_dir_all(&dir);
    let dur = match durability.as_str() {
        "NO_SYNC" => Durability::COMMIT_NO_SYNC,
        "WRITE_NO_SYNC" => Durability::COMMIT_WRITE_NO_SYNC,
        _ => Durability::COMMIT_SYNC,
    };

    let approx = records * (value_size as u64 + 40);
    println!("=== Noxu comprehensive benchmark ===");
    println!("  dir:        {dir}  (fs: {})", fst.split_whitespace().nth(1).unwrap_or("?"));
    println!("  cache:      {} GiB", cache / 1024 / 1024 / 1024);
    println!("  dataset:    {records} x {value_size}B ~= {} GiB (ratio {:.1}x cache)", approx / 1024 / 1024 / 1024, approx as f64 / cache as f64);
    println!("  threads:    {threads} (load: {load_threads})");
    println!("  seconds:    {seconds} per profile");
    println!("  durability: {durability}");
    println!("  profile:    {profile}");

    let env = Arc::new(
        Environment::open(
            EnvironmentConfig::new(std::path::PathBuf::from(&dir))
                .with_allow_create(true)
                .with_transactional(true)
                .with_cache_size(cache)
                .with_durability(dur),
        )
        .expect("open env"),
    );
    let db = Arc::new(
        env.open_database(
            None,
            "bench",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .expect("open db"),
    );

    let ctx = Ctx {
        env: Arc::clone(&env),
        db: Arc::clone(&db),
        records,
        value_size,
        threads,
        seconds,
    };

    println!("\n-- loading {records} records --");
    let lt = Instant::now();
    load(&ctx, load_threads);
    env.checkpoint(None).unwrap();
    println!("   loaded in {:.1}s", lt.elapsed().as_secs_f64());

    let all = [
        "readonly",
        "read_heavy",
        "balanced",
        "write_heavy",
        "txn_multi",
        "cursor_scan",
        "insert_only",
    ];
    let to_run: Vec<&str> =
        if profile == "all" { all.to_vec() } else { vec![profile.as_str()] };

    println!("\n{:<14} {:>14} {:>12}", "profile", "ops/s", "ops");
    for p in &to_run {
        let (ops, elapsed) = run_profile(&ctx, p);
        println!("{:<14} {:>14.0} {:>12}", p, ops as f64 / elapsed, ops);
    }

    // Combined "all-at-once": every profile's op mix, threads split across them.
    if profile == "all" {
        println!("\n-- all-profiles-at-once (mixed workload) --");
        let stop = Arc::new(AtomicBool::new(false));
        let ops = Arc::new(AtomicU64::new(0));
        let start = Instant::now();
        let handles: Vec<_> = (0..threads)
            .map(|tid| {
                let env = Arc::clone(&env);
                let db = Arc::clone(&db);
                let stop = Arc::clone(&stop);
                let ops = Arc::clone(&ops);
                let assigned = all[tid % all.len()].to_string();
                std::thread::spawn(move || {
                    let mut rng = SmallRng::seed_from_u64(0xa11 ^ tid as u64);
                    let value = vec![0x56u8; value_size];
                    let mut local = 0u64;
                    while !stop.load(Ordering::Relaxed) {
                        for _ in 0..64 {
                            do_op(&assigned, &env, &db, &mut rng, records, &value);
                            local += 1;
                        }
                    }
                    ops.fetch_add(local, Ordering::Relaxed);
                })
            })
            .collect();
        std::thread::sleep(std::time::Duration::from_secs(seconds));
        stop.store(true, Ordering::Relaxed);
        for h in handles {
            h.join().unwrap();
        }
        let el = start.elapsed().as_secs_f64();
        let total = ops.load(Ordering::Relaxed);
        println!("{:<14} {:>14.0} {:>12}", "ALL_MIXED", total as f64 / el, total);
    }

    // Silence unused-import warnings when only some paths are exercised.
    let _ = (Put::Overwrite, TransactionConfig::new());
    db.close().unwrap();
    // `env` is still shared via `ctx`; drop that ref, then close through the
    // remaining handle. (Env close is idempotent-safe on the last handle.)
    drop(ctx);
    if let Ok(e) = Arc::try_unwrap(env) {
        e.close().unwrap();
    }
}
