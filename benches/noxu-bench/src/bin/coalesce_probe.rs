//! Coalescing probe — measure commits-per-fsync as writers ramp.
//!
//! The AWS sweep proved the NVMe is idle (~0% util) yet write throughput
//! plateaus at ~9,400 w/s vs JE's ~187,000 w/s — a group-commit coalescing
//! defect, not hardware. This probe measures the ACTUAL coalescing factor
//! (commits / fdatasync) at increasing writer counts, on a REAL filesystem,
//! so the batch factor JE achieves (~250/fsync) vs Noxu (~12/fsync) is visible
//! locally without EC2.
//!
//! Env: NOXU_CP_DIR (real fs, NOT tmpfs) NOXU_CP_MAX(=64) NOXU_CP_SECS(=3)

use noxu_db::{Database, DatabaseConfig, Environment, EnvironmentConfig};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

fn envn(k: &str, d: usize) -> usize {
    std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(d)
}
fn key_bytes(i: u64) -> Vec<u8> {
    format!("{:016}", i).into_bytes()
}

fn main() {
    let dir = std::env::var("NOXU_CP_DIR").unwrap_or_else(|_| {
        std::env::temp_dir()
            .join(format!("noxu-cp-{}", std::process::id()))
            .to_string_lossy()
            .into_owned()
    });
    let _ = std::fs::create_dir_all(&dir);
    let fstype = std::process::Command::new("df")
        .args(["-T", &dir])
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .and_then(|s| {
            s.lines()
                .nth(1)
                .map(|l| l.split_whitespace().nth(1).unwrap_or("").to_string())
        })
        .unwrap_or_default();
    eprintln!(
        "dir={dir} fs={fstype} (warn: tmpfs fsync is a no-op, use real disk for true factor)"
    );

    let max_w = envn("NOXU_CP_MAX", 64);
    let secs = envn("NOXU_CP_SECS", 3) as u64;
    let records: u64 = 200_000;
    let value_size = 256;

    let nosync = std::env::var("NOXU_CP_NOSYNC").is_ok();
    eprintln!(
        "durability={}",
        if nosync {
            "CommitNoSync (isolate per-put cost)"
        } else {
            "CommitSync (default)"
        }
    );
    let env = Arc::new(
        Environment::open({
            let mut c = EnvironmentConfig::new(std::path::PathBuf::from(&dir))
                .with_allow_create(true)
                .with_transactional(true)
                .with_cache_size(256 * 1024 * 1024);
            if nosync {
                c = c.with_durability(noxu_db::Durability::COMMIT_NO_SYNC);
            }
            if let Ok(t) = std::env::var("NOXU_CP_GRPC_T") {
                c = c.with_log_group_commit_threshold(t.parse().unwrap());
            }
            if let Ok(i) = std::env::var("NOXU_CP_GRPC_I") {
                c = c.with_log_group_commit_interval_ms(i.parse().unwrap());
            }
            c
        })
        .expect("open"),
    );
    let db = Arc::new(
        env.open_database(
            None,
            "cp",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .expect("db"),
    );

    // small load so keys exist
    let v = vec![0x58u8; value_size];
    for i in 0..records {
        let _ = db.put(key_bytes(i), &v);
    }

    println!(
        "\n writers   commits/s    fsyncs/s   commits/fsync   grpCommits   p50us   p99us"
    );
    let mut counts: Vec<usize> = vec![1, 2, 4, 8, 16, 32];
    if max_w > 32 {
        counts.push(max_w);
    }
    counts.retain(|&c| c <= max_w);

    for &nw in &counts {
        let s0 = env.stats().unwrap();
        let (ops, elapsed, p50, p99) =
            write_phase(&db, nw, records, value_size, secs);
        let s1 = env.stats().unwrap();
        let fsyncs = s1.log.n_log_fsyncs.saturating_sub(s0.log.n_log_fsyncs);
        let reqs =
            s1.log.n_fsync_requests.saturating_sub(s0.log.n_fsync_requests);
        let grpc =
            s1.log.n_group_commits.saturating_sub(s0.log.n_group_commits);
        let cps = ops as f64 / elapsed;
        let fps = fsyncs as f64 / elapsed;
        let factor = if fsyncs > 0 { reqs as f64 / fsyncs as f64 } else { 0.0 };
        println!(
            " {nw:>7}   {cps:>9.0}   {fps:>9.0}   {factor:>13.1}   {grpc:>10}   {:>5} {:>7}",
            p50 / 1000,
            p99 / 1000
        );
    }
    println!("\n=== COALESCE PROBE DONE ===");
    let _ = std::fs::remove_dir_all(&dir);
}

fn write_phase(
    db: &Arc<Database>,
    nw: usize,
    records: u64,
    value_size: usize,
    secs: u64,
) -> (u64, f64, u64, u64) {
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
                let mut rng =
                    SmallRng::seed_from_u64(tid as u64 * 2_654_435_761 + 1);
                let v = vec![0x58u8; value_size];
                let mut lats = Vec::with_capacity(1 << 14);
                let mut ops = 0u64;
                barrier.wait();
                while !stop.load(Ordering::Relaxed) {
                    let k = key_bytes(rng.gen_range(0..records));
                    let t = Instant::now();
                    let _ = db.put(&k, &v);
                    if ops.is_multiple_of(8) {
                        lats.push(t.elapsed().as_nanos() as u64);
                    }
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
        for h in hs {
            lats.extend(h.join().unwrap_or_default());
        }
        lats.sort_unstable();
        let p = |q: f64| {
            if lats.is_empty() {
                0
            } else {
                lats[((lats.len() as f64 * q) as usize).min(lats.len() - 1)]
            }
        };
        (total.load(Ordering::Relaxed), elapsed, p(0.5), p(0.99))
    })
}
