//! Sustained 98/2 write/read benchmark with latency percentiles.
//!
//! Models a write-dominated ingest workload: 98% inserts / 2% reads, single
//! row per operation, primary key drawn from a monotonic sequence, value is a
//! JSON document of uniform-random size in [256, 2048] bytes. Runs for a fixed
//! wall-clock duration (default 30 min) and reports throughput plus a latency
//! histogram (p50/p90/p99/p99.9/max) sampled per operation, in 60s windows, so
//! p99 flatness can be seen while the cleaner works in the background.
//!
//! Env knobs:
//!   SW_DIR         data dir (real NVMe)
//!   SW_CACHE       cache bytes (default 8 GiB)
//!   SW_THREADS     writer threads (default 8 — past ~8 writers contend)
//!   SW_SECONDS     total run seconds (default 1800 = 30 min)
//!   SW_ENGINE      noxu | je-note (this binary is noxu; JE has JeSustained.java)
//!   SW_DURABILITY  SYNC | WRITE_NO_SYNC | NO_SYNC (default SYNC)

use noxu_db::{
    DatabaseConfig, Durability, Environment, EnvironmentConfig,
};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

fn envp(k: &str, d: u64) -> u64 {
    std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(d)
}

/// Build a JSON document of the given target size (padded to hit it exactly-ish).
fn json_value(id: u64, size: usize, rng: &mut SmallRng) -> Vec<u8> {
    // A realistic-ish JSON doc; pad the "note" field to reach `size`.
    let head = format!(
        "{{\"id\":{id},\"ts\":{},\"seq\":{},\"active\":true,\"score\":{},\"note\":\"",
        1_700_000_000u64 + rng.gen_range(0..1_000_000),
        rng.gen_range(0..u32::MAX),
        rng.gen_range(0..1000)
    );
    let tail = "\"}";
    let mut v = Vec::with_capacity(size);
    v.extend_from_slice(head.as_bytes());
    let pad = size.saturating_sub(head.len() + tail.len());
    // printable filler
    for i in 0..pad {
        v.push(b'a' + ((i % 26) as u8));
    }
    v.extend_from_slice(tail.as_bytes());
    v
}

/// Simple fixed-bucket latency histogram (microsecond buckets, log-ish).
struct Hist {
    buckets: Vec<AtomicU64>,
    max_us: AtomicU64,
}
impl Hist {
    fn new() -> Self {
        // 1us-granularity buckets up to ~65ms (65536 buckets, ~512KB of
        // atomics). Anything above 65ms lands in the top bucket but max_us
        // still tracks the true max. This gives exact p50/p99/p99.9 in the
        // sub-65ms range where a healthy commit latency lives.
        Hist { buckets: (0..65536).map(|_| AtomicU64::new(0)).collect(), max_us: AtomicU64::new(0) }
    }
    #[inline]
    fn idx(us: u64) -> usize {
        (us as usize).min(65535)
    }
    #[inline]
    fn record(&self, us: u64) {
        self.buckets[Self::idx(us)].fetch_add(1, Ordering::Relaxed);
        // track max
        let mut cur = self.max_us.load(Ordering::Relaxed);
        while us > cur {
            match self.max_us.compare_exchange_weak(cur, us, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => break,
                Err(c) => cur = c,
            }
        }
    }
    fn total(&self) -> u64 {
        self.buckets.iter().map(|b| b.load(Ordering::Relaxed)).sum()
    }
    /// Approximate percentile latency in microseconds.
    fn pct(&self, p: f64) -> u64 {
        let total = self.total();
        if total == 0 {
            return 0;
        }
        let target = (total as f64 * p) as u64;
        let mut cum = 0u64;
        for (i, b) in self.buckets.iter().enumerate() {
            cum += b.load(Ordering::Relaxed);
            if cum >= target {
                // buckets are 1us-linear: index == microseconds. The top
                // bucket (65535) means ">= 65535us"; report max in that case.
                return if i >= 65535 {
                    self.max_us.load(Ordering::Relaxed)
                } else {
                    i as u64
                };
            }
        }
        self.max_us.load(Ordering::Relaxed)
    }
}

fn fstype(dir: &str) -> String {
    std::process::Command::new("df").arg("-T").arg(dir).output().ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .and_then(|s| s.lines().nth(1).map(|l| l.to_string())).unwrap_or_default()
}

fn main() {
    let dir = std::env::var("SW_DIR").unwrap_or_else(|_| "/tmp/noxu-sw".into());
    let cache = envp("SW_CACHE", 8 * 1024 * 1024 * 1024);
    let threads = envp("SW_THREADS", 8) as usize;
    let seconds = envp("SW_SECONDS", 1800);
    let durability = std::env::var("SW_DURABILITY").unwrap_or_else(|_| "SYNC".into());

    if fstype(&dir).contains("tmpfs") {
        eprintln!("ABORT: {dir} is tmpfs; use real NVMe");
        std::process::exit(2);
    }
    let _ = std::fs::create_dir_all(&dir);
    let dur = match durability.as_str() {
        "NO_SYNC" => Durability::COMMIT_NO_SYNC,
        "WRITE_NO_SYNC" => Durability::COMMIT_WRITE_NO_SYNC,
        _ => Durability::COMMIT_SYNC,
    };

    println!("=== Noxu sustained 98/2 write/read (JSON 256-2048B, PK from sequence) ===");
    println!("  dir={dir} cache={}GiB threads={threads} seconds={seconds} dur={durability}", cache / 1024 / 1024 / 1024);

    let env = Arc::new(Environment::open(
        EnvironmentConfig::new(std::path::PathBuf::from(&dir))
            .with_allow_create(true).with_transactional(true)
            .with_cache_size(cache).with_durability(dur)
            // JE LOG_GROUP_COMMIT_THRESHOLD / INTERVAL (default 0/0 = off).
            // When set, the fsync leader waits briefly for more committers to
            // accumulate, trading a little latency for higher coalescing on
            // fast devices with few writers.
            .with_log_group_commit(
                envp("SW_GRPC_THRESHOLD", 0) as usize,
                envp("SW_GRPC_INTERVAL_MS", 0),
            ),
    ).expect("open env"));
    let db = Arc::new(env.open_database(
        None, "sustained",
        &DatabaseConfig::new().with_allow_create(true).with_transactional(true),
    ).expect("open db"));
    let env_for_close = Arc::clone(&env);

    let seq = Arc::new(AtomicU64::new(1)); // monotonic PK sequence
    let hist = Arc::new(Hist::new());
    let ops = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let start = Instant::now();

    // Per-60s-window reporter thread: prints throughput + p50/p99/p99.9/max so
    // p99 flatness under background cleaner activity is visible.
    let rep_hist = Arc::clone(&hist);
    let rep_ops = Arc::clone(&ops);
    let rep_stop = Arc::clone(&stop);
    let reporter = std::thread::spawn(move || {
        let mut last_ops = 0u64;
        let mut last_total = 0u64;
        println!("{:>4} {:>12} {:>10} {:>10} {:>10} {:>10}", "min", "ops/s", "p50us", "p99us", "p999us", "maxus");
        let mut min = 0;
        while !rep_stop.load(Ordering::Relaxed) {
            std::thread::sleep(std::time::Duration::from_secs(60));
            min += 1;
            let now_ops = rep_ops.load(Ordering::Relaxed);
            let win_ops = now_ops - last_ops;
            last_ops = now_ops;
            // window percentiles: approximate using cumulative hist (p99 over
            // the whole run is the flatness signal; also print interval rate).
            let _ = last_total;
            last_total = rep_hist.total();
            println!("{:>4} {:>12} {:>10} {:>10} {:>10} {:>10}",
                min, win_ops / 60,
                rep_hist.pct(0.50), rep_hist.pct(0.99), rep_hist.pct(0.999),
                rep_hist.max_us.load(Ordering::Relaxed));
        }
    });

    let handles: Vec<_> = (0..threads).map(|tid| {
        let db = Arc::clone(&db);
        let seq = Arc::clone(&seq);
        let hist = Arc::clone(&hist);
        let ops = Arc::clone(&ops);
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            let mut rng = SmallRng::seed_from_u64(0x5e97 ^ tid as u64);
            let mut local = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let t0 = Instant::now();
                if rng.gen_range(0..100) < 2 {
                    // 2% read: read a recent key
                    let hi = seq.load(Ordering::Relaxed);
                    let k = rng.gen_range(1..=hi.max(1));
                    let _ = db.get(k.to_be_bytes());
                } else {
                    // 98% write: fresh PK from the sequence, JSON value
                    let id = seq.fetch_add(1, Ordering::Relaxed);
                    let size = rng.gen_range(256..=2048);
                    let val = json_value(id, size, &mut rng);
                    let _ = db.put(id.to_be_bytes(), &val);
                }
                let us = t0.elapsed().as_micros() as u64;
                hist.record(us);
                local += 1;
                // Publish incrementally (cheap relaxed add) so the per-60s
                // reporter sees real per-window throughput, not 0 until join.
                ops.fetch_add(1, Ordering::Relaxed);
            }
            let _ = local;
        })
    }).collect();

    std::thread::sleep(std::time::Duration::from_secs(seconds));
    stop.store(true, Ordering::Relaxed);
    for h in handles { h.join().unwrap(); }
    reporter.join().unwrap();

    let elapsed = start.elapsed().as_secs_f64();
    let total = ops.load(Ordering::Relaxed);
    println!("\n=== FINAL ===");
    println!("  duration:   {elapsed:.0}s");
    println!("  total ops:  {total}");
    println!("  throughput: {:.0} ops/s", total as f64 / elapsed);
    println!("  keys seq'd: {}", seq.load(Ordering::Relaxed) - 1);
    let fsyncs = env.stat_fsync_count();
    let writes = total - (total / 50); // ~98% are writes (commits)
    println!(
        "  fsyncs: {fsyncs}  commits/fsync (coalescing): {:.2}",
        if fsyncs > 0 { writes as f64 / fsyncs as f64 } else { 0.0 }
    );
    println!("  latency (whole run): p50={}us p90={}us p99={}us p99.9={}us max={}us",
        hist.pct(0.50), hist.pct(0.90), hist.pct(0.99), hist.pct(0.999),
        hist.max_us.load(Ordering::Relaxed));

    db.close().unwrap();
    drop(env);
    if let Ok(e) = Arc::try_unwrap(env_for_close) {
        e.close().unwrap();
    }
}
