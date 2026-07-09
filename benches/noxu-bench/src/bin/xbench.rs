//! Cross-engine benchmark driver — Noxu side.
//!
//! Implements the shared workload spec (workload-spec.md) so results are
//! directly comparable to the WiredTiger and TidesDB C drivers: identical
//! key/value format, key distributions, op mixes, thread counts, durability,
//! and RNG seed. One binary, selected via BENCH_WORKLOAD.
//!
//! Env: BENCH_DIR BENCH_RECORDS BENCH_CACHE BENCH_VALUE BENCH_THREADS
//!      BENCH_SECONDS BENCH_DURABILITY(SYNC|NO_SYNC) BENCH_WORKLOAD BENCH_SEED
//!      BENCH_ISOLATION(default|serializable|read_uncommitted)
//!      BENCH_NO_WAIT(0|1)  (1 = per-txn immediate-abort-on-conflict)
//!      BENCH_TAIL_INTERVAL(0=off, else per-N-sec TAIL series; default 0)
//!
//! "Where Noxu leads" metrics (see docs/src/operations/lead-benchmarks.md):
//!   * Tail-latency stability: RESULT emits p999/p9999/max; set
//!     BENCH_TAIL_INTERVAL=1 for a per-second `TAIL` series so flatness is
//!     visible over time (Noxu has no GC/compaction jitter source).
//!   * Memory efficiency: RESULT emits cache_hit_rate, ln_fetch(_miss),
//!     cached_bins, lru_size, ops_per_gb — hit-rate-per-GB vs MVCC engines
//!     that spend cache on version chains.
//!   * Write amplification: RESULT emits write_amp = physical bytes written
//!     (log seq-write bytes; /proc/self/io as cross-check) / committed user
//!     bytes — the metric where single-write-per-LN beats any LSM.

#[path = "dial9_profile.rs"]
mod dial9_profile;

use noxu_db::{
    DatabaseConfig, Durability, Environment, EnvironmentConfig,
    TransactionConfig,
};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

fn envs(k: &str, d: &str) -> String {
    std::env::var(k).unwrap_or_else(|_| d.to_string())
}
fn envp(k: &str, d: u64) -> u64 {
    std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(d)
}

/// 16-byte key: 8B big-endian id + 8B mixed tail (identical to C drivers).
fn key_bytes(id: u64) -> [u8; 16] {
    let mut k = [0u8; 16];
    k[..8].copy_from_slice(&id.to_be_bytes());
    k[8..].copy_from_slice(&id.wrapping_mul(2654435761).to_be_bytes());
    k
}

/// Deterministic xorshift RNG (identical algorithm in all 3 drivers so the
/// key sequences match byte-for-byte given the same seed).
struct Rng(u64);
impl Rng {
    #[inline]
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    #[inline]
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
    #[inline]
    fn pct(&mut self) -> u32 {
        (self.next() % 100) as u32
    }
}

/// Zipfian generator (YCSB-standard, theta=0.99). Precomputed zeta.
struct Zipf {
    n: u64,
    theta: f64,
    zetan: f64,
    alpha: f64,
    eta: f64,
}
impl Zipf {
    fn new(n: u64) -> Self {
        let theta = 0.99;
        let zetan = Self::zeta(n, theta);
        let zeta2 = Self::zeta(2, theta);
        let alpha = 1.0 / (1.0 - theta);
        let eta = (1.0 - (2.0 / n as f64).powf(1.0 - theta))
            / (1.0 - zeta2 / zetan);
        Zipf { n, theta, zetan, alpha, eta }
    }
    fn zeta(n: u64, theta: f64) -> f64 {
        let mut s = 0.0;
        for i in 1..=n {
            s += 1.0 / (i as f64).powf(theta);
        }
        s
    }
    #[inline]
    fn next(&self, rng: &mut Rng) -> u64 {
        let u = (rng.next() as f64) / (u64::MAX as f64);
        let uz = u * self.zetan;
        if uz < 1.0 {
            return 0;
        }
        if uz < 1.0 + 0.5f64.powf(self.theta) {
            return 1;
        }
        let v =
            (self.n as f64 * (self.eta * u - self.eta + 1.0).powf(self.alpha))
                as u64;
        v % self.n
    }
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

/// Fixed-bucket 1us latency histogram (64k buckets), shared design with C drivers.
struct Hist {
    b: Vec<AtomicU64>,
    max: AtomicU64,
}
impl Hist {
    fn new() -> Self {
        Hist { b: (0..65536).map(|_| AtomicU64::new(0)).collect(), max: AtomicU64::new(0) }
    }
    #[inline]
    fn record(&self, us: u64) {
        self.b[(us as usize).min(65535)].fetch_add(1, Ordering::Relaxed);
        let mut c = self.max.load(Ordering::Relaxed);
        while us > c {
            match self.max.compare_exchange_weak(c, us, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => break,
                Err(o) => c = o,
            }
        }
    }
    fn pct(&self, p: f64) -> u64 {
        let total: u64 = self.b.iter().map(|x| x.load(Ordering::Relaxed)).sum();
        if total == 0 {
            return 0;
        }
        let target = (total as f64 * p) as u64;
        let mut cum = 0u64;
        for (i, x) in self.b.iter().enumerate() {
            cum += x.load(Ordering::Relaxed);
            if cum >= target {
                return if i >= 65535 { self.max.load(Ordering::Relaxed) } else { i as u64 };
            }
        }
        self.max.load(Ordering::Relaxed)
    }
    /// Cumulative snapshot of every bucket (for interval-tail diffing).
    fn snapshot(&self) -> Vec<u64> {
        self.b.iter().map(|x| x.load(Ordering::Relaxed)).collect()
    }
}

/// Percentile over the ops that landed BETWEEN two cumulative snapshots
/// (prev→cur), i.e. one reporting interval. Bucket i == i microseconds.
fn pct_interval(prev: &[u64], cur: &[u64], p: f64) -> u64 {
    let total: u64 = cur.iter().zip(prev).map(|(c, p)| c - p).sum();
    if total == 0 {
        return 0;
    }
    let target = (total as f64 * p) as u64;
    let mut cum = 0u64;
    for i in 0..cur.len() {
        cum += cur[i] - prev[i];
        if cum >= target {
            return i as u64;
        }
    }
    (cur.len() - 1) as u64
}

/// Cumulative bytes this process has physically written, per the kernel
/// (`/proc/self/io` `write_bytes`). Cross-check for the log seq-write counter.
/// Returns 0 if unavailable (e.g. non-Linux).
fn proc_write_bytes() -> u64 {
    std::fs::read_to_string("/proc/self/io")
        .ok()
        .and_then(|s| {
            s.lines().find_map(|l| {
                l.strip_prefix("write_bytes:").and_then(|v| v.trim().parse().ok())
            })
        })
        .unwrap_or(0)
}

fn main() {
    let dir = envs("BENCH_DIR", "/tmp/noxu-xbench");
    let records = envp("BENCH_RECORDS", 10_000_000);
    let cache = envp("BENCH_CACHE", 2 * 1024 * 1024 * 1024);
    let value_size = envp("BENCH_VALUE", 1024) as usize;
    let threads = envp("BENCH_THREADS", 64) as usize;
    let seconds = envp("BENCH_SECONDS", 30);
    let durability = envs("BENCH_DURABILITY", "SYNC");
    let workload = envs("BENCH_WORKLOAD", "ycsb_a");
    let seed = envp("BENCH_SEED", 0xC0FFEE);
    let isolation = envs("BENCH_ISOLATION", "default");
    let no_wait = envs("BENCH_NO_WAIT", "0") == "1";

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

    println!("=== NOXU xbench: workload={workload} records={records} cache={}GiB value={value_size} threads={threads} secs={seconds} dur={durability} iso={isolation} no_wait={no_wait} ===",
        cache / 1024 / 1024 / 1024);

    let mut ecfg = EnvironmentConfig::new(std::path::PathBuf::from(&dir));
    ecfg.set_allow_create(true);
    ecfg.set_transactional(true);
    ecfg.set_cache_size(cache);
    ecfg.set_durability(dur);
    let env = Arc::new(Environment::open(ecfg).expect("open env"));
    let db = Arc::new(
        env.open_database(
            None,
            "xbench",
            &DatabaseConfig::new().with_allow_create(true).with_transactional(true),
        )
        .expect("open db"),
    );

    // ── Load phase (batched, NO_SYNC-fast via large txns) ──
    // BENCH_SKIP_LOAD=1: reuse an already-loaded dataset (the orchestrator
    // loads once per engine, then runs many measure-only passes).
    if envs("BENCH_SKIP_LOAD", "0") == "1" {
        println!("-- skipping load (reusing existing dataset) --");
    } else {
    println!("-- loading {records} records --");
    let lt = Instant::now();
    let load_threads = 8usize;
    let per = records / load_threads as u64;
    std::thread::scope(|s| {
        for tid in 0..load_threads {
            let env = Arc::clone(&env);
            let db = Arc::clone(&db);
            let start = tid as u64 * per;
            let end = if tid == load_threads - 1 { records } else { start + per };
            s.spawn(move || {
                let value = vec![0x5Au8; value_size];
                let mut i = start;
                while i < end {
                    let batch_end = (i + 1000).min(end);
                    if let Ok(txn) = env.begin_transaction(None) {
                        let mut ok = true;
                        for j in i..batch_end {
                            if db.put_in(&txn, key_bytes(j), &value).is_err() {
                                ok = false;
                                break;
                            }
                        }
                        if ok { let _ = txn.commit(); } else { let _ = txn.abort(); }
                    }
                    i = batch_end;
                }
            });
        }
    });
    env.checkpoint(None).unwrap();
    println!("   loaded in {:.1}s", lt.elapsed().as_secs_f64());
    }

    // ── Measured phase ──
    // Optional in-process dial9 profiler (BENCH_PROFILE=cpu|offcpu). Off by
    // default; used to diagnose read/commit-path contention without external
    // perf/gdb. Started here so it covers only the measured phase, not load.
    let mut profiler = dial9_profile::Profiler::maybe_start(&envs("BENCH_PROFILE", ""));
    let stop = Arc::new(AtomicBool::new(false));
    let ops = Arc::new(AtomicU64::new(0));
    let aborts = Arc::new(AtomicU64::new(0));
    // Committed user record-writes (successful puts in committed txns). Times
    // value_size == committed user bytes, the denominator of write_amp.
    let writes = Arc::new(AtomicU64::new(0));
    // Committed read ops (successful gets). Denominator for the LN-cache
    // hit-rate (1 - LN-faults-from-log / reads).
    let reads = Arc::new(AtomicU64::new(0));
    let hist = Arc::new(Hist::new());
    // Physical-write baselines captured just before the measured phase so
    // write_amp reflects only measured-phase writes, not the load phase.
    let (env_stats0, proc_wb0) = (env.stats().ok(), proc_write_bytes());
    let log_wb0 =
        env_stats0.as_ref().map(|s| s.log.n_sequential_write_bytes).unwrap_or(0);
    // LN-fault baseline (log random reads = LN faulted from disk on a cache
    // miss). n_random_reads is the LIVE cache-miss signal; the evictor
    // ln_fetch/bin_fetch counters are declared but never incremented in the
    // engine today (see lead-benchmarks.md "stats gaps"), so hit-rate is
    // derived from the log random-read counter, not the evictor.
    let rr0 = env_stats0.as_ref().map(|s| s.log.n_random_reads).unwrap_or(0);
    let start = Instant::now();

    // Optional per-interval tail series (Noxu-leads flatness signal). Prints a
    // `TAIL` line every BENCH_TAIL_INTERVAL seconds with the percentiles of
    // ops that completed IN that interval (snapshot-diff of the histogram).
    let tail_interval = envp("BENCH_TAIL_INTERVAL", 0);
    let tail_reporter = if tail_interval > 0 {
        let hist = Arc::clone(&hist);
        let ops = Arc::clone(&ops);
        let stop = Arc::clone(&stop);
        Some(std::thread::spawn(move || {
            let mut prev = hist.snapshot();
            let mut prev_ops = 0u64;
            let mut t = 0u64;
            while !stop.load(Ordering::Relaxed) {
                std::thread::sleep(std::time::Duration::from_secs(tail_interval));
                t += tail_interval;
                let cur = hist.snapshot();
                let now_ops = ops.load(Ordering::Relaxed);
                println!("TAIL t={t} ops_s={} p50={} p99={} p999={} p9999={} max_us_bucket={}",
                    (now_ops - prev_ops) / tail_interval,
                    pct_interval(&prev, &cur, 0.50),
                    pct_interval(&prev, &cur, 0.99),
                    pct_interval(&prev, &cur, 0.999),
                    pct_interval(&prev, &cur, 0.9999),
                    pct_interval(&prev, &cur, 1.0));
                prev = cur;
                prev_ops = now_ops;
            }
        }))
    } else {
        None
    };

    let handles: Vec<_> = (0..threads)
        .map(|tid| {
            let env = Arc::clone(&env);
            let db = Arc::clone(&db);
            let stop = Arc::clone(&stop);
            let ops = Arc::clone(&ops);
            let aborts = Arc::clone(&aborts);
            let writes = Arc::clone(&writes);
            let reads = Arc::clone(&reads);
            let hist = Arc::clone(&hist);
            let workload = workload.clone();
            let isolation = isolation.clone();
            std::thread::spawn(move || {
                let mut rng = Rng(seed ^ (tid as u64).wrapping_mul(0x9E3779B9));
                let zipf = Zipf::new(records);
                let value = vec![0x5Au8; value_size];
                let insert_ctr = AtomicU64::new(records + tid as u64 * 100_000_000);
                // Isolation + no_wait both live on TransactionConfig; build
                // one config if either knob is non-default.
                let txn_cfg = if isolation != "default" || no_wait {
                    let mut c = TransactionConfig::new();
                    match isolation.as_str() {
                        "serializable" => c = c.with_serializable_isolation(true),
                        // read_uncommitted skips the record-lock probe on reads
                        // (engine: is_read_uncommitted_default / lock_ln early return).
                        "read_uncommitted" => c = c.with_read_uncommitted(true),
                        _ => {}
                    }
                    if no_wait {
                        c = c.with_no_wait(true);
                    }
                    Some(c)
                } else {
                    None
                };
                let begin = |env: &Environment| {
                    match &txn_cfg {
                        Some(c) => env.begin_transaction(Some(c)),
                        None => env.begin_transaction(None),
                    }
                };
                let mut local = 0u64;
                let mut labort = 0u64;
                // Committed record-writes this thread (per-arm; see below).
                let mut lwrites = 0u64;
                let mut lreads = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    let t0 = Instant::now();
                    match workload.as_str() {
                        "ycsb_a" => {
                            let k = key_bytes(zipf.next(&mut rng));
                            if rng.pct() < 50 {
                                if let Ok(t) = begin(&env) { if db.get_in(&t, k).is_ok() { lreads += 1; } let _ = t.commit(); }
                            } else if let Ok(t) = begin(&env) {
                                if db.put_in(&t, k, &value).is_ok() { if t.commit().is_err() { labort += 1; } else { lwrites += 1; } }
                                else { let _ = t.abort(); labort += 1; }
                            }
                        }
                        "ycsb_c" => {
                            let k = key_bytes(zipf.next(&mut rng));
                            if let Ok(t) = begin(&env) { if db.get_in(&t, k).is_ok() { lreads += 1; } let _ = t.commit(); }
                        }
                        "tdb_write" => {
                            let id = insert_ctr.fetch_add(1, Ordering::Relaxed);
                            if let Ok(t) = begin(&env) {
                                if db.put_in(&t, key_bytes(id), &value).is_ok() { if t.commit().is_err() { labort += 1; } else { lwrites += 1; } }
                                else { let _ = t.abort(); labort += 1; }
                            }
                        }
                        "txn_mix" => {
                            if let Ok(t) = begin(&env) {
                                let mut ok = true;
                                let mut puts = 0u64;
                                let mut gets = 0u64;
                                for j in 0..4 {
                                    let k = key_bytes(zipf.next(&mut rng));
                                    let r = match j {
                                        0 | 1 => db.put_in(&t, k, &value).map(|_| { puts += 1; }),
                                        2 => db.get_in(&t, k).map(|_| { gets += 1; }),
                                        _ => db.delete_in(&t, k).map(|_| ()),
                                    };
                                    if r.is_err() { ok = false; break; }
                                }
                                if ok { if t.commit().is_err() { labort += 1; } else { lwrites += puts; lreads += gets; } } else { let _ = t.abort(); labort += 1; }
                            }
                        }
                        "hotset" => {
                            // 10% of keys get 90% of ops
                            let hot = records / 10;
                            let k = if rng.pct() < 90 { key_bytes(rng.below(hot.max(1))) } else { key_bytes(rng.below(records)) };
                            if rng.pct() < 98 {
                                if let Ok(t) = begin(&env) {
                                    if db.put_in(&t, k, &value).is_ok() { if t.commit().is_err() { labort += 1; } else { lwrites += 1; } } else { let _ = t.abort(); labort += 1; }
                                }
                            } else if let Ok(t) = begin(&env) { if db.get_in(&t, k).is_ok() { lreads += 1; } let _ = t.commit(); }
                        }
                        "scan_under_write" => {
                            if tid % 2 == 0 {
                                // scanner: forward scan of 100 records from a random start
                                if let Ok(t) = begin(&env) {
                                    if let Ok(mut cur) = db.open_cursor_in(&t, None) {
                                        let _ = cur.seek(key_bytes(zipf.next(&mut rng)));
                                        for _ in 0..100 { if cur.next().ok().flatten().is_none() { break; } lreads += 1; }
                                    }
                                    let _ = t.commit();
                                }
                            } else {
                                let k = key_bytes(zipf.next(&mut rng));
                                if let Ok(t) = begin(&env) {
                                    if db.put_in(&t, k, &value).is_ok() { if t.commit().is_err() { labort += 1; } else { lwrites += 1; } } else { let _ = t.abort(); labort += 1; }
                                }
                            }
                        }
                        _ => {}
                    }
                    hist.record(t0.elapsed().as_micros() as u64);
                    local += 1;
                    ops.fetch_add(1, Ordering::Relaxed);
                }
                let _ = local;
                aborts.fetch_add(labort, Ordering::Relaxed);
                writes.fetch_add(lwrites, Ordering::Relaxed);
                reads.fetch_add(lreads, Ordering::Relaxed);
            })
        })
        .collect();

    std::thread::sleep(std::time::Duration::from_secs(seconds));
    stop.store(true, Ordering::Relaxed);
    for h in handles { h.join().unwrap(); }
    if let Some(p) = profiler.as_mut() {
        p.report(30);
    }
    if let Some(r) = tail_reporter { r.join().unwrap(); }
    let el = start.elapsed().as_secs_f64();
    let total = ops.load(Ordering::Relaxed);
    let ab = aborts.load(Ordering::Relaxed);
    let committed_writes = writes.load(Ordering::Relaxed);
    let committed_reads = reads.load(Ordering::Relaxed);

    // ── "Where Noxu leads" metrics ──────────────────────────────────────
    // Snapshot the engine stats once, after the measured phase.
    let s1 = env.stats().ok();
    // L2 memory efficiency: LN-cache hit-rate. random_reads counts LNs faulted
    // from the log on a cache miss; a read that hits cache does no random
    // read. hit_rate = 1 - faults/reads. (WT/RocksDB spend cache on version
    // chains / block cache; Noxu holds exactly one version per record, so at a
    // fixed cache it keeps more distinct records resident → higher hit-rate.)
    let rr1 = s1.as_ref().map(|s| s.log.n_random_reads).unwrap_or(0);
    let ln_faults = rr1.saturating_sub(rr0);
    let cache_hit_rate = if committed_reads > 0 {
        (1.0 - (ln_faults as f64 / committed_reads as f64)).max(0.0)
    } else {
        -1.0 // no reads in this workload (e.g. tdb_write) — n/a, not a false 1.0
    };
    let cache_gb = cache as f64 / (1024.0 * 1024.0 * 1024.0);
    let ops_per_gb = if cache_gb > 0.0 { (total as f64 / el) / cache_gb } else { 0.0 };
    // Resident-node counts (evictor instant stats). NOTE: lru_size/cached_bins
    // are refreshed only by Evictor::update_lru_stats(), which the stats path
    // does not currently call, so these often read 0 — reported for
    // transparency; a true resident-records stat is a follow-up (see docs).
    let (cached_bins, lru_size) = s1
        .as_ref()
        .map(|s| (s.evictor.cached_bins, s.evictor.lru_size))
        .unwrap_or((0, 0));

    // L3 write amplification: physical bytes written / committed user bytes.
    // Numerator: log sequential-write bytes (Noxu writes each LN once; the
    // cleaner reclaims but does not re-sort the dataset like an LSM). Falls
    // back to /proc/self/io write_bytes delta if the log counter is 0.
    let log_wb1 = s1.as_ref().map(|s| s.log.n_sequential_write_bytes).unwrap_or(0);
    let proc_wb1 = proc_write_bytes();
    let log_written = log_wb1.saturating_sub(log_wb0);
    let proc_written = proc_wb1.saturating_sub(proc_wb0);
    let user_bytes = committed_writes.saturating_mul(value_size as u64);
    let phys_bytes = if log_written > 0 { log_written } else { proc_written };
    let write_amp = if user_bytes > 0 { phys_bytes as f64 / user_bytes as f64 } else { 0.0 };

    println!("RESULT engine=noxu workload={workload} iso={isolation} dur={durability} threads={threads} \
no_wait={no_wait} throughput={:.0} ops/s ops={total} aborts={ab} abort_rate={:.4} \
p50={} p90={} p99={} p999={} p9999={} max={} \
cache_hit_rate={:.4} committed_reads={committed_reads} ln_faults={ln_faults} cached_bins={cached_bins} lru_size={lru_size} ops_per_gb={:.0} \
committed_writes={committed_writes} user_bytes={user_bytes} log_write_bytes={log_written} proc_write_bytes={proc_written} write_amp={:.3}",
        total as f64 / el, ab as f64 / total.max(1) as f64,
        hist.pct(0.50), hist.pct(0.90), hist.pct(0.99), hist.pct(0.999), hist.pct(0.9999), hist.max.load(Ordering::Relaxed),
        cache_hit_rate, ops_per_gb, write_amp);

    db.close().unwrap();
    drop(db);
    if let Ok(e) = Arc::try_unwrap(env) { e.close().unwrap(); }
}
