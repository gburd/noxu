//! Cross-engine benchmark driver — Noxu side.
//!
//! Implements the shared workload spec (workload-spec.md) so results are
//! directly comparable to the WiredTiger and TidesDB C drivers: identical
//! key/value format, key distributions, op mixes, thread counts, durability,
//! and RNG seed. One binary, selected via BENCH_WORKLOAD.
//!
//! Env: BENCH_DIR BENCH_RECORDS BENCH_CACHE BENCH_VALUE BENCH_THREADS
//!      BENCH_SECONDS BENCH_DURABILITY(SYNC|NO_SYNC) BENCH_WORKLOAD BENCH_SEED
//!      BENCH_ISOLATION(default|serializable)

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

    println!("=== NOXU xbench: workload={workload} records={records} cache={}GiB value={value_size} threads={threads} secs={seconds} dur={durability} iso={isolation} ===",
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

    // ── Measured phase ──
    let stop = Arc::new(AtomicBool::new(false));
    let ops = Arc::new(AtomicU64::new(0));
    let aborts = Arc::new(AtomicU64::new(0));
    let hist = Arc::new(Hist::new());
    let start = Instant::now();

    let handles: Vec<_> = (0..threads)
        .map(|tid| {
            let env = Arc::clone(&env);
            let db = Arc::clone(&db);
            let stop = Arc::clone(&stop);
            let ops = Arc::clone(&ops);
            let aborts = Arc::clone(&aborts);
            let hist = Arc::clone(&hist);
            let workload = workload.clone();
            let isolation = isolation.clone();
            std::thread::spawn(move || {
                let mut rng = Rng(seed ^ (tid as u64).wrapping_mul(0x9E3779B9));
                let zipf = Zipf::new(records);
                let value = vec![0x5Au8; value_size];
                let insert_ctr = AtomicU64::new(records + tid as u64 * 100_000_000);
                let txn_cfg = if isolation == "serializable" {
                    Some(TransactionConfig::new().with_serializable_isolation(true))
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
                while !stop.load(Ordering::Relaxed) {
                    let t0 = Instant::now();
                    match workload.as_str() {
                        "ycsb_a" => {
                            let k = key_bytes(zipf.next(&mut rng));
                            if rng.pct() < 50 {
                                if let Ok(t) = begin(&env) { let _ = db.get_in(&t, k); let _ = t.commit(); }
                            } else if let Ok(t) = begin(&env) {
                                if db.put_in(&t, k, &value).is_ok() { if t.commit().is_err() { labort += 1; } }
                                else { let _ = t.abort(); labort += 1; }
                            }
                        }
                        "ycsb_c" => {
                            let k = key_bytes(zipf.next(&mut rng));
                            if let Ok(t) = begin(&env) { let _ = db.get_in(&t, k); let _ = t.commit(); }
                        }
                        "tdb_write" => {
                            let id = insert_ctr.fetch_add(1, Ordering::Relaxed);
                            if let Ok(t) = begin(&env) {
                                if db.put_in(&t, key_bytes(id), &value).is_ok() { if t.commit().is_err() { labort += 1; } }
                                else { let _ = t.abort(); labort += 1; }
                            }
                        }
                        "txn_mix" => {
                            if let Ok(t) = begin(&env) {
                                let mut ok = true;
                                for j in 0..4 {
                                    let k = key_bytes(zipf.next(&mut rng));
                                    let r = match j {
                                        0 | 1 => db.put_in(&t, k, &value).map(|_| ()),
                                        2 => db.get_in(&t, k).map(|_| ()),
                                        _ => db.delete_in(&t, k).map(|_| ()),
                                    };
                                    if r.is_err() { ok = false; break; }
                                }
                                if ok { if t.commit().is_err() { labort += 1; } } else { let _ = t.abort(); labort += 1; }
                            }
                        }
                        "hotset" => {
                            // 10% of keys get 90% of ops
                            let hot = records / 10;
                            let k = if rng.pct() < 90 { key_bytes(rng.below(hot.max(1))) } else { key_bytes(rng.below(records)) };
                            if rng.pct() < 98 {
                                if let Ok(t) = begin(&env) {
                                    if db.put_in(&t, k, &value).is_ok() { if t.commit().is_err() { labort += 1; } } else { let _ = t.abort(); labort += 1; }
                                }
                            } else if let Ok(t) = begin(&env) { let _ = db.get_in(&t, k); let _ = t.commit(); }
                        }
                        "scan_under_write" => {
                            if tid % 2 == 0 {
                                // scanner: forward scan of 100 records from a random start
                                if let Ok(t) = begin(&env) {
                                    if let Ok(mut cur) = db.open_cursor_in(&t, None) {
                                        let _ = cur.seek(key_bytes(zipf.next(&mut rng)));
                                        for _ in 0..100 { if cur.next().ok().flatten().is_none() { break; } }
                                    }
                                    let _ = t.commit();
                                }
                            } else {
                                let k = key_bytes(zipf.next(&mut rng));
                                if let Ok(t) = begin(&env) {
                                    if db.put_in(&t, k, &value).is_ok() { if t.commit().is_err() { labort += 1; } } else { let _ = t.abort(); labort += 1; }
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
            })
        })
        .collect();

    std::thread::sleep(std::time::Duration::from_secs(seconds));
    stop.store(true, Ordering::Relaxed);
    for h in handles { h.join().unwrap(); }
    let el = start.elapsed().as_secs_f64();
    let total = ops.load(Ordering::Relaxed);
    let ab = aborts.load(Ordering::Relaxed);
    println!("RESULT engine=noxu workload={workload} iso={isolation} dur={durability} threads={threads} \
throughput={:.0} ops/s ops={total} aborts={ab} abort_rate={:.4} \
p50={} p90={} p99={} p999={} max={}",
        total as f64 / el, ab as f64 / total.max(1) as f64,
        hist.pct(0.50), hist.pct(0.90), hist.pct(0.99), hist.pct(0.999), hist.max.load(Ordering::Relaxed));

    db.close().unwrap();
    drop(db);
    if let Ok(e) = Arc::try_unwrap(env) { e.close().unwrap(); }
}
