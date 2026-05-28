//! Noxu DB standalone workload benchmark.
//!
//! Runs 10 diverse workloads at five scales (1K, 10K, 100K, 500K, 1M),
//! and W10 (concurrent) at six thread configurations (1→16 threads).
//!
//! Metrics collected per workload:
//!   elapsed wall-clock time, ns/op, ops/sec,
//!   CPU time (user+sys, Linux /proc/self/stat),
//!   RSS delta (Linux /proc/self/status),
//!   I/O bytes read/written (Linux /proc/self/io),
//!   on-disk directory size and bytes-per-op.
//!
//! Run with:
//!   cargo run --bin noxu-workload-bench --release

#![allow(dead_code)]

mod concurrent;
mod metrics;
mod workloads;

use metrics::{cpu_time_ms, dir_size_kb, proc_io, rss_kb};
use noxu_db::{
    Database, DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig,
};
use noxu_xa::XaEnvironment;
use std::fs;
use std::io::Write as IoWrite;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tempfile::TempDir;

// ─────────────────────────────────────────────────────────────────────────────
// Bench directory helper
//
// By default each workload uses a fresh TempDir (tmpfs on Linux).  Set
// NOXU_BENCH_DIR to a path on real NVMe/SSD storage to measure FSyncManager
// coalescing behaviour that is invisible on tmpfs (where fdatasync is instant).
//
// Example:
//   NOXU_BENCH_DIR=/mnt/nvme/noxu_bench NOXU_MAX_SCALE=100000 ./noxu-workload-bench
// ─────────────────────────────────────────────────────────────────────────────

/// Holds either a managed TempDir (auto-deleted on drop) or a real-storage
/// directory.  Real directories are deleted on drop when `cleanup=true`
/// (set via `NOXU_BENCH_CLEANUP=1` — used for large-scale runs to stay
/// within the 200 GB disk budget).
struct RealDir {
    path: PathBuf,
    cleanup: bool,
}

impl RealDir {
    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for RealDir {
    fn drop(&mut self) {
        if self.cleanup {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}

enum BenchDir {
    Temp(TempDir),
    Real(RealDir),
}

impl BenchDir {
    fn path(&self) -> &Path {
        match self {
            BenchDir::Temp(d) => d.path(),
            BenchDir::Real(r) => r.path(),
        }
    }
}

/// Create a fresh benchmark directory.
///
/// Priority:
/// 1. `NOXU_BENCH_DIR` env var (explicit override)
/// 2. `/scratch/noxu_bench` if `/scratch` is writable (NVMe on this machine)
/// 3. TempDir (tmpfs fallback — FSyncManager coalescing is invisible on tmpfs)
///
/// When `NOXU_BENCH_CLEANUP=1` the returned `RealDir` deletes itself on drop,
/// keeping peak disk consumption to ~2× the per-workload dataset size.
fn new_bench_dir(
    base: &Option<PathBuf>,
    tag: &str,
    n: usize,
    cleanup: bool,
) -> BenchDir {
    let root = match base {
        Some(r) => r.clone(),
        None => {
            let scratch = PathBuf::from("/scratch/noxu_bench");
            if scratch.parent().map(|p| p.exists()).unwrap_or(false) {
                scratch
            } else {
                return BenchDir::Temp(TempDir::new().unwrap());
            }
        }
    };
    let dir = root.join(format!("{}_{}", tag, n));
    // Remove any leftover data from a previous run.
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).expect("failed to create bench dir");
    BenchDir::Real(RealDir { path: dir, cleanup })
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

fn open_db(dir: &Path) -> (Environment, Database) {
    let cfg = EnvironmentConfig::new(dir.to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    let env = Environment::open(cfg).unwrap();
    let db = env
        .open_database(
            None,
            "bench",
            &DatabaseConfig::new().with_allow_create(true),
        )
        .unwrap();
    (env, db)
}

/// Open with aggressive group commit — used for w10_txn_conc benchmarks to show
/// FsyncManager coalescing under concurrent transactional workloads.
///
/// threshold=4, interval_ms=5: leader waits up to 5 ms for 4+ concurrent
/// committers, then fsyncs on behalf of all accumulated waiters.  The longer
/// interval (vs default 1ms) gives more threads time to pass through LWL and
/// accumulate in the FsyncManager wait queue, maximising coalescing.
fn open_db_group_commit(dir: &Path) -> (Environment, Database) {
    let cfg = EnvironmentConfig::new(dir.to_path_buf())
        .with_allow_create(true)
        .with_transactional(true)
        .with_log_group_commit(4, 5);
    let env = Environment::open(cfg).unwrap();
    let db = env
        .open_database(
            None,
            "bench",
            &DatabaseConfig::new().with_allow_create(true),
        )
        .unwrap();
    (env, db)
}

fn populate(db: &Database, n: usize, value: &[u8]) {
    for i in 0..n {
        let k = DatabaseEntry::from_vec(format!("{:010}", i).into_bytes());
        let v = DatabaseEntry::from_bytes(value);
        db.put(None, &k, &v).unwrap();
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Result type
// ─────────────────────────────────────────────────────────────────────────────

struct WorkloadResult {
    workload: String,
    scale: usize,
    threads: usize,
    elapsed_ms: f64,
    ns_per_op: f64,
    ops_per_sec: f64,
    cpu_ms: u64,
    rss_delta_kb: i64,
    read_kb: u64,
    write_kb: u64,
    disk_kb: u64,
    disk_bytes_per_op: f64,
    /// Number of fdatasync calls during this workload (port of stat).
    fsync_count: u64,
}

// ─────────────────────────────────────────────────────────────────────────────
// Timing wrapper
// ─────────────────────────────────────────────────────────────────────────────

fn run_timed<F: FnOnce() -> usize>(
    workload_name: impl Into<String>,
    scale: usize,
    threads: usize,
    data_dir: &Path,
    env: Option<&Environment>,
    f: F,
) -> WorkloadResult {
    let rss0 = rss_kb();
    let io0 = proc_io();
    let cpu0 = cpu_time_ms();
    let fsync0 = env.map(|e| e.stat_fsync_count()).unwrap_or(0);

    let t0 = std::time::Instant::now();
    let ops = f();
    let elapsed_ms = t0.elapsed().as_secs_f64() * 1000.0;

    let cpu1 = cpu_time_ms();
    let rss1 = rss_kb();
    let io1 = proc_io();
    let disk_kb = dir_size_kb(data_dir);
    let fsync1 = env.map(|e| e.stat_fsync_count()).unwrap_or(0);

    let disk_bytes_per_op =
        if ops > 0 { (disk_kb * 1024) as f64 / ops as f64 } else { 0.0 };

    WorkloadResult {
        workload: workload_name.into(),
        scale,
        threads,
        elapsed_ms,
        ns_per_op: if ops > 0 {
            elapsed_ms * 1_000_000.0 / ops as f64
        } else {
            0.0
        },
        ops_per_sec: if elapsed_ms > 0.0 {
            ops as f64 / (elapsed_ms / 1000.0)
        } else {
            0.0
        },
        cpu_ms: cpu1.saturating_sub(cpu0),
        rss_delta_kb: rss1 - rss0,
        read_kb: io1.0.saturating_sub(io0.0) / 1024,
        write_kb: io1.1.saturating_sub(io0.1) / 1024,
        disk_kb,
        disk_bytes_per_op,
        fsync_count: fsync1.saturating_sub(fsync0),
    }
}

fn print_progress(r: &WorkloadResult) {
    println!(
        "  {:<26} n={:<8} t={:<3} {:>8.1}ms  {:>11.0} ops/s  cpu={}ms",
        r.workload, r.scale, r.threads, r.elapsed_ms, r.ops_per_sec, r.cpu_ms
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// main
// ─────────────────────────────────────────────────────────────────────────────

fn main() {
    // ── NOXU_BENCH_VALUE_SIZE: value payload size in bytes (default 64).
    // Large values exercise I/O-bound paths.  At 100 KB and 1 M records the
    // active dataset is ~100 GB.
    let value_size: usize = std::env::var("NOXU_BENCH_VALUE_SIZE")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|&v: &usize| v >= 1)
        .unwrap_or(64);
    let value_bytes: Vec<u8> = vec![0x58u8; value_size]; // 'X' repeated

    // ── NOXU_BENCH_SCALES: comma-separated explicit scale list (e.g. "1000000").
    // Overrides NOXU_MAX_SCALE when set.
    let custom_scales: Option<Vec<usize>> = std::env::var("NOXU_BENCH_SCALES")
        .ok()
        .filter(|s| !s.is_empty())
        .map(|s| s.split(',').filter_map(|t| t.trim().parse().ok()).collect());

    // ── NOXU_MAX_SCALE: upper limit on the default five-scale list.
    let max_scale: usize = std::env::var("NOXU_MAX_SCALE")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&v| v > 0)
        .unwrap_or(usize::MAX);

    // ── NOXU_BENCH_CLEANUP: delete each workload directory after collecting
    // results.  Required for large-value runs to stay within disk budget.
    let cleanup: bool = std::env::var("NOXU_BENCH_CLEANUP")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);

    // ── NOXU_BENCH_DIR: explicit override for benchmark storage root.
    // Falls back to /scratch/noxu_bench (NVMe) if available, then TempDir.
    let bench_base: Option<PathBuf> = std::env::var("NOXU_BENCH_DIR")
        .ok()
        .filter(|s| !s.is_empty())
        .map(PathBuf::from);
    let storage_label = if let Some(ref b) = bench_base {
        format!("{} (NOXU_BENCH_DIR)", b.display())
    } else if PathBuf::from("/scratch").exists() {
        "/scratch/noxu_bench (NVMe auto-detected)".to_string()
    } else {
        "TempDir (tmpfs — FSyncManager coalescing window is zero)".to_string()
    };
    println!("  Storage:    {}", storage_label);
    println!(
        "  ValueSize:  {} bytes  ({:.1} MB per 1M records)",
        value_size,
        value_size as f64 * 1_000_000.0 / 1_073_741_824.0 * 1024.0
    );
    println!("  Cleanup:    {}", cleanup);

    let all_scales: &[usize] = &[1_000, 10_000, 100_000, 500_000, 1_000_000];
    let scales: Vec<usize> = if let Some(cs) = custom_scales {
        cs
    } else {
        all_scales.iter().copied().filter(|&s| s <= max_scale).collect()
    };
    let scales: &[usize] = &scales;

    // W10 concurrent configurations: (label, reader_threads, writer_threads)
    let concurrent_configs: &[(&str, usize, usize)] = &[
        ("w10_conc_1r0w", 1, 0), // read-only,  1 thread
        ("w10_conc_0r1w", 0, 1), // write-only, 1 thread
        ("w10_conc_4r0w", 4, 0), // read-only,  4 threads
        ("w10_conc_0r4w", 0, 4), // write-only, 4 threads
        ("w10_conc_4r4w", 4, 4), // mixed,      8 threads
        ("w10_conc_8r8w", 8, 8), // heavy,     16 threads
    ];

    let mut results: Vec<WorkloadResult> = Vec::new();

    for &n in scales {
        println!("\n══ Scale: {} ══", n);

        // W01: sequential write
        {
            let dir = new_bench_dir(&bench_base, "bench_2", n, cleanup);
            let (env, db) = open_db(dir.path());
            let r = run_timed(
                "w01_seq_write",
                n,
                1,
                dir.path(),
                Some(&env),
                || workloads::w01_seq_write(&db, n, &value_bytes),
            );
            print_progress(&r);
            results.push(r);
            drop(db);
            drop(env);
        }

        // W02: random write
        {
            let dir = new_bench_dir(&bench_base, "bench_3", n, cleanup);
            let (env, db) = open_db(dir.path());
            let r = run_timed(
                "w02_rand_write",
                n,
                1,
                dir.path(),
                Some(&env),
                || workloads::w02_rand_write(&db, n, &value_bytes),
            );
            print_progress(&r);
            results.push(r);
            drop(db);
            drop(env);
        }

        // W03: sequential read (pre-populate)
        {
            let dir = new_bench_dir(&bench_base, "bench_4", n, cleanup);
            let (env, db) = open_db(dir.path());
            populate(&db, n, &value_bytes);
            let r =
                run_timed("w03_seq_read", n, 1, dir.path(), Some(&env), || {
                    workloads::w03_seq_read(&db, n)
                });
            print_progress(&r);
            results.push(r);
            drop(db);
            drop(env);
        }

        // W04: random read
        {
            let dir = new_bench_dir(&bench_base, "bench_5", n, cleanup);
            let (env, db) = open_db(dir.path());
            populate(&db, n, &value_bytes);
            let r = run_timed(
                "w04_rand_read",
                n,
                1,
                dir.path(),
                Some(&env),
                || workloads::w04_rand_read(&db, n),
            );
            print_progress(&r);
            results.push(r);
            drop(db);
            drop(env);
        }

        // W05: range scan
        {
            let dir = new_bench_dir(&bench_base, "bench_6", n, cleanup);
            let (env, db) = open_db(dir.path());
            populate(&db, n, &value_bytes);
            let r = run_timed(
                "w05_range_scan",
                n,
                1,
                dir.path(),
                Some(&env),
                || workloads::w05_range_scan(&db, n),
            );
            print_progress(&r);
            results.push(r);
            drop(db);
            drop(env);
        }

        // W06: write-heavy mixed (90% write / 10% read)
        {
            let dir = new_bench_dir(&bench_base, "bench_7", n, cleanup);
            let (env, db) = open_db(dir.path());
            populate(&db, n, &value_bytes);
            let r = run_timed(
                "w06_write_heavy",
                n,
                1,
                dir.path(),
                Some(&env),
                || workloads::w06_write_heavy(&db, n, &value_bytes),
            );
            print_progress(&r);
            results.push(r);
            drop(db);
            drop(env);
        }

        // W07: read-heavy mixed (90% read / 10% write)
        {
            let dir = new_bench_dir(&bench_base, "bench_8", n, cleanup);
            let (env, db) = open_db(dir.path());
            populate(&db, n, &value_bytes);
            let r = run_timed(
                "w07_read_heavy",
                n,
                1,
                dir.path(),
                Some(&env),
                || workloads::w07_read_heavy(&db, n, &value_bytes),
            );
            print_progress(&r);
            results.push(r);
            drop(db);
            drop(env);
        }

        // W08: delete + insert pairs
        {
            let dir = new_bench_dir(&bench_base, "bench_9", n, cleanup);
            let (env, db) = open_db(dir.path());
            populate(&db, n, &value_bytes);
            let r = run_timed(
                "w08_delete_insert",
                n,
                1,
                dir.path(),
                Some(&env),
                || workloads::w08_delete_insert(&db, n, &value_bytes),
            );
            print_progress(&r);
            results.push(r);
            drop(db);
            drop(env);
        }

        // W09: transactional multi-op (3 gets + 2 puts per txn)
        //
        // get(key i) + put(key i) in the same transaction triggers the READ→WRITE
        // lock upgrade path (LockUpgrade::WritePromote).  ThinLockImpl handles
        // this correctly: the upgrade is granted immediately as LockGrantType::Promotion.
        {
            let w09_n = n;
            let dir = new_bench_dir(&bench_base, "bench_10", n, cleanup);
            let (env, db) = open_db(dir.path());
            populate(&db, w09_n, &value_bytes);
            let r = run_timed(
                "w09_txn_multi",
                w09_n,
                1,
                dir.path(),
                Some(&env),
                || workloads::w09_txn_multi(&env, &db, w09_n, &value_bytes),
            );
            print_progress(&r);
            results.push(r);
            drop(db);
            drop(env);
        }

        // W10: concurrent — six thread configurations
        // EnvironmentConfig defaults (threshold=4, interval_ms=1) provide optimal
        // group commit: leader waits up to 1ms when 1-3 threads are queued,
        // giving time for more threads to pass through LWL before the fsync.
        for &(label, rthreads, wthreads) in concurrent_configs {
            let total_threads = rthreads + wthreads;
            // Cap ops at 100K when writer threads > 4 at large scale to keep runtime sane
            let ops_n = if n > 100_000 && wthreads > 4 { 100_000 } else { n };

            let dir = new_bench_dir(&bench_base, "bench_11", n, cleanup);
            let (env, db) = open_db(dir.path());
            populate(&db, ops_n, &value_bytes);

            // Capture fsync baseline AFTER populate so we measure only workload fsyncs.
            let fsync0 = env.stat_fsync_count();

            let db_arc = Arc::new(db);
            let cpu0 = cpu_time_ms();
            let rss0 = rss_kb();
            let io0 = proc_io();

            let conc = concurrent::run_concurrent(
                Arc::clone(&db_arc),
                rthreads,
                wthreads,
                ops_n / total_threads.max(1),
                value_size,
            );

            let cpu1 = cpu_time_ms();
            let rss1 = rss_kb();
            let io1 = proc_io();
            let disk_kb = dir_size_kb(dir.path());
            let total_ops = conc.total_ops as usize;

            let r = WorkloadResult {
                workload: label.to_string(),
                scale: n,
                threads: total_threads,
                elapsed_ms: conc.elapsed_ms,
                ns_per_op: if total_ops > 0 {
                    conc.elapsed_ms * 1_000_000.0 / total_ops as f64
                } else {
                    0.0
                },
                ops_per_sec: conc.ops_per_sec,
                cpu_ms: cpu1.saturating_sub(cpu0),
                rss_delta_kb: rss1 - rss0,
                read_kb: io1.0.saturating_sub(io0.0) / 1024,
                write_kb: io1.1.saturating_sub(io0.1) / 1024,
                disk_kb,
                disk_bytes_per_op: if total_ops > 0 {
                    (disk_kb * 1024) as f64 / total_ops as f64
                } else {
                    0.0
                },
                fsync_count: env.stat_fsync_count().saturating_sub(fsync0),
            };
            print_progress(&r);
            results.push(r);

            drop(db_arc);
            drop(env);
        }

        // W10_TXN: transactional concurrent writes (8 writer threads) with
        // group commit enabled.  This is the canonical group-commit coalescing
        // benchmark: each writer wraps its put in begin_transaction/commit,
        // giving FsyncManager a chance to coalesce concurrent fsyncs.
        {
            let wthreads = 8usize;
            let ops_n = if n > 100_000 { 100_000 } else { n };
            let ops_per_thread = ops_n / wthreads;

            // No group commit variant: each commit does its own fsync.
            {
                let dir =
                    new_bench_dir(&bench_base, "bench_txn_no_gc", n, cleanup);
                let (env, db) = open_db(dir.path());
                let fsync0 = env.stat_fsync_count();
                let cpu0 = cpu_time_ms();
                let io0 = proc_io();
                let conc = concurrent::run_concurrent_txn(
                    &env,
                    &db,
                    wthreads,
                    ops_per_thread,
                    value_size,
                );
                let cpu1 = cpu_time_ms();
                let io1 = proc_io();
                let disk_kb = dir_size_kb(dir.path());
                let total_ops = conc.total_ops as usize;
                let r = WorkloadResult {
                    workload: "w10_txn_no_gc".to_string(),
                    scale: n,
                    threads: wthreads,
                    elapsed_ms: conc.elapsed_ms,
                    ns_per_op: if total_ops > 0 {
                        conc.elapsed_ms * 1_000_000.0 / total_ops as f64
                    } else {
                        0.0
                    },
                    ops_per_sec: conc.ops_per_sec,
                    cpu_ms: cpu1.saturating_sub(cpu0),
                    rss_delta_kb: 0,
                    read_kb: io1.0.saturating_sub(io0.0) / 1024,
                    write_kb: io1.1.saturating_sub(io0.1) / 1024,
                    disk_kb,
                    disk_bytes_per_op: if total_ops > 0 {
                        (disk_kb * 1024) as f64 / total_ops as f64
                    } else {
                        0.0
                    },
                    fsync_count: env.stat_fsync_count().saturating_sub(fsync0),
                };
                print_progress(&r);
                results.push(r);
                drop(db);
                drop(env);
            }

            // Group commit variant: leader coalesces fsyncs from concurrent committers.
            {
                let dir =
                    new_bench_dir(&bench_base, "bench_txn_gc", n, cleanup);
                let (env, db) = open_db_group_commit(dir.path());
                let fsync0 = env.stat_fsync_count();
                let cpu0 = cpu_time_ms();
                let io0 = proc_io();
                let conc = concurrent::run_concurrent_txn(
                    &env,
                    &db,
                    wthreads,
                    ops_per_thread,
                    value_size,
                );
                let cpu1 = cpu_time_ms();
                let io1 = proc_io();
                let disk_kb = dir_size_kb(dir.path());
                let total_ops = conc.total_ops as usize;
                let r = WorkloadResult {
                    workload: "w10_txn_group_commit".to_string(),
                    scale: n,
                    threads: wthreads,
                    elapsed_ms: conc.elapsed_ms,
                    ns_per_op: if total_ops > 0 {
                        conc.elapsed_ms * 1_000_000.0 / total_ops as f64
                    } else {
                        0.0
                    },
                    ops_per_sec: conc.ops_per_sec,
                    cpu_ms: cpu1.saturating_sub(cpu0),
                    rss_delta_kb: 0,
                    read_kb: io1.0.saturating_sub(io0.0) / 1024,
                    write_kb: io1.1.saturating_sub(io0.1) / 1024,
                    disk_kb,
                    disk_bytes_per_op: if total_ops > 0 {
                        (disk_kb * 1024) as f64 / total_ops as f64
                    } else {
                        0.0
                    },
                    fsync_count: env.stat_fsync_count().saturating_sub(fsync0),
                };
                print_progress(&r);
                results.push(r);
                drop(db);
                drop(env);
            }
        }

        // W11: recovery/startup time
        // Pre-populate outside the timer; time only the re-open.
        // Both Noxu and full 3-phase recovery (analysis + redo + undo)
        // on Environment::open().  Any speedup vs lower per-entry
        // log-replay overhead in Rust (no JVM startup, no classloading, no
        // JIT warmup) and not a missing recovery step.
        {
            let dir = new_bench_dir(&bench_base, "bench_12", n, cleanup);
            {
                let (env_pre, db_pre) = open_db(dir.path());
                populate(&db_pre, n, &value_bytes);
                drop(db_pre);
                drop(env_pre);
            }
            // Time only the re-open; env is closed before and after — no file-lock conflict.
            let r = run_timed("w11_recovery", n, 1, dir.path(), None, || {
                let (env2, db2) = open_db(dir.path());
                drop(db2);
                drop(env2);
                1
            });
            print_progress(&r);
            results.push(r);
        }

        // W12: XA two-phase commit throughput
        // Measures the overhead of XA 2PC vs single-phase commit.
        {
            let xa_n = n.min(10_000); // cap XA ops for sanity at large scale

            // W12a: full 2PC (start → end → prepare → commit)
            {
                let dir =
                    new_bench_dir(&bench_base, "bench_xa_2pc", n, cleanup);
                let (env, db) = open_db(dir.path());
                let xa = XaEnvironment::new(env);
                let r = run_timed(
                    "w12_xa_2pc",
                    xa_n,
                    1,
                    dir.path(),
                    Some(xa.inner()),
                    || workloads::w12_xa_2pc(&xa, &db, xa_n, &value_bytes),
                );
                print_progress(&r);
                results.push(r);
                drop(db);
            }

            // W12b: single-phase commit (ONEPHASE optimization)
            {
                let dir =
                    new_bench_dir(&bench_base, "bench_xa_1pc", n, cleanup);
                let (env, db) = open_db(dir.path());
                let xa = XaEnvironment::new(env);
                let r = run_timed(
                    "w12_xa_1pc",
                    xa_n,
                    1,
                    dir.path(),
                    Some(xa.inner()),
                    || workloads::w12_xa_1pc(&xa, &db, xa_n, &value_bytes),
                );
                print_progress(&r);
                results.push(r);
                drop(db);
            }

            // W12c: plain txn baseline (for comparison)
            {
                let dir =
                    new_bench_dir(&bench_base, "bench_xa_baseline", n, cleanup);
                let (env, db) = open_db(dir.path());
                populate(&db, xa_n, &value_bytes);
                let r = run_timed(
                    "w12_plain_txn",
                    xa_n,
                    1,
                    dir.path(),
                    Some(&env),
                    || workloads::w09_txn_multi(&env, &db, xa_n, &value_bytes),
                );
                print_progress(&r);
                results.push(r);
                drop(db);
                drop(env);
            }
        }

        // W13: sorted-dup secondary index walk (Wave 11-B).  Skipped at
        // scales > 10K because the documented sorted-dup cursor bugs
        // (see workloads.rs W13 module comment) make the run
        // untrustworthy at high dup counts and the safety cap dominates.
        // The setup (primary populate + secondary populate) runs *outside*
        // the timer so ns/op reflects the cursor walk only.
        if n <= 10_000 {
            let dir = new_bench_dir(&bench_base, "bench_w13", n, cleanup);
            let (env_w13, _primary_w13, secondary_w13) =
                workloads::w13_setup(dir.path(), n, &value_bytes);
            let r = run_timed(
                "w13_sec_dup_walk",
                n,
                1,
                dir.path(),
                Some(&env_w13),
                || workloads::w13_secondary_dup_walk(&secondary_w13, n),
            );
            print_progress(&r);
            results.push(r);
            drop(secondary_w13);
            drop(env_w13);
        }
    }

    // ── Print table ───────────────────────────────────────────────────────────
    let hdr = format!(
        "{:<26} {:>8} {:>7} {:>10} {:>12} {:>12} {:>8} {:>9} {:>8} {:>8} {:>8} {:>9} {:>8}",
        "Workload",
        "Scale",
        "Threads",
        "Time(ms)",
        "ns/op",
        "ops/sec",
        "CPU(ms)",
        "RSS_d(KB)",
        "rIO(KB)",
        "wIO(KB)",
        "Disk(KB)",
        "B/op",
        "Fsyncs"
    );
    println!("\n{}", "=".repeat(hdr.len()));
    println!("{hdr}");
    println!("{}", "-".repeat(hdr.len()));
    for r in &results {
        println!(
            "{:<26} {:>8} {:>7} {:>10.1} {:>12.0} {:>12.0} {:>8} {:>9} {:>8} {:>8} {:>8} {:>9.1} {:>8}",
            r.workload,
            r.scale,
            r.threads,
            r.elapsed_ms,
            r.ns_per_op,
            r.ops_per_sec,
            r.cpu_ms,
            r.rss_delta_kb,
            r.read_kb,
            r.write_kb,
            r.disk_kb,
            r.disk_bytes_per_op,
            r.fsync_count
        );
    }
    println!("{}", "=".repeat(hdr.len()));

    // ── Write CSV ─────────────────────────────────────────────────────────────
    let csv_dir = Path::new("benches/results");
    fs::create_dir_all(csv_dir).unwrap();
    let mut f = fs::File::create(csv_dir.join("noxu_results.csv")).unwrap();
    writeln!(
        f,
        "engine,workload,scale,threads,elapsed_ms,ns_per_op,ops_per_sec,\
         cpu_time_ms,rss_delta_kb,read_kb,write_kb,disk_kb,disk_bytes_per_op,fsync_count"
    )
    .unwrap();
    for r in &results {
        writeln!(
            f,
            "noxu,{},{},{},{:.3},{:.1},{:.0},{},{},{},{},{},{:.2},{}",
            r.workload,
            r.scale,
            r.threads,
            r.elapsed_ms,
            r.ns_per_op,
            r.ops_per_sec,
            r.cpu_ms,
            r.rss_delta_kb,
            r.read_kb,
            r.write_kb,
            r.disk_kb,
            r.disk_bytes_per_op,
            r.fsync_count
        )
        .unwrap();
    }
    println!("\nCSV written to benches/results/noxu_results.csv");
}
