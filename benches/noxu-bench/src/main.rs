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
use noxu_db::{Database, DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig};
use std::fs;
use std::io::Write as IoWrite;
use std::path::Path;
use std::sync::Arc;
use tempfile::TempDir;

const VALUE: &[u8] = b"noxu-workload-bench-value-XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX";

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

fn open_db(dir: &Path) -> (Environment, Database) {
    let cfg = EnvironmentConfig::new(dir.to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    let env = Environment::open(cfg).unwrap();
    let db = env
        .open_database(None, "bench", &DatabaseConfig::new().with_allow_create(true))
        .unwrap();
    (env, db)
}

fn populate(db: &Database, n: usize) {
    for i in 0..n {
        let k = DatabaseEntry::from_vec(format!("{:010}", i).into_bytes());
        let v = DatabaseEntry::from_bytes(VALUE);
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
    /// Number of fdatasync calls during this workload (port of JE nFSyncs stat).
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

    let disk_bytes_per_op = if ops > 0 { (disk_kb * 1024) as f64 / ops as f64 } else { 0.0 };

    WorkloadResult {
        workload: workload_name.into(),
        scale,
        threads,
        elapsed_ms,
        ns_per_op: if ops > 0 { elapsed_ms * 1_000_000.0 / ops as f64 } else { 0.0 },
        ops_per_sec: if elapsed_ms > 0.0 { ops as f64 / (elapsed_ms / 1000.0) } else { 0.0 },
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
    // Five scales: 1K, 10K, 100K, 500K, 1M
    // NOXU_MAX_SCALE env var limits the run (e.g. NOXU_MAX_SCALE=10000).
    let max_scale: usize = std::env::var("NOXU_MAX_SCALE")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&v| v > 0)
        .unwrap_or(usize::MAX);
    let all_scales: &[usize] = &[1_000, 10_000, 100_000, 500_000, 1_000_000];
    let scales: Vec<usize> = all_scales.iter().copied().filter(|&s| s <= max_scale).collect();
    let scales: &[usize] = &scales;

    // W10 concurrent configurations: (label, reader_threads, writer_threads)
    let concurrent_configs: &[(&str, usize, usize)] = &[
        ("w10_conc_1r0w", 1, 0),  // read-only,  1 thread
        ("w10_conc_0r1w", 0, 1),  // write-only, 1 thread
        ("w10_conc_4r0w", 4, 0),  // read-only,  4 threads
        ("w10_conc_0r4w", 0, 4),  // write-only, 4 threads
        ("w10_conc_4r4w", 4, 4),  // mixed,      8 threads
        ("w10_conc_8r8w", 8, 8),  // heavy,     16 threads
    ];

    let mut results: Vec<WorkloadResult> = Vec::new();

    for &n in scales {
        println!("\n══ Scale: {} ══", n);

        // W01: sequential write
        {
            let dir = TempDir::new().unwrap();
            let (env, db) = open_db(dir.path());
            let r = run_timed("w01_seq_write", n, 1, dir.path(), Some(&env), || {
                workloads::w01_seq_write(&db, n)
            });
            print_progress(&r);
            results.push(r);
            drop(db); drop(env);
        }

        // W02: random write
        {
            let dir = TempDir::new().unwrap();
            let (env, db) = open_db(dir.path());
            let r = run_timed("w02_rand_write", n, 1, dir.path(), Some(&env), || {
                workloads::w02_rand_write(&db, n)
            });
            print_progress(&r);
            results.push(r);
            drop(db); drop(env);
        }

        // W03: sequential read (pre-populate)
        {
            let dir = TempDir::new().unwrap();
            let (env, db) = open_db(dir.path());
            populate(&db, n);
            let r = run_timed("w03_seq_read", n, 1, dir.path(), Some(&env), || {
                workloads::w03_seq_read(&db, n)
            });
            print_progress(&r);
            results.push(r);
            drop(db); drop(env);
        }

        // W04: random read
        {
            let dir = TempDir::new().unwrap();
            let (env, db) = open_db(dir.path());
            populate(&db, n);
            let r = run_timed("w04_rand_read", n, 1, dir.path(), Some(&env), || {
                workloads::w04_rand_read(&db, n)
            });
            print_progress(&r);
            results.push(r);
            drop(db); drop(env);
        }

        // W05: range scan
        {
            let dir = TempDir::new().unwrap();
            let (env, db) = open_db(dir.path());
            populate(&db, n);
            let r = run_timed("w05_range_scan", n, 1, dir.path(), Some(&env), || {
                workloads::w05_range_scan(&db, n)
            });
            print_progress(&r);
            results.push(r);
            drop(db); drop(env);
        }

        // W06: write-heavy mixed (90% write / 10% read)
        {
            let dir = TempDir::new().unwrap();
            let (env, db) = open_db(dir.path());
            populate(&db, n);
            let r = run_timed("w06_write_heavy", n, 1, dir.path(), Some(&env), || {
                workloads::w06_write_heavy(&db, n)
            });
            print_progress(&r);
            results.push(r);
            drop(db); drop(env);
        }

        // W07: read-heavy mixed (90% read / 10% write)
        {
            let dir = TempDir::new().unwrap();
            let (env, db) = open_db(dir.path());
            populate(&db, n);
            let r = run_timed("w07_read_heavy", n, 1, dir.path(), Some(&env), || {
                workloads::w07_read_heavy(&db, n)
            });
            print_progress(&r);
            results.push(r);
            drop(db); drop(env);
        }

        // W08: delete + insert pairs
        {
            let dir = TempDir::new().unwrap();
            let (env, db) = open_db(dir.path());
            populate(&db, n);
            let r = run_timed("w08_delete_insert", n, 1, dir.path(), Some(&env), || {
                workloads::w08_delete_insert(&db, n)
            });
            print_progress(&r);
            results.push(r);
            drop(db); drop(env);
        }

        // W09: transactional multi-op (3 gets + 2 puts per txn)
        //
        // get(key i) + put(key i) in the same transaction triggers the READ→WRITE
        // lock upgrade path (LockUpgrade::WritePromote).  ThinLockImpl handles
        // this correctly: the upgrade is granted immediately as LockGrantType::Promotion.
        {
            let w09_n = n;
            let dir = TempDir::new().unwrap();
            let (env, db) = open_db(dir.path());
            populate(&db, w09_n);
            let r = run_timed("w09_txn_multi", w09_n, 1, dir.path(), Some(&env), || {
                workloads::w09_txn_multi(&env, &db, w09_n)
            });
            print_progress(&r);
            results.push(r);
            drop(db); drop(env);
        }

        // W10: concurrent — six thread configurations
        for &(label, rthreads, wthreads) in concurrent_configs {
            let total_threads = rthreads + wthreads;
            // Cap ops at 100K when writer threads > 4 at large scale to keep runtime sane
            let ops_n = if n > 100_000 && wthreads > 4 { 100_000 } else { n };

            let dir = TempDir::new().unwrap();
            let (env, db) = open_db(dir.path());
            populate(&db, ops_n);

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
                } else { 0.0 },
                ops_per_sec: conc.ops_per_sec,
                cpu_ms: cpu1.saturating_sub(cpu0),
                rss_delta_kb: rss1 - rss0,
                read_kb: io1.0.saturating_sub(io0.0) / 1024,
                write_kb: io1.1.saturating_sub(io0.1) / 1024,
                disk_kb,
                disk_bytes_per_op: if total_ops > 0 {
                    (disk_kb * 1024) as f64 / total_ops as f64
                } else { 0.0 },
                fsync_count: env.stat_fsync_count().saturating_sub(fsync0),
            };
            print_progress(&r);
            results.push(r);

            drop(db_arc);
            drop(env);
        }

        // W11: recovery/startup time
        // Pre-populate outside the timer; time only the re-open.
        // Both Noxu and JE run full 3-phase recovery (analysis + redo + undo)
        // on Environment::open().  Any speedup vs JE reflects lower per-entry
        // log-replay overhead in Rust (no JVM startup, no classloading, no
        // JIT warmup) and not a missing recovery step.
        {
            let dir = TempDir::new().unwrap();
            {
                let (env_pre, db_pre) = open_db(dir.path());
                populate(&db_pre, n);
                drop(db_pre); drop(env_pre);
            }
            // Time only the re-open; env is closed before and after — no file-lock conflict.
            let r = run_timed("w11_recovery", n, 1, dir.path(), None, || {
                let (env2, db2) = open_db(dir.path());
                drop(db2); drop(env2);
                1
            });
            print_progress(&r);
            results.push(r);
        }
    }

    // ── Print table ───────────────────────────────────────────────────────────
    let hdr = format!(
        "{:<26} {:>8} {:>7} {:>10} {:>12} {:>12} {:>8} {:>9} {:>8} {:>8} {:>8} {:>9} {:>8}",
        "Workload", "Scale", "Threads", "Time(ms)",
        "ns/op", "ops/sec", "CPU(ms)", "RSS_d(KB)",
        "rIO(KB)", "wIO(KB)", "Disk(KB)", "B/op", "Fsyncs"
    );
    println!("\n{}", "=".repeat(hdr.len()));
    println!("{hdr}");
    println!("{}", "-".repeat(hdr.len()));
    for r in &results {
        println!(
            "{:<26} {:>8} {:>7} {:>10.1} {:>12.0} {:>12.0} {:>8} {:>9} {:>8} {:>8} {:>8} {:>9.1} {:>8}",
            r.workload, r.scale, r.threads,
            r.elapsed_ms, r.ns_per_op, r.ops_per_sec,
            r.cpu_ms, r.rss_delta_kb,
            r.read_kb, r.write_kb, r.disk_kb,
            r.disk_bytes_per_op, r.fsync_count
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
            r.workload, r.scale, r.threads,
            r.elapsed_ms, r.ns_per_op, r.ops_per_sec,
            r.cpu_ms, r.rss_delta_kb,
            r.read_kb, r.write_kb, r.disk_kb,
            r.disk_bytes_per_op, r.fsync_count
        )
        .unwrap();
    }
    println!("\nCSV written to benches/results/noxu_results.csv");
}
