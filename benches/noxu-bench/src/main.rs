//! Noxu DB standalone workload benchmark.
//!
//! Runs 10 diverse workloads at three scales (1_000, 10_000, 100_000),
//! prints a human-readable table to stdout, and writes results to
//! `benches/results/noxu_results.csv`.
//!
//! Run with:
//!   cargo run --bin noxu-workload-bench --release
//!
//! # Caveat
//! Noxu DB 0.1 does not yet implement WAL writes, blocking lock acquisition,
//! or B-tree merge/compress.  Put() benchmarks measure the in-memory B-tree
//! path only and will be faster than a complete implementation.

#![allow(dead_code)]

mod concurrent;
mod metrics;
mod workloads;

use metrics::{dir_size_kb, proc_io, rss_kb};
use noxu_db::{Database, DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig};
use std::fs;
use std::io::Write as IoWrite;
use std::path::Path;
use std::sync::Arc;
use tempfile::TempDir;

/// Fixed 64-byte benchmark value used for population.
const VALUE: &[u8] = b"noxu-workload-bench-value-XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX";

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Open a fresh Noxu environment in `dir`.
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

/// Pre-populate `db` with `n` sequential records (keys 0..n).
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
    workload: &'static str,
    scale: usize,
    threads: usize,
    elapsed_ms: f64,
    ns_per_op: f64,
    ops_per_sec: f64,
    rss_delta_kb: i64,
    read_kb: u64,
    write_kb: u64,
    disk_kb: u64,
}

// ─────────────────────────────────────────────────────────────────────────────
// Timing wrapper
// ─────────────────────────────────────────────────────────────────────────────

/// Execute `f`, measuring elapsed time and I/O delta, then return a
/// `WorkloadResult`.  `data_dir` is measured for disk usage after the call.
fn run_timed<F: FnOnce() -> usize>(
    workload_name: &'static str,
    scale: usize,
    threads: usize,
    data_dir: &Path,
    before_rss: i64,
    before_io: (u64, u64),
    f: F,
) -> WorkloadResult {
    let t0 = std::time::Instant::now();
    let ops = f();
    let elapsed_ms = t0.elapsed().as_secs_f64() * 1000.0;

    let after_rss = rss_kb();
    let after_io = proc_io();
    let disk_kb = dir_size_kb(data_dir);

    WorkloadResult {
        workload: workload_name,
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
        rss_delta_kb: after_rss - before_rss,
        read_kb: after_io.0.saturating_sub(before_io.0) / 1024,
        write_kb: after_io.1.saturating_sub(before_io.1) / 1024,
        disk_kb,
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// main
// ─────────────────────────────────────────────────────────────────────────────

fn main() {
    let scales: &[usize] = &[1_000, 10_000, 100_000];
    let mut results: Vec<WorkloadResult> = Vec::new();

    for &n in scales {
        println!("--- Scale: {} ---", n);

        // ── W01: sequential write ─────────────────────────────────────────
        {
            let dir = TempDir::new().unwrap();
            let (env, db) = open_db(dir.path());
            let rss0 = rss_kb();
            let io0 = proc_io();
            let r = run_timed("w01_seq_write", n, 1, dir.path(), rss0, io0, || {
                workloads::w01_seq_write(&db, n)
            });
            println!(
                "  w01_seq_write    n={} : {:.1}ms  {:.0} ops/s",
                n, r.elapsed_ms, r.ops_per_sec
            );
            results.push(r);
            drop(db);
            drop(env);
        }

        // ── W02: random write ─────────────────────────────────────────────
        {
            let dir = TempDir::new().unwrap();
            let (env, db) = open_db(dir.path());
            let rss0 = rss_kb();
            let io0 = proc_io();
            let r = run_timed("w02_rand_write", n, 1, dir.path(), rss0, io0, || {
                workloads::w02_rand_write(&db, n)
            });
            println!(
                "  w02_rand_write   n={} : {:.1}ms  {:.0} ops/s",
                n, r.elapsed_ms, r.ops_per_sec
            );
            results.push(r);
            drop(db);
            drop(env);
        }

        // ── W03: sequential read (pre-populate) ───────────────────────────
        {
            let dir = TempDir::new().unwrap();
            let (env, db) = open_db(dir.path());
            populate(&db, n);
            let rss0 = rss_kb();
            let io0 = proc_io();
            let r = run_timed("w03_seq_read", n, 1, dir.path(), rss0, io0, || {
                workloads::w03_seq_read(&db, n)
            });
            println!(
                "  w03_seq_read     n={} : {:.1}ms  {:.0} ops/s",
                n, r.elapsed_ms, r.ops_per_sec
            );
            results.push(r);
            drop(db);
            drop(env);
        }

        // ── W04: random read ──────────────────────────────────────────────
        {
            let dir = TempDir::new().unwrap();
            let (env, db) = open_db(dir.path());
            populate(&db, n);
            let rss0 = rss_kb();
            let io0 = proc_io();
            let r = run_timed("w04_rand_read", n, 1, dir.path(), rss0, io0, || {
                workloads::w04_rand_read(&db, n)
            });
            println!(
                "  w04_rand_read    n={} : {:.1}ms  {:.0} ops/s",
                n, r.elapsed_ms, r.ops_per_sec
            );
            results.push(r);
            drop(db);
            drop(env);
        }

        // ── W05: range scan ───────────────────────────────────────────────
        {
            let dir = TempDir::new().unwrap();
            let (env, db) = open_db(dir.path());
            populate(&db, n);
            let rss0 = rss_kb();
            let io0 = proc_io();
            let r = run_timed("w05_range_scan", n, 1, dir.path(), rss0, io0, || {
                workloads::w05_range_scan(&db, n)
            });
            println!(
                "  w05_range_scan   n={} : {:.1}ms  {:.0} ops/s",
                n, r.elapsed_ms, r.ops_per_sec
            );
            results.push(r);
            drop(db);
            drop(env);
        }

        // ── W06: write-heavy mixed ────────────────────────────────────────
        {
            let dir = TempDir::new().unwrap();
            let (env, db) = open_db(dir.path());
            populate(&db, n);
            let rss0 = rss_kb();
            let io0 = proc_io();
            let r = run_timed("w06_write_heavy", n, 1, dir.path(), rss0, io0, || {
                workloads::w06_write_heavy(&db, n)
            });
            println!(
                "  w06_write_heavy  n={} : {:.1}ms  {:.0} ops/s",
                n, r.elapsed_ms, r.ops_per_sec
            );
            results.push(r);
            drop(db);
            drop(env);
        }

        // ── W07: read-heavy mixed ─────────────────────────────────────────
        {
            let dir = TempDir::new().unwrap();
            let (env, db) = open_db(dir.path());
            populate(&db, n);
            let rss0 = rss_kb();
            let io0 = proc_io();
            let r = run_timed("w07_read_heavy", n, 1, dir.path(), rss0, io0, || {
                workloads::w07_read_heavy(&db, n)
            });
            println!(
                "  w07_read_heavy   n={} : {:.1}ms  {:.0} ops/s",
                n, r.elapsed_ms, r.ops_per_sec
            );
            results.push(r);
            drop(db);
            drop(env);
        }

        // ── W08: delete + insert (only up to n=10_000) ───────────────────
        if n <= 10_000 {
            let dir = TempDir::new().unwrap();
            let (env, db) = open_db(dir.path());
            populate(&db, n);
            let rss0 = rss_kb();
            let io0 = proc_io();
            let r = run_timed("w08_delete_insert", n, 1, dir.path(), rss0, io0, || {
                workloads::w08_delete_insert(&db, n)
            });
            println!(
                "  w08_delete_insert n={} : {:.1}ms  {:.0} ops/s",
                n, r.elapsed_ms, r.ops_per_sec
            );
            results.push(r);
            drop(db);
            drop(env);
        }

        // ── W09: multi-op transaction (only up to n=10_000) ──────────────
        if n <= 10_000 {
            let dir = TempDir::new().unwrap();
            let (env, db) = open_db(dir.path());
            populate(&db, n);
            let rss0 = rss_kb();
            let io0 = proc_io();
            let r = run_timed("w09_txn_multi", n, 1, dir.path(), rss0, io0, || {
                workloads::w09_txn_multi(&env, &db, n)
            });
            println!(
                "  w09_txn_multi    n={} : {:.1}ms  {:.0} ops/s",
                n, r.elapsed_ms, r.ops_per_sec
            );
            results.push(r);
            drop(db);
            drop(env);
        }

        // ── W10: concurrent (4 readers + 4 writers, only n>=10_000) ──────
        if n >= 10_000 {
            let dir = TempDir::new().unwrap();
            let (env, db) = open_db(dir.path());
            populate(&db, n);

            let db_arc = Arc::new(db);
            let rss0 = rss_kb();
            let io0 = proc_io();

            let conc =
                concurrent::run_concurrent(Arc::clone(&db_arc), 4, 4, n / 8);

            let after_rss = rss_kb();
            let after_io = proc_io();
            let disk_kb = dir_size_kb(dir.path());

            let r = WorkloadResult {
                workload: "w10_concurrent",
                scale: n,
                threads: 8,
                elapsed_ms: conc.elapsed_ms,
                ns_per_op: if conc.total_ops > 0 {
                    conc.elapsed_ms * 1_000_000.0 / conc.total_ops as f64
                } else {
                    0.0
                },
                ops_per_sec: conc.ops_per_sec,
                rss_delta_kb: after_rss - rss0,
                read_kb: after_io.0.saturating_sub(io0.0) / 1024,
                write_kb: after_io.1.saturating_sub(io0.1) / 1024,
                disk_kb,
            };
            println!(
                "  w10_concurrent   n={} : {:.1}ms  {:.0} ops/s",
                n, r.elapsed_ms, r.ops_per_sec
            );
            results.push(r);

            // db_arc must be dropped before env to avoid a double-close panic.
            drop(db_arc);
            drop(env);
        }
    }

    // ── Print pretty table ────────────────────────────────────────────────────
    println!("\n{}", "=".repeat(110));
    println!(
        "{:<20} {:>8} {:>8} {:>10} {:>12} {:>12} {:>10} {:>9} {:>9} {:>9}",
        "Workload",
        "Scale",
        "Threads",
        "Time(ms)",
        "ns/op",
        "ops/sec",
        "DRSS(KB)",
        "rIO(KB)",
        "wIO(KB)",
        "Disk(KB)"
    );
    println!("{}", "-".repeat(110));
    for r in &results {
        println!(
            "{:<20} {:>8} {:>8} {:>10.1} {:>12.0} {:>12.0} {:>10} {:>9} {:>9} {:>9}",
            r.workload,
            r.scale,
            r.threads,
            r.elapsed_ms,
            r.ns_per_op,
            r.ops_per_sec,
            r.rss_delta_kb,
            r.read_kb,
            r.write_kb,
            r.disk_kb
        );
    }
    println!("{}", "=".repeat(110));

    // ── Write CSV ─────────────────────────────────────────────────────────────
    let csv_dir = Path::new("benches/results");
    fs::create_dir_all(csv_dir).unwrap();
    let mut f = fs::File::create(csv_dir.join("noxu_results.csv")).unwrap();
    writeln!(
        f,
        "engine,workload,scale,threads,elapsed_ms,ns_per_op,ops_per_sec,\
         rss_delta_kb,read_kb,write_kb,disk_kb"
    )
    .unwrap();
    for r in &results {
        writeln!(
            f,
            "noxu,{},{},{},{:.3},{:.1},{:.0},{},{},{},{}",
            r.workload,
            r.scale,
            r.threads,
            r.elapsed_ms,
            r.ns_per_op,
            r.ops_per_sec,
            r.rss_delta_kb,
            r.read_kb,
            r.write_kb,
            r.disk_kb
        )
        .unwrap();
    }
    println!("\nCSV written to benches/results/noxu_results.csv");
}
