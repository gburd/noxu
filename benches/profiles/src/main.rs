//! Single-workload profiler for wave 11-H.
//!
//! Runs a single workload at a single scale, with populate phase outside
//! the timed window for read workloads.  Designed for use under `perf
//! record`/`perf report`/`flamegraph`/`samply`.
//!
//! Usage:
//!   noxu-perf-profiler --workload <w03|w04|w10|w11> --scale <N>
//!     [--threads <T>]              (W10 only, default 8)
//!     [--repeats <R>]              (default 1)
//!
//! W03/W04 pre-populate then loop reads (the profiler hot path is the
//! read loop only).
//!
//! W10 spawns reader+writer threads doing concurrent puts/gets after a
//! populate phase.
//!
//! W11 populates, closes the env, then re-opens it so recovery is the
//! only timed work.

use noxu_db::{
    Database, DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig,
};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;
use tempfile::TempDir;

#[inline]
fn make_key(i: usize) -> Vec<u8> {
    format!("{:010}", i).into_bytes()
}

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

fn populate(db: &Database, n: usize, value: &[u8]) {
    for i in 0..n {
        let k = DatabaseEntry::from_vec(make_key(i));
        let v = DatabaseEntry::from_bytes(value);
        db.put(None, &k, &v).unwrap();
    }
}

fn run_w03(scale: usize, repeats: usize) {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_db(dir.path());
    populate(&db, scale, &[0x58u8; 64]);

    eprintln!("[w03] populate done; entering timed read loop");
    let t0 = Instant::now();
    let mut total = 0usize;
    for _ in 0..repeats {
        let mut data = DatabaseEntry::new();
        for i in 0..scale {
            let k = DatabaseEntry::from_vec(make_key(i));
            let _ = db.get(None, &k, &mut data).unwrap();
            total += 1;
        }
    }
    let elapsed = t0.elapsed();
    let ops_s = total as f64 / elapsed.as_secs_f64();
    eprintln!(
        "[w03] {} ops in {:?}  =>  {:.0} ops/s  ({:.0} ns/op)",
        total,
        elapsed,
        ops_s,
        elapsed.as_nanos() as f64 / total as f64
    );
    drop(db);
    drop(env);
}

fn run_w04(scale: usize, repeats: usize) {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_db(dir.path());
    populate(&db, scale, &[0x58u8; 64]);

    eprintln!("[w04] populate done; entering timed read loop");
    let t0 = Instant::now();
    let mut total = 0usize;
    let mut rng = SmallRng::seed_from_u64(99);
    let mut data = DatabaseEntry::new();
    for _ in 0..repeats {
        for _ in 0..scale {
            let idx = rng.gen_range(0..scale);
            let k = DatabaseEntry::from_vec(make_key(idx));
            let _ = db.get(None, &k, &mut data).unwrap();
            total += 1;
        }
    }
    let elapsed = t0.elapsed();
    let ops_s = total as f64 / elapsed.as_secs_f64();
    eprintln!(
        "[w04] {} ops in {:?}  =>  {:.0} ops/s  ({:.0} ns/op)",
        total,
        elapsed,
        ops_s,
        elapsed.as_nanos() as f64 / total as f64
    );
    drop(db);
    drop(env);
}

fn run_w10(scale: usize, threads: usize, repeats: usize) {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::thread;

    let dir = TempDir::new().unwrap();
    let (env, db) = open_db(dir.path());
    populate(&db, scale, &[0x58u8; 64]);

    let db = Arc::new(db);
    eprintln!(
        "[w10] populate done; spawning {} writer threads, {} ops/thread x {} repeats",
        threads,
        scale / threads,
        repeats
    );

    let total = AtomicU64::new(0);
    let total_ref = &total;
    let t0 = Instant::now();
    thread::scope(|s| {
        for tid in 0..threads {
            let db_t = Arc::clone(&db);
            s.spawn(move || {
                let mut rng = SmallRng::seed_from_u64(0xc0ffee + tid as u64);
                let v = DatabaseEntry::from_bytes(&[0x58u8; 64]);
                let n_per = (scale / threads) * repeats;
                for _ in 0..n_per {
                    let idx = rng.gen_range(0..scale);
                    let k = DatabaseEntry::from_vec(make_key(idx));
                    if rng.r#gen::<bool>() {
                        let mut data = DatabaseEntry::new();
                        let _ = db_t.get(None, &k, &mut data).unwrap();
                    } else {
                        let _ = db_t.put(None, &k, &v).unwrap();
                    }
                    total_ref.fetch_add(1, Ordering::Relaxed);
                }
            });
        }
    });
    let elapsed = t0.elapsed();
    let total_ops = total.load(Ordering::Relaxed);
    let ops_s = total_ops as f64 / elapsed.as_secs_f64();
    eprintln!(
        "[w10] {} ops in {:?}  =>  {:.0} ops/s",
        total_ops, elapsed, ops_s
    );
    drop(db);
    drop(env);
}

fn run_w11(scale: usize, repeats: usize) {
    let dir = TempDir::new().unwrap();
    {
        let (env, db) = open_db(dir.path());
        populate(&db, scale, &[0x58u8; 64]);
        drop(db);
        drop(env);
    }

    eprintln!(
        "[w11] populate done; timing {} re-opens (recovery path)",
        repeats
    );
    let t0 = Instant::now();
    for _ in 0..repeats {
        let (env, db) = open_db(dir.path());
        drop(db);
        drop(env);
    }
    let elapsed = t0.elapsed();
    eprintln!(
        "[w11] {} re-opens in {:?}  =>  {:.1} ms each",
        repeats,
        elapsed,
        elapsed.as_secs_f64() * 1000.0 / repeats as f64
    );
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut workload = "w03".to_string();
    let mut scale: usize = 10_000;
    let mut threads: usize = 8;
    let mut repeats: usize = 1;
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--workload" => {
                workload = args[i + 1].clone();
                i += 2;
            }
            "--scale" => {
                scale = args[i + 1].parse().unwrap();
                i += 2;
            }
            "--threads" => {
                threads = args[i + 1].parse().unwrap();
                i += 2;
            }
            "--repeats" => {
                repeats = args[i + 1].parse().unwrap();
                i += 2;
            }
            _ => {
                eprintln!("ignoring arg: {}", args[i]);
                i += 1;
            }
        }
    }

    eprintln!(
        "noxu-perf-profiler: workload={} scale={} threads={} repeats={}",
        workload, scale, threads, repeats
    );

    match workload.as_str() {
        "w03" => run_w03(scale, repeats),
        "w04" => run_w04(scale, repeats),
        "w10" => run_w10(scale, threads, repeats),
        "w11" => run_w11(scale, repeats),
        other => {
            eprintln!("unknown workload {}", other);
            std::process::exit(2);
        }
    }
}
