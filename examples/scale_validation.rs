//! Scale validation example.
//!
//! Inserts a large number of records in parallel, checkpoints, reopens via
//! WAL recovery, and verifies correctness.  Prints throughput, memory usage,
//! and log directory size.  Intended as a manual pre-production check, not
//! automated CI.
//!
//! # Usage
//!
//! ```text
//! cargo run --example scale_validation -- \
//!     --records 1000000 --threads 8 --dir /scratch/noxu_scale
//! ```
//!
//! # Defaults
//!
//! | Flag        | Default                     |
//! |-------------|----------------------------|
//! | `--records` | `1_000_000`                |
//! | `--threads` | `8`                        |
//! | `--dir`     | temp dir under `/scratch`  |

use noxu::{
    DatabaseConfig, DatabaseEntry, EnvironmentConfig, Get, OperationStatus,
};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// CLI helpers
// ---------------------------------------------------------------------------

struct Args {
    records: u64,
    threads: usize,
    dir: Option<PathBuf>,
}

fn parse_args() -> Args {
    let mut args = std::env::args().skip(1);
    let mut records: u64 = 1_000_000;
    let mut threads: usize = 8;
    let mut dir: Option<PathBuf> = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--records" => {
                records = args
                    .next()
                    .expect("--records requires a value")
                    .parse()
                    .expect("--records must be a positive integer");
            }
            "--threads" => {
                threads = args
                    .next()
                    .expect("--threads requires a value")
                    .parse()
                    .expect("--threads must be a positive integer");
                assert!(threads >= 1, "--threads must be ≥ 1");
            }
            "--dir" => {
                dir = Some(PathBuf::from(
                    args.next().expect("--dir requires a path"),
                ));
            }
            other => {
                eprintln!("Unknown argument: {other}");
                std::process::exit(1);
            }
        }
    }
    Args { records, threads, dir }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn dir_size_bytes(path: &Path) -> u64 {
    let mut total = 0u64;
    if let Ok(rd) = std::fs::read_dir(path) {
        for entry in rd.flatten() {
            if let Ok(meta) = entry.metadata() {
                total += meta.len();
            }
        }
    }
    total
}

/// Read peak resident set size from `/proc/self/status` (Linux only).
fn peak_rss_bytes() -> Option<u64> {
    let text = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("VmPeak:") {
            let kb: u64 = rest.split_whitespace().next()?.parse().ok()?;
            return Some(kb * 1024);
        }
    }
    None
}

fn human_bytes(b: u64) -> String {
    if b >= 1 << 30 {
        format!("{:.2} GiB", b as f64 / (1u64 << 30) as f64)
    } else if b >= 1 << 20 {
        format!("{:.2} MiB", b as f64 / (1u64 << 20) as f64)
    } else {
        format!("{:.2} KiB", b as f64 / 1024.0)
    }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = parse_args();

    // ------------------------------------------------------------------
    // Create working directory
    // ------------------------------------------------------------------
    let (db_path, _cleanup_on_exit): (PathBuf, bool) =
        if let Some(ref p) = args.dir {
            std::fs::create_dir_all(p)?;
            (p.clone(), false)
        } else {
            let p = PathBuf::from(format!(
                "/scratch/noxu_scale_{}",
                std::process::id()
            ));
            std::fs::create_dir_all(&p)?;
            (p, true)
        };

    println!(
        "scale_validation: records={}, threads={}, dir={}",
        args.records,
        args.threads,
        db_path.display()
    );

    // ------------------------------------------------------------------
    // Phase 1: parallel insert
    // ------------------------------------------------------------------
    let env = Arc::new(noxu::Environment::open(
        EnvironmentConfig::new(db_path.clone())
            .with_allow_create(true)
            .with_transactional(true),
    )?);
    let db = Arc::new(env.open_database(
        None,
        "scale",
        &DatabaseConfig::new().with_allow_create(true),
    )?);

    let records_per_thread = args.records.div_ceil(args.threads as u64);
    let total_inserted = Arc::new(AtomicU64::new(0));
    let barrier = Arc::new(Barrier::new(args.threads + 1));
    let start = Instant::now();

    // Progress reporter runs in main thread after barrier.
    let total_inserted_progress = Arc::clone(&total_inserted);
    let records_total = args.records;
    let progress_handle = {
        let barrier = Arc::clone(&barrier);
        std::thread::spawn(move || {
            barrier.wait();
            let t0 = Instant::now();
            loop {
                std::thread::sleep(Duration::from_secs(10));
                let done = total_inserted_progress.load(Ordering::Relaxed);
                if done >= records_total {
                    break;
                }
                let elapsed = t0.elapsed().as_secs_f64();
                let ops_s = done as f64 / elapsed;
                let pct = done * 100 / records_total;
                println!(
                    "  [{:3}%] {done}/{records_total} records  {ops_s:.0} ops/s",
                    pct
                );
            }
        })
    };

    let mut writer_handles = Vec::new();
    for tid in 0..args.threads {
        let env = Arc::clone(&env);
        let db = Arc::clone(&db);
        let total_inserted = Arc::clone(&total_inserted);
        let barrier = Arc::clone(&barrier);
        let threads = args.threads as u64;
        writer_handles.push(std::thread::spawn(move || {
            barrier.wait();
            let batch_size: u64 = 1_000;
            let start_key = tid as u64 * records_per_thread;
            let end_key = ((tid as u64 + 1) * records_per_thread)
                .min(threads * records_per_thread);
            let mut key_idx = start_key;
            while key_idx < end_key {
                let batch_end = (key_idx + batch_size).min(end_key);
                let batch_count = batch_end - key_idx;
                let txn = env.begin_transaction(None).unwrap();
                while key_idx < batch_end {
                    let key = format!("key:{key_idx:012}");
                    let val = format!("val:{tid:04}:{key_idx:012}");
                    let k = DatabaseEntry::from_bytes(key.as_bytes());
                    let v = DatabaseEntry::from_bytes(val.as_bytes());
                    db.put_in(&txn, &k, &v).unwrap();
                    key_idx += 1;
                }
                txn.commit().unwrap();
                total_inserted.fetch_add(batch_count, Ordering::Relaxed);
            }
        }));
    }

    barrier.wait(); // release all threads
    for h in writer_handles {
        h.join().expect("writer thread panicked");
    }
    let _ = progress_handle; // progress thread will exit when records_total reached

    let insert_elapsed = start.elapsed();
    let insert_ops_s = args.records as f64 / insert_elapsed.as_secs_f64();
    println!(
        "Phase 1 done: {records} records in {elapsed:?}  ({ops_s:.0} ops/s)",
        records = args.records,
        elapsed = insert_elapsed,
        ops_s = insert_ops_s,
    );

    // ------------------------------------------------------------------
    // Phase 2: checkpoint + close
    // ------------------------------------------------------------------
    println!("Checkpointing...");
    env.checkpoint(None)?;
    let dir_size_after_insert = dir_size_bytes(&db_path);
    drop(db);
    drop(env);
    println!(
        "  Log dir size after insert: {}",
        human_bytes(dir_size_after_insert)
    );

    // ------------------------------------------------------------------
    // Phase 3: reopen (WAL recovery)
    // ------------------------------------------------------------------
    println!("Reopening via WAL recovery...");
    let t_recovery = Instant::now();
    let env2 = noxu::Environment::open(
        EnvironmentConfig::new(db_path.clone())
            .with_allow_create(false)
            .with_transactional(true),
    )?;
    let db2 = env2.open_database(
        None,
        "scale",
        &DatabaseConfig::new().with_allow_create(false),
    )?;
    println!("  Recovery completed in {:?}", t_recovery.elapsed());

    // ------------------------------------------------------------------
    // Phase 4: full scan — verify count and sorted order
    // ------------------------------------------------------------------
    println!("Scanning all records...");
    let t_scan = Instant::now();
    let mut cursor = db2.open_cursor(None)?;
    let mut k = DatabaseEntry::new();
    let mut v = DatabaseEntry::new();
    let mut count: u64 = 0;
    let mut order_errors: u64 = 0;
    let mut prev_key: Option<String> = None;

    let mut op = Get::First;
    loop {
        let s = cursor.get(&mut k, &mut v, op, None)?;
        if s != OperationStatus::Success {
            break;
        }
        let cur_key = String::from_utf8_lossy(k.data_opt().unwrap_or_default())
            .into_owned();
        if let Some(ref p) = prev_key
            && cur_key < *p
        {
            order_errors += 1;
        }
        prev_key = Some(cur_key);
        count += 1;
        op = Get::Next;
    }
    cursor.close()?;
    println!("  Scanned {count} records in {:?}", t_scan.elapsed());

    // ------------------------------------------------------------------
    // Phase 5: post-cleaner stats
    // ------------------------------------------------------------------
    env2.checkpoint(None)?;
    let dir_size_final = dir_size_bytes(&db_path);

    let stats = env2.stats()?;
    drop(db2);
    drop(env2);

    // ------------------------------------------------------------------
    // Results
    // ------------------------------------------------------------------
    println!("\n=== Scale Validation Results ===");
    println!("  Records inserted : {}", args.records);
    println!("  Records scanned  : {count}");
    println!("  Order errors     : {order_errors}");
    println!(
        "  Insert throughput: {:.0} ops/s  ({:?} total)",
        insert_ops_s, insert_elapsed
    );
    println!("  Log dir (final)  : {}", human_bytes(dir_size_final));
    println!(
        "  Cleaner runs     : {}  deletions: {}",
        stats.cleaner.runs, stats.cleaner.deletions
    );
    if let Some(rss) = peak_rss_bytes() {
        println!("  Peak RSS (VmPeak): {}", human_bytes(rss));
    }

    let raw_data_size = args.records * 50; // approx key+value bytes per record
    let size_ratio = dir_size_final as f64 / raw_data_size as f64;
    println!("  Log/data ratio   : {size_ratio:.2}x (threshold: 1.5x)");

    // ------------------------------------------------------------------
    // Assertions
    // ------------------------------------------------------------------
    assert_eq!(count, args.records, "record count mismatch after recovery");
    assert_eq!(order_errors, 0, "{order_errors} out-of-order keys found");
    assert!(
        dir_size_final <= (raw_data_size as f64 * 1.5) as u64,
        "log dir {dir} > 1.5x raw data size {raw} — cleaner not effective",
        dir = human_bytes(dir_size_final),
        raw = human_bytes(raw_data_size),
    );

    println!("\nAll assertions passed.");
    Ok(())
}
