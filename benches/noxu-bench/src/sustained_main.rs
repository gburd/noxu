//! Sustained-load baseline: a 24h-capable workload runner that emits
//! per-window CSV metrics for downstream aggregation.
//!
//! Designed for measuring p50/p99 latency and throughput stability
//! over time. Compared to `noxu-workload-bench` (which runs N
//! workloads serially and prints a final summary), this binary runs
//! one workload continuously for a configurable duration and writes
//! one CSV row per measurement window.
//!
//! Usage:
//!
//! ```sh
//! cargo run --bin noxu-sustained-baseline --release -- \
//!     --dir /var/lib/noxu-bench \
//!     --duration-secs 86400 \
//!     --window-secs 60 \
//!     --readers 8 --writers 8 \
//!     --value-size 256 \
//!     --output baseline.csv
//! ```
//!
//! Output format (CSV with header):
//!
//! ```
//! window_start_secs,window_secs,reads,writes,read_ns_p50,read_ns_p99,
//!     write_ns_p50,write_ns_p99,rss_kb,disk_bytes_written,err_count
//! ```
//!
//! Each row covers `window_secs` seconds of activity. Aggregating
//! 1440 rows (24h × 60s windows) gives the per-hour and per-day
//! shape of throughput and latency, including any drift from
//! cleaner backlog, cache pressure, or fragmentation.

#![allow(clippy::too_many_arguments)]

use noxu_db::{DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::time::{Duration, Instant};

/// Per-thread sample-batch size before flushing to the shared
/// histogram. Larger reduces lock contention; smaller gives finer
/// resolution at short window sizes. 64 means even a 3-second
/// smoke window with low write throughput still produces a
/// non-zero p50/p99.
const SAMPLE_BATCH: usize = 64;
#[derive(Default)]
struct LatencyHistogram {
    /// Sorted at percentile-compute time.
    samples: Vec<u64>,
}

impl LatencyHistogram {
    /// Compute pN where 0.0 <= p <= 1.0. Returns 0 if empty.
    fn percentile(&mut self, p: f64) -> u64 {
        if self.samples.is_empty() {
            return 0;
        }
        self.samples.sort_unstable();
        let idx = ((self.samples.len() as f64) * p).floor() as usize;
        let idx = idx.min(self.samples.len() - 1);
        self.samples[idx]
    }
}

struct Args {
    dir: PathBuf,
    duration: Duration,
    window: Duration,
    readers: usize,
    writers: usize,
    value_size: usize,
    output: Option<PathBuf>,
}

fn parse_args() -> Args {
    let mut args = std::env::args().skip(1);
    let mut dir = PathBuf::from("/tmp/noxu-sustained");
    let mut duration_secs: u64 = 86_400;
    let mut window_secs: u64 = 60;
    let mut readers: usize = 8;
    let mut writers: usize = 8;
    let mut value_size: usize = 256;
    let mut output: Option<PathBuf> = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--dir" => {
                dir = args.next().expect("--dir requires a path").into();
            }
            "--duration-secs" => {
                duration_secs = args
                    .next()
                    .expect("--duration-secs requires a number")
                    .parse()
                    .expect("--duration-secs must be a positive integer");
            }
            "--window-secs" => {
                window_secs = args
                    .next()
                    .expect("--window-secs requires a number")
                    .parse()
                    .expect("--window-secs must be a positive integer");
            }
            "--readers" => {
                readers = args
                    .next()
                    .expect("--readers requires a number")
                    .parse()
                    .expect("--readers must be a non-negative integer");
            }
            "--writers" => {
                writers = args
                    .next()
                    .expect("--writers requires a number")
                    .parse()
                    .expect("--writers must be a non-negative integer");
            }
            "--value-size" => {
                value_size = args
                    .next()
                    .expect("--value-size requires a number")
                    .parse()
                    .expect("--value-size must be a non-negative integer");
            }
            "--output" => {
                output =
                    Some(args.next().expect("--output requires a path").into());
            }
            "-h" | "--help" => {
                eprintln!(
                    "usage: noxu-sustained-baseline [options]\n\
                     options:\n\
                     \x20 --dir <path>            env home directory\n\
                     \x20                          (default /tmp/noxu-sustained)\n\
                     \x20 --duration-secs <secs>   total run time (default 86400)\n\
                     \x20 --window-secs <secs>     metrics window (default 60)\n\
                     \x20 --readers <n>            reader threads (default 8)\n\
                     \x20 --writers <n>            writer threads (default 8)\n\
                     \x20 --value-size <bytes>     value size in bytes (default 256)\n\
                     \x20 --output <path>          CSV output (default stdout)\n"
                );
                std::process::exit(0);
            }
            other => {
                eprintln!("unknown arg: {other}");
                std::process::exit(2);
            }
        }
    }

    Args {
        dir,
        duration: Duration::from_secs(duration_secs),
        window: Duration::from_secs(window_secs),
        readers,
        writers,
        value_size,
        output,
    }
}

fn main() {
    let args = parse_args();

    if args.readers + args.writers == 0 {
        eprintln!("at least one of --readers or --writers must be > 0");
        std::process::exit(2);
    }

    eprintln!(
        "sustained baseline: dir={:?} duration={:?} window={:?} \
         readers={} writers={} value_size={}",
        args.dir,
        args.duration,
        args.window,
        args.readers,
        args.writers,
        args.value_size,
    );

    let _ = std::fs::create_dir_all(&args.dir);
    let env = Arc::new(
        Environment::open(
            EnvironmentConfig::new(args.dir.clone())
                .with_allow_create(true)
                .with_transactional(true),
        )
        .expect("open env"),
    );
    let db = Arc::new(
        env.open_database(
            None,
            "baseline",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .expect("open db"),
    );

    let stop = Arc::new(AtomicBool::new(false));
    let total_threads = args.readers + args.writers;
    let barrier = Arc::new(Barrier::new(total_threads + 1));

    // Per-thread histograms; merged at window boundaries.
    let read_hist = Arc::new(Mutex::new(LatencyHistogram::default()));
    let write_hist = Arc::new(Mutex::new(LatencyHistogram::default()));

    let read_count = Arc::new(AtomicU64::new(0));
    let write_count = Arc::new(AtomicU64::new(0));
    let err_count = Arc::new(AtomicU64::new(0));

    let mut handles = Vec::with_capacity(total_threads);

    for tid in 0..args.readers {
        let db = Arc::clone(&db);
        let stop = Arc::clone(&stop);
        let barrier = Arc::clone(&barrier);
        let hist = Arc::clone(&read_hist);
        let count = Arc::clone(&read_count);
        let err = Arc::clone(&err_count);
        let h = std::thread::spawn(move || {
            let mut rng = SmallRng::seed_from_u64(0xCAFE + tid as u64);
            let mut local_samples: Vec<u64> = Vec::with_capacity(1024);
            barrier.wait();
            while !stop.load(Ordering::Relaxed) {
                let key_id: u32 = rng.gen_range(0..1_000_000);
                let key = DatabaseEntry::from_bytes(&key_id.to_be_bytes());
                let mut val = DatabaseEntry::new();
                let t0 = Instant::now();
                match db.get(None, &key, &mut val) {
                    Ok(_) => {
                        local_samples.push(t0.elapsed().as_nanos() as u64);
                        count.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        err.fetch_add(1, Ordering::Relaxed);
                    }
                }
                if local_samples.len() == SAMPLE_BATCH {
                    let mut g = hist.lock().unwrap();
                    g.samples.append(&mut local_samples);
                }
            }
            // Flush any remaining samples.
            if !local_samples.is_empty() {
                let mut g = hist.lock().unwrap();
                g.samples.append(&mut local_samples);
            }
        });
        handles.push(h);
    }

    for tid in 0..args.writers {
        let db = Arc::clone(&db);
        let stop = Arc::clone(&stop);
        let barrier = Arc::clone(&barrier);
        let hist = Arc::clone(&write_hist);
        let count = Arc::clone(&write_count);
        let err = Arc::clone(&err_count);
        let value_size = args.value_size;
        let h = std::thread::spawn(move || {
            let mut rng = SmallRng::seed_from_u64(0xDEAD + tid as u64);
            let value: Vec<u8> = vec![0xA5; value_size];
            let mut local_samples: Vec<u64> = Vec::with_capacity(SAMPLE_BATCH);
            let mut counter: u32 = (tid as u32) << 24;
            barrier.wait();
            while !stop.load(Ordering::Relaxed) {
                let key = DatabaseEntry::from_bytes(&counter.to_be_bytes());
                let val = DatabaseEntry::from_bytes(&value);
                counter = counter.wrapping_add(1);
                let _ = rng.r#gen::<u64>(); // mix in some entropy
                let t0 = Instant::now();
                match db.put(None, &key, &val) {
                    Ok(_) => {
                        local_samples.push(t0.elapsed().as_nanos() as u64);
                        count.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        err.fetch_add(1, Ordering::Relaxed);
                    }
                }
                if local_samples.len() == SAMPLE_BATCH {
                    let mut g = hist.lock().unwrap();
                    g.samples.append(&mut local_samples);
                }
            }
            if !local_samples.is_empty() {
                let mut g = hist.lock().unwrap();
                g.samples.append(&mut local_samples);
            }
        });
        handles.push(h);
    }

    barrier.wait();
    let run_start = Instant::now();

    // Open output writer.
    let mut out: Box<dyn Write + Send> = match args.output.as_ref() {
        Some(path) => Box::new(std::io::BufWriter::new(
            std::fs::File::create(path).expect("create output"),
        )),
        None => Box::new(std::io::BufWriter::new(std::io::stdout())),
    };
    writeln!(
        out,
        "window_start_secs,window_secs,reads,writes,\
         read_ns_p50,read_ns_p99,write_ns_p50,write_ns_p99,\
         rss_kb,disk_bytes_written,err_count"
    )
    .unwrap();

    let mut prev_reads = 0u64;
    let mut prev_writes = 0u64;
    let mut prev_errs = 0u64;
    let mut prev_disk = read_disk_bytes(&args.dir);

    while run_start.elapsed() < args.duration {
        let window_end = Instant::now() + args.window;
        while Instant::now() < window_end && run_start.elapsed() < args.duration
        {
            std::thread::sleep(Duration::from_millis(100));
        }

        let now_reads = read_count.load(Ordering::Relaxed);
        let now_writes = write_count.load(Ordering::Relaxed);
        let now_errs = err_count.load(Ordering::Relaxed);
        let now_disk = read_disk_bytes(&args.dir);

        // Drain histograms — taking lock briefly per window.
        let read_p50;
        let read_p99;
        {
            let mut g = read_hist.lock().unwrap();
            read_p50 = g.percentile(0.5);
            read_p99 = g.percentile(0.99);
            g.samples.clear();
        }
        let write_p50;
        let write_p99;
        {
            let mut g = write_hist.lock().unwrap();
            write_p50 = g.percentile(0.5);
            write_p99 = g.percentile(0.99);
            g.samples.clear();
        }

        writeln!(
            out,
            "{},{},{},{},{},{},{},{},{},{},{}",
            run_start.elapsed().as_secs() - args.window.as_secs(),
            args.window.as_secs(),
            now_reads - prev_reads,
            now_writes - prev_writes,
            read_p50,
            read_p99,
            write_p50,
            write_p99,
            read_rss_kb(),
            now_disk - prev_disk,
            now_errs - prev_errs,
        )
        .unwrap();
        out.flush().unwrap();

        prev_reads = now_reads;
        prev_writes = now_writes;
        prev_errs = now_errs;
        prev_disk = now_disk;
    }

    stop.store(true, Ordering::Relaxed);
    for h in handles {
        let _ = h.join();
    }
    drop(db);
    drop(env);

    eprintln!("sustained baseline complete.");
}

/// Read RSS in KB on Linux; returns 0 elsewhere.
fn read_rss_kb() -> u64 {
    if let Ok(s) = std::fs::read_to_string("/proc/self/status") {
        for line in s.lines() {
            if let Some(rest) = line.strip_prefix("VmRSS:") {
                let mut parts = rest.split_whitespace();
                if let Some(kb) = parts.next().and_then(|s| s.parse().ok()) {
                    return kb;
                }
            }
        }
    }
    0
}

/// Sum of file sizes under `dir` in bytes (proxy for disk-bytes-written).
fn read_disk_bytes(dir: &std::path::Path) -> u64 {
    let mut total = 0u64;
    if let Ok(entries) = std::fs::read_dir(dir) {
        for e in entries.flatten() {
            if let Ok(md) = e.metadata()
                && md.is_file()
            {
                total += md.len();
            }
        }
    }
    total
}
