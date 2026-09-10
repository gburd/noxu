//! Writer tail-latency distribution vs `parking_lot`, at fixed reader counts.
//!
//! Reproduces the measurement in
//! `docs/src/internal/parking-lot-removal-2026-09.md` ("The honest remaining
//! gap: write tail latency") for the writer-reservation port: 1 writer vs N
//! hammering readers, 3s window, p50/p99/p99.9/max plus total writes
//! completed. Run on an idle, dedicated box -- this is exactly the kind of
//! number that a co-scheduled neighbour corrupts.
//!
//! Run: `cargo run --release -q -p noxu-sync --example writer_tail_latency`
use lock_api::RawRwLock;
use std::hint::black_box;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

const WINDOW: Duration = Duration::from_secs(3);
const READER_COUNTS: &[usize] = &[4, 16, 63];

struct Result {
    waits_ns: Vec<u64>,
}

impl Result {
    fn percentiles(&self) -> (u64, u64, u64, u64) {
        let mut v = self.waits_ns.clone();
        v.sort_unstable();
        let n = v.len();
        if n == 0 {
            return (0, 0, 0, 0);
        }
        let pick = |q: f64| v[((n as f64 * q) as usize).min(n - 1)];
        (pick(0.50), pick(0.99), pick(0.999), v[n - 1])
    }
}

/// 1 writer vs `readers` hammering readers for `WINDOW`. The writer times
/// EVERY acquisition (not just a sample), so the reported distribution is
/// exact, not extrapolated.
fn measure<R: RawRwLock + Send + Sync + 'static>(readers: usize) -> Result {
    let lock: Arc<lock_api::RwLock<R, u64>> =
        Arc::new(lock_api::RwLock::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let gate = Arc::new(Barrier::new(readers + 2));

    let reader_handles: Vec<_> = (0..readers)
        .map(|_| {
            let lock = Arc::clone(&lock);
            let stop = Arc::clone(&stop);
            let gate = Arc::clone(&gate);
            std::thread::spawn(move || {
                gate.wait();
                while !stop.load(Ordering::Relaxed) {
                    for _ in 0..64 {
                        let g = lock.read();
                        black_box(*g);
                    }
                }
            })
        })
        .collect();

    let writer_lock = Arc::clone(&lock);
    let writer_stop = Arc::clone(&stop);
    let writer_gate = Arc::clone(&gate);
    let writer = std::thread::spawn(move || {
        let mut waits = Vec::with_capacity(1 << 16);
        writer_gate.wait();
        while !writer_stop.load(Ordering::Relaxed) {
            let t0 = Instant::now();
            let mut g = writer_lock.write();
            let waited = t0.elapsed();
            *g = g.wrapping_add(1);
            drop(g);
            waits.push(waited.as_nanos() as u64);
        }
        waits
    });

    gate.wait();
    std::thread::sleep(WINDOW);
    stop.store(true, Ordering::Relaxed);

    let waits_ns = writer.join().expect("writer panicked");
    for h in reader_handles {
        let _ = h.join();
    }

    Result { waits_ns }
}

fn fmt_ns(ns: u64) -> String {
    if ns >= 1_000_000 {
        format!("{:.2}ms", ns as f64 / 1e6)
    } else if ns >= 1_000 {
        format!("{:.1}us", ns as f64 / 1e3)
    } else {
        format!("{ns}ns")
    }
}

fn self_check() {
    // Same intent as the other benches' self_check: prove mutual exclusion
    // actually holds under this harness before trusting any number.
    fn check<R: lock_api::RawMutex + Send + Sync + 'static>(label: &str) {
        let m: Arc<lock_api::Mutex<R, u64>> = Arc::new(lock_api::Mutex::new(0));
        let threads = 8;
        let gate = Arc::new(Barrier::new(threads));
        let handles: Vec<_> = (0..threads)
            .map(|_| {
                let m = Arc::clone(&m);
                let gate = Arc::clone(&gate);
                std::thread::spawn(move || {
                    gate.wait();
                    for _ in 0..10_000 {
                        let mut g = m.lock();
                        *g = g.wrapping_add(1);
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(*m.lock(), threads as u64 * 10_000, "{label}: lost updates");
    }
    check::<noxu_sync::RawMutex>("noxu_sync");
    check::<parking_lot::RawMutex>("parking_lot");
}

fn main() {
    self_check();

    println!(
        "# Writer tail latency: 1 writer vs N readers, {WINDOW:?} window\n"
    );
    println!("| readers | impl | p50 | p99 | p99.9 | max | writes |");
    println!("|---:|---|---:|---:|---:|---:|---:|");

    for &readers in READER_COUNTS {
        // Interleave impls within each reader count so host-condition drift
        // hits both sides roughly equally.
        let ours = measure::<noxu_sync::NoxuRawRwLock>(readers);
        let theirs = measure::<parking_lot::RawRwLock>(readers);

        let (p50, p99, p999, max) = ours.percentiles();
        println!(
            "| {readers} | noxu_sync | {} | {} | {} | {} | {} |",
            fmt_ns(p50),
            fmt_ns(p99),
            fmt_ns(p999),
            fmt_ns(max),
            ours.waits_ns.len()
        );
        let (p50, p99, p999, max) = theirs.percentiles();
        println!(
            "| {readers} | parking_lot | {} | {} | {} | {} | {} |",
            fmt_ns(p50),
            fmt_ns(p99),
            fmt_ns(p999),
            fmt_ns(max),
            theirs.waits_ns.len()
        );
    }
}
