// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Fairness + throughput of `noxu-sync` vs `parking_lot` under contention.
//!
//! ## Why this is a first-class result, not a footnote
//!
//! The review's objection to `noxu-sync` is a *package*: ~20 `unsafe` FFI
//! blocks, entanglement with the TSAN suppressions, and — the concrete
//! functional cost — non-fair futex primitives are the documented blocker for
//! JE's `env_fair_latches` (see `docs/src/operations/known-limitations.md`).
//!
//! If `noxu-sync` wins contended throughput, the mechanism matters. A
//! **barging** lock wins throughput *by* being unfair: it hands the lock to
//! whichever thread is spinning on-CPU rather than to the longest waiter,
//! which maximises hand-off rate and starves queued waiters. Reporting only
//! ns/op would present the benefit and hide its price. So this bench measures
//! both sides of that trade in the same run:
//!
//!   * **throughput** — total acquisitions in a fixed wall-clock window;
//!   * **fairness** — how those acquisitions were distributed across threads:
//!     `max/min` ratio, coefficient of variation (CoV = stddev/mean), and the
//!     starvation count (threads that got < 10 % of the mean).
//!
//! Perfect fairness is `max/min == 1.0`, `CoV == 0`. A ratio of 50:1 means
//! one thread got fifty times the service of another, which is what an engine
//! operator experiences as a latency outlier.
//!
//! ## `parking_lot`'s eventual fairness (read the numbers with this in mind)
//!
//! `parking_lot` is **not** a pure barging lock. It implements *eventual
//! fairness*: on unlock it occasionally (~every 0.5 ms per lock) performs a
//! **fair handoff**, passing ownership directly to the queue head instead of
//! releasing for a race. So `parking_lot` deliberately spends some throughput
//! to bound worst-case starvation, while `noxu-sync`'s `unlock` is an
//! unconditional `swap(UNLOCKED)` + `futex_wake(1)` with no such mechanism —
//! a woken waiter must re-CAS and can lose to a fresh arrival indefinitely.
//!
//! That difference is a design choice, not a bug in either, and it is
//! precisely why a throughput-only comparison is not a fair summary: the two
//! locks are not optimising the same objective. Any throughput gap must be
//! read next to the fairness columns below.
//!
//! ## Method
//!
//! Not a criterion bench: criterion measures per-iteration latency, and the
//! quantity of interest here is a *distribution across threads* over a fixed
//! window. Custom harness instead, `harness = false`, printing a table.
//!
//! Each configuration: N threads hammer one lock for `WINDOW`, each counting
//! its own acquisitions in a padded (cache-line-isolated) slot. Threads start
//! on a `Barrier` and stop on a shared `AtomicBool` deadline flag, so all
//! threads are live for the same window and no thread's exit decays the
//! contention for the others.
//!
//! `REPS` repetitions per configuration; the median-throughput rep is
//! reported (medians, not means, so one scheduler hiccup cannot move the
//! headline). Both impls are run back-to-back within a rep, so any slow drift
//! in host conditions hits both sides roughly equally.
//!
//! Run: `cargo bench -p noxu-sync --bench sync_fairness`

use lock_api::{RawMutex, RawRwLock};
use std::hint::black_box;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

/// Measurement window per repetition. Long enough to cross many of
/// `parking_lot`'s ~0.5 ms eventual-fairness handoff intervals, so its
/// fairness mechanism is actually exercised rather than being an artifact of
/// too short a run.
const WINDOW: Duration = Duration::from_millis(2000);

/// Repetitions per configuration; the median-throughput rep is reported.
const REPS: usize = 3;

/// Thread counts swept. The box is 64 vCPU (32 physical cores x 2 SMT), so 64
/// is exactly saturated and nothing here is oversubscribed.
const THREADS: &[usize] = &[2, 4, 8, 16, 32, 64];

/// One-in-`WRITE_EVERY` ops takes the exclusive lock in the read-heavy
/// rwlock regime — read-mostly with occasional exclusive mutation, the shape
/// of `noxu-dbi`'s cursor / `DatabaseImpl` locks (which route through
/// `dst_sync_pl` to `noxu-sync` in production). Note this is *not* the B-tree
/// node latch: that is already `parking_lot::RwLock`.
const WRITE_EVERY: u64 = 1024;

/// Cache-line-padded per-thread counter: without the padding, neighbouring
/// counters share a line and the false sharing would dominate what we are
/// trying to measure.
#[repr(align(64))]
struct PaddedCounter(AtomicU64);

/// Per-configuration result.
struct Outcome {
    /// Total acquisitions across all threads in the window.
    total: u64,
    /// Per-thread acquisition counts.
    counts: Vec<u64>,
    /// Actual elapsed window (may exceed `WINDOW` slightly).
    elapsed: Duration,
}

impl Outcome {
    /// Millions of acquisitions per second, system-wide.
    fn mops(&self) -> f64 {
        self.total as f64 / self.elapsed.as_secs_f64() / 1e6
    }

    /// Aggregate nanoseconds per acquisition (reciprocal of throughput);
    /// directly comparable with the criterion bench's threaded numbers.
    fn ns_per_op(&self) -> f64 {
        self.elapsed.as_secs_f64() * 1e9 / self.total.max(1) as f64
    }

    /// Ratio of the busiest thread's count to the least-served thread's.
    /// 1.0 is perfect fairness; large values mean starvation.
    fn max_min_ratio(&self) -> f64 {
        let max = *self.counts.iter().max().unwrap_or(&0) as f64;
        let min = *self.counts.iter().min().unwrap_or(&0) as f64;
        if min <= 0.0 { f64::INFINITY } else { max / min }
    }

    /// Coefficient of variation (stddev / mean) of the per-thread counts.
    /// 0 is perfect fairness. Robust to thread count, unlike max/min, which
    /// is a two-sample statistic.
    fn cov(&self) -> f64 {
        let n = self.counts.len() as f64;
        if n < 2.0 {
            return 0.0;
        }
        let mean = self.total as f64 / n;
        if mean <= 0.0 {
            return 0.0;
        }
        let var = self
            .counts
            .iter()
            .map(|&c| {
                let d = c as f64 - mean;
                d * d
            })
            .sum::<f64>()
            / n;
        var.sqrt() / mean
    }

    /// Threads served less than 10 % of the mean — effectively starved.
    fn starved(&self) -> usize {
        let mean = self.total as f64 / self.counts.len() as f64;
        self.counts.iter().filter(|&&c| (c as f64) < mean * 0.10).count()
    }
}

/// Runs `threads` workers hammering `op` for `WINDOW`, counting per-thread
/// acquisitions. All threads are live for the entire window: they start
/// together on a barrier and stop only when the deadline flag is set.
fn measure<F>(threads: usize, op: F) -> Outcome
where
    F: Fn(usize, u64) + Send + Sync + 'static,
{
    let counters: Arc<Vec<PaddedCounter>> = Arc::new(
        (0..threads).map(|_| PaddedCounter(AtomicU64::new(0))).collect(),
    );
    let stop = Arc::new(AtomicBool::new(false));
    let gate = Arc::new(Barrier::new(threads + 1));
    let op = Arc::new(op);

    let handles: Vec<_> = (0..threads)
        .map(|t| {
            let counters = Arc::clone(&counters);
            let stop = Arc::clone(&stop);
            let gate = Arc::clone(&gate);
            let op = Arc::clone(&op);
            std::thread::spawn(move || {
                gate.wait();
                let mut n = 0u64;
                // Check the stop flag every 64 ops: often enough to end the
                // window promptly, rarely enough that the relaxed load is not
                // a measurable part of the loop.
                while !stop.load(Ordering::Relaxed) {
                    for _ in 0..64 {
                        op(t, n);
                        n += 1;
                    }
                }
                counters[t].0.store(n, Ordering::Relaxed);
            })
        })
        .collect();

    gate.wait();
    let t0 = Instant::now();
    std::thread::sleep(WINDOW);
    stop.store(true, Ordering::Relaxed);
    for h in handles {
        h.join().expect("worker panicked");
    }
    let elapsed = t0.elapsed();

    let counts: Vec<u64> =
        counters.iter().map(|c| c.0.load(Ordering::Relaxed)).collect();
    Outcome { total: counts.iter().sum(), counts, elapsed }
}

/// Runs `REPS` reps and returns the one with median throughput.
fn measure_median<F>(threads: usize, make_op: impl Fn() -> F) -> Outcome
where
    F: Fn(usize, u64) + Send + Sync + 'static,
{
    let mut out: Vec<Outcome> =
        (0..REPS).map(|_| measure(threads, make_op())).collect();
    out.sort_by(|a, b| {
        a.mops().partial_cmp(&b.mops()).expect("throughput is finite")
    });
    out.swap_remove(REPS / 2)
}

// ---------------------------------------------------------------------------
// Workloads
// ---------------------------------------------------------------------------

fn mutex_outcome<R: RawMutex + Send + Sync + 'static>(
    threads: usize,
) -> Outcome {
    measure_median(threads, || {
        let m: Arc<lock_api::Mutex<R, u64>> = Arc::new(lock_api::Mutex::new(0));
        move |_t: usize, _i: u64| {
            let mut g = m.lock();
            *g = g.wrapping_add(1);
        }
    })
}

fn rwlock_outcome<R: RawRwLock + Send + Sync + 'static>(
    threads: usize,
    write_every: u64,
) -> Outcome {
    measure_median(threads, || {
        let l: Arc<lock_api::RwLock<R, u64>> =
            Arc::new(lock_api::RwLock::new(7));
        move |t: usize, i: u64| {
            if write_every != 0
                && (i.wrapping_add(t as u64 * 7)).is_multiple_of(write_every)
            {
                let mut g = l.write();
                *g = g.wrapping_add(1);
            } else {
                let g = l.read();
                black_box(*g);
            }
        }
    })
}

/// Result of the starvation probe: how long a single "victim" thread waits
/// for the lock while N-1 threads hammer it.
struct Starvation {
    /// Acquisitions the victim managed inside the window.
    acquisitions: u64,
    /// Worst single wait observed, nanoseconds.
    max_ns: u64,
    /// 99th-percentile wait, nanoseconds.
    p99_ns: u64,
    /// Median wait, nanoseconds.
    median_ns: u64,
    /// True if the victim was still waiting when the probe deadline expired,
    /// i.e. `max_ns` is a *floor* on the true worst case, not the worst case.
    truncated: bool,
}

/// Measures the tail latency a *single* thread sees while `hammers` other
/// threads contend for the same lock.
///
/// This is the number that turns "unfair" into an operator-visible quantity:
/// aggregate throughput can look excellent while one unlucky thread waits
/// milliseconds. A lock with a fairness mechanism bounds this; a pure barging
/// lock does not bound it at all.
///
/// The victim's every acquisition is individually timed, so this reports the
/// distribution rather than an average. A hard deadline caps the probe: if the
/// victim is starved past it, the run is marked `truncated` and `max_ns` is
/// reported as a lower bound rather than hanging the bench (an earlier version
/// of this suite hung 15 minutes on exactly this condition).
fn starvation_probe<R: RawMutex + Send + Sync + 'static>(
    hammers: usize,
) -> Starvation {
    /// Cap on the whole probe. A victim starved this long has made the point.
    const PROBE_DEADLINE: Duration = Duration::from_secs(10);

    let m: Arc<lock_api::Mutex<R, u64>> = Arc::new(lock_api::Mutex::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let gate = Arc::new(Barrier::new(hammers + 2));

    let hammer_handles: Vec<_> = (0..hammers)
        .map(|_| {
            let m = Arc::clone(&m);
            let stop = Arc::clone(&stop);
            let gate = Arc::clone(&gate);
            std::thread::spawn(move || {
                gate.wait();
                while !stop.load(Ordering::Relaxed) {
                    for _ in 0..64 {
                        let mut g = m.lock();
                        *g = g.wrapping_add(1);
                    }
                }
            })
        })
        .collect();

    let victim = {
        let m = Arc::clone(&m);
        let stop = Arc::clone(&stop);
        let gate = Arc::clone(&gate);
        std::thread::spawn(move || {
            gate.wait();
            let mut waits: Vec<u64> = Vec::with_capacity(1 << 16);
            while !stop.load(Ordering::Relaxed) {
                let t0 = Instant::now();
                let mut g = m.lock();
                let waited = t0.elapsed();
                *g = g.wrapping_add(1);
                drop(g);
                waits.push(waited.as_nanos() as u64);
                // Yield so the victim is a light, realistic participant
                // rather than a hammer itself: the question is "can a normal
                // thread get service", not "who wins a spin race".
                std::thread::yield_now();
            }
            waits
        })
    };

    gate.wait();
    std::thread::sleep(WINDOW.min(PROBE_DEADLINE));
    stop.store(true, Ordering::Relaxed);

    // The victim may be blocked inside `lock()` right now; joining could take
    // arbitrarily long under a starving lock. Bound the wait.
    let deadline = Instant::now() + PROBE_DEADLINE;
    let mut truncated = false;
    while !victim.is_finished() {
        if Instant::now() >= deadline {
            truncated = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }

    let waits = if truncated {
        // Cannot join without risking an unbounded block; the hammers are
        // stopped, so the victim will drain on its own. Report what the
        // truncation implies rather than blocking the suite.
        Vec::new()
    } else {
        victim.join().unwrap_or_default()
    };
    for h in hammer_handles {
        let _ = h.join();
    }

    if waits.is_empty() {
        return Starvation {
            acquisitions: 0,
            max_ns: PROBE_DEADLINE.as_nanos() as u64,
            p99_ns: PROBE_DEADLINE.as_nanos() as u64,
            median_ns: PROBE_DEADLINE.as_nanos() as u64,
            truncated: true,
        };
    }

    let mut sorted = waits;
    sorted.sort_unstable();
    let n = sorted.len();
    let pick = |q: f64| sorted[((n as f64 * q) as usize).min(n - 1)];
    Starvation {
        acquisitions: n as u64,
        max_ns: sorted[n - 1],
        p99_ns: pick(0.99),
        median_ns: pick(0.50),
        truncated,
    }
}

// ---------------------------------------------------------------------------
// Reporting
// ---------------------------------------------------------------------------

fn header(title: &str) {
    println!("\n## {title}");
    println!("\n| threads | impl | Mops/s | ns/op | max/min | CoV | starved |");
    println!("|---:|---|---:|---:|---:|---:|---:|");
}

fn row(threads: usize, name: &str, o: &Outcome) {
    let ratio = o.max_min_ratio();
    let ratio_s = if ratio.is_finite() {
        format!("{ratio:.2}")
    } else {
        "inf".to_string()
    };
    println!(
        "| {} | {} | {:.2} | {:.1} | {} | {:.3} | {} |",
        threads,
        name,
        o.mops(),
        o.ns_per_op(),
        ratio_s,
        o.cov(),
        o.starved(),
    );
}

fn starvation_header() {
    println!("\n## Starvation probe: 1 light victim thread vs N-1 hammers");
    println!(
        "\nThe victim times every acquisition. An unbounded `max` is the \
         operator-visible cost of a lock with no fairness mechanism. \
         `trunc` = the victim was still starved at the probe deadline, so \
         `max` is a **floor**, not the true worst case."
    );
    println!(
        "\n| threads | impl | victim acqs | median wait | p99 wait | max wait | trunc |"
    );
    println!("|---:|---|---:|---:|---:|---:|---:|");
}

fn fmt_ns(ns: u64) -> String {
    if ns >= 1_000_000 {
        format!("{:.2} ms", ns as f64 / 1e6)
    } else if ns >= 1_000 {
        format!("{:.1} us", ns as f64 / 1e3)
    } else {
        format!("{ns} ns")
    }
}

fn starvation_row(threads: usize, name: &str, s: &Starvation) {
    println!(
        "| {} | {} | {} | {} | {} | {} | {} |",
        threads,
        name,
        s.acquisitions,
        fmt_ns(s.median_ns),
        fmt_ns(s.p99_ns),
        fmt_ns(s.max_ns),
        if s.truncated { "YES" } else { "no" },
    );
}

/// Sanity gate, same intent as the criterion bench's: assert the primitives
/// really are mutually exclusive under this harness before any number is
/// printed, so a harness that stopped locking cannot report a huge win.
fn self_check() {
    fn check<R: RawMutex + Send + Sync + 'static>(label: &str) {
        let m: Arc<lock_api::Mutex<R, u64>> = Arc::new(lock_api::Mutex::new(0));
        let m2 = Arc::clone(&m);
        let counted = Arc::new(AtomicU64::new(0));
        let c2 = Arc::clone(&counted);
        let threads = 8;
        let gate = Arc::new(Barrier::new(threads));
        let handles: Vec<_> = (0..threads)
            .map(|_| {
                let m = Arc::clone(&m2);
                let c = Arc::clone(&c2);
                let gate = Arc::clone(&gate);
                std::thread::spawn(move || {
                    gate.wait();
                    for _ in 0..10_000 {
                        let mut g = m.lock();
                        *g = g.wrapping_add(1);
                        c.fetch_add(1, Ordering::Relaxed);
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().expect("worker panicked");
        }
        assert_eq!(
            *m.lock(),
            counted.load(Ordering::Relaxed),
            "{label}: lost updates — mutual exclusion is not holding, \
             every number from this harness would be meaningless"
        );
    }
    check::<noxu_sync::RawMutex>("noxu_sync");
    check::<parking_lot::RawMutex>("parking_lot");
}

fn main() {
    // criterion's `--test` / `--bench` flags are passed by `cargo bench`;
    // accept and ignore them so this target coexists with the criterion one.
    let test_only = std::env::args().any(|a| a == "--test");

    self_check();
    if test_only {
        println!("sync_fairness: self-check passed (--test: no measurement)");
        return;
    }

    println!("# noxu-sync vs parking_lot: throughput AND fairness");
    println!(
        "\nWindow {:?} per rep, median of {} reps, {} threads swept.\n\
         `max/min` = busiest thread's acquisitions / least-served thread's \
         (1.00 = perfect fairness). `CoV` = stddev/mean of per-thread counts \
         (0 = perfect). `starved` = threads served < 10 % of the mean.\n\
         parking_lot applies eventual fairness (~0.5 ms handoff), noxu-sync \
         has no fairness mechanism — expect that to show in these columns.",
        WINDOW,
        REPS,
        THREADS.len()
    );

    header("Mutex, all threads contending");
    for &t in THREADS {
        row(t, "noxu_sync", &mutex_outcome::<noxu_sync::RawMutex>(t));
        row(t, "parking_lot", &mutex_outcome::<parking_lot::RawMutex>(t));
    }

    header("RwLock, 100 % readers");
    for &t in THREADS {
        row(t, "noxu_sync", &rwlock_outcome::<noxu_sync::NoxuRawRwLock>(t, 0));
        row(t, "parking_lot", &rwlock_outcome::<parking_lot::RawRwLock>(t, 0));
    }

    header("RwLock, 1-in-1024 writers (dbi cursor/db lock shape)");
    for &t in THREADS {
        row(
            t,
            "noxu_sync",
            &rwlock_outcome::<noxu_sync::NoxuRawRwLock>(t, WRITE_EVERY),
        );
        row(
            t,
            "parking_lot",
            &rwlock_outcome::<parking_lot::RawRwLock>(t, WRITE_EVERY),
        );
    }

    starvation_header();
    for &t in THREADS {
        starvation_row(
            t,
            "noxu_sync",
            &starvation_probe::<noxu_sync::RawMutex>(t - 1),
        );
        starvation_row(
            t,
            "parking_lot",
            &starvation_probe::<parking_lot::RawMutex>(t - 1),
        );
    }
    println!();
}
