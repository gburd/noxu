// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! A/B microbenchmark: `noxu-sync`'s futex primitives vs `parking_lot`.
//!
//! ## Why this bench exists
//!
//! An external engineering review flagged the custom futex-based sync layer
//! (`noxu-sync`, ~20 `unsafe` FFI blocks, implicated in the TSAN
//! suppressions, and the documented blocker for `env_fair_latches`) with:
//! *"Publish the numbers or retire it."*  This bench produces those numbers.
//!
//! ## Method
//!
//! Both implementations expose `lock_api::RawMutex` / `lock_api::RawRwLock`,
//! so every regime below is written **once**, generic over the raw primitive,
//! and monomorphised for each side.  The measured code path is therefore
//! byte-for-byte the same shape; only the primitive differs.  Locks are
//! wrapped in `lock_api::Mutex<R, u64>` / `lock_api::RwLock<R, u64>` — the
//! same wrapper for both sides, so no wrapper overhead is attributed to
//! either.
//!
//! Regimes (each run for both impls):
//!
//! | Regime | Shape | Why this engine cares |
//! |---|---|---|
//! | `uncontended_mutex` | 1 thread, lock/incr/unlock, work sweep | The overwhelmingly common case: warm read path, log-buffer pin, stats bump |
//! | `uncontended_rwlock_read` / `_write` | 1 thread, work sweep | Tree-latch acquire on an uncontended node |
//! | `uncontended_*` `no_lock` arm | identical body, `Cell` instead of a lock | **Overhead control** — see below |
//! | `mutex_contended_short/{N}` | N threads, ~1 ns critical section | Hot shared counters (memory budget, evictor state) |
//! | `mutex_contended_long/{N}` | N threads, ~100 ns critical section | Log-buffer append, checkpoint bookkeeping |
//! | `rwlock_read_only/{N}` | N threads, 100 % shared | Hand-over-hand tree descent on hot upper INs |
//! | `rwlock_read_heavy/{N}` | N threads, 1-in-1024 exclusive | Tree latch with occasional split/eviction |
//!
//! ### The uncontended overhead control
//!
//! Uncontended lock/unlock is only ~15 ns, close enough to criterion's own
//! per-iteration loop cost that a naive reading could be mostly harness.
//! Two guards make the uncontended numbers interpretable:
//!
//!   * a **`no_lock` arm** runs a byte-identical body against a `Cell<u64>`
//!     (no lock at all), so `impl − no_lock` is the actual lock cost and the
//!     absolute number's harness component is visible rather than assumed;
//!   * a **work sweep** (`/0`, `/8`, `/32` filler iterations inside the
//!     critical section) must move all three arms by the *same* delta.  If it
//!     does, the measurement tracks real work and the lock cost is the
//!     constant offset; if the arms moved differently, the loop was being
//!     optimised unevenly and the numbers would be void.
//!
//! ### Multi-threaded timing convention (and the artifact it avoids)
//!
//! Threaded regimes use `iter_custom`: criterion's `iters` is the **total**
//! op count, split evenly across the N worker threads.  Threads are spawned,
//! rendezvous on a `Barrier`, and only then is the clock started; the
//! reported duration is wall-clock **makespan** — until the last worker has
//! completed its quota.  So the reported `time` is **aggregate ns per
//! operation** (i.e. the reciprocal of system-wide lock throughput), *not*
//! per-thread latency.  Lower is better and the number is directly
//! comparable across thread counts.
//!
//! Crucially, a worker that finishes its quota early does **not** exit: it
//! keeps issuing *uncounted* ops until every worker has finished, so
//! concurrency stays at N for the whole timed window.  Without this, a
//! **barging** (non-fair) lock lets one thread race ahead, finish, and
//! leave — decaying the contention level for the remainder and inflating
//! the measured throughput.  A first version of this bench did exactly that
//! and reported `noxu-sync` getting *faster* from 16 to 64 threads, which is
//! the signature of the artifact, not of a fast lock.  Since non-fairness is
//! the very property under review, the harness must not pay it a bonus.
//!
//! Makespan semantics do fold *some* unfairness cost into the throughput
//! number (a lock that starves one thread has a long tail).  That is
//! deliberate and is the metric an engine cares about — but it means
//! throughput and fairness are not fully separated here.  The sibling
//! `sync_fairness` bench separates them: it measures a fixed-duration
//! window and reports per-thread op counts alongside throughput.
//!
//! Thread spawn/join is outside the timed window except for the join tail;
//! `SamplingMode::Flat` keeps per-sample work large so that residual
//! spawn/teardown cost is a small, symmetric constant.
//!
//! Run: `cargo bench -p noxu-sync --bench sync_vs_parking_lot`
//! Single regime: `... --bench sync_vs_parking_lot -- uncontended_mutex`
//!
//! The companion `sync_fairness` bench reports the acquisition *fairness*
//! these throughput numbers are bought with; read the two together.

use criterion::{
    BenchmarkId, Criterion, SamplingMode, criterion_group, criterion_main,
};
use lock_api::{RawMutex, RawRwLock};
use std::hint::black_box;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

/// Thread counts swept: light contention (2–8) through heavy (16–64).
/// The measurement box is a 64-vCPU i4i.16xlarge, so 64 is exactly saturated
/// and no count is oversubscribed.
const THREADS: &[usize] = &[2, 4, 8, 16, 32, 64];

/// One-in-`WRITE_EVERY` operations takes the exclusive lock in the
/// `rwlock_read_heavy` regime.  1024 approximates a tree latch on a warm
/// internal node: overwhelmingly shared, occasionally split/evicted.
const WRITE_EVERY: u64 = 1024;

/// Iterations of the long critical section's inner loop.  ~64 dependent
/// mul+add pairs ≈ 100 ns on a Xeon, which is the order of a log-buffer
/// append or a BIN slot rewrite.
const LONG_CS_WORK: u32 = 64;

/// Filler-work levels swept in the uncontended regimes.  All three arms
/// (both impls plus the lock-free control) must move by the same delta across
/// the sweep; that is what makes the ~15 ns absolute numbers trustworthy
/// rather than criterion loop noise.
const WORK_SWEEP: &[u32] = &[0, 8, 32];

/// Deterministic, unoptimisable filler work for the "long critical section"
/// regimes.  `inline(never)` so it cannot be specialised differently between
/// the two monomorphisations.
#[inline(never)]
fn filler(iters: u32, seed: u64) -> u64 {
    let mut x = seed;
    for _ in 0..iters {
        x = x
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
    }
    x
}

// ---------------------------------------------------------------------------
// Threaded driver
// ---------------------------------------------------------------------------

/// Runs `total` operations spread over `threads` worker threads and returns
/// the wall-clock **makespan** for the batch (clock started after all workers
/// have rendezvoused, stopped when the last worker completed its quota).
///
/// `op` is `(thread_index, op_index) -> ()` and does exactly one lock
/// acquire/release.
///
/// A worker that completes its `total / threads` quota keeps issuing
/// **uncounted** ops until every other worker is done, so the offered
/// concurrency is a constant N across the whole timed window.  See the module
/// docs for why this matters: without it the harness pays a barging lock a
/// bonus for shedding contention early, which is precisely the property under
/// measurement.
fn run_threaded<F>(threads: usize, total: u64, op: F) -> Duration
where
    F: Fn(usize, u64) + Send + Sync + 'static,
{
    let per = (total / threads as u64).max(1);
    // +1 for the timing thread.
    let gate = Arc::new(Barrier::new(threads + 1));
    let remaining = Arc::new(AtomicUsize::new(threads));
    let op = Arc::new(op);

    let handles: Vec<_> = (0..threads)
        .map(|t| {
            let gate = Arc::clone(&gate);
            let remaining = Arc::clone(&remaining);
            let op = Arc::clone(&op);
            std::thread::spawn(move || {
                gate.wait();
                for i in 0..per {
                    op(t, i);
                }
                remaining.fetch_sub(1, Ordering::Release);
                // Hold the load at N until the slowest worker is done.  These
                // ops are not counted against `total`.
                let mut i = per;
                while remaining.load(Ordering::Acquire) > 0 {
                    op(t, i);
                    i = i.wrapping_add(1);
                }
            })
        })
        .collect();

    gate.wait();
    let t0 = Instant::now();
    for h in handles {
        h.join().expect("worker thread panicked");
    }
    t0.elapsed()
}

// ---------------------------------------------------------------------------
// Regimes, generic over the raw primitive
// ---------------------------------------------------------------------------

fn bench_mutex_uncontended<R: RawMutex + 'static>(
    g: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>,
    name: &str,
    work: u32,
) {
    let m: lock_api::Mutex<R, u64> = lock_api::Mutex::new(0);
    g.bench_function(BenchmarkId::new(name, work), |b| {
        b.iter(|| {
            let mut guard = m.lock();
            *guard = guard.wrapping_add(1);
            if work > 0 {
                *guard ^= filler(work, *guard);
            }
            drop(guard);
            black_box(&m);
        });
    });
}

/// Overhead control for the uncontended regimes: the same body with **no lock
/// at all**, so `impl − no_lock` is the lock's true cost and the harness's
/// share of the ~15 ns absolute number is visible rather than assumed.
fn bench_uncontended_no_lock(
    g: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>,
    work: u32,
) {
    let cell = std::cell::Cell::new(0u64);
    g.bench_function(BenchmarkId::new("no_lock", work), |b| {
        b.iter(|| {
            let mut v = cell.get().wrapping_add(1);
            if work > 0 {
                v ^= filler(work, v);
            }
            cell.set(v);
            black_box(&cell);
        });
    });
}

fn bench_mutex_contended<R>(
    g: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>,
    name: &str,
    threads: usize,
    cs_work: u32,
) where
    R: RawMutex + Send + Sync + 'static,
{
    g.bench_with_input(
        BenchmarkId::new(name, threads),
        &threads,
        |b, &threads| {
            b.iter_custom(|iters| {
                let m: Arc<lock_api::Mutex<R, u64>> =
                    Arc::new(lock_api::Mutex::new(0));
                let m2 = Arc::clone(&m);
                run_threaded(threads, iters, move |_t, i| {
                    let mut guard = m2.lock();
                    *guard = guard.wrapping_add(1);
                    if cs_work > 0 {
                        *guard ^= filler(cs_work, i);
                    }
                    drop(guard);
                })
            });
        },
    );
}

fn bench_rwlock_uncontended<R: RawRwLock + 'static>(
    g: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>,
    name: &str,
    write: bool,
    work: u32,
) {
    let l: lock_api::RwLock<R, u64> = lock_api::RwLock::new(7);
    g.bench_function(BenchmarkId::new(name, work), |b| {
        if write {
            b.iter(|| {
                let mut guard = l.write();
                *guard = guard.wrapping_add(1);
                if work > 0 {
                    *guard ^= filler(work, *guard);
                }
                drop(guard);
                black_box(&l);
            });
        } else {
            b.iter(|| {
                let guard = l.read();
                let mut v = *guard;
                if work > 0 {
                    v ^= filler(work, v);
                }
                drop(guard);
                black_box(v)
            });
        }
    });
}

/// N-thread rwlock regime.  `write_every == 0` means 100 % readers;
/// otherwise every `write_every`-th op on each thread takes the write lock.
fn bench_rwlock_threaded<R>(
    g: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>,
    name: &str,
    threads: usize,
    write_every: u64,
) where
    R: RawRwLock + Send + Sync + 'static,
{
    g.bench_with_input(
        BenchmarkId::new(name, threads),
        &threads,
        |b, &threads| {
            b.iter_custom(|iters| {
                let l: Arc<lock_api::RwLock<R, u64>> =
                    Arc::new(lock_api::RwLock::new(7));
                let l2 = Arc::clone(&l);
                run_threaded(threads, iters, move |t, i| {
                    if write_every != 0
                        && (i.wrapping_add(t as u64 * 7))
                            .is_multiple_of(write_every)
                    {
                        let mut guard = l2.write();
                        *guard = guard.wrapping_add(1);
                        drop(guard);
                    } else {
                        let guard = l2.read();
                        black_box(*guard);
                        drop(guard);
                    }
                })
            });
        },
    );
}

// ---------------------------------------------------------------------------
// Registration
// ---------------------------------------------------------------------------

/// Threaded groups get flat sampling and a small sample count: per-sample
/// work must stay large relative to spawning 64 threads, and 20 flat samples
/// of ~1 s each still gives criterion enough data for a confidence interval.
fn tune_threaded(
    g: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>,
) {
    g.sampling_mode(SamplingMode::Flat);
    g.sample_size(20);
    g.warm_up_time(Duration::from_secs(1));
    g.measurement_time(Duration::from_secs(5));
}

fn mutex_benches(c: &mut Criterion) {
    {
        let mut g = c.benchmark_group("uncontended_mutex");
        for &w in WORK_SWEEP {
            bench_mutex_uncontended::<noxu_sync::RawMutex>(
                &mut g,
                "noxu_sync",
                w,
            );
            bench_mutex_uncontended::<parking_lot::RawMutex>(
                &mut g,
                "parking_lot",
                w,
            );
            bench_uncontended_no_lock(&mut g, w);
        }
        g.finish();
    }

    for (label, cs) in [("short", 0u32), ("long", LONG_CS_WORK)] {
        let mut g = c.benchmark_group(format!("mutex_contended_{label}"));
        tune_threaded(&mut g);
        for &t in THREADS {
            bench_mutex_contended::<noxu_sync::RawMutex>(
                &mut g,
                "noxu_sync",
                t,
                cs,
            );
            bench_mutex_contended::<parking_lot::RawMutex>(
                &mut g,
                "parking_lot",
                t,
                cs,
            );
        }
        g.finish();
    }
}

fn rwlock_benches(c: &mut Criterion) {
    for (label, write) in
        [("uncontended_rwlock_read", false), ("uncontended_rwlock_write", true)]
    {
        let mut g = c.benchmark_group(label);
        for &w in WORK_SWEEP {
            bench_rwlock_uncontended::<noxu_sync::NoxuRawRwLock>(
                &mut g,
                "noxu_sync",
                write,
                w,
            );
            bench_rwlock_uncontended::<parking_lot::RawRwLock>(
                &mut g,
                "parking_lot",
                write,
                w,
            );
            bench_uncontended_no_lock(&mut g, w);
        }
        g.finish();
    }

    for (label, write_every) in
        [("rwlock_read_only", 0u64), ("rwlock_read_heavy", WRITE_EVERY)]
    {
        let mut g = c.benchmark_group(label);
        tune_threaded(&mut g);
        for &t in THREADS {
            bench_rwlock_threaded::<noxu_sync::NoxuRawRwLock>(
                &mut g,
                "noxu_sync",
                t,
                write_every,
            );
            bench_rwlock_threaded::<parking_lot::RawRwLock>(
                &mut g,
                "parking_lot",
                t,
                write_every,
            );
        }
        g.finish();
    }
}

/// Sanity gate: a bench that silently locks nothing would report a
/// spectacular win.  This asserts the primitives actually mutate shared
/// state under the driver used above, for both impls, before any timing is
/// reported.  Cheap (runs in ms) and fails loudly if the harness rots.
fn self_check(_c: &mut Criterion) {
    fn check_mutex<R: RawMutex + Send + Sync + 'static>() {
        let m: Arc<lock_api::Mutex<R, u64>> = Arc::new(lock_api::Mutex::new(0));
        // The driver issues extra *uncounted* ops to hold concurrency at N
        // (see `run_threaded`), so the total op count is not known up front.
        // Instead compare the lock-protected counter against an atomic
        // incremented in the same critical section: they can only diverge if
        // an update was lost, i.e. if mutual exclusion failed.
        let atomic = Arc::new(AtomicU64::new(0));
        let m2 = Arc::clone(&m);
        let a2 = Arc::clone(&atomic);
        run_threaded(8, 8_000, move |_t, _i| {
            let mut g = m2.lock();
            *g = g.wrapping_add(1);
            a2.fetch_add(1, Ordering::Relaxed);
        });
        assert_eq!(
            *m.lock(),
            atomic.load(Ordering::Relaxed),
            "mutex lost updates: not mutually exclusive"
        );
    }
    fn check_rwlock<R: RawRwLock + Send + Sync + 'static>() {
        let l: Arc<lock_api::RwLock<R, u64>> =
            Arc::new(lock_api::RwLock::new(0));
        let saw_torn = Arc::new(AtomicBool::new(false));
        let l2 = Arc::clone(&l);
        let torn = Arc::clone(&saw_torn);
        run_threaded(8, 8_000, move |t, i| {
            if t == 0 {
                let mut g = l2.write();
                // Writers publish an even value; readers must never see odd.
                *g = g.wrapping_add(1);
                *g = g.wrapping_add(1);
            } else {
                let g = l2.read();
                if !(*g).is_multiple_of(2) {
                    torn.store(true, Ordering::Relaxed);
                }
                black_box(i);
            }
        });
        assert!(
            !saw_torn.load(Ordering::Relaxed),
            "reader observed a mid-write value: shared/exclusive exclusion broken"
        );
    }
    check_mutex::<noxu_sync::RawMutex>();
    check_mutex::<parking_lot::RawMutex>();
    check_rwlock::<noxu_sync::NoxuRawRwLock>();
    check_rwlock::<parking_lot::RawRwLock>();
}

criterion_group!(benches, self_check, mutex_benches, rwlock_benches);
criterion_main!(benches);
