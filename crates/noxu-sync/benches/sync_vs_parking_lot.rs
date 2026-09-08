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
//! | `mutex_uncontended` | 1 thread, lock/incr/unlock | The overwhelmingly common case: warm read path, log-buffer pin, stats bump |
//! | `mutex_contended_short/{N}` | N threads, ~1 ns critical section | Hot shared counters (memory budget, evictor state) |
//! | `mutex_contended_long/{N}` | N threads, ~100 ns critical section | Log-buffer append, checkpoint bookkeeping |
//! | `rwlock_uncontended_read` / `_write` | 1 thread | Tree-latch acquire on an uncontended node |
//! | `rwlock_read_only/{N}` | N threads, 100 % shared | Hand-over-hand tree descent on hot upper INs |
//! | `rwlock_read_heavy/{N}` | N threads, 1-in-1024 exclusive | Tree latch with occasional split/eviction |
//!
//! ### Multi-threaded timing convention
//!
//! Threaded regimes use `iter_custom`: criterion's `iters` is the **total**
//! op count, split evenly across the N worker threads.  Threads are spawned,
//! rendezvous on a `Barrier`, and only then is the clock started; the
//! reported duration is wall-clock until the last thread finishes.  So the
//! reported `time` is **aggregate ns per operation** (i.e. the reciprocal of
//! system-wide lock throughput), *not* per-thread latency.  Lower is better
//! and the number is directly comparable across thread counts.
//!
//! Thread spawn/join is outside the timed window except for the join tail;
//! `SamplingMode::Flat` keeps per-sample work large so that residual
//! spawn/teardown cost is a small, symmetric constant.
//!
//! Run: `cargo bench -p noxu-sync --bench sync_vs_parking_lot`
//! Single regime: `... --bench sync_vs_parking_lot -- mutex_uncontended`

use criterion::{
    BenchmarkId, Criterion, SamplingMode, criterion_group, criterion_main,
};
use lock_api::{RawMutex, RawRwLock};
use std::hint::black_box;
use std::sync::atomic::{AtomicBool, Ordering};
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
/// the wall-clock time for the whole batch (clock started after all workers
/// have rendezvoused).
///
/// `op` is `(thread_index, op_index) -> ()` and does exactly one lock
/// acquire/release.
fn run_threaded<F>(threads: usize, total: u64, op: F) -> Duration
where
    F: Fn(usize, u64) + Send + Sync + 'static,
{
    let per = (total / threads as u64).max(1);
    // +1 for the timing thread.
    let gate = Arc::new(Barrier::new(threads + 1));
    let op = Arc::new(op);

    let handles: Vec<_> = (0..threads)
        .map(|t| {
            let gate = Arc::clone(&gate);
            let op = Arc::clone(&op);
            std::thread::spawn(move || {
                gate.wait();
                for i in 0..per {
                    op(t, i);
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
) {
    let m: lock_api::Mutex<R, u64> = lock_api::Mutex::new(0);
    g.bench_function(name, |b| {
        b.iter(|| {
            let mut guard = m.lock();
            *guard = guard.wrapping_add(1);
            drop(guard);
            black_box(&m);
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
) {
    let l: lock_api::RwLock<R, u64> = lock_api::RwLock::new(7);
    g.bench_function(name, |b| {
        if write {
            b.iter(|| {
                let mut guard = l.write();
                *guard = guard.wrapping_add(1);
                drop(guard);
                black_box(&l);
            });
        } else {
            b.iter(|| {
                let guard = l.read();
                let v = *guard;
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
        let mut g = c.benchmark_group("mutex_uncontended");
        bench_mutex_uncontended::<noxu_sync::RawMutex>(&mut g, "noxu_sync");
        bench_mutex_uncontended::<parking_lot::RawMutex>(&mut g, "parking_lot");
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
    {
        let mut g = c.benchmark_group("rwlock_uncontended_read");
        bench_rwlock_uncontended::<noxu_sync::NoxuRawRwLock>(
            &mut g,
            "noxu_sync",
            false,
        );
        bench_rwlock_uncontended::<parking_lot::RawRwLock>(
            &mut g,
            "parking_lot",
            false,
        );
        g.finish();
    }
    {
        let mut g = c.benchmark_group("rwlock_uncontended_write");
        bench_rwlock_uncontended::<noxu_sync::NoxuRawRwLock>(
            &mut g,
            "noxu_sync",
            true,
        );
        bench_rwlock_uncontended::<parking_lot::RawRwLock>(
            &mut g,
            "parking_lot",
            true,
        );
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
        let m2 = Arc::clone(&m);
        run_threaded(8, 8_000, move |_t, _i| {
            let mut g = m2.lock();
            *g += 1;
        });
        assert_eq!(
            *m.lock(),
            8_000,
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
