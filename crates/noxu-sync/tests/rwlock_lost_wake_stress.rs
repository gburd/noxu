// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Focused regression guard for the `NoxuRawRwLock` reader-park lost-wake
//! bug.
//!
//! This is the real-primitive companion to the loom model
//! (`loom_rwlock_lost_wake.rs`): it hammers the ACTUAL `NoxuRawRwLock` with
//! the lock SHAPE that reproduced the engine hang — a tight read-relay (the
//! `evict_closed_databases` sweeper) plus writers that reserve+drain (the
//! `open_database`/`close_database` openers) — and a watchdog that catches a
//! stranded waiter directly rather than relying on a whole test timing out.
//!
//! ## Scope, honestly stated
//!
//! The AUTHORITATIVE aarch64 witness for this bug is the engine-level
//! `noxu-dbi` test
//! `environment_impl::tests::dbevict1_concurrent_open_close_never_evicts_in_use_database`
//! run in a loop: it reproduces at a ~5% hang rate per run on Graviton and 0%
//! on x86. The underlying lost wake is a StoreLoad race whose window is
//! extremely narrow; in the full engine it is widened by the surrounding
//! allocator / `Arc` refcount / daemon-thread cache-line traffic. This
//! isolated microtest did NOT, on the hardware measured, reproduce the race
//! on the pre-fix code within a bounded run — the bare two-lock workload does
//! not reliably open the window the engine does. It is therefore kept as a
//! fast REGRESSION GUARD (and a documented live-primitive exercise of the
//! reserve/drain/notify + reader-park paths), not as the primary reproducer.
//! The rigorous before/after evidence lives in the loom model (which fails on
//! the pre-fix orderings and passes on the fix) and the dbevict1 loop.
//!
//! ## The watchdog
//!
//! A reader stranded by the lost-wake bug sleeps in `futex_wait(&state)`
//! forever even though the lock is free (no `WRITE_LOCKED`). The watchdog
//! samples the invariant
//!
//! > the lock is not write-locked, yet a thread is recorded waiting
//! > (`get_n_waiters() > 0`) and the slowest reader makes no progress
//!
//! for longer than `STUCK_WINDOW`, and fails loudly with that diagnosis
//! rather than hanging, so any regression is a red test and not a multi-hour
//! CI timeout.
//!
//! Run:
//!
//! ```sh
//! cargo test -p noxu-sync --release --test rwlock_lost_wake_stress -- --nocapture
//! ```

use noxu_sync::RwLock;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

/// How long a reader may make zero progress while the lock is observed free
/// before we call it a lost wake. Generous enough to never false-positive on
/// a merely-busy scheduler (normal progress is microseconds), tight enough to
/// catch a genuinely stranded waiter fast.
const STUCK_WINDOW: Duration = Duration::from_secs(2);

/// One stress round in the dbevict1 lock SHAPE: one sweeper doing a tight
/// read-relay (empty body, maximum release frequency — the
/// `evict_closed_databases` loop) plus `n_openers` threads doing
/// write-then-read (the `open_database`/`close_database` openers). Returns
/// once every opener has completed `iters` cycles, or panics if the watchdog
/// detects a stranded thread.
fn stress_round(n_openers: usize, iters: u64) {
    let lock = Arc::new(RwLock::new(0u64));
    let stop = Arc::new(AtomicBool::new(false));
    let progress: Arc<Vec<AtomicU64>> =
        Arc::new((0..n_openers).map(|_| AtomicU64::new(0)).collect());

    let mut handles = Vec::new();

    // Sweeper: tight read-relay. Each iteration takes and drops the read lock
    // with no work between, so the reader count bounces and release events
    // land at maximum density — the condition under which a writer's release
    // can race a reader mid-park.
    {
        let lock = Arc::clone(&lock);
        let stop = Arc::clone(&stop);
        handles.push(thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                let g = lock.read();
                std::hint::black_box(&*g);
            }
        }));
    }

    // Openers: write (reserve+drain while the sweeper relays reads) then read
    // (the `close_database` read that parked in the field report).
    for o in 0..n_openers {
        let lock = Arc::clone(&lock);
        let progress = Arc::clone(&progress);
        handles.push(thread::spawn(move || {
            while progress[o].load(Ordering::Relaxed) < iters {
                {
                    let mut g = lock.write();
                    *g = g.wrapping_add(1);
                }
                {
                    let g = lock.read();
                    std::hint::black_box(&*g);
                }
                progress[o].fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    // Watchdog: the lost-wake detector. Samples progress; if the SLOWEST
    // opener makes no progress for STUCK_WINDOW while the lock is repeatedly
    // observed admissible (not write-locked), a thread is stranded.
    let deadline_round = Instant::now() + Duration::from_secs(60);
    loop {
        let min_progress = (0..n_openers)
            .map(|r| progress[r].load(Ordering::Relaxed))
            .min()
            .unwrap_or(iters);
        if min_progress >= iters {
            break; // every opener finished — round complete
        }

        // Watch the slowest opener for a stall.
        let watched_start = min_progress;
        let watch_until = Instant::now() + STUCK_WINDOW;
        let mut saw_free_while_stalled = false;
        while Instant::now() < watch_until {
            let now_min = (0..n_openers)
                .map(|r| progress[r].load(Ordering::Relaxed))
                .min()
                .unwrap_or(iters);
            if now_min > watched_start || now_min >= iters {
                break; // progress resumed — not stuck
            }
            // SAFETY: `raw()` only hands out the raw lock for reading its
            // atomic state words; we call read-only observers.
            let (write_locked, waiters) = unsafe {
                let raw = lock.raw();
                (raw.is_write_locked(), raw.get_n_waiters())
            };
            // The damning observation: the lock is NOT write-held, yet a
            // thread is recorded waiting and the slowest opener is not moving.
            if !write_locked && waiters > 0 {
                saw_free_while_stalled = true;
            }
            thread::sleep(Duration::from_millis(20));
        }

        let now_min = (0..n_openers)
            .map(|r| progress[r].load(Ordering::Relaxed))
            .min()
            .unwrap_or(iters);
        if now_min == watched_start && saw_free_while_stalled {
            stop.store(true, Ordering::Relaxed);
            panic!(
                "LOST WAKE: opener stalled at {watched_start}/{iters} cycles \
                 for {STUCK_WINDOW:?} while the lock was repeatedly observed \
                 free (not write-locked) with a waiter recorded. A parked \
                 thread was never woken."
            );
        }
        assert!(
            Instant::now() < deadline_round,
            "round exceeded 60s wall clock without a clean lost-wake verdict \
             (openers stuck at {now_min}/{iters}) -- treat as a hang/regression"
        );
    }

    stop.store(true, Ordering::Relaxed);
    for h in handles {
        h.join().unwrap();
    }
}

/// A tight read-relay sweeper + several reserve/release openers: the lock
/// shape that reproduced the engine's `db_map` hang. Several rounds at a few
/// opener counts to vary the park-window timing.
#[test]
fn readers_never_stranded_by_a_reserving_writer() {
    // Modest in debug (slow), heavier in release where the race is live.
    let iters: u64 = if cfg!(debug_assertions) { 2_000 } else { 20_000 };
    for &n in &[2usize, 4, 8, 16] {
        stress_round(n, iters);
    }
}

/// A single pattern run for longer at a high opener count — many openers
/// reserving against the sweeper's read-relay, each release a chance to drop
/// a wake.
#[test]
fn high_contention_reader_relay_against_writer() {
    let iters: u64 = if cfg!(debug_assertions) { 1_500 } else { 15_000 };
    stress_round(32, iters);
}
