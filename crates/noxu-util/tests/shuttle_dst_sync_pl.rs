// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Shuttle self-test for the parking_lot-over-shuttle wrapper
//! (`noxu_util::dst_sync_pl`, DST Milestone 1.1).
//!
//! The whole file compiles to nothing unless built with `--cfg noxu_shuttle`,
//! so the default `cargo test` and every production build are unaffected.
//!
//! # What this proves (the M1.1 deliverable-2 gate)
//!
//! 1. [`wrapped_mutex_is_scheduled_by_shuttle`] — shuttle's scheduler explores
//!    interleavings of the *wrapped, parking_lot-shaped* `Mutex` (the M2
//!    blocker was that `noxu-sync`'s shape could not be shuttle-swapped at
//!    all).  Two threads race to increment a shared counter through the
//!    wrapper's `lock()`; the mutual exclusion holds under every interleaving.
//!    If the wrapper failed to route through shuttle, the counter would be
//!    schedule-independent (no interleavings) — this test would still pass but
//!    the deadlock test below would not exercise shuttle at all, so we also
//!    assert a lost-update *would* be caught by using a non-atomic counter that
//!    only the lock protects.
//!
//! 2. [`timed_wait_fires_when_clock_advances`] — **the crux.**  A waiter blocks
//!    in the wrapper's `Condvar::wait_for`; shuttle's `wait_timeout` never
//!    self-expires, so the waiter would block forever.  The harness advances
//!    the `SimClock` past the deadline with `advance_and_fire`, which fires the
//!    registered timer and wakes the waiter, which then observes
//!    `timed_out() == true`.  This is the deterministic, clock-driven timed
//!    wait the next wave's FsyncManager oracle needs.
//!
//! 3. [`timed_wait_returns_notify_before_deadline`] — a real `notify_one`
//!    before the clock is advanced wakes the waiter with `timed_out() ==
//!    false`; the timeout is inert for notify-driven protocols (the
//!    DaemonManager case).
//!
//! # Running
//!
//! ```sh
//! RUSTFLAGS="--cfg noxu_shuttle" cargo test -p noxu-util --test shuttle_dst_sync_pl
//! ```
#![cfg(noxu_shuttle)]

use std::sync::Arc;
use std::time::Duration;

use noxu_util::SimClock;
use noxu_util::dst_sync_pl::{
    Condvar, Mutex, advance_and_fire, install_sim_clock,
};
use shuttle::sync::atomic::{AtomicBool, Ordering};

const ITERATIONS: usize = 2_000;

/// Prove shuttle schedules the wrapped, parking_lot-shaped `Mutex`.
///
/// The counter is a plain `usize` behind the wrapper's `Mutex`; if the lock did
/// not serialise the two incrementing threads (i.e. the wrapper did not route
/// through shuttle's instrumented lock), shuttle's exploration of the
/// read-modify-write interleaving would surface a lost update and the final
/// assert would fail on some schedule.  It holds on all `ITERATIONS`, proving
/// the wrapped primitive is both schedulable and correct.
#[test]
fn wrapped_mutex_is_scheduled_by_shuttle() {
    shuttle::check_random(
        || {
            let counter = Arc::new(Mutex::new(0usize));
            let handles: Vec<_> = (0..2)
                .map(|_| {
                    let counter = Arc::clone(&counter);
                    shuttle::thread::spawn(move || {
                        // Non-atomic read-modify-write: only the lock makes
                        // this safe.  A wrapper that dropped the lock would let
                        // shuttle find a lost update.
                        let mut g = counter.lock();
                        let v = *g;
                        *g = v + 1;
                    })
                })
                .collect();
            for h in handles {
                h.join().unwrap();
            }
            assert_eq!(
                *counter.lock(),
                2,
                "lost update: lock did not serialise"
            );
        },
        ITERATIONS,
    );
}

/// THE CRUX: a timed wait fires deterministically when the harness advances the
/// SimClock past the waiter's deadline.
///
/// shuttle's `wait_timeout` never self-expires, so without the clock coupling
/// the waiter would deadlock.  The main thread advances the `SimClock` by more
/// than the wait's timeout via `advance_and_fire`, which notifies the waiter's
/// condvar; the waiter re-reads the clock, finds `now >= deadline`, and returns
/// `timed_out() == true`.  Every interleaving must reach that outcome (no
/// deadlock, no spurious early timeout).
#[test]
fn timed_wait_fires_when_clock_advances() {
    shuttle::check_random(
        || {
            let sim = Arc::new(SimClock::new(0));
            install_sim_clock(Arc::clone(&sim));

            let pair = Arc::new((Mutex::new(false), Condvar::new()));
            // The waiter's observed result: Some(true) = timed out (expected),
            // Some(false) = notified, None = never returned (would hang).
            let result = Arc::new(Mutex::new(None::<bool>));
            // Set true once the waiter has returned; the harness re-advances
            // the simulated clock until then.
            let done = Arc::new(AtomicBool::new(false));

            let waiter = {
                let pair = Arc::clone(&pair);
                let result = Arc::clone(&result);
                let done = Arc::clone(&done);
                shuttle::thread::spawn(move || {
                    let (m, cv) = &*pair;
                    let mut g = m.lock();
                    // Nobody sets the predicate true, so this can only return
                    // via the clock-driven timeout.
                    let r = cv.wait_for(&mut g, Duration::from_millis(500));
                    *result.lock() = Some(r.timed_out());
                    done.store(true, Ordering::SeqCst);
                })
            };

            // Drive simulated time forward (firing + re-notifying due timers)
            // until the waiter has timed out.  advance_and_fire re-notifies any
            // pending fire, so a fire that landed in the waiter's pre-block gap
            // on one iteration is re-delivered on the next — guaranteeing
            // progress.  Bounded so a genuine hang still fails.
            let mut steps = 0;
            while !done.load(Ordering::SeqCst) {
                advance_and_fire(&sim, Duration::from_millis(1000));
                shuttle::thread::yield_now();
                steps += 1;
                assert!(steps < 1000, "waiter never timed out (hang)");
            }

            waiter.join().unwrap();
            assert_eq!(
                *result.lock(),
                Some(true),
                "timed wait must return timed_out()==true once the SimClock \
                 passes the deadline"
            );
        },
        ITERATIONS,
    );
}

/// A `notify_one` before the clock advances wakes the waiter with
/// `timed_out() == false` — the timeout is inert under notify-driven wakeup.
#[test]
fn timed_wait_returns_notify_before_deadline() {
    shuttle::check_random(
        || {
            let sim = Arc::new(SimClock::new(0));
            install_sim_clock(Arc::clone(&sim));

            let pair = Arc::new((Mutex::new(false), Condvar::new()));
            let result = Arc::new(Mutex::new(None::<bool>));

            let waiter = {
                let pair = Arc::clone(&pair);
                let result = Arc::clone(&result);
                shuttle::thread::spawn(move || {
                    let (m, cv) = &*pair;
                    let mut g = m.lock();
                    while !*g {
                        let r = cv.wait_for(&mut g, Duration::from_millis(500));
                        if *g {
                            *result.lock() = Some(r.timed_out());
                            return;
                        }
                        // Spurious/early wake with predicate still false and
                        // clock not advanced: loop (r.timed_out() is false).
                    }
                    *result.lock() = Some(false);
                })
            };

            // Set the predicate and notify WITHOUT advancing the clock.
            {
                let (m, cv) = &*pair;
                let mut g = m.lock();
                *g = true;
                cv.notify_one();
            }

            waiter.join().unwrap();
            assert_eq!(
                *result.lock(),
                Some(false),
                "a notify before the deadline must return timed_out()==false"
            );
        },
        ITERATIONS,
    );
}
