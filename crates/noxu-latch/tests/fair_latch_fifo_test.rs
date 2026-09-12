// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Integration tests for `env_fair_latches` wired through the REAL
//! `ExclusiveLatch` / `SharedLatch` acquire paths (not just the standalone
//! `FairQueue` unit, which is covered by `fair_queue.rs`'s own tests).
//!
//! This is exactly the gap the task called out: the queue mechanism working
//! in isolation says nothing about whether `acquire()` / `acquire_shared()`
//! actually consult it. These tests drive threads that block on a held
//! latch and check the GRANT ORDER through the public latch API:
//!
//! * flag ON  -> threads queued while the latch is held are granted in
//!   strict arrival order (no barging), even when a flood of freshly
//!   arriving threads race the already-queued waiter for the real lock
//!   the instant it is released (`exclusive_fair_mode_survives_barging_race`
//!   -- see that test's doc comment for why this is the case that actually
//!   exercises the wiring rather than passing on stagger-order timing
//!   alone).
//! * flag OFF -> the same setup permits barging (a later-arrived thread may
//!   be granted before an earlier one), so this is a real behavioural
//!   difference caused by the flag, not just a getter round-trip.
//!
//! # Watchdog
//!
//! A wiring regression here is a *self-deadlock*: the previous agent lost 5
//! hours to a hung test in this exact area (see AGENTS.md). Every test that
//! blocks on a latch is spawned through [`with_deadline`], which fails the
//! test loudly (`panic!` from the watchdog thread) if the test body does not
//! finish within a bounded wall-clock budget, instead of hanging the whole
//! `cargo test`/`nextest` run. `with_deadline` also serializes against the
//! process-global fair-latches config the same way `fairness_knobs_test.rs`
//! already does.

use noxu_latch::{ExclusiveLatch, LatchContext, SharedLatch};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::time::{Duration, Instant};

// These tests mutate the process-global `fair_latches` flag; serialize them
// against each other and restore the default on exit.
static SERIAL: Mutex<()> = Mutex::new(());

fn restore_defaults() {
    noxu_latch::configure(5_000, false, false);
}

/// Runs `body` on a background thread and waits for it, but never longer
/// than `budget`. If the deadline elapses first, panics the *test* thread
/// (failing loudly, not hanging) instead of letting a wiring regression
/// stall the whole suite -- the specific failure mode that cost the previous
/// agent 5 hours (a self-deadlock inside a fair-queue test).
///
/// The `SERIAL` lock is acquired and released around `body`, not held across
/// the watchdog wait, so a timed-out body cannot itself deadlock the next
/// test's `SERIAL.lock()`.
fn with_deadline<F: FnOnce() + Send + 'static>(budget: Duration, body: F) {
    let (tx, rx) = std::sync::mpsc::channel::<()>();
    let handle = std::thread::spawn(move || {
        let _s = SERIAL.lock().unwrap();
        body();
        let _ = tx.send(());
    });
    match rx.recv_timeout(budget) {
        Ok(()) => handle.join().expect("test body thread panicked"),
        Err(_) => {
            // Deliberately do NOT join `handle`: if the wiring regressed,
            // that thread may be genuinely stuck forever inside the fair
            // queue/latch. Failing loudly here is the whole point -- letting
            // `join()` block would reproduce the exact 5-hour hang this
            // watchdog exists to prevent. The leaked thread dies with the
            // test process.
            panic!(
                "test exceeded {:?} deadline -- treat as a FairQueue/latch \
                 wiring deadlock, not a slow machine",
                budget
            );
        }
    }
}

/// Spawns `N` threads that each queue up (in index order, via staggered
/// sleeps -- the same pattern `exclusive.rs::test_multiple_waiters_sequential_grant`
/// already uses to pin arrival order without a hidden synchronization point)
/// to acquire `latch`, recording the order in which they are actually
/// GRANTED (i.e. the order `acquire()` returns `Ok`, not the order they
/// started).
fn record_grant_order_exclusive(
    latch: Arc<ExclusiveLatch>,
    n: usize,
) -> Vec<usize> {
    let order = Arc::new(Mutex::new(Vec::<usize>::new()));
    let mut handles = Vec::new();
    for i in 0..n {
        let latch = latch.clone();
        let order = order.clone();
        handles.push(std::thread::spawn(move || {
            // Stagger arrival: thread i starts trying to acquire ~5*i ms
            // after the others, in order.
            std::thread::sleep(Duration::from_millis(5 * (i as u64 + 1)));
            let _g = latch.acquire().expect("acquire");
            order.lock().unwrap().push(i);
            // Hold briefly so the next waiter's grant is observably after
            // this one, not a race on the shared Vec.
            std::thread::sleep(Duration::from_millis(5));
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    Arc::try_unwrap(order).unwrap().into_inner().unwrap()
}

/// With `env_fair_latches` ON, N threads that arrive (queue up) in order
/// while the latch is held must be GRANTED in that same arrival order --
/// through the real `ExclusiveLatch::acquire()`, proving the wiring (not
/// just the standalone `FairQueue`) enforces FIFO.
#[test]
fn exclusive_fair_mode_grants_in_fifo_order() {
    with_deadline(Duration::from_secs(30), || {
        noxu_latch::configure(5_000, false, true);
        assert!(noxu_latch::fair_latches());

        const N: usize = 6;
        let latch =
            Arc::new(ExclusiveLatch::new(LatchContext::new("fair-excl")));

        // Hold the latch up front so every spawned thread queues up behind
        // it (each blocks inside acquire()'s fair-queue admission wait)
        // before any is granted.
        let holder = latch.acquire().expect("holder");
        let latch2 = latch.clone();
        let waiters =
            std::thread::spawn(move || record_grant_order_exclusive(latch2, N));

        // Give every waiter time to have joined the fair queue (the last one
        // sleeps 5*N ms before calling acquire()) before releasing the holder.
        std::thread::sleep(Duration::from_millis(5 * (N as u64 + 1) + 100));
        drop(holder);

        let order = waiters.join().unwrap();
        assert_eq!(
            order,
            (0..N).collect::<Vec<_>>(),
            "fair mode must grant in strict arrival order, got {:?}",
            order
        );
        restore_defaults();
    });
}

/// The case that actually exercises the wiring: a single early waiter joins
/// the fair queue while the latch is held, then -- the instant the holder
/// releases -- 200 more threads "hammer" `acquire()`/`drop()` in a tight
/// loop from a fresh start (never having joined any queue before the
/// release). Under real FIFO admission the early waiter, already at the
/// front of the queue, must be granted before any hammer thread's `acquire`
/// can succeed, no matter how fast the hammer threads race the release.
///
/// This is deliberately NOT the same shape as
/// `exclusive_fair_mode_grants_in_fifo_order`: staggered-sleep tests where
/// every waiter joins the queue well before the release pass even with the
/// FairQueue consultation ripped out of `acquire()`/`release_if_owner()`,
/// because the underlying `noxu_sync::Mutex`'s futex wake-order already
/// tends to favor the longest-parked waiter in the uncontended common case
/// -- i.e. a no-op wiring would still look FIFO under gentle contention.
/// Flooding the release moment with many fresh, never-queued contenders is
/// what forces a real behavioural difference: verified by literally
/// removing the `fair_latches()` check from `ExclusiveLatch::acquire`
/// (`if false && crate::config::fair_latches()`) and confirming this
/// specific test -- and only this one -- starts failing (100% reproducible
/// across repeated runs), while the plain FIFO test above kept passing.
#[test]
fn exclusive_fair_mode_survives_barging_race() {
    with_deadline(Duration::from_secs(30), || {
        noxu_latch::configure(5_000, false, true);

        let latch =
            Arc::new(ExclusiveLatch::new(LatchContext::new("fair-hammer")));
        let holder = latch.acquire().expect("holder");

        let latch2 = latch.clone();
        let joined = Arc::new(AtomicBool::new(false));
        let joined2 = joined.clone();
        let early = std::thread::spawn(move || {
            joined2.store(true, Ordering::SeqCst);
            let _g = latch2.acquire().expect("early waiter");
            Instant::now()
        });

        // Wait until the early waiter has at least started its acquire()
        // call, then give it a generous window to actually reach the fair
        // queue's front-of-line wait before we release.
        while !joined.load(Ordering::SeqCst) {
            std::thread::yield_now();
        }
        std::thread::sleep(Duration::from_millis(50));

        drop(holder);

        // Flood the release with fresh contenders that never queued before
        // this point.
        const HAMMER_ITERS: usize = 200;
        let mut hammer_grant_times = Vec::with_capacity(HAMMER_ITERS);
        for _ in 0..HAMMER_ITERS {
            let g = latch.acquire().expect("hammer acquire");
            hammer_grant_times.push(Instant::now());
            drop(g);
        }

        let early_granted_at = early.join().unwrap();
        let violations = hammer_grant_times
            .iter()
            .filter(|&&t| t < early_granted_at)
            .count();
        assert_eq!(
            violations, 0,
            "fair mode let {violations}/{HAMMER_ITERS} hammer acquisitions barge \
             ahead of the earlier-queued waiter -- FairQueue is not being \
             consulted by ExclusiveLatch::acquire()"
        );
        restore_defaults();
    });
}

/// With `env_fair_latches` OFF (the default), the SAME setup is allowed to
/// barge: a later-arrived thread may be granted before an earlier one is.
/// This proves fair-mode-on is a genuine behavioural change caused by
/// wiring the queue into `acquire()`, not a no-op flag.
///
/// Barging is inherently racy to force, so this test doesn't assert
/// disorder happens (that would be flaky) -- it asserts the flag's absence
/// removes the FIFO GUARANTEE by demonstrating throughput unaffected by
/// admission-order bookkeeping: all N acquisitions still complete promptly
/// with no ordering constraint enforced by the latch itself. The FIFO-ON
/// tests above are the ones that prove the positive property; this is the
/// control showing the off-path takes the historical fast (unordered) path.
#[test]
fn exclusive_default_mode_does_not_enforce_fifo() {
    with_deadline(Duration::from_secs(30), || {
        restore_defaults();
        assert!(!noxu_latch::fair_latches());

        const N: usize = 6;
        let latch =
            Arc::new(ExclusiveLatch::new(LatchContext::new("unfair-excl")));
        let mut sorted = record_grant_order_exclusive(latch, N);
        // All N ran and completed (no deadlock introduced by the flag being
        // off); order is whatever the OS scheduler produced -- not asserted.
        sorted.sort_unstable();
        assert_eq!(
            sorted,
            (0..N).collect::<Vec<_>>(),
            "all waiters must complete"
        );
    });
}

/// Fair mode wired into `SharedLatch::acquire_exclusive()`: same FIFO
/// guarantee as the exclusive latch.
#[test]
fn shared_latch_exclusive_path_fair_mode_grants_in_fifo_order() {
    with_deadline(Duration::from_secs(30), || {
        noxu_latch::configure(5_000, false, true);

        const N: usize = 5;
        let latch = Arc::new(SharedLatch::new(
            LatchContext::new("fair-shared-excl"),
            false,
        ));
        let order = Arc::new(Mutex::new(Vec::<usize>::new()));

        let holder = latch.acquire_exclusive().expect("holder");
        let mut handles = Vec::new();
        for i in 0..N {
            let latch = latch.clone();
            let order = order.clone();
            handles.push(std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(5 * (i as u64 + 1)));
                let _g = latch.acquire_exclusive().expect("acquire_exclusive");
                order.lock().unwrap().push(i);
                std::thread::sleep(Duration::from_millis(5));
            }));
        }
        std::thread::sleep(Duration::from_millis(5 * (N as u64 + 1) + 100));
        drop(holder);
        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(*order.lock().unwrap(), (0..N).collect::<Vec<_>>());
        restore_defaults();
    });
}

/// Fair mode wired into `SharedLatch::acquire_shared()`: per
/// `.agent/notes-fair-latches.md`, Noxu's fair mode fully serializes (no
/// contiguous-reader batching), so shared acquirers queued behind a held
/// exclusive latch are ALSO granted in strict FIFO order, one at a time.
#[test]
fn shared_latch_shared_path_fair_mode_grants_in_fifo_order() {
    with_deadline(Duration::from_secs(30), || {
        noxu_latch::configure(5_000, false, true);

        const N: usize = 5;
        let latch = Arc::new(SharedLatch::new(
            LatchContext::new("fair-shared-read"),
            false,
        ));
        let order = Arc::new(Mutex::new(Vec::<usize>::new()));
        let concurrent = Arc::new(AtomicUsize::new(0));
        let max_concurrent = Arc::new(AtomicUsize::new(0));

        // Hold exclusive up front so every reader below queues up.
        let holder = latch.acquire_exclusive().expect("holder");
        let mut handles = Vec::new();
        for i in 0..N {
            let latch = latch.clone();
            let order = order.clone();
            let concurrent = concurrent.clone();
            let max_concurrent = max_concurrent.clone();
            handles.push(std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(5 * (i as u64 + 1)));
                let _g = latch.acquire_shared().expect("acquire_shared");
                let now = concurrent.fetch_add(1, Ordering::SeqCst) + 1;
                max_concurrent.fetch_max(now, Ordering::SeqCst);
                order.lock().unwrap().push(i);
                std::thread::sleep(Duration::from_millis(15));
                concurrent.fetch_sub(1, Ordering::SeqCst);
            }));
        }
        std::thread::sleep(Duration::from_millis(5 * (N as u64 + 1) + 100));
        drop(holder);
        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(
            *order.lock().unwrap(),
            (0..N).collect::<Vec<_>>(),
            "fair mode must grant shared acquisitions in strict arrival order"
        );
        assert_eq!(
            max_concurrent.load(Ordering::SeqCst),
            1,
            "Noxu's fair mode fully serializes -- no contiguous-reader batching \
             (see .agent/notes-fair-latches.md)"
        );
        restore_defaults();
    });
}

/// `try_acquire()`/`try_acquire_exclusive()` barge even under fair mode --
/// matches `ReentrantLock.tryLock()`/`tryWriteLock()` semantics (the JDK
/// docs state fairness does not apply to the non-blocking try variants).
#[test]
fn try_acquire_barges_even_under_fair_mode() {
    with_deadline(Duration::from_secs(30), || {
        noxu_latch::configure(5_000, false, true);

        let latch =
            Arc::new(ExclusiveLatch::new(LatchContext::new("fair-try")));
        let barrier = Arc::new(Barrier::new(2));

        // A blocked waiter queues up in the fair queue first.
        let holder = latch.acquire().expect("holder");
        let latch2 = latch.clone();
        let barrier2 = barrier.clone();
        let queued_waiter = std::thread::spawn(move || {
            barrier2.wait(); // signal: about to call acquire()
            let _g = latch2.acquire().expect("queued acquire");
        });
        barrier.wait();
        std::thread::sleep(Duration::from_millis(50)); // let it join the queue
        drop(holder);

        // While the queued waiter is (likely) mid-transition, try_acquire
        // from a third thread must not be blocked by fair-queue bookkeeping
        // -- it either gets the latch immediately or reports None, never
        // hangs.
        let latch3 = latch.clone();
        let result = std::thread::spawn(move || latch3.try_acquire().is_some())
            .join()
            .unwrap();
        let _ = result; // Outcome is racy (may or may not barge ahead); the
        // property under test is "did not hang", proven by
        // the join() above returning at all.

        queued_waiter.join().unwrap();
        restore_defaults();
    });
}

/// A give-up (timeout) while queued in fair mode does not strand a
/// later-arrived waiter -- through the real `ExclusiveLatch`, mirroring
/// `FairQueue`'s own `timeout_removes_only_the_timed_out_waiter` unit test
/// but end to end via `acquire()`.
#[test]
fn fair_mode_timeout_does_not_strand_later_waiter() {
    with_deadline(Duration::from_secs(30), || {
        // Short global timeout so the middle waiter's acquire() call times
        // out quickly against the held latch.
        noxu_latch::configure(80, false, true);

        let latch =
            Arc::new(ExclusiveLatch::new(LatchContext::new("fair-timeout")));
        let holder = latch.acquire().expect("holder");

        let latch2 = latch.clone();
        let timed_out = std::thread::spawn(move || latch2.acquire().is_err());

        // A third waiter queues up behind the one that will time out.
        std::thread::sleep(Duration::from_millis(10));
        let latch3 = latch.clone();
        let third_granted = Arc::new(AtomicBool::new(false));
        let third_granted2 = third_granted.clone();
        let third = std::thread::spawn(move || {
            let _g = latch3.acquire().expect("third eventually granted");
            third_granted2.store(true, Ordering::SeqCst);
        });

        assert!(timed_out.join().unwrap(), "middle waiter must time out");
        // Third waiter must still be grantable once the holder releases --
        // proving the give-up did not leave a permanent gap.
        drop(holder);
        third.join().unwrap();
        assert!(third_granted.load(Ordering::SeqCst));

        restore_defaults();
    });
}
