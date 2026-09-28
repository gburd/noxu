// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Faithful test-parity port of JE `com.sleepycat.je.latch.LatchTest`.
//!
//! Maps each of the 7 JE `@Test` methods to a Noxu `#[test]`, preserving
//! setup / operations / expected outcomes.  Language/API adaptations and
//! documented deviations are noted inline per method:
//!
//! * JE throws `EnvironmentFailureException(UNEXPECTED_STATE)` on reentrant
//!   acquire and on double-release; Noxu panics on reentrant acquire (a
//!   programming error, per `noxu-latch` docs) and uses RAII guards so a
//!   "release again" is structurally impossible — the JE double-release error
//!   path has no Noxu analogue (RAII deviation, noted per method).
//! * JE `LatchStatDefinition` per-latch stat counters (`LATCH_SELF_OWNED`,
//!   `LATCH_CONTENTION`, `LATCH_NO_WAITERS`, `LATCH_RELEASES`,
//!   `LATCH_NOWAIT_SUCCESS/UNSUCCESS`) are NOT tracked by noxu-latch — the
//!   assertions on them are N/A (see the report).  The *behavior* each stat
//!   witnesses (contention, no-wait success/failure, release) is still
//!   exercised here.
//! * JE `getNWaiters()` maps to `ExclusiveLatch::n_waiters()` /
//!   `SharedLatch::n_waiters()` (both surface the underlying
//!   `noxu_sync` futex waiter counter).

use noxu_latch::{ExclusiveLatch, SharedLatch};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

/// JE: LatchTest.testDebugOutput
///
/// JE acquires a latch and calls `LatchSupport.btreeLatchesHeldToString()`
/// (a code-coverage test for the debug/diagnostic string).  Noxu has no
/// global held-latch registry (`LatchSupport` table is N/A — see report), but
/// the per-latch `Debug` string is the direct analogue: acquire, then verify
/// the diagnostic string carries the latch name and locked state.
#[test]
fn test_debug_output() {
    let latch = ExclusiveLatch::named("LatchTest-latch1");
    let _g = latch.acquire().expect("acquireExclusive");
    let s = format!("{:?}", latch);
    assert!(s.contains("LatchTest-latch1"), "debug string carries name");
    assert!(s.contains("locked=true"), "debug string reflects held state");
}

/// JE: LatchTest.testAcquireAndReacquire
///
/// JE: acquire exclusive; re-acquire → UNEXPECTED_STATE (+ LATCH_SELF_OWNED==1);
/// release; release again → UNEXPECTED_STATE.
///
/// Noxu adaptation: reentrant acquire panics (programming error). The
/// LATCH_SELF_OWNED stat assertion is N/A (no stats). The "release again"
/// error path has no analogue: Noxu uses RAII guards, so a second release is
/// structurally impossible; `release_if_owner()` on a not-held latch is a
/// documented no-op (verified here as the closest faithful analogue).
#[test]
fn test_acquire_and_reacquire() {
    // Reentrant acquire must panic.
    let reentrant_panicked = std::panic::catch_unwind(|| {
        let latch = ExclusiveLatch::named("LatchTest-latch1");
        let _g1 = latch.acquire().expect("first acquireExclusive");
        let _g2 = latch.acquire(); // reentrant — must panic
    })
    .is_err();
    assert!(reentrant_panicked, "reentrant acquireExclusive must panic");

    // Acquire then release (RAII), then a second "release" is impossible;
    // release_if_owner on a released latch is a no-op (not held).
    let latch = ExclusiveLatch::named("LatchTest-latch1b");
    {
        let _g = latch.acquire().expect("acquireExclusive");
        assert!(latch.is_owner());
    }
    assert!(!latch.is_locked(), "released after guard drop");
    latch.release_if_owner(); // JE double-release analogue: no-op when not held
    assert!(!latch.is_locked());
}

/// JE: LatchTest.testAcquireAndReacquireShared
///
/// JE: acquire shared; `assert isOwner()`; re-acquire shared →
/// UNEXPECTED_STATE; `assert isOwner()`; release; release again →
/// UNEXPECTED_STATE.
///
/// Noxu adaptation: reentrant shared acquire panics. `is_owner()` maps to
/// JE `SharedLatch.isOwner()` (read-hold-count > 0). The double-release error
/// path is N/A (RAII).
#[test]
fn test_acquire_and_reacquire_shared() {
    // isOwner() is true while the shared guard is held, false after release.
    let latch = SharedLatch::named("LatchTest-latch2", false);
    {
        let _g = latch.acquire_shared().expect("acquireShared");
        assert!(latch.is_owner(), "isOwner() true while shared-held");
    }
    assert!(!latch.is_owner(), "isOwner() false after release");

    // Reentrant shared acquire on the same thread/latch must panic.
    let reentrant_panicked = std::panic::catch_unwind(|| {
        let latch = SharedLatch::named("LatchTest-latch2b", false);
        let _g1 = latch.acquire_shared().expect("first acquireShared");
        let _g2 = latch.acquire_shared(); // reentrant — must panic
    })
    .is_err();
    assert!(reentrant_panicked, "reentrant acquireShared must panic");
}

/// JE: LatchTest.testAcquireReleasePerformance
///
/// JE does 1,000,000 acquire/release pairs and asserts
/// `LATCH_NO_WAITERS == N` and `LATCH_RELEASES == N`.
///
/// Noxu adaptation: the stat assertions are N/A (no per-latch stats). The
/// portable, non-vacuous core is the acquire/release loop itself: every
/// uncontended acquire must succeed and leave the latch free afterwards. We
/// run a smaller count (this is a correctness test, not a benchmark) and
/// assert the invariant on every iteration.
#[test]
fn test_acquire_release_performance() {
    const N: usize = 100_000;
    let latch = ExclusiveLatch::named("LatchTest-latch1");
    for _ in 0..N {
        {
            let _g = latch.acquire().expect("acquireExclusive");
            assert!(latch.is_owner());
        }
        assert!(!latch.is_locked(), "latch free after each release");
    }
}

/// JE: LatchTest.testWait
///
/// JE (repeated 10x): thread1 acquires; spins until `getNWaiters() > 0`;
/// releases. thread2 waits for thread1 to acquire, then blocks on acquire and
/// asserts `LATCH_CONTENTION == ++n` once granted.
///
/// Noxu adaptation: the LATCH_CONTENTION stat assertion is N/A (no stats).
/// The waiter-count spin is ported faithfully via `n_waiters()` (JE
/// `getNWaiters()`), which de-vacuums the test: thread1 does not release until
/// it actually observes thread2 blocked on the latch, proving real contention.
#[test]
fn test_wait() {
    for _ in 0..10 {
        do_test_wait();
    }
}

fn do_test_wait() {
    let latch = Arc::new(ExclusiveLatch::named("testWait-latch"));
    let t1_acquired = Arc::new(AtomicBool::new(false));

    // thread1: acquire, wait until a waiter appears, then release.
    let l1 = latch.clone();
    let a1 = t1_acquired.clone();
    let t1 = std::thread::spawn(move || {
        let g = l1.acquire().expect("t1 acquireExclusive");
        a1.store(true, Ordering::SeqCst);
        // Spin until tester2 is blocked on the latch (JE getNWaiters()>0).
        let deadline = Instant::now() + Duration::from_secs(5);
        while l1.n_waiters() == 0 {
            assert!(Instant::now() < deadline, "no waiter appeared");
            std::thread::yield_now();
        }
        drop(g); // release
    });

    // thread2: wait for t1 to acquire, then block on acquire (creates the
    // waiter t1 spins for), then release.
    let l2 = latch.clone();
    let a2 = t1_acquired;
    let t2 = std::thread::spawn(move || {
        while !a2.load(Ordering::SeqCst) {
            std::thread::yield_now();
        }
        let _g = l2.acquire().expect("t2 acquireExclusive (contended)");
        // Granted only after t1 released — real contention occurred.
    });

    t1.join().unwrap();
    t2.join().unwrap();
    assert!(!latch.is_locked(), "latch free after both done");
}

/// JE: LatchTest.testAcquireNoWait
///
/// JE: thread1 acquires and holds. thread2: no-wait acquire → false
/// (+ LATCH_NOWAIT_UNSUCCESS==1); signals; thread1 releases; thread2 no-wait
/// acquire → true (+ LATCH_NOWAIT_SUCCESS==1); no-wait acquire again while
/// held → UNEXPECTED_STATE; release.
///
/// Noxu adaptation: NOWAIT stat assertions are N/A (no stats). `acquireExclusiveNoWait()`
/// maps to `try_acquire()` (Option). The reentrant no-wait error maps to a panic.
#[test]
fn test_acquire_no_wait() {
    let latch = Arc::new(ExclusiveLatch::named("testWait-latch"));
    let t1_acquired = Arc::new(AtomicBool::new(false));
    let t2_try_acquire = Arc::new(AtomicBool::new(false));
    let t1_released = Arc::new(AtomicBool::new(false));

    let l1 = latch.clone();
    let a1 = t1_acquired.clone();
    let t2try = t2_try_acquire.clone();
    let rel1 = t1_released.clone();
    let t1 = std::thread::spawn(move || {
        let g = l1.acquire().expect("t1 acquireExclusive");
        a1.store(true, Ordering::SeqCst);
        // Wait for tester2 to attempt the no-wait acquire.
        while !t2try.load(Ordering::SeqCst) {
            std::thread::yield_now();
        }
        drop(g); // release
        rel1.store(true, Ordering::SeqCst);
    });

    let l2 = latch.clone();
    let a2 = t1_acquired;
    let t2try2 = t2_try_acquire;
    let rel2 = t1_released;
    let t2 = std::thread::spawn(move || {
        // Wait for tester1 to acquire.
        while !a2.load(Ordering::SeqCst) {
            std::thread::yield_now();
        }
        // No-wait acquire should FAIL — tester1 holds it.
        assert!(
            l2.try_acquire().is_none(),
            "acquireExclusiveNoWait must fail while held by another thread"
        );
        t2try2.store(true, Ordering::SeqCst);

        // Wait for tester1 to release.
        while !rel2.load(Ordering::SeqCst) {
            std::thread::yield_now();
        }
        // Retry until success (t1's release + counter store may not yet be
        // visible as a free lock at the instant rel2 flips; a bounded spin
        // keeps the port deterministic without a fixed sleep).
        let deadline = Instant::now() + Duration::from_secs(5);
        let g = loop {
            if let Some(g) = l2.try_acquire() {
                break g;
            }
            assert!(
                Instant::now() < deadline,
                "no-wait acquire never succeeded"
            );
            std::thread::yield_now();
        };

        // No-wait acquire AGAIN while we already hold it → must panic
        // (JE UNEXPECTED_STATE; Noxu reentrancy panic).
        let reentrant_panicked =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _ = l2.try_acquire();
            }))
            .is_err();
        assert!(
            reentrant_panicked,
            "reentrant acquireExclusiveNoWait must panic"
        );

        drop(g); // release
    });

    t1.join().unwrap();
    t2.join().unwrap();
    assert!(!latch.is_locked());
}

/// JE: LatchTest.testMultipleWaiters
///
/// JE: thread1 acquires, then spins until `getNWaiters() >= N_WAITERS`. Each
/// of N waiter threads waits its turn (`getNWaiters() < waiterNumber`), then
/// acquires and asserts `getNWaiters() == N - waiterNumber - 1`, then releases.
///
/// Noxu adaptation: `n_waiters()` maps to JE `getNWaiters()`. We port the core
/// invariant: the holder observes all N threads queue up on the latch before
/// releasing, and after release every waiter is eventually granted exactly
/// once. (JE's exact per-waiter decrement assertion depends on JE's FIFO
/// `ReentrantLock(fair)` grant order and each waiter throttling its own entry
/// by `getNWaiters()`; noxu-sync's mutex is not FIFO-fair by default, so we
/// assert the count reaches N and all are granted, not a specific grant order.
/// See report for this ordering deviation.)
#[test]
fn test_multiple_waiters() {
    const N: usize = 5;
    let latch = Arc::new(ExclusiveLatch::named("testWait-latch"));
    let t1_acquired = Arc::new(AtomicBool::new(false));
    let granted = Arc::new(AtomicUsize::new(0));

    let l1 = latch.clone();
    let a1 = t1_acquired.clone();
    let t1 = std::thread::spawn(move || {
        let g = l1.acquire().expect("main acquireExclusive");
        a1.store(true, Ordering::SeqCst);
        // Wait until all N waiters are blocked on the latch.
        let deadline = Instant::now() + Duration::from_secs(10);
        while l1.n_waiters() < N {
            assert!(
                Instant::now() < deadline,
                "only {} of {} waiters queued",
                l1.n_waiters(),
                N
            );
            std::thread::yield_now();
        }
        drop(g); // release — waiters unblock one at a time
    });

    let mut waiters = Vec::new();
    for _ in 0..N {
        let lw = latch.clone();
        let aw = t1_acquired.clone();
        let gr = granted.clone();
        waiters.push(std::thread::spawn(move || {
            while !aw.load(Ordering::SeqCst) {
                std::thread::yield_now();
            }
            let _g = lw.acquire().expect("waiter acquireExclusive");
            gr.fetch_add(1, Ordering::SeqCst);
            // Hold briefly so the count is observable, then release (RAII).
        }));
    }

    t1.join().unwrap();
    for w in waiters {
        w.join().unwrap();
    }
    assert_eq!(
        granted.load(Ordering::SeqCst),
        N,
        "all {} waiters must be granted exactly once",
        N
    );
    assert!(!latch.is_locked(), "latch free after all waiters done");
}
