// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Characterisation tests for `NoxuRawRwLock`'s writer-starvation avoidance.
//!
//! ## What this pins down
//!
//! `NoxuRawRwLock` avoids writer starvation via **reservation**
//! (`raw_rwlock.rs`'s module doc comment): a writer that fails to find a
//! free window after `WRITE_SPIN_ATTEMPTS` reserves `WRITE_LOCKED`
//! unconditionally, gated only on no OTHER writer already holding it --
//! never on the reader count. From that instant no new reader can be
//! admitted (`try_lock_shared_fast`/`lock_shared_slow` refuse whenever
//! `WRITE_LOCKED` is set), so an overlapping reader stream cannot hold the
//! writer off indefinitely: reservation, not a time-based gate, is what
//! bounds the wait.
//!
//! This document previously described an *advisory* `WRITE_WAITING` bit
//! armed only after a `FAIRNESS_THRESHOLD` had elapsed, re-racing the plain
//! acquire CAS once the reader count reached zero. That design shipped,
//! measured, and was superseded by reservation
//! (`docs/src/internal/rwlock-state-table-2026-09.md`) specifically because
//! the re-race paid a futex round-trip per drain step, producing a p50 write
//! latency of ~1 ms at 63 readers against `parking_lot`'s 0 us. These tests
//! were written against the older mechanism; they still pass and still pin
//! down the same property (a queued writer is never starved by an
//! overlapping reader stream) because reservation is a STRICTLY STRONGER
//! guarantee -- it excludes new readers the instant the spin fails, rather
//! than after a threshold -- so anything the old gate could guarantee,
//! reservation guarantees strictly sooner.
//!
//! The 2026-09 A/B measurement against `parking_lot`
//! (`docs/src/internal/noxu-sync-vs-parking-lot-2026-09.md`) quantified the
//! ORIGINAL (pre-gate) starvation this design avoids: at >= 7 concurrent
//! readers on an idle 64-vCPU box a writer with no fairness mechanism at all
//! received essentially zero service for a full 3 s window, where
//! `parking_lot::RawRwLock` served ~1 million writes with a sub-millisecond
//! worst case.
//!
//! ## Why these tests are deterministic, not timing-based
//!
//! A load-based reproduction is **not portable**. Emergent starvation needs
//! readers to genuinely overlap in time, which needs enough free cores: on a
//! loaded 8-core machine the readers deschedule often enough to leave
//! `state == 0` windows, and the writer gets served (measured: only a 19x
//! reader:writer ratio locally, versus ~1e6x on the idle 64-vCPU box). A test
//! asserting the 64-vCPU ratio would fail on small CI boxes for reasons that
//! have nothing to do with the property.
//!
//! So instead of racing threads and hoping, these tests construct the
//! starving condition **deterministically** with explicit handoffs: a
//! hand-over-hand chain of readers where the next reader provably acquires
//! before the previous one releases. That holds the reader count >= 1 at
//! every instant by construction, on any machine, with no timing assumption
//! beyond a generous "the writer had a chance to try" barrier.
//!
//! ## These are characterisation tests, not correctness tests
//!
//! They assert *current, documented* behaviour: that reservation bounds
//! writer starvation. They exist so the property cannot be silently changed
//! or forgotten -- if it ever regresses, these fail loudly and say what else
//! to check. The distinction between "this is what it does" and "this is
//! what it should do" is the point.
//!
//! ## They are not vacuous (verified against a fair lock)
//!
//! A characterisation test that passes for the wrong reason is worthless, so
//! the same logic was run against `parking_lot::RawRwLock`, which *does* have
//! writer preference:
//!
//! | Assertion | `noxu_sync` | `parking_lot` |
//! |---|---|---|
//! | new reader admitted while a writer is pending | eventually `false` | **`false`** |
//! | reader can join the handoff chain | only before reservation | **refused at iteration 0** |
//!
//! Both assertions below still discriminate a starving lock from a
//! non-starving one -- the difference from `parking_lot` is WHEN admission
//! is refused (immediately vs. after a bounded spin), not WHETHER it is.

use lock_api::{RawRwLock as _, RawRwLockTimed as _};
use noxu_sync::NoxuRawRwLock;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Duration;

/// How long a blocked writer is given to demonstrate it cannot get in.
/// Generous: the point is the writer fails to acquire even with ample time,
/// and a slow CI box only makes that *more* true.
const WRITER_GRACE: Duration = Duration::from_millis(600);

/// A pending writer eventually blocks a new reader once it reserves.
///
/// This is the mechanism that bounds starvation: reservation excludes new
/// readers unconditionally, the instant the writer's initial spin fails to
/// find a free window. Fully deterministic: no races, no timing beyond one
/// bounded wait past the reservation point.
#[test]
fn a_pending_writer_eventually_blocks_a_new_reader() {
    let lock = Arc::new(NoxuRawRwLock::INIT);

    // Reader 1 holds the lock.
    lock.lock_shared();

    // A writer arrives and blocks (it cannot proceed: reader count is 1).
    let writer_blocked = Arc::new(AtomicBool::new(false));
    let writer_acquired = Arc::new(AtomicBool::new(false));
    let writer = {
        let lock = Arc::clone(&lock);
        let blocked = Arc::clone(&writer_blocked);
        let acquired = Arc::clone(&writer_acquired);
        std::thread::spawn(move || {
            blocked.store(true, Ordering::Release);
            // Bounded so a regression reports instead of hanging the suite.
            if lock.try_lock_exclusive_for(WRITER_GRACE) {
                acquired.store(true, Ordering::Release);
                // SAFETY: the timed acquire returned true, so this thread
                // holds the exclusive lock and releases it exactly once.
                unsafe { lock.unlock_exclusive() };
            }
        })
    };

    // Let the writer reach its blocking acquire.
    while !writer_blocked.load(Ordering::Acquire) {
        std::hint::spin_loop();
    }
    std::thread::sleep(Duration::from_millis(50));

    // THE PROPERTY: the writer eventually reserves and refuses new readers.
    // A brand-new reader MAY be admitted while the writer is still spinning
    // for a free window (that is what keeps hot nodes fast), but is refused
    // once the writer has reserved -- and 50 ms is far beyond
    // WRITE_SPIN_ATTEMPTS's actual duration (hundreds of CPU cycles), so by
    // now the writer must have reserved.
    assert!(
        !lock.try_lock_shared(),
        "a new reader was admitted while a writer was queued -- reservation \
         in raw_rwlock.rs is not holding, which re-opens unbounded writer \
         starvation. Update this test only if writer preference was \
         deliberately removed, and re-measure the starvation numbers in \
         docs/src/internal/noxu-sync-vs-parking-lot-2026-09.md."
    );

    // Release the original reader so the writer can proceed.
    // SAFETY: this thread holds the shared lock taken at the top of the test.
    unsafe { lock.unlock_shared() };
    writer.join().expect("writer panicked");
}

/// A hand-over-hand reader chain starves the writer indefinitely.
///
/// Each reader acquires **before** its predecessor releases, so the reader
/// count is >= 1 at every instant *by construction* -- exactly the condition
/// `lock_exclusive_slow`'s `state == 0` CAS can never observe. Deterministic
/// on any core count: the overlap is enforced by handoff, not by scheduling
/// luck.
#[test]
fn a_hand_over_hand_reader_chain_cannot_starve_the_writer() {
    /// Handoffs in the chain. Each one is a window in which a fair (or
    /// writer-preferring) lock would let the waiting writer through.
    const HANDOFFS: usize = 40;

    let lock = Arc::new(NoxuRawRwLock::INIT);

    // First reader takes the lock before the writer ever appears.
    lock.lock_shared();

    let writer_acquired = Arc::new(AtomicBool::new(false));
    let writer_started = Arc::new(AtomicBool::new(false));
    let writer = {
        let lock = Arc::clone(&lock);
        let acquired = Arc::clone(&writer_acquired);
        let started = Arc::clone(&writer_started);
        std::thread::spawn(move || {
            started.store(true, Ordering::Release);
            if lock.try_lock_exclusive_for(WRITER_GRACE) {
                acquired.store(true, Ordering::Release);
                // SAFETY: timed acquire succeeded; released exactly once.
                unsafe { lock.unlock_exclusive() };
            }
        })
    };
    while !writer_started.load(Ordering::Acquire) {
        std::hint::spin_loop();
    }
    std::thread::sleep(Duration::from_millis(30));

    // Attempt the hand-over-hand chain that USED to starve the writer: take the
    // next shared hold before releasing the previous one, so the reader count
    // never dips to zero. Once the writer has RESERVED, this chain can no
    // longer be sustained -- the very first attempt to join after that point
    // is refused, which is precisely what breaks the starvation.
    let mut joins_admitted = 0usize;
    for _ in 0..HANDOFFS {
        if !lock.try_lock_shared() {
            break;
        }
        joins_admitted += 1;
        // SAFETY: two shared holds are outstanding here; release exactly one.
        unsafe { lock.unlock_shared() };
    }
    // The first few joins may succeed (the writer is still spinning for a
    // free window); what must not happen is the chain running unbroken for
    // all HANDOFFS, because that is the unbounded starvation this guards.
    // The writer reserves once its spin fails and the chain then breaks --
    // and the reserve-CAS is guarded ONLY on WRITE_LOCKED, never on the
    // reader count, so the relay itself cannot prevent it.
    assert!(
        joins_admitted < HANDOFFS,
        "a reader joined on all {HANDOFFS} handoffs while a writer waited; the \
         chain that starves writers is still constructible, so the writer's \
         reservation never took effect"
    );

    // Release the reader that was held before the writer queued; the writer
    // must then acquire, because no new reader can slip in ahead of it.
    // SAFETY: this thread still holds the shared lock taken at the top.
    unsafe { lock.unlock_shared() };
    writer.join().expect("writer panicked");

    // THE PROPERTY: the writer acquired within its grace period rather than
    // being starved by the reader chain.
    assert!(
        writer_acquired.load(Ordering::Acquire),
        "the writer FAILED to acquire within its grace period even though no \
         new reader could join -- writer starvation has regressed. Check \
         that the reserve-CAS in lock_exclusive_slow is guarded only on \
         WRITE_LOCKED (never the reader count), and that unlock_shared wakes \
         the reserving writer via notify_drain once the reader count reaches \
         zero."
    );
}

/// Control: a **single** reader that fully releases between acquisitions does
/// *not* starve the writer, because each release leaves `state == 0`.
///
/// This isolates the cause. Starvation is a property of *overlapping* readers
/// holding the count above zero, not of "readers exist" -- so a fix must
/// target the overlap, i.e. blocking new readers when a writer waits.
#[test]
fn a_single_non_overlapping_reader_does_not_starve_the_writer() {
    let lock = Arc::new(NoxuRawRwLock::INIT);
    let stop = Arc::new(AtomicBool::new(false));
    let gate = Arc::new(Barrier::new(2));
    let writer_acqs = Arc::new(AtomicU64::new(0));

    let reader = {
        let lock = Arc::clone(&lock);
        let stop = Arc::clone(&stop);
        let gate = Arc::clone(&gate);
        std::thread::spawn(move || {
            gate.wait();
            while !stop.load(Ordering::Relaxed) {
                lock.lock_shared();
                // SAFETY: shared lock acquired immediately above, released
                // exactly once here -- no overlap with the next iteration.
                unsafe { lock.unlock_shared() };
            }
        })
    };

    let writer = {
        let lock = Arc::clone(&lock);
        let stop = Arc::clone(&stop);
        let gate = Arc::clone(&gate);
        let acqs = Arc::clone(&writer_acqs);
        std::thread::spawn(move || {
            gate.wait();
            while !stop.load(Ordering::Relaxed) {
                if lock.try_lock_exclusive_for(Duration::from_millis(50)) {
                    // SAFETY: timed acquire succeeded; released exactly once.
                    unsafe { lock.unlock_exclusive() };
                    acqs.fetch_add(1, Ordering::Relaxed);
                }
            }
        })
    };

    std::thread::sleep(Duration::from_millis(300));
    stop.store(true, Ordering::Relaxed);
    reader.join().expect("reader panicked");
    writer.join().expect("writer panicked");

    assert!(
        writer_acqs.load(Ordering::Relaxed) > 0,
        "a single reader that fully releases must leave state==0 windows the \
         writer can claim, but the writer never acquired"
    );
}

/// Writer preference must not deadlock a mixed reader/writer population.
///
/// Every deadlock introduced while implementing the earlier eventual-
/// fairness `WRITE_WAITING` admission gate showed up here and nowhere else,
/// so this is the test that earns its keep. Three distinct bugs were caught
/// by exactly this shape in that design:
///
/// 1. `unlock_shared`'s last-reader test compared the whole state word against
///    `ONE_READER`; once `WRITE_WAITING` was set that equality never held, so
///    the queued writer was never woken.
/// 2. `futex_wake(.., 1)` could hand the single wakeup to a reader, which
///    re-parked on seeing `WRITE_WAITING` and *consumed* it, stranding the
///    writer with nobody holding the lock.
/// 3. The acquiring writer reconciled `WRITE_WAITING` from a count read BEFORE
///    its CAS, so a concurrent writer leaving in that window could leave the
///    gate set with no writer behind it -- locking every reader out forever
///    (observed as 32 readers parked, 0 writers).
///
/// `WRITE_WAITING`/`FAIRNESS_THRESHOLD` were later deleted entirely in favour
/// of reservation (`docs/src/internal/rwlock-state-table-2026-09.md`), which
/// introduced its OWN four candidate failure modes -- a blind reservation
/// CAS, `is_locked_exclusive` conflating reserved with held, a reserving
/// writer re-running the acquire CAS, and `unlock_exclusive` waking only one
/// population via `else if` -- all closed by construction in the current
/// `raw_rwlock.rs` (see its module doc comment), but this watchdog is the
/// backstop if any of them, or a fifth not yet catalogued, regresses.
///
/// A regression in any of those hangs this test rather than failing it, so it
/// carries its own watchdog: the worker threads must report completion within
/// the deadline or the assertion fires.
#[test]
fn mixed_readers_and_writers_make_progress_without_deadlock() {
    const THREADS: usize = 16;
    const RUN: Duration = Duration::from_secs(2);
    /// Generous: the point is liveness, not throughput.
    const DEADLINE: Duration = Duration::from_secs(30);

    let lock = Arc::new(NoxuRawRwLock::INIT);
    let stop = Arc::new(AtomicBool::new(false));
    let ops = Arc::new(AtomicU64::new(0));
    let finished = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::new();
    for t in 0..THREADS {
        let lock = Arc::clone(&lock);
        let stop = Arc::clone(&stop);
        let ops = Arc::clone(&ops);
        let finished = Arc::clone(&finished);
        handles.push(std::thread::spawn(move || {
            let mut i = t;
            while !stop.load(Ordering::Relaxed) {
                // Mostly readers with a steady trickle of writers: the mix that
                // keeps the admission gate churning.
                if i % 32 == 0 {
                    lock.lock_exclusive();
                    // SAFETY: acquired exclusively above; released once here.
                    unsafe { lock.unlock_exclusive() };
                } else {
                    lock.lock_shared();
                    // SAFETY: acquired shared above; released once here.
                    unsafe { lock.unlock_shared() };
                }
                i = i.wrapping_add(1);
                ops.fetch_add(1, Ordering::Relaxed);
            }
            finished.fetch_add(1, Ordering::Relaxed);
        }));
    }

    std::thread::sleep(RUN);
    stop.store(true, Ordering::Relaxed);

    // Watchdog: poll for completion instead of joining, so a deadlock produces
    // a readable assertion rather than an indefinitely hung test binary.
    let start = std::time::Instant::now();
    while finished.load(Ordering::Relaxed) < THREADS {
        assert!(
            start.elapsed() < DEADLINE,
            "only {}/{THREADS} threads finished after {:?} -- the rwlock \
             deadlocked. Check (1) that a reservation CAS is guarded on \
             WRITE_LOCKED being clear, never on the reader count, and never \
             re-runs the acquire CAS once readers drain to zero (state table \
             failure modes 1 and 3), (2) that both unlock_exclusive and the \
             give-up-on-timeout path wake BOTH populations unconditionally, \
             not via `else if` (failure mode 4), and (3) that the reserving \
             writer's own drain_futex wakeup is targeted correctly and not \
             swallowed by a parked reader (see the state table's 'a third \
             futex word is required' section).",
            finished.load(Ordering::Relaxed),
            start.elapsed()
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    for h in handles {
        h.join().expect("worker panicked");
    }

    assert!(ops.load(Ordering::Relaxed) > 0, "no operations completed at all");
}
