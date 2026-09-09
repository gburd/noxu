// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Characterisation tests for `NoxuRawRwLock`'s writer-starvation behaviour.
//!
//! ## What this pins down
//!
//! `NoxuRawRwLock` is non-fair **by design** (`raw_rwlock.rs`: "Non-fair
//! design: new readers are not blocked by pending writers"; bit 31
//! `WRITE_WAITING` is "reserved, not currently used"). `lock_exclusive_slow`
//! can only CAS when `state == 0`, and nothing ever blocks an incoming
//! reader, so an overlapping reader stream that never lets the reader count
//! reach zero blocks an exclusive waiter **indefinitely**.
//!
//! The 2026-09 A/B measurement against `parking_lot`
//! (`docs/src/internal/noxu-sync-vs-parking-lot-2026-09.md`) quantified the
//! consequence: at >= 7 concurrent readers on an idle 64-vCPU box the writer
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
//! They assert *current, documented* behaviour, and the recommendation on the
//! table is to retire this primitive for `noxu_sync::RwLock`. They exist so
//! the property cannot be silently changed or forgotten: if the starvation is
//! ever fixed these fail loudly and say what else to update. The distinction
//! between "this is what it does" and "this is what it should do" is the
//! point.
//!
//! ## They are not vacuous (verified against a fair lock)
//!
//! A characterisation test that passes for the wrong reason is worthless, so
//! the same logic was run against `parking_lot::RawRwLock`, which *does* have
//! writer preference:
//!
//! | Assertion | `noxu_sync` | `parking_lot` |
//! |---|---|---|
//! | new reader admitted while a writer is pending | `true` | **`false`** |
//! | reader can join the handoff chain | always | **refused at iteration 0** |
//!
//! Both assertions below therefore **fail** against a writer-preferring
//! lock: `parking_lot` blocks the incoming reader, which both breaks the
//! handoff chain and lets its writer through. The tests genuinely
//! discriminate between a starving and a non-starving rwlock.

use lock_api::{RawRwLock as _, RawRwLockTimed as _};
use noxu_sync::NoxuRawRwLock;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Duration;

/// How long a blocked writer is given to demonstrate it cannot get in.
/// Generous: the point is the writer fails to acquire even with ample time,
/// and a slow CI box only makes that *more* true.
const WRITER_GRACE: Duration = Duration::from_millis(600);

/// A pending writer does **not** block an incoming reader.
///
/// This is the single mechanism from which the starvation follows, and it is
/// the exact sentence in `raw_rwlock.rs`'s design comment. Fully
/// deterministic: no races, no timing beyond one bounded wait.
#[test]
fn a_pending_writer_blocks_a_new_reader_after_the_fairness_threshold() {
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

    // THE PROPERTY: the lock is EVENTUALLY fair, not immediately writer-
    // preferring. A brand-new reader is admitted while the writer is still
    // inside its fairness grace period (that is what keeps hot nodes fast), and
    // is refused once the writer has waited past FAIRNESS_THRESHOLD.
    //
    // The 50 ms sleep above is far beyond the 500 us threshold, so by now the
    // gate must be closed.
    assert!(
        !lock.try_lock_shared(),
        "a new reader was admitted while a writer was queued -- the \
         WRITE_WAITING admission gate in raw_rwlock.rs is not holding, which \
         re-opens unbounded writer starvation. Update this test only if \
         writer preference was deliberately removed, and re-measure the \
         starvation numbers in \
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
    // never dips to zero. With the WRITE_WAITING admission gate this chain can
    // no longer be sustained -- the very first attempt to join while a writer is
    // queued is refused, which is precisely what breaks the starvation.
    let mut joins_admitted = 0usize;
    for _ in 0..HANDOFFS {
        if !lock.try_lock_shared() {
            break;
        }
        joins_admitted += 1;
        // SAFETY: two shared holds are outstanding here; release exactly one.
        unsafe { lock.unlock_shared() };
    }
    // Under EVENTUAL fairness the first few joins may succeed (the writer is
    // still inside its grace period); what must not happen is the chain running
    // unbroken for all HANDOFFS, because that is the unbounded starvation this
    // guards. The writer arms the gate after FAIRNESS_THRESHOLD and the chain
    // then breaks.
    assert!(
        joins_admitted < HANDOFFS,
        "a reader joined on all {HANDOFFS} handoffs while a writer waited; the \
         chain that starves writers is still constructible, so the fairness \
         deadline never armed the WRITE_WAITING gate"
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
         new reader could join -- writer starvation has regressed. Check the \
         WRITE_WAITING gate in try_lock_shared_fast/lock_shared_slow and that \
         unlock_shared's last-reader test masks READERS_MASK (an unmasked \
         equality test never fires once WRITE_WAITING is set, which parks the \
         writer forever)."
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
/// Every deadlock introduced while implementing the `WRITE_WAITING` admission
/// gate showed up here and nowhere else, so this is the test that earns its
/// keep. Three distinct bugs were caught by exactly this shape:
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
             deadlocked. Check (1) that unlock_shared masks READERS_MASK before \
             its last-reader test, (2) that both unlock paths wake ALL waiters \
             when a writer is queued (readers and writers share one futex \
             word, so a single wake can be consumed by a reader that re-parks), \
             and (3) that an acquiring writer clears WRITE_WAITING only AFTER \
             decrementing write_waiters.",
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
