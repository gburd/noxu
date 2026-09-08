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
//! table is to retire this primitive for `parking_lot::RwLock`. They exist so
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
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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
fn a_pending_writer_does_not_block_a_new_reader() {
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

    // THE PROPERTY: with a writer already waiting, a brand-new reader still
    // acquires immediately. A writer-preferring lock would make this fail.
    assert!(
        lock.try_lock_shared(),
        "a new reader was blocked by the pending writer -- NoxuRawRwLock has \
         gained writer preference. That is a behaviour change: update this \
         test, the verdict doc \
         (docs/src/internal/noxu-sync-vs-parking-lot-2026-09.md), and the \
         'Non-fair design' comment in raw_rwlock.rs."
    );
    // SAFETY: `try_lock_shared` returned true, so this thread holds a shared
    // lock; released exactly once here.
    unsafe { lock.unlock_shared() };

    // Release the original reader so the writer can finally proceed.
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
fn hand_over_hand_readers_starve_the_writer_indefinitely() {
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

    // Hand the lock reader-to-reader. `prev_held` is the guard we are holding;
    // we take the next shared lock BEFORE dropping it, so the count never
    // dips to zero and the writer's CAS condition never becomes true.
    for i in 0..HANDOFFS {
        // Acquire the next shared hold while still holding the previous one.
        assert!(
            lock.try_lock_shared(),
            "handoff {i}: a reader could not join an already-read-locked \
             lock while a writer waits -- the non-fair reader path changed"
        );
        // Now release the previous hold. Reader count went 1 -> 2 -> 1,
        // never 0.
        // SAFETY: two shared holds are outstanding at this point (the one
        // from before the loop iteration and the one just acquired); this
        // releases exactly one of them.
        unsafe { lock.unlock_shared() };
    }

    // THE PROPERTY: after 40 handoff windows and the full grace period, the
    // writer still never acquired.
    assert!(
        !writer_acquired.load(Ordering::Acquire),
        "the writer acquired the lock despite an unbroken hand-over-hand \
         reader chain -- NoxuRawRwLock no longer starves writers. That is a \
         FIX (or the primitive was retired). Good news, but update: (1) this \
         test, (2) \
         docs/src/internal/noxu-sync-vs-parking-lot-2026-09.md, (3) the \
         env_fair_latches entry in \
         docs/src/operations/known-limitations.md, and (4) the 'Non-fair \
         design' comment in raw_rwlock.rs."
    );

    // Release the final hold; the writer's timed acquire may now succeed or
    // may already have timed out. Either way it terminates.
    // SAFETY: exactly one shared hold remains outstanding here.
    unsafe { lock.unlock_shared() };
    writer.join().expect("writer panicked");
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
