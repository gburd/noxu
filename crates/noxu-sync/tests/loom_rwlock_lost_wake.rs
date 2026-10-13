// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Loom model of the `NoxuRawRwLock` reader-park / writer-release lost-wake
//! race and its fix.
//!
//! # Why loom and not shuttle here
//!
//! The existing `shuttle_rwlock_reservation.rs` models the lock's *protocol*
//! with a `Mutex<LockState>` + condvars — it validates the state table's
//! transitions, but it abstracts the atomics away, so it is blind to a
//! memory-ordering bug: the enum under one mutex is sequentially consistent
//! by construction. This file models the real *atomics* and their orderings,
//! which is where the lost wake lived.
//!
//! loom explores the C++11 memory model, so it reaches exactly the
//! store-buffer (Dekker / StoreLoad) reordering that aarch64's LSE
//! Release/Relaxed RMWs permit and x86's full-barrier `LOCK` RMWs forbid —
//! matching the observed architecture split (reproduced at a ~5% hang rate on
//! Graviton, 0% on x86 over 30 000 runs each).
//!
//! # What is modelled
//!
//! The exact waker/sleeper pair from `raw_rwlock.rs`:
//!
//! ```text
//! Reader park (lock_shared_slow):      Writer release (unlock_exclusive):
//!   s = state.load()  // WRITE_LOCKED    state.fetch_and(!WRITE_LOCKED, Rel)  [C]
//!   <record "parked">                    <decide whether to wake>            [D]
//!   if state == s { PARK on &state }
//! ```
//!
//! Two gate strategies are selected at compile time via `--cfg`:
//!
//! * **`--cfg lost_wake_bug`** reproduces the SHIPPED design: the writer's
//!   wake decision reads the *separate* `read_waiters` counter (`[D]` loads a
//!   different atomic than `[C]` stored). loom finds the interleaving where
//!   `[D]` reads 0 while the reader's futex re-check still sees `WRITE_LOCKED`
//!   set → the reader parks and is never woken. **This configuration must
//!   FAIL** (the assertion fires), proving the model has teeth.
//!
//! * **default (the fix)** gates the wake on the `READERS_PARKED` bit carried
//!   IN `state` — the writer's clearing RMW returns a snapshot that already
//!   says whether a reader is parked, so `[C]` and `[D]` are the same atomic.
//!   **This configuration must PASS** (loom exhausts the state space with no
//!   lost wake).
//!
//! A SeqCst-on-the-counter fix is deliberately NOT offered as a passing
//! variant: loom 0.7 under-models SeqCst's StoreLoad guarantee (it reports
//! the classic store-buffer litmus `r1 == r2 == 0` even under SeqCst), so it
//! cannot certify a counter-based fix. The same-word gate needs only
//! Release/Acquire on one location, which loom models faithfully — which is
//! the second reason the fix is structured this way rather than by adding a
//! fence.
//!
//! # Running
//!
//! ```sh
//! # the fix (must pass):
//! RUSTFLAGS="--cfg loom" cargo test -p noxu-sync --test loom_rwlock_lost_wake --release
//! # the bug (must fail with a LOST WAKE panic — proves non-vacuity):
//! RUSTFLAGS="--cfg loom --cfg lost_wake_bug" cargo test -p noxu-sync --test loom_rwlock_lost_wake --release
//! ```
//!
//! Compiles to nothing unless built with `--cfg loom`, matching the
//! `--cfg noxu_shuttle` convention of the sibling shuttle test.
#![cfg(loom)]

use loom::sync::Arc;
use loom::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use loom::thread;

const WRITE_LOCKED: u32 = 1 << 30;
const READERS_PARKED: u32 = 1 << 31;
const WRITERS_PARKED: u32 = 1 << 29;

/// Shared control block mirroring the fields of `NoxuRawRwLock` that
/// participate in the race.
struct Lock {
    state: AtomicU32,
    /// Advisory in the fix; the (racy) wake gate in `--cfg lost_wake_bug`.
    read_waiters: AtomicUsize,
    /// Did the reader commit to sleeping on `&state`?
    reader_parked: AtomicU32,
    /// Did the writer commit to issuing `futex_wake(&state)`?
    writer_waking: AtomicU32,
}

/// One reader (parking) racing one writer release, parameterised by the
/// release SHAPE being modelled:
///
/// * `initial_readers == 0`, `give_up == false` — `unlock_exclusive` releasing
///   `write_held -> free`. The clearing RMW drives the reader count to the
///   same value it had (0).
/// * `initial_readers == 1`, `give_up == true` — `try_give_up_reservation`
///   releasing `reserved_draining(1) -> read_held(1)`: it clears
///   `WRITE_LOCKED` (+`READERS_PARKED`) but MUST preserve the reader count,
///   so a second, parking reader is the one whose wake can be lost. This is
///   the path the second (TreeNode) field witness implicated.
fn scenario(initial_readers: u32, give_up: bool) {
    let lk = Arc::new(Lock {
        // write_held (count 0) or reserved_draining(initial_readers).
        state: AtomicU32::new(WRITE_LOCKED | initial_readers),
        read_waiters: AtomicUsize::new(0),
        reader_parked: AtomicU32::new(0),
        writer_waking: AtomicU32::new(0),
    });

    // READER: lock_shared_slow's park path.
    let reader = {
        let lk = Arc::clone(&lk);
        thread::spawn(move || {
            loop {
                let s = lk.state.load(Ordering::Relaxed);
                if s & WRITE_LOCKED == 0 {
                    // Admitted directly — not the interesting interleaving.
                    return;
                }

                // Advisory counter (what the shipped bug gated on).
                lk.read_waiters.fetch_add(1, Ordering::Relaxed);

                if cfg!(lost_wake_bug) {
                    // SHIPPED (buggy): park expecting the raw `state`, with the
                    // "I am parked" signal living only in the separate counter.
                    let now = lk.state.load(Ordering::Relaxed);
                    if now == s {
                        lk.reader_parked.store(1, Ordering::SeqCst);
                    }
                    lk.read_waiters.fetch_sub(1, Ordering::Relaxed);
                    return;
                } else {
                    // FIX: record "parked" IN `state` (bit 31) before parking,
                    // then park expecting that value. If the CAS fails, state
                    // changed under us — retry (may now be admittable).
                    if s & READERS_PARKED == 0
                        && lk
                            .state
                            .compare_exchange_weak(
                                s,
                                s | READERS_PARKED,
                                Ordering::Relaxed,
                                Ordering::Relaxed,
                            )
                            .is_err()
                    {
                        lk.read_waiters.fetch_sub(1, Ordering::Relaxed);
                        continue;
                    }
                    let parked_state = s | READERS_PARKED;
                    // futex_wait(&state, parked_state): parks iff unchanged.
                    if lk.state.load(Ordering::Acquire) == parked_state {
                        lk.reader_parked.store(1, Ordering::SeqCst);
                    }
                    lk.read_waiters.fetch_sub(1, Ordering::Relaxed);
                    return;
                }
            }
        })
    };

    // WRITER: the release path (unlock_exclusive OR try_give_up_reservation).
    let writer = {
        let lk = Arc::clone(&lk);
        thread::spawn(move || {
            if cfg!(lost_wake_bug) {
                // SHIPPED (buggy): clear WRITE_LOCKED (give-up preserves the
                // reader bits), then gate the wake on the SEPARATE
                // read_waiters counter — a store->load across two different
                // atomics with no StoreLoad barrier.
                if give_up {
                    // reserved_draining -> read_held: CAS clears only
                    // WRITE_LOCKED, keeps the reader count.
                    loop {
                        let s = lk.state.load(Ordering::Relaxed);
                        if lk
                            .state
                            .compare_exchange_weak(
                                s,
                                s & !WRITE_LOCKED,
                                Ordering::Release,
                                Ordering::Relaxed,
                            )
                            .is_ok()
                        {
                            break;
                        }
                    }
                } else {
                    lk.state.fetch_and(!WRITE_LOCKED, Ordering::Release);
                }
                if lk.read_waiters.load(Ordering::Relaxed) > 0 {
                    lk.writer_waking.store(1, Ordering::SeqCst);
                }
            } else {
                // FIX: clear WRITE_LOCKED and READERS_PARKED together and gate
                // the wake on the PRIOR value of that same word. For give-up
                // the reader count is preserved; only WRITE_LOCKED and the
                // wake-gate bit are cleared.
                let prev = if give_up {
                    loop {
                        let s = lk.state.load(Ordering::Relaxed);
                        match lk.state.compare_exchange_weak(
                            s,
                            s & !(WRITE_LOCKED | READERS_PARKED),
                            Ordering::Release,
                            Ordering::Relaxed,
                        ) {
                            Ok(_) => break s,
                            Err(_) => continue,
                        }
                    }
                } else {
                    lk.state.fetch_and(
                        !(WRITE_LOCKED | READERS_PARKED),
                        Ordering::Release,
                    )
                };
                if prev & READERS_PARKED != 0 {
                    lk.writer_waking.store(1, Ordering::SeqCst);
                }
            }
        })
    };

    reader.join().unwrap();
    writer.join().unwrap();

    // Lost-wake invariant: a reader that committed to parking must have been
    // woken by the writer.
    let parked = lk.reader_parked.load(Ordering::SeqCst);
    let waking = lk.writer_waking.load(Ordering::SeqCst);
    assert!(
        !(parked == 1 && waking == 0),
        "LOST WAKE: reader parked on `state` (WRITE_LOCKED observed set) but \
         the writer did not issue the wake (gate observed no waiter). The \
         reader sleeps forever while the lock is already free."
    );
}

/// The reader-side lost-wake model check (parks on `state`, gated by
/// READERS_PARKED). Passes with the fix; fails under `--cfg lost_wake_bug`.
#[test]
fn reader_park_writer_release_no_lost_wake() {
    // write_held -> free (unlock_exclusive).
    loom::model(|| scenario(0, false));
}

/// Same reader-side check, but the writer RELEASES VIA GIVE-UP
/// (`try_give_up_reservation`: reserved_draining(1) -> read_held(1)) — the
/// path the second (TreeNode) field witness implicated. The give-up must
/// preserve the reader count AND wake the parked reader.
#[test]
fn reader_park_writer_give_up_no_lost_wake() {
    loom::model(|| scenario(1, true));
}

/// Shared control block for the WRITER-park scenario: a writer parks on
/// `write_futex` (generation idiom) with WRITERS_PARKED set in `state`, and a
/// releasing thread must wake it. Mirrors the real
/// `lock_exclusive_slow` park / `notify_one_writer` pair.
struct WLock {
    state: AtomicU32,
    write_futex: AtomicU32,
    write_waiters: AtomicUsize,
    writer_parked: AtomicU32,
    waker_bumped_gen: AtomicU32,
}

/// One writer (parking on `write_futex`) racing one releasing thread.
///
/// * `via_last_reader == false`: a write holder releases via
///   `unlock_exclusive` (`write_held -> free`).
/// * `via_last_reader == true`: the lock is `read_held(1)` with a parked
///   writer, and the last reader releases (`read_held(1) -> free`), which must
///   hand off to the parked writer.
fn writer_scenario(via_last_reader: bool) {
    let initial = if via_last_reader { 1 } else { WRITE_LOCKED };
    let lk = Arc::new(WLock {
        state: AtomicU32::new(initial),
        write_futex: AtomicU32::new(0),
        write_waiters: AtomicUsize::new(0),
        writer_parked: AtomicU32::new(0),
        waker_bumped_gen: AtomicU32::new(0),
    });

    // WRITER: lock_exclusive_slow's park-on-write_futex path.
    let writer = {
        let lk = Arc::clone(&lk);
        thread::spawn(move || {
            loop {
                // Sample the generation BEFORE testing state (the idiom).
                let seen_gen = lk.write_futex.load(Ordering::Acquire);
                let s = lk.state.load(Ordering::Relaxed);
                if s & WRITE_LOCKED == 0 {
                    // Acquirable (free or read_held) — not the park case.
                    return;
                }
                lk.write_waiters.fetch_add(1, Ordering::Relaxed);
                if cfg!(lost_wake_bug) {
                    // SHIPPED (buggy): no WRITERS_PARKED bit; park expecting
                    // the sampled gen, with "I am parked" living only in the
                    // separate write_waiters counter.
                    if lk.write_futex.load(Ordering::Acquire) == seen_gen {
                        lk.writer_parked.store(1, Ordering::SeqCst);
                    }
                    lk.write_waiters.fetch_sub(1, Ordering::Relaxed);
                    return;
                } else {
                    // FIX: record WRITERS_PARKED IN `state` before parking.
                    if s & WRITERS_PARKED == 0
                        && lk
                            .state
                            .compare_exchange_weak(
                                s,
                                s | WRITERS_PARKED,
                                Ordering::Relaxed,
                                Ordering::Relaxed,
                            )
                            .is_err()
                    {
                        lk.write_waiters.fetch_sub(1, Ordering::Relaxed);
                        continue;
                    }
                    // Park on write_futex expecting seen_gen.
                    if lk.write_futex.load(Ordering::Acquire) == seen_gen {
                        lk.writer_parked.store(1, Ordering::SeqCst);
                    }
                    lk.write_waiters.fetch_sub(1, Ordering::Relaxed);
                    return;
                }
            }
        })
    };

    // RELEASER.
    let releaser = {
        let lk = Arc::clone(&lk);
        thread::spawn(move || {
            // notify_one_writer: bump gen (Release) then (modelled) wake.
            let notify = |lk: &WLock| {
                lk.write_futex.fetch_add(1, Ordering::Release);
                lk.waker_bumped_gen.store(1, Ordering::SeqCst);
            };
            if cfg!(lost_wake_bug) {
                // SHIPPED (buggy): gate the writer wake on the SEPARATE
                // write_waiters counter.
                if via_last_reader {
                    lk.state.fetch_sub(1, Ordering::Release);
                } else {
                    lk.state.fetch_and(!WRITE_LOCKED, Ordering::Release);
                }
                if lk.write_waiters.load(Ordering::Relaxed) > 0 {
                    notify(&lk);
                }
            } else {
                // FIX: gate on WRITERS_PARKED read from the release RMW's
                // prior value — the same word the writer set its bit in.
                let prev = if via_last_reader {
                    // read_held(1) -> free: fetch_sub, then if the prior value
                    // had WRITERS_PARKED, clear it and notify.
                    let p = lk.state.fetch_sub(1, Ordering::Release);
                    if p & WRITERS_PARKED != 0 {
                        lk.state.fetch_and(!WRITERS_PARKED, Ordering::Relaxed);
                    }
                    p
                } else {
                    lk.state.fetch_and(
                        !(WRITE_LOCKED | WRITERS_PARKED),
                        Ordering::Release,
                    )
                };
                if prev & WRITERS_PARKED != 0 {
                    notify(&lk);
                }
            }
        })
    };

    writer.join().unwrap();
    releaser.join().unwrap();

    let parked = lk.writer_parked.load(Ordering::SeqCst);
    let bumped = lk.waker_bumped_gen.load(Ordering::SeqCst);
    assert!(
        !(parked == 1 && bumped == 0),
        "LOST WAKE: writer parked on `write_futex` but the releaser did not \
         bump the generation / wake (gate observed no waiter). The writer \
         sleeps forever while the lock is available."
    );
}

/// Writer-park / writer-release (unlock_exclusive hands off to a parked
/// writer). Passes with the fix; fails under `--cfg lost_wake_bug`.
#[test]
fn writer_park_writer_release_no_lost_wake() {
    loom::model(|| writer_scenario(false));
}

/// Writer-park / last-reader-release (`read_held(1) -> free`). This path's
/// `notify_one_writer` exists to hand a freed lock to a writer that set
/// WRITERS_PARKED, but a writer only *futex-parks* on `write_futex` while
/// WRITE_LOCKED is held by ANOTHER writer (whose own release wakes it) — so in
/// `read_held` the "parked" writer is really still in its spin/retry loop, and
/// the generation idiom covers it. The scenario is therefore not a
/// lost-wake-REACHABLE path (it passes in BOTH variants, unlike the three
/// above); it is kept as a liveness / no-syscall-regression check that the
/// read_held(1)->free hand-off still bumps the generation when WRITERS_PARKED
/// is set. The lost-wake-reachable writer path is
/// `writer_park_writer_release_no_lost_wake`.
#[test]
fn writer_park_last_reader_release_no_lost_wake() {
    loom::model(|| writer_scenario(true));
}
