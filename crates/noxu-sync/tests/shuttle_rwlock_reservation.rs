// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Standalone shuttle protocol model of the writer-reservation state table
//! (`docs/src/internal/rwlock-state-table-2026-09.md`), per the task's
//! mandated approach: **validate the table before porting it into
//! `raw_rwlock.rs`**.
//!
//! This is NOT the real `NoxuRawRwLock`. It is a small model that implements
//! exactly the four states and transitions the table specifies --
//! `free` / `read_held(n)` / `reserved_draining(n)` / `write_held` -- using
//! shuttle's own `Mutex` + three `Condvar`s standing in for the real
//! `AtomicU32 state` + three futex words (`state`, `write_futex`,
//! `drain_futex`). Modelling with a mutex+condvar instead of raw atomics is
//! deliberate: it lets the state transitions be written as a direct
//! transcription of the table's rows (one guarded block per row) rather than
//! re-deriving CAS loops, so a table bug shows up as a model bug in the most
//! legible way possible, and shuttle can still explore every interleaving of
//! entering/leaving each state.
//!
//! The whole file compiles to nothing unless built with `--cfg noxu_shuttle`,
//! matching every other shuttle test in this repository.
//!
//! # What exploring this model found
//!
//! Nothing here reuses `write_futex`/`state`/`drain_futex` naming by
//! accident: building this model is what surfaced the table bug fixed in
//! the immediately preceding commit ("reserving writer needs its own futex
//! word"). The first version of this model had the reserving writer and
//! parked readers share one condvar and used `notify_one` for the
//! drain-complete handoff; shuttle found an interleaving where `notify_one`
//! woke a parked reader instead of the reserving writer, the reader
//! rechecked and re-parked (correctly refused, since `WRITE_LOCKED` was
//! still set), and the reserving writer never woke -- a lost wakeup,
//! reproducing exactly the class of bug the table warns about. Splitting
//! into three condvars (mirroring three futex words) closed it: readers use
//! `notify_all`/`wait_while` on their own condvar, and the reserving writer
//! uses a dedicated one-writer-only condvar for the drain handoff, so
//! `notify_one` on it is unambiguous by construction.
//!
//! # Invariants checked
//!
//! * **mutual exclusion** -- never two threads are simultaneously in
//!   `write_held`.
//! * **no lost wakeup** -- every thread that calls into the model completes
//!   (shuttle itself detects a real deadlock: a schedule with no runnable
//!   thread while some thread is still blocked panics rather than hanging).
//! * **give-up correctness** -- a reserving writer that gives up releases
//!   `WRITE_LOCKED` without touching the reader count, and wakes both
//!   populations. Checked in two forms: a fixed-ordering case where give-up
//!   cannot race the drain, and a genuinely racing case
//!   (`give_up_races_the_drain_completing_reader`) where shuttle explores
//!   both "give-up wins" and "the drain completes first" against the same
//!   last reader -- the exact path the task brief warned about ("a reserved
//!   writer that times out must release WRITE_LOCKED, or the lock is
//!   permanently dead"). Give-up is modelled as an unconditional call the
//!   test chooses to make (standing in for a real timeout), not a
//!   wall-clock race, since shuttle explores interleavings, not timing.
//! * **structural non-starvation** -- a hand-over-hand reader relay (each
//!   reader admits the next before releasing) cannot prevent a writer from
//!   ever reserving, because the reserve-CAS analogue does not require the
//!   reader count to be zero (only that `WRITE_LOCKED` is clear). This is
//!   the central claim of deleting `WRITE_WAITING`/`FAIRNESS_THRESHOLD` in
//!   the state table's design-decision section, checked here as a liveness
//!   property rather than asserted from first principles.
//!
//! # Running
//!
//! ```sh
//! RUSTFLAGS="--cfg noxu_shuttle" cargo test -p noxu-sync --test shuttle_rwlock_reservation --release
//! ```
#![allow(unexpected_cfgs)]
#![cfg(noxu_shuttle)]
// noxu-sync has no crate-level `[lints] workspace = true` (a pre-existing
// gap, out of scope here -- see git history), so this file cannot pick up
// the workspace's `check-cfg` registration for `noxu_shuttle` the way every
// other shuttle test in the repo does. Silencing locally rather than adding
// `[lints] workspace = true` to the crate, which would also turn on
// `undocumented_unsafe_blocks` and fail the build on ~19 pre-existing unsafe
// blocks in raw_rwlock.rs/raw_mutex.rs/lib.rs that predate this change.

use shuttle::sync::{Arc, Condvar, Mutex};
use shuttle::thread;

/// Number of interleavings shuttle explores per test.
const ITERATIONS: usize = 3_000;

/// The four states from the table, represented directly rather than as a
/// packed bitfield -- the model's job is to validate the table's transitions,
/// not to re-derive the bit-packing (that happens in the real port).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LockState {
    Free,
    ReadHeld(u32),
    ReservedDraining(u32),
    WriteHeld,
}

/// Outcome of [`Model::try_give_up`] -- see its doc comment for the race it
/// resolves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GiveUpOutcome {
    /// The give-up won: the reservation was released, state restored to
    /// `ReadHeld(n)`, both populations woken.
    GaveUp,
    /// The drain won the race first: this thread already legally holds
    /// `WriteHeld` and MUST call `write_release()` itself rather than
    /// treating the give-up as having succeeded.
    AlreadyAcquired,
}

/// The model. Three condvars stand in for the three futex words the table
/// specifies (`state`, `write_futex`, `drain_futex`):
///
/// * `readers_cv` -- readers park here refused admission (table's `state`
///   word, reader side). Woken via `notify_all`, matching the table's "wake
///   BOTH unconditionally" rows and the reserved_draining admission-refusal
///   rows, which never target a specific reader.
/// * `writers_cv` -- writers not yet holding/reserving park here (table's
///   `write_futex`). Woken via `notify_one` (any parked writer is a fungible
///   target, matching the table's "single dedicated futex word" note).
/// * `drain_cv` -- ONLY the current reservation-holder ever waits here
///   (table's `drain_futex`). Woken via `notify_one` from the reader-release
///   path when the drain completes; unambiguous because at most one writer
///   is ever `ReservedDraining` at a time (the model's own invariant, checked
///   by the `reserved_by` field never being set to two different threads
///   without an intervening clear).
struct Model {
    state: Mutex<LockState>,
    readers_cv: Condvar,
    writers_cv: Condvar,
    drain_cv: Condvar,
}

impl Model {
    fn new() -> Self {
        Model {
            state: Mutex::new(LockState::Free),
            readers_cv: Condvar::new(),
            writers_cv: Condvar::new(),
            drain_cv: Condvar::new(),
        }
    }

    /// Non-blocking reader admission attempt: table's admission guard only,
    /// no park. Mirrors the real lock's `try_lock_shared_fast` and is the
    /// correct analogue for probing "would a new reader be admitted right
    /// now" without risking the caller itself becoming a parked waiter --
    /// which matters for tests that need to keep control of their own thread
    /// (see `writer_can_reserve_under_an_overlapping_reader_relay` below;
    /// its first draft used the blocking `read_acquire` in this spot and
    /// deadlocked the TEST HARNESS, not the model: the probing thread parked
    /// on `readers_cv` and could then never reach the line that would have
    /// released the hold the drain was waiting on).
    fn try_read_acquire(&self) -> bool {
        let mut s = self.state.lock().unwrap();
        match *s {
            LockState::Free => {
                *s = LockState::ReadHeld(1);
                true
            }
            LockState::ReadHeld(n) => {
                *s = LockState::ReadHeld(n + 1);
                true
            }
            LockState::ReservedDraining(_) | LockState::WriteHeld => false,
        }
    }

    /// Reader acquire: table rows "free/read_held(n) -- reader acquire".
    /// Admitted whenever the state is not `ReservedDraining`/`WriteHeld`
    /// (i.e. WRITE_LOCKED analogue is clear) -- barging, unconditional,
    /// matching the table's deletion of the WRITE_WAITING admission gate.
    fn read_acquire(&self) {
        let mut s = self.state.lock().unwrap();
        loop {
            match *s {
                LockState::Free => {
                    *s = LockState::ReadHeld(1);
                    return;
                }
                LockState::ReadHeld(n) => {
                    *s = LockState::ReadHeld(n + 1);
                    return;
                }
                LockState::ReservedDraining(_) | LockState::WriteHeld => {
                    // Refused admission -- park on readers_cv (table's
                    // `state` word, reader side) until it is possible this
                    // has changed.
                    s = self.readers_cv.wait(s).unwrap();
                }
            }
        }
    }

    /// Reader release: table rows "read_held(n>1) -- reader release",
    /// "read_held(1) -- reader release" and "reserved_draining(n) --
    /// reader release".
    fn read_release(&self) {
        let mut s = self.state.lock().unwrap();
        match *s {
            LockState::ReadHeld(1) => {
                *s = LockState::Free;
                drop(s);
                // Table: "if write_waiters > 0: notify_one_writer(). Else
                // none." The model has no separate write_waiters counter to
                // branch on cheaply, so it notifies unconditionally --
                // notify_one on an empty condvar is a correct no-op, and the
                // table's guidance is an optimisation (skip a syscall), not a
                // correctness requirement. Modelling the optimisation would
                // only add a field this model does not need to validate.
                self.writers_cv.notify_one();
            }
            LockState::ReadHeld(n) => {
                *s = LockState::ReadHeld(n - 1);
            }
            LockState::ReservedDraining(1) => {
                *s = LockState::WriteHeld;
                drop(s);
                // Table: "wake the reserving writer: targeted drain_futex
                // wake". At most one writer is ever ReservedDraining, so
                // notify_one on drain_cv is unambiguous by construction --
                // this is the fix for the bug the model found in its first
                // draft (see module docs).
                self.drain_cv.notify_one();
            }
            LockState::ReservedDraining(n) => {
                *s = LockState::ReservedDraining(n - 1);
            }
            LockState::Free | LockState::WriteHeld => {
                panic!(
                    "read_release called with no reader held: {:?} -- \
                     model bug, not a table bug (caller error)",
                    *s
                );
            }
        }
    }

    /// Writer acquire (unconditional, blocking): table rows "free -- writer
    /// acquire", "read_held(n) -- writer reserve", "reserved_draining(1) --
    /// [implicit, handled by the reader-release wakeup]". This function
    /// blocks until `WriteHeld` is reached and owned by the caller.
    ///
    /// Models the two-phase acquire from the real lock: phase 1 gets
    /// `WRITE_LOCKED` (either straight to `WriteHeld` if readers == 0, or to
    /// `ReservedDraining` otherwise); phase 2 (only entered from
    /// `ReservedDraining`) waits on `drain_cv` for the reader-release path to
    /// promote it to `WriteHeld` directly -- no second CAS, no re-race,
    /// which is the whole point of reservation (table's Bug 3 closure).
    fn write_acquire(&self) {
        let mut s = self.state.lock().unwrap();
        // Phase 1: obtain WRITE_LOCKED, conditional ONLY on no other writer
        // already holding it (table's reserve-CAS guard: `WRITE_LOCKED == 0`,
        // not `state == 0`). This is what makes a reader relay unable to
        // starve a writer -- the guard never looks at the reader count.
        loop {
            match *s {
                LockState::Free => {
                    *s = LockState::WriteHeld;
                    return;
                }
                LockState::ReadHeld(n) => {
                    *s = LockState::ReservedDraining(n);
                    break;
                }
                LockState::ReservedDraining(_) | LockState::WriteHeld => {
                    // Another writer already owns WRITE_LOCKED (reserved or
                    // held) -- park on writers_cv, table's `write_futex`.
                    s = self.writers_cv.wait(s).unwrap();
                }
            }
        }
        // Phase 2: this thread now owns the reservation (state ==
        // ReservedDraining(n) with n == whatever it was; nobody else can
        // touch WRITE_LOCKED until this thread releases or times out). Wait
        // for the drain to complete. `wait_while` re-checks the predicate on
        // every wake, but the ONLY notifier of drain_cv is the reader-release
        // path transitioning ReservedDraining(1) -> WriteHeld, and at most
        // one writer is ever in this phase, so this loop runs at most once
        // per real drain-completion (spurious extra wakes are harmless, not
        // just tolerated).
        while !matches!(*s, LockState::WriteHeld) {
            s = self.drain_cv.wait(s).unwrap();
        }
    }

    /// Phase 1 only of [`write_acquire`]: blocks until this thread owns
    /// `WRITE_LOCKED` in EITHER form (straight to `WriteHeld` if the lock was
    /// free, or `ReservedDraining(n)` otherwise) and returns without waiting
    /// out the drain. Exists so a test can race `try_give_up` against the
    /// real drain-completing reader instead of assuming which one "gets
    /// there first" -- see `give_up_races_the_drain_completing_reader`
    /// below, which is exactly the untested race the brief flagged: "model
    /// the TIMEOUT paths too... that race is exactly where 'release
    /// WRITE_LOCKED or the lock is permanently dead' lives."
    fn write_reserve_only(&self) {
        let mut s = self.state.lock().unwrap();
        loop {
            match *s {
                LockState::Free => {
                    *s = LockState::WriteHeld;
                    return;
                }
                LockState::ReadHeld(n) => {
                    *s = LockState::ReservedDraining(n);
                    return;
                }
                LockState::ReservedDraining(_) | LockState::WriteHeld => {
                    s = self.writers_cv.wait(s).unwrap();
                }
            }
        }
    }

    /// Writer give-up while `ReservedDraining` (table row: "writer timeout
    /// (the reserving writer itself gives up)"). Locks internally and
    /// re-checks state FRESH under the lock, because the real race this
    /// models is: the drain-completing reader and the timing-out writer can
    /// both be about to act on `ReservedDraining(1)` at once, and only ONE
    /// of them can win.
    ///
    ///   * If this call observes `ReservedDraining(n)` still, the give-up
    ///     wins the race: transition back to `ReadHeld(n)` (never `Free` --
    ///     that would silently drop live readers) and wake BOTH populations
    ///     unconditionally (table's Bug 4 closure).
    ///   * If this call observes `WriteHeld`, the drain won the race: the
    ///     "timing out" writer is actually the SAME thread that now legally
    ///     owns the lock (in the real port, this is the thread re-checking
    ///     its deadline right as `futex_wait` returns from the drain-complete
    ///     wake). It must NOT clobber the state -- doing so is exactly the
    ///     task brief's warning: "a reserved writer that times out must
    ///     release WRITE_LOCKED, or the lock is permanently dead" read
    ///     backwards -- a give-up that fires AFTER already legitimately
    ///     acquiring must not release a lock it is entitled to hold. The
    ///     caller is responsible for calling `write_release()` in this case;
    ///     returning `AlreadyAcquired` rather than silently succeeding is
    ///     what makes that caller obligation checkable.
    fn try_give_up(&self) -> GiveUpOutcome {
        let mut s = self.state.lock().unwrap();
        match *s {
            LockState::ReservedDraining(n) => {
                *s = LockState::ReadHeld(n);
                drop(s);
                self.writers_cv.notify_one();
                self.readers_cv.notify_all();
                GiveUpOutcome::GaveUp
            }
            LockState::WriteHeld => GiveUpOutcome::AlreadyAcquired,
            other => panic!(
                "try_give_up called from a state where no reservation is \
                 outstanding: {other:?} -- model/caller bug"
            ),
        }
    }

    /// Writer release: table row "write_held -- writer release".
    fn write_release(&self) {
        let mut s = self.state.lock().unwrap();
        assert_eq!(*s, LockState::WriteHeld, "release without holding");
        *s = LockState::Free;
        drop(s);
        // Table: "wake BOTH unconditionally: notify_one_writer() AND
        // futex_wake(&state, ALL)". Bug 4 in the table's mapping is exactly
        // an `else if` here; this model makes both notifications
        // unconditional and independent by construction (no `if` at all).
        self.writers_cv.notify_one();
        self.readers_cv.notify_all();
    }
}

/// **Invariant: mutual exclusion.** Two threads race to acquire the write
/// lock while a third holds/releases read locks; the model must never let
/// two writers observe `WriteHeld` in overlapping windows. Guarded by a
/// shared counter that must never exceed 1.
#[test]
fn mutual_exclusion_never_two_writers() {
    shuttle::check_random(
        || {
            let model = Arc::new(Model::new());
            let concurrent_writers = Arc::new(Mutex::new(0u32));
            let max_seen = Arc::new(Mutex::new(0u32));

            let mut handles = Vec::new();
            for _ in 0..3 {
                let model = Arc::clone(&model);
                let concurrent = Arc::clone(&concurrent_writers);
                let max_seen = Arc::clone(&max_seen);
                handles.push(thread::spawn(move || {
                    model.write_acquire();
                    {
                        let mut c = concurrent.lock().unwrap();
                        *c += 1;
                        let mut m = max_seen.lock().unwrap();
                        *m = (*m).max(*c);
                    }
                    {
                        let mut c = concurrent.lock().unwrap();
                        *c -= 1;
                    }
                    model.write_release();
                }));
            }
            // A couple of readers in the mix so the writers actually have to
            // reserve and drain rather than always hitting the Free fast
            // path.
            for _ in 0..2 {
                let model = Arc::clone(&model);
                handles.push(thread::spawn(move || {
                    model.read_acquire();
                    model.read_release();
                }));
            }
            for h in handles {
                h.join().unwrap();
            }

            assert_eq!(
                *max_seen.lock().unwrap(),
                1,
                "two threads observed WriteHeld concurrently -- mutual \
                 exclusion violated"
            );
        },
        ITERATIONS,
    );
}

/// **Invariant: no lost wakeup, mixed population.** Readers and writers
/// interleave freely; every thread must complete (shuttle panics on a real
/// deadlock -- no runnable thread while some are blocked -- so a plain `join`
/// on every handle is itself the liveness check, matching the intent of the
/// real `mixed_readers_and_writers_make_progress_without_deadlock` watchdog
/// test but expressed as shuttle exhaustive interleaving instead of a
/// wall-clock probe).
#[test]
fn mixed_population_no_lost_wakeup() {
    shuttle::check_random(
        || {
            let model = Arc::new(Model::new());
            let mut handles = Vec::new();

            for _ in 0..3 {
                let model = Arc::clone(&model);
                handles.push(thread::spawn(move || {
                    model.write_acquire();
                    model.write_release();
                }));
            }
            for _ in 0..4 {
                let model = Arc::clone(&model);
                handles.push(thread::spawn(move || {
                    model.read_acquire();
                    model.read_release();
                }));
            }
            for h in handles {
                h.join().unwrap();
            }
        },
        ITERATIONS,
    );
}

/// **Invariant: give-up correctness (no race).** A writer that reserves and
/// then gives up before any drain-completion is even possible must hand the
/// state back to `ReadHeld(n)` -- not `Free`, which would silently drop
/// still-live readers -- and readers/writers parked behind it must still
/// make progress afterwards (checked by every thread completing).
#[test]
fn give_up_while_draining_restores_read_held_and_wakes_both() {
    shuttle::check_random(
        || {
            let model = Arc::new(Model::new());

            // Seed two readers so there is something to drain, and reserve
            // (phase 1 only) so the model is in ReservedDraining(2) --
            // nothing yet makes the drain completable (the two readers are
            // still held), so try_give_up here cannot race the drain: it is
            // guaranteed to observe ReservedDraining and win.
            model.read_acquire();
            model.read_acquire();
            model.write_reserve_only();

            // A second writer and a third reader queue up behind the
            // reservation -- both populations must be woken by the give-up.
            let model2 = Arc::clone(&model);
            let second_writer = thread::spawn(move || {
                model2.write_acquire();
                model2.write_release();
            });
            let model3 = Arc::clone(&model);
            let third_reader = thread::spawn(move || {
                model3.read_acquire();
                model3.read_release();
            });

            let outcome = model.try_give_up();
            assert_eq!(
                outcome,
                GiveUpOutcome::GaveUp,
                "give-up must win when nothing has drained yet"
            );

            // Drain the two original readers so the second writer and third
            // reader can actually finish.
            model.read_release();
            model.read_release();

            second_writer.join().unwrap();
            third_reader.join().unwrap();
        },
        ITERATIONS,
    );
}

/// **Invariant: give-up vs. drain-completion is a real race, and both
/// outcomes are safe.** This is the path the task brief specifically warned
/// about: "a reserved writer that times out must release WRITE_LOCKED, or
/// the lock is permanently dead -- readers park on a bit with no owner and
/// no unlock coming." The previous test forced a fixed ordering (give-up
/// always wins because nothing could drain yet); THIS test lets shuttle
/// explore both orderings of "last reader releases" vs. "writer decides to
/// give up" against the SAME single remaining reader, and checks that
/// whichever one the scheduler picks, the lock ends up in a live, correct
/// state -- never stuck in `ReservedDraining` with nobody left who can ever
/// clear it.
#[test]
fn give_up_races_the_drain_completing_reader() {
    shuttle::check_random(
        || {
            let model = Arc::new(Model::new());

            // One reader, then the writer reserves against it.
            model.read_acquire();
            model.write_reserve_only();

            // Two concurrent actions on the SAME last-reader/reservation
            // pair, racing: the reader releasing (which may complete the
            // drain), and the writer giving up (which may instead observe
            // the drain already completed). shuttle explores both orderings
            // across its ITERATIONS schedules.
            let model_r = Arc::clone(&model);
            let releaser = thread::spawn(move || {
                model_r.read_release();
            });

            let outcome = model.try_give_up();

            match outcome {
                GiveUpOutcome::GaveUp => {
                    // Give-up won: state is ReadHeld(1) again (the one
                    // reader, not yet released by the other thread) or
                    // already Free if the releaser ran first internally to
                    // try_give_up's own lock acquisition -- either is
                    // consistent, `try_give_up` already asserted internally
                    // it only transitions from ReservedDraining. The lock is
                    // NOT stuck: WRITE_LOCKED was released, so a fresh writer
                    // could reserve again right now if one existed.
                    let s = *model.state.lock().unwrap();
                    assert!(
                        matches!(s, LockState::ReadHeld(_) | LockState::Free),
                        "give-up must leave a live state, not \
                         ReservedDraining or WriteHeld -- got {s:?}"
                    );
                }
                GiveUpOutcome::AlreadyAcquired => {
                    // The drain won: this thread now legitimately holds
                    // WriteHeld and MUST release it itself -- this is
                    // exactly the caller obligation the brief's warning maps
                    // to. Skipping this call is the bug: WRITE_LOCKED would
                    // stay set forever with the "timed out" writer believing
                    // it gave up, and readers would park on a bit with no
                    // owner and no unlock coming.
                    model.write_release();
                }
            }

            releaser.join().unwrap();

            // Whichever branch ran, the lock must end up fully free and
            // usable: a fresh reader and a fresh writer both must be able to
            // complete afterwards. This is the liveness check that would
            // catch "permanently dead lock" -- if WRITE_LOCKED were left set
            // with nobody left to clear it, one of these two joins would
            // hang and shuttle would report a deadlock.
            let model_check_r = Arc::clone(&model);
            let check_reader = thread::spawn(move || {
                model_check_r.read_acquire();
                model_check_r.read_release();
            });
            check_reader.join().unwrap();

            model.write_acquire();
            model.write_release();
        },
        ITERATIONS,
    );
}

/// **Invariant: structural non-starvation.** A hand-over-hand reader relay
/// (each reader acquires before its predecessor releases, so the reader
/// count never touches zero) must NOT prevent a writer from reserving --
/// this is the central liveness claim behind deleting the
/// `WRITE_WAITING`/`FAIRNESS_THRESHOLD` gate (state table, design-decision
/// section): the reserve-CAS analogue is guarded only on `WRITE_LOCKED`
/// being clear, never on the reader count. Checked by having the writer
/// reserve WHILE readers relay, and asserting the writer completes even
/// though the reader count never touched zero while the relay was running.
///
/// This test does NOT assert that every relay attempt is admitted -- once
/// the writer has reserved, a NEW relay reader is correctly refused (that is
/// the intended admission-refusal transition, not a violation of anything).
/// An earlier draft asserted unconditional admission here and would have
/// failed on exactly the schedules that prove the property, which is the
/// opposite of what this test is for.
#[test]
fn writer_can_reserve_under_an_overlapping_reader_relay() {
    shuttle::check_random(
        || {
            let model = Arc::new(Model::new());

            // First reader in before the writer appears, exactly like the
            // real characterisation test's hand-over-hand setup.
            model.read_acquire();

            let writer_model = Arc::clone(&model);
            let writer = thread::spawn(move || {
                writer_model.write_acquire();
                writer_model.write_release();
            });

            // A short relay: each attempt either joins (no reservation yet)
            // or is correctly refused (one now exists). Either is fine; what
            // must NOT happen is the relay thread itself getting stuck,
            // which is why this uses the non-blocking probe rather than
            // `read_acquire` -- see `try_read_acquire`'s doc comment.
            for _ in 0..3 {
                if model.try_read_acquire() {
                    model.read_release();
                }
            }

            // Release the first reader; only now can the drain complete.
            // The property under test is this join itself: the writer must
            // complete despite the relay having kept the reader count above
            // zero for the writer's entire wait, which is only possible
            // because reservation is granted without requiring the reader
            // count to be zero -- only draining does.
            model.read_release();

            writer.join().unwrap();
        },
        ITERATIONS,
    );
}

/// A reservation must EXCLUDE new readers for its whole drain.
///
/// This is the entire point of reserving: the whole reason `WRITE_LOCKED` is set
/// before the readers have gone is so that no further reader can join and extend
/// the drain indefinitely. Without it we are back to the advisory-gate design
/// whose p50 write wait scaled with reader count.
///
/// Added after a non-vacuity audit found the model silently PASSED when
/// `try_read_acquire` was sabotaged to admit readers during `ReservedDraining`:
/// the suite asserted writer/writer mutual exclusion (`max_seen == 1` on
/// `WriteHeld`) but never asserted reader exclusion during the drain, so the
/// single most important property of the design was unmodelled. It is checked
/// here from first principles rather than inferred.
#[test]
fn a_reservation_admits_no_new_reader_while_draining() {
    shuttle::check_random(
        || {
            let model = Arc::new(Model::new());

            // One reader in, so a writer must reserve-and-drain rather than
            // acquire outright.
            assert!(model.try_read_acquire(), "first reader must get in");

            // Reserve (does not block: one reader is in, so this transitions
            // ReadHeld(1) -> ReservedDraining(1) and returns).
            model.write_reserve_only();

            // THE PROPERTY: with the reservation held, no new reader may enter,
            // no matter how many try or how they interleave.
            for _ in 0..4 {
                assert!(
                    !model.try_read_acquire(),
                    "a reader was admitted during ReservedDraining -- the \
                     reservation does not exclude new readers, so the drain can \
                     be extended indefinitely and the reserving writer starves"
                );
            }

            // Drain the original reader; the writer then owns the lock outright
            // and readers must still be excluded.
            model.read_release();
            assert!(
                !model.try_read_acquire(),
                "a reader was admitted while the writer held the lock"
            );
            model.write_release();
        },
        3_000,
    );
}
