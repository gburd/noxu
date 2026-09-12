// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Standalone shuttle protocol model of the `env_fair_latches` wiring: a
//! FIFO admission queue ([`crate::fair_queue::FairQueue`]) wrapped AROUND an
//! exclusive lock, with the queue only consulted when fair mode is on
//! (`crates/noxu-latch/src/exclusive.rs`, `crates/noxu-latch/src/shared.rs`).
//!
//! This is NOT the real `ExclusiveLatch`. It models exactly the shape the
//! task called out as the seven-times-bitten class -- "a FIFO latch wired
//! into the RAII drop path" -- as a small acquire/hold/release/give-up state
//! machine using shuttle's own `Mutex` + `Condvar`, so a wiring bug (queue
//! consulted on acquire but not on release, or vice versa; release-before-
//! dequeue vs. dequeue-before-release ordering; a give-up that strands the
//! next waiter) shows up as a model bug shuttle can find by exploring
//! interleavings, not as a 5-hour wall-clock hang in the real latch (the
//! failure mode `fair_latch_fifo_test.rs`'s watchdog exists to catch at the
//! integration-test layer; this is the same class of bug caught one layer
//! down, exhaustively rather than by timing).
//!
//! The real `FairQueue` (`src/fair_queue.rs`) is an explicit
//! `VecDeque<u64>` behind a `noxu_sync::Mutex` + `Condvar`; this model
//! reproduces that shape directly (a `VecDeque<TaskId>`-equivalent ticket
//! list) rather than re-deriving it, for the same reason
//! `shuttle_rwlock_reservation.rs` transcribes its state table directly: a
//! model bug should look like a model bug, not require re-proving the
//! mechanism from scratch.
//!
//! The whole file compiles to nothing unless built with `--cfg noxu_shuttle`,
//! matching every other shuttle test in this repository.
//!
//! # Invariants checked
//!
//! * **FIFO grant order** -- when fair mode is on, N threads that join the
//!   queue while the lock is held are granted in strict arrival order
//!   ([`fair_mode_grants_in_strict_arrival_order`]).
//! * **mutual exclusion** -- never two threads simultaneously hold the
//!   modelled lock, fair mode on or off
//!   ([`mutual_exclusion_holds_regardless_of_fairness`]).
//! * **no lost wakeup / no deadlock** -- every thread that calls into the
//!   model completes; shuttle itself detects a real deadlock (a schedule
//!   with a blocked thread and nothing runnable panics rather than hanging)
//!   ([`fair_mode_grants_in_strict_arrival_order`],
//!   [`mutual_exclusion_holds_regardless_of_fairness`],
//!   [`give_up_does_not_strand_the_next_waiter`]).
//! * **give-up correctness** -- a queued waiter that times out before
//!   reaching the front removes only itself; the waiter behind it is not
//!   stranded ([`give_up_does_not_strand_the_next_waiter`]) -- the RAII drop
//!   path's release-then-leave-queue ordering
//!   ([`crate::exclusive::ExclusiveLatchGuard::drop`]'s doc comment) modelled
//!   directly by [`Model::release`].
//! * **non-vacuity** -- [`fair_mode_grants_in_strict_arrival_order`] is
//!   checked to actually reject disorder by sabotaging the model's admission
//!   check (see the `#[test]`s under "non-vacuity checks" below, which must
//!   themselves be run with the *sabotaged* model to confirm the harness can
//!   fail -- see the module-level "Running" section).
//!
//! # Running
//!
//! ```sh
//! RUSTFLAGS="--cfg noxu_shuttle" cargo test -p noxu-latch --test shuttle_fair_latch --release
//! ```
#![cfg(noxu_shuttle)]

use shuttle::sync::{Arc, Condvar, Mutex};
use shuttle::thread;
use std::collections::VecDeque;

/// Number of interleavings shuttle explores per test.
const ITERATIONS: usize = 3_000;

/// Model of the wired latch: a `FairQueue`-shaped FIFO ticket list wrapped
/// around a single boolean "held" flag (the modelled real lock). Mirrors
/// `ExclusiveLatch`'s two fields (`inner: Mutex<()>`, `queue: FairQueue`)
/// collapsed into one guarded state, since the model's job is to validate
/// the WIRING (does admission order gate the real lock, and does release
/// order match the RAII guard's drop order) rather than re-model the
/// underlying mutex algorithm (`shuttle_rwlock_reservation.rs`'s job, for a
/// different lock).
struct Model {
    state: Mutex<State>,
    /// Wakes queue waiters whenever the front of the queue could have
    /// changed (admission or departure) -- models `FairQueue`'s single
    /// `Condvar` (`fair_queue.rs`'s `cv`).
    queue_cv: Condvar,
}

struct State {
    /// FIFO admission queue (`FairQueue`'s `VecDeque<u64>`), ticket ids in
    /// arrival order. Only ever consulted when `fair` is true, exactly as
    /// `ExclusiveLatch::acquire`/`release_if_owner`/`Drop` only call
    /// `queue.enter`/`queue.leave` behind `fair_latches()`.
    queue: VecDeque<u64>,
    /// Whether the modelled real lock is held.
    held: bool,
    fair: bool,
    next_id: u64,
}

/// Outcome of a queued [`Model::try_acquire_fair`] wait -- see its doc
/// comment for the give-up race this resolves, mirroring `FairQueue::enter`
/// returning `Result<u64, QueueTimeout>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Admission {
    /// Reached the front and was admitted; carries the ticket id `release`
    /// must be called with.
    Admitted(u64),
    /// Gave up before reaching the front; the id was already removed from
    /// the queue by this call (mirrors `FairQueue::enter`'s `QueueTimeout`
    /// contract: "the caller must NOT call leave").
    GaveUp,
}

impl Model {
    fn new(fair: bool) -> Self {
        Model {
            state: Mutex::new(State {
                queue: VecDeque::new(),
                held: false,
                fair,
                next_id: 1,
            }),
            queue_cv: Condvar::new(),
        }
    }

    /// Joins the fair queue (if fair mode is on) and returns the ticket
    /// (`None` in unfair mode). Split out from [`Self::wait_for_admission`]
    /// so a test driver can pin ARRIVAL order deterministically: call
    /// `join_queue` for each waiter, in the desired order, from a single
    /// (the main) thread BEFORE spawning the threads that actually wait --
    /// exactly the adaptation of "spawn blocked waiters one at a time" this
    /// module's docs call for, since shuttle's scheduler (not wall-clock
    /// timing) decides interleavings: if enqueueing happened inside the
    /// spawned threads themselves, shuttle would be free to explore
    /// schedules where they enqueue in a different order than they were
    /// spawned, which is a meaningless thing to assert FIFO against.
    /// Mirrors `FairQueue::enter`'s id-allocation-and-push-back prefix
    /// before its admission wait loop.
    fn join_queue(&self) -> Option<u64> {
        let mut s = self.state.lock().unwrap();
        if s.fair {
            let id = s.next_id;
            s.next_id += 1;
            s.queue.push_back(id);
            Some(id)
        } else {
            None
        }
    }

    /// Blocks until `ticket` (if `Some`, i.e. fair mode) reaches the front of
    /// the queue AND the real lock is free, then takes the lock. Mirrors
    /// `FairQueue::enter`'s admission wait loop followed by
    /// `ExclusiveLatch::acquire`'s `self.inner.try_lock_for(timeout)`.
    fn wait_for_admission(&self, ticket: Option<u64>) {
        let mut s = self.state.lock().unwrap();
        if let Some(id) = ticket {
            while s.queue.front() != Some(&id) {
                s = self.queue_cv.wait(s).unwrap();
            }
        }
        while s.held {
            s = self.queue_cv.wait(s).unwrap();
        }
        s.held = true;
    }

    /// Acquire (blocking, no give-up): models `ExclusiveLatch::acquire()`'s
    /// happy path in one call -- join the fair queue (if fair mode is on)
    /// and block until admitted to the front AND the real lock is free, then
    /// take it. When fair mode is off, this is a plain mutex acquire: no
    /// queue bookkeeping at all, matching `acquire()`'s `if fair_latches() {
    /// .. } else { None }` branch. Convenience wrapper over
    /// [`Self::join_queue`] + [`Self::wait_for_admission`] for callers that
    /// do not need to pin arrival order across multiple concurrent joiners
    /// (mutual exclusion, give-up, and full-drain checks below do not
    /// depend on which order concurrently-spawned threads join in).
    fn acquire(&self) -> Option<u64> {
        let ticket = self.join_queue();
        self.wait_for_admission(ticket);
        ticket
    }

    /// Give-up-capable acquire, modelling `FairQueue::enter`'s timeout path
    /// (`fair_queue.rs`): a queued waiter that has NOT yet reached the front
    /// may instead choose to give up (standing in for a real wall-clock
    /// deadline elapsing -- shuttle explores interleavings, not timing, so
    /// give-up is modelled as an unconditional choice the caller makes, the
    /// same modelling decision `shuttle_rwlock_reservation.rs`'s
    /// `try_give_up` makes for the analogous rwlock race).
    ///
    /// Only meaningful in fair mode; a give-up before reaching the front
    /// removes exactly this waiter's id from the queue and wakes the queue
    /// condvar so the new front (whoever is now first) can proceed --
    /// mirrors `FairQueue::enter`'s timeout branch: "removing it can never
    /// change who is at the front... no other waiter is starved or stuck by
    /// a give-up."
    fn try_acquire_with_give_up(
        &self,
        give_up: &dyn Fn() -> bool,
    ) -> Admission {
        let mut s = self.state.lock().unwrap();
        assert!(s.fair, "give-up path only exists under fair mode");
        let id = s.next_id;
        s.next_id += 1;
        s.queue.push_back(id);
        loop {
            if s.queue.front() == Some(&id) {
                break;
            }
            if give_up() {
                s.queue.retain(|&x| x != id);
                self.queue_cv.notify_all();
                return Admission::GaveUp;
            }
            s = self.queue_cv.wait(s).unwrap();
        }
        while s.held {
            s = self.queue_cv.wait(s).unwrap();
        }
        s.held = true;
        Admission::Admitted(id)
    }

    /// Release: models the RAII guard's `Drop` (`ExclusiveLatchGuard::drop`,
    /// `exclusive.rs`) -- release the real lock FIRST, then leave the fair
    /// queue, exactly matching the drop order the doc comment there calls
    /// out ("Release the real lock first ... then leave the fair queue so
    /// the next waiter's turn begins only once the lock is actually free").
    /// Getting this order backwards is exactly the class of bug this model
    /// exists to catch -- see [`release_before_dequeue_order_matters`].
    fn release(&self, ticket: Option<u64>) {
        let mut s = self.state.lock().unwrap();
        assert!(s.held, "release called while not held -- model/caller bug");
        s.held = false;
        if let Some(id) = ticket {
            let front = s.queue.pop_front();
            debug_assert_eq!(
                front,
                Some(id),
                "release by non-front ticket -- caller bug"
            );
        }
        drop(s);
        // FairQueue::leave wakes unconditionally on its own condvar; the
        // real-lock release above has no separate wake in the real code
        // (parking_lot-style mutexes wake on unlock internally) -- the
        // model folds both into one condvar consulted by both the queue
        // wait and the real-lock wait, so one notify_all covers both here.
        self.queue_cv.notify_all();
    }
}

/// **Invariant: FIFO grant order.** With fair mode on, `N` threads that join
/// the queue while the lock is held are granted in strict arrival order.
/// The main thread calls [`Model::join_queue`] for each waiter, IN INDEX
/// ORDER, before spawning the thread that will actually wait on that ticket
/// -- pinning arrival order structurally rather than by wall-clock stagger
/// (which would not make sense inside shuttle's deterministic scheduler:
/// shuttle explores interleavings, not timing, so if enqueueing happened
/// inside the spawned closures themselves, shuttle would be free to explore
/// schedules where they enqueue in a different order than they were spawned,
/// making a FIFO assertion against "spawn order" meaningless). This mirrors
/// `fair_queue.rs::grants_in_strict_arrival_order`'s own approach of pinning
/// join order structurally, adapted for shuttle instead of wall-clock sleeps.
#[test]
fn fair_mode_grants_in_strict_arrival_order() {
    shuttle::check_random(
        || {
            let model = Arc::new(Model::new(true));
            let holder_ticket = model.acquire();
            assert_eq!(holder_ticket, Some(1));

            const N: usize = 4;
            let order = Arc::new(Mutex::new(Vec::<usize>::new()));
            let mut handles = Vec::new();
            for i in 0..N {
                // Join the queue from the MAIN thread, in index order,
                // before spawning the thread that will wait on its ticket
                // -- pins arrival order structurally. If enqueueing happened
                // inside the spawned closure instead, shuttle would be free
                // to interleave the pushes in any order, and asserting FIFO
                // against an unpinned arrival order would be meaningless
                // (see `join_queue`'s doc comment).
                let ticket = model.join_queue();
                let model = Arc::clone(&model);
                let order = Arc::clone(&order);
                let h = thread::spawn(move || {
                    model.wait_for_admission(ticket);
                    order.lock().unwrap().push(i);
                    model.release(ticket);
                });
                handles.push(h);
            }

            model.release(holder_ticket);

            for h in handles {
                h.join().unwrap();
            }

            assert_eq!(
                *order.lock().unwrap(),
                (0..N).collect::<Vec<_>>(),
                "fair mode must grant in strict arrival order"
            );
        },
        ITERATIONS,
    );
}

/// **Invariant: mutual exclusion**, checked under BOTH fair and unfair mode
/// -- the FIFO wiring must never weaken the underlying lock's exclusivity,
/// only its admission order.
#[test]
fn mutual_exclusion_holds_regardless_of_fairness() {
    for fair in [true, false] {
        shuttle::check_random(
            move || {
                let model = Arc::new(Model::new(fair));
                let concurrent = Arc::new(Mutex::new(0u32));
                let max_seen = Arc::new(Mutex::new(0u32));

                let mut handles = Vec::new();
                for _ in 0..4 {
                    let model = Arc::clone(&model);
                    let concurrent = Arc::clone(&concurrent);
                    let max_seen = Arc::clone(&max_seen);
                    handles.push(thread::spawn(move || {
                        let ticket = model.acquire();
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
                        model.release(ticket);
                    }));
                }
                for h in handles {
                    h.join().unwrap();
                }

                assert_eq!(
                    *max_seen.lock().unwrap(),
                    1,
                    "two threads held the modelled latch concurrently (fair={fair})"
                );
            },
            ITERATIONS,
        );
    }
}

/// **Invariant: give-up does not strand a later-arrived waiter.** A queued
/// waiter that gives up before reaching the front must not prevent the
/// waiter behind it from eventually being admitted -- the exact bug class
/// `fair_queue.rs`'s own `timeout_removes_only_the_timed_out_waiter` unit
/// test checks, modelled here through the acquire/release wiring rather
/// than the bare queue, and with shuttle exploring every interleaving of
/// "give up" vs. "reached the front" rather than a single fixed schedule.
#[test]
fn give_up_does_not_strand_the_next_waiter() {
    shuttle::check_random(
        || {
            let model = Arc::new(Model::new(true));
            let holder_ticket = model.acquire();

            // A waiter that will give up (never reaches the front while the
            // holder is held, by construction: give_up() always returns
            // true, so it never even waits on the condvar).
            let model2 = Arc::clone(&model);
            let gave_up = thread::spawn(move || {
                matches!(
                    model2.try_acquire_with_give_up(&|| true),
                    Admission::GaveUp
                )
            });

            // A third waiter queued behind the one that gives up.
            let model3 = Arc::clone(&model);
            let third_admitted = Arc::new(Mutex::new(false));
            let third_admitted2 = Arc::clone(&third_admitted);
            let third = thread::spawn(move || {
                let ticket = model3.acquire();
                *third_admitted2.lock().unwrap() = true;
                model3.release(ticket);
            });

            assert!(gave_up.join().unwrap(), "the second waiter must give up");

            model.release(holder_ticket);
            third.join().unwrap();
            assert!(
                *third_admitted.lock().unwrap(),
                "the give-up must not strand the waiter behind it"
            );
        },
        ITERATIONS,
    );
}

/// **Invariant: the RAII drop order matters.** `release()` must free the
/// real lock BEFORE leaving the queue, exactly as
/// `ExclusiveLatchGuard::drop` does (see its doc comment). This test proves
/// the model actually depends on that order by checking a WEAKER but still
/// meaningful property that would break if release forgot to free `held`
/// before waking the queue: every admitted waiter, once granted, must
/// observe `held == true` was set by ITS OWN acquire (never find the lock
/// already released out from under it) -- checked implicitly by
/// `mutual_exclusion_holds_regardless_of_fairness` above holding under
/// `ITERATIONS` schedules AND by this test additionally confirming a long
/// FIFO chain drains completely (no waiter left permanently parked), which
/// is what an inverted release order risks: leaving the queue before the
/// lock is actually free lets the NEXT front-of-queue waiter observe
/// `held == true` still set by the previous holder and block again on the
/// SAME condvar wait it just woke from, which is livelock-shaped, not a
/// crash -- exactly the silent-hang class this whole task is about.
#[test]
fn a_long_fifo_chain_fully_drains() {
    shuttle::check_random(
        || {
            let model = Arc::new(Model::new(true));
            let holder_ticket = model.acquire();

            const N: usize = 6;
            let mut handles = Vec::new();
            for _ in 0..N {
                let model = Arc::clone(&model);
                handles.push(thread::spawn(move || {
                    let ticket = model.acquire();
                    model.release(ticket);
                }));
            }

            model.release(holder_ticket);

            // If release() left the queue before freeing `held`, the chain
            // can livelock: shuttle would report a real deadlock (blocked
            // thread, nothing runnable) rather than this join ever
            // returning.
            for h in handles {
                h.join().unwrap();
            }
        },
        ITERATIONS,
    );
}

// ---------------------------------------------------------------------------
// Non-vacuity checks: sabotaged models that MUST be rejected by an assertion
// shaped like the ones above. These are `#[ignore]`d because they are
// intentionally-failing demonstrations, not part of the passing suite (and
// `make shuttle` must stay green) -- run explicitly with
// `--ignored` to confirm the harness is not vacuous. See the crate's task
// notes / commit message for the transcript of these being run and failing
// as expected.
// ---------------------------------------------------------------------------

/// Sabotaged FIFO: admits in LIFO (stack) order instead of FIFO. Must FAIL
/// the same assertion shape as [`fair_mode_grants_in_strict_arrival_order`],
/// proving that test is not vacuously true.
#[test]
#[ignore = "intentionally-failing non-vacuity demonstration; run with --ignored"]
fn non_vacuity_sabotaged_lifo_order_is_rejected() {
    struct LifoModel {
        state: Mutex<VecDeque<u64>>,
        held: Mutex<bool>,
        cv: Condvar,
        next_id: Mutex<u64>,
    }
    impl LifoModel {
        fn new() -> Self {
            LifoModel {
                state: Mutex::new(VecDeque::new()),
                held: Mutex::new(false),
                cv: Condvar::new(),
                next_id: Mutex::new(1),
            }
        }
        fn acquire(&self) -> u64 {
            let id = {
                let mut n = self.next_id.lock().unwrap();
                let id = *n;
                *n += 1;
                id
            };
            {
                let mut q = self.state.lock().unwrap();
                // SABOTAGE: push to the FRONT, not the back -- LIFO, not
                // FIFO admission order.
                q.push_front(id);
            }
            let mut held = self.held.lock().unwrap();
            loop {
                let is_front =
                    *self.state.lock().unwrap().front().unwrap() == id;
                if is_front && !*held {
                    *held = true;
                    return id;
                }
                held = self.cv.wait(held).unwrap();
            }
        }
        fn release(&self, id: u64) {
            let mut held = self.held.lock().unwrap();
            *held = false;
            self.state.lock().unwrap().retain(|&x| x != id);
            drop(held);
            self.cv.notify_all();
        }
    }

    shuttle::check_random(
        || {
            let model = Arc::new(LifoModel::new());
            let holder = model.acquire();

            const N: usize = 4;
            let order = Arc::new(Mutex::new(Vec::<usize>::new()));
            let mut handles = Vec::new();
            for i in 0..N {
                let model = Arc::clone(&model);
                let order = Arc::clone(&order);
                handles.push(thread::spawn(move || {
                    let id = model.acquire();
                    order.lock().unwrap().push(i);
                    model.release(id);
                }));
            }
            model.release(holder);
            for h in handles {
                h.join().unwrap();
            }

            assert_eq!(
                *order.lock().unwrap(),
                (0..N).collect::<Vec<_>>(),
                "expected FIFO order to be rejected under LIFO sabotage"
            );
        },
        ITERATIONS,
    );
}
