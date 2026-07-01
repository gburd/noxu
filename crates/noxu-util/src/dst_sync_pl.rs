// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! `parking_lot`-shaped concurrency seam over shuttle (DST Milestone 1.1).
//!
//! # Why this exists (the M2 blocker this removes)
//!
//! The sibling [`crate::dst_sync`] module swaps `std::sync` ⇄ `shuttle::sync`,
//! but *only* modules that already use `std::sync` (the `FsyncManager` and
//! `DaemonManager`) could route through it — because `shuttle::sync` mirrors
//! the **`std::sync`** API shape (`lock()` returns a `LockResult`, `Condvar`
//! takes an *owned* guard).  The engine's `noxu-sync` crate is
//! **`parking_lot`-shaped** instead (`lock()` returns the guard directly,
//! `Condvar::wait_for(&mut guard, dur)` borrows the guard), so
//! `noxu-sync`-based modules — most importantly the `lock_manager` deadlock
//! path the next DST wave targets — could **not** be shuttle-swapped.
//!
//! This module is that missing bridge:
//!
//!   * **Default build (`#[cfg(not(noxu_shuttle))]`)** — a transparent
//!     re-export of the real [`noxu_sync`] types.  **Zero production change**:
//!     the compiler sees the identical `noxu-sync` futex primitives it always
//!     did, and `shuttle` is absent from the dependency graph.
//!   * **DST build (`#[cfg(noxu_shuttle)]`)** — thin wrappers over
//!     `shuttle::sync` that *present the `parking_lot` shape* (`lock()` unwraps
//!     the `LockResult`; `wait_for(&mut guard, dur)` adapts to shuttle by
//!     swapping the guard in place).  A `noxu-sync`-based module can then
//!     import its `Mutex`/`RwLock`/`Condvar` from here and become schedulable
//!     by shuttle without touching its call sites.
//!
//! # The timed-wait crux (coordinated with the injectable [`Clock`])
//!
//! shuttle 0.9's `Condvar::wait_timeout` **never times out** — it blocks until
//! an explicit `notify`, ignoring the duration entirely (see the `TODO support
//! the timeout case` in `shuttle-0.9.1/src/sync/condvar.rs`).  A protocol whose
//! *liveness* depends on a condvar timeout (like `FsyncManager`'s
//! `LOG_FSYNC_TIMEOUT` lost-wakeup recovery) therefore deadlocks under shuttle
//! if the wrapper silently drops the timeout.
//!
//! Rather than fake it, this wrapper makes a timed wait return **deterministic
//! under the harness's control**, coupled to a [`SimClock`]:
//!
//!   1. [`Condvar::wait_for`] under shuttle registers the waiter's *deadline*
//!      (`SimClock` tick + `dur`) in a per-execution timer registry, then does
//!      an untimed shuttle `wait`.
//!   2. The harness advances simulated time and fires due timers with
//!      [`advance_and_fire`], which notifies every condvar whose deadline has
//!      elapsed.  The woken waiter re-reads the `SimClock`, sees
//!      `now >= deadline`, and returns `timed_out() == true`.
//!
//! So a timed wait *does* work under shuttle+SimClock: it fires **exactly
//! when the harness advances the clock past the deadline**, deterministically,
//! as one more schedulable event.  A protocol like `FsyncManager` whose orphan
//! recovery is the timeout can then be driven to completion by the test
//! advancing the clock — which is precisely what the next wave's oracle needs.
//!
//!   * If a protocol's liveness does **not** depend on the timeout (a
//!     notify-driven wakeup like the `DaemonManager` shutdown), the harness
//!     simply never advances past the deadline and the real notify wakes the
//!     waiter — the timeout is inert, exactly as intended.
//!
//! Under the default (non-shuttle) build, `wait_for` is `noxu-sync`'s real
//! futex-timed wait and [`advance_and_fire`] is not present (it is shuttle-only
//! test scaffolding).

// ── Production / default: transparent noxu-sync re-export ───────────────────
#[cfg(not(noxu_shuttle))]
mod imp {
    pub use noxu_sync::{
        Condvar, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard,
        WaitTimeoutResult,
    };
}

// ── DST: parking_lot-shaped wrappers over shuttle::sync ─────────────────────
#[cfg(noxu_shuttle)]
mod imp {
    use crate::SimClock;
    use crate::clock::Clock;
    use std::cell::RefCell;
    use std::ops::{Deref, DerefMut};
    use std::time::Duration;

    use shuttle::sync as sh;

    /// Result of a timed condvar wait — mirrors `noxu_sync::WaitTimeoutResult`
    /// (and `parking_lot`'s): `.timed_out()` distinguishes a fired timeout
    /// from a notify.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct WaitTimeoutResult(bool);

    impl WaitTimeoutResult {
        /// `true` if the wait returned because its deadline elapsed (fired by
        /// [`super::advance_and_fire`]), `false` if it was notified.
        #[inline]
        pub fn timed_out(self) -> bool {
            self.0
        }
    }

    /// A `parking_lot`-shaped `Mutex` over `shuttle::sync::Mutex`.
    ///
    /// `lock()` returns the guard directly (unwrapping shuttle's `LockResult`);
    /// shuttle never poisons in the paths we test, so the unwrap is a
    /// scheduling point, not a fallibility the caller must handle.
    #[derive(Debug, Default)]
    pub struct Mutex<T>(sh::Mutex<T>);

    /// Guard type presenting the `parking_lot` shape (`Deref`/`DerefMut` to
    /// `T`, no `LockResult`).  Internally holds an `Option<sh::MutexGuard>` so
    /// the wrapper [`Condvar`] can move the inner guard through shuttle's
    /// by-value `wait` and put the re-acquired one back — all in safe code
    /// (no `unsafe`, keeping `noxu-util`'s `#![forbid(unsafe_code)]` intact).
    pub struct MutexGuard<'a, T>(Option<sh::MutexGuard<'a, T>>);

    impl<T> Deref for MutexGuard<'_, T> {
        type Target = T;
        #[inline]
        fn deref(&self) -> &T {
            self.0.as_ref().expect("guard present")
        }
    }

    impl<T> DerefMut for MutexGuard<'_, T> {
        #[inline]
        fn deref_mut(&mut self) -> &mut T {
            self.0.as_mut().expect("guard present")
        }
    }

    impl<T> Mutex<T> {
        #[inline]
        pub fn new(val: T) -> Self {
            Mutex(sh::Mutex::new(val))
        }

        /// Acquire the lock, blocking until available (parking_lot shape:
        /// returns the guard, not a `LockResult`).
        #[inline]
        pub fn lock(&self) -> MutexGuard<'_, T> {
            MutexGuard(Some(
                self.0.lock().expect("shuttle mutex poisoned (unexpected)"),
            ))
        }

        /// Non-blocking acquire; `None` if held (parking_lot shape).
        #[inline]
        pub fn try_lock(&self) -> Option<MutexGuard<'_, T>> {
            self.0.try_lock().ok().map(|g| MutexGuard(Some(g)))
        }
    }

    /// A `parking_lot`-shaped `RwLock` over `shuttle::sync::RwLock`.
    #[derive(Debug, Default)]
    pub struct RwLock<T>(sh::RwLock<T>);

    pub type RwLockReadGuard<'a, T> = sh::RwLockReadGuard<'a, T>;
    pub type RwLockWriteGuard<'a, T> = sh::RwLockWriteGuard<'a, T>;

    impl<T> RwLock<T> {
        #[inline]
        pub fn new(val: T) -> Self {
            RwLock(sh::RwLock::new(val))
        }

        #[inline]
        pub fn read(&self) -> RwLockReadGuard<'_, T> {
            self.0.read().expect("shuttle rwlock poisoned (unexpected)")
        }

        #[inline]
        pub fn write(&self) -> RwLockWriteGuard<'_, T> {
            self.0.write().expect("shuttle rwlock poisoned (unexpected)")
        }

        #[inline]
        pub fn try_read(&self) -> Option<RwLockReadGuard<'_, T>> {
            self.0.try_read().ok()
        }

        #[inline]
        pub fn try_write(&self) -> Option<RwLockWriteGuard<'_, T>> {
            self.0.try_write().ok()
        }
    }

    // ── Timer registry (per shuttle execution) ──────────────────────────────
    //
    // shuttle runs one closure per interleaving on cooperative green threads
    // within a single OS thread, so a thread-local list is per-execution state.
    // Each `wait_for` pushes its (deadline_ns, condvar-id) so the harness's
    // `advance_and_fire` can wake exactly the waiters whose deadline elapsed.
    // Over-notifying is safe: a spuriously-woken waiter re-checks its own
    // predicate and deadline and blocks again if neither is satisfied.

    thread_local! {
        static TIMERS: RefCell<Vec<Timer>> = const { RefCell::new(Vec::new()) };
        static NEXT_CONDVAR_ID: RefCell<u64> = const { RefCell::new(0) };
        // Registry of live wrapper condvars keyed by id → shared shuttle
        // condvar, so `advance_and_fire` can notify by id.  `sh::Condvar` is
        // wrapped in `Arc` so we hold a strong reference in the registry
        // (fully safe — no raw pointers, so `#![forbid(unsafe_code)]` holds).
        static CONDVARS: RefCell<Vec<(u64, std::sync::Arc<sh::Condvar>)>> =
            const { RefCell::new(Vec::new()) };
        // Per-condvar set of ids whose deadline has fired (level-triggered).
        // `advance_and_fire` inserts here BEFORE notifying; `wait_for` checks
        // (and consumes) this BEFORE blocking, closing the notify-before-wait
        // race that would otherwise lose the timeout wakeup (the same
        // lost-wakeup class the DaemonManager WakeHandle pre-check fixes).
        //
        // Reads/writes here are plain RefCell access (not shuttle-instrumented
        // visible operations), so there is no scheduling point between
        // `wait_for`'s check and its subsequent `wait()` — shuttle cannot
        // interleave a fire into that gap.
        static FIRED: RefCell<Vec<u64>> = const { RefCell::new(Vec::new()) };
    }

    struct Timer {
        deadline_ns: u64,
        condvar_id: u64,
    }

    fn next_condvar_id() -> u64 {
        NEXT_CONDVAR_ID.with(|c| {
            let mut c = c.borrow_mut();
            let id = *c;
            *c += 1;
            id
        })
    }

    /// A `parking_lot`-shaped `Condvar` over `shuttle::sync::Condvar`.
    ///
    /// `wait`/`wait_for` borrow the guard (`&mut MutexGuard`) rather than
    /// taking it by value the way `shuttle::sync::Condvar` does; the wrapper
    /// bridges the two by moving the inner `Option`-held guard out and back —
    /// entirely in safe code.
    pub struct Condvar {
        inner: std::sync::Arc<sh::Condvar>,
        id: u64,
    }

    impl Condvar {
        pub fn new() -> Self {
            let id = next_condvar_id();
            let inner = std::sync::Arc::new(sh::Condvar::new());
            CONDVARS.with(|c| {
                c.borrow_mut().push((id, std::sync::Arc::clone(&inner)))
            });
            Condvar { inner, id }
        }

        /// Untimed wait (parking_lot shape: borrows the guard).
        pub fn wait<T>(&self, guard: &mut MutexGuard<'_, T>) {
            let owned = guard.0.take().expect("guard present for wait");
            let woken = self
                .inner
                .wait(owned)
                .expect("shuttle condvar poisoned (unexpected)");
            guard.0 = Some(woken);
        }

        /// Timed wait (parking_lot shape).  Under shuttle the duration cannot
        /// self-expire; the deadline is registered and fires only when the
        /// harness calls [`super::advance_and_fire`] past it.  Returns
        /// `timed_out() == true` iff, on wake, the [`SimClock`] has reached the
        /// deadline.
        pub fn wait_for<T>(
            &self,
            guard: &mut MutexGuard<'_, T>,
            timeout: Duration,
        ) -> WaitTimeoutResult {
            let clock = current_sim_clock();
            let deadline_ns =
                clock.now_nanos().saturating_add(timeout.as_nanos() as u64);
            let cid = self.id;
            TIMERS.with(|t| {
                t.borrow_mut().push(Timer { deadline_ns, condvar_id: cid })
            });

            // Level-triggered timeout check BEFORE blocking: if the harness has
            // already advanced the clock past our deadline (and fired our
            // timer), return timed_out without ever calling `wait`.  A fire that
            // lands in the gap between this check and shuttle's internal
            // block-registration is recovered by the harness re-notifying
            // pending fires on its next `advance_and_fire` (see that fn); the
            // waiter is then woken and re-checks below.  RefCell access is not a
            // shuttle scheduling point.
            let already_fired = FIRED.with(|f| {
                let mut f = f.borrow_mut();
                if let Some(pos) = f.iter().position(|id| *id == cid) {
                    f.remove(pos);
                    true
                } else {
                    false
                }
            });
            if already_fired || clock.now_nanos() >= deadline_ns {
                return WaitTimeoutResult(true);
            }

            let owned = guard.0.take().expect("guard present for wait_for");
            let woken = self
                .inner
                .wait(owned)
                .expect("shuttle condvar poisoned (unexpected)");
            guard.0 = Some(woken);

            // Consume our fired flag if the timer woke us.
            FIRED.with(|f| {
                let mut f = f.borrow_mut();
                if let Some(pos) = f.iter().position(|id| *id == cid) {
                    f.remove(pos);
                }
            });
            let timed_out = current_sim_clock().now_nanos() >= deadline_ns;
            WaitTimeoutResult(timed_out)
        }

        pub fn notify_one(&self) {
            self.inner.notify_one();
        }

        pub fn notify_all(&self) {
            self.inner.notify_all();
        }
    }

    impl Default for Condvar {
        fn default() -> Self {
            Self::new()
        }
    }

    impl std::fmt::Debug for Condvar {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("Condvar").field("id", &self.id).finish()
        }
    }

    // ── SimClock coupling ────────────────────────────────────────────────────
    //
    // The harness installs the SimClock (as an Arc) for the current execution
    // so `wait_for` can measure deadlines without threading the clock through
    // every call.  Arc, not a raw pointer, so no `unsafe`.

    thread_local! {
        static SIM_CLOCK: RefCell<Option<std::sync::Arc<SimClock>>> =
            const { RefCell::new(None) };
    }

    /// Install the [`SimClock`] the wrapper measures deadlines against for the
    /// current shuttle execution.  Call once at the top of the test closure.
    ///
    /// Also resets the per-execution timer and condvar registries so state
    /// from a previous interleaving (shuttle reuses the OS thread across
    /// iterations, so the thread-locals persist) cannot leak in.
    pub fn install_sim_clock(clock: std::sync::Arc<SimClock>) {
        SIM_CLOCK.with(|c| *c.borrow_mut() = Some(clock));
        TIMERS.with(|t| t.borrow_mut().clear());
        CONDVARS.with(|c| c.borrow_mut().clear());
        FIRED.with(|f| f.borrow_mut().clear());
        NEXT_CONDVAR_ID.with(|c| *c.borrow_mut() = 0);
    }

    fn current_sim_clock() -> std::sync::Arc<SimClock> {
        SIM_CLOCK.with(|c| {
            c.borrow()
                .clone()
                .expect("install_sim_clock() must be called before wait_for")
        })
    }

    /// Advance the [`SimClock`] by `dur` and fire (notify) every wrapper
    /// condvar whose registered deadline has now elapsed.
    ///
    /// This is the harness's lever for the timed-wait crux: a waiter blocked in
    /// [`Condvar::wait_for`] returns `timed_out() == true` exactly when the
    /// harness advances simulated time past its deadline.  Over-notifying is
    /// safe (waiters re-check their predicate).  Returns the number of timers
    /// fired.
    pub fn advance_and_fire(clock: &SimClock, dur: Duration) -> usize {
        clock.advance(dur);
        let now = clock.now_nanos();
        // Collect the due condvar ids (removing their timers) while holding the
        // borrow, then mark them fired.
        let newly_due: Vec<u64> = TIMERS.with(|t| {
            let mut timers = t.borrow_mut();
            let mut fired = Vec::new();
            timers.retain(|timer| {
                if timer.deadline_ns <= now {
                    fired.push(timer.condvar_id);
                    false
                } else {
                    true
                }
            });
            fired
        });
        // Mark each newly-due condvar as fired BEFORE notifying, so a waiter
        // that has not yet blocked sees the flag on its pre-`wait` check
        // (level-triggered) and a waiter that is already blocked is woken by
        // notify.
        FIRED.with(|f| {
            let mut f = f.borrow_mut();
            for id in &newly_due {
                if !f.contains(id) {
                    f.push(*id);
                }
            }
        });
        // Notify every condvar that has a PENDING fired flag — not just the
        // newly-due ones.  A waiter whose fire landed in the pre-block gap on a
        // previous call (its notify was a no-op because it was not yet blocked)
        // is re-notified here, so a harness that loops `advance_and_fire` until
        // the system quiesces cannot lose the timeout wakeup.
        let pending: Vec<u64> = FIRED.with(|f| f.borrow().clone());
        for id in &pending {
            let cv = CONDVARS.with(|c| {
                c.borrow()
                    .iter()
                    .find(|(cid, _)| cid == id)
                    .map(|(_, cv)| std::sync::Arc::clone(cv))
            });
            if let Some(cv) = cv {
                cv.notify_all();
            }
        }
        newly_due.len()
    }
}

pub use imp::*;
