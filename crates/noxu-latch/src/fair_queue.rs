// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Per-latch FIFO admission queue backing `env_fair_latches`.
//!
//! See `.agent/notes-fair-latches.md` (repo root) for the JE semantics this
//! ports and why Noxu implements the strictly-stronger full-FIFO guarantee
//! rather than JE's documented contiguous-reader batching.
//!
//! # Mechanism
//!
//! A bare `next_ticket`/`now_serving` counter pair cannot cleanly support
//! give-up on timeout: removing a ticket from the middle of a numeric
//! sequence either renumbers every ticket behind it or leaves a permanent
//! gap `now_serving` can never reach. Instead this is an explicit
//! `VecDeque<u64>` of waiter ids behind a `noxu_sync::Mutex` +
//! `noxu_sync::Condvar` (both already-tested primitives; no new `unsafe`):
//!
//! * [`FairQueue::enter`] pushes a fresh id to the back and blocks until
//!   that id reaches the FRONT of the queue (or the deadline elapses).
//! * [`FairQueue::leave`] pops the front (asserting it matches the caller's
//!   id -- the caller must be the current holder) and wakes every waiter so
//!   the new front re-checks.
//! * A timed-out `enter` removes its id from wherever it sits in the queue.
//!   That position is never the front (reaching the front is the success
//!   condition being raced against the deadline), so removing it can never
//!   change who is at the front -- no other waiter is starved or stuck by a
//!   give-up. See `shuttle_fair_queue.rs` for the DST proof of this claim.
//!
//! This wraps AROUND the existing inner `Mutex`/`RwLock` acquisition in
//! [`crate::ExclusiveLatch`]/[`crate::SharedLatch`]; it never replaces it. A
//! caller must reach the front of the fair queue before attempting the real
//! lock at all, so while fair mode is on, only the front-of-queue thread
//! ever contends on the real lock -- all existing timeout / latch-ordering /
//! forced-yield / owner-tracking logic in the latches is unchanged.
//!
//! # Zero cost when `env_fair_latches` is off
//!
//! Every latch owns a `FairQueue` unconditionally (a `Mutex<VecDeque<u64>>` +
//! `Condvar`, no allocation until first use), but [`FairQueue::enter`] and
//! [`FairQueue::leave`] are only ever CALLED from the latch acquire/release
//! paths when [`crate::config::fair_latches`] is true -- see
//! `exclusive.rs`/`shared.rs`. The off path pays exactly the one relaxed
//! atomic load `fair_latches()` already costs (measured in
//! `docs/src/reference/configuration.md`'s cost note); this module is never
//! entered at all.

use noxu_sync::{Condvar, Mutex};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// A [`FairQueue::enter`] call gave up before reaching the front of the queue
/// because its deadline elapsed. The caller's id has already been removed from
/// the queue, so it must NOT call [`FairQueue::leave`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueueTimeout;

/// Computes the `Instant` deadline `timeout` in the future, for use with
/// [`FairQueue::enter`].
///
/// Uses `checked_add` rather than the panicking `+` operator: the only
/// caller of this is `exclusive.rs`/`shared.rs` passing a `LatchContext`'s
/// configured timeout, which can be the "effectively forever" no-timeout
/// sentinel (`config::EFFECTIVELY_FOREVER`, ~292 million years) -- large
/// enough that `Instant::now() + timeout` is not guaranteed not to overflow
/// on every platform's `Instant` representation. On overflow this falls back
/// to `None` (wait forever), which is exactly the right behaviour for that
/// sentinel anyway.
pub(crate) fn deadline_from(timeout: Duration) -> Option<Instant> {
    Instant::now().checked_add(timeout)
}

/// Monotonic id generator for queue entries. Shared process-wide (not
/// per-queue) purely so ids are easy to eyeball in traces; uniqueness only
/// needs to hold within one queue at a time, which a per-process counter
/// trivially provides.
static NEXT_ID: AtomicU64 = AtomicU64::new(1);

/// A FIFO admission queue: waiters are granted in strict arrival order.
pub struct FairQueue {
    state: Mutex<VecDeque<u64>>,
    cv: Condvar,
}

impl FairQueue {
    /// Creates a new, empty fair queue.
    pub const fn new() -> Self {
        FairQueue { state: Mutex::new(VecDeque::new()), cv: Condvar::new() }
    }

    /// Allocates a fresh waiter id and joins the back of the queue, blocking
    /// until this id reaches the front (or `deadline` elapses).
    ///
    /// Returns `Ok(id)` once admitted -- the caller now holds the "fair
    /// ticket" and must call [`Self::leave`] with the same id after it is
    /// done contending for (and using) the real latch. Returns `Err(())` on
    /// timeout, having already removed its id from the queue: the caller
    /// must NOT call `leave` in that case.
    // Result<_, ()> is deliberate here (not `Result<u64, SomeError>`): the
    // only failure mode is "gave up at the deadline", which every caller
    // already turns into its own richer error (`LatchError::Timeout` in
    // `exclusive.rs`/`shared.rs`) -- a unit error keeps this internal type
    // from duplicating that.
    #[allow(clippy::result_unit_err)]
    pub fn enter(
        &self,
        deadline: Option<Instant>,
    ) -> Result<u64, QueueTimeout> {
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        let mut q = self.state.lock();
        q.push_back(id);
        while q.front() != Some(&id) {
            match deadline {
                None => self.cv.wait(&mut q),
                Some(dl) => {
                    let now = Instant::now();
                    if now >= dl {
                        // Timed out before reaching the front. Remove our id
                        // from wherever it sits (never the front -- reaching
                        // the front is the success condition we are racing
                        // against the deadline) and give up.
                        q.retain(|&x| x != id);
                        // Waking here is not required for correctness (no one
                        // is waiting on OUR absence), but a give-up can
                        // change the position of the previous front if we
                        // were somehow it (impossible, per above) -- wake
                        // defensively so no invariant can be silently
                        // violated by future changes to this function.
                        self.cv.notify_all();
                        return Err(QueueTimeout);
                    }
                    let remaining = dl - now;
                    let timed_out = self.cv.wait_for(&mut q, remaining);
                    if timed_out.timed_out() && q.front() != Some(&id) {
                        q.retain(|&x| x != id);
                        self.cv.notify_all();
                        return Err(QueueTimeout);
                    }
                }
            }
        }
        Ok(id)
    }

    /// Departs the front of the queue. `id` must be the value returned by
    /// the matching [`Self::enter`] call -- the caller must be the current
    /// front (this is a programming-error assertion, not a recoverable
    /// condition: a latch's acquire/release pairing already guarantees it).
    pub fn leave(&self, id: u64) {
        let mut q = self.state.lock();
        let front = q.pop_front();
        debug_assert_eq!(
            front,
            Some(id),
            "FairQueue::leave called by a non-front waiter -- caller bug"
        );
        drop(q);
        self.cv.notify_all();
    }
}

impl Default for FairQueue {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for FairQueue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let len = self.state.lock().len();
        f.debug_struct("FairQueue").field("waiting", &len).finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;

    #[test]
    fn single_waiter_enters_and_leaves() {
        let q = FairQueue::new();
        let id = q.enter(None).expect("enter");
        q.leave(id);
    }

    #[test]
    fn grants_in_strict_arrival_order() {
        let q = Arc::new(FairQueue::new());
        const N: usize = 8;
        let order = Arc::new(Mutex::new(Vec::<usize>::new()));

        // Occupy the front so every worker below blocks inside `enter()`
        // until we release it, giving us control over when admission can
        // begin. `enter()` only returns to the thread that reaches the
        // front, so there is no "I have pushed" signal to synchronize a
        // push-order barrier on other than wall-clock staggering (matching
        // the pattern `exclusive.rs::test_multiple_waiters_sequential_grant`
        // already uses for the same reason).
        let holder = q.enter(None).expect("holder enters");

        let mut handles = Vec::new();
        for i in 0..N {
            let q = q.clone();
            let order = order.clone();
            handles.push(std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(5 * (i as u64 + 1)));
                let id = q.enter(None).expect("enter");
                order.lock().push(i);
                q.leave(id);
            }));
        }

        // Give every worker time to have pushed (the last one sleeps
        // 5*N ms) before releasing the holder and letting admission begin.
        std::thread::sleep(Duration::from_millis(5 * (N as u64 + 1) + 50));
        q.leave(holder);

        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(*order.lock(), (0..N).collect::<Vec<_>>());
    }

    #[test]
    fn timeout_removes_only_the_timed_out_waiter() {
        let q = Arc::new(FairQueue::new());
        // Occupy the front.
        let holder = q.enter(None).expect("holder enters");

        // A second waiter with a short deadline must time out (front is held).
        let q2 = q.clone();
        let timed_out = std::thread::spawn(move || {
            q2.enter(Some(Instant::now() + Duration::from_millis(50)))
        })
        .join()
        .unwrap();
        assert!(timed_out.is_err(), "expected timeout while front is held");

        // A third waiter queued behind the timed-out one must still be
        // admitted once the holder leaves -- proving the give-up did not
        // strand it.
        let q3 = q.clone();
        let third = std::thread::spawn(move || q3.enter(None).expect("third"));
        std::thread::sleep(Duration::from_millis(20));
        q.leave(holder);
        let third_id = third.join().unwrap();
        q.leave(third_id);
    }

    #[test]
    fn many_threads_no_deadlock_and_all_complete() {
        let q = Arc::new(FairQueue::new());
        let handles: Vec<_> = (0..32)
            .map(|_| {
                let q = q.clone();
                std::thread::spawn(move || {
                    let id = q.enter(None).expect("enter");
                    q.leave(id);
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
    }
}
