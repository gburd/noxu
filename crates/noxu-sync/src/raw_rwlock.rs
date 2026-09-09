//! Futex-based raw reader-writer lock implementing `lock_api::RawRwLock`.
//!
//! State encoding (single `AtomicU32`):
//!   bits 0-29: reader count  (ONE_READER = 1, max ~1 billion concurrent readers)
//!   bit  30:   WRITE_LOCKED  (exclusive writer holds the lock)
//!   bit  31:   WRITE_WAITING (a writer is queued; new readers must yield)
//!
//! Writer-preferring design: while `WRITE_WAITING` is set, new readers are
//! refused admission so a queued writer cannot be starved. Readers that already
//! hold the lock are unaffected — the bit gates *admission*, not existing
//! holders. This costs read throughput at low-to-moderate thread counts
//! relative to letting readers barge, and buys a bounded write latency; without
//! it a sustained reader stream starved writers indefinitely (measured at one
//! write per three seconds above three readers). See
//! `docs/src/internal/noxu-sync-vs-parking-lot-2026-09.md`.
//!
//! Writers are preferred over readers but are NOT ordered among themselves:
//! this is writer preference, not FIFO fairness.
//!
//! Additional fields:
//!   `read_waiters`  — readers blocked waiting for a write to finish
//!   `write_waiters` — writers blocked waiting for all readers/writers to finish
//!   `exclusive_owner` — thread ID hash of the write-lock owner

use crate::futex::{futex_wait, futex_wake};
use lock_api;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

/// State bit representing an exclusive (write) lock.
pub(crate) const WRITE_LOCKED: u32 = 1 << 30;
/// State bit indicating at least one writer is blocked waiting to acquire.
///
/// New readers refuse to enter while this is set, which is what bounds writer
/// starvation: a reader stream can no longer hold `state` above zero forever.
/// A reader that already holds the lock is unaffected -- this gates *admission*,
/// not existing holders -- so it cannot deadlock a reader that is mid-critical
/// section, and re-entrant read acquisition is not supported by this lock in
/// the first place.
const WRITE_WAITING: u32 = 1 << 31;
/// Each reader increments the state by this amount.
const ONE_READER: u32 = 1;
/// Mask for extracting the reader count (bits 0-29).
const READERS_MASK: u32 = WRITE_LOCKED - 1;

/// Futex-based raw reader-writer lock.
///
/// Implements `lock_api::RawRwLock` and `lock_api::RawRwLockTimed`.
pub struct NoxuRawRwLock {
    /// Combined state: reader count (bits 0–29) | WRITE_LOCKED (bit 30).
    pub(crate) state: AtomicU32,
    /// Number of reader threads sleeping in futex_wait.
    read_waiters: AtomicUsize,
    /// Number of writer threads sleeping in futex_wait.
    write_waiters: AtomicUsize,
    /// Thread ID hash of the exclusive owner (0 if not write-locked).
    pub(crate) exclusive_owner: AtomicU64,
}

// SAFETY: `lock_api::RawRwLock` requires that this type actually provide
// mutual exclusion between exclusive holders, and shared-but-not-exclusive
// access between shared holders, so that `lock_api` may hand out `&mut T` to an
// exclusive holder and `&T` to shared holders.
//
// That holds here: `state` packs a reader count (low bits, `READERS_MASK`) with
// a `WRITE_LOCKED` bit (1 << 30). `lock_exclusive` only succeeds by CASing
// `state` from 0 to `WRITE_LOCKED`, so an exclusive holder excludes every reader
// and every other writer; `lock_shared` only succeeds while `WRITE_LOCKED` is
// clear, so readers never coexist with a writer. All transitions are
// compare-exchange on a single atomic word with Acquire on acquisition and
// Release on release, giving the happens-before edges `lock_api` relies on.
//
// Liveness (not a soundness property, but worth stating next to the contract):
// the lock is writer-preferring via `WRITE_WAITING`, so a reader stream cannot
// starve a queued writer. Writers are not ordered among themselves. Neither
// property affects the exclusion guarantees this `unsafe impl` asserts.
unsafe impl lock_api::RawRwLock for NoxuRawRwLock {
    const INIT: Self = NoxuRawRwLock {
        state: AtomicU32::new(0),
        read_waiters: AtomicUsize::new(0),
        write_waiters: AtomicUsize::new(0),
        exclusive_owner: AtomicU64::new(0),
    };

    type GuardMarker = lock_api::GuardSend;

    // -----------------------------------------------------------------------
    // Shared (read) lock
    // -----------------------------------------------------------------------

    #[inline]
    fn lock_shared(&self) {
        if !self.try_lock_shared_fast() {
            self.lock_shared_slow(None);
        }
    }

    #[inline]
    fn try_lock_shared(&self) -> bool {
        self.try_lock_shared_fast()
    }

    #[inline]
    unsafe fn unlock_shared(&self) {
        let prev = self.state.fetch_sub(ONE_READER, Ordering::Release);
        // If we were the last reader and writers are waiting, wake one writer.
        // Compare only the READER bits: `state` may also carry WRITE_WAITING,
        // so an equality test against ONE_READER would never match once a
        // writer has queued -- which would leave that writer parked forever.
        if prev & READERS_MASK == ONE_READER
            && self.write_waiters.load(Ordering::Relaxed) > 0
        {
            // Wake ALL waiters, not one. Readers and writers park on the SAME
            // futex word, so a single wake can land on a reader -- which, seeing
            // WRITE_WAITING set, immediately re-parks and CONSUMES the wakeup,
            // leaving the writer asleep with nobody holding the lock. Waking all
            // guarantees the writer is among them; the readers that wake merely
            // re-park. The herd is bounded to this one transition (last reader
            // out with a writer queued), not the steady-state path.
            futex_wake(&self.state, i32::MAX as u32);
        }
    }

    // -----------------------------------------------------------------------
    // Exclusive (write) lock
    // -----------------------------------------------------------------------

    #[inline]
    fn lock_exclusive(&self) {
        if self
            .state
            .compare_exchange(
                0,
                WRITE_LOCKED,
                Ordering::Acquire,
                Ordering::Relaxed,
            )
            .is_ok()
        {
            self.exclusive_owner
                .store(crate::raw_mutex::thread_id(), Ordering::Relaxed);
            return;
        }
        self.lock_exclusive_slow(None);
    }

    #[inline]
    fn try_lock_exclusive(&self) -> bool {
        if self
            .state
            .compare_exchange(
                0,
                WRITE_LOCKED,
                Ordering::Acquire,
                Ordering::Relaxed,
            )
            .is_ok()
        {
            self.exclusive_owner
                .store(crate::raw_mutex::thread_id(), Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    #[inline]
    unsafe fn unlock_exclusive(&self) {
        self.exclusive_owner.store(0, Ordering::Relaxed);
        // Release the WRITE_LOCKED bit but PRESERVE WRITE_WAITING: a blind
        // `store(0)` would wipe the intent bit published by another queued
        // writer, re-opening the reader-admission window that starves it.
        // fetch_and keeps whatever WRITE_WAITING state the queue currently has.
        self.state.fetch_and(!WRITE_LOCKED & !READERS_MASK, Ordering::Release);

        // Wake writers first (reduce write starvation), then readers.
        //
        // Wake ALL when a writer is queued, for the same reason as
        // `unlock_shared`: readers and writers share one futex word, so a
        // single wake can be consumed by a reader that re-parks on seeing
        // WRITE_WAITING, stranding the writer. Waking all guarantees the writer
        // is included; superfluous readers just re-park.
        if self.write_waiters.load(Ordering::Relaxed) > 0 {
            futex_wake(&self.state, i32::MAX as u32);
        } else if self.read_waiters.load(Ordering::Relaxed) > 0 {
            // i32::MAX as u32 — kernel nr_wake is signed; u32::MAX wraps to -1.
            futex_wake(&self.state, i32::MAX as u32);
        }
    }

    #[inline]
    fn is_locked(&self) -> bool {
        self.state.load(Ordering::Relaxed) != 0
    }

    #[inline]
    fn is_locked_exclusive(&self) -> bool {
        self.state.load(Ordering::Relaxed) & WRITE_LOCKED != 0
    }
}

unsafe impl lock_api::RawRwLockTimed for NoxuRawRwLock {
    type Duration = Duration;
    type Instant = Instant;

    fn try_lock_shared_for(&self, timeout: Duration) -> bool {
        if self.try_lock_shared_fast() {
            return true;
        }
        self.lock_shared_slow(Some(Instant::now() + timeout))
    }

    fn try_lock_shared_until(&self, deadline: Instant) -> bool {
        if self.try_lock_shared_fast() {
            return true;
        }
        self.lock_shared_slow(Some(deadline))
    }

    fn try_lock_exclusive_for(&self, timeout: Duration) -> bool {
        if self
            .state
            .compare_exchange(
                0,
                WRITE_LOCKED,
                Ordering::Acquire,
                Ordering::Relaxed,
            )
            .is_ok()
        {
            self.exclusive_owner
                .store(crate::raw_mutex::thread_id(), Ordering::Relaxed);
            return true;
        }
        self.lock_exclusive_slow(Some(Instant::now() + timeout))
    }

    fn try_lock_exclusive_until(&self, deadline: Instant) -> bool {
        if self
            .state
            .compare_exchange(
                0,
                WRITE_LOCKED,
                Ordering::Acquire,
                Ordering::Relaxed,
            )
            .is_ok()
        {
            self.exclusive_owner
                .store(crate::raw_mutex::thread_id(), Ordering::Relaxed);
            return true;
        }
        self.lock_exclusive_slow(Some(deadline))
    }
}

impl NoxuRawRwLock {
    /// Fast path: try to increment reader count when no writer is active.
    #[inline]
    fn try_lock_shared_fast(&self) -> bool {
        let state = self.state.load(Ordering::Relaxed);
        // Refuse admission while the lock is write-held OR a writer is queued.
        // The WRITE_WAITING check is what makes writers non-starvable: without
        // it, an unbroken hand-over-hand reader chain keeps the reader count
        // above zero indefinitely and the writer's `state == 0` condition is
        // never observable.
        if state & (WRITE_LOCKED | WRITE_WAITING) != 0 {
            return false;
        }
        // No overflow check: WRITE_LOCKED bit acts as sentinel.
        self.state
            .compare_exchange(
                state,
                state + ONE_READER,
                Ordering::Acquire,
                Ordering::Relaxed,
            )
            .is_ok()
    }

    /// Slow path for shared lock with optional deadline.
    fn lock_shared_slow(&self, deadline: Option<Instant>) -> bool {
        loop {
            let state = self.state.load(Ordering::Relaxed);

            // Same admission rule as the fast path: yield to a queued writer.
            if state & (WRITE_LOCKED | WRITE_WAITING) == 0 {
                if self
                    .state
                    .compare_exchange_weak(
                        state,
                        state + ONE_READER,
                        Ordering::Acquire,
                        Ordering::Relaxed,
                    )
                    .is_ok()
                {
                    return true;
                }
                // CAS failed — retry.
                continue;
            }

            // Write lock is held — park until released.
            let timeout = match deadline {
                Some(dl) => {
                    let now = Instant::now();
                    if now >= dl {
                        return false;
                    }
                    Some(dl - now)
                }
                None => None,
            };

            self.read_waiters.fetch_add(1, Ordering::Relaxed);
            futex_wait(&self.state, state, timeout);
            let did_timeout =
                deadline.map(|dl| Instant::now() >= dl).unwrap_or(false);
            self.read_waiters.fetch_sub(1, Ordering::Relaxed);

            if did_timeout {
                return false;
            }
        }
    }

    /// A queued writer is abandoning its wait (timed out).
    ///
    /// Decrements the writer-waiter count and, if we were the LAST queued
    /// writer, clears `WRITE_WAITING` so readers are admitted again. Leaving the
    /// bit set with no writer behind it would lock readers out indefinitely --
    /// trading writer starvation for reader starvation.
    ///
    /// Clearing is conditional on being last, and a writer that queues
    /// concurrently re-sets the bit on its own next loop iteration, so a lost
    /// race here costs at most a brief window of reader admission, never
    /// permanent exclusion.
    fn give_up_waiting(&self) {
        let prev = self.write_waiters.fetch_sub(1, Ordering::Relaxed);
        if prev == 1 {
            self.state.fetch_and(!WRITE_WAITING, Ordering::Relaxed);
            // A reader may have parked while the bit was set; wake them all so
            // they re-evaluate admission now that it is cleared.
            if self.read_waiters.load(Ordering::Relaxed) > 0 {
                futex_wake(&self.state, i32::MAX as u32);
            }
        }
    }

    /// Slow path for exclusive lock with optional deadline.
    fn lock_exclusive_slow(&self, deadline: Option<Instant>) -> bool {
        self.write_waiters.fetch_add(1, Ordering::Relaxed);
        // Announce our intent so new readers stop entering. Set unconditionally
        // rather than CAS-once: the bit is sticky for as long as any writer is
        // queued, and the last writer to leave clears it (see the exit paths
        // below and `unlock_*`).
        self.state.fetch_or(WRITE_WAITING, Ordering::Relaxed);

        loop {
            let state = self.state.load(Ordering::Relaxed);

            // Acquirable when no readers hold it and no writer owns it. Our own
            // WRITE_WAITING bit may be set, so compare against that rather than
            // against a bare zero -- otherwise the writer that published the
            // bit would never see its own acquire condition become true.
            if state & (READERS_MASK | WRITE_LOCKED) == 0 {
                if self
                    .state
                    .compare_exchange_weak(
                        state,
                        // Preserve whatever WRITE_WAITING state we observed;
                        // reconciling it here would race with writers joining
                        // or leaving between the load and the CAS. The
                        // authoritative reconciliation happens below, AFTER the
                        // waiter count has been decremented.
                        WRITE_LOCKED | (state & WRITE_WAITING),
                        Ordering::Acquire,
                        Ordering::Relaxed,
                    )
                    .is_ok()
                {
                    self.exclusive_owner.store(
                        crate::raw_mutex::thread_id(),
                        Ordering::Relaxed,
                    );
                    // We are no longer a waiter. If nobody is queued behind us,
                    // clear the admission gate: a WRITE_WAITING bit with no
                    // writer behind it locks readers out permanently (observed
                    // as 32 readers parked with zero writers). Decrement FIRST
                    // so the count we test is authoritative.
                    if self.write_waiters.fetch_sub(1, Ordering::Relaxed) == 1 {
                        self.state.fetch_and(!WRITE_WAITING, Ordering::Relaxed);
                        // Readers may be parked on a state word that included
                        // WRITE_WAITING. We just changed that word, so their
                        // futex_wait comparison value is stale; wake them to
                        // re-evaluate. They will re-park on WRITE_LOCKED (we
                        // hold it), but not doing this risks a reader parked on
                        // a value that will never recur.
                        if self.read_waiters.load(Ordering::Relaxed) > 0 {
                            futex_wake(&self.state, i32::MAX as u32);
                        }
                    }
                    return true;
                }
                continue;
            }

            // Still contended — park.
            let timeout = match deadline {
                Some(dl) => {
                    let now = Instant::now();
                    if now >= dl {
                        self.give_up_waiting();
                        return false;
                    }
                    Some(dl - now)
                }
                None => None,
            };

            futex_wait(&self.state, state, timeout);

            if deadline.map(|dl| Instant::now() >= dl).unwrap_or(false) {
                self.give_up_waiting();
                return false;
            }
        }
    }

    /// Returns `true` if the write lock is currently held.
    #[inline]
    pub fn is_write_locked(&self) -> bool {
        self.state.load(Ordering::Relaxed) & WRITE_LOCKED != 0
    }

    /// Returns the total number of threads waiting to acquire this lock.
    #[inline]
    pub fn get_n_waiters(&self) -> usize {
        self.read_waiters.load(Ordering::Relaxed)
            + self.write_waiters.load(Ordering::Relaxed)
    }

    /// Returns the number of active readers.
    #[inline]
    pub fn reader_count(&self) -> u32 {
        self.state.load(Ordering::Relaxed) & READERS_MASK
    }

    /// Returns the exclusive owner thread ID hash (0 if not write-locked).
    #[inline]
    pub fn get_exclusive_owner(&self) -> u64 {
        self.exclusive_owner.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lock_api::{RawRwLock as _, RawRwLockTimed as _};
    use std::sync::Arc;

    /// `try_lock_exclusive` on the raw lock (the `lock_api::RawRwLock`
    /// entry point, not the wrapping `noxu_sync::RwLock`) must succeed
    /// when free and report the acquiring thread as the exclusive owner.
    #[test]
    fn raw_try_lock_exclusive_succeeds_when_free_and_sets_owner() {
        let raw = NoxuRawRwLock::INIT;
        assert!(!raw.is_locked());
        assert!(!raw.is_locked_exclusive());
        assert_eq!(raw.get_exclusive_owner(), 0);

        assert!(raw.try_lock_exclusive());
        assert!(raw.is_locked());
        assert!(raw.is_locked_exclusive());
        assert!(raw.is_write_locked());
        assert_ne!(
            raw.get_exclusive_owner(),
            0,
            "owner must be recorded after acquiring the write lock"
        );

        unsafe { raw.unlock_exclusive() };
        assert!(!raw.is_locked());
        assert_eq!(
            raw.get_exclusive_owner(),
            0,
            "owner must be cleared on unlock_exclusive"
        );
    }

    /// `try_lock_exclusive` must fail (not block, not panic) when a
    /// shared reader already holds the lock.
    #[test]
    fn raw_try_lock_exclusive_fails_while_read_locked() {
        let raw = NoxuRawRwLock::INIT;
        raw.lock_shared();
        assert!(raw.is_locked());
        assert!(!raw.is_locked_exclusive());
        assert_eq!(raw.reader_count(), 1);

        assert!(
            !raw.try_lock_exclusive(),
            "exclusive acquire must fail while a reader holds the lock"
        );

        unsafe { raw.unlock_shared() };
        assert_eq!(raw.reader_count(), 0);
        assert!(!raw.is_locked());
    }

    /// `try_lock_exclusive_for` / `try_lock_exclusive_until` (the
    /// `RawRwLockTimed` entry points) must time out rather than block
    /// forever when the lock is held, and must return control to the
    /// caller with `false`.
    #[test]
    fn raw_try_lock_exclusive_for_times_out_when_contended() {
        let raw = Arc::new(NoxuRawRwLock::INIT);
        assert!(raw.try_lock_exclusive());

        let raw2 = Arc::clone(&raw);
        let timed_out = std::thread::spawn(move || {
            !raw2.try_lock_exclusive_for(Duration::from_millis(30))
        })
        .join()
        .unwrap();
        assert!(timed_out);

        unsafe { raw.unlock_exclusive() };
    }

    /// `try_lock_shared_for` on a write-locked raw lock must time out and
    /// leave `get_n_waiters()` back at zero once the parked reader gives up
    /// -- proves the waiter counter used by `noxu_sync::RwLock::get_n_waiters`
    /// is not leaked on a timeout path.
    #[test]
    fn raw_try_lock_shared_for_times_out_and_clears_waiter_count() {
        let raw = Arc::new(NoxuRawRwLock::INIT);
        assert!(raw.try_lock_exclusive());

        let raw2 = Arc::clone(&raw);
        let timed_out = std::thread::spawn(move || {
            !raw2.try_lock_shared_for(Duration::from_millis(30))
        })
        .join()
        .unwrap();
        assert!(timed_out);
        assert_eq!(
            raw.get_n_waiters(),
            0,
            "waiter count must be decremented on the timeout path"
        );

        unsafe { raw.unlock_exclusive() };
    }

    /// A writer parked behind a reader must be woken and granted the lock
    /// once the reader releases it -- exercises the real
    /// `lock_exclusive_slow` park/wake path (not just the CAS fast path)
    /// together with `unlock_shared`'s "wake a writer" branch.
    #[test]
    fn raw_writer_parks_behind_reader_then_acquires_on_release() {
        let raw = Arc::new(NoxuRawRwLock::INIT);
        raw.lock_shared();

        let raw2 = Arc::clone(&raw);
        let writer = std::thread::spawn(move || {
            raw2.lock_exclusive();
            unsafe { raw2.unlock_exclusive() };
        });

        // Give the writer a chance to park behind the reader.
        std::thread::sleep(Duration::from_millis(30));
        assert!(!raw.is_locked_exclusive(), "reader still holds the lock");

        unsafe { raw.unlock_shared() };
        writer.join().unwrap();

        // Lock must be free again after the writer completed.
        assert!(!raw.is_locked());
    }

    /// `try_lock_exclusive_until` (deadline form) must also succeed on the
    /// fast (uncontended) path, exercising the CAS-success branch that
    /// `try_lock_exclusive_for` shares by delegation.
    #[test]
    fn raw_try_lock_exclusive_until_succeeds_fast_path() {
        let raw = NoxuRawRwLock::INIT;
        let deadline = Instant::now() + Duration::from_secs(5);
        assert!(raw.try_lock_exclusive_until(deadline));
        assert!(raw.is_locked_exclusive());
        unsafe { raw.unlock_exclusive() };
    }
}
