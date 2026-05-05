//! Futex-based raw reader-writer lock implementing `lock_api::RawRwLock`.
//!
//! State encoding (single `AtomicU32`):
//!   bits 0-29: reader count  (ONE_READER = 1, max ~1 billion concurrent readers)
//!   bit  30:   WRITE_LOCKED  (exclusive writer holds the lock)
//!   bit  31:   WRITE_WAITING (reserved, not currently used — non-fair mode)
//!
//! Non-fair design: new readers are not blocked by pending writers, matching
//! `SharedLatchImpl(fair=false)` in JE. This maximises read throughput.
//!
//! Additional fields:
//!   `read_waiters`  — readers blocked waiting for a write to finish
//!   `write_waiters` — writers blocked waiting for all readers/writers to finish
//!   `exclusive_owner` — thread ID hash of the write-lock owner
//!
//! All of the above match JE's `SharedLatchImpl` diagnostics interface.

use crate::futex::{futex_wait, futex_wake};
use lock_api;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

/// State bit representing an exclusive (write) lock.
pub(crate) const WRITE_LOCKED: u32 = 1 << 30;
/// Each reader increments the state by this amount.
const ONE_READER: u32 = 1;
/// Mask for extracting the reader count.
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

unsafe impl lock_api::RawRwLock for NoxuRawRwLock {
    #[allow(clippy::declare_interior_mutable_const)]
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
        if prev == ONE_READER && self.write_waiters.load(Ordering::Relaxed) > 0 {
            futex_wake(&self.state, 1);
        }
    }

    // -----------------------------------------------------------------------
    // Exclusive (write) lock
    // -----------------------------------------------------------------------

    #[inline]
    fn lock_exclusive(&self) {
        if self
            .state
            .compare_exchange(0, WRITE_LOCKED, Ordering::Acquire, Ordering::Relaxed)
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
            .compare_exchange(0, WRITE_LOCKED, Ordering::Acquire, Ordering::Relaxed)
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
        self.state.store(0, Ordering::Release);

        // Wake writers first (reduce write starvation), then readers.
        if self.write_waiters.load(Ordering::Relaxed) > 0 {
            futex_wake(&self.state, 1);
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
            .compare_exchange(0, WRITE_LOCKED, Ordering::Acquire, Ordering::Relaxed)
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
            .compare_exchange(0, WRITE_LOCKED, Ordering::Acquire, Ordering::Relaxed)
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
        if state & WRITE_LOCKED != 0 {
            return false;
        }
        // No overflow check: WRITE_LOCKED bit acts as sentinel.
        self.state
            .compare_exchange(state, state + ONE_READER, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
    }

    /// Slow path for shared lock with optional deadline.
    fn lock_shared_slow(&self, deadline: Option<Instant>) -> bool {
        loop {
            let state = self.state.load(Ordering::Relaxed);

            if state & WRITE_LOCKED == 0 {
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
            let did_timeout = deadline.map(|dl| Instant::now() >= dl).unwrap_or(false);
            self.read_waiters.fetch_sub(1, Ordering::Relaxed);

            if did_timeout {
                return false;
            }
        }
    }

    /// Slow path for exclusive lock with optional deadline.
    fn lock_exclusive_slow(&self, deadline: Option<Instant>) -> bool {
        self.write_waiters.fetch_add(1, Ordering::Relaxed);

        loop {
            let state = self.state.load(Ordering::Relaxed);

            // Lock is fully free (no readers, no writer).
            if state == 0 {
                if self
                    .state
                    .compare_exchange_weak(
                        0,
                        WRITE_LOCKED,
                        Ordering::Acquire,
                        Ordering::Relaxed,
                    )
                    .is_ok()
                {
                    self.exclusive_owner
                        .store(crate::raw_mutex::thread_id(), Ordering::Relaxed);
                    self.write_waiters.fetch_sub(1, Ordering::Relaxed);
                    return true;
                }
                continue;
            }

            // Still contended — park.
            let timeout = match deadline {
                Some(dl) => {
                    let now = Instant::now();
                    if now >= dl {
                        self.write_waiters.fetch_sub(1, Ordering::Relaxed);
                        return false;
                    }
                    Some(dl - now)
                }
                None => None,
            };

            futex_wait(&self.state, state, timeout);

            if deadline.map(|dl| Instant::now() >= dl).unwrap_or(false) {
                self.write_waiters.fetch_sub(1, Ordering::Relaxed);
                return false;
            }
        }
    }

    /// Returns `true` if the write lock is currently held.
    ///
    /// Matches JE's `SharedLatchImpl.isOwner()` / `isWriteLockedByCurrentThread()`.
    #[inline]
    pub fn is_write_locked(&self) -> bool {
        self.state.load(Ordering::Relaxed) & WRITE_LOCKED != 0
    }

    /// Returns the total number of threads waiting to acquire this lock.
    ///
    /// Matches JE's `SharedLatchImpl.getNWaiters()`.
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
