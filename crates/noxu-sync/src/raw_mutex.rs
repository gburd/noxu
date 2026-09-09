//! Futex-based raw mutex implementing `lock_api::RawMutex`.
//!
//! State encoding (single `AtomicU32`):
//!   0 = UNLOCKED
//!   1 = LOCKED      (no waiters)
//!   2 = LOCKED_CONTENDED (at least one thread waiting)
//!
//! Matches the algorithm used by parking_lot and the Linux kernel's
//! `futex_mutex` primitives. Spins ~40 cycles before falling back to
//! `futex_wait` to avoid syscall overhead under low contention.
//!
//! Additional fields:
//!   `waiters: AtomicUsize` — count of threads blocked in futex_wait.
//!   `owner: AtomicU64`  — hash of the owning thread's ID.

use crate::futex::{futex_wait, futex_wake};
use lock_api;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

pub(crate) const UNLOCKED: u32 = 0;
const LOCKED: u32 = 1;
const LOCKED_CONTENDED: u32 = 2;

/// Number of spin iterations before parking via futex.
/// ~40 iterations ≈ 100–200 ns on modern hardware; matches parking_lot heuristic.
const SPIN_LIMIT: usize = 40;

/// A unique, non-zero identifier for the calling thread.
///
/// The hash of `ThreadId` is cached in a thread-local so it is computed once
/// per thread rather than on every lock/unlock.  (`ThreadId::as_u64()` is
/// unstable, so we hash; hashing a fresh `DefaultHasher` per call showed up at
/// ~2% of the write-path CPU profile — the cache removes it.)
pub(crate) fn thread_id() -> u64 {
    thread_local! {
        static TID: u64 = {
            use std::hash::{Hash, Hasher};
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            std::thread::current().id().hash(&mut hasher);
            // Ensure non-zero so that 0 always means "unowned".
            hasher.finish() | 1
        };
    }
    TID.with(|t| *t)
}

/// Futex-based raw mutex.
///
/// Implements `lock_api::RawMutex` and `lock_api::RawMutexTimed` so that
/// `lock_api::Mutex<NoxuRawMutex, T>` gains the full parking_lot-compatible
/// API (`lock`, `try_lock`, `try_lock_for`, `is_locked`, `force_unlock`).
pub struct NoxuRawMutex {
    pub(crate) state: AtomicU32,
    /// Number of threads currently blocked in `futex_wait`.
    pub(crate) waiters: AtomicUsize,
    /// Thread ID hash of the current owner (0 if unlocked).
    pub(crate) owner: AtomicU64,
}

// SAFETY: `lock_api::RawMutex` requires that this type provide genuine mutual
// exclusion, so that `lock_api` may hand out `&mut T` to the single holder.
//
// That holds here: acquisition succeeds only by compare-exchanging the `state`
// word from UNLOCKED to a locked value, so at most one thread can hold the lock
// at a time; `unlock` stores UNLOCKED with Release and wakes a waiter, and
// acquisition uses Acquire, giving the happens-before edges `lock_api` relies on
// to make the guarded data's writes visible to the next holder. Waiters park in
// `futex_wait` on the same word and always re-check the predicate on wake, so a
// spurious or lost wake is a liveness concern, never a lost exclusion.
unsafe impl lock_api::RawMutex for NoxuRawMutex {
    /// Const-initializer, needed for embedding `NoxuRawMutex` directly in
    /// structs (e.g., `LogBuffer`) without heap allocation.
    const INIT: Self = NoxuRawMutex {
        state: AtomicU32::new(UNLOCKED),
        waiters: AtomicUsize::new(0),
        owner: AtomicU64::new(0),
    };

    type GuardMarker = lock_api::GuardSend;

    #[inline]
    fn lock(&self) {
        // Fast path: CAS UNLOCKED → LOCKED (no waiters yet).
        if self
            .state
            .compare_exchange(
                UNLOCKED,
                LOCKED,
                Ordering::Acquire,
                Ordering::Relaxed,
            )
            .is_ok()
        {
            self.owner.store(thread_id(), Ordering::Relaxed);
            return;
        }
        self.lock_slow(None);
    }

    #[inline]
    fn try_lock(&self) -> bool {
        if self
            .state
            .compare_exchange(
                UNLOCKED,
                LOCKED,
                Ordering::Acquire,
                Ordering::Relaxed,
            )
            .is_ok()
        {
            self.owner.store(thread_id(), Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    #[inline]
    unsafe fn unlock(&self) {
        self.owner.store(0, Ordering::Relaxed);
        let prev = self.state.swap(UNLOCKED, Ordering::Release);
        if prev == LOCKED_CONTENDED {
            futex_wake(&self.state, 1);
        }
    }

    #[inline]
    fn is_locked(&self) -> bool {
        self.state.load(Ordering::Relaxed) != UNLOCKED
    }
}

unsafe impl lock_api::RawMutexTimed for NoxuRawMutex {
    type Duration = Duration;
    type Instant = Instant;

    #[inline]
    fn try_lock_for(&self, timeout: Duration) -> bool {
        if self
            .state
            .compare_exchange(
                UNLOCKED,
                LOCKED,
                Ordering::Acquire,
                Ordering::Relaxed,
            )
            .is_ok()
        {
            self.owner.store(thread_id(), Ordering::Relaxed);
            return true;
        }
        self.lock_slow(Some(Instant::now() + timeout))
    }

    #[inline]
    fn try_lock_until(&self, deadline: Instant) -> bool {
        if self
            .state
            .compare_exchange(
                UNLOCKED,
                LOCKED,
                Ordering::Acquire,
                Ordering::Relaxed,
            )
            .is_ok()
        {
            self.owner.store(thread_id(), Ordering::Relaxed);
            return true;
        }
        self.lock_slow(Some(deadline))
    }
}

impl NoxuRawMutex {
    /// Slow-path lock with optional deadline.
    ///
    /// Spins `SPIN_LIMIT` times then falls back to futex_wait.
    /// Returns `true` if the lock was acquired, `false` if the deadline expired.
    fn lock_slow(&self, deadline: Option<Instant>) -> bool {
        let mut spin = 0usize;

        loop {
            let state = self.state.load(Ordering::Relaxed);

            // The lock just became free — grab it.
            if state == UNLOCKED {
                // Use LOCKED_CONTENDED so that unlock always wakes a waiter
                // (conservative but correct; avoids missed wakeups).
                if self
                    .state
                    .compare_exchange_weak(
                        UNLOCKED,
                        LOCKED_CONTENDED,
                        Ordering::Acquire,
                        Ordering::Relaxed,
                    )
                    .is_ok()
                {
                    self.owner.store(thread_id(), Ordering::Relaxed);
                    return true;
                }
                // CAS failed (spurious or contention) — retry without burning a spin.
                continue;
            }

            // Spin phase: burn CPU for a short time before parking.
            if spin < SPIN_LIMIT {
                spin += 1;
                std::hint::spin_loop();
                continue;
            }

            // Park phase: transition to LOCKED_CONTENDED then futex_wait.
            if state == LOCKED {
                // Mark as contended so unlock knows to wake a waiter.
                if self
                    .state
                    .compare_exchange_weak(
                        LOCKED,
                        LOCKED_CONTENDED,
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    )
                    .is_err()
                {
                    continue;
                }
            }
            // state is now LOCKED_CONTENDED (or was already).

            // Check deadline before sleeping.
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

            self.waiters.fetch_add(1, Ordering::Relaxed);
            let woke = futex_wait(&self.state, LOCKED_CONTENDED, timeout);
            self.waiters.fetch_sub(1, Ordering::Relaxed);

            if !woke {
                // futex_wait returned due to timeout.
                return false;
            }

            spin = 0;
        }
    }

    /// Returns the number of threads currently waiting to acquire this mutex.
    #[inline]
    pub fn get_n_waiters(&self) -> usize {
        self.waiters.load(Ordering::Relaxed)
    }

    /// Returns the thread-ID hash of the current owner, or 0 if unlocked.
    #[inline]
    pub fn get_owner(&self) -> u64 {
        self.owner.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lock_api::{RawMutex as _, RawMutexTimed as _};
    use std::sync::Arc;

    /// `get_owner()` must report 0 when unlocked and the acquiring
    /// thread's id-hash once locked, clearing back to 0 on unlock --
    /// this accessor exists specifically so higher layers can attribute
    /// a held lock to a thread (e.g. deadlock diagnostics).
    #[test]
    fn get_owner_reports_zero_when_unlocked_and_nonzero_when_locked() {
        let raw = NoxuRawMutex::INIT;
        assert_eq!(raw.get_owner(), 0);
        raw.lock();
        assert_ne!(raw.get_owner(), 0);
        unsafe { raw.unlock() };
        assert_eq!(raw.get_owner(), 0);
    }

    /// `try_lock_until` (the deadline-based `RawMutexTimed` entry point,
    /// distinct from the duration-based `try_lock_for`) must succeed on
    /// the uncontended fast path.
    #[test]
    fn try_lock_until_succeeds_on_fast_path() {
        let raw = NoxuRawMutex::INIT;
        let deadline = Instant::now() + Duration::from_secs(5);
        assert!(raw.try_lock_until(deadline));
        assert!(raw.is_locked());
        unsafe { raw.unlock() };
    }

    /// `try_lock_until` must time out (not block indefinitely) when the
    /// mutex is held by another thread past the deadline -- exercises the
    /// slow-path delegation distinct from `try_lock_for`'s.
    #[test]
    fn try_lock_until_times_out_when_contended() {
        let raw = Arc::new(NoxuRawMutex::INIT);
        raw.lock();

        let raw2 = Arc::clone(&raw);
        let timed_out = std::thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_millis(30);
            !raw2.try_lock_until(deadline)
        })
        .join()
        .unwrap();
        assert!(timed_out);

        unsafe { raw.unlock() };
    }

    /// A second thread contending on an already-locked mutex must park
    /// (via the spin-then-futex `lock_slow` path) and be woken once the
    /// holder unlocks -- exercises the CONTENDED transition and the
    /// `unlock()` wake branch together, not just the CAS fast path.
    #[test]
    fn contended_lock_parks_and_wakes_on_unlock() {
        let raw = Arc::new(NoxuRawMutex::INIT);
        raw.lock();

        let raw2 = Arc::clone(&raw);
        let waiter = std::thread::spawn(move || {
            raw2.lock();
            unsafe { raw2.unlock() };
        });

        // Give the second thread time to spin out and park.
        std::thread::sleep(Duration::from_millis(50));
        assert!(raw.is_locked());
        assert!(
            raw.get_n_waiters() >= 1,
            "the blocked thread must be recorded as a waiter"
        );

        unsafe { raw.unlock() };
        waiter.join().unwrap();
        assert!(!raw.is_locked());
    }
}
