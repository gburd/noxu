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
pub(crate) fn thread_id() -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    std::thread::current().id().hash(&mut hasher);
    // Ensure non-zero so that 0 always means "unowned".
    hasher.finish() | 1
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

unsafe impl lock_api::RawMutex for NoxuRawMutex {
    /// Const-initializer, needed for embedding `NoxuRawMutex` directly in
    /// structs (e.g., `LogBuffer`) without heap allocation.
    #[allow(clippy::declare_interior_mutable_const)]
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
            .compare_exchange(UNLOCKED, LOCKED, Ordering::Acquire, Ordering::Relaxed)
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
            .compare_exchange(UNLOCKED, LOCKED, Ordering::Acquire, Ordering::Relaxed)
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
            .compare_exchange(UNLOCKED, LOCKED, Ordering::Acquire, Ordering::Relaxed)
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
            .compare_exchange(UNLOCKED, LOCKED, Ordering::Acquire, Ordering::Relaxed)
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
