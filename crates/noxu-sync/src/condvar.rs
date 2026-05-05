//! Futex-based condition variable.
//!
//! Uses a sequence counter approach: `notify_one`/`notify_all` increment the
//! counter before calling `futex_wake`. A waiting thread snapshots the counter
//! before releasing the mutex; `futex_wait` returns immediately if the counter
//! has already changed, preventing missed wakeups.
//!
//! This matches the API exported by `parking_lot::Condvar`:
//!   - `wait(&mut MutexGuard<'_, T>)`
//!   - `wait_for(&mut MutexGuard<'_, T>, Duration) -> WaitTimeoutResult`
//!   - `notify_one()`
//!   - `notify_all()`

use crate::futex::{futex_wait, futex_wake};
use crate::MutexGuard;
use lock_api;
use lock_api::RawMutex as RawMutexTrait;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

/// Result of a timed condvar wait.
///
/// Drop-in replacement for `parking_lot::WaitTimeoutResult`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WaitTimeoutResult(pub(crate) bool);

impl WaitTimeoutResult {
    /// Returns `true` if the wait timed out, `false` if it was notified.
    #[inline]
    pub fn timed_out(self) -> bool {
        self.0
    }
}

/// Futex-based condition variable.
///
/// Drop-in replacement for `parking_lot::Condvar`. Works exclusively with
/// `noxu_sync::Mutex<T>` guards.
pub struct Condvar {
    /// Sequence counter: incremented by each `notify_*` call.
    /// Waiters snapshot this before releasing the mutex; `futex_wait` checks
    /// that the value still matches, preventing lost-wakeup races.
    seq: AtomicU32,
}

impl Condvar {
    /// Creates a new `Condvar`.
    pub const fn new() -> Self {
        Condvar {
            seq: AtomicU32::new(0),
        }
    }

    /// Atomically releases the mutex guard and waits for a notification.
    ///
    /// Re-acquires the mutex before returning. Spurious wakeups are possible;
    /// callers must re-check their condition in a loop.
    ///
    /// # Panics
    ///
    /// Does not panic. Safe to call from any thread holding the guard.
    pub fn wait<T>(&self, guard: &mut MutexGuard<'_, T>) {
        let seq = self.seq.load(Ordering::SeqCst);

        // Release the mutex before parking.
        let mutex = lock_api::MutexGuard::mutex(guard);
        unsafe { mutex.force_unlock() };

        // Park until seq changes (woken by notify) or spurious wakeup.
        futex_wait(&self.seq, seq, None);

        // Re-acquire before returning to caller.
        // SAFETY: We released the lock above; re-acquiring it here restores
        // the invariant that the guard is valid (lock held) on return.
        unsafe { mutex.raw().lock() };
    }

    /// Atomically releases the mutex guard, waits for a notification or timeout.
    ///
    /// Returns `WaitTimeoutResult(true)` if the timeout elapsed, `false` if notified.
    pub fn wait_for<T>(
        &self,
        guard: &mut MutexGuard<'_, T>,
        timeout: Duration,
    ) -> WaitTimeoutResult {
        let seq = self.seq.load(Ordering::SeqCst);
        let deadline = Instant::now() + timeout;

        let mutex = lock_api::MutexGuard::mutex(guard);
        unsafe { mutex.force_unlock() };

        let timed_out = loop {
            let now = Instant::now();
            if now >= deadline {
                break true;
            }
            let remaining = deadline - now;
            let woke = futex_wait(&self.seq, seq, Some(remaining));
            if !woke {
                // futex_wait returned false → timed out.
                break true;
            }
            // Check if seq changed (notification received).
            if self.seq.load(Ordering::Relaxed) != seq {
                break false;
            }
            // Spurious wakeup: re-check deadline.
            if Instant::now() >= deadline {
                break true;
            }
        };

        // SAFETY: We released the lock above; re-acquiring restores guard validity.
        unsafe { mutex.raw().lock() };
        WaitTimeoutResult(timed_out)
    }

    /// Wakes one thread waiting on this condvar.
    #[inline]
    pub fn notify_one(&self) {
        self.seq.fetch_add(1, Ordering::SeqCst);
        futex_wake(&self.seq, 1);
    }

    /// Wakes all threads waiting on this condvar.
    #[inline]
    pub fn notify_all(&self) {
        self.seq.fetch_add(1, Ordering::SeqCst);
        // Use i32::MAX as u32 — the kernel nr_wake field is signed;
        // u32::MAX would truncate to -1 and wake at most one thread.
        futex_wake(&self.seq, i32::MAX as u32);
    }
}

impl std::fmt::Debug for Condvar {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Condvar").finish_non_exhaustive()
    }
}

impl Default for Condvar {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Mutex;
    use std::sync::Arc;
    use std::time::Duration;

    #[test]
    fn test_notify_one_wakes_waiter() {
        let mutex = Arc::new(Mutex::new(false));
        let condvar = Arc::new(Condvar::new());

        let m2 = mutex.clone();
        let cv2 = condvar.clone();
        let handle = std::thread::spawn(move || {
            let mut guard = m2.lock();
            while !*guard {
                cv2.wait(&mut guard);
            }
            true
        });

        std::thread::sleep(Duration::from_millis(20));
        {
            let mut guard = mutex.lock();
            *guard = true;
            condvar.notify_one();
        }
        assert!(handle.join().unwrap());
    }

    #[test]
    fn test_notify_all_wakes_all_waiters() {
        let mutex = Arc::new(Mutex::new(0usize));
        let condvar = Arc::new(Condvar::new());
        let mut handles = Vec::new();

        for _ in 0..4 {
            let m = mutex.clone();
            let cv = condvar.clone();
            handles.push(std::thread::spawn(move || {
                let mut guard = m.lock();
                while *guard == 0 {
                    cv.wait(&mut guard);
                }
            }));
        }

        std::thread::sleep(Duration::from_millis(30));
        {
            let mut guard = mutex.lock();
            *guard = 1;
            condvar.notify_all();
        }
        for h in handles {
            h.join().unwrap();
        }
    }

    #[test]
    fn test_wait_for_times_out() {
        let mutex = Arc::new(Mutex::new(()));
        let condvar = Arc::new(Condvar::new());

        let mut guard = mutex.lock();
        let result = condvar.wait_for(&mut guard, Duration::from_millis(30));
        assert!(result.timed_out(), "should have timed out");
    }

    #[test]
    fn test_wait_for_notified_before_timeout() {
        let mutex = Arc::new(Mutex::new(()));
        let condvar = Arc::new(Condvar::new());

        let cv2 = condvar.clone();
        let handle = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(10));
            cv2.notify_one();
        });

        let mut guard = mutex.lock();
        let result = condvar.wait_for(&mut guard, Duration::from_millis(500));
        assert!(!result.timed_out(), "should have been notified");
        handle.join().unwrap();
    }
}
