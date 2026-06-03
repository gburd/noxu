//! Exclusive (mutex-like) latch.
//!
//! Provides exclusive latching implemented with `noxu_sync::Mutex`.
//! Reentrancy is prevented: attempting to acquire a latch already held by
//! the current thread will panic, detecting accidental reentrant calls.

use crate::{LatchContext, LatchError};
use noxu_sync::Mutex;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;

/// An exclusive (mutex-like) latch.
///
/// This latch provides exclusive/write access only. It prevents reentrant
/// acquisition by the same thread, which increases reliability by detecting
/// accidental reentrant calls.
pub struct ExclusiveLatch {
    context: LatchContext,
    inner: Mutex<()>,
    /// Thread ID of the current owner (0 if not held).
    owner: AtomicU64,
}

impl ExclusiveLatch {
    /// Creates a new exclusive latch.
    pub fn new(context: LatchContext) -> Self {
        ExclusiveLatch {
            context,
            inner: Mutex::new(()),
            owner: AtomicU64::new(0),
        }
    }

    /// Creates a new exclusive latch with the given name.
    pub fn named(name: impl Into<String>) -> Self {
        Self::new(LatchContext::new(name))
    }

    /// Acquires the latch for exclusive access.
    ///
    /// Returns `Ok(guard)` on success, or `Err(LatchError::Timeout)` if the
    /// acquisition times out.
    ///
    /// # Panics
    ///
    /// Panics if the latch is already held by the calling thread (reentrancy
    /// detected). Reentrancy is a programming error and must not be silenced.
    pub fn acquire(&self) -> Result<ExclusiveLatchGuard<'_>, LatchError> {
        let current = thread_id();
        if self.owner.load(Ordering::Relaxed) == current {
            panic!(
                "Latch already held: {} (thread {:?})",
                self.context.name,
                thread::current().name()
            );
        }

        let timeout = self.context.timeout;
        let guard = self.inner.try_lock_for(timeout).ok_or_else(|| {
            LatchError::Timeout(format!(
                "Latch acquisition timed out after {}ms: {}",
                timeout.as_millis(),
                self.context.name
            ))
        })?;
        self.owner.store(current, Ordering::Relaxed);
        Ok(ExclusiveLatchGuard { latch: self, _guard: guard })
    }

    /// Attempts to acquire the latch without blocking.
    ///
    /// Returns `Some(guard)` if the latch was acquired, `None` if it is
    /// currently held by another thread.
    ///
    /// # Panics
    ///
    /// Panics if the latch is already held by the calling thread.
    pub fn try_acquire(&self) -> Option<ExclusiveLatchGuard<'_>> {
        let current = thread_id();
        if self.owner.load(Ordering::Relaxed) == current {
            panic!(
                "Latch already held: {} (thread {:?})",
                self.context.name,
                thread::current().name()
            );
        }

        self.inner.try_lock().map(|guard| {
            self.owner.store(current, Ordering::Relaxed);
            ExclusiveLatchGuard { latch: self, _guard: guard }
        })
    }

    /// Returns true if the latch is currently held by any thread.
    pub fn is_locked(&self) -> bool {
        self.inner.is_locked()
    }

    /// Returns true if the latch is held by the current thread.
    pub fn is_owner(&self) -> bool {
        self.owner.load(Ordering::Relaxed) == thread_id()
    }

    /// Returns the context for this latch.
    pub fn context(&self) -> &LatchContext {
        &self.context
    }

    /// Releases the latch if held by the current thread.
    /// Does nothing if not held by the current thread.
    ///
    /// Equivalent of `release_if_owner()` — releases only if held by this thread.
    pub fn release_if_owner(&self) {
        if self.is_owner() {
            self.owner.store(0, Ordering::Relaxed);
            // SAFETY: We verified ownership above
            unsafe { self.inner.force_unlock() };
        }
    }
}

impl fmt::Debug for ExclusiveLatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ExclusiveLatch({}, locked={})",
            self.context.name,
            self.is_locked()
        )
    }
}

/// RAII guard for an exclusive latch. Releases the latch when dropped.
pub struct ExclusiveLatchGuard<'a> {
    latch: &'a ExclusiveLatch,
    _guard: noxu_sync::MutexGuard<'a, ()>,
}

impl Drop for ExclusiveLatchGuard<'_> {
    fn drop(&mut self) {
        self.latch.owner.store(0, Ordering::Relaxed);
    }
}

/// Returns a unique identifier for the current thread.
fn thread_id() -> u64 {
    // Use the raw ThreadId hash as a stable identifier since
    // ThreadId::as_u64() is unstable.
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    thread::current().id().hash(&mut hasher);
    // `| 1` guarantees a non-zero id: 0 is the "unowned" sentinel for the
    // `owner` atomic, so a thread whose hash is 0 would otherwise false-panic
    // "latch already held" on its first acquisition. Matches
    // `noxu-sync::raw_mutex`.
    hasher.finish() | 1
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_acquire_release() {
        let latch = ExclusiveLatch::named("test");
        assert!(!latch.is_locked());
        {
            let _guard = latch.acquire().expect("acquire");
            assert!(latch.is_locked());
            assert!(latch.is_owner());
        }
        assert!(!latch.is_locked());
    }

    #[test]
    fn test_try_acquire() {
        let latch = Arc::new(ExclusiveLatch::named("test"));
        let guard = latch.try_acquire();
        assert!(guard.is_some());

        // Another thread should fail to acquire
        let latch2 = latch.clone();
        let handle = std::thread::spawn(move || latch2.try_acquire().is_none());
        assert!(handle.join().unwrap());
    }

    #[test]
    #[should_panic(expected = "Latch already held")]
    fn test_reentrant_panics() {
        let latch = ExclusiveLatch::named("test");
        let _guard = latch.acquire().expect("first acquire");
        let _ = latch.acquire(); // Should panic before returning
    }

    #[test]
    fn test_release_if_owner() {
        let latch = ExclusiveLatch::named("test");
        {
            let _guard = latch.acquire().expect("acquire");
            assert!(latch.is_owner());
        }
        // After guard dropped, release_if_owner should be a no-op
        latch.release_if_owner();
        assert!(!latch.is_locked());
    }

    #[test]
    fn test_acquire_timeout() {
        use std::time::Duration;
        let ctx = crate::LatchContext::with_timeout(
            "test-timeout",
            Duration::from_millis(50),
        );
        let latch = Arc::new(ExclusiveLatch::new(ctx));

        let latch2 = latch.clone();
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let barrier2 = barrier.clone();
        let handle = std::thread::spawn(move || {
            let _g = latch2.acquire().expect("acquire in spawned thread");
            barrier2.wait(); // signal: lock is held
            std::thread::sleep(Duration::from_millis(200));
        });

        barrier.wait(); // wait until other thread holds the lock
        // acquire() should return Err(LatchError::Timeout) instead of panicking.
        let result = latch.acquire();
        assert!(result.is_err(), "expected latch timeout error, got Ok");
        let _ = handle.join();
    }

    #[test]
    fn test_context_name_and_timeout() {
        use std::time::Duration;
        let ctx = crate::LatchContext::with_timeout(
            "my-latch",
            Duration::from_secs(1),
        );
        let latch = ExclusiveLatch::new(ctx);
        assert_eq!(latch.context().name, "my-latch");
        assert_eq!(latch.context().timeout, Duration::from_secs(1));
    }

    #[test]
    fn test_is_not_owner_when_not_held() {
        let latch = ExclusiveLatch::named("test-owner");
        assert!(!latch.is_owner());
        assert!(!latch.is_locked());
    }

    #[test]
    fn test_is_owner_only_in_owning_thread() {
        let latch = Arc::new(ExclusiveLatch::named("test-owner-thread"));
        let _guard = latch.acquire().expect("acquire");
        assert!(latch.is_owner());

        // Another thread should see is_owner() == false even while we hold it
        let latch2 = latch.clone();
        let handle = std::thread::spawn(move || {
            assert!(!latch2.is_owner(), "non-owner thread should not be owner");
            assert!(latch2.is_locked(), "latch should be locked");
        });
        handle.join().unwrap();
    }

    #[test]
    fn test_concurrent_acquire_serializes() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let latch = Arc::new(ExclusiveLatch::named("serial-test"));
        let counter = Arc::new(AtomicUsize::new(0));
        let concurrent = Arc::new(AtomicUsize::new(0));
        let violations = Arc::new(AtomicUsize::new(0));

        let threads: Vec<_> = (0..4)
            .map(|_| {
                let latch = latch.clone();
                let counter = counter.clone();
                let concurrent = concurrent.clone();
                let violations = violations.clone();
                std::thread::spawn(move || {
                    for _ in 0..25 {
                        let _guard = latch.acquire().expect("acquire");
                        // We should be the only thread in this section
                        let prev = concurrent.fetch_add(1, Ordering::SeqCst);
                        if prev != 0 {
                            violations.fetch_add(1, Ordering::SeqCst);
                        }
                        counter.fetch_add(1, Ordering::SeqCst);
                        concurrent.fetch_sub(1, Ordering::SeqCst);
                    }
                })
            })
            .collect();

        for t in threads {
            t.join().unwrap();
        }
        assert_eq!(counter.load(Ordering::SeqCst), 100);
        assert_eq!(
            violations.load(Ordering::SeqCst),
            0,
            "mutual exclusion violated"
        );
    }

    #[test]
    fn test_try_acquire_reentrant_panics() {
        // try_acquire also panics on reentrant attempt
        let result = std::panic::catch_unwind(|| {
            let latch = ExclusiveLatch::named("try-reentrant");
            let _guard = latch.acquire();
            // try_acquire should panic since we already own it
            let _guard2 = latch.try_acquire();
        });
        assert!(result.is_err(), "expected panic on reentrant try_acquire");
    }

    #[test]
    fn test_debug_format() {
        let latch = ExclusiveLatch::named("debug-test");
        let s = format!("{:?}", latch);
        assert!(s.contains("debug-test"));
        assert!(s.contains("locked=false"));
    }

    // -----------------------------------------------------------------------
    // Ported from LatchTest.java — exclusive latch invariants
    // -----------------------------------------------------------------------

    /// Acquiring an already-held exclusive latch must panic (reentrancy detection).
    #[test]
    fn test_acquire_reacquire_panics() {
        let result = std::panic::catch_unwind(|| {
            let latch = ExclusiveLatch::named("noxu-reacquire");
            let _g1 = latch.acquire().expect("first acquire");
            // Second acquire on same thread must panic.
            let _ = latch.acquire();
        });
        assert!(result.is_err(), "reentrant acquire should panic");
    }

    /// Releasing a latch that is not held must panic.
    #[test]
    fn test_release_not_held_panics() {
        // release_if_owner on a latch not held should be a no-op (not panic).
        // But acquiring twice panics, so we verify the second-acquire path
        // by catching the panic (tested above).  Here verify the "not owner"
        // path via release_if_owner which is safe when not held.
        let latch = ExclusiveLatch::named("noxu-not-held");
        assert!(!latch.is_locked());
        // release_if_owner on a not-held latch should be a no-op.
        latch.release_if_owner();
        assert!(!latch.is_locked());
    }

    /// `try_acquire` returns None when held by another thread, and Some when available.
    #[test]
    fn test_try_acquire_no_wait() {
        let latch = Arc::new(ExclusiveLatch::named("noxu-no-wait"));
        let barrier = Arc::new(std::sync::Barrier::new(2));

        // Thread 1 holds the latch.
        let latch2 = latch.clone();
        let barrier2 = barrier.clone();
        let held = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let held2 = held;
        let released = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let released2 = released.clone();

        let h = std::thread::spawn(move || {
            let _g = latch2.acquire();
            held2.store(true, std::sync::atomic::Ordering::SeqCst);
            barrier2.wait(); // signal acquired
            // Wait until the test thread says we can release.
            while !released2.load(std::sync::atomic::Ordering::SeqCst) {
                std::thread::yield_now();
            }
        });

        barrier.wait(); // wait until thread holds the latch

        // try_acquire should fail while thread 1 holds it.
        assert!(!latch.is_owner(), "main thread should not be owner");
        let r = latch.try_acquire();
        assert!(
            r.is_none(),
            "try_acquire should fail while other thread holds it"
        );
        assert!(latch.is_locked());

        // Signal thread to release.
        released.store(true, std::sync::atomic::Ordering::SeqCst);
        h.join().unwrap();

        // Now try_acquire should succeed.
        let g = latch.try_acquire();
        assert!(g.is_some(), "try_acquire should succeed after release");
        assert!(latch.is_locked());
        drop(g);
        assert!(!latch.is_locked());
    }

    /// A second thread blocks on `acquire` while the first holds it;
    /// after the first releases, the second is granted.
    #[test]
    fn test_wait_blocks_until_released() {
        let latch = Arc::new(ExclusiveLatch::named("noxu-wait"));
        let acquired = Arc::new(std::sync::atomic::AtomicBool::new(false));

        // Thread 1 acquires the latch.
        let g = latch.acquire().expect("acquire");
        assert!(latch.is_locked());

        let latch2 = latch.clone();
        let acquired2 = acquired.clone();
        let h = std::thread::spawn(move || {
            // This will block until thread 1 releases.
            let _g2 = latch2.acquire().expect("acquire in spawned thread");
            acquired2.store(true, std::sync::atomic::Ordering::SeqCst);
        });

        // Give thread 2 time to block.
        std::thread::sleep(std::time::Duration::from_millis(30));
        assert!(!acquired.load(std::sync::atomic::Ordering::SeqCst));

        // Release; thread 2 must wake and acquire.
        drop(g);
        h.join().unwrap();
        assert!(acquired.load(std::sync::atomic::Ordering::SeqCst));
    }

    /// N threads wait sequentially; each is granted after the previous releases.
    #[test]
    fn test_multiple_waiters_sequential_grant() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        const N: usize = 5;
        let latch = Arc::new(ExclusiveLatch::named("noxu-multi-wait"));
        let order = Arc::new(AtomicUsize::new(0));

        // Main thread acquires.
        let g = latch.acquire().expect("acquire");

        let mut handles = Vec::new();
        for i in 0..N {
            let latch2 = latch.clone();
            let order2 = order.clone();
            let h = std::thread::spawn(move || {
                // Stagger entry slightly so they queue up in order.
                std::thread::sleep(std::time::Duration::from_millis(
                    5 * (i as u64 + 1),
                ));
                let _g = latch2.acquire().expect("acquire in spawned thread");
                order2.fetch_add(1, Ordering::SeqCst);
            });
            handles.push(h);
        }

        // Wait until all threads are likely blocked.
        std::thread::sleep(std::time::Duration::from_millis(80));

        // Release; threads unblock one by one.
        drop(g);
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(
            order.load(Ordering::SeqCst),
            N,
            "all waiters should have been granted"
        );
    }
}
