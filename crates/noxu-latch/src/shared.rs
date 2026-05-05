//! Shared/exclusive (reader-writer) latch.
//!
//! Port of `com.sleepycat.je.latch.SharedLatchImpl`.
//!
//! Extends the latch concept to provide reader-writer access. Multiple threads
//! can hold the latch in shared mode simultaneously, but exclusive mode
//! requires sole access.
//!
//! This may also operate in exclusive-only mode (matching JE's behavior where
//! BIN latches are exclusive-only but use the SharedLatch interface). In
//! exclusive-only mode, `acquire_shared()` behaves like `acquire_exclusive()`.

use crate::LatchContext;
use noxu_sync::RwLock;
use std::cell::Cell;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;

// Thread-local counter tracking how many read guards the current thread holds
// across all SharedLatch instances.  Used to detect read-to-write upgrade
// attempts that would deadlock with noxu_sync's non-reentrant RwLock.
thread_local! {
    static READ_HOLD_COUNT: Cell<u32> = const { Cell::new(0) };
}

fn increment_read_hold() {
    READ_HOLD_COUNT.with(|c| c.set(c.get().saturating_add(1)));
}

fn decrement_read_hold() {
    READ_HOLD_COUNT.with(|c| c.set(c.get().saturating_sub(1)));
}

fn read_hold_count() -> u32 {
    READ_HOLD_COUNT.with(|c| c.get())
}

/// A shared/exclusive (reader-writer) latch.
///
/// When `exclusive_only` is true, this behaves identically to an
/// ExclusiveLatch (shared acquisition degrades to exclusive). This matches
/// JE's pattern where BIN latches are exclusive-only but use the SharedLatch
/// interface for polymorphism with IN latches.
///
/// Port of `com.sleepycat.je.latch.SharedLatchImpl`.
pub struct SharedLatch {
    context: LatchContext,
    exclusive_only: bool,
    inner: RwLock<()>,
    /// Thread ID of the exclusive owner (0 if not exclusively held).
    exclusive_owner: AtomicU64,
}

impl SharedLatch {
    /// Creates a new shared latch.
    pub fn new(context: LatchContext, exclusive_only: bool) -> Self {
        SharedLatch {
            context,
            exclusive_only,
            inner: RwLock::new(()),
            exclusive_owner: AtomicU64::new(0),
        }
    }

    /// Creates a new shared latch with the given name.
    pub fn named(name: impl Into<String>, exclusive_only: bool) -> Self {
        Self::new(LatchContext::new(name), exclusive_only)
    }

    /// Returns whether this latch operates in exclusive-only mode.
    pub fn is_exclusive_only(&self) -> bool {
        self.exclusive_only
    }

    /// Acquires the latch for exclusive/write access.
    ///
    /// # Panics
    ///
    /// Panics if the latch is already held exclusively by the calling thread,
    /// if the calling thread holds any read guards (which would deadlock), or
    /// if the acquisition times out.
    pub fn acquire_exclusive(&self) -> SharedLatchWriteGuard<'_> {
        let current = thread_id();
        if self.exclusive_owner.load(Ordering::Relaxed) == current {
            panic!(
                "Latch already held exclusively: {} (thread {:?})",
                self.context.name,
                thread::current().name()
            );
        }

        // Detect read-to-write upgrade: this thread already holds a read guard
        // and attempting to acquire write would deadlock with noxu_sync's
        // non-reentrant RwLock (matches JE's EnvironmentFailureException check
        // on getReadHoldCount() > 0).
        if read_hold_count() > 0 {
            panic!(
                "Deadlock: thread holds read lock and requested write lock on latch {}",
                self.context.name
            );
        }

        let timeout = self.context.timeout;
        let guard = self.inner.try_write_for(timeout).unwrap_or_else(|| {
            panic!(
                "Latch acquisition timed out after {}ms: {}",
                timeout.as_millis(),
                self.context.name
            )
        });
        self.exclusive_owner.store(current, Ordering::Relaxed);
        SharedLatchWriteGuard { latch: self, _guard: guard }
    }

    /// Attempts to acquire the latch for exclusive access without blocking.
    ///
    /// Returns `Some(guard)` if acquired, `None` if not available.
    pub fn try_acquire_exclusive(&self) -> Option<SharedLatchWriteGuard<'_>> {
        let current = thread_id();
        if self.exclusive_owner.load(Ordering::Relaxed) == current {
            panic!(
                "Latch already held exclusively: {} (thread {:?})",
                self.context.name,
                thread::current().name()
            );
        }

        self.inner.try_write().map(|guard| {
            self.exclusive_owner.store(current, Ordering::Relaxed);
            SharedLatchWriteGuard { latch: self, _guard: guard }
        })
    }

    /// Acquires the latch for shared/read access.
    ///
    /// In exclusive-only mode, this is equivalent to `acquire_exclusive()`
    /// and returns a write guard wrapped in the enum.
    ///
    /// # Panics
    ///
    /// Panics if the latch is already held by the calling thread, or if the
    /// acquisition times out.
    pub fn acquire_shared(&self) -> SharedLatchGuard<'_> {
        if self.exclusive_only {
            SharedLatchGuard::Write(self.acquire_exclusive())
        } else {
            // Detect reentrant shared acquisition on the same thread. This
            // matches JE's SharedLatchImpl behavior: a thread must not acquire
            // the latch in shared mode more than once (reentrancy is forbidden
            // to prevent subtle ordering bugs).
            if read_hold_count() > 0 {
                panic!(
                    "Latch already held in shared mode: {} (thread {:?})",
                    self.context.name,
                    thread::current().name()
                );
            }

            let timeout = self.context.timeout;
            let guard =
                self.inner.try_read_for(timeout).unwrap_or_else(|| {
                    panic!(
                        "Latch acquisition timed out after {}ms: {}",
                        timeout.as_millis(),
                        self.context.name
                    )
                });
            increment_read_hold();
            SharedLatchGuard::Read(SharedLatchReadGuard { _guard: guard })
        }
    }

    /// Returns true if the current thread holds this latch exclusively.
    pub fn is_exclusive_owner(&self) -> bool {
        self.exclusive_owner.load(Ordering::Relaxed) == thread_id()
    }

    /// Returns the context for this latch.
    pub fn context(&self) -> &LatchContext {
        &self.context
    }
}

impl fmt::Debug for SharedLatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "SharedLatch({}, exclusive_only={})",
            self.context.name, self.exclusive_only
        )
    }
}

/// Guard returned by shared latch operations. Can be either a read or write guard.
pub enum SharedLatchGuard<'a> {
    Read(SharedLatchReadGuard<'a>),
    Write(SharedLatchWriteGuard<'a>),
}

/// RAII guard for shared/read access. Releases when dropped.
pub struct SharedLatchReadGuard<'a> {
    _guard: noxu_sync::RwLockReadGuard<'a, ()>,
}

impl Drop for SharedLatchReadGuard<'_> {
    fn drop(&mut self) {
        // Decrement before the inner guard drops to keep the count accurate
        // for any code that runs between our drop and the lock release.
        decrement_read_hold();
    }
}

/// RAII guard for exclusive/write access. Releases when dropped.
pub struct SharedLatchWriteGuard<'a> {
    latch: &'a SharedLatch,
    _guard: noxu_sync::RwLockWriteGuard<'a, ()>,
}

impl Drop for SharedLatchWriteGuard<'_> {
    fn drop(&mut self) {
        self.latch.exclusive_owner.store(0, Ordering::Relaxed);
    }
}

/// Returns a unique identifier for the current thread.
fn thread_id() -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    thread::current().id().hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_shared_access() {
        let latch = Arc::new(SharedLatch::named("test", false));

        // Multiple readers should be able to acquire simultaneously
        let _guard1 = latch.acquire_shared();
        let latch2 = latch.clone();
        let handle = std::thread::spawn(move || {
            let _guard = latch2.acquire_shared();
            true
        });
        assert!(handle.join().unwrap());
    }

    #[test]
    fn test_exclusive_blocks_shared() {
        let latch = Arc::new(SharedLatch::named("test", false));
        let _guard = latch.acquire_exclusive();
        assert!(latch.is_exclusive_owner());

        // Another thread should not be able to acquire
        let latch2 = latch.clone();
        let handle = std::thread::spawn(move || {
            latch2.try_acquire_exclusive().is_none()
        });
        assert!(handle.join().unwrap());
    }

    #[test]
    fn test_exclusive_only_mode() {
        let latch = SharedLatch::named("bin-latch", true);
        assert!(latch.is_exclusive_only());

        // acquire_shared should actually acquire exclusive
        let guard = latch.acquire_shared();
        match guard {
            SharedLatchGuard::Write(_) => {} // Expected
            SharedLatchGuard::Read(_) => {
                panic!("Expected write guard in exclusive-only mode")
            }
        }
    }

    #[test]
    #[should_panic(expected = "Latch already held")]
    fn test_reentrant_exclusive_panics() {
        let latch = SharedLatch::named("test", false);
        let _guard = latch.acquire_exclusive();
        let _guard2 = latch.acquire_exclusive(); // Should panic
    }

    #[test]
    #[should_panic(expected = "Deadlock")]
    fn test_read_to_write_upgrade_panics() {
        // Acquiring a read guard then trying to upgrade to write must panic,
        // matching JE's EnvironmentFailureException on getReadHoldCount() > 0.
        let latch = SharedLatch::named("test-upgrade", false);
        let _rguard = latch.acquire_shared();
        // This thread now holds a read guard -- exclusive acquire must panic.
        let _wguard = latch.acquire_exclusive();
    }

    #[test]
    #[should_panic(expected = "timed out")]
    fn test_exclusive_acquire_timeout() {
        use std::time::Duration;
        // Create a latch with a very short timeout so the test completes fast.
        let ctx = crate::LatchContext::with_timeout("test-timeout", Duration::from_millis(50));
        let latch = Arc::new(SharedLatch::new(ctx, false));

        // Hold write lock from another thread so this thread will time out.
        let latch2 = latch.clone();
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let barrier2 = barrier.clone();
        let handle = std::thread::spawn(move || {
            let _g = latch2.acquire_exclusive();
            barrier2.wait(); // signal: lock is held
            std::thread::sleep(Duration::from_millis(200));
        });

        barrier.wait(); // wait until the other thread holds the lock
        // Now this will try to acquire with a 50ms timeout and must panic.
        let _g = latch.acquire_exclusive();
        let _ = handle.join();
    }

    #[test]
    #[should_panic(expected = "timed out")]
    fn test_shared_acquire_timeout() {
        use std::time::Duration;
        let ctx = crate::LatchContext::with_timeout("test-timeout-r", Duration::from_millis(50));
        let latch = Arc::new(SharedLatch::new(ctx, false));

        let latch2 = latch.clone();
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let barrier2 = barrier.clone();
        let handle = std::thread::spawn(move || {
            let _g = latch2.acquire_exclusive();
            barrier2.wait();
            std::thread::sleep(Duration::from_millis(200));
        });

        barrier.wait();
        // Shared acquire should time out while write lock is held.
        let _g = latch.acquire_shared();
        let _ = handle.join();
    }

    #[test]
    fn test_is_not_exclusive_owner_when_not_held() {
        let latch = SharedLatch::named("test-owner", false);
        assert!(!latch.is_exclusive_owner());
    }

    #[test]
    fn test_is_exclusive_owner_only_in_owning_thread() {
        let latch = Arc::new(SharedLatch::named("test-owner-thread", false));
        let _guard = latch.acquire_exclusive();
        assert!(latch.is_exclusive_owner());

        let latch2 = latch.clone();
        let handle = std::thread::spawn(move || {
            assert!(!latch2.is_exclusive_owner(), "non-owner should not be owner");
        });
        handle.join().unwrap();
    }

    #[test]
    fn test_exclusive_owner_cleared_after_drop() {
        let latch = SharedLatch::named("test-drop", false);
        {
            let _guard = latch.acquire_exclusive();
            assert!(latch.is_exclusive_owner());
        }
        assert!(!latch.is_exclusive_owner());
    }

    #[test]
    fn test_context_fields() {
        use std::time::Duration;
        let ctx = crate::LatchContext::with_timeout("ctx-test", Duration::from_secs(3));
        let latch = SharedLatch::new(ctx, false);
        assert_eq!(latch.context().name, "ctx-test");
        assert_eq!(latch.context().timeout, Duration::from_secs(3));
    }

    #[test]
    fn test_debug_format() {
        let latch = SharedLatch::named("debug-test", true);
        let s = format!("{:?}", latch);
        assert!(s.contains("debug-test"));
        assert!(s.contains("exclusive_only=true"));
    }

    #[test]
    fn test_try_acquire_exclusive_blocks_shared() {
        let latch = Arc::new(SharedLatch::named("try-excl-blocks", false));
        let guard = latch.try_acquire_exclusive();
        assert!(guard.is_some());
        assert!(latch.is_exclusive_owner());

        // Another thread try_acquire_exclusive should see None
        let latch2 = latch.clone();
        let handle = std::thread::spawn(move || {
            latch2.try_acquire_exclusive().is_none()
        });
        assert!(handle.join().unwrap());
        drop(guard);
        assert!(!latch.is_exclusive_owner());
    }

    #[test]
    fn test_concurrent_exclusive_serializes() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let latch = Arc::new(SharedLatch::named("concurrent-serial", false));
        let counter = Arc::new(AtomicUsize::new(0));
        let concurrent = Arc::new(AtomicUsize::new(0));
        let violations = Arc::new(AtomicUsize::new(0));

        let threads: Vec<_> = (0..4).map(|_| {
            let latch = latch.clone();
            let counter = counter.clone();
            let concurrent = concurrent.clone();
            let violations = violations.clone();
            std::thread::spawn(move || {
                for _ in 0..25 {
                    let _guard = latch.acquire_exclusive();
                    let prev = concurrent.fetch_add(1, Ordering::SeqCst);
                    if prev != 0 {
                        violations.fetch_add(1, Ordering::SeqCst);
                    }
                    counter.fetch_add(1, Ordering::SeqCst);
                    concurrent.fetch_sub(1, Ordering::SeqCst);
                }
            })
        }).collect();

        for t in threads { t.join().unwrap(); }
        assert_eq!(counter.load(Ordering::SeqCst), 100);
        assert_eq!(violations.load(Ordering::SeqCst), 0, "mutual exclusion violated");
    }

    // -----------------------------------------------------------------------
    // Ported from LatchTest.java — shared/exclusive latch invariants
    // -----------------------------------------------------------------------

    /// Port of LatchTest.testAcquireAndReacquireShared: re-acquiring a shared
    /// latch on the same thread should panic (reentrancy prevention).
    #[test]
    fn test_je_shared_reacquire_panics() {
        let result = std::panic::catch_unwind(|| {
            let latch = SharedLatch::named("je-shared-reacquire", false);
            let _g1 = latch.acquire_shared();
            // Second shared acquire on same thread must panic.
            let _g2 = latch.acquire_shared();
        });
        assert!(result.is_err(), "reentrant shared acquire should panic");
    }

    /// Port of LatchTest.testAcquireAndReacquireShared: acquiring exclusively
    /// after a shared guard is held on the same thread must panic (would deadlock).
    #[test]
    fn test_je_read_to_write_upgrade_panics() {
        let result = std::panic::catch_unwind(|| {
            let latch = SharedLatch::named("je-rwupgrade", false);
            let _rg = latch.acquire_shared(); // increments read hold count
            let _wg = latch.acquire_exclusive(); // must panic
        });
        assert!(result.is_err(), "read-to-write upgrade should panic");
    }

    /// Port of LatchTest.testAcquireAndReacquireShared: releasing a latch that
    /// is not held (release_if_owner style) should be safe on exclusive path.
    #[test]
    fn test_je_shared_release_not_held_exclusive_path() {
        let latch = SharedLatch::named("je-not-held", false);
        // Not held at all — is_exclusive_owner should be false.
        assert!(!latch.is_exclusive_owner());
    }

    /// Port of LatchTest: multiple threads can hold shared guards simultaneously
    /// while no exclusive holder is present.
    #[test]
    fn test_je_multiple_readers_concurrent() {
        let latch = Arc::new(SharedLatch::named("je-multi-read", false));
        let ready = Arc::new((noxu_sync::Mutex::new(0usize), noxu_sync::Condvar::new()));
        let mut handles = Vec::new();

        for _ in 0..4 {
            let latch2 = latch.clone();
            let ready2 = ready.clone();
            let h = std::thread::spawn(move || {
                let _g = latch2.acquire_shared();
                {
                    let (m, cv) = &*ready2;
                    let mut g = m.lock();
                    *g += 1;
                    cv.notify_all();
                }
                // Hold shared a bit.
                std::thread::sleep(std::time::Duration::from_millis(20));
            });
            handles.push(h);
        }

        // Wait until all four have acquired.
        {
            let (m, cv) = &*ready;
            let mut g = m.lock();
            while *g < 4 {
                cv.wait(&mut g);
            }
        }
        // All four threads hold shared concurrently — verified by no timeout.
        for h in handles {
            h.join().unwrap();
        }
    }

    /// Port of LatchTest: exclusive blocks shared; after exclusive releases
    /// shared can be acquired.
    #[test]
    fn test_je_exclusive_blocks_then_shared_granted() {
        let latch = Arc::new(SharedLatch::named("je-excl-blocks-shared", false));

        // Acquire exclusive.
        let g = latch.acquire_exclusive();
        assert!(latch.is_exclusive_owner());

        let latch2 = latch.clone();
        let acquired = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let acquired2 = acquired.clone();
        let h = std::thread::spawn(move || {
            let _sg = latch2.acquire_shared();
            acquired2.store(true, std::sync::atomic::Ordering::SeqCst);
        });

        std::thread::sleep(std::time::Duration::from_millis(30));
        // Shared should not be granted yet.
        assert!(!acquired.load(std::sync::atomic::Ordering::SeqCst));

        drop(g); // release exclusive
        h.join().unwrap();
        assert!(acquired.load(std::sync::atomic::Ordering::SeqCst));
    }

    /// Port of LatchTest: try_acquire_exclusive (non-blocking) returns None
    /// while an exclusive holder is present.
    #[test]
    fn test_je_try_acquire_exclusive_no_wait() {
        let latch = Arc::new(SharedLatch::named("je-try-excl", false));
        let barrier = Arc::new(std::sync::Barrier::new(2));

        let latch2 = latch.clone();
        let barrier2 = barrier.clone();
        let h = std::thread::spawn(move || {
            let _g = latch2.acquire_exclusive();
            barrier2.wait();
            std::thread::sleep(std::time::Duration::from_millis(100));
        });

        barrier.wait();
        // try_acquire should fail while other thread holds exclusive.
        let r = latch.try_acquire_exclusive();
        assert!(r.is_none(), "try_acquire_exclusive should fail while held");
        h.join().unwrap();

        // Now it should succeed.
        let r2 = latch.try_acquire_exclusive();
        assert!(r2.is_some(), "try_acquire_exclusive should succeed after release");
        drop(r2);
    }

    /// Port of LatchTest: exclusive latch in exclusive-only mode behaves like
    /// a plain exclusive latch (shared acquisition acts as exclusive).
    #[test]
    fn test_je_exclusive_only_mode_serializes() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let latch = Arc::new(SharedLatch::named("je-excl-only", true));
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
                    for _ in 0..10 {
                        let _g = latch.acquire_shared(); // exclusive in excl-only mode
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
        assert_eq!(counter.load(Ordering::SeqCst), 40);
        assert_eq!(violations.load(Ordering::SeqCst), 0, "exclusive-only must serialize");
    }
}
