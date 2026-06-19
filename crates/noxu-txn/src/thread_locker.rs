//! ThreadLocker - per-thread locker.
//!

use hashbrown::HashSet;
use std::sync::Arc;
use std::thread;

use crate::lock_manager::LockManager;
use crate::locker::Locker;
use crate::{LockResult, LockType, TxnError};

/// A thread-based locker that shares locks with other ThreadLockers
/// on the same thread.
///
/// ThreadLocker extends BasicLocker to track which thread created it.
/// All ThreadLockers on the same thread share locks with each other,
/// which allows multiple cursors to operate without lock conflicts.
///
/// This is used for auto-commit operations where a transaction context
/// is not explicitly provided.
///
///
pub struct ThreadLocker {
    /// Unique locker ID.
    id: i64,

    /// Shared lock manager.
    lock_manager: Arc<LockManager>,

    /// Thread ID that created this locker (hashed for stable u64 representation).
    ///
    /// All ThreadLockers on the same thread share the same `thread_id` and
    /// therefore share locks with each other.
    thread_id: u64,

    /// Set of LSNs currently locked by this locker.
    locked_lsns: HashSet<u64>,

    /// Lock timeout in milliseconds (0 = infinite).
    lock_timeout_ms: u64,

    /// Whether this locker uses non-blocking locks by default.
    default_no_wait: bool,

    /// Whether this locker is open.
    is_open: bool,
}

impl ThreadLocker {
    /// Creates a new ThreadLocker for the current thread.
    ///
    /// Registers this locker's thread ID in the LockManager's sharing registry
    /// so that `LockImpl::try_lock()` can bypass conflict detection for co-owning
    /// ThreadLockers on the same thread.
    ///
    /// # Arguments
    /// * `id` - Unique locker ID
    /// * `lock_manager` - Shared lock manager
    pub fn new(id: i64, lock_manager: Arc<LockManager>) -> Self {
        let tid = get_thread_id();
        lock_manager.register_locker_sharing(id, tid as i64);
        ThreadLocker {
            id,
            lock_manager,
            thread_id: tid,
            locked_lsns: HashSet::new(),
            lock_timeout_ms: 5000, // Default 5 second timeout
            default_no_wait: false,
            is_open: true,
        }
    }

    /// Creates a ThreadLocker with a specified timeout.
    pub fn with_timeout(
        id: i64,
        lock_manager: Arc<LockManager>,
        timeout_ms: u64,
    ) -> Self {
        let tid = get_thread_id();
        lock_manager.register_locker_sharing(id, tid as i64);
        ThreadLocker {
            id,
            lock_manager,
            thread_id: tid,
            locked_lsns: HashSet::new(),
            lock_timeout_ms: timeout_ms,
            default_no_wait: false,
            is_open: true,
        }
    }

    /// Returns the thread ID that created this locker.
    pub fn get_thread_id(&self) -> u64 {
        self.thread_id
    }

    /// Release all locks held by this locker.
    pub fn release_all_locks(&mut self) -> Result<(), TxnError> {
        for &lsn in &self.locked_lsns {
            self.lock_manager.release(lsn, self.id)?;
        }
        self.locked_lsns.clear();
        Ok(())
    }

    /// Sets the lock timeout.
    pub fn set_lock_timeout(&mut self, timeout_ms: u64) {
        self.lock_timeout_ms = timeout_ms;
    }

    /// Checks that this locker is being used by the correct thread.
    fn check_thread(&self) -> Result<(), TxnError> {
        let current_thread = get_thread_id();
        if current_thread != self.thread_id {
            return Err(TxnError::StateError(format!(
                "ThreadLocker created on thread {} but used on thread {}",
                self.thread_id, current_thread
            )));
        }
        Ok(())
    }
}

impl Locker for ThreadLocker {
    fn id(&self) -> i64 {
        self.id
    }

    fn lock(
        &mut self,
        lsn: u64,
        lock_type: LockType,
        non_blocking: bool,
    ) -> Result<LockResult, TxnError> {
        if !self.is_open {
            return Err(TxnError::StateError("Locker is closed".to_string()));
        }

        // Check that we're on the right thread
        self.check_thread()?;

        // Use non_blocking parameter or default
        let use_no_wait = non_blocking || self.default_no_wait;

        // Ask the lock manager for the lock
        let grant = self.lock_manager.lock(
            lsn,
            self.id,
            lock_type,
            use_no_wait,
            false, // jump_ahead
        )?;

        // Track this lock
        if grant.is_granted() {
            self.locked_lsns.insert(lsn);
        }

        // ThreadLocker doesn't track write lock info (non-transactional)
        Ok(LockResult::simple(grant))
    }

    fn release_lock(&mut self, lsn: u64) -> Result<(), TxnError> {
        if self.locked_lsns.remove(&lsn) {
            self.lock_manager.release(lsn, self.id)?;
        }
        Ok(())
    }

    fn owns_write_lock(&self, lsn: u64) -> bool {
        self.lock_manager.is_owned_write_lock(lsn, self.id)
    }

    fn is_transactional(&self) -> bool {
        false
    }

    fn lock_timeout_ms(&self) -> u64 {
        self.lock_timeout_ms
    }

    fn default_no_wait(&self) -> bool {
        self.default_no_wait
    }

    /// Returns true if the other locker was created on the same thread.
    ///
    /// Both lockers must be
    /// ThreadLockers **and** have the same originating thread for sharing.
    /// We check via the LockManager's sharing registry (locker_id → thread_id).
    fn shares_locks_with(&self, other_id: i64) -> bool {
        self.lock_manager.same_share_group(self.id, other_id)
    }

    fn close(&mut self) {
        self.is_open = false;
        let _ = self.release_all_locks();
    }

    fn is_open(&self) -> bool {
        self.is_open
    }
}

impl Drop for ThreadLocker {
    fn drop(&mut self) {
        // Ensure locks are released when locker is dropped.
        let _ = self.release_all_locks();
        // Deregister from the sharing registry.
        self.lock_manager.unregister_locker_sharing(self.id);
    }
}

/// Gets a stable thread ID for the current thread.
///
/// Since ThreadId::as_u64() is unstable, we use a hash of the thread ID.
fn get_thread_id() -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let thread_id = thread::current().id();
    let mut hasher = DefaultHasher::new();
    thread_id.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup() -> (Arc<LockManager>, ThreadLocker) {
        let lm = Arc::new(LockManager::new());
        let locker = ThreadLocker::new(1, lm.clone());
        (lm, locker)
    }

    #[test]
    fn test_new() {
        let (_, locker) = setup();
        assert_eq!(locker.id(), 1);
        assert!(!locker.is_transactional());
        assert!(locker.is_open());
        assert!(locker.get_thread_id() > 0);
    }

    #[test]
    fn test_lock_and_release() {
        let (_, mut locker) = setup();

        // Acquire a write lock
        let result = locker.lock(100, LockType::Write, false).unwrap();
        assert!(result.is_granted());

        // Check that we own the lock
        assert!(locker.owns_write_lock(100));

        // Release the lock
        locker.release_lock(100).unwrap();
        assert!(!locker.owns_write_lock(100));
    }

    #[test]
    fn test_release_all_locks() {
        let (_, mut locker) = setup();

        // Acquire multiple locks
        locker.lock(100, LockType::Write, false).unwrap();
        locker.lock(200, LockType::Write, false).unwrap();

        locker.release_all_locks().unwrap();

        assert!(!locker.owns_write_lock(100));
        assert!(!locker.owns_write_lock(200));
    }

    #[test]
    fn test_close_releases_locks() {
        let (_, mut locker) = setup();

        locker.lock(100, LockType::Write, false).unwrap();
        assert!(locker.is_open());

        locker.close();
        assert!(!locker.is_open());
        assert!(!locker.owns_write_lock(100));
    }

    #[test]
    fn test_same_thread_check() {
        let (_, mut locker) = setup();
        // Should succeed on same thread
        let result = locker.lock(100, LockType::Write, false);
        assert!(result.is_ok());
    }

    #[test]
    fn test_get_thread_id() {
        let id1 = get_thread_id();
        let id2 = get_thread_id();
        // Same thread should have same ID
        assert_eq!(id1, id2);
    }

    /// TXN-F2 regression: two ThreadLockers on the SAME thread share locks
    /// (LockManager registers them in the same sharing group, keyed by
    /// thread id).  JE `LockImpl.tryLock` consults
    /// `locker.sharesLocksWith(ownerLocker)` on EVERY acquisition
    /// (LockImpl.java:647-648), so a conflicting (Write/Write) request from a
    /// sibling ThreadLocker must be co-granted, not blocked.
    ///
    /// Before the fix, `LockManager::lock` hard-wired sharing off, so the
    /// second locker self-conflicted: with non_blocking it was denied
    /// (LockNotAvailable); with blocking it self-deadlocked / timed out.
    #[test]
    fn test_two_thread_lockers_same_thread_share_write_lock() {
        let lm = Arc::new(LockManager::new());
        let mut a = ThreadLocker::new(1, lm.clone());
        let mut b = ThreadLocker::new(2, lm);
        // Same thread => same sharing group.
        assert_eq!(a.get_thread_id(), b.get_thread_id());

        // First write lock: granted.
        let ra = a.lock(100, LockType::Write, false).unwrap();
        assert!(ra.is_granted());

        // Sibling write lock on the SAME LSN.  non_blocking=true so a
        // regression surfaces as an immediate LockNotAvailable instead of a
        // 5s timeout hang.  With sharing honored, this must be GRANTED.
        let rb = b
            .lock(100, LockType::Write, true)
            .expect("sibling ThreadLocker must co-own the write lock");
        assert!(rb.is_granted());
    }
}
