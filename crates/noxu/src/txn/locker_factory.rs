//! Factory for creating lockers.
//!

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use crate::txn::basic_locker::BasicLocker;
use crate::txn::handle_locker::HandleLocker;
use crate::txn::lock_manager::LockManager;
use crate::txn::locker::Locker;
use crate::txn::thread_locker::ThreadLocker;

/// Factory for creating different types of lockers.
///
/// Manages locker ID generation and creates the appropriate locker type
/// based on the requested configuration. Each factory is associated with
/// a single LockManager instance.
///
///
pub struct LockerFactory {
    /// Atomic counter for generating unique locker IDs.
    next_id: AtomicI64,

    /// Shared lock manager used by all lockers created by this factory.
    lock_manager: Arc<LockManager>,

    /// Default lock timeout in milliseconds.
    default_timeout_ms: u64,
}

impl LockerFactory {
    /// Creates a new LockerFactory.
    ///
    /// # Arguments
    /// * `lock_manager` - Shared lock manager
    pub fn new(lock_manager: Arc<LockManager>) -> Self {
        LockerFactory {
            next_id: AtomicI64::new(1),
            lock_manager,
            default_timeout_ms: 5000, // 5 seconds
        }
    }

    /// Creates a new LockerFactory with a specified default timeout.
    pub fn with_timeout(
        lock_manager: Arc<LockManager>,
        timeout_ms: u64,
    ) -> Self {
        LockerFactory {
            next_id: AtomicI64::new(1),
            lock_manager,
            default_timeout_ms: timeout_ms,
        }
    }

    /// Generate the next unique locker ID.
    ///
    /// IDs are sequential starting from 1. different locker types
    /// have different ID generation strategies, but for now we use a
    /// simple sequential counter.
    pub fn next_id(&self) -> i64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Create a new BasicLocker.
    ///
    /// BasicLockers are non-transactional and release locks immediately
    /// when operations complete.
    pub fn create_basic_locker(&self) -> BasicLocker {
        BasicLocker::with_timeout(
            self.next_id(),
            self.lock_manager.clone(),
            self.default_timeout_ms,
        )
    }

    /// Create a new BasicLocker with a specified timeout.
    pub fn create_basic_locker_with_timeout(
        &self,
        timeout_ms: u64,
    ) -> BasicLocker {
        BasicLocker::with_timeout(
            self.next_id(),
            self.lock_manager.clone(),
            timeout_ms,
        )
    }

    /// Create a new BasicLocker with non-blocking mode.
    pub fn create_basic_locker_no_wait(&self) -> BasicLocker {
        BasicLocker::with_no_wait(self.next_id(), self.lock_manager.clone())
    }

    /// Create a new ThreadLocker.
    ///
    /// ThreadLockers are tied to the current thread and share locks with
    /// other ThreadLockers on the same thread.
    pub fn create_thread_locker(&self) -> ThreadLocker {
        ThreadLocker::with_timeout(
            self.next_id(),
            self.lock_manager.clone(),
            self.default_timeout_ms,
        )
    }

    /// Create a new ThreadLocker with a specified timeout.
    pub fn create_thread_locker_with_timeout(
        &self,
        timeout_ms: u64,
    ) -> ThreadLocker {
        ThreadLocker::with_timeout(
            self.next_id(),
            self.lock_manager.clone(),
            timeout_ms,
        )
    }

    /// Create a new HandleLocker.
    ///
    /// HandleLockers hold locks for the lifetime of a database handle.
    pub fn create_handle_locker(&self) -> HandleLocker {
        HandleLocker::with_timeout(
            self.next_id(),
            self.lock_manager.clone(),
            self.default_timeout_ms,
        )
    }

    /// Create a new HandleLocker with a specified timeout.
    pub fn create_handle_locker_with_timeout(
        &self,
        timeout_ms: u64,
    ) -> HandleLocker {
        HandleLocker::with_timeout(
            self.next_id(),
            self.lock_manager.clone(),
            timeout_ms,
        )
    }

    /// Create a new HandleLocker that shares locks with a buddy locker.
    ///
    /// This is used during database open to ensure the HandleLocker and
    /// the opening locker can both hold NameLN locks without conflict.
    pub fn create_handle_locker_with_buddy(
        &self,
        buddy: &dyn Locker,
    ) -> HandleLocker {
        HandleLocker::with_buddy(
            self.next_id(),
            self.lock_manager.clone(),
            buddy,
        )
    }

    /// Returns the default timeout in milliseconds.
    pub fn default_timeout_ms(&self) -> u64 {
        self.default_timeout_ms
    }

    /// Sets the default timeout for newly created lockers.
    pub fn set_default_timeout(&mut self, timeout_ms: u64) {
        self.default_timeout_ms = timeout_ms;
    }

    /// Returns a reference to the lock manager.
    pub fn lock_manager(&self) -> &Arc<LockManager> {
        &self.lock_manager
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup() -> LockerFactory {
        let lm = Arc::new(LockManager::new());
        LockerFactory::new(lm)
    }

    #[test]
    fn test_id_generation() {
        let factory = setup();
        let id1 = factory.next_id();
        let id2 = factory.next_id();
        let id3 = factory.next_id();

        assert_eq!(id1, 1);
        assert_eq!(id2, 2);
        assert_eq!(id3, 3);
    }

    #[test]
    fn test_id_generation_is_sequential() {
        let factory = setup();
        let ids: Vec<i64> = (0..10).map(|_| factory.next_id()).collect();

        for (i, &id) in ids.iter().enumerate() {
            assert_eq!(id, (i + 1) as i64);
        }
    }

    #[test]
    fn test_create_basic_locker() {
        let factory = setup();
        let locker = factory.create_basic_locker();

        assert_eq!(locker.id(), 1);
        assert!(!locker.is_transactional());
        assert!(locker.is_open());
    }

    #[test]
    fn test_create_thread_locker() {
        let factory = setup();
        let locker = factory.create_thread_locker();

        assert_eq!(locker.id(), 1);
        assert!(!locker.is_transactional());
        assert!(locker.is_open());
        assert!(locker.get_thread_id() > 0);
    }

    #[test]
    fn test_create_handle_locker() {
        let factory = setup();
        let locker = factory.create_handle_locker();

        assert_eq!(locker.id(), 1);
        assert!(!locker.is_transactional());
        assert!(locker.is_open());
    }

    #[test]
    fn test_unique_ids_across_types() {
        let factory = setup();

        let basic = factory.create_basic_locker();
        let thread = factory.create_thread_locker();
        let handle = factory.create_handle_locker();

        // Each locker should have a unique ID
        assert_eq!(basic.id(), 1);
        assert_eq!(thread.id(), 2);
        assert_eq!(handle.id(), 3);
    }

    #[test]
    fn test_with_timeout() {
        let lm = Arc::new(LockManager::new());
        let factory = LockerFactory::with_timeout(lm, 10000);

        assert_eq!(factory.default_timeout_ms(), 10000);

        let locker = factory.create_basic_locker();
        assert_eq!(locker.lock_timeout_ms(), 10000);
    }

    #[test]
    fn test_set_default_timeout() {
        let mut factory = setup();
        factory.set_default_timeout(15000);

        assert_eq!(factory.default_timeout_ms(), 15000);

        let locker = factory.create_basic_locker();
        assert_eq!(locker.lock_timeout_ms(), 15000);
    }

    #[test]
    fn test_create_with_custom_timeout() {
        let factory = setup();

        let locker = factory.create_basic_locker_with_timeout(20000);
        assert_eq!(locker.lock_timeout_ms(), 20000);
    }

    #[test]
    fn test_create_no_wait() {
        let factory = setup();
        let locker = factory.create_basic_locker_no_wait();

        assert!(locker.default_no_wait());
    }
}
