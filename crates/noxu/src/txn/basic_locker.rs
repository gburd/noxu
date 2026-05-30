//! BasicLocker - non-transactional locker.
//!

use hashbrown::HashSet;
use std::sync::Arc;

use crate::txn::lock_manager::LockManager;
use crate::txn::locker::Locker;
use crate::txn::{LockResult, LockType, TxnError};

/// A non-transactional locker.
///
/// Locks are released immediately when the cursor moves or closes.
/// Does not support commit/abort semantics or write lock tracking for undo.
///
/// BasicLocker is used for non-transactional database operations where
/// locks only need to be held for the duration of a single API call.
///
///
pub struct BasicLocker {
    /// Unique locker ID.
    id: i64,

    /// Shared lock manager.
    lock_manager: Arc<LockManager>,

    /// Set of LSNs currently locked by this locker.
    locked_lsns: HashSet<u64>,

    /// Lock timeout in milliseconds (0 = infinite).
    lock_timeout_ms: u64,

    /// Whether this locker uses non-blocking locks by default.
    default_no_wait: bool,

    /// Whether this locker is open.
    is_open: bool,

    /// Whether locking is required for the current cursor context.
    ///
    /// Set by `register_cursor()` based on `cursor.isInternalDbCursor()`.
    /// When false, the DummyLockManager grants locks without consulting
    /// the underlying lock table.
    ///
    ///
    locking_required: bool,
}

impl BasicLocker {
    /// Creates a new BasicLocker.
    ///
    /// # Arguments
    /// * `id` - Unique locker ID (often a shared constant for all BasicLockers)
    /// * `lock_manager` - Shared lock manager
    pub fn new(id: i64, lock_manager: Arc<LockManager>) -> Self {
        BasicLocker {
            id,
            lock_manager,
            locked_lsns: HashSet::new(),
            lock_timeout_ms: 5000, // Default 5 second timeout
            default_no_wait: false,
            is_open: true,
            locking_required: true,
        }
    }

    /// Creates a BasicLocker with a specified timeout.
    pub fn with_timeout(
        id: i64,
        lock_manager: Arc<LockManager>,
        timeout_ms: u64,
    ) -> Self {
        BasicLocker {
            id,
            lock_manager,
            locked_lsns: HashSet::new(),
            lock_timeout_ms: timeout_ms,
            default_no_wait: false,
            is_open: true,
            locking_required: true,
        }
    }

    /// Creates a BasicLocker with non-blocking mode.
    pub fn with_no_wait(id: i64, lock_manager: Arc<LockManager>) -> Self {
        BasicLocker {
            id,
            lock_manager,
            locked_lsns: HashSet::new(),
            lock_timeout_ms: 5000,
            default_no_wait: true,
            is_open: true,
            locking_required: true,
        }
    }

    /// Called by cursor open/init to configure whether locking is required.
    ///
    /// Sets `lockingRequired =
    /// !cursor.isInternalDbCursor()`.  Internal-DB cursors (e.g. the utilization
    /// DB cursor) bypass the lock table entirely.
    ///
    ///
    pub fn register_cursor(&mut self, is_internal_db_cursor: bool) {
        self.locking_required = !is_internal_db_cursor;
    }

    /// Release all locks held by this locker.
    ///
    /// Called when the locker is closed or when a cursor moves.
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

    /// Sets the default no-wait mode.
    pub fn set_default_no_wait(&mut self, no_wait: bool) {
        self.default_no_wait = no_wait;
    }
}

impl Locker for BasicLocker {
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

        // BasicLocker doesn't track write lock info (non-transactional)
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

    fn locking_required(&self) -> bool {
        self.locking_required
    }

    fn operation_end(&mut self) -> Result<(), TxnError> {
        self.release_all_locks()?;
        self.close();
        Ok(())
    }

    fn release_non_txn_locks(&mut self) -> Result<(), TxnError> {
        self.release_all_locks()
    }

    fn non_txn_operation_end(&mut self) -> Result<(), TxnError> {
        self.operation_end()
    }

    fn close(&mut self) {
        self.is_open = false;
        let _ = self.release_all_locks();
    }

    fn is_open(&self) -> bool {
        self.is_open
    }
}

impl Drop for BasicLocker {
    fn drop(&mut self) {
        // Ensure locks are released when locker is dropped
        let _ = self.release_all_locks();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup() -> (Arc<LockManager>, BasicLocker) {
        let lm = Arc::new(LockManager::new());
        let locker = BasicLocker::new(1, lm.clone());
        (lm, locker)
    }

    #[test]
    fn test_new() {
        let (_, locker) = setup();
        assert_eq!(locker.id(), 1);
        assert!(!locker.is_transactional());
        assert!(locker.is_open());
        assert_eq!(locker.lock_timeout_ms(), 5000);
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
        locker.lock(300, LockType::Read, false).unwrap();

        assert!(locker.owns_write_lock(100));
        assert!(locker.owns_write_lock(200));

        // Release all
        locker.release_all_locks().unwrap();

        assert!(!locker.owns_write_lock(100));
        assert!(!locker.owns_write_lock(200));
    }

    #[test]
    fn test_close_releases_locks() {
        let (_, mut locker) = setup();

        locker.lock(100, LockType::Write, false).unwrap();
        assert!(locker.is_open());
        assert!(locker.owns_write_lock(100));

        locker.close();
        assert!(!locker.is_open());
        assert!(!locker.owns_write_lock(100));
    }

    #[test]
    fn test_with_timeout() {
        let lm = Arc::new(LockManager::new());
        let locker = BasicLocker::with_timeout(1, lm, 10000);
        assert_eq!(locker.lock_timeout_ms(), 10000);
    }

    #[test]
    fn test_with_no_wait() {
        let lm = Arc::new(LockManager::new());
        let locker = BasicLocker::with_no_wait(1, lm);
        assert!(locker.default_no_wait());
    }

    #[test]
    fn test_set_lock_timeout() {
        let (_, mut locker) = setup();
        locker.set_lock_timeout(20000);
        assert_eq!(locker.lock_timeout_ms(), 20000);
    }

    #[test]
    fn test_lock_after_close_fails() {
        let (_, mut locker) = setup();
        locker.close();

        let result = locker.lock(100, LockType::Write, false);
        assert!(result.is_err());
        match result.unwrap_err() {
            TxnError::StateError(msg) => assert!(msg.contains("closed")),
            _ => panic!("Expected StateError"),
        }
    }
}
