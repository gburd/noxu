//! No-op lock manager for non-locking mode.
//!
//! Port of `com.sleepycat.je.txn.DummyLockManager` (273 lines).

use crate::{LockGrantType, LockStats, LockType, TxnError};

/// A no-op lock manager that always grants locks immediately.
///
/// Used when locking is disabled (e.g., for read-only environments or
/// when the application manages its own concurrency control).
///
/// All operations succeed immediately without any actual locking or
/// conflict detection.
///
/// Port of `com.sleepycat.je.txn.DummyLockManager`.
pub struct DummyLockManager;

impl DummyLockManager {
    /// Creates a new DummyLockManager.
    pub fn new() -> Self {
        DummyLockManager
    }

    /// Always grants locks immediately without checking for conflicts.
    ///
    /// # Arguments
    ///
    /// * `_lsn` - The LSN to lock (ignored)
    /// * `_locker_id` - The ID of the requesting locker (ignored)
    /// * `lock_type` - The type of lock requested (only used to check for None)
    /// * `_non_blocking` - Ignored (no blocking occurs)
    /// * `_jump_ahead_of_waiters` - Ignored (no waiters exist)
    ///
    /// # Returns
    ///
    /// Always returns Ok(LockGrantType::New) or Ok(LockGrantType::NoneNeeded).
    ///
    /// Port of `DummyLockManager.lock()`.
    pub fn lock(
        &self,
        _lsn: u64,
        _locker_id: i64,
        lock_type: LockType,
        _non_blocking: bool,
        _jump_ahead_of_waiters: bool,
    ) -> Result<LockGrantType, TxnError> {
        // Handle special lock types.
        if lock_type == LockType::None {
            return Ok(LockGrantType::NoneNeeded);
        }

        if lock_type == LockType::Restart {
            return Err(TxnError::RangeRestart);
        }

        // All other lock requests succeed immediately.
        Ok(LockGrantType::New)
    }

    /// No-op release.
    ///
    /// # Returns
    ///
    /// Always returns Ok(()).
    ///
    /// Port of `DummyLockManager.release()`.
    pub fn release(&self, _lsn: u64, _locker_id: i64) -> Result<(), TxnError> {
        Ok(())
    }

    /// No-op demote.
    ///
    /// # Returns
    ///
    /// Always returns Ok(()).
    ///
    /// Port of `DummyLockManager.demote()`.
    pub fn demote(&self, _lsn: u64, _locker_id: i64) -> Result<(), TxnError> {
        Ok(())
    }

    /// No-op steal lock.
    ///
    /// # Returns
    ///
    /// Always returns Ok(()).
    ///
    /// Port of `DummyLockManager.stealLock()`.
    pub fn steal_lock(
        &self,
        _lsn: u64,
        _locker_id: i64,
    ) -> Result<(), TxnError> {
        Ok(())
    }

    /// Always returns false since no locks are actually held.
    ///
    /// # Returns
    ///
    /// Always returns false.
    ///
    /// Port of `DummyLockManager.isOwnedWriteLock()`.
    pub fn is_owned_write_lock(&self, _lsn: u64, _locker_id: i64) -> bool {
        false
    }

    /// Always returns None since no locks are actually held.
    ///
    /// # Returns
    ///
    /// Always returns None.
    ///
    /// Port of `DummyLockManager.getOwnedLockType()`.
    pub fn get_owned_lock_type(
        &self,
        _lsn: u64,
        _locker_id: i64,
    ) -> Option<LockType> {
        None
    }

    /// Returns zero for both owners and waiters.
    ///
    /// # Returns
    ///
    /// Always returns (0, 0).
    pub fn get_lock_info(&self, _lsn: u64) -> (usize, usize) {
        (0, 0)
    }

    /// Returns empty statistics.
    ///
    /// # Returns
    ///
    /// LockStats with all counters set to zero.
    ///
    /// Port of `DummyLockManager.getStats()`.
    pub fn get_stats(&self) -> LockStats {
        LockStats::new()
    }

    /// Always returns zero since no locks are tracked.
    ///
    /// # Returns
    ///
    /// Always returns 0.
    pub fn n_total_locks(&self) -> usize {
        0
    }
}

impl Default for DummyLockManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_all_locks_granted() {
        let dlm = DummyLockManager::new();

        // All lock types succeed (except None and Restart).
        assert_eq!(
            dlm.lock(100, 1, LockType::Read, false, false).unwrap(),
            LockGrantType::New
        );
        assert_eq!(
            dlm.lock(100, 2, LockType::Write, false, false).unwrap(),
            LockGrantType::New
        );
        assert_eq!(
            dlm.lock(100, 3, LockType::RangeRead, false, false).unwrap(),
            LockGrantType::New
        );
        assert_eq!(
            dlm.lock(100, 4, LockType::RangeWrite, false, false).unwrap(),
            LockGrantType::New
        );
        assert_eq!(
            dlm.lock(100, 5, LockType::RangeInsert, false, false).unwrap(),
            LockGrantType::New
        );
    }

    #[test]
    fn test_no_conflicts() {
        let dlm = DummyLockManager::new();

        // Multiple lockers can "hold" the same lock simultaneously.
        assert_eq!(
            dlm.lock(100, 1, LockType::Write, false, false).unwrap(),
            LockGrantType::New
        );
        assert_eq!(
            dlm.lock(100, 2, LockType::Write, false, false).unwrap(),
            LockGrantType::New
        );

        // No conflicts detected.
    }

    #[test]
    fn test_lock_type_none() {
        let dlm = DummyLockManager::new();

        let result = dlm.lock(100, 1, LockType::None, false, false);
        assert_eq!(result.unwrap(), LockGrantType::NoneNeeded);
    }

    #[test]
    fn test_lock_type_restart() {
        let dlm = DummyLockManager::new();

        let result = dlm.lock(100, 1, LockType::Restart, false, false);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), TxnError::RangeRestart));
    }

    #[test]
    fn test_release() {
        let dlm = DummyLockManager::new();

        // Lock and release always succeed.
        dlm.lock(100, 1, LockType::Write, false, false).unwrap();
        assert!(dlm.release(100, 1).is_ok());

        // Can release non-existent lock (no-op).
        assert!(dlm.release(200, 2).is_ok());
    }

    #[test]
    fn test_demote() {
        let dlm = DummyLockManager::new();

        // Demote always succeeds (even if no lock exists).
        assert!(dlm.demote(100, 1).is_ok());
    }

    #[test]
    fn test_steal_lock() {
        let dlm = DummyLockManager::new();

        // Steal always succeeds (even if no lock exists).
        assert!(dlm.steal_lock(100, 1).is_ok());
    }

    #[test]
    fn test_is_owned_write_lock() {
        let dlm = DummyLockManager::new();

        // Always returns false (no locks tracked).
        dlm.lock(100, 1, LockType::Write, false, false).unwrap();
        assert!(!dlm.is_owned_write_lock(100, 1));
    }

    #[test]
    fn test_get_owned_lock_type() {
        let dlm = DummyLockManager::new();

        // Always returns None (no locks tracked).
        dlm.lock(100, 1, LockType::Read, false, false).unwrap();
        assert_eq!(dlm.get_owned_lock_type(100, 1), None);
    }

    #[test]
    fn test_get_lock_info() {
        let dlm = DummyLockManager::new();

        // Always returns (0, 0).
        dlm.lock(100, 1, LockType::Write, false, false).unwrap();
        assert_eq!(dlm.get_lock_info(100), (0, 0));
    }

    #[test]
    fn test_get_stats() {
        let dlm = DummyLockManager::new();

        // Perform some operations.
        dlm.lock(100, 1, LockType::Write, false, false).unwrap();
        dlm.lock(200, 2, LockType::Read, false, false).unwrap();
        dlm.release(100, 1).unwrap();

        // Stats are always zero.
        let stats = dlm.get_stats();
        assert_eq!(stats.lock_requests, 0);
        assert_eq!(stats.lock_waits, 0);
        assert_eq!(stats.n_owners, 0);
        assert_eq!(stats.n_waiters, 0);
    }

    #[test]
    fn test_n_total_locks() {
        let dlm = DummyLockManager::new();

        // Always returns 0.
        dlm.lock(100, 1, LockType::Write, false, false).unwrap();
        assert_eq!(dlm.n_total_locks(), 0);
    }

    #[test]
    fn test_non_blocking_mode() {
        let dlm = DummyLockManager::new();

        // Non-blocking mode works the same (always grants).
        assert_eq!(
            dlm.lock(100, 1, LockType::Write, true, false).unwrap(),
            LockGrantType::New
        );
    }
}
