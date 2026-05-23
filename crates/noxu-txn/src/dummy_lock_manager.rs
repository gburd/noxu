//! No-op lock manager for non-locking mode.
//!

use std::sync::Arc;

use crate::{LockGrantType, LockManager, LockStats, LockType, TxnError};

/// A no-op lock manager that always grants locks immediately.
///
/// Used when locking is disabled (`isNoLocking()` is true in the).  When a
/// locker requires locking (i.e. it is an internal-DB cursor), requests are
/// forwarded to the `superior_lock_manager` instead of being no-op'd.
///
/// `DummyLockManager` wraps the real `SyncedLockManager` as its
/// `superiorLockManager`.  `attemptLock()` delegates to the real LM when
/// `locker.lockingRequired()` is true; otherwise returns `NEW` immediately.
///
///
pub struct DummyLockManager {
    /// The real lock manager, used when `locking_required` is true.
    ///
    superior: Arc<LockManager>,
}

impl DummyLockManager {
    /// Creates a new DummyLockManager backed by the given real lock manager.
    ///
    ///
    pub fn new(superior: Arc<LockManager>) -> Self {
        DummyLockManager { superior }
    }

    /// Returns the underlying real lock manager.
    pub fn superior_lock_manager(&self) -> &Arc<LockManager> {
        &self.superior
    }

    /// Attempts a lock, delegating to the superior LM when `locking_required`.
    ///
    /// `DummyLockManager.attemptLock(lsn, locker, type, ...)`:
    ///   - if `locker.lockingRequired()` → delegate to `superiorLockManager.lock()`
    ///   - else → return `LockGrantType::NEW` immediately.
    ///
    /// The `locking_required` parameter mirrors `locker.lockingRequired()`.
    ///
    ///
    pub fn lock(
        &self,
        lsn: u64,
        locker_id: i64,
        lock_type: LockType,
        non_blocking: bool,
        jump_ahead_of_waiters: bool,
        locking_required: bool,
    ) -> Result<LockGrantType, TxnError> {
        if lock_type == LockType::None {
            return Ok(LockGrantType::NoneNeeded);
        }
        if lock_type == LockType::Restart {
            return Err(TxnError::RangeRestart);
        }

        if locking_required {
            // Delegate to the real lock manager for internal-DB cursors.
            self.superior.lock(
                lsn,
                locker_id,
                lock_type,
                non_blocking,
                jump_ahead_of_waiters,
            )
        } else {
            Ok(LockGrantType::New)
        }
    }

    /// Releases a lock, delegating to superior LM when `locking_required`.
    ///
    /// unconditional
    /// delegation in for release.
    pub fn release(
        &self,
        lsn: u64,
        locker_id: i64,
        locking_required: bool,
    ) -> Result<(), TxnError> {
        if locking_required {
            self.superior.release(lsn, locker_id)
        } else {
            Ok(())
        }
    }

    /// Demotes a write lock to read, delegating when `locking_required`.
    ///
    ///
    pub fn demote(
        &self,
        lsn: u64,
        locker_id: i64,
        locking_required: bool,
    ) -> Result<(), TxnError> {
        if locking_required {
            self.superior.demote(lsn, locker_id)
        } else {
            Ok(())
        }
    }

    /// Steals a lock, delegating when `locking_required`.
    ///
    ///
    pub fn steal_lock(
        &self,
        lsn: u64,
        locker_id: i64,
        locking_required: bool,
    ) -> Result<(), TxnError> {
        if locking_required {
            self.superior.steal_lock(lsn, locker_id)
        } else {
            Ok(())
        }
    }

    /// Returns write-lock ownership, delegating when `locking_required`.
    ///
    ///
    pub fn is_owned_write_lock(
        &self,
        lsn: u64,
        locker_id: i64,
        locking_required: bool,
    ) -> bool {
        if locking_required {
            self.superior.is_owned_write_lock(lsn, locker_id)
        } else {
            false
        }
    }

    /// Returns owned lock type, delegating when `locking_required`.
    ///
    ///
    pub fn get_owned_lock_type(
        &self,
        lsn: u64,
        locker_id: i64,
        locking_required: bool,
    ) -> Option<LockType> {
        if locking_required {
            self.superior.get_owned_lock_type(lsn, locker_id)
        } else {
            None
        }
    }

    /// Returns lock info from superior when `locking_required`, else (0,0).
    pub fn get_lock_info(
        &self,
        lsn: u64,
        locking_required: bool,
    ) -> (usize, usize) {
        if locking_required { self.superior.get_lock_info(lsn) } else { (0, 0) }
    }

    /// Returns stats from the superior lock manager.
    ///
    ///
    pub fn get_stats(&self) -> LockStats {
        self.superior.get_stats()
    }

    /// Returns the total number of locks in the superior lock manager.
    pub fn n_total_locks(&self) -> usize {
        self.superior.n_total_locks()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_dlm() -> DummyLockManager {
        let lm = Arc::new(LockManager::new());
        DummyLockManager::new(lm)
    }

    #[test]
    fn test_all_locks_granted_no_locking_required() {
        let dlm = make_dlm();
        // locking_required=false: all types return New immediately.
        assert_eq!(
            dlm.lock(100, 1, LockType::Read, false, false, false).unwrap(),
            LockGrantType::New
        );
        assert_eq!(
            dlm.lock(100, 2, LockType::Write, false, false, false).unwrap(),
            LockGrantType::New
        );
        assert_eq!(
            dlm.lock(100, 3, LockType::RangeRead, false, false, false).unwrap(),
            LockGrantType::New
        );
        assert_eq!(
            dlm.lock(100, 4, LockType::RangeWrite, false, false, false)
                .unwrap(),
            LockGrantType::New
        );
        assert_eq!(
            dlm.lock(100, 5, LockType::RangeInsert, false, false, false)
                .unwrap(),
            LockGrantType::New
        );
    }

    #[test]
    fn test_no_conflicts_when_not_locking_required() {
        let dlm = make_dlm();
        // Multiple lockers, no locking_required — all granted without conflict.
        assert_eq!(
            dlm.lock(100, 1, LockType::Write, false, false, false).unwrap(),
            LockGrantType::New
        );
        assert_eq!(
            dlm.lock(100, 2, LockType::Write, false, false, false).unwrap(),
            LockGrantType::New
        );
    }

    #[test]
    fn test_delegates_to_superior_when_locking_required() {
        let dlm = make_dlm();
        // locking_required=true: delegates to real LM, which detects conflicts.
        assert_eq!(
            dlm.lock(100, 1, LockType::Write, false, false, true).unwrap(),
            LockGrantType::New
        );
        // Second writer should be blocked (non-blocking → LockNotAvailable).
        let result = dlm.lock(100, 2, LockType::Write, true, false, true);
        assert!(result.is_err());
    }

    #[test]
    fn test_lock_type_none() {
        let dlm = make_dlm();
        let result = dlm.lock(100, 1, LockType::None, false, false, false);
        assert_eq!(result.unwrap(), LockGrantType::NoneNeeded);
    }

    #[test]
    fn test_lock_type_restart() {
        let dlm = make_dlm();
        let result = dlm.lock(100, 1, LockType::Restart, false, false, false);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), TxnError::RangeRestart));
    }

    #[test]
    fn test_release_no_locking() {
        let dlm = make_dlm();
        dlm.lock(100, 1, LockType::Write, false, false, false).unwrap();
        assert!(dlm.release(100, 1, false).is_ok());
        assert!(dlm.release(200, 2, false).is_ok());
    }

    #[test]
    fn test_demote_no_locking() {
        let dlm = make_dlm();
        assert!(dlm.demote(100, 1, false).is_ok());
    }

    #[test]
    fn test_steal_lock_no_locking() {
        let dlm = make_dlm();
        assert!(dlm.steal_lock(100, 1, false).is_ok());
    }

    #[test]
    fn test_is_owned_write_lock_no_locking() {
        let dlm = make_dlm();
        dlm.lock(100, 1, LockType::Write, false, false, false).unwrap();
        // locking_required=false: no tracking, always false.
        assert!(!dlm.is_owned_write_lock(100, 1, false));
    }

    #[test]
    fn test_get_owned_lock_type_no_locking() {
        let dlm = make_dlm();
        dlm.lock(100, 1, LockType::Read, false, false, false).unwrap();
        assert_eq!(dlm.get_owned_lock_type(100, 1, false), None);
    }

    #[test]
    fn test_get_lock_info_no_locking() {
        let dlm = make_dlm();
        dlm.lock(100, 1, LockType::Write, false, false, false).unwrap();
        assert_eq!(dlm.get_lock_info(100, false), (0, 0));
    }

    #[test]
    fn test_get_stats() {
        let dlm = make_dlm();
        // Stats come from the superior LM.
        let stats = dlm.get_stats();
        assert_eq!(stats.lock_requests, 0);
    }

    #[test]
    fn test_non_blocking_mode() {
        let dlm = make_dlm();
        assert_eq!(
            dlm.lock(100, 1, LockType::Write, true, false, false).unwrap(),
            LockGrantType::New
        );
    }

    #[test]
    fn test_superior_lock_manager() {
        let lm = Arc::new(LockManager::new());
        let dlm = DummyLockManager::new(lm.clone());
        assert!(Arc::ptr_eq(dlm.superior_lock_manager(), &lm));
    }
}
