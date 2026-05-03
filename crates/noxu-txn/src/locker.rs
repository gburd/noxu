//! Base Locker trait.
//!
//! Port of `com.sleepycat.je.txn.Locker`.

use crate::{LockResult, LockType, TxnError};

/// A Locker is JE's route to locking and transactional support.
///
/// This trait is the abstract base for BasicLocker, ThreadLocker, HandleLocker,
/// and Txn. Locker instances are a transaction shell to get to the lock manager,
/// and don't guarantee transactional semantics by themselves.
///
/// Only Txn (and its subclasses like MasterTxn, ReadonlyTxn) instances are
/// truly transactional with commit/abort semantics.
///
/// Port of `com.sleepycat.je.txn.Locker`.
pub trait Locker: Send + Sync {
    /// Returns the unique ID of this locker.
    ///
    /// For BasicLocker and ThreadLocker, this may be a shared constant.
    /// For Txn, this is a unique transaction ID used for recovery.
    fn id(&self) -> i64;

    /// Acquires a lock on the given LSN.
    ///
    /// This is the main locking entry point. Implementations determine
    /// how to interact with the LockManager and what to do with write locks.
    ///
    /// # Arguments
    /// * `lsn` - LSN of the record to lock
    /// * `lock_type` - Type of lock to acquire
    /// * `non_blocking` - If true, don't wait for lock (fail immediately if unavailable)
    fn lock(
        &mut self,
        lsn: u64,
        lock_type: LockType,
        non_blocking: bool,
    ) -> Result<LockResult, TxnError>;

    /// Releases a lock on the given LSN.
    ///
    /// For non-transactional lockers, this releases the lock immediately.
    /// For transactional lockers, this may defer release until commit/abort.
    fn release_lock(&mut self, lsn: u64) -> Result<(), TxnError>;

    /// Returns true if this locker owns a write lock on the given LSN.
    fn owns_write_lock(&self, lsn: u64) -> bool;

    /// Returns true if this locker is transactional (supports commit/abort).
    ///
    /// BasicLocker, ThreadLocker, and HandleLocker return false.
    /// Txn and its subclasses return true.
    fn is_transactional(&self) -> bool;

    /// Returns true if locks should be retained on commit (serializable isolation).
    ///
    /// Default is false. Txn with SERIALIZABLE isolation overrides this.
    fn retains_locks_on_commit(&self) -> bool {
        false
    }

    /// Returns the timeout for lock attempts in milliseconds.
    ///
    /// Zero means infinite timeout (wait forever).
    fn lock_timeout_ms(&self) -> u64;

    /// Returns true if this locker uses non-blocking lock requests by default.
    ///
    /// Default is false. Some specialized lockers may override this.
    fn default_no_wait(&self) -> bool {
        false
    }

    /// Returns true if this locker's locks can be preempted/stolen.
    ///
    /// Default is true. Replayer lockers in HA may steal locks from
    /// application lockers to maintain replica consistency.
    fn is_preemptable(&self) -> bool {
        true
    }

    /// Returns true if this locker can steal other lockers' locks.
    ///
    /// Default is false. Replayer lockers return true.
    fn is_importunate(&self) -> bool {
        false
    }

    /// Returns true if this locker allows read-uncommitted by default.
    ///
    /// Default is false. Can be set via isolation level configuration.
    fn is_read_uncommitted_default(&self) -> bool {
        false
    }

    /// Marks this locker as closed. After close, no operations should occur.
    ///
    /// Implementations should release any held locks and clean up resources.
    fn close(&mut self);

    /// Returns true if this locker is still open.
    fn is_open(&self) -> bool;
}

/// Helper trait for lockers that can be converted to trait objects.
///
/// This allows storing different locker types in the same collection.
pub trait LockerExt: Locker {
    /// Returns self as a trait object reference.
    fn as_locker(&self) -> &dyn Locker;

    /// Returns self as a mutable trait object reference.
    fn as_locker_mut(&mut self) -> &mut dyn Locker;
}

impl<T: Locker> LockerExt for T {
    fn as_locker(&self) -> &dyn Locker {
        self
    }

    fn as_locker_mut(&mut self) -> &mut dyn Locker {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test that trait methods have correct defaults.
    struct TestLocker {
        id: i64,
        is_open: bool,
    }

    impl Locker for TestLocker {
        fn id(&self) -> i64 {
            self.id
        }

        fn lock(
            &mut self,
            _lsn: u64,
            _lock_type: LockType,
            _non_blocking: bool,
        ) -> Result<LockResult, TxnError> {
            unimplemented!()
        }

        fn release_lock(&mut self, _lsn: u64) -> Result<(), TxnError> {
            Ok(())
        }

        fn owns_write_lock(&self, _lsn: u64) -> bool {
            false
        }

        fn is_transactional(&self) -> bool {
            false
        }

        fn lock_timeout_ms(&self) -> u64 {
            5000
        }

        fn close(&mut self) {
            self.is_open = false;
        }

        fn is_open(&self) -> bool {
            self.is_open
        }
    }

    #[test]
    fn test_defaults() {
        let locker = TestLocker { id: 1, is_open: true };
        assert!(!locker.retains_locks_on_commit());
        assert!(!locker.default_no_wait());
        assert!(locker.is_preemptable());
        assert!(!locker.is_importunate());
        assert!(!locker.is_read_uncommitted_default());
    }

    #[test]
    fn test_close() {
        let mut locker = TestLocker { id: 1, is_open: true };
        assert!(locker.is_open());
        locker.close();
        assert!(!locker.is_open());
    }

    // -----------------------------------------------------------------------
    // Additional coverage for default trait methods and LockerExt
    // -----------------------------------------------------------------------

    #[test]
    fn test_id() {
        let locker = TestLocker { id: 42, is_open: true };
        assert_eq!(locker.id(), 42);
    }

    #[test]
    fn test_is_not_transactional() {
        let locker = TestLocker { id: 1, is_open: true };
        assert!(!locker.is_transactional());
    }

    #[test]
    fn test_lock_timeout_ms() {
        let locker = TestLocker { id: 1, is_open: true };
        assert_eq!(locker.lock_timeout_ms(), 5000);
    }

    #[test]
    fn test_release_lock_ok() {
        let mut locker = TestLocker { id: 1, is_open: true };
        // TestLocker::release_lock is a no-op returning Ok
        assert!(locker.release_lock(100).is_ok());
    }

    #[test]
    fn test_owns_write_lock_always_false() {
        let locker = TestLocker { id: 1, is_open: true };
        assert!(!locker.owns_write_lock(100));
        assert!(!locker.owns_write_lock(0));
    }

    #[test]
    fn test_retains_locks_on_commit_default() {
        let locker = TestLocker { id: 1, is_open: true };
        // Default implementation returns false
        assert!(!locker.retains_locks_on_commit());
    }

    #[test]
    fn test_default_no_wait_default() {
        let locker = TestLocker { id: 1, is_open: true };
        // Default implementation returns false
        assert!(!locker.default_no_wait());
    }

    #[test]
    fn test_is_preemptable_default() {
        let locker = TestLocker { id: 1, is_open: true };
        // Default implementation returns true
        assert!(locker.is_preemptable());
    }

    #[test]
    fn test_is_importunate_default() {
        let locker = TestLocker { id: 1, is_open: true };
        // Default implementation returns false
        assert!(!locker.is_importunate());
    }

    #[test]
    fn test_is_read_uncommitted_default() {
        let locker = TestLocker { id: 1, is_open: true };
        // Default implementation returns false
        assert!(!locker.is_read_uncommitted_default());
    }

    #[test]
    fn test_locker_ext_as_locker() {
        let locker = TestLocker { id: 7, is_open: true };
        let as_ref: &dyn Locker = locker.as_locker();
        assert_eq!(as_ref.id(), 7);
        assert!(as_ref.is_open());
    }

    #[test]
    fn test_locker_ext_as_locker_mut() {
        let mut locker = TestLocker { id: 7, is_open: true };
        {
            let as_mut: &mut dyn Locker = locker.as_locker_mut();
            as_mut.close();
        }
        assert!(!locker.is_open());
    }

    #[test]
    fn test_multiple_closes_idempotent() {
        let mut locker = TestLocker { id: 1, is_open: true };
        locker.close();
        assert!(!locker.is_open());
        // Closing an already-closed locker should not panic
        locker.close();
        assert!(!locker.is_open());
    }

    /// A locker that overrides all default methods to non-default values,
    /// to verify those code paths are exercised.
    struct CustomDefaultsLocker;

    impl Locker for CustomDefaultsLocker {
        fn id(&self) -> i64 { 99 }

        fn lock(
            &mut self,
            _lsn: u64,
            _lock_type: LockType,
            _non_blocking: bool,
        ) -> Result<LockResult, TxnError> {
            unimplemented!()
        }

        fn release_lock(&mut self, _lsn: u64) -> Result<(), TxnError> {
            Ok(())
        }

        fn owns_write_lock(&self, _lsn: u64) -> bool { false }

        fn is_transactional(&self) -> bool { true }

        fn lock_timeout_ms(&self) -> u64 { 0 }

        fn close(&mut self) {}

        fn is_open(&self) -> bool { true }

        // Override all the default methods to non-default values
        fn retains_locks_on_commit(&self) -> bool { true }
        fn default_no_wait(&self) -> bool { true }
        fn is_preemptable(&self) -> bool { false }
        fn is_importunate(&self) -> bool { true }
        fn is_read_uncommitted_default(&self) -> bool { true }
    }

    #[test]
    fn test_custom_defaults_overrides() {
        let locker = CustomDefaultsLocker;
        assert!(locker.retains_locks_on_commit());
        assert!(locker.default_no_wait());
        assert!(!locker.is_preemptable());
        assert!(locker.is_importunate());
        assert!(locker.is_read_uncommitted_default());
        assert!(locker.is_transactional());
        assert_eq!(locker.lock_timeout_ms(), 0);
    }

    #[test]
    fn test_locker_ext_on_custom_locker() {
        let locker = CustomDefaultsLocker;
        let as_ref: &dyn Locker = locker.as_locker();
        assert_eq!(as_ref.id(), 99);
        assert!(as_ref.retains_locks_on_commit());
    }
}
