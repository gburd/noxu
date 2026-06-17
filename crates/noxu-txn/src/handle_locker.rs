//! HandleLocker - database handle locker.
//!

use hashbrown::HashSet;
use std::sync::Arc;

use crate::lock_manager::LockManager;
use crate::locker::Locker;
use crate::{LockResult, LockType, TxnError};

/// A locker for database handle locks.
///
/// HandleLocker holds locks for the lifetime of a database handle (Database object).
/// Unlike BasicLocker, these locks persist until the handle is closed, not just
/// for the duration of an API call.
///
/// The primary use case is holding a read lock on a NameLN to prevent
/// database rename, removal, or truncation while the database is open.
///
/// HandleLocker can share locks with another locker (the one used to open
/// the database) to avoid conflicts during the open operation.
///
///
pub struct HandleLocker {
    /// Unique locker ID.
    id: i64,

    /// Shared lock manager.
    lock_manager: Arc<LockManager>,

    /// Set of LSNs currently locked by this locker.
    locked_lsns: HashSet<u64>,

    /// Lock timeout in milliseconds (0 = infinite).
    lock_timeout_ms: u64,

    /// Whether this locker is open.
    is_open: bool,

    /// ID of a transaction locker we share locks with (if any).
    share_with_txn_id: Option<i64>,

    /// ID of a non-transactional locker we share locks with (if any).
    ///
    /// TXN-5 fix (2026-06-16): JE `HandleLocker.sharesLocksWith` (line ~96)
    /// also shares with the non-transactional buddy by identity via
    /// `shareWithNonTxnlLocker`. The previous Noxu code dropped this field in
    /// `with_buddy` when the buddy was non-transactional. Restored here.
    share_with_non_txn_id: Option<i64>,
}

impl HandleLocker {
    /// Creates a new HandleLocker.
    ///
    /// # Arguments
    /// * `id` - Unique locker ID
    /// * `lock_manager` - Shared lock manager
    pub fn new(id: i64, lock_manager: Arc<LockManager>) -> Self {
        HandleLocker {
            id,
            lock_manager,
            locked_lsns: HashSet::new(),
            lock_timeout_ms: 5000, // Default 5 second timeout
            is_open: true,
            share_with_txn_id: None,
            share_with_non_txn_id: None,
        }
    }

    /// Creates a HandleLocker with a specified timeout.
    pub fn with_timeout(
        id: i64,
        lock_manager: Arc<LockManager>,
        timeout_ms: u64,
    ) -> Self {
        HandleLocker {
            id,
            lock_manager,
            locked_lsns: HashSet::new(),
            lock_timeout_ms: timeout_ms,
            is_open: true,
            share_with_txn_id: None,
            share_with_non_txn_id: None,
        }
    }

    /// Creates a HandleLocker that shares locks with another locker.
    ///
    /// This is used during database open to ensure the HandleLocker and
    /// the opening locker can both hold NameLN locks without conflict.
    ///
    /// TXN-5 fix: stores both transactional and non-transactional buddy IDs,
    /// matching JE `HandleLocker` which tracks `shareWithNonTxnlLocker` as a
    /// separate field in addition to the txn-buddy ID.
    pub fn with_buddy(
        id: i64,
        lock_manager: Arc<LockManager>,
        buddy_locker: &dyn Locker,
    ) -> Self {
        let (share_with_txn, share_with_non_txn) =
            if buddy_locker.is_transactional() {
                (Some(buddy_locker.id()), None)
            } else {
                (None, Some(buddy_locker.id()))
            };

        let locker = HandleLocker {
            id,
            lock_manager,
            locked_lsns: HashSet::new(),
            lock_timeout_ms: 5000,
            is_open: true,
            share_with_txn_id: share_with_txn,
            share_with_non_txn_id: share_with_non_txn,
        };
        if let Some(bid) = share_with_txn {
            locker.register_buddy_sharing(bid);
        }
        locker
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

    /// Registers sharing with a buddy locker in the LockManager sharing registry.
    ///
    /// Called internally by `with_buddy()` so that `LockImpl::try_lock()` can
    /// bypass conflict detection between this HandleLocker and its buddy txn.
    fn register_buddy_sharing(&self, buddy_id: i64) {
        self.lock_manager.register_locker_sharing(self.id, buddy_id);
    }
}

impl Locker for HandleLocker {
    /// Returns true if this locker shares locks with the given locker.
    ///
    /// HandleLocker shares with its buddy transaction (if any), allowing the
    /// database-open locker and the handle locker to co-own NameLN locks.
    /// TXN-5 fix: also shares with the non-transactional buddy, mirroring
    /// JE `HandleLocker.sharesLocksWith` which checks both
    /// `shareWithNonTxnlLocker` and the txn-buddy id.
    fn shares_locks_with(&self, other_locker_id: i64) -> bool {
        self.share_with_txn_id == Some(other_locker_id)
            || self.share_with_non_txn_id == Some(other_locker_id)
    }

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

        // Ask the lock manager for the lock
        let grant = self.lock_manager.lock(
            lsn,
            self.id,
            lock_type,
            non_blocking,
            false, // jump_ahead
        )?;

        // Track this lock
        if grant.is_granted() {
            self.locked_lsns.insert(lsn);
        }

        // HandleLocker doesn't track write lock info (non-transactional)
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

    fn close(&mut self) {
        self.is_open = false;
        let _ = self.release_all_locks();
    }

    fn is_open(&self) -> bool {
        self.is_open
    }
}

impl Drop for HandleLocker {
    fn drop(&mut self) {
        // Ensure locks are released when locker is dropped
        let _ = self.release_all_locks();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup() -> (Arc<LockManager>, HandleLocker) {
        let lm = Arc::new(LockManager::new());
        let locker = HandleLocker::new(1, lm.clone());
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

        // Acquire multiple locks (typical for database handles holding NameLN locks)
        locker.lock(100, LockType::Read, false).unwrap();
        locker.lock(200, LockType::Read, false).unwrap();

        locker.release_all_locks().unwrap();

        // In a real implementation, we'd verify the locks are released
        assert!(locker.locked_lsns.is_empty());
    }

    #[test]
    fn test_close_releases_locks() {
        let (_, mut locker) = setup();

        locker.lock(100, LockType::Read, false).unwrap();
        assert!(locker.is_open());

        locker.close();
        assert!(!locker.is_open());
        assert!(locker.locked_lsns.is_empty());
    }

    #[test]
    fn test_with_timeout() {
        let lm = Arc::new(LockManager::new());
        let locker = HandleLocker::with_timeout(1, lm, 10000);
        assert_eq!(locker.lock_timeout_ms(), 10000);
    }

    #[test]
    fn test_long_lived_lock() {
        let (_, mut locker) = setup();

        // HandleLocker is designed to hold locks for long periods
        locker.lock(100, LockType::Read, false).unwrap();

        // Lock persists across multiple operations (unlike BasicLocker)
        // This models holding a NameLN lock while database is open
        assert!(
            locker.owns_write_lock(100) || locker.locked_lsns.contains(&100)
        );

        // Lock only released on close
        locker.close();
        assert!(locker.locked_lsns.is_empty());
    }

    #[test]
    fn test_shares_locks_with() {
        let lm = Arc::new(LockManager::new());
        let locker = HandleLocker::new(1, lm);

        // No buddy, so doesn't share with anyone
        assert!(!locker.shares_locks_with(2));
        assert!(!locker.shares_locks_with(3));
    }

    /// TXN-5 regression test: `with_buddy` using a non-transactional buddy
    /// must populate `share_with_non_txn_id` and `shares_locks_with` must
    /// return true for that buddy ID.
    ///
    /// Pre-fix: `with_buddy` set `share_with_txn_id = None` when the buddy
    /// was non-transactional, so `shares_locks_with` always returned false
    /// for non-txn buddies, contrary to JE `HandleLocker.sharesLocksWith`
    /// which checks `shareWithNonTxnlLocker` by identity.
    #[test]
    fn test_txn5_with_non_txn_buddy_shares_locks() {
        use crate::locker::Locker;

        struct FakeNonTxnLocker(i64);
        impl Locker for FakeNonTxnLocker {
            fn id(&self) -> i64 { self.0 }
            fn lock(&mut self, _: u64, _: LockType, _: bool)
                -> Result<crate::LockResult, TxnError> {
                unimplemented!()
            }
            fn release_lock(&mut self, _: u64) -> Result<(), TxnError> { Ok(()) }
            fn owns_write_lock(&self, _: u64) -> bool { false }
            fn is_transactional(&self) -> bool { false } // ← non-txn
            fn lock_timeout_ms(&self) -> u64 { 0 }
            fn close(&mut self) {}
            fn is_open(&self) -> bool { true }
            fn shares_locks_with(&self, _: i64) -> bool { false }
        }

        let lm = Arc::new(LockManager::new());
        let buddy = FakeNonTxnLocker(42);
        let handle = HandleLocker::with_buddy(10, lm, &buddy);

        // Must share with the non-txn buddy (id = 42).
        assert!(
            handle.shares_locks_with(42),
            "TXN-5: HandleLocker must share with non-txn buddy id=42"
        );
        // Must NOT share with unrelated lockers.
        assert!(
            !handle.shares_locks_with(99),
            "TXN-5: HandleLocker must not share with unrelated id=99"
        );
    }
}
