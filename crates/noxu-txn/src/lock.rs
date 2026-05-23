//! Unified lock interface that transparently handles thin and full locks.
//!
//! Provides a Lock enum that starts as a ThinLockImpl and automatically
//! mutates to a LockImpl when contention occurs.

use crate::{
    LockAttemptResult, LockImpl, LockInfo, LockType, ThinLockImpl,
    lock_info::WaiterNotify,
};

/// A lock on an LSN, either thin (single owner) or full (multiple owners/waiters).
///
/// This enum provides a unified interface that starts with a memory-optimized
/// ThinLockImpl and automatically mutates to a full LockImpl when contention
/// occurs (when a second owner or first waiter is needed).
#[derive(Debug)]
pub enum Lock {
    /// Memory-optimized lock for single owner, no waiters.
    Thin(ThinLockImpl),
    /// Full lock supporting multiple owners and waiters.
    Full(LockImpl),
}

impl Lock {
    /// Create a new thin lock.
    pub fn new_thin() -> Self {
        Lock::Thin(ThinLockImpl::new())
    }

    /// Create a new full lock.
    pub fn new_full() -> Self {
        Lock::Full(LockImpl::new())
    }

    /// Attempts to acquire the lock.
    ///
    /// If the lock is thin and needs to mutate to full, this method
    /// performs the mutation transparently.
    #[inline]
    pub fn lock(
        &mut self,
        request_type: LockType,
        locker_id: i64,
        non_blocking: bool,
        jump_ahead_of_waiters: bool,
    ) -> LockAttemptResult {
        match self {
            Lock::Thin(thin) => {
                match thin.lock(
                    request_type,
                    locker_id,
                    non_blocking,
                    jump_ahead_of_waiters,
                ) {
                    Ok(result) => result,
                    Err(mutation) => {
                        // Need to mutate to full lock
                        let mut full =
                            LockImpl::from_first_owner(mutation.existing_owner);
                        let result = full.lock(
                            request_type,
                            locker_id,
                            non_blocking,
                            jump_ahead_of_waiters,
                        );
                        *self = Lock::Full(full);
                        result
                    }
                }
            }
            Lock::Full(full) => full.lock(
                request_type,
                locker_id,
                non_blocking,
                jump_ahead_of_waiters,
            ),
        }
    }

    /// Like `lock()` but uses a sharing predicate to bypass conflict detection
    /// between co-operating lockers (e.g. ThreadLockers on the same thread).
    ///
    /// `shares_fn(owner_id)` returns true if the requesting locker shares locks
    /// with `owner_id`.
    ///
    #[inline]
    pub fn lock_with_sharing<F: Fn(i64) -> bool>(
        &mut self,
        request_type: LockType,
        locker_id: i64,
        non_blocking: bool,
        jump_ahead_of_waiters: bool,
        shares_fn: &F,
    ) -> LockAttemptResult {
        match self {
            Lock::Thin(thin) => {
                match thin.lock(
                    request_type,
                    locker_id,
                    non_blocking,
                    jump_ahead_of_waiters,
                ) {
                    Ok(result) => result,
                    Err(mutation) => {
                        // Mutate to full and use the sharing variant.
                        let mut full =
                            LockImpl::from_first_owner(mutation.existing_owner);
                        let result = full.lock_with_sharing(
                            request_type,
                            locker_id,
                            non_blocking,
                            jump_ahead_of_waiters,
                            shares_fn,
                        );
                        *self = Lock::Full(full);
                        result
                    }
                }
            }
            Lock::Full(full) => full.lock_with_sharing(
                request_type,
                locker_id,
                non_blocking,
                jump_ahead_of_waiters,
                shares_fn,
            ),
        }
    }

    /// Releases a lock held by the given locker.
    /// Returns the set of locker IDs that should be notified (woken up).
    /// Returns None if the locker wasn't an owner.
    #[inline]
    pub fn release(&mut self, locker_id: i64) -> Option<Vec<i64>> {
        match self {
            Lock::Thin(thin) => thin.release(locker_id),
            Lock::Full(full) => full.release(locker_id),
        }
    }

    /// Downgrade a write lock to a read lock.
    pub fn demote(&mut self, locker_id: i64) {
        match self {
            Lock::Thin(thin) => thin.demote(locker_id),
            Lock::Full(full) => full.demote(locker_id),
        }
    }

    /// Remove all owners except the given one (lock stealing for HA).
    /// Returns the list of locker IDs that were preempted.
    pub fn steal_lock(&mut self, locker_id: i64) -> Vec<i64> {
        match self {
            Lock::Thin(thin) => thin.steal_lock(locker_id),
            Lock::Full(full) => full.steal_lock(locker_id),
        }
    }

    /// Remove a waiter from the waiter list.
    pub fn flush_waiter(&mut self, locker_id: i64) {
        match self {
            Lock::Thin(thin) => thin.flush_waiter(locker_id),
            Lock::Full(full) => full.flush_waiter(locker_id),
        }
    }

    /// Attach a notify pair to the waiter entry for `locker_id`.
    ///
    /// Called by `LockManager::lock()` after the waiter has been registered so
    /// the releasing thread can wake the blocked thread.  Thin locks never have
    /// waiters, so this is a no-op for them.
    pub fn set_waiter_notify(&mut self, locker_id: i64, notify: WaiterNotify) {
        match self {
            Lock::Thin(_) => {
                // Thin locks never have waiters; nothing to do.
            }
            Lock::Full(full) => full.set_waiter_notify(locker_id, notify),
        }
    }

    /// Returns the locker IDs of all current owners.
    ///
    /// Used by deadlock detection to build the waits-for graph before waiting.
    pub fn get_owner_ids(&self) -> Vec<i64> {
        self.get_owners_clone().into_iter().map(|info| info.locker_id).collect()
    }

    /// Return true if locker is an owner of this Lock for lockType.
    pub fn is_owner(&self, locker_id: i64, lock_type: LockType) -> bool {
        match self {
            Lock::Thin(thin) => thin.is_owner(locker_id, lock_type),
            Lock::Full(full) => full.is_owner(locker_id, lock_type),
        }
    }

    /// Return true if locker is an owner of this Lock and this is a write lock.
    pub fn is_owned_write_lock(&self, locker_id: i64) -> bool {
        match self {
            Lock::Thin(thin) => thin.is_owned_write_lock(locker_id),
            Lock::Full(full) => full.is_owned_write_lock(locker_id),
        }
    }

    /// Return the lock type owned by this locker, or None if not an owner.
    pub fn get_owned_lock_type(&self, locker_id: i64) -> Option<LockType> {
        match self {
            Lock::Thin(thin) => thin.get_owned_lock_type(locker_id),
            Lock::Full(full) => full.get_owned_lock_type(locker_id),
        }
    }

    /// Return true if locker is a waiter on this Lock.
    pub fn is_waiter(&self, locker_id: i64) -> bool {
        match self {
            Lock::Thin(thin) => thin.is_waiter(locker_id),
            Lock::Full(full) => full.is_waiter(locker_id),
        }
    }

    /// Return the number of waiters.
    #[inline]
    pub fn n_waiters(&self) -> usize {
        match self {
            Lock::Thin(thin) => thin.n_waiters(),
            Lock::Full(full) => full.n_waiters(),
        }
    }

    /// Return the number of owners.
    #[inline]
    pub fn n_owners(&self) -> usize {
        match self {
            Lock::Thin(thin) => thin.n_owners(),
            Lock::Full(full) => full.n_owners(),
        }
    }

    /// Return the locker ID that has a write ownership on this lock.
    pub fn get_write_owner_locker_id(&self) -> Option<i64> {
        match self {
            Lock::Thin(thin) => thin.get_write_owner_locker_id(),
            Lock::Full(full) => full.get_write_owner_locker_id(),
        }
    }

    /// Get a clone of the owners list for debugging.
    pub fn get_owners_clone(&self) -> Vec<LockInfo> {
        match self {
            Lock::Thin(thin) => thin.get_owners_clone(),
            Lock::Full(full) => full.get_owners_clone(),
        }
    }

    /// Get a clone of the waiters list for debugging.
    pub fn get_waiters_clone(&self) -> Vec<LockInfo> {
        match self {
            Lock::Thin(thin) => thin.get_waiters_clone(),
            Lock::Full(full) => full.get_waiters_clone(),
        }
    }

    /// Return true if this is a thin lock.
    pub fn is_thin(&self) -> bool {
        matches!(self, Lock::Thin(_))
    }

    /// Return true if this is a full lock.
    pub fn is_full(&self) -> bool {
        matches!(self, Lock::Full(_))
    }
}

impl Default for Lock {
    fn default() -> Self {
        Self::new_thin()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::LockGrantType;

    #[test]
    fn test_starts_as_thin() {
        let lock = Lock::new_thin();
        assert!(lock.is_thin());
        assert!(!lock.is_full());
    }

    #[test]
    fn test_thin_single_owner() {
        let mut lock = Lock::new_thin();

        // Acquire a read lock
        let result = lock.lock(LockType::Read, 1, false, false);
        assert_eq!(result.grant_type, LockGrantType::New);
        assert!(lock.is_thin());
        assert_eq!(lock.n_owners(), 1);

        // Release the lock
        let notified = lock.release(1);
        assert!(notified.is_some());
        assert_eq!(notified.unwrap().len(), 0);
        assert_eq!(lock.n_owners(), 0);
        assert!(lock.is_thin());
    }

    #[test]
    fn test_thin_mutates_to_full_on_contention() {
        let mut lock = Lock::new_thin();

        // First locker acquires lock
        let result1 = lock.lock(LockType::Read, 1, false, false);
        assert_eq!(result1.grant_type, LockGrantType::New);
        assert!(lock.is_thin());

        // Second locker tries to acquire - should cause mutation to full
        let result2 = lock.lock(LockType::Read, 2, false, false);
        assert_eq!(result2.grant_type, LockGrantType::New);
        assert!(lock.is_full()); // Mutated to full!
        assert_eq!(lock.n_owners(), 2);
    }

    #[test]
    fn test_full_lock_multiple_readers() {
        let mut lock = Lock::new_full();

        // Two readers can co-own
        let result1 = lock.lock(LockType::Read, 1, false, false);
        assert_eq!(result1.grant_type, LockGrantType::New);

        let result2 = lock.lock(LockType::Read, 2, false, false);
        assert_eq!(result2.grant_type, LockGrantType::New);

        assert_eq!(lock.n_owners(), 2);
    }

    #[test]
    fn test_transparent_delegation() {
        let mut thin_lock = Lock::new_thin();
        let mut full_lock = Lock::new_full();

        // Both should behave the same for single owner operations
        let result1 = thin_lock.lock(LockType::Write, 1, false, false);
        let result2 = full_lock.lock(LockType::Write, 1, false, false);

        assert_eq!(result1.grant_type, result2.grant_type);
        assert_eq!(thin_lock.n_owners(), full_lock.n_owners());
        assert_eq!(
            thin_lock.is_owned_write_lock(1),
            full_lock.is_owned_write_lock(1)
        );
    }

    #[test]
    fn test_lock_upgrade_through_enum() {
        let mut lock = Lock::new_thin();

        // Acquire read lock
        let result1 = lock.lock(LockType::Read, 1, false, false);
        assert_eq!(result1.grant_type, LockGrantType::New);

        // Upgrade to write lock
        let result2 = lock.lock(LockType::Write, 1, false, false);
        assert_eq!(result2.grant_type, LockGrantType::Promotion);
        assert!(lock.is_thin()); // Still thin - single owner
        assert!(lock.is_owned_write_lock(1));
    }

    #[test]
    fn test_demote_through_enum() {
        let mut lock = Lock::new_thin();

        // Acquire write lock
        lock.lock(LockType::Write, 1, false, false);
        assert!(lock.is_owned_write_lock(1));

        // Demote to read
        lock.demote(1);
        assert!(!lock.is_owned_write_lock(1));
        assert!(lock.is_owner(1, LockType::Read));
    }

    #[test]
    fn test_steal_lock_through_enum() {
        let mut lock = Lock::new_thin();

        // Locker 1 acquires lock
        lock.lock(LockType::Read, 1, false, false);

        // Steal lock for locker 2
        let preempted = lock.steal_lock(2);
        assert_eq!(preempted.len(), 1);
        assert_eq!(preempted[0], 1);
    }

    #[test]
    fn test_mutation_preserves_owner() {
        let mut lock = Lock::new_thin();

        // First locker acquires write lock
        let result1 = lock.lock(LockType::Write, 1, false, false);
        assert_eq!(result1.grant_type, LockGrantType::New);
        assert!(lock.is_thin());
        assert!(lock.is_owned_write_lock(1));

        // Second locker tries to acquire read lock - should wait and mutate
        let result2 = lock.lock(LockType::Read, 2, false, false);
        assert_eq!(result2.grant_type, LockGrantType::WaitNew);
        assert!(lock.is_full()); // Mutated
        assert!(lock.is_owned_write_lock(1)); // Original owner preserved
        assert_eq!(lock.n_waiters(), 1);
    }

    #[test]
    fn test_release_after_mutation() {
        let mut lock = Lock::new_thin();

        // First locker acquires write lock
        lock.lock(LockType::Write, 1, false, false);

        // Second locker waits (causes mutation)
        lock.lock(LockType::Read, 2, false, false);
        assert!(lock.is_full());

        // First locker releases - second should be promoted
        let notified = lock.release(1);
        assert!(notified.is_some());
        let notified = notified.unwrap();
        assert_eq!(notified.len(), 1);
        assert_eq!(notified[0], 2);
        assert_eq!(lock.n_owners(), 1);
        assert!(lock.is_owner(2, LockType::Read));
    }
}
