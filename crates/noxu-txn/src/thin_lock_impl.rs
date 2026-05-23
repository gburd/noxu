//! Memory-optimized lock for the common case of a single owner with no waiters.
//!

use crate::{LockAttemptResult, LockGrantType, LockInfo, LockType};

/// Memory-optimized lock for the common case of a single owner with no waiters.
///
/// When contention occurs (multiple owners or waiters needed), this lock
/// "mutates" into a full LockImpl by returning a MutateToFull signal.
///
///
#[derive(Debug)]
pub struct ThinLockImpl {
    /// The single lock owner info, or None if unlocked.
    owner: Option<LockInfo>,
}

impl ThinLockImpl {
    /// Create a new empty ThinLockImpl.
    pub fn new() -> Self {
        Self { owner: None }
    }

    /// Create a ThinLockImpl from an existing one (used when releasing lock).
    pub fn from_thin(thin: &ThinLockImpl) -> Self {
        Self { owner: thin.owner.clone() }
    }

    /// Attempts to acquire the lock.
    ///
    /// Returns LockAttemptResult with either a grant or a signal to mutate to full lock.
    pub fn lock(
        &mut self,
        request_type: LockType,
        locker_id: i64,
        _non_blocking: bool,
        _jump_ahead_of_waiters: bool,
    ) -> Result<LockAttemptResult, MutateToFull> {
        // Check if lock is already held by someone else
        if let Some(ref owner) = self.owner
            && owner.locker_id != locker_id
        {
            // Lock is already held by someone else so mutate.
            return Err(MutateToFull { existing_owner: owner.clone() });
        }

        let grant = if let Some(owner) = self.owner.as_mut() {
            // The requestor holds this lock. Check for upgrades.
            let current_type = owner.lock_type;
            let upgrade = current_type.get_upgrade(request_type);

            if upgrade.is_illegal() {
                panic!(
                    "Illegal lock upgrade from {:?} to {:?}",
                    current_type, request_type
                );
            }

            if let Some(upgrade_type) = upgrade.get_upgrade_type() {
                // Need to upgrade
                owner.lock_type = upgrade_type;
                if upgrade.is_promotion() {
                    LockGrantType::Promotion
                } else {
                    LockGrantType::Existing
                }
            } else {
                // No upgrade needed
                LockGrantType::Existing
            }
        } else {
            // Lock is free - grant it
            self.owner = Some(LockInfo::new(locker_id, request_type));
            LockGrantType::New
        };

        Ok(LockAttemptResult::new(grant))
    }

    /// Releases a lock held by the given locker.
    /// Returns Some(empty vec) if the locker was the owner.
    /// Returns None if the locker wasn't the owner.
    pub fn release(&mut self, locker_id: i64) -> Option<Vec<i64>> {
        if let Some(ref owner) = self.owner
            && owner.locker_id == locker_id
        {
            self.owner = None;
            return Some(Vec::new()); // Empty notification list
        }
        None
    }

    /// Downgrade a write lock to a read lock.
    pub fn demote(&mut self, _locker_id: i64) {
        if let Some(ref mut owner) = self.owner
            && owner.lock_type.is_write_lock()
        {
            owner.lock_type = if owner.lock_type == LockType::RangeWrite {
                LockType::RangeRead
            } else {
                LockType::Read
            };
        }
    }

    /// Remove all owners except the given one (lock stealing for HA).
    /// Returns the list of locker IDs that were preempted.
    pub fn steal_lock(&mut self, locker_id: i64) -> Vec<i64> {
        let mut preempted = Vec::new();
        if let Some(ref owner) = self.owner
            && owner.locker_id != locker_id
        {
            preempted.push(owner.locker_id);
            self.owner = None;
        }
        preempted
    }

    /// Remove a waiter from the waiter list (no-op for thin locks).
    pub fn flush_waiter(&mut self, _locker_id: i64) {
        // Do nothing - thin locks never have waiters.
    }

    /// Return true if locker is an owner of this Lock for lockType.
    pub fn is_owner(&self, locker_id: i64, lock_type: LockType) -> bool {
        self.owner.as_ref().is_some_and(|o| {
            o.locker_id == locker_id && o.lock_type == lock_type
        })
    }

    /// Return true if locker is an owner of this Lock and this is a write lock.
    pub fn is_owned_write_lock(&self, locker_id: i64) -> bool {
        self.owner.as_ref().is_some_and(|o| {
            o.locker_id == locker_id && o.lock_type.is_write_lock()
        })
    }

    /// Return the lock type owned by this locker, or None if not an owner.
    pub fn get_owned_lock_type(&self, locker_id: i64) -> Option<LockType> {
        self.owner.as_ref().and_then(|o| {
            if o.locker_id == locker_id { Some(o.lock_type) } else { None }
        })
    }

    /// Return true if locker is a waiter on this Lock (always false for thin locks).
    pub fn is_waiter(&self, _locker_id: i64) -> bool {
        false // There can never be waiters on thin locks.
    }

    /// Return the number of waiters (always 0 for thin locks).
    pub fn n_waiters(&self) -> usize {
        0
    }

    /// Return the number of owners (0 or 1 for thin locks).
    pub fn n_owners(&self) -> usize {
        if self.owner.is_some() { 1 } else { 0 }
    }

    /// Return the locker ID that has a write ownership on this lock.
    pub fn get_write_owner_locker_id(&self) -> Option<i64> {
        self.owner.as_ref().and_then(|o| {
            if o.lock_type.is_write_lock() { Some(o.locker_id) } else { None }
        })
    }

    /// Get a clone of the owners list for debugging.
    pub fn get_owners_clone(&self) -> Vec<LockInfo> {
        if let Some(ref owner) = self.owner {
            vec![owner.clone()]
        } else {
            Vec::new()
        }
    }

    /// Get a clone of the waiters list for debugging (always empty for thin locks).
    pub fn get_waiters_clone(&self) -> Vec<LockInfo> {
        Vec::new()
    }
}

impl Default for ThinLockImpl {
    fn default() -> Self {
        Self::new()
    }
}

/// Signal that this ThinLockImpl needs to mutate to a full LockImpl.
#[derive(Debug)]
pub struct MutateToFull {
    /// The existing owner that needs to be preserved in the full lock.
    pub existing_owner: LockInfo,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_single_owner_lock_release() {
        let mut lock = ThinLockImpl::new();
        let locker_id = 1;

        // Acquire a read lock
        let result = lock.lock(LockType::Read, locker_id, false, false);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().grant_type, LockGrantType::New);
        assert_eq!(lock.n_owners(), 1);

        // Release the lock
        let notified = lock.release(locker_id);
        assert!(notified.is_some());
        assert_eq!(notified.unwrap().len(), 0);
        assert_eq!(lock.n_owners(), 0);
    }

    #[test]
    fn test_upgrade_read_to_write() {
        let mut lock = ThinLockImpl::new();

        // Acquire read lock
        let result1 = lock.lock(LockType::Read, 1, false, false);
        assert!(result1.is_ok());
        assert_eq!(result1.unwrap().grant_type, LockGrantType::New);

        // Upgrade to write lock
        let result2 = lock.lock(LockType::Write, 1, false, false);
        assert!(result2.is_ok());
        assert_eq!(result2.unwrap().grant_type, LockGrantType::Promotion);
        assert_eq!(lock.n_owners(), 1);
        assert!(lock.is_owned_write_lock(1));
    }

    #[test]
    fn test_existing_lock() {
        let mut lock = ThinLockImpl::new();

        // Acquire read lock
        let result1 = lock.lock(LockType::Read, 1, false, false);
        assert!(result1.is_ok());
        assert_eq!(result1.unwrap().grant_type, LockGrantType::New);

        // Request same lock again
        let result2 = lock.lock(LockType::Read, 1, false, false);
        assert!(result2.is_ok());
        assert_eq!(result2.unwrap().grant_type, LockGrantType::Existing);
        assert_eq!(lock.n_owners(), 1);
    }

    #[test]
    fn test_mutation_on_contention() {
        let mut lock = ThinLockImpl::new();

        // First locker acquires lock
        let result1 = lock.lock(LockType::Read, 1, false, false);
        assert!(result1.is_ok());

        // Second locker tries to acquire - should signal mutation
        let result2 = lock.lock(LockType::Read, 2, false, false);
        assert!(result2.is_err());
        let mutation = result2.unwrap_err();
        assert_eq!(mutation.existing_owner.locker_id, 1);
        assert_eq!(mutation.existing_owner.lock_type, LockType::Read);
    }

    #[test]
    fn test_demote() {
        let mut lock = ThinLockImpl::new();

        // Acquire write lock
        let result1 = lock.lock(LockType::Write, 1, false, false);
        assert!(result1.is_ok());
        assert!(lock.is_owned_write_lock(1));

        // Demote to read
        lock.demote(1);
        assert!(!lock.is_owned_write_lock(1));
        assert!(lock.is_owner(1, LockType::Read));
    }

    #[test]
    fn test_steal_lock() {
        let mut lock = ThinLockImpl::new();

        // Locker 1 acquires lock
        let result1 = lock.lock(LockType::Read, 1, false, false);
        assert!(result1.is_ok());

        // Steal lock for locker 2
        let preempted = lock.steal_lock(2);
        assert_eq!(preempted.len(), 1);
        assert_eq!(preempted[0], 1);
        assert_eq!(lock.n_owners(), 0);
    }

    #[test]
    fn test_query_methods() {
        let mut lock = ThinLockImpl::new();

        // Initially no owner
        assert_eq!(lock.n_owners(), 0);
        assert_eq!(lock.n_waiters(), 0);
        assert!(!lock.is_owner(1, LockType::Read));
        assert!(!lock.is_waiter(1));

        // Acquire read lock
        let result1 = lock.lock(LockType::Read, 1, false, false);
        assert!(result1.is_ok());

        assert_eq!(lock.n_owners(), 1);
        assert!(lock.is_owner(1, LockType::Read));
        assert!(!lock.is_owned_write_lock(1));
        assert_eq!(lock.get_owned_lock_type(1), Some(LockType::Read));
        assert_eq!(lock.get_write_owner_locker_id(), None);

        // Upgrade to write
        let result2 = lock.lock(LockType::Write, 1, false, false);
        assert!(result2.is_ok());

        assert!(lock.is_owned_write_lock(1));
        assert_eq!(lock.get_owned_lock_type(1), Some(LockType::Write));
        assert_eq!(lock.get_write_owner_locker_id(), Some(1));
    }
}
