//! Information about a lock holder or waiter.
//!

use std::sync::Arc;

use noxu_sync::{Condvar, Mutex};

use crate::LockType;

/// Shared notification pair used to wake a waiting thread when its lock is granted.
///
/// The `Mutex<bool>` holds the "granted" flag; the `Condvar` is signalled when
/// the flag transitions from false to true.  The Arc is cloned into both the
/// waiter-list entry (so the releasing thread can signal it) and the waiting
/// thread (so it can wait on it).
pub type WaiterNotify = Arc<(Mutex<bool>, Condvar)>;

/// Information about a single lock owner or waiter.
///
/// Stores the locker ID and lock type for each entity holding or waiting on a lock.
/// For waiters, `notify` carries the condvar pair used to wake the blocked thread
/// once the lock is granted.
///
///
#[derive(Debug, Clone)]
pub struct LockInfo {
    /// The ID of the locker holding/waiting for this lock.
    pub locker_id: i64,

    /// The type of lock held/requested.
    pub lock_type: LockType,

    /// Notification pair for waiters.  None for owner entries; set on waiter
    /// entries after the LockManager registers the blocked thread.
    pub notify: Option<WaiterNotify>,
}

impl LockInfo {
    /// Creates a new LockInfo with no notify pair (used for owners and initial
    /// waiter entries before the notify pair is attached).
    pub fn new(locker_id: i64, lock_type: LockType) -> Self {
        Self { locker_id, lock_type, notify: None }
    }
}

/// Equality is based only on locker_id and lock_type so that the waiter-notify
/// Arc does not affect comparisons used in the lock-upgrade / conflict checks.
impl PartialEq for LockInfo {
    fn eq(&self, other: &Self) -> bool {
        self.locker_id == other.locker_id && self.lock_type == other.lock_type
    }
}

impl Eq for LockInfo {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let info = LockInfo::new(42, LockType::Write);
        assert_eq!(info.locker_id, 42);
        assert_eq!(info.lock_type, LockType::Write);
        assert!(info.notify.is_none());
    }

    #[test]
    fn test_clone() {
        let info1 = LockInfo::new(100, LockType::Read);
        let info2 = info1.clone();
        assert_eq!(info1, info2);
    }

    #[test]
    fn test_equality() {
        let info1 = LockInfo::new(1, LockType::Read);
        let info2 = LockInfo::new(1, LockType::Read);
        let info3 = LockInfo::new(2, LockType::Read);
        let info4 = LockInfo::new(1, LockType::Write);

        assert_eq!(info1, info2);
        assert_ne!(info1, info3);
        assert_ne!(info1, info4);
    }

    #[test]
    fn test_notify_ignored_in_equality() {
        // Two LockInfo values with different notify pairs must still compare equal
        // when locker_id and lock_type match.
        let mut info1 = LockInfo::new(1, LockType::Read);
        let mut info2 = LockInfo::new(1, LockType::Read);
        info1.notify = Some(Arc::new((Mutex::new(false), Condvar::new())));
        info2.notify = Some(Arc::new((Mutex::new(false), Condvar::new())));
        assert_eq!(info1, info2);
    }
}
