//! Lock conflict result type.
//!

/// Result of checking whether two lock types conflict.
///
/// When a locker requests a lock on a record already held by another locker,
/// the lock manager checks the conflict matrix to determine if the request
/// should be allowed, blocked, or cause a restart.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockConflict {
    /// Lock is allowed, no conflict between the held and requested types.
    Allow,

    /// Lock is blocked, requester must wait for the holder to release.
    Block,

    /// Lock causes a restart (RangeRestartException in the).
    ///
    /// This occurs when a RANGE_INSERT lock is held and a range lock is requested,
    /// indicating that a phantom insert may have occurred within the range being read.
    Restart,
}

impl LockConflict {
    /// Returns true if the lock request is allowed without waiting.
    #[inline]
    pub fn is_allowed(self) -> bool {
        self == LockConflict::Allow
    }

    /// Returns true if the lock request causes a restart.
    #[inline]
    pub fn is_restart(self) -> bool {
        self == LockConflict::Restart
    }

    /// Returns true if the lock request is blocked and must wait.
    #[inline]
    pub fn is_blocked(self) -> bool {
        self == LockConflict::Block
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_allow() {
        let conflict = LockConflict::Allow;
        assert!(conflict.is_allowed());
        assert!(!conflict.is_blocked());
        assert!(!conflict.is_restart());
    }

    #[test]
    fn test_block() {
        let conflict = LockConflict::Block;
        assert!(!conflict.is_allowed());
        assert!(conflict.is_blocked());
        assert!(!conflict.is_restart());
    }

    #[test]
    fn test_restart() {
        let conflict = LockConflict::Restart;
        assert!(!conflict.is_allowed());
        assert!(!conflict.is_blocked());
        assert!(conflict.is_restart());
    }
}
