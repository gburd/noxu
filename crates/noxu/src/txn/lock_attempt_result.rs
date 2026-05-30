//! Result of a lock attempt on a Lock object.
//!

use crate::txn::LockGrantType;

/// Result of a single lock attempt on a Lock object.
///
/// This is a simple tuple returned by the low-level Lock.lock() method
/// to indicate whether the lock attempt succeeded and what grant type resulted.
///
///
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LockAttemptResult {
    /// Whether the lock attempt succeeded.
    pub success: bool,

    /// The type of lock grant that occurred.
    pub lock_grant: LockGrantType,

    /// Alias for lock_grant (for compatibility).
    pub grant_type: LockGrantType,
}

impl LockAttemptResult {
    /// Creates a new LockAttemptResult from a grant type.
    /// The success field is automatically determined based on the grant type.
    pub fn new(grant_type: LockGrantType) -> Self {
        let success = matches!(
            grant_type,
            LockGrantType::New
                | LockGrantType::Promotion
                | LockGrantType::Existing
        );
        Self { success, lock_grant: grant_type, grant_type }
    }

    /// Creates a new LockAttemptResult with explicit success value.
    pub fn with_success(success: bool, lock_grant: LockGrantType) -> Self {
        Self { success, lock_grant, grant_type: lock_grant }
    }

    /// Creates a successful lock attempt result.
    pub fn success(lock_grant: LockGrantType) -> Self {
        Self { success: true, lock_grant, grant_type: lock_grant }
    }

    /// Creates a failed lock attempt result.
    pub fn failure(lock_grant: LockGrantType) -> Self {
        Self { success: false, lock_grant, grant_type: lock_grant }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let result = LockAttemptResult::new(LockGrantType::New);
        assert!(result.success);
        assert_eq!(result.lock_grant, LockGrantType::New);
        assert_eq!(result.grant_type, LockGrantType::New);
    }

    #[test]
    fn test_with_success() {
        let result = LockAttemptResult::with_success(true, LockGrantType::New);
        assert!(result.success);
        assert_eq!(result.lock_grant, LockGrantType::New);
    }

    #[test]
    fn test_success() {
        let result = LockAttemptResult::success(LockGrantType::Promotion);
        assert!(result.success);
        assert_eq!(result.lock_grant, LockGrantType::Promotion);
    }

    #[test]
    fn test_failure() {
        let result = LockAttemptResult::failure(LockGrantType::Denied);
        assert!(!result.success);
        assert_eq!(result.lock_grant, LockGrantType::Denied);
    }
}
