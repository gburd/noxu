//! Lock grant result type.
//!

/// Result of a lock attempt.
///
/// Indicates whether a lock was granted immediately, must wait, was promoted,
/// or denied for non-blocking requests.
///
/// 
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockGrantType {
    /// New lock granted immediately.
    New,

    /// Must wait for new lock (another locker holds a conflicting lock).
    WaitNew,

    /// Lock promoted from read to write (or other upgrade).
    Promotion,

    /// Must wait for promotion (other lockers hold conflicting locks).
    WaitPromotion,

    /// Lock already held at requested level, no action needed.
    Existing,

    /// Non-blocking request denied (would have required waiting).
    Denied,

    /// Must restart due to range conflict (RangeRestartException).
    WaitRestart,

    /// No lock needed (NONE type requested for dirty reads).
    NoneNeeded,
}

impl LockGrantType {
    /// Returns true if the lock was granted immediately (no waiting).
    #[inline]
    pub fn is_granted(self) -> bool {
        matches!(
            self,
            LockGrantType::New
                | LockGrantType::Promotion
                | LockGrantType::Existing
        )
    }

    /// Returns true if the locker must wait.
    #[inline]
    pub fn must_wait(self) -> bool {
        matches!(
            self,
            LockGrantType::WaitNew
                | LockGrantType::WaitPromotion
                | LockGrantType::WaitRestart
        )
    }

    /// Returns true if this is a promotion (upgrade of existing lock).
    #[inline]
    pub fn is_promotion(self) -> bool {
        matches!(self, LockGrantType::Promotion | LockGrantType::WaitPromotion)
    }

    /// Returns true if the request was denied.
    #[inline]
    pub fn is_denied(self) -> bool {
        self == LockGrantType::Denied
    }

    /// Returns true if a restart is required.
    #[inline]
    pub fn is_restart(self) -> bool {
        self == LockGrantType::WaitRestart
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_granted() {
        assert!(LockGrantType::New.is_granted());
        assert!(LockGrantType::Promotion.is_granted());
        assert!(LockGrantType::Existing.is_granted());
        assert!(!LockGrantType::WaitNew.is_granted());
        assert!(!LockGrantType::WaitPromotion.is_granted());
        assert!(!LockGrantType::Denied.is_granted());
        assert!(!LockGrantType::WaitRestart.is_granted());
        assert!(!LockGrantType::NoneNeeded.is_granted());
    }

    #[test]
    fn test_must_wait() {
        assert!(LockGrantType::WaitNew.must_wait());
        assert!(LockGrantType::WaitPromotion.must_wait());
        assert!(LockGrantType::WaitRestart.must_wait());
        assert!(!LockGrantType::New.must_wait());
        assert!(!LockGrantType::Promotion.must_wait());
        assert!(!LockGrantType::Existing.must_wait());
        assert!(!LockGrantType::Denied.must_wait());
        assert!(!LockGrantType::NoneNeeded.must_wait());
    }

    #[test]
    fn test_is_promotion() {
        assert!(LockGrantType::Promotion.is_promotion());
        assert!(LockGrantType::WaitPromotion.is_promotion());
        assert!(!LockGrantType::New.is_promotion());
        assert!(!LockGrantType::WaitNew.is_promotion());
        assert!(!LockGrantType::Existing.is_promotion());
    }

    #[test]
    fn test_is_denied() {
        assert!(LockGrantType::Denied.is_denied());
        assert!(!LockGrantType::New.is_denied());
        assert!(!LockGrantType::WaitNew.is_denied());
        assert!(!LockGrantType::Existing.is_denied());
    }

    #[test]
    fn test_is_restart() {
        assert!(LockGrantType::WaitRestart.is_restart());
        assert!(!LockGrantType::WaitNew.is_restart());
        assert!(!LockGrantType::WaitPromotion.is_restart());
        assert!(!LockGrantType::New.is_restart());
    }
}
