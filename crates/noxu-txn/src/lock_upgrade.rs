//! Lock upgrade result type.
//!

use crate::LockType;

/// Result of checking whether a lock can be upgraded.
///
/// When a locker already holds a lock and requests a possibly stronger lock
/// on the same record, the lock manager checks if the held lock already
/// covers the request, or if an upgrade is needed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockUpgrade {
    /// Illegal upgrade (internal error  -  should never happen in correct code).
    ///
    /// This indicates a programming error, such as attempting to upgrade
    /// from or to a RANGE_INSERT lock.
    Illegal,

    /// Lock already covers the requested type, no upgrade needed.
    Existing,

    /// Upgrade to WRITE, requires promotion (may need to wait for other readers).
    WritePromote,

    /// Immediate upgrade to RANGE_READ (no waiting needed).
    RangeReadImmed,

    /// Immediate upgrade to RANGE_WRITE (no waiting needed).
    RangeWriteImmed,

    /// Upgrade to RANGE_WRITE, requires promotion (may need to wait).
    RangeWritePromote,
}

impl LockUpgrade {
    /// Returns true if the upgrade is illegal (a programming error).
    #[inline]
    pub fn is_illegal(self) -> bool {
        self == LockUpgrade::Illegal
    }

    /// Returns the new LockType if upgrade is needed, None if existing lock is sufficient.
    pub fn get_upgrade_type(self) -> Option<LockType> {
        match self {
            LockUpgrade::Existing | LockUpgrade::Illegal => None,
            LockUpgrade::WritePromote => Some(LockType::Write),
            LockUpgrade::RangeReadImmed => Some(LockType::RangeRead),
            LockUpgrade::RangeWriteImmed | LockUpgrade::RangeWritePromote => {
                Some(LockType::RangeWrite)
            }
        }
    }

    /// Returns true if this upgrade requires promotion (waiting for conflicting locks).
    ///
    /// Promotion means the upgrade cannot complete immediately and may require
    /// waiting for other lockers to release conflicting locks.
    #[inline]
    pub fn is_promotion(self) -> bool {
        matches!(
            self,
            LockUpgrade::WritePromote | LockUpgrade::RangeWritePromote
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_illegal() {
        assert!(LockUpgrade::Illegal.is_illegal());
        assert!(!LockUpgrade::Existing.is_illegal());
        assert!(LockUpgrade::Illegal.get_upgrade_type().is_none());
        assert!(!LockUpgrade::Illegal.is_promotion());
    }

    #[test]
    fn test_existing() {
        assert!(!LockUpgrade::Existing.is_illegal());
        assert!(LockUpgrade::Existing.get_upgrade_type().is_none());
        assert!(!LockUpgrade::Existing.is_promotion());
    }

    #[test]
    fn test_write_promote() {
        let upgrade = LockUpgrade::WritePromote;
        assert!(!upgrade.is_illegal());
        assert_eq!(upgrade.get_upgrade_type(), Some(LockType::Write));
        assert!(upgrade.is_promotion());
    }

    #[test]
    fn test_range_read_immed() {
        let upgrade = LockUpgrade::RangeReadImmed;
        assert!(!upgrade.is_illegal());
        assert_eq!(upgrade.get_upgrade_type(), Some(LockType::RangeRead));
        assert!(!upgrade.is_promotion());
    }

    #[test]
    fn test_range_write_immed() {
        let upgrade = LockUpgrade::RangeWriteImmed;
        assert!(!upgrade.is_illegal());
        assert_eq!(upgrade.get_upgrade_type(), Some(LockType::RangeWrite));
        assert!(!upgrade.is_promotion());
    }

    #[test]
    fn test_range_write_promote() {
        let upgrade = LockUpgrade::RangeWritePromote;
        assert!(!upgrade.is_illegal());
        assert_eq!(upgrade.get_upgrade_type(), Some(LockType::RangeWrite));
        assert!(upgrade.is_promotion());
    }

    #[test]
    fn test_all_promotions() {
        assert!(LockUpgrade::WritePromote.is_promotion());
        assert!(LockUpgrade::RangeWritePromote.is_promotion());
        assert!(!LockUpgrade::Existing.is_promotion());
        assert!(!LockUpgrade::RangeReadImmed.is_promotion());
        assert!(!LockUpgrade::RangeWriteImmed.is_promotion());
        assert!(!LockUpgrade::Illegal.is_promotion());
    }
}
