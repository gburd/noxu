//! Provisional log entry specification.
//!
//! Port of `com.sleepycat.je.log.Provisional`.
//!
//! Specifies whether to log an entry provisionally.
//!
//! Provisional log entries are tagged with the provisional attribute in the
//! log entry header. The provisional attribute can be applied to any type of
//! log entry, and is used to create atomicity among different log entries and
//! to optimize checkpoint recovery performance.
//!
//! During recovery, provisional entries may be skipped based on the checkpoint
//! end LSN:
//! - NO: Always processed by recovery (non-provisional).
//! - YES: Never processed by recovery (always provisional).
//! - BEFORE_CKPT_END: Provisional if before CkptEnd, non-provisional if after.
//!
//! See the extensive documentation in Provisional.java for details on when
//! provisional entries are used.

use noxu_util::Lsn;

/// Specifies whether a log entry is provisional and how recovery should
/// treat it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Provisional {
    /// The entry is non-provisional and is always processed by recovery.
    No,

    /// The entry is provisional and is never processed by recovery.
    Yes,

    /// The entry is provisional (not processed by recovery) if it occurs
    /// before the CkptEnd in the recovery interval, or is non-provisional
    /// (is processed) if it occurs after CkptEnd.
    BeforeCkptEnd,
}

impl Provisional {
    /// Determines whether a given log entry should be treated as provisional
    /// during recovery.
    ///
    /// # Arguments
    /// * `log_entry_lsn` - The LSN of the log entry being checked.
    /// * `ckpt_end_lsn` - The LSN of the checkpoint end entry, or NULL_LSN
    ///   if no checkpoint end was found.
    pub fn is_provisional(self, log_entry_lsn: Lsn, ckpt_end_lsn: Lsn) -> bool {
        debug_assert!(
            !log_entry_lsn.is_null(),
            "log_entry_lsn must not be NULL"
        );

        match self {
            Provisional::No => false,
            Provisional::Yes => true,
            Provisional::BeforeCkptEnd => {
                !ckpt_end_lsn.is_null() && log_entry_lsn < ckpt_end_lsn
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_never_provisional() {
        let entry_lsn = Lsn::new(1, 100);
        let ckpt_lsn = Lsn::new(1, 200);
        assert!(!Provisional::No.is_provisional(entry_lsn, ckpt_lsn));
        assert!(
            !Provisional::No.is_provisional(entry_lsn, noxu_util::NULL_LSN)
        );
    }

    #[test]
    fn test_yes_always_provisional() {
        let entry_lsn = Lsn::new(1, 100);
        let ckpt_lsn = Lsn::new(1, 200);
        assert!(Provisional::Yes.is_provisional(entry_lsn, ckpt_lsn));
        assert!(
            Provisional::Yes.is_provisional(entry_lsn, noxu_util::NULL_LSN)
        );
    }

    #[test]
    fn test_before_ckpt_end() {
        let before_lsn = Lsn::new(1, 100);
        let ckpt_lsn = Lsn::new(1, 200);
        let after_lsn = Lsn::new(1, 300);

        // Before checkpoint end: provisional
        assert!(
            Provisional::BeforeCkptEnd.is_provisional(before_lsn, ckpt_lsn)
        );

        // After checkpoint end: not provisional
        assert!(
            !Provisional::BeforeCkptEnd.is_provisional(after_lsn, ckpt_lsn)
        );

        // No checkpoint end found: not provisional
        assert!(
            !Provisional::BeforeCkptEnd
                .is_provisional(before_lsn, noxu_util::NULL_LSN)
        );
    }
}
