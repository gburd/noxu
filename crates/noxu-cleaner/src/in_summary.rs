//! Per-IN utilization summary.
//!
//! Port of `com.sleepycat.je.cleaner.INSummary` - used to trace the relative numbers
//! of full INs and BIN-deltas that are obsolete vs active.

/// Per-IN utilization summary.
///
/// Used to trace the relative numbers of full INs and BIN-deltas that are obsolete vs active.
/// May be used in the future for adjusting utilization.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct InSummary {
    /// Total number of IN log entries.
    pub total_in_count: i32,
    /// Total byte size of IN log entries.
    pub total_in_size: i32,
    /// Total number of BIN-delta log entries.
    pub total_bin_delta_count: i32,
    /// Total byte size of BIN-delta log entries.
    pub total_bin_delta_size: i32,
    /// Number of obsolete IN log entries.
    pub obsolete_in_count: i32,
    /// Byte size of obsolete IN log entries.
    pub obsolete_in_size: i32,
    /// Number of obsolete BIN-delta log entries.
    pub obsolete_bin_delta_count: i32,
    /// Byte size of obsolete BIN-delta log entries.
    pub obsolete_bin_delta_size: i32,
}

impl InSummary {
    /// Creates an empty IN summary.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns whether this summary is empty.
    pub fn is_empty(&self) -> bool {
        self.total_in_count == 0
            && self.total_bin_delta_count == 0
            && self.obsolete_in_count == 0
            && self.obsolete_bin_delta_count == 0
    }

    /// Adds the totals of the given summary object to the totals of this object.
    pub fn add(&mut self, other: &InSummary) {
        self.total_in_count += other.total_in_count;
        self.total_in_size += other.total_in_size;
        self.total_bin_delta_count += other.total_bin_delta_count;
        self.total_bin_delta_size += other.total_bin_delta_size;
        self.obsolete_in_count += other.obsolete_in_count;
        self.obsolete_in_size += other.obsolete_in_size;
        self.obsolete_bin_delta_count += other.obsolete_bin_delta_count;
        self.obsolete_bin_delta_size += other.obsolete_bin_delta_size;
    }

    /// Resets all counters to zero.
    pub fn reset(&mut self) {
        self.total_in_count = 0;
        self.total_in_size = 0;
        self.total_bin_delta_count = 0;
        self.total_bin_delta_size = 0;
        self.obsolete_in_count = 0;
        self.obsolete_in_size = 0;
        self.obsolete_bin_delta_count = 0;
        self.obsolete_bin_delta_size = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let summary = InSummary::new();
        assert!(summary.is_empty());
        assert_eq!(summary.total_in_count, 0);
        assert_eq!(summary.total_in_size, 0);
    }

    #[test]
    fn test_is_empty() {
        let mut summary = InSummary::new();
        assert!(summary.is_empty());

        summary.total_in_count = 1;
        assert!(!summary.is_empty());

        summary.total_in_count = 0;
        summary.total_bin_delta_count = 1;
        assert!(!summary.is_empty());

        summary.total_bin_delta_count = 0;
        summary.obsolete_in_count = 1;
        assert!(!summary.is_empty());

        summary.obsolete_in_count = 0;
        summary.obsolete_bin_delta_count = 1;
        assert!(!summary.is_empty());
    }

    #[test]
    fn test_add() {
        let mut summary1 = InSummary {
            total_in_count: 10,
            total_in_size: 1000,
            total_bin_delta_count: 5,
            total_bin_delta_size: 500,
            obsolete_in_count: 2,
            obsolete_in_size: 200,
            obsolete_bin_delta_count: 1,
            obsolete_bin_delta_size: 100,
        };

        let summary2 = InSummary {
            total_in_count: 3,
            total_in_size: 300,
            total_bin_delta_count: 2,
            total_bin_delta_size: 200,
            obsolete_in_count: 1,
            obsolete_in_size: 100,
            obsolete_bin_delta_count: 1,
            obsolete_bin_delta_size: 100,
        };

        summary1.add(&summary2);

        assert_eq!(summary1.total_in_count, 13);
        assert_eq!(summary1.total_in_size, 1300);
        assert_eq!(summary1.total_bin_delta_count, 7);
        assert_eq!(summary1.total_bin_delta_size, 700);
        assert_eq!(summary1.obsolete_in_count, 3);
        assert_eq!(summary1.obsolete_in_size, 300);
        assert_eq!(summary1.obsolete_bin_delta_count, 2);
        assert_eq!(summary1.obsolete_bin_delta_size, 200);
    }

    #[test]
    fn test_reset() {
        let mut summary = InSummary {
            total_in_count: 10,
            total_in_size: 1000,
            total_bin_delta_count: 5,
            total_bin_delta_size: 500,
            obsolete_in_count: 2,
            obsolete_in_size: 200,
            obsolete_bin_delta_count: 1,
            obsolete_bin_delta_size: 100,
        };

        summary.reset();
        assert!(summary.is_empty());
    }

    #[test]
    fn test_clone() {
        let summary1 = InSummary {
            total_in_count: 10,
            total_in_size: 1000,
            total_bin_delta_count: 5,
            total_bin_delta_size: 500,
            obsolete_in_count: 2,
            obsolete_in_size: 200,
            obsolete_bin_delta_count: 1,
            obsolete_bin_delta_size: 100,
        };

        let summary2 = summary1.clone();
        assert_eq!(summary1, summary2);
    }

    #[test]
    fn test_default() {
        let summary = InSummary::default();
        assert!(summary.is_empty());
    }
}
