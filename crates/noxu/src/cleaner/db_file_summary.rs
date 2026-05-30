//! Per-database-per-file utilization counters.
//!
//! the DatabaseImpl stores a
//! persistent map of file number to DbFileSummary.

/// Per-database-per-file utilization counters.
///
/// The DatabaseImpl stores a persistent map of file number to DbFileSummary.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct DbFileSummary {
    /// Number of IN log entries.
    pub total_in_count: i32,
    /// Byte size of IN log entries.
    pub total_in_size: i32,
    /// Number of LN log entries.
    pub total_ln_count: i32,
    /// Byte size of LN log entries.
    pub total_ln_size: i32,
    /// Number of obsolete IN log entries.
    pub obsolete_in_count: i32,
    /// Number of obsolete LN log entries.
    pub obsolete_ln_count: i32,
    /// Byte size of obsolete LN log entries.
    pub obsolete_ln_size: i32,
    /// Number of obsolete LNs with size counted.
    pub obsolete_ln_size_counted: i32,
}

impl DbFileSummary {
    /// Creates an empty summary.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns whether this summary is empty.
    pub fn is_empty(&self) -> bool {
        self.total_in_count == 0
            && self.total_ln_count == 0
            && self.obsolete_in_count == 0
            && self.obsolete_ln_count == 0
    }

    /// Adds the totals of the given summary object to the totals of this object.
    pub fn add(&mut self, other: &DbFileSummary) {
        self.total_in_count += other.total_in_count;
        self.total_in_size += other.total_in_size;
        self.total_ln_count += other.total_ln_count;
        self.total_ln_size += other.total_ln_size;
        self.obsolete_in_count += other.obsolete_in_count;
        self.obsolete_ln_count += other.obsolete_ln_count;
        self.obsolete_ln_size += other.obsolete_ln_size;
        self.obsolete_ln_size_counted += other.obsolete_ln_size_counted;
    }

    /// Resets all counters to zero.
    pub fn reset(&mut self) {
        self.total_in_count = 0;
        self.total_in_size = 0;
        self.total_ln_count = 0;
        self.total_ln_size = 0;
        self.obsolete_in_count = 0;
        self.obsolete_ln_count = 0;
        self.obsolete_ln_size = 0;
        self.obsolete_ln_size_counted = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let summary = DbFileSummary::new();
        assert!(summary.is_empty());
        assert_eq!(summary.total_in_count, 0);
        assert_eq!(summary.total_ln_count, 0);
    }

    #[test]
    fn test_is_empty() {
        let mut summary = DbFileSummary::new();
        assert!(summary.is_empty());

        summary.total_in_count = 1;
        assert!(!summary.is_empty());

        summary.total_in_count = 0;
        summary.total_ln_count = 1;
        assert!(!summary.is_empty());

        summary.total_ln_count = 0;
        summary.obsolete_in_count = 1;
        assert!(!summary.is_empty());

        summary.obsolete_in_count = 0;
        summary.obsolete_ln_count = 1;
        assert!(!summary.is_empty());
    }

    #[test]
    fn test_add() {
        let mut summary1 = DbFileSummary {
            total_in_count: 5,
            total_in_size: 500,
            total_ln_count: 10,
            total_ln_size: 1000,
            obsolete_in_count: 2,
            obsolete_ln_count: 3,
            obsolete_ln_size: 300,
            obsolete_ln_size_counted: 3,
        };

        let summary2 = DbFileSummary {
            total_in_count: 3,
            total_in_size: 300,
            total_ln_count: 7,
            total_ln_size: 700,
            obsolete_in_count: 1,
            obsolete_ln_count: 2,
            obsolete_ln_size: 200,
            obsolete_ln_size_counted: 2,
        };

        summary1.add(&summary2);

        assert_eq!(summary1.total_in_count, 8);
        assert_eq!(summary1.total_in_size, 800);
        assert_eq!(summary1.total_ln_count, 17);
        assert_eq!(summary1.total_ln_size, 1700);
        assert_eq!(summary1.obsolete_in_count, 3);
        assert_eq!(summary1.obsolete_ln_count, 5);
        assert_eq!(summary1.obsolete_ln_size, 500);
        assert_eq!(summary1.obsolete_ln_size_counted, 5);
    }

    #[test]
    fn test_reset() {
        let mut summary = DbFileSummary {
            total_in_count: 5,
            total_in_size: 500,
            total_ln_count: 10,
            total_ln_size: 1000,
            obsolete_in_count: 2,
            obsolete_ln_count: 3,
            obsolete_ln_size: 300,
            obsolete_ln_size_counted: 3,
        };

        summary.reset();
        assert!(summary.is_empty());
        assert_eq!(summary.total_in_size, 0);
        assert_eq!(summary.total_ln_size, 0);
    }

    #[test]
    fn test_clone() {
        let summary1 = DbFileSummary {
            total_in_count: 5,
            total_in_size: 500,
            total_ln_count: 10,
            total_ln_size: 1000,
            obsolete_in_count: 2,
            obsolete_ln_count: 3,
            obsolete_ln_size: 300,
            obsolete_ln_size_counted: 3,
        };

        let summary2 = summary1.clone();
        assert_eq!(summary1, summary2);
    }

    #[test]
    fn test_default() {
        let summary = DbFileSummary::default();
        assert!(summary.is_empty());
    }
}
