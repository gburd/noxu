//! Per-file utilization counters.
//!
//! tracks the total and obsolete
//! counts and sizes for log entries in a single log file.

/// Per-file utilization counters.
///
/// The UtilizationProfile stores a persistent map of file number to FileSummary.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct FileSummary {
    /// Total number of log entries.
    pub total_count: i32,
    /// Total bytes in log file.
    pub total_size: i32,
    /// Number of IN log entries.
    pub total_in_count: i32,
    /// Byte size of IN log entries.
    pub total_in_size: i32,
    /// Number of LN log entries.
    pub total_ln_count: i32,
    /// Byte size of LN log entries.
    pub total_ln_size: i32,
    /// Byte size of largest LN log entry.
    pub max_ln_size: i32,
    /// Number of obsolete IN log entries.
    pub obsolete_in_count: i32,
    /// Number of obsolete LN log entries.
    pub obsolete_ln_count: i32,
    /// Byte size of obsolete LN log entries.
    pub obsolete_ln_size: i32,
    /// Number of obsolete LNs with size counted.
    pub obsolete_ln_size_counted: i32,
    /// Number of TTL-expired LN log entries (subset of obsolete_ln_count).
    ///
    /// `FileSummary` expired-LN tracking used by `UtilizationCalculator`
    /// to give additional weight to files with many expired records.  Expired LNs
    /// do not need to be migrated during cleaning — they can be dropped outright —
    /// so a file with a high expired fraction is cheaper to clean than its raw
    /// utilization suggests.
    pub obsolete_expired_lns: i32,
    /// Byte size of TTL-expired LN log entries (subset of obsolete_ln_size).
    ///
    /// Used together with `obsolete_expired_lns` to compute the adjusted
    /// utilization in `FileSelector::adjusted_utilization_pct()`.
    pub obsolete_expired_size: i32,
    /// Upper-bound (gradual) expired size: `obsolete_expired_size` plus a
    /// prorated fraction of bytes expiring within the CURRENT interval (JE
    /// ExpirationProfile gradual band). The width
    /// `obsolete_expired_gradual_size - obsolete_expired_size` is the two-pass
    /// uncertainty band (CLEANER_TWO_PASS_GAP). 0 when no TTL data.
    pub obsolete_expired_gradual_size: i32,
}

impl FileSummary {
    /// Creates an empty summary.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns whether this summary contains any non-zero totals.
    pub fn is_empty(&self) -> bool {
        self.total_count == 0
            && self.total_size == 0
            && self.obsolete_in_count == 0
            && self.obsolete_ln_count == 0
    }

    /// Resets all totals to zero.
    pub fn reset(&mut self) {
        self.total_count = 0;
        self.total_size = 0;
        self.total_in_count = 0;
        self.total_in_size = 0;
        self.total_ln_count = 0;
        self.total_ln_size = 0;
        self.max_ln_size = 0;
        self.obsolete_in_count = 0;
        self.obsolete_ln_count = 0;
        self.obsolete_ln_size = 0;
        self.obsolete_ln_size_counted = 0;
        self.obsolete_expired_lns = 0;
        self.obsolete_expired_size = 0;
        self.obsolete_expired_gradual_size = 0;
    }

    /// Adds the totals of the given summary object to the totals of this object.
    pub fn add(&mut self, other: &FileSummary) {
        self.total_count += other.total_count;
        self.total_size += other.total_size;
        self.total_in_count += other.total_in_count;
        self.total_in_size += other.total_in_size;
        self.total_ln_count += other.total_ln_count;
        self.total_ln_size += other.total_ln_size;
        if self.max_ln_size < other.max_ln_size {
            self.max_ln_size = other.max_ln_size;
        }
        self.obsolete_in_count += other.obsolete_in_count;
        self.obsolete_ln_count += other.obsolete_ln_count;
        self.obsolete_ln_size += other.obsolete_ln_size;
        self.obsolete_ln_size_counted += other.obsolete_ln_size_counted;
        self.obsolete_expired_lns += other.obsolete_expired_lns;
        self.obsolete_expired_size += other.obsolete_expired_size;
        self.obsolete_expired_gradual_size +=
            other.obsolete_expired_gradual_size;
    }

    /// Returns the average size for LNs with sizes not counted, or NaN if there are no such LNs.
    ///
    /// In FileSummaryLN version 3 and greater the obsolete size is normally counted, but not in
    /// exceptional circumstances such as recovery. If it is not counted, obsolete_ln_size_counted
    /// will be less than obsolete_ln_count.
    ///
    /// In log version 8 and greater, we don't count the size when the LN is not resident in cache
    /// during update/delete, and CLEANER_FETCH_OBSOLETE_SIZE is false (the default setting).
    ///
    /// We added max_ln_size in version 8 for use in estimating obsolete LN sizes.
    ///
    /// To compute the average LN size, we only consider the LNs (both obsolete and non-obsolete)
    /// for which the size has not been counted. This increases accuracy when counted and uncounted
    /// LN sizes are not uniform. An example is when large LNs are inserted and deleted. The size of
    /// the deleted LN log entry (which is small) is always counted, but the previous version (which
    /// has a large size) may not be counted.
    fn get_avg_obsolete_ln_size_not_counted(&self) -> f32 {
        // Normalize obsolete amounts to account for double-counting.
        let obs_ln_count = self.obsolete_ln_count.min(self.total_ln_count);
        let obs_ln_size = self.obsolete_ln_size.min(self.total_ln_size);
        let obs_ln_size_counted =
            self.obsolete_ln_size_counted.min(obs_ln_count);

        let obs_count_not_counted = obs_ln_count - obs_ln_size_counted;
        if obs_count_not_counted <= 0 {
            return f32::NAN;
        }

        let total_size_not_counted = self.total_ln_size - obs_ln_size;
        let total_count_not_counted = self.total_ln_count - obs_ln_size_counted;

        if total_size_not_counted <= 0 || total_count_not_counted <= 0 {
            return f32::NAN;
        }

        total_size_not_counted as f32 / total_count_not_counted as f32
    }

    /// Returns the approximate byte size of all obsolete LN entries, using the average LN size
    /// for LN sizes that were not counted.
    pub fn get_obsolete_ln_size(&self) -> i32 {
        if self.total_ln_count == 0 {
            return 0;
        }

        // Normalize obsolete amounts to account for double-counting.
        let obs_ln_count = self.obsolete_ln_count.min(self.total_ln_count);
        let obs_ln_size = self.obsolete_ln_size.min(self.total_ln_size);
        let obs_ln_size_counted =
            self.obsolete_ln_size_counted.min(obs_ln_count);

        // Use the tracked obsolete size for all entries for which the size was counted,
        // plus the average size for all obsolete entries whose size was not counted.
        let mut obs_size = obs_ln_size as i64;
        let obs_count_not_counted = obs_ln_count - obs_ln_size_counted;
        if obs_count_not_counted > 0 {
            // When there are any obsolete LNs with sizes uncounted, we add an obsolete amount
            // that is the product of the number of LNs uncounted and the average LN size.
            let avg_ln_size_not_counted =
                self.get_avg_obsolete_ln_size_not_counted();
            if !avg_ln_size_not_counted.is_nan() {
                obs_size += (obs_count_not_counted as f32
                    * avg_ln_size_not_counted)
                    as i64;
            }
        }

        // Don't return an impossibly large estimate.
        if obs_size > self.total_ln_size as i64 {
            self.total_ln_size
        } else {
            obs_size as i32
        }
    }

    /// Returns the approximate byte size of all obsolete IN entries.
    pub fn get_obsolete_in_size(&self) -> i32 {
        if self.total_in_count == 0 {
            return 0;
        }

        // Normalize obsolete amounts to account for double-counting.
        let obs_in_count = self.obsolete_in_count.min(self.total_in_count);

        // Use average IN size to compute total.
        let size = self.total_in_size as f32;
        let avg_size_per_in = size / self.total_in_count as f32;
        (obs_in_count as f32 * avg_size_per_in) as i32
    }

    /// Returns an estimate of the total bytes that are obsolete.
    pub fn get_obsolete_size(&self) -> i32 {
        self.calculate_obsolete_size(self.get_obsolete_ln_size())
    }

    /// Calculates obsolete size using the given LN obsolete size.
    fn calculate_obsolete_size(&self, ln_obsolete_size: i32) -> i32 {
        if self.total_size <= 0 {
            return 0;
        }

        // Leftover (non-IN non-LN) space is considered obsolete.
        let leftover_size =
            self.total_size - (self.total_in_size + self.total_ln_size);

        let mut obsolete_size =
            ln_obsolete_size + self.get_obsolete_in_size() + leftover_size;

        // Don't report more obsolete bytes than the total. We may calculate more than the total
        // because of (intentional) double-counting during recovery.
        if obsolete_size > self.total_size {
            obsolete_size = self.total_size;
        }

        obsolete_size
    }

    /// Returns the active (non-obsolete) byte size.
    pub fn get_active_size(&self) -> i32 {
        self.total_size - self.get_obsolete_size()
    }

    /// Calculates utilization percentage (0.0-1.0).
    pub fn get_utilization(&self) -> f64 {
        if self.total_size == 0 {
            return 0.0;
        }
        let active_size = self.get_active_size() as f64;
        active_size / self.total_size as f64
    }

    /// Returns the TTL-adjusted active byte size.
    ///
    /// Expired LNs do not need to be migrated during cleaning — they are simply
    /// dropped.  From a cost/benefit perspective the "live data to migrate" is
    /// `active_size - expired_bytes`, which is what the cleaner actually has to
    /// write to the new file.
    ///
    /// `UtilizationCalculator` expired-size adjustment:
    ///   adjustedActive = active_size - obsolete_expired_size
    pub fn get_adjusted_active_size(&self) -> i32 {
        let active = self.get_active_size();
        // Expired bytes that are also in the active set reduce migration cost.
        // Cap at zero so we never return a negative value.
        let expired = self.obsolete_expired_size.min(active);
        active - expired
    }

    /// Returns the TTL-adjusted utilization (0.0-1.0).
    ///
    /// Files with a high proportion of expired records are cheaper to clean
    /// because expired LNs are dropped rather than migrated.  Using the
    /// adjusted utilization causes the `FileSelector` to prefer such files.
    ///
    /// `UtilizationCalculator.getBestFile()` adjusted-utilization
    /// formula: `(active_bytes - expired_bytes) / total_bytes`.
    pub fn get_adjusted_utilization(&self) -> f64 {
        if self.total_size == 0 {
            return 0.0;
        }
        let adjusted = self.get_adjusted_active_size() as f64;
        (adjusted / self.total_size as f64).clamp(0.0, 1.0)
    }

    /// Returns the total number of entries counted. This value is guaranteed to increase whenever
    /// the tracking information about a file changes. It is used as a key discriminator for
    /// FileSummaryLN records.
    pub fn get_entries_counted(&self) -> i32 {
        self.total_count + self.obsolete_ln_count + self.obsolete_in_count
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let summary = FileSummary::new();
        assert!(summary.is_empty());
        assert_eq!(summary.total_count, 0);
        assert_eq!(summary.total_size, 0);
    }

    #[test]
    fn test_is_empty() {
        let mut summary = FileSummary::new();
        assert!(summary.is_empty());

        summary.total_count = 1;
        assert!(!summary.is_empty());

        summary.total_count = 0;
        summary.total_size = 100;
        assert!(!summary.is_empty());

        summary.total_size = 0;
        summary.obsolete_in_count = 5;
        assert!(!summary.is_empty());

        summary.obsolete_in_count = 0;
        summary.obsolete_ln_count = 3;
        assert!(!summary.is_empty());
    }

    #[test]
    fn test_reset() {
        let mut summary = FileSummary {
            total_count: 10,
            total_size: 1000,
            total_in_count: 3,
            total_in_size: 300,
            total_ln_count: 7,
            total_ln_size: 700,
            max_ln_size: 100,
            obsolete_in_count: 1,
            obsolete_ln_count: 2,
            obsolete_ln_size: 200,
            obsolete_ln_size_counted: 2,
            ..Default::default()
        };

        summary.reset();
        assert!(summary.is_empty());
        assert_eq!(summary.max_ln_size, 0);
    }

    #[test]
    fn test_add() {
        let mut summary1 = FileSummary {
            total_count: 10,
            total_size: 1000,
            total_in_count: 3,
            total_in_size: 300,
            total_ln_count: 7,
            total_ln_size: 700,
            max_ln_size: 100,
            obsolete_in_count: 1,
            obsolete_ln_count: 2,
            obsolete_ln_size: 200,
            obsolete_ln_size_counted: 2,
            ..Default::default()
        };

        let summary2 = FileSummary {
            total_count: 5,
            total_size: 500,
            total_in_count: 2,
            total_in_size: 200,
            total_ln_count: 3,
            total_ln_size: 300,
            max_ln_size: 150,
            obsolete_in_count: 1,
            obsolete_ln_count: 1,
            obsolete_ln_size: 100,
            obsolete_ln_size_counted: 1,
            ..Default::default()
        };

        summary1.add(&summary2);

        assert_eq!(summary1.total_count, 15);
        assert_eq!(summary1.total_size, 1500);
        assert_eq!(summary1.total_in_count, 5);
        assert_eq!(summary1.total_in_size, 500);
        assert_eq!(summary1.total_ln_count, 10);
        assert_eq!(summary1.total_ln_size, 1000);
        assert_eq!(summary1.max_ln_size, 150); // Takes max
        assert_eq!(summary1.obsolete_in_count, 2);
        assert_eq!(summary1.obsolete_ln_count, 3);
        assert_eq!(summary1.obsolete_ln_size, 300);
        assert_eq!(summary1.obsolete_ln_size_counted, 3);
    }

    #[test]
    fn test_get_obsolete_ln_size_all_counted() {
        let summary = FileSummary {
            total_count: 10,
            total_size: 1000,
            total_ln_count: 7,
            total_ln_size: 700,
            obsolete_ln_count: 3,
            obsolete_ln_size: 300,
            obsolete_ln_size_counted: 3, // All counted
            ..Default::default()
        };

        assert_eq!(summary.get_obsolete_ln_size(), 300);
    }

    #[test]
    fn test_get_obsolete_ln_size_with_uncounted() {
        let summary = FileSummary {
            total_count: 10,
            total_size: 1000,
            total_ln_count: 10,
            total_ln_size: 1000,
            obsolete_ln_count: 4,
            obsolete_ln_size: 200, // 2 LNs counted at 100 each
            obsolete_ln_size_counted: 2,
            ..Default::default()
        };

        // Avg size of non-obsolete + uncounted = (1000 - 200) / (10 - 2) = 100
        // Uncounted obsolete = 4 - 2 = 2
        // Estimated obsolete = 200 + (2 * 100) = 400
        assert_eq!(summary.get_obsolete_ln_size(), 400);
    }

    #[test]
    fn test_get_obsolete_in_size() {
        let summary = FileSummary {
            total_count: 10,
            total_size: 1000,
            total_in_count: 5,
            total_in_size: 500,
            obsolete_in_count: 2,
            ..Default::default()
        };

        // Average IN size = 500 / 5 = 100
        // Obsolete IN size = 2 * 100 = 200
        assert_eq!(summary.get_obsolete_in_size(), 200);
    }

    #[test]
    fn test_get_obsolete_size() {
        let summary = FileSummary {
            total_count: 10,
            total_size: 1000,
            total_in_count: 3,
            total_in_size: 300,
            total_ln_count: 7,
            total_ln_size: 600,
            obsolete_in_count: 1,
            obsolete_ln_count: 3,
            obsolete_ln_size: 300,
            obsolete_ln_size_counted: 3,
            ..Default::default()
        };

        // Obsolete IN = 1 * (300/3) = 100
        // Obsolete LN = 300
        // Leftover = 1000 - (300 + 600) = 100
        // Total obsolete = 100 + 300 + 100 = 500
        assert_eq!(summary.get_obsolete_size(), 500);
    }

    #[test]
    fn test_get_active_size() {
        let summary = FileSummary {
            total_count: 10,
            total_size: 1000,
            total_in_count: 3,
            total_in_size: 300,
            total_ln_count: 7,
            total_ln_size: 600,
            obsolete_in_count: 1,
            obsolete_ln_count: 3,
            obsolete_ln_size: 300,
            obsolete_ln_size_counted: 3,
            ..Default::default()
        };

        assert_eq!(summary.get_active_size(), 500);
    }

    #[test]
    fn test_get_utilization() {
        let summary = FileSummary {
            total_count: 10,
            total_size: 1000,
            total_in_count: 3,
            total_in_size: 300,
            total_ln_count: 7,
            total_ln_size: 600,
            obsolete_in_count: 1,
            obsolete_ln_count: 3,
            obsolete_ln_size: 300,
            obsolete_ln_size_counted: 3,
            ..Default::default()
        };

        assert_eq!(summary.get_utilization(), 0.5);
    }

    #[test]
    fn test_get_utilization_empty() {
        let summary = FileSummary::new();
        assert_eq!(summary.get_utilization(), 0.0);
    }

    #[test]
    fn test_get_entries_counted() {
        let summary = FileSummary {
            total_count: 10,
            obsolete_in_count: 2,
            obsolete_ln_count: 3,
            ..Default::default()
        };

        assert_eq!(summary.get_entries_counted(), 15);
    }

    #[test]
    fn test_clone() {
        let summary1 = FileSummary {
            total_count: 10,
            total_size: 1000,
            ..Default::default()
        };

        let summary2 = summary1.clone();
        assert_eq!(summary1, summary2);
    }

    #[test]
    fn test_max_ln_size_preserved() {
        let mut summary =
            FileSummary { max_ln_size: 100, ..Default::default() };

        let other = FileSummary { max_ln_size: 50, ..Default::default() };

        summary.add(&other);
        assert_eq!(summary.max_ln_size, 100);

        let larger = FileSummary { max_ln_size: 200, ..Default::default() };

        summary.add(&larger);
        assert_eq!(summary.max_ln_size, 200);
    }

    #[test]
    fn test_obsolete_size_capped_at_total() {
        // Test double-counting protection
        let summary = FileSummary {
            total_count: 10,
            total_size: 1000,
            total_in_count: 3,
            total_in_size: 300,
            total_ln_count: 7,
            total_ln_size: 600,
            obsolete_in_count: 10,  // More than total
            obsolete_ln_count: 20,  // More than total
            obsolete_ln_size: 1500, // More than total
            obsolete_ln_size_counted: 20,
            ..Default::default()
        };

        assert_eq!(summary.get_obsolete_size(), 1000);
    }
}
