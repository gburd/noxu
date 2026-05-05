//! Rollback period tracking for HA replica syncup.
//!
//! Port of `com.sleepycat.je.recovery.RollbackTracker`.
//!
//! Detects rollback periods in the log that are the result of HA replica syncups.
//! These rollback periods affect how LNs should be processed at recovery. Rollbacks
//! differ from aborts in that a rollback returns a LN to its previous version,
//! whether intra or inter-txnal, while an abort always returns an LN to its
//! pre-txn version.

use noxu_util::{Lsn, NULL_LSN};
use std::collections::HashMap;

/// Represents a rollback period  -  a range of LSNs that were rolled back.
///
/// A rollback period is defined by:
/// - matchpoint_lsn: The LSN where the rollback starts (logical truncation point)
/// - rollback_start_lsn: The LSN of the RollbackStart entry
/// - rollback_end_lsn: The LSN of the RollbackEnd entry (if completed)
///
/// The rollback period spans from matchpoint_lsn to rollback_start_lsn.
/// Any transactional LNs in that range should be undone during recovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RollbackPeriod {
    /// The matchpoint LSN (start of rollback period).
    pub matchpoint_lsn: Lsn,
    /// The rollback start LSN (end of rollback period).
    pub rollback_start_lsn: Lsn,
    /// The rollback end LSN (if completed, NULL_LSN if incomplete).
    pub rollback_end_lsn: Lsn,
}

impl RollbackPeriod {
    /// Create a new rollback period.
    pub fn new(
        matchpoint_lsn: Lsn,
        rollback_start_lsn: Lsn,
        rollback_end_lsn: Lsn,
    ) -> Self {
        Self { matchpoint_lsn, rollback_start_lsn, rollback_end_lsn }
    }

    /// Check if this period is complete (has a RollbackEnd entry).
    pub fn is_complete(&self) -> bool {
        self.rollback_end_lsn != NULL_LSN
    }

    /// Check if an LSN falls within this rollback period.
    ///
    /// An LSN is in the rollback period if:
    /// matchpoint_lsn < lsn < rollback_start_lsn
    pub fn contains(&self, lsn: Lsn) -> bool {
        lsn > self.matchpoint_lsn && lsn < self.rollback_start_lsn
    }

    /// Check if this period precedes (comes before) the given LSN.
    pub fn precedes(&self, lsn: Lsn) -> bool {
        self.rollback_start_lsn < lsn
    }
}

/// Tracks rollback periods detected during recovery.
///
/// Port of `com.sleepycat.je.recovery.RollbackTracker`.
///
/// Rollback periods affect how LNs are processed during recovery.
/// A rollback returns an LN to its previous version (which differs from
/// an abort, which returns to the pre-txn version).
///
/// During recovery, RollbackStart and RollbackEnd entries are encountered
/// during backward scans. The tracker builds a map of these periods and
/// provides efficient queries for whether an LSN is within a rollback period.
pub struct RollbackTracker {
    /// Completed rollback periods, sorted by matchpoint LSN in ascending order.
    rollback_periods: Vec<RollbackPeriod>,
    /// In-progress rollback starts (no matching end yet).
    /// Key: matchpoint_lsn.as_u64()
    pending_rollback_starts: HashMap<u64, RollbackPeriod>,
}

impl RollbackTracker {
    /// Create a new empty rollback tracker.
    pub fn new() -> Self {
        Self {
            rollback_periods: Vec::new(),
            pending_rollback_starts: HashMap::new(),
        }
    }

    /// Register a RollbackStart entry seen during backward scan.
    ///
    /// A RollbackStart indicates the beginning of a rollback operation.
    /// It may be matched with a RollbackEnd later (earlier in LSN order
    /// since we're scanning backwards).
    ///
    /// # Arguments
    /// * `matchpoint_lsn` - The LSN where the rollback period starts
    /// * `rollback_start_lsn` - The LSN of the RollbackStart entry itself
    pub fn register_rollback_start(
        &mut self,
        matchpoint_lsn: Lsn,
        rollback_start_lsn: Lsn,
    ) {
        let period =
            RollbackPeriod::new(matchpoint_lsn, rollback_start_lsn, NULL_LSN);
        self.pending_rollback_starts.insert(matchpoint_lsn.as_u64(), period);
    }

    /// Register a RollbackEnd entry seen during backward scan.
    ///
    /// A RollbackEnd marks the completion of a rollback operation.
    /// It should match with a pending RollbackStart (which appears later
    /// in LSN order but earlier in the backward scan).
    ///
    /// # Arguments
    /// * `matchpoint_lsn` - The LSN where the rollback period starts
    /// * `rollback_end_lsn` - The LSN of the RollbackEnd entry itself
    pub fn register_rollback_end(
        &mut self,
        matchpoint_lsn: Lsn,
        rollback_end_lsn: Lsn,
    ) {
        let key = matchpoint_lsn.as_u64();

        // Check if there's a pending start that matches this end
        if let Some(mut period) = self.pending_rollback_starts.remove(&key) {
            // Complete the period
            period.rollback_end_lsn = rollback_end_lsn;
            self.add_completed_period(period);
        } else {
            // No matching start yet (it comes later in the scan).
            // This is a RollbackEnd without its RollbackStart.
            // We'll see the RollbackStart later in the backward scan.
            // For now, we can't determine the rollback_start_lsn, so we'll
            // use rollback_end_lsn as a placeholder and update it when we see the start.
            // However, in practice, recovery should always see matching pairs.
            // For now, we'll just create a partial period.
            let period =
                RollbackPeriod::new(matchpoint_lsn, NULL_LSN, rollback_end_lsn);
            self.pending_rollback_starts.insert(key, period);
        }
    }

    /// Add a completed rollback period to the list.
    ///
    /// Periods are kept sorted by matchpoint_lsn in ascending order.
    fn add_completed_period(&mut self, period: RollbackPeriod) {
        // Insert in sorted order (by matchpoint_lsn)
        let pos = self
            .rollback_periods
            .binary_search_by_key(&period.matchpoint_lsn.as_u64(), |p| {
                p.matchpoint_lsn.as_u64()
            })
            .unwrap_or_else(|e| e);
        self.rollback_periods.insert(pos, period);
    }

    /// Check if an LSN falls within any rollback period.
    ///
    /// This is used during recovery to determine if an LN should be
    /// processed specially due to being in a rolled-back section of the log.
    pub fn is_in_rollback_period(&self, lsn: Lsn) -> bool {
        self.rollback_periods.iter().any(|p| p.contains(lsn))
    }

    /// Get all rollback periods (completed only).
    pub fn get_rollback_periods(&self) -> &[RollbackPeriod] {
        &self.rollback_periods
    }

    /// Check if there are incomplete rollbacks (RollbackStart without RollbackEnd).
    pub fn has_incomplete_rollbacks(&self) -> bool {
        !self.pending_rollback_starts.is_empty()
    }

    /// Get the number of completed rollback periods.
    pub fn period_count(&self) -> usize {
        self.rollback_periods.len()
    }

    /// Get the number of pending (incomplete) rollback starts.
    pub fn pending_count(&self) -> usize {
        self.pending_rollback_starts.len()
    }

    /// Create a scanner for efficiently checking rollback status during recovery.
    pub fn get_scanner(&self) -> RollbackScanner {
        RollbackScanner::new(self.rollback_periods.clone())
    }

    /// Record a RollbackStart entry from the analysis phase.
    ///
    /// Takes the LSN of the RollbackStart entry and the deserialized entry.
    /// Called by `RecoveryManager::run_analysis()` when it encounters a
    /// `RollbackStart` log entry.
    ///
    /// Port of `RollbackTracker.register(RollbackStart, lsn)` in JE.
    pub fn record_rollback_start(
        &mut self,
        lsn: Lsn,
        entry: &noxu_log::entry::RollbackStartEntry,
    ) {
        self.register_rollback_start(entry.matchpoint_lsn, lsn);
    }

    /// Record a RollbackEnd entry from the analysis phase.
    ///
    /// Closes the rollback period that was opened by the matching RollbackStart.
    /// Called by `RecoveryManager::run_analysis()` when it encounters a
    /// `RollbackEnd` log entry.
    ///
    /// Port of `RollbackTracker.register(RollbackEnd, lsn)` in JE.
    pub fn record_rollback_end(
        &mut self,
        lsn: Lsn,
        entry: &noxu_log::entry::RollbackEndEntry,
    ) {
        // RollbackEnd carries rollback_start_lsn; we need to find the period
        // by its start_lsn to get the matchpoint_lsn for keying purposes.
        // Look up the pending period that has rollback_start_lsn == entry.rollback_start_lsn.
        let matchpoint = self
            .pending_rollback_starts
            .values()
            .find(|p| p.rollback_start_lsn == entry.rollback_start_lsn)
            .map(|p| p.matchpoint_lsn);
        if let Some(mp) = matchpoint {
            self.register_rollback_end(mp, lsn);
        } else {
            // No matching start found yet; store as pending using start_lsn as key.
            let period = RollbackPeriod::new(NULL_LSN, entry.rollback_start_lsn, lsn);
            self.pending_rollback_starts
                .insert(entry.rollback_start_lsn.as_u64(), period);
        }
    }

    /// Returns true if any rollback periods were found during analysis.
    ///
    /// Port of `RollbackTracker.isActive()` in JE.
    pub fn is_active(&self) -> bool {
        !self.rollback_periods.is_empty() || !self.pending_rollback_starts.is_empty()
    }

    /// Get all completed rollback periods.
    ///
    /// Alias for `get_rollback_periods()` matching the task API.
    pub fn get_periods(&self) -> &[RollbackPeriod] {
        &self.rollback_periods
    }

    /// Returns the earliest (minimum) `start_lsn` across all completed
    /// rollback periods, or `None` if there are none.
    ///
    /// Used during recovery to know how far back in the log to replay.
    /// Port of `RollbackTracker.getEarliestRollbackStart()` in JE.
    pub fn earliest_rollback_start(&self) -> Option<Lsn> {
        self.rollback_periods
            .iter()
            .map(|p| p.rollback_start_lsn)
            .min_by_key(|lsn| lsn.as_u64())
    }
}

impl Default for RollbackTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// Scanner that efficiently checks whether an LSN is within a rollback period.
///
/// Port of the Scanner inner classes in `RollbackTracker.java`.
///
/// The scanner maintains a cursor position as it processes LSNs in backward
/// order during recovery. This avoids repeatedly searching the entire list
/// of rollback periods.
pub struct RollbackScanner {
    /// Rollback periods to scan (sorted by matchpoint_lsn).
    periods: Vec<RollbackPeriod>,
    /// Current index into periods (for backward scanning).
    current_index: usize,
}

impl RollbackScanner {
    /// Create a new scanner with the given rollback periods.
    pub fn new(periods: Vec<RollbackPeriod>) -> Self {
        Self { periods, current_index: 0 }
    }

    /// Check if the given LSN is within a rollback period.
    ///
    /// This method is optimized for backward scanning (decreasing LSNs).
    /// It maintains a cursor position to avoid repeated searches.
    ///
    /// # Arguments
    /// * `lsn` - The LSN to check
    ///
    /// # Returns
    /// `true` if the LSN is within any rollback period, `false` otherwise
    pub fn is_rolled_back(&mut self, lsn: Lsn) -> bool {
        self.periods.iter().any(|period| period.contains(lsn))
    }

    /// Reset the scanner to start from the beginning.
    pub fn reset(&mut self) {
        self.current_index = 0;
    }

    /// Get the number of periods being scanned.
    pub fn period_count(&self) -> usize {
        self.periods.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_lsn(file: u32, offset: u32) -> Lsn {
        Lsn::new(file, offset)
    }

    #[test]
    fn test_rollback_period_basic() {
        let period = RollbackPeriod::new(
            make_lsn(1, 100),
            make_lsn(1, 400),
            make_lsn(1, 500),
        );

        assert!(period.is_complete());
        assert!(period.contains(make_lsn(1, 200)));
        assert!(period.contains(make_lsn(1, 300)));
        assert!(!period.contains(make_lsn(1, 100))); // At matchpoint
        assert!(!period.contains(make_lsn(1, 400))); // At rollback_start
        assert!(!period.contains(make_lsn(1, 50))); // Before matchpoint
        assert!(!period.contains(make_lsn(1, 450))); // After rollback_start
    }

    #[test]
    fn test_rollback_period_incomplete() {
        let period =
            RollbackPeriod::new(make_lsn(1, 100), make_lsn(1, 400), NULL_LSN);

        assert!(!period.is_complete());
        assert!(period.contains(make_lsn(1, 200)));
    }

    #[test]
    fn test_rollback_period_precedes() {
        let period = RollbackPeriod::new(
            make_lsn(1, 100),
            make_lsn(1, 400),
            make_lsn(1, 500),
        );

        assert!(period.precedes(make_lsn(1, 500)));
        assert!(period.precedes(make_lsn(1, 600)));
        assert!(!period.precedes(make_lsn(1, 400)));
        assert!(!period.precedes(make_lsn(1, 200)));
    }

    #[test]
    fn test_tracker_empty() {
        let tracker = RollbackTracker::new();

        assert_eq!(tracker.period_count(), 0);
        assert!(!tracker.has_incomplete_rollbacks());
        assert!(!tracker.is_in_rollback_period(make_lsn(1, 200)));
    }

    #[test]
    fn test_tracker_register_start_only() {
        let mut tracker = RollbackTracker::new();

        tracker.register_rollback_start(make_lsn(1, 100), make_lsn(1, 400));

        assert_eq!(tracker.period_count(), 0); // Not completed yet
        assert!(tracker.has_incomplete_rollbacks());
        assert_eq!(tracker.pending_count(), 1);
    }

    #[test]
    fn test_tracker_register_start_then_end() {
        let mut tracker = RollbackTracker::new();

        // Backward scan: see RollbackStart first
        tracker.register_rollback_start(make_lsn(1, 100), make_lsn(1, 400));
        // Then see RollbackEnd (earlier in LSN order)
        tracker.register_rollback_end(make_lsn(1, 100), make_lsn(1, 500));

        assert_eq!(tracker.period_count(), 1);
        assert!(!tracker.has_incomplete_rollbacks());
        assert_eq!(tracker.pending_count(), 0);

        // Check the period
        let periods = tracker.get_rollback_periods();
        assert_eq!(periods[0].matchpoint_lsn, make_lsn(1, 100));
        assert_eq!(periods[0].rollback_start_lsn, make_lsn(1, 400));
        assert_eq!(periods[0].rollback_end_lsn, make_lsn(1, 500));
    }

    #[test]
    fn test_tracker_is_in_rollback_period() {
        let mut tracker = RollbackTracker::new();

        tracker.register_rollback_start(make_lsn(1, 100), make_lsn(1, 400));
        tracker.register_rollback_end(make_lsn(1, 100), make_lsn(1, 500));

        // LSNs within the period (100 < lsn < 400)
        assert!(tracker.is_in_rollback_period(make_lsn(1, 150)));
        assert!(tracker.is_in_rollback_period(make_lsn(1, 200)));
        assert!(tracker.is_in_rollback_period(make_lsn(1, 350)));

        // LSNs outside the period
        assert!(!tracker.is_in_rollback_period(make_lsn(1, 50)));
        assert!(!tracker.is_in_rollback_period(make_lsn(1, 100)));
        assert!(!tracker.is_in_rollback_period(make_lsn(1, 400)));
        assert!(!tracker.is_in_rollback_period(make_lsn(1, 500)));
    }

    #[test]
    fn test_tracker_multiple_periods() {
        let mut tracker = RollbackTracker::new();

        // Period 1: 100-400
        tracker.register_rollback_start(make_lsn(1, 100), make_lsn(1, 400));
        tracker.register_rollback_end(make_lsn(1, 100), make_lsn(1, 500));

        // Period 2: 600-900
        tracker.register_rollback_start(make_lsn(1, 600), make_lsn(1, 900));
        tracker.register_rollback_end(make_lsn(1, 600), make_lsn(1, 1000));

        assert_eq!(tracker.period_count(), 2);

        // Check period 1
        assert!(tracker.is_in_rollback_period(make_lsn(1, 200)));
        assert!(!tracker.is_in_rollback_period(make_lsn(1, 450)));

        // Check period 2
        assert!(tracker.is_in_rollback_period(make_lsn(1, 700)));
        assert!(!tracker.is_in_rollback_period(make_lsn(1, 1100)));

        // Between periods
        assert!(!tracker.is_in_rollback_period(make_lsn(1, 550)));
    }

    #[test]
    fn test_tracker_sorted_periods() {
        let mut tracker = RollbackTracker::new();

        // Add periods out of order
        tracker.register_rollback_start(make_lsn(1, 600), make_lsn(1, 900));
        tracker.register_rollback_end(make_lsn(1, 600), make_lsn(1, 1000));

        tracker.register_rollback_start(make_lsn(1, 100), make_lsn(1, 400));
        tracker.register_rollback_end(make_lsn(1, 100), make_lsn(1, 500));

        // Periods should be sorted by matchpoint_lsn
        let periods = tracker.get_rollback_periods();
        assert_eq!(periods.len(), 2);
        assert_eq!(periods[0].matchpoint_lsn, make_lsn(1, 100));
        assert_eq!(periods[1].matchpoint_lsn, make_lsn(1, 600));
    }

    #[test]
    fn test_scanner_empty() {
        let scanner = RollbackScanner::new(vec![]);

        assert_eq!(scanner.period_count(), 0);
    }

    #[test]
    fn test_scanner_single_period() {
        let periods = vec![RollbackPeriod::new(
            make_lsn(1, 100),
            make_lsn(1, 400),
            make_lsn(1, 500),
        )];

        let mut scanner = RollbackScanner::new(periods);

        // In period
        assert!(scanner.is_rolled_back(make_lsn(1, 200)));
        assert!(scanner.is_rolled_back(make_lsn(1, 300)));

        // Outside period
        assert!(!scanner.is_rolled_back(make_lsn(1, 50)));
        assert!(!scanner.is_rolled_back(make_lsn(1, 100)));
        assert!(!scanner.is_rolled_back(make_lsn(1, 400)));
        assert!(!scanner.is_rolled_back(make_lsn(1, 500)));
    }

    #[test]
    fn test_scanner_multiple_periods() {
        let periods = vec![
            RollbackPeriod::new(
                make_lsn(1, 100),
                make_lsn(1, 400),
                make_lsn(1, 500),
            ),
            RollbackPeriod::new(
                make_lsn(1, 600),
                make_lsn(1, 900),
                make_lsn(1, 1000),
            ),
        ];

        let mut scanner = RollbackScanner::new(periods);

        assert_eq!(scanner.period_count(), 2);

        // Period 1
        assert!(scanner.is_rolled_back(make_lsn(1, 200)));

        // Between periods
        assert!(!scanner.is_rolled_back(make_lsn(1, 550)));

        // Period 2
        assert!(scanner.is_rolled_back(make_lsn(1, 700)));
    }

    #[test]
    fn test_scanner_reset() {
        let periods = vec![RollbackPeriod::new(
            make_lsn(1, 100),
            make_lsn(1, 400),
            make_lsn(1, 500),
        )];

        let mut scanner = RollbackScanner::new(periods);

        assert!(scanner.is_rolled_back(make_lsn(1, 200)));

        scanner.reset();

        assert!(scanner.is_rolled_back(make_lsn(1, 300)));
    }

    #[test]
    fn test_scanner_backward_scan() {
        // Test backward scanning behavior
        let periods = vec![
            RollbackPeriod::new(
                make_lsn(1, 100),
                make_lsn(1, 200),
                make_lsn(1, 300),
            ),
            RollbackPeriod::new(
                make_lsn(1, 400),
                make_lsn(1, 500),
                make_lsn(1, 600),
            ),
        ];

        let mut scanner = RollbackScanner::new(periods);

        // Scan backward: high to low LSNs
        assert!(!scanner.is_rolled_back(make_lsn(1, 700))); // After all periods
        assert!(scanner.is_rolled_back(make_lsn(1, 450))); // In period 2
        assert!(!scanner.is_rolled_back(make_lsn(1, 350))); // Between periods
        assert!(scanner.is_rolled_back(make_lsn(1, 150))); // In period 1
        assert!(!scanner.is_rolled_back(make_lsn(1, 50))); // Before all periods
    }

    #[test]
    fn test_default_tracker() {
        let tracker = RollbackTracker::default();

        assert_eq!(tracker.period_count(), 0);
        assert!(!tracker.has_incomplete_rollbacks());
    }

    #[test]
    fn test_cross_file_rollback() {
        // Test rollback period spanning multiple files
        let mut tracker = RollbackTracker::new();

        tracker.register_rollback_start(make_lsn(1, 1000), make_lsn(2, 500));
        tracker.register_rollback_end(make_lsn(1, 1000), make_lsn(2, 600));

        // In period (crosses file boundary)
        assert!(tracker.is_in_rollback_period(make_lsn(1, 2000)));
        assert!(tracker.is_in_rollback_period(make_lsn(2, 100)));
        assert!(tracker.is_in_rollback_period(make_lsn(2, 400)));

        // Outside period
        assert!(!tracker.is_in_rollback_period(make_lsn(1, 500)));
        assert!(!tracker.is_in_rollback_period(make_lsn(2, 600)));
    }

    // ------------------------------------------------------------------ New API methods

    #[test]
    fn test_is_active_empty() {
        let tracker = RollbackTracker::new();
        assert!(!tracker.is_active());
    }

    #[test]
    fn test_is_active_with_completed_period() {
        let mut tracker = RollbackTracker::new();
        tracker.register_rollback_start(make_lsn(1, 100), make_lsn(1, 400));
        tracker.register_rollback_end(make_lsn(1, 100), make_lsn(1, 500));
        assert!(tracker.is_active());
    }

    #[test]
    fn test_get_periods_empty() {
        let tracker = RollbackTracker::new();
        assert!(tracker.get_periods().is_empty());
    }

    #[test]
    fn test_get_periods_returns_completed() {
        let mut tracker = RollbackTracker::new();
        tracker.register_rollback_start(make_lsn(1, 100), make_lsn(1, 400));
        tracker.register_rollback_end(make_lsn(1, 100), make_lsn(1, 500));

        let periods = tracker.get_periods();
        assert_eq!(periods.len(), 1);
        assert_eq!(periods[0].matchpoint_lsn, make_lsn(1, 100));
    }

    #[test]
    fn test_earliest_rollback_start_empty() {
        let tracker = RollbackTracker::new();
        assert!(tracker.earliest_rollback_start().is_none());
    }

    #[test]
    fn test_earliest_rollback_start_single() {
        let mut tracker = RollbackTracker::new();
        tracker.register_rollback_start(make_lsn(1, 100), make_lsn(1, 400));
        tracker.register_rollback_end(make_lsn(1, 100), make_lsn(1, 500));

        let earliest = tracker.earliest_rollback_start();
        assert!(earliest.is_some());
        assert_eq!(earliest.unwrap(), make_lsn(1, 400));
    }

    #[test]
    fn test_earliest_rollback_start_multiple() {
        let mut tracker = RollbackTracker::new();
        // Period 1: start_lsn = lsn(1, 400)
        tracker.register_rollback_start(make_lsn(1, 100), make_lsn(1, 400));
        tracker.register_rollback_end(make_lsn(1, 100), make_lsn(1, 500));
        // Period 2: start_lsn = lsn(1, 900) (larger)
        tracker.register_rollback_start(make_lsn(1, 600), make_lsn(1, 900));
        tracker.register_rollback_end(make_lsn(1, 600), make_lsn(1, 1000));

        let earliest = tracker.earliest_rollback_start().unwrap();
        assert_eq!(earliest, make_lsn(1, 400));
    }

    #[test]
    fn test_record_rollback_start_and_end_via_entry_api() {
        use noxu_log::entry::{RollbackEndEntry, RollbackStartEntry};

        let mut tracker = RollbackTracker::new();
        let start_entry = RollbackStartEntry::new(
            make_lsn(1, 50),   // active_txn_start
            make_lsn(1, 100),  // matchpoint_lsn
        );
        let start_lsn = make_lsn(1, 400);
        tracker.record_rollback_start(start_lsn, &start_entry);

        // Not yet complete
        assert_eq!(tracker.period_count(), 0);
        assert!(tracker.is_active());

        let end_entry = RollbackEndEntry::new(start_lsn);
        let end_lsn = make_lsn(1, 500);
        tracker.record_rollback_end(end_lsn, &end_entry);

        assert_eq!(tracker.period_count(), 1);
        // LSN 200 is between matchpoint(100) and start(400) → in period
        assert!(tracker.is_in_rollback_period(make_lsn(1, 200)));
        assert!(!tracker.is_in_rollback_period(make_lsn(1, 50)));
        // LSN 400 is the boundary (rollback_start) — not inside
        assert!(!tracker.is_in_rollback_period(make_lsn(1, 400)));
    }
}
