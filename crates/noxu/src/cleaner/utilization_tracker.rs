//! In-memory utilization tracking.
//!
//! Base and per-file utilization tracking for log space accounting.
//! tracks per-file utilization changes in memory between checkpoints.
//!
//! ## Property tests
//!
//! Oracle properties comparing the tracker against a brute-force scan over
//! the LN write/delete event log live in
//! `crates/noxu-cleaner/tests/prop_tests.rs` (Wave 11-E):
//! `prop_tracker_total_size_matches_writes`,
//! `prop_tracker_obsolete_count_matches_oracle`,
//! `prop_tracker_file_set_is_union`, `prop_tracker_clear_resets`.

use crate::cleaner::tracked_file_summary::TrackedFileSummary;
use hashbrown::HashMap;

/// Tracks per-file utilization changes in memory.
///
/// The tracker maintains a map of file numbers to tracked summaries, accumulating changes
/// between checkpoints. When a checkpoint occurs, the tracked data is transferred to the
/// persistent UtilizationProfile.
#[derive(Debug)]
pub struct UtilizationTracker {
    /// Map of file_number -> TrackedFileSummary.
    tracked_files: HashMap<u32, TrackedFileSummary>,
    /// Bytes of tracked info (for memory budget).
    tracked_bytes: i64,
    /// Whether to track obsolete offset details.
    track_detail: bool,
}

impl UtilizationTracker {
    /// Creates a new utilization tracker.
    pub fn new(track_detail: bool) -> Self {
        Self { tracked_files: HashMap::new(), tracked_bytes: 0, track_detail }
    }

    /// Tracks an obsolete log entry.
    ///
    /// # Arguments
    /// * `file_number` - The file containing the obsolete entry
    /// * `offset` - The offset of the obsolete entry
    /// * `size` - The size of the obsolete entry
    /// * `count_as_ln` - Whether to count this as an LN (vs IN)
    pub fn track_obsolete(
        &mut self,
        file_number: u32,
        offset: u32,
        size: i32,
        count_as_ln: bool,
    ) {
        let tracked =
            self.tracked_files.entry(file_number).or_insert_with(|| {
                TrackedFileSummary::new(file_number, self.track_detail)
            });

        // Track the offset
        tracked.add_obsolete_offset(offset);

        // Update summary counters
        let summary = tracked.get_summary_mut();
        if count_as_ln {
            summary.obsolete_ln_count += 1;
            summary.obsolete_ln_size += size;
            summary.obsolete_ln_size_counted += 1;
        } else {
            summary.obsolete_in_count += 1;
        }

        // Update memory budget
        self.update_tracked_bytes();
    }

    /// Counts a new log entry.
    ///
    /// # Arguments
    /// * `file_number` - The file containing the new entry
    /// * `size` - The size of the entry
    /// * `is_ln` - Whether this is an LN entry
    /// * `is_in` - Whether this is an IN entry
    pub fn count_new_log_entry(
        &mut self,
        file_number: u32,
        size: i32,
        is_ln: bool,
        is_in: bool,
    ) {
        let tracked =
            self.tracked_files.entry(file_number).or_insert_with(|| {
                TrackedFileSummary::new(file_number, self.track_detail)
            });

        let summary = tracked.get_summary_mut();
        summary.total_count += 1;
        summary.total_size += size;

        if is_ln {
            summary.total_ln_count += 1;
            summary.total_ln_size += size;
            if size > summary.max_ln_size {
                summary.max_ln_size = size;
            }
        }

        if is_in {
            summary.total_in_count += 1;
            summary.total_in_size += size;
        }

        self.update_tracked_bytes();
    }

    /// Returns a reference to the tracked summary for a file.
    pub fn get_tracked_summary(
        &self,
        file_number: u32,
    ) -> Option<&TrackedFileSummary> {
        self.tracked_files.get(&file_number)
    }

    /// Returns a mutable reference to the tracked summary for a file.
    pub fn get_tracked_summary_mut(
        &mut self,
        file_number: u32,
    ) -> Option<&mut TrackedFileSummary> {
        self.tracked_files.get_mut(&file_number)
    }

    /// Returns a reference to all tracked files.
    pub fn get_tracked_files(&self) -> &HashMap<u32, TrackedFileSummary> {
        &self.tracked_files
    }

    /// Returns a mutable reference to all tracked files.
    pub fn get_tracked_files_mut(
        &mut self,
    ) -> &mut HashMap<u32, TrackedFileSummary> {
        &mut self.tracked_files
    }

    /// Removes and returns all tracked files, clearing the tracker.
    ///
    /// This is typically called when transferring tracked data to the utilization profile.
    pub fn remove_all_tracked_files(
        &mut self,
    ) -> HashMap<u32, TrackedFileSummary> {
        self.tracked_bytes = 0;
        std::mem::take(&mut self.tracked_files)
    }

    /// Returns the bytes of tracked information (for memory budget).
    pub fn get_bytes_tracked(&self) -> i64 {
        self.tracked_bytes
    }

    /// Returns the number of files being tracked.
    pub fn get_tracked_file_count(&self) -> usize {
        self.tracked_files.len()
    }

    /// Clears all tracked information.
    pub fn clear(&mut self) {
        self.tracked_files.clear();
        self.tracked_bytes = 0;
    }

    /// Updates the tracked bytes count based on current tracked files.
    fn update_tracked_bytes(&mut self) {
        self.tracked_bytes =
            self.tracked_files.values().map(|t| t.memory_size() as i64).sum();
    }
}

impl Default for UtilizationTracker {
    fn default() -> Self {
        Self::new(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let tracker = UtilizationTracker::new(true);
        assert_eq!(tracker.get_tracked_file_count(), 0);
        assert_eq!(tracker.get_bytes_tracked(), 0);
    }

    #[test]
    fn test_track_obsolete_ln() {
        let mut tracker = UtilizationTracker::new(true);
        tracker.track_obsolete(1, 100, 50, true);

        let tracked = tracker.get_tracked_summary(1).unwrap();
        assert_eq!(tracked.get_summary().obsolete_ln_count, 1);
        assert_eq!(tracked.get_summary().obsolete_ln_size, 50);
        assert_eq!(tracked.get_summary().obsolete_ln_size_counted, 1);
        assert_eq!(tracked.obsolete_offset_count(), 1);
    }

    #[test]
    fn test_track_obsolete_in() {
        let mut tracker = UtilizationTracker::new(true);
        tracker.track_obsolete(1, 100, 50, false);

        let tracked = tracker.get_tracked_summary(1).unwrap();
        assert_eq!(tracked.get_summary().obsolete_in_count, 1);
        assert_eq!(tracked.get_summary().obsolete_ln_count, 0);
    }

    #[test]
    fn test_count_new_ln_entry() {
        let mut tracker = UtilizationTracker::new(true);
        tracker.count_new_log_entry(1, 100, true, false);

        let tracked = tracker.get_tracked_summary(1).unwrap();
        assert_eq!(tracked.get_summary().total_count, 1);
        assert_eq!(tracked.get_summary().total_size, 100);
        assert_eq!(tracked.get_summary().total_ln_count, 1);
        assert_eq!(tracked.get_summary().total_ln_size, 100);
        assert_eq!(tracked.get_summary().max_ln_size, 100);
    }

    #[test]
    fn test_count_new_in_entry() {
        let mut tracker = UtilizationTracker::new(true);
        tracker.count_new_log_entry(1, 200, false, true);

        let tracked = tracker.get_tracked_summary(1).unwrap();
        assert_eq!(tracked.get_summary().total_count, 1);
        assert_eq!(tracked.get_summary().total_size, 200);
        assert_eq!(tracked.get_summary().total_in_count, 1);
        assert_eq!(tracked.get_summary().total_in_size, 200);
    }

    #[test]
    fn test_max_ln_size_tracking() {
        let mut tracker = UtilizationTracker::new(true);
        tracker.count_new_log_entry(1, 50, true, false);
        tracker.count_new_log_entry(1, 100, true, false);
        tracker.count_new_log_entry(1, 75, true, false);

        let tracked = tracker.get_tracked_summary(1).unwrap();
        assert_eq!(tracked.get_summary().max_ln_size, 100);
    }

    #[test]
    fn test_multiple_files() {
        let mut tracker = UtilizationTracker::new(true);
        tracker.count_new_log_entry(1, 100, true, false);
        tracker.count_new_log_entry(2, 200, true, false);
        tracker.count_new_log_entry(3, 300, true, false);

        assert_eq!(tracker.get_tracked_file_count(), 3);
        assert!(tracker.get_tracked_summary(1).is_some());
        assert!(tracker.get_tracked_summary(2).is_some());
        assert!(tracker.get_tracked_summary(3).is_some());
    }

    #[test]
    fn test_track_detail_disabled() {
        let mut tracker = UtilizationTracker::new(false);
        tracker.track_obsolete(1, 100, 50, true);

        let tracked = tracker.get_tracked_summary(1).unwrap();
        // Counters should be updated
        assert_eq!(tracked.get_summary().obsolete_ln_count, 1);
        // But offsets should not be tracked
        assert_eq!(tracked.obsolete_offset_count(), 0);
    }

    #[test]
    fn test_remove_all_tracked_files() {
        let mut tracker = UtilizationTracker::new(true);
        tracker.count_new_log_entry(1, 100, true, false);
        tracker.count_new_log_entry(2, 200, true, false);

        let tracked_files = tracker.remove_all_tracked_files();
        assert_eq!(tracked_files.len(), 2);
        assert_eq!(tracker.get_tracked_file_count(), 0);
        assert_eq!(tracker.get_bytes_tracked(), 0);
    }

    #[test]
    fn test_clear() {
        let mut tracker = UtilizationTracker::new(true);
        tracker.count_new_log_entry(1, 100, true, false);
        tracker.track_obsolete(1, 100, 50, true);

        tracker.clear();

        assert_eq!(tracker.get_tracked_file_count(), 0);
        assert_eq!(tracker.get_bytes_tracked(), 0);
    }

    #[test]
    fn test_get_tracked_files_mut() {
        let mut tracker = UtilizationTracker::new(true);
        tracker.count_new_log_entry(1, 100, true, false);

        {
            let files = tracker.get_tracked_files_mut();
            if let Some(tracked) = files.get_mut(&1) {
                tracked.get_summary_mut().total_count += 10;
            }
        }

        let tracked = tracker.get_tracked_summary(1).unwrap();
        assert_eq!(tracked.get_summary().total_count, 11);
    }

    #[test]
    fn test_bytes_tracked_increases() {
        let mut tracker = UtilizationTracker::new(true);
        let initial_bytes = tracker.get_bytes_tracked();

        tracker.count_new_log_entry(1, 100, true, false);
        let after_entry = tracker.get_bytes_tracked();
        assert!(after_entry > initial_bytes);

        tracker.track_obsolete(1, 100, 50, true);
        let after_obsolete = tracker.get_bytes_tracked();
        assert!(after_obsolete >= after_entry);
    }

    #[test]
    fn test_accumulate_entries_same_file() {
        let mut tracker = UtilizationTracker::new(true);

        for i in 0..10 {
            tracker.count_new_log_entry(1, 100, true, false);
            tracker.track_obsolete(1, i * 100, 50, true);
        }

        let tracked = tracker.get_tracked_summary(1).unwrap();
        assert_eq!(tracked.get_summary().total_count, 10);
        assert_eq!(tracked.get_summary().obsolete_ln_count, 10);
        assert_eq!(tracked.obsolete_offset_count(), 10);
    }

    #[test]
    fn test_default() {
        let tracker = UtilizationTracker::default();
        assert_eq!(tracker.get_tracked_file_count(), 0);
    }
}
