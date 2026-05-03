//! Delta file summary info for a tracked file.
//!
//! Port of `com.sleepycat.je.cleaner.TrackedFileSummary` - tracked files are managed
//! by the UtilizationTracker.

use crate::file_summary::FileSummary;

/// Delta file summary info for a tracked file.
///
/// Tracked files are managed by the UtilizationTracker. The methods in this struct for reading
/// obsolete offsets may be used by multiple threads without synchronization even while another
/// thread is adding offsets. This is possible because elements are never deleted from the lists.
#[derive(Debug, Clone)]
pub struct TrackedFileSummary {
    /// The file number being tracked.
    file_number: u32,
    /// The file summary counters.
    summary: FileSummary,
    /// Obsolete offsets tracked for this file.
    obsolete_offsets: Vec<u32>,
    /// Whether this summary has been modified since last flush.
    modified: bool,
    /// Whether to track obsolete offset details.
    track_detail: bool,
}

impl TrackedFileSummary {
    /// Creates an empty tracked summary.
    pub fn new(file_number: u32, track_detail: bool) -> Self {
        Self {
            file_number,
            summary: FileSummary::new(),
            obsolete_offsets: Vec::new(),
            modified: false,
            track_detail,
        }
    }

    /// Returns the file number being tracked.
    pub fn get_file_number(&self) -> u32 {
        self.file_number
    }

    /// Returns a reference to the file summary.
    pub fn get_summary(&self) -> &FileSummary {
        &self.summary
    }

    /// Returns a mutable reference to the file summary.
    pub fn get_summary_mut(&mut self) -> &mut FileSummary {
        self.modified = true;
        &mut self.summary
    }

    /// Returns whether this summary has been modified.
    pub fn is_modified(&self) -> bool {
        self.modified
    }

    /// Clears the modified flag.
    pub fn clear_modified(&mut self) {
        self.modified = false;
    }

    /// Tracks the given offset as obsolete.
    ///
    /// Must be called under the log write latch in the full implementation.
    pub fn add_obsolete_offset(&mut self, offset: u32) {
        if !self.track_detail {
            return;
        }

        self.obsolete_offsets.push(offset);
        self.modified = true;
    }

    /// Returns a reference to the obsolete offsets.
    pub fn get_obsolete_offsets(&self) -> &[u32] {
        &self.obsolete_offsets
    }

    /// Returns whether detail tracking is enabled.
    pub fn is_track_detail(&self) -> bool {
        self.track_detail
    }

    /// Resets the summary and clears obsolete offsets.
    pub fn reset(&mut self) {
        self.summary.reset();
        self.obsolete_offsets.clear();
        self.modified = false;
    }

    /// Adds the totals and offsets from another tracked summary.
    pub fn add_tracked_summary(&mut self, other: &TrackedFileSummary) {
        self.summary.add(&other.summary);
        if self.track_detail && other.track_detail {
            self.obsolete_offsets.extend_from_slice(&other.obsolete_offsets);
        }
        self.modified = true;
    }

    /// Returns the number of obsolete offsets tracked.
    pub fn obsolete_offset_count(&self) -> usize {
        self.obsolete_offsets.len()
    }

    /// Returns an estimate of memory usage in bytes.
    pub fn memory_size(&self) -> usize {
        // Base struct size + vector capacity
        std::mem::size_of::<Self>()
            + (self.obsolete_offsets.capacity() * std::mem::size_of::<u32>())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let tracked = TrackedFileSummary::new(42, true);
        assert_eq!(tracked.get_file_number(), 42);
        assert!(tracked.get_summary().is_empty());
        assert!(tracked.get_obsolete_offsets().is_empty());
        assert!(!tracked.is_modified());
        assert!(tracked.is_track_detail());
    }

    #[test]
    fn test_new_no_detail() {
        let tracked = TrackedFileSummary::new(42, false);
        assert!(!tracked.is_track_detail());
    }

    #[test]
    fn test_add_obsolete_offset() {
        let mut tracked = TrackedFileSummary::new(42, true);

        tracked.add_obsolete_offset(100);
        tracked.add_obsolete_offset(200);
        tracked.add_obsolete_offset(300);

        assert_eq!(tracked.obsolete_offset_count(), 3);
        assert_eq!(tracked.get_obsolete_offsets(), &[100, 200, 300]);
        assert!(tracked.is_modified());
    }

    #[test]
    fn test_add_obsolete_offset_no_detail() {
        let mut tracked = TrackedFileSummary::new(42, false);

        tracked.add_obsolete_offset(100);
        tracked.add_obsolete_offset(200);

        // Should not track when detail is disabled
        assert_eq!(tracked.obsolete_offset_count(), 0);
    }

    #[test]
    fn test_modify_summary() {
        let mut tracked = TrackedFileSummary::new(42, true);
        assert!(!tracked.is_modified());

        {
            let summary = tracked.get_summary_mut();
            summary.total_count = 10;
            summary.total_size = 1000;
        }

        assert!(tracked.is_modified());
        assert_eq!(tracked.get_summary().total_count, 10);
    }

    #[test]
    fn test_clear_modified() {
        let mut tracked = TrackedFileSummary::new(42, true);
        tracked.add_obsolete_offset(100);
        assert!(tracked.is_modified());

        tracked.clear_modified();
        assert!(!tracked.is_modified());
    }

    #[test]
    fn test_reset() {
        let mut tracked = TrackedFileSummary::new(42, true);

        tracked.get_summary_mut().total_count = 10;
        tracked.add_obsolete_offset(100);
        tracked.add_obsolete_offset(200);

        tracked.reset();

        assert!(tracked.get_summary().is_empty());
        assert_eq!(tracked.obsolete_offset_count(), 0);
        assert!(!tracked.is_modified());
    }

    #[test]
    fn test_add_tracked_summary() {
        let mut tracked1 = TrackedFileSummary::new(42, true);
        tracked1.get_summary_mut().total_count = 10;
        tracked1.get_summary_mut().total_size = 1000;
        tracked1.add_obsolete_offset(100);

        let mut tracked2 = TrackedFileSummary::new(43, true);
        tracked2.get_summary_mut().total_count = 5;
        tracked2.get_summary_mut().total_size = 500;
        tracked2.add_obsolete_offset(200);

        tracked1.add_tracked_summary(&tracked2);

        assert_eq!(tracked1.get_summary().total_count, 15);
        assert_eq!(tracked1.get_summary().total_size, 1500);
        assert_eq!(tracked1.obsolete_offset_count(), 2);
        assert_eq!(tracked1.get_obsolete_offsets(), &[100, 200]);
        assert!(tracked1.is_modified());
    }

    #[test]
    fn test_add_tracked_summary_mixed_detail() {
        let mut tracked1 = TrackedFileSummary::new(42, true);
        tracked1.add_obsolete_offset(100);

        let mut tracked2 = TrackedFileSummary::new(43, false);
        tracked2.get_summary_mut().total_count = 5;

        tracked1.add_tracked_summary(&tracked2);

        // Should only have offset from tracked1
        assert_eq!(tracked1.obsolete_offset_count(), 1);
    }

    #[test]
    fn test_memory_size() {
        let mut tracked = TrackedFileSummary::new(42, true);
        let base_size = tracked.memory_size();

        tracked.add_obsolete_offset(100);
        tracked.add_obsolete_offset(200);
        tracked.add_obsolete_offset(300);

        // Memory size should increase with offsets
        assert!(tracked.memory_size() >= base_size);
    }

    #[test]
    fn test_clone() {
        let mut tracked1 = TrackedFileSummary::new(42, true);
        tracked1.get_summary_mut().total_count = 10;
        tracked1.add_obsolete_offset(100);
        tracked1.add_obsolete_offset(200);

        let tracked2 = tracked1.clone();

        assert_eq!(tracked2.get_file_number(), 42);
        assert_eq!(tracked2.get_summary().total_count, 10);
        assert_eq!(tracked2.obsolete_offset_count(), 2);
        assert_eq!(tracked2.get_obsolete_offsets(), &[100, 200]);
    }

    #[test]
    fn test_get_summary_immutable() {
        let mut tracked = TrackedFileSummary::new(42, true);
        tracked.get_summary_mut().total_count = 10;
        tracked.clear_modified();

        // Getting immutable reference should not set modified flag
        let _summary = tracked.get_summary();
        assert!(!tracked.is_modified());
    }
}
