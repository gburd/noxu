//! Persistent utilization data.
//!
//! stores persistent file summaries
//! and provides methods for selecting files to clean based on utilization.

use crate::file_summary::FileSummary;
use hashbrown::HashMap;

/// Stores persistent file summaries and selects files for cleaning.
///
/// The UtilizationProfile maintains a map of file numbers to their utilization summaries.
/// It provides methods to identify the best files for cleaning based on utilization thresholds.
#[derive(Debug, Clone)]
pub struct UtilizationProfile {
    /// Map of file_number -> FileSummary (persistent).
    file_summaries: HashMap<u32, FileSummary>,
    /// Whether the profile has been modified.
    modified: bool,
}

impl UtilizationProfile {
    /// Creates a new empty utilization profile.
    pub fn new() -> Self {
        Self { file_summaries: HashMap::new(), modified: false }
    }

    /// Returns a reference to the file summary for a file.
    pub fn get_file_summary(&self, file_number: u32) -> Option<&FileSummary> {
        self.file_summaries.get(&file_number)
    }

    /// Returns a mutable reference to the file summary for a file.
    pub fn get_file_summary_mut(
        &mut self,
        file_number: u32,
    ) -> Option<&mut FileSummary> {
        self.modified = true;
        self.file_summaries.get_mut(&file_number)
    }

    /// Populates the profile with the given summaries.
    ///
    /// Replaces any existing summaries.
    pub fn populate(&mut self, summaries: HashMap<u32, FileSummary>) {
        self.file_summaries = summaries;
        self.modified = true;
    }

    /// Updates a file summary with a delta.
    ///
    /// If the file doesn't exist, creates a new summary with the delta values.
    pub fn update_file_summary(
        &mut self,
        file_number: u32,
        delta: &FileSummary,
    ) {
        self.file_summaries.entry(file_number).or_default().add(delta);
        self.modified = true;
    }

    /// Removes a file summary.
    pub fn remove_file_summary(
        &mut self,
        file_number: u32,
    ) -> Option<FileSummary> {
        self.modified = true;
        self.file_summaries.remove(&file_number)
    }

    /// Returns the file with the lowest utilization that qualifies for cleaning.
    ///
    /// Returns (file_number, utilization) or None if no file qualifies.
    ///
    /// # cost/benefit analysis (Cleaner.java:1200-1400)
    ///
    /// selects files using a cost/benefit score:
    ///
    ///   benefit = obsolete_bytes  (more bytes freed = better)
    ///   cost    = active_bytes    (live bytes needing migration = more work)
    ///   score   = benefit / max(1, cost)
    ///
    /// Files are ranked by descending score so that the file with the best
    /// ratio of obsolete data to migration work is cleaned first.
    ///
    /// 
    pub fn get_best_file_for_cleaning(
        &self,
        min_utilization: f64,
    ) -> Option<(u32, f64)> {
        self.file_summaries
            .iter()
            .filter(|(_, summary)| !summary.is_empty())
            .map(|(&file_num, summary)| {
                let util = summary.get_utilization();
                let benefit = summary.get_obsolete_size() as f64;
                let cost = (summary.get_active_size() as f64).max(1.0);
                let score = benefit / cost;
                (file_num, util, score)
            })
            .filter(|&(_, util, _)| util < min_utilization)
            .max_by(|a, b| {
                a.2.partial_cmp(&b.2).unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|(file_num, util, _)| (file_num, util))
    }

    /// Returns all files with utilization below the threshold, sorted by utilization.
    pub fn get_files_at_utilization(&self, threshold: f64) -> Vec<(u32, f64)> {
        let mut files: Vec<(u32, f64)> = self
            .file_summaries
            .iter()
            .filter(|(_, summary)| !summary.is_empty())
            .map(|(&file_num, summary)| (file_num, summary.get_utilization()))
            .filter(|&(_, util)| util < threshold)
            .collect();

        files.sort_by(|a, b| {
            a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal)
        });
        files
    }

    /// Returns all files sorted by utilization (lowest first).
    pub fn count_and_sort(&self) -> Vec<(u32, f64)> {
        let mut files: Vec<(u32, f64)> = self
            .file_summaries
            .iter()
            .filter(|(_, summary)| !summary.is_empty())
            .map(|(&file_num, summary)| (file_num, summary.get_utilization()))
            .collect();

        files.sort_by(|a, b| {
            a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal)
        });
        files
    }

    /// Returns the total size of all log files.
    pub fn get_total_log_size(&self) -> i64 {
        self.file_summaries.values().map(|s| s.total_size as i64).sum()
    }

    /// Returns the total active (non-obsolete) size across all log files.
    pub fn get_active_log_size(&self) -> i64 {
        self.file_summaries.values().map(|s| s.get_active_size() as i64).sum()
    }

    /// Returns the total obsolete size across all log files.
    pub fn get_obsolete_log_size(&self) -> i64 {
        self.file_summaries.values().map(|s| s.get_obsolete_size() as i64).sum()
    }

    /// Returns the overall utilization across all files (0.0-1.0).
    pub fn get_overall_utilization(&self) -> f64 {
        let total = self.get_total_log_size();
        if total == 0 {
            return 0.0;
        }
        let active = self.get_active_log_size();
        active as f64 / total as f64
    }

    /// Returns whether the profile has been modified.
    pub fn is_modified(&self) -> bool {
        self.modified
    }

    /// Clears the modified flag.
    pub fn clear_modified(&mut self) {
        self.modified = false;
    }

    /// Returns the number of files in the profile.
    pub fn get_file_count(&self) -> usize {
        self.file_summaries.len()
    }

    /// Returns all file numbers.
    pub fn get_file_numbers(&self) -> Vec<u32> {
        let mut files: Vec<u32> = self.file_summaries.keys().copied().collect();
        files.sort_unstable();
        files
    }

    /// Clears all summaries.
    pub fn clear(&mut self) {
        self.file_summaries.clear();
        self.modified = true;
    }
}

impl Default for UtilizationProfile {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_summary(total_size: i32, obsolete_size: i32) -> FileSummary {
        let mut summary = FileSummary::new();
        summary.total_count = 10;
        summary.total_size = total_size;
        summary.total_ln_count = 10;
        summary.total_ln_size = total_size;
        summary.obsolete_ln_count = (obsolete_size * 10) / total_size;
        summary.obsolete_ln_size = obsolete_size;
        summary.obsolete_ln_size_counted = summary.obsolete_ln_count;
        summary
    }

    #[test]
    fn test_new() {
        let profile = UtilizationProfile::new();
        assert_eq!(profile.get_file_count(), 0);
        assert!(!profile.is_modified());
    }

    #[test]
    fn test_populate() {
        let mut profile = UtilizationProfile::new();
        let mut summaries = HashMap::new();
        summaries.insert(1, create_summary(1000, 500));
        summaries.insert(2, create_summary(2000, 1000));

        profile.populate(summaries);

        assert_eq!(profile.get_file_count(), 2);
        assert!(profile.is_modified());
    }

    #[test]
    fn test_update_file_summary_new() {
        let mut profile = UtilizationProfile::new();
        let delta = create_summary(1000, 500);

        profile.update_file_summary(1, &delta);

        let summary = profile.get_file_summary(1).unwrap();
        assert_eq!(summary.total_size, 1000);
        assert!(profile.is_modified());
    }

    #[test]
    fn test_update_file_summary_existing() {
        let mut profile = UtilizationProfile::new();
        profile.update_file_summary(1, &create_summary(1000, 500));
        profile.clear_modified();

        profile.update_file_summary(1, &create_summary(500, 250));

        let summary = profile.get_file_summary(1).unwrap();
        assert_eq!(summary.total_size, 1500);
        assert!(profile.is_modified());
    }

    #[test]
    fn test_remove_file_summary() {
        let mut profile = UtilizationProfile::new();
        profile.update_file_summary(1, &create_summary(1000, 500));

        let removed = profile.remove_file_summary(1);
        assert!(removed.is_some());
        assert_eq!(profile.get_file_count(), 0);
    }

    #[test]
    fn test_get_best_file_for_cleaning() {
        let mut profile = UtilizationProfile::new();
        profile.update_file_summary(1, &create_summary(1000, 900)); // 10% util
        profile.update_file_summary(2, &create_summary(1000, 500)); // 50% util
        profile.update_file_summary(3, &create_summary(1000, 700)); // 30% util

        // Should return file with lowest utilization below threshold
        let result = profile.get_best_file_for_cleaning(0.6);
        assert_eq!(result.map(|(f, _)| f), Some(1)); // File 1 has 10% util
    }

    #[test]
    fn test_get_best_file_no_qualification() {
        let mut profile = UtilizationProfile::new();
        profile.update_file_summary(1, &create_summary(1000, 100)); // 90% util

        // No file qualifies below 50% threshold
        let result = profile.get_best_file_for_cleaning(0.5);
        assert_eq!(result, None);
    }

    #[test]
    fn test_get_files_at_utilization() {
        let mut profile = UtilizationProfile::new();
        profile.update_file_summary(1, &create_summary(1000, 900)); // 10% util
        profile.update_file_summary(2, &create_summary(1000, 500)); // 50% util
        profile.update_file_summary(3, &create_summary(1000, 700)); // 30% util
        profile.update_file_summary(4, &create_summary(1000, 100)); // 90% util

        let files = profile.get_files_at_utilization(0.6);

        // Should return files 1, 3, 2 (sorted by utilization)
        assert_eq!(files.len(), 3);
        assert_eq!(files[0].0, 1); // 10%
        assert_eq!(files[1].0, 3); // 30%
        assert_eq!(files[2].0, 2); // 50%
    }

    #[test]
    fn test_count_and_sort() {
        let mut profile = UtilizationProfile::new();
        profile.update_file_summary(1, &create_summary(1000, 500)); // 50%
        profile.update_file_summary(2, &create_summary(1000, 900)); // 10%
        profile.update_file_summary(3, &create_summary(1000, 700)); // 30%

        let files = profile.count_and_sort();

        assert_eq!(files.len(), 3);
        assert_eq!(files[0].0, 2); // 10%
        assert_eq!(files[1].0, 3); // 30%
        assert_eq!(files[2].0, 1); // 50%
    }

    #[test]
    fn test_get_total_log_size() {
        let mut profile = UtilizationProfile::new();
        profile.update_file_summary(1, &create_summary(1000, 500));
        profile.update_file_summary(2, &create_summary(2000, 1000));

        assert_eq!(profile.get_total_log_size(), 3000);
    }

    #[test]
    fn test_get_active_log_size() {
        let mut profile = UtilizationProfile::new();
        profile.update_file_summary(1, &create_summary(1000, 500)); // 500 active
        profile.update_file_summary(2, &create_summary(2000, 1000)); // 1000 active

        assert_eq!(profile.get_active_log_size(), 1500);
    }

    #[test]
    fn test_get_obsolete_log_size() {
        let mut profile = UtilizationProfile::new();
        profile.update_file_summary(1, &create_summary(1000, 500)); // 500 obsolete
        profile.update_file_summary(2, &create_summary(2000, 1000)); // 1000 obsolete

        assert_eq!(profile.get_obsolete_log_size(), 1500);
    }

    #[test]
    fn test_get_overall_utilization() {
        let mut profile = UtilizationProfile::new();
        profile.update_file_summary(1, &create_summary(1000, 500)); // 50% util
        profile.update_file_summary(2, &create_summary(1000, 500)); // 50% util

        assert_eq!(profile.get_overall_utilization(), 0.5);
    }

    #[test]
    fn test_get_overall_utilization_empty() {
        let profile = UtilizationProfile::new();
        assert_eq!(profile.get_overall_utilization(), 0.0);
    }

    #[test]
    fn test_modified_flag() {
        let mut profile = UtilizationProfile::new();
        assert!(!profile.is_modified());

        profile.update_file_summary(1, &create_summary(1000, 500));
        assert!(profile.is_modified());

        profile.clear_modified();
        assert!(!profile.is_modified());
    }

    #[test]
    fn test_get_file_numbers() {
        let mut profile = UtilizationProfile::new();
        profile.update_file_summary(3, &create_summary(1000, 500));
        profile.update_file_summary(1, &create_summary(1000, 500));
        profile.update_file_summary(2, &create_summary(1000, 500));

        let files = profile.get_file_numbers();
        assert_eq!(files, vec![1, 2, 3]); // Sorted
    }

    #[test]
    fn test_clear() {
        let mut profile = UtilizationProfile::new();
        profile.update_file_summary(1, &create_summary(1000, 500));
        profile.update_file_summary(2, &create_summary(1000, 500));

        profile.clear();

        assert_eq!(profile.get_file_count(), 0);
        assert!(profile.is_modified());
    }

    #[test]
    fn test_get_file_summary_mut_sets_modified() {
        let mut profile = UtilizationProfile::new();
        profile.update_file_summary(1, &create_summary(1000, 500));
        profile.clear_modified();

        {
            let _summary = profile.get_file_summary_mut(1);
        }

        assert!(profile.is_modified());
    }

    #[test]
    fn test_clone() {
        let mut profile1 = UtilizationProfile::new();
        profile1.update_file_summary(1, &create_summary(1000, 500));

        let profile2 = profile1.clone();
        assert_eq!(profile2.get_file_count(), 1);
        assert!(profile2.get_file_summary(1).is_some());
    }

    #[test]
    fn test_default() {
        let profile = UtilizationProfile::default();
        assert_eq!(profile.get_file_count(), 0);
    }
}
