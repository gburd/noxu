//! Main cleaner daemon for log garbage collection.
//!
//! Port of `Cleaner.java` - responsible for garbage collecting the JE log by
//! selecting least utilized files, processing them, and deleting cleaned files.

use crate::FileSelector;
use crate::cleaner_stat::CleanerStats;
use crate::file_processor::{FileProcessResult, FileProcessor};
use crate::file_protector::FileProtector;
use parking_lot::Mutex;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// The Cleaner is responsible for garbage collecting the JE log.
///
/// It selects the least utilized log file for cleaning (FileSelector),
/// reads through the log file (FileProcessor) and determines whether
/// each entry is obsolete or active. Active entries are migrated to
/// the end of the log, and the cleaned file is deleted.
///
/// The cleaner can be invoked manually via `do_clean()` or run as a
/// background daemon thread.
pub struct Cleaner {
    /// File selector for choosing files to clean.
    file_selector: Mutex<FileSelector>,

    /// File protector for preventing deletion of files in use.
    file_protector: FileProtector,

    /// Cleaner statistics.
    stats: Arc<CleanerStats>,

    /// Whether the cleaner is currently running.
    running: AtomicBool,

    /// Shutdown signal.
    shutdown: Arc<AtomicBool>,

    /// Minimum utilization threshold (0-100%).
    ///
    /// Files below this utilization are candidates for cleaning.
    min_utilization: u32,

    /// Minimum file count before cleaning starts.
    ///
    /// The cleaner won't run until at least this many files exist.
    min_file_count: u32,

    /// Minimum age of file before cleaning (in seconds).
    ///
    /// Files must be at least this old before they can be cleaned.
    min_age: u64,

    /// Total number of cleaning runs performed.
    n_runs: AtomicU64,

    /// Files pending deletion (marked safe to delete but not yet removed).
    pending_deletions: Mutex<Vec<u32>>,
}

/// Result of a cleaning operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CleanResult {
    /// Number of files successfully cleaned.
    pub files_cleaned: u32,

    /// Number of files successfully deleted.
    pub files_deleted: u32,

    /// Total number of log entries read across all cleaned files.
    pub total_entries_read: u64,
}

impl Cleaner {
    /// Creates a new cleaner with the given configuration.
    ///
    /// # Arguments
    /// * `min_utilization` - Minimum utilization threshold (0-100%)
    /// * `min_file_count` - Minimum file count before cleaning starts
    /// * `min_age` - Minimum age of file before cleaning (in seconds)
    pub fn new(
        min_utilization: u32,
        min_file_count: u32,
        min_age: u64,
    ) -> Self {
        Self {
            file_selector: Mutex::new(FileSelector::new()),
            file_protector: FileProtector::new(),
            stats: Arc::new(CleanerStats::new()),
            running: AtomicBool::new(false),
            shutdown: Arc::new(AtomicBool::new(false)),
            min_utilization: min_utilization.min(100),
            min_file_count,
            min_age,
            n_runs: AtomicU64::new(0),
            pending_deletions: Mutex::new(Vec::new()),
        }
    }

    /// Main cleaning entry point - performs cleaning of up to n_files.
    ///
    /// # Arguments
    /// * `n_files` - Maximum number of files to clean in this run
    /// * `force` - If true, ignore utilization thresholds and clean anyway
    ///
    /// # Returns
    /// Result containing cleaning statistics or an error
    pub fn do_clean(
        &self,
        n_files: u32,
        _force: bool,
    ) -> Result<CleanResult, String> {
        // Check if already running
        if self
            .running
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            return Err("Cleaner is already running".to_string());
        }

        // Ensure we reset running flag on exit
        let _guard = RunningGuard::new(&self.running);

        // Check shutdown
        if self.shutdown.load(Ordering::Relaxed) {
            return Err("Cleaner is shut down".to_string());
        }

        // Increment run counter
        self.n_runs.fetch_add(1, Ordering::Relaxed);
        self.stats.runs.fetch_add(1, Ordering::Relaxed);

        let mut files_cleaned = 0u32;
        let mut total_entries = 0u64;

        // Select files to clean (up to n_files)
        let mut files_to_clean = Vec::new();
        {
            let mut selector = self.file_selector.lock();
            for _ in 0..n_files {
                if let Some((file_number, _required_util)) =
                    selector.select_file_for_cleaning()
                {
                    files_to_clean.push(file_number);
                } else {
                    break;
                }
            }
        }

        // Process each selected file
        for file_number in files_to_clean {
            // Check shutdown before processing each file
            if self.shutdown.load(Ordering::Relaxed) {
                break;
            }

            // Protect file during processing
            self.file_protector.protect_file(file_number, "CleanerProcessing");

            // Process the file
            let result = self.process_single_file(file_number)?;

            // Unprotect after processing
            self.file_protector.unprotect_file(file_number);

            if result.completed {
                files_cleaned += 1;
                total_entries += result.entries_read;

                // Update statistics
                self.update_stats(&result);

                // Mark file as cleaned in selector
                self.file_selector.lock().mark_file_cleaned(file_number);

                // Mark file for deletion
                self.pending_deletions.lock().push(file_number);
            }
        }

        // Attempt to delete pending files
        let files_deleted = self.delete_pending_files();

        Ok(CleanResult {
            files_cleaned,
            files_deleted,
            total_entries_read: total_entries,
        })
    }

    /// Processes a single file for cleaning.
    fn process_single_file(
        &self,
        file_number: u32,
    ) -> Result<FileProcessResult, String> {
        // Create a dummy file summary for now
        // TODO: Get actual file summary from UtilizationProfile when integrated
        let file_summary = crate::FileSummary::new();

        // Create file processor and process the file
        let processor =
            FileProcessor::new(self.stats.clone(), self.shutdown.clone());
        processor.process_file_no_entries(file_number, &file_summary)
    }

    /// Updates statistics from a file processing result.
    fn update_stats(&self, result: &FileProcessResult) {
        self.stats
            .entries_read
            .fetch_add(result.entries_read, Ordering::Relaxed);
        self.stats.lns_cleaned.fetch_add(result.lns_cleaned, Ordering::Relaxed);
        self.stats.lns_dead.fetch_add(result.lns_dead, Ordering::Relaxed);
        self.stats
            .lns_migrated
            .fetch_add(result.lns_migrated, Ordering::Relaxed);
        self.stats
            .lns_obsolete
            .fetch_add(result.lns_obsolete, Ordering::Relaxed);
        self.stats.lns_locked.fetch_add(result.lns_locked, Ordering::Relaxed);
        self.stats.ins_cleaned.fetch_add(result.ins_cleaned, Ordering::Relaxed);
        self.stats.ins_dead.fetch_add(result.ins_dead, Ordering::Relaxed);
        self.stats
            .ins_migrated
            .fetch_add(result.ins_migrated, Ordering::Relaxed);
        self.stats
            .ins_obsolete
            .fetch_add(result.ins_obsolete, Ordering::Relaxed);
        self.stats
            .bin_deltas_cleaned
            .fetch_add(result.bin_deltas_cleaned, Ordering::Relaxed);
        self.stats
            .bin_deltas_dead
            .fetch_add(result.bin_deltas_dead, Ordering::Relaxed);
        self.stats
            .bin_deltas_migrated
            .fetch_add(result.bin_deltas_migrated, Ordering::Relaxed);
        self.stats
            .bin_deltas_obsolete
            .fetch_add(result.bin_deltas_obsolete, Ordering::Relaxed);
    }

    /// Deletes files that are safe to delete (not protected).
    ///
    /// Returns the number of files successfully deleted.
    fn delete_pending_files(&self) -> u32 {
        let mut pending = self.pending_deletions.lock();
        let mut deleted = 0u32;

        pending.retain(|&file_number| {
            if !self.file_protector.is_protected(file_number) {
                // File is not protected - safe to delete
                // TODO: Actual file deletion will be integrated with FileManager
                deleted += 1;
                self.stats.deletions.fetch_add(1, Ordering::Relaxed);
                false // Remove from pending list
            } else {
                true // Keep in pending list
            }
        });

        deleted
    }

    /// Adds a file to the list of files to clean.
    ///
    /// Useful for manual cleaning or prioritizing specific files.
    pub fn add_file_to_clean(&self, file_number: u32) {
        let mut selector = self.file_selector.lock();
        selector.add_file_to_clean(file_number);
    }

    /// Returns a reference to the file selector (for testing/introspection).
    pub fn get_file_selector(&self) -> &Mutex<FileSelector> {
        &self.file_selector
    }

    /// Returns a reference to the file protector.
    pub fn get_file_protector(&self) -> &FileProtector {
        &self.file_protector
    }

    /// Returns a reference to the statistics.
    pub fn get_stats(&self) -> &Arc<CleanerStats> {
        &self.stats
    }

    /// Returns whether the cleaner is currently running.
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }

    /// Signals the cleaner to shut down.
    ///
    /// This will cause in-progress cleaning to stop at the next checkpoint.
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
    }

    /// Requests that the given files be deleted once they are no longer protected.
    pub fn request_delete_files(&self, files: &[u32]) {
        let mut pending = self.pending_deletions.lock();
        pending.extend_from_slice(files);
    }

    /// Returns the total number of cleaning runs performed.
    pub fn get_run_count(&self) -> u64 {
        self.n_runs.load(Ordering::Relaxed)
    }
}

/// RAII guard to ensure the running flag is cleared on drop.
struct RunningGuard<'a> {
    running: &'a AtomicBool,
}

impl<'a> RunningGuard<'a> {
    fn new(running: &'a AtomicBool) -> Self {
        Self { running }
    }
}

impl<'a> Drop for RunningGuard<'a> {
    fn drop(&mut self) {
        self.running.store(false, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_cleaner() {
        let cleaner = Cleaner::new(50, 5, 60);
        assert!(!cleaner.is_running());
        assert_eq!(cleaner.min_utilization, 50);
        assert_eq!(cleaner.min_file_count, 5);
        assert_eq!(cleaner.min_age, 60);
        assert_eq!(cleaner.get_run_count(), 0);
    }

    #[test]
    fn test_cleaner_with_max_utilization() {
        let cleaner = Cleaner::new(150, 5, 60); // Over 100
        assert_eq!(cleaner.min_utilization, 100); // Should be clamped
    }

    #[test]
    fn test_do_clean_not_running() {
        let cleaner = Cleaner::new(50, 0, 0);
        assert!(!cleaner.is_running());

        // Should return immediately with no files (selector is empty)
        let result = cleaner.do_clean(1, false).unwrap();
        assert_eq!(result.files_cleaned, 0);
        assert_eq!(result.files_deleted, 0);
    }

    #[test]
    fn test_do_clean_increments_run_count() {
        let cleaner = Cleaner::new(50, 0, 0);
        assert_eq!(cleaner.get_run_count(), 0);

        let _ = cleaner.do_clean(1, false);
        assert_eq!(cleaner.get_run_count(), 1);

        let _ = cleaner.do_clean(1, false);
        assert_eq!(cleaner.get_run_count(), 2);
    }

    #[test]
    fn test_concurrent_clean_rejected() {
        let cleaner = Arc::new(Cleaner::new(50, 0, 0));

        // Simulate a long-running clean by holding the running flag
        cleaner.running.store(true, Ordering::Relaxed);

        // Second clean attempt should fail
        let result = cleaner.do_clean(1, false);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("already running"));

        // Clean up
        cleaner.running.store(false, Ordering::Relaxed);
    }

    #[test]
    fn test_shutdown() {
        let cleaner = Cleaner::new(50, 0, 0);
        assert!(!cleaner.shutdown.load(Ordering::Relaxed));

        cleaner.shutdown();
        assert!(cleaner.shutdown.load(Ordering::Relaxed));

        // Cleaning should fail after shutdown
        let result = cleaner.do_clean(1, false);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("shut down"));
    }

    #[test]
    fn test_add_file_to_clean() {
        let cleaner = Cleaner::new(50, 0, 0);

        cleaner.add_file_to_clean(5);
        cleaner.add_file_to_clean(10);

        let selector = cleaner.get_file_selector().lock();
        assert!(selector.is_tracked(5));
        assert!(selector.is_tracked(10));
    }

    #[test]
    fn test_file_protector_integration() {
        let cleaner = Cleaner::new(50, 0, 0);

        let protector = cleaner.get_file_protector();
        protector.protect_file(5, "Test");

        assert!(protector.is_protected(5));
        assert!(!protector.is_protected(6));
    }

    #[test]
    fn test_stats_integration() {
        let cleaner = Cleaner::new(50, 0, 0);

        let stats = cleaner.get_stats();
        stats.lns_cleaned.fetch_add(100, Ordering::Relaxed);

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.lns_cleaned, 100);
    }

    #[test]
    fn test_request_delete_files() {
        let cleaner = Cleaner::new(50, 0, 0);

        cleaner.request_delete_files(&[1, 2, 3]);

        let pending = cleaner.pending_deletions.lock();
        assert_eq!(pending.len(), 3);
        assert!(pending.contains(&1));
        assert!(pending.contains(&2));
        assert!(pending.contains(&3));
    }

    #[test]
    fn test_delete_pending_files_when_protected() {
        let cleaner = Cleaner::new(50, 0, 0);

        // Add files to pending deletion
        cleaner.request_delete_files(&[1, 2, 3]);

        // Protect file 2
        cleaner.get_file_protector().protect_file(2, "Test");

        // Attempt deletion
        let deleted = cleaner.delete_pending_files();

        // Should delete 1 and 3, but not 2
        assert_eq!(deleted, 2);

        let pending = cleaner.pending_deletions.lock();
        assert_eq!(pending.len(), 1);
        assert!(pending.contains(&2));
    }

    #[test]
    fn test_running_guard() {
        let running = AtomicBool::new(false);

        {
            running.store(true, Ordering::Relaxed);
            let _guard = RunningGuard::new(&running);
            assert!(running.load(Ordering::Relaxed));
        } // Guard drops here

        assert!(!running.load(Ordering::Relaxed));
    }

    #[test]
    fn test_clean_result() {
        let result = CleanResult {
            files_cleaned: 5,
            files_deleted: 4,
            total_entries_read: 10000,
        };

        assert_eq!(result.files_cleaned, 5);
        assert_eq!(result.files_deleted, 4);
        assert_eq!(result.total_entries_read, 10000);
    }

    #[test]
    fn test_clean_result_equality() {
        let result1 = CleanResult {
            files_cleaned: 5,
            files_deleted: 4,
            total_entries_read: 10000,
        };

        let result2 = CleanResult {
            files_cleaned: 5,
            files_deleted: 4,
            total_entries_read: 10000,
        };

        let result3 = CleanResult {
            files_cleaned: 6,
            files_deleted: 4,
            total_entries_read: 10000,
        };

        assert_eq!(result1, result2);
        assert_ne!(result1, result3);
    }

    #[test]
    fn test_do_clean_with_file_to_clean() {
        let cleaner = Cleaner::new(50, 0, 0);
        // Add a file to the selector so do_clean has work to do.
        cleaner.add_file_to_clean(7);

        let result = cleaner.do_clean(5, false).unwrap();
        // process_single_file calls process_file_no_entries → completed=true
        assert_eq!(result.files_cleaned, 1);
        // The file was not protected so it should be deleted immediately.
        assert_eq!(result.files_deleted, 1);
        assert_eq!(result.total_entries_read, 0);
    }

    #[test]
    fn test_do_clean_multiple_files() {
        let cleaner = Cleaner::new(50, 0, 0);
        cleaner.add_file_to_clean(1);
        cleaner.add_file_to_clean(2);
        cleaner.add_file_to_clean(3);

        let result = cleaner.do_clean(10, false).unwrap();
        assert_eq!(result.files_cleaned, 3);
        assert_eq!(result.files_deleted, 3);
    }

    #[test]
    fn test_do_clean_respects_n_files_limit() {
        let cleaner = Cleaner::new(50, 0, 0);
        cleaner.add_file_to_clean(10);
        cleaner.add_file_to_clean(11);
        cleaner.add_file_to_clean(12);

        // Only allow cleaning 1 file at a time.
        let result = cleaner.do_clean(1, false).unwrap();
        assert_eq!(result.files_cleaned, 1);
    }

    #[test]
    fn test_do_clean_increments_stats_runs() {
        let cleaner = Cleaner::new(50, 0, 0);
        let _ = cleaner.do_clean(1, false);
        let _ = cleaner.do_clean(1, false);

        let snapshot = cleaner.get_stats().snapshot();
        assert_eq!(snapshot.runs, 2);
    }

    #[test]
    fn test_do_clean_updates_entry_stats() {
        let cleaner = Cleaner::new(50, 0, 0);
        cleaner.add_file_to_clean(5);

        let _ = cleaner.do_clean(5, false).unwrap();

        // process_file_no_entries returns 0 entries_read but completed=true.
        let snapshot = cleaner.get_stats().snapshot();
        // runs incremented, deletions incremented
        assert_eq!(snapshot.runs, 1);
        assert_eq!(snapshot.deletions, 1);
    }

    #[test]
    fn test_do_clean_running_flag_cleared_after_completion() {
        let cleaner = Cleaner::new(50, 0, 0);
        assert!(!cleaner.is_running());

        let _ = cleaner.do_clean(1, false);

        // The running flag must be cleared after do_clean returns.
        assert!(!cleaner.is_running());
    }

    #[test]
    fn test_do_clean_file_protected_stays_pending() {
        let cleaner = Cleaner::new(50, 0, 0);
        cleaner.add_file_to_clean(42);

        // Protect the file before cleaning — deletion should be deferred.
        cleaner.get_file_protector().protect_file(42, "Hold");

        let result = cleaner.do_clean(5, false).unwrap();
        assert_eq!(result.files_cleaned, 1); // cleaned (processed)
        assert_eq!(result.files_deleted, 0); // but not deleted yet

        // Still in pending list.
        let pending = cleaner.pending_deletions.lock();
        assert!(pending.contains(&42));
    }

    #[test]
    fn test_do_clean_shutdown_during_file_loop() {
        let cleaner = Cleaner::new(50, 0, 0);
        cleaner.add_file_to_clean(1);
        cleaner.add_file_to_clean(2);
        cleaner.add_file_to_clean(3);

        // Shut down before calling do_clean.
        cleaner.shutdown();
        let result = cleaner.do_clean(10, false);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("shut down"));
    }

    #[test]
    fn test_get_file_selector_returns_selector() {
        let cleaner = Cleaner::new(50, 0, 0);
        cleaner.add_file_to_clean(99);
        let selector = cleaner.get_file_selector().lock();
        assert!(selector.is_tracked(99));
    }

    #[test]
    fn test_get_file_protector_returns_protector() {
        let cleaner = Cleaner::new(50, 0, 0);
        let protector = cleaner.get_file_protector();
        protector.protect_file(77, "Test");
        assert!(protector.is_protected(77));
    }

    #[test]
    fn test_get_stats_returns_stats_ref() {
        let cleaner = Cleaner::new(50, 0, 0);
        let stats = cleaner.get_stats();
        stats.runs.fetch_add(5, Ordering::Relaxed);
        assert_eq!(cleaner.get_stats().runs.load(Ordering::Relaxed), 5);
    }

    #[test]
    fn test_request_delete_files_empty_slice() {
        let cleaner = Cleaner::new(50, 0, 0);
        cleaner.request_delete_files(&[]);
        let pending = cleaner.pending_deletions.lock();
        assert!(pending.is_empty());
    }

    #[test]
    fn test_delete_pending_all_unprotected() {
        let cleaner = Cleaner::new(50, 0, 0);
        cleaner.request_delete_files(&[10, 20, 30]);

        let deleted = cleaner.delete_pending_files();
        assert_eq!(deleted, 3);

        let pending = cleaner.pending_deletions.lock();
        assert!(pending.is_empty());
    }

    #[test]
    fn test_delete_pending_increments_deletions_stat() {
        let cleaner = Cleaner::new(50, 0, 0);
        cleaner.request_delete_files(&[5, 6]);

        cleaner.delete_pending_files();

        let snapshot = cleaner.get_stats().snapshot();
        assert_eq!(snapshot.deletions, 2);
    }

    #[test]
    fn test_clean_result_clone() {
        let result = CleanResult {
            files_cleaned: 3,
            files_deleted: 2,
            total_entries_read: 500,
        };
        let cloned = result.clone();
        assert_eq!(cloned, result);
    }

    #[test]
    fn test_min_utilization_zero() {
        let cleaner = Cleaner::new(0, 0, 0);
        assert_eq!(cleaner.min_utilization, 0);
    }

    #[test]
    fn test_min_age_large() {
        let cleaner = Cleaner::new(50, 0, u64::MAX);
        assert_eq!(cleaner.min_age, u64::MAX);
    }
}
