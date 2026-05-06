//! File selection for cleaning.
//!
//! Port of `com.sleepycat.je.cleaner.FileSelector` - keeps track of the status of files
//! for which cleaning is in progress.
//!
//! The cost/benefit file scoring algorithm is ported from
//! `UtilizationCalculator.getBestFile()` in JE.  JE selects files using
//! average utilization: the file whose `(minUtil + maxUtil) / 2` is lowest
//! is the best candidate.  For our simplified model (no TTL/expiration):
//!
//!   obsolete_bytes = summary.get_obsolete_size()
//!   minUtil = 100 * (total - obsolete) / total   (active fraction)
//!   maxUtil = minUtil  (no expiration contribution)
//!   avgUtil = minUtil
//!
//! So the file with the **lowest utilization** (= highest obsolete fraction)
//! is chosen, subject to:
//!   - `file_number <= last_file_to_clean` (age filter, JE: `fileNum <= lastFileToClean`)
//!   - file not already in-progress (being cleaned)
//!   - file not in the `to_be_cleaned` queue already
//!
//! When `force_cleaning` is `true`, selection ignores the utilization
//! threshold and always returns the best file.

use crate::file_summary::FileSummary;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

/// Status of a file in the cleaning pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileStatus {
    /// File has been selected for cleaning but processing has not started.
    ToBeCleaned,
    /// File is currently being cleaned by a cleaner thread.
    BeingCleaned,
    /// File has been cleaned but not yet checkpointed.
    Cleaned,
    /// File has been checkpointed after cleaning.
    Checkpointed,
    /// File is fully processed and safe to delete.
    FullyProcessed,
}

/// Information about a file being cleaned.
#[derive(Debug, Clone)]
struct FileInfo {
    status: FileStatus,
    required_util: Option<i32>,
}

/// Checkpoint state snapshot for cleaned files.
#[derive(Debug, Clone, Default)]
pub struct CheckpointStartCleanerState {
    /// Files that were cleaned at checkpoint start.
    pub cleaned_files: Vec<u32>,
}

/// Tracks the status of files for which cleaning is in progress.
#[derive(Debug)]
pub struct FileSelector {
    /// Map of file number to file info.
    file_info: HashMap<u32, FileInfo>,
    /// Files waiting to be cleaned.
    to_be_cleaned: VecDeque<u32>,
    /// Files currently being cleaned.
    being_cleaned: HashSet<u32>,
    /// Files cleaned but not yet checkpointed.
    cleaned: HashSet<u32>,
    /// Files that have been checkpointed.
    checkpointed: HashSet<u32>,
    /// Files that are safe to delete.
    safe_to_delete: HashSet<u32>,
    /// Two-pass cleaning: required utilization threshold for next selection pass.
    ///
    /// When a first pass fails to reclaim enough space, `check_for_required_util`
    /// raises this threshold and sets `force_cleaning=true` to force a second pass
    /// targeting lower-utilization files.
    ///
    /// Port of `FileSelector.requiredUtil` in JE.
    required_util: Option<i32>,
    /// Two-pass cleaning: if true, bypass normal utilization threshold and
    /// always select the best candidate file.
    ///
    /// Port of `FileSelector.forceCleaning` in JE.
    force_cleaning: bool,
}

impl FileSelector {
    /// Creates a new empty file selector.
    pub fn new() -> Self {
        Self {
            file_info: HashMap::new(),
            to_be_cleaned: VecDeque::new(),
            being_cleaned: HashSet::new(),
            cleaned: HashSet::new(),
            checkpointed: HashSet::new(),
            safe_to_delete: HashSet::new(),
            required_util: None,
            force_cleaning: false,
        }
    }

    /// Checks whether a second cleaning pass is required.
    ///
    /// Called after each cleaning pass completes.  If `actual_util` is still
    /// above `target_util`, raises `required_util` by the gap and enables
    /// `force_cleaning` for the next pass.
    ///
    /// Port of `FileSelector.checkForRequiredUtilization()` in JE.
    pub fn check_for_required_util(&mut self, actual_util: i32, target_util: i32) {
        if actual_util > target_util {
            // Raise the threshold by the shortfall, capped at 100.
            let gap = actual_util - target_util;
            let new_req = actual_util.saturating_add(gap).min(100);
            self.required_util = Some(new_req);
            self.force_cleaning = true;
        } else {
            self.required_util = None;
            self.force_cleaning = false;
        }
    }

    /// Returns the current required utilization threshold (`None` if none set).
    ///
    /// Port of `FileSelector.getRequiredUtil()` in JE.
    pub fn required_util(&self) -> Option<i32> {
        self.required_util
    }

    /// Returns true if force-cleaning mode is active.
    pub fn is_force_cleaning(&self) -> bool {
        self.force_cleaning
    }

    /// Selects the next file for cleaning from the queue.
    ///
    /// Returns the file number and optional required utilization, or None if no file is available.
    pub fn select_file_for_cleaning(&mut self) -> Option<(u32, Option<i32>)> {
        if let Some(file_number) = self.to_be_cleaned.pop_front() {
            self.being_cleaned.insert(file_number);

            if let Some(info) = self.file_info.get_mut(&file_number) {
                info.status = FileStatus::BeingCleaned;
                return Some((file_number, info.required_util));
            }
        }
        None
    }

    /// Selects the best file for cleaning using JE's cost/benefit scoring.
    ///
    /// This is a port of the file-selection logic inside
    /// `UtilizationCalculator.getBestFile()` and `FileSelector.selectFileForCleaning()`
    /// in JE.
    ///
    /// Algorithm:
    /// 1. If there is already a file queued in `to_be_cleaned`, return it
    ///    immediately (it was enqueued by a prior call).
    /// 2. Otherwise, scan `file_summaries` (a sorted map of file_number →
    ///    FileSummary) and pick the file with the lowest average utilization,
    ///    subject to:
    ///    - The file must not already be in-progress (being_cleaned / cleaned /
    ///      checkpointed / safe_to_delete queues).
    ///    - `file_number <= last_file_to_clean` (age filter).
    ///    - The file must qualify: either `force_cleaning` is true, or the
    ///      file's utilization is below `min_utilization_pct`.
    /// 3. If a qualifying file is found, mark it as `BeingCleaned` and return
    ///    it.
    ///
    /// # Arguments
    /// * `file_summaries` — sorted (BTreeMap) map of file_number → summary.
    ///   Must be sorted by file number so the last key gives the newest file
    ///   (JE: `fileSummaryMap.lastKey()`).
    /// * `min_utilization_pct` — 0–100 integer threshold; files whose utilization
    ///   is at or above this are not cleaned unless `force_cleaning`.
    /// * `min_age` — minimum age (distance in file numbers from the newest file)
    ///   before a file may be cleaned. JE default is 2.
    /// * `force_cleaning` — if true, bypass the utilization threshold and always
    ///   select the best file (used in testing).
    ///
    /// # Returns
    /// `Some((file_number, required_util))` where `required_util` is the
    /// utilization target from the two-pass cleaning logic (non-None when
    /// `self.force_cleaning` is set after a first pass didn't meet the
    /// target), or `None` if no file qualifies.
    pub fn select_file_for_cleaning_with_profile(
        &mut self,
        file_summaries: &BTreeMap<u32, FileSummary>,
        min_utilization_pct: u32,
        min_age: u32,
        force_cleaning: bool,
    ) -> Option<(u32, Option<i32>)> {
        // Step 1 — if a file is already queued (from a previous scoring pass
        // that enqueued it but didn't immediately return), dequeue it now.
        if !self.to_be_cleaned.is_empty() {
            return self.select_file_for_cleaning();
        }

        if file_summaries.is_empty() {
            return None;
        }

        // The newest (highest-numbered) file is the "first active" file.
        // JE: firstActiveFile = fileSummaryMap.lastKey()
        let newest_file = *file_summaries.keys().next_back()?;

        // lastFileToClean = firstActiveFile - minAge
        // Any file with file_number > last_file_to_clean is too young to clean.
        // Use saturating_sub so that if min_age > newest_file we get 0.
        let last_file_to_clean = newest_file.saturating_sub(min_age);

        // Collect all in-progress file numbers (not eligible for re-selection).
        let in_progress: HashSet<u32> = self.file_info.keys().copied().collect();

        // Step 2 — find the file with lowest average utilization.
        // Port of JE: pick the file where (thisMinUtil + thisMaxUtil) / 2 is
        // minimised.  Without expiration, minUtil == maxUtil == avgUtil, so we
        // simply rank by utilization() ascending.
        let mut best_file: Option<u32> = None;
        let mut best_avg_util: i32 = 101; // higher than any valid utilization

        for (&file_num, summary) in file_summaries.iter() {
            // Skip in-progress files.
            if in_progress.contains(&file_num) {
                continue;
            }

            // Skip files that are too young (JE: fileNum > lastFileToClean).
            if file_num > last_file_to_clean {
                continue;
            }

            // Skip empty summaries.
            if summary.is_empty() {
                continue;
            }

            // Calculate average utilization (0–100 integer percent).
            // Port of JE: FileSummary.utilization(maxObsoleteSize, totalSize)
            // Without expiration: minUtil == maxUtil == utilization().
            let avg_util = Self::utilization_pct(summary);

            // Apply the utilization threshold filter.
            // During a second pass (`self.force_cleaning`), override the caller's
            // threshold with `self.required_util` if it is stricter (lower).
            // Port of JE: FileSelector picks files below requiredUtil when
            // forceCleaning is active.
            let effective_threshold = if self.force_cleaning {
                self.required_util.unwrap_or(min_utilization_pct as i32)
                    .min(min_utilization_pct as i32)
            } else {
                min_utilization_pct as i32
            };
            if !force_cleaning && !self.force_cleaning && avg_util >= effective_threshold {
                continue;
            }

            if best_file.is_none() || avg_util < best_avg_util {
                best_file = Some(file_num);
                best_avg_util = avg_util;
            }
        }

        let file_num = best_file?;

        // Step 3 — mark the chosen file as being cleaned.
        self.being_cleaned.insert(file_num);
        self.file_info.insert(
            file_num,
            FileInfo { status: FileStatus::BeingCleaned, required_util: None },
        );

        Some((file_num, None))
    }

    /// Returns the utilization of a file as an integer percentage 0–100.
    ///
    /// Port of `FileSummary.utilization(obsoleteSize, totalSize)` in JE.
    /// A file at 100% utilization has no obsolete bytes; 0% means all bytes
    /// are obsolete.
    fn utilization_pct(summary: &FileSummary) -> i32 {
        if summary.total_size <= 0 {
            return 0;
        }
        let active = summary.get_active_size();
        // Clamp to [0, 100].
        ((active as i64 * 100) / summary.total_size as i64).clamp(0, 100) as i32
    }

    /// Adds a file to the cleaning queue.
    pub fn add_file_to_clean(&mut self, file_number: u32) {
        self.add_file_to_clean_with_util(file_number, None);
    }

    /// Adds a file to the cleaning queue with a required utilization.
    pub fn add_file_to_clean_with_util(
        &mut self,
        file_number: u32,
        required_util: Option<i32>,
    ) {
        if !self.file_info.contains_key(&file_number) {
            self.to_be_cleaned.push_back(file_number);
            self.file_info.insert(
                file_number,
                FileInfo { status: FileStatus::ToBeCleaned, required_util },
            );
        }
    }

    /// Marks a file as cleaned (processing complete).
    pub fn mark_file_cleaned(&mut self, file_number: u32) {
        self.being_cleaned.remove(&file_number);
        self.cleaned.insert(file_number);

        if let Some(info) = self.file_info.get_mut(&file_number) {
            info.status = FileStatus::Cleaned;
        }
    }

    /// Marks a file as checkpointed.
    pub fn mark_file_checkpointed(&mut self, file_number: u32) {
        self.cleaned.remove(&file_number);
        self.checkpointed.insert(file_number);

        if let Some(info) = self.file_info.get_mut(&file_number) {
            info.status = FileStatus::Checkpointed;
        }
    }

    /// Marks a file as fully processed and safe to delete.
    pub fn mark_file_fully_processed(&mut self, file_number: u32) {
        self.checkpointed.remove(&file_number);
        self.safe_to_delete.insert(file_number);

        if let Some(info) = self.file_info.get_mut(&file_number) {
            info.status = FileStatus::FullyProcessed;
        }
    }

    /// Returns whether a file is currently being cleaned.
    pub fn is_being_cleaned(&self, file_number: u32) -> bool {
        self.being_cleaned.contains(&file_number)
    }

    /// Returns whether a file is in the system (in any state).
    pub fn is_tracked(&self, file_number: u32) -> bool {
        self.file_info.contains_key(&file_number)
    }

    /// Returns the status of a file.
    pub fn get_file_status(&self, file_number: u32) -> Option<FileStatus> {
        self.file_info.get(&file_number).map(|info| info.status)
    }

    /// Returns files that are safe to delete.
    pub fn get_safe_to_delete(&self) -> Vec<u32> {
        let mut files: Vec<u32> = self.safe_to_delete.iter().copied().collect();
        files.sort_unstable();
        files
    }

    /// Removes a file from the safe-to-delete set (after deletion).
    pub fn remove_deleted_file(&mut self, file_number: u32) {
        self.safe_to_delete.remove(&file_number);
        self.file_info.remove(&file_number);
    }

    /// Returns a checkpoint state snapshot.
    pub fn get_checkpoint_state(&self) -> CheckpointStartCleanerState {
        let mut cleaned_files: Vec<u32> =
            self.cleaned.iter().copied().collect();
        cleaned_files.sort_unstable();

        CheckpointStartCleanerState { cleaned_files }
    }

    /// Processes files at checkpoint end.
    ///
    /// Moves cleaned files to checkpointed status.
    pub fn process_checkpoint_end(
        &mut self,
        state: &CheckpointStartCleanerState,
    ) {
        for &file_number in &state.cleaned_files {
            if self.cleaned.contains(&file_number) {
                self.mark_file_checkpointed(file_number);
            }
        }
    }

    /// Returns the number of files in each state.
    pub fn get_stats(&self) -> FileSelectorStats {
        FileSelectorStats {
            to_be_cleaned: self.to_be_cleaned.len(),
            being_cleaned: self.being_cleaned.len(),
            cleaned: self.cleaned.len(),
            checkpointed: self.checkpointed.len(),
            safe_to_delete: self.safe_to_delete.len(),
        }
    }

    /// Clears all state (for testing).
    pub fn clear(&mut self) {
        self.file_info.clear();
        self.to_be_cleaned.clear();
        self.being_cleaned.clear();
        self.cleaned.clear();
        self.checkpointed.clear();
        self.safe_to_delete.clear();
    }
}

impl Default for FileSelector {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics about file selector state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileSelectorStats {
    pub to_be_cleaned: usize,
    pub being_cleaned: usize,
    pub cleaned: usize,
    pub checkpointed: usize,
    pub safe_to_delete: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let selector = FileSelector::new();
        let stats = selector.get_stats();
        assert_eq!(stats.to_be_cleaned, 0);
        assert_eq!(stats.being_cleaned, 0);
    }

    #[test]
    fn test_add_file_to_clean() {
        let mut selector = FileSelector::new();
        selector.add_file_to_clean(1);

        assert!(selector.is_tracked(1));
        assert_eq!(selector.get_file_status(1), Some(FileStatus::ToBeCleaned));

        let stats = selector.get_stats();
        assert_eq!(stats.to_be_cleaned, 1);
    }

    #[test]
    fn test_select_file_for_cleaning() {
        let mut selector = FileSelector::new();
        selector.add_file_to_clean(1);
        selector.add_file_to_clean(2);

        let result = selector.select_file_for_cleaning();
        assert_eq!(result, Some((1, None)));
        assert!(selector.is_being_cleaned(1));
        assert_eq!(selector.get_file_status(1), Some(FileStatus::BeingCleaned));

        let stats = selector.get_stats();
        assert_eq!(stats.to_be_cleaned, 1);
        assert_eq!(stats.being_cleaned, 1);
    }

    #[test]
    fn test_select_file_empty() {
        let mut selector = FileSelector::new();
        let result = selector.select_file_for_cleaning();
        assert_eq!(result, None);
    }

    #[test]
    fn test_mark_file_cleaned() {
        let mut selector = FileSelector::new();
        selector.add_file_to_clean(1);
        selector.select_file_for_cleaning();

        selector.mark_file_cleaned(1);

        assert!(!selector.is_being_cleaned(1));
        assert_eq!(selector.get_file_status(1), Some(FileStatus::Cleaned));

        let stats = selector.get_stats();
        assert_eq!(stats.being_cleaned, 0);
        assert_eq!(stats.cleaned, 1);
    }

    #[test]
    fn test_mark_file_checkpointed() {
        let mut selector = FileSelector::new();
        selector.add_file_to_clean(1);
        selector.select_file_for_cleaning();
        selector.mark_file_cleaned(1);

        selector.mark_file_checkpointed(1);

        assert_eq!(selector.get_file_status(1), Some(FileStatus::Checkpointed));

        let stats = selector.get_stats();
        assert_eq!(stats.cleaned, 0);
        assert_eq!(stats.checkpointed, 1);
    }

    #[test]
    fn test_mark_file_fully_processed() {
        let mut selector = FileSelector::new();
        selector.add_file_to_clean(1);
        selector.select_file_for_cleaning();
        selector.mark_file_cleaned(1);
        selector.mark_file_checkpointed(1);

        selector.mark_file_fully_processed(1);

        assert_eq!(
            selector.get_file_status(1),
            Some(FileStatus::FullyProcessed)
        );

        let stats = selector.get_stats();
        assert_eq!(stats.checkpointed, 0);
        assert_eq!(stats.safe_to_delete, 1);
    }

    #[test]
    fn test_get_safe_to_delete() {
        let mut selector = FileSelector::new();

        for i in 1..=3 {
            selector.add_file_to_clean(i);
            selector.select_file_for_cleaning();
            selector.mark_file_cleaned(i);
            selector.mark_file_checkpointed(i);
            selector.mark_file_fully_processed(i);
        }

        let safe = selector.get_safe_to_delete();
        assert_eq!(safe, vec![1, 2, 3]);
    }

    #[test]
    fn test_remove_deleted_file() {
        let mut selector = FileSelector::new();
        selector.add_file_to_clean(1);
        selector.select_file_for_cleaning();
        selector.mark_file_cleaned(1);
        selector.mark_file_checkpointed(1);
        selector.mark_file_fully_processed(1);

        selector.remove_deleted_file(1);

        assert!(!selector.is_tracked(1));
        assert_eq!(selector.get_safe_to_delete(), vec![]);
    }

    #[test]
    fn test_checkpoint_state() {
        let mut selector = FileSelector::new();

        selector.add_file_to_clean(1);
        selector.select_file_for_cleaning();
        selector.mark_file_cleaned(1);

        selector.add_file_to_clean(2);
        selector.select_file_for_cleaning();
        selector.mark_file_cleaned(2);

        let state = selector.get_checkpoint_state();
        assert_eq!(state.cleaned_files, vec![1, 2]);
    }

    #[test]
    fn test_process_checkpoint_end() {
        let mut selector = FileSelector::new();

        selector.add_file_to_clean(1);
        selector.select_file_for_cleaning();
        selector.mark_file_cleaned(1);

        let state = selector.get_checkpoint_state();
        selector.process_checkpoint_end(&state);

        assert_eq!(selector.get_file_status(1), Some(FileStatus::Checkpointed));

        let stats = selector.get_stats();
        assert_eq!(stats.cleaned, 0);
        assert_eq!(stats.checkpointed, 1);
    }

    #[test]
    fn test_add_file_with_util() {
        let mut selector = FileSelector::new();
        selector.add_file_to_clean_with_util(1, Some(50));

        let result = selector.select_file_for_cleaning();
        assert_eq!(result, Some((1, Some(50))));
    }

    #[test]
    fn test_fifo_order() {
        let mut selector = FileSelector::new();
        selector.add_file_to_clean(1);
        selector.add_file_to_clean(2);
        selector.add_file_to_clean(3);

        assert_eq!(selector.select_file_for_cleaning(), Some((1, None)));
        assert_eq!(selector.select_file_for_cleaning(), Some((2, None)));
        assert_eq!(selector.select_file_for_cleaning(), Some((3, None)));
    }

    #[test]
    fn test_duplicate_add() {
        let mut selector = FileSelector::new();
        selector.add_file_to_clean(1);
        selector.add_file_to_clean(1); // Should be ignored

        let stats = selector.get_stats();
        assert_eq!(stats.to_be_cleaned, 1);
    }

    #[test]
    fn test_clear() {
        let mut selector = FileSelector::new();
        selector.add_file_to_clean(1);
        selector.select_file_for_cleaning();

        selector.clear();

        let stats = selector.get_stats();
        assert_eq!(stats.to_be_cleaned, 0);
        assert_eq!(stats.being_cleaned, 0);
        assert!(!selector.is_tracked(1));
    }

    #[test]
    fn test_full_lifecycle() {
        let mut selector = FileSelector::new();

        // Add file
        selector.add_file_to_clean(42);
        assert_eq!(selector.get_file_status(42), Some(FileStatus::ToBeCleaned));

        // Select for cleaning
        let result = selector.select_file_for_cleaning();
        assert_eq!(result, Some((42, None)));
        assert_eq!(
            selector.get_file_status(42),
            Some(FileStatus::BeingCleaned)
        );

        // Mark cleaned
        selector.mark_file_cleaned(42);
        assert_eq!(selector.get_file_status(42), Some(FileStatus::Cleaned));

        // Checkpoint
        selector.mark_file_checkpointed(42);
        assert_eq!(
            selector.get_file_status(42),
            Some(FileStatus::Checkpointed)
        );

        // Fully process
        selector.mark_file_fully_processed(42);
        assert_eq!(
            selector.get_file_status(42),
            Some(FileStatus::FullyProcessed)
        );

        // Delete
        selector.remove_deleted_file(42);
        assert!(!selector.is_tracked(42));
    }

    // ── select_file_for_cleaning_with_profile tests ───────────────────────────

    /// Build a FileSummary with explicit total/obsolete sizes.
    fn make_summary(total: i32, obsolete_ln: i32) -> FileSummary {
        FileSummary {
            total_count: 10,
            total_size: total,
            total_ln_count: 10,
            total_ln_size: total,
            obsolete_ln_count: 1,
            obsolete_ln_size: obsolete_ln,
            obsolete_ln_size_counted: 1,
            ..Default::default()
        }
    }

    /// Populate a BTreeMap with (file_num, summary) pairs.
    fn make_profile(entries: &[(u32, i32, i32)]) -> BTreeMap<u32, FileSummary> {
        let mut map = BTreeMap::new();
        for &(file_num, total, obsolete) in entries {
            map.insert(file_num, make_summary(total, obsolete));
        }
        map
    }

    #[test]
    fn test_select_with_profile_picks_lowest_util() {
        // Three files with utilizations 10%, 30%, 50% (obsolete fractions 90%, 70%, 50%).
        // File 0 is newest; files 1–3 are candidates (min_age = 1 means file 0 is skipped).
        // Correction: with min_age=1 and newest=3, last_file_to_clean = 3-1 = 2.
        // File 3 (newest) is skipped; files 1 and 2 are candidates.
        // File 1: util 10% (900 obsolete / 1000 total).
        // File 2: util 50% (500 obsolete / 1000 total).
        // Threshold 60% means both qualify. File 1 should be chosen.
        let profile = make_profile(&[
            (1, 1000, 900), // 10% util
            (2, 1000, 500), // 50% util
            (3, 1000, 100), // 90% util — newest, skipped by age filter
        ]);

        let mut selector = FileSelector::new();
        let result = selector.select_file_for_cleaning_with_profile(
            &profile,
            60, // min_utilization_pct
            1,  // min_age
            false,
        );

        assert_eq!(result.map(|(f, _)| f), Some(1));
    }

    #[test]
    fn test_select_with_profile_no_qualifying_file() {
        // All files have utilization >= threshold.
        let profile = make_profile(&[
            (1, 1000, 100), // 90% util
            (2, 1000, 200), // 80% util
        ]);

        let mut selector = FileSelector::new();
        // Threshold 50% — no file is below 50%.
        let result = selector.select_file_for_cleaning_with_profile(
            &profile,
            50,
            0,
            false,
        );

        assert_eq!(result, None);
    }

    #[test]
    fn test_select_with_profile_force_cleaning_bypasses_threshold() {
        // All files above threshold — force_cleaning should still select the best.
        let profile = make_profile(&[
            (1, 1000, 100), // 90% util — best (lowest)? No: util is 90%, which is high.
            (2, 1000, 200), // 80% util — better candidate (lower util = more obsolete).
        ]);

        let mut selector = FileSelector::new();
        // With min_utilization_pct=50, no file qualifies normally.
        // With force_cleaning=true, the file with lowest utilization (2, 80%) is chosen.
        // Wait: file 2 has 200 obsolete / 1000 total → active = 800 → util = 80%.
        // file 1 has 100 obsolete / 1000 total → active = 900 → util = 90%.
        // Lower util = file 2 (80%) wins.
        let result = selector.select_file_for_cleaning_with_profile(
            &profile,
            50,
            0,
            true, // force
        );

        assert_eq!(result.map(|(f, _)| f), Some(2));
    }

    #[test]
    fn test_select_with_profile_age_filter_excludes_newest_files() {
        // Five files numbered 1..=5. min_age = 2 → last_file_to_clean = 5 - 2 = 3.
        // Files 4 and 5 are too young. Files 1, 2, 3 are candidates.
        // File 1 has the lowest utilization (most obsolete).
        let profile = make_profile(&[
            (1, 1000, 900), // util 10%
            (2, 1000, 500), // util 50%
            (3, 1000, 200), // util 80%
            (4, 1000, 100), // util 90% — too young
            (5, 1000, 50),  // util 95% — too young (newest)
        ]);

        let mut selector = FileSelector::new();
        let result = selector.select_file_for_cleaning_with_profile(
            &profile,
            60,
            2, // min_age
            false,
        );

        assert_eq!(result.map(|(f, _)| f), Some(1));
    }

    #[test]
    fn test_select_with_profile_skips_in_progress_files() {
        // Files 1 and 2 qualify, but file 1 is already being cleaned.
        // Should choose file 2.
        let profile = make_profile(&[
            (1, 1000, 900), // util 10% — best, but in progress
            (2, 1000, 500), // util 50% — second best
            (3, 1000, 100), // util 90% — newest, skipped by age filter (min_age=1)
        ]);

        let mut selector = FileSelector::new();
        // Mark file 1 as already being cleaned.
        selector.being_cleaned.insert(1);
        selector.file_info.insert(
            1,
            FileInfo {
                status: FileStatus::BeingCleaned,
                required_util: None,
            },
        );

        let result = selector.select_file_for_cleaning_with_profile(
            &profile,
            60,
            1,
            false,
        );

        assert_eq!(result.map(|(f, _)| f), Some(2));
    }

    #[test]
    fn test_select_with_profile_empty_summaries_returns_none() {
        let profile: BTreeMap<u32, FileSummary> = BTreeMap::new();

        let mut selector = FileSelector::new();
        let result = selector.select_file_for_cleaning_with_profile(
            &profile,
            50,
            0,
            false,
        );

        assert_eq!(result, None);
    }

    #[test]
    fn test_select_with_profile_single_file_age_zero() {
        // Single file, min_age=0 → last_file_to_clean = file_num (eligible).
        let profile = make_profile(&[(1, 1000, 800)]); // 20% util

        let mut selector = FileSelector::new();
        let result = selector.select_file_for_cleaning_with_profile(
            &profile,
            50,
            0,
            false,
        );

        assert_eq!(result.map(|(f, _)| f), Some(1));
    }

    #[test]
    fn test_select_with_profile_marks_file_as_being_cleaned() {
        let profile = make_profile(&[(1, 1000, 800), (2, 1000, 100)]);

        let mut selector = FileSelector::new();
        let result = selector.select_file_for_cleaning_with_profile(
            &profile,
            50,
            0,
            false,
        );

        assert!(result.is_some());
        let (file_num, _) = result.unwrap();
        assert!(selector.is_being_cleaned(file_num));
        assert_eq!(
            selector.get_file_status(file_num),
            Some(FileStatus::BeingCleaned)
        );
    }

    #[test]
    fn test_select_with_profile_returns_queued_file_first() {
        // If a file is already in to_be_cleaned (queued by add_file_to_clean),
        // select_file_for_cleaning_with_profile must return it first before
        // scoring a new one.
        let profile = make_profile(&[(1, 1000, 900), (2, 1000, 500)]);

        let mut selector = FileSelector::new();
        // Manually queue file 2.
        selector.add_file_to_clean(2);

        let result = selector.select_file_for_cleaning_with_profile(
            &profile,
            60,
            0,
            false,
        );

        // Should return file 2 (the queued file), not file 1 (best by score).
        assert_eq!(result.map(|(f, _)| f), Some(2));
    }

    #[test]
    fn test_utilization_pct_zero_total() {
        let summary = FileSummary::default();
        assert_eq!(FileSelector::utilization_pct(&summary), 0);
    }

    #[test]
    fn test_utilization_pct_all_obsolete() {
        // File where everything is obsolete → 0% active → util = 0%.
        let summary = FileSummary {
            total_size: 1000,
            total_ln_count: 1,
            total_ln_size: 1000,
            obsolete_ln_count: 1,
            obsolete_ln_size: 1000,
            obsolete_ln_size_counted: 1,
            ..Default::default()
        };
        assert_eq!(FileSelector::utilization_pct(&summary), 0);
    }

    #[test]
    fn test_utilization_pct_all_active() {
        // File with no obsolete bytes → 100% util.
        let summary = FileSummary {
            total_count: 1,
            total_size: 1000,
            total_ln_count: 1,
            total_ln_size: 1000,
            ..Default::default()
        };
        // No obsolete
        assert_eq!(FileSelector::utilization_pct(&summary), 100);
    }
}
