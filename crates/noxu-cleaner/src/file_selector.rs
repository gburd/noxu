//! File selection for cleaning.
//!
//! keeps track of the status of files
//! for which cleaning is in progress.
//!
//! Cost/benefit file scoring algorithm for log cleaning.
//! `UtilizationCalculator.getBestFile()`.  selects files using
//! TTL-adjusted utilization: the file whose adjusted utilization is lowest
//! is the best candidate.  Expired records do not need to be migrated during
//! cleaning — they can be dropped outright — so a file with a high expired
//! fraction is cheaper to clean than its raw utilization suggests.
//!
//! Adjusted utilization formula:
//!
//!   obsolete_bytes  = summary.get_obsolete_size()
//!   expired_bytes   = summary.obsolete_expired_size   (subset of obsolete)
//!   active_bytes    = total - obsolete
//!   adjusted_active = active_bytes - expired_bytes
//!   adjustedUtil    = adjusted_active / total          (0–100 integer %)
//!
//! When `obsolete_expired_size == 0` (no TTL data), adjusted_util == raw_util.
//!
//! The file with the **lowest adjusted utilization** (= highest effective
//! obsolete fraction) is chosen, subject to:
//!   - `file_number <= last_file_to_clean` (age filter, the: `fileNum <= lastFileToClean`)
//!   - file not already in-progress (being cleaned)
//!   - file not in the `to_be_cleaned` queue already
//!
//! When `force_cleaning` is `true`, selection ignores the utilization
//! threshold and always returns the best file.

use crate::file_summary::FileSummary;
use crate::ln_info::LnInfo;
use hashbrown::{HashMap, HashSet};
use noxu_util::Lsn;
use std::collections::{BTreeMap, VecDeque};

/// Database ID type used in the cleaner (mirrors `DbId` as i64).
type DbId = i64;

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
    /// Files that were in CLEANED state at checkpoint start.
    pub cleaned_files: Vec<u32>,
    /// Files that were in FULLY_PROCESSED state at checkpoint start.
    /// JE: `CheckpointStartCleanerState.fullyProcessedFiles`.
    pub fully_processed_files: Vec<u32>,
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
    /// Two-pass cleaning gate (JE CLEANER_TWO_PASS_GAP / TWO_PASS_THRESHOLD):
    /// when the chosen file's max utilization exceeds `two_pass_threshold` AND
    /// its (max - min) utilization uncertainty band is >= `two_pass_gap`, a
    /// dry-run first pass is requested (required_util = two_pass_threshold).
    two_pass_gap: i32,
    two_pass_threshold: i32,
    /// Two-pass cleaning: required utilization threshold for next selection pass.
    ///
    /// When the chosen file's utilization uncertainty band is wide enough
    /// (CFG-TWOPASS-1, JE `getBestFile`), selection raises this threshold to
    /// `twoPassThreshold` so a dry-run pass re-measures the file before it is
    /// committed for cleaning.
    ///
    ///
    required_util: Option<i32>,
    /// Two-pass cleaning: if true, bypass normal utilization threshold and
    /// always select the best candidate file.
    ///
    ///
    force_cleaning: bool,

    // ── Pending sets (CLN-1) ──────────────────────────────────────────────────
    //
    // LNs that could not be migrated because their BIN slot was locked by a
    // concurrent writer.  The file they belong to cannot be deleted until all
    // pending LNs are successfully retried and the pending set is drained.
    // JE: `FileSelector.pendingLNs` (FileSelector.java ~line 133).
    pending_lns: HashMap<Lsn, LnInfo>,

    // Database IDs whose deletion is still in progress.  A file is not safe
    // to delete while any of its databases still has pending deletion work.
    // JE: `FileSelector.pendingDBs` (FileSelector.java ~line 141).
    pending_dbs: HashSet<DbId>,

    /// Whether any pending LN or pending DB was added *during* the current
    /// checkpoint interval.  Set true in `add_pending_ln`/`add_pending_db`;
    /// snapshot at `get_checkpoint_state`; used by `process_checkpoint_end`
    /// to decide whether cleaned files must wait an extra checkpoint before
    /// being reserved for deletion.
    /// JE: `FileSelector.anyPendingDuringCheckpoint` (~line 152).
    any_pending_during_checkpoint: bool,
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
            two_pass_gap: 10, // JE CLEANER_TWO_PASS_GAP default
            two_pass_threshold: 0, // JE default 0 => minUtilization - 5
            force_cleaning: false,
            pending_lns: HashMap::new(),
            pending_dbs: HashSet::new(),
            any_pending_during_checkpoint: false,
        }
    }

    /// Configure the two-pass gate (JE CLEANER_TWO_PASS_GAP / TWO_PASS_THRESHOLD).
    /// A `threshold` of 0 means "use minUtilization - 5" (resolved at gate time).
    pub fn set_two_pass_params(&mut self, gap: i32, threshold: i32) {
        self.two_pass_gap = gap;
        self.two_pass_threshold = threshold;
    }

    /// Returns the current required utilization threshold (`None` if none set).
    ///
    /// CLN-F6: `required_util` is no longer set by any reinvented "shortfall"
    /// heuristic.  The production two-pass path (CLN-5) uses
    /// `two_pass_check` + `remove_file_from_cleaning`; this accessor is
    /// retained for the queue-driven `required_util` carried per file.
    pub fn required_util(&self) -> Option<i32> {
        self.required_util
    }

    /// Returns true if force-cleaning mode is active.
    pub fn is_force_cleaning(&self) -> bool {
        self.force_cleaning
    }

    /// Drains the next explicitly-queued TO_BE_CLEANED file.
    ///
    /// Private helper used by `select_file_for_cleaning`.  Corresponds to the
    /// `if (!toBeCleaned.isEmpty())` branch at the top of JE
    /// `FileSelector.selectFileForCleaning` (FileSelector.java ~line 175).
    ///
    /// Returns `(file_number, required_util)` or `None` if the queue is empty.
    pub fn select_from_queue(&mut self) -> Option<(u32, Option<i32>)> {
        if let Some(file_number) = self.to_be_cleaned.pop_front() {
            self.being_cleaned.insert(file_number);

            if let Some(info) = self.file_info.get_mut(&file_number) {
                info.status = FileStatus::BeingCleaned;
                return Some((file_number, info.required_util));
            }
        }
        None
    }

    /// Selects the best file for cleaning.
    ///
    /// Faithful port of JE `FileSelector.selectFileForCleaning`
    /// (FileSelector.java ~line 170):
    ///
    /// 1. If any files are already queued as TO_BE_CLEANED, dequeue and
    ///    return the first one (FIFO drain).
    /// 2. Otherwise fall through to `select_file_for_cleaning_with_policy`
    ///    (`UtilizationCalculator.getBestFile`) to score all candidate
    ///    files by TTL-adjusted utilization and pick the best one.
    ///
    /// JE: `fileSelector.selectFileForCleaning(calculator, fileSummaryMap,
    ///   forceCleaning)` (FileProcessor.java doClean ~line 393).
    ///
    /// # Arguments
    /// * `file_summary_map` – merged per-file summaries (profile + tracker),
    ///   equivalent to JE `fileSummaryMap` from `getFileSummaryMap(true)`.
    /// * `min_utilization_pct` – minimum utilization threshold (0-100).
    /// * `min_age` – minimum age in files before cleaning.
    /// * `force_cleaning` – bypass utilization threshold.
    /// * `first_active_txn_file` – CLN-4 clamping: exclude files >=
    ///   `firstActiveTxnFile`.
    /// * `min_file_utilization_pct` – JE `minFileUtilization` second-tier
    ///   threshold (CLN-F1).  When the aggregate gate fails, a file whose
    ///   max-gradual utilization is below this is still cleaned.
    pub fn select_file_for_cleaning(
        &mut self,
        file_summary_map: &BTreeMap<u32, FileSummary>,
        min_utilization_pct: u32,
        min_age: u32,
        force_cleaning: bool,
        first_active_txn_file: Option<u32>,
        min_file_utilization_pct: i32,
    ) -> Option<(u32, Option<i32>)> {
        // JE FileSelector.java ~line 175:
        // if (!toBeCleaned.isEmpty()) { return first queued file }
        if let Some(result) = self.select_from_queue() {
            return Some(result);
        }

        // JE FileSelector.java ~line 184:
        // result = calculator.getBestFile(fileSummaryMap, forceCleaning)
        // CLN-F1: wire the AGGREGATE total threshold (= minUtilization) and
        // the minFileUtilization second tier into the faithful getBestFile
        // multi-tier decision.
        self.select_file_for_cleaning_with_policy(
            file_summary_map,
            min_utilization_pct,
            min_age,
            force_cleaning,
            first_active_txn_file,
            Some(min_utilization_pct as i32),
            Some(min_file_utilization_pct),
        )
    }

    /// Removes a file from the cleaning pipeline without putting it back.
    ///
    /// Called after a two-pass (revisalRun) skip: the file's true utilization
    /// was above the threshold so it should not be re-scanned on the next
    /// pass.
    ///
    /// Faithful port of JE `FileSelector.removeFile` (FileSelector.java
    /// ~line 325), which removes the file from `fileInfoMap` entirely so it
    /// is rescored fresh on the next call to `selectFileForCleaning`.
    ///
    /// CLN NEW-3: use this instead of `put_back_file_for_cleaning` after a
    /// two-pass skip so the file is not re-enqueued and rescanned.
    pub fn remove_file_from_cleaning(&mut self, file_number: u32) {
        self.being_cleaned.remove(&file_number);
        self.file_info.remove(&file_number);
    }

    /// Selects the best file for cleaning using cost/benefit scoring.
    ///
    /// Convenience wrapper that calls `select_file_for_cleaning_with_profile_and_txn`
    /// with `first_active_txn_file = None` (no transaction-window clamping).
    ///
    /// NOTE: This is a lower-level helper used by tests. Production code should
    /// call the unified `select_file_for_cleaning` which also drains the
    /// TO_BE_CLEANED queue first (JE faithful structure).
    pub fn select_file_for_cleaning_with_profile(
        &mut self,
        file_summaries: &BTreeMap<u32, FileSummary>,
        min_utilization_pct: u32,
        min_age: u32,
        force_cleaning: bool,
    ) -> Option<(u32, Option<i32>)> {
        self.select_file_for_cleaning_with_profile_and_txn(
            file_summaries,
            min_utilization_pct,
            min_age,
            force_cleaning,
            None,
        )
    }

    /// CLN-6 / CLN-F1: Compute the AGGREGATE predicted minimum utilization
    /// across all candidate files.
    ///
    /// This is JE's `predictedMinUtil` — the utilization computed from the
    /// summed obsolete and total bytes over every file, NOT the per-file
    /// minimum.  If `predictedMinUtil >= totalThreshold`, no file qualifies
    /// and the global gate vetoes selection.
    ///
    /// JE: `UtilizationCalculator.getBestFile` (UtilizationCalculator.java
    /// ~386-389):
    ///   `predictedMinUtil = FileSummary.utilization(
    ///        predictedMaxObsoleteSize, predictedTotalSize)`
    /// where the sums accumulate `maxGradualObsoleteSize` and `totalSize`
    /// per file (~333-336).  In-progress files contribute only
    /// `totalSize - minObsoleteSize` to `predictedTotalSize` and nothing to
    /// the obsolete sum (~328-330), modelling the optimistic assumption that
    /// cleaning will reclaim their obsolete bytes.
    ///
    /// With no TTL/expiration data, `maxGradualObsoleteSize == obsoleteSize`,
    /// so this reduces to `utilization(sum_obsolete, sum_total)`.
    pub fn compute_predicted_min_util(
        file_summaries: &BTreeMap<u32, FileSummary>,
    ) -> i32 {
        Self::compute_predicted_min_util_with_in_progress(
            file_summaries,
            &HashSet::new(),
        )
    }

    /// Aggregate predicted-min-util honouring the in-progress file set.
    ///
    /// JE: `UtilizationCalculator.getBestFile` ~325-336.
    pub fn compute_predicted_min_util_with_in_progress(
        file_summaries: &BTreeMap<u32, FileSummary>,
        in_progress: &HashSet<u32>,
    ) -> i32 {
        let mut predicted_total_size: i64 = 0;
        let mut predicted_max_obsolete_size: i64 = 0;
        for (&file_num, summary) in file_summaries.iter() {
            if summary.is_empty() {
                continue;
            }
            let total = summary.total_size as i64;
            // minObsoleteSize: definite obsolete bytes (lower bound).
            let obsolete = summary.get_obsolete_size() as i64;
            let expired_lower =
                (summary.obsolete_expired_size as i64).min(total);
            let min_obsolete = obsolete.max(expired_lower);
            if in_progress.contains(&file_num) {
                // JE ~328-330: in-progress file is assumed to shrink to its
                // utilized bytes; it adds no obsolete bytes to the aggregate.
                predicted_total_size += total - min_obsolete;
                continue;
            }
            // maxGradualObsoleteSize: obsolete + gradual-expired, capped.
            let expired_gradual =
                (summary.obsolete_expired_gradual_size as i64).min(total);
            let max_gradual_obsolete = (obsolete + expired_gradual).min(total);
            predicted_total_size += total;
            predicted_max_obsolete_size += max_gradual_obsolete;
        }
        Self::utilization_of(predicted_max_obsolete_size, predicted_total_size)
    }

    /// `FileSummary.utilization(obsoleteSize, totalSize)`
    /// (FileSummary.java:292): `round(100 * (total - obsolete) / total)`.
    fn utilization_of(obsolete: i64, total: i64) -> i32 {
        if total <= 0 {
            return 0;
        }
        let active = (total - obsolete).max(0) as f64;
        ((100.0 * active) / total as f64).round() as i32
    }

    /// Selects the best file for cleaning, optionally clamped to a
    /// first-active-transaction file boundary (CLN-4).
    ///
    /// See `select_file_for_cleaning_with_profile` for the full algorithm
    /// description.  This variant adds the `first_active_txn_file` guard:
    /// if `Some(n)`, files with `file_number >= n` are excluded because
    /// they may still be needed by the oldest open transaction.
    ///
    /// NOTE: This is a lower-level helper. Production code should call
    /// the unified `select_file_for_cleaning` which also drains the
    /// TO_BE_CLEANED queue first (JE faithful structure).
    ///
    /// JE: `UtilizationCalculator.getBestFile` clamps
    /// `firstActiveFile = min(fileSummaryMap.lastKey(), firstActiveTxnFile)`
    /// before computing `lastFileToClean`.
    pub fn select_file_for_cleaning_with_profile_and_txn(
        &mut self,
        file_summaries: &BTreeMap<u32, FileSummary>,
        min_utilization_pct: u32,
        min_age: u32,
        force_cleaning: bool,
        first_active_txn_file: Option<u32>,
    ) -> Option<(u32, Option<i32>)> {
        self.select_file_for_cleaning_with_policy(
            file_summaries,
            min_utilization_pct,
            min_age,
            force_cleaning,
            first_active_txn_file,
            None, // predicted_total_threshold: None = no global gate
            None, // min_file_utilization_pct: None = no second tier
        )
    }

    /// Full-policy file selection with CLN-4/CLN-6 support.
    ///
    /// # CLN-6 tiers (JE `UtilizationCalculator.getBestFile`)
    /// 1. **Global gate**: if `predicted_total_threshold` is `Some(t)` and
    ///    `predictedMinUtil >= t`, return `None` (no file qualifies globally).
    /// 2. **Per-file threshold**: files with `adj_util >= min_utilization_pct`
    ///    are excluded unless `force_cleaning` is `true`.
    /// 3. **Second tier**: `min_file_utilization_pct` (JE `minFileUtilization`)
    ///    is a second per-file threshold applied in addition to
    ///    `min_utilization_pct`.  When set, the file must be below *both*
    ///    thresholds to qualify in the normal pass (i.e. the effective
    ///    threshold is `min(min_utilization_pct, min_file_utilization_pct)`).
    ///    When `force_cleaning` is `true`, the second tier is bypassed.
    ///
    /// JE: `UtilizationCalculator.getBestFile` ~lines 174-425.
    pub fn select_file_for_cleaning_with_policy(
        &mut self,
        file_summaries: &BTreeMap<u32, FileSummary>,
        min_utilization_pct: u32,
        min_age: u32,
        force_cleaning: bool,
        first_active_txn_file: Option<u32>,
        predicted_total_threshold: Option<i32>,
        min_file_utilization_pct: Option<i32>,
    ) -> Option<(u32, Option<i32>)> {
        // Step 1 -- if a file is already queued (from a previous scoring pass
        // that enqueued it but didn't immediately return), dequeue it now.
        if !self.to_be_cleaned.is_empty() {
            return self.select_from_queue();
        }

        if file_summaries.is_empty() {
            return None;
        }

        // The newest (highest-numbered) file is the "first active" file.
        // FirstActiveFile = fileSummaryMap.lastKey()
        let newest_file = *file_summaries.keys().next_back()?;

        // CLN-4: clamp by first_active_txn_file so we don't select a file
        // that is still inside an open transaction's log window.
        // JE: firstActiveFile = Math.min(fileSummaryMap.lastKey(), firstActiveTxnFile)
        let effective_newest = match first_active_txn_file {
            Some(txn_file) if txn_file < newest_file => txn_file,
            _ => newest_file,
        };

        // lastFileToClean = firstActiveFile - minAge
        // Any file with file_number > last_file_to_clean is too young to clean.
        // Use saturating_sub so that if min_age > newest_file we get 0.
        let last_file_to_clean = effective_newest.saturating_sub(min_age);

        // Collect all in-progress file numbers (not eligible for re-selection).
        let in_progress: HashSet<u32> =
            self.file_info.keys().copied().collect();

        // CLN-F1: faithful `UtilizationCalculator.getBestFile` candidate loop
        // (UtilizationCalculator.java ~344-378).  Track:
        //   * bestFile           = lowest avg utilization, and
        //   * bestGradualFile    = lowest max-gradual utilization,
        // over EVERY age-eligible, non-in-progress file.  NO file is excluded
        // by its OWN utilization in this loop — the threshold is applied below
        // as an AGGREGATE decision, never per file.
        let mut best_file: Option<u32> = None;
        let mut best_avg_util: i32 = 101; // higher than any valid utilization
        let mut best_gradual_file: Option<u32> = None;
        let mut best_gradual_max_util: i32 = 101;

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

            // JE ~348-359: thisAvgUtil = (thisMinUtil + thisMaxUtil) / 2.
            // thisMinUtil uses the gradual (upper) expired bound (most
            // optimistic / lowest util); thisMaxUtil uses the lower bound.
            let this_min = Self::min_utilization_pct(summary);
            let this_max = Self::max_utilization_pct(summary);
            let this_avg = (this_min + this_max) / 2;
            if best_file.is_none() || this_avg < best_avg_util {
                best_file = Some(file_num);
                best_avg_util = this_avg;
            }

            // JE ~364-372: bestGradualFile = lowest max-gradual utilization
            // (= thisMinUtil here, which uses the gradual expired bound).
            let this_gradual_max = this_min;
            if best_gradual_file.is_none()
                || this_gradual_max < best_gradual_max_util
            {
                best_gradual_file = Some(file_num);
                best_gradual_max_util = this_gradual_max;
            }
        }

        // CLN-F1: multi-tier decision (UtilizationCalculator.java ~404-425).
        // totalThreshold defaults to min_utilization_pct when the caller did
        // not override it (JE always has a threshold); fileThreshold defaults
        // to 0, which disables the second tier.
        let total_threshold =
            predicted_total_threshold.unwrap_or(min_utilization_pct as i32);
        let file_threshold = min_file_utilization_pct.unwrap_or(0);
        let forced = force_cleaning || self.force_cleaning;

        let predicted_min_util =
            Self::compute_predicted_min_util_with_in_progress(
                file_summaries,
                &in_progress,
            );

        // Tier 1: predictedMinUtil < totalThreshold -> clean bestFile.
        // Tier 2: bestGradualFileMaxUtil < fileThreshold -> clean bestGradual.
        // Tier 4: forceCleaning -> clean bestFile (FilesToMigrate / tier 3 is
        //         handled separately by the TO_BE_CLEANED queue drain above).
        let file_num = if !forced && predicted_min_util < total_threshold {
            best_file?
        } else if !forced && best_gradual_max_util < file_threshold {
            best_gradual_file?
        } else if forced {
            best_file?
        } else {
            return None;
        };

        // Two-pass gate (JE UtilizationCalculator.getBestFile, ~line 447-457):
        // if the chosen file's MAX utilization exceeds twoPassThreshold AND its
        // (max - min) utilization uncertainty band is >= twoPassGap, request a
        // first (dry-run) pass that recomputes true utilization, with
        // pass1RequiredUtil = twoPassThreshold. The band comes from the
        // file's lower (definite) vs gradual (upper) expired-bytes bounds.
        let chosen_required_util = if !self.force_cleaning {
            if let Some(summary) = file_summaries.get(&file_num) {
                let this_min = Self::min_utilization_pct(summary);
                let this_max = Self::max_utilization_pct(summary);
                // threshold 0 => minUtilization - 5 (JE Cleaner.java:421-422).
                let threshold = if self.two_pass_threshold == 0 {
                    (min_utilization_pct as i32 - 5).max(0)
                } else {
                    self.two_pass_threshold
                };
                if this_max > threshold
                    && (this_max - this_min) >= self.two_pass_gap
                {
                    Some(threshold)
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };

        // Step 3 -- mark the chosen file as being cleaned.
        self.being_cleaned.insert(file_num);
        self.file_info.insert(
            file_num,
            FileInfo {
                status: FileStatus::BeingCleaned,
                required_util: chosen_required_util,
            },
        );

        Some((file_num, chosen_required_util))
    }

    ///
    ///
    /// A file at 100% utilization has no obsolete bytes; 0% means all bytes
    /// are obsolete.
    pub fn utilization_pct(summary: &FileSummary) -> i32 {
        if summary.total_size <= 0 {
            return 0;
        }
        let active = summary.get_active_size();
        // Clamp to [0, 100].
        ((active as i64 * 100) / summary.total_size as i64).clamp(0, 100) as i32
    }

    /// Returns the TTL-adjusted utilization as an integer percentage 0–100.
    ///
    /// Expired LNs tracked in `FileSummary::obsolete_expired_size` do not
    /// need to be migrated during cleaning.  This method subtracts their
    /// byte size from the "active bytes" numerator so files with many expired
    /// records are scored as cheaper to clean.
    ///
    /// `UtilizationCalculator.getBestFile()` TTL-adjustment:
    ///   adjusted_active = active_bytes - expired_bytes
    ///   adjusted_util   = adjusted_active / total_bytes  (clamped 0–100)
    ///
    /// When `obsolete_expired_size == 0` this is identical to `utilization_pct`.
    pub fn adjusted_utilization_pct(summary: &FileSummary) -> i32 {
        if summary.total_size <= 0 {
            return 0;
        }
        let adjusted = summary.get_adjusted_active_size();
        ((adjusted as i64 * 100) / summary.total_size as i64).clamp(0, 100)
            as i32
    }

    /// MIN utilization for the two-pass gate (JE `thisMinUtil`): utilization
    /// after subtracting the UPPER (gradual) expired-bytes bound — the most
    /// optimistic (lowest) utilization, since the gradual bound counts the most
    /// bytes as expired. `utilization(obsolete + expiredGradual, total)`.
    fn min_utilization_pct(summary: &FileSummary) -> i32 {
        if summary.total_size <= 0 {
            return 0;
        }
        let obsolete = summary.get_obsolete_size() as i64;
        let expired_gradual = (summary.obsolete_expired_gradual_size as i64)
            .min(summary.total_size as i64);
        let active =
            (summary.total_size as i64 - obsolete - expired_gradual).max(0);
        ((active * 100) / summary.total_size as i64).clamp(0, 100) as i32
    }

    /// MAX utilization for the two-pass gate (JE `thisMaxUtil`): utilization
    /// after subtracting only the LOWER (definite) expired-bytes bound — the
    /// most pessimistic (highest) utilization.
    /// `utilization(obsolete + expiredLower, total)`.
    fn max_utilization_pct(summary: &FileSummary) -> i32 {
        if summary.total_size <= 0 {
            return 0;
        }
        let obsolete = summary.get_obsolete_size() as i64;
        let expired_lower = (summary.obsolete_expired_size as i64)
            .min(summary.total_size as i64);
        let active =
            (summary.total_size as i64 - obsolete - expired_lower).max(0);
        ((active * 100) / summary.total_size as i64).clamp(0, 100) as i32
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
    /// Returns `true` if there are files queued for cleaning.
    ///
    /// Used by the adaptive throttle to determine whether to shorten the
    /// cleaner daemon's sleep interval.
    pub fn has_files_to_clean(&self) -> bool {
        !self.to_be_cleaned.is_empty() || self.is_force_cleaning()
    }

    /// REC-F: whether any cleaned-but-not-yet-reclaimed files exist, meaning a
    /// checkpoint is needed before they can be deleted.
    ///
    /// JE `FileSelector.isCheckpointNeeded`:
    /// `getNumberOfFiles(CLEANED) > 0 || getNumberOfFiles(FULLY_PROCESSED) > 0`.
    /// Noxu's three-state barrier splits JE's FULLY_PROCESSED across the
    /// `cleaned`, `checkpointed`, and `safe_to_delete` sets; a checkpoint is
    /// needed whenever a file is still mid-barrier (`cleaned` or
    /// `checkpointed`) — once it reaches `safe_to_delete` no further
    /// checkpoint is required, it just awaits deletion.
    pub fn is_checkpoint_needed(&self) -> bool {
        !self.cleaned.is_empty() || !self.checkpointed.is_empty()
    }

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

    /// Re-inserts a file into the `safe_to_delete` set after it was removed
    /// but could not be deleted yet because it was protected.
    ///
    /// Used by `Cleaner::delete_safe_files` to restore the deletion-pending
    /// state for a file that was still protected at delete time.
    pub fn add_safe_to_delete_back(&mut self, file_number: u32) {
        self.safe_to_delete.insert(file_number);
    }

    /// Returns a checkpoint state snapshot.
    ///
    /// Also snapshots `any_pending_during_checkpoint` so that
    /// `process_checkpoint_end` can decide whether CLEANED files may be
    /// immediately reserved or must wait another checkpoint.
    ///
    /// JE: `FileSelector.getFilesAtCheckpointStart` (FileSelector.java ~line 369).
    pub fn get_checkpoint_state(&mut self) -> CheckpointStartCleanerState {
        // Snapshot the pending flag.  If either set is non-empty right now,
        // the current checkpoint interval has pending items.
        // JE lines 371-373: anyPendingDuringCheckpoint = !pendingLNs.isEmpty() || !pendingDBs.isEmpty()
        self.any_pending_during_checkpoint =
            !self.pending_lns.is_empty() || !self.pending_dbs.is_empty();

        let mut cleaned_files: Vec<u32> =
            self.cleaned.iter().copied().collect();
        cleaned_files.sort_unstable();

        let mut fully_processed_files: Vec<u32> =
            self.safe_to_delete.iter().copied().collect();
        fully_processed_files.sort_unstable();

        CheckpointStartCleanerState { cleaned_files, fully_processed_files }
    }

    /// Processes files at checkpoint end.
    ///
    /// Implements the two-checkpoint deletion barrier (JE
    /// `FileSelector.updateFilesAtCheckpointEnd`):
    ///
    /// 1. FULLY_PROCESSED files (those captured in `state.fully_processed_files`
    ///    at checkpoint start) are already safe — keep them; they will be
    ///    returned by `get_safe_to_delete()` as before.
    ///
    /// 2. Files that were in the `checkpointed` state *before* the current
    ///    checkpoint started (i.e. NOT in `state.cleaned_files`) are advanced
    ///    to `safe_to_delete` **only when** no pending items blocked the
    ///    checkpoint (`!any_pending_during_checkpoint`).
    ///    If pending items existed, they become FULLY_PROCESSED instead, which
    ///    requires one more checkpoint via `update_processed_files`.
    ///    JE `updateFilesAtCheckpointEnd` line 415: `if (anyPendingDuringCheckpoint)`.
    ///
    /// 3. Files that were in the `cleaned` state when the *current* checkpoint
    ///    started (`state.cleaned_files`) are advanced to `checkpointed`.
    ///
    /// JE: `FileSelector.updateFilesAtCheckpointEnd` (FileSelector.java ~line 398).
    pub fn process_checkpoint_end(
        &mut self,
        state: &CheckpointStartCleanerState,
    ) {
        // Step 1: advance already-checkpointed files to safe_to_delete,
        // but only if no pending items were present during this checkpoint
        // interval (JE line 415: if (anyPendingDuringCheckpoint) { CHECKPOINTED } else { reserved })
        let already_checkpointed: Vec<u32> =
            self.checkpointed.iter().copied().collect();
        if self.any_pending_during_checkpoint {
            // Pending items existed — cleaned files must wait another checkpoint.
            // Do NOT advance checkpointed → safe_to_delete yet; they will be
            // promoted by `update_processed_files` once the pending sets drain.
            // (They remain in CHECKPOINTED state.)
        } else {
            // No pending items during this checkpoint — safe to reserve.
            for file_number in already_checkpointed {
                self.mark_file_fully_processed(file_number);
            }
        }

        // Step 2: advance cleaned files (from checkpoint-start snapshot)
        // to checkpointed.
        for &file_number in &state.cleaned_files {
            if self.cleaned.contains(&file_number) {
                self.mark_file_checkpointed(file_number);
            }
        }

        // Step 3: attempt to drain pending → advance CHECKPOINTED → FULLY_PROCESSED.
        self.update_processed_files();
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
        self.pending_lns.clear();
        self.pending_dbs.clear();
        self.any_pending_during_checkpoint = false;
    }

    // ── Pending LN / DB methods (CLN-1) ───────────────────────────────────────

    /// Adds an LN that could not be migrated (lock denied) to the pending set.
    ///
    /// Returns `true` if the LSN was already in the set (duplicate), which
    /// normally doesn’t happen but is harmless.
    ///
    /// Also sets `any_pending_during_checkpoint = true` so the next
    /// `process_checkpoint_end` knows to gate the deletion barrier.
    ///
    /// JE: `FileSelector.addPendingLN` (FileSelector.java ~line 455).
    pub fn add_pending_ln(&mut self, log_lsn: Lsn, info: LnInfo) -> bool {
        self.any_pending_during_checkpoint = true;
        self.pending_lns.insert(log_lsn, info).is_some()
    }

    /// Returns a snapshot of all pending LNs, or `None` if the set is empty.
    ///
    /// JE: `FileSelector.getPendingLNs` (FileSelector.java ~line 467).
    pub fn get_pending_lns(&self) -> Option<Vec<(Lsn, LnInfo)>> {
        if self.pending_lns.is_empty() {
            None
        } else {
            Some(
                self.pending_lns.iter().map(|(&k, v)| (k, v.clone())).collect(),
            )
        }
    }

    /// Removes a successfully-retried LN from the pending set.
    ///
    /// Calls `update_processed_files` afterwards so that if both pending sets
    /// are now empty, CHECKPOINTED files are immediately promoted.
    ///
    /// JE: `FileSelector.removePendingLN` (FileSelector.java ~line 477).
    pub fn remove_pending_ln(&mut self, log_lsn: Lsn) {
        self.pending_lns.remove(&log_lsn);
        self.update_processed_files();
    }

    /// Returns the number of pending LNs.
    pub fn get_pending_ln_count(&self) -> usize {
        self.pending_lns.len()
    }

    /// Adds a database whose deletion is still in progress.
    ///
    /// JE: `FileSelector.addPendingDB` (FileSelector.java ~line 493).
    pub fn add_pending_db(&mut self, db_id: DbId) -> bool {
        self.any_pending_during_checkpoint = true;
        self.pending_dbs.insert(db_id)
    }

    /// Returns a snapshot of pending database IDs, or `None` if empty.
    ///
    /// JE: `FileSelector.getPendingDBs` (FileSelector.java ~line 507).
    pub fn get_pending_dbs(&self) -> Option<Vec<DbId>> {
        if self.pending_dbs.is_empty() {
            None
        } else {
            Some(self.pending_dbs.iter().copied().collect())
        }
    }

    /// Removes a database from the pending set.
    ///
    /// JE: `FileSelector.removePendingDB` (FileSelector.java ~line 521).
    pub fn remove_pending_db(&mut self, db_id: DbId) {
        self.pending_dbs.remove(&db_id);
        self.update_processed_files();
    }

    /// Returns `true` if the pending-during-checkpoint flag is set.
    pub fn any_pending_during_checkpoint(&self) -> bool {
        self.any_pending_during_checkpoint
    }

    /// Returns whether both pending sets are empty.
    pub fn all_pending_drained(&self) -> bool {
        self.pending_lns.is_empty() && self.pending_dbs.is_empty()
    }

    /// Moves a file from BEING_CLEANED back to TO_BE_CLEANED.
    ///
    /// Called when `process_single_file` fails or is interrupted, so the file
    /// is retried on the next cleaning pass rather than stuck forever in
    /// `BEING_CLEANED`.
    ///
    /// JE: `FileSelector.putBackFileForCleaning` (FileSelector.java ~line 325).
    pub fn put_back_file_for_cleaning(&mut self, file_number: u32) {
        if !self.being_cleaned.contains(&file_number) {
            // Already removed (e.g. by shutdown) — ignore.
            return;
        }
        self.being_cleaned.remove(&file_number);
        self.to_be_cleaned.push_back(file_number);
        if let Some(info) = self.file_info.get_mut(&file_number) {
            info.status = FileStatus::ToBeCleaned;
        }
    }

    // ── Private helpers ──────────────────────────────────────────────────

    /// When both pending sets are empty, advance CHECKPOINTED → FULLY_PROCESSED.
    ///
    /// This is called after every `remove_pending_ln` and `remove_pending_db`
    /// so that files are promoted as soon as the last blocker clears.
    ///
    /// JE: `FileSelector.updateProcessedFiles` (FileSelector.java ~line 549).
    fn update_processed_files(&mut self) {
        if self.pending_lns.is_empty() && self.pending_dbs.is_empty() {
            let checkpointed: Vec<u32> =
                self.checkpointed.iter().copied().collect();
            for file_number in checkpointed {
                self.mark_file_fully_processed(file_number);
            }
        }
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

        let result = selector.select_from_queue();
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
        let result = selector.select_from_queue();
        assert_eq!(result, None);
    }

    #[test]
    fn test_mark_file_cleaned() {
        let mut selector = FileSelector::new();
        selector.add_file_to_clean(1);
        selector.select_from_queue();

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
        selector.select_from_queue();
        selector.mark_file_cleaned(1);

        selector.mark_file_checkpointed(1);

        assert_eq!(selector.get_file_status(1), Some(FileStatus::Checkpointed));

        let stats = selector.get_stats();
        assert_eq!(stats.cleaned, 0);
        assert_eq!(stats.checkpointed, 1);
    }

    /// REC-F: `is_checkpoint_needed` mirrors JE `isCheckpointNeeded` — true
    /// while a file is mid-barrier (CLEANED or CHECKPOINTED), false once it is
    /// safe-to-delete or untracked.
    #[test]
    fn test_rec_f_is_checkpoint_needed() {
        let mut selector = FileSelector::new();
        assert!(
            !selector.is_checkpoint_needed(),
            "empty selector: no checkpoint needed"
        );

        // CLEANED: a checkpoint is needed to advance the barrier.
        selector.add_file_to_clean(1);
        selector.select_from_queue();
        selector.mark_file_cleaned(1);
        assert!(
            selector.is_checkpoint_needed(),
            "REC-F: CLEANED file needs a checkpoint"
        );

        // CHECKPOINTED: still needs one more checkpoint to reach safe_to_delete.
        selector.mark_file_checkpointed(1);
        assert!(
            selector.is_checkpoint_needed(),
            "REC-F: CHECKPOINTED file still needs a checkpoint"
        );

        // FULLY_PROCESSED (safe_to_delete): no further checkpoint needed.
        selector.mark_file_fully_processed(1);
        assert!(
            !selector.is_checkpoint_needed(),
            "REC-F: safe-to-delete file needs no further checkpoint"
        );
    }

    #[test]
    fn test_mark_file_fully_processed() {
        let mut selector = FileSelector::new();
        selector.add_file_to_clean(1);
        selector.select_from_queue();
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
            selector.select_from_queue();
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
        selector.select_from_queue();
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
        selector.select_from_queue();
        selector.mark_file_cleaned(1);

        selector.add_file_to_clean(2);
        selector.select_from_queue();
        selector.mark_file_cleaned(2);

        let state = selector.get_checkpoint_state();
        assert_eq!(state.cleaned_files, vec![1, 2]);
    }

    #[test]
    fn test_process_checkpoint_end() {
        let mut selector = FileSelector::new();

        selector.add_file_to_clean(1);
        selector.select_from_queue();
        selector.mark_file_cleaned(1);

        let state = selector.get_checkpoint_state();
        selector.process_checkpoint_end(&state);

        // With no pending LNs/DBs (anyPendingDuringCheckpoint = false),
        // JE promotes CLEANED directly to reserved (FullyProcessed) in one
        // checkpoint.  (JE updateFilesAtCheckpointEnd: else { makeReservedFiles }).
        assert_eq!(
            selector.get_file_status(1),
            Some(FileStatus::FullyProcessed)
        );

        let stats = selector.get_stats();
        assert_eq!(stats.cleaned, 0);
        assert_eq!(stats.checkpointed, 0);
        assert_eq!(stats.safe_to_delete, 1);
    }

    #[test]
    fn test_process_checkpoint_end_with_pending_needs_two_checkpoints() {
        // When pending LNs exist (anyPendingDuringCheckpoint = true),
        // CLEANED files must pass through CHECKPOINTED and require a second
        // checkpoint before becoming FullyProcessed.
        let mut selector = FileSelector::new();

        selector.add_file_to_clean(1);
        selector.select_from_queue();
        selector.mark_file_cleaned(1);

        // Simulate a pending LN — this sets any_pending_during_checkpoint.
        let lsn = noxu_util::Lsn::new(1, 100);
        selector.add_pending_ln(
            lsn,
            crate::LnInfo::new(lsn, 1, vec![1u8], 64, false, 0),
        );

        // Checkpoint 1: file should only advance to CHECKPOINTED.
        let state = selector.get_checkpoint_state();
        selector.process_checkpoint_end(&state);

        assert_eq!(selector.get_file_status(1), Some(FileStatus::Checkpointed));

        // Drain the pending LN — this calls update_processed_files which promotes
        // CHECKPOINTED → FullyProcessed immediately.
        selector.remove_pending_ln(lsn);
        assert_eq!(
            selector.get_file_status(1),
            Some(FileStatus::FullyProcessed)
        );
    }

    #[test]
    fn test_add_file_with_util() {
        let mut selector = FileSelector::new();
        selector.add_file_to_clean_with_util(1, Some(50));

        let result = selector.select_from_queue();
        assert_eq!(result, Some((1, Some(50))));
    }

    #[test]
    fn test_fifo_order() {
        let mut selector = FileSelector::new();
        selector.add_file_to_clean(1);
        selector.add_file_to_clean(2);
        selector.add_file_to_clean(3);

        assert_eq!(selector.select_from_queue(), Some((1, None)));
        assert_eq!(selector.select_from_queue(), Some((2, None)));
        assert_eq!(selector.select_from_queue(), Some((3, None)));
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
        selector.select_from_queue();

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
        let result = selector.select_from_queue();
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
            &profile, 60, // min_utilization_pct
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
        let result = selector
            .select_file_for_cleaning_with_profile(&profile, 50, 0, false);

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
            &profile, 50, 0, true, // force
        );

        assert_eq!(result.map(|(f, _)| f), Some(2));
    }

    #[test]
    fn test_select_with_profile_age_filter_excludes_newest_files() {
        // Five files numbered 1..=5. min_age = 2 → last_file_to_clean = 5 - 2 = 3.
        // Files 4 and 5 are too young. Files 1, 2, 3 are candidates.
        // File 1 has the lowest utilization (most obsolete).  CLN-F1: the
        // aggregate predictedMinUtil (57%) is below the 60% threshold so
        // cleaning proceeds and bestFile = file 1.
        let profile = make_profile(&[
            (1, 1000, 900), // util 10%
            (2, 1000, 600), // util 40%
            (3, 1000, 500), // util 50%
            (4, 1000, 100), // util 90% — too young
            (5, 1000, 50),  // util 95% — too young (newest)
        ]);

        let mut selector = FileSelector::new();
        let result = selector.select_file_for_cleaning_with_profile(
            &profile, 60, 2, // min_age
            false,
        );

        assert_eq!(result.map(|(f, _)| f), Some(1));
    }

    #[test]
    fn test_select_with_profile_skips_in_progress_files() {
        // Files 1 and 2 qualify, but file 1 is already being cleaned.
        // Should choose file 2.  CLN-F1: the aggregate predictedMinUtil must
        // be below threshold for cleaning to proceed; file 2 is heavily
        // obsolete so the aggregate stays under 60%.
        let profile = make_profile(&[
            (1, 1000, 900), // util 10% — best, but in progress
            (2, 1000, 900), // util 10% — chosen (file 1 in progress)
            (3, 1000, 100), // util 90% — newest, skipped by age filter (min_age=1)
        ]);

        let mut selector = FileSelector::new();
        // Mark file 1 as already being cleaned.
        selector.being_cleaned.insert(1);
        selector.file_info.insert(
            1,
            FileInfo { status: FileStatus::BeingCleaned, required_util: None },
        );

        let result = selector
            .select_file_for_cleaning_with_profile(&profile, 60, 1, false);

        assert_eq!(result.map(|(f, _)| f), Some(2));
    }

    #[test]
    fn test_select_with_profile_empty_summaries_returns_none() {
        let profile: BTreeMap<u32, FileSummary> = BTreeMap::new();

        let mut selector = FileSelector::new();
        let result = selector
            .select_file_for_cleaning_with_profile(&profile, 50, 0, false);

        assert_eq!(result, None);
    }

    #[test]
    fn test_select_with_profile_single_file_age_zero() {
        // Single file, min_age=0 → last_file_to_clean = file_num (eligible).
        let profile = make_profile(&[(1, 1000, 800)]); // 20% util

        let mut selector = FileSelector::new();
        let result = selector
            .select_file_for_cleaning_with_profile(&profile, 50, 0, false);

        assert_eq!(result.map(|(f, _)| f), Some(1));
    }

    #[test]
    fn test_select_with_profile_marks_file_as_being_cleaned() {
        let profile = make_profile(&[(1, 1000, 900), (2, 1000, 800)]);

        let mut selector = FileSelector::new();
        let result = selector
            .select_file_for_cleaning_with_profile(&profile, 50, 0, false);

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

        let result = selector
            .select_file_for_cleaning_with_profile(&profile, 60, 0, false);

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

    // ── TTL-adjusted utilization tests ───────────────────────────────────────

    /// A file with 30% live data but 200 bytes of expired records has an
    /// adjusted_util lower than its raw_util — it is cheaper to clean.
    #[test]
    fn test_adjusted_utilization_lower_than_raw_when_expired() {
        // total=1000, obsolete_ln=700 (raw active=300), expired subset=200
        // raw_util      = 300/1000 = 30%
        // adjusted_util = (300-200)/1000 = 10%
        let summary = FileSummary {
            total_count: 10,
            total_size: 1000,
            total_ln_count: 10,
            total_ln_size: 1000,
            obsolete_ln_count: 7,
            obsolete_ln_size: 700,
            obsolete_ln_size_counted: 7,
            obsolete_expired_lns: 2,
            obsolete_expired_size: 200,
            ..Default::default()
        };
        let raw = FileSelector::utilization_pct(&summary);
        let adj = FileSelector::adjusted_utilization_pct(&summary);
        assert_eq!(raw, 30, "raw utilization should be 30%");
        assert_eq!(adj, 10, "adjusted utilization should be 10%");
        assert!(adj < raw, "adjusted must be lower than raw when expired > 0");
    }

    /// When no expired records exist, adjusted_util equals raw_util.
    #[test]
    fn test_adjusted_utilization_equals_raw_when_no_expired() {
        let summary = FileSummary {
            total_count: 10,
            total_size: 1000,
            total_ln_count: 10,
            total_ln_size: 1000,
            obsolete_ln_count: 5,
            obsolete_ln_size: 500,
            obsolete_ln_size_counted: 5,
            obsolete_expired_lns: 0,
            obsolete_expired_size: 0,
            ..Default::default()
        };
        assert_eq!(
            FileSelector::utilization_pct(&summary),
            FileSelector::adjusted_utilization_pct(&summary),
            "no expired records: adjusted == raw"
        );
    }

    /// FileSelector prefers the file with expired records over one with equal
    /// raw utilization but no expired records, because the expired file is
    /// cheaper to clean.
    #[test]
    fn test_select_prefers_file_with_expired_records() {
        // File 1: 30% raw util, 200/300 active bytes are expired → adj = 10%
        // File 2: 30% raw util, no expired records → adj = 30%
        // File 3: newest — skipped by age filter (min_age=1)
        let mut map = BTreeMap::new();
        map.insert(
            1u32,
            FileSummary {
                total_count: 10,
                total_size: 1000,
                total_ln_count: 10,
                total_ln_size: 1000,
                obsolete_ln_count: 7,
                obsolete_ln_size: 700,
                obsolete_ln_size_counted: 7,
                obsolete_expired_lns: 2,
                obsolete_expired_size: 200,
                ..Default::default()
            },
        );
        map.insert(
            2u32,
            FileSummary {
                total_count: 10,
                total_size: 1000,
                total_ln_count: 10,
                total_ln_size: 1000,
                obsolete_ln_count: 7,
                obsolete_ln_size: 700,
                obsolete_ln_size_counted: 7,
                obsolete_expired_lns: 0,
                obsolete_expired_size: 0,
                ..Default::default()
            },
        );
        map.insert(
            3u32,
            FileSummary {
                total_count: 1,
                total_size: 100,
                total_ln_count: 1,
                total_ln_size: 100,
                ..Default::default()
            },
        );

        let mut selector = FileSelector::new();
        // threshold 50% → both files qualify (both adj < 50%); file 1 wins (10% < 30%)
        let result =
            selector.select_file_for_cleaning_with_profile(&map, 50, 1, false);
        assert_eq!(
            result.map(|(f, _)| f),
            Some(1),
            "file with expired records (adj=10%) should be preferred over adj=30%"
        );
    }

    // ── Two-pass cleaning tests ───────────────────────────────────────────────

    /// CFG-TWOPASS-1: when the chosen file's (max-min) utilization band is at
    /// least twoPassGap and its max-util exceeds twoPassThreshold, selection
    /// requests a dry-run pass (required_util = threshold), matching JE `getBestFile`.
    #[test]
    fn test_two_pass_gate_fires_on_uncertainty_band() {
        // CFG-TWOPASS-1: when the chosen file's (max-min) utilization band
        // (driven by the lower vs gradual expired bounds) is >= twoPassGap and
        // its max-util > twoPassThreshold, selection requests a dry-run pass
        // (required_util = threshold). JE getBestFile two-pass gate.
        let mut sel = FileSelector::new();
        // gap=10, threshold=20 (explicit).
        sel.set_two_pass_params(10, 20);

        // File 0: total 1000, no obsolete, but a WIDE expiration band:
        //   lower (definite) expired = 100  -> max_util = (1000-0-100)/1000 = 90
        //   gradual (upper) expired  = 400  -> min_util = (1000-0-400)/1000 = 60
        //   band = 90 - 60 = 30 >= gap(10); max_util 90 > threshold(20) -> fire.
        let mut summaries = std::collections::BTreeMap::new();
        let mut fs = FileSummary::new();
        fs.total_size = 1000;
        fs.total_count = 10;
        fs.total_ln_count = 10;
        fs.total_ln_size = 1000;
        fs.obsolete_expired_size = 100; // lower bound
        fs.obsolete_expired_gradual_size = 400; // upper (gradual) bound
        summaries.insert(0u32, fs);

        // min_utilization 95 so the file (max_util 90 < 95) qualifies for cleaning.
        let result = sel.select_file_for_cleaning_with_policy(
            &summaries,
            95,
            0,
            false,
            Some(1_000_000),
            None,
            None,
        );
        let (file, required_util) = result.expect("a file must be selected");
        assert_eq!(file, 0);
        assert_eq!(
            required_util,
            Some(20),
            "two-pass gate must request a dry-run pass (required_util=threshold)              when the uncertainty band >= gap and max_util > threshold"
        );
    }

    // ── CLN-5 acceptance tests ────────────────────────────────────────────────────

    /// When `required_util` is set to a value >= 0, a file whose recalculated
    /// utilization exceeds that threshold must NOT be cleaned (JE two-pass).
    ///
    /// This test validates `select_file_for_cleaning` returns the `required_util`
    /// value so that `Cleaner::two_pass_check` can act on it.
    #[test]
    fn test_cln5_required_util_is_returned() {
        let mut selector = FileSelector::new();
        selector.add_file_to_clean_with_util(42, Some(60));
        let result = selector.select_from_queue();
        assert_eq!(
            result,
            Some((42, Some(60))),
            "CLN-5: required_util must be returned from select_file_for_cleaning"
        );
    }

    // ── CLN-6 acceptance tests ────────────────────────────────────────────────────

    fn make_summary_sized(total: i32, obsolete: i32) -> FileSummary {
        FileSummary {
            total_count: 10,
            total_size: total,
            total_ln_count: 10,
            total_ln_size: total,
            obsolete_ln_count: 1,
            obsolete_ln_size: obsolete,
            obsolete_ln_size_counted: 1,
            ..Default::default()
        }
    }

    /// CLN-F1: aggregate gate, NOT per-file exclusion.
    ///
    /// The aggregate predictedMinUtil is BELOW the threshold (cleaning is
    /// warranted overall) but the best candidate file's OWN avg utilization is
    /// at or above min_utilization.  The faithful getBestFile must STILL select
    /// the best file because the threshold is applied to the aggregate, not per
    /// file.
    ///
    /// Pre-fix (per-file exclusion `avg_util >= min_utilization` skips):
    /// the only candidate is skipped and the log grows (under-clean).
    ///
    /// JE: `UtilizationCalculator.getBestFile` ~344-378 (no per-file exclusion)
    /// and ~409 (`if (predictedMinUtil < totalThreshold) fileChosen = bestFile`).
    #[test]
    fn test_clnf1_aggregate_below_threshold_selects_high_util_best_file() {
        // File 1: 55% util — its OWN util is ABOVE the 50% threshold, and it is
        //         the only age-eligible candidate.
        // File 2: 5% util  — very obsolete, but the NEWEST file (age-excluded
        //         as a candidate, yet it pulls the aggregate well below 50%).
        let mut map = BTreeMap::new();
        map.insert(1u32, make_summary_sized(1000, 450)); // 55% util (> 50%)
        map.insert(2u32, make_summary_sized(1000, 950)); // 5% util (newest)

        // Aggregate: utilization(450 + 950, 2000) = utilization(1400, 2000)
        //          = round(100 * 600 / 2000) = 30% < 50% -> cleaning warranted.
        assert_eq!(FileSelector::compute_predicted_min_util(&map), 30);

        let mut selector = FileSelector::new();
        // min_age = 1 -> last_file_to_clean = 2 - 1 = 1; candidate: file 1 only.
        // File 1's own util (55%) >= min_utilization (50%): a PER-FILE gate
        // would skip it (returning None), but the AGGREGATE is below threshold,
        // so bestFile (file 1) MUST be selected.
        let result = selector.select_file_for_cleaning(
            &map, 50,    // min_utilization_pct == totalThreshold
            1,     // min_age
            false, // force
            None,  // first_active_txn_file
            5,     // min_file_utilization_pct
        );
        assert_eq!(
            result.map(|(f, _)| f),
            Some(1),
            "CLN-F1: aggregate below threshold must select bestFile even when \
             its own util >= min_utilization (pre-fix: skipped -> None)"
        );
    }

    /// CLN-F1: aggregate ABOVE threshold -> no file cleaned (prevents
    /// over-cleaning).  A sub-threshold individual file must NOT be cleaned
    /// when the aggregate says cleaning isn't warranted and no second-tier
    /// file qualifies.
    ///
    /// JE: `UtilizationCalculator.getBestFile` ~409-419 — with predictedMinUtil
    /// at or above totalThreshold and no bestGradualFile below
    /// minFileUtilization, fileChosen is null.
    #[test]
    fn test_clnf1_aggregate_above_threshold_cleans_nothing() {
        // File 1: 40% util (its OWN util is below the 50% threshold).
        // File 2: 95% util (newest).
        // Aggregate: utilization(600 + 50, 2000) = utilization(650, 2000)
        //          = round(100 * 1350 / 2000) = 68% >= 50% -> NOT warranted.
        let mut map = BTreeMap::new();
        map.insert(1u32, make_summary_sized(1000, 600)); // 40% util
        map.insert(2u32, make_summary_sized(1000, 50)); // 95% util (newest)

        assert_eq!(FileSelector::compute_predicted_min_util(&map), 68);

        let mut selector = FileSelector::new();
        // min_file_utilization = 5 -> file 1 (40%) is NOT below 5%, so tier-2
        // does not fire either.
        let result = selector.select_file_for_cleaning(
            &map, 50,    // min_utilization_pct
            1,     // min_age
            false, // force
            None,  // first_active_txn_file
            5,     // min_file_utilization_pct
        );
        assert_eq!(
            result, None,
            "CLN-F1: aggregate above threshold must clean nothing (pre-fix: \
             over-cleans the sub-threshold file 1)"
        );
    }

    /// CLN-6 Tier 1: when `predictedMinUtil >= totalThreshold`, no file is
    /// selected (global gate vetoes selection).
    ///
    /// JE: `if (predictedMinUtil < totalThreshold) { fileChosen = ... }`
    /// (~UtilizationCalculator.java line 409).
    #[test]
    fn test_cln6_global_gate_vetoes_when_predicted_above_threshold() {
        // Files at 80% util (low obsolete). predictedMinUtil = 80%.
        // totalThreshold = 50%. Since 80% >= 50%, no file should be selected.
        let mut map = BTreeMap::new();
        map.insert(1u32, make_summary_sized(1000, 200)); // 80% util
        map.insert(2u32, make_summary_sized(1000, 100)); // 90% util (newest)

        let mut selector = FileSelector::new();
        let result = selector.select_file_for_cleaning_with_policy(
            &map,
            50,       // min_utilization_pct
            1,        // min_age
            false,    // force_cleaning
            None,     // first_active_txn_file
            Some(50), // predicted_total_threshold = 50%
            None,     // min_file_utilization_pct
        );
        assert_eq!(
            result, None,
            "CLN-6: global gate must veto selection when predictedMinUtil >= totalThreshold"
        );
    }

    /// CLN-6 Tier 1: when `predictedMinUtil < totalThreshold`, a file is
    /// selected normally.
    #[test]
    fn test_cln6_global_gate_passes_when_predicted_below_threshold() {
        // File 1 at 10% util, file 2 at 20% util. Aggregate predictedMinUtil =
        // utilization(1700, 2000) = 15%.  totalThreshold = 50%. Since 15% < 50%,
        // selection proceeds and bestFile = file 1.
        let mut map = BTreeMap::new();
        map.insert(1u32, make_summary_sized(1000, 900)); // 10% util
        map.insert(2u32, make_summary_sized(1000, 800)); // 20% util (newest)

        let mut selector = FileSelector::new();
        let result = selector.select_file_for_cleaning_with_policy(
            &map,
            50,
            1,
            false,
            None,
            Some(50), // predicted_total_threshold
            None,
        );
        assert_eq!(
            result.map(|(f, _)| f),
            Some(1),
            "CLN-6: global gate should pass when predictedMinUtil < totalThreshold"
        );
    }

    /// CLN-6 Tier 3: `min_file_utilization_pct` sets a stricter per-file
    /// threshold.  Only files below BOTH thresholds qualify in normal mode.
    #[test]
    fn test_cln6_min_file_utilization_second_tier() {
        // File 1: 40% util (below normal 50% threshold but above second-tier 30%)
        // File 2: 20% util (below both thresholds) → should be selected
        // File 3: newest, skipped by age filter
        let mut map = BTreeMap::new();
        map.insert(1u32, make_summary_sized(1000, 600)); // 40% util
        map.insert(2u32, make_summary_sized(1000, 800)); // 20% util
        map.insert(3u32, make_summary_sized(1000, 100)); // 90% util (newest)

        let mut selector = FileSelector::new();
        // With min_file_utilization_pct = 30:
        // effective_threshold = min(50, 30) = 30%
        // File 1 (40%) >= 30% → excluded; File 2 (20%) < 30% → selected.
        let result = selector.select_file_for_cleaning_with_policy(
            &map,
            50,
            1,
            false,
            None,
            None,     // no global gate
            Some(30), // min_file_utilization_pct
        );
        assert_eq!(
            result.map(|(f, _)| f),
            Some(2),
            "CLN-6: second-tier threshold should exclude file 1 (40%) and select file 2 (20%)"
        );
    }

    /// CLN-6: `force_cleaning` bypasses both global gate and second tier.
    #[test]
    fn test_cln6_force_cleaning_bypasses_all_tiers() {
        // All files above both thresholds.
        let mut map = BTreeMap::new();
        map.insert(1u32, make_summary_sized(1000, 300)); // 70% util
        map.insert(2u32, make_summary_sized(1000, 100)); // 90% util (newest)

        let mut selector = FileSelector::new();
        let result = selector.select_file_for_cleaning_with_policy(
            &map,
            50,
            1,
            true, // force_cleaning
            None,
            Some(50), // global gate would veto
            Some(30), // second tier would exclude
        );
        // force_cleaning bypasses all gates → file 1 (70%) is selected.
        assert_eq!(
            result.map(|(f, _)| f),
            Some(1),
            "CLN-6: force_cleaning must bypass global gate and second tier"
        );
    }

    // ── CLN-13 acceptance tests ──────────────────────────────────────────────────

    /// `compute_predicted_min_util` returns the AGGREGATE utilization
    /// (summed obsolete / summed total), matching JE `predictedMinUtil`.
    #[test]
    fn test_cln13_compute_predicted_min_util() {
        let mut map = BTreeMap::new();
        map.insert(1u32, make_summary_sized(1000, 900)); // 10% util
        map.insert(2u32, make_summary_sized(1000, 500)); // 50% util
        map.insert(3u32, make_summary_sized(1000, 100)); // 90% util

        // Aggregate: utilization(1500, 3000) = round(100 * 1500 / 3000) = 50%.
        let predicted = FileSelector::compute_predicted_min_util(&map);
        assert_eq!(
            predicted, 50,
            "CLN-F1: predictedMinUtil is the AGGREGATE util = 50%"
        );
    }
}
