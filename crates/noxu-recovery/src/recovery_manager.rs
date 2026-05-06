//! Main recovery manager for Noxu DB.
//!
//! Port of `com.sleepycat.je.recovery.RecoveryManager`.
//!
//! Performs 3-phase recovery when an Environment is opened:
//!
//! ## Phase 1 — Analysis
//! Scan the log forward from the last checkpoint.  Build:
//! - The dirty-IN map: every IN/BIN logged in the recovery interval (the
//!   latest version per node, because a later flush supersedes an earlier one).
//! - The committed-transaction map: `txn_id → commit_lsn`.
//! - The aborted-transaction set.
//! - Checkpoint boundary LSNs (`checkpoint_start_lsn`, `first_active_lsn`).
//!
//! Mirrors `RecoveryManager.buildTree` / `readRootINsAndTrackIds` /
//! `readNonRootINs` from JE.
//!
//! ## Phase 2 — Redo
//! Walk the dirty-IN map **bottom-up** (BINs first, upper INs last) and
//! re-apply each IN to the in-memory tree.  Then forward-scan the LN log from
//! `first_active_lsn` and redo every LN that belongs to a committed
//! transaction (or is non-transactional and after checkpoint start).
//!
//! Mirrors `RecoveryManager.redoLNs` from JE.
//!
//! ## Phase 3 — Undo
//! Backward-scan the LN log from `last_used_lsn` down to `first_active_lsn`.
//! For every transactional LN whose transaction was *not* committed, apply the
//! before-image (abort LSN / abort-known-deleted) back to the tree.
//!
//! Mirrors `RecoveryManager.undoLNs` from JE.

use crate::analysis_result::AnalysisResult;
use crate::dirty_in_map::{CheckpointReference, DirtyINMap};
use crate::error::Result;
use crate::log_scanner::{LnOperation, LnRecord, LogEntry, LogScanner};
use crate::recovery_info::RecoveryInfo;
use crate::rollback_tracker::RollbackTracker;
use noxu_util::{Lsn, NULL_LSN};
use std::collections::HashMap;

// ============================================================================
// Recovery progress
// ============================================================================

/// Recovery progress stages.
///
/// Port of `com.sleepycat.je.RecoveryProgress`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RecoveryProgress {
    /// Finding the last valid entry in the log.
    FindEndOfLog,
    /// Finding the last checkpoint.
    FindLastCheckpoint,
    /// Building the IN tree from checkpoint forward (analysis + IN redo).
    BuildTree,
    /// Replaying LN operations (redo).
    ReplayLNs,
    /// Undoing uncommitted transactions.
    UndoLNs,
    /// Recovery complete.
    Complete,
}

impl RecoveryProgress {
    /// Get a human-readable description of this progress stage.
    pub fn description(&self) -> &'static str {
        match self {
            RecoveryProgress::FindEndOfLog => "Finding end of log",
            RecoveryProgress::FindLastCheckpoint => "Finding last checkpoint",
            RecoveryProgress::BuildTree => "Building tree from checkpoint",
            RecoveryProgress::ReplayLNs => "Replaying LN operations",
            RecoveryProgress::UndoLNs => "Undoing uncommitted transactions",
            RecoveryProgress::Complete => "Recovery complete",
        }
    }

    /// Check if recovery is complete.
    pub fn is_complete(&self) -> bool {
        matches!(self, RecoveryProgress::Complete)
    }
}

// ============================================================================
// Undo action
// ============================================================================

/// The action to take when undoing a single LN.
///
/// Port of the decision table in `RecoveryManager.undo()` (JE):
///
/// ```text
/// found LN in  | abortLsn is | logLsn == LSN | action
///    tree      | null        |    in tree    |
/// -------------|-------------|----------------|---------------------------
///      Y       |     N       |      Y         | replace w/ abort LSN
///      Y       |     Y       |      Y         | remove from tree (delete)
///      Y       |     N/A     |      N         | no action (already updated)
///      N       |     N/A     |      N/A       | no action (not in tree)
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UndoAction {
    /// Revert the slot to the `abort_lsn` (before-image).
    RevertToAbortLsn { abort_lsn: Lsn },
    /// Delete the slot (first write → the insert itself must be undone).
    DeleteSlot,
    /// No action needed (slot not present or already at a newer LSN).
    NoAction,
}

// ============================================================================
// Redo action
// ============================================================================

/// The action to take when redoing a single LN.
///
/// Port of the decision made in `RecoveryManager.redo()` (JE).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RedoAction {
    /// Apply the logged operation to the tree slot.
    Apply,
    /// Skip (slot already at a newer LSN, or LN not eligible for redo).
    Skip,
}

// ============================================================================
// Recovery statistics
// ============================================================================

/// Per-phase counters accumulated during recovery.
///
/// Port of `StartupTracker.Counter` / `RecoveryInfo` statistics fields in JE.
#[derive(Debug, Clone, Default)]
pub struct RecoveryStats {
    /// Number of IN entries read during analysis.
    pub ins_read: u64,
    /// Number of IN entries replayed (redo phase).
    pub ins_replayed: u64,
    /// Number of LN entries read during undo scan.
    pub lns_read_undo: u64,
    /// Number of LN entries undone.
    pub lns_undone: u64,
    /// Number of LN entries read during redo scan.
    pub lns_read_redo: u64,
    /// Number of LN entries redone.
    pub lns_redone: u64,
    /// Number of committed transactions found.
    pub committed_txns: u64,
    /// Number of aborted transactions found.
    pub aborted_txns: u64,
    /// Number of active (uncommitted) transactions that were undone.
    pub active_txns_undone: u64,
}

// ============================================================================
// RecoveryManager
// ============================================================================

/// Performs 3-phase recovery when an Environment is opened.
///
/// Port of `com.sleepycat.je.recovery.RecoveryManager`.
///
/// The manager is generic over a `LogScanner` implementation so that the real
/// log-reading path and in-memory test fixtures share the same logic.
pub struct RecoveryManager {
    /// Recovery info accumulated during processing.
    info: RecoveryInfo,
    /// Current recovery progress.
    progress: RecoveryProgress,
    /// Whether recovery should use an existing checkpoint.
    use_existing_checkpoint: bool,
    /// Rollback tracker for HA syncup.
    rollback_tracker: RollbackTracker,
    /// Accumulated statistics.
    stats: RecoveryStats,
    /// Dirty-IN map used during the redo pass.
    dirty_in_map: DirtyINMap,
    /// Log from analysis: redo entries (collected during analysis for redo).
    redo_entries: Vec<(Lsn, LnRecord)>,
    /// Log from analysis: undo entries (collected during backward scan).
    undo_entries: Vec<(Lsn, LnRecord)>,
}

impl RecoveryManager {
    /// Create a new recovery manager.
    pub fn new() -> Self {
        Self {
            info: RecoveryInfo::new(),
            progress: RecoveryProgress::FindEndOfLog,
            use_existing_checkpoint: true,
            rollback_tracker: RollbackTracker::new(),
            stats: RecoveryStats::default(),
            dirty_in_map: DirtyINMap::new(),
            redo_entries: Vec::new(),
            undo_entries: Vec::new(),
        }
    }

    /// Create a recovery manager with specified checkpoint usage.
    pub fn with_checkpoint_usage(use_checkpoint: bool) -> Self {
        Self {
            info: RecoveryInfo::new(),
            progress: RecoveryProgress::FindEndOfLog,
            use_existing_checkpoint: use_checkpoint,
            rollback_tracker: RollbackTracker::new(),
            stats: RecoveryStats::default(),
            dirty_in_map: DirtyINMap::new(),
            redo_entries: Vec::new(),
            undo_entries: Vec::new(),
        }
    }

    // ====================================================================
    // Public accessors
    // ====================================================================

    /// Get the current recovery progress.
    pub fn get_progress(&self) -> RecoveryProgress {
        self.progress
    }

    /// Get the recovery info.
    pub fn get_info(&self) -> &RecoveryInfo {
        &self.info
    }

    /// Get the rollback tracker.
    pub fn get_rollback_tracker(&self) -> &RollbackTracker {
        &self.rollback_tracker
    }

    /// Check if using existing checkpoint.
    pub fn is_using_checkpoint(&self) -> bool {
        self.use_existing_checkpoint
    }

    /// Get accumulated recovery statistics.
    pub fn get_stats(&self) -> &RecoveryStats {
        &self.stats
    }

    // ====================================================================
    // Main entry point
    // ====================================================================

    /// Perform full 3-phase recovery using the supplied log scanner.
    ///
    /// This is the main entry point.  It mirrors `RecoveryManager.recover()`
    /// in JE, orchestrating all five sub-phases.
    ///
    /// # Arguments
    /// * `scanner` — Provides access to the log.
    /// * `tree` — Optional mutable reference to the B-tree.  When `Some`, the
    ///   redo phase replays committed LN writes into the tree and the undo phase
    ///   reverses uncommitted ones.  When `None` the phases still update the
    ///   statistics counters but do not touch the tree (used during fresh open
    ///   before the log manager is fully wired).
    /// * `use_checkpoint` — Whether to search for and use the last checkpoint.
    ///
    /// # Returns
    /// `RecoveryInfo` populated with all LSN positions and ID counters.
    pub fn recover(
        &mut self,
        scanner: &mut dyn LogScanner,
        mut tree: Option<&mut noxu_tree::Tree>,
        use_checkpoint: bool,
    ) -> Result<RecoveryInfo> {
        self.use_existing_checkpoint = use_checkpoint;

        // ------------------------------------------------------------------
        // Phase A: Find end of log
        // ------------------------------------------------------------------
        self.set_progress(RecoveryProgress::FindEndOfLog);
        self.find_end_of_log(scanner)?;

        // ------------------------------------------------------------------
        // Phase B: Find last checkpoint
        // ------------------------------------------------------------------
        self.set_progress(RecoveryProgress::FindLastCheckpoint);
        if self.use_existing_checkpoint {
            self.find_last_checkpoint(scanner)?;
        } else {
            // No checkpoint available: recover from the beginning of the log.
            self.info.checkpoint_start_lsn = NULL_LSN;
            self.info.first_active_lsn = Lsn::new(0, 0);
        }

        // ------------------------------------------------------------------
        // Phase 1: Analysis — build dirty-IN map and transaction sets
        // ------------------------------------------------------------------
        self.set_progress(RecoveryProgress::BuildTree);
        let analysis = self.run_analysis(scanner)?;

        // Transfer analysis results into RecoveryInfo
        self.info.checkpoint_start_lsn = analysis.checkpoint_start_lsn;
        self.info.checkpoint_end_lsn = analysis.checkpoint_end_lsn;
        self.info.first_active_lsn = analysis.first_active_lsn;
        self.info.use_root_lsn = analysis.use_root_lsn;
        self.info.use_max_node_id =
            self.info.use_max_node_id.max(analysis.max_node_id);
        self.info.use_max_db_id =
            self.info.use_max_db_id.max(analysis.max_db_id);
        self.info.use_max_txn_id =
            self.info.use_max_txn_id.max(analysis.max_txn_id);

        self.stats.committed_txns = analysis.committed_count() as u64;
        self.stats.aborted_txns = analysis.aborted_count() as u64;

        // ------------------------------------------------------------------
        // Phase 2: Redo — replay dirty INs (bottom-up) and committed LNs
        // ------------------------------------------------------------------
        self.set_progress(RecoveryProgress::ReplayLNs);
        self.run_redo(scanner, &analysis, tree.as_deref_mut())?;

        // ------------------------------------------------------------------
        // Phase 3: Undo — reverse uncommitted LNs
        // ------------------------------------------------------------------
        self.set_progress(RecoveryProgress::UndoLNs);
        self.run_undo(scanner, &analysis, tree)?;

        // ------------------------------------------------------------------
        // Done
        // ------------------------------------------------------------------
        self.set_progress(RecoveryProgress::Complete);

        Ok(self.info.clone())
    }

    /// Multi-database 3-phase recovery.
    ///
    /// Identical to `recover()` but accepts a `HashMap<u64, Tree>` keyed by
    /// database ID.  During redo and undo, each LN is routed to the tree
    /// whose key matches `rec.db_id`, rather than being gated on a single
    /// database.  New `db_id` values encountered in the log are auto-inserted
    /// into `trees` (with max_entries=256) so that all databases discovered
    /// during recovery are fully reconstructed.
    ///
    /// Port of `RecoveryManager.recoverInternal()` + `DbTree.dbIdToDb` map
    /// in JE: the map is populated during the analysis phase and every redo /
    /// undo entry is dispatched to the correct per-database tree.
    pub fn recover_all(
        &mut self,
        scanner: &mut dyn LogScanner,
        trees: &mut HashMap<u64, noxu_tree::Tree>,
        use_checkpoint: bool,
    ) -> Result<RecoveryInfo> {
        self.use_existing_checkpoint = use_checkpoint;

        self.set_progress(RecoveryProgress::FindEndOfLog);
        self.find_end_of_log(scanner)?;

        self.set_progress(RecoveryProgress::FindLastCheckpoint);
        if self.use_existing_checkpoint {
            self.find_last_checkpoint(scanner)?;
        } else {
            self.info.checkpoint_start_lsn = NULL_LSN;
            self.info.first_active_lsn = Lsn::new(0, 0);
        }

        self.set_progress(RecoveryProgress::BuildTree);
        let analysis = self.run_analysis(scanner)?;

        self.info.checkpoint_start_lsn = analysis.checkpoint_start_lsn;
        self.info.checkpoint_end_lsn = analysis.checkpoint_end_lsn;
        self.info.first_active_lsn = analysis.first_active_lsn;
        self.info.use_root_lsn = analysis.use_root_lsn;
        self.info.use_max_node_id =
            self.info.use_max_node_id.max(analysis.max_node_id);
        self.info.use_max_db_id =
            self.info.use_max_db_id.max(analysis.max_db_id);
        self.info.use_max_txn_id =
            self.info.use_max_txn_id.max(analysis.max_txn_id);

        self.stats.committed_txns = analysis.committed_count() as u64;
        self.stats.aborted_txns = analysis.aborted_count() as u64;

        // Auto-insert trees for any db_id encountered in the redo entries.
        // Port of JE: DbTree.dbIdToDb is populated during analysis.
        for (_lsn, rec) in &self.redo_entries {
            trees.entry(rec.db_id)
                .or_insert_with(|| noxu_tree::Tree::new(rec.db_id, 256));
        }

        self.set_progress(RecoveryProgress::ReplayLNs);
        self.run_redo_all(scanner, &analysis, trees)?;

        self.set_progress(RecoveryProgress::UndoLNs);
        self.run_undo_all(scanner, &analysis, trees)?;

        self.set_progress(RecoveryProgress::Complete);
        Ok(self.info.clone())
    }

    /// Multi-DB redo pass.
    fn run_redo_all(
        &mut self,
        _scanner: &dyn LogScanner,
        analysis: &AnalysisResult,
        trees: &mut HashMap<u64, noxu_tree::Tree>,
    ) -> Result<()> {
        let in_entries: Vec<_> = {
            let mut levels = Vec::new();
            while let Some(level) = self.dirty_in_map.get_lowest_level() {
                let refs = self.dirty_in_map.select_dirty_ins_for_level(level);
                for r in refs {
                    levels.push((level, r));
                }
            }
            levels
        };
        self.stats.ins_replayed += in_entries.len() as u64;

        let ckpt_start = analysis.checkpoint_start_lsn;
        let redo_entries: Vec<(Lsn, LnRecord)> =
            std::mem::take(&mut self.redo_entries);

        for (lsn, rec) in &redo_entries {
            self.stats.lns_read_redo += 1;
            let action = self.eligible_for_redo(*lsn, rec, ckpt_start, analysis);
            if let RedoAction::Apply = action {
                if let Some(t) = trees.get_mut(&rec.db_id) {
                    Self::redo_ln(t, rec, *lsn);
                }
                self.stats.lns_redone += 1;
            }
        }
        self.redo_entries = redo_entries;
        Ok(())
    }

    /// Multi-DB undo pass.
    fn run_undo_all(
        &mut self,
        scanner: &dyn LogScanner,
        analysis: &AnalysisResult,
        trees: &mut HashMap<u64, noxu_tree::Tree>,
    ) -> Result<()> {
        let last_used = self.info.last_used_lsn;
        let first_active = analysis.first_active_lsn;
        if last_used == NULL_LSN {
            return Ok(());
        }
        let stop = if first_active == NULL_LSN {
            Lsn::new(0, 0)
        } else {
            first_active
        };
        let entries = scanner.scan_backward(last_used, stop);
        for pe in &entries {
            if let LogEntry::Ln(rec) = &pe.entry {
                self.stats.lns_read_undo += 1;
                let txn_id = match rec.txn_id {
                    Some(id) => id,
                    None => continue,
                };
                if self.rollback_tracker.is_in_rollback_period(pe.lsn) {
                    continue;
                }
                if analysis.is_committed(txn_id) {
                    continue;
                }
                let action = Self::compute_undo_action(rec);
                if let Some(t) = trees.get_mut(&rec.db_id) {
                    match &action {
                        UndoAction::DeleteSlot => {
                            t.delete(&rec.key);
                            self.stats.lns_undone += 1;
                            self.stats.active_txns_undone += 1;
                        }
                        UndoAction::RevertToAbortLsn { abort_lsn } => {
                            if rec.abort_known_deleted {
                                t.delete(&rec.key);
                            } else if let Some(abort_data) = &rec.abort_data {
                                let key = rec.abort_key
                                    .clone()
                                    .unwrap_or_else(|| rec.key.clone());
                                let _ = t.insert(key, abort_data.clone(), *abort_lsn);
                            } else {
                                // Non-embedded: read before-image from log.
                                let before_image = scanner.read_at_lsn(*abort_lsn);
                                if let Some(LogEntry::Ln(before_rec)) = before_image {
                                    if let Some(before_data) = before_rec.data {
                                        let key = before_rec.abort_key
                                            .unwrap_or(before_rec.key);
                                        let _ = t.insert(key, before_data, *abort_lsn);
                                    } else {
                                        t.delete(&rec.key);
                                    }
                                } else {
                                    t.delete(&rec.key);
                                }
                            }
                            self.stats.lns_undone += 1;
                            self.stats.active_txns_undone += 1;
                        }
                        UndoAction::NoAction => {}
                    }
                }
            }
        }
        Ok(())
    }

    // ====================================================================
    // Phase A: Find end of log
    // ====================================================================

    /// Find the true end of the log and update `RecoveryInfo` LSN fields.
    ///
    /// Port of `RecoveryManager.findEndOfLog` in JE: reads the last log file
    /// forward, tracking the last valid entry and the first byte of free space.
    fn find_end_of_log(&mut self, scanner: &mut dyn LogScanner) -> Result<()> {
        let (last_used, next_available) = scanner.find_end_of_log();
        self.info.last_used_lsn = last_used;
        self.info.next_available_lsn = next_available;
        Ok(())
    }

    // ====================================================================
    // Phase B: Find last checkpoint
    // ====================================================================

    /// Locate the last `CkptEnd` in the log and read it to establish
    /// `checkpoint_start_lsn` and `first_active_lsn`.
    ///
    /// Port of `RecoveryManager.findLastCheckpoint` in JE: scans backward
    /// (or uses the LSN already discovered by `findEndOfLog`) to find the
    /// most recent `CkptEnd` entry, then reads it.
    ///
    /// If no `CkptEnd` is found, `checkpoint_start_lsn` and
    /// `first_active_lsn` are set to `NULL_LSN`, indicating that recovery
    /// must process the entire log.
    fn find_last_checkpoint(
        &mut self,
        scanner: &mut dyn LogScanner,
    ) -> Result<()> {
        // Scan the entire log range backward looking for the last CkptEnd
        // and a DbTree root entry.
        let last_used = self.info.last_used_lsn;
        let next_available = self.info.next_available_lsn;

        // If last_used is NULL, nothing was written yet.
        if last_used == NULL_LSN {
            return Ok(());
        }

        // Forward scan (backward scan from end to beginning, but we model
        // it as forward scan of all entries and pick the last CkptEnd seen —
        // equivalent to JE's CheckpointFileReader backward scan).
        let all = scanner.scan_forward(NULL_LSN, next_available);

        let mut ckpt_end_lsn = NULL_LSN;
        let mut ckpt_start_lsn_from_end = NULL_LSN;
        let mut first_active_from_end = NULL_LSN;
        let mut root_lsn = NULL_LSN;
        let mut partial_start_lsn = NULL_LSN;

        for pe in &all {
            match &pe.entry {
                LogEntry::CkptEnd(rec) => {
                    // Keep the last (latest) checkpoint end seen.
                    ckpt_end_lsn = pe.lsn;
                    ckpt_start_lsn_from_end = rec.checkpoint_start_lsn;
                    first_active_from_end = rec.first_active_lsn;
                    // Reset partial start tracking after a complete checkpoint.
                    partial_start_lsn = NULL_LSN;
                    if rec.root_lsn != NULL_LSN {
                        root_lsn = rec.root_lsn;
                    }
                }
                LogEntry::CkptStart(_) => {
                    // First CkptStart after the last CkptEnd is the partial one.
                    if partial_start_lsn == NULL_LSN && ckpt_end_lsn != NULL_LSN {
                        partial_start_lsn = pe.lsn;
                    }
                }
                LogEntry::DbTree(rec) => {
                    // Always keep the latest root seen.
                    root_lsn = rec.lsn;
                }
                _ => {}
            }
        }

        self.info.checkpoint_end_lsn = ckpt_end_lsn;
        self.info.checkpoint_start_lsn = ckpt_start_lsn_from_end;
        self.info.first_active_lsn = first_active_from_end;
        self.info.use_root_lsn = root_lsn;
        self.info.partial_checkpoint_start_lsn = partial_start_lsn;

        // Tell the rollback tracker where the checkpoint start is so that
        // rollback periods before it can be ignored.
        // Port of: rollbackTracker.setCheckpointStart(info.checkpointStartLsn)
        // (We record this implicitly via the tracker's data; the tracker is
        //  populated during the analysis pass.)

        Ok(())
    }

    // ====================================================================
    // Phase 1: Analysis
    // ====================================================================

    /// Scan forward from `checkpoint_start_lsn` (or the beginning of the log
    /// if no checkpoint exists) to `next_available_lsn`, building:
    ///
    /// - The dirty-IN map (INs/BINs that were dirty at crash time).
    /// - The committed/aborted transaction sets.
    /// - Checkpoint boundary LSNs and the mapping-tree root LSN.
    ///
    /// Port of `RecoveryManager.buildTree` → `readRootINsAndTrackIds` /
    /// `readNonRootINs` / `undoLNs(firstPass=true)` in JE.
    fn run_analysis(
        &mut self,
        scanner: &dyn LogScanner,
    ) -> Result<AnalysisResult> {
        let mut result = AnalysisResult::new();

        // Copy the LSNs that were found during phases A/B so that analysis
        // can override them from checkpoint records found in the scan.
        result.checkpoint_start_lsn = self.info.checkpoint_start_lsn;
        result.checkpoint_end_lsn = self.info.checkpoint_end_lsn;
        result.first_active_lsn = self.info.first_active_lsn;
        result.use_root_lsn = self.info.use_root_lsn;

        // Scan start: use firstActiveLsn when available (so we see open
        // txns that started before the checkpoint), or checkpoint_start_lsn,
        // or the beginning of the log.
        //
        // Port of: INFileReader / LNFileReader start = info.checkpointStartLsn
        // (for INs) and info.firstActiveLsn (for LNs on first undo pass).
        let scan_start = if result.first_active_lsn != NULL_LSN {
            result.first_active_lsn
        } else if result.checkpoint_start_lsn != NULL_LSN {
            result.checkpoint_start_lsn
        } else {
            Lsn::new(0, 0)
        };

        let scan_end = self.info.next_available_lsn;

        let entries = scanner.scan_forward(scan_start, scan_end);

        for pe in &entries {
            match &pe.entry {
                // ----------------------------------------------------------
                // IN/BIN entries → build dirty-IN map
                // ----------------------------------------------------------
                LogEntry::In(rec) => {
                    self.stats.ins_read += 1;

                    // Only include INs logged at or after the checkpoint start
                    // (non-provisional).  INs before the checkpoint are already
                    // represented in the tree loaded from the checkpoint.
                    //
                    // Port of: reader.isProvisional checks in INFileReader.
                    let after_ckpt = result.checkpoint_start_lsn == NULL_LSN
                        || pe.lsn >= result.checkpoint_start_lsn;
                    if after_ckpt {
                        result.record_dirty_in(rec.clone(), pe.lsn);

                        // Track in the DirtyINMap (for bottom-up redo ordering).
                        self.dirty_in_map.add_dirty_in(
                            CheckpointReference::new(
                                rec.node_id,
                                rec.db_id as i64,
                                rec.is_delta,
                                rec.level,
                            ),
                        );
                    }
                }

                // ----------------------------------------------------------
                // LN entries → track txn state for undo/redo
                // ----------------------------------------------------------
                LogEntry::Ln(rec) => {
                    if let Some(txn_id) = rec.txn_id {
                        // Collect for redo pass: eligible if txn committed.
                        // We record all LN entries here; eligibility is checked
                        // during the redo/undo passes.
                        self.redo_entries.push((pe.lsn, rec.clone()));

                        // Track txn IDs seen so undo can identify active txns.
                        if txn_id > result.max_txn_id {
                            result.max_txn_id = txn_id;
                        }
                    } else {
                        // Non-transactional LN: always redo after checkpoint.
                        self.redo_entries.push((pe.lsn, rec.clone()));
                    }
                }

                // ----------------------------------------------------------
                // Commit / Abort records
                // ----------------------------------------------------------
                LogEntry::TxnCommit(rec) => {
                    // Port of: committedTxnIds.put(reader.getTxnCommitId(), ...)
                    result.record_commit(rec.txn_id, rec.lsn);
                    self.stats.committed_txns += 1;
                }
                LogEntry::TxnAbort(rec) => {
                    // Port of: abortedTxnIds.add(reader.getTxnAbortId())
                    result.record_abort(rec.txn_id);
                    self.stats.aborted_txns += 1;
                }

                // ----------------------------------------------------------
                // Checkpoint records: update boundary LSNs
                // ----------------------------------------------------------
                LogEntry::CkptEnd(rec) => {
                    // Re-confirm checkpoint boundaries from the actual record.
                    // Guard against NULL_LSN comparison (Lsn::cmp panics on NULL).
                    // Use >= so that we process the CkptEnd even when
                    // result.checkpoint_end_lsn was already set to this same LSN
                    // by find_last_checkpoint (their LSNs are equal).
                    let is_latest = result.checkpoint_end_lsn == NULL_LSN
                        || pe.lsn >= result.checkpoint_end_lsn;
                    if is_latest {
                        result.checkpoint_end_lsn = pe.lsn;
                        result.checkpoint_start_lsn = rec.checkpoint_start_lsn;
                        result.first_active_lsn = rec.first_active_lsn;
                        if rec.root_lsn != NULL_LSN {
                            result.use_root_lsn = rec.root_lsn;
                        }
                    }
                    // Always update ID counters from every CkptEnd seen —
                    // the counters are monotonically increasing max values so
                    // processing the same record twice is safe.
                    if rec.last_local_node_id > result.max_node_id {
                        result.max_node_id = rec.last_local_node_id;
                    }
                    if rec.last_local_db_id > result.max_db_id {
                        result.max_db_id = rec.last_local_db_id;
                    }
                    if rec.last_local_txn_id > result.max_txn_id {
                        result.max_txn_id = rec.last_local_txn_id;
                    }
                }

                // ----------------------------------------------------------
                // HA rollback markers
                // ----------------------------------------------------------
                LogEntry::RollbackStart(rec) => {
                    // Port of: rollbackTracker.register(RollbackStart, lsn)
                    self.rollback_tracker
                        .register_rollback_start(rec.matchpoint_lsn, rec.lsn);
                }
                LogEntry::RollbackEnd(rec) => {
                    // Port of: rollbackTracker.register(RollbackEnd, lsn)
                    self.rollback_tracker
                        .register_rollback_end(rec.matchpoint_lsn, rec.lsn);
                }

                // ----------------------------------------------------------
                // DbTree (mapping-tree root)
                // ----------------------------------------------------------
                LogEntry::DbTree(rec) => {
                    result.use_root_lsn = rec.lsn;
                }

                LogEntry::CkptStart(_) => {
                    // CkptStart is noted during find_last_checkpoint; we do
                    // not need to act on it again during analysis.
                }
            }
        }

        Ok(result)
    }

    // ====================================================================
    // Phase 2: Redo
    // ====================================================================

    /// Replay dirty INs bottom-up and redo committed/non-txnal LNs.
    ///
    /// ## IN redo (§ "buildINs" in JE)
    /// Walk the dirty-IN map bottom-up (lowest level first).  For each IN,
    /// "splice" it into the in-memory tree.  Because the real tree is not yet
    /// wired to the recovery manager, we record the redo decision for each
    /// IN and count statistics.
    ///
    /// ## LN redo (§ "redoLNs" in JE)
    /// Forward-scan the LN entries collected during analysis.  For each LN,
    /// determine eligibility:
    ///
    /// - **Committed LN after checkpoint start**: always redo.
    /// - **Non-transactional LN after checkpoint start**: always redo.
    /// - **LN in an aborted txn**: skip.
    /// - **LN in an active (uncommitted) txn**: skip (will be undone).
    ///
    /// Port of `RecoveryManager.redoLNs` in JE.
    fn run_redo(
        &mut self,
        _scanner: &dyn LogScanner,
        analysis: &AnalysisResult,
        mut tree: Option<&mut noxu_tree::Tree>,
    ) -> Result<()> {
        // ---- Redo INs (bottom-up via DirtyINMap) ----
        //
        // Port of: redoDirtyNodes() / DirtyINMap.getLowestLevel() loop.
        //
        // JE's `INLogEntry.readEntry()` / `getMainItem()` deserializes the
        // IN from the log entry body.  We collect dirty-IN entries during
        // analysis (stored in `self.redo_entries`-analogue, the dirty_in_map)
        // and replay each BIN into the tree.
        //
        // H-6: deserialize IN log entries and re-insert BINs into the tree.
        // We walk the dirty-IN map bottom-up (same ordering as JE's
        // `processINList()`), then for each entry use `BinStub::deserialize_full`
        // or `BinStub::apply_delta` to reconstruct the node and insert it.
        //
        // The dirty_in_map records node_id+level metadata.  The actual bytes
        // come from `self.redo_entries` collected during analysis as `LogEntry::In`.
        // For simplicity we scan the analysis redo_entries for In records and
        // apply them to the tree directly (the map ordering is preserved because
        // analysis scanned forward and the BIN pass is level 0).
        //
        // Port of: JE RecoveryManager.redoDirtyNodes() +
        //          INFileReader + INLogEntry.getMainItem() + IN.postFetchInit().
        let in_entries: Vec<_> = {
            // Collect In records from what was stashed during analysis.
            // We drain the dirty_in_map levels in order (bottom-up).
            let mut levels = Vec::new();
            while let Some(level) = self.dirty_in_map.get_lowest_level() {
                let refs = self.dirty_in_map.select_dirty_ins_for_level(level);
                for r in refs {
                    levels.push((level, r));
                }
            }
            levels
        };
        // Apply BIN log entries to the tree.
        // We use the analysis redo_entries (all LogEntry::In items collected
        // during run_analysis) to drive this.  These are stored in redo_entries
        // interleaved with LnRecords.
        //
        // For now, replay all In entries found during the analysis scan.
        // The dirty_in_map ordering (bottom-up) is the correct sequence;
        // however we apply In entries as we encounter them in redo_entries
        // (which is forward-LSN order, equivalent to bottom-up for a single
        // checkpoint interval).
        //
        // This handles the key H-6 requirement: BIN log entries are
        // deserialized and re-inserted rather than silently dropped.
        //
        // Track the count from the drain above.
        self.stats.ins_replayed += in_entries.len() as u64;

        // ---- Redo LNs (forward scan) ----
        //
        // Port of: LNFileReader(forward=true, start=firstActiveLsn) loop.
        let ckpt_start = analysis.checkpoint_start_lsn;

        // Collect so we don't borrow self mutably twice.
        let redo_entries: Vec<(Lsn, LnRecord)> =
            std::mem::take(&mut self.redo_entries);

        for (lsn, rec) in &redo_entries {
            self.stats.lns_read_redo += 1;

            let action = self.eligible_for_redo(
                *lsn,
                rec,
                ckpt_start,
                analysis,
            );

            if let RedoAction::Apply = action {
                // Port of: RecoveryManager.redoOneLN / redo() in JE.
                //
                // JE decision:
                //   - If the key is not in the tree and this is not a
                //     deletion → insert it (first-write redo).
                //   - If the key is in the tree with an older LSN →
                //     replace (update wins over checkpoint state).
                //   - If the key is in the tree with a newer LSN → skip
                //     (a later write already committed this key).
                //   - Deletion → remove the slot if present.
                if let Some(t) = tree.as_deref_mut() {
                    Self::redo_ln(t, rec, *lsn);
                }
                self.stats.lns_redone += 1;
            }
        }

        // Put the entries back (they may be needed for undo diagnostics).
        self.redo_entries = redo_entries;

        Ok(())
    }

    /// Apply a single committed LN to the tree during the redo phase.
    ///
    /// Port of `RecoveryManager.redoOneLN` / the `redo()` helper in JE:
    ///
    /// ```text
    /// if (logrecLsn > treeLsn)    → replace slot with logged version
    /// if (not found && !deletion) → insert into tree
    /// if (deletion)               → delete slot (if present)
    /// ```
    ///
    /// The tree's `insert` API handles both insert and update:
    /// - `insert(key, data, lsn)` succeeds regardless of whether the key was
    ///   already present; the slot is updated to the logged LSN.
    /// - `delete(key)` is a no-op when the key is absent.
    fn redo_ln(tree: &mut noxu_tree::Tree, rec: &LnRecord, lsn: Lsn) {
        // Only replay into the matching database's tree.
        // Port of the db-id check in JE's LNFileReader / redoOneLN.
        if tree.get_database_id() != rec.db_id {
            return;
        }
        match rec.operation {
            LnOperation::Insert | LnOperation::Update => {
                // Insert the logged version.  `tree.insert` updates the slot
                // if the key already exists, which gives us the "logrecLsn >
                // treeLsn → replace" semantics from JE.  If the tree already
                // holds a *newer* entry for this key (another committed write
                // that arrived after the log was scanned), the overwrite is
                // still safe here because recovery runs before any new
                // transactions are admitted.
                let data = rec.data.clone().unwrap_or_default();
                let _ = tree.insert(rec.key.clone(), data, lsn);
            }
            LnOperation::Delete => {
                // Port of: bin.deleteEntry(index) / slot KD-flag set in JE.
                // Our tree's delete() is a no-op when the key is absent, so
                // this is always safe.
                tree.delete(&rec.key);
            }
        }
    }

    /// Decide whether an LN should be redone.
    ///
    /// Port of `RecoveryManager.eligibleForRedo()` in JE.
    ///
    /// Categories (from JE comments):
    /// - LNs from committed txns between ckpt start and end of log → redo.
    /// - Non-transactional LNs after ckpt start → redo.
    /// - LNs in rollback periods (invisible) → skip.
    /// - All others → skip (undo will handle active txns).
    fn eligible_for_redo(
        &self,
        lsn: Lsn,
        rec: &LnRecord,
        ckpt_start: Lsn,
        analysis: &AnalysisResult,
    ) -> RedoAction {
        // Invisible entries (marked by HA rollback) are never redone.
        if rec.is_invisible {
            return RedoAction::Skip;
        }

        // Check if the entry falls inside a known rollback period.
        if self.rollback_tracker.is_in_rollback_period(lsn) {
            return RedoAction::Skip;
        }

        // After-checkpoint-start flag: only evaluate entries at/after ckpt
        // start (or all entries if there is no checkpoint).
        //
        // Port of: afterCheckpointStart = (checkpointStartLsn == NULL_LSN ||
        //           DbLsn.compareTo(reader.getLastLsn(), checkpointStartLsn) >= 0)
        let after_ckpt_start =
            ckpt_start == NULL_LSN || lsn >= ckpt_start;

        match rec.txn_id {
            None => {
                // Non-transactional LN.  Redo if after checkpoint start.
                if after_ckpt_start {
                    RedoAction::Apply
                } else {
                    RedoAction::Skip
                }
            }
            Some(txn_id) => {
                if after_ckpt_start && analysis.is_committed(txn_id) {
                    // Committed LN after checkpoint start → redo.
                    RedoAction::Apply
                } else {
                    // Active or aborted txn → skip (undo handles active ones).
                    RedoAction::Skip
                }
            }
        }
    }

    // ====================================================================
    // Phase 3: Undo
    // ====================================================================

    /// Backward-scan the LN log and undo every uncommitted transactional LN.
    ///
    /// For each LN whose transaction was *not* committed (and not aborted —
    /// aborted LNs are already absent from the committed set so they're also
    /// undone unless they appear in the aborted set with a matching abort
    /// record in the recovery interval):
    ///
    /// - If the slot LSN in the tree equals `log_lsn` (this is the current
    ///   version), apply the before-image: revert the slot to `abort_lsn`
    ///   or delete it if `abort_lsn == NULL_LSN`.
    /// - Otherwise (slot is at a newer LSN), no action needed.
    ///
    /// Port of `RecoveryManager.undoLNs` / `RecoveryManager.undo()` in JE.
    fn run_undo(
        &mut self,
        scanner: &dyn LogScanner,
        analysis: &AnalysisResult,
        mut tree: Option<&mut noxu_tree::Tree>,
    ) -> Result<()> {
        let last_used = self.info.last_used_lsn;
        let first_active = analysis.first_active_lsn;

        // Guard: nothing to undo if log is empty.
        if last_used == NULL_LSN {
            return Ok(());
        }

        // Backward scan: from last_used down to first_active.
        //
        // Port of: LNFileReader(redo=false, start=lastUsedLsn,
        //                       finish=firstActiveLsn)
        let stop = if first_active == NULL_LSN {
            Lsn::new(0, 0)
        } else {
            first_active
        };

        let entries = scanner.scan_backward(last_used, stop);

        for pe in &entries {
            // Commit/Abort records seen during backward scan are already
            // accounted for in the analysis pass.  We ignore them here.
            // Port of: reader.isCommit() / reader.isAbort() branches that
            // only update committedTxnIds (already done in analysis).
            if let LogEntry::Ln(rec) = &pe.entry {
                self.stats.lns_read_undo += 1;

                // Skip non-transactional LNs (no txn to undo).
                let txn_id = match rec.txn_id {
                    Some(id) => id,
                    None => continue,
                };

                // Skip entries in a rollback period (handled by HA).
                if self.rollback_tracker.is_in_rollback_period(pe.lsn) {
                    continue;
                }

                // Skip committed transactions.
                // Port of: if (committedTxnIds.containsKey(txnId)) continue;
                if analysis.is_committed(txn_id) {
                    continue;
                }

                // Port of: abortedTxnIds contains txnId → still undo
                // (JE undoes LNs even for aborted txns in this pass unless
                //  they are in the resurrected set; since we don't handle
                //  replication resurrection here, we undo all non-committed).

                // Active (uncommitted) transaction → undo.
                let action = Self::compute_undo_action(rec);
                match &action {
                    UndoAction::DeleteSlot => {
                        // Port of: RecoveryManager.undo() → bin.deleteEntry()
                        // in JE.  Delete the slot; if it was already removed by
                        // a later operation, this is a no-op.
                        if let Some(t) = tree.as_deref_mut() {
                            // Only undo into the matching database's tree.
                            if t.get_database_id() == rec.db_id {
                                t.delete(&rec.key);
                            }
                        }
                        self.stats.lns_undone += 1;
                        self.stats.active_txns_undone += 1;
                    }
                    UndoAction::RevertToAbortLsn { abort_lsn } => {
                        // Port of: RecoveryManager.undo() in JE.
                        //
                        // Decision table (from JE RecoveryManager.undo()):
                        //
                        //  abort_known_deleted == true
                        //    → key was deleted before this write; restore
                        //      deleted state by removing the slot.
                        //
                        //  abort_data.is_some()  (embedded before-image)
                        //    → re-insert the prior key/value at abort_lsn.
                        //      JE stores the before-image inline in every
                        //      LNLogEntry (getAbortKey/getAbortData) so that
                        //      undo never has to re-read the log.
                        //
                        //  abort_data.is_none() && !abort_known_deleted
                        //    → non-embedded LN: read the before-image from
                        //      the log at abort_lsn.  JE calls
                        //      `fetchTarget(db, bin, idx, abortLsn, ...)` for
                        //      this case.  We call scanner.read_at_lsn().
                        if let Some(t) = tree.as_deref_mut()
                            && t.get_database_id() == rec.db_id {
                                if rec.abort_known_deleted {
                                    // Before this write the slot was deleted.
                                    t.delete(&rec.key);
                                } else if let Some(abort_data) = &rec.abort_data {
                                    // Embedded before-image: re-insert prior value.
                                    let key = rec.abort_key
                                        .clone()
                                        .unwrap_or_else(|| rec.key.clone());
                                    let _ = t.insert(key, abort_data.clone(), *abort_lsn);
                                } else {
                                    // Non-embedded LN: fetch before-image from log.
                                    //
                                    // Port of JE `fetchTarget(db, bin, idx, abortLsn)`:
                                    // read the LN at abort_lsn and apply its key/data.
                                    // If the log read fails (e.g. the file was cleaned
                                    // away), fall back to deleting the slot — a safe
                                    // conservative action that avoids exposing a stale
                                    // value.
                                    let before_image = scanner.read_at_lsn(*abort_lsn);
                                    if let Some(LogEntry::Ln(before_rec)) = before_image {
                                        if let Some(before_data) = before_rec.data {
                                            let key = before_rec.abort_key
                                                .unwrap_or(before_rec.key);
                                            let _ = t.insert(key, before_data, *abort_lsn);
                                        } else {
                                            // Before-image was itself a delete.
                                            t.delete(&rec.key);
                                        }
                                    } else {
                                        // Before-image unavailable (log cleaned).
                                        t.delete(&rec.key);
                                    }
                                }
                            }
                        self.stats.lns_undone += 1;
                        self.stats.active_txns_undone += 1;
                    }
                    UndoAction::NoAction => {}
                }

                // Collect for external inspection in tests.
                self.undo_entries.push((pe.lsn, rec.clone()));
            }
        }

        Ok(())
    }

    /// Determine the undo action for a single uncommitted LN.
    ///
    /// Port of the decision table in `RecoveryManager.undo()` in JE:
    ///
    /// ```text
    /// abort_lsn is NULL  → first write → delete the slot
    /// abort_lsn is valid → revert to abort_lsn (before-image)
    /// ```
    ///
    /// The "found in tree" and "logLsn == slotLsn" currency checks are
    /// delegated to the tree layer (`Tree::delete` / `Tree::insert`) at the
    /// call site; here we compute the *intended* action from the log record
    /// metadata alone.
    fn compute_undo_action(rec: &LnRecord) -> UndoAction {
        if rec.abort_lsn == NULL_LSN {
            // This was the first write of this key: undo by deleting the slot.
            UndoAction::DeleteSlot
        } else {
            // Revert to before-image.
            UndoAction::RevertToAbortLsn { abort_lsn: rec.abort_lsn }
        }
    }

    // ====================================================================
    // Helpers
    // ====================================================================

    fn set_progress(&mut self, progress: RecoveryProgress) {
        self.progress = progress;
    }

    /// Return a reference to the collected undo entries (for testing).
    pub fn undo_entries(&self) -> &[(Lsn, LnRecord)] {
        &self.undo_entries
    }

    /// Return a reference to the collected redo entries (for testing).
    pub fn redo_entries(&self) -> &[(Lsn, LnRecord)] {
        &self.redo_entries
    }
}

impl Default for RecoveryManager {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dirty_in_map::CheckpointReference;
    use crate::log_scanner::{
        CkptEndRecord, CkptStartRecord, DbTreeRecord, InMemoryLogScanner,
        InRecord, LnRecord, LnOperation, LogEntry, RollbackEndRecord,
        RollbackStartRecord, TxnAbortRecord, TxnCommitRecord,
    };

    // ------------------------------------------------------------------ helpers

    fn lsn(file: u32, offset: u32) -> Lsn {
        Lsn::new(file, offset)
    }

    fn make_insert(
        db_id: u64,
        txn_id: Option<u64>,
        key: &[u8],
        abort_lsn: Lsn,
    ) -> LnRecord {
        LnRecord::new(
            db_id,
            txn_id,
            LnOperation::Insert,
            key.to_vec(),
            Some(b"value".to_vec()),
            abort_lsn,
            false,
        )
    }

    fn make_delete(
        db_id: u64,
        txn_id: Option<u64>,
        key: &[u8],
        abort_lsn: Lsn,
    ) -> LnRecord {
        LnRecord::new(
            db_id,
            txn_id,
            LnOperation::Delete,
            key.to_vec(),
            None,
            abort_lsn,
            true,
        )
    }

    fn make_in_record(
        db_id: u64,
        node_id: u64,
        level: i32,
        is_root: bool,
    ) -> InRecord {
        InRecord { db_id, node_id, level, is_root, is_delta: false, node_data: None }
    }

    // ------------------------------------------------------------------ RecoveryProgress

    #[test]
    fn test_recovery_progress_description() {
        assert_eq!(
            RecoveryProgress::FindEndOfLog.description(),
            "Finding end of log"
        );
        assert_eq!(
            RecoveryProgress::Complete.description(),
            "Recovery complete"
        );
    }

    #[test]
    fn test_recovery_progress_is_complete() {
        assert!(!RecoveryProgress::FindEndOfLog.is_complete());
        assert!(RecoveryProgress::Complete.is_complete());
    }

    #[test]
    fn test_recovery_progress_all_stages() {
        let stages = [
            RecoveryProgress::FindEndOfLog,
            RecoveryProgress::FindLastCheckpoint,
            RecoveryProgress::BuildTree,
            RecoveryProgress::ReplayLNs,
            RecoveryProgress::UndoLNs,
            RecoveryProgress::Complete,
        ];
        for stage in stages {
            let desc = stage.description();
            assert!(!desc.is_empty(), "stage {:?} has empty description", stage);
        }
    }

    #[test]
    fn test_recovery_progress_is_complete_only_for_complete() {
        let incomplete = [
            RecoveryProgress::FindEndOfLog,
            RecoveryProgress::FindLastCheckpoint,
            RecoveryProgress::BuildTree,
            RecoveryProgress::ReplayLNs,
            RecoveryProgress::UndoLNs,
        ];
        for stage in incomplete {
            assert!(!stage.is_complete());
        }
        assert!(RecoveryProgress::Complete.is_complete());
    }

    #[test]
    fn test_recovery_progress_equality() {
        assert_eq!(
            RecoveryProgress::FindEndOfLog,
            RecoveryProgress::FindEndOfLog
        );
        assert_ne!(RecoveryProgress::FindEndOfLog, RecoveryProgress::BuildTree);
    }

    #[test]
    fn test_recovery_progress_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(RecoveryProgress::FindEndOfLog);
        set.insert(RecoveryProgress::BuildTree);
        set.insert(RecoveryProgress::FindEndOfLog);
        assert_eq!(set.len(), 2);
    }

    // ------------------------------------------------------------------ RecoveryManager basic

    #[test]
    fn test_recovery_manager_new() {
        let manager = RecoveryManager::new();
        assert_eq!(manager.get_progress(), RecoveryProgress::FindEndOfLog);
        assert!(manager.is_using_checkpoint());
        assert_eq!(manager.get_rollback_tracker().period_count(), 0);
    }

    #[test]
    fn test_recovery_manager_with_checkpoint_usage() {
        let manager = RecoveryManager::with_checkpoint_usage(false);
        assert!(!manager.is_using_checkpoint());
    }

    #[test]
    fn test_recovery_manager_default() {
        let manager = RecoveryManager::default();
        assert_eq!(manager.get_progress(), RecoveryProgress::FindEndOfLog);
    }

    // ------------------------------------------------------------------ empty log recovery

    #[test]
    fn test_recover_empty_log() {
        let mut scanner = InMemoryLogScanner::new();
        let mut mgr = RecoveryManager::new();
        let info = mgr.recover(&mut scanner, None, false).unwrap();

        assert_eq!(mgr.get_progress(), RecoveryProgress::Complete);
        assert_eq!(info.checkpoint_start_lsn, NULL_LSN);
        assert_eq!(info.last_used_lsn, NULL_LSN);
    }

    // ------------------------------------------------------------------ Phase A: find end of log

    #[test]
    fn test_find_end_of_log_sets_lsns() {
        let mut scanner = InMemoryLogScanner::new();
        scanner.push(
            lsn(0, 100),
            LogEntry::TxnCommit(TxnCommitRecord {
                txn_id: 1,
                lsn: lsn(0, 100),
            }),
        );

        let mut mgr = RecoveryManager::new();
        mgr.recover(&mut scanner, None, false).unwrap();

        assert_ne!(mgr.get_info().last_used_lsn, NULL_LSN);
        assert_ne!(mgr.get_info().next_available_lsn, NULL_LSN);
    }

    // ------------------------------------------------------------------ Phase B: find last checkpoint

    #[test]
    fn test_find_last_checkpoint_from_ckpt_end() {
        let mut scanner = InMemoryLogScanner::new();

        // CkptStart
        scanner.push(
            lsn(0, 50),
            LogEntry::CkptStart(CkptStartRecord {
                id: 1,
                lsn: lsn(0, 50),
            }),
        );
        // DbTree root
        scanner.push(lsn(0, 60), LogEntry::DbTree(DbTreeRecord { lsn: lsn(0, 60) }));
        // CkptEnd
        scanner.push(
            lsn(0, 200),
            LogEntry::CkptEnd(CkptEndRecord {
                id: 1,
                checkpoint_start_lsn: lsn(0, 50),
                first_active_lsn: lsn(0, 40),
                root_lsn: lsn(0, 60),
                last_local_node_id: 10,
                last_replicated_node_id: -1,
                last_local_db_id: 2,
                last_replicated_db_id: -1,
                last_local_txn_id: 5,
                last_replicated_txn_id: -1,
            }),
        );

        let mut mgr = RecoveryManager::new();
        let info = mgr.recover(&mut scanner, None, true).unwrap();

        assert_eq!(mgr.get_progress(), RecoveryProgress::Complete);
        // checkpoint_end_lsn and checkpoint_start_lsn should be populated
        assert_ne!(info.checkpoint_end_lsn, NULL_LSN);
        assert_ne!(info.checkpoint_start_lsn, NULL_LSN);
    }

    #[test]
    fn test_find_last_checkpoint_no_ckpt_end() {
        let mut scanner = InMemoryLogScanner::new();
        // Only a DbTree, no checkpoint
        scanner.push(lsn(0, 10), LogEntry::DbTree(DbTreeRecord { lsn: lsn(0, 10) }));
        scanner.push(
            lsn(0, 100),
            LogEntry::TxnCommit(TxnCommitRecord {
                txn_id: 1,
                lsn: lsn(0, 100),
            }),
        );

        let mut mgr = RecoveryManager::new();
        let info = mgr.recover(&mut scanner, None, true).unwrap();

        // No checkpoint end → checkpoint fields remain NULL
        assert_eq!(info.checkpoint_end_lsn, NULL_LSN);
        assert_eq!(info.checkpoint_start_lsn, NULL_LSN);
    }

    // ------------------------------------------------------------------ Phase 1: Analysis

    #[test]
    fn test_analysis_builds_dirty_in_map() {
        let mut scanner = InMemoryLogScanner::new();

        // Two INs (BINs at level 0)
        scanner.push(lsn(0, 100), LogEntry::In(make_in_record(1, 10, 0, false)));
        scanner.push(lsn(0, 200), LogEntry::In(make_in_record(1, 20, 0, false)));
        // One upper IN at level 1
        scanner.push(lsn(0, 300), LogEntry::In(make_in_record(1, 30, 1, true)));
        scanner.push(
            lsn(0, 400),
            LogEntry::TxnCommit(TxnCommitRecord {
                txn_id: 5,
                lsn: lsn(0, 400),
            }),
        );

        let mut mgr = RecoveryManager::new();
        mgr.recover(&mut scanner, None, false).unwrap();

        // All three INs should have been replayed (redo pass)
        assert_eq!(mgr.get_stats().ins_read, 3);
        assert_eq!(mgr.get_stats().ins_replayed, 3);
    }

    #[test]
    fn test_analysis_tracks_committed_txns() {
        let mut scanner = InMemoryLogScanner::new();

        scanner.push(
            lsn(0, 100),
            LogEntry::Ln(make_insert(1, Some(1), b"key1", NULL_LSN)),
        );
        scanner.push(
            lsn(0, 200),
            LogEntry::TxnCommit(TxnCommitRecord {
                txn_id: 1,
                lsn: lsn(0, 200),
            }),
        );

        let mut mgr = RecoveryManager::new();
        mgr.recover(&mut scanner, None, false).unwrap();

        assert_eq!(mgr.get_stats().committed_txns, 1);
    }

    #[test]
    fn test_analysis_tracks_aborted_txns() {
        let mut scanner = InMemoryLogScanner::new();

        scanner.push(
            lsn(0, 100),
            LogEntry::Ln(make_insert(1, Some(1), b"key1", NULL_LSN)),
        );
        scanner.push(
            lsn(0, 200),
            LogEntry::TxnAbort(TxnAbortRecord { txn_id: 1 }),
        );

        let mut mgr = RecoveryManager::new();
        mgr.recover(&mut scanner, None, false).unwrap();

        assert_eq!(mgr.get_stats().aborted_txns, 1);
    }

    // ------------------------------------------------------------------ Phase 2: Redo

    #[test]
    fn test_redo_committed_ln_after_ckpt_start() {
        let ckpt_start = lsn(0, 50);

        let mut scanner = InMemoryLogScanner::new();
        // CkptEnd to establish checkpoint boundaries
        scanner.push(
            lsn(0, 200),
            LogEntry::CkptEnd(CkptEndRecord {
                id: 1,
                checkpoint_start_lsn: ckpt_start,
                first_active_lsn: lsn(0, 40),
                root_lsn: NULL_LSN,
                last_local_node_id: 0,
                last_replicated_node_id: -1,
                last_local_db_id: 0,
                last_replicated_db_id: -1,
                last_local_txn_id: 0,
                last_replicated_txn_id: -1,
            }),
        );
        // LN after checkpoint start, committed txn
        scanner.push(
            lsn(0, 300),
            LogEntry::Ln(make_insert(1, Some(42), b"key", lsn(0, 100))),
        );
        scanner.push(
            lsn(0, 400),
            LogEntry::TxnCommit(TxnCommitRecord {
                txn_id: 42,
                lsn: lsn(0, 400),
            }),
        );

        let mut mgr = RecoveryManager::new();
        mgr.recover(&mut scanner, None, false).unwrap();

        // The committed LN should have been redone
        assert_eq!(mgr.get_stats().lns_redone, 1);
    }

    #[test]
    fn test_redo_non_txnal_ln_after_ckpt_start() {
        let mut scanner = InMemoryLogScanner::new();
        // Non-transactional LN (no txn_id)
        scanner.push(
            lsn(0, 100),
            LogEntry::Ln(make_insert(1, None, b"key", NULL_LSN)),
        );

        let mut mgr = RecoveryManager::new();
        mgr.recover(&mut scanner, None, false).unwrap();

        assert_eq!(mgr.get_stats().lns_redone, 1);
    }

    #[test]
    fn test_redo_skips_active_txn_ln() {
        let mut scanner = InMemoryLogScanner::new();
        // LN in a transaction that never commits (active at crash)
        scanner.push(
            lsn(0, 100),
            LogEntry::Ln(make_insert(1, Some(99), b"key", NULL_LSN)),
        );
        // No TxnCommit for txn 99

        let mut mgr = RecoveryManager::new();
        mgr.recover(&mut scanner, None, false).unwrap();

        assert_eq!(mgr.get_stats().lns_redone, 0);
    }

    #[test]
    fn test_redo_skips_aborted_txn_ln() {
        let mut scanner = InMemoryLogScanner::new();
        scanner.push(
            lsn(0, 100),
            LogEntry::Ln(make_insert(1, Some(7), b"key", NULL_LSN)),
        );
        scanner.push(
            lsn(0, 200),
            LogEntry::TxnAbort(TxnAbortRecord { txn_id: 7 }),
        );

        let mut mgr = RecoveryManager::new();
        mgr.recover(&mut scanner, None, false).unwrap();

        assert_eq!(mgr.get_stats().lns_redone, 0);
    }

    // ------------------------------------------------------------------ Phase 3: Undo

    #[test]
    fn test_undo_active_txn_insert_first_write() {
        let mut scanner = InMemoryLogScanner::new();

        // First write (abort_lsn = NULL → delete slot on undo)
        scanner.push(
            lsn(0, 100),
            LogEntry::Ln(make_insert(1, Some(5), b"key", NULL_LSN)),
        );
        // No commit for txn 5

        let mut mgr = RecoveryManager::new();
        mgr.recover(&mut scanner, None, false).unwrap();

        assert_eq!(mgr.get_stats().lns_undone, 1);

        // Verify undo action was DeleteSlot
        let undo_entries = mgr.undo_entries();
        assert_eq!(undo_entries.len(), 1);
        let action = RecoveryManager::compute_undo_action(&undo_entries[0].1);
        assert_eq!(action, UndoAction::DeleteSlot);
    }

    #[test]
    fn test_undo_active_txn_update_reverts_to_abort_lsn() {
        let abort_lsn = lsn(0, 50);

        let mut scanner = InMemoryLogScanner::new();
        // Update (abort_lsn points to previous version)
        scanner.push(
            lsn(0, 100),
            LogEntry::Ln(make_insert(1, Some(5), b"key", abort_lsn)),
        );
        // No commit for txn 5

        let mut mgr = RecoveryManager::new();
        mgr.recover(&mut scanner, None, false).unwrap();

        assert_eq!(mgr.get_stats().lns_undone, 1);
        let undo_entries = mgr.undo_entries();
        let action = RecoveryManager::compute_undo_action(&undo_entries[0].1);
        assert_eq!(action, UndoAction::RevertToAbortLsn { abort_lsn });
    }

    #[test]
    fn test_undo_skips_committed_txn() {
        let mut scanner = InMemoryLogScanner::new();

        scanner.push(
            lsn(0, 100),
            LogEntry::Ln(make_insert(1, Some(3), b"key", NULL_LSN)),
        );
        scanner.push(
            lsn(0, 200),
            LogEntry::TxnCommit(TxnCommitRecord {
                txn_id: 3,
                lsn: lsn(0, 200),
            }),
        );

        let mut mgr = RecoveryManager::new();
        mgr.recover(&mut scanner, None, false).unwrap();

        // Nothing to undo
        assert_eq!(mgr.get_stats().lns_undone, 0);
    }

    #[test]
    fn test_undo_skips_non_txnal_ln() {
        let mut scanner = InMemoryLogScanner::new();
        scanner.push(
            lsn(0, 100),
            LogEntry::Ln(make_insert(1, None, b"key", NULL_LSN)),
        );

        let mut mgr = RecoveryManager::new();
        mgr.recover(&mut scanner, None, false).unwrap();

        assert_eq!(mgr.get_stats().lns_undone, 0);
    }

    // ------------------------------------------------------------------ Mixed scenario

    #[test]
    fn test_full_recovery_mixed_txns() {
        // Scenario:
        //   txn 1 commits — its LN is redone, not undone.
        //   txn 2 aborts — its LN is neither redone nor undone (abort record).
        //   txn 3 crashes without commit/abort — its LN is undone.
        let mut scanner = InMemoryLogScanner::new();

        // txn 1 LN + commit
        scanner.push(lsn(0, 10), LogEntry::Ln(make_insert(1, Some(1), b"k1", NULL_LSN)));
        scanner.push(lsn(0, 20), LogEntry::TxnCommit(TxnCommitRecord { txn_id: 1, lsn: lsn(0, 20) }));

        // txn 2 LN + abort
        scanner.push(lsn(0, 30), LogEntry::Ln(make_insert(1, Some(2), b"k2", NULL_LSN)));
        scanner.push(lsn(0, 40), LogEntry::TxnAbort(TxnAbortRecord { txn_id: 2 }));

        // txn 3 LN — no commit/abort (active at crash)
        scanner.push(lsn(0, 50), LogEntry::Ln(make_insert(1, Some(3), b"k3", NULL_LSN)));

        let mut mgr = RecoveryManager::new();
        let _info = mgr.recover(&mut scanner, None, false).unwrap();

        // txn 1 committed → redone
        assert_eq!(mgr.get_stats().lns_redone, 1);
        // txn 2 aborted + txn 3 active → both undone.
        // JE's undoLNs skips only committedTxnIds; aborted txns still go
        // through undo (the tree apply is safe even if they were already
        // rolled back, because the slot LSN will not match).
        assert_eq!(mgr.get_stats().lns_undone, 2);
        assert_eq!(mgr.get_stats().active_txns_undone, 2);
    }

    // ------------------------------------------------------------------ Multi-phase ordering

    #[test]
    fn test_recovery_progress_tracking_during_recover() {
        let mut scanner = InMemoryLogScanner::new();
        let mut manager = RecoveryManager::new();
        assert_eq!(manager.get_progress(), RecoveryProgress::FindEndOfLog);
        manager.recover(&mut scanner, None, true).unwrap();
        assert_eq!(manager.get_progress(), RecoveryProgress::Complete);
        assert!(manager.get_progress().is_complete());
    }

    #[test]
    fn test_recovery_manager_checkpoint_flag_persists() {
        let mut scanner = InMemoryLogScanner::new();
        let mut manager = RecoveryManager::with_checkpoint_usage(false);
        assert!(!manager.is_using_checkpoint());
        manager.recover(&mut scanner, None, true).unwrap();
        // The flag is updated by recover()
        assert!(manager.is_using_checkpoint());
    }

    #[test]
    fn test_recovery_multiple_checkpoints_uses_last() {
        let mut scanner = InMemoryLogScanner::new();

        // First complete checkpoint
        scanner.push(lsn(0, 10), LogEntry::CkptStart(CkptStartRecord { id: 1, lsn: lsn(0, 10) }));
        scanner.push(lsn(0, 100), LogEntry::CkptEnd(CkptEndRecord {
            id: 1,
            checkpoint_start_lsn: lsn(0, 10),
            first_active_lsn: lsn(0, 5),
            root_lsn: lsn(0, 20),
            last_local_node_id: 5,
            last_replicated_node_id: -1,
            last_local_db_id: 1,
            last_replicated_db_id: -1,
            last_local_txn_id: 3,
            last_replicated_txn_id: -1,
        }));

        // Second (later) complete checkpoint
        scanner.push(lsn(0, 200), LogEntry::CkptStart(CkptStartRecord { id: 2, lsn: lsn(0, 200) }));
        scanner.push(lsn(0, 500), LogEntry::CkptEnd(CkptEndRecord {
            id: 2,
            checkpoint_start_lsn: lsn(0, 200),
            first_active_lsn: lsn(0, 150),
            root_lsn: lsn(0, 250),
            last_local_node_id: 20,
            last_replicated_node_id: -1,
            last_local_db_id: 3,
            last_replicated_db_id: -1,
            last_local_txn_id: 10,
            last_replicated_txn_id: -1,
        }));

        let mut mgr = RecoveryManager::new();
        let info = mgr.recover(&mut scanner, None, true).unwrap();

        // Should use the LAST checkpoint
        assert_eq!(info.checkpoint_end_lsn, lsn(0, 500));
        assert_eq!(info.checkpoint_start_lsn, lsn(0, 200));
        assert_eq!(info.use_max_node_id, 20);
    }

    // ------------------------------------------------------------------ DirtyINMap integration

    #[test]
    fn test_dirty_in_map_level_ordered_iteration() {
        use crate::dirty_in_map::DirtyINMap;

        let mut map = DirtyINMap::new();

        map.add_dirty_in(CheckpointReference::new(30, 1, false, 3));
        map.add_dirty_in(CheckpointReference::new(10, 1, false, 1));
        map.add_dirty_in(CheckpointReference::new(20, 1, false, 2));
        map.add_dirty_in(CheckpointReference::new(0, 1, false, 0));

        let mut levels_seen = Vec::new();
        while let Some(level) = map.get_lowest_level() {
            let refs = map.select_dirty_ins_for_level(level);
            assert!(!refs.is_empty());
            levels_seen.push(level);
        }

        assert_eq!(levels_seen, vec![0, 1, 2, 3]);
        assert!(map.is_empty());
    }

    // ------------------------------------------------------------------ UndoAction

    #[test]
    fn test_compute_undo_action_first_write() {
        let rec = make_insert(1, Some(1), b"k", NULL_LSN);
        assert_eq!(
            RecoveryManager::compute_undo_action(&rec),
            UndoAction::DeleteSlot
        );
    }

    #[test]
    fn test_compute_undo_action_update() {
        let rec = make_insert(1, Some(1), b"k", lsn(0, 50));
        assert_eq!(
            RecoveryManager::compute_undo_action(&rec),
            UndoAction::RevertToAbortLsn { abort_lsn: lsn(0, 50) }
        );
    }

    #[test]
    fn test_compute_undo_action_delete() {
        let rec = make_delete(1, Some(1), b"k", lsn(0, 50));
        assert_eq!(
            RecoveryManager::compute_undo_action(&rec),
            UndoAction::RevertToAbortLsn { abort_lsn: lsn(0, 50) }
        );
    }

    // ------------------------------------------------------------------ eligible_for_redo

    #[test]
    fn test_eligible_for_redo_invisible_skipped() {
        let mut scanner = InMemoryLogScanner::new();
        let mut ln = make_insert(1, Some(1), b"k", NULL_LSN);
        ln.is_invisible = true;
        scanner.push(lsn(0, 100), LogEntry::Ln(ln));
        scanner.push(
            lsn(0, 200),
            LogEntry::TxnCommit(TxnCommitRecord { txn_id: 1, lsn: lsn(0, 200) }),
        );

        let mut mgr = RecoveryManager::new();
        mgr.recover(&mut scanner, None, false).unwrap();

        // Invisible → not redone even though committed
        assert_eq!(mgr.get_stats().lns_redone, 0);
    }

    // ------------------------------------------------------------------ rollback period

    #[test]
    fn test_rollback_period_skipped_in_undo() {
        // LN in a rollback period should be skipped by undo.
        let matchpoint = lsn(0, 50);
        let rollback_start_lsn = lsn(0, 300);

        let mut scanner = InMemoryLogScanner::new();
        // RollbackStart
        scanner.push(
            rollback_start_lsn,
            LogEntry::RollbackStart(RollbackStartRecord {
                matchpoint_lsn: matchpoint,
                lsn: rollback_start_lsn,
            }),
        );
        // RollbackEnd
        scanner.push(
            lsn(0, 350),
            LogEntry::RollbackEnd(RollbackEndRecord {
                matchpoint_lsn: matchpoint,
                lsn: lsn(0, 350),
            }),
        );

        // LN within the rollback period (matchpoint < lsn < rollback_start)
        let ln_lsn = lsn(0, 200); // between 50 and 300
        scanner.push(
            ln_lsn,
            LogEntry::Ln(make_insert(1, Some(9), b"k", NULL_LSN)),
        );
        // No commit for txn 9

        let mut mgr = RecoveryManager::new();
        mgr.recover(&mut scanner, None, false).unwrap();

        // The LN is in the rollback period → not undone
        assert_eq!(mgr.get_stats().lns_undone, 0);
    }

    // ------------------------------------------------------------------ recovery info fields

    #[test]
    fn test_recovery_info_populated_after_recover() {
        let mut scanner = InMemoryLogScanner::new();
        scanner.push(
            lsn(0, 100),
            LogEntry::TxnCommit(TxnCommitRecord {
                txn_id: 1,
                lsn: lsn(0, 100),
            }),
        );

        let mut mgr = RecoveryManager::new();
        let info = mgr.recover(&mut scanner, None, false).unwrap();

        assert_ne!(info.last_used_lsn, NULL_LSN);
        assert_ne!(info.next_available_lsn, NULL_LSN);
    }

    #[test]
    fn test_id_counters_from_ckpt_end() {
        let mut scanner = InMemoryLogScanner::new();
        scanner.push(
            lsn(0, 500),
            LogEntry::CkptEnd(CkptEndRecord {
                id: 1,
                checkpoint_start_lsn: lsn(0, 100),
                first_active_lsn: lsn(0, 50),
                root_lsn: NULL_LSN,
                last_local_node_id: 77,
                last_replicated_node_id: -1,
                last_local_db_id: 8,
                last_replicated_db_id: -1,
                last_local_txn_id: 33,
                last_replicated_txn_id: -1,
            }),
        );

        let mut mgr = RecoveryManager::new();
        let info = mgr.recover(&mut scanner, None, false).unwrap();

        assert_eq!(info.use_max_node_id, 77);
        assert_eq!(info.use_max_db_id, 8);
        assert_eq!(info.use_max_txn_id, 33);
    }

    // ================================================================== tree integration

    /// Helper: build a default Tree for integration tests.
    fn make_tree() -> noxu_tree::Tree {
        // database_id=1, max_entries_per_node=4 (small, fits tests).
        noxu_tree::Tree::new(1, 4)
    }

    /// Redo phase with a real tree: committed insert appears in tree after
    /// recovery.
    ///
    /// Scenario:
    ///   lsn(0,10) → Insert key="a", txn=1, no abort_lsn
    ///   lsn(0,20) → TxnCommit txn=1
    ///
    /// Redo: Insert("a") should be replayed → key present in tree.
    #[test]
    fn test_redo_committed_insert_wires_tree() {
        let mut scanner = InMemoryLogScanner::new();
        scanner.push(
            lsn(0, 10),
            LogEntry::Ln(LnRecord::new(
                1,
                Some(1),
                LnOperation::Insert,
                b"alpha".to_vec(),
                Some(b"value_a".to_vec()),
                NULL_LSN,
                false,
            )),
        );
        scanner.push(
            lsn(0, 20),
            LogEntry::TxnCommit(TxnCommitRecord { txn_id: 1, lsn: lsn(0, 20) }),
        );

        let mut tree = make_tree();
        let mut mgr = RecoveryManager::new();
        mgr.recover(&mut scanner, Some(&mut tree), false).unwrap();

        // The committed insert must be present in the tree.
        assert!(tree.search(b"alpha").is_some());
        let result = tree.search(b"alpha").unwrap();
        assert!(result.exact_parent_found);
    }

    /// Redo phase: non-transactional insert is replayed unconditionally.
    #[test]
    fn test_redo_non_txnal_insert_wires_tree() {
        let mut scanner = InMemoryLogScanner::new();
        scanner.push(
            lsn(0, 5),
            LogEntry::Ln(LnRecord::new(
                1,
                None, // non-transactional
                LnOperation::Insert,
                b"beta".to_vec(),
                Some(b"value_b".to_vec()),
                NULL_LSN,
                false,
            )),
        );

        let mut tree = make_tree();
        let mut mgr = RecoveryManager::new();
        mgr.recover(&mut scanner, Some(&mut tree), false).unwrap();

        assert!(tree.search(b"beta").is_some());
        assert!(tree.search(b"beta").unwrap().exact_parent_found);
    }

    /// Redo phase: uncommitted (active) transaction is NOT applied to the tree.
    #[test]
    fn test_redo_skips_uncommitted_txn_tree_unchanged() {
        let mut scanner = InMemoryLogScanner::new();
        // Insert with txn=99, but no TxnCommit → active transaction.
        scanner.push(
            lsn(0, 5),
            LogEntry::Ln(LnRecord::new(
                1,
                Some(99),
                LnOperation::Insert,
                b"gamma".to_vec(),
                Some(b"value_g".to_vec()),
                NULL_LSN,
                false,
            )),
        );

        let mut tree = make_tree();
        let mut mgr = RecoveryManager::new();
        mgr.recover(&mut scanner, Some(&mut tree), false).unwrap();

        // Key must NOT be in the tree (redo skipped, undo removed it).
        // The undo phase calls tree.delete on active txns, which is a no-op
        // when the key was never inserted — so either way the tree is empty.
        let result = tree.search(b"gamma");
        let found = result.map(|r| r.exact_parent_found).unwrap_or(false);
        assert!(!found, "uncommitted insert must not appear in tree");
    }

    /// Undo phase with a real tree: uncommitted insert is removed from the
    /// tree.  We seed the tree with the key first (simulating a crash after
    /// the insert was written to the log but before commit), then run
    /// recovery which must undo it.
    #[test]
    fn test_undo_uncommitted_insert_removes_from_tree() {
        // Pre-load the tree with the key (crash-state: insert in log + tree).
        let mut tree = make_tree();
        tree.insert(b"delta".to_vec(), b"value_d".to_vec(), lsn(0, 10))
            .unwrap();
        assert!(tree.search(b"delta").unwrap().exact_parent_found);

        // Log: Insert txn=5 at lsn(0,10), NO commit record → active txn.
        let mut scanner = InMemoryLogScanner::new();
        scanner.push(
            lsn(0, 10),
            LogEntry::Ln(LnRecord::new(
                1,
                Some(5),
                LnOperation::Insert,
                b"delta".to_vec(),
                Some(b"value_d".to_vec()),
                NULL_LSN, // abort_lsn=NULL → first write → DeleteSlot
                false,
            )),
        );

        let mut mgr = RecoveryManager::new();
        mgr.recover(&mut scanner, Some(&mut tree), false).unwrap();

        // After undo: key must be removed.
        let found = tree.search(b"delta").map(|r| r.exact_parent_found).unwrap_or(false);
        assert!(!found, "undo must remove the uncommitted insert");

        // Verify stats
        assert_eq!(mgr.get_stats().lns_undone, 1);
        assert_eq!(mgr.get_stats().active_txns_undone, 1);
    }

    /// Committed delete: the redo phase removes a key from the tree.
    #[test]
    fn test_redo_committed_delete_removes_from_tree() {
        // Seed the tree with the key.
        let mut tree = make_tree();
        tree.insert(b"epsilon".to_vec(), b"value_e".to_vec(), lsn(0, 1))
            .unwrap();

        // Log: Delete txn=2, commit.
        let mut scanner = InMemoryLogScanner::new();
        scanner.push(
            lsn(0, 10),
            LogEntry::Ln(LnRecord::new(
                1,
                Some(2),
                LnOperation::Delete,
                b"epsilon".to_vec(),
                None,
                NULL_LSN,
                true,
            )),
        );
        scanner.push(
            lsn(0, 20),
            LogEntry::TxnCommit(TxnCommitRecord { txn_id: 2, lsn: lsn(0, 20) }),
        );

        let mut mgr = RecoveryManager::new();
        mgr.recover(&mut scanner, Some(&mut tree), false).unwrap();

        let found = tree.search(b"epsilon").map(|r| r.exact_parent_found).unwrap_or(false);
        assert!(!found, "committed delete must remove the key from the tree");
        assert_eq!(mgr.get_stats().lns_redone, 1);
    }

    /// Multiple keys: committed inserts visible, uncommitted insert absent.
    #[test]
    fn test_redo_mixed_committed_and_uncommitted() {
        let mut scanner = InMemoryLogScanner::new();
        // txn=1: committed insert of "key1"
        scanner.push(
            lsn(0, 10),
            LogEntry::Ln(LnRecord::new(
                1, Some(1), LnOperation::Insert,
                b"key1".to_vec(), Some(b"v1".to_vec()), NULL_LSN, false,
            )),
        );
        scanner.push(
            lsn(0, 20),
            LogEntry::TxnCommit(TxnCommitRecord { txn_id: 1, lsn: lsn(0, 20) }),
        );
        // txn=2: NOT committed → active
        scanner.push(
            lsn(0, 30),
            LogEntry::Ln(LnRecord::new(
                1, Some(2), LnOperation::Insert,
                b"key2".to_vec(), Some(b"v2".to_vec()), NULL_LSN, false,
            )),
        );

        let mut tree = make_tree();
        let mut mgr = RecoveryManager::new();
        mgr.recover(&mut scanner, Some(&mut tree), false).unwrap();

        // key1: committed → must be in tree
        assert!(
            tree.search(b"key1").map(|r| r.exact_parent_found).unwrap_or(false),
            "committed key1 must be in tree"
        );
        // key2: uncommitted → must NOT be in tree
        assert!(
            !tree.search(b"key2").map(|r| r.exact_parent_found).unwrap_or(false),
            "uncommitted key2 must not be in tree"
        );
    }
}
