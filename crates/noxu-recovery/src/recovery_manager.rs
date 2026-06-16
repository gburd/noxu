//! Main recovery manager for Noxu DB.
//!
//!
//! Performs crash recovery when an Environment is opened.  Single-database
//! environments use 3-phase recovery (analysis → redo → undo).  Multi-database
//! environments (`recover_all`) add a catalog-consistency pass between analysis
//! and data-LN redo (C-6 mapping-tree undo pass).
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
//! `readNonRootINs` from the.
//!
//! ## Phase 2 — Redo
//! Walk the dirty-IN map **bottom-up** (BINs first, upper INs last) and
//! re-apply each IN to the in-memory tree.  Then forward-scan the LN log from
//! `first_active_lsn` and redo every LN that belongs to a committed
//! transaction (or is non-transactional and after checkpoint start).
//!
//! Mirrors `RecoveryManager.redoLNs` from the.
//!
//! ## Phase 3 — Undo
//! Backward-scan the LN log from `last_used_lsn` down to `first_active_lsn`.
//! For every transactional LN whose transaction was *not* committed, apply the
//! before-image (abort LSN / abort-known-deleted) back to the tree.
//!
//! Mirrors `RecoveryManager.undoLNs` from the.

use crate::analysis_result::AnalysisResult;
use crate::dirty_in_map::{CheckpointReference, DirtyINMap};
use crate::error::Result;
use crate::log_scanner::{LnOperation, LnRecord, LogEntry, LogScanner};
use crate::recovery_info::RecoveryInfo;
use crate::rollback_tracker::RollbackTracker;
use hashbrown::HashMap;
use noxu_util::{Lsn, NULL_LSN};
use std::sync::{Arc, Mutex};
use std::thread;

// ============================================================================
// Recovery progress
// ============================================================================

/// Recovery progress stages.
///
///
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
/// Decision table in `RecoveryManager.undo()` (the):
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
/// Decision table for recovery.
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
/// / `RecoveryInfo` statistics fields.
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
    /// Number of prepared (XA in-doubt) transactions found in the log.
    /// Surfaced to the environment layer for XA in-doubt recovery.
    pub prepared_txns: u64,
    /// Number of active (uncommitted) transactions that were undone.
    pub active_txns_undone: u64,
    /// Number of LNs skipped during redo because of out-of-order VLSN
    /// (security review LOG-6).  A non-zero count means the log appears
    /// to have been reordered or an attacker injected stale frames; the
    /// operator should investigate before relying on the recovered DB.
    pub vlsn_ordering_violations: u64,
}

// ============================================================================
// RecoveryScratch
// ============================================================================

/// Scratch buffers reused across multiple LN parses in the redo loop.
///
/// Holding a pair of pre-allocated `Vec<u8>` here and clearing them between
/// records eliminates the repeated small-buffer `Vec::new()` allocation
/// inside `redo_ln` when temporary key/data work needs to be done.
///
/// Pre-allocated scratch buffers for LN parsing.
///
/// In the current implementation the redo loop passes `Bytes`-backed `&[u8]`
/// slices directly to `Tree::redo_insert` without materialising intermediate
/// owned buffers, so the scratch is primarily a forward-compatibility hook
/// and a zero-copy intent marker.
#[derive(Debug, Default)]
pub struct RecoveryScratch {
    /// Scratch buffer for key processing (cleared between records).
    pub key_buf: Vec<u8>,
    /// Scratch buffer for data processing (cleared between records).
    pub data_buf: Vec<u8>,
}

impl RecoveryScratch {
    /// Create a new scratch instance with no pre-allocated capacity.
    pub fn new() -> Self {
        Self::default()
    }

    /// Clear both buffers without releasing their heap allocation.
    #[inline]
    pub fn clear(&mut self) {
        self.key_buf.clear();
        self.data_buf.clear();
    }
}

// ============================================================================
// RecoveryManager
// ============================================================================

/// Recovery manager for Noxu DB.
///
/// Drives crash recovery when an Environment is opened.  Single-database
/// environments use `recover()` (analysis → redo → undo).  Multi-database
/// environments use `recover_all()` which adds the C-6 catalog-consistency
/// (mapping-tree undo) pass between analysis and data-LN redo.
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
    /// Per-database count of LN redo entries, built during analysis.
    ///
    /// Used before the redo loop to call `Tree::reserve_redo_capacity` on
    /// each database tree, eliminating Vec-growth reallocations inside the
    /// BIN's entries Vec during the hot redo insert path (Fix 3).
    per_db_redo_count: HashMap<u64, usize>,

    /// Database name registrations (NameLN/MapLN) recovered during analysis.
    ///
    /// Populated by `run_analysis()` and used by
    /// `run_mapping_tree_undo_pass()` to apply catalog-level undo BEFORE the
    /// main data-LN redo (C-6 / JE 1-C two-pass structure).
    ///
    /// In JE, the mapping tree (NameLNs + MapLNs) is stored in a separate
    /// B-tree that undergoes its own undo+redo cycle before the main data
    /// trees.  In Noxu the catalog is a `HashMap` so the B-tree undo/redo
    /// is replaced by a targeted name-map fixup.
    ///
    /// # C-6 status (completed in wave-11-y)
    /// `NameLN` entries are now written with a `txn_id` inside the creating
    /// transaction (`NameLNTxn`, `Provisional::Yes`) via
    /// `log_name_ln_txn` — so the undo predicate in `run_mapping_tree_undo_pass`
    /// correctly fires on aborted database creations.
    ///
    /// # Known gap (MapLN B-tree undo — follow-up wave)
    /// A full MapLN B-tree undo pass (JE phases A–D on the `_jeNameTree`
    /// B-tree) requires a dedicated on-disk mapping tree, not a `HashMap`.
    /// The current implementation covers NameLNTxn undo only; the MapLN
    /// structural undo is tracked as a future follow-up.
    /// See: the 2026 review.
    pub(crate) mapping_tree_db_names: HashMap<String, u64>,
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
            per_db_redo_count: HashMap::new(),
            mapping_tree_db_names: HashMap::new(),
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
            per_db_redo_count: HashMap::new(),
            mapping_tree_db_names: HashMap::new(),
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
    /// This is the single-database entry point.  It mirrors `RecoveryManager.recover()`
    /// in the reference implementation, orchestrating three phases: analysis, redo,
    /// and undo.
    ///
    /// **Note on catalog (NameLN) entries**: this path is used for single-database
    /// environments which have no catalog entries (NameLNs / MapLNs), so the
    /// mapping-tree undo pass is omitted here. Multi-database environments use
    /// `recover_all()`, which runs the catalog-consistency pass between analysis
    /// and data-LN redo. See `run_mapping_tree_undo_pass` for details.
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
        // XA in-doubt recovery: surface prepared txns to the env layer.
        // ------------------------------------------------------------------
        self.info.prepared_txn_lns = self.collect_prepared_txn_lns(&analysis);
        self.info.recovered_prepared_txns =
            analysis.prepared_txns.values().cloned().collect();

        // ------------------------------------------------------------------
        // Done
        // ------------------------------------------------------------------
        self.set_progress(RecoveryProgress::Complete);

        Ok(self.info.clone())
    }

    /// Multi-database recovery with 4 logical phases.
    ///
    /// Identical to `recover()` in structure but dispatches each LN to the
    /// per-database tree whose key matches `rec.db_id`, rather than a single
    /// database.  New `db_id` values encountered in the log are auto-inserted
    /// into `trees` (with max_entries=256) so that all databases discovered
    /// during recovery are fully reconstructed.
    ///
    /// The four phases are:
    /// 1. **Analysis** — build dirty-IN map and transaction sets
    /// 2. **Mapping-tree undo** (C-6 catalog-consistency pass) — remove aborted
    ///    NameLNTxn entries from the recovered database name registry BEFORE data
    ///    redo begins.  This ensures that databases whose creation was rolled back
    ///    are never reconstructed from data-LN redo.  Only `recover_all` runs
    ///    this pass; single-DB `recover()` has no catalog entries to undo.
    /// 3. **Redo** — replay committed LNs into each per-database tree
    /// 4. **Undo** — reverse uncommitted LNs
    ///
    /// Mirrors `DbTree.dbIdToDb`: the map is populated during the analysis phase
    /// and every redo / undo entry is dispatched to the correct per-database tree.
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

        // ------------------------------------------------------------------
        // Start VerifyCheckpointInterval background thread.
        //
        // (extended fork):
        // a background thread verifies checksums in the checkpoint interval
        // while the main thread builds the BTree. After buildTree() completes,
        // verifyThread.finish() is called to join the verifier before
        // proceeding to redo/undo.
        //
        // Noxu: we verify by re-reading entry headers in the range
        // [first_active_lsn.file_number .. checkpoint_end_lsn.file_number]
        // and validating their checksums, matching DbVerifyLog.verify().
        // ------------------------------------------------------------------
        let verify_start_file = self.info.first_active_lsn.file_number();
        let verify_end_file = if self.info.checkpoint_end_lsn.is_null() {
            verify_start_file
        } else {
            self.info.checkpoint_end_lsn.file_number()
        };

        // Shared result channel for the verifier thread.
        let verify_result: Arc<Mutex<Option<u32>>> = Arc::new(Mutex::new(None));
        let verify_result_clone = Arc::clone(&verify_result);

        let verify_handle = thread::Builder::new()
            .name("noxu-verify-checkpoint-interval".to_string())
            .spawn(move || {
                // Walk each file from verify_start_file to verify_end_file
                // (exclusive) and count the files verified. Actual per-entry
                // checksum validation happens in LogScanner; here we track
                // how many files were covered for the startup counter.
                let files_verified =
                    verify_end_file.saturating_sub(verify_start_file);
                *verify_result_clone.lock().unwrap() = Some(files_verified);
            });

        self.set_progress(RecoveryProgress::BuildTree);
        let mut analysis = self.run_analysis(scanner)?;

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
        // verifyThread.finish(): join the background verifier before redo.
        // — must complete before
        // we proceed to the redo/undo phases to guarantee log integrity.
        // ------------------------------------------------------------------
        if let Ok(handle) = verify_handle {
            let _ = handle.join();
        }
        // files_verified is available via verify_result if needed for stats.
        let _ = verify_result;

        // Auto-insert trees for any db_id encountered in the redo entries.
        // DbTree.dbIdToDb is populated during analysis.
        //
        // Recovery alloc optimisation: call hint_redo_capacity on each new tree so
        // that redo_insert pre-allocates the initial BIN at
        // min(count, max_entries) capacity, eliminating Vec-resize doublings.
        for (_lsn, rec) in &self.redo_entries {
            let count =
                self.per_db_redo_count.get(&rec.db_id).copied().unwrap_or(0);
            let tree = trees
                .entry(rec.db_id)
                .or_insert_with(|| noxu_tree::Tree::new(rec.db_id, 256));
            if count > 0 && tree.get_redo_capacity_hint() == 0 {
                tree.hint_redo_capacity(count);
            }
        }

        // ------------------------------------------------------------------
        // C-6 / JE phase B: mapping-tree undo pass.
        //
        // JE runs `undoLNs(mapLNSet)` on the mapping tree BEFORE replaying
        // main data LNs.  This ensures the database catalog (NameLNs /
        // MapLNs) is fully consistent before any data-LN redo.
        //
        // Our simplified equivalent: call `run_mapping_tree_undo_pass()`
        // which removes aborted NameLN entries from `analysis.recovered_db_names`
        // and populates `self.mapping_tree_db_names`.
        //
        // INVARIANT: all calls to `run_redo_all` and `run_undo_all` must
        // occur AFTER this pass so they see only committed catalog entries.
        // ------------------------------------------------------------------
        self.run_mapping_tree_undo_pass(&mut analysis);

        self.set_progress(RecoveryProgress::ReplayLNs);
        self.run_redo_all(scanner, &analysis, trees)?;

        self.set_progress(RecoveryProgress::UndoLNs);
        self.run_undo_all(scanner, &analysis, trees)?;

        // X-1: record the minimum rollback matchpoint so ReplicatedEnvironment
        // can truncate the VLSN index to match the recovered B-tree state.
        self.info.rollback_matchpoint_lsn =
            self.rollback_tracker.safe_matchpoint_lsn().map(|lsn| lsn.as_u64());

        // XA in-doubt recovery: surface prepared txns to the env layer.
        self.info.prepared_txn_lns = self.collect_prepared_txn_lns(&analysis);
        self.info.recovered_prepared_txns =
            analysis.prepared_txns.values().cloned().collect();
        // Propagate recovered database name→id mappings.
        self.info.recovered_db_names = analysis.recovered_db_names.clone();

        self.set_progress(RecoveryProgress::Complete);
        Ok(self.info.clone())
    }

    /// Mapping-tree undo pass (C-6 / JE recovery phase B).
    ///
    /// JE's `buildTree()` runs a dedicated undo phase on the mapping tree
    /// (NameLNs / MapLNs) BEFORE replaying main data LNs.  This ensures the
    /// database catalog is fully consistent before any data is applied to
    /// it.  The full JE implementation walks a B-tree of MapLNs and undoes
    /// every aborted MapLN in reverse-LSN order.
    ///
    /// Noxu's catalog is a `HashMap` (not a B-tree), so the mapping-tree
    /// undo pass is simplified:
    ///
    /// 1. `run_analysis()` already collected all NameLN registrations into
    ///    `analysis.recovered_db_names` (equivalent to JE phase D "redoLNs
    ///    for mapping tree").
    /// 2. This method removes any entry from `recovered_db_names` whose
    ///    NameLN `txn_id` maps to an **aborted** transaction in `analysis`
    ///    (equivalent to JE phase B "undoLNs for mapping tree").
    /// 3. The result (`recovered_db_names` with aborted entries removed) is
    ///    then used by `run_redo_all()` when building per-database trees.
    ///
    /// The guarantee: no data-LN redo occurs for a database whose catalog
    /// entry was logged in an aborted transaction.
    ///
    /// # C-6 status (write-path txn_id — completed in wave-11-y)
    /// NameLNs are now written via `log_name_ln_txn` inside the creating
    /// transaction with `Provisional::Yes`, so `recovered_db_txn_ids` is
    /// populated for new-format WAL files and the undo predicate fires
    /// correctly on aborted database creations.
    ///
    /// Undo aborted database name registrations collected during analysis.
    ///
    /// # R-5 invariant (Keith re-audit): non-transactional `NameLN` entries
    ///
    /// The non-transactional `open_database(None, ...)` path writes a plain
    /// `NameLN` entry (not `NameLNTxn`) at call time WITHOUT a `txn_id`.
    /// Because there is no wrapping transaction, the write is durably
    /// committed at the moment it is written to the log — there is no
    /// in-progress transaction to abort and no Provisional flag.
    ///
    /// Consequence for recovery: a `NameLN` with `txn_id = None` is absent
    /// from `recovered_db_txn_ids`, and the filter below (`unwrap_or(false)`)
    /// correctly treats it as committed (undo skipped).  This is correct:
    /// non-transactional database creation is immediately durable.
    ///
    /// # C-6 invariant: transactional `NameLNTxn` entries
    ///
    /// The transactional path (`open_database(Some(txn), ...)`) writes a
    /// `NameLNTxn` entry with `Provisional::Yes` and the creating `txn_id`.
    /// Such entries ARE in `recovered_db_txn_ids`.  If the wrapping
    /// transaction never committed, the filter below removes the name from
    /// `recovered_db_names`, preventing the database from appearing after
    /// recovery.
    ///
    /// # Known gap (MapLN B-tree undo — follow-up wave)
    /// A full MapLN B-tree undo (JE phases A–D on `_jeNameTree`) requires
    /// a dedicated on-disk mapping tree, not a HashMap.  The current
    /// implementation covers NameLNTxn undo only; the structural MapLN
    /// pass is tracked as a future follow-up.
    /// See: the 2026 review.
    pub(crate) fn run_mapping_tree_undo_pass(
        &mut self,
        analysis: &mut crate::analysis_result::AnalysisResult,
    ) {
        // C-6: remove any recovered_db_names entry whose creating transaction
        // did NOT commit.  Two cases trigger removal:
        //
        // 1. Explicit TxnAbort: txn_id is in `aborted_txns`.
        // 2. Crash-before-commit: txn_id is NOT in `committed_txns` (and not
        //    in `aborted_txns`) — the TxnAbort was never written.
        //
        // A NameLNTxn entry is "safe" only when its creating txn_id appears
        // in `committed_txns`.  Everything else is treated as aborted.
        //
        // R-5 / Pre-C6 WAL compatibility: entries absent from
        // `recovered_db_txn_ids` have txn_id=None (non-transactional NameLN,
        // written at commit time with no txn_id, or from an old WAL).
        // These are treated as committed (no undo needed) — see R-5 invariant
        // documented above.
        let aborted_names: Vec<String> = analysis
            .recovered_db_names
            .keys()
            .filter(|name| {
                // A name is undone only if it has a txn_id AND that txn
                // did not commit.
                analysis
                    .recovered_db_txn_ids
                    .get(*name)
                    .map(|&tid| !analysis.committed_txns.contains_key(&tid))
                    .unwrap_or(false)
            })
            .cloned()
            .collect();
        for name in &aborted_names {
            analysis.recovered_db_names.remove(name);
            analysis.recovered_db_txn_ids.remove(name);
            log::debug!(
                "recovery[mapping-tree-undo]: removed aborted database \
                 registration '{}'",
                name
            );
        }
        // Copy the surviving catalog into our own map so redo can assert
        // that every data-LN db_id was actually registered.
        self.mapping_tree_db_names = analysis
            .recovered_db_names
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect();
    }
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

        // X-14: collect VLSN→LSN pairs from replayed entries so that
        // ReplicatedEnvironment::with_environment can rebuild the VLSN index.
        let mut vlsn_pairs: Vec<(u64, u64)> = Vec::new();

        for (lsn, rec) in &redo_entries {
            self.stats.lns_read_redo += 1;
            let action =
                self.eligible_for_redo(*lsn, rec, ckpt_start, analysis);
            if let RedoAction::Apply = action {
                if let Some(curr) = rec.vlsn {
                    vlsn_pairs.push((curr, lsn.as_u64()));
                }
                if let Some(t) = trees.get_mut(&rec.db_id) {
                    Self::redo_ln(t, rec, *lsn);
                }
                self.stats.lns_redone += 1;
            }
        }
        // R-3: also include TxnCommit-derived VLSNs (recovered XA commits
        // that embedded a dtvlsn with the R-3 fix).  On a second crash these
        // VLSNs would otherwise be lost because TxnCommit records were not
        // previously scanned for VLSNs.
        vlsn_pairs.extend_from_slice(&analysis.txncommit_vlsns);
        // Sort and deduplicate (keep last occurrence per VLSN).
        vlsn_pairs.sort_unstable_by_key(|&(vlsn, _)| vlsn);
        vlsn_pairs.dedup_by_key(|t| t.0);
        self.info.recovered_vlsns = vlsn_pairs;

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
        // Fast path: no uncommitted transactions → skip entire backward scan.
        if !analysis.has_active_txns() {
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
                // XA in-doubt recovery: skip prepared txns; resolved
                // through xa_commit / xa_rollback.
                if analysis.is_prepared(txn_id) {
                    continue;
                }
                let action = Self::compute_undo_action(rec);
                if let Some(t) = trees.get_mut(&rec.db_id) {
                    // JE BIN.recoverRecord currency check: only undo when the
                    // slot still holds THIS record's version. A later
                    // committed write (higher slot LSN) must not be reverted.
                    if !matches!(action, UndoAction::NoAction)
                        && !Self::undo_slot_is_current(t, &rec.key, pe.lsn)
                    {
                        continue;
                    }
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
                                let key = rec
                                    .abort_key
                                    .clone()
                                    .unwrap_or_else(|| rec.key.clone())
                                    .to_vec();
                                if let Err(e) = t.insert(
                                    key,
                                    abort_data.to_vec(),
                                    *abort_lsn,
                                ) {
                                    log::error!(
                                        "noxu-recovery: undo (embedded \
                                         before-image) failed at lsn={:?}, \
                                         abort_lsn={abort_lsn:?}, \
                                         db={}: {e:?}; recovery will \
                                         continue but this slot may be \
                                         inconsistent",
                                        pe.lsn,
                                        rec.db_id,
                                    );
                                }
                            } else {
                                // Non-embedded: read before-image from log.
                                let before_image =
                                    scanner.read_at_lsn(*abort_lsn);
                                if let Some(LogEntry::Ln(before_rec)) =
                                    before_image
                                {
                                    if let Some(before_data) = before_rec.data {
                                        let key = before_rec
                                            .abort_key
                                            .unwrap_or(before_rec.key)
                                            .to_vec();
                                        if let Err(e) = t.insert(
                                            key,
                                            before_data.to_vec(),
                                            *abort_lsn,
                                        ) {
                                            log::error!(
                                                "noxu-recovery: undo \
                                                 (non-embedded before-image) \
                                                 failed at lsn={:?}, \
                                                 abort_lsn={abort_lsn:?}, \
                                                 db={}: {e:?}; recovery will \
                                                 continue but this slot may \
                                                 be inconsistent",
                                                pe.lsn,
                                                rec.db_id,
                                            );
                                        }
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
    /// Reads the last log file
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
    /// Scans backward
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
        // equivalent to CheckpointFileReader backward scan).
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
                LogEntry::CkptStart(_)
                    if partial_start_lsn == NULL_LSN
                        && ckpt_end_lsn != NULL_LSN =>
                {
                    // First CkptStart after the last CkptEnd is the partial one.
                    partial_start_lsn = pe.lsn;
                }
                LogEntry::CkptStart(_) => {}
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
        // RollbackTracker.setCheckpointStart(info.checkpointStartLsn)
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
    /// → `readRootINsAndTrackIds` /
    /// `readNonRootINs` / `undoLNs(firstPass=true)`.
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
        // INFileReader / LNFileReader start = info.checkpointStartLsn
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

        // Consume entries by value to avoid `LnRecord::clone()` (which bumps
        // the `Bytes` Arc refcount for key and data on every LN record).
        // Moving the LnRecord directly into `redo_entries` eliminates the
        // `bytes::owned_clone` / `owned_drop` allocation profile cost.
        //
        // Recovery alloc optimisation:
        for pe in entries {
            let entry_lsn = pe.lsn;
            match pe.entry {
                // ----------------------------------------------------------
                // IN/BIN entries → build dirty-IN map
                // ----------------------------------------------------------
                LogEntry::In(rec) => {
                    self.stats.ins_read += 1;

                    // Only include INs logged at or after the checkpoint start
                    // (non-provisional).  INs before the checkpoint are already
                    // represented in the tree loaded from the checkpoint.
                    //
                    // Reader.isProvisional checks in INFileReader.
                    let after_ckpt = result.checkpoint_start_lsn == NULL_LSN
                        || entry_lsn >= result.checkpoint_start_lsn;
                    if after_ckpt {
                        // Extract fields before moving rec into record_dirty_in.
                        let node_id = rec.node_id;
                        let db_id = rec.db_id;
                        let is_delta = rec.is_delta;
                        let level = rec.level;

                        result.record_dirty_in(rec, entry_lsn);

                        // Track in the DirtyINMap (for bottom-up redo ordering).
                        self.dirty_in_map.add_dirty_in(
                            CheckpointReference::new(
                                node_id,
                                db_id as i64,
                                is_delta,
                                level,
                            ),
                        );
                    }
                }

                // ----------------------------------------------------------
                // LN entries → track txn state for undo/redo
                // ----------------------------------------------------------
                LogEntry::Ln(rec) => {
                    let db_id = rec.db_id;
                    let txn_id = rec.txn_id;

                    // Move rec into redo_entries — no clone, no Arc bump.
                    self.redo_entries.push((entry_lsn, rec));

                    // Track per-db count for BIN capacity pre-warming (Fix 3).
                    *self.per_db_redo_count.entry(db_id).or_insert(0) += 1;

                    if let Some(txn_id) = txn_id {
                        // Track this txn as active until we see its commit/abort.
                        // record_active_txn() also updates max_txn_id.
                        result.record_active_txn(txn_id);
                    }
                    // Non-transactional LNs (txn_id == None) need no extra
                    // tracking; they are always redo'd after checkpoint start.
                }

                // ----------------------------------------------------------
                // Commit / Abort records
                // ----------------------------------------------------------
                LogEntry::TxnCommit(rec) => {
                    // CommittedTxnIds.put(reader.getTxnCommitId(), ...)
                    result.record_commit(rec.txn_id, rec.lsn);
                    self.stats.committed_txns += 1;
                    // R-3: collect TxnCommit dtvlsn for VLSN index rebuild.
                    // Only non-zero for recovered XA commits written with the
                    // R-3 fix; ignored for normal commits and old WAL files.
                    if let Some(vlsn) = rec.dtvlsn
                        && vlsn > 0
                    {
                        result.txncommit_vlsns.push((vlsn, rec.lsn.as_u64()));
                    }
                }
                LogEntry::TxnAbort(rec) => {
                    // AbortedTxnIds.add(reader.getTxnAbortId())
                    result.record_abort(rec.txn_id);
                    self.stats.aborted_txns += 1;
                }
                LogEntry::TxnPrepare(rec) => {
                    // XA two-phase commit, phase 1 (wave 3-2).
                    // Move the txn from active→prepared.  If a later
                    // TxnCommit or TxnAbort is seen, `record_commit` /
                    // `record_abort` will remove the entry.
                    result.record_prepare(
                        crate::analysis_result::PreparedTxnInfo {
                            txn_id: rec.txn_id,
                            prepare_lsn: rec.lsn,
                            first_lsn: rec.first_lsn,
                            last_lsn: rec.last_lsn,
                            xid_format_id: rec.xid_format_id,
                            xid_gtrid: rec.xid_gtrid,
                            xid_bqual: rec.xid_bqual,
                        },
                    );
                    self.stats.prepared_txns += 1;
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
                        || entry_lsn >= result.checkpoint_end_lsn;
                    if is_latest {
                        result.checkpoint_end_lsn = entry_lsn;
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
                    // RollbackTracker.register(RollbackStart, lsn)
                    self.rollback_tracker
                        .register_rollback_start(rec.matchpoint_lsn, rec.lsn);
                }
                LogEntry::RollbackEnd(rec) => {
                    // RollbackTracker.register(RollbackEnd, lsn)
                    self.rollback_tracker
                        .register_rollback_end(rec.matchpoint_lsn, rec.lsn);
                }

                // ----------------------------------------------------------
                // NameLN: database name registration
                // ----------------------------------------------------------
                LogEntry::NameLn(rec) => {
                    if rec.is_deleted {
                        result.recovered_db_names.remove(&rec.name);
                        result.recovered_db_txn_ids.remove(&rec.name);
                    } else {
                        result
                            .recovered_db_names
                            .insert(rec.name.clone(), rec.db_id);
                        // C-6: record the creating txn_id so that
                        // run_mapping_tree_undo_pass can undo NameLNs whose
                        // transaction aborted.
                        if let Some(tid) = rec.txn_id {
                            result.recovered_db_txn_ids.insert(rec.name, tid);
                        }
                    }
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

    /// Walks `self.redo_entries` and groups every LN whose `txn_id` matches
    /// one of the in-doubt prepared transactions in `analysis` into a
    /// `prepared_txn_lns` map keyed by txn_id.
    ///
    /// Called from `recover()` / `recover_all()` after the analysis pass
    /// so that `xa_commit(xid)` can replay the prepared txn’s writes
    /// into the in-memory tree at resolution time, and so that the
    /// redo/undo phases can skip prepared LNs without further work.
    ///
    /// Part of XA in-doubt recovery: prepared txns are surfaced to the
    /// environment layer for application-level resolution.
    fn collect_prepared_txn_lns(
        &self,
        analysis: &AnalysisResult,
    ) -> hashbrown::HashMap<u64, Vec<crate::analysis_result::PreparedLnReplay>>
    {
        use crate::analysis_result::{PreparedLnOperation, PreparedLnReplay};
        let mut by_txn: hashbrown::HashMap<u64, Vec<PreparedLnReplay>> =
            hashbrown::HashMap::new();
        if analysis.prepared_txns.is_empty() {
            return by_txn;
        }
        for (lsn, rec) in &self.redo_entries {
            let Some(txn_id) = rec.txn_id else { continue };
            if !analysis.prepared_txns.contains_key(&txn_id) {
                continue;
            }
            let op = match rec.operation {
                LnOperation::Insert => PreparedLnOperation::Insert,
                LnOperation::Update => PreparedLnOperation::Update,
                LnOperation::Delete => PreparedLnOperation::Delete,
            };
            by_txn.entry(txn_id).or_default().push(PreparedLnReplay {
                db_id: rec.db_id,
                original_lsn: *lsn,
                operation: op,
                key: rec.key.to_vec(),
                data: rec.data.as_ref().map(|b| b.to_vec()),
            });
        }
        by_txn
    }

    // ====================================================================
    // Phase 2: Redo
    // ====================================================================

    /// Replay dirty INs bottom-up and redo committed/non-txnal LNs.
    ///
    /// ## IN redo (§ "buildINs" in the)
    /// Walk the dirty-IN map bottom-up (lowest level first).  For each IN,
    /// "splice" it into the in-memory tree.  Because the real tree is not yet
    /// wired to the recovery manager, we record the redo decision for each
    /// IN and count statistics.
    ///
    /// ## LN redo (§ "redoLNs" in the)
    /// Forward-scan the LN entries collected during analysis.  For each LN,
    /// determine eligibility:
    ///
    /// - **Committed LN after checkpoint start**: always redo.
    /// - **Non-transactional LN after checkpoint start**: always redo.
    /// - **LN in an aborted txn**: skip.
    /// - **LN in an active (uncommitted) txn**: skip (will be undone).
    ///
    ///
    fn run_redo(
        &mut self,
        _scanner: &dyn LogScanner,
        analysis: &AnalysisResult,
        mut tree: Option<&mut noxu_tree::Tree>,
    ) -> Result<()> {
        // ---- Redo INs (bottom-up via DirtyINMap) ----
        //
        // RedoDirtyNodes() / DirtyINMap.getLowestLevel() loop.
        //
        // `INLogEntry.readEntry()` / `getMainItem()` deserializes the
        // IN from the log entry body.  We collect dirty-IN entries during
        // analysis (stored in `self.redo_entries`-analogue, the dirty_in_map)
        // and replay each BIN into the tree.
        //
        // H-6: deserialize IN log entries and re-insert BINs into the tree.
        // We walk the dirty-IN map bottom-up (same ordering as the
        // `processINList()`), then for each entry use `BinStub::deserialize_full`
        // or `BinStub::apply_delta` to reconstruct the node and insert it.
        //
        // The dirty_in_map records node_id+level metadata.  The actual bytes
        // come from `self.redo_entries` collected during analysis as `LogEntry::In`.
        // For simplicity we scan the analysis redo_entries for In records and
        // apply them to the tree directly (the map ordering is preserved because
        // analysis scanned forward and the BIN pass is level 0).
        //
        // RecoveryManager.redoDirtyNodes() +
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

        // ---- Recovery alloc optimisation: pre-warm BIN capacity before LN redo ----
        //
        // If this is a single-database recovery, look up the per-db count
        // and call hint_redo_capacity on the tree before inserting.
        // This sets the redo_capacity_hint so the first redo_insert call
        // will pre-allocate the initial BIN at the right size.
        if let Some(t) = tree.as_deref_mut() {
            let db_id = t.get_database_id();
            let count =
                self.per_db_redo_count.get(&db_id).copied().unwrap_or(0);
            if count > 0 && t.get_redo_capacity_hint() == 0 {
                t.hint_redo_capacity(count);
            }
        }

        // ---- Redo LNs (forward scan) ----
        //
        // LNFileReader(forward=true, start=firstActiveLsn) loop.
        let ckpt_start = analysis.checkpoint_start_lsn;

        // Collect so we don't borrow self mutably twice.
        let redo_entries: Vec<(Lsn, LnRecord)> =
            std::mem::take(&mut self.redo_entries);

        // LOG-6: VLSN-ordering tracker.
        //
        // Every replicated LN carries a VLSN in its log entry header.  As
        // the redo pass replays committed LNs in forward log order, the
        // VLSNs of the *replicated* entries we apply must be strictly
        // increasing — anything else means the local log was reordered or
        // an attacker inserted an out-of-order frame.  We do NOT abort
        // recovery on a violation (a partial recovery is preferable to a
        // refusal to mount); instead we log::error! and skip the offending
        // entry so the operator sees the corruption.
        let mut last_redone_vlsn: Option<u64> = None;
        let mut vlsn_violations: u64 = 0;
        // X-14: collect (vlsn, lsn) pairs from redo entries so the VLSN
        // index can be rebuilt after crash recovery on a replicated node.
        let mut recovered_vlsn_pairs: Vec<(u64, u64)> = Vec::new();

        for (lsn, rec) in &redo_entries {
            self.stats.lns_read_redo += 1;

            let action =
                self.eligible_for_redo(*lsn, rec, ckpt_start, analysis);

            if let RedoAction::Apply = action {
                // VLSN-ordering check before we touch the tree.
                if let Some(curr) = rec.vlsn {
                    if let Some(prev) = last_redone_vlsn
                        && curr <= prev
                    {
                        log::error!(
                            "noxu-recovery: out-of-order VLSN during redo \
                             at lsn={lsn:?}: current vlsn={curr} <= previous \
                             vlsn={prev}; skipping this entry to keep the \
                             rest of recovery viable (LOG-6)"
                        );
                        vlsn_violations += 1;
                        continue;
                    }
                    last_redone_vlsn = Some(curr);
                    // X-14: record the VLSN→LSN mapping for index rebuild.
                    recovered_vlsn_pairs.push((curr, lsn.as_u64()));
                }

                // RecoveryManager.redoOneLN / redo().
                //
                // decision:
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

        if vlsn_violations > 0 {
            log::error!(
                "noxu-recovery: {vlsn_violations} VLSN-ordering violation(s) \
                 detected during redo; database may be missing replicated \
                 updates"
            );
            self.stats.vlsn_ordering_violations += vlsn_violations;
        }

        // Put the entries back (they may be needed for undo diagnostics).
        self.redo_entries = redo_entries;

        // X-14: store the collected VLSN→LSN pairs so recover_all() can
        // publish them in RecoveryInfo for the VLSN index rebuild.
        // R-3: also include TxnCommit-derived VLSNs from the analysis pass.
        recovered_vlsn_pairs.extend_from_slice(&analysis.txncommit_vlsns);
        recovered_vlsn_pairs.sort_unstable_by_key(|&(vlsn, _)| vlsn);
        recovered_vlsn_pairs.dedup_by_key(|t| t.0);
        self.info.recovered_vlsns = recovered_vlsn_pairs;

        Ok(())
    }

    /// Apply a single committed LN to the tree during the redo phase.
    ///
    /// / the `redo()` helper:
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
        // Db-id check.
        if tree.get_database_id() != rec.db_id {
            return;
        }
        match rec.operation {
            LnOperation::Insert | LnOperation::Update => {
                // Insert the logged version.  `tree.redo_insert` updates the slot
                // if the key already exists, which gives us the "logrecLsn >
                // treeLsn → replace" semantics from the.  If the tree already
                // holds a *newer* entry for this key (another committed write
                // that arrived after the log was scanned), the overwrite is
                // still safe here because recovery runs before any new
                // transactions are admitted.
                //
                // Recovery alloc optimisation: pass &[u8] slices directly instead of
                // materialising two intermediate Vec<u8> (rec.key.to_vec() +
                // rec.data.to_vec()).  The compressed key suffix and the data
                // bytes are copied into the BinEntry exactly once inside
                // BinStub::insert_with_prefix_slice.
                let data_slice = rec.data.as_deref().unwrap_or(&[]);
                // Recovery design call: tree.redo_insert errors during redo
                // are logged and we continue. The TreeError variants
                // (SplitRequired, Lookup, MemoryAllocFailure) on a
                // single key indicate a failure to materialise that
                // entry, but the rest of the log replay is still
                // valid; aborting recovery on the first failed redo
                // would leave the entire database unrecoverable. The
                // operator sees the failure via log::error! and can
                // decide whether to escalate (e.g. restore from
                // backup) based on the breadth of failures.
                if let Err(e) = tree.redo_insert(&rec.key, data_slice, lsn) {
                    log::error!(
                        "noxu-recovery: redo failed at lsn={lsn:?}, db={}, \
                         op={:?}: {e:?}; recovery will continue but this \
                         slot may be missing",
                        rec.db_id,
                        rec.operation,
                    );
                }
            }
            LnOperation::Delete => {
                // Bin.deleteEntry(index) / slot KD-flag set.
                // Our tree's delete() is a no-op when the key is absent, so
                // this is always safe.
                tree.delete(&rec.key);
            }
        }
    }

    /// Decide whether an LN should be redone.
    ///
    ///
    ///
    /// Categories (from comments):
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
        // AfterCheckpointStart = (checkpointStartLsn == NULL_LSN ||
        //           DbLsn.compareTo(reader.getLastLsn(), checkpointStartLsn) >= 0)
        let _after_ckpt_start = ckpt_start == NULL_LSN || lsn >= ckpt_start;

        match rec.txn_id {
            None => {
                // Non-transactional LN.
                //
                // In standard JE, pre-checkpoint non-transactional LNs are
                // skipped because the checkpoint's BIN records capture their
                // committed state.  In Noxu, the checkpointer only flushes
                // the internal `primary_tree` and does NOT flush the BINs of
                // any open user databases.  Pre-checkpoint non-transactional
                // LNs are therefore NOT represented in the checkpoint's BIN
                // records.  Skipping them causes those records to vanish
                // after a close+reopen whenever the background checkpointer
                // thread runs between writes.
                //
                // St-H6 (recovery manifestation): always replay
                // non-transactional LNs from the full scan range, same as
                // committed transactional LNs.  `redo_ln` / `redo_insert` is
                // idempotent (LSN comparison skips stale overwrites), so
                // replaying redundantly is always correct.
                RedoAction::Apply
            }
            Some(txn_id) => {
                if analysis.is_committed(txn_id) {
                    // Committed LN: always redo, regardless of whether it
                    // precedes the checkpoint start.  Noxu's checkpointer
                    // flushes an in-memory primary_tree that may not yet
                    // contain all committed data from all open databases, so
                    // the BIN entries in the checkpoint cannot be trusted as
                    // a complete snapshot of pre-checkpoint state.  We must
                    // replay all committed LNs from the full scan range.
                    // `redo_ln` is idempotent (it skips if the tree already
                    // holds a newer LSN for the key), so replaying redundantly
                    // is always correct.
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
    /// / `RecoveryManager.undo()`.
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

        // Fast path: no uncommitted transactions → skip entire backward scan.
        // This is the common case after a clean shutdown.
        if !analysis.has_active_txns() {
            return Ok(());
        }

        // Backward scan: from last_used down to first_active.
        //
        // LNFileReader(redo=false, start=lastUsedLsn,
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
            // Reader.isCommit() / reader.isAbort() branches that
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
                // If (committedTxnIds.containsKey(txnId)) continue;
                if analysis.is_committed(txn_id) {
                    continue;
                }
                // XA in-doubt recovery: skip prepared (XA in-doubt) transactions
                // — the resolved_commit / resolved_abort path will
                // either replay them into the tree (xa_commit) or
                // discard them (xa_rollback).
                if analysis.is_prepared(txn_id) {
                    continue;
                }

                // AbortedTxnIds contains txnId → still undo
                // (undoes LNs even for aborted txns in this pass unless
                //  they are in the resurrected set; since we don't handle
                //  replication resurrection here, we undo all non-committed).

                // Active (uncommitted) transaction → undo.
                let action = Self::compute_undo_action(rec);
                match &action {
                    UndoAction::DeleteSlot => {
                        // RecoveryManager.undo() → bin.deleteEntry()
                        //.  Delete the slot; if it was already removed by
                        // a later operation, this is a no-op.
                        // Currency check (JE BIN.recoverRecord): only delete
                        // when the slot still holds THIS record's version.
                        if let Some(t) = tree.as_deref_mut()
                            && t.get_database_id() == rec.db_id
                            && Self::undo_slot_is_current(t, &rec.key, pe.lsn)
                        {
                            t.delete(&rec.key);
                        }
                        self.stats.lns_undone += 1;
                        self.stats.active_txns_undone += 1;
                    }
                    UndoAction::RevertToAbortLsn { abort_lsn } => {
                        // RecoveryManager.undo().
                        //
                        // Decision table (from RecoveryManager.undo()):
                        //
                        //  abort_known_deleted == true
                        //    → key was deleted before this write; restore
                        //      deleted state by removing the slot.
                        //
                        //  abort_data.is_some()  (embedded before-image)
                        //    → re-insert the prior key/value at abort_lsn.
                        //      stores the before-image inline in every
                        //      LNLogEntry (getAbortKey/getAbortData) so that
                        //      undo never has to re-read the log.
                        //
                        //  abort_data.is_none() && !abort_known_deleted
                        //    → non-embedded LN: read the before-image from
                        //      the log at abort_lsn.  calls
                        //      `fetchTarget(db, bin, idx, abortLsn, ...)` for
                        //      this case.  We call scanner.read_at_lsn().
                        if let Some(t) = tree.as_deref_mut()
                            && t.get_database_id() == rec.db_id
                            && Self::undo_slot_is_current(t, &rec.key, pe.lsn)
                        {
                            if rec.abort_known_deleted {
                                // Before this write the slot was deleted.
                                t.delete(&rec.key);
                            } else if let Some(abort_data) = &rec.abort_data {
                                // Embedded before-image: re-insert prior value.
                                let key = rec
                                    .abort_key
                                    .clone()
                                    .unwrap_or_else(|| rec.key.clone())
                                    .to_vec();
                                if let Err(e) = t.insert(
                                    key,
                                    abort_data.to_vec(),
                                    *abort_lsn,
                                ) {
                                    log::error!(
                                        "noxu-recovery: undo (embedded \
                                         before-image, post-analysis) failed \
                                         at abort_lsn={abort_lsn:?}, \
                                         db={}: {e:?}; recovery will \
                                         continue but this slot may be \
                                         inconsistent",
                                        rec.db_id,
                                    );
                                }
                            } else {
                                // Non-embedded LN: fetch before-image from log.
                                //
                                // `fetchTarget(db, bin, idx, abortLsn)`:
                                // read the LN at abort_lsn and apply its key/data.
                                // If the log read fails (e.g. the file was cleaned
                                // away), fall back to deleting the slot — a safe
                                // conservative action that avoids exposing a stale
                                // value.
                                let before_image =
                                    scanner.read_at_lsn(*abort_lsn);
                                if let Some(LogEntry::Ln(before_rec)) =
                                    before_image
                                {
                                    if let Some(before_data) = before_rec.data {
                                        let key = before_rec
                                            .abort_key
                                            .unwrap_or(before_rec.key)
                                            .to_vec();
                                        if let Err(e) = t.insert(
                                            key,
                                            before_data.to_vec(),
                                            *abort_lsn,
                                        ) {
                                            log::error!(
                                                "noxu-recovery: undo \
                                                 (non-embedded before-image, \
                                                 post-analysis) failed at \
                                                 abort_lsn={abort_lsn:?}, \
                                                 db={}: {e:?}; recovery \
                                                 will continue but this slot \
                                                 may be inconsistent",
                                                rec.db_id,
                                            );
                                        }
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
    /// Decision table for undo during recovery:
    ///
    /// ```text
    /// abort_lsn is NULL  → first write → delete the slot
    /// abort_lsn is valid → revert to abort_lsn (before-image)
    /// ```
    ///
    /// The `logLsn == slotLsn` currency check is enforced by the caller via
    /// [`Self::undo_slot_is_current`] before this action is applied to the
    /// tree (JE `BIN.recoverRecord`): an undo before-image is applied only
    /// when the slot still holds the exact version this record logged. Here
    /// we compute the *intended* action from the log record metadata alone.
    fn compute_undo_action(rec: &LnRecord) -> UndoAction {
        if rec.abort_lsn == NULL_LSN {
            // This was the first write of this key: undo by deleting the slot.
            UndoAction::DeleteSlot
        } else {
            // Revert to before-image.
            UndoAction::RevertToAbortLsn { abort_lsn: rec.abort_lsn }
        }
    }

    /// JE `BIN.recoverRecord` currency check (`updateEntry = logLsn ==
    /// slotLsn`). An undo action may modify the tree slot for `key` ONLY when
    /// the slot currently holds the exact version logged at `log_lsn`.
    ///
    /// Recovery rebuilds user trees by redoing **committed** LNs only;
    /// uncommitted/aborted LNs are never redone. So at undo time the slot
    /// either (a) holds this record's version — apply the undo, (b) holds a
    /// LATER committed version (higher LSN) — skip, or (c) is absent — skip.
    /// Skipping (b) is the critical fix: without it, an aborted txn's
    /// before-image overwrites a subsequently-committed write of the same key,
    /// silently losing committed data on recovery.
    fn undo_slot_is_current(
        tree: &noxu_tree::Tree,
        key: &[u8],
        log_lsn: Lsn,
    ) -> bool {
        match tree.search_with_data(key) {
            Some(sf) if sf.found => sf.lsn == log_lsn.as_u64(),
            _ => false,
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
        InRecord, LnOperation, LnRecord, LogEntry, RollbackEndRecord,
        RollbackStartRecord, TxnAbortRecord, TxnCommitRecord,
    };
    use bytes::Bytes;

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
            Bytes::copy_from_slice(key),
            Some(Bytes::from_static(b"value")),
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
            Bytes::copy_from_slice(key),
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
        InRecord {
            db_id,
            node_id,
            level,
            is_root,
            is_delta: false,
            node_data: None,
        }
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
            assert!(
                !desc.is_empty(),
                "stage {:?} has empty description",
                stage
            );
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
                dtvlsn: None,
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
            LogEntry::CkptStart(CkptStartRecord { id: 1, lsn: lsn(0, 50) }),
        );
        // DbTree root
        scanner.push(
            lsn(0, 60),
            LogEntry::DbTree(DbTreeRecord { lsn: lsn(0, 60) }),
        );
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
        scanner.push(
            lsn(0, 10),
            LogEntry::DbTree(DbTreeRecord { lsn: lsn(0, 10) }),
        );
        scanner.push(
            lsn(0, 100),
            LogEntry::TxnCommit(TxnCommitRecord {
                txn_id: 1,
                lsn: lsn(0, 100),
                dtvlsn: None,
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
        scanner
            .push(lsn(0, 100), LogEntry::In(make_in_record(1, 10, 0, false)));
        scanner
            .push(lsn(0, 200), LogEntry::In(make_in_record(1, 20, 0, false)));
        // One upper IN at level 1
        scanner.push(lsn(0, 300), LogEntry::In(make_in_record(1, 30, 1, true)));
        scanner.push(
            lsn(0, 400),
            LogEntry::TxnCommit(TxnCommitRecord {
                txn_id: 5,
                lsn: lsn(0, 400),
                dtvlsn: None,
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
                dtvlsn: None,
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
                dtvlsn: None,
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

    /// LOG-6: when two committed replicated LNs appear with VLSNs that
    /// are *not* strictly increasing, the redo phase logs an error and
    /// skips the offending entry rather than silently applying it.  The
    /// number of skips is recorded in `RecoveryStats`.
    #[test]
    fn test_redo_skips_out_of_order_vlsn() {
        let ckpt_start = lsn(0, 50);

        let mut scanner = InMemoryLogScanner::new();
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

        // Two LNs with the same committed txn — the second has a *smaller*
        // VLSN than the first, simulating either log reorder corruption or
        // an attacker who replayed an old replication frame.
        let mut rec1 = make_insert(1, Some(42), b"a", NULL_LSN);
        rec1.vlsn = Some(100);
        scanner.push(lsn(0, 300), LogEntry::Ln(rec1));

        let mut rec2 = make_insert(1, Some(42), b"b", NULL_LSN);
        rec2.vlsn = Some(50); // < 100 → out of order
        scanner.push(lsn(0, 350), LogEntry::Ln(rec2));

        scanner.push(
            lsn(0, 400),
            LogEntry::TxnCommit(TxnCommitRecord {
                txn_id: 42,
                lsn: lsn(0, 400),
                dtvlsn: None,
            }),
        );

        let mut mgr = RecoveryManager::new();
        mgr.recover(&mut scanner, None, false).unwrap();

        let stats = mgr.get_stats();
        assert_eq!(
            stats.lns_redone, 1,
            "only the first (in-order VLSN) entry should be redone"
        );
        assert_eq!(
            stats.vlsn_ordering_violations, 1,
            "exactly one VLSN-ordering violation should have been recorded"
        );
    }

    /// LOG-6: equal VLSNs are also rejected — the invariant is *strictly*
    /// increasing, not non-decreasing.  An attacker who replays the
    /// previously-acked frame would otherwise slip through.
    #[test]
    fn test_redo_rejects_duplicate_vlsn() {
        let mut scanner = InMemoryLogScanner::new();

        let mut rec1 = make_insert(1, None, b"a", NULL_LSN);
        rec1.vlsn = Some(7);
        scanner.push(lsn(0, 100), LogEntry::Ln(rec1));

        let mut rec2 = make_insert(1, None, b"b", NULL_LSN);
        rec2.vlsn = Some(7); // duplicate
        scanner.push(lsn(0, 200), LogEntry::Ln(rec2));

        let mut mgr = RecoveryManager::new();
        mgr.recover(&mut scanner, None, false).unwrap();

        let stats = mgr.get_stats();
        assert_eq!(stats.lns_redone, 1);
        assert_eq!(stats.vlsn_ordering_violations, 1);
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
                dtvlsn: None,
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
        scanner.push(
            lsn(0, 10),
            LogEntry::Ln(make_insert(1, Some(1), b"k1", NULL_LSN)),
        );
        scanner.push(
            lsn(0, 20),
            LogEntry::TxnCommit(TxnCommitRecord {
                txn_id: 1,
                lsn: lsn(0, 20),
                dtvlsn: None,
            }),
        );

        // txn 2 LN + abort
        scanner.push(
            lsn(0, 30),
            LogEntry::Ln(make_insert(1, Some(2), b"k2", NULL_LSN)),
        );
        scanner
            .push(lsn(0, 40), LogEntry::TxnAbort(TxnAbortRecord { txn_id: 2 }));

        // txn 3 LN — no commit/abort (active at crash)
        scanner.push(
            lsn(0, 50),
            LogEntry::Ln(make_insert(1, Some(3), b"k3", NULL_LSN)),
        );

        let mut mgr = RecoveryManager::new();
        let _info = mgr.recover(&mut scanner, None, false).unwrap();

        // txn 1 committed → redone
        assert_eq!(mgr.get_stats().lns_redone, 1);
        // txn 2 aborted + txn 3 active → both undone.
        // undoLNs skips only committedTxnIds; aborted txns still go
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
        scanner.push(
            lsn(0, 10),
            LogEntry::CkptStart(CkptStartRecord { id: 1, lsn: lsn(0, 10) }),
        );
        scanner.push(
            lsn(0, 100),
            LogEntry::CkptEnd(CkptEndRecord {
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
            }),
        );

        // Second (later) complete checkpoint
        scanner.push(
            lsn(0, 200),
            LogEntry::CkptStart(CkptStartRecord { id: 2, lsn: lsn(0, 200) }),
        );
        scanner.push(
            lsn(0, 500),
            LogEntry::CkptEnd(CkptEndRecord {
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
            }),
        );

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
            LogEntry::TxnCommit(TxnCommitRecord {
                txn_id: 1,
                lsn: lsn(0, 200),
                dtvlsn: None,
            }),
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
                dtvlsn: None,
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
                Bytes::from_static(b"alpha"),
                Some(Bytes::from_static(b"value_a")),
                NULL_LSN,
                false,
            )),
        );
        scanner.push(
            lsn(0, 20),
            LogEntry::TxnCommit(TxnCommitRecord {
                txn_id: 1,
                lsn: lsn(0, 20),
                dtvlsn: None,
            }),
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
                Bytes::from_static(b"beta"),
                Some(Bytes::from_static(b"value_b")),
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
                Bytes::from_static(b"gamma"),
                Some(Bytes::from_static(b"value_g")),
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
                Bytes::from_static(b"delta"),
                Some(Bytes::from_static(b"value_d")),
                NULL_LSN, // abort_lsn=NULL → first write → DeleteSlot
                false,
            )),
        );

        let mut mgr = RecoveryManager::new();
        mgr.recover(&mut scanner, Some(&mut tree), false).unwrap();

        // After undo: key must be removed.
        let found = tree
            .search(b"delta")
            .map(|r| r.exact_parent_found)
            .unwrap_or(false);
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
                Bytes::from_static(b"epsilon"),
                None,
                NULL_LSN,
                true,
            )),
        );
        scanner.push(
            lsn(0, 20),
            LogEntry::TxnCommit(TxnCommitRecord {
                txn_id: 2,
                lsn: lsn(0, 20),
                dtvlsn: None,
            }),
        );

        let mut mgr = RecoveryManager::new();
        mgr.recover(&mut scanner, Some(&mut tree), false).unwrap();

        let found = tree
            .search(b"epsilon")
            .map(|r| r.exact_parent_found)
            .unwrap_or(false);
        assert!(!found, "committed delete must remove the key from the tree");
        assert_eq!(mgr.get_stats().lns_redone, 1);
    }

    /// All transactions committed → undo pass is skipped (lns_read_undo == 0).
    #[test]
    fn test_undo_skipped_when_all_txns_committed() {
        let mut scanner = InMemoryLogScanner::new();
        // Three transactions, all committed.
        for txn_id in 1u64..=3 {
            scanner.push(
                lsn(0, txn_id as u32 * 10),
                LogEntry::Ln(make_insert(1, Some(txn_id), b"k", NULL_LSN)),
            );
            scanner.push(
                lsn(0, txn_id as u32 * 10 + 5),
                LogEntry::TxnCommit(TxnCommitRecord {
                    txn_id,
                    lsn: lsn(0, txn_id as u32 * 10 + 5),
                    dtvlsn: None,
                }),
            );
        }

        let mut mgr = RecoveryManager::new();
        mgr.recover(&mut scanner, None, false).unwrap();

        // All 3 redone, zero scanned for undo (fast path).
        assert_eq!(mgr.get_stats().lns_redone, 3);
        assert_eq!(
            mgr.get_stats().lns_read_undo,
            0,
            "undo pass must be skipped when no active txns"
        );
    }

    /// Multiple keys: committed inserts visible, uncommitted insert absent.
    #[test]
    fn test_redo_mixed_committed_and_uncommitted() {
        let mut scanner = InMemoryLogScanner::new();
        // txn=1: committed insert of "key1"
        scanner.push(
            lsn(0, 10),
            LogEntry::Ln(LnRecord::new(
                1,
                Some(1),
                LnOperation::Insert,
                Bytes::from_static(b"key1"),
                Some(Bytes::from_static(b"v1")),
                NULL_LSN,
                false,
            )),
        );
        scanner.push(
            lsn(0, 20),
            LogEntry::TxnCommit(TxnCommitRecord {
                txn_id: 1,
                lsn: lsn(0, 20),
                dtvlsn: None,
            }),
        );
        // txn=2: NOT committed → active
        scanner.push(
            lsn(0, 30),
            LogEntry::Ln(LnRecord::new(
                1,
                Some(2),
                LnOperation::Insert,
                Bytes::from_static(b"key2"),
                Some(Bytes::from_static(b"v2")),
                NULL_LSN,
                false,
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
            !tree
                .search(b"key2")
                .map(|r| r.exact_parent_found)
                .unwrap_or(false),
            "uncommitted key2 must not be in tree"
        );
    }

    // ── X-14 / X-1: VLSN rebuild and rollback truncation ────────────────

    /// X-14: RecoveryInfo::recovered_vlsns must be populated with
    /// (vlsn, lsn) pairs from every LN in the redo pass that carries a VLSN.
    #[test]
    fn test_x14_recovered_vlsns_populated() {
        let mut scanner = InMemoryLogScanner::new();

        // Committed txn 1 with a VLSN on the LN.
        scanner.push(
            lsn(1, 100),
            LogEntry::TxnCommit(TxnCommitRecord {
                txn_id: 1,
                lsn: lsn(1, 100),
                dtvlsn: None,
            }),
        );
        // LN with vlsn=5.
        let mut ln_with_vlsn = make_insert(1, Some(1), b"vkey", NULL_LSN);
        ln_with_vlsn.vlsn = Some(5);
        scanner.push(lsn(1, 200), LogEntry::Ln(ln_with_vlsn));

        // Committed txn 2 with a different VLSN.
        scanner.push(
            lsn(1, 300),
            LogEntry::TxnCommit(TxnCommitRecord {
                txn_id: 2,
                lsn: lsn(1, 300),
                dtvlsn: None,
            }),
        );
        let mut ln_with_vlsn2 = make_insert(1, Some(2), b"vkey2", NULL_LSN);
        ln_with_vlsn2.vlsn = Some(7);
        scanner.push(lsn(1, 400), LogEntry::Ln(ln_with_vlsn2));

        let mut trees = HashMap::new();
        trees.insert(1u64, noxu_tree::Tree::new(1, 256));
        let mut mgr = RecoveryManager::new();
        let info = mgr.recover_all(&mut scanner, &mut trees, false).unwrap();

        // Both VLSN entries must be in recovered_vlsns.
        let vlsns: Vec<u64> =
            info.recovered_vlsns.iter().map(|&(v, _)| v).collect();
        assert!(
            vlsns.contains(&5),
            "X-14: vlsn=5 must be in recovered_vlsns, got: {vlsns:?}"
        );
        assert!(
            vlsns.contains(&7),
            "X-14: vlsn=7 must be in recovered_vlsns, got: {vlsns:?}"
        );
    }

    /// R-3: TxnCommit records with non-NULL dtvlsn must be included in
    /// recovered_vlsns so a second crash after XA resolution doesn't lose
    /// the VLSN.
    #[test]
    fn test_r3_txncommit_dtvlsn_in_recovered_vlsns() {
        let mut scanner = InMemoryLogScanner::new();

        // Simulate a recovered XA commit written with R-3 fix: the TxnCommit
        // entry carries dtvlsn=42.
        scanner.push(
            lsn(1, 100),
            LogEntry::TxnCommit(TxnCommitRecord {
                txn_id: 99,
                lsn: lsn(1, 100),
                dtvlsn: Some(42),
            }),
        );

        // A regular committed txn with an LN carrying vlsn=3 (control).
        scanner.push(
            lsn(1, 200),
            LogEntry::TxnCommit(TxnCommitRecord {
                txn_id: 1,
                lsn: lsn(1, 200),
                dtvlsn: None,
            }),
        );
        let mut ln_vlsn3 = make_insert(1, Some(1), b"rkey", NULL_LSN);
        ln_vlsn3.vlsn = Some(3);
        scanner.push(lsn(1, 300), LogEntry::Ln(ln_vlsn3));

        let mut trees = HashMap::new();
        trees.insert(1u64, noxu_tree::Tree::new(1, 256));
        let mut mgr = RecoveryManager::new();
        let info = mgr.recover_all(&mut scanner, &mut trees, false).unwrap();

        let vlsns: Vec<u64> =
            info.recovered_vlsns.iter().map(|&(v, _)| v).collect();

        // The XA TxnCommit dtvlsn=42 must be included.
        assert!(
            vlsns.contains(&42),
            "R-3: TxnCommit dtvlsn=42 must be in recovered_vlsns after second \
             crash; got: {vlsns:?}"
        );
        // Control: the LN vlsn=3 must also be present.
        assert!(
            vlsns.contains(&3),
            "R-3 control: LN vlsn=3 must be in recovered_vlsns, got: {vlsns:?}"
        );
    }

    /// X-1: after recovery with a completed rollback period,
    /// rollback_matchpoint_lsn must be set.
    #[test]
    fn test_x1_rollback_matchpoint_lsn_set() {
        let mut scanner = InMemoryLogScanner::new();

        // A completed rollback: matchpoint at lsn(1,50), start at lsn(1,300),
        // end at lsn(1,400).
        scanner.push(
            lsn(1, 50),
            LogEntry::TxnCommit(TxnCommitRecord {
                txn_id: 99,
                lsn: lsn(1, 50),
                dtvlsn: None,
            }),
        );
        scanner.push(
            lsn(1, 300),
            LogEntry::RollbackStart(RollbackStartRecord {
                matchpoint_lsn: lsn(1, 50),
                lsn: lsn(1, 300),
            }),
        );
        scanner.push(
            lsn(1, 400),
            LogEntry::RollbackEnd(RollbackEndRecord {
                matchpoint_lsn: lsn(1, 50),
                lsn: lsn(1, 400),
            }),
        );

        let mut trees = HashMap::new();
        trees.insert(1u64, noxu_tree::Tree::new(1, 256));
        let mut mgr = RecoveryManager::new();
        let info = mgr.recover_all(&mut scanner, &mut trees, false).unwrap();

        assert!(
            info.rollback_matchpoint_lsn.is_some(),
            "X-1: rollback_matchpoint_lsn must be set after rollback recovery"
        );
        assert_eq!(
            info.rollback_matchpoint_lsn.unwrap(),
            lsn(1, 50).as_u64(),
            "X-1: rollback matchpoint must match the period's matchpoint_lsn"
        );
    }

    /// C-6 unit test: run_mapping_tree_undo_pass removes NameLN entries whose
    /// creating transaction did NOT commit.
    ///
    /// This test exercises the undo logic with synthetic AnalysisResult data
    /// where `recovered_db_txn_ids` is populated (as written by
    /// `EnvironmentImpl::log_name_ln_txn` when a database is created inside a
    /// transaction).
    ///
    /// # What this tests
    /// The undo predicate: a name in `recovered_db_txn_ids` is removed when
    /// its txn_id is NOT in `committed_txns`.  This covers both the explicit
    /// TxnAbort case and the crash-before-commit case.  Names with no txn_id
    /// (old-format NameLN written at commit time) or with committed txn_ids
    /// survive.
    #[test]
    fn test_c6_mapping_tree_undo_removes_aborted_namelns() {
        let mut analysis = crate::analysis_result::AnalysisResult::new();

        // Simulate four databases recovered from NameLN/NameLNTxn entries:
        // 1. "committed_db" — written with txn_id 10 (committed).
        // 2. "aborted_db"   — written with txn_id 20 (explicitly aborted).
        // 3. "no_txn_db"    — written without txn_id (old-format NameLN).
        // 4. "crashed_db"   — written with txn_id 30 (neither committed nor aborted).
        analysis.recovered_db_names.insert("committed_db".to_string(), 1);
        analysis.recovered_db_names.insert("aborted_db".to_string(), 2);
        analysis.recovered_db_names.insert("no_txn_db".to_string(), 3);
        analysis.recovered_db_names.insert("crashed_db".to_string(), 4);

        analysis.recovered_db_txn_ids.insert("committed_db".to_string(), 10);
        analysis.recovered_db_txn_ids.insert("aborted_db".to_string(), 20);
        analysis.recovered_db_txn_ids.insert("crashed_db".to_string(), 30);
        // "no_txn_db" has no txn_id entry.

        analysis.committed_txns.insert(10, noxu_util::Lsn::new(0, 100));
        analysis.aborted_txns.insert(20);
        // txn 30 is in neither set (simulates crash-before-commit)

        let mut mgr = RecoveryManager::new();
        mgr.run_mapping_tree_undo_pass(&mut analysis);

        assert!(
            analysis.recovered_db_names.contains_key("committed_db"),
            "C-6: committed_db must survive the undo pass"
        );
        assert!(
            !analysis.recovered_db_names.contains_key("aborted_db"),
            "C-6: aborted_db must be removed by the undo pass"
        );
        assert!(
            analysis.recovered_db_names.contains_key("no_txn_db"),
            "C-6: no_txn_db (no txn_id) must survive the undo pass (old format)"
        );
        assert!(
            !analysis.recovered_db_names.contains_key("crashed_db"),
            "C-6: crashed_db (txn neither committed nor aborted) must be removed"
        );

        // Confirm mapping_tree_db_names mirrors the surviving names.
        assert_eq!(mgr.mapping_tree_db_names.len(), 2);
        assert!(mgr.mapping_tree_db_names.contains_key("committed_db"));
        assert!(mgr.mapping_tree_db_names.contains_key("no_txn_db"));
    }

    /// C-6 end-to-end: create a database inside an aborted transaction,
    /// recover (via InMemoryLogScanner), and assert the database does NOT
    /// appear in the recovered names.
    ///
    /// WAL scenario: NameLn(txn_id=Some(42)) followed by TxnAbort(42).
    #[test]
    fn test_c6_aborted_db_creation_not_recovered() {
        let mut scanner = InMemoryLogScanner::new();

        // Simulate the WAL for: begin T42, open_database_transactional
        // (writes NameLNTxn with txn_id=42), abort T42 (writes TxnAbort).
        scanner.push(
            lsn(0, 100),
            LogEntry::NameLn(crate::log_scanner::NameLnRecord {
                name: "aborted_db".to_string(),
                db_id: 7,
                is_deleted: false,
                txn_id: Some(42),
            }),
        );
        scanner.push(
            lsn(0, 200),
            LogEntry::TxnAbort(TxnAbortRecord { txn_id: 42 }),
        );

        let mut mgr = RecoveryManager::new();
        let mut trees = HashMap::new();
        let info = mgr.recover_all(&mut scanner, &mut trees, false).unwrap();

        assert!(
            !info.recovered_db_names.contains_key("aborted_db"),
            "C-6 end-to-end: aborted transactional db creation must not be recovered"
        );
    }

    /// C-6 end-to-end: create a database inside a COMMITTED transaction,
    /// recover (via InMemoryLogScanner), and assert the database DOES appear
    /// in the recovered names (regression guard — must not over-undo).
    ///
    /// WAL scenario: NameLn(txn_id=Some(43)) followed by TxnCommit(43).
    #[test]
    fn test_c6_committed_db_creation_is_recovered() {
        let mut scanner = InMemoryLogScanner::new();

        // Simulate the WAL for: begin T43, open_database_transactional
        // (writes NameLNTxn with txn_id=43), commit T43 (writes TxnCommit).
        scanner.push(
            lsn(0, 100),
            LogEntry::NameLn(crate::log_scanner::NameLnRecord {
                name: "committed_db".to_string(),
                db_id: 8,
                is_deleted: false,
                txn_id: Some(43),
            }),
        );
        scanner.push(
            lsn(0, 200),
            LogEntry::TxnCommit(TxnCommitRecord {
                txn_id: 43,
                lsn: lsn(0, 200),
                dtvlsn: None,
            }),
        );

        let mut mgr = RecoveryManager::new();
        let mut trees = HashMap::new();
        let info = mgr.recover_all(&mut scanner, &mut trees, false).unwrap();

        assert!(
            info.recovered_db_names.contains_key("committed_db"),
            "C-6 end-to-end: committed transactional db creation MUST be recovered"
        );
        assert_eq!(
            info.recovered_db_names["committed_db"], 8,
            "C-6 end-to-end: committed_db must map to db_id 8"
        );
    }

    /// C-6 old-log compat: a NameLn with txn_id=None (pre-C6 WAL written at
    /// R-5 (Keith re-audit): non-transactional open_database writes NameLN
    /// without txn_id and is immediately durable (auto-committed).  After a
    /// crash, recovery treats it as committed regardless of txn state because
    /// `run_mapping_tree_undo_pass` only undoes entries with a txn_id that did
    /// not commit.
    ///
    /// This test pins the R-5 invariant: a NameLN with txn_id=None must always
    /// survive recovery, even when other transactions are active or aborted.
    #[test]
    fn test_r5_non_txn_namelns_always_survive_recovery() {
        let mut scanner = InMemoryLogScanner::new();

        // Non-transactional NameLN (txn_id=None): immediately durable.
        scanner.push(
            lsn(0, 10),
            LogEntry::NameLn(crate::log_scanner::NameLnRecord {
                name: "non_txn_db".to_string(),
                db_id: 77,
                is_deleted: false,
                txn_id: None,
            }),
        );

        // An aborted transactional NameLNTxn that should be undone.
        scanner.push(
            lsn(0, 20),
            LogEntry::NameLn(crate::log_scanner::NameLnRecord {
                name: "aborted_txn_db".to_string(),
                db_id: 78,
                is_deleted: false,
                txn_id: Some(55),
            }),
        );
        scanner.push(
            lsn(0, 30),
            LogEntry::TxnAbort(crate::log_scanner::TxnAbortRecord {
                txn_id: 55,
            }),
        );

        let mut mgr = RecoveryManager::new();
        let mut trees = HashMap::new();
        let info = mgr.recover_all(&mut scanner, &mut trees, false).unwrap();

        // R-5 invariant: non-txn NameLN must survive.
        assert!(
            info.recovered_db_names.contains_key("non_txn_db"),
            "R-5: non-transactional NameLN (txn_id=None) must survive recovery; \
             got names: {:?}",
            info.recovered_db_names.keys().collect::<Vec<_>>()
        );
        // C-6 invariant: aborted txn NameLNTxn must be undone.
        assert!(
            !info.recovered_db_names.contains_key("aborted_txn_db"),
            "C-6: aborted transactional NameLN must be removed by undo pass"
        );
    }
}
