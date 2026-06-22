//! Checkpoint daemon for Noxu DB.
//!
//!
//! The Checkpointer flushes dirty IN nodes from the tree to the log in
//! bottom-up order. This bounds recovery time and ensures durability.

use crate::checkpoint_end::CheckpointEnd;
use crate::checkpoint_start::CheckpointStart;
use crate::checkpoint_stat::CheckpointStats;
use crate::dirty_in_map::DirtyINMap;
use crate::error::{RecoveryError, Result};
use noxu_cleaner::UtilizationTracker;
use noxu_log::entry::FileSummaryLnEntry;
use noxu_log::entry::bin_delta_log_entry::BinDeltaLogEntry;
use noxu_log::entry::in_log_entry::InLogEntry;
use noxu_log::{LogEntryType, LogManager, Provisional};
use noxu_sync::Mutex;
use noxu_tree::tree::{Tree, TreeNode};
use noxu_txn::TxnManager;
use noxu_util::{Lsn, NULL_LSN};
use parking_lot::RwLock as NodeRwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, RwLock};

/// Configuration for checkpoint behavior.
///
///
///
/// Controls when and how checkpoints are performed.
#[derive(Debug, Clone)]
pub struct CheckpointConfig {
    /// Force a checkpoint even if nothing is dirty.
    pub force: bool,
    /// Minimize recovery time (checkpoint all dirty nodes).
    pub minimize_recovery_time: bool,
    /// Bytes written between checkpoints (0 = time-based only).
    pub bytes_interval: u64,
    /// Milliseconds between checkpoints (0 = disabled).
    pub time_interval: u64,
    /// BIN-delta percent threshold (JE `TREE_BIN_DELTA` / `BIN_DELTA_PERCENT`,
    /// 0–75, default 25).  A BIN is logged as a delta only when its delta-slot
    /// count is `<= nEntries * bin_delta_percent / 100`.  See
    /// `BinStub::should_log_delta` / JE `DatabaseImpl.getBinDeltaPercent()`.
    pub bin_delta_percent: i32,
}

impl CheckpointConfig {
    /// Create a new checkpoint configuration with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set force flag.
    pub fn force(mut self, force: bool) -> Self {
        self.force = force;
        self
    }

    /// Set minimize recovery time flag.
    pub fn minimize_recovery_time(mut self, minimize: bool) -> Self {
        self.minimize_recovery_time = minimize;
        self
    }

    /// Set bytes interval.
    pub fn bytes_interval(mut self, bytes: u64) -> Self {
        self.bytes_interval = bytes;
        self
    }

    /// Set time interval in milliseconds.
    pub fn time_interval(mut self, millis: u64) -> Self {
        self.time_interval = millis;
        self
    }

    /// Set the BIN-delta percent threshold (`TREE_BIN_DELTA`, 0–75).
    pub fn bin_delta_percent(mut self, percent: i32) -> Self {
        self.bin_delta_percent = percent;
        self
    }
}

impl Default for CheckpointConfig {
    fn default() -> Self {
        CheckpointConfig {
            force: false,
            minimize_recovery_time: false,
            bytes_interval: 20_000_000, // 20MB default
            time_interval: 0, // Time-based checkpoints disabled by default
            // JE BIN_DELTA_PERCENT default (TREE_BIN_DELTA, 0–75).
            bin_delta_percent: 25,
        }
    }
}

/// Result of a checkpoint operation.
///
/// Contains information about what was flushed during the checkpoint.
#[derive(Debug, Clone)]
pub struct CheckpointResult {
    /// The checkpoint ID.
    pub checkpoint_id: u64,
    /// LSN of the CheckpointStart entry.
    pub start_lsn: Lsn,
    /// LSN of the CheckpointEnd entry.
    pub end_lsn: Lsn,
    /// Number of full INs flushed.
    pub full_ins_flushed: u64,
    /// Number of full BINs flushed.
    pub full_bins_flushed: u64,
    /// Number of delta INs flushed.
    pub delta_ins_flushed: u64,
    /// Time spent on checkpoint in milliseconds.
    pub elapsed_ms: u64,
}

impl CheckpointResult {
    /// Total nodes flushed.
    pub fn total_nodes_flushed(&self) -> u64 {
        self.full_ins_flushed + self.full_bins_flushed + self.delta_ins_flushed
    }
}

/// The Checkpointer flushes dirty IN nodes to the log.
///
///
///
/// Checkpoint flushes must be done in ascending order from the bottom
/// of the tree up. This ensures that recovery can reconstruct the tree
/// from the checkpoint.
///
/// # Checkpoint Algorithm
///
/// 1. Generate checkpoint ID
/// 2. Create and log CheckpointStart
/// 3. Build dirty IN map (organized by Btree level)
/// 4. Flush dirty INs level by level (bottom-up)
///    - Bottom levels logged provisionally
///    - Top level logged non-provisionally
/// 5. Create and log CheckpointEnd
/// 6. Update statistics
///
/// This implementation flushes dirty BINs via `flush_dirty_bins_internal()`,
/// which writes full BIN or BINDelta log entries depending on the dirty-slot
/// fraction (TREE_BIN_DELTA = 25%). Upper INs (level ≥ 2) are flushed
/// by `flush_upper_ins_internal()` after the BIN pass, bottom-up, using
/// `Provisional::Yes` for intermediate levels and `Provisional::No` for
/// the root. File utilization summaries are persisted via
/// `persist_file_summaries()` at the end of each checkpoint.
pub struct Checkpointer {
    /// Checkpoint statistics
    stats: Arc<CheckpointStats>,
    /// Next checkpoint ID
    next_checkpoint_id: AtomicU64,
    /// The dirty IN map for the current checkpoint
    dirty_map: Mutex<DirtyINMap>,
    /// LSN of the last checkpoint start
    last_checkpoint_start: Mutex<Lsn>,
    /// LSN of the last checkpoint end
    last_checkpoint_end: Mutex<Lsn>,
    /// Whether a checkpoint is in progress
    checkpoint_in_progress: AtomicBool,
    /// Per-database highest IN-level being flushed in the current checkpoint.
    ///
    /// Maps `db_id → highest dirty upper-IN level` for every tree that has
    /// dirty upper INs in this checkpoint pass.  A tree absent from the map
    /// has no dirty upper INs → its highest flush level is 0 → an evicted BIN
    /// from that tree gets `Provisional::No` (no covering ancestor will be
    /// written).  Cleared when the checkpoint finishes or is abandoned.
    ///
    /// JE ref: `DirtyINMap.highestFlushLevels` (per-`DatabaseImpl` map) /
    /// `DirtyINMap.coordinateEvictionWithCheckpoint` / `getHighestFlushLevel`.
    ///
    /// CC-4 residual fix: the old single `AtomicI32` held the *global* max
    /// across all trees, causing a BIN evicted from a tree with **no** dirty
    /// upper INs to be logged `Provisional::Yes` (covered by a non-provisional
    /// ancestor that the checkpoint never actually writes for that tree).
    checkpoint_flush_levels: std::sync::Mutex<HashMap<u64, i32>>,
    /// Shutdown flag
    shutdown: AtomicBool,
    /// Condvar for interruptible daemon sleep — notified by `request_shutdown()`
    /// so the daemon thread wakes up immediately instead of waiting the full
    /// sleep interval.
    shutdown_condvar: Condvar,
    /// Mutex paired with `shutdown_condvar`.
    shutdown_mutex: std::sync::Mutex<bool>,
    /// Configuration
    config: CheckpointConfig,
    /// Optional LogManager for writing CkptStart/CkptEnd WAL entries.
    log_manager: Option<Arc<LogManager>>,
    /// Optional Tree reference for flushing dirty BINs in step 4.
    ///
    /// When `None` (unit tests without a real tree) step 4 is a no-op.
    tree: Option<Arc<RwLock<Tree>>>,
    /// Database ID to pass to `Tree::collect_dirty_bins()`.
    db_id: u64,
    /// Registry of ALL open user-database trees (Stage-1 fix).
    ///
    /// Maps `db_id as i64` → `Arc<RwLock<Tree>>` for every database the
    /// environment has opened.  The checkpointer must flush dirty BINs from
    /// EVERY tree, not just the primary one, so that committed LNs written to
    /// user databases are captured in a BIN entry before `CkptEnd` is written.
    /// JE walks a single env-wide `INList` that covers all databases;
    /// Noxu achieves the same effect by iterating this registry.
    ///
    /// `None` until `with_db_trees_registry` is called (unit tests without a
    /// full environment).
    db_trees_registry:
        Option<Arc<std::sync::Mutex<HashMap<i64, Arc<RwLock<Tree>>>>>>,
    /// Bytes written to the log since the last checkpoint.
    ///
    /// Incremented by `wakeup_after_write()`. When this exceeds
    /// `checkpoint_bytes_interval` a checkpoint is triggered immediately.
    ///
    /// Write-byte accumulation.
    bytes_since_checkpoint: AtomicU64,
    /// Bytes-written threshold that triggers an immediate checkpoint.
    ///
    /// Default: 10 MiB (10 * 1024 * 1024).  Set to 0 to disable.
    ///
    /// REC-D: wired from `CHECKPOINTER_BYTES_INTERVAL` (default 20 MB) by the
    /// environment via `with_bytes_interval`. JE Checkpointer ctor:
    /// `logSizeBytesInterval = configManager.getLong(CHECKPOINTER_BYTES_INTERVAL)`.
    checkpoint_bytes_interval: u64,
    /// Time-based checkpoint interval in milliseconds (0 = time-based
    /// checkpoints disabled, bytes-only).
    ///
    /// REC-D: wired from `CHECKPOINTER_WAKEUP_INTERVAL` by the environment via
    /// `with_time_interval`. JE `getWakeupPeriod`: bytes-OR-time, with the
    /// byte interval taking precedence when non-zero. The daemon paces its
    /// own sleep at this interval; `is_runnable` consults it only when the
    /// byte interval is disabled (matches JE `isRunnable` useTimeInterval
    /// branch, which fires only when `logSizeBytesInterval == 0`).
    checkpoint_time_interval_ms: u64,
    /// Optional utilization tracker for persisting file summaries.
    ///
    /// When set, `persist_file_summaries()` iterates tracked summaries and
    /// writes `FileSummaryLN` WAL entries.
    utilization_tracker: Option<Arc<std::sync::Mutex<UtilizationTracker>>>,
    /// Optional cleaner reference for the post-checkpoint callback.
    ///
    /// After each successful `do_checkpoint`, the checkpointer calls
    /// `cleaner.after_checkpoint(&state)` to advance the three-state
    /// checkpoint barrier in `FileSelector`.  X-5 fix.
    cleaner: Option<Arc<noxu_cleaner::Cleaner>>,
    /// Optional transaction manager for T-F3/T-F4: first-active-LSN tracking.
    ///
    /// When `Some`, `do_checkpoint` queries `txn_manager.get_first_active_lsn()`
    /// and writes the result into `CkptEnd.first_active_lsn` instead of the
    /// conservative `Lsn::new(0,0)` full-scan sentinel.  This bounds the
    /// recovery scan to entries at or after the earliest active transaction's
    /// first logged LSN, reducing crash-recovery time.
    ///
    /// Safe only after Stage 1 (all user-database BINs are checkpointed);
    /// `None` for unit tests without a full environment.
    txn_manager: Option<Arc<TxnManager>>,
    /// REC-S: id sources read at checkpoint time to write the real last
    /// node/db/txn id values into `CheckpointEnd` (instead of zeros).
    ///
    /// JE `Checkpointer.doCheckpoint` writes `getLastLocalNodeId` /
    /// `getLastLocalDbId` / `getLastLocalTxnId` into the `CheckpointEnd`.
    /// `next_db_id` mirrors the env's db-id counter (last db-id = value-1);
    /// the last txn-id is read from `txn_manager.get_last_local_txn_id()`;
    /// the last node-id comes from the single tree-wide node counter
    /// (`noxu_tree::peek_next_node_id_counter`, L-30).  `None` keeps the old
    /// zero behaviour for unit tests without a full environment.
    next_db_id: Option<Arc<std::sync::atomic::AtomicI64>>,
}

impl Checkpointer {
    /// Create a new Checkpointer.
    ///
    /// # Arguments
    /// * `config` - Checkpoint configuration
    pub fn new(config: CheckpointConfig) -> Self {
        Self {
            stats: Arc::new(CheckpointStats::new()),
            next_checkpoint_id: AtomicU64::new(1),
            dirty_map: Mutex::new(DirtyINMap::new()),
            last_checkpoint_start: Mutex::new(noxu_util::NULL_LSN),
            last_checkpoint_end: Mutex::new(noxu_util::NULL_LSN),
            checkpoint_in_progress: AtomicBool::new(false),
            checkpoint_flush_levels: std::sync::Mutex::new(HashMap::new()),
            shutdown: AtomicBool::new(false),
            shutdown_condvar: Condvar::new(),
            shutdown_mutex: std::sync::Mutex::new(false),
            config,
            log_manager: None,
            tree: None,
            db_id: 0,
            db_trees_registry: None,
            bytes_since_checkpoint: AtomicU64::new(0),
            checkpoint_bytes_interval: 10 * 1024 * 1024, // 10 MiB default
            checkpoint_time_interval_ms: 0, // time-based disabled by default
            utilization_tracker: None,
            cleaner: None,
            txn_manager: None,
            next_db_id: None,
        }
    }

    /// Set the bytes-written threshold that triggers an immediate checkpoint.
    ///
    ///
    pub fn with_bytes_interval(mut self, bytes: u64) -> Self {
        self.checkpoint_bytes_interval = bytes;
        self
    }

    /// Set the time-based checkpoint interval (milliseconds).
    ///
    /// REC-D: wired from `CHECKPOINTER_WAKEUP_INTERVAL`. JE `getWakeupPeriod`
    /// computes bytes-OR-time with bytes taking precedence; `isRunnable`
    /// consults the time interval only when the byte interval is 0.
    pub fn with_time_interval(mut self, millis: u64) -> Self {
        self.checkpoint_time_interval_ms = millis;
        self
    }

    /// Attach a LogManager so that `do_checkpoint` writes real WAL entries.
    ///
    /// Call this before invoking `do_checkpoint` when a writable log is
    /// available (i.e. from `EnvironmentImpl`).
    pub fn with_log_manager(mut self, lm: Arc<LogManager>) -> Self {
        self.log_manager = Some(lm);
        self
    }

    /// Attach a Tree so that `do_checkpoint` flushes dirty BINs in step 4.
    ///
    /// `db_id` is the database ID passed to `Tree::collect_dirty_bins()`.
    /// `Checkpointer` receiving the environment's tree reference.
    pub fn with_tree(mut self, tree: Arc<RwLock<Tree>>, db_id: u64) -> Self {
        self.tree = Some(tree);
        self.db_id = db_id;
        self
    }

    /// Wire the env-wide db-tree registry so the checkpointer flushes ALL
    /// user-database dirty BINs, not just the primary tree.
    ///
    /// This is the Stage-1 fix: JE's `Checkpointer.processINList` walks a
    /// single env-wide `INList` covering all databases.  Noxu achieves the
    /// same effect by iterating `db_trees_registry` and flushing each tree.
    pub fn with_db_trees_registry(
        mut self,
        registry: Arc<std::sync::Mutex<HashMap<i64, Arc<RwLock<Tree>>>>>,
    ) -> Self {
        self.db_trees_registry = Some(registry);
        self
    }

    /// Attach a UtilizationTracker so that `persist_file_summaries()` writes
    /// real `FileSummaryLN` WAL entries during each checkpoint.
    ///
    /// `Checkpointer` receiving the environment's utilization tracker.
    pub fn with_utilization_tracker(
        mut self,
        tracker: Arc<std::sync::Mutex<UtilizationTracker>>,
    ) -> Self {
        self.utilization_tracker = Some(tracker);
        self
    }

    /// Wire a cleaner so that `do_checkpoint` calls
    /// `cleaner.after_checkpoint()` after a successful checkpoint.
    ///
    /// This is the X-5 fix: it activates the three-state checkpoint barrier
    /// (`cleaned → checkpointed → safe_to_delete`) in `FileSelector` so that
    /// log files are only deleted after their migrations have been captured by
    /// two successive checkpoints.
    pub fn with_cleaner(mut self, cleaner: Arc<noxu_cleaner::Cleaner>) -> Self {
        self.cleaner = Some(cleaner);
        self
    }

    /// Wire the transaction manager so `do_checkpoint` can compute the real
    /// `first_active_lsn` for `CkptEnd` (T-F3/T-F4).
    ///
    /// Safe to call only after Stage 1 (user-database BINs are checkpointed);
    /// before Stage 1 a non-zero `first_active_lsn` would cause recovery to
    /// skip committed LNs not captured in any BIN.
    pub fn with_txn_manager(mut self, txn_manager: Arc<TxnManager>) -> Self {
        self.txn_manager = Some(txn_manager);
        self
    }

    /// REC-S: wire the env's db-id counter so `do_checkpoint` writes the real
    /// last node/db/txn id values into `CheckpointEnd` instead of zeros.
    ///
    /// `next_db_id` is the env's `AtomicI64` (last allocated db-id =
    /// `next_db_id - 1`).  The last txn-id is read from the wired
    /// `txn_manager`; the last node-id from the tree-wide node counter.
    ///
    /// JE `Checkpointer.doCheckpoint` reads `envImpl.getNodeSequence()
    /// .getLastLocalNodeId()`, `getDbTree().getLastLocalDbId()`, and
    /// `getTxnManager().getLastLocalTxnId()` into the `CheckpointEnd`.
    pub fn with_id_sources(
        mut self,
        next_db_id: Arc<std::sync::atomic::AtomicI64>,
    ) -> Self {
        self.next_db_id = Some(next_db_id);
        self
    }

    /// Accumulate bytes written and trigger a checkpoint when the threshold
    /// is exceeded.
    ///
    /// Called after each WAL write from `EnvironmentImpl` (or LogManager) with
    /// the number of bytes appended.  When the running total exceeds
    /// `checkpoint_bytes_interval` the counter is reset and
    /// `do_checkpoint("wakeup")` is invoked synchronously.
    ///
    ///
    pub fn wakeup_after_write(&self, bytes: u64) {
        if self.checkpoint_bytes_interval == 0 {
            return;
        }
        let prev =
            self.bytes_since_checkpoint.fetch_add(bytes, Ordering::Relaxed);
        if prev + bytes >= self.checkpoint_bytes_interval {
            // Reset counter *before* triggering so parallel callers don't
            // all pile in at once — best-effort, not strictly once.
            self.bytes_since_checkpoint.store(0, Ordering::Relaxed);
            // Ignore errors: a concurrent checkpoint may be in progress.
            let _ = self.do_checkpoint("wakeup_after_write");
        }
    }

    /// Whether a periodic (daemon) checkpoint should run now (JE
    /// `Checkpointer.isRunnable`). Without this gate the daemon wrote a
    /// checkpoint on every wakeup tick even on a fully idle environment
    /// (wasted I/O). Returns true if:
    ///   - `force`, OR
    ///   - REC-F: the cleaner has files pending reclaim
    ///     (`needCheckpointForCleanedFiles()` → `isCheckpointNeeded()`), even
    ///     with no writes — so an idle env still reclaims cleaned files, OR
    ///   - bytes written since the last checkpoint >= the byte interval, OR
    ///   - (only when the byte interval is disabled) the time interval elapsed
    ///     AND something was written since the last checkpoint
    ///     (`bytes_since_checkpoint > 0` — JE's `lastUsedLsn !=
    ///     lastCheckpointEnd` idle-guard).
    ///
    /// JE ref: `Checkpointer.isRunnable` — order is force, then
    /// `wakeupAfterNoWrites && needCheckpointForCleanedFiles()`, then the
    /// bytes-OR-time interval (bytes takes precedence; the time branch only
    /// runs when `logSizeBytesInterval == 0`).
    pub fn is_runnable(&self, force: bool) -> bool {
        if force {
            return true;
        }
        // REC-F: wake for cleaner-pending files even on an idle environment.
        // JE `isRunnable`: `if (wakeupAfterNoWrites && needCheckpointForCleanedFiles())
        // return true;`.  Noxu folds `wakeupAfterNoWrites` into the cleaner
        // query directly — `needs_checkpoint_for_cleaned_files()` is true iff
        // the cleaner reports CLEANED/FULLY_PROCESSED files pending reclaim.
        if self.needs_checkpoint_for_cleaned_files() {
            return true;
        }
        let bytes_since = self.bytes_since_checkpoint.load(Ordering::Relaxed);
        if self.checkpoint_bytes_interval != 0 {
            // Bytes interval takes precedence (JE getWakeupPeriod): when it is
            // non-zero the time branch is never consulted.
            return bytes_since >= self.checkpoint_bytes_interval;
        }
        // Time-cadence branch (only reached when the byte interval is 0): the
        // caller (the daemon) only invokes this once per wakeup interval, so
        // reaching here means the time interval has elapsed. JE's idle-guard
        // (`lastUsedLsn != lastCheckpointEnd`) maps to "something was written
        // since the last checkpoint" — i.e. bytes_since > 0. Skip the
        // checkpoint entirely on an idle environment.
        bytes_since > 0
    }

    /// REC-F: whether the cleaner has files pending reclaim that a checkpoint
    /// would unblock.  Mirrors JE `Checkpointer.needCheckpointForCleanedFiles`
    /// → `cleaner.getFileSelector().isCheckpointNeeded()` (any CLEANED or
    /// FULLY_PROCESSED files exist).  Returns `false` when no cleaner is
    /// wired.
    fn needs_checkpoint_for_cleaned_files(&self) -> bool {
        self.cleaner.as_ref().map(|c| c.is_checkpoint_needed()).unwrap_or(false)
    }

    /// Test-only: bump `bytes_since_checkpoint` without triggering a
    /// checkpoint (wakeup_after_write would fire do_checkpoint at the
    /// threshold). Used to exercise `is_runnable`.
    #[cfg(test)]
    pub fn note_bytes_for_test(&self, bytes: u64) {
        self.bytes_since_checkpoint.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Returns `true` if the given BIN node has been checkpointed at least
    /// once (its `last_full_lsn` is not NULL_LSN).
    ///
    /// The evictor calls this before evicting a node: a node that has never
    /// been checkpointed would be lost on eviction because it has no on-disk
    /// representation yet.
    ///
    ///
    pub fn is_checkpointed(node: &NodeRwLock<TreeNode>) -> bool {
        let guard = node.read();
        match &*guard {
            TreeNode::Bottom(b) => b.last_full_lsn != NULL_LSN,
            // Non-BIN internal nodes are always considered checkpointed for
            // eviction purposes (they are reconstructed from their children).
            _ => true,
        }
    }

    /// Persist file utilization summaries to the WAL.
    ///
    /// Writes a `FileSummaryLN` log entry for each tracked file summary so
    /// that utilization data survives a restart.
    ///
    ///
    ///
    /// Requires both a `LogManager` (via `with_log_manager`) and a
    /// `UtilizationTracker` (via `with_utilization_tracker`) to be wired.
    /// Returns `Ok(())` without writing if either is absent.
    pub fn persist_file_summaries(&self) -> Result<()> {
        let (Some(lm), Some(tracker_lock)) =
            (&self.log_manager, &self.utilization_tracker)
        else {
            return Ok(());
        };

        let tracker = tracker_lock.lock().unwrap_or_else(|e| e.into_inner());
        let tracked_files = tracker.get_tracked_files();
        if tracked_files.is_empty() {
            return Ok(());
        }

        for (file_number, tracked) in tracked_files {
            let summary = tracked.get_summary();
            // C7: persist the full FileSummary breakdown (LN/IN totals +
            // obsolete + maxLNSize) AND the packed obsolete-offset list, so
            // the on-disk FileSummaryLN is as faithful as the in-memory
            // TrackedFileSummary.  JE: FileSummaryLN.writeToLog ->
            // baseSummary.writeToLog (11 ints) + obsoleteOffsets.writeToLog.
            let mut packed = noxu_cleaner::PackedOffsets::new();
            packed.pack(tracked.get_obsolete_offsets());
            let entry = FileSummaryLnEntry::new(
                *file_number as u64,
                summary.total_count,
                summary.total_size,
                summary.total_in_count,
                summary.total_in_size,
                summary.total_ln_count,
                summary.total_ln_size,
                summary.max_ln_size,
                summary.obsolete_in_count,
                summary.obsolete_ln_count,
                summary.obsolete_ln_size,
                summary.obsolete_ln_size_counted,
                packed.get_count() as u32,
                packed.get_data().to_vec(),
            );
            let mut buf = bytes::BytesMut::with_capacity(entry.log_size());
            entry.write_to_log(&mut buf);
            lm.log(
                LogEntryType::FileSummaryLN,
                &buf,
                Provisional::No,
                false,
                false,
            )
            .map_err(|e| {
                RecoveryError::CheckpointError(format!(
                    "persist_file_summaries log write failed: {e}"
                ))
            })?;
            log::debug!(
                "persist_file_summaries: wrote FileSummaryLN for file {}",
                file_number
            );
        }
        Ok(())
    }

    /// Perform a checkpoint.
    pub fn do_checkpoint(&self, invoker: &str) -> Result<CheckpointResult> {
        // Check if shutdown
        if self.shutdown.load(Ordering::Acquire) {
            return Err(RecoveryError::CheckpointError(
                "Checkpointer has been shut down".to_string(),
            ));
        }

        // Check if already in progress
        if self
            .checkpoint_in_progress
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(RecoveryError::CheckpointError(
                "Checkpoint already in progress".to_string(),
            ));
        }

        let start_time = std::time::Instant::now();

        // Ensure we clear the in-progress flag (and flush_levels map) on exit.
        let _guard = CheckpointGuard {
            flag: &self.checkpoint_in_progress,
            flush_levels: &self.checkpoint_flush_levels,
        };

        // Step 1: Generate checkpoint ID
        let checkpoint_id =
            self.next_checkpoint_id.fetch_add(1, Ordering::SeqCst);

        // X-5: snapshot the cleaner's "cleaned" file set at checkpoint START
        // (before we write CkptStart) so we know which files were in the
        // cleaned state when this checkpoint began.  Passed to
        // `after_checkpoint` at the end of this function.
        let cleaner_state =
            self.cleaner.as_ref().map(|c| c.get_checkpoint_start_state());

        // Step 2: Write CkptStart entry to WAL (or synthesise a fake LSN when
        // no LogManager is wired — used by unit tests that don't need I/O).
        let start_lsn = if let Some(lm) = &self.log_manager {
            let ckpt_start = CheckpointStart::new(checkpoint_id, invoker);
            let mut buf = Vec::with_capacity(ckpt_start.log_size());
            ckpt_start.write_to_log(&mut buf).map_err(|e| {
                RecoveryError::CheckpointError(format!(
                    "CkptStart serialization failed: {e}"
                ))
            })?;
            lm.log(
                LogEntryType::CkptStart,
                &buf,
                Provisional::No,
                false, // flush_required
                false, // fsync_required
            )
            .map_err(|e| {
                RecoveryError::CheckpointError(format!(
                    "CkptStart WAL write failed: {e}"
                ))
            })?
        } else {
            // No LogManager attached — synthetic LSN so existing tests pass.
            Lsn::new(0, checkpoint_id as u32)
        };

        // Step 3: Build dirty IN map
        let mut dirty_map = self.dirty_map.lock();
        dirty_map.clear();
        drop(dirty_map);

        // Step 4a: Flush dirty BINs.
        //
        // For each dirty BIN in the tree decide — using TREE_BIN_DELTA
        // threshold of 25 % — whether to write a BINDelta or a full BIN.
        //
        // `Checkpointer.processINList()` + `logIN()` (BIN path).
        let mut flush_result = self.flush_dirty_bins_internal()?;

        // Step 4b: Flush dirty upper INs (level ≥ 2) bottom-up.
        //
        // After BINs are written their parent INs are dirtied by splits.
        // These must be logged before CkptEnd to make the checkpoint complete.
        // Intermediate levels use Provisional::Yes (subsumed by root);
        // the root level uses Provisional::No (anchors the checkpoint).
        //
        // `Checkpointer.processINList()` upper-IN loop +
        // `Checkpointer.logIN()` for non-BIN nodes.
        let upper_result = self.flush_upper_ins_internal()?;
        flush_result.full_ins_flushed += upper_result.full_ins_flushed;

        // Step 5: Write CkptEnd entry to WAL.
        //
        // T-F3 is NOT yet active: first_active_lsn stays Lsn::new(0,0) (full
        // scan from start of log).  Setting a non-zero first_active_lsn would
        // bound the recovery scan — but that requires pre-loading BINs from
        // the checkpoint into the recovery tree before replaying LNs (P-2
        // BIN-preload infrastructure).  Without P-2, starting from any LSN
        // other than 0 silently drops pre-checkpoint committed LNs.
        //
        // Stage 2 wires T-F4 (update_first_lsn is called on first txn write,
        // get_first_active_lsn() now returns a real LSN), but the consumer
        // (T-F3 scan bounding) is deferred until P-2 lands.
        //
        // Backward compat: Lsn::new(0,0) tells recovery to full-scan from
        // the start, which is correct and was always the behaviour.
        let first_active_lsn: noxu_util::Lsn = noxu_util::Lsn::new(0, 0);
        // (T-F4: txn_manager is wired; get_first_active_lsn() returns real
        // LSN for future P-2 use; suppress unused warning.)
        let _ = &self.txn_manager;

        // REC-S: read the env's current last node/db/txn ids and write the
        // REAL values into CheckpointEnd (instead of the old hardcoded zeros)
        // so recovery folds them into use_max_* and the env seeds its
        // sequences past them on restart.  JE Checkpointer.doCheckpoint writes
        // getLastLocalNodeId / getLastLocalDbId / getLastLocalTxnId.
        //   - last node-id: the tree-wide node counter (L-30); the next id to
        //     be handed out is `peek_next_node_id_counter()`, so the last
        //     allocated id is that minus 1 (saturating).
        //   - last db-id: the env's next_db_id minus 1.
        //   - last txn-id: txn_manager.get_last_local_txn_id().
        let last_local_node_id: u64 =
            noxu_tree::tree::peek_next_node_id_counter().saturating_sub(1);
        let last_local_db_id: u64 = self
            .next_db_id
            .as_ref()
            .map(|n| {
                n.load(std::sync::atomic::Ordering::Relaxed).saturating_sub(1)
                    as u64
            })
            .unwrap_or(0);
        let last_local_txn_id: u64 = self
            .txn_manager
            .as_ref()
            .map(|t| t.get_last_local_txn_id().max(0) as u64)
            .unwrap_or(0);

        let end_lsn = if let Some(lm) = &self.log_manager {
            let ckpt_end = CheckpointEnd::new(
                checkpoint_id,
                invoker,
                start_lsn,
                // REC-P / REC-B: root_lsn is intentionally always None.  JE
                // records the mapping-tree root here (Checkpointer.flushRoot
                // → CheckpointEnd.rootLsn), but Noxu's catalog is an in-memory
                // HashMap rebuilt from NameLN WAL entries during recovery
                // (REC-B authorized divergence), so there is no mapping tree
                // to flush and no root LSN to record.  Per-DB utilization is
                // persisted via persist_file_summaries (FileSummaryLN), not a
                // mapping-tree MapLN flush.
                None, // root_lsn
                first_active_lsn,
                // REC-S: real id maxima (were hardcoded 0).
                last_local_node_id,
                0, // last_replicated_node_id (HA: deferred)
                last_local_db_id,
                0, // last_replicated_db_id (HA: deferred)
                last_local_txn_id,
                0,     // last_replicated_txn_id (HA: deferred)
                false, // cleaned_files_to_delete
            );
            let mut buf = Vec::with_capacity(ckpt_end.log_size());
            ckpt_end.write_to_log(&mut buf).map_err(|e| {
                RecoveryError::CheckpointError(format!(
                    "CkptEnd serialization failed: {e}"
                ))
            })?;
            lm.log(
                LogEntryType::CkptEnd,
                &buf,
                Provisional::No,
                true, // flush_required
                // REC-F1: fsync the CkptEnd entry before returning.  JE
                // Checkpointer.doCheckpoint (~line 895):
                //   lastCheckpointEnd = logManager.logForceFlush(
                //       endEntry, true /*fsyncRequired*/, ...);
                // "We must flush and fsync to ensure that cleaned files are
                // not referenced. This also ensures that this checkpoint is
                // not wasted if we crash."  The fsync MUST precede the
                // cleaner.after_checkpoint() barrier advance below (and JE
                // fsyncs inside doCheckpoint before
                // updateFilesAtCheckpointEnd), so ALL callers — close,
                // daemon, and bytes-triggered wakeup_after_write — get a
                // durable CkptEnd, not just close.
                true, // fsync_required
            )
            .map_err(|e| {
                RecoveryError::CheckpointError(format!(
                    "CkptEnd WAL write failed: {e}"
                ))
            })?
        } else {
            // No LogManager attached — synthetic LSN so existing tests pass.
            Lsn::new(0, (checkpoint_id as u32) + 1)
        };

        // Step 6: Update statistics
        *self.last_checkpoint_start.lock() = start_lsn;
        *self.last_checkpoint_end.lock() = end_lsn;
        // Reset the runnable-gate state: bytes written since this checkpoint.
        self.bytes_since_checkpoint.store(0, Ordering::Relaxed);

        let elapsed_ms = start_time.elapsed().as_millis() as u64;

        self.stats.checkpoints.fetch_add(1, Ordering::Relaxed);
        self.stats
            .full_in_flush
            .fetch_add(flush_result.full_ins_flushed, Ordering::Relaxed);
        self.stats
            .full_bin_flush
            .fetch_add(flush_result.full_bins_flushed, Ordering::Relaxed);
        self.stats
            .delta_in_flush
            .fetch_add(flush_result.delta_ins_flushed, Ordering::Relaxed);
        self.stats.last_ckpt_id.store(checkpoint_id, Ordering::Relaxed);
        self.stats.last_ckpt_start.store(start_lsn.as_u64(), Ordering::Relaxed);
        self.stats.last_ckpt_end.store(end_lsn.as_u64(), Ordering::Relaxed);
        self.stats.last_ckpt_interval.store(elapsed_ms, Ordering::Relaxed);

        // X-5: advance the cleaner's three-state checkpoint barrier now that
        // a checkpoint has successfully completed.  Cleaned files that were
        // snapshotted at checkpoint-start (`cleaner_state`) move to
        // `checkpointed`; previously-checkpointed files move to
        // `safe_to_delete` and will be removed on the next `delete_safe_files`
        // call.
        if let (Some(cleaner), Some(state)) = (&self.cleaner, cleaner_state) {
            cleaner.after_checkpoint(&state);
        }

        Ok(CheckpointResult {
            checkpoint_id,
            start_lsn,
            end_lsn,
            full_ins_flushed: flush_result.full_ins_flushed,
            full_bins_flushed: flush_result.full_bins_flushed,
            delta_ins_flushed: flush_result.delta_ins_flushed,
            elapsed_ms,
        })
    }

    /// Get the LSN of the last checkpoint start.
    pub fn get_last_checkpoint_start(&self) -> Lsn {
        *self.last_checkpoint_start.lock()
    }

    /// Get the LSN of the last checkpoint end.
    pub fn get_last_checkpoint_end(&self) -> Lsn {
        *self.last_checkpoint_end.lock()
    }

    /// Check if a checkpoint is currently in progress.
    pub fn is_checkpoint_in_progress(&self) -> bool {
        self.checkpoint_in_progress.load(Ordering::Acquire)
    }

    /// Choose the [`Provisional`] flag for a node being evicted by the evictor.
    ///
    /// Returns `Provisional::Yes` when a checkpoint is in progress **and** the
    /// node's level is strictly below the **tree-specific** highest flush level
    /// for `db_id` (meaning the checkpoint will write a non-provisional ancestor
    /// for that tree that subsumes this entry).  Returns `Provisional::No` if
    /// no checkpoint is in progress, or if `db_id` has no dirty upper INs in
    /// this checkpoint (level absent from map → 0 → not covered).
    ///
    /// # JE reference
    /// `Checkpointer.coordinateEvictionWithCheckpoint` →
    /// `DirtyINMap.coordinateEvictionWithCheckpoint` which calls
    /// `getHighestFlushLevel(db)` — **per-`DatabaseImpl`** lookup.  If the db
    /// is absent from `highestFlushLevels`, `getHighestFlushLevel` returns
    /// `IN.MIN_LEVEL` (≤ 0) making the comparison false → `Provisional::NO`.
    ///
    /// # CC-4 residual
    /// The prior implementation stored a single global max-level (`AtomicI32`)
    /// that was the maximum across ALL trees.  A BIN evicted from tree A (no
    /// dirty upper INs) got `Provisional::Yes` because tree B's level was
    /// non-zero, but NO non-provisional ancestor was written for tree A →
    /// recovery discards the provisional BIN → data loss on crash before the
    /// next checkpoint.  Per-tree lookup (this method) fixes that: tree A's
    /// level is absent → 0 → `Provisional::No` (authoritative log entry).
    ///
    /// # Race window
    /// Same benign race as JE: if the checkpoint finishes between the
    /// `in_progress` read and the log write, the BIN may be logged
    /// `Provisional::Yes` without a covering ancestor in *this* checkpoint, but
    /// the next checkpoint will cover it.  Logging `Yes` without strict need is
    /// safe (log bloat only); the reverse is what causes recovery inconsistency.
    pub fn get_eviction_provisional(
        &self,
        db_id: u64,
        node_level: i32,
    ) -> Provisional {
        if !self.checkpoint_in_progress.load(Ordering::Acquire) {
            return Provisional::No;
        }
        // Look up this tree's flush level.  Missing entry means no dirty upper
        // INs → level 0 → condition false → Provisional::No.
        let max_flush = self
            .checkpoint_flush_levels
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&db_id)
            .copied()
            .unwrap_or(0);
        if max_flush > 0 && node_level < max_flush {
            Provisional::Yes
        } else {
            Provisional::No
        }
    }

    pub fn get_stats(&self) -> Arc<CheckpointStats> {
        Arc::clone(&self.stats)
    }

    /// Get the configuration.
    pub fn get_config(&self) -> &CheckpointConfig {
        &self.config
    }

    /// Request shutdown of the checkpointer.
    ///
    /// Sets the shutdown flag AND wakes up the daemon thread so it exits
    /// immediately without waiting the full sleep interval.
    pub fn request_shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
        // Wake up any thread sleeping in wait_for_shutdown_or_timeout().
        if let Ok(mut guard) = self.shutdown_mutex.lock() {
            *guard = true;
        }
        self.shutdown_condvar.notify_all();
    }

    /// Check if shutdown has been requested.
    pub fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::Acquire)
    }

    /// Sleep for `duration` or until `request_shutdown()` is called.
    ///
    /// Used by the daemon thread in `EnvironmentImpl` instead of
    /// `thread::sleep()` so that shutdown is immediate.
    pub fn wait_for_shutdown_or_timeout(&self, duration: std::time::Duration) {
        if let Ok(guard) = self.shutdown_mutex.lock() {
            // wait_timeout returns immediately when the condvar is notified.
            let _ = self.shutdown_condvar.wait_timeout(guard, duration);
        }
    }

    /// Get the next checkpoint ID (without incrementing).
    pub fn peek_next_checkpoint_id(&self) -> u64 {
        self.next_checkpoint_id.load(Ordering::SeqCst)
    }

    /// REC-G: seed the checkpoint-interval baselines from a recovered
    /// checkpoint, so the FIRST post-recovery checkpoint interval is measured
    /// from the recovered `CkptEnd` rather than from process start.
    ///
    /// Without this, `last_checkpoint_start`/`_end` start at `NULL_LSN` and
    /// `bytes_since_checkpoint` at 0 after recovery, so the bytes/time gate
    /// would treat all log written before the crash as "since the last
    /// checkpoint" — firing a redundant checkpoint immediately, or (for the
    /// time branch) measuring the interval from the wrong baseline.
    ///
    /// JE ref: `Checkpointer.initIntervals(lastCheckpointStart,
    /// lastCheckpointEnd, lastCheckpointMillis)` — called from
    /// `RecoveryManager.recover()` after the recovery scan completes.  Noxu
    /// passes the recovered `checkpoint_start_lsn` / `checkpoint_end_lsn`
    /// (NULL_LSN when the log had no prior checkpoint, matching JE).
    pub fn init_intervals(
        &self,
        last_checkpoint_start: Lsn,
        last_checkpoint_end: Lsn,
    ) {
        *self.last_checkpoint_start.lock() = last_checkpoint_start;
        *self.last_checkpoint_end.lock() = last_checkpoint_end;
        // A freshly-recovered environment has written nothing since the
        // recovered checkpoint; reset the byte accumulator so the gate does
        // not immediately fire on pre-crash log volume.
        self.bytes_since_checkpoint.store(0, Ordering::Relaxed);
    }

    /// REC-H: continue the checkpoint-ID sequence after recovery instead of
    /// restarting at 1.  The next checkpoint will use `last_checkpoint_id + 1`.
    ///
    /// The ID is a debug/log tag (not a correctness key), but it should not
    /// regress or collide across restarts.  Seeded from the recovered
    /// `CkptEnd.id`.
    ///
    /// JE ref: `Checkpointer.setCheckpointId(lastCheckpointId)` — "can only be
    /// done after recovery"; JE stores `checkpointId = lastCheckpointId` and
    /// `incrementProgress`/`generateCheckpointId` advances from there.  Noxu's
    /// `do_checkpoint` does `fetch_add(1)`, so we seed `next_checkpoint_id =
    /// last_checkpoint_id + 1` to make the next emitted ID strictly greater.
    pub fn set_checkpoint_id(&self, last_checkpoint_id: u64) {
        self.next_checkpoint_id.store(last_checkpoint_id + 1, Ordering::SeqCst);
    }

    /// Flush all dirty BINs to the log (public, unit-result API).
    ///
    /// Calls the internal flush logic and discards the detailed `FlushResult`,
    /// returning only success/failure.  Use this from external callers (e.g.
    /// daemon threads) that do not need per-BIN counts.
    ///
    /// `Checkpointer.doCheckpoint()` partial flush path.
    pub fn flush_dirty_bins(&self) -> Result<()> {
        self.flush_dirty_bins_internal().map(|_| ())
    }

    /// Internal flush all dirty BINs to the log.
    ///
    /// Flushes dirty BINs from `self.tree` (primary tree) AND from every
    /// tree in `self.db_trees_registry` (user databases).
    ///
    /// JE `Checkpointer.processINList` walks a single env-wide `INList`
    /// covering all databases; Noxu achieves the same effect by iterating the
    /// `db_trees_registry` and calling the per-tree BIN-flush logic for each.
    ///
    /// For each dirty BIN `BinStub::should_log_delta(bin_delta_percent)`
    /// (faithful JE `BIN.shouldLogDelta`, BIN.java:1892) decides:
    /// - delta-slot count `<= nEntries * bin_delta_percent / 100` (and no
    ///   prohibit / a prior full exists) → write `BINDelta` entry (delta path)
    /// - otherwise                  → write full `BIN` entry (full path)
    ///
    /// Also calls `persist_file_summaries()` to ensure utilization data is
    /// durable.
    ///
    /// `Checkpointer.processINList()` + `Checkpointer.logIN()`.
    pub(crate) fn flush_dirty_bins_internal(&self) -> Result<FlushResult> {
        let mut result = FlushResult::default();

        let lm = match &self.log_manager {
            Some(lm) => lm,
            // No log manager — nothing to flush (unit tests).
            None => return Ok(result),
        };

        // Stage-1: flush the primary tree (if wired) then every user-database
        // tree from the registry.  JE's equivalent is processINList walking
        // the single env-wide INList that covers all databases.
        //
        // IMPORTANT: the primary_tree (self.tree, db_id=1) and the user-database
        // real_tree for db_id=1 are DIFFERENT Arc<RwLock<Tree>> objects.  The
        // primary_tree is used by the cleaner for LN migration but is never
        // written by user operations.  User data lives in the real_trees stored
        // in db_trees_registry.  We flush both: primary_tree first (harmless
        // if empty), then all registry trees (where user data lives).
        // No skip guard — the registry trees are always distinct objects from
        // self.tree even when their db_id happens to match self.db_id.
        let mut trees_to_flush: Vec<(u64, Arc<RwLock<Tree>>)> = Vec::new();
        if let Some(t) = &self.tree {
            trees_to_flush.push((self.db_id, Arc::clone(t)));
        }
        if let Some(reg) = &self.db_trees_registry
            && let Ok(guard) = reg.lock()
        {
            for (&db_id_i64, tree_arc) in guard.iter() {
                let db_id = db_id_i64 as u64;
                trees_to_flush.push((db_id, Arc::clone(tree_arc)));
            }
        }

        for (db_id, tree_arc) in trees_to_flush {
            let r = Self::flush_one_tree_bins(
                db_id,
                &tree_arc,
                lm,
                self.config.bin_delta_percent,
            )?;
            result.full_bins_flushed += r.full_bins_flushed;
            result.delta_ins_flushed += r.delta_ins_flushed;
        }

        // Persist file utilization summaries so they survive restarts.
        self.persist_file_summaries()?;

        Ok(result)
    }

    /// Flush dirty BINs for a single tree to the WAL.
    ///
    /// Extracted so both `flush_dirty_bins_internal` (primary tree) and the
    /// per-user-database loop can share the same logic without duplicating the
    /// TREE_BIN_DELTA decision or the X-8 early-exit guard.
    fn flush_one_tree_bins(
        db_id: u64,
        tree_arc: &Arc<RwLock<Tree>>,
        lm: &Arc<LogManager>,
        bin_delta_percent: i32,
    ) -> Result<FlushResult> {
        let mut result = FlushResult::default();

        // Collect dirty BINs under a read lock on the tree.
        let dirty_bins = {
            let tree_guard = tree_arc.read().map_err(|_| {
                RecoveryError::CheckpointError(
                    "tree lock poisoned during checkpoint".to_string(),
                )
            })?;
            tree_guard.collect_dirty_bins(db_id)
        };

        // The delta-vs-full decision per BIN is made by
        // `BinStub::should_log_delta(bin_delta_percent)` below — faithful JE
        // `BIN.shouldLogDelta` (count-based + configurable percent).

        for (_node_db_id, bin_arc) in dirty_bins {
            // Acquire write lock to serialize + clear dirty flags.
            let mut bin_guard = bin_arc.write();

            let b = match &mut *bin_guard {
                TreeNode::Bottom(b) => b,
                _ => continue, // not a BIN (defensive)
            };

            let dirty = b.dirty_count();

            // X-8: skip nodes that the evictor already flushed and cleared
            // between our dirty-BIN snapshot (under tree read lock) and the
            // per-node write-lock acquisition.
            if !b.dirty && dirty == 0 {
                continue;
            }

            // TREE_BIN_DELTA decision — faithful JE `BIN.shouldLogDelta`
            // (BIN.java:1892): COUNT-based (numDeltas = dirty slots) against the
            // CONFIGURABLE percent limit, with the isBINDelta fast path, the
            // numDeltas<=0 guard, and the isDeltaProhibited / lastFullLsn==NULL
            // bound — all encapsulated in `BinStub::should_log_delta`.
            let use_delta = b.should_log_delta(bin_delta_percent);

            if use_delta {
                // --- BIN-delta path ---
                let delta_bytes = b.serialize_delta();
                let entry = BinDeltaLogEntry::new(
                    db_id,
                    b.last_full_lsn,
                    b.last_delta_lsn, // prev_delta_lsn
                    delta_bytes,
                );
                let mut buf = bytes::BytesMut::with_capacity(entry.log_size());
                entry.write_to_log(&mut buf);
                let delta_logged_lsn = lm
                    .log(
                        LogEntryType::BINDelta,
                        &buf,
                        Provisional::No,
                        false, // flush_required
                        false, // fsync_required — fsync at CkptEnd
                    )
                    .map_err(|e| {
                        RecoveryError::CheckpointError(format!(
                            "BINDelta WAL write failed: {e}"
                        ))
                    })?;
                b.last_delta_lsn = delta_logged_lsn; // advance chain for next delta
                b.clear_dirty_after_delta_log();
                result.delta_ins_flushed += 1;
            } else {
                // --- Full BIN path ---
                let full_bytes = b.serialize_full();
                let entry = InLogEntry::new(
                    db_id,
                    b.last_full_lsn,
                    NULL_LSN, // prev_delta_lsn
                    full_bytes,
                );
                let mut buf = bytes::BytesMut::with_capacity(entry.log_size());
                entry.write_to_log(&mut buf);
                let logged_lsn = lm
                    .log(
                        LogEntryType::BIN,
                        &buf,
                        Provisional::No,
                        false, // flush_required
                        false, // fsync_required — fsync at CkptEnd
                    )
                    .map_err(|e| {
                        RecoveryError::CheckpointError(format!(
                            "BIN WAL write failed: {e}"
                        ))
                    })?;
                b.last_delta_lsn = NULL_LSN; // full BIN resets delta chain
                b.clear_dirty_after_full_log(logged_lsn);
                result.full_bins_flushed += 1;
            }
        }

        Ok(result)
    }

    /// Flush all dirty upper INs (level ≥ 2) bottom-up to the WAL.
    ///
    /// Flushes upper INs from `self.tree` (primary tree) AND from every tree
    /// in `self.db_trees_registry` (user databases), mirroring
    /// `flush_dirty_bins_internal`'s all-trees iteration.
    ///
    /// `Checkpointer.processINList()` upper-IN pass +
    /// `Checkpointer.logIN()` for `TreeNode::Internal` nodes.
    fn flush_upper_ins_internal(&self) -> Result<FlushResult> {
        let mut result = FlushResult::default();

        let lm = match &self.log_manager {
            Some(lm) => lm,
            None => return Ok(result),
        };

        let mut trees_to_flush: Vec<(u64, Arc<RwLock<Tree>>)> = Vec::new();
        if let Some(t) = &self.tree {
            trees_to_flush.push((self.db_id, Arc::clone(t)));
        }
        if let Some(reg) = &self.db_trees_registry
            && let Ok(guard) = reg.lock()
        {
            for (&db_id_i64, tree_arc) in guard.iter() {
                let db_id = db_id_i64 as u64;
                // No skip guard: registry trees are distinct objects from self.tree.
                trees_to_flush.push((db_id, Arc::clone(tree_arc)));
            }
        }

        // CC-4 residual fix: compute the per-tree highest flush level before
        // any logging begins.  Populate checkpoint_flush_levels with one entry
        // per tree that has dirty upper INs.  Trees absent from the map have
        // no dirty upper INs → their BINs must NOT be logged Provisional::Yes.
        //
        // JE ref: DirtyINMap.highestFlushLevels (Map<DatabaseImpl, Integer>);
        // getHighestFlushLevel(db) returns IN.MIN_LEVEL (0) for absent keys,
        // making coordinateEvictionWithCheckpoint return Provisional.NO.
        //
        // Memory ordering: the map is populated inside the Mutex before the
        // first WAL write.  The evictor acquires the same Mutex to read it
        // (Mutex provides the necessary happens-before).  The RAII guard in
        // do_checkpoint clears the map via CheckpointGuard::drop.
        {
            let mut levels = self
                .checkpoint_flush_levels
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            levels.clear();
            for (db_id, tree_arc) in &trees_to_flush {
                // REC-AA: the recorded highest flush level is
                // `max(dirty-upper-IN-level) + 1`, bounded by the root level
                // — JE DirtyINMap.updateFlushLevels flushes at least one level
                // ABOVE the highest dirty node (`(ckptFlushExtraLevel || isBIN)
                // && !isRoot` → `level += 1`) so the lower level is logged
                // provisionally and recovery skips reprocessing it.  The `+1`
                // is bounded by the root level (`!isRoot` guard) so we never
                // claim to flush above the tree root — a node AT the root level
                // is the non-provisional anchor and must NOT itself be marked
                // coverable.
                let flush_level = tree_arc.read().ok().and_then(|guard| {
                    let dirty_ins = guard.collect_dirty_upper_ins(*db_id);
                    let max_dirty =
                        dirty_ins.iter().map(|(lvl, _)| *lvl).max()?;
                    // Root level bounds the +1.  The root is the
                    // highest-level resident node.
                    let root_level = guard
                        .get_root()
                        .map(|r| r.read().level())
                        .unwrap_or(max_dirty);
                    Some((max_dirty + 1).min(root_level))
                });
                if let Some(level) = flush_level
                    && level > 0
                {
                    levels.insert(*db_id, level);
                }
            }
        }

        for (db_id, tree_arc) in trees_to_flush {
            let r = Self::flush_one_tree_upper_ins(db_id, &tree_arc, lm)?;
            result.full_ins_flushed += r.full_ins_flushed;
        }

        Ok(result)
    }

    /// Flush dirty upper INs for a single tree to the WAL.
    fn flush_one_tree_upper_ins(
        db_id: u64,
        tree_arc: &Arc<RwLock<Tree>>,
        lm: &Arc<LogManager>,
    ) -> Result<FlushResult> {
        let mut result = FlushResult::default();

        // Collect dirty upper INs under a read lock.
        let dirty_ins = {
            let tree_guard = tree_arc.read().map_err(|_| {
                RecoveryError::CheckpointError(
                    "tree lock poisoned during upper-IN flush".to_string(),
                )
            })?;
            tree_guard.collect_dirty_upper_ins(db_id)
        };

        if dirty_ins.is_empty() {
            return Ok(result);
        }

        // The maximum level present is the root level; it must be logged
        // Provisional::No.  All others use Provisional::Yes.
        let max_level =
            dirty_ins.iter().map(|(lvl, _)| *lvl).max().unwrap_or(0);

        for (level, node_arc) in &dirty_ins {
            let mut node_guard = node_arc.write();

            if !node_guard.is_dirty() {
                continue; // may have been cleared by a concurrent checkpoint
            }

            // Serialize the upper IN using the existing `write_to_bytes()` path.
            let node_bytes = node_guard.write_to_bytes();
            let provisional = if *level == max_level {
                Provisional::No
            } else {
                Provisional::Yes
            };

            let entry = InLogEntry::new(
                db_id,
                noxu_util::NULL_LSN, // prev_full_lsn — no previous version tracking for upper INs yet
                noxu_util::NULL_LSN, // prev_delta_lsn
                node_bytes,
            );
            let mut buf = bytes::BytesMut::with_capacity(entry.log_size());
            entry.write_to_log(&mut buf);
            lm.log(
                LogEntryType::IN,
                &buf,
                provisional,
                false, // flush_required
                false, // fsync_required — fsync at CkptEnd
            )
            .map_err(|e| {
                RecoveryError::CheckpointError(format!(
                    "IN WAL write failed: {e}"
                ))
            })?;

            node_guard.set_dirty(false);
            result.full_ins_flushed += 1;
        }

        Ok(result)
    }
}

/// RAII guard to ensure `checkpoint_in_progress` and `checkpoint_flush_levels`
/// are cleared when the checkpoint finishes or is abandoned.
///
/// CC-4 residual: `flush_levels` must be cleared so the evictor stops
/// returning `Provisional::Yes` for any tree after the checkpoint ends.
struct CheckpointGuard<'a> {
    flag: &'a AtomicBool,
    flush_levels: &'a std::sync::Mutex<HashMap<u64, i32>>,
}

impl<'a> Drop for CheckpointGuard<'a> {
    fn drop(&mut self) {
        // Clear per-tree flush levels before clearing the in_progress flag.
        // An evictor that reads in_progress=true will still see the (stale)
        // map; once in_progress goes false the map contents are irrelevant.
        if let Ok(mut levels) = self.flush_levels.lock() {
            levels.clear();
        }
        self.flag.store(false, Ordering::Release);
    }
}

/// Internal struct for tracking flush results.
#[derive(Debug, Default)]
pub(crate) struct FlushResult {
    full_ins_flushed: u64,
    full_bins_flushed: u64,
    delta_ins_flushed: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_checkpoint_config_default() {
        let config = CheckpointConfig::default();
        assert!(!config.force);
        assert!(!config.minimize_recovery_time);
        assert_eq!(config.bytes_interval, 20_000_000);
        assert_eq!(config.time_interval, 0);
    }

    #[test]
    fn test_checkpoint_config_builder() {
        let config = CheckpointConfig::new()
            .force(true)
            .minimize_recovery_time(true)
            .bytes_interval(10_000_000)
            .time_interval(5000);
        assert!(config.force);
        assert!(config.minimize_recovery_time);
        assert_eq!(config.bytes_interval, 10_000_000);
        assert_eq!(config.time_interval, 5000);
    }

    #[test]
    fn test_checkpoint_result() {
        let result = CheckpointResult {
            checkpoint_id: 42,
            start_lsn: Lsn::new(1, 100),
            end_lsn: Lsn::new(1, 200),
            full_ins_flushed: 10,
            full_bins_flushed: 20,
            delta_ins_flushed: 5,
            elapsed_ms: 250,
        };
        assert_eq!(result.checkpoint_id, 42);
        assert_eq!(result.total_nodes_flushed(), 35);
    }

    #[test]
    fn test_checkpointer_new() {
        let checkpointer = Checkpointer::new(CheckpointConfig::default());
        assert!(!checkpointer.is_checkpoint_in_progress());
        assert!(!checkpointer.is_shutdown());
        assert_eq!(checkpointer.peek_next_checkpoint_id(), 1);
        assert_eq!(
            checkpointer.get_last_checkpoint_start(),
            noxu_util::NULL_LSN
        );
        assert_eq!(checkpointer.get_last_checkpoint_end(), noxu_util::NULL_LSN);
    }

    #[test]
    fn test_checkpointer_do_checkpoint() {
        let checkpointer = Checkpointer::new(CheckpointConfig::default());
        let result = checkpointer.do_checkpoint("test").unwrap();
        assert_eq!(result.checkpoint_id, 1);
        assert!(result.start_lsn != noxu_util::NULL_LSN);
        assert!(result.end_lsn != noxu_util::NULL_LSN);
        assert_eq!(result.total_nodes_flushed(), 0);
    }

    #[test]
    fn test_checkpointer_sequential_ids() {
        let checkpointer = Checkpointer::new(CheckpointConfig::default());
        let result1 = checkpointer.do_checkpoint("test1").unwrap();
        let result2 = checkpointer.do_checkpoint("test2").unwrap();
        assert_eq!(result1.checkpoint_id, 1);
        assert_eq!(result2.checkpoint_id, 2);
    }

    #[test]
    fn test_checkpointer_concurrent_checkpoint_fails() {
        let checkpointer = Checkpointer::new(CheckpointConfig::default());
        checkpointer.checkpoint_in_progress.store(true, Ordering::Release);
        let result = checkpointer.do_checkpoint("test");
        assert!(result.is_err());
        if let Err(RecoveryError::CheckpointError(msg)) = result {
            assert!(msg.contains("already in progress"));
        } else {
            panic!("Expected CheckpointError");
        }
    }

    #[test]
    fn test_checkpointer_shutdown() {
        let checkpointer = Checkpointer::new(CheckpointConfig::default());
        checkpointer.request_shutdown();
        assert!(checkpointer.is_shutdown());
        let result = checkpointer.do_checkpoint("test");
        assert!(result.is_err());
        if let Err(RecoveryError::CheckpointError(msg)) = result {
            assert!(msg.contains("shut down"));
        } else {
            panic!("Expected CheckpointError");
        }
    }

    #[test]
    fn test_checkpointer_last_lsns() {
        let checkpointer = Checkpointer::new(CheckpointConfig::default());
        let result = checkpointer.do_checkpoint("test").unwrap();
        assert_eq!(checkpointer.get_last_checkpoint_start(), result.start_lsn);
        assert_eq!(checkpointer.get_last_checkpoint_end(), result.end_lsn);
    }

    #[test]
    fn test_checkpointer_stats() {
        let checkpointer = Checkpointer::new(CheckpointConfig::default());
        let stats = checkpointer.get_stats();
        assert_eq!(stats.checkpoints.load(Ordering::Relaxed), 0);
        checkpointer.do_checkpoint("test").unwrap();
        assert_eq!(stats.checkpoints.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_checkpoint_guard() {
        let flag = AtomicBool::new(false);
        let levels: std::sync::Mutex<HashMap<u64, i32>> =
            std::sync::Mutex::new(HashMap::from([(1u64, 3i32)]));
        {
            flag.store(true, Ordering::Release);
            let _guard = CheckpointGuard { flag: &flag, flush_levels: &levels };
            assert!(flag.load(Ordering::Acquire));
        }
        assert!(!flag.load(Ordering::Acquire));
        assert!(
            levels.lock().unwrap().is_empty(),
            "guard must clear flush_levels map"
        );
    }

    #[test]
    fn test_checkpoint_config_cloning() {
        let config1 = CheckpointConfig::new().force(true).bytes_interval(1000);
        let config2 = config1.clone();
        assert_eq!(config1.force, config2.force);
        assert_eq!(config1.bytes_interval, config2.bytes_interval);
    }

    #[test]
    fn test_checkpoint_result_cloning() {
        let result1 = CheckpointResult {
            checkpoint_id: 1,
            start_lsn: Lsn::new(1, 100),
            end_lsn: Lsn::new(1, 200),
            full_ins_flushed: 10,
            full_bins_flushed: 20,
            delta_ins_flushed: 5,
            elapsed_ms: 100,
        };
        let result2 = result1.clone();
        assert_eq!(result1.checkpoint_id, result2.checkpoint_id);
    }

    #[test]
    fn test_peek_next_checkpoint_id() {
        let checkpointer = Checkpointer::new(CheckpointConfig::default());
        assert_eq!(checkpointer.peek_next_checkpoint_id(), 1);
        checkpointer.do_checkpoint("test").unwrap();
        assert_eq!(checkpointer.peek_next_checkpoint_id(), 2);
    }

    #[test]
    fn test_multiple_checkpoints_update_lsns() {
        let checkpointer = Checkpointer::new(CheckpointConfig::default());
        let result1 = checkpointer.do_checkpoint("test1").unwrap();
        let result2 = checkpointer.do_checkpoint("test2").unwrap();
        assert_eq!(checkpointer.get_last_checkpoint_start(), result2.start_lsn);
        assert_eq!(checkpointer.get_last_checkpoint_end(), result2.end_lsn);
        assert_ne!(result1.start_lsn, result2.start_lsn);
    }

    // -----------------------------------------------------------------------
    // Tests that require a real LogManager / FileManager
    // -----------------------------------------------------------------------

    #[test]
    fn test_checkpoint_writes_wal_entries() {
        use noxu_log::{FileManager, LogManager};
        use std::sync::Arc;
        use tempfile::TempDir;

        let dir = TempDir::new().unwrap();
        let fm = Arc::new(
            FileManager::new(dir.path(), false, 64 * 1024 * 1024, 100).unwrap(),
        );
        let lm =
            Arc::new(LogManager::new(Arc::clone(&fm), 3, 1024 * 1024, 65536));

        let checkpointer = Checkpointer::new(CheckpointConfig::default())
            .with_log_manager(Arc::clone(&lm));

        let result = checkpointer.do_checkpoint("test_wal").unwrap();

        // Both LSNs must be non-null and the end must follow the start.
        assert!(
            !result.start_lsn.is_null(),
            "start_lsn must not be NULL after a WAL-backed checkpoint"
        );
        assert!(
            !result.end_lsn.is_null(),
            "end_lsn must not be NULL after a WAL-backed checkpoint"
        );
        assert!(
            result.end_lsn.as_u64() > result.start_lsn.as_u64(),
            "end_lsn ({:?}) must be greater than start_lsn ({:?})",
            result.end_lsn,
            result.start_lsn
        );

        // The stored LSNs on the checkpointer must match the returned result.
        assert_eq!(checkpointer.get_last_checkpoint_start(), result.start_lsn);
        assert_eq!(checkpointer.get_last_checkpoint_end(), result.end_lsn);
    }

    #[test]
    fn test_two_sequential_wal_checkpoints_have_increasing_lsns() {
        use noxu_log::{FileManager, LogManager};
        use std::sync::Arc;
        use tempfile::TempDir;

        let dir = TempDir::new().unwrap();
        let fm = Arc::new(
            FileManager::new(dir.path(), false, 64 * 1024 * 1024, 100).unwrap(),
        );
        let lm =
            Arc::new(LogManager::new(Arc::clone(&fm), 3, 1024 * 1024, 65536));

        let checkpointer = Checkpointer::new(CheckpointConfig::default())
            .with_log_manager(Arc::clone(&lm));

        let r1 = checkpointer.do_checkpoint("first").unwrap();
        let r2 = checkpointer.do_checkpoint("second").unwrap();

        // Each successive checkpoint must have strictly higher LSNs.
        assert!(
            r2.start_lsn.as_u64() > r1.end_lsn.as_u64(),
            "second start ({:?}) must follow first end ({:?})",
            r2.start_lsn,
            r1.end_lsn
        );
        assert!(
            r2.end_lsn.as_u64() > r2.start_lsn.as_u64(),
            "second end ({:?}) must follow second start ({:?})",
            r2.end_lsn,
            r2.start_lsn
        );
    }

    /// REC-F1 reproduce-first: every `do_checkpoint` path must make the
    /// `CkptEnd` entry durable with an fsync BEFORE the cleaner barrier is
    /// advanced.  JE Checkpointer.doCheckpoint (~line 895) calls
    /// `logManager.logForceFlush(endEntry, true /*fsyncRequired*/, ...)`
    /// with the comment "We must flush and fsync to ensure that cleaned
    /// files are not referenced. This also ensures that this checkpoint is
    /// not wasted if we crash."  Without the fsync, an auto/daemon
    /// checkpoint advances the safe-to-delete barrier off a non-durable
    /// checkpoint — a crash can then lose committed/migrated data.
    #[test]
    fn test_do_checkpoint_fsyncs_ckpt_end() {
        use noxu_log::{FileManager, LogManager};
        use std::sync::Arc;
        use tempfile::TempDir;

        let dir = TempDir::new().unwrap();
        let fm = Arc::new(
            FileManager::new(dir.path(), false, 64 * 1024 * 1024, 100).unwrap(),
        );
        let lm =
            Arc::new(LogManager::new(Arc::clone(&fm), 3, 1024 * 1024, 65536));

        let checkpointer = Checkpointer::new(CheckpointConfig::default())
            .with_log_manager(Arc::clone(&lm));

        let before = lm.fsync_count();
        checkpointer.do_checkpoint("daemon").unwrap();
        let after = lm.fsync_count();

        assert!(
            after > before,
            "do_checkpoint must fsync the CkptEnd entry (JE logForceFlush \
             fsyncRequired=true); fsync_count before={before} after={after}"
        );
    }

    // -----------------------------------------------------------------------
    // Tests for new methods: wakeup_after_write, is_checkpointed,
    // persist_file_summaries
    // -----------------------------------------------------------------------

    /// `wakeup_after_write` triggers a checkpoint once accumulated bytes
    /// exceed the configured threshold.
    #[test]
    fn test_wakeup_after_write_triggers_checkpoint() {
        let checkpointer = Checkpointer::new(CheckpointConfig::default())
            .with_bytes_interval(100); // tiny threshold for testing

        // Initial state: no checkpoints performed yet.
        assert_eq!(checkpointer.stats.checkpoints.load(Ordering::Relaxed), 0);

        // Write 99 bytes — below threshold; no checkpoint yet.
        checkpointer.wakeup_after_write(99);
        assert_eq!(
            checkpointer.stats.checkpoints.load(Ordering::Relaxed),
            0,
            "no checkpoint should fire below the threshold"
        );

        // Write 1 more byte — reaches threshold; checkpoint fires.
        checkpointer.wakeup_after_write(1);
        assert_eq!(
            checkpointer.stats.checkpoints.load(Ordering::Relaxed),
            1,
            "exactly one checkpoint should fire when threshold is reached"
        );

        // Counter should have been reset; another 100 bytes should trigger again.
        checkpointer.wakeup_after_write(100);
        assert_eq!(
            checkpointer.stats.checkpoints.load(Ordering::Relaxed),
            2,
            "second checkpoint should fire after counter reset"
        );
    }

    /// `wakeup_after_write` with interval=0 is a no-op.
    #[test]
    fn test_wakeup_after_write_disabled_when_interval_zero() {
        let checkpointer = Checkpointer::new(CheckpointConfig::default())
            .with_bytes_interval(0);

        checkpointer.wakeup_after_write(u64::MAX);
        assert_eq!(
            checkpointer.stats.checkpoints.load(Ordering::Relaxed),
            0,
            "no checkpoint should fire when interval is 0"
        );
    }

    /// `is_checkpointed` returns `false` for a BIN whose `last_full_lsn` is
    /// NULL_LSN (never checkpointed) and `true` after setting a non-NULL LSN.
    #[test]
    fn test_is_runnable_idle_guard() {
        // The daemon must NOT checkpoint an idle environment every wakeup.
        // is_runnable(false) is false until the relevant interval trips; force
        // is always true.
        //
        // REC-D: when the byte interval is set (non-zero) it takes precedence
        // (JE getWakeupPeriod / isRunnable: useTimeInterval stays 0), so a
        // sub-interval write is NOT runnable — only crossing the byte interval
        // is.
        let cp = Checkpointer::new(CheckpointConfig::default())
            .with_bytes_interval(1024);
        // Idle: nothing written since the last checkpoint.
        assert!(!cp.is_runnable(false), "idle env must not be runnable");
        // Force always runs (JE config.getForce()).
        assert!(cp.is_runnable(true), "force must always be runnable");
        // A sub-interval write is NOT runnable when a byte interval is set
        // (bytes takes precedence over time per JE isRunnable).
        cp.note_bytes_for_test(100);
        assert!(
            !cp.is_runnable(false),
            "sub-interval write must not be runnable when a byte interval is set \
             (REC-D: bytes takes precedence over the time branch)"
        );
        // Crossing the byte interval is runnable.
        cp.note_bytes_for_test(2000);
        assert!(cp.is_runnable(false));
    }

    /// REC-D: when the byte interval is DISABLED (0) the time branch applies
    /// — any write since the last checkpoint makes the daemon runnable on its
    /// next wakeup (JE isRunnable useTimeInterval branch with the
    /// `lastUsedLsn != lastCheckpointEnd` idle-guard).
    #[test]
    fn test_is_runnable_time_branch_when_bytes_disabled() {
        let cp = Checkpointer::new(CheckpointConfig::default())
            .with_bytes_interval(0); // bytes disabled → time-based
        // Idle: nothing written → not runnable (idle-guard).
        assert!(!cp.is_runnable(false), "idle time-based env must not run");
        // Any write makes it runnable on the next wakeup tick.
        cp.note_bytes_for_test(1);
        assert!(
            cp.is_runnable(false),
            "time branch: a write since the last checkpoint makes it runnable"
        );
    }

    #[test]
    fn test_is_checkpointed() {
        use noxu_tree::tree::{BinStub, TreeNode};
        use parking_lot::RwLock as NodeRwLock;

        // Build a BIN node with last_full_lsn = NULL_LSN.
        let bin = BinStub {
            node_id: 1,
            level: 0,
            entries: vec![],
            key_prefix: vec![],
            dirty: false,
            is_delta: false,
            last_full_lsn: noxu_util::NULL_LSN,
            last_delta_lsn: noxu_util::NULL_LSN,
            generation: 0,
            parent: None,
            // St-H6: test-only BIN; use true to match the engine-wide
            // hours-only invariant and avoid any accidental comparison with
            // a non-zero expiration_time.
            expiration_in_hours: true,
            cursor_count: 0,
            prohibit_next_delta: false,
        };
        let node = NodeRwLock::new(TreeNode::Bottom(bin));

        // Not yet checkpointed.
        assert!(
            !Checkpointer::is_checkpointed(&node),
            "fresh BIN should not be checkpointed"
        );

        // Simulate a checkpoint by setting last_full_lsn.
        {
            let mut guard = node.write();
            if let TreeNode::Bottom(ref mut b) = *guard {
                b.last_full_lsn = Lsn::new(1, 100);
            }
        }

        assert!(
            Checkpointer::is_checkpointed(&node),
            "BIN should be checkpointed after last_full_lsn is set"
        );
    }

    /// `persist_file_summaries` returns Ok(()) without panicking.
    #[test]
    fn test_persist_file_summaries_is_ok() {
        let checkpointer = Checkpointer::new(CheckpointConfig::default());
        assert!(checkpointer.persist_file_summaries().is_ok());
    }

    /// `persist_file_summaries` with a wired UtilizationTracker actually writes
    /// a `FileSummaryLN` entry to the WAL.
    ///
    /// Wires a real LogManager + UtilizationTracker, calls
    /// `persist_file_summaries()`, then scans the log file with
    /// `LogFileReader` to verify at least one `FileSummaryLN` entry was
    /// written.
    #[test]
    fn test_persist_file_summaries_writes_file_summary_ln_to_log() {
        use noxu_cleaner::UtilizationTracker;
        use noxu_log::{FileManager, LogEntryType, LogFileReader};
        use tempfile::TempDir;

        let dir = TempDir::new().unwrap();
        let fm = Arc::new(
            FileManager::new(dir.path(), false, 64 * 1024 * 1024, 100).unwrap(),
        );
        let lm =
            Arc::new(LogManager::new(Arc::clone(&fm), 3, 1024 * 1024, 65536));

        // Populate the tracker with a non-empty file summary so something is
        // written when persist_file_summaries() is called.
        let mut tracker = UtilizationTracker::new(true);
        tracker.count_new_log_entry(0, 128, true, false);
        tracker.track_obsolete(0, 64, 64, true);
        let tracker_arc = Arc::new(std::sync::Mutex::new(tracker));

        let checkpointer = Checkpointer::new(CheckpointConfig::default())
            .with_log_manager(Arc::clone(&lm))
            .with_utilization_tracker(Arc::clone(&tracker_arc));

        checkpointer.persist_file_summaries().unwrap();

        // Flush to disk so the reader can see the bytes.
        lm.flush_sync().unwrap();

        // Scan all log entries in file 0 and look for FileSummaryLN.
        let mut reader = LogFileReader::open(Arc::clone(&fm), 0).unwrap();
        let mut found = false;
        while let Some((_lsn, entry_type, _payload)) = reader.read_next() {
            if entry_type == LogEntryType::FileSummaryLN {
                found = true;
                break;
            }
        }
        assert!(
            found,
            "expected a FileSummaryLN entry in the log after persist_file_summaries()"
        );
    }

    /// Checkpoint with a real tree flushes dirty BINs — step 4.
    ///
    /// Inserts a few keys (marking BIN slots dirty), then runs a checkpoint
    /// and verifies the dirty count drops to zero after the checkpoint writes
    /// BIN/BINDelta entries to the WAL.
    #[test]
    fn test_checkpoint_flushes_dirty_bins() {
        use noxu_log::FileManager;
        use noxu_tree::tree::Tree;
        use noxu_util::lsn::Lsn;
        use tempfile::TempDir;

        let dir = TempDir::new().unwrap();
        let fm = Arc::new(
            FileManager::new(dir.path(), false, 64 * 1024 * 1024, 100).unwrap(),
        );
        let lm =
            Arc::new(LogManager::new(Arc::clone(&fm), 3, 1024 * 1024, 65536));

        // Build a tree with dirty BINs.
        let tree = Tree::new(1, 256);
        tree.insert(b"apple".to_vec(), b"fruit".to_vec(), Lsn::new(1, 1))
            .unwrap();
        tree.insert(b"banana".to_vec(), b"fruit".to_vec(), Lsn::new(1, 2))
            .unwrap();
        tree.insert(b"cherry".to_vec(), b"fruit".to_vec(), Lsn::new(1, 3))
            .unwrap();

        let tree_arc = Arc::new(RwLock::new(tree));

        // Verify dirty BINs exist before checkpoint.
        let dirty_before = tree_arc.read().unwrap().collect_dirty_bins(1);
        assert!(
            !dirty_before.is_empty(),
            "should have dirty BINs before checkpoint"
        );

        let checkpointer = Checkpointer::new(CheckpointConfig::default())
            .with_log_manager(Arc::clone(&lm))
            .with_tree(Arc::clone(&tree_arc), 1);

        let result = checkpointer.do_checkpoint("test").unwrap();
        assert!(
            result.total_nodes_flushed() > 0,
            "checkpoint should flush dirty BINs"
        );

        // After checkpoint, dirty BINs should be cleared.
        let dirty_after = tree_arc.read().unwrap().collect_dirty_bins(1);
        assert!(dirty_after.is_empty(), "no dirty BINs after checkpoint");
    }

    /// X-8 regression: checkpointer must not write a redundant empty BINDelta
    /// for a node that the evictor already flushed and cleared between the
    /// dirty-BIN snapshot and the per-node write-lock acquisition.
    ///
    /// Simulates the race by:
    /// 1. Building a tree with dirty BINs.
    /// 2. Collecting the dirty-BIN snapshot (as the checkpointer would).
    /// 3. Acquiring each BIN's write lock and calling
    ///    `clear_dirty_after_full_log` (simulating the evictor flushing).
    /// 4. Running `flush_dirty_bins_internal` and asserting that zero
    ///    BINDelta or full-BIN entries are written (nothing left to flush).
    #[test]
    fn test_x8_no_redundant_bindelta_after_evictor_flush() {
        use noxu_log::FileManager;
        use noxu_tree::tree::Tree;
        use noxu_util::lsn::Lsn;
        use tempfile::TempDir;

        let dir = TempDir::new().unwrap();
        let fm = Arc::new(
            FileManager::new(dir.path(), false, 64 * 1024 * 1024, 100).unwrap(),
        );
        let lm =
            Arc::new(LogManager::new(Arc::clone(&fm), 3, 1024 * 1024, 65536));

        // Build a tree with a dirty BIN.
        let tree = Tree::new(1, 256);
        tree.insert(b"alpha".to_vec(), b"v1".to_vec(), Lsn::new(1, 1)).unwrap();
        tree.insert(b"beta".to_vec(), b"v2".to_vec(), Lsn::new(1, 2)).unwrap();

        let tree_arc = Arc::new(RwLock::new(tree));

        // Snapshot dirty BINs (as the checkpointer does under tree read lock).
        let dirty_bins = tree_arc.read().unwrap().collect_dirty_bins(1);
        assert!(!dirty_bins.is_empty(), "precondition: must have dirty BINs");

        // Simulate the evictor flushing every dirty BIN (writes a full BIN
        // entry to WAL and clears the dirty flag) BEFORE the checkpointer
        // acquires the per-node write lock.
        let evictor_lsn = Lsn::new(2, 0); // fake "evictor-wrote" LSN
        for (_db_id, bin_arc) in &dirty_bins {
            let mut guard = bin_arc.write();
            if let TreeNode::Bottom(ref mut b) = *guard {
                // Mark the BIN as "already flushed" by the evictor.
                b.clear_dirty_after_full_log(evictor_lsn);
            }
        }

        // Now build the checkpointer and run the internal flush over the
        // stale snapshot (all BINs are now clean).
        let checkpointer = Checkpointer::new(CheckpointConfig::default())
            .with_log_manager(Arc::clone(&lm))
            .with_tree(Arc::clone(&tree_arc), 1);

        let result = checkpointer
            .flush_dirty_bins_internal()
            .expect("flush_dirty_bins_internal failed");

        // X-8 fix: with the guard `if !b.dirty && dirty == 0 { continue; }`,
        // the checkpointer must skip the already-clean BINs entirely.  No
        // BINDelta or full-BIN entries should be written.
        assert_eq!(
            result.delta_ins_flushed, 0,
            "X-8: checkpointer must not write a redundant BINDelta for a BIN the evictor already flushed"
        );
        assert_eq!(
            result.full_bins_flushed, 0,
            "X-8: checkpointer must not write a redundant full-BIN for a BIN the evictor already flushed"
        );
    }

    // -----------------------------------------------------------------------
    // CC-4: get_eviction_provisional tests (per-tree after residual fix)
    // -----------------------------------------------------------------------

    /// CC-4 acceptance test 1: Provisional::No when no checkpoint is in
    /// progress, regardless of db_id or node level.
    ///
    /// JE ref: coordinateEvictionWithCheckpoint — if no checkpoint is active,
    /// evicted nodes are logged non-provisionally.
    #[test]
    fn test_cc4_no_checkpoint_in_progress_yields_provisional_no() {
        let ckpt = Checkpointer::new(CheckpointConfig::default());
        assert_eq!(
            ckpt.get_eviction_provisional(1, 1),
            Provisional::No,
            "CC-4: no checkpoint in progress must yield Provisional::No"
        );
        assert_eq!(ckpt.get_eviction_provisional(1, 2), Provisional::No);
    }

    /// CC-4 acceptance test 2: Provisional::Yes when a checkpoint is in
    /// progress and the node's level is below the tree's max flush level.
    ///
    /// JE ref: coordinateEvictionWithCheckpoint — node.level < highestFlushLevel
    /// (for THIS db) => Provisional::YES.
    #[test]
    fn test_cc4_below_max_flush_level_yields_provisional_yes() {
        let ckpt = Checkpointer::new(CheckpointConfig::default());
        ckpt.checkpoint_in_progress.store(true, Ordering::Release);
        ckpt.checkpoint_flush_levels.lock().unwrap().insert(42u64, 2i32);

        assert_eq!(
            ckpt.get_eviction_provisional(42, 1),
            Provisional::Yes,
            "CC-4: BIN below tree's max_flush_level must yield Provisional::Yes"
        );

        ckpt.checkpoint_in_progress.store(false, Ordering::Release);
        ckpt.checkpoint_flush_levels.lock().unwrap().clear();
    }

    /// CC-4 acceptance test 3: Provisional::No when the node's level is at or
    /// above the tree's max flush level.
    ///
    /// JE ref: coordinateEvictionWithCheckpoint — node.level >= highestFlushLevel
    /// => Provisional::NO.
    #[test]
    fn test_cc4_at_or_above_max_flush_level_yields_provisional_no() {
        let ckpt = Checkpointer::new(CheckpointConfig::default());
        ckpt.checkpoint_in_progress.store(true, Ordering::Release);
        ckpt.checkpoint_flush_levels.lock().unwrap().insert(42u64, 2i32);

        assert_eq!(
            ckpt.get_eviction_provisional(42, 2),
            Provisional::No,
            "CC-4: node at max_flush_level must yield Provisional::No"
        );
        assert_eq!(
            ckpt.get_eviction_provisional(42, 3),
            Provisional::No,
            "CC-4: node above max_flush_level must yield Provisional::No"
        );

        ckpt.checkpoint_in_progress.store(false, Ordering::Release);
        ckpt.checkpoint_flush_levels.lock().unwrap().clear();
    }

    /// CC-4 residual acceptance test: a BIN from tree A (no dirty upper INs)
    /// must NOT be logged Provisional::Yes even when tree B has dirty upper INs
    /// at a higher level.  This is the exact scenario that caused data loss
    /// with the old global `AtomicI32`.
    ///
    /// Fail-pre: on `origin/main` (global level) `get_eviction_provisional(DB_A, 1)`
    /// returned `Provisional::Yes` — a lie, no covering ancestor was written for
    /// tree A.  Pass-post: per-tree lookup returns `Provisional::No` for tree A.
    ///
    /// JE ref: DirtyINMap.getHighestFlushLevel returns IN.MIN_LEVEL (0) for a
    /// DatabaseImpl absent from highestFlushLevels → comparison false → NO.
    #[test]
    fn test_cc4_residual_tree_a_no_upper_ins_yields_provisional_no() {
        const DB_A: u64 = 1; // only BINs dirty; no dirty upper INs
        const DB_B: u64 = 2; // has dirty upper INs at level 2

        let ckpt = Checkpointer::new(CheckpointConfig::default());
        ckpt.checkpoint_in_progress.store(true, Ordering::Release);

        // Only tree B gets an entry in the per-tree flush levels map.
        ckpt.checkpoint_flush_levels.lock().unwrap().insert(DB_B, 2i32);

        // Tree A's BIN (level 1) must be non-provisional: no covering ancestor.
        assert_eq!(
            ckpt.get_eviction_provisional(DB_A, 1),
            Provisional::No,
            "CC-4 residual: tree A has no dirty upper INs; BIN must be \
             Provisional::No (not covered by any ancestor in this checkpoint)"
        );

        // Tree B's BIN (level 1) may be provisional: its level-2 ancestor will
        // be written non-provisionally.
        assert_eq!(
            ckpt.get_eviction_provisional(DB_B, 1),
            Provisional::Yes,
            "CC-4: tree B has a dirty upper IN at level 2; BIN must be Provisional::Yes"
        );

        ckpt.checkpoint_in_progress.store(false, Ordering::Release);
        ckpt.checkpoint_flush_levels.lock().unwrap().clear();
    }

    /// CC-4: CheckpointGuard clears the flush_levels map on drop.
    #[test]
    fn test_cc4_guard_resets_max_flush_level() {
        let flag = AtomicBool::new(true);
        let levels: std::sync::Mutex<HashMap<u64, i32>> =
            std::sync::Mutex::new(HashMap::from([(7u64, 5i32)]));
        {
            let _guard = CheckpointGuard { flag: &flag, flush_levels: &levels };
        }
        assert!(levels.lock().unwrap().is_empty(), "guard must clear map");
        assert!(!flag.load(Ordering::Acquire));
    }

    // -----------------------------------------------------------------------
    // REC-D: the configured bytes-interval must reach the runnable gate
    // (not the hardcoded 10 MiB default).
    // -----------------------------------------------------------------------

    /// REC-D fail-pre/pass-post: a Checkpointer built with a configured
    /// bytes-interval must use THAT value in `is_runnable`, not the hardcoded
    /// 10 MiB.  JE Checkpointer ctor:
    /// `logSizeBytesInterval = configManager.getLong(CHECKPOINTER_BYTES_INTERVAL)`
    /// and `isRunnable` compares the bytes-since-checkpoint against it.
    ///
    /// Fail-pre: before REC-D the env wired only `CheckpointConfig.bytes_interval`
    /// (a field `is_runnable` never reads) while the gate used the hardcoded
    /// `checkpoint_bytes_interval = 10 MiB`.  A 1 KiB configured interval would
    /// NOT trip the gate at 1 KiB of writes.  Pass-post: `with_bytes_interval`
    /// threads the configured value into the gate.
    #[test]
    fn test_rec_d_configured_bytes_interval_drives_runnable() {
        // Configure a 1 KiB interval (far below the old 10 MiB default).
        let cp = Checkpointer::new(CheckpointConfig::default())
            .with_bytes_interval(1024);

        // Just below the configured interval: not runnable.
        cp.note_bytes_for_test(1000);
        assert!(
            !cp.is_runnable(false),
            "REC-D: 1000 bytes < configured 1 KiB interval must not be runnable"
        );

        // Cross the configured interval: runnable.  (With the old hardcoded
        // 10 MiB default this would stay false until 10 MiB of writes.)
        cp.note_bytes_for_test(100);
        assert!(
            cp.is_runnable(false),
            "REC-D: crossing the configured 1 KiB interval must be runnable; \
             the gate must use the configured interval, not 10 MiB"
        );
    }

    // -----------------------------------------------------------------------
    // REC-F: an idle environment with cleaner-pending files must trigger a
    // checkpoint (JE wakeupAfterNoWrites / needCheckpointForCleanedFiles).
    // -----------------------------------------------------------------------

    /// REC-F fail-pre/pass-post: with no bytes written since the last
    /// checkpoint, `is_runnable(false)` must still return true when the
    /// cleaner reports files pending reclaim (CLEANED set non-empty).
    ///
    /// JE `Checkpointer.isRunnable`:
    /// `if (wakeupAfterNoWrites && needCheckpointForCleanedFiles()) return true;`
    /// where `needCheckpointForCleanedFiles()` →
    /// `cleaner.getFileSelector().isCheckpointNeeded()`.
    ///
    /// Fail-pre: before REC-F `is_runnable` consulted only bytes; an idle env
    /// with cleaned-but-unreclaimed files returned false, so reclamation
    /// stalled until the next write-driven checkpoint.
    #[test]
    fn test_rec_f_idle_env_with_cleaner_pending_is_runnable() {
        use noxu_cleaner::Cleaner;
        use std::sync::Arc;

        let cleaner = Arc::new(Cleaner::new(50, 1, 0));

        let cp = Checkpointer::new(CheckpointConfig::default())
            .with_bytes_interval(1024)
            .with_cleaner(Arc::clone(&cleaner));

        // Idle environment: nothing written since the last checkpoint, no
        // cleaned files yet.
        assert!(
            !cp.is_runnable(false),
            "REC-F precondition: idle env with no pending files must not be runnable"
        );

        // Simulate the cleaner cleaning a file: it moves to the CLEANED state
        // (cleaned-but-not-checkpointed).  A checkpoint is now needed to
        // advance the deletion barrier.
        {
            let mut selector = cleaner.get_file_selector().lock();
            selector.add_file_to_clean(7);
            selector.mark_file_cleaned(7);
        }

        assert!(
            cleaner.is_checkpoint_needed(),
            "REC-F: cleaner must report a checkpoint is needed for the CLEANED file"
        );
        // Still no bytes written, but the idle-reclaim trigger fires.
        assert!(
            cp.is_runnable(false),
            "REC-F: idle env with cleaner-pending files must be runnable \
             (JE wakeupAfterNoWrites / needCheckpointForCleanedFiles)"
        );
    }

    // -----------------------------------------------------------------------
    // REC-G: init_intervals seeds the interval baselines from a recovered
    // checkpoint (JE Checkpointer.initIntervals).
    // -----------------------------------------------------------------------

    /// REC-G fail-pre/pass-post: after recovery the checkpointer's interval
    /// baselines must equal the recovered CkptEnd LSNs, not NULL_LSN.
    ///
    /// Fail-pre: a freshly-constructed Checkpointer has
    /// `last_checkpoint_start`/`_end` == NULL_LSN, so the first post-recovery
    /// interval is measured from process start.  Pass-post: `init_intervals`
    /// seeds them from the recovered CkptEnd.
    ///
    /// JE ref: `Checkpointer.initIntervals(lastCheckpointStart,
    /// lastCheckpointEnd, lastCheckpointMillis)`.
    #[test]
    fn test_rec_g_init_intervals_seeds_baselines() {
        let cp = Checkpointer::new(CheckpointConfig::default());
        // Fail-pre baseline: fresh checkpointer starts at NULL_LSN.
        assert_eq!(cp.get_last_checkpoint_start(), noxu_util::NULL_LSN);
        assert_eq!(cp.get_last_checkpoint_end(), noxu_util::NULL_LSN);

        // Simulate recovery surfacing a CkptEnd at (start=4:400, end=5:500).
        let recovered_start = Lsn::new(4, 400);
        let recovered_end = Lsn::new(5, 500);
        // Pretend the env wrote some pre-crash bytes before recovery.
        cp.note_bytes_for_test(9999);

        cp.init_intervals(recovered_start, recovered_end);

        assert_eq!(
            cp.get_last_checkpoint_start(),
            recovered_start,
            "REC-G: baseline start must equal recovered CkptEnd start"
        );
        assert_eq!(
            cp.get_last_checkpoint_end(),
            recovered_end,
            "REC-G: baseline end must equal recovered CkptEnd end"
        );
        // The byte accumulator is reset so pre-crash volume does not
        // immediately trip the runnable gate.
        assert!(
            !cp.is_runnable(false),
            "REC-G: byte accumulator must reset on init_intervals"
        );
    }

    // -----------------------------------------------------------------------
    // REC-H: set_checkpoint_id continues the sequence after recovery
    // (JE Checkpointer.setCheckpointId).
    // -----------------------------------------------------------------------

    /// REC-H fail-pre/pass-post: after recovery the next checkpoint ID must
    /// continue from the recovered CkptEnd id, not restart at 1.
    ///
    /// Fail-pre: a fresh Checkpointer's first checkpoint id is 1, colliding
    /// with pre-crash ids.  Pass-post: `set_checkpoint_id(recovered_id)` makes
    /// the next emitted id `recovered_id + 1`.
    ///
    /// JE ref: `Checkpointer.setCheckpointId(lastCheckpointId)`.
    #[test]
    fn test_rec_h_set_checkpoint_id_continues_sequence() {
        let cp = Checkpointer::new(CheckpointConfig::default());
        // Fail-pre: a fresh checkpointer would issue id 1.
        assert_eq!(cp.peek_next_checkpoint_id(), 1);

        // Recovery found a CkptEnd with id 42.
        cp.set_checkpoint_id(42);
        assert_eq!(
            cp.peek_next_checkpoint_id(),
            43,
            "REC-H: next checkpoint id must be recovered_id + 1"
        );

        // The next checkpoint must use 43, not 1.
        let result = cp.do_checkpoint("post_recovery").unwrap();
        assert_eq!(
            result.checkpoint_id, 43,
            "REC-H: post-recovery checkpoint id must continue the sequence"
        );
    }

    // -----------------------------------------------------------------------
    // REC-AA: the highest-flush-level is max(dirty-upper-IN-level) + 1,
    // bounded by the root level (JE DirtyINMap.updateFlushLevels).
    // -----------------------------------------------------------------------

    /// REC-AA fail-pre/pass-post: the per-tree highest flush level recorded
    /// for eviction coordination must be `max(dirty-upper-IN-level) + 1`
    /// (bounded by the root level), so a BIN evicted during the checkpoint is
    /// logged `Provisional::Yes` (covered by a non-provisional ancestor).
    ///
    /// Fail-pre: before REC-AA `collect_dirty_upper_ins` returned a
    /// root-relative depth (root=0) instead of the node's tree level, so the
    /// flush-levels map held tiny depths (1, 2) while the evictor compared the
    /// BIN's real `BIN_LEVEL` (`MAIN_LEVEL|1`); `BIN_LEVEL < 2` is always false
    /// → every BIN was logged `Provisional::No`, and the JE `+1` adjustment
    /// was absent entirely.
    ///
    /// Pass-post: levels are real tree levels, the recorded flush level is
    /// `max_dirty_upper_in_level + 1` bounded by the root, and a BIN at
    /// `BIN_LEVEL` gets `Provisional::Yes`.
    ///
    /// JE ref: `DirtyINMap.updateFlushLevels` (`(ckptFlushExtraLevel || isBIN)
    /// && !isRoot` → `level += 1`) / `Checkpointer.flushDirtyNodes`.
    #[test]
    fn test_rec_aa_flush_level_is_max_dirty_plus_one() {
        use noxu_log::FileManager;
        use noxu_tree::tree::{BIN_LEVEL, Tree};
        use noxu_util::lsn::Lsn;
        use tempfile::TempDir;

        let dir = TempDir::new().unwrap();
        let fm = Arc::new(
            FileManager::new(dir.path(), false, 64 * 1024 * 1024, 100).unwrap(),
        );
        let lm =
            Arc::new(LogManager::new(Arc::clone(&fm), 3, 1024 * 1024, 65536));

        // Fanout 4 + 20 inserts forces root splits → a 3-level tree
        // (root at MAIN_LEVEL|3, upper INs at |2, BINs at BIN_LEVEL=|1), with
        // dirty upper INs from the splits.
        let tree = Tree::new(1, 4);
        for i in 0u32..20 {
            let key = format!("key{:04}", i).into_bytes();
            let data = format!("data{}", i).into_bytes();
            tree.insert(key, data, Lsn::new(1, 100 + i)).unwrap();
        }
        let root_level = tree.get_root().unwrap().read().level();
        let dirty_uppers = tree.collect_dirty_upper_ins(1);
        assert!(
            !dirty_uppers.is_empty(),
            "precondition: the split tree must have dirty upper INs"
        );
        let max_dirty = dirty_uppers.iter().map(|(l, _)| *l).max().unwrap();
        // Levels must be real tree levels, not depths (REC-AA fail-pre would
        // have tiny depths here).
        assert!(
            max_dirty >= (noxu_tree::MAIN_LEVEL | 2),
            "upper-IN levels must be real tree levels (>= MAIN_LEVEL|2), got {max_dirty}"
        );

        let tree_arc = Arc::new(RwLock::new(tree));
        let cp = Checkpointer::new(CheckpointConfig::default())
            .with_log_manager(Arc::clone(&lm))
            .with_tree(Arc::clone(&tree_arc), 1);

        // Mark a checkpoint in progress and run the upper-IN flush, which
        // populates checkpoint_flush_levels with the REC-AA value.
        cp.checkpoint_in_progress.store(true, Ordering::Release);
        cp.flush_upper_ins_internal().unwrap();

        let recorded = cp
            .checkpoint_flush_levels
            .lock()
            .unwrap()
            .get(&1u64)
            .copied()
            .expect("db 1 must have a recorded flush level");

        let expected = (max_dirty + 1).min(root_level);
        assert_eq!(
            recorded, expected,
            "REC-AA: flush level must be max(dirty-upper-IN-level)+1 bounded by root"
        );

        // A BIN at BIN_LEVEL must be covered (Provisional::Yes): the recorded
        // flush level is strictly above it.
        assert!(
            BIN_LEVEL < recorded,
            "BIN_LEVEL ({BIN_LEVEL}) must be < recorded flush level ({recorded})"
        );
        assert_eq!(
            cp.get_eviction_provisional(1, BIN_LEVEL),
            Provisional::Yes,
            "REC-AA: a BIN below the flush level must be Provisional::Yes"
        );

        cp.checkpoint_in_progress.store(false, Ordering::Release);
        cp.checkpoint_flush_levels.lock().unwrap().clear();
    }
}
