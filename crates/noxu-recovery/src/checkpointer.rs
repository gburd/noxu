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
}

impl Default for CheckpointConfig {
    fn default() -> Self {
        CheckpointConfig {
            force: false,
            minimize_recovery_time: false,
            bytes_interval: 20_000_000, // 20MB default
            time_interval: 0, // Time-based checkpoints disabled by default
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
    checkpoint_bytes_interval: u64,
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
            utilization_tracker: None,
            cleaner: None,
            txn_manager: None,
        }
    }

    /// Set the bytes-written threshold that triggers an immediate checkpoint.
    ///
    ///
    pub fn with_bytes_interval(mut self, bytes: u64) -> Self {
        self.checkpoint_bytes_interval = bytes;
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
            let obsolete_count =
                (summary.obsolete_ln_count + summary.obsolete_in_count) as i64;
            let entry = FileSummaryLnEntry::new(
                *file_number as u64,
                summary.total_count as i64,
                summary.total_size as i64,
                obsolete_count,
                summary.obsolete_ln_size as i64,
                summary.obsolete_ln_size_counted > 0,
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

        // Ensure we clear the in-progress flag on exit
        let _guard = CheckpointGuard { flag: &self.checkpoint_in_progress };

        // Step 1: Generate checkpoint ID
        let checkpoint_id =
            self.next_checkpoint_id.fetch_add(1, Ordering::SeqCst);

        // X-5: snapshot the cleaner's "cleaned" file set at checkpoint START
        // (before we write CkptStart) so we know which files were in the
        // cleaned state when this checkpoint began.  Passed to
        // `after_checkpoint` at the end of this function.
        let cleaner_state = self
            .cleaner
            .as_ref()
            .map(|c| c.get_checkpoint_start_state());

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

        let end_lsn = if let Some(lm) = &self.log_manager {
            let ckpt_end = CheckpointEnd::new(
                checkpoint_id,
                invoker,
                start_lsn,
                None, // root_lsn  (P1/P2 will fill this)
                first_active_lsn,
                0,
                0,
                0,
                0,
                0,
                0,     // ID sequence values (P1/P2 will fill these)
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
                true,  // flush_required — ensure both entries reach disk
                false, // fsync_required
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

    /// Get checkpoint statistics.
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
    /// For each dirty BIN the TREE_BIN_DELTA threshold (25 %) decides:
    /// - dirty_count / total ≤ 0.25 → write `BINDelta` entry (delta path)
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
            let r = Self::flush_one_tree_bins(db_id, &tree_arc, lm)?;
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

        // TREE_BIN_DELTA: if dirty fraction ≤ 25 % write a delta.
        const TREE_BIN_DELTA: f64 = 0.25;

        for (_node_db_id, bin_arc) in dirty_bins {
            // Acquire write lock to serialize + clear dirty flags.
            let mut bin_guard = bin_arc.write();

            let b = match &mut *bin_guard {
                TreeNode::Bottom(b) => b,
                _ => continue, // not a BIN (defensive)
            };

            let total = b.entries.len();
            let dirty = b.dirty_count();

            // X-8: skip nodes that the evictor already flushed and cleared
            // between our dirty-BIN snapshot (under tree read lock) and the
            // per-node write-lock acquisition.
            if !b.dirty && dirty == 0 {
                continue;
            }

            let use_delta = total > 0
                && (dirty as f64 / total as f64) <= TREE_BIN_DELTA
                && b.last_full_lsn != NULL_LSN; // need a previous full to delta from

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

/// RAII guard to ensure checkpoint_in_progress flag is cleared.
struct CheckpointGuard<'a> {
    flag: &'a AtomicBool,
}

impl<'a> Drop for CheckpointGuard<'a> {
    fn drop(&mut self) {
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
        {
            flag.store(true, Ordering::Release);
            let _guard = CheckpointGuard { flag: &flag };
            assert!(flag.load(Ordering::Acquire));
        }
        assert!(!flag.load(Ordering::Acquire));
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
}
