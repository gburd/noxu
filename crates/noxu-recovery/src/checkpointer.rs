//! Checkpoint daemon for Noxu DB.
//!
//! Port of `com.sleepycat.je.recovery.Checkpointer`.
//!
//! The Checkpointer flushes dirty IN nodes from the tree to the log in
//! bottom-up order. This bounds recovery time and ensures durability.

use crate::checkpoint_end::CheckpointEnd;
use crate::checkpoint_start::CheckpointStart;
use crate::checkpoint_stat::CheckpointStats;
use crate::dirty_in_map::DirtyINMap;
use crate::error::{RecoveryError, Result};
use noxu_log::entry::bin_delta_log_entry::BinDeltaLogEntry;
use noxu_log::entry::in_log_entry::InLogEntry;
use noxu_log::{LogEntryType, LogManager, Provisional};
use noxu_tree::tree::{Tree, TreeNode};
use noxu_util::{Lsn, NULL_LSN};
use parking_lot::Mutex;
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// Configuration for checkpoint behavior.
///
/// Port of `com.sleepycat.je.CheckpointConfig`.
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
/// Port of `com.sleepycat.je.recovery.Checkpointer`.
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
/// This implementation is a well-structured stub. Actual tree traversal
/// and IN flushing will be integrated when wiring to noxu-tree.
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
            config,
            log_manager: None,
            tree: None,
            db_id: 0,
        }
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
    /// Port of JE `Checkpointer` receiving the environment's tree reference.
    pub fn with_tree(mut self, tree: Arc<RwLock<Tree>>, db_id: u64) -> Self {
        self.tree = Some(tree);
        self.db_id = db_id;
        self
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

        // Step 4: Flush dirty BINs.
        //
        // For each dirty BIN in the tree decide — using JE's TREE_BIN_DELTA
        // threshold of 25 % — whether to write a BINDelta or a full BIN.
        //
        // Port of JE `Checkpointer.processINList()` + `logIN()`.
        let flush_result = self.flush_dirty_bins()?;

        // Step 5: Write CkptEnd entry to WAL.
        let end_lsn = if let Some(lm) = &self.log_manager {
            let ckpt_end = CheckpointEnd::new(
                checkpoint_id,
                invoker,
                start_lsn,
                None,                // root_lsn  (P1/P2 will fill this)
                noxu_util::NULL_LSN, // first_active_lsn
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
    pub fn request_shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
    }

    /// Check if shutdown has been requested.
    pub fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::Acquire)
    }

    /// Get the next checkpoint ID (without incrementing).
    pub fn peek_next_checkpoint_id(&self) -> u64 {
        self.next_checkpoint_id.load(Ordering::SeqCst)
    }

    /// Flush all dirty BINs to the log.
    ///
    /// For each dirty BIN the JE TREE_BIN_DELTA threshold (25 %) decides:
    /// - dirty_count / total ≤ 0.25 → write `BINDelta` entry (delta path)
    /// - otherwise                  → write full `BIN` entry (full path)
    ///
    /// After a successful write the BIN's dirty flags are cleared and (for
    /// full writes) `last_full_lsn` is updated to the entry's LSN.
    ///
    /// Port of JE `Checkpointer.processINList()` + `Checkpointer.logIN()`.
    fn flush_dirty_bins(&self) -> Result<FlushResult> {
        let mut result = FlushResult::default();

        let lm = match &self.log_manager {
            Some(lm) => lm,
            // No log manager — nothing to flush (unit tests).
            None => return Ok(result),
        };

        let tree_arc = match &self.tree {
            Some(t) => t,
            // No tree attached — step 4 is a no-op.
            None => return Ok(result),
        };

        // Collect dirty BINs under a read lock on the tree.
        let dirty_bins = {
            let tree_guard = tree_arc.read().map_err(|_| {
                RecoveryError::CheckpointError(
                    "tree lock poisoned during checkpoint".to_string(),
                )
            })?;
            tree_guard.collect_dirty_bins(self.db_id)
        };

        // JE TREE_BIN_DELTA: if dirty fraction ≤ 25 % write a delta.
        const TREE_BIN_DELTA: f64 = 0.25;

        for (_db_id, bin_arc) in dirty_bins {
            // Acquire write lock to serialize + clear dirty flags.
            let mut bin_guard = bin_arc.write().map_err(|_| {
                RecoveryError::CheckpointError(
                    "BIN lock poisoned during checkpoint".to_string(),
                )
            })?;

            let b = match &mut *bin_guard {
                TreeNode::Bottom(b) => b,
                _ => continue, // not a BIN (defensive)
            };

            let total = b.entries.len();
            let dirty = b.dirty_count();

            if total == 0 && !b.dirty {
                continue;
            }

            let use_delta = total > 0
                && (dirty as f64 / total as f64) <= TREE_BIN_DELTA
                && b.last_full_lsn != NULL_LSN; // need a previous full to delta from

            if use_delta {
                // --- BIN-delta path ---
                let delta_bytes = b.serialize_delta();
                let entry = BinDeltaLogEntry::new(
                    self.db_id,
                    b.last_full_lsn,
                    NULL_LSN, // prev_delta_lsn — we don't chain deltas yet
                    delta_bytes,
                );
                let mut buf = bytes::BytesMut::with_capacity(entry.log_size());
                entry.write_to_log(&mut buf);
                lm.log(
                    LogEntryType::BINDelta,
                    &buf,
                    Provisional::No,
                    false, // flush_required
                    false, // fsync_required — fsync at CkptEnd
                )
                .map_err(|e| {
                    RecoveryError::CheckpointError(format!("BINDelta WAL write failed: {e}"))
                })?;
                b.clear_dirty_after_delta_log();
                result.delta_ins_flushed += 1;
            } else {
                // --- Full BIN path ---
                let full_bytes = b.serialize_full();
                let entry = InLogEntry::new(
                    self.db_id,
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
                        RecoveryError::CheckpointError(format!("BIN WAL write failed: {e}"))
                    })?;
                b.clear_dirty_after_full_log(logged_lsn);
                result.full_bins_flushed += 1;
            }
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
struct FlushResult {
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
            FileManager::new(dir.path(), false, 64 * 1024 * 1024, 100)
                .unwrap(),
        );
        let lm = Arc::new(LogManager::new(Arc::clone(&fm), 3, 1024 * 1024, 65536));

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
            FileManager::new(dir.path(), false, 64 * 1024 * 1024, 100)
                .unwrap(),
        );
        let lm = Arc::new(LogManager::new(Arc::clone(&fm), 3, 1024 * 1024, 65536));

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
        use std::sync::RwLock;
        use tempfile::TempDir;

        let dir = TempDir::new().unwrap();
        let fm = Arc::new(
            FileManager::new(dir.path(), false, 64 * 1024 * 1024, 100).unwrap(),
        );
        let lm = Arc::new(LogManager::new(Arc::clone(&fm), 3, 1024 * 1024, 65536));

        // Build a tree with dirty BINs.
        let mut tree = Tree::new(1, 256);
        tree.insert(b"apple".to_vec(), b"fruit".to_vec(), Lsn::new(1, 1)).unwrap();
        tree.insert(b"banana".to_vec(), b"fruit".to_vec(), Lsn::new(1, 2)).unwrap();
        tree.insert(b"cherry".to_vec(), b"fruit".to_vec(), Lsn::new(1, 3)).unwrap();

        let tree_arc = Arc::new(RwLock::new(tree));

        // Verify dirty BINs exist before checkpoint.
        let dirty_before = tree_arc.read().unwrap().collect_dirty_bins(1);
        assert!(!dirty_before.is_empty(), "should have dirty BINs before checkpoint");

        let checkpointer = Checkpointer::new(CheckpointConfig::default())
            .with_log_manager(Arc::clone(&lm))
            .with_tree(Arc::clone(&tree_arc), 1);

        let result = checkpointer.do_checkpoint("test").unwrap();
        assert!(result.total_nodes_flushed() > 0, "checkpoint should flush dirty BINs");

        // After checkpoint, dirty BINs should be cleared.
        let dirty_after = tree_arc.read().unwrap().collect_dirty_bins(1);
        assert!(dirty_after.is_empty(), "no dirty BINs after checkpoint");
    }
}
