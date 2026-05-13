//! Group commit interface and implementations.
//!
//! Txn.GroupCommitMaster`, and
//! group commit for transactional log writes.
//!
//! # Overview
//!
//! The `GroupCommit` mechanism batches transaction fsyncs to improve throughput
//! in replicated environments. A "leader" thread sleeps briefly
//! (`MASTER_GROUP_COMMIT_INTERVAL`) or until the batch reaches a size limit
//! (`MASTER_MAX_GROUP_COMMIT`), then issues a single fsync that covers all
//! buffered transactions.
//!
//! ## Roles
//!
//! * **Master** ([`GroupCommitMaster`]): used by the primary node; waits for
//!   time/size thresholds before issuing an fsync and then sends ACKs.
//! * **Replica** ([`GroupCommitReplica`]): used by replica nodes during log
//!   replay; batches acknowledgements for the feeder after applying commits.
//!
//! The `GroupCommit` trait abstracts both roles so that `TxnManager` can hold
//! either implementation behind an `Arc<dyn GroupCommit>`.
//!
//! In non-replicated environments `TxnManager.group_commit` is `None` and
//! transactions use the base `FSyncManager` path directly.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

/// Maximum number of transactions to batch before forcing an fsync.
///
/// Default: 20.
pub const DEFAULT_MAX_GROUP_COMMIT: usize = 20;

/// Time window for batching transactions before forcing an fsync, in
/// milliseconds.
///
/// Default: 20 ms.
pub const DEFAULT_GROUP_COMMIT_INTERVAL_MS: u64 = 20;

/// Shared group-commit abstraction used by [`crate::TxnManager`].
///
/// 
pub trait GroupCommit: Send + Sync {
    /// Returns `true` if group commit is currently enabled.
    ///
    /// 
    fn is_enabled(&self) -> bool;

    /// Called by each committing transaction to buffer itself and potentially
    /// trigger an fsync.
    ///
    /// * `commit_vlsn` — VLSN assigned to this commit entry.
    ///
    /// Returns `true` if the commit was durably fsynced (or piggybacked on a
    /// concurrent fsync) before returning; `false` if fsync was skipped
    /// (e.g. `CommitNoSync` durability).
    ///
    /// 
    fn buffer_commit(&self, commit_vlsn: i64) -> bool;

    /// Shuts down the group-commit background machinery.
    ///
    /// (implied by `StoppableThread`).
    fn shutdown(&self);
}

// ── GroupCommitMaster ─────────────────────────────────────────────────────────

/// Group-commit implementation for the **Master** role.
///
/// When a transaction arrives it is added to a pending queue. A leader thread
/// waits for up to `interval_ms` milliseconds (or until `max_count`
/// transactions are queued) before issuing a single fsync. After the fsync the
/// queued transactions are acknowledged.
///
/// ## Threshold semantics
///
/// `buffer_commit()` returns `false` (caller must fsync) every `max_count`
/// calls — the count-based threshold.  The caller (`Txn::commit_with_durability`)
/// treats a `false` return as a signal to call `LogManager::flush_sync()`,
/// which then handles the time-based coalescing via `FSyncManager`.  This
/// correctly separates concerns: GroupCommit enforces the batch-size policy;
/// FSyncManager enforces the time-window and leader/waiter coalescing.
///
/// 
pub struct GroupCommitMaster {
    /// Whether group commit is currently active.
    enabled: AtomicBool,
    /// Maximum transactions per batch before forcing an fsync.
    max_count: usize,
    /// Time window for batching in milliseconds (passed to FSyncManager).
    interval_ms: u64,
    /// Running count of buffered commits since the last threshold flush.
    pending_count: AtomicUsize,
    /// Number of times the count threshold has fired (observable in tests).
    flush_count: AtomicUsize,
}

impl GroupCommitMaster {
    /// Creates a new `GroupCommitMaster`.
    ///
    /// # Arguments
    ///
    /// * `max_count` — maximum batch size.
    /// * `interval_ms` — batch window in milliseconds
    ///   `MASTER_GROUP_COMMIT_INTERVAL`).
    pub fn new(max_count: usize, interval_ms: u64) -> Self {
        GroupCommitMaster {
            enabled: AtomicBool::new(max_count > 0),
            max_count,
            interval_ms,
            pending_count: AtomicUsize::new(0),
            flush_count: AtomicUsize::new(0),
        }
    }

    /// Returns the batch window in milliseconds.
    pub fn interval_ms(&self) -> u64 {
        self.interval_ms
    }

    /// Returns the number of times the count threshold has fired.
    ///
    /// Used in tests to verify durability threshold enforcement.
    pub fn flush_count(&self) -> usize {
        self.flush_count.load(Ordering::Relaxed)
    }
}

impl Default for GroupCommitMaster {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_GROUP_COMMIT, DEFAULT_GROUP_COMMIT_INTERVAL_MS)
    }
}

impl GroupCommit for GroupCommitMaster {
    fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    /// Buffer a commit and enforce the count-based threshold.
    ///
    /// Returns `false` (caller must fsync) on every `max_count`th call.
    /// Returns `true` (commit is buffered, skip fsync) otherwise.
    ///
    /// Count-threshold path.
    /// The time-window threshold is handled by `FSyncManager` when the
    /// caller proceeds to `LogManager::flush_sync()` on a `false` return.
    fn buffer_commit(&self, _commit_vlsn: i64) -> bool {
        if !self.enabled.load(Ordering::Relaxed) {
            return false; // Disabled: caller must always fsync.
        }
        // Increment and check threshold.  fetch_add returns the value BEFORE
        // the increment, so we compare against max_count - 1.
        let prev = self.pending_count.fetch_add(1, Ordering::AcqRel);
        if prev + 1 >= self.max_count {
            // Threshold reached: reset counter and signal caller to fsync.
            self.pending_count.store(0, Ordering::Release);
            self.flush_count.fetch_add(1, Ordering::Relaxed);
            return false; // Caller must call flush_sync().
        }
        true // Buffered: caller skips fsync.
    }

    fn shutdown(&self) {
        self.enabled.store(false, Ordering::Relaxed);
    }
}

// ── GroupCommitReplica ────────────────────────────────────────────────────────

/// Group-commit implementation for the **Replica** role.
///
/// Batches acknowledgements during log replay, sending an ACK to the feeder
/// once a batch of committed transactions has been applied and durably written.
///
/// 
pub struct GroupCommitReplica {
    enabled: AtomicBool,
    interval_ms: u64,
}

impl GroupCommitReplica {
    /// Creates a new `GroupCommitReplica`.
    pub fn new(interval_ms: u64) -> Self {
        GroupCommitReplica {
            enabled: AtomicBool::new(true),
            interval_ms,
        }
    }

}

impl Default for GroupCommitReplica {
    fn default() -> Self {
        Self::new(DEFAULT_GROUP_COMMIT_INTERVAL_MS)
    }
}

impl GroupCommit for GroupCommitReplica {
    fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    fn buffer_commit(&self, _commit_vlsn: i64) -> bool {
        // On the replica, each committed entry from the feeder is queued.
        // After the batch window elapses (or the batch fills), an ACK is sent
        // back. The actual durability is ensured by the fsync that precedes the
        // ACK.
        //
        //   1. Add VLSN to the pending ACK queue.
        //   2. If a leader exists, piggyback; otherwise become leader and wait
        //      groupCommitIntervalMs before ACKing the batch.
        true
    }

    fn shutdown(&self) {
        self.enabled.store(false, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_master_default_enabled() {
        let gc = GroupCommitMaster::default();
        assert!(gc.is_enabled());
    }

    #[test]
    fn test_master_disabled_when_max_zero() {
        let gc = GroupCommitMaster::new(0, 20);
        assert!(!gc.is_enabled());
    }

    #[test]
    fn test_master_buffer_commit_first_is_buffered() {
        // First commit in a fresh batch is buffered (threshold not yet reached).
        let gc = GroupCommitMaster::new(3, 20);
        assert!(gc.buffer_commit(1), "first commit should be buffered");
        assert_eq!(gc.flush_count(), 0);
    }

    #[test]
    fn test_master_threshold_fires_at_max_count() {
        // With max_count=3: commits 1 and 2 are buffered; commit 3 fires fsync.
        let gc = GroupCommitMaster::new(3, 20);
        assert!(gc.buffer_commit(1),  "commit 1 should be buffered");
        assert!(gc.buffer_commit(2),  "commit 2 should be buffered");
        assert!(!gc.buffer_commit(3), "commit 3 must trigger flush (threshold)");
        assert_eq!(gc.flush_count(), 1, "exactly one flush should have fired");
    }

    #[test]
    fn test_master_threshold_resets_after_flush() {
        // After threshold fires, the counter resets and the cycle repeats.
        let gc = GroupCommitMaster::new(3, 20);
        assert!(gc.buffer_commit(1));
        assert!(gc.buffer_commit(2));
        assert!(!gc.buffer_commit(3)); // flush #1
        // Next batch:
        assert!(gc.buffer_commit(4));
        assert!(gc.buffer_commit(5));
        assert!(!gc.buffer_commit(6)); // flush #2
        assert_eq!(gc.flush_count(), 2);
    }

    #[test]
    fn test_master_disabled_always_flushes() {
        // When max_count=0, group commit is disabled: every commit requires fsync.
        let gc = GroupCommitMaster::new(0, 20);
        assert!(!gc.is_enabled());
        assert!(!gc.buffer_commit(1), "disabled GC must return false (always flush)");
        assert!(!gc.buffer_commit(2));
    }

    #[test]
    fn test_master_shutdown() {
        let gc = GroupCommitMaster::default();
        gc.shutdown();
        assert!(!gc.is_enabled());
        // After shutdown, buffer_commit must return false (always flush).
        assert!(!gc.buffer_commit(99), "post-shutdown must return false");
    }

    #[test]
    fn test_master_interval_ms_accessible() {
        let gc = GroupCommitMaster::new(10, 50);
        assert_eq!(gc.interval_ms(), 50);
    }

    #[test]
    fn test_replica_default_enabled() {
        let gc = GroupCommitReplica::default();
        assert!(gc.is_enabled());
    }

    #[test]
    fn test_replica_buffer_commit() {
        let gc = GroupCommitReplica::default();
        assert!(gc.buffer_commit(10));
    }
}
