//! Analysis phase results for 3-phase recovery.
//!
//! The analysis phase (Phase 1) scans the log forward from the last checkpoint
//! and builds three data structures:
//!
//! 1. **Dirty IN map** — internal nodes that were dirty at crash time and must
//!    be replayed during redo.  Keyed by `(db_id, node_id)`, deduped to the
//!    *latest* logged version.
//!
//! 2. **Committed transaction set** — all transaction IDs that reached a
//!    `TxnCommit` record in the recovery interval, together with the LSN of
//!    that commit record.  Mirrors `committedTxnIds` in `RecoveryManager.java`.
//!
//! 3. **Aborted transaction set** — all transaction IDs for which a `TxnAbort`
//!    record was seen.  These are skipped entirely during undo.  Mirrors
//!    `abortedTxnIds` in `RecoveryManager.java`.
//!
//! After analysis, the recovery manager knows exactly which INs to redo and
//! which LNs to undo.

use crate::log_scanner::InRecord;
use noxu_util::{Lsn, NULL_LSN};
use std::collections::{HashMap, HashSet};

/// Key that uniquely identifies a dirty IN across databases.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DirtyInKey {
    /// Database ID.
    pub db_id: u64,
    /// Node ID within that database.
    pub node_id: u64,
}

impl DirtyInKey {
    /// Create a new key.
    pub fn new(db_id: u64, node_id: u64) -> Self {
        Self { db_id, node_id }
    }
}

/// Entry in the dirty IN map: the latest logged version of an IN.
#[derive(Debug, Clone)]
pub struct DirtyInEntry {
    /// The IN record as read from the log.
    pub record: InRecord,
    /// LSN at which this version was logged.
    pub lsn: Lsn,
}

/// Results produced by Phase 1 (analysis).
///
/// Port of the data structures populated by `RecoveryManager.buildTree` and
/// the `undoLNs`/`redoLNs` preparation in JE's `RecoveryManager.java`.
#[derive(Debug)]
pub struct AnalysisResult {
    /// Dirty INs that must be replayed during redo, keyed by `(db_id, node_id)`.
    ///
    /// We keep only the *latest* logged entry per node (same as JE: a later
    /// checkpoint flush of the same IN supersedes an earlier one).
    pub dirty_ins: HashMap<DirtyInKey, DirtyInEntry>,

    /// Committed transaction IDs → LSN of the commit record.
    ///
    /// Port of `committedTxnIds` in `RecoveryManager.java`.
    pub committed_txns: HashMap<u64, Lsn>,

    /// Aborted transaction IDs.
    ///
    /// Port of `abortedTxnIds` in `RecoveryManager.java`.
    pub aborted_txns: HashSet<u64>,

    /// Maximum node ID seen in the recovery interval.
    pub max_node_id: u64,

    /// Maximum database ID seen.
    pub max_db_id: u64,

    /// Maximum transaction ID seen.
    pub max_txn_id: u64,

    /// LSN of the last checkpoint start found during analysis
    /// (`NULL_LSN` if none was found).
    pub checkpoint_start_lsn: Lsn,

    /// LSN of the last checkpoint end found during analysis
    /// (`NULL_LSN` if none found).
    pub checkpoint_end_lsn: Lsn,

    /// LSN of the first active transaction at the last checkpoint
    /// (`NULL_LSN` if no checkpoint).
    pub first_active_lsn: Lsn,

    /// LSN of the mapping-tree root.
    pub use_root_lsn: Lsn,
}

impl AnalysisResult {
    /// Create an empty `AnalysisResult`.
    pub fn new() -> Self {
        Self {
            dirty_ins: HashMap::new(),
            committed_txns: HashMap::new(),
            aborted_txns: HashSet::new(),
            max_node_id: 0,
            max_db_id: 0,
            max_txn_id: 0,
            checkpoint_start_lsn: NULL_LSN,
            checkpoint_end_lsn: NULL_LSN,
            first_active_lsn: NULL_LSN,
            use_root_lsn: NULL_LSN,
        }
    }

    /// Record a dirty IN seen at `lsn`.
    ///
    /// If the same `(db_id, node_id)` was already seen at an earlier LSN,
    /// the newer entry replaces it (matches JE's behaviour: the last logged
    /// version is the one to replay).
    pub fn record_dirty_in(&mut self, record: InRecord, lsn: Lsn) {
        let key = DirtyInKey::new(record.db_id, record.node_id);
        // Track max IDs
        if record.node_id > self.max_node_id {
            self.max_node_id = record.node_id;
        }
        if record.db_id > self.max_db_id {
            self.max_db_id = record.db_id;
        }
        let entry = self.dirty_ins.entry(key).or_insert_with(|| DirtyInEntry {
            record: record.clone(),
            lsn,
        });
        // Keep the latest version
        if lsn > entry.lsn {
            entry.record = record;
            entry.lsn = lsn;
        }
    }

    /// Record a committed transaction.
    pub fn record_commit(&mut self, txn_id: u64, commit_lsn: Lsn) {
        if txn_id > self.max_txn_id {
            self.max_txn_id = txn_id;
        }
        self.committed_txns.insert(txn_id, commit_lsn);
    }

    /// Record an aborted transaction.
    pub fn record_abort(&mut self, txn_id: u64) {
        if txn_id > self.max_txn_id {
            self.max_txn_id = txn_id;
        }
        self.aborted_txns.insert(txn_id);
    }

    /// Returns `true` if `txn_id` committed in the recovery interval.
    pub fn is_committed(&self, txn_id: u64) -> bool {
        self.committed_txns.contains_key(&txn_id)
    }

    /// Returns `true` if `txn_id` aborted in the recovery interval.
    pub fn is_aborted(&self, txn_id: u64) -> bool {
        self.aborted_txns.contains(&txn_id)
    }

    /// Returns `true` if `txn_id` was neither committed nor aborted
    /// (active at crash time, must be undone).
    pub fn is_active(&self, txn_id: u64) -> bool {
        !self.is_committed(txn_id) && !self.is_aborted(txn_id)
    }

    /// Number of dirty INs tracked.
    pub fn dirty_in_count(&self) -> usize {
        self.dirty_ins.len()
    }

    /// Number of committed transactions tracked.
    pub fn committed_count(&self) -> usize {
        self.committed_txns.len()
    }

    /// Number of aborted transactions tracked.
    pub fn aborted_count(&self) -> usize {
        self.aborted_txns.len()
    }

    /// Consume the dirty IN map, returning all entries grouped by tree level
    /// in ascending order (bottom-up = BINs first).
    ///
    /// Port of the bottom-up level iteration in `DirtyINMap` used by
    /// `redoDirtyNodes` in JE.
    pub fn take_dirty_ins_by_level(
        &mut self,
    ) -> Vec<(i32, Vec<DirtyInEntry>)> {
        let mut by_level: HashMap<i32, Vec<DirtyInEntry>> = HashMap::new();
        for entry in self.dirty_ins.drain().map(|(_, v)| v) {
            by_level.entry(entry.record.level).or_default().push(entry);
        }
        let mut result: Vec<(i32, Vec<DirtyInEntry>)> =
            by_level.into_iter().collect();
        result.sort_by_key(|(level, _)| *level);
        result
    }
}

impl Default for AnalysisResult {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log_scanner::InRecord;

    fn make_in(db_id: u64, node_id: u64, level: i32, is_root: bool) -> InRecord {
        InRecord { db_id, node_id, level, is_root, is_delta: false, node_data: None }
    }

    fn lsn(file: u32, offset: u32) -> Lsn {
        Lsn::new(file, offset)
    }

    #[test]
    fn test_new_is_empty() {
        let ar = AnalysisResult::new();
        assert_eq!(ar.dirty_in_count(), 0);
        assert_eq!(ar.committed_count(), 0);
        assert_eq!(ar.aborted_count(), 0);
        assert_eq!(ar.checkpoint_start_lsn, NULL_LSN);
        assert_eq!(ar.checkpoint_end_lsn, NULL_LSN);
        assert_eq!(ar.first_active_lsn, NULL_LSN);
        assert_eq!(ar.use_root_lsn, NULL_LSN);
    }

    #[test]
    fn test_default_is_empty() {
        let ar = AnalysisResult::default();
        assert_eq!(ar.dirty_in_count(), 0);
    }

    #[test]
    fn test_record_dirty_in() {
        let mut ar = AnalysisResult::new();
        ar.record_dirty_in(make_in(1, 10, 0, false), lsn(0, 100));
        assert_eq!(ar.dirty_in_count(), 1);
    }

    #[test]
    fn test_record_dirty_in_deduplication_keeps_latest() {
        let mut ar = AnalysisResult::new();
        ar.record_dirty_in(make_in(1, 10, 0, false), lsn(0, 100));
        ar.record_dirty_in(make_in(1, 10, 0, false), lsn(0, 200));
        // Same node → still 1 entry, but at lsn 200
        assert_eq!(ar.dirty_in_count(), 1);
        let key = DirtyInKey::new(1, 10);
        assert_eq!(ar.dirty_ins[&key].lsn, lsn(0, 200));
    }

    #[test]
    fn test_record_dirty_in_older_does_not_replace_newer() {
        let mut ar = AnalysisResult::new();
        ar.record_dirty_in(make_in(1, 10, 0, false), lsn(0, 200));
        // Insert older LSN — should not replace
        ar.record_dirty_in(make_in(1, 10, 0, false), lsn(0, 100));
        let key = DirtyInKey::new(1, 10);
        assert_eq!(ar.dirty_ins[&key].lsn, lsn(0, 200));
    }

    #[test]
    fn test_record_commit_and_abort() {
        let mut ar = AnalysisResult::new();
        ar.record_commit(1, lsn(0, 50));
        ar.record_abort(2);

        assert!(ar.is_committed(1));
        assert!(!ar.is_committed(2));
        assert!(ar.is_aborted(2));
        assert!(!ar.is_aborted(1));
    }

    #[test]
    fn test_is_active() {
        let mut ar = AnalysisResult::new();
        ar.record_commit(1, lsn(0, 50));
        ar.record_abort(2);

        assert!(!ar.is_active(1)); // committed
        assert!(!ar.is_active(2)); // aborted
        assert!(ar.is_active(3)); // never seen → active at crash
    }

    #[test]
    fn test_max_node_id_tracking() {
        let mut ar = AnalysisResult::new();
        ar.record_dirty_in(make_in(1, 100, 0, false), lsn(0, 10));
        ar.record_dirty_in(make_in(1, 50, 0, false), lsn(0, 20));
        assert_eq!(ar.max_node_id, 100);
    }

    #[test]
    fn test_max_txn_id_tracking() {
        let mut ar = AnalysisResult::new();
        ar.record_commit(5, lsn(0, 10));
        ar.record_abort(3);
        assert_eq!(ar.max_txn_id, 5);
    }

    #[test]
    fn test_take_dirty_ins_by_level_ordering() {
        let mut ar = AnalysisResult::new();
        // level 2 node
        ar.record_dirty_in(make_in(1, 1, 2, false), lsn(0, 10));
        // level 0 node (BIN)
        ar.record_dirty_in(make_in(1, 2, 0, false), lsn(0, 20));
        // level 1 node
        ar.record_dirty_in(make_in(1, 3, 1, false), lsn(0, 30));

        let groups = ar.take_dirty_ins_by_level();
        assert_eq!(groups.len(), 3);
        assert_eq!(groups[0].0, 0); // BIN first
        assert_eq!(groups[1].0, 1);
        assert_eq!(groups[2].0, 2);

        // map should be empty after take
        assert_eq!(ar.dirty_in_count(), 0);
    }

    #[test]
    fn test_take_dirty_ins_by_level_same_level_grouped() {
        let mut ar = AnalysisResult::new();
        ar.record_dirty_in(make_in(1, 1, 0, false), lsn(0, 10));
        ar.record_dirty_in(make_in(1, 2, 0, false), lsn(0, 20));
        ar.record_dirty_in(make_in(1, 3, 1, false), lsn(0, 30));

        let groups = ar.take_dirty_ins_by_level();
        assert_eq!(groups[0].0, 0);
        assert_eq!(groups[0].1.len(), 2); // two BIN nodes
        assert_eq!(groups[1].0, 1);
        assert_eq!(groups[1].1.len(), 1);
    }

    #[test]
    fn test_dirty_in_key_equality() {
        let k1 = DirtyInKey::new(1, 10);
        let k2 = DirtyInKey::new(1, 10);
        let k3 = DirtyInKey::new(1, 20);
        assert_eq!(k1, k2);
        assert_ne!(k1, k3);
    }

    #[test]
    fn test_multiple_dbs() {
        let mut ar = AnalysisResult::new();
        ar.record_dirty_in(make_in(1, 10, 0, false), lsn(0, 10));
        ar.record_dirty_in(make_in(2, 10, 0, false), lsn(0, 20)); // same node_id different db
        assert_eq!(ar.dirty_in_count(), 2);
        assert_eq!(ar.max_db_id, 2);
    }
}
