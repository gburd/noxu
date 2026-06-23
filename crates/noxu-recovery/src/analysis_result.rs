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
use hashbrown::{HashMap, HashSet};
use noxu_util::{Lsn, NULL_LSN};

/// A single LN entry queued for replay when a recovered prepared
/// transaction is committed via `xa_commit(xid)`.
///
/// Stored as owned bytes so the recovered prepared-txn list outlives
/// the file-mmap region that the analysis pass scanned.  This pays a
/// `Vec` allocation per prepared LN at recovery time, which is
/// acceptable given that prepared transactions are bounded in size by
/// the application’s XA workflow.
///
/// Wave 3-2 of the v1.5+ remediation plan.
#[derive(Debug, Clone)]
pub struct PreparedLnReplay {
    /// Database id this LN belongs to.
    pub db_id: u64,
    /// LSN where this LN was originally logged.
    pub original_lsn: Lsn,
    /// LN operation: insert, update, or delete.
    pub operation: PreparedLnOperation,
    /// LN key.
    pub key: Vec<u8>,
    /// LN value (`None` for deletes).
    pub data: Option<Vec<u8>>,
}

/// LN operation type for a prepared-txn replay record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PreparedLnOperation {
    Insert,
    Update,
    Delete,
}

/// Information about a transaction that was found prepared (XA phase 1
/// completed) but not yet committed or rolled back.
///
/// Surfaced to the XA layer via
/// `RecoveryInfo::recovered_prepared_txns()` so that `xa_recover()` can
/// return the in-doubt XIDs and a subsequent `xa_commit(xid)` /
/// `xa_rollback(xid)` can resolve the transaction.
///
/// Wave 3-2 of the v1.5+ remediation plan.
#[derive(Debug, Clone)]
pub struct PreparedTxnInfo {
    /// Transaction id of the prepared transaction.
    pub txn_id: u64,
    /// LSN of the `TxnPrepare` frame.  `xa_commit` writes a `TxnCommit`
    /// at a fresh LSN; this LSN is retained for diagnostics.
    pub prepare_lsn: Lsn,
    /// LSN of the first LN logged by this transaction (NULL_LSN if none).
    pub first_lsn: Lsn,
    /// LSN of the last LN logged before the prepare frame.  Used to bound
    /// the WAL re-scan that replays the prepared txn’s writes during
    /// `xa_commit` resolution.
    pub last_lsn: Lsn,
    /// XID format identifier (-1 == null).
    pub xid_format_id: i32,
    /// XID global transaction id (0..=64 bytes).
    pub xid_gtrid: Vec<u8>,
    /// XID branch qualifier (0..=64 bytes).
    pub xid_bqual: Vec<u8>,
}

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
/// Data structures populated by `RecoveryManager.buildTree` and
/// the `undoLNs`/`redoLNs` preparation in the equivalent `RecoveryManager.java`.
#[derive(Debug)]
pub struct AnalysisResult {
    /// Dirty INs that must be replayed during redo, keyed by `(db_id, node_id)`.
    ///
    /// We keep only the *latest* logged entry per node (same as the: a later
    /// checkpoint flush of the same IN supersedes an earlier one).
    pub dirty_ins: HashMap<DirtyInKey, DirtyInEntry>,

    /// Committed transaction IDs → LSN of the commit record.
    ///
    /// In `RecoveryManager.java`.
    pub committed_txns: HashMap<u64, Lsn>,

    /// Aborted transaction IDs.
    ///
    /// In `RecoveryManager.java`.
    pub aborted_txns: HashSet<u64>,

    /// Prepared transaction IDs (XA phase 1 completed) -> per-txn info.
    ///
    /// A txn lands here when the analysis pass sees its `TxnPrepare`
    /// frame.  It is REMOVED from this map (and added to
    /// `committed_txns` or `aborted_txns`) if a subsequent `TxnCommit`
    /// / `TxnAbort` is seen — the prepare is then resolved cleanly and
    /// no special handling is needed.  Transactions left in this map
    /// after the analysis pass are in-doubt and must be surfaced to
    /// the XA layer.
    ///
    /// Wave 3-2 of the v1.5+ remediation plan.
    pub prepared_txns: HashMap<u64, PreparedTxnInfo>,

    /// Transaction IDs seen in the recovery interval that have neither
    /// committed nor aborted — i.e., active at crash time, must be undone.
    ///
    /// Populated when an LN with a `txn_id` is seen; the ID is removed when
    /// `record_commit` or `record_abort` is called.  When empty after the full
    /// analysis pass, Phase 3 (undo) can be skipped entirely (clean shutdown).
    ///
    /// Mirrors the implicit set derived from `undoTxnIds` in
    /// `RecoveryManager.java`.
    pub active_txn_ids: HashSet<u64>,

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

    /// REC-H: the ID of the last `CkptEnd` found during analysis (`None` if
    /// none).  Used to continue the checkpoint-ID sequence after recovery.
    pub last_checkpoint_id: Option<u64>,

    /// LSN of the first active transaction at the last checkpoint
    /// (`NULL_LSN` if no checkpoint).
    pub first_active_lsn: Lsn,

    /// LSN of the mapping-tree root.
    pub use_root_lsn: Lsn,

    /// Database name → ID mappings accumulated from NameLN entries.
    /// Populated by `record_name_ln`; consumed by `RecoveryManager`.
    pub recovered_db_names: hashbrown::HashMap<String, u64>,

    /// Database name → creating-txn-id, for NameLN entries that carried a
    /// txn_id (C-6 fix).  Only present for `NameLNTxn` WAL entries; absent
    /// for non-transactional `NameLN` entries (pre-C6 or commit-time writes).
    ///
    /// Used by `run_mapping_tree_undo_pass` to remove names whose creating
    /// transaction aborted.
    pub recovered_db_txn_ids: hashbrown::HashMap<String, u64>,

    /// Database name → persisted comparator identities `(btree, dup)` from
    /// NameLN data (DBI-14).  `None` entries mean byte-ordered.  Consumed by
    /// `open_database` to enforce JE's comparator mismatch semantics on open.
    pub recovered_db_comparators:
        hashbrown::HashMap<String, (Option<String>, Option<String>)>,

    /// R-3: (vlsn, commit_lsn_u64) pairs from TxnCommit records whose
    /// `dtvlsn` payload field is non-zero.
    ///
    /// Populated during the analysis pass when processing TxnCommit records
    /// written with the R-3 fix (recovered XA commits that pre-allocated a
    /// VLSN before writing the WAL entry).  Merged into `recovered_vlsns` by
    /// the redo pass so a second crash does not lose these VLSNs.
    pub txncommit_vlsns: Vec<(u64, u64)>,
}

impl AnalysisResult {
    /// Create an empty `AnalysisResult`.
    pub fn new() -> Self {
        Self {
            dirty_ins: HashMap::new(),
            committed_txns: HashMap::new(),
            aborted_txns: HashSet::new(),
            prepared_txns: HashMap::new(),
            active_txn_ids: HashSet::new(),
            max_node_id: 0,
            max_db_id: 0,
            max_txn_id: 0,
            checkpoint_start_lsn: NULL_LSN,
            checkpoint_end_lsn: NULL_LSN,
            last_checkpoint_id: None,
            first_active_lsn: NULL_LSN,
            use_root_lsn: NULL_LSN,
            recovered_db_names: hashbrown::HashMap::new(),
            recovered_db_txn_ids: hashbrown::HashMap::new(),
            recovered_db_comparators: hashbrown::HashMap::new(),
            txncommit_vlsns: Vec::new(),
        }
    }

    /// Record a dirty IN seen at `lsn`.
    ///
    /// If the same `(db_id, node_id)` was already seen at an earlier LSN,
    /// the newer entry replaces it (behaviour: the last logged
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
        let entry = self
            .dirty_ins
            .entry(key)
            .or_insert_with(|| DirtyInEntry { record: record.clone(), lsn });
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
        self.active_txn_ids.remove(&txn_id);
        // A commit AFTER a prepare resolves the in-doubt txn cleanly —
        // remove from prepared so it does NOT appear in xa_recover.
        self.prepared_txns.remove(&txn_id);
    }

    /// Record an aborted transaction.
    pub fn record_abort(&mut self, txn_id: u64) {
        if txn_id > self.max_txn_id {
            self.max_txn_id = txn_id;
        }
        self.aborted_txns.insert(txn_id);
        self.active_txn_ids.remove(&txn_id);
        self.prepared_txns.remove(&txn_id);
    }

    /// Record a prepared transaction (`TxnPrepare` frame seen).
    ///
    /// The txn is removed from `active_txn_ids` (it is not active any
    /// more — it is in-doubt), and added to `prepared_txns`.  If a
    /// later `record_commit` or `record_abort` is called for the same
    /// id, the entry is removed from `prepared_txns` and the txn is
    /// considered cleanly resolved.
    ///
    /// Wave 3-2 of the v1.5+ remediation plan.
    pub fn record_prepare(&mut self, info: PreparedTxnInfo) {
        if info.txn_id > self.max_txn_id {
            self.max_txn_id = info.txn_id;
        }
        self.active_txn_ids.remove(&info.txn_id);
        self.prepared_txns.insert(info.txn_id, info);
    }

    /// Record a transactional LN seen during analysis (txn neither committed
    /// nor aborted yet).
    pub fn record_active_txn(&mut self, txn_id: u64) {
        // Defensive precondition: if this txn has already been recorded as
        // committed or aborted, do not re-add it to `active_txn_ids`.  An
        // out-of-order caller could otherwise create a phantom active txn
        // that causes `has_active_txns()` to return true after a clean
        // shutdown, triggering a spurious undo pass.
        //
        // In production the analysis pass sees log entries chronologically
        // so `record_commit` / `record_abort` always precede any later
        // re-encounter of the same txn id.  This guard makes the method
        // safe even if the caller violates that ordering assumption.
        if self.committed_txns.contains_key(&txn_id)
            || self.aborted_txns.contains(&txn_id)
        {
            return;
        }
        if txn_id > self.max_txn_id {
            self.max_txn_id = txn_id;
        }
        self.active_txn_ids.insert(txn_id);
    }

    /// Returns `true` if any transactions were active at crash time.
    ///
    /// When `false`, the undo phase (Phase 3) can be skipped entirely —
    /// all transactions committed or aborted cleanly before shutdown.
    pub fn has_active_txns(&self) -> bool {
        !self.active_txn_ids.is_empty()
    }

    /// Returns `true` if `txn_id` committed in the recovery interval.
    pub fn is_committed(&self, txn_id: u64) -> bool {
        self.committed_txns.contains_key(&txn_id)
    }

    /// Returns `true` if `txn_id` aborted in the recovery interval.
    pub fn is_aborted(&self, txn_id: u64) -> bool {
        self.aborted_txns.contains(&txn_id)
    }

    /// Returns `true` if `txn_id` was prepared (XA phase 1 completed)
    /// and not yet resolved.  These transactions are reported by
    /// `xa_recover()` and require explicit `xa_commit` / `xa_rollback`.
    pub fn is_prepared(&self, txn_id: u64) -> bool {
        self.prepared_txns.contains_key(&txn_id)
    }

    /// Returns `true` if `txn_id` was neither committed nor aborted
    /// (active at crash time, must be undone).
    pub fn is_active(&self, txn_id: u64) -> bool {
        !self.is_committed(txn_id)
            && !self.is_aborted(txn_id)
            && !self.is_prepared(txn_id)
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
    /// Bottom-up level iteration.
    /// `redoDirtyNodes`.
    pub fn take_dirty_ins_by_level(&mut self) -> Vec<(i32, Vec<DirtyInEntry>)> {
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

    fn make_in(
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
            is_provisional: false,
            node_data: None,
            prev_full_lsn: noxu_util::NULL_LSN,
        }
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
    fn test_has_active_txns_empty_by_default() {
        let ar = AnalysisResult::new();
        assert!(!ar.has_active_txns());
    }

    #[test]
    fn test_record_active_txn_appears_in_active_set() {
        let mut ar = AnalysisResult::new();
        ar.record_active_txn(7);
        assert!(ar.has_active_txns());
        assert!(ar.active_txn_ids.contains(&7));
    }

    #[test]
    fn test_record_commit_removes_from_active() {
        let mut ar = AnalysisResult::new();
        ar.record_active_txn(7);
        ar.record_commit(7, lsn(0, 100));
        assert!(!ar.has_active_txns());
    }

    #[test]
    fn test_record_abort_removes_from_active() {
        let mut ar = AnalysisResult::new();
        ar.record_active_txn(9);
        ar.record_abort(9);
        assert!(!ar.has_active_txns());
    }

    #[test]
    fn test_partially_committed_txns_leaves_active() {
        let mut ar = AnalysisResult::new();
        ar.record_active_txn(1);
        ar.record_active_txn(2);
        ar.record_commit(1, lsn(0, 100));
        // txn 2 still active
        assert!(ar.has_active_txns());
        assert!(ar.active_txn_ids.contains(&2));
        assert!(!ar.active_txn_ids.contains(&1));
    }

    #[test]
    fn test_max_txn_id_from_record_active_txn() {
        let mut ar = AnalysisResult::new();
        ar.record_active_txn(42);
        assert_eq!(ar.max_txn_id, 42);
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
