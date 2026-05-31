//! Log scanning abstraction for recovery.
//!
//! `INFileReader`, `LNFileReader`, `LastFileReader`, `CheckpointFileReader`.
//!
//! Provides a `LogEntry` enum covering all entry types that recovery needs to
//! process, and a `LogScanner` trait so that the real log-reading path and
//! in-memory test fixtures share the same interface.

use bytes::Bytes;
use noxu_util::{Lsn, NULL_LSN};

/// Operation type carried by a transactional LN.
///
/// LN subtype discrimination.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LnOperation {
    Insert,
    Update,
    Delete,
}

/// A single leaf-node (LN) record as seen during recovery.
///
/// Carries the minimum information needed by the undo/redo phases:
/// - which transaction it belongs to (if any)
/// - what key/value were written
/// - the abort LSN and abort-known-deleted flag (before-image info)
///
/// Variable-length fields use [`bytes::Bytes`] so that the file-backed scanner
/// can store zero-copy slices of the mmap'd log region — cloning a `Bytes` is
/// O(1) (Arc refcount bump) and no heap allocation is needed until the bytes
/// are actually materialised into the B-tree at the redo/undo boundary.
///
/// Data extracted from log entries.
#[derive(Debug, Clone)]
pub struct LnRecord {
    /// Database ID that owns this LN.
    pub db_id: u64,
    /// Transaction ID, `None` for non-transactional LNs.
    pub txn_id: Option<u64>,
    /// The operation type.
    pub operation: LnOperation,
    /// The key bytes — zero-copy slice when built by the file-backed scanner.
    pub key: Bytes,
    /// The value bytes, `None` for deletes.
    pub data: Option<Bytes>,
    /// LSN to revert to on undo (before-image LSN, NULL_LSN = first write).
    pub abort_lsn: Lsn,
    /// Whether the slot was known-deleted before this operation.
    pub abort_known_deleted: bool,
    /// Key of the before-image (None when same as `key`).
    ///
    /// In — populated when an embedded
    /// before-image has a different key (key-updating operations).
    pub abort_key: Option<Bytes>,
    /// Data of the before-image (embedded in the log entry itself).
    ///
    /// In — populated for all embedded
    /// LNs in the extended fork so that undo does NOT need to re-read the log.
    /// `None` for non-embedded LNs (rare in modern ) and for first writes
    /// where the before-image is "deleted" (use `abort_known_deleted` instead).
    pub abort_data: Option<Bytes>,
    /// Whether this entry has been marked invisible (rolled-back by HA).
    pub is_invisible: bool,
    /// Whether this entry belongs to a replicated transaction.
    pub is_replicated: bool,
    /// VLSN of this LN if the original log entry header carried one.
    ///
    /// Populated by the file-backed `LogScanner` from the entry header.
    /// `None` for entries that were never replicated, or for in-memory
    /// test fixtures that do not synthesise VLSNs.  Used by the redo
    /// phase to verify that VLSNs of replicated entries are observed in
    /// strictly-increasing order (security review LOG-6).
    pub vlsn: Option<u64>,
}

impl LnRecord {
    /// Create a new LN record.
    pub fn new(
        db_id: u64,
        txn_id: Option<u64>,
        operation: LnOperation,
        key: Bytes,
        data: Option<Bytes>,
        abort_lsn: Lsn,
        abort_known_deleted: bool,
    ) -> Self {
        Self {
            db_id,
            txn_id,
            operation,
            key,
            data,
            abort_lsn,
            abort_known_deleted,
            abort_key: None,
            abort_data: None,
            is_invisible: false,
            is_replicated: false,
            vlsn: None,
        }
    }
}

/// An internal-node (IN/BIN) record as seen during recovery.
///
/// Data extracted from log entries.
#[derive(Debug, Clone)]
pub struct InRecord {
    /// Database ID that owns this IN.
    pub db_id: u64,
    /// Node ID.
    pub node_id: u64,
    /// Level in the B-tree (0 = BIN, higher = upper INs).
    pub level: i32,
    /// Whether this is the root of its database tree.
    pub is_root: bool,
    /// Whether this is a BIN-delta.
    pub is_delta: bool,
    /// Raw serialized node bytes as written by `BinStub::serialize_full()` or
    /// `BinStub::serialize_delta()`.  Present when the log scanner can parse
    /// the payload; `None` for scanner stubs that don't carry node data.
    ///
    /// / `BINDeltaLogEntry.getMainItem()`
    /// in — the deserialized IN/BIN object available after `readEntry()`.
    pub node_data: Option<Vec<u8>>,
}

/// A checkpoint-start record.
#[derive(Debug, Clone)]
pub struct CkptStartRecord {
    /// Checkpoint ID.
    pub id: u64,
    /// LSN of this CkptStart entry.
    pub lsn: Lsn,
}

/// A checkpoint-end record.
#[derive(Debug, Clone)]
pub struct CkptEndRecord {
    /// Checkpoint ID.
    pub id: u64,
    /// LSN of the matching CkptStart.
    pub checkpoint_start_lsn: Lsn,
    /// LSN of the first active transaction at checkpoint time.
    pub first_active_lsn: Lsn,
    /// Root LSN (mapping tree root).
    pub root_lsn: Lsn,
    /// ID counters.
    pub last_local_node_id: u64,
    pub last_replicated_node_id: i64,
    pub last_local_db_id: u64,
    pub last_replicated_db_id: i64,
    pub last_local_txn_id: u64,
    pub last_replicated_txn_id: i64,
}

/// A transaction-commit record.
#[derive(Debug, Clone)]
pub struct TxnCommitRecord {
    /// The committing transaction ID.
    pub txn_id: u64,
    /// LSN of this commit record.
    pub lsn: Lsn,
    /// R-3: durable-transaction VLSN embedded in the WAL entry payload.
    ///
    /// Non-zero only for recovered XA commits written with R-3 fix
    /// (`write_txn_commit_for_recovered` pre-allocates and embeds the VLSN).
    /// The X-14 VLSN rebuild includes this VLSN in `recovered_vlsns` so a
    /// second crash after XA resolution does not lose the VLSN.
    pub dtvlsn: Option<u64>,
}

/// A transaction-abort record.
#[derive(Debug, Clone)]
pub struct TxnAbortRecord {
    /// The aborting transaction ID.
    pub txn_id: u64,
}

/// A transaction-prepare record (XA two-phase commit, wave 3-2).
///
/// Recovery must:
///   * NOT undo a transaction whose tail entry is `TxnPrepare` — the
///     transaction is in-doubt waiting for `xa_commit` / `xa_rollback`.
///   * NOT redo its LN entries into the in-memory tree (prepared writes
///     are invisible until resolution).
///   * Surface the (xid, txn_id, first_lsn, last_lsn) tuple to the XA
///     layer via `RecoveryInfo::recovered_prepared_txns()` so
///     `xa_recover()` can return the in-doubt XID and a subsequent
///     `xa_commit(xid)` / `xa_rollback(xid)` can resolve it.
#[derive(Debug, Clone)]
pub struct TxnPrepareRecord {
    /// The prepared transaction ID.
    pub txn_id: u64,
    /// LSN of the first LN logged by this transaction (NULL_LSN if none).
    pub first_lsn: Lsn,
    /// LSN of the last LN logged before this prepare frame.
    pub last_lsn: Lsn,
    /// LSN of this `TxnPrepare` entry itself.
    pub lsn: Lsn,
    /// XID format identifier (-1 == null).
    pub xid_format_id: i32,
    /// XID global transaction id (0..=64 bytes).
    pub xid_gtrid: Vec<u8>,
    /// XID branch qualifier (0..=64 bytes).
    pub xid_bqual: Vec<u8>,
}

/// A rollback-start record (HA replica syncup).
#[derive(Debug, Clone)]
pub struct RollbackStartRecord {
    /// Matchpoint LSN (start of rollback period).
    pub matchpoint_lsn: Lsn,
    /// LSN of this RollbackStart entry.
    pub lsn: Lsn,
}

/// A rollback-end record (HA replica syncup).
#[derive(Debug, Clone)]
pub struct RollbackEndRecord {
    /// Matchpoint LSN this end pairs with.
    pub matchpoint_lsn: Lsn,
    /// LSN of this RollbackEnd entry.
    pub lsn: Lsn,
}

/// A DbTree (mapping-tree root) record.
#[derive(Debug, Clone)]
pub struct DbTreeRecord {
    /// LSN at which the mapping tree root was logged.
    pub lsn: Lsn,
}

/// Union of all log entry types that the 3-phase recovery processes.
///
/// This mirrors the set of `LogEntryType` variants that `buildTree`, `undoLNs`,
/// and `redoLNs` in `RecoveryManager.java` handle.
#[derive(Debug, Clone)]
pub enum LogEntry {
    /// Internal node (IN, BIN, BIN-delta).
    In(InRecord),
    /// Leaf node (any transactional or non-transactional LN variant).
    Ln(LnRecord),
    /// Checkpoint start.
    CkptStart(CkptStartRecord),
    /// Checkpoint end.
    CkptEnd(CkptEndRecord),
    /// Transaction commit.
    TxnCommit(TxnCommitRecord),
    /// Transaction abort.
    TxnAbort(TxnAbortRecord),
    /// Transaction prepare (XA two-phase commit, wave 3-2).
    TxnPrepare(TxnPrepareRecord),
    /// HA rollback start.
    RollbackStart(RollbackStartRecord),
    /// HA rollback end.
    RollbackEnd(RollbackEndRecord),
    /// Mapping-tree root.
    DbTree(DbTreeRecord),
    /// Database name registration (NameLN / NameLNTxn).
    NameLn(NameLnRecord),
}

/// Database name registration record (NameLN).
///
/// Carries the mapping between a database name and its integer ID as
/// written to the WAL by `EnvironmentImpl::open_database` whenever a new
/// database is created.  During the analysis pass, these records are
/// collected into `RecoveryInfo::recovered_db_names` so that the name_map
/// can be restored on a subsequent open — including read-only reopens where
/// `allow_create=false` would otherwise fail with `DatabaseNotFound`.
///
/// `is_deleted = true` marks a Remove (or Truncate) operation that should
/// remove the name from the registry.
#[derive(Debug, Clone)]
pub struct NameLnRecord {
    pub name: String,
    pub db_id: u64,
    pub is_deleted: bool,
    /// Transaction ID of the creating transaction, if any.
    ///
    /// `None` means the NameLN was written outside of a transaction (or the
    /// WAL entry predates C-6 and did not carry a txn_id).
    ///
    /// Used by `run_mapping_tree_undo_pass` to remove NameLNs whose creating
    /// transaction is in the aborted-transaction set.  A `None` txn_id is
    /// treated as committed (no undo needed) to preserve backward compat with
    /// pre-C6 WAL files.
    pub txn_id: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct PositionedEntry {
    /// LSN of this entry.
    pub lsn: Lsn,
    /// The entry payload.
    pub entry: LogEntry,
}

impl PositionedEntry {
    /// Create a new positioned entry.
    pub fn new(lsn: Lsn, entry: LogEntry) -> Self {
        Self { lsn, entry }
    }
}

/// Abstract log scanner used by the 3-phase recovery.
///
/// Mirrors the contract of `LastFileReader`, `INFileReader`, and
/// `LNFileReader` behind a single trait so that both the real log path and
/// in-memory test fixtures can satisfy it.
///
/// The scanner yields entries in **forward** LSN order (as required by the
/// redo phase) or **backward** LSN order (as required by the undo phase),
/// depending on which scan method is called.
pub trait LogScanner {
    /// Return the LSN of the first valid entry and the next-available (EOF)
    /// LSN found by scanning the last log file.
    ///
    /// / `LastFileReader`.
    fn find_end_of_log(&mut self) -> (Lsn, Lsn);

    /// Scan **forward** from `start_lsn` up to (but not including)
    /// `end_lsn`, yielding every entry.
    ///
    /// Forward-reading log scan path.
    fn scan_forward(
        &self,
        start_lsn: Lsn,
        end_lsn: Lsn,
    ) -> Vec<PositionedEntry>;

    /// Scan **backward** from `start_lsn` down to `stop_lsn` (inclusive),
    /// yielding every entry in reverse LSN order.
    ///
    /// Backward-reading log scan path for undo.
    fn scan_backward(
        &self,
        start_lsn: Lsn,
        stop_lsn: Lsn,
    ) -> Vec<PositionedEntry>;

    /// Read the single log entry at exactly `lsn`.
    ///
    /// Returns `None` if the entry is not found.  Used during the undo phase
    /// to fetch the before-image of a disk-resident LN at its `abort_lsn`.
    ///
    /// In which calls
    /// `fetchTarget(db, bin, idx, abortLsn, ...)` to read the before-image
    /// directly from the log when it is not embedded in the LN log entry.
    fn read_at_lsn(&self, lsn: Lsn) -> Option<LogEntry>;

    /// Scan **forward** from `start_lsn` to `end_lsn`, invoking `cb` for each
    /// entry without collecting into an intermediate `Vec`.
    ///
    /// The default implementation calls `scan_forward()` and iterates the
    /// returned `Vec`.  Override this method in the real file-backed scanner
    /// to process entries inline (streaming), eliminating the O(N) allocation
    /// and improving cache locality during the analysis phase.
    ///
    /// LNFileReader / INFileReader read loop.
    fn scan_forward_fn(
        &self,
        start_lsn: Lsn,
        end_lsn: Lsn,
        cb: &mut dyn FnMut(PositionedEntry),
    ) {
        for pe in self.scan_forward(start_lsn, end_lsn) {
            cb(pe);
        }
    }
}

/// An in-memory `LogScanner` backed by a `Vec<PositionedEntry>`.
///
/// Used for unit-testing the recovery logic without actual log files.
/// Entries must be inserted in forward LSN order; backward scanning
/// simply reverses the subset.
pub struct InMemoryLogScanner {
    /// All entries sorted by ascending LSN.
    entries: Vec<PositionedEntry>,
    /// The last valid (used) LSN in this mock log.
    last_used_lsn: Lsn,
    /// The next-available (EOF) LSN.
    next_available_lsn: Lsn,
}

impl InMemoryLogScanner {
    /// Create an empty in-memory scanner.
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            last_used_lsn: NULL_LSN,
            next_available_lsn: NULL_LSN,
        }
    }

    /// Append an entry.  Entries should be added in ascending LSN order.
    pub fn push(&mut self, lsn: Lsn, entry: LogEntry) {
        let positioned = PositionedEntry::new(lsn, entry);
        // Track last-used and next-available
        if self.last_used_lsn == NULL_LSN || lsn > self.last_used_lsn {
            self.last_used_lsn = lsn;
            // next_available = lsn + 1 offset unit (logical)
            self.next_available_lsn =
                Lsn::new(lsn.file_number(), lsn.file_offset() + 1);
        }
        self.entries.push(positioned);
    }

    /// Explicitly set the end-of-log LSNs.
    pub fn set_end_of_log(&mut self, last_used: Lsn, next_available: Lsn) {
        self.last_used_lsn = last_used;
        self.next_available_lsn = next_available;
    }

    /// Return a reference to all stored entries.
    pub fn entries(&self) -> &[PositionedEntry] {
        &self.entries
    }
}

impl Default for InMemoryLogScanner {
    fn default() -> Self {
        Self::new()
    }
}

impl LogScanner for InMemoryLogScanner {
    fn find_end_of_log(&mut self) -> (Lsn, Lsn) {
        (self.last_used_lsn, self.next_available_lsn)
    }

    fn scan_forward(
        &self,
        start_lsn: Lsn,
        end_lsn: Lsn,
    ) -> Vec<PositionedEntry> {
        self.entries
            .iter()
            .filter(|e| {
                (start_lsn == NULL_LSN || e.lsn >= start_lsn)
                    && (end_lsn == NULL_LSN || e.lsn < end_lsn)
            })
            .cloned()
            .collect()
    }

    fn scan_backward(
        &self,
        start_lsn: Lsn,
        stop_lsn: Lsn,
    ) -> Vec<PositionedEntry> {
        let mut result: Vec<PositionedEntry> = self
            .entries
            .iter()
            .filter(|e| {
                (start_lsn == NULL_LSN || e.lsn <= start_lsn)
                    && (stop_lsn == NULL_LSN || e.lsn >= stop_lsn)
            })
            .cloned()
            .collect();
        result.sort_by_key(|b| std::cmp::Reverse(b.lsn));
        result
    }

    fn read_at_lsn(&self, target_lsn: Lsn) -> Option<LogEntry> {
        self.entries
            .iter()
            .find(|e| e.lsn == target_lsn)
            .map(|e| e.entry.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lsn(file: u32, offset: u32) -> Lsn {
        Lsn::new(file, offset)
    }

    #[test]
    fn test_in_memory_scanner_empty() {
        let mut scanner = InMemoryLogScanner::new();
        let (last, next) = scanner.find_end_of_log();
        assert_eq!(last, NULL_LSN);
        assert_eq!(next, NULL_LSN);
        assert!(scanner.scan_forward(NULL_LSN, NULL_LSN).is_empty());
        assert!(scanner.scan_backward(NULL_LSN, NULL_LSN).is_empty());
    }

    #[test]
    fn test_in_memory_scanner_push_updates_end_of_log() {
        let mut scanner = InMemoryLogScanner::new();

        scanner.push(
            lsn(0, 100),
            LogEntry::TxnCommit(TxnCommitRecord {
                txn_id: 1,
                lsn: lsn(0, 100),
                    dtvlsn: None,
            }),
        );
        scanner.push(
            lsn(0, 200),
            LogEntry::TxnCommit(TxnCommitRecord {
                txn_id: 2,
                lsn: lsn(0, 200),
                    dtvlsn: None,
            }),
        );

        let (last, next) = scanner.find_end_of_log();
        assert_eq!(last, lsn(0, 200));
        assert_eq!(next, lsn(0, 201));
    }

    #[test]
    fn test_scan_forward_range() {
        let mut scanner = InMemoryLogScanner::new();
        for i in 0u32..5 {
            scanner.push(
                lsn(0, i * 100),
                LogEntry::TxnCommit(TxnCommitRecord {
                    txn_id: i as u64,
                    lsn: lsn(0, i * 100),
                    dtvlsn: None,
                }),
            );
        }

        // scan [100, 300)
        let entries = scanner.scan_forward(lsn(0, 100), lsn(0, 300));
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].lsn, lsn(0, 100));
        assert_eq!(entries[1].lsn, lsn(0, 200));
    }

    #[test]
    fn test_scan_backward_range() {
        let mut scanner = InMemoryLogScanner::new();
        for i in 0u32..5 {
            scanner.push(
                lsn(0, i * 100),
                LogEntry::TxnCommit(TxnCommitRecord {
                    txn_id: i as u64,
                    lsn: lsn(0, i * 100),
                    dtvlsn: None,
                }),
            );
        }

        // scan backward [300, 100]
        let entries = scanner.scan_backward(lsn(0, 300), lsn(0, 100));
        assert_eq!(entries.len(), 3);
        // must be in descending order
        assert_eq!(entries[0].lsn, lsn(0, 300));
        assert_eq!(entries[1].lsn, lsn(0, 200));
        assert_eq!(entries[2].lsn, lsn(0, 100));
    }

    #[test]
    fn test_scan_forward_null_bounds() {
        let mut scanner = InMemoryLogScanner::new();
        scanner.push(
            lsn(0, 100),
            LogEntry::TxnAbort(TxnAbortRecord { txn_id: 1 }),
        );
        scanner.push(
            lsn(0, 200),
            LogEntry::TxnAbort(TxnAbortRecord { txn_id: 2 }),
        );

        // NULL_LSN bounds = no filter
        let entries = scanner.scan_forward(NULL_LSN, NULL_LSN);
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn test_set_end_of_log_explicit() {
        let mut scanner = InMemoryLogScanner::new();
        scanner.set_end_of_log(lsn(1, 500), lsn(1, 501));
        let (last, next) = scanner.find_end_of_log();
        assert_eq!(last, lsn(1, 500));
        assert_eq!(next, lsn(1, 501));
    }

    #[test]
    fn test_ln_record_fields() {
        let rec = LnRecord::new(
            42,
            Some(7),
            LnOperation::Insert,
            Bytes::from_static(b"key"),
            Some(Bytes::from_static(b"val")),
            NULL_LSN,
            false,
        );
        assert_eq!(rec.db_id, 42);
        assert_eq!(rec.txn_id, Some(7));
        assert_eq!(rec.operation, LnOperation::Insert);
        assert!(!rec.is_invisible);
        assert!(!rec.is_replicated);
    }

    #[test]
    fn test_in_record_fields() {
        let rec = InRecord {
            db_id: 1,
            node_id: 99,
            level: 2,
            is_root: true,
            is_delta: false,
            node_data: None,
        };
        assert_eq!(rec.level, 2);
        assert!(rec.is_root);
    }
}
