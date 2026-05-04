//! Log scanning abstraction for recovery.
//!
//! Port of the log reader types used in `RecoveryManager.java`:
//! `INFileReader`, `LNFileReader`, `LastFileReader`, `CheckpointFileReader`.
//!
//! Provides a `LogEntry` enum covering all entry types that recovery needs to
//! process, and a `LogScanner` trait so that the real log-reading path and
//! in-memory test fixtures share the same interface.

use noxu_util::{Lsn, NULL_LSN};

/// Operation type carried by a transactional LN.
///
/// Port of the LN subtype discrimination done in JE's `LNFileReader`.
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
/// Port of the data extracted from `LNLogEntry<?>` in `LNFileReader`.
#[derive(Debug, Clone)]
pub struct LnRecord {
    /// Database ID that owns this LN.
    pub db_id: u64,
    /// Transaction ID, `None` for non-transactional LNs.
    pub txn_id: Option<u64>,
    /// The operation type.
    pub operation: LnOperation,
    /// The key bytes.
    pub key: Vec<u8>,
    /// The value bytes, `None` for deletes.
    pub data: Option<Vec<u8>>,
    /// LSN to revert to on undo (before-image LSN, NULL_LSN = first write).
    pub abort_lsn: Lsn,
    /// Whether the slot was known-deleted before this operation.
    pub abort_known_deleted: bool,
    /// Key of the before-image (None when same as `key`).
    ///
    /// Port of `LNLogEntry.getAbortKey()` in JE — populated when an embedded
    /// before-image has a different key (key-updating operations).
    pub abort_key: Option<Vec<u8>>,
    /// Data of the before-image (embedded in the log entry itself).
    ///
    /// Port of `LNLogEntry.getAbortData()` in JE — populated for all embedded
    /// LNs in the NoSQL fork so that undo does NOT need to re-read the log.
    /// `None` for non-embedded LNs (rare in modern JE) and for first writes
    /// where the before-image is "deleted" (use `abort_known_deleted` instead).
    pub abort_data: Option<Vec<u8>>,
    /// Whether this entry has been marked invisible (rolled-back by HA).
    pub is_invisible: bool,
    /// Whether this entry belongs to a replicated transaction.
    pub is_replicated: bool,
}

impl LnRecord {
    /// Create a new LN record.
    pub fn new(
        db_id: u64,
        txn_id: Option<u64>,
        operation: LnOperation,
        key: Vec<u8>,
        data: Option<Vec<u8>>,
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
        }
    }
}

/// An internal-node (IN/BIN) record as seen during recovery.
///
/// Port of the data extracted from `INFileReader` in JE.
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
}

/// A transaction-abort record.
#[derive(Debug, Clone)]
pub struct TxnAbortRecord {
    /// The aborting transaction ID.
    pub txn_id: u64,
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
    /// HA rollback start.
    RollbackStart(RollbackStartRecord),
    /// HA rollback end.
    RollbackEnd(RollbackEndRecord),
    /// Mapping-tree root.
    DbTree(DbTreeRecord),
}

/// A log entry together with its position in the log.
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
/// Mirrors the contract of JE's `LastFileReader`, `INFileReader`, and
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
    /// Port of `RecoveryManager.findEndOfLog` / `LastFileReader`.
    fn find_end_of_log(&mut self) -> (Lsn, Lsn);

    /// Scan **forward** from `start_lsn` up to (but not including)
    /// `end_lsn`, yielding every entry.
    ///
    /// Port of the forward-reading `INFileReader` and `LNFileReader` paths.
    fn scan_forward(
        &self,
        start_lsn: Lsn,
        end_lsn: Lsn,
    ) -> Vec<PositionedEntry>;

    /// Scan **backward** from `start_lsn` down to `stop_lsn` (inclusive),
    /// yielding every entry in reverse LSN order.
    ///
    /// Port of the backward-reading `LNFileReader` path used by `undoLNs`.
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
    /// Port of `RecoveryManager.undo()` in JE which calls
    /// `fetchTarget(db, bin, idx, abortLsn, ...)` to read the before-image
    /// directly from the log when it is not embedded in the LN log entry.
    fn read_at_lsn(&self, lsn: Lsn) -> Option<LogEntry>;
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
        result.sort_by(|a, b| b.lsn.cmp(&a.lsn));
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
            }),
        );
        scanner.push(
            lsn(0, 200),
            LogEntry::TxnCommit(TxnCommitRecord {
                txn_id: 2,
                lsn: lsn(0, 200),
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
            b"key".to_vec(),
            Some(b"val".to_vec()),
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
        };
        assert_eq!(rec.level, 2);
        assert!(rec.is_root);
    }
}
