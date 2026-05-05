//! File-manager-backed log scanner for recovery.
//!
//! Implements `LogScanner` over a real `FileManager` + `LogManager` pair so
//! that `RecoveryManager::recover()` can be called during `Environment::open()`
//! on an existing database directory.
//!
//! Port of `LastFileReader` / `INFileReader` / `LNFileReader` usage in
//! `RecoveryManager.recover()`.

use std::sync::Arc;

use noxu_log::{
    FileManager,
    entry::{BinDeltaLogEntry, InLogEntry, LnLogEntry, TxnEndEntry},
    entry_header::{MAX_HEADER_SIZE, MIN_HEADER_SIZE},
    entry_type::LogEntryType,
    file_header::FILE_HEADER_SIZE,
};
use noxu_recovery::{
    CkptEndRecord, CkptStartRecord, CheckpointEnd, CheckpointStart, InRecord,
    LnOperation, LnRecord, LogEntry, LogScanner, PositionedEntry,
    TxnAbortRecord, TxnCommitRecord,
};
use noxu_util::{Lsn, NULL_LSN};

// Maximum plausible payload size (64 MiB) for a sanity check while scanning.
const MAX_SANE_ITEM_SIZE: usize = 64 * 1024 * 1024;

/// `LogScanner` implementation backed by a real `FileManager`.
///
/// Implements the three scan primitives required by `RecoveryManager`:
/// - `find_end_of_log` — scans log files to discover the true end of the log
///   and updates the `FileManager`'s LSN positions via `set_last_position()`.
/// - `scan_forward`  — sequential forward scan over a LSN range.
/// - `scan_backward` — reverse-order scan (implemented as forward + reverse).
pub struct FileManagerLogScanner {
    file_manager: Arc<FileManager>,
}

impl FileManagerLogScanner {
    pub fn new(file_manager: Arc<FileManager>) -> Self {
        Self { file_manager }
    }

    // ------------------------------------------------------------------
    // Internal: raw byte scanning
    // ------------------------------------------------------------------

    /// Parse a raw log entry at the given file/offset position.
    ///
    /// Returns `(entry_size, LogEntry)` on success, or `None` if the bytes
    /// don't form a valid entry (e.g. zero-filled region past the last write).
    fn read_raw_entry(
        &self,
        file_num: u32,
        offset: u64,
    ) -> Option<(usize, Option<LogEntry>)> {
        // Read the minimum header.
        let mut hdr = [0u8; MIN_HEADER_SIZE];
        let n = self
            .file_manager
            .read_from_file(file_num, offset, &mut hdr)
            .ok()?;
        if n < MIN_HEADER_SIZE {
            return None;
        }

        // Zero-filled region past the last written byte — stop scanning.
        if hdr[4] == 0 {
            return None;
        }

        let entry_type_num = hdr[4];
        let flags = hdr[5];
        let item_size =
            u32::from_le_bytes([hdr[10], hdr[11], hdr[12], hdr[13]]) as usize;

        // Sanity-check item_size before allocating.
        if item_size > MAX_SANE_ITEM_SIZE {
            return None;
        }

        let vlsn_present =
            (flags & 0x08) != 0 || (flags & 0x20) != 0; // VLSN_PRESENT | REPLICATED
        let header_size =
            if vlsn_present { MAX_HEADER_SIZE } else { MIN_HEADER_SIZE };
        let entry_size = header_size + item_size;

        // Read the full entry (header + payload).
        let mut full_buf = vec![0u8; entry_size];
        let n = self
            .file_manager
            .read_from_file(file_num, offset, &mut full_buf)
            .ok()?;
        if n < entry_size {
            return None; // Truncated write at end of log.
        }

        let payload = &full_buf[header_size..];
        let log_entry = Self::parse_payload(entry_type_num, payload);

        Some((entry_size, log_entry))
    }

    /// Convert a raw (entry_type_num, payload) pair to a `LogEntry`.
    ///
    /// Returns `None` for entry types recovery does not need to process
    /// (FileHeader, Trace, etc.).
    fn parse_payload(
        entry_type_num: u8,
        payload: &[u8],
    ) -> Option<LogEntry> {
        let entry_type = LogEntryType::from_type_num(entry_type_num)?;

        match entry_type {
            // Transaction end ─────────────────────────────────────────
            LogEntryType::TxnCommit => {
                let e = TxnEndEntry::read_from_log(payload).ok()?;
                Some(LogEntry::TxnCommit(TxnCommitRecord {
                    txn_id: e.txn_id as u64,
                    lsn: NULL_LSN, // Filled in by caller with actual LSN.
                }))
            }
            LogEntryType::TxnAbort => {
                let e = TxnEndEntry::read_from_log(payload).ok()?;
                Some(LogEntry::TxnAbort(TxnAbortRecord {
                    txn_id: e.txn_id as u64,
                }))
            }

            // LN entries ──────────────────────────────────────────────
            LogEntryType::InsertLN
            | LogEntryType::InsertLNTxn
            | LogEntryType::UpdateLN
            | LogEntryType::UpdateLNTxn
            | LogEntryType::DeleteLN
            | LogEntryType::DeleteLNTxn => {
                let is_txn = matches!(
                    entry_type,
                    LogEntryType::InsertLNTxn
                        | LogEntryType::UpdateLNTxn
                        | LogEntryType::DeleteLNTxn
                );
                let e = LnLogEntry::read_from_log(payload, is_txn).ok()?;
                let op = match entry_type {
                    LogEntryType::InsertLN | LogEntryType::InsertLNTxn => {
                        LnOperation::Insert
                    }
                    LogEntryType::UpdateLN | LogEntryType::UpdateLNTxn => {
                        LnOperation::Update
                    }
                    _ => LnOperation::Delete,
                };
                let mut rec = LnRecord::new(
                    e.db_id,
                    e.txn_id.map(|id| id as u64),
                    op,
                    e.key,
                    e.data,
                    e.abort_lsn,
                    e.abort_known_deleted,
                );
                rec.abort_key = e.abort_key;
                rec.abort_data = e.abort_data;
                Some(LogEntry::Ln(rec))
            }

            // IN / BIN entries ────────────────────────────────────────
            LogEntryType::IN | LogEntryType::BIN => {
                let e = InLogEntry::read_from_log(payload).ok()?;
                // Extract node_id from the serialized node_data so the
                // recovery redo pass can key on it.  The format written by
                // BinStub::serialize_full() starts with node_id(u64BE).
                let node_id = if e.node_data.len() >= 8 {
                    u64::from_be_bytes(e.node_data[0..8].try_into().ok()?)
                } else {
                    0
                };
                Some(LogEntry::In(InRecord {
                    db_id: e.db_id,
                    node_id,
                    level: 0,    // level not embedded in this format; 0 = BIN
                    is_root: false,
                    is_delta: false,
                    node_data: Some(e.node_data),
                }))
            }
            LogEntryType::BINDelta => {
                let e = BinDeltaLogEntry::read_from_log(payload).ok()?;
                let node_id = if e.delta_data.len() >= 8 {
                    u64::from_be_bytes(e.delta_data[0..8].try_into().ok()?)
                } else {
                    0
                };
                Some(LogEntry::In(InRecord {
                    db_id: e.db_id,
                    node_id,
                    level: 0,
                    is_root: false,
                    is_delta: true,
                    node_data: Some(e.delta_data),
                }))
            }

            // Checkpoint entries ──────────────────────────────────────
            LogEntryType::CkptStart => {
                let e = CheckpointStart::read_from_log(payload).ok()?;
                Some(LogEntry::CkptStart(CkptStartRecord {
                    id: e.get_id(),
                    lsn: NULL_LSN, // Filled in by caller.
                }))
            }
            LogEntryType::CkptEnd => {
                let e = CheckpointEnd::read_from_log(payload).ok()?;
                Some(LogEntry::CkptEnd(CkptEndRecord {
                    id: e.get_id(),
                    checkpoint_start_lsn: e.get_checkpoint_start_lsn(),
                    first_active_lsn: e.get_first_active_lsn(),
                    root_lsn: e
                        .get_root_lsn()
                        .unwrap_or(NULL_LSN),
                    last_local_node_id: e.get_last_local_node_id(),
                    last_replicated_node_id: e.get_last_replicated_node_id(),
                    last_local_db_id: e.get_last_local_db_id(),
                    last_replicated_db_id: e.get_last_replicated_db_id(),
                    last_local_txn_id: e.get_last_local_txn_id(),
                    last_replicated_txn_id: e.get_last_replicated_txn_id(),
                }))
            }

            // Everything else (FileHeader, Trace, MapLN, NameLN, etc.) ─
            _ => None,
        }
    }

    /// Scan forward through all log files collecting `PositionedEntry` items
    /// whose LSN falls in `[start_lsn, end_lsn)`.
    ///
    /// `NULL_LSN` for `start_lsn` means "from the beginning of the first file".
    /// `NULL_LSN` for `end_lsn`   means "to the end of the log".
    fn scan_files_forward(
        &self,
        start_lsn: Lsn,
        end_lsn: Lsn,
    ) -> Vec<PositionedEntry> {
        let mut results = Vec::new();

        let file_nums = match self.file_manager.list_file_numbers() {
            Ok(nums) => nums,
            Err(_) => return results,
        };
        if file_nums.is_empty() {
            return results;
        }

        let start_file = if start_lsn == NULL_LSN {
            *file_nums.first().unwrap()
        } else {
            start_lsn.file_number()
        };

        for &file_num in file_nums.iter().filter(|&&n| n >= start_file) {
            // Determine where in this file to start reading.
            let file_start_offset: u64 =
                if start_lsn != NULL_LSN && file_num == start_lsn.file_number()
                {
                    start_lsn.file_offset() as u64
                } else {
                    FILE_HEADER_SIZE as u64
                };

            // Determine the file length.
            let file_len = match self.file_manager.get_file_length(file_num) {
                Ok(len) => len,
                Err(_) => continue,
            };

            let mut offset = file_start_offset;

            while offset < file_len {
                // Enforce end_lsn upper bound.
                if end_lsn != NULL_LSN {
                    let cur_lsn = Lsn::new(file_num, offset as u32);
                    if cur_lsn >= end_lsn {
                        return results;
                    }
                }

                match self.read_raw_entry(file_num, offset) {
                    None => break, // End of written data in this file.
                    Some((entry_size, parsed)) => {
                        let entry_lsn = Lsn::new(file_num, offset as u32);

                        if let Some(mut log_entry) = parsed {
                            // Patch the LSN into records that carry it.
                            match &mut log_entry {
                                LogEntry::TxnCommit(r) => {
                                    r.lsn = entry_lsn;
                                }
                                LogEntry::CkptStart(r) => {
                                    r.lsn = entry_lsn;
                                }
                                _ => {}
                            }

                            results.push(PositionedEntry::new(
                                entry_lsn, log_entry,
                            ));
                        }

                        offset += entry_size as u64;
                    }
                }
            }
        }

        results
    }
}

impl LogScanner for FileManagerLogScanner {
    /// Find the true end of the log by scanning through all log files.
    ///
    /// Also calls `FileManager::set_last_position()` to restore the
    /// manager's LSN state so subsequent writes continue from the correct
    /// position rather than overwriting existing data.
    ///
    /// Returns `(last_used_lsn, next_available_lsn)`.
    fn find_end_of_log(&mut self) -> (Lsn, Lsn) {
        let file_nums = match self.file_manager.list_file_numbers() {
            Ok(nums) => nums,
            Err(_) => return (NULL_LSN, NULL_LSN),
        };

        if file_nums.is_empty() {
            // Fresh environment: no log files written yet.
            return (NULL_LSN, NULL_LSN);
        }

        // Scan files from the last one backward to find the last valid entry.
        // We scan each file fully in forward order and keep the last valid
        // entry we see.  Scanning in reverse-file order lets us stop as soon
        // as we find a non-empty file.
        let mut last_used_lsn = NULL_LSN;
        let mut next_available_lsn = NULL_LSN;

        'outer: for &file_num in file_nums.iter().rev() {
            let file_len =
                match self.file_manager.get_file_length(file_num) {
                    Ok(l) => l,
                    Err(_) => continue,
                };

            let mut offset = FILE_HEADER_SIZE as u64;
            let mut last_valid_offset: Option<u64> = None;
            let mut last_entry_size = 0usize;

            while offset < file_len {
                match self.read_raw_entry(file_num, offset) {
                    None => break,
                    Some((entry_size, _)) => {
                        last_valid_offset = Some(offset);
                        last_entry_size = entry_size;
                        offset += entry_size as u64;
                    }
                }
            }

            if let Some(valid_offset) = last_valid_offset {
                let end_offset = valid_offset + last_entry_size as u64;
                last_used_lsn = Lsn::new(file_num, valid_offset as u32);

                // next_available is the byte immediately after the last entry.
                // If we're at the end of this file, the next write goes to
                // the start of the next file.
                next_available_lsn = if end_offset >= file_len
                    && file_nums.last().copied() != Some(file_num)
                {
                    let next_file = file_num + 1;
                    Lsn::new(next_file, FILE_HEADER_SIZE as u32)
                } else {
                    Lsn::new(file_num, end_offset as u32)
                };

                // Restore the FileManager's LSN state so subsequent log()
                // calls write after the last valid entry rather than
                // overwriting it at offset FILE_HEADER_SIZE.
                self.file_manager
                    .set_last_position(next_available_lsn, last_used_lsn);

                break 'outer;
            }
            // This file had no valid entries; check the previous file.
        }

        (last_used_lsn, next_available_lsn)
    }

    fn scan_forward(
        &self,
        start_lsn: Lsn,
        end_lsn: Lsn,
    ) -> Vec<PositionedEntry> {
        self.scan_files_forward(start_lsn, end_lsn)
    }

    fn scan_backward(
        &self,
        start_lsn: Lsn,
        stop_lsn: Lsn,
    ) -> Vec<PositionedEntry> {
        // Collect forward then reverse; this matches the InMemoryLogScanner
        // contract (entries in descending LSN order).
        let mut entries = self.scan_files_forward(stop_lsn, NULL_LSN);

        // Keep only entries at or below start_lsn.
        if start_lsn != NULL_LSN {
            entries.retain(|e| e.lsn <= start_lsn);
        }

        entries.sort_by(|a, b| b.lsn.cmp(&a.lsn));
        entries
    }

    /// Read the single log entry at exactly `target_lsn` by scanning the
    /// appropriate log file.
    ///
    /// Used during the undo phase to fetch the before-image of a
    /// disk-resident LN at its `abort_lsn`.
    ///
    /// Port of `RecoveryManager.undo()` → `fetchTarget(...)` in JE.
    fn read_at_lsn(&self, target_lsn: Lsn) -> Option<LogEntry> {
        if target_lsn == NULL_LSN {
            return None;
        }
        // scan_files_forward with end_lsn = next offset after target is
        // the cheapest way to read exactly one entry.
        let end_lsn = Lsn::new(
            target_lsn.file_number(),
            target_lsn.file_offset() + 1,
        );
        let entries = self.scan_files_forward(target_lsn, end_lsn);
        entries
            .into_iter()
            .find(|e| e.lsn == target_lsn)
            .map(|e| e.entry)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use noxu_log::{LogEntryType, LogManager, Provisional};
    use noxu_util::vlsn::NULL_VLSN;
    use noxu_log::entry::TxnEndEntry;
    use bytes::BytesMut;
    use tempfile::TempDir;

    fn make_manager(dir: &std::path::Path) -> (Arc<FileManager>, Arc<LogManager>) {
        let fm = Arc::new(
            FileManager::new(dir, false, 64 * 1024 * 1024, 100).unwrap(),
        );
        let lm = Arc::new(LogManager::new(Arc::clone(&fm), 3, 1024 * 1024, 65536));
        (fm, lm)
    }

    #[test]
    fn test_find_end_of_log_empty() {
        let dir = TempDir::new().unwrap();
        let (fm, _lm) = make_manager(dir.path());
        let mut scanner = FileManagerLogScanner::new(Arc::clone(&fm));
        let (last, next) = scanner.find_end_of_log();
        assert_eq!(last, NULL_LSN, "empty log: last_used should be NULL");
        assert_eq!(next, NULL_LSN, "empty log: next_available should be NULL");
    }

    #[test]
    fn test_find_end_of_log_after_write() {
        let dir = TempDir::new().unwrap();
        let (fm, lm) = make_manager(dir.path());

        // Write a commit entry.
        let entry = TxnEndEntry::new_commit(42, NULL_LSN, 0, 0, NULL_VLSN);
        let mut buf = BytesMut::with_capacity(entry.log_size());
        entry.write_to_log(&mut buf);
        lm.log(LogEntryType::TxnCommit, &buf, Provisional::No, true, false)
            .unwrap();
        lm.flush_sync().unwrap();
        drop(lm);

        // Re-open scanner and find end.
        let mut scanner = FileManagerLogScanner::new(Arc::clone(&fm));
        let (last, next) = scanner.find_end_of_log();
        assert_ne!(last, NULL_LSN, "after write: last_used should be non-null");
        assert!(next > last, "next_available must be after last_used");
    }

    #[test]
    fn test_scan_forward_finds_commit() {
        let dir = TempDir::new().unwrap();
        let (fm, lm) = make_manager(dir.path());

        // Write a commit for txn 99.
        let entry = TxnEndEntry::new_commit(99, NULL_LSN, 0, 0, NULL_VLSN);
        let mut buf = BytesMut::with_capacity(entry.log_size());
        entry.write_to_log(&mut buf);
        lm.log(LogEntryType::TxnCommit, &buf, Provisional::No, true, false)
            .unwrap();
        lm.flush_sync().unwrap();
        drop(lm);

        let mut scanner = FileManagerLogScanner::new(Arc::clone(&fm));
        let (_, next) = scanner.find_end_of_log();

        let entries = scanner.scan_forward(NULL_LSN, next);
        assert!(!entries.is_empty(), "should find at least one entry");

        let commit = entries
            .iter()
            .find(|e| matches!(e.entry, LogEntry::TxnCommit(ref r) if r.txn_id == 99));
        assert!(commit.is_some(), "should find TxnCommit for txn 99");
    }

    #[test]
    fn test_scan_backward_reverses_order() {
        let dir = TempDir::new().unwrap();
        let (fm, lm) = make_manager(dir.path());

        // Write two commits.
        for txn_id in [10i64, 20i64] {
            let e = TxnEndEntry::new_commit(txn_id, NULL_LSN, 0, 0, NULL_VLSN);
            let mut buf = BytesMut::with_capacity(e.log_size());
            e.write_to_log(&mut buf);
            lm.log(LogEntryType::TxnCommit, &buf, Provisional::No, true, false)
                .unwrap();
        }
        lm.flush_sync().unwrap();
        drop(lm);

        let mut scanner = FileManagerLogScanner::new(Arc::clone(&fm));
        let (last, _) = scanner.find_end_of_log();

        let entries = scanner.scan_backward(last, NULL_LSN);
        assert!(entries.len() >= 2, "should find both commits");

        // Entries should be in descending LSN order.
        for w in entries.windows(2) {
            assert!(w[0].lsn >= w[1].lsn, "backward scan must be descending");
        }
    }

    #[test]
    fn test_find_end_restores_file_manager_position() {
        let dir = TempDir::new().unwrap();
        let (fm, lm) = make_manager(dir.path());

        let e = TxnEndEntry::new_commit(7, NULL_LSN, 0, 0, NULL_VLSN);
        let mut buf = BytesMut::with_capacity(e.log_size());
        e.write_to_log(&mut buf);
        lm.log(LogEntryType::TxnCommit, &buf, Provisional::No, true, false)
            .unwrap();
        lm.flush_sync().unwrap();
        drop(lm);

        let mut scanner = FileManagerLogScanner::new(Arc::clone(&fm));
        let (last, next) = scanner.find_end_of_log();

        // After find_end_of_log, the file manager should reflect those LSNs.
        assert_eq!(fm.get_last_used_lsn(), last);
        assert_eq!(fm.get_next_available_lsn(), next);
    }
}
