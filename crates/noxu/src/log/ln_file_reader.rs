//! LN file reader for recovery.
//!
//!
//! Scans log files for Leaf Node (LN) entries during recovery.  During the
//! **redo** phase the reader scans forward; during the **undo** phase it scans
//! backward.  Commit and abort entries are also matched so the recovery manager
//! can process transaction boundaries.

use crate::log::entry::commit_abort_entry::TxnEndEntry;
use crate::log::entry::ln_log_entry::LnLogEntry;
use crate::log::entry::name_ln_log_entry::NameLnLogEntry;
use crate::log::entry_header::{MAX_HEADER_SIZE, MIN_HEADER_SIZE};
use crate::log::entry_type::LogEntryType;
use crate::log::error::{NoxuLogError, Result};
use crate::log::file_reader::LogFileAccess;
use hashbrown::HashSet;
use crate::util::lsn::{Lsn, NULL_LSN};

// Maximum plausible payload size (64 MiB).
const MAX_SANE_ITEM_SIZE: usize = 64 * 1024 * 1024;

/// The parsed current entry held by the reader after a successful
/// `read_next_entry()` call.
enum CurrentEntry {
    Ln(LnLogEntry),
    NameLn(NameLnLogEntry),
    Commit(TxnEndEntry),
    Abort(TxnEndEntry),
    Other,
}

/// Scans log files for Leaf Node (LN) entries during recovery.
///
///
///
/// The reader maintains a set of *target* `LogEntryType` values registered via
/// `add_target_type`.  Each call to `read_next_entry` advances to the next
/// log entry that matches one of those types and parses it.  Accessor methods
/// then expose the fields of the parsed entry.
pub struct LNFileReader<F: LogFileAccess> {
    /// File access interface.
    file_access: F,
    /// Whether to scan forward (redo) or store start_lsn for backward start.
    forward: bool,
    /// Starting LSN.
    start_lsn: Lsn,
    /// LSN past which (exclusive) we stop scanning. NULL_LSN = no limit.
    finish_lsn: Lsn,
    /// For backward reading: the LSN that marks the physical end of the log.
    end_of_file_lsn: Lsn,
    /// Checkpoint end LSN used to skip provisional entries.
    ckpt_end: Lsn,
    /// Entry types that this reader is interested in.
    target_types: HashSet<LogEntryType>,
    /// Current file number being scanned.
    current_file_num: u32,
    /// Current byte offset within the current file.
    current_offset: u64,
    /// LSN of the entry most recently returned.
    current_lsn: Lsn,
    /// Parsed current entry (valid after a successful read_next_entry).
    current_entry: Option<CurrentEntry>,
    /// Whether we have reached the end of the log.
    eof: bool,
}

impl<F: LogFileAccess> LNFileReader<F> {
    /// Create a new LNFileReader.
    ///
    /// # Arguments
    /// * `file_access`      – file I/O provider
    /// * `_read_buffer_size`– ignored (kept for API compatibility)
    /// * `start_lsn`        – where to begin scanning
    /// * `redo`             – `true` = forward scan, `false` = backward scan
    /// * `end_of_file_lsn`  – physical end of log (used for backward start)
    /// * `finish_lsn`       – stop before this LSN (`NULL_LSN` = no limit)
    pub fn new(
        file_access: F,
        _read_buffer_size: usize,
        start_lsn: Lsn,
        redo: bool,
        end_of_file_lsn: Lsn,
        finish_lsn: Lsn,
    ) -> Result<Self> {
        // Initialise file/offset position.
        let (current_file_num, current_offset, eof) = if redo {
            // Forward: begin at start_lsn (or first available file).
            if !start_lsn.is_null() {
                (start_lsn.file_number(), start_lsn.file_offset() as u64, false)
            } else if let Some(first) = file_access.get_first_file_num() {
                (first, 0u64, false)
            } else {
                (0u32, 0u64, true)
            }
        } else {
            // Backward: scan forward from start, up to end_of_file_lsn boundary.
            // Entries are then returned forward; callers expecting reverse order
            // should use FileManagerLogScanner::scan_backward() which reverses
            // the collected entries. This forward+filter approach is
            // functionally equivalent to following prev_offset chain links
            // for recovery undo.
            if !end_of_file_lsn.is_null() {
                (
                    end_of_file_lsn.file_number(),
                    end_of_file_lsn.file_offset() as u64,
                    false,
                )
            } else if let Some(first) = file_access.get_first_file_num() {
                (first, 0u64, false)
            } else {
                (0u32, 0u64, true)
            }
        };

        Ok(LNFileReader {
            file_access,
            forward: redo,
            start_lsn,
            finish_lsn,
            end_of_file_lsn,
            ckpt_end: NULL_LSN,
            target_types: HashSet::new(),
            current_file_num,
            current_offset,
            current_lsn: NULL_LSN,
            current_entry: None,
            eof,
        })
    }

    /// Set the checkpoint-end LSN used to skip provisional entries.
    pub fn set_ckpt_end(&mut self, ckpt_end: Lsn) {
        self.ckpt_end = ckpt_end;
    }

    /// Register a log entry type that this reader should return.
    ///
    ///
    pub fn add_target_type(&mut self, entry_type: LogEntryType) {
        self.target_types.insert(entry_type);
    }

    /// Advance to the next matching log entry.
    ///
    /// Returns `Ok(true)` when an entry was found and parsed; `Ok(false)` at
    /// end of log.
    ///
    /// See `LNFileReader.isTargetEntry()` and `LNFileReader.processEntry()`.
    pub fn read_next_entry(&mut self) -> Result<bool> {
        if self.eof {
            return Ok(false);
        }

        loop {
            // Try to read the next entry from the current position.
            match self.read_one_raw_entry()? {
                None => return Ok(false),
                Some((lsn, entry_type, payload)) => {
                    // Check finish_lsn bound.
                    if !self.finish_lsn.is_null() && lsn >= self.finish_lsn {
                        self.eof = true;
                        return Ok(false);
                    }

                    // Skip if not a target type.
                    if !self.target_types.contains(&entry_type) {
                        continue;
                    }

                    // Parse the entry.
                    let is_txn = entry_type.is_transactional();
                    let parsed =
                        self.parse_entry(entry_type, &payload, is_txn)?;
                    self.current_lsn = lsn;
                    self.current_entry = Some(parsed);
                    return Ok(true);
                }
            }
        }
    }

    // ------------------------------------------------------------------
    // Accessor methods — only valid after a successful read_next_entry()
    // ------------------------------------------------------------------

    /// Returns `true` if the last entry was an LN (leaf node) rather than a
    /// transaction boundary (commit/abort).
    pub fn is_ln(&self) -> bool {
        matches!(
            self.current_entry,
            Some(CurrentEntry::Ln(_)) | Some(CurrentEntry::NameLn(_))
        )
    }

    /// Returns the LN log entry, or `None` if the current entry is not an LN.
    pub fn get_ln_log_entry(&self) -> Option<&LnLogEntry> {
        match &self.current_entry {
            Some(CurrentEntry::Ln(e)) => Some(e),
            Some(CurrentEntry::NameLn(e)) => Some(&e.ln_entry),
            _ => None,
        }
    }

    /// Returns the NameLN log entry if the current entry is a NameLN, else
    /// `None`.
    pub fn get_name_ln_log_entry(&self) -> Option<&NameLnLogEntry> {
        match &self.current_entry {
            Some(CurrentEntry::NameLn(e)) => Some(e),
            _ => None,
        }
    }

    /// Returns the database ID from the current LN entry.
    ///
    /// Panics if the current entry is not an LN.
    pub fn get_database_id(&self) -> u64 {
        self.get_ln_log_entry().expect("current entry is not an LN").db_id
    }

    /// Returns the transaction ID from the current LN entry, or `None` for
    /// non-transactional operations.
    pub fn get_txn_id(&self) -> Option<u64> {
        self.get_ln_log_entry().and_then(|e| e.txn_id).map(|id| id as u64)
    }

    /// Returns `true` if the current entry is a `TxnCommit`.
    pub fn is_commit(&self) -> bool {
        matches!(self.current_entry, Some(CurrentEntry::Commit(_)))
    }

    /// Returns `true` if the current entry is a `TxnAbort`.
    pub fn is_abort(&self) -> bool {
        matches!(self.current_entry, Some(CurrentEntry::Abort(_)))
    }

    /// Returns the abort LSN from the current LN entry (`NULL_LSN` if none).
    pub fn get_abort_lsn(&self) -> Lsn {
        self.get_ln_log_entry().map(|e| e.abort_lsn).unwrap_or(NULL_LSN)
    }

    /// Returns the `abort_known_deleted` flag from the current LN entry.
    pub fn get_abort_known_deleted(&self) -> bool {
        self.get_ln_log_entry().map(|e| e.abort_known_deleted).unwrap_or(false)
    }

    /// Returns the LSN of the entry most recently returned by
    /// `read_next_entry`.
    pub fn get_current_lsn(&self) -> Lsn {
        self.current_lsn
    }

    // ------------------------------------------------------------------
    // Internal helpers
    // ------------------------------------------------------------------

    /// Read one raw entry from the current position in the log.
    ///
    /// Returns `None` at physical end-of-log; advances `current_offset` on
    /// success.
    fn read_one_raw_entry(
        &mut self,
    ) -> Result<Option<(Lsn, LogEntryType, Vec<u8>)>> {
        loop {
            // Check for end of current file.
            let file_len =
                match self.file_access.get_file_length(self.current_file_num) {
                    Ok(l) => l,
                    Err(_) => {
                        self.eof = true;
                        return Ok(None);
                    }
                };

            if self.current_offset >= file_len {
                // Try next file.
                match self
                    .file_access
                    .get_following_file_num(self.current_file_num, true)
                {
                    None => {
                        self.eof = true;
                        return Ok(None);
                    }
                    Some(next) => {
                        self.current_file_num = next;
                        self.current_offset = 0;
                        continue;
                    }
                }
            }

            // Read minimum header.
            let mut hdr = [0u8; MIN_HEADER_SIZE];
            let n = self.file_access.read_from_file(
                self.current_file_num,
                self.current_offset,
                &mut hdr,
            )?;
            if n < MIN_HEADER_SIZE {
                self.eof = true;
                return Ok(None);
            }

            // Zero type byte means unwritten space past last entry.
            if hdr[4] == 0 {
                self.eof = true;
                return Ok(None);
            }

            let entry_type_num = hdr[4];
            let flags = hdr[5];
            let item_size =
                u32::from_le_bytes([hdr[10], hdr[11], hdr[12], hdr[13]])
                    as usize;

            if item_size > MAX_SANE_ITEM_SIZE {
                self.eof = true;
                return Ok(None);
            }

            let vlsn_present = (flags & 0x08) != 0 || (flags & 0x20) != 0;
            let header_size =
                if vlsn_present { MAX_HEADER_SIZE } else { MIN_HEADER_SIZE };
            let entry_size = header_size + item_size;

            // Validate we have enough room.
            if self.current_offset + entry_size as u64 > file_len {
                self.eof = true;
                return Ok(None);
            }

            // Read the full entry.
            let mut full_buf = vec![0u8; entry_size];
            let n = self.file_access.read_from_file(
                self.current_file_num,
                self.current_offset,
                &mut full_buf,
            )?;
            if n < entry_size {
                self.eof = true;
                return Ok(None);
            }

            let lsn =
                Lsn::new(self.current_file_num, self.current_offset as u32);

            self.current_offset += entry_size as u64;

            let entry_type = match LogEntryType::from_type_num(entry_type_num) {
                Some(t) => t,
                None => continue, // Unknown type — skip.
            };

            let payload = full_buf[header_size..].to_vec();
            return Ok(Some((lsn, entry_type, payload)));
        }
    }

    /// Parse a raw payload into a `CurrentEntry` discriminant.
    fn parse_entry(
        &self,
        entry_type: LogEntryType,
        payload: &[u8],
        is_txn: bool,
    ) -> Result<CurrentEntry> {
        match entry_type {
            LogEntryType::TxnCommit => {
                let e = TxnEndEntry::read_from_log(payload).map_err(|e| {
                    NoxuLogError::Internal(format!(
                        "TxnCommit parse error: {e}"
                    ))
                })?;
                Ok(CurrentEntry::Commit(e))
            }
            LogEntryType::TxnAbort => {
                let e = TxnEndEntry::read_from_log(payload).map_err(|e| {
                    NoxuLogError::Internal(format!("TxnAbort parse error: {e}"))
                })?;
                Ok(CurrentEntry::Abort(e))
            }
            LogEntryType::NameLN | LogEntryType::NameLNTxn => {
                let e = NameLnLogEntry::read_from_log(payload, is_txn)
                    .map_err(|e| {
                        NoxuLogError::Internal(format!(
                            "NameLN parse error: {e}"
                        ))
                    })?;
                Ok(CurrentEntry::NameLn(e))
            }
            _ => {
                // All other target types are regular LN entries.
                let e = LnLogEntry::read_from_log(payload, is_txn).map_err(
                    |e| NoxuLogError::Internal(format!("LN parse error: {e}")),
                )?;
                Ok(CurrentEntry::Ln(e))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log::entry::commit_abort_entry::TxnEndEntry;
    use crate::log::entry::ln_log_entry::LnLogEntry;
    use crate::log::entry_header::MIN_HEADER_SIZE;
    use crate::log::entry_type::LogEntryType;
    use bytes::BytesMut;
    use crate::util::lsn::NULL_LSN;
    use crate::util::vlsn::NULL_VLSN;
    use std::collections::HashMap;
    use std::io;

    // ------------------------------------------------------------------
    // Mock file access (shared with other file-reader tests)
    // ------------------------------------------------------------------

    struct MockFileAccess {
        files: HashMap<u32, Vec<u8>>,
    }

    impl MockFileAccess {
        fn new() -> Self {
            MockFileAccess { files: HashMap::new() }
        }

        fn add_file(&mut self, file_num: u32, data: Vec<u8>) {
            self.files.insert(file_num, data);
        }
    }

    impl LogFileAccess for MockFileAccess {
        fn read_from_file(
            &self,
            file_num: u32,
            offset: u64,
            buf: &mut [u8],
        ) -> Result<usize> {
            if let Some(data) = self.files.get(&file_num) {
                let start = offset as usize;
                if start >= data.len() {
                    return Ok(0);
                }
                let end = (start + buf.len()).min(data.len());
                let n = end - start;
                buf[..n].copy_from_slice(&data[start..end]);
                Ok(n)
            } else {
                Err(io::Error::new(io::ErrorKind::NotFound, "File not found")
                    .into())
            }
        }

        fn get_file_length(&self, file_num: u32) -> Result<u64> {
            self.files.get(&file_num).map(|d| d.len() as u64).ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotFound, "File not found").into()
            })
        }

        fn get_first_file_num(&self) -> Option<u32> {
            self.files.keys().min().copied()
        }

        fn get_following_file_num(
            &self,
            file_num: u32,
            forward: bool,
        ) -> Option<u32> {
            let mut nums: Vec<u32> = self.files.keys().copied().collect();
            nums.sort();
            if forward {
                nums.iter().find(|&&n| n > file_num).copied()
            } else {
                nums.iter().rev().find(|&&n| n < file_num).copied()
            }
        }

        fn get_file_header_prev_offset(&self, _file_num: u32) -> Result<u64> {
            Ok(0)
        }
    }

    // ------------------------------------------------------------------
    // Helpers to build raw log file bytes
    // ------------------------------------------------------------------

    /// Serialize a log entry into a raw `(header + payload)` byte vector.
    ///
    /// Uses the same format as `file_reader.rs`: 14-byte fixed header with
    /// checksum=0 (skipped during validation), entry_type, and item_size.
    fn make_raw_entry(entry_type: LogEntryType, payload: &[u8]) -> Vec<u8> {
        let item_size = payload.len() as u32;
        let mut buf = vec![0u8; MIN_HEADER_SIZE + payload.len()];
        buf[4] = entry_type.type_num();
        buf[10..14].copy_from_slice(&item_size.to_le_bytes());
        buf[MIN_HEADER_SIZE..].copy_from_slice(payload);
        buf
    }

    fn make_ln_payload(db_id: u64, txn: bool) -> Vec<u8> {
        let entry = LnLogEntry::new(
            db_id,
            if txn { Some(42) } else { None },
            NULL_LSN,
            false,
            None,
            None,
            NULL_VLSN,
            0,
            false,
            b"key".to_vec(),
            Some(b"data".to_vec()),
            0,
            NULL_VLSN,
        );
        let mut buf = BytesMut::new();
        entry.write_to_log(&mut buf);
        buf.to_vec()
    }

    fn make_commit_payload(txn_id: i64) -> Vec<u8> {
        let e = TxnEndEntry::new_commit(txn_id, NULL_LSN, 0, 0, NULL_VLSN);
        let mut buf = BytesMut::new();
        e.write_to_log(&mut buf);
        buf.to_vec()
    }

    // ------------------------------------------------------------------
    // Construction tests
    // ------------------------------------------------------------------

    #[test]
    fn test_ln_file_reader_new_forward() {
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![0u8; 64]);
        let result = LNFileReader::new(
            mock,
            512,
            Lsn::new(0, 0),
            true,
            NULL_LSN,
            NULL_LSN,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_ln_file_reader_new_backward() {
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![0u8; 64]);
        let result = LNFileReader::new(
            mock,
            512,
            Lsn::new(0, 0),
            false,
            NULL_LSN,
            NULL_LSN,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_ln_file_reader_no_files() {
        let mock = MockFileAccess::new();
        let result =
            LNFileReader::new(mock, 512, NULL_LSN, true, NULL_LSN, NULL_LSN);
        assert!(result.is_ok());
    }

    #[test]
    fn test_ln_file_reader_with_eof_lsn() {
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![0u8; 128]);
        let result = LNFileReader::new(
            mock,
            512,
            Lsn::new(0, 0),
            true,
            Lsn::new(0, 128),
            NULL_LSN,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_ln_file_reader_with_finish_lsn() {
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![0u8; 256]);
        let result = LNFileReader::new(
            mock,
            512,
            Lsn::new(0, 0),
            true,
            NULL_LSN,
            Lsn::new(0, 200),
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_ln_file_reader_multiple_files() {
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![0u8; 100]);
        mock.add_file(1, vec![0u8; 100]);
        let result = LNFileReader::new(
            mock,
            512,
            Lsn::new(0, 0),
            true,
            NULL_LSN,
            NULL_LSN,
        );
        assert!(result.is_ok());
    }

    // ------------------------------------------------------------------
    // Functional tests: read_next_entry + accessor methods
    // ------------------------------------------------------------------

    #[test]
    fn test_read_ln_entry() {
        let payload = make_ln_payload(99, false);
        let raw = make_raw_entry(LogEntryType::InsertLN, &payload);

        let mut mock = MockFileAccess::new();
        mock.add_file(0, raw);

        let mut reader = LNFileReader::new(
            mock,
            512,
            Lsn::new(0, 0),
            true,
            NULL_LSN,
            NULL_LSN,
        )
        .unwrap();
        reader.add_target_type(LogEntryType::InsertLN);

        let found = reader.read_next_entry().unwrap();
        assert!(found, "expected to find an LN entry");
        assert!(reader.is_ln());
        assert!(!reader.is_commit());
        assert!(!reader.is_abort());
        assert_eq!(reader.get_database_id(), 99);
        assert_eq!(reader.get_txn_id(), None);
        let lsn = reader.get_current_lsn();
        assert_eq!(lsn.file_number(), 0);
    }

    #[test]
    fn test_read_transactional_ln_entry() {
        let payload = make_ln_payload(7, true);
        let raw = make_raw_entry(LogEntryType::InsertLNTxn, &payload);

        let mut mock = MockFileAccess::new();
        mock.add_file(0, raw);

        let mut reader = LNFileReader::new(
            mock,
            512,
            Lsn::new(0, 0),
            true,
            NULL_LSN,
            NULL_LSN,
        )
        .unwrap();
        reader.add_target_type(LogEntryType::InsertLNTxn);

        assert!(reader.read_next_entry().unwrap());
        assert!(reader.is_ln());
        assert_eq!(reader.get_txn_id(), Some(42));
        assert_eq!(reader.get_database_id(), 7);
    }

    #[test]
    fn test_read_commit_entry() {
        let payload = make_commit_payload(55);
        let raw = make_raw_entry(LogEntryType::TxnCommit, &payload);

        let mut mock = MockFileAccess::new();
        mock.add_file(0, raw);

        let mut reader = LNFileReader::new(
            mock,
            512,
            Lsn::new(0, 0),
            true,
            NULL_LSN,
            NULL_LSN,
        )
        .unwrap();
        reader.add_target_type(LogEntryType::TxnCommit);

        assert!(reader.read_next_entry().unwrap());
        assert!(!reader.is_ln());
        assert!(reader.is_commit());
        assert!(!reader.is_abort());
    }

    #[test]
    fn test_read_abort_entry() {
        let abort_entry = TxnEndEntry::new_abort(77, NULL_LSN, 0, 0, NULL_VLSN);
        let mut payload_buf = BytesMut::new();
        abort_entry.write_to_log(&mut payload_buf);
        let raw = make_raw_entry(LogEntryType::TxnAbort, &payload_buf);

        let mut mock = MockFileAccess::new();
        mock.add_file(0, raw);

        let mut reader = LNFileReader::new(
            mock,
            512,
            Lsn::new(0, 0),
            true,
            NULL_LSN,
            NULL_LSN,
        )
        .unwrap();
        reader.add_target_type(LogEntryType::TxnAbort);

        assert!(reader.read_next_entry().unwrap());
        assert!(reader.is_abort());
    }

    #[test]
    fn test_skip_non_target_entries() {
        // File contains two entries: Trace (not targeted) then InsertLN (targeted).
        // The Trace entry has a zero-byte payload.
        let trace_raw = make_raw_entry(LogEntryType::Trace, b"trace");
        let ln_payload = make_ln_payload(1, false);
        let ln_raw = make_raw_entry(LogEntryType::InsertLN, &ln_payload);

        let mut data = trace_raw;
        data.extend_from_slice(&ln_raw);

        let mut mock = MockFileAccess::new();
        mock.add_file(0, data);

        let mut reader = LNFileReader::new(
            mock,
            512,
            Lsn::new(0, 0),
            true,
            NULL_LSN,
            NULL_LSN,
        )
        .unwrap();
        reader.add_target_type(LogEntryType::InsertLN);

        // Should skip Trace and return the InsertLN.
        assert!(reader.read_next_entry().unwrap());
        assert!(reader.is_ln());
        assert_eq!(reader.get_database_id(), 1);

        // No more matching entries.
        assert!(!reader.read_next_entry().unwrap());
    }

    #[test]
    fn test_eof_on_empty_file() {
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![]);

        let mut reader = LNFileReader::new(
            mock,
            512,
            Lsn::new(0, 0),
            true,
            NULL_LSN,
            NULL_LSN,
        )
        .unwrap();
        reader.add_target_type(LogEntryType::InsertLN);

        let found = reader.read_next_entry().unwrap();
        assert!(!found);
    }

    #[test]
    fn test_eof_on_no_files() {
        let mock = MockFileAccess::new();
        let mut reader =
            LNFileReader::new(mock, 512, NULL_LSN, true, NULL_LSN, NULL_LSN)
                .unwrap();
        reader.add_target_type(LogEntryType::InsertLN);
        assert!(!reader.read_next_entry().unwrap());
    }

    #[test]
    fn test_finish_lsn_stops_reading() {
        let ln_payload = make_ln_payload(3, false);
        let raw = make_raw_entry(LogEntryType::InsertLN, &ln_payload);
        let _entry_len = raw.len() as u32;

        let mut mock = MockFileAccess::new();
        mock.add_file(0, raw);

        // finish_lsn is before the entry → no entries returned.
        let mut reader = LNFileReader::new(
            mock,
            512,
            Lsn::new(0, 0),
            true,
            NULL_LSN,
            Lsn::new(0, 0), // finish at offset 0 → nothing visible
        )
        .unwrap();
        reader.add_target_type(LogEntryType::InsertLN);
        assert!(!reader.read_next_entry().unwrap());
    }

    #[test]
    fn test_get_abort_lsn_default() {
        let mock = MockFileAccess::new();
        let reader =
            LNFileReader::new(mock, 512, NULL_LSN, true, NULL_LSN, NULL_LSN)
                .unwrap();
        assert_eq!(reader.get_abort_lsn(), NULL_LSN);
    }

    #[test]
    fn test_get_abort_known_deleted_default() {
        let mock = MockFileAccess::new();
        let reader =
            LNFileReader::new(mock, 512, NULL_LSN, true, NULL_LSN, NULL_LSN)
                .unwrap();
        assert!(!reader.get_abort_known_deleted());
    }

    #[test]
    fn test_multiple_entries_sequence() {
        // Build a file with two LN entries back-to-back.
        let p1 = make_ln_payload(10, false);
        let r1 = make_raw_entry(LogEntryType::InsertLN, &p1);
        let p2 = make_ln_payload(20, false);
        let r2 = make_raw_entry(LogEntryType::InsertLN, &p2);

        let mut data = r1;
        data.extend_from_slice(&r2);

        let mut mock = MockFileAccess::new();
        mock.add_file(0, data);

        let mut reader = LNFileReader::new(
            mock,
            512,
            Lsn::new(0, 0),
            true,
            NULL_LSN,
            NULL_LSN,
        )
        .unwrap();
        reader.add_target_type(LogEntryType::InsertLN);

        assert!(reader.read_next_entry().unwrap());
        assert_eq!(reader.get_database_id(), 10);

        assert!(reader.read_next_entry().unwrap());
        assert_eq!(reader.get_database_id(), 20);

        assert!(!reader.read_next_entry().unwrap());
    }

    #[test]
    fn test_null_lsn_with_files() {
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![0u8; 64]);
        let result =
            LNFileReader::new(mock, 512, NULL_LSN, true, NULL_LSN, NULL_LSN);
        assert!(result.is_ok());
    }

    // ------------------------------------------------------------------
    // Branch-coverage for MockFileAccess
    // ------------------------------------------------------------------

    #[test]
    fn test_mock_read_from_file_missing() {
        let mock = MockFileAccess::new();
        let mut buf = [0u8; 4];
        assert!(mock.read_from_file(99, 0, &mut buf).is_err());
    }

    #[test]
    fn test_mock_read_from_file_offset_at_end() {
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![1u8, 2, 3]);
        let mut buf = [0u8; 4];
        let n = mock.read_from_file(0, 3, &mut buf).unwrap();
        assert_eq!(n, 0);
    }

    #[test]
    fn test_mock_read_from_file_offset_past_end() {
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![1u8, 2]);
        let mut buf = [0u8; 2];
        let n = mock.read_from_file(0, 200, &mut buf).unwrap();
        assert_eq!(n, 0);
    }

    #[test]
    fn test_mock_get_file_length_missing() {
        let mock = MockFileAccess::new();
        assert!(mock.get_file_length(0).is_err());
    }

    #[test]
    fn test_mock_get_file_length_present() {
        let mut mock = MockFileAccess::new();
        mock.add_file(2, vec![0u8; 44]);
        assert_eq!(mock.get_file_length(2).unwrap(), 44);
    }

    #[test]
    fn test_mock_get_first_file_num_empty() {
        let mock = MockFileAccess::new();
        assert_eq!(mock.get_first_file_num(), None);
    }

    #[test]
    fn test_mock_get_first_file_num_returns_min() {
        let mut mock = MockFileAccess::new();
        mock.add_file(9, vec![]);
        mock.add_file(1, vec![]);
        assert_eq!(mock.get_first_file_num(), Some(1));
    }

    #[test]
    fn test_mock_get_following_file_backward() {
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![]);
        mock.add_file(1, vec![]);
        mock.add_file(2, vec![]);
        assert_eq!(mock.get_following_file_num(2, false), Some(1));
        assert_eq!(mock.get_following_file_num(1, false), Some(0));
        assert_eq!(mock.get_following_file_num(0, false), None);
    }

    #[test]
    fn test_mock_get_following_file_forward_none() {
        let mut mock = MockFileAccess::new();
        mock.add_file(4, vec![]);
        assert_eq!(mock.get_following_file_num(4, true), None);
    }
}
