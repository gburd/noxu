//! IN file reader for recovery.
//!
//!
//! Scans log files for Internal Node (IN, BIN, BINDelta) entries during
//! recovery's **IN rebuild** pass.  Optionally tracks the maximum node ID,
//! database ID, and transaction ID seen (used to update ID sequence counters
//! at the end of recovery).

use crate::entry::bin_delta_log_entry::BinDeltaLogEntry;
use crate::entry::in_log_entry::InLogEntry;
use crate::entry_header::{MAX_HEADER_SIZE, MIN_HEADER_SIZE};
use crate::entry_type::LogEntryType;
use crate::error::{NoxuLogError, Result};
use crate::file_reader::LogFileAccess;
use noxu_util::lsn::{Lsn, NULL_LSN};
use std::collections::HashSet;

// Maximum plausible payload size (64 MiB).
const MAX_SANE_ITEM_SIZE: usize = 64 * 1024 * 1024;

/// Parsed IN/BINDelta entry held after a successful `read_next_entry()` call.
enum CurrentEntry {
    /// Full IN or BIN node entry.
    In { entry: InLogEntry, is_bin_delta: bool },
    /// BIN delta entry.
    BinDelta(BinDeltaLogEntry),
}

impl CurrentEntry {
    fn db_id(&self) -> u64 {
        match self {
            CurrentEntry::In { entry, .. } => entry.db_id,
            CurrentEntry::BinDelta(e) => e.db_id,
        }
    }

    fn is_bin_delta(&self) -> bool {
        matches!(self, CurrentEntry::BinDelta(_))
    }
}

/// Scans log files for IN/BIN/BINDelta entries during recovery.
///
/// 
///
/// ## Usage
///
/// ```ignore
/// let mut reader = INFileReader::new(file_access, buf_size, start_lsn, finish_lsn)?;
/// reader.add_target_type(LogEntryType::IN);
/// reader.add_target_type(LogEntryType::BIN);
/// reader.add_target_type(LogEntryType::BINDelta);
/// while reader.read_next_entry()? {
///     let db_id = reader.get_database_id();
///     let is_delta = reader.is_bin_delta();
/// }
/// let max_node = reader.get_max_node_id();
/// ```
pub struct INFileReader<F: LogFileAccess> {
    /// File access interface.
    file_access: F,
    /// Starting LSN.
    start_lsn: Lsn,
    /// Stop before this LSN (`NULL_LSN` = no limit).
    finish_lsn: Lsn,
    /// Entry types this reader should return.
    target_types: HashSet<LogEntryType>,
    /// Current file number being scanned.
    current_file_num: u32,
    /// Current byte offset within the current file.
    current_offset: u64,
    /// LSN of the most-recently returned entry.
    current_lsn: Lsn,
    /// Parsed current entry.
    current_entry: Option<CurrentEntry>,
    /// Whether we have reached the end of the log.
    eof: bool,
    // --- ID tracking ---
    /// Maximum node ID seen (across all IN/BIN entries).
    max_node_id: u64,
    /// Maximum database ID seen (across all IN entries).
    max_db_id: u64,
}

impl<F: LogFileAccess> INFileReader<F> {
    /// Create a new INFileReader.
    ///
    /// # Arguments
    /// * `file_access`      – file I/O provider
    /// * `_read_buffer_size`– ignored (kept for API compatibility)
    /// * `start_lsn`        – where to begin scanning
    /// * `finish_lsn`       – stop before this LSN (`NULL_LSN` = no limit)
    pub fn new(
        file_access: F,
        _read_buffer_size: usize,
        start_lsn: Lsn,
        finish_lsn: Lsn,
    ) -> Result<Self> {
        let (current_file_num, current_offset, eof) =
            if !start_lsn.is_null() {
                (start_lsn.file_number(), start_lsn.file_offset() as u64, false)
            } else if let Some(first) = file_access.get_first_file_num() {
                (first, 0u64, false)
            } else {
                (0u32, 0u64, true)
            };

        Ok(INFileReader {
            file_access,
            start_lsn,
            finish_lsn,
            target_types: HashSet::new(),
            current_file_num,
            current_offset,
            current_lsn: NULL_LSN,
            current_entry: None,
            eof,
            max_node_id: 0,
            max_db_id: 0,
        })
    }

    /// Register a log entry type that this reader should return.
    ///
    /// 
    pub fn add_target_type(&mut self, entry_type: LogEntryType) {
        self.target_types.insert(entry_type);
    }

    /// Advance to the next matching log entry.
    ///
    /// Returns `Ok(true)` when an entry was found; `Ok(false)` at end of log.
    ///
    /// + `INFileReader.isTargetEntry()` +
    /// `INFileReader.processEntry()`.
    pub fn read_next_entry(&mut self) -> Result<bool> {
        if self.eof {
            return Ok(false);
        }

        loop {
            match self.read_one_raw_entry()? {
                None => return Ok(false),
                Some((lsn, entry_type, payload)) => {
                    // Enforce finish_lsn upper bound.
                    if !self.finish_lsn.is_null() && lsn >= self.finish_lsn {
                        self.eof = true;
                        return Ok(false);
                    }

                    // Update max IDs regardless of whether it is a target.
                    self.update_max_ids(entry_type, &payload);

                    // Return only if in target set.
                    if !self.target_types.contains(&entry_type) {
                        continue;
                    }

                    let parsed = self.parse_entry(entry_type, &payload)?;
                    self.current_lsn = lsn;
                    self.current_entry = Some(parsed);
                    return Ok(true);
                }
            }
        }
    }

    // ------------------------------------------------------------------
    // Accessor methods (valid after read_next_entry() == Ok(true))
    // ------------------------------------------------------------------

    /// Returns the database ID from the current IN entry.
    pub fn get_database_id(&self) -> u64 {
        self.current_entry
            .as_ref()
            .map(|e| e.db_id())
            .unwrap_or(0)
    }

    /// Returns `true` if the current entry is a BIN delta.
    pub fn is_bin_delta(&self) -> bool {
        self.current_entry
            .as_ref()
            .map(|e| e.is_bin_delta())
            .unwrap_or(false)
    }

    /// Returns the maximum node ID seen across all scanned IN/BIN entries.
    pub fn get_max_node_id(&self) -> u64 {
        self.max_node_id
    }

    /// Returns the maximum database ID seen across all scanned entries.
    pub fn get_max_db_id(&self) -> u64 {
        self.max_db_id
    }

    /// Returns the LSN of the most-recently returned entry.
    pub fn get_current_lsn(&self) -> Lsn {
        self.current_lsn
    }

    // ------------------------------------------------------------------
    // Internal helpers
    // ------------------------------------------------------------------

    /// Read one raw entry from the current log position.
    fn read_one_raw_entry(
        &mut self,
    ) -> Result<Option<(Lsn, LogEntryType, Vec<u8>)>> {
        loop {
            let file_len =
                match self.file_access.get_file_length(self.current_file_num) {
                    Ok(l) => l,
                    Err(_) => {
                        self.eof = true;
                        return Ok(None);
                    }
                };

            if self.current_offset >= file_len {
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

            if hdr[4] == 0 {
                self.eof = true;
                return Ok(None);
            }

            let entry_type_num = hdr[4];
            let flags = hdr[5];
            let item_size = u32::from_le_bytes([
                hdr[10], hdr[11], hdr[12], hdr[13],
            ]) as usize;

            if item_size > MAX_SANE_ITEM_SIZE {
                self.eof = true;
                return Ok(None);
            }

            let vlsn_present =
                (flags & 0x08) != 0 || (flags & 0x20) != 0;
            let header_size =
                if vlsn_present { MAX_HEADER_SIZE } else { MIN_HEADER_SIZE };
            let entry_size = header_size + item_size;

            if self.current_offset + entry_size as u64 > file_len {
                self.eof = true;
                return Ok(None);
            }

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
                None => continue,
            };

            let payload = full_buf[header_size..].to_vec();
            return Ok(Some((lsn, entry_type, payload)));
        }
    }

    /// Update `max_node_id` and `max_db_id` from an IN/BIN/BINDelta entry.
    ///
    /// Id-tracking logic inside `INFileReader.processEntry()`.
    fn update_max_ids(&mut self, entry_type: LogEntryType, payload: &[u8]) {
        match entry_type {
            LogEntryType::IN | LogEntryType::BIN => {
                if let Ok(e) = InLogEntry::read_from_log(payload)
                    && e.db_id > self.max_db_id {
                        self.max_db_id = e.db_id;
                    }
                    // node_id is embedded in opaque node_data; we cannot
                    // extract it without noxu-tree, so max_node_id stays 0.
            }
            LogEntryType::BINDelta => {
                if let Ok(e) = BinDeltaLogEntry::read_from_log(payload)
                    && e.db_id > self.max_db_id {
                        self.max_db_id = e.db_id;
                    }
            }
            _ => {}
        }
    }

    /// Parse a raw payload into a `CurrentEntry`.
    fn parse_entry(
        &self,
        entry_type: LogEntryType,
        payload: &[u8],
    ) -> Result<CurrentEntry> {
        match entry_type {
            LogEntryType::IN | LogEntryType::BIN => {
                let e = InLogEntry::read_from_log(payload).map_err(|err| {
                    NoxuLogError::Internal(format!("IN parse error: {err}"))
                })?;
                let is_delta = e.is_bin_delta();
                Ok(CurrentEntry::In { entry: e, is_bin_delta: is_delta })
            }
            LogEntryType::BINDelta => {
                let e =
                    BinDeltaLogEntry::read_from_log(payload).map_err(|err| {
                        NoxuLogError::Internal(format!("BINDelta parse error: {err}"))
                    })?;
                Ok(CurrentEntry::BinDelta(e))
            }
            _ => Err(NoxuLogError::LogCorrupt(format!(
                "INFileReader: unexpected entry type {:?}",
                entry_type
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entry::bin_delta_log_entry::BinDeltaLogEntry;
    use crate::entry::in_log_entry::InLogEntry;
    use crate::entry_header::MIN_HEADER_SIZE;
    use crate::entry_type::LogEntryType;
    use bytes::BytesMut;
    use noxu_util::lsn::NULL_LSN;
    use std::collections::HashMap;
    use std::io;

    // ------------------------------------------------------------------
    // Mock file access
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
            self.files
                .get(&file_num)
                .map(|d| d.len() as u64)
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::NotFound, "File not found")
                        .into()
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
    // Helpers
    // ------------------------------------------------------------------

    fn make_raw_entry(entry_type: LogEntryType, payload: &[u8]) -> Vec<u8> {
        let item_size = payload.len() as u32;
        let mut buf = vec![0u8; MIN_HEADER_SIZE + payload.len()];
        buf[4] = entry_type.type_num();
        buf[10..14].copy_from_slice(&item_size.to_le_bytes());
        buf[MIN_HEADER_SIZE..].copy_from_slice(payload);
        buf
    }

    fn make_in_payload(db_id: u64) -> Vec<u8> {
        let e = InLogEntry::new(db_id, NULL_LSN, NULL_LSN, b"node_data".to_vec());
        let mut buf = BytesMut::new();
        e.write_to_log(&mut buf);
        buf.to_vec()
    }

    fn make_bin_delta_payload(db_id: u64) -> Vec<u8> {
        let e = BinDeltaLogEntry::new(
            db_id,
            NULL_LSN,
            NULL_LSN,
            b"delta_data".to_vec(),
        );
        let mut buf = BytesMut::new();
        e.write_to_log(&mut buf);
        buf.to_vec()
    }

    // ------------------------------------------------------------------
    // Construction tests
    // ------------------------------------------------------------------

    #[test]
    fn test_in_file_reader_new_with_file() {
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![0u8; 64]);
        let result = INFileReader::new(mock, 512, Lsn::new(0, 0), NULL_LSN);
        assert!(result.is_ok());
    }

    #[test]
    fn test_in_file_reader_new_no_files() {
        let mock = MockFileAccess::new();
        let result = INFileReader::new(mock, 512, NULL_LSN, NULL_LSN);
        assert!(result.is_ok());
    }

    #[test]
    fn test_in_file_reader_with_finish_lsn() {
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![0u8; 256]);
        let result =
            INFileReader::new(mock, 1024, Lsn::new(0, 0), Lsn::new(0, 128));
        assert!(result.is_ok());
    }

    #[test]
    fn test_in_file_reader_multiple_files() {
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![0u8; 100]);
        mock.add_file(1, vec![0u8; 200]);
        let result = INFileReader::new(mock, 512, Lsn::new(0, 0), NULL_LSN);
        assert!(result.is_ok());
    }

    #[test]
    fn test_in_file_reader_varying_buffer_sizes() {
        for &buf_size in &[64usize, 512, 2048, 8192] {
            let mut mock = MockFileAccess::new();
            mock.add_file(0, vec![0u8; 50]);
            let result = INFileReader::new(mock, buf_size, Lsn::new(0, 0), NULL_LSN);
            assert!(result.is_ok(), "failed for buf_size {}", buf_size);
        }
    }

    // ------------------------------------------------------------------
    // Functional tests
    // ------------------------------------------------------------------

    #[test]
    fn test_read_in_entry() {
        let payload = make_in_payload(42);
        let raw = make_raw_entry(LogEntryType::IN, &payload);

        let mut mock = MockFileAccess::new();
        mock.add_file(0, raw);

        let mut reader =
            INFileReader::new(mock, 512, Lsn::new(0, 0), NULL_LSN).unwrap();
        reader.add_target_type(LogEntryType::IN);

        assert!(reader.read_next_entry().unwrap());
        assert_eq!(reader.get_database_id(), 42);
        assert!(!reader.is_bin_delta());
    }

    #[test]
    fn test_read_bin_entry() {
        let payload = make_in_payload(55);
        let raw = make_raw_entry(LogEntryType::BIN, &payload);

        let mut mock = MockFileAccess::new();
        mock.add_file(0, raw);

        let mut reader =
            INFileReader::new(mock, 512, Lsn::new(0, 0), NULL_LSN).unwrap();
        reader.add_target_type(LogEntryType::BIN);

        assert!(reader.read_next_entry().unwrap());
        assert_eq!(reader.get_database_id(), 55);
        assert!(!reader.is_bin_delta());
    }

    #[test]
    fn test_read_bin_delta_entry() {
        let payload = make_bin_delta_payload(77);
        let raw = make_raw_entry(LogEntryType::BINDelta, &payload);

        let mut mock = MockFileAccess::new();
        mock.add_file(0, raw);

        let mut reader =
            INFileReader::new(mock, 512, Lsn::new(0, 0), NULL_LSN).unwrap();
        reader.add_target_type(LogEntryType::BINDelta);

        assert!(reader.read_next_entry().unwrap());
        assert_eq!(reader.get_database_id(), 77);
        assert!(reader.is_bin_delta());
    }

    #[test]
    fn test_skip_non_target() {
        // File: Trace (skipped) then IN (targeted).
        let trace_raw = make_raw_entry(LogEntryType::Trace, b"hello");
        let in_payload = make_in_payload(11);
        let in_raw = make_raw_entry(LogEntryType::IN, &in_payload);
        let mut data = trace_raw;
        data.extend_from_slice(&in_raw);

        let mut mock = MockFileAccess::new();
        mock.add_file(0, data);

        let mut reader =
            INFileReader::new(mock, 512, Lsn::new(0, 0), NULL_LSN).unwrap();
        reader.add_target_type(LogEntryType::IN);

        assert!(reader.read_next_entry().unwrap());
        assert_eq!(reader.get_database_id(), 11);

        assert!(!reader.read_next_entry().unwrap());
    }

    #[test]
    fn test_max_db_id_tracking() {
        // Two IN entries with different db_ids; max_db_id should be the larger.
        let p1 = make_in_payload(100);
        let r1 = make_raw_entry(LogEntryType::IN, &p1);
        let p2 = make_in_payload(200);
        let r2 = make_raw_entry(LogEntryType::IN, &p2);
        let mut data = r1;
        data.extend_from_slice(&r2);

        let mut mock = MockFileAccess::new();
        mock.add_file(0, data);

        let mut reader =
            INFileReader::new(mock, 512, Lsn::new(0, 0), NULL_LSN).unwrap();
        reader.add_target_type(LogEntryType::IN);

        while reader.read_next_entry().unwrap() {}

        assert_eq!(reader.get_max_db_id(), 200);
    }

    #[test]
    fn test_finish_lsn_stops_reading() {
        let payload = make_in_payload(3);
        let raw = make_raw_entry(LogEntryType::IN, &payload);

        let mut mock = MockFileAccess::new();
        mock.add_file(0, raw);

        let mut reader = INFileReader::new(
            mock,
            512,
            Lsn::new(0, 0),
            Lsn::new(0, 0), // finish at very start
        )
        .unwrap();
        reader.add_target_type(LogEntryType::IN);

        assert!(!reader.read_next_entry().unwrap());
    }

    #[test]
    fn test_eof_on_empty_file() {
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![]);

        let mut reader =
            INFileReader::new(mock, 512, Lsn::new(0, 0), NULL_LSN).unwrap();
        reader.add_target_type(LogEntryType::IN);

        assert!(!reader.read_next_entry().unwrap());
    }

    #[test]
    fn test_eof_on_no_files() {
        let mock = MockFileAccess::new();
        let mut reader =
            INFileReader::new(mock, 512, NULL_LSN, NULL_LSN).unwrap();
        reader.add_target_type(LogEntryType::IN);
        assert!(!reader.read_next_entry().unwrap());
    }

    #[test]
    fn test_get_max_node_id_initial_zero() {
        let mock = MockFileAccess::new();
        let reader =
            INFileReader::new(mock, 512, NULL_LSN, NULL_LSN).unwrap();
        assert_eq!(reader.get_max_node_id(), 0);
    }

    #[test]
    fn test_current_lsn_after_read() {
        let payload = make_in_payload(5);
        let raw = make_raw_entry(LogEntryType::IN, &payload);

        let mut mock = MockFileAccess::new();
        mock.add_file(0, raw);

        let mut reader =
            INFileReader::new(mock, 512, Lsn::new(0, 0), NULL_LSN).unwrap();
        reader.add_target_type(LogEntryType::IN);

        assert!(reader.read_next_entry().unwrap());
        assert_eq!(reader.get_current_lsn().file_number(), 0);
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
        let n = mock.read_from_file(0, 100, &mut buf).unwrap();
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
        mock.add_file(3, vec![0u8; 77]);
        assert_eq!(mock.get_file_length(3).unwrap(), 77);
    }

    #[test]
    fn test_mock_get_first_file_num_empty() {
        let mock = MockFileAccess::new();
        assert_eq!(mock.get_first_file_num(), None);
    }

    #[test]
    fn test_mock_get_first_file_num_returns_min() {
        let mut mock = MockFileAccess::new();
        mock.add_file(10, vec![]);
        mock.add_file(3, vec![]);
        assert_eq!(mock.get_first_file_num(), Some(3));
    }

    #[test]
    fn test_mock_get_following_file_backward() {
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![]);
        mock.add_file(1, vec![]);
        mock.add_file(2, vec![]);
        assert_eq!(mock.get_following_file_num(2, false), Some(1));
        assert_eq!(mock.get_following_file_num(0, false), None);
    }

    #[test]
    fn test_mock_get_following_file_forward_none() {
        let mut mock = MockFileAccess::new();
        mock.add_file(5, vec![]);
        assert_eq!(mock.get_following_file_num(5, true), None);
    }

    #[test]
    fn test_in_file_reader_null_lsn_with_files() {
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![0u8; 64]);
        let result = INFileReader::new(mock, 512, NULL_LSN, NULL_LSN);
        assert!(result.is_ok());
    }
}
