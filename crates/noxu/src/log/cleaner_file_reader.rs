//! Cleaner file reader for log file garbage collection.
//!
//!
//! Scans a single log file and classifies each entry into one of the
//! following categories (mirroring the Java constants):
//!
//! | Category    | Value |
//! |-------------|-------|
//! | LN          | 0     |
//! | IN          | 1     |
//! | BIN delta   | 2     |
//! | DbTree      | 3     |
//! | File header | 4     |
//!
//! The reader is used by the cleaner to count true utilization and to decide
//! whether each entry is live or obsolete.

use crate::log::entry::bin_delta_log_entry::BinDeltaLogEntry;
use crate::log::entry::in_log_entry::InLogEntry;
use crate::log::entry::ln_log_entry::LnLogEntry;
use crate::log::entry_header::{MAX_HEADER_SIZE, MIN_HEADER_SIZE};
use crate::log::entry_type::LogEntryType;
use crate::log::error::Result;
use crate::log::file_reader::LogFileAccess;
use crate::util::lsn::{Lsn, NULL_LSN};

// Maximum plausible payload size (64 MiB).
const MAX_SANE_ITEM_SIZE: usize = 64 * 1024 * 1024;

/// Category constants — mirror the Java static `byte` constants.
const CAT_LN: u8 = 0;
const CAT_IN: u8 = 1;
const CAT_BIN_DELTA: u8 = 2;
const CAT_DBTREE: u8 = 3;
const CAT_FILE_HEADER: u8 = 4;

/// Parsed current entry.
enum CurrentEntry {
    Ln(LnLogEntry),
    In(InLogEntry),
    BinDelta(BinDeltaLogEntry),
    Other,
}

/// Scans a log file and classifies entries for the cleaner.
///
///
///
/// Unlike recovery-oriented readers, this reader processes **every** entry
/// (not just a target set) so that the full file utilization can be measured.
/// Callers use the `is_ln()`, `is_in()`, `is_bin_delta()` predicates to
/// distinguish categories, and `get_database_id()` to route entries to the
/// correct per-database accounting.
pub struct CleanerFileReader<F: LogFileAccess> {
    /// File access interface.
    file_access: F,
    /// The single file number this reader is scanning.
    file_num: u32,
    /// Current byte offset within the file.
    current_offset: u64,
    /// LSN of the most-recently processed entry.
    current_lsn: Lsn,
    /// Category of the current entry (CAT_* constant).
    current_category: u8,
    /// Parsed current entry.
    current_entry: Option<CurrentEntry>,
    /// Whether we have reached the end of the file.
    eof: bool,
}

impl<F: LogFileAccess> CleanerFileReader<F> {
    /// Create a CleanerFileReader for a single log file.
    ///
    /// # Arguments
    /// * `file_access`      – file I/O provider
    /// * `_read_buffer_size`– ignored (kept for API compatibility)
    /// * `start_lsn`        – where to begin scanning (use `NULL_LSN` for file
    ///   start; `file_num` takes precedence for the file)
    /// * `file_num`         – the log file number to scan
    pub fn new(
        file_access: F,
        _read_buffer_size: usize,
        start_lsn: Lsn,
        file_num: u32,
    ) -> Result<Self> {
        // Start offset: honour start_lsn only when it refers to this file.
        let start_offset =
            if !start_lsn.is_null() && start_lsn.file_number() == file_num {
                start_lsn.file_offset() as u64
            } else {
                0u64
            };

        // If the file does not exist, mark EOF immediately rather than
        // returning an error — the reader is simply empty.
        let eof = file_access.get_file_length(file_num).is_err();

        Ok(CleanerFileReader {
            file_access,
            file_num,
            current_offset: start_offset,
            current_lsn: NULL_LSN,
            current_category: CAT_LN,
            current_entry: None,
            eof,
        })
    }

    /// Advance to the next entry in the file.
    ///
    /// Returns `Ok(true)` when an entry was read; `Ok(false)` at end of file.
    ///
    /// every entry is "processed"
    /// (there is no `isTargetEntry` filter here).
    pub fn read_next_entry(&mut self) -> Result<bool> {
        if self.eof {
            return Ok(false);
        }

        loop {
            let file_len = match self.file_access.get_file_length(self.file_num)
            {
                Ok(l) => l,
                Err(_) => {
                    self.eof = true;
                    return Ok(false);
                }
            };

            if self.current_offset >= file_len {
                self.eof = true;
                return Ok(false);
            }

            // Read the minimum header.
            let mut hdr = [0u8; MIN_HEADER_SIZE];
            let n = self.file_access.read_from_file(
                self.file_num,
                self.current_offset,
                &mut hdr,
            )?;
            if n < MIN_HEADER_SIZE {
                self.eof = true;
                return Ok(false);
            }

            // Zero type byte → past last written entry.
            if hdr[4] == 0 {
                self.eof = true;
                return Ok(false);
            }

            let entry_type_num = hdr[4];
            let flags = hdr[5];
            let item_size =
                u32::from_le_bytes([hdr[10], hdr[11], hdr[12], hdr[13]])
                    as usize;

            if item_size > MAX_SANE_ITEM_SIZE {
                self.eof = true;
                return Ok(false);
            }

            let vlsn_present = (flags & 0x08) != 0 || (flags & 0x20) != 0;
            let header_size =
                if vlsn_present { MAX_HEADER_SIZE } else { MIN_HEADER_SIZE };
            let entry_size = header_size + item_size;

            if self.current_offset + entry_size as u64 > file_len {
                self.eof = true;
                return Ok(false);
            }

            let mut full_buf = vec![0u8; entry_size];
            let n = self.file_access.read_from_file(
                self.file_num,
                self.current_offset,
                &mut full_buf,
            )?;
            if n < entry_size {
                self.eof = true;
                return Ok(false);
            }

            let lsn = Lsn::new(self.file_num, self.current_offset as u32);
            self.current_offset += entry_size as u64;
            self.current_lsn = lsn;

            let payload = &full_buf[header_size..];

            // Classify and (optionally) parse.
            let entry_type = LogEntryType::from_type_num(entry_type_num);

            let (category, parsed) = match entry_type {
                Some(LogEntryType::FileHeader) => {
                    (CAT_FILE_HEADER, CurrentEntry::Other)
                }
                Some(LogEntryType::DbTree) => (CAT_DBTREE, CurrentEntry::Other),
                Some(LogEntryType::BINDelta) => {
                    let e = BinDeltaLogEntry::read_from_log(payload)
                        .unwrap_or_else(|_| {
                            BinDeltaLogEntry::new(0, NULL_LSN, NULL_LSN, vec![])
                        });
                    (CAT_BIN_DELTA, CurrentEntry::BinDelta(e))
                }
                Some(t) if t.is_in_type() => {
                    // IN or BIN (non-delta).
                    let e = InLogEntry::read_from_log(payload).unwrap_or_else(
                        |_| InLogEntry::new(0, NULL_LSN, NULL_LSN, vec![]),
                    );
                    (CAT_IN, CurrentEntry::In(e))
                }
                Some(t) if is_ln_type(t) => {
                    let is_txn = t.is_transactional();
                    let e = LnLogEntry::read_from_log(payload, is_txn)
                        .unwrap_or_else(|_| {
                            LnLogEntry::new(
                                0,
                                None,
                                NULL_LSN,
                                false,
                                None,
                                None,
                                crate::util::vlsn::NULL_VLSN,
                                0,
                                false,
                                vec![],
                                None,
                                0,
                                crate::util::vlsn::NULL_VLSN,
                            )
                        });
                    (CAT_LN, CurrentEntry::Ln(e))
                }
                _ => {
                    // Unknown or uninteresting type — skip.
                    continue;
                }
            };

            self.current_category = category;
            self.current_entry = Some(parsed);
            return Ok(true);
        }
    }

    // ------------------------------------------------------------------
    // Accessor methods
    // ------------------------------------------------------------------

    /// Returns `true` if the current entry is an LN (leaf node).
    pub fn is_ln(&self) -> bool {
        self.current_category == CAT_LN
    }

    /// Returns `true` if the current entry is an IN/BIN (non-delta).
    pub fn is_in(&self) -> bool {
        self.current_category == CAT_IN
    }

    /// Returns `true` if the current entry is a BIN delta.
    pub fn is_bin_delta(&self) -> bool {
        self.current_category == CAT_BIN_DELTA
    }

    /// Returns `true` if the current entry is the file header.
    pub fn is_file_header(&self) -> bool {
        self.current_category == CAT_FILE_HEADER
    }

    /// Returns `true` if the current entry is a DbTree entry.
    pub fn is_db_tree(&self) -> bool {
        self.current_category == CAT_DBTREE
    }

    /// Returns the database ID from the current entry, or `0` if not
    /// applicable (e.g. file header).
    pub fn get_database_id(&self) -> u64 {
        match &self.current_entry {
            Some(CurrentEntry::Ln(e)) => e.db_id,
            Some(CurrentEntry::In(e)) => e.db_id,
            Some(CurrentEntry::BinDelta(e)) => e.db_id,
            _ => 0,
        }
    }

    /// Returns the node ID from the current IN/BIN entry.
    ///
    /// Always `0` in the current implementation because the node ID is
    /// embedded in the opaque `node_data` blob and cannot be extracted
    /// without noxu-tree integration.
    pub fn get_node_id(&self) -> u64 {
        0
    }

    /// Returns `true` if the current LN is embedded in the parent BIN.
    pub fn is_embedded_ln(&self) -> bool {
        match &self.current_entry {
            Some(CurrentEntry::Ln(e)) => e.embedded_ln,
            _ => false,
        }
    }

    /// Returns the LSN of the most-recently processed entry.
    pub fn get_current_lsn(&self) -> Lsn {
        self.current_lsn
    }
}

/// Returns `true` if the given `LogEntryType` is an LN-family type.
///
/// This mirrors `LogEntryType.isLNType()`.
fn is_ln_type(t: LogEntryType) -> bool {
    matches!(
        t,
        LogEntryType::InsertLN
            | LogEntryType::UpdateLN
            | LogEntryType::DeleteLN
            | LogEntryType::InsertLNTxn
            | LogEntryType::UpdateLNTxn
            | LogEntryType::DeleteLNTxn
            | LogEntryType::MapLN
            | LogEntryType::NameLN
            | LogEntryType::NameLNTxn
            | LogEntryType::FileSummaryLN
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log::entry::bin_delta_log_entry::BinDeltaLogEntry;
    use crate::log::entry::in_log_entry::InLogEntry;
    use crate::log::entry::ln_log_entry::LnLogEntry;
    use crate::log::entry_header::MIN_HEADER_SIZE;
    use crate::log::entry_type::LogEntryType;
    use bytes::BytesMut;
    use crate::util::lsn::NULL_LSN;
    use crate::util::vlsn::NULL_VLSN;
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
                let bytes_to_copy = end - start;
                buf[..bytes_to_copy].copy_from_slice(&data[start..end]);
                Ok(bytes_to_copy)
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

    fn make_ln_payload(db_id: u64) -> Vec<u8> {
        let e = LnLogEntry::new(
            db_id,
            None,
            NULL_LSN,
            false,
            None,
            None,
            NULL_VLSN,
            0,
            false,
            b"key".to_vec(),
            Some(b"val".to_vec()),
            0,
            NULL_VLSN,
        );
        let mut buf = BytesMut::new();
        e.write_to_log(&mut buf);
        buf.to_vec()
    }

    fn make_in_payload(db_id: u64) -> Vec<u8> {
        let e = InLogEntry::new(db_id, NULL_LSN, NULL_LSN, b"nd".to_vec());
        let mut buf = BytesMut::new();
        e.write_to_log(&mut buf);
        buf.to_vec()
    }

    fn make_bin_delta_payload(db_id: u64) -> Vec<u8> {
        let e =
            BinDeltaLogEntry::new(db_id, NULL_LSN, NULL_LSN, b"dd".to_vec());
        let mut buf = BytesMut::new();
        e.write_to_log(&mut buf);
        buf.to_vec()
    }

    // ------------------------------------------------------------------
    // Construction tests
    // ------------------------------------------------------------------

    #[test]
    fn test_cleaner_file_reader_new_with_file() {
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![0u8; 64]);
        let result = CleanerFileReader::new(mock, 512, Lsn::new(0, 0), 0);
        assert!(result.is_ok());
    }

    #[test]
    fn test_cleaner_file_reader_new_empty_store() {
        let mock = MockFileAccess::new();
        let result = CleanerFileReader::new(mock, 512, NULL_LSN, 0);
        assert!(result.is_ok());
    }

    #[test]
    fn test_cleaner_file_reader_specific_file_num() {
        let mut mock = MockFileAccess::new();
        mock.add_file(5, vec![1u8; 128]);
        let result = CleanerFileReader::new(mock, 1024, Lsn::new(5, 0), 5);
        assert!(result.is_ok());
    }

    #[test]
    fn test_cleaner_file_reader_different_buffer_sizes() {
        for &buf_size in &[64usize, 128, 256, 1024, 4096] {
            let mut mock = MockFileAccess::new();
            mock.add_file(0, vec![0u8; 32]);
            let result =
                CleanerFileReader::new(mock, buf_size, Lsn::new(0, 0), 0);
            assert!(result.is_ok(), "failed for buf_size {}", buf_size);
        }
    }

    #[test]
    fn test_cleaner_file_reader_with_multiple_files() {
        // Reader only scans file_num=1; other files are irrelevant.
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![0u8; 100]);
        mock.add_file(1, vec![0u8; 200]);
        mock.add_file(2, vec![0u8; 50]);
        let result = CleanerFileReader::new(mock, 512, Lsn::new(1, 0), 1);
        assert!(result.is_ok());
    }

    // ------------------------------------------------------------------
    // Functional tests
    // ------------------------------------------------------------------

    #[test]
    fn test_read_ln_entry() {
        let payload = make_ln_payload(10);
        let raw = make_raw_entry(LogEntryType::InsertLN, &payload);
        let mut mock = MockFileAccess::new();
        mock.add_file(0, raw);

        let mut reader =
            CleanerFileReader::new(mock, 512, Lsn::new(0, 0), 0).unwrap();

        assert!(reader.read_next_entry().unwrap());
        assert!(reader.is_ln());
        assert!(!reader.is_in());
        assert!(!reader.is_bin_delta());
        assert_eq!(reader.get_database_id(), 10);
    }

    #[test]
    fn test_read_in_entry() {
        let payload = make_in_payload(20);
        let raw = make_raw_entry(LogEntryType::IN, &payload);
        let mut mock = MockFileAccess::new();
        mock.add_file(0, raw);

        let mut reader =
            CleanerFileReader::new(mock, 512, Lsn::new(0, 0), 0).unwrap();

        assert!(reader.read_next_entry().unwrap());
        assert!(reader.is_in());
        assert!(!reader.is_ln());
        assert_eq!(reader.get_database_id(), 20);
        assert_eq!(reader.get_node_id(), 0);
    }

    #[test]
    fn test_read_bin_delta_entry() {
        let payload = make_bin_delta_payload(30);
        let raw = make_raw_entry(LogEntryType::BINDelta, &payload);
        let mut mock = MockFileAccess::new();
        mock.add_file(0, raw);

        let mut reader =
            CleanerFileReader::new(mock, 512, Lsn::new(0, 0), 0).unwrap();

        assert!(reader.read_next_entry().unwrap());
        assert!(reader.is_bin_delta());
        assert_eq!(reader.get_database_id(), 30);
    }

    #[test]
    fn test_eof_on_empty_file() {
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![]);

        let mut reader =
            CleanerFileReader::new(mock, 512, Lsn::new(0, 0), 0).unwrap();
        assert!(!reader.read_next_entry().unwrap());
    }

    #[test]
    fn test_eof_on_missing_file() {
        let mock = MockFileAccess::new();
        let mut reader =
            CleanerFileReader::new(mock, 512, NULL_LSN, 0).unwrap();
        assert!(!reader.read_next_entry().unwrap());
    }

    #[test]
    fn test_multiple_entry_types() {
        let p_ln = make_ln_payload(1);
        let r_ln = make_raw_entry(LogEntryType::InsertLN, &p_ln);
        let p_in = make_in_payload(2);
        let r_in = make_raw_entry(LogEntryType::BIN, &p_in);
        let p_bd = make_bin_delta_payload(3);
        let r_bd = make_raw_entry(LogEntryType::BINDelta, &p_bd);

        let mut data = r_ln;
        data.extend_from_slice(&r_in);
        data.extend_from_slice(&r_bd);

        let mut mock = MockFileAccess::new();
        mock.add_file(0, data);

        let mut reader =
            CleanerFileReader::new(mock, 512, Lsn::new(0, 0), 0).unwrap();

        assert!(reader.read_next_entry().unwrap());
        assert!(reader.is_ln());
        assert_eq!(reader.get_database_id(), 1);

        assert!(reader.read_next_entry().unwrap());
        assert!(reader.is_in());
        assert_eq!(reader.get_database_id(), 2);

        assert!(reader.read_next_entry().unwrap());
        assert!(reader.is_bin_delta());
        assert_eq!(reader.get_database_id(), 3);

        assert!(!reader.read_next_entry().unwrap());
    }

    #[test]
    fn test_current_lsn_advances() {
        let p1 = make_ln_payload(5);
        let r1 = make_raw_entry(LogEntryType::InsertLN, &p1);
        let p2 = make_ln_payload(6);
        let r2 = make_raw_entry(LogEntryType::InsertLN, &p2);
        let mut data = r1;
        data.extend_from_slice(&r2);

        let mut mock = MockFileAccess::new();
        mock.add_file(0, data);

        let mut reader =
            CleanerFileReader::new(mock, 512, Lsn::new(0, 0), 0).unwrap();

        assert!(reader.read_next_entry().unwrap());
        let lsn1 = reader.get_current_lsn();

        assert!(reader.read_next_entry().unwrap());
        let lsn2 = reader.get_current_lsn();

        assert!(lsn2 > lsn1, "LSN should advance between entries");
    }

    #[test]
    fn test_is_embedded_ln_false_by_default() {
        let payload = make_ln_payload(9);
        let raw = make_raw_entry(LogEntryType::InsertLN, &payload);
        let mut mock = MockFileAccess::new();
        mock.add_file(0, raw);

        let mut reader =
            CleanerFileReader::new(mock, 512, Lsn::new(0, 0), 0).unwrap();
        assert!(reader.read_next_entry().unwrap());
        assert!(!reader.is_embedded_ln());
    }

    // ------------------------------------------------------------------
    // Branch-coverage for MockFileAccess
    // ------------------------------------------------------------------

    #[test]
    fn test_mock_read_from_file_file_not_found() {
        let mock = MockFileAccess::new();
        let mut buf = [0u8; 4];
        let result = mock.read_from_file(99, 0, &mut buf);
        assert!(result.is_err());
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
        let mut buf = [0u8; 4];
        let n = mock.read_from_file(0, 100, &mut buf).unwrap();
        assert_eq!(n, 0);
    }

    #[test]
    fn test_mock_get_file_length_not_found() {
        let mock = MockFileAccess::new();
        assert!(mock.get_file_length(42).is_err());
    }

    #[test]
    fn test_mock_get_file_length_ok() {
        let mut mock = MockFileAccess::new();
        mock.add_file(7, vec![0u8; 50]);
        assert_eq!(mock.get_file_length(7).unwrap(), 50);
    }

    #[test]
    fn test_mock_get_first_file_num_empty() {
        let mock = MockFileAccess::new();
        assert_eq!(mock.get_first_file_num(), None);
    }

    #[test]
    fn test_mock_get_first_file_num_returns_min() {
        let mut mock = MockFileAccess::new();
        mock.add_file(5, vec![]);
        mock.add_file(2, vec![]);
        mock.add_file(8, vec![]);
        assert_eq!(mock.get_first_file_num(), Some(2));
    }

    #[test]
    fn test_mock_get_following_file_num_forward_none() {
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![]);
        assert_eq!(mock.get_following_file_num(0, true), None);
    }

    #[test]
    fn test_mock_get_following_file_num_backward() {
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![]);
        mock.add_file(1, vec![]);
        mock.add_file(2, vec![]);
        assert_eq!(mock.get_following_file_num(2, false), Some(1));
        assert_eq!(mock.get_following_file_num(1, false), Some(0));
        assert_eq!(mock.get_following_file_num(0, false), None);
    }

    #[test]
    fn test_mock_get_following_file_num_forward() {
        let mut mock = MockFileAccess::new();
        mock.add_file(3, vec![]);
        mock.add_file(7, vec![]);
        assert_eq!(mock.get_following_file_num(3, true), Some(7));
        assert_eq!(mock.get_following_file_num(7, true), None);
    }

    #[test]
    fn test_cleaner_file_reader_null_lsn_with_files() {
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![0u8; 32]);
        let result = CleanerFileReader::new(mock, 512, NULL_LSN, 0);
        assert!(result.is_ok());
    }
}
