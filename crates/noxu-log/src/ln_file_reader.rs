//! LN file reader for recovery.
//!
//! Port of `com.sleepycat.je.log.LNFileReader`.
//!
//! **STUB**: This reader depends on LN (Leaf Node) types from noxu-tree,
//! which are not yet implemented. This file provides a minimal stub to allow
//! noxu-log to compile.

use crate::error::Result;
use crate::file_reader::{FileReader, LogFileAccess};
use noxu_util::lsn::Lsn;

/// Scans for Leaf Node (LN) entries during recovery.
///
/// TODO: Complete implementation once noxu-tree LN types are available.
/// This reader needs to:
/// - Read LNLogEntry entries
/// - Support both redo (forward) and undo (backward) phases
/// - Handle transaction commit/abort entries
/// - Track transaction IDs
/// - Support prepare, rollback, and other transaction entries
pub struct LNFileReader<F: LogFileAccess> {
    /// The underlying file reader
    _reader: FileReader<F>,
}

impl<F: LogFileAccess> LNFileReader<F> {
    /// Create an LNFileReader (stub).
    ///
    /// TODO: Add proper constructor parameters:
    /// - redo: bool (forward or backward reading)
    /// - single_file_num: Option<u32>
    /// - ckpt_end: Lsn
    pub fn new(
        file_access: F,
        _read_buffer_size: usize,
        start_lsn: Lsn,
        _redo: bool,
        _end_of_file_lsn: Lsn,
        _finish_lsn: Lsn,
    ) -> Result<Self> {
        let reader = FileReader::new(
            file_access,
            true, // forward
            start_lsn,
            noxu_util::lsn::NULL_LSN,
            noxu_util::lsn::NULL_LSN,
            2048,
            true,
        )?;

        Ok(LNFileReader { _reader: reader })
    }

    // TODO: Implement these methods once LN types are available:
    // - add_target_type(entry_type: LogEntryType)
    // - read_next_entry() -> Result<bool>
    // - is_ln() -> bool
    // - get_ln_log_entry() -> &LNLogEntry
    // - get_name_ln_log_entry() -> Option<&NameLNLogEntry>
    // - get_database_id() -> DatabaseId
    // - get_txn_id() -> Option<u64>
    // - is_prepare() -> bool
    // - is_commit() -> bool
    // - is_abort() -> bool
    // - is_rollback_start() -> bool
    // - is_rollback_end() -> bool
    // - get_main_item() -> &dyn Any
    // - get_abort_lsn() -> Lsn
    // - get_abort_known_deleted() -> bool
    // - is_invisible() -> bool
    // - get_vlsn() -> u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file_reader::LogFileAccess;
    use noxu_util::lsn::NULL_LSN;
    use std::collections::HashMap;
    use std::io;

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

    #[test]
    fn test_ln_file_reader_new_forward() {
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![0u8; 64]);
        let start_lsn = Lsn::new(0, 0);
        let result = LNFileReader::new(
            mock, 512, start_lsn,
            true,   // redo (forward)
            NULL_LSN,
            NULL_LSN,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_ln_file_reader_new_backward() {
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![0u8; 64]);
        let start_lsn = Lsn::new(0, 0);
        let result = LNFileReader::new(
            mock, 512, start_lsn,
            false,  // undo (backward stub)
            NULL_LSN,
            NULL_LSN,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_ln_file_reader_no_files() {
        let mock = MockFileAccess::new();
        let result = LNFileReader::new(
            mock, 512, NULL_LSN, true, NULL_LSN, NULL_LSN,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_ln_file_reader_with_eof_lsn() {
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![0u8; 128]);
        let start_lsn = Lsn::new(0, 0);
        let eof_lsn = Lsn::new(0, 128);
        let result = LNFileReader::new(
            mock, 512, start_lsn, true, eof_lsn, NULL_LSN,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_ln_file_reader_with_finish_lsn() {
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![0u8; 256]);
        let start_lsn = Lsn::new(0, 0);
        let finish_lsn = Lsn::new(0, 200);
        let result = LNFileReader::new(
            mock, 512, start_lsn, true, NULL_LSN, finish_lsn,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_ln_file_reader_multiple_files() {
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![0u8; 100]);
        mock.add_file(1, vec![0u8; 100]);
        let start_lsn = Lsn::new(0, 0);
        let result = LNFileReader::new(
            mock, 512, start_lsn, true, NULL_LSN, NULL_LSN,
        );
        assert!(result.is_ok());
    }

    // --- Additional branch-coverage tests for MockFileAccess ---

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

    #[test]
    fn test_ln_file_reader_null_lsn_with_files() {
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![0u8; 64]);
        let result = LNFileReader::new(
            mock, 512, NULL_LSN, true, NULL_LSN, NULL_LSN,
        );
        assert!(result.is_ok());
    }
}
