//! Cleaner file reader for log file garbage collection.
//!
//! Port of `com.sleepycat.je.log.CleanerFileReader`.
//!
//! **STUB**: This reader depends on cleaner types from noxu-cleaner and tree
//! types from noxu-tree, which are not yet implemented. This file provides a
//! minimal stub to allow noxu-log to compile.

use crate::error::Result;
use crate::file_reader::{FileReader, LogFileAccess};
use noxu_util::lsn::Lsn;

/// Scans log files to count utilization and support cleaning.
///
/// TODO: Complete implementation once noxu-cleaner types are available.
/// This reader needs to:
/// - Track file utilization (FileSummary)
/// - Track IN utilization (INSummary)
/// - Track expiration info (ExpirationTracker)
/// - Count total, obsolete, IN, and LN sizes
/// - Track first and last VLSN in files
/// - Handle all node types (IN, BIN, LN, BIN-delta, etc.)
pub struct CleanerFileReader<F: LogFileAccess> {
    /// The underlying file reader
    _reader: FileReader<F>,
}

impl<F: LogFileAccess> CleanerFileReader<F> {
    /// Create a CleanerFileReader (stub).
    ///
    /// TODO: Add proper constructor parameters:
    /// - file_num: u32 (single file to scan)
    /// - file_summary: &mut FileSummary (returns true utilization)
    /// - in_summary: &mut INSummary (returns IN utilization)
    /// - exp_tracker: Option<&mut ExpirationTracker> (returns expiration info)
    pub fn new(
        file_access: F,
        _read_buffer_size: usize,
        start_lsn: Lsn,
        _file_num: u32,
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

        Ok(CleanerFileReader { _reader: reader })
    }

    // TODO: Implement these methods once cleaner types are available:
    // - read_next_entry() -> Result<bool>
    // - get_file_summary() -> &FileSummary
    // - get_in_summary() -> &INSummary
    // - get_first_vlsn() -> Option<VLSN>
    // - get_last_vlsn() -> VLSN
    // - count_obsolete(...) (complex signature)
    // - is_ln() -> bool
    // - is_in() -> bool
    // - is_bin_delta() -> bool
    // - get_database_id() -> DatabaseId
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
                let bytes_to_copy = end - start;
                buf[..bytes_to_copy].copy_from_slice(&data[start..end]);
                Ok(bytes_to_copy)
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
    fn test_cleaner_file_reader_new_with_file() {
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![0u8; 64]);
        let start_lsn = Lsn::new(0, 0);
        let result = CleanerFileReader::new(mock, 512, start_lsn, 0);
        assert!(result.is_ok());
    }

    #[test]
    fn test_cleaner_file_reader_new_empty_store() {
        let mock = MockFileAccess::new();
        let result = CleanerFileReader::new(mock, 512, NULL_LSN, 0);
        // Forward reader with NULL_LSN and no files sets eof=true; still Ok.
        assert!(result.is_ok());
    }

    #[test]
    fn test_cleaner_file_reader_specific_file_num() {
        let mut mock = MockFileAccess::new();
        mock.add_file(5, vec![1u8; 128]);
        let start_lsn = Lsn::new(5, 0);
        let result = CleanerFileReader::new(mock, 1024, start_lsn, 5);
        assert!(result.is_ok());
    }

    #[test]
    fn test_cleaner_file_reader_different_buffer_sizes() {
        for &buf_size in &[64usize, 128, 256, 1024, 4096] {
            let mut mock = MockFileAccess::new();
            mock.add_file(0, vec![0u8; 32]);
            let start_lsn = Lsn::new(0, 0);
            let result =
                CleanerFileReader::new(mock, buf_size, start_lsn, 0);
            assert!(result.is_ok(), "failed for buf_size {}", buf_size);
        }
    }

    #[test]
    fn test_cleaner_file_reader_with_multiple_files() {
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![0u8; 100]);
        mock.add_file(1, vec![0u8; 200]);
        mock.add_file(2, vec![0u8; 50]);
        let start_lsn = Lsn::new(1, 0);
        let result = CleanerFileReader::new(mock, 512, start_lsn, 1);
        assert!(result.is_ok());
    }

    // --- Additional branch-coverage tests for MockFileAccess ---

    #[test]
    fn test_mock_read_from_file_file_not_found() {
        let mock = MockFileAccess::new();
        let mut buf = [0u8; 4];
        let result = mock.read_from_file(99, 0, &mut buf);
        assert!(result.is_err());
    }

    #[test]
    fn test_mock_read_from_file_offset_at_end() {
        // start >= data.len() branch: returns Ok(0)
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
        let result = mock.get_file_length(42);
        assert!(result.is_err());
    }

    #[test]
    fn test_mock_get_file_length_ok() {
        let mut mock = MockFileAccess::new();
        mock.add_file(7, vec![0u8; 50]);
        let len = mock.get_file_length(7).unwrap();
        assert_eq!(len, 50);
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
        // No file > 0
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
        // NULL_LSN with files: uses get_first_file_num() → Some(0)
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![0u8; 32]);
        let result = CleanerFileReader::new(mock, 512, NULL_LSN, 0);
        assert!(result.is_ok());
    }
}
