//! IN file reader for recovery.
//!
//! Port of `com.sleepycat.je.log.INFileReader`.
//!
//! **STUB**: This reader depends on IN (Internal Node) types from noxu-tree,
//! which are not yet implemented. This file provides a minimal stub to allow
//! noxu-log to compile.

use crate::error::Result;
use crate::file_reader::{FileReader, LogFileAccess};
use noxu_util::lsn::Lsn;

/// Scans for Internal Node entries during recovery.
///
/// TODO: Complete implementation once noxu-tree IN types are available.
/// This reader needs to:
/// - Track node IDs, database IDs, and transaction IDs
/// - Count utilization for recovery
/// - Handle INLogEntry, BINDelta, and related types
/// - Support VLSN tracking for replication
pub struct INFileReader<F: LogFileAccess> {
    /// The underlying file reader
    _reader: FileReader<F>,
}

impl<F: LogFileAccess> INFileReader<F> {
    /// Create an INFileReader (stub).
    ///
    /// TODO: Add proper constructor parameters once requirements are clear:
    /// - track_ids: bool
    /// - partial_ckpt_start: Lsn
    /// - ckpt_end: Lsn
    /// - utilization_tracker: RecoveryUtilizationTracker
    pub fn new(
        file_access: F,
        _read_buffer_size: usize,
        start_lsn: Lsn,
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

        Ok(INFileReader { _reader: reader })
    }

    // TODO: Implement these methods once IN types are available:
    // - add_target_type(entry_type: LogEntryType)
    // - read_next_entry() -> Result<bool>
    // - get_in(db_impl: &DatabaseImpl) -> &IN
    // - get_database_id() -> DatabaseId
    // - get_max_node_id() -> u64
    // - get_max_db_id() -> u64
    // - get_max_txn_id() -> u64
    // - is_delete_info() -> bool
    // - is_bin_delta() -> bool
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
    fn test_in_file_reader_new_with_file() {
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![0u8; 64]);
        let start_lsn = Lsn::new(0, 0);
        let result =
            INFileReader::new(mock, 512, start_lsn, NULL_LSN);
        assert!(result.is_ok());
    }

    #[test]
    fn test_in_file_reader_new_no_files() {
        let mock = MockFileAccess::new();
        let result =
            INFileReader::new(mock, 512, NULL_LSN, NULL_LSN);
        assert!(result.is_ok());
    }

    #[test]
    fn test_in_file_reader_with_finish_lsn() {
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![0u8; 256]);
        let start_lsn = Lsn::new(0, 0);
        let finish_lsn = Lsn::new(0, 128);
        let result =
            INFileReader::new(mock, 1024, start_lsn, finish_lsn);
        assert!(result.is_ok());
    }

    #[test]
    fn test_in_file_reader_multiple_files() {
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![0u8; 100]);
        mock.add_file(1, vec![0u8; 200]);
        let start_lsn = Lsn::new(0, 0);
        let result =
            INFileReader::new(mock, 512, start_lsn, NULL_LSN);
        assert!(result.is_ok());
    }

    #[test]
    fn test_in_file_reader_varying_buffer_sizes() {
        for &buf_size in &[64usize, 512, 2048, 8192] {
            let mut mock = MockFileAccess::new();
            mock.add_file(0, vec![0u8; 50]);
            let start_lsn = Lsn::new(0, 0);
            let result =
                INFileReader::new(mock, buf_size, start_lsn, NULL_LSN);
            assert!(result.is_ok(), "failed for buf_size {}", buf_size);
        }
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
