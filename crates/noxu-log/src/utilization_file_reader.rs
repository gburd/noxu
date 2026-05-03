//! Utilization file reader for tracking log file space usage.
//!
//! Port of `com.sleepycat.je.log.UtilizationFileReader`.
//!
//! **STUB**: This reader depends on cleaner utilization types from noxu-cleaner,
//! which are not yet implemented. This file provides a minimal stub to allow
//! noxu-log to compile.

use crate::error::Result;
use crate::file_reader::{FileReader, LogFileAccess};
use noxu_util::lsn::Lsn;

/// Scans log files to track utilization information.
///
/// TODO: Complete implementation once noxu-cleaner types are available.
/// This is similar to CleanerFileReader but focuses specifically on
/// utilization tracking. May be merged with CleanerFileReader or kept separate
/// depending on the cleaner architecture.
pub struct UtilizationFileReader<F: LogFileAccess> {
    /// The underlying file reader
    _reader: FileReader<F>,
}

impl<F: LogFileAccess> UtilizationFileReader<F> {
    /// Create a UtilizationFileReader (stub).
    ///
    /// TODO: Determine exact requirements once cleaner design is finalized.
    pub fn new(
        file_access: F,
        _read_buffer_size: usize,
        start_lsn: Lsn,
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

        Ok(UtilizationFileReader { _reader: reader })
    }

    // TODO: Implement methods once utilization tracking types are available.
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
    fn test_utilization_file_reader_new_with_file() {
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![0u8; 64]);
        let start_lsn = Lsn::new(0, 0);
        let result = UtilizationFileReader::new(mock, 512, start_lsn);
        assert!(result.is_ok());
    }

    #[test]
    fn test_utilization_file_reader_new_empty_store() {
        let mock = MockFileAccess::new();
        let result = UtilizationFileReader::new(mock, 512, NULL_LSN);
        assert!(result.is_ok());
    }

    #[test]
    fn test_utilization_file_reader_multiple_files() {
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![1u8; 128]);
        mock.add_file(1, vec![2u8; 256]);
        let start_lsn = Lsn::new(0, 0);
        let result = UtilizationFileReader::new(mock, 1024, start_lsn);
        assert!(result.is_ok());
    }

    #[test]
    fn test_utilization_file_reader_large_buffer() {
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![0u8; 32]);
        let start_lsn = Lsn::new(0, 0);
        let result = UtilizationFileReader::new(mock, 65536, start_lsn);
        assert!(result.is_ok());
    }

    #[test]
    fn test_utilization_file_reader_nonzero_start_offset() {
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![0u8; 200]);
        let start_lsn = Lsn::new(0, 50);
        let result = UtilizationFileReader::new(mock, 512, start_lsn);
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
        let n = mock.read_from_file(0, 500, &mut buf).unwrap();
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
        mock.add_file(1, vec![0u8; 88]);
        assert_eq!(mock.get_file_length(1).unwrap(), 88);
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
        mock.add_file(1, vec![]);
        mock.add_file(3, vec![]);
        assert_eq!(mock.get_first_file_num(), Some(1));
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
        mock.add_file(10, vec![]);
        assert_eq!(mock.get_following_file_num(10, true), None);
    }

    #[test]
    fn test_utilization_file_reader_null_lsn_with_files() {
        // NULL_LSN + files: init uses get_first_file_num() → Some path
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![0u8; 64]);
        let result = UtilizationFileReader::new(mock, 512, NULL_LSN);
        assert!(result.is_ok());
    }
}
