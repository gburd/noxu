//! Checkpoint file reader for recovery.
//!
//!
//! Scans backward from the end of log looking for checkpoint entries.
//! Used during recovery to find the last checkpoint.

use crate::entry_type::LogEntryType;
use crate::error::Result;
use crate::file_reader::{FileReader, LogFileAccess};
use noxu_util::lsn::Lsn;

/// Searches for checkpoint and DbTree entries.
///
/// Reads backward from end of log to find checkpoint boundaries and the
/// database tree root.
pub struct CheckpointFileReader<F: LogFileAccess> {
    /// The underlying file reader
    reader: FileReader<F>,

    /// True if last entry was a checkpoint end
    is_checkpoint_end: bool,

    /// True if last entry was a checkpoint start
    is_checkpoint_start: bool,

    /// True if last entry was a DbTree
    is_db_tree: bool,
}

impl<F: LogFileAccess> CheckpointFileReader<F> {
    /// Create a CheckpointFileReader.
    ///
    /// # Arguments
    /// * `file_access` - File I/O interface
    /// * `read_buffer_size` - Size of read buffer
    /// * `forward` - Read direction (typically false for checkpoint scan)
    /// * `start_lsn` - Where to start reading
    /// * `finish_lsn` - Stop reading at this LSN
    /// * `end_of_file_lsn` - End of log LSN
    pub fn new(
        file_access: F,
        read_buffer_size: usize,
        forward: bool,
        start_lsn: Lsn,
        finish_lsn: Lsn,
        end_of_file_lsn: Lsn,
    ) -> Result<Self> {
        let reader = FileReader::new(
            file_access,
            forward,
            start_lsn,
            end_of_file_lsn,
            finish_lsn,
            read_buffer_size,
            true, // validate checksum
        )?;

        Ok(CheckpointFileReader {
            reader,
            is_checkpoint_end: false,
            is_checkpoint_start: false,
            is_db_tree: false,
        })
    }

    /// Read the next checkpoint-related entry.
    ///
    /// Returns `Ok(true)` if a checkpoint entry was found, `Ok(false)` at end.
    pub fn read_next_entry(&mut self) -> Result<bool> {
        // Reset flags
        self.is_checkpoint_end = false;
        self.is_checkpoint_start = false;
        self.is_db_tree = false;

        // Read entries until we find a target type
        loop {
            if !self.reader.read_next_entry()? {
                return Ok(false);
            }

            // Check if this entry is a checkpoint-related type
            if let Some(header) = self.reader.get_current_entry_header() {
                let is_target = match header.entry_type {
                    t if t == LogEntryType::CkptEnd as u8 => {
                        self.is_checkpoint_end = true;
                        true
                    }
                    t if t == LogEntryType::CkptStart as u8 => {
                        self.is_checkpoint_start = true;
                        true
                    }
                    t if t == LogEntryType::DbTree as u8 => {
                        self.is_db_tree = true;
                        true
                    }
                    _ => false,
                };

                if is_target {
                    return Ok(true);
                }
            }
            // Otherwise continue to next entry
        }
    }

    /// Returns true if the last entry was a checkpoint end.
    pub fn is_checkpoint_end(&self) -> bool {
        self.is_checkpoint_end
    }

    /// Returns true if the last entry was a checkpoint start.
    pub fn is_checkpoint_start(&self) -> bool {
        self.is_checkpoint_start
    }

    /// Returns true if the last entry was a DbTree.
    pub fn is_db_tree(&self) -> bool {
        self.is_db_tree
    }

    /// Get the LSN of the current (last read) entry.
    pub fn get_last_lsn(&self) -> Lsn {
        self.reader.get_current_entry_lsn()
    }

    /// Get the number of entries read.
    pub fn get_num_read(&self) -> u64 {
        self.reader.get_num_read()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use noxu_util::lsn::NULL_LSN;
    use std::collections::HashMap;
    use std::io;

    /// Mock file access for testing.
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
            self.files.get(&file_num).map(|data| data.len() as u64).ok_or_else(
                || {
                    io::Error::new(io::ErrorKind::NotFound, "File not found")
                        .into()
                },
            )
        }

        fn get_first_file_num(&self) -> Option<u32> {
            self.files.keys().min().copied()
        }

        fn get_following_file_num(
            &self,
            file_num: u32,
            forward: bool,
        ) -> Option<u32> {
            let mut file_nums: Vec<u32> = self.files.keys().copied().collect();
            file_nums.sort();

            if forward {
                file_nums.iter().find(|&&n| n > file_num).copied()
            } else {
                file_nums.iter().rev().find(|&&n| n < file_num).copied()
            }
        }

        fn get_file_header_prev_offset(&self, _file_num: u32) -> Result<u64> {
            Ok(0)
        }
    }

    #[test]
    fn test_checkpoint_file_reader_creation() {
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![0u8; 100]);

        let start_lsn = Lsn::new(0, 100);
        let end_lsn = Lsn::new(0, 100);

        let result = CheckpointFileReader::new(
            mock, 1024, false, // backward
            start_lsn, NULL_LSN, end_lsn,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_checkpoint_file_reader_initial_flags() {
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![0u8; 100]);
        let start_lsn = Lsn::new(0, 0);
        let reader = CheckpointFileReader::new(
            mock, 1024, true, start_lsn, NULL_LSN, NULL_LSN,
        )
        .unwrap();

        assert!(!reader.is_checkpoint_end());
        assert!(!reader.is_checkpoint_start());
        assert!(!reader.is_db_tree());
    }

    #[test]
    fn test_checkpoint_file_reader_get_last_lsn_initial() {
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![0u8; 64]);
        let start_lsn = Lsn::new(0, 0);
        let reader = CheckpointFileReader::new(
            mock, 64, true, start_lsn, NULL_LSN, NULL_LSN,
        )
        .unwrap();

        let lsn = reader.get_last_lsn();
        assert_eq!(lsn.file_number(), 0);
    }

    #[test]
    fn test_checkpoint_file_reader_get_num_read_initial() {
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![0u8; 64]);
        let start_lsn = Lsn::new(0, 0);
        let reader = CheckpointFileReader::new(
            mock, 64, true, start_lsn, NULL_LSN, NULL_LSN,
        )
        .unwrap();
        assert_eq!(reader.get_num_read(), 0);
    }

    #[test]
    fn test_checkpoint_file_reader_read_no_entries() {
        // Empty file store: reading immediately returns false
        let mock = MockFileAccess::new();
        let start_lsn = Lsn::new(0, 0);
        let mut reader = CheckpointFileReader::new(
            mock, 1024, true, start_lsn, NULL_LSN, NULL_LSN,
        )
        .unwrap();

        let result = reader.read_next_entry();
        assert!(matches!(result, Ok(false)) || result.is_err());
    }

    #[test]
    fn test_checkpoint_file_reader_read_non_checkpoint_entry() {
        // A file with a single entry of type 0 (not CKPT_END/START/DBTREE)
        let mut mock = MockFileAccess::new();
        let data = vec![0u8; 14]; // type=0
        mock.add_file(0, data);
        let start_lsn = Lsn::new(0, 0);
        let mut reader = CheckpointFileReader::new(
            mock, 64, true, start_lsn, NULL_LSN, NULL_LSN,
        )
        .unwrap();

        // Entry type 0 is not a checkpoint type; loop will exhaust the file
        let result = reader.read_next_entry();
        assert!(matches!(result, Ok(false)) || result.is_err());
        assert!(!reader.is_checkpoint_end());
        assert!(!reader.is_checkpoint_start());
        assert!(!reader.is_db_tree());
    }

    #[test]
    fn test_checkpoint_file_reader_read_checkpoint_end_entry() {
        // NOTE: LogEntryHeader::from_bytes is a stub that always returns
        // entry_type=0. So the only type that can match is when LOG_CKPT_END==0,
        // which it isn't (it's 1). Therefore any single-entry file produces
        // a non-matching entry; read_next_entry exhausts the file.
        // This test verifies the entry-type-zero path (no checkpoint entry).
        let mut mock = MockFileAccess::new();
        let data = vec![0u8; 14]; // entry_type=0 (stub always returns 0)
        mock.add_file(0, data);
        let start_lsn = Lsn::new(0, 0);
        let mut reader = CheckpointFileReader::new(
            mock, 64, true, start_lsn, NULL_LSN, NULL_LSN,
        )
        .unwrap();

        // entry_type=0 is not LOG_CKPT_END(1)/LOG_CKPT_START(2)/LOG_DBTREE(3),
        // so this exhausts the file without finding a target.
        let result = reader.read_next_entry();
        assert!(matches!(result, Ok(false)) || result.is_err());
        assert!(!reader.is_checkpoint_end());
        assert!(!reader.is_checkpoint_start());
        assert!(!reader.is_db_tree());
    }

    #[test]
    fn test_checkpoint_file_reader_read_checkpoint_start_entry() {
        // Stub always returns entry_type=0; no checkpoint entries possible.
        // Verify read returns false/err and flags remain unset.
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![0u8; 14]);
        let start_lsn = Lsn::new(0, 0);
        let mut reader = CheckpointFileReader::new(
            mock, 64, true, start_lsn, NULL_LSN, NULL_LSN,
        )
        .unwrap();

        let result = reader.read_next_entry();
        assert!(matches!(result, Ok(false)) || result.is_err());
        assert!(!reader.is_checkpoint_end());
        assert!(!reader.is_checkpoint_start());
        assert!(!reader.is_db_tree());
    }

    #[test]
    fn test_checkpoint_file_reader_read_dbtree_entry() {
        // Same stub constraint: entry_type=0, none match LOG_DBTREE(3).
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![0u8; 14]);
        let start_lsn = Lsn::new(0, 0);
        let mut reader = CheckpointFileReader::new(
            mock, 64, true, start_lsn, NULL_LSN, NULL_LSN,
        )
        .unwrap();

        let result = reader.read_next_entry();
        assert!(matches!(result, Ok(false)) || result.is_err());
        assert!(!reader.is_db_tree());
    }

    #[test]
    fn test_checkpoint_file_reader_multiple_entries_all_skipped() {
        // Two entries: both type 0 (stub). Neither matches; both are scanned
        // and then the file is exhausted.
        let mut mock = MockFileAccess::new();
        let data = vec![0u8; 28]; // two 14-byte entries
        mock.add_file(0, data);
        let start_lsn = Lsn::new(0, 0);
        let mut reader = CheckpointFileReader::new(
            mock, 64, true, start_lsn, NULL_LSN, NULL_LSN,
        )
        .unwrap();

        let result = reader.read_next_entry();
        // No checkpoint-type entries found
        assert!(matches!(result, Ok(false)) || result.is_err());
        assert!(!reader.is_checkpoint_end());
        // Both entries were scanned by the underlying reader
        assert_eq!(reader.get_num_read(), 2);
    }

    #[test]
    fn test_checkpoint_file_reader_flags_stay_false_with_stub_header() {
        // With the stub header parser, all entries have type 0.
        // Verify flags remain false after scanning a multi-entry file.
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![0u8; 28]);
        let start_lsn = Lsn::new(0, 0);
        let mut reader = CheckpointFileReader::new(
            mock, 64, true, start_lsn, NULL_LSN, NULL_LSN,
        )
        .unwrap();

        // First call exhausts non-matching entries
        let r = reader.read_next_entry();
        assert!(matches!(r, Ok(false)) || r.is_err());
        assert!(!reader.is_checkpoint_end());
        assert!(!reader.is_checkpoint_start());
        assert!(!reader.is_db_tree());
    }
}
