//! Search file reader for finding specific entry types.
//!
//!
//! Reads forward through the log looking for entries of a specific type.

use crate::error::Result;
use crate::file_reader::{FileReader, LogFileAccess};
use hashbrown::HashSet;
use noxu_util::lsn::{Lsn, NULL_LSN};

/// Searches for log entries of specific types.
///
/// Reads forward from a starting LSN, filtering for target entry types.
pub struct SearchFileReader<F: LogFileAccess> {
    /// The underlying file reader
    reader: FileReader<F>,

    /// Target entry types to match
    target_types: HashSet<u8>,
}

impl<F: LogFileAccess> SearchFileReader<F> {
    /// Create a SearchFileReader.
    ///
    /// # Arguments
    /// * `file_access` - File I/O interface
    /// * `read_buffer_size` - Size of read buffer
    /// * `forward` - Read direction (typically true)
    /// * `start_lsn` - Where to start reading
    /// * `end_of_file_lsn` - End of log (for backward reading)
    /// * `target_types` - Set of entry type numbers to search for
    pub fn new(
        file_access: F,
        read_buffer_size: usize,
        forward: bool,
        start_lsn: Lsn,
        end_of_file_lsn: Lsn,
        target_types: HashSet<u8>,
    ) -> Result<Self> {
        let reader = FileReader::new(
            file_access,
            forward,
            start_lsn,
            end_of_file_lsn,
            NULL_LSN, // no finish
            read_buffer_size,
            true, // validate checksum
        )?;

        Ok(SearchFileReader { reader, target_types })
    }

    /// Add a target entry type to search for.
    pub fn add_target_type(&mut self, entry_type: u8) {
        self.target_types.insert(entry_type);
    }

    /// Read the next matching entry.
    ///
    /// Returns `Ok(true)` if a matching entry was found, `Ok(false)` at end.
    pub fn read_next_entry(&mut self) -> Result<bool> {
        // The FileReader will call our is_target_entry via callback
        // For now, we'll implement filtering in read_next_entry itself
        loop {
            if !self.reader.read_next_entry()? {
                return Ok(false);
            }

            // Check if this entry matches our target types
            if let Some(header) = self.reader.get_current_entry_header()
                && self.target_types.contains(&header.entry_type)
            {
                return Ok(true);
            }
            // Otherwise continue to next entry
        }
    }

    /// Get the LSN of the current (last read) entry.
    pub fn get_last_lsn(&self) -> Lsn {
        self.reader.get_current_entry_lsn()
    }

    /// Get the number of entries read (including non-target entries).
    pub fn get_num_read(&self) -> u64 {
        self.reader.get_num_read()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
    fn test_search_file_reader_creation() {
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![0u8; 100]);

        let start_lsn = Lsn::new(0, 0);
        let target_types = [1, 2, 3].iter().copied().collect();

        let result = SearchFileReader::new(
            mock,
            1024,
            true,
            start_lsn,
            NULL_LSN,
            target_types,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_search_file_reader_empty_target_types() {
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![0u8; 64]);
        let start_lsn = Lsn::new(0, 0);
        let result = SearchFileReader::new(
            mock,
            64,
            true,
            start_lsn,
            NULL_LSN,
            HashSet::new(),
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_search_file_reader_add_target_type() {
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![0u8; 64]);
        let start_lsn = Lsn::new(0, 0);
        let mut reader = SearchFileReader::new(
            mock,
            64,
            true,
            start_lsn,
            NULL_LSN,
            HashSet::new(),
        )
        .unwrap();

        reader.add_target_type(5);
        reader.add_target_type(10);
        // No assertions possible on internal set, but ensure no panic
    }

    #[test]
    fn test_search_file_reader_get_last_lsn_initial() {
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![0u8; 64]);
        let start_lsn = Lsn::new(0, 0);
        let reader = SearchFileReader::new(
            mock,
            64,
            true,
            start_lsn,
            NULL_LSN,
            HashSet::new(),
        )
        .unwrap();

        let lsn = reader.get_last_lsn();
        assert_eq!(lsn.file_number(), 0);
    }

    #[test]
    fn test_search_file_reader_get_num_read_initial() {
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![0u8; 64]);
        let start_lsn = Lsn::new(0, 0);
        let reader = SearchFileReader::new(
            mock,
            64,
            true,
            start_lsn,
            NULL_LSN,
            HashSet::new(),
        )
        .unwrap();
        assert_eq!(reader.get_num_read(), 0);
    }

    #[test]
    fn test_search_file_reader_read_next_entry_no_files() {
        let mock = MockFileAccess::new();
        let start_lsn = Lsn::new(0, 0);
        let mut reader = SearchFileReader::new(
            mock,
            64,
            true,
            start_lsn,
            NULL_LSN,
            HashSet::new(),
        )
        .unwrap();
        let result = reader.read_next_entry();
        assert!(matches!(result, Ok(false)) || result.is_err());
    }

    #[test]
    fn test_search_file_reader_read_matching_type_zero() {
        // LogEntryHeader::from_bytes is a stub returning entry_type=0 always.
        // So type 0 is the only type that can ever match.
        let mut mock = MockFileAccess::new();
        let data = vec![0u8; 14]; // stub: entry_type=0
        mock.add_file(0, data);
        let start_lsn = Lsn::new(0, 0);
        let target_types = [0u8].iter().copied().collect();
        let mut reader = SearchFileReader::new(
            mock,
            64,
            true,
            start_lsn,
            NULL_LSN,
            target_types,
        )
        .unwrap();

        let result = reader.read_next_entry();
        assert!(matches!(result, Ok(true)));
        assert_eq!(reader.get_num_read(), 1);
    }

    #[test]
    fn test_search_file_reader_skip_non_matching_type() {
        // Entry has stub entry_type=0, but we target type 7. No match.
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![0u8; 14]);
        let start_lsn = Lsn::new(0, 0);
        let target_types = [7u8].iter().copied().collect();
        let mut reader = SearchFileReader::new(
            mock,
            64,
            true,
            start_lsn,
            NULL_LSN,
            target_types,
        )
        .unwrap();

        let result = reader.read_next_entry();
        assert!(matches!(result, Ok(false)) || result.is_err());
    }

    #[test]
    fn test_search_file_reader_multiple_entries_first_matches() {
        // Two entries of stub type 0; target is 0 → first entry matches
        let mut mock = MockFileAccess::new();
        let data = vec![0u8; 28];
        mock.add_file(0, data);
        let start_lsn = Lsn::new(0, 0);
        let target_types = [0u8].iter().copied().collect();
        let mut reader = SearchFileReader::new(
            mock,
            64,
            true,
            start_lsn,
            NULL_LSN,
            target_types,
        )
        .unwrap();

        assert!(matches!(reader.read_next_entry(), Ok(true)));
        assert_eq!(reader.get_num_read(), 1);
    }

    #[test]
    fn test_search_file_reader_second_entry_matches_after_skip() {
        // With stub, all entries are type 0. First entry matches type 0,
        // then second entry also matches type 0 on next call.
        let mut mock = MockFileAccess::new();
        let data = vec![0u8; 28];
        mock.add_file(0, data);
        let start_lsn = Lsn::new(0, 0);
        let target_types = [0u8].iter().copied().collect();
        let mut reader = SearchFileReader::new(
            mock,
            64,
            true,
            start_lsn,
            NULL_LSN,
            target_types,
        )
        .unwrap();

        assert!(matches!(reader.read_next_entry(), Ok(true)));
        assert_eq!(reader.get_num_read(), 1);
        assert!(matches!(reader.read_next_entry(), Ok(true)));
        assert_eq!(reader.get_num_read(), 2);
    }

    #[test]
    fn test_search_file_reader_no_match_exhausts_file() {
        // Two entries, neither matches (target=7, stub returns 0)
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![0u8; 28]);
        let start_lsn = Lsn::new(0, 0);
        let target_types = [7u8].iter().copied().collect();
        let mut reader = SearchFileReader::new(
            mock,
            64,
            true,
            start_lsn,
            NULL_LSN,
            target_types,
        )
        .unwrap();

        let result = reader.read_next_entry();
        assert!(matches!(result, Ok(false)) || result.is_err());
        // Underlying reader scanned both entries
        assert_eq!(reader.get_num_read(), 2);
    }

    #[test]
    fn test_search_file_reader_add_type_dynamically() {
        // Add type 0 after creation; stub always returns type 0 so it matches
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![0u8; 14]);
        let start_lsn = Lsn::new(0, 0);
        let mut reader = SearchFileReader::new(
            mock,
            64,
            true,
            start_lsn,
            NULL_LSN,
            HashSet::new(),
        )
        .unwrap();

        reader.add_target_type(0); // type 0 always produced by stub
        let result = reader.read_next_entry();
        assert!(matches!(result, Ok(true)));
    }
}
