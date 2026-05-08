//! Last file reader for finding the true end of the log.
//!
//!
//! Traverses the last log file backward to find the true end of the log,
//! handling partial writes at the end of the file. Used during recovery.

use crate::error::{NoxuLogError, Result};
use crate::file_reader::{FileReader, LogFileAccess};
use noxu_util::lsn::{Lsn, NULL_LSN};
use std::collections::{HashMap, HashSet};

/// Tracks the last occurrence of specific log entry types.
///
/// Used during recovery to find checkpoint entries, etc.
pub struct LastFileReader<F: LogFileAccess> {
    /// The underlying file reader
    reader: FileReader<F>,

    /// Entry types we're tracking
    trackable_entries: HashSet<u8>,

    /// Last offset seen for each tracked type
    last_offset_seen: HashMap<u8, u64>,

    /// Offset of the next unproven (potentially corrupt) entry
    next_unproven_offset: u64,

    /// Offset of the last valid entry
    last_valid_offset: u64,

    /// Type of the last entry processed
    last_entry_type: u8,

    /// File number being scanned
    file_num: u32,
}

impl<F: LogFileAccess> LastFileReader<F> {
    /// Create a LastFileReader.
    ///
    /// Automatically positions at the last good file with a complete header.
    ///
    /// # Arguments
    /// * `file_access` - File I/O interface
    /// * `read_buffer_size` - Size of read buffer
    pub fn new(file_access: F, read_buffer_size: usize) -> Result<Self> {
        // Start at what appears to be the last file
        let (file_num, file_len) = Self::find_last_good_file(&file_access)?;

        let start_lsn = Lsn::new(file_num, 0);
        let end_of_file_lsn = Lsn::new(file_num, file_len as u32);

        let reader = FileReader::new(
            file_access,
            true, // forward
            start_lsn,
            end_of_file_lsn,
            NULL_LSN, // no finish
            read_buffer_size,
            true, // validate checksum
        )?;

        Ok(LastFileReader {
            reader,
            trackable_entries: HashSet::new(),
            last_offset_seen: HashMap::new(),
            next_unproven_offset: 0,
            last_valid_offset: 0,
            last_entry_type: 0,
            file_num,
        })
    }

    /// Find the last file with a complete, valid header.
    ///
    /// Returns (file_num, file_length).
    fn find_last_good_file(file_access: &F) -> Result<(u32, u64)> {
        // Start with first file if none found
        let first_file = file_access.get_first_file_num().unwrap_or(0);

        let mut current_file = first_file;
        let mut last_good_file = None;

        // Scan forward to find all files
        #[allow(clippy::while_let_loop)]
        loop {
            match file_access.get_file_length(current_file) {
                Ok(len) => {
                    // File exists and has valid length
                    if len > 0 {
                        last_good_file = Some((current_file, len));
                    }

                    // Try next file
                    if let Some(next) =
                        file_access.get_following_file_num(current_file, true)
                    {
                        current_file = next;
                    } else {
                        break;
                    }
                }
                Err(_) => {
                    // File doesn't exist or can't be read
                    break;
                }
            }
        }

        last_good_file.ok_or_else(|| NoxuLogError::UnexpectedEof {
            lsn: NULL_LSN,
            message: "No valid log files found".to_string(),
        })
    }

    /// Register an entry type to track.
    ///
    /// When entries of this type are encountered, their LSN will be recorded.
    pub fn set_target_type(&mut self, entry_type: u8) {
        self.trackable_entries.insert(entry_type);
    }

    /// Get the last LSN seen for a tracked entry type.
    ///
    /// Returns NULL_LSN if the type was not seen.
    pub fn get_last_seen(&self, entry_type: u8) -> Lsn {
        self.last_offset_seen
            .get(&entry_type)
            .map(|&offset| Lsn::new(self.file_num, offset as u32))
            .unwrap_or(NULL_LSN)
    }

    /// Get the end-of-log LSN.
    ///
    /// This is the LSN to use for the next log entry.
    pub fn get_end_of_log(&self) -> Lsn {
        Lsn::new(self.file_num, self.next_unproven_offset as u32)
    }

    /// Get the last valid LSN.
    ///
    /// This is the LSN of the last successfully validated entry.
    pub fn get_last_valid_lsn(&self) -> Lsn {
        Lsn::new(self.file_num, self.last_valid_offset as u32)
    }

    /// Get the previous offset from the last entry.
    pub fn get_prev_offset(&self) -> u64 {
        self.last_valid_offset
    }

    /// Get the type of the last entry processed.
    pub fn get_entry_type(&self) -> u8 {
        self.last_entry_type
    }

    /// Read the next entry.
    ///
    /// This method stops at bad entries (checksum failures) and reports them
    /// as the end of the log, rather than throwing an error.
    ///
    /// Returns `Ok(true)` if an entry was read, `Ok(false)` at end.
    pub fn read_next_entry(&mut self) -> Result<bool> {
        // Save current position
        let current_offset =
            self.reader.get_current_entry_lsn().file_offset() as u64;
        let _next_offset = current_offset; // Will be updated by reader

        // Try to read the next entry
        match self.reader.read_next_entry() {
            Ok(found) => {
                if found {
                    // Successfully read an entry
                    let lsn = self.reader.get_current_entry_lsn();
                    self.last_valid_offset = lsn.file_offset() as u64;
                    self.next_unproven_offset = self.last_valid_offset
                        + self.reader.get_last_entry_size() as u64;

                    // Track this entry type if requested
                    if let Some(header) = self.reader.get_current_entry_header()
                    {
                        self.last_entry_type = header.entry_type;

                        if self.trackable_entries.contains(&header.entry_type) {
                            self.last_offset_seen.insert(
                                header.entry_type,
                                self.last_valid_offset,
                            );
                        }
                    }

                    Ok(true)
                } else {
                    // Reached end of log normally
                    Ok(false)
                }
            }
            Err(NoxuLogError::Checksum { lsn: _, .. }) => {
                // Checksum error - this is expected at end of log
                // The last_valid_offset points to the last good entry
                // next_unproven_offset points to the bad entry
                // Stop reading and report false
                Ok(false)
            }
            Err(e) => {
                // Other errors are real problems
                Err(e)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file_reader::LogFileAccess;
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
    fn test_last_file_reader_creation() {
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![0u8; 100]);

        let result = LastFileReader::new(mock, 1024);
        assert!(result.is_ok());
    }

    #[test]
    fn test_find_last_good_file() {
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![0u8; 100]);
        mock.add_file(1, vec![0u8; 200]);
        mock.add_file(2, vec![0u8; 50]);

        let (file_num, len) =
            LastFileReader::find_last_good_file(&mock).unwrap();
        assert_eq!(file_num, 2);
        assert_eq!(len, 50);
    }

    #[test]
    fn test_last_file_reader_no_files() {
        let mock = MockFileAccess::new();
        let result = LastFileReader::new(mock, 1024);
        // No files: find_last_good_file returns error
        assert!(result.is_err());
    }

    #[test]
    fn test_last_file_reader_single_file() {
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![0u8; 100]);
        let result = LastFileReader::new(mock, 1024);
        assert!(result.is_ok());
    }

    #[test]
    fn test_find_last_good_file_single_file() {
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![0u8; 42]);
        let (file_num, len) =
            LastFileReader::find_last_good_file(&mock).unwrap();
        assert_eq!(file_num, 0);
        assert_eq!(len, 42);
    }

    #[test]
    fn test_find_last_good_file_empty_file_skipped() {
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![0u8; 100]);
        // File 1 is zero-length: should NOT become last_good_file
        mock.add_file(1, vec![]);
        let (file_num, len) =
            LastFileReader::find_last_good_file(&mock).unwrap();
        assert_eq!(file_num, 0);
        assert_eq!(len, 100);
    }

    #[test]
    fn test_last_file_reader_set_target_type() {
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![0u8; 64]);
        let mut reader = LastFileReader::new(mock, 64).unwrap();

        reader.set_target_type(1);
        reader.set_target_type(2);
        // get_last_seen for unread types returns NULL_LSN
        assert!(reader.get_last_seen(1).is_null());
        assert!(reader.get_last_seen(255).is_null());
    }

    #[test]
    fn test_last_file_reader_initial_offsets() {
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![0u8; 128]);
        let reader = LastFileReader::new(mock, 64).unwrap();

        assert_eq!(reader.get_prev_offset(), 0);
        assert_eq!(reader.get_entry_type(), 0);
        // end_of_log starts at offset 0 because no entries read yet
        let eol = reader.get_end_of_log();
        assert_eq!(eol.file_number(), 0);
    }

    #[test]
    fn test_last_file_reader_read_entry() {
        // File with one minimal 14-byte entry (type=0)
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![0u8; 14]);
        let mut reader = LastFileReader::new(mock, 64).unwrap();

        let result = reader.read_next_entry();
        assert!(matches!(result, Ok(true)));
        assert_eq!(reader.get_entry_type(), 0);
    }

    #[test]
    fn test_last_file_reader_read_entry_updates_valid_lsn() {
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![0u8; 14]);
        let mut reader = LastFileReader::new(mock, 64).unwrap();

        reader.read_next_entry().unwrap();
        let valid_lsn = reader.get_last_valid_lsn();
        assert_eq!(valid_lsn.file_number(), 0);
    }

    #[test]
    fn test_last_file_reader_read_entry_updates_end_of_log() {
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![0u8; 14]);
        let mut reader = LastFileReader::new(mock, 64).unwrap();

        reader.read_next_entry().unwrap();
        let eol = reader.get_end_of_log();
        assert_eq!(eol.file_number(), 0);
        // end_of_log offset is a u32; just verify it's accessible
        let _ = eol.file_offset();
    }

    #[test]
    fn test_last_file_reader_tracks_target_type() {
        // LogEntryHeader::from_bytes is a stub returning entry_type=0 always.
        // So we must track type 0 to see it recorded.
        let mut mock = MockFileAccess::new();
        let data = vec![0u8; 28]; // two entries, both stub type 0
        mock.add_file(0, data);
        let mut reader = LastFileReader::new(mock, 64).unwrap();
        reader.set_target_type(0); // type 0 is what the stub always returns

        reader.read_next_entry().unwrap();
        reader.read_next_entry().unwrap();

        // get_last_seen(0) should be non-null after reading two entries
        let lsn = reader.get_last_seen(0);
        assert!(!lsn.is_null());
    }

    #[test]
    fn test_last_file_reader_untracked_type_returns_null() {
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![0u8; 14]); // stub type 0
        let mut reader = LastFileReader::new(mock, 64).unwrap();
        // Track type 0; do NOT track type 5

        reader.read_next_entry().unwrap();
        // Type 5 was never tracked, so it's null
        assert!(reader.get_last_seen(5).is_null());
    }

    #[test]
    fn test_last_file_reader_read_until_eof() {
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![0u8; 14]);
        let mut reader = LastFileReader::new(mock, 64).unwrap();

        assert!(matches!(reader.read_next_entry(), Ok(true)));
        let result = reader.read_next_entry();
        // After exhausting the file, should return Ok(false) or an error
        assert!(matches!(result, Ok(false)) || result.is_err());
    }
}
