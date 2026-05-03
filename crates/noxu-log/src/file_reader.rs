//! Base file reader for sequential log scanning.
//!
//! Port of `com.sleepycat.je.log.FileReader`.
//!
//! A FileReader traverses the log files, reading chunks at a time. It provides
//! an iterator-like interface via its `read_next_entry()` method. Concrete
//! implementations control which entries to process and what to do with them.

use crate::error::{NoxuLogError, Result};
use noxu_util::lsn::{Lsn, NULL_LSN};

/// Trait for file I/O access.
///
/// Abstracts the underlying file access for testing and modularity.
/// FileManager will implement this trait once it's available.
pub trait LogFileAccess {
    /// Read data from a log file at the specified position.
    ///
    /// Returns the number of bytes actually read (may be less than buf.len()
    /// if end of file is reached).
    fn read_from_file(
        &self,
        file_num: u32,
        offset: u64,
        buf: &mut [u8],
    ) -> Result<usize>;

    /// Get the length of a log file in bytes.
    fn get_file_length(&self, file_num: u32) -> Result<u64>;

    /// Get the first file number in the log, or None if no files exist.
    fn get_first_file_num(&self) -> Option<u32>;

    /// Get the next file number after `file_num` (forward or backward).
    ///
    /// Returns None if no such file exists.
    fn get_following_file_num(
        &self,
        file_num: u32,
        forward: bool,
    ) -> Option<u32>;

    /// Get the file header's previous offset field for backward scanning.
    fn get_file_header_prev_offset(&self, file_num: u32) -> Result<u64>;
}

/// Minimal log entry header stub.
///
/// TODO: Replace with actual LogEntryHeader from entry module.
#[derive(Debug, Clone)]
pub struct LogEntryHeader {
    /// Entry type identifier
    pub entry_type: u8,
    /// Entry version
    pub version: u8,
    /// Previous entry offset (for backward scanning)
    pub prev_offset: u64,
    /// Size of this header
    pub header_size: usize,
    /// Size of the entry data (item)
    pub item_size: usize,
    /// Checksum of header + data
    pub checksum: u32,
    /// Whether this entry is replicated
    pub replicated: bool,
}

impl LogEntryHeader {
    /// Minimum header size (before any variable portion).
    pub const MIN_HEADER_SIZE: usize = 14;

    /// Returns the total size of the entry (header + data).
    pub fn entry_size(&self) -> usize {
        self.header_size + self.item_size
    }

    /// Returns whether this header has a variable-length portion.
    pub fn is_variable_length(&self) -> bool {
        // TODO: Determine based on entry type
        false
    }

    /// Returns the size of the variable portion.
    pub fn variable_portion_size(&self) -> usize {
        // TODO: Calculate based on entry type
        0
    }

    /// Stub: Parse basic header from buffer.
    ///
    /// TODO: Replace with actual deserialization logic.
    pub fn from_bytes(_buf: &[u8]) -> Result<Self> {
        // Placeholder implementation
        Ok(LogEntryHeader {
            entry_type: 0,
            version: 0,
            prev_offset: 0,
            header_size: Self::MIN_HEADER_SIZE,
            item_size: 0,
            checksum: 0,
            replicated: false,
        })
    }
}

/// Base file reader for sequential log scanning.
///
/// Reads forward or backward through the log, entry by entry.
/// Subclasses (via callbacks) control which entries to process.
pub struct FileReader<F: LogFileAccess> {
    /// File access interface
    file_access: F,

    /// Direction of reading (true = forward, false = backward)
    forward: bool,

    /// Current entry's LSN
    current_entry_lsn: Lsn,

    /// LSN of the next entry to read (forward mode)
    next_entry_lsn: Lsn,

    /// Start LSN (inclusive)
    start_lsn: Lsn,

    /// End LSN (exclusive for forward, or finish for backward)
    finish_lsn: Lsn,

    /// Read buffer
    read_buffer: Vec<u8>,

    /// Buffer size for reads
    read_buffer_size: usize,

    /// Current position within read buffer
    buffer_offset: usize,

    /// Number of valid bytes in buffer
    buffer_length: usize,

    /// The file number currently being read
    current_file_num: u32,

    /// Current file offset where buffer was filled from
    current_file_offset: u64,

    /// Whether to validate checksums
    validate_checksum: bool,

    /// Current entry header
    current_entry_header: Option<LogEntryHeader>,

    /// Previous entry offset (for backward scanning)
    current_entry_prev_offset: u64,

    /// Current entry's offset within file
    current_entry_offset: u64,

    /// Next entry's offset within file (forward mode)
    next_entry_offset: u64,

    /// Number of entries read
    entries_read: u64,

    /// Whether we've reached end of log
    eof: bool,

    /// Save buffer for piecing together entries that span buffer boundaries
    save_buffer: Vec<u8>,
}

impl<F: LogFileAccess> FileReader<F> {
    /// Create a new FileReader.
    ///
    /// # Arguments
    /// * `file_access` - File I/O interface
    /// * `forward` - Read direction (true = forward, false = backward)
    /// * `start_lsn` - Starting LSN (where to begin reading)
    /// * `end_of_file_lsn` - End of log LSN (for backward reading)
    /// * `finish_lsn` - Stop reading at this LSN (NULL_LSN = read to end)
    /// * `read_buffer_size` - Size of read buffer
    /// * `validate_checksum` - Whether to validate entry checksums
    pub fn new(
        file_access: F,
        forward: bool,
        start_lsn: Lsn,
        end_of_file_lsn: Lsn,
        finish_lsn: Lsn,
        read_buffer_size: usize,
        validate_checksum: bool,
    ) -> Result<Self> {
        let mut reader = FileReader {
            file_access,
            forward,
            current_entry_lsn: NULL_LSN,
            next_entry_lsn: NULL_LSN,
            start_lsn,
            finish_lsn,
            read_buffer: vec![0u8; read_buffer_size],
            read_buffer_size,
            buffer_offset: 0,
            buffer_length: 0,
            current_file_num: 0,
            current_file_offset: 0,
            validate_checksum,
            current_entry_header: None,
            current_entry_prev_offset: 0,
            current_entry_offset: 0,
            next_entry_offset: 0,
            entries_read: 0,
            eof: false,
            save_buffer: Vec::with_capacity(read_buffer_size),
        };

        reader.init_starting_position(start_lsn, end_of_file_lsn)?;
        Ok(reader)
    }

    /// Initialize the starting position for reading.
    fn init_starting_position(
        &mut self,
        start_lsn: Lsn,
        end_of_file_lsn: Lsn,
    ) -> Result<()> {
        self.eof = false;

        if self.forward {
            // Forward reading: start at start_lsn (or beginning of log)
            if !start_lsn.is_null() {
                self.current_file_num = start_lsn.file_number();
                self.current_file_offset = start_lsn.file_offset() as u64;
                self.next_entry_offset = start_lsn.file_offset() as u64;
            } else {
                // Start at beginning of log
                if let Some(first_file) = self.file_access.get_first_file_num()
                {
                    self.current_file_num = first_file;
                    self.current_file_offset = 0;
                    self.next_entry_offset = 0;
                } else {
                    self.eof = true;
                }
            }
        } else {
            // Backward reading: start at end_of_file_lsn
            assert!(
                !start_lsn.is_null(),
                "start_lsn must be valid for backward reading"
            );
            assert!(
                !end_of_file_lsn.is_null(),
                "end_of_file_lsn must be valid for backward reading"
            );

            self.current_file_num = end_of_file_lsn.file_number();
            self.current_file_offset = end_of_file_lsn.file_offset() as u64;
            self.current_entry_offset = end_of_file_lsn.file_offset() as u64;

            // Set up prev_offset for the first read
            if start_lsn.file_number() == end_of_file_lsn.file_number() {
                self.current_entry_prev_offset = start_lsn.file_offset() as u64;
            } else {
                self.current_entry_prev_offset = 0;
            }
        }

        Ok(())
    }

    /// Read the next entry from the log.
    ///
    /// Returns `Ok(true)` if an entry was read, `Ok(false)` if at end of log.
    pub fn read_next_entry(&mut self) -> Result<bool> {
        while !self.eof {
            // Read the header
            self.get_log_entry_in_buffer()?;

            // Read minimum header
            let header_buf =
                self.read_data(LogEntryHeader::MIN_HEADER_SIZE, true)?;
            let header = LogEntryHeader::from_bytes(header_buf)?;

            // Update offsets for forward reading
            if self.forward {
                self.current_entry_offset = self.next_entry_offset;
                self.next_entry_offset += header.entry_size() as u64;
            }

            self.current_entry_header = Some(header.clone());
            self.current_entry_prev_offset = header.prev_offset;

            // Check if this is a target entry
            if !self.is_target_entry()? {
                // Skip non-target entries
                continue;
            }

            // Read entry data
            let item_size = header.item_size;
            let _entry_data = self.read_data(item_size, true)?;

            // Validate checksum if enabled
            if self.validate_checksum {
                // TODO: Implement checksum validation
            }

            // Process the entry
            if self.process_entry()? {
                self.entries_read += 1;
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Get the log entry positioned in the read buffer.
    fn get_log_entry_in_buffer(&mut self) -> Result<()> {
        if self.forward {
            self.set_forward_position()?;
        } else {
            self.set_backward_position()?;
        }
        Ok(())
    }

    /// Set the position for forward reading.
    fn set_forward_position(&mut self) -> Result<()> {
        // Check if we've passed the finish LSN
        if !self.finish_lsn.is_null() {
            let next_lsn =
                Lsn::new(self.current_file_num, self.next_entry_offset as u32);
            if next_lsn >= self.finish_lsn {
                self.eof = true;
                return Err(NoxuLogError::UnexpectedEof {
                    lsn: next_lsn,
                    message: "Reached finish LSN".to_string(),
                });
            }
        }
        Ok(())
    }

    /// Set the position for backward reading.
    fn set_backward_position(&mut self) -> Result<()> {
        // Check if we need to move to a previous entry or file
        if self.current_entry_prev_offset != 0
            && self.buffer_contains_offset(self.current_entry_prev_offset)
        {
            // Entry is in current buffer
            self.position_buffer(self.current_entry_prev_offset);
        } else {
            // Need to read a different part of the file or different file
            if self.current_entry_prev_offset == 0 {
                // Move to previous file
                let prev_offset = self
                    .file_access
                    .get_file_header_prev_offset(self.current_file_num)?;
                let prev_file = self
                    .file_access
                    .get_following_file_num(self.current_file_num, false)
                    .ok_or_else(|| NoxuLogError::UnexpectedEof {
                        lsn: Lsn::new(self.current_file_num, 0),
                        message: "No previous file".to_string(),
                    })?;

                self.current_entry_prev_offset = prev_offset;
                self.current_file_num = prev_file;
            }

            // Fill buffer at new position
            self.fill_buffer_at(self.current_entry_prev_offset)?;
        }

        self.current_entry_offset = self.current_entry_prev_offset;

        // Check finish LSN
        if !self.finish_lsn.is_null() {
            let next_lsn = Lsn::new(
                self.current_file_num,
                self.current_entry_prev_offset as u32,
            );
            if next_lsn < self.finish_lsn {
                self.eof = true;
                return Err(NoxuLogError::UnexpectedEof {
                    lsn: next_lsn,
                    message: "Reached finish LSN (backward)".to_string(),
                });
            }
        }

        Ok(())
    }

    /// Read data from the log.
    ///
    /// Returns a slice of the requested data. May require multiple buffer fills.
    fn read_data(
        &mut self,
        amount: usize,
        _collect_data: bool,
    ) -> Result<&[u8]> {
        let mut already_read = 0;

        while already_read < amount && !self.eof {
            let bytes_available = self.buffer_length - self.buffer_offset;

            if bytes_available > 0 {
                let bytes_needed = amount - already_read;
                let bytes_to_copy = bytes_available.min(bytes_needed);

                if already_read > 0 {
                    // Need to accumulate in save buffer
                    let start = self.buffer_offset;
                    let end = self.buffer_offset + bytes_to_copy;
                    self.save_buffer
                        .extend_from_slice(&self.read_buffer[start..end]);
                    self.buffer_offset = end;
                    already_read += bytes_to_copy;
                } else {
                    // Can return directly from read buffer
                    if bytes_available >= bytes_needed {
                        let start = self.buffer_offset;
                        let end = start + bytes_needed;
                        self.buffer_offset = end;
                        return Ok(&self.read_buffer[start..end]);
                    } else {
                        // Need to accumulate
                        let start = self.buffer_offset;
                        let end = self.buffer_offset + bytes_available;
                        self.save_buffer.clear();
                        self.save_buffer
                            .extend_from_slice(&self.read_buffer[start..end]);
                        self.buffer_offset = end;
                        already_read += bytes_available;
                    }
                }
            } else {
                // Need to fill buffer
                self.fill_next_buffer()?;
            }
        }

        if already_read < amount {
            let lsn = Lsn::new(
                self.current_file_num,
                self.current_entry_offset as u32,
            );
            return Err(NoxuLogError::UnexpectedEof {
                lsn,
                message: format!("Need {} bytes, got {}", amount, already_read),
            });
        }

        Ok(&self.save_buffer[..amount])
    }

    /// Fill the buffer from the current position.
    fn fill_next_buffer(&mut self) -> Result<()> {
        // Move to next position
        self.current_file_offset += self.buffer_length as u64;

        // Check if we need to move to next file
        let file_len =
            self.file_access.get_file_length(self.current_file_num)?;
        if self.current_file_offset >= file_len {
            // Move to next file
            if let Some(next_file) = self
                .file_access
                .get_following_file_num(self.current_file_num, true)
            {
                self.current_file_num = next_file;
                self.current_file_offset = 0;
                self.next_entry_offset = 0;
            } else {
                self.eof = true;
                let lsn = Lsn::new(
                    self.current_file_num,
                    self.current_file_offset as u32,
                );
                return Err(NoxuLogError::UnexpectedEof {
                    lsn,
                    message: "No next file".to_string(),
                });
            }
        }

        // Read from file
        let bytes_read = self.file_access.read_from_file(
            self.current_file_num,
            self.current_file_offset,
            &mut self.read_buffer,
        )?;

        if bytes_read == 0 {
            self.eof = true;
            let lsn = Lsn::new(
                self.current_file_num,
                self.current_file_offset as u32,
            );
            return Err(NoxuLogError::UnexpectedEof {
                lsn,
                message: "File read returned 0 bytes".to_string(),
            });
        }

        self.buffer_offset = 0;
        self.buffer_length = bytes_read;

        Ok(())
    }

    /// Fill buffer at a specific offset.
    fn fill_buffer_at(&mut self, offset: u64) -> Result<()> {
        self.current_file_offset = offset;
        self.buffer_offset = 0;
        self.buffer_length = 0;
        self.fill_next_buffer()
    }

    /// Check if the buffer contains a given offset.
    fn buffer_contains_offset(&self, offset: u64) -> bool {
        offset >= self.current_file_offset
            && offset < self.current_file_offset + self.buffer_length as u64
    }

    /// Position the buffer to a specific offset within it.
    fn position_buffer(&mut self, offset: u64) {
        assert!(self.buffer_contains_offset(offset));
        self.buffer_offset = (offset - self.current_file_offset) as usize;
    }

    /// Check if the current entry is a target for processing.
    ///
    /// Subclasses override this to filter entries.
    /// Default: all entries are targets.
    fn is_target_entry(&self) -> Result<bool> {
        Ok(true)
    }

    /// Process the current entry.
    ///
    /// Subclasses override this to handle entries.
    /// Returns true if the entry should be returned to the caller.
    fn process_entry(&mut self) -> Result<bool> {
        Ok(true)
    }

    /// Get the current entry's LSN.
    pub fn get_current_entry_lsn(&self) -> Lsn {
        Lsn::new(self.current_file_num, self.current_entry_offset as u32)
    }

    /// Get the current entry header.
    pub fn get_current_entry_header(&self) -> Option<&LogEntryHeader> {
        self.current_entry_header.as_ref()
    }

    /// Get the number of entries read.
    pub fn get_num_read(&self) -> u64 {
        self.entries_read
    }

    /// Get the size of the last entry read (header + data).
    pub fn get_last_entry_size(&self) -> usize {
        self.current_entry_header.as_ref().map(|h| h.entry_size()).unwrap_or(0)
    }

    /// Check if the current entry is replicated.
    pub fn entry_is_replicated(&self) -> bool {
        self.current_entry_header
            .as_ref()
            .map(|h| h.replicated)
            .unwrap_or(false)
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
            Ok(self
                .files
                .get(&file_num)
                .map(|data| data.len() as u64)
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::NotFound, "File not found")
                })?)
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
    fn test_mock_file_access() {
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![1, 2, 3, 4, 5]);

        let mut buf = [0u8; 3];
        let n = mock.read_from_file(0, 1, &mut buf).unwrap();
        assert_eq!(n, 3);
        assert_eq!(&buf, &[2, 3, 4]);
    }

    #[test]
    fn test_file_reader_creation() {
        let mock = MockFileAccess::new();
        let start_lsn = Lsn::new(0, 0);
        let result = FileReader::new(
            mock, true, start_lsn, NULL_LSN, NULL_LSN, 1024, true,
        );

        // Will set eof=true because no files exist
        assert!(result.is_ok());
    }

    #[test]
    fn test_file_reader_creation_with_null_lsn() {
        // NULL_LSN with no files: eof is set but construction succeeds
        let mock = MockFileAccess::new();
        let result =
            FileReader::new(mock, true, NULL_LSN, NULL_LSN, NULL_LSN, 512, false);
        assert!(result.is_ok());
    }

    #[test]
    fn test_file_reader_creation_with_files_forward() {
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![0u8; 128]);
        let start_lsn = Lsn::new(0, 0);
        let result = FileReader::new(
            mock, true, start_lsn, NULL_LSN, NULL_LSN, 64, false,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_file_reader_get_current_entry_lsn_initial() {
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![0u8; 128]);
        let start_lsn = Lsn::new(0, 0);
        let reader = FileReader::new(
            mock, true, start_lsn, NULL_LSN, NULL_LSN, 64, false,
        )
        .unwrap();

        let lsn = reader.get_current_entry_lsn();
        // Before reading, current_entry_offset == 0
        assert_eq!(lsn.file_number(), 0);
    }

    #[test]
    fn test_file_reader_get_current_entry_header_none_initially() {
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![0u8; 64]);
        let start_lsn = Lsn::new(0, 0);
        let reader = FileReader::new(
            mock, true, start_lsn, NULL_LSN, NULL_LSN, 32, false,
        )
        .unwrap();

        assert!(reader.get_current_entry_header().is_none());
    }

    #[test]
    fn test_file_reader_get_num_read_initial() {
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![0u8; 64]);
        let start_lsn = Lsn::new(0, 0);
        let reader = FileReader::new(
            mock, true, start_lsn, NULL_LSN, NULL_LSN, 32, false,
        )
        .unwrap();

        assert_eq!(reader.get_num_read(), 0);
    }

    #[test]
    fn test_file_reader_get_last_entry_size_no_header() {
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![0u8; 64]);
        let start_lsn = Lsn::new(0, 0);
        let reader = FileReader::new(
            mock, true, start_lsn, NULL_LSN, NULL_LSN, 32, false,
        )
        .unwrap();

        assert_eq!(reader.get_last_entry_size(), 0);
    }

    #[test]
    fn test_file_reader_entry_is_replicated_initial() {
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![0u8; 64]);
        let start_lsn = Lsn::new(0, 0);
        let reader = FileReader::new(
            mock, true, start_lsn, NULL_LSN, NULL_LSN, 32, false,
        )
        .unwrap();

        assert!(!reader.entry_is_replicated());
    }

    #[test]
    fn test_file_reader_read_next_entry_eof_no_files() {
        let mock = MockFileAccess::new();
        let mut reader = FileReader::new(
            mock, true, NULL_LSN, NULL_LSN, NULL_LSN, 64, false,
        )
        .unwrap();

        let result = reader.read_next_entry();
        // EOF is set, loop exits immediately returning Ok(false)
        assert!(matches!(result, Ok(false)));
    }

    #[test]
    fn test_file_reader_read_next_entry_small_file() {
        // File smaller than MIN_HEADER_SIZE: should hit EOF/error
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![0u8; 8]); // less than 14 bytes
        let start_lsn = Lsn::new(0, 0);
        let mut reader = FileReader::new(
            mock, true, start_lsn, NULL_LSN, NULL_LSN, 64, false,
        )
        .unwrap();

        // Should return an error because there's not enough data for a full header
        let result = reader.read_next_entry();
        assert!(result.is_err() || matches!(result, Ok(false)));
    }

    #[test]
    fn test_file_reader_read_next_entry_exactly_header_size() {
        // File exactly MIN_HEADER_SIZE: header says item_size=0, so one entry
        let mut mock = MockFileAccess::new();
        // 14 bytes of zeros: entry_type=0, version=0, item_size=0, prev_offset=0
        mock.add_file(0, vec![0u8; LogEntryHeader::MIN_HEADER_SIZE]);
        let start_lsn = Lsn::new(0, 0);
        let mut reader = FileReader::new(
            mock, true, start_lsn, NULL_LSN, NULL_LSN, 64, false,
        )
        .unwrap();

        let result = reader.read_next_entry();
        // Should succeed and read one entry
        assert!(matches!(result, Ok(true)));
        assert_eq!(reader.get_num_read(), 1);
    }

    #[test]
    fn test_file_reader_read_next_entry_multiple() {
        // File with two minimal 14-byte entries
        let mut mock = MockFileAccess::new();
        let data = vec![0u8; LogEntryHeader::MIN_HEADER_SIZE * 2];
        mock.add_file(0, data);
        let start_lsn = Lsn::new(0, 0);
        let mut reader = FileReader::new(
            mock, true, start_lsn, NULL_LSN, NULL_LSN, 64, false,
        )
        .unwrap();

        assert!(matches!(reader.read_next_entry(), Ok(true)));
        assert!(matches!(reader.read_next_entry(), Ok(true)));
        assert_eq!(reader.get_num_read(), 2);
    }

    #[test]
    fn test_file_reader_finish_lsn_stops_reading() {
        let mut mock = MockFileAccess::new();
        let data = vec![0u8; LogEntryHeader::MIN_HEADER_SIZE * 4];
        mock.add_file(0, data);
        let start_lsn = Lsn::new(0, 0);
        // Finish after 1 entry
        let finish_lsn =
            Lsn::new(0, LogEntryHeader::MIN_HEADER_SIZE as u32);
        let mut reader = FileReader::new(
            mock,
            true,
            start_lsn,
            NULL_LSN,
            finish_lsn,
            64,
            false,
        )
        .unwrap();

        // First entry is at offset 0, before finish_lsn, so we read it
        assert!(matches!(reader.read_next_entry(), Ok(true)));
        // Second call: next_entry_offset == finish_lsn, stop
        let result = reader.read_next_entry();
        assert!(result.is_err() || matches!(result, Ok(false)));
    }

    #[test]
    fn test_file_reader_header_methods() {
        let hdr = LogEntryHeader::from_bytes(&[0u8; 14]).unwrap();
        assert_eq!(hdr.header_size, LogEntryHeader::MIN_HEADER_SIZE);
        assert_eq!(hdr.item_size, 0);
        assert_eq!(hdr.entry_size(), LogEntryHeader::MIN_HEADER_SIZE);
        assert!(!hdr.is_variable_length());
        assert_eq!(hdr.variable_portion_size(), 0);
        assert!(!hdr.replicated);
    }

    #[test]
    fn test_file_reader_spans_two_files() {
        // Two files; after exhausting the first, reader moves to second
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![0u8; LogEntryHeader::MIN_HEADER_SIZE]);
        mock.add_file(1, vec![0u8; LogEntryHeader::MIN_HEADER_SIZE]);
        let start_lsn = Lsn::new(0, 0);
        let mut reader = FileReader::new(
            mock, true, start_lsn, NULL_LSN, NULL_LSN, 8, false,
        )
        .unwrap();

        // First entry from file 0
        assert!(matches!(reader.read_next_entry(), Ok(true)));
        // Next attempt fills new buffer; should either read entry in file 1
        // or return eof/error gracefully
        let _ = reader.read_next_entry(); // don't assert, just ensure no panic
    }

    #[test]
    fn test_mock_file_access_read_out_of_bounds() {
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![1, 2, 3]);
        let mut buf = [0u8; 5];
        // Reading past end returns fewer bytes
        let n = mock.read_from_file(0, 1, &mut buf).unwrap();
        assert_eq!(n, 2);
        assert_eq!(buf[0], 2);
        assert_eq!(buf[1], 3);
    }

    #[test]
    fn test_mock_file_access_read_at_end() {
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![1, 2, 3]);
        let mut buf = [0u8; 5];
        let n = mock.read_from_file(0, 3, &mut buf).unwrap();
        assert_eq!(n, 0);
    }

    #[test]
    fn test_mock_file_access_missing_file() {
        let mock = MockFileAccess::new();
        let mut buf = [0u8; 4];
        assert!(mock.read_from_file(99, 0, &mut buf).is_err());
        assert!(mock.get_file_length(99).is_err());
    }

    #[test]
    fn test_mock_file_access_following_file_backward() {
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![0u8; 1]);
        mock.add_file(1, vec![0u8; 1]);
        mock.add_file(2, vec![0u8; 1]);
        assert_eq!(mock.get_following_file_num(2, false), Some(1));
        assert_eq!(mock.get_following_file_num(0, false), None);
    }

    #[test]
    fn test_mock_file_access_file_header_prev_offset() {
        let mock = MockFileAccess::new();
        assert_eq!(mock.get_file_header_prev_offset(0).unwrap(), 0);
    }
}
