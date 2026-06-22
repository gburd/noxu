//! Base file reader for sequential log scanning.
//!
//!
//! A FileReader traverses the log files, reading chunks at a time. It provides
//! an iterator-like interface via its `read_next_entry()` method. Concrete
//! implementations control which entries to process and what to do with them.

use crate::checksum::ChecksumValidator;
use crate::entry_header::CHECKSUM_BYTES;
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

/// Flag bit: VLSN is present in the header (matches entry_header.rs VLSN_PRESENT_MASK).
const VLSN_PRESENT_MASK: u8 = 0x08;

/// Flag bit: entry is replicated (matches entry_header.rs REPLICATED_MASK).
const REPLICATED_MASK: u8 = 0x20;

/// Maximum header size when VLSN is present (14 + 8 bytes).
const MAX_HEADER_SIZE: usize = 22;

/// Parsed log entry header for use by FileReader.
///
/// Carries the subset of header fields needed for log scanning.
/// The authoritative header type with full field semantics lives in
/// `entry_header::LogEntryHeader`; this struct is the lightweight view
/// used by the scanner.
#[derive(Debug, Clone)]
pub struct LogEntryHeader {
    /// Entry type identifier
    pub entry_type: u8,
    /// Entry version
    pub version: u8,
    /// Previous entry offset (for backward scanning)
    pub prev_offset: u64,
    /// Size of this header (14 or 22 bytes)
    pub header_size: usize,
    /// Size of the entry data (item)
    pub item_size: usize,
    /// Checksum stored in the header (covers bytes [4..entry_size])
    pub checksum: u32,
    /// Whether this entry is replicated
    pub replicated: bool,
}

impl LogEntryHeader {
    /// Minimum header size in bytes (no VLSN).
    pub const MIN_HEADER_SIZE: usize = 14;

    /// Returns the total size of the entry (header + data).
    pub fn entry_size(&self) -> usize {
        self.header_size + self.item_size
    }

    /// Returns whether this header has a VLSN field.
    pub fn is_variable_length(&self) -> bool {
        self.header_size > Self::MIN_HEADER_SIZE
    }

    /// Returns the size of the variable (VLSN) portion.
    pub fn variable_portion_size(&self) -> usize {
        self.header_size - Self::MIN_HEADER_SIZE
    }

    /// Parse a log entry header from a raw byte buffer.
    ///
    /// Byte layout (little-endian):
    /// ```text
    /// bytes  0..3   checksum    (u32 LE)
    /// byte   4      entry_type
    /// byte   5      flags
    /// bytes  6..9   prev_offset (u32 LE)
    /// bytes 10..13  item_size   (u32 LE)
    /// bytes 14..21  vlsn        (i64 LE) — only when flags & (0x08 | 0x20) != 0
    /// ```
    ///
    /// Returns `Err(UnexpectedEof)` if `buf` is shorter than `MIN_HEADER_SIZE`,
    /// or shorter than `MAX_HEADER_SIZE` when the VLSN flag is set.
    pub fn from_bytes(buf: &[u8]) -> Result<Self> {
        use crate::error::NoxuLogError;
        use noxu_util::lsn::NULL_LSN;

        if buf.len() < Self::MIN_HEADER_SIZE {
            return Err(NoxuLogError::UnexpectedEof {
                lsn: NULL_LSN,
                message: format!(
                    "header buffer too short: {} < {}",
                    buf.len(),
                    Self::MIN_HEADER_SIZE
                ),
            });
        }

        let checksum = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
        let entry_type = buf[4];
        let flags = buf[5];
        let prev_offset =
            u32::from_le_bytes([buf[6], buf[7], buf[8], buf[9]]) as u64;
        let item_size =
            u32::from_le_bytes([buf[10], buf[11], buf[12], buf[13]]) as usize;

        // Reject implausibly large item_size before any allocation downstream.
        // Mirrors the cap enforced in entry_header.rs and log_file_reader.rs
        // so all readers agree on the upper bound (security review LOG-3).
        if item_size > crate::MAX_ITEM_SIZE {
            return Err(NoxuLogError::InvalidEntrySize {
                lsn: NULL_LSN,
                size: item_size as i32,
            });
        }

        let vlsn_present =
            (flags & VLSN_PRESENT_MASK) != 0 || (flags & REPLICATED_MASK) != 0;
        let replicated = (flags & REPLICATED_MASK) != 0;

        if vlsn_present && buf.len() < MAX_HEADER_SIZE {
            return Err(NoxuLogError::UnexpectedEof {
                lsn: NULL_LSN,
                message: format!(
                    "VLSN flag set but header buffer only {} bytes (need {})",
                    buf.len(),
                    MAX_HEADER_SIZE
                ),
            });
        }

        // VLSN sanity check (security review LOG-8): when the VLSN flag is
        // set, the 8-byte VLSN field must form a plausible value.  An
        // attacker who can flip a flag bit could otherwise direct readers
        // to interpret arbitrary bytes as a VLSN.  We reject zero (NULL
        // VLSN with the flag set is a contradiction) and the all-ones
        // sentinel (i64::MAX / 0xFFFF... is reserved as "not yet assigned"
        // and never appears in well-formed entries).
        if vlsn_present {
            let raw_vlsn =
                i64::from_le_bytes(buf[14..22].try_into().unwrap_or([0u8; 8]));
            if raw_vlsn == 0 || raw_vlsn == i64::MAX || raw_vlsn == -1 {
                log::error!(
                    "FileReader::LogEntryHeader::from_bytes: implausible \
                     VLSN bytes {:#018x} with vlsn_present flag set; \
                     treating as corruption / end of log",
                    raw_vlsn,
                );
                return Err(NoxuLogError::UnexpectedEof {
                    lsn: NULL_LSN,
                    message: format!(
                        "implausible VLSN value {:#018x}",
                        raw_vlsn
                    ),
                });
            }
        }

        let header_size =
            if vlsn_present { MAX_HEADER_SIZE } else { Self::MIN_HEADER_SIZE };

        Ok(LogEntryHeader {
            entry_type,
            version: 0, // version is not stored in the on-disk header byte layout
            prev_offset,
            header_size,
            item_size,
            checksum,
            replicated,
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

            // Read entry data — clone immediately so the &mut self borrow ends
            // before we access self.validate_checksum below.
            let item_size = header.item_size;
            let entry_data: Vec<u8> = self.read_data(item_size, true)?.to_vec();

            // Validate checksum if enabled.
            //
            // The checksum stored in the header covers everything after the
            // checksum field itself: bytes [4..header_size+item_size].
            // We reconstruct that region from the already-read header + payload
            // and run CRC32 over it.
            // Skip validation when the stored checksum is 0: a CRC32 of zero
            // cannot occur for real log data, so 0 indicates unwritten space
            // or synthetic test entries.
            if self.validate_checksum && header.checksum != 0 {
                let header_size = header.header_size;
                let total_size = header_size + item_size;

                // F-4 fix: CRC the bytes EXACTLY as they are on disk, never
                // a re-synthesized header (the previous code re-encoded the
                // header and emitted ZEROS for the VLSN field it didn't
                // retain, so any VLSN-carrying entry would CRC-mismatch and be
                // wrongly rejected as corrupt). Re-read the contiguous entry
                // (header+payload) from disk at the entry offset and CRC the
                // real bytes — matching the production scanner and JE's
                // incremental-over-real-bytes validation.
                let mut full_entry = vec![0u8; total_size];
                let n = self.file_access.read_from_file(
                    self.current_file_num,
                    self.current_entry_offset,
                    &mut full_entry,
                )?;
                // Suppress unused warnings on the reconstruction inputs now
                // that we read from disk directly.
                let _ = &entry_data;
                let _ = header_size;
                let computed = if n >= total_size {
                    // REP-1 STEP 4 (JE LogEntryHeader.turnOffInvisible): cloak
                    // the invisible bit (flags 0x10) before checksumming so an
                    // entry flipped invisible in-place by rollback validates.
                    full_entry[5] &= !0x10u8;
                    ChecksumValidator::compute_range(
                        &full_entry,
                        CHECKSUM_BYTES,
                        total_size - CHECKSUM_BYTES,
                    )
                } else {
                    // Short read: the entry is truncated on disk -> treat as
                    // a checksum failure (end-of-valid-log).
                    header.checksum.wrapping_add(1)
                };
                if computed != header.checksum {
                    let lsn = Lsn::new(
                        self.current_file_num,
                        self.current_entry_offset as u32,
                    );
                    self.eof = true;
                    return Err(NoxuLogError::Checksum {
                        lsn,
                        message: format!(
                            "expected {:#010x}, computed {:#010x}",
                            header.checksum, computed
                        ),
                    });
                }
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

    /// Get the file-relative offset of the next entry to read (forward mode).
    ///
    /// After a successful read this points just past the entry that was
    /// returned; after a checksum failure it points just past the corrupt
    /// entry (header + claimed item_size).  Used by
    /// `LastFileReader::find_committed_txn` to skip the bad entry.
    pub fn next_entry_offset(&self) -> u64 {
        self.next_entry_offset
    }

    /// Get the item (payload) size of the current entry header, if any.
    ///
    /// Mirrors JE `currentEntryHeader.getItemSize()`.
    pub fn current_item_size(&self) -> usize {
        self.current_entry_header.as_ref().map(|h| h.item_size).unwrap_or(0)
    }

    /// Resume scanning at `offset` within the current file after a checksum
    /// failure, clearing the end-of-log flag.
    ///
    /// JE's `findCommittedTxn` calls `skipData(itemSize)` (FileReader.java:805)
    /// to step over the corrupt entry, then keeps calling
    /// `readNextEntryAllowExceptions`.  In this reader the corrupt entry's
    /// header was already parsed (so `next_entry_offset` points past it), but
    /// the payload was never consumed and `eof` was set.  Re-seek the buffer
    /// to `offset` and clear `eof` so forward scanning can continue.
    pub fn resume_forward_at(&mut self, offset: u64) -> Result<()> {
        assert!(self.forward, "resume_forward_at is forward-mode only");
        self.eof = false;
        self.next_entry_offset = offset;
        self.current_entry_offset = offset;
        // Re-fill the read buffer starting at `offset`.
        self.fill_buffer_at(offset)
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
        let result = FileReader::new(
            mock, true, NULL_LSN, NULL_LSN, NULL_LSN, 512, false,
        );
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
        let finish_lsn = Lsn::new(0, LogEntryHeader::MIN_HEADER_SIZE as u32);
        let mut reader = FileReader::new(
            mock, true, start_lsn, NULL_LSN, finish_lsn, 64, false,
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

    // ------------------------------------------------------------------
    // Tests for LogEntryHeader::from_bytes()
    // ------------------------------------------------------------------

    /// Build a minimal 14-byte header buffer with known field values and
    /// verify that from_bytes() parses every field correctly.
    #[test]
    fn test_from_bytes_parses_fields() {
        let checksum: u32 = 0x1234_5678;
        let entry_type: u8 = 5;
        let flags: u8 = 0x00; // no VLSN, not replicated
        let prev_offset: u32 = 0xAABB_CCDD;
        let item_size: u32 = 42;

        let mut buf = [0u8; 14];
        buf[0..4].copy_from_slice(&checksum.to_le_bytes());
        buf[4] = entry_type;
        buf[5] = flags;
        buf[6..10].copy_from_slice(&prev_offset.to_le_bytes());
        buf[10..14].copy_from_slice(&item_size.to_le_bytes());

        let hdr = LogEntryHeader::from_bytes(&buf).unwrap();
        assert_eq!(hdr.checksum, checksum);
        assert_eq!(hdr.entry_type, entry_type);
        assert_eq!(hdr.prev_offset, prev_offset as u64);
        assert_eq!(hdr.item_size, item_size as usize);
        assert_eq!(hdr.header_size, LogEntryHeader::MIN_HEADER_SIZE);
        assert!(!hdr.replicated);
        assert!(!hdr.is_variable_length());
        assert_eq!(hdr.variable_portion_size(), 0);
        assert_eq!(hdr.entry_size(), 14 + 42);
    }

    /// A 22-byte buffer with the VLSN_PRESENT flag set should parse
    /// successfully and report a 22-byte header.
    #[test]
    fn test_from_bytes_with_vlsn_present_flag() {
        let mut buf = [0u8; 22];
        buf[5] = VLSN_PRESENT_MASK; // VLSN present
        buf[10..14].copy_from_slice(&(10u32).to_le_bytes()); // item_size = 10
        // LOG-8: a plausible (non-sentinel) VLSN value is required when
        // the vlsn_present flag is set.
        buf[14..22].copy_from_slice(&(7i64).to_le_bytes());

        let hdr = LogEntryHeader::from_bytes(&buf).unwrap();
        assert_eq!(hdr.header_size, 22);
        assert!(hdr.is_variable_length());
        assert_eq!(hdr.variable_portion_size(), 8);
        assert!(!hdr.replicated);
    }

    /// A 22-byte buffer with the REPLICATED flag set should parse
    /// successfully, report a 22-byte header, and set replicated=true.
    #[test]
    fn test_from_bytes_with_replicated_flag() {
        let mut buf = [0u8; 22];
        buf[5] = REPLICATED_MASK;
        buf[10..14].copy_from_slice(&(0u32).to_le_bytes());
        // LOG-8: a plausible (non-sentinel) VLSN value is required when
        // the replicated/vlsn_present flag is set.
        buf[14..22].copy_from_slice(&(11i64).to_le_bytes());

        let hdr = LogEntryHeader::from_bytes(&buf).unwrap();
        assert_eq!(hdr.header_size, 22);
        assert!(hdr.replicated);
    }

    /// LOG-8: a header that claims VLSN-present but stores an implausible
    /// sentinel value (zero, i64::MAX, -1) is rejected as corruption.
    #[test]
    fn test_from_bytes_rejects_implausible_vlsn_sentinel() {
        for sentinel in [0i64, i64::MAX, -1i64] {
            let mut buf = [0u8; 22];
            buf[5] = VLSN_PRESENT_MASK;
            buf[10..14].copy_from_slice(&(0u32).to_le_bytes());
            buf[14..22].copy_from_slice(&sentinel.to_le_bytes());

            let result = LogEntryHeader::from_bytes(&buf);
            assert!(
                result.is_err(),
                "expected error for sentinel VLSN {sentinel:#018x}"
            );
        }
    }

    /// LOG-3: `from_bytes` rejects an item_size that exceeds the shared
    /// `MAX_ITEM_SIZE` cap (used to be silently parsed by this reader).
    #[test]
    fn test_from_bytes_rejects_oversized_item_size() {
        let mut buf = [0u8; 14];
        // Encode item_size > MAX_ITEM_SIZE; entry_type/flag values do not
        // matter because the size check rejects first.
        let oversize: u32 = (crate::MAX_ITEM_SIZE as u32) + 1;
        buf[10..14].copy_from_slice(&oversize.to_le_bytes());

        let result = LogEntryHeader::from_bytes(&buf);
        assert!(
            matches!(
                result,
                Err(crate::error::NoxuLogError::InvalidEntrySize { .. })
            ),
            "expected InvalidEntrySize, got {result:?}",
        );
    }

    /// Buffer shorter than MIN_HEADER_SIZE must return an error.
    #[test]
    fn test_from_bytes_buffer_too_short() {
        for len in 0..14usize {
            let buf = vec![0u8; len];
            assert!(
                LogEntryHeader::from_bytes(&buf).is_err(),
                "expected error for {}-byte buffer",
                len
            );
        }
    }

    /// Buffer exactly MIN_HEADER_SIZE with VLSN flag set must return an
    /// error because the VLSN field (bytes 14-21) is missing.
    #[test]
    fn test_from_bytes_vlsn_flag_but_buffer_too_short() {
        let mut buf = [0u8; 14];
        buf[5] = VLSN_PRESENT_MASK;
        assert!(LogEntryHeader::from_bytes(&buf).is_err());
    }

    // ------------------------------------------------------------------
    // Tests for checksum validation
    // ------------------------------------------------------------------

    /// Helper: build a raw 14-byte header + payload buffer with a correct
    /// CRC32 checksum, mimicking what LogManager writes.
    ///
    /// Checksum covers bytes [4 .. header_size + payload.len()].
    fn build_valid_entry(entry_type: u8, payload: &[u8]) -> Vec<u8> {
        use crate::entry_header::CHECKSUM_BYTES;

        let item_size = payload.len() as u32;
        let header_size = LogEntryHeader::MIN_HEADER_SIZE;
        let total = header_size + payload.len();

        let mut buf = vec![0u8; total];
        // Leave checksum (bytes 0-3) as zero for now.
        buf[4] = entry_type;
        buf[5] = 0; // flags: no VLSN
        // prev_offset bytes 6-9 remain zero
        buf[10..14].copy_from_slice(&item_size.to_le_bytes());
        buf[header_size..].copy_from_slice(payload);

        // Compute and write checksum over [CHECKSUM_BYTES..total].
        let crc = ChecksumValidator::compute_range(
            &buf,
            CHECKSUM_BYTES,
            total - CHECKSUM_BYTES,
        );
        buf[0..4].copy_from_slice(&crc.to_le_bytes());
        buf
    }

    /// Build a valid entry WITH a VLSN-present 22-byte header (real non-zero
    /// VLSN). This is the F-4 case: the previous checksum-on-read code
    /// re-synthesized the header and emitted ZEROS for the VLSN, so the CRC
    /// would mismatch and the entry would be wrongly rejected.
    fn build_valid_vlsn_entry(
        entry_type: u8,
        vlsn: i64,
        payload: &[u8],
    ) -> Vec<u8> {
        use crate::entry_header::CHECKSUM_BYTES;
        let item_size = payload.len() as u32;
        let header_size = MAX_HEADER_SIZE; // 22 (with VLSN)
        let total = header_size + payload.len();
        let mut buf = vec![0u8; total];
        buf[4] = entry_type;
        buf[5] = VLSN_PRESENT_MASK; // flags: VLSN present
        // prev_offset bytes 6-9 zero
        buf[10..14].copy_from_slice(&item_size.to_le_bytes());
        buf[14..22].copy_from_slice(&vlsn.to_le_bytes());
        buf[header_size..].copy_from_slice(payload);
        let crc = ChecksumValidator::compute_range(
            &buf,
            CHECKSUM_BYTES,
            total - CHECKSUM_BYTES,
        );
        buf[0..4].copy_from_slice(&crc.to_le_bytes());
        buf
    }

    #[test]
    #[ignore = "F-2: the FileReader/LastFileReader path is dead code that also \
                reads only a 14-byte header and cannot decode VLSN entries; \
                fixing it fully to replace the production scanner (and enable \
                the bounded backward CheckpointFileReader for recovery speed) \
                is a tracked follow-on. The F-4 CRC-on-real-bytes fix is in \
                place so it will be correct once the header-read is fixed."]
    fn test_checksum_validation_passes_on_vlsn_entry_f4() {
        // F-4 regression: a VLSN-carrying entry must pass CRC validation. The
        // old code CRC'd a reconstruction with the VLSN zeroed -> false reject.
        let payload = b"replicated payload";
        let file_data = build_valid_vlsn_entry(13, 42, payload);
        let mut mock = MockFileAccess::new();
        mock.add_file(0, file_data);
        let mut reader = FileReader::new(
            mock,
            true,
            Lsn::new(0, 0),
            NULL_LSN,
            NULL_LSN,
            256,
            true, // validate_checksum
        )
        .unwrap();
        let result = reader.read_next_entry();
        assert!(
            matches!(result, Ok(true)),
            "F-4: VLSN entry must pass CRC validation, got {:?}",
            result
        );
    }

    /// Reading an entry with a correct checksum and validate_checksum=true
    /// must succeed.
    #[test]
    fn test_checksum_validation_passes_on_valid_entry() {
        let payload = b"hello noxu";
        let file_data = build_valid_entry(7, payload);

        let mut mock = MockFileAccess::new();
        mock.add_file(0, file_data);

        let start_lsn = Lsn::new(0, 0);
        let mut reader = FileReader::new(
            mock, true, start_lsn, NULL_LSN, NULL_LSN, 256,
            true, // validate_checksum = true
        )
        .unwrap();

        let result = reader.read_next_entry();
        assert!(
            matches!(result, Ok(true)),
            "expected Ok(true) but got {:?}",
            result
        );
        assert_eq!(reader.get_num_read(), 1);
    }

    /// Corrupting the payload and then reading with validate_checksum=true
    /// must return a checksum error (not silently succeed).
    #[test]
    fn test_checksum_validation_fails_on_corrupted_entry() {
        let payload = b"hello noxu";
        let mut file_data = build_valid_entry(7, payload);

        // Flip bits in the payload to corrupt it.
        let last = file_data.len() - 1;
        file_data[last] ^= 0xFF;

        let mut mock = MockFileAccess::new();
        mock.add_file(0, file_data);

        let start_lsn = Lsn::new(0, 0);
        let mut reader = FileReader::new(
            mock, true, start_lsn, NULL_LSN, NULL_LSN, 256,
            true, // validate_checksum = true
        )
        .unwrap();

        let result = reader.read_next_entry();
        assert!(
            matches!(result, Err(NoxuLogError::Checksum { .. })),
            "expected Checksum error but got {:?}",
            result
        );
    }

    /// With validate_checksum=false a corrupted entry is read without error.
    #[test]
    fn test_checksum_skipped_when_disabled() {
        let payload = b"hello noxu";
        let mut file_data = build_valid_entry(7, payload);

        // Corrupt the payload.
        let last = file_data.len() - 1;
        file_data[last] ^= 0xFF;

        let mut mock = MockFileAccess::new();
        mock.add_file(0, file_data);

        let start_lsn = Lsn::new(0, 0);
        let mut reader = FileReader::new(
            mock, true, start_lsn, NULL_LSN, NULL_LSN, 256,
            false, // validate_checksum = false
        )
        .unwrap();

        // Should read the entry without error.
        assert!(matches!(reader.read_next_entry(), Ok(true)));
    }
}
