//! File header entry for log files.
//!
//!
//! Each log file begins with a FileHeader that identifies the file number,
//! log version, and the LSN of the last entry in the previous file.

use byteorder::{BigEndian, ReadBytesExt};
use bytes::{BufMut, BytesMut};
use crate::util::lsn::Lsn;
use std::io::{self, Cursor};
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;

/// Error type for file header operations.
#[derive(Debug, Error)]
pub enum FileHeaderError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("Invalid log version: expected <= {expected}, got {actual}")]
    InvalidLogVersion { expected: u32, actual: u32 },
    #[error("Wrong file number: expected {expected}, got {actual}")]
    WrongFileNumber { expected: u64, actual: u64 },
}

/// File header information for a log file.
///
/// This appears as the first entry in every log file and contains metadata
/// about the file's position in the log sequence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileHeader {
    /// File number (0-indexed, unsigned 32-bit value stored as u64).
    pub file_num: u64,
    /// LSN of the last entry in the previous file.
    pub last_entry_in_prev_file: Lsn,
    /// Timestamp when this file was created (milliseconds since epoch).
    pub timestamp: u64,
    /// Log format version.
    pub log_version: u32,
}

impl FileHeader {
    /// Creates a new file header.
    pub fn new(
        file_num: u64,
        last_entry_in_prev_file: Lsn,
        log_version: u32,
    ) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        Self { file_num, last_entry_in_prev_file, timestamp, log_version }
    }

    /// Returns the serialized size of a file header (fixed size).
    pub const fn log_size() -> usize {
        8 + // timestamp (i64)
        4 + // file_num (u32)
        8 + // last_entry_in_prev_file (Lsn as u64)
        4 // log_version (u32)
    }

    /// Writes this file header to a buffer.
    pub fn write_to_log(&self, buf: &mut BytesMut) {
        buf.put_i64(self.timestamp as i64);
        buf.put_u32(self.file_num as u32);
        buf.put_u64(self.last_entry_in_prev_file.as_u64());
        buf.put_u32(self.log_version);
    }

    /// Reads a file header from a buffer.
    pub fn read_from_log(buf: &[u8]) -> Result<Self, FileHeaderError> {
        let mut cursor = Cursor::new(buf);

        let timestamp = cursor.read_i64::<BigEndian>()? as u64;
        let file_num = cursor.read_u32::<BigEndian>()? as u64;
        let last_entry_lsn_raw = cursor.read_u64::<BigEndian>()?;
        let last_entry_in_prev_file = Lsn::from_u64(last_entry_lsn_raw);
        let log_version = cursor.read_u32::<BigEndian>()?;

        Ok(Self { file_num, last_entry_in_prev_file, timestamp, log_version })
    }

    /// Validates this file header.
    ///
    /// Checks that the file number matches the expected value and that the
    /// log version is not newer than the current version.
    pub fn validate(
        &self,
        expected_file_num: u64,
        current_log_version: u32,
    ) -> Result<(), FileHeaderError> {
        if self.log_version > current_log_version {
            return Err(FileHeaderError::InvalidLogVersion {
                expected: current_log_version,
                actual: self.log_version,
            });
        }

        if self.file_num != expected_file_num {
            return Err(FileHeaderError::WrongFileNumber {
                expected: expected_file_num,
                actual: self.file_num,
            });
        }

        Ok(())
    }
}

/// Log entry wrapper for FileHeader.
///
/// This wraps the FileHeader loggable for insertion into the log entry system.
#[derive(Debug, Clone)]
pub struct FileHeaderEntry {
    pub header: FileHeader,
}

impl FileHeaderEntry {
    /// Creates a new file header entry.
    pub fn new(
        file_num: u64,
        last_entry_in_prev_file: Lsn,
        log_version: u32,
    ) -> Self {
        Self {
            header: FileHeader::new(
                file_num,
                last_entry_in_prev_file,
                log_version,
            ),
        }
    }

    /// Returns the serialized size.
    pub fn log_size(&self) -> usize {
        FileHeader::log_size()
    }

    /// Writes this entry to a buffer.
    pub fn write_to_log(&self, buf: &mut BytesMut) {
        self.header.write_to_log(buf);
    }

    /// Reads an entry from a buffer.
    pub fn read_from_log(buf: &[u8]) -> Result<Self, FileHeaderError> {
        Ok(Self { header: FileHeader::read_from_log(buf)? })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::NULL_LSN;

    #[test]
    fn test_file_header_roundtrip() {
        let header = FileHeader::new(42, Lsn::new(10, 5000), 1);

        let mut buf = BytesMut::new();
        header.write_to_log(&mut buf);

        let decoded = FileHeader::read_from_log(&buf).unwrap();
        assert_eq!(header.file_num, decoded.file_num);
        assert_eq!(
            header.last_entry_in_prev_file,
            decoded.last_entry_in_prev_file
        );
        assert_eq!(header.log_version, decoded.log_version);
        assert_eq!(header.timestamp, decoded.timestamp);
    }

    #[test]
    fn test_file_header_entry_roundtrip() {
        let entry = FileHeaderEntry::new(0, NULL_LSN, 1);

        let mut buf = BytesMut::new();
        entry.write_to_log(&mut buf);

        let decoded = FileHeaderEntry::read_from_log(&buf).unwrap();
        assert_eq!(entry.header.file_num, decoded.header.file_num);
        assert_eq!(entry.header.log_version, decoded.header.log_version);
    }

    #[test]
    fn test_validate_success() {
        let header = FileHeader::new(5, NULL_LSN, 1);
        assert!(header.validate(5, 1).is_ok());
        assert!(header.validate(5, 2).is_ok()); // Current version higher is OK
    }

    #[test]
    fn test_validate_wrong_file_number() {
        let header = FileHeader::new(5, NULL_LSN, 1);
        let result = header.validate(6, 1);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_version_too_new() {
        let header = FileHeader::new(5, NULL_LSN, 10);
        let result = header.validate(5, 9);
        assert!(result.is_err());
    }

    #[test]
    fn test_log_size() {
        assert_eq!(FileHeader::log_size(), 24);
    }
}
