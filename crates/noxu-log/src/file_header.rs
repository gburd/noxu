//! File header for log files.
//!
//! Port of `com.sleepycat.je.log.FileHeader`.
//!
//! Each log file begins with a header containing metadata about the file
//! and a pointer to the last entry in the previous file.

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::io::{self, Read, Write};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::error::{LogError, Result};

/// Current log version number.
///
/// This is the Noxu DB log format version, NOT the JE version.
/// Noxu uses a new, Rust-native log format incompatible with JE.
pub const LOG_VERSION: u32 = 1;

/// Size of the file header on disk (bytes).
pub const FILE_HEADER_SIZE: usize = 8 + 4 + 4 + 4; // timestamp + file_num + prev_offset + version

/// File header written at the beginning of each log file.
///
/// The header contains:
/// - `timestamp`: Unix timestamp (milliseconds) when file was created
/// - `file_number`: The log file number (0-based, sequential)
/// - `last_entry_in_prev_file`: File offset of last entry in previous file (for chaining)
/// - `log_version`: Log format version number
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileHeader {
    /// Unix timestamp in milliseconds.
    pub timestamp: u64,
    /// Log file number.
    pub file_number: u32,
    /// Offset of the last entry in the previous file (0 if this is the first file).
    pub last_entry_in_prev_file: u32,
    /// Log format version.
    pub log_version: u32,
}

impl FileHeader {
    /// Creates a new file header.
    ///
    /// # Arguments
    ///
    /// * `file_number` - The sequential file number (0-based)
    /// * `last_entry_in_prev_file` - Offset of last entry in previous file, or 0
    pub fn new(file_number: u32, last_entry_in_prev_file: u32) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("System time before UNIX epoch")
            .as_millis() as u64;

        FileHeader {
            timestamp,
            file_number,
            last_entry_in_prev_file,
            log_version: LOG_VERSION,
        }
    }

    /// Reads a file header from a reader.
    pub fn read_from<R: Read>(reader: &mut R) -> Result<Self> {
        let timestamp = reader.read_u64::<BigEndian>()?;
        let file_number = reader.read_u32::<BigEndian>()?;
        let last_entry_in_prev_file = reader.read_u32::<BigEndian>()?;
        let log_version = reader.read_u32::<BigEndian>()?;

        Ok(FileHeader {
            timestamp,
            file_number,
            last_entry_in_prev_file,
            log_version,
        })
    }

    /// Writes the file header to a writer.
    pub fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_u64::<BigEndian>(self.timestamp)?;
        writer.write_u32::<BigEndian>(self.file_number)?;
        writer.write_u32::<BigEndian>(self.last_entry_in_prev_file)?;
        writer.write_u32::<BigEndian>(self.log_version)?;
        Ok(())
    }

    /// Validates the header against expected values.
    ///
    /// # Arguments
    ///
    /// * `expected_file_num` - The file number we expect this header to have
    ///
    /// # Returns
    ///
    /// The log version from the header if valid.
    pub fn validate(&self, expected_file_num: u32) -> Result<u32> {
        if self.log_version > LOG_VERSION {
            return Err(LogError::VersionMismatch {
                expected: LOG_VERSION,
                found: self.log_version,
                file_num: self.file_number,
            });
        }

        if self.file_number != expected_file_num {
            return Err(LogError::InvalidHeader {
                file_num: self.file_number,
                message: format!(
                    "Expected file number {expected_file_num:08x}, found {:08x}",
                    self.file_number
                ),
            });
        }

        Ok(self.log_version)
    }

    /// Returns the offset of the last entry in the previous file.
    pub fn last_entry_in_prev_file_offset(&self) -> u32 {
        self.last_entry_in_prev_file
    }

    /// Returns the size of the file header in bytes.
    pub const fn size() -> usize {
        FILE_HEADER_SIZE
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_file_header_roundtrip() {
        let header = FileHeader::new(42, 0x1000);

        let mut buf = Vec::new();
        header.write_to(&mut buf).unwrap();

        assert_eq!(buf.len(), FILE_HEADER_SIZE);

        let mut cursor = Cursor::new(buf);
        let decoded = FileHeader::read_from(&mut cursor).unwrap();

        assert_eq!(decoded.file_number, 42);
        assert_eq!(decoded.last_entry_in_prev_file, 0x1000);
        assert_eq!(decoded.log_version, LOG_VERSION);
        assert!(decoded.timestamp > 0);
    }

    #[test]
    fn test_file_header_validate() {
        let header = FileHeader::new(10, 500);

        // Valid case
        assert!(header.validate(10).is_ok());
        assert_eq!(header.validate(10).unwrap(), LOG_VERSION);

        // Wrong file number
        assert!(header.validate(11).is_err());
    }

    #[test]
    fn test_file_header_size() {
        assert_eq!(FileHeader::size(), FILE_HEADER_SIZE);
        assert_eq!(FILE_HEADER_SIZE, 20);
    }
}
