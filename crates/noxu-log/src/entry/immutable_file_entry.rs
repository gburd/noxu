//! ImmutableFile marker log entry.
//!
//! Port of `com.sleepycat.je.log.ImmutableFile`.
//!
//! Marks a log file as immutable — no further writes will be appended to it.
//! Written when a log file is closed and transitioned to read-only status.
//! Used during recovery to identify the boundary of active log files.

use byteorder::{BigEndian, ReadBytesExt};
use bytes::{BufMut, BytesMut};
use std::io::{self, Cursor};
use thiserror::Error;

/// Error type for ImmutableFile log entry operations.
#[derive(Debug, Error)]
pub enum ImmutableFileEntryError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
}

/// ImmutableFile marker log entry.
///
/// Marks a specific log file as immutable. Once written, no additional log
/// entries will be appended to the indicated file.
///
/// # Fields
///
/// - `file_number`: The log file number being marked immutable
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImmutableFileEntry {
    /// The log file number being marked immutable.
    pub file_number: u64,
}

impl ImmutableFileEntry {
    /// Creates a new ImmutableFile entry.
    pub fn new(file_number: u64) -> Self {
        Self { file_number }
    }

    /// Returns the serialized size in bytes.
    pub fn log_size(&self) -> usize {
        8 // file_number
    }

    /// Writes this entry to a buffer.
    pub fn write_to_log(&self, buf: &mut BytesMut) {
        buf.put_u64(self.file_number);
    }

    /// Reads an entry from a buffer.
    pub fn read_from_log(buf: &[u8]) -> Result<Self, ImmutableFileEntryError> {
        let mut cursor = Cursor::new(buf);
        let file_number = cursor.read_u64::<BigEndian>()?;
        Ok(Self { file_number })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_immutable_file_roundtrip() {
        let entry = ImmutableFileEntry::new(42);

        let mut buf = BytesMut::new();
        entry.write_to_log(&mut buf);

        let decoded = ImmutableFileEntry::read_from_log(&buf).unwrap();
        assert_eq!(entry, decoded);
        assert_eq!(decoded.file_number, 42);
    }

    #[test]
    fn test_immutable_file_zero() {
        let entry = ImmutableFileEntry::new(0);

        let mut buf = BytesMut::new();
        entry.write_to_log(&mut buf);

        let decoded = ImmutableFileEntry::read_from_log(&buf).unwrap();
        assert_eq!(entry, decoded);
        assert_eq!(decoded.file_number, 0);
    }

    #[test]
    fn test_log_size() {
        let entry = ImmutableFileEntry::new(1);
        assert_eq!(entry.log_size(), 8);
        let mut buf = BytesMut::new();
        entry.write_to_log(&mut buf);
        assert_eq!(buf.len(), entry.log_size());
    }
}
