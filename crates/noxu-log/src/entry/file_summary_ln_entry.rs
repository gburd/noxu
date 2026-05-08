//! FileSummaryLN log entry.
//!
//!
//! Records per-file utilization statistics used by the cleaner to determine
//! which log files are candidates for cleaning. Written periodically by the
//! cleaner and during checkpoints.

use byteorder::{BigEndian, ReadBytesExt};
use bytes::{BufMut, BytesMut};
use std::io::{self, Cursor};
use thiserror::Error;

/// Error type for FileSummaryLN log entry operations.
#[derive(Debug, Error)]
pub enum FileSummaryLnEntryError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
}

/// FileSummaryLN log entry.
///
/// Stores utilization statistics for a single log file. The cleaner reads
/// these entries to determine file utilization and select files for cleaning.
///
/// # Fields
///
/// - `file_number`: Log file number these statistics apply to
/// - `total_count`: Total number of log entries in the file
/// - `total_size`: Total byte size of all log entries in the file
/// - `obsolete_count`: Number of obsolete log entries
/// - `obsolete_size`: Total byte size of obsolete log entries
/// - `obsolete_size_counted`: Whether obsolete sizes have been fully accounted
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileSummaryLnEntry {
    /// Log file number these statistics apply to.
    pub file_number: u64,
    /// Total number of log entries in the file.
    pub total_count: i64,
    /// Total byte size of all log entries in the file.
    pub total_size: i64,
    /// Number of obsolete log entries.
    pub obsolete_count: i64,
    /// Total byte size of obsolete log entries.
    pub obsolete_size: i64,
    /// Whether obsolete sizes have been fully accounted for.
    pub obsolete_size_counted: bool,
}

impl FileSummaryLnEntry {
    /// Creates a new FileSummaryLN entry.
    pub fn new(
        file_number: u64,
        total_count: i64,
        total_size: i64,
        obsolete_count: i64,
        obsolete_size: i64,
        obsolete_size_counted: bool,
    ) -> Self {
        Self {
            file_number,
            total_count,
            total_size,
            obsolete_count,
            obsolete_size,
            obsolete_size_counted,
        }
    }

    /// Returns the serialized size in bytes.
    pub fn log_size(&self) -> usize {
        8 + // file_number
        8 + // total_count
        8 + // total_size
        8 + // obsolete_count
        8 + // obsolete_size
        1   // obsolete_size_counted (bool as u8)
    }

    /// Writes this entry to a buffer.
    pub fn write_to_log(&self, buf: &mut BytesMut) {
        buf.put_u64(self.file_number);
        buf.put_i64(self.total_count);
        buf.put_i64(self.total_size);
        buf.put_i64(self.obsolete_count);
        buf.put_i64(self.obsolete_size);
        buf.put_u8(if self.obsolete_size_counted { 1 } else { 0 });
    }

    /// Reads an entry from a buffer.
    pub fn read_from_log(buf: &[u8]) -> Result<Self, FileSummaryLnEntryError> {
        let mut cursor = Cursor::new(buf);
        let file_number = cursor.read_u64::<BigEndian>()?;
        let total_count = cursor.read_i64::<BigEndian>()?;
        let total_size = cursor.read_i64::<BigEndian>()?;
        let obsolete_count = cursor.read_i64::<BigEndian>()?;
        let obsolete_size = cursor.read_i64::<BigEndian>()?;
        let obsolete_size_counted = cursor.read_u8()? != 0;
        Ok(Self {
            file_number,
            total_count,
            total_size,
            obsolete_count,
            obsolete_size,
            obsolete_size_counted,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_file_summary_ln_roundtrip() {
        let entry = FileSummaryLnEntry::new(5, 1000, 512000, 200, 102400, true);

        let mut buf = BytesMut::new();
        entry.write_to_log(&mut buf);

        let decoded = FileSummaryLnEntry::read_from_log(&buf).unwrap();
        assert_eq!(entry, decoded);
        assert_eq!(decoded.file_number, 5);
        assert_eq!(decoded.total_count, 1000);
        assert_eq!(decoded.total_size, 512000);
        assert_eq!(decoded.obsolete_count, 200);
        assert_eq!(decoded.obsolete_size, 102400);
        assert!(decoded.obsolete_size_counted);
    }

    #[test]
    fn test_file_summary_ln_not_counted() {
        let entry = FileSummaryLnEntry::new(0, 0, 0, 0, 0, false);

        let mut buf = BytesMut::new();
        entry.write_to_log(&mut buf);

        let decoded = FileSummaryLnEntry::read_from_log(&buf).unwrap();
        assert_eq!(entry, decoded);
        assert!(!decoded.obsolete_size_counted);
    }

    #[test]
    fn test_log_size() {
        let entry = FileSummaryLnEntry::new(1, 10, 100, 5, 50, false);
        assert_eq!(entry.log_size(), 8 + 8 + 8 + 8 + 8 + 1);
        let mut buf = BytesMut::new();
        entry.write_to_log(&mut buf);
        assert_eq!(buf.len(), entry.log_size());
    }
}
