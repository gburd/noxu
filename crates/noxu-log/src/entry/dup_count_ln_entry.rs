//! DupCountLN log entry.
//!
//! Port of `com.sleepycat.je.log.entry.DupCountLNLogEntry`.
//!
//! Contains a duplicate count value stored in old-format duplicate databases.
//! The count tracks the total number of duplicate records for a given key.
//! Written as part of the duplicate sub-tree structure in legacy log formats.

use byteorder::{BigEndian, ReadBytesExt};
use bytes::{BufMut, BytesMut};
use std::io::{self, Cursor};
use thiserror::Error;

/// Error type for DupCountLN log entry operations.
#[derive(Debug, Error)]
pub enum DupCountLnEntryError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
}

/// DupCountLN log entry.
///
/// Stores the duplicate record count for a key in old-format duplicate
/// databases. Used during recovery of legacy log files.
///
/// # Fields
///
/// - `dup_count`: Total number of duplicate records for the associated key
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DupCountLnEntry {
    /// Total number of duplicate records for this key.
    pub dup_count: i32,
}

impl DupCountLnEntry {
    /// Creates a new DupCountLN entry.
    pub fn new(dup_count: i32) -> Self {
        Self { dup_count }
    }

    /// Returns the serialized size in bytes.
    pub fn log_size(&self) -> usize {
        4 // dup_count (i32)
    }

    /// Writes this entry to a buffer.
    pub fn write_to_log(&self, buf: &mut BytesMut) {
        buf.put_i32(self.dup_count);
    }

    /// Reads an entry from a buffer.
    pub fn read_from_log(buf: &[u8]) -> Result<Self, DupCountLnEntryError> {
        let mut cursor = Cursor::new(buf);
        let dup_count = cursor.read_i32::<BigEndian>()?;
        Ok(Self { dup_count })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dup_count_ln_roundtrip() {
        let entry = DupCountLnEntry::new(42);

        let mut buf = BytesMut::new();
        entry.write_to_log(&mut buf);

        let decoded = DupCountLnEntry::read_from_log(&buf).unwrap();
        assert_eq!(entry, decoded);
        assert_eq!(decoded.dup_count, 42);
    }

    #[test]
    fn test_dup_count_ln_zero() {
        let entry = DupCountLnEntry::new(0);

        let mut buf = BytesMut::new();
        entry.write_to_log(&mut buf);

        let decoded = DupCountLnEntry::read_from_log(&buf).unwrap();
        assert_eq!(entry, decoded);
        assert_eq!(decoded.dup_count, 0);
    }

    #[test]
    fn test_dup_count_ln_negative() {
        let entry = DupCountLnEntry::new(-1);

        let mut buf = BytesMut::new();
        entry.write_to_log(&mut buf);

        let decoded = DupCountLnEntry::read_from_log(&buf).unwrap();
        assert_eq!(entry, decoded);
        assert_eq!(decoded.dup_count, -1);
    }

    #[test]
    fn test_log_size() {
        let entry = DupCountLnEntry::new(100);
        assert_eq!(entry.log_size(), 4);
        let mut buf = BytesMut::new();
        entry.write_to_log(&mut buf);
        assert_eq!(buf.len(), entry.log_size());
    }
}
