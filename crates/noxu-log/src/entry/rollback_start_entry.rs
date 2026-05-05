//! RollbackStart log entry.
//!
//! Port of `com.sleepycat.je.txn.RollbackStart`.
//!
//! Written at the start of a replay (HA) rollback to mark the beginning of
//! the recovery period. Contains the LSN of the start of the transaction being
//! rolled back and the matchpoint LSN.

use byteorder::{BigEndian, ReadBytesExt};
use bytes::{BufMut, BytesMut};
use noxu_util::lsn::Lsn;
use std::io::{self, Cursor};
use thiserror::Error;

/// Error type for RollbackStart log entry operations.
#[derive(Debug, Error)]
pub enum RollbackStartEntryError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
}

/// RollbackStart log entry.
///
/// Marks the beginning of an HA rollback. Written before any log entries are
/// rolled back so that recovery can identify the rollback period.
///
/// # Fields
///
/// - `active_txn_start`: LSN of the start of the transaction being rolled back
/// - `matchpoint_lsn`: LSN of the matchpoint that triggered the rollback
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RollbackStartEntry {
    /// LSN of the start of the transaction being rolled back.
    pub active_txn_start: Lsn,
    /// LSN of the matchpoint that triggered the rollback.
    pub matchpoint_lsn: Lsn,
}

impl RollbackStartEntry {
    /// Creates a new RollbackStart entry.
    pub fn new(active_txn_start: Lsn, matchpoint_lsn: Lsn) -> Self {
        Self { active_txn_start, matchpoint_lsn }
    }

    /// Returns the serialized size in bytes.
    pub fn log_size(&self) -> usize {
        8 + // active_txn_start
        8   // matchpoint_lsn
    }

    /// Writes this entry to a buffer.
    pub fn write_to_log(&self, buf: &mut BytesMut) {
        buf.put_u64(self.active_txn_start.as_u64());
        buf.put_u64(self.matchpoint_lsn.as_u64());
    }

    /// Reads an entry from a buffer.
    pub fn read_from_log(buf: &[u8]) -> Result<Self, RollbackStartEntryError> {
        let mut cursor = Cursor::new(buf);
        let active_txn_start = Lsn::from_u64(cursor.read_u64::<BigEndian>()?);
        let matchpoint_lsn = Lsn::from_u64(cursor.read_u64::<BigEndian>()?);
        Ok(Self { active_txn_start, matchpoint_lsn })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use noxu_util::lsn::{Lsn, NULL_LSN};

    #[test]
    fn test_rollback_start_roundtrip() {
        let entry = RollbackStartEntry::new(Lsn::new(3, 1200), Lsn::new(7, 4000));

        let mut buf = BytesMut::new();
        entry.write_to_log(&mut buf);

        let decoded = RollbackStartEntry::read_from_log(&buf).unwrap();
        assert_eq!(entry, decoded);
        assert_eq!(decoded.active_txn_start, Lsn::new(3, 1200));
        assert_eq!(decoded.matchpoint_lsn, Lsn::new(7, 4000));
    }

    #[test]
    fn test_rollback_start_null_lsns() {
        let entry = RollbackStartEntry::new(NULL_LSN, NULL_LSN);

        let mut buf = BytesMut::new();
        entry.write_to_log(&mut buf);

        let decoded = RollbackStartEntry::read_from_log(&buf).unwrap();
        assert_eq!(entry, decoded);
    }

    #[test]
    fn test_log_size() {
        let entry = RollbackStartEntry::new(NULL_LSN, NULL_LSN);
        assert_eq!(entry.log_size(), 16);
        assert_eq!(entry.log_size(), buf_size(&entry));
    }

    fn buf_size(entry: &RollbackStartEntry) -> usize {
        let mut buf = BytesMut::new();
        entry.write_to_log(&mut buf);
        buf.len()
    }
}
