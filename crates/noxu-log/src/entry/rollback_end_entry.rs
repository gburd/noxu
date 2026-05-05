//! RollbackEnd log entry.
//!
//! Port of `com.sleepycat.je.txn.RollbackEnd`.
//!
//! Written at the end of an HA rollback. Contains a back-pointer to the
//! matching RollbackStart entry so recovery can bracket the rollback period.

use byteorder::{BigEndian, ReadBytesExt};
use bytes::{BufMut, BytesMut};
use noxu_util::lsn::Lsn;
use std::io::{self, Cursor};
use thiserror::Error;

/// Error type for RollbackEnd log entry operations.
#[derive(Debug, Error)]
pub enum RollbackEndEntryError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
}

/// RollbackEnd log entry.
///
/// Marks the completion of an HA rollback. Written after all log entries in
/// the rollback period have been rolled back.
///
/// # Fields
///
/// - `rollback_start_lsn`: LSN of the matching RollbackStart entry
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RollbackEndEntry {
    /// LSN of the matching RollbackStart entry.
    pub rollback_start_lsn: Lsn,
}

impl RollbackEndEntry {
    /// Creates a new RollbackEnd entry.
    pub fn new(rollback_start_lsn: Lsn) -> Self {
        Self { rollback_start_lsn }
    }

    /// Returns the serialized size in bytes.
    pub fn log_size(&self) -> usize {
        8 // rollback_start_lsn
    }

    /// Writes this entry to a buffer.
    pub fn write_to_log(&self, buf: &mut BytesMut) {
        buf.put_u64(self.rollback_start_lsn.as_u64());
    }

    /// Reads an entry from a buffer.
    pub fn read_from_log(buf: &[u8]) -> Result<Self, RollbackEndEntryError> {
        let mut cursor = Cursor::new(buf);
        let rollback_start_lsn = Lsn::from_u64(cursor.read_u64::<BigEndian>()?);
        Ok(Self { rollback_start_lsn })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use noxu_util::lsn::{Lsn, NULL_LSN};

    #[test]
    fn test_rollback_end_roundtrip() {
        let entry = RollbackEndEntry::new(Lsn::new(3, 1200));

        let mut buf = BytesMut::new();
        entry.write_to_log(&mut buf);

        let decoded = RollbackEndEntry::read_from_log(&buf).unwrap();
        assert_eq!(entry, decoded);
        assert_eq!(decoded.rollback_start_lsn, Lsn::new(3, 1200));
    }

    #[test]
    fn test_rollback_end_null_lsn() {
        let entry = RollbackEndEntry::new(NULL_LSN);

        let mut buf = BytesMut::new();
        entry.write_to_log(&mut buf);

        let decoded = RollbackEndEntry::read_from_log(&buf).unwrap();
        assert_eq!(entry, decoded);
    }

    #[test]
    fn test_log_size() {
        let entry = RollbackEndEntry::new(NULL_LSN);
        assert_eq!(entry.log_size(), 8);
        let mut buf = BytesMut::new();
        entry.write_to_log(&mut buf);
        assert_eq!(buf.len(), entry.log_size());
    }
}
