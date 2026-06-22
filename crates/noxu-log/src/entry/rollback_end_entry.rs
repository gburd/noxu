//! RollbackEnd log entry.
//!
//! Port of `com.sleepycat.je.txn.RollbackEnd`.
//!
//! Written at the end of an HA rollback. JE's `RollbackEnd` carries both the
//! matchpoint LSN and a back-pointer to the matching `RollbackStart` entry,
//! plus a debugging timestamp:
//!
//! ```text
//! RollbackEnd {
//!     matchpointLSN    : long,
//!     rollbackStartLSN : long,
//!     time             : Timestamp,
//! }
//! ```
//!
//! Recovery uses `rollbackStartLSN` to bracket the rollback period and
//! `matchpointLSN` (`RollbackEnd.getMatchpoint()`) as the logical truncation
//! point. See `RollbackEnd.java`.
//!
//! # On-disk format
//!
//! HA-only, new-format (`LOG_VERSION = 3`) entry. `matchpoint_lsn` was added
//! in v6.3.0 (REP-1); the previous single-field shape was never produced by a
//! released non-HA build. See CHANGELOG.

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
/// - `matchpoint_lsn`: LSN of the matchpoint (logical truncation point)
///   (`RollbackEnd.matchpointLSN`).
/// - `rollback_start_lsn`: LSN of the matching RollbackStart entry
///   (`RollbackEnd.rollbackStartLSN`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RollbackEndEntry {
    /// LSN of the matchpoint (logical truncation point).
    pub matchpoint_lsn: Lsn,
    /// LSN of the matching RollbackStart entry.
    pub rollback_start_lsn: Lsn,
}

impl RollbackEndEntry {
    /// Creates a new RollbackEnd entry.
    pub fn new(matchpoint_lsn: Lsn, rollback_start_lsn: Lsn) -> Self {
        Self { matchpoint_lsn, rollback_start_lsn }
    }

    /// Returns the serialized size in bytes.
    pub fn log_size(&self) -> usize {
        8 + // matchpoint_lsn
        8 // rollback_start_lsn
    }

    /// Writes this entry to a buffer.
    pub fn write_to_log(&self, buf: &mut BytesMut) {
        buf.put_u64(self.matchpoint_lsn.as_u64());
        buf.put_u64(self.rollback_start_lsn.as_u64());
    }

    /// Reads an entry from a buffer.
    pub fn read_from_log(buf: &[u8]) -> Result<Self, RollbackEndEntryError> {
        let mut cursor = Cursor::new(buf);
        let matchpoint_lsn = Lsn::from_u64(cursor.read_u64::<BigEndian>()?);
        let rollback_start_lsn = Lsn::from_u64(cursor.read_u64::<BigEndian>()?);
        Ok(Self { matchpoint_lsn, rollback_start_lsn })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use noxu_util::lsn::{Lsn, NULL_LSN};

    #[test]
    fn test_rollback_end_roundtrip() {
        let entry = RollbackEndEntry::new(Lsn::new(2, 800), Lsn::new(3, 1200));

        let mut buf = BytesMut::new();
        entry.write_to_log(&mut buf);

        let decoded = RollbackEndEntry::read_from_log(&buf).unwrap();
        assert_eq!(entry, decoded);
        assert_eq!(decoded.matchpoint_lsn, Lsn::new(2, 800));
        assert_eq!(decoded.rollback_start_lsn, Lsn::new(3, 1200));
    }

    #[test]
    fn test_rollback_end_null_lsn() {
        let entry = RollbackEndEntry::new(NULL_LSN, NULL_LSN);

        let mut buf = BytesMut::new();
        entry.write_to_log(&mut buf);

        let decoded = RollbackEndEntry::read_from_log(&buf).unwrap();
        assert_eq!(entry, decoded);
    }

    #[test]
    fn test_log_size() {
        let entry = RollbackEndEntry::new(NULL_LSN, NULL_LSN);
        assert_eq!(entry.log_size(), 16);
        let mut buf = BytesMut::new();
        entry.write_to_log(&mut buf);
        assert_eq!(buf.len(), entry.log_size());
    }
}
