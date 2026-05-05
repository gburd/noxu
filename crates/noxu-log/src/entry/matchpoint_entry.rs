//! Matchpoint log entry.
//!
//! Port of `com.sleepycat.je.rep.impl.node.Matchpoint`.
//!
//! Written by the HA (High Availability) layer as a named synchronization
//! point in the replication stream. Carries both a physical LSN and a virtual
//! VLSN so that replicas can find a common synchronization position.

use byteorder::{BigEndian, ReadBytesExt};
use bytes::{BufMut, BytesMut};
use noxu_util::{lsn::Lsn, vlsn::Vlsn};
use std::io::{self, Cursor};
use thiserror::Error;

/// Error type for Matchpoint log entry operations.
#[derive(Debug, Error)]
pub enum MatchpointEntryError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
}

/// Matchpoint log entry.
///
/// A named synchronization point in the replication stream. Replicas use
/// matchpoints to find a common position for synchronization and log replay.
///
/// # Fields
///
/// - `lsn`: Physical LSN of this matchpoint
/// - `vlsn`: Virtual LSN (VLSN) assigned to this matchpoint
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchpointEntry {
    /// Physical LSN of this matchpoint.
    pub lsn: Lsn,
    /// Virtual LSN assigned to this matchpoint.
    pub vlsn: Vlsn,
}

impl MatchpointEntry {
    /// Creates a new Matchpoint entry.
    pub fn new(lsn: Lsn, vlsn: Vlsn) -> Self {
        Self { lsn, vlsn }
    }

    /// Returns the serialized size in bytes.
    pub fn log_size(&self) -> usize {
        8 + // lsn
        8   // vlsn (i64 sequence number)
    }

    /// Writes this entry to a buffer.
    pub fn write_to_log(&self, buf: &mut BytesMut) {
        buf.put_u64(self.lsn.as_u64());
        buf.put_i64(self.vlsn.sequence());
    }

    /// Reads an entry from a buffer.
    pub fn read_from_log(buf: &[u8]) -> Result<Self, MatchpointEntryError> {
        let mut cursor = Cursor::new(buf);
        let lsn = Lsn::from_u64(cursor.read_u64::<BigEndian>()?);
        let vlsn_seq = cursor.read_i64::<BigEndian>()?;
        let vlsn = Vlsn::new(vlsn_seq);
        Ok(Self { lsn, vlsn })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use noxu_util::{lsn::NULL_LSN, vlsn::NULL_VLSN};

    #[test]
    fn test_matchpoint_roundtrip() {
        let entry = MatchpointEntry::new(Lsn::new(10, 5000), Vlsn::new(99));

        let mut buf = BytesMut::new();
        entry.write_to_log(&mut buf);

        let decoded = MatchpointEntry::read_from_log(&buf).unwrap();
        assert_eq!(entry, decoded);
        assert_eq!(decoded.lsn, Lsn::new(10, 5000));
        assert_eq!(decoded.vlsn, Vlsn::new(99));
    }

    #[test]
    fn test_matchpoint_null_values() {
        let entry = MatchpointEntry::new(NULL_LSN, NULL_VLSN);

        let mut buf = BytesMut::new();
        entry.write_to_log(&mut buf);

        let decoded = MatchpointEntry::read_from_log(&buf).unwrap();
        assert_eq!(entry, decoded);
    }

    #[test]
    fn test_log_size() {
        let entry = MatchpointEntry::new(NULL_LSN, NULL_VLSN);
        assert_eq!(entry.log_size(), 16);
        let mut buf = BytesMut::new();
        entry.write_to_log(&mut buf);
        assert_eq!(buf.len(), entry.log_size());
    }
}
