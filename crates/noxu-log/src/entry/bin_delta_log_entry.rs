//! BIN Delta log entry.
//!
//! Port of `com.sleepycat.je.log.entry.BINDeltaLogEntry`.
//!
//! BINDeltaLogEntry represents a partial BIN (Bottom Internal Node) that
//! contains only the slots that have changed since the last full BIN or
//! delta was logged. This is a space optimization for large BINs.

use byteorder::{BigEndian, ReadBytesExt};
use bytes::{BufMut, BytesMut};
use noxu_util::lsn::Lsn;
use std::io::{self, Cursor};
use thiserror::Error;

/// Error type for BIN delta log entry operations.
#[derive(Debug, Error)]
pub enum BinDeltaLogEntryError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
}

/// BIN Delta log entry.
///
/// Represents a partial BIN containing only the slots that have been modified
/// (the "delta"). Unlike a full BIN log entry, a delta saves space by only
/// logging the changed portions of the BIN.
///
/// # Fields
///
/// - `db_id`: Database ID
/// - `prev_full_lsn`: LSN of the previous full BIN version
/// - `prev_delta_lsn`: LSN of the previous delta (NULL_LSN if prev was full)
/// - `delta_data`: Serialized BIN delta data (only modified slots)
///
/// NOTE: Since tree types (BIN) aren't implemented yet, we use Vec<u8>
/// as placeholder for serialized delta data.
#[derive(Debug, Clone)]
pub struct BinDeltaLogEntry {
    /// Database ID.
    pub db_id: u64,
    /// LSN of previous full BIN version.
    pub prev_full_lsn: Lsn,
    /// LSN of previous delta version (NULL_LSN if previous was full).
    pub prev_delta_lsn: Lsn,
    /// Serialized BIN delta data (dirty slots only).
    ///
    /// Carries the actual delta serialization produced by `BinStub::serialize_delta()`.
    /// The format is: node_id(u64BE) | num_dirty(u32BE) | per-slot (slot_idx + entry data).
    /// Deserialization is performed by `BinStub::deserialize_delta()` during recovery.
    pub delta_data: Vec<u8>,
}

impl BinDeltaLogEntry {
    /// Creates a new BIN delta log entry.
    pub fn new(
        db_id: u64,
        prev_full_lsn: Lsn,
        prev_delta_lsn: Lsn,
        delta_data: Vec<u8>,
    ) -> Self {
        Self { db_id, prev_full_lsn, prev_delta_lsn, delta_data }
    }

    /// Returns true (this is a BIN delta entry).
    pub fn is_bin_delta(&self) -> bool {
        true
    }

    /// Returns the serialized size in bytes.
    pub fn log_size(&self) -> usize {
        8 + // db_id
        8 + // prev_full_lsn
        8 + // prev_delta_lsn
        4 + self.delta_data.len() // delta_data
    }

    /// Writes this entry to a buffer.
    pub fn write_to_log(&self, buf: &mut BytesMut) {
        buf.put_u64(self.db_id);
        buf.put_u64(self.prev_full_lsn.as_u64());
        buf.put_u64(self.prev_delta_lsn.as_u64());
        buf.put_u32(self.delta_data.len() as u32);
        buf.extend_from_slice(&self.delta_data);
    }

    /// Reads an entry from a buffer.
    pub fn read_from_log(buf: &[u8]) -> Result<Self, BinDeltaLogEntryError> {
        let mut cursor = Cursor::new(buf);

        let db_id = cursor.read_u64::<BigEndian>()?;
        let prev_full_lsn = Lsn::from_u64(cursor.read_u64::<BigEndian>()?);
        let prev_delta_lsn = Lsn::from_u64(cursor.read_u64::<BigEndian>()?);

        let delta_len = cursor.read_u32::<BigEndian>()? as usize;
        let mut delta_data = vec![0u8; delta_len];
        io::Read::read_exact(&mut cursor, &mut delta_data)?;

        Ok(Self { db_id, prev_full_lsn, prev_delta_lsn, delta_data })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use noxu_util::NULL_LSN;

    #[test]
    fn test_bin_delta_log_entry_roundtrip() {
        let delta_data = b"fake_serialized_BIN_delta".to_vec();
        let entry = BinDeltaLogEntry::new(
            99,
            Lsn::new(5, 2000),
            Lsn::new(6, 3000),
            delta_data,
        );

        let mut buf = BytesMut::new();
        entry.write_to_log(&mut buf);

        let decoded = BinDeltaLogEntry::read_from_log(&buf).unwrap();
        assert_eq!(entry.db_id, decoded.db_id);
        assert_eq!(entry.prev_full_lsn, decoded.prev_full_lsn);
        assert_eq!(entry.prev_delta_lsn, decoded.prev_delta_lsn);
        assert_eq!(entry.delta_data, decoded.delta_data);
    }

    #[test]
    fn test_is_bin_delta() {
        let entry = BinDeltaLogEntry::new(1, NULL_LSN, NULL_LSN, vec![]);
        assert!(entry.is_bin_delta());
    }

    #[test]
    fn test_log_size() {
        let entry = BinDeltaLogEntry::new(1, NULL_LSN, NULL_LSN, vec![1, 2, 3]);
        assert_eq!(entry.log_size(), 8 + 8 + 8 + 4 + 3);
    }
}
