//! IN (Internal Node) log entry.
//!
//! Port of `com.sleepycat.je.log.entry.INLogEntry`.
//!
//! INLogEntry represents a B-tree internal node being written to the log.
//! This includes both upper internal nodes (UINs) and bottom internal nodes
//! (BINs) that are not being written as deltas.

use byteorder::{BigEndian, ReadBytesExt};
use bytes::{BufMut, BytesMut};
use noxu_util::lsn::Lsn;
use std::io::{self, Cursor};
use thiserror::Error;

/// Error type for IN log entry operations.
#[derive(Debug, Error)]
pub enum InLogEntryError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
}

/// IN (Internal Node) log entry.
///
/// Represents a B-tree internal node (either UIN or full BIN) being logged.
///
/// # Fields
///
/// - `db_id`: Database ID
/// - `prev_full_lsn`: LSN of the previous full version of this node
/// - `prev_delta_lsn`: LSN of the previous delta version (NULL_LSN if prev was full)
/// - `node_data`: Serialized IN/BIN data
///
/// NOTE: Since tree types (IN, BIN) aren't implemented yet, we use Vec<u8>
/// as placeholder for serialized node data.
#[derive(Debug, Clone)]
pub struct InLogEntry {
    /// Database ID.
    pub db_id: u64,
    /// LSN of previous full version of this node.
    pub prev_full_lsn: Lsn,
    /// LSN of previous delta version (NULL_LSN if previous was full).
    pub prev_delta_lsn: Lsn,
    /// Serialized node data.
    ///
    /// Carries the actual BIN/IN serialization produced by `BinStub::serialize_full()`.
    /// The format is: node_id(u64BE) | num_entries(u32BE) | per-slot data.
    /// Deserialization is performed by `BinStub::deserialize_full()` during recovery.
    pub node_data: Vec<u8>,
}

impl InLogEntry {
    /// Creates a new IN log entry.
    pub fn new(
        db_id: u64,
        prev_full_lsn: Lsn,
        prev_delta_lsn: Lsn,
        node_data: Vec<u8>,
    ) -> Self {
        Self { db_id, prev_full_lsn, prev_delta_lsn, node_data }
    }

    /// Returns true if this is a BIN delta entry (always false for INLogEntry).
    ///
    /// This is overridden in BinDeltaLogEntry.
    pub fn is_bin_delta(&self) -> bool {
        false
    }

    /// Returns the serialized size in bytes.
    pub fn log_size(&self) -> usize {
        8 + // db_id
        8 + // prev_full_lsn
        8 + // prev_delta_lsn
        4 + self.node_data.len() // node_data
    }

    /// Writes this entry to a buffer.
    pub fn write_to_log(&self, buf: &mut BytesMut) {
        buf.put_u64(self.db_id);
        buf.put_u64(self.prev_full_lsn.as_u64());
        buf.put_u64(self.prev_delta_lsn.as_u64());
        buf.put_u32(self.node_data.len() as u32);
        buf.extend_from_slice(&self.node_data);
    }

    /// Reads an entry from a buffer.
    pub fn read_from_log(buf: &[u8]) -> Result<Self, InLogEntryError> {
        let mut cursor = Cursor::new(buf);

        let db_id = cursor.read_u64::<BigEndian>()?;
        let prev_full_lsn = Lsn::from_u64(cursor.read_u64::<BigEndian>()?);
        let prev_delta_lsn = Lsn::from_u64(cursor.read_u64::<BigEndian>()?);

        let node_len = cursor.read_u32::<BigEndian>()? as usize;
        let mut node_data = vec![0u8; node_len];
        io::Read::read_exact(&mut cursor, &mut node_data)?;

        Ok(Self { db_id, prev_full_lsn, prev_delta_lsn, node_data })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use noxu_util::NULL_LSN;

    #[test]
    fn test_in_log_entry_roundtrip() {
        let node_data = b"fake_serialized_IN_node_data".to_vec();
        let entry = InLogEntry::new(
            42,
            Lsn::new(10, 5000),
            NULL_LSN,
            node_data.clone(),
        );

        let mut buf = BytesMut::new();
        entry.write_to_log(&mut buf);

        let decoded = InLogEntry::read_from_log(&buf).unwrap();
        assert_eq!(entry.db_id, decoded.db_id);
        assert_eq!(entry.prev_full_lsn, decoded.prev_full_lsn);
        assert_eq!(entry.prev_delta_lsn, decoded.prev_delta_lsn);
        assert_eq!(entry.node_data, decoded.node_data);
    }

    #[test]
    fn test_is_bin_delta() {
        let entry = InLogEntry::new(1, NULL_LSN, NULL_LSN, vec![]);
        assert!(!entry.is_bin_delta());
    }

    #[test]
    fn test_log_size() {
        let entry = InLogEntry::new(1, NULL_LSN, NULL_LSN, vec![1, 2, 3, 4, 5]);
        assert_eq!(entry.log_size(), 8 + 8 + 8 + 4 + 5);
    }
}
