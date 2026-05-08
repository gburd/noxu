//! OldBINDelta log entry.
//!
//!
//! Represents an old-format BIN delta log entry used during recovery of logs
//! written by earlier versions. Contains raw serialized delta data along with
//! the node and database IDs.

use byteorder::{BigEndian, ReadBytesExt};
use bytes::{BufMut, BytesMut};
use std::io::{self, Cursor};
use thiserror::Error;

/// Error type for OldBINDelta log entry operations.
#[derive(Debug, Error)]
pub enum OldBinDeltaEntryError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
}

/// OldBINDelta log entry.
///
/// Carries an old-format BIN delta, used during recovery of legacy log files.
/// The delta payload is stored as opaque bytes.
///
/// # Fields
///
/// - `node_id`: BIN node ID
/// - `db_id`: Database ID
/// - `data`: Serialized old-format BIN delta payload
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OldBinDeltaEntry {
    /// BIN node ID.
    pub node_id: u64,
    /// Database ID.
    pub db_id: u64,
    /// Serialized delta payload.
    pub data: Vec<u8>,
}

impl OldBinDeltaEntry {
    /// Creates a new OldBINDelta entry.
    pub fn new(node_id: u64, db_id: u64, data: Vec<u8>) -> Self {
        Self { node_id, db_id, data }
    }

    /// Returns the serialized size in bytes.
    pub fn log_size(&self) -> usize {
        8 + // node_id
        8 + // db_id
        4 + self.data.len() // data (len prefix + payload)
    }

    /// Writes this entry to a buffer.
    pub fn write_to_log(&self, buf: &mut BytesMut) {
        buf.put_u64(self.node_id);
        buf.put_u64(self.db_id);
        buf.put_u32(self.data.len() as u32);
        buf.extend_from_slice(&self.data);
    }

    /// Reads an entry from a buffer.
    pub fn read_from_log(buf: &[u8]) -> Result<Self, OldBinDeltaEntryError> {
        let mut cursor = Cursor::new(buf);
        let node_id = cursor.read_u64::<BigEndian>()?;
        let db_id = cursor.read_u64::<BigEndian>()?;
        let data_len = cursor.read_u32::<BigEndian>()? as usize;
        let mut data = vec![0u8; data_len];
        io::Read::read_exact(&mut cursor, &mut data)?;
        Ok(Self { node_id, db_id, data })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_old_bin_delta_roundtrip() {
        let payload = b"serialized_old_bin_delta".to_vec();
        let entry = OldBinDeltaEntry::new(77, 3, payload.clone());

        let mut buf = BytesMut::new();
        entry.write_to_log(&mut buf);

        let decoded = OldBinDeltaEntry::read_from_log(&buf).unwrap();
        assert_eq!(entry, decoded);
        assert_eq!(decoded.node_id, 77);
        assert_eq!(decoded.db_id, 3);
        assert_eq!(decoded.data, payload);
    }

    #[test]
    fn test_old_bin_delta_empty_data() {
        let entry = OldBinDeltaEntry::new(0, 0, vec![]);

        let mut buf = BytesMut::new();
        entry.write_to_log(&mut buf);

        let decoded = OldBinDeltaEntry::read_from_log(&buf).unwrap();
        assert_eq!(entry, decoded);
        assert!(decoded.data.is_empty());
    }

    #[test]
    fn test_log_size() {
        let data = b"delta".to_vec();
        let entry = OldBinDeltaEntry::new(1, 2, data.clone());
        assert_eq!(entry.log_size(), 8 + 8 + 4 + data.len());
        let mut buf = BytesMut::new();
        entry.write_to_log(&mut buf);
        assert_eq!(buf.len(), entry.log_size());
    }
}
