//! INDeleteInfo log entry.
//!
//!
//! Written when an IN node is deleted during tree compression. Contains the
//! node ID, idKey of the deleted node, and the database ID. Used during
//! recovery to replay tree compression operations.

use byteorder::{BigEndian, ReadBytesExt};
use bytes::{BufMut, BytesMut};
use std::io::{self, Cursor};
use thiserror::Error;

/// Error type for INDeleteInfo log entry operations.
#[derive(Debug, Error)]
pub enum InDeleteInfoEntryError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
}

/// INDeleteInfo log entry.
///
/// Records the deletion of an IN node during tree compression. Used by
/// recovery to replay tree compression and maintain B-tree integrity.
///
/// # Fields
///
/// - `deleted_node_id`: Node ID of the deleted IN node
/// - `deleted_id_key`: The idKey of the deleted IN node
/// - `database_id`: Database ID that contains the deleted node
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InDeleteInfoEntry {
    /// Node ID of the deleted IN node.
    pub deleted_node_id: u64,
    /// The idKey of the deleted IN node.
    pub deleted_id_key: Vec<u8>,
    /// Database ID containing the deleted node.
    pub database_id: u64,
}

impl InDeleteInfoEntry {
    /// Creates a new INDeleteInfo entry.
    pub fn new(deleted_node_id: u64, deleted_id_key: Vec<u8>, database_id: u64) -> Self {
        Self { deleted_node_id, deleted_id_key, database_id }
    }

    /// Returns the serialized size in bytes.
    pub fn log_size(&self) -> usize {
        8 + // deleted_node_id
        4 + self.deleted_id_key.len() + // deleted_id_key (len prefix + data)
        8   // database_id
    }

    /// Writes this entry to a buffer.
    pub fn write_to_log(&self, buf: &mut BytesMut) {
        buf.put_u64(self.deleted_node_id);
        buf.put_u32(self.deleted_id_key.len() as u32);
        buf.extend_from_slice(&self.deleted_id_key);
        buf.put_u64(self.database_id);
    }

    /// Reads an entry from a buffer.
    pub fn read_from_log(buf: &[u8]) -> Result<Self, InDeleteInfoEntryError> {
        let mut cursor = Cursor::new(buf);
        let deleted_node_id = cursor.read_u64::<BigEndian>()?;
        let key_len = cursor.read_u32::<BigEndian>()? as usize;
        let mut deleted_id_key = vec![0u8; key_len];
        io::Read::read_exact(&mut cursor, &mut deleted_id_key)?;
        let database_id = cursor.read_u64::<BigEndian>()?;
        Ok(Self { deleted_node_id, deleted_id_key, database_id })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_in_delete_info_roundtrip() {
        let entry = InDeleteInfoEntry::new(42, b"idkey_data".to_vec(), 7);

        let mut buf = BytesMut::new();
        entry.write_to_log(&mut buf);

        let decoded = InDeleteInfoEntry::read_from_log(&buf).unwrap();
        assert_eq!(entry, decoded);
        assert_eq!(decoded.deleted_node_id, 42);
        assert_eq!(decoded.deleted_id_key, b"idkey_data");
        assert_eq!(decoded.database_id, 7);
    }

    #[test]
    fn test_in_delete_info_empty_key() {
        let entry = InDeleteInfoEntry::new(1, vec![], 99);

        let mut buf = BytesMut::new();
        entry.write_to_log(&mut buf);

        let decoded = InDeleteInfoEntry::read_from_log(&buf).unwrap();
        assert_eq!(entry, decoded);
        assert!(decoded.deleted_id_key.is_empty());
    }

    #[test]
    fn test_log_size() {
        let key = b"somekey".to_vec();
        let entry = InDeleteInfoEntry::new(1, key.clone(), 2);
        assert_eq!(entry.log_size(), 8 + 4 + key.len() + 8);
        let mut buf = BytesMut::new();
        entry.write_to_log(&mut buf);
        assert_eq!(buf.len(), entry.log_size());
    }
}
