//! INDupDeleteInfo log entry.
//!
//!
//! Written when a duplicate-tree IN node is deleted during tree compression.
//! Same fields as INDeleteInfo but applies to the duplicate sub-tree. Used
//! during recovery to replay dup-tree compression operations.

use byteorder::{BigEndian, ReadBytesExt};
use bytes::{BufMut, BytesMut};
use std::io::{self, Cursor};
use thiserror::Error;

/// Error type for INDupDeleteInfo log entry operations.
#[derive(Debug, Error)]
pub enum InDupDeleteInfoEntryError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
}

/// INDupDeleteInfo log entry.
///
/// Records the deletion of a duplicate-tree IN node during tree compression.
/// Structurally identical to INDeleteInfo but pertains to the duplicate
/// sub-tree rather than the main tree.
///
/// # Fields
///
/// - `deleted_node_id`: Node ID of the deleted dup-tree IN node
/// - `deleted_id_key`: The idKey of the deleted dup-tree IN node
/// - `database_id`: Database ID that contains the deleted node
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InDupDeleteInfoEntry {
    /// Node ID of the deleted dup-tree IN node.
    pub deleted_node_id: u64,
    /// The idKey of the deleted dup-tree IN node.
    pub deleted_id_key: Vec<u8>,
    /// Database ID containing the deleted node.
    pub database_id: u64,
}

impl InDupDeleteInfoEntry {
    /// Creates a new INDupDeleteInfo entry.
    pub fn new(
        deleted_node_id: u64,
        deleted_id_key: Vec<u8>,
        database_id: u64,
    ) -> Self {
        Self { deleted_node_id, deleted_id_key, database_id }
    }

    /// Returns the serialized size in bytes.
    pub fn log_size(&self) -> usize {
        8 + // deleted_node_id
        4 + self.deleted_id_key.len() + // deleted_id_key (len prefix + data)
        8 // database_id
    }

    /// Writes this entry to a buffer.
    pub fn write_to_log(&self, buf: &mut BytesMut) {
        buf.put_u64(self.deleted_node_id);
        buf.put_u32(self.deleted_id_key.len() as u32);
        buf.extend_from_slice(&self.deleted_id_key);
        buf.put_u64(self.database_id);
    }

    /// Reads an entry from a buffer.
    pub fn read_from_log(
        buf: &[u8],
    ) -> Result<Self, InDupDeleteInfoEntryError> {
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
    fn test_in_dupdelete_info_roundtrip() {
        let entry = InDupDeleteInfoEntry::new(55, b"dup_idkey".to_vec(), 12);

        let mut buf = BytesMut::new();
        entry.write_to_log(&mut buf);

        let decoded = InDupDeleteInfoEntry::read_from_log(&buf).unwrap();
        assert_eq!(entry, decoded);
        assert_eq!(decoded.deleted_node_id, 55);
        assert_eq!(decoded.deleted_id_key, b"dup_idkey");
        assert_eq!(decoded.database_id, 12);
    }

    #[test]
    fn test_in_dupdelete_info_empty_key() {
        let entry = InDupDeleteInfoEntry::new(0, vec![], 0);

        let mut buf = BytesMut::new();
        entry.write_to_log(&mut buf);

        let decoded = InDupDeleteInfoEntry::read_from_log(&buf).unwrap();
        assert_eq!(entry, decoded);
    }

    #[test]
    fn test_log_size() {
        let key = b"dupkey".to_vec();
        let entry = InDupDeleteInfoEntry::new(10, key.clone(), 20);
        assert_eq!(entry.log_size(), 8 + 4 + key.len() + 8);
        let mut buf = BytesMut::new();
        entry.write_to_log(&mut buf);
        assert_eq!(buf.len(), entry.log_size());
    }
}
