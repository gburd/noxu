//! DelDupLN log entry.
//!
//!
//! A deletion record for a sorted-duplicate LN. Contains both the primary key
//! and the duplicate key (data key) so that the correct slot in the duplicate
//! sub-tree can be located and removed during recovery.

use byteorder::{BigEndian, ReadBytesExt};
use bytes::{BufMut, BytesMut};
use std::io::{self, Cursor};
use thiserror::Error;

/// Error type for DelDupLN log entry operations.
#[derive(Debug, Error)]
pub enum DelDupLnEntryError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
}

/// DelDupLN log entry.
///
/// Records the deletion of a leaf node in a sorted-duplicate database. Both
/// the primary key and the duplicate key are stored so recovery can identify
/// the exact slot.
///
/// # Fields
///
/// - `key`: Primary key
/// - `dup_key`: Duplicate (data) key identifying the specific duplicate record
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DelDupLnEntry {
    /// Primary key.
    pub key: Vec<u8>,
    /// Duplicate key (identifies the specific duplicate record).
    pub dup_key: Vec<u8>,
}

impl DelDupLnEntry {
    /// Creates a new DelDupLN entry.
    pub fn new(key: Vec<u8>, dup_key: Vec<u8>) -> Self {
        Self { key, dup_key }
    }

    /// Returns the serialized size in bytes.
    pub fn log_size(&self) -> usize {
        4 + self.key.len() + // key (len prefix + data)
        4 + self.dup_key.len() // dup_key (len prefix + data)
    }

    /// Writes this entry to a buffer.
    pub fn write_to_log(&self, buf: &mut BytesMut) {
        buf.put_u32(self.key.len() as u32);
        buf.extend_from_slice(&self.key);
        buf.put_u32(self.dup_key.len() as u32);
        buf.extend_from_slice(&self.dup_key);
    }

    /// Reads an entry from a buffer.
    pub fn read_from_log(buf: &[u8]) -> Result<Self, DelDupLnEntryError> {
        let mut cursor = Cursor::new(buf);

        let key_len = cursor.read_u32::<BigEndian>()? as usize;
        let mut key = vec![0u8; key_len];
        io::Read::read_exact(&mut cursor, &mut key)?;

        let dup_key_len = cursor.read_u32::<BigEndian>()? as usize;
        let mut dup_key = vec![0u8; dup_key_len];
        io::Read::read_exact(&mut cursor, &mut dup_key)?;

        Ok(Self { key, dup_key })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_del_dup_ln_roundtrip() {
        let entry =
            DelDupLnEntry::new(b"primary_key".to_vec(), b"dup_key_value".to_vec());

        let mut buf = BytesMut::new();
        entry.write_to_log(&mut buf);

        let decoded = DelDupLnEntry::read_from_log(&buf).unwrap();
        assert_eq!(entry, decoded);
        assert_eq!(decoded.key, b"primary_key");
        assert_eq!(decoded.dup_key, b"dup_key_value");
    }

    #[test]
    fn test_del_dup_ln_empty_keys() {
        let entry = DelDupLnEntry::new(vec![], vec![]);

        let mut buf = BytesMut::new();
        entry.write_to_log(&mut buf);

        let decoded = DelDupLnEntry::read_from_log(&buf).unwrap();
        assert_eq!(entry, decoded);
    }

    #[test]
    fn test_log_size() {
        let key = b"pk".to_vec();
        let dup_key = b"dk".to_vec();
        let entry = DelDupLnEntry::new(key.clone(), dup_key.clone());
        assert_eq!(entry.log_size(), 4 + key.len() + 4 + dup_key.len());
        let mut buf = BytesMut::new();
        entry.write_to_log(&mut buf);
        assert_eq!(buf.len(), entry.log_size());
    }
}
