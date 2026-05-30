//! OldLN log entry.
//!
//! Old format — .
//!
//! Represents a legacy LN log entry format that does not carry abort fields.
//! Used during recovery of logs written by older versions of the database.
//! Contains only the record key and data payload.

use byteorder::{BigEndian, ReadBytesExt};
use bytes::{BufMut, BytesMut};
use std::io::{self, Cursor};
use thiserror::Error;

/// Error type for OldLN log entry operations.
#[derive(Debug, Error)]
pub enum OldLnEntryError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
}

/// OldLN log entry.
///
/// Legacy format LN entry without abort fields. Used for reading log files
/// written by older versions of /Noxu DB during recovery.
///
/// # Fields
///
/// - `key`: Record key
/// - `data`: Record data (`None` for deletions)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OldLnEntry {
    /// Record key.
    pub key: Vec<u8>,
    /// Record data (`None` for deletions).
    pub data: Option<Vec<u8>>,
}

impl OldLnEntry {
    /// Creates a new OldLN entry for a write operation.
    pub fn new(key: Vec<u8>, data: Option<Vec<u8>>) -> Self {
        Self { key, data }
    }

    /// Returns true if this represents a deletion.
    pub fn is_deleted(&self) -> bool {
        self.data.is_none()
    }

    /// Returns the serialized size in bytes.
    pub fn log_size(&self) -> usize {
        let data_size = match &self.data {
            Some(d) => 4 + d.len(),
            None => 4, // length field set to 0
        };
        4 + self.key.len() + // key (len prefix + data)
        data_size
    }

    /// Writes this entry to a buffer.
    pub fn write_to_log(&self, buf: &mut BytesMut) {
        buf.put_u32(self.key.len() as u32);
        buf.extend_from_slice(&self.key);
        match &self.data {
            Some(d) => {
                buf.put_u32(d.len() as u32);
                buf.extend_from_slice(d);
            }
            None => {
                buf.put_u32(0);
            }
        }
    }

    /// Reads an entry from a buffer.
    pub fn read_from_log(buf: &[u8]) -> Result<Self, OldLnEntryError> {
        let mut cursor = Cursor::new(buf);
        let key_len = cursor.read_u32::<BigEndian>()? as usize;
        let mut key = vec![0u8; key_len];
        io::Read::read_exact(&mut cursor, &mut key)?;

        let data_len = cursor.read_u32::<BigEndian>()? as usize;
        let data = if data_len > 0 {
            let mut d = vec![0u8; data_len];
            io::Read::read_exact(&mut cursor, &mut d)?;
            Some(d)
        } else {
            None
        };

        Ok(Self { key, data })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_old_ln_roundtrip_with_data() {
        let entry =
            OldLnEntry::new(b"mykey".to_vec(), Some(b"mydata".to_vec()));

        let mut buf = BytesMut::new();
        entry.write_to_log(&mut buf);

        let decoded = OldLnEntry::read_from_log(&buf).unwrap();
        assert_eq!(entry, decoded);
        assert_eq!(decoded.key, b"mykey");
        assert_eq!(decoded.data, Some(b"mydata".to_vec()));
        assert!(!decoded.is_deleted());
    }

    #[test]
    fn test_old_ln_roundtrip_deletion() {
        let entry = OldLnEntry::new(b"deletedkey".to_vec(), None);

        let mut buf = BytesMut::new();
        entry.write_to_log(&mut buf);

        let decoded = OldLnEntry::read_from_log(&buf).unwrap();
        assert_eq!(entry, decoded);
        assert!(decoded.is_deleted());
    }

    #[test]
    fn test_log_size_with_data() {
        let key = b"k".to_vec();
        let data = b"value".to_vec();
        let entry = OldLnEntry::new(key.clone(), Some(data.clone()));
        assert_eq!(entry.log_size(), 4 + key.len() + 4 + data.len());
        let mut buf = BytesMut::new();
        entry.write_to_log(&mut buf);
        assert_eq!(buf.len(), entry.log_size());
    }

    #[test]
    fn test_log_size_deletion() {
        let key = b"k".to_vec();
        let entry = OldLnEntry::new(key.clone(), None);
        assert_eq!(entry.log_size(), 4 + key.len() + 4);
        let mut buf = BytesMut::new();
        entry.write_to_log(&mut buf);
        assert_eq!(buf.len(), entry.log_size());
    }
}
