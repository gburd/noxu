//! Base type for transaction end log entries (commit/abort).
//!

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::io;

/// Base type for transaction end log entries (commit/abort).
///
///
#[derive(Debug, Clone)]
pub struct TxnEnd {
    /// Transaction ID.
    pub id: i64,
    /// Timestamp of the commit/abort.
    pub timestamp_ms: u64,
    /// LSN of the last log entry written by this txn.
    pub last_lsn: u64,
    /// Replication master node ID (0 if not replicated).
    pub rep_master_node_id: i32,
    /// Durable transaction VLSN.
    pub dtvlsn: i64,
}

impl TxnEnd {
    /// Creates a new TxnEnd.
    pub fn new(
        id: i64,
        last_lsn: u64,
        rep_master_node_id: i32,
        dtvlsn: i64,
    ) -> Self {
        let timestamp_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        TxnEnd { id, timestamp_ms, last_lsn, rep_master_node_id, dtvlsn }
    }

    /// Returns true if this transaction logged any entries.
    pub fn has_logged_entries(&self) -> bool {
        self.last_lsn != crate::util::NULL_LSN.as_u64()
    }

    /// Returns the serialized size in bytes.
    pub fn log_size(&self) -> usize {
        8 + 8 + 8 + 4 + 8 // id + timestamp + last_lsn + master_id + dtvlsn = 36
    }

    /// Writes this TxnEnd to a byte buffer (big-endian).
    pub fn write_to_log(&self, buf: &mut Vec<u8>) {
        buf.write_i64::<BigEndian>(self.id).unwrap();
        buf.write_u64::<BigEndian>(self.timestamp_ms).unwrap();
        buf.write_u64::<BigEndian>(self.last_lsn).unwrap();
        buf.write_i32::<BigEndian>(self.rep_master_node_id).unwrap();
        buf.write_i64::<BigEndian>(self.dtvlsn).unwrap();
    }

    /// Reads a TxnEnd from a byte buffer (big-endian).
    pub fn read_from_log(buf: &[u8]) -> io::Result<Self> {
        let mut cursor = io::Cursor::new(buf);
        let id = cursor.read_i64::<BigEndian>()?;
        let timestamp_ms = cursor.read_u64::<BigEndian>()?;
        let last_lsn = cursor.read_u64::<BigEndian>()?;
        let rep_master_node_id = cursor.read_i32::<BigEndian>()?;
        let dtvlsn = cursor.read_i64::<BigEndian>()?;

        Ok(TxnEnd { id, timestamp_ms, last_lsn, rep_master_node_id, dtvlsn })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let end = TxnEnd::new(42, 1000, 1, 100);
        assert_eq!(end.id, 42);
        assert_eq!(end.last_lsn, 1000);
        assert_eq!(end.rep_master_node_id, 1);
        assert_eq!(end.dtvlsn, 100);
        assert!(end.timestamp_ms > 0);
    }

    #[test]
    fn test_has_logged_entries() {
        let end1 = TxnEnd::new(1, crate::util::NULL_LSN.as_u64(), 0, 0);
        assert!(!end1.has_logged_entries());

        let end2 = TxnEnd::new(1, 1000, 0, 0);
        assert!(end2.has_logged_entries());
    }

    #[test]
    fn test_log_size() {
        let end = TxnEnd::new(1, 1000, 0, 0);
        assert_eq!(end.log_size(), 36);
    }

    #[test]
    fn test_serialization_round_trip() {
        let original = TxnEnd::new(12345, 67890, 5, 999);

        let mut buf = Vec::new();
        original.write_to_log(&mut buf);
        assert_eq!(buf.len(), 36);

        let deserialized = TxnEnd::read_from_log(&buf).unwrap();
        assert_eq!(deserialized.id, original.id);
        assert_eq!(deserialized.timestamp_ms, original.timestamp_ms);
        assert_eq!(deserialized.last_lsn, original.last_lsn);
        assert_eq!(
            deserialized.rep_master_node_id,
            original.rep_master_node_id
        );
        assert_eq!(deserialized.dtvlsn, original.dtvlsn);
    }
}
