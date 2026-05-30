//! Transaction commit and abort log entries.
//!
//! along with .txn.TxnCommit` and `TxnAbort`.
//!
//! These entries mark the end of a transaction (either commit or abort).

use byteorder::{BigEndian, ReadBytesExt};
use bytes::{BufMut, BytesMut};
use crate::util::{lsn::Lsn, vlsn::Vlsn};
use std::io::{self, Cursor};
use thiserror::Error;

/// Error type for transaction end entry operations.
#[derive(Debug, Error)]
pub enum TxnEndError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
}

/// Transaction end type (commit or abort).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnEndType {
    Commit,
    Abort,
}

/// Transaction end entry (commit or abort).
///
/// Records the completion of a transaction. Contains the transaction ID,
/// timestamp, and replication metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxnEndEntry {
    /// Type of transaction end.
    pub end_type: TxnEndType,
    /// Transaction ID.
    pub txn_id: i64,
    /// LSN of the last log entry written by this transaction.
    pub last_lsn: Lsn,
    /// Timestamp of the commit/abort (milliseconds since epoch).
    pub timestamp: u64,
    /// Master node ID for replication (0 if not replicated).
    pub master_node_id: i32,
    /// Durable Transaction VLSN for replication consistency.
    pub dtvlsn: Vlsn,
}

impl TxnEndEntry {
    /// Creates a new transaction commit entry.
    pub fn new_commit(
        txn_id: i64,
        last_lsn: Lsn,
        timestamp: u64,
        master_node_id: i32,
        dtvlsn: Vlsn,
    ) -> Self {
        Self {
            end_type: TxnEndType::Commit,
            txn_id,
            last_lsn,
            timestamp,
            master_node_id,
            dtvlsn,
        }
    }

    /// Creates a new transaction abort entry.
    pub fn new_abort(
        txn_id: i64,
        last_lsn: Lsn,
        timestamp: u64,
        master_node_id: i32,
        dtvlsn: Vlsn,
    ) -> Self {
        Self {
            end_type: TxnEndType::Abort,
            txn_id,
            last_lsn,
            timestamp,
            master_node_id,
            dtvlsn,
        }
    }

    /// Returns true if this is a commit entry.
    pub fn is_commit(&self) -> bool {
        self.end_type == TxnEndType::Commit
    }

    /// Returns true if this is an abort entry.
    pub fn is_abort(&self) -> bool {
        self.end_type == TxnEndType::Abort
    }

    /// Returns the serialized size in bytes.
    pub fn log_size(&self) -> usize {
        1 + // end_type
        8 + // txn_id
        8 + // last_lsn
        8 + // timestamp
        4 + // master_node_id
        8 // dtvlsn
    }

    /// Writes this entry to a buffer.
    pub fn write_to_log(&self, buf: &mut BytesMut) {
        buf.put_u8(match self.end_type {
            TxnEndType::Commit => 1,
            TxnEndType::Abort => 2,
        });
        buf.put_i64(self.txn_id);
        buf.put_u64(self.last_lsn.as_u64());
        buf.put_u64(self.timestamp);
        buf.put_i32(self.master_node_id);
        buf.put_i64(self.dtvlsn.sequence());
    }

    /// Reads an entry from a buffer.
    pub fn read_from_log(buf: &[u8]) -> Result<Self, TxnEndError> {
        let mut cursor = Cursor::new(buf);

        let end_type_byte = cursor.read_u8()?;
        let end_type = match end_type_byte {
            1 => TxnEndType::Commit,
            2 => TxnEndType::Abort,
            _ => TxnEndType::Commit, // Default to commit for unknown values
        };

        let txn_id = cursor.read_i64::<BigEndian>()?;
        let last_lsn = Lsn::from_u64(cursor.read_u64::<BigEndian>()?);
        let timestamp = cursor.read_u64::<BigEndian>()?;
        let master_node_id = cursor.read_i32::<BigEndian>()?;
        let dtvlsn_seq = cursor.read_i64::<BigEndian>()?;
        let dtvlsn = Vlsn::new(dtvlsn_seq);

        Ok(Self {
            end_type,
            txn_id,
            last_lsn,
            timestamp,
            master_node_id,
            dtvlsn,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::lsn::NULL_LSN;
    use crate::util::vlsn::NULL_VLSN;

    #[test]
    fn test_commit_roundtrip() {
        let entry = TxnEndEntry::new_commit(
            123,
            Lsn::new(1, 1000),
            999888777,
            5,
            Vlsn::new(42),
        );

        let mut buf = BytesMut::new();
        entry.write_to_log(&mut buf);

        let decoded = TxnEndEntry::read_from_log(&buf).unwrap();
        assert_eq!(entry, decoded);
        assert!(decoded.is_commit());
        assert!(!decoded.is_abort());
    }

    #[test]
    fn test_abort_roundtrip() {
        let entry = TxnEndEntry::new_abort(
            456,
            Lsn::new(2, 2000),
            111222333,
            0,
            NULL_VLSN,
        );

        let mut buf = BytesMut::new();
        entry.write_to_log(&mut buf);

        let decoded = TxnEndEntry::read_from_log(&buf).unwrap();
        assert_eq!(entry, decoded);
        assert!(decoded.is_abort());
        assert!(!decoded.is_commit());
    }

    #[test]
    fn test_log_size() {
        let entry = TxnEndEntry::new_commit(1, NULL_LSN, 0, 0, NULL_VLSN);
        assert_eq!(entry.log_size(), 37);
    }
}
