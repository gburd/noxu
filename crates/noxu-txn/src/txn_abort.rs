//! TxnAbort log entry.
//!
//! Port of `com.sleepycat.je.txn.TxnAbort`.

use crate::txn_end::TxnEnd;
use std::io;

/// A TxnAbort is logged when a transaction aborts.
///
/// Port of `com.sleepycat.je.txn.TxnAbort`.
#[derive(Debug, Clone)]
pub struct TxnAbort {
    pub end: TxnEnd,
}

impl TxnAbort {
    /// Creates a new TxnAbort log entry.
    pub fn new(id: i64, last_lsn: u64, master_id: i32, dtvlsn: i64) -> Self {
        TxnAbort { end: TxnEnd::new(id, last_lsn, master_id, dtvlsn) }
    }

    /// Returns the serialized size in bytes.
    pub fn log_size(&self) -> usize {
        self.end.log_size()
    }

    /// Writes this TxnAbort to a byte buffer.
    pub fn write_to_log(&self, buf: &mut Vec<u8>) {
        self.end.write_to_log(buf);
    }

    /// Reads a TxnAbort from a byte buffer.
    pub fn read_from_log(buf: &[u8]) -> io::Result<Self> {
        let end = TxnEnd::read_from_log(buf)?;
        Ok(TxnAbort { end })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let abort = TxnAbort::new(100, 5000, 2, 200);
        assert_eq!(abort.end.id, 100);
        assert_eq!(abort.end.last_lsn, 5000);
        assert_eq!(abort.end.rep_master_node_id, 2);
        assert_eq!(abort.end.dtvlsn, 200);
    }

    #[test]
    fn test_log_size() {
        let abort = TxnAbort::new(1, 1000, 0, 0);
        assert_eq!(abort.log_size(), 36);
    }

    #[test]
    fn test_serialization_round_trip() {
        let original = TxnAbort::new(999, 12345, 7, 54321);

        let mut buf = Vec::new();
        original.write_to_log(&mut buf);
        assert_eq!(buf.len(), 36);

        let deserialized = TxnAbort::read_from_log(&buf).unwrap();
        assert_eq!(deserialized.end.id, original.end.id);
        assert_eq!(deserialized.end.timestamp_ms, original.end.timestamp_ms);
        assert_eq!(deserialized.end.last_lsn, original.end.last_lsn);
        assert_eq!(
            deserialized.end.rep_master_node_id,
            original.end.rep_master_node_id
        );
        assert_eq!(deserialized.end.dtvlsn, original.end.dtvlsn);
    }
}
