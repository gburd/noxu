//! Log item structure for write operations.
//!
//!
//! A LogItem holds the results of logging an entry: the assigned LSN,
//! the serialized bytes, and the header. Used primarily by replication
//! to access the materialized form of logged entries.

use crate::entry_header::LogEntryHeader;
use bytes::Bytes;
use noxu_util::Lsn;

/// Values returned when an item is logged.
///
/// This struct is used as a simple container for returning multiple values
/// from log write operations.
#[derive(Debug, Clone)]
pub struct LogItem {
    /// LSN of the new log entry.
    ///
    /// Is NULL_LSN if a BIN-delta is logged. If not NULL_LSN for a tree node,
    /// is typically used to update the slot in the parent IN.
    pub lsn: Lsn,

    /// Size of the new log entry (header + item).
    ///
    /// Used to update the LN slot in the BIN.
    pub size: usize,

    /// The header of the new log entry.
    ///
    /// Used by replication to do VLSN tracking and implement a tip cache.
    pub header: Option<LogEntryHeader>,

    /// The bytes of the new log entry.
    ///
    /// Used by replication to implement a tip cache. This includes both
    /// the header and the entry data.
    pub buffer: Option<Bytes>,
}

impl LogItem {
    /// Creates a new LogItem with NULL_LSN and zero size.
    pub fn new() -> Self {
        LogItem {
            lsn: noxu_util::NULL_LSN,
            size: 0,
            header: None,
            buffer: None,
        }
    }

    /// Creates a LogItem with the specified LSN and size.
    pub fn with_lsn_and_size(lsn: Lsn, size: usize) -> Self {
        LogItem { lsn, size, header: None, buffer: None }
    }

    /// Creates a complete LogItem with all fields.
    pub fn complete(
        lsn: Lsn,
        size: usize,
        header: LogEntryHeader,
        buffer: Bytes,
    ) -> Self {
        LogItem { lsn, size, header: Some(header), buffer: Some(buffer) }
    }

    /// Returns true if this LogItem has a valid LSN assigned.
    pub fn has_lsn(&self) -> bool {
        !self.lsn.is_null()
    }

    /// Returns true if this LogItem has header and buffer data.
    pub fn is_complete(&self) -> bool {
        self.header.is_some() && self.buffer.is_some()
    }
}

impl Default for LogItem {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let item = LogItem::new();
        assert!(item.lsn.is_null());
        assert_eq!(item.size, 0);
        assert!(!item.has_lsn());
        assert!(!item.is_complete());
    }

    #[test]
    fn test_with_lsn_and_size() {
        let lsn = Lsn::new(1, 100);
        let item = LogItem::with_lsn_and_size(lsn, 256);
        assert_eq!(item.lsn, lsn);
        assert_eq!(item.size, 256);
        assert!(item.has_lsn());
        assert!(!item.is_complete());
    }
}
