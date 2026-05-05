//! Empty log entry.
//!
//! Port of `com.sleepycat.je.log.entry.EmptyLogEntry`.
//!
//! Used for log entry types that need no additional data beyond the entry
//! type itself, such as checkpoint start/end markers.

use bytes::{BufMut, BytesMut};
use std::io;
use thiserror::Error;

/// Error type for empty log entry operations.
#[derive(Debug, Error)]
pub enum EmptyLogEntryError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
}

/// Empty log entry.
///
/// Contains no information - the LogEntryType in the header is sufficient.
/// A single dummy byte is written to satisfy buffer requirements in checksums
/// and file readers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EmptyLogEntry;

impl EmptyLogEntry {
    /// Creates a new empty log entry.
    pub fn new() -> Self {
        Self
    }

    /// Returns the serialized size (always 1).
    pub const fn log_size() -> usize {
        1
    }

    /// Writes this entry to a buffer (writes a single dummy byte).
    pub fn write_to_log(&self, buf: &mut BytesMut) {
        buf.put_u8(42); // Arbitrary marker byte
    }

    /// Reads an entry from a buffer (consumes one byte).
    pub fn read_from_log(buf: &[u8]) -> Result<Self, EmptyLogEntryError> {
        if buf.is_empty() {
            return Err(EmptyLogEntryError::Io(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Empty buffer",
            )));
        }
        // Just consume the byte, value doesn't matter
        Ok(Self)
    }
}

impl Default for EmptyLogEntry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_log_entry_roundtrip() {
        let entry = EmptyLogEntry::new();

        let mut buf = BytesMut::new();
        entry.write_to_log(&mut buf);

        assert_eq!(buf.len(), 1);

        let decoded = EmptyLogEntry::read_from_log(&buf).unwrap();
        assert_eq!(entry, decoded);
    }

    #[test]
    fn test_log_size() {
        assert_eq!(EmptyLogEntry::log_size(), 1);
    }

    #[test]
    fn test_default() {
        let entry = EmptyLogEntry;
        assert_eq!(entry, EmptyLogEntry::new());
    }
}
