//! Trace log entry.
//!
//!
//! Used for logging critical event tracing messages into the log files.
//! Only critical messages that should always be included should use this.

use byteorder::{BigEndian, ReadBytesExt};
use bytes::{BufMut, BytesMut};
use std::io::{self, Cursor};
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;

/// Error type for trace log entry operations.
#[derive(Debug, Error)]
pub enum TraceLogEntryError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("UTF-8 error: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),
}

/// Trace log entry.
///
/// Records a trace message along with a timestamp. Used for logging critical
/// events and debugging information directly into the log file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceLogEntry {
    /// Timestamp (milliseconds since epoch).
    pub timestamp: u64,
    /// Trace message.
    pub message: String,
}

impl TraceLogEntry {
    /// Creates a new trace log entry with the current timestamp.
    pub fn new(message: String) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        Self { timestamp, message }
    }

    /// Creates a trace log entry with a specific timestamp.
    pub fn with_timestamp(timestamp: u64, message: String) -> Self {
        Self { timestamp, message }
    }

    /// Returns the serialized size in bytes.
    pub fn log_size(&self) -> usize {
        8 + // timestamp
        4 + self.message.len() // message length + bytes
    }

    /// Writes this entry to a buffer.
    pub fn write_to_log(&self, buf: &mut BytesMut) {
        buf.put_u64(self.timestamp);
        let msg_bytes = self.message.as_bytes();
        buf.put_u32(msg_bytes.len() as u32);
        buf.extend_from_slice(msg_bytes);
    }

    /// Reads an entry from a buffer.
    pub fn read_from_log(buf: &[u8]) -> Result<Self, TraceLogEntryError> {
        let mut cursor = Cursor::new(buf);

        let timestamp = cursor.read_u64::<BigEndian>()?;
        let msg_len = cursor.read_u32::<BigEndian>()? as usize;

        let mut msg_bytes = vec![0u8; msg_len];
        io::Read::read_exact(&mut cursor, &mut msg_bytes)?;
        let message = String::from_utf8(msg_bytes)?;

        Ok(Self { timestamp, message })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trace_log_entry_roundtrip() {
        let entry = TraceLogEntry::new("Test trace message".to_string());

        let mut buf = BytesMut::new();
        entry.write_to_log(&mut buf);

        let decoded = TraceLogEntry::read_from_log(&buf).unwrap();
        assert_eq!(entry.message, decoded.message);
        assert_eq!(entry.timestamp, decoded.timestamp);
    }

    #[test]
    fn test_trace_with_timestamp() {
        let entry = TraceLogEntry::with_timestamp(
            123456789,
            "Historical message".to_string(),
        );

        let mut buf = BytesMut::new();
        entry.write_to_log(&mut buf);

        let decoded = TraceLogEntry::read_from_log(&buf).unwrap();
        assert_eq!(entry.timestamp, 123456789);
        assert_eq!(decoded.message, "Historical message");
    }

    #[test]
    fn test_log_size() {
        let entry = TraceLogEntry::new("Hello".to_string());
        assert_eq!(entry.log_size(), 8 + 4 + 5);
    }

    #[test]
    fn test_empty_message() {
        let entry = TraceLogEntry::new(String::new());

        let mut buf = BytesMut::new();
        entry.write_to_log(&mut buf);

        let decoded = TraceLogEntry::read_from_log(&buf).unwrap();
        assert_eq!(decoded.message, "");
    }
}
