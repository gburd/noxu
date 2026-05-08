//! RestoreRequired log entry.
//!
//!
//! Indicates that the environment's log files are not recoverable and that
//! some curative action is required before the environment can be opened.

use byteorder::{BigEndian, ReadBytesExt};
use bytes::{BufMut, BytesMut};
use std::collections::HashMap;
use std::io::{self, Cursor};
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;

/// Error type for RestoreRequired operations.
#[derive(Debug, Error)]
pub enum RestoreRequiredError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("UTF-8 error: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),
    #[error("Invalid failure type: {0}")]
    InvalidFailureType(String),
}

/// The type of failure that requires restore action.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FailureType {
    /// Environment needs network restore from replication.
    NetworkRestore,
    /// Log checksum error detected.
    LogChecksum,
    /// B-tree corruption detected.
    BtreeCorruption,
}

impl FailureType {
    /// Converts the failure type to a string identifier.
    pub fn as_str(&self) -> &'static str {
        match self {
            FailureType::NetworkRestore => "NETWORK_RESTORE",
            FailureType::LogChecksum => "LOG_CHECKSUM",
            FailureType::BtreeCorruption => "BTREE_CORRUPTION",
        }
    }

    /// Parses a failure type from a string identifier.
    pub fn parse(s: &str) -> Result<Self, RestoreRequiredError> {
        match s {
            "NETWORK_RESTORE" => Ok(FailureType::NetworkRestore),
            "LOG_CHECKSUM" => Ok(FailureType::LogChecksum),
            "BTREE_CORRUPTION" => Ok(FailureType::BtreeCorruption),
            _ => Err(RestoreRequiredError::InvalidFailureType(s.to_string())),
        }
    }
}

/// RestoreRequired entry.
///
/// Indicates that the environment requires restoration before it can be used.
/// Contains the failure type and associated properties that describe what
/// needs to be fixed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoreRequired {
    /// The type of failure that occurred.
    pub failure_type: FailureType,
    /// Timestamp when the failure was recorded.
    pub timestamp: u64,
    /// Properties describing the failure and how to fix it.
    pub properties: HashMap<String, String>,
}

impl RestoreRequired {
    /// Creates a new RestoreRequired entry.
    pub fn new(
        failure_type: FailureType,
        properties: HashMap<String, String>,
    ) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        Self { failure_type, timestamp, properties }
    }

    /// Creates a RestoreRequired entry with a specific timestamp.
    pub fn with_timestamp(
        failure_type: FailureType,
        timestamp: u64,
        properties: HashMap<String, String>,
    ) -> Self {
        Self { failure_type, timestamp, properties }
    }

    /// Returns the serialized size in bytes.
    pub fn log_size(&self) -> usize {
        let type_str = self.failure_type.as_str();
        let mut size = 4 + type_str.len(); // failure_type string
        size += 8; // timestamp

        // Properties map size
        size += 4; // property count
        for (key, value) in &self.properties {
            size += 4 + key.len(); // key
            size += 4 + value.len(); // value
        }

        size
    }

    /// Writes this entry to a buffer.
    pub fn write_to_log(&self, buf: &mut BytesMut) {
        // Write failure type
        let type_str = self.failure_type.as_str();
        buf.put_u32(type_str.len() as u32);
        buf.extend_from_slice(type_str.as_bytes());

        // Write timestamp
        buf.put_u64(self.timestamp);

        // Write properties
        buf.put_u32(self.properties.len() as u32);
        for (key, value) in &self.properties {
            buf.put_u32(key.len() as u32);
            buf.extend_from_slice(key.as_bytes());
            buf.put_u32(value.len() as u32);
            buf.extend_from_slice(value.as_bytes());
        }
    }

    /// Reads an entry from a buffer.
    pub fn read_from_log(buf: &[u8]) -> Result<Self, RestoreRequiredError> {
        let mut cursor = Cursor::new(buf);

        // Read failure type
        let type_len = cursor.read_u32::<BigEndian>()? as usize;
        let mut type_bytes = vec![0u8; type_len];
        io::Read::read_exact(&mut cursor, &mut type_bytes)?;
        let type_str = String::from_utf8(type_bytes)?;
        let failure_type = FailureType::parse(&type_str)?;

        // Read timestamp
        let timestamp = cursor.read_u64::<BigEndian>()?;

        // Read properties
        let prop_count = cursor.read_u32::<BigEndian>()? as usize;
        let mut properties = HashMap::new();
        for _ in 0..prop_count {
            let key_len = cursor.read_u32::<BigEndian>()? as usize;
            let mut key_bytes = vec![0u8; key_len];
            io::Read::read_exact(&mut cursor, &mut key_bytes)?;
            let key = String::from_utf8(key_bytes)?;

            let value_len = cursor.read_u32::<BigEndian>()? as usize;
            let mut value_bytes = vec![0u8; value_len];
            io::Read::read_exact(&mut cursor, &mut value_bytes)?;
            let value = String::from_utf8(value_bytes)?;

            properties.insert(key, value);
        }

        Ok(Self { failure_type, timestamp, properties })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_restore_required_roundtrip() {
        let mut props = HashMap::new();
        props.insert("error_file".to_string(), "0000001a.jdb".to_string());
        props.insert("error_offset".to_string(), "12345".to_string());

        let entry =
            RestoreRequired::new(FailureType::LogChecksum, props.clone());

        let mut buf = BytesMut::new();
        entry.write_to_log(&mut buf);

        let decoded = RestoreRequired::read_from_log(&buf).unwrap();
        assert_eq!(entry.failure_type, decoded.failure_type);
        assert_eq!(entry.properties, decoded.properties);
    }

    #[test]
    fn test_all_failure_types() {
        for failure_type in [
            FailureType::NetworkRestore,
            FailureType::LogChecksum,
            FailureType::BtreeCorruption,
        ] {
            let entry = RestoreRequired::new(failure_type, HashMap::new());

            let mut buf = BytesMut::new();
            entry.write_to_log(&mut buf);

            let decoded = RestoreRequired::read_from_log(&buf).unwrap();
            assert_eq!(failure_type, decoded.failure_type);
        }
    }

    #[test]
    fn test_failure_type_string_conversion() {
        assert_eq!(FailureType::NetworkRestore.as_str(), "NETWORK_RESTORE");
        assert_eq!(
            FailureType::parse("NETWORK_RESTORE").unwrap(),
            FailureType::NetworkRestore
        );
    }

    #[test]
    fn test_empty_properties() {
        let entry =
            RestoreRequired::new(FailureType::BtreeCorruption, HashMap::new());

        let mut buf = BytesMut::new();
        entry.write_to_log(&mut buf);

        let decoded = RestoreRequired::read_from_log(&buf).unwrap();
        assert!(decoded.properties.is_empty());
    }
}
