//! Database operation type enum.
//!
//!
//! Identifies the type of database operation (create, remove, truncate, rename,
//! update config) that caused a NameLN to be logged. Used for replication.

use byteorder::ReadBytesExt;
use bytes::{BufMut, BytesMut};
use std::io::{self, Cursor};
use thiserror::Error;

/// Error type for database operation type operations.
#[derive(Debug, Error)]
pub enum DbOperationTypeError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("Invalid DbOperationType value: {0}")]
    InvalidValue(u8),
}

/// Database operation types that can be replicated.
///
/// Used in NameLNLogEntry to document the type of API operation which
/// instigated the logging of a NameLN, enabling replication of database
/// operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum DbOperationType {
    /// No specific operation.
    #[default]
    None,
    /// Database creation.
    Create,
    /// Database removal.
    Remove,
    /// Database truncation.
    Truncate,
    /// Database rename.
    Rename,
    /// Database configuration update.
    UpdateConfig,
}

impl DbOperationType {
    /// Returns the byte value for this operation type.
    #[inline]
    pub fn as_u8(self) -> u8 {
        match self {
            DbOperationType::None => 0,
            DbOperationType::Create => 1,
            DbOperationType::Remove => 2,
            DbOperationType::Truncate => 3,
            DbOperationType::Rename => 4,
            DbOperationType::UpdateConfig => 5,
        }
    }

    /// Creates a DbOperationType from a byte value.
    pub fn from_u8(value: u8) -> Result<Self, DbOperationTypeError> {
        match value {
            0 => Ok(DbOperationType::None),
            1 => Ok(DbOperationType::Create),
            2 => Ok(DbOperationType::Remove),
            3 => Ok(DbOperationType::Truncate),
            4 => Ok(DbOperationType::Rename),
            5 => Ok(DbOperationType::UpdateConfig),
            _ => Err(DbOperationTypeError::InvalidValue(value)),
        }
    }

    /// Returns true if this operation type requires database configuration to be written.
    pub fn is_write_config_type(self) -> bool {
        matches!(self, DbOperationType::Create | DbOperationType::UpdateConfig)
    }

    /// Returns the serialized size in bytes.
    pub const fn log_size() -> usize {
        1
    }

    /// Writes this operation type to a buffer.
    pub fn write_to_log(&self, buf: &mut BytesMut) {
        buf.put_u8(self.as_u8());
    }

    /// Reads an operation type from a buffer.
    pub fn read_from_log(buf: &[u8]) -> Result<Self, DbOperationTypeError> {
        let mut cursor = Cursor::new(buf);
        let value = cursor.read_u8()?;
        Self::from_u8(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_roundtrip() {
        for op in [
            DbOperationType::None,
            DbOperationType::Create,
            DbOperationType::Remove,
            DbOperationType::Truncate,
            DbOperationType::Rename,
            DbOperationType::UpdateConfig,
        ] {
            let mut buf = BytesMut::new();
            op.write_to_log(&mut buf);
            let decoded = DbOperationType::read_from_log(&buf).unwrap();
            assert_eq!(op, decoded);
        }
    }

    #[test]
    fn test_is_write_config_type() {
        assert!(!DbOperationType::None.is_write_config_type());
        assert!(DbOperationType::Create.is_write_config_type());
        assert!(!DbOperationType::Remove.is_write_config_type());
        assert!(!DbOperationType::Truncate.is_write_config_type());
        assert!(!DbOperationType::Rename.is_write_config_type());
        assert!(DbOperationType::UpdateConfig.is_write_config_type());
    }

    #[test]
    fn test_invalid_value() {
        let result = DbOperationType::from_u8(99);
        assert!(result.is_err());
    }

    #[test]
    fn test_log_size() {
        assert_eq!(DbOperationType::log_size(), 1);
    }
}
