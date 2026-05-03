//! Database identifier.
//!
//! Port of `com.sleepycat.je.dbi.DatabaseId`.

/// Unique identifier for a database.
///
/// Port of `com.sleepycat.je.dbi.DatabaseId`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DatabaseId(i64);

impl DatabaseId {
    /// Creates a new DatabaseId.
    pub fn new(id: i64) -> Self {
        DatabaseId(id)
    }

    /// Returns the raw ID value.
    pub fn id(&self) -> i64 {
        self.0
    }

    /// Returns the raw ID value (alias for compatibility).
    pub fn as_i64(&self) -> i64 {
        self.0
    }

    /// Returns the serialized size using packed long encoding.
    ///
    /// Simplified to always return 8 bytes.
    pub fn log_size(&self) -> usize {
        8
    }

    /// Writes this DatabaseId to a log buffer.
    pub fn write_to_log(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.0.to_be_bytes());
    }

    /// Reads a DatabaseId from a log buffer.
    pub fn read_from_log(buf: &[u8]) -> std::io::Result<Self> {
        if buf.len() < 8 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "buffer too short for DatabaseId",
            ));
        }
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&buf[..8]);
        Ok(DatabaseId(i64::from_be_bytes(bytes)))
    }
}

impl std::fmt::Display for DatabaseId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_and_id() {
        let db_id = DatabaseId::new(42);
        assert_eq!(db_id.id(), 42);
    }

    #[test]
    fn test_equality() {
        let id1 = DatabaseId::new(10);
        let id2 = DatabaseId::new(10);
        let id3 = DatabaseId::new(20);

        assert_eq!(id1, id2);
        assert_ne!(id1, id3);
    }

    #[test]
    fn test_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(DatabaseId::new(1));
        set.insert(DatabaseId::new(1));
        set.insert(DatabaseId::new(2));

        assert_eq!(set.len(), 2);
    }

    #[test]
    fn test_ordering() {
        let id1 = DatabaseId::new(10);
        let id2 = DatabaseId::new(20);
        let id3 = DatabaseId::new(30);

        assert!(id1 < id2);
        assert!(id2 < id3);
        assert!(id1 < id3);
    }

    #[test]
    fn test_serialization_round_trip() {
        let original = DatabaseId::new(12345);
        let mut buf = Vec::new();

        original.write_to_log(&mut buf);
        assert_eq!(buf.len(), 8);

        let deserialized = DatabaseId::read_from_log(&buf).unwrap();
        assert_eq!(original, deserialized);
    }

    #[test]
    fn test_display() {
        let db_id = DatabaseId::new(42);
        assert_eq!(format!("{}", db_id), "42");
    }

    #[test]
    fn test_log_size() {
        let db_id = DatabaseId::new(42);
        assert_eq!(db_id.log_size(), 8);
    }

    #[test]
    fn test_read_from_log_short_buffer() {
        let buf = vec![0u8; 4]; // Too short
        let result = DatabaseId::read_from_log(&buf);
        assert!(result.is_err());
    }
}
