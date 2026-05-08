//! Loggable trait for log entry serialization.
//!
//! `BasicVersionedWriteLoggable`.
//!
//! Classes that implement Loggable know how to serialize and deserialize
//! themselves to/from the log format.

use crate::error::Result;

/// A type that can be serialized to and deserialized from the log.
///
/// This is the core trait for log entry serialization. All log entries and
/// loggable objects must implement this trait.
pub trait Loggable: Sized {
    /// Returns the number of bytes needed to serialize this object.
    fn log_size(&self) -> usize;

    /// Serializes this object into the provided buffer.
    ///
    /// The buffer must have at least `log_size()` bytes available.
    fn write_to_log(&self, buf: &mut Vec<u8>) -> Result<()>;

    /// Deserializes this object from the provided buffer.
    ///
    /// # Arguments
    /// * `buf` - The source buffer containing the serialized data.
    /// * `version` - The log version that was used to write this entry.
    fn read_from_log(buf: &[u8], version: u8) -> Result<Self>;

    /// Returns the transaction ID embedded within this loggable object.
    ///
    /// Objects that have no transaction ID should return 0.
    fn transaction_id(&self) -> u64 {
        0
    }

    /// Returns a debug representation of this object for log dumping.
    ///
    /// Used by log analysis and debugging tools.
    fn dump_log(&self, _verbose: bool) -> String {
        format!("{:?}", std::any::type_name::<Self>())
    }
}

/// Extension of Loggable that supports writing in multiple log versions.
///
/// 
///
/// Types that implement this trait can serialize themselves in earlier log
/// formats to support replication during upgrades where the master has been
/// upgraded and replicas have not.
pub trait VersionedWriteLoggable: Loggable {
    /// Returns the log version of the most recent format change for this
    /// loggable item.
    fn last_format_change(&self) -> u8;

    /// Returns the number of bytes needed to serialize this object in the
    /// format for the specified log version.
    ///
    /// # Arguments
    /// * `log_version` - The target log version.
    /// * `for_replication` - Whether the entry will be sent over the wire
    ///   (replication stream) rather than written to the log.
    fn log_size_versioned(
        &self,
        log_version: u8,
        for_replication: bool,
    ) -> usize;

    /// Serializes this object into the provided buffer in the format for
    /// the specified log version.
    ///
    /// # Arguments
    /// * `buf` - The destination buffer.
    /// * `log_version` - The target log version.
    /// * `for_replication` - Whether the entry will be sent over the wire.
    fn write_to_log_versioned(
        &self,
        buf: &mut Vec<u8>,
        log_version: u8,
        for_replication: bool,
    ) -> Result<()>;

    /// Returns whether this format has a variant optimized for replication.
    fn has_replication_format(&self) -> bool {
        false
    }

    /// Returns whether it is worthwhile to materialize and re-serialize a
    /// log entry in a format optimized for replication.
    ///
    /// Implementations should check efficiently without instantiating the
    /// log entry object.
    fn is_replication_format_worthwhile(
        &self,
        _src_version: u8,
        _dest_version: u8,
    ) -> bool {
        false
    }
}

/// Basic implementation of VersionedWriteLoggable that writes in a single
/// format by default.
///
/// 
///
/// Types can implement this trait to get default single-format behavior,
/// then override specific methods to support multiple versions.
pub trait BasicVersionedWriteLoggable:
    Loggable + VersionedWriteLoggable
{
    /// The current log version used for writing.
    const CURRENT_LOG_VERSION: u8 = 15; // LOG_VERSION from LogEntryType

    /// Default implementation that delegates to the unversioned method.
    fn log_size_versioned_default(
        &self,
        _log_version: u8,
        _for_replication: bool,
    ) -> usize {
        self.log_size()
    }

    /// Default implementation that delegates to the unversioned method.
    fn write_to_log_versioned_default(
        &self,
        buf: &mut Vec<u8>,
        _log_version: u8,
        _for_replication: bool,
    ) -> Result<()> {
        self.write_to_log(buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Simple test struct
    #[derive(Debug, Clone, PartialEq)]
    struct TestEntry {
        value: u32,
    }

    impl Loggable for TestEntry {
        fn log_size(&self) -> usize {
            4
        }

        fn write_to_log(&self, buf: &mut Vec<u8>) -> Result<()> {
            buf.extend_from_slice(&self.value.to_le_bytes());
            Ok(())
        }

        fn read_from_log(buf: &[u8], _version: u8) -> Result<Self> {
            if buf.len() < 4 {
                return Err(crate::error::NoxuLogError::UnexpectedEof {
                    lsn: noxu_util::NULL_LSN,
                    message: "not enough data".to_string(),
                });
            }
            let value = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
            Ok(TestEntry { value })
        }
    }

    #[test]
    fn test_loggable_roundtrip() {
        let entry = TestEntry { value: 0x12345678 };
        let mut buf = Vec::new();
        entry.write_to_log(&mut buf).unwrap();
        assert_eq!(buf.len(), entry.log_size());

        let decoded = TestEntry::read_from_log(&buf, 15).unwrap();
        assert_eq!(entry, decoded);
    }

    #[test]
    fn test_loggable_zero_value() {
        let entry = TestEntry { value: 0 };
        let mut buf = Vec::new();
        entry.write_to_log(&mut buf).unwrap();
        assert_eq!(buf, vec![0, 0, 0, 0]);
        let decoded = TestEntry::read_from_log(&buf, 1).unwrap();
        assert_eq!(decoded.value, 0);
    }

    #[test]
    fn test_loggable_max_value() {
        let entry = TestEntry { value: u32::MAX };
        let mut buf = Vec::new();
        entry.write_to_log(&mut buf).unwrap();
        let decoded = TestEntry::read_from_log(&buf, 1).unwrap();
        assert_eq!(decoded.value, u32::MAX);
    }

    #[test]
    fn test_loggable_log_size_matches_written_bytes() {
        for value in [0u32, 1, 100, 0xDEAD_BEEF, u32::MAX] {
            let entry = TestEntry { value };
            let mut buf = Vec::new();
            entry.write_to_log(&mut buf).unwrap();
            assert_eq!(buf.len(), entry.log_size());
        }
    }

    #[test]
    fn test_loggable_read_from_log_too_short() {
        let result = TestEntry::read_from_log(&[1, 2, 3], 1);
        assert!(result.is_err());
    }

    #[test]
    fn test_loggable_read_from_log_empty() {
        let result = TestEntry::read_from_log(&[], 1);
        assert!(result.is_err());
    }

    #[test]
    fn test_loggable_read_from_log_extra_bytes_ignored() {
        let entry = TestEntry { value: 42 };
        let mut buf = Vec::new();
        entry.write_to_log(&mut buf).unwrap();
        buf.extend_from_slice(&[0xFF, 0xFF]); // extra bytes
        let decoded = TestEntry::read_from_log(&buf, 1).unwrap();
        assert_eq!(decoded.value, 42);
    }

    #[test]
    fn test_loggable_transaction_id_default() {
        let entry = TestEntry { value: 123 };
        assert_eq!(entry.transaction_id(), 0);
    }

    #[test]
    fn test_loggable_dump_log_default() {
        let entry = TestEntry { value: 1 };
        let s = entry.dump_log(false);
        // Should contain the type name
        assert!(!s.is_empty());
    }

    #[test]
    fn test_loggable_dump_log_verbose() {
        let entry = TestEntry { value: 1 };
        let s1 = entry.dump_log(false);
        let s2 = entry.dump_log(true);
        // Default implementation ignores verbose flag
        assert_eq!(s1, s2);
    }

    // A type that implements VersionedWriteLoggable with a custom format change.
    #[derive(Debug, Clone, PartialEq)]
    struct VersionedEntry {
        x: u8,
    }

    impl Loggable for VersionedEntry {
        fn log_size(&self) -> usize {
            1
        }

        fn write_to_log(&self, buf: &mut Vec<u8>) -> Result<()> {
            buf.push(self.x);
            Ok(())
        }

        fn read_from_log(buf: &[u8], _version: u8) -> Result<Self> {
            if buf.is_empty() {
                return Err(crate::error::NoxuLogError::UnexpectedEof {
                    lsn: noxu_util::NULL_LSN,
                    message: "empty".to_string(),
                });
            }
            Ok(VersionedEntry { x: buf[0] })
        }
    }

    impl VersionedWriteLoggable for VersionedEntry {
        fn last_format_change(&self) -> u8 {
            10
        }

        fn log_size_versioned(
            &self,
            _log_version: u8,
            _for_replication: bool,
        ) -> usize {
            self.log_size()
        }

        fn write_to_log_versioned(
            &self,
            buf: &mut Vec<u8>,
            _log_version: u8,
            _for_replication: bool,
        ) -> Result<()> {
            self.write_to_log(buf)
        }
    }

    impl BasicVersionedWriteLoggable for VersionedEntry {}

    #[test]
    fn test_versioned_write_loggable_last_format_change() {
        let e = VersionedEntry { x: 5 };
        assert_eq!(e.last_format_change(), 10);
    }

    #[test]
    fn test_versioned_write_loggable_log_size_versioned() {
        let e = VersionedEntry { x: 5 };
        assert_eq!(e.log_size_versioned(15, false), 1);
        assert_eq!(e.log_size_versioned(1, true), 1);
    }

    #[test]
    fn test_versioned_write_loggable_write_versioned() {
        let e = VersionedEntry { x: 0xAB };
        let mut buf = Vec::new();
        e.write_to_log_versioned(&mut buf, 15, false).unwrap();
        assert_eq!(buf, vec![0xAB]);
    }

    #[test]
    fn test_versioned_write_loggable_has_replication_format_default() {
        let e = VersionedEntry { x: 1 };
        assert!(!e.has_replication_format());
    }

    #[test]
    fn test_versioned_write_loggable_is_replication_format_worthwhile_default() {
        let e = VersionedEntry { x: 1 };
        assert!(!e.is_replication_format_worthwhile(1, 15));
    }

    #[test]
    fn test_basic_versioned_loggable_log_size_default() {
        let e = VersionedEntry { x: 7 };
        assert_eq!(e.log_size_versioned_default(15, false), 1);
    }

    #[test]
    fn test_basic_versioned_loggable_write_default() {
        let e = VersionedEntry { x: 0x99 };
        let mut buf = Vec::new();
        e.write_to_log_versioned_default(&mut buf, 15, false).unwrap();
        assert_eq!(buf, vec![0x99]);
    }
}
