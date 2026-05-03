//! Checkpoint start log entry.
//!
//! Port of `com.sleepycat.je.recovery.CheckpointStart`.

use crate::error::Result;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::io::Cursor;
use std::time::SystemTime;

/// Marks the beginning of a checkpoint in the log.
///
/// This entry is written when a checkpoint starts. It contains the checkpoint ID,
/// the time when the checkpoint started, and information about who invoked it
/// (e.g., recovery, daemon, API call, cleaner).
///
/// Port of `com.sleepycat.je.recovery.CheckpointStart`.
#[derive(Debug, Clone)]
pub struct CheckpointStart {
    /// Checkpoint ID - unique identifier for this checkpoint.
    id: u64,
    /// Time when the checkpoint started.
    start_time: SystemTime,
    /// Who invoked this checkpoint (e.g., "recovery", "daemon", "api", "cleaner").
    invoker: String,
}

impl CheckpointStart {
    /// Creates a new checkpoint start entry with the current time.
    ///
    /// # Arguments
    /// * `id` - Unique checkpoint ID
    /// * `invoker` - String identifying who triggered this checkpoint
    pub fn new(id: u64, invoker: &str) -> Self {
        Self { id, start_time: SystemTime::now(), invoker: invoker.to_string() }
    }

    /// Creates a checkpoint start entry with a specific timestamp.
    ///
    /// Used primarily for testing and deserialization.
    pub fn with_time(id: u64, invoker: &str, start_time: SystemTime) -> Self {
        Self { id, start_time, invoker: invoker.to_string() }
    }

    /// Returns the checkpoint ID.
    pub fn get_id(&self) -> u64 {
        self.id
    }

    /// Returns the checkpoint start time.
    pub fn get_start_time(&self) -> SystemTime {
        self.start_time
    }

    /// Returns who invoked this checkpoint.
    pub fn get_invoker(&self) -> &str {
        &self.invoker
    }

    /// Returns the serialized size in bytes.
    ///
    /// Format:
    /// - id: 8 bytes (u64, big-endian)
    /// - invoker_len: 2 bytes (u16, big-endian)
    /// - invoker: variable length UTF-8 string
    /// - timestamp_secs: 8 bytes (i64, big-endian)
    /// - timestamp_nanos: 4 bytes (u32, big-endian)
    pub fn log_size(&self) -> usize {
        8 + 2 + self.invoker.len() + 8 + 4
    }

    /// Writes this entry to the log buffer.
    ///
    /// # Arguments
    /// * `buf` - Buffer to write to
    pub fn write_to_log(&self, buf: &mut Vec<u8>) -> Result<()> {
        // Write checkpoint ID
        buf.write_u64::<BigEndian>(self.id)?;

        // Write invoker string length and content
        buf.write_u16::<BigEndian>(self.invoker.len() as u16)?;
        buf.extend_from_slice(self.invoker.as_bytes());

        // Write timestamp
        let duration = self
            .start_time
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default();
        buf.write_i64::<BigEndian>(duration.as_secs() as i64)?;
        buf.write_u32::<BigEndian>(duration.subsec_nanos())?;

        Ok(())
    }

    /// Reads a checkpoint start entry from the log.
    ///
    /// # Arguments
    /// * `buf` - Buffer containing the serialized entry
    pub fn read_from_log(buf: &[u8]) -> Result<Self> {
        let mut cursor = Cursor::new(buf);

        // Read checkpoint ID
        let id = cursor.read_u64::<BigEndian>()?;

        // Read invoker string
        let invoker_len = cursor.read_u16::<BigEndian>()? as usize;
        let pos = cursor.position() as usize;
        if pos + invoker_len > buf.len() {
            return Err(crate::error::RecoveryError::InvalidCheckpoint(
                "invoker string length exceeds buffer".to_string(),
            ));
        }
        let invoker = String::from_utf8(buf[pos..pos + invoker_len].to_vec())
            .map_err(|e| {
            crate::error::RecoveryError::InvalidCheckpoint(format!(
                "invalid UTF-8: {}",
                e
            ))
        })?;
        cursor.set_position((pos + invoker_len) as u64);

        // Read timestamp
        let secs = cursor.read_i64::<BigEndian>()?;
        let nanos = cursor.read_u32::<BigEndian>()?;
        let start_time = SystemTime::UNIX_EPOCH
            + std::time::Duration::new(secs as u64, nanos);

        Ok(Self { id, start_time, invoker })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_new() {
        let ckpt = CheckpointStart::new(123, "daemon");
        assert_eq!(ckpt.get_id(), 123);
        assert_eq!(ckpt.get_invoker(), "daemon");
        assert!(ckpt.get_start_time() <= SystemTime::now());
    }

    #[test]
    fn test_with_time() {
        let time = SystemTime::UNIX_EPOCH + Duration::from_secs(1000);
        let ckpt = CheckpointStart::with_time(456, "api", time);
        assert_eq!(ckpt.get_id(), 456);
        assert_eq!(ckpt.get_invoker(), "api");
        assert_eq!(ckpt.get_start_time(), time);
    }

    #[test]
    fn test_log_size() {
        let ckpt = CheckpointStart::new(1, "daemon");
        // 8 (id) + 2 (len) + 6 (daemon) + 8 (secs) + 4 (nanos) = 28
        assert_eq!(ckpt.log_size(), 28);

        let ckpt2 = CheckpointStart::new(1, "recovery");
        // 8 + 2 + 8 + 8 + 4 = 30
        assert_eq!(ckpt2.log_size(), 30);
    }

    #[test]
    fn test_serialization_round_trip() {
        let original = CheckpointStart::new(12345, "cleaner");

        let mut buf = Vec::new();
        original.write_to_log(&mut buf).unwrap();
        assert_eq!(buf.len(), original.log_size());

        let deserialized = CheckpointStart::read_from_log(&buf).unwrap();
        assert_eq!(deserialized.get_id(), original.get_id());
        assert_eq!(deserialized.get_invoker(), original.get_invoker());

        // Compare timestamps (within 1ms tolerance for precision)
        let orig_duration = original
            .get_start_time()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap();
        let deser_duration = deserialized
            .get_start_time()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap();
        assert_eq!(orig_duration.as_secs(), deser_duration.as_secs());
        assert_eq!(orig_duration.subsec_nanos(), deser_duration.subsec_nanos());
    }

    #[test]
    fn test_serialization_various_invokers() {
        let invokers = vec!["recovery", "daemon", "api", "cleaner", "test"];

        for invoker in invokers {
            let ckpt = CheckpointStart::new(999, invoker);
            let mut buf = Vec::new();
            ckpt.write_to_log(&mut buf).unwrap();

            let restored = CheckpointStart::read_from_log(&buf).unwrap();
            assert_eq!(restored.get_invoker(), invoker);
        }
    }

    #[test]
    fn test_serialization_with_specific_time() {
        let time =
            SystemTime::UNIX_EPOCH + Duration::new(1234567890, 123456789);
        let ckpt = CheckpointStart::with_time(777, "test", time);

        let mut buf = Vec::new();
        ckpt.write_to_log(&mut buf).unwrap();

        let restored = CheckpointStart::read_from_log(&buf).unwrap();
        assert_eq!(restored.get_id(), 777);
        assert_eq!(restored.get_start_time(), time);
    }

    #[test]
    fn test_empty_invoker() {
        let ckpt = CheckpointStart::new(1, "");
        let mut buf = Vec::new();
        ckpt.write_to_log(&mut buf).unwrap();

        let restored = CheckpointStart::read_from_log(&buf).unwrap();
        assert_eq!(restored.get_invoker(), "");
    }

    #[test]
    fn test_long_invoker() {
        let long_invoker = "a".repeat(1000);
        let ckpt = CheckpointStart::new(1, &long_invoker);
        let mut buf = Vec::new();
        ckpt.write_to_log(&mut buf).unwrap();

        let restored = CheckpointStart::read_from_log(&buf).unwrap();
        assert_eq!(restored.get_invoker(), long_invoker);
    }

    #[test]
    fn test_invalid_buffer_too_short() {
        let buf = vec![0u8; 5]; // Too short for even the ID
        let result = CheckpointStart::read_from_log(&buf);
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_invoker_length() {
        let mut buf = Vec::new();
        buf.write_u64::<BigEndian>(1).unwrap(); // ID
        buf.write_u16::<BigEndian>(1000).unwrap(); // Claim 1000 bytes for invoker
        buf.extend_from_slice(b"short"); // But only provide 5 bytes

        let result = CheckpointStart::read_from_log(&buf);
        assert!(result.is_err());
    }
}
