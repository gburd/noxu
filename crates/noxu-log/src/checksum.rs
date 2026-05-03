//! Checksum validation utilities.
//!
//! Port of `com.sleepycat.je.log.ChecksumValidator`.
//!
//! Uses CRC32 (via crc32fast) for log entry checksum validation.

use crate::error::{LogError, Result};
use crc32fast::Hasher;
use noxu_util::Lsn;

/// Checksum validator for log entries.
///
/// Uses CRC32 for fast checksum computation and validation.
pub struct ChecksumValidator {
    hasher: Hasher,
}

impl ChecksumValidator {
    /// Creates a new checksum validator.
    pub fn new() -> Self {
        ChecksumValidator { hasher: Hasher::new() }
    }

    /// Resets the validator to compute a new checksum.
    pub fn reset(&mut self) {
        self.hasher = Hasher::new();
    }

    /// Updates the checksum with the given data.
    ///
    /// # Arguments
    /// * `buf` - The buffer containing data to include in the checksum.
    /// * `offset` - Starting offset in the buffer.
    /// * `length` - Number of bytes to process.
    pub fn update(
        &mut self,
        buf: &[u8],
        offset: usize,
        length: usize,
    ) -> Result<()> {
        if offset + length > buf.len() {
            return Err(LogError::Internal(format!(
                "Checksum update out of bounds: offset={}, length={}, buf_len={}",
                offset,
                length,
                buf.len()
            )));
        }

        self.hasher.update(&buf[offset..offset + length]);
        Ok(())
    }

    /// Updates the checksum with all data in the buffer.
    pub fn update_all(&mut self, buf: &[u8]) {
        self.hasher.update(buf);
    }

    /// Validates that the computed checksum matches the expected value.
    ///
    /// # Arguments
    /// * `expected` - The expected checksum value.
    /// * `lsn` - The LSN of the entry being validated (for error reporting).
    pub fn validate(&self, expected: u32, lsn: Lsn) -> Result<()> {
        let actual = self.hasher.clone().finalize();
        if expected != actual {
            Err(LogError::Checksum {
                lsn,
                message: format!("expected {:#x}, got {:#x}", expected, actual),
            })
        } else {
            Ok(())
        }
    }

    /// Returns the current checksum value without consuming the validator.
    pub fn value(&self) -> u32 {
        self.hasher.clone().finalize()
    }

    /// Computes a checksum for a complete buffer.
    ///
    /// Convenience method for one-shot checksum computation.
    pub fn compute(buf: &[u8]) -> u32 {
        let mut hasher = Hasher::new();
        hasher.update(buf);
        hasher.finalize()
    }

    /// Computes a checksum for a portion of a buffer.
    pub fn compute_range(buf: &[u8], offset: usize, length: usize) -> u32 {
        let mut hasher = Hasher::new();
        hasher.update(&buf[offset..offset + length]);
        hasher.finalize()
    }
}

impl Default for ChecksumValidator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_checksum_computation() {
        let data = b"Hello, Noxu DB!";
        let checksum1 = ChecksumValidator::compute(data);
        let checksum2 = ChecksumValidator::compute(data);
        assert_eq!(checksum1, checksum2);
    }

    #[test]
    fn test_incremental_update() {
        let data1 = b"Hello, ";
        let data2 = b"Noxu DB!";
        let full_data = b"Hello, Noxu DB!";

        let mut validator = ChecksumValidator::new();
        validator.update_all(data1);
        validator.update_all(data2);
        let incremental = validator.value();

        let full = ChecksumValidator::compute(full_data);
        assert_eq!(incremental, full);
    }

    #[test]
    fn test_validate_success() {
        let data = b"test data";
        let checksum = ChecksumValidator::compute(data);

        let mut validator = ChecksumValidator::new();
        validator.update_all(data);

        let lsn = Lsn::new(1, 0);
        assert!(validator.validate(checksum, lsn).is_ok());
    }

    #[test]
    fn test_validate_failure() {
        let data = b"test data";
        let mut validator = ChecksumValidator::new();
        validator.update_all(data);

        let lsn = Lsn::new(1, 0);
        let wrong_checksum = 0xDEADBEEF;
        assert!(validator.validate(wrong_checksum, lsn).is_err());
    }

    #[test]
    fn test_reset() {
        let data = b"test";
        let mut validator = ChecksumValidator::new();
        validator.update_all(data);
        let first = validator.value();

        validator.reset();
        validator.update_all(data);
        let second = validator.value();

        assert_eq!(first, second);
    }

    #[test]
    fn test_update_with_offset_and_length() {
        let data = b"XXXhelloYYY";
        let mut validator = ChecksumValidator::new();
        validator.update(&data[..], 3, 5).unwrap(); // "hello"

        let expected = ChecksumValidator::compute(b"hello");
        assert_eq!(validator.value(), expected);
    }

    #[test]
    fn test_update_out_of_bounds_returns_error() {
        let data = b"hello";
        let mut validator = ChecksumValidator::new();
        let result = validator.update(&data[..], 3, 10); // 3+10 > 5
        assert!(result.is_err());
    }

    #[test]
    fn test_compute_range() {
        let data = b"AAAbbbCCC";
        let range_checksum = ChecksumValidator::compute_range(&data[..], 3, 3); // "bbb"
        let full_checksum = ChecksumValidator::compute(b"bbb");
        assert_eq!(range_checksum, full_checksum);
    }

    #[test]
    fn test_compute_empty_data() {
        let c1 = ChecksumValidator::compute(b"");
        let c2 = ChecksumValidator::compute(b"");
        assert_eq!(c1, c2);
        // Different from non-empty data
        let c3 = ChecksumValidator::compute(b"x");
        assert_ne!(c1, c3);
    }

    #[test]
    fn test_default_equals_new() {
        let v1 = ChecksumValidator::new();
        let v2 = ChecksumValidator::default();
        // Both start fresh; feeding the same data produces the same checksum.
        let data = b"noxu";
        let mut v1 = v1;
        let mut v2 = v2;
        v1.update_all(data);
        v2.update_all(data);
        assert_eq!(v1.value(), v2.value());
    }

    #[test]
    fn test_value_not_consumed() {
        // value() should be callable multiple times without resetting state.
        let data = b"idempotent";
        let mut validator = ChecksumValidator::new();
        validator.update_all(data);
        let v1 = validator.value();
        let v2 = validator.value();
        assert_eq!(v1, v2);
    }

    #[test]
    fn test_incremental_vs_one_shot_multiple_chunks() {
        let chunks: &[&[u8]] = &[b"alpha", b"beta", b"gamma"];
        let full: Vec<u8> =
            chunks.iter().flat_map(|c| c.iter().copied()).collect();

        let mut validator = ChecksumValidator::new();
        for chunk in chunks {
            validator.update_all(chunk);
        }
        assert_eq!(validator.value(), ChecksumValidator::compute(&full));
    }
}
