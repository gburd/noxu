//! Record number binding.
//!

use crate::db::DatabaseEntry;

use crate::bind::entry_binding::EntryBinding;
use crate::bind::error::{BindError, Result};

/// A binding for u64 record numbers stored as big-endian 8-byte arrays.
///
///
#[derive(Debug, Clone, Copy, Default)]
pub struct RecordNumberBinding;

impl RecordNumberBinding {
    /// Creates a new `RecordNumberBinding`.
    pub fn new() -> Self {
        Self
    }

    /// Converts a `DatabaseEntry` to a record number (u64).
    pub fn entry_to_record_number(entry: &DatabaseEntry) -> Result<u64> {
        let data = entry.data();
        if data.len() < 8 {
            return Err(BindError::BufferUnderflow {
                needed: 8,
                available: data.len(),
            });
        }
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&data[..8]);
        Ok(u64::from_be_bytes(bytes))
    }

    /// Converts a record number (u64) to a `DatabaseEntry`.
    pub fn record_number_to_entry(number: u64, entry: &mut DatabaseEntry) {
        entry.set_data(&number.to_be_bytes());
    }
}

impl EntryBinding<u64> for RecordNumberBinding {
    fn entry_to_object(&self, entry: &DatabaseEntry) -> Result<u64> {
        Self::entry_to_record_number(entry)
    }

    fn object_to_entry(
        &self,
        object: &u64,
        entry: &mut DatabaseEntry,
    ) -> Result<()> {
        Self::record_number_to_entry(*object, entry);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_round_trip() {
        let binding = RecordNumberBinding::new();
        let number = 12345u64;
        let mut entry = DatabaseEntry::new();
        binding.object_to_entry(&number, &mut entry).unwrap();
        let result = binding.entry_to_object(&entry).unwrap();
        assert_eq!(number, result);
    }

    #[test]
    fn test_zero() {
        let binding = RecordNumberBinding::new();
        let mut entry = DatabaseEntry::new();
        binding.object_to_entry(&0u64, &mut entry).unwrap();
        assert_eq!(0u64, binding.entry_to_object(&entry).unwrap());
    }

    #[test]
    fn test_max() {
        let binding = RecordNumberBinding::new();
        let mut entry = DatabaseEntry::new();
        binding.object_to_entry(&u64::MAX, &mut entry).unwrap();
        assert_eq!(u64::MAX, binding.entry_to_object(&entry).unwrap());
    }

    #[test]
    fn test_underflow() {
        let binding = RecordNumberBinding::new();
        let entry = DatabaseEntry::from_bytes(&[1, 2, 3]);
        let result = binding.entry_to_object(&entry);
        assert!(result.is_err());
    }
}
