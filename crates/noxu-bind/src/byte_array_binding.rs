//! Raw byte array binding.
//!
//! Port of `com.sleepycat.bind.ByteArrayBinding`.

use noxu_db::DatabaseEntry;

use crate::entry_binding::EntryBinding;
use crate::error::Result;

/// A simple pass-through binding for raw byte arrays (`Vec<u8>`).
///
/// This binding stores and retrieves byte arrays without any transformation.
///
/// Port of `com.sleepycat.bind.ByteArrayBinding`.
#[derive(Debug, Clone, Copy, Default)]
pub struct ByteArrayBinding;

impl ByteArrayBinding {
    /// Creates a new `ByteArrayBinding`.
    pub fn new() -> Self {
        Self
    }
}

impl EntryBinding<Vec<u8>> for ByteArrayBinding {
    fn entry_to_object(&self, entry: &DatabaseEntry) -> Result<Vec<u8>> {
        Ok(entry.data().to_vec())
    }

    fn object_to_entry(
        &self,
        object: &Vec<u8>,
        entry: &mut DatabaseEntry,
    ) -> Result<()> {
        entry.set_data(object);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_round_trip() {
        let binding = ByteArrayBinding::new();
        let data = vec![1, 2, 3, 4, 5];
        let mut entry = DatabaseEntry::new();
        binding.object_to_entry(&data, &mut entry).unwrap();
        let result = binding.entry_to_object(&entry).unwrap();
        assert_eq!(data, result);
    }

    #[test]
    fn test_empty() {
        let binding = ByteArrayBinding::new();
        let data = vec![];
        let mut entry = DatabaseEntry::new();
        binding.object_to_entry(&data, &mut entry).unwrap();
        let result = binding.entry_to_object(&entry).unwrap();
        assert_eq!(data, result);
    }
}
