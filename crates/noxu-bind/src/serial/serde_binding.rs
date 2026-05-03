//! Serde-based entry binding for Noxu DB.
//!
//! Port of `com.sleepycat.bind.serial.SerialBinding` adapted for Rust's serde
//! framework. Instead of Java serialization, this uses a compact binary format
//! implemented in [`super::simple_serial`].
//!
//! ## Required dependencies (to be added to Cargo.toml)
//!
//! ```toml
//! serde = { version = "1", features = ["derive"] }
//! ```

use std::marker::PhantomData;

use serde::Serialize;
use serde::de::DeserializeOwned;

use noxu_db::DatabaseEntry;

use crate::Result;
use crate::entry_binding::EntryBinding;
use crate::serial::simple_serial;

/// Binding that uses a compact binary format via serde for serialization.
///
/// Any type implementing `Serialize + DeserializeOwned` can be stored in and
/// retrieved from database entries using this binding.
///
/// Port of `com.sleepycat.bind.serial.SerialBinding` adapted for serde.
///
/// # Examples
///
/// ```ignore
/// use serde::{Serialize, Deserialize};
/// use noxu_bind::serial::serde_binding::SerdeBinding;
/// use noxu_bind::entry_binding::EntryBinding;
/// use noxu_db::DatabaseEntry;
///
/// #[derive(Serialize, Deserialize, Debug, PartialEq)]
/// struct Person {
///     name: String,
///     age: u32,
/// }
///
/// let binding = SerdeBinding::<Person>::new();
/// let person = Person { name: "Alice".into(), age: 30 };
///
/// let mut entry = DatabaseEntry::new();
/// binding.object_to_entry(&person, &mut entry).unwrap();
///
/// let decoded = binding.entry_to_object(&entry).unwrap();
/// assert_eq!(decoded, person);
/// ```
pub struct SerdeBinding<T> {
    _phantom: PhantomData<T>,
}

impl<T> SerdeBinding<T> {
    /// Creates a new serde-based binding.
    pub fn new() -> Self {
        Self { _phantom: PhantomData }
    }
}

impl<T> Default for SerdeBinding<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Clone for SerdeBinding<T> {
    fn clone(&self) -> Self {
        Self::new()
    }
}

impl<T> std::fmt::Debug for SerdeBinding<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SerdeBinding")
            .field("type", &std::any::type_name::<T>())
            .finish()
    }
}

impl<T: Serialize + DeserializeOwned> EntryBinding<T> for SerdeBinding<T> {
    fn entry_to_object(&self, entry: &DatabaseEntry) -> Result<T> {
        let data = entry.data();
        simple_serial::from_bytes(data)
    }

    fn object_to_entry(
        &self,
        object: &T,
        entry: &mut DatabaseEntry,
    ) -> Result<()> {
        let bytes = simple_serial::to_bytes(object)?;
        entry.set_data_vec(bytes);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[test]
    fn test_u32_round_trip() {
        let binding = SerdeBinding::<u32>::new();
        let mut entry = DatabaseEntry::new();
        binding.object_to_entry(&42u32, &mut entry).unwrap();
        assert_eq!(binding.entry_to_object(&entry).unwrap(), 42u32);
    }

    #[test]
    fn test_string_round_trip() {
        let binding = SerdeBinding::<String>::new();
        let mut entry = DatabaseEntry::new();
        let s = "hello world".to_string();
        binding.object_to_entry(&s, &mut entry).unwrap();
        assert_eq!(binding.entry_to_object(&entry).unwrap(), s);
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct TestRecord {
        id: u64,
        name: String,
        active: bool,
    }

    #[test]
    fn test_struct_round_trip() {
        let binding = SerdeBinding::<TestRecord>::new();
        let record =
            TestRecord { id: 12345, name: "test".to_string(), active: true };
        let mut entry = DatabaseEntry::new();
        binding.object_to_entry(&record, &mut entry).unwrap();
        assert_eq!(binding.entry_to_object(&entry).unwrap(), record);
    }

    #[test]
    fn test_vec_round_trip() {
        let binding = SerdeBinding::<Vec<u32>>::new();
        let v = vec![1, 2, 3, 4, 5];
        let mut entry = DatabaseEntry::new();
        binding.object_to_entry(&v, &mut entry).unwrap();
        assert_eq!(binding.entry_to_object(&entry).unwrap(), v);
    }

    #[test]
    fn test_option_round_trip() {
        let binding = SerdeBinding::<Option<String>>::new();
        let mut entry = DatabaseEntry::new();

        binding.object_to_entry(&Some("yes".to_string()), &mut entry).unwrap();
        assert_eq!(
            binding.entry_to_object(&entry).unwrap(),
            Some("yes".to_string())
        );

        binding.object_to_entry(&None, &mut entry).unwrap();
        assert_eq!(binding.entry_to_object(&entry).unwrap(), None);
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    enum Status {
        Active,
        Inactive,
        Pending(String),
    }

    #[test]
    fn test_enum_round_trip() {
        let binding = SerdeBinding::<Status>::new();
        let mut entry = DatabaseEntry::new();

        binding.object_to_entry(&Status::Active, &mut entry).unwrap();
        assert_eq!(binding.entry_to_object(&entry).unwrap(), Status::Active);

        binding
            .object_to_entry(&Status::Pending("review".to_string()), &mut entry)
            .unwrap();
        assert_eq!(
            binding.entry_to_object(&entry).unwrap(),
            Status::Pending("review".to_string())
        );
    }

    #[test]
    fn test_default() {
        let binding = SerdeBinding::<u32>::default();
        let mut entry = DatabaseEntry::new();
        binding.object_to_entry(&7u32, &mut entry).unwrap();
        assert_eq!(binding.entry_to_object(&entry).unwrap(), 7u32);
    }

    #[test]
    fn test_clone() {
        let binding = SerdeBinding::<u32>::new();
        let cloned = binding.clone();
        let mut entry = DatabaseEntry::new();
        binding.object_to_entry(&99u32, &mut entry).unwrap();
        assert_eq!(cloned.entry_to_object(&entry).unwrap(), 99u32);
    }

    #[test]
    fn test_debug() {
        let binding = SerdeBinding::<u32>::new();
        let debug = format!("{:?}", binding);
        assert!(debug.contains("SerdeBinding"));
    }

    #[test]
    fn test_empty_entry_error() {
        let binding = SerdeBinding::<u32>::new();
        let entry = DatabaseEntry::new();
        // Empty entry has no data  -  should fail on deserialization
        assert!(binding.entry_to_object(&entry).is_err());
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Nested {
        inner: TestRecord,
        tags: Vec<String>,
    }

    #[test]
    fn test_nested_struct_round_trip() {
        let binding = SerdeBinding::<Nested>::new();
        let nested = Nested {
            inner: TestRecord {
                id: 1,
                name: "nested".to_string(),
                active: false,
            },
            tags: vec!["a".to_string(), "b".to_string()],
        };
        let mut entry = DatabaseEntry::new();
        binding.object_to_entry(&nested, &mut entry).unwrap();
        assert_eq!(binding.entry_to_object(&entry).unwrap(), nested);
    }

    #[test]
    fn test_tuple_round_trip() {
        let binding = SerdeBinding::<(u32, String, bool)>::new();
        let val = (42u32, "hello".to_string(), true);
        let mut entry = DatabaseEntry::new();
        binding.object_to_entry(&val, &mut entry).unwrap();
        assert_eq!(binding.entry_to_object(&entry).unwrap(), val);
    }

    #[test]
    fn test_entry_data_is_set() {
        let binding = SerdeBinding::<u32>::new();
        let mut entry = DatabaseEntry::new();
        assert!(entry.is_empty());
        binding.object_to_entry(&42u32, &mut entry).unwrap();
        assert!(!entry.is_empty());
        assert!(entry.get_data().is_some());
    }
}
