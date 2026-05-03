//! Composite binding combining tuple-encoded keys with serde-serialized data.
//!
//! Port of `com.sleepycat.bind.serial.TupleSerialBinding`  -  an entity binding
//! where the key is encoded using tuple format (compact, sortable binary) and
//! the data is serialized using serde via [`super::simple_serial`].
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
use crate::entry_binding::{EntityBinding, EntryBinding};
use crate::serial::serde_binding::SerdeBinding;

/// Entity binding that uses serde for both key and data serialization.
///
/// This is a simplified version of JE's `TupleSerialBinding`. In the full
/// implementation, keys would use a dedicated tuple encoding for sort-order
/// preservation; here both key and data use the compact serde binary format
/// from [`SerdeBinding`].
///
/// The entity type `E` is split into a key part `K` and a data part `V` via
/// user-provided extraction functions.
///
/// Port of `com.sleepycat.bind.serial.TupleSerialBinding`.
///
/// # Examples
///
/// ```ignore
/// use serde::{Serialize, Deserialize};
/// use noxu_bind::serial::tuple_serde_binding::TupleSerdeBinding;
/// use noxu_bind::entry_binding::EntityBinding;
/// use noxu_db::DatabaseEntry;
///
/// #[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
/// struct Employee {
///     id: u64,
///     name: String,
///     department: String,
/// }
///
/// let binding = TupleSerdeBinding::<u64, Employee>::new(
///     |emp: &Employee| emp.id,
///     |key: u64, data: Employee| data,
/// );
/// ```
pub struct TupleSerdeBinding<K, V> {
    /// Extracts the key from the entity.
    key_extractor: Box<dyn Fn(&V) -> K + Send + Sync>,
    /// Combines key and data into an entity.
    entity_creator: Box<dyn Fn(K, V) -> V + Send + Sync>,
    _phantom: PhantomData<(K, V)>,
}

impl<K, V> TupleSerdeBinding<K, V>
where
    K: Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
{
    /// Creates a new composite binding with the given key extractor and entity creator.
    ///
    /// - `key_extractor`: extracts the key from an entity value.
    /// - `entity_creator`: reconstructs the entity from key and data.
    pub fn new<FKey, FCreate>(
        key_extractor: FKey,
        entity_creator: FCreate,
    ) -> Self
    where
        FKey: Fn(&V) -> K + Send + Sync + 'static,
        FCreate: Fn(K, V) -> V + Send + Sync + 'static,
    {
        Self {
            key_extractor: Box::new(key_extractor),
            entity_creator: Box::new(entity_creator),
            _phantom: PhantomData,
        }
    }
}

impl<K, V> std::fmt::Debug for TupleSerdeBinding<K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TupleSerdeBinding")
            .field("key_type", &std::any::type_name::<K>())
            .field("value_type", &std::any::type_name::<V>())
            .finish()
    }
}

impl<K, V> EntityBinding<V> for TupleSerdeBinding<K, V>
where
    K: Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
{
    fn entry_to_object(
        &self,
        key: &DatabaseEntry,
        data: &DatabaseEntry,
    ) -> Result<V> {
        let key_binding = SerdeBinding::<K>::new();
        let data_binding = SerdeBinding::<V>::new();
        let k = key_binding.entry_to_object(key)?;
        let v = data_binding.entry_to_object(data)?;
        Ok((self.entity_creator)(k, v))
    }

    fn object_to_key(&self, object: &V, key: &mut DatabaseEntry) -> Result<()> {
        let key_binding = SerdeBinding::<K>::new();
        let k = (self.key_extractor)(object);
        key_binding.object_to_entry(&k, key)
    }

    fn object_to_data(
        &self,
        object: &V,
        data: &mut DatabaseEntry,
    ) -> Result<()> {
        let data_binding = SerdeBinding::<V>::new();
        data_binding.object_to_entry(object, data)
    }
}

/// A simpler entity binding where key and value types are distinct and the
/// entity is the pair `(K, V)`.
///
/// This avoids requiring closure-based extraction by working directly with
/// tuples.
///
/// Port of `com.sleepycat.bind.serial.TupleSerialBinding` (simplified form).
pub struct TupleSerdeKeyDataBinding<K, V> {
    _phantom: PhantomData<(K, V)>,
}

impl<K, V> TupleSerdeKeyDataBinding<K, V> {
    /// Creates a new key-data binding.
    pub fn new() -> Self {
        Self { _phantom: PhantomData }
    }
}

impl<K, V> Default for TupleSerdeKeyDataBinding<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> Clone for TupleSerdeKeyDataBinding<K, V> {
    fn clone(&self) -> Self {
        Self::new()
    }
}

impl<K, V> std::fmt::Debug for TupleSerdeKeyDataBinding<K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TupleSerdeKeyDataBinding")
            .field("key_type", &std::any::type_name::<K>())
            .field("value_type", &std::any::type_name::<V>())
            .finish()
    }
}

impl<K, V> EntityBinding<(K, V)> for TupleSerdeKeyDataBinding<K, V>
where
    K: Serialize + DeserializeOwned + Clone,
    V: Serialize + DeserializeOwned + Clone,
{
    fn entry_to_object(
        &self,
        key: &DatabaseEntry,
        data: &DatabaseEntry,
    ) -> Result<(K, V)> {
        let key_binding = SerdeBinding::<K>::new();
        let data_binding = SerdeBinding::<V>::new();
        let k = key_binding.entry_to_object(key)?;
        let v = data_binding.entry_to_object(data)?;
        Ok((k, v))
    }

    fn object_to_key(
        &self,
        object: &(K, V),
        key: &mut DatabaseEntry,
    ) -> Result<()> {
        let key_binding = SerdeBinding::<K>::new();
        key_binding.object_to_entry(&object.0, key)
    }

    fn object_to_data(
        &self,
        object: &(K, V),
        data: &mut DatabaseEntry,
    ) -> Result<()> {
        let data_binding = SerdeBinding::<V>::new();
        data_binding.object_to_entry(&object.1, data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    struct Employee {
        id: u64,
        name: String,
        department: String,
    }

    #[test]
    fn test_tuple_serde_binding_round_trip() {
        let binding = TupleSerdeBinding::<u64, Employee>::new(
            |emp| emp.id,
            |_key, data| data,
        );

        let emp = Employee {
            id: 42,
            name: "Alice".to_string(),
            department: "Engineering".to_string(),
        };

        let mut key_entry = DatabaseEntry::new();
        let mut data_entry = DatabaseEntry::new();

        binding.object_to_key(&emp, &mut key_entry).unwrap();
        binding.object_to_data(&emp, &mut data_entry).unwrap();

        let decoded = binding.entry_to_object(&key_entry, &data_entry).unwrap();
        assert_eq!(decoded, emp);
    }

    #[test]
    fn test_tuple_serde_binding_key_extraction() {
        let binding = TupleSerdeBinding::<u64, Employee>::new(
            |emp| emp.id,
            |_key, data| data,
        );

        let emp = Employee {
            id: 99,
            name: "Bob".to_string(),
            department: "Sales".to_string(),
        };

        let mut key_entry = DatabaseEntry::new();
        binding.object_to_key(&emp, &mut key_entry).unwrap();

        let key_binding = SerdeBinding::<u64>::new();
        let key: u64 = key_binding.entry_to_object(&key_entry).unwrap();
        assert_eq!(key, 99);
    }

    #[test]
    fn test_key_data_binding_round_trip() {
        let binding = TupleSerdeKeyDataBinding::<u32, String>::new();

        let entity = (42u32, "hello".to_string());
        let mut key_entry = DatabaseEntry::new();
        let mut data_entry = DatabaseEntry::new();

        binding.object_to_key(&entity, &mut key_entry).unwrap();
        binding.object_to_data(&entity, &mut data_entry).unwrap();

        let decoded = binding.entry_to_object(&key_entry, &data_entry).unwrap();
        assert_eq!(decoded, entity);
    }

    #[test]
    fn test_key_data_binding_with_struct() {
        #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
        struct Product {
            name: String,
            price: f64,
        }

        let binding = TupleSerdeKeyDataBinding::<u64, Product>::new();

        let entity =
            (100u64, Product { name: "Widget".to_string(), price: 9.99 });

        let mut key_entry = DatabaseEntry::new();
        let mut data_entry = DatabaseEntry::new();

        binding.object_to_key(&entity, &mut key_entry).unwrap();
        binding.object_to_data(&entity, &mut data_entry).unwrap();

        let decoded = binding.entry_to_object(&key_entry, &data_entry).unwrap();
        assert_eq!(decoded, entity);
    }

    #[test]
    fn test_key_data_binding_default() {
        let binding = TupleSerdeKeyDataBinding::<u32, String>::default();
        let entity = (1u32, "test".to_string());
        let mut key_entry = DatabaseEntry::new();
        let mut data_entry = DatabaseEntry::new();
        binding.object_to_key(&entity, &mut key_entry).unwrap();
        binding.object_to_data(&entity, &mut data_entry).unwrap();
        let decoded = binding.entry_to_object(&key_entry, &data_entry).unwrap();
        assert_eq!(decoded, entity);
    }

    #[test]
    fn test_key_data_binding_clone() {
        let binding = TupleSerdeKeyDataBinding::<u32, String>::new();
        let cloned = binding.clone();
        let entity = (7u32, "clone".to_string());
        let mut key_entry = DatabaseEntry::new();
        let mut data_entry = DatabaseEntry::new();
        binding.object_to_key(&entity, &mut key_entry).unwrap();
        binding.object_to_data(&entity, &mut data_entry).unwrap();
        let decoded = cloned.entry_to_object(&key_entry, &data_entry).unwrap();
        assert_eq!(decoded, entity);
    }

    #[test]
    fn test_key_data_binding_debug() {
        let binding = TupleSerdeKeyDataBinding::<u32, String>::new();
        let debug = format!("{:?}", binding);
        assert!(debug.contains("TupleSerdeKeyDataBinding"));
    }

    #[test]
    fn test_tuple_serde_binding_debug() {
        let binding = TupleSerdeBinding::<u64, Employee>::new(
            |emp| emp.id,
            |_key, data| data,
        );
        let debug = format!("{:?}", binding);
        assert!(debug.contains("TupleSerdeBinding"));
    }

    #[test]
    fn test_key_data_with_option_value() {
        let binding = TupleSerdeKeyDataBinding::<String, Option<u64>>::new();
        let entity = ("key".to_string(), Some(42u64));
        let mut key_entry = DatabaseEntry::new();
        let mut data_entry = DatabaseEntry::new();
        binding.object_to_key(&entity, &mut key_entry).unwrap();
        binding.object_to_data(&entity, &mut data_entry).unwrap();
        let decoded = binding.entry_to_object(&key_entry, &data_entry).unwrap();
        assert_eq!(decoded, entity);
    }

    #[test]
    fn test_key_data_with_vec_value() {
        let binding = TupleSerdeKeyDataBinding::<u32, Vec<String>>::new();
        let entity = (1u32, vec!["a".to_string(), "b".to_string()]);
        let mut key_entry = DatabaseEntry::new();
        let mut data_entry = DatabaseEntry::new();
        binding.object_to_key(&entity, &mut key_entry).unwrap();
        binding.object_to_data(&entity, &mut data_entry).unwrap();
        let decoded = binding.entry_to_object(&key_entry, &data_entry).unwrap();
        assert_eq!(decoded, entity);
    }

    #[test]
    fn test_entity_creator_transforms() {
        // Test that entity_creator can transform the reconstructed entity
        let binding = TupleSerdeBinding::<u64, Employee>::new(
            |emp| emp.id,
            |key, mut data| {
                // Override the id from the key (simulating key->entity injection)
                data.id = key;
                data
            },
        );

        let emp = Employee {
            id: 42,
            name: "Test".to_string(),
            department: "Eng".to_string(),
        };

        let mut key_entry = DatabaseEntry::new();
        let mut data_entry = DatabaseEntry::new();
        binding.object_to_key(&emp, &mut key_entry).unwrap();
        binding.object_to_data(&emp, &mut data_entry).unwrap();

        let decoded = binding.entry_to_object(&key_entry, &data_entry).unwrap();
        assert_eq!(decoded.id, 42);
        assert_eq!(decoded.name, "Test");
    }
}
