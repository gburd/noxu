//! Composite binding combining sort-preserving tuple-encoded keys with
//! serde-serialized data.
//!
//! Keys are encoded using the `SortKey` trait, which produces a fixed-width
//! big-endian representation for integers (with sign-bit flipping for signed
//! types) and null-escaped, null-terminated sequences for strings and byte
//! slices. This encoding is sort-preserving: lexicographic byte comparison of
//! encoded keys matches the natural `Ord` ordering of the original values.
//!
//! Data (the non-key payload) is serialized using serde via the compact
//! binary encoding from [`super::simple_serial`]. Data encoding does not need
//! to be sort-preserving because the B-tree only compares keys.

use std::marker::PhantomData;

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::db::DatabaseEntry;

use crate::bind::Result;
use crate::bind::entry_binding::{EntityBinding, EntryBinding};
use crate::bind::serial::serde_binding::SerdeBinding;
use crate::bind::tuple::sort_key::SortKey;
use crate::bind::tuple::{TupleInput, TupleOutput};

/// Entity binding that uses sort-preserving tuple encoding for keys and serde
/// binary encoding for data.
///
/// The key type `K` must implement `SortKey`, which guarantees that the
/// byte-wise order of encoded keys matches the `Ord` order of the original
/// values. This makes range scans, `get_next`, `get_prev`, and sorted map
/// operations correct without requiring a custom comparator.
///
/// The entity type `V` is split into a key `K` and a data payload `V` via
/// user-provided extraction functions.
///
/// # Examples
///
/// ```ignore
/// use serde::{Serialize, Deserialize};
/// use crate::bind::serial::tuple_serde_binding::TupleSerdeBinding;
/// use crate::bind::entry_binding::EntityBinding;
/// use crate::db::DatabaseEntry;
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
    K: SortKey,
    V: Serialize + DeserializeOwned,
{
    /// Creates a new composite binding with the given key extractor and entity creator.
    ///
    /// - `key_extractor`: extracts the sort key from an entity value.
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
    K: SortKey,
    V: Serialize + DeserializeOwned,
{
    fn entry_to_object(
        &self,
        key: &DatabaseEntry,
        data: &DatabaseEntry,
    ) -> Result<V> {
        let mut inp = TupleInput::new(key.data());
        let k = K::decode_sort_key(&mut inp)?;
        let data_binding = SerdeBinding::<V>::new();
        let v = data_binding.entry_to_object(data)?;
        Ok((self.entity_creator)(k, v))
    }

    fn object_to_key(&self, object: &V, key: &mut DatabaseEntry) -> Result<()> {
        let mut out = TupleOutput::new();
        let k = (self.key_extractor)(object);
        k.encode_sort_key(&mut out);
        key.set_data_vec(out.into_vec());
        Ok(())
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
    K: SortKey + Clone,
    V: Serialize + DeserializeOwned + Clone,
{
    fn entry_to_object(
        &self,
        key: &DatabaseEntry,
        data: &DatabaseEntry,
    ) -> Result<(K, V)> {
        let mut inp = TupleInput::new(key.data());
        let k = K::decode_sort_key(&mut inp)?;
        let data_binding = SerdeBinding::<V>::new();
        let v = data_binding.entry_to_object(data)?;
        Ok((k, v))
    }

    fn object_to_key(
        &self,
        object: &(K, V),
        key: &mut DatabaseEntry,
    ) -> Result<()> {
        let mut out = TupleOutput::new();
        object.0.encode_sort_key(&mut out);
        key.set_data_vec(out.into_vec());
        Ok(())
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

        // Decode using TupleInput (sort-preserving big-endian encoding).
        let mut inp = TupleInput::new(key_entry.data());
        let key: u64 = u64::decode_sort_key(&mut inp).unwrap();
        assert_eq!(key, 99);
    }

    /// Verify that encoded u64 keys are exactly 8 bytes (fixed-width big-endian).
    #[test]
    fn test_u64_key_is_8_bytes_fixed_width() {
        let binding = TupleSerdeBinding::<u64, Employee>::new(
            |emp| emp.id,
            |_key, data| data,
        );
        for id in [0u64, 1, 2, 10, 100, u64::MAX] {
            let emp =
                Employee { id, name: String::new(), department: String::new() };
            let mut key_entry = DatabaseEntry::new();
            binding.object_to_key(&emp, &mut key_entry).unwrap();
            assert_eq!(
                key_entry.data().len(),
                8,
                "u64 key must be 8 bytes (id={})",
                id
            );
        }
    }

    /// The primary correctness guarantee: lexicographic byte order of encoded
    /// u64 keys matches numeric order.
    #[test]
    fn test_u64_key_sort_order_preserved() {
        let binding = TupleSerdeBinding::<u64, Employee>::new(
            |emp| emp.id,
            |_key, data| data,
        );

        let key_bytes = |id: u64| {
            let emp =
                Employee { id, name: String::new(), department: String::new() };
            let mut key_entry = DatabaseEntry::new();
            binding.object_to_key(&emp, &mut key_entry).unwrap();
            key_entry.get_data().unwrap().to_vec()
        };

        let b0 = key_bytes(0);
        let b1 = key_bytes(1);
        let b2 = key_bytes(2);
        let b10 = key_bytes(10);
        let bmax = key_bytes(u64::MAX);

        assert!(b0 < b1, "0 < 1");
        assert!(b1 < b2, "1 < 2");
        assert!(b2 < b10, "2 < 10");
        assert!(b10 < bmax, "10 < MAX");
    }

    /// i64 keys should sort with negatives before zero before positives.
    #[test]
    fn test_i64_key_sort_order_preserved() {
        let binding = TupleSerdeBinding::<i64, Employee>::new(
            |emp| emp.id as i64,
            |_key, data| data,
        );

        let key_bytes = |id: i64| {
            let emp = Employee {
                id: id as u64,
                name: String::new(),
                department: String::new(),
            };
            let mut key_entry = DatabaseEntry::new();
            binding.object_to_key(&emp, &mut key_entry).unwrap();
            key_entry.get_data().unwrap().to_vec()
        };

        let vals = [i64::MIN, -1000i64, -1, 0, 1, 1000, i64::MAX];
        for w in vals.windows(2) {
            assert!(
                key_bytes(w[0]) < key_bytes(w[1]),
                "i64 sort order: {} should be < {}",
                w[0],
                w[1]
            );
        }
    }

    /// String keys sort lexicographically.
    #[test]
    fn test_string_key_sort_order_preserved() {
        let binding = TupleSerdeBinding::<String, Employee>::new(
            |emp| emp.name.clone(),
            |_key, data| data,
        );

        let key_bytes = |name: &str| {
            let emp = Employee {
                id: 0,
                name: name.to_string(),
                department: String::new(),
            };
            let mut key_entry = DatabaseEntry::new();
            binding.object_to_key(&emp, &mut key_entry).unwrap();
            key_entry.get_data().unwrap().to_vec()
        };

        assert!(key_bytes("a") < key_bytes("b"));
        assert!(key_bytes("abc") < key_bytes("abd"));
        assert!(key_bytes("a") < key_bytes("aa"));
        assert!(key_bytes("") < key_bytes("a"));
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

    /// u32 keys in TupleSerdeKeyDataBinding sort correctly.
    #[test]
    fn test_key_data_binding_u32_sort_order() {
        let binding = TupleSerdeKeyDataBinding::<u32, String>::new();

        let key_bytes = |k: u32| {
            let entity = (k, String::new());
            let mut key_entry = DatabaseEntry::new();
            binding.object_to_key(&entity, &mut key_entry).unwrap();
            key_entry.get_data().unwrap().to_vec()
        };

        let vals = [0u32, 1, 2, 10, 100, 1000, u32::MAX];
        for w in vals.windows(2) {
            assert!(
                key_bytes(w[0]) < key_bytes(w[1]),
                "{} should sort before {}",
                w[0],
                w[1]
            );
        }
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
        let binding = TupleSerdeBinding::<u64, Employee>::new(
            |emp| emp.id,
            |key, mut data| {
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

    /// Verify that u32 key bytes are exactly 4 bytes fixed-width big-endian.
    #[test]
    fn test_u32_key_is_4_bytes_fixed_width() {
        let binding = TupleSerdeKeyDataBinding::<u32, String>::new();
        for k in [0u32, 1, 255, 256, u32::MAX] {
            let entity = (k, String::new());
            let mut key_entry = DatabaseEntry::new();
            binding.object_to_key(&entity, &mut key_entry).unwrap();
            assert_eq!(
                key_entry.data().len(),
                4,
                "u32 key must be 4 bytes (k={})",
                k
            );
        }
    }
}
