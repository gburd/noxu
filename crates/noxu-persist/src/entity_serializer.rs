//! Entity serialization traits.
//!
//! Provides the `EntitySerializer` trait for converting entities to and from
//! byte representations. This is separate from the `Entity` trait to allow
//! different serialization formats to be plugged in.
//!
//! Port of serialization aspects of `com.sleepycat.persist.impl`.

use crate::entity::Entity;
use crate::error::Result;

/// Trait for serializing and deserializing entities to and from bytes.
///
/// This trait is separate from `Entity` so that different serialization
/// strategies can be used with the same entity type. For example, one
/// serializer might use a compact binary format while another might use
/// a human-readable format.
///
/// Port of serialization aspects of `com.sleepycat.persist.impl.PersistCatalog`.
///
/// # Example
///
/// ```
/// use noxu_persist::{Entity, EntitySerializer, PrimaryKey};
/// use noxu_persist::error::Result;
///
/// struct User { id: u64, name: String }
///
/// impl Entity for User {
///     type PrimaryKey = u64;
///     fn primary_key(&self) -> &u64 { &self.id }
///     fn entity_name() -> &'static str { "User" }
/// }
///
/// struct UserSerializer;
///
/// impl EntitySerializer<User> for UserSerializer {
///     fn serialize(&self, entity: &User) -> Result<Vec<u8>> {
///         let mut buf = Vec::new();
///         buf.extend_from_slice(&entity.id.to_be_bytes());
///         let name_bytes = entity.name.as_bytes();
///         buf.extend_from_slice(&(name_bytes.len() as u32).to_be_bytes());
///         buf.extend_from_slice(name_bytes);
///         Ok(buf)
///     }
///
///     fn deserialize(&self, bytes: &[u8]) -> Result<User> {
///         // ... deserialize from bytes ...
///         # Ok(User { id: 1, name: "test".to_string() })
///     }
/// }
/// ```
pub trait EntitySerializer<E: Entity> {
    /// Serializes an entity to a byte vector.
    ///
    /// # Errors
    /// Returns `PersistError::SerializationError` if the entity cannot be serialized.
    fn serialize(&self, entity: &E) -> Result<Vec<u8>>;

    /// Deserializes an entity from a byte slice.
    ///
    /// # Errors
    /// Returns `PersistError::SerializationError` if the bytes cannot be deserialized.
    fn deserialize(&self, bytes: &[u8]) -> Result<E>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entity::Entity;
    use crate::error::PersistError;

    #[derive(Clone, Debug, PartialEq)]
    struct Item {
        id: u32,
        value: String,
    }

    impl Entity for Item {
        type PrimaryKey = u32;

        fn primary_key(&self) -> &u32 {
            &self.id
        }

        fn entity_name() -> &'static str {
            "Item"
        }
    }

    struct ItemSerializer;

    impl EntitySerializer<Item> for ItemSerializer {
        fn serialize(&self, entity: &Item) -> Result<Vec<u8>> {
            let mut buf = Vec::new();
            buf.extend_from_slice(&entity.id.to_be_bytes());
            let val_bytes = entity.value.as_bytes();
            buf.extend_from_slice(&(val_bytes.len() as u32).to_be_bytes());
            buf.extend_from_slice(val_bytes);
            Ok(buf)
        }

        fn deserialize(&self, bytes: &[u8]) -> Result<Item> {
            if bytes.len() < 8 {
                return Err(PersistError::SerializationError(
                    "not enough bytes for Item".to_string(),
                ));
            }
            let id =
                u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
            let val_len =
                u32::from_be_bytes([bytes[4], bytes[5], bytes[6], bytes[7]])
                    as usize;
            if bytes.len() < 8 + val_len {
                return Err(PersistError::SerializationError(
                    "not enough bytes for Item value".to_string(),
                ));
            }
            let value = String::from_utf8(bytes[8..8 + val_len].to_vec())
                .map_err(|e| {
                    PersistError::SerializationError(format!(
                        "invalid UTF-8: {}",
                        e
                    ))
                })?;
            Ok(Item { id, value })
        }
    }

    #[test]
    fn test_serialize_round_trip() {
        let ser = ItemSerializer;
        let item = Item { id: 42, value: "hello".to_string() };
        let bytes = ser.serialize(&item).unwrap();
        let decoded = ser.deserialize(&bytes).unwrap();
        assert_eq!(item, decoded);
    }

    #[test]
    fn test_serialize_empty_value() {
        let ser = ItemSerializer;
        let item = Item { id: 1, value: String::new() };
        let bytes = ser.serialize(&item).unwrap();
        let decoded = ser.deserialize(&bytes).unwrap();
        assert_eq!(item, decoded);
    }

    #[test]
    fn test_deserialize_too_short() {
        let ser = ItemSerializer;
        let result = ser.deserialize(&[1, 2, 3]);
        assert!(result.is_err());
    }

    #[test]
    fn test_deserialize_truncated_value() {
        let ser = ItemSerializer;
        // id=1, value_len=100, but only 2 bytes of value
        let bytes = vec![0, 0, 0, 1, 0, 0, 0, 100, 65, 66];
        let result = ser.deserialize(&bytes);
        assert!(result.is_err());
    }

    // --- Additional branch-coverage tests ---

    #[test]
    fn test_deserialize_invalid_utf8_in_value() {
        let ser = ItemSerializer;
        // Craft bytes: id=1 (4 bytes), value_len=3 (4 bytes), then 3 invalid UTF-8 bytes.
        let mut bytes = vec![0u8, 0, 0, 1]; // id = 1
        bytes.extend_from_slice(&3u32.to_be_bytes()); // value_len = 3
        bytes.extend_from_slice(&[0xFF, 0xFE, 0xFD]); // invalid UTF-8
        let result = ser.deserialize(&bytes);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("invalid UTF-8") || err_msg.contains("serialization"), "{}", err_msg);
    }

    #[test]
    fn test_serialize_large_value() {
        let ser = ItemSerializer;
        let large_value = "x".repeat(10_000);
        let item = Item { id: 0xDEADBEEF, value: large_value.clone() };
        let bytes = ser.serialize(&item).unwrap();
        let decoded = ser.deserialize(&bytes).unwrap();
        assert_eq!(decoded.id, 0xDEADBEEF);
        assert_eq!(decoded.value, large_value);
    }

    #[test]
    fn test_serialize_max_id() {
        let ser = ItemSerializer;
        let item = Item { id: u32::MAX, value: "max".to_string() };
        let bytes = ser.serialize(&item).unwrap();
        let decoded = ser.deserialize(&bytes).unwrap();
        assert_eq!(decoded.id, u32::MAX);
        assert_eq!(decoded.value, "max");
    }

    #[test]
    fn test_serialize_id_zero_empty_value() {
        let ser = ItemSerializer;
        let item = Item { id: 0, value: String::new() };
        let bytes = ser.serialize(&item).unwrap();
        // id bytes: 4, value_len bytes: 4, value: 0 → total 8 bytes
        assert_eq!(bytes.len(), 8);
        let decoded = ser.deserialize(&bytes).unwrap();
        assert_eq!(decoded, item);
    }

    #[test]
    fn test_deserialize_exactly_8_bytes_valid() {
        let ser = ItemSerializer;
        // Exactly 8 bytes: id=0, value_len=0 → valid empty value
        let bytes = vec![0u8, 0, 0, 0, 0, 0, 0, 0];
        let result = ser.deserialize(&bytes);
        assert!(result.is_ok());
        let item = result.unwrap();
        assert_eq!(item.id, 0);
        assert_eq!(item.value, "");
    }

    #[test]
    fn test_entity_name() {
        assert_eq!(Item::entity_name(), "Item");
    }

    #[test]
    fn test_primary_key_returns_id() {
        let item = Item { id: 99, value: "test".to_string() };
        assert_eq!(*item.primary_key(), 99u32);
    }
}
