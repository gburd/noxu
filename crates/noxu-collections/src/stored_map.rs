//! Map view of a database.
//!
//! Port of `com.sleepycat.collections.StoredMap`.

use crate::error::{CollectionError, Result};
use crate::stored_iterator::{
    StoredIterator, StoredKeyIterator, StoredValueIterator,
};
use noxu_db::{Database, DatabaseEntry, OperationStatus};
use std::collections::BTreeSet;
use std::sync::Mutex;

/// A map-like view of a database.
///
/// Port of `com.sleepycat.collections.StoredMap`.
///
/// Provides a familiar map interface over a Noxu DB database. Keys and
/// values are raw byte vectors (`Vec<u8>`). Records can be inserted,
/// retrieved, removed, and iterated.
///
/// The `StoredMap` maintains an internal key index (a `BTreeSet`) that
/// tracks all keys known to this view. This index is populated when
/// records are inserted via `put()` and updated on `remove()`. For
/// databases that already contain data, call `register_key()` or use
/// `contains_key()` to populate the index.
///
/// # Example
/// ```ignore
/// use noxu_collections::StoredMap;
///
/// let map = StoredMap::new(&db, false);
/// map.put(b"key1", b"value1").unwrap();
/// let value = map.get(b"key1").unwrap();
/// assert_eq!(value, Some(b"value1".to_vec()));
/// ```
pub struct StoredMap<'db> {
    /// Reference to the underlying database.
    db: &'db Database,
    /// Whether this map view is read-only.
    read_only: bool,
    /// Internal sorted key index for iteration support.
    /// Since the cursor API does not expose keys, we maintain our own
    /// sorted key set to support iteration.
    key_index: Mutex<BTreeSet<Vec<u8>>>,
}

impl<'db> StoredMap<'db> {
    /// Creates a new map view of the given database.
    ///
    /// # Arguments
    /// * `db` - The database to provide a map view over
    /// * `read_only` - If true, write operations will return `CollectionError::ReadOnly`
    pub fn new(db: &'db Database, read_only: bool) -> Self {
        StoredMap { db, read_only, key_index: Mutex::new(BTreeSet::new()) }
    }

    /// Retrieves the value associated with the given key.
    ///
    /// Returns `None` if the key is not found in the database.
    /// On success, the key is registered in the internal key index.
    ///
    /// # Arguments
    /// * `key` - The key to look up
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let key_entry = DatabaseEntry::from_bytes(key);
        let mut data_entry = DatabaseEntry::new();

        match self.db.get(None, &key_entry, &mut data_entry)? {
            OperationStatus::Success => {
                // Register key in our index since it exists
                self.key_index.lock().unwrap().insert(key.to_vec());
                Ok(Some(
                    data_entry
                        .get_data()
                        .map(|d| d.to_vec())
                        .unwrap_or_default(),
                ))
            }
            _ => Ok(None),
        }
    }

    /// Inserts or updates a key-value pair.
    ///
    /// Returns the previous value if the key was already present,
    /// or `None` if this is a new key.
    ///
    /// # Arguments
    /// * `key` - The key to insert/update
    /// * `value` - The value to store
    ///
    /// # Errors
    /// Returns `CollectionError::ReadOnly` if this map is read-only.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<Option<Vec<u8>>> {
        if self.read_only {
            return Err(CollectionError::ReadOnly);
        }

        // Try to get the old value first
        let old_value = self.get(key)?;

        let key_entry = DatabaseEntry::from_bytes(key);
        let data_entry = DatabaseEntry::from_bytes(value);

        self.db.put(None, &key_entry, &data_entry)?;

        // Register key in our index
        self.key_index.lock().unwrap().insert(key.to_vec());

        Ok(old_value)
    }

    /// Removes a key-value pair from the database.
    ///
    /// Returns the previous value if the key was present,
    /// or `None` if the key was not found.
    ///
    /// # Arguments
    /// * `key` - The key to remove
    ///
    /// # Errors
    /// Returns `CollectionError::ReadOnly` if this map is read-only.
    pub fn remove(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if self.read_only {
            return Err(CollectionError::ReadOnly);
        }

        // Get the old value first
        let old_value = self.get(key)?;

        let key_entry = DatabaseEntry::from_bytes(key);
        self.db.delete(None, &key_entry)?;

        // Remove from our key index
        self.key_index.lock().unwrap().remove(key);

        Ok(old_value)
    }

    /// Tests whether the given key exists in the database.
    ///
    /// If the key exists, it is registered in the internal key index.
    ///
    /// # Arguments
    /// * `key` - The key to check
    pub fn contains_key(&self, key: &[u8]) -> Result<bool> {
        let key_entry = DatabaseEntry::from_bytes(key);
        let mut data_entry = DatabaseEntry::new();

        match self.db.get(None, &key_entry, &mut data_entry)? {
            OperationStatus::Success => {
                self.key_index.lock().unwrap().insert(key.to_vec());
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    /// Returns the number of records in the database.
    ///
    /// Uses the database's `count()` method for an accurate count.
    pub fn len(&self) -> Result<u64> {
        Ok(self.db.count()?)
    }

    /// Returns whether the database is empty.
    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.len()? == 0)
    }

    /// Returns whether this map view is read-only.
    pub fn is_read_only(&self) -> bool {
        self.read_only
    }

    /// Registers a key in the internal key index.
    ///
    /// This is useful for populating the index with keys that are
    /// already in the database but were not inserted through this
    /// `StoredMap` instance.
    ///
    /// # Arguments
    /// * `key` - The key to register
    pub fn register_key(&self, key: &[u8]) {
        self.key_index.lock().unwrap().insert(key.to_vec());
    }

    /// Registers multiple keys in the internal key index.
    ///
    /// Convenience method for bulk registration.
    ///
    /// # Arguments
    /// * `keys` - The keys to register
    pub fn register_keys(&self, keys: &[&[u8]]) {
        let mut index = self.key_index.lock().unwrap();
        for key in keys {
            index.insert(key.to_vec());
        }
    }

    /// Returns a snapshot of the current key index.
    ///
    /// The returned vector contains all keys known to this map view,
    /// sorted in ascending order.
    pub fn known_keys(&self) -> Vec<Vec<u8>> {
        self.key_index.lock().unwrap().iter().cloned().collect()
    }

    /// Returns an iterator over all key-value pairs.
    ///
    /// Entries are yielded in sorted key order. The iterator works from
    /// a snapshot of the key index taken at the time of this call.
    pub fn iter(&self) -> Result<StoredIterator<'db>> {
        let keys = self.known_keys();
        Ok(StoredIterator::new(self.db, keys))
    }

    /// Returns an iterator over all keys.
    ///
    /// Keys are yielded in sorted order. The iterator works from a
    /// snapshot of the key index taken at the time of this call.
    pub fn keys(&self) -> Result<StoredKeyIterator> {
        let keys = self.known_keys();
        Ok(StoredKeyIterator::new(keys))
    }

    /// Returns an iterator over all values.
    ///
    /// Values are yielded in key-sorted order. The iterator works from
    /// a snapshot of the key index taken at the time of this call.
    pub fn values(&self) -> Result<StoredValueIterator<'db>> {
        let keys = self.known_keys();
        Ok(StoredValueIterator::new(self.db, keys))
    }

    /// Returns a reference to the underlying database.
    pub fn database(&self) -> &'db Database {
        self.db
    }

    /// Clears all records from the database.
    ///
    /// Removes all entries that are tracked in the key index.
    ///
    /// # Errors
    /// Returns `CollectionError::ReadOnly` if this map is read-only.
    pub fn clear(&self) -> Result<()> {
        if self.read_only {
            return Err(CollectionError::ReadOnly);
        }

        let keys: Vec<Vec<u8>> =
            self.key_index.lock().unwrap().iter().cloned().collect();
        for key in &keys {
            let key_entry = DatabaseEntry::from_vec(key.clone());
            let _ = self.db.delete(None, &key_entry);
        }

        self.key_index.lock().unwrap().clear();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use noxu_db::{DatabaseConfig, Environment, EnvironmentConfig};
    use tempfile::TempDir;

    fn setup_env_and_db() -> (TempDir, Environment, Database) {
        let temp_dir = TempDir::new().unwrap();
        let env_config = EnvironmentConfig::new(temp_dir.path().to_path_buf())
            .with_allow_create(true);
        let env = Environment::open(env_config).unwrap();
        let db_config = DatabaseConfig::new().with_allow_create(true);
        let db = env.open_database(None, "testdb", &db_config).unwrap();
        (temp_dir, env, db)
    }

    #[test]
    fn test_new_map() {
        let (_td, _env, db) = setup_env_and_db();
        let map = StoredMap::new(&db, false);
        assert!(!map.is_read_only());
    }

    #[test]
    fn test_new_read_only_map() {
        let (_td, _env, db) = setup_env_and_db();
        let map = StoredMap::new(&db, true);
        assert!(map.is_read_only());
    }

    #[test]
    fn test_put_and_get() {
        let (_td, _env, db) = setup_env_and_db();
        let map = StoredMap::new(&db, false);

        let old = map.put(b"key1", b"value1").unwrap();
        assert!(old.is_none());

        let val = map.get(b"key1").unwrap();
        assert_eq!(val, Some(b"value1".to_vec()));
    }

    #[test]
    fn test_put_overwrite() {
        let (_td, _env, db) = setup_env_and_db();
        let map = StoredMap::new(&db, false);

        map.put(b"key1", b"value1").unwrap();
        let old = map.put(b"key1", b"value2").unwrap();
        assert_eq!(old, Some(b"value1".to_vec()));

        let val = map.get(b"key1").unwrap();
        assert_eq!(val, Some(b"value2".to_vec()));
    }

    #[test]
    fn test_get_nonexistent() {
        let (_td, _env, db) = setup_env_and_db();
        let map = StoredMap::new(&db, false);

        let val = map.get(b"nonexistent").unwrap();
        assert!(val.is_none());
    }

    #[test]
    fn test_remove() {
        let (_td, _env, db) = setup_env_and_db();
        let map = StoredMap::new(&db, false);

        map.put(b"key1", b"value1").unwrap();
        let old = map.remove(b"key1").unwrap();
        assert_eq!(old, Some(b"value1".to_vec()));

        let val = map.get(b"key1").unwrap();
        assert!(val.is_none());
    }

    #[test]
    fn test_remove_nonexistent() {
        let (_td, _env, db) = setup_env_and_db();
        let map = StoredMap::new(&db, false);

        let old = map.remove(b"nonexistent").unwrap();
        assert!(old.is_none());
    }

    #[test]
    fn test_contains_key() {
        let (_td, _env, db) = setup_env_and_db();
        let map = StoredMap::new(&db, false);

        assert!(!map.contains_key(b"key1").unwrap());
        map.put(b"key1", b"value1").unwrap();
        assert!(map.contains_key(b"key1").unwrap());
    }

    #[test]
    fn test_len_and_is_empty() {
        let (_td, _env, db) = setup_env_and_db();
        let map = StoredMap::new(&db, false);

        assert!(map.is_empty().unwrap());
        assert_eq!(map.len().unwrap(), 0);

        map.put(b"key1", b"value1").unwrap();
        assert!(!map.is_empty().unwrap());
        assert_eq!(map.len().unwrap(), 1);

        map.put(b"key2", b"value2").unwrap();
        assert_eq!(map.len().unwrap(), 2);
    }

    #[test]
    fn test_read_only_put() {
        let (_td, _env, db) = setup_env_and_db();
        let map = StoredMap::new(&db, true);

        let result = map.put(b"key1", b"value1");
        assert!(matches!(result, Err(CollectionError::ReadOnly)));
    }

    #[test]
    fn test_read_only_remove() {
        let (_td, _env, db) = setup_env_and_db();
        let map = StoredMap::new(&db, true);

        let result = map.remove(b"key1");
        assert!(matches!(result, Err(CollectionError::ReadOnly)));
    }

    #[test]
    fn test_read_only_get() {
        let (_td, _env, db) = setup_env_and_db();

        // Put data using another map view
        let writer = StoredMap::new(&db, false);
        writer.put(b"key1", b"value1").unwrap();

        // Read via read-only map
        let reader = StoredMap::new(&db, true);
        let val = reader.get(b"key1").unwrap();
        assert_eq!(val, Some(b"value1".to_vec()));
    }

    #[test]
    fn test_iter() {
        let (_td, _env, db) = setup_env_and_db();
        let map = StoredMap::new(&db, false);

        map.put(b"cherry", b"c").unwrap();
        map.put(b"apple", b"a").unwrap();
        map.put(b"banana", b"b").unwrap();

        let items: Vec<_> = map.iter().unwrap().map(|r| r.unwrap()).collect();
        assert_eq!(items.len(), 3);
        assert_eq!(items[0].0, b"apple");
        assert_eq!(items[1].0, b"banana");
        assert_eq!(items[2].0, b"cherry");
    }

    #[test]
    fn test_keys() {
        let (_td, _env, db) = setup_env_and_db();
        let map = StoredMap::new(&db, false);

        map.put(b"cherry", b"c").unwrap();
        map.put(b"apple", b"a").unwrap();
        map.put(b"banana", b"b").unwrap();

        let keys: Vec<_> = map.keys().unwrap().map(|r| r.unwrap()).collect();
        assert_eq!(keys.len(), 3);
        assert_eq!(keys[0], b"apple");
        assert_eq!(keys[1], b"banana");
        assert_eq!(keys[2], b"cherry");
    }

    #[test]
    fn test_values() {
        let (_td, _env, db) = setup_env_and_db();
        let map = StoredMap::new(&db, false);

        map.put(b"cherry", b"c").unwrap();
        map.put(b"apple", b"a").unwrap();
        map.put(b"banana", b"b").unwrap();

        let vals: Vec<_> = map.values().unwrap().map(|r| r.unwrap()).collect();
        assert_eq!(vals.len(), 3);
        assert_eq!(vals[0], b"a");
        assert_eq!(vals[1], b"b");
        assert_eq!(vals[2], b"c");
    }

    #[test]
    fn test_register_key() {
        let (_td, _env, db) = setup_env_and_db();

        // Put data directly via database
        let k = DatabaseEntry::from_bytes(b"existing");
        let v = DatabaseEntry::from_bytes(b"data");
        db.put(None, &k, &v).unwrap();

        // Create map and register the existing key
        let map = StoredMap::new(&db, true);
        map.register_key(b"existing");

        let keys = map.known_keys();
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0], b"existing");

        // Iteration should now include this key
        let items: Vec<_> = map.iter().unwrap().map(|r| r.unwrap()).collect();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].1, b"data");
    }

    #[test]
    fn test_register_keys() {
        let (_td, _env, db) = setup_env_and_db();
        let map = StoredMap::new(&db, false);

        map.register_keys(&[b"a", b"b", b"c"]);
        let keys = map.known_keys();
        assert_eq!(keys.len(), 3);
    }

    #[test]
    fn test_clear() {
        let (_td, _env, db) = setup_env_and_db();
        let map = StoredMap::new(&db, false);

        map.put(b"key1", b"value1").unwrap();
        map.put(b"key2", b"value2").unwrap();
        assert_eq!(map.len().unwrap(), 2);

        map.clear().unwrap();
        assert_eq!(map.len().unwrap(), 0);
        assert!(map.known_keys().is_empty());
    }

    #[test]
    fn test_clear_read_only() {
        let (_td, _env, db) = setup_env_and_db();
        let map = StoredMap::new(&db, true);

        let result = map.clear();
        assert!(matches!(result, Err(CollectionError::ReadOnly)));
    }

    #[test]
    fn test_database_accessor() {
        let (_td, _env, db) = setup_env_and_db();
        let map = StoredMap::new(&db, false);
        assert_eq!(map.database().get_database_name(), "testdb");
    }

    #[test]
    fn test_known_keys_sorted() {
        let (_td, _env, db) = setup_env_and_db();
        let map = StoredMap::new(&db, false);

        map.put(b"zebra", b"z").unwrap();
        map.put(b"apple", b"a").unwrap();
        map.put(b"mango", b"m").unwrap();

        let keys = map.known_keys();
        assert_eq!(keys[0], b"apple");
        assert_eq!(keys[1], b"mango");
        assert_eq!(keys[2], b"zebra");
    }

    #[test]
    fn test_empty_key_and_value() {
        let (_td, _env, db) = setup_env_and_db();
        let map = StoredMap::new(&db, false);

        map.put(b"", b"").unwrap();
        let val = map.get(b"").unwrap();
        assert_eq!(val, Some(b"".to_vec()));
    }

    #[test]
    fn test_binary_data() {
        let (_td, _env, db) = setup_env_and_db();
        let map = StoredMap::new(&db, false);

        let key = &[0u8, 255, 128, 64];
        let value = &[255u8, 0, 1, 254];

        map.put(key, value).unwrap();
        let val = map.get(key).unwrap();
        assert_eq!(val, Some(value.to_vec()));
    }

    #[test]
    fn test_multiple_puts_same_key() {
        let (_td, _env, db) = setup_env_and_db();
        let map = StoredMap::new(&db, false);

        map.put(b"key", b"v1").unwrap();
        map.put(b"key", b"v2").unwrap();
        map.put(b"key", b"v3").unwrap();

        let val = map.get(b"key").unwrap();
        assert_eq!(val, Some(b"v3".to_vec()));
        // Key index should have only one entry for this key
        assert_eq!(map.known_keys().len(), 1);
    }
}
