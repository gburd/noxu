//! Sorted map view of a database.
//!
//! Port of `com.sleepycat.collections.StoredSortedMap`.

use crate::error::Result;
use crate::stored_iterator::{
    StoredIterator, StoredKeyIterator, StoredValueIterator,
};
use crate::stored_map::StoredMap;
use noxu_db::Database;

/// A sorted map view of a database.
///
/// Port of `com.sleepycat.collections.StoredSortedMap`.
///
/// Provides all the operations of `StoredMap` plus sorted-map operations
/// like `first_key()`, `last_key()`, and range iteration. Keys are
/// maintained in their natural byte order.
///
/// # Example
/// ```ignore
/// use noxu_collections::StoredSortedMap;
///
/// let map = StoredSortedMap::new(&db, false);
/// map.put(b"banana", b"b").unwrap();
/// map.put(b"apple", b"a").unwrap();
/// map.put(b"cherry", b"c").unwrap();
///
/// assert_eq!(map.first_key().unwrap(), Some(b"apple".to_vec()));
/// assert_eq!(map.last_key().unwrap(), Some(b"cherry".to_vec()));
/// ```
pub struct StoredSortedMap<'db> {
    /// The underlying StoredMap providing basic map operations.
    inner: StoredMap<'db>,
}

impl<'db> StoredSortedMap<'db> {
    /// Creates a new sorted map view of the given database.
    ///
    /// # Arguments
    /// * `db` - The database to provide a sorted map view over
    /// * `read_only` - If true, write operations will return `CollectionError::ReadOnly`
    pub fn new(db: &'db Database, read_only: bool) -> Self {
        StoredSortedMap { inner: StoredMap::new(db, read_only) }
    }

    /// Returns the first (smallest) key in the database, or `None` if empty.
    pub fn first_key(&self) -> Result<Option<Vec<u8>>> {
        let keys = self.inner.known_keys();
        Ok(keys.into_iter().next())
    }

    /// Returns the last (largest) key in the database, or `None` if empty.
    pub fn last_key(&self) -> Result<Option<Vec<u8>>> {
        let keys = self.inner.known_keys();
        Ok(keys.into_iter().last())
    }

    /// Returns an iterator over all key-value pairs in sorted key order.
    pub fn iter(&self) -> Result<StoredIterator<'db>> {
        self.inner.iter()
    }

    /// Returns an iterator starting from the given key (inclusive).
    ///
    /// Only entries with keys greater than or equal to `start_key` are included.
    ///
    /// # Arguments
    /// * `start_key` - The lower bound (inclusive) for iteration
    pub fn iter_from(&self, start_key: &[u8]) -> Result<StoredIterator<'db>> {
        let keys = self.inner.known_keys();
        Ok(StoredIterator::new_from(self.inner.database(), keys, start_key))
    }

    /// Returns a reverse iterator over all key-value pairs.
    ///
    /// Entries are yielded in descending key order.
    pub fn iter_reverse(&self) -> Result<StoredIterator<'db>> {
        let keys = self.inner.known_keys();
        Ok(StoredIterator::new_reverse(self.inner.database(), keys))
    }

    /// Returns an iterator over all keys in sorted order.
    pub fn keys(&self) -> Result<StoredKeyIterator> {
        self.inner.keys()
    }

    /// Returns an iterator over all values in key-sorted order.
    pub fn values(&self) -> Result<StoredValueIterator<'db>> {
        self.inner.values()
    }

    /// Retrieves the value associated with the given key.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.inner.get(key)
    }

    /// Inserts or updates a key-value pair.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<Option<Vec<u8>>> {
        self.inner.put(key, value)
    }

    /// Removes a key-value pair.
    pub fn remove(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.inner.remove(key)
    }

    /// Tests whether the given key exists.
    pub fn contains_key(&self, key: &[u8]) -> Result<bool> {
        self.inner.contains_key(key)
    }

    /// Returns the number of records.
    pub fn len(&self) -> Result<u64> {
        self.inner.len()
    }

    /// Returns whether the database is empty.
    pub fn is_empty(&self) -> Result<bool> {
        self.inner.is_empty()
    }

    /// Returns whether this map view is read-only.
    pub fn is_read_only(&self) -> bool {
        self.inner.is_read_only()
    }

    /// Registers a key in the internal key index.
    pub fn register_key(&self, key: &[u8]) {
        self.inner.register_key(key);
    }

    /// Registers multiple keys in the internal key index.
    pub fn register_keys(&self, keys: &[&[u8]]) {
        self.inner.register_keys(keys);
    }

    /// Returns a snapshot of known keys in sorted order.
    pub fn known_keys(&self) -> Vec<Vec<u8>> {
        self.inner.known_keys()
    }

    /// Returns a reference to the underlying database.
    pub fn database(&self) -> &'db Database {
        self.inner.database()
    }

    /// Returns a reference to the inner `StoredMap`.
    pub fn as_map(&self) -> &StoredMap<'db> {
        &self.inner
    }

    /// Clears all records.
    pub fn clear(&self) -> Result<()> {
        self.inner.clear()
    }

    /// Returns the first (smallest) key-value pair, or `None` if empty.
    pub fn first_entry(&self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        match self.first_key()? {
            Some(key) => {
                let val = self.get(&key)?;
                Ok(val.map(|v| (key, v)))
            }
            None => Ok(None),
        }
    }

    /// Returns the last (largest) key-value pair, or `None` if empty.
    pub fn last_entry(&self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        match self.last_key()? {
            Some(key) => {
                let val = self.get(&key)?;
                Ok(val.map(|v| (key, v)))
            }
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::CollectionError;
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

    fn populate_sorted_map<'db>(map: &StoredSortedMap<'db>) {
        map.put(b"cherry", b"c").unwrap();
        map.put(b"apple", b"a").unwrap();
        map.put(b"banana", b"b").unwrap();
        map.put(b"date", b"d").unwrap();
        map.put(b"elderberry", b"e").unwrap();
    }

    #[test]
    fn test_first_key() {
        let (_td, _env, db) = setup_env_and_db();
        let map = StoredSortedMap::new(&db, false);
        populate_sorted_map(&map);

        assert_eq!(map.first_key().unwrap(), Some(b"apple".to_vec()));
    }

    #[test]
    fn test_last_key() {
        let (_td, _env, db) = setup_env_and_db();
        let map = StoredSortedMap::new(&db, false);
        populate_sorted_map(&map);

        assert_eq!(map.last_key().unwrap(), Some(b"elderberry".to_vec()));
    }

    #[test]
    fn test_first_key_empty() {
        let (_td, _env, db) = setup_env_and_db();
        let map = StoredSortedMap::new(&db, false);

        assert_eq!(map.first_key().unwrap(), None);
    }

    #[test]
    fn test_last_key_empty() {
        let (_td, _env, db) = setup_env_and_db();
        let map = StoredSortedMap::new(&db, false);

        assert_eq!(map.last_key().unwrap(), None);
    }

    #[test]
    fn test_iter_sorted() {
        let (_td, _env, db) = setup_env_and_db();
        let map = StoredSortedMap::new(&db, false);
        populate_sorted_map(&map);

        let items: Vec<_> = map.iter().unwrap().map(|r| r.unwrap()).collect();
        assert_eq!(items.len(), 5);
        assert_eq!(items[0].0, b"apple");
        assert_eq!(items[1].0, b"banana");
        assert_eq!(items[2].0, b"cherry");
        assert_eq!(items[3].0, b"date");
        assert_eq!(items[4].0, b"elderberry");
    }

    #[test]
    fn test_iter_from() {
        let (_td, _env, db) = setup_env_and_db();
        let map = StoredSortedMap::new(&db, false);
        populate_sorted_map(&map);

        let items: Vec<_> =
            map.iter_from(b"cherry").unwrap().map(|r| r.unwrap()).collect();
        assert_eq!(items.len(), 3);
        assert_eq!(items[0].0, b"cherry");
        assert_eq!(items[1].0, b"date");
        assert_eq!(items[2].0, b"elderberry");
    }

    #[test]
    fn test_iter_from_between_keys() {
        let (_td, _env, db) = setup_env_and_db();
        let map = StoredSortedMap::new(&db, false);
        populate_sorted_map(&map);

        // "cat" is between "banana" and "cherry"
        let items: Vec<_> =
            map.iter_from(b"cat").unwrap().map(|r| r.unwrap()).collect();
        assert_eq!(items.len(), 3);
        assert_eq!(items[0].0, b"cherry");
    }

    #[test]
    fn test_iter_from_past_all() {
        let (_td, _env, db) = setup_env_and_db();
        let map = StoredSortedMap::new(&db, false);
        populate_sorted_map(&map);

        let items: Vec<_> =
            map.iter_from(b"zzz").unwrap().map(|r| r.unwrap()).collect();
        assert!(items.is_empty());
    }

    #[test]
    fn test_iter_reverse() {
        let (_td, _env, db) = setup_env_and_db();
        let map = StoredSortedMap::new(&db, false);
        populate_sorted_map(&map);

        let items: Vec<_> =
            map.iter_reverse().unwrap().map(|r| r.unwrap()).collect();
        assert_eq!(items.len(), 5);
        assert_eq!(items[0].0, b"elderberry");
        assert_eq!(items[1].0, b"date");
        assert_eq!(items[2].0, b"cherry");
        assert_eq!(items[3].0, b"banana");
        assert_eq!(items[4].0, b"apple");
    }

    #[test]
    fn test_first_entry() {
        let (_td, _env, db) = setup_env_and_db();
        let map = StoredSortedMap::new(&db, false);
        populate_sorted_map(&map);

        let entry = map.first_entry().unwrap().unwrap();
        assert_eq!(entry.0, b"apple");
        assert_eq!(entry.1, b"a");
    }

    #[test]
    fn test_last_entry() {
        let (_td, _env, db) = setup_env_and_db();
        let map = StoredSortedMap::new(&db, false);
        populate_sorted_map(&map);

        let entry = map.last_entry().unwrap().unwrap();
        assert_eq!(entry.0, b"elderberry");
        assert_eq!(entry.1, b"e");
    }

    #[test]
    fn test_first_entry_empty() {
        let (_td, _env, db) = setup_env_and_db();
        let map = StoredSortedMap::new(&db, false);
        assert!(map.first_entry().unwrap().is_none());
    }

    #[test]
    fn test_delegated_operations() {
        let (_td, _env, db) = setup_env_and_db();
        let map = StoredSortedMap::new(&db, false);

        // Test put/get/remove/contains_key
        map.put(b"key", b"val").unwrap();
        assert!(map.contains_key(b"key").unwrap());
        assert_eq!(map.get(b"key").unwrap(), Some(b"val".to_vec()));
        assert_eq!(map.len().unwrap(), 1);
        assert!(!map.is_empty().unwrap());

        map.remove(b"key").unwrap();
        assert!(!map.contains_key(b"key").unwrap());
    }

    #[test]
    fn test_as_map() {
        let (_td, _env, db) = setup_env_and_db();
        let sorted_map = StoredSortedMap::new(&db, false);
        let map = sorted_map.as_map();
        assert!(!map.is_read_only());
    }

    #[test]
    fn test_read_only() {
        let (_td, _env, db) = setup_env_and_db();
        let map = StoredSortedMap::new(&db, true);
        assert!(map.is_read_only());
        assert!(matches!(map.put(b"k", b"v"), Err(CollectionError::ReadOnly)));
    }

    #[test]
    fn test_single_entry() {
        let (_td, _env, db) = setup_env_and_db();
        let map = StoredSortedMap::new(&db, false);
        map.put(b"only", b"one").unwrap();

        assert_eq!(map.first_key().unwrap(), Some(b"only".to_vec()));
        assert_eq!(map.last_key().unwrap(), Some(b"only".to_vec()));
    }
}
