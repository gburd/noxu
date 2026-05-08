//! Database iterators for collection views.
//!
//!
//! Provides iterators over database records. Unlike the StoredIterator
//! which wraps a live cursor, these iterators work from a snapshot of
//! sorted keys and fetch values on demand from the database.

use crate::error::{CollectionError, Result};
use noxu_db::{Database, DatabaseEntry, OperationStatus};

/// Iterator over database records yielding (key, value) pairs.
///
/// 
///
/// This iterator yields key-value pairs as `(Vec<u8>, Vec<u8>)`. Records
/// are returned in sorted key order. The iterator takes a snapshot of keys
/// at creation time and fetches values from the database on each call to
/// `next()`.
///
/// # Note
///
/// Because this iterator snapshots keys at creation time, concurrent
/// modifications to the database may cause some entries to be missing
/// (if deleted after snapshot) or stale.
pub struct StoredIterator<'db> {
    /// Reference to the database for fetching values.
    db: &'db Database,
    /// Sorted snapshot of keys to iterate over.
    keys: Vec<Vec<u8>>,
    /// Current position in the keys vector.
    position: usize,
    /// Whether to iterate in reverse order.
    reverse: bool,
}

impl<'db> StoredIterator<'db> {
    /// Creates a new iterator over all records in the database.
    ///
    /// Keys are snapshotted at creation time and sorted in ascending order.
    pub fn new(db: &'db Database, keys: Vec<Vec<u8>>) -> Self {
        let mut sorted_keys = keys;
        sorted_keys.sort();
        StoredIterator { db, keys: sorted_keys, position: 0, reverse: false }
    }

    /// Creates a new reverse iterator over all records in the database.
    ///
    /// Keys are snapshotted at creation time and iterated in descending order.
    pub fn new_reverse(db: &'db Database, keys: Vec<Vec<u8>>) -> Self {
        let mut sorted_keys = keys;
        sorted_keys.sort();
        sorted_keys.reverse();
        StoredIterator { db, keys: sorted_keys, position: 0, reverse: true }
    }

    /// Creates a new iterator starting from the given key (inclusive).
    ///
    /// Only keys greater than or equal to `start_key` are included.
    /// Keys are iterated in ascending order.
    pub fn new_from(
        db: &'db Database,
        keys: Vec<Vec<u8>>,
        start_key: &[u8],
    ) -> Self {
        let mut sorted_keys = keys;
        sorted_keys.sort();
        sorted_keys.retain(|k| k.as_slice() >= start_key);
        StoredIterator { db, keys: sorted_keys, position: 0, reverse: false }
    }

    /// Returns the number of remaining entries.
    pub fn remaining(&self) -> usize {
        if self.position >= self.keys.len() {
            0
        } else {
            self.keys.len() - self.position
        }
    }

    /// Returns whether the iterator is in reverse mode.
    pub fn is_reverse(&self) -> bool {
        self.reverse
    }
}

impl<'db> Iterator for StoredIterator<'db> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.position >= self.keys.len() {
            return None;
        }

        let key_bytes = self.keys[self.position].clone();
        self.position += 1;

        let key_entry = DatabaseEntry::from_vec(key_bytes.clone());
        let mut data_entry = DatabaseEntry::new();

        match self.db.get(None, &key_entry, &mut data_entry) {
            Ok(OperationStatus::Success) => {
                let value = data_entry
                    .get_data()
                    .map(|d| d.to_vec())
                    .unwrap_or_default();
                Some(Ok((key_bytes, value)))
            }
            Ok(OperationStatus::NotFound) => {
                // Key was deleted between snapshot and fetch; skip to next
                self.next()
            }
            Ok(_) => {
                // Unexpected status
                self.next()
            }
            Err(e) => Some(Err(CollectionError::DatabaseError(e))),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.remaining();
        (0, Some(remaining))
    }
}

/// Iterator over database keys only.
///
/// Yields keys as `Vec<u8>` in sorted order. This iterator does not
/// fetch values from the database, making it more efficient when only
/// keys are needed.
pub struct StoredKeyIterator {
    /// Sorted snapshot of keys to iterate over.
    keys: Vec<Vec<u8>>,
    /// Current position in the keys vector.
    position: usize,
}

impl StoredKeyIterator {
    /// Creates a new key iterator from a sorted key snapshot.
    pub fn new(keys: Vec<Vec<u8>>) -> Self {
        let mut sorted_keys = keys;
        sorted_keys.sort();
        StoredKeyIterator { keys: sorted_keys, position: 0 }
    }

    /// Returns the number of remaining keys.
    pub fn remaining(&self) -> usize {
        if self.position >= self.keys.len() {
            0
        } else {
            self.keys.len() - self.position
        }
    }
}

impl Iterator for StoredKeyIterator {
    type Item = Result<Vec<u8>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.position >= self.keys.len() {
            return None;
        }

        let key = self.keys[self.position].clone();
        self.position += 1;
        Some(Ok(key))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.remaining();
        (remaining, Some(remaining))
    }
}

/// Iterator over database values only.
///
/// Yields values as `Vec<u8>` in key-sorted order. Values are fetched
/// from the database on each call to `next()`.
pub struct StoredValueIterator<'db> {
    /// The underlying key-value iterator.
    inner: StoredIterator<'db>,
}

impl<'db> StoredValueIterator<'db> {
    /// Creates a new value iterator from a database and key snapshot.
    pub fn new(db: &'db Database, keys: Vec<Vec<u8>>) -> Self {
        StoredValueIterator { inner: StoredIterator::new(db, keys) }
    }

    /// Returns the number of remaining values.
    pub fn remaining(&self) -> usize {
        self.inner.remaining()
    }
}

impl<'db> Iterator for StoredValueIterator<'db> {
    type Item = Result<Vec<u8>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|result| result.map(|(_, v)| v))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use noxu_db::{DatabaseConfig, Environment, EnvironmentConfig};
    use tempfile::TempDir;

    fn setup_db_with_data() -> (TempDir, Environment, Database, Vec<Vec<u8>>) {
        let temp_dir = TempDir::new().unwrap();
        let env_config = EnvironmentConfig::new(temp_dir.path().to_path_buf())
            .with_allow_create(true);
        let env = Environment::open(env_config).unwrap();
        let db_config = DatabaseConfig::new().with_allow_create(true);
        let db = env.open_database(None, "testdb", &db_config).unwrap();

        let keys: Vec<Vec<u8>> =
            vec![b"cherry".to_vec(), b"apple".to_vec(), b"banana".to_vec()];

        for key in &keys {
            let k = DatabaseEntry::from_vec(key.clone());
            let v = DatabaseEntry::from_vec(
                format!("val_{}", String::from_utf8_lossy(key)).into_bytes(),
            );
            db.put(None, &k, &v).unwrap();
        }

        (temp_dir, env, db, keys)
    }

    #[test]
    fn test_stored_iterator_sorted_order() {
        let (_td, _env, db, keys) = setup_db_with_data();
        let iter = StoredIterator::new(&db, keys);
        let items: Vec<_> = iter.map(|r| r.unwrap()).collect();

        assert_eq!(items.len(), 3);
        assert_eq!(items[0].0, b"apple");
        assert_eq!(items[1].0, b"banana");
        assert_eq!(items[2].0, b"cherry");
    }

    #[test]
    fn test_stored_iterator_values() {
        let (_td, _env, db, keys) = setup_db_with_data();
        let iter = StoredIterator::new(&db, keys);
        let items: Vec<_> = iter.map(|r| r.unwrap()).collect();

        assert_eq!(items[0].1, b"val_apple");
        assert_eq!(items[1].1, b"val_banana");
        assert_eq!(items[2].1, b"val_cherry");
    }

    #[test]
    fn test_stored_iterator_reverse() {
        let (_td, _env, db, keys) = setup_db_with_data();
        let iter = StoredIterator::new_reverse(&db, keys);
        let items: Vec<_> = iter.map(|r| r.unwrap()).collect();

        assert_eq!(items.len(), 3);
        assert_eq!(items[0].0, b"cherry");
        assert_eq!(items[1].0, b"banana");
        assert_eq!(items[2].0, b"apple");
    }

    #[test]
    fn test_stored_iterator_from() {
        let (_td, _env, db, keys) = setup_db_with_data();
        let iter = StoredIterator::new_from(&db, keys, b"banana");
        let items: Vec<_> = iter.map(|r| r.unwrap()).collect();

        assert_eq!(items.len(), 2);
        assert_eq!(items[0].0, b"banana");
        assert_eq!(items[1].0, b"cherry");
    }

    #[test]
    fn test_stored_iterator_empty() {
        let temp_dir = TempDir::new().unwrap();
        let env_config = EnvironmentConfig::new(temp_dir.path().to_path_buf())
            .with_allow_create(true);
        let env = Environment::open(env_config).unwrap();
        let db_config = DatabaseConfig::new().with_allow_create(true);
        let db = env.open_database(None, "testdb", &db_config).unwrap();

        let iter = StoredIterator::new(&db, vec![]);
        let items: Vec<_> = iter.collect::<Vec<_>>();
        assert!(items.is_empty());
    }

    #[test]
    fn test_stored_iterator_remaining() {
        let (_td, _env, db, keys) = setup_db_with_data();
        let mut iter = StoredIterator::new(&db, keys);

        assert_eq!(iter.remaining(), 3);
        iter.next();
        assert_eq!(iter.remaining(), 2);
        iter.next();
        assert_eq!(iter.remaining(), 1);
        iter.next();
        assert_eq!(iter.remaining(), 0);
    }

    #[test]
    fn test_stored_iterator_skips_deleted() {
        let (_td, _env, db, keys) = setup_db_with_data();

        // Delete "banana" from the database
        let banana_key = DatabaseEntry::from_bytes(b"banana");
        db.delete(None, &banana_key).unwrap();

        let iter = StoredIterator::new(&db, keys);
        let items: Vec<_> = iter.map(|r| r.unwrap()).collect();

        // Should skip the deleted key
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].0, b"apple");
        assert_eq!(items[1].0, b"cherry");
    }

    #[test]
    fn test_stored_key_iterator() {
        let keys =
            vec![b"cherry".to_vec(), b"apple".to_vec(), b"banana".to_vec()];
        let iter = StoredKeyIterator::new(keys);
        let items: Vec<_> = iter.map(|r| r.unwrap()).collect();

        assert_eq!(items.len(), 3);
        assert_eq!(items[0], b"apple");
        assert_eq!(items[1], b"banana");
        assert_eq!(items[2], b"cherry");
    }

    #[test]
    fn test_stored_key_iterator_empty() {
        let iter = StoredKeyIterator::new(vec![]);
        let items: Vec<_> = iter.collect::<Vec<_>>();
        assert!(items.is_empty());
    }

    #[test]
    fn test_stored_key_iterator_remaining() {
        let keys = vec![b"a".to_vec(), b"b".to_vec()];
        let mut iter = StoredKeyIterator::new(keys);
        assert_eq!(iter.remaining(), 2);
        iter.next();
        assert_eq!(iter.remaining(), 1);
        iter.next();
        assert_eq!(iter.remaining(), 0);
    }

    #[test]
    fn test_stored_value_iterator() {
        let (_td, _env, db, keys) = setup_db_with_data();
        let iter = StoredValueIterator::new(&db, keys);
        let items: Vec<_> = iter.map(|r| r.unwrap()).collect();

        assert_eq!(items.len(), 3);
        assert_eq!(items[0], b"val_apple");
        assert_eq!(items[1], b"val_banana");
        assert_eq!(items[2], b"val_cherry");
    }

    #[test]
    fn test_stored_value_iterator_empty() {
        let temp_dir = TempDir::new().unwrap();
        let env_config = EnvironmentConfig::new(temp_dir.path().to_path_buf())
            .with_allow_create(true);
        let env = Environment::open(env_config).unwrap();
        let db_config = DatabaseConfig::new().with_allow_create(true);
        let db = env.open_database(None, "testdb", &db_config).unwrap();

        let iter = StoredValueIterator::new(&db, vec![]);
        let items: Vec<_> = iter.collect::<Vec<_>>();
        assert!(items.is_empty());
    }

    #[test]
    fn test_stored_iterator_size_hint() {
        let (_td, _env, db, keys) = setup_db_with_data();
        let iter = StoredIterator::new(&db, keys);
        let (lower, upper) = iter.size_hint();
        assert_eq!(lower, 0);
        assert_eq!(upper, Some(3));
    }

    #[test]
    fn test_stored_key_iterator_size_hint() {
        let keys = vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()];
        let iter = StoredKeyIterator::new(keys);
        let (lower, upper) = iter.size_hint();
        assert_eq!(lower, 3);
        assert_eq!(upper, Some(3));
    }

    #[test]
    fn test_stored_iterator_is_reverse() {
        let (_td, _env, db, keys) = setup_db_with_data();

        let iter = StoredIterator::new(&db, keys.clone());
        assert!(!iter.is_reverse());

        let iter = StoredIterator::new_reverse(&db, keys);
        assert!(iter.is_reverse());
    }

    #[test]
    fn test_stored_iterator_from_beyond_all_keys() {
        let (_td, _env, db, keys) = setup_db_with_data();
        let iter = StoredIterator::new_from(&db, keys, b"zzz");
        let items: Vec<_> = iter.collect::<Vec<_>>();
        assert!(items.is_empty());
    }

    #[test]
    fn test_stored_iterator_single_record() {
        let temp_dir = TempDir::new().unwrap();
        let env_config = EnvironmentConfig::new(temp_dir.path().to_path_buf())
            .with_allow_create(true);
        let env = Environment::open(env_config).unwrap();
        let db_config = DatabaseConfig::new().with_allow_create(true);
        let db = env.open_database(None, "testdb", &db_config).unwrap();

        let k = DatabaseEntry::from_bytes(b"only");
        let v = DatabaseEntry::from_bytes(b"one");
        db.put(None, &k, &v).unwrap();

        let keys = vec![b"only".to_vec()];
        let iter = StoredIterator::new(&db, keys);
        let items: Vec<_> = iter.map(|r| r.unwrap()).collect();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].0, b"only");
        assert_eq!(items[0].1, b"one");
    }
}
