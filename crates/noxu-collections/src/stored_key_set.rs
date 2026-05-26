//! Key set view of a database.
//!

use crate::error::Result;
use crate::stored_iterator::StoredKeyIterator;
use noxu_db::{Database, DatabaseEntry, OperationStatus};
use std::collections::BTreeSet;
use std::sync::Mutex;

/// A set view of database keys.
///
/// Provides a set interface over the keys of a Noxu DB database.
/// Keys are returned in sorted byte order.
///
/// Like `StoredMap`, this view maintains an internal key index that
/// must be populated through `contains()`, `register_key()`, or
/// `register_keys()` calls for pre-existing data.
///
/// # v1.5 limitations
///
/// All `StoredKeySet` operations are **auto-commit only** — every
/// `contains` / `add` / `remove` issues the underlying `Database` call
/// with `txn = None`.  Threading `Option<&Transaction>` through the
/// API is tracked for v1.6 (audit findings #1, #3, #4).
///
/// # Example
/// ```ignore
/// use noxu_collections::StoredKeySet;
///
/// let key_set = StoredKeySet::new(&db);
/// key_set.register_key(b"key1");
/// assert!(key_set.contains(b"key1").unwrap());
/// ```
pub struct StoredKeySet<'db> {
    /// Reference to the underlying database.
    db: &'db Database,
    /// Internal sorted key index.
    key_index: Mutex<BTreeSet<Vec<u8>>>,
}

impl<'db> StoredKeySet<'db> {
    /// Creates a new key set view of the given database.
    ///
    /// # Arguments
    /// * `db` - The database whose keys to view
    pub fn new(db: &'db Database) -> Self {
        StoredKeySet { db, key_index: Mutex::new(BTreeSet::new()) }
    }

    /// Tests whether the given key exists in the database.
    ///
    /// If the key exists, it is registered in the internal key index.
    ///
    /// # Arguments
    /// * `key` - The key to check
    pub fn contains(&self, key: &[u8]) -> Result<bool> {
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
    pub fn len(&self) -> Result<u64> {
        Ok(self.db.count()?)
    }

    /// Returns whether the database is empty.
    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.len()? == 0)
    }

    /// Returns an iterator over all known keys in sorted order.
    ///
    /// The iterator works from a snapshot of the key index taken
    /// at the time of this call.
    pub fn iter(&self) -> Result<StoredKeyIterator> {
        let keys = self.known_keys();
        Ok(StoredKeyIterator::new(keys))
    }

    /// Registers a key in the internal key index.
    ///
    /// # Arguments
    /// * `key` - The key to register
    pub fn register_key(&self, key: &[u8]) {
        self.key_index.lock().unwrap().insert(key.to_vec());
    }

    /// Registers multiple keys in the internal key index.
    ///
    /// # Arguments
    /// * `keys` - The keys to register
    pub fn register_keys(&self, keys: &[&[u8]]) {
        let mut index = self.key_index.lock().unwrap();
        for key in keys {
            index.insert(key.to_vec());
        }
    }

    /// Returns a snapshot of known keys in sorted order.
    pub fn known_keys(&self) -> Vec<Vec<u8>> {
        self.key_index.lock().unwrap().iter().cloned().collect()
    }

    /// Returns a reference to the underlying database.
    pub fn database(&self) -> &'db Database {
        self.db
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

    fn populate_db(db: &Database) {
        let pairs: Vec<(&[u8], &[u8])> =
            vec![(b"cherry", b"val"), (b"apple", b"val"), (b"banana", b"val")];
        for (key, val) in pairs {
            let k = DatabaseEntry::from_bytes(key);
            let v = DatabaseEntry::from_bytes(val);
            db.put(None, &k, &v).unwrap();
        }
    }

    #[test]
    fn test_new_key_set() {
        let (_td, _env, db) = setup_env_and_db();
        let set = StoredKeySet::new(&db);
        assert!(set.is_empty().unwrap());
    }

    #[test]
    fn test_contains_existing() {
        let (_td, _env, db) = setup_env_and_db();
        populate_db(&db);

        let set = StoredKeySet::new(&db);
        assert!(set.contains(b"apple").unwrap());
        assert!(set.contains(b"banana").unwrap());
        assert!(set.contains(b"cherry").unwrap());
    }

    #[test]
    fn test_contains_nonexistent() {
        let (_td, _env, db) = setup_env_and_db();
        populate_db(&db);

        let set = StoredKeySet::new(&db);
        assert!(!set.contains(b"dragonfruit").unwrap());
    }

    #[test]
    fn test_len() {
        let (_td, _env, db) = setup_env_and_db();
        populate_db(&db);

        let set = StoredKeySet::new(&db);
        assert_eq!(set.len().unwrap(), 3);
    }

    #[test]
    fn test_is_empty() {
        let (_td, _env, db) = setup_env_and_db();
        let set = StoredKeySet::new(&db);
        assert!(set.is_empty().unwrap());

        populate_db(&db);
        assert!(!set.is_empty().unwrap());
    }

    #[test]
    fn test_iter_with_registered_keys() {
        let (_td, _env, db) = setup_env_and_db();
        populate_db(&db);

        let set = StoredKeySet::new(&db);
        // Register keys (simulating discovery)
        set.register_keys(&[b"cherry" as &[u8], b"apple", b"banana"]);

        let keys: Vec<_> = set.iter().unwrap().map(|r| r.unwrap()).collect();
        assert_eq!(keys.len(), 3);
        assert_eq!(keys[0], b"apple");
        assert_eq!(keys[1], b"banana");
        assert_eq!(keys[2], b"cherry");
    }

    #[test]
    fn test_contains_populates_index() {
        let (_td, _env, db) = setup_env_and_db();
        populate_db(&db);

        let set = StoredKeySet::new(&db);
        // contains() should add keys to the index
        set.contains(b"apple").unwrap();
        set.contains(b"cherry").unwrap();

        let keys = set.known_keys();
        assert_eq!(keys.len(), 2);
        assert_eq!(keys[0], b"apple");
        assert_eq!(keys[1], b"cherry");
    }

    #[test]
    fn test_register_key() {
        let (_td, _env, db) = setup_env_and_db();
        let set = StoredKeySet::new(&db);

        set.register_key(b"key1");
        let keys = set.known_keys();
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0], b"key1");
    }

    #[test]
    fn test_database_accessor() {
        let (_td, _env, db) = setup_env_and_db();
        let set = StoredKeySet::new(&db);
        assert_eq!(set.database().get_database_name(), "testdb");
    }
}
