//! Value set view of a database.
//!

use crate::error::Result;
use crate::stored_iterator::StoredValueIterator;
use noxu_db::Database;
use std::collections::BTreeSet;
use std::sync::Mutex;

/// A collection view of database values.
///
/// Provides a collection interface over the values stored in a Noxu DB
/// database. Values are yielded in key-sorted order during iteration.
///
/// Like `StoredMap`, this view maintains an internal key index that
/// must be populated for iteration support. Use `register_key()` or
/// `register_keys()` to populate the index.
///
/// # v1.5 limitations
///
/// All `StoredValueSet` operations are **auto-commit only** — every
/// fetch issues the underlying `Database` call with `txn = None`.
/// Threading `Option<&Transaction>` through the API is tracked for
/// v1.6 (audit findings #1, #3, #4).
///
/// # Example
/// ```ignore
/// use noxu_collections::StoredValueSet;
///
/// let values = StoredValueSet::new(&db);
/// values.register_keys(&[b"key1", b"key2"]);
/// for val in values.iter().unwrap() {
///     println!("{:?}", val.unwrap());
/// }
/// ```
pub struct StoredValueSet<'db> {
    /// Reference to the underlying database.
    db: &'db Database,
    /// Internal sorted key index for iteration.
    key_index: Mutex<BTreeSet<Vec<u8>>>,
}

impl<'db> StoredValueSet<'db> {
    /// Creates a new value set view of the given database.
    ///
    /// # Arguments
    /// * `db` - The database whose values to view
    pub fn new(db: &'db Database) -> Self {
        StoredValueSet { db, key_index: Mutex::new(BTreeSet::new()) }
    }

    /// Returns the number of records in the database.
    pub fn len(&self) -> Result<u64> {
        Ok(self.db.count()?)
    }

    /// Returns whether the database is empty.
    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.len()? == 0)
    }

    /// Returns an iterator over all known values in key-sorted order.
    ///
    /// The iterator works from a snapshot of the key index taken
    /// at the time of this call. Values are fetched from the database
    /// on demand.
    pub fn iter(&self) -> Result<StoredValueIterator<'db>> {
        let keys = self.known_keys();
        Ok(StoredValueIterator::new(self.db, keys))
    }

    /// Registers a key in the internal key index.
    ///
    /// # Arguments
    /// * `key` - The key to register (the value associated with this key
    ///   will be included in iteration)
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
    use noxu_db::{
        DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig,
    };
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
        let pairs = vec![
            (b"cherry".as_slice(), b"c".as_slice()),
            (b"apple", b"a"),
            (b"banana", b"b"),
        ];
        for (k, v) in pairs {
            let key = DatabaseEntry::from_bytes(k);
            let val = DatabaseEntry::from_bytes(v);
            db.put(None, &key, &val).unwrap();
        }
    }

    #[test]
    fn test_new_value_set() {
        let (_td, _env, db) = setup_env_and_db();
        let set = StoredValueSet::new(&db);
        assert!(set.is_empty().unwrap());
    }

    #[test]
    fn test_len() {
        let (_td, _env, db) = setup_env_and_db();
        populate_db(&db);

        let set = StoredValueSet::new(&db);
        assert_eq!(set.len().unwrap(), 3);
    }

    #[test]
    fn test_is_empty() {
        let (_td, _env, db) = setup_env_and_db();
        let set = StoredValueSet::new(&db);
        assert!(set.is_empty().unwrap());

        populate_db(&db);
        assert!(!set.is_empty().unwrap());
    }

    #[test]
    fn test_iter_with_registered_keys() {
        let (_td, _env, db) = setup_env_and_db();
        populate_db(&db);

        let set = StoredValueSet::new(&db);
        set.register_keys(&[b"apple", b"banana", b"cherry"]);

        let vals: Vec<_> = set.iter().unwrap().map(|r| r.unwrap()).collect();
        assert_eq!(vals.len(), 3);
        // Values in key-sorted order
        assert_eq!(vals[0], b"a");
        assert_eq!(vals[1], b"b");
        assert_eq!(vals[2], b"c");
    }

    #[test]
    fn test_iter_empty() {
        let (_td, _env, db) = setup_env_and_db();
        let set = StoredValueSet::new(&db);

        let vals: Vec<_> = set.iter().unwrap().collect::<Vec<_>>();
        assert!(vals.is_empty());
    }

    #[test]
    fn test_register_key() {
        let (_td, _env, db) = setup_env_and_db();
        let set = StoredValueSet::new(&db);

        set.register_key(b"key1");
        let keys = set.known_keys();
        assert_eq!(keys.len(), 1);
    }

    #[test]
    fn test_register_keys() {
        let (_td, _env, db) = setup_env_and_db();
        let set = StoredValueSet::new(&db);

        set.register_keys(&[b"a", b"b", b"c"]);
        let keys = set.known_keys();
        assert_eq!(keys.len(), 3);
    }

    #[test]
    fn test_database_accessor() {
        let (_td, _env, db) = setup_env_and_db();
        let set = StoredValueSet::new(&db);
        assert_eq!(set.database().get_database_name(), "testdb");
    }

    #[test]
    fn test_iter_skips_deleted_keys() {
        let (_td, _env, db) = setup_env_and_db();
        populate_db(&db);

        let set = StoredValueSet::new(&db);
        set.register_keys(&[b"apple", b"banana", b"cherry"]);

        // Delete banana
        let banana_key = DatabaseEntry::from_bytes(b"banana");
        db.delete(None, &banana_key).unwrap();

        let vals: Vec<_> = set.iter().unwrap().map(|r| r.unwrap()).collect();
        assert_eq!(vals.len(), 2);
        assert_eq!(vals[0], b"a");
        assert_eq!(vals[1], b"c");
    }
}
