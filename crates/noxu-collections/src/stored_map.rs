//! Typed map view of a database.
//!
//! Wave 2B redesign (v1.6).  `StoredMap<K, V, KB, VB>` is the typed
//! map surface: keys and values are arbitrary Rust types, with
//! [`EntryBinding`] implementations doing the byte ↔ typed conversion.
//!
//! Every operation accepts `txn: Option<&Transaction>`.  `None` is
//! auto-commit; `Some(&t)` participates in the caller's transaction.
//! This is the BDB-JE shape, and it matches `noxu_db::Database` and
//! `noxu_db::SecondaryDatabase` so a typed `StoredMap` composes
//! cleanly with the rest of the engine.

use std::marker::PhantomData;

use noxu_bind::EntryBinding;
use noxu_db::{Database, Get, OperationStatus, Transaction};

use crate::error::{CollectionError, Result};
use crate::internal::{
    ScanDirection, StartKey, decode_value, encode_key, encode_value,
    scan_records,
};
use crate::stored_iterator::StoredIterator;

/// A typed map-like view of a database.
///
/// `K` is the key type and `V` is the value type.  `KB` and `VB` are
/// the [`EntryBinding`]s that convert between the typed values and
/// the on-disk byte representation.
///
/// # Transaction threading
///
/// Every method accepts `txn: Option<&Transaction>`.  Pass `None` to
/// run as auto-commit (the engine allocates a synthetic auto-txn for
/// each call) or `Some(&t)` to participate in `t`.  This is the v1.6
/// API shape — it matches BDB-JE's `StoredMap` and the
/// `noxu_db::Database` / `SecondaryDatabase` signature.
///
/// # Example
///
/// ```ignore
/// use noxu_bind::{IntBinding, StringBinding};
/// use noxu_collections::StoredMap;
///
/// let map: StoredMap<i32, String, _, _> =
///     StoredMap::new(&db, IntBinding, StringBinding);
///
/// // Auto-commit:
/// map.put(None, &1, &"alpha".to_string())?;
///
/// // Participate in a user txn:
/// let txn = env.begin_transaction(None)?;
/// map.put(Some(&txn), &2, &"beta".to_string())?;
/// txn.commit()?;
/// ```
pub struct StoredMap<'db, K, V, KB, VB>
where
    KB: EntryBinding<K>,
    VB: EntryBinding<V>,
{
    pub(crate) db: &'db Database,
    pub(crate) key_binding: KB,
    pub(crate) value_binding: VB,
    pub(crate) read_only: bool,
    pub(crate) _marker: PhantomData<fn() -> (K, V)>,
}

impl<'db, K, V, KB, VB> StoredMap<'db, K, V, KB, VB>
where
    KB: EntryBinding<K>,
    VB: EntryBinding<V>,
{
    /// Creates a new typed map view of the given database.
    ///
    /// The map is read-write by default.  Use [`Self::new_read_only`]
    /// for a view that rejects mutating operations with
    /// [`CollectionError::ReadOnly`].
    pub fn new(db: &'db Database, key_binding: KB, value_binding: VB) -> Self {
        StoredMap {
            db,
            key_binding,
            value_binding,
            read_only: false,
            _marker: PhantomData,
        }
    }

    /// Creates a new read-only typed map view of the given database.
    ///
    /// Mutating operations (`put`, `remove`, `clear`) return
    /// [`CollectionError::ReadOnly`].
    pub fn new_read_only(
        db: &'db Database,
        key_binding: KB,
        value_binding: VB,
    ) -> Self {
        StoredMap {
            db,
            key_binding,
            value_binding,
            read_only: true,
            _marker: PhantomData,
        }
    }

    /// Returns whether this map view is read-only.
    pub fn is_read_only(&self) -> bool {
        self.read_only
    }

    /// Returns a reference to the underlying database.
    pub fn database(&self) -> &'db Database {
        self.db
    }

    /// Returns a reference to the key binding.
    pub fn key_binding(&self) -> &KB {
        &self.key_binding
    }

    /// Returns a reference to the value binding.
    pub fn value_binding(&self) -> &VB {
        &self.value_binding
    }

    /// Retrieves the value associated with the given key.
    ///
    /// Returns `Ok(None)` if the key is not present in the database.
    pub fn get(&self, txn: Option<&Transaction>, key: &K) -> Result<Option<V>> {
        let key_entry = encode_key(&self.key_binding, key)?;
        let mut data_entry = noxu_db::DatabaseEntry::new();
        match self.db.get(txn, &key_entry, &mut data_entry)? {
            OperationStatus::Success => {
                Ok(Some(decode_value(&self.value_binding, &data_entry)?))
            }
            _ => Ok(None),
        }
    }

    /// Inserts or updates a key-value pair.
    ///
    /// Returns the previous value associated with `key`, or `None`
    /// if the key was not present.  This is the `Map.put(...)`
    /// semantic from `java.util.Map`, matching BDB-JE.
    pub fn put(
        &self,
        txn: Option<&Transaction>,
        key: &K,
        value: &V,
    ) -> Result<Option<V>> {
        if self.read_only {
            return Err(CollectionError::ReadOnly);
        }

        // Read-then-write under the user's txn so the read+write are
        // serialisable as a single unit; if `txn` is `None` each call
        // is its own auto-txn and the pair is observably non-atomic
        // (acceptable v1.6 documented caveat — the same trade-off
        // applies to BDB-JE's auto-commit `StoredMap.put`).
        let key_entry = encode_key(&self.key_binding, key)?;
        let value_entry = encode_value(&self.value_binding, value)?;

        let old_value = {
            let mut data_entry = noxu_db::DatabaseEntry::new();
            match self.db.get(txn, &key_entry, &mut data_entry)? {
                OperationStatus::Success => {
                    Some(decode_value(&self.value_binding, &data_entry)?)
                }
                _ => None,
            }
        };

        self.db.put(txn, &key_entry, &value_entry)?;
        Ok(old_value)
    }

    /// Removes the entry for `key` and returns the previous value, or
    /// `None` if no entry was present.
    pub fn remove(
        &self,
        txn: Option<&Transaction>,
        key: &K,
    ) -> Result<Option<V>> {
        if self.read_only {
            return Err(CollectionError::ReadOnly);
        }

        let key_entry = encode_key(&self.key_binding, key)?;

        let old_value = {
            let mut data_entry = noxu_db::DatabaseEntry::new();
            match self.db.get(txn, &key_entry, &mut data_entry)? {
                OperationStatus::Success => {
                    Some(decode_value(&self.value_binding, &data_entry)?)
                }
                _ => None,
            }
        };

        if old_value.is_some() {
            self.db.delete(txn, &key_entry)?;
        }
        Ok(old_value)
    }

    /// Returns whether `key` is present in the database.
    pub fn contains_key(
        &self,
        txn: Option<&Transaction>,
        key: &K,
    ) -> Result<bool> {
        let key_entry = encode_key(&self.key_binding, key)?;
        let mut data_entry = noxu_db::DatabaseEntry::new();
        match self.db.get(txn, &key_entry, &mut data_entry)? {
            OperationStatus::Success => Ok(true),
            _ => Ok(false),
        }
    }

    /// Returns the number of records.
    ///
    /// Goes to [`Database::count`] which Sprint 1A fixed for
    /// sorted-duplicate databases.
    pub fn len(&self, _txn: Option<&Transaction>) -> Result<usize> {
        // `Database::count` does not currently take a txn; the count
        // is a B-tree property.  The `_txn` parameter is preserved on
        // the API for future per-txn snapshotting and matches the
        // BDB-JE / noxu_db `count(txn)` signature.
        let n = self.db.count()?;
        Ok(usize::try_from(n).unwrap_or(usize::MAX))
    }

    /// Returns whether the database is empty.
    pub fn is_empty(&self, txn: Option<&Transaction>) -> Result<bool> {
        Ok(self.len(txn)? == 0)
    }

    /// Returns a snapshot iterator over every (key, value) pair.
    ///
    /// The iterator is materialised eagerly: at the call to `iter()`
    /// the cursor walks every record under `txn` and decodes every
    /// pair into the returned `Vec`-backed iterator.
    pub fn iter(
        &self,
        txn: Option<&Transaction>,
    ) -> Result<StoredIterator<(K, V)>> {
        let items = scan_records(
            self.db,
            txn,
            StartKey::None,
            ScanDirection::Forward,
            &self.key_binding,
            &self.value_binding,
            |k, v| (k, v),
        )?;
        Ok(StoredIterator::from_vec(items))
    }

    /// Returns a snapshot iterator over keys.
    pub fn keys(&self, txn: Option<&Transaction>) -> Result<StoredIterator<K>> {
        let items = scan_records(
            self.db,
            txn,
            StartKey::None,
            ScanDirection::Forward,
            &self.key_binding,
            &self.value_binding,
            |k, _v| k,
        )?;
        Ok(StoredIterator::from_vec(items))
    }

    /// Returns a snapshot iterator over values.
    pub fn values(
        &self,
        txn: Option<&Transaction>,
    ) -> Result<StoredIterator<V>> {
        let items = scan_records(
            self.db,
            txn,
            StartKey::None,
            ScanDirection::Forward,
            &self.key_binding,
            &self.value_binding,
            |_k, v| v,
        )?;
        Ok(StoredIterator::from_vec(items))
    }

    /// Removes every record from the database.
    ///
    /// Walks a cursor under `txn` and calls `delete` for each record
    /// it encounters.  When `txn` is `Some(&t)` every delete is part
    /// of the user txn and `clear` is atomic on commit/abort.  When
    /// `txn` is `None` each delete is its own auto-txn — concurrent
    /// readers may observe a partially-cleared database.
    pub fn clear(&self, txn: Option<&Transaction>) -> Result<()> {
        if self.read_only {
            return Err(CollectionError::ReadOnly);
        }

        let mut cursor = self.db.open_cursor(txn, None)?;
        let mut key = noxu_db::DatabaseEntry::new();
        let mut data = noxu_db::DatabaseEntry::new();

        while let OperationStatus::Success =
            cursor.get(&mut key, &mut data, Get::First, None)?
        {
            cursor.delete()?;
        }

        cursor.close()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use noxu_bind::{ByteArrayBinding, IntBinding, StringBinding};
    use noxu_db::{
        DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig,
    };
    use tempfile::TempDir;

    fn setup_env() -> (TempDir, Environment) {
        let td = TempDir::new().unwrap();
        let env_config = EnvironmentConfig::new(td.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true);
        let env = Environment::open(env_config).unwrap();
        (td, env)
    }

    fn open_db(env: &Environment, name: &str) -> noxu_db::Database {
        let db_config = DatabaseConfig::new().with_allow_create(true);
        env.open_database(None, name, &db_config).unwrap()
    }

    #[test]
    fn typed_put_get_round_trip() {
        let (_td, env) = setup_env();
        let db = open_db(&env, "typed_put_get");
        let map: StoredMap<'_, i32, String, _, _> =
            StoredMap::new(&db, IntBinding, StringBinding);

        let old = map.put(None, &1, &"alpha".to_string()).unwrap();
        assert!(old.is_none());

        assert_eq!(map.get(None, &1).unwrap(), Some("alpha".to_string()),);
        assert!(map.contains_key(None, &1).unwrap());
        assert!(!map.contains_key(None, &99).unwrap());
    }

    #[test]
    fn typed_put_returns_old_value() {
        let (_td, env) = setup_env();
        let db = open_db(&env, "typed_put_old");
        let map: StoredMap<'_, i32, String, _, _> =
            StoredMap::new(&db, IntBinding, StringBinding);

        map.put(None, &1, &"alpha".to_string()).unwrap();
        let old = map.put(None, &1, &"beta".to_string()).unwrap();
        assert_eq!(old, Some("alpha".to_string()));
        assert_eq!(map.get(None, &1).unwrap(), Some("beta".to_string()));
    }

    #[test]
    fn typed_remove_returns_old_value() {
        let (_td, env) = setup_env();
        let db = open_db(&env, "typed_remove");
        let map: StoredMap<'_, i32, String, _, _> =
            StoredMap::new(&db, IntBinding, StringBinding);

        map.put(None, &7, &"hello".to_string()).unwrap();
        let removed = map.remove(None, &7).unwrap();
        assert_eq!(removed, Some("hello".to_string()));
        assert_eq!(map.get(None, &7).unwrap(), None);

        // Removing a missing key returns None.
        assert_eq!(map.remove(None, &999).unwrap(), None);
    }

    #[test]
    fn typed_len_and_is_empty() {
        let (_td, env) = setup_env();
        let db = open_db(&env, "typed_len");
        let map: StoredMap<'_, i32, String, _, _> =
            StoredMap::new(&db, IntBinding, StringBinding);

        assert!(map.is_empty(None).unwrap());
        assert_eq!(map.len(None).unwrap(), 0);

        for i in 0..5 {
            map.put(None, &i, &format!("v{i}")).unwrap();
        }
        assert!(!map.is_empty(None).unwrap());
        assert_eq!(map.len(None).unwrap(), 5);
    }

    #[test]
    fn typed_iter_yields_decoded_pairs() {
        let (_td, env) = setup_env();
        let db = open_db(&env, "typed_iter");
        let map: StoredMap<'_, i32, String, _, _> =
            StoredMap::new(&db, IntBinding, StringBinding);

        map.put(None, &3, &"three".to_string()).unwrap();
        map.put(None, &1, &"one".to_string()).unwrap();
        map.put(None, &2, &"two".to_string()).unwrap();

        let items: Vec<(i32, String)> =
            map.iter(None).unwrap().map(Result::unwrap).collect();

        // IntBinding sorts numerically, so the natural cursor order is
        // 1, 2, 3.
        assert_eq!(items.len(), 3);
        assert_eq!(items[0], (1, "one".to_string()));
        assert_eq!(items[1], (2, "two".to_string()));
        assert_eq!(items[2], (3, "three".to_string()));
    }

    #[test]
    fn typed_keys_and_values() {
        let (_td, env) = setup_env();
        let db = open_db(&env, "typed_kv");
        let map: StoredMap<'_, i32, String, _, _> =
            StoredMap::new(&db, IntBinding, StringBinding);

        map.put(None, &1, &"one".to_string()).unwrap();
        map.put(None, &2, &"two".to_string()).unwrap();

        let keys: Vec<i32> =
            map.keys(None).unwrap().map(Result::unwrap).collect();
        assert_eq!(keys, vec![1, 2]);

        let values: Vec<String> =
            map.values(None).unwrap().map(Result::unwrap).collect();
        assert_eq!(values, vec!["one".to_string(), "two".to_string()]);
    }

    #[test]
    fn typed_clear_empties_database() {
        let (_td, env) = setup_env();
        let db = open_db(&env, "typed_clear");
        let map: StoredMap<'_, i32, String, _, _> =
            StoredMap::new(&db, IntBinding, StringBinding);

        for i in 0..10 {
            map.put(None, &i, &format!("v{i}")).unwrap();
        }
        assert_eq!(map.len(None).unwrap(), 10);

        map.clear(None).unwrap();
        assert_eq!(map.len(None).unwrap(), 0);
        assert!(map.iter(None).unwrap().next().is_none());
    }

    #[test]
    fn typed_read_only_rejects_writes() {
        let (_td, env) = setup_env();
        let db = open_db(&env, "typed_ro");
        let map: StoredMap<'_, i32, String, _, _> =
            StoredMap::new_read_only(&db, IntBinding, StringBinding);
        assert!(map.is_read_only());

        let r = map.put(None, &1, &"x".to_string());
        assert!(matches!(r, Err(CollectionError::ReadOnly)));

        let r = map.remove(None, &1);
        assert!(matches!(r, Err(CollectionError::ReadOnly)));

        let r = map.clear(None);
        assert!(matches!(r, Err(CollectionError::ReadOnly)));
    }

    #[test]
    fn typed_participates_in_user_txn_commit() {
        let (_td, env) = setup_env();
        let db = open_db(&env, "typed_txn_commit");
        let map: StoredMap<'_, i32, String, _, _> =
            StoredMap::new(&db, IntBinding, StringBinding);

        let txn = env.begin_transaction(None).unwrap();
        map.put(Some(&txn), &1, &"a".to_string()).unwrap();
        map.put(Some(&txn), &2, &"b".to_string()).unwrap();
        txn.commit().unwrap();

        assert_eq!(map.get(None, &1).unwrap(), Some("a".to_string()));
        assert_eq!(map.get(None, &2).unwrap(), Some("b".to_string()));
    }

    #[test]
    fn typed_participates_in_user_txn_abort() {
        let (_td, env) = setup_env();
        let db = open_db(&env, "typed_txn_abort");
        let map: StoredMap<'_, i32, String, _, _> =
            StoredMap::new(&db, IntBinding, StringBinding);

        // Pre-populate.
        map.put(None, &1, &"original".to_string()).unwrap();

        let txn = env.begin_transaction(None).unwrap();
        map.put(Some(&txn), &1, &"modified".to_string()).unwrap();
        map.put(Some(&txn), &2, &"new".to_string()).unwrap();
        txn.abort().unwrap();

        assert_eq!(map.get(None, &1).unwrap(), Some("original".to_string()));
        assert_eq!(map.get(None, &2).unwrap(), None);
    }

    #[test]
    fn byte_array_binding_round_trip() {
        let (_td, env) = setup_env();
        let db = open_db(&env, "byte_map");
        let map: StoredMap<'_, Vec<u8>, Vec<u8>, _, _> =
            StoredMap::new(&db, ByteArrayBinding, ByteArrayBinding);

        map.put(None, &b"hello".to_vec(), &b"world".to_vec()).unwrap();
        assert_eq!(
            map.get(None, &b"hello".to_vec()).unwrap(),
            Some(b"world".to_vec()),
        );
    }

    #[test]
    fn database_accessor() {
        let (_td, env) = setup_env();
        let db = open_db(&env, "accessor");
        let map: StoredMap<'_, i32, String, _, _> =
            StoredMap::new(&db, IntBinding, StringBinding);
        assert_eq!(map.database().get_database_name(), "accessor");
    }

    #[test]
    fn iter_visits_pre_existing_records_no_index_required() {
        let (_td, env) = setup_env();
        let db = open_db(&env, "preexisting");

        // Write directly through Database (no Stored* tracking).
        for i in 1u64..=5 {
            let k = DatabaseEntry::from_vec(i.to_be_bytes().to_vec());
            let v = DatabaseEntry::from_vec(format!("v{i}").into_bytes());
            db.put(None, &k, &v).unwrap();
        }

        // Open a typed map over the same database; iter() must see
        // every record without any "register_key" call.  This is the
        // central point of the Wave 2B redesign.
        let map: StoredMap<'_, Vec<u8>, Vec<u8>, _, _> =
            StoredMap::new(&db, ByteArrayBinding, ByteArrayBinding);
        let items: Vec<_> =
            map.iter(None).unwrap().map(Result::unwrap).collect();
        assert_eq!(items.len(), 5);
    }
}
