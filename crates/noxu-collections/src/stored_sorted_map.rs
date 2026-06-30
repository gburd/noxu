//! Typed sorted-map view of a database.
//!
//! `StoredSortedMap<K, V, KB, VB>` adds
//! sorted-map operations (`first_key`, `last_key`, `iter_from`,
//! `iter_reverse`) on top of [`StoredMap`].  Every operation accepts
//! `txn: Option<&Transaction>`, matching the BDB-JE shape.

use noxu_bind::EntryBinding;
use noxu_db::{Database, Transaction};

use crate::error::Result;
use crate::internal::{
    ScanDirection, StartKey, cursor_endpoint, decode_key, encode_key,
    scan_iter, scan_iter_owned_start,
};
use crate::stored_iterator::StoredIterator;
use crate::stored_map::StoredMap;

/// A typed sorted-map view of a database.
///
/// All `StoredMap` operations are forwarded to the inner map; this
/// type adds sorted-map navigation (`first_key`, `last_key`,
/// `iter_from`, `iter_reverse`).
pub struct StoredSortedMap<'db, K, V, KB, VB>
where
    KB: EntryBinding<K>,
    VB: EntryBinding<V>,
{
    inner: StoredMap<'db, K, V, KB, VB>,
}

impl<'db, K, V, KB, VB> StoredSortedMap<'db, K, V, KB, VB>
where
    KB: EntryBinding<K>,
    VB: EntryBinding<V>,
{
    /// Creates a new typed sorted-map view of the given database.
    pub fn new(db: &'db Database, key_binding: KB, value_binding: VB) -> Self {
        StoredSortedMap {
            inner: StoredMap::new(db, key_binding, value_binding),
        }
    }

    /// Creates a new read-only typed sorted-map view.
    pub fn new_read_only(
        db: &'db Database,
        key_binding: KB,
        value_binding: VB,
    ) -> Self {
        StoredSortedMap {
            inner: StoredMap::new_read_only(db, key_binding, value_binding),
        }
    }

    /// Returns whether this view is read-only.
    pub fn is_read_only(&self) -> bool {
        self.inner.is_read_only()
    }

    /// Returns a reference to the underlying database.
    pub fn database(&self) -> &'db Database {
        self.inner.database()
    }

    /// Returns a reference to the inner [`StoredMap`].
    pub fn as_map(&self) -> &StoredMap<'db, K, V, KB, VB> {
        &self.inner
    }

    /// Inserts or updates a key-value pair.  See [`StoredMap::put`].
    pub fn put(
        &self,
        txn: Option<&Transaction>,
        key: &K,
        value: &V,
    ) -> Result<Option<V>> {
        self.inner.put(txn, key, value)
    }

    /// Retrieves the value associated with the given key.
    pub fn get(&self, txn: Option<&Transaction>, key: &K) -> Result<Option<V>> {
        self.inner.get(txn, key)
    }

    /// Removes the entry for `key`.
    pub fn remove(
        &self,
        txn: Option<&Transaction>,
        key: &K,
    ) -> Result<Option<V>> {
        self.inner.remove(txn, key)
    }

    /// Returns whether `key` is present.
    pub fn contains_key(
        &self,
        txn: Option<&Transaction>,
        key: &K,
    ) -> Result<bool> {
        self.inner.contains_key(txn, key)
    }

    /// Returns the number of records.
    pub fn len(&self, txn: Option<&Transaction>) -> Result<usize> {
        self.inner.len(txn)
    }

    /// Returns whether the database is empty.
    pub fn is_empty(&self, txn: Option<&Transaction>) -> Result<bool> {
        self.inner.is_empty(txn)
    }

    /// Removes every record.
    pub fn clear(&self, txn: Option<&Transaction>) -> Result<()> {
        self.inner.clear(txn)
    }

    /// Lazy forward iterator over every (key, value) pair (review P1-7).
    /// See [`StoredMap::iter`](crate::StoredMap::iter) for the
    /// laziness/lifetime contract.
    pub fn iter<'a>(
        &'a self,
        txn: Option<&'a Transaction>,
    ) -> Result<impl Iterator<Item = Result<(K, V)>> + 'a>
    where
        K: 'a,
        V: 'a,
    {
        self.inner.iter(txn)
    }

    /// Lazy forward iterator over keys.
    pub fn keys<'a>(
        &'a self,
        txn: Option<&'a Transaction>,
    ) -> Result<impl Iterator<Item = Result<K>> + 'a>
    where
        K: 'a,
        V: 'a,
    {
        self.inner.keys(txn)
    }

    /// Lazy forward iterator over values.
    pub fn values<'a>(
        &'a self,
        txn: Option<&'a Transaction>,
    ) -> Result<impl Iterator<Item = Result<V>> + 'a>
    where
        K: 'a,
        V: 'a,
    {
        self.inner.values(txn)
    }

    /// Eager snapshot iterator over every (key, value) pair.
    /// See [`StoredMap::snapshot`](crate::StoredMap::snapshot).
    pub fn snapshot(
        &self,
        txn: Option<&Transaction>,
    ) -> Result<StoredIterator<(K, V)>> {
        self.inner.snapshot(txn)
    }

    /// Eager snapshot iterator over keys.
    pub fn keys_snapshot(
        &self,
        txn: Option<&Transaction>,
    ) -> Result<StoredIterator<K>> {
        self.inner.keys_snapshot(txn)
    }

    /// Eager snapshot iterator over values.
    pub fn values_snapshot(
        &self,
        txn: Option<&Transaction>,
    ) -> Result<StoredIterator<V>> {
        self.inner.values_snapshot(txn)
    }

    /// Returns the smallest key, or `None` if the database is empty.
    pub fn first_key(&self, txn: Option<&Transaction>) -> Result<Option<K>> {
        Ok(self.first_entry(txn)?.map(|(k, _)| k))
    }

    /// Returns the largest key, or `None` if the database is empty.
    pub fn last_key(&self, txn: Option<&Transaction>) -> Result<Option<K>> {
        Ok(self.last_entry(txn)?.map(|(k, _)| k))
    }

    /// Returns the (key, value) pair with the smallest key, or `None`.
    pub fn first_entry(
        &self,
        txn: Option<&Transaction>,
    ) -> Result<Option<(K, V)>> {
        cursor_endpoint(
            self.inner.database(),
            txn,
            self.inner.key_binding(),
            self.inner.value_binding(),
            noxu_db::Get::First,
        )
    }

    /// Returns the (key, value) pair with the largest key, or `None`.
    pub fn last_entry(
        &self,
        txn: Option<&Transaction>,
    ) -> Result<Option<(K, V)>> {
        cursor_endpoint(
            self.inner.database(),
            txn,
            self.inner.key_binding(),
            self.inner.value_binding(),
            noxu_db::Get::Last,
        )
    }

    /// Lazy forward iterator starting at `start_key` (inclusive lower
    /// bound).
    ///
    /// Encodes `start_key` via the key binding and walks the cursor
    /// from the smallest key `>= encoded(start_key)`.  Lazy (review
    /// P1-7); see [`iter`](Self::iter) for the lifetime contract.
    pub fn iter_from<'a>(
        &'a self,
        txn: Option<&'a Transaction>,
        start_key: &K,
    ) -> Result<impl Iterator<Item = Result<(K, V)>> + 'a>
    where
        K: 'a,
        V: 'a,
    {
        let start_entry = encode_key(self.inner.key_binding(), start_key)?;
        let bytes = start_entry.data_opt().unwrap_or(&[]).to_vec();
        scan_iter_owned_start(
            self.inner.database(),
            txn,
            Some(bytes),
            ScanDirection::Forward,
            self.inner.key_binding(),
            self.inner.value_binding(),
            |k, v| (k, v),
        )
    }

    /// Lazy reverse iterator over every (key, value) pair (largest key
    /// first).  See [`iter`](Self::iter) for the lifetime contract.
    pub fn iter_reverse<'a>(
        &'a self,
        txn: Option<&'a Transaction>,
    ) -> Result<impl Iterator<Item = Result<(K, V)>> + 'a>
    where
        K: 'a,
        V: 'a,
    {
        scan_iter(
            self.inner.database(),
            txn,
            StartKey::None,
            ScanDirection::Reverse,
            self.inner.key_binding(),
            self.inner.value_binding(),
            |k, v| (k, v),
        )
    }

    /// Returns the smallest key strictly greater than `key`, or `None`.
    ///
    /// Useful for stepping through keys when only the bindings are
    /// available.  Walks forward from `Get::First` and skips keys
    /// `<= bound` (the `noxu-dbi` `SearchGte`-then-`Next` path is
    /// known to mis-position; see `internal::scan_records` for the
    /// rationale).
    pub fn higher_key(
        &self,
        txn: Option<&Transaction>,
        key: &K,
    ) -> Result<Option<K>> {
        let key_entry = encode_key(self.inner.key_binding(), key)?;
        let bound = key_entry.data_opt().unwrap_or(&[]).to_vec();

        let mut cursor =
            crate::internal::open_cursor(self.inner.database(), txn, None)?;
        let mut k_buf = noxu_db::DatabaseEntry::new();
        let mut d_buf = noxu_db::DatabaseEntry::new();
        let mut status =
            cursor.get(&mut k_buf, &mut d_buf, noxu_db::Get::First, None)?;
        let mut result: Option<K> = None;
        while matches!(status, noxu_db::OperationStatus::Success) {
            let cur = k_buf.data_opt().unwrap_or(&[]);
            if cur > bound.as_slice() {
                result = Some(decode_key(self.inner.key_binding(), &k_buf)?);
                break;
            }
            status =
                cursor.get(&mut k_buf, &mut d_buf, noxu_db::Get::Next, None)?;
        }
        cursor.close()?;
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use noxu_bind::{IntBinding, StringBinding};
    use noxu_db::{DatabaseConfig, Environment, EnvironmentConfig};
    use tempfile::TempDir;

    fn setup() -> (TempDir, Environment, noxu_db::Database) {
        let td = TempDir::new().unwrap();
        let env = Environment::open(
            EnvironmentConfig::new(td.path().to_path_buf())
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap();
        let db = env
            .open_database(
                None,
                "ssm",
                &DatabaseConfig::new()
                    .with_allow_create(true)
                    .with_transactional(true),
            )
            .unwrap();
        (td, env, db)
    }

    fn populate(
        map: &StoredSortedMap<'_, i32, String, IntBinding, StringBinding>,
    ) {
        for (k, v) in
            [(3, "three"), (1, "one"), (2, "two"), (5, "five"), (4, "four")]
        {
            map.put(None, &k, &v.to_string()).unwrap();
        }
    }

    #[test]
    fn first_and_last_key() {
        let (_td, _env, db) = setup();
        let map: StoredSortedMap<'_, i32, String, _, _> =
            StoredSortedMap::new(&db, IntBinding, StringBinding);
        populate(&map);

        assert_eq!(map.first_key(None).unwrap(), Some(1));
        assert_eq!(map.last_key(None).unwrap(), Some(5));
    }

    #[test]
    fn first_and_last_entry() {
        let (_td, _env, db) = setup();
        let map: StoredSortedMap<'_, i32, String, _, _> =
            StoredSortedMap::new(&db, IntBinding, StringBinding);
        populate(&map);

        assert_eq!(
            map.first_entry(None).unwrap(),
            Some((1, "one".to_string())),
        );
        assert_eq!(
            map.last_entry(None).unwrap(),
            Some((5, "five".to_string())),
        );
    }

    #[test]
    fn first_last_empty() {
        let (_td, _env, db) = setup();
        let map: StoredSortedMap<'_, i32, String, _, _> =
            StoredSortedMap::new(&db, IntBinding, StringBinding);
        assert_eq!(map.first_key(None).unwrap(), None);
        assert_eq!(map.last_key(None).unwrap(), None);
        assert_eq!(map.first_entry(None).unwrap(), None);
        assert_eq!(map.last_entry(None).unwrap(), None);
    }

    #[test]
    fn iter_reverse() {
        let (_td, _env, db) = setup();
        let map: StoredSortedMap<'_, i32, String, _, _> =
            StoredSortedMap::new(&db, IntBinding, StringBinding);
        populate(&map);

        let items: Vec<_> =
            map.iter_reverse(None).unwrap().map(Result::unwrap).collect();
        let keys: Vec<i32> = items.iter().map(|(k, _)| *k).collect();
        assert_eq!(keys, vec![5, 4, 3, 2, 1]);
    }

    #[test]
    fn iter_from_inclusive() {
        let (_td, _env, db) = setup();
        let map: StoredSortedMap<'_, i32, String, _, _> =
            StoredSortedMap::new(&db, IntBinding, StringBinding);
        populate(&map);

        let items: Vec<_> =
            map.iter_from(None, &3).unwrap().map(Result::unwrap).collect();
        let keys: Vec<i32> = items.iter().map(|(k, _)| *k).collect();
        assert_eq!(keys, vec![3, 4, 5]);
    }

    #[test]
    fn iter_from_between_keys() {
        let (_td, _env, db) = setup();
        let map: StoredSortedMap<'_, i32, String, _, _> =
            StoredSortedMap::new(&db, IntBinding, StringBinding);
        // 1, 2, 4, 5
        for k in [1, 2, 4, 5] {
            map.put(None, &k, &format!("{k}")).unwrap();
        }
        // start key 3 → smallest key >= 3 is 4
        let items: Vec<_> =
            map.iter_from(None, &3).unwrap().map(Result::unwrap).collect();
        let keys: Vec<i32> = items.iter().map(|(k, _)| *k).collect();
        assert_eq!(keys, vec![4, 5]);
    }

    #[test]
    fn higher_key() {
        let (_td, _env, db) = setup();
        let map: StoredSortedMap<'_, i32, String, _, _> =
            StoredSortedMap::new(&db, IntBinding, StringBinding);
        populate(&map);

        assert_eq!(map.higher_key(None, &1).unwrap(), Some(2));
        assert_eq!(map.higher_key(None, &3).unwrap(), Some(4));
        assert_eq!(map.higher_key(None, &5).unwrap(), None);
        // For a key not in the map, we get the smallest key strictly
        // greater than it.  IntBinding sorts ints two's-complement so
        // 0 < 1 < ... < 5.
        assert_eq!(map.higher_key(None, &0).unwrap(), Some(1));
    }

    #[test]
    fn participates_in_user_txn() {
        let (_td, env, db) = setup();
        let map: StoredSortedMap<'_, i32, String, _, _> =
            StoredSortedMap::new(&db, IntBinding, StringBinding);

        let txn = env.begin_transaction(None).unwrap();
        map.put(Some(&txn), &1, &"one".to_string()).unwrap();
        map.put(Some(&txn), &2, &"two".to_string()).unwrap();
        assert_eq!(map.first_key(Some(&txn)).unwrap(), Some(1));
        txn.commit().unwrap();

        assert_eq!(map.first_key(None).unwrap(), Some(1));
    }
}
