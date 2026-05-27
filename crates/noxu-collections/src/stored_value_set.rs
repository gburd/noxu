//! Typed value-collection view of a database.
//!
//! Wave 2B redesign (v1.6).  `StoredValueSet<V, VB>` is a collection
//! view focused on the *values* of a Noxu database.  It mirrors
//! BDB-JE's `StoredValueSet` and is most useful in conjunction with a
//! sorted-duplicate database (one logical key, many values), although
//! the v1.6 surface works on any database — iteration order is the
//! natural cursor walk order.
//!
//! Note: in v1.6 sorted-duplicate semantics for the underlying
//! database are still v1.6-future work; this view treats every
//! record's value as an element of the multiset and ignores the key.

use std::marker::PhantomData;

use noxu_bind::EntryBinding;
use noxu_db::{Database, OperationStatus, Transaction};

use crate::error::Result;
use crate::stored_iterator::StoredIterator;

/// A typed collection view of database values.
pub struct StoredValueSet<'db, V, VB>
where
    VB: EntryBinding<V>,
{
    db: &'db Database,
    value_binding: VB,
    _marker: PhantomData<fn() -> V>,
}

impl<'db, V, VB> StoredValueSet<'db, V, VB>
where
    VB: EntryBinding<V>,
{
    /// Creates a new typed value-collection view.
    pub fn new(db: &'db Database, value_binding: VB) -> Self {
        StoredValueSet { db, value_binding, _marker: PhantomData }
    }

    /// Returns a reference to the underlying database.
    pub fn database(&self) -> &'db Database {
        self.db
    }

    /// Returns a reference to the value binding.
    pub fn value_binding(&self) -> &VB {
        &self.value_binding
    }

    /// Returns the number of records in the database.
    pub fn len(&self, _txn: Option<&Transaction>) -> Result<usize> {
        let n = self.db.count()?;
        Ok(usize::try_from(n).unwrap_or(usize::MAX))
    }

    /// Returns whether the database is empty.
    pub fn is_empty(&self, txn: Option<&Transaction>) -> Result<bool> {
        Ok(self.len(txn)? == 0)
    }

    /// Returns whether `value` is present anywhere in the database.
    ///
    /// This is `O(N)`: it walks every record under `txn`, decoding
    /// values until it finds a match (or exhausts the database).
    pub fn contains(&self, txn: Option<&Transaction>, value: &V) -> Result<bool>
    where
        V: PartialEq,
    {
        let mut cursor = self.db.open_cursor(txn, None)?;
        let mut key = noxu_db::DatabaseEntry::new();
        let mut data = noxu_db::DatabaseEntry::new();
        let mut status =
            cursor.get(&mut key, &mut data, noxu_db::Get::First, None)?;
        let mut found = false;
        while matches!(status, OperationStatus::Success) {
            let v = self.value_binding.entry_to_object(&data).map_err(|e| {
                crate::error::CollectionError::BindingError(e.to_string())
            })?;
            if &v == value {
                found = true;
                break;
            }
            status =
                cursor.get(&mut key, &mut data, noxu_db::Get::Next, None)?;
        }
        cursor.close()?;
        Ok(found)
    }

    /// Returns a snapshot iterator over every value.
    pub fn iter(&self, txn: Option<&Transaction>) -> Result<StoredIterator<V>> {
        use crate::internal::{ScanDirection, StartKey, scan_records};
        use noxu_bind::ByteArrayBinding;

        let key_binding = ByteArrayBinding;
        let items = scan_records::<Vec<u8>, V, ByteArrayBinding, VB, V, _>(
            self.db,
            txn,
            StartKey::None,
            ScanDirection::Forward,
            &key_binding,
            &self.value_binding,
            |_k, v| v,
        )?;
        Ok(StoredIterator::from_vec(items))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use noxu_bind::{IntBinding, StringBinding};
    use noxu_db::{DatabaseConfig, Environment, EnvironmentConfig};
    use tempfile::TempDir;

    use crate::stored_map::StoredMap;

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
                "vset",
                &DatabaseConfig::new().with_allow_create(true),
            )
            .unwrap();
        (td, env, db)
    }

    #[test]
    fn iter_yields_values_in_key_order() {
        let (_td, _env, db) = setup();
        let map: StoredMap<'_, i32, String, _, _> =
            StoredMap::new(&db, IntBinding, StringBinding);
        for (k, v) in [(3, "three"), (1, "one"), (2, "two")] {
            map.put(None, &k, &v.to_string()).unwrap();
        }

        let set: StoredValueSet<'_, String, _> =
            StoredValueSet::new(&db, StringBinding);
        let values: Vec<String> =
            set.iter(None).unwrap().map(Result::unwrap).collect();
        assert_eq!(
            values,
            vec!["one".to_string(), "two".to_string(), "three".to_string()],
        );
    }

    #[test]
    fn contains_walks_values() {
        let (_td, _env, db) = setup();
        let map: StoredMap<'_, i32, String, _, _> =
            StoredMap::new(&db, IntBinding, StringBinding);
        map.put(None, &1, &"alpha".to_string()).unwrap();
        map.put(None, &2, &"beta".to_string()).unwrap();

        let set: StoredValueSet<'_, String, _> =
            StoredValueSet::new(&db, StringBinding);
        assert!(set.contains(None, &"alpha".to_string()).unwrap());
        assert!(set.contains(None, &"beta".to_string()).unwrap());
        assert!(!set.contains(None, &"missing".to_string()).unwrap());
    }

    #[test]
    fn len_and_is_empty() {
        let (_td, _env, db) = setup();
        let set: StoredValueSet<'_, String, _> =
            StoredValueSet::new(&db, StringBinding);
        assert!(set.is_empty(None).unwrap());

        let map: StoredMap<'_, i32, String, _, _> =
            StoredMap::new(&db, IntBinding, StringBinding);
        for i in 0..3 {
            map.put(None, &i, &format!("v{i}")).unwrap();
        }
        assert_eq!(set.len(None).unwrap(), 3);
    }
}
