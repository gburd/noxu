//! Typed key-set view of a database.
//!
//! `StoredKeySet<K, KB>` exposes a set
//! interface over the *keys* of a Noxu database.  Values stored
//! under those keys are opaque payloads (the canonical use is
//! `()` / a single byte) and are never decoded.

use std::marker::PhantomData;

use noxu_bind::EntryBinding;
use noxu_db::{Database, OperationStatus, Transaction};

use crate::error::{CollectionError, Result};
use crate::internal::encode_key;
use crate::stored_iterator::StoredIterator;

/// A typed set view of database keys.
///
/// Iteration produces decoded keys (type `K`) in the natural order
/// imposed by the on-disk byte representation of the key (i.e. the
/// order produced by `EntryBinding::object_to_entry`).
pub struct StoredKeySet<'db, K, KB>
where
    KB: EntryBinding<K>,
{
    db: &'db Database,
    key_binding: KB,
    read_only: bool,
    _marker: PhantomData<fn() -> K>,
}

impl<'db, K, KB> StoredKeySet<'db, K, KB>
where
    KB: EntryBinding<K>,
{
    /// Creates a new typed key-set view of the given database.
    pub fn new(db: &'db Database, key_binding: KB) -> Self {
        StoredKeySet { db, key_binding, read_only: false, _marker: PhantomData }
    }

    /// Creates a new read-only typed key-set view.
    pub fn new_read_only(db: &'db Database, key_binding: KB) -> Self {
        StoredKeySet { db, key_binding, read_only: true, _marker: PhantomData }
    }

    /// Returns whether the view is read-only.
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

    /// Adds `key` to the set, storing an empty value under it.
    ///
    /// Returns `true` if the key was newly inserted, `false` if it
    /// was already present (matching `java.util.Set.add` semantics).
    pub fn add(&self, txn: Option<&Transaction>, key: &K) -> Result<bool> {
        if self.read_only {
            return Err(CollectionError::ReadOnly);
        }
        let key_entry = encode_key(&self.key_binding, key)?;
        // JE StoredKeySet.add uses putNoOverwrite — a single ATOMIC op that
        // returns whether the key was new. The prior get-then-put was a TOCTOU
        // (two threads could both observe "absent" and both return true).
        // Empty value payload; `StoredKeySet` is set-of-keys.
        let empty = noxu_db::DatabaseEntry::from_bytes(b"");
        if crate::internal::db_put_no_overwrite(
            self.db, txn, &key_entry, &empty,
        )? {
            Ok(true)
        } else {
            // KeyExists => already present; Set.add returns false (unchanged).
            Ok(false)
        }
    }

    /// Returns whether `key` is in the set.
    pub fn contains(&self, txn: Option<&Transaction>, key: &K) -> Result<bool> {
        let key_entry = encode_key(&self.key_binding, key)?;
        Ok(crate::internal::db_get(self.db, txn, &key_entry)?.is_some())
    }

    /// Removes `key` from the set.  Returns whether the key was
    /// present before the call.
    pub fn remove(&self, txn: Option<&Transaction>, key: &K) -> Result<bool> {
        if self.read_only {
            return Err(CollectionError::ReadOnly);
        }
        let key_entry = encode_key(&self.key_binding, key)?;
        let deleted = crate::internal::db_delete(self.db, txn, &key_entry)?;
        Ok(deleted)
    }

    /// Returns the number of elements.
    pub fn len(&self, _txn: Option<&Transaction>) -> Result<usize> {
        let n = self.db.count()?;
        Ok(usize::try_from(n).unwrap_or(usize::MAX))
    }

    /// Returns whether the set is empty.
    pub fn is_empty(&self, txn: Option<&Transaction>) -> Result<bool> {
        Ok(self.len(txn)? == 0)
    }

    /// Returns a snapshot iterator over every key.
    pub fn iter(&self, txn: Option<&Transaction>) -> Result<StoredIterator<K>> {
        // We don't care about values here — use `ByteArrayBinding` for
        // the value side and discard it.  Decoding the empty payload
        // through the byte-array binding is a single-allocation
        // no-op.
        use crate::internal::{ScanDirection, StartKey, scan_records};
        use noxu_bind::ByteArrayBinding;

        let value_binding = ByteArrayBinding;
        let items = scan_records::<K, Vec<u8>, KB, ByteArrayBinding, K, _>(
            self.db,
            txn,
            StartKey::None,
            ScanDirection::Forward,
            &self.key_binding,
            &value_binding,
            |k, _v| k,
        )?;
        Ok(StoredIterator::from_vec(items))
    }

    /// Removes every element.
    pub fn clear(&self, txn: Option<&Transaction>) -> Result<()> {
        if self.read_only {
            return Err(CollectionError::ReadOnly);
        }
        let mut cursor = crate::internal::open_cursor(self.db, txn, None)?;
        let mut key = noxu_db::DatabaseEntry::new();
        let mut data = noxu_db::DatabaseEntry::new();
        while let OperationStatus::Success =
            cursor.get(&mut key, &mut data, noxu_db::Get::First, None)?
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
    use noxu_bind::IntBinding;
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
                "kset",
                &DatabaseConfig::new()
                    .with_allow_create(true)
                    .with_transactional(true),
            )
            .unwrap();
        (td, env, db)
    }

    #[test]
    fn add_and_contains() {
        let (_td, _env, db) = setup();
        let set: StoredKeySet<'_, i32, _> = StoredKeySet::new(&db, IntBinding);

        assert!(set.add(None, &1).unwrap());
        assert!(!set.add(None, &1).unwrap()); // already present
        assert!(set.contains(None, &1).unwrap());
        assert!(!set.contains(None, &2).unwrap());
    }

    #[test]
    fn remove_returns_presence() {
        let (_td, _env, db) = setup();
        let set: StoredKeySet<'_, i32, _> = StoredKeySet::new(&db, IntBinding);

        set.add(None, &1).unwrap();
        assert!(set.remove(None, &1).unwrap());
        assert!(!set.remove(None, &1).unwrap()); // already gone
        assert!(!set.contains(None, &1).unwrap());
    }

    #[test]
    fn iter_yields_keys_in_order() {
        let (_td, _env, db) = setup();
        let set: StoredKeySet<'_, i32, _> = StoredKeySet::new(&db, IntBinding);
        for i in [3, 1, 2] {
            set.add(None, &i).unwrap();
        }
        let keys: Vec<i32> =
            set.iter(None).unwrap().map(Result::unwrap).collect();
        assert_eq!(keys, vec![1, 2, 3]);
    }

    #[test]
    fn clear_empties() {
        let (_td, _env, db) = setup();
        let set: StoredKeySet<'_, i32, _> = StoredKeySet::new(&db, IntBinding);
        for i in 0..5 {
            set.add(None, &i).unwrap();
        }
        assert_eq!(set.len(None).unwrap(), 5);
        set.clear(None).unwrap();
        assert_eq!(set.len(None).unwrap(), 0);
    }

    #[test]
    fn participates_in_user_txn() {
        let (_td, env, db) = setup();
        let set: StoredKeySet<'_, i32, _> = StoredKeySet::new(&db, IntBinding);
        let txn = env.begin_transaction(None).unwrap();
        set.add(Some(&txn), &7).unwrap();
        assert!(set.contains(Some(&txn), &7).unwrap());
        txn.abort().unwrap();
        // Aborted: not present.
        assert!(!set.contains(None, &7).unwrap());
    }

    #[test]
    fn read_only_rejects_writes() {
        let (_td, _env, db) = setup();
        let set: StoredKeySet<'_, i32, _> =
            StoredKeySet::new_read_only(&db, IntBinding);
        assert!(matches!(set.add(None, &1), Err(CollectionError::ReadOnly)));
        assert!(matches!(set.remove(None, &1), Err(CollectionError::ReadOnly)));
        assert!(matches!(set.clear(None), Err(CollectionError::ReadOnly)));
    }
}
