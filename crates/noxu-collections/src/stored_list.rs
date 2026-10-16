//! Typed list view of a database.
//!
//! `StoredList<V, VB>` is a sequence-indexed
//! list backed by a Noxu database.  Indices are 0-based `usize`
//! values encoded as 8-byte big-endian keys; iteration order matches
//! insertion order.
//!
//! # Compaction
//!
//! Unlike the v1.5 stop-gap, v1.6 `remove(idx)` performs **shift-down
//! compaction**: every record at index `i > idx` is read, written back
//! at `i - 1`, and the original is deleted.  `next_index` is then
//! decremented by 1.  This matches BDB-JE's
//! `StoredList.remove(int index)` contract and `Vec::remove`'s
//! semantics.
//!
//! Cost: `O(N - idx)` database operations per remove.  The whole
//! compaction runs under ONE transaction (an auto-commit transaction
//! when `txn` is `None`), so a crash mid-compaction rolls the entire
//! renumber back -- the list is never left partially compacted
//! (FID-DPL-18; JE RECNO renumber within the operation txn).
//!
//! # Index assignment
//!
//! Indices are persistent, engine-derived record numbers: `push` reads
//! the current last key under the operation transaction and appends at
//! `last + 1` with `putNoOverwrite` (FID-DPL-17; JE `DataView.append` /
//! `StoredList` RECNO append).  There is no process-local counter, so two
//! handles on the same database -- or a fresh handle on an existing one
//! -- always agree on the next index and never overwrite an existing
//! element.

use std::marker::PhantomData;

use noxu_bind::EntryBinding;
use noxu_db::{
    Database, DatabaseEntry, Get, LockMode, OperationStatus, Transaction,
};

use crate::error::{CollectionError, Result};
use crate::internal::{decode_value, encode_value};

/// A typed list-like view of a database.
///
/// # Index encoding
///
/// Indices are 8-byte big-endian `u64` keys, so byte-lex order on the
/// underlying database matches numeric order on the index.  The
/// largest index a list can hold is `u64::MAX`.
///
/// # Concurrency
///
/// The next index is derived from the persistent last key under the
/// operation transaction (FID-DPL-17), not a process-local counter, so
/// multiple handles -- in the same process or across processes -- always
/// agree.  Concurrent `push(None, ..)` calls serialise through the
/// auto-commit transaction's write lock on the tail, so each gets a
/// distinct record number (`putNoOverwrite` rejects a collision).
pub struct StoredList<'db, V, VB>
where
    VB: EntryBinding<V>,
{
    db: &'db Database,
    value_binding: VB,
    read_only: bool,
    _marker: PhantomData<fn() -> V>,
}

impl<'db, V, VB> StoredList<'db, V, VB>
where
    VB: EntryBinding<V>,
{
    /// Creates a new list view of the given database.
    ///
    /// The next index is always derived from the persistent last key
    /// (FID-DPL-17), so this view sees existing records correctly whether
    /// the database is empty or already populated; [`StoredList::open`]
    /// is retained as an explicit alias that also validates the key
    /// width.
    pub fn new(db: &'db Database, value_binding: VB) -> Self {
        StoredList { db, value_binding, read_only: false, _marker: PhantomData }
    }

    /// Opens a list view over an existing database, recovering the
    /// next-index counter from the largest existing key.
    ///
    /// `open` walks the underlying database with a single
    /// `Get::Last` cursor read.  If the database is empty,
    /// `next_index` is initialised to 0.  If the database already
    /// contains records, the largest 8-byte big-endian key is decoded
    /// as a `u64` and `next_index` is set to `last + 1`.
    ///
    /// Returns [`CollectionError::IllegalState`] if the largest key
    /// is not 8 bytes long (i.e. the database was not produced by
    /// `StoredList`).
    pub fn open(db: &'db Database, value_binding: VB) -> Result<Self> {
        Self::open_with_txn(db, value_binding, None)
    }

    /// Like [`Self::open`], but uses the supplied txn for the
    /// recovery scan.  Allows the list to be opened from inside a
    /// user transaction.
    pub fn open_with_txn(
        db: &'db Database,
        value_binding: VB,
        txn: Option<&Transaction>,
    ) -> Result<Self> {
        let mut cursor = crate::internal::open_cursor(db, txn, None)?;
        let mut key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();
        let next = match cursor.get(&mut key, &mut data, Get::Last, None)? {
            OperationStatus::Success => {
                let bytes = key.data_opt().unwrap_or(&[]);
                if bytes.len() != 8 {
                    let _ = cursor.close();
                    return Err(CollectionError::IllegalState(format!(
                        "StoredList::open: largest key is {} bytes; \
                         expected an 8-byte big-endian index. Database \
                         was not produced by StoredList; use \
                         StoredList::new explicitly if this is intentional.",
                        bytes.len()
                    )));
                }
                let mut buf = [0u8; 8];
                buf.copy_from_slice(bytes);
                let last = u64::from_be_bytes(buf);
                last.saturating_add(1) as usize
            }
            _ => 0,
        };
        cursor.close()?;
        // `next` is validated above purely to reject a non-StoredList
        // database (non-8-byte key); the authoritative next index is
        // always re-derived from the persistent last key per operation
        // (FID-DPL-17), so it is not cached here.
        let _ = next;
        Ok(StoredList {
            db,
            value_binding,
            read_only: false,
            _marker: PhantomData,
        })
    }

    /// Marks the list view as read-only (mutating ops fail with
    /// [`CollectionError::ReadOnly`]).
    pub fn into_read_only(mut self) -> Self {
        self.read_only = true;
        self
    }

    /// Returns whether the view is read-only.
    pub fn is_read_only(&self) -> bool {
        self.read_only
    }

    /// Returns a reference to the underlying database.
    pub fn database(&self) -> &'db Database {
        self.db
    }

    /// Returns a reference to the value binding.
    pub fn value_binding(&self) -> &VB {
        &self.value_binding
    }

    /// Encodes a `usize` index as an 8-byte big-endian key.
    pub fn index_to_key(index: usize) -> [u8; 8] {
        (index as u64).to_be_bytes()
    }

    /// Returns the next index that would be used by `push`.
    ///
    /// Derived from the persistent last key (FID-DPL-17): `last + 1`, or
    /// `0` when the list is empty.  Auto-commit read.
    pub fn next_index(&self) -> usize {
        self.persistent_end(None).unwrap_or(0)
    }

    /// The persistent end-of-list index (`last key + 1`, or `0` when
    /// empty), read under `txn` via a single `Get::Last` cursor.  This is
    /// the authoritative, engine-derived high-water mark (FID-DPL-17) --
    /// there is no process-local counter.
    ///
    /// Positions with a write lock (`LockMode::Rmw`) when `txn` is the
    /// operation's own transaction so a concurrent appender cannot read
    /// the same tail and collide; a plain read (`None` lock mode) is used
    /// for the auto-commit `next_index()` / `len()` query.
    fn persistent_end(&self, txn: Option<&Transaction>) -> Result<usize> {
        self.persistent_end_locked(txn, false)
    }

    fn persistent_end_locked(
        &self,
        txn: Option<&Transaction>,
        write_lock: bool,
    ) -> Result<usize> {
        let mut cursor = crate::internal::open_cursor(self.db, txn, None)?;
        let mut key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();
        let lock = if write_lock { Some(LockMode::Rmw) } else { None };
        let end = match cursor.get(&mut key, &mut data, Get::Last, lock)? {
            OperationStatus::Success => {
                let bytes = key.data_opt().unwrap_or(&[]);
                if bytes.len() != 8 {
                    let _ = cursor.close();
                    return Err(CollectionError::IllegalState(format!(
                        "StoredList: largest key is {} bytes; expected an \
                         8-byte big-endian index",
                        bytes.len()
                    )));
                }
                let mut buf = [0u8; 8];
                buf.copy_from_slice(bytes);
                u64::from_be_bytes(buf).saturating_add(1) as usize
            }
            _ => 0,
        };
        cursor.close()?;
        Ok(end)
    }

    /// Run `op` under ONE transaction: the caller's `txn` when `Some`, or
    /// a fresh auto-commit transaction (via
    /// [`noxu_db::Database::run_in_auto_commit`]) when `None`.  This is the
    /// JE `beginAutoCommit` / `commitAutoCommit` boundary: on a
    /// transactional database the auto-commit path gets a real auto-txn
    /// (`Some`), on a non-transactional one it runs with no txn (`None`),
    /// and the op commits on `Ok` / aborts on `Err`.
    ///
    /// A non-database [`CollectionError`] produced inside the auto-commit
    /// path (a decode failure, a `putNoOverwrite` collision) is stashed
    /// and restored verbatim after the engine aborts the auto-txn, so the
    /// caller sees the exact error (not a lossy string).
    fn in_txn<T, F>(&self, txn: Option<&Transaction>, op: F) -> Result<T>
    where
        F: FnOnce(&Database, Option<&Transaction>) -> Result<T>,
    {
        match txn {
            Some(_) => op(self.db, txn),
            None => {
                let mut stash: Option<CollectionError> = None;
                let mut op = Some(op);
                let res = self.db.run_in_auto_commit(|t| {
                    let op = op.take().expect("invariant: op run once");
                    match op(self.db, t) {
                        Ok(v) => Ok(v),
                        // A DB error flows through unchanged so the engine
                        // aborts with the real NoxuError.
                        Err(CollectionError::DatabaseError(e)) => Err(e),
                        // A non-DB collection error: stash it, trigger the
                        // abort with a sentinel, restore it below.
                        Err(other) => {
                            stash = Some(other);
                            Err(noxu_db::NoxuError::OperationNotAllowed(
                                "StoredList auto-commit op failed".to_string(),
                            ))
                        }
                    }
                });
                match res {
                    Ok(v) => Ok(v),
                    Err(e) => Err(stash.unwrap_or_else(|| e.into())),
                }
            }
        }
    }

    /// Appends `value` to the end of the list and returns the
    /// assigned index.
    pub fn push(&self, txn: Option<&Transaction>, value: &V) -> Result<usize> {
        if self.read_only {
            return Err(CollectionError::ReadOnly);
        }
        let value_entry = encode_value(&self.value_binding, value)?;
        // FID-DPL-17: derive the index from the PERSISTENT last key under
        // the operation transaction (write-locking the tail so a
        // concurrent appender cannot pick the same index), then append
        // with putNoOverwrite -- a stale index can never silently
        // overwrite an existing element (JE DataView.append).  The whole
        // read-end + append is one transaction.
        self.in_txn(txn, |db, t| {
            let index = self.persistent_end_locked(t, true)?;
            let key_bytes = Self::index_to_key(index);
            let key_entry = DatabaseEntry::from_bytes(&key_bytes);
            let inserted = crate::internal::db_put_no_overwrite(
                db,
                t,
                &key_entry,
                &value_entry,
            )?;
            if !inserted {
                // The tail write lock makes this unreachable in practice;
                // surface it rather than silently overwrite (JE's
                // putNoOverwrite contract).
                return Err(CollectionError::ConcurrentModification);
            }
            Ok(index)
        })
    }

    /// Retrieves the value at `index`, or `None` if the slot is empty
    /// (e.g. past the high-water mark or removed by a previous
    /// `remove` that then crashed mid-shift).
    pub fn get(
        &self,
        txn: Option<&Transaction>,
        index: usize,
    ) -> Result<Option<V>> {
        let key_bytes = Self::index_to_key(index);
        let key_entry = DatabaseEntry::from_bytes(&key_bytes);
        match crate::internal::db_get(self.db, txn, &key_entry)? {
            Some(bytes) => {
                let data_entry = DatabaseEntry::from_bytes(&bytes);
                Ok(Some(decode_value(&self.value_binding, &data_entry)?))
            }
            None => Ok(None),
        }
    }

    /// Removes and returns the last element.  Returns `None` if the
    /// list is empty.
    pub fn pop(&self, txn: Option<&Transaction>) -> Result<Option<V>> {
        if self.read_only {
            return Err(CollectionError::ReadOnly);
        }
        // Read-end + read-last + delete-last is one transaction so pop is
        // atomic and sees the persistent tail (FID-DPL-17/18).
        self.in_txn(txn, |db, t| {
            let end = self.persistent_end_locked(t, true)?;
            if end == 0 {
                return Ok(None);
            }
            let index = end - 1;
            let key_bytes = Self::index_to_key(index);
            let key_entry = DatabaseEntry::from_bytes(&key_bytes);
            let val = match crate::internal::db_get(db, t, &key_entry)? {
                Some(bytes) => {
                    let data_entry = DatabaseEntry::from_bytes(&bytes);
                    Some(decode_value(&self.value_binding, &data_entry)?)
                }
                None => None,
            };
            if val.is_some() {
                crate::internal::db_delete(db, t, &key_entry)?;
            }
            Ok(val)
        })
    }

    /// Removes the value at `index` with shift-down compaction.
    ///
    /// Every record at indices `index + 1 .. next_index` is moved
    /// down by one slot, so after the call the list is dense again
    /// and `next_index` is decremented by 1.  Returns the removed
    /// value, or `None` if no value was stored at `index`.
    ///
    /// # Atomicity
    ///
    /// The whole compaction runs under ONE transaction -- the caller's
    /// `txn` when `Some`, or a single auto-commit transaction when `None`
    /// (FID-DPL-18).  A crash or error mid-compaction rolls the entire
    /// renumber back, so the list is never left with duplicate or missing
    /// elements (JE RECNO renumber within the operation txn).
    pub fn remove(
        &self,
        txn: Option<&Transaction>,
        index: usize,
    ) -> Result<Option<V>> {
        if self.read_only {
            return Err(CollectionError::ReadOnly);
        }
        // FID-DPL-18: the whole read-target + shift-down renumber runs in
        // ONE transaction (an auto-commit txn when `txn` is `None`), so a
        // crash or error mid-compaction rolls the entire renumber back --
        // never a duplicated or missing element (JE RECNO renumber within
        // the operation txn).
        self.in_txn(txn, |db, t| {
            // Write-lock the tail and read the persistent end.
            let high = self.persistent_end_locked(t, true)?;
            if index >= high {
                return Ok(None);
            }

            // Snapshot the removed value so we can return it.
            let target_key_bytes = Self::index_to_key(index);
            let target_key = DatabaseEntry::from_bytes(&target_key_bytes);
            let removed = match crate::internal::db_get(db, t, &target_key)? {
                Some(bytes) => {
                    let target_data = DatabaseEntry::from_bytes(&bytes);
                    Some(decode_value(&self.value_binding, &target_data)?)
                }
                None => None,
            };

            // Shift every subsequent record down by one slot.
            for src in (index + 1)..high {
                let dst = src - 1;
                let src_key_bytes = Self::index_to_key(src);
                let dst_key_bytes = Self::index_to_key(dst);
                let src_key = DatabaseEntry::from_bytes(&src_key_bytes);
                let dst_key = DatabaseEntry::from_bytes(&dst_key_bytes);

                match crate::internal::db_get(db, t, &src_key)? {
                    Some(bytes) => {
                        let dst_value = DatabaseEntry::from_vec(bytes);
                        crate::internal::db_put(db, t, &dst_key, &dst_value)?;
                        crate::internal::db_delete(db, t, &src_key)?;
                    }
                    None => {
                        crate::internal::db_delete(db, t, &dst_key)?;
                    }
                }
            }

            // If we never entered the loop (index == high - 1), delete the
            // target slot itself.
            if index == high.saturating_sub(1) && removed.is_some() {
                crate::internal::db_delete(db, t, &target_key)?;
            }
            Ok(removed)
        })
    }

    /// Returns the number of elements (the persistent high-water mark,
    /// `last key + 1`), read under `txn` (FID-DPL-17).
    pub fn len(&self, txn: Option<&Transaction>) -> Result<usize> {
        self.persistent_end(txn)
    }

    /// Returns whether the list is empty.
    pub fn is_empty(&self, txn: Option<&Transaction>) -> Result<bool> {
        Ok(self.len(txn)? == 0)
    }

    /// Returns a **lazy** iterator over every value, in index order
    /// (review P1-7).
    ///
    /// O(1) to create; holds a live cursor and decodes one value per
    /// `next()`.  When `txn` is `Some(&t)` the iterator borrows `t`.
    pub fn iter<'a>(
        &'a self,
        txn: Option<&'a Transaction>,
    ) -> Result<impl Iterator<Item = Result<V>> + 'a>
    where
        V: 'a,
    {
        use crate::internal::{
            BYTE_ARRAY_BINDING, ScanDirection, StartKey, scan_iter,
        };
        scan_iter(
            self.db,
            txn,
            StartKey::None,
            ScanDirection::Forward,
            &BYTE_ARRAY_BINDING,
            &self.value_binding,
            |_k, v| v,
        )
    }

    /// Removes every element.
    pub fn clear(&self, txn: Option<&Transaction>) -> Result<()> {
        if self.read_only {
            return Err(CollectionError::ReadOnly);
        }
        // One transaction for the whole clear (FID-DPL-15/18 shape); the
        // persistent high-water mark drops to 0 because every key is gone.
        match txn {
            Some(_) => crate::internal::clear_all(self.db, txn)?,
            None => self.db.run_in_auto_commit(|t| {
                crate::internal::clear_all(self.db, t)
            })?,
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use noxu_bind::StringBinding;
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
                "list",
                &DatabaseConfig::new()
                    .with_allow_create(true)
                    .with_transactional(true),
            )
            .unwrap();
        (td, env, db)
    }

    #[test]
    fn push_and_get() {
        let (_td, _env, db) = setup();
        let list: StoredList<'_, String, _> =
            StoredList::new(&db, StringBinding);

        assert_eq!(list.push(None, &"a".to_string()).unwrap(), 0);
        assert_eq!(list.push(None, &"b".to_string()).unwrap(), 1);
        assert_eq!(list.get(None, 0).unwrap(), Some("a".to_string()));
        assert_eq!(list.get(None, 1).unwrap(), Some("b".to_string()));
        assert_eq!(list.get(None, 99).unwrap(), None);
        assert_eq!(list.len(None).unwrap(), 2);
    }

    #[test]
    fn pop_returns_last() {
        let (_td, _env, db) = setup();
        let list: StoredList<'_, String, _> =
            StoredList::new(&db, StringBinding);

        list.push(None, &"a".to_string()).unwrap();
        list.push(None, &"b".to_string()).unwrap();
        assert_eq!(list.pop(None).unwrap(), Some("b".to_string()));
        assert_eq!(list.next_index(), 1);
        assert_eq!(list.pop(None).unwrap(), Some("a".to_string()));
        assert_eq!(list.pop(None).unwrap(), None);
    }

    /// Compaction test: insert 10 items, remove every other
    /// element, iterate, assert no gaps and the values are in the
    /// expected order.  This is the contract the prompt specifies.
    #[test]
    fn remove_compacts_no_gaps() {
        let (_td, _env, db) = setup();
        let list: StoredList<'_, String, _> =
            StoredList::new(&db, StringBinding);

        // Insert 10 items.
        for i in 0..10 {
            list.push(None, &format!("v{i}")).unwrap();
        }
        assert_eq!(list.next_index(), 10);

        // Remove every other element of the *original* list (indices
        // 1, 3, 5, 7, 9).  After each remove the list compacts down,
        // so the surviving original-index `2k+1` is at current index
        // `k+1` after the previous `k` removes.  In other words: the
        // sequence of `index` arguments to `remove` is 1, 2, 3, 4, 5.
        for current_idx in 1..=5 {
            let removed = list.remove(None, current_idx).unwrap();
            assert!(
                removed.is_some(),
                "remove({}) must return Some",
                current_idx,
            );
        }

        // After 5 removes the list has 5 elements.
        assert_eq!(list.len(None).unwrap(), 5);
        assert_eq!(list.next_index(), 5);

        // No gaps: every index 0..5 has a value.
        let collected: Vec<Option<String>> =
            (0..5).map(|i| list.get(None, i).unwrap()).collect();
        assert!(
            collected.iter().all(|v| v.is_some()),
            "expected dense list, got {:?}",
            collected,
        );

        // The retained values are the original even-indexed entries
        // in their original order.
        let expected = vec![
            "v0".to_string(),
            "v2".to_string(),
            "v4".to_string(),
            "v6".to_string(),
            "v8".to_string(),
        ];
        let actual: Vec<String> =
            collected.into_iter().map(Option::unwrap).collect();
        assert_eq!(actual, expected);

        // iter() also yields the values in the same order with no
        // gaps.
        let via_iter: Vec<String> =
            list.iter(None).unwrap().map(Result::unwrap).collect();
        assert_eq!(via_iter, expected);
    }

    #[test]
    fn remove_at_head_compacts() {
        let (_td, _env, db) = setup();
        let list: StoredList<'_, String, _> =
            StoredList::new(&db, StringBinding);
        for i in 0..3 {
            list.push(None, &format!("v{i}")).unwrap();
        }
        let removed = list.remove(None, 0).unwrap();
        assert_eq!(removed, Some("v0".to_string()));
        // After head removal: [v1, v2] at indices 0, 1.
        assert_eq!(list.get(None, 0).unwrap(), Some("v1".to_string()));
        assert_eq!(list.get(None, 1).unwrap(), Some("v2".to_string()));
        assert_eq!(list.get(None, 2).unwrap(), None);
        assert_eq!(list.next_index(), 2);
    }

    #[test]
    fn remove_at_tail_compacts() {
        let (_td, _env, db) = setup();
        let list: StoredList<'_, String, _> =
            StoredList::new(&db, StringBinding);
        for i in 0..3 {
            list.push(None, &format!("v{i}")).unwrap();
        }
        // Remove the last element (index 2).
        let removed = list.remove(None, 2).unwrap();
        assert_eq!(removed, Some("v2".to_string()));
        assert_eq!(list.get(None, 0).unwrap(), Some("v0".to_string()));
        assert_eq!(list.get(None, 1).unwrap(), Some("v1".to_string()));
        assert_eq!(list.get(None, 2).unwrap(), None);
        assert_eq!(list.next_index(), 2);
    }

    #[test]
    fn remove_out_of_range_returns_none() {
        let (_td, _env, db) = setup();
        let list: StoredList<'_, String, _> =
            StoredList::new(&db, StringBinding);
        list.push(None, &"a".to_string()).unwrap();
        assert_eq!(list.remove(None, 5).unwrap(), None);
        assert_eq!(list.next_index(), 1);
    }

    #[test]
    fn remove_compaction_under_user_txn_is_atomic() {
        let (_td, env, db) = setup();
        let list: StoredList<'_, String, _> =
            StoredList::new(&db, StringBinding);
        for i in 0..5 {
            list.push(None, &format!("v{i}")).unwrap();
        }

        let txn = env.begin_transaction(None).unwrap();
        let removed = list.remove(Some(&txn), 1).unwrap();
        assert_eq!(removed, Some("v1".to_string()));
        // Inside the txn the list is compacted.
        assert_eq!(list.get(Some(&txn), 0).unwrap(), Some("v0".to_string()));
        assert_eq!(list.get(Some(&txn), 1).unwrap(), Some("v2".to_string()));
        // Abort: every shift rolls back.
        txn.abort().unwrap();
        // Process-local next_index was decremented inside `remove`;
        // the reopen path (`StoredList::open`) is the way to recover
        // the on-disk truth after an abort.
        let recovered =
            StoredList::<String, _>::open(&db, StringBinding).unwrap();
        assert_eq!(recovered.next_index(), 5);
        assert_eq!(recovered.get(None, 1).unwrap(), Some("v1".to_string()));
    }

    #[test]
    fn open_recovers_next_index_after_reopen() {
        let td = TempDir::new().unwrap();
        let path = td.path().to_path_buf();

        // First session: write 3 entries, close.
        {
            let env = Environment::open(
                EnvironmentConfig::new(path.clone()).with_allow_create(true),
            )
            .unwrap();
            let db = env
                .open_database(
                    None,
                    "reopen",
                    &DatabaseConfig::new()
                        .with_allow_create(true)
                        .with_transactional(true),
                )
                .unwrap();
            let list: StoredList<'_, String, _> =
                StoredList::new(&db, StringBinding);
            for i in 0..3 {
                list.push(None, &format!("v{i}")).unwrap();
            }
            let _ = db.close();
        }

        // Second session: open with `open` and confirm recovery.
        {
            let env = Environment::open(
                EnvironmentConfig::new(path).with_allow_create(true),
            )
            .unwrap();
            let db = env
                .open_database(
                    None,
                    "reopen",
                    &DatabaseConfig::new()
                        .with_allow_create(true)
                        .with_transactional(true),
                )
                .unwrap();
            let list: StoredList<'_, String, _> =
                StoredList::open(&db, StringBinding).unwrap();
            assert_eq!(list.next_index(), 3);
            // A push after reopen lands at index 3, not 0.
            assert_eq!(list.push(None, &"v3".to_string()).unwrap(), 3);
            assert_eq!(
                list.get(None, 0).unwrap(),
                Some("v0".to_string()),
                "existing entries must survive reopen",
            );
        }
    }

    #[test]
    fn open_rejects_mixed_use_database() {
        let (_td, _env, db) = setup();
        // Write a non-8-byte key directly.
        let key = DatabaseEntry::from_bytes(b"not-an-index");
        let val = DatabaseEntry::from_bytes(b"v");
        db.put(key.data(), val.data()).unwrap();

        let err = StoredList::<String, _>::open(&db, StringBinding)
            .err()
            .expect("open must fail");
        assert!(matches!(err, CollectionError::IllegalState(_)));
    }

    #[test]
    fn read_only_rejects_writes() {
        let (_td, _env, db) = setup();
        let list =
            StoredList::<String, _>::new(&db, StringBinding).into_read_only();
        assert!(matches!(
            list.push(None, &"x".to_string()),
            Err(CollectionError::ReadOnly)
        ));
        assert!(matches!(list.pop(None), Err(CollectionError::ReadOnly)));
        assert!(matches!(list.remove(None, 0), Err(CollectionError::ReadOnly)));
        assert!(matches!(list.clear(None), Err(CollectionError::ReadOnly)));
    }

    #[test]
    fn iter_yields_values_in_index_order() {
        let (_td, _env, db) = setup();
        let list: StoredList<'_, String, _> =
            StoredList::new(&db, StringBinding);
        for i in 0..5 {
            list.push(None, &format!("v{i}")).unwrap();
        }
        let values: Vec<String> =
            list.iter(None).unwrap().map(Result::unwrap).collect();
        assert_eq!(
            values,
            vec![
                "v0".to_string(),
                "v1".to_string(),
                "v2".to_string(),
                "v3".to_string(),
                "v4".to_string(),
            ],
        );
    }
}
