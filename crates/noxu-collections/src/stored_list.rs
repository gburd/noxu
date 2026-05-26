//! Typed list view of a database.
//!
//! Wave 2B redesign (v1.6).  `StoredList<V, VB>` is a sequence-indexed
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
//! Cost: `O(N - idx)` database operations per remove.  All shifts are
//! issued under the supplied `txn`; if `txn` is `None`, each shift is
//! its own auto-txn and a crash mid-shift can leave the list with
//! partial compaction.  Pass `Some(&txn)` (e.g. via
//! [`crate::TransactionRunner`]) to make the whole compaction atomic.

use std::marker::PhantomData;
use std::sync::Mutex;

use noxu_bind::EntryBinding;
use noxu_db::{Database, DatabaseEntry, Get, OperationStatus, Transaction};

use crate::error::{CollectionError, Result};
use crate::internal::{decode_value, encode_value};
use crate::stored_iterator::StoredIterator;

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
/// `next_index` is process-local state guarded by a `Mutex`.  A list
/// opened twice in the same process behaves correctly; multiple
/// processes pushing to the same list will race on the counter and
/// must coordinate externally.  This is unchanged from v1.5.
pub struct StoredList<'db, V, VB>
where
    VB: EntryBinding<V>,
{
    db: &'db Database,
    value_binding: VB,
    next_index: Mutex<usize>,
    read_only: bool,
    _marker: PhantomData<fn() -> V>,
}

impl<'db, V, VB> StoredList<'db, V, VB>
where
    VB: EntryBinding<V>,
{
    /// Creates a new list view of the given database.
    ///
    /// **Does not recover the next-index counter.**  This constructor
    /// is the fast path for brand-new (or known-empty) databases:
    /// `next_index` starts at 0.  When reopening a database that may
    /// already contain records, use [`StoredList::open`] instead.
    pub fn new(db: &'db Database, value_binding: VB) -> Self {
        StoredList {
            db,
            value_binding,
            next_index: Mutex::new(0),
            read_only: false,
            _marker: PhantomData,
        }
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
        let mut cursor = db.open_cursor(txn, None)?;
        let mut key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();
        let next = match cursor.get(&mut key, &mut data, Get::Last, None)? {
            OperationStatus::Success => {
                let bytes = key.get_data().unwrap_or(&[]);
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
        Ok(StoredList {
            db,
            value_binding,
            next_index: Mutex::new(next),
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
    pub fn next_index(&self) -> usize {
        *self.next_index.lock().unwrap()
    }

    /// Appends `value` to the end of the list and returns the
    /// assigned index.
    pub fn push(&self, txn: Option<&Transaction>, value: &V) -> Result<usize> {
        if self.read_only {
            return Err(CollectionError::ReadOnly);
        }
        let mut next = self.next_index.lock().unwrap();
        let index = *next;
        let key_bytes = Self::index_to_key(index);
        let key_entry = DatabaseEntry::from_bytes(&key_bytes);
        let value_entry = encode_value(&self.value_binding, value)?;
        self.db.put(txn, &key_entry, &value_entry)?;
        *next = index + 1;
        Ok(index)
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
        let mut data_entry = DatabaseEntry::new();
        match self.db.get(txn, &key_entry, &mut data_entry)? {
            OperationStatus::Success => {
                Ok(Some(decode_value(&self.value_binding, &data_entry)?))
            }
            _ => Ok(None),
        }
    }

    /// Removes and returns the last element.  Returns `None` if the
    /// list is empty.
    pub fn pop(&self, txn: Option<&Transaction>) -> Result<Option<V>> {
        if self.read_only {
            return Err(CollectionError::ReadOnly);
        }
        let mut next = self.next_index.lock().unwrap();
        if *next == 0 {
            return Ok(None);
        }
        let index = *next - 1;
        let key_bytes = Self::index_to_key(index);
        let key_entry = DatabaseEntry::from_bytes(&key_bytes);

        // Read the old value so we can return it.
        let mut data_entry = DatabaseEntry::new();
        let val = match self.db.get(txn, &key_entry, &mut data_entry)? {
            OperationStatus::Success => {
                Some(decode_value(&self.value_binding, &data_entry)?)
            }
            _ => None,
        };
        if val.is_some() {
            self.db.delete(txn, &key_entry)?;
            *next = index;
        }
        Ok(val)
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
    /// The whole compaction is issued under `txn`.  When `txn` is
    /// `Some(&t)`, the entire operation is atomic on the user's
    /// commit/abort.  When `txn` is `None`, each individual shift
    /// is its own auto-txn — a crash mid-compaction can leave the
    /// list with duplicate entries and an inconsistent
    /// `next_index`.  Pass a real txn for crash-atomic semantics.
    pub fn remove(
        &self,
        txn: Option<&Transaction>,
        index: usize,
    ) -> Result<Option<V>> {
        if self.read_only {
            return Err(CollectionError::ReadOnly);
        }
        let mut next = self.next_index.lock().unwrap();
        if index >= *next {
            return Ok(None);
        }

        // Snapshot the removed value so we can return it.
        let target_key_bytes = Self::index_to_key(index);
        let target_key = DatabaseEntry::from_bytes(&target_key_bytes);
        let mut target_data = DatabaseEntry::new();
        let removed = match self.db.get(txn, &target_key, &mut target_data)? {
            OperationStatus::Success => {
                Some(decode_value(&self.value_binding, &target_data)?)
            }
            _ => None,
        };

        // Shift every subsequent record down by one slot.  We read
        // the source slot, write its bytes at the destination slot,
        // and delete the source.
        let high = *next; // exclusive upper bound
        for src in (index + 1)..high {
            let dst = src - 1;
            let src_key_bytes = Self::index_to_key(src);
            let dst_key_bytes = Self::index_to_key(dst);
            let src_key = DatabaseEntry::from_bytes(&src_key_bytes);
            let dst_key = DatabaseEntry::from_bytes(&dst_key_bytes);

            let mut src_data = DatabaseEntry::new();
            match self.db.get(txn, &src_key, &mut src_data)? {
                OperationStatus::Success => {
                    let payload = src_data.get_data().unwrap_or(&[]).to_vec();
                    let dst_value = DatabaseEntry::from_vec(payload);
                    self.db.put(txn, &dst_key, &dst_value)?;
                    self.db.delete(txn, &src_key)?;
                }
                _ => {
                    // Source slot is empty — remove the destination
                    // slot too so the list stays dense.  This handles
                    // the case where a concurrent writer or a
                    // previous crashed remove left a hole.
                    self.db.delete(txn, &dst_key)?;
                }
            }
        }

        // Either way, the slot at `high - 1` no longer exists.  If
        // we never entered the loop (index == high - 1), delete the
        // target slot itself.
        if index == high.saturating_sub(1) && removed.is_some() {
            self.db.delete(txn, &target_key)?;
        }

        // Decrement next_index by 1 if anything was actually removed.
        if removed.is_some() {
            *next = high - 1;
        }
        Ok(removed)
    }

    /// Returns the number of elements.
    ///
    /// Implemented in terms of `next_index()` rather than
    /// `Database::count()` because the list contract tracks the
    /// high-water mark, not the live record count.  After
    /// compaction these are equal; before crashed-mid-shift they
    /// would diverge.
    pub fn len(&self, _txn: Option<&Transaction>) -> Result<usize> {
        Ok(*self.next_index.lock().unwrap())
    }

    /// Returns whether the list is empty.
    pub fn is_empty(&self, txn: Option<&Transaction>) -> Result<bool> {
        Ok(self.len(txn)? == 0)
    }

    /// Returns a snapshot iterator over every value, in index order.
    pub fn iter(
        &self,
        txn: Option<&Transaction>,
    ) -> Result<StoredIterator<V>> {
        use crate::internal::{scan_records, ScanDirection, StartKey};
        use noxu_bind::ByteArrayBinding;

        let key_binding = ByteArrayBinding;
        let items =
            scan_records::<Vec<u8>, V, ByteArrayBinding, VB, V, _>(
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

    /// Removes every element.
    pub fn clear(&self, txn: Option<&Transaction>) -> Result<()> {
        if self.read_only {
            return Err(CollectionError::ReadOnly);
        }
        let mut cursor = self.db.open_cursor(txn, None)?;
        let mut key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();
        while let OperationStatus::Success =
            cursor.get(&mut key, &mut data, Get::First, None)?
        {
            cursor.delete()?;
        }
        cursor.close()?;
        *self.next_index.lock().unwrap() = 0;
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
                &DatabaseConfig::new().with_allow_create(true),
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

    /// Wave 2B compaction test: insert 10 items, remove every other
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
        let collected: Vec<Option<String>> = (0..5)
            .map(|i| list.get(None, i).unwrap())
            .collect();
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

        let txn = env.begin_transaction(None, None).unwrap();
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
        let recovered = StoredList::<String, _>::open(&db, StringBinding)
            .unwrap();
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
                EnvironmentConfig::new(path.clone())
                    .with_allow_create(true),
            )
            .unwrap();
            let db = env
                .open_database(
                    None,
                    "reopen",
                    &DatabaseConfig::new().with_allow_create(true),
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
                EnvironmentConfig::new(path)
                    .with_allow_create(true),
            )
            .unwrap();
            let db = env
                .open_database(
                    None,
                    "reopen",
                    &DatabaseConfig::new().with_allow_create(true),
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
        db.put(None, &key, &val).unwrap();

        let err = StoredList::<String, _>::open(&db, StringBinding)
            .err()
            .expect("open must fail");
        assert!(matches!(err, CollectionError::IllegalState(_)));
    }

    #[test]
    fn read_only_rejects_writes() {
        let (_td, _env, db) = setup();
        let list = StoredList::<String, _>::new(&db, StringBinding)
            .into_read_only();
        assert!(matches!(
            list.push(None, &"x".to_string()),
            Err(CollectionError::ReadOnly)
        ));
        assert!(matches!(list.pop(None), Err(CollectionError::ReadOnly)));
        assert!(matches!(
            list.remove(None, 0),
            Err(CollectionError::ReadOnly)
        ));
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
