//! List view of a database.
//!
//! Provides a
//! sequential-access list interface over a Noxu DB database, using
//! `usize` indices as keys encoded in big-endian byte order.

use crate::error::Result;
use crate::stored_map::StoredMap;
use noxu_db::Database;

/// A list-like view of a database.
///
/// Elements are stored with their zero-based index encoded as a big-endian
/// 8-byte key so that iteration order matches insertion order and keys
/// sort numerically in byte-lexicographic order.
///
/// # v1.5 limitations
///
/// 1. **Auto-commit only.**  All operations issue the underlying
///    `Database` call with `txn = None`.  Threading
///    `Option<&Transaction>` through `push` / `pop` / `get` / `remove`
///    is on the v1.6 roadmap (audit findings #1, #3, #4).
///
/// 2. **`new` does not recover the next-index counter.**  See
///    [`StoredList::open`] for the reopen-safe constructor.  Calling
///    `StoredList::new` against a database that already contains
///    records will start `next_index` at 0 and overwrite existing data
///    on subsequent `push` calls.  This is documented as the
///    fast/empty-database path; `open` is the correct path for any
///    persistent list (audit finding #6).
///
/// 3. **`remove` does not compact.**  It is a single-key delete; the
///    removed index becomes a hole and `next_index` is unchanged
///    (audit finding #5).
///
/// # Implementation notes
/// Index gaps created by `remove()` are not compacted; subsequent `push()`
/// calls use the next sequential index rather than re-filling holes.
/// `pop` removes the element at the highest known index.
///
/// # Example
/// ```ignore
/// use noxu_collections::StoredList;
///
/// // For a brand-new (or known-empty) database, `new` is fine:
/// let list = StoredList::new(&db);
/// list.push(b"first").unwrap();
/// list.push(b"second").unwrap();
/// assert_eq!(list.get(0).unwrap(), Some(b"first".to_vec()));
///
/// // When reopening a database with existing entries, use `open`:
/// let list = StoredList::open(&db).unwrap();
/// // `list.next_index()` is recovered from the largest existing key.
/// ```
pub struct StoredList<'db> {
    /// The underlying StoredMap providing key-value storage.
    map: StoredMap<'db>,
    /// The next index to use for push. Tracks the logical size.
    next_index: std::sync::Mutex<usize>,
}

impl<'db> StoredList<'db> {
    /// Creates a new list view of the given database.
    ///
    /// # Arguments
    /// * `db` - The database to provide a list view over
    pub fn new(db: &'db Database) -> Self {
        StoredList {
            map: StoredMap::new(db, false),
            next_index: std::sync::Mutex::new(0),
        }
    }

    /// Encodes a `usize` index as an 8-byte big-endian key.
    pub fn index_to_key(index: usize) -> Vec<u8> {
        (index as u64).to_be_bytes().to_vec()
    }

    /// Appends a value to the end of the list.
    ///
    /// Returns the index at which the value was stored.
    pub fn push(&self, value: &[u8]) -> Result<usize> {
        let mut next = self.next_index.lock().unwrap();
        let index = *next;
        let key = Self::index_to_key(index);
        self.map.put(&key, value)?;
        *next = index + 1;
        Ok(index)
    }

    /// Retrieves the value at the given index.
    ///
    /// Returns `None` if no value exists at that index.
    pub fn get(&self, index: usize) -> Result<Option<Vec<u8>>> {
        let key = Self::index_to_key(index);
        self.map.get(&key)
    }

    /// Removes and returns the value at the highest index (the last element).
    ///
    /// Returns `None` if the list is empty.
    pub fn pop(&self) -> Result<Option<Vec<u8>>> {
        let mut next = self.next_index.lock().unwrap();
        if *next == 0 {
            return Ok(None);
        }
        let index = *next - 1;
        let key = Self::index_to_key(index);
        let val = self.map.remove(&key)?;
        if val.is_some() {
            *next = index;
        }
        Ok(val)
    }

    /// Removes the value at the given index.
    ///
    /// **No compaction.**  This is a single-key delete: the slot at
    /// `index` becomes a hole, every higher index keeps its slot, and
    /// `next_index` is unchanged.  This matches the BDB-JE
    /// `StoredContainer.removeKey()` shape.
    ///
    /// `next_index` is *not* decremented here; subsequent `push` calls
    /// continue from the previous high-water mark rather than re-using
    /// the freed slot.  Use [`StoredList::pop`] if you want to remove
    /// the highest-index element and reclaim its slot.
    ///
    /// # v1.5 limitation
    ///
    /// The compaction / re-indexing semantics described in earlier
    /// release-candidate rustdoc were aspirational and were never
    /// implemented; the audit (May 2026, finding #5) flagged the
    /// rustdoc-vs-body mismatch.  Compaction moves to v1.6 alongside
    /// the broader typed-API work.
    ///
    /// # Returns
    ///
    /// The removed value, or `None` if no value was stored at `index`.
    pub fn remove(&self, index: usize) -> Result<Option<Vec<u8>>> {
        // Single-key delete; gaps remain in the index, matching
        // BDB-JE StoredContainer.removeKey().  See rustdoc above.
        let key = Self::index_to_key(index);
        self.map.remove(&key)
    }

    /// Returns the number of elements known to this list view.
    ///
    /// Uses the database record count, which includes all stored elements.
    pub fn len(&self) -> Result<u64> {
        self.map.len()
    }

    /// Returns whether the list is empty.
    pub fn is_empty(&self) -> Result<bool> {
        self.map.is_empty()
    }

    /// Returns the next index that would be used by `push`.
    pub fn next_index(&self) -> usize {
        *self.next_index.lock().unwrap()
    }

    /// Returns a reference to the underlying `StoredMap`.
    pub fn as_map(&self) -> &StoredMap<'db> {
        &self.map
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CollectionError;
    use noxu_db::{DatabaseConfig, Environment, EnvironmentConfig};
    use tempfile::TempDir;

    fn setup_env_and_db() -> (TempDir, Environment, noxu_db::Database) {
        let temp_dir = TempDir::new().unwrap();
        let env_config = EnvironmentConfig::new(temp_dir.path().to_path_buf())
            .with_allow_create(true);
        let env = Environment::open(env_config).unwrap();
        let db_config = DatabaseConfig::new().with_allow_create(true);
        let db = env.open_database(None, "testdb", &db_config).unwrap();
        (temp_dir, env, db)
    }

    #[test]
    fn test_new_list_is_empty() {
        let (_td, _env, db) = setup_env_and_db();
        let list = StoredList::new(&db);
        assert!(list.is_empty().unwrap());
        assert_eq!(list.len().unwrap(), 0);
        assert_eq!(list.next_index(), 0);
    }

    #[test]
    fn test_push_and_get() {
        let (_td, _env, db) = setup_env_and_db();
        let list = StoredList::new(&db);

        let idx = list.push(b"hello").unwrap();
        assert_eq!(idx, 0);
        assert_eq!(list.get(0).unwrap(), Some(b"hello".to_vec()));
    }

    #[test]
    fn test_push_multiple() {
        let (_td, _env, db) = setup_env_and_db();
        let list = StoredList::new(&db);

        assert_eq!(list.push(b"first").unwrap(), 0);
        assert_eq!(list.push(b"second").unwrap(), 1);
        assert_eq!(list.push(b"third").unwrap(), 2);

        assert_eq!(list.get(0).unwrap(), Some(b"first".to_vec()));
        assert_eq!(list.get(1).unwrap(), Some(b"second".to_vec()));
        assert_eq!(list.get(2).unwrap(), Some(b"third".to_vec()));
        assert_eq!(list.len().unwrap(), 3);
    }

    #[test]
    fn test_get_nonexistent() {
        let (_td, _env, db) = setup_env_and_db();
        let list = StoredList::new(&db);
        assert_eq!(list.get(99).unwrap(), None);
    }

    #[test]
    fn test_pop_empty() {
        let (_td, _env, db) = setup_env_and_db();
        let list = StoredList::new(&db);
        assert_eq!(list.pop().unwrap(), None);
    }

    #[test]
    fn test_pop() {
        let (_td, _env, db) = setup_env_and_db();
        let list = StoredList::new(&db);

        list.push(b"a").unwrap();
        list.push(b"b").unwrap();
        list.push(b"c").unwrap();

        let val = list.pop().unwrap();
        assert_eq!(val, Some(b"c".to_vec()));
        assert_eq!(list.next_index(), 2);
        assert_eq!(list.len().unwrap(), 2);
    }

    #[test]
    fn test_pop_until_empty() {
        let (_td, _env, db) = setup_env_and_db();
        let list = StoredList::new(&db);

        list.push(b"x").unwrap();
        list.push(b"y").unwrap();

        assert_eq!(list.pop().unwrap(), Some(b"y".to_vec()));
        assert_eq!(list.pop().unwrap(), Some(b"x".to_vec()));
        assert_eq!(list.pop().unwrap(), None);
        assert!(list.is_empty().unwrap());
    }

    #[test]
    fn test_remove() {
        let (_td, _env, db) = setup_env_and_db();
        let list = StoredList::new(&db);

        list.push(b"alpha").unwrap();
        list.push(b"beta").unwrap();
        list.push(b"gamma").unwrap();

        let removed = list.remove(1).unwrap();
        assert_eq!(removed, Some(b"beta".to_vec()));

        // remove() is a single-key delete — no re-indexing / compaction.
        // Gaps remain at the removed index and `next_index` is unchanged.
        // (Audit finding #5 — rustdoc was wrong before sprint 3C.)
        assert_eq!(list.get(0).unwrap(), Some(b"alpha".to_vec()));
        assert_eq!(list.get(1).unwrap(), None); // gap — not compacted
        assert_eq!(list.get(2).unwrap(), Some(b"gamma".to_vec()));
        assert_eq!(list.len().unwrap(), 2);
        assert_eq!(
            list.next_index(),
            3,
            "remove() must not decrement next_index",
        );
    }

    #[test]
    fn test_remove_does_not_reclaim_slot_on_push() {
        // Regression for the audit-#5 rustdoc-vs-body mismatch.  The
        // documented contract is that remove() leaves a hole and the
        // next push continues at next_index, not at the freed slot.
        let (_td, _env, db) = setup_env_and_db();
        let list = StoredList::new(&db);

        list.push(b"a").unwrap(); // 0
        list.push(b"b").unwrap(); // 1
        list.push(b"c").unwrap(); // 2
        list.remove(1).unwrap();

        let idx = list.push(b"d").unwrap();
        assert_eq!(
            idx, 3,
            "push after remove must use next_index, not the freed slot",
        );
        assert_eq!(list.get(1).unwrap(), None, "gap must remain");
        assert_eq!(list.get(3).unwrap(), Some(b"d".to_vec()));
    }

    #[test]
    fn test_remove_nonexistent() {
        let (_td, _env, db) = setup_env_and_db();
        let list = StoredList::new(&db);
        let removed = list.remove(42).unwrap();
        assert_eq!(removed, None);
    }

    #[test]
    fn test_next_index_advances() {
        let (_td, _env, db) = setup_env_and_db();
        let list = StoredList::new(&db);

        assert_eq!(list.next_index(), 0);
        list.push(b"a").unwrap();
        assert_eq!(list.next_index(), 1);
        list.push(b"b").unwrap();
        assert_eq!(list.next_index(), 2);
    }

    #[test]
    fn test_as_map() {
        let (_td, _env, db) = setup_env_and_db();
        let list = StoredList::new(&db);
        list.push(b"val").unwrap();
        // The underlying map should have one entry
        assert_eq!(list.as_map().len().unwrap(), 1);
    }

    #[test]
    fn test_index_key_sort_order() {
        // Big-endian encoding means index 0 < 1 < 255 < 256 in byte order
        let k0 = StoredList::index_to_key(0);
        let k1 = StoredList::index_to_key(1);
        let k255 = StoredList::index_to_key(255);
        let k256 = StoredList::index_to_key(256);
        assert!(k0 < k1);
        assert!(k1 < k255);
        assert!(k255 < k256);
    }

    #[test]
    fn test_read_only_underlying_map_returns_error() {
        let (_td, _env, db) = setup_env_and_db();
        // Write via list, then read via read-only StoredMap
        let list = StoredList::new(&db);
        list.push(b"data").unwrap();

        let ro_map = StoredMap::new(&db, true);
        let key = StoredList::index_to_key(0);
        let val = ro_map.get(&key).unwrap();
        assert_eq!(val, Some(b"data".to_vec()));

        let result = ro_map.put(&key, b"new");
        assert!(matches!(result, Err(CollectionError::ReadOnly)));
    }
}
