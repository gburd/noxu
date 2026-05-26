//! Secondary cursor for iterating a secondary (index) database.
//!
//!
//! A SecondaryCursor iterates secondary index entries and transparently
//! fetches the corresponding primary data.  The cursor iterates over
//! (secondary_key, primary_key, primary_data) triples.
//!
//! Write operations (put, putCurrent, putNoDupData, putNoOverwrite) are
//! prohibited on a secondary cursor — use the primary database instead.
//!
//! The `delete` operation deletes the *primary* record (which cascades to
//! all secondary index entries for that primary record).

use crate::cursor::Cursor;
use crate::cursor_config::CursorConfig;
use crate::database_entry::DatabaseEntry;
use crate::error::{NoxuError, Result};
use crate::get::Get;
use crate::operation_status::OperationStatus;
use crate::secondary_database::SecondaryDatabase;
use crate::transaction::Transaction;

/// A cursor that iterates a secondary index database.
///
///
///
/// Each iteration step returns three values:
/// * `key`   — the secondary key (the index key).
/// * `p_key` — the primary key (stored as the secondary record's value).
/// * `data`  — the primary record's data (fetched from the primary database).
///
/// # Example
/// ```ignore
/// let mut cursor = secondary_db.open_cursor(None, None)?;
/// let mut sec_key = DatabaseEntry::new();
/// let mut p_key   = DatabaseEntry::new();
/// let mut data    = DatabaseEntry::new();
///
/// let mut status = cursor.get_first(&mut sec_key, &mut p_key, &mut data)?;
/// while status == OperationStatus::Success {
///     // process sec_key, p_key, data ...
///     status = cursor.get_next(&mut sec_key, &mut p_key, &mut data)?;
/// }
/// cursor.close()?;
/// ```
pub struct SecondaryCursor<'a> {
    /// Cursor over the secondary index storage (sec_key -> pri_key).
    inner: Cursor,
    /// Back-reference to the owning SecondaryDatabase (for primary lookups).
    secondary_db: &'a SecondaryDatabase,
}

impl<'a> SecondaryCursor<'a> {
    /// Creates a new SecondaryCursor.  Called by `SecondaryDatabase::open_cursor`.
    ///
    /// `txn` and `config` are forwarded to the inner `Database::open_cursor`
    /// call so the secondary cursor participates in the caller's transaction
    /// and honours any cursor-level configuration.  See API audit 2026-05
    /// secondary-join finding F4: the previous signature dropped both
    /// arguments on the floor and ran every secondary cursor auto-commit.
    pub(crate) fn new(
        secondary_db: &'a SecondaryDatabase,
        txn: Option<&Transaction>,
        config: Option<&CursorConfig>,
    ) -> Result<Self> {
        let inner = secondary_db.inner_db().open_cursor(txn, config)?;
        Ok(Self { inner, secondary_db })
    }

    // ------------------------------------------------------------------
    // Put operations — all prohibited on a secondary cursor.
    // ------------------------------------------------------------------

    /// Not allowed on a secondary cursor.
    pub fn put(
        &mut self,
        _key: &DatabaseEntry,
        _data: &DatabaseEntry,
    ) -> Result<OperationStatus> {
        Err(NoxuError::OperationNotAllowed(
            "put is not allowed on a secondary cursor".to_string(),
        ))
    }

    // ------------------------------------------------------------------
    // Delete — deletes the primary record.
    // ------------------------------------------------------------------

    /// Deletes the primary record at the current cursor position.
    ///
    ///
    ///
    /// Reads the primary key from the current secondary record, then calls
    /// `Database::delete` on the primary database.  The secondary index
    /// entries for the deleted primary record are cleaned up by
    /// `SecondaryDatabase::delete_all_for_primary`.
    pub fn delete(&mut self) -> Result<OperationStatus> {
        // Read the current secondary record to obtain the primary key.
        let mut sec_key = DatabaseEntry::new();
        let mut p_key_entry = DatabaseEntry::new();
        let status = self.inner.get(
            &mut sec_key,
            &mut p_key_entry,
            Get::Current,
            None,
        )?;
        if status != OperationStatus::Success {
            return Ok(OperationStatus::NotFound);
        }
        // p_key_entry now holds the primary key (stored as the secondary value).
        let pri_key = {
            let bytes = p_key_entry.get_data().unwrap_or(&[]).to_vec();
            DatabaseEntry::from_bytes(&bytes)
        };

        // Fetch primary data for secondary cleanup.
        let mut pri_data = DatabaseEntry::new();
        {
            let primary = self.secondary_db.primary_db().lock();
            let s = primary.get(None, &pri_key, &mut pri_data)?;
            if s == OperationStatus::Success {
                // Remove all secondary index entries for this primary key.
                // Sprint 4½ plumbed `txn` through `delete_all_for_primary`,
                // but `SecondaryCursor` does not currently store its txn
                // handle (the inner `Cursor` already participates in it),
                // so the cascade still runs auto-committed.  Wiring the
                // txn into `SecondaryCursor::delete` is tracked as audit
                // finding F5 follow-up work.
                let old_data = pri_data.clone();
                drop(primary);
                self.secondary_db.delete_all_for_primary(
                    None,
                    &pri_key,
                    Some(&old_data),
                )?;
            }
        }

        // Delete the primary record.
        let primary = self.secondary_db.primary_db().lock();
        let del_status = primary.delete(None, &pri_key)?;

        // Reset the inner cursor state (current position is now deleted).
        let _ = self.inner.get(
            &mut DatabaseEntry::new(),
            &mut DatabaseEntry::new(),
            Get::Current,
            None,
        );

        Ok(del_status)
    }

    // ------------------------------------------------------------------
    // Get operations — each fetches secondary key, primary key, and
    // primary data.
    // ------------------------------------------------------------------

    /// Returns the current key/primary-key/primary-data triple.
    ///
    ///
    pub fn get_current(
        &mut self,
        key: &mut DatabaseEntry,
        p_key: &mut DatabaseEntry,
        data: &mut DatabaseEntry,
    ) -> Result<OperationStatus> {
        self.get_with_mode(key, p_key, data, Get::Current)
    }

    /// Moves to the first record and returns the triple.
    ///
    ///
    pub fn get_first(
        &mut self,
        key: &mut DatabaseEntry,
        p_key: &mut DatabaseEntry,
        data: &mut DatabaseEntry,
    ) -> Result<OperationStatus> {
        self.get_with_mode(key, p_key, data, Get::First)
    }

    /// Moves to the last record and returns the triple.
    ///
    ///
    pub fn get_last(
        &mut self,
        key: &mut DatabaseEntry,
        p_key: &mut DatabaseEntry,
        data: &mut DatabaseEntry,
    ) -> Result<OperationStatus> {
        self.get_with_mode(key, p_key, data, Get::Last)
    }

    /// Moves to the next record and returns the triple.
    ///
    ///
    pub fn get_next(
        &mut self,
        key: &mut DatabaseEntry,
        p_key: &mut DatabaseEntry,
        data: &mut DatabaseEntry,
    ) -> Result<OperationStatus> {
        self.get_with_mode(key, p_key, data, Get::Next)
    }

    /// Moves to the previous record and returns the triple.
    ///
    ///
    pub fn get_prev(
        &mut self,
        key: &mut DatabaseEntry,
        p_key: &mut DatabaseEntry,
        data: &mut DatabaseEntry,
    ) -> Result<OperationStatus> {
        self.get_with_mode(key, p_key, data, Get::Prev)
    }

    /// Searches for the given secondary key (exact match).
    ///
    ///
    ///
    /// # Arguments
    /// * `search_key` - The secondary key to search for (input).
    /// * `p_key` - Output: receives the primary key.
    /// * `data` - Output: receives the primary record data.
    pub fn get_search_key(
        &mut self,
        search_key: &DatabaseEntry,
        p_key: &mut DatabaseEntry,
        data: &mut DatabaseEntry,
    ) -> Result<OperationStatus> {
        // Fetch the primary key stored in the secondary index.
        let mut stored_pk = DatabaseEntry::new();
        // For Search, key is input-only; clone to satisfy &mut parameter.
        let mut search_key_mut = search_key.clone();
        let status = self.inner.get(
            &mut search_key_mut,
            &mut stored_pk,
            Get::Search,
            None,
        )?;

        if status != OperationStatus::Success {
            return Ok(OperationStatus::NotFound);
        }

        // stored_pk = the primary key (value of the secondary record).
        let pri_key_bytes = stored_pk.get_data().unwrap_or(&[]).to_vec();
        p_key.set_data(&pri_key_bytes);

        // Fetch primary data.
        let pri_key_entry = DatabaseEntry::from_bytes(&pri_key_bytes);
        let primary = self.secondary_db.primary_db().lock();
        let pri_status = primary.get(None, &pri_key_entry, data)?;
        if pri_status != OperationStatus::Success {
            return Err(NoxuError::SecondaryIntegrityException(format!(
                "Secondary '{}' refers to missing primary key",
                self.secondary_db.get_database_name()
            )));
        }

        Ok(OperationStatus::Success)
    }

    /// Searches for the first secondary key >= `search_key`.
    ///
    /// `search_key` is updated in place with the actual key found (which
    /// may be strictly greater than the input).  See Wave 1C audit
    /// cleanup (secondary-join “fragile two-step get_search_key_range”
    /// Low) — the v1.5.0 implementation issued a redundant `Get::Current`
    /// probe after the SearchGte to re-read the key, which silently
    /// discarded errors and re-locked the cursor.  The underlying
    /// `Cursor::get(Get::SearchGte)` already writes the discovered key
    /// back into `search_key`, so a single call is sufficient.
    pub fn get_search_key_range(
        &mut self,
        search_key: &mut DatabaseEntry,
        p_key: &mut DatabaseEntry,
        data: &mut DatabaseEntry,
    ) -> Result<OperationStatus> {
        let mut stored_pk = DatabaseEntry::new();
        let status =
            self.inner.get(search_key, &mut stored_pk, Get::SearchGte, None)?;

        if status != OperationStatus::Success {
            return Ok(OperationStatus::NotFound);
        }

        // `Cursor::get` for SearchGte already wrote the discovered key
        // back into `search_key`; copy the primary key out of the inner
        // cursor's data slot and resolve the primary record.
        let pri_key_bytes = stored_pk.get_data().unwrap_or(&[]).to_vec();
        p_key.set_data(&pri_key_bytes);

        let pri_key_entry = DatabaseEntry::from_bytes(&pri_key_bytes);
        let primary = self.secondary_db.primary_db().lock();
        let pri_status = primary.get(None, &pri_key_entry, data)?;
        if pri_status != OperationStatus::Success {
            return Err(NoxuError::SecondaryIntegrityException(format!(
                "Secondary '{}' refers to missing primary key",
                self.secondary_db.get_database_name()
            )));
        }

        Ok(OperationStatus::Success)
    }

    /// Closes the cursor.
    pub fn close(&mut self) -> Result<()> {
        self.inner.close()
    }

    /// Returns whether the cursor is valid (not closed).
    pub fn is_valid(&self) -> bool {
        self.inner.is_valid()
    }

    // ------------------------------------------------------------------
    // Join-cursor helpers (used by JoinCursor internals).
    // ------------------------------------------------------------------

    /// Returns the primary key at the current cursor position *without*
    /// fetching primary data.  Returns `None` if the cursor is not
    /// positioned on a record.
    pub(crate) fn get_current_primary_key_only(
        &mut self,
    ) -> Result<Option<Vec<u8>>> {
        let mut sec_key = DatabaseEntry::new();
        let mut pri_key_entry = DatabaseEntry::new();
        // A "not positioned" or "not found" condition returns Ok(None) so the
        // caller (JoinCursor) can treat it as an empty candidate set rather than
        // propagating a spurious error.
        let status = match self.inner.get(
            &mut sec_key,
            &mut pri_key_entry,
            Get::Current,
            None,
        ) {
            Ok(s) => s,
            Err(_) => return Ok(None),
        };
        if status != OperationStatus::Success {
            return Ok(None);
        }
        Ok(pri_key_entry.get_data().map(|d| d.to_vec()))
    }

    /// Returns the secondary key bytes at the current cursor position.
    /// Returns `None` if the cursor is not positioned on a record.
    pub(crate) fn get_current_sec_key_bytes(
        &mut self,
    ) -> Result<Option<Vec<u8>>> {
        let mut sec_key = DatabaseEntry::new();
        let mut pri_key_entry = DatabaseEntry::new();
        let status = match self.inner.get(
            &mut sec_key,
            &mut pri_key_entry,
            Get::Current,
            None,
        ) {
            Ok(s) => s,
            Err(_) => return Ok(None),
        };
        if status != OperationStatus::Success {
            return Ok(None);
        }
        Ok(sec_key.get_data().map(|d| d.to_vec()))
    }

    /// Returns an estimate of the number of primary keys that share the
    /// current secondary key.  In the current one-to-one secondary model
    /// this is always 0 or 1; with duplicate support it will reflect the
    /// actual duplicate count.
    pub(crate) fn count_estimate(&mut self) -> u64 {
        self.inner.count().unwrap_or_default()
    }

    /// Advances to the next record that has the **same** secondary key as
    /// the current position (i.e. the next "duplicate").
    ///
    /// In the current one-to-one secondary model the cursor stores exactly
    /// one primary key per secondary key, so this always returns
    /// `NotFound`.  When full duplicate support is added this will iterate
    /// the duplicate set.
    pub(crate) fn get_next_dup(&mut self) -> Result<OperationStatus> {
        let Some(current_sk) = self.get_current_sec_key_bytes()? else {
            return Ok(OperationStatus::NotFound);
        };
        let mut sec_key = DatabaseEntry::new();
        let mut pri_key_entry = DatabaseEntry::new();
        let status = self.inner.get(
            &mut sec_key,
            &mut pri_key_entry,
            Get::Next,
            None,
        )?;
        if status != OperationStatus::Success {
            return Ok(OperationStatus::NotFound);
        }
        let new_sk = sec_key.get_data().map(|d| d.to_vec()).unwrap_or_default();
        if new_sk == current_sk {
            Ok(OperationStatus::Success)
        } else {
            // Stepped onto a different secondary key — not a duplicate.
            Ok(OperationStatus::NotFound)
        }
    }

    /// Returns `true` if the primary key at the current cursor position
    /// matches `candidate`.  Used by `JoinCursor` to probe secondary
    /// cursors without touching the primary database.
    pub(crate) fn has_candidate_primary_key(
        &mut self,
        candidate: &[u8],
    ) -> Result<bool> {
        match self.get_current_primary_key_only()? {
            Some(pk) => Ok(pk == candidate),
            None => Ok(false),
        }
    }

    // ------------------------------------------------------------------
    // Private helpers
    // ------------------------------------------------------------------

    /// Core get operation: positions the inner cursor, reads the primary key
    /// from the secondary record value, then fetches primary data.
    ///
    /// # Arguments
    /// * `key` - For Search modes: input key.  For other modes: output key.
    /// * `p_key` - Output: the primary key.
    /// * `data` - Output: the primary data.
    /// * `mode` - The get mode to use on the inner cursor.
    fn get_with_mode(
        &mut self,
        key: &mut DatabaseEntry,
        p_key: &mut DatabaseEntry,
        data: &mut DatabaseEntry,
        mode: Get,
    ) -> Result<OperationStatus> {
        // Step 1: position the inner cursor on the secondary record.
        // The inner cursor returns (sec_key, pri_key) where the "data" side
        // is actually the primary key.
        let mut pri_key_bytes_entry = DatabaseEntry::new();
        let status =
            self.inner.get(key, &mut pri_key_bytes_entry, mode, None)?;

        if status != OperationStatus::Success {
            return Ok(OperationStatus::NotFound);
        }

        // `key` is an output parameter: Cursor::get writes back the current
        // secondary key for all get modes (navigation and search).
        // `key` is always an output DatabaseEntry for all cursor ops.

        // Step 2: the "data" from the inner cursor IS the primary key.
        let pri_key_bytes =
            pri_key_bytes_entry.get_data().unwrap_or(&[]).to_vec();
        p_key.set_data(&pri_key_bytes);

        // Step 3: look up the primary record.
        let pri_key_entry = DatabaseEntry::from_bytes(&pri_key_bytes);
        let primary = self.secondary_db.primary_db().lock();
        let pri_status = primary.get(None, &pri_key_entry, data)?;

        if pri_status != OperationStatus::Success {
            // Secondary refers to a missing primary — integrity issue.
            // In this causes the cursor to skip the record (in
            // READ_UNCOMMITTED mode) or throws an exception.  We return
            // SecondaryIntegrityException to match the non-transactional path.
            return Err(NoxuError::SecondaryIntegrityException(format!(
                "Secondary '{}' refers to missing primary key",
                self.secondary_db.get_database_name()
            )));
        }

        Ok(OperationStatus::Success)
    }
}

impl Drop for SecondaryCursor<'_> {
    fn drop(&mut self) {
        if self.inner.is_valid() {
            let _ = self.inner.close();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::Database;
    use crate::database_config::DatabaseConfig;
    use crate::environment::Environment;
    use crate::environment_config::EnvironmentConfig;
    use crate::secondary_config::{SecondaryConfig, SecondaryKeyCreator};
    use crate::secondary_database::SecondaryDatabase;
    use noxu_sync::Mutex;
    use std::sync::Arc;
    use tempfile::TempDir;

    struct FirstByteKeyCreator;
    impl SecondaryKeyCreator for FirstByteKeyCreator {
        fn create_secondary_key(
            &self,
            _db: &Database,
            _key: &DatabaseEntry,
            data: &DatabaseEntry,
            result: &mut DatabaseEntry,
        ) -> bool {
            if let Some(d) = data.get_data()
                && !d.is_empty()
            {
                result.set_data(&d[..1]);
                return true;
            }
            false
        }
    }

    fn temp_env_primary_secondary()
    -> (TempDir, Environment, Arc<Mutex<Database>>, SecondaryDatabase) {
        let temp_dir = TempDir::new().unwrap();
        let env_config = EnvironmentConfig::new(temp_dir.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true);
        let env = Environment::open(env_config).unwrap();

        let db_config = DatabaseConfig::new().with_allow_create(true);
        let primary_db =
            env.open_database(None, "primary", &db_config).unwrap();
        let primary = Arc::new(Mutex::new(primary_db));

        let sec_db_config = DatabaseConfig::new().with_allow_create(true);
        let sec_db =
            env.open_database(None, "secondary", &sec_db_config).unwrap();
        let sec_config = SecondaryConfig::new()
            .with_allow_create(true)
            .with_key_creator(Box::new(FirstByteKeyCreator));
        let secondary =
            SecondaryDatabase::open(Arc::clone(&primary), sec_db, sec_config)
                .unwrap();

        (temp_dir, env, primary, secondary)
    }

    fn insert_and_index(
        primary: &Arc<Mutex<Database>>,
        secondary: &SecondaryDatabase,
        key: &[u8],
        value: &[u8],
    ) {
        let pk = DatabaseEntry::from_bytes(key);
        let pv = DatabaseEntry::from_bytes(value);
        primary.lock().put(None, &pk, &pv).unwrap();
        secondary.update_secondary(None, &pk, None, Some(&pv)).unwrap();
    }

    #[test]
    fn test_cursor_get_first_last() {
        let (_tmp, _env, primary, secondary) = temp_env_primary_secondary();
        insert_and_index(&primary, &secondary, b"pk2", b"Banana");
        insert_and_index(&primary, &secondary, b"pk1", b"Apple");
        insert_and_index(&primary, &secondary, b"pk3", b"Cherry");

        let mut cursor = secondary.open_cursor(None, None).unwrap();
        let mut sec_key = DatabaseEntry::new();
        let mut p_key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();

        let status =
            cursor.get_first(&mut sec_key, &mut p_key, &mut data).unwrap();
        assert_eq!(status, OperationStatus::Success);
        assert_eq!(data.get_data().unwrap(), b"Apple");

        let status =
            cursor.get_last(&mut sec_key, &mut p_key, &mut data).unwrap();
        assert_eq!(status, OperationStatus::Success);
        assert_eq!(data.get_data().unwrap(), b"Cherry");
    }

    #[test]
    fn test_cursor_get_next() {
        let (_tmp, _env, primary, secondary) = temp_env_primary_secondary();
        insert_and_index(&primary, &secondary, b"pk1", b"Apple");
        insert_and_index(&primary, &secondary, b"pk2", b"Banana");
        insert_and_index(&primary, &secondary, b"pk3", b"Cherry");

        let mut cursor = secondary.open_cursor(None, None).unwrap();
        let mut sec_key = DatabaseEntry::new();
        let mut p_key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();

        let mut results: Vec<Vec<u8>> = Vec::new();
        let mut status =
            cursor.get_first(&mut sec_key, &mut p_key, &mut data).unwrap();
        while status == OperationStatus::Success {
            results.push(data.get_data().unwrap().to_vec());
            status =
                cursor.get_next(&mut sec_key, &mut p_key, &mut data).unwrap();
        }

        assert_eq!(results.len(), 3);
        assert_eq!(results[0], b"Apple");
        assert_eq!(results[1], b"Banana");
        assert_eq!(results[2], b"Cherry");
    }

    #[test]
    fn test_cursor_get_search_key() {
        let (_tmp, _env, primary, secondary) = temp_env_primary_secondary();
        insert_and_index(&primary, &secondary, b"pk1", b"Mango");
        insert_and_index(&primary, &secondary, b"pk2", b"Kiwi");

        let mut cursor = secondary.open_cursor(None, None).unwrap();
        let search = DatabaseEntry::from_bytes(b"M");
        let mut p_key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();

        let status =
            cursor.get_search_key(&search, &mut p_key, &mut data).unwrap();
        assert_eq!(status, OperationStatus::Success);
        assert_eq!(data.get_data().unwrap(), b"Mango");
        assert_eq!(p_key.get_data().unwrap(), b"pk1");
    }

    #[test]
    fn test_cursor_search_key_not_found() {
        let (_tmp, _env, primary, secondary) = temp_env_primary_secondary();
        insert_and_index(&primary, &secondary, b"pk1", b"Apple");

        let mut cursor = secondary.open_cursor(None, None).unwrap();
        let search = DatabaseEntry::from_bytes(b"Z");
        let mut p_key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();

        let status =
            cursor.get_search_key(&search, &mut p_key, &mut data).unwrap();
        assert_eq!(status, OperationStatus::NotFound);
    }

    #[test]
    fn test_cursor_empty_database() {
        let (_tmp, _env, _primary, secondary) = temp_env_primary_secondary();

        let mut cursor = secondary.open_cursor(None, None).unwrap();
        let mut sec_key = DatabaseEntry::new();
        let mut p_key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();

        let status =
            cursor.get_first(&mut sec_key, &mut p_key, &mut data).unwrap();
        assert_eq!(status, OperationStatus::NotFound);
    }

    #[test]
    fn test_cursor_put_not_allowed() {
        let (_tmp, _env, _primary, secondary) = temp_env_primary_secondary();
        let mut cursor = secondary.open_cursor(None, None).unwrap();
        let key = DatabaseEntry::from_bytes(b"key");
        let data = DatabaseEntry::from_bytes(b"data");
        assert!(cursor.put(&key, &data).is_err());
    }

    #[test]
    fn test_cursor_close() {
        let (_tmp, _env, _primary, secondary) = temp_env_primary_secondary();
        let mut cursor = secondary.open_cursor(None, None).unwrap();
        assert!(cursor.is_valid());
        cursor.close().unwrap();
        assert!(!cursor.is_valid());
    }

    #[test]
    fn test_cursor_get_prev() {
        let (_tmp, _env, primary, secondary) = temp_env_primary_secondary();
        insert_and_index(&primary, &secondary, b"pk1", b"Apple");
        insert_and_index(&primary, &secondary, b"pk2", b"Banana");
        insert_and_index(&primary, &secondary, b"pk3", b"Cherry");

        let mut cursor = secondary.open_cursor(None, None).unwrap();
        let mut sec_key = DatabaseEntry::new();
        let mut p_key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();

        // Position at last
        let status =
            cursor.get_last(&mut sec_key, &mut p_key, &mut data).unwrap();
        assert_eq!(status, OperationStatus::Success);
        assert_eq!(data.get_data().unwrap(), b"Cherry");

        // Step back to prev
        let status =
            cursor.get_prev(&mut sec_key, &mut p_key, &mut data).unwrap();
        assert_eq!(status, OperationStatus::Success);
        assert_eq!(data.get_data().unwrap(), b"Banana");

        // Step back again
        let status =
            cursor.get_prev(&mut sec_key, &mut p_key, &mut data).unwrap();
        assert_eq!(status, OperationStatus::Success);
        assert_eq!(data.get_data().unwrap(), b"Apple");

        // No more prev
        let status =
            cursor.get_prev(&mut sec_key, &mut p_key, &mut data).unwrap();
        assert_eq!(status, OperationStatus::NotFound);
    }

    #[test]
    fn test_cursor_get_current() {
        let (_tmp, _env, primary, secondary) = temp_env_primary_secondary();
        insert_and_index(&primary, &secondary, b"pk1", b"Mango");

        let mut cursor = secondary.open_cursor(None, None).unwrap();
        let mut sec_key = DatabaseEntry::new();
        let mut p_key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();

        // First positions the cursor
        let status =
            cursor.get_first(&mut sec_key, &mut p_key, &mut data).unwrap();
        assert_eq!(status, OperationStatus::Success);
        assert_eq!(data.get_data().unwrap(), b"Mango");

        // get_current should return the same record
        let mut sec_key2 = DatabaseEntry::new();
        let mut p_key2 = DatabaseEntry::new();
        let mut data2 = DatabaseEntry::new();
        let status2 =
            cursor.get_current(&mut sec_key2, &mut p_key2, &mut data2).unwrap();
        assert_eq!(status2, OperationStatus::Success);
        assert_eq!(data2.get_data().unwrap(), b"Mango");
        assert_eq!(p_key2.get_data(), p_key.get_data());
    }

    #[test]
    fn test_cursor_get_search_key_range_exact() {
        let (_tmp, _env, primary, secondary) = temp_env_primary_secondary();
        insert_and_index(&primary, &secondary, b"pk1", b"Apple");
        insert_and_index(&primary, &secondary, b"pk2", b"Banana");
        insert_and_index(&primary, &secondary, b"pk3", b"Cherry");

        let mut cursor = secondary.open_cursor(None, None).unwrap();
        // Search key "B" — exact match for first byte of "Banana"
        let mut search_key = DatabaseEntry::from_bytes(b"B");
        let mut p_key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();

        let status = cursor
            .get_search_key_range(&mut search_key, &mut p_key, &mut data)
            .unwrap();
        assert_eq!(status, OperationStatus::Success);
        assert_eq!(data.get_data().unwrap(), b"Banana");
    }

    #[test]
    fn test_cursor_get_search_key_range_gte() {
        let (_tmp, _env, primary, secondary) = temp_env_primary_secondary();
        // Insert keys with first bytes: A, C (no B)
        insert_and_index(&primary, &secondary, b"pk1", b"Apple");
        insert_and_index(&primary, &secondary, b"pk3", b"Cherry");

        let mut cursor = secondary.open_cursor(None, None).unwrap();
        // Search for "B" — no exact match, but "C" (Cherry) is >= "B"
        let mut search_key = DatabaseEntry::from_bytes(b"B");
        let mut p_key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();

        let status = cursor
            .get_search_key_range(&mut search_key, &mut p_key, &mut data)
            .unwrap();
        assert_eq!(status, OperationStatus::Success);
        assert_eq!(data.get_data().unwrap(), b"Cherry");
    }

    #[test]
    fn test_cursor_get_search_key_range_not_found() {
        let (_tmp, _env, primary, secondary) = temp_env_primary_secondary();
        insert_and_index(&primary, &secondary, b"pk1", b"Apple");

        let mut cursor = secondary.open_cursor(None, None).unwrap();
        // "Z" is beyond everything
        let mut search_key = DatabaseEntry::from_bytes(b"Z");
        let mut p_key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();

        let status = cursor
            .get_search_key_range(&mut search_key, &mut p_key, &mut data)
            .unwrap();
        assert_eq!(status, OperationStatus::NotFound);
    }

    #[test]
    fn test_cursor_full_navigation_sequence() {
        let (_tmp, _env, primary, secondary) = temp_env_primary_secondary();
        // Insert 4 records with distinct first bytes
        insert_and_index(&primary, &secondary, b"pk1", b"Apple");
        insert_and_index(&primary, &secondary, b"pk2", b"Banana");
        insert_and_index(&primary, &secondary, b"pk3", b"Cherry");
        insert_and_index(&primary, &secondary, b"pk4", b"Durian");

        let mut cursor = secondary.open_cursor(None, None).unwrap();
        let mut sec_key = DatabaseEntry::new();
        let mut p_key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();

        // Forward traversal via Next
        let s = cursor.get_first(&mut sec_key, &mut p_key, &mut data).unwrap();
        assert_eq!(s, OperationStatus::Success);
        let first_data = data.get_data().unwrap().to_vec();

        let s = cursor.get_next(&mut sec_key, &mut p_key, &mut data).unwrap();
        assert_eq!(s, OperationStatus::Success);
        let second_data = data.get_data().unwrap().to_vec();

        // The two records should differ
        assert_ne!(first_data, second_data);

        // Jump to last
        let s = cursor.get_last(&mut sec_key, &mut p_key, &mut data).unwrap();
        assert_eq!(s, OperationStatus::Success);
        assert_eq!(data.get_data().unwrap(), b"Durian");

        // Prev from last
        let s = cursor.get_prev(&mut sec_key, &mut p_key, &mut data).unwrap();
        assert_eq!(s, OperationStatus::Success);
        assert_eq!(data.get_data().unwrap(), b"Cherry");
    }

    #[test]
    fn test_cursor_get_search_key_returns_pkey() {
        let (_tmp, _env, primary, secondary) = temp_env_primary_secondary();
        insert_and_index(&primary, &secondary, b"mypk", b"Kiwi");

        let mut cursor = secondary.open_cursor(None, None).unwrap();
        let search = DatabaseEntry::from_bytes(b"K"); // first byte of "Kiwi"
        let mut p_key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();

        let status =
            cursor.get_search_key(&search, &mut p_key, &mut data).unwrap();
        assert_eq!(status, OperationStatus::Success);
        assert_eq!(p_key.get_data().unwrap(), b"mypk");
        assert_eq!(data.get_data().unwrap(), b"Kiwi");
    }

    #[test]
    fn test_cursor_drop_closes_automatically() {
        // Verify Drop impl doesn't panic even when cursor is still valid
        let (_tmp, _env, _primary, secondary) = temp_env_primary_secondary();
        let cursor = secondary.open_cursor(None, None).unwrap();
        // Drop without explicit close — should not panic
        drop(cursor);
    }

    #[test]
    fn test_cursor_next_at_end_returns_not_found() {
        let (_tmp, _env, primary, secondary) = temp_env_primary_secondary();
        insert_and_index(&primary, &secondary, b"pk1", b"Apple");

        let mut cursor = secondary.open_cursor(None, None).unwrap();
        let mut sec_key = DatabaseEntry::new();
        let mut p_key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();

        // Move to first (the only record)
        cursor.get_first(&mut sec_key, &mut p_key, &mut data).unwrap();
        // Next should be NotFound
        let status =
            cursor.get_next(&mut sec_key, &mut p_key, &mut data).unwrap();
        assert_eq!(status, OperationStatus::NotFound);
    }
}
