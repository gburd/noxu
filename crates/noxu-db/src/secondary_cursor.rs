//! Secondary cursor for iterating a secondary (index) database.
//!
//! A `SecondaryCursor` iterates secondary index entries and transparently
//! fetches the corresponding primary data.  Each step yields a triple:
//! `(secondary_key, primary_key, primary_data)`.
//!
//! Write operations (put, putCurrent, putNoDupData, putNoOverwrite) are
//! prohibited on a secondary cursor — use the primary database instead.
//!
//! # Sorted-dup duplicate enumeration
//!
//! As of v1.6 secondaries are sorted-dup: a single `secondary_key` may
//! map to many primaries.  `get_search_key` positions on the **first**
//! duplicate of `search_key`; subsequent calls to [`get_next_dup`] /
//! [`get_prev_dup`] enumerate the rest.  [`Get::Next`] iterates the
//! whole index in `(sec_key, pri_key)` lex order.
//!
//! # Cascade delete
//!
//! `delete` removes the *primary* record at the cursor position.  The
//! primary's automatic-maintenance hook then removes every secondary
//! index entry that pointed at it.  Both deletes participate in the
//! cursor's transaction (Wave 1B / audit F5).

use crate::cursor::Cursor;
use crate::cursor_config::CursorConfig;
use crate::database_entry::DatabaseEntry;
use crate::error::{NoxuError, Result};
use crate::get::Get;
use crate::operation_status::OperationStatus;
use crate::secondary_database::SecondaryDatabase;
use crate::transaction::Transaction;

/// A cursor that iterates a secondary index database.
pub struct SecondaryCursor<'a> {
    /// Cursor over the secondary index storage (sec_key -> pri_key…).
    inner: Cursor,
    /// Back-reference to the owning SecondaryDatabase (for primary lookups).
    secondary_db: &'a SecondaryDatabase,
    /// Transaction handle the cursor was opened under.
    txn: Option<&'a Transaction>,
}

impl<'a> SecondaryCursor<'a> {
    pub(crate) fn new(
        secondary_db: &'a SecondaryDatabase,
        txn: Option<&'a Transaction>,
        config: Option<&CursorConfig>,
    ) -> Result<Self> {
        let inner = secondary_db.inner_db().open_cursor(txn, config)?;
        Ok(Self { inner, secondary_db, txn })
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
    // Delete — deletes the primary record + cascades secondary cleanup.
    // ------------------------------------------------------------------

    pub fn delete(&mut self) -> Result<OperationStatus> {
        // Read the current secondary record to obtain the primary key.
        let mut sec_key = DatabaseEntry::new();
        let mut p_key_entry = DatabaseEntry::new();
        let status =
            self.inner.get(&mut sec_key, &mut p_key_entry, Get::Current, None)?;
        if status != OperationStatus::Success {
            return Ok(OperationStatus::NotFound);
        }
        let pri_key = {
            let bytes = p_key_entry.get_data().unwrap_or(&[]).to_vec();
            DatabaseEntry::from_bytes(&bytes)
        };

        // Drive the primary delete; the primary's auto-maintenance
        // hook will clean up every secondary entry that pointed to
        // pri_key (including the slot the cursor is positioned on).
        let del_status = {
            let primary = self.secondary_db.primary_db().lock();
            primary.delete(self.txn, &pri_key)?
        };

        Ok(del_status)
    }

    // ------------------------------------------------------------------
    // Get operations — each fetches secondary key, primary key, and
    // primary data.
    // ------------------------------------------------------------------

    pub fn get_current(
        &mut self,
        key: &mut DatabaseEntry,
        p_key: &mut DatabaseEntry,
        data: &mut DatabaseEntry,
    ) -> Result<OperationStatus> {
        self.get_with_mode(key, p_key, data, Get::Current)
    }

    pub fn get_first(
        &mut self,
        key: &mut DatabaseEntry,
        p_key: &mut DatabaseEntry,
        data: &mut DatabaseEntry,
    ) -> Result<OperationStatus> {
        self.get_with_mode(key, p_key, data, Get::First)
    }

    pub fn get_last(
        &mut self,
        key: &mut DatabaseEntry,
        p_key: &mut DatabaseEntry,
        data: &mut DatabaseEntry,
    ) -> Result<OperationStatus> {
        self.get_with_mode(key, p_key, data, Get::Last)
    }

    pub fn get_next(
        &mut self,
        key: &mut DatabaseEntry,
        p_key: &mut DatabaseEntry,
        data: &mut DatabaseEntry,
    ) -> Result<OperationStatus> {
        self.get_with_mode(key, p_key, data, Get::Next)
    }

    pub fn get_prev(
        &mut self,
        key: &mut DatabaseEntry,
        p_key: &mut DatabaseEntry,
        data: &mut DatabaseEntry,
    ) -> Result<OperationStatus> {
        self.get_with_mode(key, p_key, data, Get::Prev)
    }

    /// Advances to the next duplicate of the current secondary key.
    ///
    /// Returns `OperationStatus::NotFound` once the duplicate set is
    /// exhausted (i.e. the cursor would step onto a different
    /// secondary key).
    pub fn get_next_dup(
        &mut self,
        key: &mut DatabaseEntry,
        p_key: &mut DatabaseEntry,
        data: &mut DatabaseEntry,
    ) -> Result<OperationStatus> {
        self.get_with_mode(key, p_key, data, Get::NextDup)
    }

    /// Searches for the given secondary key (exact match, first dup).
    ///
    /// In a sorted-dup secondary the cursor is positioned on the
    /// **first** primary key bound to `search_key`.  Use
    /// [`Self::get_next_dup`] to walk the rest.
    pub fn get_search_key(
        &mut self,
        search_key: &DatabaseEntry,
        p_key: &mut DatabaseEntry,
        data: &mut DatabaseEntry,
    ) -> Result<OperationStatus> {
        let mut search_key_mut = search_key.clone();
        self.get_with_mode(&mut search_key_mut, p_key, data, Get::Search)
    }

    /// Searches for the exact `(sec_key, pri_key)` pair.
    ///
    /// Returns `OperationStatus::Success` if the pair is present and
    /// positions the cursor on it; otherwise `OperationStatus::NotFound`.
    pub fn get_search_both(
        &mut self,
        search_key: &DatabaseEntry,
        search_pri_key: &DatabaseEntry,
        data: &mut DatabaseEntry,
    ) -> Result<OperationStatus> {
        let mut sk = search_key.clone();
        let mut spk = search_pri_key.clone();
        let st = self.inner.get(&mut sk, &mut spk, Get::SearchBoth, None)?;
        if st != OperationStatus::Success {
            return Ok(OperationStatus::NotFound);
        }
        // Resolve the primary record.
        let pri_key_bytes = spk.get_data().unwrap_or(&[]).to_vec();
        let pri_key_entry = DatabaseEntry::from_bytes(&pri_key_bytes);
        let primary = self.secondary_db.primary_db().lock();
        let pri_status = primary.get(self.txn, &pri_key_entry, data)?;
        if pri_status != OperationStatus::Success {
            return Err(NoxuError::SecondaryIntegrityException(format!(
                "Secondary '{}' refers to missing primary key",
                self.secondary_db.get_database_name()
            )));
        }
        Ok(OperationStatus::Success)
    }

    /// Searches for the first secondary key >= `search_key`.
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
        let pri_key_bytes = stored_pk.get_data().unwrap_or(&[]).to_vec();
        p_key.set_data(&pri_key_bytes);
        let pri_key_entry = DatabaseEntry::from_bytes(&pri_key_bytes);
        let primary = self.secondary_db.primary_db().lock();
        let pri_status = primary.get(self.txn, &pri_key_entry, data)?;
        if pri_status != OperationStatus::Success {
            return Err(NoxuError::SecondaryIntegrityException(format!(
                "Secondary '{}' refers to missing primary key",
                self.secondary_db.get_database_name()
            )));
        }
        Ok(OperationStatus::Success)
    }

    pub fn close(&mut self) -> Result<()> {
        self.inner.close()
    }

    pub fn is_valid(&self) -> bool {
        self.inner.is_valid()
    }

    // ------------------------------------------------------------------
    // Join-cursor helpers (used by JoinCursor internals).
    // ------------------------------------------------------------------

    pub(crate) fn get_current_primary_key_only(
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
        Ok(pri_key_entry.get_data().map(|d| d.to_vec()))
    }

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

    /// Returns the count of primary keys that share the current
    /// secondary key.  In sorted-dup mode this is the duplicate count
    /// reported by the inner cursor.
    pub(crate) fn count_estimate(&mut self) -> u64 {
        self.inner.count().unwrap_or_default()
    }

    /// Advances to the next duplicate of the current secondary key
    /// (driver method used by [`crate::join_cursor::JoinCursor`]).
    pub(crate) fn next_dup_only(&mut self) -> Result<OperationStatus> {
        let mut sec_key = DatabaseEntry::new();
        let mut pri_key_entry = DatabaseEntry::new();
        let status =
            self.inner.get(&mut sec_key, &mut pri_key_entry, Get::NextDup, None)?;
        if status != OperationStatus::Success {
            return Ok(OperationStatus::NotFound);
        }
        Ok(OperationStatus::Success)
    }

    /// Returns `true` if the primary key at the current cursor position
    /// matches `candidate`.
    pub(crate) fn has_candidate_primary_key(
        &mut self,
        candidate: &[u8],
    ) -> Result<bool> {
        match self.get_current_primary_key_only()? {
            Some(pk) => Ok(pk == candidate),
            None => Ok(false),
        }
    }

    /// Positions this cursor at the exact `(sec_key, pri_key)` pair on
    /// its inner sorted-dup index, without resolving the primary record.
    ///
    /// Used by [`crate::join_cursor::JoinCursor`] to probe whether a
    /// candidate primary key is present in this cursor's secondary key
    /// duplicate set.  Returns `Ok(true)` if found, `Ok(false)` otherwise.
    pub(crate) fn position_on_pair(
        &mut self,
        sec_key: &[u8],
        pri_key: &[u8],
    ) -> Result<bool> {
        let mut sk = DatabaseEntry::from_bytes(sec_key);
        let mut spk = DatabaseEntry::from_bytes(pri_key);
        let st = self.inner.get(&mut sk, &mut spk, Get::SearchBoth, None)?;
        Ok(st == OperationStatus::Success)
    }

    // ------------------------------------------------------------------
    // Private helpers
    // ------------------------------------------------------------------

    fn get_with_mode(
        &mut self,
        key: &mut DatabaseEntry,
        p_key: &mut DatabaseEntry,
        data: &mut DatabaseEntry,
        mode: Get,
    ) -> Result<OperationStatus> {
        let mut pri_key_bytes_entry = DatabaseEntry::new();
        let status =
            self.inner.get(key, &mut pri_key_bytes_entry, mode, None)?;
        if status != OperationStatus::Success {
            return Ok(OperationStatus::NotFound);
        }
        let pri_key_bytes =
            pri_key_bytes_entry.get_data().unwrap_or(&[]).to_vec();
        p_key.set_data(&pri_key_bytes);
        let pri_key_entry = DatabaseEntry::from_bytes(&pri_key_bytes);
        let primary = self.secondary_db.primary_db().lock();
        let pri_status = primary.get(self.txn, &pri_key_entry, data)?;
        if pri_status != OperationStatus::Success {
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

        let sec_db_config = DatabaseConfig::new()
            .with_allow_create(true)
            .with_sorted_duplicates(true);
        let sec_db =
            env.open_database(None, "secondary", &sec_db_config).unwrap();
        let sec_config = SecondaryConfig::new()
            .with_allow_create(true)
            .with_sorted_duplicates(true)
            .with_key_creator(Box::new(FirstByteKeyCreator));
        let secondary =
            SecondaryDatabase::open(Arc::clone(&primary), sec_db, sec_config)
                .unwrap();

        (temp_dir, env, primary, secondary)
    }

    fn insert_via_primary(
        primary: &Arc<Mutex<Database>>,
        key: &[u8],
        value: &[u8],
    ) {
        let pk = DatabaseEntry::from_bytes(key);
        let pv = DatabaseEntry::from_bytes(value);
        primary.lock().put(None, &pk, &pv).unwrap();
    }

    #[test]
    fn test_cursor_get_first_last() {
        let (_tmp, _env, primary, secondary) = temp_env_primary_secondary();
        insert_via_primary(&primary, b"pk2", b"Banana");
        insert_via_primary(&primary, b"pk1", b"Apple");
        insert_via_primary(&primary, b"pk3", b"Cherry");

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
        insert_via_primary(&primary, b"pk1", b"Apple");
        insert_via_primary(&primary, b"pk2", b"Banana");
        insert_via_primary(&primary, b"pk3", b"Cherry");

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
        insert_via_primary(&primary, b"pk1", b"Mango");
        insert_via_primary(&primary, b"pk2", b"Kiwi");

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
        insert_via_primary(&primary, b"pk1", b"Apple");

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
    fn test_cursor_get_search_key_range_exact() {
        let (_tmp, _env, primary, secondary) = temp_env_primary_secondary();
        insert_via_primary(&primary, b"pk1", b"Apple");
        insert_via_primary(&primary, b"pk2", b"Banana");
        insert_via_primary(&primary, b"pk3", b"Cherry");

        let mut cursor = secondary.open_cursor(None, None).unwrap();
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
        insert_via_primary(&primary, b"pk1", b"Apple");
        insert_via_primary(&primary, b"pk3", b"Cherry");

        let mut cursor = secondary.open_cursor(None, None).unwrap();
        let mut search_key = DatabaseEntry::from_bytes(b"B");
        let mut p_key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();

        let status = cursor
            .get_search_key_range(&mut search_key, &mut p_key, &mut data)
            .unwrap();
        assert_eq!(status, OperationStatus::Success);
        assert_eq!(data.get_data().unwrap(), b"Cherry");
    }

    /// v1.6: get_search_key + get_next_dup walks the entire duplicate
    /// set bound to a shared secondary key.
    #[test]
    fn test_cursor_get_next_dup_walks_duplicates() {
        let (_tmp, _env, primary, secondary) = temp_env_primary_secondary();
        insert_via_primary(&primary, b"pk1", b"Apple");
        insert_via_primary(&primary, b"pk2", b"Apricot");
        insert_via_primary(&primary, b"pk3", b"Avocado");
        insert_via_primary(&primary, b"pk4", b"Banana");

        let mut cursor = secondary.open_cursor(None, None).unwrap();
        let mut p_key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();
        let st = cursor
            .get_search_key(
                &DatabaseEntry::from_bytes(b"A"),
                &mut p_key,
                &mut data,
            )
            .unwrap();
        assert_eq!(st, OperationStatus::Success);

        let mut found = vec![p_key.get_data().unwrap().to_vec()];
        loop {
            let mut sk = DatabaseEntry::new();
            let mut pk = DatabaseEntry::new();
            let mut d = DatabaseEntry::new();
            match cursor.get_next_dup(&mut sk, &mut pk, &mut d).unwrap() {
                OperationStatus::Success => {
                    found.push(pk.get_data().unwrap().to_vec());
                }
                _ => break,
            }
        }
        found.sort();
        assert_eq!(found, vec![b"pk1".to_vec(), b"pk2".to_vec(), b"pk3".to_vec()]);
    }
}
