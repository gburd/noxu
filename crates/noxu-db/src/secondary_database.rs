//! Secondary database handle.
//!
//!
//! A secondary database is an index over a primary database.  Records are
//! automatically maintained when the primary is written.  Reads via a
//! secondary return primary data; deletes via a secondary delete the
//! corresponding primary record.
//!
//! The mapping of secondary keys to primary records is stored in an ordinary
//! Database whose records have the form:
//!
//!   key   = secondary_key
//!   value = primary_key
//!
//! On every primary `put` the secondary is updated via `update_secondary`.
//! On every primary `delete` the secondary entry is removed.

use crate::cursor::Cursor;
use crate::cursor_config::CursorConfig;
use crate::database::Database;
use crate::database_entry::DatabaseEntry;
use crate::error::{NoxuError, Result};
use crate::operation_status::OperationStatus;
use crate::secondary_config::SecondaryConfig;
use crate::secondary_cursor::SecondaryCursor;
use crate::transaction::Transaction;
use noxu_dbi::{CursorImpl, GetMode};
use noxu_sync::Mutex;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// A secondary (index) database handle.
///
///
///
/// Secondary databases are always associated with a primary database.
/// Key characteristics:
/// - Direct `put` calls are prohibited; use the primary database instead.
/// - `delete` on a secondary deletes the primary record (and all its
///   secondary index entries).
/// - `get` returns primary record data, not secondary data.
/// - `open_cursor` returns a [`SecondaryCursor`].
///
/// # Example
/// ```ignore
/// use noxu_db::{Database, DatabaseEntry};
/// use noxu_db::secondary_config::{SecondaryConfig, SecondaryKeyCreator};
/// use noxu_db::secondary_database::SecondaryDatabase;
///
/// struct MyKeyCreator;
/// impl SecondaryKeyCreator for MyKeyCreator { /* ... */ }
///
/// let sec_config = SecondaryConfig::new()
///     .with_allow_create(true)
///     .with_allow_populate(true)
///     .with_key_creator(Box::new(MyKeyCreator));
///
/// let secondary = SecondaryDatabase::open(primary_db, "my_index", sec_config)?;
/// ```
pub struct SecondaryDatabase {
    /// The underlying secondary index storage (sec_key -> pri_key).
    inner: Database,
    /// The primary database this index is associated with.
    primary: Arc<Mutex<Database>>,
    /// The secondary configuration (holds key creator callback, etc.).
    config: SecondaryConfig,
    /// Whether this secondary is fully populated (not in incremental mode).
    is_fully_populated: AtomicBool,
}

impl SecondaryDatabase {
    /// Opens or creates a secondary database associated with `primary`.
    ///
    ///
    ///
    /// # Arguments
    /// * `primary` - The primary database handle, shared via `Arc<Mutex<_>>`.
    /// * `secondary_db` - An already-opened `Database` that will serve as the
    ///   underlying storage for the secondary index.
    /// * `config` - The secondary configuration (must include a key creator).
    ///
    /// # Errors
    /// - `NoxuError::IllegalArgument` if the configuration is invalid.
    pub fn open(
        primary: Arc<Mutex<Database>>,
        secondary_db: Database,
        config: SecondaryConfig,
    ) -> Result<Self> {
        // Validate the config w.r.t. the primary's read-only flag.
        let primary_read_only = primary.lock().get_config().read_only;
        config
            .validate(primary_read_only)
            .map_err(NoxuError::IllegalArgument)?;

        let sec = SecondaryDatabase {
            inner: secondary_db,
            primary,
            config,
            is_fully_populated: AtomicBool::new(true),
        };

        // If allow_populate and the secondary is empty, populate from primary.
        if sec.config.allow_populate {
            sec.populate_if_empty()?;
        }

        Ok(sec)
    }

    // ------------------------------------------------------------------
    // Public API
    // ------------------------------------------------------------------

    /// Returns the database name of the secondary index.
    pub fn get_database_name(&self) -> &str {
        self.inner.get_database_name()
    }

    /// Returns the secondary configuration.
    ///
    ///
    pub fn get_config(&self) -> &SecondaryConfig {
        &self.config
    }

    /// Returns whether this handle is open.
    pub fn is_valid(&self) -> bool {
        self.inner.is_valid()
    }

    /// Closes the secondary database handle.
    ///
    ///
    pub fn close(&self) -> Result<()> {
        self.inner.close()
    }

    /// Retrieves a primary record by secondary key.
    ///
    ///
    ///
    /// Looks up `key` in the secondary index, obtains the primary key stored
    /// there, then fetches the corresponding record from the primary database.
    ///
    /// # Arguments
    /// * `txn` - Optional transaction.
    /// * `key` - The secondary key to search for.
    /// * `p_key` - Output: receives the primary key found.
    /// * `data` - Output: receives the primary record data.
    ///
    /// # Returns
    /// `OperationStatus::Success` if found; `OperationStatus::NotFound` otherwise.
    pub fn get(
        &self,
        txn: Option<&Transaction>,
        key: &DatabaseEntry,
        p_key: &mut DatabaseEntry,
        data: &mut DatabaseEntry,
    ) -> Result<OperationStatus> {
        self.check_open()?;
        self.check_readable()?;

        // Look up the secondary key in the index to get the primary key.
        let mut pri_key_entry = DatabaseEntry::new();
        let status = self.inner.get(txn, key, &mut pri_key_entry)?;

        if status != OperationStatus::Success {
            return Ok(OperationStatus::NotFound);
        }

        // Store the primary key in the output parameter.
        if let Some(pk) = pri_key_entry.get_data() {
            p_key.set_data(pk);
        }

        // Now fetch the primary record.
        let primary = self.primary.lock();
        let pri_status = primary.get(txn, &pri_key_entry, data)?;
        if pri_status != OperationStatus::Success {
            // Secondary refers to a missing primary — integrity issue.
            return Err(NoxuError::SecondaryIntegrityException(format!(
                "Secondary '{}' refers to missing primary key",
                self.get_database_name()
            )));
        }

        Ok(OperationStatus::Success)
    }

    /// Deletes all primary records whose secondary key equals `key`.
    ///
    ///
    ///
    /// All duplicate secondary index entries with the given secondary key are
    /// found and their corresponding primary records deleted.  Each primary
    /// deletion in turn removes all secondary index entries for that primary
    /// record.
    ///
    /// # Arguments
    /// * `txn` - Optional transaction.
    /// * `key` - The secondary key whose primary records should be deleted.
    ///
    /// # Returns
    /// `OperationStatus::Success` if at least one record was deleted;
    /// `OperationStatus::NotFound` if the key was not found.
    pub fn delete(
        &self,
        txn: Option<&Transaction>,
        key: &DatabaseEntry,
    ) -> Result<OperationStatus> {
        self.check_open()?;

        // Use a secondary cursor to iterate all duplicates of the secondary key.
        let mut sec_cursor = self.open_cursor_internal()?;
        let mut p_key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();

        // Position to the first record with this secondary key.
        let status = sec_cursor.get_search_key(key, &mut p_key, &mut data)?;

        if status != OperationStatus::Success {
            return Ok(OperationStatus::NotFound);
        }

        // We found at least one; iterate and delete all matching primary records.
        loop {
            let pri_key_bytes = p_key.get_data().unwrap_or(&[]).to_vec();
            let pri_key_entry = DatabaseEntry::from_bytes(&pri_key_bytes);

            // 1. Remove all secondary entries for this primary record first.
            //    This includes the current secondary key entry we found.
            //    UpdateSecondaryOnDelete calls updateSecondary.
            let old_data = data.clone();
            self.delete_all_for_primary(&pri_key_entry, Some(&old_data))?;

            // 2. Delete the primary record.
            {
                let primary = self.primary.lock();
                let _ = primary.delete(txn, &pri_key_entry)?;
            }

            // Re-search for the key to find any remaining duplicates.
            // Since delete_all_for_primary cleaned up secondary entries,
            // this should return NotFound when no more duplicates exist.
            p_key = DatabaseEntry::new();
            data = DatabaseEntry::new();
            let next_status =
                sec_cursor.get_search_key(key, &mut p_key, &mut data)?;
            if next_status != OperationStatus::Success {
                break;
            }
        }

        Ok(OperationStatus::Success)
    }

    /// Opens a cursor on the secondary database.
    ///
    /// When `txn` is `Some(_)`, the inner cursor over the secondary index
    /// participates in the supplied transaction — reads acquire shared
    /// locks via the txn's locker and any writes (currently only the
    /// primary delete cascade triggered by `SecondaryCursor::delete`) are
    /// rolled back when the txn aborts.  When `txn` is `None` the inner
    /// cursor runs in auto-commit mode.
    ///
    /// `config` is forwarded to the inner `Database::open_cursor` call so
    /// `read_uncommitted` and other cursor-level flags propagate correctly.
    ///
    /// # Returns
    /// A `SecondaryCursor` that iterates secondary index entries and returns
    /// primary data.
    pub fn open_cursor(
        &self,
        txn: Option<&Transaction>,
        config: Option<&CursorConfig>,
    ) -> Result<SecondaryCursor<'_>> {
        self.check_open()?;
        self.check_readable()?;
        SecondaryCursor::new(self, txn, config)
    }

    /// Starts incremental population mode.
    ///
    ///
    pub fn start_incremental_population(&self) {
        self.is_fully_populated.store(false, Ordering::Release);
    }

    /// Ends incremental population mode.
    ///
    ///
    pub fn end_incremental_population(&self) {
        self.is_fully_populated.store(true, Ordering::Release);
    }

    /// Returns whether incremental population is currently enabled.
    ///
    ///
    pub fn is_incremental_population_enabled(&self) -> bool {
        !self.is_fully_populated.load(Ordering::Acquire)
    }

    // ------------------------------------------------------------------
    // Internal helpers called by Database and SecondaryCursor
    // ------------------------------------------------------------------

    /// Updates the secondary index when a primary record is inserted or updated.
    ///
    ///
    ///
    /// Called from `Database::put_and_update_secondaries` (see database.rs
    /// integration layer) and from application code that manages secondary
    /// index updates manually (i.e., without integrated auto-update support).
    ///
    /// # Arguments
    /// * `pri_key` - The primary key.
    /// * `old_data` - The previous primary data, or `None` on insert.
    /// * `new_data` - The new primary data, or `None` on delete.
    pub fn update_secondary(
        &self,
        pri_key: &DatabaseEntry,
        old_data: Option<&DatabaseEntry>,
        new_data: Option<&DatabaseEntry>,
    ) -> Result<()> {
        let key_creator = &self.config.key_creator;
        let multi_key_creator = &self.config.multi_key_creator;

        // Tombstones (both old and new are None) — nothing to do.
        if old_data.is_none() && new_data.is_none() {
            return Ok(());
        }

        if let Some(creator) = key_creator {
            // Single-key creator path.
            let old_sec_key = old_data.and_then(|od| {
                let mut sk = DatabaseEntry::new();
                // The inner.* borrow requires a temporary Database borrow.
                // We use &self.inner directly, which satisfies the lifetime.
                if creator.create_secondary_key(
                    &self.inner,
                    pri_key,
                    od,
                    &mut sk,
                ) {
                    Some(sk)
                } else {
                    None
                }
            });

            let new_sec_key = new_data.and_then(|nd| {
                let mut sk = DatabaseEntry::new();
                if creator.create_secondary_key(
                    &self.inner,
                    pri_key,
                    nd,
                    &mut sk,
                ) {
                    Some(sk)
                } else {
                    None
                }
            });

            let do_delete = old_sec_key.is_some()
                && old_sec_key.as_ref() != new_sec_key.as_ref();
            let do_insert = new_sec_key.is_some()
                && new_sec_key.as_ref() != old_sec_key.as_ref();

            if do_delete {
                self.delete_sec_key(old_sec_key.as_ref().unwrap(), pri_key)?;
            }
            if do_insert {
                self.insert_sec_key(new_sec_key.as_ref().unwrap(), pri_key)?;
            }
        } else if let Some(multi_creator) = multi_key_creator {
            // Multi-key creator path.
            let empty = Vec::<DatabaseEntry>::new();

            let old_keys: Vec<DatabaseEntry> = if let Some(od) = old_data {
                let mut keys = Vec::new();
                multi_creator.create_secondary_keys(
                    &self.inner,
                    pri_key,
                    od,
                    &mut keys,
                );
                keys
            } else {
                empty.clone()
            };

            let new_keys: Vec<DatabaseEntry> = if let Some(nd) = new_data {
                let mut keys = Vec::new();
                multi_creator.create_secondary_keys(
                    &self.inner,
                    pri_key,
                    nd,
                    &mut keys,
                );
                keys
            } else {
                empty
            };

            // Delete keys that are no longer present.
            for old_key in &old_keys {
                if !new_keys.contains(old_key) {
                    self.delete_sec_key(old_key, pri_key)?;
                }
            }
            // Insert keys that were not present before.
            for new_key in &new_keys {
                if !old_keys.contains(new_key) {
                    self.insert_sec_key(new_key, pri_key)?;
                }
            }
        }

        Ok(())
    }

    /// Removes all secondary index entries for the given primary key.
    ///
    /// Called when a primary record is deleted.
    pub(crate) fn delete_all_for_primary(
        &self,
        pri_key: &DatabaseEntry,
        old_data: Option<&DatabaseEntry>,
    ) -> Result<()> {
        self.update_secondary(pri_key, old_data, None)
    }

    /// Returns a reference to the inner index `Database`.
    pub(crate) fn inner_db(&self) -> &Database {
        &self.inner
    }

    /// Returns a reference to the primary `Database` (via the mutex).
    pub(crate) fn primary_db(&self) -> &Arc<Mutex<Database>> {
        &self.primary
    }

    // ------------------------------------------------------------------
    // Private helpers
    // ------------------------------------------------------------------

    /// Inserts a secondary index entry: (sec_key -> pri_key).
    fn insert_sec_key(
        &self,
        sec_key: &DatabaseEntry,
        pri_key: &DatabaseEntry,
    ) -> Result<()> {
        // The secondary database stores sec_key -> pri_key.
        // If the secondary allows duplicates, use NoOverwrite-equivalent
        // (NO_DUP_DATA).  If unique, use NoOverwrite.
        // For this implementation we use Overwrite (idempotent), which is
        // safe for the fully-populated path since insert_sec_key is only
        // called when the key did not previously exist.
        let mut cursor = self.make_inner_cursor()?;
        cursor
            .put(sec_key, pri_key, crate::put::Put::Overwrite)
            .map_err(|e| NoxuError::OperationNotAllowed(e.to_string()))?;
        Ok(())
    }

    /// Deletes a secondary index entry: (sec_key -> pri_key).
    fn delete_sec_key(
        &self,
        sec_key: &DatabaseEntry,
        pri_key: &DatabaseEntry,
    ) -> Result<()> {
        // Find the exact (sec_key, pri_key) pair and delete it.
        // For non-dup databases a simple key search suffices.
        // For dup databases we need a SEARCH_BOTH, but since the inner
        // database is always configured as a simple key->value store in our
        // implementation (sec_key->pri_key, no dup support at the b-tree level),
        // a key search is correct.
        let mut cursor = self.make_inner_cursor()?;
        let mut stored_pk = DatabaseEntry::new();
        // Clone sec_key because Cursor::get requires &mut but key is input-only for Search.
        let mut sec_key_mut = sec_key.clone();
        let status = cursor
            .get(
                &mut sec_key_mut,
                &mut stored_pk,
                crate::get::Get::Search,
                None,
            )
            .map_err(|e| NoxuError::OperationNotAllowed(e.to_string()))?;

        if status == OperationStatus::Success {
            // Verify the stored primary key matches before deleting.
            if stored_pk.get_data() == pri_key.get_data() {
                cursor.delete().map_err(|e| {
                    NoxuError::OperationNotAllowed(e.to_string())
                })?;
            }
        }
        // If not found, the secondary may already have been cleaned up; ignore.
        Ok(())
    }

    /// Builds a writable `Cursor` on the inner secondary index `Database`.
    fn make_inner_cursor(&self) -> Result<Cursor> {
        self.inner.open_cursor(None, None)
    }

    /// Builds a `SecondaryCursor` on this secondary database (internal).
    ///
    /// Called from auto-commit code paths that do not have a transaction
    /// handle (e.g. `SecondaryDatabase::delete`, which currently runs
    /// secondary cleanup auto-committed; see audit finding F5 for the
    /// follow-up work to plumb the txn through these paths).
    fn open_cursor_internal(&self) -> Result<SecondaryCursor<'_>> {
        SecondaryCursor::new(self, None, None)
    }

    /// Populates the secondary index from the primary if the secondary is empty.
    ///
    /// Population logic in `SecondaryDatabase.init`.
    fn populate_if_empty(&self) -> Result<()> {
        // Check if the secondary is empty.
        let sec_count = self.inner.count()?;
        if sec_count > 0 {
            return Ok(());
        }

        // Use direct CursorImpl scan to access both key and value.
        let primary = self.primary.lock();
        self.populate_from_primary_scan(&primary)?;

        Ok(())
    }

    /// Scans the primary database and inserts secondary index entries.
    fn populate_from_primary_scan(&self, primary: &Database) -> Result<()> {
        // We access the inner DatabaseImpl directly to read both key and value.
        // The public Cursor::get API currently only returns data, not key.
        // Use a dedicated scan loop via CursorImpl.
        let mut cursor = CursorImpl::new(Arc::clone(&primary.db_impl), 0);

        let mut first_status = cursor
            .get_first()
            .map_err(|e| NoxuError::OperationNotAllowed(e.to_string()))?;

        while first_status == noxu_dbi::OperationStatus::Success {
            let (k, v) = cursor
                .get_current()
                .map_err(|e| NoxuError::OperationNotAllowed(e.to_string()))?;

            let pri_key = DatabaseEntry::from_bytes(&k);
            let pri_data = DatabaseEntry::from_bytes(&v);

            // Create secondary key(s) and insert them.
            if let Some(creator) = &self.config.key_creator {
                let mut sec_key = DatabaseEntry::new();
                if creator.create_secondary_key(
                    &self.inner,
                    &pri_key,
                    &pri_data,
                    &mut sec_key,
                ) {
                    self.insert_sec_key(&sec_key, &pri_key)?;
                }
            } else if let Some(multi_creator) = &self.config.multi_key_creator {
                let mut sec_keys = Vec::new();
                multi_creator.create_secondary_keys(
                    &self.inner,
                    &pri_key,
                    &pri_data,
                    &mut sec_keys,
                );
                for sec_key in sec_keys {
                    self.insert_sec_key(&sec_key, &pri_key)?;
                }
            }

            first_status = cursor
                .retrieve_next(GetMode::Next)
                .map_err(|e| NoxuError::OperationNotAllowed(e.to_string()))?;
        }

        Ok(())
    }

    /// Checks that this database is open.
    fn check_open(&self) -> Result<()> {
        if !self.inner.is_valid() {
            return Err(NoxuError::DatabaseClosed);
        }
        Ok(())
    }

    /// Checks that this database is readable (not in incremental population mode).
    fn check_readable(&self) -> Result<()> {
        if !self.is_fully_populated.load(Ordering::Acquire) {
            return Err(NoxuError::OperationNotAllowed(
                "Incremental population is currently enabled".to_string(),
            ));
        }
        Ok(())
    }
}

impl Drop for SecondaryDatabase {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database_config::DatabaseConfig;
    use crate::environment::Environment;
    use crate::environment_config::EnvironmentConfig;
    use crate::secondary_config::{SecondaryConfig, SecondaryKeyCreator};
    use tempfile::TempDir;

    /// A simple key creator that uses the first byte of the value as the
    /// secondary key.
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

    fn temp_env() -> (TempDir, Environment) {
        let temp_dir = TempDir::new().unwrap();
        let env_config = EnvironmentConfig::new(temp_dir.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true);
        let env = Environment::open(env_config).unwrap();
        (temp_dir, env)
    }

    fn open_primary(env: &Environment, name: &str) -> Database {
        let config = DatabaseConfig::new().with_allow_create(true);
        env.open_database(None, name, &config).unwrap()
    }

    fn open_secondary(
        primary: Arc<Mutex<Database>>,
        env: &Environment,
        name: &str,
    ) -> SecondaryDatabase {
        let sec_db_config = DatabaseConfig::new().with_allow_create(true);
        let sec_db = env.open_database(None, name, &sec_db_config).unwrap();
        let sec_config = SecondaryConfig::new()
            .with_allow_create(true)
            .with_key_creator(Box::new(FirstByteKeyCreator));
        SecondaryDatabase::open(primary, sec_db, sec_config).unwrap()
    }

    #[test]
    fn test_open_secondary() {
        let (_tmp, env) = temp_env();
        let primary = Arc::new(Mutex::new(open_primary(&env, "primary")));
        let secondary = open_secondary(Arc::clone(&primary), &env, "secondary");
        assert!(secondary.is_valid());
        assert_eq!(secondary.get_database_name(), "secondary");
    }

    #[test]
    fn test_put_primary_updates_secondary() {
        let (_tmp, env) = temp_env();
        let primary = Arc::new(Mutex::new(open_primary(&env, "primary")));
        let secondary = open_secondary(Arc::clone(&primary), &env, "secondary");

        // Write to primary; secondary is not auto-updated here because
        // Database::put does not know about secondaries by default.
        // We manually call update_secondary for this test.
        let pri_key = DatabaseEntry::from_bytes(b"pk1");
        let pri_data = DatabaseEntry::from_bytes(b"Avalon");
        {
            let primary = primary.lock();
            primary.put(None, &pri_key, &pri_data).unwrap();
        }

        // Update the secondary index manually (mimics the integration layer).
        secondary.update_secondary(&pri_key, None, Some(&pri_data)).unwrap();

        // Retrieve by secondary key (first byte of "Avalon" = 'A' = 0x41).
        let sec_key = DatabaseEntry::from_bytes(b"A");
        let mut p_key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();
        let status =
            secondary.get(None, &sec_key, &mut p_key, &mut data).unwrap();

        assert_eq!(status, OperationStatus::Success);
        assert_eq!(p_key.get_data().unwrap(), b"pk1");
        assert_eq!(data.get_data().unwrap(), b"Avalon");
    }

    #[test]
    fn test_get_by_secondary_key() {
        let (_tmp, env) = temp_env();
        let primary = Arc::new(Mutex::new(open_primary(&env, "primary")));
        let secondary = open_secondary(Arc::clone(&primary), &env, "secondary");

        // Insert primary records and index them.
        let records: &[(&[u8], &[u8])] =
            &[(b"pk1", b"Apple"), (b"pk2", b"Banana"), (b"pk3", b"Avocado")];

        for (k, v) in records {
            let pk = DatabaseEntry::from_bytes(k);
            let pv = DatabaseEntry::from_bytes(v);
            {
                primary.lock().put(None, &pk, &pv).unwrap();
            }
            secondary.update_secondary(&pk, None, Some(&pv)).unwrap();
        }

        // Search by secondary key 'B'.
        let sec_key = DatabaseEntry::from_bytes(b"B");
        let mut p_key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();
        let status =
            secondary.get(None, &sec_key, &mut p_key, &mut data).unwrap();

        assert_eq!(status, OperationStatus::Success);
        assert_eq!(data.get_data().unwrap(), b"Banana");

        // Search for non-existent secondary key.
        let missing = DatabaseEntry::from_bytes(b"Z");
        let status =
            secondary.get(None, &missing, &mut p_key, &mut data).unwrap();
        assert_eq!(status, OperationStatus::NotFound);
    }

    #[test]
    fn test_delete_via_secondary() {
        let (_tmp, env) = temp_env();
        let primary = Arc::new(Mutex::new(open_primary(&env, "primary")));
        let secondary = open_secondary(Arc::clone(&primary), &env, "secondary");

        let pri_key = DatabaseEntry::from_bytes(b"pk1");
        let pri_data = DatabaseEntry::from_bytes(b"Cherry");
        {
            primary.lock().put(None, &pri_key, &pri_data).unwrap();
        }
        secondary.update_secondary(&pri_key, None, Some(&pri_data)).unwrap();

        // Delete via secondary key.
        let sec_key = DatabaseEntry::from_bytes(b"C");
        let status = secondary.delete(None, &sec_key).unwrap();
        assert_eq!(status, OperationStatus::Success);

        // Primary record should be gone.
        let mut data = DatabaseEntry::new();
        let get_status = primary.lock().get(None, &pri_key, &mut data).unwrap();
        assert_eq!(get_status, OperationStatus::NotFound);
    }

    #[test]
    fn test_update_changes_secondary_key() {
        let (_tmp, env) = temp_env();
        let primary = Arc::new(Mutex::new(open_primary(&env, "primary")));
        let secondary = open_secondary(Arc::clone(&primary), &env, "secondary");

        let pri_key = DatabaseEntry::from_bytes(b"pk1");
        let old_data = DatabaseEntry::from_bytes(b"Mango");
        let new_data = DatabaseEntry::from_bytes(b"Pineapple");

        {
            primary.lock().put(None, &pri_key, &old_data).unwrap();
        }
        secondary.update_secondary(&pri_key, None, Some(&old_data)).unwrap();

        // Now update the primary; the secondary key 'M' should be replaced by 'P'.
        {
            primary.lock().put(None, &pri_key, &new_data).unwrap();
        }
        secondary
            .update_secondary(&pri_key, Some(&old_data), Some(&new_data))
            .unwrap();

        // Old key 'M' should no longer be in the secondary.
        let old_sec = DatabaseEntry::from_bytes(b"M");
        let mut pk = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();
        let status = secondary.get(None, &old_sec, &mut pk, &mut data).unwrap();
        assert_eq!(status, OperationStatus::NotFound);

        // New key 'P' should be present.
        let new_sec = DatabaseEntry::from_bytes(b"P");
        let status = secondary.get(None, &new_sec, &mut pk, &mut data).unwrap();
        assert_eq!(status, OperationStatus::Success);
        assert_eq!(data.get_data().unwrap(), b"Pineapple");
    }

    #[test]
    fn test_cursor_scan_secondary() {
        let (_tmp, env) = temp_env();
        let primary = Arc::new(Mutex::new(open_primary(&env, "primary")));
        let secondary = open_secondary(Arc::clone(&primary), &env, "secondary");

        // Insert records with distinct first bytes.
        let records: &[(&[u8], &[u8])] =
            &[(b"pk1", b"Banana"), (b"pk2", b"Cherry"), (b"pk3", b"Apple")];
        for (k, v) in records {
            let pk = DatabaseEntry::from_bytes(k);
            let pv = DatabaseEntry::from_bytes(v);
            primary.lock().put(None, &pk, &pv).unwrap();
            secondary.update_secondary(&pk, None, Some(&pv)).unwrap();
        }

        // Iterate via SecondaryCursor and collect all secondary keys encountered.
        let mut cursor = secondary.open_cursor(None, None).unwrap();
        let mut sec_keys_seen: Vec<Vec<u8>> = Vec::new();
        let mut sec_key = DatabaseEntry::new();
        let mut p_key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();

        let status =
            cursor.get_first(&mut sec_key, &mut p_key, &mut data).unwrap();
        let mut current = status;
        while current == OperationStatus::Success {
            if let Some(k) = sec_key.get_data() {
                sec_keys_seen.push(k.to_vec());
            }
            current =
                cursor.get_next(&mut sec_key, &mut p_key, &mut data).unwrap();
        }

        // We expect 3 entries (A, B, C in secondary key order).
        assert_eq!(sec_keys_seen.len(), 3);
        assert_eq!(sec_keys_seen[0], b"A");
        assert_eq!(sec_keys_seen[1], b"B");
        assert_eq!(sec_keys_seen[2], b"C");
    }

    #[test]
    fn test_incremental_population() {
        let (_tmp, env) = temp_env();
        let primary = Arc::new(Mutex::new(open_primary(&env, "primary")));
        let secondary = open_secondary(Arc::clone(&primary), &env, "secondary");

        secondary.start_incremental_population();
        assert!(secondary.is_incremental_population_enabled());

        // Reads should fail during incremental population.
        let sec_key = DatabaseEntry::from_bytes(b"A");
        let mut pk = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();
        let result = secondary.get(None, &sec_key, &mut pk, &mut data);
        assert!(result.is_err());

        secondary.end_incremental_population();
        assert!(!secondary.is_incremental_population_enabled());
    }

    #[test]
    fn test_populate_on_open() {
        let (_tmp, env) = temp_env();
        let primary = Arc::new(Mutex::new(open_primary(&env, "primary")));

        // Pre-populate the primary.
        let records: &[(&[u8], &[u8])] =
            &[(b"pk1", b"Grape"), (b"pk2", b"Watermelon")];
        for (k, v) in records {
            primary
                .lock()
                .put(
                    None,
                    &DatabaseEntry::from_bytes(k),
                    &DatabaseEntry::from_bytes(v),
                )
                .unwrap();
        }

        // Open secondary with allow_populate=true.
        let sec_db_config = DatabaseConfig::new().with_allow_create(true);
        let sec_db =
            env.open_database(None, "secondary_pop", &sec_db_config).unwrap();
        let sec_config = SecondaryConfig::new()
            .with_allow_create(true)
            .with_allow_populate(true)
            .with_key_creator(Box::new(FirstByteKeyCreator));
        let secondary =
            SecondaryDatabase::open(Arc::clone(&primary), sec_db, sec_config)
                .unwrap();

        // The secondary should have been populated.
        let sec_key_g = DatabaseEntry::from_bytes(b"G");
        let mut pk = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();
        let status =
            secondary.get(None, &sec_key_g, &mut pk, &mut data).unwrap();
        assert_eq!(status, OperationStatus::Success);
        assert_eq!(data.get_data().unwrap(), b"Grape");
    }
}
