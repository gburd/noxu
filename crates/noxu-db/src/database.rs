//! Database handle.
//!
//! Port of `com.sleepycat.je.Database`.

use crate::cursor::Cursor;
use crate::cursor_config::CursorConfig;
use crate::database_config::DatabaseConfig;
use crate::database_entry::DatabaseEntry;
use crate::error::{NoxuError, Result};
use crate::operation_status::OperationStatus;
use crate::sequence::Sequence;
use crate::sequence_config::SequenceConfig;
use crate::transaction::Transaction;
use noxu_dbi::{CursorImpl, DatabaseImpl, EnvironmentImpl, GetMode, PutMode, SearchMode};
use noxu_sync::{Mutex, RwLock};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// A database handle.
///
/// Port of `com.sleepycat.je.Database`.
///
/// Database handles provide methods for inserting, retrieving, and
/// deleting records. A database belongs to a single environment.
///
/// # Example
/// ```ignore
/// use noxu_db::{Environment, EnvironmentConfig, DatabaseConfig, DatabaseEntry};
/// use std::path::PathBuf;
///
/// let env_config = EnvironmentConfig::new(PathBuf::from("/tmp/mydb"))
///     .allow_create(true);
/// let env = Environment::open(env_config).unwrap();
///
/// let db_config = DatabaseConfig::new().allow_create(true);
/// let db = env.open_database(None, "mydb", &db_config).unwrap();
///
/// let key = DatabaseEntry::from_bytes(b"key1");
/// let value = DatabaseEntry::from_bytes(b"value1");
/// db.put(None, &key, &value).unwrap();
///
/// db.close().unwrap();
/// env.close().unwrap();
/// ```
pub struct Database {
    /// Name of this database
    name: String,
    /// Database ID
    id: u64,
    /// Configuration
    config: DatabaseConfig,
    /// The underlying DatabaseImpl (shared with the EnvironmentImpl).
    pub(crate) db_impl: Arc<RwLock<DatabaseImpl>>,
    /// Back-reference to the owning EnvironmentImpl (for close/cleanup).
    env_impl: Arc<Mutex<EnvironmentImpl>>,
    /// Whether this handle is open
    open: AtomicBool,
}

/// State of a database handle.
///
/// Port of `Database.DbState`.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DbState {
    /// Database is open and operational
    Open,
    /// Database has been closed
    Closed,
    /// Database is in an invalid state
    Invalid,
}

impl Database {
    /// Creates a CursorImpl, wired to the WAL and lock manager when the
    /// environment has them.
    fn make_cursor(&self) -> CursorImpl {
        let env = self.env_impl.lock();
        let lock_manager = Arc::clone(env.get_lock_manager());
        match env.get_log_manager() {
            Some(lm) => {
                CursorImpl::with_log_manager(Arc::clone(&self.db_impl), 0, lm)
                    .with_lock_manager(lock_manager)
            }
            None => CursorImpl::new(Arc::clone(&self.db_impl), 0)
                .with_lock_manager(lock_manager),
        }
    }

    /// Creates a CursorImpl wired to the given transaction for write-lock tracking.
    ///
    /// Behaves like `make_cursor()` but additionally calls `.with_txn()` so
    /// that write operations acquire locks via the transaction's `Txn` and
    /// record abort before-images in `WriteLockInfo`.
    ///
    /// Port of `DatabaseImpl.openCursor(txn, config)` in JE which passes the
    /// transaction's `Locker` to the new `CursorImpl`.
    fn make_cursor_for_txn(&self, txn: &Transaction) -> CursorImpl {
        let cursor = self.make_cursor();
        if let Some(inner) = txn.get_inner_txn() {
            cursor.with_txn(inner)
        } else {
            cursor
        }
    }

    /// Auto-commit flush: when `txn` is `None` (auto-commit mode), flush and
    /// fsync the log before returning to the caller.
    ///
    /// Port of JE `Database.put/delete` auto-commit path: a non-transactional
    /// mutation is wrapped in an implicit `AutoTxn` that commits with
    /// `CommitSync` durability (fsync) before the method returns, giving the
    /// same durability guarantee as an explicit committed transaction.
    fn auto_commit_sync(&self, txn: Option<&Transaction>) -> Result<()> {
        if txn.is_some() {
            return Ok(()); // explicit txn handles its own commit/fsync
        }
        if let Some(lm) = self.env_impl.lock().get_log_manager() {
            lm.flush_sync()
                .map_err(|e| NoxuError::OperationNotAllowed(e.to_string()))?;
        }
        Ok(())
    }

    /// Creates a new database handle.
    ///
    /// Internal constructor called by Environment.
    pub(crate) fn new(
        name: String,
        id: u64,
        config: DatabaseConfig,
        db_impl: Arc<RwLock<DatabaseImpl>>,
        env_impl: Arc<Mutex<EnvironmentImpl>>,
    ) -> Self {
        Database {
            name,
            id,
            config,
            db_impl,
            env_impl,
            open: AtomicBool::new(true),
        }
    }

    /// Retrieves a record by key.
    ///
    /// Port of `Database.get(Transaction, DatabaseEntry, DatabaseEntry, LockMode)`.
    ///
    /// # Arguments
    /// * `txn` - Optional transaction handle (currently ignored)
    /// * `key` - The search key
    /// * `data` - Output parameter to receive the data
    ///
    /// # Returns
    /// `OperationStatus::Success` if found, `OperationStatus::NotFound` otherwise
    ///
    /// # Errors
    /// Returns an error if the database is closed
    pub fn get(
        &self,
        _txn: Option<&Transaction>,
        key: &DatabaseEntry,
        data: &mut DatabaseEntry,
    ) -> Result<OperationStatus> {
        self.check_open()?;

        let key_bytes = match key.get_data() {
            Some(k) => k,
            None => return Ok(OperationStatus::NotFound),
        };

        let mut cursor = CursorImpl::new(Arc::clone(&self.db_impl), 0);
        match cursor
            .search(key_bytes, None, SearchMode::Set)
            .map_err(|e| NoxuError::OperationNotAllowed(e.to_string()))?
        {
            noxu_dbi::OperationStatus::Success => {
                let (_, value) = cursor
                    .get_current()
                    .map_err(|e| NoxuError::OperationNotAllowed(e.to_string()))?;
                // Partial get: return only the requested slice.
                // Port of JE DatabaseEntry partial-read logic.
                if data.is_partial() {
                    let off = data.get_partial_offset();
                    let len = data.get_partial_length();
                    let end = (off + len).min(value.len());
                    let slice = if off < value.len() { &value[off..end] } else { &[] };
                    data.set_data(slice);
                } else {
                    data.set_data(&value);
                }
                Ok(OperationStatus::Success)
            }
            _ => Ok(OperationStatus::NotFound),
        }
    }

    /// Inserts or updates a record.
    ///
    /// Port of `Database.put(Transaction, DatabaseEntry, DatabaseEntry)`.
    ///
    /// # Arguments
    /// * `txn` - Optional transaction handle (currently ignored)
    /// * `key` - The key to insert/update
    /// * `data` - The data to store
    ///
    /// # Returns
    /// `OperationStatus::Success` on success
    ///
    /// # Errors
    /// Returns an error if the database is closed or read-only
    pub fn put(
        &self,
        txn: Option<&Transaction>,
        key: &DatabaseEntry,
        data: &DatabaseEntry,
    ) -> Result<OperationStatus> {
        self.check_open()?;
        self.check_writable()?;

        let key_bytes = key.get_data().unwrap_or(&[]);

        // Partial put: read-modify-write using the partial offset/length.
        // Port of JE LN.combinePuts() — existing bytes outside [offset..offset+length]
        // are preserved; only the specified range is replaced with new data.
        let write_bytes: Vec<u8>;
        let data_bytes: &[u8] = if data.is_partial() {
            let new_bytes = data.get_data().unwrap_or(&[]);
            let off = data.get_partial_offset();
            let len = data.get_partial_length();
            // Fetch the existing record to splice into.
            let existing = {
                let mut tmp_entry = DatabaseEntry::new();
                let mut tmp_cursor = self.make_cursor();
                match tmp_cursor.search(key_bytes, None, noxu_dbi::SearchMode::Set)
                    .map_err(|e| NoxuError::OperationNotAllowed(e.to_string()))?
                {
                    noxu_dbi::OperationStatus::Success => {
                        let (_, v) = tmp_cursor.get_current()
                            .map_err(|e| NoxuError::OperationNotAllowed(e.to_string()))?;
                        tmp_entry.set_data(&v);
                        tmp_entry.get_data().unwrap_or(&[]).to_vec()
                    }
                    _ => vec![0u8; off + len],
                }
            };
            let total_len = (off + len).max(existing.len());
            let mut patched = existing;
            patched.resize(total_len, 0);
            let copy_len = new_bytes.len().min(len);
            patched[off..off + copy_len].copy_from_slice(&new_bytes[..copy_len]);
            write_bytes = patched;
            &write_bytes
        } else {
            data.get_data().unwrap_or(&[])
        };

        let mut cursor = match txn {
            Some(t) => self.make_cursor_for_txn(t),
            None => self.make_cursor(),
        };
        cursor
            .put(key_bytes, data_bytes, PutMode::Overwrite)
            .map_err(|e| NoxuError::OperationNotAllowed(e.to_string()))?;

        // Auto-commit: fsync before returning (port of JE AutoTxn CommitSync).
        self.auto_commit_sync(txn)?;

        Ok(OperationStatus::Success)
    }

    /// Inserts a record, failing if the key already exists.
    ///
    /// Port of `Database.putNoOverwrite(Transaction, DatabaseEntry, DatabaseEntry)`.
    ///
    /// # Arguments
    /// * `txn` - Optional transaction handle (currently ignored)
    /// * `key` - The key to insert
    /// * `data` - The data to store
    ///
    /// # Returns
    /// `OperationStatus::Success` if inserted, `OperationStatus::KeyExists` if key already exists
    ///
    /// # Errors
    /// Returns an error if the database is closed or read-only
    pub fn put_no_overwrite(
        &self,
        txn: Option<&Transaction>,
        key: &DatabaseEntry,
        data: &DatabaseEntry,
    ) -> Result<OperationStatus> {
        self.check_open()?;
        self.check_writable()?;

        let key_bytes = key.get_data().unwrap_or(&[]);
        let data_bytes = data.get_data().unwrap_or(&[]);

        let mut cursor = match txn {
            Some(t) => self.make_cursor_for_txn(t),
            None => self.make_cursor(),
        };
        let status = match cursor
            .put(key_bytes, data_bytes, PutMode::NoOverwrite)
            .map_err(|e| NoxuError::OperationNotAllowed(e.to_string()))?
        {
            noxu_dbi::OperationStatus::KeyExist => OperationStatus::KeyExists,
            _ => OperationStatus::Success,
        };
        // Auto-commit: fsync before returning (port of JE AutoTxn CommitSync).
        self.auto_commit_sync(txn)?;
        Ok(status)
    }

    /// Deletes a record by key.
    ///
    /// Port of `Database.delete(Transaction, DatabaseEntry)`.
    ///
    /// # Arguments
    /// * `txn` - Optional transaction handle (currently ignored)
    /// * `key` - The key to delete
    ///
    /// # Returns
    /// `OperationStatus::Success` if deleted, `OperationStatus::NotFound` if key didn't exist
    ///
    /// # Errors
    /// Returns an error if the database is closed or read-only
    pub fn delete(
        &self,
        txn: Option<&Transaction>,
        key: &DatabaseEntry,
    ) -> Result<OperationStatus> {
        self.check_open()?;
        self.check_writable()?;

        let key_bytes = match key.get_data() {
            Some(k) => k,
            None => return Ok(OperationStatus::NotFound),
        };

        let mut cursor = match txn {
            Some(t) => self.make_cursor_for_txn(t),
            None => self.make_cursor(),
        };
        // First search to position the cursor
        let status = match cursor
            .search(key_bytes, None, SearchMode::Set)
            .map_err(|e| NoxuError::OperationNotAllowed(e.to_string()))?
        {
            noxu_dbi::OperationStatus::Success => {
                cursor
                    .delete()
                    .map_err(|e| NoxuError::OperationNotAllowed(e.to_string()))?;
                OperationStatus::Success
            }
            _ => OperationStatus::NotFound,
        };
        // Auto-commit: fsync before returning (port of JE AutoTxn CommitSync).
        self.auto_commit_sync(txn)?;
        Ok(status)
    }

    /// Opens a cursor for iterating over database records.
    ///
    /// Port of `Database.openCursor(Transaction, CursorConfig)`.
    ///
    /// # Arguments
    /// * `txn` - Optional transaction handle (currently ignored)
    /// * `config` - Optional cursor configuration
    ///
    /// # Returns
    /// A new cursor handle
    ///
    /// # Errors
    /// Returns an error if the database is closed
    pub fn open_cursor(
        &self,
        _txn: Option<&Transaction>,
        config: Option<&CursorConfig>,
    ) -> Result<Cursor> {
        self.check_open()?;

        let read_only = config.map(|c| c.read_uncommitted).unwrap_or(false)
            || self.config.read_only;

        let cursor_impl = if read_only {
            CursorImpl::new(Arc::clone(&self.db_impl), 0)
        } else {
            self.make_cursor()
        };

        Ok(Cursor::from_impl(cursor_impl, read_only))
    }

    /// Opens (and optionally creates) a sequence backed by this database.
    ///
    /// Port of `Database.openSequence(Transaction, DatabaseEntry, SequenceConfig)`.
    ///
    /// # Arguments
    /// * `key`    - The database key under which the sequence record is stored.
    /// * `config` - Sequence configuration (use `SequenceConfig::new()` for defaults).
    ///
    /// # Errors
    /// Returns an error if the database is closed, the config is invalid, or
    /// `allow_create` is false and the sequence does not exist.
    pub fn open_sequence<'db>(
        &'db self,
        key: &DatabaseEntry,
        config: SequenceConfig,
    ) -> Result<Sequence<'db>> {
        self.check_open()?;
        Sequence::open(self, key, config)
    }

    /// Closes the database handle.
    ///
    /// Port of `Database.close()`.
    ///
    /// # Errors
    /// Returns an error if the database is already closed
    pub fn close(&self) -> Result<()> {
        if !self.open.load(Ordering::Acquire) {
            return Err(NoxuError::DatabaseClosed);
        }

        self.open.store(false, Ordering::Release);
        let _ = self.env_impl.lock().close_database(noxu_dbi::DatabaseId::new(self.id as i64));
        Ok(())
    }

    /// Returns the database name.
    ///
    /// Port of `Database.getDatabaseName()`.
    pub fn get_database_name(&self) -> &str {
        &self.name
    }

    /// Returns the database configuration.
    ///
    /// Port of `Database.getConfig()`.
    pub fn get_config(&self) -> &DatabaseConfig {
        &self.config
    }

    /// Returns an approximate count of records in the database.
    ///
    /// Port of `Database.count()`.
    ///
    /// # Errors
    /// Returns an error if the database is closed
    pub fn count(&self) -> Result<u64> {
        self.check_open()?;

        // Count by scanning via cursor: get_first then next until exhausted.
        // This is O(n) but correct for the current tree implementation.
        let mut cursor = CursorImpl::new(Arc::clone(&self.db_impl), 0);
        let first_status = cursor
            .get_first()
            .map_err(|e| NoxuError::OperationNotAllowed(e.to_string()))?;

        if first_status != noxu_dbi::OperationStatus::Success {
            return Ok(0);
        }

        let mut count = 1u64;
        loop {
            let status = cursor
                .retrieve_next(GetMode::Next)
                .map_err(|e| NoxuError::OperationNotAllowed(e.to_string()))?;
            if status != noxu_dbi::OperationStatus::Success {
                break;
            }
            count += 1;
        }

        Ok(count)
    }

    /// Returns all records as `(key_bytes, data_bytes)` pairs in key order.
    ///
    /// This is a helper for schema evolution: the public `Cursor` interface
    /// does not expose key bytes during iteration, so this method uses the
    /// lower-level `CursorImpl` directly to collect both halves of every
    /// record in a single pass.
    ///
    /// # Errors
    /// Returns an error if the database is closed or a cursor operation fails.
    pub fn scan_all_kv(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.check_open()?;

        let mut cursor = CursorImpl::new(Arc::clone(&self.db_impl), 0);
        let first_status = cursor
            .get_first()
            .map_err(|e| NoxuError::OperationNotAllowed(e.to_string()))?;

        if first_status != noxu_dbi::OperationStatus::Success {
            return Ok(Vec::new());
        }

        let mut records = Vec::new();
        loop {
            let (k, v) = cursor
                .get_current()
                .map_err(|e| NoxuError::OperationNotAllowed(e.to_string()))?;
            records.push((k, v));

            let status = cursor
                .retrieve_next(GetMode::Next)
                .map_err(|e| NoxuError::OperationNotAllowed(e.to_string()))?;
            if status != noxu_dbi::OperationStatus::Success {
                break;
            }
        }

        Ok(records)
    }

    /// Returns whether the database handle is valid.
    ///
    /// Port of `Database.isValid()`.
    pub fn is_valid(&self) -> bool {
        self.open.load(Ordering::Acquire)
    }

    /// Returns the current state of the database handle.
    pub fn state(&self) -> DbState {
        if self.open.load(Ordering::Acquire) {
            DbState::Open
        } else {
            DbState::Closed
        }
    }

    /// Checks if the database is open, returns an error if not.
    fn check_open(&self) -> Result<()> {
        if !self.open.load(Ordering::Acquire) {
            return Err(NoxuError::DatabaseClosed);
        }
        Ok(())
    }

    /// Checks if the database is writable, returns an error if not.
    fn check_writable(&self) -> Result<()> {
        if self.config.read_only {
            return Err(NoxuError::ReadOnly);
        }
        Ok(())
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        // Best effort close on drop
        let _ = self.close();
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::environment::Environment;
    use crate::environment_config::EnvironmentConfig;
    use tempfile::TempDir;

    fn temp_env_and_db() -> (TempDir, Environment, Database) {
        let temp_dir = TempDir::new().unwrap();
        let env_config = EnvironmentConfig::new(temp_dir.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true);
        let env = Environment::open(env_config).unwrap();

        let db_config = DatabaseConfig::new().with_allow_create(true);
        let db = env.open_database(None, "testdb", &db_config).unwrap();

        (temp_dir, env, db)
    }

    #[test]
    fn test_database_name() {
        let (_temp_dir, _env, db) = temp_env_and_db();
        assert_eq!(db.get_database_name(), "testdb");
    }

    #[test]
    fn test_put_and_get() {
        let (_temp_dir, _env, db) = temp_env_and_db();

        let key = DatabaseEntry::from_bytes(b"key1");
        let value = DatabaseEntry::from_bytes(b"value1");

        let result = db.put(None, &key, &value).unwrap();
        assert_eq!(result, OperationStatus::Success);

        let mut retrieved = DatabaseEntry::new();
        let result = db.get(None, &key, &mut retrieved).unwrap();
        assert_eq!(result, OperationStatus::Success);
        assert_eq!(retrieved.get_data().unwrap(), b"value1");
    }

    #[test]
    fn test_get_nonexistent() {
        let (_temp_dir, _env, db) = temp_env_and_db();

        let key = DatabaseEntry::from_bytes(b"nonexistent");
        let mut data = DatabaseEntry::new();

        let result = db.get(None, &key, &mut data).unwrap();
        assert_eq!(result, OperationStatus::NotFound);
    }

    #[test]
    fn test_put_updates_existing() {
        let (_temp_dir, _env, db) = temp_env_and_db();

        let key = DatabaseEntry::from_bytes(b"key1");
        let value1 = DatabaseEntry::from_bytes(b"value1");
        let value2 = DatabaseEntry::from_bytes(b"value2");

        db.put(None, &key, &value1).unwrap();
        db.put(None, &key, &value2).unwrap();

        let mut retrieved = DatabaseEntry::new();
        db.get(None, &key, &mut retrieved).unwrap();
        assert_eq!(retrieved.get_data().unwrap(), b"value2");
    }

    #[test]
    fn test_put_no_overwrite_success() {
        let (_temp_dir, _env, db) = temp_env_and_db();

        let key = DatabaseEntry::from_bytes(b"key1");
        let value = DatabaseEntry::from_bytes(b"value1");

        let result = db.put_no_overwrite(None, &key, &value).unwrap();
        assert_eq!(result, OperationStatus::Success);
    }

    #[test]
    fn test_put_no_overwrite_key_exists() {
        let (_temp_dir, _env, db) = temp_env_and_db();

        let key = DatabaseEntry::from_bytes(b"key1");
        let value1 = DatabaseEntry::from_bytes(b"value1");
        let value2 = DatabaseEntry::from_bytes(b"value2");

        db.put(None, &key, &value1).unwrap();
        let result = db.put_no_overwrite(None, &key, &value2).unwrap();
        assert_eq!(result, OperationStatus::KeyExists);

        // Verify original value is unchanged
        let mut retrieved = DatabaseEntry::new();
        db.get(None, &key, &mut retrieved).unwrap();
        assert_eq!(retrieved.get_data().unwrap(), b"value1");
    }

    #[test]
    fn test_delete() {
        let (_temp_dir, _env, db) = temp_env_and_db();

        let key = DatabaseEntry::from_bytes(b"key1");
        let value = DatabaseEntry::from_bytes(b"value1");

        db.put(None, &key, &value).unwrap();
        let result = db.delete(None, &key).unwrap();
        assert_eq!(result, OperationStatus::Success);

        let mut retrieved = DatabaseEntry::new();
        let result = db.get(None, &key, &mut retrieved).unwrap();
        assert_eq!(result, OperationStatus::NotFound);
    }

    #[test]
    fn test_delete_nonexistent() {
        let (_temp_dir, _env, db) = temp_env_and_db();

        let key = DatabaseEntry::from_bytes(b"nonexistent");
        let result = db.delete(None, &key).unwrap();
        assert_eq!(result, OperationStatus::NotFound);
    }

    #[test]
    fn test_count() {
        let (_temp_dir, _env, db) = temp_env_and_db();

        assert_eq!(db.count().unwrap(), 0);

        let key1 = DatabaseEntry::from_bytes(b"key1");
        let value1 = DatabaseEntry::from_bytes(b"value1");
        db.put(None, &key1, &value1).unwrap();
        assert_eq!(db.count().unwrap(), 1);

        let key2 = DatabaseEntry::from_bytes(b"key2");
        let value2 = DatabaseEntry::from_bytes(b"value2");
        db.put(None, &key2, &value2).unwrap();
        assert_eq!(db.count().unwrap(), 2);

        db.delete(None, &key1).unwrap();
        assert_eq!(db.count().unwrap(), 1);
    }

    #[test]
    fn test_close() {
        let (_temp_dir, _env, db) = temp_env_and_db();
        assert!(db.is_valid());
        db.close().unwrap();
        assert!(!db.is_valid());
    }

    #[test]
    fn test_close_twice_fails() {
        let (_temp_dir, _env, db) = temp_env_and_db();
        db.close().unwrap();
        let result = db.close();
        assert!(result.is_err());
    }

    #[test]
    fn test_operations_on_closed_database_fail() {
        let (_temp_dir, _env, db) = temp_env_and_db();
        db.close().unwrap();

        let key = DatabaseEntry::from_bytes(b"key1");
        let value = DatabaseEntry::from_bytes(b"value1");
        let mut data = DatabaseEntry::new();

        assert!(db.get(None, &key, &mut data).is_err());
        assert!(db.put(None, &key, &value).is_err());
        assert!(db.put_no_overwrite(None, &key, &value).is_err());
        assert!(db.delete(None, &key).is_err());
        assert!(db.count().is_err());
        assert!(db.open_cursor(None, None).is_err());
    }

    #[test]
    fn test_state() {
        let (_temp_dir, _env, db) = temp_env_and_db();
        assert_eq!(db.state(), DbState::Open);
        db.close().unwrap();
        assert_eq!(db.state(), DbState::Closed);
    }

    #[test]
    fn test_read_only_database() {
        let temp_dir = TempDir::new().unwrap();
        let env_config = EnvironmentConfig::new(temp_dir.path().to_path_buf())
            .with_allow_create(true);
        let env = Environment::open(env_config).unwrap();

        let db_config =
            DatabaseConfig::new().with_allow_create(true).with_read_only(true);
        let db = env.open_database(None, "readonly_db", &db_config).unwrap();

        let key = DatabaseEntry::from_bytes(b"key1");
        let value = DatabaseEntry::from_bytes(b"value1");

        // Write operations should fail
        assert!(db.put(None, &key, &value).is_err());
        assert!(db.put_no_overwrite(None, &key, &value).is_err());
        assert!(db.delete(None, &key).is_err());
    }

    #[test]
    fn test_multiple_databases() {
        let temp_dir = TempDir::new().unwrap();
        let env_config = EnvironmentConfig::new(temp_dir.path().to_path_buf())
            .with_allow_create(true);
        let env = Environment::open(env_config).unwrap();

        let db_config = DatabaseConfig::new().with_allow_create(true);
        let db1 = env.open_database(None, "db1", &db_config).unwrap();
        let db2 = env.open_database(None, "db2", &db_config).unwrap();

        let key = DatabaseEntry::from_bytes(b"key1");
        let value1 = DatabaseEntry::from_bytes(b"value1");
        let value2 = DatabaseEntry::from_bytes(b"value2");

        db1.put(None, &key, &value1).unwrap();
        db2.put(None, &key, &value2).unwrap();

        let mut retrieved1 = DatabaseEntry::new();
        let mut retrieved2 = DatabaseEntry::new();

        db1.get(None, &key, &mut retrieved1).unwrap();
        db2.get(None, &key, &mut retrieved2).unwrap();

        assert_eq!(retrieved1.get_data().unwrap(), b"value1");
        assert_eq!(retrieved2.get_data().unwrap(), b"value2");
    }

    #[test]
    fn test_empty_keys_and_values() {
        let (_temp_dir, _env, db) = temp_env_and_db();

        let empty_key = DatabaseEntry::from_bytes(b"");
        let empty_value = DatabaseEntry::from_bytes(b"");

        let result = db.put(None, &empty_key, &empty_value).unwrap();
        assert_eq!(result, OperationStatus::Success);

        let mut retrieved = DatabaseEntry::new();
        let result = db.get(None, &empty_key, &mut retrieved).unwrap();
        assert_eq!(result, OperationStatus::Success);
        assert_eq!(retrieved.get_data().unwrap(), b"");
    }

    #[test]
    fn test_large_keys_and_values() {
        let (_temp_dir, _env, db) = temp_env_and_db();

        let large_key = DatabaseEntry::from_bytes(&vec![b'k'; 1000]);
        let large_value = DatabaseEntry::from_bytes(&vec![b'v'; 10000]);

        db.put(None, &large_key, &large_value).unwrap();

        let mut retrieved = DatabaseEntry::new();
        db.get(None, &large_key, &mut retrieved).unwrap();
        assert_eq!(retrieved.get_data().unwrap().len(), 10000);
        assert!(retrieved.get_data().unwrap().iter().all(|&b| b == b'v'));
    }

    #[test]
    fn test_binary_keys_and_values() {
        let (_temp_dir, _env, db) = temp_env_and_db();

        let binary_key = DatabaseEntry::from_bytes(&[0u8, 1, 2, 255, 254, 253]);
        let binary_value = DatabaseEntry::from_bytes(&[255u8, 0, 128, 64, 32]);

        db.put(None, &binary_key, &binary_value).unwrap();

        let mut retrieved = DatabaseEntry::new();
        db.get(None, &binary_key, &mut retrieved).unwrap();
        assert_eq!(retrieved.get_data().unwrap(), &[255u8, 0, 128, 64, 32]);
    }

    #[test]
    fn test_scan_all_kv_empty() {
        let (_temp_dir, _env, db) = temp_env_and_db();
        let kv = db.scan_all_kv().unwrap();
        assert!(kv.is_empty());
    }

    #[test]
    fn test_scan_all_kv_returns_records() {
        let (_temp_dir, _env, db) = temp_env_and_db();
        db.put(None, &DatabaseEntry::from_vec(vec![1]), &DatabaseEntry::from_vec(vec![10])).unwrap();
        db.put(None, &DatabaseEntry::from_vec(vec![2]), &DatabaseEntry::from_vec(vec![20])).unwrap();
        let kv = db.scan_all_kv().unwrap();
        assert_eq!(kv.len(), 2);
    }

    #[test]
    fn test_scan_all_kv_then_delete() {
        let (_temp_dir, _env, db) = temp_env_and_db();
        db.put(None, &DatabaseEntry::from_vec(vec![1]), &DatabaseEntry::from_vec(vec![10])).unwrap();
        db.put(None, &DatabaseEntry::from_vec(vec![2]), &DatabaseEntry::from_vec(vec![20])).unwrap();

        let kv = db.scan_all_kv().unwrap();
        assert_eq!(kv.len(), 2);

        for (k, _v) in &kv {
            let status = db.delete(None, &DatabaseEntry::from_vec(k.clone())).unwrap();
            assert_eq!(
                status,
                OperationStatus::Success,
                "delete failed for key {:?}",
                k
            );
        }

        let count = db.count().unwrap();
        assert_eq!(count, 0, "expected 0 records after deletes, got {}", count);
    }

    #[test]
    fn test_scan_all_kv_then_delete_u64_be_keys() {
        // Simulate the exact pattern used in EntityStore::evolve: big-endian u64 keys.
        let (_temp_dir, _env, db) = temp_env_and_db();
        for id in [1u64, 2u64] {
            let key_bytes = id.to_be_bytes().to_vec();
            let val_bytes = format!("user{}", id).into_bytes();
            db.put(
                None,
                &DatabaseEntry::from_vec(key_bytes),
                &DatabaseEntry::from_vec(val_bytes),
            )
            .unwrap();
        }
        assert_eq!(db.count().unwrap(), 2);

        let records = db.scan_all_kv().unwrap();
        assert_eq!(records.len(), 2);

        for (k, _v) in records {
            let status = db.delete(None, &DatabaseEntry::from_vec(k.clone())).unwrap();
            assert_eq!(
                status,
                OperationStatus::Success,
                "delete failed for u64 key {:?}",
                k
            );
        }
        assert_eq!(db.count().unwrap(), 0);
    }

    // ========================================================================
    // Additional branch-coverage tests
    // ========================================================================

    /// get() with a None-data DatabaseEntry returns NotFound.
    #[test]
    fn test_get_with_none_key_data_returns_not_found() {
        let (_temp_dir, _env, db) = temp_env_and_db();
        let key_none = DatabaseEntry::new(); // no data set
        let mut data = DatabaseEntry::new();

        let result = db.get(None, &key_none, &mut data).unwrap();
        assert_eq!(result, OperationStatus::NotFound);
    }

    /// delete() with a None-data DatabaseEntry returns NotFound.
    #[test]
    fn test_delete_with_none_key_data_returns_not_found() {
        let (_temp_dir, _env, db) = temp_env_and_db();
        let key_none = DatabaseEntry::new();

        let result = db.delete(None, &key_none).unwrap();
        assert_eq!(result, OperationStatus::NotFound);
    }

    /// open_cursor() with a CursorConfig that has read_uncommitted=true makes
    /// the cursor read-only.
    #[test]
    fn test_open_cursor_read_uncommitted_config_makes_read_only() {
        use crate::cursor_config::CursorConfig;
        let (_temp_dir, _env, db) = temp_env_and_db();

        let config = CursorConfig::new().with_read_uncommitted(true);
        let cursor = db.open_cursor(None, Some(&config)).unwrap();
        assert!(cursor.is_read_only());
    }

    /// open_cursor() with no config and a non-read-only database produces a
    /// writable cursor.
    #[test]
    fn test_open_cursor_no_config_writable_db_is_writable() {
        let (_temp_dir, _env, db) = temp_env_and_db();
        let cursor = db.open_cursor(None, None).unwrap();
        assert!(!cursor.is_read_only());
    }

    /// scan_all_kv() on a closed database returns an error.
    #[test]
    fn test_scan_all_kv_on_closed_database_fails() {
        let (_temp_dir, _env, db) = temp_env_and_db();
        db.close().unwrap();
        let result = db.scan_all_kv();
        assert!(result.is_err());
    }

    /// put_no_overwrite() on a read-only database returns an error.
    #[test]
    fn test_put_no_overwrite_on_read_only_database_fails() {
        let temp_dir = TempDir::new().unwrap();
        let env_config = EnvironmentConfig::new(temp_dir.path().to_path_buf())
            .with_allow_create(true);
        let env = Environment::open(env_config).unwrap();

        let db_config = DatabaseConfig::new().with_allow_create(true).with_read_only(true);
        let db = env.open_database(None, "ro_db", &db_config).unwrap();

        let key = DatabaseEntry::from_bytes(b"k");
        let val = DatabaseEntry::from_bytes(b"v");
        let result = db.put_no_overwrite(None, &key, &val);
        assert!(result.is_err());
    }

    // =====================================================================
    // cursor-failure map_err coverage: use the test hook in noxu-dbi to
    // force cursor operations to return Err, exercising the map_err closures
    // in Database::get / put / put_no_overwrite / delete / count / scan_all_kv.
    // =====================================================================

    /// Covers the map_err closure on `cursor.search(...)` inside `get()`.
    #[test]
    fn test_get_search_map_err_via_hook() {
        let (_tmp, _env, db) = temp_env_and_db();
        noxu_dbi::set_cursor_fail_after(1); // fail on the 1st check_state (search)
        let key = DatabaseEntry::from_bytes(b"any");
        let mut data = DatabaseEntry::new();
        let result = db.get(None, &key, &mut data);
        noxu_dbi::clear_cursor_fail_flag();
        assert!(result.is_err());
    }

    /// Covers the map_err closure on `cursor.get_current()` inside `get()`.
    #[test]
    fn test_get_get_current_map_err_via_hook() {
        let (_tmp, _env, db) = temp_env_and_db();
        // Insert a key so search can succeed.
        db.put(None, &DatabaseEntry::from_bytes(b"k"), &DatabaseEntry::from_bytes(b"v")).unwrap();
        // fail on the 2nd check (check_initialized inside get_current).
        noxu_dbi::set_cursor_fail_after(2);
        let key = DatabaseEntry::from_bytes(b"k");
        let mut data = DatabaseEntry::new();
        let result = db.get(None, &key, &mut data);
        noxu_dbi::clear_cursor_fail_flag();
        assert!(result.is_err());
    }

    /// Covers the map_err closure on `cursor.put(...)` inside `put()`.
    #[test]
    fn test_put_map_err_via_hook() {
        let (_tmp, _env, db) = temp_env_and_db();
        noxu_dbi::set_cursor_fail_after(1);
        let key = DatabaseEntry::from_bytes(b"k");
        let val = DatabaseEntry::from_bytes(b"v");
        let result = db.put(None, &key, &val);
        noxu_dbi::clear_cursor_fail_flag();
        assert!(result.is_err());
    }

    /// Covers the map_err closure on `cursor.put(...)` inside `put_no_overwrite()`.
    #[test]
    fn test_put_no_overwrite_map_err_via_hook() {
        let (_tmp, _env, db) = temp_env_and_db();
        noxu_dbi::set_cursor_fail_after(1);
        let key = DatabaseEntry::from_bytes(b"k");
        let val = DatabaseEntry::from_bytes(b"v");
        let result = db.put_no_overwrite(None, &key, &val);
        noxu_dbi::clear_cursor_fail_flag();
        assert!(result.is_err());
    }

    /// Covers the map_err closure on `cursor.search(...)` inside `delete()`.
    #[test]
    fn test_delete_search_map_err_via_hook() {
        let (_tmp, _env, db) = temp_env_and_db();
        noxu_dbi::set_cursor_fail_after(1);
        let key = DatabaseEntry::from_bytes(b"k");
        let result = db.delete(None, &key);
        noxu_dbi::clear_cursor_fail_flag();
        assert!(result.is_err());
    }

    /// Covers the map_err closure on `cursor.delete()` inside `delete()`.
    #[test]
    fn test_delete_delete_map_err_via_hook() {
        let (_tmp, _env, db) = temp_env_and_db();
        db.put(None, &DatabaseEntry::from_bytes(b"k"), &DatabaseEntry::from_bytes(b"v")).unwrap();
        // fail on the 2nd check_state (the delete() call, after search succeeds).
        noxu_dbi::set_cursor_fail_after(2);
        let key = DatabaseEntry::from_bytes(b"k");
        let result = db.delete(None, &key);
        noxu_dbi::clear_cursor_fail_flag();
        assert!(result.is_err());
    }

    /// Covers the map_err closure on `cursor.get_first()` inside `count()`.
    #[test]
    fn test_count_get_first_map_err_via_hook() {
        let (_tmp, _env, db) = temp_env_and_db();
        noxu_dbi::set_cursor_fail_after(1);
        let result = db.count();
        noxu_dbi::clear_cursor_fail_flag();
        assert!(result.is_err());
    }

    /// Covers the map_err closure on `cursor.retrieve_next(...)` inside `count()`.
    #[test]
    fn test_count_retrieve_next_map_err_via_hook() {
        let (_tmp, _env, db) = temp_env_and_db();
        db.put(None, &DatabaseEntry::from_bytes(b"k"), &DatabaseEntry::from_bytes(b"v")).unwrap();
        // fail on the 2nd check_state (retrieve_next, after get_first succeeds).
        noxu_dbi::set_cursor_fail_after(2);
        let result = db.count();
        noxu_dbi::clear_cursor_fail_flag();
        assert!(result.is_err());
    }

    /// Covers the map_err closure on `cursor.get_first()` inside `scan_all_kv()`.
    #[test]
    fn test_scan_all_kv_get_first_map_err_via_hook() {
        let (_tmp, _env, db) = temp_env_and_db();
        noxu_dbi::set_cursor_fail_after(1);
        let result = db.scan_all_kv();
        noxu_dbi::clear_cursor_fail_flag();
        assert!(result.is_err());
    }

    /// Covers the map_err closure on `cursor.get_current()` inside `scan_all_kv()`.
    #[test]
    fn test_scan_all_kv_get_current_map_err_via_hook() {
        let (_tmp, _env, db) = temp_env_and_db();
        db.put(None, &DatabaseEntry::from_bytes(b"k"), &DatabaseEntry::from_bytes(b"v")).unwrap();
        // fail on the 2nd check (check_initialized inside get_current, after get_first succeeds).
        noxu_dbi::set_cursor_fail_after(2);
        let result = db.scan_all_kv();
        noxu_dbi::clear_cursor_fail_flag();
        assert!(result.is_err());
    }

    /// Covers the map_err closure on `cursor.retrieve_next(...)` inside `scan_all_kv()`.
    #[test]
    fn test_scan_all_kv_retrieve_next_map_err_via_hook() {
        let (_tmp, _env, db) = temp_env_and_db();
        db.put(None, &DatabaseEntry::from_bytes(b"k"), &DatabaseEntry::from_bytes(b"v")).unwrap();
        // fail on the 3rd check (retrieve_next, after get_first and get_current succeed).
        noxu_dbi::set_cursor_fail_after(3);
        let result = db.scan_all_kv();
        noxu_dbi::clear_cursor_fail_flag();
        assert!(result.is_err());
    }

}
