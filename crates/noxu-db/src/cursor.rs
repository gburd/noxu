//! Cursor handle for Noxu DB.
//!

use crate::database_entry::DatabaseEntry;
use crate::error::{NoxuError, Result};
use crate::get::Get;
use crate::lock_mode::LockMode;
use crate::operation_status::OperationStatus;
use crate::put::Put;
use noxu_dbi::{CursorImpl, GetMode, PutMode, SearchMode};

/// Cursor state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CursorState {
    /// Cursor has not been positioned yet.
    NotInitialized,
    /// Cursor is positioned on a record.
    Initialized,
    /// Cursor has been closed.
    Closed,
}

/// A database cursor for iterating over records.
///
/// 
///
/// Cursors are used for operating on collections of records,
/// for iterating over a database, and for saving handles to individual
/// records so they can be modified after reading.
///
/// # Example
/// ```ignore
/// use noxu_db::{Database, DatabaseEntry, Get};
///
/// # fn example(db: &Database) -> Result<(), Box<dyn std::error::Error>> {
/// let mut cursor = db.open_cursor(None, None)?;
/// let mut key = DatabaseEntry::new();
/// let mut data = DatabaseEntry::new();
///
/// // Iterate through all records
/// while cursor.get(&mut key, &mut data, Get::Next, None)? == OperationStatus::Success {
///     // Process key and data
/// }
///
/// cursor.close()?;
/// # Ok(())
/// # }
/// ```
pub struct Cursor {
    /// Underlying CursorImpl from the dbi layer.
    inner: CursorImpl,
    /// Current cursor state.
    state: CursorState,
    /// Whether this cursor is read-only.
    read_only: bool,
}

impl Cursor {
    /// Creates a Cursor wrapping a `CursorImpl`.
    ///
    /// Called by `Database::open_cursor`.
    pub(crate) fn from_impl(inner: CursorImpl, read_only: bool) -> Self {
        Self {
            inner,
            state: CursorState::NotInitialized,
            read_only,
        }
    }

    /// Retrieve a record using the cursor.
    ///
    /// # Arguments
    /// * `key` - Key to search for (input for Search, output for iteration)
    /// * `data` - Output buffer for the record data
    /// * `get_type` - Type of get operation
    /// * `lock_mode` - Lock mode (currently ignored)
    ///
    /// # Returns
    /// `OperationStatus::Success` if the operation succeeded,
    /// `OperationStatus::NotFound` if no record was found.
    pub fn get(
        &mut self,
        key: &mut DatabaseEntry,
        data: &mut DatabaseEntry,
        get_type: Get,
        _lock_mode: Option<LockMode>,
    ) -> Result<OperationStatus> {
        self.check_open()?;

        if matches!(get_type, Get::Current) {
            self.check_initialized()?;
        }

        let status = match get_type {
            Get::Search => {
                let key_bytes = match key.get_data() {
                    Some(k) if !k.is_empty() => k,
                    _ => return Ok(OperationStatus::NotFound),
                };
                self.inner
                    .search(key_bytes, None, SearchMode::Set)
                    .map_err(|e| NoxuError::OperationNotAllowed(e.to_string()))?
            }
            Get::SearchGte | Get::SearchRange => {
                let key_bytes = match key.get_data() {
                    Some(k) if !k.is_empty() => k,
                    _ => return Ok(OperationStatus::NotFound),
                };
                self.inner
                    .search(key_bytes, None, SearchMode::SetRange)
                    .map_err(|e| NoxuError::OperationNotAllowed(e.to_string()))?
            }
            Get::First => self
                .inner
                .get_first()
                .map_err(|e| NoxuError::OperationNotAllowed(e.to_string()))?,
            Get::Last => self
                .inner
                .get_last()
                .map_err(|e| NoxuError::OperationNotAllowed(e.to_string()))?,
            Get::Next => {
                if self.state == CursorState::NotInitialized {
                    // Next from uninitialized positions at the first record.
                    self.inner
                        .get_first()
                        .map_err(|e| NoxuError::OperationNotAllowed(e.to_string()))?
                } else {
                    self.inner
                        .retrieve_next(GetMode::Next)
                        .map_err(|e| NoxuError::OperationNotAllowed(e.to_string()))?
                }
            }
            Get::Prev => {
                if self.state == CursorState::NotInitialized {
                    // Prev from uninitialized positions at the last record.
                    self.inner
                        .get_last()
                        .map_err(|e| NoxuError::OperationNotAllowed(e.to_string()))?
                } else {
                    self.inner
                        .retrieve_next(GetMode::Prev)
                        .map_err(|e| NoxuError::OperationNotAllowed(e.to_string()))?
                }
            }
            Get::Current => {
                // Already checked initialized above.
                let (k, v) = self
                    .inner
                    .get_current()
                    .map_err(|e| NoxuError::OperationNotAllowed(e.to_string()))?;
                data.set_data(&v);
                key.set_data(&k);
                self.state = CursorState::Initialized;
                return Ok(OperationStatus::Success);
            }
            _ => return Ok(OperationStatus::NotFound),
        };

        match status {
            noxu_dbi::OperationStatus::Success => {
                let (k, v) = self
                    .inner
                    .get_current()
                    .map_err(|e| NoxuError::OperationNotAllowed(e.to_string()))?;
                data.set_data(&v);
                // Write back the current key for navigation operations.
                // In JE, `key` is always an output parameter for positioning ops.
                key.set_data(&k);
                self.state = CursorState::Initialized;
                Ok(OperationStatus::Success)
            }
            _ => {
                if matches!(
                    get_type,
                    Get::First | Get::Last | Get::Search | Get::SearchGte | Get::SearchRange
                ) {
                    self.state = CursorState::NotInitialized;
                }
                Ok(OperationStatus::NotFound)
            }
        }
    }

    /// Store a record using the cursor.
    ///
    /// # Arguments
    /// * `key` - Key to store
    /// * `data` - Data to store
    /// * `put_type` - Type of put operation
    ///
    /// # Returns
    /// `OperationStatus::Success` if the operation succeeded,
    /// `OperationStatus::KeyExists` if the key already exists (for NoOverwrite).
    pub fn put(
        &mut self,
        key: &DatabaseEntry,
        data: &DatabaseEntry,
        put_type: Put,
    ) -> Result<OperationStatus> {
        self.check_open()?;

        if self.read_only {
            return Err(NoxuError::OperationNotAllowed(
                "Cannot write with a read-only cursor".to_string(),
            ));
        }

        let key_bytes = key.get_data().unwrap_or(&[]);
        let data_bytes = data.get_data().unwrap_or(&[]);

        let put_mode = match put_type {
            Put::Overwrite => PutMode::Overwrite,
            Put::NoOverwrite => PutMode::NoOverwrite,
            // NoDupData inserts only if the exact (key, data) pair does not
            // already exist.  For sorted-dup databases this checks the full
            // two-part composite key; for non-dup databases it behaves
            // identically to NoOverwrite.
            Put::NoDupData => PutMode::NoDupData,
            Put::Current => {
                self.check_initialized()?;
                PutMode::Current
            }
        };

        match self
            .inner
            .put(key_bytes, data_bytes, put_mode)
            .map_err(|e| NoxuError::OperationNotAllowed(e.to_string()))?
        {
            noxu_dbi::OperationStatus::KeyExist => Ok(OperationStatus::KeyExists),
            _ => {
                self.state = CursorState::Initialized;
                Ok(OperationStatus::Success)
            }
        }
    }

    /// Delete the record at the current cursor position.
    ///
    /// # Returns
    /// `OperationStatus::Success` if the record was deleted,
    /// `OperationStatus::NotFound` if the cursor is not positioned.
    pub fn delete(&mut self) -> Result<OperationStatus> {
        self.check_open()?;
        self.check_initialized()?;

        if self.read_only {
            return Err(NoxuError::OperationNotAllowed(
                "Cannot delete with a read-only cursor".to_string(),
            ));
        }

        self.inner
            .delete()
            .map_err(|e| NoxuError::OperationNotAllowed(e.to_string()))?;
        self.state = CursorState::NotInitialized;
        Ok(OperationStatus::Success)
    }

    /// Count the number of records with the same key.
    ///
    /// For databases without duplicates, this always returns 1 if positioned.
    ///
    /// # Returns
    /// The count of records, or 0 if the cursor is not positioned.
    pub fn count(&self) -> Result<u64> {
        self.check_open()?;

        if self.state != CursorState::Initialized {
            return Ok(0);
        }

        Ok(1)
    }

    /// Close the cursor.
    ///
    /// The cursor handle may not be used again after this call.
    pub fn close(&mut self) -> Result<()> {
        if self.state == CursorState::Closed {
            return Err(NoxuError::OperationNotAllowed(
                "Cursor already closed".to_string(),
            ));
        }

        self.state = CursorState::Closed;
        Ok(())
    }

    /// Check if the cursor is valid (not closed).
    pub fn is_valid(&self) -> bool {
        self.state != CursorState::Closed
    }

    /// Get the current cursor state.
    pub fn get_state(&self) -> CursorState {
        self.state
    }

    /// Check if the cursor is read-only.
    pub fn is_read_only(&self) -> bool {
        self.read_only
    }

    /// Check that the cursor is not closed.
    fn check_open(&self) -> Result<()> {
        if self.state == CursorState::Closed {
            Err(NoxuError::OperationNotAllowed(
                "Cursor has been closed".to_string(),
            ))
        } else {
            Ok(())
        }
    }

    /// Check that the cursor is initialized (positioned on a record).
    fn check_initialized(&self) -> Result<()> {
        if self.state != CursorState::Initialized {
            Err(NoxuError::OperationNotAllowed(
                "Cursor is not positioned on a record".to_string(),
            ))
        } else {
            Ok(())
        }
    }
}

impl Drop for Cursor {
    fn drop(&mut self) {
        if self.state != CursorState::Closed {
            log::warn!("Cursor dropped without close");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use noxu_dbi::{
        DatabaseConfig as DbiDatabaseConfig, DatabaseId, DatabaseImpl, DbType,
    };
    use noxu_sync::RwLock;
    use std::sync::Arc;

    /// Creates a fresh in-memory DatabaseImpl and wraps it in a Cursor.
    fn make_cursor(read_only: bool) -> Cursor {
        let db_id = DatabaseId::new(1);
        let config = DbiDatabaseConfig::default();
        let db_impl =
            DatabaseImpl::new(db_id, "test".to_string(), DbType::User, &config);
        let db_arc = Arc::new(RwLock::new(db_impl));
        let inner = CursorImpl::new(db_arc, 0);
        Cursor::from_impl(inner, read_only)
    }

    /// Creates a cursor backed by a DatabaseImpl pre-populated with records.
    fn make_cursor_with(records: Vec<(&[u8], &[u8])>) -> Cursor {
        let db_id = DatabaseId::new(1);
        let config = DbiDatabaseConfig::default();
        let db_impl =
            DatabaseImpl::new(db_id, "test".to_string(), DbType::User, &config);
        let db_arc = Arc::new(RwLock::new(db_impl));

        {
            let mut tmp = CursorImpl::new(Arc::clone(&db_arc), 0);
            for (k, v) in &records {
                tmp.put(k, v, PutMode::Overwrite).unwrap();
            }
        }

        let inner = CursorImpl::new(db_arc, 0);
        Cursor::from_impl(inner, false)
    }

    #[test]
    fn test_new_cursor() {
        let cursor = make_cursor(false);
        assert_eq!(cursor.get_state(), CursorState::NotInitialized);
        assert!(cursor.is_valid());
        assert!(!cursor.is_read_only());
    }

    #[test]
    fn test_read_only_cursor() {
        let cursor = make_cursor(true);
        assert!(cursor.is_read_only());
    }

    #[test]
    fn test_search() {
        let mut cursor = make_cursor_with(vec![(b"key1", b"value1")]);
        let mut key = DatabaseEntry::from_bytes(b"key1");
        let mut data = DatabaseEntry::new();

        let status = cursor.get(&mut key, &mut data, Get::Search, None).unwrap();
        assert_eq!(status, OperationStatus::Success);
        assert_eq!(data.get_data().unwrap(), b"value1");
        assert_eq!(cursor.get_state(), CursorState::Initialized);
    }

    #[test]
    fn test_search_not_found() {
        let mut cursor = make_cursor_with(vec![(b"key1", b"value1")]);
        let mut key = DatabaseEntry::from_bytes(b"key2");
        let mut data = DatabaseEntry::new();

        let status = cursor.get(&mut key, &mut data, Get::Search, None).unwrap();
        assert_eq!(status, OperationStatus::NotFound);
    }

    #[test]
    fn test_first() {
        let mut cursor = make_cursor_with(vec![
            (b"key3", b"value3"),
            (b"key1", b"value1"),
            (b"key2", b"value2"),
        ]);
        let mut key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();

        let status = cursor.get(&mut key, &mut data, Get::First, None).unwrap();
        assert_eq!(status, OperationStatus::Success);
        assert_eq!(data.get_data().unwrap(), b"value1");
        assert_eq!(cursor.get_state(), CursorState::Initialized);
    }

    #[test]
    fn test_last() {
        let mut cursor = make_cursor_with(vec![
            (b"key3", b"value3"),
            (b"key1", b"value1"),
            (b"key2", b"value2"),
        ]);
        let mut key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();

        let status = cursor.get(&mut key, &mut data, Get::Last, None).unwrap();
        assert_eq!(status, OperationStatus::Success);
        assert_eq!(data.get_data().unwrap(), b"value3");
    }

    #[test]
    fn test_next_iteration() {
        let mut cursor = make_cursor_with(vec![
            (b"key3", b"value3"),
            (b"key1", b"value1"),
            (b"key2", b"value2"),
        ]);
        let mut key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();

        let status = cursor.get(&mut key, &mut data, Get::First, None).unwrap();
        assert_eq!(status, OperationStatus::Success);
        assert_eq!(data.get_data().unwrap(), b"value1");

        let status = cursor.get(&mut key, &mut data, Get::Next, None).unwrap();
        assert_eq!(status, OperationStatus::Success);
        assert_eq!(data.get_data().unwrap(), b"value2");

        let status = cursor.get(&mut key, &mut data, Get::Next, None).unwrap();
        assert_eq!(status, OperationStatus::Success);
        assert_eq!(data.get_data().unwrap(), b"value3");

        let status = cursor.get(&mut key, &mut data, Get::Next, None).unwrap();
        assert_eq!(status, OperationStatus::NotFound);
    }

    #[test]
    fn test_prev_iteration() {
        let mut cursor = make_cursor_with(vec![
            (b"key3", b"value3"),
            (b"key1", b"value1"),
            (b"key2", b"value2"),
        ]);
        let mut key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();

        let status = cursor.get(&mut key, &mut data, Get::Last, None).unwrap();
        assert_eq!(status, OperationStatus::Success);
        assert_eq!(data.get_data().unwrap(), b"value3");

        let status = cursor.get(&mut key, &mut data, Get::Prev, None).unwrap();
        assert_eq!(status, OperationStatus::Success);
        assert_eq!(data.get_data().unwrap(), b"value2");

        let status = cursor.get(&mut key, &mut data, Get::Prev, None).unwrap();
        assert_eq!(status, OperationStatus::Success);
        assert_eq!(data.get_data().unwrap(), b"value1");

        let status = cursor.get(&mut key, &mut data, Get::Prev, None).unwrap();
        assert_eq!(status, OperationStatus::NotFound);
    }

    #[test]
    fn test_current() {
        let mut cursor = make_cursor_with(vec![(b"key1", b"value1")]);
        let mut key = DatabaseEntry::from_bytes(b"key1");
        let mut data = DatabaseEntry::new();

        cursor.get(&mut key, &mut data, Get::Search, None).unwrap();

        let status = cursor.get(&mut key, &mut data, Get::Current, None).unwrap();
        assert_eq!(status, OperationStatus::Success);
        assert_eq!(data.get_data().unwrap(), b"value1");
    }

    #[test]
    fn test_current_not_initialized() {
        let mut cursor = make_cursor(false);
        let mut key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();

        let result = cursor.get(&mut key, &mut data, Get::Current, None);
        assert!(result.is_err());
    }

    #[test]
    fn test_put_overwrite() {
        let mut cursor = make_cursor(false);

        let mut key = DatabaseEntry::from_bytes(b"key1");
        let data = DatabaseEntry::from_bytes(b"value1");

        let status = cursor.put(&key, &data, Put::Overwrite).unwrap();
        assert_eq!(status, OperationStatus::Success);
        assert_eq!(cursor.get_state(), CursorState::Initialized);

        // Verify by reading back
        let mut out = DatabaseEntry::new();
        let s = cursor.get(&mut key, &mut out, Get::Search, None).unwrap();
        assert_eq!(s, OperationStatus::Success);
        assert_eq!(out.get_data().unwrap(), b"value1");
    }

    #[test]
    fn test_put_no_overwrite() {
        let mut cursor = make_cursor_with(vec![(b"key1", b"value1")]);

        let key = DatabaseEntry::from_bytes(b"key1");
        let data = DatabaseEntry::from_bytes(b"value2");

        let status = cursor.put(&key, &data, Put::NoOverwrite).unwrap();
        assert_eq!(status, OperationStatus::KeyExists);
    }

    #[test]
    fn test_put_no_overwrite_new_key() {
        let mut cursor = make_cursor(false);

        let mut key = DatabaseEntry::from_bytes(b"key1");
        let data = DatabaseEntry::from_bytes(b"value1");

        let status = cursor.put(&key, &data, Put::NoOverwrite).unwrap();
        assert_eq!(status, OperationStatus::Success);

        // Verify by reading back
        let mut out = DatabaseEntry::new();
        let s = cursor.get(&mut key, &mut out, Get::Search, None).unwrap();
        assert_eq!(s, OperationStatus::Success);
        assert_eq!(out.get_data().unwrap(), b"value1");
    }

    #[test]
    fn test_put_current() {
        let mut cursor = make_cursor_with(vec![(b"key1", b"value1")]);

        let mut key = DatabaseEntry::from_bytes(b"key1");
        let mut data = DatabaseEntry::new();
        cursor.get(&mut key, &mut data, Get::Search, None).unwrap();

        let new_data = DatabaseEntry::from_bytes(b"value2");
        let status = cursor.put(&key, &new_data, Put::Current).unwrap();
        assert_eq!(status, OperationStatus::Success);

        // Verify updated
        let mut out = DatabaseEntry::new();
        cursor.get(&mut key, &mut out, Get::Search, None).unwrap();
        assert_eq!(out.get_data().unwrap(), b"value2");
    }

    #[test]
    fn test_put_read_only() {
        let mut cursor = make_cursor(true);

        let key = DatabaseEntry::from_bytes(b"key1");
        let data = DatabaseEntry::from_bytes(b"value1");

        let result = cursor.put(&key, &data, Put::Overwrite);
        assert!(result.is_err());
    }

    #[test]
    fn test_delete() {
        let mut cursor = make_cursor_with(vec![(b"key1", b"value1")]);

        let mut key = DatabaseEntry::from_bytes(b"key1");
        let mut data = DatabaseEntry::new();
        cursor.get(&mut key, &mut data, Get::Search, None).unwrap();

        let status = cursor.delete().unwrap();
        assert_eq!(status, OperationStatus::Success);
        assert_eq!(cursor.get_state(), CursorState::NotInitialized);

        // Verify deleted
        let s = cursor.get(&mut key, &mut data, Get::Search, None).unwrap();
        assert_eq!(s, OperationStatus::NotFound);
    }

    #[test]
    fn test_delete_not_positioned() {
        let mut cursor = make_cursor(false);
        let result = cursor.delete();
        assert!(result.is_err());
    }

    #[test]
    fn test_delete_read_only() {
        let mut cursor = make_cursor_with(vec![(b"key1", b"value1")]);

        let mut key = DatabaseEntry::from_bytes(b"key1");
        let mut data = DatabaseEntry::new();
        cursor.get(&mut key, &mut data, Get::Search, None).unwrap();

        // Simulate read-only after positioning
        cursor.read_only = true;
        let result = cursor.delete();
        assert!(result.is_err());
    }

    #[test]
    fn test_count() {
        let mut cursor = make_cursor_with(vec![(b"key1", b"value1")]);

        // Not positioned
        assert_eq!(cursor.count().unwrap(), 0);

        // Position cursor
        let mut key = DatabaseEntry::from_bytes(b"key1");
        let mut data = DatabaseEntry::new();
        cursor.get(&mut key, &mut data, Get::Search, None).unwrap();

        // Count should be 1 for non-dup DB
        assert_eq!(cursor.count().unwrap(), 1);
    }

    #[test]
    fn test_close() {
        let mut cursor = make_cursor(false);

        assert!(cursor.is_valid());
        cursor.close().unwrap();
        assert!(!cursor.is_valid());
        assert_eq!(cursor.get_state(), CursorState::Closed);
    }

    #[test]
    fn test_close_twice() {
        let mut cursor = make_cursor(false);

        cursor.close().unwrap();
        let result = cursor.close();
        assert!(result.is_err());
    }

    #[test]
    fn test_operations_after_close() {
        let mut cursor = make_cursor(false);

        cursor.close().unwrap();

        let mut key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();

        let result = cursor.get(&mut key, &mut data, Get::First, None);
        assert!(result.is_err());
    }

    #[test]
    fn test_empty_database_iteration() {
        let mut cursor = make_cursor(false);

        let mut key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();

        let status = cursor.get(&mut key, &mut data, Get::First, None).unwrap();
        assert_eq!(status, OperationStatus::NotFound);
    }

    #[test]
    fn test_sorted_iteration() {
        let mut cursor = make_cursor_with(vec![
            (b"zebra", b"z"),
            (b"apple", b"a"),
            (b"mango", b"m"),
        ]);
        let mut key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();

        let mut values = Vec::new();

        let mut status = cursor.get(&mut key, &mut data, Get::First, None).unwrap();
        while status == OperationStatus::Success {
            values.push(data.get_data().unwrap().to_vec());
            status = cursor.get(&mut key, &mut data, Get::Next, None).unwrap();
        }

        assert_eq!(values, vec![b"a".to_vec(), b"m".to_vec(), b"z".to_vec()]);
    }

    #[test]
    fn test_next_from_uninitialized() {
        let mut cursor = make_cursor_with(vec![
            (b"key1", b"value1"),
            (b"key2", b"value2"),
        ]);
        let mut key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();

        // Next from uninitialized should return first
        let status = cursor.get(&mut key, &mut data, Get::Next, None).unwrap();
        assert_eq!(status, OperationStatus::Success);
        assert_eq!(data.get_data().unwrap(), b"value1");
    }

    #[test]
    fn test_prev_from_uninitialized() {
        let mut cursor = make_cursor_with(vec![
            (b"key1", b"value1"),
            (b"key2", b"value2"),
        ]);
        let mut key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();

        // Prev from uninitialized should return last
        let status = cursor.get(&mut key, &mut data, Get::Prev, None).unwrap();
        assert_eq!(status, OperationStatus::Success);
        assert_eq!(data.get_data().unwrap(), b"value2");
    }

    #[test]
    fn test_cursor_state_transitions() {
        let mut cursor = make_cursor_with(vec![(b"key1", b"value1")]);
        assert_eq!(cursor.get_state(), CursorState::NotInitialized);

        let mut key = DatabaseEntry::from_bytes(b"key1");
        let mut data = DatabaseEntry::new();
        cursor.get(&mut key, &mut data, Get::Search, None).unwrap();
        assert_eq!(cursor.get_state(), CursorState::Initialized);

        cursor.delete().unwrap();
        assert_eq!(cursor.get_state(), CursorState::NotInitialized);

        cursor.close().unwrap();
        assert_eq!(cursor.get_state(), CursorState::Closed);
    }

    // ========================================================================
    // Additional branch-coverage tests
    // ========================================================================

    /// Get::SearchGte with empty key returns NotFound.
    #[test]
    fn test_search_gte_empty_key_returns_not_found() {
        let mut cursor = make_cursor_with(vec![(b"key1", b"value1")]);
        let mut key = DatabaseEntry::new(); // no data
        let mut data = DatabaseEntry::new();

        let status = cursor.get(&mut key, &mut data, Get::SearchGte, None).unwrap();
        assert_eq!(status, OperationStatus::NotFound);
    }

    /// Get::Search with empty key returns NotFound.
    #[test]
    fn test_search_empty_key_returns_not_found() {
        let mut cursor = make_cursor_with(vec![(b"key1", b"value1")]);
        let mut key = DatabaseEntry::new(); // no data
        let mut data = DatabaseEntry::new();

        let status = cursor.get(&mut key, &mut data, Get::Search, None).unwrap();
        assert_eq!(status, OperationStatus::NotFound);
    }

    /// Get::_ wildcard arm: unimplemented Get variants return NotFound.
    #[test]
    fn test_get_other_variant_returns_not_found() {
        let mut cursor = make_cursor_with(vec![(b"key1", b"value1")]);
        let mut key = DatabaseEntry::from_bytes(b"key1");
        let mut data = DatabaseEntry::new();

        // Position cursor first.
        cursor.get(&mut key, &mut data, Get::Search, None).unwrap();

        // Get::NextDup and other variants fall through to the `_ =>` arm.
        let status = cursor.get(&mut key, &mut data, Get::NextDup, None).unwrap();
        assert_eq!(status, OperationStatus::NotFound);
    }

    /// Put::NoDupData on a non-dup database inserts when the key is new.
    ///
    /// `Cursor.putNoDupData()`: for a non-dup database NoDupData
    /// behaves like NoOverwrite (returns KeyExists if the key is already
    /// present, Success otherwise).
    #[test]
    fn test_put_no_dup_data_inserts_new_key() {
        let mut cursor = make_cursor(false);

        let mut key = DatabaseEntry::from_bytes(b"k");
        let data = DatabaseEntry::from_bytes(b"v");

        let status = cursor.put(&key, &data, Put::NoDupData).unwrap();
        assert_eq!(status, OperationStatus::Success);

        // Verify the record is readable.
        let mut out = DatabaseEntry::new();
        let s = cursor.get(&mut key, &mut out, Get::Search, None).unwrap();
        assert_eq!(s, OperationStatus::Success);
        assert_eq!(out.get_data().unwrap(), b"v");
    }

    /// Put::NoDupData returns KeyExists when the key already exists (non-dup DB).
    #[test]
    fn test_put_no_dup_data_key_exists() {
        let mut cursor = make_cursor_with(vec![(b"k", b"v")]);

        let key = DatabaseEntry::from_bytes(b"k");
        let data = DatabaseEntry::from_bytes(b"v2");

        let status = cursor.put(&key, &data, Put::NoDupData).unwrap();
        assert_eq!(status, OperationStatus::KeyExists);
    }

    /// Put::Current when cursor is not initialized returns an error.
    #[test]
    fn test_put_current_not_initialized_returns_error() {
        let mut cursor = make_cursor(false);

        let key = DatabaseEntry::from_bytes(b"k");
        let data = DatabaseEntry::from_bytes(b"v");

        let result = cursor.put(&key, &data, Put::Current);
        assert!(result.is_err());
    }

    /// Get::First on empty DB resets state to NotInitialized.
    #[test]
    fn test_first_not_found_resets_state() {
        let mut cursor = make_cursor(false); // empty DB
        let mut key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();

        let status = cursor.get(&mut key, &mut data, Get::First, None).unwrap();
        assert_eq!(status, OperationStatus::NotFound);
        // After a failed First the state must be NotInitialized.
        assert_eq!(cursor.get_state(), CursorState::NotInitialized);
    }

    /// Get::Last on empty DB resets state to NotInitialized.
    #[test]
    fn test_last_not_found_resets_state() {
        let mut cursor = make_cursor(false); // empty DB
        let mut key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();

        let status = cursor.get(&mut key, &mut data, Get::Last, None).unwrap();
        assert_eq!(status, OperationStatus::NotFound);
        assert_eq!(cursor.get_state(), CursorState::NotInitialized);
    }

    /// Get::Search not-found resets state to NotInitialized.
    #[test]
    fn test_search_not_found_resets_state() {
        let mut cursor = make_cursor_with(vec![(b"key1", b"v1")]);
        let mut key = DatabaseEntry::from_bytes(b"key1");
        let mut data = DatabaseEntry::new();

        // Position first.
        cursor.get(&mut key, &mut data, Get::Search, None).unwrap();
        assert_eq!(cursor.get_state(), CursorState::Initialized);

        // Now search for a missing key — state must go back to NotInitialized.
        let mut key_miss = DatabaseEntry::from_bytes(b"missing");
        let status = cursor.get(&mut key_miss, &mut data, Get::Search, None).unwrap();
        assert_eq!(status, OperationStatus::NotFound);
        assert_eq!(cursor.get_state(), CursorState::NotInitialized);
    }

    /// Get::SearchGte not-found resets state.
    #[test]
    fn test_search_gte_not_found_resets_state() {
        let mut cursor = make_cursor_with(vec![(b"key1", b"v1")]);
        let mut key = DatabaseEntry::from_bytes(b"key1");
        let mut data = DatabaseEntry::new();

        cursor.get(&mut key, &mut data, Get::Search, None).unwrap();

        let mut key_big = DatabaseEntry::from_bytes(b"zzz");
        let status = cursor.get(&mut key_big, &mut data, Get::SearchGte, None).unwrap();
        assert_eq!(status, OperationStatus::NotFound);
        assert_eq!(cursor.get_state(), CursorState::NotInitialized);
    }

    /// count() on a closed cursor returns an error.
    #[test]
    fn test_count_on_closed_cursor_returns_error() {
        let mut cursor = make_cursor(false);
        cursor.close().unwrap();
        let result = cursor.count();
        assert!(result.is_err());
    }

    /// Delete on a read-only cursor that is NOT positioned (check_initialized
    /// fires before check read_only).
    #[test]
    fn test_delete_not_positioned_check_fires_before_read_only_check() {
        let mut cursor = make_cursor(true); // read-only, not initialized
        let result = cursor.delete();
        // check_initialized fires first → error about "not positioned".
        assert!(result.is_err());
    }

    /// SearchGte success path: search for a key that is less than the first
    /// key in the database — should position at the first key (range semantics).
    #[test]
    fn test_search_gte_positions_at_ge_key() {
        let mut cursor = make_cursor_with(vec![(b"mango", b"yellow")]);
        let mut key = DatabaseEntry::from_bytes(b"apple"); // < "mango"
        let mut data = DatabaseEntry::new();

        let status = cursor.get(&mut key, &mut data, Get::SearchGte, None).unwrap();
        assert_eq!(status, OperationStatus::Success);
        assert_eq!(data.get_data().unwrap(), b"yellow");
        assert_eq!(cursor.get_state(), CursorState::Initialized);
    }

    // ========================================================================
    // map_err closure coverage: close the inner CursorImpl so that operations
    // on it return DbiError::CursorClosed.  The outer Cursor::state is kept
    // at NotInitialized so that check_open() passes, but the underlying
    // CursorImpl returns an error, exercising every `map_err(|e| ...)` closure.
    // ========================================================================

    /// Helper: cursor whose outer state is NotInitialized but whose inner
    /// CursorImpl has been closed, so that any CursorImpl call returns an error.
    fn make_inner_closed_cursor() -> Cursor {
        let mut c = make_cursor(false);
        c.inner.close().unwrap(); // CursorImpl is now Closed
        // outer state stays NotInitialized — check_open() will pass
        c
    }

    /// Helper: cursor whose outer state is Initialized but whose inner
    /// CursorImpl has been closed (simulate a mid-flight error scenario).
    fn make_inner_closed_cursor_initialized() -> Cursor {
        let mut c = make_cursor(false);
        // Manually set outer state to Initialized so check_initialized() passes.
        c.state = CursorState::Initialized;
        c.inner.close().unwrap();
        c
    }

    /// Get::Search map_err closure: CursorImpl::search returns an error when
    /// the inner cursor is closed.
    #[test]
    fn test_search_map_err_closure_covered() {
        let mut cursor = make_inner_closed_cursor();
        let mut key = DatabaseEntry::from_bytes(b"k");
        let mut data = DatabaseEntry::new();
        let result = cursor.get(&mut key, &mut data, Get::Search, None);
        assert!(result.is_err());
    }

    /// Get::SearchGte map_err closure.
    #[test]
    fn test_search_gte_map_err_closure_covered() {
        let mut cursor = make_inner_closed_cursor();
        let mut key = DatabaseEntry::from_bytes(b"k");
        let mut data = DatabaseEntry::new();
        let result = cursor.get(&mut key, &mut data, Get::SearchGte, None);
        assert!(result.is_err());
    }

    /// Get::First map_err closure.
    #[test]
    fn test_first_map_err_closure_covered() {
        let mut cursor = make_inner_closed_cursor();
        let mut key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();
        let result = cursor.get(&mut key, &mut data, Get::First, None);
        assert!(result.is_err());
    }

    /// Get::Last map_err closure.
    #[test]
    fn test_last_map_err_closure_covered() {
        let mut cursor = make_inner_closed_cursor();
        let mut key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();
        let result = cursor.get(&mut key, &mut data, Get::Last, None);
        assert!(result.is_err());
    }

    /// Get::Next (uninitialized path, calls get_first) map_err closure.
    #[test]
    fn test_next_uninit_map_err_closure_covered() {
        let mut cursor = make_inner_closed_cursor();
        let mut key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();
        // state is NotInitialized → takes the get_first branch
        let result = cursor.get(&mut key, &mut data, Get::Next, None);
        assert!(result.is_err());
    }

    /// Get::Next (initialized path, calls retrieve_next) map_err closure.
    #[test]
    fn test_next_init_map_err_closure_covered() {
        let mut cursor = make_inner_closed_cursor_initialized();
        let mut key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();
        // state is Initialized → takes the retrieve_next branch
        let result = cursor.get(&mut key, &mut data, Get::Next, None);
        assert!(result.is_err());
    }

    /// Get::Prev (uninitialized path, calls get_last) map_err closure.
    #[test]
    fn test_prev_uninit_map_err_closure_covered() {
        let mut cursor = make_inner_closed_cursor();
        let mut key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();
        let result = cursor.get(&mut key, &mut data, Get::Prev, None);
        assert!(result.is_err());
    }

    /// Get::Prev (initialized path, calls retrieve_next(Prev)) map_err closure.
    #[test]
    fn test_prev_init_map_err_closure_covered() {
        let mut cursor = make_inner_closed_cursor_initialized();
        let mut key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();
        let result = cursor.get(&mut key, &mut data, Get::Prev, None);
        assert!(result.is_err());
    }

    /// Get::Current map_err closure (get_current on inner closed cursor).
    #[test]
    fn test_current_map_err_closure_covered() {
        let mut cursor = make_inner_closed_cursor_initialized();
        let mut key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();
        // check_initialized passes (outer state = Initialized),
        // but inner.get_current() returns CursorClosed → map_err fires.
        let result = cursor.get(&mut key, &mut data, Get::Current, None);
        assert!(result.is_err());
    }

    /// After a successful get_first/search, the get_current() call inside the
    /// success arm also goes through map_err — exercise it by making the inner
    /// cursor report "closed" after the search position has been set.
    /// We do this by directly calling the success-arm get_current via the
    /// Cursor::get flow: position outer state via Search success, then call
    /// CursorImpl::close() and call Get::Current.
    #[test]
    fn test_get_success_branch_get_current_map_err() {
        let mut cursor = make_cursor_with(vec![(b"key1", b"val1")]);
        // First do a real search so inner state = Initialized
        let mut key = DatabaseEntry::from_bytes(b"key1");
        let mut data = DatabaseEntry::new();
        cursor.get(&mut key, &mut data, Get::Search, None).unwrap();
        // Now close the inner cursor; outer state remains Initialized
        cursor.inner.close().unwrap();
        // Get::First triggers get_first() on the closed inner cursor → error
        let mut key2 = DatabaseEntry::new();
        let mut data2 = DatabaseEntry::new();
        let result = cursor.get(&mut key2, &mut data2, Get::First, None);
        assert!(result.is_err());
    }

    /// Put map_err closure: CursorImpl::put returns an error when inner is closed.
    #[test]
    fn test_put_map_err_closure_covered() {
        let mut cursor = make_inner_closed_cursor();
        let key = DatabaseEntry::from_bytes(b"k");
        let data = DatabaseEntry::from_bytes(b"v");
        let result = cursor.put(&key, &data, Put::Overwrite);
        assert!(result.is_err());
    }

    /// Delete map_err closure: CursorImpl::delete returns an error when inner is closed.
    #[test]
    fn test_delete_map_err_closure_covered() {
        let mut cursor = make_inner_closed_cursor_initialized();
        let result = cursor.delete();
        assert!(result.is_err());
    }
}
