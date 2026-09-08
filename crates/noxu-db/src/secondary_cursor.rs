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
//! The `delete` operation deletes the *primary* record and cascades to
//! every secondary index entry that referred to that primary record
//! (within this secondary database).  Both deletes participate in the
//! transaction the cursor was opened under (Wave 1B / audit F5): they
//! commit together when the txn commits, and roll back together when
//! the txn aborts.  When the cursor was opened with `txn = None` the
//! cascade runs auto-committed, matching the v1.4 behaviour.

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
/// let mut cursor = secondary_db.open_cursor(None)?;
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
    inner: Cursor<'a>,
    /// Back-reference to the owning SecondaryDatabase (for primary lookups).
    secondary_db: &'a SecondaryDatabase,
    /// Transaction handle the cursor was opened under.
    txn: Option<&'a Transaction>,
    /// Whether this cursor runs in read-uncommitted (dirty-read) mode.
    /// When true, a missing primary during read-through is skipped rather
    /// than raising SecondaryIntegrityException.  D8 fix.
    /// Ref: JE SecondaryCursor getNextWithKeySearch() dirty-read skip.
    read_uncommitted: bool,
    /// F1: the secondary key this cursor is parked on for a JoinCursor probe,
    /// captured on the first `has_candidate_primary_key` call. Subsequent
    /// probes SearchBoth against THIS key (not the cursor's live position,
    /// which the SearchBoth itself moves within the dup set).
    join_probe_key: Option<Vec<u8>>,
}

impl<'a> SecondaryCursor<'a> {
    /// Creates a new SecondaryCursor.  Called by `SecondaryDatabase::open_cursor`.
    ///
    /// `txn` and `config` are forwarded to the inner `Database::open_cursor`
    /// call so the secondary cursor participates in the caller's transaction
    /// and honours any cursor-level configuration.  See API audit 2026-05
    /// secondary-join finding F4: the previous signature dropped both
    /// arguments on the floor and ran every secondary cursor auto-commit.
    ///
    /// The txn handle is also stored on
    /// the `SecondaryCursor` itself so that primary lookups and the
    /// [`SecondaryCursor::delete`] cascade execute under the same
    /// transaction as the inner secondary cursor.  Previously these
    /// out-of-band primary operations always ran auto-committed, leaking
    /// secondary-cleanup writes around an aborted user txn.
    pub(crate) fn new(
        secondary_db: &'a SecondaryDatabase,
        txn: Option<&'a Transaction>,
        config: Option<&CursorConfig>,
    ) -> Result<Self> {
        let read_uncommitted = config.is_some_and(|c| c.read_uncommitted);
        let inner =
            secondary_db.inner_db().open_cursor_internal(txn, config)?;
        Ok(Self {
            inner,
            secondary_db,
            txn,
            read_uncommitted,
            join_probe_key: None,
        })
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

    /// Deletes the primary record at the current cursor position and
    /// every secondary index entry (within this secondary database) that
    /// referred to it.
    ///
    /// # Cascade semantics
    ///
    /// 1. The primary key is read from the current secondary record.
    /// 2. The primary data is fetched so the key creator can recompute
    ///    every secondary key it produced for this primary record.
    /// 3. `SecondaryDatabase::delete_all_for_primary` is invoked to
    ///    remove every matching secondary index entry — **under the
    ///    cursor's stored txn**.  This is more general than just
    ///    deleting the entry the cursor is positioned on: a multi-key
    ///    creator may have produced several secondary keys for the
    ///    primary, all of which become orphans once the primary is
    ///    deleted.
    /// 4. The primary record itself is deleted — also under the
    ///    cursor's stored txn.
    ///
    /// # Atomicity
    ///
    /// When the cursor was opened with `Some(&txn)` (see
    /// [`SecondaryDatabase::open_cursor`]), every step above runs
    /// inside `txn`.  Aborting the txn rolls back **both** the primary
    /// delete and every secondary cleanup performed by this method;
    /// committing the txn persists both.  Before v1.6 this method
    /// dropped the txn on the floor and ran the cascade
    /// auto-committed, so an aborted user txn could leave the primary
    /// behind while still removing the secondary entries (or vice
    /// versa.
    ///
    /// When the cursor was opened with `None`, every step runs
    /// auto-committed, which preserves the v1.4 behaviour.
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
            let bytes = p_key_entry.data_opt().unwrap_or(&[]).to_vec();
            DatabaseEntry::from_bytes(&bytes)
        };

        // Fetch primary data for secondary cleanup.  Reads run under the
        // cursor's txn so the lookup observes uncommitted writes the
        // caller's own txn has already made (e.g. an earlier put followed
        // by a delete in the same txn) and so cross-txn isolation is
        // honoured.
        let mut pri_data = DatabaseEntry::new();
        let pri_found = {
            let primary = self.secondary_db.primary_db().lock();
            primary.get_into(self.txn, &pri_key, &mut pri_data)?
        };

        if pri_found {
            // primary.delete() auto-maintains secondary entries via the hook.
            // Do NOT call delete_all_for_primary first (would trigger D7).
        }

        // Delete the primary; the auto-hook removes all secondary entries.
        let deleted = {
            let primary = self.secondary_db.primary_db().lock();
            match self.txn {
                Some(t) => primary.delete_in(t, &pri_key)?,
                None => primary.delete(&pri_key)?,
            }
        };

        Ok(if deleted {
            OperationStatus::Success
        } else {
            OperationStatus::NotFound
        })
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

    /// Moves to the next duplicate of the current secondary key, returning
    /// the (sec_key, p_key, data) triple of that duplicate.
    ///
    /// v1.6 sorted-dup secondaries (Decision 1B / audit C4): when a single
    /// secondary key is shared by multiple primaries, this enumerates them
    /// in cursor order.  Returns [`OperationStatus::NotFound`] when the
    /// cursor steps onto a different secondary key, which is the standard
    /// signal for “end of duplicate run”.
    pub fn get_next_dup_full(
        &mut self,
        key: &mut DatabaseEntry,
        p_key: &mut DatabaseEntry,
        data: &mut DatabaseEntry,
    ) -> Result<OperationStatus> {
        self.step_dup_full(key, p_key, data, /* forward = */ true)
    }

    /// Moves to the previous duplicate of the current secondary key,
    /// returning the (sec_key, p_key, data) triple of that duplicate.
    ///
    /// Returns [`OperationStatus::NotFound`] when the cursor steps onto a
    /// different secondary key, marking the start of the duplicate run.
    pub fn get_prev_dup_full(
        &mut self,
        key: &mut DatabaseEntry,
        p_key: &mut DatabaseEntry,
        data: &mut DatabaseEntry,
    ) -> Result<OperationStatus> {
        self.step_dup_full(key, p_key, data, /* forward = */ false)
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
        let pri_key_bytes = stored_pk.data_opt().unwrap_or(&[]).to_vec();
        p_key.set_data(&pri_key_bytes);

        // Fetch primary data under the cursor's txn so the lookup
        // observes the caller's uncommitted writes and honours cross-txn
        // isolation (Wave 1B).
        let pri_key_entry = DatabaseEntry::from_bytes(&pri_key_bytes);
        let primary = self.secondary_db.primary_db().lock();
        let pri_found = primary.get_into(self.txn, &pri_key_entry, data)?;
        if !pri_found {
            // D8: if running in dirty-read (read-uncommitted) mode, skip
            // the record rather than raising SecondaryIntegrityException.
            // JE SecondaryCursor dirty-read primary-missing skip.
            // Ref: SecondaryCursor.java getWithPrimaryData() dirty-read path.
            if self.read_uncommitted {
                return Ok(OperationStatus::NotFound);
            }
            return Err(NoxuError::SecondaryIntegrityException(format!(
                "Secondary '{}' refers to missing primary key",
                self.secondary_db.name()
            )));
        }

        Ok(OperationStatus::Success)
    }

    /// Searches for the first secondary key >= `search_key`.
    ///
    /// `search_key` is updated in place with the actual key found (which
    /// may be strictly greater than the input).  See
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
        let pri_key_bytes = stored_pk.data_opt().unwrap_or(&[]).to_vec();
        p_key.set_data(&pri_key_bytes);

        let pri_key_entry = DatabaseEntry::from_bytes(&pri_key_bytes);
        let primary = self.secondary_db.primary_db().lock();
        let pri_found = primary.get_into(self.txn, &pri_key_entry, data)?;
        if !pri_found {
            // D8: dirty-read skip (see get_search_key site for full comment).
            if self.read_uncommitted {
                return Ok(OperationStatus::NotFound);
            }
            return Err(NoxuError::SecondaryIntegrityException(format!(
                "Secondary '{}' refers to missing primary key",
                self.secondary_db.name()
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
        Ok(pri_key_entry.data_opt().map(|d| d.to_vec()))
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
        Ok(sec_key.data_opt().map(|d| d.to_vec()))
    }

    /// Returns an estimate of the number of primary keys that share the
    /// current secondary key (the duplicate count at the current position).
    /// For sorted-dup secondaries this reflects the actual number of
    /// duplicates; for non-dup secondaries it is 0 or 1.
    pub(crate) fn count_estimate(&mut self) -> u64 {
        self.inner.count().unwrap_or_default()
    }

    /// Advances to the next record that has the **same** secondary key as
    /// the current position (i.e. the next "duplicate").
    ///
    /// For sorted-dup secondaries this walks the duplicate set; for non-dup
    /// secondaries the cursor stores exactly one primary key per secondary
    /// key, so this returns `NotFound` after the single record.
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
        let new_sk = sec_key.data_opt().map(|d| d.to_vec()).unwrap_or_default();
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
        // F1 (JE JoinCursor.retrieveNext): probe whether THIS secondary index
        // contains the pair (current secondary key, candidate primary key).
        // JE issues `secCursor.search(secKey, candidatePK, SearchMode.BOTH)` —
        // an exact (sec_key, pk) lookup that scans the whole duplicate set, NOT
        // a comparison against the single PK the cursor happens to sit on.
        // Reading only Get::Current drops valid matches whenever a secondary
        // key maps to more than one primary record.
        //
        // Read the secondary key the join parked this cursor on, then do a
        // SearchBoth(sec_key, candidate) on the underlying secondary-index
        // storage (sec_key -> pri_key).
        // Capture the join secondary key ONCE (the SearchBoth below moves the
        // cursor within the dup set, so we must not re-read Get::Current on a
        // later probe — it would no longer be the join key).
        if self.join_probe_key.is_none() {
            let mut cur_sec_key = DatabaseEntry::new();
            let mut cur_pk = DatabaseEntry::new();
            let st = match self.inner.get(
                &mut cur_sec_key,
                &mut cur_pk,
                Get::Current,
                None,
            ) {
                Ok(s) => s,
                Err(_) => return Ok(false),
            };
            if st != OperationStatus::Success {
                return Ok(false);
            }
            self.join_probe_key = Some(cur_sec_key.data().to_vec());
        }
        let sec_key = self.join_probe_key.clone().unwrap();
        // SearchBoth: exact (sec_key, candidate-pk) match in the dup set.
        let mut k = DatabaseEntry::from_bytes(&sec_key);
        let mut d = DatabaseEntry::from_bytes(candidate);
        match self.inner.get(&mut k, &mut d, Get::SearchBoth, None) {
            Ok(OperationStatus::Success) => Ok(true),
            _ => Ok(false),
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
            pri_key_bytes_entry.data_opt().unwrap_or(&[]).to_vec();
        p_key.set_data(&pri_key_bytes);

        // Step 3: look up the primary record under the cursor's txn so
        // the lookup honours the caller's transaction (Wave 1B).
        let pri_key_entry = DatabaseEntry::from_bytes(&pri_key_bytes);
        let primary = self.secondary_db.primary_db().lock();
        let pri_found = primary.get_into(self.txn, &pri_key_entry, data)?;

        if !pri_found {
            // D8: dirty-read (read-uncommitted) mode — skip the record
            // instead of raising SecondaryIntegrityException.  The primary
            // may have been concurrently deleted; under dirty-read we return
            // NotFound and let the caller advance.
            // Non-dirty-read: raise SecondaryIntegrityException (secondary
            // index is inconsistent).
            // Ref: SecondaryCursor.java getNextWithKeySearch() dirty-read path.
            if self.read_uncommitted {
                return Ok(OperationStatus::NotFound);
            }
            return Err(NoxuError::SecondaryIntegrityException(format!(
                "Secondary '{}' refers to missing primary key",
                self.secondary_db.name()
            )));
        }

        Ok(OperationStatus::Success)
    }
}

impl<'a> SecondaryCursor<'a> {
    /// Stepwise helper for `get_next_dup_full` / `get_prev_dup_full`.
    ///
    /// Reads the current secondary key, steps the inner cursor in the
    /// requested direction, then verifies the new secondary key still
    /// matches.  If it does, fetches the primary data; otherwise reports
    /// `NotFound` (run end).
    fn step_dup_full(
        &mut self,
        key: &mut DatabaseEntry,
        p_key: &mut DatabaseEntry,
        data: &mut DatabaseEntry,
        forward: bool,
    ) -> Result<OperationStatus> {
        let Some(current_sk) = self.get_current_sec_key_bytes()? else {
            return Ok(OperationStatus::NotFound);
        };

        let mode = if forward { Get::Next } else { Get::Prev };
        let status = self.get_with_mode(key, p_key, data, mode)?;
        if status != OperationStatus::Success {
            return Ok(OperationStatus::NotFound);
        }

        let new_sk = key.data_opt().map(|d| d.to_vec()).unwrap_or_default();
        if new_sk == current_sk {
            Ok(OperationStatus::Success)
        } else {
            // Stepped onto a different secondary key — end of the
            // duplicate run.  We deliberately do not back the inner
            // cursor up; callers that want to keep iterating should
            // switch to plain Next/Prev.
            Ok(OperationStatus::NotFound)
        }
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
            if let Some(d) = data.data_opt()
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

        let db_config = DatabaseConfig::new()
            .with_allow_create(true)
            .with_transactional(true);
        let primary_db =
            env.open_database(None, "primary", &db_config).unwrap();
        let primary = Arc::new(Mutex::new(primary_db));

        let sec_db_config = DatabaseConfig::new()
            .with_allow_create(true)
            .with_transactional(true)
            .with_sorted_duplicates(true);
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
        _secondary: &SecondaryDatabase,
        key: &[u8],
        value: &[u8],
    ) {
        let pk = DatabaseEntry::from_bytes(key);
        let pv = DatabaseEntry::from_bytes(value);
        // The secondary is registered with the primary via the auto-hook;
        // primary.put() already triggers update_secondary.  Don't call
        // update_secondary manually or the same (sec_key, pri_key) pair
        // will be inserted twice → D6 SecondaryIntegrityException.
        primary.lock().put(&pk, &pv).unwrap();
    }

    #[test]
    fn test_cursor_get_first_last() {
        let (_tmp, _env, primary, secondary) = temp_env_primary_secondary();
        insert_and_index(&primary, &secondary, b"pk2", b"Banana");
        insert_and_index(&primary, &secondary, b"pk1", b"Apple");
        insert_and_index(&primary, &secondary, b"pk3", b"Cherry");

        let mut cursor = secondary.open_cursor(None).unwrap();
        let mut sec_key = DatabaseEntry::new();
        let mut p_key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();

        let status =
            cursor.get_first(&mut sec_key, &mut p_key, &mut data).unwrap();
        assert_eq!(status, OperationStatus::Success);
        assert_eq!(data.data_opt().unwrap(), b"Apple");

        let status =
            cursor.get_last(&mut sec_key, &mut p_key, &mut data).unwrap();
        assert_eq!(status, OperationStatus::Success);
        assert_eq!(data.data_opt().unwrap(), b"Cherry");
    }

    #[test]
    fn test_cursor_get_next() {
        let (_tmp, _env, primary, secondary) = temp_env_primary_secondary();
        insert_and_index(&primary, &secondary, b"pk1", b"Apple");
        insert_and_index(&primary, &secondary, b"pk2", b"Banana");
        insert_and_index(&primary, &secondary, b"pk3", b"Cherry");

        let mut cursor = secondary.open_cursor(None).unwrap();
        let mut sec_key = DatabaseEntry::new();
        let mut p_key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();

        let mut results: Vec<Vec<u8>> = Vec::new();
        let mut status =
            cursor.get_first(&mut sec_key, &mut p_key, &mut data).unwrap();
        while status == OperationStatus::Success {
            results.push(data.data_opt().unwrap().to_vec());
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

        let mut cursor = secondary.open_cursor(None).unwrap();
        let search = DatabaseEntry::from_bytes(b"M");
        let mut p_key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();

        let status =
            cursor.get_search_key(&search, &mut p_key, &mut data).unwrap();
        assert_eq!(status, OperationStatus::Success);
        assert_eq!(data.data_opt().unwrap(), b"Mango");
        assert_eq!(p_key.data_opt().unwrap(), b"pk1");
    }

    #[test]
    fn test_cursor_search_key_not_found() {
        let (_tmp, _env, primary, secondary) = temp_env_primary_secondary();
        insert_and_index(&primary, &secondary, b"pk1", b"Apple");

        let mut cursor = secondary.open_cursor(None).unwrap();
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

        let mut cursor = secondary.open_cursor(None).unwrap();
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
        let mut cursor = secondary.open_cursor(None).unwrap();
        let key = DatabaseEntry::from_bytes(b"key");
        let data = DatabaseEntry::from_bytes(b"data");
        assert!(cursor.put(&key, &data).is_err());
    }

    #[test]
    fn test_cursor_close() {
        let (_tmp, _env, _primary, secondary) = temp_env_primary_secondary();
        let mut cursor = secondary.open_cursor(None).unwrap();
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

        let mut cursor = secondary.open_cursor(None).unwrap();
        let mut sec_key = DatabaseEntry::new();
        let mut p_key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();

        // Position at last
        let status =
            cursor.get_last(&mut sec_key, &mut p_key, &mut data).unwrap();
        assert_eq!(status, OperationStatus::Success);
        assert_eq!(data.data_opt().unwrap(), b"Cherry");

        // Step back to prev
        let status =
            cursor.get_prev(&mut sec_key, &mut p_key, &mut data).unwrap();
        assert_eq!(status, OperationStatus::Success);
        assert_eq!(data.data_opt().unwrap(), b"Banana");

        // Step back again
        let status =
            cursor.get_prev(&mut sec_key, &mut p_key, &mut data).unwrap();
        assert_eq!(status, OperationStatus::Success);
        assert_eq!(data.data_opt().unwrap(), b"Apple");

        // No more prev
        let status =
            cursor.get_prev(&mut sec_key, &mut p_key, &mut data).unwrap();
        assert_eq!(status, OperationStatus::NotFound);
    }

    #[test]
    fn test_cursor_get_current() {
        let (_tmp, _env, primary, secondary) = temp_env_primary_secondary();
        insert_and_index(&primary, &secondary, b"pk1", b"Mango");

        let mut cursor = secondary.open_cursor(None).unwrap();
        let mut sec_key = DatabaseEntry::new();
        let mut p_key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();

        // First positions the cursor
        let status =
            cursor.get_first(&mut sec_key, &mut p_key, &mut data).unwrap();
        assert_eq!(status, OperationStatus::Success);
        assert_eq!(data.data_opt().unwrap(), b"Mango");

        // get_current should return the same record
        let mut sec_key2 = DatabaseEntry::new();
        let mut p_key2 = DatabaseEntry::new();
        let mut data2 = DatabaseEntry::new();
        let status2 =
            cursor.get_current(&mut sec_key2, &mut p_key2, &mut data2).unwrap();
        assert_eq!(status2, OperationStatus::Success);
        assert_eq!(data2.data_opt().unwrap(), b"Mango");
        assert_eq!(p_key2.data_opt(), p_key.data_opt());
    }

    #[test]
    fn test_cursor_get_search_key_range_exact() {
        let (_tmp, _env, primary, secondary) = temp_env_primary_secondary();
        insert_and_index(&primary, &secondary, b"pk1", b"Apple");
        insert_and_index(&primary, &secondary, b"pk2", b"Banana");
        insert_and_index(&primary, &secondary, b"pk3", b"Cherry");

        let mut cursor = secondary.open_cursor(None).unwrap();
        // Search key "B" — exact match for first byte of "Banana"
        let mut search_key = DatabaseEntry::from_bytes(b"B");
        let mut p_key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();

        let status = cursor
            .get_search_key_range(&mut search_key, &mut p_key, &mut data)
            .unwrap();
        assert_eq!(status, OperationStatus::Success);
        assert_eq!(data.data_opt().unwrap(), b"Banana");
    }

    #[test]
    fn test_cursor_get_search_key_range_gte() {
        let (_tmp, _env, primary, secondary) = temp_env_primary_secondary();
        // Insert keys with first bytes: A, C (no B)
        insert_and_index(&primary, &secondary, b"pk1", b"Apple");
        insert_and_index(&primary, &secondary, b"pk3", b"Cherry");

        let mut cursor = secondary.open_cursor(None).unwrap();
        // Search for "B" — no exact match, but "C" (Cherry) is >= "B"
        let mut search_key = DatabaseEntry::from_bytes(b"B");
        let mut p_key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();

        let status = cursor
            .get_search_key_range(&mut search_key, &mut p_key, &mut data)
            .unwrap();
        assert_eq!(status, OperationStatus::Success);
        assert_eq!(data.data_opt().unwrap(), b"Cherry");
    }

    #[test]
    fn test_cursor_get_search_key_range_not_found() {
        let (_tmp, _env, primary, secondary) = temp_env_primary_secondary();
        insert_and_index(&primary, &secondary, b"pk1", b"Apple");

        let mut cursor = secondary.open_cursor(None).unwrap();
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

        let mut cursor = secondary.open_cursor(None).unwrap();
        let mut sec_key = DatabaseEntry::new();
        let mut p_key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();

        // Forward traversal via Next
        let s = cursor.get_first(&mut sec_key, &mut p_key, &mut data).unwrap();
        assert_eq!(s, OperationStatus::Success);
        let first_data = data.data_opt().unwrap().to_vec();

        let s = cursor.get_next(&mut sec_key, &mut p_key, &mut data).unwrap();
        assert_eq!(s, OperationStatus::Success);
        let second_data = data.data_opt().unwrap().to_vec();

        // The two records should differ
        assert_ne!(first_data, second_data);

        // Jump to last
        let s = cursor.get_last(&mut sec_key, &mut p_key, &mut data).unwrap();
        assert_eq!(s, OperationStatus::Success);
        assert_eq!(data.data_opt().unwrap(), b"Durian");

        // Prev from last
        let s = cursor.get_prev(&mut sec_key, &mut p_key, &mut data).unwrap();
        assert_eq!(s, OperationStatus::Success);
        assert_eq!(data.data_opt().unwrap(), b"Cherry");
    }

    #[test]
    fn test_cursor_get_search_key_returns_pkey() {
        let (_tmp, _env, primary, secondary) = temp_env_primary_secondary();
        insert_and_index(&primary, &secondary, b"mypk", b"Kiwi");

        let mut cursor = secondary.open_cursor(None).unwrap();
        let search = DatabaseEntry::from_bytes(b"K"); // first byte of "Kiwi"
        let mut p_key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();

        let status =
            cursor.get_search_key(&search, &mut p_key, &mut data).unwrap();
        assert_eq!(status, OperationStatus::Success);
        assert_eq!(p_key.data_opt().unwrap(), b"mypk");
        assert_eq!(data.data_opt().unwrap(), b"Kiwi");
    }

    #[test]
    fn test_cursor_drop_closes_automatically() {
        // Verify Drop impl doesn't panic even when cursor is still valid
        let (_tmp, _env, _primary, secondary) = temp_env_primary_secondary();
        let cursor = secondary.open_cursor(None).unwrap();
        // Drop without explicit close — should not panic
        drop(cursor);
    }

    #[test]
    fn test_cursor_next_at_end_returns_not_found() {
        let (_tmp, _env, primary, secondary) = temp_env_primary_secondary();
        insert_and_index(&primary, &secondary, b"pk1", b"Apple");

        let mut cursor = secondary.open_cursor(None).unwrap();
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

    // ── traversal: the get_* family ──────────────────────────────────────
    //
    // Every `get_*` method funnels into `get_with_mode`, which does the
    // three-step secondary read (secondary key -> primary key -> primary data).
    // The existing tests cover only the two search modes; first/last/next/prev
    // and the dup-full navigators were untested, and they are the ones a range
    // scan is built out of.
    //
    // The invariant that matters throughout: EVERY successful get must return a
    // consistent TRIPLE. A secondary cursor whose secondary key, primary key
    // and primary data came from different records is the worst failure mode
    // here, because each field looks individually plausible.

    /// Assert the triple is self-consistent: the secondary key must be
    /// derivable from the primary data (first byte, per `FirstByteKeyCreator`),
    /// and the primary data must be what the primary database holds under the
    /// returned primary key.
    fn assert_consistent_triple(
        primary: &Arc<Mutex<Database>>,
        key: &DatabaseEntry,
        p_key: &DatabaseEntry,
        data: &DatabaseEntry,
    ) {
        let sec = key.data_opt().expect("secondary key must be populated");
        let pk = p_key.data_opt().expect("primary key must be populated");
        let d = data.data_opt().expect("primary data must be populated");

        assert_eq!(
            sec,
            &d[..1],
            "the secondary key must be the one the creator derives from THIS \
             record's data, not another record's"
        );
        let held = primary.lock().get(pk).unwrap();
        assert_eq!(
            held.as_deref(),
            Some(d),
            "the primary data must be what the primary holds under the \
             returned primary key"
        );
    }

    #[test]
    fn first_and_last_return_consistent_triples_at_both_ends() {
        let (_t, _env, primary, secondary) = temp_env_primary_secondary();
        for (k, v) in [
            (&b"p1"[..], &b"aaa"[..]),
            (&b"p2"[..], &b"bbb"[..]),
            (&b"p3"[..], &b"ccc"[..]),
        ] {
            insert_and_index(&primary, &secondary, k, v);
        }
        let mut cursor = secondary.open_cursor(None).unwrap();

        let (mut k, mut pk, mut d) =
            (DatabaseEntry::new(), DatabaseEntry::new(), DatabaseEntry::new());
        assert_eq!(
            cursor.get_first(&mut k, &mut pk, &mut d).unwrap(),
            OperationStatus::Success
        );
        assert_eq!(k.data_opt().unwrap(), b"a", "first secondary key is 'a'");
        assert_consistent_triple(&primary, &k, &pk, &d);

        let (mut k, mut pk, mut d) =
            (DatabaseEntry::new(), DatabaseEntry::new(), DatabaseEntry::new());
        assert_eq!(
            cursor.get_last(&mut k, &mut pk, &mut d).unwrap(),
            OperationStatus::Success
        );
        assert_eq!(k.data_opt().unwrap(), b"c", "last secondary key is 'c'");
        assert_consistent_triple(&primary, &k, &pk, &d);
    }

    /// A forward scan must visit every secondary key in order and stop, with
    /// every triple consistent. This is the composition first+next that a range
    /// scan is built from.
    #[test]
    fn a_forward_scan_visits_every_record_in_secondary_key_order() {
        let (_t, _env, primary, secondary) = temp_env_primary_secondary();
        for (k, v) in [
            (&b"p1"[..], &b"aaa"[..]),
            (&b"p2"[..], &b"bbb"[..]),
            (&b"p3"[..], &b"ccc"[..]),
        ] {
            insert_and_index(&primary, &secondary, k, v);
        }
        let mut cursor = secondary.open_cursor(None).unwrap();

        let mut seen = Vec::new();
        let (mut k, mut pk, mut d) =
            (DatabaseEntry::new(), DatabaseEntry::new(), DatabaseEntry::new());
        let mut status = cursor.get_first(&mut k, &mut pk, &mut d).unwrap();
        while status == OperationStatus::Success {
            assert_consistent_triple(&primary, &k, &pk, &d);
            seen.push(k.data_opt().unwrap().to_vec());
            k = DatabaseEntry::new();
            pk = DatabaseEntry::new();
            d = DatabaseEntry::new();
            status = cursor.get_next(&mut k, &mut pk, &mut d).unwrap();
            assert!(seen.len() < 100, "scan did not terminate");
        }
        assert_eq!(
            seen,
            vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()],
            "the forward scan must visit every secondary key, in order"
        );
    }

    /// And a backward scan must visit the same records in reverse. A `prev`
    /// that silently behaved like `next` would pass any test that only checked
    /// the set of records visited.
    #[test]
    fn a_backward_scan_visits_the_same_records_in_reverse() {
        let (_t, _env, primary, secondary) = temp_env_primary_secondary();
        for (k, v) in [
            (&b"p1"[..], &b"aaa"[..]),
            (&b"p2"[..], &b"bbb"[..]),
            (&b"p3"[..], &b"ccc"[..]),
        ] {
            insert_and_index(&primary, &secondary, k, v);
        }
        let mut cursor = secondary.open_cursor(None).unwrap();

        let mut seen = Vec::new();
        let (mut k, mut pk, mut d) =
            (DatabaseEntry::new(), DatabaseEntry::new(), DatabaseEntry::new());
        let mut status = cursor.get_last(&mut k, &mut pk, &mut d).unwrap();
        while status == OperationStatus::Success {
            assert_consistent_triple(&primary, &k, &pk, &d);
            seen.push(k.data_opt().unwrap().to_vec());
            k = DatabaseEntry::new();
            pk = DatabaseEntry::new();
            d = DatabaseEntry::new();
            status = cursor.get_prev(&mut k, &mut pk, &mut d).unwrap();
            assert!(seen.len() < 100, "scan did not terminate");
        }
        assert_eq!(
            seen,
            vec![b"c".to_vec(), b"b".to_vec(), b"a".to_vec()],
            "prev must walk backwards, not forwards"
        );
    }

    /// `get_current` must re-emit the position without advancing, and must
    /// report NotFound before the cursor has been positioned at all.
    #[test]
    fn get_current_re_emits_the_position_without_advancing() {
        let (_t, _env, primary, secondary) = temp_env_primary_secondary();
        insert_and_index(&primary, &secondary, b"p1", b"aaa");
        insert_and_index(&primary, &secondary, b"p2", b"bbb");
        let mut cursor = secondary.open_cursor(None).unwrap();

        let (mut k, mut pk, mut d) =
            (DatabaseEntry::new(), DatabaseEntry::new(), DatabaseEntry::new());
        cursor.get_first(&mut k, &mut pk, &mut d).unwrap();
        let at = k.data_opt().unwrap().to_vec();

        for _ in 0..3 {
            let (mut k2, mut pk2, mut d2) = (
                DatabaseEntry::new(),
                DatabaseEntry::new(),
                DatabaseEntry::new(),
            );
            assert_eq!(
                cursor.get_current(&mut k2, &mut pk2, &mut d2).unwrap(),
                OperationStatus::Success
            );
            assert_eq!(
                k2.data_opt().unwrap(),
                at.as_slice(),
                "get_current must not advance the cursor"
            );
            assert_consistent_triple(&primary, &k2, &pk2, &d2);
        }
    }

    /// Two primary records sharing a secondary key are duplicates of that key.
    /// `get_next_dup_full` must walk within the dup set and stop at its end
    /// rather than spilling into the next secondary key -- that spill is the
    /// natural bug, and it would make a dup scan silently return foreign
    /// records.
    #[test]
    fn dup_navigation_stays_within_the_duplicate_set() {
        let (_t, _env, primary, secondary) = temp_env_primary_secondary();
        // Three records under secondary key "a", one under "b".
        insert_and_index(&primary, &secondary, b"p1", b"a-one");
        insert_and_index(&primary, &secondary, b"p2", b"a-two");
        insert_and_index(&primary, &secondary, b"p3", b"a-three");
        insert_and_index(&primary, &secondary, b"p9", b"b-other");

        let mut cursor = secondary.open_cursor(None).unwrap();
        let (mut pk, mut d) = (DatabaseEntry::new(), DatabaseEntry::new());
        let search = DatabaseEntry::from_bytes(b"a");
        assert_eq!(
            cursor.get_search_key(&search, &mut pk, &mut d).unwrap(),
            OperationStatus::Success
        );

        let mut in_set = 1;
        loop {
            let mut k = DatabaseEntry::new();
            pk = DatabaseEntry::new();
            d = DatabaseEntry::new();
            match cursor.get_next_dup_full(&mut k, &mut pk, &mut d).unwrap() {
                OperationStatus::Success => {
                    assert_eq!(
                        k.data_opt().unwrap(),
                        b"a",
                        "get_next_dup_full must not spill into the next \
                         secondary key"
                    );
                    assert_consistent_triple(&primary, &k, &pk, &d);
                    in_set += 1;
                }
                _ => break,
            }
            assert!(in_set < 20, "dup walk did not terminate");
        }
        assert_eq!(
            in_set, 3,
            "all three duplicates of 'a' must be visited, and only those"
        );
    }

    /// The backward dup navigator must do the same, in reverse.
    #[test]
    fn backward_dup_navigation_also_stays_within_the_set() {
        let (_t, _env, primary, secondary) = temp_env_primary_secondary();
        insert_and_index(&primary, &secondary, b"p1", b"a-one");
        insert_and_index(&primary, &secondary, b"p2", b"a-two");
        insert_and_index(&primary, &secondary, b"z9", b"z-other");

        let mut cursor = secondary.open_cursor(None).unwrap();
        let (mut k, mut pk, mut d) =
            (DatabaseEntry::new(), DatabaseEntry::new(), DatabaseEntry::new());
        // Position on the first dup of "a", advance to the second, then walk
        // back. Recording the forward path lets the backward path be checked
        // against it rather than merely counted.
        let search = DatabaseEntry::from_bytes(b"a");
        cursor.get_search_key(&search, &mut pk, &mut d).unwrap();
        let first_pk = pk.data_opt().unwrap().to_vec();

        assert_eq!(
            cursor.get_next_dup_full(&mut k, &mut pk, &mut d).unwrap(),
            OperationStatus::Success,
            "there are two dups of 'a', so a forward step must succeed"
        );
        assert_eq!(k.data_opt().unwrap(), b"a");
        let second_pk = pk.data_opt().unwrap().to_vec();
        assert_ne!(first_pk, second_pk, "the two dups are distinct records");

        // Step back: must land on the FIRST dup again, still inside the set.
        let (mut k2, mut pk2, mut d2) =
            (DatabaseEntry::new(), DatabaseEntry::new(), DatabaseEntry::new());
        assert_eq!(
            cursor.get_prev_dup_full(&mut k2, &mut pk2, &mut d2).unwrap(),
            OperationStatus::Success
        );
        assert_eq!(
            k2.data_opt().unwrap(),
            b"a",
            "get_prev_dup_full must not spill out of the dup set"
        );
        assert_eq!(
            pk2.data_opt().unwrap(),
            first_pk.as_slice(),
            "stepping back from the second dup must land on the first"
        );

        // One more step back leaves the set, so it must report NotFound rather
        // than spilling into a lower secondary key.
        let (mut k3, mut pk3, mut d3) =
            (DatabaseEntry::new(), DatabaseEntry::new(), DatabaseEntry::new());
        assert_eq!(
            cursor.get_prev_dup_full(&mut k3, &mut pk3, &mut d3).unwrap(),
            OperationStatus::NotFound,
            "walking off the front of the dup set must stop, not spill"
        );
    }

    /// A traversal on an EMPTY secondary must report NotFound from both ends
    /// rather than erroring or returning a garbage triple.
    ///
    /// `get_current` is deliberately different: on an UNPOSITIONED cursor it
    /// returns `OperationNotAllowed`, not `NotFound`. That asymmetry is worth
    /// pinning -- "no record here" and "you never positioned me" are different
    /// caller mistakes, and conflating them would let a caller loop forever on
    /// `get_current` believing the database was empty.
    #[test]
    fn traversal_of_an_empty_secondary_reports_not_found() {
        let (_t, _env, _primary, secondary) = temp_env_primary_secondary();
        let mut cursor = secondary.open_cursor(None).unwrap();
        let (mut k, mut pk, mut d) =
            (DatabaseEntry::new(), DatabaseEntry::new(), DatabaseEntry::new());
        assert_eq!(
            cursor.get_first(&mut k, &mut pk, &mut d).unwrap(),
            OperationStatus::NotFound
        );
        assert_eq!(
            cursor.get_last(&mut k, &mut pk, &mut d).unwrap(),
            OperationStatus::NotFound
        );
        assert!(
            matches!(
                cursor.get_current(&mut k, &mut pk, &mut d),
                Err(NoxuError::OperationNotAllowed(_))
            ),
            "an unpositioned get_current is a caller error, distinct from \
             an empty-database NotFound"
        );
    }

    /// `put` on a secondary cursor must ALWAYS be refused. A secondary index is
    /// derived state; letting a caller write into it directly would desynchronise
    /// it from the primary with no way to detect the drift.
    #[test]
    fn put_through_a_secondary_cursor_is_always_refused() {
        let (_t, _env, primary, secondary) = temp_env_primary_secondary();
        insert_and_index(&primary, &secondary, b"p1", b"aaa");
        let mut cursor = secondary.open_cursor(None).unwrap();

        let key = DatabaseEntry::from_bytes(b"a");
        let data = DatabaseEntry::from_bytes(b"p9");
        assert!(matches!(
            cursor.put(&key, &data),
            Err(NoxuError::OperationNotAllowed(_)),
        ));

        // Even when positioned -- the refusal is unconditional.
        let (mut k, mut pk, mut d) =
            (DatabaseEntry::new(), DatabaseEntry::new(), DatabaseEntry::new());
        cursor.get_first(&mut k, &mut pk, &mut d).unwrap();
        assert!(cursor.put(&key, &data).is_err());
    }

    /// A closed secondary cursor must report itself invalid, and closing must be
    /// tolerated more than once (the Drop glue closes too).
    #[test]
    fn closing_a_secondary_cursor_invalidates_it_and_is_repeatable() {
        let (_t, _env, primary, secondary) = temp_env_primary_secondary();
        insert_and_index(&primary, &secondary, b"p1", b"aaa");
        let mut cursor = secondary.open_cursor(None).unwrap();
        assert!(cursor.is_valid());

        cursor.close().unwrap();
        assert!(!cursor.is_valid());
        cursor.close().unwrap();
        assert!(!cursor.is_valid());
    }
}
