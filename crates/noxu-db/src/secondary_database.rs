//! Secondary database handle.
//!
//! A secondary database is an index over a primary database.  Records are
//! automatically maintained when the primary is written.  Reads via a
//! secondary return primary data; deletes via a secondary delete the
//! corresponding primary record.
//!
//! The mapping of secondary keys to primary records is stored in an ordinary
//! `Database` configured for sorted duplicates.  Records have the form:
//!
//!   key   = secondary_key
//!   value = primary_key (each duplicate of the same secondary key is one
//!                        primary key that maps to it)
//!
//! Many primaries may share a secondary key; the inner index stores them
//! as sorted duplicates of the same secondary key.
//!
//! # v1.6 contract — automatic associate() maintenance
//!
//! When a [`SecondaryDatabase`] is opened against a primary, it is
//! registered on the primary's secondary list.  Every subsequent
//! [`Database::put`] / [`Database::delete`] on the primary
//! automatically updates **all** registered secondaries inside the same
//! transaction (or the synthetic auto-commit transaction the engine
//! allocates when `txn = None`).  Aborting the primary's txn rolls back
//! the secondary updates atomically.
//!
//! [`SecondaryDatabase::update_secondary`] remains as a manual-update
//! escape hatch for callers that want to drive secondary maintenance
//! explicitly (for example, for population from an external feed), but
//! application code that goes through `Database::put` / `Database::delete`
//! no longer has to call it.
//!
//! # v1.6 contract — sorted-dup secondaries
//!
//! Multiple primary records may produce the same secondary key.  The
//! inner index stores `(sec_key, pri_key)` pairs as duplicates of
//! `sec_key`; cursor reads via [`SecondaryCursor`] enumerate them with
//! `Get::SearchKey` + `Get::NextDup` semantics, giving full
//! sorted-duplicate fan-out.
//!
//! The inner secondary database **must** be opened with
//! [`DatabaseConfig::with_sorted_duplicates(true)`]; a non-sorted-dup
//! inner DB causes [`SecondaryDatabase::open`] to return a typed
//! [`NoxuError::IllegalArgument`].
//!
//! # v1.6 contract — foreign-key constraints
//!
//! A `SecondaryConfig::foreign_key_database` registers a foreign-key
//! relationship: every secondary key produced by this index must exist
//! as a primary key in the named foreign DB.  When a record is deleted
//! from the foreign DB, every referring child record is handled per
//! [`ForeignKeyDeleteAction`]:
//!
//! * [`ForeignKeyDeleteAction::Abort`] — the foreign delete fails with
//!   [`NoxuError::ForeignConstraintViolation`].  The transaction is
//!   left in a state the caller can roll back or continue from.
//! * [`ForeignKeyDeleteAction::Cascade`] — every primary referrer is
//!   deleted under the same txn.  Cascade is transitive (cascades
//!   triggered by other cascades follow); cycles are detected and
//!   bailed out with [`NoxuError::ForeignConstraintViolation`].
//! * [`ForeignKeyDeleteAction::Nullify`] — every primary referrer is
//!   updated through the user-supplied
//!   [`ForeignKeyNullifier`] / [`ForeignMultiKeyNullifier`] so the FK
//!   field becomes empty/null.

use crate::cursor::Cursor;
use crate::cursor_config::CursorConfig;
use crate::database::{Database, RegisteredFkReferrer, RegisteredSecondary};
use crate::database_entry::DatabaseEntry;
use crate::error::{NoxuError, Result};
use crate::operation_status::OperationStatus;
use crate::secondary_config::{ForeignKeyDeleteAction, SecondaryConfig};
use crate::secondary_cursor::SecondaryCursor;
use crate::transaction::Transaction;
use noxu_dbi::{CursorImpl, GetMode};
use noxu_sync::Mutex;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// Internal state of a [`SecondaryDatabase`].
///
/// Held behind an `Arc` so the primary database can keep a `Weak<_>`
/// reference for automatic-maintenance and FK cascade fan-out without
/// creating a cycle.  Dropping the `SecondaryDatabase` drops the
/// strong `Arc`; the next time the primary iterates its registry the
/// dangling weak reference is purged.
pub(crate) struct SecondaryState {
    /// The underlying secondary index storage (sec_key -> [pri_key…]).
    pub(crate) inner: Database,
    /// The primary database this index is associated with.
    pub(crate) primary: Arc<Mutex<Database>>,
    /// The secondary configuration (holds key creator callback, etc.).
    pub(crate) config: SecondaryConfig,
    /// Whether this secondary is fully populated (not in incremental mode).
    pub(crate) is_fully_populated: AtomicBool,
    /// Optional FK target — `Some` iff the config registered a
    /// `foreign_key_database`.  The primary that this secondary's
    /// keys reference (i.e. the **parent** in parent → child FK).
    pub(crate) fk_target: Option<Arc<Mutex<Database>>>,
}

/// A secondary (index) database handle.
///
/// Secondary databases are always associated with a primary database.
/// Key characteristics:
///
/// - Direct `put` calls are prohibited; use the primary database instead.
/// - `delete` on a secondary deletes the primary record (and all its
///   secondary index entries).
/// - `get` returns primary record data, not secondary data.
/// - `open_cursor` returns a [`SecondaryCursor`].
///
/// Maintenance happens automatically: every [`Database::put`] /
/// [`Database::delete`] on the primary updates this secondary inside
/// the same transaction.  Manual maintenance via
/// [`update_secondary`](Self::update_secondary) is still supported
/// (for explicit-population workflows) but is no longer required.
///
/// # Sorted-dup contract
///
/// The inner DB **must** be opened with
/// [`DatabaseConfig::with_sorted_duplicates(true)`].  Multiple primary
/// records may produce the same secondary key; they coexist as
/// duplicates of `sec_key`.
///
/// # Example
/// ```ignore
/// use noxu_db::{Database, DatabaseEntry, DatabaseConfig};
/// use noxu_db::secondary_config::{SecondaryConfig, SecondaryKeyCreator};
/// use noxu_db::secondary_database::SecondaryDatabase;
///
/// let inner_cfg = DatabaseConfig::new()
///     .with_allow_create(true)
///     .with_sorted_duplicates(true);
/// let inner = env.open_database(None, "my_index", &inner_cfg)?;
///
/// let sec_config = SecondaryConfig::new()
///     .with_allow_create(true)
///     .with_allow_populate(true)
///     .with_key_creator(Box::new(MyKeyCreator));
///
/// let secondary =
///     SecondaryDatabase::open(primary_db_arc, inner, sec_config)?;
/// ```
pub struct SecondaryDatabase {
    pub(crate) state: Arc<SecondaryState>,
}

impl SecondaryDatabase {
    /// Opens or creates a secondary database associated with `primary`.
    ///
    /// The `secondary_db` argument is an already-opened `Database` that
    /// will serve as the underlying storage.  It **must** have been
    /// opened with [`DatabaseConfig::with_sorted_duplicates(true)`] so
    /// that multiple primaries sharing a secondary key are stored as
    /// duplicates.
    ///
    /// On success the returned handle is registered on the primary's
    /// secondary list; subsequent primary writes drive this index
    /// automatically.
    ///
    /// # Errors
    /// - [`NoxuError::IllegalArgument`] if the configuration is invalid,
    ///   or if `secondary_db` was not opened with
    ///   `with_sorted_duplicates(true)`.
    /// - [`NoxuError::IllegalArgument`] if `config.foreign_key_database`
    ///   names a database that has not been registered via
    ///   [`Self::open_with_foreign_key`] — see that method for the
    ///   FK-aware constructor.
    pub fn open(
        primary: Arc<Mutex<Database>>,
        secondary_db: Database,
        config: SecondaryConfig,
    ) -> Result<Self> {
        Self::open_inner(primary, secondary_db, config, None)
    }

    /// Opens a secondary database with a foreign-key relationship to
    /// `fk_target`.
    ///
    /// `fk_target` must be the `Arc<Mutex<Database>>` for the primary
    /// database whose key space the FK references.  The
    /// `SecondaryConfig` must also set
    /// [`SecondaryConfig::with_foreign_key_database`] to that DB's
    /// name (used purely for diagnostic messages).
    ///
    /// On success this secondary is registered on `fk_target` as a
    /// foreign-key referrer; deletes against `fk_target` will invoke
    /// the configured [`ForeignKeyDeleteAction`].
    pub fn open_with_foreign_key(
        primary: Arc<Mutex<Database>>,
        secondary_db: Database,
        config: SecondaryConfig,
        fk_target: Arc<Mutex<Database>>,
    ) -> Result<Self> {
        Self::open_inner(primary, secondary_db, config, Some(fk_target))
    }

    fn open_inner(
        primary: Arc<Mutex<Database>>,
        secondary_db: Database,
        config: SecondaryConfig,
        fk_target: Option<Arc<Mutex<Database>>>,
    ) -> Result<Self> {
        // Validate the config w.r.t. the primary's read-only flag.
        let primary_read_only = primary.lock().get_config().read_only;
        config
            .validate(primary_read_only)
            .map_err(NoxuError::IllegalArgument)?;

        // Sorted-dup contract: the inner DB must support sorted duplicates.
        if !secondary_db.get_config().sorted_duplicates {
            return Err(NoxuError::IllegalArgument(
                "secondary inner database must be opened with \
                 with_sorted_duplicates(true) — a sec_key may map to many \
                 primary keys (v1.6+)"
                    .to_string(),
            ));
        }

        // FK invariant: if the config requested an FK target, the caller
        // must use `open_with_foreign_key`.  Equally, if `open_inner` was
        // given an `fk_target` but the config has no FK fields set, we
        // refuse — the constructor and the config must agree.
        if config.has_foreign_key_config() && fk_target.is_none() {
            return Err(NoxuError::IllegalArgument(
                "SecondaryConfig sets foreign-key fields but the FK target \
                 Database handle was not supplied; use \
                 SecondaryDatabase::open_with_foreign_key()"
                    .to_string(),
            ));
        }
        if !config.has_foreign_key_config() && fk_target.is_some() {
            return Err(NoxuError::IllegalArgument(
                "open_with_foreign_key() called without any \
                 foreign-key fields set on the SecondaryConfig; either \
                 set foreign_key_database / foreign_key_delete_action / \
                 a nullifier, or use SecondaryDatabase::open()"
                    .to_string(),
            ));
        }

        let state = Arc::new(SecondaryState {
            inner: secondary_db,
            primary: Arc::clone(&primary),
            config,
            is_fully_populated: AtomicBool::new(true),
            fk_target: fk_target.clone(),
        });

        // Populate from the primary if requested and the secondary is
        // empty.  Done before registration so the registration sees a
        // consistent index and also so any error here drops the
        // newly-created Arc cleanly.
        if state.config.allow_populate {
            populate_if_empty(&state)?;
        }

        // Register the secondary on the primary so primary writes drive it.
        primary.lock().__register_secondary(RegisteredSecondary {
            state: Arc::downgrade(&state),
        });

        // If we have an FK target, register this secondary as a referrer
        // on that target's database so deletes there fire our action.
        if let Some(target) = &fk_target {
            target.lock().__register_fk_referrer(RegisteredFkReferrer {
                state: Arc::downgrade(&state),
            });
        }

        Ok(SecondaryDatabase { state })
    }

    // ------------------------------------------------------------------
    // Public API
    // ------------------------------------------------------------------

    /// Returns the database name of the secondary index.
    pub fn get_database_name(&self) -> &str {
        self.state.inner.get_database_name()
    }

    /// Returns the secondary configuration.
    pub fn get_config(&self) -> &SecondaryConfig {
        &self.state.config
    }

    /// Returns whether this handle is open.
    pub fn is_valid(&self) -> bool {
        self.state.inner.is_valid()
    }

    /// Closes the secondary database handle.
    pub fn close(&self) -> Result<()> {
        self.state.inner.close()
    }

    /// Returns the number of records in the secondary index.
    pub fn count(&self) -> Result<u64> {
        self.state.inner.count()
    }

    /// Returns `true` if any record with the given secondary key exists.
    pub fn exists(
        &self,
        txn: Option<&Transaction>,
        key: &DatabaseEntry,
    ) -> Result<bool> {
        let mut data = DatabaseEntry::new();
        let status = self.state.inner.get(txn, key, &mut data)?;
        Ok(status == OperationStatus::Success)
    }

    /// Removes every record from the secondary index, leaving the
    /// associated primary database untouched.  See JE's
    /// `SecondaryDatabase.truncate(...)`.
    pub fn truncate(&self) -> Result<u64> {
        let pre = self.count()?;
        let mut cursor = self.state.inner.open_cursor(None, None)?;
        let mut sec_key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();
        if cursor.get(&mut sec_key, &mut data, crate::get::Get::First, None)?
            != OperationStatus::Success
        {
            return Ok(0);
        }
        loop {
            cursor.delete()?;
            match cursor.get(
                &mut sec_key,
                &mut data,
                crate::get::Get::Next,
                None,
            )? {
                OperationStatus::Success => continue,
                _ => break,
            }
        }
        Ok(pre)
    }

    /// Retrieves a primary record by secondary key.
    ///
    /// In the sorted-dup model, `key` may map to several primaries; this
    /// method returns the **first** one (smallest primary-key in
    /// duplicate order, mirroring JE's `SecondaryDatabase.get()` /
    /// `SecondaryCursor` defaulting to the first dup).  Use
    /// [`SecondaryCursor`] + `Get::SearchKey` / `Get::NextDup` to walk
    /// the full set.
    pub fn get(
        &self,
        txn: Option<&Transaction>,
        key: &DatabaseEntry,
        p_key: &mut DatabaseEntry,
        data: &mut DatabaseEntry,
    ) -> Result<OperationStatus> {
        self.check_open()?;
        self.check_readable()?;

        // Position to the first duplicate via a cursor.  `Database::get`
        // already returns the first duplicate but does not yield the
        // primary key (which IS the duplicate value), so we use a
        // cursor.
        let mut cur = self.state.inner.open_cursor(txn, None)?;
        let mut sk = key.clone();
        let mut pk_entry = DatabaseEntry::new();
        let status = cur.get(
            &mut sk,
            &mut pk_entry,
            crate::get::Get::Search,
            None,
        )?;
        if status != OperationStatus::Success {
            return Ok(OperationStatus::NotFound);
        }
        if let Some(pk) = pk_entry.get_data() {
            p_key.set_data(pk);
        }

        let primary = self.state.primary.lock();
        let pri_status = primary.get(txn, &pk_entry, data)?;
        if pri_status != OperationStatus::Success {
            return Err(NoxuError::SecondaryIntegrityException(format!(
                "Secondary '{}' refers to missing primary key",
                self.get_database_name()
            )));
        }
        Ok(OperationStatus::Success)
    }

    /// Deletes all primary records whose secondary key equals `key`.
    pub fn delete(
        &self,
        txn: Option<&Transaction>,
        key: &DatabaseEntry,
    ) -> Result<OperationStatus> {
        self.check_open()?;

        // Collect every primary key currently bound to `key` first,
        // then drive `Database::delete` for each.  We collect upfront
        // because each primary delete cascades through the maintenance
        // hook and mutates the secondary index — iterating while
        // mutating would be fragile.
        let mut pris: Vec<Vec<u8>> = Vec::new();
        {
            let mut cursor = self.state.inner.open_cursor(txn, None)?;
            let mut sk = key.clone();
            let mut pk_entry = DatabaseEntry::new();
            let st = cursor.get(
                &mut sk,
                &mut pk_entry,
                crate::get::Get::Search,
                None,
            )?;
            if st == OperationStatus::Success {
                if let Some(pk) = pk_entry.get_data() {
                    pris.push(pk.to_vec());
                }
                loop {
                    let mut next_sk = DatabaseEntry::new();
                    let mut next_pk = DatabaseEntry::new();
                    let next_st = cursor.get(
                        &mut next_sk,
                        &mut next_pk,
                        crate::get::Get::NextDup,
                        None,
                    )?;
                    if next_st != OperationStatus::Success {
                        break;
                    }
                    if let Some(pk) = next_pk.get_data() {
                        pris.push(pk.to_vec());
                    }
                }
            }
        }

        if pris.is_empty() {
            return Ok(OperationStatus::NotFound);
        }

        let primary = self.state.primary.lock();
        for pri_key_bytes in pris {
            let pri_key_entry = DatabaseEntry::from_bytes(&pri_key_bytes);
            // Auto-maintenance on the primary will clean up secondary
            // entries (including this one) as part of the delete.
            let _ = primary.delete(txn, &pri_key_entry)?;
        }

        Ok(OperationStatus::Success)
    }

    /// Opens a cursor on the secondary database.
    pub fn open_cursor<'a>(
        &'a self,
        txn: Option<&'a Transaction>,
        config: Option<&CursorConfig>,
    ) -> Result<SecondaryCursor<'a>> {
        self.check_open()?;
        self.check_readable()?;
        SecondaryCursor::new(self, txn, config)
    }

    /// Starts incremental population mode.
    pub fn start_incremental_population(&self) {
        self.state.is_fully_populated.store(false, Ordering::Release);
    }

    /// Ends incremental population mode.
    pub fn end_incremental_population(&self) {
        self.state.is_fully_populated.store(true, Ordering::Release);
    }

    /// Returns whether incremental population is currently enabled.
    pub fn is_incremental_population_enabled(&self) -> bool {
        !self.state.is_fully_populated.load(Ordering::Acquire)
    }

    // ------------------------------------------------------------------
    // Manual-update API (escape hatch for explicit population)
    // ------------------------------------------------------------------

    /// Updates the secondary index when a primary record is inserted,
    /// updated, or deleted.
    ///
    /// In v1.6 this is no longer required for normal use:
    /// [`Database::put`] / [`Database::delete`] drive every registered
    /// secondary automatically.  The method is preserved as an escape
    /// hatch for explicit-population workflows (e.g. bulk import that
    /// constructs the secondary index out of band) and for backward
    /// compatibility.
    pub fn update_secondary(
        &self,
        txn: Option<&Transaction>,
        pri_key: &DatabaseEntry,
        old_data: Option<&DatabaseEntry>,
        new_data: Option<&DatabaseEntry>,
    ) -> Result<()> {
        update_secondary_impl(&self.state, txn, pri_key, old_data, new_data)
    }

    // ------------------------------------------------------------------
    // Internal helpers used by SecondaryCursor / JoinCursor.
    // ------------------------------------------------------------------

    pub(crate) fn delete_all_for_primary(
        &self,
        txn: Option<&Transaction>,
        pri_key: &DatabaseEntry,
        old_data: Option<&DatabaseEntry>,
    ) -> Result<()> {
        update_secondary_impl(&self.state, txn, pri_key, old_data, None)
    }

    pub(crate) fn inner_db(&self) -> &Database {
        &self.state.inner
    }

    pub(crate) fn primary_db(&self) -> &Arc<Mutex<Database>> {
        &self.state.primary
    }

    // ------------------------------------------------------------------
    // Private helpers
    // ------------------------------------------------------------------

    fn check_open(&self) -> Result<()> {
        if !self.state.inner.is_valid() {
            return Err(NoxuError::DatabaseClosed);
        }
        Ok(())
    }

    fn check_readable(&self) -> Result<()> {
        if !self.state.is_fully_populated.load(Ordering::Acquire) {
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

// ─────────────────────────────────────────────────────────────────────
// Free-functions operating on `SecondaryState` directly.
//
// They are free-functions rather than methods on `SecondaryDatabase`
// because the primary's automatic-maintenance hook holds a
// `Weak<SecondaryState>`, not a `&SecondaryDatabase`.
// ─────────────────────────────────────────────────────────────────────

/// Updates the secondary index for a single (pri_key, old, new) triple.
///
/// This is the workhorse of both the manual-update path
/// ([`SecondaryDatabase::update_secondary`]) and the automatic
/// maintenance path triggered by `Database::put` / `Database::delete`.
pub(crate) fn update_secondary_impl(
    state: &SecondaryState,
    txn: Option<&Transaction>,
    pri_key: &DatabaseEntry,
    old_data: Option<&DatabaseEntry>,
    new_data: Option<&DatabaseEntry>,
) -> Result<()> {
    let key_creator = &state.config.key_creator;
    let multi_key_creator = &state.config.multi_key_creator;

    if old_data.is_none() && new_data.is_none() {
        return Ok(());
    }

    if let Some(creator) = key_creator {
        let old_sec_key = old_data.and_then(|od| {
            let mut sk = DatabaseEntry::new();
            if creator.create_secondary_key(&state.inner, pri_key, od, &mut sk)
            {
                Some(sk)
            } else {
                None
            }
        });

        let new_sec_key = new_data.and_then(|nd| {
            let mut sk = DatabaseEntry::new();
            if creator.create_secondary_key(&state.inner, pri_key, nd, &mut sk)
            {
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
            delete_sec_pair(state, txn, old_sec_key.as_ref().unwrap(), pri_key)?;
        }
        if do_insert {
            // FK pre-check: every new sec_key must exist as a primary
            // key in the FK target.
            if let Some(target) = &state.fk_target {
                check_fk_target(target, new_sec_key.as_ref().unwrap(), txn)?;
            }
            insert_sec_pair(state, txn, new_sec_key.as_ref().unwrap(), pri_key)?;
        }
    } else if let Some(multi_creator) = multi_key_creator {
        let old_keys: Vec<DatabaseEntry> = if let Some(od) = old_data {
            let mut keys = Vec::new();
            multi_creator.create_secondary_keys(
                &state.inner,
                pri_key,
                od,
                &mut keys,
            );
            keys
        } else {
            Vec::new()
        };

        let new_keys: Vec<DatabaseEntry> = if let Some(nd) = new_data {
            let mut keys = Vec::new();
            multi_creator.create_secondary_keys(
                &state.inner,
                pri_key,
                nd,
                &mut keys,
            );
            keys
        } else {
            Vec::new()
        };

        for old_key in &old_keys {
            if !new_keys.contains(old_key) {
                delete_sec_pair(state, txn, old_key, pri_key)?;
            }
        }
        for new_key in &new_keys {
            if !old_keys.contains(new_key) {
                if let Some(target) = &state.fk_target {
                    check_fk_target(target, new_key, txn)?;
                }
                insert_sec_pair(state, txn, new_key, pri_key)?;
            }
        }
    }

    Ok(())
}

/// Inserts a `(sec_key, pri_key)` pair into the inner sorted-dup index.
///
/// Uses [`Put::NoOverwrite`], which on a sorted-dup database fails iff
/// the exact `(sec_key, pri_key)` pair already exists — so an
/// idempotent re-insert is a clean no-op rather than a collision.
fn insert_sec_pair(
    state: &SecondaryState,
    txn: Option<&Transaction>,
    sec_key: &DatabaseEntry,
    pri_key: &DatabaseEntry,
) -> Result<()> {
    let mut cursor = state.inner.open_cursor(txn, None)?;
    let status = cursor
        .put(sec_key, pri_key, crate::put::Put::NoOverwrite)
        .map_err(|e| NoxuError::OperationNotAllowed(e.to_string()))?;
    match status {
        OperationStatus::Success | OperationStatus::KeyExists => Ok(()),
        other => Err(NoxuError::OperationNotAllowed(format!(
            "unexpected put status from secondary index insert: {other:?}"
        ))),
    }
}

/// Deletes a single `(sec_key, pri_key)` pair from the inner sorted-dup
/// index.  Walks the duplicates of `sec_key` until it finds the one
/// whose data equals `pri_key`, then deletes that exact slot.
fn delete_sec_pair(
    state: &SecondaryState,
    txn: Option<&Transaction>,
    sec_key: &DatabaseEntry,
    pri_key: &DatabaseEntry,
) -> Result<()> {
    let mut cursor = state.inner.open_cursor(txn, None)?;
    let mut sk = sec_key.clone();
    let mut data = pri_key.clone();
    // Search for the exact (sec_key, pri_key) tuple — sorted-dup DBs
    // expose this via Get::SearchBoth.
    let st = cursor
        .get(&mut sk, &mut data, crate::get::Get::SearchBoth, None)
        .map_err(|e| NoxuError::OperationNotAllowed(e.to_string()))?;
    if st == OperationStatus::Success {
        cursor.delete()?;
    }
    // If not found, the secondary may already have been cleaned up; ignore.
    Ok(())
}

/// FK target probe: verifies that `sec_key` (the FK reference value)
/// exists as a primary key in `target`.  Errors with
/// [`NoxuError::ForeignConstraintViolation`] if not present.
fn check_fk_target(
    target: &Arc<Mutex<Database>>,
    sec_key: &DatabaseEntry,
    txn: Option<&Transaction>,
) -> Result<()> {
    let target = target.lock();
    let mut probe = DatabaseEntry::new();
    let st = target.get(txn, sec_key, &mut probe)?;
    if st != OperationStatus::Success {
        return Err(NoxuError::ForeignConstraintViolation(format!(
            "foreign-key reference does not exist in target database '{}'",
            target.get_database_name()
        )));
    }
    Ok(())
}

/// Populates the secondary index from the primary if the secondary is
/// empty.  Mirrors JE's `SecondaryDatabase.init` population logic.
fn populate_if_empty(state: &SecondaryState) -> Result<()> {
    if state.inner.count()? > 0 {
        return Ok(());
    }
    let primary = state.primary.lock();
    populate_from_primary_scan(state, &primary)?;
    Ok(())
}

fn populate_from_primary_scan(
    state: &SecondaryState,
    primary: &Database,
) -> Result<()> {
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

        if let Some(creator) = &state.config.key_creator {
            let mut sec_key = DatabaseEntry::new();
            if creator.create_secondary_key(
                &state.inner,
                &pri_key,
                &pri_data,
                &mut sec_key,
            ) {
                insert_sec_pair(state, None, &sec_key, &pri_key)?;
            }
        } else if let Some(multi) = &state.config.multi_key_creator {
            let mut keys = Vec::new();
            multi.create_secondary_keys(
                &state.inner,
                &pri_key,
                &pri_data,
                &mut keys,
            );
            for sk in keys {
                insert_sec_pair(state, None, &sk, &pri_key)?;
            }
        }

        first_status = cursor
            .retrieve_next(GetMode::Next)
            .map_err(|e| NoxuError::OperationNotAllowed(e.to_string()))?;
    }
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────
// FK delete-watcher entry points (called from `Database::delete`).
// ─────────────────────────────────────────────────────────────────────

/// Per-action behaviour applied by [`crate::database::Database::delete`]
/// when a primary that has registered FK referrers is being deleted.
///
/// Returns the list of `(referring_primary, action)` work items the
/// caller's delete loop needs to execute under the same transaction.
pub(crate) struct FkDeleteAction<'a> {
    pub state: &'a SecondaryState,
    /// The key being deleted on the FK target.
    pub deleted_key: &'a DatabaseEntry,
    /// The transaction the delete is running under.
    pub txn: Option<&'a Transaction>,
}

/// Output of a single FK-referrer's plan stage: the set of primary
/// records (in *this referrer's* primary database) that need to be
/// deleted, nullified, or refused.
pub(crate) enum FkDeletePlan {
    /// The Abort action: the delete must fail with this error.
    Abort(String),
    /// The Cascade action: the listed `(child_primary, primary_key)`
    /// pairs must be deleted.  Each pair is `Arc<Mutex<Database>>` so
    /// the executor can drive the child's `Database::delete` (which
    /// will recursively trigger any cascades chained from there).
    Cascade(Vec<(Arc<Mutex<Database>>, Vec<u8>)>),
    /// The Nullify action: the listed `(child_primary, primary_key,
    /// new_data)` triples must be re-put on the child primary.
    Nullify(Vec<(Arc<Mutex<Database>>, Vec<u8>, Vec<u8>)>),
}

impl<'a> FkDeleteAction<'a> {
    /// Plans the FK action for a single referrer.  Reads from the
    /// referrer's secondary index (under `txn`) to find every child
    /// primary key bound to `deleted_key`.
    pub(crate) fn plan(&self) -> Result<FkDeletePlan> {
        // Walk the referrer's inner secondary index for every duplicate
        // of `deleted_key` — those are the children that point at the
        // FK-target record.
        let mut child_pris: Vec<Vec<u8>> = Vec::new();
        {
            let mut cur = self.state.inner.open_cursor(self.txn, None)?;
            let mut sk = self.deleted_key.clone();
            let mut pk_entry = DatabaseEntry::new();
            let st = cur.get(
                &mut sk,
                &mut pk_entry,
                crate::get::Get::Search,
                None,
            )?;
            if st == OperationStatus::Success {
                if let Some(pk) = pk_entry.get_data() {
                    child_pris.push(pk.to_vec());
                }
                loop {
                    let mut next_sk = DatabaseEntry::new();
                    let mut next_pk = DatabaseEntry::new();
                    let next_st = cur.get(
                        &mut next_sk,
                        &mut next_pk,
                        crate::get::Get::NextDup,
                        None,
                    )?;
                    if next_st != OperationStatus::Success {
                        break;
                    }
                    if let Some(pk) = next_pk.get_data() {
                        child_pris.push(pk.to_vec());
                    }
                }
            }
        }

        match self.state.config.foreign_key_delete_action {
            ForeignKeyDeleteAction::Abort => {
                if child_pris.is_empty() {
                    return Ok(FkDeletePlan::Cascade(Vec::new()));
                }
                let key_hex = self
                    .deleted_key
                    .get_data()
                    .map(|b| {
                        b.iter().map(|b| format!("{b:02x}")).collect::<String>()
                    })
                    .unwrap_or_default();
                Ok(FkDeletePlan::Abort(format!(
                    "delete of foreign-key 0x{} would orphan {} record(s) in \
                     secondary '{}' (action = Abort)",
                    key_hex,
                    child_pris.len(),
                    self.state.inner.get_database_name()
                )))
            }
            ForeignKeyDeleteAction::Cascade => Ok(FkDeletePlan::Cascade(
                child_pris
                    .into_iter()
                    .map(|pk| (Arc::clone(&self.state.primary), pk))
                    .collect(),
            )),
            ForeignKeyDeleteAction::Nullify => {
                let mut out: Vec<(Arc<Mutex<Database>>, Vec<u8>, Vec<u8>)> =
                    Vec::new();
                let primary = self.state.primary.lock();
                for pk in child_pris {
                    let pk_e = DatabaseEntry::from_bytes(&pk);
                    let mut data = DatabaseEntry::new();
                    let st = primary.get(self.txn, &pk_e, &mut data)?;
                    if st != OperationStatus::Success {
                        // Already gone — skip silently.
                        continue;
                    }
                    let new_data = nullify_for(self.state, &pk_e, &data)?;
                    out.push((
                        Arc::clone(&self.state.primary),
                        pk,
                        new_data
                            .get_data()
                            .map(|b| b.to_vec())
                            .unwrap_or_default(),
                    ));
                }
                Ok(FkDeletePlan::Nullify(out))
            }
        }
    }
}

/// Calls the user-supplied nullifier (single or multi) and returns the
/// resulting `DatabaseEntry`.  The nullifier mutates a clone so the
/// original primary record is untouched until the caller re-puts.
fn nullify_for(
    state: &SecondaryState,
    pri_key: &DatabaseEntry,
    pri_data: &DatabaseEntry,
) -> Result<DatabaseEntry> {
    let mut data = pri_data.clone();
    if let Some(nul) = &state.config.foreign_key_nullifier {
        nul.nullify_foreign_key(&state.inner, &mut data);
    } else if let Some(nul) = &state.config.foreign_multi_key_nullifier {
        // We need to know which secondary key to nullify; recompute it
        // from the OLD primary data via the multi-key creator and pass
        // each one in turn.
        if let Some(multi) = &state.config.multi_key_creator {
            let mut keys = Vec::new();
            multi.create_secondary_keys(
                &state.inner,
                pri_key,
                pri_data,
                &mut keys,
            );
            for sk in keys {
                nul.nullify_foreign_key(&state.inner, pri_key, &mut data, &sk);
            }
        }
    } else {
        return Err(NoxuError::IllegalArgument(
            "ForeignKeyDeleteAction::Nullify requires \
             foreign_key_nullifier or foreign_multi_key_nullifier"
                .to_string(),
        ));
    }
    Ok(data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database_config::DatabaseConfig;
    use crate::environment::Environment;
    use crate::environment_config::EnvironmentConfig;
    use crate::secondary_config::{SecondaryConfig, SecondaryKeyCreator};
    use tempfile::TempDir;

    /// First-byte secondary key.
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

    fn open_inner_dup_db(env: &Environment, name: &str) -> Database {
        env.open_database(
            None,
            name,
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_sorted_duplicates(true),
        )
        .unwrap()
    }

    fn open_secondary(
        primary: Arc<Mutex<Database>>,
        env: &Environment,
        name: &str,
    ) -> SecondaryDatabase {
        let sec_db = open_inner_dup_db(env, name);
        let sec_config = SecondaryConfig::new()
            .with_allow_create(true)
            .with_sorted_duplicates(true)
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

    /// v1.6: SecondaryDatabase::open must reject a non-sorted-dup inner DB.
    #[test]
    fn test_open_rejects_non_sorted_dup_inner() {
        let (_tmp, env) = temp_env();
        let primary = Arc::new(Mutex::new(open_primary(&env, "primary")));
        let inner = env
            .open_database(
                None,
                "secondary",
                &DatabaseConfig::new().with_allow_create(true),
            )
            .unwrap();
        let cfg = SecondaryConfig::new()
            .with_allow_create(true)
            .with_key_creator(Box::new(FirstByteKeyCreator));
        let result = SecondaryDatabase::open(Arc::clone(&primary), inner, cfg);
        match result {
            Err(NoxuError::IllegalArgument(msg)) => {
                assert!(
                    msg.contains("sorted_duplicates"),
                    "expected sorted_duplicates wording: {msg}"
                );
            }
            Ok(_) => panic!("expected IllegalArgument"),
            Err(e) => panic!("expected IllegalArgument, got {e:?}"),
        }
    }

    #[test]
    fn test_put_primary_auto_updates_secondary() {
        let (_tmp, env) = temp_env();
        let primary = Arc::new(Mutex::new(open_primary(&env, "primary")));
        let secondary = open_secondary(Arc::clone(&primary), &env, "secondary");

        // No manual update_secondary call: Database::put drives the
        // registered secondary automatically.
        let pri_key = DatabaseEntry::from_bytes(b"pk1");
        let pri_data = DatabaseEntry::from_bytes(b"Avalon");
        primary.lock().put(None, &pri_key, &pri_data).unwrap();

        let sec_key = DatabaseEntry::from_bytes(b"A");
        let mut p_key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();
        let status =
            secondary.get(None, &sec_key, &mut p_key, &mut data).unwrap();

        assert_eq!(status, OperationStatus::Success);
        assert_eq!(p_key.get_data().unwrap(), b"pk1");
        assert_eq!(data.get_data().unwrap(), b"Avalon");
    }

    /// v1.6: many-to-one — five primaries with the same secondary-key
    /// first byte coexist as duplicates.
    #[test]
    fn test_many_to_one_secondary_via_auto_maintenance() {
        let (_tmp, env) = temp_env();
        let primary = Arc::new(Mutex::new(open_primary(&env, "primary")));
        let secondary = open_secondary(Arc::clone(&primary), &env, "secondary");

        let entries: &[(&[u8], &[u8])] = &[
            (b"pk1", b"Apple"),
            (b"pk2", b"Apricot"),
            (b"pk3", b"Avocado"),
            (b"pk4", b"Almond"),
            (b"pk5", b"Anchovy"),
        ];
        for &(pk, val) in entries {
            primary
                .lock()
                .put(
                    None,
                    &DatabaseEntry::from_bytes(pk),
                    &DatabaseEntry::from_bytes(val),
                )
                .unwrap();
        }

        // Iterate every (pk, val) bound to sec_key 'A' via the cursor.
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

        // Walk every duplicate via get_next_dup.
        let mut counted: Vec<Vec<u8>> =
            vec![p_key.get_data().unwrap().to_vec()];
        loop {
            let mut sk = DatabaseEntry::new();
            let mut pk = DatabaseEntry::new();
            let mut d = DatabaseEntry::new();
            match cursor.get_next_dup(&mut sk, &mut pk, &mut d).unwrap() {
                OperationStatus::Success => {
                    counted.push(pk.get_data().unwrap().to_vec());
                }
                _ => break,
            }
        }
        counted.sort();
        let mut expected: Vec<Vec<u8>> =
            entries.iter().map(|(pk, _)| pk.to_vec()).collect();
        expected.sort();
        assert_eq!(counted, expected);
    }

    /// v1.6: deleting one primary leaves the others bound to the
    /// shared secondary key intact.
    #[test]
    fn test_many_to_one_delete_one_keeps_rest() {
        let (_tmp, env) = temp_env();
        let primary = Arc::new(Mutex::new(open_primary(&env, "primary")));
        let secondary = open_secondary(Arc::clone(&primary), &env, "secondary");

        for &(pk, val) in &[
            (&b"pk1"[..], &b"Apple"[..]),
            (&b"pk2"[..], &b"Apricot"[..]),
            (&b"pk3"[..], &b"Avocado"[..]),
        ] {
            primary
                .lock()
                .put(
                    None,
                    &DatabaseEntry::from_bytes(pk),
                    &DatabaseEntry::from_bytes(val),
                )
                .unwrap();
        }

        // Delete pk2.
        primary.lock().delete(None, &DatabaseEntry::from_bytes(b"pk2")).unwrap();

        // pk1, pk3 still indexed under 'A'; pk2 is gone.
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
        let mut found: Vec<Vec<u8>> =
            vec![p_key.get_data().unwrap().to_vec()];
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
        assert_eq!(found, vec![b"pk1".to_vec(), b"pk3".to_vec()]);
    }

    #[test]
    fn test_get_by_secondary_key() {
        let (_tmp, env) = temp_env();
        let primary = Arc::new(Mutex::new(open_primary(&env, "primary")));
        let secondary = open_secondary(Arc::clone(&primary), &env, "secondary");

        let records: &[(&[u8], &[u8])] =
            &[(b"pk1", b"Apple"), (b"pk2", b"Banana"), (b"pk3", b"Cherry")];
        for (k, v) in records {
            let pk = DatabaseEntry::from_bytes(k);
            let pv = DatabaseEntry::from_bytes(v);
            primary.lock().put(None, &pk, &pv).unwrap();
        }

        let sec_key = DatabaseEntry::from_bytes(b"B");
        let mut p_key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();
        let status =
            secondary.get(None, &sec_key, &mut p_key, &mut data).unwrap();

        assert_eq!(status, OperationStatus::Success);
        assert_eq!(data.get_data().unwrap(), b"Banana");

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
        primary.lock().put(None, &pri_key, &pri_data).unwrap();

        let sec_key = DatabaseEntry::from_bytes(b"C");
        let status = secondary.delete(None, &sec_key).unwrap();
        assert_eq!(status, OperationStatus::Success);

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

        primary.lock().put(None, &pri_key, &old_data).unwrap();
        primary.lock().put(None, &pri_key, &new_data).unwrap();

        let old_sec = DatabaseEntry::from_bytes(b"M");
        let mut pk = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();
        let status = secondary.get(None, &old_sec, &mut pk, &mut data).unwrap();
        assert_eq!(status, OperationStatus::NotFound);

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

        let records: &[(&[u8], &[u8])] =
            &[(b"pk1", b"Banana"), (b"pk2", b"Cherry"), (b"pk3", b"Apple")];
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

        let sec_db = open_inner_dup_db(&env, "secondary_pop");
        let sec_config = SecondaryConfig::new()
            .with_allow_create(true)
            .with_allow_populate(true)
            .with_sorted_duplicates(true)
            .with_key_creator(Box::new(FirstByteKeyCreator));
        let secondary =
            SecondaryDatabase::open(Arc::clone(&primary), sec_db, sec_config)
                .unwrap();

        let sec_key_g = DatabaseEntry::from_bytes(b"G");
        let mut pk = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();
        let status =
            secondary.get(None, &sec_key_g, &mut pk, &mut data).unwrap();
        assert_eq!(status, OperationStatus::Success);
        assert_eq!(data.get_data().unwrap(), b"Grape");
    }

    #[test]
    fn test_count_exists_truncate_round_trip() {
        let (_tmp, env) = temp_env();
        let primary = Arc::new(Mutex::new(open_primary(&env, "pri")));
        let secondary = open_secondary(Arc::clone(&primary), &env, "sec");

        assert_eq!(secondary.count().unwrap(), 0);
        assert!(
            !secondary.exists(None, &DatabaseEntry::from_bytes(b"A")).unwrap()
        );

        for (pk, pv) in &[
            (&b"pk1"[..], &b"Apple"[..]),
            (&b"pk2"[..], &b"Banana"[..]),
            (&b"pk3"[..], &b"Cherry"[..]),
        ] {
            let pk_e = DatabaseEntry::from_bytes(pk);
            let pv_e = DatabaseEntry::from_bytes(pv);
            primary.lock().put(None, &pk_e, &pv_e).unwrap();
        }

        assert_eq!(secondary.count().unwrap(), 3);
        assert!(
            secondary.exists(None, &DatabaseEntry::from_bytes(b"A")).unwrap()
        );
        assert!(
            secondary.exists(None, &DatabaseEntry::from_bytes(b"C")).unwrap()
        );
        assert!(
            !secondary.exists(None, &DatabaseEntry::from_bytes(b"Z")).unwrap()
        );

        let removed = secondary.truncate().unwrap();
        assert_eq!(removed, 3);
        assert_eq!(secondary.count().unwrap(), 0);
    }
}
