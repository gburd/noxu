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
//!
//! # Atomicity with the primary write (Sprint 4½)
//!
//! [`SecondaryDatabase::update_secondary`] takes an explicit
//! `Option<&Transaction>` parameter that is forwarded to every
//! [`Database`] operation it performs against the inner secondary index.
//! When the caller threads the *same* `txn` through
//! [`Database::put`] / [`Database::delete`] **and**
//! [`SecondaryDatabase::update_secondary`], the primary write and the
//! secondary index update are atomic — committing or aborting the txn
//! commits or rolls back **both** sides together.  See
//! `docs/src/transactions/secondary-with-txn.md` for the canonical
//! pattern.
//!
//! Pre-Sprint-4½ (v1.4 / v1.5.0-rc1 / v1.5.0-rc2) `update_secondary` ran
//! auto-committed regardless of any caller transaction, leaving a
//! partial-atomicity gap (see audit Theme 2 / finding F5): an aborted
//! primary `put` could leave the secondary entry behind on disk.  The
//! gap is closed for the manual-update pattern.  Automatic
//! `associate()`-style maintenance — where `Database::put` itself
//! drives all attached secondaries inside the same txn — remains v1.6
//! work.
//!
//! # v1.5 limitations
//!
//! See [`docs/src/internal/v1.5-decisions-2026-05.md`].
//!
//! - **Decision 1B** — v1.5 secondaries are honestly **one-to-one**: a given
//!   secondary key may map to at most one primary key.  Two distinct
//!   primaries that produce the same secondary key cause the second
//!   `update_secondary` (or `populate_if_empty`) to fail with a typed
//!   [`NoxuError::Unsupported`] (closes audit finding C4).  Sorted-dup
//!   secondaries are planned for v1.6.
//! - **Decision 2C** — foreign-key constraints are not enforced in v1.5.
//!   [`SecondaryDatabase::open`] rejects any [`SecondaryConfig`] whose
//!   foreign-key fields are set with [`NoxuError::Unsupported`] (closes
//!   audit findings C2, F1, F16).  Full FK support is planned for v1.6.
//! - **Automatic secondary maintenance** is not implemented in v1.5;
//!   callers must invoke `update_secondary` manually after each primary
//!   `put` / `delete` (planned for v1.6).

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
/// # v1.5 limitations
///
/// - **One-to-one only** (Decision 1B): a given secondary key may map to
///   at most one primary record.  Sorted-dup secondaries are planned for
///   v1.6.  Two distinct primaries that produce the same secondary key
///   cause the second `update_secondary` to fail with
///   [`NoxuError::Unsupported`].
/// - **Foreign-key constraints not enforced** (Decision 2C):
///   [`SecondaryDatabase::open`] rejects [`SecondaryConfig`]s whose
///   foreign-key fields are set.  Full FK support is planned for v1.6.
/// - **No automatic maintenance**: callers manually invoke
///   [`update_secondary`](Self::update_secondary) after each primary
///   `put` / `delete`.  An automatic `associate()`-style hook is planned
///   for v1.6.
///
/// # Atomicity with the primary write
///
/// As of v1.5 (Sprint 4½) `update_secondary` participates in the
/// caller's transaction when one is supplied.  Threading the same
/// `txn` through both [`Database::put`] and
/// [`update_secondary`](Self::update_secondary) makes the primary +
/// secondary update **atomic**: aborting the txn rolls both back,
/// committing the txn persists both.  Passing `None` runs each call
/// auto-committed, which restores the v1.4 behaviour and is acceptable
/// when the caller does not need cross-database atomicity.
///
/// See `docs/src/internal/v1.5-decisions-2026-05.md` and
/// `docs/src/transactions/secondary-with-txn.md`.
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
    /// - [`NoxuError::IllegalArgument`] if the configuration is invalid.
    /// - [`NoxuError::Unsupported`] if the configuration sets any foreign-key
    ///   constraint field (`foreign_key_database`,
    ///   `foreign_key_delete_action != Abort`, `foreign_key_nullifier`, or
    ///   `foreign_multi_key_nullifier`).  v1.5 does not enforce FK
    ///   constraints; full FK support is planned for v1.6 — see Decision 2C
    ///   in `docs/src/internal/v1.5-decisions-2026-05.md` (closes audit
    ///   findings C2 / F1 / F16).
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

        // Decision 2C: reject FK configurations in v1.5.  The fields are
        // accepted by the builder for forward source compatibility (v1.6
        // will honour them) but the runtime cannot enforce them today, so
        // we surface a typed error at open time rather than silently
        // ignoring user configuration.  Closes audit findings C2 / F1 /
        // F16.
        if config.has_foreign_key_config() {
            return Err(NoxuError::Unsupported(
                "foreign-key constraints are not supported in v1.5; clear \
                 SecondaryConfig.foreign_key_database / \
                 foreign_key_delete_action / foreign_key_nullifier / \
                 foreign_multi_key_nullifier (planned for v1.6)"
                    .to_string(),
            ));
        }

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

    /// Returns the number of records in the secondary index.
    ///
    /// Equivalent to `Database::count` on the underlying inner index
    /// database; included on `SecondaryDatabase` for symmetry with JE's
    /// `SecondaryDatabase.count()` method.  See Wave 1C audit cleanup
    /// (secondary-join “missing count/exists/truncate” Low).
    ///
    /// # Errors
    /// Returns [`NoxuError::DatabaseClosed`] if the secondary handle has
    /// been closed.
    pub fn count(&self) -> Result<u64> {
        self.inner.count()
    }

    /// Returns `true` if any record with the given secondary key exists.
    ///
    /// This avoids the cost of reading the primary record — unlike
    /// [`Self::get`], which traverses the secondary, then the primary
    /// database.  Useful for membership probes inside hot paths.
    ///
    /// # Errors
    /// Propagates any error from the underlying secondary lookup.
    pub fn exists(
        &self,
        txn: Option<&Transaction>,
        key: &DatabaseEntry,
    ) -> Result<bool> {
        let mut data = DatabaseEntry::new();
        let status = self.inner.get(txn, key, &mut data)?;
        Ok(status == OperationStatus::Success)
    }

    /// Removes every record from the secondary index, leaving the
    /// associated primary database untouched.
    ///
    /// **Caveat.** Truncating a secondary index without re-running
    /// `populate_if_empty` (or replaying the primary-side updates)
    /// leaves the secondary in a state that is not consistent with the
    /// primary.  Most callers should drop the secondary's primary keys
    /// via [`Database::truncate_database`] on the inner DB or repopulate
    /// the index afterwards.  Returned for symmetry with JE's
    /// `SecondaryDatabase.truncate(...)`.
    ///
    /// Returns the number of records that were in the index before the
    /// truncate.  See Wave 1C audit cleanup (secondary-join “missing
    /// count/exists/truncate” Low).
    ///
    /// # Errors
    /// Returns [`NoxuError::DatabaseClosed`] if the secondary handle has
    /// been closed, or any error returned by the underlying delete
    /// loop.
    pub fn truncate(&self) -> Result<u64> {
        let pre = self.count()?;
        // Walk every (sec_key, pri_key) pair via a primary-table-style
        // scan and delete each.  The inner index is an ordinary
        // Database, so this is just a cursor scan + delete.
        let mut cursor = self.inner.open_cursor(None, None)?;
        let mut sec_key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();
        // get_first returns NotFound if the index is empty.
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

        // Use a secondary cursor (under the caller's txn so the scan
        // participates in the user's transaction) to iterate all
        // duplicates of the secondary key.
        let mut sec_cursor = self.open_cursor_internal(txn)?;
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
            //    UpdateSecondaryOnDelete calls updateSecondary.  Sprint 4½
            //    forwards `txn` so the cleanup is atomic with the primary
            //    delete below.
            let old_data = data.clone();
            self.delete_all_for_primary(txn, &pri_key_entry, Some(&old_data))?;

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
    /// When `txn` is `Some(_)`, the inner cursor over the secondary
    /// index participates in the supplied transaction — reads acquire
    /// shared locks via the txn's locker and writes acquire exclusive
    /// locks tracked by the txn.  Wave 1B (audit F5 follow-up) extends
    /// this to the *primary* lookups and the
    /// [`SecondaryCursor::delete`] cascade as well: the cursor stores
    /// the txn handle and forwards it to every primary `get` /
    /// `delete` and to `delete_all_for_primary`.  Aborting the txn
    /// rolls back **both** the secondary entry and the primary record
    /// removed by `SecondaryCursor::delete` (and every secondary
    /// cleanup it triggers).  When `txn` is `None`, every operation
    /// runs auto-committed, matching the v1.4 behaviour.
    ///
    /// `config` is forwarded to the inner `Database::open_cursor` call so
    /// `read_uncommitted` and other cursor-level flags propagate correctly.
    ///
    /// # Lifetime contract (breaking change in Wave 1B)
    ///
    /// The returned [`SecondaryCursor`] borrows both the
    /// `SecondaryDatabase` and — when supplied — the `Transaction`,
    /// because primary deletes and cleanup writes are deferred until
    /// `SecondaryCursor::delete` is called.  Callers must therefore
    /// keep the `Transaction` alive at least as long as the cursor.
    /// In practice this is the same lifetime rule that already applies
    /// to [`Database::open_cursor`]; it is now enforced statically by
    /// the type system.
    ///
    /// # Returns
    /// A `SecondaryCursor` that iterates secondary index entries and returns
    /// primary data.
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
    /// Called from application code that manages secondary index updates
    /// manually (v1.5 has no automatic `associate()`-style hook — that is
    /// v1.6 work).
    ///
    /// # Atomicity (Sprint 4½)
    ///
    /// When `txn` is `Some(&t)`, **all** I/O performed by this method
    /// (cursor opens, `insert_sec_key`, `delete_sec_key`) is executed
    /// under `t`.  If the caller used the same `t` for the primary
    /// [`Database::put`] / [`Database::delete`] that prompted this
    /// update, the primary write and every affected secondary index
    /// entry commit or abort together.  This is the recommended
    /// pattern; see `docs/src/transactions/secondary-with-txn.md`.
    ///
    /// When `txn` is `None`, every inner secondary write runs
    /// auto-committed (v1.4 behaviour).  This is intentionally
    /// available so callers that do not need cross-database atomicity
    /// — e.g. one-shot population or single-threaded scripts — do not
    /// need to allocate a transaction.
    ///
    /// **Idempotent re-insert** (Decision 1B): if `update_secondary` is
    /// invoked twice with the same `(sec_key, pri_key)` pair (whether
    /// auto-commit or under the same `txn`), the second call is a
    /// no-op rather than a [`NoxuError::Unsupported`] collision — see
    /// [`Self::insert_sec_key`].
    ///
    /// # Arguments
    /// * `txn` - Optional transaction.  Pass the same handle that
    ///   drives the primary write to make both updates atomic.
    /// * `pri_key` - The primary key.
    /// * `old_data` - The previous primary data, or `None` on insert.
    /// * `new_data` - The new primary data, or `None` on delete.
    pub fn update_secondary(
        &self,
        txn: Option<&Transaction>,
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
                self.delete_sec_key(
                    txn,
                    old_sec_key.as_ref().unwrap(),
                    pri_key,
                )?;
            }
            if do_insert {
                self.insert_sec_key(
                    txn,
                    new_sec_key.as_ref().unwrap(),
                    pri_key,
                )?;
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
                    self.delete_sec_key(txn, old_key, pri_key)?;
                }
            }
            // Insert keys that were not present before.
            for new_key in &new_keys {
                if !old_keys.contains(new_key) {
                    self.insert_sec_key(txn, new_key, pri_key)?;
                }
            }
        }

        Ok(())
    }

    /// Removes all secondary index entries for the given primary key.
    ///
    /// Called when a primary record is deleted.  `txn` is forwarded to
    /// [`Self::update_secondary`] so the cleanup participates in the
    /// caller's transaction (Sprint 4½).
    pub(crate) fn delete_all_for_primary(
        &self,
        txn: Option<&Transaction>,
        pri_key: &DatabaseEntry,
        old_data: Option<&DatabaseEntry>,
    ) -> Result<()> {
        self.update_secondary(txn, pri_key, old_data, None)
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
    ///
    /// Decision 1B (`docs/src/internal/v1.5-decisions-2026-05.md`):
    /// v1.5 secondaries are one-to-one.  We use
    /// [`crate::put::Put::NoOverwrite`] so a collision — two distinct
    /// primary keys mapping to the same secondary key — returns
    /// [`OperationStatus::KeyExists`] from the cursor, which we surface
    /// as a typed [`NoxuError::Unsupported`] explaining that sorted-dup
    /// secondaries are planned for v1.6.
    ///
    /// Pre-Sprint-3 this used `Put::Overwrite`, which let the second
    /// primary silently destroy the first primary's mapping (audit
    /// finding C4).  The new behaviour is honest about what v1.5
    /// supports.
    fn insert_sec_key(
        &self,
        txn: Option<&Transaction>,
        sec_key: &DatabaseEntry,
        pri_key: &DatabaseEntry,
    ) -> Result<()> {
        let mut cursor = self.make_inner_cursor(txn)?;
        let status = cursor
            .put(sec_key, pri_key, crate::put::Put::NoOverwrite)
            .map_err(|e| NoxuError::OperationNotAllowed(e.to_string()))?;
        match status {
            OperationStatus::Success => Ok(()),
            OperationStatus::KeyExists => {
                // Decision 1B: distinguish idempotent re-insert of the
                // *same* (sec_key, pri_key) pair from a true cross-primary
                // collision.  An idempotent re-insert is a misuse the
                // documented manual-`update_secondary` pattern can produce
                // (calling `update_secondary(pk, None, Some(data))` twice
                // for the same primary instead of
                // `update_secondary(pk, Some(old), Some(new))`); we treat
                // it as a no-op so existing v1.4 callers do not break.
                // A *cross-primary* collision is the v1.6-feature
                // (sorted-dup secondaries) gap and is reported as
                // [`NoxuError::Unsupported`].
                let mut probe_key = sec_key.clone();
                let mut existing_pk = DatabaseEntry::new();
                let mut probe_cursor = self.make_inner_cursor(txn)?;
                let probe_status = probe_cursor
                    .get(
                        &mut probe_key,
                        &mut existing_pk,
                        crate::get::Get::Search,
                        None,
                    )
                    .map_err(|e| {
                        NoxuError::OperationNotAllowed(e.to_string())
                    })?;
                if probe_status == OperationStatus::Success
                    && existing_pk.get_data() == pri_key.get_data()
                {
                    // Same primary key already mapped here — idempotent.
                    return Ok(());
                }
                let sec_hex = sec_key
                    .get_data()
                    .map(|b| {
                        b.iter().map(|b| format!("{b:02x}")).collect::<String>()
                    })
                    .unwrap_or_default();
                let existing_hex = existing_pk
                    .get_data()
                    .map(|b| {
                        b.iter().map(|b| format!("{b:02x}")).collect::<String>()
                    })
                    .unwrap_or_default();
                let pri_hex = pri_key
                    .get_data()
                    .map(|b| {
                        b.iter().map(|b| format!("{b:02x}")).collect::<String>()
                    })
                    .unwrap_or_default();
                Err(NoxuError::Unsupported(format!(
                    "v1.5 secondaries are one-to-one; primary key 0x{existing_hex} \
                     already maps to secondary key 0x{sec_hex} (cannot also \
                     map primary key 0x{pri_hex}; sorted-dup secondaries are \
                     planned for v1.6)"
                )))
            }
            other => Err(NoxuError::OperationNotAllowed(format!(
                "unexpected put status from secondary index insert: {other:?}"
            ))),
        }
    }

    /// Deletes a secondary index entry: (sec_key -> pri_key).
    ///
    /// `txn` is forwarded to the inner cursor so the delete participates
    /// in the caller's transaction (Sprint 4½).
    fn delete_sec_key(
        &self,
        txn: Option<&Transaction>,
        sec_key: &DatabaseEntry,
        pri_key: &DatabaseEntry,
    ) -> Result<()> {
        // Find the exact (sec_key, pri_key) pair and delete it.
        // For non-dup databases a simple key search suffices.
        // For dup databases we need a SEARCH_BOTH, but since the inner
        // database is always configured as a simple key->value store in our
        // implementation (sec_key->pri_key, no dup support at the b-tree level),
        // a key search is correct.
        let mut cursor = self.make_inner_cursor(txn)?;
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
    ///
    /// `txn` is forwarded to [`Database::open_cursor`] so writes through
    /// the cursor participate in the caller's transaction (Sprint 4½).
    fn make_inner_cursor(&self, txn: Option<&Transaction>) -> Result<Cursor> {
        self.inner.open_cursor(txn, None)
    }

    /// Builds a `SecondaryCursor` on this secondary database (internal).
    ///
    /// `txn` is forwarded to [`SecondaryCursor::new`] so all inner-database
    /// reads and the cascade primary delete participate in the caller's
    /// transaction (Wave 1B / audit F5).  Used from
    /// [`SecondaryDatabase::delete`] to drive the secondary scan under
    /// the caller's txn.
    fn open_cursor_internal<'a>(
        &'a self,
        txn: Option<&'a Transaction>,
    ) -> Result<SecondaryCursor<'a>> {
        SecondaryCursor::new(self, txn, None)
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

            // Create secondary key(s) and insert them.  Population runs
            // at `SecondaryDatabase::open` time, before any user txn
            // exists, so we auto-commit each insert (`txn = None`).
            if let Some(creator) = &self.config.key_creator {
                let mut sec_key = DatabaseEntry::new();
                if creator.create_secondary_key(
                    &self.inner,
                    &pri_key,
                    &pri_data,
                    &mut sec_key,
                ) {
                    self.insert_sec_key(None, &sec_key, &pri_key)?;
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
                    self.insert_sec_key(None, &sec_key, &pri_key)?;
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
        secondary
            .update_secondary(None, &pri_key, None, Some(&pri_data))
            .unwrap();

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

        // Insert primary records and index them.  Each record uses a
        // distinct first byte so the v1.5 one-to-one secondary contract
        // (Decision 1B) is satisfied.
        let records: &[(&[u8], &[u8])] =
            &[(b"pk1", b"Apple"), (b"pk2", b"Banana"), (b"pk3", b"Cherry")];

        for (k, v) in records {
            let pk = DatabaseEntry::from_bytes(k);
            let pv = DatabaseEntry::from_bytes(v);
            {
                primary.lock().put(None, &pk, &pv).unwrap();
            }
            secondary.update_secondary(None, &pk, None, Some(&pv)).unwrap();
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
        secondary
            .update_secondary(None, &pri_key, None, Some(&pri_data))
            .unwrap();

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
        secondary
            .update_secondary(None, &pri_key, None, Some(&old_data))
            .unwrap();

        // Now update the primary; the secondary key 'M' should be replaced by 'P'.
        {
            primary.lock().put(None, &pri_key, &new_data).unwrap();
        }
        secondary
            .update_secondary(None, &pri_key, Some(&old_data), Some(&new_data))
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
            secondary.update_secondary(None, &pk, None, Some(&pv)).unwrap();
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

    /// Wave 1C audit cleanup (secondary-join "missing count/exists/truncate"
    /// Low) — the new convenience methods on SecondaryDatabase delegate
    /// to the inner index DB and surface the JE-shape API for the
    /// secondary side.
    #[test]
    fn test_count_exists_truncate_round_trip() {
        let (_tmp, env) = temp_env();
        let primary = Arc::new(Mutex::new(open_primary(&env, "pri")));
        let secondary = open_secondary(Arc::clone(&primary), &env, "sec");

        // Empty index → count == 0, no key exists.
        assert_eq!(secondary.count().unwrap(), 0);
        assert!(
            !secondary.exists(None, &DatabaseEntry::from_bytes(b"A")).unwrap()
        );

        // Populate three primaries with distinct first-byte secondary keys.
        for (pk, pv) in &[
            (&b"pk1"[..], &b"Apple"[..]),
            (&b"pk2"[..], &b"Banana"[..]),
            (&b"pk3"[..], &b"Cherry"[..]),
        ] {
            let pk_e = DatabaseEntry::from_bytes(pk);
            let pv_e = DatabaseEntry::from_bytes(pv);
            primary.lock().put(None, &pk_e, &pv_e).unwrap();
            secondary.update_secondary(None, &pk_e, None, Some(&pv_e)).unwrap();
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

        // Truncate clears every record and reports the pre-truncate count.
        let removed = secondary.truncate().unwrap();
        assert_eq!(removed, 3);
        assert_eq!(secondary.count().unwrap(), 0);
        assert!(
            !secondary.exists(None, &DatabaseEntry::from_bytes(b"A")).unwrap()
        );
    }
}
