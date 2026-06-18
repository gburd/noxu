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
//! See the 2026 review.
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
use std::cell::RefCell;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

thread_local! {
    /// Cycle-detection frame for FK cascades and nullifications.
    /// Contains every `(db_id, fk_value)` pair the current thread is
    /// in the middle of cascading.  See [`FkReferrer`].
    static FK_CASCADE_GUARD: RefCell<HashSet<(u64, Vec<u8>)>> =
        RefCell::new(HashSet::new());
}

/// Trait implemented by [`SecondaryHookState`] so a primary
/// [`Database`] can keep the secondary registry as
/// `Vec<Weak<dyn SecondaryHook + Send + Sync>>` without naming the
/// concrete state struct (the struct holds a non-`Send` config field
/// for some user-supplied callbacks; the trait only exposes the
/// txn-driven update entry point and the secondary's name for
/// diagnostics).
///
/// v1.6 (audit C3 — the `associate()`-style hook).
pub(crate) trait SecondaryHook {
    /// Updates this secondary index after a primary write.  Called by
    /// `Database::put` (`old_data` = `None`, `new_data = Some(…)`),
    /// `Database::delete` (`old_data = Some(…)`, `new_data = None`),
    /// or a primary update path (both `Some`).  `txn` is the same
    /// transaction that drove the primary write so the secondary update
    /// participates atomically.
    fn maintain(
        &self,
        txn: Option<&Transaction>,
        pri_key: &DatabaseEntry,
        old_data: Option<&DatabaseEntry>,
        new_data: Option<&DatabaseEntry>,
    ) -> Result<()>;

    /// Returns the secondary's database name (used in diagnostics).
    fn name(&self) -> String;
}

/// Trait implemented by [`SecondaryHookState`] for the FK referrer
/// registry (v1.6 audit C2 / Decision 2C).  When a record is deleted
/// from a foreign-key target primary, the engine iterates every
/// registered referrer and calls
/// [`FkReferrer::on_foreign_key_deleted`] under the same caller-supplied
/// txn.  The implementation runs the configured
/// [`ForeignKeyDeleteAction`] for every secondary key matching the
/// deleted foreign key.
pub(crate) trait FkReferrer {
    /// Called when a foreign-DB primary record is about to be deleted.
    /// `fk_value` is the primary key of the foreign DB record (which
    /// may also be a secondary key in this child index).
    ///
    /// Returning `Err(NoxuError::ForeignConstraintViolation(…))` aborts
    /// the foreign delete (Abort action).  Returning `Ok(())` may have
    /// already mutated the child primary records (Cascade / Nullify).
    fn on_foreign_key_deleted(
        &self,
        txn: Option<&Transaction>,
        fk_value: &DatabaseEntry,
    ) -> Result<()>;

    /// Returns the child secondary's database name (used in error messages).
    fn name(&self) -> String;
}

/// Internal state of a [`SecondaryDatabase`].
///
/// Held behind an `Arc` so the primary database can keep a `Weak<_>`
/// reference for automatic-maintenance fan-out without creating a
/// cycle.  Dropping the [`SecondaryDatabase`] handle drops the strong
/// `Arc`; the next primary registration will purge the now-dangling
/// `Weak`.
pub(crate) struct SecondaryHookState {
    /// The underlying secondary index storage (sec_key -> [pri_key…]).
    pub(crate) inner: Database,
    /// The primary database this index is associated with.
    pub(crate) primary: Arc<Mutex<Database>>,
    /// The secondary configuration (holds key creator callback, etc.).
    pub(crate) config: SecondaryConfig,
    /// Whether this secondary is fully populated (not in incremental mode).
    pub(crate) is_fully_populated: AtomicBool,
}

impl SecondaryHookState {
    /// Updates this secondary index after a primary insert / update /
    /// delete.  Mirrors the v1.5 [`SecondaryDatabase::update_secondary`]
    /// behaviour but lives on the state so it can be invoked from the
    /// [`SecondaryHook`] trait impl as well as the public
    /// [`SecondaryDatabase`] facade.
    pub(crate) fn update_secondary(
        &self,
        txn: Option<&Transaction>,
        pri_key: &DatabaseEntry,
        old_data: Option<&DatabaseEntry>,
        new_data: Option<&DatabaseEntry>,
    ) -> Result<()> {
        let key_creator = &self.config.key_creator;
        let multi_key_creator = &self.config.multi_key_creator;

        if old_data.is_none() && new_data.is_none() {
            return Ok(());
        }

        if let Some(creator) = key_creator {
            let old_sec_key = old_data.and_then(|od| {
                let mut sk = DatabaseEntry::new();
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
            for old_key in &old_keys {
                if !new_keys.contains(old_key) {
                    self.delete_sec_key(txn, old_key, pri_key)?;
                }
            }
            for new_key in &new_keys {
                if !old_keys.contains(new_key) {
                    self.insert_sec_key(txn, new_key, pri_key)?;
                }
            }
        }

        Ok(())
    }

    /// Inserts a (sec_key, pri_key) duplicate.  See the
    /// [`SecondaryDatabase`] impl for the full doc-comment; this is the
    /// state-side implementation that the public method delegates to.
    fn insert_sec_key(
        &self,
        txn: Option<&Transaction>,
        sec_key: &DatabaseEntry,
        pri_key: &DatabaseEntry,
    ) -> Result<()> {
        // F3 (JE SecondaryDatabase.insertKey): when a foreign-key database is
        // configured, the new secondary key MUST exist as a key in that foreign
        // DB, else the referential constraint is violated. JE enforces this on
        // every secondary insert (not just on delete). Check before the index
        // put.
        //
        // EXCEPT when we are inside an FK cascade/nullify (the thread-local
        // guard is non-empty): the nullify-rewrite re-maintains the secondary
        // with the nullified key, which is an internal integrity operation, not
        // a user insert — re-checking it would (a) wrongly reject the nullified
        // key and (b) re-lock the foreign DB we are already holding (deadlock).
        let in_cascade = FK_CASCADE_GUARD.with(|g| !g.borrow().is_empty());
        if !in_cascade
            && let Some(foreign_db) = &self.config.foreign_key_database
        {
            let fdb = foreign_db.lock();
            let mut scratch = DatabaseEntry::new();
            let found = matches!(
                fdb.get(txn, sec_key, &mut scratch).map_err(|e| {
                    NoxuError::OperationNotAllowed(e.to_string())
                })?,
                OperationStatus::Success,
            );
            drop(fdb);
            if !found {
                return Err(NoxuError::ForeignConstraintViolation(format!(
                    "foreign key not allowed: {:?} is not present in the                      foreign database",
                    sec_key.data()
                )));
            }
        }
        let mut cursor = self.make_inner_cursor(txn)?;
        let status =
            cursor
                .put(sec_key, pri_key, crate::put::Put::NoDupData)
                .map_err(|e| NoxuError::OperationNotAllowed(e.to_string()))?;
        match status {
            OperationStatus::Success => Ok(()),
            OperationStatus::KeyExists => {
                // D6: duplicate (sec_key, pri_key) pair already in the index.
                // When fully populated this indicates a corrupt/inconsistent
                // secondary index.  Raise SecondaryIntegrityException.
                // Ref: SecondaryDatabase.java insertSecKey() KEYEXIST branch.
                if self
                    .is_fully_populated
                    .load(std::sync::atomic::Ordering::Acquire)
                {
                    Err(NoxuError::SecondaryIntegrityException(
                        "duplicate (sec_key, pri_key) already in secondary index; \
                         secondary index is inconsistent"
                            .into(),
                    ))
                } else {
                    // Not fully populated: during populate(), duplicates can
                    // legitimately exist if the index was partially built.
                    Ok(())
                }
            }
            other => Err(NoxuError::OperationNotAllowed(format!(
                "unexpected put status from secondary index insert: {other:?}"
            ))),
        }
    }

    /// Deletes the exact (sec_key, pri_key) duplicate.
    fn delete_sec_key(
        &self,
        txn: Option<&Transaction>,
        sec_key: &DatabaseEntry,
        pri_key: &DatabaseEntry,
    ) -> Result<()> {
        let mut cursor = self.make_inner_cursor(txn)?;
        let mut sec_key_mut = sec_key.clone();
        let mut pri_key_mut = pri_key.clone();
        let status = cursor
            .get(
                &mut sec_key_mut,
                &mut pri_key_mut,
                crate::get::Get::SearchBoth,
                None,
            )
            .map_err(|e| NoxuError::OperationNotAllowed(e.to_string()))?;
        if status == OperationStatus::Success {
            cursor
                .delete()
                .map_err(|e| NoxuError::OperationNotAllowed(e.to_string()))?;
        } else {
            // D7: (sec_key, pri_key) pair not found in the secondary index.
            // When fully populated this means the index is missing an entry
            // that should be there (corrupt secondary index).
            // Ref: SecondaryDatabase.java deleteSecKey() missing-entry branch.
            if self
                .is_fully_populated
                .load(std::sync::atomic::Ordering::Acquire)
            {
                return Err(NoxuError::SecondaryIntegrityException(
                    "(sec_key, pri_key) pair not found in secondary index during delete; \
                     secondary index is missing a required entry"
                        .into(),
                ));
            }
        }
        Ok(())
    }

    fn make_inner_cursor(&self, txn: Option<&Transaction>) -> Result<Cursor> {
        self.inner.open_cursor(txn, None)
    }
}

impl SecondaryHook for SecondaryHookState {
    fn maintain(
        &self,
        txn: Option<&Transaction>,
        pri_key: &DatabaseEntry,
        old_data: Option<&DatabaseEntry>,
        new_data: Option<&DatabaseEntry>,
    ) -> Result<()> {
        self.update_secondary(txn, pri_key, old_data, new_data)
    }

    fn name(&self) -> String {
        self.inner.get_database_name().to_string()
    }
}

impl FkReferrer for SecondaryHookState {
    /// v1.6 (audit C2 / Decision 2C): handles a foreign-DB delete by
    /// dispatching on the configured [`ForeignKeyDeleteAction`].
    ///
    /// * `Abort`   — if any child secondary entry has `sec_key == fk_value`,
    ///   return [`NoxuError::ForeignConstraintViolation`].
    /// * `Cascade` — wired in step 9.
    /// * `Nullify` — wired in step 10.
    fn on_foreign_key_deleted(
        &self,
        txn: Option<&Transaction>,
        fk_value: &DatabaseEntry,
    ) -> Result<()> {
        match self.config.foreign_key_delete_action {
            crate::secondary_config::ForeignKeyDeleteAction::Abort => {
                // Probe the inner secondary index for any duplicate of
                // `fk_value`.  If we find at least one, the foreign
                // delete must abort.
                let mut probe_key = fk_value.clone();
                let mut probe_pk = DatabaseEntry::new();
                let mut cursor = self.inner.open_cursor(txn, None)?;
                let st = cursor
                    .get(
                        &mut probe_key,
                        &mut probe_pk,
                        crate::get::Get::Search,
                        None,
                    )
                    .map_err(|e| {
                        NoxuError::OperationNotAllowed(e.to_string())
                    })?;
                if st == OperationStatus::Success {
                    let fk_hex = fk_value
                        .get_data()
                        .map(|b| {
                            b.iter()
                                .map(|b| format!("{b:02x}"))
                                .collect::<String>()
                        })
                        .unwrap_or_default();
                    return Err(NoxuError::ForeignConstraintViolation(
                        format!(
                            "foreign-key delete aborted: secondary '{}' \
                             still references foreign key 0x{fk_hex} \
                             (ForeignKeyDeleteAction::Abort)",
                            self.inner.get_database_name()
                        ),
                    ));
                }
                Ok(())
            }
            crate::secondary_config::ForeignKeyDeleteAction::Cascade => {
                // v1.6 step 9 — transitive cascade with cycle detection.
                //
                // For every primary record indexed under `fk_value` in
                // *this* secondary, delete the primary.  The primary's
                // own [`Database::delete`] fan-out re-enters this hook
                // for any deeper cascades; the thread-local guard keeps
                // a cycle from spinning forever.
                let primary = Arc::clone(&self.primary);
                let db_id = primary.lock().db_id_for_fk_guard();
                let fk_bytes = fk_value.get_data().unwrap_or(&[]).to_vec();

                if !FK_CASCADE_GUARD
                    .with(|c| c.borrow_mut().insert((db_id, fk_bytes.clone())))
                {
                    // Already cascading on this (db, key) frame — skip
                    // to break the cycle.  This matches JE's
                    // `cascadeDeletePrimaries` cycle-skip logic.
                    return Ok(());
                }

                // Collect every child primary key indexed under fk_value.
                let child_pris: Vec<DatabaseEntry> = {
                    let mut child_keys = Vec::new();
                    let mut cursor = self.inner.open_cursor(txn, None)?;
                    let mut sk = fk_value.clone();
                    let mut pk = DatabaseEntry::new();
                    let mut st = cursor
                        .get(&mut sk, &mut pk, crate::get::Get::Search, None)
                        .map_err(|e| {
                            NoxuError::OperationNotAllowed(e.to_string())
                        })?;
                    while st == OperationStatus::Success {
                        if sk.get_data().unwrap_or(&[])
                            != fk_value.get_data().unwrap_or(&[])
                        {
                            break;
                        }
                        if let Some(b) = pk.get_data() {
                            child_keys.push(DatabaseEntry::from_bytes(b));
                        }
                        st = cursor
                            .get(&mut sk, &mut pk, crate::get::Get::Next, None)
                            .map_err(|e| {
                                NoxuError::OperationNotAllowed(e.to_string())
                            })?;
                    }
                    child_keys
                };

                // Apply the cascade.  Each `primary.delete` re-enters
                // the maintenance plumbing on the child primary so its
                // secondaries and any deeper FK relationships are
                // honoured.  Errors propagate so the caller's txn rolls
                // the cascade back together with the originating delete.
                let cascade_result: Result<()> = (|| {
                    let primary_guard = primary.lock();
                    for child_pri in child_pris {
                        primary_guard.delete(txn, &child_pri)?;
                    }
                    Ok(())
                })();

                FK_CASCADE_GUARD.with(|c| {
                    c.borrow_mut().remove(&(db_id, fk_bytes));
                });

                cascade_result
            }
            crate::secondary_config::ForeignKeyDeleteAction::Nullify => {
                // v1.6 step 10 — nullify the FK field on every child
                // primary record indexed under fk_value, then re-put
                // the modified record so auto-maintenance cleans up
                // the now-stale secondary entry.
                //
                // Cycle detection mirrors the Cascade arm: even though
                // a Nullify cannot directly cascade through more FK
                // edges, a child primary update may itself be a
                // foreign-key-delete from another perspective via the
                // auto-maintenance fan-out, so we still guard the
                // (db, key) frame.
                let primary = Arc::clone(&self.primary);
                let db_id = primary.lock().db_id_for_fk_guard();
                let fk_bytes = fk_value.get_data().unwrap_or(&[]).to_vec();
                if !FK_CASCADE_GUARD
                    .with(|c| c.borrow_mut().insert((db_id, fk_bytes.clone())))
                {
                    return Ok(());
                }

                let single = self.config.foreign_key_nullifier.as_deref();
                let multi = self.config.foreign_multi_key_nullifier.as_deref();

                // Collect (child_primary_key, child_primary_data) pairs.
                let child_records: Vec<(DatabaseEntry, DatabaseEntry)> = {
                    let mut child = Vec::new();
                    let mut cursor = self.inner.open_cursor(txn, None)?;
                    let mut sk = fk_value.clone();
                    let mut pk = DatabaseEntry::new();
                    let mut st = cursor
                        .get(&mut sk, &mut pk, crate::get::Get::Search, None)
                        .map_err(|e| {
                            NoxuError::OperationNotAllowed(e.to_string())
                        })?;
                    while st == OperationStatus::Success {
                        if sk.get_data().unwrap_or(&[])
                            != fk_value.get_data().unwrap_or(&[])
                        {
                            break;
                        }
                        // Fetch the child primary's data so the
                        // nullifier sees it.
                        let child_pri = DatabaseEntry::from_bytes(
                            pk.get_data().unwrap_or(&[]),
                        );
                        let mut data = DatabaseEntry::new();
                        let g =
                            primary.lock().get(txn, &child_pri, &mut data)?;
                        if g == OperationStatus::Success {
                            child.push((child_pri, data));
                        }
                        st = cursor
                            .get(&mut sk, &mut pk, crate::get::Get::Next, None)
                            .map_err(|e| {
                                NoxuError::OperationNotAllowed(e.to_string())
                            })?;
                    }
                    child
                };

                let nullify_result: Result<()> = (|| {
                    for (child_pri, mut child_data) in child_records {
                        let modified = match (single, multi) {
                            (Some(n), _) => n.nullify_foreign_key(
                                &self.inner,
                                &mut child_data,
                            ),
                            (None, Some(mn)) => mn.nullify_foreign_key(
                                &self.inner,
                                &child_pri,
                                &mut child_data,
                                fk_value,
                            ),
                            (None, None) => {
                                return Err(NoxuError::IllegalArgument(
                                    "ForeignKeyDeleteAction::Nullify requires a \
                                     ForeignKeyNullifier or \
                                     ForeignMultiKeyNullifier on the \
                                     SecondaryConfig"
                                        .to_string(),
                                ));
                            }
                        };
                        if modified {
                            // Re-put the modified record under the
                            // caller's txn.  Auto-maintenance on the
                            // child primary handles clearing the stale
                            // secondary entries.
                            primary.lock().put(txn, &child_pri, &child_data)?;
                        }
                    }
                    Ok(())
                })();

                FK_CASCADE_GUARD.with(|c| {
                    c.borrow_mut().remove(&(db_id, fk_bytes));
                });

                nullify_result
            }
        }
    }

    fn name(&self) -> String {
        self.inner.get_database_name().to_string()
    }
}

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
/// `update_secondary` participates in the
/// caller's transaction when one is supplied.  Threading the same
/// `txn` through both [`Database::put`] and
/// [`update_secondary`](Self::update_secondary) makes the primary +
/// secondary update **atomic**: aborting the txn rolls both back,
/// committing the txn persists both.  Passing `None` runs each call
/// auto-committed, which restores the v1.4 behaviour and is acceptable
/// when the caller does not need cross-database atomicity.
///
/// See the 2026 review and
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
    /// All shared state behind an `Arc` so the primary registry can
    /// keep `Weak<dyn SecondaryHook + Send + Sync>` references
    /// (Decision 1B / audit C3).  Every public method on
    /// `SecondaryDatabase` accesses the fields through `state.field`.
    state: Arc<SecondaryHookState>,
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
    /// - [`NoxuError::IllegalArgument`] if the configuration is invalid,
    ///   or if the inner `secondary_db` was not opened with
    ///   `DatabaseConfig::with_sorted_duplicates(true)` (v1.6 sorted-dup
    ///   secondaries — closes audit C4).
    /// - [`NoxuError::Unsupported`] if the configuration sets any foreign-key
    ///   constraint field (`foreign_key_database`,
    ///   `foreign_key_delete_action != Abort`, `foreign_key_nullifier`, or
    ///   `foreign_multi_key_nullifier`).  v1.5 does not enforce FK
    ///   constraints; full FK support is planned for v1.6 — see Decision 2C
    ///   in the 2026 review (closes audit
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

        // v1.6 (Decision 1B / audit C4): the inner secondary index DB
        // must be opened with sorted_duplicates so multiple primary
        // records can share the same secondary key as duplicates of
        // the (sec_key) entry.  Reject otherwise — in v1.5 we used
        // Put::NoOverwrite and surfaced cross-primary collisions as
        // NoxuError::Unsupported; v1.6 stores them as duplicates.
        if !secondary_db.get_config().sorted_duplicates {
            return Err(NoxuError::IllegalArgument(
                "v1.6 secondary databases require the inner index DB to \
                 be opened with DatabaseConfig::with_sorted_duplicates(true) \
                 — see the 2026 review Decision 1B"
                    .to_string(),
            ));
        }

        // v1.6 (audit C2 / Decision 2C): foreign-key constraints are
        // now enforced when the user supplies the foreign DB handle
        // via [`SecondaryConfig::with_foreign_key_database_handle`].
        // The `name`-only setter remains advisory — a config that
        // names a foreign DB but never wires the handle is rejected
        // here so the user is not silently left with an unenforced
        // constraint.  Cascade / Nullify still require the handle and
        // the matching nullifier (steps 9 / 10).
        let fk_handle = config.foreign_key_database.clone();
        if config.foreign_key_database_name.is_some() && fk_handle.is_none() {
            return Err(NoxuError::IllegalArgument(
                "SecondaryConfig.foreign_key_database_name is set without \
                 a foreign_key_database handle; v1.6 FK enforcement requires \
                 calling SecondaryConfig::with_foreign_key_database_handle()"
                    .to_string(),
            ));
        }
        if (config.foreign_key_nullifier.is_some()
            || config.foreign_multi_key_nullifier.is_some())
            && fk_handle.is_none()
        {
            return Err(NoxuError::IllegalArgument(
                "foreign-key nullifier is set without a foreign_key_database \
                 handle (call SecondaryConfig::with_foreign_key_database_handle)"
                    .to_string(),
            ));
        }

        let state = Arc::new(SecondaryHookState {
            inner: secondary_db,
            primary,
            config,
            is_fully_populated: AtomicBool::new(true),
        });

        // v1.6 (audit C3): register the secondary on the primary so
        // future `Database::put` / `Database::delete` calls fan out to
        // it automatically.  We downgrade to `Weak` so dropping the
        // `SecondaryDatabase` handle removes it from the registry on
        // the next iteration.
        {
            let weak: std::sync::Weak<dyn SecondaryHook + Send + Sync> =
                Arc::downgrade(&state) as _;
            state.primary.lock().register_secondary(weak);
        }

        // v1.6 (audit C2 / Decision 2C): if the secondary references a
        // foreign primary DB, register as an FK referrer there so its
        // `Database::delete` can call back into us with the configured
        // ForeignKeyDeleteAction.
        if let Some(fk_handle) = fk_handle {
            let weak: std::sync::Weak<dyn FkReferrer + Send + Sync> =
                Arc::downgrade(&state) as _;
            fk_handle.lock().register_fk_referrer(weak);
        }

        let sec = SecondaryDatabase { state };

        // If allow_populate and the secondary is empty, populate from primary.
        if sec.state.config.allow_populate {
            sec.populate_if_empty()?;
        }

        Ok(sec)
    }

    // ------------------------------------------------------------------
    // Public API
    // ------------------------------------------------------------------

    /// Returns the database name of the secondary index.
    pub fn get_database_name(&self) -> &str {
        self.state.inner.get_database_name()
    }

    /// Returns the secondary configuration.
    ///
    ///
    pub fn get_config(&self) -> &SecondaryConfig {
        &self.state.config
    }

    /// Returns whether this handle is open.
    pub fn is_valid(&self) -> bool {
        self.state.inner.is_valid()
    }

    /// Closes the secondary database handle.
    ///
    ///
    pub fn close(&self) -> Result<()> {
        self.state.inner.close()
    }

    /// Returns the number of records in the secondary index.
    ///
    /// Equivalent to `Database::count` on the underlying inner index
    /// database; included on `SecondaryDatabase` for symmetry with JE's
    /// `SecondaryDatabase.count()` method.  See
    /// (secondary-join “missing count/exists/truncate” Low).
    ///
    /// # Errors
    /// Returns [`NoxuError::DatabaseClosed`] if the secondary handle has
    /// been closed.
    pub fn count(&self) -> Result<u64> {
        self.state.inner.count()
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
        let status = self.state.inner.get(txn, key, &mut data)?;
        Ok(status == OperationStatus::Success)
    }

    /// Removes every record from the secondary index, leaving the
    /// associated primary database untouched.
    ///
    /// **Caveat.** Truncating a secondary index without re-running
    /// `populate_if_empty` (or replaying the primary-side updates)
    /// leaves the secondary in a state that is not consistent with the
    /// primary.  Most callers should drop the secondary's primary keys
    /// via `Database::truncate_database` on the inner DB or repopulate
    /// the index afterwards.  Returned for symmetry with JE's
    /// `SecondaryDatabase.truncate(...)`.
    ///
    /// Returns the number of records that were in the index before the
    /// truncate.
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
        let mut cursor = self.state.inner.open_cursor(None, None)?;
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
        let status = self.state.inner.get(txn, key, &mut pri_key_entry)?;

        if status != OperationStatus::Success {
            return Ok(OperationStatus::NotFound);
        }

        // Store the primary key in the output parameter.
        if let Some(pk) = pri_key_entry.get_data() {
            p_key.set_data(pk);
        }

        // Now fetch the primary record.
        let primary = self.state.primary.lock();
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

            // Delete the primary record.  The auto-hook registered with the
            // primary will remove all secondary index entries for this primary
            // (including the current secondary key entry we found) atomically.
            // Do NOT call delete_all_for_primary first — that would cause D7
            // (double-delete: auto-hook tries to remove entries already gone).
            {
                let primary = self.state.primary.lock();
                let _ = primary.delete(txn, &pri_key_entry)?;
            }

            // Re-search for the key to find any remaining duplicates.
            // Since primary.delete() cleaned up secondary entries via auto-hook,
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
    /// locks tracked by the txn.  Secondary cursors also
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
    /// # Lifetime contract
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
        self.state.is_fully_populated.store(false, Ordering::Release);
    }

    /// Ends incremental population mode.
    ///
    ///
    pub fn end_incremental_population(&self) {
        self.state.is_fully_populated.store(true, Ordering::Release);
    }

    /// Returns whether incremental population is currently enabled.
    ///
    ///
    pub fn is_incremental_population_enabled(&self) -> bool {
        !self.state.is_fully_populated.load(Ordering::Acquire)
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
    /// # Atomicity
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
    /// `Self::insert_sec_key`.
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
        // Delegated to the state so the [`SecondaryHook`] trait impl can
        // share the same body when `Database::put` / `Database::delete`
        // drives automatic maintenance (audit C3).
        self.state.update_secondary(txn, pri_key, old_data, new_data)
    }

    /// Removes all secondary index entries for the given primary key.
    ///
    /// Called when a primary record is deleted.  `txn` is forwarded to
    /// [`Self::update_secondary`] so the cleanup participates in the
    /// caller's transaction.
    pub(crate) fn delete_all_for_primary(
        &self,
        txn: Option<&Transaction>,
        pri_key: &DatabaseEntry,
        old_data: Option<&DatabaseEntry>,
    ) -> Result<()> {
        self.state.update_secondary(txn, pri_key, old_data, None)
    }

    /// Returns a reference to the inner index `Database`.
    pub(crate) fn inner_db(&self) -> &Database {
        &self.state.inner
    }

    /// Returns a reference to the primary `Database` (via the mutex).
    pub(crate) fn primary_db(&self) -> &Arc<Mutex<Database>> {
        &self.state.primary
    }

    // ------------------------------------------------------------------
    // Private helpers
    // ------------------------------------------------------------------

    /// Inserts a secondary index entry: (sec_key -> pri_key).
    ///
    /// v1.6 (Decision 1B / audit C4): the inner index DB is sorted-dup,
    /// so multiple primary records that produce the same `sec_key` are
    /// stored as duplicates of `sec_key`.  Delegates to the state-side
    /// implementation so the [`SecondaryHook`] trait shares it.
    fn insert_sec_key(
        &self,
        txn: Option<&Transaction>,
        sec_key: &DatabaseEntry,
        pri_key: &DatabaseEntry,
    ) -> Result<()> {
        self.state.insert_sec_key(txn, sec_key, pri_key)
    }

    /// Deletes a secondary index entry: (sec_key -> pri_key).  Delegates
    /// to the state-side implementation.
    #[allow(dead_code)]
    fn delete_sec_key(
        &self,
        txn: Option<&Transaction>,
        sec_key: &DatabaseEntry,
        pri_key: &DatabaseEntry,
    ) -> Result<()> {
        self.state.delete_sec_key(txn, sec_key, pri_key)
    }

    /// Builds a writable `Cursor` on the inner secondary index `Database`.
    /// Delegates to the state-side implementation.
    #[allow(dead_code)]
    fn make_inner_cursor(&self, txn: Option<&Transaction>) -> Result<Cursor> {
        self.state.inner.open_cursor(txn, None)
    }

    /// Builds a `SecondaryCursor` on this secondary database (internal).
    ///
    /// `txn` is forwarded to [`SecondaryCursor::new`] so all inner-database
    /// reads and the cascade primary delete participate in the caller's
    /// transaction.  Used from
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
        let sec_count = self.state.inner.count()?;
        if sec_count > 0 {
            return Ok(());
        }

        // Use direct CursorImpl scan to access both key and value.
        let primary = self.state.primary.lock();
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
            if let Some(creator) = &self.state.config.key_creator {
                let mut sec_key = DatabaseEntry::new();
                if creator.create_secondary_key(
                    &self.state.inner,
                    &pri_key,
                    &pri_data,
                    &mut sec_key,
                ) {
                    self.insert_sec_key(None, &sec_key, &pri_key)?;
                }
            } else if let Some(multi_creator) =
                &self.state.config.multi_key_creator
            {
                let mut sec_keys = Vec::new();
                multi_creator.create_secondary_keys(
                    &self.state.inner,
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
        if !self.state.inner.is_valid() {
            return Err(NoxuError::DatabaseClosed);
        }
        Ok(())
    }

    /// Checks that this database is readable (not in incremental population mode).
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
        // v1.6 sorted-dup secondaries: inner index DB must allow dups.
        let sec_db_config = DatabaseConfig::new()
            .with_allow_create(true)
            .with_sorted_duplicates(true);
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

        // primary.put() automatically maintains the secondary via the
        // registered hook (v1.6 auto-maintenance).
        let pri_key = DatabaseEntry::from_bytes(b"pk1");
        let pri_data = DatabaseEntry::from_bytes(b"Avalon");
        {
            let primary = primary.lock();
            primary.put(None, &pri_key, &pri_data).unwrap();
        }

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
            // Auto-hook maintains secondary; no explicit update_secondary needed.
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
            // Auto-hook maintains secondary.
        }

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
            // Auto-hook inserts (M, pk1) into secondary.
        }

        // Now update the primary; the secondary key 'M' should be replaced by 'P'.
        // Auto-hook fetches old_data, deletes (M, pk1), inserts (P, pk1).
        {
            primary.lock().put(None, &pri_key, &new_data).unwrap();
        }

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
            // Auto-hook maintains secondary.
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
        let sec_db_config = DatabaseConfig::new()
            .with_allow_create(true)
            .with_sorted_duplicates(true);
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

    /// Note:
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
            // Auto-hook maintains secondary.
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
