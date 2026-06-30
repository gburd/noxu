//! Database / transaction triggers.
//!
//! Port of JE `com.sleepycat.je.trigger.Trigger` + `TransactionTrigger`.
//!
//! A `Trigger` is a user-supplied callback object registered on a database via
//! [`crate::DatabaseConfig`].  The engine fires its methods on data changes
//! (`put` / `delete`) and on transaction resolution (`commit` / `abort`).
//!
//! # JE mapping (faithful)
//!
//! JE splits the contract across two Java interfaces that a single trigger
//! object may both implement:
//!
//! * `com.sleepycat.je.trigger.Trigger` — `getName`, lifecycle
//!   (`addTrigger` / `removeTrigger`) and the record operations
//!   `put(txn, key, oldData, newData)` / `delete(txn, key, oldData)`.
//! * `com.sleepycat.je.trigger.TransactionTrigger` — `commit(txn)` /
//!   `abort(txn)`, invoked from `Txn.commit` / `Txn.abort` for every database
//!   that was modified within the transaction (`TriggerManager.runCommitTriggers`
//!   / `runAbortTriggers`).
//!
//! JE dispatches to `TransactionTrigger` via `instanceof` (a trigger that does
//! not implement it simply has no commit/abort behaviour).  The Rust idiom is a
//! single `Trigger` trait whose `commit` / `abort` methods default to no-ops:
//! a trigger that only cares about record operations leaves them unimplemented,
//! exactly mirroring "does not implement `TransactionTrigger`".  This avoids a
//! second trait object and the downcast dance while preserving the JE
//! semantics.
//!
//! # Transaction argument
//!
//! JE passes the public `Transaction` handle.  Noxu passes the transaction id
//! (`Option<u64>`; `None` when the operation is non-transactional /
//! auto-commit) instead.  The trait lives in `noxu-dbi`, below `noxu-db` in the
//! dependency graph, so it cannot name `noxu_db::Transaction`; the id is the
//! faithful, dependency-clean signal of "which transaction this fired under"
//! and matches JE's `Transaction.getId()`.
//!
//! # Firing semantics (faithful to JE)
//!
//! * `put` / `delete` fire **within** the transaction, **after** the record
//!   modification has been applied — JE `Cursor.putNotify` /
//!   `Cursor.deleteInternal` call `TriggerManager.runPutTriggers` /
//!   `runDeleteTriggers` after the actual tree mutation.  A trigger therefore
//!   observes the change and can make accompanying changes under the same
//!   transaction; on abort those changes are rolled back with the transaction.
//! * `commit` / `abort` fire on the transaction's resolution, once per
//!   modified database, in trigger registration order (JE iterates
//!   `dbImpl.getTriggers()` in list order).
//! * Multiple triggers fire in **registration order** (JE stores them in a
//!   `List<Trigger>` and iterates it).
//!
//! # Persistence / replication adaptation (diverges from JE — documented)
//!
//! JE's `PersistentTrigger` serializes the trigger's *class name* into the
//! database record and re-instantiates the trigger by name on open.  A Rust
//! closure / trait object has no portable, reconstructable name, so — exactly
//! as the DBI-14 comparator API does — Noxu triggers are **runtime-registered
//! only**: they are *not* persisted and *not* replicated.  Applications must
//! re-register triggers on every [`crate::DatabaseConfig`] open.  This matches
//! JE's own current state: the `Trigger.java` Javadoc warns that "Only
//! transient triggers are currently supported" and that triggers "must be
//! configured on each node in a rep group separately".

/// A user-supplied database / transaction trigger.
///
/// Register one or more triggers on a [`crate::DatabaseConfig`]; the engine
/// fires the record-operation methods ([`put`](Trigger::put) /
/// [`delete`](Trigger::delete)) within the transaction after each change, and
/// the transaction-lifecycle methods ([`commit`](Trigger::commit) /
/// [`abort`](Trigger::abort)) when the transaction resolves.
///
/// JE `com.sleepycat.je.trigger.Trigger` + `TransactionTrigger`.
pub trait Trigger: Send + Sync {
    /// The trigger's name.  All triggers on one database must have unique
    /// names.  JE `Trigger.getName`.
    fn name(&self) -> &str;

    /// The trigger method invoked after a successful `put`, i.e. one that
    /// actually modified the database.
    ///
    /// For a new insert, `old_data` is `None`; for an update of an existing
    /// record, `old_data` is `Some(previous)`.  `new_data` is always present.
    /// Fired within the transaction, after the change is applied.
    ///
    /// JE `Trigger.put(Transaction, DatabaseEntry key, DatabaseEntry oldData,
    /// DatabaseEntry newData)`.
    ///
    /// * `txn_id` — the transaction id, or `None` if non-transactional.
    /// * `key` — the (non-null) primary key.
    /// * `old_data` — the data before the change, or `None` if the record did
    ///   not previously exist.
    /// * `new_data` — the (non-null) data after the change.
    fn put(
        &self,
        txn_id: Option<u64>,
        key: &[u8],
        old_data: Option<&[u8]>,
        new_data: &[u8],
    );

    /// The trigger method invoked after a successful `delete`, i.e. one that
    /// actually removed a key/data pair.  Fired within the transaction, after
    /// the change is applied.
    ///
    /// JE `Trigger.delete(Transaction, DatabaseEntry key,
    /// DatabaseEntry oldData)`.
    ///
    /// * `txn_id` — the transaction id, or `None` if non-transactional.
    /// * `key` — the (non-null) primary key.
    /// * `old_data` — the (non-null) data that was associated with the deleted
    ///   key.
    fn delete(&self, txn_id: Option<u64>, key: &[u8], old_data: &[u8]);

    /// The trigger method invoked after the transaction that modified this
    /// trigger's database has committed.  Only invoked if the database was
    /// modified during the transaction.  Default: no-op (JE: trigger does not
    /// implement `TransactionTrigger`).
    ///
    /// JE `TransactionTrigger.commit(Transaction)`.
    fn commit(&self, _txn_id: u64) {}

    /// The trigger method invoked after the transaction that modified this
    /// trigger's database has aborted.  Only invoked if the database was
    /// modified during the transaction.  Default: no-op.
    ///
    /// JE `TransactionTrigger.abort(Transaction)`.
    fn abort(&self, _txn_id: u64) {}

    /// Lifecycle hook invoked when the trigger is added to the database
    /// (the first trigger method invoked, exactly once).  Default: no-op.
    ///
    /// JE `Trigger.addTrigger(Transaction)`.
    fn add_trigger(&self, _txn_id: Option<u64>) {}

    /// Lifecycle hook invoked when the trigger is removed from the database
    /// (e.g. on close).  Default: no-op.
    ///
    /// JE `Trigger.removeTrigger(Transaction)`.
    fn remove_trigger(&self, _txn_id: Option<u64>) {}
}
