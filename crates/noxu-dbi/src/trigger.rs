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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// A trigger that implements ONLY the two required record-operation
    /// methods. In JE terms this is a `Trigger` that does *not* also implement
    /// `TransactionTrigger`, so it must have no commit/abort behaviour — the
    /// contract the four default methods encode.
    struct RecordOnly {
        name: String,
        calls: AtomicUsize,
    }

    impl Trigger for RecordOnly {
        fn name(&self) -> &str {
            &self.name
        }
        fn put(
            &self,
            _txn_id: Option<u64>,
            _key: &[u8],
            _old: Option<&[u8]>,
            _new: &[u8],
        ) {
            self.calls.fetch_add(1, Ordering::SeqCst);
        }
        fn delete(&self, _txn_id: Option<u64>, _key: &[u8], _old: &[u8]) {
            self.calls.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// The four lifecycle/transaction methods default to no-ops. Two things
    /// must hold and neither is free: they must not panic (a
    /// `todo!()`/`unimplemented!()` default would turn every commit of a
    /// database carrying a record-only trigger into a crash), and they must not
    /// forward into `put`/`delete` (which would fabricate record events out of
    /// transaction events).
    #[test]
    fn unimplemented_lifecycle_methods_are_no_ops_not_panics_or_forwards() {
        let t = RecordOnly {
            name: "record-only".to_string(),
            calls: AtomicUsize::new(0),
        };

        t.commit(7);
        t.abort(7);
        t.add_trigger(Some(7));
        t.add_trigger(None);
        t.remove_trigger(Some(7));
        t.remove_trigger(None);

        assert_eq!(
            t.calls.load(Ordering::SeqCst),
            0,
            "the default lifecycle methods must not fire put/delete"
        );

        // The record methods still work — the defaults did not shadow them.
        t.put(Some(7), b"k", None, b"v");
        t.delete(Some(7), b"k", b"v");
        assert_eq!(t.calls.load(Ordering::SeqCst), 2);
    }

    /// The same must hold through a trait object, which is how the engine
    /// actually holds triggers (`Vec<Arc<dyn Trigger>>` on `DatabaseImpl`). A
    /// default method reached through the vtable is a distinct dispatch path
    /// from one reached on the concrete type.
    #[test]
    fn defaults_are_reachable_through_a_trait_object() {
        let t: Arc<dyn Trigger> = Arc::new(RecordOnly {
            name: "boxed".to_string(),
            calls: AtomicUsize::new(0),
        });
        assert_eq!(t.name(), "boxed");
        t.add_trigger(None);
        t.commit(1);
        t.abort(1);
        t.remove_trigger(None);
    }

    /// A trigger that DOES override the transaction methods must have its own
    /// versions called, not the defaults — otherwise the default-no-op design
    /// would silently swallow real `TransactionTrigger` implementations.
    #[test]
    fn an_overriding_trigger_gets_its_own_lifecycle_methods() {
        struct Full {
            committed: AtomicUsize,
            aborted: AtomicUsize,
            added: AtomicUsize,
            removed: AtomicUsize,
        }
        impl Trigger for Full {
            fn name(&self) -> &str {
                "full"
            }
            fn put(
                &self,
                _t: Option<u64>,
                _k: &[u8],
                _o: Option<&[u8]>,
                _n: &[u8],
            ) {
            }
            fn delete(&self, _t: Option<u64>, _k: &[u8], _o: &[u8]) {}
            fn commit(&self, _txn_id: u64) {
                self.committed.fetch_add(1, Ordering::SeqCst);
            }
            fn abort(&self, _txn_id: u64) {
                self.aborted.fetch_add(1, Ordering::SeqCst);
            }
            fn add_trigger(&self, _txn_id: Option<u64>) {
                self.added.fetch_add(1, Ordering::SeqCst);
            }
            fn remove_trigger(&self, _txn_id: Option<u64>) {
                self.removed.fetch_add(1, Ordering::SeqCst);
            }
        }

        let t: Arc<dyn Trigger> = Arc::new(Full {
            committed: AtomicUsize::new(0),
            aborted: AtomicUsize::new(0),
            added: AtomicUsize::new(0),
            removed: AtomicUsize::new(0),
        });
        t.add_trigger(Some(1));
        t.commit(1);
        t.abort(2);
        t.remove_trigger(Some(1));

        // Downcast-free check: re-read through a concrete handle.
        let concrete = Arc::new(Full {
            committed: AtomicUsize::new(0),
            aborted: AtomicUsize::new(0),
            added: AtomicUsize::new(0),
            removed: AtomicUsize::new(0),
        });
        concrete.add_trigger(Some(1));
        concrete.commit(1);
        concrete.commit(2);
        concrete.abort(3);
        concrete.remove_trigger(None);
        assert_eq!(concrete.added.load(Ordering::SeqCst), 1);
        assert_eq!(concrete.committed.load(Ordering::SeqCst), 2);
        assert_eq!(concrete.aborted.load(Ordering::SeqCst), 1);
        assert_eq!(concrete.removed.load(Ordering::SeqCst), 1);
    }
}
