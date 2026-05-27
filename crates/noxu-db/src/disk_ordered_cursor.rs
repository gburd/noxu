//! Disk-ordered cursor for high-throughput unordered scans.
//!
//! A [`DiskOrderedCursor`] returns user records in approximate **on-disk
//! order** — the order in which their LN log entries appear in the
//! write-ahead log — rather than in B-tree key order.  This enables bulk
//! export, full-DB replication catch-up, and analytical scans without paying
//! the cost of B-tree traversal or lock acquisition.
//!
//! # Trade-offs vs [`crate::Cursor`]
//!
//! | Property | `Cursor` | `DiskOrderedCursor` |
//! |---|---|---|
//! | Order of returned keys | Key order (B-tree) | Disk order (log append order) |
//! | Lock acquisition       | Yes (per record)   | **No** |
//! | Isolation              | Per-txn isolation  | At best `READ_UNCOMMITTED` |
//! | Throughput             | Limited by random I/O on B-tree pages | Sequential log read |
//! | Deduplication of stale versions | Yes — only the latest committed value is returned | **No** by default — the same key may appear multiple times if it was updated, and a deleted key may still appear |
//!
//! # Consistency guarantees
//!
//! The records returned by a `DiskOrderedCursor` correspond to the state of
//! the database at the moment each LN was written to the log, which may
//! include uncommitted writes.  Concurrent inserts/updates/deletes performed
//! during the scan are **not** required to be visible.  Applications that
//! need a transactionally-consistent snapshot should drain in-flight writers
//! before opening the cursor (e.g. by holding a quiesce barrier).
//!
//! # Stale versions
//!
//! By default the cursor matches BDB JE: every LN that survives in the log
//! and belongs to one of the targeted databases is yielded, even if a newer
//! version of the same key follows.  This is the JE-correct behaviour for
//! bulk-export workflows that want to observe every committed mutation.
//! Set [`DiskOrderedCursorConfig::dedup_keys`] to `true` to filter stale
//! versions client-side (see field docs for caveats).
//!
//! # Producer thread
//!
//! Opening a cursor spawns a single background producer thread that reads
//! the log files sequentially and pushes decoded `(db_idx, key, data)`
//! tuples through a bounded channel.  The thread is joined when the cursor
//! is dropped or [`DiskOrderedCursor::close`] is called.

use std::marker::PhantomData;

use noxu_dbi::DiskOrderedCursorImpl;

use crate::database::Database;
use crate::database_entry::DatabaseEntry;
use crate::error::{NoxuError, Result};
use crate::operation_status::OperationStatus;

/// Configuration for a [`DiskOrderedCursor`].
///
/// Mirrors the field set of `DiskOrderedCursorConfig` but uses
/// Rust-idiomatic builder methods.  All fields have sensible defaults
/// matching JE.
#[derive(Debug, Clone)]
pub struct DiskOrderedCursorConfig {
    /// Maximum number of `(key, data)` entries the producer thread may queue
    /// before blocking.
    ///
    /// Default: `1000` (matches JE's `DOS_PRODUCER_QUEUE_SIZE`).
    pub queue_size: usize,
    /// Maximum number of LSNs the producer accumulates before yielding a
    /// batch downstream.  Currently advisory — the producer streams entries
    /// one at a time; this field is preserved for JE shape compatibility
    /// and future batched-fetch support.
    ///
    /// Default: `usize::MAX`.
    pub lsn_batch_size: usize,
    /// Maximum number of bytes the in-flight queue may occupy before the
    /// producer thread blocks.  Approximate — measured as the sum of key +
    /// data lengths of buffered entries.
    ///
    /// Default: `usize::MAX`.
    pub internal_memory_limit: usize,
    /// If `true`, only keys are read from the log; data is left empty.
    /// Slightly faster because the on-disk LN value bytes are skipped.
    ///
    /// Default: `false`.
    pub keys_only: bool,
    /// JE legacy flag — scan only BIN entries.  Honoured as an alias for
    /// `keys_only` in this implementation because Noxu's log iterator
    /// always emits LN payloads (no separate BIN-only scan path is
    /// available at this layer).
    ///
    /// Default: `false`.
    pub bins_only: bool,
    /// JE legacy flag — count records without materialising key/data.
    /// Currently honoured as `keys_only` plus a discard policy on data.
    /// `next()` still returns one `Success` per record so the application
    /// can compute the count by iterating.
    ///
    /// Default: `false`.
    pub count_only: bool,
    /// **Noxu extension.** If `true`, the cursor maintains a `HashSet` of
    /// `(db_idx, key)` pairs already returned and skips duplicates.  This
    /// can be expensive on large scans — the set grows linearly with
    /// distinct keys.  Default `false` matches JE.
    ///
    /// Note: even with `dedup_keys = true`, the cursor returns the *first*
    /// version of a key that the log scan encounters, which is the
    /// **oldest** version — not the latest.  For latest-only semantics
    /// the application must run a regular B-tree scan.
    pub dedup_keys: bool,
}

impl Default for DiskOrderedCursorConfig {
    fn default() -> Self {
        Self {
            queue_size: 1000,
            lsn_batch_size: usize::MAX,
            internal_memory_limit: usize::MAX,
            keys_only: false,
            bins_only: false,
            count_only: false,
            dedup_keys: false,
        }
    }
}

impl DiskOrderedCursorConfig {
    /// Returns a configuration with default settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the producer queue size.
    pub fn with_queue_size(mut self, queue_size: usize) -> Self {
        self.queue_size = queue_size.max(1);
        self
    }

    /// Sets the LSN batch size (advisory).
    pub fn with_lsn_batch_size(mut self, lsn_batch_size: usize) -> Self {
        self.lsn_batch_size = lsn_batch_size.max(1);
        self
    }

    /// Sets the internal memory limit in bytes.
    pub fn with_internal_memory_limit(
        mut self,
        internal_memory_limit: usize,
    ) -> Self {
        self.internal_memory_limit = internal_memory_limit.max(1);
        self
    }

    /// Sets keys-only mode (no data is read).
    pub fn with_keys_only(mut self, keys_only: bool) -> Self {
        self.keys_only = keys_only;
        self
    }

    /// Sets BINs-only mode (alias for `keys_only` in Noxu).
    pub fn with_bins_only(mut self, bins_only: bool) -> Self {
        self.bins_only = bins_only;
        self
    }

    /// Sets count-only mode.
    pub fn with_count_only(mut self, count_only: bool) -> Self {
        self.count_only = count_only;
        self
    }

    /// Enables client-side dedup of repeated keys.
    pub fn with_dedup_keys(mut self, dedup_keys: bool) -> Self {
        self.dedup_keys = dedup_keys;
        self
    }
}

/// A cursor that returns records in on-disk order rather than key order.
///
/// See the [module-level docs][self] for trade-offs and consistency
/// guarantees.
///
/// # Lifetime
///
/// The lifetime parameter `'env` ties the cursor to the borrow of the
/// `Database` slice passed to [`Database::open_disk_ordered_cursor`] and
/// [`open_disk_ordered_cursor_multi`], preventing the application from
/// closing a database while the cursor is still scanning.
///
/// # Example
///
/// ```ignore
/// use noxu_db::{DatabaseEntry, DiskOrderedCursorConfig, OperationStatus};
///
/// # fn example(db: &noxu_db::Database) -> noxu_db::Result<()> {
/// let mut cursor = db.open_disk_ordered_cursor(
///     DiskOrderedCursorConfig::new().with_queue_size(64),
/// )?;
/// let mut key = DatabaseEntry::new();
/// let mut data = DatabaseEntry::new();
///
/// while cursor.next(&mut key, &mut data)? == OperationStatus::Success {
///     // ...process key + data...
/// }
/// cursor.close()?;
/// # Ok(())
/// # }
/// ```
pub struct DiskOrderedCursor<'env> {
    inner: DiskOrderedCursorImpl,
    /// Cached value of the most-recent successful `next()` so that
    /// [`Self::current`] can re-emit it without re-reading the queue.
    last: Option<(Vec<u8>, Vec<u8>)>,
    /// `true` once [`Self::close`] has been called or the producer is
    /// drained.  After this, all operations return `OperationStatus::NotFound`.
    closed: bool,
    /// Borrows the slice of `&Database` handles to keep them alive.
    _marker: PhantomData<&'env ()>,
}

impl<'env> DiskOrderedCursor<'env> {
    pub(crate) fn from_impl(inner: DiskOrderedCursorImpl) -> Self {
        Self { inner, last: None, closed: false, _marker: PhantomData }
    }

    /// Advances the cursor to the next record.
    ///
    /// On `Ok(Success)` the `key` and `data` `DatabaseEntry`s are populated
    /// with the next record's bytes.  On `Ok(NotFound)` the cursor has
    /// reached end-of-log and no further records will be returned (it is
    /// safe — and idempotent — to call again).
    ///
    /// # Errors
    /// * [`NoxuError::CursorClosed`] if [`Self::close`] has been called.
    /// * [`NoxuError::IoError`] / [`NoxuError::LogChecksumMismatch`] if the
    ///   producer thread reported a permanent log-read error.
    pub fn next(
        &mut self,
        key: &mut DatabaseEntry,
        data: &mut DatabaseEntry,
    ) -> Result<OperationStatus> {
        if self.closed {
            return Err(NoxuError::CursorClosed);
        }
        match self.inner.next_entry()? {
            Some((k, d)) => {
                key.set_data(&k);
                data.set_data(&d);
                self.last = Some((k, d));
                Ok(OperationStatus::Success)
            }
            None => Ok(OperationStatus::NotFound),
        }
    }

    /// Returns the most recent record yielded by [`Self::next`] without
    /// advancing.
    ///
    /// Returns `OperationStatus::NotFound` if the cursor has not yet been
    /// advanced or has reached end-of-log.
    pub fn current(
        &self,
        key: &mut DatabaseEntry,
        data: &mut DatabaseEntry,
    ) -> Result<OperationStatus> {
        if self.closed {
            return Err(NoxuError::CursorClosed);
        }
        match &self.last {
            Some((k, d)) => {
                key.set_data(k);
                data.set_data(d);
                Ok(OperationStatus::Success)
            }
            None => Ok(OperationStatus::NotFound),
        }
    }

    /// Closes the cursor, signalling and joining the producer thread.
    ///
    /// Idempotent — calling `close` on an already-closed cursor is a no-op
    /// and returns `Ok(())`.  This is also called automatically when the
    /// cursor is dropped, so applications using RAII can rely on the drop
    /// glue rather than calling `close` explicitly.
    pub fn close(mut self) -> Result<()> {
        self.close_in_place()
    }

    fn close_in_place(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;
        self.inner.shutdown();
        Ok(())
    }
}

impl Drop for DiskOrderedCursor<'_> {
    fn drop(&mut self) {
        // Close-on-drop matches JE behaviour and ensures the producer
        // thread is always joined.
        let _ = self.close_in_place();
    }
}

impl Database {
    /// Opens a single-database disk-ordered cursor.
    ///
    /// This is a convenience for the common case; for a multi-database
    /// scan use [`open_disk_ordered_cursor_multi`].
    pub fn open_disk_ordered_cursor(
        &self,
        config: DiskOrderedCursorConfig,
    ) -> Result<DiskOrderedCursor<'_>> {
        let dbs: [&Database; 1] = [self];
        // Build a vector view that owns the slice for the call (the impl
        // copies what it needs out of the slice before returning).
        let inner = DiskOrderedCursorImpl::open(
            self.cached_log_manager().cloned(),
            vec![self.database_id_for_doc()],
            noxu_dbi::DiskOrderedCursorOptions {
                queue_size: config.queue_size,
                lsn_batch_size: config.lsn_batch_size,
                internal_memory_limit: config.internal_memory_limit,
                keys_only: config.keys_only
                    || config.bins_only
                    || config.count_only,
                dedup_keys: config.dedup_keys,
            },
        )?;
        // Validate the database is open and reserve the dbs binding to
        // satisfy the borrow checker (forces &self to live for 'env).
        self.check_open_for_doc()?;
        let _ = dbs;
        Ok(DiskOrderedCursor::from_impl(inner))
    }
}

/// Opens a disk-ordered cursor that scans entries from any of the given
/// databases.
///
/// All databases must belong to the same [`crate::Environment`].  The cursor
/// holds a borrow of the slice for its entire lifetime, which prevents any of
/// the databases from being closed mid-scan.
///
/// # Errors
///
/// * [`NoxuError::IllegalArgument`] if `databases` is empty or contains
///   handles from different environments.
/// * [`NoxuError::DatabaseClosed`] if any of the databases has been closed.
/// * [`NoxuError::IoError`] if the producer thread cannot be spawned.
pub fn open_disk_ordered_cursor_multi<'env>(
    databases: &'env [&'env Database],
    config: DiskOrderedCursorConfig,
) -> Result<DiskOrderedCursor<'env>> {
    if databases.is_empty() {
        return Err(NoxuError::IllegalArgument(
            "open_disk_ordered_cursor: at least one database is required"
                .into(),
        ));
    }

    // Each Database has a snapshot LogManager; verify they all share the
    // same one (i.e. the same environment).  If a database is non-WAL the
    // disk-ordered scan returns no entries, but the construction still
    // succeeds for API consistency.
    let log_manager = databases[0].cached_log_manager().cloned();
    for db in &databases[1..] {
        let other = db.cached_log_manager().cloned();
        match (&log_manager, &other) {
            (Some(a), Some(b)) if !std::sync::Arc::ptr_eq(a, b) => {
                return Err(NoxuError::IllegalArgument(
                    "open_disk_ordered_cursor: all databases must share \
                     the same environment"
                        .into(),
                ));
            }
            (Some(_), None) | (None, Some(_)) => {
                return Err(NoxuError::IllegalArgument(
                    "open_disk_ordered_cursor: all databases must share \
                     the same environment"
                        .into(),
                ));
            }
            _ => {}
        }
    }

    let mut db_ids = Vec::with_capacity(databases.len());
    for db in databases {
        db.check_open_for_doc()?;
        db_ids.push(db.database_id_for_doc());
    }

    let inner = DiskOrderedCursorImpl::open(
        log_manager,
        db_ids,
        noxu_dbi::DiskOrderedCursorOptions {
            queue_size: config.queue_size,
            lsn_batch_size: config.lsn_batch_size,
            internal_memory_limit: config.internal_memory_limit,
            keys_only: config.keys_only
                || config.bins_only
                || config.count_only,
            dedup_keys: config.dedup_keys,
        },
    )?;

    Ok(DiskOrderedCursor::from_impl(inner))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_defaults_match_je_shape() {
        let c = DiskOrderedCursorConfig::default();
        assert_eq!(c.queue_size, 1000);
        assert_eq!(c.lsn_batch_size, usize::MAX);
        assert_eq!(c.internal_memory_limit, usize::MAX);
        assert!(!c.keys_only);
        assert!(!c.bins_only);
        assert!(!c.count_only);
        assert!(!c.dedup_keys);
    }

    #[test]
    fn config_builders_clamp_zero_to_one() {
        let c = DiskOrderedCursorConfig::new()
            .with_queue_size(0)
            .with_lsn_batch_size(0)
            .with_internal_memory_limit(0);
        assert_eq!(c.queue_size, 1);
        assert_eq!(c.lsn_batch_size, 1);
        assert_eq!(c.internal_memory_limit, 1);
    }

    #[test]
    fn config_builders_chain() {
        let c = DiskOrderedCursorConfig::new()
            .with_queue_size(8)
            .with_keys_only(true)
            .with_dedup_keys(true);
        assert_eq!(c.queue_size, 8);
        assert!(c.keys_only);
        assert!(c.dedup_keys);
    }
}
