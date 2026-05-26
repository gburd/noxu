//! Internal cursor implementation.
//!
//!
//! The core traversal logic mirrors `CursorImpl.getNext()` (line 2546):
//!
//! ```text
//! while (bin != null) {
//!     latchBIN();
//!     if (forward ? ++index < nEntries : --index >= 0) {
//!         if record is valid: return it
//!     } else {
//!         bin = tree.getNextBin(anchorBIN) or tree.getPrevBin(anchorBIN)
//!         index = -1  (or nEntries for backward)
//!     }
//! }
//! ```
//!
//! Cross-BIN traversal is implemented: when the current BIN is exhausted,
//! `retrieve_next` calls `Tree::get_next_bin` / `Tree::get_prev_bin` to move
//! to the adjacent BIN and continues iteration there.

#[cfg(any(test, feature = "testing"))]
use std::cell::Cell;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicI64, Ordering};

use bytes::BytesMut;
use noxu_log::{LogEntryType, LogManager, Provisional, entry::LnLogEntry};
use noxu_tree::{BinEntry, Tree};
use noxu_txn::{LockManager, LockType, Locker, Txn};

use crate::dup_key_data;
use crate::throughput_stats::ThroughputStats;
use noxu_sync::RwLock;
use noxu_util::{Lsn, vlsn::NULL_VLSN};

use crate::{
    DbiError, GetMode, OperationStatus, PutMode, SearchMode,
    database_impl::DatabaseImpl,
};

/// Cursor states.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CursorState {
    NotInitialized,
    Initialized,
    Closed,
}

/// Result flags for cursor search operations.
pub const FOUND: u32 = 0x1;
pub const EXACT_KEY: u32 = 0x2;
pub const FOUND_LAST: u32 = 0x4;

/// Unique cursor ID generator.
static NEXT_CURSOR_ID: AtomicI64 = AtomicI64::new(1);

// Test-only hook: countdown to forced cursor failure.
//
// When the countdown is N (> 0), each `check_state`/`check_initialized` call
// decrements it by 1.  When it reaches 1 the decrement fires, it resets to 0,
// and the call returns `Err(DbiError::CursorClosed)`.
//
// `set_cursor_fail_after(1)` => fail on the next check (the 1st call).
// `set_cursor_fail_after(2)` => skip the 1st check, fail on the 2nd call.
//
// This lets `noxu-db` tests exercise both `map_err` closures inside a single
// `Database` method (e.g. `get()` has one closure on `search` and another on
// `get_current`).
#[cfg(any(test, feature = "testing"))]
thread_local! {
    static CURSOR_FAIL_COUNTDOWN: Cell<u32> = const { Cell::new(0) };
}

/// Set countdown so the Nth cursor-check call returns `DbiError::CursorClosed`.
/// `n = 1` → fail immediately on the next check.
/// Only available in test/testing builds.
#[cfg(any(test, feature = "testing"))]
pub fn set_cursor_fail_after(n: u32) {
    CURSOR_FAIL_COUNTDOWN.with(|c| c.set(n));
}

/// Clear the cursor fail countdown (idempotent).
#[cfg(any(test, feature = "testing"))]
pub fn clear_cursor_fail_flag() {
    CURSOR_FAIL_COUNTDOWN.with(|c| c.set(0));
}

/// Decrement the countdown and return `true` if this call should fail.
#[cfg(any(test, feature = "testing"))]
fn tick_fail() -> bool {
    CURSOR_FAIL_COUNTDOWN.with(|c| {
        let v = c.get();
        if v == 0 {
            false
        } else if v == 1 {
            c.set(0);
            true
        } else {
            c.set(v - 1);
            false
        }
    })
}

/// The internal implementation of a database cursor.
///
/// A CursorImpl tracks a position in a database and provides
/// get/put/delete operations. The cursor state machine ensures
/// proper initialization before operations.
///
/// a cursor tracks its position via a BIN reference and slot index.
/// This implementation wires cursor traversal to `noxu_tree::Tree`:
///
/// * `get_first` / `get_last` — use `Tree::get_first_node()` /
///   `Tree::get_last_node()`.
/// * `retrieve_next` — increments `current_index` within the BIN and, when
///   the BIN is exhausted, calls `Tree::get_next_bin()` /
///   `Tree::get_prev_bin()` to cross BIN boundaries
///   `CursorImpl.getNext()`).
/// * `search` — uses `Tree::search()` to locate the exact key.
/// * `put` / `delete` — mutate the tree in-place using `Tree::insert()` /
///   `Tree::delete()`.
///
/// (4096 lines in 7.5.11).
pub struct CursorImpl {
    /// Unique cursor ID (for debugging and hashCode).
    id: i64,
    /// The database this cursor operates on.
    db_impl: Arc<RwLock<DatabaseImpl>>,
    /// The locker (transaction or auto-commit) for this cursor.
    locker_id: i64,
    /// Current cursor state.
    state: CursorState,

    /// Current position: the key at the cursor's position.
    current_key: Option<Vec<u8>>,
    /// Current position: the data at the cursor's position.
    current_data: Option<Vec<u8>>,
    /// Current position: the LSN of the record.
    current_lsn: u64,
    /// Current position: the BIN index (slot in the current BIN).
    ///
    /// In this is `CursorImpl.index`. -1 means "before first entry".
    current_index: i32,

    /// The BIN Arc the cursor is currently pinned to, if any.
    ///
    /// Increments `BinStub.cursor_count` via `Tree::pin_bin()` so the
    /// evictor skips this BIN while the cursor is positioned on it.
    /// Cleared (and unpinned) when the cursor is closed or moves to a new BIN.
    current_bin_arc: Option<
        std::sync::Arc<noxu_tree::NodeRwLock<noxu_tree::tree::TreeNode>>,
    >,

    /// Write-ahead log manager for recording data operations.
    /// None for read-only cursors or cursors created outside a real env.
    log_manager: Option<Arc<LogManager>>,
    /// Lock manager for per-record read/write locking.
    /// None for cursors created outside a real env (e.g., unit tests).
    ///
    /// `CursorImpl.locker` — the locker calls `locker.lock(lsn,
    /// LockType.READ, ...)` via `lockLN()` before returning each record.
    lock_manager: Option<Arc<LockManager>>,

    /// Optional explicit transaction backing this cursor.
    ///
    /// When `Some`, write operations acquire locks via the `Txn` and record
    /// `WriteLockInfo` (abort before-images) so the transaction can undo each
    /// modification on abort.
    ///
    /// When `None` (auto-commit), write locks are acquired directly from
    /// `lock_manager` using the cursor's own `id` as the locker and released
    /// immediately after the write is logged (auto-commit semantics).
    ///
    /// (Txn subtype).
    txn_ref: Option<Arc<Mutex<Txn>>>,
    /// Throughput counters shared with all cursors on this database.
    throughput: Arc<ThroughputStats>,
}

impl CursorImpl {
    /// Creates a new CursorImpl for the given database.
    ///
    /// The cursor is initially in the NotInitialized state and must be
    /// positioned via a search operation before get/put/delete operations
    /// can be performed.
    ///
    /// # Arguments
    ///
    /// * `db_impl` - The database implementation this cursor operates on
    /// * `locker_id` - The locker (transaction) ID for this cursor
    pub fn new(db_impl: Arc<RwLock<DatabaseImpl>>, locker_id: i64) -> Self {
        let throughput = db_impl.read().throughput.clone();
        CursorImpl {
            id: NEXT_CURSOR_ID.fetch_add(1, Ordering::Relaxed),
            db_impl,
            locker_id,
            state: CursorState::NotInitialized,
            current_key: None,
            current_data: None,
            current_lsn: noxu_util::NULL_LSN.as_u64(),
            current_index: -1,
            current_bin_arc: None,
            log_manager: None,
            lock_manager: None,
            txn_ref: None,
            throughput,
        }
    }

    /// Creates a new CursorImpl wired to a WAL.
    ///
    /// Write operations (`put`, `delete`) will record `LnLogEntry` entries in
    /// the provided `LogManager` before mutating the in-memory tree.
    pub fn with_log_manager(
        db_impl: Arc<RwLock<DatabaseImpl>>,
        locker_id: i64,
        log_manager: Arc<LogManager>,
    ) -> Self {
        let throughput = db_impl.read().throughput.clone();
        CursorImpl {
            id: NEXT_CURSOR_ID.fetch_add(1, Ordering::Relaxed),
            db_impl,
            locker_id,
            state: CursorState::NotInitialized,
            current_key: None,
            current_data: None,
            current_lsn: noxu_util::NULL_LSN.as_u64(),
            current_index: -1,
            current_bin_arc: None,
            log_manager: Some(log_manager),
            lock_manager: None,
            txn_ref: None,
            throughput,
        }
    }

    /// Wires a lock manager for per-record locking.
    ///
    /// `CursorImpl` receiving a `Locker` from
    /// `DatabaseImpl.openCursor()`.  Returns `self` for builder-style chaining.
    pub fn with_lock_manager(mut self, lock_manager: Arc<LockManager>) -> Self {
        self.lock_manager = Some(lock_manager);
        self
    }

    /// Wires an explicit transaction for write-lock tracking.
    ///
    /// When set, write operations (`put`, `delete`) acquire WRITE locks via
    /// the `Txn` and record abort before-images in `WriteLockInfo`, enabling
    /// transaction rollback.
    ///
    /// Being constructed with a `Txn` locker.
    /// Returns `self` for builder-style chaining.
    pub fn with_txn(mut self, txn: Arc<Mutex<Txn>>) -> Self {
        self.txn_ref = Some(txn);
        self
    }

    /// Gets the before-image (old_data, old_lsn) for `key` from the tree.
    ///
    /// Returns `(None, NULL_LSN)` if the key does not exist (new insert).
    fn get_slot_before_image(&self, key: &[u8]) -> (Option<Vec<u8>>, u64) {
        let db = self.db_impl.read();
        if let Some(tree) = db.get_real_tree() {
            match Self::get_data_from_tree(tree, key) {
                Some((data, lsn)) => (Some(data), lsn),
                None => (None, noxu_util::NULL_LSN.as_u64()),
            }
        } else {
            (None, noxu_util::NULL_LSN.as_u64())
        }
    }

    /// Returns true if `key` exists in the committed tree.
    ///
    /// `CursorImpl.isPresent()` / lock-check path: with lock-based
    /// isolation, writes go directly to the BIN, so the tree reflects the
    /// current committed-or-locked state.  Callers that need to check
    /// existence before a `NoOverwrite`/`NoDupData` insert consult the tree
    /// directly; if a concurrent writer holds a WRITE lock the subsequent
    /// `lock_ln()` call will block until that writer commits or aborts.
    fn key_exists_in_view(&self, key: &[u8]) -> bool {
        let db = self.db_impl.read();
        if let Some(tree) = db.get_real_tree() {
            tree.search(key).map(|sr| sr.exact_parent_found).unwrap_or(false)
        } else {
            false
        }
    }

    /// Inserts or updates `key`/`data` at `new_lsn` in the B-tree.
    ///
    /// `CursorImpl.insertRecordInternal()` / `bin.updateEntry()`:
    /// writes go directly to the BIN immediately.  Read-committed isolation
    /// is enforced by the lock manager — concurrent readers block on the
    /// WRITE lock held by this cursor's txn until it commits or aborts.
    ///
    /// When the tree reports a **new** insert (`is_new == true`), increments
    /// the per-database entry count.
    fn apply_tree_insert(&self, key: Vec<u8>, data: Vec<u8>, new_lsn: Lsn) {
        let db = self.db_impl.read();
        if let Some(tree) = db.get_real_tree()
            && let Ok(is_new) = tree.insert(key, data, new_lsn)
            && is_new
        {
            db.increment_entry_count();
        }
    }

    /// Deletes `key` from the B-tree.
    ///
    /// `CursorImpl.deleteCurrentRecord()` / `bin.deleteEntry()`:
    /// the deletion is applied to the BIN immediately.  Concurrent readers
    /// that try to acquire a READ lock on the deleted slot's LSN block until
    /// the writer's WRITE lock is released (commit or abort).
    ///
    /// When the tree confirms the key was actually removed (`deleted == true`),
    /// decrements the per-database entry count (.
    /// counter).
    fn apply_tree_delete(&self, key: Vec<u8>, _del_lsn: Lsn) {
        let db = self.db_impl.read();
        if let Some(tree) = db.get_real_tree()
            && tree.delete(&key)
        {
            db.decrement_entry_count();
        }
    }

    /// Acquires a WRITE lock on `old_lsn` before writing to the log.
    ///
    /// For txn-backed cursors, calls `Txn::lock()` (lock persists until commit/abort).
    /// For auto-commit cursors (lock_manager only), uses cursor `id` as locker.
    /// A NULL_LSN (new insert with no prior version) does not need a pre-log lock.
    ///
    ///
    fn lock_write_before_log(&self, old_lsn: u64) -> Result<(), DbiError> {
        if old_lsn == noxu_util::NULL_LSN.as_u64() {
            return Ok(());
        }
        if let Some(txn) = &self.txn_ref {
            txn.lock()
                .unwrap()
                .lock(old_lsn, LockType::Write, false)
                .map_err(DbiError::TxnError)?;
        } else if let Some(lm) = &self.lock_manager {
            lm.lock(old_lsn, self.id, LockType::Write, false, false)
                .map_err(DbiError::TxnError)?;
        }
        Ok(())
    }

    /// Moves the write lock to `new_lsn` and records abort before-image info.
    ///
    /// For txn-backed cursors:
    ///   - If `old_lsn` is valid: moves lock via `Txn::move_write_lock_to_new_lsn()`.
    ///   - Otherwise (new insert): acquires a new write lock on `new_lsn`.
    ///   - Records abort info so the txn can undo on abort.
    ///   - Notes the log entry on the txn for TxnCommit/Abort chaining.
    ///
    /// For auto-commit cursors:
    ///   - Acquires write lock on `new_lsn`, releases both old and new locks
    ///     immediately (auto-commit releases after the write is logged).
    ///
    /// / `Txn.moveWriteLockToNewLsn()`.
    fn finalize_write_lock(
        &self,
        old_lsn: u64,
        new_lsn: Lsn,
        abort_key: Option<Vec<u8>>,
        abort_data: Option<Vec<u8>>,
    ) -> Result<(), DbiError> {
        let new_lsn_u64 = new_lsn.as_u64();
        // Deferred-write or no log manager: no LSN assigned, nothing to lock.
        if new_lsn_u64 == noxu_util::NULL_LSN.as_u64() {
            return Ok(());
        }

        if let Some(txn) = &self.txn_ref {
            let db_id = self.db_impl.read().get_id().id() as u64;
            let mut guard = txn.lock().unwrap();
            if old_lsn != noxu_util::NULL_LSN.as_u64() {
                // Move the existing write lock from old slot to new slot.
                guard
                    .move_write_lock_to_new_lsn(old_lsn, new_lsn_u64)
                    .map_err(DbiError::TxnError)?;
            } else {
                // New insert: no old lock to move — acquire a fresh write lock.
                guard
                    .lock(new_lsn_u64, LockType::Write, false)
                    .map_err(DbiError::TxnError)?;
            }
            let abort_known_deleted = old_lsn == noxu_util::NULL_LSN.as_u64();
            guard.set_write_lock_abort_info(
                new_lsn_u64,
                old_lsn,
                abort_key,
                abort_data,
                abort_known_deleted,
                db_id,
            );
            guard.note_log_entry(new_lsn_u64);
        } else if let Some(lm) = &self.lock_manager {
            // Auto-commit: acquire write lock, then release immediately.
            lm.lock(new_lsn_u64, self.id, LockType::Write, false, false)
                .map_err(DbiError::TxnError)?;
            if old_lsn != noxu_util::NULL_LSN.as_u64() {
                let _ = lm.release(old_lsn, self.id);
            }
            let _ = lm.release(new_lsn_u64, self.id);
        }
        Ok(())
    }

    /// Returns true if the underlying database uses sorted duplicates.
    ///
    /// When true, every (key, data) pair is stored as a two-part composite
    /// key via `dup_key_data::combine()` and the tree uses a custom comparator.
    #[inline]
    fn is_sorted_dup(&self) -> bool {
        self.db_impl.read().get_sorted_duplicates()
    }

    /// Returns the unique cursor ID.
    ///
    /// Used for debugging and cursor tracking.
    pub fn get_id(&self) -> i64 {
        self.id
    }

    /// Returns the database this cursor operates on.
    pub fn get_database(&self) -> &Arc<RwLock<DatabaseImpl>> {
        &self.db_impl
    }

    /// Returns the locker ID.
    pub fn get_locker_id(&self) -> i64 {
        self.locker_id
    }

    /// Returns true if the cursor is initialized (positioned on a record).
    pub fn is_initialized(&self) -> bool {
        self.state == CursorState::Initialized
    }

    /// Returns true if the cursor is closed.
    pub fn is_closed(&self) -> bool {
        self.state == CursorState::Closed
    }

    /// Returns the current key, if positioned.
    pub fn get_current_key(&self) -> Option<&[u8]> {
        self.current_key.as_deref()
    }

    /// Returns the current data, if positioned.
    pub fn get_current_data(&self) -> Option<&[u8]> {
        self.current_data.as_deref()
    }

    /// Returns the current LSN, if positioned.
    pub fn get_current_lsn(&self) -> u64 {
        self.current_lsn
    }

    /// Checks the cursor is not closed.
    fn check_state(&self) -> Result<(), DbiError> {
        #[cfg(any(test, feature = "testing"))]
        if tick_fail() {
            return Err(DbiError::CursorClosed);
        }
        match self.state {
            CursorState::Closed => Err(DbiError::CursorClosed),
            _ => Ok(()),
        }
    }

    /// Checks the cursor is initialized.
    fn check_initialized(&self) -> Result<(), DbiError> {
        #[cfg(any(test, feature = "testing"))]
        if tick_fail() {
            return Err(DbiError::CursorClosed);
        }
        match self.state {
            CursorState::Closed => Err(DbiError::CursorClosed),
            CursorState::NotInitialized => Err(DbiError::CursorNotInitialized),
            CursorState::Initialized => Ok(()),
        }
    }

    /// Positions the cursor at a specific key.
    ///
    /// / `CursorImpl.searchRange()`.
    ///
    /// Uses `Tree::search(key)` to locate the BIN slot for the key:
    ///
    /// * `SearchMode::Set` / `SearchMode::Both` — exact key match required.
    ///   Returns `NotFound` if the key is not present.
    /// * `SearchMode::SetRange` / `SearchMode::BothRange` — positions at the
    ///   first key >= the search key (range search).  Currently degrades to
    ///   an exact-match check; full range support requires iterating forward
    ///   until the key is >= the search key.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to search for
    /// * `data` - Optional data for Both/BothRange modes
    /// * `search_mode` - The search mode (Set, Both, SetRange, BothRange)
    ///
    /// # Returns
    ///
    /// * `Success` if the key was found and cursor positioned
    /// * `NotFound` if the key does not exist
    pub fn search(
        &mut self,
        key: &[u8],
        data: Option<&[u8]>,
        search_mode: SearchMode,
    ) -> Result<OperationStatus, DbiError> {
        self.check_state()?;

        let is_dup = self.is_sorted_dup();

        if is_dup {
            return self.search_dup(key, data, search_mode);
        }

        // Non-dup path (original logic).
        let found = {
            let db = self.db_impl.read();
            if let Some(tree) = db.get_real_tree() {
                tree.search(key)
                    .map(|sr| sr.exact_parent_found)
                    .unwrap_or(false)
            } else {
                false
            }
        };

        match search_mode {
            SearchMode::Set | SearchMode::Both => {
                if found {
                    let result: Option<(Vec<u8>, u64)> = {
                        let db = self.db_impl.read();
                        if let Some(tree) = db.get_real_tree() {
                            Self::get_data_from_tree(tree, key)
                        } else {
                            None
                        }
                    };
                    let (slot_data, slot_lsn) = match result {
                        Some((d, l)) => (Some(d), l),
                        None => (
                            data.map(|d| d.to_vec()),
                            noxu_util::NULL_LSN.as_u64(),
                        ),
                    };
                    // If a writer held the write lock when we called lock_ln,
                    // our pre-fetched slot_data is stale — re-read from the BIN
                    // after the writer commits/aborts.  If lock_ln returned
                    // immediately (no contention), slot_data is still valid.
                    let contended = self.lock_ln(slot_lsn)?;
                    let final_data = if contended {
                        let db = self.db_impl.read();
                        db.get_real_tree()
                            .and_then(|tree| {
                                Self::get_data_from_tree(tree, key)
                            })
                            .map(|(d, _)| d)
                            .map(Some)
                            .unwrap_or(slot_data)
                    } else {
                        slot_data
                    };
                    self.current_key = Some(key.to_vec());
                    self.current_data = final_data;
                    self.current_lsn = slot_lsn;
                    self.current_index = 0;
                    self.state = CursorState::Initialized;
                    // Pin the BIN the cursor is now positioned on.
                    let bin_arc = {
                        let db = self.db_impl.read();
                        db.get_real_tree().and_then(|tree| {
                            tree.get_root()
                                .and_then(|r| Self::find_bin_for_key(r, key))
                        })
                    };
                    self.update_bin_pin(bin_arc);
                    Ok(OperationStatus::Success)
                } else {
                    Ok(OperationStatus::NotFound)
                }
            }
            SearchMode::SetRange | SearchMode::BothRange => {
                if found {
                    let result: Option<(Vec<u8>, u64)> = {
                        let db = self.db_impl.read();
                        if let Some(tree) = db.get_real_tree() {
                            Self::get_data_from_tree(tree, key)
                        } else {
                            None
                        }
                    };
                    let (slot_data, slot_lsn) = match result {
                        Some((d, l)) => (Some(d), l),
                        None => (
                            data.map(|d| d.to_vec()),
                            noxu_util::NULL_LSN.as_u64(),
                        ),
                    };
                    let contended = self.lock_ln(slot_lsn)?;
                    let final_data = if contended {
                        let db = self.db_impl.read();
                        db.get_real_tree()
                            .and_then(|tree| {
                                Self::get_data_from_tree(tree, key)
                            })
                            .map(|(d, _)| d)
                            .map(Some)
                            .unwrap_or(slot_data)
                    } else {
                        slot_data
                    };
                    self.current_key = Some(key.to_vec());
                    self.current_data = final_data;
                    self.current_lsn = slot_lsn;
                    self.current_index = 0;
                    self.state = CursorState::Initialized;
                    // Pin the BIN the cursor is now positioned on.
                    let bin_arc = {
                        let db = self.db_impl.read();
                        db.get_real_tree().and_then(|tree| {
                            tree.get_root()
                                .and_then(|r| Self::find_bin_for_key(r, key))
                        })
                    };
                    self.update_bin_pin(bin_arc);
                    Ok(OperationStatus::Success)
                } else {
                    let next_entry: Option<(Vec<u8>, Vec<u8>, u64)> = {
                        let db = self.db_impl.read();
                        if let Some(tree) = db.get_real_tree() {
                            Self::find_range_entry(tree, key)
                        } else {
                            None
                        }
                    };
                    match next_entry {
                        Some((k, v, lsn)) => {
                            self.lock_ln(lsn)?;
                            // Pin the BIN for the range-found key.
                            let bin_arc = {
                                let db = self.db_impl.read();
                                db.get_real_tree().and_then(|tree| {
                                    tree.get_root().and_then(|r| {
                                        Self::find_bin_for_key(r, &k)
                                    })
                                })
                            };
                            self.current_key = Some(k);
                            self.current_data = Some(v);
                            self.current_lsn = lsn;
                            self.current_index = 0;
                            self.state = CursorState::Initialized;
                            self.update_bin_pin(bin_arc);
                            Ok(OperationStatus::Success)
                        }
                        None => Ok(OperationStatus::NotFound),
                    }
                }
            }
        }
    }

    /// Sorted-dup variant of `search()`.
    ///
    /// For sorted-dup databases (key, data) pairs are stored as two-part
    /// composite keys `[key][data][packed_key_len]`.  This method builds the
    /// appropriate two-part search key and delegates to the tree's
    /// comparator-aware range finder.
    ///
    /// Dup path from 7.5.
    fn search_dup(
        &mut self,
        key: &[u8],
        data: Option<&[u8]>,
        search_mode: SearchMode,
    ) -> Result<OperationStatus, DbiError> {
        let search_two_part_key: Vec<u8> = match search_mode {
            // Both / BothRange: search for the exact (key, data) pair.
            SearchMode::Both | SearchMode::BothRange => {
                dup_key_data::combine(key, data.unwrap_or(b""))
            }
            // Set / SetRange: position at the first entry whose primary key
            // >= `key` — use the lower bound (smallest possible two-part key
            // for this primary key).
            SearchMode::Set | SearchMode::SetRange => {
                dup_key_data::lower_bound(key)
            }
        };

        let entry: Option<(Vec<u8>, Vec<u8>, u64)> = {
            let db = self.db_impl.read();
            if let Some(tree) = db.get_real_tree() {
                tree.first_entry_at_or_after(&search_two_part_key)
            } else {
                None
            }
        };

        match entry {
            Some((raw_key, _, slot_lsn)) => {
                // raw_key is the two-part key found; check that the primary
                // key part matches what was requested (for Set and Both).
                let matches = match search_mode {
                    SearchMode::Set => dup_key_data::matches_key(&raw_key, key),
                    SearchMode::Both => raw_key == search_two_part_key,
                    SearchMode::SetRange => {
                        // Any key >= the search key is valid.
                        true
                    }
                    SearchMode::BothRange => {
                        // Position at the first (key, data) where data >=
                        // the given data; primary key must still match.
                        dup_key_data::matches_key(&raw_key, key)
                    }
                };
                if matches {
                    self.lock_ln(slot_lsn)?;
                    // Store the raw two-part key; get_current() will decode it.
                    self.current_key = Some(raw_key);
                    self.current_data = None; // decoded lazily in get_current()
                    self.current_lsn = slot_lsn;
                    self.current_index = 0;
                    self.state = CursorState::Initialized;
                    Ok(OperationStatus::Success)
                } else {
                    Ok(OperationStatus::NotFound)
                }
            }
            None => Ok(OperationStatus::NotFound),
        }
    }

    /// Acquires a read lock on a log record by LSN.
    ///
    /// `CursorImpl.lockLN(LockType.READ)`.  When no lock manager
    /// is wired (read-only cursors / unit tests) this is a no-op.
    ///
    /// For txn-backed cursors the lock is tracked in the `Txn` and held until
    /// commit/abort.  For auto-commit cursors the lock is acquired (to wait
    /// for any current exclusive writer to finish) and then released
    /// immediately — mirroring `AutoTxn` single-operation semantics.
    ///
    /// Returns an error only when the lock would deadlock or the locker is
    /// invalid; `NULL_LSN` records are skipped (lock-free slots).
    ///
    /// Returns `Ok(contended)` where `contended = true` means the lock was
    /// not immediately available — a concurrent writer held an exclusive lock
    /// and we had to wait.  When `contended` is `true`, any data pre-fetched
    /// before calling this method may be stale (the writer may have committed
    /// or aborted during the wait), and the caller should re-read from the BIN.
    /// When `contended` is `false`, the lock was granted immediately with no
    /// intervening write, so pre-fetched data remains valid.
    fn lock_ln(&self, lsn: u64) -> Result<bool, DbiError> {
        if lsn == noxu_util::NULL_LSN.as_u64() {
            return Ok(false);
        }
        if let Some(txn) = &self.txn_ref {
            let mut guard = txn.lock().unwrap();
            // Try non-blocking first to detect write contention without waiting.
            let contended = match guard.lock(lsn, LockType::Read, true) {
                Ok(_) => false, // granted immediately — no concurrent writer
                Err(noxu_txn::TxnError::LockNotAvailable { .. }) => {
                    // A writer holds the lock; block until they commit/abort.
                    guard
                        .lock(lsn, LockType::Read, false)
                        .map_err(DbiError::TxnError)?;
                    true
                }
                Err(e) => return Err(DbiError::TxnError(e)),
            };
            // Read-committed: release the read lock immediately after each
            // operation so concurrent writers are not blocked for the txn
            // duration.  Under serializable isolation the lock is held until
            // commit/abort (tracked in Txn.read_locks).
            if guard.is_read_committed_isolation() {
                guard.release_lock(lsn).map_err(DbiError::TxnError)?;
            }
            Ok(contended)
        } else if let Some(lm) = &self.lock_manager {
            // Auto-commit: detect contention via non-blocking attempt first.
            let contended =
                match lm.lock(lsn, self.id, LockType::Read, true, false) {
                    Ok(_) => {
                        lm.release(lsn, self.id).map_err(DbiError::TxnError)?;
                        false
                    }
                    Err(noxu_txn::TxnError::LockNotAvailable { .. }) => {
                        lm.lock(lsn, self.id, LockType::Read, false, false)
                            .map_err(DbiError::TxnError)?;
                        lm.release(lsn, self.id).map_err(DbiError::TxnError)?;
                        true
                    }
                    Err(e) => return Err(DbiError::TxnError(e)),
                };
            Ok(contended)
        } else {
            Ok(false)
        }
    }

    /// Fetches the data associated with `key` from a tree (BIN-level lookup).
    ///
    /// Returns `(data, slot_lsn)` so the caller can acquire a read lock.
    ///
    /// Data-read path in `CursorImpl.lockAndGetCurrent()`.
    fn get_data_from_tree(tree: &Tree, key: &[u8]) -> Option<(Vec<u8>, u64)> {
        use noxu_tree::tree::TreeNode;
        let root = tree.get_root()?;
        // Descend to the BIN that should contain `key` (not always the leftmost).
        let bin_arc = Self::find_bin_for_key(root, key)?;
        let guard = bin_arc.read();
        match &*guard {
            TreeNode::Bottom(bin) => {
                // BIN entries store compressed (suffix) keys under the BIN's
                // key_prefix. If the key doesn't start with the prefix,
                // it is not in this BIN — return None rather than panicking.
                if !bin.key_prefix.is_empty()
                    && !key.starts_with(bin.key_prefix.as_slice())
                {
                    return None;
                }
                let suffix = bin.compress_key(key);
                bin.entries
                    .iter()
                    .find(|e| e.key.as_slice() == suffix.as_slice())
                    .map(|e| {
                        (e.data.clone().unwrap_or_default(), e.lsn.as_u64())
                    })
            }
            _ => None,
        }
    }

    /// Finds the first entry in the tree whose key >= `key`.
    ///
    /// Returns `(key, data, slot_lsn)` so the caller can acquire a read lock.
    ///
    /// # Algorithm
    ///
    /// SearchGte is a two-step probe:
    ///
    ///   1. Locate the BIN that *should* contain `key` via
    ///      `find_bin_for_key` and scan it for the smallest entry whose
    ///      full key is `>= key`.  The seed `key` is *not* required to
    ///      share the BIN's learned `key_prefix` — we explicitly handle
    ///      the three legal seed/`key_prefix` relationships:
    ///
    ///      * `key.starts_with(key_prefix)` — cheap suffix comparison;
    ///        the stored `entries[i].key` are suffixes under that prefix,
    ///        so we compare against `&key[plen..]`.
    ///      * `key < key_prefix` lexicographically — every full key in
    ///        this BIN starts with `key_prefix` and is therefore strictly
    ///        greater than `key`; the answer is `entries[0]`.  This
    ///        includes the common case of a short search seed (e.g.
    ///        `b"K\0"`) on a BIN whose learned prefix has grown longer
    ///        than the seed (`b"K\0bucket\0…"`).
    ///      * `key > key_prefix` lexicographically — every full key in
    ///        this BIN is strictly less than `key`; nothing here matches,
    ///        fall through to step 2.
    ///
    ///   2. If step 1 returned nothing (either no entry in the chosen
    ///      BIN satisfies `>= key`, or the BIN was empty / the seed sits
    ///      lex-after the BIN's prefix) call `Tree::get_next_bin(key)`
    ///      and return its first entry, which by B+tree invariants is
    ///      strictly greater than `key`.
    ///
    /// # Why step 2's first entry is the correct answer
    ///
    /// `find_bin_for_key` descends by picking, at each internal level,
    /// the largest separator `<= key`.  If it lands on BIN `B` reached
    /// via slot `p` of some ancestor, then `separator(p) <= key` and
    /// (when slot `p+1` exists) `separator(p+1) > key` strictly —
    /// otherwise descent would have picked `p+1`.  By the B+tree
    /// key-range invariant every key in the subtree rooted at `slot(p+1)`
    /// is `>= separator(p+1) > key`.  `Tree::get_next_bin` returns the
    /// leftmost BIN of exactly that next-sibling subtree, so its first
    /// entry is the smallest key in the whole tree that is `> key`.
    /// One probe, deterministically correct — no looping needed.
    ///
    /// # Locking
    ///
    /// The step-1 BIN read lock is released before step 2 fires so that
    /// `get_next_bin`'s own latch-coupled descent is unconstrained and
    /// other threads (especially writers crossing this BIN) are not
    /// blocked on a lock we no longer need.
    ///
    /// # Empty intermediate BINs
    ///
    /// If the chosen BIN is empty *and* `get_next_bin` returns an empty
    /// BIN (a transient state under delete-heavy workloads, before the
    /// cleaner has collapsed it), this returns `None` and the caller
    /// reports `NotFound`.  This matches `Get::Next`'s behaviour today;
    /// see also the follow-up note in
    /// `cursor_search_gte_skips_past_empty_bin_is_pre_existing_limit`.
    fn find_range_entry(
        tree: &Tree,
        key: &[u8],
    ) -> Option<(Vec<u8>, Vec<u8>, u64)> {
        use noxu_tree::tree::TreeNode;

        // Step 1: scan the BIN that should contain `key`.  The read lock
        // is dropped at the end of this block before step 2 runs.
        let in_current: Option<(Vec<u8>, Vec<u8>, u64)> = {
            let root = tree.get_root()?;
            // Use find_bin_for_key so range searches also work for non-leftmost BINs.
            let bin_arc = Self::find_bin_for_key(root, key)?;
            let guard = bin_arc.read();
            match &*guard {
                TreeNode::Bottom(bin) => {
                    let plen = bin.key_prefix.len();

                    if plen != 0 && !key.starts_with(bin.key_prefix.as_slice())
                    {
                        // Seed does not share this BIN's learned prefix.
                        // Decide by lex-comparing seed against key_prefix;
                        // never call compress_key (which requires `starts_with`).
                        if key < bin.key_prefix.as_slice() {
                            // Every key in this BIN is > seed.
                            bin.entries.first().and_then(|e| {
                                bin.get_full_key(0).map(|fk| {
                                    (
                                        fk,
                                        e.data.clone().unwrap_or_default(),
                                        e.lsn.as_u64(),
                                    )
                                })
                            })
                        } else {
                            // Every key in this BIN is < seed; let step 2
                            // handle it.
                            None
                        }
                    } else {
                        // Cheap path: suffix comparison.
                        let suffix = &key[plen..];
                        bin.entries
                            .iter()
                            .enumerate()
                            .find(|(_, e)| e.key.as_slice() >= suffix)
                            .and_then(|(i, e)| {
                                bin.get_full_key(i).map(|fk| {
                                    (
                                        fk,
                                        e.data.clone().unwrap_or_default(),
                                        e.lsn.as_u64(),
                                    )
                                })
                            })
                    }
                }
                _ => None,
            }
            // bin_arc read lock dropped here.
        };

        if let Some(r) = in_current {
            return Some(r);
        }

        // Step 2: chosen BIN had nothing >= key.  By B+tree invariants the
        // first entry of the next BIN is strictly > key, which satisfies
        // SearchGte.  No iteration: one call, one answer.
        let next = tree.get_next_bin(key)?;
        let e = next.into_iter().next()?;
        Some((e.key, e.data.unwrap_or_default(), e.lsn.as_u64()))
    }

    /// Descends from the given node to the leftmost BIN, returning its Arc.
    fn descend_to_bin(
        node: std::sync::Arc<noxu_tree::NodeRwLock<noxu_tree::tree::TreeNode>>,
    ) -> Option<std::sync::Arc<noxu_tree::NodeRwLock<noxu_tree::tree::TreeNode>>>
    {
        use noxu_tree::tree::TreeNode;
        let mut current = node;
        loop {
            let (is_bin, child) = {
                let g = current.read();
                let is_bin = g.is_bin();
                let child = if !is_bin {
                    match &*g {
                        TreeNode::Internal(n) => {
                            n.entries.first().and_then(|e| e.child.clone())
                        }
                        _ => None,
                    }
                } else {
                    None
                };
                (is_bin, child)
            };
            if is_bin {
                return Some(current);
            }
            current = child?;
        }
    }

    /// Descends from the given node to the rightmost BIN, returning its Arc.
    fn descend_to_last_bin(
        node: std::sync::Arc<noxu_tree::NodeRwLock<noxu_tree::tree::TreeNode>>,
    ) -> Option<std::sync::Arc<noxu_tree::NodeRwLock<noxu_tree::tree::TreeNode>>>
    {
        use noxu_tree::tree::TreeNode;
        let mut current = node;
        loop {
            let (is_bin, child) = {
                let g = current.read();
                let is_bin = g.is_bin();
                let child = if !is_bin {
                    match &*g {
                        TreeNode::Internal(n) => {
                            n.entries.last().and_then(|e| e.child.clone())
                        }
                        _ => None,
                    }
                } else {
                    None
                };
                (is_bin, child)
            };
            if is_bin {
                return Some(current);
            }
            current = child?;
        }
    }

    /// Positions the cursor at the first (smallest) record in the database.
    ///
    /// .
    ///
    /// Uses `Tree::get_first_node()` to descend to the leftmost BIN, then
    /// positions the cursor at slot 0.
    ///
    /// # Returns
    ///
    /// * `Success` if the tree is non-empty
    /// * `NotFound` if the tree is empty
    pub fn get_first(&mut self) -> Result<OperationStatus, DbiError> {
        self.check_state()?;

        let result: Option<(
            Vec<u8>,
            Vec<u8>,
            i32,
            u64,
            std::sync::Arc<noxu_tree::NodeRwLock<noxu_tree::tree::TreeNode>>,
        )> = {
            let db = self.db_impl.read();
            if let Some(tree) = db.get_real_tree() {
                if tree.is_empty() {
                    None
                } else {
                    use noxu_tree::tree::TreeNode;
                    tree.get_root().and_then(|r| {
                        let bin_arc = Self::descend_to_bin(r)?;
                        let (key, data, lsn) = {
                            let g = bin_arc.read();
                            match &*g {
                                TreeNode::Bottom(bin) => {
                                    if bin.entries.is_empty() {
                                        return None;
                                    }
                                    (
                                        bin.get_full_key(0).unwrap_or_default(),
                                        bin.entries[0]
                                            .data
                                            .clone()
                                            .unwrap_or_default(),
                                        bin.entries[0].lsn.as_u64(),
                                    )
                                }
                                _ => return None,
                            }
                        };
                        Some((key, data, 0i32, lsn, bin_arc))
                    })
                }
            } else {
                None
            }
        };

        match result {
            Some((key, data, idx, lsn, bin_arc)) => {
                self.lock_ln(lsn)?;
                self.current_key = Some(key);
                self.current_data = Some(data);
                self.current_lsn = lsn;
                self.current_index = idx;
                self.state = CursorState::Initialized;
                self.update_bin_pin(Some(bin_arc));
                Ok(OperationStatus::Success)
            }
            None => Ok(OperationStatus::NotFound),
        }
    }

    /// Positions the cursor at the last (largest) record in the database.
    ///
    /// .
    ///
    /// Uses `Tree::get_last_node()` to descend to the rightmost BIN, then
    /// positions the cursor at the last slot.
    ///
    /// # Returns
    ///
    /// * `Success` if the tree is non-empty
    /// * `NotFound` if the tree is empty
    pub fn get_last(&mut self) -> Result<OperationStatus, DbiError> {
        self.check_state()?;

        let result: Option<(
            Vec<u8>,
            Vec<u8>,
            i32,
            u64,
            std::sync::Arc<noxu_tree::NodeRwLock<noxu_tree::tree::TreeNode>>,
        )> = {
            let db = self.db_impl.read();
            if let Some(tree) = db.get_real_tree() {
                if tree.is_empty() {
                    None
                } else {
                    use noxu_tree::tree::TreeNode;
                    tree.get_root().and_then(|r| {
                        let bin_arc = Self::descend_to_last_bin(r)?;
                        let (key, data, last_idx, lsn) = {
                            let g = bin_arc.read();
                            match &*g {
                                TreeNode::Bottom(bin) => {
                                    let n = bin.entries.len();
                                    if n == 0 {
                                        return None;
                                    }
                                    let last_idx = n - 1;
                                    (
                                        bin.get_full_key(last_idx)
                                            .unwrap_or_default(),
                                        bin.entries[last_idx]
                                            .data
                                            .clone()
                                            .unwrap_or_default(),
                                        last_idx as i32,
                                        bin.entries[last_idx].lsn.as_u64(),
                                    )
                                }
                                _ => return None,
                            }
                        };
                        Some((key, data, last_idx, lsn, bin_arc))
                    })
                }
            } else {
                None
            }
        };

        match result {
            Some((key, data, idx, lsn, bin_arc)) => {
                self.lock_ln(lsn)?;
                self.current_key = Some(key);
                self.current_data = Some(data);
                self.current_lsn = lsn;
                self.current_index = idx;
                self.state = CursorState::Initialized;
                self.update_bin_pin(Some(bin_arc));
                Ok(OperationStatus::Success)
            }
            None => Ok(OperationStatus::NotFound),
        }
    }

    /// Retrieves the current record.
    ///
    /// Returns the key and data at the cursor's current position.
    ///
    /// # Returns
    ///
    /// A tuple of (key, data) for the current record.
    ///
    /// # Errors
    ///
    /// * `CursorNotInitialized` if the cursor is not positioned on a record
    /// * `CursorClosed` if the cursor has been closed
    pub fn get_current(&self) -> Result<(Vec<u8>, Vec<u8>), DbiError> {
        self.check_initialized()?;

        let raw_key =
            self.current_key.clone().ok_or(DbiError::CursorNotInitialized)?;
        let raw_data = self.current_data.clone().unwrap_or_default();

        // For sorted-dup databases the tree stores two-part composite keys.
        // current_key holds the raw two-part key; split it for the caller.
        if self.is_sorted_dup()
            && let Some((pk, data)) = dup_key_data::split(&raw_key)
        {
            return Ok((pk, data));
        }
        Ok((raw_key, raw_data))
    }

    /// Returns true if the slot the cursor is positioned on has been deleted
    /// since the cursor was last positioned.
    ///
    /// : analogous to checking KNOWN_DELETED_BIT / entry removal on
    /// Cursor.getCurrentLN() path — returns KEYEMPTY when the record is gone.
    pub fn is_current_slot_deleted(&self) -> bool {
        use noxu_tree::tree::TreeNode;
        let current_key = match &self.current_key {
            Some(k) => k,
            None => return false,
        };
        let bin_arc = match &self.current_bin_arc {
            Some(a) => a,
            None => return false,
        };
        let idx = self.current_index as usize;
        let guard = bin_arc.read();
        if let TreeNode::Bottom(bin) = &*guard {
            if idx >= bin.entries.len() {
                return true; // entry was removed
            }
            let plen = bin.key_prefix.len();
            let expected_suffix: &[u8] =
                if plen == 0 || current_key.len() <= plen {
                    current_key.as_slice()
                } else {
                    &current_key[plen..]
                };
            let stored = bin.entries[idx].key.as_slice();
            if stored != expected_suffix {
                return true; // different key at this index = deleted and shifted
            }
            bin.entries[idx].known_deleted
        } else {
            false
        }
    }

    /// Moves the cursor to the next/previous record.
    ///
    /// .
    ///
    /// Advances `current_index` within the current BIN.  When the BIN is
    /// exhausted (forward: `index >= nEntries`; backward: `index < 0`) the
    /// cursor moves to the adjacent BIN via `Tree::get_next_bin()` /
    /// `Tree::get_prev_bin()`, mirroring call to
    /// `tree.getNextBin(anchorBIN)` / `tree.getPrevBin(anchorBIN)`.
    ///
    /// The GetMode parameter controls direction and duplicate handling:
    ///
    /// * `Next` / `NextNoDup` / `NextDup` — move forward
    /// * `Prev` / `PrevNoDup` / `PrevDup` — move backward
    ///
    /// # Returns
    ///
    /// * `Success` if positioned on a new record
    /// * `NotFound` if there are no more records in that direction
    pub fn retrieve_next(
        &mut self,
        mode: GetMode,
    ) -> Result<OperationStatus, DbiError> {
        self.check_state()?;

        if self.state == CursorState::NotInitialized {
            return Ok(OperationStatus::NotFound);
        }

        let is_dup = self.is_sorted_dup();

        // BDB-JE contract: NEXT_DUP / PREV_DUP advance only within the
        // duplicate-set of the current key.  On a non-sorted-dup database
        // every key has exactly one record, so there can never be another
        // duplicate of the current position — the only correct answer is
        // NotFound.  Without this early-return, the dup-filter below is
        // gated on `is_dup` and the cursor would silently degenerate into
        // plain Next / Prev semantics, returning the next *different* key
        // and violating the documented contract.  See
        // `docs/src/internal/api-audit-2026-05-cursor.md` Finding 5.
        if !is_dup && matches!(mode, GetMode::NextDup | GetMode::PrevDup) {
            return Ok(OperationStatus::NotFound);
        }

        // For NextDup/PrevDup/NextNoDup/PrevNoDup, capture the primary key of
        // the current position before advancing.
        let current_primary_key: Option<Vec<u8>> = if is_dup {
            self.current_key.as_ref().and_then(|raw| dup_key_data::get_key(raw))
        } else {
            None
        };

        let forward = mode.is_forward();
        let next_index = if forward {
            self.current_index + 1
        } else {
            self.current_index - 1
        };

        // Within-BIN traversal.
        //
        // Fast path (O(1)): use the pinned `current_bin_arc` to read
        // `next_index` directly, avoiding a root-to-leaf B-tree traversal on
        // every cursor step.
        //
        // Slow path (O(log N)): only taken when `current_bin_arc` is not yet
        // set (e.g. first advance after `get_first()` in an older code path).
        // We save the discovered arc so subsequent steps use the fast path.
        use noxu_tree::tree::TreeNode;
        let entry: Option<(Vec<u8>, Vec<u8>, i32, u64)>;
        let new_bin_arc: Option<
            std::sync::Arc<noxu_tree::NodeRwLock<noxu_tree::tree::TreeNode>>,
        >;

        if let Some(bin_arc) = &self.current_bin_arc {
            // Fast path: pinned BIN — no tree traversal.
            {
                let g = bin_arc.read();
                if let TreeNode::Bottom(bin) = &*g {
                    if next_index >= 0 && next_index < bin.entries.len() as i32
                    {
                        let idx = next_index as usize;
                        entry = Some((
                            bin.get_full_key(idx).unwrap_or_default(),
                            bin.entries[idx].data.clone().unwrap_or_default(),
                            next_index,
                            bin.entries[idx].lsn.as_u64(),
                        ));
                    } else {
                        entry = None; // BIN exhausted — fall through to cross-BIN
                    }
                } else {
                    entry = None;
                }
            }
            new_bin_arc = None;
        } else {
            // Slow path: traverse from root, then pin the discovered BIN.
            let current_key_slice_opt =
                self.current_key.as_deref().map(|s| s.to_vec());
            let db = self.db_impl.read();
            if let Some(tree) = db.get_real_tree() {
                if tree.is_empty() {
                    entry = None;
                    new_bin_arc = None;
                } else if let (Some(current_key), Some(root)) =
                    (current_key_slice_opt.as_deref(), tree.get_root())
                {
                    if let Some(bin_arc) =
                        Self::find_bin_for_key(root, current_key)
                    {
                        // Clone so we can move the arc after the read guard is dropped.
                        let arc_to_save = bin_arc.clone();
                        {
                            let g = bin_arc.read();
                            if let TreeNode::Bottom(bin) = &*g {
                                if next_index >= 0
                                    && next_index < bin.entries.len() as i32
                                {
                                    let idx = next_index as usize;
                                    entry = Some((
                                        bin.get_full_key(idx)
                                            .unwrap_or_default(),
                                        bin.entries[idx]
                                            .data
                                            .clone()
                                            .unwrap_or_default(),
                                        next_index,
                                        bin.entries[idx].lsn.as_u64(),
                                    ));
                                    new_bin_arc = Some(arc_to_save);
                                } else {
                                    entry = None;
                                    new_bin_arc = None;
                                }
                            } else {
                                entry = None;
                                new_bin_arc = None;
                            }
                        }
                    } else {
                        entry = None;
                        new_bin_arc = None;
                    }
                } else {
                    entry = None;
                    new_bin_arc = None;
                }
            } else {
                entry = None;
                new_bin_arc = None;
            }
        }

        // Pin the BIN we discovered via the slow path.
        if new_bin_arc.is_some() {
            self.update_bin_pin(new_bin_arc);
        }

        if let Some((key, data, idx, lsn)) = entry {
            // For dup-mode traversal modes, filter by primary key.
            if is_dup {
                let s = self.apply_dup_filter(
                    key,
                    data,
                    idx,
                    lsn,
                    mode,
                    current_primary_key.as_deref(),
                    forward,
                )?;
                return Ok(s);
            }
            self.lock_ln(lsn)?;
            self.current_key = Some(key);
            self.current_data = Some(data);
            self.current_lsn = lsn;
            self.current_index = idx;
            return Ok(OperationStatus::Success);
        }

        // Current BIN exhausted — cross to adjacent BIN.
        let anchor_key: Vec<u8> = match &self.current_key {
            Some(k) => k.clone(),
            None => return Ok(OperationStatus::NotFound),
        };

        let adjacent_entries: Option<Vec<BinEntry>> = {
            let db = self.db_impl.read();
            if let Some(tree) = db.get_real_tree() {
                if forward {
                    tree.get_next_bin(&anchor_key)
                } else {
                    tree.get_prev_bin(&anchor_key)
                }
            } else {
                None
            }
        };

        match adjacent_entries {
            Some(entries) if !entries.is_empty() => {
                let (raw_key, raw_data, idx, lsn) = if forward {
                    let e = entries.into_iter().next().unwrap();
                    (e.key, e.data.unwrap_or_default(), 0i32, e.lsn.as_u64())
                } else {
                    let last_idx = (entries.len() - 1) as i32;
                    let e = entries.into_iter().last().unwrap();
                    (
                        e.key,
                        e.data.unwrap_or_default(),
                        last_idx,
                        e.lsn.as_u64(),
                    )
                };
                if is_dup {
                    let s = self.apply_dup_filter(
                        raw_key,
                        raw_data,
                        idx,
                        lsn,
                        mode,
                        current_primary_key.as_deref(),
                        forward,
                    )?;
                    return Ok(s);
                }
                self.lock_ln(lsn)?;
                // Crossed into a new BIN — update the cursor pin.
                let new_key_ref = raw_key.clone();
                let bin_arc = {
                    let db = self.db_impl.read();
                    db.get_real_tree().and_then(|tree| {
                        tree.get_root().and_then(|r| {
                            Self::find_bin_for_key(r, &new_key_ref)
                        })
                    })
                };
                self.current_key = Some(raw_key);
                self.current_data = Some(raw_data);
                self.current_lsn = lsn;
                self.current_index = idx;
                self.update_bin_pin(bin_arc);
                Ok(OperationStatus::Success)
            }
            _ => Ok(OperationStatus::NotFound),
        }
    }

    /// Applies sorted-dup filtering rules after moving to `(raw_key, raw_data,
    /// idx)`.
    ///
    /// * `NextDup` / `PrevDup` — succeed only if the new entry's primary key
    ///   equals the saved primary key; return NotFound otherwise.
    /// * `NextNoDup` / `PrevNoDup` — advance past all entries that share the
    ///   same primary key as the saved position, returning the first entry with
    ///   a DIFFERENT primary key.
    /// * `Next` / `Prev` — accept any entry.
    fn apply_dup_filter(
        &mut self,
        mut raw_key: Vec<u8>,
        mut raw_data: Vec<u8>,
        mut idx: i32,
        mut lsn: u64,
        mode: GetMode,
        prev_primary_key: Option<&[u8]>,
        forward: bool,
    ) -> Result<OperationStatus, DbiError> {
        loop {
            let new_pk = dup_key_data::get_key(&raw_key);
            match mode {
                GetMode::NextDup | GetMode::PrevDup => {
                    // Stay on the same primary key.
                    let same = match (&new_pk, prev_primary_key) {
                        (Some(npk), Some(ppk)) => npk.as_slice() == ppk,
                        _ => false,
                    };
                    if same {
                        self.lock_ln(lsn)?;
                        self.current_key = Some(raw_key);
                        self.current_data = Some(raw_data);
                        self.current_lsn = lsn;
                        self.current_index = idx;
                        return Ok(OperationStatus::Success);
                    } else {
                        return Ok(OperationStatus::NotFound);
                    }
                }
                GetMode::NextNoDup | GetMode::PrevNoDup => {
                    // Skip entries with the same primary key as `prev_primary_key`.
                    let same = match (&new_pk, prev_primary_key) {
                        (Some(npk), Some(ppk)) => npk.as_slice() == ppk,
                        _ => false,
                    };
                    if !same {
                        self.lock_ln(lsn)?;
                        self.current_key = Some(raw_key);
                        self.current_data = Some(raw_data);
                        self.current_lsn = lsn;
                        self.current_index = idx;
                        return Ok(OperationStatus::Success);
                    }
                    // Need to advance further.
                    // Increment/decrement idx and try to read from the tree.
                    if forward {
                        idx += 1;
                    } else {
                        idx -= 1;
                    }
                    let next = {
                        let db = self.db_impl.read();
                        if let Some(tree) = db.get_real_tree() {
                            if tree.is_empty() {
                                None
                            } else {
                                use noxu_tree::tree::TreeNode;
                                tree.get_root().and_then(|r| {
                                    // Use the current raw_key to find the BIN.
                                    let bin_arc =
                                        Self::find_bin_for_key(r, &raw_key)?;
                                    let g = bin_arc.read();
                                    match &*g {
                                        TreeNode::Bottom(bin) => {
                                            if idx < 0
                                                || idx
                                                    >= bin.entries.len() as i32
                                            {
                                                None
                                            } else {
                                                let i = idx as usize;
                                                Some((
                                                    bin.get_full_key(i)
                                                        .unwrap_or_default(),
                                                    bin.entries[i]
                                                        .data
                                                        .clone()
                                                        .unwrap_or_default(),
                                                    idx,
                                                    bin.entries[i].lsn.as_u64(),
                                                ))
                                            }
                                        }
                                        _ => None,
                                    }
                                })
                            }
                        } else {
                            None
                        }
                    };
                    match next {
                        Some((k, d, i, l)) => {
                            raw_key = k;
                            raw_data = d;
                            idx = i;
                            lsn = l;
                            // Loop continues.
                        }
                        None => {
                            // BIN exhausted — cross to adjacent BIN.
                            let anchor = raw_key.clone();
                            let adj: Option<Vec<BinEntry>> = {
                                let db = self.db_impl.read();
                                if let Some(tree) = db.get_real_tree() {
                                    if forward {
                                        tree.get_next_bin(&anchor)
                                    } else {
                                        tree.get_prev_bin(&anchor)
                                    }
                                } else {
                                    None
                                }
                            };
                            match adj {
                                Some(entries) if !entries.is_empty() => {
                                    let (k, d, i, l) = if forward {
                                        let e =
                                            entries.into_iter().next().unwrap();
                                        (
                                            e.key,
                                            e.data.unwrap_or_default(),
                                            0i32,
                                            e.lsn.as_u64(),
                                        )
                                    } else {
                                        let li = (entries.len() - 1) as i32;
                                        let e =
                                            entries.into_iter().last().unwrap();
                                        (
                                            e.key,
                                            e.data.unwrap_or_default(),
                                            li,
                                            e.lsn.as_u64(),
                                        )
                                    };
                                    raw_key = k;
                                    raw_data = d;
                                    idx = i;
                                    lsn = l;
                                    // Loop continues.
                                }
                                _ => return Ok(OperationStatus::NotFound),
                            }
                        }
                    }
                }
                // Next / Prev: accept any entry.
                GetMode::Next | GetMode::Prev => {
                    self.lock_ln(lsn)?;
                    self.current_key = Some(raw_key);
                    self.current_data = Some(raw_data);
                    self.current_lsn = lsn;
                    self.current_index = idx;
                    return Ok(OperationStatus::Success);
                }
            }
        }
    }

    /// Descends from `node` to the BIN whose key range contains `key`.
    ///
    /// This mirrors the search path in `Tree::search()` — at each upper IN
    /// we follow the child slot with the largest key <= `key`.  Returns the
    /// `Arc` of the matching BIN, or `None` if the tree is empty / malformed.
    fn find_bin_for_key(
        node: std::sync::Arc<noxu_tree::NodeRwLock<noxu_tree::tree::TreeNode>>,
        key: &[u8],
    ) -> Option<std::sync::Arc<noxu_tree::NodeRwLock<noxu_tree::tree::TreeNode>>>
    {
        use noxu_tree::tree::TreeNode;
        let mut current = node;
        loop {
            let (is_bin, child) = {
                let g = current.read();
                let is_bin = g.is_bin();
                let child = if !is_bin {
                    match &*g {
                        TreeNode::Internal(n) => {
                            if n.entries.is_empty() {
                                return None;
                            }
                            // Slot 0 carries a virtual key (-infinity); follow
                            // the largest key <= search key (same logic as
                            // Tree::search and Tree::insert_recursive).
                            let mut idx = 0usize;
                            for (i, entry) in n.entries.iter().enumerate() {
                                if i == 0 {
                                    idx = 0;
                                } else if entry.key.as_slice() <= key {
                                    idx = i;
                                } else {
                                    break;
                                }
                            }
                            n.entries.get(idx).and_then(|e| e.child.clone())
                        }
                        _ => None,
                    }
                } else {
                    None
                };
                (is_bin, child)
            };
            if is_bin {
                return Some(current);
            }
            current = child?;
        }
    }

    /// Inserts or updates a record at the cursor position.
    ///
    /// Write path:
    ///
    /// 1. Checks state and, for `Current` mode, that the cursor is initialized.
    /// 2. For `NoOverwrite`: searches the tree; returns `KeyExist` if found.
    /// 3. Calls `Tree::insert(key, data, lsn)` to insert/update in the BIN.
    /// 4. Updates the cursor position to the newly written record.
    ///
    /// Note: locking (step 2 in the) and WAL logging (step 3 in the) are not
    /// yet wired here — they require LogManager integration (P0 gap).
    ///
    /// # Arguments
    ///
    /// * `key` - The key to insert/update
    /// * `data` - The data value
    /// * `put_mode` - The insertion mode
    ///
    /// # Returns
    ///
    /// * `Success` if the record was inserted/updated
    /// * `KeyExist` if NoOverwrite mode and key already exists
    pub fn put(
        &mut self,
        key: &[u8],
        data: &[u8],
        put_mode: PutMode,
    ) -> Result<OperationStatus, DbiError> {
        self.check_state()?;

        // For sorted-dup databases: encode (key, data) as a two-part composite
        // key.  The tree stores `combine(key, data)` with no slot data.
        // Dup path in 7.5.
        if self.is_sorted_dup() {
            return self.put_dup(key, data, put_mode);
        }

        match put_mode {
            PutMode::Current => {
                self.check_initialized()?;
                let current_key = self
                    .current_key
                    .clone()
                    .ok_or(DbiError::CursorNotInitialized)?;
                let (old_data, old_lsn) =
                    self.get_slot_before_image(&current_key);
                self.lock_write_before_log(old_lsn)?;
                let new_lsn = self.log_ln_write(
                    &current_key,
                    Some(data),
                    self.locker_id,
                )?;
                self.finalize_write_lock(
                    old_lsn,
                    new_lsn,
                    Some(current_key.clone()),
                    old_data,
                )?;
                self.apply_tree_insert(current_key, data.to_vec(), new_lsn);
                self.current_data = Some(data.to_vec());
                self.current_lsn = new_lsn.as_u64();
                Ok(OperationStatus::Success)
            }
            PutMode::NoOverwrite => {
                if self.key_exists_in_view(key) {
                    return Ok(OperationStatus::KeyExist);
                }
                // New insert: old_lsn is NULL (abort_known_deleted=true).
                let (old_data, old_lsn) = self.get_slot_before_image(key);
                self.lock_write_before_log(old_lsn)?;
                let new_lsn =
                    self.log_ln_write(key, Some(data), self.locker_id)?;
                self.finalize_write_lock(
                    old_lsn,
                    new_lsn,
                    Some(key.to_vec()),
                    old_data,
                )?;
                self.apply_tree_insert(key.to_vec(), data.to_vec(), new_lsn);
                self.current_key = Some(key.to_vec());
                self.current_data = Some(data.to_vec());
                self.current_lsn = new_lsn.as_u64();
                self.current_index = 0;
                self.state = CursorState::Initialized;
                Ok(OperationStatus::Success)
            }
            // NoDupData on a non-dup database behaves like NoOverwrite:
            // returns KeyExist if the key already exists, otherwise inserts.
            // `Cursor.putNoDupData()` non-dup branch.
            PutMode::NoDupData => {
                if self.key_exists_in_view(key) {
                    return Ok(OperationStatus::KeyExist);
                }
                let (old_data, old_lsn) = self.get_slot_before_image(key);
                self.lock_write_before_log(old_lsn)?;
                let new_lsn =
                    self.log_ln_write(key, Some(data), self.locker_id)?;
                self.finalize_write_lock(
                    old_lsn,
                    new_lsn,
                    Some(key.to_vec()),
                    old_data,
                )?;
                self.apply_tree_insert(key.to_vec(), data.to_vec(), new_lsn);
                self.current_key = Some(key.to_vec());
                self.current_data = Some(data.to_vec());
                self.current_lsn = new_lsn.as_u64();
                self.current_index = 0;
                self.state = CursorState::Initialized;
                Ok(OperationStatus::Success)
            }
            PutMode::Overwrite => {
                let (old_data, old_lsn) = self.get_slot_before_image(key);
                self.lock_write_before_log(old_lsn)?;
                let new_lsn =
                    self.log_ln_write(key, Some(data), self.locker_id)?;
                self.finalize_write_lock(
                    old_lsn,
                    new_lsn,
                    Some(key.to_vec()),
                    old_data,
                )?;
                self.apply_tree_insert(key.to_vec(), data.to_vec(), new_lsn);
                self.current_key = Some(key.to_vec());
                self.current_data = Some(data.to_vec());
                self.current_lsn = new_lsn.as_u64();
                self.current_index = 0;
                self.state = CursorState::Initialized;
                Ok(OperationStatus::Success)
            }
        }
    }

    /// Sorted-dup variant of `put()`.
    ///
    /// Encodes (key, data) as a two-part composite key and stores it in the
    /// tree with empty slot data.  The tree's custom comparator ensures
    /// correct ordering.
    ///
    /// Dup path from 7.5.
    fn put_dup(
        &mut self,
        key: &[u8],
        data: &[u8],
        put_mode: PutMode,
    ) -> Result<OperationStatus, DbiError> {
        let two_part_key = dup_key_data::combine(key, data);

        match put_mode {
            PutMode::NoDupData | PutMode::NoOverwrite => {
                // Return KeyExist if the exact (key, data) pair already exists.
                let exists = {
                    let db = self.db_impl.read();
                    if let Some(tree) = db.get_real_tree() {
                        tree.search(&two_part_key)
                            .map(|sr| sr.exact_parent_found)
                            .unwrap_or(false)
                    } else {
                        false
                    }
                };
                if exists {
                    return Ok(OperationStatus::KeyExist);
                }
                let new_lsn = self.log_ln_write(
                    &two_part_key,
                    Some(b""),
                    self.locker_id,
                )?;
                {
                    let db = self.db_impl.read();
                    if let Some(tree) = db.get_real_tree() {
                        let _ =
                            tree.insert(two_part_key.clone(), vec![], new_lsn);
                    }
                }
                self.current_key = Some(two_part_key);
                self.current_data = None;
                self.current_lsn = new_lsn.as_u64();
                self.current_index = 0;
                self.state = CursorState::Initialized;
                Ok(OperationStatus::Success)
            }
            PutMode::Current => {
                // Replace the data of the currently positioned record.
                // In dup mode this means replacing the current two-part key
                // with a new one (delete old, insert new).
                self.check_initialized()?;
                let old_key = self
                    .current_key
                    .clone()
                    .ok_or(DbiError::CursorNotInitialized)?;
                // Delete the old two-part key.
                self.log_ln_write(&old_key, None, self.locker_id)?;
                {
                    let db = self.db_impl.read();
                    if let Some(tree) = db.get_real_tree() {
                        tree.delete(&old_key);
                    }
                }
                // Insert the new two-part key.
                let new_lsn = self.log_ln_write(
                    &two_part_key,
                    Some(b""),
                    self.locker_id,
                )?;
                {
                    let db = self.db_impl.read();
                    if let Some(tree) = db.get_real_tree() {
                        let _ =
                            tree.insert(two_part_key.clone(), vec![], new_lsn);
                    }
                }
                self.current_key = Some(two_part_key);
                self.current_data = None;
                self.current_lsn = new_lsn.as_u64();
                Ok(OperationStatus::Success)
            }
            PutMode::Overwrite => {
                // Insert or replace the exact (key, data) pair.
                let new_lsn = self.log_ln_write(
                    &two_part_key,
                    Some(b""),
                    self.locker_id,
                )?;
                {
                    let db = self.db_impl.read();
                    if let Some(tree) = db.get_real_tree() {
                        let _ =
                            tree.insert(two_part_key.clone(), vec![], new_lsn);
                    }
                }
                self.current_key = Some(two_part_key);
                self.current_data = None;
                self.current_lsn = new_lsn.as_u64();
                self.current_index = 0;
                self.state = CursorState::Initialized;
                Ok(OperationStatus::Success)
            }
        }
    }

    /// Writes an LN (Leaf Node) log entry for a put or delete operation.
    ///
    /// Returns the LSN assigned to the entry, or NULL_LSN if no log manager
    /// is configured (e.g., read-only or test cursor).
    fn log_ln_write(
        &self,
        key: &[u8],
        data: Option<&[u8]>,
        txn_id: i64,
    ) -> Result<Lsn, DbiError> {
        // Deferred-write databases skip WAL logging entirely.
        // Data is flushed to disk only at eviction or checkpoint.
        // `CursorImpl.java` deferred-write check before logManager.log().
        if self.db_impl.read().is_deferred_write() {
            return Ok(noxu_util::NULL_LSN);
        }

        let lm = match &self.log_manager {
            Some(lm) => lm,
            None => return Ok(noxu_util::NULL_LSN),
        };

        let db_id = self.db_impl.read().get_id().id() as u64;
        let txn_id_opt = if txn_id != 0 { Some(txn_id) } else { None };

        let entry = LnLogEntry::new(
            db_id,
            txn_id_opt,
            Lsn::from_u64(self.current_lsn), // abort_lsn: before-image LSN (current slot LSN before this write)
            false,                           // abort_known_deleted
            None,                            // abort_key
            None,                            // abort_data
            NULL_VLSN,                       // abort_vlsn
            0,                               // abort_expiration
            true,                            // embedded_ln
            key.to_vec(),
            data.map(|d| d.to_vec()),
            0,         // expiration
            NULL_VLSN, // vlsn
        );

        let mut buf = BytesMut::with_capacity(entry.log_size());
        entry.write_to_log(&mut buf);

        let entry_type = if data.is_some() {
            LogEntryType::InsertLN
        } else {
            LogEntryType::DeleteLN
        };

        // Pass the previous slot LSN as old_lsn so the UtilizationTracker
        // marks the previous version obsolete (the: countObsoleteNode with oldLsn).
        let old_lsn_opt = if self.current_lsn != noxu_util::NULL_LSN.as_u64() {
            Some(Lsn::from_u64(self.current_lsn))
        } else {
            None
        };

        lm.log_with_old_lsn(
            entry_type,
            &buf,
            Provisional::No,
            false,
            false,
            old_lsn_opt,
        )
        .map_err(DbiError::from)
    }

    /// Deletes the record at the cursor position.
    ///
    /// Delete path:
    ///
    /// 1. Checks that the cursor is initialized.
    /// 2. Writes a DeleteLN log entry to the WAL (if log manager is present).
    /// 3. Calls `Tree::delete(key)` to remove the entry from the BIN.
    /// 4. Resets cursor to NotInitialized (matching behaviour).
    ///
    /// # Returns
    ///
    /// * `Success` if the record was deleted
    ///
    /// # Errors
    ///
    /// * `CursorNotInitialized` if cursor is not positioned
    /// * `CursorClosed` if cursor has been closed
    pub fn delete(&mut self) -> Result<OperationStatus, DbiError> {
        self.check_initialized()?;

        // For sorted-dup databases, current_key IS the two-part composite key
        // stored in the tree.  For non-dup databases it is the plain key.
        // In both cases current_key is the correct tree-delete key.
        if let Some(tree_key) = self.current_key.clone() {
            let (old_data, old_lsn) = self.get_slot_before_image(&tree_key);
            self.lock_write_before_log(old_lsn)?;
            let del_lsn = self.log_ln_write(&tree_key, None, self.locker_id)?;
            self.finalize_write_lock(
                old_lsn,
                del_lsn,
                Some(tree_key.clone()),
                old_data,
            )?;
            self.apply_tree_delete(tree_key, del_lsn);
        }

        self.current_key = None;
        self.current_data = None;
        self.current_lsn = noxu_util::NULL_LSN.as_u64();
        self.current_index = -1;
        self.state = CursorState::NotInitialized;

        Ok(OperationStatus::Success)
    }

    /// Counts the number of duplicates at the current key position.
    ///
    /// For sorted-dup databases, traverses all records sharing the same
    /// primary key. For non-dup databases, returns 1 if positioned.
    ///
    /// 7.5.
    ///
    /// # Returns
    ///
    /// The count of duplicate records at the current key.
    ///
    /// # Errors
    ///
    /// * `CursorNotInitialized` if cursor is not positioned
    /// * `CursorClosed` if cursor has been closed
    pub fn count(&self) -> Result<i64, DbiError> {
        self.check_initialized()?;

        // For sorted-dup databases, count all entries sharing the same primary
        // key as the current position.
        //
        // Strategy: clone the cursor at the current position (already on some
        // dup for the primary key), then:
        //   1. Walk backward with PrevDup until NotFound — each success is one
        //      dup before the original position.
        //   2. Walk forward with NextDup until NotFound — each success is one
        //      dup after.
        //   3. Total = backward + 1 + forward.
        //
        // This reuses the existing retrieve_next(PrevDup/NextDup) logic which
        // correctly handles BIN boundaries and the two-part key filter.
        if self.is_sorted_dup() {
            let mut scratch = self.dup(true)?;
            let mut backward: i64 = 0;
            while let Ok(OperationStatus::Success) =
                scratch.retrieve_next(GetMode::PrevDup)
            {
                backward += 1;
            }
            // scratch is now at the first dup for this primary key.
            let mut forward: i64 = 0;
            while let Ok(OperationStatus::Success) =
                scratch.retrieve_next(GetMode::NextDup)
            {
                forward += 1;
            }
            return Ok(backward + 1 + forward);
        }

        Ok(1)
    }

    /// Creates a duplicate of this cursor at the same position.
    ///
    /// If `same_position` is true, the new cursor is positioned at the
    /// same record as this cursor. Otherwise, the new cursor is created
    /// in the NotInitialized state.
    ///
    /// The duplicated cursor shares the same locker (transaction) as
    /// the original cursor.
    ///
    /// # Arguments
    ///
    /// * `same_position` - Whether to copy the current position
    ///
    /// # Returns
    ///
    /// A new CursorImpl with the same or uninitialized position.
    ///
    /// # Errors
    ///
    /// * `CursorClosed` if the cursor has been closed
    pub fn dup(&self, same_position: bool) -> Result<CursorImpl, DbiError> {
        self.check_state()?;

        let mut new_cursor = match &self.log_manager {
            Some(lm) => CursorImpl::with_log_manager(
                self.db_impl.clone(),
                self.locker_id,
                lm.clone(),
            ),
            None => CursorImpl::new(self.db_impl.clone(), self.locker_id),
        };
        if let Some(lm) = &self.lock_manager {
            new_cursor.lock_manager = Some(lm.clone());
        }

        if same_position && self.state == CursorState::Initialized {
            new_cursor.current_key = self.current_key.clone();
            new_cursor.current_data = self.current_data.clone();
            new_cursor.current_lsn = self.current_lsn;
            new_cursor.current_index = self.current_index;
            new_cursor.state = CursorState::Initialized;
        }

        Ok(new_cursor)
    }

    /// Closes the cursor.
    ///
    /// Releases all resources held by the cursor, including any BIN latches
    /// and cursor-level locks. After closing, all operations on the cursor
    /// will return `CursorClosed` errors.
    ///
    /// Closing a cursor multiple times is safe and has no effect after the
    /// Updates the cursor's BIN pin when moving to a new BIN.
    ///
    /// Decrements  on the old BIN (if any) and increments it
    /// on  (if ).  No-op when the cursor stays on the same BIN
    /// (pointer equality checked via ).
    ///
    /// Matching  /
    ///  calls in cursor positioning.
    fn update_bin_pin(
        &mut self,
        new_bin: Option<
            std::sync::Arc<noxu_tree::NodeRwLock<noxu_tree::tree::TreeNode>>,
        >,
    ) {
        // Same BIN — nothing to do.
        match (&self.current_bin_arc, &new_bin) {
            (Some(old), Some(new)) if std::sync::Arc::ptr_eq(old, new) => {
                return;
            }
            _ => {}
        }
        // Unpin old BIN.
        if let Some(old_arc) = self.current_bin_arc.take() {
            noxu_tree::Tree::unpin_bin(&old_arc);
        }
        // Pin new BIN.
        if let Some(ref new_arc) = new_bin {
            noxu_tree::Tree::pin_bin(new_arc);
        }
        self.current_bin_arc = new_bin;
    }

    /// first close.
    ///
    /// # Returns
    ///
    ///  always (never fails).
    pub fn close(&mut self) -> Result<(), DbiError> {
        if self.state == CursorState::Closed {
            return Ok(());
        }

        // Release BIN pin — prevents evictor from seeing a stale cursor_count.
        self.update_bin_pin(None);

        self.current_key = None;
        self.current_data = None;
        self.current_lsn = noxu_util::NULL_LSN.as_u64();
        self.current_index = -1;
        self.state = CursorState::Closed;

        Ok(())
    }
}

impl Drop for CursorImpl {
    /// Ensures the cursor is closed when dropped.
    ///
    /// This provides automatic cleanup if the user forgets to explicitly
    /// close the cursor. Note that it's still better practice to call
    /// close() explicitly to handle potential errors.
    fn drop(&mut self) {
        if self.state != CursorState::Closed {
            let _ = self.close();
        }
    }
}

#[cfg(test)]
#[expect(clippy::field_reassign_with_default)]
mod tests {
    use super::*;
    use crate::{DatabaseConfig, DatabaseId, DbType};

    /// Creates a test DatabaseImpl for cursor testing.
    fn create_test_database() -> Arc<RwLock<DatabaseImpl>> {
        let db_id = DatabaseId::new(1);
        let config = DatabaseConfig::default();
        let db_impl = DatabaseImpl::new(
            db_id,
            "test_db".to_string(),
            DbType::User,
            &config,
        );
        Arc::new(RwLock::new(db_impl))
    }

    #[test]
    fn test_new_cursor_not_initialized() {
        let db = create_test_database();
        let cursor = CursorImpl::new(db, 100);

        assert!(!cursor.is_initialized());
        assert!(!cursor.is_closed());
        assert_eq!(cursor.get_locker_id(), 100);
        assert!(cursor.get_current_key().is_none());
        assert!(cursor.get_current_data().is_none());
    }

    #[test]
    fn test_search_positions_cursor() {
        let db = create_test_database();
        let mut cursor = CursorImpl::new(db, 100);

        let key = b"test_key";
        let data = b"test_data";

        // Insert into tree first, then search.
        cursor.put(key, data, PutMode::Overwrite).unwrap();
        let status = cursor.search(key, Some(data), SearchMode::Set).unwrap();
        assert_eq!(status, OperationStatus::Success);
        assert!(cursor.is_initialized());
        assert_eq!(cursor.get_current_key(), Some(key.as_slice()));
        assert_eq!(cursor.get_current_data(), Some(data.as_slice()));
    }

    #[test]
    fn test_get_current_after_search() {
        let db = create_test_database();
        let mut cursor = CursorImpl::new(db, 100);

        let key = b"my_key";
        let data = b"my_data";

        // Insert into tree first, then search.
        cursor.put(key, data, PutMode::Overwrite).unwrap();
        cursor.search(key, Some(data), SearchMode::Set).unwrap();
        let (ret_key, ret_data) = cursor.get_current().unwrap();

        assert_eq!(ret_key, key);
        assert_eq!(ret_data, data);
    }

    #[test]
    fn test_get_current_before_initialization() {
        let db = create_test_database();
        let cursor = CursorImpl::new(db, 100);

        let result = cursor.get_current();
        assert!(matches!(result, Err(DbiError::CursorNotInitialized)));
    }

    #[test]
    fn test_retrieve_next_from_uninitialized() {
        let db = create_test_database();
        let mut cursor = CursorImpl::new(db, 100);

        let status = cursor.retrieve_next(GetMode::Next).unwrap();
        assert_eq!(status, OperationStatus::NotFound);
    }

    #[test]
    fn test_put_overwrite() {
        let db = create_test_database();
        let mut cursor = CursorImpl::new(db, 100);

        let key = b"key1";
        let data = b"data1";

        let status = cursor.put(key, data, PutMode::Overwrite).unwrap();
        assert_eq!(status, OperationStatus::Success);
        assert!(cursor.is_initialized());
        assert_eq!(cursor.get_current_key(), Some(key.as_slice()));
    }

    #[test]
    fn test_put_no_overwrite_when_key_exists() {
        let db = create_test_database();
        let mut cursor = CursorImpl::new(db, 100);

        let key = b"key1";
        let data1 = b"data1";
        let data2 = b"data2";

        // First put succeeds
        cursor.put(key, data1, PutMode::Overwrite).unwrap();

        // Second put with NoOverwrite should return KeyExist
        let status = cursor.put(key, data2, PutMode::NoOverwrite).unwrap();
        assert_eq!(status, OperationStatus::KeyExist);
    }

    #[test]
    fn test_put_current_requires_initialization() {
        let db = create_test_database();
        let mut cursor = CursorImpl::new(db, 100);

        let key = b"key1";
        let data = b"data1";

        let result = cursor.put(key, data, PutMode::Current);
        assert!(matches!(result, Err(DbiError::CursorNotInitialized)));
    }

    #[test]
    fn test_put_current_after_initialization() {
        let db = create_test_database();
        let mut cursor = CursorImpl::new(db, 100);

        let key = b"key1";
        let data1 = b"data1";
        let data2 = b"data2";

        // Insert first, then search to position cursor, then update with Current mode.
        cursor.put(key, data1, PutMode::Overwrite).unwrap();
        cursor.search(key, Some(data1), SearchMode::Set).unwrap();

        // Update with Current mode
        let status = cursor.put(key, data2, PutMode::Current).unwrap();
        assert_eq!(status, OperationStatus::Success);
        assert_eq!(cursor.get_current_data(), Some(data2.as_slice()));
    }

    #[test]
    fn test_delete_requires_initialization() {
        let db = create_test_database();
        let mut cursor = CursorImpl::new(db, 100);

        let result = cursor.delete();
        assert!(matches!(result, Err(DbiError::CursorNotInitialized)));
    }

    #[test]
    fn test_delete_resets_state() {
        let db = create_test_database();
        let mut cursor = CursorImpl::new(db, 100);

        let key = b"key1";
        let data = b"data1";

        // Insert, search to position, then delete.
        cursor.put(key, data, PutMode::Overwrite).unwrap();
        cursor.search(key, Some(data), SearchMode::Set).unwrap();
        assert!(cursor.is_initialized());

        // Delete
        let status = cursor.delete().unwrap();
        assert_eq!(status, OperationStatus::Success);
        assert!(!cursor.is_initialized());
        assert!(cursor.get_current_key().is_none());
    }

    #[test]
    fn test_dup_with_same_position() {
        let db = create_test_database();
        let mut cursor = CursorImpl::new(db, 100);

        let key = b"key1";
        let data = b"data1";

        // Insert, search to position, then dup.
        cursor.put(key, data, PutMode::Overwrite).unwrap();
        cursor.search(key, Some(data), SearchMode::Set).unwrap();

        // Duplicate with same position
        let dup_cursor = cursor.dup(true).unwrap();
        assert!(dup_cursor.is_initialized());
        assert_eq!(dup_cursor.get_current_key(), Some(key.as_slice()));
        assert_eq!(dup_cursor.get_current_data(), Some(data.as_slice()));
        assert_eq!(dup_cursor.get_locker_id(), 100);

        // Should have different IDs
        assert_ne!(cursor.get_id(), dup_cursor.get_id());
    }

    #[test]
    fn test_dup_without_same_position() {
        let db = create_test_database();
        let mut cursor = CursorImpl::new(db, 100);

        let key = b"key1";
        let data = b"data1";

        // Insert, search to position, then dup without position.
        cursor.put(key, data, PutMode::Overwrite).unwrap();
        cursor.search(key, Some(data), SearchMode::Set).unwrap();

        // Duplicate without position
        let dup_cursor = cursor.dup(false).unwrap();
        assert!(!dup_cursor.is_initialized());
        assert!(dup_cursor.get_current_key().is_none());
        assert_eq!(dup_cursor.get_locker_id(), 100);
    }

    #[test]
    fn test_close_sets_state() {
        let db = create_test_database();
        let mut cursor = CursorImpl::new(db, 100);

        cursor.close().unwrap();
        assert!(cursor.is_closed());
    }

    #[test]
    fn test_operations_after_close() {
        let db = create_test_database();
        let mut cursor = CursorImpl::new(db, 100);

        cursor.close().unwrap();

        // All operations should return CursorClosed
        assert!(matches!(
            cursor.search(b"key", None, SearchMode::Set),
            Err(DbiError::CursorClosed)
        ));
        assert!(matches!(cursor.get_current(), Err(DbiError::CursorClosed)));
        assert!(matches!(
            cursor.retrieve_next(GetMode::Next),
            Err(DbiError::CursorClosed)
        ));
        assert!(matches!(
            cursor.put(b"key", b"data", PutMode::Overwrite),
            Err(DbiError::CursorClosed)
        ));
        assert!(matches!(cursor.delete(), Err(DbiError::CursorClosed)));
        assert!(matches!(cursor.count(), Err(DbiError::CursorClosed)));
        assert!(matches!(cursor.dup(true), Err(DbiError::CursorClosed)));
    }

    #[test]
    fn test_close_idempotent() {
        let db = create_test_database();
        let mut cursor = CursorImpl::new(db, 100);

        cursor.close().unwrap();
        cursor.close().unwrap(); // Should not panic
        assert!(cursor.is_closed());
    }

    #[test]
    fn test_drop_calls_close() {
        let db = create_test_database();
        let mut cursor = CursorImpl::new(db.clone(), 100);

        let key = b"key1";
        let data = b"data1";
        cursor.put(key, data, PutMode::Overwrite).unwrap();
        cursor.search(key, Some(data), SearchMode::Set).unwrap();

        // Drop without explicit close
        drop(cursor);

        // Create another cursor to verify no issues
        let cursor2 = CursorImpl::new(db, 200);
        assert!(!cursor2.is_closed());
    }

    #[test]
    fn test_count_returns_one() {
        let db = create_test_database();
        let mut cursor = CursorImpl::new(db, 100);

        let key = b"key1";
        let data = b"data1";
        cursor.put(key, data, PutMode::Overwrite).unwrap();
        cursor.search(key, Some(data), SearchMode::Set).unwrap();

        let count = cursor.count().unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn test_unique_cursor_ids() {
        let db = create_test_database();
        let cursor1 = CursorImpl::new(db.clone(), 100);
        let cursor2 = CursorImpl::new(db.clone(), 100);
        let cursor3 = CursorImpl::new(db, 100);

        assert_ne!(cursor1.get_id(), cursor2.get_id());
        assert_ne!(cursor2.get_id(), cursor3.get_id());
        assert_ne!(cursor1.get_id(), cursor3.get_id());
    }

    // -----------------------------------------------------------------------
    // New unit tests for real B-tree traversal (get_first, get_last,
    // retrieve_next).
    // -----------------------------------------------------------------------

    /// get_first on an empty database returns NotFound.
    ///
    /// positionFirstOrLast on an empty tree.
    #[test]
    fn test_get_first_empty_tree() {
        let db = create_test_database();
        let mut cursor = CursorImpl::new(db, 100);
        let status = cursor.get_first().unwrap();
        assert_eq!(status, OperationStatus::NotFound);
    }

    /// get_last on an empty database returns NotFound.
    #[test]
    fn test_get_last_empty_tree() {
        let db = create_test_database();
        let mut cursor = CursorImpl::new(db, 100);
        let status = cursor.get_last().unwrap();
        assert_eq!(status, OperationStatus::NotFound);
    }

    /// get_first positions at smallest key after multiple puts.
    #[test]
    fn test_get_first_after_multiple_puts() {
        let db = create_test_database();
        let mut cursor = CursorImpl::new(db, 100);

        cursor.put(b"mango", b"m", PutMode::Overwrite).unwrap();
        cursor.put(b"apple", b"a", PutMode::Overwrite).unwrap();
        cursor.put(b"kiwi", b"k", PutMode::Overwrite).unwrap();

        let s = cursor.get_first().unwrap();
        assert_eq!(s, OperationStatus::Success);
        assert_eq!(cursor.get_current_key(), Some(b"apple".as_slice()));
        assert_eq!(cursor.get_current_data(), Some(b"a".as_slice()));
    }

    /// get_last positions at largest key after multiple puts.
    #[test]
    fn test_get_last_after_multiple_puts() {
        let db = create_test_database();
        let mut cursor = CursorImpl::new(db, 100);

        cursor.put(b"apple", b"a", PutMode::Overwrite).unwrap();
        cursor.put(b"mango", b"m", PutMode::Overwrite).unwrap();
        cursor.put(b"kiwi", b"k", PutMode::Overwrite).unwrap();

        let s = cursor.get_last().unwrap();
        assert_eq!(s, OperationStatus::Success);
        assert_eq!(cursor.get_current_key(), Some(b"mango".as_slice()));
        assert_eq!(cursor.get_current_data(), Some(b"m".as_slice()));
    }

    /// retrieve_next(Next) advances forward through the BIN.
    ///
    #[test]
    fn test_retrieve_next_forward() {
        let db = create_test_database();
        let mut cursor = CursorImpl::new(db, 100);

        cursor.put(b"a", b"1", PutMode::Overwrite).unwrap();
        cursor.put(b"b", b"2", PutMode::Overwrite).unwrap();
        cursor.put(b"c", b"3", PutMode::Overwrite).unwrap();

        cursor.get_first().unwrap();
        assert_eq!(cursor.get_current_key(), Some(b"a".as_slice()));

        let s = cursor.retrieve_next(GetMode::Next).unwrap();
        assert_eq!(s, OperationStatus::Success);
        assert_eq!(cursor.get_current_key(), Some(b"b".as_slice()));

        let s = cursor.retrieve_next(GetMode::Next).unwrap();
        assert_eq!(s, OperationStatus::Success);
        assert_eq!(cursor.get_current_key(), Some(b"c".as_slice()));

        let s = cursor.retrieve_next(GetMode::Next).unwrap();
        assert_eq!(s, OperationStatus::NotFound, "should be exhausted");
    }

    /// retrieve_next(Prev) traverses backward through the BIN.
    ///
    #[test]
    fn test_retrieve_next_backward() {
        let db = create_test_database();
        let mut cursor = CursorImpl::new(db, 100);

        cursor.put(b"a", b"1", PutMode::Overwrite).unwrap();
        cursor.put(b"b", b"2", PutMode::Overwrite).unwrap();
        cursor.put(b"c", b"3", PutMode::Overwrite).unwrap();

        cursor.get_last().unwrap();
        assert_eq!(cursor.get_current_key(), Some(b"c".as_slice()));

        let s = cursor.retrieve_next(GetMode::Prev).unwrap();
        assert_eq!(s, OperationStatus::Success);
        assert_eq!(cursor.get_current_key(), Some(b"b".as_slice()));

        let s = cursor.retrieve_next(GetMode::Prev).unwrap();
        assert_eq!(s, OperationStatus::Success);
        assert_eq!(cursor.get_current_key(), Some(b"a".as_slice()));

        let s = cursor.retrieve_next(GetMode::Prev).unwrap();
        assert_eq!(s, OperationStatus::NotFound, "should be exhausted");
    }

    /// A single key: get_first succeeds; retrieve_next(Next) returns NotFound.
    #[test]
    fn test_single_entry_traversal() {
        let db = create_test_database();
        let mut cursor = CursorImpl::new(db, 100);

        cursor.put(b"only", b"val", PutMode::Overwrite).unwrap();

        let s = cursor.get_first().unwrap();
        assert_eq!(s, OperationStatus::Success);
        assert_eq!(cursor.get_current_key(), Some(b"only".as_slice()));

        let s = cursor.retrieve_next(GetMode::Next).unwrap();
        assert_eq!(s, OperationStatus::NotFound);
    }

    /// retrieve_next from NotInitialized state returns NotFound (not an error).
    ///
    /// The: getNext asserts mustBeInitialized; we convert this to
    /// NotFound per Rust convention.
    #[test]
    fn test_retrieve_next_from_not_initialized_returns_not_found() {
        let db = create_test_database();
        let mut cursor = CursorImpl::new(db, 100);

        let s = cursor.retrieve_next(GetMode::Next).unwrap();
        assert_eq!(s, OperationStatus::NotFound);
    }

    /// put + NoOverwrite returns KeyExist when key is already in the tree.
    #[test]
    fn test_put_no_overwrite_tree_check() {
        let db = create_test_database();
        let mut cursor = CursorImpl::new(db, 100);

        cursor.put(b"key", b"v1", PutMode::Overwrite).unwrap();
        let s = cursor.put(b"key", b"v2", PutMode::NoOverwrite).unwrap();
        assert_eq!(s, OperationStatus::KeyExist);

        // Verify original value is still there.
        cursor.search(b"key", None, SearchMode::Set).unwrap();
        let (_, data) = cursor.get_current().unwrap();
        assert_eq!(data, b"v1");
    }

    /// After delete the tree no longer contains the key (search returns NotFound).
    #[test]
    fn test_delete_removes_from_tree() {
        let db = create_test_database();
        let mut cursor = CursorImpl::new(db, 100);

        cursor.put(b"key", b"val", PutMode::Overwrite).unwrap();
        cursor.search(b"key", None, SearchMode::Set).unwrap();
        cursor.delete().unwrap();

        let s = cursor.search(b"key", None, SearchMode::Set).unwrap();
        assert_eq!(s, OperationStatus::NotFound);
    }

    /// Range search: positions at the first key >= search key.
    #[test]
    fn test_search_set_range_finds_ge_key() {
        let db = create_test_database();
        let mut cursor = CursorImpl::new(db, 100);

        cursor.put(b"aaa", b"1", PutMode::Overwrite).unwrap();
        cursor.put(b"bbb", b"2", PutMode::Overwrite).unwrap();
        cursor.put(b"ccc", b"3", PutMode::Overwrite).unwrap();

        // Search for "bb" (not present) — should land on "bbb".
        let s = cursor.search(b"bb", None, SearchMode::SetRange).unwrap();
        assert_eq!(s, OperationStatus::Success);
        assert_eq!(cursor.get_current_key(), Some(b"bbb".as_slice()));
    }

    /// Range search beyond all keys returns NotFound.
    #[test]
    fn test_search_set_range_beyond_all_keys() {
        let db = create_test_database();
        let mut cursor = CursorImpl::new(db, 100);

        cursor.put(b"aaa", b"1", PutMode::Overwrite).unwrap();
        cursor.put(b"bbb", b"2", PutMode::Overwrite).unwrap();

        let s = cursor.search(b"zzz", None, SearchMode::SetRange).unwrap();
        assert_eq!(s, OperationStatus::NotFound);
    }

    // -----------------------------------------------------------------------
    // Sorted-duplicate key tests
    // -----------------------------------------------------------------------

    fn create_dup_database() -> Arc<RwLock<DatabaseImpl>> {
        let db_id = DatabaseId::new(2);
        let mut config = DatabaseConfig::default();
        config.sorted_duplicates = true;
        let db_impl = DatabaseImpl::new(
            db_id,
            "dup_test_db".to_string(),
            DbType::User,
            &config,
        );
        Arc::new(RwLock::new(db_impl))
    }

    /// Basic put + get_current round-trip for sorted-dup database.
    ///
    /// `DupKeyDataTest.testCombineSplit()`.
    #[test]
    fn test_dup_put_and_get_current() {
        let db = create_dup_database();
        let mut cursor = CursorImpl::new(db, 1);

        let s = cursor.put(b"key", b"data", PutMode::Overwrite).unwrap();
        assert_eq!(s, OperationStatus::Success);

        let (pk, d) = cursor.get_current().unwrap();
        assert_eq!(pk, b"key");
        assert_eq!(d, b"data");
    }

    /// Multiple data values for the same primary key.
    ///
    /// `SortedDuplicatesTest.testMultipleDups()`.
    #[test]
    fn test_dup_multiple_data_per_key() {
        let db = create_dup_database();
        let mut cursor = CursorImpl::new(db, 1);

        cursor.put(b"key", b"aaa", PutMode::Overwrite).unwrap();
        cursor.put(b"key", b"bbb", PutMode::Overwrite).unwrap();
        cursor.put(b"key", b"ccc", PutMode::Overwrite).unwrap();

        // search Set: positions at the first entry for "key"
        let s = cursor.search(b"key", None, SearchMode::Set).unwrap();
        assert_eq!(s, OperationStatus::Success);

        let (pk, d) = cursor.get_current().unwrap();
        assert_eq!(pk, b"key");
        assert_eq!(d, b"aaa", "first dup should have smallest data");
    }

    /// search Both: positions at the exact (key, data) pair.
    ///
    /// `CursorImpl.searchBothExact()` dup path.
    #[test]
    fn test_dup_search_both_exact() {
        let db = create_dup_database();
        let mut cursor = CursorImpl::new(db, 1);

        cursor.put(b"key", b"aaa", PutMode::Overwrite).unwrap();
        cursor.put(b"key", b"bbb", PutMode::Overwrite).unwrap();
        cursor.put(b"key", b"ccc", PutMode::Overwrite).unwrap();

        let s = cursor.search(b"key", Some(b"bbb"), SearchMode::Both).unwrap();
        assert_eq!(s, OperationStatus::Success);
        let (pk, d) = cursor.get_current().unwrap();
        assert_eq!(pk, b"key");
        assert_eq!(d, b"bbb");
    }

    /// search Both: returns NotFound when exact pair doesn't exist.
    #[test]
    fn test_dup_search_both_not_found() {
        let db = create_dup_database();
        let mut cursor = CursorImpl::new(db, 1);

        cursor.put(b"key", b"aaa", PutMode::Overwrite).unwrap();

        let s = cursor.search(b"key", Some(b"zzz"), SearchMode::Both).unwrap();
        assert_eq!(s, OperationStatus::NotFound);
    }

    /// NoDupData returns KeyExist when exact (key, data) already stored.
    ///
    /// `SortedDuplicatesTest.testNoDupData()`.
    #[test]
    fn test_dup_no_dup_data_returns_key_exist() {
        let db = create_dup_database();
        let mut cursor = CursorImpl::new(db, 1);

        cursor.put(b"key", b"val", PutMode::Overwrite).unwrap();

        let s = cursor.put(b"key", b"val", PutMode::NoDupData).unwrap();
        assert_eq!(s, OperationStatus::KeyExist);
    }

    /// NoDupData succeeds for a different data value under the same key.
    #[test]
    fn test_dup_no_dup_data_different_data_ok() {
        let db = create_dup_database();
        let mut cursor = CursorImpl::new(db, 1);

        cursor.put(b"key", b"val1", PutMode::Overwrite).unwrap();

        let s = cursor.put(b"key", b"val2", PutMode::NoDupData).unwrap();
        assert_eq!(s, OperationStatus::Success);
    }

    /// NextDup traversal visits all dups of the current primary key.
    ///
    /// `CursorImpl.getNext(GetMode.NEXT_DUP)` path.
    #[test]
    fn test_dup_next_dup_traversal() {
        let db = create_dup_database();
        let mut cursor = CursorImpl::new(db, 1);

        cursor.put(b"key", b"a", PutMode::Overwrite).unwrap();
        cursor.put(b"key", b"b", PutMode::Overwrite).unwrap();
        cursor.put(b"key", b"c", PutMode::Overwrite).unwrap();
        // Different primary key — should NOT appear in NextDup.
        cursor.put(b"zzz", b"x", PutMode::Overwrite).unwrap();

        // Position at first dup.
        cursor.search(b"key", None, SearchMode::Set).unwrap();
        let (_, d) = cursor.get_current().unwrap();
        assert_eq!(d, b"a");

        let s = cursor.retrieve_next(GetMode::NextDup).unwrap();
        assert_eq!(s, OperationStatus::Success);
        let (pk, d) = cursor.get_current().unwrap();
        assert_eq!(pk, b"key");
        assert_eq!(d, b"b");

        let s = cursor.retrieve_next(GetMode::NextDup).unwrap();
        assert_eq!(s, OperationStatus::Success);
        let (_, d) = cursor.get_current().unwrap();
        assert_eq!(d, b"c");

        // No more dups for "key".
        let s = cursor.retrieve_next(GetMode::NextDup).unwrap();
        assert_eq!(s, OperationStatus::NotFound);
    }

    /// NextNoDup skips all dups of the current primary key.
    ///
    /// `CursorImpl.getNext(GetMode.NEXT_NO_DUP)`.
    #[test]
    fn test_dup_next_no_dup_skips_dups() {
        let db = create_dup_database();
        let mut cursor = CursorImpl::new(db, 1);

        cursor.put(b"aaa", b"1", PutMode::Overwrite).unwrap();
        cursor.put(b"aaa", b"2", PutMode::Overwrite).unwrap();
        cursor.put(b"bbb", b"x", PutMode::Overwrite).unwrap();

        // Position at first entry for "aaa".
        cursor.search(b"aaa", None, SearchMode::Set).unwrap();
        let (pk, _) = cursor.get_current().unwrap();
        assert_eq!(pk, b"aaa");

        // NextNoDup should skip "aaa" dups and land on "bbb".
        let s = cursor.retrieve_next(GetMode::NextNoDup).unwrap();
        assert_eq!(s, OperationStatus::Success);
        let (pk, d) = cursor.get_current().unwrap();
        assert_eq!(pk, b"bbb");
        assert_eq!(d, b"x");
    }

    /// Dup delete removes only the specific (key, data) pair.
    ///
    /// `SortedDuplicatesTest.testDeleteDup()`.
    #[test]
    fn test_dup_delete_specific_pair() {
        let db = create_dup_database();
        let mut cursor = CursorImpl::new(db, 1);

        cursor.put(b"key", b"a", PutMode::Overwrite).unwrap();
        cursor.put(b"key", b"b", PutMode::Overwrite).unwrap();

        // Position at "key"/"b" and delete it.
        cursor.search(b"key", Some(b"b"), SearchMode::Both).unwrap();
        cursor.delete().unwrap();

        // "key"/"a" should still exist.
        let s = cursor.search(b"key", None, SearchMode::Set).unwrap();
        assert_eq!(s, OperationStatus::Success);
        let (pk, d) = cursor.get_current().unwrap();
        assert_eq!(pk, b"key");
        assert_eq!(d, b"a");

        // "key"/"b" should be gone.
        let s = cursor.search(b"key", Some(b"b"), SearchMode::Both).unwrap();
        assert_eq!(s, OperationStatus::NotFound);
    }

    /// Dup prefix-ambiguity ordering is correct.
    ///
    ///
    /// Key "a" data "bc" must sort before key "ab" data "c".
    #[test]
    fn test_dup_ordering_prefix_ambiguity() {
        let db = create_dup_database();
        let mut cursor = CursorImpl::new(db, 1);

        // "ab"/"c" inserted first to stress comparator.
        cursor.put(b"ab", b"c", PutMode::Overwrite).unwrap();
        cursor.put(b"a", b"bc", PutMode::Overwrite).unwrap();

        // Forward scan should give ("a","bc") then ("ab","c").
        cursor.get_first().unwrap();
        let (pk, d) = cursor.get_current().unwrap();
        assert_eq!(pk, b"a");
        assert_eq!(d, b"bc");

        cursor.retrieve_next(GetMode::Next).unwrap();
        let (pk, d) = cursor.get_current().unwrap();
        assert_eq!(pk, b"ab");
        assert_eq!(d, b"c");
    }

    // -----------------------------------------------------------------------
    // Cross-BIN cursor traversal test
    // -----------------------------------------------------------------------

    /// Full forward scan visits all 200 entries across multiple BINs in sorted
    /// order.
    ///
    /// We use a DatabaseImpl whose underlying Tree is created with a small
    /// `max_entries_per_node` (4) so that 200 inserts force many splits and
    /// fill multiple BINs.  The cursor must cross every BIN boundary without
    /// losing any entry.
    ///
    /// CursorImplTest multi-BIN scan: insert N records, open
    /// cursor at first, call getNext() until NotFound, assert count == N and
    /// keys are in ascending order.
    #[test]
    fn test_full_scan_crosses_multiple_bins() {
        // Build a database with a small node fanout (4) so 200 inserts force
        // many BIN splits.  DatabaseConfig::node_max_entries controls the
        // Tree::max_entries_per_node passed to Tree::new().
        let db_id = DatabaseId::new(42);
        let mut config = DatabaseConfig::default();
        config.set_node_max_entries(4); // tiny fanout → many BINs
        let db_impl = DatabaseImpl::new(
            db_id,
            "scan_test".to_string(),
            DbType::User,
            &config,
        );
        let db = Arc::new(RwLock::new(db_impl));

        const N: usize = 200;

        // Insert 200 entries with zero-padded decimal keys so lexicographic
        // order == numeric order.
        {
            let mut cursor = CursorImpl::new(db.clone(), 1);
            for i in 0..N {
                let key = format!("{:08}", i).into_bytes();
                let val = format!("v{}", i).into_bytes();
                let s = cursor.put(&key, &val, PutMode::Overwrite).unwrap();
                assert_eq!(s, OperationStatus::Success, "put {} failed", i);
            }
        }

        // Forward scan: get_first + repeated get_next.
        let mut cursor = CursorImpl::new(db.clone(), 2);
        let s = cursor.get_first().unwrap();
        assert_eq!(s, OperationStatus::Success, "get_first should succeed");

        let mut visited: Vec<Vec<u8>> = Vec::new();
        visited.push(cursor.get_current_key().unwrap().to_vec());

        loop {
            let s = cursor.retrieve_next(GetMode::Next).unwrap();
            match s {
                OperationStatus::Success => {
                    visited.push(cursor.get_current_key().unwrap().to_vec());
                }
                OperationStatus::NotFound => break,
                other => panic!("unexpected status {:?}", other),
            }
        }

        assert_eq!(
            visited.len(),
            N,
            "full scan must visit exactly {} entries, got {}",
            N,
            visited.len()
        );

        // Verify keys are in ascending (sorted) order.
        for i in 1..visited.len() {
            assert!(
                visited[i - 1] < visited[i],
                "keys out of order at position {}: {:?} >= {:?}",
                i,
                std::str::from_utf8(&visited[i - 1]).unwrap_or("?"),
                std::str::from_utf8(&visited[i]).unwrap_or("?"),
            );
        }

        // Backward scan: get_last + repeated get_prev.
        let mut cursor_back = CursorImpl::new(db, 3);
        let s = cursor_back.get_last().unwrap();
        assert_eq!(s, OperationStatus::Success, "get_last should succeed");

        let mut visited_back: Vec<Vec<u8>> = Vec::new();
        visited_back.push(cursor_back.get_current_key().unwrap().to_vec());

        loop {
            let s = cursor_back.retrieve_next(GetMode::Prev).unwrap();
            match s {
                OperationStatus::Success => {
                    visited_back
                        .push(cursor_back.get_current_key().unwrap().to_vec());
                }
                OperationStatus::NotFound => break,
                other => panic!("unexpected backward status {:?}", other),
            }
        }

        assert_eq!(
            visited_back.len(),
            N,
            "backward scan must visit exactly {} entries, got {}",
            N,
            visited_back.len()
        );

        // Backward scan should be the reverse of forward scan.
        let mut visited_back_rev = visited_back.clone();
        visited_back_rev.reverse();
        assert_eq!(
            visited_back_rev, visited,
            "backward scan reversed must equal forward scan"
        );
    }
}
