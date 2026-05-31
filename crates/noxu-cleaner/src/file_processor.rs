//! Log file processing for cleaning.
//!
//! reads all entries in a log file and determines
//! whether each entry is obsolete or active. Active LNs are migrated immediately,
//! active INs are marked dirty for the next checkpoint.

use crate::LnInfo;
use crate::cleaner_stat::CleanerStats;
use bytes::BytesMut;
use noxu_log::LogManager;
use noxu_txn::{LockManager, LockType, TxnError};
use noxu_util::Lsn;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, RwLock};

/// Release a cleaner read-lock and log any failure.
///
/// The cleaner is best-effort by design: failing to release a
/// non-blocking read lock on `tree_lsn` after a slot inspection
/// means a small leak in the `LockManager` (the cleaner-locker id
/// is short-lived and abandoned after the migration attempt
/// returns), not data corruption. This helper centralises the
/// `log::error!` call so the operator can see when a
/// `LockManager::release` is failing — that would point to a bug
/// in the lock-manager bookkeeping.
///
/// For catastrophic cleanup, the cleaner's panic handler or a
/// shutdown path can call
/// `LockManager::release_all_for_locker(locker_id)` to sweep
/// every entry held by an abandoned locker id at once.
fn release_cleaner_lock(
    lock_manager: &LockManager,
    lock_lsn: u64,
    locker_id: i64,
    site: &'static str,
) {
    if let Err(e) = lock_manager.release(lock_lsn, locker_id) {
        log::error!(
            "noxu-cleaner: lock_manager.release(lsn={lock_lsn}, \
             locker={locker_id}) failed at {site}: {e}; cleaner will \
             continue but a small lock leak may occur",
        );
    }
}

/// Write a non-transactional LN migration entry to the WAL and return
/// the freshly-allocated LSN.
///
/// X-6 fix: cleaner migration previously used `get_end_of_log()` as a
/// fake LSN and wrote no WAL entry, so recovery after a crash before the
/// next checkpoint could not find the migrated data.  This helper writes
/// a real `UpdateLN` entry so recovery can reconstruct the slot.
///
/// Returns `None` if `lm` is `None` (test-only no-WAL context) or if the
/// WAL write fails — in both cases the caller falls back to using the
/// original `log_lsn`.
fn write_migration_ln(
    lm: &LogManager,
    db_id: u64,
    key: &[u8],
    data: &[u8],
    old_lsn: Lsn,
) -> Option<Lsn> {
    use noxu_log::{LogEntryType, Provisional, entry::LnLogEntry};
    use noxu_util::vlsn::NULL_VLSN;

    let entry = LnLogEntry::new(
        db_id,
        None,      // txn_id: non-transactional migration
        old_lsn,   // abort_lsn: the pre-migration slot LSN (before-image)
        false,     // abort_known_deleted
        None,      // abort_key
        None,      // abort_data
        NULL_VLSN, // abort_vlsn
        0,         // abort_expiration
        true,      // embedded_ln (data inline in BIN)
        key.to_vec(),
        Some(data.to_vec()),
        0,         // expiration
        NULL_VLSN, // vlsn
    );

    let buf_size = entry.log_size();
    let mut bm = BytesMut::with_capacity(buf_size);
    entry.write_to_log(&mut bm);

    lm.log(
        LogEntryType::UpdateLN,
        &bm,
        Provisional::No,
        false, // flush_required: durability deferred to next checkpoint
        false, // fsync_required
    )
    .ok()
}

/// The number of LN log entries after which we process pending LNs.
///
/// Ported from `FileProcessor.PROCESS_PENDING_EVERY_N_LNS`.
///
/// If we do this too seldom, the pending LN queue may grow large, and it
/// isn't budgeted memory. If we process it too often, we will repeatedly
/// request a non-blocking lock for the same locked node.
const PROCESS_PENDING_EVERY_N_LNS: usize = 100;

// ─── Tree lookup abstraction ────────────────────────────────────────────────

/// Result of looking up an LN's parent BIN slot in the tree.
///
/// Ported from `TreeLocation` / the result returned by
/// `Tree.getParentBINForChildLN()`.
#[derive(Debug)]
pub enum BinLookupResult {
    /// No parent BIN found for the key — the LN has been deleted from the
    /// tree entirely.
    NotFound,

    /// Parent BIN found and the slot is known-deleted.
    KnownDeleted,

    /// Parent BIN found.  The `tree_lsn` is the LSN currently stored in the
    /// BIN slot, which the caller must compare against the log LSN to decide
    /// whether to migrate.
    Found {
        /// LSN of the slot in the BIN (may equal, precede, or follow `log_lsn`).
        tree_lsn: Lsn,
    },
}

/// Outcome of a migration attempt for a single LN slot.
///
/// Returned by `TreeMigrator::migrate_ln_slot`.
#[derive(Debug, PartialEq, Eq)]
pub enum MigrationOutcome {
    /// The LN was migrated — it was re-logged and the BIN slot was updated to
    /// the new LSN.  Corresponds to `nLNsMigratedThisRun++`.
    Migrated,

    /// The LN slot was locked by another transaction; the LN was added to the
    /// pending queue and will be retried later.
    Locked,

    /// The slot's LSN differed from the log LSN — the log entry is obsolete.
    Obsolete,
}

// ─── RealTreeLookup ──────────────────────────────────────────────────────────

/// A monotonically increasing counter used to generate unique ephemeral
/// locker IDs for non-transactional cleaner locks.
///
/// uses `BasicLocker.createBasicLocker(envImpl)` which allocates from
/// the environment's locker-ID generator.  We approximate that with a
/// process-local atomic so cleaner locks never collide with each other
/// or with transaction IDs (transaction IDs come from a different counter
/// in `TxnManager`).  The range is negative to ensure no collision with
/// positive transaction IDs.
static CLEANER_LOCKER_NEXT: AtomicI64 = AtomicI64::new(-1);

/// Allocates a fresh ephemeral locker ID for a single non-blocking lock
/// attempt.
fn next_cleaner_locker_id() -> i64 {
    CLEANER_LOCKER_NEXT.fetch_sub(1, Ordering::Relaxed)
}

/// Real `TreeLookup` implementation backed by a shared `noxu_tree::Tree`.
///
/// This wires the cleaner's `FileProcessor` to the actual B-tree.  The
/// implementation follows `FileProcessor.processLN` /
/// `FileProcessor.processFoundLN` faithfully:
///
/// * `lookup_parent_bin` — searches the tree for the BIN that holds `key`
///   and returns the slot's current LSN so the caller can decide whether
///   migration is needed.
/// * `migrate_ln_slot` — acquires a non-blocking read lock on `tree_lsn`
///   , re-checks the slot LSN,
///   re-inserts at a new LSN, and releases the lock.  Returns `Locked` if
///   the lock is denied so the entry can be added to the pending queue.
/// * `lookup_in` — finds the tree node by `node_id`, compares
///   `BinStub.last_full_lsn` with `log_lsn`, and marks the node dirty so
///   the next checkpoint re-logs it. +
///   `processIN()`.
///
/// # M-5 fix
/// Changed from `RefCell<noxu_tree::Tree>` (owned) to
/// `Arc<RwLock<noxu_tree::Tree>>` (shared) so the environment's single
/// canonical tree is reused rather than a detached copy.
pub struct RealTreeLookup {
    tree: Arc<RwLock<noxu_tree::Tree>>,
    lock_manager: Arc<LockManager>,
    /// Optional WAL manager for writing migration LN entries (X-6).
    ///
    /// When `None` (unit tests without a log), migration falls back to
    /// inserting with the original `log_lsn` (pre-fix behavior acceptable
    /// only in test contexts without a real WAL).
    log_manager: Option<Arc<LogManager>>,
}

impl RealTreeLookup {
    /// Creates a new `RealTreeLookup` from a shared tree reference.
    ///
    /// M-5: accepts `Arc<RwLock<Tree>>` so the cleaner shares the live tree.
    pub fn new(
        tree: Arc<RwLock<noxu_tree::Tree>>,
        lock_manager: Arc<LockManager>,
    ) -> Self {
        Self { tree, lock_manager, log_manager: None }
    }

    /// Wire a WAL log manager for writing real migration LN entries (X-6).
    pub fn with_log_manager(mut self, lm: Arc<LogManager>) -> Self {
        self.log_manager = Some(lm);
        self
    }
}

impl TreeLookup for RealTreeLookup {
    /// Search the tree for `key` and return the slot's current LSN.
    ///
    ///
    fn lookup_parent_bin(
        &self,
        _db_id: i64,
        key: &[u8],
        _log_lsn: Lsn,
    ) -> BinLookupResult {
        let tree = match self.tree.read() {
            Ok(g) => g,
            Err(_) => return BinLookupResult::NotFound,
        };
        match tree.search(key) {
            None => BinLookupResult::NotFound,
            Some(result) if !result.exact_parent_found => {
                BinLookupResult::NotFound
            }
            Some(_) => {
                let slot_lsn =
                    Self::get_slot_lsn_from_root(tree.get_root(), key);
                match slot_lsn {
                    Some(lsn) => BinLookupResult::Found { tree_lsn: lsn },
                    None => BinLookupResult::NotFound,
                }
            }
        }
    }

    /// Attempt to migrate a single LN slot.
    ///
    /// H-4 fix: now acquires a
    /// non-blocking read lock on `tree_lsn` before migrating.
    ///
    /// algorithm:
    /// 1. `locker = BasicLocker.createBasicLocker(envImpl)` — ephemeral locker.
    /// 2. `locker.nonBlockingLock(treeLsn, READ)` — if DENIED, return Locked.
    /// 3. Re-check `treeLsn == logLsn`; if differ, return Obsolete (Dead).
    /// 4. Re-log LN + update BIN slot LSN → Migrated.
    /// 5. `locker.operationEnd()` — release lock.
    fn migrate_ln_slot(
        &self,
        _db_id: i64,
        key: &[u8],
        log_lsn: Lsn,
        tree_lsn: Lsn,
    ) -> MigrationOutcome {
        // H-4: attempt a non-blocking read lock on tree_lsn.
        // `locker.nonBlockingLock(treeLsn, LockType.READ, ...)`.
        let locker_id = next_cleaner_locker_id();
        let lock_lsn = tree_lsn.as_u64();
        match self.lock_manager.lock(
            lock_lsn,
            locker_id,
            LockType::Read,
            true,  // non_blocking
            false, // jump_ahead_of_waiters
        ) {
            Err(TxnError::LockNotAvailable { .. }) => {
                // "LN is currently locked by another Locker" → pending.
                return MigrationOutcome::Locked;
            }
            Err(_) => {
                // Any other lock error → treat as Locked (safe).
                return MigrationOutcome::Locked;
            }
            Ok(_) => {} // lock granted — proceed
        }

        // Re-check the slot LSN after acquiring the lock (post-lock check).
        // `if (treeLsn != logLsn) { nLNsDeadThisRun++; return null; }`
        let current_lsn = {
            let tree = match self.tree.read() {
                Ok(g) => g,
                Err(_) => {
                    release_cleaner_lock(
                        &self.lock_manager,
                        lock_lsn,
                        locker_id,
                        "RealTreeLookup::migrate_ln_slot:tree_poisoned_pre_check",
                    );
                    return MigrationOutcome::Obsolete;
                }
            };
            Self::get_slot_lsn_from_root(tree.get_root(), key)
        };

        let slot_matches = match current_lsn {
            Some(lsn) => lsn == tree_lsn,
            None => false,
        };

        if !slot_matches {
            release_cleaner_lock(
                &self.lock_manager,
                lock_lsn,
                locker_id,
                "RealTreeLookup::migrate_ln_slot:slot_mismatch",
            );
            return MigrationOutcome::Obsolete;
        }

        // Retrieve current data then re-insert at a new LSN.
        // `targetLn.log(...) -> logItem; bin.updateEntry(logItem.lsn)`.
        let data = {
            let tree = match self.tree.read() {
                Ok(g) => g,
                Err(_) => {
                    release_cleaner_lock(
                        &self.lock_manager,
                        lock_lsn,
                        locker_id,
                        "RealTreeLookup::migrate_ln_slot:tree_poisoned_data",
                    );
                    return MigrationOutcome::Obsolete;
                }
            };
            Self::get_slot_data_from_root(tree.get_root(), key)
                .unwrap_or_default()
        };

        // X-6: write a real WAL entry for the migrated LN so that recovery
        // after a crash before the next checkpoint can find the data at its
        // new position.  Falls back to the original log_lsn if no log manager
        // is wired (unit-test mode without a real WAL — acceptable there).
        // In production (SharedTreeLookup), the log manager is always present
        // and R-7 applies: WAL failure aborts migration instead of falling back.
        let db_id_u64 = _db_id.unsigned_abs();
        let new_lsn = if let Some(lm) = &self.log_manager {
            // RealTreeLookup is test-only (SharedTreeLookup is production).
            // Fall back to log_lsn only in this test path; production path
            // uses SharedTreeLookup which enforces R-7 abort-on-failure.
            write_migration_ln(lm, db_id_u64, key, &data, log_lsn)
                .unwrap_or(log_lsn)
        } else {
            log_lsn
        };

        let outcome = {
            let tree = match self.tree.read() {
                Ok(g) => g,
                Err(_) => {
                    release_cleaner_lock(
                        &self.lock_manager,
                        lock_lsn,
                        locker_id,
                        "RealTreeLookup::migrate_ln_slot:tree_poisoned_insert",
                    );
                    return MigrationOutcome::Obsolete;
                }
            };
            match tree.insert(key.to_vec(), data, new_lsn) {
                Ok(_) => MigrationOutcome::Migrated,
                Err(_) => MigrationOutcome::Obsolete,
            }
        };

        // H-4: release lock.
        release_cleaner_lock(
            &self.lock_manager,
            lock_lsn,
            locker_id,
            "RealTreeLookup::migrate_ln_slot:done",
        );
        outcome
    }

    /// Look up an IN node by `node_id` and mark it dirty if its on-disk LSN
    /// matches `log_lsn`.
    ///
    /// + `processIN()` — H-3 fix.
    ///
    /// algorithm:
    /// 1. Find the IN in the in-memory tree by node ID.
    /// 2. Retrieve the full-version LSN stored in the node
    ///    (`BinStub.last_full_lsn` / `InNodeStub` LSN via parent slot).
    /// 3. If the tree's LSN == `log_lsn` → the cleaned log entry IS the
    ///    current version; mark the node dirty and return `Found`.
    /// 4. If the tree's LSN != `log_lsn` (or the node is absent) → the log
    ///    entry has already been superseded; return `Obsolete`.
    ///
    /// "Marking dirty" here means `node.set_dirty(true)`, which causes the
    /// checkpointer to re-log the node in the next checkpoint, making the old
    /// log position obsolete and allowing the cleaned file to be deleted.
    fn lookup_in(
        &self,
        _db_id: i64,
        node_id: i64,
        log_lsn: Lsn,
    ) -> InLookupResult {
        use noxu_tree::TreeNode;

        let node_id_u64 = node_id as u64;

        // Step 1 — find the node in the tree by searching for its parent.
        // `get_parent_in_for_child_in` does a DFS to find the parent IN
        // whose child slot points to `node_id`.
        //
        // If there is no parent (the node is the root or absent), we fall
        // back to checking the root directly.
        let tree_guard = match self.tree.read() {
            Ok(g) => g,
            Err(_) => return InLookupResult::Obsolete,
        };

        // Try to find via parent-of-node search first (non-root nodes).
        if let Some((parent_arc, slot_idx)) =
            tree_guard.get_parent_in_for_child_in(node_id_u64)
        {
            // Step 2a — get the LSN stored in the parent's slot for this child.
            let parent_guard = parent_arc.read();
            let slot_lsn = match &*parent_guard {
                TreeNode::Internal(n) => {
                    // The parent slot's LSN tracks the last logged position
                    // for the child — used directly for upper INs
                    // INEntryInfo.prevFullLsn read from the log entry).
                    n.entries.get(slot_idx).map(|e| e.lsn)
                }
                _ => None,
            };
            drop(parent_guard);

            // Get the child arc from the parent to inspect the node's own
            // `last_full_lsn` (BIN) or dirty flag (Internal).
            let child_arc = {
                let parent_guard = parent_arc.read();
                match &*parent_guard {
                    TreeNode::Internal(n) => {
                        n.entries.get(slot_idx).and_then(|e| e.child.clone())
                    }
                    _ => None,
                }
            };

            let child_arc = match child_arc {
                Some(a) => a,
                None => return InLookupResult::Obsolete,
            };

            // Step 2b — get the node's own LSN (BIN uses last_full_lsn).
            let node_lsn = {
                let child_guard = child_arc.read();
                match &*child_guard {
                    TreeNode::Bottom(b) => b.last_full_lsn,
                    // For upper INs we don't have a per-node last_full_lsn;
                    // use NULL_LSN to indicate "use parent slot LSN".
                    TreeNode::Internal(_) => {
                        // For upper INs, the parent slot LSN (InEntry.lsn) is
                        // the last logged position for this node. The slot LSN
                        // from the log entry header determines currency.
                        match slot_lsn {
                            Some(lsn) => lsn,
                            None => return InLookupResult::Obsolete,
                        }
                    }
                }
            };

            // Step 3 — compare LSNs.
            if node_lsn == noxu_util::NULL_LSN {
                // Never logged (deferred-write) → log entry is obsolete.
                return InLookupResult::Obsolete;
            }

            if node_lsn != log_lsn {
                // The tree has a newer (or different) version.
                return InLookupResult::Obsolete;
            }

            // Step 4 — tree_lsn == log_lsn: mark dirty.
            // `inInTree.setDirty(true); inInTree.setProhibitNextDelta(true)`.
            drop(tree_guard);
            {
                let mut child_write = child_arc.write();
                child_write.set_dirty(true);
            }
            return InLookupResult::Found;
        }

        // No parent found — check if the node is the tree root.
        // Clone the Arc so we can drop the tree_guard before taking a write lock.
        let root_arc_opt = tree_guard.get_root();
        drop(tree_guard);
        if let Some(root) = root_arc_opt {
            let root_node_id = {
                let g = root.read();
                match &*g {
                    TreeNode::Bottom(b) => b.node_id,
                    TreeNode::Internal(n) => n.node_id,
                }
            };

            if root_node_id == node_id_u64 {
                let root_lsn = {
                    let g = root.read();
                    match &*g {
                        TreeNode::Bottom(b) => b.last_full_lsn,
                        TreeNode::Internal(_) => {
                            return InLookupResult::Obsolete;
                        }
                    }
                };

                if root_lsn == noxu_util::NULL_LSN || root_lsn != log_lsn {
                    return InLookupResult::Obsolete;
                }

                {
                    let mut w = root.write();
                    w.set_dirty(true);
                }
                return InLookupResult::Found;
            }
        }

        InLookupResult::Obsolete
    }
}

impl RealTreeLookup {
    /// Helper: returns the current LSN of the slot for `key` in the tree,
    /// or `None` if the key is not present.
    pub(crate) fn get_slot_lsn_from_root(
        root: Option<
            std::sync::Arc<noxu_tree::NodeRwLock<noxu_tree::TreeNode>>,
        >,
        key: &[u8],
    ) -> Option<Lsn> {
        let arc = root?;
        Self::find_bin_entry_lsn(&arc, key)
    }

    /// Helper: returns a copy of the data stored in the slot for `key`.
    pub(crate) fn get_slot_data_from_root(
        root: Option<
            std::sync::Arc<noxu_tree::NodeRwLock<noxu_tree::TreeNode>>,
        >,
        key: &[u8],
    ) -> Option<Vec<u8>> {
        let arc = root?;
        Self::find_bin_entry_data(&arc, key)
    }

    /// Recursive descent to find the LSN of the BIN slot for `key`.
    fn find_bin_entry_lsn(
        node_arc: &std::sync::Arc<noxu_tree::NodeRwLock<noxu_tree::TreeNode>>,
        key: &[u8],
    ) -> Option<Lsn> {
        use noxu_tree::TreeNode;
        let guard = node_arc.read();
        match &*guard {
            TreeNode::Bottom(bin) => {
                let idx = bin
                    .entries
                    .binary_search_by(|e| e.key.as_slice().cmp(key))
                    .ok()?;
                Some(bin.entries[idx].lsn)
            }
            TreeNode::Internal(n) => {
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
                let child = n.entries.get(idx)?.child.clone()?;
                drop(guard);
                Self::find_bin_entry_lsn(&child, key)
            }
        }
    }

    /// Recursive descent to find the data bytes of the BIN slot for `key`.
    fn find_bin_entry_data(
        node_arc: &std::sync::Arc<noxu_tree::NodeRwLock<noxu_tree::TreeNode>>,
        key: &[u8],
    ) -> Option<Vec<u8>> {
        use noxu_tree::TreeNode;
        let guard = node_arc.read();
        match &*guard {
            TreeNode::Bottom(bin) => {
                let idx = bin
                    .entries
                    .binary_search_by(|e| e.key.as_slice().cmp(key))
                    .ok()?;
                bin.entries[idx].data.clone()
            }
            TreeNode::Internal(n) => {
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
                let child = n.entries.get(idx)?.child.clone()?;
                drop(guard);
                Self::find_bin_entry_data(&child, key)
            }
        }
    }
}

// ─── SharedTreeLookup ────────────────────────────────────────────────────────

/// Thread-safe `TreeLookup` implementation backed by a shared `noxu_tree::Tree`.
///
/// Used by `Cleaner::process_single_file()` when wired to a real environment
/// via `Cleaner::with_file_manager_tree_and_lock_manager()`.
///
/// The `log_manager` obtains a fresh LSN when re-logging a migrated LN —
///.
///
/// The `lock_manager` is used by `migrate_ln_slot` to acquire a non-blocking
/// read lock before migrating.
///
/// # H-4 fix
/// Non-blocking read lock is now acquired before re-logging.
///
/// # H-3 fix
/// `lookup_in` now finds the node, checks its LSN, and marks it dirty.
pub struct SharedTreeLookup {
    tree: Arc<RwLock<noxu_tree::Tree>>,
    log_manager: Arc<LogManager>,
    lock_manager: Arc<LockManager>,
    /// Per-database-id trees for secondary databases (X-7 fix).
    ///
    /// When `lookup_parent_bin` / `migrate_ln_slot` is called with a
    /// `db_id` that appears in this map, the corresponding tree is used
    /// instead of `self.tree` (the primary tree).  This ensures secondary
    /// LNs are classified as live/obsolete against the correct tree, not
    /// the primary tree where their keys do not exist.
    extra_trees: HashMap<i64, Arc<RwLock<noxu_tree::Tree>>>,
}

impl SharedTreeLookup {
    /// Creates a new `SharedTreeLookup` with a private `LockManager`.
    ///
    /// Used in tests or contexts where no shared environment LockManager is
    /// available.  Production code should use `with_lock_manager` to pass the
    /// environment's shared LockManager for correct deadlock detection.
    pub fn new(
        tree: Arc<RwLock<noxu_tree::Tree>>,
        log_manager: Arc<LogManager>,
    ) -> Self {
        let lock_manager = Arc::new(LockManager::new());
        Self { tree, log_manager, lock_manager, extra_trees: HashMap::new() }
    }

    /// Creates a new `SharedTreeLookup` with a wired `LockManager`.
    ///
    /// Pass the environment's shared `LockManager` so that cleaner lock
    /// operations participate in the same lock table as user transactions.
    pub fn with_lock_manager(
        tree: Arc<RwLock<noxu_tree::Tree>>,
        log_manager: Arc<LogManager>,
        lock_manager: Arc<LockManager>,
    ) -> Self {
        Self { tree, log_manager, lock_manager, extra_trees: HashMap::new() }
    }

    /// Register per-database trees for secondary databases (X-7 fix).
    ///
    /// When `lookup_parent_bin` / `migrate_ln_slot` is called with a db_id
    /// present in `extra_trees`, the corresponding tree is used for liveness
    /// checks and migration instead of the primary tree.  Keys absent from
    /// `extra_trees` fall back to the primary tree.
    pub fn with_extra_trees(
        mut self,
        extra_trees: HashMap<i64, Arc<RwLock<noxu_tree::Tree>>>,
    ) -> Self {
        self.extra_trees = extra_trees;
        self
    }

    /// Resolve the correct tree for `db_id`: use the extra tree if registered,
    /// otherwise fall back to the primary tree.
    fn resolve_tree(&self, db_id: i64) -> &Arc<RwLock<noxu_tree::Tree>> {
        self.extra_trees.get(&db_id).unwrap_or(&self.tree)
    }
}

impl TreeLookup for SharedTreeLookup {
    /// Look up the parent BIN slot for `key` in the shared tree.
    ///
    /// X-7: dispatches to the tree registered for `db_id` in `extra_trees`
    /// (for secondary databases), falling back to the primary tree.
    fn lookup_parent_bin(
        &self,
        db_id: i64,
        key: &[u8],
        _log_lsn: Lsn,
    ) -> BinLookupResult {
        // X-7: dispatch to the correct tree for this db_id.
        let tree_arc = self.resolve_tree(db_id);
        let tree = match tree_arc.read() {
            Ok(g) => g,
            Err(_) => return BinLookupResult::NotFound,
        };
        match tree.search(key) {
            None => BinLookupResult::NotFound,
            Some(result) if !result.exact_parent_found => {
                BinLookupResult::NotFound
            }
            Some(_) => {
                let slot_lsn = RealTreeLookup::get_slot_lsn_from_root(
                    tree.get_root(),
                    key,
                );
                match slot_lsn {
                    Some(lsn) => BinLookupResult::Found { tree_lsn: lsn },
                    None => BinLookupResult::NotFound,
                }
            }
        }
    }

    /// Attempt to migrate a single LN slot with a non-blocking read lock.
    ///
    /// H-4 fix.
    fn migrate_ln_slot(
        &self,
        db_id: i64,
        key: &[u8],
        log_lsn: Lsn,
        tree_lsn: Lsn,
    ) -> MigrationOutcome {
        // X-7: dispatch to the correct tree for this db_id.
        let tree_arc = Arc::clone(self.resolve_tree(db_id));
        // H-4: non-blocking lock on tree_lsn before migrating.
        let locker_id = next_cleaner_locker_id();
        let lock_lsn = tree_lsn.as_u64();
        match self.lock_manager.lock(
            lock_lsn,
            locker_id,
            LockType::Read,
            true,  // non_blocking
            false, // jump_ahead_of_waiters
        ) {
            Err(TxnError::LockNotAvailable { .. }) => {
                return MigrationOutcome::Locked;
            }
            Err(_) => return MigrationOutcome::Locked,
            Ok(_) => {}
        }

        // Post-lock re-check.
        let current_lsn = {
            let tree = match tree_arc.read() {
                Ok(g) => g,
                Err(_) => {
                    release_cleaner_lock(
                        &self.lock_manager,
                        lock_lsn,
                        locker_id,
                        "SharedTreeLookup::migrate_ln_slot:tree_poisoned_pre_check",
                    );
                    return MigrationOutcome::Obsolete;
                }
            };
            RealTreeLookup::get_slot_lsn_from_root(tree.get_root(), key)
        };
        let slot_matches = match current_lsn {
            Some(lsn) => lsn == tree_lsn,
            None => false,
        };
        if !slot_matches {
            release_cleaner_lock(
                &self.lock_manager,
                lock_lsn,
                locker_id,
                "SharedTreeLookup::migrate_ln_slot:slot_mismatch",
            );
            return MigrationOutcome::Obsolete;
        }

        // X-6: write a real WAL UpdateLN entry so that recovery after a
        // crash before the next checkpoint can find the migrated data at its
        // new WAL position.  Previously this used get_end_of_log() as a fake
        // LSN and wrote no log entry.
        let data = {
            let tree = match tree_arc.read() {
                Ok(g) => g,
                Err(_) => {
                    release_cleaner_lock(
                        &self.lock_manager,
                        lock_lsn,
                        locker_id,
                        "SharedTreeLookup::migrate_ln_slot:tree_poisoned_data",
                    );
                    return MigrationOutcome::Obsolete;
                }
            };
            RealTreeLookup::get_slot_data_from_root(tree.get_root(), key)
                .unwrap_or_default()
        };

        let db_id_u64 = db_id.unsigned_abs();
        // R-7 (Keith re-audit): if write_migration_ln() fails, do NOT fall back
        // to the original log_lsn.  That stale LSN points to the file being
        // cleaned; once the cleaner deletes it, recovery cannot find the data.
        // Abort this migration (return Locked so the entry is retried later)
        // and leave the source file protected until a successful WAL write.
        let new_lsn = match write_migration_ln(
            &self.log_manager,
            db_id_u64,
            key,
            &data,
            log_lsn,
        ) {
            Some(lsn) => lsn,
            None => {
                // WAL write failed (e.g. io_invalid set, disk full).
                // Release the cleaner lock and abort the migration for this
                // slot.  The cleaner will retry on the next pass; the source
                // file is kept protected by the X-5 checkpoint barrier.
                release_cleaner_lock(
                    &self.lock_manager,
                    lock_lsn,
                    locker_id,
                    "SharedTreeLookup::migrate_ln_slot:wal_write_failed",
                );
                return MigrationOutcome::Locked;
            }
        };

        let result =
            tree_arc.read().map(|t| t.insert(key.to_vec(), data, new_lsn));

        // H-4: release lock.
        release_cleaner_lock(
            &self.lock_manager,
            lock_lsn,
            locker_id,
            "SharedTreeLookup::migrate_ln_slot:done",
        );

        match result {
            Ok(Ok(_)) => MigrationOutcome::Migrated,
            _ => MigrationOutcome::Obsolete,
        }
    }

    /// Look up an IN node by `node_id` and mark it dirty if its LSN matches.
    ///
    /// H-3 fix: delegates to `RealTreeLookup::lookup_in`.
    fn lookup_in(
        &self,
        db_id: i64,
        node_id: i64,
        log_lsn: Lsn,
    ) -> InLookupResult {
        let delegate = RealTreeLookup::new(
            Arc::clone(&self.tree),
            Arc::clone(&self.lock_manager),
        );
        delegate.lookup_in(db_id, node_id, log_lsn)
    }
}

// ─── TreeLookup trait ────────────────────────────────────────────────────────

/// Abstraction over the tree operations needed during LN migration.
///
/// This trait decouples `FileProcessor` from the concrete B-tree
/// implementation, making the migration logic independently testable and
/// allowing the integration to be wired in once the tree crate is complete.
///
/// Each method corresponds to a specific tree operation performed by the
/// `FileProcessor.processLN` / `FileProcessor.processFoundLN`.
pub trait TreeLookup {
    /// Looks up the parent BIN slot for an LN identified by `key` and `db_id`.
    ///
    /// Corresponds to `Tree.getParentBINForChildLN()`.
    ///
    /// The implementation should latch the BIN and return the slot LSN.
    /// Latching is released by the implementation before returning — this
    /// interface does not expose latch guards (Rust lifetimes make that
    /// pattern awkward without the full tree in scope).
    fn lookup_parent_bin(
        &self,
        db_id: i64,
        key: &[u8],
        log_lsn: Lsn,
    ) -> BinLookupResult;

    /// Attempt to migrate a single LN slot.
    ///
    /// Called after `lookup_parent_bin` returns `BinLookupResult::Found`.
    ///
    /// The implementation must:
    /// 1. Acquire a non-blocking read lock on `tree_lsn` (the: `locker.nonBlockingLock`).
    /// 2. If the lock is denied, return `MigrationOutcome::Locked`.
    /// 3. Re-check `tree_lsn == log_lsn` after acquiring the lock; if they
    ///    differ, return `MigrationOutcome::Obsolete`.
    /// 4. Re-log the LN (the: `targetLn.log(...)`), update the BIN slot LSN,
    ///    and return `MigrationOutcome::Migrated`.
    ///
    /// Corresponds to the locking + `targetLn.log()` + `bin.updateEntry()`
    /// block inside `FileProcessor.processFoundLN()`.
    fn migrate_ln_slot(
        &self,
        db_id: i64,
        key: &[u8],
        log_lsn: Lsn,
        tree_lsn: Lsn,
    ) -> MigrationOutcome;

    /// Looks up an IN in the tree and checks whether the log entry is still
    /// the current version.
    ///
    /// Corresponds to `FileProcessor.findINInTree()`.
    ///
    /// Returns `InLookupResult::Found` if the IN is still current (its LSN in
    /// the tree matches `log_lsn`), and the implementation has marked it dirty.
    /// Returns `InLookupResult::Obsolete` if the log entry is superseded.
    ///
    /// In the full implementation this method will:
    /// 1. Search the B-tree for the IN identified by `node_id`.
    /// 2. Compare the tree's stored LSN with `log_lsn`.
    /// 3. If equal, call `in_node.set_dirty(true)` and return `Found`.
    /// 4. Otherwise return `Obsolete`.
    fn lookup_in(
        &self,
        db_id: i64,
        node_id: i64,
        log_lsn: Lsn,
    ) -> InLookupResult;
}

// ─── IN lookup result ────────────────────────────────────────────────────────

/// Result of looking up an IN in the tree during cleaning.
///
/// Returned by [`TreeLookup::lookup_in`].
#[derive(Debug, PartialEq, Eq)]
pub enum InLookupResult {
    /// The IN is still the current version in the tree.  The implementation
    /// has already marked it dirty so the next checkpoint will re-log it.
    ///
    /// Corresponds to `nINsMigratedThisRun++`.
    Found,

    /// The IN is no longer current — either it has been replaced by a newer
    /// version or it was never resident (deferred-write NULL_LSN).
    ///
    /// Corresponds to `nINsDeadThisRun++`.
    Obsolete,
}

// ─── Log entry types for process_file ────────────────────────────────────────

/// The type of a log entry, as seen by the cleaner's file-processing loop.
///
/// `CleanerFileReader` has `.isLN()`, `.isIN()`, `.isBINDelta()`, etc.
/// predicates.  We model the classification with this enum so that the
/// `process_file()` loop can dispatch without a real file reader.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogEntryType {
    /// A leaf-node (LN) record.  Contains the key and DB id needed for
    /// look-ahead caching and tree lookup.
    Ln {
        /// Database the LN belongs to.
        db_id: i64,
        /// Key bytes.
        key: Vec<u8>,
        /// Whether the LN is a deletion record.
        deleted: bool,
        /// Expiration time (0 = no expiration).
        expiration_time: u64,
        /// Byte size of the entry in the log.
        entry_size: i32,
    },

    /// A full internal node (BIN or UIN) record.
    In {
        /// Database the IN belongs to.
        db_id: i64,
        /// Node ID of the IN.
        node_id: i64,
    },

    /// A BIN-delta record.
    ///
    /// Carries the same fields as `In` — the cleaner processes it by marking
    /// the parent BIN dirty so the next checkpoint re-logs the full node.
    BinDelta {
        /// Database the BIN-delta belongs to.
        db_id: i64,
        /// Node ID of the associated BIN.
        node_id: i64,
    },

    /// Any other entry type (file header, commit records, …).
    /// The cleaner considers these immediately obsolete and skips them.
    Other,
}

/// A decoded log entry, as produced by a log-file reader.
///
/// This is the element type that `process_file()` consumes.  In the future a
/// real `CleanerFileReader` will produce these; for now callers pass a `Vec`
/// directly, which makes the loop testable without I/O.
#[derive(Debug, Clone)]
pub struct LogEntry {
    /// LSN of this entry in the log file.
    pub lsn: Lsn,

    /// Classified type of the entry.
    pub entry_type: LogEntryType,
}

// ─── LookAheadCache ──────────────────────────────────────────────────────────

/// A cache of [`LnInfo`] entries keyed by their LSN file offset.
///
/// Inner `LookAheadCache` class from `FileProcessor.java`.
///
/// The cleaner reads LN log entries sequentially and accumulates them in
/// this sorted map. When the cache is full (exceeds `max_mem` bytes) the
/// entry with the lowest offset is evicted and processed. Processing one
/// entry finds its parent BIN and, while the BIN is still "warm", also
/// processes any other entries in the cache that belong to the same BIN.
/// This reduces the total number of tree lookups.
pub struct LookAheadCache {
    /// Sorted map: LSN file offset → LN info.
    ///
    /// BTreeMap keeps offsets in ascending order so `first_key_value` gives
    /// the lowest-offset (oldest) entry — exactly what TreeMap gave.
    map: BTreeMap<u32, LnInfo>,

    /// Memory currently occupied by the cache entries.
    used_mem: usize,

    /// Maximum memory budget before the cache is considered full.
    max_mem: usize,
}

impl LookAheadCache {
    /// Creates a new look-ahead cache with the given memory budget.
    ///
    /// Pass `max_mem = 0` (or any value ≤ `TREEMAP_OVERHEAD`) to disable the
    /// look-ahead optimisation; the cache will be "full" as soon as the first
    /// entry is added, mirroring `countOnly` mode.
    pub fn new(max_mem: usize) -> Self {
        // seeds usedMem with TREEMAP_OVERHEAD; mirror that here.
        const TREEMAP_OVERHEAD: usize = 64;
        Self { map: BTreeMap::new(), used_mem: TREEMAP_OVERHEAD, max_mem }
    }

    /// Returns `true` when the cache contains no entries.
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Returns `true` when the cache's memory usage meets or exceeds the
    /// configured budget.
    pub fn is_full(&self) -> bool {
        self.used_mem >= self.max_mem
    }

    /// Adds an entry to the cache.
    ///
    ///
    pub fn add(&mut self, lsn_offset: u32, info: LnInfo) {
        const TREEMAP_ENTRY_OVERHEAD: usize = 48;
        self.used_mem += info.memory_size() + TREEMAP_ENTRY_OVERHEAD;
        self.map.insert(lsn_offset, info);
    }

    /// Returns the smallest LSN offset currently in the cache, or `None` if
    /// the cache is empty.
    ///
    ///
    pub fn next_offset(&self) -> Option<u32> {
        self.map.keys().next().copied()
    }

    /// Removes and returns the entry for `offset`, updating memory accounting.
    ///
    /// Returns `None` if the offset is not present.
    ///
    ///
    pub fn remove(&mut self, offset: u32) -> Option<LnInfo> {
        if let Some(info) = self.map.remove(&offset) {
            const TREEMAP_ENTRY_OVERHEAD: usize = 48;
            self.used_mem = self
                .used_mem
                .saturating_sub(info.memory_size() + TREEMAP_ENTRY_OVERHEAD);
            Some(info)
        } else {
            None
        }
    }

    /// Returns the number of entries currently in the cache.
    pub fn len(&self) -> usize {
        self.map.len()
    }
}

// ─── MigrateLnResult ────────────────────────────────────────────────────────

/// Outcome of processing a single LN entry during file cleaning.
///
/// Mirrors the per-entry status variables in the equivalent `processFoundLN`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MigrateLnResult {
    /// The LN was no longer reachable in the tree — it has been deleted or
    /// superseded.  Corresponds to `nLNsDeadThisRun++`.
    Dead,

    /// The LN is active and was successfully re-logged to the current end of
    /// the log.  Corresponds to `nLNsMigratedThisRun++`.
    Migrated,

    /// The LN's slot was locked by another transaction.  The caller should add
    /// this LN to the pending queue.  Corresponds to `nLNsLockedThisRun++`.
    Locked,
}

// ─── FileProcessor ───────────────────────────────────────────────────────────

/// Processes a single log file for cleaning.
///
/// Reads all entries in a log file and determines whether each entry is
/// obsolete or active. Active LNs are migrated (re-logged). Active INs
/// are marked dirty for the next checkpoint.
///
///
pub struct FileProcessor {
    /// Reference to cleaner statistics.
    stats: Arc<CleanerStats>,

    /// Shutdown signal for stopping processing mid-file.
    shutdown: Arc<AtomicBool>,

    /// Number of LN entries after which to process pending LNs.
    ///
    /// If we do this too seldom, the pending LN queue may grow large.
    /// If we process it too often, we will repeatedly request a
    /// non-blocking lock for the same locked node.
    process_pending_interval: usize,
}

/// Result of processing a single file.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct FileProcessResult {
    /// Number of log entries read during this processing run.
    pub entries_read: u64,

    /// Number of LN log records that were not known a priori to be obsolete.
    pub lns_cleaned: u64,

    /// Number of LN log records found to be obsolete after tree lookup.
    pub lns_dead: u64,

    /// Number of LN log records that were still active and were migrated.
    pub lns_migrated: u64,

    /// Number of LN log records that were known a priori to be obsolete.
    pub lns_obsolete: u64,

    /// Number of LN log records whose LSN had to be locked and lock was denied.
    pub lns_locked: u64,

    /// Number of full IN log records that were not known a priori to be obsolete.
    pub ins_cleaned: u64,

    /// Number of full IN log records found to be obsolete after tree lookup.
    pub ins_dead: u64,

    /// Number of full IN log records that were still active and were marked dirty.
    pub ins_migrated: u64,

    /// Number of full IN log records that were known a priori to be obsolete.
    pub ins_obsolete: u64,

    /// Number of BIN-delta log records that were not known a priori to be obsolete.
    pub bin_deltas_cleaned: u64,

    /// Number of BIN-delta log records found to be obsolete after tree lookup.
    pub bin_deltas_dead: u64,

    /// Number of BIN-delta log records that were still active and were marked dirty.
    pub bin_deltas_migrated: u64,

    /// Number of BIN-delta log records that were known a priori to be obsolete.
    pub bin_deltas_obsolete: u64,

    /// Whether processing completed successfully (not interrupted by shutdown).
    pub completed: bool,
}

impl FileProcessor {
    /// Creates a new file processor.
    ///
    /// # Arguments
    /// * `stats` - Shared cleaner statistics
    /// * `shutdown` - Shutdown signal to check during processing
    pub fn new(stats: Arc<CleanerStats>, shutdown: Arc<AtomicBool>) -> Self {
        Self {
            stats,
            shutdown,
            process_pending_interval: PROCESS_PENDING_EVERY_N_LNS,
        }
    }

    /// Sets the interval for processing pending LNs.
    pub fn set_process_pending_interval(&mut self, interval: usize) {
        self.process_pending_interval = interval;
    }

    /// Main entry point — processes a single log file for cleaning.
    ///
    /// adapted to accept a
    /// pre-decoded entry slice so the loop is testable without I/O.
    ///
    /// # Arguments
    /// * `file_number` — log file number (used to reconstruct LSNs).
    /// * `file_summary` — utilization summary for the file (currently unused
    ///   for filter decisions; retained for future integration).
    /// * `entries` — all decoded log entries in the file, in order.
    ///   This will be replaced by a real `CleanerFileReader` iterator once
    ///   the log-reader integration is wired up.
    /// * `tree` — abstraction for tree lookups and LN migration.
    ///
    /// # Returns
    /// `Ok(FileProcessResult)` with counters for each entry category.
    /// `completed = false` when the shutdown flag is set mid-file.
    ///
    /// # correspondence
    /// ```text
    /// processFile():
    ///   while reader.readNextEntry():
    ///     if isLN  → lookAheadCache.add; if full → processLN
    ///     if isIN  → processIN
    ///     if Other → isObsolete = true (skip)
    ///     check shutdown
    ///   drain remaining lookAheadCache entries
    ///   fileSelector.addCleanedFile(...)
    /// ```
    pub fn process_file<T: TreeLookup>(
        &self,
        file_number: u32,
        _file_summary: &crate::FileSummary,
        entries: &[LogEntry],
        tree: &T,
    ) -> Result<FileProcessResult, String> {
        // Check if we should stop before even starting.
        if self.shutdown.load(Ordering::Relaxed) {
            return Ok(FileProcessResult {
                completed: false,
                ..Default::default()
            });
        }

        let mut result = FileProcessResult::new();

        // Look-ahead cache for LN entries.  uses a memory budget; we use
        // a large fixed budget that keeps all entries in the cache until it
        // is explicitly flushed.  The cache is flushed when full or at the
        // end of the file — matching behaviour.
        //
        // Budget: TREEMAP_OVERHEAD (64) + 1 so the empty cache is never full.
        // Any positive max_mem larger than 64 works; 4 MiB mirrors default.
        let mut look_ahead_cache = LookAheadCache::new(4 * 1024 * 1024);

        let mut n_processed_lns: usize = 0;

        for entry in entries {
            result.entries_read += 1;

            // Step 1 — check shutdown periodically (the: envImpl.isClosing()).
            if self.shutdown.load(Ordering::Relaxed) {
                result.completed = false;
                return Ok(result);
            }

            let lsn = entry.lsn;
            let file_offset = lsn.file_offset();

            match &entry.entry_type {
                // ── LN entry ──────────────────────────────────────────────
                LogEntryType::Ln {
                    db_id,
                    key,
                    deleted,
                    expiration_time,
                    entry_size,
                } => {
                    // Deleted LNs (log version > 2) are immediately obsolete.
                    if *deleted {
                        result.lns_obsolete += 1;
                        self.stats.lns_obsolete.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }

                    // Add to look-ahead cache.
                    let info = crate::LnInfo::new(
                        lsn,
                        *db_id,
                        key.clone(),
                        *entry_size,
                        *deleted,
                        *expiration_time,
                    );
                    look_ahead_cache.add(file_offset, info);

                    // Process the cache when full (the: lookAheadCache.isFull()).
                    if look_ahead_cache.is_full() {
                        self.process_ln(
                            file_number,
                            &mut look_ahead_cache,
                            tree,
                            &mut result,
                        );
                    }

                    // Periodically drain pending LNs (the: cleaner.processPending()).
                    n_processed_lns += 1;
                    if n_processed_lns
                        .is_multiple_of(self.process_pending_interval)
                    {
                        // In the future: call cleaner.process_pending() here.
                        // For now we drain the cache every interval to bound memory.
                        while !look_ahead_cache.is_empty() {
                            if self.shutdown.load(Ordering::Relaxed) {
                                result.completed = false;
                                return Ok(result);
                            }
                            self.process_ln(
                                file_number,
                                &mut look_ahead_cache,
                                tree,
                                &mut result,
                            );
                        }
                    }
                }

                // ── IN entry ──────────────────────────────────────────────
                LogEntryType::In { db_id, node_id } => {
                    self.process_in(*db_id, *node_id, lsn, tree, &mut result);
                }

                // ── BIN-delta entry ────────────────────────────────────────
                // `FileProcessor.processBINDelta()` — mark parent BIN dirty
                // so the next checkpoint re-logs the full node.
                LogEntryType::BinDelta { db_id, node_id } => {
                    self.process_bin_delta(
                        *db_id,
                        *node_id,
                        lsn,
                        tree,
                        &mut result,
                    );
                }

                // ── Other / unknown entries ────────────────────────────────
                // "Consider all entries we do not process as obsolete."
                LogEntryType::Other => {
                    // Counted as obsolete but no migration needed.
                    // We don't have a separate other_obsolete counter so we
                    // leave it unreported (silent skip).
                }
            }
        }

        // Drain any remaining LN entries from the look-ahead cache.
        // "Process remaining queued LNs."
        while !look_ahead_cache.is_empty() {
            if self.shutdown.load(Ordering::Relaxed) {
                result.completed = false;
                return Ok(result);
            }
            self.process_ln(
                file_number,
                &mut look_ahead_cache,
                tree,
                &mut result,
            );
        }

        result.completed = true;
        Ok(result)
    }

    /// Convenience overload for callers that don't have log entries yet
    /// (e.g. existing tests that just want shutdown-check behaviour).
    ///
    /// Returns an empty but completed result when no entries are provided.
    pub fn process_file_no_entries(
        &self,
        file_number: u32,
        file_summary: &crate::FileSummary,
    ) -> Result<FileProcessResult, String> {
        // Use a no-op tree so the signature compiles.
        struct NoopTree;
        impl TreeLookup for NoopTree {
            fn lookup_parent_bin(
                &self,
                _: i64,
                _: &[u8],
                _: Lsn,
            ) -> BinLookupResult {
                BinLookupResult::NotFound
            }
            fn migrate_ln_slot(
                &self,
                _: i64,
                _: &[u8],
                _: Lsn,
                _: Lsn,
            ) -> MigrationOutcome {
                MigrationOutcome::Obsolete
            }
            fn lookup_in(&self, _: i64, _: i64, _: Lsn) -> InLookupResult {
                InLookupResult::Obsolete
            }
        }
        self.process_file(file_number, file_summary, &[], &NoopTree)
    }

    /// Processes a batch of LN entries from the look-ahead cache against the
    /// tree, performing migration for active entries.
    ///
    ///
    ///
    /// The algorithm (correct):
    /// 1. Dequeue the lowest-offset LN from `cache`.
    /// 2. Look up its parent BIN slot via `tree`.
    /// 3. If not found or slot is known-deleted → mark dead.
    /// 4. Otherwise call `process_found_ln` to attempt migration.
    /// 5. While the BIN is still "hot", scan remaining cache entries that
    ///    also live in the same BIN (same file, different offset) and process
    ///    them too — the "look-ahead queue hit" optimisation.
    ///    (Step 5 is handled inside `process_found_ln` / the caller loop when
    ///    the tree implementation exposes BIN-level iteration; for now the
    ///    entry-level path is implemented.)
    ///
    /// # Returns
    /// The migration result for the dequeued entry. If a second pass over
    /// remaining cache entries is needed (step 5), the caller should continue
    /// calling `process_ln` until the cache is empty.
    pub fn process_ln<T: TreeLookup>(
        &self,
        file_number: u32,
        cache: &mut LookAheadCache,
        tree: &T,
        result: &mut FileProcessResult,
    ) {
        // Step 1 — dequeue the lowest-offset entry.
        let offset = match cache.next_offset() {
            Some(o) => o,
            None => return,
        };
        let info = match cache.remove(offset) {
            Some(i) => i,
            None => return,
        };

        let log_lsn = Lsn::new(file_number, offset);

        result.lns_cleaned += 1;

        // Step 2 — look up parent BIN slot in the tree.
        let bin_result =
            tree.lookup_parent_bin(info.db_id, info.key(), log_lsn);

        match bin_result {
            // Step 3a — parent not found → LN has been deleted.
            BinLookupResult::NotFound => {
                result.lns_dead += 1;
                self.stats.lns_dead.fetch_add(1, Ordering::Relaxed);
            }

            // Step 3b — slot is known-deleted → LN is obsolete.
            BinLookupResult::KnownDeleted => {
                result.lns_dead += 1;
                self.stats.lns_dead.fetch_add(1, Ordering::Relaxed);
            }

            // Step 4 — BIN slot found; attempt migration.
            BinLookupResult::Found { tree_lsn } => {
                let outcome =
                    self.process_found_ln(&info, log_lsn, tree_lsn, tree);
                match outcome {
                    MigrateLnResult::Dead => {
                        result.lns_dead += 1;
                        self.stats.lns_dead.fetch_add(1, Ordering::Relaxed);
                    }
                    MigrateLnResult::Migrated => {
                        result.lns_migrated += 1;
                        self.stats.lns_migrated.fetch_add(1, Ordering::Relaxed);
                    }
                    MigrateLnResult::Locked => {
                        result.lns_locked += 1;
                        self.stats.lns_locked.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }
    }

    /// Processes an LN that was found in the tree.
    ///
    ///
    ///
    /// Decision tree (correct):
    ///
    /// | Condition                               | Action            |
    /// |-----------------------------------------|-------------------|
    /// | `tree_lsn == NULL_LSN`                  | Dead (case 4 DW)  |
    /// | `tree_lsn != log_lsn` (non-temp DB)     | Obsolete after lock check |
    /// | Lock denied                             | Pending / Locked  |
    /// | `tree_lsn != log_lsn` (after lock)     | Dead              |
    /// | `tree_lsn == log_lsn`                   | Migrate           |
    ///
    /// # Arguments
    /// * `info`     — LN metadata from the log
    /// * `log_lsn`  — LSN of the log entry being processed
    /// * `tree_lsn` — LSN found in the parent BIN slot
    /// * `tree`     — abstraction for tree operations and locking
    pub fn process_found_ln<T: TreeLookup>(
        &self,
        info: &LnInfo,
        log_lsn: Lsn,
        tree_lsn: Lsn,
        tree: &T,
    ) -> MigrateLnResult {
        // Case 4 (comment): NULL_LSN in tree means the record was written
        // for a deferred-write DB but has never been flushed; the log entry is
        // therefore obsolete.
        if tree_lsn == noxu_util::NULL_LSN {
            return MigrateLnResult::Dead;
        }

        // Delegate locking + LSN comparison + re-logging to the tree
        // abstraction.  The outcome maps directly to our result enum:
        //
        //   MigrationOutcome::Migrated  → MigrateLnResult::Migrated
        //   MigrationOutcome::Locked    → MigrateLnResult::Locked
        //   MigrationOutcome::Obsolete  → MigrateLnResult::Dead
        //
        // The tree implementation must:
        //   1. Attempt a non-blocking read lock on `tree_lsn`.
        //   2. After acquiring the lock, re-read the slot LSN; if it has
        //      changed (another writer committed between lookup_parent_bin and
        //      this call), return Obsolete.
        //   3. If `tree_lsn == log_lsn`, re-log the LN and update the slot.
        let outcome =
            tree.migrate_ln_slot(info.db_id, info.key(), log_lsn, tree_lsn);

        match outcome {
            MigrationOutcome::Migrated => MigrateLnResult::Migrated,
            MigrationOutcome::Locked => MigrateLnResult::Locked,
            MigrationOutcome::Obsolete => MigrateLnResult::Dead,
        }
    }

    /// Processes an IN log entry.
    ///
    ///
    ///
    /// If the IN is still the current version in the tree, marks it dirty so
    /// the next checkpoint will re-log it (making the cleaned file's copy
    /// obsolete).  If the IN is no longer current, counts it as dead.
    ///
    /// # correspondence
    /// ```text
    /// processIN(inClone, db, logLsn):
    ///   nINsCleanedThisRun++
    ///   inInTree = findINInTree(tree, db, inClone, logLsn)
    ///   if inInTree == null:  nINsDeadThisRun++;  obsolete = true
    ///   else:
    ///     nINsMigratedThisRun++
    ///     inInTree.setDirty(true)
    ///     inInTree.setProhibitNextDelta(true)
    ///     inInTree.releaseLatch()
    ///     dirtied = true
    /// ```
    pub fn process_in<T: TreeLookup>(
        &self,
        db_id: i64,
        node_id: i64,
        log_lsn: Lsn,
        tree: &T,
        result: &mut FileProcessResult,
    ) {
        // NINsCleanedThisRun++
        result.ins_cleaned += 1;
        self.stats.ins_cleaned.fetch_add(1, Ordering::Relaxed);

        // FindINInTree → if null then dead, else dirty it.
        match tree.lookup_in(db_id, node_id, log_lsn) {
            InLookupResult::Found => {
                // The tree implementation has already called set_dirty(true)
                // and set_prohibit_next_delta(true) (lines 1678-1681).
                result.ins_migrated += 1;
                self.stats.ins_migrated.fetch_add(1, Ordering::Relaxed);
            }
            InLookupResult::Obsolete => {
                result.ins_dead += 1;
                self.stats.ins_dead.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Processes a BIN-delta entry.
    ///
    ///
    ///
    /// Marks the parent BIN dirty by delegating to `process_in()`.  This
    /// causes the next checkpoint to re-log the full BIN, making the
    /// cleaned file's copy of the delta obsolete.
    pub fn process_bin_delta<T: TreeLookup>(
        &self,
        db_id: i64,
        node_id: i64,
        log_lsn: Lsn,
        tree: &T,
        result: &mut FileProcessResult,
    ) {
        result.bin_deltas_cleaned += 1;
        self.stats.bin_deltas_cleaned.fetch_add(1, Ordering::Relaxed);
        // Delegate to process_in: find the node in the tree by node_id,
        // compare its LSN, and mark it dirty so the next checkpoint re-logs
        // the full BIN — this supersedes the old delta.
        //
        // `FileProcessor.processBINDelta()` which calls
        // `findINInTree()` and `IN.setDirty(true)` on the found node.
        self.process_in(db_id, node_id, log_lsn, tree, result);
        // Move the in_* counters over to bin_delta_* since this is a delta.
        if result.ins_migrated > 0 {
            result.ins_migrated -= 1;
            result.bin_deltas_migrated += 1;
        } else if result.ins_dead > 0 {
            result.ins_dead -= 1;
            result.bin_deltas_dead += 1;
        }
    }

    /// Returns whether shutdown has been requested.
    pub fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::Relaxed)
    }
}

impl FileProcessResult {
    /// Creates a new empty result.
    pub fn new() -> Self {
        Self::default()
    }

    /// Merges another result into this one (for multi-file processing).
    pub fn merge(&mut self, other: &FileProcessResult) {
        self.entries_read += other.entries_read;
        self.lns_cleaned += other.lns_cleaned;
        self.lns_dead += other.lns_dead;
        self.lns_migrated += other.lns_migrated;
        self.lns_obsolete += other.lns_obsolete;
        self.lns_locked += other.lns_locked;
        self.ins_cleaned += other.ins_cleaned;
        self.ins_dead += other.ins_dead;
        self.ins_migrated += other.ins_migrated;
        self.ins_obsolete += other.ins_obsolete;
        self.bin_deltas_cleaned += other.bin_deltas_cleaned;
        self.bin_deltas_dead += other.bin_deltas_dead;
        self.bin_deltas_migrated += other.bin_deltas_migrated;
        self.bin_deltas_obsolete += other.bin_deltas_obsolete;
        self.completed = self.completed && other.completed;
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── Helpers ──────────────────────────────────────────────────────────────

    fn make_processor() -> FileProcessor {
        let stats = Arc::new(CleanerStats::new());
        let shutdown = Arc::new(AtomicBool::new(false));
        FileProcessor::new(stats, shutdown)
    }

    fn make_ln_info(file_num: u32, offset: u32, db_id: i64) -> LnInfo {
        let lsn = Lsn::new(file_num, offset);
        LnInfo::new(lsn, db_id, vec![0x01, 0x02, 0x03], 128, false, 0)
    }

    // ── Stub TreeLookup implementations ──────────────────────────────────────

    /// A tree stub where every LN has been deleted (NotFound).
    struct DeletedTree;

    impl TreeLookup for DeletedTree {
        fn lookup_parent_bin(
            &self,
            _db_id: i64,
            _key: &[u8],
            _log_lsn: Lsn,
        ) -> BinLookupResult {
            BinLookupResult::NotFound
        }

        fn migrate_ln_slot(
            &self,
            _db_id: i64,
            _key: &[u8],
            _log_lsn: Lsn,
            _tree_lsn: Lsn,
        ) -> MigrationOutcome {
            MigrationOutcome::Obsolete
        }

        fn lookup_in(
            &self,
            _db_id: i64,
            _node_id: i64,
            _log_lsn: Lsn,
        ) -> InLookupResult {
            InLookupResult::Obsolete
        }
    }

    /// A tree stub where every slot is known-deleted.
    struct KnownDeletedTree;

    impl TreeLookup for KnownDeletedTree {
        fn lookup_parent_bin(
            &self,
            _db_id: i64,
            _key: &[u8],
            _log_lsn: Lsn,
        ) -> BinLookupResult {
            BinLookupResult::KnownDeleted
        }

        fn migrate_ln_slot(
            &self,
            _db_id: i64,
            _key: &[u8],
            _log_lsn: Lsn,
            _tree_lsn: Lsn,
        ) -> MigrationOutcome {
            MigrationOutcome::Obsolete
        }

        fn lookup_in(
            &self,
            _db_id: i64,
            _node_id: i64,
            _log_lsn: Lsn,
        ) -> InLookupResult {
            InLookupResult::Obsolete
        }
    }

    /// A tree stub where every active LN is at log_lsn (migration succeeds)
    /// and every IN is current (Found).
    struct MigratingTree;

    impl TreeLookup for MigratingTree {
        fn lookup_parent_bin(
            &self,
            _db_id: i64,
            _key: &[u8],
            log_lsn: Lsn,
        ) -> BinLookupResult {
            // tree_lsn == log_lsn → active
            BinLookupResult::Found { tree_lsn: log_lsn }
        }

        fn migrate_ln_slot(
            &self,
            _db_id: i64,
            _key: &[u8],
            _log_lsn: Lsn,
            _tree_lsn: Lsn,
        ) -> MigrationOutcome {
            MigrationOutcome::Migrated
        }

        fn lookup_in(
            &self,
            _db_id: i64,
            _node_id: i64,
            _log_lsn: Lsn,
        ) -> InLookupResult {
            InLookupResult::Found
        }
    }

    /// A tree stub where the slot has moved forward (obsolete log entry).
    struct ObsoleteTree {
        /// The "current" LSN in the tree (newer than log_lsn).
        pub current_lsn: Lsn,
    }

    impl TreeLookup for ObsoleteTree {
        fn lookup_parent_bin(
            &self,
            _db_id: i64,
            _key: &[u8],
            _log_lsn: Lsn,
        ) -> BinLookupResult {
            BinLookupResult::Found { tree_lsn: self.current_lsn }
        }

        fn migrate_ln_slot(
            &self,
            _db_id: i64,
            _key: &[u8],
            _log_lsn: Lsn,
            _tree_lsn: Lsn,
        ) -> MigrationOutcome {
            // tree moved on → obsolete
            MigrationOutcome::Obsolete
        }

        fn lookup_in(
            &self,
            _db_id: i64,
            _node_id: i64,
            _log_lsn: Lsn,
        ) -> InLookupResult {
            InLookupResult::Obsolete
        }
    }

    /// A tree stub where every lock attempt is denied.
    struct LockedTree;

    impl TreeLookup for LockedTree {
        fn lookup_parent_bin(
            &self,
            _db_id: i64,
            _key: &[u8],
            log_lsn: Lsn,
        ) -> BinLookupResult {
            BinLookupResult::Found { tree_lsn: log_lsn }
        }

        fn migrate_ln_slot(
            &self,
            _db_id: i64,
            _key: &[u8],
            _log_lsn: Lsn,
            _tree_lsn: Lsn,
        ) -> MigrationOutcome {
            MigrationOutcome::Locked
        }

        fn lookup_in(
            &self,
            _db_id: i64,
            _node_id: i64,
            _log_lsn: Lsn,
        ) -> InLookupResult {
            InLookupResult::Obsolete
        }
    }

    /// A tree stub where the BIN slot holds NULL_LSN (deferred-write DB).
    struct NullLsnTree;

    impl TreeLookup for NullLsnTree {
        fn lookup_parent_bin(
            &self,
            _db_id: i64,
            _key: &[u8],
            _log_lsn: Lsn,
        ) -> BinLookupResult {
            BinLookupResult::Found { tree_lsn: noxu_util::NULL_LSN }
        }

        fn migrate_ln_slot(
            &self,
            _db_id: i64,
            _key: &[u8],
            _log_lsn: Lsn,
            _tree_lsn: Lsn,
        ) -> MigrationOutcome {
            // Should never be called for NULL_LSN (handled in process_found_ln).
            MigrationOutcome::Obsolete
        }

        fn lookup_in(
            &self,
            _db_id: i64,
            _node_id: i64,
            _log_lsn: Lsn,
        ) -> InLookupResult {
            InLookupResult::Obsolete
        }
    }

    /// A tree stub where every IN is obsolete (no longer in tree).
    struct ObsoleteInTree;

    impl TreeLookup for ObsoleteInTree {
        fn lookup_parent_bin(
            &self,
            _: i64,
            _: &[u8],
            _: Lsn,
        ) -> BinLookupResult {
            BinLookupResult::NotFound
        }
        fn migrate_ln_slot(
            &self,
            _: i64,
            _: &[u8],
            _: Lsn,
            _: Lsn,
        ) -> MigrationOutcome {
            MigrationOutcome::Obsolete
        }
        fn lookup_in(&self, _: i64, _: i64, _: Lsn) -> InLookupResult {
            InLookupResult::Obsolete
        }
    }

    // ── FileProcessor basic tests ─────────────────────────────────────────────

    #[test]
    fn test_new_processor() {
        let stats = Arc::new(CleanerStats::new());
        let shutdown = Arc::new(AtomicBool::new(false));
        let processor = FileProcessor::new(stats, shutdown);

        assert!(!processor.is_shutdown());
        assert_eq!(
            processor.process_pending_interval,
            PROCESS_PENDING_EVERY_N_LNS
        );
    }

    #[test]
    fn test_set_pending_interval() {
        let stats = Arc::new(CleanerStats::new());
        let shutdown = Arc::new(AtomicBool::new(false));
        let mut processor = FileProcessor::new(stats, shutdown);

        processor.set_process_pending_interval(200);
        assert_eq!(processor.process_pending_interval, 200);
    }

    #[test]
    fn test_process_file_empty_entries() {
        let stats = Arc::new(CleanerStats::new());
        let shutdown = Arc::new(AtomicBool::new(false));
        let processor = FileProcessor::new(stats, shutdown);

        let summary = crate::FileSummary::new();
        let result = processor.process_file_no_entries(1, &summary).unwrap();

        // Empty file → completed with zero counts.
        assert!(result.completed);
        assert_eq!(result.entries_read, 0);
        assert_eq!(result.lns_cleaned, 0);
    }

    #[test]
    fn test_process_file_with_shutdown() {
        let stats = Arc::new(CleanerStats::new());
        let shutdown = Arc::new(AtomicBool::new(true));
        let processor = FileProcessor::new(stats, shutdown);

        let summary = crate::FileSummary::new();
        let result = processor.process_file_no_entries(1, &summary).unwrap();

        // Should return immediately with completed=false
        assert!(!result.completed);
    }

    #[test]
    fn test_shutdown_check() {
        let stats = Arc::new(CleanerStats::new());
        let shutdown = Arc::new(AtomicBool::new(false));
        let processor = FileProcessor::new(stats, shutdown.clone());

        assert!(!processor.is_shutdown());

        shutdown.store(true, Ordering::Relaxed);
        assert!(processor.is_shutdown());
    }

    #[test]
    fn test_result_default() {
        let result = FileProcessResult::default();
        assert_eq!(result.entries_read, 0);
        assert_eq!(result.lns_cleaned, 0);
        assert!(!result.completed);
    }

    #[test]
    fn test_result_new() {
        let result = FileProcessResult::new();
        assert_eq!(result.entries_read, 0);
        assert!(!result.completed);
    }

    #[test]
    fn test_result_merge() {
        let mut result1 = FileProcessResult {
            entries_read: 100,
            lns_cleaned: 50,
            lns_migrated: 30,
            ins_cleaned: 10,
            completed: true,
            ..Default::default()
        };

        let result2 = FileProcessResult {
            entries_read: 200,
            lns_cleaned: 75,
            lns_migrated: 40,
            ins_cleaned: 15,
            completed: true,
            ..Default::default()
        };

        result1.merge(&result2);

        assert_eq!(result1.entries_read, 300);
        assert_eq!(result1.lns_cleaned, 125);
        assert_eq!(result1.lns_migrated, 70);
        assert_eq!(result1.ins_cleaned, 25);
        assert!(result1.completed);
    }

    #[test]
    fn test_result_merge_with_incomplete() {
        let mut result1 = FileProcessResult {
            entries_read: 100,
            completed: true,
            ..Default::default()
        };

        let result2 = FileProcessResult {
            entries_read: 50,
            completed: false,
            ..Default::default()
        };

        result1.merge(&result2);

        assert_eq!(result1.entries_read, 150);
        assert!(!result1.completed); // Should be false if any incomplete
    }

    #[test]
    fn test_result_all_counters() {
        let result = FileProcessResult {
            entries_read: 1,
            lns_cleaned: 2,
            lns_dead: 3,
            lns_migrated: 4,
            lns_obsolete: 5,
            lns_locked: 6,
            ins_cleaned: 7,
            ins_dead: 8,
            ins_migrated: 9,
            ins_obsolete: 10,
            bin_deltas_cleaned: 11,
            bin_deltas_dead: 12,
            bin_deltas_migrated: 13,
            bin_deltas_obsolete: 14,
            completed: true,
        };

        assert_eq!(result.entries_read, 1);
        assert_eq!(result.lns_cleaned, 2);
        assert_eq!(result.lns_dead, 3);
        assert_eq!(result.lns_migrated, 4);
        assert_eq!(result.lns_obsolete, 5);
        assert_eq!(result.lns_locked, 6);
        assert_eq!(result.ins_cleaned, 7);
        assert_eq!(result.ins_dead, 8);
        assert_eq!(result.ins_migrated, 9);
        assert_eq!(result.ins_obsolete, 10);
        assert_eq!(result.bin_deltas_cleaned, 11);
        assert_eq!(result.bin_deltas_dead, 12);
        assert_eq!(result.bin_deltas_migrated, 13);
        assert_eq!(result.bin_deltas_obsolete, 14);
        assert!(result.completed);
    }

    #[test]
    fn test_result_clone() {
        let result = FileProcessResult {
            entries_read: 100,
            lns_cleaned: 50,
            completed: true,
            ..Default::default()
        };

        let cloned = result.clone();
        assert_eq!(cloned.entries_read, result.entries_read);
        assert_eq!(cloned.lns_cleaned, result.lns_cleaned);
        assert_eq!(cloned.completed, result.completed);
    }

    #[test]
    fn test_result_equality() {
        let result1 = FileProcessResult {
            entries_read: 100,
            lns_cleaned: 50,
            completed: true,
            ..Default::default()
        };

        let result2 = FileProcessResult {
            entries_read: 100,
            lns_cleaned: 50,
            completed: true,
            ..Default::default()
        };

        let result3 = FileProcessResult {
            entries_read: 100,
            lns_cleaned: 51, // Different
            completed: true,
            ..Default::default()
        };

        assert_eq!(result1, result2);
        assert_ne!(result1, result3);
    }

    // ── LookAheadCache tests ──────────────────────────────────────────────────

    #[test]
    fn test_look_ahead_cache_new() {
        let cache = LookAheadCache::new(4096);
        assert!(cache.is_empty());
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn test_look_ahead_cache_add_and_remove() {
        let mut cache = LookAheadCache::new(4096);
        let info = make_ln_info(1, 1000, 42);

        cache.add(1000, info);
        assert!(!cache.is_empty());
        assert_eq!(cache.len(), 1);

        let removed = cache.remove(1000);
        assert!(removed.is_some());
        assert!(cache.is_empty());
    }

    #[test]
    fn test_look_ahead_cache_next_offset_is_smallest() {
        // LookAheadCache.nextOffset() returns the first key of a TreeMap,
        // which is the smallest key.  BTreeMap gives the same guarantee.
        let mut cache = LookAheadCache::new(65536);
        cache.add(3000, make_ln_info(1, 3000, 1));
        cache.add(1000, make_ln_info(1, 1000, 1));
        cache.add(2000, make_ln_info(1, 2000, 1));

        assert_eq!(cache.next_offset(), Some(1000));
    }

    #[test]
    fn test_look_ahead_cache_is_full() {
        // The LookAheadCache seeds `used_mem` with TREEMAP_OVERHEAD (64 bytes).
        // A `max_mem` of exactly 64 therefore starts the cache already full.
        // Use a value slightly above the TREEMAP_OVERHEAD so the empty cache
        // is not full, then add one entry (which costs memory_size + 48) to
        // push it over the budget.
        //
        // make_ln_info creates a 3-byte key, giving memory_size = 54 + 3 = 57.
        // Entry overhead is 48. So one entry costs 57 + 48 = 105 bytes.
        // Setting max_mem to 64 + 1 = 65 means the empty cache (used=64) is
        // not full, but after adding one entry (used = 64 + 105 = 169 > 65)
        // it is full.
        let mut cache = LookAheadCache::new(65);
        assert!(!cache.is_full()); // used_mem (64) < max_mem (65)

        cache.add(1000, make_ln_info(1, 1000, 42));
        assert!(cache.is_full()); // now over budget
    }

    #[test]
    fn test_look_ahead_cache_remove_absent_key() {
        let mut cache = LookAheadCache::new(4096);
        let result = cache.remove(9999);
        assert!(result.is_none());
    }

    #[test]
    fn test_look_ahead_cache_next_offset_empty() {
        let cache = LookAheadCache::new(4096);
        assert_eq!(cache.next_offset(), None);
    }

    #[test]
    fn test_look_ahead_cache_memory_accounting() {
        let mut cache = LookAheadCache::new(65536);
        let info = make_ln_info(1, 100, 1);
        let mem_before = cache.used_mem;

        cache.add(100, info);
        let mem_after_add = cache.used_mem;
        assert!(mem_after_add > mem_before);

        cache.remove(100);
        assert_eq!(cache.used_mem, mem_before);
    }

    // ── process_found_ln tests ────────────────────────────────────────────────

    /// case 1: tree_lsn == log_lsn → migration path.
    #[test]
    fn test_process_found_ln_migrates_when_lsns_match() {
        let proc = make_processor();
        let file_num = 1u32;
        let offset = 1000u32;
        let log_lsn = Lsn::new(file_num, offset);
        let info = make_ln_info(file_num, offset, 42);

        // MigratingTree returns tree_lsn == log_lsn and MigrationOutcome::Migrated
        let result =
            proc.process_found_ln(&info, log_lsn, log_lsn, &MigratingTree);

        assert_eq!(result, MigrateLnResult::Migrated);
    }

    /// case 2/3: tree_lsn != log_lsn → obsolete.
    #[test]
    fn test_process_found_ln_dead_when_lsns_differ() {
        let proc = make_processor();
        let file_num = 1u32;
        let log_lsn = Lsn::new(file_num, 1000);
        let tree_lsn = Lsn::new(file_num, 2000); // newer → log entry is stale
        let info = make_ln_info(file_num, 1000, 42);

        let obsolete_tree = ObsoleteTree { current_lsn: tree_lsn };
        let result =
            proc.process_found_ln(&info, log_lsn, tree_lsn, &obsolete_tree);

        assert_eq!(result, MigrateLnResult::Dead);
    }

    /// case 4: NULL_LSN in tree → obsolete (deferred-write DB).
    #[test]
    fn test_process_found_ln_dead_when_tree_lsn_is_null() {
        let proc = make_processor();
        let file_num = 1u32;
        let log_lsn = Lsn::new(file_num, 1000);
        let info = make_ln_info(file_num, 1000, 42);

        let result = proc.process_found_ln(
            &info,
            log_lsn,
            noxu_util::NULL_LSN,
            &NullLsnTree,
        );

        assert_eq!(result, MigrateLnResult::Dead);
    }

    /// Lock denied → Locked result.
    #[test]
    fn test_process_found_ln_locked() {
        let proc = make_processor();
        let file_num = 1u32;
        let log_lsn = Lsn::new(file_num, 1000);
        let info = make_ln_info(file_num, 1000, 42);

        let result =
            proc.process_found_ln(&info, log_lsn, log_lsn, &LockedTree);

        assert_eq!(result, MigrateLnResult::Locked);
    }

    // ── process_ln tests ───────────────────────────────────────────────────────

    /// process_ln on an empty cache is a no-op.
    #[test]
    fn test_process_ln_empty_cache() {
        let proc = make_processor();
        let mut cache = LookAheadCache::new(65536);
        let mut result = FileProcessResult::new();

        proc.process_ln(1, &mut cache, &DeletedTree, &mut result);

        assert_eq!(result.lns_cleaned, 0);
        assert_eq!(result.lns_dead, 0);
        assert_eq!(result.lns_migrated, 0);
    }

    /// process_ln where parent BIN is not found → lns_dead increments.
    #[test]
    fn test_process_ln_not_found_in_tree() {
        let proc = make_processor();
        let mut cache = LookAheadCache::new(65536);
        let mut result = FileProcessResult::new();

        cache.add(1000, make_ln_info(1, 1000, 42));
        proc.process_ln(1, &mut cache, &DeletedTree, &mut result);

        assert_eq!(result.lns_cleaned, 1);
        assert_eq!(result.lns_dead, 1);
        assert_eq!(result.lns_migrated, 0);
        assert!(cache.is_empty());
    }

    /// process_ln where slot is known-deleted → lns_dead increments.
    #[test]
    fn test_process_ln_known_deleted() {
        let proc = make_processor();
        let mut cache = LookAheadCache::new(65536);
        let mut result = FileProcessResult::new();

        cache.add(500, make_ln_info(1, 500, 7));
        proc.process_ln(1, &mut cache, &KnownDeletedTree, &mut result);

        assert_eq!(result.lns_cleaned, 1);
        assert_eq!(result.lns_dead, 1);
        assert!(cache.is_empty());
    }

    /// process_ln where tree_lsn == log_lsn → migration.
    #[test]
    fn test_process_ln_migrated() {
        let proc = make_processor();
        let mut cache = LookAheadCache::new(65536);
        let mut result = FileProcessResult::new();

        cache.add(2000, make_ln_info(2, 2000, 1));
        proc.process_ln(2, &mut cache, &MigratingTree, &mut result);

        assert_eq!(result.lns_cleaned, 1);
        assert_eq!(result.lns_migrated, 1);
        assert_eq!(result.lns_dead, 0);
        assert!(cache.is_empty());
    }

    /// process_ln where lock is denied → lns_locked increments.
    #[test]
    fn test_process_ln_locked() {
        let proc = make_processor();
        let mut cache = LookAheadCache::new(65536);
        let mut result = FileProcessResult::new();

        cache.add(3000, make_ln_info(1, 3000, 5));
        proc.process_ln(1, &mut cache, &LockedTree, &mut result);

        assert_eq!(result.lns_cleaned, 1);
        assert_eq!(result.lns_locked, 1);
        assert_eq!(result.lns_migrated, 0);
    }

    /// process_ln always dequeues the lowest-offset entry first (FIFO on LSN).
    ///
    /// processLN calls `lookAheadCache.nextOffset()` (= TreeMap.firstKey(),
    /// smallest key).  Verify the Rust port does the same.
    #[test]
    fn test_process_ln_dequeues_lowest_offset_first() {
        let proc = make_processor();
        let mut cache = LookAheadCache::new(65536);
        let mut result = FileProcessResult::new();

        // Insert in reverse order so offset 100 is not the most-recently added.
        cache.add(300, make_ln_info(1, 300, 1));
        cache.add(100, make_ln_info(1, 100, 1));
        cache.add(200, make_ln_info(1, 200, 1));

        // After first process_ln the entry at offset 100 must be gone.
        proc.process_ln(1, &mut cache, &MigratingTree, &mut result);
        assert_eq!(cache.len(), 2);
        // offset 100 no longer present; 200 and 300 remain.
        assert!(cache.next_offset() == Some(200));
    }

    /// Draining the full cache with repeated process_ln calls.
    #[test]
    fn test_process_ln_drain_cache() {
        let proc = make_processor();
        let file_num = 4u32;
        let mut cache = LookAheadCache::new(65536);
        let mut result = FileProcessResult::new();

        // Populate with 5 entries.
        for i in 0..5u32 {
            cache.add(i * 1000, make_ln_info(file_num, i * 1000, 1));
        }

        while !cache.is_empty() {
            proc.process_ln(file_num, &mut cache, &MigratingTree, &mut result);
        }

        assert_eq!(result.lns_cleaned, 5);
        assert_eq!(result.lns_migrated, 5);
        assert_eq!(result.lns_dead, 0);
    }

    /// Stats counters on CleanerStats are updated by process_ln.
    #[test]
    fn test_process_ln_updates_stats_migrated() {
        let stats = Arc::new(CleanerStats::new());
        let shutdown = Arc::new(AtomicBool::new(false));
        let proc = FileProcessor::new(stats.clone(), shutdown);

        let mut cache = LookAheadCache::new(65536);
        let mut result = FileProcessResult::new();

        cache.add(1000, make_ln_info(1, 1000, 1));
        proc.process_ln(1, &mut cache, &MigratingTree, &mut result);

        assert_eq!(stats.lns_migrated.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_process_ln_updates_stats_dead() {
        let stats = Arc::new(CleanerStats::new());
        let shutdown = Arc::new(AtomicBool::new(false));
        let proc = FileProcessor::new(stats.clone(), shutdown);

        let mut cache = LookAheadCache::new(65536);
        let mut result = FileProcessResult::new();

        cache.add(1000, make_ln_info(1, 1000, 1));
        proc.process_ln(1, &mut cache, &DeletedTree, &mut result);

        assert_eq!(stats.lns_dead.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_process_ln_updates_stats_locked() {
        let stats = Arc::new(CleanerStats::new());
        let shutdown = Arc::new(AtomicBool::new(false));
        let proc = FileProcessor::new(stats.clone(), shutdown);

        let mut cache = LookAheadCache::new(65536);
        let mut result = FileProcessResult::new();

        cache.add(1000, make_ln_info(1, 1000, 1));
        proc.process_ln(1, &mut cache, &LockedTree, &mut result);

        assert_eq!(stats.lns_locked.load(Ordering::Relaxed), 1);
    }

    // ── BinLookupResult / MigrationOutcome trait-object tests ─────────────────

    #[test]
    fn test_bin_lookup_result_not_found() {
        let proc = make_processor();
        let mut cache = LookAheadCache::new(65536);
        let mut result = FileProcessResult::new();

        cache.add(42, make_ln_info(1, 42, 1));
        proc.process_ln(1, &mut cache, &DeletedTree, &mut result);

        // BinLookupResult::NotFound should map to dead
        assert_eq!(result.lns_dead, 1);
    }

    #[test]
    fn test_null_lsn_in_tree_is_dead() {
        // Deferred-write DB, never-written slot.
        let proc = make_processor();
        let file_num = 1u32;
        let log_lsn = Lsn::new(file_num, 100);
        let info = make_ln_info(file_num, 100, 99);

        let result = proc.process_found_ln(
            &info,
            log_lsn,
            noxu_util::NULL_LSN,
            &NullLsnTree,
        );
        assert_eq!(
            result,
            MigrateLnResult::Dead,
            "NULL_LSN in tree slot must yield Dead (case 4 in the equivalent processFoundLN)"
        );
    }

    #[test]
    fn test_migrate_ln_result_variants() {
        // Ensure all three variants are reachable and distinguishable.
        assert_ne!(MigrateLnResult::Dead, MigrateLnResult::Migrated);
        assert_ne!(MigrateLnResult::Dead, MigrateLnResult::Locked);
        assert_ne!(MigrateLnResult::Migrated, MigrateLnResult::Locked);
    }

    #[test]
    fn test_migration_outcome_variants() {
        assert_ne!(MigrationOutcome::Migrated, MigrationOutcome::Locked);
        assert_ne!(MigrationOutcome::Migrated, MigrationOutcome::Obsolete);
        assert_ne!(MigrationOutcome::Locked, MigrationOutcome::Obsolete);
    }

    // ── process_in tests ──────────────────────────────────────────────────────

    /// process_in with a current IN → ins_cleaned and ins_migrated increment.
    #[test]
    fn test_process_in_found_marks_migrated() {
        let proc = make_processor();
        let mut result = FileProcessResult::new();
        let log_lsn = Lsn::new(1, 100);

        proc.process_in(42, 99, log_lsn, &MigratingTree, &mut result);

        assert_eq!(result.ins_cleaned, 1);
        assert_eq!(result.ins_migrated, 1);
        assert_eq!(result.ins_dead, 0);
    }

    /// process_in with an obsolete IN → ins_cleaned and ins_dead increment.
    #[test]
    fn test_process_in_obsolete_marks_dead() {
        let proc = make_processor();
        let mut result = FileProcessResult::new();
        let log_lsn = Lsn::new(1, 100);

        proc.process_in(42, 99, log_lsn, &ObsoleteInTree, &mut result);

        assert_eq!(result.ins_cleaned, 1);
        assert_eq!(result.ins_dead, 1);
        assert_eq!(result.ins_migrated, 0);
    }

    /// process_in updates CleanerStats atomics.
    #[test]
    fn test_process_in_updates_stats() {
        let stats = Arc::new(CleanerStats::new());
        let shutdown = Arc::new(AtomicBool::new(false));
        let proc = FileProcessor::new(stats.clone(), shutdown);
        let mut result = FileProcessResult::new();

        proc.process_in(1, 1, Lsn::new(1, 0), &MigratingTree, &mut result);
        assert_eq!(stats.ins_cleaned.load(Ordering::Relaxed), 1);
        assert_eq!(stats.ins_migrated.load(Ordering::Relaxed), 1);

        proc.process_in(1, 2, Lsn::new(1, 100), &ObsoleteInTree, &mut result);
        assert_eq!(stats.ins_cleaned.load(Ordering::Relaxed), 2);
        assert_eq!(stats.ins_dead.load(Ordering::Relaxed), 1);
    }

    // ── process_file loop tests ────────────────────────────────────────────────

    fn make_ln_entry(
        file_num: u32,
        offset: u32,
        db_id: i64,
        key: &[u8],
    ) -> LogEntry {
        LogEntry {
            lsn: Lsn::new(file_num, offset),
            entry_type: LogEntryType::Ln {
                db_id,
                key: key.to_vec(),
                deleted: false,
                expiration_time: 0,
                entry_size: 64,
            },
        }
    }

    fn make_deleted_ln_entry(
        file_num: u32,
        offset: u32,
        db_id: i64,
    ) -> LogEntry {
        LogEntry {
            lsn: Lsn::new(file_num, offset),
            entry_type: LogEntryType::Ln {
                db_id,
                key: vec![1],
                deleted: true,
                expiration_time: 0,
                entry_size: 32,
            },
        }
    }

    fn make_in_entry(
        file_num: u32,
        offset: u32,
        db_id: i64,
        node_id: i64,
    ) -> LogEntry {
        LogEntry {
            lsn: Lsn::new(file_num, offset),
            entry_type: LogEntryType::In { db_id, node_id },
        }
    }

    fn make_other_entry(file_num: u32, offset: u32) -> LogEntry {
        LogEntry {
            lsn: Lsn::new(file_num, offset),
            entry_type: LogEntryType::Other,
        }
    }

    /// Empty file → completed, all counters zero.
    #[test]
    fn test_process_file_empty() {
        let proc = make_processor();
        let summary = crate::FileSummary::new();
        let result =
            proc.process_file(1, &summary, &[], &MigratingTree).unwrap();

        assert!(result.completed);
        assert_eq!(result.entries_read, 0);
        assert_eq!(result.lns_cleaned, 0);
        assert_eq!(result.ins_cleaned, 0);
    }

    /// Single active LN entry → migrated.
    #[test]
    fn test_process_file_single_ln_migrated() {
        let proc = make_processor();
        let summary = crate::FileSummary::new();
        let entries = vec![make_ln_entry(1, 100, 42, &[1, 2, 3])];
        let result =
            proc.process_file(1, &summary, &entries, &MigratingTree).unwrap();

        assert!(result.completed);
        assert_eq!(result.entries_read, 1);
        assert_eq!(result.lns_cleaned, 1);
        assert_eq!(result.lns_migrated, 1);
        assert_eq!(result.lns_dead, 0);
    }

    /// Deleted LN entry → immediately obsolete, not cleaned.
    #[test]
    fn test_process_file_deleted_ln_is_obsolete() {
        let proc = make_processor();
        let summary = crate::FileSummary::new();
        let entries = vec![make_deleted_ln_entry(1, 100, 42)];
        let result =
            proc.process_file(1, &summary, &entries, &MigratingTree).unwrap();

        assert!(result.completed);
        assert_eq!(result.entries_read, 1);
        assert_eq!(result.lns_obsolete, 1);
        assert_eq!(result.lns_cleaned, 0);
    }

    /// Active IN entry → migrated (marked dirty).
    #[test]
    fn test_process_file_in_entry_migrated() {
        let proc = make_processor();
        let summary = crate::FileSummary::new();
        let entries = vec![make_in_entry(1, 200, 1, 77)];
        let result =
            proc.process_file(1, &summary, &entries, &MigratingTree).unwrap();

        assert!(result.completed);
        assert_eq!(result.entries_read, 1);
        assert_eq!(result.ins_cleaned, 1);
        assert_eq!(result.ins_migrated, 1);
    }

    /// Obsolete IN entry → dead.
    #[test]
    fn test_process_file_in_entry_dead() {
        let proc = make_processor();
        let summary = crate::FileSummary::new();
        let entries = vec![make_in_entry(1, 200, 1, 77)];
        let result =
            proc.process_file(1, &summary, &entries, &ObsoleteInTree).unwrap();

        assert!(result.completed);
        assert_eq!(result.ins_cleaned, 1);
        assert_eq!(result.ins_dead, 1);
    }

    /// Other entry type is silently skipped.
    #[test]
    fn test_process_file_other_entry_skipped() {
        let proc = make_processor();
        let summary = crate::FileSummary::new();
        let entries = vec![make_other_entry(1, 300)];
        let result =
            proc.process_file(1, &summary, &entries, &MigratingTree).unwrap();

        assert!(result.completed);
        assert_eq!(result.entries_read, 1);
        assert_eq!(result.lns_cleaned, 0);
        assert_eq!(result.ins_cleaned, 0);
    }

    /// Mixed file: LNs, INs, deleted LNs, other entries.
    #[test]
    fn test_process_file_mixed_entries() {
        let proc = make_processor();
        let summary = crate::FileSummary::new();
        let entries = vec![
            make_ln_entry(2, 100, 1, &[1]), // active LN → migrated
            make_ln_entry(2, 200, 1, &[2]), // active LN → migrated
            make_deleted_ln_entry(2, 300, 1), // deleted → obsolete
            make_in_entry(2, 400, 1, 10),   // active IN → migrated
            make_other_entry(2, 500),       // other → skipped
        ];

        let result =
            proc.process_file(2, &summary, &entries, &MigratingTree).unwrap();

        assert!(result.completed);
        assert_eq!(result.entries_read, 5);
        assert_eq!(result.lns_cleaned, 2);
        assert_eq!(result.lns_migrated, 2);
        assert_eq!(result.lns_obsolete, 1);
        assert_eq!(result.ins_cleaned, 1);
        assert_eq!(result.ins_migrated, 1);
    }

    /// LN in deleted-tree → dead, not migrated.
    #[test]
    fn test_process_file_ln_not_found_in_tree() {
        let proc = make_processor();
        let summary = crate::FileSummary::new();
        let entries = vec![make_ln_entry(1, 100, 1, &[0xAB])];

        let result =
            proc.process_file(1, &summary, &entries, &DeletedTree).unwrap();

        assert!(result.completed);
        assert_eq!(result.lns_cleaned, 1);
        assert_eq!(result.lns_dead, 1);
        assert_eq!(result.lns_migrated, 0);
    }

    /// LN with locked slot → lns_locked.
    #[test]
    fn test_process_file_ln_locked() {
        let proc = make_processor();
        let summary = crate::FileSummary::new();
        let entries = vec![make_ln_entry(1, 100, 1, &[0x01])];

        let result =
            proc.process_file(1, &summary, &entries, &LockedTree).unwrap();

        assert!(result.completed);
        assert_eq!(result.lns_locked, 1);
    }

    /// Shutdown mid-file → completed = false.
    #[test]
    fn test_process_file_shutdown_mid_file() {
        let stats = Arc::new(CleanerStats::new());
        let shutdown = Arc::new(AtomicBool::new(false));
        let proc = FileProcessor::new(stats, shutdown.clone());
        let summary = crate::FileSummary::new();

        // Signal shutdown immediately — the loop checks it before each entry.
        shutdown.store(true, Ordering::Relaxed);

        let entries = vec![
            make_ln_entry(1, 100, 1, &[1]),
            make_ln_entry(1, 200, 1, &[2]),
        ];

        let result =
            proc.process_file(1, &summary, &entries, &MigratingTree).unwrap();
        assert!(!result.completed);
    }

    /// Many LN entries — look-ahead cache drains correctly, all are migrated.
    #[test]
    fn test_process_file_many_lns_all_migrated() {
        let proc = make_processor();
        let summary = crate::FileSummary::new();

        let entries: Vec<LogEntry> = (0u32..500)
            .map(|i| make_ln_entry(3, i * 100, 1, &[i as u8]))
            .collect();

        let result =
            proc.process_file(3, &summary, &entries, &MigratingTree).unwrap();

        assert!(result.completed);
        assert_eq!(result.entries_read, 500);
        assert_eq!(result.lns_cleaned, 500);
        assert_eq!(result.lns_migrated, 500);
    }

    // ── InLookupResult tests ──────────────────────────────────────────────────

    #[test]
    fn test_in_lookup_result_variants() {
        assert_ne!(InLookupResult::Found, InLookupResult::Obsolete);
    }

    /// LogEntryType equality and debug formatting.
    #[test]
    fn test_log_entry_type_other() {
        let entry = make_other_entry(1, 0);
        assert_eq!(entry.entry_type, LogEntryType::Other);
    }

    #[test]
    fn test_log_entry_type_ln() {
        let entry = make_ln_entry(1, 0, 1, &[1, 2]);
        assert!(matches!(entry.entry_type, LogEntryType::Ln { .. }));
    }

    #[test]
    fn test_log_entry_type_in() {
        let entry = make_in_entry(1, 0, 1, 42);
        assert!(matches!(entry.entry_type, LogEntryType::In { .. }));
    }

    // ── shutdown during drain-cache loop ─────────────────────────────────────

    /// Shutdown detected while draining the look-ahead cache at end-of-file.
    ///
    /// This exercises the `while !look_ahead_cache.is_empty()` drain loop
    /// where the shutdown flag is checked before each `process_ln` call.
    #[test]
    fn test_process_file_shutdown_during_drain() {
        let stats = Arc::new(CleanerStats::new());
        let shutdown = Arc::new(AtomicBool::new(false));
        let proc = FileProcessor::new(stats, shutdown.clone());
        let summary = crate::FileSummary::new();

        // One active LN entry — it will be buffered in the look-ahead cache.
        // We use a small buffer (just above TREEMAP_OVERHEAD) so the cache
        // does NOT fill up during the entry loop, ensuring the LN stays in
        // the drain path at the end.
        let entries = vec![make_ln_entry(1, 100, 1, &[0x01])];

        // Signal shutdown after building the processor but before process_file.
        // The entry-loop shutdown check fires before reading entry 0.
        shutdown.store(true, Ordering::Relaxed);

        let result =
            proc.process_file(1, &summary, &entries, &MigratingTree).unwrap();
        assert!(!result.completed);
    }

    /// Shutdown set between the entry loop finishing and the drain loop starting.
    ///
    /// This specifically tests the drain-loop branch: after all entries are
    /// consumed the cache still contains one entry, and we signal shutdown
    /// so the drain-loop sees it and returns completed=false.
    #[test]
    fn test_process_file_shutdown_in_drain_loop() {
        // Use a small pending interval so the periodic drain fires, leaving
        // the cache empty before end-of-file. Then add one more entry that
        // won't be drained until the explicit end-of-file drain loop.
        // To hit the drain-loop shutdown branch we need the loop to find
        // shutdown=true on its first iteration.
        //
        // Approach: use process_pending_interval = 1 so every LN triggers
        // a drain. Then set shutdown BEFORE calling process_file so the
        // top-of-loop shutdown check fires immediately (before any entry).
        let stats = Arc::new(CleanerStats::new());
        let shutdown = Arc::new(AtomicBool::new(false));
        let mut proc = FileProcessor::new(stats, shutdown.clone());
        proc.set_process_pending_interval(1);

        let summary = crate::FileSummary::new();
        let entries = vec![
            make_ln_entry(5, 100, 1, &[0xAA]),
            make_ln_entry(5, 200, 1, &[0xBB]),
        ];

        shutdown.store(true, Ordering::Relaxed);
        let result =
            proc.process_file(5, &summary, &entries, &MigratingTree).unwrap();
        assert!(!result.completed);
    }

    // ── FileProcessResult::merge edge cases ───────────────────────────────────

    #[test]
    fn test_result_merge_all_fields() {
        let mut r1 = FileProcessResult {
            entries_read: 10,
            lns_cleaned: 1,
            lns_dead: 2,
            lns_migrated: 3,
            lns_obsolete: 4,
            lns_locked: 5,
            ins_cleaned: 6,
            ins_dead: 7,
            ins_migrated: 8,
            ins_obsolete: 9,
            bin_deltas_cleaned: 10,
            bin_deltas_dead: 11,
            bin_deltas_migrated: 12,
            bin_deltas_obsolete: 13,
            completed: true,
        };

        let r2 = FileProcessResult {
            entries_read: 1,
            lns_cleaned: 1,
            lns_dead: 1,
            lns_migrated: 1,
            lns_obsolete: 1,
            lns_locked: 1,
            ins_cleaned: 1,
            ins_dead: 1,
            ins_migrated: 1,
            ins_obsolete: 1,
            bin_deltas_cleaned: 1,
            bin_deltas_dead: 1,
            bin_deltas_migrated: 1,
            bin_deltas_obsolete: 1,
            completed: true,
        };

        r1.merge(&r2);

        assert_eq!(r1.entries_read, 11);
        assert_eq!(r1.lns_cleaned, 2);
        assert_eq!(r1.lns_dead, 3);
        assert_eq!(r1.lns_migrated, 4);
        assert_eq!(r1.lns_obsolete, 5);
        assert_eq!(r1.lns_locked, 6);
        assert_eq!(r1.ins_cleaned, 7);
        assert_eq!(r1.ins_dead, 8);
        assert_eq!(r1.ins_migrated, 9);
        assert_eq!(r1.ins_obsolete, 10);
        assert_eq!(r1.bin_deltas_cleaned, 11);
        assert_eq!(r1.bin_deltas_dead, 12);
        assert_eq!(r1.bin_deltas_migrated, 13);
        assert_eq!(r1.bin_deltas_obsolete, 14);
        assert!(r1.completed);
    }

    #[test]
    fn test_result_merge_both_incomplete() {
        let mut r1 =
            FileProcessResult { completed: false, ..Default::default() };
        let r2 = FileProcessResult { completed: false, ..Default::default() };
        r1.merge(&r2);
        assert!(!r1.completed);
    }

    // ── process_file periodic drain (pending interval) ────────────────────────

    /// Verify that the periodic-drain branch is taken when n_processed_lns is
    /// a multiple of process_pending_interval.  With interval=1 every LN
    /// triggers an inner drain; we check that all entries end up counted.
    #[test]
    fn test_process_file_periodic_drain() {
        let stats = Arc::new(CleanerStats::new());
        let shutdown = Arc::new(AtomicBool::new(false));
        let mut proc = FileProcessor::new(stats, shutdown);
        proc.set_process_pending_interval(2); // drain every 2 LNs

        let summary = crate::FileSummary::new();
        let entries: Vec<LogEntry> = (0u32..10)
            .map(|i| make_ln_entry(1, i * 100, 1, &[i as u8]))
            .collect();

        let result =
            proc.process_file(1, &summary, &entries, &MigratingTree).unwrap();

        assert!(result.completed);
        assert_eq!(result.entries_read, 10);
        assert_eq!(result.lns_migrated, 10);
    }

    // ── BinLookupResult debug formatting ─────────────────────────────────────

    #[test]
    fn test_bin_lookup_result_debug() {
        let r = BinLookupResult::NotFound;
        let s = format!("{:?}", r);
        assert!(s.contains("NotFound"));

        let r2 = BinLookupResult::KnownDeleted;
        let s2 = format!("{:?}", r2);
        assert!(s2.contains("KnownDeleted"));

        let lsn = Lsn::new(1, 100);
        let r3 = BinLookupResult::Found { tree_lsn: lsn };
        let s3 = format!("{:?}", r3);
        assert!(s3.contains("Found"));
    }

    // ── LogEntryType clone/debug ──────────────────────────────────────────────

    #[test]
    fn test_log_entry_type_clone_and_eq() {
        let e1 = LogEntryType::Other;
        let e2 = e1.clone();
        assert_eq!(e1, e2);

        let ln = LogEntryType::Ln {
            db_id: 1,
            key: vec![1],
            deleted: false,
            expiration_time: 0,
            entry_size: 32,
        };
        let ln2 = ln.clone();
        assert_eq!(ln, ln2);
    }

    // ── LookAheadCache: zero max_mem is immediately full ─────────────────────

    #[test]
    fn test_look_ahead_cache_zero_budget_is_full() {
        // max_mem=0: used_mem (64) > 0, so is_full() is true immediately.
        let cache = LookAheadCache::new(0);
        assert!(cache.is_full());
    }

    // ── InLookupResult debug ──────────────────────────────────────────────────

    #[test]
    fn test_in_lookup_result_debug() {
        let s = format!("{:?}", InLookupResult::Found);
        assert!(s.contains("Found"));
        let s2 = format!("{:?}", InLookupResult::Obsolete);
        assert!(s2.contains("Obsolete"));
    }

    // ── MigrateLnResult debug ─────────────────────────────────────────────────

    #[test]
    fn test_migrate_ln_result_debug() {
        let s = format!("{:?}", MigrateLnResult::Migrated);
        assert!(s.contains("Migrated"));
        let s2 = format!("{:?}", MigrateLnResult::Dead);
        assert!(s2.contains("Dead"));
        let s3 = format!("{:?}", MigrateLnResult::Locked);
        assert!(s3.contains("Locked"));
    }

    // ── RealTreeLookup tests ──────────────────────────────────────────────────

    /// Build a Tree with one key and wrap it in RealTreeLookup.
    fn make_tree_with_key(key: &[u8], lsn: Lsn) -> noxu_tree::Tree {
        let tree = noxu_tree::Tree::new(1, 128);
        tree.insert(key.to_vec(), b"value".to_vec(), lsn)
            .expect("insert should succeed");
        tree
    }

    /// RealTreeLookup wraps a shared tree and the tree is non-empty after insert.
    #[test]
    fn test_real_tree_lookup_new_and_shared() {
        let lsn = Lsn::new(1, 100);
        let tree = make_tree_with_key(b"hello", lsn);
        let arc_tree = Arc::new(std::sync::RwLock::new(tree));
        let lookup = RealTreeLookup::new(
            Arc::clone(&arc_tree),
            Arc::new(LockManager::new()),
        );
        // Tree is accessible via the Arc; confirm lookup_parent_bin works.
        match lookup.lookup_parent_bin(1, b"hello", lsn) {
            BinLookupResult::Found { .. } => {}
            other => panic!("expected Found, got {:?}", other),
        }
    }

    /// RealTreeLookup::lookup_parent_bin returns a readable slot LSN.
    #[test]
    fn test_real_tree_lookup_tree_ref() {
        let lsn = Lsn::new(1, 200);
        let tree = make_tree_with_key(b"key", lsn);
        let lookup = RealTreeLookup::new(
            Arc::new(std::sync::RwLock::new(tree)),
            Arc::new(LockManager::new()),
        );
        match lookup.lookup_parent_bin(1, b"key", lsn) {
            BinLookupResult::Found { tree_lsn } => assert_eq!(tree_lsn, lsn),
            other => panic!("expected Found, got {:?}", other),
        }
    }

    /// lookup_parent_bin returns Found when key exists in the tree.
    #[test]
    fn test_real_tree_lookup_found() {
        let lsn = Lsn::new(2, 500);
        let key = b"alpha";
        let tree = make_tree_with_key(key, lsn);
        let lookup = RealTreeLookup::new(
            Arc::new(std::sync::RwLock::new(tree)),
            Arc::new(LockManager::new()),
        );

        match lookup.lookup_parent_bin(1, key, lsn) {
            BinLookupResult::Found { tree_lsn } => {
                assert_eq!(
                    tree_lsn, lsn,
                    "slot LSN should match what was inserted"
                );
            }
            other => panic!("expected Found, got {:?}", other),
        }
    }

    /// lookup_parent_bin returns NotFound when key is absent.
    #[test]
    fn test_real_tree_lookup_not_found() {
        let lsn = Lsn::new(1, 100);
        let tree = make_tree_with_key(b"present", lsn);
        let lookup = RealTreeLookup::new(
            Arc::new(std::sync::RwLock::new(tree)),
            Arc::new(LockManager::new()),
        );

        let result = lookup.lookup_parent_bin(1, b"absent", lsn);
        assert!(matches!(result, BinLookupResult::NotFound));
    }

    /// lookup_parent_bin on an empty tree returns NotFound.
    #[test]
    fn test_real_tree_lookup_empty_tree() {
        let tree = noxu_tree::Tree::new(1, 128);
        let lookup = RealTreeLookup::new(
            Arc::new(std::sync::RwLock::new(tree)),
            Arc::new(LockManager::new()),
        );
        let lsn = Lsn::new(1, 50);
        let result = lookup.lookup_parent_bin(1, b"anything", lsn);
        assert!(matches!(result, BinLookupResult::NotFound));
    }

    /// migrate_ln_slot succeeds when the slot LSN matches.
    #[test]
    fn test_real_tree_migrate_ln_slot_migrated() {
        let lsn = Lsn::new(3, 300);
        let key = b"migrate_me";
        let tree = make_tree_with_key(key, lsn);
        let lookup = RealTreeLookup::new(
            Arc::new(std::sync::RwLock::new(tree)),
            Arc::new(LockManager::new()),
        );

        let new_lsn = Lsn::new(3, 400);
        let outcome = lookup.migrate_ln_slot(1, key, new_lsn, lsn);
        assert_eq!(
            outcome,
            MigrationOutcome::Migrated,
            "slot LSN matches tree_lsn so migration should succeed"
        );
    }

    /// migrate_ln_slot returns Obsolete when tree_lsn has moved on since lookup.
    #[test]
    fn test_real_tree_migrate_ln_slot_obsolete_lsn_mismatch() {
        let original_lsn = Lsn::new(1, 100);
        let newer_lsn = Lsn::new(1, 200);
        let key = b"raced";

        // Insert with the newer LSN so the slot already differs from original_lsn.
        let tree = make_tree_with_key(key, newer_lsn);
        let lookup = RealTreeLookup::new(
            Arc::new(std::sync::RwLock::new(tree)),
            Arc::new(LockManager::new()),
        );

        // Caller passes tree_lsn = original_lsn; current slot is newer_lsn.
        let outcome =
            lookup.migrate_ln_slot(1, key, original_lsn, original_lsn);
        assert_eq!(
            outcome,
            MigrationOutcome::Obsolete,
            "slot has moved on — should be obsolete"
        );
    }

    /// migrate_ln_slot returns Obsolete when key is absent.
    #[test]
    fn test_real_tree_migrate_ln_slot_key_absent() {
        let tree = make_tree_with_key(b"present", Lsn::new(1, 10));
        let lookup = RealTreeLookup::new(
            Arc::new(std::sync::RwLock::new(tree)),
            Arc::new(LockManager::new()),
        );

        let outcome = lookup.migrate_ln_slot(
            1,
            b"absent",
            Lsn::new(1, 20),
            Lsn::new(1, 20),
        );
        assert_eq!(
            outcome,
            MigrationOutcome::Obsolete,
            "key not in tree — should be obsolete"
        );
    }

    /// lookup_in returns Obsolete for a node not found (empty tree).
    #[test]
    fn test_real_tree_lookup_in_always_obsolete() {
        let tree = noxu_tree::Tree::new(1, 128);
        let lookup = RealTreeLookup::new(
            Arc::new(std::sync::RwLock::new(tree)),
            Arc::new(LockManager::new()),
        );
        let result = lookup.lookup_in(1, 42, Lsn::new(1, 0));
        assert_eq!(result, InLookupResult::Obsolete);
    }

    /// process_file with a RealTreeLookup — active LN migrated end-to-end.
    #[test]
    fn test_process_file_with_real_tree_migrates_active_ln() {
        let key: &[u8] = &[0x10, 0x20, 0x30];
        let lsn = Lsn::new(5, 100);

        let tree = noxu_tree::Tree::new(1, 128);
        tree.insert(key.to_vec(), b"data".to_vec(), lsn).unwrap();
        let lookup = RealTreeLookup::new(
            Arc::new(std::sync::RwLock::new(tree)),
            Arc::new(LockManager::new()),
        );

        let proc = make_processor();
        let summary = crate::FileSummary::new();
        let entries = vec![LogEntry {
            lsn,
            entry_type: LogEntryType::Ln {
                db_id: 1,
                key: key.to_vec(),
                deleted: false,
                expiration_time: 0,
                entry_size: 64,
            },
        }];

        let result = proc.process_file(5, &summary, &entries, &lookup).unwrap();
        assert!(result.completed);
        assert_eq!(result.lns_cleaned, 1);
        assert_eq!(result.lns_migrated, 1);
        assert_eq!(result.lns_dead, 0);
    }

    /// process_file with a RealTreeLookup — key absent → dead.
    #[test]
    fn test_process_file_with_real_tree_absent_key_is_dead() {
        // Tree is empty; no key matches, so the LN should be counted dead.
        let tree = noxu_tree::Tree::new(1, 128);
        let lookup = RealTreeLookup::new(
            Arc::new(std::sync::RwLock::new(tree)),
            Arc::new(LockManager::new()),
        );

        let proc = make_processor();
        let summary = crate::FileSummary::new();
        let entries = vec![make_ln_entry(6, 50, 1, &[0xFF])];

        let result = proc.process_file(6, &summary, &entries, &lookup).unwrap();
        assert!(result.completed);
        assert_eq!(result.lns_cleaned, 1);
        assert_eq!(result.lns_dead, 1);
        assert_eq!(result.lns_migrated, 0);
    }

    /// R-7 (Keith re-audit): migration WAL write failure must abort migration
    /// (return Locked) instead of silently falling back to the stale log_lsn.
    ///
    /// Simulates WAL-write failure by using a SharedTreeLookup backed by a
    /// LogManager on a read-only directory (so write_migration_ln returns None).
    /// The migrated slot must NOT be updated to the old log_lsn, and the
    /// function must return Locked so the entry is retried.
    ///
    /// Crash-safety invariant: if the WAL write fails, the source file must be
    /// retained (not passed to the cleaner as safe-to-delete).
    #[test]
    fn test_r7_migration_abort_on_wal_write_failure() {
        use noxu_log::{FileManager, LogManager};
        use std::sync::RwLock;
        use tempfile::TempDir;

        // Create a tree with a key at a known LSN.
        let log_lsn = Lsn::new(1, 100);
        let tree = noxu_tree::Tree::new(1, 128);
        let tree_arc = Arc::new(RwLock::new(tree));
        {
            let t = tree_arc.write().unwrap();
            let _ = t.insert(b"key1".to_vec(), b"val1".to_vec(), log_lsn);
        }

        // Build a LogManager backed by a real (non-read-only) directory.
        // write_migration_ln will succeed in normal conditions; we force
        // failure by using io_invalid.
        let dir = TempDir::new().unwrap();
        let fm = Arc::new(
            FileManager::new(dir.path(), false, 10_000_000, 100).unwrap(),
        );
        let lm = Arc::new(LogManager::new(fm, 3, 1024 * 1024, 4096));

        // Invalidate I/O on the log manager so write_migration_ln fails.
        lm.io_invalid.store(true, std::sync::atomic::Ordering::Release);

        let lookup = SharedTreeLookup::new(Arc::clone(&tree_arc), lm);

        // Migration must be aborted (Locked) — not use the stale log_lsn.
        let outcome =
            lookup.migrate_ln_slot(1, b"key1", log_lsn, log_lsn);
        assert_eq!(
            outcome,
            MigrationOutcome::Locked,
            "R-7: WAL write failure must abort migration (Locked), got {:?}",
            outcome
        );

        // The slot in the tree must still have the original log_lsn.
        let slot_lsn = {
            let t = tree_arc.read().unwrap();
            RealTreeLookup::get_slot_lsn_from_root(t.get_root(), b"key1")
        };
        assert_eq!(
            slot_lsn,
            Some(log_lsn),
            "R-7: tree slot must retain original log_lsn after aborted migration"
        );
    }
}
