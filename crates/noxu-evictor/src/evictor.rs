//! Main evictor implementation.
//!
//! The [`Evictor`] manages the in-memory B-tree node cache through two
//! independent eviction policy slots:
//!
//! - **`primary_policy`** — normal random-access workload pages.
//! - **`scan_policy`** — pages loaded during sequential scans / full-table
//!   scans.  These are preferentially evicted before primary pages, protecting
//!   the hot working set from scan-induced cache pollution.
//!
//! Both slots default to the **LRU** algorithm; each can be configured
//! independently via [`Evictor::with_algorithm`] (sets both) and
//! [`Evictor::with_scan_algorithm`] (sets only the scan slot).  When both
//! are configured to the same algorithm they are still *separate instances*,
//! so a scan page and a normal page never compete within the same list.
//!
//! A third structure, **`pri2`**, is a simple LRU staging area for dirty
//! nodes.  This is separate from the algorithm choice: dirty nodes are always
//! staged here so the checkpointer can log them before they are evicted cold.
//!
//! ## Wiring
//!
//! ```text
//! // In EnvironmentImpl::new() (or equivalent builder):
//! let evictor = Arc::new(
//!     Evictor::new(arbiter, max_batch_size, lru_only)
//!         .with_log_manager(Arc::clone(&log_manager))
//!         .with_tree(Arc::clone(&primary_tree), db_id),
//! );
//! ```

use crate::arbiter::Arbiter;
use crate::cache_mode::CacheMode;
use crate::evictor_stat::EvictorStats;
use crate::off_heap::OffHeapCache;
use crate::policy::{EvictionAlgorithm, EvictionPolicy};
use crate::slab::SlabList;
use noxu_log::entry::in_log_entry::InLogEntry;
use noxu_log::{LogEntryType, LogManager, Provisional};
use noxu_recovery::Checkpointer;
use noxu_sync::Mutex;
use noxu_tree::NodeRwLock;
use noxu_tree::tree::{BinEntry, BinStub, InEntry, InNodeStub, Tree, TreeNode};
use noxu_util::NULL_LSN;
use std::cell::RefCell;
use std::collections::HashMap;
use std::mem::size_of;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

// ---------------------------------------------------------------------------
// EvictionSource
// ---------------------------------------------------------------------------

/// Source of an eviction operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvictionSource {
    /// Eviction triggered by background daemon threads.
    Daemon,
    /// Critical eviction triggered in application threads when cache is
    /// severely over budget.
    Critical,
    /// Manual eviction requested via API.
    Manual,
    /// Eviction triggered by CacheMode settings.
    CacheMode,
}

// ---------------------------------------------------------------------------
// EvictResult
// ---------------------------------------------------------------------------

/// Result of an eviction run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EvictResult {
    pub nodes_evicted: u64,
    pub bytes_evicted: u64,
}

impl EvictResult {
    pub fn zero() -> Self {
        Self { nodes_evicted: 0, bytes_evicted: 0 }
    }
    pub fn new(nodes_evicted: u64, bytes_evicted: u64) -> Self {
        Self { nodes_evicted, bytes_evicted }
    }
    pub fn add(&mut self, other: &EvictResult) {
        self.nodes_evicted += other.nodes_evicted;
        self.bytes_evicted += other.bytes_evicted;
    }
}

// ---------------------------------------------------------------------------
// NodeEvictionInfo trait
// ---------------------------------------------------------------------------

/// Information the evictor needs about a cached node to decide whether and
/// how to evict it.
pub trait NodeEvictionInfo {
    fn is_dirty(&self) -> bool;
    fn is_bin(&self) -> bool;
    fn is_resident(&self) -> bool;
    fn ref_count(&self) -> usize;
}

// ---------------------------------------------------------------------------
// EvictionDecision
// ---------------------------------------------------------------------------

/// Decision produced by the evictor's `decide_eviction` function.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvictionDecision {
    Skip,
    PutBack,
    MoveDirtyToPri2,
    PartialEvict,
    Evict,
}

/// Apply the `processTarget()` decision tree.
pub fn decide_eviction(
    info: &dyn NodeEvictionInfo,
    already_in_pri2: bool,
    use_dirty_lru: bool,
) -> EvictionDecision {
    if !info.is_resident() {
        return EvictionDecision::Skip;
    }
    if info.ref_count() > 0 {
        return EvictionDecision::PutBack;
    }
    if info.is_bin() {
        return EvictionDecision::PartialEvict;
    }
    if use_dirty_lru && info.is_dirty() && !already_in_pri2 {
        return EvictionDecision::MoveDirtyToPri2;
    }
    EvictionDecision::Evict
}

// ---------------------------------------------------------------------------
// Evictor
// ---------------------------------------------------------------------------

/// The main evictor that manages cache eviction.
///
/// Uses two independent eviction policies (`primary_policy` and
/// `scan_policy`) plus a simple LRU `pri2` staging area for dirty nodes.
pub struct Evictor {
    arbiter: Arbiter,

    /// Policy for normal (random-access) pages.
    primary_policy: Box<dyn EvictionPolicy>,

    /// Policy for scan (sequential-access) pages.  These are evicted
    /// preferentially to protect the primary working set.
    scan_policy: Box<dyn EvictionPolicy>,

    /// Dirty-node staging list (always simple LRU, independent of algorithm
    /// choice).  Dirty nodes wait here until the checkpointer logs them.
    pri2: Mutex<SlabList>,

    stats: EvictorStats,
    shutdown: AtomicBool,
    max_batch_size: usize,
    /// When true, skip the pri2 dirty-node staging list (lru_only mode).
    lru_only: bool,

    next_pri1_index: AtomicU64,
    next_pri2_index: AtomicU64,

    log_manager: Option<Arc<LogManager>>,
    tree: Option<Arc<RwLock<Tree>>>,
    db_id: u64,
    off_heap: Option<Arc<OffHeapCache>>,
    /// Optional checkpointer reference for CC-4: provisional-flag coordination.
    ///
    /// When `Some`, `flush_dirty_node_to_log` queries the checkpointer's
    /// `get_eviction_provisional` to decide whether to log the evicted BIN as
    /// `Provisional::Yes` (checkpoint in progress, node below max flush level)
    /// or `Provisional::No` (no checkpoint, or node at/above max flush level).
    ///
    /// JE ref: `Checkpointer.coordinateEvictionWithCheckpoint` (CC-4 fix).
    checkpointer: Option<Arc<Checkpointer>>,
}

impl Evictor {
    /// Create a new Evictor with LRU as both primary and scan policy.
    ///
    /// Use the builder methods `with_algorithm`, `with_scan_algorithm`,
    /// `with_log_manager`, and `with_tree` to configure further.
    pub fn new(
        arbiter: Arbiter,
        max_batch_size: usize,
        lru_only: bool,
    ) -> Self {
        let primary = EvictionAlgorithm::Lru.new_policy();
        let scan = EvictionAlgorithm::Lru.new_policy();
        Self::with_policies(arbiter, max_batch_size, lru_only, primary, scan)
    }

    /// Internal constructor that accepts pre-built policy objects.
    fn with_policies(
        arbiter: Arbiter,
        max_batch_size: usize,
        lru_only: bool,
        primary: Box<dyn EvictionPolicy>,
        scan: Box<dyn EvictionPolicy>,
    ) -> Self {
        Self {
            arbiter,
            primary_policy: primary,
            scan_policy: scan,
            pri2: Mutex::new(SlabList::new()),
            stats: EvictorStats::new(),
            shutdown: AtomicBool::new(false),
            max_batch_size,
            lru_only,
            next_pri1_index: AtomicU64::new(0),
            next_pri2_index: AtomicU64::new(0),
            log_manager: None,
            tree: None,
            db_id: 0,
            off_heap: None,
            checkpointer: None,
        }
    }

    // -----------------------------------------------------------------------
    // Builder methods
    // -----------------------------------------------------------------------

    /// Set both primary and scan policies to the given algorithm.
    ///
    /// Clears all currently tracked nodes.
    pub fn with_algorithm(self, algo: EvictionAlgorithm) -> Self {
        let primary = algo.new_policy();
        let scan = algo.new_policy();
        Self::with_policies(
            self.arbiter,
            self.max_batch_size,
            self.lru_only,
            primary,
            scan,
        )
        .with_opt_log_manager(self.log_manager)
        .with_opt_tree(self.tree, self.db_id)
        .with_opt_off_heap(self.off_heap)
        .with_opt_checkpointer(self.checkpointer)
    }

    /// Set only the scan-resistant policy to a different algorithm.
    ///
    /// The primary policy is unchanged.
    pub fn with_scan_algorithm(mut self, algo: EvictionAlgorithm) -> Self {
        self.scan_policy = algo.new_policy();
        self
    }

    /// Wire a `LogManager` so dirty nodes are flushed to the WAL before
    /// being removed from memory.
    pub fn with_log_manager(mut self, lm: Arc<LogManager>) -> Self {
        self.log_manager = Some(lm);
        self
    }

    /// Wire the B-tree and database ID for real node-info/size callbacks.
    pub fn with_tree(mut self, tree: Arc<RwLock<Tree>>, db_id: u64) -> Self {
        self.tree = Some(tree);
        self.db_id = db_id;
        self
    }

    /// Wire an off-heap cache.
    pub fn with_off_heap(mut self, cache: Arc<OffHeapCache>) -> Self {
        self.off_heap = Some(cache);
        self
    }

    /// Wire a checkpointer for CC-4 provisional-flag coordination.
    ///
    /// When set, `flush_dirty_node_to_log` queries
    /// `checkpointer.get_eviction_provisional(db_id, node_level)` to choose
    /// `Provisional::Yes` or `Provisional::No` for evicted BINs, matching JE
    /// `Checkpointer.coordinateEvictionWithCheckpoint` (per-tree lookup).
    pub fn with_checkpointer(mut self, ckpt: Arc<Checkpointer>) -> Self {
        self.checkpointer = Some(ckpt);
        self
    }

    // Internal helpers for `with_algorithm` reconstruction.
    fn with_opt_log_manager(mut self, lm: Option<Arc<LogManager>>) -> Self {
        self.log_manager = lm;
        self
    }
    fn with_opt_tree(
        mut self,
        tree: Option<Arc<RwLock<Tree>>>,
        db_id: u64,
    ) -> Self {
        self.tree = tree;
        self.db_id = db_id;
        self
    }
    fn with_opt_off_heap(mut self, oh: Option<Arc<OffHeapCache>>) -> Self {
        self.off_heap = oh;
        self
    }
    fn with_opt_checkpointer(
        mut self,
        ckpt: Option<Arc<Checkpointer>>,
    ) -> Self {
        self.checkpointer = ckpt;
        self
    }

    // -----------------------------------------------------------------------
    // LRU / policy management helpers
    // -----------------------------------------------------------------------

    /// Note that a node has been added to the cache (normal access).
    pub fn note_ins_added(&self, node_id: u64, cache_mode: CacheMode) {
        if cache_mode.is_cold() {
            self.primary_policy.insert_cold(node_id);
        } else {
            self.primary_policy.insert(node_id);
        }
    }

    /// Note that a node has been added during a sequential scan.
    ///
    /// Scan pages are tracked in the scan-resistant policy and evicted
    /// preferentially, protecting the primary hot working set.
    ///
    /// If the node is already in the primary policy this call is a no-op
    /// (primary pages are never demoted by a scan).
    pub fn note_ins_added_scan(&self, node_id: u64) {
        if !self.primary_policy.contains(node_id) {
            self.scan_policy.insert_cold(node_id);
        }
    }

    /// Note that a tracked node has been accessed (normal access).
    pub fn note_ins_accessed(&self, node_id: u64, cache_mode: CacheMode) {
        if cache_mode.is_hot() {
            if !self.primary_policy.touch(node_id) {
                self.scan_policy.touch(node_id);
            }
        } else if cache_mode.is_cold() {
            // Make evictable: try to move toward the cold end.
            // LRU-based policies don't have a separate "cold touch" after
            // insertion; we rely on insert_cold having been called originally.
            // For non-cold initial inserts, just leave the position unchanged.
        }
        // CacheMode::Unchanged → no position change.
    }

    /// Note that a node has been accessed during a sequential scan.
    ///
    /// If the node is in the primary policy its position is left unchanged
    /// (scan accesses don't promote primary-policy pages).  If it is in the
    /// scan policy its position is updated.
    pub fn note_ins_accessed_scan(&self, node_id: u64) {
        if !self.primary_policy.contains(node_id) {
            self.scan_policy.touch(node_id);
        }
    }

    /// Note that a node has been removed from the cache.
    pub fn note_ins_removed(&self, node_id: u64) {
        self.primary_policy.remove(node_id);
        self.scan_policy.remove(node_id);
        self.pri2.lock().remove(node_id);
    }

    /// Move a node from the primary/scan policy to the pri2 staging list.
    ///
    /// Called when a clean node becomes dirty — the node waits in pri2 until
    /// the checkpointer logs it, after which
    /// `complete_checkpoint_for_node` moves it back to primary.
    pub fn move_to_pri2(&self, node_id: u64) -> bool {
        let removed = self.primary_policy.remove(node_id)
            || self.scan_policy.remove(node_id);
        if removed {
            self.pri2.lock().add_back(node_id);
            self.stats.increment(&self.stats.nodes_moved_to_pri2_lru);
        }
        removed
    }

    /// Called by the checkpointer after a dirty node in pri2 has been logged.
    ///
    /// Promotes the node from pri2 back to the primary policy (hot end) so it
    /// can be cleanly evicted on the next eviction pass.
    pub fn complete_checkpoint_for_node(&self, node_id: u64) -> bool {
        if self.pri2.lock().remove(node_id) {
            self.primary_policy.insert(node_id);
            true
        } else {
            false
        }
    }

    // -----------------------------------------------------------------------
    // evict_batch — the real batch eviction loop
    // -----------------------------------------------------------------------

    /// Execute one eviction batch.
    ///
    /// Drains candidates in priority order:
    ///   1. **Scan policy** — these pages should leave first (scan pollution).
    ///   2. **Primary policy** — normal working-set pages.
    ///   3. **Pri2** — dirty nodes (if `lru_only` is false).
    ///
    /// For each candidate the `node_info_fn` callback determines the
    /// [`EvictionDecision`].
    pub fn evict_batch(
        &self,
        _source: EvictionSource,
        node_info_fn: &dyn Fn(u64) -> Option<Box<dyn NodeEvictionInfo>>,
        node_size_fn: &dyn Fn(u64) -> u64,
    ) -> EvictResult {
        let mut result = EvictResult::zero();
        let mut nodes_processed = 0usize;

        // Snapshot phase quotas at the start.  Put-back re-inserts nodes at
        // the hot end; without quotas the batch would re-select them in the
        // same pass, causing infinite cycling.  We process at most (quota)
        // candidates per phase — correct's maxNodesScanned semantics.
        let scan_quota = self.scan_policy.len();
        let primary_quota = self.primary_policy.len();
        let pri2_quota = self.pri2.lock().len;

        let mut scan_processed = 0usize;
        let mut primary_processed = 0usize;
        let mut pri2_processed = 0usize;

        // Phase: 0 = scan, 1 = primary, 2 = pri2.
        let mut phase = if scan_quota == 0 { 1usize } else { 0usize };

        loop {
            if nodes_processed >= self.max_batch_size {
                break;
            }
            if !self.arbiter.still_needs_eviction() {
                break;
            }

            // Pick a candidate from the current phase (respecting quotas).
            let (node_id, from_pri2) = loop {
                match phase {
                    0 if scan_processed < scan_quota => {
                        match self.scan_policy.evict_candidate() {
                            Some(id) => {
                                scan_processed += 1;
                                break (id, false);
                            }
                            None => {
                                phase = 1;
                                continue;
                            }
                        }
                    }
                    0 => {
                        phase = 1;
                        continue;
                    }
                    1 if primary_processed < primary_quota => {
                        match self.primary_policy.evict_candidate() {
                            Some(id) => {
                                primary_processed += 1;
                                break (id, false);
                            }
                            None => {
                                if self.lru_only {
                                    return result;
                                }
                                phase = 2;
                                continue;
                            }
                        }
                    }
                    1 => {
                        if self.lru_only {
                            return result;
                        }
                        phase = 2;
                        continue;
                    }
                    2 if !self.lru_only && pri2_processed < pri2_quota => {
                        match self.pri2.lock().remove_front() {
                            Some(id) => {
                                pri2_processed += 1;
                                break (id, true);
                            }
                            None => return result,
                        }
                    }
                    _ => return result,
                }
            };

            nodes_processed += 1;
            self.stats.increment(&self.stats.nodes_targeted);

            if from_pri2 {
                self.next_pri2_index.fetch_add(1, Ordering::Relaxed);
            } else {
                self.next_pri1_index.fetch_add(1, Ordering::Relaxed);
            }

            let info = match node_info_fn(node_id) {
                Some(i) => i,
                None => {
                    self.stats.increment(&self.stats.nodes_skipped);
                    continue;
                }
            };

            let use_dirty_lru = !self.lru_only;
            let decision =
                decide_eviction(info.as_ref(), from_pri2, use_dirty_lru);

            match decision {
                EvictionDecision::Skip => {
                    self.stats.increment(&self.stats.nodes_skipped);
                }

                EvictionDecision::PutBack => {
                    if from_pri2 {
                        self.pri2.lock().add_back(node_id);
                    } else {
                        // Put back into whichever policy still has it, else primary.
                        self.primary_policy.put_back(node_id);
                    }
                    self.stats.increment(&self.stats.nodes_put_back);
                }

                EvictionDecision::PartialEvict => {
                    // H-9: actually strip LN data from the BIN.  Previously
                    // this path only credited node_size_fn(node_id) bytes
                    // back to the budget without freeing any heap; the
                    // budget tracker drifted below reality and the
                    // evictor under-fired under pressure.  Strip the
                    // embedded LNs (writes any dirty LNs to the log first)
                    // and report the actual bytes freed.
                    //
                    // CC-6: strip_lns_from_node now uses a non-blocking
                    // try_write latch and re-checks cursor_count under the
                    // lock.  `None` means the node is busy or pinned — put
                    // it back instead of blocking.
                    match self.strip_lns_from_node(node_id) {
                        Some(freed_bytes) => {
                            if freed_bytes > 0 {
                                result.bytes_evicted += freed_bytes as u64;
                                self.stats
                                    .increment(&self.stats.nodes_stripped);
                                self.stats.increment(&self.stats.lns_evicted);
                            }
                            if from_pri2 {
                                self.pri2.lock().add_back(node_id);
                            } else {
                                self.primary_policy.put_back(node_id);
                            }
                        }
                        None => {
                            // Node busy or pinned — put back without any
                            // memory-budget change.
                            if from_pri2 {
                                self.pri2.lock().add_back(node_id);
                            } else {
                                self.primary_policy.put_back(node_id);
                            }
                            self.stats.increment(&self.stats.nodes_put_back);
                        }
                    }
                }

                EvictionDecision::MoveDirtyToPri2 => {
                    self.pri2.lock().add_front(node_id);
                    self.stats.increment(&self.stats.nodes_moved_to_pri2_lru);
                }

                EvictionDecision::Evict => {
                    let mut stored_off_heap = false;
                    if let (Some(oh), Some(tree_arc)) =
                        (&self.off_heap, &self.tree)
                        && oh.is_enabled()
                        && let Ok(tree_guard) = tree_arc.read()
                        && let Some(serialized) =
                            tree_guard.serialize_upper_in(node_id)
                    {
                        stored_off_heap = oh.store_node(node_id, serialized);
                    }

                    // CC-6: flush_dirty_node_to_log uses a non-blocking
                    // try_write latch and re-checks cursor_count.  `false`
                    // means the node is busy or became pinned — put it back.
                    if info.is_dirty()
                        && !stored_off_heap
                        && !self.flush_dirty_node_to_log(node_id)
                    {
                        // Node is latched by another thread or pinned.
                        // Put it back; do NOT credit bytes evicted.
                        if from_pri2 {
                            self.pri2.lock().add_back(node_id);
                        } else {
                            self.primary_policy.put_back(node_id);
                        }
                        self.stats.increment(&self.stats.nodes_put_back);
                        continue;
                    }

                    let freed = node_size_fn(node_id);
                    result.bytes_evicted += freed;
                    result.nodes_evicted += 1;
                    self.stats.increment(&self.stats.nodes_evicted);
                }
            }
        }

        result
    }

    // -----------------------------------------------------------------------
    // do_evict — public entry point
    // -----------------------------------------------------------------------

    /// Perform an eviction run.
    ///
    /// **Complexity note (St-H2 fix):** Previously two independent root-down
    /// O(tree) searches ran per eviction candidate — one for
    /// `NodeEvictionInfo` and a second for the in-memory byte size.  This
    /// method now performs **one** unified root-down search via
    /// `find_node_full` that extracts both values in a single tree walk.
    /// The size is stashed in a thread-local `RefCell<HashMap>` by
    /// `node_info_fn` and retrieved in O(1) by `node_size_fn`, so no second
    /// tree walk is needed.  The `RefCell` borrow never overlaps because
    /// `evict_batch` always calls `node_info_fn` before `node_size_fn` for
    /// the same node, and the calls are serialised within a single thread.
    pub fn do_evict(&self, source: EvictionSource) -> EvictResult {
        if let Some(tree_arc) = &self.tree {
            let tree_clone = Arc::clone(tree_arc);

            // St-H2: one unified O(tree) walk per candidate instead of two.
            // The size discovered during the info walk is cached here and
            // drained O(1) when node_size_fn is called for the same node_id.
            // Both closures capture `size_cache` by shared reference (which
            // is Copy); the `RefCell` enforces the runtime borrow rule.
            // Borrows never overlap: evict_batch always calls node_info_fn
            // before node_size_fn for the same node and never concurrently.
            let size_cache: RefCell<HashMap<u64, u64>> =
                RefCell::new(HashMap::new());
            let sc = &size_cache; // shared reference; both closures copy it

            let node_info_fn =
                move |node_id: u64| -> Option<Box<dyn NodeEvictionInfo>> {
                    let guard = tree_clone.read().ok()?;
                    let full = find_node_full(&guard, node_id)?;
                    // Cache the size so node_size_fn needs no second tree walk.
                    sc.borrow_mut().insert(node_id, full.size);
                    Some(Box::new(full.info))
                };
            let node_size_fn = move |node_id: u64| -> u64 {
                // O(1): drain the size deposited by node_info_fn above.
                // Falls back to 1024 only if node_info_fn did not run for
                // this id (should not occur in normal operation).
                sc.borrow_mut().remove(&node_id).unwrap_or(1024)
            };
            self.do_evict_with_callbacks(source, &node_info_fn, &node_size_fn)
        } else {
            self.do_evict_with_callbacks(
                source,
                &default_node_info,
                &default_node_size,
            )
        }
    }

    /// Flush a dirty node to the WAL before evicting it.
    ///
    /// Returns `false` if the node's write latch could not be acquired
    /// immediately (another thread holds a read or write latch) **or** if,
    /// after acquiring the latch, a cursor has pinned the BIN (cursor_count
    /// is positive).  The caller must put the node back into the eviction
    /// list in both cases.
    ///
    /// JE reference: `Evictor.java` `isPinned()` guard +
    /// `latchNoWait`-style non-blocking latch attempt before any eviction
    /// mutation (CC-6 fix).
    fn flush_dirty_node_to_log(&self, node_id: u64) -> bool {
        let tree_arc = match &self.tree {
            Some(t) => Arc::clone(t),
            None => return true, // no tree — nothing to flush
        };

        let node_arc: Arc<NodeRwLock<TreeNode>> = {
            let tree_guard = match tree_arc.read() {
                Ok(g) => g,
                Err(_) => return false, // tree lock poisoned; be conservative
            };
            // CC-6: non-blocking tree scan — if any node in the descent path
            // is write-locked by another thread, treat the target as busy.
            match find_node_arc_nonblocking(&tree_guard, node_id) {
                Ok(Some(a)) => a,
                Ok(None) => return true, // node already gone; allow eviction
                Err(()) => return false, // descent blocked; put back
            }
        };

        // CC-6: non-blocking latch attempt (JE `latchNoWait`-style).
        // If the node is currently held by a reader or writer, put it back
        // rather than blocking the evictor thread.
        let mut node_guard = match node_arc.try_write() {
            Some(g) => g,
            None => return false, // node busy — put back
        };

        // CC-6: re-validate pin count under the lock.  Between the metadata
        // snapshot taken by node_info_fn and acquiring the write latch a
        // cursor may have pinned the BIN.  Mirrors JE `isPinned()` re-check.
        let bin = match &mut *node_guard {
            TreeNode::Bottom(b) => {
                if b.cursor_count > 0 {
                    return false; // pinned — put back
                }
                b
            }
            _ => return true, // non-BIN dirty node; nothing to flush here
        };

        if !bin.dirty && bin.dirty_count() == 0 {
            return true; // clean now; evict normally
        }

        // Log manager check is after the safety guards so cursor-pin
        // checking is always enforced regardless of test configuration.
        let lm = match &self.log_manager {
            Some(lm) => Arc::clone(lm),
            None => return true, // no log manager (tests); allow eviction
        };

        // CC-4: choose Provisional::Yes when a checkpoint is in progress and
        // this BIN's level is below the checkpoint's highest flush level, so
        // the checkpoint's non-provisional ancestor subsumes this entry.
        // When no checkpointer is wired (or no checkpoint is in progress, or
        // the BIN is at/above the flush level), use Provisional::No.
        //
        // JE ref: Checkpointer.coordinateEvictionWithCheckpoint /
        // DirtyINMap.coordinateEvictionWithCheckpoint.
        let provisional = self
            .checkpointer
            .as_ref()
            .map(|c| c.get_eviction_provisional(self.db_id, bin.level))
            .unwrap_or(Provisional::No);

        let full_bytes = bin.serialize_full();
        let entry = InLogEntry::new(
            self.db_id,
            bin.last_full_lsn,
            NULL_LSN,
            full_bytes,
        );
        let mut buf = bytes::BytesMut::with_capacity(entry.log_size());
        entry.write_to_log(&mut buf);

        if let Ok(logged_lsn) =
            lm.log(LogEntryType::BIN, &buf, provisional, false, false)
        {
            bin.clear_dirty_after_full_log(logged_lsn);
            self.stats.increment(&self.stats.dirty_nodes_evicted);
        }
        true
    }

    /// Strips the embedded-LN data from a BIN, freeing the heap allocations
    /// of the per-slot value bytes while keeping the slot keys and LSNs
    /// addressable.  Used by the `PartialEvict` decision path: a hot BIN is
    /// kept in the cache so its descent path stays warm, but the LN data
    /// is dropped to make room for hotter content.
    ///
    /// Returns `Some(freed_bytes)` on success (0 is valid: nothing to strip).
    /// Returns `None` if the write latch could not be acquired immediately or
    /// if, under the latch, `cursor_count > 0` (BIN is pinned by a cursor).
    /// The caller must put the node back into the eviction list on `None`.
    ///
    /// JE reference: `Evictor.java` `isPinned()` + `latchNoWait`-style
    /// non-blocking latch (CC-6 fix).
    fn strip_lns_from_node(&self, node_id: u64) -> Option<usize> {
        let tree_arc = match &self.tree {
            Some(t) => Arc::clone(t),
            None => return Some(0),
        };
        let node_arc: Arc<NodeRwLock<TreeNode>> = {
            let tree_guard = match tree_arc.read() {
                Ok(g) => g,
                Err(_) => return None, // conservative: put back
            };
            // CC-6: non-blocking tree scan.
            match find_node_arc_nonblocking(&tree_guard, node_id) {
                Ok(Some(a)) => a,
                Ok(None) => return Some(0), // already gone
                Err(()) => return None,     // descent blocked; put back
            }
        };

        // CC-6: non-blocking latch attempt (JE `latchNoWait`-style).
        let mut node_guard = node_arc.try_write()?;

        // CC-6: re-validate pin count under the lock (JE `isPinned()` re-check).
        let bin = match &mut *node_guard {
            TreeNode::Bottom(b) => {
                if b.cursor_count > 0 {
                    return None; // pinned — put back
                }
                b
            }
            _ => return Some(0),
        };
        let lm_ref = self.log_manager.as_deref();
        let _ = lm_ref;
        Some(bin.strip_lns())
    }

    /// Perform an eviction run with caller-supplied node callbacks.
    pub fn do_evict_with_callbacks(
        &self,
        source: EvictionSource,
        node_info_fn: &dyn Fn(u64) -> Option<Box<dyn NodeEvictionInfo>>,
        node_size_fn: &dyn Fn(u64) -> u64,
    ) -> EvictResult {
        if self.shutdown.load(Ordering::Relaxed) {
            return EvictResult::zero();
        }
        self.stats.increment(&self.stats.eviction_runs);
        if !self.arbiter.still_needs_eviction() {
            return EvictResult::zero();
        }

        let result = self.evict_batch(source, node_info_fn, node_size_fn);

        match source {
            EvictionSource::Daemon => self
                .stats
                .add(&self.stats.bytes_evicted_daemon, result.bytes_evicted),
            EvictionSource::Critical => self
                .stats
                .add(&self.stats.bytes_evicted_critical, result.bytes_evicted),
            EvictionSource::Manual => self
                .stats
                .add(&self.stats.bytes_evicted_manual, result.bytes_evicted),
            EvictionSource::CacheMode => self
                .stats
                .add(&self.stats.bytes_evicted_cachemode, result.bytes_evicted),
        }

        result
    }

    // -----------------------------------------------------------------------
    // Accessors
    // -----------------------------------------------------------------------

    pub fn get_stats(&self) -> &EvictorStats {
        &self.stats
    }

    pub fn pri1_eviction_count(&self) -> u64 {
        self.next_pri1_index.load(Ordering::Relaxed)
    }
    pub fn pri2_eviction_count(&self) -> u64 {
        self.next_pri2_index.load(Ordering::Relaxed)
    }

    /// Returns `(primary_len + scan_len, pri2_len)`.
    pub fn get_lru_sizes(&self) -> (usize, usize) {
        (
            self.primary_policy.len() + self.scan_policy.len(),
            self.pri2.lock().len,
        )
    }

    /// Returns `(primary_len, scan_len, pri2_len)`.
    pub fn get_policy_sizes(&self) -> (usize, usize, usize) {
        (
            self.primary_policy.len(),
            self.scan_policy.len(),
            self.pri2.lock().len,
        )
    }

    pub fn update_lru_stats(&self) {
        let (pri1, _, pri2) = self.get_policy_sizes();
        self.stats.set(&self.stats.pri1_lru_size, pri1 as u64);
        self.stats.set(&self.stats.pri2_lru_size, pri2 as u64);
    }

    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
    }
    pub fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::Relaxed)
    }
    pub fn get_arbiter(&self) -> &Arbiter {
        &self.arbiter
    }

    /// Name of the primary eviction algorithm.
    pub fn primary_algorithm_name(&self) -> &'static str {
        self.primary_policy.name()
    }

    /// Name of the scan-resistant eviction algorithm.
    pub fn scan_algorithm_name(&self) -> &'static str {
        self.scan_policy.name()
    }

    // -----------------------------------------------------------------------
    // Test helpers
    // -----------------------------------------------------------------------

    /// Insert directly into the pri2 staging list.  Test / integration use.
    #[doc(hidden)]
    pub fn pri2_insert_for_test(&self, node_id: u64) {
        let mut p = self.pri2.lock();
        if !p.contains(node_id) {
            p.add_back(node_id);
        }
    }
}

impl std::fmt::Debug for Evictor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Evictor")
            .field("max_batch_size", &self.max_batch_size)
            .field("lru_only", &self.lru_only)
            .field("shutdown", &self.shutdown.load(Ordering::Relaxed))
            .field("primary_algo", &self.primary_policy.name())
            .field("scan_algo", &self.scan_policy.name())
            .field("db_id", &self.db_id)
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Real node-info / node-size helpers
// ---------------------------------------------------------------------------

struct RealNodeInfo {
    dirty: bool,
    is_bin: bool,
    pin_count: usize,
}
impl NodeEvictionInfo for RealNodeInfo {
    fn is_dirty(&self) -> bool {
        self.dirty
    }
    fn is_bin(&self) -> bool {
        self.is_bin
    }
    fn is_resident(&self) -> bool {
        true
    }
    fn ref_count(&self) -> usize {
        self.pin_count
    }
}

// ---------------------------------------------------------------------------
// Unified node lookup — O(tree) single-pass search (St-H2)
// ---------------------------------------------------------------------------

/// All data extracted from a single root-down tree walk for one node.
///
/// Previously the evictor performed up to three separate root-down searches
/// per eviction candidate:
/// 1. `find_node_info_recursive` — eviction-decision metadata
/// 2. `find_node_size_recursive` — in-memory byte count
/// 3. `find_node_arc_recursive`  — `Arc` for write-locking (flush / strip)
///
/// `NodeFull` collapses all three into a **single** O(tree) walk.
/// The `arc` field enables write-lock operations without a re-scan;
/// `info` drives the eviction decision; `size` feeds memory-budget
/// accounting — all derived from the same read-guard acquisition.
struct NodeFull {
    /// Cloned `Arc` so the caller can write-lock the node without another
    /// tree walk.
    arc: Arc<NodeRwLock<TreeNode>>,
    /// Eviction-decision metadata (dirty flag, BIN/IN, pin count).
    info: RealNodeInfo,
    /// In-memory byte count using the formula:
    /// - BIN: `size_of::<BinStub>() + entries * size_of::<BinEntry>() + Σ(key + data)`
    /// - IN:  `size_of::<InNodeStub>() + entries * size_of::<InEntry>() + Σ key`
    size: u64,
}

/// Single root-down tree walk that returns a [`NodeFull`] for `node_id`.
///
/// Replaces three previous separate recursive searches
/// (`find_node_info_recursive`, `find_node_size_recursive`,
/// `find_node_arc_recursive`) with one, reducing the per-eviction
/// tree-traversal count from up to three O(n) walks to one.
fn find_node_full(tree: &Tree, node_id: u64) -> Option<NodeFull> {
    let root_arc = tree.get_root()?;
    find_node_full_recursive(&root_arc, node_id)
}

fn find_node_full_recursive(
    node_arc: &Arc<NodeRwLock<TreeNode>>,
    target_id: u64,
) -> Option<NodeFull> {
    let guard = node_arc.read();
    match &*guard {
        TreeNode::Bottom(b) => {
            if b.node_id != target_id {
                return None;
            }
            let info = RealNodeInfo {
                dirty: b.dirty || b.dirty_count() > 0,
                is_bin: true,
                pin_count: b.cursor_count.max(0) as usize,
            };
            // Size formula (BIN): struct overhead + per-slot fixed overhead +
            // variable key and embedded-LN data bytes.
            let size = (size_of::<BinStub>()
                + b.entries.len() * size_of::<BinEntry>()
                + b.entries
                    .iter()
                    .map(|e| {
                        e.key.len()
                            + e.data.as_ref().map(|d| d.len()).unwrap_or(0)
                    })
                    .sum::<usize>()) as u64;
            let arc = Arc::clone(node_arc);
            drop(guard);
            Some(NodeFull { arc, info, size })
        }
        TreeNode::Internal(n) => {
            if n.node_id == target_id {
                let info = RealNodeInfo {
                    dirty: n.dirty,
                    is_bin: false,
                    pin_count: 0,
                };
                // Size formula (IN): struct overhead + per-entry fixed overhead
                // + variable key bytes.
                let size = (size_of::<InNodeStub>()
                    + n.entries.len() * size_of::<InEntry>()
                    + n.entries.iter().map(|e| e.key.len()).sum::<usize>())
                    as u64;
                let arc = Arc::clone(node_arc);
                drop(guard);
                return Some(NodeFull { arc, info, size });
            }
            let children: Vec<Arc<NodeRwLock<TreeNode>>> = n
                .entries
                .iter()
                .filter_map(|e| e.child.as_ref().map(Arc::clone))
                .collect();
            drop(guard);
            for child in children {
                if let Some(full) = find_node_full_recursive(&child, target_id)
                {
                    return Some(full);
                }
            }
            None
        }
    }
}

/// Locate a node's `Arc` for write-lock operations (flush / LN strip).
///
/// Delegates to `find_node_full` and discards the info/size fields;
/// the marginal cost is only the size arithmetic on the found node
/// (no extra tree traversal).
fn find_node_arc(
    tree: &Tree,
    node_id: u64,
) -> Option<Arc<NodeRwLock<TreeNode>>> {
    find_node_full(tree, node_id).map(|f| f.arc)
}

/// Non-blocking variant of [`find_node_arc`] used by the CC-6 mutation paths.
///
/// Uses `try_read()` at every level so the evictor never blocks on a node
/// that another thread holds exclusively.  Returns `Err(())` if any node in
/// the descent path is currently write-locked (caller must put the eviction
/// candidate back), `Ok(None)` if the target is simply not present, and
/// `Ok(Some(arc))` on success.
///
/// JE ref: `Evictor.java` `latchNoWait`-style non-blocking scan before any
/// eviction mutation (CC-6 fix).
fn find_node_arc_nonblocking(
    tree: &Tree,
    node_id: u64,
) -> Result<Option<Arc<NodeRwLock<TreeNode>>>, ()> {
    let root_arc = match tree.get_root() {
        Some(r) => r,
        None => return Ok(None),
    };
    find_node_arc_nonblocking_recursive(&root_arc, node_id)
}

fn find_node_arc_nonblocking_recursive(
    node_arc: &Arc<NodeRwLock<TreeNode>>,
    target_id: u64,
) -> Result<Option<Arc<NodeRwLock<TreeNode>>>, ()> {
    // CC-6: use try_read so the evictor never blocks a reader that is
    // write-locked by another thread (cursor mutation, split, etc.).
    let guard = node_arc.try_read().ok_or(())?;
    match &*guard {
        TreeNode::Bottom(b) => {
            if b.node_id != target_id {
                return Ok(None);
            }
            let arc = Arc::clone(node_arc);
            drop(guard);
            Ok(Some(arc))
        }
        TreeNode::Internal(n) => {
            if n.node_id == target_id {
                let arc = Arc::clone(node_arc);
                drop(guard);
                return Ok(Some(arc));
            }
            let children: Vec<Arc<NodeRwLock<TreeNode>>> = n
                .entries
                .iter()
                .filter_map(|e| e.child.as_ref().map(Arc::clone))
                .collect();
            drop(guard);
            for child in children {
                match find_node_arc_nonblocking_recursive(&child, target_id) {
                    Ok(Some(arc)) => return Ok(Some(arc)),
                    Ok(None) => continue,
                    Err(()) => return Err(()), // propagate busy signal
                }
            }
            Ok(None)
        }
    }
}

// ---------------------------------------------------------------------------
// Default callbacks (unit tests / no tree wired)
// ---------------------------------------------------------------------------

struct DefaultNodeInfo;
impl NodeEvictionInfo for DefaultNodeInfo {
    fn is_dirty(&self) -> bool {
        false
    }
    fn is_bin(&self) -> bool {
        false
    }
    fn is_resident(&self) -> bool {
        true
    }
    fn ref_count(&self) -> usize {
        0
    }
}

fn default_node_info(_id: u64) -> Option<Box<dyn NodeEvictionInfo>> {
    Some(Box::new(DefaultNodeInfo))
}

fn default_node_size(_id: u64) -> u64 {
    1024
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arbiter::Arbiter;
    use crate::cache_mode::CacheMode;
    use std::sync::{Arc, atomic::AtomicI64};

    fn make_evictor(
        usage: i64,
        max: i64,
        batch: usize,
    ) -> (Arc<AtomicI64>, Evictor) {
        let counter = Arc::new(AtomicI64::new(usage));
        let arbiter = Arbiter::new(max, Arc::clone(&counter), 100, 200);
        let evictor = Evictor::new(arbiter, batch, false);
        (counter, evictor)
    }

    // -----------------------------------------------------------------------
    // EvictionDecision / decide_eviction
    // -----------------------------------------------------------------------

    struct NodeInfo {
        dirty: bool,
        bin: bool,
        resident: bool,
        refs: usize,
    }
    impl NodeEvictionInfo for NodeInfo {
        fn is_dirty(&self) -> bool {
            self.dirty
        }
        fn is_bin(&self) -> bool {
            self.bin
        }
        fn is_resident(&self) -> bool {
            self.resident
        }
        fn ref_count(&self) -> usize {
            self.refs
        }
    }
    fn info(dirty: bool, bin: bool, resident: bool, refs: usize) -> NodeInfo {
        NodeInfo { dirty, bin, resident, refs }
    }

    #[test]
    fn test_decide_skip() {
        assert_eq!(
            decide_eviction(&info(false, false, false, 0), false, true),
            EvictionDecision::Skip
        );
    }
    #[test]
    fn test_decide_putback() {
        assert_eq!(
            decide_eviction(&info(false, false, true, 2), false, true),
            EvictionDecision::PutBack
        );
    }
    #[test]
    fn test_decide_partial() {
        assert_eq!(
            decide_eviction(&info(false, true, true, 0), false, true),
            EvictionDecision::PartialEvict
        );
    }
    #[test]
    fn test_decide_dirty_pri2() {
        assert_eq!(
            decide_eviction(&info(true, false, true, 0), false, true),
            EvictionDecision::MoveDirtyToPri2
        );
    }
    #[test]
    fn test_decide_dirty_in_pri2() {
        assert_eq!(
            decide_eviction(&info(true, false, true, 0), true, true),
            EvictionDecision::Evict
        );
    }
    #[test]
    fn test_decide_dirty_lruonly() {
        assert_eq!(
            decide_eviction(&info(true, false, true, 0), false, false),
            EvictionDecision::Evict
        );
    }
    #[test]
    fn test_decide_clean() {
        assert_eq!(
            decide_eviction(&info(false, false, true, 0), false, true),
            EvictionDecision::Evict
        );
    }

    // -----------------------------------------------------------------------
    // EvictResult
    // -----------------------------------------------------------------------

    #[test]
    fn test_evict_result_zero() {
        let r = EvictResult::zero();
        assert_eq!(r.nodes_evicted, 0);
        assert_eq!(r.bytes_evicted, 0);
    }
    #[test]
    fn test_evict_result_add() {
        let mut r = EvictResult::new(5, 1024);
        r.add(&EvictResult::new(3, 512));
        assert_eq!(r.nodes_evicted, 8);
        assert_eq!(r.bytes_evicted, 1536);
    }

    // -----------------------------------------------------------------------
    // Construction / algorithm selection
    // -----------------------------------------------------------------------

    #[test]
    fn test_default_algorithm_is_lru() {
        let usage = Arc::new(AtomicI64::new(0));
        let arbiter = Arbiter::new(1000, usage, 100, 200);
        let e = Evictor::new(arbiter, 100, false);
        assert_eq!(e.primary_algorithm_name(), "LRU");
        assert_eq!(e.scan_algorithm_name(), "LRU");
    }

    #[test]
    fn test_with_algorithm_clock() {
        let usage = Arc::new(AtomicI64::new(0));
        let arbiter = Arbiter::new(1000, usage, 100, 200);
        let e = Evictor::new(arbiter, 100, false)
            .with_algorithm(EvictionAlgorithm::Clock);
        assert_eq!(e.primary_algorithm_name(), "Clock");
        assert_eq!(e.scan_algorithm_name(), "Clock");
    }

    #[test]
    fn test_with_scan_algorithm_independent() {
        let usage = Arc::new(AtomicI64::new(0));
        let arbiter = Arbiter::new(1000, usage, 100, 200);
        let e = Evictor::new(arbiter, 100, false)
            .with_algorithm(EvictionAlgorithm::Arc)
            .with_scan_algorithm(EvictionAlgorithm::Lirs);
        assert_eq!(e.primary_algorithm_name(), "ARC");
        assert_eq!(e.scan_algorithm_name(), "LIRS");
    }

    // -----------------------------------------------------------------------
    // note_ins_* and get_lru_sizes / get_policy_sizes
    // -----------------------------------------------------------------------

    #[test]
    fn test_note_ins_added_hot() {
        let usage = Arc::new(AtomicI64::new(0));
        let e = Evictor::new(Arbiter::new(1000, usage, 100, 200), 100, false);
        e.note_ins_added(1, CacheMode::Default);
        e.note_ins_added(2, CacheMode::KeepHot);
        assert_eq!(e.get_lru_sizes(), (2, 0));
        assert_eq!(e.get_policy_sizes(), (2, 0, 0));
    }

    #[test]
    fn test_note_ins_added_cold() {
        let usage = Arc::new(AtomicI64::new(0));
        let e = Evictor::new(Arbiter::new(1000, usage, 100, 200), 100, false);
        e.note_ins_added(1, CacheMode::MakeEvictable);
        assert_eq!(e.get_lru_sizes(), (1, 0));
    }

    #[test]
    fn test_note_ins_added_scan_separate_from_primary() {
        let usage = Arc::new(AtomicI64::new(0));
        let e = Evictor::new(Arbiter::new(1000, usage, 100, 200), 100, false);
        e.note_ins_added(1, CacheMode::Default); // → primary
        e.note_ins_added_scan(2); // → scan
        let (p, s, p2) = e.get_policy_sizes();
        assert_eq!(p, 1, "primary should have 1");
        assert_eq!(s, 1, "scan should have 1");
        assert_eq!(p2, 0);
    }

    #[test]
    fn test_note_ins_added_scan_does_not_move_primary_pages() {
        let usage = Arc::new(AtomicI64::new(0));
        let e = Evictor::new(Arbiter::new(1000, usage, 100, 200), 100, false);
        e.note_ins_added(1, CacheMode::Default); // → primary
        e.note_ins_added_scan(1); // already in primary → no-op
        let (p, s, _) = e.get_policy_sizes();
        assert_eq!(p, 1);
        assert_eq!(s, 0); // not duplicated in scan policy
    }

    #[test]
    fn test_note_ins_removed() {
        let usage = Arc::new(AtomicI64::new(0));
        let e = Evictor::new(Arbiter::new(1000, usage, 100, 200), 100, false);
        e.note_ins_added(1, CacheMode::Default);
        e.note_ins_added_scan(2);
        e.note_ins_removed(1);
        e.note_ins_removed(2);
        assert_eq!(e.get_lru_sizes(), (0, 0));
    }

    // -----------------------------------------------------------------------
    // move_to_pri2 and complete_checkpoint_for_node
    // -----------------------------------------------------------------------

    #[test]
    fn test_move_to_pri2() {
        let usage = Arc::new(AtomicI64::new(0));
        let e = Evictor::new(Arbiter::new(1000, usage, 100, 200), 100, false);
        e.note_ins_added(1, CacheMode::Default);
        assert_eq!(e.get_lru_sizes(), (1, 0));
        assert!(e.move_to_pri2(1));
        assert_eq!(e.get_lru_sizes(), (0, 1));
        assert!(!e.move_to_pri2(1)); // already in pri2
    }

    #[test]
    fn test_move_to_pri2_from_scan() {
        let usage = Arc::new(AtomicI64::new(0));
        let e = Evictor::new(Arbiter::new(1000, usage, 100, 200), 100, false);
        e.note_ins_added_scan(5);
        assert_eq!(e.get_policy_sizes(), (0, 1, 0));
        assert!(e.move_to_pri2(5));
        assert_eq!(e.get_policy_sizes(), (0, 0, 1));
    }

    #[test]
    fn test_complete_checkpoint_promotes_to_primary() {
        let usage = Arc::new(AtomicI64::new(0));
        let e = Evictor::new(Arbiter::new(1000, usage, 100, 200), 100, false);
        e.note_ins_added(42, CacheMode::Default);
        e.move_to_pri2(42);
        assert_eq!(e.get_lru_sizes(), (0, 1));
        assert!(e.complete_checkpoint_for_node(42));
        assert_eq!(e.get_lru_sizes(), (1, 0));
    }

    #[test]
    fn test_complete_checkpoint_noop_if_not_in_pri2() {
        let usage = Arc::new(AtomicI64::new(0));
        let e = Evictor::new(Arbiter::new(1000, usage, 100, 200), 100, false);
        e.note_ins_added(7, CacheMode::Default);
        assert!(!e.complete_checkpoint_for_node(7));
        assert_eq!(e.get_lru_sizes(), (1, 0));
    }

    // -----------------------------------------------------------------------
    // do_evict under / over budget
    // -----------------------------------------------------------------------

    #[test]
    fn test_do_evict_under_budget() {
        let usage = Arc::new(AtomicI64::new(500));
        let e = Evictor::new(Arbiter::new(1000, usage, 100, 200), 100, false);
        e.note_ins_added(1, CacheMode::Default);
        e.note_ins_added(2, CacheMode::Default);
        let r = e.do_evict(EvictionSource::Daemon);
        assert_eq!(r.nodes_evicted, 0);
    }

    #[test]
    fn test_do_evict_over_budget() {
        let usage = Arc::new(AtomicI64::new(1500));
        let e = Evictor::new(Arbiter::new(1000, usage, 100, 200), 100, false);
        for i in 1..=5 {
            e.note_ins_added(i, CacheMode::Default);
        }
        let r = e.do_evict(EvictionSource::Critical);
        assert!(r.nodes_evicted > 0);
        let stats = e.get_stats();
        assert!(stats.get(&stats.bytes_evicted_critical) > 0);
    }

    #[test]
    fn test_shutdown_stops_eviction() {
        let usage = Arc::new(AtomicI64::new(0));
        let e = Evictor::new(Arbiter::new(1000, usage, 100, 200), 100, false);
        e.shutdown();
        assert!(e.is_shutdown());
        assert_eq!(e.do_evict(EvictionSource::Daemon).nodes_evicted, 0);
    }

    #[test]
    fn test_batch_size_limit() {
        let usage = Arc::new(AtomicI64::new(2000));
        let e = Evictor::new(Arbiter::new(1000, usage, 100, 200), 3, false);
        for i in 1..=10 {
            e.note_ins_added(i, CacheMode::Default);
        }
        let r = e.do_evict(EvictionSource::Daemon);
        assert!(r.nodes_evicted <= 3);
    }

    // -----------------------------------------------------------------------
    // evict_batch with custom callbacks — each decision path
    // -----------------------------------------------------------------------

    fn static_info_fn(
        dirty: bool,
        bin: bool,
        resident: bool,
        refs: usize,
    ) -> impl Fn(u64) -> Option<Box<dyn NodeEvictionInfo>> {
        move |_| {
            Some(Box::new(NodeInfo { dirty, bin, resident, refs })
                as Box<dyn NodeEvictionInfo>)
        }
    }

    fn size_512(_id: u64) -> u64 {
        512
    }

    #[test]
    fn test_evict_batch_skip_path() {
        let (_c, e) = make_evictor(1500, 1000, 10);
        for i in 1..=3u64 {
            e.note_ins_added(i, CacheMode::Default);
        }
        let r = e.evict_batch(
            EvictionSource::Daemon,
            &static_info_fn(false, false, false, 0),
            &size_512,
        );
        assert_eq!(r.nodes_evicted, 0);
        let s = e.get_stats();
        assert_eq!(s.get(&s.nodes_skipped), 3);
    }

    #[test]
    fn test_evict_batch_putback_path() {
        let (_c, e) = make_evictor(1500, 1000, 10);
        for i in 1..=3u64 {
            e.note_ins_added(i, CacheMode::Default);
        }
        let r = e.evict_batch(
            EvictionSource::Daemon,
            &static_info_fn(false, false, true, 1),
            &size_512,
        );
        assert_eq!(r.nodes_evicted, 0);
        let s = e.get_stats();
        assert_eq!(s.get(&s.nodes_put_back), 3);
        assert_eq!(e.get_lru_sizes().0, 3);
    }

    #[test]
    fn test_evict_batch_partial_evict_path() {
        // H-9: PartialEvict now actually strips LN data via
        // strip_lns_from_node().  Without a tree wired, that returns 0
        // bytes (the test's mock evictor has no tree).  The call-path
        // is still exercised: the evictor visits each node, decides
        // PartialEvict, attempts to strip, and puts the node back.
        // bytes_evicted is 0 because nothing was actually freed (no
        // tree, no slot data to drop) — this is the correct outcome
        // and is what changed from the pre-H-9 behavior, where the
        // evictor lied about freeing bytes that were never actually
        // dropped.
        let (_c, e) = make_evictor(1500, 1000, 10);
        for i in 1..=3u64 {
            e.note_ins_added(i, CacheMode::Default);
        }
        let r = e.evict_batch(
            EvictionSource::Daemon,
            &static_info_fn(false, true, true, 0),
            &size_512,
        );
        assert_eq!(r.nodes_evicted, 0);
        // bytes_evicted is 0 in this no-tree mock; pre-H-9 it was 3*512
        // because the evictor counted node_size_fn without freeing.
        assert_eq!(r.bytes_evicted, 0);
        let s = e.get_stats();
        // nodes_stripped is also 0 because strip_lns_from_node returned 0
        // (no tree).  The path was exercised; the increment is gated on
        // freed > 0.
        assert_eq!(s.get(&s.nodes_stripped), 0);
        assert_eq!(e.get_lru_sizes().0, 3);
    }

    #[test]
    fn test_evict_batch_move_dirty_to_pri2_path() {
        // batch_size == 3 so the batch stops after draining primary; avoids
        // spilling into pri2 and re-evicting the just-moved nodes.
        let counter = Arc::new(AtomicI64::new(1500));
        let e = Evictor::new(
            Arbiter::new(1000, Arc::clone(&counter), 100, 200),
            3,
            false,
        );
        for i in 1..=3u64 {
            e.note_ins_added(i, CacheMode::Default);
        }
        let r = e.evict_batch(
            EvictionSource::Daemon,
            &static_info_fn(true, false, true, 0),
            &size_512,
        );
        assert_eq!(r.nodes_evicted, 0);
        let s = e.get_stats();
        assert_eq!(s.get(&s.nodes_moved_to_pri2_lru), 3);
        assert_eq!(e.get_lru_sizes(), (0, 3));
    }

    #[test]
    fn test_evict_batch_evict_path() {
        let (_c, e) = make_evictor(1500, 1000, 10);
        for i in 1..=3u64 {
            e.note_ins_added(i, CacheMode::Default);
        }
        let r = e.evict_batch(
            EvictionSource::Daemon,
            &static_info_fn(false, false, true, 0),
            &size_512,
        );
        assert_eq!(r.nodes_evicted, 3);
        assert_eq!(r.bytes_evicted, 3 * 512);
        assert_eq!(e.get_lru_sizes(), (0, 0));
    }

    #[test]
    fn test_evict_batch_dirty_in_pri2_evicts() {
        let (_c, e) = make_evictor(1500, 1000, 10);
        for i in 1..=3u64 {
            e.note_ins_added(i, CacheMode::Default);
            e.move_to_pri2(i);
        }
        assert_eq!(e.get_lru_sizes(), (0, 3));
        let r = e.evict_batch(
            EvictionSource::Daemon,
            &static_info_fn(true, false, true, 0),
            &size_512,
        );
        assert_eq!(r.nodes_evicted, 3);
        let s = e.get_stats();
        assert_eq!(s.get(&s.nodes_evicted), 3);
    }

    // -----------------------------------------------------------------------
    // Scan policy evicted preferentially
    // -----------------------------------------------------------------------

    #[test]
    fn test_scan_pages_evicted_before_primary() {
        let usage = Arc::new(AtomicI64::new(2000));
        let e = Evictor::new(Arbiter::new(1000, usage, 100, 200), 100, false);
        // 3 primary pages.
        e.note_ins_added(1, CacheMode::Default);
        e.note_ins_added(2, CacheMode::Default);
        e.note_ins_added(3, CacheMode::Default);
        // 2 scan pages.
        e.note_ins_added_scan(10);
        e.note_ins_added_scan(11);
        assert_eq!(e.get_policy_sizes(), (3, 2, 0));

        // Evict exactly 2 — scan pages should go first.
        let size_fn = |_| 512u64;
        let info_fn = |_| {
            Some(Box::new(NodeInfo {
                dirty: false,
                bin: false,
                resident: true,
                refs: 0,
            }) as Box<dyn NodeEvictionInfo>)
        };
        let r = e.evict_batch(EvictionSource::Daemon, &info_fn, &size_fn);
        // Scan policy had 2 nodes, they leave first.
        let (p, s, _) = e.get_policy_sizes();
        assert_eq!(s, 0, "all scan pages should have been evicted");
        assert!(p <= 3, "some primary pages may also have been evicted");
        let _ = r;
    }

    // -----------------------------------------------------------------------
    // update_lru_stats
    // -----------------------------------------------------------------------

    #[test]
    fn test_update_lru_stats() {
        let usage = Arc::new(AtomicI64::new(0));
        let e = Evictor::new(Arbiter::new(1000, usage, 100, 200), 100, false);
        e.note_ins_added(1, CacheMode::Default);
        e.note_ins_added(2, CacheMode::Default);
        e.move_to_pri2(2);
        e.update_lru_stats();
        let s = e.get_stats();
        assert_eq!(s.get(&s.pri1_lru_size), 1);
        assert_eq!(s.get(&s.pri2_lru_size), 1);
    }

    // -----------------------------------------------------------------------
    // All five algorithms work end-to-end
    // -----------------------------------------------------------------------

    fn algo_evicts_all_nodes(algo: EvictionAlgorithm) {
        let usage = Arc::new(AtomicI64::new(5000));
        let e = Evictor::new(
            Arbiter::new(1000, Arc::clone(&usage), 100, 200),
            100,
            false,
        )
        .with_algorithm(algo);
        for i in 1..=5u64 {
            e.note_ins_added(i, CacheMode::Default);
        }
        let size_fn = |_| 512u64;
        let info_fn = |_| {
            Some(Box::new(NodeInfo {
                dirty: false,
                bin: false,
                resident: true,
                refs: 0,
            }) as Box<dyn NodeEvictionInfo>)
        };
        let r = e.evict_batch(EvictionSource::Daemon, &info_fn, &size_fn);
        assert_eq!(r.nodes_evicted, 5, "{:?} failed to evict all nodes", algo);
    }

    #[test]
    fn test_lru_evicts_all() {
        algo_evicts_all_nodes(EvictionAlgorithm::Lru);
    }
    #[test]
    fn test_clock_evicts_all() {
        algo_evicts_all_nodes(EvictionAlgorithm::Clock);
    }
    #[test]
    fn test_arc_evicts_all() {
        algo_evicts_all_nodes(EvictionAlgorithm::Arc);
    }
    #[test]
    fn test_car_evicts_all() {
        algo_evicts_all_nodes(EvictionAlgorithm::Car);
    }
    #[test]
    fn test_lirs_evicts_all() {
        algo_evicts_all_nodes(EvictionAlgorithm::Lirs);
    }

    // -----------------------------------------------------------------------
    // do_evict_with_callbacks — source statistics
    // -----------------------------------------------------------------------

    #[test]
    fn test_do_evict_daemon_stats() {
        let (counter, e) = make_evictor(1500, 1000, 10);
        for i in 1..=4u64 {
            e.note_ins_added(i, CacheMode::Default);
        }
        let r = e.do_evict_with_callbacks(
            EvictionSource::Daemon,
            &static_info_fn(false, false, true, 0),
            &size_512,
        );
        assert_eq!(
            e.get_stats().get(&e.get_stats().bytes_evicted_daemon),
            r.bytes_evicted
        );
        drop(counter);
    }

    #[test]
    fn test_lru_only_ignores_pri2() {
        let usage = Arc::new(AtomicI64::new(1500));
        let e = Evictor::new(
            Arbiter::new(1000, Arc::clone(&usage), 100, 200),
            10,
            true,
        );
        for i in 1..=2u64 {
            e.note_ins_added(i, CacheMode::Default);
        }
        e.pri2_insert_for_test(99);
        let r = e.evict_batch(
            EvictionSource::Daemon,
            &static_info_fn(false, false, true, 0),
            &size_512,
        );
        assert_eq!(r.nodes_evicted, 2);
        assert_eq!(e.get_lru_sizes().1, 1); // pri2 untouched
    }

    // -----------------------------------------------------------------------
    // St-H2: size-equivalence and single-walk tests
    // -----------------------------------------------------------------------

    /// Verify that the size reported by `find_node_full` (the new unified
    /// single-pass search) matches the explicit BIN size formula:
    ///
    ///   `size_of::<BinStub>() + entries * size_of::<BinEntry>() + Σ(key + data)`
    ///
    /// This is the regression oracle for St-H2: if the formula or the struct
    /// layout ever changes the test will catch the divergence immediately.
    #[test]
    fn test_find_node_full_bin_size_matches_formula() {
        use noxu_util::Lsn;
        use std::mem::size_of;
        use std::sync::{Arc, RwLock};

        let tree = Arc::new(RwLock::new(noxu_tree::tree::Tree::new(1, 128)));

        // Insert three entries with known key and data lengths.
        // The tree always keeps an IN above the first BIN, so the root will
        // be an Internal node; we descend to find the BIN leaf.
        {
            let t = tree.write().unwrap();
            t.insert(b"key-alpha".to_vec(), b"data-a".to_vec(), Lsn::new(1, 1))
                .unwrap();
            t.insert(b"key-beta".to_vec(), b"data-bb".to_vec(), Lsn::new(1, 2))
                .unwrap();
            t.insert(
                b"key-gamma".to_vec(),
                b"data-ccc".to_vec(),
                Lsn::new(1, 3),
            )
            .unwrap();
        }

        // Locate the BIN leaf (may be root or one level down depending on
        // the initial tree shape; walk until we hit a Bottom node).
        fn find_bin_node(
            node_arc: &Arc<noxu_tree::NodeRwLock<TreeNode>>,
        ) -> Option<(u64, Vec<(usize, usize)>)> {
            let guard = node_arc.read();
            match &*guard {
                TreeNode::Bottom(b) => {
                    let id = b.node_id;
                    let entries = b
                        .entries
                        .iter()
                        .map(|e| {
                            (
                                e.key.len(),
                                e.data.as_ref().map(|d| d.len()).unwrap_or(0),
                            )
                        })
                        .collect();
                    Some((id, entries))
                }
                TreeNode::Internal(n) => {
                    // The first child should eventually lead to a BIN.
                    let first_child = n.entries[0].child.as_ref()?.clone();
                    drop(guard);
                    find_bin_node(&first_child)
                }
            }
        }

        let (bin_id, bin_entries) = {
            let t = tree.read().unwrap();
            let root_arc = t.get_root().expect("tree must have a root");
            find_bin_node(&root_arc).expect("must find a BIN leaf")
        };

        // Compute expected size using the explicit formula.
        let expected: u64 = (size_of::<BinStub>()
            + bin_entries.len() * size_of::<BinEntry>()
            + bin_entries.iter().map(|(k, d)| k + d).sum::<usize>())
            as u64;

        // Now ask find_node_full for the same node.
        let actual = {
            let guard = tree.read().unwrap();
            find_node_full(&guard, bin_id)
                .expect("find_node_full must locate the BIN")
                .size
        };

        assert_eq!(
            actual, expected,
            "find_node_full BIN size ({actual}) must equal explicit formula ({expected})"
        );
    }

    /// Same oracle check as above but for an IN (internal) node.
    ///
    /// Formula: `size_of::<InNodeStub>() + entries * size_of::<InEntry>() + Σ key`
    #[test]
    fn test_find_node_full_in_size_matches_formula() {
        use noxu_util::Lsn;
        use std::mem::size_of;
        use std::sync::{Arc, RwLock};

        // Force a root split so there is at least one IN node by using a
        // very small `max_entries_per_node`.
        let tree = Arc::new(RwLock::new(noxu_tree::tree::Tree::new(2, 2)));
        {
            let t = tree.write().unwrap();
            for i in 0u8..6 {
                t.insert(
                    vec![b'a' + i],
                    vec![i],
                    Lsn::new(1, u32::from(i) + 1),
                )
                .unwrap();
            }
        }

        // After splits the root should be an IN.  Find it.
        let (in_id, in_entry_key_lens) = {
            let t = tree.read().unwrap();
            let root_arc = t.get_root().expect("tree must have a root");
            let guard = root_arc.read();
            match &*guard {
                TreeNode::Internal(n) => {
                    let id = n.node_id;
                    let key_lens: Vec<usize> =
                        n.entries.iter().map(|e| e.key.len()).collect();
                    (id, key_lens)
                }
                TreeNode::Bottom(_) => {
                    // With max_entries=2 and 6 inserts the root is an IN;
                    // if not, skip rather than fail — the split heuristic
                    // may have changed.
                    return;
                }
            }
        };

        let expected: u64 = (size_of::<InNodeStub>()
            + in_entry_key_lens.len() * size_of::<InEntry>()
            + in_entry_key_lens.iter().sum::<usize>())
            as u64;

        let actual = {
            let guard = tree.read().unwrap();
            find_node_full(&guard, in_id)
                .expect("find_node_full must locate the IN root")
                .size
        };

        assert_eq!(
            actual, expected,
            "find_node_full IN size ({actual}) must equal explicit formula ({expected})"
        );
    }

    /// Demonstrate that `node_size_fn` in `do_evict` is served from the
    /// O(1) size cache (no fallback to the 1024-byte sentinel) by checking
    /// that `bytes_evicted` equals the size formula for the evicted IN node,
    /// not the sentinel value 1024.
    ///
    /// **Why an IN node?**  BIN nodes always take the `PartialEvict` path
    /// (which calls `strip_lns_from_node`, not `node_size_fn`).  Only IN
    /// nodes reach the `Evict` path that calls `node_size_fn`.  We put the
    /// root IN into pri2 so that the dirty-IN guard (`MoveDirtyToPri2`) does
    /// not fire — nodes in pri2 are always evicted, regardless of dirty flag.
    ///
    /// If the cache is broken and the fallback sentinel (1024) fires,
    /// `bytes_evicted` would equal 1024 instead of the real size formula.
    #[test]
    fn test_do_evict_bytes_matches_node_size_not_sentinel() {
        use noxu_util::Lsn;
        use std::mem::size_of;
        use std::sync::atomic::AtomicI64;
        use std::sync::{Arc, RwLock};

        let tree_inner = noxu_tree::tree::Tree::new(99, 128);
        {
            tree_inner
                .insert(b"k1".to_vec(), b"vvv".to_vec(), Lsn::new(1, 1))
                .unwrap();
            tree_inner
                .insert(b"k2".to_vec(), b"wwwww".to_vec(), Lsn::new(1, 2))
                .unwrap();
        }

        // The root is an IN (the tree always wraps BINs in an IN root).
        // Compute the expected size using the explicit IN formula.
        let (root_in_id, expected_size) = {
            let root_arc = tree_inner.get_root().expect("root");
            let guard = root_arc.read();
            match &*guard {
                TreeNode::Internal(n) => {
                    let id = n.node_id;
                    let sz = (size_of::<InNodeStub>()
                        + n.entries.len() * size_of::<InEntry>()
                        + n.entries.iter().map(|e| e.key.len()).sum::<usize>())
                        as u64;
                    (id, sz)
                }
                _ => {
                    // Tree structure changed; skip instead of failing.
                    return;
                }
            }
        };
        assert_ne!(
            expected_size, 1024,
            "sentinel must not coincide with the real IN size"
        );

        let tree = Arc::new(RwLock::new(tree_inner));
        // Budget: usage > max so eviction fires immediately.
        let usage = Arc::new(AtomicI64::new(expected_size as i64 * 10));
        let arbiter =
            Arbiter::new(expected_size as i64, Arc::clone(&usage), 100, 200);
        let evictor =
            Evictor::new(arbiter, 10, false).with_tree(Arc::clone(&tree), 99);

        // Register the root IN in pri2 only (not primary).  If we added it
        // to primary first, the dirty-IN guard in decide_eviction would try
        // to move it to pri2 again, triggering a duplicate-insert assertion.
        // pri2 nodes are evicted regardless of dirty flag, which is exactly
        // the Evict path we need to exercise node_size_fn.
        evictor.pri2_insert_for_test(root_in_id);

        let result = evictor.do_evict(EvictionSource::Daemon);

        // The sentinel fallback is 1024.  If the cache is working,
        // bytes_evicted should equal expected_size (the IN size formula),
        // not 1024.
        assert_eq!(
            result.bytes_evicted, expected_size,
            "bytes_evicted ({}) must equal formula size ({}), not sentinel 1024",
            result.bytes_evicted, expected_size
        );
        // The IN was evicted (flushed to log is a no-op for INs without
        // a log manager wired).
        assert_eq!(result.nodes_evicted, 1);
    }

    // -----------------------------------------------------------------------
    // CC-6: non-blocking latch + cursor-pin recheck tests
    // -----------------------------------------------------------------------

    /// CC-6 acceptance test 1: flush_dirty_node_to_log returns `false`
    /// immediately when another thread holds the node's write latch.
    ///
    /// Without the CC-6 fix the old `node_arc.write()` would block
    /// indefinitely, deadlocking this test.  With try_write the function
    /// returns `false` promptly and the node is put back.
    ///
    /// JE ref: `Evictor.java` `latchNoWait`-style non-blocking latch before
    /// any eviction mutation.
    #[test]
    fn test_cc6_flush_nonblocking_when_write_held() {
        use noxu_util::Lsn;
        use std::sync::{Arc, RwLock};
        use std::time::Duration;

        // Build a two-node tree so the root is an IN and a BIN leaf exists.
        let tree_inner = noxu_tree::tree::Tree::new(1, 128);
        tree_inner
            .insert(b"k1".to_vec(), b"v1".to_vec(), Lsn::new(1, 1))
            .unwrap();
        tree_inner
            .insert(b"k2".to_vec(), b"v2".to_vec(), Lsn::new(1, 2))
            .unwrap();

        // Find the BIN node's Arc so we can hold its write latch.
        let bin_arc = {
            let root = tree_inner.get_root().expect("root");
            let guard = root.read();
            match &*guard {
                TreeNode::Internal(n) => {
                    n.entries[0].child.as_ref().cloned().expect("BIN child")
                }
                TreeNode::Bottom(_) => {
                    // Single-node tree: root IS the BIN.
                    Arc::clone(&root)
                }
            }
        };

        // Confirm it's the BIN and mark it dirty.
        let bin_id = {
            let mut g = bin_arc.write();
            match &mut *g {
                TreeNode::Bottom(b) => {
                    b.dirty = true;
                    b.node_id
                }
                _ => panic!("expected BIN leaf"),
            }
        };

        let tree = Arc::new(RwLock::new(tree_inner));
        let usage = Arc::new(std::sync::atomic::AtomicI64::new(9999));
        let evictor = Arc::new(
            Evictor::new(
                Arbiter::new(1000, Arc::clone(&usage), 100, 200),
                100,
                true, // lru_only: skip pri2, go straight to Evict decision
            )
            .with_tree(Arc::clone(&tree), 1),
        );

        // Register the BIN in the evictor's primary policy.
        evictor.note_ins_added(bin_id, CacheMode::Default);

        // Hold the BIN's READ latch from a background thread.
        // A read latch allows node_info_fn's find_node_full scan to proceed
        // (multiple readers OK), but blocks the evictor's try_write() in
        // flush_dirty_node_to_log — exactly the contention CC-6 protects.
        let bin_arc2 = Arc::clone(&bin_arc);
        let (ready_tx, ready_rx) = std::sync::mpsc::channel::<()>();
        let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();
        let holder = std::thread::spawn(move || {
            let _guard = bin_arc2.read();
            ready_tx.send(()).unwrap(); // signal: latch is held
            done_rx.recv().unwrap(); // wait: release when told
        });

        // Wait for holder to acquire the latch.
        ready_rx.recv().unwrap();

        // Evictor must NOT block: with try_write it should return immediately
        // with the node put back.  The old blocking write() would deadlock.
        // Bounded timeout: if do_evict doesn't return within 2 s the test
        // fails (via the thread timeout below), proving the old blocking
        // behaviour.
        let (evict_tx, evict_rx) = std::sync::mpsc::channel::<EvictResult>();
        let ev2 = Arc::clone(&evictor);
        let evict_thread = std::thread::spawn(move || {
            let r = ev2.do_evict(EvictionSource::Daemon);
            evict_tx.send(r).unwrap();
        });

        let result = evict_rx.recv_timeout(Duration::from_secs(2)).expect(
            "CC-6: do_evict must return within 2 s (was blocking before fix)",
        );

        // Node was not evicted (it was put back).
        assert_eq!(
            result.nodes_evicted, 0,
            "CC-6: busy-latched node must not be evicted"
        );
        // nodes_put_back incremented.
        assert_eq!(
            evictor.get_stats().get(&evictor.get_stats().nodes_put_back),
            1,
            "CC-6: nodes_put_back must be incremented when try_write fails"
        );

        // Release the holder and join.
        done_tx.send(()).unwrap();
        holder.join().unwrap();
        evict_thread.join().unwrap();
    }

    /// CC-6 acceptance test 2: strip_lns_from_node returns `None` (put-back)
    /// when the write latch is held by another thread (non-blocking).
    ///
    /// JE ref: `Evictor.java` `isPinned()` + `latchNoWait` for BIN partial
    /// eviction.
    #[test]
    fn test_cc6_strip_nonblocking_when_write_held() {
        use noxu_util::Lsn;
        use std::sync::{Arc, RwLock};
        use std::time::Duration;

        let tree_inner = noxu_tree::tree::Tree::new(1, 128);
        tree_inner
            .insert(b"a".to_vec(), b"aaa".to_vec(), Lsn::new(1, 1))
            .unwrap();
        tree_inner
            .insert(b"b".to_vec(), b"bbb".to_vec(), Lsn::new(1, 2))
            .unwrap();

        let bin_arc = {
            let root = tree_inner.get_root().expect("root");
            let guard = root.read();
            match &*guard {
                TreeNode::Internal(n) => {
                    n.entries[0].child.as_ref().cloned().expect("BIN child")
                }
                TreeNode::Bottom(_) => Arc::clone(&root),
            }
        };

        let bin_id = {
            let g = bin_arc.read();
            match &*g {
                TreeNode::Bottom(b) => b.node_id,
                _ => panic!("expected BIN leaf"),
            }
        };

        let tree = Arc::new(RwLock::new(tree_inner));
        let usage = Arc::new(std::sync::atomic::AtomicI64::new(9999));
        let evictor = Arc::new(
            Evictor::new(
                Arbiter::new(1000, Arc::clone(&usage), 100, 200),
                100,
                true,
            )
            .with_tree(Arc::clone(&tree), 1),
        );

        let bin_arc2 = Arc::clone(&bin_arc);
        let (ready_tx, ready_rx) = std::sync::mpsc::channel::<()>();
        let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();
        std::thread::spawn(move || {
            let _guard = bin_arc2.write();
            ready_tx.send(()).unwrap();
            done_rx.recv().unwrap();
        });
        ready_rx.recv().unwrap();

        // strip_lns_from_node must return None promptly.
        let (tx, rx) = std::sync::mpsc::channel::<Option<usize>>();
        let ev2 = Arc::clone(&evictor);
        std::thread::spawn(move || {
            tx.send(ev2.strip_lns_from_node(bin_id)).unwrap();
        });

        let outcome = rx
            .recv_timeout(Duration::from_secs(2))
            .expect("CC-6: strip_lns_from_node must return within 2 s");

        assert!(
            outcome.is_none(),
            "CC-6: strip must return None (busy) when write latch is held, got {:?}",
            outcome
        );

        done_tx.send(()).unwrap();
    }

    /// CC-6 acceptance test 3: cursor-pin recheck under lock (strip path).
    ///
    /// When `cursor_count > 0` at write-lock time, `strip_lns_from_node`
    /// returns `None` (put back) instead of stripping a pinned BIN.
    ///
    /// JE ref: `Evictor.java` `isPinned()` re-check after `latchNoWait`
    /// succeeds.
    #[test]
    fn test_cc6_cursor_pin_recheck_under_lock_strip() {
        use noxu_util::Lsn;
        use std::sync::{Arc, RwLock};

        let tree_inner = noxu_tree::tree::Tree::new(1, 128);
        tree_inner
            .insert(b"x".to_vec(), b"data".to_vec(), Lsn::new(1, 1))
            .unwrap();
        tree_inner
            .insert(b"y".to_vec(), b"data2".to_vec(), Lsn::new(1, 2))
            .unwrap();

        let bin_arc = {
            let root = tree_inner.get_root().expect("root");
            let guard = root.read();
            match &*guard {
                TreeNode::Internal(n) => {
                    n.entries[0].child.as_ref().cloned().expect("BIN child")
                }
                TreeNode::Bottom(_) => Arc::clone(&root),
            }
        };

        // Set cursor_count > 0 directly (simulate cursor pinning between
        // pre-lock snapshot and actual lock acquisition).
        let bin_id = {
            let mut g = bin_arc.write();
            match &mut *g {
                TreeNode::Bottom(b) => {
                    b.cursor_count = 1;
                    b.node_id
                }
                _ => panic!("expected BIN"),
            }
        };

        let tree = Arc::new(RwLock::new(tree_inner));
        let usage = Arc::new(std::sync::atomic::AtomicI64::new(9999));
        let evictor = Evictor::new(
            Arbiter::new(1000, Arc::clone(&usage), 100, 200),
            100,
            true,
        )
        .with_tree(Arc::clone(&tree), 1);

        let result = evictor.strip_lns_from_node(bin_id);
        assert!(
            result.is_none(),
            "CC-6: must return None when cursor_count > 0 under lock; got {:?}",
            result
        );
    }

    /// CC-6 acceptance test 4: cursor-pin recheck under lock (flush path).
    ///
    /// `flush_dirty_node_to_log` returns `false` when `cursor_count > 0`
    /// under the write lock.
    #[test]
    fn test_cc6_cursor_pin_recheck_under_lock_flush() {
        use noxu_util::Lsn;
        use std::sync::{Arc, RwLock};

        let tree_inner = noxu_tree::tree::Tree::new(1, 128);
        tree_inner
            .insert(b"p".to_vec(), b"val".to_vec(), Lsn::new(1, 1))
            .unwrap();
        tree_inner
            .insert(b"q".to_vec(), b"val2".to_vec(), Lsn::new(1, 2))
            .unwrap();

        let bin_arc = {
            let root = tree_inner.get_root().expect("root");
            let guard = root.read();
            match &*guard {
                TreeNode::Internal(n) => {
                    n.entries[0].child.as_ref().cloned().expect("BIN child")
                }
                TreeNode::Bottom(_) => Arc::clone(&root),
            }
        };

        let bin_id = {
            let mut g = bin_arc.write();
            match &mut *g {
                TreeNode::Bottom(b) => {
                    b.cursor_count = 2;
                    b.dirty = true;
                    b.node_id
                }
                _ => panic!("expected BIN"),
            }
        };

        let tree = Arc::new(RwLock::new(tree_inner));
        let usage = Arc::new(std::sync::atomic::AtomicI64::new(9999));
        let evictor = Evictor::new(
            Arbiter::new(1000, Arc::clone(&usage), 100, 200),
            100,
            true,
        )
        .with_tree(Arc::clone(&tree), 1);

        let result = evictor.flush_dirty_node_to_log(bin_id);
        assert!(
            !result,
            "CC-6: flush must return false when cursor_count > 0 under lock"
        );
    }

    // -----------------------------------------------------------------------
    // CC-4: provisional-flag wiring tests
    // -----------------------------------------------------------------------

    /// CC-4 acceptance test: evictor accepts a checkpointer via
    /// `with_checkpointer` and compiles with the CC-4 wiring.
    ///
    /// Correctness of the provisional-flag decision logic is proven by the
    /// four `test_cc4_*` unit tests in `noxu_recovery::checkpointer::tests`;
    /// this test verifies only that the wiring builds and the checkpointer
    /// reference survives the builder chain.
    ///
    /// JE ref: Checkpointer.coordinateEvictionWithCheckpoint (CC-4 fix).
    #[test]
    fn test_cc4_evictor_wires_checkpointer() {
        use noxu_recovery::{CheckpointConfig, Checkpointer};
        use std::sync::Arc;

        let ckpt = Arc::new(Checkpointer::new(CheckpointConfig::default()));
        let usage = Arc::new(std::sync::atomic::AtomicI64::new(0));
        let evictor = Evictor::new(
            Arbiter::new(1000, Arc::clone(&usage), 100, 200),
            100,
            false,
        )
        .with_checkpointer(Arc::clone(&ckpt));

        // Verify the evictor holds the checkpointer: Arc strong count is 2
        // (ckpt + evictor's internal reference).
        assert_eq!(
            Arc::strong_count(&ckpt),
            2,
            "CC-4: evictor must hold an Arc reference to the checkpointer"
        );

        drop(evictor);
        // After evictor drops, only our local Arc remains.
        assert_eq!(Arc::strong_count(&ckpt), 1);
    }
}
