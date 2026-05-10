//! Main evictor implementation.
//!

// ---------------------------------------------------------------------------
// CLUSTER-B-WIRING: environment_impl.rs must call the following after
// constructing the Evictor to wire in the real tree and log-manager:
//
//   // In EnvironmentImpl::new() (or equivalent builder), after building
//   // the Evictor:
//   let evictor = Arc::new(
//       Evictor::new(arbiter, max_batch_size, lru_only)
//           .with_log_manager(Arc::clone(&log_manager))   // <-- C-3 / H-1
//           .with_tree(Arc::clone(&primary_tree), db_id), // <-- C-3 / H-1
//   );
//
//   // Store the Arc<Evictor> on EnvironmentImpl as `evictor`.
//
// The LogManager and Tree Arcs must already exist before the Evictor is
// constructed.  Wiring order:  FileManager → LogManager → Tree → Evictor.
// ---------------------------------------------------------------------------

use crate::arbiter::Arbiter;
use crate::cache_mode::CacheMode;
use crate::evictor_stat::EvictorStats;
use crate::lru_list::LruList;
use noxu_log::entry::in_log_entry::InLogEntry;
use noxu_log::{LogEntryType, LogManager, Provisional};
use noxu_tree::tree::{BinEntry, BinStub, InEntry, InNodeStub, Tree, TreeNode};
use noxu_util::NULL_LSN;
use crate::off_heap::OffHeapCache;
use std::mem::size_of;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

/// Source of an eviction operation.
///
/// Different eviction sources have different priorities and behaviors.
/// Statistics are tracked separately for each source.
///
/// 
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvictionSource {
    /// Eviction triggered by background daemon threads (evictor pool threads).
    Daemon,

    /// Critical eviction triggered in application threads when cache is
    /// severely over budget.
    Critical,

    /// Manual eviction requested via API (Environment.evictMemory).
    Manual,

    /// Eviction triggered by CacheMode settings (EVICT_LN, EVICT_BIN).
    CacheMode,
}

/// Result of an eviction run.
///
/// Tracks how many nodes and bytes were evicted during an eviction operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EvictResult {
    /// Number of nodes evicted.
    pub nodes_evicted: u64,

    /// Number of bytes evicted.
    pub bytes_evicted: u64,
}

impl EvictResult {
    /// Create a new EvictResult with zero counts.
    pub fn zero() -> Self {
        Self { nodes_evicted: 0, bytes_evicted: 0 }
    }

    /// Create a new EvictResult.
    pub fn new(nodes_evicted: u64, bytes_evicted: u64) -> Self {
        Self { nodes_evicted, bytes_evicted }
    }

    /// Accumulate another result into this one.
    pub fn add(&mut self, other: &EvictResult) {
        self.nodes_evicted += other.nodes_evicted;
        self.bytes_evicted += other.bytes_evicted;
    }
}

// ---------------------------------------------------------------------------
// NodeEvictionInfo trait
// ---------------------------------------------------------------------------

/// Information the evictor needs about a cached node in order to decide
/// whether (and how) to evict it.
///
/// This trait is the boundary between the evictor and the B-tree layer.
/// The evictor never holds direct references to tree nodes; callers supply
/// a short-lived implementation of this trait so the evictor can apply its
/// decision tree without creating a circular-dependency between crates.
///
/// Mirrors the per-node state that `processTarget()` inspects before
/// taking action on an eviction target:
/// - `isPinned()` / cursor-count → `ref_count`
/// - `getDirty()` → `is_dirty`
/// - `isBIN()` → `is_bin`
/// - `getInListResident()` / `hasCachedChildren()` → `is_resident`
pub trait NodeEvictionInfo {
    /// Returns true when the node's dirty flag is set (needs to be logged
    /// before it can be truly removed from memory).
    fn is_dirty(&self) -> bool;

    /// Returns true when the node is a BIN (Bottom Internal Node), which may
    /// contain evictable LN children that can be stripped before evicting the
    /// BIN itself.
    fn is_bin(&self) -> bool;

    /// Returns true when the node is still present in the in-memory tree and
    /// has not already been evicted by another thread.
    fn is_resident(&self) -> bool;

    /// Reference count: number of active cursors or other pins on this node.
    /// A value of `0` means the node is not actively in use and is safe to
    /// evict (subject to other checks).
    fn ref_count(&self) -> usize;
}

// ---------------------------------------------------------------------------
// EvictionDecision enum
// ---------------------------------------------------------------------------

/// Decision made by the evictor's decision tree for a single target node.
///
/// Mirrors the outcomes enumerated in the equivalent `processTarget()` Javadoc:
///
/// | name              | Rust variant      | Meaning |
/// |----------------------|-------------------|---------|
/// | SKIP                 | `Skip`            | Leave node alone; another thread already acted on it |
/// | PUT_BACK             | `PutBack`         | Return node to the LRU it came from unchanged |
/// | PARTIAL_EVICT        | `PartialEvict`    | Strip child LNs from a BIN, then put BIN back |
/// | MOVE_DIRTY_TO_PRI2   | `MoveDirtyToPri2` | Node is dirty — defer to pri2 so checkpointer can log it |
/// | EVICT                | `Evict`           | Remove the node from the LRU / cache entirely |
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvictionDecision {
    /// Cannot act on this node: it is no longer in the resident set (already
    /// evicted by another thread or path), or it is pinned in a way that
    /// prevents any action.
    Skip,

    /// Return the node to the back of the LRU list it originally came from.
    /// No bytes are freed. Used when the node is pinned (active cursors) or
    /// when some other condition prevents eviction.
    PutBack,

    /// Move the node to the front of the priority-2 (dirty) LRU list.
    /// The checkpointer will log it and then call
    /// `complete_checkpoint_for_node()` to promote it back to pri1, at which
    /// point it becomes eligible for a clean eviction.
    ///
    /// Only applied when `lru_only` is false and the node was in pri1.
    MoveDirtyToPri2,

    /// The node is a BIN with resident LN children that can be stripped
    /// (evicted from the BIN) without evicting the BIN itself. After
    /// stripping, the BIN is put back into the LRU.
    PartialEvict,

    /// Fully evict the node from the cache.
    Evict,
}

// ---------------------------------------------------------------------------
// Decision tree helper
// ---------------------------------------------------------------------------

/// Apply `processTarget()` decision tree to a node described by
/// `info`, and return the appropriate `EvictionDecision`.
///
/// # Decision tree (mirrors `processTarget()`)
///
/// 1. `!is_resident` → `Skip`  (evicted by another thread)
/// 2. `ref_count > 0` → `PutBack`  (pinned / active cursors)
/// 3. `is_bin` → `PartialEvict`  (strip LNs first)
/// 4. `is_dirty && !already_in_pri2 && use_dirty_lru` → `MoveDirtyToPri2`
/// 5. otherwise → `Evict`
///
/// The `already_in_pri2` and `use_dirty_lru` parameters let the caller
/// convey LRU-level context that is not visible through `NodeEvictionInfo`.
pub fn decide_eviction(
    info: &dyn NodeEvictionInfo,
    already_in_pri2: bool,
    use_dirty_lru: bool,
) -> EvictionDecision {
    // 1. Node is no longer in the resident in-memory set.
    if !info.is_resident() {
        return EvictionDecision::Skip;
    }

    // 2. Node is pinned (active cursor or other holder).
    if info.ref_count() > 0 {
        return EvictionDecision::PutBack;
    }

    // 3. BIN with potentially evictable LN children → strip first.
    if info.is_bin() {
        return EvictionDecision::PartialEvict;
    }

    // 4. Dirty node in pri1: give it a second chance by moving to pri2 so
    //    the checkpointer can log it before we evict it cold.
    if use_dirty_lru && info.is_dirty() && !already_in_pri2 {
        return EvictionDecision::MoveDirtyToPri2;
    }

    // 5. Clean (or already-in-pri2 dirty) node: evict it.
    EvictionDecision::Evict
}

// ---------------------------------------------------------------------------
// Evictor struct
// ---------------------------------------------------------------------------

/// The main evictor that manages cache eviction.
///
/// The Evictor is responsible for:
/// - Tracking nodes in LRU lists
/// - Selecting eviction candidates
/// - Performing eviction when memory budget is exceeded
/// - Collecting eviction statistics
///
/// The evictor can be invoked from multiple threads:
/// - Background daemon threads (evictor pool)
/// - Application threads (critical eviction)
/// - Manual eviction requests
///
/// 
pub struct Evictor {
    /// Arbiter for determining when eviction is needed.
    arbiter: Arbiter,

    /// LRU lists for tracking eviction candidates.
    lru: LruList,

    /// Statistics tracking.
    stats: EvictorStats,

    /// Shutdown flag.
    shutdown: AtomicBool,

    /// Maximum number of nodes to process in a single eviction batch.
    max_batch_size: usize,

    /// If true, only use LRU-based eviction (no special handling for dirty nodes).
    lru_only: bool,

    /// Cumulative count of nodes evicted from the priority-1 LRU list.
    ///
    /// uses an array of pri1 LRU lists and round-robins across them using
    /// this index.  Since Noxu uses a single combined LRU list per priority,
    /// this counter is used as a monotonic eviction counter for pri1 nodes,
    /// matching `next_pri1_index` semantics.
    ///
    /// 
    next_pri1_index: AtomicU64,

    /// Cumulative count of nodes evicted from the priority-2 LRU list.
    ///
    /// 
    next_pri2_index: AtomicU64,

    /// Optional LogManager for flushing dirty nodes to the WAL before
    /// eviction.  Wired by `with_log_manager()`.
    ///
    /// `Evictor.envImpl.getLogManager()` reference used inside
    /// `evict()` when `target.getDirty()` is true.
    log_manager: Option<Arc<LogManager>>,

    /// Optional B-tree reference.  Required for the real node-info and
    /// node-size callbacks (`do_evict`) and for dirty-write-before-eviction.
    /// Wired by `with_tree()`.
    ///
    /// Per-IN lookup.
    tree: Option<Arc<RwLock<Tree>>>,

    /// Database ID associated with `tree`.  Required to identify which BINs
    /// belong to this database when calling `collect_dirty_bins`.
    db_id: u64,

    /// Optional off-heap cache for upper-IN nodes.
    ///
    /// JE `OffHeapAllocator`: when an upper-IN is evicted from the main
    /// cache it is serialised and placed here.  On the next fetch the
    /// in-memory representation is reconstructed from off-heap bytes rather
    /// than performing a log-file read.
    off_heap: Option<Arc<OffHeapCache>>,
}

impl Evictor {
    /// Create a new Evictor.
    ///
    /// # Arguments
    /// * `arbiter` - Arbiter for determining eviction needs
    /// * `max_batch_size` - Maximum nodes to evict in one batch
    /// * `lru_only` - If true, use simple LRU without priority lists
    pub fn new(
        arbiter: Arbiter,
        max_batch_size: usize,
        lru_only: bool,
    ) -> Self {
        Self {
            arbiter,
            lru: LruList::new(),
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
        }
    }

    // -----------------------------------------------------------------------
    // Optional wiring builders
    // -----------------------------------------------------------------------

    /// Wire a `LogManager` into the evictor so that dirty nodes are written to
    /// the WAL before they are removed from memory.
    ///
    /// Mirrors the same pattern used by `Checkpointer::with_log_manager`.
    ///
    /// `Evictor.envImpl.getLogManager()` used inside `evict()`.
    pub fn with_log_manager(mut self, lm: Arc<LogManager>) -> Self {
        self.log_manager = Some(lm);
        self
    }

    /// Wire the B-tree and database ID into the evictor so that `do_evict()`
    /// can inspect real `TreeNode` metadata and flush dirty BINs.
    ///
    /// Mirrors the same pattern used by `Checkpointer::with_tree`.
    ///
    /// Per-IN lookup.
    pub fn with_tree(mut self, tree: Arc<RwLock<Tree>>, db_id: u64) -> Self {
        self.tree = Some(tree);
        self.db_id = db_id;
        self
    }

    /// Wire an off-heap cache into the evictor.
    ///
    /// When wired, evicted upper-IN nodes (level > 1) are serialised and
    /// placed in the off-heap store.  On the next tree fetch those INs are
    /// reconstructed from off-heap bytes rather than from the log file.
    ///
    /// Mirrors JE `OffHeapCache` integration in `Evictor.doEvict()`.
    pub fn with_off_heap(mut self, cache: Arc<OffHeapCache>) -> Self {
        self.off_heap = Some(cache);
        self
    }

    // -----------------------------------------------------------------------
    // evict_batch — the real batch eviction loop
    // -----------------------------------------------------------------------

    /// Execute one eviction batch.
    ///
    /// Mirrors `evictBatch()`:
    ///
    /// 1. Drain priority-1 LRU up to `max_batch_size` nodes.
    /// 2. For each node, call `decide_eviction()` via the supplied callback.
    /// 3. Act on the decision: skip, put-back, partial-evict, move-to-pri2,
    ///    or evict.
    /// 4. Once pri1 is exhausted (or the batch limit is hit), switch to pri2
    ///    and repeat.
    /// 5. Stop when the arbiter says budget is satisfied.
    ///
    /// # Arguments
    /// * `source` — what triggered this eviction run
    /// * `node_info_fn` — callback: given a node id, returns a boxed
    ///   `NodeEvictionInfo` describing that node.  Returns `None` when the
    ///   node is not known (treat as `Skip`).
    /// * `node_size_fn` — callback: given a node id, returns the number of
    ///   heap bytes that would be freed by evicting it.
    ///
    /// # Returns
    /// `EvictResult` with cumulative counts for this batch.
    pub fn evict_batch(
        &self,
        _source: EvictionSource,
        node_info_fn: &dyn Fn(u64) -> Option<Box<dyn NodeEvictionInfo>>,
        node_size_fn: &dyn Fn(u64) -> u64,
    ) -> EvictResult {
        let mut result = EvictResult::zero();
        let mut nodes_processed = 0usize;

        // Phase 1: drain pri1 LRU, then switch to pri2 if still needed.
        // initialises maxNodesScanned to the current pri1 size and
        // transitions to pri2 when that many nodes have been scanned.
        let mut in_pri1 = true;
        let pri1_quota = self.lru.len();

        loop {
            if nodes_processed >= self.max_batch_size {
                break;
            }
            if !self.arbiter.still_needs_eviction() {
                break;
            }

            // Pick the next candidate from the appropriate priority list.
            let (node_id, from_pri2) = if in_pri1 {
                match self.lru.remove_front() {
                    Some(id) => (id, false),
                    None => {
                        // Pri1 exhausted; fall through to pri2 if allowed.
                        if self.lru_only {
                            break;
                        }
                        in_pri1 = false;
                        match self.lru.pri2_remove_front() {
                            Some(id) => (id, true),
                            None => break,
                        }
                    }
                }
            } else {
                match self.lru.pri2_remove_front() {
                    Some(id) => (id, true),
                    None => break,
                }
            };

            nodes_processed += 1;
            self.stats.increment(&self.stats.nodes_targeted);
            // Track which priority tier this node was pulled from.
            if from_pri2 {
                self.next_pri2_index.fetch_add(1, Ordering::Relaxed);
            } else {
                self.next_pri1_index.fetch_add(1, Ordering::Relaxed);
            }

            // Obtain node metadata via callback.
            let info = match node_info_fn(node_id) {
                Some(i) => i,
                None => {
                    // Node not known — treat as already evicted.
                    self.stats.increment(&self.stats.nodes_skipped);
                    // Transition to pri2 after pri1_quota nodes in pri1.
                    if in_pri1 && nodes_processed >= pri1_quota {
                        in_pri1 = false;
                    }
                    continue;
                }
            };

            let use_dirty_lru = !self.lru_only;
            let decision = decide_eviction(info.as_ref(), from_pri2, use_dirty_lru);

            match decision {
                EvictionDecision::Skip => {
                    // Leave the node out of the LRU (already gone).
                    self.stats.increment(&self.stats.nodes_skipped);
                }

                EvictionDecision::PutBack => {
                    // Return to the back of whichever list it came from.
                    if from_pri2 {
                        self.lru.pri2_add_back(node_id);
                    } else {
                        self.lru.add_back(node_id);
                    }
                    self.stats.increment(&self.stats.nodes_put_back);
                }

                EvictionDecision::PartialEvict => {
                    // Strip LNs from the BIN, then put the BIN back.
                    // The actual stripping is performed by the tree layer;
                    // here we account for whatever bytes the callback says
                    // were freed and put the BIN back in its LRU.
                    let freed = node_size_fn(node_id);
                    if freed > 0 {
                        result.bytes_evicted += freed;
                        self.stats.increment(&self.stats.nodes_stripped);
                        self.stats.increment(&self.stats.lns_evicted);
                    }
                    // BIN itself goes back to the appropriate LRU.
                    if from_pri2 {
                        self.lru.pri2_add_back(node_id);
                    } else {
                        self.lru.add_back(node_id);
                    }
                }

                EvictionDecision::MoveDirtyToPri2 => {
                    // calls pri2AddFront (cold end) so the checkpointer
                    // encounters dirty nodes quickly.
                    self.lru.pri2_add_front(node_id);
                    self.stats.increment(&self.stats.nodes_moved_to_pri2_lru);
                }

                EvictionDecision::Evict => {
                    // JE: if (target.getDirty() && !storedOffHeap) log to WAL
                    // then parent.detachNode(index, logged, loggedLsn).
                    //
                    // Off-heap path: if an off-heap cache is wired and the
                    // node is an upper IN (level > 1), serialise it into the
                    // off-heap store.  This avoids a log-file read on the
                    // next tree traversal that needs this IN.
                    let mut stored_off_heap = false;
                    if let (Some(oh), Some(tree_arc)) = (&self.off_heap, &self.tree) {
                        if oh.is_enabled() {
                            if let Ok(tree_guard) = tree_arc.read() {
                                if let Some(serialized) =
                                    tree_guard.serialize_upper_in(node_id)
                                {
                                    stored_off_heap = oh.store_node(node_id, serialized);
                                }
                            }
                        }
                    }

                    // Only flush dirty nodes to log if NOT stored off-heap
                    // (JE: !storedOffHeap check before logging).
                    if info.is_dirty() && !stored_off_heap {
                        self.flush_dirty_node_to_log(node_id);
                    }

                    // Full eviction: account for freed bytes, update counters.
                    let freed = node_size_fn(node_id);
                    result.bytes_evicted += freed;
                    result.nodes_evicted += 1;
                    self.stats.increment(&self.stats.nodes_evicted);
                    // dirty_nodes_evicted is incremented inside
                    // flush_dirty_node_to_log when a WAL write succeeds;
                    // here we only count clean-node evictions explicitly via
                    // nodes_evicted above.
                }
            }

            // After scanning pri1_quota nodes in pri1, switch to pri2.
            if in_pri1 && nodes_processed >= pri1_quota {
                if self.lru_only {
                    break;
                }
                in_pri1 = false;
            }
        }

        result
    }

    // -----------------------------------------------------------------------
    // do_evict — public entry point, wires into evict_batch
    // -----------------------------------------------------------------------

    /// Perform an eviction run.
    ///
    /// This is the main eviction entry point that:
    /// 1. Checks if eviction is needed via the arbiter.
    /// 2. If a tree is wired via `with_tree()`, uses real node-info/size
    ///    callbacks backed by live `TreeNode` data (H-1 fix).  Otherwise
    ///    falls back to the default stub callbacks suitable for unit testing.
    /// 3. For each node selected for full `Evict`, flushes the node to the WAL
    ///    first if it is dirty and a `LogManager` is wired (C-3 fix).
    /// 4. Updates source-specific byte statistics.
    ///
    /// # Arguments
    /// * `source` - Source of the eviction request
    ///
    /// # Returns
    /// Result containing nodes and bytes evicted.
    pub fn do_evict(&self, source: EvictionSource) -> EvictResult {
        if let Some(tree_arc) = &self.tree {
            // H-1: real callbacks backed by live TreeNode data.
            let tree_clone = Arc::clone(tree_arc);
            let tree_clone2 = Arc::clone(tree_arc);

            let node_info_fn = move |node_id: u64| -> Option<Box<dyn NodeEvictionInfo>> {
                let guard = tree_clone.read().ok()?;
                real_node_info(&guard, node_id)
            };

            let node_size_fn = move |node_id: u64| -> u64 {
                match tree_clone2.read() {
                    Ok(g) => real_node_size(&g, node_id),
                    Err(_) => 1024,
                }
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

    /// Write a dirty node to the WAL before evicting it (C-3 fix).
    ///
    /// `Evictor.evict()`:
    /// ```java
    /// if (target.getDirty() && !storedOffHeap) {
    ///     loggedLsn = target.log(allowBinDeltas, provisional, bgIO, parent);
    ///     logged = true;
    /// }
    /// long evictedBytes = target.getBudgetedMemorySize();
    /// parent.detachNode(index, logged /*updateLsn*/, loggedLsn);
    /// ```
    ///
    /// We always write a full BIN here (not a delta) for correctness — the
    /// checkpointer handles delta optimisation; the evictor only needs
    /// durability.
    fn flush_dirty_node_to_log(&self, node_id: u64) {
        let lm = match &self.log_manager {
            Some(lm) => Arc::clone(lm),
            None => return, // no LogManager wired — skip (e.g. unit tests)
        };
        let tree_arc = match &self.tree {
            Some(t) => Arc::clone(t),
            None => return,
        };

        // Find the node Arc under a read lock (no deadlock risk), then
        // drop the read lock before write-locking the individual node.
        let node_arc: Arc<RwLock<TreeNode>> = {
            let tree_guard = match tree_arc.read() {
                Ok(g) => g,
                Err(_) => return,
            };
            match find_node_arc(&tree_guard, node_id) {
                Some(a) => a,
                None => return,
            }
        }; // tree read lock released here

        let mut node_guard = match node_arc.write() {
            Ok(g) => g,
            Err(_) => return,
        };

        let bin = match &mut *node_guard {
            TreeNode::Bottom(b) => b,
            // Upper INs: dirty-flush is handled by the checkpointer, not here.
            _ => return,
        };

        if !bin.dirty && bin.dirty_count() == 0 {
            return; // already clean
        }

        // Serialize and write a full BIN log entry.
        let full_bytes = bin.serialize_full();
        let entry = InLogEntry::new(
            self.db_id,
            bin.last_full_lsn,
            NULL_LSN, // prev_delta_lsn
            full_bytes,
        );
        let mut buf = bytes::BytesMut::with_capacity(entry.log_size());
        entry.write_to_log(&mut buf);

        if let Ok(logged_lsn) = lm.log(
            LogEntryType::BIN,
            &buf,
            Provisional::No,
            false, // flush_required
            false, // fsync_required — fsync at next checkpoint/commit boundary
        ) {
            // Clear dirty flags and update last_full_lsn.
            // Parent.detachNode(index, updateLsn=true, loggedLsn)
            bin.clear_dirty_after_full_log(logged_lsn);
            self.stats.increment(&self.stats.dirty_nodes_evicted);
        }
        // On log error we leave dirty=true; the node stays in memory (the
        // evictor's LRU removal will happen but the tree still has the data).
    }

    /// Perform an eviction run with caller-supplied node callbacks.
    ///
    /// This variant is intended for integration with the actual B-tree layer
    /// (or for fine-grained unit testing).
    ///
    /// # Arguments
    /// * `source` - Source of the eviction request
    /// * `node_info_fn` - Returns `NodeEvictionInfo` for a given node id
    /// * `node_size_fn` - Returns the byte size of a given node
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

        // Update source-specific byte counters.
        match source {
            EvictionSource::Daemon => {
                self.stats.add(
                    &self.stats.bytes_evicted_daemon,
                    result.bytes_evicted,
                );
            }
            EvictionSource::Critical => {
                self.stats.add(
                    &self.stats.bytes_evicted_critical,
                    result.bytes_evicted,
                );
            }
            EvictionSource::Manual => {
                self.stats.add(
                    &self.stats.bytes_evicted_manual,
                    result.bytes_evicted,
                );
            }
            EvictionSource::CacheMode => {
                self.stats.add(
                    &self.stats.bytes_evicted_cachemode,
                    result.bytes_evicted,
                );
            }
        }

        result
    }

    // -----------------------------------------------------------------------
    // Checkpoint integration
    // -----------------------------------------------------------------------

    /// Called by the checkpointer after a dirty node has been logged.
    ///
    /// In JE, after the checkpointer logs (flushes) a dirty node that was
    /// sitting in the priority-2 LRU, it moves that node back to the priority-1
    /// LRU so it can be evicted cleanly on the next eviction pass.
    ///
    /// This is the Rust equivalent of that promotion path.
    ///
    /// # Arguments
    /// * `node_id` - ID of the node that has just been logged by the checkpointer
    ///
    /// # Returns
    /// `true` if the node was found in pri2 and promoted to pri1.
    pub fn complete_checkpoint_for_node(&self, node_id: u64) -> bool {
        if self.lru.pri2_remove(node_id) {
            // Put it at the back of pri1 (hot end) — it was just written, so
            // it is relatively recently accessed.
            self.lru.add_back(node_id);
            true
        } else {
            false
        }
    }

    // -----------------------------------------------------------------------
    // LRU management helpers
    // -----------------------------------------------------------------------

    /// Note that a node has been added to the cache.
    ///
    /// Adds the node to the appropriate LRU list based on the cache mode.
    ///
    /// # Arguments
    /// * `node_id` - ID of the node being added
    /// * `cache_mode` - Cache mode controlling LRU placement
    pub fn note_ins_added(&self, node_id: u64, cache_mode: CacheMode) {
        if cache_mode.is_cold() {
            self.lru.add_front(node_id);
        } else {
            self.lru.add_back(node_id);
        }
    }

    /// Note that a node has been accessed.
    ///
    /// Updates the node's position in the LRU list based on the cache mode.
    ///
    /// # Arguments
    /// * `node_id` - ID of the node being accessed
    /// * `cache_mode` - Cache mode controlling LRU behavior
    pub fn note_ins_accessed(&self, node_id: u64, cache_mode: CacheMode) {
        if cache_mode.is_hot() {
            self.lru.move_back(node_id);
        } else if cache_mode.is_cold() {
            self.lru.move_front(node_id);
        }
        // If unchanged, don't move
    }

    /// Note that a node has been removed from the cache.
    ///
    /// Removes the node from whichever LRU list it's in.
    ///
    /// # Arguments
    /// * `node_id` - ID of the node being removed
    pub fn note_ins_removed(&self, node_id: u64) {
        self.lru.remove_from_either(node_id);
    }

    /// Move a node from priority-1 to priority-2 LRU list.
    ///
    /// This is used when a clean node becomes dirty and we want to
    /// defer its eviction.
    ///
    /// # Arguments
    /// * `node_id` - ID of the node to move
    pub fn move_to_pri2(&self, node_id: u64) -> bool {
        if self.lru.remove(node_id) {
            self.lru.pri2_add_back(node_id);
            self.stats.increment(&self.stats.nodes_moved_to_pri2_lru);
            true
        } else {
            false
        }
    }

    // -----------------------------------------------------------------------
    // Accessors
    // -----------------------------------------------------------------------

    /// Get a reference to the statistics.
    pub fn get_stats(&self) -> &EvictorStats {
        &self.stats
    }

    /// Returns the cumulative count of nodes evicted from the priority-1 LRU.
    ///
    /// Used for round-robin selection
    /// across multiple pri1 LRU lists; here counts total pri1 evictions).
    pub fn pri1_eviction_count(&self) -> u64 {
        self.next_pri1_index.load(Ordering::Relaxed)
    }

    /// Returns the cumulative count of nodes evicted from the priority-2 LRU.
    ///
    /// 
    pub fn pri2_eviction_count(&self) -> u64 {
        self.next_pri2_index.load(Ordering::Relaxed)
    }

    /// Get the current LRU sizes.
    pub fn get_lru_sizes(&self) -> (usize, usize) {
        (self.lru.len(), self.lru.pri2_len())
    }

    /// Update LRU size statistics.
    pub fn update_lru_stats(&self) {
        let (pri1_size, pri2_size) = self.get_lru_sizes();
        self.stats.set(&self.stats.pri1_lru_size, pri1_size as u64);
        self.stats.set(&self.stats.pri2_lru_size, pri2_size as u64);
    }

    /// Initiate shutdown.
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
    }

    /// Check if shutdown has been requested.
    pub fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::Relaxed)
    }

    /// Get a reference to the arbiter.
    pub fn get_arbiter(&self) -> &Arbiter {
        &self.arbiter
    }
}

impl std::fmt::Debug for Evictor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Evictor")
            .field("max_batch_size", &self.max_batch_size)
            .field("lru_only", &self.lru_only)
            .field("shutdown", &self.shutdown.load(Ordering::Relaxed))
            .field("db_id", &self.db_id)
            .field("log_manager_wired", &self.log_manager.is_some())
            .field("tree_wired", &self.tree.is_some())
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Real node-info / node-size helpers (H-1 fix)
// ---------------------------------------------------------------------------

/// Snapshot of a node's eviction-relevant metadata.
///
/// `IN.getDirty()` / `IN.getBudgetedMemorySize()` / `IN.isBIN()`.
struct RealNodeInfo {
    dirty: bool,
    is_bin: bool,
    /// Number of cursors currently positioned on this node.
    /// `IN.cursorSet.size()` used by `Evictor.selectIN()`.
    pin_count: usize,
}

impl NodeEvictionInfo for RealNodeInfo {
    fn is_dirty(&self) -> bool { self.dirty }
    fn is_bin(&self) -> bool { self.is_bin }
    fn is_resident(&self) -> bool { true } // found in tree → resident
    fn ref_count(&self) -> usize { self.pin_count }
}

/// Walk the tree to find a node by ID and return a `RealNodeInfo` snapshot.
///
/// `selectIN()` / `processTarget()` — we read node metadata under
/// the tree read lock so the evictor does not hold the tree lock across
/// the full eviction decision.
fn real_node_info(tree: &Tree, node_id: u64) -> Option<Box<dyn NodeEvictionInfo>> {
    let root_arc = tree.get_root()?;
    find_node_info_recursive(&root_arc, node_id)
}

fn find_node_info_recursive(
    node_arc: &Arc<RwLock<TreeNode>>,
    target_id: u64,
) -> Option<Box<dyn NodeEvictionInfo>> {
    let guard = node_arc.read().ok()?;
    match &*guard {
        TreeNode::Bottom(b) => {
            if b.node_id == target_id {
                Some(Box::new(RealNodeInfo {
                    dirty: b.dirty || b.dirty_count() > 0,
                    is_bin: true,
                    pin_count: b.cursor_count.max(0) as usize,
                }))
            } else {
                None
            }
        }
        TreeNode::Internal(n) => {
            if n.node_id == target_id {
                return Some(Box::new(RealNodeInfo { dirty: n.dirty, is_bin: false, pin_count: 0 }));
            }
            // Collect child arcs before dropping the guard.
            let children: Vec<Arc<RwLock<TreeNode>>> = n.entries.iter()
                .filter_map(|e| e.child.as_ref().map(Arc::clone))
                .collect();
            drop(guard);
            for child in children {
                if let Some(info) = find_node_info_recursive(&child, target_id) {
                    return Some(info);
                }
            }
            None
        }
    }
}

/// Compute the actual heap size of a node by its ID.
///
/// `IN.getBudgetedMemorySize()`.
fn real_node_size(tree: &Tree, node_id: u64) -> u64 {
    let root_arc = match tree.get_root() {
        Some(r) => r,
        None => return 1024,
    };
    find_node_size_recursive(&root_arc, node_id).unwrap_or(1024)
}

fn find_node_size_recursive(
    node_arc: &Arc<RwLock<TreeNode>>,
    target_id: u64,
) -> Option<u64> {
    let guard = node_arc.read().ok()?;
    match &*guard {
        TreeNode::Bottom(b) => {
            if b.node_id == target_id {
                let sz = size_of::<BinStub>()
                    + b.entries.len() * size_of::<BinEntry>()
                    + b.entries.iter().map(|e| {
                        e.key.len() + e.data.as_ref().map(|d| d.len()).unwrap_or(0)
                    }).sum::<usize>();
                Some(sz as u64)
            } else {
                None
            }
        }
        TreeNode::Internal(n) => {
            if n.node_id == target_id {
                let sz = size_of::<InNodeStub>()
                    + n.entries.len() * size_of::<InEntry>()
                    + n.entries.iter().map(|e| e.key.len()).sum::<usize>();
                return Some(sz as u64);
            }
            let children: Vec<Arc<RwLock<TreeNode>>> = n.entries.iter()
                .filter_map(|e| e.child.as_ref().map(Arc::clone))
                .collect();
            drop(guard);
            for child in children {
                if let Some(sz) = find_node_size_recursive(&child, target_id) {
                    return Some(sz);
                }
            }
            None
        }
    }
}

/// Find the `Arc<RwLock<TreeNode>>` for a given node_id.
///
/// Used by `flush_dirty_node_to_log` so we can write-lock just the node
/// (not the entire tree) during WAL serialisation.
fn find_node_arc(tree: &Tree, node_id: u64) -> Option<Arc<RwLock<TreeNode>>> {
    let root_arc = tree.get_root()?;
    find_node_arc_recursive(&root_arc, node_id)
}

fn find_node_arc_recursive(
    node_arc: &Arc<RwLock<TreeNode>>,
    target_id: u64,
) -> Option<Arc<RwLock<TreeNode>>> {
    let guard = node_arc.read().ok()?;
    match &*guard {
        TreeNode::Bottom(b) => {
            if b.node_id == target_id {
                Some(Arc::clone(node_arc))
            } else {
                None
            }
        }
        TreeNode::Internal(n) => {
            if n.node_id == target_id {
                return Some(Arc::clone(node_arc));
            }
            let children: Vec<Arc<RwLock<TreeNode>>> = n.entries.iter()
                .filter_map(|e| e.child.as_ref().map(Arc::clone))
                .collect();
            drop(guard);
            for child in children {
                if let Some(found) = find_node_arc_recursive(&child, target_id) {
                    return Some(found);
                }
            }
            None
        }
    }
}

// ---------------------------------------------------------------------------
// Default callbacks used by do_evict() (test / integration stub)
// ---------------------------------------------------------------------------

/// Default `NodeEvictionInfo` implementation used by `do_evict()`.
///
/// Represents an evictable, clean, non-BIN node with no active references.
/// Production code should supply a real implementation via
/// `do_evict_with_callbacks()`.
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

/// Returns a `DefaultNodeInfo` for every node id (always evictable).
fn default_node_info(_node_id: u64) -> Option<Box<dyn NodeEvictionInfo>> {
    Some(Box::new(DefaultNodeInfo))
}

/// Returns a conservative 1 KiB estimate for every node id.
///
/// Used only in unit tests or when no tree is wired; real eviction
/// uses `real_node_size()` which traverses the live tree.
fn default_node_size(_node_id: u64) -> u64 {
    1024
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, atomic::AtomicI64};

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn make_evictor(usage: i64, max: i64, batch: usize) -> (Arc<AtomicI64>, Evictor) {
        let counter = Arc::new(AtomicI64::new(usage));
        let arbiter = Arbiter::new(max, Arc::clone(&counter), 100, 200);
        let evictor = Evictor::new(arbiter, batch, false);
        (counter, evictor)
    }

    // -----------------------------------------------------------------------
    // EvictionDecision / decide_eviction tests
    // -----------------------------------------------------------------------

    struct NodeInfo {
        dirty: bool,
        bin: bool,
        resident: bool,
        refs: usize,
    }

    impl NodeEvictionInfo for NodeInfo {
        fn is_dirty(&self) -> bool { self.dirty }
        fn is_bin(&self) -> bool { self.bin }
        fn is_resident(&self) -> bool { self.resident }
        fn ref_count(&self) -> usize { self.refs }
    }

    fn info(dirty: bool, bin: bool, resident: bool, refs: usize) -> NodeInfo {
        NodeInfo { dirty, bin, resident, refs }
    }

    #[test]
    fn test_decide_eviction_skip_not_resident() {
        let n = info(false, false, /*resident=*/false, 0);
        assert_eq!(decide_eviction(&n, false, true), EvictionDecision::Skip);
    }

    #[test]
    fn test_decide_eviction_putback_pinned() {
        let n = info(false, false, true, /*refs=*/2);
        assert_eq!(decide_eviction(&n, false, true), EvictionDecision::PutBack);
    }

    #[test]
    fn test_decide_eviction_partial_evict_bin() {
        // A BIN with no refs → PartialEvict regardless of dirty flag.
        let n = info(false, /*bin=*/true, true, 0);
        assert_eq!(decide_eviction(&n, false, true), EvictionDecision::PartialEvict);

        let n_dirty = info(true, true, true, 0);
        assert_eq!(decide_eviction(&n_dirty, false, true), EvictionDecision::PartialEvict);
    }

    #[test]
    fn test_decide_eviction_move_dirty_to_pri2() {
        // Dirty non-BIN in pri1 with dirty LRU enabled → MoveDirtyToPri2.
        let n = info(/*dirty=*/true, false, true, 0);
        assert_eq!(
            decide_eviction(&n, /*already_in_pri2=*/false, /*use_dirty_lru=*/true),
            EvictionDecision::MoveDirtyToPri2,
        );
    }

    #[test]
    fn test_decide_eviction_dirty_already_in_pri2_evicts() {
        // Dirty node already in pri2 → Evict (no more second chances).
        let n = info(true, false, true, 0);
        assert_eq!(
            decide_eviction(&n, /*already_in_pri2=*/true, true),
            EvictionDecision::Evict,
        );
    }

    #[test]
    fn test_decide_eviction_dirty_lru_disabled_evicts() {
        // Dirty node but use_dirty_lru=false (lru_only mode) → Evict.
        let n = info(true, false, true, 0);
        assert_eq!(
            decide_eviction(&n, false, /*use_dirty_lru=*/false),
            EvictionDecision::Evict,
        );
    }

    #[test]
    fn test_decide_eviction_clean_evicts() {
        let n = info(false, false, true, 0);
        assert_eq!(decide_eviction(&n, false, true), EvictionDecision::Evict);
    }

    // -----------------------------------------------------------------------
    // EvictResult tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_eviction_source() {
        assert_eq!(EvictionSource::Daemon, EvictionSource::Daemon);
        assert_ne!(EvictionSource::Daemon, EvictionSource::Critical);
    }

    #[test]
    fn test_evict_result_zero() {
        let result = EvictResult::zero();
        assert_eq!(result.nodes_evicted, 0);
        assert_eq!(result.bytes_evicted, 0);
    }

    #[test]
    fn test_evict_result_new() {
        let result = EvictResult::new(5, 1024);
        assert_eq!(result.nodes_evicted, 5);
        assert_eq!(result.bytes_evicted, 1024);
    }

    #[test]
    fn test_evict_result_add() {
        let mut result = EvictResult::new(5, 1024);
        let other = EvictResult::new(3, 512);
        result.add(&other);
        assert_eq!(result.nodes_evicted, 8);
        assert_eq!(result.bytes_evicted, 1536);
    }

    // -----------------------------------------------------------------------
    // Evictor construction / basic API
    // -----------------------------------------------------------------------

    #[test]
    fn test_evictor_new() {
        let usage = Arc::new(AtomicI64::new(0));
        let arbiter = Arbiter::new(1000, usage, 100, 200);
        let evictor = Evictor::new(arbiter, 100, false);

        assert!(!evictor.is_shutdown());
        assert_eq!(evictor.get_lru_sizes(), (0, 0));
    }

    #[test]
    fn test_note_ins_added_hot() {
        let usage = Arc::new(AtomicI64::new(0));
        let arbiter = Arbiter::new(1000, usage, 100, 200);
        let evictor = Evictor::new(arbiter, 100, false);

        evictor.note_ins_added(1, CacheMode::Default);
        evictor.note_ins_added(2, CacheMode::KeepHot);

        assert_eq!(evictor.get_lru_sizes(), (2, 0));
    }

    #[test]
    fn test_note_ins_added_cold() {
        let usage = Arc::new(AtomicI64::new(0));
        let arbiter = Arbiter::new(1000, usage, 100, 200);
        let evictor = Evictor::new(arbiter, 100, false);

        evictor.note_ins_added(1, CacheMode::MakeEvictable);

        assert_eq!(evictor.get_lru_sizes(), (1, 0));
        // Cold nodes go to front, so should be evicted first
    }

    #[test]
    fn test_note_ins_removed() {
        let usage = Arc::new(AtomicI64::new(0));
        let arbiter = Arbiter::new(1000, usage, 100, 200);
        let evictor = Evictor::new(arbiter, 100, false);

        evictor.note_ins_added(1, CacheMode::Default);
        evictor.note_ins_added(2, CacheMode::Default);
        assert_eq!(evictor.get_lru_sizes(), (2, 0));

        evictor.note_ins_removed(1);
        assert_eq!(evictor.get_lru_sizes(), (1, 0));
    }

    #[test]
    fn test_move_to_pri2() {
        let usage = Arc::new(AtomicI64::new(0));
        let arbiter = Arbiter::new(1000, usage, 100, 200);
        let evictor = Evictor::new(arbiter, 100, false);

        evictor.note_ins_added(1, CacheMode::Default);
        assert_eq!(evictor.get_lru_sizes(), (1, 0));

        assert!(evictor.move_to_pri2(1));
        assert_eq!(evictor.get_lru_sizes(), (0, 1));

        // Can't move again (not in pri1)
        assert!(!evictor.move_to_pri2(1));
    }

    // -----------------------------------------------------------------------
    // do_evict tests (using default callbacks)
    // -----------------------------------------------------------------------

    #[test]
    fn test_do_evict_under_budget() {
        let usage = Arc::new(AtomicI64::new(500));
        let arbiter = Arbiter::new(1000, usage, 100, 200);
        let evictor = Evictor::new(arbiter, 100, false);

        // Add some nodes
        evictor.note_ins_added(1, CacheMode::Default);
        evictor.note_ins_added(2, CacheMode::Default);

        // Under budget, should not evict
        let result = evictor.do_evict(EvictionSource::Daemon);
        assert_eq!(result.nodes_evicted, 0);
    }

    #[test]
    fn test_do_evict_over_budget() {
        let usage = Arc::new(AtomicI64::new(1500));
        let arbiter = Arbiter::new(1000, usage, 100, 200);
        let evictor = Evictor::new(arbiter, 100, false);

        // Add some nodes
        for i in 1..=5 {
            evictor.note_ins_added(i, CacheMode::Default);
        }

        // Over budget, should evict
        let result = evictor.do_evict(EvictionSource::Critical);
        assert!(result.nodes_evicted > 0);
        assert!(result.bytes_evicted > 0);

        // Check stats were updated
        let stats = evictor.get_stats();
        assert_eq!(stats.get(&stats.eviction_runs), 1);
        assert!(stats.get(&stats.bytes_evicted_critical) > 0);
    }

    #[test]
    fn test_shutdown() {
        let usage = Arc::new(AtomicI64::new(0));
        let arbiter = Arbiter::new(1000, usage, 100, 200);
        let evictor = Evictor::new(arbiter, 100, false);

        assert!(!evictor.is_shutdown());
        evictor.shutdown();
        assert!(evictor.is_shutdown());

        // Eviction should not run after shutdown
        let result = evictor.do_evict(EvictionSource::Daemon);
        assert_eq!(result.nodes_evicted, 0);
    }

    #[test]
    fn test_update_lru_stats() {
        let usage = Arc::new(AtomicI64::new(0));
        let arbiter = Arbiter::new(1000, usage, 100, 200);
        let evictor = Evictor::new(arbiter, 100, false);

        evictor.note_ins_added(1, CacheMode::Default);
        evictor.note_ins_added(2, CacheMode::Default);
        evictor.move_to_pri2(2);

        evictor.update_lru_stats();

        let stats = evictor.get_stats();
        assert_eq!(stats.get(&stats.pri1_lru_size), 1);
        assert_eq!(stats.get(&stats.pri2_lru_size), 1);
    }

    #[test]
    fn test_batch_size_limit() {
        let usage = Arc::new(AtomicI64::new(2000));
        let arbiter = Arbiter::new(1000, usage, 100, 200);
        let evictor = Evictor::new(arbiter, 3, false); // Small batch size

        // Add many nodes
        for i in 1..=10 {
            evictor.note_ins_added(i, CacheMode::Default);
        }

        let result = evictor.do_evict(EvictionSource::Daemon);
        // Should not exceed batch size
        assert!(result.nodes_evicted <= 3);
    }

    // -----------------------------------------------------------------------
    // evict_batch with custom callbacks — each decision path
    // -----------------------------------------------------------------------

    /// Helper: build an info function that always returns the same NodeInfo.
    fn static_info_fn(
        dirty: bool, bin: bool, resident: bool, refs: usize,
    ) -> impl Fn(u64) -> Option<Box<dyn NodeEvictionInfo>> {
        move |_| Some(Box::new(NodeInfo { dirty, bin, resident, refs }) as Box<dyn NodeEvictionInfo>)
    }

    /// Fixed size callback returning 512 bytes per node.
    fn size_512(_id: u64) -> u64 { 512 }

    #[test]
    fn test_evict_batch_skip_path() {
        // Nodes report not-resident → Skip, no eviction, no put-back.
        let (_counter, evictor) = make_evictor(1500, 1000, 10);
        for i in 1..=3u64 {
            evictor.note_ins_added(i, CacheMode::Default);
        }
        let result = evictor.evict_batch(
            EvictionSource::Daemon,
            &static_info_fn(false, false, /*resident=*/false, 0),
            &size_512,
        );
        assert_eq!(result.nodes_evicted, 0);
        assert_eq!(result.bytes_evicted, 0);
        let stats = evictor.get_stats();
        assert_eq!(stats.get(&stats.nodes_skipped), 3);
        // All nodes were removed from the LRU by the batch and not re-added.
        assert_eq!(evictor.get_lru_sizes(), (0, 0));
    }

    #[test]
    fn test_evict_batch_putback_path() {
        // Nodes are pinned (ref_count > 0) → PutBack.
        let (_counter, evictor) = make_evictor(1500, 1000, 10);
        for i in 1..=3u64 {
            evictor.note_ins_added(i, CacheMode::Default);
        }
        let result = evictor.evict_batch(
            EvictionSource::Daemon,
            &static_info_fn(false, false, true, /*refs=*/1),
            &size_512,
        );
        assert_eq!(result.nodes_evicted, 0);
        let stats = evictor.get_stats();
        assert_eq!(stats.get(&stats.nodes_put_back), 3);
        // Nodes are back in pri1.
        assert_eq!(evictor.get_lru_sizes(), (3, 0));
    }

    #[test]
    fn test_evict_batch_partial_evict_path() {
        // BIN nodes → PartialEvict: bytes freed and BIN put back.
        let (_counter, evictor) = make_evictor(1500, 1000, 10);
        for i in 1..=3u64 {
            evictor.note_ins_added(i, CacheMode::Default);
        }
        let result = evictor.evict_batch(
            EvictionSource::Daemon,
            &static_info_fn(false, /*bin=*/true, true, 0),
            &size_512,
        );
        // No full evictions, but bytes freed from stripped LNs.
        assert_eq!(result.nodes_evicted, 0);
        assert_eq!(result.bytes_evicted, 3 * 512);
        let stats = evictor.get_stats();
        assert_eq!(stats.get(&stats.nodes_stripped), 3);
        // BINs put back into LRU.
        assert_eq!(evictor.get_lru_sizes(), (3, 0));
    }

    #[test]
    fn test_evict_batch_move_dirty_to_pri2_path() {
        // Dirty non-BIN nodes in pri1 with dirty LRU enabled → MoveDirtyToPri2.
        //
        // Use batch size == pri1 count (3) so the batch stops after draining
        // pri1; it does not spill into pri2 and re-evict the just-moved nodes.
        let counter = Arc::new(AtomicI64::new(1500));
        let arbiter = Arbiter::new(1000, Arc::clone(&counter), 100, 200);
        let evictor = Evictor::new(arbiter, /*max_batch_size=*/3, false);
        for i in 1..=3u64 {
            evictor.note_ins_added(i, CacheMode::Default);
        }
        assert_eq!(evictor.get_lru_sizes(), (3, 0));
        let result = evictor.evict_batch(
            EvictionSource::Daemon,
            &static_info_fn(/*dirty=*/true, false, true, 0),
            &size_512,
        );
        // All 3 nodes were moved to pri2, none fully evicted yet.
        assert_eq!(result.nodes_evicted, 0);
        let stats = evictor.get_stats();
        assert_eq!(stats.get(&stats.nodes_moved_to_pri2_lru), 3);
        // All nodes now reside in pri2.
        assert_eq!(evictor.get_lru_sizes(), (0, 3));
    }

    #[test]
    fn test_evict_batch_evict_path() {
        // Clean non-BIN nodes → Evict.
        let (_counter, evictor) = make_evictor(1500, 1000, 10);
        for i in 1..=3u64 {
            evictor.note_ins_added(i, CacheMode::Default);
        }
        let result = evictor.evict_batch(
            EvictionSource::Daemon,
            &static_info_fn(false, false, true, 0),
            &size_512,
        );
        assert_eq!(result.nodes_evicted, 3);
        assert_eq!(result.bytes_evicted, 3 * 512);
        assert_eq!(evictor.get_lru_sizes(), (0, 0));
    }

    #[test]
    fn test_evict_batch_dirty_already_in_pri2_evicts() {
        // Dirty nodes that start in pri2 should be evicted, not moved again.
        let (_counter, evictor) = make_evictor(1500, 1000, 10);
        // Add to pri1 then move to pri2 manually.
        for i in 1..=3u64 {
            evictor.note_ins_added(i, CacheMode::Default);
            evictor.move_to_pri2(i);
        }
        assert_eq!(evictor.get_lru_sizes(), (0, 3));

        // evict_batch in non-lru_only mode; nodes come from pri2.
        let result = evictor.evict_batch(
            EvictionSource::Daemon,
            &static_info_fn(/*dirty=*/true, false, true, 0),
            &size_512,
        );
        assert_eq!(result.nodes_evicted, 3);
        assert_eq!(result.bytes_evicted, 3 * 512);
        let stats = evictor.get_stats();
        // dirty_nodes_evicted is only incremented when flush_dirty_node_to_log
        // successfully writes to the WAL.  Without a wired LogManager it is
        // 0; nodes_evicted tracks all full evictions regardless of dirty flag.
        assert_eq!(stats.get(&stats.nodes_evicted), 3);
    }

    // -----------------------------------------------------------------------
    // complete_checkpoint_for_node tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_complete_checkpoint_promotes_pri2_to_pri1() {
        let (_counter, evictor) = make_evictor(0, 1000, 10);

        evictor.note_ins_added(42, CacheMode::Default);
        evictor.move_to_pri2(42);
        assert_eq!(evictor.get_lru_sizes(), (0, 1));

        // Simulate checkpointer logging the node.
        assert!(evictor.complete_checkpoint_for_node(42));
        assert_eq!(evictor.get_lru_sizes(), (1, 0));
    }

    #[test]
    fn test_complete_checkpoint_noop_if_not_in_pri2() {
        let (_counter, evictor) = make_evictor(0, 1000, 10);

        evictor.note_ins_added(7, CacheMode::Default);
        // Node is in pri1, not pri2.
        assert!(!evictor.complete_checkpoint_for_node(7));
        // Still in pri1.
        assert_eq!(evictor.get_lru_sizes(), (1, 0));
    }

    #[test]
    fn test_complete_checkpoint_unknown_node() {
        let (_counter, evictor) = make_evictor(0, 1000, 10);
        assert!(!evictor.complete_checkpoint_for_node(999));
    }

    // -----------------------------------------------------------------------
    // do_evict_with_callbacks — source statistics integration
    // -----------------------------------------------------------------------

    #[test]
    fn test_do_evict_with_callbacks_daemon_stats() {
        let (counter, evictor) = make_evictor(1500, 1000, 10);
        for i in 1..=4u64 { evictor.note_ins_added(i, CacheMode::Default); }

        let result = evictor.do_evict_with_callbacks(
            EvictionSource::Daemon,
            &static_info_fn(false, false, true, 0),
            &size_512,
        );
        assert!(result.bytes_evicted > 0);
        let stats = evictor.get_stats();
        assert_eq!(stats.get(&stats.bytes_evicted_daemon), result.bytes_evicted);
        drop(counter);
    }

    #[test]
    fn test_do_evict_with_callbacks_manual_stats() {
        let (_counter, evictor) = make_evictor(1500, 1000, 10);
        for i in 1..=4u64 { evictor.note_ins_added(i, CacheMode::Default); }

        let result = evictor.do_evict_with_callbacks(
            EvictionSource::Manual,
            &static_info_fn(false, false, true, 0),
            &size_512,
        );
        let stats = evictor.get_stats();
        assert_eq!(stats.get(&stats.bytes_evicted_manual), result.bytes_evicted);
    }

    #[test]
    fn test_do_evict_with_callbacks_cachemode_stats() {
        let (_counter, evictor) = make_evictor(1500, 1000, 10);
        for i in 1..=4u64 { evictor.note_ins_added(i, CacheMode::Default); }

        let result = evictor.do_evict_with_callbacks(
            EvictionSource::CacheMode,
            &static_info_fn(false, false, true, 0),
            &size_512,
        );
        let stats = evictor.get_stats();
        assert_eq!(stats.get(&stats.bytes_evicted_cachemode), result.bytes_evicted);
    }

    // -----------------------------------------------------------------------
    // Interaction between pri1 / pri2 in evict_batch
    // -----------------------------------------------------------------------

    #[test]
    fn test_evict_batch_drains_pri1_then_pri2() {
        // Place 2 nodes in pri1 and 2 in pri2; batch should process pri1 first.
        let (_counter, evictor) = make_evictor(1500, 1000, 10);
        for i in 1..=2u64 { evictor.note_ins_added(i, CacheMode::Default); }
        for i in 3..=4u64 {
            evictor.note_ins_added(i, CacheMode::Default);
            evictor.move_to_pri2(i);
        }
        assert_eq!(evictor.get_lru_sizes(), (2, 2));

        let result = evictor.evict_batch(
            EvictionSource::Daemon,
            &static_info_fn(false, false, true, 0),
            &size_512,
        );
        // All 4 nodes evicted.
        assert_eq!(result.nodes_evicted, 4);
        assert_eq!(evictor.get_lru_sizes(), (0, 0));
    }

    #[test]
    fn test_lru_only_mode_ignores_pri2() {
        // With lru_only=true the evictor should not touch pri2.
        let usage = Arc::new(AtomicI64::new(1500));
        let arbiter = Arbiter::new(1000, Arc::clone(&usage), 100, 200);
        let evictor = Evictor::new(arbiter, 10, /*lru_only=*/true);

        for i in 1..=2u64 { evictor.note_ins_added(i, CacheMode::Default); }
        // Manually place a node in pri2 (bypassing the normal API).
        evictor.lru.pri2_add_back(99);

        let result = evictor.evict_batch(
            EvictionSource::Daemon,
            &static_info_fn(false, false, true, 0),
            &size_512,
        );
        // Only pri1 nodes evicted.
        assert_eq!(result.nodes_evicted, 2);
        // pri2 node untouched.
        assert_eq!(evictor.get_lru_sizes(), (0, 1));
    }
}
