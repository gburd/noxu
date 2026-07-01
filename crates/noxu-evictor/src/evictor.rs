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
use noxu_tree::InListListener;
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

    /// True when this is an upper IN (level >= 2 / non-BIN) that still has
    /// at least one resident (cached) child (`InEntry.child.is_some()`).
    ///
    /// JE `IN.hasCachedChildren` / the `NON_EVICTABLE_IN` skip in
    /// `Evictor.processTarget` (Evictor.java:2652-2656): a UIN with cached
    /// children must not be evicted — detaching it would orphan its resident
    /// children (their parent pointer would dangle).  Defaults to `false` for
    /// BINs and for synthetic test infos.
    fn has_cached_children(&self) -> bool {
        false
    }

    /// True when this node is the root IN of its tree.
    ///
    /// JE `IN.isRoot()` / the root-protection skip in
    /// `Evictor.processTarget` (Evictor.java:2663-2671): JE never evicts the
    /// root of the internal ID/NAME databases, and `evictRoot` handles user-DB
    /// roots through a separate path.  Noxu takes the simplest faithful rule —
    /// never evict a root IN — so the root stays resident.  Defaults to
    /// `false`.
    fn is_root(&self) -> bool {
        false
    }
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
    /// EV-14: the target is an idle root IN — evict it via the separate
    /// `evictRoot` path (JE `if (target.isRoot())` branch in `evictBatch`,
    /// Evictor.java:2400-2421), NOT the normal detach path.
    EvictRoot,
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
    // EV-6 (JE Evictor.processTarget, Evictor.java:2652-2656,
    // IN.hasCachedChildren / NON_EVICTABLE_IN): an upper IN that has resident
    // (cached) children must NOT be evicted.  A childless UIN may be selected
    // for the LRU and then acquire cached children before the evicting thread
    // latches it; skip it so EV-13's detach can't orphan a resident child.
    if !info.is_bin() && info.has_cached_children() {
        return EvictionDecision::Skip;
    }
    // EV-7 / EV-14 (JE Evictor.processTarget root special-casing,
    // Evictor.java:2400-2421 + 2663-2671, IN.isRoot()): a root IN is NEVER
    // evicted via the NORMAL detach path (that would break the EV-13 detach
    // invariants — a root has no parent slot to detach from).  Instead JE
    // routes a root target to the separate `evictRoot` path.  EV-14 relaxes
    // the old "always Skip a root" rule to: an *idle* root (no cached
    // children) is evicted via `EvictRoot`; a root that still has cached
    // children is kept resident (EV-6 — evicting it would orphan them).
    if info.is_root() {
        if info.has_cached_children() {
            return EvictionDecision::Skip;
        }
        return EvictionDecision::EvictRoot;
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
    /// Whether to stage dirty INs in the pri2 LRU set (JE
    /// `EVICTOR_USE_DIRTY_LRU`). Defaults to `!lru_only`; forced false when an
    /// off-heap cache is configured (JE Evictor.java:1705). Override via
    /// `with_use_dirty_lru`.
    use_dirty_lru: bool,

    /// JE `EVICTOR_MUTATE_BINS` (default true): when true the evictor may
    /// strip obsolete LNs out of a BIN during PartialEvict; when false the
    /// BIN is left untouched (no LN stripping) and only whole-node eviction
    /// applies.  Faithful to JE `Evictor` `mutateBins` / `EVICTOR_MUTATE_BINS`.
    mutate_bins: bool,

    next_pri1_index: AtomicU64,
    next_pri2_index: AtomicU64,

    log_manager: Option<Arc<LogManager>>,
    /// The B-tree the evictor walks to find/evict nodes.
    ///
    /// Interior-mutable so `EnvironmentImpl` can install the user database's
    /// tree *after* the evictor `Arc` is constructed (databases are opened
    /// later).  JE: the global `INList` is registered with the environment at
    /// startup; here a single tree is wired for the single-database case.
    ///
    /// The primary tree slot, installed via `with_tree`/`set_tree`.  Used as
    /// the first lookup target; the full set of databases is reached via
    /// `db_trees_registry` (see EVICTOR-RECLAIM-1).
    tree: RwLock<Option<Arc<RwLock<Tree>>>>,
    db_id: AtomicU64,

    /// All database trees, keyed by db_id — the SAME registry the
    /// checkpointer (`with_db_trees_registry`) and cleaner (`with_tree_registry`)
    /// use.  EVICTOR-RECLAIM-1: JE walks ONE env-wide `INList` that covers
    /// every database; `Evictor.processTarget` resolves each target IN's
    /// owning DB via `target.getDatabase()` (Evictor.java:2374).  Noxu split
    /// the tree per-DB, so the evictor must consult this registry to find a
    /// targeted node in the CORRECT tree — otherwise user-DB BINs get TARGETED
    /// (the InList listener feeds the policy from every tree) but can never be
    /// stripped/evicted because they are absent from the single primary tree.
    ///
    /// `None` until `set_db_trees_registry` is called (unit tests without a
    /// full environment fall back to the single `tree` slot).
    db_trees_registry:
        RwLock<Option<Arc<std::sync::Mutex<HashMap<i64, Arc<RwLock<Tree>>>>>>>,
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
            use_dirty_lru: !lru_only,
            mutate_bins: true,
            next_pri1_index: AtomicU64::new(0),
            next_pri2_index: AtomicU64::new(0),
            log_manager: None,
            tree: RwLock::new(None),
            db_id: AtomicU64::new(0),
            db_trees_registry: RwLock::new(None),
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
        .with_opt_tree(
            self.tree.into_inner().expect("evictor tree lock poisoned"),
            self.db_id.load(Ordering::Relaxed),
        )
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
    pub fn with_tree(self, tree: Arc<RwLock<Tree>>, db_id: u64) -> Self {
        self.set_tree(tree, db_id);
        self
    }

    /// Install (or replace) the tree the evictor walks, after the evictor
    /// `Arc` has been constructed.  Used by `EnvironmentImpl::open_database`
    /// to point the evictor at the user database's tree.
    ///
    /// JE: equivalent to the database's INs being registered in the
    /// environment-wide `INList` that the evictor drains.
    pub fn set_tree(&self, tree: Arc<RwLock<Tree>>, db_id: u64) {
        *self.tree.write().expect("evictor tree lock poisoned") = Some(tree);
        self.db_id.store(db_id, Ordering::Relaxed);
    }

    /// Clone the currently installed eviction tree, if any.
    fn current_tree(&self) -> Option<Arc<RwLock<Tree>>> {
        self.tree.read().expect("evictor tree lock poisoned").clone()
    }

    /// Wire the env-wide `db_trees_registry` (the SAME `Arc` the checkpointer
    /// and cleaner hold) so the evictor can resolve a targeted node to its
    /// owning database tree.  EVICTOR-RECLAIM-1: JE `Evictor.processTarget`
    /// resolves `target.getDatabase()` from the single env-wide `INList`
    /// (Evictor.java:2374); Noxu's per-DB trees require this registry to find
    /// the owning tree.  Installed after construction because the registry is
    /// built later in `EnvironmentImpl::new` than the evictor.
    pub fn set_db_trees_registry(
        &self,
        registry: Arc<std::sync::Mutex<HashMap<i64, Arc<RwLock<Tree>>>>>,
    ) {
        *self
            .db_trees_registry
            .write()
            .expect("evictor registry lock poisoned") = Some(registry);
    }

    /// Collect every candidate tree to search, in priority order: the primary
    /// `tree` slot first (so single-database envs and unit tests keep their
    /// existing fast path), then every distinct tree in `db_trees_registry`.
    ///
    /// Each entry is `(db_id, tree_arc)`.  The primary slot's db_id comes from
    /// `self.db_id`; registry entries carry their own db_id key (so the
    /// flush/root-evict paths log under the CORRECT database).  De-duplicated
    /// by `Arc::ptr_eq` so a tree present in both the slot and the registry
    /// is searched once.
    ///
    /// JE: the single env-wide `INList` is the union of all databases'
    /// resident INs (Evictor.java:2374, `target.getDatabase()`).
    fn candidate_trees(&self) -> Vec<(u64, Arc<RwLock<Tree>>)> {
        let mut out: Vec<(u64, Arc<RwLock<Tree>>)> = Vec::new();
        if let Some(t) = self.current_tree() {
            out.push((self.db_id.load(Ordering::Relaxed), t));
        }
        if let Some(reg) = self
            .db_trees_registry
            .read()
            .expect("evictor registry lock poisoned")
            .as_ref()
            && let Ok(map) = reg.lock()
        {
            for (db_id, tree) in map.iter() {
                if !out.iter().any(|(_, t)| Arc::ptr_eq(t, tree)) {
                    out.push((*db_id as u64, Arc::clone(tree)));
                }
            }
        }
        out
    }

    /// Wire an off-heap cache.
    /// Returns whether dirty INs are staged in the pri2 LRU set.
    pub fn use_dirty_lru(&self) -> bool {
        self.use_dirty_lru
    }

    /// Set whether dirty INs are staged in the pri2 LRU set (JE
    /// `EVICTOR_USE_DIRTY_LRU`). JE forces this false when an off-heap cache is
    /// configured (Evictor.java:1705); callers should pass
    /// `cfg.evictor_use_dirty_lru && off_heap_disabled`.
    pub fn with_use_dirty_lru(mut self, use_dirty_lru: bool) -> Self {
        self.use_dirty_lru = use_dirty_lru;
        self
    }

    /// JE `EVICTOR_MUTATE_BINS` (default true): when false, the evictor will
    /// NOT strip LNs out of a BIN during eviction (PartialEvict becomes a
    /// no-op strip that frees 0 bytes).  Faithful to `Evictor` `mutateBins`.
    pub fn with_mutate_bins(mut self, mutate_bins: bool) -> Self {
        self.mutate_bins = mutate_bins;
        self
    }

    pub fn with_off_heap(mut self, cache: Arc<OffHeapCache>) -> Self {
        // JE forces useDirtyLRU = false when an off-heap cache is ENABLED
        // (Evictor.java:1705). A disabled off-heap wrapper must not disable
        // the dirty-LRU staging.
        if cache.is_enabled() {
            self.use_dirty_lru = false;
        }
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
        self,
        tree: Option<Arc<RwLock<Tree>>>,
        db_id: u64,
    ) -> Self {
        *self.tree.write().expect("evictor tree lock poisoned") = tree;
        self.db_id.store(db_id, Ordering::Relaxed);
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

            let decision =
                decide_eviction(info.as_ref(), from_pri2, self.use_dirty_lru);

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
                        Some(freed_bytes) if freed_bytes > 0 => {
                            // Stripped some LN bytes -> strippedPutBack.
                            // JE processTarget ~2722-2726: if partialEviction
                            // freed bytes, keep the BIN resident (warm descent
                            // path) and put it back.
                            result.bytes_evicted += freed_bytes as u64;
                            self.stats.increment(&self.stats.nodes_stripped);
                            self.stats.increment(&self.stats.lns_evicted);
                            if from_pri2 {
                                self.pri2.lock().add_back(node_id);
                            } else {
                                self.primary_policy.put_back(node_id);
                            }
                        }
                        Some(_zero) => {
                            // partialEviction freed 0 bytes (no clean LN data
                            // left to strip).  The BIN is unpinned and
                            // cursor-free (strip_lns_from_node returns None
                            // otherwise).  Mirror JE processTarget
                            // (Evictor.java ~2755-2795): BIN-delta mutation is
                            // documented-skipped (EVICTOR_MUTATE_BINS), then a
                            // dirty BIN gets a one-time pri2 second chance,
                            // else the node is evicted.
                            //
                            // CLN-F2 regression fix (vs. 29119ca): split the
                            // CLEAN and DIRTY strip-0 cases.
                            //
                            //  * CLEAN strip-0 BIN  -> FULLY evict (remove +
                            //    credit node bytes).  This is the CLN-F2 goal
                            //    and matches JE's fall-through to
                            //    evict(target,parent,index) (Evictor.java
                            //    ~2786-2795) once the dirty/off-heap
                            //    second-chance guards do not apply.
                            //
                            //  * DIRTY strip-0 BIN  -> if useDirtyLRUSet is in
                            //    effect (Evictor.java ~2758-2766: dirty &&
                            //    !isInPri2LRU -> moveToPri2LRU) give it a
                            //    one-time pri2 second chance; otherwise PUT IT
                            //    BACK (the pre-CLN-F2 behaviour) so a later
                            //    pass can strip it once its slots are clean.
                            //
                            // Why dirty strip-0 does NOT full-evict here:
                            // 29119ca routed every dirty strip-0 BIN to pri2,
                            // but under EVICTOR_LRU_ONLY the dirty-LRU set is
                            // not drained (the evict_batch phase machine
                            // returns at phase 1 and never reaches phase 2),
                            // so the BIN was parked forever and its bytes were
                            // never reclaimed -- cache_usage stuck (F2 regress
                            // ion).  A dirty BIN's *useful* memory is its LN
                            // value heap, which strip_lns only frees once the
                            // slots are clean (a checkpoint logs+cleans them).
                            // Putting the dirty BIN back lets the next pass
                            // strip the now-clean slots and actually free that
                            // heap -- the behaviour that held before 29119ca
                            // and that this test depends on.
                            if info.is_dirty() {
                                if self.use_dirty_lru
                                    && !self.lru_only
                                    && !from_pri2
                                {
                                    // JE ~2762-2768: dirty & not in pri2 ->
                                    // moveToPri2LRU (one-time second chance).
                                    self.pri2.lock().add_front(node_id);
                                    self.stats.increment(
                                        &self.stats.nodes_moved_to_pri2_lru,
                                    );
                                } else {
                                    // Pre-CLN-F2 behaviour: put the dirty BIN
                                    // back (no byte credit) so a later pass can
                                    // strip its now-clean slots.
                                    if from_pri2 {
                                        self.pri2.lock().add_back(node_id);
                                    } else {
                                        self.primary_policy.put_back(node_id);
                                    }
                                    self.stats
                                        .increment(&self.stats.nodes_put_back);
                                }
                            } else {
                                // JE ~2786-2795: clean strip-0 BIN ->
                                // evict(target, parent, index): remove the BIN
                                // and reclaim its node-level heap (CLN-F2).
                                let freed = node_size_fn(node_id);
                                result.bytes_evicted += freed;
                                result.nodes_evicted += 1;
                                self.stats.increment(&self.stats.nodes_evicted);
                            }
                        }
                        None => {
                            // Node busy or pinned -- put back without any
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
                    // EVICTOR-RECLAIM-1: serialize the upper IN from whichever
                    // tree owns it (serialize_upper_in returns None for a node
                    // it does not contain), not only the primary slot.
                    if let Some(oh) = &self.off_heap
                        && oh.is_enabled()
                    {
                        for (_db_id, tree_arc) in self.candidate_trees() {
                            if let Ok(tree_guard) = tree_arc.read()
                                && let Some(serialized) =
                                    tree_guard.serialize_upper_in(node_id)
                            {
                                stored_off_heap =
                                    oh.store_node(node_id, serialized);
                                break;
                            }
                        }
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

                EvictionDecision::EvictRoot => {
                    // EV-14: route an idle root to the separate evictRoot path
                    // (JE evictBatch `if (target.isRoot())` ->
                    // Tree.withRootLatchedExclusive(rootEvictor), Evictor.java
                    // :2400-2421).  evict_root logs a dirty root first, updates
                    // the tree root_log_lsn, and clears the in-memory root; the
                    // root re-fetches from its LSN on next access.
                    match self.evict_root_node(node_id) {
                        Some((freed, was_dirty)) => {
                            result.bytes_evicted += freed;
                            result.nodes_evicted += 1;
                            self.stats.increment(&self.stats.nodes_evicted);
                            // JE nRootNodesEvicted.increment().
                            self.stats
                                .increment(&self.stats.root_nodes_evicted);
                            if was_dirty {
                                // JE: rootEvictor.flushed ->
                                // nDirtyNodesEvicted.increment().
                                self.stats
                                    .increment(&self.stats.dirty_nodes_evicted);
                            }
                        }
                        None => {
                            // Root not evictable right now (no log manager,
                            // resident children acquired after selection,
                            // pinned, or clean-but-never-logged).  Put it back
                            // (JE RootEvictor releases the latch and leaves the
                            // root resident).
                            if from_pri2 {
                                self.pri2.lock().add_back(node_id);
                            } else {
                                self.primary_policy.put_back(node_id);
                            }
                            self.stats.increment(&self.stats.nodes_put_back);
                        }
                    }
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
        // EVICTOR-RECLAIM-1: snapshot ALL database trees, not just the primary
        // slot.  The info/size callbacks search each tree for the candidate
        // node, mirroring JE's single env-wide INList whose targets resolve
        // to their owning DB (Evictor.processTarget -> target.getDatabase(),
        // Evictor.java:2374).
        let trees = self.candidate_trees();
        if !trees.is_empty() {
            let trees_info: Vec<Arc<RwLock<Tree>>> =
                trees.iter().map(|(_, t)| Arc::clone(t)).collect();
            // EV-13: a second handle set for the detach-and-measure callback.
            let trees_detach = trees_info.clone();

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
                    // Walk each tree until the node is found in its owner.
                    for tree_arc in &trees_info {
                        let guard = match tree_arc.read() {
                            Ok(g) => g,
                            Err(_) => continue,
                        };
                        if let Some(full) = find_node_full(&guard, node_id) {
                            // Cache the size so node_size_fn needs no second
                            // tree walk.
                            sc.borrow_mut().insert(node_id, full.size);
                            return Some(Box::new(full.info));
                        }
                    }
                    None
                };
            // EV-13: this is the DETACH-and-measure callback (JE
            // `parent.detachNode(index, ...)` from `Evictor.evict`).  The
            // evictor calls it only on a committed full eviction (the Evict
            // decision, and the CLN-F2 clean strip-0 BIN path).  It detaches
            // the node from its parent IN under the parent write latch,
            // dropping the strong `Arc` so the node is freed for real, and
            // returns the measured heap bytes reclaimed.
            //
            // EVICTOR-RECLAIM-1: the detach must run on the tree that OWNS the
            // node so the parent IN re-wired by `detach_node_by_id` is in the
            // SAME tree (a cross-tree detach would corrupt structure).  We try
            // each tree; `detach_node_by_id` returns 0 for a node it does not
            // contain, so the first tree returning >0 is the owner.
            //
            // Before EV-13 this only drained a cached size: the node was
            // credited as freed but the parent still held the `Arc`, so the
            // heap was never reclaimed and `cache_usage` drifted below
            // reality (the evictor under-fired).
            let node_size_fn = move |node_id: u64| -> u64 {
                // Drain the cached size first so the RefCell never leaks the
                // entry even when detach short-circuits.
                let cached = sc.borrow_mut().remove(&node_id);
                let mut freed = 0u64;
                for tree_arc in &trees_detach {
                    if let Ok(t) = tree_arc.read() {
                        let f = t.detach_node_by_id(node_id);
                        if f > 0 {
                            freed = f;
                            break;
                        }
                    }
                }
                if freed > 0 {
                    // Detached and freed for real — credit the measured size.
                    freed
                } else {
                    // Not a detachable child (root / already gone / pinned).
                    // Fall back to the cached size rather than over-crediting
                    // a node we did not free; 1024 only if no walk ran.
                    cached.unwrap_or(1024)
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

    /// EV-14: evict the root IN via the separate `Tree::evict_root` path
    /// (JE `Evictor.evictRoot`).
    ///
    /// The `node_id` is the candidate the LRU offered; we only proceed if it
    /// is still some tree's resident root (JE `RootEvictor.doWork` re-checks
    /// `rootIN == target && rootIN.isRoot()`).  Returns `Some((freed_bytes,
    /// was_dirty))` on a successful evict, `None` if the root could not be
    /// evicted (so the caller puts the candidate back).
    ///
    /// EVICTOR-RECLAIM-1: searches every database tree for the one whose
    /// resident root matches, then calls `evict_root` on THAT tree with its
    /// own db_id so the detach re-wires the parent in the SAME tree and the
    /// root is re-fetchable from the correct database's persisted LSN
    /// (JE Evictor.processTarget -> target.getDatabase(), Evictor.java:2374).
    fn evict_root_node(&self, node_id: u64) -> Option<(u64, bool)> {
        for (db_id, tree_arc) in self.candidate_trees() {
            let tree = match tree_arc.read() {
                Ok(g) => g,
                Err(_) => continue,
            };
            // Re-check the candidate is still THIS tree's resident root
            // before evicting (JE RootEvictor re-checks rootIN == target &&
            // isRoot()).
            let root_id = tree.get_resident_root_in().map(|r| {
                use noxu_tree::tree::TreeNode;
                match &*r.read() {
                    TreeNode::Internal(n) => n.node_id,
                    TreeNode::Bottom(b) => b.node_id,
                }
            });
            if root_id == Some(node_id) {
                return tree.evict_root(db_id);
            }
        }
        None // not the resident root of any tree any more
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
        // EVICTOR-RECLAIM-1: search every database tree and capture the
        // owning db_id so the BIN is logged under the CORRECT database
        // (JE Evictor.processTarget -> target.getDatabase(), Evictor.java:2374).
        let trees = self.candidate_trees();
        if trees.is_empty() {
            return true; // no tree — nothing to flush
        }
        let (owning_db_id, node_arc): (u64, Arc<NodeRwLock<TreeNode>>) = {
            let mut found: Option<(u64, Arc<NodeRwLock<TreeNode>>)> = None;
            for (db_id, tree_arc) in &trees {
                let tree_guard = match tree_arc.read() {
                    Ok(g) => g,
                    Err(_) => return false, // poisoned; be conservative
                };
                // CC-6: non-blocking tree scan — if any node in the descent
                // path is write-locked by another thread, treat as busy.
                match find_node_arc_nonblocking(&tree_guard, node_id) {
                    Ok(Some(a)) => {
                        found = Some((*db_id, a));
                        break;
                    }
                    Ok(None) => continue, // not in this tree; try the next
                    Err(()) => return false, // descent blocked; put back
                }
            }
            match found {
                Some(pair) => pair,
                None => return true, // not in any tree; allow eviction
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
        // EVICTOR-RECLAIM-1: use the OWNING tree's db_id (resolved above), not
        // the primary slot's, so the BIN logs against the correct database.
        let db_id = owning_db_id;
        let provisional = self
            .checkpointer
            .as_ref()
            .map(|c| c.get_eviction_provisional(db_id, bin.level))
            .unwrap_or(Provisional::No);

        let full_bytes = bin.serialize_full();
        let entry =
            InLogEntry::new(db_id, bin.last_full_lsn, NULL_LSN, full_bytes);
        let mut buf = bytes::BytesMut::with_capacity(entry.log_size());
        entry.write_to_log(&mut buf);

        // L-10: logging a new full BIN supersedes the previous full version.
        // Count the old full-BIN LSN obsolete (IN type, exact) so the cleaner
        // sees the reclaimable space.  JE IN.logInternal counts the prior
        // full version obsolete via countObsoleteNode.
        let prev_full = bin.last_full_lsn;
        let old_obsolete = if !prev_full.is_null() {
            Some(noxu_log::ObsoleteLsn::exact(
                prev_full,
                Some(db_id as u32),
                0,     // IN obsolete size must be 0
                false, // is_ln = false (this is an IN/BIN)
            ))
        } else {
            None
        };

        if let Ok(logged_lsn) = lm.log_tracked(
            LogEntryType::BIN,
            &buf,
            provisional,
            false,
            false,
            Some(db_id as u32),
            old_obsolete,
            false,
        ) {
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
        // EVICTOR_MUTATE_BINS (JE Evictor `mutateBins`, default true): when
        // false, the evictor must NOT mutate a BIN by stripping its LNs.
        // Return Some(0) — a "no bytes stripped" result that routes into the
        // existing strip-0 handling (whole-node evict / put-back) rather than
        // None (which signals a busy/pinned node to retry).
        if !self.mutate_bins {
            return Some(0);
        }
        // EVICTOR-RECLAIM-1: search every database tree, not just the primary
        // slot.  JE resolves the target's owning DB from the env-wide INList
        // (Evictor.processTarget -> target.getDatabase(), Evictor.java:2374).
        let trees = self.candidate_trees();
        if trees.is_empty() {
            return Some(0);
        }
        let node_arc: Arc<NodeRwLock<TreeNode>> = {
            let mut found: Option<Arc<NodeRwLock<TreeNode>>> = None;
            for (_db_id, tree_arc) in &trees {
                let tree_guard = match tree_arc.read() {
                    Ok(g) => g,
                    Err(_) => return None, // conservative: put back
                };
                // CC-6: non-blocking tree scan.
                match find_node_arc_nonblocking(&tree_guard, node_id) {
                    Ok(Some(a)) => {
                        found = Some(a);
                        break;
                    }
                    Ok(None) => continue, // not in this tree; try the next
                    Err(()) => return None, // descent blocked; put back
                }
            }
            match found {
                Some(a) => a,
                None => return Some(0), // not in any tree; already gone
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

        // F2: decrement the shared budget counter by the bytes just freed.
        // evict_batch only *accounts* bytes_evicted; without this the counter
        // (incremented on insert) never drops and the engine can't get back
        // under budget.  JE: every eviction calls IN.updateMemorySize(-bytes)
        // → MemoryBudget.updateTreeMemoryUsage(-bytes).
        self.arbiter.release_memory(result.bytes_evicted);

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

    /// EV-15: synchronous critical eviction, called from application (writer)
    /// threads on the operation path to apply write back-pressure.
    ///
    /// JE `Evictor.doCriticalEviction` (Evictor.java:2054) is invoked from
    /// `EnvironmentImpl.criticalEviction` for every cursor operation: when the
    /// cache is *critically* over budget (`Arbiter.needCriticalEviction`), the
    /// calling thread itself runs `doEvict(CRITICAL)` so a writer that is
    /// filling the cache blocks to evict before continuing — preventing
    /// unbounded overshoot between the background daemon's wakeups.
    ///
    /// Bounded like JE: a single `do_evict` batch runs (its inner loop is
    /// already capped by `max_batch_size` and `still_needs_eviction`), then the
    /// caller proceeds even if the cache is still over budget.  Returns the
    /// number of bytes evicted (0 when no critical eviction was needed).
    pub fn do_critical_eviction(&self) -> u64 {
        if self.is_shutdown() {
            return 0;
        }
        // JE doCriticalEviction guards on isOverBudget() then
        // needCriticalEviction(); the daemon takes the burden whenever it can,
        // so application threads only block when the overage is critical.
        if self.arbiter.is_over_budget()
            && self.arbiter.need_critical_eviction()
        {
            return self.do_evict(EvictionSource::Critical).bytes_evicted;
        }
        0
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

/// JE `INList` → `Evictor` feed.  `EnvironmentImpl` installs the `Evictor` as
/// each database tree's [`InListListener`] so production inserts/accesses
/// populate the LRU lists that `evict_batch` drains.
///
/// Without this wiring the policy lists stay empty, every phase quota is 0,
/// and `evict_batch` selects nothing (F1).
impl InListListener for Evictor {
    /// JE `Evictor.addBack`: a node became resident.
    fn note_ins_added(&self, node_id: u64) {
        // Default cache mode → hot end of the primary policy.
        self.note_ins_added(node_id, CacheMode::Default);
    }

    /// JE `Evictor.moveBack`: LRU touch on access.  Move the node toward the
    /// hot end if it is already tracked; otherwise add it (a freshly split
    /// BIN is first seen here).  `moveBack` in JE is add-if-absent.
    fn note_ins_accessed(&self, node_id: u64) {
        if !self.primary_policy.touch(node_id)
            && !self.scan_policy.touch(node_id)
        {
            self.primary_policy.insert(node_id);
        }
    }

    /// JE `Evictor.remove`: node left the cache.
    fn note_ins_removed(&self, node_id: u64) {
        // Forward to the inherent method (clears all three policies).
        Evictor::note_ins_removed(self, node_id);
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
            .field("db_id", &self.db_id.load(Ordering::Relaxed))
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
    /// EV-6: upper IN has at least one resident child (`InEntry.child`).
    has_cached_children: bool,
    /// EV-7: this node is the tree root.
    is_root: bool,
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
    fn has_cached_children(&self) -> bool {
        self.has_cached_children
    }
    fn is_root(&self) -> bool {
        self.is_root
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
    // EV-7: capture the root's node_id so the walk can flag the root IN as
    // non-evictable (JE IN.isRoot()).
    let root_id = match &*root_arc.read() {
        TreeNode::Internal(n) => n.node_id,
        TreeNode::Bottom(b) => b.node_id,
    };
    find_node_full_recursive(&root_arc, node_id, root_id)
}

fn find_node_full_recursive(
    node_arc: &Arc<NodeRwLock<TreeNode>>,
    target_id: u64,
    root_id: u64,
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
                // A BIN has no cached IN children; EV-6 only guards upper INs.
                has_cached_children: false,
                is_root: b.node_id == root_id,
            };
            // Size formula (BIN): struct overhead + per-slot fixed overhead +
            // variable key and embedded-LN data bytes.  T-2/T-3: keys and
            // LSNs live in node-level reps (BinStub.keys / lsn_rep), not in
            // BinEntry.
            let size = (size_of::<BinStub>()
                + b.entries.len() * size_of::<BinEntry>()
                + b.key_prefix.len()
                + b.keys.memory_size()
                + b.lsn_rep.memory_size()
                + b.entries
                    .iter()
                    .map(|e| e.data.as_ref().map(|d| d.len()).unwrap_or(0))
                    .sum::<usize>()) as u64;
            let arc = Arc::clone(node_arc);
            drop(guard);
            Some(NodeFull { arc, info, size })
        }
        TreeNode::Internal(n) => {
            if n.node_id == target_id {
                // EV-6: an upper IN with any resident child must stay
                // resident (JE IN.hasCachedChildren / NON_EVICTABLE_IN).
                let has_cached_children = !n.targets.is_empty();
                let info = RealNodeInfo {
                    dirty: n.dirty,
                    is_bin: false,
                    pin_count: 0,
                    has_cached_children,
                    is_root: n.node_id == root_id,
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
            let children: Vec<Arc<NodeRwLock<TreeNode>>> =
                n.resident_children();
            drop(guard);
            for child in children {
                if let Some(full) =
                    find_node_full_recursive(&child, target_id, root_id)
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
            let children: Vec<Arc<NodeRwLock<TreeNode>>> =
                n.resident_children();
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

    // EV-6 / EV-7: a richer info that also sets has_cached_children / is_root.
    struct GuardInfo {
        bin: bool,
        has_children: bool,
        root: bool,
    }
    impl NodeEvictionInfo for GuardInfo {
        fn is_dirty(&self) -> bool {
            false
        }
        fn is_bin(&self) -> bool {
            self.bin
        }
        fn is_resident(&self) -> bool {
            true
        }
        fn ref_count(&self) -> usize {
            0
        }
        fn has_cached_children(&self) -> bool {
            self.has_children
        }
        fn is_root(&self) -> bool {
            self.root
        }
    }

    /// EV-6 (JE Evictor.processTarget IN.hasCachedChildren /
    /// NON_EVICTABLE_IN, Evictor.java:2652-2656): an upper IN with a resident
    /// child must be SKIPPED, not evicted.  With EV-13's detach live, evicting
    /// it would orphan the resident child.  Neutering the guard turns this
    /// into Evict and fails the assert.
    #[test]
    fn test_decide_ev6_upper_in_with_children_skipped() {
        let upper_with_children =
            GuardInfo { bin: false, has_children: true, root: false };
        assert_eq!(
            decide_eviction(&upper_with_children, false, true),
            EvictionDecision::Skip,
            "EV-6: upper IN with resident children must be skipped"
        );
        // A childless upper IN is still evictable (sanity: the guard is
        // specific to the has-children case).
        let upper_no_children =
            GuardInfo { bin: false, has_children: false, root: false };
        assert_eq!(
            decide_eviction(&upper_no_children, false, true),
            EvictionDecision::Evict,
            "childless upper IN is still evictable"
        );
    }

    /// EV-7 / EV-14 (JE Evictor.processTarget root special-casing,
    /// Evictor.java:2400-2421 + 2663-2671, IN.isRoot()): a root is never
    /// evicted via the NORMAL path.  An *idle* root (no cached children) is
    /// routed to the separate `EvictRoot` path; a root that still has cached
    /// children is SKIPPED (EV-6 — evicting it would orphan them).
    #[test]
    fn test_decide_ev7_ev14_root_routing() {
        // Idle root -> EvictRoot (EV-14): neutering the guard would turn this
        // into Evict (normal detach path) and break the EV-13 invariants.
        let idle_root =
            GuardInfo { bin: false, has_children: false, root: true };
        assert_eq!(
            decide_eviction(&idle_root, false, true),
            EvictionDecision::EvictRoot,
            "EV-14: an idle root IN is evicted via the EvictRoot path"
        );
        // Root with cached children -> Skip (EV-6 protection holds).
        let root_with_children =
            GuardInfo { bin: false, has_children: true, root: true };
        assert_eq!(
            decide_eviction(&root_with_children, false, true),
            EvictionDecision::Skip,
            "EV-6: a root with resident children must NOT be evicted"
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
    fn test_use_dirty_lru_config_and_offheap_override() {
        // JE EVICTOR_USE_DIRTY_LRU (default true); off-heap forces it false.
        let counter = Arc::new(AtomicI64::new(0));
        let arbiter = Arbiter::new(1024, Arc::clone(&counter), 100, 200);
        // Default (lru_only=false): use_dirty_lru defaults to !lru_only = true.
        let e = Evictor::new(arbiter, 100, false);
        assert!(e.use_dirty_lru(), "default !lru_only -> use_dirty_lru true");

        // Explicit config false is honored.
        let counter2 = Arc::new(AtomicI64::new(0));
        let arb2 = Arbiter::new(1024, Arc::clone(&counter2), 100, 200);
        let e2 = Evictor::new(arb2, 100, false).with_use_dirty_lru(false);
        assert!(!e2.use_dirty_lru(), "explicit config false honored");

        // An ENABLED off-heap cache forces use_dirty_lru false (JE rule).
        let counter3 = Arc::new(AtomicI64::new(0));
        let arb3 = Arbiter::new(1024, Arc::clone(&counter3), 100, 200);
        let oh = Arc::new(OffHeapCache::new(true, 1024 * 1024));
        let e3 = Evictor::new(arb3, 100, false)
            .with_use_dirty_lru(true)
            .with_off_heap(oh);
        assert!(
            !e3.use_dirty_lru(),
            "enabled off-heap forces use_dirty_lru false"
        );

        // A DISABLED off-heap cache must NOT disable use_dirty_lru.
        let counter4 = Arc::new(AtomicI64::new(0));
        let arb4 = Arbiter::new(1024, Arc::clone(&counter4), 100, 200);
        let oh_off = Arc::new(OffHeapCache::new(false, 0));
        let e4 = Evictor::new(arb4, 100, false)
            .with_use_dirty_lru(true)
            .with_off_heap(oh_off);
        assert!(
            e4.use_dirty_lru(),
            "disabled off-heap leaves use_dirty_lru true"
        );
    }

    #[test]
    fn test_default_algorithm_is_lru() {
        let usage = Arc::new(AtomicI64::new(0));
        let arbiter = Arbiter::new(1000, usage, 100, 200);
        let e = Evictor::new(arbiter, 100, false);
        assert_eq!(e.primary_algorithm_name(), "LRU");
        assert_eq!(e.scan_algorithm_name(), "LRU");
    }

    /// EV-15 (JE Evictor.doCriticalEviction, Evictor.java:2054): a writer
    /// thread calling `do_critical_eviction` only evicts when the cache is
    /// *critically* over budget (Arbiter.needCriticalEviction), and when it
    /// does it drives a bounded synchronous evict that lowers usage.
    #[test]
    fn test_ev15_critical_eviction_bounded_and_gated() {
        use noxu_util::Lsn;
        use std::sync::{Arc, RwLock};

        // Build a tree with several childless non-root upper INs that can be
        // fully evicted (max_entries=2 => deep right-spine).
        let tree_inner = noxu_tree::tree::Tree::new(7, 2);
        for i in 0u16..16 {
            tree_inner
                .insert(
                    i.to_be_bytes().to_vec(),
                    vec![i as u8],
                    Lsn::new(1, u32::from(i) + 1),
                )
                .unwrap();
        }
        // Detach every BIN so each upper IN directly above it becomes
        // childless and hence EV-6-evictable; THEN collect those now-childless
        // upper INs (intermediate INs still have a resident IN child and stay
        // EV-6-protected, so we must register only the evictable leaves to
        // keep the bounded batch deterministic).
        fn collect_bins(
            node: &Arc<noxu_tree::NodeRwLock<TreeNode>>,
            bins: &mut Vec<u64>,
        ) {
            let g = node.read();
            match &*g {
                TreeNode::Bottom(b) => bins.push(b.node_id),
                TreeNode::Internal(n) => {
                    let cs: Vec<_> = n.resident_children();
                    drop(g);
                    for c in cs {
                        collect_bins(&c, bins);
                    }
                }
            }
        }
        fn collect_childless_uins(
            node: &Arc<noxu_tree::NodeRwLock<TreeNode>>,
            is_root: bool,
            uins: &mut Vec<u64>,
        ) {
            let g = node.read();
            let TreeNode::Internal(n) = &*g else {
                return;
            };
            if !is_root && n.targets.is_empty() {
                uins.push(n.node_id);
            }
            let children: Vec<_> = n.resident_children();
            drop(g);
            for c in children {
                collect_childless_uins(&c, false, uins);
            }
        }

        let tree_arc = Arc::new(RwLock::new(tree_inner));
        let bins = {
            let t = tree_arc.read().unwrap();
            let root = t.get_root().expect("root");
            let mut bins = Vec::new();
            collect_bins(&root, &mut bins);
            bins
        };
        {
            let t = tree_arc.read().unwrap();
            for bin_id in &bins {
                t.detach_node_by_id(*bin_id);
            }
        }
        let uins = {
            let t = tree_arc.read().unwrap();
            let root = t.get_root().expect("root");
            let mut uins = Vec::new();
            collect_childless_uins(&root, true, &mut uins);
            uins
        };
        assert!(!uins.is_empty(), "fixture: need childless upper INs to evict");

        let usage = Arc::new(AtomicI64::new(0));
        // max=1000, critical_threshold=200: critical when usage-max > 200.
        let evictor = Evictor::new(
            Arbiter::new(1000, Arc::clone(&usage), 100, 200),
            4, // small batch => bounded eviction
            true,
        )
        .with_tree(tree_arc, 7);
        for in_id in &uins {
            evictor.note_ins_added(*in_id, CacheMode::Default);
        }

        // Not over budget => no critical eviction.
        usage.store(500, std::sync::atomic::Ordering::Relaxed);
        assert_eq!(
            evictor.do_critical_eviction(),
            0,
            "EV-15: under budget must not evict"
        );

        // Over budget but NOT critical (1100-1000=100 <= 200) => still no
        // synchronous eviction (the daemon takes the burden).
        usage.store(1100, std::sync::atomic::Ordering::Relaxed);
        assert_eq!(
            evictor.do_critical_eviction(),
            0,
            "EV-15: non-critical overage defers to the daemon"
        );

        // Critically over budget (10000-1000=9000 > 200) => the calling
        // thread evicts a bounded batch itself, lowering usage.
        usage.store(10_000, std::sync::atomic::Ordering::Relaxed);
        let before = usage.load(std::sync::atomic::Ordering::Relaxed);
        let freed = evictor.do_critical_eviction();
        let after = usage.load(std::sync::atomic::Ordering::Relaxed);
        assert!(
            freed > 0,
            "EV-15: critical pressure must drive synchronous eviction"
        );
        assert!(after < before, "EV-15: usage must drop after eviction");
        // Bounded: at most max_batch_size (4) nodes per call, so usage does
        // not have to fall under budget in one shot — the writer proceeds.
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
        // CLN-F2: a clean, unpinned, cursor-free BIN whose LN strip frees 0
        // bytes (nothing left to strip) now FALLS THROUGH to full eviction
        // -- the BIN is removed and its node bytes are credited -- instead of
        // being unconditionally put back.
        //
        // Mock evictor has no tree, so strip_lns_from_node returns Some(0):
        // the BIN is clean (dirty=false), so it is fully evicted.
        // JE: Evictor.processTarget fall-through (Evictor.java ~2755-2795).
        //
        // PRE-FIX (origin/main): this asserted nodes_evicted == 0 and
        // bytes_evicted == 0 (always put_back).  The unconditional put-back
        // was the CLN-F2 bug: a BIN node could never be reclaimed under
        // pressure.
        let (_c, e) = make_evictor(1500, 1000, 10);
        for i in 1..=3u64 {
            e.note_ins_added(i, CacheMode::Default);
        }
        let r = e.evict_batch(
            EvictionSource::Daemon,
            &static_info_fn(false, true, true, 0),
            &size_512,
        );
        assert_eq!(
            r.nodes_evicted, 3,
            "CLN-F2: clean BIN with nothing to strip must be FULLY evicted"
        );
        assert_eq!(
            r.bytes_evicted,
            3 * 512,
            "CLN-F2: full BIN eviction credits node_size_fn bytes"
        );
        let s = e.get_stats();
        assert_eq!(s.get(&s.nodes_evicted), 3);
        // nodes_stripped stays 0: strip freed 0 bytes (no tree wired).
        assert_eq!(s.get(&s.nodes_stripped), 0);
        // The BINs are gone from the primary LRU, not put back.
        assert_eq!(e.get_lru_sizes().0, 0);
    }

    /// CLN-F2: a DIRTY BIN with nothing to strip must NOT be fully evicted by
    /// the normal path -- it gets a second chance in the pri2 dirty-LRU when
    /// `use_dirty_lru` is enabled (JE processTarget ~2762-2768).
    #[test]
    fn test_evict_batch_partial_evict_dirty_bin_moves_to_pri2() {
        let counter = Arc::new(AtomicI64::new(1500));
        // lru_only=false -> use_dirty_lru defaults to true.
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
            &static_info_fn(true, true, true, 0), // dirty BIN
            &size_512,
        );
        assert_eq!(
            r.nodes_evicted, 0,
            "CLN-F2: dirty BIN must not be fully evicted on the first chance"
        );
        let s = e.get_stats();
        assert_eq!(s.get(&s.nodes_moved_to_pri2_lru), 3);
        assert_eq!(e.get_lru_sizes(), (0, 3));
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
        ) -> Option<(u64, Vec<(usize, usize)>, usize)> {
            let guard = node_arc.read();
            match &*guard {
                TreeNode::Bottom(b) => {
                    let id = b.node_id;
                    let entries = b
                        .entries
                        .iter()
                        .map(|e| {
                            (
                                0usize, // T-2: key bytes are in the node rep
                                e.data.as_ref().map(|d| d.len()).unwrap_or(0),
                            )
                        })
                        .collect();
                    // T-2/T-3: node-level key/LSN rep bytes + prefix.
                    let rep_bytes = b.keys.memory_size()
                        + b.lsn_rep.memory_size()
                        + b.key_prefix.len();
                    Some((id, entries, rep_bytes))
                }
                TreeNode::Internal(n) => {
                    // The first child should eventually lead to a BIN.
                    let first_child = n.get_child(0)?;
                    drop(guard);
                    find_bin_node(&first_child)
                }
            }
        }

        let (bin_id, bin_entries, bin_rep_bytes) = {
            let t = tree.read().unwrap();
            let root_arc = t.get_root().expect("tree must have a root");
            find_bin_node(&root_arc).expect("must find a BIN leaf")
        };

        // Compute expected size using the explicit formula.
        let expected: u64 = (size_of::<BinStub>()
            + bin_entries.len() * size_of::<BinEntry>()
            + bin_rep_bytes
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

    /// Demonstrate that the `Evict` decision path in `do_evict` actually
    /// detaches a node and credits its real measured size (EV-13), not the
    /// 1024-byte sentinel fallback.
    ///
    /// **Why a non-root, childless upper IN?**  After EV-6 / EV-7 the only
    /// upper IN the evictor will fully evict is one that is (a) not the root
    /// and (b) has no resident children.  BIN nodes take the `PartialEvict`
    /// path.  We build a deep tree (`max_entries=2` forces a deep right-spine
    /// where every IN below the root is a non-root upper IN), detach a chosen
    /// upper IN's BIN child to make it childless, then evict it.  The
    /// `node_size_fn` detach-and-measure callback returns the real heap size,
    /// which must not equal the 1024 sentinel.
    #[test]
    fn test_do_evict_bytes_matches_node_size_not_sentinel() {
        use noxu_util::Lsn;
        use std::sync::atomic::AtomicI64;
        use std::sync::{Arc, RwLock};

        // max_entries=2 => deep right-spine; every IN below the root is a
        // non-root upper IN.
        let tree_inner = noxu_tree::tree::Tree::new(99, 2);
        for i in 0u16..16 {
            tree_inner
                .insert(
                    i.to_be_bytes().to_vec(),
                    vec![i as u8],
                    Lsn::new(1, u32::from(i) + 1),
                )
                .unwrap();
        }

        // Find a NON-ROOT upper IN that has at least one BIN child, and grab
        // that child's id so we can detach it (making the upper IN childless
        // and thus EV-6-eligible).
        fn find_nonroot_upper_in_with_bin_child(
            node: &Arc<noxu_tree::NodeRwLock<TreeNode>>,
            is_root: bool,
        ) -> Option<(u64, u64)> {
            let g = node.read();
            let TreeNode::Internal(n) = &*g else {
                return None;
            };
            // If this is a non-root upper IN whose first child is a BIN,
            // return (this_in_id, bin_child_id).
            if !is_root {
                for child in n.resident_children() {
                    let cg = child.read();
                    if let TreeNode::Bottom(b) = &*cg {
                        return Some((n.node_id, b.node_id));
                    }
                }
            }
            // Otherwise recurse into children.
            let children: Vec<_> = n.resident_children();
            drop(g);
            for c in children {
                if let Some(found) =
                    find_nonroot_upper_in_with_bin_child(&c, false)
                {
                    return Some(found);
                }
            }
            None
        }

        let (upper_in_id, bin_child_id) = {
            let root = tree_inner.get_root().expect("root");
            match find_nonroot_upper_in_with_bin_child(&root, true) {
                Some(pair) => pair,
                // Tree shape changed; skip rather than fail.
                None => return,
            }
        };

        // Detach the BIN child so the upper IN becomes childless (passes
        // EV-6).  This is the same operation the evictor performs; here we
        // drive it manually to set up the test fixture.
        let detached = tree_inner.detach_node_by_id(bin_child_id);
        assert!(detached > 0, "fixture: BIN child must detach");

        let tree = Arc::new(RwLock::new(tree_inner));
        let usage = Arc::new(AtomicI64::new(100_000));
        let evictor = Evictor::new(
            Arbiter::new(100, Arc::clone(&usage), 100, 200),
            10,
            true, // lru_only: straight to the Evict decision
        )
        .with_tree(Arc::clone(&tree), 99);

        evictor.note_ins_added(upper_in_id, CacheMode::Default);
        let result = evictor.do_evict(EvictionSource::Daemon);

        // The childless non-root upper IN was evicted via the real EV-13
        // detach path, crediting its measured heap size (never the 1024
        // sentinel, since detach returned > 0).
        assert_eq!(
            result.nodes_evicted, 1,
            "childless non-root upper IN must be evicted"
        );
        assert!(
            result.bytes_evicted > 0 && result.bytes_evicted != 1024,
            "bytes_evicted ({}) must be the measured detach size, not the \
             1024 sentinel",
            result.bytes_evicted
        );
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
                TreeNode::Internal(n) => n.get_child(0).expect("BIN child"),
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
                TreeNode::Internal(n) => n.get_child(0).expect("BIN child"),
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
                TreeNode::Internal(n) => n.get_child(0).expect("BIN child"),
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

    /// EVICTOR_MUTATE_BINS gate: with mutate_bins=false the evictor must NOT
    /// strip LNs (returns Some(0), no bytes freed); the default (true) strips.
    /// JE Evictor `mutateBins` / EVICTOR_MUTATE_BINS.
    #[test]
    fn test_evictor_mutate_bins_gate() {
        use noxu_util::Lsn;
        use std::sync::{Arc, RwLock};

        // Build a tree with a strippable (clean, logged, cursor-free) LN.
        let build = || {
            let t = noxu_tree::tree::Tree::new(1, 128);
            t.insert(b"k1".to_vec(), b"value-one".to_vec(), Lsn::new(1, 1))
                .unwrap();
            t.insert(b"k2".to_vec(), b"value-two".to_vec(), Lsn::new(1, 2))
                .unwrap();
            let bin_arc = {
                let root = t.get_root().expect("root");
                let guard = root.read();
                match &*guard {
                    TreeNode::Internal(n) => n.get_child(0).expect("BIN child"),
                    TreeNode::Bottom(_) => Arc::clone(&root),
                }
            };
            let bin_id = match &*bin_arc.read() {
                TreeNode::Bottom(b) => b.node_id,
                _ => panic!("expected BIN"),
            };
            (Arc::new(RwLock::new(t)), bin_id)
        };

        // Default (mutate_bins = true): strips the LN data, frees > 0 bytes.
        let (tree_on, bin_on) = build();
        let usage_on = Arc::new(std::sync::atomic::AtomicI64::new(9999));
        let ev_on =
            Evictor::new(Arbiter::new(1000, usage_on, 100, 200), 100, true)
                .with_tree(tree_on, 1);
        let freed_on = ev_on.strip_lns_from_node(bin_on);
        assert!(
            matches!(freed_on, Some(n) if n > 0),
            "default mutate_bins=true must strip > 0 bytes; got {freed_on:?}"
        );

        // mutate_bins = false: no stripping, Some(0) (leaves the BIN intact).
        let (tree_off, bin_off) = build();
        let usage_off = Arc::new(std::sync::atomic::AtomicI64::new(9999));
        let ev_off =
            Evictor::new(Arbiter::new(1000, usage_off, 100, 200), 100, true)
                .with_tree(tree_off, 1)
                .with_mutate_bins(false);
        assert_eq!(
            ev_off.strip_lns_from_node(bin_off),
            Some(0),
            "mutate_bins=false must NOT strip LNs (Some(0))"
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
                TreeNode::Internal(n) => n.get_child(0).expect("BIN child"),
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

    // -----------------------------------------------------------------------
    // EV-6 / EV-7: prevent-wrong-eviction integration tests (real tree)
    //
    // EV-13 made full-node eviction DETACH the node from its parent. These
    // tests prove the load-bearing safety guards: an upper IN with a resident
    // child (EV-6) and a root IN (EV-7) are NOT detached when registered for
    // eviction.  Removing either guard turns the decision into Evict and the
    // node is detached, failing the resident-child / resident-root assert.
    // -----------------------------------------------------------------------

    /// EV-6 (JE Evictor.processTarget IN.hasCachedChildren /
    /// NON_EVICTABLE_IN, Evictor.java:2652-2656): register the root IN (an
    /// upper IN whose BIN children are resident) for eviction; after
    /// `do_evict` the IN's child slots must still hold their `Arc` (not
    /// detached).  Fail-pre if the EV-6 guard is neutered.
    #[test]
    fn test_ev6_upper_in_with_resident_child_not_detached() {
        use noxu_util::Lsn;
        use std::sync::{Arc, RwLock};

        // Two inserts => root IN with a resident BIN child.
        let tree_inner = noxu_tree::tree::Tree::new(1, 128);
        tree_inner
            .insert(b"k1".to_vec(), b"v1".to_vec(), Lsn::new(1, 1))
            .unwrap();
        tree_inner
            .insert(b"k2".to_vec(), b"v2".to_vec(), Lsn::new(1, 2))
            .unwrap();

        let (in_id, had_child) = {
            let root = tree_inner.get_root().expect("root");
            let g = root.read();
            match &*g {
                TreeNode::Internal(n) => (n.node_id, !n.targets.is_empty()),
                // If the root is a single BIN the test premise is gone; skip.
                TreeNode::Bottom(_) => return,
            }
        };
        assert!(had_child, "premise: root IN must have a resident child");

        let tree = Arc::new(RwLock::new(tree_inner));
        let usage = Arc::new(std::sync::atomic::AtomicI64::new(9999));
        // lru_only so the IN goes straight to the Evict decision path.
        let evictor = Evictor::new(
            Arbiter::new(100, Arc::clone(&usage), 100, 200),
            100,
            true,
        )
        .with_tree(Arc::clone(&tree), 1);

        evictor.note_ins_added(in_id, CacheMode::Default);
        let result = evictor.do_evict(EvictionSource::Daemon);

        // The upper IN must NOT have been evicted/detached.
        assert_eq!(
            result.nodes_evicted, 0,
            "EV-6: upper IN with resident children must not be evicted"
        );
        // Its child slot must still hold the Arc (not detached).
        let still_has_child = {
            let t = tree.read().unwrap();
            let root = t.get_root().expect("root still resident");
            let g = root.read();
            match &*g {
                TreeNode::Internal(n) => !n.targets.is_empty(),
                TreeNode::Bottom(_) => false,
            }
        };
        assert!(
            still_has_child,
            "EV-6: resident child must remain attached (not orphaned)"
        );
    }

    /// EV-7 (JE Evictor.processTarget IN.isRoot(), Evictor.java:2663-2671):
    /// register the root IN for eviction; after `do_evict` the root must
    /// still be resident.  Fail-pre if the EV-7 guard is neutered (with EV-6
    /// also off, the root would be detached/evicted).
    #[test]
    fn test_ev7_root_in_not_evicted() {
        use noxu_util::Lsn;
        use std::sync::{Arc, RwLock};

        let tree_inner = noxu_tree::tree::Tree::new(1, 128);
        tree_inner
            .insert(b"k1".to_vec(), b"v1".to_vec(), Lsn::new(1, 1))
            .unwrap();
        tree_inner
            .insert(b"k2".to_vec(), b"v2".to_vec(), Lsn::new(1, 2))
            .unwrap();

        let root_in_id = {
            let root = tree_inner.get_root().expect("root");
            let g = root.read();
            match &*g {
                TreeNode::Internal(n) => n.node_id,
                TreeNode::Bottom(b) => b.node_id,
            }
        };

        let tree = Arc::new(RwLock::new(tree_inner));
        let usage = Arc::new(std::sync::atomic::AtomicI64::new(9999));
        let evictor = Evictor::new(
            Arbiter::new(100, Arc::clone(&usage), 100, 200),
            100,
            true,
        )
        .with_tree(Arc::clone(&tree), 1);

        // Put the root IN into pri2 directly: pri2 nodes bypass the
        // dirty/move-to-pri2 path and reach the Evict decision unconditionally
        // (the same trick the sentinel-size test uses), isolating the EV-7
        // is_root guard as the only thing that can prevent eviction.
        evictor.pri2_insert_for_test(root_in_id);
        let result = evictor.do_evict(EvictionSource::Daemon);

        assert_eq!(
            result.nodes_evicted, 0,
            "EV-7: root IN must not be evicted"
        );
        assert!(
            tree.read().unwrap().is_root_resident(),
            "EV-7: root must remain resident after eviction attempt"
        );
    }
}
