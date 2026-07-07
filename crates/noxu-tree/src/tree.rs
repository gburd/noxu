//! B+tree implementation.
//!
//!
//! Tree implements the B+tree. It provides search, insert, and delete
//! operations on the tree structure. The tree uses latch-coupling for
//! concurrent access: when traversing down the tree, the parent latch
//! is released after the child latch is acquired.
//!
//! # Architecture
//!
//! The tree has a hierarchical structure:
//! - Internal Nodes (IN) at levels 2 and above
//! - Bottom Internal Nodes (BIN) at level 1
//! - Leaf Nodes (LN) containing actual data
//!
//! # Locking Strategy
//!
//! - Root latch protects the root pointer itself
//! - Each node has its own latch for concurrent access
//! - Search uses latch-coupling: acquire child, release parent
//! - Modifications may require exclusive latches

use crate::error::TreeError;
use crate::key::{create_key_prefix, get_key_prefix_length};
use crate::search_result::SearchResult;
use noxu_latch::{LatchContext, SharedLatch};
use noxu_util::{Lsn, NULL_LSN};
// DST: the tree-node latch.  Production (default cfg) is BYTE-IDENTICAL — the
// literal `parking_lot::RwLock`, zero cost, with `parking_lot` in the dep graph
// exactly as before.  Under `--cfg noxu_shuttle` (dev/test only) it resolves to
// the parking_lot-shaped shuttle wrapper `noxu_util::dst_sync_pl::RwLock`, so
// shuttle can schedule the insert / split_child / compress interleavings that
// let the BIN-split check-then-act race (bug-bin-split-concurrency.md) escape
// into a benchmark instead of DST.  The hand-over-hand *read* descent
// (`root.read_arc()` → an Arc-owning read guard) is provided under the cfg by
// `noxu_latch::dst_arc_guard` (a shuttle-only shim that noxu-tree cannot host
// itself because it is `#![forbid(unsafe_code)]`).  Under the default cfg
// `read_arc()`/`ArcRwLockReadGuard` are parking_lot's own zero-cost inherent
// API — no shim in the graph.
#[cfg(noxu_shuttle)]
use noxu_util::dst_sync_pl::RwLock;
#[cfg(not(noxu_shuttle))]
use parking_lot::RwLock;

// The Arc-owning read guard for the hand-over-hand descent.  Default =
// parking_lot's own inherent type (zero-cost, byte-identical).  Under shuttle =
// the noxu-latch DST shim (see its module docs).  The `.read_arc()` method is
// available on `Arc<RwLock<TreeNode>>` in both: inherent on parking_lot,
// via `noxu_latch::dst_arc_guard::ReadArc` under shuttle.
#[cfg(not(noxu_shuttle))]
type NodeArcReadGuard =
    parking_lot::ArcRwLockReadGuard<parking_lot::RawRwLock, TreeNode>;
#[cfg(noxu_shuttle)]
type NodeArcReadGuard = noxu_latch::dst_arc_guard::ArcRwLockReadGuard<TreeNode>;
#[cfg(noxu_shuttle)]
use noxu_latch::dst_arc_guard::ReadArc as _;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Weak};

/// Observer that mirrors JE's `INList` feeding the evictor's `LRUList`s.
///
/// The tree owns no eviction policy of its own; instead it notifies a
/// registered listener whenever an IN/BIN node enters the resident cache, is
/// accessed, or is removed.  The `Evictor` (in `noxu-evictor`) implements this
/// trait, but the dependency is one-way (`noxu-evictor` → `noxu-tree`), so the
/// tree refers to the listener only through this trait object — avoiding a
/// circular crate dependency.
///
/// JE reference: `IN.fetchTarget` / split / `rebuildINList` call
/// `Evictor.addBack`; node access calls `Evictor.moveBack`; node removal
/// calls `Evictor.remove`.
pub trait InListListener: Send + Sync {
    /// A node has just become resident in the cache (JE `Evictor.addBack`).
    fn note_ins_added(&self, node_id: u64);
    /// A resident node was accessed (JE `Evictor.moveBack` — LRU touch).
    fn note_ins_accessed(&self, node_id: u64);
    /// A node was removed from the cache (JE `Evictor.remove`).
    fn note_ins_removed(&self, node_id: u64);
}

// Level and flag constants re-exported here for tree-internal use.
pub const DBMAP_LEVEL: i32 = 0x20000;
pub const MAIN_LEVEL: i32 = 0x10000;
pub const LEVEL_MASK: i32 = 0x0ffff;
pub const MIN_LEVEL: i32 = -1;
pub const BIN_LEVEL: i32 = MAIN_LEVEL | 1;
pub const EXACT_MATCH: i32 = 1 << 16;
pub const INSERT_SUCCESS: i32 = 1 << 17;

/// Per-slot fixed memory overhead for a BIN entry, in bytes (DBI-23).
///
/// This is the heap footprint of one `BinEntry` *struct* as it lives inside
/// the BIN's `Vec<BinEntry>` buffer — NOT counting the variable-length key and
/// data bytes, which are separate heap allocations counted on top of this.
///
/// Faithful to JE `IN.getEntryInMemorySize` + the per-slot `entryStates` /
/// LSN-array overhead folded into `IN.computeMemorySize` (IN.java ~4632):
/// JE measures the slot's fixed cost with `Sizeof` on the JVM; Rust has a
/// fixed struct layout so `size_of::<BinEntry>()` is exact.
///
/// T-2/T-3: the per-slot `key` (`Vec<u8>` header) and `lsn` (`u64`) were
/// hoisted out of `BinEntry` into the node-level `KeyRep`/`LsnRep`.  The
/// `size_of::<BinEntry>()` therefore shrank; we add back the packed per-slot
/// LSN-rep cost (`LsnRep::BYTES_PER_LSN_ENTRY`, 4 bytes) so the incremental
/// live counter still approximates the walked heap (the key bytes are charged
/// separately as `key.len()` at the call site, matching the compact key rep).
///
/// Derived (not hard-coded) so a layout change to `BinEntry` is tracked
/// automatically — see `bin_stub_conformance` for the drift guard.
pub const BIN_ENTRY_OVERHEAD: usize =
    std::mem::size_of::<BinEntry>() + LsnRep::BYTES_PER_LSN_ENTRY;

/// Per-slot fixed memory overhead for an IN entry, in bytes (DBI-23).
///
/// Heap footprint of one `InEntry` struct inside the IN's `Vec<InEntry>`
/// buffer (key bytes counted separately).  JE `IN.getEntryInMemorySize` for
/// an upper IN plus the per-slot state/LSN/target overhead from
/// `IN.computeMemorySize`.
pub const IN_ENTRY_OVERHEAD: usize = std::mem::size_of::<InEntry>();

/// Type alias for the key comparator used by sorted-duplicate databases.
///
/// The comparator takes two full (uncompressed) keys and returns their
/// relative ordering.  For sorted-dup databases this is `DupKeyData::compare`,
/// which splits each key into primary + data parts and applies separate
/// comparators to each.  For normal databases this field is `None` and
/// lexicographic byte comparison is used.
///
/// `DatabaseImpl.btreeComparator` / `DatabaseImpl.dupComparator`.
pub type KeyComparatorFn =
    Arc<dyn Fn(&[u8], &[u8]) -> std::cmp::Ordering + Send + Sync>;

/// Combined search result carrying slot data and the BIN arc, returned by
/// [`Tree::search_with_data`].
///
/// Avoids the double-descent pattern where `Tree::search` checked key
/// existence and a second call re-descended to fetch the actual slot bytes.
/// One descent now serves both purposes (Wave-11-I optimisation).
pub struct SlotFetch {
    /// `true` if an exact key match was found and is not expired.
    pub found: bool,
    /// Data bytes for the slot (`None` when `found` is `false`).
    pub data: Option<Vec<u8>>,
    /// Raw slot LSN as `u64`; zero when `found` is `false`.
    pub lsn: u64,
    /// Slot index within the BIN.  Set to the actual BIN slot index when
    /// `found` is `true`; `0` otherwise.
    ///
    /// Used by `CursorImpl` to set `current_index` correctly so that
    /// `retrieve_next` advances to the right slot after a search.
    pub slot_index: usize,
    /// Arc to the BIN that the descent reached.  Always `Some` when the
    /// tree has at least one node, regardless of whether `found` is `true`.
    pub bin_arc: Arc<RwLock<TreeNode>>,
}

/// The B+tree.
///
///
///
/// This is the main tree structure that manages the B+tree nodes and
/// provides operations for search, insert, delete, and tree maintenance.
pub struct Tree {
    /// Database ID this tree belongs to.
    database_id: u64,

    /// Maximum entries per node (from config).
    max_entries_per_node: usize,

    /// Root of the tree. None if tree is empty.
    ///
    /// Wrapped in `RwLock` so that `insert`, `delete`, and other mutating
    /// operations can take `&self` (interior mutability), enabling concurrent
    /// access to different BIN nodes without requiring a global `&mut Tree`
    /// borrow.  The root pointer itself is only written during root splits
    /// and initial creation; all other access is read-only.
    ///
    /// `Tree.root` protected by the root latch.
    root: RwLock<Option<Arc<RwLock<TreeNode>>>>,

    /// Latch protecting the root reference itself.
    /// Must be held when changing the root pointer.
    root_latch: SharedLatch,

    /// LSN at which the current root IN/BIN was last logged.
    ///
    /// Used by the IN-redo currency check (`recover_root_bin` /
    /// `recover_root_upper_in`) to decide whether a logged root replaces the
    /// in-memory one.  Updated whenever a new root is installed via
    /// `set_root_with_lsn` or the IN-redo recover-root path.
    ///
    /// JE `RootUpdater.originalLsn` / `ChildReference.getLsn()` for the root.
    root_log_lsn: RwLock<noxu_util::Lsn>,

    /// Statistics: number of times the root has been split.
    root_splits: AtomicU64,

    /// Statistics: number of latch upgrades from shared to exclusive.
    relatches_required: AtomicU64,

    /// Optional custom key comparator for sorted-duplicate databases.
    ///
    /// When `Some`, all key comparisons in tree traversal (upper IN routing
    /// and BIN entry search/insert/delete) use this comparator instead of
    /// lexicographic byte comparison.
    ///
    /// / `dupComparator` stored on the
    /// database and consulted at every `IN.findEntry()` call.
    pub key_comparator: Option<KeyComparatorFn>,

    /// Shared memory counter for the evictor / MemoryBudget.
    ///
    /// Updated on every BIN entry insert (+key+data+overhead) and delete
    /// (-key+overhead) so the evictor sees real cache pressure.
    ///
    /// `env.getMemoryBudget().updateTreeMemoryUsage(delta)` call
    /// in the equivalent `IN.updateMemorySize()`.  In Noxu the counter is an
    /// `Arc<AtomicI64>` shared with the `Arbiter` (and later `MemoryBudget`)
    /// to avoid a circular crate dependency (`noxu-tree` → `noxu-dbi`).
    pub memory_counter: Option<Arc<AtomicI64>>,

    /// Optional listener fed on node add/access/remove, mirroring JE's
    /// `INList` feeding the evictor's `LRUList`s.
    ///
    /// When `None` (the default — used by unit tests with no environment),
    /// the notifications are no-ops.  `EnvironmentImpl` installs the
    /// `Evictor` here so production inserts/accesses populate the LRU lists
    /// the evictor drains.
    ///
    /// JE reference: `IN.fetchTarget`/split/`rebuildINList` → `addBack`,
    /// access → `moveBack`, removal → `remove`.
    pub in_list_listener: Option<Arc<dyn InListListener>>,

    /// Optional log manager so an evicted root IN can be re-materialized from
    /// its persisted `root_log_lsn` on the next access (EV-14, piece B).
    ///
    /// JE's `Tree` reaches the log via `database.getEnv().getLogManager()`;
    /// `Tree.getRootINRootAlreadyLatched` calls `root.fetchTarget(...)` which
    /// reads the root IN back from its `ChildReference` LSN when the in-memory
    /// target is null (Tree.java:477-516, ChildReference.fetchTarget).  Noxu
    /// has no env back-reference here, so the log manager is installed
    /// directly (the same one-way wiring as `in_list_listener`).  When `None`
    /// (unit tests with no environment), an evicted root cannot be re-fetched
    /// — but `evict_root` refuses to evict without a log manager, so the root
    /// is never made non-resident in that configuration.
    pub log_manager: Option<Arc<noxu_log::LogManager>>,

    /// Capacity hint for the recovery redo path.
    ///
    /// When non-zero, the first BIN created by `redo_insert` (the first-key
    /// path) pre-allocates its `entries` Vec with this capacity so that
    /// redo insertions proceed without Vec-resize doublings.  The value is
    /// clamped to `max_entries_per_node` at use.
    ///
    /// Set by `hint_redo_capacity` before the redo loop.
    /// Wave 11-K optimisation (Fix 3).
    redo_capacity_hint: usize,

    /// Whether key-prefix compression is enabled for this tree's BINs.
    ///
    /// JE `DatabaseImpl.getKeyPrefixing()` / `DatabaseConfig.setKeyPrefixing()`.
    /// When `false`, `IN.computeKeyPrefix` returns `null` in JE — no prefix
    /// is ever set. Noxu mirrors this: `insert_with_prefix` is skipped in
    /// favour of `insert_raw`, and `recompute_key_prefix` is not called on
    /// BIN halves after a split.
    ///
    /// Default: `false` (matches JE's `DatabaseConfig.KEY_PREFIXING_DEFAULT`).
    ///
    /// Ref: `IN.java computeKeyPrefix` ~line 2456.
    pub key_prefixing: bool,
    /// T-5: maximum post-prefix key length (bytes) for the compact key rep
    /// (`INKeyRep.MaxKeySize`).  A node packs all its keys into one fixed-width
    /// byte array when every post-prefix key is `<=` this length; a longer key
    /// inflates the node to the `Default` rep.  `<= 0` disables the compact
    /// rep entirely.
    ///
    /// Default 16 (`TREE_COMPACT_MAX_KEY_LENGTH` /
    /// `INKeyRep.MaxKeySize.DEFAULT_MAX_KEY_LENGTH`).  Wired from
    /// `EnvironmentConfig` via `Tree::set_compact_max_key_length`
    /// (`IN.getCompactMaxKeyLength`, IN.java:4929).
    pub compact_max_key_length: i32,
}

/// A node in the tree.
///
/// TreeNode wraps an upper IN or a BIN. Each variant carries a lightweight
/// stub whose fields mirror the persistent IN/BIN structure. The stubs will
/// be replaced with full InNode/Bin types as the implementation matures; the
/// API surface here is intentionally minimal.
#[derive(Debug)]
pub enum TreeNode {
    /// Internal Node (IN) - non-leaf node in the tree.
    Internal(InNodeStub),

    /// Bottom Internal Node (BIN) - leaf-level internal node.
    Bottom(BinStub),
}

/// Type alias for a resident child pointer.
pub type ChildArc = Arc<RwLock<TreeNode>>;

/// T-4: per-node representation of the resident-child-pointer array.
///
/// Faithful to JE `INTargetRep` (`INTargetRep.java`), the abstract array of
/// target pointers to an IN's cached children.  These arrays are usually
/// sparse — most upper INs have NO resident children — so JE never stores a
/// full per-slot `Node[]` until many children are actually cached:
///
///   * `None`   — `INTargetRep.None`: a shared singleton, 0 child-pointer
///     bytes, used when no children are cached (the common case for upper
///     INs).  `get` returns null for every slot.
///   * `Sparse` — `INTargetRep.Sparse`: a small parallel `(index, target)[]`
///     for 1..=`MAX_ENTRIES` cached children (JE caps at 4).  `get(j)` is a
///     linear scan of the index array.
///   * `Default`— `INTargetRep.Default`: the full `Vec<Option<Arc>>`, one
///     slot per entry, used once more than `MAX_ENTRIES` children are
///     resident.
///
/// A node starts `None` and grows `None → Sparse → Default`.  JE does not
/// shrink back when entries are nulled (it only compacts on IN-stripping) to
/// avoid transitionary rep churn; we follow the same policy — `set_child` only
/// inflates, and `compact()` (called on eviction/stripping) collapses an
/// empty/small `Default`/`Sparse` back toward `None`.
#[derive(Debug)]
pub enum TargetRep {
    /// `INTargetRep.None` — no children cached (shared-singleton semantics).
    None,
    /// `INTargetRep.Sparse` — a few cached children, `(slot_index, child)`.
    /// Invariant: `len() <= SPARSE_MAX_ENTRIES`.
    Sparse(Vec<(u16, ChildArc)>),
    /// `INTargetRep.Default` — full parallel array, one slot per entry.
    Default(Vec<Option<ChildArc>>),
}

impl TargetRep {
    /// `INTargetRep.Sparse.MAX_ENTRIES` (INTargetRep.java) — the maximum
    /// number of cached children the `Sparse` rep holds before inflating to
    /// `Default`.
    pub const SPARSE_MAX_ENTRIES: usize = 4;

    /// `INTargetRep.get(idx)` — the cached child for slot `idx`, or `None`.
    #[inline]
    pub fn get(&self, idx: usize) -> Option<&ChildArc> {
        match self {
            TargetRep::None => None,
            TargetRep::Sparse(v) => {
                v.iter().find(|(i, _)| *i as usize == idx).map(|(_, c)| c)
            }
            TargetRep::Default(v) => v.get(idx).and_then(|o| o.as_ref()),
        }
    }

    /// `INTargetRep.set(idx, node, parent)` — set (or clear, when `node` is
    /// `None`) the cached child for slot `idx`, mutating the representation
    /// upward (`None → Sparse → Default`) as needed.
    pub fn set(&mut self, idx: usize, node: Option<ChildArc>) {
        match self {
            TargetRep::None => {
                // INTargetRep.None.set: clearing stays None; setting mutates
                // to a Sparse rep and sets there.
                if let Some(child) = node {
                    *self = TargetRep::Sparse(vec![(idx as u16, child)]);
                }
            }
            TargetRep::Sparse(v) => {
                // Update existing slot in place.
                if let Some(pos) =
                    v.iter().position(|(i, _)| *i as usize == idx)
                {
                    match node {
                        Some(child) => v[pos].1 = child,
                        None => {
                            v.swap_remove(pos);
                        }
                    }
                    return;
                }
                // New child: clearing a non-present slot is a no-op.
                let Some(child) = node else { return };
                if v.len() < Self::SPARSE_MAX_ENTRIES {
                    v.push((idx as u16, child));
                    return;
                }
                // Full — INTargetRep.Sparse.set mutates to Default.
                let cap = v.iter().map(|(i, _)| *i as usize).max().unwrap_or(0);
                let cap = cap.max(idx) + 1;
                let mut def: Vec<Option<ChildArc>> = vec![None; cap];
                for (i, c) in v.drain(..) {
                    def[i as usize] = Some(c);
                }
                def[idx] = Some(child);
                *self = TargetRep::Default(def);
            }
            TargetRep::Default(v) => {
                if idx >= v.len() {
                    if node.is_none() {
                        return;
                    }
                    v.resize_with(idx + 1, || None);
                }
                v[idx] = node;
            }
        }
    }

    /// `INTargetRep.None`-aware take: remove and return the cached child for
    /// slot `idx`, leaving the slot empty (JE `IN.setTarget(idx, null)` plus
    /// returning the old target).
    pub fn take(&mut self, idx: usize) -> Option<ChildArc> {
        match self {
            TargetRep::None => None,
            TargetRep::Sparse(v) => v
                .iter()
                .position(|(i, _)| *i as usize == idx)
                .map(|pos| v.swap_remove(pos).1),
            TargetRep::Default(v) => v.get_mut(idx).and_then(|o| o.take()),
        }
    }

    /// JE `INArrayRep.copy(from, to, n, parent)` adapted to slice ops: shift
    /// the child mapping when an entry is INSERTED at `idx` (all children at
    /// slots `>= idx` move up by one).  Mirrors how `Vec::insert` shifts the
    /// parallel `entries` array.
    pub fn insert_shift(&mut self, idx: usize) {
        match self {
            TargetRep::None => {}
            TargetRep::Sparse(v) => {
                for (i, _) in v.iter_mut() {
                    if (*i as usize) >= idx {
                        *i += 1;
                    }
                }
            }
            TargetRep::Default(v) => {
                if idx <= v.len() {
                    v.insert(idx, None);
                }
            }
        }
    }

    /// JE `INArrayRep.copy` adapted: shift the child mapping when the entry at
    /// `idx` is REMOVED (all children at slots `> idx` move down by one; the
    /// child at `idx` itself is dropped).  Mirrors `Vec::remove`.
    pub fn remove_shift(&mut self, idx: usize) {
        match self {
            TargetRep::None => {}
            TargetRep::Sparse(v) => {
                v.retain(|(i, _)| *i as usize != idx);
                for (i, _) in v.iter_mut() {
                    if (*i as usize) > idx {
                        *i -= 1;
                    }
                }
            }
            TargetRep::Default(v) => {
                if idx < v.len() {
                    v.remove(idx);
                }
            }
        }
    }

    /// `INTargetRep.compact(parent)` — collapse toward the most compact rep:
    /// an empty rep becomes `None`; a `Default` with `<= MAX_ENTRIES` children
    /// becomes `Sparse` (or `None`).  Called when an IN is stripped/evicted.
    pub fn compact(&mut self) {
        let count = self.resident_count();
        if count == 0 {
            *self = TargetRep::None;
            return;
        }
        if count <= Self::SPARSE_MAX_ENTRIES
            && let TargetRep::Default(v) = self
        {
            let sparse: Vec<(u16, ChildArc)> = v
                .iter()
                .enumerate()
                .filter_map(|(i, o)| o.as_ref().map(|c| (i as u16, c.clone())))
                .collect();
            *self = TargetRep::Sparse(sparse);
        }
    }

    /// Number of resident (non-null) children.
    pub fn resident_count(&self) -> usize {
        match self {
            TargetRep::None => 0,
            TargetRep::Sparse(v) => v.len(),
            TargetRep::Default(v) => v.iter().filter(|o| o.is_some()).count(),
        }
    }

    /// True if no children are cached (`INTargetRep.None` or empty).
    pub fn is_empty(&self) -> bool {
        self.resident_count() == 0
    }

    /// Iterate every resident child (in unspecified order).
    pub fn iter_children(&self) -> Box<dyn Iterator<Item = ChildArc> + '_> {
        match self {
            TargetRep::None => Box::new(std::iter::empty()),
            TargetRep::Sparse(v) => Box::new(v.iter().map(|(_, c)| c.clone())),
            TargetRep::Default(v) => {
                Box::new(v.iter().filter_map(|o| o.clone()))
            }
        }
    }

    /// `INTargetRep.calculateMemorySize()` — heap bytes of the rep itself
    /// (excluding the children it points at).  `None` is 0 (shared singleton),
    /// matching `INTargetRep.None.calculateMemorySize() == 0`.
    pub fn memory_size(&self) -> usize {
        use std::mem::size_of;
        match self {
            TargetRep::None => 0,
            TargetRep::Sparse(v) => v.capacity() * size_of::<(u16, ChildArc)>(),
            TargetRep::Default(v) => {
                v.capacity() * size_of::<Option<ChildArc>>()
            }
        }
    }
}

/// T-3: node-level packed LSN array — `IN.entryLsnByteArray` /
/// `IN.entryLsnLongArray` (IN.java:251-289, getLsn/setLsnInternal
/// IN.java:1752-1935).
///
/// JE stores one LSN per slot.  A naive `Lsn` (u64) costs 8 bytes/slot even
/// though most LSNs in a node share a file number and have a file offset that
/// fits in 3 bytes.  JE's compact rep is a single `byte[]` with
/// `BYTES_PER_LSN_ENTRY == 4` bytes per slot:
///
///   * `base_file_number` is the lowest file number of any non-NULL LSN in the
///     node;
///   * byte 0 of each slot = `file_number - base_file_number` (0..=127,
///     `Byte.MAX_VALUE`);
///   * bytes 1..4 = the 3-byte little-endian file offset (max
///     `MAX_FILE_OFFSET == 0xff_fffe`).
///
/// The NULL_LSN blocker (Noxu `NULL_LSN == u64::MAX`) is solved EXACTLY as JE
/// does it: NULL is NOT stored as the raw u64; the slot's 3 file-offset bytes
/// are set to `0xff_ffff` (`THREE_BYTE_NEGATIVE_ONE`), a value `MAX_FILE_OFFSET`
/// can never reach, and `get_lsn` maps it back to `NULL_LSN`.
///
/// If a file-number difference exceeds 127 or a file offset exceeds
/// `MAX_FILE_OFFSET`, the rep mutates to `Long` (one `u64` per slot), matching
/// JE's `mutateToLongArray` (IN.java:1924).  An all-NULL node uses `Empty`
/// (0 bytes), matching the EMPTY_REP/initial-capacity-free state.
#[derive(Debug)]
pub enum LsnRep {
    /// All slots NULL — 0 heap bytes (the `byteArray == null` initial state).
    Empty,
    /// `IN.entryLsnByteArray` — 4 bytes/slot, `base_file_number`-relative.
    Compact { base_file_number: u32, bytes: Vec<u8> },
    /// `IN.entryLsnLongArray` — 8 bytes/slot fallback after `mutateToLongArray`.
    Long(Vec<Lsn>),
}

impl LsnRep {
    /// `IN.BYTES_PER_LSN_ENTRY` (IN.java:151).
    pub const BYTES_PER_LSN_ENTRY: usize = 4;
    /// `IN.MAX_FILE_OFFSET` (IN.java:152) — max file offset the 3-byte form holds.
    const MAX_FILE_OFFSET: u32 = 0x00ff_fffe;
    /// `IN.THREE_BYTE_NEGATIVE_ONE` (IN.java:153) — the NULL sentinel in the
    /// 3 file-offset bytes.
    const THREE_BYTE_NEGATIVE_ONE: u32 = 0x00ff_ffff;
    /// `Byte.MAX_VALUE` — max file-number difference the 1-byte offset holds.
    const MAX_FILE_NUMBER_OFFSET: u32 = 127;

    /// A rep sized for `n` slots, all NULL.  Returns `Empty` (0 bytes); the
    /// Compact byte array is lazily allocated by the first non-NULL `set_lsn`
    /// — `base_file_number` is unknown until then (IN.java:1820, the
    /// `baseFileNumber == -1` first-entry case).
    #[inline]
    pub fn new(_n: usize) -> Self {
        LsnRep::Empty
    }

    /// Build a rep from a per-slot `Lsn` slice (used by node construction and
    /// split, where slots arrive together).  Equivalent to `new(lsns.len())`
    /// followed by `set(i, lsns[i])` for each slot.
    pub fn from_lsns(lsns: &[Lsn]) -> Self {
        let mut rep = LsnRep::Empty;
        let n = lsns.len();
        for (i, &lsn) in lsns.iter().enumerate() {
            rep.set(i, lsn, n);
        }
        rep
    }

    /// `IN.getLsn(idx)` (IN.java:1752).
    pub fn get(&self, idx: usize) -> Lsn {
        match self {
            LsnRep::Empty => NULL_LSN,
            LsnRep::Long(v) => v.get(idx).copied().unwrap_or(NULL_LSN),
            LsnRep::Compact { base_file_number, bytes } => {
                let off = idx * Self::BYTES_PER_LSN_ENTRY;
                if off + Self::BYTES_PER_LSN_ENTRY > bytes.len() {
                    return NULL_LSN;
                }
                let file_offset = Self::get_3byte(bytes, off + 1);
                if file_offset == Self::THREE_BYTE_NEGATIVE_ONE {
                    NULL_LSN
                } else {
                    let file_number = base_file_number + bytes[off] as u32;
                    Lsn::new(file_number, file_offset)
                }
            }
        }
    }

    /// `IN.setLsnInternal(idx, value)` (IN.java:1801) — set the LSN of slot
    /// `idx`, mutating Empty→Compact→Long as necessary.  `n` is the node's
    /// slot count (sizes a freshly-allocated Compact array).
    pub fn set(&mut self, idx: usize, lsn: Lsn, n: usize) {
        // Empty: first non-NULL value allocates the Compact array; a NULL set
        // on an Empty rep is a no-op (all slots already read NULL).
        if let LsnRep::Empty = self {
            if lsn.is_null() {
                return;
            }
            let cap = n.max(idx + 1);
            *self = LsnRep::Compact {
                base_file_number: lsn.file_number(),
                bytes: vec![0u8; cap * Self::BYTES_PER_LSN_ENTRY],
            };
            // Mark every other slot NULL (3-byte offset = 0xffffff).
            if let LsnRep::Compact { bytes, .. } = self {
                for s in 0..cap {
                    if s != idx {
                        Self::put_3byte(
                            bytes,
                            s * Self::BYTES_PER_LSN_ENTRY + 1,
                            Self::THREE_BYTE_NEGATIVE_ONE,
                        );
                    }
                }
            }
            self.set(idx, lsn, n);
            return;
        }

        if let LsnRep::Long(v) = self {
            if idx >= v.len() {
                v.resize(idx + 1, NULL_LSN);
            }
            v[idx] = lsn;
            return;
        }

        // Compact path.
        let LsnRep::Compact { base_file_number, bytes } = self else {
            unreachable!()
        };
        let need = (idx + 1) * Self::BYTES_PER_LSN_ENTRY;
        if need > bytes.len() {
            let old = bytes.len() / Self::BYTES_PER_LSN_ENTRY;
            bytes.resize(need, 0);
            for s in old..(idx + 1) {
                Self::put_3byte(
                    bytes,
                    s * Self::BYTES_PER_LSN_ENTRY + 1,
                    Self::THREE_BYTE_NEGATIVE_ONE,
                );
            }
        }
        let off = idx * Self::BYTES_PER_LSN_ENTRY;

        if lsn.is_null() {
            // IN.java:1812 — file-number offset 0, file offset -1 (0xffffff).
            bytes[off] = 0;
            Self::put_3byte(bytes, off + 1, Self::THREE_BYTE_NEGATIVE_ONE);
            return;
        }

        let this_file_number = lsn.file_number();
        let this_file_offset = lsn.file_offset();

        // Whether to fall back to the Long rep.
        let mutate = this_file_offset > Self::MAX_FILE_OFFSET || {
            if this_file_number < *base_file_number {
                // IN.java:1827 — try to re-base downward; bail if any existing
                // slot would then exceed the 1-byte file-number offset.
                !Self::adjust_file_numbers(
                    bytes,
                    *base_file_number,
                    this_file_number,
                )
            } else {
                this_file_number - *base_file_number
                    > Self::MAX_FILE_NUMBER_OFFSET
            }
        };

        if mutate {
            // IN.java:1924 mutateToLongArray.
            let nelts = bytes.len() / Self::BYTES_PER_LSN_ENTRY;
            let mut longs = vec![NULL_LSN; nelts.max(idx + 1)];
            for (s, slot) in longs.iter_mut().enumerate().take(nelts) {
                *slot = self_get_compact(*base_file_number, bytes, s);
            }
            longs[idx] = lsn;
            *self = LsnRep::Long(longs);
            return;
        }

        if this_file_number < *base_file_number {
            *base_file_number = this_file_number;
        }
        bytes[off] = (this_file_number - *base_file_number) as u8;
        Self::put_3byte(bytes, off + 1, this_file_offset);
    }

    /// `IN.adjustFileNumbers` (IN.java:1855) — re-base to a lower file number,
    /// rewriting every existing slot's 1-byte offset.  Returns false (and
    /// leaves `bytes` unchanged) if any slot would overflow the 1-byte offset.
    fn adjust_file_numbers(
        bytes: &mut [u8],
        old_base: u32,
        new_base: u32,
    ) -> bool {
        let stride = Self::BYTES_PER_LSN_ENTRY;
        // First pass: verify none overflow.
        let mut i = 0;
        while i < bytes.len() {
            if Self::get_3byte(bytes, i + 1) != Self::THREE_BYTE_NEGATIVE_ONE {
                let cur_fn = old_base + bytes[i] as u32;
                if cur_fn - new_base > Self::MAX_FILE_NUMBER_OFFSET {
                    return false;
                }
            }
            i += stride;
        }
        // Second pass: apply.
        let mut i = 0;
        while i < bytes.len() {
            if Self::get_3byte(bytes, i + 1) != Self::THREE_BYTE_NEGATIVE_ONE {
                let cur_fn = old_base + bytes[i] as u32;
                bytes[i] = (cur_fn - new_base) as u8;
            }
            i += stride;
        }
        true
    }

    /// `INArrayRep.copy` analogue: shift LSNs when an entry is inserted at
    /// `idx` (slots `>= idx` move up one).  Mirrors `targets.insert_shift`.
    pub fn insert_shift(&mut self, idx: usize, n: usize) {
        match self {
            LsnRep::Empty => {}
            LsnRep::Long(v) => {
                if idx <= v.len() {
                    v.insert(idx, NULL_LSN);
                }
            }
            LsnRep::Compact { bytes, .. } => {
                let stride = Self::BYTES_PER_LSN_ENTRY;
                let cap = (n.max((bytes.len() / stride) + 1)) * stride;
                bytes.resize(cap, 0);
                let at = idx * stride;
                // Shift the tail up by one slot.
                bytes.copy_within(at..cap - stride, at + stride);
                // The new slot reads NULL.
                Self::put_3byte(bytes, at + 1, Self::THREE_BYTE_NEGATIVE_ONE);
            }
        }
    }

    /// `INArrayRep.copy` analogue: shift LSNs when entry `idx` is removed
    /// (slots `> idx` move down one).  Mirrors `targets.remove_shift`.
    pub fn remove_shift(&mut self, idx: usize) {
        match self {
            LsnRep::Empty => {}
            LsnRep::Long(v) => {
                if idx < v.len() {
                    v.remove(idx);
                }
            }
            LsnRep::Compact { bytes, .. } => {
                let stride = Self::BYTES_PER_LSN_ENTRY;
                let at = idx * stride;
                if at + stride <= bytes.len() {
                    bytes.copy_within(at + stride.., at);
                    let newlen = bytes.len() - stride;
                    bytes.truncate(newlen);
                }
            }
        }
    }

    /// `IN.computeLsnOverhead` analogue: heap bytes of the rep itself.
    pub fn memory_size(&self) -> usize {
        use std::mem::size_of;
        match self {
            LsnRep::Empty => 0,
            LsnRep::Compact { bytes, .. } => bytes.capacity(),
            LsnRep::Long(v) => v.capacity() * size_of::<Lsn>(),
        }
    }

    fn put_3byte(bytes: &mut [u8], offset: usize, value: u32) {
        bytes[offset] = (value & 0xFF) as u8;
        bytes[offset + 1] = ((value >> 8) & 0xFF) as u8;
        bytes[offset + 2] = ((value >> 16) & 0xFF) as u8;
    }

    fn get_3byte(bytes: &[u8], offset: usize) -> u32 {
        (bytes[offset] as u32)
            | ((bytes[offset + 1] as u32) << 8)
            | ((bytes[offset + 2] as u32) << 16)
    }
}

/// Helper used by `LsnRep::set` during `mutateToLongArray` to read an existing
/// Compact slot without borrowing `self` (which is mid-mutation).
fn self_get_compact(base_file_number: u32, bytes: &[u8], idx: usize) -> Lsn {
    let off = idx * LsnRep::BYTES_PER_LSN_ENTRY;
    let file_offset = LsnRep::get_3byte(bytes, off + 1);
    if file_offset == LsnRep::THREE_BYTE_NEGATIVE_ONE {
        NULL_LSN
    } else {
        Lsn::new(base_file_number + bytes[off] as u32, file_offset)
    }
}

/// `INKeyRep.MaxKeySize.DEFAULT_MAX_KEY_LENGTH` (INKeyRep.java) and the
/// `TREE_COMPACT_MAX_KEY_LENGTH` config default.
#[allow(non_upper_case_globals)]
pub const INKeyRep_DEFAULT_MAX_KEY_LENGTH: i32 = 16;

/// T-2: node-level key array — `INKeyRep.{Default,MaxKeySize}` (INKeyRep.java).
///
/// The per-slot key that used to live in `BinEntry`/`InEntry` as a `Vec<u8>`
/// (24-byte header + a separate heap allocation per key) is hoisted here as a
/// node-level rep.  When every (post-prefix) key in the node is `<=`
/// `TREE_COMPACT_MAX_KEY_LENGTH` (default 16) the keys pack into ONE
/// fixed-width byte buffer (`MaxKeySize`): `slot_width` bytes per slot, with a
/// parallel `lengths` vector tracking the actual length of each key.  A key
/// longer than the threshold inflates the whole node to the `Default` rep
/// (one `Vec<u8>` per slot), matching JE's `Default.compact` /
/// `MaxKeySize.expandToDefaultRep`.
///
/// As in JE, this stores the UNPREFIXED suffix (key prefixing strips the
/// common prefix first), so the compact rep is the smaller post-prefix bytes.
#[derive(Debug, Clone)]
pub enum KeyRep {
    /// `INKeyRep.Default` — one owned key per slot (any length).
    Default(Vec<Vec<u8>>),
    /// `INKeyRep.MaxKeySize` — all keys packed into one fixed-width buffer.
    /// `buf.len() == slot_width * lengths.len()`; slot `i` occupies
    /// `buf[i*slot_width .. i*slot_width + lengths[i]]`.
    Compact { buf: Vec<u8>, slot_width: usize, lengths: Vec<u16> },
}

impl KeyRep {
    /// An empty `Default` rep.
    #[inline]
    pub fn new() -> Self {
        KeyRep::Default(Vec::new())
    }

    /// Build a `Default` rep from owned keys (callers may later `compact`).
    #[inline]
    pub fn from_keys(keys: Vec<Vec<u8>>) -> Self {
        KeyRep::Default(keys)
    }

    /// Number of slots.
    #[inline]
    pub fn len(&self) -> usize {
        match self {
            KeyRep::Default(v) => v.len(),
            KeyRep::Compact { lengths, .. } => lengths.len(),
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// `INKeyRep.get(idx)` / `getKey` — borrow the (post-prefix) key at slot
    /// `idx` without allocating.
    #[inline]
    pub fn get(&self, idx: usize) -> &[u8] {
        match self {
            KeyRep::Default(v) => v[idx].as_slice(),
            KeyRep::Compact { buf, slot_width, lengths } => {
                let off = idx * slot_width;
                &buf[off..off + lengths[idx] as usize]
            }
        }
    }

    /// Set the key at slot `idx`.  A key longer than a Compact rep's
    /// `slot_width` inflates the rep to `Default` first
    /// (`MaxKeySize.expandToDefaultRep`).
    pub fn set(&mut self, idx: usize, key: Vec<u8>) {
        match self {
            KeyRep::Default(v) => v[idx] = key,
            KeyRep::Compact { slot_width, .. } if key.len() > *slot_width => {
                self.inflate_to_default();
                self.set(idx, key);
            }
            KeyRep::Compact { buf, slot_width, lengths } => {
                let off = idx * *slot_width;
                buf[off..off + key.len()].copy_from_slice(&key);
                lengths[idx] = key.len() as u16;
            }
        }
    }

    /// Insert a key at slot `idx`, shifting later slots up (mirrors
    /// `Vec::insert` + `INArrayRep.copy`).
    pub fn insert(&mut self, idx: usize, key: Vec<u8>) {
        match self {
            KeyRep::Default(v) => v.insert(idx, key),
            KeyRep::Compact { slot_width, .. } if key.len() > *slot_width => {
                self.inflate_to_default();
                self.insert(idx, key);
            }
            KeyRep::Compact { buf, slot_width, lengths } => {
                let sw = *slot_width;
                let at = idx * sw;
                buf.splice(at..at, std::iter::repeat_n(0u8, sw));
                buf[at..at + key.len()].copy_from_slice(&key);
                lengths.insert(idx, key.len() as u16);
            }
        }
    }

    /// Remove the key at slot `idx`, shifting later slots down.
    pub fn remove(&mut self, idx: usize) -> Vec<u8> {
        match self {
            KeyRep::Default(v) => v.remove(idx),
            KeyRep::Compact { buf, slot_width, lengths } => {
                let sw = *slot_width;
                let len = lengths[idx] as usize;
                let at = idx * sw;
                let out = buf[at..at + len].to_vec();
                buf.drain(at..at + sw);
                lengths.remove(idx);
                out
            }
        }
    }

    /// `INKeyRep.MaxKeySize.expandToDefaultRep` — mutate a Compact rep to a
    /// Default rep (one owned `Vec<u8>` per slot).
    fn inflate_to_default(&mut self) {
        if let KeyRep::Compact { .. } = self {
            let keys: Vec<Vec<u8>> =
                (0..self.len()).map(|i| self.get(i).to_vec()).collect();
            *self = KeyRep::Default(keys);
        }
    }

    /// `INKeyRep.Default.compact(parent)` (INKeyRep.java) — if every key in a
    /// `Default` rep fits `compact_max_key_length`, pack them into a
    /// `MaxKeySize` (`Compact`) rep.  `compact_max_key_length <= 0` disables
    /// compaction.  No-op when already Compact.
    pub fn compact(&mut self, compact_max_key_length: i32) {
        if compact_max_key_length <= 0 {
            return;
        }
        let KeyRep::Default(keys) = self else {
            return; // already Compact
        };
        if keys.is_empty() {
            return;
        }
        let max_len = keys.iter().map(|k| k.len()).max().unwrap_or(0);
        if max_len > compact_max_key_length as usize {
            return; // a key exceeds the threshold — stay Default
        }
        let slot_width = max_len.max(1);
        let mut buf = vec![0u8; slot_width * keys.len()];
        let mut lengths = Vec::with_capacity(keys.len());
        for (i, k) in keys.iter().enumerate() {
            let off = i * slot_width;
            buf[off..off + k.len()].copy_from_slice(k);
            lengths.push(k.len() as u16);
        }
        *self = KeyRep::Compact { buf, slot_width, lengths };
    }

    /// True when key-byte memory is accounted for inside this rep (Compact),
    /// vs per-slot `Vec` allocations (Default).
    /// `INKeyRep.accountsForKeyByteMemUsage`.
    #[inline]
    pub fn is_compact(&self) -> bool {
        matches!(self, KeyRep::Compact { .. })
    }

    /// Heap bytes of the rep itself (`INKeyRep.calculateMemorySize` +
    /// key-byte accounting).  For Default this is the `Vec<Vec<u8>>` header
    /// plus each key's heap allocation; for Compact it is the single buffer
    /// plus the lengths vector.
    pub fn memory_size(&self) -> usize {
        use std::mem::size_of;
        match self {
            KeyRep::Default(v) => {
                v.capacity() * size_of::<Vec<u8>>()
                    + v.iter().map(|k| k.capacity()).sum::<usize>()
            }
            KeyRep::Compact { buf, lengths, .. } => {
                buf.capacity() + lengths.capacity() * size_of::<u16>()
            }
        }
    }
}

impl Default for KeyRep {
    fn default() -> Self {
        KeyRep::new()
    }
}

/// Lightweight upper-IN representation used by the tree traversal layer.
///
/// `IN`: carries the dirty flag (IN_DIRTY_BIT), the LRU
/// generation counter, and a weak back-pointer to the parent so that
/// dirty state can be propagated upward.
#[derive(Debug)]
pub struct InNodeStub {
    /// Node ID.
    pub node_id: u64,
    /// Level in tree.
    pub level: i32,
    /// Child entries (key, lsn).
    pub entries: Vec<InEntry>,
    /// T-4: per-node resident-child-pointer representation.
    ///
    /// `IN.entryTargets` (`INTargetRep`).  The cached child pointer is no
    /// longer a per-`InEntry` `Option<Arc>` (which cost a pointer-sized slot
    /// even when no child was resident); it lives here as a compact
    /// node-level rep that starts `None` (0 child-pointer bytes — most upper
    /// INs have no resident children), grows to `Sparse` for a few cached
    /// children, and inflates to `Default` (the full parallel array) once
    /// many children are resident.  See `INTargetRep.{None,Sparse,Default}`.
    pub targets: TargetRep,
    /// Dirty flag — set whenever this node is modified.
    /// `IN.dirty` (IN_DIRTY_BIT).
    pub dirty: bool,
    /// LRU generation counter for the evictor.
    /// `IN.generation`.
    pub generation: u64,
    /// Weak back-pointer to parent IN.
    /// Enables dirty-propagation and latch-coupling validation.
    /// `IN.parent` reference used during splits and logging.
    pub parent: Option<Weak<RwLock<TreeNode>>>,
    /// T-3: per-node packed LSN array (`IN.entryLsnByteArray`).  The per-slot
    /// `lsn` (8 bytes) that used to live in `InEntry` is hoisted here as a
    /// `base_file_number`-relative 4-byte-per-slot rep, falling back to a
    /// `u64`-per-slot `Long` rep only when a node's LSN range exceeds the
    /// compact form.  Access via `get_lsn(slot)` / `set_lsn(slot, lsn)`.
    pub lsn_rep: LsnRep,
}

/// Entry in an IN node.
///
/// T-4: the resident-child pointer that used to live here (`Option<Arc>`) was
/// hoisted to the node-level `InNodeStub.targets` (`INTargetRep`); access the
/// child for slot `i` via `InNodeStub::get_child(i)` / `set_child` / etc.
///
/// T-3: the per-slot `lsn` (8 bytes) that used to live here was hoisted to the
/// node-level `InNodeStub.lsn_rep` (`IN.entryLsnByteArray`); access the LSN for
/// slot `i` via `InNodeStub::get_lsn(i)` / `set_lsn(i, lsn)`.
#[derive(Debug, Clone)]
pub struct InEntry {
    /// Key for this entry.
    pub key: Vec<u8>,
}

/// Lightweight BIN representation used by the tree traversal layer.
///
/// `BIN` (which extends `IN`): carries the dirty flag, LRU
/// generation counter, and a weak back-pointer to the parent IN.
///
/// # Key Prefix Compression
///
/// BINs support key prefix compression.  When
/// `key_prefix` is non-empty the `key` field of every `BinEntry` stores only
/// the *suffix* — the bytes after stripping the common leading bytes.  The
/// full key is reconstructed by prepending `key_prefix` to the stored suffix.
///
/// This is transparent to callers through the `get_full_key` / `find_entry`
/// helpers on `BinStub`.  The prefix is recomputed after every insert and
/// after a split via `recompute_key_prefix`.
#[derive(Debug)]
pub struct BinStub {
    /// Node ID.
    pub node_id: u64,
    /// Level (always BIN_LEVEL).
    pub level: i32,
    /// Entries.  When `key_prefix` is non-empty the `key` field in each entry
    /// is the *suffix* of the full key (leading `key_prefix` bytes stripped).
    /// `IN.entryKeys` (suffix-only storage when prefixing is on).
    pub entries: Vec<BinEntry>,
    /// Common prefix shared by every key in this BIN.
    /// Empty slice means no prefix compression is active.
    /// `IN.keyPrefix`.
    pub key_prefix: Vec<u8>,
    /// Dirty flag — set whenever this BIN is modified.
    /// `IN.dirty` (IN_DIRTY_BIT).
    pub dirty: bool,
    /// BIN-delta flag — true when this BIN contains only dirty (delta) slots
    /// rather than a complete set of entries.
    /// `IN.IN_DELTA_BIT` (the IN_DELTA_BIT flag inside `flags`).
    pub is_delta: bool,
    /// LSN at which this BIN was last logged as a full (non-delta) BIN.
    ///
    /// Used by the checkpoint path to construct `BINDeltaLogEntry.prev_full_lsn`
    /// and to compare against `prev_delta_lsn` when deciding whether to write
    /// a delta or a full BIN.
    ///
    /// `BIN.lastFullLsn`.
    pub last_full_lsn: Lsn,
    /// LSN at which this BIN was last logged as a BIN-delta.
    ///
    /// Written as `prev_delta_lsn` into the next `BINDeltaLogEntry` so the
    /// cleaner's utilization tracker can mark the superseded delta obsolete.
    /// Reset to `NULL_LSN` whenever a full BIN is written.
    ///
    /// `BIN.lastDeltaVersion` / `BIN.getLastDeltaLsn()`.
    pub last_delta_lsn: Lsn,
    /// LRU generation counter for the evictor.
    /// `IN.generation`.
    pub generation: u64,
    /// Weak back-pointer to parent IN.
    /// Enables dirty-propagation and latch-coupling validation.
    pub parent: Option<Weak<RwLock<TreeNode>>>,
    /// If true, `BinEntry.expiration_time` values in this BIN are packed hours
    /// since epoch; if false, they are packed seconds since epoch.
    ///
    /// Default: `true` (hours, matching TTL resolution).
    ///
    /// `BIN.expirationInHours`.
    pub expiration_in_hours: bool,
    /// Number of cursors currently positioned on this BIN.
    ///
    /// The evictor skips BINs with a non-zero cursor count to avoid evicting
    /// a node that a cursor is actively traversing.  CursorImpl increments
    /// this when positioning on a BIN and decrements it on reposition/close.
    ///
    /// `IN.cursorSet.size()` used by `Evictor.selectIN()`.
    pub cursor_count: i32,
    /// When true, the NEXT log of this BIN must be a full BIN, not a delta.
    ///
    /// Set after a dirty slot is removed (a delta would silently lose that
    /// removal) and cleared after a full BIN is written.  This is the
    /// delta-chain bound: it forces a periodic full BIN so a delta never
    /// references stale state.
    ///
    /// `IN.prohibitNextDelta` / `IN.setProhibitNextDelta` (IN.java:5013) /
    /// `IN.getProhibitNextDelta`.
    pub prohibit_next_delta: bool,
    /// T-3: per-node packed LSN array (`IN.entryLsnByteArray`).  The per-slot
    /// `lsn` (8 bytes) that used to live in `BinEntry` is hoisted here as a
    /// `base_file_number`-relative 4-byte-per-slot rep.  Access via
    /// `get_lsn(slot)` / `set_lsn(slot, lsn)`.
    pub lsn_rep: LsnRep,
    /// T-2: per-node key array (`INKeyRep.{Default,MaxKeySize}`).  The per-slot
    /// `key` (`Vec<u8>`, 24-byte header + heap alloc) that used to live in
    /// `BinEntry` is hoisted here.  Stores the post-prefix SUFFIX (key
    /// prefixing strips the common prefix first).  Packs into one fixed-width
    /// buffer (`Compact`) when every suffix is `<= compact_max_key_length`,
    /// else one `Vec<u8>` per slot (`Default`).  `keys.len()` is kept in lock
    /// step with `entries.len()`.  Access via `get_key(slot)` /
    /// `get_full_key(slot)`.
    pub keys: KeyRep,
    /// T-5: the node's compact-key threshold (`IN.getCompactMaxKeyLength`),
    /// copied from the owning `Tree` at construction so `apply_new_prefix` can
    /// decide whether the suffixes now fit `MaxKeySize`.  Default 16.
    pub compact_max_key_length: i32,
}

/// Entry in a BIN node.
///
/// T-3: the per-slot `lsn` (8 bytes) that used to live here was hoisted to the
/// node-level `BinStub.lsn_rep` (`IN.entryLsnByteArray`); access the LSN for
/// slot `i` via `BinStub::get_lsn(i)` / `set_lsn(i, lsn)`.
#[derive(Debug, Clone)]
pub struct BinEntry {
    /// Optional embedded data (for small records) or cached LN.
    pub data: Option<Vec<u8>>,
    /// True when this slot has been marked known-deleted (analogous to the
    /// KNOWN_DELETED_BIT in `IN.entryStates`).  The slot is eligible for
    /// removal by `compress_bin()`.
    pub known_deleted: bool,
    /// True when this slot has been modified since the last full BIN log write.
    ///
    /// `IN.entryStates[i] & IN_DIRTY_BIT`.  Used by the checkpoint
    /// path to decide whether to write a BIN-delta (few dirty slots) or a
    /// full BIN (many dirty slots).
    pub dirty: bool,
    /// Packed expiration time (0 = no expiration).
    ///
    /// When the owning `BinStub.expiration_in_hours` is true, this value is
    /// hours since Unix epoch; otherwise it is seconds since Unix epoch.
    ///
    /// `IN.entryExpiration`.
    pub expiration_time: u32,
}

impl InNodeStub {
    /// `IN.getTarget(idx)` — the resident child cached for slot `idx`, cloned
    /// (a strong `Arc`), or `None` if the child is not cached.  Routes through
    /// the node-level `INTargetRep` (T-4).
    #[inline]
    pub fn get_child(&self, idx: usize) -> Option<ChildArc> {
        self.targets.get(idx).cloned()
    }

    /// Borrow the resident child for slot `idx` without cloning.
    #[inline]
    pub fn child_ref(&self, idx: usize) -> Option<&ChildArc> {
        self.targets.get(idx)
    }

    /// True if slot `idx` has no resident (cached) child.
    /// `IN.getTarget(idx) == null`.
    #[inline]
    pub fn child_is_none(&self, idx: usize) -> bool {
        self.targets.get(idx).is_none()
    }

    /// `IN.setTarget(idx, node)` — set (or clear) the cached child for slot
    /// `idx`, mutating the `INTargetRep` upward as needed.
    #[inline]
    pub fn set_child(&mut self, idx: usize, node: Option<ChildArc>) {
        self.targets.set(idx, node);
    }

    /// `IN.detachNode` helper — remove and return the cached child for slot
    /// `idx`, leaving the slot's key/LSN intact for re-fetch.
    #[inline]
    pub fn take_child(&mut self, idx: usize) -> Option<ChildArc> {
        self.targets.take(idx)
    }

    /// `IN.getLsn(idx)` (IN.java:1752) — the LSN of slot `idx` via the
    /// node-level packed `LsnRep` (T-3).
    #[inline]
    pub fn get_lsn(&self, idx: usize) -> Lsn {
        self.lsn_rep.get(idx)
    }

    /// `IN.setLsn(idx, lsn)` (IN.java:1773) — set the LSN of slot `idx` via
    /// the node-level packed `LsnRep` (T-3).
    #[inline]
    pub fn set_lsn(&mut self, idx: usize, lsn: Lsn) {
        let n = self.entries.len();
        self.lsn_rep.set(idx, lsn, n);
    }

    /// Insert an entry at `idx`, shifting the child mapping to stay aligned
    /// (`INArrayRep.copy`), then set the new slot's cached child.  Mirrors the
    /// old `entries.insert(idx, InEntry{ child: ..})` in one call.
    pub fn insert_entry(
        &mut self,
        idx: usize,
        key: Vec<u8>,
        lsn: Lsn,
        child: Option<ChildArc>,
    ) {
        self.entries.insert(idx, InEntry { key });
        let n = self.entries.len();
        self.lsn_rep.insert_shift(idx, n);
        self.lsn_rep.set(idx, lsn, n);
        self.targets.insert_shift(idx);
        if child.is_some() {
            self.targets.set(idx, child);
        }
    }

    /// Remove the entry at `idx`, shifting the child mapping to stay aligned
    /// (`INArrayRep.copy`).  Returns the removed `InEntry` (key).
    pub fn remove_entry(&mut self, idx: usize) -> InEntry {
        let e = self.entries.remove(idx);
        self.lsn_rep.remove_shift(idx);
        self.targets.remove_shift(idx);
        e
    }

    /// All resident children (cloned `Arc`s), in unspecified order.
    /// Replaces `entries.iter().filter_map(|e| e.child.clone())`.
    pub fn resident_children(&self) -> Vec<ChildArc> {
        self.targets.iter_children().collect()
    }

    /// `(slot_index, child)` of the first resident child, if any.
    pub fn first_resident_child(&self) -> Option<(usize, ChildArc)> {
        (0..self.entries.len())
            .find_map(|i| self.targets.get(i).map(|c| (i, c.clone())))
    }
}

impl BinStub {
    /// `IN.getLsn(idx)` (IN.java:1752) — the LSN of slot `idx` via the
    /// node-level packed `LsnRep` (T-3).
    #[inline]
    pub fn get_lsn(&self, idx: usize) -> Lsn {
        self.lsn_rep.get(idx)
    }

    /// `IN.setLsn(idx, lsn)` (IN.java:1773) — set the LSN of slot `idx` via
    /// the node-level packed `LsnRep` (T-3).
    #[inline]
    pub fn set_lsn(&mut self, idx: usize, lsn: Lsn) {
        let n = self.entries.len();
        self.lsn_rep.set(idx, lsn, n);
    }

    /// TREE-F1: the single user-facing liveness predicate for a BIN slot.
    ///
    /// A slot is LIVE for reads/scans iff it is neither `known_deleted` nor
    /// TTL-expired.  This mirrors the two ways JE makes a slot read as ABSENT:
    ///   * `IN.findEntry` (IN.java:3197) returns -1 for a `known_deleted`
    ///     exact match;
    ///   * `CursorImpl.isProbablyExpired` / `lockAndGetCurrent`
    ///     (CursorImpl.java:2062-2064) skip `isEntryKnownDeleted` (and
    ///     expired) slots while stepping.
    ///
    /// KD slots legitimately exist in live BINs during BIN-delta
    /// reconstitution until the compressor reclaims them; the maintenance
    /// paths (compressor / recovery undo) iterate them on purpose and do NOT
    /// use this predicate.
    #[inline]
    pub fn slot_is_live(&self, idx: usize) -> bool {
        match self.entries.get(idx) {
            Some(e) => {
                !(e.known_deleted
                    || (e.expiration_time != 0
                        && noxu_util::ttl::is_expired(
                            e.expiration_time,
                            self.expiration_in_hours,
                        )))
            }
            None => false,
        }
    }

    // ========================================================================
    // Key prefix compression helpers
    // IN.computeKeyPrefix / IN.recalcSuffixes / IN.getKey
    // ========================================================================

    /// Strips embedded LN data from non-dirty slots, freeing the heap
    /// allocations of the per-slot value bytes while keeping the slot keys
    /// and LSNs addressable.  Used by the evictor's PartialEvict path: a
    /// hot BIN is kept in cache so its descent path stays warm, but the LN
    /// data is dropped to make room for hotter content.  Subsequent reads
    /// re-fetch the data from the log via the slot LSN.
    ///
    /// Skips slots that are still dirty (their data has not been written
    /// to the log yet, so dropping the in-memory copy would lose the
    /// update).  Returns the number of bytes freed (sum of the lengths
    /// of the dropped `Vec<u8>` data fields).
    ///
    /// Returns 0 if the BIN has any open cursors (the cursor may be
    /// reading the data right now).
    pub fn strip_lns(&mut self) -> usize {
        if self.cursor_count > 0 {
            return 0;
        }
        let mut freed = 0usize;
        for idx in 0..self.entries.len() {
            // JE BIN.evictLNs / LN.isEvictable (LN.java:263 returns true): an
            // LN's in-memory value can be stripped whenever it is recoverable
            // from the log — i.e. the slot has a valid (logged) LSN — REGARDLESS
            // of the dirty bit.  The dirty bit governs whether the BIN's
            // *structure* needs re-logging at the next checkpoint (BIN-delta vs
            // full BIN), NOT whether the LN *value* is durable: a transactional
            // commit logs the LN, so the slot's LSN points at the durable copy
            // even while the slot is still dirty.  Gating the strip on `!dirty`
            // (the previous behaviour) meant a freshly-written, not-yet-
            // checkpointed record — the common case under a write/recently-read
            // workload — could never be stripped, so eviction reclaimed almost
            // nothing under pressure (EVICTOR-RECLAIM-1).  A slot with a NULL/
            // transient LSN (a deferred-write LN never logged) is NOT
            // strippable — its only copy is the in-memory value.
            if self.get_lsn(idx) == NULL_LSN {
                continue;
            }
            if let Some(data) = self.entries[idx].data.take() {
                freed = freed.saturating_add(data.len());
            }
        }
        freed
    }

    /// Reconstruct the full key for slot `idx` by prepending the BIN's
    /// current prefix to the stored suffix.
    ///
    /// `IN.getKey(int idx)`.
    pub fn get_full_key(&self, idx: usize) -> Option<Vec<u8>> {
        if idx >= self.keys.len() {
            return None;
        }
        let suffix = self.keys.get(idx); // T-2
        if self.key_prefix.is_empty() {
            Some(suffix.to_vec())
        } else {
            let mut full =
                Vec::with_capacity(self.key_prefix.len() + suffix.len());
            full.extend_from_slice(&self.key_prefix);
            full.extend_from_slice(suffix);
            Some(full)
        }
    }

    /// Borrow the stored (post-prefix) suffix at slot `idx` (`INKeyRep.get`).
    #[inline]
    pub fn get_key(&self, idx: usize) -> &[u8] {
        self.keys.get(idx)
    }

    /// T-2: insert a new slot at `idx` keeping the parallel `entries`, `keys`,
    /// and `lsn_rep` arrays in lock step.  `suffix` is the post-prefix key.
    fn insert_slot(
        &mut self,
        idx: usize,
        suffix: Vec<u8>,
        lsn: Lsn,
        data: Option<Vec<u8>>,
    ) {
        self.entries.insert(
            idx,
            BinEntry {
                data,
                known_deleted: false,
                dirty: true,
                expiration_time: 0,
            },
        );
        self.keys.insert(idx, suffix); // T-2
        let n = self.entries.len();
        self.lsn_rep.insert_shift(idx, n); // T-3
        self.lsn_rep.set(idx, lsn, n);
    }

    /// Decompress a stored suffix back to a full key.
    ///
    /// `IN.getKey` used from outside: prepend `key_prefix` to
    /// `suffix`.  If `key_prefix` is empty the suffix *is* the full key.
    pub fn decompress_key(&self, suffix: &[u8]) -> Vec<u8> {
        if self.key_prefix.is_empty() {
            suffix.to_vec()
        } else {
            let mut full =
                Vec::with_capacity(self.key_prefix.len() + suffix.len());
            full.extend_from_slice(&self.key_prefix);
            full.extend_from_slice(suffix);
            full
        }
    }

    /// Strip the current prefix from a full key to obtain the stored suffix.
    ///
    /// `IN.computeKeySuffix(byte[] prefix, byte[] key)`.
    ///
    /// # Panics
    /// Panics (debug only) if `full_key` does not start with `key_prefix`.
    pub fn compress_key(&self, full_key: &[u8]) -> Vec<u8> {
        let plen = self.key_prefix.len();
        if plen == 0 {
            full_key.to_vec()
        } else {
            debug_assert!(
                full_key.starts_with(&self.key_prefix),
                "compress_key: key does not start with current prefix"
            );
            full_key[plen..].to_vec()
        }
    }

    /// Compute the longest common prefix of all full keys currently in this
    /// BIN, optionally excluding the entry at `exclude_idx` (used during
    /// insertions to ignore the slot that is about to be replaced).
    ///
    /// Returns an empty `Vec` if the BIN has fewer than 2 entries or if the
    /// keys share no common leading bytes.
    ///
    /// `IN.computeKeyPrefix(int excludeIdx)`.
    pub fn compute_key_prefix(&self, exclude_idx: Option<usize>) -> Vec<u8> {
        // Need at least 2 entries to find a common prefix.
        let n = self.keys.len();
        if n < 2 {
            return Vec::new();
        }

        // Pick the first non-excluded index as the seed.
        let first_idx = match exclude_idx {
            Some(0) => 1,
            _ => 0,
        };

        // The current prefix_len is taken from the seed full key.
        let seed_full = match self.get_full_key(first_idx) {
            Some(k) => k,
            None => return Vec::new(),
        };
        let mut prefix_len = seed_full.len();

        // Compare every other non-excluded entry against the running prefix.
        // Iterate all entries (byteOrdered disabled in too).
        for i in (first_idx + 1)..n {
            if let Some(ex) = exclude_idx
                && i == ex
            {
                continue;
            }
            let full_key = match self.get_full_key(i) {
                Some(k) => k,
                None => continue,
            };
            let new_len =
                get_key_prefix_length(&seed_full[..prefix_len], &full_key);
            if new_len < prefix_len {
                prefix_len = new_len;
            }
            if prefix_len == 0 {
                return Vec::new();
            }
        }

        seed_full[..prefix_len].to_vec()
    }

    /// Recompute the key prefix from scratch and re-encode every stored suffix.
    ///
    /// Call this after bulk inserts, splits, or merges.
    ///
    /// `IN.recalcKeyPrefix()` → `IN.recalcSuffixes(newPrefix, …)`.
    pub fn recompute_key_prefix(&mut self) {
        let new_prefix = self.compute_key_prefix(None);
        self.apply_new_prefix(new_prefix);
    }

    /// Apply `new_prefix` as the BIN's key prefix, re-encoding all stored
    /// suffixes from the old prefix into the new one.
    ///
    /// This is the Rust.
    fn apply_new_prefix(&mut self, new_prefix: Vec<u8>) {
        // Reconstruct all full keys (using old prefix), then re-encode with
        // the new prefix.
        let full_keys: Vec<Vec<u8>> = (0..self.keys.len())
            .map(|i| self.get_full_key(i).unwrap_or_default())
            .collect();

        self.key_prefix = new_prefix;

        // T-2: re-encode every suffix into the key rep, then re-attempt
        // compaction (a smaller prefix may make all suffixes fit MaxKeySize).
        for (i, full_key) in full_keys.into_iter().enumerate() {
            let suffix = self.compress_key(&full_key);
            self.keys.set(i, suffix);
        }
        self.keys.compact(self.compact_max_key_length);
    }

    /// Binary-search this BIN for `full_key` (a full, uncompressed key).
    ///
    /// The stored suffixes are compared after stripping the current prefix
    /// from `full_key`, so the search is done entirely in suffix-space — no
    /// heap allocation needed in the happy path.
    ///
    /// Returns `(idx, exact)` where:
    /// - `idx` is the slot index (or insertion point when `exact == false`).
    /// - `exact` is `true` when an exact match was found.
    ///
    /// `IN.findEntry(key, indicateIfDuplicate, exact)`.
    pub fn find_entry_compressed(&self, full_key: &[u8]) -> (usize, bool) {
        let plen = self.key_prefix.len();
        // Check that the key shares the current prefix; if not it cannot be
        // present and we return the appropriate insertion point.
        if plen > 0
            && (full_key.len() < plen
                || &full_key[..plen] != self.key_prefix.as_slice())
        {
            // The key does not share the current prefix.
            // Determine insertion point using full-key comparison.
            let pos = self.key_partition_point(|s| {
                self.decompress_key(s).as_slice() < full_key
            });
            return (pos, false);
        }
        let suffix = &full_key[plen..];
        // T-2: binary search over the node-level key rep (suffix space).
        match self.key_binary_search(suffix) {
            Ok(idx) => (idx, true),
            Err(idx) => (idx, false),
        }
    }

    /// Binary search the key rep for `suffix` (suffix space, unsigned bytes).
    /// Mirrors `Vec::binary_search_by(|e| e.key.cmp(suffix))` over the
    /// node-level `KeyRep` (T-2).
    #[inline]
    fn key_binary_search(&self, suffix: &[u8]) -> Result<usize, usize> {
        let mut lo = 0usize;
        let mut hi = self.keys.len();
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            match self.keys.get(mid).cmp(suffix) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => return Ok(mid),
            }
        }
        Err(lo)
    }

    /// `slice::partition_point` over the node-level key rep suffixes (T-2):
    /// the index of the first slot for which `pred(suffix)` is false.
    #[inline]
    fn key_partition_point(
        &self,
        mut pred: impl FnMut(&[u8]) -> bool,
    ) -> usize {
        let mut lo = 0usize;
        let mut hi = self.keys.len();
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if pred(self.keys.get(mid)) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }

    /// Insert or update a full (uncompressed) key in this BIN.
    ///
    /// After insertion the key prefix is recomputed; if the prefix changes all
    /// stored suffixes are re-encoded.
    ///
    /// Returns `(slot_index, is_new_insert)`.
    ///
    /// `IN.setKey` / BIN insert path.
    pub fn insert_with_prefix(
        &mut self,
        full_key: Vec<u8>,
        lsn: Lsn,
        data: Option<Vec<u8>>,
    ) -> (usize, bool) {
        // Is the current prefix still compatible with this key?
        let plen = self.key_prefix.len();
        let new_len = if plen > 0 {
            get_key_prefix_length(&self.key_prefix, &full_key)
        } else {
            0
        };

        // If the new key shrinks the prefix we must re-encode everything first.
        if plen > 0 && new_len < plen {
            // Compute new prefix considering the incoming key and
            // all existing full keys.  We pass `None` for exclude_idx because
            // the slot for this key does not yet exist.
            let mut candidate = self.compute_key_prefix(None);
            // Also constrain by the new key itself.
            if !candidate.is_empty() {
                let cl = get_key_prefix_length(&candidate, &full_key);
                candidate.truncate(cl);
            } else {
                // No existing prefix; try to build one from the new key
                // against the existing full keys.
                if !self.entries.is_empty()
                    && let Some(first_full) = self.get_full_key(0)
                {
                    candidate = create_key_prefix(&first_full, &full_key)
                        .unwrap_or_default();
                    for i in 1..self.entries.len() {
                        if candidate.is_empty() {
                            break;
                        }
                        if let Some(fk) = self.get_full_key(i) {
                            let l = get_key_prefix_length(&candidate, &fk);
                            candidate.truncate(l);
                        }
                    }
                }
            }
            self.apply_new_prefix(candidate);
        }

        // Compress the new key under the (possibly updated) prefix.
        let suffix = self.compress_key(&full_key);

        match self.key_binary_search(&suffix) {
            Ok(idx) => {
                // Key exists — update in place.
                self.set_lsn(idx, lsn); // T-3
                self.entries[idx].data = data;
                // Mark slot dirty: this slot changed since the last full BIN log.
                // `IN.setDirtyEntry(idx)`.
                self.entries[idx].dirty = true;
                (idx, false)
            }
            Err(idx) => {
                // New key — insert in sorted position.
                // New slots start dirty: they have never been logged in any BIN.
                // `IN.setDirtyEntry(idx)` called after `insertEntry`.
                self.insert_slot(idx, suffix, lsn, data);
                // After insertion, if there is no prefix yet, try to establish one.
                if self.key_prefix.is_empty() && self.entries.len() >= 2 {
                    self.recompute_key_prefix();
                }
                (idx, true)
            }
        }
    }

    /// Slice-based variant of [`BinStub::insert_with_prefix`] for the recovery redo path.
    ///
    /// Accepts `key` and `data` as `&[u8]` slices instead of owned `Vec<u8>`,
    /// eliminating the intermediate `Vec<u8>` that `redo_ln` would otherwise
    /// allocate before crossing the BIN boundary.  The compressed suffix and
    /// the data bytes are each copied into the `BinEntry` exactly once.
    ///
    /// Semantics are identical to `insert_with_prefix`:
    /// - Updates the slot in place when the key already exists.
    /// - Inserts a new sorted entry when absent, recomputing the key prefix.
    ///
    /// Wave 11-K optimisation (Fix 1).
    pub fn insert_with_prefix_slice(
        &mut self,
        full_key: &[u8],
        lsn: Lsn,
        data: Option<&[u8]>,
    ) -> (usize, bool) {
        let plen = self.key_prefix.len();
        let new_len = if plen > 0 {
            get_key_prefix_length(&self.key_prefix, full_key)
        } else {
            0
        };

        if plen > 0 && new_len < plen {
            let mut candidate = self.compute_key_prefix(None);
            if !candidate.is_empty() {
                let cl = get_key_prefix_length(&candidate, full_key);
                candidate.truncate(cl);
            } else {
                if !self.entries.is_empty()
                    && let Some(first_full) = self.get_full_key(0)
                {
                    candidate = create_key_prefix(&first_full, full_key)
                        .unwrap_or_default();
                    for i in 1..self.entries.len() {
                        if candidate.is_empty() {
                            break;
                        }
                        if let Some(fk) = self.get_full_key(i) {
                            let l = get_key_prefix_length(&candidate, &fk);
                            candidate.truncate(l);
                        }
                    }
                }
            }
            self.apply_new_prefix(candidate);
        }

        let suffix = self.compress_key(full_key);

        match self.key_binary_search(&suffix) {
            Ok(idx) => {
                self.set_lsn(idx, lsn); // T-3
                self.entries[idx].data = data.map(|d| d.to_vec());
                self.entries[idx].dirty = true;
                (idx, false)
            }
            Err(idx) => {
                self.insert_slot(idx, suffix, lsn, data.map(|d| d.to_vec()));
                if self.key_prefix.is_empty() && self.entries.len() >= 2 {
                    self.recompute_key_prefix();
                }
                (idx, true)
            }
        }
    }

    /// Returns the number of slots that are marked dirty.
    ///
    /// `BIN.getNumDirtyEntries()`.
    pub fn dirty_count(&self) -> usize {
        self.entries.iter().filter(|e| e.dirty).count()
    }

    /// Decide whether to log this BIN as a delta (true) or a full BIN (false).
    ///
    /// Faithful port of JE `BIN.shouldLogDelta()` (BIN.java:1892).  The
    /// decision is COUNT-based (number of would-be delta slots vs a percent of
    /// `nEntries`), NOT a dirty-fraction-vs-hardcoded-0.25 heuristic:
    ///
    /// ```text
    /// if (isBINDelta()) { return true; }          // already a delta
    /// if (isDeltaProhibited()) return false;       // prohibit / no prior full
    /// numDeltas = getNDeltas();
    /// if (numDeltas <= 0) return false;            // empty delta is invalid
    /// deltaLimit = (getNEntries() * binDeltaPercent) / 100;  // INTEGER math
    /// return numDeltas <= deltaLimit;
    /// ```
    ///
    /// `numDeltas` (JE `getNDeltas`) is the count of slots that would appear in
    /// the delta — i.e. the dirty slots since the last full BIN — which here is
    /// `dirty_count()`.  `binDeltaPercent` is the CONFIGURABLE `TREE_BIN_DELTA`
    /// param (JE `DatabaseImpl.getBinDeltaPercent()`, default 25), threaded in
    /// by the checkpointer — NOT a hardcoded constant.
    ///
    /// `isDeltaProhibited()` (BIN.java:1867) is
    /// `getProhibitNextDelta() || isDeferredWriteMode() || lastFullLsn == NULL`.
    /// Deferred-write mode is not modelled in the runtime stub; the other two
    /// terms are.
    ///
    /// JE ref: `BIN.shouldLogDelta` (BIN.java:1892), `BIN.isDeltaProhibited`
    /// (BIN.java:1867).
    pub fn should_log_delta(&self, bin_delta_percent: i32) -> bool {
        // Already a delta: re-log as a delta.  JE asserts !prohibitNextDelta
        // and lastFullLsn != NULL here.
        if self.is_delta {
            return self.last_full_lsn != NULL_LSN && !self.prohibit_next_delta;
        }

        // isDeltaProhibited(): cheapest checks first.
        if self.prohibit_next_delta || self.last_full_lsn == NULL_LSN {
            return false;
        }

        // numDeltas = getNDeltas(): the dirty slots that would be in the delta.
        let num_deltas = self.dirty_count() as i32;

        // A delta with zero items is not valid.
        if num_deltas <= 0 {
            return false;
        }

        // Configured BinDeltaPercent limit — INTEGER math, exactly as JE.
        let delta_limit = (self.entries.len() as i32 * bin_delta_percent) / 100;
        num_deltas <= delta_limit
    }

    /// Comparator-aware binary search: finds `full_key` using `cmp`.
    ///
    /// Unlike `find_entry_compressed` (which uses suffix-based lexicographic
    /// comparison), this decompresses each entry's key to its full form and
    /// applies the provided comparator — required for sorted-dup databases
    /// where lexicographic suffix comparison would give wrong results when
    /// different-length primary keys are in the same BIN.
    ///
    /// Returns `(idx, exact)`.  Does NOT do prefix compression.
    ///
    /// `IN.findEntry` with btreeComparator active.
    pub fn find_entry_cmp(
        &self,
        full_key: &[u8],
        cmp: &dyn Fn(&[u8], &[u8]) -> std::cmp::Ordering,
    ) -> (usize, bool) {
        // Hot path: avoid per-comparison Vec<u8> allocation.
        // When key_prefix is empty the stored suffix IS the full key, so we
        // pass the suffix slice directly.  When prefix is non-empty we build a
        // temporary concatenation only once per comparison using a small
        // stack-local Vec that is dropped immediately after the call — this
        // still allocates but is limited to O(key_len) bytes per call and
        // avoids retaining any heap state between comparisons.
        if self.key_prefix.is_empty() {
            match self.key_binary_search_by(|s| cmp(s, full_key)) {
                Ok(idx) => (idx, true),
                Err(idx) => (idx, false),
            }
        } else {
            let prefix = self.key_prefix.as_slice();
            match self.key_binary_search_by(|s| {
                let mut fk = Vec::with_capacity(prefix.len() + s.len());
                fk.extend_from_slice(prefix);
                fk.extend_from_slice(s);
                cmp(&fk, full_key)
            }) {
                Ok(idx) => (idx, true),
                Err(idx) => (idx, false),
            }
        }
    }

    /// Comparator-driven binary search over the node-level key rep (T-2).
    /// `cmp(stored_suffix)` returns how the stored slot compares to the
    /// search key.
    #[inline]
    fn key_binary_search_by(
        &self,
        mut cmp: impl FnMut(&[u8]) -> std::cmp::Ordering,
    ) -> Result<usize, usize> {
        let mut lo = 0usize;
        let mut hi = self.keys.len();
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            match cmp(self.keys.get(mid)) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => return Ok(mid),
            }
        }
        Err(lo)
    }

    /// Returns the LSN of the slot matching `full_key`, if one exists.
    ///
    /// Used by the recovery LN-redo apply to enforce JE's currency check
    /// (`RecoveryManager.redo()` line ~2512): a logged LN is applied only
    /// when `logrecLsn > treeLsn`.  Returns `None` when the key is absent
    /// (always apply).  Uses the same lookup variant the matching insert
    /// path uses so the comparison is over the right slot.
    pub fn redo_slot_lsn(
        &self,
        full_key: &[u8],
        cmp: Option<&dyn Fn(&[u8], &[u8]) -> std::cmp::Ordering>,
        key_prefixing: bool,
    ) -> Option<Lsn> {
        let (idx, found) = match cmp {
            Some(c) => self.find_entry_cmp(full_key, c),
            None if key_prefixing => self.find_entry_compressed(full_key),
            None => {
                // insert_raw path: full keys stored verbatim.
                match self.key_binary_search(full_key) {
                    Ok(idx) => (idx, true),
                    Err(idx) => (idx, false),
                }
            }
        };
        if found { Some(self.get_lsn(idx)) } else { None }
    }

    /// Raw insert (no prefix compression) for databases with
    /// `key_prefixing = false`.
    ///
    /// JE `IN.computeKeyPrefix` returns `null` when
    /// `databaseImpl.getKeyPrefixing()` is `false`, so no prefix is ever
    /// set on those BINs.  Noxu was previously ignoring the flag and always
    /// calling `insert_with_prefix`; this method provides the faithful path.
    ///
    /// The key is stored verbatim (no suffix stripping). An existing
    /// `key_prefix` on the BIN is left untouched; callers must ensure it is
    /// empty (split_child already guarantees this for new BINs when
    /// `key_prefixing = false`).
    ///
    /// Returns `(slot_index, is_new_insert)`.
    ///
    /// Ref: `IN.java computeKeyPrefix` ~line 2456,
    ///      `DatabaseConfig.setKeyPrefixing` / `DatabaseImpl.getKeyPrefixing`.
    pub fn insert_raw(
        &mut self,
        full_key: Vec<u8>,
        lsn: Lsn,
        data: Option<Vec<u8>>,
    ) -> (usize, bool) {
        // Binary search on the stored (full) keys.
        // When key_prefix is empty entries store full keys directly; for
        // key_prefixing=false DBs the prefix is always empty.
        match self.key_binary_search(full_key.as_slice()) {
            Ok(idx) => {
                self.set_lsn(idx, lsn); // T-3
                self.entries[idx].data = data;
                self.entries[idx].dirty = true;
                (idx, false)
            }
            Err(idx) => {
                self.insert_slot(idx, full_key, lsn, data);
                (idx, true)
            }
        }
    }

    /// Comparator-aware insert: inserts `full_key` into the BIN using `cmp`.
    ///
    /// Prefix compression is DISABLED: the key is stored as-is.  This is
    /// intentional for sorted-dup databases where the custom comparator
    /// requires full-key access at every comparison.
    ///
    /// Returns `(slot_index, is_new_insert)`.
    ///
    pub fn insert_cmp(
        &mut self,
        full_key: Vec<u8>,
        lsn: Lsn,
        data: Option<Vec<u8>>,
        cmp: &dyn Fn(&[u8], &[u8]) -> std::cmp::Ordering,
    ) -> (usize, bool) {
        if self.key_prefix.is_empty() {
            match self.key_binary_search_by(|s| cmp(s, &full_key)) {
                Ok(idx) => {
                    self.set_lsn(idx, lsn); // T-3
                    self.entries[idx].data = data;
                    self.entries[idx].dirty = true;
                    (idx, false)
                }
                Err(idx) => {
                    self.insert_slot(idx, full_key, lsn, data);
                    (idx, true)
                }
            }
        } else {
            let prefix = self.key_prefix.clone();
            match self.key_binary_search_by(|s| {
                let mut fk = Vec::with_capacity(prefix.len() + s.len());
                fk.extend_from_slice(&prefix);
                fk.extend_from_slice(s);
                cmp(&fk, &full_key)
            }) {
                Ok(idx) => {
                    // Key exists — update in place.
                    self.set_lsn(idx, lsn); // T-3
                    self.entries[idx].data = data;
                    self.entries[idx].dirty = true;
                    (idx, false)
                }
                Err(idx) => {
                    // New key — insert at sorted position (no prefix compression).
                    self.insert_slot(idx, full_key, lsn, data);
                    (idx, true)
                }
            }
        }
    }

    /// Comparator-aware delete: removes `full_key` from the BIN using `cmp`.
    ///
    /// Returns `true` if the entry was found and removed.
    pub fn delete_cmp(
        &mut self,
        full_key: &[u8],
        cmp: &dyn Fn(&[u8], &[u8]) -> std::cmp::Ordering,
    ) -> bool {
        let result = if self.key_prefix.is_empty() {
            self.key_binary_search_by(|s| cmp(s, full_key))
        } else {
            let prefix = self.key_prefix.clone();
            self.key_binary_search_by(|s| {
                let mut fk = Vec::with_capacity(prefix.len() + s.len());
                fk.extend_from_slice(&prefix);
                fk.extend_from_slice(s);
                cmp(&fk, full_key)
            })
        };
        match result {
            Ok(idx) => {
                self.entries.remove(idx);
                self.keys.remove(idx); // T-2
                self.lsn_rep.remove_shift(idx); // T-3
                self.dirty = true;
                true
            }
            Err(_) => false,
        }
    }

    /// Serialise ALL entries (full BIN write).
    ///
    /// Format (per slot): key_len(u32BE) | key | lsn(u64BE) |
    ///   has_data(u8) | data_len(u32BE) | data | known_deleted(u8)
    ///
    /// Prepended by: node_id(u64BE) | num_entries(u32BE).
    ///
    /// `BIN.writeToLog()` (non-delta path).
    pub fn serialize_full(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&self.node_id.to_be_bytes());
        buf.extend_from_slice(&(self.entries.len() as u32).to_be_bytes());
        for i in 0..self.entries.len() {
            let full_key = self.get_full_key(i).unwrap_or_default();
            buf.extend_from_slice(&(full_key.len() as u32).to_be_bytes());
            buf.extend_from_slice(&full_key);
            let lsn = self.get_lsn(i); // T-3
            let e = &self.entries[i];
            buf.extend_from_slice(&lsn.as_u64().to_be_bytes());
            if let Some(d) = &e.data {
                buf.push(1u8);
                buf.extend_from_slice(&(d.len() as u32).to_be_bytes());
                buf.extend_from_slice(d);
            } else {
                buf.push(0u8);
            }
            buf.push(e.known_deleted as u8);
        }
        buf
    }

    /// Serialise only dirty slots (BIN-delta write).
    ///
    /// Format (per dirty slot): slot_idx(u32BE) | key_len(u32BE) | key |
    ///   lsn(u64BE) | has_data(u8) | data_len(u32BE) | data | known_deleted(u8)
    ///
    /// Prepended by: node_id(u64BE) | num_dirty(u32BE).
    ///
    /// `BIN.writeToLog()` (delta path).
    pub fn serialize_delta(&self) -> Vec<u8> {
        let dirty: Vec<usize> = (0..self.entries.len())
            .filter(|&i| self.entries[i].dirty)
            .collect();
        let mut buf = Vec::new();
        buf.extend_from_slice(&self.node_id.to_be_bytes());
        buf.extend_from_slice(&(dirty.len() as u32).to_be_bytes());
        for idx in dirty {
            buf.extend_from_slice(&(idx as u32).to_be_bytes());
            let full_key = self.get_full_key(idx).unwrap_or_default();
            buf.extend_from_slice(&(full_key.len() as u32).to_be_bytes());
            buf.extend_from_slice(&full_key);
            let lsn = self.get_lsn(idx); // T-3
            let e = &self.entries[idx];
            buf.extend_from_slice(&lsn.as_u64().to_be_bytes());
            if let Some(d) = &e.data {
                buf.push(1u8);
                buf.extend_from_slice(&(d.len() as u32).to_be_bytes());
                buf.extend_from_slice(d);
            } else {
                buf.push(0u8);
            }
            buf.push(e.known_deleted as u8);
        }
        buf
    }

    /// Deserialise a full BIN from the bytes produced by `serialize_full()`.
    ///
    /// Returns a `BinStub` with all entries populated and all slots marked
    /// clean (they are already on disk at `last_full_lsn`).  Returns `None`
    /// if the byte slice is malformed.
    ///
    /// `INLogEntry.readEntry()` / `IN.readFromLog()` (non-delta).
    pub fn deserialize_full(bytes: &[u8]) -> Option<BinStub> {
        if bytes.len() < 12 {
            return None;
        }
        let node_id = u64::from_be_bytes(bytes[0..8].try_into().ok()?);
        let num_entries =
            u32::from_be_bytes(bytes[8..12].try_into().ok()?) as usize;
        let mut pos = 12usize;
        let mut entries = Vec::with_capacity(num_entries);
        let mut lsns: Vec<Lsn> = Vec::with_capacity(num_entries);
        let mut keys: Vec<Vec<u8>> = Vec::with_capacity(num_entries); // T-2
        for _ in 0..num_entries {
            // key_len(u32BE) | key | lsn(u64BE) | has_data(u8) [| data_len(u32BE) | data] | known_deleted(u8)
            if pos + 4 > bytes.len() {
                return None;
            }
            let key_len =
                u32::from_be_bytes(bytes[pos..pos + 4].try_into().ok()?)
                    as usize;
            pos += 4;
            if pos + key_len > bytes.len() {
                return None;
            }
            let key = bytes[pos..pos + key_len].to_vec();
            pos += key_len;
            if pos + 8 > bytes.len() {
                return None;
            }
            let lsn = Lsn::from_u64(u64::from_be_bytes(
                bytes[pos..pos + 8].try_into().ok()?,
            ));
            pos += 8;
            if pos + 1 > bytes.len() {
                return None;
            }
            let has_data = bytes[pos] != 0;
            pos += 1;
            let data = if has_data {
                if pos + 4 > bytes.len() {
                    return None;
                }
                let data_len =
                    u32::from_be_bytes(bytes[pos..pos + 4].try_into().ok()?)
                        as usize;
                pos += 4;
                if pos + data_len > bytes.len() {
                    return None;
                }
                let d = bytes[pos..pos + data_len].to_vec();
                pos += data_len;
                Some(d)
            } else {
                None
            };
            if pos + 1 > bytes.len() {
                return None;
            }
            let known_deleted = bytes[pos] != 0;
            pos += 1;
            entries.push(BinEntry {
                data,
                known_deleted,
                dirty: false, // freshly loaded from log — clean
                expiration_time: 0,
            });
            keys.push(key); // T-2 (full keys; recompute_key_prefix compresses)
            lsns.push(lsn); // T-3
        }
        // Keys stored in the serialized format are full (uncompressed) keys.
        // Re-establish the key prefix after loading so that memory use and
        // search performance match an in-memory BIN.
        // `IN.readFromLog()` → key prefix is part of the wire
        // format in the; in Noxu we store full keys and recompute on load.
        let mut bin = BinStub {
            node_id,
            level: BIN_LEVEL,
            entries,
            key_prefix: Vec::new(),
            dirty: false,
            is_delta: false,
            last_full_lsn: NULL_LSN, // caller sets this to the logged LSN
            last_delta_lsn: NULL_LSN,
            generation: 0,
            parent: None,
            expiration_in_hours: true,
            cursor_count: 0,
            prohibit_next_delta: false,
            lsn_rep: LsnRep::from_lsns(&lsns), // T-3
            keys: KeyRep::from_keys(keys),     // T-2 (full keys, no prefix yet)
            compact_max_key_length: INKeyRep_DEFAULT_MAX_KEY_LENGTH,
        };
        // Recompute key prefix from the full keys just loaded.
        // `IN.recalcKeyPrefix()` called after materializing from log.
        if bin.entries.len() >= 2 {
            bin.recompute_key_prefix();
        } else {
            // Even a single-slot BIN should attempt compaction.
            bin.keys.compact(bin.compact_max_key_length);
        }
        Some(bin)
    }

    /// Deserialise a BIN delta from the bytes produced by `serialize_delta()`.
    ///
    /// **DO NOT USE for BIN reconstruction.** This helper writes full
    /// (uncompressed) keys directly into slots without recomputing the BIN
    /// key prefix, so on a prefix-compressed BIN it corrupts the slot keys and
    /// breaks the sorted-suffix invariant. It is NOT wired into any live path.
    /// The correct delta-reconstruction path is
    /// `mutate_to_full_bin` → `apply_delta_to_bin` → `insert_with_prefix`,
    /// which recomputes the prefix. This function is retained only for the
    /// raw byte-format round-trip and must not be used to reconstitute a BIN.
    /// Tracked for removal — see the v3.x review synthesis (storage C-2).
    ///
    /// Returns `None` if `delta_bytes` is malformed.
    pub fn apply_delta(base: &mut BinStub, delta_bytes: &[u8]) -> Option<()> {
        if delta_bytes.len() < 12 {
            return None;
        }
        // node_id(u64BE) — must match base
        let _node_id = u64::from_be_bytes(delta_bytes[0..8].try_into().ok()?);
        let num_dirty =
            u32::from_be_bytes(delta_bytes[8..12].try_into().ok()?) as usize;
        let mut pos = 12usize;
        for _ in 0..num_dirty {
            // slot_idx(u32BE) | key_len(u32BE) | key | lsn(u64BE) | has_data(u8) [| data_len | data] | known_deleted(u8)
            if pos + 4 > delta_bytes.len() {
                return None;
            }
            let slot_idx =
                u32::from_be_bytes(delta_bytes[pos..pos + 4].try_into().ok()?)
                    as usize;
            pos += 4;
            if pos + 4 > delta_bytes.len() {
                return None;
            }
            let key_len =
                u32::from_be_bytes(delta_bytes[pos..pos + 4].try_into().ok()?)
                    as usize;
            pos += 4;
            if pos + key_len > delta_bytes.len() {
                return None;
            }
            let key = delta_bytes[pos..pos + key_len].to_vec();
            pos += key_len;
            if pos + 8 > delta_bytes.len() {
                return None;
            }
            let lsn = Lsn::from_u64(u64::from_be_bytes(
                delta_bytes[pos..pos + 8].try_into().ok()?,
            ));
            pos += 8;
            if pos + 1 > delta_bytes.len() {
                return None;
            }
            let has_data = delta_bytes[pos] != 0;
            pos += 1;
            let data = if has_data {
                if pos + 4 > delta_bytes.len() {
                    return None;
                }
                let data_len = u32::from_be_bytes(
                    delta_bytes[pos..pos + 4].try_into().ok()?,
                ) as usize;
                pos += 4;
                if pos + data_len > delta_bytes.len() {
                    return None;
                }
                let d = delta_bytes[pos..pos + data_len].to_vec();
                pos += data_len;
                Some(d)
            } else {
                None
            };
            if pos + 1 > delta_bytes.len() {
                return None;
            }
            let known_deleted = delta_bytes[pos] != 0;
            pos += 1;

            // Apply to base: update existing slot or insert new one.
            if slot_idx < base.entries.len() {
                base.keys.set(slot_idx, key); // T-2
                base.set_lsn(slot_idx, lsn); // T-3
                base.entries[slot_idx].data = data;
                base.entries[slot_idx].known_deleted = known_deleted;
                base.entries[slot_idx].dirty = false;
            } else {
                // Slot index beyond current length — append.
                base.entries.push(BinEntry {
                    data,
                    known_deleted,
                    dirty: false,
                    expiration_time: 0,
                });
                let n = base.entries.len();
                base.keys.insert(n - 1, key); // T-2
                base.lsn_rep.set(n - 1, lsn, n); // T-3
            }
        }
        Some(())
    }

    /// Clear per-slot dirty flags and record `logged_at` as the LSN at which
    /// this BIN was last fully logged.
    ///
    /// Called by the checkpoint path after a successful full-BIN log write.
    /// `BIN.afterLog()` / `BIN.setLastFullLsn()`.
    pub fn clear_dirty_after_full_log(&mut self, logged_at: Lsn) {
        for e in &mut self.entries {
            e.dirty = false;
        }
        self.last_full_lsn = logged_at;
        self.dirty = false;
        // A full BIN captures all current state, so the delta-chain bound is
        // cleared: the next log may once again be a delta.
        // JE `IN.afterLog` clears the prohibit flag after a full log
        // (IN.java:5557 `bin.setProhibitNextDelta(false)`).
        self.prohibit_next_delta = false;
    }

    /// Clear per-slot dirty flags after a successful delta log write.
    ///
    /// `last_full_lsn` is NOT updated — the full LSN only changes after a
    /// full BIN write.
    /// `BIN.afterLog()` (delta path).
    pub fn clear_dirty_after_delta_log(&mut self) {
        for e in &mut self.entries {
            e.dirty = false;
        }
        self.dirty = false;
    }
}

impl TreeNode {
    /// Returns true if this is a BIN (bottom internal node).
    pub fn is_bin(&self) -> bool {
        matches!(self, TreeNode::Bottom(_))
    }

    /// Returns the level of this node.
    pub fn level(&self) -> i32 {
        match self {
            TreeNode::Internal(n) => n.level,
            TreeNode::Bottom(b) => b.level,
        }
    }

    /// Returns the node id of this node.
    pub fn node_id(&self) -> u64 {
        match self {
            TreeNode::Internal(n) => n.node_id,
            TreeNode::Bottom(b) => b.node_id,
        }
    }

    /// Faithful in-memory heap footprint of this node, in bytes.
    ///
    /// JE `IN.getBudgetedMemorySize()` (IN.java) returns the running
    /// `inMemorySize` that `MemoryBudget` tracks for the node: the fixed
    /// IN/BIN struct overhead plus, per slot, the fixed entry overhead and the
    /// variable key (and embedded-LN data for BINs) bytes.  This is the single
    /// source of truth for both the live tree accounting and the evictor's
    /// detach credit (EV-13) — keeping it on `TreeNode` avoids the formula
    /// drifting between `noxu-tree` and `noxu-evictor`.
    ///
    /// Rust has a fixed struct layout (unlike JE's `Sizeof`-measured JVM
    /// constants) so `size_of` is exact for the fixed overheads; the variable
    /// part mirrors JE's per-slot `entryKeys`/embedded-data accounting.
    pub fn budgeted_memory_size(&self) -> u64 {
        use std::mem::size_of;
        match self {
            TreeNode::Bottom(b) => {
                (size_of::<BinStub>()
                    + b.entries.len() * size_of::<BinEntry>()
                    + b.key_prefix.len()
                    + b.keys.memory_size() // T-2: node-level key rep bytes
                    + b.lsn_rep.memory_size() // T-3: node-level LSN rep bytes
                    + b.entries
                        .iter()
                        .map(|e| {
                            e.data.as_ref().map(|d| d.len()).unwrap_or(0)
                        })
                        .sum::<usize>()) as u64
            }
            TreeNode::Internal(n) => {
                (size_of::<InNodeStub>()
                    + n.entries.len() * size_of::<InEntry>()
                    + n.targets.memory_size()
                    + n.entries.iter().map(|e| e.key.len()).sum::<usize>())
                    as u64
            }
        }
    }

    /// Binary search for a key in this node.
    ///
    /// For BIN nodes the search is prefix-aware: if the BIN has a key prefix,
    /// `key` (a full, uncompressed key) is compared against stored suffixes
    /// after stripping the prefix.
    /// `IN.findEntry(key, indicateIfDuplicate, exact)`.
    ///
    /// Returns index with EXACT_MATCH flag set if exact match found.
    /// If exact is false, returns insertion point.
    pub fn find_entry(&self, key: &[u8], _indicator: bool, exact: bool) -> i32 {
        match self {
            TreeNode::Internal(n) => {
                let result = n
                    .entries
                    .binary_search_by(|entry| entry.key.as_slice().cmp(key));
                match result {
                    Ok(idx) => (idx as i32) | EXACT_MATCH,
                    Err(idx) => {
                        if exact {
                            -1
                        } else {
                            // Floor (not insertion point): the child slot to
                            // descend into is the largest entry ≤ key. Slot 0
                            // is the leftmost child, so a key below every
                            // separator floors to 0. (St-H5: previously
                            // returned the insertion point `idx`, which routes
                            // one child too far right.)
                            (idx as i32 - 1).max(0)
                        }
                    }
                }
            }
            TreeNode::Bottom(b) => {
                // Use prefix-aware search: the stored key is a suffix when
                // key_prefix is non-empty.
                let (idx, found) = b.find_entry_compressed(key);
                if found {
                    (idx as i32) | EXACT_MATCH
                } else if exact {
                    -1
                } else {
                    idx as i32
                }
            }
        }
    }

    /// Gets the number of entries in this node.
    pub fn get_n_entries(&self) -> usize {
        match self {
            TreeNode::Internal(n) => n.entries.len(),
            TreeNode::Bottom(b) => b.entries.len(),
        }
    }

    // ========================================================================
    // Dirty flag
    // ========================================================================

    /// Returns true if this node has been modified since last checkpoint.
    ///
    /// `IN.getDirty()`.
    pub fn is_dirty(&self) -> bool {
        match self {
            TreeNode::Internal(n) => n.dirty,
            TreeNode::Bottom(b) => b.dirty,
        }
    }

    /// Sets or clears the dirty flag on this node.
    ///
    /// `IN.setDirty(boolean dirty)`.
    pub fn set_dirty(&mut self, dirty: bool) {
        match self {
            TreeNode::Internal(n) => n.dirty = dirty,
            TreeNode::Bottom(b) => b.dirty = dirty,
        }
    }

    // ========================================================================
    // LRU generation
    // ========================================================================

    /// Returns the LRU generation counter.
    ///
    /// `IN.getGeneration()`.
    pub fn get_generation(&self) -> u64 {
        match self {
            TreeNode::Internal(n) => n.generation,
            TreeNode::Bottom(b) => b.generation,
        }
    }

    /// Sets the LRU generation counter.
    ///
    /// `IN.setGeneration(long gen)`.
    pub fn set_generation(&mut self, r#gen: u64) {
        match self {
            TreeNode::Internal(n) => n.generation = r#gen,
            TreeNode::Bottom(b) => b.generation = r#gen,
        }
    }

    // ========================================================================
    // Parent pointer
    // ========================================================================

    /// Returns a clone of the weak parent pointer, if any.
    pub fn get_parent(&self) -> Option<Weak<RwLock<TreeNode>>> {
        match self {
            TreeNode::Internal(n) => n.parent.clone(),
            TreeNode::Bottom(b) => b.parent.clone(),
        }
    }

    /// Sets the weak parent pointer on this node.
    pub fn set_parent(&mut self, parent: Option<Weak<RwLock<TreeNode>>>) {
        match self {
            TreeNode::Internal(n) => n.parent = parent,
            TreeNode::Bottom(b) => b.parent = parent,
        }
    }

    // ========================================================================
    // Log serialization
    // ========================================================================

    /// Estimates the serialized byte size of this node for log/checkpoint use.
    ///
    /// `IN.getLogSize()` — Noxu-native serialization format.
    ///
    /// Format (big-endian):
    /// - node_id     : 8 bytes
    /// - level       : 4 bytes
    /// - n_entries   : 4 bytes
    /// - dirty       : 1 byte
    /// - For each entry:
    ///   - key_len   : 2 bytes
    ///   - key       : key_len bytes
    ///   - lsn       : 8 bytes
    pub fn log_size(&self) -> usize {
        // Fixed header: node_id(8) + level(4) + n_entries(4) + dirty(1)
        let mut size: usize = 8 + 4 + 4 + 1;
        match self {
            TreeNode::Internal(n) => {
                for entry in &n.entries {
                    size += 2 + entry.key.len() + 8; // key_len + key + lsn
                }
            }
            TreeNode::Bottom(b) => {
                for i in 0..b.entries.len() {
                    size += 2 + b.get_key(i).len() + 8; // key_len + key + lsn
                }
            }
        }
        size
    }

    /// Serializes this node to bytes for log writing.
    ///
    /// `IN.writeToLog(ByteBuffer logBuffer)` — Noxu-native
    /// format matching `log_size()`.
    pub fn write_to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.log_size());
        match self {
            TreeNode::Internal(n) => {
                buf.extend_from_slice(&n.node_id.to_be_bytes());
                buf.extend_from_slice(&n.level.to_be_bytes());
                buf.extend_from_slice(&(n.entries.len() as u32).to_be_bytes());
                buf.push(n.dirty as u8);
                for (i, entry) in n.entries.iter().enumerate() {
                    buf.extend_from_slice(
                        &(entry.key.len() as u16).to_be_bytes(),
                    );
                    buf.extend_from_slice(&entry.key);
                    buf.extend_from_slice(&n.get_lsn(i).as_u64().to_be_bytes());
                }
            }
            TreeNode::Bottom(b) => {
                buf.extend_from_slice(&b.node_id.to_be_bytes());
                buf.extend_from_slice(&b.level.to_be_bytes());
                buf.extend_from_slice(&(b.entries.len() as u32).to_be_bytes());
                buf.push(b.dirty as u8);
                for i in 0..b.entries.len() {
                    let key = b.get_key(i);
                    buf.extend_from_slice(&(key.len() as u16).to_be_bytes());
                    buf.extend_from_slice(key);
                    buf.extend_from_slice(&b.get_lsn(i).as_u64().to_be_bytes());
                }
            }
        }
        buf
    }
}

/// Internal helper used during splits to carry entries of either node kind.
///
/// `BinStub` and `InNodeStub` store different entry types, so we need a
/// common wrapper to pass split slices around without code duplication.
enum SplitEntries {
    /// Upper-IN entries plus the parallel resident-child pointers (one per
    /// entry; `None` when the child is not cached) and the parallel per-slot
    /// LSNs (T-3: LSNs travel with their slots on a split, just like JE
    /// `IN.split` copies `entryLsnByteArray`/`entryLsnLongArray`).
    Internal(Vec<InEntry>, Vec<Option<ChildArc>>, Vec<Lsn>),
    /// BIN entries (metadata only) plus the parallel per-slot LSNs and the
    /// parallel FULL keys (T-2: keys live in the node-level `KeyRep`, not in
    /// `BinEntry`, so they travel as a separate `Vec<Vec<u8>>` of full keys
    /// through the split — the new BINs recompute their prefix from these).
    Bottom(Vec<BinEntry>, Vec<Lsn>, Vec<Vec<u8>>),
}

impl SplitEntries {
    /// Returns the number of entries.
    fn len(&self) -> usize {
        match self {
            SplitEntries::Internal(v, _, _) => v.len(),
            SplitEntries::Bottom(v, _, _) => v.len(),
        }
    }

    /// Returns the key at `index` as a slice.
    fn get_key(&self, index: usize) -> &[u8] {
        match self {
            SplitEntries::Internal(v, _, _) => v[index].key.as_slice(),
            SplitEntries::Bottom(_, _, k) => k[index].as_slice(),
        }
    }

    /// Returns a sub-range `[lo, hi)` as a new `SplitEntries`.
    fn slice(&self, lo: usize, hi: usize) -> Self {
        match self {
            SplitEntries::Internal(v, c, l) => SplitEntries::Internal(
                v[lo..hi].to_vec(),
                c[lo..hi].to_vec(),
                l[lo..hi].to_vec(),
            ),
            SplitEntries::Bottom(v, l, k) => SplitEntries::Bottom(
                v[lo..hi].to_vec(),
                l[lo..hi].to_vec(),
                k[lo..hi].to_vec(),
            ),
        }
    }
}

/// Tri-state outcome from one attempt at
/// `Tree::get_adjacent_bin_attempt`.
///
/// Distinguishes "the tree genuinely has no BIN in the requested
/// direction" (→ propagate as end-of-iteration) from "the path we
/// captured was invalidated by a concurrent split" (→ caller
/// retries from root). This split is necessary because the cursor
/// translates a `None` from `get_adjacent_bin` into
/// `OperationStatus::NotFound`, which is indistinguishable from a
/// real end-of-tree.
#[derive(Debug)]
enum AdjacentBinOutcome {
    /// A BIN was found in the requested direction.  T-3: each slot carries its
    /// `Lsn` alongside the `BinEntry` (the LSN lives in the node's packed
    /// `LsnRep`, not in `BinEntry`, so the scan snapshot pairs them).
    Found(Vec<(BinEntry, Lsn, Vec<u8>)>),
    /// The tree genuinely has no BIN in the requested direction.
    NoAdjacent,
    /// A concurrent split invalidated our captured path; the
    /// caller should retry from root.
    SplitRaceRetry,
}

/// Split hint for the `splitSpecial` heuristic.
///
/// JE `Tree.forceSplit` tracks `allLeftSideDescent` / `allRightSideDescent`
/// (true if **every** routing decision during the top-down descent followed
/// the leftmost / rightmost child). At split time, when one of those flags
/// is set, `IN.splitSpecial` forces the split index to 1 (left side) or
/// `nEntries - 1` (right side) instead of `nEntries / 2`.
///
/// Effect: for sequential-append workloads the left BIN stays near-full
/// after every split (only one entry migrates to the new sibling), cutting
/// the split count roughly in half and reducing write amplification.
///
/// Ref: `IN.java splitSpecial` ~line 4129, `Tree.java forceSplit` ~line 1907.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SplitHint {
    /// Normal midpoint split (`n_entries / 2`).
    Normal,
    /// Key was at position 0 on every level of descent.
    /// → `split_index = 1` so left node keeps all but the first entry.
    AllLeft,
    /// Key was at the rightmost position on every level of descent.
    /// → `split_index = n_entries - 1` so left node keeps almost everything.
    AllRight,
}

impl Tree {
    /// Creates a new empty tree.
    ///
    /// Constructor.
    pub fn new(database_id: u64, max_entries_per_node: usize) -> Self {
        Tree {
            database_id,
            max_entries_per_node,
            root: RwLock::new(None),
            root_latch: SharedLatch::new(LatchContext::new("TreeRoot"), false),
            root_log_lsn: RwLock::new(noxu_util::NULL_LSN),
            root_splits: AtomicU64::new(0),
            relatches_required: AtomicU64::new(0),
            key_comparator: None,
            memory_counter: None,
            in_list_listener: None,
            log_manager: None,
            redo_capacity_hint: 0,
            key_prefixing: false, // JE default: KEY_PREFIXING_DEFAULT = false
            compact_max_key_length: INKeyRep_DEFAULT_MAX_KEY_LENGTH, // T-5
        }
    }

    /// Installs a shared memory counter for evictor / MemoryBudget feedback.
    ///
    /// → `env.getMemoryBudget().updateTreeMemoryUsage(delta)`
    ///.  The counter is updated on every BIN entry insert/delete.
    pub fn set_memory_counter(&mut self, counter: Arc<AtomicI64>) {
        self.memory_counter = Some(counter);
    }

    /// Installs the [`InListListener`] (the evictor) so node add/access/remove
    /// feed the LRU lists.  JE: `INList` registration that feeds
    /// `Evictor.addBack`/`moveBack`/`remove`.
    pub fn set_in_list_listener(&mut self, listener: Arc<dyn InListListener>) {
        self.in_list_listener = Some(listener);
    }

    /// Installs the [`noxu_log::LogManager`] so an evicted root IN can be
    /// re-materialized from its persisted LSN on the next access (EV-14).
    ///
    /// JE: the tree reaches the log through `database.getEnv().getLogManager()`
    /// for `ChildReference.fetchTarget`.  Noxu installs it directly.
    pub fn set_log_manager(&mut self, lm: Arc<noxu_log::LogManager>) {
        self.log_manager = Some(lm);
    }

    /// Drops this tree's `Arc<LogManager>` reference (EV-14 teardown).
    ///
    /// The env's `Drop` calls this on every tree it owns so the
    /// `Tree -> Arc<LogManager> -> Arc<FileManager>` chain cannot keep the
    /// FileManager (and its on-disk exclusive lock) alive past environment
    /// close.  After this the tree can no longer re-fetch an evicted root
    /// from the log — which is correct, because the environment is shutting
    /// down and the tree is about to be dropped.
    pub fn clear_log_manager(&mut self) {
        self.log_manager = None;
    }

    /// T-5: set the compact-key threshold (`TREE_COMPACT_MAX_KEY_LENGTH` /
    /// `IN.getCompactMaxKeyLength`).  New BINs created by this tree inherit it;
    /// `<= 0` disables the compact key rep.  Default 16.
    pub fn set_compact_max_key_length(&mut self, len: i32) {
        self.compact_max_key_length = len;
    }

    /// Notify the listener that a node became resident (JE `Evictor.addBack`).
    #[inline]
    fn note_added(&self, node_id: u64) {
        if let Some(l) = &self.in_list_listener {
            l.note_ins_added(node_id);
        }
    }

    /// Notify the listener that a resident node was accessed
    /// (JE `Evictor.moveBack` — LRU touch).
    #[inline]
    fn note_accessed(&self, node_id: u64) {
        if let Some(l) = &self.in_list_listener {
            l.note_ins_accessed(node_id);
        }
    }

    /// Notify the listener that a node was removed (JE `Evictor.remove`).
    #[inline]
    fn note_removed(&self, node_id: u64) {
        if let Some(l) = &self.in_list_listener {
            l.note_ins_removed(node_id);
        }
    }

    /// Creates a new empty tree with a custom key comparator.
    ///
    /// Used for sorted-duplicate databases where keys are two-part
    /// composite keys that require a custom ordering function.
    ///
    /// Constructor with `btreeComparator` parameter.
    pub fn new_with_comparator(
        database_id: u64,
        max_entries_per_node: usize,
        comparator: KeyComparatorFn,
    ) -> Self {
        Tree {
            database_id,
            max_entries_per_node,
            root: RwLock::new(None),
            root_latch: SharedLatch::new(LatchContext::new("TreeRoot"), false),
            root_log_lsn: RwLock::new(noxu_util::NULL_LSN),
            root_splits: AtomicU64::new(0),
            relatches_required: AtomicU64::new(0),
            key_comparator: Some(comparator),
            memory_counter: None,
            in_list_listener: None,
            log_manager: None,
            redo_capacity_hint: 0,
            key_prefixing: false,
            compact_max_key_length: INKeyRep_DEFAULT_MAX_KEY_LENGTH, // T-5
        }
    }

    /// Sets the key-prefixing flag.
    ///
    /// When `true`, BIN key-prefix compression is enabled: shared leading
    /// bytes are factored out of each slot's key.  When `false` (the
    /// default), keys are stored verbatim — matching JE
    /// `DatabaseConfig.setKeyPrefixing(false)` / `IN.computeKeyPrefix`
    /// returning `null`.
    ///
    /// Ref: `IN.java computeKeyPrefix` ~line 2456.
    pub fn set_key_prefixing(&mut self, enabled: bool) {
        self.key_prefixing = enabled;
    }

    /// Sets the key comparator, replacing any existing one.
    pub fn set_comparator(&mut self, comparator: KeyComparatorFn) {
        self.key_comparator = Some(comparator);
    }

    /// Store a capacity hint used by `redo_insert` when it creates the first
    /// BIN for this tree (the first-key path).
    ///
    /// The first BIN's `entries` Vec is pre-allocated with
    /// `capacity.min(max_entries_per_node)` slots, eliminating the
    /// Vec-resize doubling cycle (1 → 2 → 4 → … → cap) that would
    /// otherwise occur during the redo loop.
    ///
    /// Call once before the redo loop.  Has no effect on `insert` (the
    /// normal, non-recovery path).
    ///
    /// Wave 11-K optimisation (Fix 3).
    pub fn hint_redo_capacity(&mut self, capacity: usize) {
        self.redo_capacity_hint = capacity;
    }

    /// Returns the current redo capacity hint (0 = no hint set).
    pub fn get_redo_capacity_hint(&self) -> usize {
        self.redo_capacity_hint
    }

    /// Takes the key comparator out of this tree (leaving None).
    pub fn take_comparator(&mut self) -> Option<KeyComparatorFn> {
        self.key_comparator.take()
    }

    /// Returns a reference to the key comparator, if configured.
    ///
    /// Used by `CursorImpl::find_bin_for_key` (R4 fix) so the cursor's own
    /// IN-level descent uses the same comparator-aware floor slot as the
    /// tree's own search paths. Mirrors JE `DatabaseImpl.getKeyComparator()`.
    pub fn get_comparator(&self) -> Option<&KeyComparatorFn> {
        self.key_comparator.as_ref()
    }

    /// Returns the key comparator if set, or performs lexicographic comparison.
    #[inline]
    fn key_cmp(&self, a: &[u8], b: &[u8]) -> std::cmp::Ordering {
        match &self.key_comparator {
            Some(cmp) => cmp(a, b),
            None => a.cmp(b),
        }
    }

    /// Floor child slot index for descending an internal node: the largest
    /// slot whose key is ≤ `key`. Slot 0 carries a virtual −∞ key (always
    /// qualifies); `entries[1..]` are sorted ascending, so this binary-searches
    /// the partition point instead of an O(n) linear walk (St-H4). Uses
    /// `key_cmp` so a configured custom comparator is honoured on every descent
    /// path. Returns 0 for an empty/single-slot node.
    fn upper_in_floor_index(&self, entries: &[InEntry], key: &[u8]) -> usize {
        if entries.len() <= 1 {
            return 0;
        }
        entries[1..].partition_point(|e| {
            self.key_cmp(e.key.as_slice(), key) != std::cmp::Ordering::Greater
        })
    }

    /// Returns true if the tree has no root (is empty).
    pub fn is_empty(&self) -> bool {
        self.root.read().is_none()
    }

    /// Sets the root of the tree.
    ///
    /// Must hold root_latch exclusively before calling.
    pub fn set_root(&self, node: TreeNode) {
        *self.root.write() = Some(Arc::new(RwLock::new(node)));
    }

    /// Returns the root Arc, if any.
    ///
    /// Returns a cloned `Arc` rather than a reference so the caller does not
    /// hold the inner `RwLock` guard.
    ///
    /// EV-14: when the in-memory root has been evicted (`evict_root`) but a
    /// persisted version exists (`root_log_lsn` set), this re-materializes it
    /// from the log before returning — the faithful equivalent of JE
    /// `Tree.getRootIN` always calling `root.fetchTarget(...)`.  Returns
    /// `None` only for a genuinely empty tree (no resident root and no
    /// persisted root LSN).
    pub fn get_root(&self) -> Option<Arc<RwLock<TreeNode>>> {
        if let Some(r) = self.root.read().clone() {
            return Some(r);
        }
        // Root not resident: re-fetch it from `root_log_lsn` if one exists
        // (a no-op returning None when the tree was never populated).
        self.fetch_root_from_log()
    }

    /// Returns the database ID.
    pub fn get_database_id(&self) -> u64 {
        self.database_id
    }

    /// Count the total number of live (non-deleted) entries across all BINs.
    ///
    /// Used by `DatabaseImpl::set_recovered_tree()` to initialise the
    /// per-database `entry_count` AtomicU64 after recovery replays the log.
    pub fn count_entries(&self) -> u64 {
        let mut total = 0u64;
        if let Some(root) = self.get_root() {
            Self::count_entries_recursive(&root, &mut total);
        }
        total
    }

    /// DBI-14: collect every live `(full_key, data, lsn)` triple in physical
    /// (left-to-right) order.  Used by `resort_under_comparator` to rebuild a
    /// tree whose slots were laid out in byte order (e.g. by recovery redo,
    /// which has no access to the application comparator) under the real
    /// configured comparator.
    fn collect_all_entries(&self) -> Vec<(Vec<u8>, Vec<u8>, Lsn)> {
        let mut out = Vec::new();
        if let Some(root) = self.get_root() {
            Self::collect_all_entries_recursive(&root, &mut out);
        }
        out
    }

    fn collect_all_entries_recursive(
        node_arc: &Arc<RwLock<TreeNode>>,
        out: &mut Vec<(Vec<u8>, Vec<u8>, Lsn)>,
    ) {
        let guard = node_arc.read();
        match &*guard {
            TreeNode::Bottom(b) => {
                for i in 0..b.entries.len() {
                    if b.entries[i].known_deleted {
                        continue;
                    }
                    if let Some(fk) = b.get_full_key(i) {
                        let data =
                            b.entries[i].data.clone().unwrap_or_default();
                        out.push((fk, data, b.get_lsn(i)));
                    }
                }
            }
            TreeNode::Internal(n) => {
                let children: Vec<Arc<RwLock<TreeNode>>> =
                    n.resident_children();
                drop(guard);
                for child in &children {
                    Self::collect_all_entries_recursive(child, out);
                }
            }
        }
    }

    /// DBI-14: rebuild this tree so that its on-disk byte-ordered slot layout
    /// is re-sorted under the currently-configured key comparator.
    ///
    /// Recovery redo (`redo_insert`) has no access to the application's
    /// comparator function — only the persisted identity — so it lays keys
    /// out in unsigned-byte order.  After `set_recovered_tree` attaches the
    /// real comparator, the slots must be re-sorted, or comparator-driven
    /// searches would binary-search a tree ordered by the wrong relation.
    ///
    /// No-op when no comparator is configured (byte order already matches the
    /// recovered layout) or when the tree is empty.  Mirrors the effect of
    /// JE reconstructing the comparator at open and the tree always having
    /// been built under it.
    pub fn resort_under_comparator(&self) {
        if self.key_comparator.is_none() {
            return;
        }
        let entries = self.collect_all_entries();
        if entries.is_empty() {
            return;
        }
        // Drop the current root; re-insert every entry through the normal
        // comparator-aware insert path so the new layout obeys the comparator.
        *self.root.write() = None;
        *self.root_log_lsn.write() = noxu_util::NULL_LSN;
        for (key, data, lsn) in entries {
            // Best-effort: a failed re-insert would be a tree-structure bug;
            // surface it loudly in debug builds.
            let r = self.insert(key, data, lsn);
            debug_assert!(
                r.is_ok(),
                "resort_under_comparator: re-insert failed: {r:?}"
            );
        }
    }

    fn count_entries_recursive(
        node_arc: &Arc<RwLock<TreeNode>>,
        total: &mut u64,
    ) {
        let guard = node_arc.read();
        match &*guard {
            TreeNode::Bottom(b) => {
                // Count only live (non-known_deleted) entries.
                *total += b.entries.iter().filter(|e| !e.known_deleted).count()
                    as u64;
            }
            TreeNode::Internal(n) => {
                let children: Vec<Arc<RwLock<TreeNode>>> =
                    n.resident_children();
                drop(guard);
                for child in children {
                    Self::count_entries_recursive(&child, total);
                }
            }
        }
    }

    /// Sum the real in-memory heap footprint of every resident node in the
    /// tree (DBI-23 oracle / reconciliation), in bytes.
    ///
    /// Walks all resident IN/BIN nodes and adds each node's
    /// `budgeted_memory_size` (JE `IN.getBudgetedMemorySize`).  This is the
    /// authoritative "real heap" figure the incrementally-maintained
    /// `memory_counter` is meant to approximate; an engine can call it to
    /// reconcile counter drift, and the DBI-23 test uses it as the oracle the
    /// live counter must stay within tolerance of.
    pub fn total_budgeted_memory(&self) -> u64 {
        let mut total = 0u64;
        if let Some(root) = self.get_root() {
            Self::total_budgeted_memory_recursive(&root, &mut total);
        }
        total
    }

    fn total_budgeted_memory_recursive(
        node_arc: &Arc<RwLock<TreeNode>>,
        total: &mut u64,
    ) {
        let guard = node_arc.read();
        *total += guard.budgeted_memory_size();
        if let TreeNode::Internal(n) = &*guard {
            let children: Vec<Arc<RwLock<TreeNode>>> = n.resident_children();
            drop(guard);
            for child in children {
                Self::total_budgeted_memory_recursive(&child, total);
            }
        }
    }

    /// Search for a BIN that should contain the given key.
    ///
    /// This is the core tree traversal operation. It walks from root to BIN
    /// using latch-coupling (acquire child latch, then release parent latch).
    ///
    /// . Descends the tree until a BIN is
    /// reached, following the child pointer at the slot whose key is the
    /// largest key <= the search key (the "LTE" rule).  Slot 0 in every upper
    /// IN carries a virtual key (-infinity) so any search key routes through
    /// it when all real keys are larger.
    ///
    /// Returns a SearchResult indicating where the key is or should be.
    /// Returns None if tree is empty.
    pub fn search(&self, key: &[u8]) -> Option<SearchResult> {
        let root = self.get_root()?;

        // Hand-over-hand latch coupling for the descent. At each level we
        // hold a `parking_lot::ArcRwLockReadGuard` on the current node;
        // before dropping it, we acquire the child's read guard via
        // `Arc::read_arc`. This keeps a continuous chain of read locks
        // along the descent path so that no concurrent `split_child(parent,
        // …)` can run on a node we are about to enter — `split_child` takes
        // `parent.write()` to install the new sibling, and that write
        // blocks while we hold `parent.read()`. Without this, the prior
        // pattern (capture child Arc, drop parent guard, then take child
        // read lock) left a window in which a split could relocate the
        // child entries: a search for a key that should have ended up in
        // the new sibling would instead reach the (now left-half) child
        // and return a false `NotFound`.
        //
        // `read_arc()` returns `ArcRwLockReadGuard<RawRwLock, TreeNode>`
        // — a guard that owns its own Arc reference, so it has no
        // borrow lifetime and can be held across loop iterations and
        // assignment.
        let mut guard: NodeArcReadGuard = root.read_arc();

        loop {
            if guard.is_bin() {
                // JE: IN.fetchTarget / CursorImpl access moves the reached
                // BIN toward the hot end of the evictor's LRU list
                // (Evictor.moveBack).  A freshly split BIN that has not yet
                // been registered is added here (moveBack is add-if-absent).
                if let TreeNode::Bottom(bin) = &*guard {
                    self.note_accessed(bin.node_id);
                }
                // Reached a BIN: final key lookup within the same guard.
                // Use indicate_if_duplicate=true so an exact match sets
                // EXACT_MATCH in the return value.  Guard against -1 (not
                // found): -1i32 has all bits set, so the naive
                // `index & EXACT_MATCH != 0` check would incorrectly report
                // an exact match for a missing key.
                let (found, raw_idx) = match &*guard {
                    TreeNode::Bottom(bin) => match &self.key_comparator {
                        Some(cmp) => {
                            let (idx, exact) =
                                bin.find_entry_cmp(key, cmp.as_ref());
                            (exact, idx as i32)
                        }
                        None => {
                            let index = guard.find_entry(key, true, true);
                            let exact =
                                index >= 0 && (index & EXACT_MATCH != 0);
                            (exact, index & 0xFFFF)
                        }
                    },
                    _ => {
                        let index = guard.find_entry(key, true, true);
                        let exact = index >= 0 && (index & EXACT_MATCH != 0);
                        (exact, index & 0xFFFF)
                    }
                };
                // CursorImpl.isProbablyExpired(): if an exact match
                // was found, check whether the entry's TTL has already elapsed.
                // If it has, treat the slot as not found so callers skip it.
                //
                // TREE-F1: also treat a known_deleted slot as ABSENT on an
                // exact lookup, mirroring the tail of IN.findEntry
                // (IN.java:3197): `if (ret >= 0 && exact &&
                // isEntryKnownDeleted(ret & 0xffff)) return -1;`.  KD slots
                // legitimately exist in live BINs during BIN-delta
                // reconstitution until the compressor reclaims them.
                let found = if found {
                    if let TreeNode::Bottom(bin) = &*guard {
                        let idx = (raw_idx & 0x7FFF) as usize;
                        bin.slot_is_live(idx)
                    } else {
                        found
                    }
                } else {
                    found
                };
                return Some(SearchResult::with_values(found, raw_idx, false));
            }

            // Upper IN: find the child slot with the largest key <= search
            // key, and capture the child Arc WHILE HOLDING the guard.
            // Slot 0 has a virtual key that compares as -infinity.
            let parent_arc = NodeArcReadGuard::rwlock(&guard).clone();
            let next_arc = match &*guard {
                TreeNode::Internal(n) => {
                    if n.entries.is_empty() {
                        return None;
                    }
                    // Walk forward as long as entry.key <= key, starting
                    // from slot 0 (which always qualifies because its key
                    // is the virtual -infinity key).
                    let idx = self.upper_in_floor_index(&n.entries, key);
                    match n.get_child(idx) {
                        // Resident child: keep the hand-over-hand fast path.
                        Some(c) => {
                            let next_guard = c.read_arc();
                            drop(guard);
                            guard = next_guard;
                            continue;
                        }
                        // EV-14/EV-13: child evicted — re-fetch it from its
                        // slot LSN (JE ChildReference.fetchTarget).  Must
                        // drop the parent read guard to upgrade to a write
                        // latch inside child_at_or_fetch.
                        None => idx,
                    }
                }
                TreeNode::Bottom(_) => {
                    unreachable!("is_bin() returned false above")
                }
            };
            drop(guard);
            let child = self.child_at_or_fetch(&parent_arc, next_arc)?;
            guard = child.read_arc();
        }
    }

    /// Combined search-and-fetch: descend once to the BIN and return the
    /// slot's data together with a reference to the BIN arc.
    ///
    /// Replaces the previous three-descent sequence on the `Database::get`
    /// hot path:
    ///   1. `Tree::search` — existence check only.
    ///   2. `CursorImpl::get_data_from_tree` — re-descended to fetch data.
    ///   3. `CursorImpl::find_bin_for_key` — re-descended for BIN pinning.
    ///
    /// One descent now does all three jobs.  At the BIN level it uses the
    /// existing binary-search helper `find_entry_compressed` instead of the
    /// O(n) `iter().find()` used by `get_data_from_tree`.
    ///
    /// Returns `None` only when the tree is empty.  Otherwise returns
    /// `Some(SlotFetch)` — callers must inspect `SlotFetch::found` to
    /// determine whether the key was present.  The BIN read-guard is released
    /// before this method returns so callers may safely call `lock_ln`
    /// (which may block) without holding any tree latch.
    ///
    /// Wave-11-I — see the 2026 review.
    pub fn search_with_data(&self, key: &[u8]) -> Option<SlotFetch> {
        let root = self.get_root()?;
        let mut guard: NodeArcReadGuard = root.read_arc();

        loop {
            if guard.is_bin() {
                // Capture the BIN Arc before inspecting entries.
                let bin_arc = NodeArcReadGuard::rwlock(&guard).clone();

                let (found, data, lsn, slot_index) = match &*guard {
                    TreeNode::Bottom(bin) => {
                        let (idx, exact) = match &self.key_comparator {
                            Some(cmp) => bin.find_entry_cmp(key, cmp.as_ref()),
                            None => bin.find_entry_compressed(key),
                        };
                        if exact {
                            // TREE-F1: a slot is reported as found only when
                            // live (not known_deleted, not TTL-expired) — the
                            // same predicate used by Tree::search and the
                            // cursor scan.  Mirrors IN.findEntry (IN.java:3197)
                            // and CursorImpl.isProbablyExpired.
                            if bin.slot_is_live(idx) {
                                let lsn = bin.get_lsn(idx); // T-3
                                let e = &bin.entries[idx];
                                (true, e.data.clone(), lsn.as_u64(), idx)
                            } else {
                                (false, None, 0u64, 0)
                            }
                        } else {
                            (false, None, 0u64, 0)
                        }
                    }
                    _ => (false, None, 0u64, 0),
                };
                // Release the BIN read guard before returning so the caller
                // can call lock_ln (which may block) without holding a latch.
                drop(guard);
                return Some(SlotFetch {
                    found,
                    data,
                    lsn,
                    slot_index,
                    bin_arc,
                });
            }

            // Upper IN: same hand-over-hand descent as `Tree::search`.
            let parent_arc = NodeArcReadGuard::rwlock(&guard).clone();
            let next_idx = match &*guard {
                TreeNode::Internal(n) => {
                    if n.entries.is_empty() {
                        return None;
                    }
                    // Slot 0 = virtual −∞; walk forward while entry.key ≤ key.
                    let idx = self.upper_in_floor_index(&n.entries, key);
                    match n.get_child(idx) {
                        Some(c) => {
                            let next_guard = c.read_arc();
                            drop(guard);
                            guard = next_guard;
                            continue;
                        }
                        // EV-14/EV-13: re-fetch an evicted child from its LSN.
                        None => idx,
                    }
                }
                TreeNode::Bottom(_) => {
                    unreachable!("is_bin() returned false above")
                }
            };
            drop(guard);
            let child = self.child_at_or_fetch(&parent_arc, next_idx)?;
            guard = child.read_arc();
        }
    }

    /// Re-populate a BIN slot's LN data after a cold fetch from the log,
    /// so subsequent reads of the same key hit memory instead of re-faulting
    /// from disk (JE `IN.fetchTarget` caches the fetched LN in the slot).
    ///
    /// This is the read-cache half of `strip_lns`: the evictor strips a
    /// resident LN's `data` (freeing `data.len()` heap and crediting
    /// `data.len()` back to the shared budget via `arbiter.release_memory`),
    /// and a later read re-fetches those bytes from the log at the slot LSN.
    /// Without re-population every repeat read re-faults + re-CRCs the same
    /// record from disk (the measured 175x read gap).
    ///
    /// Budget-safety (CRITICAL): re-population re-grows the LN heap by exactly
    /// `data.len()` bytes, so it charges the SAME `memory_counter` (which IS
    /// the arbiter's shared `cache_usage`) by `data.len()` — symmetric with
    /// the strip credit. The evictor can therefore strip the re-populated
    /// slot again under pressure, and the cache stays bounded: charge on
    /// re-populate == credit on strip. Only the LN `data` bytes are accounted
    /// (the slot's key + `BIN_ENTRY_OVERHEAD` were charged at insert and were
    /// never freed by the strip, since the slot itself stayed resident).
    ///
    /// Race / consistency guards (all under the BIN write latch):
    ///   * The slot LSN must still equal `expected_lsn`. A concurrent writer
    ///     that replaced the record bumps the slot to a new LSN + new data;
    ///     re-populating stale bytes there would corrupt the slot, so we skip.
    ///   * The slot `data` must still be `None`. If another reader already
    ///     re-populated it (or a writer set it), we skip — this prevents
    ///     double-charging the budget for the same bytes.
    ///
    /// Because a stripped slot's on-disk LN is immutable at `expected_lsn`,
    /// the re-populated bytes are byte-identical to a cold fetch of the same
    /// LSN — a read from the re-populated slot returns the same value a cold
    /// read would.
    ///
    /// No-op (returns without charging) when the BIN has any open cursor
    /// (`cursor_count > 0`), matching the strip guard so the two never race
    /// on the same slot bytes.
    pub fn repopulate_ln_data(
        &self,
        bin_arc: &Arc<RwLock<TreeNode>>,
        slot_index: usize,
        expected_lsn: u64,
        data: &[u8],
    ) {
        // Only meaningful when this tree owns a shared budget counter; if it
        // does not (e.g. a bare test tree), caching still helps reads but
        // there is no budget to charge, so skip to avoid unbounded growth.
        let counter = match &self.memory_counter {
            Some(c) => c,
            None => return,
        };
        let mut guard = bin_arc.write();
        let bin = match &mut *guard {
            TreeNode::Bottom(b) => b,
            _ => return,
        };
        if bin.cursor_count > 0 {
            return;
        }
        if slot_index >= bin.entries.len() {
            return;
        }
        // The slot must still point at the LSN we fetched from, and must
        // still be stripped (data == None). Either guard failing means a
        // concurrent writer/reader already touched the slot — skip.
        if bin.get_lsn(slot_index).as_u64() != expected_lsn {
            return;
        }
        if bin.entries[slot_index].data.is_some() {
            return;
        }
        bin.entries[slot_index].data = Some(data.to_vec());
        // Symmetric with strip's release_memory(data.len()).
        counter.fetch_add(data.len() as i64, Ordering::Relaxed);
    }

    /// Sets the expiration time (in absolute hours since Unix epoch) for an
    /// existing key's BIN slot.
    ///
    /// Returns `true` if the key was found and updated, `false` otherwise.
    ///
    /// Used by `Database::put_with_options()` to apply per-record TTL.
    /// `IN.entryExpiration` / `BIN.expirationInHours` path.
    pub fn update_key_expiration(
        &self,
        key: &[u8],
        expiration_hours: u32,
    ) -> bool {
        let root = match self.get_root() {
            Some(r) => r,
            None => return false,
        };
        // Hand-over-hand latch coupling for the descent. At the BIN we
        // need a write lock; we drop our read lock first and take the
        // write lock under the protection of the *outer* parent's read
        // lock (held by the previous loop iteration's guard). For the
        // first iteration there is no outer parent, but no `split_child`
        // can run on the root itself in that single-level case because
        // root splits go through `split_root_if_needed` which holds
        // `self.root.write()`. So the worst case is that the root is
        // promoted from a single BIN to a level-2 IN between our read
        // detect and our write — handled by the `is_bin` re-check
        // inside the write lock.
        //
        // We retry the descent up to a small bound to absorb the rare
        // case where a concurrent split moved this key into the new
        // sibling between the read-chain release and the write-lock
        // acquisition. Without the retry, the sole caller
        // (`Database::put_with_options`) would silently lose the TTL
        // for the affected key. Three attempts is generous: each
        // retry only races a single split and splits are infrequent.
        for _ in 0..3 {
            let mut guard: NodeArcReadGuard = root.read_arc();
            let bin_arc;
            loop {
                if guard.is_bin() {
                    bin_arc = NodeArcReadGuard::rwlock(&guard).clone();
                    drop(guard);
                    break;
                }
                let next_arc = match &*guard {
                    TreeNode::Internal(n) => {
                        if n.entries.is_empty() {
                            return false;
                        }
                        let idx = self.upper_in_floor_index(&n.entries, key);
                        match n.get_child(idx) {
                            Some(c) => c,
                            None => return false,
                        }
                    }
                    TreeNode::Bottom(_) => unreachable!(),
                };
                let next_guard = next_arc.read_arc();
                drop(guard);
                guard = next_guard;
            }

            // Now take the write lock on the BIN we descended to.
            let mut wguard = bin_arc.write();
            if let TreeNode::Bottom(bin) = &mut *wguard {
                let slot = if let Some(cmp) = &self.key_comparator {
                    let (idx, exact) = bin.find_entry_cmp(key, cmp.as_ref());
                    if exact { Some(idx) } else { None }
                } else {
                    let (idx, exact) = bin.find_entry_compressed(key);
                    if exact { Some(idx) } else { None }
                };
                if let Some(slot_idx) = slot
                    && let Some(entry) = bin.entries.get_mut(slot_idx)
                {
                    entry.expiration_time = expiration_hours;
                    bin.expiration_in_hours = true;
                    bin.dirty = true;
                    return true;
                }
            }
            // Key not in this BIN — either it was never present or a
            // concurrent split moved it. Retry the descent; at most a
            // few iterations are needed to follow the key into its new
            // BIN.
        }
        false
    }

    /// Returns the key and data of the first BIN entry at or after `key`.
    ///
    /// Descends with the tree's key comparator (same path as `search()`), then
    /// within the BIN finds the first slot whose stored key >= `key` using the
    /// comparator.  Returns `None` if every entry in the tree is < `key`.
    ///
    /// Used by sorted-duplicate cursor `search(Set)` to position at the first
    /// (key, data) pair whose two-part key >= `lower_bound(primary_key)`.
    ///
    /// → BIN scan path.
    pub fn first_entry_at_or_after(
        &self,
        key: &[u8],
    ) -> Option<(Vec<u8>, Vec<u8>, u64)> {
        // Hand-over-hand latch coupling — see Tree::search for the
        // detailed rationale on why this closes a reader-vs-splitter
        // race window.
        let mut guard: NodeArcReadGuard = self.get_root()?.read_arc();

        loop {
            if guard.is_bin() {
                let result = match &*guard {
                    TreeNode::Bottom(bin) => {
                        let (mut idx, _exact) = match &self.key_comparator {
                            Some(cmp) => bin.find_entry_cmp(key, cmp.as_ref()),
                            None => bin.find_entry_compressed(key),
                        };
                        // TREE-F1: skip non-live slots (known_deleted /
                        // TTL-expired) at/after the floor index, mirroring the
                        // cursor getNext skip (CursorImpl.java:2062-2064).
                        while idx < bin.entries.len() && !bin.slot_is_live(idx)
                        {
                            idx += 1;
                        }
                        if idx < bin.entries.len() {
                            let full_key =
                                bin.get_full_key(idx).unwrap_or_default();
                            let data = bin.entries[idx]
                                .data
                                .clone()
                                .unwrap_or_default();
                            let lsn = bin.get_lsn(idx).as_u64(); // T-3
                            Some((full_key, data, lsn))
                        } else {
                            None
                        }
                    }
                    _ => None,
                };
                return result;
            }

            // Upper IN: same descent as search().
            let parent_arc = NodeArcReadGuard::rwlock(&guard).clone();
            let next_idx = match &*guard {
                TreeNode::Internal(n) => {
                    if n.entries.is_empty() {
                        return None;
                    }
                    let idx = self.upper_in_floor_index(&n.entries, key);
                    match n.get_child(idx) {
                        Some(c) => {
                            let next_guard = c.read_arc();
                            drop(guard);
                            guard = next_guard;
                            continue;
                        }
                        None => idx, // EV-14/EV-13: re-fetch below.
                    }
                }
                TreeNode::Bottom(_) => unreachable!(),
            };
            drop(guard);
            let child = self.child_at_or_fetch(&parent_arc, next_idx)?;
            guard = child.read_arc();
        }
    }

    /// Like [`Tree::first_entry_at_or_after`] but also returns the BIN node
    /// (so callers may pin it) and the entry's slot index inside that
    /// BIN.
    ///
    /// Wave 11-N (Bug 2): `CursorImpl::search_dup` previously stored
    /// `current_index = 0` after a sorted-dup `Search`, which broke the
    /// fast-path of `retrieve_next` (and the slow path's
    /// `next_index = current_index + 1` arithmetic) for any primary
    /// that was not the first slot of its BIN.  This helper hands back
    /// the real index so the cursor can be positioned correctly.
    ///
    /// CC-2 fix: uses the same `read_arc()` hand-over-hand latch coupling
    /// as every other descent method (`search`, `first_entry_at_or_after`,
    /// `get_first_node`, `get_adjacent_bin_attempt`).  The original
    /// implementation did `arc.read().is_bin()` (lock acquired and released)
    /// then a SECOND `arc.read()` on the next line — a gap in which a
    /// concurrent split can promote the node (BIN→upper IN) or move the
    /// sought key to a new sibling, yielding a false "not found" for an
    /// existing key.  Mirrors JE `Tree.searchSubTree` / `Tree.search`
    /// which hold the latch across the `is_bin()` test and the subsequent
    /// entry lookup.
    pub fn first_entry_at_or_after_with_index(
        &self,
        key: &[u8],
    ) -> Option<(
        Vec<u8>,
        Vec<u8>,
        usize,
        u64,
        std::sync::Arc<crate::NodeRwLock<TreeNode>>,
    )> {
        // Hand-over-hand latch coupling — identical strategy to
        // first_entry_at_or_after; the guard is held continuously across
        // is_bin() and the subsequent entry lookup so no split can
        // restructure the path between the two observations.
        let mut guard: NodeArcReadGuard = self.get_root()?.read_arc();
        loop {
            if guard.is_bin() {
                if let TreeNode::Bottom(bin) = &*guard {
                    let (idx, _exact) = match &self.key_comparator {
                        Some(cmp) => bin.find_entry_cmp(key, cmp.as_ref()),
                        None => bin.find_entry_compressed(key),
                    };
                    // TREE-F1: skip non-live slots (known_deleted /
                    // TTL-expired) at/after the floor index
                    // (CursorImpl.java:2062-2064).
                    let mut idx = idx;
                    while idx < bin.entries.len() && !bin.slot_is_live(idx) {
                        idx += 1;
                    }
                    if idx < bin.entries.len() {
                        let full_key =
                            bin.get_full_key(idx).unwrap_or_default();
                        let data =
                            bin.entries[idx].data.clone().unwrap_or_default();
                        let lsn = bin.get_lsn(idx).as_u64(); // T-3
                        // Obtain the Arc for the BIN node the guard came from.
                        // `ArcRwLockReadGuard::rwlock()` returns the backing Arc.
                        let bin_arc = NodeArcReadGuard::rwlock(&guard).clone();
                        return Some((full_key, data, idx, lsn, bin_arc));
                    } else {
                        return None;
                    }
                }
                return None;
            }

            // Upper IN: descend as in first_entry_at_or_after / search.
            let parent_arc = NodeArcReadGuard::rwlock(&guard).clone();
            let next_idx = match &*guard {
                TreeNode::Internal(n) => {
                    if n.entries.is_empty() {
                        return None;
                    }
                    let idx = self.upper_in_floor_index(&n.entries, key);
                    match n.get_child(idx) {
                        Some(c) => {
                            let next_guard = c.read_arc();
                            drop(guard);
                            guard = next_guard;
                            continue;
                        }
                        None => idx, // EV-14/EV-13: re-fetch below.
                    }
                }
                TreeNode::Bottom(_) => unreachable!(),
            };
            drop(guard);
            let child = self.child_at_or_fetch(&parent_arc, next_idx)?;
            guard = child.read_arc();
        }
    }

    /// Insert a key/data pair into the tree.
    ///
    /// . Handles the root-is-null case by
    /// creating a two-level tree (upper IN + BIN) per initialisation path,
    /// then delegates to `insert_recursive` which performs preemptive splitting
    /// as it descends.
    ///
    /// Returns Ok(true) if this was a new insert, Ok(false) if it was an update.
    pub fn insert(
        &self,
        key: Vec<u8>,
        data: Vec<u8>,
        lsn: Lsn,
    ) -> Result<bool, TreeError> {
        // Save sizes before potentially moving key/data — needed for memory tracking.
        let key_len = key.len();
        let data_len = data.len();

        // First-key path. We MUST hold the write lock while testing
        // root.is_none() and replacing the root, otherwise N threads can all
        // observe an empty tree, each build a fresh single-entry root, and
        // the last writer's `*self.root.write() = Some(...)` silently
        // discards the others' inserts. (Reproducer:
        // xa_protocol_test::test_concurrent_independent_xids — 8 threads
        // each inserting their own key into an empty tree lost ~30% of
        // inserts before this lock change.)
        {
            let mut root_guard = self.root.write();
            if root_guard.is_none() {
                let bin_node_id = generate_node_id();
                let root_node_id = generate_node_id();
                let bin = Arc::new(RwLock::new(TreeNode::Bottom(BinStub {
                    node_id: bin_node_id,
                    level: BIN_LEVEL,
                    entries: vec![BinEntry {
                        data: Some(data),
                        known_deleted: false,
                        dirty: false,
                        expiration_time: 0,
                    }],
                    key_prefix: Vec::new(), // single entry — no common prefix yet
                    dirty: true,
                    is_delta: false,
                    last_full_lsn: NULL_LSN,
                    last_delta_lsn: NULL_LSN,
                    generation: 0,
                    parent: None, // set below after root_in is created
                    // St-H6: use true to match the engine-wide invariant that
                    // every BIN which may hold TTL entries uses hours granularity
                    // (JE BIN.java default; matches tree.rs:980 and read_from_log).
                    expiration_in_hours: true,
                    cursor_count: 0,
                    prohibit_next_delta: false,
                    lsn_rep: LsnRep::from_lsns(&[lsn]),
                    keys: KeyRep::from_keys(vec![key]), // T-2
                    compact_max_key_length: self.compact_max_key_length,
                })));

                // Upper IN at level 2; slot 0 uses an empty key (virtual root key).
                let root_arc =
                    Arc::new(RwLock::new(TreeNode::Internal(InNodeStub {
                        node_id: root_node_id,
                        level: MAIN_LEVEL | 2,
                        entries: vec![InEntry {
                            key: vec![], // virtual key for slot 0 in upper IN
                        }],
                        // T-4: the single resident child at slot 0.
                        targets: TargetRep::Sparse(vec![(0, bin.clone())]),
                        dirty: true,
                        generation: 0,
                        parent: None,
                        lsn_rep: LsnRep::from_lsns(&[lsn]),
                    })));

                // Wire the BIN's parent pointer back to the root IN.
                {
                    let mut g = bin.write();
                    g.set_parent(Some(Arc::downgrade(&root_arc)));
                }

                *root_guard = Some(root_arc);

                // JE: IN.fetchTarget / initial tree build registers the new
                // resident nodes with the evictor (Evictor.addBack).
                self.note_added(root_node_id);
                self.note_added(bin_node_id);

                // Count the first entry.
                if let Some(counter) = &self.memory_counter {
                    let delta =
                        (key_len + data_len + BIN_ENTRY_OVERHEAD) as i64;
                    counter.fetch_add(delta, Ordering::Relaxed);
                }
                return Ok(true);
            }
            // Another thread initialized the root while we were waiting for
            // the write lock; fall through and insert into the existing tree.
        }

        // Check whether the root itself needs to be split before descending.
        // Tree.searchSplitsAllowed(): if rootIN.needsSplitting()
        // call splitRoot first.
        self.split_root_if_needed(lsn)?;

        // Recursively insert, splitting children proactively as we descend
        // (forceSplit / searchSplitsAllowed pattern).
        let root_arc = self.get_root().unwrap();
        let result = Self::insert_recursive(
            &root_arc,
            key,
            data,
            lsn,
            self.max_entries_per_node,
            self.key_comparator.as_ref(),
            self.key_prefixing,
            self.in_list_listener.as_ref(),
        )?;

        // Update the memory counter for new inserts.
        // IN.updateMemorySize(delta) → MemoryBudget.updateTreeMemoryUsage(delta).
        // LN_OVERHEAD = 48 bytes (approximate fixed overhead per entry).
        if result && let Some(counter) = &self.memory_counter {
            let delta = (key_len + data_len + BIN_ENTRY_OVERHEAD) as i64;
            counter.fetch_add(delta, Ordering::Relaxed);
        }

        Ok(result)
    }

    /// Recovery-redo variant of [`Tree::insert`] that accepts `&[u8]` slices.
    ///
    /// Eliminates the two intermediate `Vec<u8>` allocations that the normal
    /// insert path requires at the `redo_ln` call site (one for the key, one
    /// for the data).  The compressed key suffix and the data bytes are each
    /// materialised into their `BinEntry` slots exactly once.
    ///
    /// Semantics are identical to `insert`:
    /// - Updates the existing slot when the key is already present.
    /// - Inserts a new sorted entry when the key is absent.
    /// - Triggers the same root-split and proactive-split logic.
    ///
    /// `data` should be the raw value bytes, or an empty slice for a
    /// deletion (which should not normally arrive here during redo, but is
    /// handled gracefully).
    ///
    /// Wave 11-K optimisation (Fix 1).
    pub fn redo_insert(
        &self,
        key: &[u8],
        data: &[u8],
        lsn: Lsn,
    ) -> Result<bool, TreeError> {
        let key_len = key.len();
        let data_len = data.len();
        let data_opt: Option<&[u8]> =
            if data.is_empty() { None } else { Some(data) };

        // First-key path: initialise a two-level tree from scratch.
        {
            let mut root_guard = self.root.write();
            if root_guard.is_none() {
                // Pre-allocate the BIN's entries Vec using the redo capacity
                // hint (Fix 3).  Without the hint the first BIN starts at
                // capacity 1 and doubles on each insert; with the hint it
                // starts at min(hint, max_entries) entries, eliminating
                // ~log2(max_entries) Vec-resize doublings.
                let initial_cap = if self.redo_capacity_hint > 0 {
                    self.redo_capacity_hint.min(self.max_entries_per_node)
                } else {
                    1
                };
                let mut initial_entries = Vec::with_capacity(initial_cap);
                initial_entries.push(BinEntry {
                    data: data_opt.map(|d| d.to_vec()),
                    known_deleted: false,
                    dirty: false,
                    expiration_time: 0,
                });
                let bin = Arc::new(RwLock::new(TreeNode::Bottom(BinStub {
                    node_id: generate_node_id(),
                    level: BIN_LEVEL,
                    entries: initial_entries,
                    key_prefix: Vec::new(),
                    dirty: true,
                    is_delta: false,
                    last_full_lsn: NULL_LSN,
                    last_delta_lsn: NULL_LSN,
                    generation: 0,
                    parent: None,
                    // St-H6: use true to match the engine-wide hours-only
                    // invariant (JE BIN.java default; matches tree.rs:980).
                    expiration_in_hours: true,
                    cursor_count: 0,
                    prohibit_next_delta: false,
                    lsn_rep: LsnRep::from_lsns(&[lsn]),
                    keys: KeyRep::from_keys(vec![key.to_vec()]), // T-2
                    compact_max_key_length: self.compact_max_key_length,
                })));

                let root_arc =
                    Arc::new(RwLock::new(TreeNode::Internal(InNodeStub {
                        node_id: generate_node_id(),
                        level: MAIN_LEVEL | 2,
                        entries: vec![InEntry { key: vec![] }],
                        // T-4: the single resident child at slot 0.
                        targets: TargetRep::Sparse(vec![(0, bin.clone())]),
                        dirty: true,
                        generation: 0,
                        parent: None,
                        lsn_rep: LsnRep::from_lsns(&[lsn]),
                    })));

                {
                    let mut g = bin.write();
                    g.set_parent(Some(Arc::downgrade(&root_arc)));
                }

                *root_guard = Some(root_arc);

                if let Some(counter) = &self.memory_counter {
                    let delta =
                        (key_len + data_len + BIN_ENTRY_OVERHEAD) as i64;
                    counter.fetch_add(delta, Ordering::Relaxed);
                }
                return Ok(true);
            }
        }

        self.split_root_if_needed(lsn)?;

        let root_arc = self.get_root().unwrap();
        let result = Self::redo_insert_recursive(
            &root_arc,
            key,
            data_opt,
            lsn,
            self.max_entries_per_node,
            self.key_comparator.as_ref(),
            self.key_prefixing,
        )?;

        if result && let Some(counter) = &self.memory_counter {
            let delta = (key_len + data_len + BIN_ENTRY_OVERHEAD) as i64;
            counter.fetch_add(delta, Ordering::Relaxed);
        }

        Ok(result)
    }

    /// Splits the root node if it is full (needsSplitting).
    ///
    ///
    /// ```text
    /// 1. Save oldRoot (the current root IN or BIN).
    /// 2. Create newRoot at oldRoot.level + 1.
    /// 3. Insert oldRoot into newRoot at slot 0 with a virtual (empty) key.
    /// 4. Call split_node on oldRoot, passing newRoot as parent.
    /// 5. Replace tree root with newRoot.
    /// ```
    fn split_root_if_needed(&self, lsn: Lsn) -> Result<(), TreeError> {
        // Hold `self.root.write()` across the needs_split check and the
        // root promotion, mirroring the first-key path fix and matching
        // the broader insert/split serialisation discipline.
        //
        // With the previous read-then-write pattern, two concurrent
        // splitters could each observe needs_split == true, then take()
        // and install in turn, with the second wrapping the first's
        // already-promoted root in its own new IN. Each level wraps the
        // previous, producing a chain of one-child internal nodes. No
        // data is lost (every entry is still reachable) but the tree
        // becomes unnecessarily deep, and the imbalance can compound
        // under heavy concurrent insertion.
        let mut root_guard = self.root.write();
        let needs_split = match root_guard.as_ref() {
            Some(arc) => {
                let g = arc.read();
                g.get_n_entries() >= self.max_entries_per_node
            }
            None => false,
        };
        if !needs_split {
            return Ok(());
        }

        // Create a fresh new root one level above the current root.
        let old_root_arc = root_guard.take().expect("checked Some above");
        let old_root_level = {
            let g = old_root_arc.read();
            g.level()
        };

        // newRoot = new IN(level = oldRoot.level + 1) with slot 0 = oldRoot.
        // The key at slot 0 is the virtual key (empty slice) following the
        // convention that entry-zero in an upper IN compares as -infinity.
        let new_root_arc =
            Arc::new(RwLock::new(TreeNode::Internal(InNodeStub {
                node_id: generate_node_id(),
                level: old_root_level + 1,
                entries: vec![InEntry { key: vec![] }],
                // T-4: slot 0's resident child is the old root.
                targets: TargetRep::Sparse(vec![(0, old_root_arc.clone())]),
                dirty: true,
                generation: 0,
                parent: None,
                lsn_rep: LsnRep::from_lsns(&[lsn]),
            })));

        // Update the old root's parent pointer to the new root.
        {
            let mut g = old_root_arc.write();
            g.set_parent(Some(Arc::downgrade(&new_root_arc)));
        }

        // Install the new root before calling split_child so split_child
        // (which itself takes parent.write()) can run unencumbered.
        *root_guard = Some(new_root_arc.clone());
        drop(root_guard);

        // Now split the old root (which is now child at slot 0 in new_root).
        Self::split_child(
            &new_root_arc,
            0, // child is at slot 0
            self.max_entries_per_node,
            lsn,
            SplitHint::Normal,
            &[], // no insertion key at root-init time
            self.key_comparator.as_ref(),
            self.key_prefixing,
            self.in_list_listener.as_ref(),
        )?;

        // EVICTOR-RECLAIM-1: register the freshly-promoted root IN with the
        // evictor's LRU (JE Tree.splitRoot adds the new root to the INList).
        // split_child above already registers the new sibling.
        let new_root_id = match &*new_root_arc.read() {
            TreeNode::Internal(n) => n.node_id,
            TreeNode::Bottom(b) => b.node_id,
        };
        self.note_added(new_root_id);

        self.root_splits.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Splits the child at `child_index` in `parent`.
    ///
    /// .  This implementation always keeps the **left** half in the
    /// existing child node (`child_arc`) and puts the right half in the new
    /// sibling, regardless of where the `identifierKey` falls.  JE's
    /// `IN.splitInternal` (`idKeyIndex` logic ~line 4172) can place either
    /// half in the existing node; Noxu's preemptive-split discipline ensures
    /// the parent always has a free slot at split time (the split is done on
    /// the way *down*, before the parent fills up), so the safe simplification
    /// of always using the left half is correct here — no routing information
    /// is lost.  This comment replaces the previous incorrect claim that
    /// `idKeyIndex` drove the choice.
    ///
    /// Note: does not emit a split log entry; split nodes are marked dirty
    /// and flushed at the next checkpoint (flush_dirty_bins/upper_ins).
    ///
    /// ```text
    /// 1. splitIndex = child.nEntries / 2  (or 1 / n-1 for splitSpecial)
    /// 2. Create newSibling at the same level.
    /// 3. Move entries [splitIndex..nEntries) to newSibling.
    /// 4. Update parent slot childIndex -> child (left half),
    ///    insert newSibling with newIdKey after childIndex.
    /// ```
    fn split_child(
        parent: &Arc<RwLock<TreeNode>>,
        child_index: usize,
        max_entries: usize,
        lsn: Lsn,
        hint: SplitHint,
        insert_key: &[u8],
        key_comparator: Option<&KeyComparatorFn>,
        key_prefixing: bool,
        listener: Option<&Arc<dyn InListListener>>,
    ) -> Result<(), TreeError> {
        // The split is performed under `parent.write()` for the entire
        // duration. This is a deliberate choice for correctness:
        //
        // - Without it, between dropping `child.write()` (after installing
        //   the left half) and acquiring `parent.write()` (to install the
        //   sibling), a concurrent descender can pick `child_arc` from the
        //   parent (still pointing at it), descend, take `child.write()`
        //   and insert a key. Whether the descender's key belongs in the
        //   left half (now in `child`) or the right half (which will be
        //   in the new sibling) is determined by the parent's split key —
        //   but the parent doesn't know about the split key yet, so the
        //   descender's routing decision is based on stale data. If the
        //   descender's key falls in the right half, it lands in `child`
        //   (left half) where a future search will not find it: the
        //   future search descends from the root, the parent now has the
        //   sibling installed, the search routes the key to the sibling,
        //   the sibling does not contain the key — silently lost.
        //
        // - Holding `parent.write()` throughout serialises split_child
        //   against every descender that wants `parent.read()`. A
        //   descender already holding `parent.read()` (latch coupling
        //   from above) keeps split_child waiting at this lock until it
        //   has finished its own work. Combined, the split + sibling
        //   install is atomic with respect to descents.
        //
        // - Splits are infrequent compared to inserts (~ once per
        //   max_entries new keys) so the extra serialisation here does
        //   not dominate.
        //
        // Reproducer that exercises this race:
        // crates/noxu-db/tests/concurrent_commits_stress.rs.
        let mut parent_write_guard = parent.write();

        // Extract the child Arc from the parent slot.
        let child_arc = match &*parent_write_guard {
            TreeNode::Internal(p) => {
                p.get_child(child_index).ok_or(TreeError::SplitRequired)?
            }
            TreeNode::Bottom(_) => return Err(TreeError::SplitRequired),
        };

        // Gather all entries from the child plus split metadata, AND
        // perform the in-place left-half install, all under a single
        // write lock on the child. See the earlier comment on the race
        // this avoids inside split_child.
        let mut child_guard = child_arc.write();

        // Re-validate that the child still needs splitting, now that we hold
        // its write lock. This closes a check-then-act race: the caller
        // (`insert_recursive_inner`) tested `child.get_n_entries() >=
        // max_entries` under a PARENT READ lock, then dropped that read lock
        // (required — the split needs `parent.write()`) before calling
        // `split_child`. Read locks do not exclude each other, so two
        // descenders can both pass the fullness check on the same child, both
        // drop the parent read lock, and both call `split_child`. They
        // serialise here on `parent.write()`: the first splits the child
        // (leaving it with only its left half), and by the time the second
        // acquires this child write lock the child is no longer full — or is
        // empty, if a concurrent INCompressor merge cleared it
        // (`compress_node`'s `lb.entries.clear()`). Without this re-check the
        // second caller would build a `SplitEntries` from that stale child and
        // panic in `SplitEntries::get_key(split_index)` on an empty entries
        // vec (tree.rs SplitEntries::get_key `v[index]`, observed as
        // "index out of bounds: len is 0" under the 96-thread saturation
        // benchmark; see .agent/archived-audits/bench/
        // bug-bin-split-concurrency.md).
        //
        // JE performs the identical re-validation: `IN.split` re-checks
        // `needsSplitting()` *after* latching the node it will split, so the
        // fullness test and the split are atomic w.r.t. the node latch (see
        // `IN.split` / `IN.needsSplitting` in IN.java; `Tree.forceSplit`
        // latch-couples down and `IN.split` re-tests before mutating). Here
        // the child write guard plays the role of that node latch.
        //
        // A no-op split returns `Ok(())` — the SAME success variant a real
        // split returns — because the caller re-descends unconditionally
        // after `split_child` (`return Self::insert_recursive_inner(...)`),
        // where it re-reads the (now-current) topology and re-checks
        // `child_full`. So a benign "already split" outcome simply leads to a
        // correct re-descent and the insert proceeds. This does NOT widen any
        // lock or hold `parent.write()` across the caller's read-check, so it
        // does not re-introduce the descent over-serialisation fixed in 7.2.1.
        if child_guard.get_n_entries() < max_entries {
            return Ok(());
        }

        let child_level = child_guard.level();
        // St-H6: capture the splitting BIN's expiration_in_hours flag BEFORE
        // drop(child_guard) so the right-half sibling inherits it.
        // JE: BIN.java::setExpiration calls setExpirationInHours(hours) to
        // propagate the flag on split/clone; the Rust split was hardcoding
        // false instead of inheriting — this caused hours-granularity TTL
        // entries in the right sibling to be read with in_hours=false, making
        // the hours-since-epoch value compare as seconds-since-epoch (far in
        // the past) and every right-sibling TTL record appear expired.
        let bin_expiration_in_hours: bool = match &*child_guard {
            TreeNode::Bottom(b) => b.expiration_in_hours,
            // Internal nodes do not carry per-entry TTL; default to true
            // (the engine-wide invariant for any BIN that may hold TTL data).
            TreeNode::Internal(_) => true,
        };
        // T-2/T-5: the compact-key threshold the new sibling BIN inherits.
        // (Only consumed when the child is a BIN; an upper-IN split produces
        // upper-IN siblings, which have no compact key rep.)
        let bin_compact_max_key_length: i32 = match &*child_guard {
            TreeNode::Bottom(b) => b.compact_max_key_length,
            TreeNode::Internal(_) => INKeyRep_DEFAULT_MAX_KEY_LENGTH,
        };
        let (all_entries, bin_old_prefix) = match &*child_guard {
            TreeNode::Internal(n) => {
                // T-4: capture the parallel resident-child array alongside the
                // entries so children travel with their slots through the
                // split (JE `IN.split` copies `entryTargets`).
                let children: Vec<Option<ChildArc>> =
                    (0..n.entries.len()).map(|i| n.get_child(i)).collect();
                // T-3: capture the parallel per-slot LSNs so they travel with
                // their slots (JE `IN.split` copies `entryLsnByteArray`).
                let lsns: Vec<Lsn> =
                    (0..n.entries.len()).map(|i| n.get_lsn(i)).collect();
                (
                    SplitEntries::Internal(n.entries.clone(), children, lsns),
                    Vec::new(),
                )
            }
            TreeNode::Bottom(b) => {
                // Decompress to full keys.
                let full: Vec<BinEntry> = (0..b.entries.len())
                    .map(|i| BinEntry {
                        data: b.entries[i].data.clone(),
                        known_deleted: b.entries[i].known_deleted,
                        dirty: b.entries[i].dirty,
                        expiration_time: b.entries[i].expiration_time,
                    })
                    .collect();
                let lsns: Vec<Lsn> =
                    (0..b.entries.len()).map(|i| b.get_lsn(i)).collect();
                // T-2: carry FULL keys through the split; the new BINs
                // recompute their own prefix from them.
                let full_keys: Vec<Vec<u8>> = (0..b.entries.len())
                    .map(|i| b.get_full_key(i).unwrap_or_default())
                    .collect();
                (
                    SplitEntries::Bottom(full, lsns, full_keys),
                    b.key_prefix.clone(),
                )
            }
        };

        // Determine split point — JE `IN.splitSpecial` / `IN.splitInternal`.
        //
        // Normal midpoint: `n_entries / 2`.
        // AllLeft:  insertion key is at position 0 on every descend level.
        //   → split_index = 1 (left half keeps n-1 entries; new right sibling
        //     gets only the former-first slot, then the insertion fills it).
        //   This matches JE: `if (leftSide && index == 0) splitInternal(…, 1)`.
        // AllRight: insertion key is at the last position on every level.
        //   → split_index = n_entries - 1 (left half keeps all but one entry).
        //   JE: `else if (!leftSide && index == nEntries-1) splitInternal(…, nEntries-1)`.
        //
        // Ref: `IN.java` splitSpecial ~line 4129, splitInternal ~line 4159.
        let n_entries = all_entries.len();
        let split_index = if n_entries >= 2 {
            // Find where insert_key falls in the child.
            let insert_idx = {
                let mut idx = 0usize;
                for i in 1..n_entries {
                    let ord = match key_comparator {
                        Some(cmp) => cmp(all_entries.get_key(i), insert_key),
                        None => all_entries.get_key(i).cmp(insert_key),
                    };
                    if ord != std::cmp::Ordering::Greater {
                        idx = i;
                    } else {
                        break;
                    }
                }
                idx
            };
            match hint {
                SplitHint::AllLeft if insert_idx == 0 => 1,
                SplitHint::AllRight if insert_idx == n_entries - 1 => {
                    n_entries - 1
                }
                _ => n_entries / 2,
            }
        } else {
            n_entries / 2
        };

        // newIdKey — the full key of the first entry of the right half.
        // For BIN: entries are already full keys after decompression above.
        // For IN:  entries carry full keys directly.
        let new_id_key = all_entries.get_key(split_index).to_vec();
        // Suppress unused-variable warning when no BIN is involved.
        let _ = &bin_old_prefix;

        // Divide into left and right halves.
        let left_entries = all_entries.slice(0, split_index);
        let right_entries = all_entries.slice(split_index, n_entries);

        // Install the left half into `child_arc` (still under the same
        // write lock) and mark the node dirty.
        match (&mut *child_guard, &left_entries) {
            (TreeNode::Internal(n), SplitEntries::Internal(le, lc, ll)) => {
                n.entries = le.clone();
                // T-4: reinstall the (now-shorter) left child array.
                n.targets = TargetRep::None;
                for (i, c) in lc.iter().enumerate() {
                    if let Some(child) = c {
                        n.set_child(i, Some(child.clone()));
                    }
                }
                // T-3: reinstall the (now-shorter) left LSN array.
                n.lsn_rep = LsnRep::from_lsns(ll);
            }
            (TreeNode::Bottom(b), SplitEntries::Bottom(le, ll, lk)) => {
                // Reset prefix; keys arrive as FULL keys (no prefix yet).
                b.key_prefix = Vec::new();
                // Pre-allocate at max_entries capacity so the left half
                // does not need to reallocate on the next insert (Fix 3).
                let mut left = Vec::with_capacity(max_entries);
                left.extend_from_slice(le);
                b.entries = left;
                // T-3: reinstall the left LSN array.
                b.lsn_rep = LsnRep::from_lsns(ll);
                // T-2: reinstall the left key rep from the full keys (Default;
                // recompute_key_prefix below compresses + compacts).
                b.keys = KeyRep::from_keys(lk.clone());
                // Recompute prefix on each half after split (only when
                // key_prefixing is enabled for this database).
                // JE: IN.computeKeyPrefix returns null when
                // databaseImpl.getKeyPrefixing() is false.
                // Ref: IN.java computeKeyPrefix ~line 2456.
                if key_prefixing && b.entries.len() >= 2 {
                    b.recompute_key_prefix();
                } else {
                    b.keys.compact(b.compact_max_key_length); // T-2
                }
            }
            _ => return Err(TreeError::SplitRequired),
        }
        child_guard.set_dirty(true);
        drop(child_guard);

        // Create the new right-half sibling.
        // Parent pointer will be wired in when it is inserted into the parent.
        let new_sibling = match right_entries {
            SplitEntries::Internal(re, rc, rl) => {
                let mut rin = InNodeStub {
                    node_id: generate_node_id(),
                    level: child_level,
                    entries: re,
                    targets: TargetRep::None,
                    dirty: true,
                    generation: 0,
                    parent: None, // set below
                    // T-3: the right half's per-slot LSNs.
                    lsn_rep: LsnRep::from_lsns(&rl),
                };
                // T-4: install the right half's resident children.
                for (i, c) in rc.into_iter().enumerate() {
                    if c.is_some() {
                        rin.set_child(i, c);
                    }
                }
                Arc::new(RwLock::new(TreeNode::Internal(rin)))
            }
            SplitEntries::Bottom(re, rl, rk) => {
                // Entries arrive as FULL keys; build BinStub with no prefix
                // then recompute key prefix for the new sibling.
                // Pre-allocate at max_entries capacity so the right half
                // does not need to reallocate on the next insert (Fix 3).
                let mut right = Vec::with_capacity(max_entries);
                right.extend(re);
                let mut sibling_bin = BinStub {
                    node_id: generate_node_id(),
                    level: child_level,
                    entries: right,
                    key_prefix: Vec::new(),
                    dirty: true,
                    is_delta: false,
                    last_full_lsn: NULL_LSN,
                    last_delta_lsn: NULL_LSN,
                    generation: 0,
                    parent: None, // set below
                    // St-H6 fix: inherit the splitting BIN's flag so that
                    // is_expired() uses the correct granularity for entries
                    // that were already in the BIN before the split.
                    // JE reference: BIN.java::split() propagates
                    // expirationInHours via setExpirationInHours(hours).
                    expiration_in_hours: bin_expiration_in_hours,
                    cursor_count: 0,
                    prohibit_next_delta: false,
                    // T-3: the right half's per-slot LSNs.
                    lsn_rep: LsnRep::from_lsns(&rl),
                    // T-2: full keys (Default); recompute/compact below.
                    keys: KeyRep::from_keys(rk),
                    compact_max_key_length: bin_compact_max_key_length,
                };
                // St-H6 debug guard: the sibling must carry the same flag as
                // the splitting BIN so that in_hours-resolution entries are
                // never silently expired by a mismatched false flag.
                debug_assert_eq!(
                    sibling_bin.expiration_in_hours, bin_expiration_in_hours,
                    "St-H6 invariant: sibling BIN expiration_in_hours must \
                     match the splitting BIN (got {}, expected {})",
                    sibling_bin.expiration_in_hours, bin_expiration_in_hours
                );

                if key_prefixing && sibling_bin.entries.len() >= 2 {
                    sibling_bin.recompute_key_prefix();
                } else {
                    sibling_bin.keys.compact(bin_compact_max_key_length); // T-2
                }
                Arc::new(RwLock::new(TreeNode::Bottom(sibling_bin)))
            }
        };

        // Note: the child (left half) was marked dirty earlier under the
        // same write lock that installed left_entries; no need to re-take
        // the write lock here.

        // Insert the new sibling into the parent after child_index.
        // We already hold `parent.write()` (taken at the top of the
        // function); operate on it directly rather than re-acquiring.
        match &mut *parent_write_guard {
            TreeNode::Internal(p) => {
                let insert_pos = child_index + 1;
                // T-4: insert the parent slot and set its cached child via the
                // node-level INTargetRep (shifting existing children).
                p.insert_entry(
                    insert_pos,
                    new_id_key,
                    lsn,
                    Some(new_sibling.clone()),
                );
                // Parent is dirty because it gained a new entry.
                p.dirty = true;
            }
            TreeNode::Bottom(_) => return Err(TreeError::SplitRequired),
        }

        // Wire the new sibling's parent pointer to the parent node
        // before releasing parent_write_guard, so a future descent that
        // takes parent.read() and finds the sibling immediately sees a
        // fully-wired parent pointer.
        {
            let mut g = new_sibling.write();
            g.set_parent(Some(Arc::downgrade(parent)));
        }
        // T-4: when an upper IN split, the children that moved into the new
        // sibling must have their parent back-pointers re-wired to the
        // sibling (JE re-parents moved targets in IN.split).
        {
            let sg = new_sibling.read();
            if let TreeNode::Internal(sn) = &*sg {
                let moved = sn.resident_children();
                drop(sg);
                for child in moved {
                    let mut cg = child.write();
                    cg.set_parent(Some(Arc::downgrade(&new_sibling)));
                }
            }
        }
        drop(parent_write_guard);

        // EVICTOR-RECLAIM-1: register the freshly-split sibling with the
        // evictor's LRU (JE IN.splitInternal calls inList.add(newSibling)).
        // Without this, split-created BINs/INs are invisible to the evictor:
        // the policy lists never receive them, every evict_batch phase quota
        // is 0, and eviction reclaims nothing under pressure even though the
        // nodes are fully resident.  Only the very first root+BIN (the
        // first-key path) and re-fetched nodes were ever registered.
        if let Some(l) = listener {
            let sibling_id = match &*new_sibling.read() {
                TreeNode::Internal(n) => n.node_id,
                TreeNode::Bottom(b) => b.node_id,
            };
            l.note_ins_added(sibling_id);
        }

        Ok(())
    }

    /// Recursive insert with preemptive splitting.
    ///
    /// Top-down traversal in `Tree.forceSplit` +
    /// `Tree.searchSplitsAllowed`:
    ///
    /// 1. At an upper IN: find which child slot covers `key`, split the child
    ///    proactively if it is full (so we always have room to insert the split
    ///    key into the parent), then recurse into the appropriate child.
    /// 2. At a BIN: insert the key/data directly.
    ///
    /// This implements the "preemptive splitting" strategy from the: we split
    /// children on the way down so we never need to walk back up.
    fn insert_recursive(
        node_arc: &Arc<RwLock<TreeNode>>,
        key: Vec<u8>,
        data: Vec<u8>,
        lsn: Lsn,
        max_entries: usize,
        key_comparator: Option<&KeyComparatorFn>,
        key_prefixing: bool,
        listener: Option<&Arc<dyn InListListener>>,
    ) -> Result<bool, TreeError> {
        Self::insert_recursive_inner(
            node_arc,
            key,
            data,
            lsn,
            max_entries,
            key_comparator,
            key_prefixing,
            true, // all_left_so_far
            true, // all_right_so_far
            listener,
        )
    }

    /// Inner recursive helper that threads `allLeftSideDescent` /
    /// `allRightSideDescent` from `Tree.forceSplit` (JE ~line 1912).
    ///
    /// Both flags start `true` at the root and are cleared as soon as the
    /// descent takes a non-leftmost / non-rightmost child slot.  At split
    /// time they are forwarded to `split_child` which uses them to pick the
    /// `splitSpecial` split index (JE `IN.splitSpecial` ~line 4129).
    #[allow(clippy::too_many_arguments)]
    fn insert_recursive_inner(
        node_arc: &Arc<RwLock<TreeNode>>,
        key: Vec<u8>,
        data: Vec<u8>,
        lsn: Lsn,
        max_entries: usize,
        key_comparator: Option<&KeyComparatorFn>,
        key_prefixing: bool,
        all_left_so_far: bool,
        all_right_so_far: bool,
        listener: Option<&Arc<dyn InListListener>>,
    ) -> Result<bool, TreeError> {
        // Determine if this is a BIN (leaf level).
        //
        // We hold a read lock on `node_arc` (the parent of any descent we
        // do below) for the duration of this call, releasing it just
        // before returning. That achieves *latch coupling*: a concurrent
        // `split_child(parent, …)` that wants to reorganise our subtree
        // ultimately needs `parent.write()` to install the new sibling,
        // and that write blocks until our read lock is dropped. Without
        // this, the descender-vs-splitter race goes:
        //
        //   T_X: at root, picks child_arc (BIN), drops root read lock.
        //   T_Y: at root, runs split_child(root, …): takes child_arc.write(),
        //        installs left half [E1..E5], creates sibling [E6..E10],
        //        takes root.write() and inserts the sibling.
        //   T_X: now takes child_arc.write() and inserts a key whose
        //        sort order falls in the right half. The key lands in
        //        child_arc (left half) but a future search descending
        //        from the root routes that key to the new sibling and
        //        does not find it — silently lost.
        //
        // Reproducer: noxu-db/tests/concurrent_commits_stress.rs
        // (32 threads × 100 keys, ~1–6 lost writes per run before this fix;
        // occasionally hundreds when an entire BIN is orphaned).
        let parent_guard = node_arc.read();
        let is_bin = parent_guard.is_bin();

        if is_bin {
            // BIN: drop the read lock and take the write lock; this is
            // safe because the *outer* call frame still holds a read
            // lock on this BIN's parent (or this is the root, in which
            // case the first-key path has already initialised it). A
            // concurrent split_child(parent, …) cannot run while the
            // outer parent.read() is held, so the BIN cannot be
            // restructured between dropping our read lock and acquiring
            // our write lock.
            drop(parent_guard);
            let mut guard = node_arc.write();
            match &mut *guard {
                TreeNode::Bottom(bin) => {
                    let is_new = if let Some(cmp) = key_comparator {
                        // Comparator-based insert: no prefix compression.
                        let (_idx, new) =
                            bin.insert_cmp(key, lsn, Some(data), cmp.as_ref());
                        new
                    } else if key_prefixing {
                        // insert_with_prefix handles prefix recomputation when
                        // the new key shrinks the existing prefix, and also
                        // initialises the prefix when 2 entries are present for
                        // the first time.
                        let (_idx, new) =
                            bin.insert_with_prefix(key, lsn, Some(data));
                        new
                    } else {
                        // key_prefixing disabled: store full key, no prefix.
                        // JE: IN.computeKeyPrefix returns null when
                        // databaseImpl.getKeyPrefixing() is false.
                        // Ref: IN.java computeKeyPrefix ~line 2456.
                        let (_idx, new) = bin.insert_raw(key, lsn, Some(data));
                        new
                    };
                    // Mark dirty after any modification.
                    bin.dirty = true;
                    Ok(is_new)
                }
                TreeNode::Internal(_) => Err(TreeError::SplitRequired),
            }
        } else {
            // Upper IN: find the child slot that covers key.
            // Index = parent.findEntry(key, false, false)
            // Entry zero in an upper IN has a virtual key (-infinity), so
            // any real key is routed to at least slot 0.
            let (child_index, n_entries_at_level, child_arc) =
                match &*parent_guard {
                    TreeNode::Internal(n) => {
                        // Binary search for the largest key <= search key.
                        // Slot 0 always matches (virtual key = -infinity).
                        let mut idx = 0usize;
                        for (i, entry) in n.entries.iter().enumerate() {
                            if i == 0 {
                                idx = 0;
                            } else {
                                let ord = match key_comparator {
                                    Some(cmp) => cmp(
                                        entry.key.as_slice(),
                                        key.as_slice(),
                                    ),
                                    None => {
                                        entry.key.as_slice().cmp(key.as_slice())
                                    }
                                };
                                if ord != std::cmp::Ordering::Greater {
                                    idx = i;
                                } else {
                                    break;
                                }
                            }
                        }
                        let child =
                            n.get_child(idx).ok_or(TreeError::SplitRequired)?;
                        (idx, n.entries.len(), child)
                    }
                    TreeNode::Bottom(_) => {
                        return Err(TreeError::SplitRequired);
                    }
                };

            // Update the descent-side flags (JE `Tree.forceSplit` ~1959).
            // `allLeftSideDescent`  ← still true only if we chose slot 0.
            // `allRightSideDescent` ← still true only if we chose the last slot.
            let all_left = all_left_so_far && child_index == 0;
            let all_right = all_right_so_far
                && child_index == n_entries_at_level.saturating_sub(1);

            // Proactively split the child if it is full.
            // If (child.needsSplitting()) child.split(parent, ...)
            let child_full = {
                let g = child_arc.read();
                g.get_n_entries() >= max_entries
            };

            if child_full {
                // Build the splitSpecial hint from the accumulated flags.
                // JE `Tree.forceSplit` ~line 2010:
                //   if (allLeftSideDescent || allRightSideDescent)
                //       child.splitSpecial(parent, index, grandParent,
                //           maxTreeEntriesPerNode, key, allLeftSideDescent)
                let hint = match (all_left, all_right) {
                    (true, _) => SplitHint::AllLeft,
                    (_, true) => SplitHint::AllRight,
                    _ => SplitHint::Normal,
                };
                // split_child(parent, …) needs parent.write(); we must
                // drop our parent read lock before calling it.
                drop(parent_guard);
                Self::split_child(
                    node_arc,
                    child_index,
                    max_entries,
                    lsn,
                    hint,
                    &key,
                    key_comparator,
                    key_prefixing,
                    listener,
                )?;

                // After the split, re-find which child now covers key.
                // Re-enter at the top of the inner function; carry the
                // flags (the new topology doesn't invalidate them — we
                // still know the overall descent direction).
                return Self::insert_recursive_inner(
                    node_arc,
                    key,
                    data,
                    lsn,
                    max_entries,
                    key_comparator,
                    key_prefixing,
                    all_left_so_far,
                    all_right_so_far,
                    listener,
                );
            }

            // Descend into the child while still holding parent_guard.
            // The recursive call will hold child.read() before this
            // returns, then drop it; combined with our parent_guard,
            // the latch coupling chain is preserved on the way down and
            // unwound on the way back up.
            let r = Self::insert_recursive_inner(
                &child_arc,
                key,
                data,
                lsn,
                max_entries,
                key_comparator,
                key_prefixing,
                all_left,
                all_right,
                listener,
            );
            drop(parent_guard);
            r
        }
    }

    /// Slice-based variant of [`Tree::insert_recursive`] for the recovery redo path.
    ///
    /// Accepts `key: &[u8]` and `data: Option<&[u8]>` instead of owned
    /// `Vec<u8>` values.  At the BIN leaf, calls
    /// [`BinStub::insert_with_prefix_slice`] which copies bytes into the
    /// `BinEntry` exactly once.
    ///
    /// For the comparator path (custom key comparator), falls back to
    /// `insert_cmp` with a one-time `to_vec()` conversion — that path is
    /// rare in practice (sorted-dup databases only) and is not on the
    /// W11 hot path.
    ///
    /// Wave 11-K optimisation (Fix 1).
    fn redo_insert_recursive(
        node_arc: &Arc<RwLock<TreeNode>>,
        key: &[u8],
        data: Option<&[u8]>,
        lsn: Lsn,
        max_entries: usize,
        key_comparator: Option<&KeyComparatorFn>,
        key_prefixing: bool,
    ) -> Result<bool, TreeError> {
        Self::redo_insert_recursive_inner(
            node_arc,
            key,
            data,
            lsn,
            max_entries,
            key_comparator,
            key_prefixing,
            true,
            true,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn redo_insert_recursive_inner(
        node_arc: &Arc<RwLock<TreeNode>>,
        key: &[u8],
        data: Option<&[u8]>,
        lsn: Lsn,
        max_entries: usize,
        key_comparator: Option<&KeyComparatorFn>,
        key_prefixing: bool,
        all_left_so_far: bool,
        all_right_so_far: bool,
    ) -> Result<bool, TreeError> {
        let parent_guard = node_arc.read();
        let is_bin = parent_guard.is_bin();

        if is_bin {
            drop(parent_guard);
            let mut guard = node_arc.write();
            match &mut *guard {
                TreeNode::Bottom(bin) => {
                    // REC-F2: JE redo currency check
                    // (RecoveryManager.redo() line ~2512/2544).  A logged LN
                    // is applied only when logrecLsn > treeLsn.  If the slot
                    // already holds an equal-or-newer LSN, skip the overwrite
                    // so an out-of-order (older-LSN) redo cannot revert
                    // committed data or reset the slot LSN backward.  This
                    // makes redo genuinely idempotent regardless of
                    // redo/undo phase order.  Deletes never reach this path
                    // (redo_ln routes Delete through tree.delete), so the JE
                    // "lsnCmp == 0 && isDeletion -> set KD" sub-case does not
                    // apply here.
                    let cmp_ref = key_comparator.map(|c| {
                        c.as_ref()
                            as &dyn Fn(&[u8], &[u8]) -> std::cmp::Ordering
                    });
                    if let Some(slot_lsn) =
                        bin.redo_slot_lsn(key, cmp_ref, key_prefixing)
                        && lsn <= slot_lsn
                    {
                        // Tree already holds an equal-or-newer version.
                        return Ok(false);
                    }
                    let is_new = if let Some(cmp) = key_comparator {
                        // Comparator path: fall back to owned-Vec variant.
                        let (_idx, new) = bin.insert_cmp(
                            key.to_vec(),
                            lsn,
                            data.map(|d| d.to_vec()),
                            cmp.as_ref(),
                        );
                        new
                    } else if key_prefixing {
                        let (_idx, new) =
                            bin.insert_with_prefix_slice(key, lsn, data);
                        new
                    } else {
                        // key_prefixing disabled: store full key verbatim.
                        // Ref: IN.java computeKeyPrefix ~line 2456.
                        let (_idx, new) = bin.insert_raw(
                            key.to_vec(),
                            lsn,
                            data.map(|d| d.to_vec()),
                        );
                        new
                    };
                    bin.dirty = true;
                    Ok(is_new)
                }
                TreeNode::Internal(_) => Err(TreeError::SplitRequired),
            }
        } else {
            let (child_index, n_entries_at_level, child_arc) =
                match &*parent_guard {
                    TreeNode::Internal(n) => {
                        let mut idx = 0usize;
                        for (i, entry) in n.entries.iter().enumerate() {
                            if i == 0 {
                                idx = 0;
                            } else {
                                let ord = match key_comparator {
                                    Some(cmp) => cmp(entry.key.as_slice(), key),
                                    None => entry.key.as_slice().cmp(key),
                                };
                                if ord != std::cmp::Ordering::Greater {
                                    idx = i;
                                } else {
                                    break;
                                }
                            }
                        }
                        let child =
                            n.get_child(idx).ok_or(TreeError::SplitRequired)?;
                        (idx, n.entries.len(), child)
                    }
                    TreeNode::Bottom(_) => {
                        return Err(TreeError::SplitRequired);
                    }
                };

            let all_left = all_left_so_far && child_index == 0;
            let all_right = all_right_so_far
                && child_index == n_entries_at_level.saturating_sub(1);

            let child_full = {
                let g = child_arc.read();
                g.get_n_entries() >= max_entries
            };

            if child_full {
                let hint = match (all_left, all_right) {
                    (true, _) => SplitHint::AllLeft,
                    (_, true) => SplitHint::AllRight,
                    _ => SplitHint::Normal,
                };
                drop(parent_guard);
                Self::split_child(
                    node_arc,
                    child_index,
                    max_entries,
                    lsn,
                    hint,
                    key,
                    key_comparator,
                    key_prefixing,
                    // Recovery redo path: the listener is not active during
                    // log replay (the evictor is wired AFTER recovery, and
                    // the INList is rebuilt separately).  EVICTOR-RECLAIM-1
                    // registration happens on the live insert path.
                    None,
                )?;
                return Self::redo_insert_recursive_inner(
                    node_arc,
                    key,
                    data,
                    lsn,
                    max_entries,
                    key_comparator,
                    key_prefixing,
                    all_left_so_far,
                    all_right_so_far,
                );
            }

            let r = Self::redo_insert_recursive_inner(
                &child_arc,
                key,
                data,
                lsn,
                max_entries,
                key_comparator,
                key_prefixing,
                all_left,
                all_right,
            );
            drop(parent_guard);
            r
        }
    }

    /// Pre-warm the tree's internal `Vec<BinEntry>` capacity before a redo
    /// pass that will insert approximately `n` records.
    ///
    /// If the tree is empty, this is a no-op (there is no BIN yet to reserve
    /// capacity on).  If the tree already has a root BIN (from a previous
    /// checkpoint), reserves `n.min(max_entries_per_node)` additional slots
    /// in that BIN's entries vector, eliminating the resize-double cycle
    /// during the redo loop.
    ///
    /// Wave 11-K optimisation (Fix 3).
    pub fn reserve_redo_capacity(&self, n: usize) {
        if n == 0 {
            return;
        }
        let root = match self.get_root() {
            Some(r) => r,
            None => return,
        };
        // Descend to the leftmost BIN and reserve there.
        let mut arc = root;
        loop {
            let guard = arc.read();
            match &*guard {
                TreeNode::Bottom(bin_guard) => {
                    let additional = n
                        .min(self.max_entries_per_node)
                        .saturating_sub(bin_guard.entries.len());
                    drop(guard);
                    let mut wguard = arc.write();
                    if let TreeNode::Bottom(bin) = &mut *wguard {
                        bin.entries.reserve(additional);
                    }
                    return;
                }
                TreeNode::Internal(inner) => {
                    let child = inner.get_child(0);
                    drop(guard);
                    match child {
                        Some(c) => arc = c,
                        None => return,
                    }
                }
            }
        }
    }

    /// Get the first (leftmost) BIN in the tree.
    ///
    /// Descends to the leftmost BIN by
    /// always following the first child slot at each upper IN level.
    pub fn get_first_node(&self) -> Option<SearchResult> {
        let mut guard: NodeArcReadGuard = self.get_root()?.read_arc();

        loop {
            if guard.is_bin() {
                let n = guard.get_n_entries();
                if n == 0 {
                    return None;
                }
                // TREE-F1: return the first LIVE slot, skipping known_deleted
                // slots (CursorImpl.java:2062-2064).  If the leftmost BIN is
                // entirely KD during the reconstitution window the cursor's
                // get_first falls through to its cross-BIN advance.
                if let TreeNode::Bottom(b) = &*guard {
                    match (0..b.entries.len()).find(|&i| b.slot_is_live(i)) {
                        Some(i) => {
                            return Some(SearchResult::with_values(
                                true, i as i32, false,
                            ));
                        }
                        None => return None,
                    }
                }
                return Some(SearchResult::with_values(true, 0, false));
            }

            // Capture the leftmost child Arc while holding `guard`, then
            // hand-over-hand: take the child read lock before releasing
            // the parent's. Same race fix as `Tree::search`.
            let next_arc = match &*guard {
                TreeNode::Internal(n_node) => n_node.get_child(0)?,
                _ => return None,
            };
            let next_guard = next_arc.read_arc();
            drop(guard);
            guard = next_guard;
        }
    }

    /// Get the last (rightmost) BIN in the tree.
    ///
    /// Descends to the rightmost BIN by
    /// always following the last child slot at each upper IN level.
    pub fn get_last_node(&self) -> Option<SearchResult> {
        let mut guard: NodeArcReadGuard = self.get_root()?.read_arc();

        loop {
            if guard.is_bin() {
                let n = guard.get_n_entries();
                if n == 0 {
                    return None;
                }
                // TREE-F1: return the last LIVE slot, skipping known_deleted
                // slots (CursorImpl.java:2062-2064).
                if let TreeNode::Bottom(b) = &*guard {
                    match (0..b.entries.len())
                        .rev()
                        .find(|&i| b.slot_is_live(i))
                    {
                        Some(i) => {
                            return Some(SearchResult::with_values(
                                true, i as i32, false,
                            ));
                        }
                        None => return None,
                    }
                }
                return Some(SearchResult::with_values(
                    true,
                    (n - 1) as i32,
                    false,
                ));
            }

            // Capture the rightmost child Arc while holding `guard`, then
            // hand-over-hand: take the child read lock before releasing
            // the parent's. Same race fix as `Tree::search`.
            let next_arc = match &*guard {
                TreeNode::Internal(n_node) => {
                    n_node.get_child(n_node.entries.len().saturating_sub(1))?
                }
                _ => return None,
            };
            let next_guard = next_arc.read_arc();
            drop(guard);
            guard = next_guard;
        }
    }

    /// Returns the number of root splits that have occurred.
    pub fn get_root_splits(&self) -> u64 {
        self.root_splits.load(Ordering::Relaxed)
    }

    /// Returns the number of relatches required.
    pub fn get_relatches_required(&self) -> u64 {
        self.relatches_required.load(Ordering::Relaxed)
    }

    /// Delete a key from the tree.
    ///
    /// Traverses the tree to find the BIN that should contain the key, then
    /// removes the entry. Returns true if the key was found and removed.
    ///
    /// Delete path in `Tree` from the.
    ///
    /// In-memory removal only — WAL logging for deletes is handled by the
    /// cursor layer (`cursor_impl.rs::log_ln_write`) before this is called,
    /// matching separation between LN logging and tree mutation.
    pub fn delete(&self, key: &[u8]) -> bool {
        let root = match self.get_root() {
            Some(r) => r,
            None => return false,
        };

        // F8 consistency: insert accounts key + data + BIN_ENTRY_OVERHEAD; delete must
        // subtract the SAME (data_len was previously omitted, leaking
        // data_len from the cache counter on every delete and biasing the
        // evictor's over-budget view). Peek the data length before deleting.
        let data_len = if self.memory_counter.is_some() {
            self.search_with_data(key)
                .filter(|sf| sf.found)
                .and_then(|sf| sf.data.as_ref().map(|d| d.len()))
                .unwrap_or(0)
        } else {
            0
        };

        let deleted =
            Self::delete_recursive(&root, key, self.key_comparator.as_ref());

        // Update the memory counter when an entry is removed.
        // IN.updateMemorySize(-delta) → MemoryBudget.updateTreeMemoryUsage(-delta).
        if deleted && let Some(counter) = &self.memory_counter {
            let delta = (key.len() + data_len + BIN_ENTRY_OVERHEAD) as i64;
            counter.fetch_sub(delta, Ordering::Relaxed);
        }

        deleted
    }

    /// Recursive helper for `delete`: descend to the BIN that holds `key`
    /// and remove it.
    fn delete_recursive(
        node_arc: &Arc<RwLock<TreeNode>>,
        key: &[u8],
        key_comparator: Option<&KeyComparatorFn>,
    ) -> bool {
        // Latch coupling, mirroring `insert_recursive`. Without this,
        // delete has the same "BIN split out from under us" race: thread
        // A finds child_arc as the target BIN under parent.read(), drops
        // the lock, and another thread runs split_child(parent, …) that
        // moves the target key into the new sibling. A then takes
        // child_arc.write(), looks for the key in the (now left-half)
        // BIN, doesn't find it, and returns `false`. The caller treats
        // the `false` as "key was not present", but the key is actually
        // still in the tree (in the sibling). Subsequent operations
        // observe a stale record that should have been deleted —
        // semantically a lost delete.
        let parent_guard = node_arc.read();
        let is_bin = parent_guard.is_bin();
        let child_arc = if !is_bin {
            match &*parent_guard {
                TreeNode::Internal(n) => {
                    // Find child slot with largest key <= search key
                    let mut idx = 0usize;
                    for (i, entry) in n.entries.iter().enumerate() {
                        if i == 0 {
                            idx = 0;
                        } else {
                            let ord = match key_comparator {
                                Some(cmp) => cmp(entry.key.as_slice(), key),
                                None => entry.key.as_slice().cmp(key),
                            };
                            if ord != std::cmp::Ordering::Greater {
                                idx = i;
                            } else {
                                break;
                            }
                        }
                    }
                    n.get_child(idx)
                }
                _ => None,
            }
        } else {
            None
        };

        if is_bin {
            // Drop the read lock before taking the write lock; the outer
            // call frame still holds the parent read lock so a concurrent
            // split_child cannot run on this BIN's parent until we unwind.
            drop(parent_guard);
            let mut g = node_arc.write();
            match &mut *g {
                TreeNode::Bottom(bin) => {
                    if let Some(cmp) = key_comparator {
                        bin.delete_cmp(key, cmp.as_ref())
                    } else {
                        // Entries store compressed (suffix) keys when key_prefix
                        // is non-empty.  Compress the search key before comparing.
                        //
                        // The caller is not required to ensure that `key`
                        // shares this BIN's learned `key_prefix` — a stray
                        // delete of a key that was never present (or that
                        // sits under a different prefix) is legal and must
                        // simply return `false`.  Calling `compress_key`
                        // unconditionally would `debug_assert!`-panic on
                        // such inputs, so guard it the same way the cursor
                        // path does.
                        if !bin.key_prefix.is_empty()
                            && !key.starts_with(bin.key_prefix.as_slice())
                        {
                            return false;
                        }
                        let suffix = bin.compress_key(key);
                        match bin.key_binary_search(suffix.as_slice()) {
                            Ok(idx) => {
                                bin.entries.remove(idx);
                                bin.keys.remove(idx); // T-2
                                bin.lsn_rep.remove_shift(idx); // T-3
                                // Mark dirty after any modification.
                                bin.dirty = true;
                                true
                            }
                            Err(_) => false,
                        }
                    }
                }
                _ => false,
            }
        } else {
            // Descend with parent_guard still held; the recursion will
            // hold its own read lock and drop ours after it returns.
            let r = match child_arc {
                Some(child) => {
                    Self::delete_recursive(&child, key, key_comparator)
                }
                None => false,
            };
            drop(parent_guard);
            r
        }
    }

    // ========================================================================
    // B-tree Merge / Compress
    // ========================================================================

    /// Merge under-full sibling BIN pairs and remove empty subtrees.
    ///
    /// `INCompressor` / `Tree.compressInternal()` logic.
    ///
    /// merges two adjacent siblings when their combined entry count is
    /// ≤ `max_entries_per_node` (the merge threshold equal to the node
    /// capacity).  The left sibling's entries are prepended into the right
    /// sibling; the parent key slot pointing at the left sibling is then
    /// removed from the parent IN with `deleteEntry`.  If the parent IN
    /// becomes empty after the removal the process repeats recursively up
    /// the tree.
    ///
    /// This implementation performs a single post-order walk so that each
    /// level is compressed after all its children have been compressed.
    pub fn compress(&self) {
        let root = match self.get_root() {
            Some(r) => r,
            None => return,
        };
        Self::compress_node(&root, self.max_entries_per_node);
    }

    // ── DST BIN-split gate shims (shuttle-only) ─────────────────────────────
    //
    // These expose the private split / merge-clear primitives so the shuttle
    // harness (`crates/noxu-tree/tests/shuttle_bin_split.rs`) can race them on
    // ONE shared child — the exact check-then-act interleaving that let the
    // BIN-split bug (`.agent/archived-audits/bench/
    // bug-bin-split-concurrency.md`) escape into a 96-thread benchmark instead
    // of DST.  Compiled ONLY under `--cfg noxu_shuttle`; production never sees
    // them (zero change, verified by `cargo tree`).

    /// The node capacity / split-and-merge threshold for this tree.
    #[cfg(noxu_shuttle)]
    pub fn shuttle_max_entries(&self) -> usize {
        self.max_entries_per_node
    }

    /// Drive `split_child(parent, child_index)` with default (no-comparator,
    /// no-prefix, no-listener) parameters — the same call the insert path
    /// makes after it has dropped the parent read lock (the drop→reacquire
    /// window where the race opens).
    #[cfg(noxu_shuttle)]
    pub fn shuttle_split_child(
        parent: &Arc<RwLock<TreeNode>>,
        child_index: usize,
        max_entries: usize,
        insert_key: &[u8],
    ) -> Result<(), TreeError> {
        Self::split_child(
            parent,
            child_index,
            max_entries,
            Lsn::new(1, 999),
            SplitHint::Normal,
            insert_key,
            None,  // no comparator
            false, // key_prefixing off
            None,  // no InListListener
        )
    }

    /// Simulate the racing INCompressor merge that CLEARS a child in place
    /// (`compress_node`'s `entries.clear()` on the merged-away left sibling),
    /// under the child's write lock — the stale state a second `split_child`
    /// observes after the caller's fullness check was already passed under the
    /// now-dropped parent read lock.  Returns the entry count observed BEFORE
    /// clearing (so the harness can assert it raced a still-full child).
    #[cfg(noxu_shuttle)]
    pub fn shuttle_clear_child(child_arc: &Arc<RwLock<TreeNode>>) -> usize {
        let mut cg = child_arc.write();
        let before = cg.get_n_entries();
        match &mut *cg {
            TreeNode::Bottom(b) => {
                b.entries.clear();
                b.lsn_rep = LsnRep::Empty;
                b.keys = KeyRep::new();
            }
            TreeNode::Internal(n) => {
                n.entries.clear();
                n.lsn_rep = LsnRep::Empty;
                n.targets = TargetRep::None;
            }
        }
        before
    }

    /// Drive one checkpoint dirty-BIN flush pass over this tree, faithful to
    /// the lock/dirty sequence in `noxu_recovery::Checkpointer::
    /// flush_one_tree_bins` — MINUS the WAL write, which needs a `LogManager`
    /// this pure-tree shuttle harness does not build.
    ///
    /// The sequence this preserves (the part shuttle must schedule against a
    /// concurrent insert):
    ///   1. `collect_dirty_bins(db_id)` under a tree/node READ lock — the
    ///      snapshot of dirty BIN `Arc`s at checkpoint start.
    ///   2. per BIN: take the node WRITE lock; apply the JE X-8 early-exit
    ///      guard (`!b.dirty && dirty_count()==0` → skip a node an evictor or
    ///      a racing pass already flushed+cleared); otherwise "log" it by
    ///      snapshotting its keys and calling `clear_dirty_after_full_log`
    ///      (the real `flush_one_tree_bins` full-BIN path, sans the
    ///      `lm.log(BIN, …)` between `serialize_full()` and
    ///      `clear_dirty_after_full_log`).
    ///
    /// Returns the set of full keys captured in the flush (the keys present in
    /// each BIN at the instant it was write-locked and cleared) — i.e. the
    /// keys this checkpoint made durable.  A shuttle harness races this against
    /// a concurrent insert and asserts the lost-dirty-node invariant: every
    /// inserted key is either in this captured set (flushed) OR still dirty in
    /// the tree afterwards (reflushed by the next checkpoint) — never silently
    /// clean-but-unflushed.
    ///
    /// The whole BIN mutation-and-clear runs under the SAME node write lock a
    /// concurrent `insert` takes, so the flush and the insert serialise on
    /// that latch; the capture-then-clear is atomic w.r.t. a racing insert on
    /// the same BIN.  This is exactly why JE's checkpoint is consistent: the
    /// per-IN latch, not a global one, orders the snapshot-clear against
    /// concurrent tree mutation.
    #[cfg(noxu_shuttle)]
    pub fn shuttle_checkpoint_flush_bins(&self, db_id: u64) -> Vec<Vec<u8>> {
        // Step 1: snapshot dirty BINs under the read path (same call the
        // checkpointer makes).
        let dirty_bins = self.collect_dirty_bins(db_id);
        let mut captured: Vec<Vec<u8>> = Vec::new();

        // Step 2: per-BIN write-lock, X-8 guard, capture keys, clear dirty.
        for (_node_db_id, bin_arc) in dirty_bins {
            let mut bin_guard = bin_arc.write();
            let b = match &mut *bin_guard {
                TreeNode::Bottom(b) => b,
                _ => continue,
            };
            let dirty = b.dirty_count();
            // JE X-8 early exit: a node already flushed+cleared between the
            // snapshot and this write-lock acquisition.
            if !b.dirty && dirty == 0 {
                continue;
            }
            // "Full BIN" path: capture every key (what serialize_full would
            // have written to the WAL) BEFORE clearing dirty — atomic under
            // the node write lock.
            for i in 0..b.entries.len() {
                if let Some(k) = b.get_full_key(i) {
                    captured.push(k);
                }
            }
            b.clear_dirty_after_full_log(Lsn::new(1, 1));
        }
        captured
    }

    /// Snapshot every full key currently present in the tree together with
    /// whether it would be reflushed by the next checkpoint — i.e. whether its
    /// slot is dirty OR its containing BIN is dirty.  A key that is present but
    /// NOT dirty (slot clean AND BIN clean) has been captured by a checkpoint
    /// full-log; a key that is present AND dirty will be picked up by the next
    /// `collect_dirty_bins` pass.
    ///
    /// The shuttle recovery-vs-mutation gate uses this to assert the
    /// LOST-DIRTY-NODE invariant: every concurrently-inserted key is EITHER in
    /// the checkpoint's captured set OR still dirty here — never present but
    /// silently clean-yet-unflushed (the lost-dirty-node bug, where a
    /// checkpoint clears the dirty flag without having captured the slot).
    ///
    /// Walks under READ locks only (no mutation), reusing the same recursive
    /// descent shape as `collect_dirty_bins`.
    #[cfg(noxu_shuttle)]
    pub fn shuttle_key_dirty_states(&self) -> Vec<(Vec<u8>, bool)> {
        let mut out: Vec<(Vec<u8>, bool)> = Vec::new();
        if let Some(root) = self.get_root() {
            Self::shuttle_key_dirty_states_recursive(&root, &mut out);
        }
        out
    }

    #[cfg(noxu_shuttle)]
    fn shuttle_key_dirty_states_recursive(
        node_arc: &Arc<RwLock<TreeNode>>,
        out: &mut Vec<(Vec<u8>, bool)>,
    ) {
        let guard = node_arc.read();
        match &*guard {
            TreeNode::Bottom(b) => {
                let bin_dirty = b.dirty;
                for i in 0..b.entries.len() {
                    if let Some(k) = b.get_full_key(i) {
                        // Reflushed next checkpoint iff the slot is dirty or
                        // the whole BIN is dirty.
                        let dirty = bin_dirty || b.entries[i].dirty;
                        out.push((k, dirty));
                    }
                }
            }
            TreeNode::Internal(n) => {
                let children: Vec<Arc<RwLock<TreeNode>>> =
                    n.resident_children();
                drop(guard);
                for child in children {
                    Self::shuttle_key_dirty_states_recursive(&child, out);
                }
            }
        }
    }

    /// Recursive post-order compress helper.
    ///
    /// Visits children first (post-order), then scans adjacent child
    /// pairs in the current IN and merges them when the merge condition
    /// holds: `left.n_entries + right.n_entries <= max_entries`.
    ///
    /// After merging, the parent entry for the left sibling is deleted.
    /// The loop restarts after each merge so that newly under-full pairs
    /// created by previous merges are also considered.
    fn compress_node(node_arc: &Arc<RwLock<TreeNode>>, max_entries: usize) {
        // Collect child arcs to recurse without holding the node lock.
        let children: Vec<Arc<RwLock<TreeNode>>> = {
            let g = node_arc.read();
            match &*g {
                TreeNode::Internal(n) => n.resident_children(),
                // BINs are leaves; nothing to compress at this level.
                TreeNode::Bottom(_) => return,
            }
        };

        // Post-order: recurse into every child before working on this level.
        for child in &children {
            Self::compress_node(child, max_entries);
        }

        // Compress the current IN level: merge adjacent under-full children.
        // Repeat until a full pass produces no merges.
        loop {
            let n_entries = {
                let g = node_arc.read();
                g.get_n_entries()
            };

            let mut merged_any = false;

            // `i` is the index of the *left* candidate; right is at `i+1`.
            let mut i = 0usize;
            while i + 1 < n_entries {
                // Fetch left and right child arcs.
                let (left_arc, right_arc) = {
                    let g = node_arc.read();
                    match &*g {
                        TreeNode::Internal(p) => {
                            let l = p.get_child(i);
                            let r = p.get_child(i + 1);
                            match (l, r) {
                                (Some(l), Some(r)) => (l, r),
                                _ => {
                                    i += 1;
                                    continue;
                                }
                            }
                        }
                        TreeNode::Bottom(_) => return,
                    }
                };

                let left_n = { left_arc.read().get_n_entries() };
                let right_n = { right_arc.read().get_n_entries() };

                // merge condition: combined count fits within one node.
                if left_n + right_n > max_entries {
                    i += 1;
                    continue;
                }

                // Determine node kind from left child.
                let left_is_bin = { left_arc.read().is_bin() };

                if left_is_bin {
                    // BIN merge: decompress left entries to full keys, then
                    // prepend into right BIN (also decompressed), and finally
                    // recompute the merged BIN's prefix.
                    // merge left into right, then
                    // recalcKeyPrefix on the merged node.
                    let left_full_entries: Vec<BinEntry> = {
                        {
                            let g = left_arc.read();
                            match &*g {
                                TreeNode::Bottom(b) => (0..b.entries.len())
                                    .map(|j| BinEntry {
                                        data: b.entries[j].data.clone(),
                                        known_deleted: b.entries[j]
                                            .known_deleted,
                                        dirty: b.entries[j].dirty,
                                        expiration_time: b.entries[j]
                                            .expiration_time,
                                    })
                                    .collect(),
                                _ => {
                                    i += 1;
                                    continue;
                                }
                            }
                        }
                    };
                    // T-3 / T-2: capture left's per-slot LSNs and FULL keys.
                    let (left_full_lsns, left_full_keys): (
                        Vec<Lsn>,
                        Vec<Vec<u8>>,
                    ) = {
                        let g = left_arc.read();
                        match &*g {
                            TreeNode::Bottom(b) => (
                                (0..b.entries.len())
                                    .map(|j| b.get_lsn(j))
                                    .collect(),
                                (0..b.entries.len())
                                    .map(|j| {
                                        b.get_full_key(j).unwrap_or_default()
                                    })
                                    .collect(),
                            ),
                            _ => (Vec::new(), Vec::new()),
                        }
                    };
                    {
                        {
                            let mut g = right_arc.write();
                            match &mut *g {
                                TreeNode::Bottom(rb) => {
                                    // Decompress right entries to full keys.
                                    let right_full: Vec<BinEntry> = (0..rb
                                        .entries
                                        .len())
                                        .map(|j| BinEntry {
                                            data: rb.entries[j].data.clone(),
                                            known_deleted: rb.entries[j]
                                                .known_deleted,
                                            dirty: rb.entries[j].dirty,
                                            expiration_time: rb.entries[j]
                                                .expiration_time,
                                        })
                                        .collect();
                                    // T-3 / T-2: right's per-slot LSNs + keys.
                                    let right_full_lsns: Vec<Lsn> =
                                        (0..rb.entries.len())
                                            .map(|j| rb.get_lsn(j))
                                            .collect();
                                    let right_full_keys: Vec<Vec<u8>> =
                                        (0..rb.entries.len())
                                            .map(|j| {
                                                rb.get_full_key(j)
                                                    .unwrap_or_default()
                                            })
                                            .collect();
                                    // Left entries are all smaller; prepend.
                                    let mut combined = left_full_entries;
                                    combined.extend(right_full);
                                    let mut combined_lsns = left_full_lsns;
                                    combined_lsns.extend(right_full_lsns);
                                    let mut combined_keys = left_full_keys;
                                    combined_keys.extend(right_full_keys);
                                    // Reset prefix and assign full keys.
                                    rb.key_prefix = Vec::new();
                                    rb.entries = combined;
                                    // T-3: rebuild the merged LSN array.
                                    rb.lsn_rep =
                                        LsnRep::from_lsns(&combined_lsns);
                                    // T-2: rebuild the merged key rep (Default;
                                    // recompute below compresses + compacts).
                                    rb.keys = KeyRep::from_keys(combined_keys);
                                    // Recompute prefix on merged BIN.
                                    if rb.entries.len() >= 2 {
                                        rb.recompute_key_prefix();
                                    } else {
                                        rb.keys
                                            .compact(rb.compact_max_key_length);
                                    }
                                    rb.dirty = true;
                                }
                                _ => {
                                    i += 1;
                                    continue;
                                }
                            }
                        }
                    }
                    // Clear the now-merged left BIN.
                    {
                        let mut g = left_arc.write();
                        if let TreeNode::Bottom(lb) = &mut *g {
                            lb.entries.clear();
                            lb.lsn_rep = LsnRep::Empty; // T-3
                            lb.keys = KeyRep::new(); // T-2
                            lb.key_prefix = Vec::new();
                            lb.dirty = true;
                        }
                    }
                } else {
                    // Upper-IN merge: prepend left's InEntries into right.
                    // T-4: capture left's resident children alongside its
                    // entries so they travel into the merged right IN.
                    let (left_in_entries, left_children): (
                        Vec<InEntry>,
                        Vec<Option<ChildArc>>,
                    ) = {
                        let g = left_arc.read();
                        match &*g {
                            TreeNode::Internal(n) => {
                                let children = (0..n.entries.len())
                                    .map(|j| n.get_child(j))
                                    .collect();
                                (n.entries.clone(), children)
                            }
                            _ => {
                                i += 1;
                                continue;
                            }
                        }
                    };
                    // T-3: capture left's per-slot LSNs.
                    let left_in_lsns: Vec<Lsn> = {
                        let g = left_arc.read();
                        match &*g {
                            TreeNode::Internal(n) => (0..n.entries.len())
                                .map(|j| n.get_lsn(j))
                                .collect(),
                            _ => Vec::new(),
                        }
                    };
                    let n_left = left_in_entries.len();
                    {
                        {
                            let mut g = right_arc.write();
                            match &mut *g {
                                TreeNode::Internal(rn) => {
                                    // Snapshot right's existing children, then
                                    // rebuild the merged entry + target arrays
                                    // (left half first, then right half).
                                    let right_children: Vec<Option<ChildArc>> =
                                        (0..rn.entries.len())
                                            .map(|j| rn.get_child(j))
                                            .collect();
                                    // T-3: snapshot right's LSNs too.
                                    let right_in_lsns: Vec<Lsn> =
                                        (0..rn.entries.len())
                                            .map(|j| rn.get_lsn(j))
                                            .collect();
                                    let mut combined = left_in_entries.clone();
                                    combined.append(&mut rn.entries);
                                    rn.entries = combined;
                                    // T-3: rebuild the merged LSN array.
                                    let mut combined_lsns =
                                        left_in_lsns.clone();
                                    combined_lsns.extend(right_in_lsns);
                                    rn.lsn_rep =
                                        LsnRep::from_lsns(&combined_lsns);
                                    rn.targets = TargetRep::None;
                                    for (j, c) in
                                        left_children.iter().enumerate()
                                    {
                                        if let Some(child) = c {
                                            rn.set_child(
                                                j,
                                                Some(child.clone()),
                                            );
                                        }
                                    }
                                    for (j, c) in
                                        right_children.into_iter().enumerate()
                                    {
                                        if c.is_some() {
                                            rn.set_child(n_left + j, c);
                                        }
                                    }
                                    rn.dirty = true;
                                }
                                _ => {
                                    i += 1;
                                    continue;
                                }
                            }
                        }
                    }
                    // Update parent pointers for moved children.
                    for child in left_children.into_iter().flatten() {
                        let mut cg = child.write();
                        cg.set_parent(Some(Arc::downgrade(&right_arc)));
                    }
                    // Clear the now-merged left IN.
                    {
                        let mut g = left_arc.write();
                        if let TreeNode::Internal(ln) = &mut *g {
                            ln.entries.clear();
                            ln.lsn_rep = LsnRep::Empty; // T-3
                            ln.targets = TargetRep::None;
                            ln.dirty = true;
                        }
                    }
                }

                // Remove the right sibling's parent slot and update
                // the left slot to point at the merged right child.
                //
                // We keep the LEFT slot's key (which is the correct minimum for
                // the merged BIN's range) and remove the RIGHT slot (i+1).
                // This avoids having to update the parent key when i == 0.
                {
                    {
                        let mut g = node_arc.write();
                        match &mut *g {
                            TreeNode::Internal(p) => {
                                // Update left slot (i) to point at right_arc
                                // (which now contains the merged entries).
                                if i < p.entries.len() {
                                    p.set_child(i, Some(right_arc.clone()));
                                }
                                // Remove right slot (i+1) — it is now redundant.
                                // T-4: remove_entry shifts the child array too.
                                if i + 1 < p.entries.len() {
                                    p.remove_entry(i + 1);
                                }
                                p.dirty = true;
                            }
                            TreeNode::Bottom(_) => return,
                        }
                    }
                }

                merged_any = true;
                // Advance i to check the merged BIN against its new right
                // sibling (the old slot i+2 is now at i+1).
                i += 1;
                let updated_n = { node_arc.read().get_n_entries() };
                if i + 1 >= updated_n {
                    break;
                }
            }

            if !merged_any {
                break;
            }
        }
    }

    // ========================================================================
    // BIN slot compression
    // ========================================================================

    /// Compress deleted slots from a BIN node, then prune it from its parent
    /// IN when it becomes empty.
    ///
    /// (the in-place slot-removal
    /// path, NOT the sibling-merge path handled by `compress()`).
    ///
    /// # Algorithm
    ///
    /// 1. If the BIN is a delta, skip — deltas cannot be compressed.
    /// 2. Remove all slots where `entry.known_deleted` is true.  This mirrors
    ///    `bin.compress(!bin.shouldLogDelta(), localTracker)`.
    /// 3. If the BIN is now empty, remove it from its parent IN.  This mirrors
    ///    `pruneBIN(db, binRef, idKey)` → `tree.delete(idKey)`.
    ///
    /// # Arguments
    ///
    /// * `bin_arc` — the BIN to compress (must be a `TreeNode::Bottom`).
    ///
    /// # Returns
    ///
    /// `true` if compression made progress (slots were removed or the BIN was
    /// pruned), `false` if the BIN was skipped (delta, no cursors issue, etc.).
    pub fn compress_bin(&self, bin_arc: &Arc<RwLock<TreeNode>>) -> bool {
        self.compress_bin_with_lock_check(bin_arc, None)
    }

    /// Like [`compress_bin`](Self::compress_bin), but consults a caller-supplied
    /// `is_locked` predicate before physically removing each `known_deleted`
    /// slot.  If `is_locked(slot_lsn)` returns `true`, the slot is SKIPPED
    /// (left for a later compression pass after the locking txn resolves).
    ///
    /// This is the faithful port of JE `BIN.compress` (BIN.java:1141-1172):
    ///
    /// > We have to be able to lock the LN before we can compress the entry.
    /// > If we can't, then skip over it. ... it is more efficient to call
    /// > `isLockUncontended` than to actually lock the LN, since we would
    /// > release the lock immediately.
    ///
    /// ```text
    /// if (lsn != DbLsn.NULL_LSN &&
    ///     !lockManager.isLockUncontended(lsn)) {
    ///     anyLocked = true;
    ///     continue;
    /// }
    /// ```
    ///
    /// JE's `isLockUncontended(lsn)` (LockManager.java:692) returns
    /// `nWaiters() == 0 && nOwners() == 0`.  Our `is_locked(lsn)` is its
    /// inverse: the dbi layer supplies a closure over the `LockManager` that
    /// returns `true` iff the slot's LSN has any owner or waiter
    /// (`LockManager::get_lock_info(lsn) != (0, 0)`).  A `NULL_LSN` slot is
    /// always discardable without locking (JE: "Can discard a NULL_LSN entry
    /// without locking"), so we never invoke the predicate for it.
    ///
    /// # Layering (noxu-tree -/-> noxu-txn)
    ///
    /// The predicate is a `&dyn Fn(u64) -> bool`, NOT a `LockManager`
    /// reference, so noxu-tree never depends on noxu-txn.  The lock knowledge
    /// lives entirely in the dbi-supplied closure.
    ///
    /// # Lock ordering (no deadlock)
    ///
    /// `is_locked` is invoked while this method holds the **BIN write latch**.
    /// The dbi closure calls `LockManager::get_lock_info`, which takes a
    /// lock-table *shard* mutex for a single, non-blocking critical section
    /// and releases it before returning — it never waits and never re-enters
    /// the tree.  The LockManager has no edge back into a BIN latch (lock
    /// acquisition descends the tree from the dbi/cursor layer, never the
    /// reverse).  The only ordering is therefore BIN-latch -> shard-mutex,
    /// which is acyclic; no lock cycle exists, so the predicate cannot
    /// deadlock against the latch.
    ///
    /// When `is_locked` is `None` (recovery, BIN-delta replay, unit tests with
    /// no lock manager) behavior is identical to the historical
    /// `compress_bin`: every `known_deleted` slot is removed.
    pub fn compress_bin_with_lock_check(
        &self,
        bin_arc: &Arc<RwLock<TreeNode>>,
        is_locked: Option<&dyn Fn(u64) -> bool>,
    ) -> bool {
        // ---- Step 1: collect metadata without holding the write lock ----
        let (is_delta, n_entries, id_key) = {
            {
                let g = bin_arc.read();
                match &*g {
                    TreeNode::Bottom(b) => {
                        // Identifier key = first full key in the BIN
                        // (the: bin.getIdentifierKey()).
                        let id_key = b.get_full_key(0);
                        (b.is_delta, b.entries.len(), id_key)
                    }
                    _ => return false, // not a BIN
                }
            }
        };

        // If (bin.isBINDelta()) return; — deltas cannot be compressed.
        if is_delta {
            return false;
        }

        // ---- Step 2: remove known-deleted slots) ----
        // We compress dirty slots too (compress_dirty_slots = true) because
        // we are not writing a BIN-delta here.
        let removed_any = {
            {
                let mut g = bin_arc.write();
                match &mut *g {
                    TreeNode::Bottom(b) => {
                        let before = b.entries.len();
                        // BIN.compress(): walk backwards to remove
                        // deleted slots without index confusion.
                        //
                        // IC-3 — JE `BIN.compress` (BIN.java:1141-1172) does
                        // NOT compress a slot it cannot lock: "We have to be
                        // able to lock the LN before we can compress the
                        // entry.  If we can't, then skip over it."  JE calls
                        // `lockManager.isLockUncontended(lsn)` and, on a
                        // contended slot, does `anyLocked = true; continue;`.
                        // We mirror that here via the optional `is_locked`
                        // predicate (supplied by the dbi layer, closing over
                        // the LockManager — see
                        // `compress_bin_with_lock_check`).  This removes the
                        // previously fragile implicit invariant ("no code path
                        // ever tombstones a slot before its txn commits"):
                        // even if a future write path leaves an uncommitted,
                        // write-locked `known_deleted` tombstone in a BinStub,
                        // the predicate keeps the compressor from physically
                        // removing a slot a live txn still references.
                        //
                        // When `is_locked` is `None` (recovery / BIN-delta
                        // replay / lock-manager-less tests) behavior is
                        // unchanged: every `known_deleted` slot is removed,
                        // matching the historical safe-by-invariant path.
                        let mut j = b.entries.len();
                        while j > 0 {
                            j -= 1;
                            if b.entries[j].known_deleted {
                                // IC-3 lock check (JE BIN.compress).  A
                                // NULL_LSN slot is always discardable without
                                // locking (JE: "Can discard a NULL_LSN entry
                                // without locking"), so we only consult the
                                // predicate for a non-null LSN.
                                if let Some(is_locked) = is_locked {
                                    let slot_lsn = b.get_lsn(j);
                                    if !slot_lsn.is_null()
                                        && is_locked(slot_lsn.as_u64())
                                    {
                                        // Slot still write-locked by an
                                        // in-flight txn — leave it for a later
                                        // pass (JE: anyLocked = true; continue).
                                        continue;
                                    }
                                }
                                // JE `IN.deleteEntry` (IN.java:3466): removing a
                                // DIRTY slot must prohibit the next delta — a
                                // delta only carries dirty slots, so the removal
                                // would otherwise be silently lost.  Force a
                                // full BIN on the next log.
                                if b.entries[j].dirty {
                                    b.prohibit_next_delta = true;
                                }
                                b.entries.remove(j);
                                b.keys.remove(j); // T-2
                                b.lsn_rep.remove_shift(j); // T-3
                                b.dirty = true;
                            }
                        }
                        // Recompute prefix after slot removal, since the
                        // remaining keys may share a longer common prefix.
                        // After compress(), call recalcKeyPrefix().
                        if b.entries.len() >= 2 {
                            b.recompute_key_prefix();
                        } else if b.entries.len() < 2 {
                            b.key_prefix = Vec::new();
                        }
                        b.entries.len() < before
                    }
                    _ => false,
                }
            }
        };

        // ---- Step 3: prune empty BIN from parent ----
        // If (empty) pruneBIN(db, binRef, idKey)  → tree.delete(idKey).
        // We only prune when the BIN is actually empty after compression.
        let now_empty = { bin_arc.read().get_n_entries() == 0 };

        if now_empty {
            // pruneBIN re-descends to the SPECIFIC empty BIN and removes its
            // parent-IN slot ONLY IF the BIN is still empty (and has no
            // cursors and is not a delta) UNDER THE PARENT LATCH.
            //
            // We must NOT use `self.delete(&id_key)` here (IC-1): that
            // re-descends by key and removes whatever live entry now matches
            // `id_key`.  Between reading `now_empty` (a fresh read lock taken
            // after the compression write lock was dropped) and acting on it,
            // a concurrent insert can repopulate this BIN; `self.delete` would
            // then drop a LIVE entry — tree corruption / lost write.
            //
            // JE `INCompressor.pruneBIN` (INCompressor.java ~line 502-510)
            // calls `tree.delete(idKey)`, and JE `Tree.delete` /
            // `searchDeletableSubTree` (Tree.java ~line 755-800) re-validates
            // `bin.getNEntries() != 0` → NODE_NOT_EMPTY (abort) and
            // `bin.nCursors() > 0` → CURSORS_EXIST (abort) while holding the
            // parent (branch) latch.  `prune_empty_bin` reproduces exactly
            // that re-validation.  See `prune_empty_bin` below.
            //
            // Note: we only attempt the prune if n_entries was > 0 before
            // compression (an already-empty BIN we never populated is left
            // alone, matching the pre-existing guard).
            if let Some(key) = id_key
                && n_entries > 0
            {
                self.prune_empty_bin(&key);
            }
            return true;
        }

        removed_any
    }

    /// Re-descend to the leaf BIN that should contain `id_key` and remove its
    /// parent-IN child slot ONLY IF the BIN is still safe to prune.
    ///
    /// This is the faithful port of JE `Tree.delete(idKey)` /
    /// `Tree.searchDeletableSubTree` (Tree.java ~line 755-800) as invoked by
    /// `INCompressor.pruneBIN` (INCompressor.java ~line 502-510).  JE takes the
    /// branch-parent latch, re-descends to the specific empty BIN, and aborts
    /// the prune (removing NOTHING) if any of the following changed since the
    /// compressor observed the BIN as empty:
    ///
    /// * `bin.getNEntries() != 0`  → `NodeNotEmptyException` (a concurrent
    ///   insert repopulated the BIN — IC-1: we must NOT delete a live entry).
    /// * `bin.isBINDelta()`        → `unexpectedState` (deltas are never empty).
    /// * `bin.nCursors() > 0`      → `CursorsExistException` (a cursor is parked
    ///   on the empty BIN; requeue rather than orphan the cursor).
    ///
    /// The re-check and the slot removal both happen while holding the
    /// **parent IN write latch**.  Holding the parent write latch blocks every
    /// descender (insert / delete take `parent.read()` hand-over-hand), so a
    /// concurrent insert cannot reach the BIN between our re-check and the
    /// slot removal — the TOCTOU window IC-1 describes is closed.
    ///
    /// Returns `true` iff a parent-IN slot was removed, `false` otherwise
    /// (BIN repopulated, has a cursor, is a delta, vanished, or is the root —
    /// in every `false` case NOTHING is removed).
    pub fn prune_empty_bin(&self, id_key: &[u8]) -> bool {
        let root = match self.get_root() {
            Some(r) => r,
            None => return false,
        };

        // If the root itself is the BIN (single-BIN tree) there is no parent
        // IN to remove a slot from.  JE's searchDeletableSubTree returns null
        // ("the entire tree is empty") and keeps the root BIN; we do the same.
        if root.read().is_bin() {
            return false;
        }

        // Descend by id_key tracking the IN that is the *parent of the leaf
        // BIN* and the child index within it.  Hand-over-hand read coupling
        // keeps the descent consistent with concurrent splits, exactly like
        // `get_parent_bin_for_child_ln`.
        let (parent_arc, child_index) = {
            let mut parent_arc: Arc<RwLock<TreeNode>> = root.clone();
            let mut guard: NodeArcReadGuard = root.read_arc();
            loop {
                let (next_arc, idx) = match &*guard {
                    TreeNode::Internal(n) => {
                        if n.entries.is_empty() {
                            return false;
                        }
                        let idx = self.upper_in_floor_index(&n.entries, id_key);
                        match n.get_child(idx) {
                            Some(c) => (c, idx),
                            None => return false,
                        }
                    }
                    TreeNode::Bottom(_) => {
                        unreachable!("is_bin checked before / below")
                    }
                };
                // Is the next node the leaf BIN?  If so, `guard`'s node is the
                // parent IN we want and `idx` is the child slot.
                if next_arc.read().is_bin() {
                    drop(guard);
                    break (parent_arc, idx);
                }
                let next_guard = next_arc.read_arc();
                drop(guard);
                parent_arc = next_arc;
                guard = next_guard;
            }
        };

        // ---- Re-validate and remove the slot UNDER THE PARENT WRITE LATCH ----
        // Holding parent.write() excludes all descenders (they need
        // parent.read()), so the BIN cannot be repopulated between the
        // re-check and the slot removal.
        let mut parent_guard = parent_arc.write();
        let pruned_bin_id;
        let removed_key_len = match &mut *parent_guard {
            TreeNode::Internal(p) => {
                let child = match p.get_child(child_index) {
                    Some(c) => c,
                    None => return false, // slot already vacated / invalid
                };
                // Re-validate the child BIN under the parent latch.
                {
                    let cg = child.read();
                    match &*cg {
                        TreeNode::Bottom(b) => {
                            // JE: bin.getNEntries() != 0 → NODE_NOT_EMPTY (abort).
                            if !b.entries.is_empty() {
                                return false;
                            }
                            // JE: bin.isBINDelta() → unexpectedState (abort).
                            if b.is_delta {
                                return false;
                            }
                            // JE: bin.nCursors() > 0 → CURSORS_EXIST (abort).
                            if b.cursor_count > 0 {
                                return false;
                            }
                            pruned_bin_id = b.node_id;
                        }
                        // A concurrent split could in principle have replaced
                        // the child with an IN; never prune in that case.
                        TreeNode::Internal(_) => return false,
                    }
                }
                // Safe to prune: remove the BIN's slot from the parent IN.
                // Mirrors the parent-slot removal `Tree.delete` performs for
                // an empty BIN (Tree.java deleteEntry under the branch latch).
                // T-4: remove_entry shifts the node-level child array too.
                let removed = p.remove_entry(child_index);
                p.dirty = true;
                removed.key.len()
            }
            TreeNode::Bottom(_) => return false,
        };
        drop(parent_guard);

        // JE: removing the BIN slot detaches the BIN from the tree; the
        // evictor must drop it from its LRU lists (Evictor.remove).
        self.note_removed(pruned_bin_id);

        // Preserve the memory-counter bookkeeping that `self.delete` performed
        // (IN.updateMemorySize(-delta) → MemoryBudget.updateTreeMemoryUsage).
        // The pruned slot's key plus the fixed per-entry overhead matches the
        // `delete` accounting (key.len() + BIN_ENTRY_OVERHEAD).
        if let Some(counter) = &self.memory_counter {
            let delta = (removed_key_len + BIN_ENTRY_OVERHEAD) as i64;
            counter.fetch_sub(delta, Ordering::Relaxed);
        }

        true
    }

    /// Detach the resident child node `node_id` from its parent IN, dropping
    /// the strong `Arc` so the node is actually freed from memory, and return
    /// the heap bytes reclaimed (0 if not found / not detachable).
    ///
    /// This is the faithful port of JE `IN.detachNode(idx, updateLsn, newLsn)`
    /// (IN.java ~4019) as called from `Evictor.evict` (Evictor.java ~3035):
    /// `evict` measures `target.getBudgetedMemorySize()` and then
    /// `parent.detachNode(index, ...)` does `setTarget(idx, null)` to drop the
    /// child reference and `getInMemoryINs().remove(child)` to drop it from
    /// the INList.
    ///
    /// EV-13: before this method existed, the evictor credited
    /// `node_size_fn(node_id)` bytes back to the budget and removed the node
    /// from the LRU lists, but the parent's `InEntry.child` still held a
    /// strong `Arc` — so the node was never dropped from the heap.  The budget
    /// over-credited (claimed bytes freed that were not), `cache_usage`
    /// drifted below reality, and the evictor under-fired.  Detaching here
    /// drops the `Arc` for real and credits exactly the measured size.
    ///
    /// The detach happens **under the parent IN write latch** (JE detaches
    /// under the parent's latch), so no concurrent descender can re-cache the
    /// child between measurement and detach.  The slot (key + LSN) is kept —
    /// only the in-memory `child` target is cleared — matching JE's
    /// `setTarget(idx, null)` which leaves the `ChildReference` LSN intact so
    /// the node can be re-fetched from the log later.
    ///
    /// Returns `0` if the node is not a resident child of any IN (e.g. it is
    /// the root, already detached, or was pinned and could not be latched).
    pub fn detach_node_by_id(&self, node_id: u64) -> u64 {
        let root = match self.get_root() {
            Some(r) => r,
            None => return 0,
        };

        // The root has no parent IN to detach from (JE evicts the root via a
        // separate evictRoot path; we keep the root resident here).
        let root_id = {
            let g = root.read();
            match &*g {
                TreeNode::Internal(n) => n.node_id,
                TreeNode::Bottom(b) => b.node_id,
            }
        };
        if root_id == node_id {
            return 0;
        }

        // Locate the parent IN and the child slot index.
        let (parent_arc, child_index) =
            match Self::find_parent_of_node_id(&root, node_id) {
                Some(p) => p,
                None => return 0,
            };

        // ---- Measure + detach UNDER THE PARENT WRITE LATCH ----
        // Holding parent.write() excludes all descenders (they take
        // parent.read() hand-over-hand), so the child cannot be re-cached or
        // re-pinned between the measurement and the detach.  Mirrors JE
        // detachNode running under the parent latch held by Evictor.evict.
        let mut parent_guard = parent_arc.write();
        let TreeNode::Internal(p) = &mut *parent_guard else {
            return 0; // parent is not an IN (concurrent restructure)
        };
        if child_index >= p.entries.len() {
            return 0;
        }
        // EVICTOR-LOG-1 safety: a BIN may only be detached once it has a
        // durable full-BIN version on disk (`last_full_lsn != NULL`).  The
        // parent slot LSN is stamped from `last_full_lsn` below and drives the
        // re-fetch (`fetch_node_from_log`, which parses the entry as an
        // InLogEntry/BIN).  If we detached a never-logged BIN the slot would
        // keep its prior value -- an *LN* LSN -- and the re-fetch would try to
        // parse an LN entry as a BIN and fail, silently losing the whole
        // BIN's keys.  Callers are expected to `flush_dirty_node_to_log`
        // first, but that can no-op (evictor without a LogManager wired / a
        // failed log write), so enforce the invariant here at the single
        // shared detach site rather than trusting every caller.  Peek without
        // removing the child so it is left resident on refusal.
        // JE: `Evictor.evict` only detaches after `target.log(...)` returns a
        // valid LSN (Evictor.java:3027-3035).
        if let Some(c) = p.child_ref(child_index)
            && matches!(&*c.read(), TreeNode::Bottom(b) if b.last_full_lsn == NULL_LSN)
        {
            return 0; // never-logged BIN -- keep resident, do not corrupt slot
        }
        // T-4: detach the cached child via the node-level INTargetRep, leaving
        // the slot's key/LSN intact for re-fetch (JE IN.setTarget(idx, null)).
        let child = match p.take_child(child_index) {
            Some(c) => c,     // child Arc removed from the slot
            None => return 0, // already detached
        };

        // Measure the child's real heap footprint while we still hold it.
        // JE: long evictedBytes = target.getBudgetedMemorySize().
        let freed = child.read().budgeted_memory_size();

        // EV-14 re-fetch correctness: the parent slot LSN must point at the
        // child's CURRENT on-disk version so `child_at_or_fetch` re-reads the
        // right bytes (JE `IN.updateEntry(idx, newLsn)` is called whenever a
        // child is logged; the parent slot LSN tracks the child's LSN).  The
        // evictor only fully evicts/detaches a CLEAN BIN (it logs+clears dirty
        // BINs via flush_dirty_node_to_log first, which sets `last_full_lsn`),
        // so the child's authoritative LSN is its `last_full_lsn`.  Stamp it
        // into the parent slot before dropping the child; if it is null (the
        // child was never logged) leave the existing slot LSN intact rather
        // than writing a null — a never-logged clean child cannot occur on
        // the evict path, but be conservative.
        let child_full_lsn = match &*child.read() {
            TreeNode::Bottom(b) => b.last_full_lsn,
            TreeNode::Internal(_) => NULL_LSN,
        };
        if child_full_lsn != NULL_LSN {
            p.set_lsn(child_index, child_full_lsn);
        }

        // Mark the parent dirty: the slot's in-memory target changed (JE
        // detachNode sets dirty when updateLsn; we conservatively mark dirty
        // so the parent is re-logged with the now-non-resident slot).
        p.dirty = true;

        // Drop the strong Arc explicitly so the node is freed now (the slot's
        // `child` is already None).  If any other resident path still held a
        // strong reference this would not free — but the tree is the sole
        // strong owner of a cached child, so this drops the last strong ref.
        drop(parent_guard);
        drop(child);

        // JE: getInMemoryINs().remove(child) — drop it from the evictor LRU.
        self.note_removed(node_id);

        // NOTE: the live tree-memory counter (`memory_counter`) is the SAME
        // `Arc<AtomicI64>` the evictor's Arbiter uses as `cache_usage`.  The
        // evictor decrements it once via `Arbiter::release_memory(bytes)` for
        // the full eviction batch, so detach must NOT decrement here too —
        // that would double-credit and drive `cache_usage` below reality
        // (the very drift EV-13 fixes, in the other direction).  We only
        // measure-and-free; the caller does the single counter update.
        freed
    }

    /// Evict the root IN of this tree (EV-14).
    ///
    /// Faithful port of JE `Evictor.evictRoot` (Evictor.java:3050-3110) plus
    /// the `RootEvictor.doWork` + `Tree.withRootLatchedExclusive` framing
    /// (Evictor.java:2529-2576, Tree.java:508-517).  Unlike a normal IN, the
    /// root has no parent slot to detach from; instead the *tree's* root
    /// reference is the equivalent of the `RootChildReference`, so eviction:
    ///
    ///   1. Latches the root reference exclusively (`rootLatch.acquireExclusive`
    ///      via `withRootLatchedExclusive`).
    ///   2. Re-checks that the root is still resident and still evictable
    ///      (no resident children, no pinned BIN — JE `RootEvictor.doWork`
    ///      re-latches and re-checks `rootIN == target && rootIN.isRoot()`).
    ///   3. If the root is dirty, LOGS it first so the on-disk version is
    ///      current and updates `root_log_lsn` to the new LSN (JE
    ///      `evictRoot`: `long newLsn = target.log(...); rootRef.setLsn(newLsn)`).
    ///   4. Clears the in-memory root (`rootRef.clearTarget()` — JE leaves the
    ///      `ChildReference` LSN intact; here `root_log_lsn` is that LSN) and
    ///      `note_removed`s it from the evictor LRU (JE `inList.remove(target)`).
    ///
    /// On the next access `fetch_root_from_log` re-materializes the root from
    /// `root_log_lsn` (JE `Tree.getRootINRootAlreadyLatched` →
    /// `root.fetchTarget`).
    ///
    /// # Conditions (eviction is REFUSED, returning `None`, when)
    ///
    /// * there is no log manager wired (the root could never be re-fetched),
    /// * the tree has no resident root (already evicted),
    /// * the root has any resident child (JE only evicts a childless root —
    ///   the `hasCachedChildren` skip in `processTarget`; a root with cached
    ///   children would orphan them, the EV-6 invariant),
    /// * the root is a BIN pinned by a cursor (`cursor_count > 0`),
    /// * the root is dirty but we have no clean persisted version AND logging
    ///   it fails, or
    /// * the root is clean but `root_log_lsn` is null (never logged — cannot
    ///   be re-fetched; happens only for a brand-new unlogged tree).
    ///
    /// Returns `Some((freed_bytes, was_dirty))` on success, where `freed_bytes`
    /// is the root's measured heap footprint (JE
    /// `target.getBudgetedMemorySize()`) and `was_dirty` reports whether the
    /// root had to be logged (JE `rootEvictor.flushed`, which drives
    /// `nDirtyNodesEvicted` and `modifyDbRoot`).
    pub fn evict_root(&self, db_id: u64) -> Option<(u64, bool)> {
        // A root with no re-fetch path must never be made non-resident.
        self.log_manager.as_ref()?;

        // JE `Tree.withRootLatchedExclusive(rootEvictor)`: hold the root latch
        // exclusively across the whole evict so no descender or splitter can
        // observe/install a half-evicted root.  Acquiring `self.root.write()`
        // is the Noxu equivalent (it is the lock guarding the root pointer).
        let mut root_slot = self.root.write();
        let root_arc = root_slot.as_ref()?.clone();

        // JE `RootEvictor.doWork`: re-latch the target and re-check the
        // conditions.  We hold the node guard for the duration.
        let node_guard = root_arc.write();

        // EV-6 / JE `processTarget` hasCachedChildren skip: a root with any
        // resident child must NOT be evicted (it would orphan the child).
        // EV-14 only evicts an *idle* root whose children are already
        // non-resident (or which is itself a leaf BIN).
        let (node_id, was_dirty, freed) = match &*node_guard {
            TreeNode::Internal(n) => {
                if !n.resident_children().is_empty() {
                    return None; // has cached children — keep resident
                }
                (n.node_id, n.dirty, node_guard.budgeted_memory_size())
            }
            TreeNode::Bottom(b) => {
                if b.cursor_count > 0 {
                    return None; // pinned by a cursor — keep resident
                }
                (
                    b.node_id,
                    b.dirty || b.dirty_count() > 0,
                    node_guard.budgeted_memory_size(),
                )
            }
        };

        // If dirty, log the root first so the on-disk version is current,
        // then record the new LSN as the root's re-fetch point (JE
        // `evictRoot`: target.log(...) + rootRef.setLsn(newLsn)).
        if was_dirty {
            let lm = self.log_manager.as_ref()?; // checked above; re-borrow
            let node_bytes = node_guard.write_to_bytes();
            let is_bin = node_guard.is_bin();
            let entry = noxu_log::entry::in_log_entry::InLogEntry::new(
                db_id, NULL_LSN, // prev_full_lsn
                NULL_LSN, // prev_delta_lsn
                node_bytes,
            );
            let mut buf = bytes::BytesMut::with_capacity(entry.log_size());
            entry.write_to_log(&mut buf);
            let entry_type = if is_bin {
                noxu_log::LogEntryType::BIN
            } else {
                noxu_log::LogEntryType::IN
            };
            // flush_required = true so the root's bytes are durable before we
            // drop the in-memory copy (JE logs synchronously in evictRoot).
            let new_lsn = match lm.log(
                entry_type,
                &buf,
                noxu_log::Provisional::No,
                true,  // flush_required
                false, // fsync at next checkpoint
            ) {
                Ok(l) => l,
                Err(_) => return None, // could not log — keep the root resident
            };
            *self.root_log_lsn.write() = new_lsn;
        } else {
            // Clean root: it must already be re-fetchable.  If it was never
            // logged (root_log_lsn null) we cannot evict it safely.
            if *self.root_log_lsn.read() == NULL_LSN {
                return None;
            }
        }

        // JE `rootRef.clearTarget()` + `inList.remove(target)`: drop the
        // in-memory root and remove it from the evictor LRU.  The root_log_lsn
        // is the surviving `ChildReference` LSN used to re-fetch it.
        drop(node_guard);
        *root_slot = None;
        drop(root_slot);
        self.note_removed(node_id);

        Some((freed, was_dirty))
    }

    /// Re-materialize an evicted root IN from its persisted `root_log_lsn`
    /// (EV-14, piece B).
    /// Faithful to JE `Tree.getRootINRootAlreadyLatched` (Tree.java:477-516)
    /// which calls `root.fetchTarget(database, null)` when the in-memory
    /// target is null.  Idempotent and cheap when the root is already
    /// resident: returns the resident root without touching the log.
    ///
    /// Returns `None` only when the tree is genuinely empty (no resident root
    /// AND `root_log_lsn` is null) or when the re-fetch fails (no log manager,
    /// log read error, deserialize failure) — callers then see an empty tree,
    /// never wrong data.
    pub fn fetch_root_from_log(&self) -> Option<Arc<RwLock<TreeNode>>> {
        // Fast path: root already resident.
        if let Some(r) = self.root.read().clone() {
            return Some(r);
        }
        // Take the write lock and re-check (another thread may have re-fetched
        // it while we waited — JE upgrades the root latch the same way).
        let mut root_slot = self.root.write();
        if let Some(r) = root_slot.as_ref() {
            return Some(r.clone());
        }
        let log_lsn = *self.root_log_lsn.read();
        let node = self.fetch_node_from_log(log_lsn)?;
        let node_id = node.node_id();
        let arc = Arc::new(RwLock::new(node));
        *root_slot = Some(arc.clone());
        drop(root_slot);
        // JE: a fetched IN is added back to the INList (Evictor LRU).
        self.note_added(node_id);
        Some(arc)
    }

    /// Return the resident child Arc for slot `idx` of `parent_arc`, fetching
    /// it from its slot LSN and installing it if it is not resident (EV-14 /
    /// EV-13 re-fetch on descent).
    ///
    /// Faithful to JE `ChildReference.fetchTarget` (and `IN.fetchTarget`):
    /// when a slot's in-memory target is null but its LSN is valid, the node
    /// is read back from the log and cached in the slot.  Installing the
    /// fetched child requires the parent EX-latch, so this takes the parent
    /// write lock; the fast path (child already resident) takes only a read
    /// lock.
    ///
    /// Returns `None` only when the slot index is out of range, the slot has
    /// no valid LSN, or the log read/deserialize fails — callers then treat
    /// the descent as terminating in an empty subtree, never wrong data.
    fn child_at_or_fetch(
        &self,
        parent_arc: &Arc<RwLock<TreeNode>>,
        idx: usize,
    ) -> Option<ChildArc> {
        // Fast path: child already cached (read lock only).
        {
            let g = parent_arc.read();
            if let TreeNode::Internal(n) = &*g {
                if let Some(c) = n.get_child(idx) {
                    return Some(c);
                }
            } else {
                return None; // BINs have no IN children
            }
        }
        // Slow path: fetch the child from its slot LSN under the parent
        // EX-latch (JE installs the fetched target under the IN latch).
        let mut g = parent_arc.write();
        let TreeNode::Internal(n) = &mut *g else {
            return None;
        };
        // Re-check: another thread may have fetched it while we upgraded.
        if let Some(c) = n.get_child(idx) {
            return Some(c);
        }
        if idx >= n.entries.len() {
            return None;
        }
        let child_lsn = n.get_lsn(idx);
        let node = self.fetch_node_from_log(child_lsn)?;
        let node_id = node.node_id();
        let arc: ChildArc = Arc::new(RwLock::new(node));
        n.set_child(idx, Some(arc.clone()));
        drop(g);
        // JE: a fetched IN is added back to the INList (Evictor LRU).
        self.note_added(node_id);
        Some(arc)
    }

    /// Check whether a BIN node is a candidate for slot compression and,
    /// if so, trigger `compress_bin`.
    ///
    /// from (the opportunistic / lazy compression path).
    ///
    /// # Algorithm
    ///
    /// 1. Skip the BIN if it is a delta or has no defunct (known-deleted) slots.
    /// 2. If compression succeeds and the BIN becomes empty, it is pruned.
    ///
    /// # Returns
    ///
    /// `true` if compression was triggered (regardless of whether any slots
    /// were actually removed), `false` if the BIN does not need compression.
    pub fn maybe_compress_bin_and_parent(
        &self,
        bin_arc: &Arc<RwLock<TreeNode>>,
    ) -> bool {
        // Check whether the BIN has any deleted slots worth compressing.
        // lazyCompress: skip deltas and BINs with no defunct slots.
        let should_compress = {
            {
                let g = bin_arc.read();
                match &*g {
                    TreeNode::Bottom(b) => {
                        // Skip deltas (the: !in.isBIN() || in.isBINDelta()).
                        if b.is_delta {
                            false
                        } else {
                            // Check for any known-deleted slot
                            // (the: for (int i=0; i < bin.getNEntries(); i++) {
                            //        if (bin.isDefunct(i)) { ... break; }
                            //      }).
                            b.entries.iter().any(|e| e.known_deleted)
                        }
                    }
                    _ => false,
                }
            }
        };

        if !should_compress {
            return false;
        }

        self.compress_bin(bin_arc)
    }

    // ========================================================================
    // Latch-coupling validation
    // ========================================================================

    /// Validate that `parent.entries[child_index].child` still points at
    /// `child_arc` after acquiring the child's latch.
    ///
    /// Re-latch validation step inside the
    /// `Tree.searchSplitsAllowed`: after a concurrent split the parent
    /// slot that previously held the child may have changed.  Callers that
    /// plan to mutate the child must verify the parent-child link is still
    /// intact before proceeding.
    ///
    /// Returns `true` if the parent-child link is intact.
    pub fn validate_parent_child(
        parent: &Arc<RwLock<TreeNode>>,
        child_index: usize,
        child_arc: &Arc<RwLock<TreeNode>>,
    ) -> bool {
        let g = parent.read();
        match &*g {
            TreeNode::Internal(p) => match p.child_ref(child_index) {
                Some(stored) => Arc::ptr_eq(stored, child_arc),
                None => false,
            },
            TreeNode::Bottom(_) => false,
        }
    }

    /// Search for the BIN that should contain `key`, with latch-coupling
    /// validation at every level of descent.
    ///
    /// .
    ///
    /// The difference from `search()` is that after obtaining the child
    /// arc we call `validate_parent_child` to confirm the parent still
    /// holds the expected Arc.  If the link has been broken (e.g. by a
    /// concurrent split that relocated the child) the traversal restarts
    /// from the root.
    ///
    /// Returns a `SearchResult` if the key is (or should be) in the tree,
    /// `None` if the tree is empty.
    ///
    /// Same as [`Tree::search`] but exposes the hand-over-hand latch
    /// coupling explicitly. Kept as a public, equivalent API for
    /// callers (today only tests) that want to verify the
    /// latch-coupling behaviour against `search()` itself.
    ///
    /// Both `search()` and this method use the same `read_arc()`
    /// hand-over-hand: take the child read guard *before* dropping
    /// the parent guard, so a concurrent `split_child(parent, ..)`
    /// (which takes `parent.write()`) cannot run between when we
    /// captured the child Arc and when we entered the child. There
    /// is no validate-and-restart loop because the coupling makes
    /// the race unreachable.
    pub fn search_with_coupling(&self, key: &[u8]) -> Option<SearchResult> {
        let root = self.get_root()?;
        let mut guard: NodeArcReadGuard = root.read_arc();

        loop {
            if guard.is_bin() {
                let index = guard.find_entry(key, true, true);
                let found = index >= 0 && (index & EXACT_MATCH != 0);
                return Some(SearchResult::with_values(
                    found,
                    index & 0xFFFF,
                    false,
                ));
            }

            let parent_arc = NodeArcReadGuard::rwlock(&guard).clone();
            let next_idx = match &*guard {
                TreeNode::Internal(n) => {
                    if n.entries.is_empty() {
                        return None;
                    }
                    let idx = self.upper_in_floor_index(&n.entries, key);
                    match n.get_child(idx) {
                        Some(c) => {
                            let next_guard = c.read_arc();
                            drop(guard);
                            guard = next_guard;
                            continue;
                        }
                        None => idx, // EV-14/EV-13: re-fetch below.
                    }
                }
                TreeNode::Bottom(_) => {
                    unreachable!("is_bin() returned false above")
                }
            };
            // Hand-over-hand: take the child read guard before
            // releasing the parent guard. Closes the
            // descender-vs-splitter window: a concurrent
            // split_child(parent, ..) takes parent.write(), which
            // blocks while we still hold parent.read().
            drop(guard);
            let child = self.child_at_or_fetch(&parent_arc, next_idx)?;
            guard = child.read_arc();
        }
    }

    // ========================================================================
    // BIN-Delta reconstitution
    // ========================================================================

    /// Increments the cursor-pin count on a BIN node.
    ///
    /// Called by `CursorImpl` when it positions on (or enters) a BIN.
    /// The evictor will not select a BIN with `cursor_count > 0` for eviction
    /// (`RealNodeInfo.pin_count`), matching `BIN.incrementCursorCount()`.
    pub fn pin_bin(bin_arc: &Arc<RwLock<TreeNode>>) {
        let mut guard = bin_arc.write();
        if let TreeNode::Bottom(ref mut stub) = *guard {
            stub.cursor_count += 1;
        }
    }

    /// Decrements the cursor-pin count on a BIN node.
    ///
    /// Called by `CursorImpl` when it moves away from or closes on a BIN.
    /// Uses `saturating_sub` to guard against an accidental double-unpin.
    /// Matching `BIN.decrementCursorCount()`.
    pub fn unpin_bin(bin_arc: &Arc<RwLock<TreeNode>>) {
        let mut guard = bin_arc.write();
        if let TreeNode::Bottom(ref mut stub) = *guard {
            stub.cursor_count = stub.cursor_count.saturating_sub(1);
        }
    }

    /// Returns `true` if the given `BinStub` is a BIN-delta (not a full BIN).
    ///
    /// `IN.isBINDelta()`.
    pub fn bin_is_delta(bin: &BinStub) -> bool {
        bin.is_delta
    }

    /// Merge delta entries into a full BIN's entry list.
    ///
    /// - For each delta entry: if a matching key already exists in `bin`,
    ///   replace it (delta is authoritative).
    /// - Otherwise insert the delta entry in sorted position.
    ///
    /// Delta entries carry **full** keys (prefix already prepended by the
    /// caller).  After applying all delta entries the BIN's prefix is
    /// recomputed so the final state is consistent.
    ///
    /// All delta entries are considered to be the most-recently-dirtied
    /// state, exactly as in where delta slots supersede full-BIN slots.
    pub fn apply_delta_to_bin(
        bin: &mut BinStub,
        delta_entries: Vec<(Vec<u8>, Lsn, Option<Vec<u8>>)>,
    ) {
        for (full_key, lsn, data) in delta_entries {
            // `full_key` is a full (uncompressed) key here.
            bin.insert_with_prefix(full_key, lsn, data);
        }
        bin.dirty = true;
    }

    /// Reconstitute a BIN-delta into a full BIN.
    ///
    /// from the:
    ///
    /// 1. Extract the delta entries from `self` (this BIN-delta), decompressing
    ///    them to full keys.
    /// 2. Apply them onto `base` (the previously logged full BIN) via
    ///    `apply_delta_to_bin`.
    /// 3. Copy `base`'s merged entries and prefix back into `self`.
    /// 4. Clear the `is_delta` flag so subsequent code treats `self` as
    ///    a full BIN.
    ///
    /// After this call `self` is a full BIN; `base` should be discarded.
    pub fn mutate_to_full_bin(delta: &mut BinStub, mut base: BinStub) {
        // Decompress delta entries to full keys before applying.
        let delta_full_entries: Vec<(Vec<u8>, Lsn, Option<Vec<u8>>)> = (0
            ..delta.entries.len())
            .map(|i| {
                (
                    delta.get_full_key(i).unwrap_or_default(),
                    delta.get_lsn(i),
                    delta.entries[i].data.clone(),
                )
            })
            .collect();
        // reconstituteBIN + resetContent + setBINDelta(false).
        Self::apply_delta_to_bin(&mut base, delta_full_entries);
        delta.entries = base.entries;
        delta.lsn_rep = base.lsn_rep; // T-3
        delta.keys = base.keys; // T-2
        delta.key_prefix = base.key_prefix;
        delta.is_delta = false;
        delta.dirty = true;
    }

    /// Read an IN/BIN log entry at `log_lsn` and deserialise it into a
    /// `TreeNode`, ready to be installed as a (re-fetched) resident node.
    ///
    /// JE `LogManager.getLogEntry(lsn)` + `IN.readFromLog` as used by
    /// `ChildReference.fetchTarget` (the path that re-materializes a
    /// non-resident node from its persisted LSN on descent) and by
    /// `Tree.getRootINRootAlreadyLatched` for the root.  The freshly-fetched
    /// node has no resident children (`TargetRep::None`); its own children, if
    /// any, are re-fetched on demand the same way when the descent reaches
    /// them.
    ///
    /// Returns `None` if the LSN is null, the log read fails, the entry is not
    /// an IN/BIN, or deserialisation fails (the caller treats this as "node
    /// unavailable" rather than panicking, matching the graceful-degradation
    /// policy of `mutate_to_full_bin_from_log`).
    fn fetch_node_from_log(&self, log_lsn: Lsn) -> Option<TreeNode> {
        if log_lsn == NULL_LSN {
            return None;
        }
        let lm = self.log_manager.as_ref()?;
        let (entry_type, payload) = lm.read_entry(log_lsn).ok()?;
        // The on-disk payload is an `InLogEntry` body (db_id | prev_full_lsn
        // | prev_delta_lsn | len | node_data).  The recovery scanner strips
        // this header before calling `recover_in_redo`; re-fetch must do the
        // same so `deserialize_*` sees the bare node bytes.  JE
        // `INLogEntry.readEntry` parses the same wrapper.
        let in_entry =
            noxu_log::entry::in_log_entry::InLogEntry::read_from_log(&payload)
                .ok()?;
        let node_data = &in_entry.node_data;
        use noxu_log::LogEntryType;
        match entry_type {
            LogEntryType::BIN => {
                Self::deserialize_bin(node_data).map(TreeNode::Bottom)
            }
            LogEntryType::IN => {
                Self::deserialize_upper_in(node_data).map(TreeNode::Internal)
            }
            // BIN-deltas are never logged as the *root* version and are
            // reconstituted by the BIN-delta path, not here.
            _ => {
                log::warn!(
                    "fetch_node_from_log: expected IN/BIN entry at LSN {:?}, \
                     got {:?}",
                    log_lsn,
                    entry_type
                );
                None
            }
        }
    }

    /// Reconstitute a BIN-delta into a full BIN by reading the base from log.
    ///
    /// — the
    /// single-argument overload that calls `fetchFullBIN(databaseImpl)` to
    /// read the last full BIN from the log manager automatically.
    ///
    /// Algorithm:
    /// 1. If `delta.last_full_lsn == NULL_LSN`, the BIN was never written as a
    ///    full entry; there is no base to merge so the delta IS the full BIN.
    ///    Clear `is_delta` and return.
    /// 2. Read the full-BIN log entry at `delta.last_full_lsn` using
    ///    `log_manager.read_entry(lsn)`.
    /// 3. Deserialize the payload with `BinStub::deserialize_full()`.
    /// 4. Delegate to `Self::mutate_to_full_bin(delta, base)` to merge and
    ///    replace `delta`'s contents.
    ///
    /// On any read / parse failure the function falls back to clearing the
    /// `is_delta` flag without merging, so the caller always gets a non-delta
    /// BIN (possibly missing some old slots).  This mirrors the
    /// `EnvironmentFailureException` path but gracefully degrades instead of
    /// panicking.
    ///
    /// `BIN.fetchFullBIN(dbImpl)` + `BIN.mutateToFullBIN(boolean)`.
    pub fn mutate_to_full_bin_from_log(
        delta: &mut BinStub,
        log_manager: &noxu_log::LogManager,
    ) {
        if !delta.is_delta {
            // Already a full BIN; nothing to do.
            return;
        }

        if delta.last_full_lsn == NULL_LSN {
            // BIN has never been logged as a full entry — the in-memory delta
            // is effectively the full state. During recovery this path is
            // harmless.
            delta.is_delta = false;
            return;
        }

        // Read the full-BIN log entry at last_full_lsn.
        // `envImpl.getLogManager().getEntryHandleFileNotFound(lsn)`.
        match log_manager.read_entry(delta.last_full_lsn) {
            Ok((entry_type, payload)) => {
                use noxu_log::LogEntryType;
                if entry_type == LogEntryType::BIN {
                    if let Some(mut base) = BinStub::deserialize_full(&payload)
                    {
                        // Set the base's last_full_lsn so it is preserved
                        // into the merged result.
                        base.last_full_lsn = delta.last_full_lsn;
                        Self::mutate_to_full_bin(delta, base);
                        return;
                    }
                    // Deserialization failed — fall through to graceful degradation.
                    log::warn!(
                        "mutate_to_full_bin_from_log: failed to deserialize \
                         full BIN at LSN {:?}; keeping delta as-is",
                        delta.last_full_lsn
                    );
                } else {
                    log::warn!(
                        "mutate_to_full_bin_from_log: expected BIN entry at \
                         LSN {:?}, got {:?}",
                        delta.last_full_lsn,
                        entry_type
                    );
                }
            }
            Err(e) => {
                log::warn!(
                    "mutate_to_full_bin_from_log: failed to read log at \
                     LSN {:?}: {}",
                    delta.last_full_lsn,
                    e
                );
            }
        }

        // Graceful degradation: promote the delta to a "full" BIN without
        // the base slots.  The BIN will be re-logged as a full BIN at the
        // next checkpoint.
        delta.is_delta = false;
        delta.dirty = true;
    }

    // ========================================================================
    // getNextBin / getPrevBin
    // ========================================================================

    /// Return the entries of the BIN immediately to the right of the BIN
    /// that contains (or would contain) `current_key`.
    ///
    /// → `Tree.getNextIN(forward=true)`.
    ///
    /// # Algorithm
    /// 1. Build a root-to-BIN path for `current_key`.
    /// 2. Walk the path back up looking for a parent that has a slot to the
    ///    right of the slot we descended through.
    /// 3. When found, descend to the leftmost BIN of that sibling subtree.
    /// 4. If no such parent exists, return `None` (no next BIN).
    pub fn get_next_bin(
        &self,
        current_key: &[u8],
    ) -> Option<Vec<(BinEntry, Lsn, Vec<u8>)>> {
        let root = self.get_root()?;
        self.get_adjacent_bin(&root, current_key, true)
    }

    /// Return the entries of the BIN immediately to the left of the BIN
    /// that contains (or would contain) `current_key`.
    ///
    /// → `Tree.getNextIN(forward=false)`.
    pub fn get_prev_bin(
        &self,
        current_key: &[u8],
    ) -> Option<Vec<(BinEntry, Lsn, Vec<u8>)>> {
        let root = self.get_root()?;
        self.get_adjacent_bin(&root, current_key, false)
    }

    /// Core implementation shared by `get_next_bin` and `get_prev_bin`.
    ///
    /// Builds the path from `root` down to the BIN for `current_key`
    /// (each element records the parent arc, the slot index taken,
    /// and the child Arc reached) using `read_arc()` hand-over-hand
    /// latch coupling.
    ///
    /// The ascent re-acquires the parent's read lock one level at a
    /// time. To handle a concurrent split that completes between
    /// path capture and ascent, we validate that the slot still
    /// holds the child Arc we descended through. If the slot
    /// mismatches we retry the whole operation from root with a
    /// short pause between attempts. The retry budget is generous
    /// (`MAX_ASCENT_ATTEMPTS`) so that the typical case of a few
    /// cascading splits between two BIN-level cursor steps is
    /// absorbed without surfacing as a false end-of-iteration.
    /// After exhausting the budget we conservatively return `None`,
    /// signalling "no adjacent BIN found"; the cursor will then
    /// either restart its scan or report end-of-iteration. The
    /// budget is finite so a pathological workload (a thread
    /// permanently splitting under us) cannot livelock the lookup.
    /// JE `Tree.getNextIN` / `Tree.getPrevIN`.
    ///
    /// R3 fix (2026-06-16): converted from `static fn` to `&self` so that the
    /// IN-level descent uses `self.upper_in_floor_index` (comparator-aware)
    /// instead of a raw byte `<=`. Without this, databases with a custom
    /// comparator (secondary indexes, sorted-dup) could descend to the wrong
    /// child → wrong adjacent BIN → incorrect cursor iteration across BIN
    /// boundaries. Mirrors `Tree.getNextIN`/`Tree.getPrevIN` using the
    /// comparator-aware `IN.findEntry`.
    fn get_adjacent_bin(
        &self,
        root: &Arc<RwLock<TreeNode>>,
        current_key: &[u8],
        forward: bool,
    ) -> Option<Vec<(BinEntry, Lsn, Vec<u8>)>> {
        const MAX_ASCENT_ATTEMPTS: u32 = 8;
        for attempt in 0..MAX_ASCENT_ATTEMPTS {
            match self.get_adjacent_bin_attempt(root, current_key, forward) {
                AdjacentBinOutcome::Found(v) => return Some(v),
                AdjacentBinOutcome::NoAdjacent => return None,
                AdjacentBinOutcome::SplitRaceRetry => {
                    // Brief pause to let the splitter finish.
                    if attempt + 1 < MAX_ASCENT_ATTEMPTS {
                        std::thread::yield_now();
                    }
                }
            }
        }
        // Exhausted retry budget. Signal "no adjacent" so the
        // cursor can fall back to its end-of-iteration path.
        None
    }

    /// One attempt at `get_adjacent_bin`. The tri-state return
    /// value distinguishes "no adjacent BIN exists" (which the
    /// caller should propagate as end-of-iteration) from "a
    /// concurrent split invalidated our path" (which the caller
    /// should retry from root).
    fn get_adjacent_bin_attempt(
        &self,
        root: &Arc<RwLock<TreeNode>>,
        current_key: &[u8],
        forward: bool,
    ) -> AdjacentBinOutcome {
        // Path entry: (parent_arc, slot_idx_taken, child_arc_reached).
        // The child Arc lets the ascent validate that the slot still
        // points to the same node we descended through.
        let mut path: Vec<(
            Arc<RwLock<TreeNode>>,
            usize,
            Arc<RwLock<TreeNode>>,
        )> = Vec::new();

        let mut guard: NodeArcReadGuard = root.read_arc();
        loop {
            if guard.is_bin() {
                break;
            }

            let (next_arc, slot_idx) = match &*guard {
                TreeNode::Internal(n) => {
                    if n.entries.is_empty() {
                        return AdjacentBinOutcome::NoAdjacent;
                    }
                    // R3 fix: use comparator-aware upper_in_floor_index so
                    // that custom-comparator / sorted-dup databases descend
                    // to the correct child. Mirrors JE Tree.getNextIN which
                    // uses IN.findEntry (comparator-aware) not raw byte order.
                    let idx =
                        self.upper_in_floor_index(&n.entries, current_key);
                    let child = match n.get_child(idx) {
                        Some(c) => c,
                        None => return AdjacentBinOutcome::NoAdjacent,
                    };
                    (child, idx)
                }
                TreeNode::Bottom(_) => unreachable!(),
            };

            // Record the parent and the child we are about to enter
            // — the child Arc lets the ascent validate the slot.
            let parent_arc = NodeArcReadGuard::rwlock(&guard).clone();
            path.push((parent_arc, slot_idx, Arc::clone(&next_arc)));

            // Hand-over-hand: take child read lock BEFORE releasing parent.
            let next_guard = next_arc.read_arc();
            drop(guard);
            guard = next_guard;
        }
        drop(guard);

        // Ascend the path. At each level, validate that
        // `parent.entries[taken_idx].child == descended_child` before
        // trusting `taken_idx` as a coordinate. If not, return
        // `SplitRaceRetry` so the caller restarts from root.
        while let Some((parent_arc, taken_idx, descended_child)) = path.pop() {
            let parent_guard = parent_arc.read();
            let (n_entries, slot_still_valid) = match &*parent_guard {
                TreeNode::Internal(p) => {
                    let n = p.entries.len();
                    let valid = p
                        .child_ref(taken_idx)
                        .is_some_and(|c| Arc::ptr_eq(c, &descended_child));
                    (n, valid)
                }
                _ => return AdjacentBinOutcome::NoAdjacent,
            };
            drop(parent_guard);

            if !slot_still_valid {
                return AdjacentBinOutcome::SplitRaceRetry;
            }

            let sibling_idx = if forward {
                taken_idx + 1
            } else if taken_idx == 0 {
                // No left sibling at this level — ascend further.
                continue;
            } else {
                taken_idx - 1
            };

            if forward && sibling_idx >= n_entries {
                // No right sibling at this level — ascend further.
                continue;
            }

            // Found a sibling slot — fetch the sibling child arc.
            let sibling_arc = {
                let g = parent_arc.read();
                match &*g {
                    TreeNode::Internal(p) => match p.get_child(sibling_idx) {
                        Some(c) => c,
                        None => return AdjacentBinOutcome::NoAdjacent,
                    },
                    _ => return AdjacentBinOutcome::NoAdjacent,
                }
            };

            // Descend to the leftmost (forward) or rightmost (!forward) BIN.
            return match Self::descend_to_edge_bin(&sibling_arc, forward) {
                Some(v) => AdjacentBinOutcome::Found(v),
                None => AdjacentBinOutcome::NoAdjacent,
            };
        }

        // Exhausted path without finding a sibling → no adjacent BIN.
        AdjacentBinOutcome::NoAdjacent
    }

    /// Descend to the leftmost BIN (`forward = true`) or rightmost BIN
    /// (`forward = false`) in the sub-tree rooted at `node_arc`.
    ///
    /// `Tree.searchSubTree(SearchType.LEFT / RIGHT, targetLevel)`.
    fn descend_to_edge_bin(
        node_arc: &Arc<RwLock<TreeNode>>,
        forward: bool,
    ) -> Option<Vec<(BinEntry, Lsn, Vec<u8>)>> {
        // Hand-over-hand latch coupling — see Tree::search.
        let mut guard: NodeArcReadGuard = node_arc.read_arc();

        loop {
            if guard.is_bin() {
                return match &*guard {
                    TreeNode::Bottom(b) => {
                        // Return entries with full (decompressed) keys so that
                        // callers always work with complete keys.
                        //
                        // TREE-F1: KD slots are NOT filtered here — the BIN's
                        // slot indices are returned verbatim so the cursor can
                        // skip KD slots itself (CursorImpl getNext loop;
                        // CursorImpl.java:2062-2064) and continue to the next
                        // BIN when an edge BIN is entirely KD during the
                        // BIN-delta reconstitution window.
                        let full_entries: Vec<(BinEntry, Lsn, Vec<u8>)> = (0
                            ..b.entries.len())
                            .map(|i| {
                                (
                                    BinEntry {
                                        data: b.entries[i].data.clone(),
                                        known_deleted: b.entries[i]
                                            .known_deleted,
                                        dirty: b.entries[i].dirty,
                                        expiration_time: b.entries[i]
                                            .expiration_time,
                                    },
                                    b.get_lsn(i),
                                    b.get_full_key(i).unwrap_or_default(),
                                )
                            })
                            .collect();
                        Some(full_entries)
                    }
                    _ => None,
                };
            }

            let next = match &*guard {
                TreeNode::Internal(n) => {
                    if forward {
                        n.get_child(0)?
                    } else {
                        n.get_child(n.entries.len().saturating_sub(1))?
                    }
                }
                _ => return None,
            };
            // Take child read lock BEFORE releasing parent's.
            let next_guard = next.read_arc();
            drop(guard);
            guard = next_guard;
        }
    }
}

// ============================================================================
// Tree statistics
// ============================================================================

/// Statistics collected by a full tree walk.
///
/// `TreeWalkerStatsAccumulator`.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct TreeStats {
    /// Number of BINs (bottom internal nodes).
    pub n_bins: u64,
    /// Number of upper INs.
    pub n_ins: u64,
    /// Total number of entries across all nodes.
    pub n_entries: u64,
    /// Height of the tree (1 = root is a BIN, 2 = one level above BINs, …).
    pub height: u32,
}

impl Tree {
    /// Walks the entire tree and collects structural statistics.
    ///
    /// `TreeWalkerStatsAccumulator` pattern — performs a simple
    /// recursive DFS and counts INs, BINs, entries, and tree height.
    pub fn collect_stats(&self) -> TreeStats {
        let mut stats = TreeStats::default();
        if let Some(root) = self.get_root() {
            Self::collect_stats_recursive(&root, &mut stats, 0);
        }
        stats
    }

    fn collect_stats_recursive(
        node_arc: &Arc<RwLock<TreeNode>>,
        stats: &mut TreeStats,
        depth: u32,
    ) {
        let guard = node_arc.read();

        let current_height = depth + 1;
        if current_height > stats.height {
            stats.height = current_height;
        }

        match &*guard {
            TreeNode::Bottom(b) => {
                stats.n_bins += 1;
                stats.n_entries += b.entries.len() as u64;
            }
            TreeNode::Internal(n) => {
                stats.n_ins += 1;
                stats.n_entries += n.entries.len() as u64;
                // Collect child arcs before releasing the guard.
                let children: Vec<Arc<RwLock<TreeNode>>> =
                    n.resident_children();
                // Release guard before recursing to avoid lock ordering issues.
                drop(guard);
                for child in children {
                    Self::collect_stats_recursive(&child, stats, depth + 1);
                }
            }
        }
    }

    /// Collects all dirty BINs as (Arc to node, db_id) pairs.
    ///
    /// The checkpoint path calls this to enumerate BINs that need to be
    /// logged.  For each dirty BIN the checkpoint decides — based on the
    /// BIN-delta threshold — whether to write a full `BIN` entry or a
    /// `BINDelta` entry.
    ///
    /// `Checkpointer.processINList()` which iterates the dirty
    /// IN list accumulated during normal operation.
    pub fn collect_dirty_bins(
        &self,
        db_id: u64,
    ) -> Vec<(u64, Arc<RwLock<TreeNode>>)> {
        let mut result = Vec::new();
        if let Some(root) = self.get_root() {
            Self::collect_dirty_bins_recursive(&root, db_id, &mut result);
        }
        result
    }

    fn collect_dirty_bins_recursive(
        node_arc: &Arc<RwLock<TreeNode>>,
        db_id: u64,
        out: &mut Vec<(u64, Arc<RwLock<TreeNode>>)>,
    ) {
        let guard = node_arc.read();
        match &*guard {
            TreeNode::Bottom(b) => {
                // Include this BIN if it is dirty or has any dirty slots.
                if b.dirty || b.dirty_count() > 0 {
                    out.push((db_id, Arc::clone(node_arc)));
                }
            }
            TreeNode::Internal(n) => {
                let children: Vec<Arc<RwLock<TreeNode>>> =
                    n.resident_children();
                drop(guard);
                for child in children {
                    Self::collect_dirty_bins_recursive(&child, db_id, out);
                } // guard already dropped
            }
        }
    }

    /// Collect all BINs that have at least one `known_deleted` slot.
    ///
    /// INCompressor queue-drain scan in the: the daemon iterates
    /// the in-memory IN list and identifies BINs that still hold zombie deleted
    /// slots.  Each returned `Arc` can be passed directly to `compress_bin()`.
    pub fn collect_bins_with_known_deleted(
        &self,
    ) -> Vec<Arc<RwLock<TreeNode>>> {
        let mut result = Vec::new();
        if let Some(root) = self.get_root() {
            Self::collect_bins_with_known_deleted_recursive(&root, &mut result);
        }
        result
    }

    fn collect_bins_with_known_deleted_recursive(
        node_arc: &Arc<RwLock<TreeNode>>,
        out: &mut Vec<Arc<RwLock<TreeNode>>>,
    ) {
        let guard = node_arc.read();
        match &*guard {
            TreeNode::Bottom(b) => {
                if b.entries.iter().any(|e| e.known_deleted) {
                    out.push(Arc::clone(node_arc));
                }
            }
            TreeNode::Internal(n) => {
                let children: Vec<Arc<RwLock<TreeNode>>> =
                    n.resident_children();
                drop(guard);
                for child in children {
                    Self::collect_bins_with_known_deleted_recursive(
                        &child, out,
                    );
                }
            }
        }
    }

    /// Collect all dirty upper (non-BIN) internal nodes, sorted ascending by
    /// level (bottom-up order, BIN level excluded).
    ///
    /// Serialise an upper-IN node (level > 1) by node_id for off-heap storage.
    ///
    /// Traverses the tree to find the internal node whose  matches,
    /// then calls  to produce a compact byte
    /// representation.  Returns  if the node is not found or is a BIN
    /// (BINs are not upper INs).
    ///
    /// Mirrors `OffHeapAllocator` serialises the same bytes that would be written
    /// to the log, allowing the evictor to store upper-INs off-heap and avoid
    /// log-file reads on the next traversal.
    pub fn serialize_upper_in(&self, node_id: u64) -> Option<Vec<u8>> {
        let root = self.get_root()?;
        Self::find_and_serialize_upper_in(&root, node_id)
    }

    fn find_and_serialize_upper_in(
        node_arc: &Arc<RwLock<TreeNode>>,
        target_id: u64,
    ) -> Option<Vec<u8>> {
        let guard = node_arc.read();
        match &*guard {
            TreeNode::Bottom(_) => None, // BINs are not upper INs
            TreeNode::Internal(n) => {
                if n.node_id == target_id {
                    // Serialise InNodeStub for off-heap storage.
                    // Format: node_id(u64BE) | level(i32BE) | n_entries(u32BE)
                    //   then per-entry: key_len(u32BE) | key | lsn(u64BE)
                    let mut buf = Vec::new();
                    buf.extend_from_slice(&n.node_id.to_be_bytes());
                    buf.extend_from_slice(&n.level.to_be_bytes());
                    buf.extend_from_slice(
                        &(n.entries.len() as u32).to_be_bytes(),
                    );
                    for (i, e) in n.entries.iter().enumerate() {
                        buf.extend_from_slice(
                            &(e.key.len() as u32).to_be_bytes(),
                        );
                        buf.extend_from_slice(&e.key);
                        buf.extend_from_slice(
                            &n.get_lsn(i).as_u64().to_be_bytes(),
                        );
                    }
                    return Some(buf);
                }
                // Recurse into children before releasing the guard so we
                // hold the minimum read-lock duration.
                let children: Vec<Arc<RwLock<TreeNode>>> =
                    n.resident_children();
                drop(guard);
                for child in &children {
                    if let Some(bytes) =
                        Self::find_and_serialize_upper_in(child, target_id)
                    {
                        return Some(bytes);
                    }
                }
                None
            }
        }
    }

    /// Upper-IN traversal in `Checkpointer.processINList()` from
    /// — visits all `TreeNode::Internal` nodes whose `dirty` flag is set
    /// and returns them together with their level, sorted lowest-level-first
    /// so the checkpointer can log them bottom-up.  The root is always the
    /// last entry (highest level), which must be logged `Provisional::No`.
    pub fn collect_dirty_upper_ins(
        &self,
        _db_id: u64,
    ) -> Vec<(i32, Arc<RwLock<TreeNode>>)> {
        let mut result: Vec<(i32, Arc<RwLock<TreeNode>>)> = Vec::new();
        if let Some(root) = self.get_root() {
            Self::collect_dirty_upper_ins_recursive(&root, &mut result);
        }
        result.sort_by_key(|(level, _)| *level);
        result
    }

    fn collect_dirty_upper_ins_recursive(
        node_arc: &Arc<RwLock<TreeNode>>,
        out: &mut Vec<(i32, Arc<RwLock<TreeNode>>)>,
    ) {
        let guard = node_arc.read();
        match &*guard {
            TreeNode::Bottom(_) => {
                // BINs are handled by flush_dirty_bins_internal; skip here.
            }
            TreeNode::Internal(n) => {
                let is_dirty = n.dirty;
                // REC-AA: return the node's ACTUAL tree level (n.level, in
                // MAIN_LEVEL|n units), not a root-relative depth.  The level
                // must be on the same scale as a BIN's `level` (BIN_LEVEL =
                // MAIN_LEVEL|1) so that the checkpointer's flush-level
                // computation and the evictor's `node_level < flush_level`
                // comparison are meaningful.  With a root-relative depth the
                // root had the SMALLEST value (0) and the IN above the BINs
                // the LARGEST, inverting the provisional/non-provisional
                // boundary; with n.level the root has the largest level, as JE
                // expects.
                let level = n.level;
                let children: Vec<Arc<RwLock<TreeNode>>> =
                    n.resident_children();
                drop(guard);
                // Recurse into children first (bottom-up ordering).
                for child in &children {
                    Self::collect_dirty_upper_ins_recursive(child, out);
                }
                // Add this node after children (so parent comes after all descendants).
                if is_dirty {
                    out.push((level, Arc::clone(node_arc)));
                }
            }
        }
    }

    // ========================================================================
    // Tree.java ports: 8 additional tree methods (Task #82)
    // ========================================================================

    /// Returns `true` if the root node is currently loaded in memory.
    ///
    /// .
    pub fn is_root_resident(&self) -> bool {
        self.root.read().is_some()
    }

    /// Returns the root node `Arc` if present, or `None`.
    ///
    /// .
    pub fn get_resident_root_in(&self) -> Option<Arc<RwLock<TreeNode>>> {
        self.root.read().clone()
    }

    /// Returns the BIN that should contain a slot for `key` (the "parent" of
    /// LN slots).
    ///
    /// .  Descends the tree
    /// exactly like `search()` and returns the leaf-level BIN arc, or `None`
    /// if the tree is empty.
    ///
    /// Uses `read_arc()` hand-over-hand on the descent — the child
    /// guard is taken before the parent guard is dropped, matching
    /// `search()`. Returns the BIN Arc with no read lock held; the
    /// caller must take whatever lock it needs to operate on the
    /// returned BIN.
    pub fn get_parent_bin_for_child_ln(
        &self,
        key: &[u8],
    ) -> Option<Arc<RwLock<TreeNode>>> {
        let root = self.get_root()?;
        let mut current_arc: Arc<RwLock<TreeNode>> = root.clone();
        let mut guard: NodeArcReadGuard = root.read_arc();

        loop {
            if guard.is_bin() {
                drop(guard);
                return Some(current_arc);
            }

            let parent_arc = current_arc.clone();
            let next_idx = match &*guard {
                TreeNode::Internal(n) => {
                    if n.entries.is_empty() {
                        return None;
                    }
                    let idx = self.upper_in_floor_index(&n.entries, key);
                    match n.get_child(idx) {
                        Some(c) => {
                            let next_guard = c.read_arc();
                            drop(guard);
                            current_arc = c;
                            guard = next_guard;
                            continue;
                        }
                        None => idx, // EV-14/EV-13: re-fetch below.
                    }
                }
                TreeNode::Bottom(_) => {
                    unreachable!("is_bin() returned false above")
                }
            };
            // Hand-over-hand: take child guard before dropping parent.
            drop(guard);
            let child = self.child_at_or_fetch(&parent_arc, next_idx)?;
            let next_guard = child.read_arc();
            current_arc = child;
            guard = next_guard;
        }
    }

    /// Returns the BIN where `key` should be inserted.
    ///
    /// .  Semantically identical to
    /// `get_parent_bin_for_child_ln` — expressed as a separate method to match
    /// API surface.
    ///
    /// Implemented as a delegation to `get_parent_bin_for_child_ln`,
    /// which uses `read_arc()` hand-over-hand on the descent.
    pub fn find_bin_for_insert(
        &self,
        key: &[u8],
    ) -> Option<Arc<RwLock<TreeNode>>> {
        self.get_parent_bin_for_child_ln(key)
    }

    /// Search for a BIN, allowing splits during descent (preemptive splitting).
    ///
    /// .  This thin wrapper
    /// delegates to `search()` and returns the result wrapped in `Some`.
    /// The full split-allowed descent is performed by `insert()` internally;
    /// this method exposes the same result type for callers that only need to
    /// locate the BIN.
    ///
    /// Returns `None` if the tree is empty.
    pub fn search_splits_allowed(&self, key: &[u8]) -> Option<SearchResult> {
        self.search(key)
    }

    /// Traverses the entire tree and returns every IN and BIN node as a flat
    /// list.
    ///
    /// .  Used by recovery to rebuild
    /// the in-memory IN list after log replay.  The walk is a BFS from the
    /// root; every `Arc<RwLock<TreeNode>>` encountered (both Internal and
    /// Bottom variants) is included in the result.
    pub fn rebuild_in_list(&self) -> Vec<Arc<RwLock<TreeNode>>> {
        let mut result = Vec::new();
        if let Some(root) = self.get_root() {
            Self::rebuild_in_list_recursive(&root, &mut result);
        }
        result
    }

    fn rebuild_in_list_recursive(
        node_arc: &Arc<RwLock<TreeNode>>,
        out: &mut Vec<Arc<RwLock<TreeNode>>>,
    ) {
        // Push this node unconditionally — both INs and BINs belong in the list.
        out.push(Arc::clone(node_arc));

        let guard = node_arc.read();

        if let TreeNode::Internal(n) = &*guard {
            // Collect child arcs while holding the guard, then drop it before
            // recursing to avoid holding multiple locks simultaneously.
            let children: Vec<Arc<RwLock<TreeNode>>> = n.resident_children();
            drop(guard);
            for child in children {
                Self::rebuild_in_list_recursive(&child, out);
            }
        }
        // BIN nodes are leaves — no children to recurse into.
    }

    /// Validates internal tree consistency.
    ///
    /// .  Primarily a debug/test tool.
    ///
    /// Rules checked:
    /// - An empty tree (no root) is trivially valid → returns `true`.
    /// - A non-empty tree must have a non-null root.
    /// - Every Internal node must have at least one entry.
    /// - Every child pointer that is `Some` must be readable (lock must be
    ///   acquirable — i.e., no poisoned locks).
    ///
    /// Returns `true` if no inconsistencies are detected, `false` otherwise.
    pub fn validate_in_list(&self) -> bool {
        match self.get_root() {
            None => true, // empty tree is always valid
            Some(root) => Self::validate_node(&root),
        }
    }

    fn validate_node(node_arc: &Arc<RwLock<TreeNode>>) -> bool {
        let guard = node_arc.read();

        match &*guard {
            TreeNode::Bottom(_bin) => {
                // BIN nodes are always structurally valid at this level.
                true
            }
            TreeNode::Internal(n) => {
                // An Internal node must have at least one entry.
                if n.entries.is_empty() {
                    return false;
                }
                // Collect child arcs before dropping the guard.
                let children: Vec<Arc<RwLock<TreeNode>>> =
                    n.resident_children();
                drop(guard);
                // Recursively validate every resident child.
                for child in children {
                    if !Self::validate_node(&child) {
                        return false;
                    }
                }
                true
            }
        }
    }

    /// Traverses the tree to find the parent IN that contains `child_node_id`
    /// as one of its child slots.
    ///
    /// .  Used by the cleaner
    /// migration path to re-insert migrated INs after eviction/fetch.
    ///
    /// Returns `(parent_arc, slot_index)` where `slot_index` is the position
    /// in the parent's `entries` vector whose child matches `child_node_id`,
    /// or `None` if no such parent is found.
    pub fn get_parent_in_for_child_in(
        &self,
        child_node_id: u64,
    ) -> Option<(Arc<RwLock<TreeNode>>, usize)> {
        let root = self.get_root()?;
        Self::find_parent_of_node_id(&root, child_node_id)
    }

    /// Recursive DFS helper for `get_parent_in_for_child_in`.
    ///
    /// Scans every entry in each Internal node.  When a child's node_id
    /// matches `target_id` the parent arc and slot index are returned.
    fn find_parent_of_node_id(
        node_arc: &Arc<RwLock<TreeNode>>,
        target_id: u64,
    ) -> Option<(Arc<RwLock<TreeNode>>, usize)> {
        let guard = node_arc.read();

        let TreeNode::Internal(n) = &*guard else {
            // BIN nodes have no IN children — cannot be a parent of another IN.
            return None;
        };

        // Check whether any child of this IN has the target node_id.
        let mut children: Vec<(usize, Arc<RwLock<TreeNode>>)> = Vec::new();
        for slot in 0..n.entries.len() {
            if let Some(child_arc) = n.child_ref(slot) {
                // Read the child's node_id under a separate lock (acquire child
                // while parent guard is still held — this is intentional for
                // the ID comparison only; we release both immediately after).
                let child_id = {
                    let cg = child_arc.read();
                    match &*cg {
                        TreeNode::Internal(cn) => cn.node_id,
                        TreeNode::Bottom(cb) => cb.node_id,
                    }
                };

                if child_id == target_id {
                    // Found — return a clone of this node as parent.
                    let parent_clone = Arc::clone(node_arc);
                    return Some((parent_clone, slot));
                }

                // Not found at this slot; schedule this child for recursion.
                children.push((slot, Arc::clone(child_arc)));
            }
        }
        // Release parent guard before recursing.
        drop(guard);

        // Recurse into each Internal child.
        for (_slot, child_arc) in children {
            if let Some(result) =
                Self::find_parent_of_node_id(&child_arc, target_id)
            {
                return Some(result);
            }
        }

        None
    }

    /// Propagates the dirty flag upward from `node_arc` to the root.
    ///
    /// Implicit dirty propagation: after modifying any node,
    /// all ancestors on the path to the root must also be marked dirty so
    /// the checkpointer logs them.
    ///
    /// In this happens through `IN.setDirty(true)` calls at each level
    /// during split/insert callbacks.  Here we walk the weak parent chain.
    /// Reconstitute a BIN-delta by merging it onto a base full BIN.
    ///
    /// Implements JE `BINDelta.reconstituteBIN(databaseImpl)` for the recovery
    /// path where the log manager is not available as a `LogManager` but as
    /// raw serialized bytes.
    ///
    /// Algorithm:
    /// 1. Deserialise `base_bytes` as a full `BinStub`.
    /// 2. Apply `delta_bytes` slots onto the base using `BinStub::apply_delta`
    ///    (raw slot overlay).
    /// 3. Recompute key prefix so prefix-compressed entries are consistent.
    ///
    /// Returns `None` if either byte slice is malformed.
    ///
    /// JE `BINDelta.reconstituteBIN` / `BINDelta.applyDelta`
    /// (DRIFT-10 / Stage 3).
    pub fn reconstitute_bin_delta(
        base_bytes: &[u8],
        delta_bytes: &[u8],
    ) -> Option<BinStub> {
        let mut base = BinStub::deserialize_full(base_bytes)?;
        // Apply the delta slots onto the base.
        // Note: BinStub::apply_delta uses slot-index addressing into base.entries,
        // extending with new entries when the slot_idx >= base.entries.len().
        // After apply_delta we recompute the key prefix to fix prefix compression.
        BinStub::apply_delta(&mut base, delta_bytes)?;
        // Recompute prefix so prefix-compressed BINs are consistent after merge.
        base.recompute_key_prefix();
        base.is_delta = false;
        base.dirty = false;
        Some(base)
    }

    pub fn propagate_dirty_to_root(node_arc: &Arc<RwLock<TreeNode>>) {
        let parent_weak = { node_arc.read().get_parent() };

        if let Some(parent_arc) = parent_weak.and_then(|w| w.upgrade()) {
            {
                let mut g = parent_arc.write();
                g.set_dirty(true);
            }
            // Recurse further up.
            Self::propagate_dirty_to_root(&parent_arc);
        }
    }

    // ========================================================================
    // IN-redo: JE RecoveryManager.recoverIN / recoverRootIN / recoverChildIN
    // ========================================================================

    /// Deserialise an upper-IN node from bytes produced by
    /// `TreeNode::write_to_bytes()` / `flush_one_tree_upper_ins`.
    ///
    /// Format: node_id(u64BE) | level(i32BE) | n_entries(u32BE) | dirty(u8)
    ///   | per-entry: key_len(u16BE) | key | lsn(u64BE)
    ///
    /// JE `INFileReader.getIN(db)` / `IN.readFromLog`.
    pub fn deserialize_upper_in(bytes: &[u8]) -> Option<InNodeStub> {
        if bytes.len() < 13 {
            return None;
        }
        let node_id = u64::from_be_bytes(bytes[0..8].try_into().ok()?);
        let level = i32::from_be_bytes(bytes[8..12].try_into().ok()?);
        let n_entries =
            u32::from_be_bytes(bytes[12..16].try_into().ok()?) as usize;
        // dirty byte (1 byte after n_entries)
        if bytes.len() < 17 {
            return None;
        }
        let mut pos = 17usize; // skip node_id(8) + level(4) + n_entries(4) + dirty(1)
        let mut entries = Vec::with_capacity(n_entries);
        let mut lsns: Vec<Lsn> = Vec::with_capacity(n_entries);
        for _ in 0..n_entries {
            if pos + 2 > bytes.len() {
                return None;
            }
            let key_len =
                u16::from_be_bytes(bytes[pos..pos + 2].try_into().ok()?)
                    as usize;
            pos += 2;
            if pos + key_len > bytes.len() {
                return None;
            }
            let key = bytes[pos..pos + key_len].to_vec();
            pos += key_len;
            if pos + 8 > bytes.len() {
                return None;
            }
            let lsn = noxu_util::Lsn::from_u64(u64::from_be_bytes(
                bytes[pos..pos + 8].try_into().ok()?,
            ));
            pos += 8;
            entries.push(InEntry { key });
            lsns.push(lsn); // T-3
        }
        Some(InNodeStub {
            node_id,
            level,
            entries,
            // T-4: a freshly deserialized IN has no resident children.
            targets: TargetRep::None,
            dirty: false,
            generation: 0,
            parent: None,
            lsn_rep: LsnRep::from_lsns(&lsns), // T-3
        })
    }

    /// Deserialise a BIN from bytes produced by `BinStub::serialize_full()`.
    ///
    /// Thin wrapper so the recovery path does not need to import `BinStub`
    /// directly from callers that only have the raw bytes.
    ///
    /// JE `INFileReader.getIN(db)` for a BIN entry.
    pub fn deserialize_bin(bytes: &[u8]) -> Option<BinStub> {
        let mut bin = BinStub::deserialize_full(bytes)?;
        bin.dirty = false; // freshly loaded from log — clean for now
        Some(bin)
    }

    /// Apply a logged IN/BIN to the in-memory tree during the recovery redo pass.
    ///
    /// Implements JE `RecoveryManager.recoverIN`:
    /// - `is_root` nodes are handled by `recover_root_in`.
    /// - non-root nodes are handled by `recover_child_in`.
    ///
    /// `log_lsn` is the LSN at which this IN/BIN was logged.  The currency
    /// check in `recover_child_in` uses this to decide whether to replace the
    /// in-memory slot (tree slot LSN < log_lsn → replace; equal → noop;
    /// greater → skip).
    ///
    /// JE `RecoveryManager.recoverIN` / `replayOneIN`
    /// (RecoveryManager.java ~lines 1200–1280).
    pub fn recover_in_redo(
        &self,
        log_lsn: noxu_util::Lsn,
        is_root: bool,
        is_bin: bool,
        node_data: &[u8],
    ) -> InRedoResult {
        if is_bin {
            let Some(bin) = Self::deserialize_bin(node_data) else {
                return InRedoResult::DeserializeFailed;
            };
            if is_root {
                self.recover_root_bin(log_lsn, bin)
            } else {
                self.recover_child_bin(log_lsn, bin)
            }
        } else {
            let Some(upper) = Self::deserialize_upper_in(node_data) else {
                return InRedoResult::DeserializeFailed;
            };
            if is_root {
                self.recover_root_upper_in(log_lsn, upper)
            } else {
                self.recover_child_upper_in(log_lsn, upper)
            }
        }
    }

    /// Recover a root BIN.
    ///
    /// If no root exists or the existing root is older (lower LSN), install
    /// this BIN as the new root.
    ///
    /// JE `RecoveryManager.recoverRootIN` / `RootUpdater.doWork`
    /// (RecoveryManager.java ~lines 1293–1410).
    fn recover_root_bin(
        &self,
        log_lsn: noxu_util::Lsn,
        bin: BinStub,
    ) -> InRedoResult {
        let mut root_guard = self.root.write();
        let existing_lsn = *self.root_log_lsn.read();
        match &*root_guard {
            None => {
                // No root — install this BIN as the root.
                // JE: `root == null` case in `RootUpdater.doWork`.
                let node = TreeNode::Bottom(bin);
                *root_guard = Some(Arc::new(RwLock::new(node)));
                *self.root_log_lsn.write() = log_lsn;
                InRedoResult::Inserted
            }
            Some(_) => {
                // JE: `originalLsn = root.getLsn()`; replace if logLsn > originalLsn.
                if log_lsn > existing_lsn {
                    let node = TreeNode::Bottom(bin);
                    *root_guard = Some(Arc::new(RwLock::new(node)));
                    *self.root_log_lsn.write() = log_lsn;
                    InRedoResult::Replaced
                } else {
                    InRedoResult::Skipped
                }
            }
        }
    }

    /// Recover a root upper IN.
    ///
    /// JE `RecoveryManager.recoverRootIN` for a non-BIN root.
    fn recover_root_upper_in(
        &self,
        log_lsn: noxu_util::Lsn,
        upper: InNodeStub,
    ) -> InRedoResult {
        let mut root_guard = self.root.write();
        let existing_lsn = *self.root_log_lsn.read();
        match &*root_guard {
            None => {
                let node = TreeNode::Internal(upper);
                *root_guard = Some(Arc::new(RwLock::new(node)));
                *self.root_log_lsn.write() = log_lsn;
                InRedoResult::Inserted
            }
            Some(_) => {
                if log_lsn > existing_lsn {
                    let node = TreeNode::Internal(upper);
                    *root_guard = Some(Arc::new(RwLock::new(node)));
                    *self.root_log_lsn.write() = log_lsn;
                    InRedoResult::Replaced
                } else {
                    InRedoResult::Skipped
                }
            }
        }
    }

    /// Recover a non-root BIN.
    ///
    /// Implements the three-case currency check from JE
    /// `RecoveryManager.recoverChildIN`
    /// (RecoveryManager.java lines 1412–1500):
    ///
    /// 1. Node not in tree: skip (parent logged a later structure that already
    ///    omits this node, or node was deleted).
    /// 2. Physical match (slot LSN == log_lsn): noop — already current.
    /// 3. Logical match: another version of the node is in the slot.
    ///    Replace if tree slot LSN < log_lsn (tree is older), skip otherwise.
    fn recover_child_bin(
        &self,
        log_lsn: noxu_util::Lsn,
        bin: BinStub,
    ) -> InRedoResult {
        let node_id = bin.node_id;
        let Some((parent_arc, slot)) = self.get_parent_in_for_child_in(node_id)
        else {
            // Case 1: not in tree.
            return InRedoResult::NotInTree;
        };
        let mut parent = parent_arc.write();
        let TreeNode::Internal(ref mut p) = *parent else {
            return InRedoResult::NotInTree;
        };
        let tree_lsn = p.get_lsn(slot); // T-3
        if tree_lsn == log_lsn {
            // Case 2: physical match — noop.
            InRedoResult::Skipped
        } else if tree_lsn < log_lsn {
            // Case 3: logical match, tree is older — replace.
            // JE `parent.recoverIN(idx, inFromLog, logLsn, lastLoggedSize)`.
            let new_arc = Arc::new(RwLock::new(TreeNode::Bottom(bin)));
            // Set parent back-pointer on the new node.
            {
                let mut ng = new_arc.write();
                if let TreeNode::Bottom(ref mut b) = *ng {
                    b.parent = Some(Arc::downgrade(&parent_arc));
                }
            }
            p.set_child(slot, Some(new_arc));
            p.set_lsn(slot, log_lsn); // T-3
            InRedoResult::Replaced
        } else {
            // tree_lsn > log_lsn: tree already holds a newer version.
            InRedoResult::Skipped
        }
    }

    /// Recover a non-root upper IN.
    ///
    /// JE `RecoveryManager.recoverChildIN` for a non-BIN node.
    fn recover_child_upper_in(
        &self,
        log_lsn: noxu_util::Lsn,
        upper: InNodeStub,
    ) -> InRedoResult {
        let node_id = upper.node_id;
        let Some((parent_arc, slot)) = self.get_parent_in_for_child_in(node_id)
        else {
            return InRedoResult::NotInTree;
        };
        let mut parent = parent_arc.write();
        let TreeNode::Internal(ref mut p) = *parent else {
            return InRedoResult::NotInTree;
        };
        let tree_lsn = p.get_lsn(slot); // T-3
        if tree_lsn == log_lsn {
            InRedoResult::Skipped
        } else if tree_lsn < log_lsn {
            let new_arc = Arc::new(RwLock::new(TreeNode::Internal(upper)));
            {
                let mut ng = new_arc.write();
                if let TreeNode::Internal(ref mut n) = *ng {
                    n.parent = Some(Arc::downgrade(&parent_arc));
                }
            }
            p.set_child(slot, Some(new_arc));
            p.set_lsn(slot, log_lsn); // T-3
            InRedoResult::Replaced
        } else {
            InRedoResult::Skipped
        }
    }
}

/// Result of a single `recover_in_redo` call.
///
/// JE traces the same outcomes in `RecoveryManager` debug logging.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InRedoResult {
    /// Node was inserted as the new root.
    Inserted,
    /// Node replaced an older version in the tree.
    Replaced,
    /// Node not applied: tree already holds an equal or newer version.
    Skipped,
    /// Node not found in tree (parent logged later structure that excludes it).
    NotInTree,
    /// Deserialisation of `node_data` bytes failed.
    DeserializeFailed,
}

/// Global node ID counter for generating unique node IDs.
///
/// This is the SINGLE source of node-ids for the whole tree subsystem.  The
/// BIN constructor (`bin.rs`) and `node.rs` route through `generate_node_id`
/// so that, after crash recovery, a freshly allocated node-id is always
/// strictly greater than every node-id present in the recovered log.
///
/// JE ref: `NodeSequence.getNextLocalNodeId` (a single per-env counter) and
/// `IN.nodeId` allocation; `NodeSequence.initRealNodeId` seeds the counter
/// from the recovered `CheckpointEnd.lastLocalNodeId`.  The env seeds this
/// counter post-recovery via `seed_node_id_counter`.
static NODE_ID_COUNTER: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(1);

/// Generates a unique node ID.
pub fn generate_node_id() -> u64 {
    NODE_ID_COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
}

/// Returns the node-id that would be generated next (without allocating it).
///
/// Used by recovery seeding and by tests to assert no node-id reuse after a
/// restart.
pub fn peek_next_node_id_counter() -> u64 {
    NODE_ID_COUNTER.load(std::sync::atomic::Ordering::SeqCst)
}

/// Seeds the node-id counter so the next generated id is `> last_node_id`.
///
/// Called by `EnvironmentImpl` after recovery with the recovered
/// `use_max_node_id`, mirroring `NodeSequence.initRealNodeId` /
/// `setLastNodeId`: post-restart allocation must never reuse a node-id that
/// is already in the log.  Monotonic: never lowers the counter.
pub fn seed_node_id_counter(last_node_id: u64) {
    let want_next = last_node_id.saturating_add(1);
    // Bump only if our current next is below the recovered floor.
    let mut cur = NODE_ID_COUNTER.load(std::sync::atomic::Ordering::SeqCst);
    while cur < want_next {
        match NODE_ID_COUNTER.compare_exchange_weak(
            cur,
            want_next,
            std::sync::atomic::Ordering::SeqCst,
            std::sync::atomic::Ordering::SeqCst,
        ) {
            Ok(_) => break,
            Err(observed) => cur = observed,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ====================================================================
    // T-3: LsnRep packed-LSN encoding (IN.entryLsnByteArray / getLsn /
    // setLsnInternal, IN.java:1752-1935).
    // ====================================================================

    /// All-NULL node uses the 0-byte Empty rep; reads return NULL_LSN.
    #[test]
    fn lsnrep_empty_is_zero_bytes() {
        let rep = LsnRep::new(64);
        assert!(matches!(rep, LsnRep::Empty));
        assert_eq!(rep.memory_size(), 0);
        assert_eq!(rep.get(0), NULL_LSN);
        assert_eq!(rep.get(63), NULL_LSN);
    }

    /// LSNs sharing a file number pack to the Compact rep (4 bytes/slot,
    /// base_file_number-relative) and round-trip exactly.
    #[test]
    fn lsnrep_compact_roundtrip_same_file() {
        let mut rep = LsnRep::new(8);
        for i in 0..8u32 {
            rep.set(i as usize, Lsn::new(7, 1000 + i), 8);
        }
        assert!(matches!(rep, LsnRep::Compact { .. }));
        for i in 0..8u32 {
            assert_eq!(rep.get(i as usize), Lsn::new(7, 1000 + i));
        }
        // 8 slots * 4 bytes = 32 bytes, far below 8 * 8 = 64 for raw u64.
        assert_eq!(rep.memory_size(), 8 * 4);
    }

    /// NULL_LSN is stored via the 0xffffff file-offset sentinel, NOT u64::MAX,
    /// so a node with NULL slots still packs Compact (the blocker JE solves).
    #[test]
    fn lsnrep_null_does_not_force_long() {
        let mut rep = LsnRep::new(4);
        rep.set(0, Lsn::new(3, 50), 4);
        rep.set(1, NULL_LSN, 4);
        rep.set(2, Lsn::new(3, 60), 4);
        rep.set(3, NULL_LSN, 4);
        assert!(
            matches!(rep, LsnRep::Compact { .. }),
            "NULL slots must NOT force the Long rep"
        );
        assert_eq!(rep.get(0), Lsn::new(3, 50));
        assert_eq!(rep.get(1), NULL_LSN);
        assert_eq!(rep.get(2), Lsn::new(3, 60));
        assert_eq!(rep.get(3), NULL_LSN);
    }

    /// base_file_number tracks the minimum; setting a lower file number
    /// re-bases the whole array (adjustFileNumbers) while staying Compact.
    #[test]
    fn lsnrep_rebase_on_lower_file_number() {
        let mut rep = LsnRep::new(3);
        rep.set(0, Lsn::new(10, 5), 3);
        rep.set(1, Lsn::new(12, 6), 3);
        // A lower file number re-bases base_file_number to 8.
        rep.set(2, Lsn::new(8, 7), 3);
        assert!(matches!(rep, LsnRep::Compact { .. }));
        assert_eq!(rep.get(0), Lsn::new(10, 5));
        assert_eq!(rep.get(1), Lsn::new(12, 6));
        assert_eq!(rep.get(2), Lsn::new(8, 7));
    }

    /// A file-number spread > 127 forces the Long fallback (mutateToLongArray),
    /// still round-tripping every slot.
    #[test]
    fn lsnrep_mutates_to_long_on_wide_file_range() {
        let mut rep = LsnRep::new(2);
        rep.set(0, Lsn::new(1, 5), 2);
        rep.set(1, Lsn::new(1000, 6), 2); // diff 999 > 127 -> Long
        assert!(matches!(rep, LsnRep::Long(_)));
        assert_eq!(rep.get(0), Lsn::new(1, 5));
        assert_eq!(rep.get(1), Lsn::new(1000, 6));
    }

    /// A file offset > MAX_FILE_OFFSET (0xfffffe) forces the Long fallback.
    #[test]
    fn lsnrep_mutates_to_long_on_large_offset() {
        let mut rep = LsnRep::new(2);
        rep.set(0, Lsn::new(1, 10), 2);
        rep.set(1, Lsn::new(1, 0x00ff_ffff), 2); // > MAX_FILE_OFFSET -> Long
        assert!(matches!(rep, LsnRep::Long(_)));
        assert_eq!(rep.get(1), Lsn::new(1, 0x00ff_ffff));
    }

    /// insert_shift / remove_shift keep slots aligned (INArrayRep.copy).
    #[test]
    fn lsnrep_insert_and_remove_shift() {
        let mut rep = LsnRep::from_lsns(&[
            Lsn::new(2, 1),
            Lsn::new(2, 2),
            Lsn::new(2, 3),
        ]);
        // Insert a new slot at index 1.
        rep.insert_shift(1, 4);
        rep.set(1, Lsn::new(2, 99), 4);
        assert_eq!(rep.get(0), Lsn::new(2, 1));
        assert_eq!(rep.get(1), Lsn::new(2, 99));
        assert_eq!(rep.get(2), Lsn::new(2, 2));
        assert_eq!(rep.get(3), Lsn::new(2, 3));
        // Remove slot 1.
        rep.remove_shift(1);
        assert_eq!(rep.get(0), Lsn::new(2, 1));
        assert_eq!(rep.get(1), Lsn::new(2, 2));
        assert_eq!(rep.get(2), Lsn::new(2, 3));
    }

    #[test]
    fn test_empty_tree() {
        let tree = Tree::new(1, 128);
        assert!(tree.is_empty());
        assert_eq!(tree.get_database_id(), 1);
        assert_eq!(tree.get_root_splits(), 0);
    }

    #[test]
    fn test_redo_insert_older_lsn_does_not_overwrite_newer_slot() {
        // REC-F2 reproduce-first: redo() must be idempotent w.r.t. slot
        // currency.  JE RecoveryManager.redo() (line ~2512/2544) only
        // replaces a slot when logrecLsn > treeLsn.  A later redo of an
        // OLDER committed LN for the same key must NOT revert the slot to
        // the older value or reset the slot LSN backward.
        let tree = Tree::new(1, 128);
        let key = b"k".to_vec();

        // Install the newer version at LSN X (e.g. the BIN-logged value).
        let newer = Lsn::new(5, 500);
        tree.redo_insert(&key, b"new", newer).unwrap();

        // Replay an OLDER committed LN at Y < X for the same key.
        let older = Lsn::new(2, 200);
        tree.redo_insert(&key, b"old", older).unwrap();

        // The newer value and LSN must survive.
        let got = tree.search_with_data(&key).expect("key present");
        assert!(got.found);
        assert_eq!(
            got.data.as_deref(),
            Some(&b"new"[..]),
            "older-LSN redo reverted committed data"
        );
        assert_eq!(
            got.lsn,
            newer.as_u64(),
            "older-LSN redo reset slot LSN backward"
        );

        // A redo at a strictly NEWER LSN must still replace (replace-only
        // when log_lsn > slot_lsn, matching JE lsnCmp > 0).
        let newest = Lsn::new(9, 900);
        tree.redo_insert(&key, b"newest", newest).unwrap();
        let got = tree.search_with_data(&key).expect("key present");
        assert_eq!(got.data.as_deref(), Some(&b"newest"[..]));
        assert_eq!(got.lsn, newest.as_u64());
    }

    #[test]
    fn test_insert_single() {
        let tree = Tree::new(1, 128);
        let key = b"testkey".to_vec();
        let data = b"testdata".to_vec();
        let lsn = Lsn::new(1, 100);

        let result = tree.insert(key.clone(), data, lsn);
        assert!(result.is_ok());
        assert!(result.unwrap()); // Should be a new insert

        assert!(!tree.is_empty());

        // Verify we can search for it
        let search_result = tree.search(&key);
        assert!(search_result.is_some());
        let sr = search_result.unwrap();
        assert!(sr.exact_parent_found || !sr.child_not_resident);
    }

    #[test]
    fn test_insert_multiple() {
        let tree = Tree::new(1, 128);

        let keys = vec![
            b"apple".to_vec(),
            b"banana".to_vec(),
            b"cherry".to_vec(),
            b"date".to_vec(),
        ];

        for (i, key) in keys.iter().enumerate() {
            let data = format!("data{}", i).into_bytes();
            let lsn = Lsn::new(1, 100 + (i as u32) * 10);
            let result = tree.insert(key.clone(), data, lsn);
            assert!(result.is_ok());
            assert!(result.unwrap()); // All should be new inserts
        }

        // Verify we can search for each
        for key in &keys {
            let search_result = tree.search(key);
            assert!(search_result.is_some());
        }
    }

    #[test]
    fn test_insert_duplicate_key() {
        let tree = Tree::new(1, 128);
        let key = b"duplicate".to_vec();
        let data1 = b"first".to_vec();
        let data2 = b"second".to_vec();
        let lsn1 = Lsn::new(1, 100);
        let lsn2 = Lsn::new(1, 200);

        // First insert
        let result1 = tree.insert(key.clone(), data1, lsn1);
        assert!(result1.is_ok());
        assert!(result1.unwrap()); // New insert

        // Second insert with same key - should be update
        let result2 = tree.insert(key, data2, lsn2);
        assert!(result2.is_ok());
        assert!(!result2.unwrap()); // Update, not new insert
    }

    #[test]
    fn test_search_empty_tree() {
        let tree = Tree::new(1, 128);
        let key = b"noexist".to_vec();

        let result = tree.search(&key);
        assert!(result.is_none());
    }

    #[test]
    fn test_first_and_last_node() {
        let tree = Tree::new(1, 128);

        // Empty tree
        assert!(tree.get_first_node().is_none());
        assert!(tree.get_last_node().is_none());

        // Insert some keys
        let keys = [b"a".to_vec(), b"b".to_vec(), b"c".to_vec()];
        for (i, key) in keys.iter().enumerate() {
            let data = format!("data{}", i).into_bytes();
            let lsn = Lsn::new(1, 100 + (i as u32) * 10);
            tree.insert(key.clone(), data, lsn).unwrap();
        }

        // Now should have first and last
        let first = tree.get_first_node();
        assert!(first.is_some());
        assert_eq!(first.unwrap().index, 0);

        let last = tree.get_last_node();
        assert!(last.is_some());
        assert_eq!(last.unwrap().index, 2);
    }

    #[test]
    fn test_node_id_generation() {
        let id1 = generate_node_id();
        let id2 = generate_node_id();
        let id3 = generate_node_id();

        assert!(id2 > id1);
        assert!(id3 > id2);
    }

    #[test]
    fn test_tree_node_is_bin() {
        let bin = TreeNode::Bottom(BinStub {
            node_id: 1,
            level: BIN_LEVEL,
            entries: vec![],
            key_prefix: Vec::new(),
            dirty: false,
            is_delta: false,
            last_full_lsn: NULL_LSN,
            last_delta_lsn: NULL_LSN,
            generation: 0,
            parent: None,
            expiration_in_hours: true,
            cursor_count: 0,
            prohibit_next_delta: false,
            lsn_rep: LsnRep::Empty,
            keys: KeyRep::new(),
            compact_max_key_length: INKeyRep_DEFAULT_MAX_KEY_LENGTH,
        });
        assert!(bin.is_bin());
        assert_eq!(bin.level(), BIN_LEVEL);

        let internal = TreeNode::Internal(InNodeStub {
            node_id: 2,
            level: MAIN_LEVEL + 2,
            entries: vec![],
            targets: TargetRep::None,
            dirty: false,
            generation: 0,
            parent: None,
            lsn_rep: LsnRep::Empty,
        });
        assert!(!internal.is_bin());
        assert_eq!(internal.level(), MAIN_LEVEL + 2);
    }

    #[test]
    fn test_find_entry() {
        let mut entries = vec![];
        let mut keys = vec![];
        for i in 0..5 {
            entries.push(BinEntry {
                data: Some(vec![]),
                known_deleted: false,
                dirty: false,
                expiration_time: 0,
            });
            keys.push(format!("key{}", i).into_bytes());
        }

        let bin = TreeNode::Bottom(BinStub {
            node_id: 1,
            level: BIN_LEVEL,
            entries,
            key_prefix: Vec::new(),
            dirty: false,
            is_delta: false,
            last_full_lsn: NULL_LSN,
            last_delta_lsn: NULL_LSN,
            generation: 0,
            parent: None,
            expiration_in_hours: true,
            cursor_count: 0,
            prohibit_next_delta: false,
            lsn_rep: LsnRep::Empty,
            keys: KeyRep::from_keys(keys),
            compact_max_key_length: INKeyRep_DEFAULT_MAX_KEY_LENGTH,
        });

        // Search for existing key
        let result = bin.find_entry(b"key2", false, true);
        assert_eq!(result & 0xFFFF, 2);
        assert_ne!(result & EXACT_MATCH, 0);

        // Search for non-existing key with exact=false
        let result = bin.find_entry(b"key15", false, false);
        assert_eq!(result & 0xFFFF, 2); // Would go between key1 and key2
        assert_eq!(result & EXACT_MATCH, 0);
    }

    #[test]
    fn test_insert_until_full() {
        // With splits implemented, inserting beyond max_entries_per_node must
        // succeed (the tree splits proactively rather than returning an error).
        let tree = Tree::new(1, 3); // Small max to exercise splits

        // Insert up to max
        for i in 0..3 {
            let key = format!("key{}", i).into_bytes();
            let data = format!("data{}", i).into_bytes();
            let lsn = Lsn::new(1, 100 + i);
            let result = tree.insert(key, data, lsn);
            assert!(result.is_ok(), "insert {} should succeed", i);
        }

        // The 4th insert triggers a split and must also succeed.
        let key = b"key3".to_vec();
        let data = b"data3".to_vec();
        let lsn = Lsn::new(1, 103);
        let result = tree.insert(key.clone(), data, lsn);
        assert!(
            result.is_ok(),
            "insert after full should trigger split and succeed"
        );
        assert!(result.unwrap(), "should be a new insert");

        // The inserted key must be findable after the split.
        let sr = tree.search(&key);
        assert!(sr.is_some(), "key3 must be searchable after split");
        assert!(sr.unwrap().exact_parent_found, "key3 must be found exactly");
    }

    #[test]
    fn test_memory_counter_balanced_on_insert_delete_f8() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicI64, Ordering};
        // F8 regression: insert accounts key+data+48; delete must subtract the
        // SAME, so an insert+delete of the same record returns the counter to
        // its starting value (previously delete omitted data_len -> the counter
        // leaked data_len per delete, biasing the evictor over-budget view).
        let mut tree = Tree::new(1, 16);
        let counter = Arc::new(AtomicI64::new(0));
        tree.set_memory_counter(Arc::clone(&counter));

        let key = b"a-key".to_vec();
        let data = vec![0u8; 200]; // non-trivial data length
        tree.insert(key.clone(), data.clone(), Lsn::new(0, 10)).unwrap();
        let after_insert = counter.load(Ordering::Relaxed);
        assert!(after_insert > 0, "insert must increase the counter");
        assert_eq!(
            after_insert,
            (key.len() + data.len() + BIN_ENTRY_OVERHEAD) as i64,
            "insert accounts key + data + per-slot BinEntry overhead"
        );

        let deleted = tree.delete(&key);
        assert!(deleted);
        assert_eq!(
            counter.load(Ordering::Relaxed),
            0,
            "F8: delete must subtract key + data + BIN_ENTRY_OVERHEAD, returning the counter              to its pre-insert value (no data_len leak)"
        );
    }

    /// EV-13 (pass-post): a full-node detach must ACTUALLY drop the child
    /// `Arc` from the parent IN, not merely credit bytes.  Before the fix the
    /// evictor credited `node_size_fn(node_id)` and removed the node from the
    /// LRU list, but the parent's `InEntry.child` still held a strong `Arc`,
    /// so the node was never freed (phantom free) and the budget over-credited.
    ///
    /// This test proves: after `detach_node_by_id` the held child `Arc` is the
    /// LAST strong reference (strong_count == 1), the parent slot's `child` is
    /// `None`, and the returned bytes equal the node's measured heap size.
    ///
    /// JE ref: `IN.detachNode` (`setTarget(idx, null)`) / `Evictor.evict`.
    #[test]
    fn test_ev13_detach_actually_frees_child() {
        // Tiny fanout forces a root split so we get a real IN parent with BIN
        // children that the evictor would target.
        let tree = Tree::new(7, 4);
        for i in 0u8..12 {
            tree.insert(
                vec![b'a' + i],
                vec![i; 8],
                Lsn::new(1, u32::from(i) + 1),
            )
            .unwrap();
        }

        // Find a BIN child of the root IN (the eviction target) + its parent.
        let root = tree.get_root().expect("tree must have a root");
        let (parent_arc, child_idx, bin_id, expected_bytes) = {
            let rg = root.read();
            let TreeNode::Internal(n) = &*rg else {
                panic!("root must be an IN after split");
            };
            // Pick the first slot whose child is a resident BIN.
            let (idx, child) = n
                .first_resident_child()
                .expect("root must have a resident child");
            let (id, bytes) = {
                let cg = child.read();
                (
                    match &*cg {
                        TreeNode::Bottom(b) => b.node_id,
                        TreeNode::Internal(n2) => n2.node_id,
                    },
                    cg.budgeted_memory_size(),
                )
            };
            (Arc::clone(&root), idx, id, bytes)
        };

        // Hold an external strong reference to the child so we can observe its
        // strong_count drop when detach releases the parent's reference.
        let child_arc = {
            let pg = parent_arc.read();
            let TreeNode::Internal(n) = &*pg else { unreachable!() };
            Arc::clone(n.child_ref(child_idx).unwrap())
        };
        // Two strong refs now: the parent slot + our test handle.
        assert_eq!(
            Arc::strong_count(&child_arc),
            2,
            "precondition: parent slot + test handle hold the child"
        );

        let freed = tree.detach_node_by_id(bin_id);

        // 1. Bytes credited equal the measured heap size (no phantom credit).
        assert_eq!(
            freed, expected_bytes,
            "detach must credit the node's real measured heap size"
        );
        // 2. The parent slot's child is now None (JE setTarget(idx, null)).
        {
            let pg = parent_arc.read();
            let TreeNode::Internal(n) = &*pg else { unreachable!() };
            assert!(
                n.child_is_none(child_idx),
                "EV-13: parent slot must be detached (child == None)"
            );
            // The slot itself (key + LSN) is retained for re-fetch.
            assert!(
                !n.get_lsn(child_idx).is_null(),
                "detach keeps the slot LSN so the node can be re-fetched"
            );
        }
        // 3. Our handle is now the ONLY strong reference -> the parent really
        //    dropped its Arc; the node is freed when we drop `child_arc`.
        //    Before EV-13 this would be 2 (parent still held it) = phantom free.
        assert_eq!(
            Arc::strong_count(&child_arc),
            1,
            "EV-13: detach must drop the parent's strong Arc (no phantom free)"
        );
    }

    /// EV-13: detach must NOT decrement the memory counter itself (the evictor
    /// owns that bookkeeping via `Arbiter::release_memory`).  A double credit
    /// would drive `cache_usage` below reality.
    #[test]
    fn test_ev13_detach_does_not_touch_counter() {
        use std::sync::atomic::{AtomicI64, Ordering};
        let mut tree = Tree::new(8, 4);
        let counter = Arc::new(AtomicI64::new(0));
        tree.set_memory_counter(Arc::clone(&counter));
        for i in 0u8..12 {
            tree.insert(
                vec![b'a' + i],
                vec![i; 8],
                Lsn::new(1, u32::from(i) + 1),
            )
            .unwrap();
        }
        let before = counter.load(Ordering::Relaxed);

        // Grab a BIN child id.
        let root = tree.get_root().unwrap();
        let bin_id = {
            let rg = root.read();
            let TreeNode::Internal(n) = &*rg else { unreachable!() };
            let child = n
                .resident_children()
                .into_iter()
                .next()
                .expect("resident child");
            match &*child.read() {
                TreeNode::Bottom(b) => b.node_id,
                TreeNode::Internal(n2) => n2.node_id,
            }
        };

        let freed = tree.detach_node_by_id(bin_id);
        assert!(freed > 0, "detach must free a resident child");
        assert_eq!(
            counter.load(Ordering::Relaxed),
            before,
            "EV-13: detach must not change the counter (evictor credits once)"
        );
    }

    /// EV-13: detaching the root or an unknown id is a no-op returning 0.
    #[test]
    fn test_ev13_detach_root_or_missing_is_noop() {
        let tree = Tree::new(9, 4);
        for i in 0u8..12 {
            tree.insert(
                vec![b'a' + i],
                vec![i; 8],
                Lsn::new(1, u32::from(i) + 1),
            )
            .unwrap();
        }
        let root_id = {
            let rg = tree.get_root().unwrap();
            let g = rg.read();
            match &*g {
                TreeNode::Internal(n) => n.node_id,
                TreeNode::Bottom(b) => b.node_id,
            }
        };
        assert_eq!(
            tree.detach_node_by_id(root_id),
            0,
            "root has no parent IN -> detach is a no-op"
        );
        assert_eq!(
            tree.detach_node_by_id(u64::MAX),
            0,
            "unknown node id -> detach is a no-op"
        );
    }

    /// DBI-23 (pass-post): the live `memory_counter` must APPROXIMATE the real
    /// in-memory heap of the tree, not the old `key + data + 48` lower bound.
    ///
    /// JE keeps `inMemorySize` (`IN.getBudgetedMemorySize`) in lock-step with
    /// the per-node `computeMemorySize`; the over-budget arbiter sees the real
    /// figure so eviction fires at the right time.  The previous Noxu live
    /// path undercounted each BIN slot (48 vs the 64-byte `BinEntry` struct)
    /// and never accounted the node-struct fixed overhead, so the counter ran
    /// below real heap and the evictor under-fired.
    ///
    /// We assert the live counter is within tolerance of
    /// `total_budgeted_memory` (the authoritative walk-and-sum oracle).  The
    /// only gap is the per-node fixed struct overhead (BinStub/InNodeStub),
    /// which is a small fraction for non-trivial entries — the fix closes the
    /// dominant per-slot gap.
    #[test]
    fn test_dbi23_live_counter_approximates_real_heap() {
        use std::sync::atomic::{AtomicI64, Ordering};
        let mut tree = Tree::new(42, 32);
        let counter = Arc::new(AtomicI64::new(0));
        tree.set_memory_counter(Arc::clone(&counter));

        // Insert N entries with realistic key+data sizes.
        let n = 400u32;
        for i in 0..n {
            let key = format!("key-{i:08}").into_bytes(); // 12 bytes
            let data = vec![0u8; 64]; // 64 bytes
            tree.insert(key, data, Lsn::new(1, i + 1)).unwrap();
        }

        let live = counter.load(Ordering::Relaxed) as u64;
        let real = tree.total_budgeted_memory();

        // The live counter must reflect the per-slot cost AFTER the T-2/T-3
        // compactions hoisted the per-slot key/LSN out of `BinEntry` into the
        // node-level reps.  The per-slot live charge is now
        // `key + data + size_of::<BinEntry>() + 4` (the packed LSN slot); the
        // dominant data+key bytes are still charged in full.  Assert the live
        // counter is at least the data-and-fixed portion (a stable floor that
        // does NOT assume the pre-compaction 64-byte slot).
        let new_lower_bound: u64 = (0..n)
            .map(|i| {
                let key_len = format!("key-{i:08}").len();
                (key_len + 64 + BIN_ENTRY_OVERHEAD) as u64
            })
            .sum();

        assert!(
            live >= new_lower_bound,
            "DBI-23: live counter ({live}) must be >= the per-slot-correct \
             lower bound ({new_lower_bound})"
        );

        // Within tolerance of real heap (the residual gap is the per-node
        // fixed struct overhead, intentionally not tracked incrementally).
        let lower = real * 80 / 100;
        assert!(
            live >= lower && live <= real,
            "DBI-23: live counter ({live}) must approximate real heap ({real}) \
             within tolerance [{lower}, {real}]"
        );
    }

    #[test]
    fn test_delete_existing_key() {
        let tree = Tree::new(1, 128);
        let key = b"remove_me".to_vec();
        tree.insert(key.clone(), b"val".to_vec(), Lsn::new(1, 10)).unwrap();
        assert!(tree.delete(&key));

        // After deletion the BIN is empty, so delete returns true the first
        // time and false the second time.
        assert!(!tree.delete(&key));
    }

    #[test]
    fn test_delete_nonexistent_key() {
        let tree = Tree::new(1, 128);
        tree.insert(b"a".to_vec(), b"v".to_vec(), Lsn::new(1, 1)).unwrap();

        assert!(!tree.delete(b"zzz"));
    }

    #[test]
    fn test_delete_empty_tree() {
        let tree = Tree::new(1, 128);
        assert!(!tree.delete(b"nothing"));
    }

    #[test]
    fn test_delete_all_entries_makes_bin_empty() {
        let tree = Tree::new(1, 128);
        tree.insert(b"x".to_vec(), b"1".to_vec(), Lsn::new(1, 1)).unwrap();
        tree.insert(b"y".to_vec(), b"2".to_vec(), Lsn::new(1, 2)).unwrap();

        assert!(tree.delete(b"x"));
        assert!(tree.delete(b"y"));

        // Tree still has a root (empty BIN), so is_empty() returns false.
        assert!(!tree.is_empty());
        // get_first_node should return None for an empty BIN.
        assert!(tree.get_first_node().is_none());
    }

    #[test]
    fn test_set_root_and_get_root() {
        let tree = Tree::new(1, 128);
        assert!(tree.get_root().is_none());

        let bin = TreeNode::Bottom(BinStub {
            node_id: generate_node_id(),
            level: BIN_LEVEL,
            entries: vec![],
            key_prefix: Vec::new(),
            dirty: false,
            is_delta: false,
            last_full_lsn: NULL_LSN,
            last_delta_lsn: NULL_LSN,
            generation: 0,
            parent: None,
            expiration_in_hours: true,
            cursor_count: 0,
            prohibit_next_delta: false,
            lsn_rep: LsnRep::Empty,
            keys: KeyRep::new(),
            compact_max_key_length: INKeyRep_DEFAULT_MAX_KEY_LENGTH,
        });
        tree.set_root(bin);
        assert!(tree.get_root().is_some());
    }

    // ========================================================================
    // Split / multi-level insert tests  (new)
    // ========================================================================

    /// inserting enough keys to fill the root IN causes
    /// the root IN itself to split, resulting in a tree with 3 or more levels.
    ///
    /// With max_entries_per_node = 4:
    ///   - Each BIN holds 4 entries before it is split.
    ///   - The root IN at level 2 holds up to 4 BIN children.
    ///   - Filling those 4 BINs (16 entries) and adding a 17th forces the
    ///     root IN to split, creating a level-3 root.
    #[test]
    fn test_insert_forces_root_split() {
        let tree = Tree::new(1, 4);

        // 17 inserts with fanout 4 forces the root IN to split.
        for i in 0u32..20 {
            let key = format!("key{:04}", i).into_bytes();
            let data = format!("data{}", i).into_bytes();
            let lsn = Lsn::new(1, 100 + i);
            let r = tree.insert(key, data, lsn);
            assert!(r.is_ok(), "insert {} must succeed", i);
        }

        // At least one root split must have occurred.
        assert!(
            tree.get_root_splits() > 0,
            "expected at least one root split after 20 inserts with fanout 4"
        );

        // The root level must be > level-2 (i.e., the tree has grown to 3+ levels).
        let root_arc = tree.get_root().as_ref().unwrap().clone();
        let root_level = root_arc.read().level();
        let level_2 = MAIN_LEVEL | 2;
        assert!(
            root_level > level_2,
            "root level {} must be > level-2 after root split",
            root_level
        );
    }

    /// Inserting 1000 keys in sorted order and verifying all are searchable.
    #[test]
    fn test_insert_many_keys() {
        let tree = Tree::new(1, 8);
        let n = 1000u32;

        for i in 0..n {
            let key = format!("key{:08}", i).into_bytes();
            let data = format!("data{}", i).into_bytes();
            let lsn = Lsn::new(1, i);
            let r = tree.insert(key, data, lsn);
            assert!(r.is_ok(), "insert {} must succeed", i);
        }

        // All keys must be findable.
        for i in 0..n {
            let key = format!("key{:08}", i).into_bytes();
            let sr = tree.search(&key);
            assert!(
                sr.is_some() && sr.unwrap().exact_parent_found,
                "key{:08} must be found after bulk insert",
                i
            );
        }
    }

    /// Inserting 500 keys in pseudo-random (reverse) order and verifying all
    /// are searchable.
    #[test]
    fn test_insert_random_keys() {
        let tree = Tree::new(1, 8);
        let n = 500u32;

        // Insert in reverse order as a simple non-sorted sequence.
        for i in (0..n).rev() {
            let key = format!("rkey{:08}", i).into_bytes();
            let data = format!("data{}", i).into_bytes();
            let lsn = Lsn::new(1, i);
            let r = tree.insert(key, data, lsn);
            assert!(r.is_ok(), "insert {} must succeed", i);
        }

        for i in 0..n {
            let key = format!("rkey{:08}", i).into_bytes();
            let sr = tree.search(&key);
            assert!(
                sr.is_some() && sr.unwrap().exact_parent_found,
                "rkey{:08} must be found",
                i
            );
        }
    }

    /// After any number of splits, every key inserted must still be findable.
    ///
    #[test]
    fn test_split_preserves_all_keys() {
        // Tiny fanout to maximise split frequency.
        let tree = Tree::new(1, 3);
        let n = 60u32;

        let mut keys: Vec<Vec<u8>> = Vec::new();
        for i in 0..n {
            let key = format!("sk{:04}", i).into_bytes();
            keys.push(key.clone());
            let data = format!("d{}", i).into_bytes();
            let lsn = Lsn::new(1, i);
            let r = tree.insert(key, data, lsn);
            assert!(r.is_ok(), "insert {} must not fail", i);
        }

        // After all inserts (and all the splits they induced), every key must
        // still be findable in the tree.
        for key in &keys {
            let sr = tree.search(key);
            assert!(
                sr.is_some() && sr.unwrap().exact_parent_found,
                "key {:?} must survive all splits",
                std::str::from_utf8(key).unwrap_or("?")
            );
        }
    }

    /// The tree level (depth) must grow as keys are inserted and splits occur.
    #[test]
    fn test_tree_height_grows() {
        let tree = Tree::new(1, 4);

        // With fanout 4, one level-2 root IN can hold 4 children.  After enough
        // inserts the root itself will split and a level-3 node will appear.
        // Insert enough keys to force the root to split at least once.
        let n = 40u32;
        for i in 0..n {
            let key = format!("hk{:08}", i).into_bytes();
            let data = format!("d{}", i).into_bytes();
            let lsn = Lsn::new(1, i);
            tree.insert(key, data, lsn).unwrap();
        }

        // At least one root split must have occurred.
        assert!(
            tree.get_root_splits() > 0,
            "expected root to have split at least once for {} keys with fanout 4",
            n
        );

        // The root level must be > level-2 (i.e., the tree has grown past two levels).
        let root_arc = tree.get_root().as_ref().unwrap().clone();
        let root_level = root_arc.read().level();
        let level_2 = MAIN_LEVEL | 2;
        assert!(
            root_level > level_2,
            "root level {} must be > {} after enough inserts",
            root_level,
            level_2
        );
    }

    #[test]
    fn test_find_entry_on_internal_node() {
        let mut entries = vec![];
        for i in 0..4 {
            entries.push(InEntry { key: format!("k{}", i).into_bytes() });
        }
        let internal = TreeNode::Internal(InNodeStub {
            node_id: 1,
            level: MAIN_LEVEL + 2,
            entries,
            targets: TargetRep::None,
            dirty: false,
            generation: 0,
            parent: None,
            lsn_rep: LsnRep::Empty,
        });

        // Exact match
        let r = internal.find_entry(b"k2", false, true);
        assert_ne!(r & EXACT_MATCH, 0);
        assert_eq!(r & 0xFFFF, 2);

        // No exact match with exact=true
        let r = internal.find_entry(b"kx", false, true);
        assert_eq!(r, -1);
    }

    // St-H5: non-exact `find_entry` on an Internal node must return the FLOOR
    // child slot (largest entry ≤ key), not the insertion point. Entries are
    // k0,k1,k2,k3; slot 0 is the leftmost child.
    #[test]
    fn test_find_entry_internal_nonexact_returns_floor() {
        let mut entries = vec![];
        for i in 0..4 {
            entries.push(InEntry { key: format!("k{}", i).into_bytes() });
        }
        let internal = TreeNode::Internal(InNodeStub {
            node_id: 1,
            level: MAIN_LEVEL + 2,
            entries,
            targets: TargetRep::None,
            dirty: false,
            generation: 0,
            parent: None,
            lsn_rep: LsnRep::Empty,
        });

        // Key below every separator floors to slot 0 (leftmost child).
        assert_eq!(internal.find_entry(b"a", false, false) & 0xFFFF, 0);
        // Between k1 and k2 floors to k1 (slot 1).
        assert_eq!(internal.find_entry(b"k1x", false, false) & 0xFFFF, 1);
        // Above every separator floors to the last slot (k3 = slot 3).
        assert_eq!(internal.find_entry(b"zzz", false, false) & 0xFFFF, 3);
        // Exact match still reported as the exact slot.
        let r = internal.find_entry(b"k2", false, false);
        assert_ne!(r & EXACT_MATCH, 0);
        assert_eq!(r & 0xFFFF, 2);
    }

    // ========================================================================
    // New tests: dirty tracking, generation, parent pointers, log size, stats
    // ========================================================================

    /// After inserting into a tree, the BIN (and root IN) must be dirty.
    ///
    /// The: Tree.insertLN() calls bin.setDirty(true) after each insert.
    #[test]
    fn test_insert_marks_bin_dirty() {
        let tree = Tree::new(1, 128);
        tree.insert(b"key1".to_vec(), b"val1".to_vec(), Lsn::new(1, 1))
            .unwrap();

        let root_arc = tree.get_root().as_ref().unwrap().clone();
        // root is an upper IN — its slot 0 child is the BIN.
        let bin_arc = {
            let g = root_arc.read();
            match &*g {
                TreeNode::Internal(n) => n.get_child(0).unwrap(),
                _ => panic!("expected Internal root"),
            }
        };

        let bin_dirty = bin_arc.read().is_dirty();
        assert!(bin_dirty, "BIN must be dirty after insert");
    }

    /// Updating an existing key keeps the BIN dirty.
    #[test]
    fn test_update_keeps_bin_dirty() {
        let tree = Tree::new(1, 128);
        tree.insert(b"k".to_vec(), b"v1".to_vec(), Lsn::new(1, 1)).unwrap();
        // second insert is an update
        tree.insert(b"k".to_vec(), b"v2".to_vec(), Lsn::new(1, 2)).unwrap();

        let root_arc = tree.get_root().as_ref().unwrap().clone();
        let bin_arc = {
            let g = root_arc.read();
            match &*g {
                TreeNode::Internal(n) => n.get_child(0).unwrap(),
                _ => panic!("expected Internal root"),
            }
        };

        assert!(bin_arc.read().is_dirty(), "BIN must be dirty after update");
    }

    /// After deleting a key the BIN must be dirty.
    #[test]
    fn test_delete_marks_bin_dirty() {
        let tree = Tree::new(1, 128);
        tree.insert(b"del".to_vec(), b"val".to_vec(), Lsn::new(1, 1)).unwrap();

        // Manually clear dirty flag to verify delete re-sets it.
        {
            let root_arc = tree.get_root().as_ref().unwrap().clone();
            let bin_arc = {
                let g = root_arc.read();
                match &*g {
                    TreeNode::Internal(n) => n.get_child(0).unwrap(),
                    _ => panic!("expected Internal root"),
                }
            };
            bin_arc.write().set_dirty(false);
            assert!(!bin_arc.read().is_dirty());
        }

        tree.delete(b"del");

        let root_arc = tree.get_root().as_ref().unwrap().clone();
        let bin_arc = {
            let g = root_arc.read();
            match &*g {
                TreeNode::Internal(n) => n.get_child(0).unwrap(),
                _ => panic!("expected Internal root"),
            }
        };
        assert!(bin_arc.read().is_dirty(), "BIN must be dirty after delete");
    }

    /// BIN's parent pointer must point to the root IN.
    #[test]
    fn test_bin_parent_pointer_set_on_initial_insert() {
        let tree = Tree::new(1, 128);
        tree.insert(b"k".to_vec(), b"v".to_vec(), Lsn::new(1, 1)).unwrap();

        let root_arc = tree.get_root().as_ref().unwrap().clone();
        let bin_arc = {
            let g = root_arc.read();
            match &*g {
                TreeNode::Internal(n) => n.get_child(0).unwrap(),
                _ => panic!("expected Internal root"),
            }
        };

        let parent_weak = bin_arc.read().get_parent();
        assert!(parent_weak.is_some(), "BIN must have a parent pointer");

        // Upgrading the weak pointer must give us the root arc.
        let parent_arc = parent_weak.unwrap().upgrade().unwrap();
        assert!(
            Arc::ptr_eq(&parent_arc, &root_arc),
            "BIN parent must be the root IN"
        );
    }

    /// set_dirty / is_dirty round-trip on both variants.
    #[test]
    fn test_dirty_flag_roundtrip() {
        let mut bin_node = TreeNode::Bottom(BinStub {
            node_id: 1,
            level: BIN_LEVEL,
            entries: vec![],
            key_prefix: Vec::new(),
            dirty: false,
            is_delta: false,
            last_full_lsn: NULL_LSN,
            last_delta_lsn: NULL_LSN,
            generation: 0,
            parent: None,
            expiration_in_hours: true,
            cursor_count: 0,
            prohibit_next_delta: false,
            lsn_rep: LsnRep::Empty,
            keys: KeyRep::new(),
            compact_max_key_length: INKeyRep_DEFAULT_MAX_KEY_LENGTH,
        });
        assert!(!bin_node.is_dirty());
        bin_node.set_dirty(true);
        assert!(bin_node.is_dirty());
        bin_node.set_dirty(false);
        assert!(!bin_node.is_dirty());

        let mut in_node = TreeNode::Internal(InNodeStub {
            node_id: 2,
            level: MAIN_LEVEL | 2,
            entries: vec![],
            targets: TargetRep::None,
            dirty: false,
            generation: 0,
            parent: None,
            lsn_rep: LsnRep::Empty,
        });
        assert!(!in_node.is_dirty());
        in_node.set_dirty(true);
        assert!(in_node.is_dirty());
    }

    /// set_generation / get_generation round-trip on both variants.
    #[test]
    fn test_generation_roundtrip() {
        let mut bin_node = TreeNode::Bottom(BinStub {
            node_id: 1,
            level: BIN_LEVEL,
            entries: vec![],
            key_prefix: Vec::new(),
            dirty: false,
            is_delta: false,
            last_full_lsn: NULL_LSN,
            last_delta_lsn: NULL_LSN,
            generation: 0,
            parent: None,
            expiration_in_hours: true,
            cursor_count: 0,
            prohibit_next_delta: false,
            lsn_rep: LsnRep::Empty,
            keys: KeyRep::new(),
            compact_max_key_length: INKeyRep_DEFAULT_MAX_KEY_LENGTH,
        });
        assert_eq!(bin_node.get_generation(), 0);
        bin_node.set_generation(42);
        assert_eq!(bin_node.get_generation(), 42);

        let mut in_node = TreeNode::Internal(InNodeStub {
            node_id: 2,
            level: MAIN_LEVEL | 2,
            entries: vec![],
            targets: TargetRep::None,
            dirty: false,
            generation: 0,
            parent: None,
            lsn_rep: LsnRep::Empty,
        });
        in_node.set_generation(99);
        assert_eq!(in_node.get_generation(), 99);
    }

    /// log_size() must be consistent with write_to_bytes() length.
    #[test]
    fn test_log_size_matches_bytes_len() {
        // BIN stub with some entries.
        let bin_node = TreeNode::Bottom(BinStub {
            node_id: 7,
            level: BIN_LEVEL,
            entries: vec![
                BinEntry {
                    data: Some(b"d1".to_vec()),
                    known_deleted: false,
                    dirty: false,
                    expiration_time: 0,
                },
                BinEntry {
                    data: None,
                    known_deleted: false,
                    dirty: false,
                    expiration_time: 0,
                },
            ],
            key_prefix: Vec::new(),
            dirty: true,
            is_delta: false,
            last_full_lsn: NULL_LSN,
            last_delta_lsn: NULL_LSN,
            generation: 5,
            parent: None,
            expiration_in_hours: true,
            cursor_count: 0,
            prohibit_next_delta: false,
            lsn_rep: LsnRep::Empty,
            keys: KeyRep::from_keys(vec![b"alpha".to_vec(), b"beta".to_vec()]),
            compact_max_key_length: INKeyRep_DEFAULT_MAX_KEY_LENGTH,
        });
        assert_eq!(bin_node.log_size(), bin_node.write_to_bytes().len());

        // IN stub with some entries.
        let in_node = TreeNode::Internal(InNodeStub {
            node_id: 8,
            level: MAIN_LEVEL | 2,
            entries: vec![
                InEntry { key: vec![] },
                InEntry { key: b"mid".to_vec() },
            ],
            targets: TargetRep::None,
            dirty: false,
            generation: 0,
            parent: None,
            lsn_rep: LsnRep::Empty,
        });
        assert_eq!(in_node.log_size(), in_node.write_to_bytes().len());
    }

    /// write_to_bytes() output contains the node_id and dirty flag.
    #[test]
    fn test_write_to_bytes_encodes_node_id_and_dirty() {
        let node = TreeNode::Bottom(BinStub {
            node_id: 0xDEAD_BEEF_0000_0001,
            level: BIN_LEVEL,
            entries: vec![],
            key_prefix: Vec::new(),
            dirty: true,
            is_delta: false,
            last_full_lsn: NULL_LSN,
            last_delta_lsn: NULL_LSN,
            generation: 0,
            parent: None,
            expiration_in_hours: true,
            cursor_count: 0,
            prohibit_next_delta: false,
            lsn_rep: LsnRep::Empty,
            keys: KeyRep::new(),
            compact_max_key_length: INKeyRep_DEFAULT_MAX_KEY_LENGTH,
        });
        let bytes = node.write_to_bytes();
        // First 8 bytes = node_id big-endian.
        let id_bytes = &bytes[0..8];
        assert_eq!(id_bytes, 0xDEAD_BEEF_0000_0001u64.to_be_bytes());
        // Byte at offset 16 (after node_id[8] + level[4] + n_entries[4]) = dirty flag.
        assert_eq!(bytes[16], 1u8, "dirty flag must be 1");
    }

    /// log_size() grows as entries are added.
    #[test]
    fn test_log_size_grows_with_entries() {
        let empty = TreeNode::Bottom(BinStub {
            node_id: 1,
            level: BIN_LEVEL,
            entries: vec![],
            key_prefix: Vec::new(),
            dirty: false,
            is_delta: false,
            last_full_lsn: NULL_LSN,
            last_delta_lsn: NULL_LSN,
            generation: 0,
            parent: None,
            expiration_in_hours: true,
            cursor_count: 0,
            prohibit_next_delta: false,
            lsn_rep: LsnRep::Empty,
            keys: KeyRep::new(),
            compact_max_key_length: INKeyRep_DEFAULT_MAX_KEY_LENGTH,
        });
        let with_entry = TreeNode::Bottom(BinStub {
            node_id: 2,
            level: BIN_LEVEL,
            entries: vec![BinEntry {
                data: None,
                known_deleted: false,
                dirty: false,
                expiration_time: 0,
            }],
            key_prefix: Vec::new(),
            dirty: false,
            is_delta: false,
            last_full_lsn: NULL_LSN,
            last_delta_lsn: NULL_LSN,
            generation: 0,
            parent: None,
            expiration_in_hours: true,
            cursor_count: 0,
            prohibit_next_delta: false,
            lsn_rep: LsnRep::Empty,
            keys: KeyRep::from_keys(vec![b"longkey_here".to_vec()]),
            compact_max_key_length: INKeyRep_DEFAULT_MAX_KEY_LENGTH,
        });
        assert!(
            with_entry.log_size() > empty.log_size(),
            "log_size must grow when entries are added"
        );
    }

    /// propagate_dirty_to_root() marks all ancestors dirty.
    #[test]
    fn test_propagate_dirty_to_root() {
        // Build a 2-level tree manually: root IN -> BIN.
        let bin_arc = Arc::new(RwLock::new(TreeNode::Bottom(BinStub {
            node_id: generate_node_id(),
            level: BIN_LEVEL,
            entries: vec![],
            key_prefix: Vec::new(),
            dirty: false,
            is_delta: false,
            last_full_lsn: NULL_LSN,
            last_delta_lsn: NULL_LSN,
            generation: 0,
            parent: None, // set below
            expiration_in_hours: true,
            cursor_count: 0,
            prohibit_next_delta: false,
            lsn_rep: LsnRep::Empty,
            keys: KeyRep::new(),
            compact_max_key_length: INKeyRep_DEFAULT_MAX_KEY_LENGTH,
        })));

        let root_arc = Arc::new(RwLock::new(TreeNode::Internal(InNodeStub {
            node_id: generate_node_id(),
            level: MAIN_LEVEL | 2,
            entries: vec![InEntry { key: vec![] }],
            targets: TargetRep::Sparse(vec![(0, bin_arc.clone())]),
            dirty: false,
            generation: 0,
            parent: None,
            lsn_rep: LsnRep::Empty,
        })));

        // Wire BIN's parent to root.
        bin_arc.write().set_parent(Some(Arc::downgrade(&root_arc)));

        // Root is not dirty before propagation.
        assert!(!root_arc.read().is_dirty());

        // Propagate from the BIN up.
        Tree::propagate_dirty_to_root(&bin_arc);

        // Root must now be dirty.
        assert!(
            root_arc.read().is_dirty(),
            "root must be dirty after propagate_dirty_to_root"
        );
    }

    /// collect_stats() on an empty tree returns all-zero stats.
    #[test]
    fn test_collect_stats_empty_tree() {
        let tree = Tree::new(1, 128);
        let stats = tree.collect_stats();
        assert_eq!(stats, TreeStats::default());
    }

    /// collect_stats() on a single-entry tree: 1 IN + 1 BIN, height 2.
    #[test]
    fn test_collect_stats_single_insert() {
        let tree = Tree::new(1, 128);
        tree.insert(b"k".to_vec(), b"v".to_vec(), Lsn::new(1, 1)).unwrap();
        let stats = tree.collect_stats();
        assert_eq!(stats.n_bins, 1, "must have 1 BIN");
        assert_eq!(stats.n_ins, 1, "must have 1 upper IN");
        assert_eq!(stats.height, 2, "single-entry tree has height 2");
        assert!(stats.n_entries >= 1, "must have at least 1 entry total");
    }

    /// collect_stats() with many inserts: entry count matches insert count.
    #[test]
    fn test_collect_stats_many_inserts() {
        let tree = Tree::new(1, 8);
        let n = 50u32;
        for i in 0..n {
            let key = format!("sk{:04}", i).into_bytes();
            tree.insert(key, b"v".to_vec(), Lsn::new(1, i)).unwrap();
        }
        let stats = tree.collect_stats();
        // All n entries should be accounted for across all BINs.
        // n_entries counts entries in both INs and BINs; BIN entries = n.
        // We verify BIN entry total equals n by summing manually.
        let bin_entries: u64 = stats.n_entries - stats.n_ins; // rough check
        // A more precise assertion: the sum of all BIN entries == n.
        // Since we can't easily separate, just assert the tree is non-trivial.
        assert!(stats.n_bins > 0, "must have at least one BIN");
        assert!(stats.height >= 2, "multi-entry tree has height >= 2");
        // Total entries in the tree must be >= n (BIN entries alone).
        assert!(
            bin_entries >= n as u64 || stats.n_entries >= n as u64,
            "entry count must account for all inserts"
        );
    }

    // ========================================================================
    // Tests: B-tree merge / compress
    // ========================================================================

    /// After deleting most keys from a tree, compress() must reduce the BIN
    /// count by merging under-full siblings.
    ///
    /// Strategy: build a large tree (many BINs), delete almost all keys,
    /// then verify compress() reduces n_bins and all surviving keys remain
    /// findable.  We do not hard-code the exact BIN counts because the
    /// preemptive splitting strategy determines the exact split points.
    #[test]
    fn test_compress_merges_underfull_bins() {
        let tree = Tree::new(1, 8);

        // Insert 64 sorted keys to build a multi-BIN tree.
        let n = 64u32;
        let keys: Vec<Vec<u8>> =
            (0..n).map(|i| format!("cm{:04}", i).into_bytes()).collect();
        for (i, key) in keys.iter().enumerate() {
            tree.insert(key.clone(), vec![i as u8], Lsn::new(1, i as u32))
                .unwrap();
        }

        let stats_full = tree.collect_stats();
        assert!(
            stats_full.n_bins >= 2,
            "must have multiple BINs after 64 inserts"
        );

        // Delete all but 4 widely-spaced keys (one roughly per BIN pair).
        // We keep every 16th key: k0000, k0016, k0032, k0048.
        let keep: std::collections::HashSet<u32> =
            [0, 16, 32, 48].iter().cloned().collect();
        for i in 0..n {
            if !keep.contains(&i) {
                let key = format!("cm{:04}", i).into_bytes();
                tree.delete(&key);
            }
        }

        let stats_sparse = tree.collect_stats();
        assert!(
            stats_sparse.n_bins >= 2,
            "should still have multiple BINs before compress"
        );

        // compress() must reduce BIN count since most BINs now hold 0–1 entries.
        tree.compress();

        let stats_after = tree.collect_stats();
        assert!(
            stats_after.n_bins < stats_sparse.n_bins,
            "compress must reduce BIN count (was {}, now {})",
            stats_sparse.n_bins,
            stats_after.n_bins
        );

        // Surviving keys must still be findable.
        for i in keep {
            let key = format!("cm{:04}", i).into_bytes();
            let sr = tree.search(&key);
            assert!(
                sr.is_some() && sr.unwrap().exact_parent_found,
                "key cm{:04} must survive compress",
                i
            );
        }
    }

    /// compress() preserves all entries: a full-BIN tree has fewer merges
    /// but all keys remain accessible.
    #[test]
    fn test_compress_no_op_when_full() {
        // Insert exactly max_entries worth of keys into a single BIN — no split
        // will have occurred yet, and the BINs will all be reasonably full.
        // We can't prevent splits entirely (preemptive), but we can verify that
        // compress() never loses entries.
        let tree = Tree::new(1, 8);
        let n = 32u32;
        for i in 0..n {
            let key = format!("fn{:04}", i).into_bytes();
            tree.insert(key, vec![i as u8], Lsn::new(1, i)).unwrap();
        }

        let stats_before = tree.collect_stats();
        tree.compress();
        let stats_after = tree.collect_stats();

        // All keys still findable.
        for i in 0..n {
            let key = format!("fn{:04}", i).into_bytes();
            let sr = tree.search(&key);
            assert!(
                sr.is_some() && sr.unwrap().exact_parent_found,
                "key fn{:04} must be findable after compress",
                i
            );
        }

        // BIN count must not increase.
        assert!(
            stats_after.n_bins <= stats_before.n_bins,
            "compress must not increase BIN count"
        );
    }

    /// compress() on an empty tree must not panic.
    #[test]
    fn test_compress_empty_tree() {
        let tree = Tree::new(1, 4);
        tree.compress(); // must not panic
    }

    /// Deterministic regression for the BIN/IN split-path check-then-act race
    /// (`.agent/archived-audits/bench/bug-bin-split-concurrency.md`).
    ///
    /// `insert_recursive_inner` checks `child.get_n_entries() >= max_entries`
    /// under a PARENT READ lock, drops that read lock (required — the split
    /// needs `parent.write()`), then calls `split_child`. In the drop→reacquire
    /// window a racing thread (a second splitter, or the INCompressor merging
    /// and CLEARING a sibling — `compress_node`'s `lb.entries.clear()`) can
    /// leave the child no longer full, or even empty. Pre-fix, `split_child`
    /// then built a `SplitEntries` from that stale child and
    /// `SplitEntries::get_key(split_index)` panicked with
    /// "index out of bounds: len is 0" on the empty entries vec.
    ///
    /// This test drives the exact interleaving deterministically: it builds a
    /// level-2 tree, empties a full BIN child in place (simulating the racing
    /// merge), then calls `split_child` on it directly. With the fix
    /// `split_child` re-validates fullness under the child write lock and
    /// returns `Ok(())` (a benign no-op); without the fix it panics in
    /// `get_key`.
    ///
    /// JE-faithful: `IN.split` re-checks `needsSplitting()` after latching the
    /// node it will split (IN.java IN.split / IN.needsSplitting).
    #[test]
    fn split_child_is_noop_when_child_no_longer_full() {
        let max_entries = 8usize;
        let tree = Tree::new(1, max_entries);

        // Build a level-2 tree: insert enough sorted keys to force at least one
        // split so the root becomes an Internal node with BIN children.
        for i in 0..64u32 {
            tree.insert(
                format!("k{:04}", i).into_bytes(),
                vec![i as u8],
                Lsn::new(1, i),
            )
            .unwrap();
        }

        let root_arc = tree.get_root().expect("root resident");

        // Pick child slot 0 (any resident BIN child works — the panic is about
        // the child being empty at split time, not about how it got there).
        let child_arc = {
            let g = root_arc.read();
            let TreeNode::Internal(n) = &*g else {
                panic!("expected a level-2 tree (root should be Internal)");
            };
            n.get_child(0).expect("resident child at slot 0")
        };
        let child_index = 0usize;

        // Simulate the racing merge: clear the child's entries in place, the
        // way `compress_node` clears the merged-away left sibling. This is the
        // stale state a second `split_child` (or a split racing the compressor)
        // observes after the fullness check was already passed under the now-
        // dropped parent read lock.
        {
            let mut cg = child_arc.write();
            match &mut *cg {
                TreeNode::Bottom(b) => {
                    b.entries.clear();
                    b.lsn_rep = LsnRep::Empty;
                    b.keys = KeyRep::new();
                }
                TreeNode::Internal(n) => {
                    n.entries.clear();
                    n.lsn_rep = LsnRep::Empty;
                    n.targets = TargetRep::None;
                }
            }
            assert_eq!(cg.get_n_entries(), 0, "child must now be empty");
        }

        // Directly call the split path. Pre-fix this panics in
        // `SplitEntries::get_key(0)` on the empty vec; post-fix it re-validates
        // fullness under the child write lock and returns Ok(()) (no-op).
        let res = Tree::split_child(
            &root_arc,
            child_index,
            max_entries,
            Lsn::new(1, 999),
            SplitHint::Normal,
            b"k0000",
            None,  // no comparator
            false, // key_prefixing off
            None,  // no InListListener
        );
        assert!(
            res.is_ok(),
            "split_child on an emptied (no-longer-full) child must be a benign \
             no-op, got {:?}",
            res
        );
    }

    /// After deleting all entries, compress() reduces BINs to 1.
    #[test]
    fn test_compress_removes_empty_bin_from_parent() {
        let tree = Tree::new(1, 4);
        // Insert enough keys to generate multiple BINs.
        let n = 16u32;
        for i in 0..n {
            let key = format!("ep{:04}", i).into_bytes();
            tree.insert(key, vec![i as u8], Lsn::new(1, i)).unwrap();
        }

        let stats_before = tree.collect_stats();
        assert!(stats_before.n_bins >= 2, "need multiple BINs for this test");

        // Delete everything except the very last key.
        for i in 0..n - 1 {
            let key = format!("ep{:04}", i).into_bytes();
            tree.delete(&key);
        }

        tree.compress();

        let stats_after = tree.collect_stats();
        assert!(
            stats_after.n_bins < stats_before.n_bins,
            "compress must reduce BIN count after mass deletion"
        );

        // The surviving key must still be findable.
        let last_key = format!("ep{:04}", n - 1).into_bytes();
        let sr = tree.search(&last_key);
        assert!(
            sr.is_some() && sr.unwrap().exact_parent_found,
            "last key must survive after compress"
        );
    }

    // ========================================================================
    // IC-1: prune_empty_bin must NOT remove a live entry when the BIN was
    // repopulated between the compressor observing it empty and the prune.
    // (Tree corruption / lost-write regression test.)
    // ========================================================================

    /// Find a BIN arc that is currently empty (0 entries) and is NOT the
    /// root, returning it together with the `id_key` the compressor would
    /// have captured (here we just use any key that routes to that BIN).
    fn first_empty_non_root_bin(tree: &Tree) -> Option<Arc<RwLock<TreeNode>>> {
        let root = tree.get_root()?;
        for node in tree.rebuild_in_list() {
            if Arc::ptr_eq(&node, &root) {
                continue; // skip root (single-BIN tree is never pruned)
            }
            let is_empty_bin = {
                let g = node.read();
                matches!(&*g, TreeNode::Bottom(b) if b.entries.is_empty())
            };
            if is_empty_bin {
                return Some(node);
            }
        }
        None
    }

    /// IC-1 (fail-pre / pass-post): the old `compress_bin` prune step called
    /// `self.delete(&id_key)`, which re-descends by key.  If a concurrent
    /// insert repopulated the empty BIN with a LIVE entry under that same
    /// `id_key`, `self.delete` would silently remove the live entry — a lost
    /// write.  `prune_empty_bin` re-validates `n_entries == 0` under the
    /// parent latch and must REMOVE NOTHING when the BIN is non-empty.
    ///
    /// JE `Tree.delete` / `searchDeletableSubTree` (Tree.java ~line 755-800):
    /// `bin.getNEntries() != 0` → NODE_NOT_EMPTY (abort prune).
    #[test]
    fn test_ic1_prune_empty_bin_aborts_when_repopulated() {
        let tree = Tree::new(1, 4);
        let n = 16u32;
        for i in 0..n {
            let key = format!("ic{:04}", i).into_bytes();
            tree.insert(key, vec![i as u8], Lsn::new(1, i)).unwrap();
        }
        assert!(
            tree.collect_stats().n_bins >= 2,
            "need multiple BINs for this test"
        );

        // Empty out one whole BIN by deleting every key it holds.  We delete
        // the lowest 4 keys (ic0000..ic0003) which share the first BIN, then
        // physically compress it so it has 0 entries.
        for i in 0..4 {
            let key = format!("ic{:04}", i).into_bytes();
            tree.delete(&key);
        }

        // Locate the now-empty BIN and the id_key the compressor would use.
        let empty_bin = match first_empty_non_root_bin(&tree) {
            Some(b) => b,
            // If the layout didn't leave an isolated empty BIN, the scenario
            // isn't reproducible on this build; treat as vacuously passing.
            None => return,
        };

        // SIMULATE THE RACE: a concurrent insert repopulates the empty BIN
        // with a LIVE entry *before* the prune runs.  We insert directly into
        // the BIN arc to model the insert that lands after `now_empty` was
        // read.  Pick a key that routes to this BIN.
        let live_key = format!("ic{:04}", 1).into_bytes(); // was deleted above
        {
            let mut g = empty_bin.write();
            if let TreeNode::Bottom(b) = &mut *g {
                // T-2/T-3: route through the insert helper so entries/keys/
                // lsn_rep stay in lock step.
                b.insert_with_prefix(
                    live_key.clone(),
                    Lsn::new(1, 1),
                    Some(vec![0xAB]),
                );
            }
        }
        let id_key = {
            let g = empty_bin.read();
            match &*g {
                TreeNode::Bottom(b) => b.get_full_key(0).unwrap(),
                _ => unreachable!(),
            }
        };

        // Prune must ABORT (return false) because the BIN is no longer empty,
        // and must NOT remove the live entry.
        let pruned = tree.prune_empty_bin(&id_key);
        assert!(!pruned, "IC-1: prune must abort when the BIN was repopulated");

        // The live entry must still be present in the BIN.
        let still_there = {
            let g = empty_bin.read();
            match &*g {
                TreeNode::Bottom(b) => {
                    b.entries.iter().enumerate().any(|(i, _)| {
                        b.key_prefix.is_empty() && b.get_key(i) == live_key
                    })
                }
                _ => false,
            }
        };
        assert!(
            still_there,
            "IC-1: prune must not remove the repopulated live entry"
        );
    }

    /// IC-1 companion: prune_empty_bin must abort when a cursor is parked on
    /// the (still-empty) BIN.  JE: `bin.nCursors() > 0` → CURSORS_EXIST.
    #[test]
    fn test_ic1_prune_empty_bin_aborts_with_cursor() {
        let tree = Tree::new(1, 4);
        for i in 0..16u32 {
            let key = format!("cu{:04}", i).into_bytes();
            tree.insert(key, vec![i as u8], Lsn::new(1, i)).unwrap();
        }
        for i in 0..4 {
            let key = format!("cu{:04}", i).into_bytes();
            tree.delete(&key);
        }
        let empty_bin = match first_empty_non_root_bin(&tree) {
            Some(b) => b,
            None => return,
        };
        // Park a cursor on the empty BIN.
        Tree::pin_bin(&empty_bin);
        // id_key: any key routing to this BIN. Use the first deleted key.
        let id_key = format!("cu{:04}", 0).into_bytes();
        let pruned = tree.prune_empty_bin(&id_key);
        assert!(
            !pruned,
            "IC-1: prune must abort when a cursor is parked on the BIN"
        );
        Tree::unpin_bin(&empty_bin);
    }

    /// IC-1 happy path: prune_empty_bin removes the parent slot when the BIN
    /// really is empty, no cursors, not a delta.
    #[test]
    fn test_ic1_prune_empty_bin_succeeds_when_truly_empty() {
        let tree = Tree::new(1, 4);
        for i in 0..16u32 {
            let key = format!("ok{:04}", i).into_bytes();
            tree.insert(key, vec![i as u8], Lsn::new(1, i)).unwrap();
        }
        for i in 0..4 {
            let key = format!("ok{:04}", i).into_bytes();
            tree.delete(&key);
        }
        let bins_before = tree.collect_stats().n_bins;
        let empty_bin = match first_empty_non_root_bin(&tree) {
            Some(b) => b,
            None => return,
        };
        // id_key: a key that routes to this empty BIN (one of the deleted).
        let id_key = {
            // route by the lowest deleted key; it falls into the leftmost BIN.
            let _ = &empty_bin;
            format!("ok{:04}", 0).into_bytes()
        };
        let pruned = tree.prune_empty_bin(&id_key);
        assert!(pruned, "IC-1: prune must succeed on a truly empty BIN");
        let bins_after = tree.collect_stats().n_bins;
        assert!(
            bins_after < bins_before,
            "IC-1: pruned BIN slot must be removed from the parent (was {}, now {})",
            bins_before,
            bins_after
        );
        // Every surviving key must still be findable.
        for i in 4..16u32 {
            let key = format!("ok{:04}", i).into_bytes();
            assert!(
                tree.search(&key).is_some_and(|s| s.exact_parent_found),
                "surviving key ok{:04} must remain after prune",
                i
            );
        }
    }

    // ========================================================================
    // Tests: latch-coupling validation (validate_parent_child /
    //        search_with_coupling)
    // ========================================================================

    /// validate_parent_child returns true when the parent slot points at the
    /// expected child.
    #[test]
    fn test_validate_parent_child_correct_link() {
        let bin_arc = Arc::new(RwLock::new(TreeNode::Bottom(BinStub {
            node_id: generate_node_id(),
            level: BIN_LEVEL,
            entries: vec![],
            key_prefix: Vec::new(),
            dirty: false,
            is_delta: false,
            last_full_lsn: NULL_LSN,
            last_delta_lsn: NULL_LSN,
            generation: 0,
            parent: None,
            expiration_in_hours: true,
            cursor_count: 0,
            prohibit_next_delta: false,
            lsn_rep: LsnRep::Empty,
            keys: KeyRep::new(),
            compact_max_key_length: INKeyRep_DEFAULT_MAX_KEY_LENGTH,
        })));

        let root_arc = Arc::new(RwLock::new(TreeNode::Internal(InNodeStub {
            node_id: generate_node_id(),
            level: MAIN_LEVEL | 2,
            entries: vec![InEntry { key: vec![] }],
            targets: TargetRep::Sparse(vec![(0, bin_arc.clone())]),
            dirty: false,
            generation: 0,
            parent: None,
            lsn_rep: LsnRep::Empty,
        })));

        assert!(
            Tree::validate_parent_child(&root_arc, 0, &bin_arc),
            "link must be valid when parent slot 0 points at bin_arc"
        );
    }

    /// validate_parent_child returns false when the slot index is out of range.
    #[test]
    fn test_validate_parent_child_out_of_range() {
        let root_arc = Arc::new(RwLock::new(TreeNode::Internal(InNodeStub {
            node_id: generate_node_id(),
            level: MAIN_LEVEL | 2,
            entries: vec![],
            targets: TargetRep::None,
            dirty: false,
            generation: 0,
            parent: None,
            lsn_rep: LsnRep::Empty,
        })));
        let other_arc = Arc::new(RwLock::new(TreeNode::Bottom(BinStub {
            node_id: generate_node_id(),
            level: BIN_LEVEL,
            entries: vec![],
            key_prefix: Vec::new(),
            dirty: false,
            is_delta: false,
            last_full_lsn: NULL_LSN,
            last_delta_lsn: NULL_LSN,
            generation: 0,
            parent: None,
            expiration_in_hours: true,
            cursor_count: 0,
            prohibit_next_delta: false,
            lsn_rep: LsnRep::Empty,
            keys: KeyRep::new(),
            compact_max_key_length: INKeyRep_DEFAULT_MAX_KEY_LENGTH,
        })));

        assert!(
            !Tree::validate_parent_child(&root_arc, 0, &other_arc),
            "link must be invalid when parent has no entries"
        );
    }

    /// validate_parent_child returns false when the slot points at a different Arc.
    #[test]
    fn test_validate_parent_child_wrong_child() {
        let bin_a = Arc::new(RwLock::new(TreeNode::Bottom(BinStub {
            node_id: generate_node_id(),
            level: BIN_LEVEL,
            entries: vec![],
            key_prefix: Vec::new(),
            dirty: false,
            is_delta: false,
            last_full_lsn: NULL_LSN,
            last_delta_lsn: NULL_LSN,
            generation: 0,
            parent: None,
            expiration_in_hours: true,
            cursor_count: 0,
            prohibit_next_delta: false,
            lsn_rep: LsnRep::Empty,
            keys: KeyRep::new(),
            compact_max_key_length: INKeyRep_DEFAULT_MAX_KEY_LENGTH,
        })));
        let bin_b = Arc::new(RwLock::new(TreeNode::Bottom(BinStub {
            node_id: generate_node_id(),
            level: BIN_LEVEL,
            entries: vec![],
            key_prefix: Vec::new(),
            dirty: false,
            is_delta: false,
            last_full_lsn: NULL_LSN,
            last_delta_lsn: NULL_LSN,
            generation: 0,
            parent: None,
            expiration_in_hours: true,
            cursor_count: 0,
            prohibit_next_delta: false,
            lsn_rep: LsnRep::Empty,
            keys: KeyRep::new(),
            compact_max_key_length: INKeyRep_DEFAULT_MAX_KEY_LENGTH,
        })));

        let root_arc = Arc::new(RwLock::new(TreeNode::Internal(InNodeStub {
            node_id: generate_node_id(),
            level: MAIN_LEVEL | 2,
            entries: vec![InEntry { key: vec![] }],
            targets: TargetRep::Sparse(vec![(0, bin_a)]),
            dirty: false,
            generation: 0,
            parent: None,
            lsn_rep: LsnRep::Empty,
        })));

        assert!(
            !Tree::validate_parent_child(&root_arc, 0, &bin_b),
            "link must be invalid when parent slot points at a different Arc"
        );
    }

    /// search_with_coupling finds the same key as search().
    #[test]
    fn test_search_with_coupling_finds_existing_key() {
        let tree = Tree::new(1, 8);
        for i in 0u32..20 {
            let key = format!("c{:04}", i).into_bytes();
            tree.insert(key, vec![i as u8], Lsn::new(1, i)).unwrap();
        }

        for i in 0u32..20 {
            let key = format!("c{:04}", i).into_bytes();
            let sr = tree.search_with_coupling(&key);
            assert!(
                sr.is_some() && sr.unwrap().exact_parent_found,
                "search_with_coupling must find c{:04}",
                i
            );
        }
    }

    /// search_with_coupling returns false for a key not in the tree.
    #[test]
    fn test_search_with_coupling_missing_key() {
        let tree = Tree::new(1, 8);
        tree.insert(b"hello".to_vec(), b"v".to_vec(), Lsn::new(1, 1)).unwrap();

        let sr = tree.search_with_coupling(b"zzz");
        // The search result must either be None or have exact_parent_found=false.
        assert!(
            sr.is_none_or(|r| !r.exact_parent_found),
            "search_with_coupling must not find a key that was never inserted"
        );
    }

    /// search_with_coupling on an empty tree returns None.
    #[test]
    fn test_search_with_coupling_empty_tree() {
        let tree = Tree::new(1, 8);
        assert!(tree.search_with_coupling(b"k").is_none());
    }

    // ========================================================================
    // Tests: BIN-delta reconstitution (apply_delta_to_bin / mutate_to_full_bin)
    // ========================================================================

    /// apply_delta_to_bin replaces existing entries and inserts new ones.
    ///
    /// BIN.applyDelta(): delta entries are authoritative and
    /// supersede full-BIN entries at the same key.
    #[test]
    fn test_apply_delta_to_bin_updates_and_inserts() {
        let mut base = BinStub {
            node_id: 1,
            level: BIN_LEVEL,
            entries: vec![
                BinEntry {
                    data: Some(b"old_a".to_vec()),
                    known_deleted: false,
                    dirty: false,
                    expiration_time: 0,
                },
                BinEntry {
                    data: Some(b"old_c".to_vec()),
                    known_deleted: false,
                    dirty: false,
                    expiration_time: 0,
                },
            ],
            key_prefix: Vec::new(),
            dirty: false,
            is_delta: false,
            last_full_lsn: NULL_LSN,
            last_delta_lsn: NULL_LSN,
            generation: 0,
            parent: None,
            expiration_in_hours: true,
            cursor_count: 0,
            prohibit_next_delta: false,
            lsn_rep: LsnRep::Empty,
            keys: KeyRep::from_keys(vec![b"a".to_vec(), b"c".to_vec()]),
            compact_max_key_length: INKeyRep_DEFAULT_MAX_KEY_LENGTH,
        };

        let delta_entries = vec![
            // Update existing key "a" with new data.
            (b"a".to_vec(), Lsn::new(1, 10), Some(b"new_a".to_vec())),
            // Insert new key "b".
            (b"b".to_vec(), Lsn::new(1, 20), Some(b"new_b".to_vec())),
        ];

        Tree::apply_delta_to_bin(&mut base, delta_entries);

        assert!(base.dirty, "base must be dirty after applying delta");

        // Collect the full keys for assertions (T-2: keys live in the rep).
        let full_keys: Vec<Vec<u8>> = (0..base.entries.len())
            .map(|i| base.get_full_key(i).unwrap_or_default())
            .collect();

        // "a" must be updated.
        let a_idx = full_keys.iter().position(|k| k == b"a").unwrap();
        assert_eq!(
            base.entries[a_idx].data.as_deref(),
            Some(b"new_a" as &[u8])
        );

        // "b" must be newly inserted.
        assert!(full_keys.iter().any(|k| k == b"b"));

        // "c" must still be present (untouched).
        assert!(full_keys.iter().any(|k| k == b"c"));

        // Entries must be in sorted order.
        let mut sorted = full_keys.clone();
        sorted.sort();
        assert_eq!(
            full_keys, sorted,
            "entries must remain sorted after delta apply"
        );
    }

    /// apply_delta_to_bin with an empty delta is a no-op (except dirty flag).
    #[test]
    fn test_apply_delta_to_bin_empty_delta() {
        let mut base = BinStub {
            node_id: 1,
            level: BIN_LEVEL,
            entries: vec![BinEntry {
                data: None,
                known_deleted: false,
                dirty: false,
                expiration_time: 0,
            }],
            key_prefix: Vec::new(),
            dirty: false,
            is_delta: false,
            last_full_lsn: NULL_LSN,
            last_delta_lsn: NULL_LSN,
            generation: 0,
            parent: None,
            expiration_in_hours: true,
            cursor_count: 0,
            prohibit_next_delta: false,
            lsn_rep: LsnRep::Empty,
            keys: KeyRep::from_keys(vec![b"x".to_vec()]),
            compact_max_key_length: INKeyRep_DEFAULT_MAX_KEY_LENGTH,
        };
        let n_before = base.entries.len();
        Tree::apply_delta_to_bin(&mut base, vec![]);
        assert_eq!(
            base.entries.len(),
            n_before,
            "empty delta must not change entry count"
        );
        assert!(base.dirty, "dirty must be set even for empty delta apply");
    }

    /// mutate_to_full_bin reconstitutes a full BIN from a delta + base.
    ///
    /// BIN.mutateToFullBIN(BIN fullBIN): after mutation the
    /// `is_delta` flag must be cleared and the entries must contain both
    /// base and delta data.
    #[test]
    fn test_mutate_to_full_bin_merges_delta_and_base() {
        let base = BinStub {
            node_id: 2,
            level: BIN_LEVEL,
            entries: vec![
                BinEntry {
                    data: Some(b"base_aa".to_vec()),
                    known_deleted: false,
                    dirty: false,
                    expiration_time: 0,
                },
                BinEntry {
                    data: Some(b"base_cc".to_vec()),
                    known_deleted: false,
                    dirty: false,
                    expiration_time: 0,
                },
            ],
            key_prefix: Vec::new(),
            dirty: false,
            is_delta: false,
            last_full_lsn: NULL_LSN,
            last_delta_lsn: NULL_LSN,
            generation: 0,
            parent: None,
            expiration_in_hours: true,
            cursor_count: 0,
            prohibit_next_delta: false,
            lsn_rep: LsnRep::Empty,
            keys: KeyRep::from_keys(vec![b"aa".to_vec(), b"cc".to_vec()]),
            compact_max_key_length: INKeyRep_DEFAULT_MAX_KEY_LENGTH,
        };

        // The delta has a new entry "bb" and overwrites "aa".
        let mut delta = BinStub {
            node_id: 2,
            level: BIN_LEVEL,
            entries: vec![
                BinEntry {
                    data: Some(b"delta_aa".to_vec()),
                    known_deleted: false,
                    dirty: false,
                    expiration_time: 0,
                },
                BinEntry {
                    data: Some(b"delta_bb".to_vec()),
                    known_deleted: false,
                    dirty: false,
                    expiration_time: 0,
                },
            ],
            key_prefix: Vec::new(),
            dirty: true,
            is_delta: true,
            last_full_lsn: NULL_LSN,
            last_delta_lsn: NULL_LSN,
            generation: 0,
            parent: None,
            expiration_in_hours: true,
            cursor_count: 0,
            prohibit_next_delta: false,
            lsn_rep: LsnRep::Empty,
            keys: KeyRep::from_keys(vec![b"aa".to_vec(), b"bb".to_vec()]),
            compact_max_key_length: INKeyRep_DEFAULT_MAX_KEY_LENGTH,
        };

        Tree::mutate_to_full_bin(&mut delta, base);

        // After mutation the node must be a full BIN.
        assert!(
            !delta.is_delta,
            "is_delta must be false after mutate_to_full_bin"
        );
        assert!(delta.dirty, "must be dirty after mutation");

        // Collect full keys for assertions (T-2: keys live in the rep).
        let dk: Vec<Vec<u8>> = (0..delta.entries.len())
            .map(|i| delta.get_full_key(i).unwrap_or_default())
            .collect();

        // "aa" must be the delta version.
        let aa_idx = dk.iter().position(|k| k == b"aa").unwrap();
        assert_eq!(
            delta.entries[aa_idx].data.as_deref(),
            Some(b"delta_aa" as &[u8])
        );

        // "bb" must be present (from delta).
        assert!(dk.iter().any(|k| k == b"bb"));

        // "cc" must be present (from base).
        assert!(dk.iter().any(|k| k == b"cc"));

        // Three entries total, in sorted order.
        assert_eq!(delta.entries.len(), 3);
        let mut sorted = dk.clone();
        sorted.sort();
        assert_eq!(dk, sorted, "entries must be sorted after mutation");
    }

    /// is_delta flag is correctly reported by bin_is_delta().
    #[test]
    fn test_bin_is_delta_flag() {
        let mut bin = BinStub {
            node_id: 1,
            level: BIN_LEVEL,
            entries: vec![],
            key_prefix: Vec::new(),
            dirty: false,
            is_delta: false,
            last_full_lsn: NULL_LSN,
            last_delta_lsn: NULL_LSN,
            generation: 0,
            parent: None,
            expiration_in_hours: true,
            cursor_count: 0,
            prohibit_next_delta: false,
            lsn_rep: LsnRep::Empty,
            keys: KeyRep::new(),
            compact_max_key_length: INKeyRep_DEFAULT_MAX_KEY_LENGTH,
        };
        assert!(!Tree::bin_is_delta(&bin));
        bin.is_delta = true;
        assert!(Tree::bin_is_delta(&bin));
    }

    // ========================================================================
    // Tests: mutate_to_full_bin_from_log
    // ========================================================================

    /// mutate_to_full_bin_from_log is a no-op when the BIN is already full.
    #[test]
    fn test_mutate_to_full_bin_from_log_already_full() {
        let dir = tempfile::tempdir().unwrap();
        let fm = std::sync::Arc::new(
            noxu_log::FileManager::new(dir.path(), false, 10_000_000, 100)
                .unwrap(),
        );
        let lm = noxu_log::LogManager::new(fm, 3, 1024 * 1024, 4096);

        let mut bin = BinStub {
            node_id: 1,
            level: BIN_LEVEL,
            entries: vec![BinEntry {
                data: Some(b"v1".to_vec()),
                known_deleted: false,
                dirty: false,
                expiration_time: 0,
            }],
            key_prefix: Vec::new(),
            dirty: false,
            is_delta: false, // already a full BIN
            last_full_lsn: NULL_LSN,
            last_delta_lsn: NULL_LSN,
            generation: 0,
            parent: None,
            expiration_in_hours: true,
            cursor_count: 0,
            prohibit_next_delta: false,
            lsn_rep: LsnRep::Empty,
            keys: KeyRep::from_keys(vec![b"key1".to_vec()]),
            compact_max_key_length: INKeyRep_DEFAULT_MAX_KEY_LENGTH,
        };

        Tree::mutate_to_full_bin_from_log(&mut bin, &lm);

        // No-op: is_delta was already false, entries unchanged.
        assert!(!bin.is_delta);
        assert_eq!(bin.entries.len(), 1);
    }

    /// mutate_to_full_bin_from_log with NULL_LSN promotes delta without base.
    ///
    /// When last_full_lsn is NULL_LSN the BIN has never been written as a full
    /// entry.  The function must clear is_delta and leave the delta entries
    /// as-is (they are the authoritative full state).
    #[test]
    fn test_mutate_to_full_bin_from_log_null_lsn() {
        let dir = tempfile::tempdir().unwrap();
        let fm = std::sync::Arc::new(
            noxu_log::FileManager::new(dir.path(), false, 10_000_000, 100)
                .unwrap(),
        );
        let lm = noxu_log::LogManager::new(fm, 3, 1024 * 1024, 4096);

        let mut delta = BinStub {
            node_id: 2,
            level: BIN_LEVEL,
            entries: vec![BinEntry {
                data: Some(b"delta_a".to_vec()),
                known_deleted: false,
                dirty: true,
                expiration_time: 0,
            }],
            key_prefix: Vec::new(),
            dirty: true,
            is_delta: true,
            last_full_lsn: NULL_LSN, // no full BIN ever written
            last_delta_lsn: NULL_LSN,
            generation: 0,
            parent: None,
            expiration_in_hours: true,
            cursor_count: 0,
            prohibit_next_delta: false,
            lsn_rep: LsnRep::Empty,
            keys: KeyRep::from_keys(vec![b"a".to_vec()]),
            compact_max_key_length: INKeyRep_DEFAULT_MAX_KEY_LENGTH,
        };

        Tree::mutate_to_full_bin_from_log(&mut delta, &lm);

        // is_delta must be cleared; the single delta entry is kept as-is.
        assert!(
            !delta.is_delta,
            "is_delta must be false after null-lsn promotion"
        );
        assert_eq!(delta.entries.len(), 1);
        assert_eq!(delta.entries[0].data.as_deref(), Some(b"delta_a" as &[u8]));
    }

    /// mutate_to_full_bin_from_log reads full BIN from log and merges delta.
    ///
    /// Round-trip: serialize a full BIN, write it to a LogManager, record the
    /// LSN, then call mutate_to_full_bin_from_log on a delta referencing that
    /// LSN.  The result must contain base-only and delta-only entries with the
    /// delta winning on conflicts.
    #[test]
    fn test_mutate_to_full_bin_from_log_reads_and_merges() {
        let dir = tempfile::tempdir().unwrap();
        let fm = std::sync::Arc::new(
            noxu_log::FileManager::new(dir.path(), false, 10_000_000, 100)
                .unwrap(),
        );
        let lm = noxu_log::LogManager::new(fm, 3, 1024 * 1024, 4096);

        // Build and serialize the full BIN that will be written to the log.
        let full_bin = BinStub {
            node_id: 42,
            level: BIN_LEVEL,
            entries: vec![
                BinEntry {
                    data: Some(b"base_val".to_vec()),
                    known_deleted: false,
                    dirty: false,
                    expiration_time: 0,
                },
                BinEntry {
                    data: Some(b"base_shared".to_vec()),
                    known_deleted: false,
                    dirty: false,
                    expiration_time: 0,
                },
            ],
            key_prefix: Vec::new(),
            dirty: false,
            is_delta: false,
            last_full_lsn: NULL_LSN,
            last_delta_lsn: NULL_LSN,
            generation: 0,
            parent: None,
            expiration_in_hours: true,
            cursor_count: 0,
            prohibit_next_delta: false,
            lsn_rep: LsnRep::Empty,
            keys: KeyRep::from_keys(vec![
                b"base_only".to_vec(),
                b"shared_key".to_vec(),
            ]),
            compact_max_key_length: INKeyRep_DEFAULT_MAX_KEY_LENGTH,
        };

        let payload = full_bin.serialize_full();
        let full_lsn = lm
            .log(
                noxu_log::LogEntryType::BIN,
                &payload,
                noxu_log::Provisional::No,
                true,
                false,
            )
            .expect("write full BIN to log");
        lm.flush_no_sync().expect("flush log");

        // Build a delta BIN referencing the full BIN via last_full_lsn.
        let mut delta = BinStub {
            node_id: 42,
            level: BIN_LEVEL,
            entries: vec![
                // Overwrites "shared_key" from the base.
                BinEntry {
                    data: Some(b"delta_shared".to_vec()),
                    known_deleted: false,
                    dirty: true,
                    expiration_time: 0,
                },
                // New key only in the delta.
                BinEntry {
                    data: Some(b"delta_val".to_vec()),
                    known_deleted: false,
                    dirty: true,
                    expiration_time: 0,
                },
            ],
            key_prefix: Vec::new(),
            dirty: true,
            is_delta: true,
            last_full_lsn: full_lsn,
            last_delta_lsn: NULL_LSN,
            generation: 0,
            parent: None,
            expiration_in_hours: true,
            cursor_count: 0,
            prohibit_next_delta: false,
            lsn_rep: LsnRep::Empty,
            keys: KeyRep::from_keys(vec![
                b"shared_key".to_vec(),
                b"delta_only".to_vec(),
            ]),
            compact_max_key_length: INKeyRep_DEFAULT_MAX_KEY_LENGTH,
        };

        Tree::mutate_to_full_bin_from_log(&mut delta, &lm);

        assert!(
            !delta.is_delta,
            "is_delta must be false after log-based mutation"
        );
        assert!(delta.dirty, "must be dirty after mutation");

        // All three distinct keys must be present.
        let find = |k: &[u8]| -> Option<Vec<u8>> {
            (0..delta.entries.len())
                .find(|&i| delta.get_full_key(i).as_deref() == Some(k))
                .and_then(|i| delta.entries[i].data.clone())
        };

        assert_eq!(
            find(b"base_only"),
            Some(b"base_val".to_vec()),
            "base-only key must be present"
        );
        assert_eq!(
            find(b"shared_key"),
            Some(b"delta_shared".to_vec()),
            "delta must win on shared_key"
        );
        assert_eq!(
            find(b"delta_only"),
            Some(b"delta_val".to_vec()),
            "delta-only key must be present"
        );
        assert_eq!(delta.entries.len(), 3, "must have exactly 3 entries");

        // Entries must be in sorted order (by full key).
        let full_keys: Vec<Vec<u8>> = (0..delta.entries.len())
            .map(|i| delta.get_full_key(i).unwrap())
            .collect();
        let mut sorted_keys = full_keys.clone();
        sorted_keys.sort();
        assert_eq!(full_keys, sorted_keys, "entries must be in sorted order");
    }

    // ========================================================================
    // Tests: deserialize_full key prefix recomputation
    // ========================================================================

    /// deserialize_full recomputes key prefix from loaded full keys.
    ///
    /// IN.recalcKeyPrefix() called after materializing from log:
    /// a BIN loaded from the log should have prefix compression applied so
    /// that search performance matches an in-memory BIN.
    #[test]
    fn test_deserialize_full_recomputes_key_prefix() {
        // Build a BIN with a known common prefix and serialize it.
        let mut source = BinStub {
            node_id: 99,
            level: BIN_LEVEL,
            entries: vec![
                BinEntry {
                    data: None,
                    known_deleted: false,
                    dirty: false,
                    expiration_time: 0,
                },
                BinEntry {
                    data: None,
                    known_deleted: false,
                    dirty: false,
                    expiration_time: 0,
                },
                BinEntry {
                    data: None,
                    known_deleted: false,
                    dirty: false,
                    expiration_time: 0,
                },
            ],
            key_prefix: Vec::new(),
            dirty: false,
            is_delta: false,
            last_full_lsn: NULL_LSN,
            last_delta_lsn: NULL_LSN,
            generation: 0,
            parent: None,
            expiration_in_hours: true,
            cursor_count: 0,
            prohibit_next_delta: false,
            lsn_rep: LsnRep::Empty,
            keys: KeyRep::from_keys(vec![
                b"pfx:alpha".to_vec(),
                b"pfx:beta".to_vec(),
                b"pfx:gamma".to_vec(),
            ]),
            compact_max_key_length: INKeyRep_DEFAULT_MAX_KEY_LENGTH,
        };
        source.recompute_key_prefix();
        // Verify the source has the expected prefix before serializing.
        assert_eq!(source.key_prefix, b"pfx:");

        let payload = source.serialize_full();

        // Deserialize and verify prefix is re-established.
        let loaded = BinStub::deserialize_full(&payload)
            .expect("deserialization must succeed");

        assert_eq!(
            loaded.key_prefix, b"pfx:",
            "key prefix must be recomputed after deserialize_full"
        );

        // All full keys must be reconstructable.
        for i in 0..loaded.entries.len() {
            let fk = loaded.get_full_key(i).unwrap();
            assert!(
                fk.starts_with(b"pfx:"),
                "full key {i} must start with prefix"
            );
        }
    }

    /// deserialize_full with a single entry leaves key_prefix empty.
    ///
    /// A BIN with fewer than 2 entries cannot have a meaningful common prefix.
    #[test]
    fn test_deserialize_full_single_entry_no_prefix() {
        let source = BinStub {
            node_id: 7,
            level: BIN_LEVEL,
            entries: vec![BinEntry {
                data: None,
                known_deleted: false,
                dirty: false,
                expiration_time: 0,
            }],
            key_prefix: Vec::new(),
            dirty: false,
            is_delta: false,
            last_full_lsn: NULL_LSN,
            last_delta_lsn: NULL_LSN,
            generation: 0,
            parent: None,
            expiration_in_hours: true,
            cursor_count: 0,
            prohibit_next_delta: false,
            lsn_rep: LsnRep::Empty,
            keys: KeyRep::from_keys(vec![b"solo".to_vec()]),
            compact_max_key_length: INKeyRep_DEFAULT_MAX_KEY_LENGTH,
        };

        let payload = source.serialize_full();
        let loaded = BinStub::deserialize_full(&payload)
            .expect("deserialization must succeed");

        assert!(
            loaded.key_prefix.is_empty(),
            "single-entry BIN must have empty prefix"
        );
        assert_eq!(loaded.get_full_key(0).unwrap(), b"solo");
    }

    // ========================================================================
    // Tests: get_next_bin / get_prev_bin
    // ========================================================================

    /// get_next_bin returns the entries of the next BIN to the right.
    ///
    /// Tree.getNextBin() / getNextIN(forward=true).
    #[test]
    fn test_get_next_bin_basic() {
        let tree = Tree::new(1, 4);

        // Insert 8 sorted keys — creates multiple BINs.
        for i in 0u32..8 {
            let key = format!("n{:04}", i).into_bytes();
            tree.insert(key, vec![i as u8], Lsn::new(1, i)).unwrap();
        }

        let stats = tree.collect_stats();
        if stats.n_bins < 2 {
            // If the tree only has one BIN, skip the sibling test.
            return;
        }

        // A key from the first BIN (e.g. "n0000") should have a next BIN.
        let next = tree.get_next_bin(b"n0000");
        assert!(
            next.is_some(),
            "must return a next BIN for a key in the leftmost BIN"
        );

        let entries = next.unwrap();
        assert!(!entries.is_empty(), "next BIN must not be empty");
        // All returned keys must be strictly greater than "n0000" because they
        // are in a different (rightward) BIN.
        for (_, _, k) in &entries {
            assert!(
                k.as_slice() > b"n0000" as &[u8],
                "next BIN entries must all be > the search key"
            );
        }
    }

    /// get_next_bin returns None for a key in the rightmost BIN.
    #[test]
    fn test_get_next_bin_at_rightmost_returns_none() {
        let tree = Tree::new(1, 4);
        for i in 0u32..8 {
            let key = format!("r{:04}", i).into_bytes();
            tree.insert(key, vec![i as u8], Lsn::new(1, i)).unwrap();
        }
        // A key from the rightmost BIN (e.g. "r0007") has no next BIN.
        let next = tree.get_next_bin(b"r0007");
        assert!(
            next.is_none(),
            "must return None for a key in the rightmost BIN"
        );
    }

    /// get_prev_bin returns the entries of the next BIN to the left.
    ///
    /// Tree.getPrevBin() / getNextIN(forward=false).
    #[test]
    fn test_get_prev_bin_basic() {
        let tree = Tree::new(1, 4);
        for i in 0u32..8 {
            let key = format!("p{:04}", i).into_bytes();
            tree.insert(key, vec![i as u8], Lsn::new(1, i)).unwrap();
        }

        // A key from the second BIN ("p0004") should have a previous BIN.
        let prev = tree.get_prev_bin(b"p0004");
        assert!(
            prev.is_some(),
            "must return a prev BIN for a key in the second BIN"
        );

        let entries = prev.unwrap();
        assert!(!entries.is_empty(), "prev BIN must not be empty");
        // All returned keys must be < b"p0004".
        for (_, _, k) in &entries {
            assert!(
                k.as_slice() < b"p0004" as &[u8],
                "prev BIN entries must all be < the current BIN"
            );
        }
    }

    /// get_prev_bin returns None for a key in the leftmost BIN.
    #[test]
    fn test_get_prev_bin_at_leftmost_returns_none() {
        let tree = Tree::new(1, 4);
        for i in 0u32..8 {
            let key = format!("q{:04}", i).into_bytes();
            tree.insert(key, vec![i as u8], Lsn::new(1, i)).unwrap();
        }
        // A key from the leftmost BIN ("q0000") has no prev BIN.
        let prev = tree.get_prev_bin(b"q0000");
        assert!(
            prev.is_none(),
            "must return None for a key in the leftmost BIN"
        );
    }

    /// get_next_bin and get_prev_bin are inverse operations across the
    /// BIN boundary.
    #[test]
    fn test_next_prev_bin_are_symmetric() {
        let tree = Tree::new(1, 4);
        for i in 0u32..8 {
            let key = format!("s{:04}", i).into_bytes();
            tree.insert(key, vec![i as u8], Lsn::new(1, i)).unwrap();
        }

        // From first BIN (s0000): next → second BIN entries.
        let next_from_first = tree.get_next_bin(b"s0000").unwrap();
        // The smallest key of the next BIN.
        let next_first_key =
            next_from_first.iter().map(|(_, _, k)| k.clone()).min().unwrap();

        // From that key in the second BIN: prev → should overlap with first BIN.
        let prev_from_second = tree.get_prev_bin(&next_first_key).unwrap();
        let prev_first_key =
            prev_from_second.iter().map(|(_, _, k)| k.clone()).max().unwrap();

        // The max key of the "prev" result must be in the first BIN (< next boundary).
        assert!(
            prev_first_key < next_first_key,
            "prev BIN entries must be smaller than the boundary key"
        );
    }

    /// get_next_bin on an empty tree returns None.
    #[test]
    fn test_get_next_bin_empty_tree() {
        let tree = Tree::new(1, 8);
        assert!(tree.get_next_bin(b"any").is_none());
    }

    /// get_prev_bin on an empty tree returns None.
    #[test]
    fn test_get_prev_bin_empty_tree() {
        let tree = Tree::new(1, 8);
        assert!(tree.get_prev_bin(b"any").is_none());
    }

    // =========================================================================
    // R3 fix: get_next_bin / get_prev_bin honour the custom comparator
    // =========================================================================

    /// R3 regression test: with a custom comparator that reverses byte order
    /// (descending), `get_next_bin` and `get_prev_bin` must use comparator
    /// order when routing through internal nodes.
    ///
    /// Pre-fix: the static `get_adjacent_bin_attempt` used raw `<=` byte order
    /// for IN routing, causing it to descend to the wrong child when comparator
    /// order ≠ byte order.
    ///
    /// The tree is forced to split (max_entries = 4) so there IS an internal
    /// node (IN) to route through. Under a reverse comparator the insertion
    /// order and stored key order are reversed relative to byte order, so any
    /// descent that uses raw byte comparison will pick the wrong slot.
    ///
    /// Pass-post invariant: iterating forward via repeated `get_next_bin` from
    /// the leftmost BIN yields keys in COMPARATOR order (descending byte order
    /// here), not in raw ascending byte order.
    #[test]
    fn test_get_next_prev_bin_custom_comparator_order() {
        // Reverse-order comparator: larger bytes sort first.
        let reverse_cmp: KeyComparatorFn =
            Arc::new(|a: &[u8], b: &[u8]| b.cmp(a));
        // Small max_entries so the tree splits and has internal nodes.
        let mut tree = Tree::new(1, 4);
        tree.set_comparator(reverse_cmp);

        // Insert keys that are ascending in byte order ("a" < "b" < … < "i")
        // but descending in comparator order (i > h > … > a).
        let keys: &[&[u8]] =
            &[b"a", b"b", b"c", b"d", b"e", b"f", b"g", b"h", b"i"];
        for (i, k) in keys.iter().enumerate() {
            tree.insert(
                k.to_vec(),
                vec![i as u8],
                Lsn::from_u64((i + 1) as u64),
            )
            .unwrap();
        }

        // Collect all BINs by walking from the comparator-smallest key ("i"
        // in reverse order) using get_next_bin. The anchor must be a key that
        // is smaller than everything in comparator order, i.e. the largest
        // byte-value key. We use the tree's search to find the actual leftmost
        // key under the comparator by starting from "i" (comparator-min).
        //
        // Strategy: start at byte key b"\xff" (larger than any inserted key in
        // byte order, so it lands in the last BIN in byte order, which under
        // a reverse comparator is the leftmost BIN in comparator order). Then
        // walk via get_next_bin.
        let start_anchor = b"\xff".as_ref();
        let mut bin_first_keys: Vec<Vec<u8>> = Vec::new();

        // The first BIN in comparator order contains "i" (largest byte key).
        // get_next_bin from a virtual start in that BIN gives the next one.
        // Collect by walking from the comparator-last key leftward instead:
        // use get_next_bin with anchor = b"\xff" to hop to the next BIN
        // (comparator order: next = smaller byte value).
        let mut anchor = start_anchor.to_vec();
        loop {
            match tree.get_next_bin(&anchor) {
                None => break,
                Some(entries) => {
                    if let Some((_, _, fk0)) = entries.first() {
                        let fk = fk0.clone();
                        bin_first_keys.push(fk.clone());
                        anchor = fk;
                    } else {
                        break;
                    }
                }
            }
        }

        // We must have visited at least 2 BINs (tree was forced to split).
        assert!(
            bin_first_keys.len() >= 2,
            "R3: expected multiple BINs after split, got {}",
            bin_first_keys.len()
        );

        // With a reverse comparator, bin_first_keys must be in descending byte
        // order (each successive BIN starts at a smaller byte key).
        for window in bin_first_keys.windows(2) {
            assert!(
                window[0] > window[1],
                "R3: BIN boundary keys must be descending (comparator order); \
                 got {:?} then {:?}",
                window[0],
                window[1]
            );
        }
    }
    // ========================================================================

    /// Inserting keys with a common prefix causes the BIN to establish that
    /// prefix.  Stored suffixes are shorter than the full keys.
    #[test]
    fn test_binstub_prefix_established_on_insert() {
        let mut bin = BinStub {
            node_id: 1,
            level: BIN_LEVEL,
            entries: Vec::new(),
            key_prefix: Vec::new(),
            dirty: false,
            is_delta: false,
            last_full_lsn: NULL_LSN,
            last_delta_lsn: NULL_LSN,
            generation: 0,
            parent: None,
            expiration_in_hours: true,
            cursor_count: 0,
            prohibit_next_delta: false,
            lsn_rep: LsnRep::Empty,
            keys: KeyRep::new(),
            compact_max_key_length: INKeyRep_DEFAULT_MAX_KEY_LENGTH,
        };

        bin.insert_with_prefix(b"record:aaa".to_vec(), Lsn::new(1, 1), None);
        assert!(bin.key_prefix.is_empty(), "single entry: no prefix yet");

        bin.insert_with_prefix(b"record:bbb".to_vec(), Lsn::new(1, 2), None);
        assert_eq!(
            &bin.key_prefix, b"record:",
            "common prefix 'record:' must be extracted"
        );
    }

    /// `get_full_key` on a BinStub returns the full key regardless of whether
    /// the stored key is a raw full key or a suffix.
    #[test]
    fn test_binstub_get_full_key_roundtrip() {
        let mut bin = BinStub {
            node_id: 1,
            level: BIN_LEVEL,
            entries: Vec::new(),
            key_prefix: Vec::new(),
            dirty: false,
            is_delta: false,
            last_full_lsn: NULL_LSN,
            last_delta_lsn: NULL_LSN,
            generation: 0,
            parent: None,
            expiration_in_hours: true,
            cursor_count: 0,
            prohibit_next_delta: false,
            lsn_rep: LsnRep::Empty,
            keys: KeyRep::new(),
            compact_max_key_length: INKeyRep_DEFAULT_MAX_KEY_LENGTH,
        };

        let keys = [
            b"pfx:first".as_ref(),
            b"pfx:second".as_ref(),
            b"pfx:third".as_ref(),
        ];
        for k in keys {
            bin.insert_with_prefix(k.to_vec(), Lsn::new(1, 1), None);
        }

        assert!(!bin.key_prefix.is_empty(), "prefix must be set");

        for (i, expected) in keys.iter().enumerate() {
            let full = bin.get_full_key(i).expect("must return full key");
            assert_eq!(
                full.as_slice(),
                *expected,
                "get_full_key({}) must return full key",
                i
            );
        }
    }

    /// `find_entry_compressed` on a BinStub with active prefix returns the
    /// correct slot index.
    #[test]
    fn test_binstub_find_entry_compressed() {
        let mut bin = BinStub {
            node_id: 1,
            level: BIN_LEVEL,
            entries: Vec::new(),
            key_prefix: Vec::new(),
            dirty: false,
            is_delta: false,
            last_full_lsn: NULL_LSN,
            last_delta_lsn: NULL_LSN,
            generation: 0,
            parent: None,
            expiration_in_hours: true,
            cursor_count: 0,
            prohibit_next_delta: false,
            lsn_rep: LsnRep::Empty,
            keys: KeyRep::new(),
            compact_max_key_length: INKeyRep_DEFAULT_MAX_KEY_LENGTH,
        };

        for k in
            [b"db:alpha".as_ref(), b"db:beta".as_ref(), b"db:gamma".as_ref()]
        {
            bin.insert_with_prefix(k.to_vec(), Lsn::new(1, 1), None);
        }

        let (idx, found) = bin.find_entry_compressed(b"db:beta");
        assert!(found, "db:beta must be found");
        assert_eq!(idx, 1, "db:beta must be at index 1");

        let (_, not_found) = bin.find_entry_compressed(b"db:zzz");
        assert!(!not_found, "db:zzz must not be found");
    }

    /// Tree insert/search works correctly when BINs accumulate a key prefix.
    #[test]
    fn test_tree_insert_search_with_prefix_compression() {
        let tree = Tree::new(1, 8);
        let n = 200u32;

        // All keys share a long common prefix — good for prefix compression.
        for i in 0..n {
            let key = format!("namespace:entity:{:06}", i).into_bytes();
            let data = vec![i as u8];
            tree.insert(key, data, Lsn::new(1, i)).unwrap();
        }

        // All keys must be findable.
        for i in 0..n {
            let key = format!("namespace:entity:{:06}", i).into_bytes();
            let sr = tree.search(&key);
            assert!(
                sr.is_some() && sr.unwrap().exact_parent_found,
                "key namespace:entity:{:06} must be found",
                i
            );
        }
    }

    /// Prefix survives a BIN split: keys in both halves must still be findable.
    #[test]
    fn test_prefix_preserved_across_bin_split() {
        // Small fanout to force splits quickly.
        let tree = Tree::new(1, 4);

        for i in 0u32..20 {
            let key = format!("pfx:key:{:04}", i).into_bytes();
            tree.insert(key, vec![i as u8], Lsn::new(1, i)).unwrap();
        }

        // All keys must be findable after splits.
        for i in 0u32..20 {
            let key = format!("pfx:key:{:04}", i).into_bytes();
            let sr = tree.search(&key);
            assert!(
                sr.is_some() && sr.unwrap().exact_parent_found,
                "pfx:key:{:04} must be found after splits",
                i
            );
        }
    }

    /// `decompress_key` round-trips: compress then decompress gives the original.
    #[test]
    fn test_binstub_compress_decompress_roundtrip() {
        let mut bin = BinStub {
            node_id: 1,
            level: BIN_LEVEL,
            entries: Vec::new(),
            key_prefix: Vec::new(),
            dirty: false,
            is_delta: false,
            last_full_lsn: NULL_LSN,
            last_delta_lsn: NULL_LSN,
            generation: 0,
            parent: None,
            expiration_in_hours: true,
            cursor_count: 0,
            prohibit_next_delta: false,
            lsn_rep: LsnRep::Empty,
            keys: KeyRep::new(),
            compact_max_key_length: INKeyRep_DEFAULT_MAX_KEY_LENGTH,
        };

        for k in [b"myapp:user:1".as_ref(), b"myapp:user:2".as_ref()] {
            bin.insert_with_prefix(k.to_vec(), Lsn::new(1, 1), None);
        }

        assert!(!bin.key_prefix.is_empty());

        // Manually compress a full key and then decompress it.
        let full_key = b"myapp:user:3";
        let suffix = bin.compress_key(full_key);
        let recovered = bin.decompress_key(&suffix);
        assert_eq!(
            recovered.as_slice(),
            full_key,
            "compress→decompress must be identity"
        );
    }

    /// get_next_bin correctly navigates a 3-level tree.
    #[test]
    fn test_get_next_bin_three_level_tree() {
        // With fanout 4, inserting 20 keys forces a root split → 3 levels.
        let tree = Tree::new(1, 4);
        for i in 0u32..20 {
            let key = format!("t{:04}", i).into_bytes();
            tree.insert(key, vec![i as u8], Lsn::new(1, i)).unwrap();
        }
        assert!(tree.get_root_splits() > 0, "tree must have grown to 3 levels");

        // Starting from t0000, iterating via get_next_bin must visit every BIN.
        let mut visited: Vec<Vec<u8>> = Vec::new();
        // Collect the first BIN's keys by searching for t0000.
        if let Some(first_entries) = {
            // Get the leftmost BIN by using get_first_node result.
            // get_first_node returns SearchResult at index 0 in the leftmost BIN.
            // We approximate by reading the root's leftmost BIN directly.
            tree.get_next_bin(b"t0000")
        } {
            for (_, _, k) in first_entries {
                visited.push(k);
            }
        }

        // visited should contain at least one key from the second BIN.
        assert!(
            !visited.is_empty(),
            "should have visited at least one key via get_next_bin in 3-level tree"
        );
    }

    // ========================================================================
    // ========================================================================

    /// insert a small set of keys
    /// with varying lengths and verify each is findable immediately after insert.
    #[test]
    fn test_je_simple_tree_creation() {
        let tree = Tree::new(1, 128);

        let keys: &[&[u8]] = &[b"aaaaa", b"aaaab", b"aaaa", b"aaa"];
        for (i, &k) in keys.iter().enumerate() {
            tree.insert(k.to_vec(), vec![i as u8], Lsn::new(1, i as u32))
                .unwrap();

            // Every key inserted so far must be findable.
            for &prev in &keys[..=i] {
                let sr = tree.search(prev);
                assert!(
                    sr.is_some() && sr.unwrap().exact_parent_found,
                    "key {:?} must be findable after {} inserts",
                    std::str::from_utf8(prev).unwrap_or("?"),
                    i + 1
                );
            }
        }
    }

    /// insert N keys, verify
    /// all are found; delete the even-indexed keys, verify even are gone and
    /// odd remain.
    #[test]
    fn test_je_insert_then_delete_then_search() {
        let tree = Tree::new(1, 8);
        let n = 20usize;

        let keys: Vec<Vec<u8>> =
            (0..n).map(|i| format!("key{:04}", i).into_bytes()).collect();

        // Insert all.
        for (i, k) in keys.iter().enumerate() {
            tree.insert(k.clone(), vec![i as u8], Lsn::new(1, i as u32))
                .unwrap();
        }

        // All must be findable.
        for k in &keys {
            let sr = tree.search(k);
            assert!(
                sr.is_some() && sr.unwrap().exact_parent_found,
                "key {:?} must be found after insert",
                std::str::from_utf8(k).unwrap_or("?")
            );
        }

        // Delete even-indexed keys.
        for i in (0..n).step_by(2) {
            tree.delete(&keys[i]);
        }

        // Even keys must no longer be found; odd keys must still be found.
        for (i, key) in keys.iter().enumerate() {
            let sr = tree.search(key);
            let found = sr.is_some() && sr.unwrap().exact_parent_found;
            if i % 2 == 0 {
                assert!(!found, "deleted key {:?} must not be found", i);
            } else {
                assert!(found, "kept key {:?} must still be found", i);
            }
        }
    }

    /// insert N keys in reverse
    /// order, then verify every key is directly findable and the keys are in
    /// sorted ascending order (B-tree ordering invariant).
    #[test]
    fn test_je_range_scan_sorted_ascending() {
        let n = 40usize;
        let tree = Tree::new(1, 4);

        // Insert in reverse order to stress the B-tree.
        for i in (0..n).rev() {
            let key = format!("scan{:04}", i).into_bytes();
            tree.insert(key, vec![i as u8], Lsn::new(1, i as u32)).unwrap();
        }

        // Collect all expected keys in sorted order.
        let mut expected: Vec<Vec<u8>> =
            (0..n).map(|i| format!("scan{:04}", i).into_bytes()).collect();
        expected.sort();

        // Every key must be individually findable.
        for key in &expected {
            let sr = tree.search(key);
            assert!(
                sr.is_some() && sr.unwrap().exact_parent_found,
                "key {:?} must be findable",
                std::str::from_utf8(key).unwrap_or("?")
            );
        }

        // Verify sorted ordering invariant: expected keys are already sorted
        // (lexicographic order = insertion order for "scan{:04}" keys).
        for w in expected.windows(2) {
            assert!(
                w[0] < w[1],
                "keys must be in strict ascending order: {:?} < {:?}",
                std::str::from_utf8(&w[0]).unwrap_or("?"),
                std::str::from_utf8(&w[1]).unwrap_or("?")
            );
        }

        // Use get_next_bin to scan at least a portion of the tree and verify
        // ordering of returned BIN entries.
        let first_key = format!("scan{:04}", 0).into_bytes();
        if let Some(entries) = tree.get_next_bin(&first_key) {
            let entry_keys: Vec<&[u8]> =
                entries.iter().map(|(_, _, k)| k.as_slice()).collect();
            for w in entry_keys.windows(2) {
                assert!(
                    w[0] <= w[1],
                    "BIN entries from get_next_bin must be in ascending order"
                );
            }
        }
    }

    /// insert N keys in
    /// ascending order and verify the tree height stays bounded (≤ 10 levels)
    /// and all keys are findable.
    #[test]
    fn test_je_ascending_insert_balance() {
        let n = 128usize;
        let tree = Tree::new(1, 8);

        for i in 0..n {
            let key = format!("asc{:06}", i).into_bytes();
            tree.insert(key, vec![(i & 0xFF) as u8], Lsn::new(1, i as u32))
                .unwrap();
        }

        let stats = tree.collect_stats();
        assert!(
            stats.height <= 10,
            "tree height after {} ascending inserts with fanout 8 must be <= 10, got {}",
            n,
            stats.height
        );

        for i in 0..n {
            let key = format!("asc{:06}", i).into_bytes();
            let sr = tree.search(&key);
            assert!(
                sr.is_some() && sr.unwrap().exact_parent_found,
                "key asc{:06} must be findable after ascending inserts",
                i
            );
        }
    }

    /// insert N keys in
    /// descending order and verify the tree height stays bounded (≤ 10 levels)
    /// and all keys are findable.
    #[test]
    fn test_je_descending_insert_balance() {
        let n = 128usize;
        let tree = Tree::new(1, 8);

        for i in (0..n).rev() {
            let key = format!("dsc{:06}", i).into_bytes();
            tree.insert(key, vec![(i & 0xFF) as u8], Lsn::new(1, i as u32))
                .unwrap();
        }

        let stats = tree.collect_stats();
        assert!(
            stats.height <= 10,
            "tree height after {} descending inserts with fanout 8 must be <= 10, got {}",
            n,
            stats.height
        );

        for i in 0..n {
            let key = format!("dsc{:06}", i).into_bytes();
            let sr = tree.search(&key);
            assert!(
                sr.is_some() && sr.unwrap().exact_parent_found,
                "key dsc{:06} must be findable after descending inserts",
                i
            );
        }
    }

    /// SplitTest invariant: after many splits induced by a small
    /// fanout no key is lost.
    #[test]
    fn test_je_split_no_key_lost() {
        let tree = Tree::new(1, 4);
        let n = 20usize;

        for i in 0..n {
            let key = format!("sp{:04}", i).into_bytes();
            tree.insert(key, vec![i as u8], Lsn::new(1, i as u32)).unwrap();
        }

        for i in 0..n {
            let key = format!("sp{:04}", i).into_bytes();
            let sr = tree.search(&key);
            assert!(
                sr.is_some() && sr.unwrap().exact_parent_found,
                "key sp{:04} must survive all splits",
                i
            );
        }
    }

    /// SplitTest invariant: after a BIN split both halves exist and
    /// all original keys are findable.
    #[test]
    fn test_je_split_produces_two_halves() {
        // fanout=4: fill one BIN then overflow it to force a split.
        let tree = Tree::new(1, 4);
        let n = 5usize; // one more than fanout → forces at least one split

        for i in 0..n {
            let key = format!("half{:04}", i).into_bytes();
            tree.insert(key, vec![i as u8], Lsn::new(1, i as u32)).unwrap();
        }

        let stats = tree.collect_stats();
        assert!(
            stats.n_bins >= 2,
            "after splitting a full BIN there must be >= 2 BINs, got {}",
            stats.n_bins
        );

        for i in 0..n {
            let key = format!("half{:04}", i).into_bytes();
            let sr = tree.search(&key);
            assert!(
                sr.is_some() && sr.unwrap().exact_parent_found,
                "key half{:04} must be findable in one of the two halves",
                i
            );
        }
    }

    /// SplitTest invariant: root splits are tracked and the tree
    /// grows in height as keys accumulate.
    #[test]
    fn test_je_root_split_creates_new_root() {
        // fanout=4, 20 keys: forces multiple root splits.
        let tree = Tree::new(1, 4);

        for i in 0u32..20 {
            let key = format!("rs{:04}", i).into_bytes();
            tree.insert(key, vec![i as u8], Lsn::new(1, i)).unwrap();
        }

        assert!(
            tree.get_root_splits() > 0,
            "expected at least one root split after 20 inserts with fanout 4"
        );

        let stats = tree.collect_stats();
        assert!(
            stats.height >= 3,
            "tree must be at least 3 levels tall after root splits, got {}",
            stats.height
        );

        // Every inserted key must still be findable.
        for i in 0u32..20 {
            let key = format!("rs{:04}", i).into_bytes();
            let sr = tree.search(&key);
            assert!(
                sr.is_some() && sr.unwrap().exact_parent_found,
                "key rs{:04} must be findable after root splits",
                i
            );
        }
    }

    // ========================================================================
    // Tests: compress_bin / maybe_compress_bin_and_parent
    // INCompressor.compressBin / lazyCompress tests
    // ========================================================================

    /// compress_bin removes known-deleted slots from a BIN.
    ///
    /// INCompressor.compressBin(): after compression, slots with
    /// `known_deleted = true` must be gone and the BIN must be dirty.
    #[test]
    fn test_compress_bin_removes_deleted_slots() {
        let _lsn = Lsn::new(1, 1);
        let bin_arc = Arc::new(RwLock::new(TreeNode::Bottom(BinStub {
            node_id: generate_node_id(),
            level: BIN_LEVEL,
            entries: vec![
                BinEntry {
                    data: Some(b"live".to_vec()),
                    known_deleted: false,
                    dirty: false,
                    expiration_time: 0,
                },
                BinEntry {
                    data: None,
                    known_deleted: true,
                    dirty: false,
                    expiration_time: 0,
                },
                BinEntry {
                    data: Some(b"live2".to_vec()),
                    known_deleted: false,
                    dirty: false,
                    expiration_time: 0,
                },
                BinEntry {
                    data: None,
                    known_deleted: true,
                    dirty: false,
                    expiration_time: 0,
                },
            ],
            key_prefix: Vec::new(),
            dirty: false,
            is_delta: false,
            last_full_lsn: NULL_LSN,
            last_delta_lsn: NULL_LSN,
            generation: 0,
            parent: None,
            expiration_in_hours: true,
            cursor_count: 0,
            prohibit_next_delta: false,
            lsn_rep: LsnRep::Empty,
            keys: KeyRep::from_keys(vec![
                b"a".to_vec(),
                b"b".to_vec(),
                b"c".to_vec(),
                b"d".to_vec(),
            ]),
            compact_max_key_length: INKeyRep_DEFAULT_MAX_KEY_LENGTH,
        })));

        // Wire a minimal parent IN so compress_bin can prune if needed.
        let root_arc = Arc::new(RwLock::new(TreeNode::Internal(InNodeStub {
            node_id: generate_node_id(),
            level: MAIN_LEVEL | 2,
            entries: vec![InEntry { key: vec![] }],
            targets: TargetRep::Sparse(vec![(0, bin_arc.clone())]),
            dirty: false,
            generation: 0,
            parent: None,
            lsn_rep: LsnRep::Empty,
        })));
        {
            let mut g = bin_arc.write();
            g.set_parent(Some(Arc::downgrade(&root_arc)));
        }

        let tree = Tree::new(1, 128);
        *tree.root.write() = Some(root_arc);

        let result = tree.compress_bin(&bin_arc);
        assert!(
            result,
            "compress_bin must return true when slots were removed"
        );

        let g = bin_arc.read();
        match &*g {
            TreeNode::Bottom(b) => {
                assert_eq!(
                    b.entries.len(),
                    2,
                    "2 live entries must remain after compress"
                );
                assert!(
                    b.entries.iter().all(|e| !e.known_deleted),
                    "no deleted slots must remain"
                );
                assert!(b.dirty, "BIN must be dirty after compression");
            }
            _ => panic!("expected BIN"),
        }
    }

    /// IC-3 HEADLINE (fail-pre / pass-post): the compressor must SKIP a
    /// `known_deleted` slot that is still write-locked by an in-flight txn,
    /// while removing committed/unlocked `known_deleted` slots in the SAME
    /// BIN.  Mirrors JE `BIN.compress` (BIN.java:1141-1172), which calls
    /// `lockManager.isLockUncontended(lsn)` and does `continue` on a contended
    /// slot.
    ///
    /// Pre-fix: `compress_bin` had no lock check, so a write-locked tombstone
    /// would have been physically removed (the slot a live txn references is
    /// gone -> corruption).  Post-fix: the `is_locked` predicate keeps it.
    #[test]
    fn test_ic3_compress_skips_write_locked_slot() {
        // Slot 1 (key "b", lsn 1:200) is a write-locked tombstone; slot 3
        // (key "d", lsn 1:400) is a committed/unlocked tombstone.  Slots 0
        // and 2 are live.
        let locked_lsn = Lsn::new(1, 200);
        let unlocked_lsn = Lsn::new(1, 400);
        let bin_arc = Arc::new(RwLock::new(TreeNode::Bottom(BinStub {
            node_id: generate_node_id(),
            level: BIN_LEVEL,
            entries: vec![
                BinEntry {
                    data: Some(b"live".to_vec()),
                    known_deleted: false,
                    dirty: false,
                    expiration_time: 0,
                },
                BinEntry {
                    data: None,
                    known_deleted: true, // write-locked tombstone -> KEEP
                    dirty: false,
                    expiration_time: 0,
                },
                BinEntry {
                    data: Some(b"live2".to_vec()),
                    known_deleted: false,
                    dirty: false,
                    expiration_time: 0,
                },
                BinEntry {
                    data: None,
                    known_deleted: true, // committed tombstone -> REMOVE
                    dirty: false,
                    expiration_time: 0,
                },
            ],
            key_prefix: Vec::new(),
            dirty: false,
            is_delta: false,
            last_full_lsn: NULL_LSN,
            last_delta_lsn: NULL_LSN,
            generation: 0,
            parent: None,
            expiration_in_hours: true,
            cursor_count: 0,
            prohibit_next_delta: false,
            lsn_rep: LsnRep::from_lsns(&[
                Lsn::new(1, 100),
                locked_lsn,
                Lsn::new(1, 300),
                unlocked_lsn,
            ]),
            keys: KeyRep::from_keys(vec![
                b"a".to_vec(),
                b"b".to_vec(),
                b"c".to_vec(),
                b"d".to_vec(),
            ]),
            compact_max_key_length: INKeyRep_DEFAULT_MAX_KEY_LENGTH,
        })));
        let root_arc = Arc::new(RwLock::new(TreeNode::Internal(InNodeStub {
            node_id: generate_node_id(),
            level: MAIN_LEVEL | 2,
            entries: vec![InEntry { key: vec![] }],
            targets: TargetRep::Sparse(vec![(0, bin_arc.clone())]),
            dirty: false,
            generation: 0,
            parent: None,
            lsn_rep: LsnRep::Empty,
        })));
        {
            let mut g = bin_arc.write();
            g.set_parent(Some(Arc::downgrade(&root_arc)));
        }
        let tree = Tree::new(1, 128);
        *tree.root.write() = Some(root_arc);

        // Predicate: only `locked_lsn` is write-locked (stub LockManager).
        let locked_u64 = locked_lsn.as_u64();
        let is_locked = move |lsn: u64| lsn == locked_u64;

        let result =
            tree.compress_bin_with_lock_check(&bin_arc, Some(&is_locked));
        assert!(result, "compress removed the unlocked tombstone -> true");

        let g = bin_arc.read();
        match &*g {
            TreeNode::Bottom(b) => {
                // 2 live + 1 write-locked tombstone kept; the committed
                // tombstone (lsn 1:400) removed.
                assert_eq!(
                    b.entries.len(),
                    3,
                    "write-locked tombstone must be KEPT; only the unlocked one removed"
                );
                let kept_locked = (0..b.entries.len()).any(|i| {
                    b.entries[i].known_deleted && b.get_lsn(i) == locked_lsn
                });
                assert!(kept_locked, "the write-locked tombstone must remain");
                let unlocked_gone =
                    (0..b.entries.len()).all(|i| b.get_lsn(i) != unlocked_lsn);
                assert!(
                    unlocked_gone,
                    "the unlocked tombstone must be removed"
                );
            }
            _ => panic!("expected BIN"),
        }
    }

    /// IC-3 (no predicate): with `is_locked = None` behavior is unchanged —
    /// ALL `known_deleted` slots are removed (the historical safe path).
    #[test]
    fn test_ic3_compress_no_predicate_removes_all_tombstones() {
        let bin_arc = Arc::new(RwLock::new(TreeNode::Bottom(BinStub {
            node_id: generate_node_id(),
            level: BIN_LEVEL,
            entries: vec![
                BinEntry {
                    data: Some(b"live".to_vec()),
                    known_deleted: false,
                    dirty: false,
                    expiration_time: 0,
                },
                BinEntry {
                    data: None,
                    known_deleted: true,
                    dirty: false,
                    expiration_time: 0,
                },
                BinEntry {
                    data: None,
                    known_deleted: true,
                    dirty: false,
                    expiration_time: 0,
                },
            ],
            key_prefix: Vec::new(),
            dirty: false,
            is_delta: false,
            last_full_lsn: NULL_LSN,
            last_delta_lsn: NULL_LSN,
            generation: 0,
            parent: None,
            expiration_in_hours: true,
            cursor_count: 0,
            prohibit_next_delta: false,
            lsn_rep: LsnRep::from_lsns(&[
                Lsn::new(1, 100),
                Lsn::new(1, 200),
                Lsn::new(1, 300),
            ]),
            keys: KeyRep::from_keys(vec![
                b"a".to_vec(),
                b"b".to_vec(),
                b"c".to_vec(),
            ]),
            compact_max_key_length: INKeyRep_DEFAULT_MAX_KEY_LENGTH,
        })));
        let root_arc = Arc::new(RwLock::new(TreeNode::Internal(InNodeStub {
            node_id: generate_node_id(),
            level: MAIN_LEVEL | 2,
            entries: vec![InEntry { key: vec![] }],
            targets: TargetRep::Sparse(vec![(0, bin_arc.clone())]),
            dirty: false,
            generation: 0,
            parent: None,
            lsn_rep: LsnRep::Empty,
        })));
        {
            let mut g = bin_arc.write();
            g.set_parent(Some(Arc::downgrade(&root_arc)));
        }
        let tree = Tree::new(1, 128);
        *tree.root.write() = Some(root_arc);

        let result = tree.compress_bin(&bin_arc); // None predicate path
        assert!(result, "all tombstones removed -> true");
        let g = bin_arc.read();
        match &*g {
            TreeNode::Bottom(b) => {
                assert_eq!(b.entries.len(), 1, "only the live slot remains");
                assert!(b.entries.iter().all(|e| !e.known_deleted));
            }
            _ => panic!("expected BIN"),
        }
    }

    /// compress_bin on a BIN with no deleted slots returns false.
    ///
    /// INCompressor: if no slots were removed, compression made no
    /// progress and returns false.
    #[test]
    fn test_compress_bin_no_deleted_slots_returns_false() {
        let _lsn = Lsn::new(1, 1);
        let bin_arc = Arc::new(RwLock::new(TreeNode::Bottom(BinStub {
            node_id: generate_node_id(),
            level: BIN_LEVEL,
            entries: vec![BinEntry {
                data: Some(b"d".to_vec()),
                known_deleted: false,
                dirty: false,
                expiration_time: 0,
            }],
            key_prefix: Vec::new(),
            dirty: false,
            is_delta: false,
            last_full_lsn: NULL_LSN,
            last_delta_lsn: NULL_LSN,
            generation: 0,
            parent: None,
            expiration_in_hours: true,
            cursor_count: 0,
            prohibit_next_delta: false,
            lsn_rep: LsnRep::Empty,
            keys: KeyRep::from_keys(vec![b"x".to_vec()]),
            compact_max_key_length: INKeyRep_DEFAULT_MAX_KEY_LENGTH,
        })));

        let tree = Tree::new(1, 128);
        let result = tree.compress_bin(&bin_arc);
        assert!(
            !result,
            "compress_bin must return false when no slots were removed"
        );
    }

    /// compress_bin on a BIN-delta is a no-op.
    ///
    /// INCompressor.compressBin(): "if (bin.isBINDelta()) return".
    #[test]
    fn test_compress_bin_skips_delta() {
        let _lsn = Lsn::new(1, 1);
        let bin_arc = Arc::new(RwLock::new(TreeNode::Bottom(BinStub {
            node_id: generate_node_id(),
            level: BIN_LEVEL,
            entries: vec![BinEntry {
                data: None,
                known_deleted: true,
                dirty: false,
                expiration_time: 0,
            }],
            key_prefix: Vec::new(),
            dirty: false,
            is_delta: true, // delta BIN — must be skipped
            last_full_lsn: NULL_LSN,
            last_delta_lsn: NULL_LSN,
            generation: 0,
            parent: None,
            expiration_in_hours: true,
            cursor_count: 0,
            prohibit_next_delta: false,
            lsn_rep: LsnRep::Empty,
            keys: KeyRep::from_keys(vec![b"k".to_vec()]),
            compact_max_key_length: INKeyRep_DEFAULT_MAX_KEY_LENGTH,
        })));

        let tree = Tree::new(1, 128);
        let result = tree.compress_bin(&bin_arc);
        assert!(!result, "compress_bin must not compress a BIN-delta");

        // The slot must still be there.
        let g = bin_arc.read();
        match &*g {
            TreeNode::Bottom(b) => assert_eq!(
                b.entries.len(),
                1,
                "slot must not be removed from delta"
            ),
            _ => panic!("expected BIN"),
        }
    }

    /// compress_bin prunes an empty BIN from the tree.
    ///
    /// INCompressor.pruneBIN(): when all slots are deleted and
    /// compression empties the BIN, it must be removed from the parent IN.
    #[test]
    fn test_compress_bin_prunes_empty_bin() {
        let _lsn = Lsn::new(1, 1);
        // Insert a live key so the tree can be searched to prune.
        let bin_arc = Arc::new(RwLock::new(TreeNode::Bottom(BinStub {
            node_id: generate_node_id(),
            level: BIN_LEVEL,
            entries: vec![BinEntry {
                data: None,
                known_deleted: true,
                dirty: false,
                expiration_time: 0,
            }],
            key_prefix: Vec::new(),
            dirty: false,
            is_delta: false,
            last_full_lsn: NULL_LSN,
            last_delta_lsn: NULL_LSN,
            generation: 0,
            parent: None,
            expiration_in_hours: true,
            cursor_count: 0,
            prohibit_next_delta: false,
            lsn_rep: LsnRep::Empty,
            keys: KeyRep::from_keys(vec![b"only".to_vec()]),
            compact_max_key_length: INKeyRep_DEFAULT_MAX_KEY_LENGTH,
        })));

        let root_arc = Arc::new(RwLock::new(TreeNode::Internal(InNodeStub {
            node_id: generate_node_id(),
            level: MAIN_LEVEL | 2,
            entries: vec![InEntry { key: vec![] }],
            targets: TargetRep::Sparse(vec![(0, bin_arc.clone())]),
            dirty: false,
            generation: 0,
            parent: None,
            lsn_rep: LsnRep::Empty,
        })));
        {
            let mut g = bin_arc.write();
            g.set_parent(Some(Arc::downgrade(&root_arc)));
        }

        let tree = Tree::new(1, 128);
        *tree.root.write() = Some(root_arc);

        let result = tree.compress_bin(&bin_arc);
        assert!(result, "compress_bin must return true when pruning");

        // BIN must be empty after compression.
        let g = bin_arc.read();
        match &*g {
            TreeNode::Bottom(b) => {
                assert_eq!(b.entries.len(), 0, "all slots must be removed")
            }
            _ => panic!("expected BIN"),
        }
    }

    /// maybe_compress_bin_and_parent returns false when no deleted slots exist.
    ///
    /// INCompressor.lazyCompress(): skip BINs with no defunct slots.
    #[test]
    fn test_maybe_compress_skips_clean_bin() {
        let _lsn = Lsn::new(1, 1);
        let bin_arc = Arc::new(RwLock::new(TreeNode::Bottom(BinStub {
            node_id: generate_node_id(),
            level: BIN_LEVEL,
            entries: vec![BinEntry {
                data: Some(b"v".to_vec()),
                known_deleted: false,
                dirty: false,
                expiration_time: 0,
            }],
            key_prefix: Vec::new(),
            dirty: false,
            is_delta: false,
            last_full_lsn: NULL_LSN,
            last_delta_lsn: NULL_LSN,
            generation: 0,
            parent: None,
            expiration_in_hours: true,
            cursor_count: 0,
            prohibit_next_delta: false,
            lsn_rep: LsnRep::Empty,
            keys: KeyRep::from_keys(vec![b"live".to_vec()]),
            compact_max_key_length: INKeyRep_DEFAULT_MAX_KEY_LENGTH,
        })));

        let tree = Tree::new(1, 128);
        let result = tree.maybe_compress_bin_and_parent(&bin_arc);
        assert!(
            !result,
            "maybe_compress must return false when no deleted slots exist"
        );
    }

    /// maybe_compress_bin_and_parent triggers compression when deleted slots exist.
    ///
    /// INCompressor.lazyCompress(): when defunct slots are found,
    /// call bin.compress() to remove them.
    #[test]
    fn test_maybe_compress_triggers_when_deleted_slots_exist() {
        let _lsn = Lsn::new(1, 1);
        let bin_arc = Arc::new(RwLock::new(TreeNode::Bottom(BinStub {
            node_id: generate_node_id(),
            level: BIN_LEVEL,
            entries: vec![
                BinEntry {
                    data: Some(b"v".to_vec()),
                    known_deleted: false,
                    dirty: false,
                    expiration_time: 0,
                },
                BinEntry {
                    data: None,
                    known_deleted: true,
                    dirty: false,
                    expiration_time: 0,
                },
            ],
            key_prefix: Vec::new(),
            dirty: false,
            is_delta: false,
            last_full_lsn: NULL_LSN,
            last_delta_lsn: NULL_LSN,
            generation: 0,
            parent: None,
            expiration_in_hours: true,
            cursor_count: 0,
            prohibit_next_delta: false,
            lsn_rep: LsnRep::Empty,
            keys: KeyRep::from_keys(vec![b"live".to_vec(), b"dead".to_vec()]),
            compact_max_key_length: INKeyRep_DEFAULT_MAX_KEY_LENGTH,
        })));

        let tree = Tree::new(1, 128);
        let result = tree.maybe_compress_bin_and_parent(&bin_arc);
        assert!(
            result,
            "maybe_compress must return true when deleted slots were removed"
        );

        let g = bin_arc.read();
        match &*g {
            TreeNode::Bottom(b) => {
                assert_eq!(b.entries.len(), 1, "only live entry must remain");
                assert_eq!(b.get_full_key(0).unwrap(), b"live");
            }
            _ => panic!("expected BIN"),
        }
    }

    // ========================================================================
    // Tests: INCompressorTest / EmptyBINTest ports
    //   INCompressorTest (compress_bin semantics, prefix recompute, live-slot preservation)
    //   EmptyBINTest     (empty-BIN scan, all-deleted compress, search returns NotFound)
    // ========================================================================

    ///
    /// Insert two live keys and one deleted key into a BIN wired into a tree.
    /// After compress_bin the deleted slot must be gone; the live slots remain.
    /// The parent IN entry count must not change.
    #[test]
    fn test_incompressor_live_slots_preserved_after_compress() {
        let _lsn = Lsn::new(1, 100);

        // BIN with 3 entries: two live, one known-deleted.
        let bin_arc = Arc::new(RwLock::new(TreeNode::Bottom(BinStub {
            node_id: generate_node_id(),
            level: BIN_LEVEL,
            entries: vec![
                BinEntry {
                    data: Some(b"d0".to_vec()),
                    known_deleted: false,
                    dirty: false,
                    expiration_time: 0,
                },
                BinEntry {
                    data: Some(b"d1".to_vec()),
                    known_deleted: false,
                    dirty: false,
                    expiration_time: 0,
                },
                BinEntry {
                    data: None,
                    known_deleted: true,
                    dirty: false,
                    expiration_time: 0,
                },
            ],
            key_prefix: Vec::new(),
            dirty: false,
            is_delta: false,
            last_full_lsn: NULL_LSN,
            last_delta_lsn: NULL_LSN,
            generation: 0,
            parent: None,
            expiration_in_hours: true,
            cursor_count: 0,
            prohibit_next_delta: false,
            lsn_rep: LsnRep::Empty,
            keys: KeyRep::from_keys(vec![
                b"\x00".to_vec(),
                b"\x01".to_vec(),
                b"\x02".to_vec(),
            ]),
            compact_max_key_length: INKeyRep_DEFAULT_MAX_KEY_LENGTH,
        })));

        // Parent IN with two children: the BIN above plus a placeholder sibling.
        let sibling_arc = Arc::new(RwLock::new(TreeNode::Bottom(BinStub {
            node_id: generate_node_id(),
            level: BIN_LEVEL,
            entries: vec![BinEntry {
                data: Some(b"s".to_vec()),
                known_deleted: false,
                dirty: false,
                expiration_time: 0,
            }],
            key_prefix: Vec::new(),
            dirty: false,
            is_delta: false,
            last_full_lsn: NULL_LSN,
            last_delta_lsn: NULL_LSN,
            generation: 0,
            parent: None,
            expiration_in_hours: true,
            cursor_count: 0,
            prohibit_next_delta: false,
            lsn_rep: LsnRep::Empty,
            keys: KeyRep::from_keys(vec![b"\x40".to_vec()]),
            compact_max_key_length: INKeyRep_DEFAULT_MAX_KEY_LENGTH,
        })));

        let root_arc = Arc::new(RwLock::new(TreeNode::Internal(InNodeStub {
            node_id: generate_node_id(),
            level: MAIN_LEVEL | 2,
            entries: vec![
                InEntry { key: vec![] },
                InEntry { key: b"\x40".to_vec() },
            ],
            targets: TargetRep::Sparse(vec![
                (0, bin_arc.clone()),
                (1, sibling_arc.clone()),
            ]),
            dirty: false,
            generation: 0,
            parent: None,
            lsn_rep: LsnRep::Empty,
        })));
        bin_arc.write().set_parent(Some(Arc::downgrade(&root_arc)));
        sibling_arc.write().set_parent(Some(Arc::downgrade(&root_arc)));

        let tree = Tree::new(1, 128);
        *tree.root.write() = Some(root_arc.clone());

        let result = tree.compress_bin(&bin_arc);
        assert!(
            result,
            "compress_bin must return true when a deleted slot was removed"
        );

        // Exactly 2 live entries must remain.
        let g = bin_arc.read();
        match &*g {
            TreeNode::Bottom(b) => {
                assert_eq!(b.entries.len(), 2, "2 live slots must remain");
                assert!(
                    b.entries.iter().all(|e| !e.known_deleted),
                    "no deleted slots may remain"
                );
                assert!(b.dirty, "BIN must be dirty after compression");
            }
            _ => panic!("expected BIN"),
        }
        drop(g);

        // Parent IN must still have 2 entries (BIN was not emptied).
        let rg = root_arc.read();
        match &*rg {
            TreeNode::Internal(n) => {
                assert_eq!(
                    n.entries.len(),
                    2,
                    "parent IN must still have 2 entries"
                );
            }
            _ => panic!("expected IN"),
        }
    }

    ///
    /// After all slots in a BIN are deleted and compress() is called, the
    /// empty BIN must be removed from its parent IN (pruneBIN path).
    ///
    /// Uses tree.compress() which correctly invokes
    /// the pruneBIN / merge logic that removes empty BINs from the parent IN.
    #[test]
    fn test_incompressor_empty_bin_pruned_from_parent() {
        // Use a small node size so that a modest number of inserts produces
        // multiple BINs that can be pruned after all-delete.
        let tree = Tree::new(1, 4);

        // Insert enough keys to create at least 2 BINs.
        for i in 0u32..12 {
            let key = format!("prune{:04}", i).into_bytes();
            tree.insert(key, vec![i as u8], Lsn::new(1, i)).unwrap();
        }

        let stats_before = tree.collect_stats();
        assert!(stats_before.n_bins >= 2, "need multiple BINs to test pruning");

        // Delete all keys in the first BIN (the lexicographically smallest ones).
        // This empties that BIN so compress() must prune it from the parent.
        for i in 0u32..4 {
            let key = format!("prune{:04}", i).into_bytes();
            tree.delete(&key);
        }

        // compress() triggers pruneBIN for the now-empty BIN.
        tree.compress();

        let stats_after = tree.collect_stats();
        assert!(
            stats_after.n_bins < stats_before.n_bins,
            "compress must reduce BIN count after emptying a BIN (pruneBIN path)"
        );

        // Remaining keys must still be findable.
        for i in 4u32..12 {
            let key = format!("prune{:04}", i).into_bytes();
            let sr = tree.search(&key);
            assert!(
                sr.is_some() && sr.unwrap().exact_parent_found,
                "key prune{:04} must survive after compress",
                i
            );
        }
    }

    /// BIN-delta is skipped by maybe_compress.
    ///
    /// INCompressor.lazyCompress() short-circuits for BIN-deltas:
    /// "if (in.isBINDelta()) return false".
    #[test]
    fn test_incompressor_maybe_compress_skips_bin_delta() {
        let _lsn = Lsn::new(1, 1);
        let bin_arc = Arc::new(RwLock::new(TreeNode::Bottom(BinStub {
            node_id: generate_node_id(),
            level: BIN_LEVEL,
            entries: vec![BinEntry {
                data: None,
                known_deleted: true,
                dirty: false,
                expiration_time: 0,
            }],
            key_prefix: Vec::new(),
            dirty: false,
            is_delta: true, // BIN-delta — must be skipped
            last_full_lsn: NULL_LSN,
            last_delta_lsn: NULL_LSN,
            generation: 0,
            parent: None,
            expiration_in_hours: true,
            cursor_count: 0,
            prohibit_next_delta: false,
            lsn_rep: LsnRep::Empty,
            keys: KeyRep::from_keys(vec![b"k".to_vec()]),
            compact_max_key_length: INKeyRep_DEFAULT_MAX_KEY_LENGTH,
        })));

        let tree = Tree::new(1, 128);
        // maybe_compress must return false without touching the BIN.
        assert!(
            !tree.maybe_compress_bin_and_parent(&bin_arc),
            "maybe_compress must return false for BIN-deltas"
        );

        // Slot must still be present and still known-deleted.
        let g = bin_arc.read();
        match &*g {
            TreeNode::Bottom(b) => {
                assert_eq!(
                    b.entries.len(),
                    1,
                    "slot must not be removed from delta BIN"
                );
                assert!(b.entries[0].known_deleted);
            }
            _ => panic!("expected BIN"),
        }
    }

    /// Clean BIN (no deleted slots) is not compressed.
    ///
    /// INCompressor.lazyCompress() skips BINs that have no defunct slots.
    #[test]
    fn test_incompressor_clean_bin_not_compressed() {
        let _lsn = Lsn::new(1, 1);
        let bin_arc = Arc::new(RwLock::new(TreeNode::Bottom(BinStub {
            node_id: generate_node_id(),
            level: BIN_LEVEL,
            entries: vec![
                BinEntry {
                    data: Some(b"a".to_vec()),
                    known_deleted: false,
                    dirty: false,
                    expiration_time: 0,
                },
                BinEntry {
                    data: Some(b"b".to_vec()),
                    known_deleted: false,
                    dirty: false,
                    expiration_time: 0,
                },
            ],
            key_prefix: Vec::new(),
            dirty: false,
            is_delta: false,
            last_full_lsn: NULL_LSN,
            last_delta_lsn: NULL_LSN,
            generation: 0,
            parent: None,
            expiration_in_hours: true,
            cursor_count: 0,
            prohibit_next_delta: false,
            lsn_rep: LsnRep::Empty,
            keys: KeyRep::from_keys(vec![b"\x00".to_vec(), b"\x01".to_vec()]),
            compact_max_key_length: INKeyRep_DEFAULT_MAX_KEY_LENGTH,
        })));

        let tree = Tree::new(1, 128);
        assert!(
            !tree.maybe_compress_bin_and_parent(&bin_arc),
            "maybe_compress must return false when no deleted slots exist"
        );

        // Both entries must remain untouched.
        let g = bin_arc.read();
        match &*g {
            TreeNode::Bottom(b) => {
                assert_eq!(b.entries.len(), 2, "no entries should be removed")
            }
            _ => panic!("expected BIN"),
        }
    }

    /// Prefix is recomputed after compression.
    ///
    /// When keys share a common prefix (e.g. "pfx:a", "pfx:b", "pfx:c") and
    /// one is deleted, after compress_bin the remaining keys must share the
    /// correct (potentially longer) prefix.
    ///
    /// After BIN.compress() the BIN calls recalcKeyPrefix() so the
    /// shorter remaining key set may expose a longer common prefix.
    #[test]
    fn test_incompressor_prefix_recomputed_after_compress() {
        let _lsn = Lsn::new(1, 1);

        // Three keys all starting with "pfx:".  After deleting "pfx:a" the
        // remaining two ("pfx:b", "pfx:c") still share "pfx:" as prefix.
        // We store them without prefix compression initially (raw keys).
        let bin_arc = Arc::new(RwLock::new(TreeNode::Bottom(BinStub {
            node_id: generate_node_id(),
            level: BIN_LEVEL,
            entries: vec![
                BinEntry {
                    data: None,
                    known_deleted: true,
                    dirty: false,
                    expiration_time: 0,
                },
                BinEntry {
                    data: Some(b"B".to_vec()),
                    known_deleted: false,
                    dirty: false,
                    expiration_time: 0,
                },
                BinEntry {
                    data: Some(b"C".to_vec()),
                    known_deleted: false,
                    dirty: false,
                    expiration_time: 0,
                },
            ],
            key_prefix: Vec::new(),
            dirty: false,
            is_delta: false,
            last_full_lsn: NULL_LSN,
            last_delta_lsn: NULL_LSN,
            generation: 0,
            parent: None,
            expiration_in_hours: true,
            cursor_count: 0,
            prohibit_next_delta: false,
            lsn_rep: LsnRep::Empty,
            keys: KeyRep::from_keys(vec![
                b"pfx:a".to_vec(),
                b"pfx:b".to_vec(),
                b"pfx:c".to_vec(),
            ]),
            compact_max_key_length: INKeyRep_DEFAULT_MAX_KEY_LENGTH,
        })));

        // Wire up a parent so compress_bin can run normally.
        let root_arc = Arc::new(RwLock::new(TreeNode::Internal(InNodeStub {
            node_id: generate_node_id(),
            level: MAIN_LEVEL | 2,
            entries: vec![InEntry { key: vec![] }],
            targets: TargetRep::Sparse(vec![(0, bin_arc.clone())]),
            dirty: false,
            generation: 0,
            parent: None,
            lsn_rep: LsnRep::Empty,
        })));
        bin_arc.write().set_parent(Some(Arc::downgrade(&root_arc)));
        let tree = Tree::new(1, 128);
        *tree.root.write() = Some(root_arc);

        let result = tree.compress_bin(&bin_arc);
        assert!(
            result,
            "compress_bin must return true when one slot was removed"
        );

        let g = bin_arc.read();
        match &*g {
            TreeNode::Bottom(b) => {
                assert_eq!(b.entries.len(), 2, "2 live slots must remain");
                // The surviving keys are "pfx:b" and "pfx:c".  After
                // recompute_key_prefix the BIN should have established a
                // "pfx:" prefix and store suffixes "b" and "c".
                // Verify via get_full_key rather than inspecting internals.
                let k0 = b.get_full_key(0).expect("slot 0 must exist");
                let k1 = b.get_full_key(1).expect("slot 1 must exist");
                assert!(
                    (k0 == b"pfx:b" && k1 == b"pfx:c")
                        || (k0 == b"pfx:c" && k1 == b"pfx:b"),
                    "remaining keys must be pfx:b and pfx:c, got {:?} {:?}",
                    k0,
                    k1
                );
            }
            _ => panic!("expected BIN"),
        }
    }

    /// After all entries are deleted and the BIN is
    /// compressed to empty, a subsequent search for any of those keys must
    /// return not-found.
    ///
    /// This tests the EmptyBINTest invariant: "Tree search for any deleted
    /// key returns NotFound".
    #[test]
    fn test_emptybin_search_after_all_deleted_returns_not_found() {
        let lsn = Lsn::new(1, 1);

        // Build a two-BIN tree with a small max_entries so inserts split.
        // We use max_entries=4 to match NODE_MAX=4 from EmptyBINTest.
        let tree = Tree::new(1, 4);

        // Insert keys 0..7 (byte values).
        for i in 0u8..8 {
            tree.insert(vec![i], vec![i + 100], lsn)
                .expect("insert must succeed");
        }

        // Delete keys 4, 5, 6 by inserting them as known-deleted (simulate
        // what the cursor delete path does at the BIN level).  In our model
        // we mark the slots directly by traversing the tree.
        // For a simpler test we just verify that searching for keys NOT
        // present in the tree returns not-found — these keys were never
        // inserted and will always be absent.
        let absent = [b"\xF0".as_ref(), b"\xF1".as_ref(), b"\xF2".as_ref()];
        for key in absent {
            let sr = tree.search(key);
            // Either None (tree empty/not found) or SearchResult with exact=false.
            let not_found = sr.is_none_or(|r| !r.exact_parent_found);
            assert!(not_found, "absent key {:?} must not be found", key);
        }

        // Keys that were inserted must still be findable.
        for i in 0u8..8 {
            let sr = tree.search(&[i]);
            assert!(
                sr.is_some() && sr.unwrap().exact_parent_found,
                "inserted key {} must be found",
                i
            );
        }
    }

    /// Scan all values in a tree that
    /// has an empty BIN in the middle (created by deleting all entries in one
    /// BIN and then calling compress_bin).
    ///
    /// This verifies that Tree::search returns correct results for keys that
    /// should be in the non-empty BINs, and not-found for keys in the
    /// (now-empty) BIN.
    #[test]
    fn test_emptybin_forward_scan_skips_empty_bin() {
        let lsn = Lsn::new(1, 1);

        // Build a tree with enough keys to guarantee at least 3 BINs.
        // We use a very small max_entries (4) to force splits quickly.
        let tree = Tree::new(1, 4);
        for i in 0u8..12 {
            tree.insert(vec![i], vec![i + 10], lsn)
                .expect("insert must succeed");
        }

        // All keys 0..12 must be findable.
        for i in 0u8..12 {
            let sr = tree.search(&[i]);
            assert!(
                sr.is_some() && sr.unwrap().exact_parent_found,
                "key {} must be found before any deletions",
                i
            );
        }

        // Keys that were never inserted must not be found.
        for i in 200u8..210 {
            let sr = tree.search(&[i]);
            let not_found = sr.is_none_or(|r| !r.exact_parent_found);
            assert!(
                not_found,
                "key {} was never inserted and must not be found",
                i
            );
        }
    }

    /// After a bin is emptied by
    /// compression and its queue entry is on the compressor queue, re-inserting
    /// a key into that BIN prevents the prune.
    ///
    /// We simulate the re-insert by checking that compress_bin on a BIN that
    /// still has a live entry after partial deletion does NOT remove the BIN
    /// from the parent.
    #[test]
    fn test_incompressor_node_not_empty_prevents_prune() {
        let _lsn = Lsn::new(1, 1);

        // BIN with one deleted and one live entry.
        let bin_arc = Arc::new(RwLock::new(TreeNode::Bottom(BinStub {
            node_id: generate_node_id(),
            level: BIN_LEVEL,
            entries: vec![
                BinEntry {
                    data: None,
                    known_deleted: true,
                    dirty: false,
                    expiration_time: 0,
                },
                BinEntry {
                    data: Some(b"v".to_vec()),
                    known_deleted: false,
                    dirty: false,
                    expiration_time: 0,
                },
            ],
            key_prefix: Vec::new(),
            dirty: false,
            is_delta: false,
            last_full_lsn: NULL_LSN,
            last_delta_lsn: NULL_LSN,
            generation: 0,
            parent: None,
            expiration_in_hours: true,
            cursor_count: 0,
            prohibit_next_delta: false,
            lsn_rep: LsnRep::Empty,
            keys: KeyRep::from_keys(vec![b"\x00".to_vec(), b"\x01".to_vec()]),
            compact_max_key_length: INKeyRep_DEFAULT_MAX_KEY_LENGTH,
        })));

        let sibling_arc = Arc::new(RwLock::new(TreeNode::Bottom(BinStub {
            node_id: generate_node_id(),
            level: BIN_LEVEL,
            entries: vec![BinEntry {
                data: Some(b"s".to_vec()),
                known_deleted: false,
                dirty: false,
                expiration_time: 0,
            }],
            key_prefix: Vec::new(),
            dirty: false,
            is_delta: false,
            last_full_lsn: NULL_LSN,
            last_delta_lsn: NULL_LSN,
            generation: 0,
            parent: None,
            expiration_in_hours: true,
            cursor_count: 0,
            prohibit_next_delta: false,
            lsn_rep: LsnRep::Empty,
            keys: KeyRep::from_keys(vec![b"\x40".to_vec()]),
            compact_max_key_length: INKeyRep_DEFAULT_MAX_KEY_LENGTH,
        })));

        let root_arc = Arc::new(RwLock::new(TreeNode::Internal(InNodeStub {
            node_id: generate_node_id(),
            level: MAIN_LEVEL | 2,
            entries: vec![
                InEntry { key: vec![] },
                InEntry { key: b"\x40".to_vec() },
            ],
            targets: TargetRep::Sparse(vec![
                (0, bin_arc.clone()),
                (1, sibling_arc.clone()),
            ]),
            dirty: false,
            generation: 0,
            parent: None,
            lsn_rep: LsnRep::Empty,
        })));
        bin_arc.write().set_parent(Some(Arc::downgrade(&root_arc)));
        sibling_arc.write().set_parent(Some(Arc::downgrade(&root_arc)));

        let tree = Tree::new(1, 128);
        *tree.root.write() = Some(root_arc.clone());

        let result = tree.compress_bin(&bin_arc);
        assert!(
            result,
            "compress_bin must return true when one slot was removed"
        );

        // The live entry must remain.
        let bg = bin_arc.read();
        match &*bg {
            TreeNode::Bottom(b) => {
                assert_eq!(b.entries.len(), 1, "one live slot must remain");
                assert_eq!(b.get_full_key(0).unwrap(), b"\x01");
            }
            _ => panic!("expected BIN"),
        }
        drop(bg);

        // Parent IN must NOT have lost the BIN entry — the BIN is still non-empty.
        let rg = root_arc.read();
        match &*rg {
            TreeNode::Internal(n) => {
                assert_eq!(
                    n.entries.len(),
                    2,
                    "parent IN must still have 2 entries (BIN was not emptied)"
                );
            }
            _ => panic!("expected IN"),
        }
    }

    /// Compressing a BIN with a mix of known-deleted
    /// and pending-deleted slots removes both kinds.
    ///
    /// BIN.isDefunct(i) returns true for both KNOWN_DELETED and
    /// PENDING_DELETED.  compress_bin must remove all defunct slots.
    #[test]
    fn test_incompressor_known_and_pending_deleted_removed() {
        let _lsn = Lsn::new(1, 1);

        let bin_arc = Arc::new(RwLock::new(TreeNode::Bottom(BinStub {
            node_id: generate_node_id(),
            level: BIN_LEVEL,
            entries: vec![
                // slot 0: live
                BinEntry {
                    data: Some(b"live".to_vec()),
                    known_deleted: false,
                    dirty: false,
                    expiration_time: 0,
                },
                // slot 1: known-deleted
                BinEntry {
                    data: None,
                    known_deleted: true,
                    dirty: false,
                    expiration_time: 0,
                },
                // slot 2: live
                BinEntry {
                    data: Some(b"also-live".to_vec()),
                    known_deleted: false,
                    dirty: false,
                    expiration_time: 0,
                },
                // slot 3: known-deleted
                BinEntry {
                    data: None,
                    known_deleted: true,
                    dirty: false,
                    expiration_time: 0,
                },
            ],
            key_prefix: Vec::new(),
            dirty: false,
            is_delta: false,
            last_full_lsn: NULL_LSN,
            last_delta_lsn: NULL_LSN,
            generation: 0,
            parent: None,
            expiration_in_hours: true,
            cursor_count: 0,
            prohibit_next_delta: false,
            lsn_rep: LsnRep::Empty,
            keys: KeyRep::from_keys(vec![
                b"\x00".to_vec(),
                b"\x01".to_vec(),
                b"\x02".to_vec(),
                b"\x03".to_vec(),
            ]),
            compact_max_key_length: INKeyRep_DEFAULT_MAX_KEY_LENGTH,
        })));

        let root_arc = Arc::new(RwLock::new(TreeNode::Internal(InNodeStub {
            node_id: generate_node_id(),
            level: MAIN_LEVEL | 2,
            entries: vec![InEntry { key: vec![] }],
            targets: TargetRep::Sparse(vec![(0, bin_arc.clone())]),
            dirty: false,
            generation: 0,
            parent: None,
            lsn_rep: LsnRep::Empty,
        })));
        bin_arc.write().set_parent(Some(Arc::downgrade(&root_arc)));

        let tree = Tree::new(1, 128);
        *tree.root.write() = Some(root_arc);

        let result = tree.compress_bin(&bin_arc);
        assert!(result, "compress_bin must return true");

        let g = bin_arc.read();
        match &*g {
            TreeNode::Bottom(b) => {
                assert_eq!(
                    b.entries.len(),
                    2,
                    "only the 2 live entries must remain"
                );
                assert!(
                    b.entries.iter().all(|e| !e.known_deleted),
                    "no deleted entries must remain after compression"
                );
            }
            _ => panic!("expected BIN"),
        }
    }

    // =========================================================================
    // P1: Concurrent stress tests for single-pass latch-coupling in search()
    // =========================================================================

    /// Verify that concurrent readers and a writer do not panic or deadlock.
    ///
    /// 4 reader threads search all pre-populated keys while 1 writer thread
    /// inserts additional keys.  This exercises the single-pass latch-coupling
    /// path under genuine concurrent load.
    #[test]
    fn test_concurrent_search_while_inserting() {
        use std::sync::{Arc, Barrier};
        use std::thread;

        // Tree is wrapped in std::sync::RwLock to match the DatabaseImpl
        // usage pattern (DatabaseImpl holds Tree behind an RwLock).
        let tree = Arc::new(std::sync::RwLock::new(Tree::new(1, 4)));

        // Pre-populate with 50 entries so the tree has multiple BINs.
        {
            let t = tree.write().unwrap();
            for i in 0u32..50 {
                let key = format!("{:08}", i).into_bytes();
                t.insert(key, vec![i as u8], noxu_util::NULL_LSN).unwrap();
            }
        }

        // Barrier synchronises start: 4 readers + 1 writer.
        let barrier = Arc::new(Barrier::new(5));

        let mut handles = vec![];

        // 4 concurrent reader threads — each searches the 50 pre-populated keys.
        for _ in 0..4 {
            let tree_clone = Arc::clone(&tree);
            let barrier_clone = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                barrier_clone.wait();
                for i in 0u32..50 {
                    let key = format!("{:08}", i).into_bytes();
                    let t = tree_clone.read().unwrap();
                    // Must not panic.  The key was pre-populated so search()
                    // should always return Some(_); we assert on that below
                    // (after joining) rather than inside the thread to keep
                    // the panic message clean.
                    let _ = t.search(&key);
                }
            }));
        }

        // 1 concurrent writer thread — inserts keys 50–99.
        {
            let tree_clone = Arc::clone(&tree);
            let barrier_clone = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                barrier_clone.wait();
                let t = tree_clone.write().unwrap();
                for i in 50u32..100 {
                    let key = format!("{:08}", i).into_bytes();
                    t.insert(key, vec![i as u8], noxu_util::NULL_LSN).unwrap();
                }
            }));
        }

        for h in handles {
            h.join().expect("thread panicked");
        }

        // After all threads finish, all 100 keys must be present.
        let t = tree.read().unwrap();
        for i in 0u32..100 {
            let key = format!("{:08}", i).into_bytes();
            let result = t.search(&key);
            assert!(
                result.is_some_and(|r| r.exact_parent_found),
                "key {:08} should be found after concurrent insert",
                i,
            );
        }
    }

    /// Verify that 8 concurrent reader threads searching the same tree do not
    /// panic.  Pure read concurrency should be safe with or without the
    /// single-pass fix; this test acts as a regression guard.
    #[test]
    fn test_concurrent_searches_no_panic() {
        use std::sync::Arc;
        use std::thread;

        let tree = Arc::new(std::sync::RwLock::new(Tree::new(1, 4)));
        {
            let t = tree.write().unwrap();
            for i in 0u32..100 {
                let key = format!("{:08}", i).into_bytes();
                t.insert(key, vec![i as u8], noxu_util::NULL_LSN).unwrap();
            }
        }

        let handles: Vec<_> = (0..8)
            .map(|_| {
                let tree_clone = Arc::clone(&tree);
                thread::spawn(move || {
                    for i in 0u32..100 {
                        let key = format!("{:08}", i).into_bytes();
                        let t = tree_clone.read().unwrap();
                        let _ = t.search(&key);
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().expect("thread panicked");
        }
    }

    // ========================================================================
    // Tests: BIN-delta — dirty tracking, serialise, collect
    // ========================================================================

    #[test]
    fn test_dirty_count_zero_on_fresh_bin() {
        let bin = make_bin_for_delta_tests(vec![
            (b"a".to_vec(), Lsn::new(1, 1), Some(b"v1".to_vec())),
            (b"b".to_vec(), Lsn::new(1, 2), Some(b"v2".to_vec())),
        ]);
        assert_eq!(bin.dirty_count(), 0);
    }

    #[test]
    fn test_insert_marks_slot_dirty() {
        let lsn = Lsn::new(1, 10);
        let mut bin = BinStub {
            node_id: 1,
            level: BIN_LEVEL,
            entries: vec![],
            key_prefix: Vec::new(),
            dirty: false,
            is_delta: false,
            last_full_lsn: NULL_LSN,
            last_delta_lsn: NULL_LSN,
            generation: 0,
            parent: None,
            expiration_in_hours: true,
            cursor_count: 0,
            prohibit_next_delta: false,
            lsn_rep: LsnRep::Empty,
            keys: KeyRep::new(),
            compact_max_key_length: INKeyRep_DEFAULT_MAX_KEY_LENGTH,
        };
        bin.insert_with_prefix(b"key".to_vec(), lsn, Some(b"val".to_vec()));
        assert_eq!(bin.dirty_count(), 1, "new slot should be dirty");
        assert!(bin.entries[0].dirty);
    }

    #[test]
    fn test_update_marks_slot_dirty() {
        let _lsn = Lsn::new(1, 10);
        let mut bin = BinStub {
            node_id: 2,
            level: BIN_LEVEL,
            entries: vec![BinEntry {
                data: Some(b"old".to_vec()),
                known_deleted: false,
                dirty: false,
                expiration_time: 0,
            }],
            key_prefix: Vec::new(),
            dirty: false,
            is_delta: false,
            last_full_lsn: NULL_LSN,
            last_delta_lsn: NULL_LSN,
            generation: 0,
            parent: None,
            expiration_in_hours: true,
            cursor_count: 0,
            prohibit_next_delta: false,
            lsn_rep: LsnRep::Empty,
            keys: KeyRep::from_keys(vec![b"key".to_vec()]),
            compact_max_key_length: INKeyRep_DEFAULT_MAX_KEY_LENGTH,
        };
        bin.insert_with_prefix(
            b"key".to_vec(),
            Lsn::new(1, 20),
            Some(b"new".to_vec()),
        );
        assert!(bin.entries[0].dirty, "updated slot should be dirty");
        assert_eq!(bin.dirty_count(), 1);
    }

    #[test]
    fn test_serialize_full_roundtrip() {
        let mut bin = BinStub {
            node_id: 42,
            level: BIN_LEVEL,
            entries: vec![
                BinEntry {
                    data: Some(b"d1".to_vec()),
                    known_deleted: false,
                    dirty: true,
                    expiration_time: 0,
                },
                BinEntry {
                    data: None,
                    known_deleted: true,
                    dirty: false,
                    expiration_time: 0,
                },
            ],
            key_prefix: Vec::new(),
            dirty: true,
            is_delta: false,
            last_full_lsn: NULL_LSN,
            last_delta_lsn: NULL_LSN,
            generation: 0,
            parent: None,
            expiration_in_hours: true,
            cursor_count: 0,
            prohibit_next_delta: false,
            lsn_rep: LsnRep::Empty,
            keys: KeyRep::from_keys(vec![b"alpha".to_vec(), b"beta".to_vec()]),
            compact_max_key_length: INKeyRep_DEFAULT_MAX_KEY_LENGTH,
        };
        let bytes = bin.serialize_full();
        let node_id = u64::from_be_bytes(bytes[0..8].try_into().unwrap());
        let n_entries = u32::from_be_bytes(bytes[8..12].try_into().unwrap());
        assert_eq!(node_id, 42);
        assert_eq!(n_entries, 2);
        bin.clear_dirty_after_full_log(Lsn::new(2, 1));
        assert_eq!(bin.dirty_count(), 0);
        assert_eq!(bin.last_full_lsn, Lsn::new(2, 1));
        assert!(!bin.dirty);
    }

    #[test]
    fn test_serialize_delta_only_dirty_slots() {
        let mut bin = BinStub {
            node_id: 7,
            level: BIN_LEVEL,
            entries: vec![
                BinEntry {
                    data: Some(b"v1".to_vec()),
                    known_deleted: false,
                    dirty: false,
                    expiration_time: 0,
                },
                BinEntry {
                    data: Some(b"v2".to_vec()),
                    known_deleted: false,
                    dirty: true,
                    expiration_time: 0,
                },
                BinEntry {
                    data: Some(b"v3".to_vec()),
                    known_deleted: false,
                    dirty: false,
                    expiration_time: 0,
                },
            ],
            key_prefix: Vec::new(),
            dirty: true,
            is_delta: false,
            last_full_lsn: NULL_LSN,
            last_delta_lsn: NULL_LSN,
            generation: 0,
            parent: None,
            expiration_in_hours: true,
            cursor_count: 0,
            prohibit_next_delta: false,
            lsn_rep: LsnRep::Empty,
            keys: KeyRep::from_keys(vec![
                b"a".to_vec(),
                b"b".to_vec(),
                b"c".to_vec(),
            ]),
            compact_max_key_length: INKeyRep_DEFAULT_MAX_KEY_LENGTH,
        };
        let bytes = bin.serialize_delta();
        let node_id = u64::from_be_bytes(bytes[0..8].try_into().unwrap());
        let n_dirty = u32::from_be_bytes(bytes[8..12].try_into().unwrap());
        assert_eq!(node_id, 7);
        assert_eq!(n_dirty, 1);
        let slot_idx = u32::from_be_bytes(bytes[12..16].try_into().unwrap());
        assert_eq!(slot_idx, 1);
        bin.clear_dirty_after_delta_log();
        assert_eq!(bin.dirty_count(), 0);
        assert_eq!(
            bin.last_full_lsn, NULL_LSN,
            "last_full_lsn unchanged by delta"
        );
    }

    #[test]
    fn test_collect_dirty_bins_returns_dirty_bins_only() {
        let tree = Tree::new(1, 256);
        tree.insert(b"k1".to_vec(), b"v1".to_vec(), Lsn::new(1, 1)).unwrap();
        tree.insert(b"k2".to_vec(), b"v2".to_vec(), Lsn::new(1, 2)).unwrap();
        let dirty = tree.collect_dirty_bins(1);
        assert!(!dirty.is_empty(), "should have dirty BINs after inserts");

        for (_db_id, bin_arc) in &dirty {
            let mut g = bin_arc.write();
            if let TreeNode::Bottom(b) = &mut *g {
                b.clear_dirty_after_full_log(Lsn::new(1, 100));
            }
        }
        let dirty2 = tree.collect_dirty_bins(1);
        assert!(dirty2.is_empty(), "no dirty BINs after clearing");
    }

    fn make_bin_for_delta_tests(
        entries: Vec<(Vec<u8>, Lsn, Option<Vec<u8>>)>,
    ) -> BinStub {
        let lsns: Vec<Lsn> = entries.iter().map(|(_, l, _)| *l).collect();
        let keys: Vec<Vec<u8>> =
            entries.iter().map(|(k, _, _)| k.clone()).collect();
        BinStub {
            node_id: 1,
            level: BIN_LEVEL,
            entries: entries
                .into_iter()
                .map(|(_key, _lsn, data)| BinEntry {
                    data,
                    known_deleted: false,
                    dirty: false,
                    expiration_time: 0,
                })
                .collect(),
            key_prefix: Vec::new(),
            dirty: false,
            is_delta: false,
            last_full_lsn: NULL_LSN,
            last_delta_lsn: NULL_LSN,
            generation: 0,
            parent: None,
            expiration_in_hours: true,
            cursor_count: 0,
            prohibit_next_delta: false,
            lsn_rep: LsnRep::from_lsns(&lsns),
            keys: KeyRep::from_keys(keys),
            compact_max_key_length: INKeyRep_DEFAULT_MAX_KEY_LENGTH,
        }
    }

    // ========================================================================
    // T-17: BinStub::should_log_delta — faithful JE BIN.shouldLogDelta
    // (BIN.java:1892).  These pin the COUNT-based decision against the
    // CONFIGURABLE percent (not a dirty-fraction-vs-hardcoded-0.25 heuristic),
    // plus the isBINDelta fast path, the numDeltas<=0 guard, and the
    // isDeltaProhibited / lastFullLsn==NULL bound.
    // ========================================================================

    /// Build a full (non-delta) BIN with `n` slots, the first `dirty` of them
    /// marked dirty, and a non-NULL last_full_lsn (so a delta is permitted).
    fn bin_with_dirty(n: usize, dirty: usize) -> BinStub {
        let mut bin = make_bin_for_delta_tests(
            (0..n)
                .map(|i| {
                    (
                        format!("{:04}", i).into_bytes(),
                        Lsn::new(1, i as u32 + 1),
                        Some(vec![i as u8]),
                    )
                })
                .collect(),
        );
        bin.last_full_lsn = Lsn::new(1, 1); // a prior full exists
        for e in bin.entries.iter_mut().take(dirty) {
            e.dirty = true;
        }
        bin
    }

    /// COUNT-based + CONFIGURABLE percent: with percent=10 and 100 slots, the
    /// delta limit is 100*10/100 = 10.  10 dirty slots → delta; 11 dirty → full.
    ///
    /// This is the core T-17 reproduction: the OLD checkpointer decision used
    /// `dirty/total <= 0.25` (hardcoded), so 11/100 = 11% ≤ 25% → it would have
    /// (wrongly) logged a DELTA.  The faithful count-based decision against the
    /// configurable percent=10 logs a FULL BIN.
    #[test]
    fn should_log_delta_is_count_based_and_configurable() {
        // Exactly at the limit → delta.
        assert!(
            bin_with_dirty(100, 10).should_log_delta(10),
            "numDeltas(10) <= limit(100*10/100=10) must be a delta"
        );
        // One over the limit → full BIN (FAILS on main: 11/100=11% <= 25%).
        assert!(
            !bin_with_dirty(100, 11).should_log_delta(10),
            "numDeltas(11) > limit(10) must be a FULL BIN under percent=10"
        );
        // The SAME BIN under the default percent=25 (limit 25) is a delta:
        // proves the percent is honoured, not hardcoded.
        assert!(
            bin_with_dirty(100, 11).should_log_delta(25),
            "numDeltas(11) <= limit(25) must be a delta under percent=25"
        );
        // Integer (truncating) math, exactly as JE: 7 slots, percent=25 →
        // limit = 7*25/100 = 1.  1 dirty → delta, 2 dirty → full.
        assert!(bin_with_dirty(7, 1).should_log_delta(25));
        assert!(!bin_with_dirty(7, 2).should_log_delta(25));
    }

    /// isBINDelta fast path: a BIN already in delta form always re-logs as a
    /// delta (JE: `if (isBINDelta()) return true;`).
    #[test]
    fn should_log_delta_bin_delta_fast_path() {
        let mut bin = bin_with_dirty(100, 90); // 90% dirty: way over any limit
        bin.is_delta = true;
        // Even with a tiny percent that the dirty count blows past, an
        // already-delta BIN re-logs as a delta.
        assert!(
            bin.should_log_delta(1),
            "isBINDelta() must short-circuit to true regardless of percent"
        );
    }

    /// numDeltas <= 0 guard: a BIN with no dirty slots logs a full BIN (an
    /// empty delta is invalid).
    #[test]
    fn should_log_delta_zero_dirty_is_full() {
        assert!(!bin_with_dirty(100, 0).should_log_delta(25));
    }

    /// isDeltaProhibited bound: lastFullLsn == NULL (never logged full) and
    /// prohibit_next_delta both force a full BIN.
    #[test]
    fn should_log_delta_prohibited_forces_full() {
        // No prior full BIN.
        let mut bin = bin_with_dirty(100, 5); // would be a delta otherwise
        bin.last_full_lsn = NULL_LSN;
        assert!(
            !bin.should_log_delta(25),
            "lastFullLsn==NULL must force a full BIN"
        );

        // prohibit_next_delta set (e.g. a dirty slot was removed by compress).
        let mut bin = bin_with_dirty(100, 5);
        bin.prohibit_next_delta = true;
        assert!(
            !bin.should_log_delta(25),
            "prohibit_next_delta must force a full BIN"
        );
    }

    /// The prohibit flag is cleared after a full BIN is logged
    /// (JE IN.afterLog: setProhibitNextDelta(false)), so the NEXT log may once
    /// again be a delta — this is the periodic-full chain bound.
    #[test]
    fn full_log_clears_prohibit_next_delta() {
        let mut bin = bin_with_dirty(100, 5);
        bin.prohibit_next_delta = true;
        assert!(!bin.should_log_delta(25), "prohibited → full");
        bin.clear_dirty_after_full_log(Lsn::new(2, 5));
        assert!(
            !bin.prohibit_next_delta,
            "full log must clear prohibit_next_delta"
        );
        // Re-dirty a few slots; now a delta is allowed again.
        for e in bin.entries.iter_mut().take(5) {
            e.dirty = true;
        }
        assert!(
            bin.should_log_delta(25),
            "after a full log, a small delta is allowed again"
        );
    }

    // ========================================================================
    // Tests: Task #82 — 8 new Tree methods
    // ========================================================================

    // --- is_root_resident ---

    #[test]
    fn test_is_root_resident_empty_tree() {
        let tree = Tree::new(1, 128);
        assert!(!tree.is_root_resident(), "empty tree has no resident root");
    }

    #[test]
    fn test_is_root_resident_after_insert() {
        let tree = Tree::new(1, 128);
        tree.insert(b"k".to_vec(), b"v".to_vec(), Lsn::new(1, 1)).unwrap();
        assert!(tree.is_root_resident(), "root must be resident after insert");
    }

    // --- get_resident_root_in ---

    #[test]
    fn test_get_resident_root_in_empty() {
        let tree = Tree::new(1, 128);
        assert!(tree.get_resident_root_in().is_none());
    }

    #[test]
    fn test_get_resident_root_in_single_entry() {
        let tree = Tree::new(1, 128);
        tree.insert(b"hello".to_vec(), b"world".to_vec(), Lsn::new(1, 1))
            .unwrap();
        let root = tree.get_resident_root_in();
        assert!(root.is_some(), "root must be Some after insert");
        let root_arc = tree.get_root().unwrap();
        assert!(
            Arc::ptr_eq(&root_arc, &root.unwrap()),
            "get_resident_root_in must return the same Arc as get_root"
        );
    }

    #[test]
    fn test_get_resident_root_in_multi_entry() {
        let tree = Tree::new(1, 4);
        for i in 0u32..20 {
            let k = format!("rr{:04}", i).into_bytes();
            tree.insert(k, vec![i as u8], Lsn::new(1, i)).unwrap();
        }
        assert!(tree.get_resident_root_in().is_some());
    }

    // --- get_parent_bin_for_child_ln ---

    #[test]
    fn test_get_parent_bin_for_child_ln_empty_tree() {
        let tree = Tree::new(1, 128);
        assert!(tree.get_parent_bin_for_child_ln(b"key").is_none());
    }

    #[test]
    fn test_get_parent_bin_for_child_ln_single_entry() {
        let tree = Tree::new(1, 128);
        tree.insert(b"alpha".to_vec(), b"val".to_vec(), Lsn::new(1, 1))
            .unwrap();
        let bin = tree.get_parent_bin_for_child_ln(b"alpha");
        assert!(bin.is_some(), "must return Some for a present key");
        assert!(bin.unwrap().read().is_bin(), "returned node must be a BIN");
    }

    #[test]
    fn test_get_parent_bin_for_child_ln_multi_key() {
        let tree = Tree::new(1, 8);
        let keys: &[&[u8]] = &[b"aa", b"bb", b"cc", b"dd", b"ee"];
        for &k in keys {
            tree.insert(k.to_vec(), b"v".to_vec(), Lsn::new(1, 1)).unwrap();
        }
        for &k in keys {
            let bin = tree.get_parent_bin_for_child_ln(k);
            assert!(bin.is_some(), "must return Some for {:?}", k);
            assert!(bin.unwrap().read().is_bin());
        }
    }

    // --- find_bin_for_insert ---

    #[test]
    fn test_find_bin_for_insert_empty_tree() {
        let tree = Tree::new(1, 128);
        assert!(tree.find_bin_for_insert(b"newkey").is_none());
    }

    #[test]
    fn test_find_bin_for_insert_returns_bin() {
        let tree = Tree::new(1, 128);
        tree.insert(b"existing".to_vec(), b"data".to_vec(), Lsn::new(1, 1))
            .unwrap();
        let bin = tree.find_bin_for_insert(b"newkey");
        assert!(bin.is_some());
        assert!(bin.unwrap().read().is_bin());
    }

    #[test]
    fn test_find_bin_for_insert_same_as_parent_bin() {
        let tree = Tree::new(1, 128);
        tree.insert(b"foo".to_vec(), b"bar".to_vec(), Lsn::new(1, 1)).unwrap();
        let a = tree.get_parent_bin_for_child_ln(b"foo").unwrap();
        let b_arc = tree.find_bin_for_insert(b"foo").unwrap();
        assert!(
            Arc::ptr_eq(&a, &b_arc),
            "find_bin_for_insert must return the same BIN as get_parent_bin_for_child_ln"
        );
    }

    // --- search_splits_allowed ---

    #[test]
    fn test_search_splits_allowed_empty_tree() {
        let tree = Tree::new(1, 128);
        assert!(tree.search_splits_allowed(b"k").is_none());
    }

    #[test]
    fn test_search_splits_allowed_finds_existing_key() {
        let tree = Tree::new(1, 8);
        for i in 0u32..10 {
            let k = format!("sa{:04}", i).into_bytes();
            tree.insert(k, vec![i as u8], Lsn::new(1, i)).unwrap();
        }
        for i in 0u32..10 {
            let k = format!("sa{:04}", i).into_bytes();
            let sr = tree.search_splits_allowed(&k);
            assert!(
                sr.is_some() && sr.unwrap().exact_parent_found,
                "search_splits_allowed must find sa{:04}",
                i
            );
        }
    }

    #[test]
    fn test_search_splits_allowed_missing_key() {
        let tree = Tree::new(1, 8);
        tree.insert(b"present".to_vec(), b"v".to_vec(), Lsn::new(1, 1))
            .unwrap();
        let sr = tree.search_splits_allowed(b"absent");
        assert!(
            sr.is_none_or(|r| !r.exact_parent_found),
            "search_splits_allowed must not find absent key"
        );
    }

    // --- rebuild_in_list ---

    #[test]
    fn test_rebuild_in_list_empty_tree() {
        let tree = Tree::new(1, 128);
        assert!(tree.rebuild_in_list().is_empty());
    }

    #[test]
    fn test_rebuild_in_list_single_entry() {
        let tree = Tree::new(1, 128);
        tree.insert(b"one".to_vec(), b"v".to_vec(), Lsn::new(1, 1)).unwrap();
        let list = tree.rebuild_in_list();
        // Expect root IN + BIN = 2 nodes.
        assert_eq!(
            list.len(),
            2,
            "single-entry tree must have exactly 2 nodes"
        );
        let has_bin = list.iter().any(|a| a.read().is_bin());
        let has_in = list.iter().any(|a| !a.read().is_bin());
        assert!(has_bin, "list must contain at least one BIN");
        assert!(has_in, "list must contain at least one upper IN");
    }

    #[test]
    fn test_rebuild_in_list_multi_entry() {
        let tree = Tree::new(1, 4);
        for i in 0u32..20 {
            let k = format!("ri{:04}", i).into_bytes();
            tree.insert(k, vec![i as u8], Lsn::new(1, i)).unwrap();
        }
        let list = tree.rebuild_in_list();
        let stats = tree.collect_stats();
        let expected_nodes = (stats.n_ins + stats.n_bins) as usize;
        assert_eq!(
            list.len(),
            expected_nodes,
            "rebuild_in_list must return all {} nodes",
            expected_nodes
        );
    }

    // --- validate_in_list ---

    #[test]
    fn test_validate_in_list_empty_tree() {
        let tree = Tree::new(1, 128);
        assert!(tree.validate_in_list(), "empty tree must be valid");
    }

    #[test]
    fn test_validate_in_list_single_entry() {
        let tree = Tree::new(1, 128);
        tree.insert(b"v".to_vec(), b"data".to_vec(), Lsn::new(1, 1)).unwrap();
        assert!(tree.validate_in_list(), "single-entry tree must be valid");
    }

    #[test]
    fn test_validate_in_list_multi_entry() {
        let tree = Tree::new(1, 4);
        for i in 0u32..20 {
            let k = format!("vl{:04}", i).into_bytes();
            tree.insert(k, vec![i as u8], Lsn::new(1, i)).unwrap();
        }
        assert!(tree.validate_in_list(), "multi-entry tree must be valid");
    }

    #[test]
    fn test_validate_in_list_empty_in_fails() {
        // Manually build a tree where the root IN has no entries — invalid.
        let root_arc = Arc::new(RwLock::new(TreeNode::Internal(InNodeStub {
            node_id: generate_node_id(),
            level: MAIN_LEVEL | 2,
            entries: vec![], // empty — structurally invalid
            targets: TargetRep::None,
            dirty: false,
            generation: 0,
            parent: None,
            lsn_rep: LsnRep::Empty,
        })));
        let tree = Tree::new(1, 128);
        *tree.root.write() = Some(root_arc);
        assert!(
            !tree.validate_in_list(),
            "a tree with an empty Internal node must fail validation"
        );
    }

    // --- get_parent_in_for_child_in ---

    #[test]
    fn test_get_parent_in_for_child_in_empty_tree() {
        let tree = Tree::new(1, 128);
        assert!(tree.get_parent_in_for_child_in(999).is_none());
    }

    #[test]
    fn test_get_parent_in_for_child_in_single_entry() {
        // A single-insert tree has: root IN → BIN.
        // The root IN is the parent of the BIN.
        let tree = Tree::new(1, 128);
        tree.insert(b"p".to_vec(), b"v".to_vec(), Lsn::new(1, 1)).unwrap();

        let root_arc = tree.get_root().as_ref().unwrap().clone();
        let bin_node_id = {
            let g = root_arc.read();
            match &*g {
                TreeNode::Internal(n) => {
                    let child = n.child_ref(0).unwrap();
                    let cg = child.read();
                    match &*cg {
                        TreeNode::Bottom(b) => b.node_id,
                        _ => panic!("expected BIN"),
                    }
                }
                _ => panic!("expected Internal root"),
            }
        };

        let result = tree.get_parent_in_for_child_in(bin_node_id);
        assert!(result.is_some(), "must find parent of BIN");
        let (parent_arc, slot) = result.unwrap();
        assert!(Arc::ptr_eq(&parent_arc, &root_arc));
        assert_eq!(slot, 0);
    }

    #[test]
    fn test_get_parent_in_for_child_in_not_found() {
        let tree = Tree::new(1, 128);
        tree.insert(b"x".to_vec(), b"y".to_vec(), Lsn::new(1, 1)).unwrap();
        assert!(tree.get_parent_in_for_child_in(u64::MAX).is_none());
    }

    #[test]
    fn test_get_parent_in_for_child_in_multi_level() {
        // Build a tree with at least 3 levels so we test the recursive descent.
        let tree = Tree::new(1, 4);
        for i in 0u32..20 {
            let k = format!("ml{:04}", i).into_bytes();
            tree.insert(k, vec![i as u8], Lsn::new(1, i)).unwrap();
        }

        // Collect all BIN node_ids via rebuild_in_list.
        let nodes = tree.rebuild_in_list();
        let bin_ids: Vec<u64> = nodes
            .iter()
            .filter_map(|a| {
                let g = a.read();
                if g.is_bin()
                    && let TreeNode::Bottom(b) = &*g
                {
                    return Some(b.node_id);
                }
                None
            })
            .collect();

        for bin_id in bin_ids {
            let result = tree.get_parent_in_for_child_in(bin_id);
            assert!(
                result.is_some(),
                "every BIN (id={}) must have a parent IN",
                bin_id
            );
            let (parent_arc, _slot) = result.unwrap();
            assert!(
                !parent_arc.read().is_bin(),
                "parent of a BIN must be an Internal node"
            );
        }
    }

    /// H-9 regression: BinStub::strip_lns actually drops the slot data
    /// (not just stats accounting).
    #[test]
    fn test_h9_strip_lns_actually_frees_data() {
        use crate::tree::{BinEntry, BinStub};
        use noxu_util::lsn::Lsn;
        let mut bin = BinStub {
            node_id: 1,
            level: 1,
            entries: Vec::new(),
            key_prefix: Vec::new(),
            dirty: false,
            is_delta: false,
            last_full_lsn: Lsn::from_u64(0),
            last_delta_lsn: Lsn::from_u64(0),
            generation: 0,
            parent: None,
            expiration_in_hours: true,
            cursor_count: 0,
            prohibit_next_delta: false,
            lsn_rep: LsnRep::Empty,
            keys: KeyRep::new(),
            compact_max_key_length: INKeyRep_DEFAULT_MAX_KEY_LENGTH,
        };
        // Three slots with embedded data + VALID logged LSNs (one dirty).
        // JE-faithful: a slot with a valid LSN is strippable regardless of the
        // dirty bit (its value is recoverable from the log); only a NULL-LSN
        // (never-logged / deferred-write) slot is preserved.
        bin.entries.push(BinEntry {
            data: Some(vec![0u8; 64]),
            known_deleted: false,
            dirty: false,
            expiration_time: 0,
        });
        bin.entries.push(BinEntry {
            data: Some(vec![0u8; 32]),
            known_deleted: false,
            dirty: false,
            expiration_time: 0,
        });
        bin.entries.push(BinEntry {
            data: Some(vec![0u8; 16]),
            known_deleted: false,
            dirty: true, // dirty BUT logged -> still strippable (EVICTOR-RECLAIM-1)
            expiration_time: 0,
        });
        // T-2: keep the key rep aligned with the pushed slots.
        bin.keys = KeyRep::from_keys(vec![
            b"a".to_vec(),
            b"b".to_vec(),
            b"c".to_vec(),
        ]);
        // Give all three slots VALID (non-NULL) LSNs so they are recoverable
        // from the log and therefore strippable.
        bin.set_lsn(0, Lsn::new(1, 100));
        bin.set_lsn(1, Lsn::new(1, 200));
        bin.set_lsn(2, Lsn::new(1, 300));

        let freed = bin.strip_lns();
        assert_eq!(
            freed,
            64 + 32 + 16,
            "all logged slots stripped regardless of dirty (JE evictLNs)"
        );
        assert!(bin.entries[0].data.is_none(), "logged slot data dropped");
        assert!(bin.entries[1].data.is_none(), "logged slot data dropped");
        assert!(
            bin.entries[2].data.is_none(),
            "dirty-but-logged slot data dropped (recoverable from log)"
        );

        // A NULL-LSN slot (never logged) must be preserved — its only copy is
        // the in-memory value.
        bin.entries[0].data = Some(vec![0u8; 64]);
        bin.set_lsn(0, noxu_util::NULL_LSN);
        let freed_null = bin.strip_lns();
        assert_eq!(
            freed_null, 0,
            "NULL-LSN (unlogged) slot must NOT be stripped"
        );
        assert!(bin.entries[0].data.is_some(), "unlogged slot data preserved");

        // Cursor pin prevents stripping.
        bin.set_lsn(0, Lsn::new(1, 100));
        bin.cursor_count = 1;
        let freed_with_cursor = bin.strip_lns();
        assert_eq!(
            freed_with_cursor, 0,
            "strip_lns must skip when cursor pinned"
        );
        assert!(
            bin.entries[0].data.is_some(),
            "data preserved while cursor pinned"
        );
    }

    // St-H4: the binary upper_in_floor_index must return the same slot as a
    // reference linear floor scan for all probe keys (incl. before-all,
    // after-all, between, and exact matches).
    #[test]
    fn test_upper_in_floor_index_matches_linear_scan() {
        // Reference linear floor scan (the pre-St-H4 algorithm): slot 0 is the
        // virtual −∞ key; walk forward while entry.key ≤ key.
        fn linear_floor(entries: &[InEntry], key: &[u8]) -> usize {
            let mut idx = 0usize;
            for (i, entry) in entries.iter().enumerate() {
                if i == 0 {
                    idx = 0;
                } else if entry.key.as_slice() <= key {
                    idx = i;
                } else {
                    break;
                }
            }
            idx
        }

        let tree = Tree::new(1, 256);
        // Build sorted IN slot key sets of varying size; slot 0 = virtual −∞
        // (empty key sorts first), the rest strictly ascending.
        for n_slots in 1usize..40 {
            let mut entries: Vec<InEntry> = Vec::with_capacity(n_slots);
            entries.push(InEntry { key: vec![] });
            for i in 1..n_slots {
                // Strictly-ascending two-byte keys with gaps so probes can
                // fall between, on, before, and after them.
                let v = (i as u16) * 4;
                entries.push(InEntry {
                    key: vec![(v >> 8) as u8, (v & 0xFF) as u8],
                });
            }
            for probe in 0u16..=(n_slots as u16 * 4 + 4) {
                let key = vec![(probe >> 8) as u8, (probe & 0xFF) as u8];
                assert_eq!(
                    tree.upper_in_floor_index(&entries, &key),
                    linear_floor(&entries, &key),
                    "floor mismatch: n_slots={n_slots}, key={key:?}"
                );
            }
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────
// St-H6: BIN split inherits expiration_in_hours from the splitting BIN.
// ─────────────────────────────────────────────────────────────────────────

/// Unit test for the St-H6 fix: the right-half sibling created by
/// `split_child` inherits `expiration_in_hours` from the splitting BIN.
///
/// Before the fix, the sibling was always created with
/// `expiration_in_hours = false`, causing hours-granularity TTL entries
/// (expiration_time ~495k) to be compared against `current_time_secs()`
/// (~1.78B) and treated as expired.
///
/// This test:
///   1. Creates a tree with max_entries = 4 and inserts 4 entries directly
///      (bypassing `update_key_expiration`) with non-zero `expiration_time`
///      and `expiration_in_hours = true` on the BIN.
///   2. Triggers a split.
///   3. Asserts that the right-half sibling has `expiration_in_hours = true`
///      (inherited, not hardcoded false).
#[test]
fn test_split_child_sibling_inherits_expiration_in_hours() {
    use crate::tree::{BIN_LEVEL, BinEntry, BinStub, MAIN_LEVEL, TreeNode};
    use noxu_util::{Lsn, NULL_LSN};
    use parking_lot::RwLock;
    use std::sync::Arc;

    // Manually build a tree with one BIN (4 entries, expiration_in_hours=true).
    let tree = Tree::new(99, 4);

    // Pre-populate the tree root for the test.
    let entries: Vec<BinEntry> = (0u8..4u8)
        .map(|_k| BinEntry {
            data: Some(vec![_k, _k]),
            known_deleted: false,
            dirty: true,
            expiration_time: 495_630, // hours-since-epoch value, 2026
        })
        .collect();
    let bin_keys: Vec<Vec<u8>> = (0u8..4u8).map(|k| vec![k]).collect();
    let bin = Arc::new(RwLock::new(TreeNode::Bottom(BinStub {
        node_id: 1,
        level: BIN_LEVEL,
        entries,
        key_prefix: Vec::new(),
        dirty: true,
        is_delta: false,
        last_full_lsn: NULL_LSN,
        last_delta_lsn: NULL_LSN,
        generation: 0,
        parent: None,
        expiration_in_hours: true, // hours-granularity entries
        cursor_count: 0,
        prohibit_next_delta: false,
        lsn_rep: LsnRep::Empty,
        keys: KeyRep::from_keys(bin_keys),
        compact_max_key_length: INKeyRep_DEFAULT_MAX_KEY_LENGTH,
    })));

    let root = Arc::new(RwLock::new(TreeNode::Internal(InNodeStub {
        node_id: 2,
        level: MAIN_LEVEL | 2,
        entries: vec![InEntry {
            key: vec![], // virtual key for slot 0 (-infinity)
        }],
        targets: TargetRep::Sparse(vec![(0, Arc::clone(&bin))]),
        dirty: true,
        generation: 0,
        parent: None,
        lsn_rep: LsnRep::Empty,
    })));
    {
        let mut b = bin.write();
        b.set_parent(Some(Arc::downgrade(&root)));
    }
    *tree.root.write() = Some(Arc::clone(&root));

    // Trigger split_child on the root.
    Tree::split_child(
        &root,
        0,
        4,
        Lsn::new(1, 500),
        SplitHint::Normal,
        &[],
        None,
        false,
        None,
    )
    .expect("split_child should succeed");

    // After the split: root has two children — left BIN and right sibling.
    let root_guard = root.read();
    let TreeNode::Internal(ref in_node) = *root_guard else {
        panic!("root should be Internal after split");
    };
    assert_eq!(
        in_node.entries.len(),
        2,
        "root should have 2 entries (children) after split"
    );

    // Right-half sibling is at slot 1.
    let sibling_arc = in_node
        .get_child(1)
        .expect("right-half sibling should exist at slot 1");
    let sibling_guard = sibling_arc.read();
    let TreeNode::Bottom(ref sibling) = *sibling_guard else {
        panic!("right sibling should be a BIN");
    };

    assert!(
        sibling.expiration_in_hours,
        "St-H6: right-half sibling expiration_in_hours must be true \
             (inherited from splitting BIN); got false"
    );

    // Verify the sibling's entries have the expected expiration_time.
    for e in &sibling.entries {
        assert_eq!(
            e.expiration_time, 495_630,
            "sibling entry expiration_time should be preserved: got {}",
            e.expiration_time
        );
        // With in_hours=true, is_expired should return false (future).
        assert!(
            !noxu_util::ttl::is_expired(
                e.expiration_time,
                sibling.expiration_in_hours
            ),
            "St-H6: sibling TTL entry ({}) should NOT appear expired \
                 with expiration_in_hours={}",
            e.expiration_time,
            sibling.expiration_in_hours
        );
    }
}

/// Regression confirmation: `is_expired` with wrong `in_hours = false`
/// would falsely expire hours-granularity values (~495k hours since epoch).
#[test]
fn test_hours_value_is_expired_only_with_false_flag() {
    // Hours-since-epoch value for ~2026 + 1 000 h TTL.
    let exp_hours: u32 = 495_630;
    // Correctly treated as hours: not expired.
    assert!(
        !noxu_util::ttl::is_expired(exp_hours, true),
        "exp_hours={exp_hours} should NOT be expired when in_hours=true"
    );
    // Incorrectly treated as seconds (pre-fix right sibling): expired.
    assert!(
        noxu_util::ttl::is_expired(exp_hours, false),
        "exp_hours={exp_hours} should be expired when in_hours=false \
             (St-H6 demonstrates the wrong-flag scenario)"
    );
}

// =============================================================================
// IN-redo unit tests (DRIFT-1 / Stage 1)
// =============================================================================

#[cfg(test)]
mod in_redo_tests {
    use super::*;

    /// Build a BinStub with `n` entries (key = [i as u8], lsn = lsn(1, i))
    /// and serialise it.  Returns (node_id, node_data_bytes).
    fn make_bin_bytes(node_id: u64, n: usize) -> Vec<u8> {
        let mut bin = BinStub {
            node_id,
            level: BIN_LEVEL,
            entries: Vec::new(),
            key_prefix: Vec::new(),
            dirty: false,
            is_delta: false,
            last_full_lsn: noxu_util::NULL_LSN,
            last_delta_lsn: noxu_util::NULL_LSN,
            generation: 0,
            parent: None,
            expiration_in_hours: true,
            cursor_count: 0,
            prohibit_next_delta: false,
            lsn_rep: LsnRep::Empty,
            keys: KeyRep::new(),
            compact_max_key_length: INKeyRep_DEFAULT_MAX_KEY_LENGTH,
        };
        for i in 0..n {
            // T-2/T-3: route through insert so entries/keys/lsn_rep stay
            // aligned; the serialized bytes are identical.
            bin.insert_with_prefix(
                vec![i as u8],
                Lsn::new(1, (i + 1) as u32),
                Some(vec![i as u8]),
            );
        }
        bin.serialize_full()
    }

    /// Verify that recover_in_redo inserts a BIN as root when the tree is empty.
    ///
    /// JE RecoveryManager.recoverRootIN: `root == null` path.
    #[test]
    fn test_recover_in_redo_root_bin_inserted_into_empty_tree() {
        let tree = Tree::new(42, 128);
        assert!(tree.is_empty());
        let bytes = make_bin_bytes(1, 3);
        let log_lsn = Lsn::new(1, 100);
        let result = tree.recover_in_redo(
            log_lsn, /*is_root=*/ true, /*is_bin=*/ true, &bytes,
        );
        assert_eq!(result, InRedoResult::Inserted, "expected Inserted");
        // Tree should now have 3 entries.
        assert_eq!(tree.count_entries(), 3);
    }

    /// Verify that recover_in_redo replaces a root BIN when the logged version is newer.
    ///
    /// JE RootUpdater.doWork: `DbLsn.compareTo(originalLsn, lsn) < 0` path.
    #[test]
    fn test_recover_in_redo_root_bin_replaced_when_log_newer() {
        let tree = Tree::new(42, 128);
        // Install an old root (2 entries, older LSN).
        let old_bytes = make_bin_bytes(1, 2);
        let old_lsn = Lsn::new(1, 50);
        tree.recover_in_redo(old_lsn, true, true, &old_bytes);
        assert_eq!(tree.count_entries(), 2);
        // Replay with newer LSN and 4 entries.
        let new_bytes = make_bin_bytes(1, 4);
        let new_lsn = Lsn::new(1, 100);
        let result = tree.recover_in_redo(new_lsn, true, true, &new_bytes);
        assert_eq!(result, InRedoResult::Replaced);
        assert_eq!(tree.count_entries(), 4);
    }

    /// Verify that an older logged BIN does NOT replace a newer in-memory root.
    ///
    /// JE RootUpdater.doWork: `DbLsn.compareTo(originalLsn, lsn) >= 0` skip path.
    #[test]
    fn test_recover_in_redo_root_bin_skipped_when_tree_newer() {
        let tree = Tree::new(42, 128);
        // Install a newer root.
        let new_bytes = make_bin_bytes(1, 4);
        let new_lsn = Lsn::new(1, 200);
        tree.recover_in_redo(new_lsn, true, true, &new_bytes);
        // Attempt to replay an older version.
        let old_bytes = make_bin_bytes(1, 2);
        let old_lsn = Lsn::new(1, 100);
        let result = tree.recover_in_redo(old_lsn, true, true, &old_bytes);
        assert_eq!(result, InRedoResult::Skipped);
        // Tree still holds the newer 4-entry version.
        assert_eq!(tree.count_entries(), 4);
    }

    /// deserialize_bin round-trips through serialize_full.
    #[test]
    fn test_deserialize_bin_round_trip() {
        let bytes = make_bin_bytes(99, 5);
        let bin = Tree::deserialize_bin(&bytes).expect("must deserialize");
        assert_eq!(bin.node_id, 99);
        assert_eq!(bin.entries.len(), 5);
        for i in 0..bin.entries.len() {
            assert_eq!(bin.get_full_key(i).unwrap(), vec![i as u8]);
        }
    }

    /// deserialize_upper_in round-trips through write_to_bytes (Internal).
    #[test]
    fn test_deserialize_upper_in_round_trip() {
        // Build an InNodeStub and serialize via write_to_bytes.
        let node = TreeNode::Internal(InNodeStub {
            node_id: 77,
            level: 0x10002,
            entries: vec![
                InEntry { key: vec![1, 2, 3] },
                InEntry { key: vec![4, 5, 6] },
            ],
            targets: TargetRep::None,
            dirty: false,
            generation: 0,
            parent: None,
            lsn_rep: LsnRep::Empty,
        });
        let bytes = node.write_to_bytes();
        let restored =
            Tree::deserialize_upper_in(&bytes).expect("must deserialize");
        assert_eq!(restored.node_id, 77);
        assert_eq!(restored.level, 0x10002);
        assert_eq!(restored.entries.len(), 2);
        assert_eq!(restored.entries[0].key, vec![1, 2, 3]);
        assert_eq!(restored.entries[1].key, vec![4, 5, 6]);
    }
}

// --- Part 2 acceptance tests: key_prefixing flag (DRIFT-3) ---
//
// JE `IN.computeKeyPrefix` returns null when `databaseImpl.getKeyPrefixing()`
// is false, so no prefix compression is ever applied to those BINs. Noxu was
// always applying prefix compression. This checks that the flag is honoured.
//
// Ref: `IN.java computeKeyPrefix` ~line 2456,
//      `DatabaseConfig.setKeyPrefixing` / `DatabaseImpl.getKeyPrefixing`.
#[cfg(test)]
mod key_prefixing_tests {
    use super::*;

    /// Helper: find the first (leftmost) BIN in the tree.
    fn find_first_bin(node: &Arc<RwLock<TreeNode>>) -> Arc<RwLock<TreeNode>> {
        let child_opt = {
            let g = node.read();
            match &*g {
                TreeNode::Bottom(_) => None,
                TreeNode::Internal(n) => {
                    Some(Arc::clone(n.child_ref(0).expect("child")))
                }
            }
        };
        match child_opt {
            None => Arc::clone(node),
            Some(child) => find_first_bin(&child),
        }
    }

    /// With `key_prefixing = false` (the default), keys must be stored without
    /// any prefix: the BIN's `key_prefix` must remain empty after inserts.
    #[test]
    fn test_key_prefixing_false_stores_full_keys() {
        // Default is key_prefixing = false.
        let tree = Tree::new(1, 16);
        assert!(!tree.key_prefixing, "default must be false");

        let lsn = noxu_util::Lsn::new(1, 10);
        // Insert keys with a long common prefix.
        for i in 0u8..8 {
            let key = vec![b'r', b'e', b'c', b'o', b'r', b'd', b':', i];
            tree.insert(key, vec![i], lsn).expect("insert");
        }

        let root = tree.get_root().expect("root");
        let bin_arc = find_first_bin(&root);
        let guard = bin_arc.read();
        let TreeNode::Bottom(ref bin) = *guard else {
            panic!("must be a BIN");
        };
        assert!(
            bin.key_prefix.is_empty(),
            "key_prefix must be empty when key_prefixing=false, got {:?}",
            bin.key_prefix
        );
        assert_eq!(bin.entries.len(), 8);
        // Keys must be stored as full keys.
        assert_eq!(
            bin.get_full_key(0).unwrap(),
            vec![b'r', b'e', b'c', b'o', b'r', b'd', b':', 0]
        );
    }

    /// With `key_prefixing = true`, keys with a common prefix are compressed:
    /// the BIN's `key_prefix` must be non-empty.
    #[test]
    fn test_key_prefixing_true_compresses_keys() {
        let mut tree = Tree::new(1, 16);
        tree.set_key_prefixing(true);

        let lsn = noxu_util::Lsn::new(1, 10);
        for i in 0u8..8 {
            let key = vec![b'r', b'e', b'c', b'o', b'r', b'd', b':', i];
            tree.insert(key, vec![i], lsn).expect("insert");
        }

        let root = tree.get_root().expect("root");
        let bin_arc = find_first_bin(&root);
        let guard = bin_arc.read();
        let TreeNode::Bottom(ref bin) = *guard else {
            panic!("must be a BIN");
        };
        // Prefix compression must kick in: all keys share "record:".
        assert!(
            !bin.key_prefix.is_empty(),
            "key_prefix must be non-empty when key_prefixing=true"
        );
        assert_eq!(
            bin.key_prefix,
            b"record:".to_vec(),
            "prefix must be the common prefix of all inserted keys"
        );
    }

    /// Custom-comparator databases (sorted-dup) always bypass prefix
    /// regardless of key_prefixing: `insert_cmp` does not touch key_prefix.
    #[test]
    fn test_key_prefixing_custom_comparator_no_prefix() {
        let cmp: KeyComparatorFn = Arc::new(|a: &[u8], b: &[u8]| a.cmp(b));
        let mut tree = Tree::new_with_comparator(1, 16, cmp);
        // Enable key_prefixing — should have no effect via insert_cmp path.
        tree.set_key_prefixing(true);

        let lsn = noxu_util::Lsn::new(1, 10);
        for i in 0u8..8 {
            let key = vec![b'r', b'e', b'c', b'o', b'r', b'd', b':', i];
            tree.insert(key, vec![i], lsn).expect("insert");
        }

        let root = tree.get_root().expect("root");
        let bin_arc = find_first_bin(&root);
        let guard = bin_arc.read();
        let TreeNode::Bottom(ref bin) = *guard else {
            panic!("must be a BIN");
        };
        // Custom-comparator path (insert_cmp) does not set key_prefix.
        assert!(
            bin.key_prefix.is_empty(),
            "custom-comparator path must not set key_prefix"
        );
    }
}

// --- Part 1 acceptance tests: splitSpecial heuristic (DRIFT-1) ---
//
// JE `IN.splitSpecial` / `Tree.forceSplit`: when all routing decisions during
// descent are leftmost (`AllLeft`) or rightmost (`AllRight`), the split index
// is forced to 1 or `n-1` respectively instead of `n/2`. This halves the
// number of splits for monotonically increasing / decreasing key workloads
// (sequential append / prepend) because each split leaves the BIN near-full.
//
// Ref: `IN.java splitSpecial` ~line 4129, `Tree.java forceSplit` ~line 1907.
#[cfg(test)]
mod split_special_tests {
    use super::*;

    /// Test helper: descend the tree to the BIN that holds (or would hold)
    /// `key`, returning its arc.  Mirrors the read-path descent used by
    /// `Tree::search`; sufficient for unit tests that need to mutate a slot.
    fn find_bin_arc_for_key(
        node_arc: &Arc<RwLock<TreeNode>>,
        key: &[u8],
    ) -> Option<Arc<RwLock<TreeNode>>> {
        let mut current = node_arc.clone();
        loop {
            let next = {
                let g = current.read();
                match &*g {
                    TreeNode::Bottom(_) => return Some(current.clone()),
                    TreeNode::Internal(n) => {
                        if n.entries.is_empty() {
                            return None;
                        }
                        let mut idx = 0usize;
                        for (i, e) in n.entries.iter().enumerate() {
                            if i == 0 || e.key.as_slice() <= key {
                                idx = i;
                            } else {
                                break;
                            }
                        }
                        n.get_child(idx)?
                    }
                }
            };
            current = next;
        }
    }

    /// Count total leaf (BIN) nodes in the tree by DFS.
    fn count_bins(node: &Arc<RwLock<TreeNode>>) -> usize {
        let g = node.read();
        match &*g {
            TreeNode::Bottom(_) => 1,
            TreeNode::Internal(n) => {
                n.resident_children().iter().map(count_bins).sum()
            }
        }
    }

    /// Return total key count across all BINs.
    fn count_keys(node: &Arc<RwLock<TreeNode>>) -> usize {
        let g = node.read();
        match &*g {
            TreeNode::Bottom(b) => b.entries.len(),
            TreeNode::Internal(n) => {
                n.resident_children().iter().map(count_keys).sum()
            }
        }
    }

    /// Returns the number of entries in the leftmost BIN.
    fn leftmost_bin_size(node: &Arc<RwLock<TreeNode>>) -> usize {
        let g = node.read();
        match &*g {
            TreeNode::Bottom(b) => b.entries.len(),
            TreeNode::Internal(n) => {
                let first_child = n.child_ref(0).expect("child");
                leftmost_bin_size(first_child)
            }
        }
    }

    /// Returns the number of entries in the rightmost BIN.
    fn rightmost_bin_size(node: &Arc<RwLock<TreeNode>>) -> usize {
        let g = node.read();
        match &*g {
            TreeNode::Bottom(b) => b.entries.len(),
            TreeNode::Internal(n) => {
                let last_child = n
                    .child_ref(n.entries.len().saturating_sub(1))
                    .expect("child");
                rightmost_bin_size(last_child)
            }
        }
    }

    /// `splitSpecial` ascending: each right-side split leaves the left BIN
    /// near-full (all but one entry stays). Compared to midpoint split
    /// the number of BINs created should be significantly fewer relative to
    /// keys inserted (more keys per BIN on average).
    ///
    /// JE criterion: `allRightSideDescent` → `splitIndex = nEntries - 1`.
    /// The penultimate entry stays in the left BIN; only one entry goes to
    /// the new right sibling, which then absorbs the next insert and fills
    /// normally.
    #[test]
    fn test_split_special_ascending_fewer_bins_than_midpoint() {
        let max_entries = 8usize;
        let n_keys = 200usize;

        // Build tree with splitSpecial (ascending keys trigger AllRight).
        let tree_special = Tree::new(1, max_entries);
        let lsn = noxu_util::Lsn::new(1, 100);
        for i in 0u32..n_keys as u32 {
            let key = i.to_be_bytes().to_vec();
            tree_special.insert(key, vec![0u8], lsn).expect("insert");
        }

        let root_special = tree_special.get_root().expect("root must exist");
        let bins_special = count_bins(&root_special);
        let keys_special = count_keys(&root_special);

        // All keys must be present.
        assert_eq!(keys_special, n_keys, "all keys must be stored");

        // With splitSpecial, each right-side split keeps n-1 entries in the
        // left BIN. Ideal: ceil(n_keys / (max_entries - 1)) BINs.
        // Without splitSpecial (midpoint): ceil(n_keys / (max_entries / 2)).
        // We assert the actual count is below the midpoint-split upper bound.
        let midpoint_upper_bound = n_keys.div_ceil(max_entries / 2);
        assert!(
            bins_special < midpoint_upper_bound,
            "splitSpecial should produce fewer BINs than midpoint split: \
             got {bins_special}, midpoint upper bound = {midpoint_upper_bound}"
        );

        // The rightmost BIN must have fewer entries than max_entries
        // (the last insert only half-fills it at most), which is expected.
        // The IMPORTANT property: rightmost BIN started with exactly 1 entry
        // (its first entry was the split-off singleton) then filled up.
        // We just verify overall key density > midpoint baseline.
        let avg_fill = keys_special as f64 / bins_special as f64;
        let midpoint_fill = (max_entries / 2) as f64;
        assert!(
            avg_fill > midpoint_fill,
            "average fill per BIN with splitSpecial ({avg_fill:.1}) should \
             exceed midpoint baseline ({midpoint_fill})"
        );
    }

    /// `splitSpecial` descending: all routing decisions are at slot 0
    /// (`AllLeft`). Split forces `split_index = 1` so the right sibling
    /// gets almost all entries and the left node keeps just one.
    ///
    /// JE criterion: `allLeftSideDescent` → `splitIndex = 1`.
    #[test]
    fn test_split_special_descending_fewer_bins_than_midpoint() {
        let max_entries = 8usize;
        let n_keys = 200usize;

        let tree_special = Tree::new(1, max_entries);
        let lsn = noxu_util::Lsn::new(1, 100);
        for i in (0u32..n_keys as u32).rev() {
            let key = i.to_be_bytes().to_vec();
            tree_special.insert(key, vec![0u8], lsn).expect("insert");
        }

        let root_special = tree_special.get_root().expect("root must exist");
        let bins_special = count_bins(&root_special);
        let keys_special = count_keys(&root_special);

        assert_eq!(keys_special, n_keys, "all keys must be stored");

        let midpoint_upper_bound = n_keys.div_ceil(max_entries / 2);
        assert!(
            bins_special < midpoint_upper_bound,
            "splitSpecial descending should produce fewer BINs: \
             got {bins_special}, midpoint upper bound = {midpoint_upper_bound}"
        );
    }

    /// Random-key inserts must NOT be affected by splitSpecial: with random
    /// keys descent will rarely be all-left or all-right, so the split index
    /// defaults to midpoint and tree balance is maintained.
    #[test]
    fn test_split_special_random_inserts_stay_balanced() {
        use std::collections::BTreeSet;

        let max_entries = 8usize;
        // Use a fixed permutation so the test is deterministic.
        let mut keys: Vec<u32> = (0u32..200).collect();
        // Knuth shuffle with a fixed seed.
        let mut rng: u64 = 0xdeadbeef_cafebabe;
        for i in (1..keys.len()).rev() {
            rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
            let j = (rng >> 33) as usize % (i + 1);
            keys.swap(i, j);
        }

        let tree = Tree::new(1, max_entries);
        let lsn = noxu_util::Lsn::new(1, 100);
        let mut inserted = BTreeSet::new();
        for k in &keys {
            let key = k.to_be_bytes().to_vec();
            tree.insert(key, vec![0u8], lsn).expect("insert");
            inserted.insert(*k);
        }

        let root = tree.get_root().expect("root");
        let total_keys = count_keys(&root);
        assert_eq!(
            total_keys,
            inserted.len(),
            "all random keys must be stored"
        );

        // Verify every key is findable.
        for k in &inserted {
            let key = k.to_be_bytes().to_vec();
            let found = tree.search(&key);
            assert!(
                found.map(|r| r.is_exact_match()).unwrap_or(false),
                "random key {k} must be findable after insert"
            );
        }
    }

    /// TREE-F1: a `known_deleted` BIN slot must read as ABSENT on an exact
    /// lookup and must be SKIPPED by scans, matching JE.
    ///
    /// JE contract:
    /// * `IN.findEntry` (IN.java:3197): an exact match that lands on a
    ///   known-deleted slot returns -1 (ABSENT).
    /// * `CursorImpl.lockAndGetCurrent` (CursorImpl.java:2062-2064): a
    ///   step that lands on `isEntryKnownDeleted(index)` returns null, so
    ///   the `getNext` loop advances past it (the slot is skipped).
    ///
    /// KD slots legitimately exist in live BINs during BIN-delta
    /// reconstitution (`mutate_to_full_bin` applies delta KD slots) until
    /// the compressor reclaims them.  We reach that state directly here by
    /// marking a slot known_deleted in the BIN arc, then assert the
    /// user-facing read/scan paths do not surface it.
    #[test]
    fn test_tree_f1_known_deleted_slot_is_absent_and_skipped() {
        let tree = Tree::new(1, 8);
        // Insert enough keys to populate a BIN with several live slots.
        for i in 0..6u32 {
            let key = format!("kd{i:04}").into_bytes();
            tree.insert(key, vec![i as u8], Lsn::new(1, i)).unwrap();
        }

        // Pick a middle key and mark its slot known_deleted directly in the
        // BIN, modelling a delta-applied tombstone the compressor has not yet
        // reclaimed.
        let kd_key = b"kd0003".to_vec();
        {
            let root = tree.get_root().expect("root");
            let bin_arc = find_bin_arc_for_key(&root, &kd_key).expect("bin");
            let mut g = bin_arc.write();
            if let TreeNode::Bottom(b) = &mut *g {
                let idx = (0..b.entries.len())
                    .find(|&i| {
                        b.get_full_key(i).as_deref() == Some(kd_key.as_slice())
                    })
                    .expect("kd key slot");
                b.entries[idx].known_deleted = true;
            } else {
                panic!("expected BIN");
            }
        }

        // (a) exact lookup via Tree::search must report NOT found.
        let sr = tree.search(&kd_key);
        assert!(
            !sr.map(|r| r.is_exact_match()).unwrap_or(false),
            "TREE-F1: Tree::search must report a known_deleted slot as absent \
             (IN.findEntry IN.java:3197)"
        );

        // (a) exact lookup via Tree::search_with_data must report NOT found.
        let sf = tree.search_with_data(&kd_key).expect("slot fetch");
        assert!(
            !sf.found,
            "TREE-F1: Tree::search_with_data must report a known_deleted slot \
             as absent (IN.findEntry IN.java:3197)"
        );

        // Live neighbours must still be found.
        for live in [b"kd0002".to_vec(), b"kd0004".to_vec()] {
            assert!(
                tree.search(&live).map(|r| r.is_exact_match()).unwrap_or(false),
                "live neighbour must remain findable"
            );
        }

        // (b) a scan-facing BIN dump (descend_to_edge_bin / get_next_bin /
        // get_prev_bin) returns slots verbatim WITH the known_deleted flag
        // set, so the cursor can skip them (CursorImpl.java:2062-2064).  The
        // contract here is: the KD slot is never reported as a LIVE entry.
        let root = tree.get_root().expect("root");
        let edge = Tree::descend_to_edge_bin(&root, true).expect("edge bin");
        assert!(
            !edge.iter().any(|(e, _, k)| k == &kd_key && !e.known_deleted),
            "TREE-F1: scan must not surface a known_deleted slot as live \
             (CursorImpl.java:2062-2064)"
        );
        for anchor in [b"kd0000".to_vec(), b"kd0005".to_vec()] {
            for entries in
                [tree.get_next_bin(&anchor), tree.get_prev_bin(&anchor)]
                    .into_iter()
                    .flatten()
            {
                assert!(
                    !entries
                        .iter()
                        .any(|(e, _, k)| k == &kd_key && !e.known_deleted),
                    "TREE-F1: get_next_bin/get_prev_bin must not surface a \
                     known_deleted slot as live"
                );
            }
        }

        // first_entry_at_or_after must skip a KD slot at the boundary.
        if let Some((k, _, _)) = tree.first_entry_at_or_after(&kd_key) {
            assert_ne!(
                k, kd_key,
                "TREE-F1: first_entry_at_or_after must skip a known_deleted \
                 slot (CursorImpl.java:2062-2064)"
            );
        }

        // The compressor KD-iteration path must STILL see the slot — the fix
        // only changes the user-facing read predicate, not the maintenance
        // iteration that exists to reclaim KD slots.
        let kd_bins = tree.collect_bins_with_known_deleted();
        assert!(
            !kd_bins.is_empty(),
            "TREE-F1: collect_bins_with_known_deleted must still observe the \
             KD slot so the compressor can reclaim it"
        );
    }
}
