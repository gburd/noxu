//! Eviction policy trait and algorithm selection enum.
//!
//! An [`EvictionPolicy`] tracks a set of node IDs and decides which one to
//! evict next.  All methods take `&self` — implementations use interior
//! mutability (a `Mutex` around their state) so they can be shared across
//! threads as `Arc<dyn EvictionPolicy>`.

use std::fmt;

/// Pluggable cache eviction policy.
///
/// The policy owns a set of node IDs.  It decides the eviction order but does
/// not hold the actual cached data — that lives in the B-tree layer.  The
/// [`crate::evictor::Evictor`] calls these methods to manage its two
/// independent tracking sets: `primary` (normal accesses) and `scan_resistant`
/// (sequential / scan accesses).
///
/// # Thread safety
/// All implementations are `Send + Sync`.
pub trait EvictionPolicy: Send + Sync + fmt::Debug {
    /// Insert a new node at the hot (most-recently-used) end.
    /// Silently ignores the call if the node is already tracked.
    fn insert(&self, node_id: u64);

    /// Insert a new node at the cold (least-recently-used / evictable) end.
    /// Used for scan pages or nodes that should be evicted preferentially.
    /// Silently ignores the call if the node is already tracked.
    fn insert_cold(&self, node_id: u64);

    /// Record a hit on an already-tracked node (move toward MRU end).
    /// Returns `false` if the node is not in this policy.
    fn touch(&self, node_id: u64) -> bool;

    /// Remove a specific node from tracking.
    /// Returns `false` if the node was not tracked.
    fn remove(&self, node_id: u64) -> bool;

    /// Select and remove the best eviction candidate.
    /// Returns `None` if no nodes are tracked.
    fn evict_candidate(&self) -> Option<u64>;

    /// Return a node to the policy after it was selected but could not be
    /// evicted (e.g., it was pinned).  The node is re-inserted at the hot end.
    fn put_back(&self, node_id: u64);

    /// Returns `true` if this node is currently tracked.
    fn contains(&self, node_id: u64) -> bool;

    /// Number of nodes currently tracked.
    fn len(&self) -> usize;

    /// True if no nodes are tracked.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Human-readable algorithm name (for logging / stats).
    fn name(&self) -> &'static str;
}

// ---------------------------------------------------------------------------
// EvictionAlgorithm enum
// ---------------------------------------------------------------------------

/// Selects the cache eviction algorithm used by a policy slot in the
/// [`crate::evictor::Evictor`].
///
/// The default is [`Lru`][EvictionAlgorithm::Lru].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum EvictionAlgorithm {
    /// Least Recently Used — slab doubly-linked list + HashMap index.
    ///
    /// O(1) per operation; simple and cache-friendly.  Susceptible to
    /// sequential-scan cache pollution.
    #[default]
    Lru,

    /// Clock (Second Chance) — circular buffer with per-page reference bits.
    ///
    /// O(1) amortised; lower constant overhead than LRU.  Gives each page one
    /// free pass before eviction (reference bit cleared on first encounter).
    /// Used by Postgres for its shared buffer manager (`freelist`).
    Clock,

    /// Adaptive Replacement Cache (ARC).
    ///
    /// Maintains two LRU lists (T1 = single-touch, T2 = multi-touch) whose
    /// relative sizes adapt automatically to the observed access pattern.
    /// Inherently resistant to sequential scan pollution.
    ///
    /// Reference: Megiddo & Modha, USENIX FAST 2003.
    Arc,

    /// Clock with Adaptive Replacement (CAR).
    ///
    /// Replaces ARC's LRU lists with Clock lists to reduce overhead while
    /// retaining ARC's adaptive two-pool behaviour.
    ///
    /// Reference: Bansal & Modha, USENIX FAST 2004.
    Car,

    /// Low Inter-reference Recency Set (LIRS).
    ///
    /// Classifies pages as LIR (hot, protected) or HIR (cold, evictable)
    /// based on inter-reference recency.  Evicts HIR pages first; provides
    /// strong protection against sequential scan pollution with low overhead.
    ///
    /// Reference: Jiang & Zhang, ACM SIGMETRICS 2002.
    Lirs,
}

impl EvictionAlgorithm {
    /// Instantiate a fresh, empty policy for this algorithm.
    pub fn new_policy(self) -> Box<dyn EvictionPolicy> {
        use crate::policies::{ArcPolicy, CarPolicy, ClockPolicy, LirsPolicy, LruPolicy};
        match self {
            EvictionAlgorithm::Lru   => Box::new(LruPolicy::new()),
            EvictionAlgorithm::Clock => Box::new(ClockPolicy::new()),
            EvictionAlgorithm::Arc   => Box::new(ArcPolicy::new()),
            EvictionAlgorithm::Car   => Box::new(CarPolicy::new()),
            EvictionAlgorithm::Lirs  => Box::new(LirsPolicy::new()),
        }
    }
}
