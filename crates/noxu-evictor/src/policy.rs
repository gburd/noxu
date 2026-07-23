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
/// The default is [`Lru`][EvictionAlgorithm::Lru] — the JE-faithful policy
/// (JE's evictor is LRU-with-priorities).  The scan-resistant alternatives
/// (`Clock`, `Arc`, `Car`, `Lirs`, `CoolHot`) are gated behind the
/// `experimental-eviction-policies` cargo feature (default OFF): they are
/// preserved for future benchmark work but are not part of the default,
/// benchmark-validated surface.  See
/// `docs/src/reference/eviction-policies.md`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum EvictionAlgorithm {
    /// Least Recently Used — slab doubly-linked list + HashMap index.
    ///
    /// O(1) per operation; simple and cache-friendly.  The JE-faithful
    /// default (JE's `Evictor` / `LRUEvictor`).  Susceptible to
    /// sequential-scan cache pollution, which the B-tree layer mitigates via
    /// the scan-resistant tracking slot and `CacheMode`.
    #[default]
    Lru,

    /// COOL/HOT 2-bit cooling clock (LeanStore / 2Q-A1 model).
    ///
    /// *Experimental* — requires the `experimental-eviction-policies` feature.
    ///
    /// A node is HOT (working set) or COOL (eviction candidate) plus a
    /// second-chance reference bit.  A node is *admitted* COOL; a second
    /// access promotes it COOL → HOT.  The sweep prefers COOL victims and
    /// only demotes HOT → COOL once a full pass finds none.  Scan-resistant
    /// **by construction**.
    ///
    /// Ported from the PostgreSQL buffer-manager cooling-stage proposal; see
    /// `crate::policies::coolhot`.
    #[cfg(feature = "experimental-eviction-policies")]
    CoolHot,

    /// Clock (Second Chance) — circular buffer with per-page reference bits.
    ///
    /// *Experimental* — requires the `experimental-eviction-policies` feature.
    ///
    /// O(1) amortised; lower constant overhead than LRU.  Gives each page one
    /// free pass before eviction (reference bit cleared on first encounter).
    /// Used by Postgres for its shared buffer manager (`freelist`).
    #[cfg(feature = "experimental-eviction-policies")]
    Clock,

    /// Adaptive Replacement Cache (ARC).
    ///
    /// *Experimental* — requires the `experimental-eviction-policies` feature.
    /// **Patent note:** ARC was covered by IBM patent US 6,996,676 (and
    /// related), now expired.  See `docs/src/reference/eviction-policies.md`.
    ///
    /// Maintains two LRU lists (T1 = single-touch, T2 = multi-touch) whose
    /// relative sizes adapt automatically to the observed access pattern.
    /// Inherently resistant to sequential scan pollution.
    ///
    /// Reference: Megiddo & Modha, USENIX FAST 2003.
    #[cfg(feature = "experimental-eviction-policies")]
    Arc,

    /// Clock with Adaptive Replacement (CAR).
    ///
    /// *Experimental* — requires the `experimental-eviction-policies` feature.
    /// **Patent note:** CAR derives from IBM's ARC work (see `Arc`).  See
    /// `docs/src/reference/eviction-policies.md`.
    ///
    /// Replaces ARC's LRU lists with Clock lists to reduce overhead while
    /// retaining ARC's adaptive two-pool behaviour.
    ///
    /// Reference: Bansal & Modha, USENIX FAST 2004.
    #[cfg(feature = "experimental-eviction-policies")]
    Car,

    /// Low Inter-reference Recency Set (LIRS).
    ///
    /// *Experimental* — requires the `experimental-eviction-policies` feature.
    ///
    /// Classifies pages as LIR (hot, protected) or HIR (cold, evictable)
    /// based on inter-reference recency.  Evicts HIR pages first; provides
    /// strong protection against sequential scan pollution with low overhead.
    ///
    /// Reference: Jiang & Zhang, ACM SIGMETRICS 2002.
    #[cfg(feature = "experimental-eviction-policies")]
    Lirs,
}

impl EvictionAlgorithm {
    /// Parse an algorithm name (case-insensitive) as used by the
    /// `noxu.evictor.algorithm` config parameter. Unknown names fall back to
    /// the default ([`Lru`][EvictionAlgorithm::Lru]) so a typo never panics at
    /// env-open; callers that want strictness can compare the result.
    ///
    /// The scan-resistant policy names (`clock`, `arc`, `car`, `lirs`,
    /// `coolhot`) are only recognised when the `experimental-eviction-policies`
    /// feature is enabled; otherwise they log a warning and fall back to LRU.
    pub fn from_name(name: &str) -> EvictionAlgorithm {
        let lower = name.trim().to_ascii_lowercase();
        match lower.as_str() {
            "lru" => EvictionAlgorithm::Lru,
            #[cfg(feature = "experimental-eviction-policies")]
            "clock" => EvictionAlgorithm::Clock,
            #[cfg(feature = "experimental-eviction-policies")]
            "arc" => EvictionAlgorithm::Arc,
            #[cfg(feature = "experimental-eviction-policies")]
            "car" => EvictionAlgorithm::Car,
            #[cfg(feature = "experimental-eviction-policies")]
            "lirs" => EvictionAlgorithm::Lirs,
            #[cfg(feature = "experimental-eviction-policies")]
            "coolhot" | "cool_hot" | "cool-hot" => EvictionAlgorithm::CoolHot,
            #[cfg(not(feature = "experimental-eviction-policies"))]
            "clock" | "arc" | "car" | "lirs" | "coolhot" | "cool_hot"
            | "cool-hot" => {
                log::warn!(
                    "eviction policy \"{lower}\" requires the \
                     experimental-eviction-policies feature; falling back to LRU"
                );
                EvictionAlgorithm::Lru
            }
            _ => EvictionAlgorithm::Lru,
        }
    }

    /// Instantiate a fresh, empty policy for this algorithm.
    pub fn new_policy(self) -> Box<dyn EvictionPolicy> {
        use crate::policies::LruPolicy;
        #[cfg(feature = "experimental-eviction-policies")]
        use crate::policies::{
            ArcPolicy, CarPolicy, ClockPolicy, CoolHotPolicy, LirsPolicy,
        };
        match self {
            EvictionAlgorithm::Lru => Box::new(LruPolicy::new()),
            #[cfg(feature = "experimental-eviction-policies")]
            EvictionAlgorithm::CoolHot => Box::new(CoolHotPolicy::new()),
            #[cfg(feature = "experimental-eviction-policies")]
            EvictionAlgorithm::Clock => Box::new(ClockPolicy::new()),
            #[cfg(feature = "experimental-eviction-policies")]
            EvictionAlgorithm::Arc => Box::new(ArcPolicy::new()),
            #[cfg(feature = "experimental-eviction-policies")]
            EvictionAlgorithm::Car => Box::new(CarPolicy::new()),
            #[cfg(feature = "experimental-eviction-policies")]
            EvictionAlgorithm::Lirs => Box::new(LirsPolicy::new()),
        }
    }
}
