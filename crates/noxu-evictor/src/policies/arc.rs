//! Adaptive Replacement Cache (ARC) eviction policy.
//!
//! ARC maintains four lists:
//!   T1 — pages seen exactly once recently (single-touch, recency)
//!   T2 — pages seen at least twice recently (multi-touch, frequency)
//!   B1 — ghost entries for pages evicted from T1 (LRU, no cached data)
//!   B2 — ghost entries for pages evicted from T2 (LRU, no cached data)
//!
//! A target parameter `p` controls the relative size of T1 vs T2.  It
//! increases when a ghost hit occurs in B1 (indicating that T1 was too small)
//! and decreases when a ghost hit occurs in B2 (indicating T2 was too small).
//!
//! On eviction (`evict_candidate`):
//!   if |T1| > p  →  evict LRU(T1), add to B1
//!   else         →  evict LRU(T2), add to B2
//!
//! References:
//!   Megiddo & Modha, "ARC: A Self-Tuning, Low Overhead Replacement Cache",
//!   USENIX FAST 2003.

use crate::policy::EvictionPolicy;
use crate::slab::SlabList;
use noxu_sync::Mutex;

/// Maximum ghost set size as a multiple of the live set (T1+T2).  Ghost
/// entries beyond this limit are evicted from the oldest end of B1/B2 to
/// prevent unbounded memory use.
const MAX_GHOST_RATIO: usize = 2;
const MIN_GHOST_CAP: usize = 64;

#[derive(Debug)]
struct ArcState {
    /// Recent single-touch pages.
    t1: SlabList,
    /// Frequent multi-touch pages.
    t2: SlabList,
    /// Ghost entries for T1 evictions.
    b1: SlabList,
    /// Ghost entries for T2 evictions.
    b2: SlabList,
    /// Target size for T1 (floating point for smooth adaptation).
    p: f64,
}

impl ArcState {
    fn new() -> Self {
        Self {
            t1: SlabList::new(),
            t2: SlabList::new(),
            b1: SlabList::new(),
            b2: SlabList::new(),
            p: 0.0,
        }
    }

    fn live_len(&self) -> usize {
        self.t1.len + self.t2.len
    }

    fn ghost_cap(&self) -> usize {
        (self.live_len() * MAX_GHOST_RATIO).max(MIN_GHOST_CAP)
    }

    /// Trim ghost set `b` to at most `cap` entries.
    fn trim_ghost(b: &mut SlabList, cap: usize) {
        while b.len > cap {
            b.remove_front();
        }
    }

    /// Core replacement: select a victim from T1 or T2 based on `p`.
    /// Returns the evicted node_id and a flag indicating whether it came
    /// from T1 (true) or T2 (false).
    fn replace(&mut self) -> Option<(u64, bool)> {
        if self.t1.len == 0 && self.t2.len == 0 {
            return None;
        }
        let t1_len = self.t1.len as f64;
        // Evict from T1 when it is larger than target, unless T1 is empty.
        let evict_t1 = self.t1.len > 0 && (t1_len > self.p || self.t2.len == 0);
        if evict_t1 {
            let id = self.t1.remove_front().unwrap();
            self.b1.add_back(id);
            let cap = self.ghost_cap();
            Self::trim_ghost(&mut self.b1, cap);
            Some((id, true))
        } else {
            let id = self.t2.remove_front().unwrap();
            self.b2.add_back(id);
            let cap = self.ghost_cap();
            Self::trim_ghost(&mut self.b2, cap);
            Some((id, false))
        }
    }

    fn insert(&mut self, id: u64) {
        if self.t1.contains(id) || self.t2.contains(id) {
            // Already tracked — treat as a hit.
            self.on_hit(id);
            return;
        }
        if self.b1.contains(id) {
            // Ghost hit in B1: T1 was too small → increase p.
            let b1 = self.b1.len as f64;
            let b2 = self.b2.len as f64;
            let delta = if b1 >= b2 { 1.0 } else { b2 / b1.max(1.0) };
            self.p =
                (self.p + delta).min((self.t1.len + self.t2.len + 1) as f64);
            self.replace();
            self.b1.remove(id);
            self.t2.add_back(id);
            return;
        }
        if self.b2.contains(id) {
            // Ghost hit in B2: T2 was too small → decrease p.
            let b1 = self.b1.len as f64;
            let b2 = self.b2.len as f64;
            let delta = if b2 >= b1 { 1.0 } else { b1 / b2.max(1.0) };
            self.p = (self.p - delta).max(0.0);
            self.replace();
            self.b2.remove(id);
            self.t2.add_back(id);
            return;
        }
        // Complete miss: add to T1 (recency).
        self.t1.add_back(id);
    }

    fn on_hit(&mut self, id: u64) {
        if self.t1.remove(id) {
            // Promote from T1 to T2 (now seen more than once).
            self.t2.add_back(id);
        } else {
            // Already in T2: move to MRU end.
            self.t2.move_back(id);
        }
    }
}

/// Adaptive Replacement Cache eviction policy.
#[derive(Debug)]
pub struct ArcPolicy {
    state: Mutex<ArcState>,
}

impl ArcPolicy {
    pub fn new() -> Self {
        Self { state: Mutex::new(ArcState::new()) }
    }
}

impl Default for ArcPolicy {
    fn default() -> Self {
        Self::new()
    }
}

impl EvictionPolicy for ArcPolicy {
    fn insert(&self, node_id: u64) {
        self.state.lock().insert(node_id);
    }

    fn insert_cold(&self, node_id: u64) {
        // Insert at the cold (LRU) end of T1.
        let mut s = self.state.lock();
        if !s.t1.contains(node_id) && !s.t2.contains(node_id) {
            if s.b1.contains(node_id) || s.b2.contains(node_id) {
                // Ghost hit — use normal insert path.
                s.insert(node_id);
            } else {
                s.t1.add_front(node_id); // cold end of T1
            }
        }
    }

    fn touch(&self, node_id: u64) -> bool {
        let mut s = self.state.lock();
        if s.t1.contains(node_id) || s.t2.contains(node_id) {
            s.on_hit(node_id);
            true
        } else {
            false
        }
    }

    fn remove(&self, node_id: u64) -> bool {
        let mut s = self.state.lock();
        if s.t1.remove(node_id) {
            return true;
        }
        if s.t2.remove(node_id) {
            return true;
        }
        // Also clean up ghost entries if the caller explicitly removes them.
        if s.b1.remove(node_id) {
            return true;
        }
        s.b2.remove(node_id)
    }

    fn evict_candidate(&self) -> Option<u64> {
        self.state.lock().replace().map(|(id, _)| id)
    }

    fn put_back(&self, node_id: u64) {
        // Returned node goes back to T2 MRU end (it was used, just pinned).
        let mut s = self.state.lock();
        if !s.t1.contains(node_id) && !s.t2.contains(node_id) {
            s.t2.add_back(node_id);
        }
    }

    fn contains(&self, node_id: u64) -> bool {
        let s = self.state.lock();
        s.t1.contains(node_id) || s.t2.contains(node_id)
    }

    fn len(&self) -> usize {
        let s = self.state.lock();
        s.t1.len + s.t2.len
    }

    fn name(&self) -> &'static str {
        "ARC"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::policy::EvictionPolicy;

    #[test]
    fn test_arc_promotes_on_second_access() {
        let p = ArcPolicy::new();
        // First insert: goes to T1.
        p.insert(1);
        p.insert(2);
        p.insert(3);
        assert_eq!(p.len(), 3);
        // Second access: promote to T2.
        p.touch(1);
        assert_eq!(p.len(), 3);
    }

    #[test]
    fn test_arc_evict_candidate() {
        let p = ArcPolicy::new();
        p.insert(1);
        p.insert(2);
        p.insert(3);
        let v = p.evict_candidate().unwrap();
        assert!(v == 1 || v == 2 || v == 3);
        assert_eq!(p.len(), 2);
    }

    #[test]
    fn test_arc_ghost_hit_adjusts_p() {
        let p = ArcPolicy::new();
        // Insert 4 pages, evict 2 (they become B1 ghosts).
        for i in 1u64..=4 {
            p.insert(i);
        }
        let v1 = p.evict_candidate().unwrap(); // goes to B1
        let v2 = p.evict_candidate().unwrap(); // goes to B1
        // Re-insert v1 — ghost hit in B1 should increase p.
        let before_p = { p.state.lock().p };
        p.insert(v1);
        let after_p = { p.state.lock().p };
        assert!(after_p >= before_p, "p should not decrease on B1 ghost hit");
        let _ = v2;
    }

    #[test]
    fn test_arc_put_back() {
        let p = ArcPolicy::new();
        p.insert(1);
        p.insert(2);
        let v = p.evict_candidate().unwrap();
        p.put_back(v);
        assert_eq!(p.len(), 2); // v was re-inserted into T2
    }

    #[test]
    fn test_arc_remove() {
        let p = ArcPolicy::new();
        p.insert(1);
        p.insert(2);
        p.insert(3);
        assert!(p.remove(2));
        assert!(!p.remove(2));
        assert_eq!(p.len(), 2);
    }

    /// Ghost sets should not grow unboundedly.
    #[test]
    fn test_arc_ghost_bounded() {
        let p = ArcPolicy::new();
        // Insert and evict 1000 unique pages.
        for i in 0u64..1000 {
            p.insert(i);
            p.evict_candidate();
        }
        let s = p.state.lock();
        // Ghost sets should be bounded by MAX_GHOST_RATIO * live_len.
        let ghost_cap = (s.live_len() * MAX_GHOST_RATIO).max(MIN_GHOST_CAP);
        assert!(s.b1.len <= ghost_cap, "B1 too large: {}", s.b1.len);
        assert!(s.b2.len <= ghost_cap, "B2 too large: {}", s.b2.len);
    }
}
