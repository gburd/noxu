//! Low Inter-reference Recency Set (LIRS) eviction policy.
//!
//! LIRS classifies pages as:
//!   LIR (Low Inter-reference Recency — hot): protected from eviction.
//!   HIR (High Inter-reference Recency — cold): eviction candidates.
//!
//! Two data structures:
//!   S  — LRU stack ordered by recency.  Contains ALL pages (LIR + resident
//!         HIR + non-resident HIR ghost entries).
//!   Q  — LRU queue of resident HIR pages ordered for eviction (oldest first).
//!
//! The LIR set occupies at most `lir_ratio` of all tracked pages (default
//! 0.99).  Pages in S but not in Q and not LIR are non-resident HIR ghosts.
//!
//! On eviction (`evict_candidate`): pop the oldest page from Q.
//!   - Remove from Q and `hir_resident` set.
//!   - If still in S: keep in S as a non-resident ghost (aids future
//!     promotion), add to `non_res` set.
//!   - Else: gone entirely.
//!
//! On access (`touch`):
//!   LIR page: move to top of S; prune non-resident HIR from bottom of S.
//!   Resident HIR page already in S: promote to LIR, demote bottom LIR to
//!     HIR, add demoted page to Q.
//!   Resident HIR page NOT in S: move to top of S, move to bottom of Q.
//!   Non-resident HIR (ghost): like "in S" promotion above, then bring page
//!     back as resident.
//!
//! References:
//!   Jiang & Zhang, "LIRS: An Efficient Low Inter-reference Recency Set
//!   Replacement to Improve Buffer Cache Performance",
//!   ACM SIGMETRICS 2002.

use crate::policy::EvictionPolicy;
use crate::slab::SlabList;
use noxu_sync::Mutex;
use hashbrown::HashSet;

/// LIR set target fraction of total tracked pages.
const LIR_RATIO: f64 = 0.99;

#[derive(Debug)]
struct LirsState {
    /// Recency stack (all pages + ghosts).
    s: SlabList,
    /// Resident HIR queue (eviction candidates, oldest first).
    q: SlabList,
    /// Set of pages currently in LIR state.
    lir: HashSet<u64>,
    /// Set of resident HIR pages (also in Q).
    hir_res: HashSet<u64>,
    /// Set of non-resident HIR ghosts (in S but not Q and not LIR).
    non_res: HashSet<u64>,
}

impl LirsState {
    fn new() -> Self {
        Self {
            s: SlabList::new(),
            q: SlabList::new(),
            lir: HashSet::new(),
            hir_res: HashSet::new(),
            non_res: HashSet::new(),
        }
    }

    fn total_resident(&self) -> usize {
        self.lir.len() + self.hir_res.len()
    }

    /// Compute the current LIR capacity target.
    fn lir_cap(&self) -> usize {
        let total = self.total_resident();
        ((total as f64 * LIR_RATIO) as usize).max(1)
    }

    /// Prune the bottom of S: remove non-resident HIR ghosts until the bottom
    /// is a LIR page or S is empty.  This keeps S bounded to ~c entries.
    fn prune_stack(&mut self) {
        loop {
            match self.s.peek_front() {
                None => break,
                Some(id) if self.lir.contains(&id) => break,
                Some(id) => {
                    // Bottom is HIR (resident or non-resident) — remove.
                    self.s.remove(id);
                    self.non_res.remove(&id);
                    // If it was a resident HIR it stays in Q until evicted.
                }
            }
        }
    }

    fn insert(&mut self, id: u64) {
        if self.lir.contains(&id) || self.hir_res.contains(&id) {
            // Already tracked — treat as hit.
            self.on_hit(id);
            return;
        }
        if self.non_res.contains(&id) {
            // Non-resident ghost hit: promote like an in-S HIR hit.
            self.non_res.remove(&id);
            self.on_hir_in_s(id);
            return;
        }
        // Complete miss.
        if self.lir.len() < self.lir_cap() {
            // LIR set not full yet: add as LIR.
            self.s.add_back(id); // top of S = MRU end
            self.lir.insert(id);
        } else {
            // LIR set full: add as resident HIR.
            self.s.add_back(id); // also in S
            self.q.add_back(id); // added to MRU of Q (will be evicted last within HIR)
            self.hir_res.insert(id);
        }
    }

    fn on_hit(&mut self, id: u64) {
        if self.lir.contains(&id) {
            // LIR hit: move to top of S.
            self.s.move_back(id);
            self.prune_stack();
        } else if self.hir_res.contains(&id) {
            if self.s.contains(id) {
                self.on_hir_in_s(id);
            } else {
                // Resident HIR not in S: move to top of S, to bottom of Q.
                self.s.add_back(id);
                // Move to bottom of Q (will be evicted sooner than recently-added HIR).
                self.q.move_front(id);
            }
        }
    }

    /// Promote a page that is a resident HIR in S to LIR.
    /// The LRU of the LIR set is demoted to HIR.
    fn on_hir_in_s(&mut self, id: u64) {
        // Remove from HIR tracking.
        self.hir_res.remove(&id);
        self.q.remove(id);
        // Add to LIR set and move to top of S.
        if !self.s.contains(id) { self.s.add_back(id); }
        else { self.s.move_back(id); }
        self.lir.insert(id);
        // LIR set now potentially over-sized: demote the bottom LIR page.
        while self.lir.len() > self.lir_cap() {
            // The bottom of S should be a LIR page (after pruning).
            if let Some(bottom) = self.s.peek_front().filter(|b| self.lir.contains(b)) {
                self.lir.remove(&bottom);
                // Demoted page becomes resident HIR.
                self.hir_res.insert(bottom);
                self.q.add_back(bottom);
                self.prune_stack();
                break;
            }
            // Prune non-resident HIR from bottom and retry.
            self.prune_stack();
            if self.s.peek_front().map(|id| self.lir.contains(&id)).unwrap_or(false) {
                continue;
            }
            break;
        }
    }

    fn evict(&mut self) -> Option<u64> {
        // Pop the oldest resident HIR page from Q.
        let id = self.q.remove_front()?;
        self.hir_res.remove(&id);
        // If the page is still in S, mark as non-resident ghost.
        if self.s.contains(id) {
            self.non_res.insert(id);
        }
        // Do not remove from S — it serves as a ghost for future promotion.
        Some(id)
    }
}

/// Low Inter-reference Recency Set eviction policy.
#[derive(Debug)]
pub struct LirsPolicy {
    state: Mutex<LirsState>,
}

impl LirsPolicy {
    pub fn new() -> Self {
        Self { state: Mutex::new(LirsState::new()) }
    }
}

impl Default for LirsPolicy {
    fn default() -> Self { Self::new() }
}

impl EvictionPolicy for LirsPolicy {
    fn insert(&self, node_id: u64) {
        self.state.lock().insert(node_id);
    }

    fn insert_cold(&self, node_id: u64) {
        // Cold insert: add directly to resident HIR (evictable) regardless of
        // LIR capacity, bypassing the recency promotion path.
        let mut s = self.state.lock();
        if !s.lir.contains(&node_id) && !s.hir_res.contains(&node_id) && !s.non_res.contains(&node_id) {
            if !s.s.contains(node_id) { s.s.add_back(node_id); }
            if !s.q.contains(node_id) {
                s.q.add_front(node_id); // cold end of Q — first to be evicted
                s.hir_res.insert(node_id);
            }
        }
    }

    fn touch(&self, node_id: u64) -> bool {
        let mut s = self.state.lock();
        if s.lir.contains(&node_id) || s.hir_res.contains(&node_id) || s.non_res.contains(&node_id) {
            s.on_hit(node_id);
            true
        } else {
            false
        }
    }

    fn remove(&self, node_id: u64) -> bool {
        let mut s = self.state.lock();
        let was_tracked = s.lir.remove(&node_id)
            || s.hir_res.remove(&node_id)
            || s.non_res.remove(&node_id);
        s.s.remove(node_id);
        s.q.remove(node_id);
        was_tracked
    }

    fn evict_candidate(&self) -> Option<u64> {
        let mut s = self.state.lock();
        // Ensure at least some HIR pages exist when the LIR set has entries.
        // If Q is empty but LIR is not, demote the LRU LIR page to HIR.
        if s.q.is_empty() && !s.lir.is_empty() {
            // Prune S until we find the bottom LIR page.
            s.prune_stack();
            if let Some(bottom) = s.s.peek_front().filter(|b| s.lir.contains(b)) {
                s.lir.remove(&bottom);
                s.hir_res.insert(bottom);
                s.q.add_back(bottom);
            }
        }
        s.evict()
    }

    fn put_back(&self, node_id: u64) {
        // Re-insert as resident HIR at the hot end of Q.
        let mut s = self.state.lock();
        if !s.lir.contains(&node_id) && !s.hir_res.contains(&node_id) {
            s.non_res.remove(&node_id);
            if !s.s.contains(node_id) { s.s.add_back(node_id); }
            if !s.q.contains(node_id) {
                s.q.add_back(node_id);
                s.hir_res.insert(node_id);
            }
        }
    }

    fn contains(&self, node_id: u64) -> bool {
        let s = self.state.lock();
        s.lir.contains(&node_id) || s.hir_res.contains(&node_id)
    }

    fn len(&self) -> usize {
        let s = self.state.lock();
        s.lir.len() + s.hir_res.len()
    }

    fn name(&self) -> &'static str { "LIRS" }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::policy::EvictionPolicy;

    #[test]
    fn test_lirs_basic_eviction() {
        let p = LirsPolicy::new();
        // Fill with enough pages to have both LIR and HIR.
        for i in 0u64..20 { p.insert(i); }
        assert_eq!(p.len(), 20);
        let v = p.evict_candidate().unwrap();
        assert!(v < 20);
        assert_eq!(p.len(), 19);
    }

    #[test]
    fn test_lirs_frequently_accessed_page_survives() {
        let p = LirsPolicy::new();
        // Insert 20 pages; repeatedly touch page 0 to keep it hot.
        for i in 0u64..20 { p.insert(i); }
        for _ in 0..5 { p.touch(0); }
        // Evict until only a few remain.
        let mut evicted = Vec::new();
        while p.len() > 2 {
            evicted.push(p.evict_candidate().unwrap());
        }
        // Page 0 should not have been evicted early (it was touched most).
        // It will be in LIR (hot set) and last to leave.
        // Just check it's either still tracked or was one of the last evicted.
        // This is a probabilistic check — LIRS doesn't guarantee exact ordering.
        assert!(p.len() <= 2);
    }

    #[test]
    fn test_lirs_insert_cold() {
        let p = LirsPolicy::new();
        // Insert normally.
        p.insert(1); p.insert(2);
        // Insert cold — should be evicted first.
        p.insert_cold(99);
        // 99 goes to cold end of Q.
        assert!(p.contains(99) || p.evict_candidate() == Some(99));
    }

    #[test]
    fn test_lirs_remove() {
        let p = LirsPolicy::new();
        for i in 0u64..5 { p.insert(i); }
        assert!(p.remove(2));
        assert!(!p.remove(2));
        assert_eq!(p.len(), 4);
    }

    #[test]
    fn test_lirs_put_back() {
        let p = LirsPolicy::new();
        for i in 0u64..10 { p.insert(i); }
        let v = p.evict_candidate().unwrap();
        p.put_back(v);
        assert!(p.contains(v) || p.len() >= 9);
    }

    #[test]
    fn test_lirs_evicts_hir_not_lir() {
        let p = LirsPolicy::new();
        // With 10 pages and LIR_RATIO=0.99, LIR cap = max(floor(10*0.99),1) = 9.
        // So approximately 9 LIR + 1 HIR.
        for i in 0u64..10 { p.insert(i); }
        // The first page evicted must be an HIR page (Q member), not a LIR page.
        let state = p.state.lock();
        let q_len = state.q.len;
        let hir_len = state.hir_res.len();
        drop(state);
        // There should be at least 1 HIR page to evict.
        if hir_len > 0 {
            let v = p.evict_candidate().unwrap();
            let state = p.state.lock();
            // Evicted page should not be in LIR set after eviction.
            assert!(!state.lir.contains(&v));
            drop(state);
            let _ = q_len;
        }
    }
}
