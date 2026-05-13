//! Clock with Adaptive Replacement (CAR) eviction policy.
//!
//! CAR combines ARC's adaptive two-pool idea with Clock's O(1) reference-bit
//! mechanism:
//!   T1 — single-touch pages (Clock list with ref bits)
//!   T2 — multi-touch / frequent pages (Clock list with ref bits)
//!   B1 — ghost entries for T1 evictions (no ref bits needed)
//!   B2 — ghost entries for T2 evictions
//!
//! Replacement (called on eviction):
//!   Loop until a victim is found:
//!     - If T1.hand.ref_bit == 1: clear bit, move page from T1 to T2,
//!       advance T1 hand.
//!     - Else if T1.len > p: evict T1.hand, add to B1, advance T1 hand.
//!     - Else (T1.hand.ref_bit == 0, T1.len <= p): inspect T2.
//!       If T2.hand.ref_bit == 1: clear bit, advance T2 hand.
//!       Else: evict T2.hand, add to B2, advance T2 hand.
//!
//! References:
//!   Bansal & Modha, "CAR: Clock with Adaptive Replacement",
//!   USENIX FAST 2004.

use crate::policy::EvictionPolicy;
use crate::slab::{SlabList, SENTINEL};
use noxu_sync::Mutex;
use hashbrown::HashMap;

const MAX_GHOST_RATIO: usize = 2;
const MIN_GHOST_CAP: usize = 64;

// ---------------------------------------------------------------------------
// Clock list helper — a SlabList where each node additionally stores a
// reference bit, plus a "hand" node-id tracking the current position.
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct ClockList {
    list: SlabList,
    ref_bits: HashMap<u64, bool>,
    hand: u64, // u64::MAX when empty
}

impl ClockList {
    fn new() -> Self {
        Self { list: SlabList::new(), ref_bits: HashMap::new(), hand: u64::MAX }
    }

    fn add(&mut self, id: u64, hot: bool) {
        if self.list.contains(id) { return; }
        self.list.add_back(id);
        self.ref_bits.insert(id, hot);
        if self.hand == u64::MAX {
            self.hand = self.list.peek_front().unwrap_or(u64::MAX);
        }
    }

    /// True if the hand's node has a set reference bit.
    fn hand_ref(&self) -> bool {
        if self.hand == u64::MAX { return false; }
        *self.ref_bits.get(&self.hand).unwrap_or(&false)
    }

    /// Clear the hand's reference bit without evicting.
    fn clear_hand_ref(&mut self) {
        if self.hand != u64::MAX {
            self.ref_bits.insert(self.hand, false);
        }
    }

    /// Evict the node at the hand position and advance the hand.
    /// Returns the evicted node_id, or None if empty.
    fn evict_hand(&mut self) -> Option<u64> {
        let id = self.hand;
        if id == u64::MAX { return None; }
        self.advance(id);
        self.list.remove(id);
        self.ref_bits.remove(&id);
        Some(id)
    }

    /// Advance hand past `current` (wrap at tail).
    fn advance(&mut self, current: u64) {
        let slot = self.list.slot_of(current);
        if slot == SENTINEL {
            self.hand = self.list.peek_front().unwrap_or(u64::MAX);
            return;
        }
        let next_slot = self.list.slab[slot].as_ref().unwrap().next;
        if next_slot == SENTINEL {
            self.hand = self.list.peek_front().unwrap_or(u64::MAX);
        } else {
            self.hand = self.list.slab[next_slot].as_ref().unwrap().id;
        }
    }

    /// Move the hand's node from this list into `other`, setting its ref bit
    /// to `hot`.  Returns the moved node_id, or None if empty.
    fn move_hand_to(&mut self, other: &mut ClockList, hot: bool) -> Option<u64> {
        let id = self.hand;
        if id == u64::MAX { return None; }
        self.advance(id);
        self.list.remove(id);
        self.ref_bits.remove(&id);
        other.add(id, hot);
        Some(id)
    }

    fn set_ref(&mut self, id: u64, bit: bool) -> bool {
        if self.list.contains(id) {
            self.ref_bits.insert(id, bit);
            true
        } else {
            false
        }
    }

    fn remove(&mut self, id: u64) -> bool {
        if self.hand == id { self.advance(id); }
        if self.list.remove(id) {
            self.ref_bits.remove(&id);
            true
        } else {
            false
        }
    }

    fn contains(&self, id: u64) -> bool { self.list.contains(id) }
    fn len(&self) -> usize { self.list.len }
    fn peek_front(&self) -> Option<u64> { self.list.peek_front() }
}

// ---------------------------------------------------------------------------
// CAR state
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct CarState {
    t1: ClockList,
    t2: ClockList,
    b1: SlabList, // ghost — no ref bits needed
    b2: SlabList,
    p: f64,
}

impl CarState {
    fn new() -> Self {
        Self { t1: ClockList::new(), t2: ClockList::new(), b1: SlabList::new(), b2: SlabList::new(), p: 0.0 }
    }

    fn live_len(&self) -> usize { self.t1.len() + self.t2.len() }

    fn ghost_cap(&self) -> usize {
        (self.live_len() * MAX_GHOST_RATIO).max(MIN_GHOST_CAP)
    }

    fn trim_ghost(b: &mut SlabList, cap: usize) {
        while b.len > cap { b.remove_front(); }
    }

    /// Core replacement: find a victim page.
    fn replace(&mut self) -> Option<u64> {
        if self.t1.len() == 0 && self.t2.len() == 0 { return None; }
        let max_iters = (self.t1.len() + self.t2.len()) * 4 + 2;
        for _ in 0..max_iters {
            // --- Inspect T1 hand ---
            if self.t1.len() > 0 {
                if self.t1.hand_ref() {
                    // Reference bit set: move page to T2 with ref_bit = false.
                    self.t1.clear_hand_ref();
                    let moved = self.t1.hand;
                    // advance hand first, then move
                    let cur = moved;
                    self.t1.advance(cur);
                    self.t1.list.remove(moved);
                    self.t1.ref_bits.remove(&moved);
                    self.t2.add(moved, false);
                } else {
                    // ref_bit == 0
                    let t1_len = self.t1.len() as f64;
                    if t1_len > self.p || self.t2.len() == 0 {
                        // Evict from T1.
                        let id = self.t1.evict_hand().unwrap();
                        // Defensive: skip duplicate ghost add (can occur
                        // when replace() is re-entered during ghost-hit
                        // adaptation in insert()).
                        if !self.b1.contains(id) { self.b1.add_back(id); }
                        let cap = self.ghost_cap();
                        Self::trim_ghost(&mut self.b1, cap);
                        return Some(id);
                    } else {
                        // Fall through to T2.
                    }
                }
            }

            // --- Inspect T2 hand ---
            if self.t2.len() > 0 {
                if self.t2.hand_ref() {
                    // Give second chance.
                    self.t2.clear_hand_ref();
                    let cur = self.t2.hand;
                    self.t2.advance(cur);
                } else {
                    // Evict from T2.
                    let id = self.t2.evict_hand().unwrap();
                    if !self.b2.contains(id) { self.b2.add_back(id); }
                    let cap = self.ghost_cap();
                    Self::trim_ghost(&mut self.b2, cap);
                    return Some(id);
                }
            }

            if self.t1.len() == 0 && self.t2.len() == 0 { break; }
        }
        // Fallback: evict whatever is at the front of T1 or T2.
        if let Some(id) = self.t1.evict_hand() { return Some(id); }
        self.t2.evict_hand()
    }

    fn insert(&mut self, id: u64) {
        if self.t1.contains(id) || self.t2.contains(id) {
            self.on_hit(id);
            return;
        }
        if self.b1.contains(id) {
            // Ghost hit in B1.
            let b1 = self.b1.len as f64;
            let b2 = self.b2.len as f64;
            let delta = if b1 >= b2 { 1.0 } else { b2 / b1.max(1.0) };
            self.p = (self.p + delta).min((self.t1.len() + self.t2.len() + 1) as f64);
            self.replace();
            self.b1.remove(id);
            self.t2.add(id, false);
            return;
        }
        if self.b2.contains(id) {
            // Ghost hit in B2.
            let b1 = self.b1.len as f64;
            let b2 = self.b2.len as f64;
            let delta = if b2 >= b1 { 1.0 } else { b1 / b2.max(1.0) };
            self.p = (self.p - delta).max(0.0);
            self.replace();
            self.b2.remove(id);
            self.t2.add(id, false);
            return;
        }
        // Complete miss → add to T1.
        self.t1.add(id, false);
    }

    fn on_hit(&mut self, id: u64) {
        if self.t1.set_ref(id, true) { /* ref bit set in T1 */ }
        else { self.t2.set_ref(id, true); /* ref bit set in T2 */ }
    }
}

// ---------------------------------------------------------------------------
// Public wrapper
// ---------------------------------------------------------------------------

/// Clock with Adaptive Replacement eviction policy.
#[derive(Debug)]
pub struct CarPolicy {
    state: Mutex<CarState>,
}

impl CarPolicy {
    pub fn new() -> Self {
        Self { state: Mutex::new(CarState::new()) }
    }
}

impl Default for CarPolicy {
    fn default() -> Self { Self::new() }
}

impl EvictionPolicy for CarPolicy {
    fn insert(&self, node_id: u64) {
        self.state.lock().insert(node_id);
    }

    fn insert_cold(&self, node_id: u64) {
        let mut s = self.state.lock();
        if !s.t1.contains(node_id) && !s.t2.contains(node_id) {
            if s.b1.contains(node_id) || s.b2.contains(node_id) {
                s.insert(node_id);
            } else {
                // Add to T1 at cold end (front of circular list).
                s.t1.list.add_front(node_id);
                s.t1.ref_bits.insert(node_id, false);
                if s.t1.hand == u64::MAX {
                    s.t1.hand = node_id;
                }
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
        if s.t1.remove(node_id) { return true; }
        if s.t2.remove(node_id) { return true; }
        if s.b1.remove(node_id) { return true; }
        s.b2.remove(node_id)
    }

    fn evict_candidate(&self) -> Option<u64> {
        self.state.lock().replace()
    }

    fn put_back(&self, node_id: u64) {
        let mut s = self.state.lock();
        if !s.t1.contains(node_id) && !s.t2.contains(node_id) {
            s.t2.add(node_id, true); // re-insert as frequently-used
        }
    }

    fn contains(&self, node_id: u64) -> bool {
        let s = self.state.lock();
        s.t1.contains(node_id) || s.t2.contains(node_id)
    }

    fn len(&self) -> usize {
        let s = self.state.lock();
        s.t1.len() + s.t2.len()
    }

    fn name(&self) -> &'static str { "CAR" }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::policy::EvictionPolicy;

    #[test]
    fn test_car_evicts_all() {
        let p = CarPolicy::new();
        p.insert(1); p.insert(2); p.insert(3);
        let mut evicted = Vec::new();
        for _ in 0..3 {
            evicted.push(p.evict_candidate().unwrap());
        }
        evicted.sort();
        assert_eq!(evicted, vec![1, 2, 3]);
        assert_eq!(p.evict_candidate(), None);
    }

    #[test]
    fn test_car_touch_promotes() {
        let p = CarPolicy::new();
        p.insert(1); p.insert(2); p.insert(3);
        // Touch 1 twice to set ref_bit.
        p.touch(1); p.touch(1);
        assert_eq!(p.len(), 3);
    }

    #[test]
    fn test_car_remove() {
        let p = CarPolicy::new();
        p.insert(1); p.insert(2); p.insert(3);
        assert!(p.remove(2));
        assert!(!p.remove(2));
        assert_eq!(p.len(), 2);
    }

    #[test]
    fn test_car_ghost_bounded() {
        let p = CarPolicy::new();
        for i in 0u64..200 {
            p.insert(i);
            p.evict_candidate();
        }
        let s = p.state.lock();
        let ghost_cap = (s.live_len() * MAX_GHOST_RATIO).max(MIN_GHOST_CAP);
        assert!(s.b1.len <= ghost_cap);
        assert!(s.b2.len <= ghost_cap);
    }
}
