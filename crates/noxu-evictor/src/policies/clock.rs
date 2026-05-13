//! Clock (Second-Chance) eviction policy.
//!
//! A circular buffer with per-page reference bits.  When a candidate is
//! inspected:
//!  - ref_bit == 1 → clear bit and give the page a second chance (advance hand)
//!  - ref_bit == 0 → evict
//!
//! This is the algorithm used by Postgres for its shared buffer manager
//! (`freelist`).

use crate::policy::EvictionPolicy;
use crate::slab::{SlabList, SENTINEL};
use noxu_sync::Mutex;
use hashbrown::HashMap;

#[derive(Debug)]
struct ClockState {
    list: SlabList,
    /// Per-node reference bits.
    ref_bits: HashMap<u64, bool>,
    /// The node_id currently under the clock hand.  `u64::MAX` when empty.
    hand: u64,
}

impl ClockState {
    fn new() -> Self {
        Self { list: SlabList::new(), ref_bits: HashMap::new(), hand: u64::MAX }
    }

    /// Add node at back with the given reference bit.
    fn add(&mut self, id: u64, hot: bool) {
        if self.list.contains(id) { return; }
        self.list.add_back(id);
        self.ref_bits.insert(id, hot);
        // Point hand at the first node if this is the first insertion.
        if self.hand == u64::MAX {
            self.hand = self.list.peek_front().unwrap_or(u64::MAX);
        }
    }

    /// Advance the hand to the successor of `current` (wrapping at tail).
    fn advance_hand(&mut self, current: u64) {
        let slot = self.list.slot_of(current);
        if slot == SENTINEL {
            // Node was already removed — jump to head.
            self.hand = self.list.peek_front().unwrap_or(u64::MAX);
            return;
        }
        // Follow `next`; wrap to head when at tail.
        let next_slot = self.list.slab[slot].as_ref().unwrap().next;
        if next_slot == SENTINEL {
            self.hand = self.list.peek_front().unwrap_or(u64::MAX);
        } else {
            self.hand = self.list.slab[next_slot].as_ref().unwrap().id;
        }
    }

    fn evict(&mut self) -> Option<u64> {
        if self.list.len == 0 { return None; }
        // Scan at most 2 × list length (one full pass clears all bits; second
        // pass evicts the first 0-bit node).
        let max_iters = self.list.len * 2 + 1;
        for _ in 0..max_iters {
            let candidate = self.hand;
            if candidate == u64::MAX { break; }
            let bit = *self.ref_bits.get(&candidate).unwrap_or(&false);
            if bit {
                // Give second chance: clear bit and advance.
                self.ref_bits.insert(candidate, false);
                self.advance_hand(candidate);
            } else {
                // Evict: advance hand before removing.
                self.advance_hand(candidate);
                self.list.remove(candidate);
                self.ref_bits.remove(&candidate);
                return Some(candidate);
            }
        }
        // Degenerate: all bits were set through two full passes — just evict
        // whatever the hand points at now.
        let candidate = self.hand;
        if candidate != u64::MAX {
            self.advance_hand(candidate);
            self.list.remove(candidate);
            self.ref_bits.remove(&candidate);
            Some(candidate)
        } else {
            None
        }
    }
}

/// Clock (Second-Chance) eviction policy.
#[derive(Debug)]
pub struct ClockPolicy {
    state: Mutex<ClockState>,
}

impl ClockPolicy {
    pub fn new() -> Self {
        Self { state: Mutex::new(ClockState::new()) }
    }
}

impl Default for ClockPolicy {
    fn default() -> Self { Self::new() }
}

impl EvictionPolicy for ClockPolicy {
    fn insert(&self, node_id: u64) {
        self.state.lock().add(node_id, true);
    }

    fn insert_cold(&self, node_id: u64) {
        self.state.lock().add(node_id, false);
    }

    fn touch(&self, node_id: u64) -> bool {
        let mut s = self.state.lock();
        if s.list.contains(node_id) {
            s.ref_bits.insert(node_id, true);
            true
        } else {
            false
        }
    }

    fn remove(&self, node_id: u64) -> bool {
        let mut s = self.state.lock();
        if s.hand == node_id {
            let id = node_id;
            s.advance_hand(id);
        }
        if s.list.remove(node_id) {
            s.ref_bits.remove(&node_id);
            true
        } else {
            false
        }
    }

    fn evict_candidate(&self) -> Option<u64> {
        self.state.lock().evict()
    }

    fn put_back(&self, node_id: u64) {
        // Re-insert at back with ref_bit = true (recently used).
        let mut s = self.state.lock();
        if !s.list.contains(node_id) {
            s.add(node_id, true);
        }
    }

    fn contains(&self, node_id: u64) -> bool {
        self.state.lock().list.contains(node_id)
    }

    fn len(&self) -> usize {
        self.state.lock().list.len
    }

    fn name(&self) -> &'static str { "Clock" }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::policy::EvictionPolicy;

    #[test]
    fn test_clock_cold_evicted_first() {
        let p = ClockPolicy::new();
        // Insert cold (ref_bit=0) then hot (ref_bit=1).
        p.insert_cold(1);
        p.insert(2);
        // 1 has ref_bit=0 → evicted first.
        assert_eq!(p.evict_candidate(), Some(1));
        assert_eq!(p.evict_candidate(), Some(2));
    }

    #[test]
    fn test_clock_second_chance() {
        let p = ClockPolicy::new();
        // All inserted hot (ref_bit=1).
        p.insert(1); p.insert(2); p.insert(3);
        // First evict_candidate: clears ref bits of all on first pass,
        // then evicts the first one (1) on second pass.
        let v = p.evict_candidate().unwrap();
        // After two full passes the hand starts at the first node (1).
        assert_eq!(v, 1);
        assert_eq!(p.len(), 2);
    }

    #[test]
    fn test_clock_touch_resets_bit() {
        let p = ClockPolicy::new();
        p.insert_cold(1); p.insert_cold(2); p.insert_cold(3);
        // Touch node 2 → ref_bit = 1.
        p.touch(2);
        // 1 has ref_bit=0 → evicted first.
        assert_eq!(p.evict_candidate(), Some(1));
        // 3 has ref_bit=0 → next.
        assert_eq!(p.evict_candidate(), Some(3));
        // 2 had ref_bit=1; after first pass it was cleared but no other
        // 0-bit candidate; second pass evicts 2.
        assert_eq!(p.evict_candidate(), Some(2));
    }

    #[test]
    fn test_clock_remove_hand_node() {
        let p = ClockPolicy::new();
        p.insert_cold(1); p.insert_cold(2); p.insert_cold(3);
        // Remove the current hand node (1 = head = cold end).
        assert!(p.remove(1));
        assert_eq!(p.len(), 2);
        assert_eq!(p.evict_candidate(), Some(2));
        assert_eq!(p.evict_candidate(), Some(3));
    }
}
