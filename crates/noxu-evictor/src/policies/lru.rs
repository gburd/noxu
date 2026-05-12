//! LRU eviction policy — slab doubly-linked list with O(1) operations.

use crate::policy::EvictionPolicy;
use crate::slab::SlabList;
use noxu_sync::Mutex;

/// Least-Recently-Used eviction policy.
///
/// Backed by a slab-allocated intrusive doubly-linked list + HashMap index.
/// All operations are O(1) amortised.
#[derive(Debug)]
pub struct LruPolicy {
    state: Mutex<SlabList>,
}

impl LruPolicy {
    pub fn new() -> Self {
        Self { state: Mutex::new(SlabList::new()) }
    }
}

impl Default for LruPolicy {
    fn default() -> Self { Self::new() }
}

impl EvictionPolicy for LruPolicy {
    fn insert(&self, node_id: u64) {
        let mut s = self.state.lock();
        if !s.contains(node_id) { s.add_back(node_id); }
    }

    fn insert_cold(&self, node_id: u64) {
        let mut s = self.state.lock();
        if !s.contains(node_id) { s.add_front(node_id); }
    }

    fn touch(&self, node_id: u64) -> bool {
        self.state.lock().move_back(node_id)
    }

    fn remove(&self, node_id: u64) -> bool {
        self.state.lock().remove(node_id)
    }

    fn evict_candidate(&self) -> Option<u64> {
        self.state.lock().remove_front()
    }

    fn put_back(&self, node_id: u64) {
        let mut s = self.state.lock();
        if !s.contains(node_id) { s.add_back(node_id); }
    }

    fn contains(&self, node_id: u64) -> bool {
        self.state.lock().contains(node_id)
    }

    fn len(&self) -> usize {
        self.state.lock().len
    }

    fn name(&self) -> &'static str { "LRU" }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::policy::EvictionPolicy;

    #[test]
    fn test_lru_basic_order() {
        let p = LruPolicy::new();
        p.insert(1); p.insert(2); p.insert(3);
        assert_eq!(p.evict_candidate(), Some(1));
        assert_eq!(p.evict_candidate(), Some(2));
        assert_eq!(p.evict_candidate(), Some(3));
        assert_eq!(p.evict_candidate(), None);
    }

    #[test]
    fn test_lru_touch_promotes() {
        let p = LruPolicy::new();
        p.insert(1); p.insert(2); p.insert(3);
        p.touch(1);
        assert_eq!(p.evict_candidate(), Some(2));
        assert_eq!(p.evict_candidate(), Some(3));
        assert_eq!(p.evict_candidate(), Some(1));
    }

    #[test]
    fn test_lru_insert_cold() {
        let p = LruPolicy::new();
        p.insert(1); p.insert(2);
        p.insert_cold(0);
        assert_eq!(p.evict_candidate(), Some(0));
        assert_eq!(p.evict_candidate(), Some(1));
    }

    #[test]
    fn test_lru_put_back() {
        let p = LruPolicy::new();
        p.insert(1); p.insert(2);
        let v = p.evict_candidate().unwrap();
        p.put_back(v);
        // put_back re-inserts at hot end
        assert_eq!(p.evict_candidate(), Some(2));
        assert_eq!(p.evict_candidate(), Some(1));
    }

    #[test]
    fn test_lru_remove() {
        let p = LruPolicy::new();
        p.insert(1); p.insert(2); p.insert(3);
        assert!(p.remove(2));
        assert!(!p.remove(2));
        assert_eq!(p.len(), 2);
        assert_eq!(p.evict_candidate(), Some(1));
        assert_eq!(p.evict_candidate(), Some(3));
    }
}
