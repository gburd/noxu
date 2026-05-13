//! LRU list tracking for eviction candidates.
//!

use noxu_sync::Mutex;
use hashbrown::HashMap;

// Sentinel index value used as the list head/tail marker.
const SENTINEL: usize = usize::MAX;

/// A node in the intrusive doubly-linked list.
#[derive(Debug, Clone)]
struct Node {
    /// The node ID stored at this slot.
    id: u64,
    /// Index of the previous node (SENTINEL = no previous node).
    prev: usize,
    /// Index of the next node (SENTINEL = no next node).
    next: usize,
}

/// A single LRU list tracking node IDs.
///
/// Implemented as a slab-allocated intrusive doubly-linked list.
/// - A slab (Vec<Option<Node>>) provides stable slot indices.
/// - A free list (Vec<usize>) tracks recycled slots.
/// - A HashMap<u64, usize> maps node_id -> slab slot for O(1) lookup.
/// - head/tail are slab indices pointing to the cold (LRU) and hot (MRU) ends.
///
/// All operations -- insert, remove, move -- are O(1) amortized.
///
/// Convention: front = cold end (LRU, evicted first); back = hot end (MRU).
///
/// The LRU list data structures from Evictor.
#[derive(Debug)]
struct LruListImpl {
    /// Slab of nodes; None means the slot is free.
    slab: Vec<Option<Node>>,
    /// Free slots available for reuse.
    free: Vec<usize>,
    /// Map from node_id to its slab index.
    index: HashMap<u64, usize>,
    /// Slab index of the cold/front (LRU) end; SENTINEL when empty.
    head: usize,
    /// Slab index of the hot/back (MRU) end; SENTINEL when empty.
    tail: usize,
    /// Number of live nodes in the list.
    len: usize,
}

impl LruListImpl {
    fn new() -> Self {
        Self {
            slab: Vec::new(),
            free: Vec::new(),
            index: HashMap::new(),
            head: SENTINEL,
            tail: SENTINEL,
            len: 0,
        }
    }

    /// Allocate a new slab slot and return its index.
    fn alloc_slot(&mut self, node_id: u64) -> usize {
        if let Some(slot) = self.free.pop() {
            self.slab[slot] = Some(Node { id: node_id, prev: SENTINEL, next: SENTINEL });
            slot
        } else {
            let slot = self.slab.len();
            self.slab.push(Some(Node { id: node_id, prev: SENTINEL, next: SENTINEL }));
            slot
        }
    }

    /// Free a slab slot.
    fn free_slot(&mut self, slot: usize) {
        self.slab[slot] = None;
        self.free.push(slot);
    }

    /// Unlink a slot from the doubly-linked list without freeing it.
    fn unlink(&mut self, slot: usize) {
        let (prev, next) = {
            let node = self.slab[slot].as_ref().unwrap();
            (node.prev, node.next)
        };
        if prev == SENTINEL {
            self.head = next;
        } else {
            self.slab[prev].as_mut().unwrap().next = next;
        }
        if next == SENTINEL {
            self.tail = prev;
        } else {
            self.slab[next].as_mut().unwrap().prev = prev;
        }
    }

    /// Link a slot at the back (hot/MRU end) of the list.
    fn link_back(&mut self, slot: usize) {
        let old_tail = self.tail;
        {
            let node = self.slab[slot].as_mut().unwrap();
            node.prev = old_tail;
            node.next = SENTINEL;
        }
        if old_tail == SENTINEL {
            self.head = slot;
        } else {
            self.slab[old_tail].as_mut().unwrap().next = slot;
        }
        self.tail = slot;
    }

    /// Link a slot at the front (cold/LRU end) of the list.
    fn link_front(&mut self, slot: usize) {
        let old_head = self.head;
        {
            let node = self.slab[slot].as_mut().unwrap();
            node.prev = SENTINEL;
            node.next = old_head;
        }
        if old_head == SENTINEL {
            self.tail = slot;
        } else {
            self.slab[old_head].as_mut().unwrap().prev = slot;
        }
        self.head = slot;
    }

    /// Add a node to the back (hot end, MRU) of the list. O(1).
    fn add_back(&mut self, node_id: u64) {
        debug_assert!(!self.index.contains_key(&node_id));
        let slot = self.alloc_slot(node_id);
        self.link_back(slot);
        self.index.insert(node_id, slot);
        self.len += 1;
    }

    /// Add a node to the front (cold end, LRU) of the list. O(1).
    fn add_front(&mut self, node_id: u64) {
        debug_assert!(!self.index.contains_key(&node_id));
        let slot = self.alloc_slot(node_id);
        self.link_front(slot);
        self.index.insert(node_id, slot);
        self.len += 1;
    }

    /// Move a node to the back (hot end, MRU) of the list. O(1).
    fn move_back(&mut self, node_id: u64) -> bool {
        if let Some(&slot) = self.index.get(&node_id) {
            if slot == self.tail {
                return true; // Already at back.
            }
            self.unlink(slot);
            self.link_back(slot);
            true
        } else {
            false
        }
    }

    /// Move a node to the front (cold end, LRU) of the list. O(1).
    fn move_front(&mut self, node_id: u64) -> bool {
        if let Some(&slot) = self.index.get(&node_id) {
            if slot == self.head {
                return true; // Already at front.
            }
            self.unlink(slot);
            self.link_front(slot);
            true
        } else {
            false
        }
    }

    /// Remove and return the node at the front (cold end, LRU) of the list. O(1).
    fn remove_front(&mut self) -> Option<u64> {
        if self.head == SENTINEL {
            return None;
        }
        let slot = self.head;
        let node_id = self.slab[slot].as_ref().unwrap().id;
        self.unlink(slot);
        self.free_slot(slot);
        self.index.remove(&node_id);
        self.len -= 1;
        Some(node_id)
    }

    /// Remove a specific node from the list. O(1).
    fn remove(&mut self, node_id: u64) -> bool {
        if let Some(slot) = self.index.remove(&node_id) {
            self.unlink(slot);
            self.free_slot(slot);
            self.len -= 1;
            true
        } else {
            false
        }
    }

    /// Check if a node is in the list. O(1).
    fn contains(&self, node_id: u64) -> bool {
        self.index.contains_key(&node_id)
    }

    /// Get the number of nodes in the list. O(1).
    fn len(&self) -> usize {
        self.len
    }
}

/// A two-priority LRU list system.
///
/// uses two LRU lists:
/// - Priority 1 (mixed): contains clean and dirty nodes when no off-heap cache
/// - Priority 2 (dirty): contains dirty nodes that should be evicted last
///
/// This structure manages both lists.
///
/// Dual LRU list system.
#[derive(Debug)]
pub struct LruList {
    /// Priority-1 LRU list (mixed or normal).
    pri1: Mutex<LruListImpl>,
    /// Priority-2 LRU list (dirty or level-2).
    pri2: Mutex<LruListImpl>,
}

impl LruList {
    /// Create a new dual-priority LRU list.
    pub fn new() -> Self {
        Self {
            pri1: Mutex::new(LruListImpl::new()),
            pri2: Mutex::new(LruListImpl::new()),
        }
    }

    /// Add a node to the back of the priority-1 list (hot end).
    pub fn add_back(&self, node_id: u64) {
        self.pri1.lock().add_back(node_id);
    }

    /// Add a node to the front of the priority-1 list (cold end).
    pub fn add_front(&self, node_id: u64) {
        self.pri1.lock().add_front(node_id);
    }

    /// Add a node to the back of the priority-2 list (hot end).
    pub fn pri2_add_back(&self, node_id: u64) {
        self.pri2.lock().add_back(node_id);
    }

    /// Add a node to the front of the priority-2 list (cold end).
    pub fn pri2_add_front(&self, node_id: u64) {
        self.pri2.lock().add_front(node_id);
    }

    /// Move a node to the back of the priority-1 list.
    pub fn move_back(&self, node_id: u64) -> bool {
        self.pri1.lock().move_back(node_id)
    }

    /// Move a node to the front of the priority-1 list.
    pub fn move_front(&self, node_id: u64) -> bool {
        self.pri1.lock().move_front(node_id)
    }

    /// Move a node to the back of the priority-2 list.
    pub fn pri2_move_back(&self, node_id: u64) -> bool {
        self.pri2.lock().move_back(node_id)
    }

    /// Remove and return a node from the front of the priority-1 list.
    pub fn remove_front(&self) -> Option<u64> {
        self.pri1.lock().remove_front()
    }

    /// Remove and return a node from the front of the priority-2 list.
    pub fn pri2_remove_front(&self) -> Option<u64> {
        self.pri2.lock().remove_front()
    }

    /// Remove a node from the priority-1 list.
    pub fn remove(&self, node_id: u64) -> bool {
        self.pri1.lock().remove(node_id)
    }

    /// Remove a node from the priority-2 list.
    pub fn pri2_remove(&self, node_id: u64) -> bool {
        self.pri2.lock().remove(node_id)
    }

    /// Remove a node from either list (tries both).
    pub fn remove_from_either(&self, node_id: u64) -> bool {
        self.remove(node_id) || self.pri2_remove(node_id)
    }

    /// Check if a node is in the priority-1 list.
    pub fn contains(&self, node_id: u64) -> bool {
        self.pri1.lock().contains(node_id)
    }

    /// Check if a node is in the priority-2 list.
    pub fn pri2_contains(&self, node_id: u64) -> bool {
        self.pri2.lock().contains(node_id)
    }

    /// Get the size of the priority-1 list.
    pub fn len(&self) -> usize {
        self.pri1.lock().len()
    }

    /// Get the size of the priority-2 list.
    pub fn pri2_len(&self) -> usize {
        self.pri2.lock().len()
    }

    /// Get the total size of both lists.
    pub fn total_len(&self) -> usize {
        self.len() + self.pri2_len()
    }

    /// Check if both lists are empty.
    pub fn is_empty(&self) -> bool {
        self.total_len() == 0
    }

    // ------------------------------------------------------------------
    // Convenience API (single-priority view of the priority-1 list).
    // ------------------------------------------------------------------

    /// Insert a node at the MRU (hot) end of the priority-1 list. O(1).
    ///
    /// Equivalent to `add_back`.
    pub fn insert(&self, node_id: u64) {
        self.pri1.lock().add_back(node_id);
    }

    /// Remove the LRU (cold) node from the priority-1 list and return it. O(1).
    ///
    /// Equivalent to `remove_front`.
    pub fn pop_lru(&self) -> Option<u64> {
        self.pri1.lock().remove_front()
    }

    /// Move a node to the MRU (hot) end of the priority-1 list. O(1).
    ///
    /// Marks the node as recently used. Returns false if the node is not
    /// in the priority-1 list.
    pub fn touch(&self, node_id: u64) -> bool {
        self.pri1.lock().move_back(node_id)
    }
}

impl Default for LruList {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lru_list_impl_basic() {
        let mut list = LruListImpl::new();
        assert_eq!(list.len(), 0);

        list.add_back(1);
        list.add_back(2);
        list.add_back(3);

        assert_eq!(list.len(), 3);
        assert!(list.contains(1));
        assert!(list.contains(2));
        assert!(list.contains(3));
        assert!(!list.contains(4));
    }

    #[test]
    fn test_lru_list_impl_remove_front() {
        let mut list = LruListImpl::new();
        list.add_back(1);
        list.add_back(2);
        list.add_back(3);

        assert_eq!(list.remove_front(), Some(1));
        assert_eq!(list.len(), 2);
        assert!(!list.contains(1));

        assert_eq!(list.remove_front(), Some(2));
        assert_eq!(list.remove_front(), Some(3));
        assert_eq!(list.remove_front(), None);
    }

    #[test]
    fn test_lru_list_impl_add_front() {
        let mut list = LruListImpl::new();
        list.add_front(1);
        list.add_front(2);
        list.add_front(3);

        assert_eq!(list.remove_front(), Some(3));
        assert_eq!(list.remove_front(), Some(2));
        assert_eq!(list.remove_front(), Some(1));
    }

    #[test]
    fn test_lru_list_impl_move_back() {
        let mut list = LruListImpl::new();
        list.add_back(1);
        list.add_back(2);
        list.add_back(3);

        assert!(list.move_back(1));
        assert_eq!(list.remove_front(), Some(2));
        assert_eq!(list.remove_front(), Some(3));
        assert_eq!(list.remove_front(), Some(1));
    }

    #[test]
    fn test_lru_list_impl_move_front() {
        let mut list = LruListImpl::new();
        list.add_back(1);
        list.add_back(2);
        list.add_back(3);

        assert!(list.move_front(3));
        assert_eq!(list.remove_front(), Some(3));
        assert_eq!(list.remove_front(), Some(1));
        assert_eq!(list.remove_front(), Some(2));
    }

    #[test]
    fn test_lru_list_impl_remove() {
        let mut list = LruListImpl::new();
        list.add_back(1);
        list.add_back(2);
        list.add_back(3);

        assert!(list.remove(2));
        assert_eq!(list.len(), 2);
        assert!(!list.contains(2));
        assert!(list.contains(1));
        assert!(list.contains(3));

        assert!(!list.remove(2)); // Already removed
    }

    #[test]
    fn test_lru_list_dual_priority() {
        let lru = LruList::new();
        assert_eq!(lru.len(), 0);
        assert_eq!(lru.pri2_len(), 0);
        assert!(lru.is_empty());

        lru.add_back(1);
        lru.add_back(2);
        lru.pri2_add_back(3);

        assert_eq!(lru.len(), 2);
        assert_eq!(lru.pri2_len(), 1);
        assert_eq!(lru.total_len(), 3);
        assert!(!lru.is_empty());

        assert!(lru.contains(1));
        assert!(lru.contains(2));
        assert!(!lru.contains(3));
        assert!(lru.pri2_contains(3));
    }

    #[test]
    fn test_lru_list_remove_from_either() {
        let lru = LruList::new();
        lru.add_back(1);
        lru.pri2_add_back(2);

        assert!(lru.remove_from_either(1));
        assert!(lru.remove_from_either(2));
        assert!(!lru.remove_from_either(3));
        assert!(lru.is_empty());
    }

    #[test]
    fn test_lru_list_thread_safety() {
        use std::sync::Arc;
        use std::thread;

        let lru = Arc::new(LruList::new());
        let mut handles = vec![];

        for i in 0..10 {
            let lru_clone = Arc::clone(&lru);
            handles.push(thread::spawn(move || {
                lru_clone.add_back(i);
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(lru.len(), 10);
    }

    // ------------------------------------------------------------------
    // Tests for the insert / pop_lru / touch / remove O(1) API.
    // ------------------------------------------------------------------

    #[test]
    fn test_insert_pop_lru_order() {
        let lru = LruList::new();

        lru.insert(10);
        lru.insert(20);
        lru.insert(30);

        assert_eq!(lru.len(), 3);

        // pop_lru should return them in insertion (LRU-first) order.
        assert_eq!(lru.pop_lru(), Some(10));
        assert_eq!(lru.pop_lru(), Some(20));
        assert_eq!(lru.pop_lru(), Some(30));
        assert_eq!(lru.pop_lru(), None);
        assert!(lru.is_empty());
    }

    #[test]
    fn test_touch_moves_to_mru() {
        let lru = LruList::new();

        lru.insert(1);
        lru.insert(2);
        lru.insert(3);

        // Touch node 1: it was LRU, should now be MRU.
        assert!(lru.touch(1));

        // LRU order is now: 2, 3, 1.
        assert_eq!(lru.pop_lru(), Some(2));
        assert_eq!(lru.pop_lru(), Some(3));
        assert_eq!(lru.pop_lru(), Some(1));
    }

    #[test]
    fn test_touch_already_mru() {
        let lru = LruList::new();

        lru.insert(1);
        lru.insert(2);

        // Touch the already-MRU node.
        assert!(lru.touch(2));

        // Order unchanged.
        assert_eq!(lru.pop_lru(), Some(1));
        assert_eq!(lru.pop_lru(), Some(2));
    }

    #[test]
    fn test_touch_nonexistent() {
        let lru = LruList::new();
        lru.insert(1);

        // Touching a node that is not in the list returns false.
        assert!(!lru.touch(99));
        assert_eq!(lru.len(), 1);
    }

    #[test]
    fn test_remove_arbitrary_node() {
        let lru = LruList::new();

        lru.insert(10);
        lru.insert(20);
        lru.insert(30);

        // Remove the middle node.
        assert!(lru.remove(20));
        assert_eq!(lru.len(), 2);
        assert!(!lru.contains(20));

        // Removing again returns false.
        assert!(!lru.remove(20));

        // Remaining order preserved.
        assert_eq!(lru.pop_lru(), Some(10));
        assert_eq!(lru.pop_lru(), Some(30));
    }

    #[test]
    fn test_insert_many_pop_lru_all() {
        let lru = LruList::new();
        let n = 100u64;

        for i in 0..n {
            lru.insert(i);
        }
        assert_eq!(lru.len(), n as usize);

        for i in 0..n {
            assert_eq!(lru.pop_lru(), Some(i));
        }
        assert_eq!(lru.pop_lru(), None);
        assert!(lru.is_empty());
    }

    #[test]
    fn test_slot_reuse_after_remove() {
        // Verify that after removes the slab slots are reused (slab does not
        // grow unboundedly with repeated insert/remove cycles).
        let mut inner = LruListImpl::new();

        inner.add_back(1);
        inner.remove(1);
        let slab_len_after_one_cycle = inner.slab.len();

        // Next insert should reuse the freed slot.
        inner.add_back(2);
        assert_eq!(inner.slab.len(), slab_len_after_one_cycle);
        assert!(inner.contains(2));
    }

    #[test]
    fn test_impl_move_back_already_back() {
        let mut list = LruListImpl::new();
        list.add_back(1);
        list.add_back(2);

        // 2 is already at the back; move_back should be a no-op.
        assert!(list.move_back(2));
        assert_eq!(list.remove_front(), Some(1));
        assert_eq!(list.remove_front(), Some(2));
    }

    #[test]
    fn test_impl_move_front_already_front() {
        let mut list = LruListImpl::new();
        list.add_back(1);
        list.add_back(2);

        // 1 is already at the front; move_front should be a no-op.
        assert!(list.move_front(1));
        assert_eq!(list.remove_front(), Some(1));
        assert_eq!(list.remove_front(), Some(2));
    }

    #[test]
    fn test_impl_remove_head_and_tail() {
        let mut list = LruListImpl::new();
        list.add_back(1);
        list.add_back(2);
        list.add_back(3);

        // Remove head.
        assert!(list.remove(1));
        assert_eq!(list.remove_front(), Some(2));

        // Remove tail.
        list.add_back(4);
        assert!(list.remove(4));
        assert_eq!(list.remove_front(), Some(3));
        assert_eq!(list.remove_front(), None);
    }
}
