//! Slab-allocated intrusive doubly-linked list — shared primitive for eviction
//! policies.
//!
//! head = cold/LRU end (evicted first), tail = hot/MRU end (most recently
//! used).  All operations are O(1) amortised.

use hashbrown::HashMap;

pub(crate) const SENTINEL: usize = usize::MAX;

/// One slot in the slab.
#[derive(Debug, Clone)]
pub(crate) struct SlabNode {
    pub id: u64,
    pub prev: usize,
    pub next: usize,
}

/// Slab-allocated intrusive doubly-linked list with O(1) insert / remove /
/// move and O(1) lookup by node id.
#[derive(Debug)]
pub(crate) struct SlabList {
    pub slab: Vec<Option<SlabNode>>,
    pub free: Vec<usize>,
    pub index: HashMap<u64, usize>,
    /// Cold/LRU end (evicted first).
    pub head: usize,
    /// Hot/MRU end (most recently used).
    pub tail: usize,
    pub len: usize,
}

impl SlabList {
    pub fn new() -> Self {
        Self {
            slab: Vec::new(),
            free: Vec::new(),
            index: HashMap::new(),
            head: SENTINEL,
            tail: SENTINEL,
            len: 0,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Allocate a slab slot for `id` (node has dangling prev/next).
    pub fn alloc_slot(&mut self, id: u64) -> usize {
        if let Some(slot) = self.free.pop() {
            self.slab[slot] =
                Some(SlabNode { id, prev: SENTINEL, next: SENTINEL });
            slot
        } else {
            let slot = self.slab.len();
            self.slab.push(Some(SlabNode {
                id,
                prev: SENTINEL,
                next: SENTINEL,
            }));
            slot
        }
    }

    /// Return a slot to the free list.
    pub fn free_slot(&mut self, slot: usize) {
        self.slab[slot] = None;
        self.free.push(slot);
    }

    /// Unlink `slot` from the list (does not free the slot).
    pub fn unlink(&mut self, slot: usize) {
        let (prev, next) = {
            let n = self.slab[slot].as_ref().unwrap();
            (n.prev, n.next)
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

    /// Link `slot` at the back (hot/MRU end).
    pub fn link_back(&mut self, slot: usize) {
        let old_tail = self.tail;
        let n = self.slab[slot].as_mut().unwrap();
        n.prev = old_tail;
        n.next = SENTINEL;
        if old_tail == SENTINEL {
            self.head = slot;
        } else {
            self.slab[old_tail].as_mut().unwrap().next = slot;
        }
        self.tail = slot;
    }

    /// Link `slot` at the front (cold/LRU end).
    pub fn link_front(&mut self, slot: usize) {
        let old_head = self.head;
        let n = self.slab[slot].as_mut().unwrap();
        n.prev = SENTINEL;
        n.next = old_head;
        if old_head == SENTINEL {
            self.tail = slot;
        } else {
            self.slab[old_head].as_mut().unwrap().prev = slot;
        }
        self.head = slot;
    }

    /// Add `id` at the hot/MRU end (back). Panics if already present (debug).
    pub fn add_back(&mut self, id: u64) {
        debug_assert!(!self.index.contains_key(&id));
        let slot = self.alloc_slot(id);
        self.link_back(slot);
        self.index.insert(id, slot);
        self.len += 1;
    }

    /// Add `id` at the cold/LRU end (front). Panics if already present (debug).
    pub fn add_front(&mut self, id: u64) {
        debug_assert!(!self.index.contains_key(&id));
        let slot = self.alloc_slot(id);
        self.link_front(slot);
        self.index.insert(id, slot);
        self.len += 1;
    }

    /// Move `id` to the hot/MRU end.  Returns false if not present.
    pub fn move_back(&mut self, id: u64) -> bool {
        if let Some(&slot) = self.index.get(&id) {
            if slot != self.tail {
                self.unlink(slot);
                self.link_back(slot);
            }
            true
        } else {
            false
        }
    }

    /// Move `id` to the cold/LRU end.  Returns false if not present.
    pub fn move_front(&mut self, id: u64) -> bool {
        if let Some(&slot) = self.index.get(&id) {
            if slot != self.head {
                self.unlink(slot);
                self.link_front(slot);
            }
            true
        } else {
            false
        }
    }

    /// Remove and return the cold/LRU node.  Returns None if empty.
    pub fn remove_front(&mut self) -> Option<u64> {
        if self.head == SENTINEL {
            return None;
        }
        let slot = self.head;
        let id = self.slab[slot].as_ref().unwrap().id;
        self.unlink(slot);
        self.free_slot(slot);
        self.index.remove(&id);
        self.len -= 1;
        Some(id)
    }

    /// Remove and return the hot/MRU node.  Returns None if empty.
    pub fn remove_back(&mut self) -> Option<u64> {
        if self.tail == SENTINEL {
            return None;
        }
        let slot = self.tail;
        let id = self.slab[slot].as_ref().unwrap().id;
        self.unlink(slot);
        self.free_slot(slot);
        self.index.remove(&id);
        self.len -= 1;
        Some(id)
    }

    /// Remove a specific node by id.  Returns false if not present.
    pub fn remove(&mut self, id: u64) -> bool {
        if let Some(slot) = self.index.remove(&id) {
            self.unlink(slot);
            self.free_slot(slot);
            self.len -= 1;
            true
        } else {
            false
        }
    }

    /// Returns true if `id` is present.
    pub fn contains(&self, id: u64) -> bool {
        self.index.contains_key(&id)
    }

    /// Id of the node at the cold/LRU end without removing.  None if empty.
    pub fn peek_front(&self) -> Option<u64> {
        if self.head == SENTINEL {
            None
        } else {
            Some(self.slab[self.head].as_ref().unwrap().id)
        }
    }

    /// Id of the node at the hot/MRU end without removing.  None if empty.
    pub fn peek_back(&self) -> Option<u64> {
        if self.tail == SENTINEL {
            None
        } else {
            Some(self.slab[self.tail].as_ref().unwrap().id)
        }
    }

    /// Slot index for `id`, or SENTINEL.
    pub fn slot_of(&self, id: u64) -> usize {
        self.index.get(&id).copied().unwrap_or(SENTINEL)
    }

    /// Next node id after `slot` in the hot→cold direction (following `next`).
    /// Returns None when `slot` is the tail or is SENTINEL.
    pub fn next_id(&self, slot: usize) -> Option<u64> {
        if slot == SENTINEL {
            return None;
        }
        let next = self.slab[slot].as_ref()?.next;
        if next == SENTINEL { None } else { Some(self.slab[next].as_ref()?.id) }
    }
}

impl Default for SlabList {
    fn default() -> Self {
        Self::new()
    }
}
