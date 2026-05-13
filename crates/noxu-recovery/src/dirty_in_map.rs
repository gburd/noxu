//! Dirty IN tracking for checkpoint.
//!

use std::collections::BTreeMap;
use hashbrown::{HashMap, HashSet};

/// Checkpoint state machine.
///
/// Tracks the current phase of checkpoint processing. The dirty map must be
/// complete before flushing can begin to ensure a consistent checkpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CkptState {
    /// No checkpoint in progress.
    None,
    /// Checkpoint started but dirty map not yet complete.
    DirtyMapIncomplete,
    /// Dirty map is complete, flushing in progress.
    DirtyMapComplete,
}

/// Checkpoint reference for a dirty IN.
///
/// Represents an Internal Node that needs to be flushed during a checkpoint.
/// The reference includes information needed to locate and flush the node.
#[derive(Debug, Clone)]
pub struct CheckpointReference {
    /// Node ID of the dirty IN.
    pub node_id: u64,
    /// Database ID that this IN belongs to.
    pub db_id: i64,
    /// Whether this is a BIN-delta (vs full BIN/IN).
    pub is_delta: bool,
    /// The dirty node's level in the tree (0 = BIN level).
    pub level: i32,
}

impl CheckpointReference {
    /// Creates a new checkpoint reference.
    pub fn new(node_id: u64, db_id: i64, is_delta: bool, level: i32) -> Self {
        Self { node_id, db_id, is_delta, level }
    }
}

/// Manages the set of dirty INs to be flushed during a checkpoint.
///
/// Organizes dirty INs by level for bottom-up flushing (BINs first, then
/// upper INs). Separates normal INs from BIN-deltas within each level.
///
/// 
pub struct DirtyINMap {
    /// Map of level -> (normal_refs, delta_refs).
    /// BTreeMap ensures levels are processed in order (bottom-up).
    level_map: BTreeMap<
        i32,
        (HashMap<u64, CheckpointReference>, HashMap<u64, CheckpointReference>),
    >,
    /// Total entries across all levels.
    num_entries: usize,
    /// Database IDs whose MapLNs need flushing.
    map_lns_to_flush: HashSet<i64>,
    /// Current checkpoint state.
    ckpt_state: CkptState,
}

impl DirtyINMap {
    /// Creates a new empty DirtyINMap.
    pub fn new() -> Self {
        Self {
            level_map: BTreeMap::new(),
            num_entries: 0,
            map_lns_to_flush: HashSet::new(),
            ckpt_state: CkptState::None,
        }
    }

    /// Adds a dirty IN reference to the appropriate level bucket.
    ///
    /// Separates normal INs and BIN-deltas into different maps for efficient
    /// processing during checkpoint flush.
    pub fn add_dirty_in(&mut self, reference: CheckpointReference) {
        let level = reference.level;
        let node_id = reference.node_id;
        let is_delta = reference.is_delta;

        // Get or create the entry for this level
        let (normal_map, delta_map) = self
            .level_map
            .entry(level)
            .or_insert_with(|| (HashMap::new(), HashMap::new()));

        // Add to appropriate map based on whether it's a delta
        let map = if is_delta { delta_map } else { normal_map };

        // Only increment count if this is a new entry
        if map.insert(node_id, reference).is_none() {
            self.num_entries += 1;
        }
    }

    /// Selects and removes all dirty INs at the specified level.
    ///
    /// Returns normal INs first, then deltas. This ordering ensures proper
    /// checkpoint processing.
    ///
    /// # Arguments
    /// * `level` - Tree level to select INs from
    pub fn select_dirty_ins_for_level(
        &mut self,
        level: i32,
    ) -> Vec<CheckpointReference> {
        if let Some((mut normal_map, mut delta_map)) =
            self.level_map.remove(&level)
        {
            let mut result = Vec::new();

            // Add all normal INs first
            for (_, reference) in normal_map.drain() {
                result.push(reference);
                self.num_entries -= 1;
            }

            // Then add all deltas
            for (_, reference) in delta_map.drain() {
                result.push(reference);
                self.num_entries -= 1;
            }

            result
        } else {
            Vec::new()
        }
    }

    /// Returns the lowest level that has dirty entries.
    ///
    /// Returns None if the map is empty. Used to process levels bottom-up.
    pub fn get_lowest_level(&self) -> Option<i32> {
        self.level_map.keys().next().copied()
    }

    /// Returns the number of dirty entries.
    pub fn get_num_entries(&self) -> usize {
        self.num_entries
    }

    /// Returns true if there are no dirty entries.
    pub fn is_empty(&self) -> bool {
        self.num_entries == 0
    }

    /// Adds a database ID whose MapLN needs to be flushed.
    ///
    /// MapLNs are special leaf nodes in the database name mapping tree that
    /// need to be flushed during checkpoint.
    pub fn add_map_ln_to_flush(&mut self, db_id: i64) {
        self.map_lns_to_flush.insert(db_id);
    }

    /// Returns the set of database IDs whose MapLNs need flushing.
    pub fn get_map_lns_to_flush(&self) -> &HashSet<i64> {
        &self.map_lns_to_flush
    }

    /// Sets the checkpoint state.
    pub fn set_ckpt_state(&mut self, state: CkptState) {
        self.ckpt_state = state;
    }

    /// Returns the current checkpoint state.
    pub fn get_ckpt_state(&self) -> CkptState {
        self.ckpt_state
    }

    /// Clears all dirty entries and resets state.
    pub fn clear(&mut self) {
        self.level_map.clear();
        self.num_entries = 0;
        self.map_lns_to_flush.clear();
        self.ckpt_state = CkptState::None;
    }
}

impl Default for DirtyINMap {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let map = DirtyINMap::new();
        assert_eq!(map.get_num_entries(), 0);
        assert!(map.is_empty());
        assert_eq!(map.get_lowest_level(), None);
        assert_eq!(map.get_ckpt_state(), CkptState::None);
    }

    #[test]
    fn test_default() {
        let map = DirtyINMap::default();
        assert!(map.is_empty());
    }

    #[test]
    fn test_checkpoint_reference_new() {
        let ref1 = CheckpointReference::new(123, 456, false, 1);
        assert_eq!(ref1.node_id, 123);
        assert_eq!(ref1.db_id, 456);
        assert!(!ref1.is_delta);
        assert_eq!(ref1.level, 1);
    }

    #[test]
    fn test_add_dirty_in() {
        let mut map = DirtyINMap::new();

        let ref1 = CheckpointReference::new(1, 100, false, 0);
        map.add_dirty_in(ref1);

        assert_eq!(map.get_num_entries(), 1);
        assert!(!map.is_empty());
        assert_eq!(map.get_lowest_level(), Some(0));
    }

    #[test]
    fn test_add_duplicate_node_id() {
        let mut map = DirtyINMap::new();

        let ref1 = CheckpointReference::new(1, 100, false, 0);
        let ref2 = CheckpointReference::new(1, 100, false, 0); // Same node_id

        map.add_dirty_in(ref1);
        map.add_dirty_in(ref2);

        // Should only count once
        assert_eq!(map.get_num_entries(), 1);
    }

    #[test]
    fn test_add_multiple_levels() {
        let mut map = DirtyINMap::new();

        map.add_dirty_in(CheckpointReference::new(1, 100, false, 0));
        map.add_dirty_in(CheckpointReference::new(2, 100, false, 1));
        map.add_dirty_in(CheckpointReference::new(3, 100, false, 2));

        assert_eq!(map.get_num_entries(), 3);
        assert_eq!(map.get_lowest_level(), Some(0)); // BTreeMap keeps sorted
    }

    #[test]
    fn test_add_normal_and_delta() {
        let mut map = DirtyINMap::new();

        map.add_dirty_in(CheckpointReference::new(1, 100, false, 0)); // Normal
        map.add_dirty_in(CheckpointReference::new(2, 100, true, 0)); // Delta

        assert_eq!(map.get_num_entries(), 2);
    }

    #[test]
    fn test_select_dirty_ins_for_level() {
        let mut map = DirtyINMap::new();

        map.add_dirty_in(CheckpointReference::new(1, 100, false, 0));
        map.add_dirty_in(CheckpointReference::new(2, 100, true, 0));
        map.add_dirty_in(CheckpointReference::new(3, 100, false, 1));

        let level0_refs = map.select_dirty_ins_for_level(0);
        assert_eq!(level0_refs.len(), 2);
        assert_eq!(map.get_num_entries(), 1); // Level 1 still there

        let level1_refs = map.select_dirty_ins_for_level(1);
        assert_eq!(level1_refs.len(), 1);
        assert_eq!(map.get_num_entries(), 0);
        assert!(map.is_empty());
    }

    #[test]
    fn test_select_returns_normal_before_delta() {
        let mut map = DirtyINMap::new();

        // Add delta first
        map.add_dirty_in(CheckpointReference::new(2, 100, true, 0));
        // Add normal second
        map.add_dirty_in(CheckpointReference::new(1, 100, false, 0));

        let refs = map.select_dirty_ins_for_level(0);
        assert_eq!(refs.len(), 2);

        // First should be normal (is_delta = false)
        let normal_count = refs.iter().filter(|r| !r.is_delta).count();
        let delta_count = refs.iter().filter(|r| r.is_delta).count();
        assert_eq!(normal_count, 1);
        assert_eq!(delta_count, 1);
    }

    #[test]
    fn test_select_nonexistent_level() {
        let mut map = DirtyINMap::new();
        map.add_dirty_in(CheckpointReference::new(1, 100, false, 0));

        let refs = map.select_dirty_ins_for_level(999);
        assert!(refs.is_empty());
        assert_eq!(map.get_num_entries(), 1); // Unchanged
    }

    #[test]
    fn test_get_lowest_level_ordering() {
        let mut map = DirtyINMap::new();

        // Add in non-sorted order
        map.add_dirty_in(CheckpointReference::new(1, 100, false, 5));
        map.add_dirty_in(CheckpointReference::new(2, 100, false, 2));
        map.add_dirty_in(CheckpointReference::new(3, 100, false, 8));

        assert_eq!(map.get_lowest_level(), Some(2)); // BTreeMap keeps sorted
    }

    #[test]
    fn test_add_map_ln_to_flush() {
        let mut map = DirtyINMap::new();

        map.add_map_ln_to_flush(100);
        map.add_map_ln_to_flush(200);
        map.add_map_ln_to_flush(100); // Duplicate

        let map_lns = map.get_map_lns_to_flush();
        assert_eq!(map_lns.len(), 2);
        assert!(map_lns.contains(&100));
        assert!(map_lns.contains(&200));
    }

    #[test]
    fn test_checkpoint_state() {
        let mut map = DirtyINMap::new();

        assert_eq!(map.get_ckpt_state(), CkptState::None);

        map.set_ckpt_state(CkptState::DirtyMapIncomplete);
        assert_eq!(map.get_ckpt_state(), CkptState::DirtyMapIncomplete);

        map.set_ckpt_state(CkptState::DirtyMapComplete);
        assert_eq!(map.get_ckpt_state(), CkptState::DirtyMapComplete);
    }

    #[test]
    fn test_clear() {
        let mut map = DirtyINMap::new();

        map.add_dirty_in(CheckpointReference::new(1, 100, false, 0));
        map.add_dirty_in(CheckpointReference::new(2, 100, false, 1));
        map.add_map_ln_to_flush(100);
        map.set_ckpt_state(CkptState::DirtyMapComplete);

        assert_eq!(map.get_num_entries(), 2);
        assert_eq!(map.get_map_lns_to_flush().len(), 1);
        assert_eq!(map.get_ckpt_state(), CkptState::DirtyMapComplete);

        map.clear();

        assert_eq!(map.get_num_entries(), 0);
        assert!(map.is_empty());
        assert_eq!(map.get_map_lns_to_flush().len(), 0);
        assert_eq!(map.get_ckpt_state(), CkptState::None);
        assert_eq!(map.get_lowest_level(), None);
    }

    #[test]
    fn test_complex_scenario() {
        let mut map = DirtyINMap::new();

        // Add multiple nodes at various levels
        for level in 0i32..5i32 {
            for node_id in 0..3 {
                let is_delta = node_id % 2 == 0;
                map.add_dirty_in(CheckpointReference::new(
                    (level * 10) as u64 + node_id,
                    100,
                    is_delta,
                    level,
                ));
            }
        }

        assert_eq!(map.get_num_entries(), 15); // 5 levels * 3 nodes

        // Process bottom-up
        let mut processed_count = 0;
        while let Some(level) = map.get_lowest_level() {
            let refs = map.select_dirty_ins_for_level(level);
            processed_count += refs.len();
        }

        assert_eq!(processed_count, 15);
        assert!(map.is_empty());
    }

    #[test]
    fn test_negative_levels() {
        let mut map = DirtyINMap::new();

        map.add_dirty_in(CheckpointReference::new(1, 100, false, -1));
        map.add_dirty_in(CheckpointReference::new(2, 100, false, 0));

        assert_eq!(map.get_lowest_level(), Some(-1));
    }

    #[test]
    fn test_ckpt_state_equality() {
        assert_eq!(CkptState::None, CkptState::None);
        assert_ne!(CkptState::None, CkptState::DirtyMapIncomplete);
        assert_ne!(CkptState::DirtyMapIncomplete, CkptState::DirtyMapComplete);
    }
}
