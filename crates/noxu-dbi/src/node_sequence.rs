//! Node ID and transient LSN generation.
//!

use noxu_util::Lsn;
use std::sync::atomic::{AtomicI64, Ordering};

/// Generates unique node IDs and transient LSNs.
///
/// 
pub struct NodeSequence {
    last_local_node_id: AtomicI64,
    last_replicated_node_id: AtomicI64,
    last_transient_lsn_offset: AtomicI64,
}

impl NodeSequence {
    /// First local node ID.
    pub const FIRST_LOCAL_NODE_ID: i64 = 1;

    /// First replicated node ID (negative).
    pub const FIRST_REPLICATED_NODE_ID: i64 = -10;

    /// Creates a new NodeSequence.
    pub fn new() -> Self {
        NodeSequence {
            last_local_node_id: AtomicI64::new(Self::FIRST_LOCAL_NODE_ID - 1),
            last_replicated_node_id: AtomicI64::new(
                Self::FIRST_REPLICATED_NODE_ID + 1,
            ),
            last_transient_lsn_offset: AtomicI64::new(0),
        }
    }

    /// Gets the next local node ID.
    pub fn get_next_local_node_id(&self) -> i64 {
        self.last_local_node_id.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// Gets the last allocated local node ID.
    pub fn get_last_local_node_id(&self) -> i64 {
        self.last_local_node_id.load(Ordering::Relaxed)
    }

    /// Sets the last node IDs (used during recovery).
    pub fn set_last_node_id(&self, last_replicated: i64, last_local: i64) {
        self.last_replicated_node_id.store(last_replicated, Ordering::Relaxed);
        self.last_local_node_id.store(last_local, Ordering::Relaxed);
    }

    /// Gets the next transient LSN.
    ///
    /// Transient LSNs use file number 0xFFFFFFFF and are never persisted.
    pub fn get_next_transient_lsn(&self) -> u64 {
        let offset =
            self.last_transient_lsn_offset.fetch_add(1, Ordering::Relaxed);
        Lsn::new(0xFFFFFFFF, offset as u32).as_u64()
    }
}

impl Default for NodeSequence {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let seq = NodeSequence::new();
        assert_eq!(
            seq.get_last_local_node_id(),
            NodeSequence::FIRST_LOCAL_NODE_ID - 1
        );
    }

    #[test]
    fn test_next_local_node_id() {
        let seq = NodeSequence::new();
        let id1 = seq.get_next_local_node_id();
        let id2 = seq.get_next_local_node_id();
        let id3 = seq.get_next_local_node_id();

        assert_eq!(id1, NodeSequence::FIRST_LOCAL_NODE_ID);
        assert_eq!(id2, NodeSequence::FIRST_LOCAL_NODE_ID + 1);
        assert_eq!(id3, NodeSequence::FIRST_LOCAL_NODE_ID + 2);
    }

    #[test]
    fn test_get_last_local_node_id() {
        let seq = NodeSequence::new();
        assert_eq!(
            seq.get_last_local_node_id(),
            NodeSequence::FIRST_LOCAL_NODE_ID - 1
        );

        seq.get_next_local_node_id();
        assert_eq!(
            seq.get_last_local_node_id(),
            NodeSequence::FIRST_LOCAL_NODE_ID
        );

        seq.get_next_local_node_id();
        assert_eq!(
            seq.get_last_local_node_id(),
            NodeSequence::FIRST_LOCAL_NODE_ID + 1
        );
    }

    #[test]
    fn test_set_last_node_id() {
        let seq = NodeSequence::new();
        seq.set_last_node_id(-100, 1000);

        assert_eq!(seq.get_last_local_node_id(), 1000);

        let next_id = seq.get_next_local_node_id();
        assert_eq!(next_id, 1001);
    }

    #[test]
    fn test_transient_lsn_generation() {
        let seq = NodeSequence::new();

        let lsn1 = seq.get_next_transient_lsn();
        let lsn2 = seq.get_next_transient_lsn();
        let lsn3 = seq.get_next_transient_lsn();

        // All should be different
        assert_ne!(lsn1, lsn2);
        assert_ne!(lsn2, lsn3);
        assert_ne!(lsn1, lsn3);

        // All should use file number 0xFFFFFFFF
        let lsn1_obj = Lsn::from_u64(lsn1);
        let lsn2_obj = Lsn::from_u64(lsn2);
        let lsn3_obj = Lsn::from_u64(lsn3);

        assert_eq!(lsn1_obj.file_number(), 0xFFFFFFFF);
        assert_eq!(lsn2_obj.file_number(), 0xFFFFFFFF);
        assert_eq!(lsn3_obj.file_number(), 0xFFFFFFFF);

        // Offsets should be sequential
        assert_eq!(lsn1_obj.file_offset(), 0);
        assert_eq!(lsn2_obj.file_offset(), 1);
        assert_eq!(lsn3_obj.file_offset(), 2);
    }

    #[test]
    fn test_concurrent_node_id_generation() {
        use std::sync::Arc;
        use std::thread;

        let seq = Arc::new(NodeSequence::new());
        let mut handles = vec![];

        for _ in 0..10 {
            let seq_clone = Arc::clone(&seq);
            let handle = thread::spawn(move || {
                let mut ids = vec![];
                for _ in 0..100 {
                    ids.push(seq_clone.get_next_local_node_id());
                }
                ids
            });
            handles.push(handle);
        }

        let mut all_ids = vec![];
        for handle in handles {
            let ids = handle.join().unwrap();
            all_ids.extend(ids);
        }

        // All IDs should be unique
        all_ids.sort();
        let mut dedup = all_ids.clone();
        dedup.dedup();
        assert_eq!(all_ids.len(), dedup.len());

        // Should have generated 1000 IDs
        assert_eq!(all_ids.len(), 1000);
    }
}
