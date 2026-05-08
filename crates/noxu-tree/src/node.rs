//! Base node types and identifiers for the B-tree.
//!
//!
//! In JE, Node is an abstract base class. In Rust, we use an enum for the
//! closed set of node types, plus utilities for node ID generation.

use std::sync::atomic::{AtomicI64, Ordering};

/// Sentinel value representing a null/uninitialized node ID.
pub const NULL_NODE_ID: i64 = -1;

/// Identifies the kind of a tree node.
///
/// This enum represents the closed set of node types in the B-tree.
/// Each type has specific semantics and behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NodeType {
    /// Upper Internal Node - non-leaf node in the B-tree.
    IN,

    /// Bottom Internal Node - leaf-level node containing references to LNs.
    BIN,

    /// BIN Delta - a partial BIN containing only changed slots.
    BINDelta,

    /// Leaf Node - contains actual data records.
    LN,

    /// MapLN - special LN that references a database metadata tree.
    MapLN,

    /// NameLN - special LN that maps database names to database IDs.
    NameLN,

    /// FileSummaryLN - special LN that tracks log file utilization.
    FileSummaryLN,
}

impl NodeType {
    /// Returns true if this is any type of LN (leaf node).
    #[inline]
    pub fn is_ln(self) -> bool {
        matches!(
            self,
            NodeType::LN
                | NodeType::MapLN
                | NodeType::NameLN
                | NodeType::FileSummaryLN
        )
    }

    /// Returns true if this is any type of IN (internal node).
    ///
    /// This includes IN, BIN, and BINDelta.
    #[inline]
    pub fn is_in(self) -> bool {
        matches!(self, NodeType::IN | NodeType::BIN | NodeType::BINDelta)
    }

    /// Returns true if this is a BIN or BINDelta.
    #[inline]
    pub fn is_bin(self) -> bool {
        matches!(self, NodeType::BIN | NodeType::BINDelta)
    }

    /// Returns true if this is an upper IN (non-leaf internal node).
    #[inline]
    pub fn is_upper_in(self) -> bool {
        matches!(self, NodeType::IN)
    }

    /// Returns true if this is a BIN delta.
    #[inline]
    pub fn is_bin_delta(self) -> bool {
        matches!(self, NodeType::BINDelta)
    }

    /// Returns the tree level for this node type.
    ///
    /// LNs are level 0. For internal nodes, the level is determined at runtime
    /// and stored in the IN structure, so this returns -1 as a sentinel.
    #[inline]
    pub fn level(self) -> i32 {
        if self.is_ln() {
            0
        } else {
            -1 // Level is stored in the IN/BIN itself
        }
    }

    /// Returns a string name for this node type.
    pub fn name(self) -> &'static str {
        match self {
            NodeType::IN => "IN",
            NodeType::BIN => "BIN",
            NodeType::BINDelta => "BINDelta",
            NodeType::LN => "LN",
            NodeType::MapLN => "MapLN",
            NodeType::NameLN => "NameLN",
            NodeType::FileSummaryLN => "FileSummaryLN",
        }
    }
}

impl std::fmt::Display for NodeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

/// Global counter for generating unique node IDs.
///
/// Node IDs are unique within an environment instance (not persisted across
/// environment open/close). They start at 1 and increment monotonically.
static NEXT_NODE_ID: AtomicI64 = AtomicI64::new(1);

/// Generates a new unique node ID.
///
/// Node IDs are used to identify nodes in memory. They are not the same as
/// LSNs (which identify the on-disk location). Node IDs are transient and
/// start from 1 each time the environment is opened.
///
/// # Returns
/// A unique positive i64 node ID
pub fn generate_node_id() -> i64 {
    NEXT_NODE_ID.fetch_add(1, Ordering::Relaxed)
}

/// Resets the node ID counter to 1.
///
/// This is used during environment initialization or in testing.
/// In production, this is called when an environment is opened.
pub fn reset_node_id_counter() {
    NEXT_NODE_ID.store(1, Ordering::Relaxed);
}

/// Returns the next node ID that would be generated (without incrementing).
///
/// Used for testing and diagnostics.
pub fn peek_next_node_id() -> i64 {
    NEXT_NODE_ID.load(Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_type_is_ln() {
        assert!(NodeType::LN.is_ln());
        assert!(NodeType::MapLN.is_ln());
        assert!(NodeType::NameLN.is_ln());
        assert!(NodeType::FileSummaryLN.is_ln());

        assert!(!NodeType::IN.is_ln());
        assert!(!NodeType::BIN.is_ln());
        assert!(!NodeType::BINDelta.is_ln());
    }

    #[test]
    fn test_node_type_is_in() {
        assert!(NodeType::IN.is_in());
        assert!(NodeType::BIN.is_in());
        assert!(NodeType::BINDelta.is_in());

        assert!(!NodeType::LN.is_in());
        assert!(!NodeType::MapLN.is_in());
    }

    #[test]
    fn test_node_type_is_bin() {
        assert!(NodeType::BIN.is_bin());
        assert!(NodeType::BINDelta.is_bin());

        assert!(!NodeType::IN.is_bin());
        assert!(!NodeType::LN.is_bin());
    }

    #[test]
    fn test_node_type_is_upper_in() {
        assert!(NodeType::IN.is_upper_in());

        assert!(!NodeType::BIN.is_upper_in());
        assert!(!NodeType::BINDelta.is_upper_in());
        assert!(!NodeType::LN.is_upper_in());
    }

    #[test]
    fn test_node_type_is_bin_delta() {
        assert!(NodeType::BINDelta.is_bin_delta());

        assert!(!NodeType::BIN.is_bin_delta());
        assert!(!NodeType::IN.is_bin_delta());
        assert!(!NodeType::LN.is_bin_delta());
    }

    #[test]
    fn test_node_type_level() {
        // All LN types are level 0
        assert_eq!(NodeType::LN.level(), 0);
        assert_eq!(NodeType::MapLN.level(), 0);
        assert_eq!(NodeType::NameLN.level(), 0);
        assert_eq!(NodeType::FileSummaryLN.level(), 0);

        // Internal nodes return -1 (level stored elsewhere)
        assert_eq!(NodeType::IN.level(), -1);
        assert_eq!(NodeType::BIN.level(), -1);
        assert_eq!(NodeType::BINDelta.level(), -1);
    }

    #[test]
    fn test_node_type_name() {
        assert_eq!(NodeType::IN.name(), "IN");
        assert_eq!(NodeType::BIN.name(), "BIN");
        assert_eq!(NodeType::BINDelta.name(), "BINDelta");
        assert_eq!(NodeType::LN.name(), "LN");
        assert_eq!(NodeType::MapLN.name(), "MapLN");
        assert_eq!(NodeType::NameLN.name(), "NameLN");
        assert_eq!(NodeType::FileSummaryLN.name(), "FileSummaryLN");
    }

    #[test]
    fn test_node_type_display() {
        assert_eq!(format!("{}", NodeType::BIN), "BIN");
        assert_eq!(format!("{}", NodeType::LN), "LN");
    }

    #[test]
    fn test_generate_node_id() {
        reset_node_id_counter();

        let id1 = generate_node_id();
        let id2 = generate_node_id();
        let id3 = generate_node_id();

        assert_eq!(id1, 1);
        assert_eq!(id2, 2);
        assert_eq!(id3, 3);
        assert!(id1 != NULL_NODE_ID);
    }

    #[test]
    fn test_reset_node_id_counter() {
        reset_node_id_counter();
        assert_eq!(generate_node_id(), 1);
        assert_eq!(generate_node_id(), 2);

        reset_node_id_counter();
        assert_eq!(generate_node_id(), 1);
    }

    #[test]
    fn test_peek_next_node_id() {
        reset_node_id_counter();

        let next = peek_next_node_id();
        assert_eq!(next, 1);

        generate_node_id();
        let next2 = peek_next_node_id();
        assert_eq!(next2, 2);

        // Peek doesn't increment
        let next3 = peek_next_node_id();
        assert_eq!(next3, 2);
    }

    #[test]
    fn test_null_node_id_constant() {
        assert_eq!(NULL_NODE_ID, -1);
        assert!(generate_node_id() != NULL_NODE_ID);
    }
}
