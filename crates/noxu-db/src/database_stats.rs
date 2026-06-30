//! Per-database statistics.
//!
//! Implements `DatabaseStats` (abstract) and `BtreeStats` (concrete).

/// Base statistics type for a database.
///
/// Implements abstract `DatabaseStats`.  All concrete database stats in
/// Noxu are represented by [`BtreeStats`].
#[derive(Clone, Debug, Default)]
pub struct DatabaseStats {
    /// B-tree statistics for this database.
    pub btree: BtreeStats,
}

/// B-tree statistics for a single database.
///
/// Returned by [`Database::stats`][crate::database::Database::stats].
///
/// Implements `BtreeStats` with the most commonly used fields:
///
/// | Field | |
/// |-------|--------------|
/// | `leaf_node_count` | `getLNCount()` |
/// | `deleted_leaf_node_count` | `getDeletedLNCount()` |
/// | `bottom_internal_node_count` | `getBottomInternalNodeCount()` |
/// | `internal_node_count` | `getInternalNodeCount()` |
/// | `main_tree_max_depth` | `getMainTreeMaxDepth()` |
#[derive(Clone, Debug, Default)]
pub struct BtreeStats {
    /// Total number of leaf-node (LN) records in the tree.
    /// Equivalent to the approximate record count for the database.
    pub leaf_node_count: u64,
    /// Number of known-deleted LN slots not yet compacted.
    pub deleted_leaf_node_count: u64,
    /// Number of Bottom Internal Nodes (BINs — leaf-level inner nodes).
    pub bottom_internal_node_count: u64,
    /// Number of upper Internal Nodes (INs above BIN level).
    pub internal_node_count: u64,
    /// Maximum depth of the main tree (root-to-BIN path length).
    pub main_tree_max_depth: u32,
}
