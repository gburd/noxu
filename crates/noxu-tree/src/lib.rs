#![forbid(unsafe_code)]
// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT
#![allow(dead_code, clippy::type_complexity, clippy::too_many_arguments)]
//! > **Internal component of the [`noxu`](https://crates.io/crates/noxu) database.**
//! >
//! > This crate is published only so the `noxu` umbrella crate can depend on it.
//! > Use `noxu` (`noxu = "3"`) in applications; depend on this crate directly only
//! > if you are extending the engine internals. Its API may change without a major
//! > version bump.
//!
//! B-tree implementation for Noxu DB.
//!
//! the main in-memory cache containing
//! persistent B-tree nodes (IN, BIN, LN) and access methods.
//!
//! # Module Structure
//!
//! - `error`  -  Tree error types
//! - `entry_states`  -  Slot state bit flags (KD, PD, dirty, embedded)
//! - `key`  -  Key comparison and prefix utilities
//! - `node`  -  Base node types and ID generation
//! - `child_reference`  -  Reference from parent to child node
//! - `in_node`  -  Internal Node (core B-tree node)
//! - `ln`  -  Leaf Node (data records)
//! - `tree`  -  B+tree operations (search, insert, split)
//!
//! # Architecture
//!
//! The tree is structured as a B+tree with:
//! - Internal Nodes (IN) at higher levels
//! - Bottom Internal Nodes (BIN) at the leaf level
//! - Leaf Nodes (LN) containing actual data
//!
//! The tree uses latch-coupling for concurrent access and supports
//! splits, compression, and efficient caching.

// Error types
pub mod error;

// Foundation types - implemented by other agents
pub mod entry_states;
pub mod key;
pub mod node;
pub mod tree_utils;

// Tree node references
pub mod child_reference;
pub mod delta_info;
pub mod search_result;
pub mod tracking_info;
pub mod tree_location;

// Foundation utility modules

// Tree nodes

// Leaf nodes - implemented by other agents
pub mod file_summary_ln;
pub mod ln;
pub mod map_ln;
pub mod name_ln;
pub mod uncached_ln;
pub mod versioned_ln;

// Tree operations
pub mod tree;

// Re-exports for convenience
pub use error::TreeError;
pub use ln::Ln;
pub use tree::InListListener;

// Re-export from other agent modules (if they compile)
pub use file_summary_ln::{FileSummary, FileSummaryLn};
pub use map_ln::MapLn;
pub use name_ln::NameLn;
pub use uncached_ln::{make_uncached_ln, make_uncached_ln_from_bytes};
pub use versioned_ln::make_versioned_ln;

// Tree types
pub use tree::{
    BinEntry, BinStub, ChildArc, InEntry, InNodeStub, InRedoResult,
    KeyComparatorFn, KeyRep, LsnRep, SlotFetch, TargetRep, Tree, TreeNode,
    TreeStats, generate_node_id, peek_next_node_id_counter,
    seed_node_id_counter,
};

// Tree node level constants and search-result flags.  These are defined in
// `tree` (the runtime implementation); the former `in_node` definitions were
// removed with the shelved faithful `InNode` (T-1).
pub use tree::{
    BIN_LEVEL, DBMAP_LEVEL, EXACT_MATCH, INSERT_SUCCESS, LEVEL_MASK,
    MAIN_LEVEL, MIN_LEVEL,
};

// Re-export foundation types
pub use child_reference::ChildReference;
pub use entry_states::SlotState;
pub use key::KeyComparator;
pub use node::{NULL_NODE_ID, NodeType};
pub use search_result::SearchResult;
pub use tree_location::TreeLocation;

// Re-export the RwLock used for tree nodes so downstream crates can reference
// the same type without depending on parking_lot directly.
pub use parking_lot::RwLock as NodeRwLock;
