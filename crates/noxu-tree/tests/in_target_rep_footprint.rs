//! T-4 footprint guard: `INTargetRep` None/Sparse/Default compaction.
//!
//! Proves the heap-footprint win from hoisting the per-`InEntry` resident
//! child pointer (`Option<Arc>`) to a node-level `INTargetRep` (`TargetRep`):
//!
//!   * An upper IN with NO resident children uses `TargetRep::None` — 0
//!     child-pointer bytes — instead of `N * size_of::<Option<Arc>>()`.
//!   * A few resident children use `TargetRep::Sparse`; many inflate to
//!     `TargetRep::Default`.
//!
//! Faithful to JE `INTargetRep.{None,Sparse,Default}` (INTargetRep.java) and
//! `INTargetRep.Sparse.MAX_ENTRIES == 4`.
//!
//! Pre-compaction model: every slot carried an `Option<Arc<RwLock<TreeNode>>>`
//! (8 bytes on 64-bit) whether or not a child was resident.  This test asserts
//! the post-compaction `budgeted_memory_size()` is strictly smaller than that
//! pre-compaction model for the common (no-resident-children) upper IN.

use std::mem::size_of;
use std::sync::Arc;

use noxu_tree::tree::TreeNode;
use noxu_tree::{ChildArc, InEntry, InNodeStub, TargetRep};
use noxu_util::Lsn;
use parking_lot::RwLock;

const MAIN_LEVEL: i32 = 0x20000;

fn empty_in(n: usize) -> InNodeStub {
    let entries = (0..n)
        .map(|i| InEntry {
            key: vec![(i % 256) as u8, (i / 256) as u8],
            lsn: Lsn::from_u64(100 + i as u64),
        })
        .collect();
    InNodeStub {
        node_id: 1,
        level: MAIN_LEVEL | 2,
        entries,
        targets: TargetRep::None,
        dirty: false,
        generation: 0,
        parent: None,
    }
}

fn dummy_child() -> ChildArc {
    Arc::new(RwLock::new(TreeNode::Internal(empty_in(0))))
}

/// An upper IN with no resident children must use `TargetRep::None` (0
/// child-pointer bytes) — `INTargetRep.None`.
#[test]
fn no_resident_children_uses_none_rep() {
    let n = empty_in(64);
    assert!(matches!(n.targets, TargetRep::None));
    assert_eq!(n.targets.memory_size(), 0, "None rep must cost 0 bytes");
    assert_eq!(n.targets.resident_count(), 0);
    for i in 0..64 {
        assert!(n.get_child(i).is_none());
    }
}

/// Numerically prove the footprint reduction vs the pre-compaction layout
/// where each slot carried an `Option<Arc>` child pointer.
#[test]
fn no_children_footprint_smaller_than_pre_compaction() {
    let n = 256usize;
    let node = TreeNode::Internal(empty_in(n));
    let post = node.budgeted_memory_size();

    // Pre-compaction model: InEntry used to be 8 bytes wider (the
    // Option<Arc> child), and there was NO node-level target rep.
    let pre = post + (n as u64) * size_of::<Option<ChildArc>>() as u64;

    assert!(
        post < pre,
        "T-4: post-compaction footprint ({post}) must be < pre ({pre})"
    );
    // The saving is exactly the removed per-slot pointer (None rep is 0).
    assert_eq!(
        pre - post,
        (n as u64) * size_of::<Option<ChildArc>>() as u64,
        "saving must equal the eliminated per-slot Option<Arc>"
    );
}

/// A few resident children use `Sparse`; the 5th inflates to `Default`.
/// `INTargetRep.Sparse.MAX_ENTRIES == 4`.
#[test]
fn grows_none_to_sparse_to_default() {
    let mut n = empty_in(16);
    assert!(matches!(n.targets, TargetRep::None));

    // First child -> Sparse.
    n.set_child(0, Some(dummy_child()));
    assert!(matches!(n.targets, TargetRep::Sparse(_)), "1 child -> Sparse");

    // Up to MAX_ENTRIES (4) stay Sparse.
    for i in 1..TargetRep::SPARSE_MAX_ENTRIES {
        n.set_child(i, Some(dummy_child()));
    }
    assert!(matches!(n.targets, TargetRep::Sparse(_)), "4 children -> Sparse");
    assert_eq!(n.targets.resident_count(), TargetRep::SPARSE_MAX_ENTRIES);

    // The 5th resident child inflates to Default.
    n.set_child(TargetRep::SPARSE_MAX_ENTRIES, Some(dummy_child()));
    assert!(
        matches!(n.targets, TargetRep::Default(_)),
        "5 children -> Default"
    );
    assert_eq!(n.targets.resident_count(), TargetRep::SPARSE_MAX_ENTRIES + 1);
}

/// `INTargetRep.compact` collapses a Default rep back to Sparse/None when
/// children are stripped (eviction path).
#[test]
fn compact_collapses_back() {
    let mut n = empty_in(16);
    for i in 0..6 {
        n.set_child(i, Some(dummy_child()));
    }
    assert!(matches!(n.targets, TargetRep::Default(_)));

    // Strip all but two children, then compact -> Sparse.
    for i in 0..4 {
        n.take_child(i);
    }
    n.targets.compact();
    assert!(matches!(n.targets, TargetRep::Sparse(_)), "2 left -> Sparse");

    // Strip the rest, compact -> None (0 bytes).
    n.take_child(4);
    n.take_child(5);
    n.targets.compact();
    assert!(matches!(n.targets, TargetRep::None), "0 left -> None");
    assert_eq!(n.targets.memory_size(), 0);
}

/// Children stay aligned with their slots across insert/remove (the
/// `INArrayRep.copy` shift semantics) — a correctness guard on the layout
/// change, not just footprint.
#[test]
fn child_mapping_survives_insert_remove() {
    let mut n = empty_in(4);
    let c1 = dummy_child();
    let c3 = dummy_child();
    n.set_child(1, Some(c1.clone()));
    n.set_child(3, Some(c3.clone()));

    // Insert a new entry at slot 1: children at >=1 shift up by one.
    n.insert_entry(1, vec![9, 9], Lsn::from_u64(999), None);
    assert!(n.get_child(0).is_none());
    assert!(n.get_child(1).is_none(), "inserted slot has no child");
    assert!(Arc::ptr_eq(&n.get_child(2).unwrap(), &c1), "c1 shifted 1->2");
    assert!(Arc::ptr_eq(&n.get_child(4).unwrap(), &c3), "c3 shifted 3->4");

    // Remove slot 2 (where c1 now lives): c1 dropped, c3 shifts down.
    n.remove_entry(2);
    assert!(n.get_child(2).is_none());
    assert!(Arc::ptr_eq(&n.get_child(3).unwrap(), &c3), "c3 shifted 4->3");
}
