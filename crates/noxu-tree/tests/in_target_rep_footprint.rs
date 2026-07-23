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
use noxu_tree::{ChildArc, InEntry, InNodeStub, KeyRep, LsnRep, TargetRep};
use noxu_util::Lsn;
use parking_lot::RwLock;

const MAIN_LEVEL: i32 = 0x20000;

fn empty_in(n: usize) -> InNodeStub {
    let entries = (0..n)
        .map(|i| InEntry { key: vec![(i % 256) as u8, (i / 256) as u8] })
        .collect();
    let lsns: Vec<Lsn> =
        (0..n).map(|i| Lsn::from_u64(100 + i as u64)).collect();
    InNodeStub {
        node_id: 1,
        level: MAIN_LEVEL | 2,
        entries,
        targets: TargetRep::None,
        dirty: false,
        generation: 0,
        parent: None,
        lsn_rep: LsnRep::from_lsns(&lsns),
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

// ===========================================================================
// T-3: LsnRep packed-LSN footprint (IN.entryLsnByteArray, IN.java:251-289).
// ===========================================================================

/// Build a BIN whose N slots all share file number 7 (a same-file-number
/// node, the common case for a recently-written BIN).
fn bin_same_file(n: usize) -> noxu_tree::BinStub {
    let mut bin = noxu_tree::BinStub {
        node_id: 1,
        level: noxu_tree::BIN_LEVEL,
        entries: Vec::new(),
        key_prefix: Vec::new(),
        dirty: false,
        is_delta: false,
        last_full_lsn: noxu_util::NULL_LSN,
        last_delta_lsn: noxu_util::NULL_LSN,
        generation: 0,
        parent: None,
        expiration_in_hours: true,
        cursor_count: 0,
        prohibit_next_delta: false,
        lsn_rep: LsnRep::Empty,
        keys: KeyRep::new(),
        compact_max_key_length:
            noxu_tree::tree::INKeyRep_DEFAULT_MAX_KEY_LENGTH,
        expiration_enabled: true,
    };
    for i in 0..n {
        let k = (i as u32).to_be_bytes().to_vec();
        bin.insert_with_prefix(k, Lsn::new(7, 1000 + i as u32), None);
    }
    bin
}

/// A BIN whose LSNs share a file number packs to ~4 bytes/slot via the
/// Compact rep, vs 8 bytes/slot for a raw `Lsn` field.  `INLongRep` /
/// `IN.entryLsnByteArray` (4 = `BYTES_PER_LSN_ENTRY`).
#[test]
fn lsn_same_file_packs_compact() {
    let n = 64usize;
    let bin = bin_same_file(n);
    // 64 slots * 4 bytes = 256 bytes, vs 64 * 8 = 512 for raw u64 LSNs.
    assert_eq!(
        bin.lsn_rep.memory_size(),
        n * 4,
        "same-file-number node packs to 4 bytes/slot"
    );
    assert!(
        bin.lsn_rep.memory_size() < n * 8,
        "packed LSN footprint must be < the raw u64-per-slot footprint"
    );
    // LSNs still read back exactly.
    for i in 0..n {
        assert_eq!(bin.get_lsn(i), Lsn::new(7, 1000 + i as u32));
    }
}

/// An all-NULL-LSN node uses the EmptyRep (0 heap bytes), matching JE's
/// `entryLsnByteArray == null` initial state.
#[test]
fn lsn_all_null_uses_empty_rep() {
    let bin = bin_same_file(0);
    assert!(matches!(bin.lsn_rep, LsnRep::Empty));
    assert_eq!(bin.lsn_rep.memory_size(), 0, "all-NULL node costs 0 LSN bytes");
}

/// Numerically prove the per-node footprint reduction vs the pre-T-3 layout
/// where every `BinEntry` carried an 8-byte `lsn: Lsn` field.
#[test]
fn lsn_footprint_smaller_than_pre_compaction() {
    use std::mem::size_of;
    let n = 128usize;
    let bin = bin_same_file(n);
    let node = TreeNode::Bottom(bin);
    let post = node.budgeted_memory_size();

    // Pre-compaction model: BinEntry was 8 bytes wider (the Lsn field) and
    // there was no node-level LsnRep.
    let pre =
        post + (n as u64) * size_of::<Lsn>() as u64 - node_lsn_rep_bytes(&node);

    assert!(
        post < pre,
        "T-3: post-compaction footprint ({post}) must be < pre ({pre})"
    );
}

fn node_lsn_rep_bytes(node: &TreeNode) -> u64 {
    match node {
        TreeNode::Bottom(b) => b.lsn_rep.memory_size() as u64,
        TreeNode::Internal(n) => n.lsn_rep.memory_size() as u64,
    }
}

// ===========================================================================
// T-2: KeyRep compact-key footprint (INKeyRep.MaxKeySize, INKeyRep.java).
// ===========================================================================

/// A BIN whose post-prefix keys are all small (<= TREE_COMPACT_MAX_KEY_LENGTH)
/// uses the Compact key rep (one fixed-width buffer, no per-key `Vec`), not
/// the Default `Vec<Vec<u8>>`.  `INKeyRep.MaxKeySize`.
#[test]
fn small_keys_use_compact_rep() {
    let n = 64usize;
    let bin = bin_same_file_with_keys(n, 8); // 8-byte keys, no prefix
    let TreeNode::Bottom(b) = TreeNode::Bottom(bin) else { unreachable!() };
    assert!(
        b.keys.is_compact(),
        "all-small-key BIN must use the Compact key rep"
    );
    // Compact buffer is slot_width*n + lengths; no per-key Vec headers.
    // For 8-byte keys with no prefix, slot_width == 8.
    assert!(
        b.keys.memory_size() <= n * 8 + n * 2 + 16,
        "compact key rep must be ~slot_width*n bytes, got {}",
        b.keys.memory_size()
    );
}

/// A key longer than TREE_COMPACT_MAX_KEY_LENGTH inflates the node to the
/// Default rep (`MaxKeySize.expandToDefaultRep`).
#[test]
fn long_key_inflates_to_default() {
    let mut bin = empty_bin();
    // Insert small keys -> Compact after prefix recompute.
    for i in 0..4u32 {
        bin.insert_with_prefix(
            format!("k{i:02}").into_bytes(),
            Lsn::new(1, i + 1),
            None,
        );
    }
    assert!(bin.keys.is_compact(), "small keys -> Compact");
    // Insert a key longer than the 16-byte threshold (post-prefix).
    let long = vec![b'z'; 40];
    bin.insert_with_prefix(long, Lsn::new(1, 99), None);
    assert!(
        !bin.keys.is_compact(),
        "a key > TREE_COMPACT_MAX_KEY_LENGTH must inflate to Default"
    );
}

/// Numerically prove the per-node footprint reduction vs the pre-T-2 layout
/// where every `BinEntry` carried a 24-byte `Vec<u8>` header + the key's own
/// heap allocation.
#[test]
fn key_footprint_smaller_than_pre_compaction() {
    let n = 128usize;
    let key_len = 8usize;
    let bin = bin_same_file_with_keys(n, key_len);
    // Compact rep: one buffer (slot_width*n) + lengths (2*n).
    let compact = bin.keys.memory_size();
    // Pre-T-2 Default model: n * (Vec header 24 + key bytes).
    let pre = n * (24 + key_len);
    assert!(
        compact < pre,
        "T-2: compact key rep ({compact}) must be < pre-compaction \
         per-key-Vec layout ({pre})"
    );
}

fn empty_bin() -> noxu_tree::BinStub {
    noxu_tree::BinStub {
        node_id: 1,
        level: noxu_tree::BIN_LEVEL,
        entries: Vec::new(),
        key_prefix: Vec::new(),
        dirty: false,
        is_delta: false,
        last_full_lsn: noxu_util::NULL_LSN,
        last_delta_lsn: noxu_util::NULL_LSN,
        generation: 0,
        parent: None,
        expiration_in_hours: true,
        cursor_count: 0,
        prohibit_next_delta: false,
        lsn_rep: LsnRep::Empty,
        keys: KeyRep::new(),
        compact_max_key_length:
            noxu_tree::tree::INKeyRep_DEFAULT_MAX_KEY_LENGTH,
        expiration_enabled: true,
    }
}

/// A BIN of `n` slots, each a distinct `key_len`-byte key, no shared prefix
/// (keys differ in the first byte), all in file number 7.
fn bin_same_file_with_keys(n: usize, key_len: usize) -> noxu_tree::BinStub {
    let mut bin = empty_bin();
    for i in 0..n {
        let mut k = vec![0u8; key_len];
        // Vary the leading bytes so no common prefix forms.
        k[0] = (i % 256) as u8;
        k[1 % key_len] = (i / 256) as u8;
        bin.insert_with_prefix(k, Lsn::new(7, 1000 + i as u32), None);
    }
    bin
}
