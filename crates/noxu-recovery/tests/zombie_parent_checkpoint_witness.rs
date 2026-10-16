// rv-recon: witness needs the `testing` seam; skipped under
// default-feature `cargo test --workspace`.
#![cfg(feature = "testing")]
//! rv-recon zombie-parent witness (coordinator item 3).
//!
//! A checkpoint snapshots a dirty BIN B (reachable under its parent P at
//! snapshot time), then the evictor detaches P (an upper-IN eviction that
//! leaves B resident — the TOCTOU JE closes with hasCachedChildren +
//! processTarget re-check, which Noxu does not fully replicate). B's `parent`
//! back-pointer still names the detached zombie P. When the checkpointer's
//! per-BIN flush loop reaches B:
//!   * BUGGY (back-pointer resolution): `parent_arc_of_child(B)` returns the
//!     zombie P, the ptr_eq identity check passes, and the checkpointer LOGS B
//!     at a NEW LSN and stamps the zombie — a stale image recovery redo can
//!     resurrect over the committed value (Probe C mechanism).
//!   * FIXED (root-down resolution): `get_parent_in_for_child_in(B)` searches
//!     from the live root, does NOT find B (its parent is detached), so the
//!     checkpointer SKIPS it (JE `!exactParentFound -> return`).
//!
//! RED on a0a62ef4, GREEN with the merge-prep/recon root-down fix.
//!
//! Drives the REAL per-BIN flush loop via `flush_bins_list_for_test` (the loop
//! body is shared verbatim with production `flush_one_tree_bins`).
use noxu_log::entry::ln_log_entry::LnLogEntry;
use noxu_log::{FileManager, LogEntryType, LogManager, Provisional};
use noxu_recovery::{CheckpointConfig, Checkpointer};
use noxu_tree::NodeRwLock;
use noxu_tree::tree::{Tree, TreeNode};
use noxu_util::{NULL_LSN, Vlsn};
use std::sync::{Arc, RwLock as StdRwLock};

fn insert_logged(
    lm: &Arc<LogManager>,
    tree: &Tree,
    key: &[u8],
    val: &[u8],
    db: u64,
) {
    let entry = LnLogEntry::new(
        db,
        None,
        NULL_LSN,
        false,
        None,
        None,
        Vlsn::new(-1),
        0,
        true,
        key.to_vec(),
        Some(val.to_vec()),
        0,
        Vlsn::new(-1),
    );
    let mut buf = bytes::BytesMut::new();
    entry.write_to_log(&mut buf);
    let lsn = lm
        .log(LogEntryType::InsertLN, &buf, Provisional::No, true, false)
        .unwrap();
    tree.insert(key.to_vec(), val.to_vec(), lsn).unwrap();
}

#[test]
fn checkpointer_must_not_flush_a_zombie_parented_bin() {
    let dir = tempfile::tempdir().unwrap();
    let fm =
        Arc::new(FileManager::new(dir.path(), false, 10_000_000, 100).unwrap());
    let lm = Arc::new(LogManager::new(fm, 3, 1024 * 1024, 4096));

    // 3-level tree so a BIN B has a non-root parent P and a grandparent G.
    let mut tree_inner = Tree::new(1, 2);
    tree_inner.set_log_manager(Arc::clone(&lm));
    for i in 0u16..32 {
        insert_logged(&lm, &tree_inner, &i.to_be_bytes(), b"LIVE", 1);
    }
    let tree_arc = Arc::new(StdRwLock::new(tree_inner));

    // Identify B (holds key 0) and its parent P.
    let (b_arc, b_id, p_id) = {
        let t = tree_arc.read().unwrap();
        let b = t.search_with_data(&0u16.to_be_bytes()).unwrap().bin_arc;
        let b_id = b.read().node_id();
        let (p, _s) =
            t.get_parent_in_for_child_in(b_id).expect("B has parent P");
        (b, b_id, p.read().node_id())
    };

    // SNAPSHOT the dirty-BIN set NOW, while B is reachable under P (exactly
    // what the checkpointer does under the tree read latch).  Make B dirty
    // first so it is collected.
    {
        let mut g = b_arc.write();
        if let TreeNode::Bottom(b) = &mut *g {
            b.dirty = true;
        }
    }
    let snapshot: Vec<(u64, Arc<NodeRwLock<TreeNode>>)> =
        tree_arc.read().unwrap().collect_dirty_bins(1);
    assert!(
        snapshot.iter().any(|(_, a)| a.read().node_id() == b_id),
        "fixture: B must be in the dirty-BIN snapshot"
    );

    // Hold a STRONG Arc to the zombie parent P so it stays Arc-alive after
    // detach (models a concurrent descender / in-flight ref). Then DETACH P
    // from its grandparent via the production detach path — the evictor
    // upper-IN eviction, which does NOT re-check that P is childless. B stays
    // resident in P and B.parent still names P.
    let p_strong = Tree::parent_arc_of_child(&b_arc).expect("B.parent -> P");
    assert_eq!(p_strong.read().node_id(), p_id);
    // BUG-EVICT-UIN-WITH-CHILDREN: the production `detach_node_by_id` now
    // REFUSES to detach a UIN with a resident child, so the zombie this
    // witness needs can no longer be manufactured through the real path.
    // Use the test-only forcing seam (bypasses ONLY the new UIN re-check)
    // to create the detached-but-Arc-alive parent and exercise the
    // checkpointer's root-down flush skip (defense in depth) against a
    // state production can no longer reach.
    let freed = tree_arc
        .read()
        .unwrap()
        .detach_node_by_id_bypassing_uin_guard_for_test(p_id);
    assert!(
        freed > 0,
        "fixture: the evictor path must detach P (UIN w/ resident child)"
    );

    // Confirm the zombie state the checkpointer will encounter: P is gone from
    // the live tree, but B.parent still names the Arc-alive zombie P.
    assert!(
        tree_arc.read().unwrap().get_parent_in_for_child_in(b_id).is_none(),
        "P must be detached from the live root (root-down resolve finds nothing)"
    );
    assert!(
        Tree::parent_arc_of_child(&b_arc)
            .map(|p| Arc::ptr_eq(&p, &p_strong))
            .unwrap_or(false),
        "B's back-pointer must still name the zombie P"
    );

    // Corrupt B's in-memory image to a STALE value (what a pre-detach image
    // would carry); keep it dirty so the loop would log it.
    {
        let mut g = b_arc.write();
        if let TreeNode::Bottom(b) = &mut *g {
            for e in b.entries.iter_mut() {
                e.data = Some(bytes::Bytes::from_static(b"STALE"));
            }
            b.dirty = true;
        }
    }
    let b_full_before = {
        let g = b_arc.read();
        if let TreeNode::Bottom(b) = &*g { b.last_full_lsn } else { NULL_LSN }
    };

    // Drive the REAL per-BIN flush loop with the snapshot (which still holds B).
    let ckpt = Checkpointer::new(CheckpointConfig::default())
        .with_log_manager(Arc::clone(&lm))
        .with_tree(Arc::clone(&tree_arc), 1);
    ckpt.flush_bins_list_for_test(1, &tree_arc, snapshot)
        .expect("flush_bins_list_for_test");

    let b_full_after = {
        let g = b_arc.read();
        if let TreeNode::Bottom(b) = &*g { b.last_full_lsn } else { NULL_LSN }
    };
    let flushed_zombie =
        b_full_after != b_full_before && b_full_after != NULL_LSN;
    // Also: did the checkpointer stamp the zombie P's slot?
    let zombie_stamped = {
        let g = p_strong.read();
        if let TreeNode::Internal(p) = &*g {
            (0..p.entries.len()).any(|s| {
                p.child_ref(s).map(|c| Arc::ptr_eq(c, &b_arc)).unwrap_or(false)
                    && p.get_lsn(s) == b_full_after
            })
        } else {
            false
        }
    };
    eprintln!(
        "WITNESS: flushed_zombie={flushed_zombie} (b_full {b_full_before:?} -> {b_full_after:?}), zombie_stamped={zombie_stamped}"
    );

    assert!(
        !flushed_zombie,
        "DATA-LOSS DEFECT: the checkpointer LOGGED a detached (zombie) BIN at a new LSN \
         ({b_full_after:?}) — a stale image recovery redo can resurrect over the committed \
         value. The reconcile's back-pointer parent resolution does not detect a \
         detached-but-Arc-alive parent. FIX: root-down get_parent_in_for_child_in."
    );
    assert!(
        !zombie_stamped,
        "the checkpointer stamped the detached zombie parent's slot with the new LSN"
    );
    let _ = (p_strong,);
}
