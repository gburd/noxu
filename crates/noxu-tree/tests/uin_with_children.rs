//! BUG-EVICT-UIN-WITH-CHILDREN — deterministic tree-level witness.
//!
//! JE invariant: *an upper IN (UIN) with resident children is never evicted.*
//! JE enforces it in `Evictor.processTarget` (Evictor.java:2645-2656) by
//! re-checking `hasCachedChildrenFlag()` UNDER the target's EX latch, with the
//! comment that a childless UIN may be *selected* for eviction and then
//! acquire cached children before the evicting thread EX-latches it.
//!
//! Noxu's evictor decides eviction from a PRE-latch residency snapshot
//! (`decide_eviction` reads `has_cached_children` from `find_node_full` under
//! a tree READ latch), then detaches via `Tree::detach_node_by_id`.  In the
//! gap between the snapshot and the detach a descent can fault a BIN back in
//! under the selected UIN (`child_at_or_fetch`).  Before the fix,
//! `detach_node_by_id` had NO upper-IN childlessness re-check, so it would
//! `take_child` a UIN out of its parent slot WHILE it still pointed at a
//! resident child — orphaning the live child under a detached ZOMBIE parent.
//!
//! This witness reproduces that TOCTOU deterministically at the tree level
//! (no threads): make a lowest UIN childless, then — modelling the fault that
//! races the evictor between selection and detach — re-fault a BIN under it,
//! then call `detach_node_by_id(uin)` and assert it is REFUSED (the UIN stays
//! attached and the child stays reachable root-down).  A control detaches a
//! genuinely childless UIN to prove the fix does not over-refuse.
//!
//! RED on the base (re-check removed): `detach_node_by_id` returns > 0 and the
//! UIN is detached with a resident child (zombie parent).  GREEN after: it
//! returns 0 and the UIN stays attached.

use noxu_sync::RwLock;
use noxu_tree::tree::{Tree, TreeNode};
use noxu_util::{Lsn, NULL_LSN};
use std::sync::Arc;

const MAIN_LEVEL: i32 = 0x10000;
/// A lowest upper IN (direct parent of BINs) has level `MAIN_LEVEL | 2`.
const LOWEST_UIN_LEVEL: i32 = MAIN_LEVEL | 2;

fn make_log(dir: &std::path::Path) -> Arc<noxu_log::LogManager> {
    let fm = Arc::new(
        noxu_log::FileManager::new(dir, false, 10_000_000, 100).unwrap(),
    );
    Arc::new(noxu_log::LogManager::new(fm, 3, 1024 * 1024, 4096))
}

/// Depth-first search of the resident tree for the arc with `node_id`.
fn find_arc(
    arc: &Arc<RwLock<TreeNode>>,
    node_id: u64,
) -> Option<Arc<RwLock<TreeNode>>> {
    let g = arc.read();
    if g.node_id() == node_id {
        return Some(arc.clone());
    }
    if let TreeNode::Internal(n) = &*g {
        for c in n.resident_children() {
            if let Some(a) = find_arc(&c, node_id) {
                return Some(a);
            }
        }
    }
    None
}

/// Collect `(uin_id, bin_child_ids, a_key_under_the_uin)` for every lowest UIN
/// (direct BIN parent) with at least two resident BIN children.
fn lowest_uins(
    arc: &Arc<RwLock<TreeNode>>,
    out: &mut Vec<(u64, Vec<u64>, Vec<u8>)>,
) {
    let g = arc.read();
    if let TreeNode::Internal(n) = &*g {
        if n.level == LOWEST_UIN_LEVEL {
            let mut bins = Vec::new();
            let mut a_key = None;
            for c in n.resident_children() {
                let cg = c.read();
                if let TreeNode::Bottom(b) = &*cg {
                    bins.push(b.node_id);
                    if a_key.is_none() {
                        a_key = b.get_full_key(0);
                    }
                }
            }
            if bins.len() >= 2 {
                out.push((n.node_id, bins, a_key.unwrap()));
            }
        } else {
            for c in n.resident_children() {
                lowest_uins(&c, out);
            }
        }
    }
}

/// Log a resident BIN as a full-BIN image, stamp `last_full_lsn`, publish the
/// logged LSN into its parent slot, and detach it — exactly as the evictor's
/// `flush_dirty_node_to_log` + `detach_node_by_id` do.
fn log_and_detach_bin(tree: &Tree, lm: &noxu_log::LogManager, bin_id: u64) {
    let root = tree.get_root().expect("root");
    let bin_arc = find_arc(&root, bin_id).expect("bin arc");
    let payload = {
        let g = bin_arc.read();
        match &*g {
            TreeNode::Bottom(b) => b.serialize_full(),
            _ => unreachable!("expected a BIN"),
        }
    };
    let entry = noxu_log::entry::in_log_entry::InLogEntry::new(
        1, NULL_LSN, NULL_LSN, payload,
    );
    let mut buf = bytes::BytesMut::with_capacity(entry.log_size());
    entry.write_to_log(&mut buf);
    let lsn = lm
        .log(
            noxu_log::LogEntryType::BIN,
            &buf,
            noxu_log::Provisional::No,
            true,
            false,
        )
        .expect("log BIN");
    {
        let mut g = bin_arc.write();
        if let TreeNode::Bottom(b) = &mut *g {
            b.clear_dirty_after_full_log(lsn);
        }
    }
    Tree::update_parent_slot_lsn(&bin_arc, lsn);
    let freed = tree.detach_node_by_id(bin_id);
    assert!(freed > 0, "setup: BIN {bin_id} must detach");
}

/// Returns the number of resident children of the (resident) UIN `uin_id`.
fn resident_child_count(tree: &Tree, uin_id: u64) -> usize {
    let root = tree.get_root().expect("root");
    let arc = find_arc(&root, uin_id).expect("uin arc still resident");
    let g = arc.read();
    match &*g {
        TreeNode::Internal(n) => n.targets.resident_count(),
        TreeNode::Bottom(_) => panic!("{uin_id} is not a UIN"),
    }
}

/// HEADLINE: the evictor selects a childless UIN, a child is faulted in under
/// it before detach, and `detach_node_by_id` must REFUSE (the UIN stays
/// attached and the faulted child is reachable root-down).
#[test]
fn detach_refuses_uin_that_reacquired_a_child() {
    let dir = tempfile::tempdir().unwrap();
    let lm = make_log(dir.path());
    let mut tree = Tree::new(1, 4); // small fanout => a 3+-level tree
    tree.set_log_manager(Arc::clone(&lm));

    // Build a multi-level tree with several lowest UINs over BINs.
    for i in 0u16..40 {
        let k = vec![(i >> 8) as u8, (i & 0xff) as u8];
        tree.insert(k, vec![i as u8], Lsn::new(1, i as u32)).unwrap();
    }

    // Find lowest UINs (direct BIN parents) with >= 2 resident BIN children.
    let root = tree.get_root().unwrap();
    let mut uins = Vec::new();
    lowest_uins(&root, &mut uins);
    assert!(
        uins.len() >= 2,
        "test fixture must have >= 2 lowest UINs; got {}",
        uins.len()
    );

    // Target UIN: make it childless by logging + detaching ALL its BINs.
    let (target_uin, target_bins, key_under_target) = uins[0].clone();
    for bin_id in &target_bins {
        log_and_detach_bin(&tree, &lm, *bin_id);
    }
    lm.flush_no_sync().expect("flush");
    assert_eq!(
        resident_child_count(&tree, target_uin),
        0,
        "setup: target UIN {target_uin} must be childless (evictor-selectable)"
    );

    // ---- The TOCTOU: between the evictor's childless snapshot and the detach,
    // a concurrent descent faults a BIN back in under the target UIN. We model
    // that fault deterministically with a search routed under the UIN. ----
    let fetched = tree
        .search_with_data(&key_under_target)
        .expect("search must route under the target UIN and re-fault a BIN");
    assert!(
        fetched.found,
        "the key under the UIN must be found after re-fault"
    );
    assert_eq!(
        resident_child_count(&tree, target_uin),
        1,
        "a BIN must now be resident under the target UIN (the raced fault)"
    );

    // ---- detach_node_by_id of the UIN must REFUSE (return 0). ----
    let freed = tree.detach_node_by_id(target_uin);
    assert_eq!(
        freed, 0,
        "detach_node_by_id must REFUSE a UIN that re-acquired a resident \
         child (BUG-EVICT-UIN-WITH-CHILDREN); detaching it orphans the child \
         under a zombie parent"
    );

    // The UIN stays attached and resident.
    assert_eq!(
        resident_child_count(&tree, target_uin),
        1,
        "the refused UIN must stay resident with its re-faulted child"
    );

    // The faulted child is reachable ROOT-DOWN (not orphaned): the key under
    // the UIN still resolves through a normal descent.
    let after = tree
        .search_with_data(&key_under_target)
        .expect("the child must stay reachable root-down after the refusal");
    assert!(after.found, "the key must still be found via root-down descent");
    assert_eq!(after.data, fetched.data, "and return the same value");
}

/// CONTROL: a genuinely childless lowest UIN still detaches normally — the fix
/// must not over-refuse. We detach ALL BINs under a second UIN and leave it
/// childless (no re-fault), then detach the UIN itself.
#[test]
fn detach_allows_truly_childless_uin() {
    let dir = tempfile::tempdir().unwrap();
    let lm = make_log(dir.path());
    let mut tree = Tree::new(1, 4);
    tree.set_log_manager(Arc::clone(&lm));
    for i in 0u16..40 {
        let k = vec![(i >> 8) as u8, (i & 0xff) as u8];
        tree.insert(k, vec![i as u8], Lsn::new(1, i as u32)).unwrap();
    }
    let root = tree.get_root().unwrap();
    let mut uins = Vec::new();
    lowest_uins(&root, &mut uins);
    assert!(!uins.is_empty(), "need at least one lowest UIN");

    // Use the LAST lowest UIN as the control (independent of the headline).
    let (ctrl_uin, ctrl_bins, _) = uins.last().unwrap().clone();
    for bin_id in &ctrl_bins {
        log_and_detach_bin(&tree, &lm, *bin_id);
    }
    lm.flush_no_sync().expect("flush");
    assert_eq!(
        resident_child_count(&tree, ctrl_uin),
        0,
        "control UIN {ctrl_uin} must be childless"
    );

    // The UIN itself must have a durable image before detach; log + publish it
    // the same way the evictor's upper-IN flush does.
    let uin_arc = find_arc(&tree.get_root().unwrap(), ctrl_uin).unwrap();
    let payload = {
        let g = uin_arc.read();
        match &*g {
            TreeNode::Internal(_) => g.write_to_bytes(),
            _ => unreachable!(),
        }
    };
    let entry = noxu_log::entry::in_log_entry::InLogEntry::new(
        1, NULL_LSN, NULL_LSN, payload,
    );
    let mut buf = bytes::BytesMut::with_capacity(entry.log_size());
    entry.write_to_log(&mut buf);
    let lsn = lm
        .log(
            noxu_log::LogEntryType::IN,
            &buf,
            noxu_log::Provisional::No,
            true,
            false,
        )
        .expect("log IN");
    {
        let mut g = uin_arc.write();
        g.set_dirty(false);
    }
    Tree::update_parent_slot_lsn(&uin_arc, lsn);
    drop(uin_arc);

    let freed = tree.detach_node_by_id(ctrl_uin);
    assert!(
        freed > 0,
        "control: a truly childless UIN must detach (the fix must not \
         over-refuse)"
    );
}

/// DURABILITY TAIL: after the refusal, a value written into the (still-live)
/// child survives being logged (checkpoint), evicted (its BIN detached), and
/// re-fetched from the log via a ROOT-DOWN descent — proving the child was not
/// orphaned.  On the base (UIN detached), the child's parent is the detached
/// zombie, so the root-down publication of the child's logged image cannot
/// reach the live grandparent slot; after the child BIN is evicted, the live
/// tree re-fetches a stale image and the committed value is gone.
///
/// This drives the PRODUCTION `detach_node_by_id(uin)` (not the forcing seam):
/// the FIX refuses it (child stays live), the BASE detaches it (child
/// orphaned), and only the refusal keeps the value durable.
#[test]
fn committed_child_write_survives_log_roundtrip_after_refusal() {
    let dir = tempfile::tempdir().unwrap();
    let lm = make_log(dir.path());
    let mut tree = Tree::new(1, 4);
    tree.set_log_manager(Arc::clone(&lm));
    for i in 0u16..40 {
        let k = vec![(i >> 8) as u8, (i & 0xff) as u8];
        tree.insert(k, vec![i as u8], Lsn::new(1, i as u32)).unwrap();
    }

    let root = tree.get_root().unwrap();
    let mut uins = Vec::new();
    lowest_uins(&root, &mut uins);
    let (target_uin, target_bins, key_under_target) = uins[0].clone();

    // Make the UIN childless, then re-fault one BIN under it (the race).
    for bin_id in &target_bins {
        log_and_detach_bin(&tree, &lm, *bin_id);
    }
    lm.flush_no_sync().unwrap();
    let child_bin =
        tree.search_with_data(&key_under_target).expect("re-fault").bin_arc;
    let child_id = child_bin.read().node_id();

    // The evictor reaches detach of the UIN. FIX refuses; BASE detaches.
    let _ = tree.detach_node_by_id(target_uin);

    // Write a NEW committed value directly into the (still-resident) child
    // slot that `key_under_target` covers — a mutation that lands regardless
    // of the UIN detach state (the child Arc is alive on both base and fix).
    {
        let mut g = child_bin.write();
        if let TreeNode::Bottom(b) = &mut *g {
            let (slot, exact) = b.find_entry_compressed(&key_under_target);
            assert!(exact, "the key must have an exact slot in the child BIN");
            b.entries[slot].data =
                Some(bytes::Bytes::from_static(b"V2-COMMITTED"));
            b.dirty = true;
        } else {
            panic!("child must be a BIN");
        }
    }

    // Log the child BIN (checkpoint) and publish ITS LSN into its parent slot
    // ROOT-DOWN (JE getParentINForChildIN — the fix's publication path).  On
    // the fix the child's parent is the live UIN, so this stamps the real
    // grandparent path; on the base the child is orphaned, so the root-down
    // resolve finds no live parent and the live slot keeps a STALE image LSN.
    let payload = match &*child_bin.read() {
        TreeNode::Bottom(b) => b.serialize_full(),
        _ => unreachable!(),
    };
    let entry = noxu_log::entry::in_log_entry::InLogEntry::new(
        1, NULL_LSN, NULL_LSN, payload,
    );
    let mut buf = bytes::BytesMut::with_capacity(entry.log_size());
    entry.write_to_log(&mut buf);
    let lsn = lm
        .log(
            noxu_log::LogEntryType::BIN,
            &buf,
            noxu_log::Provisional::No,
            true,
            false,
        )
        .unwrap();
    if let TreeNode::Bottom(b) = &mut *child_bin.write() {
        b.clear_dirty_after_full_log(lsn);
    }
    tree.update_parent_slot_lsn_rootdown(&child_bin, lsn);
    lm.flush_no_sync().unwrap();
    drop(child_bin);

    // Evict the child BIN (allowed — it is a BIN), then re-fetch ROOT-DOWN.
    let _ = tree.detach_node_by_id(child_id);

    // The committed value must round-trip: a fresh root-down descent re-fetches
    // the child from the log at the slot LSN the publication stamped.
    let after = tree
        .search_with_data(&key_under_target)
        .expect("the key must resolve root-down after the log round-trip");
    assert!(
        after.found,
        "DATA-LOSS: the committed child write did not survive the log \
         round-trip — the child was orphaned under a zombie parent \
         (BUG-EVICT-UIN-WITH-CHILDREN)"
    );
    assert_eq!(
        after.data.as_deref(),
        Some(b"V2-COMMITTED".as_slice()),
        "DATA-LOSS: the re-fetched value is STALE — the committed write to the \
         child was lost because the live grandparent slot was never stamped \
         with the fresh image (BUG-EVICT-UIN-WITH-CHILDREN)"
    );
}
