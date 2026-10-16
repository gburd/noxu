// BUG-EVICT-UIN-WITH-CHILDREN: this witness drives the evictor pre-detach
// hook, which only exists under the `testing` feature; skip it under the
// default-feature `cargo test --workspace`.
#![cfg(feature = "testing")]
//! BUG-EVICT-UIN-WITH-CHILDREN — REAL-evictor TOCTOU witness.
//!
//! Drives the production evict batch (`Evictor::do_evict`) end to end and
//! reproduces the selection->detach race JE closes in `Evictor.processTarget`
//! (Evictor.java:2645-2656): a childless upper IN is SELECTED for eviction
//! from a pre-latch residency snapshot, then — in the window before the
//! evictor latches it to detach — a descent faults a child back in under it.
//!
//! Noxu's evictor reads `has_cached_children` from a pre-latch
//! `find_node_full` snapshot in `do_evict`'s `node_info_fn`, then detaches via
//! `node_size_fn` -> `Tree::detach_node_by_id`.  We model the raced fault with
//! the test-only `pre_detach_hook`, fired in `node_size_fn` with the candidate
//! node id IMMEDIATELY BEFORE the detach: the hook `search`es a key routed
//! under the selected UIN, which `child_at_or_fetch`-installs a resident BIN
//! back under it — exactly the state JE's under-latch re-check skips.
//!
//! RED on the base (UIN re-check removed): the real evictor detaches the UIN
//! WITH its re-faulted resident child — a zombie parent, the data-loss hole.
//! GREEN after: `detach_node_by_id` refuses under the parent latch, the UIN
//! stays attached, and the child stays reachable root-down.

use noxu_evictor::{Arbiter, CacheMode, EvictionSource, Evictor};
use noxu_tree::tree::{Tree, TreeNode};
use noxu_util::Lsn;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, RwLock};

fn find_arc(
    arc: &Arc<noxu_sync::RwLock<TreeNode>>,
    id: u64,
) -> Option<Arc<noxu_sync::RwLock<TreeNode>>> {
    let g = arc.read();
    if g.node_id() == id {
        return Some(arc.clone());
    }
    if let TreeNode::Internal(n) = &*g {
        for c in n.resident_children() {
            if let Some(a) = find_arc(&c, id) {
                return Some(a);
            }
        }
    }
    None
}

/// Log + detach a specific BIN so its parent upper IN loses a resident child
/// (mirrors the evictor's flush_dirty_node_to_log + detach_node_by_id).
fn log_and_detach_bin(
    tree: &Tree,
    root: &Arc<noxu_sync::RwLock<TreeNode>>,
    lm: &noxu_log::LogManager,
    bin_id: u64,
) {
    let bin = find_arc(root, bin_id).expect("bin");
    let payload = match &*bin.read() {
        TreeNode::Bottom(b) => b.serialize_full(),
        _ => panic!("not a BIN"),
    };
    let entry = noxu_log::entry::in_log_entry::InLogEntry::new(
        1,
        Lsn::from_u64(0),
        Lsn::from_u64(0),
        payload,
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
    if let TreeNode::Bottom(b) = &mut *bin.write() {
        b.clear_dirty_after_full_log(lsn);
    }
    Tree::update_parent_slot_lsn(&bin, lsn);
    assert!(tree.detach_node_by_id(bin_id) > 0, "setup: detach bin {bin_id}");
}

/// Find a NON-ROOT lowest upper IN (direct BIN parent), returning its id, its
/// BIN children ids, and a key the first child covers (to route a re-fault).
fn find_nonroot_lowest_uin(tree: &Tree) -> Option<(u64, Vec<u64>, Vec<u8>)> {
    let root = tree.get_root()?;
    fn walk(
        arc: &Arc<noxu_sync::RwLock<TreeNode>>,
        is_root: bool,
    ) -> Option<(u64, Vec<u64>, Vec<u8>)> {
        let g = arc.read();
        let TreeNode::Internal(n) = &*g else { return None };
        let my_id = n.node_id;
        let children = n.resident_children();
        let all_bins = !children.is_empty()
            && children
                .iter()
                .all(|c| matches!(&*c.read(), TreeNode::Bottom(_)));
        if !is_root && all_bins {
            let mut bins = Vec::new();
            let mut a_key = None;
            for c in &children {
                if let TreeNode::Bottom(b) = &*c.read() {
                    bins.push(b.node_id);
                    if a_key.is_none() {
                        a_key = b.get_full_key(0);
                    }
                }
            }
            return Some((my_id, bins, a_key.unwrap()));
        }
        drop(g);
        for c in children {
            if let Some(found) = walk(&c, false) {
                return Some(found);
            }
        }
        None
    }
    walk(&root, true)
}

fn resident_child_count(tree: &Tree, uin_id: u64) -> usize {
    let root = tree.get_root().expect("root");
    match find_arc(&root, uin_id) {
        Some(arc) => match &*arc.read() {
            TreeNode::Internal(n) => n.targets.resident_count(),
            TreeNode::Bottom(_) => panic!("{uin_id} is not a UIN"),
        },
        None => 0, // detached from the live tree
    }
}

#[test]
fn real_evictor_refuses_uin_that_reacquired_a_child_before_detach() {
    let dir = tempfile::tempdir().unwrap();
    let fm = Arc::new(
        noxu_log::FileManager::new(dir.path(), false, 10_000_000, 100).unwrap(),
    );
    let lm = Arc::new(noxu_log::LogManager::new(fm, 3, 1024 * 1024, 4096));

    let counter = Arc::new(AtomicI64::new(0));
    // Arbiter with a tiny budget so `is_over_budget()` is true and the Manual
    // path proceeds; Manual eviction is proactive regardless.
    let arbiter = Arbiter::new(1, Arc::clone(&counter), 1, 0);
    let evictor = Arc::new(
        Evictor::new(arbiter, 64, false).with_log_manager(Arc::clone(&lm)),
    );

    let mut tree = Tree::new(1, 4); // small fanout => multi-level tree
    tree.set_log_manager(Arc::clone(&lm));
    tree.set_memory_counter(Arc::clone(&counter));
    let tree_arc = Arc::new(RwLock::new(tree));
    evictor.set_tree(Arc::clone(&tree_arc), 1);

    // Build a multi-level tree and pick a non-root lowest UIN.
    let (uin_id, bin_ids, key_under_uin) = {
        let t = tree_arc.read().unwrap();
        for i in 0u16..40 {
            let k = vec![(i >> 8) as u8, (i & 0xff) as u8];
            t.insert(k, vec![i as u8], Lsn::new(1, u32::from(i) + 1)).unwrap();
        }
        find_nonroot_lowest_uin(&t)
            .expect("fixture: need a non-root lowest UIN with BIN children")
    };

    // Make the UIN childless so the evictor SELECTS it (its pre-latch snapshot
    // sees has_cached_children == false).
    {
        let t = tree_arc.read().unwrap();
        let root = t.get_root().unwrap();
        for bid in &bin_ids {
            log_and_detach_bin(&t, &root, &lm, *bid);
        }
        lm.flush_no_sync().expect("flush");
    }
    assert_eq!(
        resident_child_count(&tree_arc.read().unwrap(), uin_id),
        0,
        "setup: the UIN must be childless (evictor-selectable)"
    );

    // ---- The TOCTOU: install a pre-detach hook that, between the evictor's
    // childless snapshot and the detach, faults a BIN back in under the
    // selected UIN (what a racing descent would do). ----
    let faulted = Arc::new(AtomicI64::new(0));
    {
        let tree_h = Arc::clone(&tree_arc);
        let key_h = key_under_uin.clone();
        let uin_h = uin_id;
        let faulted_h = Arc::clone(&faulted);
        evictor.set_pre_detach_hook_for_test(Box::new(move |node_id: u64| {
            // Only fire for the UIN we are targeting, and only once.
            if node_id == uin_h && faulted_h.load(Ordering::Relaxed) == 0 {
                let t = tree_h.read().unwrap();
                let r = t.search_with_data(&key_h);
                assert!(
                    r.map(|r| r.found).unwrap_or(false),
                    "hook: the raced descent must re-fault a BIN under the UIN"
                );
                faulted_h.store(1, Ordering::Relaxed);
            }
        }));
    }

    // Prime the LRU with the UIN and run the REAL evict batch repeatedly.
    evictor.note_ins_added(uin_id, CacheMode::Default);
    for _ in 0..20 {
        evictor.do_evict(EvictionSource::Manual);
        // Stop once the hook has fired (the race has been exercised).
        if faulted.load(Ordering::Relaxed) == 1 {
            break;
        }
        evictor.note_ins_added(uin_id, CacheMode::Default);
    }

    assert_eq!(
        faulted.load(Ordering::Relaxed),
        1,
        "the pre-detach hook must have fired (the UIN was selected and the \
         child race was exercised through the real evict batch)"
    );

    // HEADLINE: the UIN must NOT have been detached — it re-acquired a
    // resident child in the race, and the under-latch re-check refuses.
    assert!(
        find_arc(&tree_arc.read().unwrap().get_root().unwrap(), uin_id)
            .is_some(),
        "DATA-LOSS DEFECT: the real evictor detached a UIN that re-acquired a \
         resident child between selection and detach (BUG-EVICT-UIN-WITH-\
         CHILDREN) — the child is orphaned under a zombie parent"
    );
    assert_eq!(
        resident_child_count(&tree_arc.read().unwrap(), uin_id),
        1,
        "the refused UIN must stay resident with its re-faulted child"
    );

    // The faulted child is reachable ROOT-DOWN (not orphaned).
    let after =
        tree_arc.read().unwrap().search_with_data(&key_under_uin).expect(
            "the child must stay reachable root-down after the refusal",
        );
    assert!(after.found, "the key under the UIN must still resolve root-down");
}
