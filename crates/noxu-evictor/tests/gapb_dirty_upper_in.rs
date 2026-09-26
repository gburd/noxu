//! GAP B (EVICTOR-UPPER-IN-1) evictor-level integration: a real Evictor wired
//! to a real Tree + LogManager must LOG a dirty *upper* IN before detaching it,
//! publishing the fresh logged LSN into the grandparent slot — exactly as JE
//! `Evictor.evict` logs ANY dirty target before `parent.detachNode(...)`
//! (Evictor.java:3013-3035, IN.detachNode IN.java:4019-4027).
//!
//! Bug (base, before EVICTOR-UPPER-IN-1): `flush_dirty_node_to_log` returned
//! `true` for a non-BIN node WITHOUT logging it, and `detach_node_by_id` forced
//! `child_full_lsn = NULL` for an Internal child so the grandparent slot kept
//! its pre-change LSN. A dirty upper IN carrying an unlogged structural change
//! was therefore dropped while the grandparent still pointed at the stale
//! on-disk image — the change is lost on a crash before the next checkpoint.
//!
//! This drives the REAL production path deterministically: build a multi-level
//! tree, make a non-root upper IN childless (detach its BINs) and DIRTY
//! (an unlogged structural mutation), feed it to the evictor's LRU, and evict.
//! The evictor routes the dirty childless upper IN to
//! `flush_dirty_node_to_log`, which now calls `log_dirty_upper_in` (write an
//! `InLogEntry` as `LogEntryType::IN`, clear dirty, publish the fresh LSN into
//! the grandparent slot). We assert the grandparent slot LSN ADVANCED to a
//! real WAL LSN (on base it would stay the stale pre-change LSN) and that the
//! surviving keys still re-fetch correctly through the tree's own descent.

use noxu_evictor::{Arbiter, CacheMode, EvictionSource, Evictor};
use noxu_tree::tree::{Tree, TreeNode};
use noxu_util::Lsn;
use std::sync::atomic::AtomicI64;
use std::sync::{Arc, RwLock};

fn make_log(dir: &std::path::Path) -> Arc<noxu_log::LogManager> {
    let fm = Arc::new(
        noxu_log::FileManager::new(dir, false, 10_000_000, 100).unwrap(),
    );
    Arc::new(noxu_log::LogManager::new(fm, 3, 1024 * 1024, 4096))
}

fn find_arc(
    arc: &Arc<noxu_sync::RwLock<TreeNode>>,
    id: u64,
) -> Option<Arc<noxu_sync::RwLock<TreeNode>>> {
    let g = arc.read();
    let this_id = match &*g {
        TreeNode::Bottom(b) => b.node_id,
        TreeNode::Internal(n) => n.node_id,
    };
    if this_id == id {
        return Some(arc.clone());
    }
    match &*g {
        TreeNode::Bottom(_) => None,
        TreeNode::Internal(n) => {
            for c in n.resident_children() {
                if let Some(a) = find_arc(&c, id) {
                    return Some(a);
                }
            }
            None
        }
    }
}

/// Log + detach a specific BIN so its parent upper IN loses a resident child.
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
    assert!(tree.detach_node_by_id(bin_id) > 0, "detach bin {bin_id}");
}

/// Find a NON-ROOT upper IN that has at least one BIN child, returning its id,
/// its BIN children ids, and the id of its own parent (the grandparent of the
/// BINs).  Skips the root (EV-7).
fn find_nonroot_upper_in(
    tree: &Tree,
) -> Option<(u64, Vec<u64>, u64 /* grandparent id */)> {
    let root = tree.get_root()?;
    fn walk(
        arc: &Arc<noxu_sync::RwLock<TreeNode>>,
        parent_id: Option<u64>,
    ) -> Option<(u64, Vec<u64>, u64)> {
        let g = arc.read();
        let TreeNode::Internal(n) = &*g else {
            return None;
        };
        let my_id = n.node_id;
        let children = n.resident_children();
        // Is this a non-root upper IN whose children are all BINs?
        if let Some(gp) = parent_id {
            let bin_ids: Vec<u64> = children
                .iter()
                .filter_map(|c| match &*c.read() {
                    TreeNode::Bottom(b) => Some(b.node_id),
                    _ => None,
                })
                .collect();
            if !bin_ids.is_empty() && bin_ids.len() == children.len() {
                return Some((my_id, bin_ids, gp));
            }
        }
        // Recurse.
        drop(g);
        for c in children {
            if let Some(found) = walk(&c, Some(my_id)) {
                return Some(found);
            }
        }
        None
    }
    walk(&root, None)
}

#[test]
fn evictor_logs_dirty_upper_in_before_detach() {
    let dir = tempfile::tempdir().unwrap();
    let lm = make_log(dir.path());

    let counter = Arc::new(AtomicI64::new(0));
    // max=1 byte with evict_bytes small -> still_needs_eviction() is always
    // true, so the manual evictor evicts every LRU candidate.
    let arbiter = Arbiter::new(1, Arc::clone(&counter), 1, 0);
    let evictor = Arc::new(
        Evictor::new(arbiter, 64, false).with_log_manager(Arc::clone(&lm)),
    );

    // max_entries=2 => deep right-spine; INs below the root are non-root
    // upper INs, and there is a grandparent above them.
    let mut tree = Tree::new(1, 2);
    tree.set_log_manager(Arc::clone(&lm));
    tree.set_memory_counter(Arc::clone(&counter));
    // NOTE: deliberately NOT wiring the InListListener: it would auto-register
    // every IN in the deep spine into the LRU and the evictor would evict a
    // flood of other dirty INs. We add ONLY the target upper IN as a candidate
    // (via `note_ins_added` below) so the assertion is about THAT node.
    let tree_arc = Arc::new(RwLock::new(tree));
    evictor.set_tree(Arc::clone(&tree_arc), 1);

    let n = 16u16;
    let (upper_id, bin_ids, gp_id) = {
        let t = tree_arc.read().unwrap();
        for i in 0..n {
            t.insert(
                i.to_be_bytes().to_vec(),
                vec![i as u8, (i >> 8) as u8],
                Lsn::new(1, u32::from(i) + 1),
            )
            .unwrap();
        }
        find_nonroot_upper_in(&t)
            .expect("fixture: need a non-root upper IN with BIN children")
    };

    // Record the grandparent slot LSN pointing at the target upper IN BEFORE
    // eviction (the pre-change on-disk image the base bug would leave stale).
    // Capture the (grandparent id, slot index) so we can re-read the SAME slot
    // after the upper IN is detached (detach nulls the resident child pointer
    // but keeps the slot key/LSN in the grandparent).
    let (gp_slot_lsn_before, gp_slot_index) = {
        let t = tree_arc.read().unwrap();
        let (parent_arc, slot) =
            t.get_parent_in_for_child_in(upper_id).expect("gp of upper");
        let (lsn, pid) = match &*parent_arc.read() {
            TreeNode::Internal(p) => (p.get_lsn(slot), p.node_id),
            _ => panic!("grandparent must be an IN"),
        };
        assert_eq!(pid, gp_id, "sanity: grandparent id matches");
        (lsn, slot)
    };

    // Detach all BINs under the target upper IN so it becomes childless
    // (EV-6 eligible), then mark the upper IN DIRTY — modelling an unlogged
    // structural change (a post-split / post-prune child slot) that MUST be
    // logged before the node can be dropped.
    {
        let t = tree_arc.read().unwrap();
        let root = t.get_root().unwrap();
        for bid in &bin_ids {
            log_and_detach_bin(&t, &root, &lm, *bid);
        }
        let upper = find_arc(&root, upper_id).expect("upper still resident");
        {
            let mut g = upper.write();
            match &mut *g {
                TreeNode::Internal(_) => g.set_dirty(true),
                _ => panic!("target must be an upper IN"),
            }
        }
        assert!(
            find_arc(&root, upper_id)
                .map(|a| match &*a.read() {
                    TreeNode::Internal(u) => u.resident_children().is_empty(),
                    _ => false,
                })
                .unwrap_or(false),
            "fixture: target upper IN must be childless"
        );
    }

    // Feed the dirty childless upper IN to the evictor's LRU and evict.
    evictor.note_ins_added(upper_id, CacheMode::Default);
    for _ in 0..20 {
        evictor.do_evict(EvictionSource::Manual);
        if find_arc(&tree_arc.read().unwrap().get_root().unwrap(), upper_id)
            .is_none()
        {
            break;
        }
    }

    // The dirty upper IN was logged (dirty_nodes_evicted incremented) and
    // detached (no longer resident).
    let dirty_evicted =
        evictor.get_stats().get(&evictor.get_stats().dirty_nodes_evicted);
    assert!(
        dirty_evicted > 0,
        "GAP B: the dirty upper IN must be LOGGED before detach \
         (dirty_nodes_evicted stays 0 on base); got {dirty_evicted}"
    );

    // The grandparent slot LSN must have ADVANCED to a real WAL LSN — the
    // fresh logged image of the upper IN. On base the slot kept
    // `gp_slot_lsn_before` (the stale pre-change image) and the structural
    // change was lost. Re-read the SAME grandparent slot: detach keeps the
    // slot key/LSN in the grandparent (only the resident child pointer is
    // nulled), so the freshened LSN is observable there.
    let gp_slot_lsn_after = {
        let t = tree_arc.read().unwrap();
        let root = t.get_root().unwrap();
        let gp = find_arc(&root, gp_id).expect("grandparent resident");
        match &*gp.read() {
            TreeNode::Internal(p) => p.get_lsn(gp_slot_index),
            _ => panic!("gp is IN"),
        }
    };
    assert_ne!(
        gp_slot_lsn_after, gp_slot_lsn_before,
        "GAP B: grandparent slot LSN must be freshened to the logged upper-IN \
         image (before={gp_slot_lsn_before:?} after={gp_slot_lsn_after:?}); on \
         base it stays stale and the structural change is lost on recovery"
    );
    assert!(
        !gp_slot_lsn_after.is_null(),
        "the freshened slot LSN must be a real WAL LSN, got NULL"
    );

    // Surviving keys still re-fetch correctly through the tree's descent
    // (which will re-fetch the logged upper IN from the freshened slot LSN).
    lm.flush_no_sync().expect("flush");
    let t = tree_arc.read().unwrap();
    for i in 0..n {
        let r = t.search_with_data(&i.to_be_bytes());
        // Some subtrees were detached by the fixture; only assert the keys
        // that are still reachable read back the correct value.
        if let Some(sf) = r
            && sf.found
        {
            assert_eq!(
                sf.data.as_deref(),
                Some(&[i as u8, (i >> 8) as u8][..]),
                "key {i} re-fetched WRONG data"
            );
        }
    }
}
