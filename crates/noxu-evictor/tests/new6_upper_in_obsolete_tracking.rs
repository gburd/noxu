//! NEW-6 (accounting fidelity): when the evictor re-logs a dirty *upper* IN
//! before detaching it (the GAP B / EVICTOR-UPPER-IN-1 path), the SUPERSEDED
//! prior on-disk image of that upper IN must be counted OBSOLETE in the
//! cleaner's utilization tracker — exactly as JE `IN.logInternal` counts the
//! prior version obsolete via `countObsoleteNode` (IN.java) whenever an IN is
//! re-logged.
//!
//! Bug (base, before this fix): `log_dirty_upper_in` logged the fresh upper-IN
//! image via `lm.log(LogEntryType::IN, ...)` (db_id=None, NO obsolete-tracking
//! of the prior slot LSN), whereas the BIN path uses
//! `lm.log_tracked(..., Some(db_id), old_obsolete, ...)` so the superseded
//! full-BIN image is counted obsolete. Consequence: the superseded upper-IN
//! image was NOT counted obsolete, so the cleaner under-estimated reclaimable
//! space in files holding old IN versions. This is a SPACE-ACCOUNTING fidelity
//! gap, NOT data loss — recovery correctness is already covered by
//! `gapb_dirty_upper_in.rs`.
//!
//! Fix (JE parity): `log_dirty_upper_in` now uses `log_tracked` with the
//! owning `db_id` and the PRIOR grandparent-slot LSN as `old_obsolete`, so the
//! superseded upper-IN image is counted obsolete (IN, exact, size 0 — the same
//! shape the BIN path uses).
//!
//! This test wires a real `UtilizationTracker` (via `UtilizationTrackerObserver`)
//! onto the `LogManager`, drives the REAL evictor upper-IN log path, and
//! asserts the file holding the PRIOR upper-IN LSN gains an obsolete-IN count.
//! On base the obsolete-IN count for that file stays 0; on the fix it becomes
//! > 0.

use noxu_cleaner::{UtilizationTracker, UtilizationTrackerObserver};
use noxu_evictor::{Arbiter, CacheMode, EvictionSource, Evictor};
use noxu_sync::Mutex;
use noxu_tree::tree::{Tree, TreeNode};
use noxu_util::Lsn;
use std::sync::atomic::AtomicI64;
use std::sync::{Arc, RwLock};

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
fn find_nonroot_upper_in(tree: &Tree) -> Option<(u64, Vec<u64>, u64)> {
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
fn evictor_counts_prior_upper_in_image_obsolete() {
    let dir = tempfile::tempdir().unwrap();
    let fm = Arc::new(
        noxu_log::FileManager::new(dir.path(), false, 10_000_000, 100).unwrap(),
    );
    // Wire a real UtilizationTracker onto the LogManager via the cleaner's
    // observer, so the evictor's `log_tracked` obsolete counts land in the
    // tracker's per-file summaries (the space-accounting surface NEW-6 fixes).
    let tracker = Arc::new(Mutex::new(UtilizationTracker::new(true)));
    let mut lm_owned = noxu_log::LogManager::new(fm, 3, 1024 * 1024, 4096);
    lm_owned.set_write_observer(Arc::new(UtilizationTrackerObserver::new(
        Arc::clone(&tracker),
    )));
    let lm = Arc::new(lm_owned);

    let counter = Arc::new(AtomicI64::new(0));
    let arbiter = Arbiter::new(1, Arc::clone(&counter), 1, 0);
    let evictor = Arc::new(
        Evictor::new(arbiter, 64, false).with_log_manager(Arc::clone(&lm)),
    );

    let mut tree = Tree::new(1, 2);
    tree.set_log_manager(Arc::clone(&lm));
    tree.set_memory_counter(Arc::clone(&counter));
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

    // The PRIOR grandparent slot LSN pointing at the target upper IN — the
    // superseded on-disk image that the re-log must count obsolete.
    let (prior_slot_lsn, gp_slot_index) = {
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
    assert!(
        !prior_slot_lsn.is_null(),
        "fixture: the upper IN must have a non-null prior slot LSN to obsolete"
    );
    let prior_file = prior_slot_lsn.file_number();

    // Baseline obsolete-IN count for the file holding the prior image.
    let obsolete_in_before = tracker
        .lock()
        .get_tracked_summary(prior_file)
        .map(|s| s.get_summary().obsolete_in_count)
        .unwrap_or(0);

    // Make the target upper IN childless (EV-6 eligible) and DIRTY.
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
    }

    // Evict the dirty childless upper IN through the REAL evictor path.
    evictor.note_ins_added(upper_id, CacheMode::Default);
    for _ in 0..20 {
        evictor.do_evict(EvictionSource::Manual);
        if find_arc(&tree_arc.read().unwrap().get_root().unwrap(), upper_id)
            .is_none()
        {
            break;
        }
    }

    // The upper IN was logged (GAP B behaviour preserved).
    let dirty_evicted =
        evictor.get_stats().get(&evictor.get_stats().dirty_nodes_evicted);
    assert!(
        dirty_evicted > 0,
        "the dirty upper IN must be LOGGED before detach; got {dirty_evicted}"
    );

    // NEW-6: the superseded upper-IN image at the PRIOR slot LSN must now be
    // counted obsolete (IN) for the file that held it. On base this count
    // stays at its baseline (the re-log used lm.log without obsolete-tracking).
    let obsolete_in_after = tracker
        .lock()
        .get_tracked_summary(prior_file)
        .map(|s| s.get_summary().obsolete_in_count)
        .unwrap_or(0);
    assert!(
        obsolete_in_after > obsolete_in_before,
        "NEW-6: the superseded upper-IN image (prior LSN {prior_slot_lsn:?}, \
         file {prior_file}) must be counted obsolete after the re-log \
         (JE IN.logInternal -> countObsoleteNode); obsolete_in_count \
         before={obsolete_in_before} after={obsolete_in_after}"
    );

    // The re-logged upper IN is under the OWNING db_id (1) in the per-DB
    // summary too — JE tags the IN entry with its database (CLN-9 axis).
    let db_obsolete_in = tracker
        .lock()
        .get_db_file_summary(1, prior_file)
        .map(|s| s.obsolete_in_count)
        .unwrap_or(0);
    assert!(
        db_obsolete_in > 0,
        "NEW-6: the superseded upper-IN image must be counted obsolete under \
         the owning db_id (per-DB axis); got {db_obsolete_in}"
    );

    // Keep the gap-B invariant observable: the grandparent slot LSN advanced.
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
        gp_slot_lsn_after, prior_slot_lsn,
        "GAP B: grandparent slot LSN must be freshened to the logged image"
    );
}
