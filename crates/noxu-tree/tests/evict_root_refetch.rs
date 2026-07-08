//! EV-14 gate test: evict a root IN, then re-fetch it from the log and prove
//! the data round-trips correctly.  A re-fetch that returns WRONG data is a
//! corruption bug far worse than never evicting (the EV-14 gate), so this is
//! the primary correctness check.

use noxu_tree::tree::{Tree, TreeNode};
use noxu_util::Lsn;
use std::sync::Arc;

fn make_log(dir: &std::path::Path) -> Arc<noxu_log::LogManager> {
    let fm = Arc::new(
        noxu_log::FileManager::new(dir, false, 10_000_000, 100).unwrap(),
    );
    Arc::new(noxu_log::LogManager::new(fm, 3, 1024 * 1024, 4096))
}

/// Log every resident BIN so its `last_full_lsn` is current, then detach it
/// (mirrors the evictor: flush_dirty_node_to_log + detach_node_by_id).  After
/// this the root upper-IN has no resident children.
fn log_and_detach_all_bins(tree: &Tree, lm: &noxu_log::LogManager) {
    let root = tree.get_root().expect("root");
    // Collect resident BIN node ids.
    fn bin_ids(arc: &Arc<parking_lot::RwLock<TreeNode>>, out: &mut Vec<u64>) {
        let g = arc.read();
        match &*g {
            TreeNode::Bottom(b) => out.push(b.node_id),
            TreeNode::Internal(n) => {
                for c in n.resident_children() {
                    bin_ids(&c, out);
                }
            }
        }
    }
    let mut ids = Vec::new();
    bin_ids(&root, &mut ids);

    for id in ids {
        // Find the BIN arc, log it as a full BIN, stamp last_full_lsn, detach.
        fn find_arc(
            arc: &Arc<parking_lot::RwLock<TreeNode>>,
            id: u64,
        ) -> Option<Arc<parking_lot::RwLock<TreeNode>>> {
            let g = arc.read();
            match &*g {
                TreeNode::Bottom(b) if b.node_id == id => Some(arc.clone()),
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
        let bin_arc = find_arc(&root, id).expect("bin arc");
        // Log the BIN as a full BIN (wrapped in an InLogEntry, as the evictor
        // and checkpointer do), then record its logged LSN.
        let payload = {
            let g = bin_arc.read();
            match &*g {
                TreeNode::Bottom(b) => b.serialize_full(),
                _ => unreachable!(),
            }
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
        {
            let mut g = bin_arc.write();
            if let TreeNode::Bottom(b) = &mut *g {
                b.clear_dirty_after_full_log(lsn);
            }
        }
        // Detach the BIN (drops the resident child, stamps parent slot LSN).
        let freed = tree.detach_node_by_id(id);
        assert!(freed > 0, "detach must free bytes for bin {id}");
    }
    lm.flush_no_sync().expect("flush");
}

/// HEADLINE (case 1): an idle root IN with no resident children is evicted via
/// evict_root, and a subsequent read RE-FETCHES the root (and its children)
/// from the log and returns CORRECT data.
#[test]
fn evict_idle_root_then_refetch_correct() {
    let dir = tempfile::tempdir().unwrap();
    let lm = make_log(dir.path());

    let mut tree = Tree::new(1, 8); // fanout 8: a 2-level tree (root over BINs)
    tree.set_log_manager(Arc::clone(&lm));

    let n = 20u8;
    for i in 0..n {
        tree.insert(
            vec![b'a' + i],
            vec![i, i + 1, i + 2],
            Lsn::new(1, u32::from(i) + 1),
        )
        .unwrap();
    }

    // All keys readable while resident.
    for i in 0..n {
        assert!(tree.search_with_data(&[b'a' + i]).expect("slot").found);
    }

    // This test exercises the 2-level case (root directly over BINs).  Assert
    // the shape so a future fanout change can't silently turn it into a
    // multi-level tree (where intermediate INs would also need detaching).
    {
        let root = tree.get_root().expect("root");
        let g = root.read();
        if let TreeNode::Internal(n) = &*g {
            for c in n.resident_children() {
                assert!(
                    c.read().is_bin(),
                    "test precondition: root's children must all be BINs"
                );
            }
        }
    }

    // Make the root idle: log + detach every resident BIN.
    log_and_detach_all_bins(&tree, &lm);

    // Now the root upper-IN has no resident children -> evict_root succeeds.
    let (freed, _was_dirty) =
        tree.evict_root(1).expect("idle root must be evictable");
    assert!(freed > 0, "evict_root frees the root's bytes");
    assert!(!tree.is_root_resident(), "root non-resident after evict");

    // RE-FETCH on access: every key reads back CORRECTLY (root re-fetched from
    // root_log_lsn, each BIN re-fetched from its parent slot LSN).
    for i in 0..n {
        let r = tree.search_with_data(&[b'a' + i]).unwrap_or_else(|| {
            panic!("key {} re-fetch lost", (b'a' + i) as char)
        });
        assert!(r.found, "key {} must re-fetch correctly", (b'a' + i) as char);
        assert_eq!(
            r.data.as_deref(),
            Some(&[i, i + 1, i + 2][..]),
            "key {} re-fetched WRONG data",
            (b'a' + i) as char
        );
    }
    assert!(tree.is_root_resident(), "root resident after re-fetch");
}

/// (2) evict_root refuses without a log manager (no re-fetch path).
#[test]
fn evict_root_refused_without_log_manager() {
    let mut tree = Tree::new(1, 32);
    tree.insert(b"k".to_vec(), b"v".to_vec(), Lsn::new(1, 1)).unwrap();
    assert!(
        tree.evict_root(1).is_none(),
        "no log manager => root must NOT be evicted"
    );
    assert!(tree.is_root_resident(), "root stays resident");
    let _ = &mut tree;
}

/// (2b) evict_root refuses a root with resident children (EV-6/7 protection).
#[test]
fn evict_root_refused_with_resident_children() {
    let dir = tempfile::tempdir().unwrap();
    let lm = make_log(dir.path());
    let mut tree = Tree::new(1, 4); // small fanout forces an upper-IN root
    tree.set_log_manager(Arc::clone(&lm));
    for i in 0u8..16 {
        tree.insert(vec![b'a' + i], vec![i; 4], Lsn::new(1, u32::from(i) + 1))
            .unwrap();
    }
    // The root upper IN still has resident children -> must refuse.
    assert!(
        tree.evict_root(1).is_none(),
        "root with resident children must NOT be evicted (EV-6/7)"
    );
    assert!(tree.is_root_resident());
}

/// EVICTOR-LOG-1 regression: `detach_node_by_id` must REFUSE to detach a
/// never-logged (dirty) BIN whose `last_full_lsn` is NULL.  Detaching such a
/// BIN would leave the parent slot pointing at its prior value -- an *LN* LSN
/// -- and the next re-fetch would try to parse that LN entry as a BIN and
/// fail, silently losing every key in the BIN.  This was the exact mechanism
/// behind the dataset >> cache record loss: the evictor's `log_manager` was
/// unwired, so `flush_dirty_node_to_log` no-oped (`return true` without
/// logging), and detach then corrupted the slot.
///
/// JE `Evictor.evict` only calls `parent.detachNode(...)` AFTER
/// `target.log(...)` returns a valid LSN (Evictor.java:3027-3035); a BIN is
/// never detached without a durable full version on disk.
#[test]
fn detach_refuses_never_logged_bin_then_succeeds_after_log() {
    let dir = tempfile::tempdir().unwrap();
    let lm = make_log(dir.path());
    let mut tree = Tree::new(1, 8); // 2-level: root over BINs
    tree.set_log_manager(Arc::clone(&lm));

    let n = 20u8;
    for i in 0..n {
        tree.insert(
            vec![b'a' + i],
            vec![i, i + 1],
            Lsn::new(1, u32::from(i) + 1),
        )
        .unwrap();
    }

    // Collect resident BIN ids; these were just inserted and never logged, so
    // their `last_full_lsn` is NULL.
    let root = tree.get_root().expect("root");
    let mut ids = Vec::new();
    if let TreeNode::Internal(nd) = &*root.read() {
        for c in nd.resident_children() {
            if let TreeNode::Bottom(b) = &*c.read() {
                assert_eq!(
                    b.last_full_lsn,
                    noxu_util::NULL_LSN,
                    "precondition: freshly inserted BIN is never-logged"
                );
                ids.push(b.node_id);
            }
        }
    }
    assert!(!ids.is_empty(), "expected at least one resident BIN");

    // (a) Detach must REFUSE every never-logged BIN (return 0) and leave it
    // resident.
    for &id in &ids {
        assert_eq!(
            tree.detach_node_by_id(id),
            0,
            "never-logged BIN {id} must NOT be detached"
        );
    }
    // Every key still readable (nothing was corrupted).
    for i in 0..n {
        let r = tree.search_with_data(&[b'a' + i]).expect("slot");
        assert!(
            r.found,
            "key {} lost after refused detach",
            (b'a' + i) as char
        );
        assert_eq!(r.data.as_deref(), Some(&[i, i + 1][..]));
    }

    // (b) After properly logging the BINs (flush_dirty_node_to_log semantics),
    // detach succeeds and re-fetch returns CORRECT data.
    log_and_detach_all_bins(&tree, &lm);
    for i in 0..n {
        let r = tree.search_with_data(&[b'a' + i]).unwrap_or_else(|| {
            panic!("key {} re-fetch lost", (b'a' + i) as char)
        });
        assert!(
            r.found,
            "key {} must re-fetch after log+detach",
            (b'a' + i) as char
        );
        assert_eq!(
            r.data.as_deref(),
            Some(&[i, i + 1][..]),
            "key {} re-fetched WRONG data",
            (b'a' + i) as char
        );
    }
}
