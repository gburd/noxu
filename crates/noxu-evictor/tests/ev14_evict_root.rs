//! EV-14 evictor-level integration: a real Evictor wired to a real Tree +
//! LogManager evicts an idle root via the evictRoot path and the data
//! re-fetches correctly.  Deterministic (no daemon, no cache-pressure timing).

use noxu_evictor::{Arbiter, EvictionSource, Evictor};
use noxu_tree::InListListener;
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

/// Log + detach every resident BIN so the root upper-IN becomes idle.
fn log_and_detach_all_bins(tree: &Tree, lm: &noxu_log::LogManager) {
    let root = tree.get_root().expect("root");
    let mut ids = Vec::new();
    fn collect(arc: &Arc<parking_lot::RwLock<TreeNode>>, out: &mut Vec<u64>) {
        let g = arc.read();
        match &*g {
            TreeNode::Bottom(b) => out.push(b.node_id),
            TreeNode::Internal(n) => {
                for c in n.resident_children() {
                    collect(&c, out);
                }
            }
        }
    }
    collect(&root, &mut ids);
    for id in ids {
        fn find(
            arc: &Arc<parking_lot::RwLock<TreeNode>>,
            id: u64,
        ) -> Option<Arc<parking_lot::RwLock<TreeNode>>> {
            let g = arc.read();
            match &*g {
                TreeNode::Bottom(b) if b.node_id == id => Some(arc.clone()),
                TreeNode::Bottom(_) => None,
                TreeNode::Internal(n) => {
                    for c in n.resident_children() {
                        if let Some(a) = find(&c, id) {
                            return Some(a);
                        }
                    }
                    None
                }
            }
        }
        let bin = find(&root, id).expect("bin");
        let payload = match &*bin.read() {
            TreeNode::Bottom(b) => b.serialize_full(),
            _ => unreachable!(),
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
        assert!(tree.detach_node_by_id(id) > 0, "detach bin {id}");
    }
    lm.flush_no_sync().expect("flush");
}

#[test]
fn evictor_evicts_idle_root_via_evict_root() {
    let dir = tempfile::tempdir().unwrap();
    let lm = make_log(dir.path());

    // Shared memory counter: the tree's counter == the arbiter's cache_usage.
    let counter = Arc::new(AtomicI64::new(0));
    // Arbiter set so still_needs_eviction() is true (usage above the budget).
    let arbiter = Arbiter::new(1, Arc::clone(&counter), 1, 0); // max=1 byte -> always evict

    let evictor = Arc::new(
        Evictor::new(arbiter, 64, false).with_log_manager(Arc::clone(&lm)),
    );

    // Build a 2-level tree wired to the evictor (LRU feed) + log + counter.
    let mut tree = Tree::new(1, 8);
    tree.set_log_manager(Arc::clone(&lm));
    tree.set_memory_counter(Arc::clone(&counter));
    tree.set_in_list_listener(Arc::clone(&evictor) as Arc<dyn InListListener>);
    let tree_arc = Arc::new(RwLock::new(tree));
    evictor.set_tree(Arc::clone(&tree_arc), 1);

    let n = 20u8;
    {
        let t = tree_arc.read().unwrap();
        for i in 0..n {
            t.insert(
                vec![b'a' + i],
                vec![i, i + 1, i + 2],
                Lsn::new(1, u32::from(i) + 1),
            )
            .unwrap();
        }
        // 2-level precondition: root's children must all be BINs.
        let root = t.get_root().unwrap();
        if let TreeNode::Internal(rn) = &*root.read() {
            for c in rn.resident_children() {
                assert!(c.read().is_bin(), "test precondition: 2-level tree");
            }
        }
        // Make the root idle.
        log_and_detach_all_bins(&t, &lm);
    }

    // Drive the evictor: the root is the only node left in the LRU and is now
    // idle (no cached children) -> decide_eviction returns EvictRoot and
    // Evictor routes it through Tree::evict_root.
    let mut root_evicted = 0;
    for _ in 0..20 {
        evictor.do_evict(EvictionSource::Manual);
        root_evicted =
            evictor.get_stats().get(&evictor.get_stats().root_nodes_evicted);
        if root_evicted > 0 {
            break;
        }
    }
    assert!(
        root_evicted > 0,
        "EV-14: the evictor must evict the idle root via evictRoot \
         (root_nodes_evicted stays 0 on main); got {root_evicted}"
    );
    assert!(
        !tree_arc.read().unwrap().is_root_resident(),
        "root must be non-resident after evictRoot"
    );

    // RE-FETCH correctness: every key reads back correctly through the tree's
    // own descent (root re-fetched from root_log_lsn, each BIN re-fetched from
    // its parent slot LSN).
    let t = tree_arc.read().unwrap();
    for i in 0..n {
        let r = t
            .search_with_data(&[b'a' + i])
            .unwrap_or_else(|| panic!("key {} lost", (b'a' + i) as char));
        assert!(r.found, "key {} must re-fetch", (b'a' + i) as char);
        assert_eq!(
            r.data.as_deref(),
            Some(&[i, i + 1, i + 2][..]),
            "key {} re-fetched WRONG data",
            (b'a' + i) as char
        );
    }
    assert!(t.is_root_resident(), "root resident after re-fetch");
}
