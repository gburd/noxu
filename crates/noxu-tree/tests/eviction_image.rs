//! Full-image controls for eviction's parent publication.
use noxu_log::{LogEntryType, Provisional, entry::in_log_entry::InLogEntry};
use noxu_tree::tree::{Tree, TreeNode};
use noxu_util::{Lsn, NULL_LSN};
use std::sync::Arc;

#[test]
fn detach_publishes_full_image_over_missing_or_older_slot() {
    for missing in [Some(NULL_LSN), Some(Lsn::transient_lsn(1)), None] {
        let dir = tempfile::tempdir().unwrap();
        let fm = Arc::new(
            noxu_log::FileManager::new(dir.path(), false, 10_000_000, 100)
                .unwrap(),
        );
        let lm = Arc::new(noxu_log::LogManager::new(fm, 3, 1024 * 1024, 4096));
        let mut tree = Tree::new(1, 8);
        tree.set_log_manager(Arc::clone(&lm));
        for i in 0..20u8 {
            tree.insert(vec![i], b"old".to_vec(), NULL_LSN).unwrap();
        }
        let bin = tree.search_with_data(&[0]).unwrap().bin_arc;
        let id = bin.read().node_id();
        let (parent, slot) = tree.get_parent_in_for_child_in(id).unwrap();
        let log_full = || {
            let mut guard = bin.write();
            let TreeNode::Bottom(b) = &mut *guard else { panic!("BIN") };
            let entry = InLogEntry::new(
                1,
                b.last_full_lsn,
                NULL_LSN,
                b.serialize_full(),
            );
            let mut buf = bytes::BytesMut::new();
            entry.write_to_log(&mut buf);
            let lsn = lm
                .log(LogEntryType::BIN, &buf, Provisional::No, true, false)
                .unwrap();
            b.clear_dirty_after_full_log(lsn);
            lsn
        };
        let old = log_full();
        if let TreeNode::Internal(p) = &mut *parent.write() {
            p.set_lsn(slot, missing.unwrap_or(old));
        }
        tree.insert(vec![0], b"new full".to_vec(), NULL_LSN).unwrap();
        assert!(tree.delete(&[1]));
        let new = log_full();
        assert!(new > old);
        drop(bin);
        assert!(tree.detach_node_by_id(id) > 0);
        if let TreeNode::Internal(p) = &*parent.read() {
            assert!(p.child_is_none(slot));
            assert_eq!(p.get_lsn(slot), new);
        }
        let fetched = tree.search_with_data(&[0]).unwrap();
        assert_eq!(fetched.data.as_deref(), Some(b"new full".as_slice()));
        assert!(!tree.search_with_data(&[1]).is_some_and(|r| r.found));
    }
}

#[test]
fn null_lsn_is_not_an_image_to_publish() {
    let tree = Tree::new(1, 8);
    for i in 0..20u8 {
        tree.insert(vec![i], vec![i], NULL_LSN).unwrap();
    }
    let bin = tree.search_with_data(&[0]).unwrap().bin_arc;
    let id = bin.read().node_id();
    let (parent, slot) = tree.get_parent_in_for_child_in(id).unwrap();
    if let TreeNode::Internal(p) = &mut *parent.write() {
        p.set_lsn(slot, Lsn::transient_lsn(1));
    }
    assert_eq!(tree.detach_node_by_id(id), 0);
    if let TreeNode::Internal(p) = &*parent.read() {
        assert!(p.get_child(slot).is_some());
        assert_eq!(p.get_lsn(slot), Lsn::transient_lsn(1));
    }
}
