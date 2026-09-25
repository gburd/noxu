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
            let entry = noxu_log::entry::ln_log_entry::LnLogEntry::new(
                1,
                None,
                NULL_LSN,
                false,
                None,
                None,
                noxu_util::Vlsn::new(-1),
                0,
                true,
                vec![i],
                Some(b"old".to_vec()),
                0,
                noxu_util::Vlsn::new(-1),
            );
            let mut buf = bytes::BytesMut::new();
            entry.write_to_log(&mut buf);
            let lsn = lm
                .log(LogEntryType::InsertLN, &buf, Provisional::No, true, false)
                .unwrap();
            tree.insert(vec![i], b"old".to_vec(), lsn).unwrap();
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
            if let Some(missing) = missing {
                p.set_lsn(slot, missing);
            } else {
                // This is the real LN placeholder installed by Tree::insert,
                // not a manufactured earlier BIN image.
                assert_eq!(
                    lm.read_entry(p.get_lsn(slot)).unwrap().0,
                    LogEntryType::InsertLN
                );
            }
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

/// EVICTOR-PIN-1 (audit GAP A): a BIN logged+cleared by the evictor's flush
/// phase, then RE-DIRTIED in the flush→detach window (the child latch is free
/// between the two phases), must be REFUSED by `detach_node_by_id` — detaching
/// it would publish the stale flushed image and lose the re-dirty mutation on
/// refault. The BIN must stay resident with its slot intact.
///
/// Control: the SAME BIN, left clean after logging, detaches normally.
#[test]
fn detach_refuses_bin_redirtied_since_flush() {
    let dir = tempfile::tempdir().unwrap();
    let fm = Arc::new(
        noxu_log::FileManager::new(dir.path(), false, 10_000_000, 100).unwrap(),
    );
    let lm = Arc::new(noxu_log::LogManager::new(fm, 3, 1024 * 1024, 4096));
    let mut tree = Tree::new(1, 8);
    tree.set_log_manager(Arc::clone(&lm));
    for i in 0..20u8 {
        tree.insert(vec![i], b"v".to_vec(), Lsn::new(1, i as u32)).unwrap();
    }
    let bin = tree.search_with_data(&[0]).unwrap().bin_arc;
    let id = bin.read().node_id();
    let (parent, slot) = tree.get_parent_in_for_child_in(id).unwrap();

    // Phase 1 (flush): log the full BIN and clear its dirty flag, exactly as
    // `Evictor::flush_dirty_node_to_log` does, publishing the logged LSN.
    let logged = {
        let mut guard = bin.write();
        let TreeNode::Bottom(b) = &mut *guard else { panic!("BIN") };
        let entry =
            InLogEntry::new(1, b.last_full_lsn, NULL_LSN, b.serialize_full());
        let mut buf = bytes::BytesMut::new();
        entry.write_to_log(&mut buf);
        let lsn = lm
            .log(LogEntryType::BIN, &buf, Provisional::No, true, false)
            .unwrap();
        b.clear_dirty_after_full_log(lsn);
        lsn
    };
    // Publish the flushed LSN into the parent slot (what the evictor's detach
    // would install for a clean flushed child).
    if let TreeNode::Internal(p) = &mut *parent.write() {
        p.set_lsn(slot, logged);
    }

    // ---- Control: clean flushed BIN detaches ----
    // (Do this on a SECOND clean BIN so the re-dirty test below is isolated.)
    let clean_bin = tree.search_with_data(&[10]).unwrap().bin_arc;
    let clean_id = clean_bin.read().node_id();
    {
        let mut guard = clean_bin.write();
        let TreeNode::Bottom(b) = &mut *guard else { panic!("BIN") };
        let entry =
            InLogEntry::new(1, b.last_full_lsn, NULL_LSN, b.serialize_full());
        let mut buf = bytes::BytesMut::new();
        entry.write_to_log(&mut buf);
        let lsn = lm
            .log(LogEntryType::BIN, &buf, Provisional::No, true, false)
            .unwrap();
        b.clear_dirty_after_full_log(lsn);
    }
    drop(clean_bin);
    assert!(
        tree.detach_node_by_id(clean_id) > 0,
        "control: a clean logged BIN must detach"
    );

    // ---- GAP A: re-dirty the flushed BIN in the flush→detach window ----
    // A concurrent cursor inserts a fresh key into the just-flushed BIN.
    tree.insert(vec![0, 0xEE], b"window-write".to_vec(), Lsn::new(2, 1))
        .unwrap();
    assert!(
        matches!(&*bin.read(), TreeNode::Bottom(b) if b.dirty || b.dirty_count() > 0),
        "BIN must be dirty after the window insert"
    );
    drop(bin);

    // detach must REFUSE (return 0): publishing the flushed image would lose
    // the window-inserted key on refault.
    assert_eq!(
        tree.detach_node_by_id(id),
        0,
        "GAP A: detach must refuse a BIN re-dirtied since its flush snapshot"
    );
    // BIN stays resident, slot intact, key still present.
    if let TreeNode::Internal(p) = &*parent.read() {
        assert!(p.get_child(slot).is_some(), "refused BIN must stay resident");
    }
    let win = tree.search_with_data(&[0, 0xEE]).unwrap();
    assert_eq!(win.data.as_deref(), Some(b"window-write".as_slice()));
}

/// EVICTOR-PIN-1 (audit GAP A): a BIN pinned by a cursor (`cursor_count > 0`)
/// in the flush→detach window must be refused by `detach_node_by_id`.
#[test]
fn detach_refuses_pinned_bin() {
    let dir = tempfile::tempdir().unwrap();
    let fm = Arc::new(
        noxu_log::FileManager::new(dir.path(), false, 10_000_000, 100).unwrap(),
    );
    let lm = Arc::new(noxu_log::LogManager::new(fm, 3, 1024 * 1024, 4096));
    let mut tree = Tree::new(1, 8);
    tree.set_log_manager(Arc::clone(&lm));
    for i in 0..20u8 {
        tree.insert(vec![i], b"v".to_vec(), Lsn::new(1, i as u32)).unwrap();
    }
    let bin = tree.search_with_data(&[0]).unwrap().bin_arc;
    let id = bin.read().node_id();
    let (parent, slot) = tree.get_parent_in_for_child_in(id).unwrap();
    // Flush: log + clear dirty + publish, then pin (cursor registers).
    {
        let mut guard = bin.write();
        let TreeNode::Bottom(b) = &mut *guard else { panic!("BIN") };
        let entry =
            InLogEntry::new(1, b.last_full_lsn, NULL_LSN, b.serialize_full());
        let mut buf = bytes::BytesMut::new();
        entry.write_to_log(&mut buf);
        let lsn = lm
            .log(LogEntryType::BIN, &buf, Provisional::No, true, false)
            .unwrap();
        b.clear_dirty_after_full_log(lsn);
        if let TreeNode::Internal(p) = &mut *parent.write() {
            p.set_lsn(slot, lsn);
        }
        // Window: a cursor pins the BIN.
        b.cursor_count += 1;
    }
    drop(bin);
    assert_eq!(
        tree.detach_node_by_id(id),
        0,
        "GAP A: detach must refuse a BIN pinned by a cursor"
    );
    if let TreeNode::Internal(p) = &*parent.read() {
        assert!(p.get_child(slot).is_some(), "pinned BIN must stay resident");
    }
}
