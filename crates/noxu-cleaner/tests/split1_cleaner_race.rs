//! SPLIT-1 witness (tag-native): the cleaner must not count a detached but
//! live BIN's current image obsolete and delete the file under it.
//!
//! At v7.11.0 `RealTreeLookup::lookup_in` locates the parent slot via the
//! RESIDENT-only DFS and, if the child slot is non-resident (the evictor
//! detached the BIN), returns `Obsolete` WITHOUT re-faulting — counting the
//! live on-disk image dead and letting the cleaner delete the file that holds
//! it.  The next re-fault then dangles ("Log file not found": SplitRequired
//! forever / lost insert).
//!
//! JE `FileProcessor.findINInTree` -> `getParentINForChildIN(doFetch=true)`
//! returns the parent EX-latched with the child resident, compares
//! getLastFullLsn / slot LSN and `IN.setDirty(true)` under that latch
//! (FileProcessor.java:1723-1835).  The fix re-faults the detached child
//! under the parent WRITE latch before deciding.
//!
//! Oracle: after logging a BIN, publishing its slot LSN, and detaching it,
//! `lookup_in(bin_id, bin_lsn)` MUST be `Found` (live, migrated), never
//! `Obsolete`.
use std::sync::Arc;

use noxu_cleaner::{InLookupResult, RealTreeLookup, TreeLookup};
use noxu_log::entry::in_log_entry::InLogEntry;
use noxu_log::{FileManager, LogEntryType, LogManager, Provisional};
use noxu_tree::{Tree, TreeNode};
use noxu_txn::LockManager;
use noxu_util::{Lsn, NULL_LSN};
use std::sync::RwLock;

#[test]
fn detached_live_bin_is_not_counted_obsolete() {
    let dir = tempfile::tempdir().unwrap();
    let fm =
        Arc::new(FileManager::new(dir.path(), false, 10_000_000, 100).unwrap());
    let lm = Arc::new(LogManager::new(fm, 3, 1024 * 1024, 4096));
    let mut tree = Tree::new(1, 4);
    tree.set_log_manager(Arc::clone(&lm));
    let db_id = tree.get_database_id();
    let tree = Arc::new(RwLock::new(tree));

    // Insert enough keys to build a multi-level tree with several BINs.
    {
        let t = tree.read().unwrap();
        for i in 0u32..24 {
            let key = format!("k{:04}", i).into_bytes();
            t.insert(key, vec![i as u8; 8], Lsn::new(1, 100 + i)).unwrap();
        }
    }

    // Pick a non-root BIN; log it as a full BIN; publish its slot LSN so it is
    // detachable + re-fetchable (mirrors evictor flush_dirty_node_to_log).
    let (bin_id, bin_lsn) = {
        let t = tree.read().unwrap();
        let bin = t.first_bin_arc().expect("a BIN");
        let (node_id, bytes) = {
            let g = bin.read();
            let TreeNode::Bottom(b) = &*g else { panic!("not a BIN") };
            (b.node_id, b.serialize_full())
        };
        let entry = InLogEntry::new(db_id, NULL_LSN, NULL_LSN, bytes);
        let mut buf = bytes::BytesMut::with_capacity(entry.log_size());
        entry.write_to_log(&mut buf);
        let lsn = lm
            .log(LogEntryType::BIN, &buf, Provisional::No, true, false)
            .unwrap();
        {
            let mut g = bin.write();
            if let TreeNode::Bottom(b) = &mut *g {
                b.last_full_lsn = lsn;
                b.dirty = false;
                for e in b.entries.iter_mut() {
                    e.dirty = false;
                }
            }
        }
        Tree::update_parent_slot_lsn(&bin, lsn);
        (node_id, lsn)
    };

    // Detach the BIN (evictor eviction): parent slot keeps bin_lsn, child
    // becomes non-resident.
    let freed = tree.read().unwrap().detach_node_by_id(bin_id);
    assert!(freed > 0, "logged clean BIN must detach");

    // The cleaner now processes the file holding bin_lsn.  The live image MUST
    // be recognised as live (Found) and re-faulted, never counted obsolete.
    let lookup =
        RealTreeLookup::new(Arc::clone(&tree), Arc::new(LockManager::new()));
    let verdict = lookup.lookup_in(db_id as i64, bin_id as i64, bin_lsn);
    assert_eq!(
        verdict,
        InLookupResult::Found,
        "SPLIT-1: a detached-but-live BIN at its current slot LSN must be \
         Found (live), not Obsolete — the cleaner would delete the file \
         under a live reference"
    );
}

#[test]
fn superseded_image_is_counted_obsolete() {
    // Control: the fix must NOT make every image live.  A superseded LSN (an
    // older full-BIN image whose slot now points at a newer one) must still be
    // Obsolete so the old file can be reclaimed.
    let dir = tempfile::tempdir().unwrap();
    let fm =
        Arc::new(FileManager::new(dir.path(), false, 10_000_000, 100).unwrap());
    let lm = Arc::new(LogManager::new(fm, 3, 1024 * 1024, 4096));
    let mut tree = Tree::new(1, 4);
    tree.set_log_manager(Arc::clone(&lm));
    let db_id = tree.get_database_id();
    let tree = Arc::new(RwLock::new(tree));
    {
        let t = tree.read().unwrap();
        for i in 0u32..24 {
            let key = format!("k{:04}", i).into_bytes();
            t.insert(key, vec![i as u8; 8], Lsn::new(1, 100 + i)).unwrap();
        }
    }
    let log_bin = |tree: &Arc<RwLock<Tree>>| -> (u64, Lsn) {
        let t = tree.read().unwrap();
        let bin = t.first_bin_arc().expect("a BIN");
        let (node_id, bytes) = {
            let g = bin.read();
            let TreeNode::Bottom(b) = &*g else { panic!("not a BIN") };
            (b.node_id, b.serialize_full())
        };
        let entry = InLogEntry::new(db_id, NULL_LSN, NULL_LSN, bytes);
        let mut buf = bytes::BytesMut::with_capacity(entry.log_size());
        entry.write_to_log(&mut buf);
        let lsn = lm
            .log(LogEntryType::BIN, &buf, Provisional::No, true, false)
            .unwrap();
        {
            let mut g = bin.write();
            if let TreeNode::Bottom(b) = &mut *g {
                b.last_full_lsn = lsn;
                b.dirty = false;
                for e in b.entries.iter_mut() {
                    e.dirty = false;
                }
            }
        }
        Tree::update_parent_slot_lsn(&bin, lsn);
        (node_id, lsn)
    };
    let (bin_id, old_lsn) = log_bin(&tree);
    let (_, new_lsn) = log_bin(&tree);
    assert!(new_lsn > old_lsn);

    let lookup =
        RealTreeLookup::new(Arc::clone(&tree), Arc::new(LockManager::new()));
    // The OLD (superseded) LSN must be Obsolete, resident or not.
    assert_eq!(
        lookup.lookup_in(db_id as i64, bin_id as i64, old_lsn),
        InLookupResult::Obsolete,
        "a superseded BIN image must still be counted obsolete"
    );
}
