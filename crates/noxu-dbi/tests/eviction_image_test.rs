//! Regression: eviction must not replace a published BINDelta with its base.
#![cfg(not(noxu_shuttle))]

use noxu_dbi::{
    CursorImpl, DatabaseConfig, DbiEnvConfig, EnvironmentImpl, OperationStatus,
    PutMode, SearchMode,
};
use noxu_tree::tree::TreeNode;
use std::sync::Arc;

#[test]
fn eviction_refault_preserves_checkpointed_delta_image() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = DbiEnvConfig {
        run_evictor: false,
        run_checkpointer: false,
        run_cleaner: false,
        run_in_compressor: false,
        log_flush_no_sync_interval_ms: 0,
        evictor_mutate_bins: false,
        max_off_heap_memory: 0,
        ..DbiEnvConfig::default()
    };
    let env = EnvironmentImpl::from_dbi_config(dir.path(), &cfg).unwrap();
    let mut db_cfg = DatabaseConfig::new();
    db_cfg.set_allow_create(true);
    let db = env.open_database("images", &db_cfg).unwrap();
    let mut cursor = CursorImpl::new(Arc::clone(&db), 1);
    for i in 0..200u32 {
        cursor.put(&i.to_be_bytes(), b"old", PutMode::Overwrite).unwrap();
    }
    cursor.close().unwrap();
    env.run_checkpoint().unwrap();

    let mut cursor = CursorImpl::new(Arc::clone(&db), 2);
    cursor.put(&0u32.to_be_bytes(), b"new", PutMode::Overwrite).unwrap();
    cursor.close().unwrap();
    env.run_checkpoint().unwrap();

    let tree = db.read().get_real_tree_arc().unwrap();
    let root = tree.read().unwrap().get_root().unwrap();
    let (bin_id, delta_lsn) = {
        let guard = root.read();
        let TreeNode::Internal(parent) = &*guard else {
            panic!("non-root BIN required")
        };
        let child = parent.get_child(0).unwrap();
        let guard = child.read();
        let TreeNode::Bottom(bin) = &*guard else { panic!("BIN required") };
        assert!(!bin.last_delta_lsn.is_null(), "must actually log a delta");
        assert_ne!(bin.last_full_lsn, bin.last_delta_lsn);
        assert_eq!(
            parent.get_lsn(0),
            bin.last_delta_lsn,
            "checkpoint published delta"
        );
        (bin.node_id, bin.last_delta_lsn)
    };
    // Force pressure only AFTER the two checkpoints; no timing or synthetic LSNs.
    let evictor = env.get_evictor();
    evictor.get_arbiter().set_max_memory(1);
    for _ in 0..10 {
        env.evict_memory();
        if tree.read().unwrap().get_parent_in_for_child_in(bin_id).is_none() {
            break;
        }
    }
    assert!(
        tree.read().unwrap().get_parent_in_for_child_in(bin_id).is_none(),
        "affected BIN must really detach"
    );
    assert!(evictor.get_stats().get(&evictor.get_stats().nodes_evicted) > 0);
    // Stop further eviction while checking the actual disk refault, not a held Arc.
    evictor.get_arbiter().set_max_memory(i64::MAX / 2);
    let mut cursor = CursorImpl::new(Arc::clone(&db), 3);
    for i in 0..200u32 {
        assert_eq!(
            cursor.search(&i.to_be_bytes(), None, SearchMode::Set).unwrap(),
            OperationStatus::Success
        );
        let expected: &[u8] = if i == 0 { b"new" } else { b"old" };
        assert_eq!(
            cursor.get_current_data(),
            Some(expected),
            "key {i}, published delta {delta_lsn:?}"
        );
    }
    cursor.close().unwrap();
    env.close().unwrap();

    let env = EnvironmentImpl::from_dbi_config(dir.path(), &cfg).unwrap();
    let db = env.open_database("images", &db_cfg).unwrap();
    let mut cursor = CursorImpl::new(db, 4);
    for i in 0..200u32 {
        assert_eq!(
            cursor.search(&i.to_be_bytes(), None, SearchMode::Set).unwrap(),
            OperationStatus::Success
        );
        let expected: &[u8] = if i == 0 { b"new" } else { b"old" };
        assert_eq!(
            cursor.get_current_data(),
            Some(expected),
            "reopened key {i}"
        );
    }
    cursor.close().unwrap();
    env.close().unwrap();
}
