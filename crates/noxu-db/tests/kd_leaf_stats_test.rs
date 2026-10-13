//! BUG-STATS-KD-LEAF-OVERCOUNT regression.
//!
//! A known-deleted (KD) slot retained by a transactional delete (until the
//! commit + compressor remove it) is hidden from every read path but still
//! occupies a BIN entry.  `BtreeStats::leaf_node_count` (JE `getLNCount()`) must count LIVE
//! records only; the retained KD slots belong to `deleted_leaf_node_count`
//! (JE `getDeletedLNCount()`).  JE's stats walk calls
//! `treeStatsAccumulator.incrementDeletedLNCount()` for a KD / defunct slot
//! instead of counting it as an LN (CursorImpl.java:2063-2085).
//!
//! Before the fix, `collect_stats_recursive` counted `b.entries.len()` — all
//! slots including KD — so a committed-but-not-yet-compacted delete inflated
//! `leaf_node_count` by the number of tombstones.
#![cfg(not(noxu_shuttle))]

use noxu_db::{
    Database, DatabaseConfig, Durability, Environment, EnvironmentConfig,
};
use std::path::Path;

fn env_cfg(p: &Path, compressor: bool) -> EnvironmentConfig {
    let mut c = EnvironmentConfig::new(p.to_path_buf())
        .with_transactional(true)
        .with_allow_create(true)
        .with_durability(Durability::COMMIT_SYNC);
    c.set_run_cleaner(false);
    c.set_run_checkpointer(false);
    c.set_run_in_compressor(compressor);
    c
}
fn db_cfg() -> DatabaseConfig {
    DatabaseConfig::new().with_allow_create(true).with_transactional(true)
}

fn seed(db: &Database) {
    for k in [&b"a"[..], b"b", b"c", b"d"] {
        db.put(k, b"v").unwrap();
    }
}

#[test]
fn committed_txn_delete_counts_kd_as_deleted_not_live() {
    let dir = tempfile::tempdir().unwrap();
    // Compressor OFF: KD slots linger after commit, so stats can observe them.
    let env = Environment::open(env_cfg(dir.path(), false)).unwrap();
    let db = env.open_database(None, "d", &db_cfg()).unwrap();
    seed(&db);

    let txn = env.begin_transaction(None).unwrap();
    assert!(db.delete_in(&txn, b"b").unwrap());
    assert!(db.delete_in(&txn, b"d").unwrap());
    txn.commit().unwrap();

    // Reads: deleted keys invisible, live count = 2.
    assert!(db.get(b"b").unwrap().is_none());
    assert!(db.get(b"d").unwrap().is_none());
    assert_eq!(db.count().unwrap(), 2);

    let st = db.stats(None).unwrap();
    assert_eq!(
        st.btree.leaf_node_count, 2,
        "leaf_node_count must be LIVE records only (JE getLNCount)"
    );
    assert_eq!(
        st.btree.deleted_leaf_node_count, 2,
        "retained KD slots belong to deleted_leaf_node_count (JE \
         getDeletedLNCount)"
    );

    // Compress away the KD slots: deleted drops to 0, live stays 2.
    env.compress().unwrap();
    let st2 = db.stats(None).unwrap();
    assert_eq!(st2.btree.leaf_node_count, 2, "live unchanged after compress");
    assert_eq!(
        st2.btree.deleted_leaf_node_count, 0,
        "KD slots removed by the compressor"
    );

    db.close().unwrap();
    env.close().unwrap();
}

#[test]
fn aborted_txn_delete_restores_live_and_counts_no_deleted() {
    let dir = tempfile::tempdir().unwrap();
    let env = Environment::open(env_cfg(dir.path(), false)).unwrap();
    let db = env.open_database(None, "d", &db_cfg()).unwrap();
    seed(&db);

    let txn = env.begin_transaction(None).unwrap();
    assert!(db.delete_in(&txn, b"b").unwrap());
    assert!(db.delete_in(&txn, b"d").unwrap());
    txn.abort().unwrap();

    // Abort restored both records: all 4 live, none deleted.
    assert_eq!(db.count().unwrap(), 4);
    let st = db.stats(None).unwrap();
    assert_eq!(st.btree.leaf_node_count, 4, "abort restores live records");
    assert_eq!(
        st.btree.deleted_leaf_node_count, 0,
        "an aborted delete leaves no KD slot"
    );

    db.close().unwrap();
    env.close().unwrap();
}
