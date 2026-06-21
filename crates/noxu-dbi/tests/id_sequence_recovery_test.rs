//! ID-SEQUENCE RECOVERY tests (REC-S + REC-C + L-30).
//!
//! Recovery computes the max node/db/txn ids seen in the log
//! (`RecoveryInfo::use_max_*`) but, before this fix, NOTHING:
//!   - persisted the real values into `CheckpointEnd` (they were written as
//!     zeros), nor
//!   - seeded the env's `next_db_id` / `TxnManager.next_txn_id` /
//!     `NodeSequence` from the recovered maxes.
//!
//! Result: after a restart the env restarts its id counters at 1, so a
//! db-id / txn-id / node-id present in the recovered log can be REUSED — a
//! catalog / in-doubt-XA corruption hazard.
//!
//! JE recovers all three sequences as a recovery contract:
//!   `DbTree.setLastDbId`, `TxnManager.setLastTxnId`,
//!   `NodeSequence.initRealNodeId` / `setLastNodeId`.
//!
//! These tests assert the post-fix invariant: after recovery, a NEWLY
//! allocated db_id / txn_id / node_id is STRICTLY GREATER than the max in
//! the recovered log.  They FAIL on `main` (counters restart at 1) and PASS
//! after seeding.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use noxu_dbi::{DatabaseConfig, EnvironmentImpl};
use tempfile::TempDir;

/// Open a fresh transactional environment rooted at `dir`.
fn open_env(dir: &std::path::Path) -> EnvironmentImpl {
    EnvironmentImpl::new(
        dir, /*read_only=*/ false, /*transactional=*/ true,
    )
    .expect("open environment")
}

// ---------------------------------------------------------------------------
// REC-C / L-30: db-id, txn-id, node-id are all seeded past the recovered max
// ---------------------------------------------------------------------------

/// Advance all three id sequences, checkpoint, drop (clean close), reopen,
/// and assert that the recovered env allocates ids strictly greater than the
/// maxima present in the recovered log.
///
/// FAIL-PRE: on `main` `next_db_id`, `TxnManager.next_txn_id`, and the tree
/// node-id counters all restart at 1, so the first post-recovery allocation
/// collides with an id already in the log.
#[test]
fn ids_do_not_restart_at_one_after_recovery() {
    let dir = TempDir::new().unwrap();

    // ---- Phase 1: advance the sequences, then checkpoint + clean close. ----
    let (max_db_id, max_txn_id, max_node_id) = {
        let env = open_env(dir.path());

        // Advance db-id: create several user databases.
        for i in 0..5 {
            let cfg = DatabaseConfig::new();
            let mut cfg = cfg;
            cfg.allow_create = true;
            env.open_database(&format!("db_{i}"), &cfg).expect("open_database");
        }
        let max_db_id = env.peek_next_db_id() - 1;

        // Advance txn-id: begin several transactions.
        let mut last_txn = 0i64;
        for _ in 0..7 {
            let t = env.begin_txn().expect("begin_txn");
            last_txn = t.id_as_locker();
        }
        let max_txn_id = env.get_txn_manager().get_last_local_txn_id();
        assert!(max_txn_id >= last_txn);

        // Advance node-id: pump the node-id generator directly (a real put
        // path allocates node-ids for new BIN/IN nodes; we advance the
        // generator explicitly so the assertion does not depend on tree
        // structure details).
        for _ in 0..11 {
            noxu_tree::generate_node_id();
        }
        let max_node_id = noxu_tree::peek_next_node_id_counter() - 1;

        // Persist the maxima into a CheckpointEnd.
        env.run_checkpoint_with_invoker("id_seq_test").expect("checkpoint");

        // Clean close (drop) — env will be reopened from the log.
        env.close().expect("close");
        (max_db_id, max_txn_id, max_node_id)
    };

    assert!(max_db_id >= 5, "expected several db-ids allocated");
    assert!(max_txn_id >= 7, "expected several txn-ids allocated");
    assert!(max_node_id >= 11, "expected several node-ids allocated");

    // ---- Phase 2: reopen → recovery → assert no reuse. ----
    let env = open_env(dir.path());

    // db-id: next allocation must be > max db-id in the log.
    let next_db = env.peek_next_db_id();
    assert!(
        next_db > max_db_id,
        "db-id reuse after recovery: next_db_id={next_db} but log holds \
         db-id up to {max_db_id} (JE DbTree.setLastDbId)"
    );

    // txn-id: a freshly begun txn must have id > max txn-id in the log.
    let fresh_txn = env.begin_txn().expect("begin_txn after recovery");
    assert!(
        (fresh_txn.id_as_locker()) > max_txn_id,
        "txn-id reuse after recovery: fresh txn id={} but log holds txn-id \
         up to {max_txn_id} (JE TxnManager.setLastTxnId)",
        fresh_txn.id_as_locker()
    );

    // node-id: the node-id generator must be seeded past the recovered max.
    let fresh_node = noxu_tree::generate_node_id();
    assert!(
        fresh_node > max_node_id,
        "node-id reuse after recovery: fresh node id={fresh_node} but log \
         holds node-id up to {max_node_id} (JE NodeSequence.initRealNodeId)"
    );

    env.close().expect("close");
}
