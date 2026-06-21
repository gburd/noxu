//! REP-6: a live replica's `EnvironmentLogWriter` must feed the env's
//! SHARED, persisted VLSN index — not a throwaway.
//!
//! Before the fix, `become_replica` constructed a fresh
//! `Arc::new(VlsnIndex::new(10))` and handed THAT to the replica receive
//! loop's `EnvironmentLogWriter`. The env's shared `self.vlsn_index` (the one
//! `flush_to_disk` persists and `get_vlsn_range` / election ranking read) was
//! never updated by received entries. Consequence: the persisted `vlsn.idx`,
//! the reported VLSN range, and the DTVLSN-ranking `own_vlsn` lagged the
//! actually-received stream, widening catch-up (or forcing an unnecessary
//! network restore) after a clean restart.
//!
//! JE: the replica's `VLSNIndex` IS the environment's persisted index — the
//! same object recovery loads and `flush`/`awaitConsistency` read. There is
//! no separate "replica receive index".
//!
//! This test builds an `EnvironmentLogWriter` the same two ways
//! `become_replica` could (shared vs throwaway) and shows that only the
//! shared one makes `env.get_vlsn_range()` reflect received entries.

use std::sync::Arc;

use noxu_dbi::EnvironmentImpl;
use noxu_rep::stream::{EnvironmentLogWriter, LogWriter};
use noxu_rep::vlsn::VlsnIndex;
use noxu_rep::{RepConfig, ReplicatedEnvironment};

fn cfg(name: &str, env_home: &std::path::Path) -> RepConfig {
    RepConfig::builder("rep6_group", name, "127.0.0.1")
        .node_port(0)
        .env_home(env_home.to_path_buf())
        .build()
}

/// REP-6 reproduce-first: entries written by the replica receive loop's
/// `EnvironmentLogWriter` update the env's SHARED index (visible via
/// `get_vlsn_range`), not a throwaway.
///
/// - FAILS on main: `become_replica` feeds a throwaway, so the shared index
///   (and `get_vlsn_range`) stays empty no matter what is received. The
///   `throwaway` half of this test demonstrates that exact lag.
/// - PASSES after: `become_replica` feeds `Arc::clone(&self.vlsn_index)`, so
///   received entries advance the shared/persisted index.
#[test]
fn test_replica_writer_updates_shared_index() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let env_impl = Arc::new(
        EnvironmentImpl::new(tmp.path(), false, true)
            .expect("EnvironmentImpl::new"),
    );
    let rep_env = Arc::new(
        ReplicatedEnvironment::new(cfg("replica", tmp.path()))
            .expect("ReplicatedEnvironment::new"),
    );
    rep_env.with_environment(Arc::clone(&env_impl));
    let log_mgr = env_impl.get_log_manager().expect("log_manager");

    // Baseline: the env's reported range is empty before any entry.
    assert!(
        rep_env.get_vlsn_range().is_empty(),
        "env VLSN range must start empty"
    );

    // --- THROWAWAY (the old become_replica behaviour) --------------------
    // A fresh index is NOT the env's shared index; writing into it leaves
    // env.get_vlsn_range() unchanged.
    {
        let throwaway = Arc::new(VlsnIndex::new(10));
        let mut writer =
            EnvironmentLogWriter::new(Arc::clone(&log_mgr), throwaway);
        // entry_type 10 = InsertLN (a valid LogEntryType).
        writer.write_entry(1, 10, b"payload-1").expect("write_entry");
        assert!(
            rep_env.get_vlsn_range().is_empty(),
            "throwaway index must NOT update the env's shared range (the bug)"
        );
    }

    // --- SHARED (the fixed become_replica behaviour) ---------------------
    // The env's shared index is what become_replica now feeds; writing into
    // it advances env.get_vlsn_range().
    {
        let shared = rep_env.vlsn_index_arc();
        let mut writer =
            EnvironmentLogWriter::new(Arc::clone(&log_mgr), shared);
        // A commit at vlsn 5 (entry_type 30 = TxnCommit): must advance the
        // shared range's last AND, via REP-5 dispatch, its commit/sync.
        writer.write_entry(5, 30, b"commit-5").expect("write_entry");

        let range = rep_env.get_vlsn_range();
        assert_eq!(
            range.get_last(),
            5,
            "shared index: env range must reflect received vlsn 5"
        );
        assert_eq!(
            rep_env.get_current_vlsn(),
            5,
            "shared index: get_current_vlsn (own_vlsn for ranking) reflects 5"
        );
        // REP-5 dispatch through the replica writer: TxnCommit advances both
        // commit and sync boundaries.
        assert_eq!(range.get_commit_vlsn(), 5, "commit_vlsn advanced");
        assert_eq!(range.get_sync_vlsn(), 5, "sync_vlsn advanced (commit)");
    }
}
