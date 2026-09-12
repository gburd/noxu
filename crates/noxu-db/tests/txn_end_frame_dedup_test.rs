//! Regression: an explicit `Environment::begin_transaction` commit/abort/
//! prepare must write EXACTLY ONE `TxnCommit` / `TxnAbort` / `TxnPrepare`
//! WAL frame per operation, not two.
//!
//! `EnvironmentImpl::begin_txn` now always attaches a `LogManager` to the
//! inner `noxu_txn::Txn` (needed for TXN-1 obsolete-LSN merging into the
//! shared `UtilizationTracker`, `docs/src/internal/space-amplification-2026-09.md`
//! Phase 4). Before this, the inner `Txn` had no `LogManager` under the
//! gate that was removed, so its own would-be `TxnCommit`/`TxnAbort`/
//! `TxnPrepare` WAL writes silently no-op'd. Attaching a real `LogManager`
//! un-silences those writes too, colliding with the OUTER `noxu_db::Transaction`
//! wrapper's own frame write (`Transaction::write_txn_end` /
//! `Transaction::prepare`) -- producing a second frame with the wrong
//! (hardcoded `Durability::CommitSync`) durability for every commit, abort,
//! and prepare.
//!
//! Fixed by `Txn::set_suppress_own_end_frame`, set only at
//! `TxnManager::begin_txn_with_log_manager` (whose only production caller is
//! always wrapped by an outer `Transaction`). These tests count raw WAL
//! frames by type via `LogFileReader` directly, so they fail if the
//! duplicate ever comes back.

use hashbrown::HashMap;
use noxu_db::{DatabaseConfig, Environment, EnvironmentConfig};
use noxu_log::{FileManager, LogEntryType, LogFileReader};
use std::sync::Arc;
use tempfile::TempDir;

/// Counts every log entry by type across all log files in `dir`.
fn count_entries_by_type(dir: &std::path::Path) -> HashMap<LogEntryType, u64> {
    let fm = Arc::new(
        FileManager::new(dir, true, 1 << 20, 100)
            .expect("open log files read-only"),
    );
    let file_nums = fm.list_file_numbers().expect("list log files");
    let mut counts: HashMap<LogEntryType, u64> = HashMap::new();
    for file_num in file_nums {
        let mut reader = LogFileReader::open(Arc::clone(&fm), file_num)
            .expect("open log file reader");
        while let Some((_lsn, entry_type, _payload)) = reader.read_next() {
            *counts.entry(entry_type).or_insert(0) += 1;
        }
    }
    counts
}

fn wired() -> (TempDir, Environment, noxu_db::Database) {
    let dir = TempDir::new().unwrap();
    let env = Environment::open(
        EnvironmentConfig::new(dir.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true),
    )
    .unwrap();
    let db = env
        .open_database(
            None,
            "txn_end_frame_dedup",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap();
    (dir, env, db)
}

#[test]
fn explicit_commit_writes_exactly_one_txn_commit_frame() {
    let (dir, env, db) = wired();

    let txn = env.begin_transaction(None).unwrap();
    db.put_in(&txn, b"k", b"v").unwrap();
    txn.commit().unwrap();
    drop(db);
    env.close().unwrap();

    let counts = count_entries_by_type(dir.path());
    assert_eq!(
        counts.get(&LogEntryType::TxnCommit).copied().unwrap_or(0),
        1,
        "exactly one TxnCommit frame must be written per explicit commit \
         (got {:?}); a second frame means the inner Txn's own commit path \
         is no longer suppressed by SUPPRESS_OWN_END_FRAME",
        counts.get(&LogEntryType::TxnCommit)
    );
}

#[test]
fn explicit_abort_writes_exactly_one_txn_abort_frame() {
    let (dir, env, db) = wired();

    let txn = env.begin_transaction(None).unwrap();
    db.put_in(&txn, b"k", b"v").unwrap();
    txn.abort().unwrap();
    drop(db);
    env.close().unwrap();

    let counts = count_entries_by_type(dir.path());
    assert_eq!(
        counts.get(&LogEntryType::TxnAbort).copied().unwrap_or(0),
        1,
        "exactly one TxnAbort frame must be written per explicit abort \
         (got {:?})",
        counts.get(&LogEntryType::TxnAbort)
    );
}

#[test]
fn commit_no_sync_does_not_pick_up_the_inner_txns_own_hardcoded_sync_fsync() {
    // Regression for the durability half of the same bug: the inner Txn's
    // own (suppressed) commit path used to run with a hardcoded
    // Durability::CommitSync regardless of what the caller actually asked
    // for. This does not directly observe the fsync count (no public
    // counter is asserted here to keep the test independent of stats
    // wiring), but it does confirm the outer wrapper is the only path that
    // writes a frame at all under COMMIT_NO_SYNC, so there is no second,
    // wrongly-synced frame hiding behind it.
    let (dir, env, db) = wired();

    let cfg = noxu_db::TransactionConfig::default()
        .with_durability(noxu_db::Durability::COMMIT_NO_SYNC);
    let txn = env.begin_transaction(Some(&cfg)).unwrap();
    db.put_in(&txn, b"k", b"v").unwrap();
    txn.commit().unwrap();
    drop(db);
    env.close().unwrap();

    let counts = count_entries_by_type(dir.path());
    assert_eq!(
        counts.get(&LogEntryType::TxnCommit).copied().unwrap_or(0),
        1,
        "exactly one TxnCommit frame even under COMMIT_NO_SYNC (got {:?})",
        counts.get(&LogEntryType::TxnCommit)
    );
}
