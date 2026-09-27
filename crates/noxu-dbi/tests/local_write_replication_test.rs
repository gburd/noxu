// Copyright (C) 2024-2025 Greg Burd.  Apache-2.0 OR MIT.
//! Regression: a write is only permitted when a database's replicated-ness
//! and its writing transaction's local-write setting disagree in the
//! expected way (a replicated database written by an ordinarily-
//! replicating locker; a non-replicated database written by a locally-
//! writing locker).
//!
//! This is a mechanical, unconditional check at the cursor layer: it just
//! compares the database's replicated flag against the locker's
//! local-write flag, whatever they happen to be. The POLICY of what a
//! locker's local-write flag defaults to (and whether a caller's request
//! to change it is honored) is a layer up, at transaction-creation time
//! (`noxu-db::Environment::begin_transaction` resolves it from whether the
//! environment itself is replicated) — this test file covers only the
//! mechanical enforcement, using `Txn::set_local_write` directly to drive
//! both sides of the comparison.

#![cfg(not(noxu_shuttle))]

use std::sync::{Arc, Mutex};

use noxu_dbi::{CursorImpl, DatabaseConfig, EnvironmentImpl, PutMode};
use tempfile::TempDir;

fn tmp_env() -> (TempDir, EnvironmentImpl) {
    let dir = TempDir::new().unwrap();
    let env = EnvironmentImpl::new(dir.path(), false, true).unwrap();
    (dir, env)
}

// JE: PerDbReplicationTest.testStandalone — "A database in a standalone
// (non-replicated) environment should not be replicated." Noxu asserts the
// same: a DB opened in a non-replicated EnvironmentImpl is never marked
// replicated regardless of DatabaseConfig::replicated's default, and a
// default (local-write) locker may write it. (The JE close/reopen
// round-trip of the replicated bit and the replicated-env config-mismatch
// rejection are N/A here: Noxu's is_replicated is a runtime env flag, not a
// persisted per-DB attribute re-validated at open — see tp-je-rep.md
// ConversionTest / PerDbReplicationTest notes.)
#[test]
fn non_replicated_environment_default_locker_can_always_write() {
    let (_dir, env) = tmp_env();
    assert!(!env.is_replicated());

    let mut cfg = DatabaseConfig::new();
    cfg.set_allow_create(true);
    let db = env.open_database("d", &cfg).unwrap();
    assert!(
        !db.read().is_replicated(),
        "a database in a non-replicated environment must never be marked \
         replicated, regardless of DatabaseConfig::replicated's default"
    );

    // A freshly created Txn's local-write default (true) must permit a
    // write against a non-replicated database with no extra setup.
    let txn = env.begin_txn().unwrap();
    assert!(txn.is_local_write(), "a fresh Txn defaults to local_write=true");
    let mut cursor = CursorImpl::new(Arc::clone(&db), txn.id_as_locker())
        .with_txn(Arc::new(Mutex::new(txn)));
    let res = cursor.put(b"k", b"v", PutMode::Overwrite);
    assert!(res.is_ok(), "default locker must not be blocked; got {res:?}");
}

// JE: LocalWriteTxnTest.testReadAndWrite (write-enforcement subset) +
// PerDbReplicationTest.testNotReplicated. Mirrors JE's checkWriteAllowed
// matrix: an ordinary (replicating) locker may write a replicated DB but a
// local-write locker may not; a local-write locker may write a
// non-replicated DB but an ordinary replicating locker may not. Also covers
// testNotReplicated's core: a DB opened with set_replicated(false) in a
// replicated env stays non-replicated. Consistency-enforcement and
// non-txnal-read halves of testReadAndWrite are covered by
// noxu-rep::rep10_consistency_policy_test (the replica read gate).
#[test]
fn replicated_environment_enforces_local_write_agreement() {
    let (_dir, env) = tmp_env();
    env.set_replicated(true);
    assert!(env.is_replicated());

    // A replicated database (the DatabaseConfig default).
    let mut rep_cfg = DatabaseConfig::new();
    rep_cfg.set_allow_create(true);
    let rep_db = env.open_database("rep", &rep_cfg).unwrap();
    assert!(
        rep_db.read().is_replicated(),
        "DatabaseConfig::replicated defaults true and the environment is \
         replicated, so the database must be marked replicated"
    );

    // A database explicitly opted OUT of replication.
    let mut local_cfg = DatabaseConfig::new();
    local_cfg.set_allow_create(true);
    local_cfg.set_replicated(false);
    let local_db = env.open_database("local", &local_cfg).unwrap();
    assert!(!local_db.read().is_replicated());

    let write_with =
        |db: &Arc<noxu_util::dst_sync_pl::RwLock<noxu_dbi::DatabaseImpl>>,
         local_write: bool|
         -> Result<noxu_dbi::OperationStatus, noxu_dbi::DbiError> {
            let mut txn = env.begin_txn().unwrap();
            txn.set_local_write(local_write);
            let mut cursor =
                CursorImpl::new(Arc::clone(db), txn.id_as_locker())
                    .with_txn(Arc::new(Mutex::new(txn)));
            cursor.put(b"k", b"v", PutMode::Overwrite)
        };

    // Ordinary (replicating) locker writing to the replicated database: OK.
    assert!(write_with(&rep_db, false).is_ok());
    // Local-write locker writing to the replicated database: REJECTED.
    assert!(write_with(&rep_db, true).is_err());

    // Local-write locker writing to the non-replicated database: OK.
    assert!(write_with(&local_db, true).is_ok());
    // Ordinary (replicating) locker writing to the non-replicated database:
    // REJECTED (there is nothing to replicate it to consistently).
    assert!(write_with(&local_db, false).is_err());
}
