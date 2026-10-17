// Copyright (C) 2024-2026 Greg Burd.  Apache-2.0 OR MIT.
//! BUG-BARE-RMW-LOCK-LEAK witness (public API).
//!
//! Through the public API: `Database::open_cursor(None)` -> `Cursor` RMW read
//! (`LockMode::Rmw`) -> close / drop.  A later transaction writing the SAME
//! key must NOT be blocked by a WRITE lock the bare cursor leaked.  Mirrors
//! BDB-JE non-txn cursor locking: for non-transactional operations both read
//! and write locks are held only while the cursor is positioned and are
//! released when the cursor moves or closes (LockMode.java javadoc;
//! `BasicLocker.releaseNonTxnLocks`, BasicLocker.java:198).
#![cfg(not(noxu_shuttle))]

use noxu_db::{
    DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig, Get,
    LockMode, OperationStatus,
};
use std::time::Instant;

fn open_env(dir: &std::path::Path) -> Environment {
    let mut cfg = EnvironmentConfig::new(dir.to_path_buf())
        .with_transactional(true)
        .with_allow_create(true)
        .with_run_checkpointer(false)
        .with_run_cleaner(false);
    // Short lock timeout: a leaked WRITE lock makes the later writer wait, and
    // we want that wait to resolve into an observable timeout quickly rather
    // than hanging the test.
    cfg.set_lock_timeout(400);
    Environment::open(cfg).unwrap()
}

/// A bare cursor RMW-reads a key, then closes/drops.  A later transaction that
/// writes the SAME key must not be blocked by a leaked WRITE lock.
fn public_rmw_does_not_block_later_writer(close: bool) {
    let dir = tempfile::tempdir().unwrap();
    let env = open_env(dir.path());
    let db = env
        .open_database(
            None,
            "d",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap();
    db.put(b"k", b"v0").unwrap();

    // Bare (auto-commit) cursor: no explicit transaction.
    {
        let mut cursor = db.open_cursor(None).unwrap();
        let mut key = DatabaseEntry::from_bytes(b"k");
        let mut data = DatabaseEntry::new();
        let st = cursor
            .get(&mut key, &mut data, Get::Search, Some(LockMode::Rmw))
            .unwrap();
        assert_eq!(st, OperationStatus::Success);
        assert_eq!(data.data(), b"v0");
        if close {
            cursor.close().unwrap();
        }
        // else: dropped at end of scope.
    }

    // A separate transaction writes the same key.  With the leak this blocks
    // until the lock timeout and returns a timeout error; fixed, it succeeds
    // immediately.
    let txn = env.begin_transaction(None).unwrap();
    let start = Instant::now();
    let res = db.put_in(&txn, b"k", b"v1");
    let elapsed = start.elapsed();
    res.unwrap_or_else(|e| {
        panic!(
            "later writer blocked by leaked bare-RMW lock (close={close}): \
             {e:?} after {elapsed:?}"
        )
    });
    txn.commit().unwrap();
    assert_eq!(db.get(b"k").unwrap().as_deref(), Some(&b"v1"[..]));
    drop(db);
    env.close().unwrap();
}

#[test]
fn public_bare_rmw_close_does_not_block_later_writer() {
    public_rmw_does_not_block_later_writer(true);
}

#[test]
fn public_bare_rmw_drop_does_not_block_later_writer() {
    public_rmw_does_not_block_later_writer(false);
}
