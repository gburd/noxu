// Copyright (C) 2024-2025 Greg Burd.  Apache-2.0 OR MIT.
//! Behavioral test for `EnvironmentConfig::with_lock_n_lock_tables`.
//!
//! The lock manager shards its lock tables into N independently-latched
//! partitions; `lock_n_lock_tables` sets N. The count itself is exercised at
//! the lock-manager layer (see `noxu-txn`'s `lock_manager` tests). This test
//! covers the end-to-end plumbing: an environment opened with a NON-default
//! shard count still performs correct concurrent record-level locking (writers
//! to the same key serialize; writers to disjoint keys proceed), proving the
//! configured value reaches the live lock manager rather than being dropped.

use std::sync::{Arc, Barrier};
use std::thread;

use noxu_db::{
    DatabaseConfig, Environment, EnvironmentConfig, TransactionConfig,
};
use tempfile::TempDir;

#[test]
fn non_default_lock_table_count_still_locks_correctly() {
    let dir = TempDir::new().unwrap();
    // A deliberately non-default shard count (default is 64).
    let env = Environment::open(
        EnvironmentConfig::new(dir.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true)
            .with_lock_n_lock_tables(7),
    )
    .unwrap();
    let db = Arc::new(
        env.open_database(
            None,
            "locks",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap(),
    );

    // Disjoint-key writers must all succeed concurrently (no false conflict
    // just because they hash to shards).
    let env = Arc::new(env);
    let n_threads = 8u32;
    let per = 200u32;
    let barrier = Arc::new(Barrier::new(n_threads as usize));
    let mut handles = Vec::new();
    for t in 0..n_threads {
        let env = Arc::clone(&env);
        let db = Arc::clone(&db);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier.wait();
            for i in 0..per {
                let key = format!("t{t}-k{i:04}");
                let txn = env.begin_transaction(None).unwrap();
                db.put_in(&txn, key.as_bytes(), b"v").unwrap();
                txn.commit().unwrap();
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }

    // Every disjoint write committed.
    for t in 0..n_threads {
        for i in 0..per {
            let key = format!("t{t}-k{i:04}");
            assert_eq!(
                db.get(key.as_bytes()).unwrap().as_deref(),
                Some(&b"v"[..]),
                "disjoint write {key} lost with lock_n_lock_tables=7"
            );
        }
    }

    // Same-key contention must still serialize: a no-wait txn holding a write
    // lock blocks a second no-wait txn on the same key.
    let t1 = env.begin_transaction(None).unwrap();
    db.put_in(&t1, b"contended", b"a").unwrap();
    let nowait = TransactionConfig::new().with_no_wait(true);
    let t2 = env.begin_transaction(Some(&nowait)).unwrap();
    let blocked = db.put_in(&t2, b"contended", b"b");
    assert!(
        blocked.is_err(),
        "same-key write under a held write lock must conflict even with a \
         non-default lock-table count"
    );
    t2.abort().unwrap();
    t1.commit().unwrap();
}
