// Copyright (C) 2024-2026 Greg Burd.  Apache-2.0 OR MIT.
//! 7.11.1 publish-before-lock witness (coordinator point 3): a read-committed
//! reader must NEVER observe an UNCOMMITTED insert's value.  JE `lockLN`
//! attempts the record lock WHILE the BIN is latched, so a reader that sees a
//! published-but-uncommitted LSN also sees the inserter's WRITE lock and
//! BLOCKS.  Noxu's lock path resolves the slot under the BIN latch, DROPS the
//! latch, then attempts the (non-blocking) record lock -- on aarch64's weak
//! memory ordering the reader can see the published new LSN but miss the
//! inserter's WRITE lock, so its non-blocking READ is granted and it reads the
//! uncommitted value: a dirty read (isolation S2).
//!
//! RED on aarch64 under load if the publish-before-lock-visibility window is
//! reachable on a committed-isolation point read; GREEN after the
//! lock-while-latched fix.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use noxu_db::transaction_config::TransactionConfig;
use noxu_db::{
    DatabaseConfig, Environment, EnvironmentConfig, LockMode, ReadOptions,
};
use noxu_sync::Mutex;
use tempfile::TempDir;

fn open_env(dir: &TempDir) -> Environment {
    Environment::open(
        EnvironmentConfig::new(dir.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true),
    )
    .unwrap()
}

#[test]
fn read_committed_never_sees_uncommitted_insert() {
    let dir = TempDir::new().unwrap();
    let env = Arc::new(open_env(&dir));
    let db = Arc::new(Mutex::new(
        env.open_database(
            None,
            "pri",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap(),
    ));

    let dirty = Arc::new(AtomicU64::new(0));
    let reads = Arc::new(AtomicU64::new(0));
    let done = Arc::new(AtomicBool::new(false));

    // Writer: churn an INSERT then ABORT of key "hot" with value b"UNCOMMIT"
    // under an explicit txn, so the key is repeatedly published-uncommitted
    // then rolled back (never committed-present).
    let env_w = Arc::clone(&env);
    let db_w = Arc::clone(&db);
    let done_w = Arc::clone(&done);
    let writer = std::thread::spawn(move || {
        while !done_w.load(Ordering::Acquire) {
            let txn = env_w.begin_transaction(None).unwrap();
            {
                let g = db_w.lock();
                let _ = g.put_in(&txn, b"hot", b"UNCOMMIT");
            }
            // Hold the uncommitted insert briefly, then ABORT: "hot" is never
            // committed-present, so any reader value == b"UNCOMMIT" is a dirty
            // read of uncommitted data.
            txn.abort().unwrap();
        }
    });

    // Readers: read-committed point gets of "hot"; must see NotFound (writer
    // uncommitted / aborted) or block, NEVER the uncommitted value.
    let mut readers = Vec::new();
    for _ in 0..6 {
        let env_r = Arc::clone(&env);
        let db_r = Arc::clone(&db);
        let dirty_r = Arc::clone(&dirty);
        let reads_r = Arc::clone(&reads);
        let done_r = Arc::clone(&done);
        readers.push(std::thread::spawn(move || {
            let opts =
                ReadOptions::new().with_lock_mode(LockMode::ReadCommitted);
            for _ in 0..3000 {
                if done_r.load(Ordering::Acquire) {
                    break;
                }
                let mut cfg = TransactionConfig::new();
                cfg.set_read_committed(true);
                let txn = env_r.begin_transaction(Some(&cfg)).unwrap();
                let g = db_r.lock();
                match g.get_with_options(Some(&txn), b"hot", &opts) {
                    Ok(Some(v)) => {
                        reads_r.fetch_add(1, Ordering::Relaxed);
                        if v.as_ref() == b"UNCOMMIT" {
                            dirty_r.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    Ok(None) => {
                        reads_r.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {}
                }
                drop(g);
                let _ = txn.commit();
            }
        }));
    }

    for r in readers {
        r.join().unwrap();
    }
    done.store(true, Ordering::Release);
    writer.join().unwrap();

    assert_eq!(
        dirty.load(Ordering::Relaxed),
        0,
        "read-committed reader observed an UNCOMMITTED insert value \
         ({} times of {} reads) -- a dirty read (publish-before-lock)",
        dirty.load(Ordering::Relaxed),
        reads.load(Ordering::Relaxed),
    );
    assert!(reads.load(Ordering::Relaxed) > 0);
}
