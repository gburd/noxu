// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! A14-min / X-10 direct witness: a READ_COMMITTED point read whose slot LSN
//! moves under a concurrent writer's UNCOMMITTED put must never return the
//! writer's uncommitted bytes.
//!
//! At v7.11.0 `CursorImpl::search(SearchMode::Set)` re-read the slot's CURRENT
//! bytes via `get_data_from_tree` when `slot_lsn_changed`, WITHOUT re-locking
//! the moved LSN -- so an RC reader could observe a writer's in-place
//! uncommitted mutation ("Bvalue") that is later aborted.  (This is the
//! read-path isolation gap the secondary read path hits once it drops its
//! handle Mutex in A7; here it is driven directly through `Database::get_in`,
//! with no secondary and no handle Mutex, so it reproduces on the base by
//! itself.)
//!
//! JE `CursorImpl.lockLN` re-locks the slot's current LSN after an LSN change
//! (CursorImpl.java:3539-3689), so the read waits out the writer's txn.
//!
//! Oracle: an RC reader of "K" sees ONLY committed values; it must never see
//! the aborting writer's "Bvalue".
#![cfg(not(noxu_shuttle))]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;

use noxu_db::{
    Database, DatabaseConfig, Environment, EnvironmentConfig, TransactionConfig,
};
use tempfile::TempDir;

fn open_env_db() -> (TempDir, Arc<Environment>, Database) {
    let dir = TempDir::new().unwrap();
    let env = Environment::open(
        EnvironmentConfig::new(dir.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true),
    )
    .unwrap();
    let env = Arc::new(env);
    let db = env
        .open_database(
            None,
            "x10direct",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap();
    (dir, env, db)
}

#[test]
fn rc_point_read_never_sees_an_aborting_writers_uncommitted_value() {
    let (_dir, env, db) = open_env_db();
    // Seed K = "Avalue".
    db.put(b"K", b"Avalue").unwrap();

    let db = Arc::new(db);
    let torn = Arc::new(AtomicBool::new(false));
    let done = Arc::new(AtomicBool::new(false));
    let iters = Arc::new(AtomicU64::new(0));

    // Reader: RC point reads of "K"; must only ever see a committed value.
    // The only committed value in this test is "Avalue" (the writer always
    // aborts), so anything else -- in particular "Bvalue" -- is a torn read.
    let r_db = Arc::clone(&db);
    let r_env = Arc::clone(&env);
    let r_torn = Arc::clone(&torn);
    let r_done = Arc::clone(&done);
    let r_iters = Arc::clone(&iters);
    let reader = thread::spawn(move || {
        let rc = TransactionConfig::new().with_read_committed(true);
        while !r_done.load(Ordering::Relaxed) {
            let Ok(txn) = r_env.begin_transaction(Some(&rc)) else { continue };
            if let Ok(Some(v)) = r_db.get_in(&txn, b"K")
                && v.as_ref() != b"Avalue"
            {
                r_torn.store(true, Ordering::Relaxed);
            }
            let _ = txn.commit();
            r_iters.fetch_add(1, Ordering::Relaxed);
        }
    });

    // Writer: abort cycle -- put K="Bvalue" (uncommitted, moves K's LSN and
    // writes "Bvalue" in place), then abort (restores "Avalue").
    for _ in 0..4000 {
        let txn = env.begin_transaction(None).unwrap();
        db.put_in(&txn, b"K", b"Bvalue").unwrap();
        txn.abort().unwrap();
    }
    done.store(true, Ordering::Relaxed);
    reader.join().unwrap();

    assert!(
        !torn.load(Ordering::Relaxed),
        "X-10: RC point read observed an aborting writer's uncommitted value \
         (torn read). iterations={}",
        iters.load(Ordering::Relaxed)
    );
}
