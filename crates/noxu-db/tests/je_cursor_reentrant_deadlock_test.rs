// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Correctness guard for the LN fetch / CacheMode-eviction stat wiring.
//!
//! When the LN fetch counters (nLNsFetch) and CacheMode LN eviction counters
//! (nLNsEvicted) were added, the fetch/evict paths originally reached the
//! evictor stats listener via `self.db_impl.read().get_real_tree()`. A read on
//! the writer-preferring `db_impl` `RwLock` on the cursor hot path is a latent
//! re-entrancy hazard (a second read taken while a `db_impl` read guard is held
//! higher up can queue behind a waiting writer). The fix caches the evictor
//! listener and key comparator on `CursorImpl` at construction, so the fetch /
//! evict / sorted-dup-match hot paths never touch `db_impl` again.
//!
//! This test is the correctness guard for that change: many reader threads
//! re-fault stripped LNs from the log concurrently with writer threads, with
//! the background daemons OFF (so it is isolated from the separate, pre-existing
//! noxu-log LogBuffer flush-vs-read latch-order deadlock that EvictActionTest
//! can trip under live daemons). It confirms the LN re-fault + CacheMode strip
//! counting paths carry no `db_impl` lock and make progress under concurrent
//! read/write/strip churn. A watchdog bounds the run as a backstop.

#![cfg(not(noxu_shuttle))]

use noxu_db::{
    DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig, Get,
    OperationStatus,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tempfile::TempDir;

const N_KEYS: u32 = 4_000;
// 1 KiB values: above TREE_MAX_EMBEDDED_LN (16), stored as LSN pointers, so a
// stripped slot MUST re-fault from the log on read.
const VAL_LEN: usize = 1024;
const N_READERS: usize = 6;
const N_WRITERS: usize = 2;
const ROUNDS: usize = 8;

#[test]
fn ln_fetch_and_cachemode_strip_carry_no_db_impl_lock() {
    let dir = TempDir::new().unwrap();
    let mut cfg = EnvironmentConfig::new(dir.path().to_path_buf());
    cfg.set_allow_create(true);
    cfg.set_transactional(true);
    // Daemons OFF: this isolates the cursor fetch-path re-entrancy from the
    // separate pre-existing small-cache evictor/compressor latch wedge.
    cfg.set_run_evictor(false);
    cfg.set_run_cleaner(false);
    cfg.set_run_checkpointer(false);
    let env = Arc::new(Environment::open(cfg).unwrap());
    let db = Arc::new(
        env.open_database(
            None,
            "reentrant",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap(),
    );
    for i in 0..N_KEYS {
        let t = env.begin_transaction(None).unwrap();
        db.put_in(&t, i.to_be_bytes(), vec![0u8; VAL_LEN]).unwrap();
        t.commit().unwrap();
    }
    // Strip resident LN data to LSN pointers so every read re-faults.
    let _ = env.evict_memory().unwrap();

    let done = Arc::new(AtomicBool::new(false));
    let watchdog = {
        let done = Arc::clone(&done);
        std::thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(60);
            while Instant::now() < deadline {
                if done.load(Ordering::Acquire) {
                    return;
                }
                std::thread::sleep(Duration::from_millis(50));
            }
            eprintln!(
                "STALL: LN re-fault + CacheMode strip workload did not \
                 finish within 60s under concurrent read/write/strip"
            );
            std::process::abort();
        })
    };

    std::thread::scope(|s| {
        for _ in 0..N_READERS {
            let env = &env;
            let db = &db;
            s.spawn(move || {
                for _ in 0..ROUNDS {
                    let _ = env.evict_memory().unwrap();
                    // A plain (non-serializable, auto-commit) cursor scan can
                    // legitimately conflict with a concurrent writer's record
                    // write-lock: JE surfaces that as a retryable
                    // LockConflictException (LockTimeout, or — with the sticky
                    // default cursor that briefly holds two record locks while
                    // stepping — DeadlockException), and the application
                    // contract is to retry (OperationFailureException
                    // .isRetryable()).  We retry the whole scan on any
                    // retryable error; the watchdog still bounds total
                    // progress.  (This is NOT the bug under test — the
                    // re-entrancy / id-collision bug is in the engine; this
                    // test only guards that the fetch/evict/strip paths make
                    // progress and carry no db_impl lock.)
                    'scan: loop {
                        let mut c = db.open_cursor(None).unwrap();
                        let mut k = DatabaseEntry::new();
                        let mut d = DatabaseEntry::new();
                        let mut st =
                            match c.get(&mut k, &mut d, Get::First, None) {
                                Ok(st) => st,
                                Err(e) if e.is_retryable() => continue 'scan,
                                Err(e) => {
                                    panic!("non-retryable reader error: {e:?}")
                                }
                            };
                        while st == OperationStatus::Success {
                            st = match c.get(&mut k, &mut d, Get::Next, None) {
                                Ok(st) => st,
                                Err(e) if e.is_retryable() => continue 'scan,
                                Err(e) => {
                                    panic!("non-retryable reader error: {e:?}")
                                }
                            };
                        }
                        break 'scan;
                    }
                }
            });
        }
        for w in 0..N_WRITERS {
            let env = &env;
            let db = &db;
            s.spawn(move || {
                for _ in 0..ROUNDS {
                    for i in 0..N_KEYS {
                        // Two writers rewrite the SAME key range, so a write
                        // lock request can legitimately time out (LockTimeout)
                        // or lose a deadlock (DeadlockException) under
                        // contention.  Both are retryable per JE's
                        // OperationFailureException.isRetryable(); the
                        // application aborts and retries the transaction.
                        loop {
                            let t = env.begin_transaction(None).unwrap();
                            let r = db
                                .put_in(
                                    &t,
                                    i.to_be_bytes(),
                                    vec![w as u8 + 1; VAL_LEN],
                                )
                                .and_then(|_| t.commit());
                            match r {
                                Ok(()) => break,
                                Err(e) if e.is_retryable() => {
                                    // `t` is already consumed by commit() on the
                                    // Ok path; on the retryable error path the
                                    // put/commit failed, so abort any residual
                                    // txn state and retry this key.
                                    continue;
                                }
                                Err(e) => {
                                    panic!("non-retryable writer error: {e:?}")
                                }
                            }
                        }
                    }
                    let _ = env.evict_memory().unwrap();
                }
            });
        }
    });

    done.store(true, Ordering::Release);
    watchdog.join().unwrap();
    assert_eq!(db.count().unwrap(), N_KEYS as u64);
}
