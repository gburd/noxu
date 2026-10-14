// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Deterministic liveness witness for **BUG-PERSIST-RANGELAST-HANG**.
//!
//! The hard-hang variant found on parity/integrate needed `range_last`'s
//! restart loop (which does not exist at v7.11.0), but the DEADLOCK CLASS
//! itself exists at the tag: every `PrimaryIndex` record op holds the
//! `Arc<Mutex<Database>>` handle guard ACROSS the blocking `Database` call
//! (primary_index.rs:199 get, :287 put, :317 put_no_overwrite, :347 delete,
//! :391 contains, :406 count, :426/:454 cursor opens).  With two EXPLICIT
//! transactions that each hold a record lock across the Mutex boundary, that
//! yields a Mutex<->record-lock cycle INVISIBLE to the txn deadlock detector
//! (the handle Mutex is not a txn lock), so it hangs with no abort — exactly
//! the F1 / BUG-SEC-MUTEX-DEADLOCK (A7) class.
//!
//! Sequence (per T2 op variant):
//!   T1: begin txn1; put(txn1, k1)        -> write-locks k1 (held to commit);
//!                                           Mutex released after the call.
//!   T2: begin txn2; <op>(txn2, k1)       -> takes self.db.lock(), then BLOCKS
//!                                           on k1's write lock WHILE HOLDING
//!                                           the Mutex.
//!   T1: (once T2 is parked) put(txn1, k2)-> needs self.db.lock(), held by T2
//!                                           -> blocks.  txn1 can never commit,
//!                                           T2 never gets k1: deadlock.
//!
//! At the tag this hangs (rc=124 under `timeout`, caught here by a watchdog
//! that aborts).  With A8's lock-free `op_handle`, T2 blocks on k1 holding NO
//! Mutex, T1's second put proceeds, txn1 commits, T2 then gets k1 -> GREEN.
#![cfg(not(noxu_shuttle))]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use noxu_db::{Environment, EnvironmentConfig, TransactionConfig};
use noxu_persist::{
    Entity, EntitySerializer, EntityStore, PrimaryIndex, Result, StoreConfig,
};
use tempfile::TempDir;

#[derive(Clone, Debug, PartialEq)]
struct Rec {
    id: u64,
    v: u64,
}
impl Entity for Rec {
    type PrimaryKey = u64;
    fn primary_key(&self) -> &u64 {
        &self.id
    }
    fn entity_name() -> &'static str {
        "RecD"
    }
}
struct RecSer;
impl EntitySerializer<Rec> for RecSer {
    fn serialize(&self, r: &Rec) -> Result<Vec<u8>> {
        let mut b = Vec::with_capacity(16);
        b.extend_from_slice(&r.id.to_be_bytes());
        b.extend_from_slice(&r.v.to_be_bytes());
        Ok(b)
    }
    fn deserialize(&self, bytes: &[u8]) -> Result<Rec> {
        Ok(Rec {
            id: u64::from_be_bytes(bytes[0..8].try_into().unwrap()),
            v: u64::from_be_bytes(bytes[8..16].try_into().unwrap()),
        })
    }
}

#[derive(Clone, Copy, Debug)]
enum T2Op {
    Get,
    Contains,
    Delete,
    Count,
    CursorNext,
}

/// Run one deadlock-shape scenario with the given T2 op.  Returns when the
/// scenario COMPLETES (no deadlock); a deadlock is caught by the process-level
/// watchdog (abort) armed by the caller.
fn run_scenario(op: T2Op) {
    let td = TempDir::new().unwrap();
    // Long lock timeout: we want the deadlock to HANG (watchdog catches it),
    // not resolve into a spurious LockTimeout before the cycle is proven.
    let env = Environment::open(
        EnvironmentConfig::new(td.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true),
    )
    .unwrap();
    let env = Arc::new(env);
    let mut store = EntityStore::open(
        &env,
        StoreConfig::new("recdstore")
            .with_allow_create(true)
            .with_transactional(true),
    )
    .unwrap();

    let k1: u64 = 1;
    let k2: u64 = 2;
    // Seed both keys so get/contains/delete/count/cursor all have a target.
    let seed: PrimaryIndex<u64, Rec> = store.get_primary_index().unwrap();
    seed.put(None, &RecSer, &Rec { id: k1, v: 0 }).unwrap();
    seed.put(None, &RecSer, &Rec { id: k2, v: 0 }).unwrap();

    // One shared PrimaryIndex (one self.db Mutex) for both txns.
    let idx: PrimaryIndex<u64, Rec> = store.get_primary_index().unwrap();
    let idx = Arc::new(idx);

    // Handshake signals.
    let t2_in_op = Arc::new(AtomicBool::new(false)); // T2 entered its op
    let k1_locked = Arc::new(AtomicBool::new(false)); // T1 holds k1 write lock

    // T1: holds k1's write lock in txn1, then (after T2 parks) puts k2.
    let t1_env = Arc::clone(&env);
    let t1_idx = Arc::clone(&idx);
    let t1_parked = Arc::clone(&t2_in_op);
    let k1_locked_t1 = Arc::clone(&k1_locked);
    let t1 = thread::spawn(move || {
        let ser = RecSer;
        let tc = TransactionConfig::new().with_lock_timeout_ms(300_000);
        let txn1 = t1_env.begin_transaction(Some(&tc)).unwrap();
        // PHASE 1: write-lock k1; held until commit.  Mutex released after.
        t1_idx.put(Some(&txn1), &ser, &Rec { id: k1, v: 1 }).unwrap();
        k1_locked_t1.store(true, Ordering::Release);
        // PHASE 3: wait until T2 has ENTERED its op on k1 (parking on k1's
        // write lock while holding the handle Mutex), plus a grace period so
        // T2 is definitely inside the record-lock wait.
        while !t1_parked.load(Ordering::Acquire) {
            thread::sleep(Duration::from_millis(2));
        }
        thread::sleep(Duration::from_millis(200));
        // Second op needs self.db.lock() -> held by T2 pre-fix -> blocks.
        t1_idx.put(Some(&txn1), &ser, &Rec { id: k2, v: 1 }).unwrap();
        txn1.commit().unwrap();
    });

    // T2: begins txn2, runs <op>(k1) which blocks on k1's write lock.
    let t2_env = Arc::clone(&env);
    let t2_idx = Arc::clone(&idx);
    let t2_flag = Arc::clone(&t2_in_op);
    let k1_locked_t2 = Arc::clone(&k1_locked);
    let t2 = thread::spawn(move || {
        let ser = RecSer;
        let tc = TransactionConfig::new().with_lock_timeout_ms(300_000);
        let txn2 = t2_env.begin_transaction(Some(&tc)).unwrap();
        // PHASE 2: wait until T1 holds k1's write lock, THEN run op(k1).
        while !k1_locked_t2.load(Ordering::Acquire) {
            thread::sleep(Duration::from_millis(2));
        }
        t2_flag.store(true, Ordering::Release);
        // Pre-fix: takes self.db.lock() then blocks on k1 holding the Mutex.
        let res = match op {
            T2Op::Get => t2_idx
                .get(Some(&txn2), &ser, &k1)
                .map(|v| format!("get={:?}", v.map(|r| r.v))),
            T2Op::Contains => t2_idx
                .contains(Some(&txn2), &k1)
                .map(|b| format!("contains={b}")),
            T2Op::Delete => {
                t2_idx.delete(Some(&txn2), &k1).map(|b| format!("delete={b}"))
            }
            T2Op::Count => t2_idx.count().map(|c| format!("count={c}")),
            T2Op::CursorNext => {
                let mut it = t2_idx.keys(Some(&txn2))?;
                let n = it.next();
                Ok(format!("cursor_next_some={}", n.is_some()))
            }
        };
        let _ = res;
        txn2.commit().ok();
        Ok::<(), noxu_persist::PersistError>(())
    });

    t1.join().unwrap();
    let _ = t2.join().unwrap();
}

/// Arm a watchdog that aborts the process if `done` is not set within `secs`,
/// turning the (detector-invisible) Mutex<->record hang into a hard failure.
fn with_watchdog(
    name: &'static str,
    secs: u64,
    body: impl FnOnce() + Send + 'static,
) {
    let done = Arc::new(AtomicBool::new(false));
    let wd_done = Arc::clone(&done);
    let wd = thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(secs);
        while Instant::now() < deadline {
            if wd_done.load(Ordering::Acquire) {
                return;
            }
            thread::sleep(Duration::from_millis(50));
        }
        eprintln!(
            "STALL (BUG-PERSIST-RANGELAST-HANG deadlock class): {name} wedged"
        );
        std::process::abort();
    });
    let worker = thread::spawn(body);
    worker.join().expect("scenario worker panicked");
    done.store(true, Ordering::Release);
    wd.join().unwrap();
}

macro_rules! deadlock_variant {
    ($name:ident, $op:expr) => {
        #[test]
        fn $name() {
            with_watchdog(stringify!($name), 30, || run_scenario($op));
        }
    };
}

// DEADLOCK CARRIERS (RED on the v7.11.0 base -> watchdog SIGABRT; GREEN after
// A8): these take a txn-scoped RECORD lock on k1 AND hold `self.db.lock()`
// across the whole blocking call, so T2 parks on k1 holding the Mutex while
// T1 parks on the Mutex -> the detector-invisible Mutex<->record cycle.
deadlock_variant!(t2_get_vs_t1_two_op_txn, T2Op::Get);
deadlock_variant!(t2_contains_vs_t1_two_op_txn, T2Op::Contains);
deadlock_variant!(t2_delete_vs_t1_two_op_txn, T2Op::Delete);
// LIVENESS CHECKS (GREEN on BOTH base and fix): `count()` takes NO txn (so no
// k1 record-lock edge forms), and the cursor OPEN already scopes `self.db.lock()`
// to the open at the tag (the Mutex is not held across the per-step read), so
// neither carries THIS cycle shape.  They guard that A8's op_handle rewiring of
// those sites does not introduce a hang.
deadlock_variant!(t2_count_vs_t1_two_op_txn, T2Op::Count);
deadlock_variant!(t2_cursor_next_vs_t1_two_op_txn, T2Op::CursorNext);
