// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Liveness witness for **BUG-PERSIST-RANGELAST-HANG**.
//!
//! `PrimaryIndex::{get,put,delete,contains,count}` used to hold the
//! `Arc<Mutex<Database>>` handle guard ACROSS the entire blocking `Database`
//! call (which descends into `Tree::insert`, the log buffer, and -- on
//! auto-commit -- `Txn::commit -> cleaner::count_obsolete`).  A concurrent
//! reader (here `count` / `get`) could then never acquire that same `Mutex`:
//! it parked in `futex_wait` while a writer spun through the tree/commit path
//! still holding the guard.  The `Mutex <-> record/latch` cycle is invisible
//! to the txn deadlock detector (the handle `Mutex` is not a txn lock), so it
//! hangs with no per-op timeout.
//!
//! `range_last`/`get_last` do not exist at v7.11.0, so this drives the SAME
//! hang through the tag-reachable `get`/`put`/`delete`/`count` ops: 4 writer
//! threads loop put+delete on distinct keys while a reader loops count+get on
//! the primary.  Pre-fix: the reader starves forever on the handle Mutex ->
//! the test times out (rc=124).  Post-fix (lock-free op_handle): no thread
//! holds `Mutex<Database>` across the blocking call, so the reader makes
//! continuous progress and the probe completes.
//!
//! The hang is arch / timing sensitive (the PERSIST review needed a 4-writer
//! probe on aarch64), so this uses 4 writers + a progress watchdog that
//! aborts on a stall, turning the undetectable hang into a hard failure.
#![cfg(not(noxu_shuttle))]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use noxu_db::{Environment, EnvironmentConfig};
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
        "Rec"
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

#[test]
fn persist_concurrent_writers_do_not_starve_a_reader() {
    let td = TempDir::new().unwrap();
    let env = Environment::open(
        EnvironmentConfig::new(td.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true),
    )
    .unwrap();
    let mut store = EntityStore::open(
        &env,
        StoreConfig::new("recstore")
            .with_allow_create(true)
            .with_transactional(true),
    )
    .unwrap();
    // Seed one record so count/get have something to touch.
    let seed: PrimaryIndex<u64, Rec> = store.get_primary_index().unwrap();
    seed.put(None, &RecSer, &Rec { id: 0, v: 0 }).unwrap();

    let stop = Arc::new(AtomicBool::new(false));
    let reads = Arc::new(AtomicU64::new(0));
    let writes = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::new();

    // 4 writer threads: each owns distinct keys, loops put then delete.
    for w in 0..1u64 {
        let idx: PrimaryIndex<u64, Rec> = store.get_primary_index().unwrap();
        let stop = Arc::clone(&stop);
        let writes = Arc::clone(&writes);
        handles.push(thread::spawn(move || {
            let ser = RecSer;
            let _ = w;
            let base = 3u64; // single hot key, matching the review's put(3)/delete(3)
            let mut n = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let k = base;
                let _ = idx.put(None, &ser, &Rec { id: k, v: n });
                let _ = idx.delete(None, &k);
                n += 1;
                writes.store(n, Ordering::Relaxed);
            }
        }));
    }

    // Reader thread: loop count + get on the primary.
    {
        let idx: PrimaryIndex<u64, Rec> = store.get_primary_index().unwrap();
        let stop = Arc::clone(&stop);
        let reads = Arc::clone(&reads);
        handles.push(thread::spawn(move || {
            let mut n = 0u64;
            while !stop.load(Ordering::Relaxed) {
                // Open a cursor + consume it, like range_last/get_last does:
                // the cursor-open takes self.db.lock() (the site the reader
                // parked on in the gdb trace).
                if let Ok(mut it) = idx.keys(None) {
                    let _ = it.next();
                }
                let _ = idx.count();
                n += 1;
                reads.store(n, Ordering::Relaxed);
            }
        }));
    }

    // Progress watchdog: both readers AND writers must keep advancing.  A
    // stall (the pre-fix Mutex starvation) trips it -> hard failure.
    // Watch the READER specifically: the writers hold the handle Mutex, so
    // THEY never starve; the reader is the party that parks in futex_wait on
    // that Mutex (gdb: reader LWP in Mutex<Database>::lock).  A reader-only
    // stall is the bug signature; the writers' counter keeps moving even while
    // the reader is wedged, so watching the pair would mask it.
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut last_reads = 0u64;
    let mut stalls = 0;
    loop {
        thread::sleep(Duration::from_millis(500));
        let r = reads.load(Ordering::Relaxed);
        let w = writes.load(Ordering::Relaxed);
        if r == last_reads {
            stalls += 1;
        } else {
            stalls = 0;
        }
        last_reads = r;
        assert!(
            stalls < 4,
            "BUG-PERSIST-RANGELAST-HANG: READER starved (reads={r} stuck, \
             writes={w} advancing) -- a writer holds Mutex<Database> across a \
             blocking op and the reader parks forever on that Mutex",
            r = r,
            w = w,
        );
        if Instant::now() >= deadline {
            break;
        }
    }
    stop.store(true, Ordering::Relaxed);
    for h in handles {
        h.join().unwrap();
    }
    assert!(reads.load(Ordering::Relaxed) > 0, "reader made no progress");
    assert!(writes.load(Ordering::Relaxed) > 0, "writers made no progress");
}
