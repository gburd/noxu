// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! A8 companion (step-4): a READ_COMMITTED read through `PrimaryIndex` must
//! never observe an aborting writer's uncommitted value — the same torn-read
//! invariant as `secondary_decisions_test::test_x10`, but driven through the
//! DPL `PrimaryIndex` (whose record ops now go through the lock-free
//! `op_handle` after A8).  The op_handle read path is `Database::get_in ->
//! CursorImpl::search(SearchMode::Set)`, which A14-min made torn-read-safe, so
//! A8 does NOT expose a new RC isolation hole.
#![cfg(not(noxu_shuttle))]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;

use noxu_db::{Environment, EnvironmentConfig};
use noxu_persist::{
    Entity, EntitySerializer, EntityStore, PrimaryIndex, Result, StoreConfig,
};
use tempfile::TempDir;

#[derive(Clone, Debug, PartialEq)]
struct Rec {
    id: u64,
    tag: u8,
}
impl Entity for Rec {
    type PrimaryKey = u64;
    fn primary_key(&self) -> &u64 {
        &self.id
    }
    fn entity_name() -> &'static str {
        "RecT"
    }
}
struct RecSer;
impl EntitySerializer<Rec> for RecSer {
    fn serialize(&self, r: &Rec) -> Result<Vec<u8>> {
        let mut b = Vec::with_capacity(9);
        b.extend_from_slice(&r.id.to_be_bytes());
        b.push(r.tag);
        Ok(b)
    }
    fn deserialize(&self, bytes: &[u8]) -> Result<Rec> {
        Ok(Rec {
            id: u64::from_be_bytes(bytes[0..8].try_into().unwrap()),
            tag: bytes[8],
        })
    }
}

#[test]
fn primary_index_rc_read_never_sees_an_aborting_writers_value() {
    let td = TempDir::new().unwrap();
    let env = Environment::open(
        EnvironmentConfig::new(td.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true),
    )
    .unwrap();
    let mut store = EntityStore::open(
        &env,
        StoreConfig::new("rectstore")
            .with_allow_create(true)
            .with_transactional(true),
    )
    .unwrap();
    // Seed id=7 tag=b'A' (committed).
    let seed: PrimaryIndex<u64, Rec> = store.get_primary_index().unwrap();
    seed.put(None, &RecSer, &Rec { id: 7, tag: b'A' }).unwrap();

    let torn = Arc::new(AtomicBool::new(false));
    let done = Arc::new(AtomicBool::new(false));
    let iters = Arc::new(AtomicU64::new(0));

    // RC reader: get(7); must only ever see the committed tag b'A'.
    let r_idx: PrimaryIndex<u64, Rec> = store.get_primary_index().unwrap();
    // The reader drives RC-style reads through `get(None)` (auto-commit:
    // read-then-release), which exercises the same search(Set) + lock_ln path
    // the op_handle read uses, so A14-min's torn-read guard applies.
    let r_torn = Arc::clone(&torn);
    let r_done = Arc::clone(&done);
    let r_it = Arc::clone(&iters);
    let reader = thread::spawn(move || {
        let ser = RecSer;
        while !r_done.load(Ordering::Relaxed) {
            if let Ok(Some(rec)) = r_idx.get(None, &ser, &7u64)
                && rec.tag != b'A'
            {
                r_torn.store(true, Ordering::Relaxed);
            }
            r_it.fetch_add(1, Ordering::Relaxed);
        }
    });

    // Writer: abort cycle -- put id=7 tag=b'B' (uncommitted), then abort.
    let w_idx: PrimaryIndex<u64, Rec> = store.get_primary_index().unwrap();
    for _ in 0..5000 {
        let txn = env.begin_transaction(None).unwrap();
        w_idx.put(Some(&txn), &RecSer, &Rec { id: 7, tag: b'B' }).unwrap();
        txn.abort().unwrap();
    }
    done.store(true, Ordering::Relaxed);
    reader.join().unwrap();

    assert!(
        !torn.load(Ordering::Relaxed),
        "PrimaryIndex RC read observed an aborting writer's uncommitted value. \
         iters={}",
        iters.load(Ordering::Relaxed)
    );
}
