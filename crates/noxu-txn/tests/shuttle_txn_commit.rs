// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Shuttle concurrency-permutation tests for the `TxnManager`
//! begin / commit / abort path (DST txn wave).
//!
//! The whole file compiles to nothing unless built with `--cfg noxu_shuttle`,
//! so the default `cargo test` and every production build are unaffected.
//! Under the cfg, `TxnManager`'s `all_txns` map (a `noxu_sync::RwLock` in
//! production) resolves through the parking_lot-over-shuttle wrapper
//! `noxu_util::dst_sync_pl`, its `next_txn_id` allocator resolves through
//! `noxu_util::dst_sync::atomic`, and the lock manager's locker-label registry
//! resolves through `noxu_util::dst_sync` — so shuttle's scheduler explores the
//! begin / commit / abort interleavings of the *real* `TxnManager`.
//!
//! # Invariants (mapped to `noxu-spec` `wal_commit` where a spec analogue
//! exists)
//!
//!   * **txn-id-uniqueness** — no two concurrently-begun transactions are
//!     handed the same id.  Maps to the `wal_commit` spec's strictly-monotonic
//!     LSN allocator invariant: `next_txn_id.fetch_add` must be a linearizable
//!     allocator even when many threads begin at once.
//!   * **commit/abort atomicity** — a transaction ends up in *exactly one* of
//!     {committed, aborted}, never both and never neither.  The manager's
//!     `all_txns` entry is removed exactly once; the `n_commits` / `n_aborts`
//!     counters partition the finished txns.  Maps to the `wal_commit` spec's
//!     2-state committed invariant (a txn transitions to Committed at most
//!     once).
//!   * **all_txns integrity** — under interleaved begin/commit/abort the map
//!     has no lost entry (a begun-but-not-finished txn is present) and no
//!     leaked entry (a finished txn is absent).  At quiescence the map size
//!     equals begins − commits − aborts.
//!   * **no lost wakeup** — begin/commit/abort never hang (the join gate below
//!     would time out shuttle); every spawned thread completes.
//!
//! # Not vacuous
//!
//! `id_allocation_is_unique` collects every allocated id into a shared set and
//! asserts no duplicate.  If `next_txn_id` were a plain non-atomic counter (or
//! routed through a non-instrumented primitive), shuttle would schedule two
//! `fetch_add`s to observe the same pre-increment value and the duplicate-id
//! assert would fire — see the `not_vacuous` note in
//! `interleaved_commit_abort_partition` for the analogous broken-interleaving
//! argument on the map.
//!
//! # Running
//!
//! ```sh
//! RUSTFLAGS="--cfg noxu_shuttle" cargo test -p noxu-txn --test shuttle_txn_commit
//! ```
#![cfg(noxu_shuttle)]

use std::collections::HashSet;

use noxu_txn::{LockManager, Locker, TxnManager};
use shuttle::sync::atomic::{AtomicUsize, Ordering};
use shuttle::sync::{Arc, Mutex};

/// Number of interleavings shuttle explores per test.
const ITERATIONS: usize = 2_000;

/// txn-id-uniqueness: N threads each begin a transaction concurrently.  Every
/// allocated id must be distinct — no two `begin_txn` calls may observe the
/// same pre-increment value of `next_txn_id`.
///
/// Not vacuous: the ids are gathered into a shared `HashSet`; a duplicate would
/// fail the `insert`-returned-`true` assert.  With the allocator routed through
/// the seam, shuttle is free to interleave the two `fetch_add`s in any order,
/// so a lost-update bug (non-atomic increment) would be caught.
#[test]
fn id_allocation_is_unique() {
    shuttle::check_random(
        || {
            let lm = Arc::new(LockManager::new());
            let mgr = Arc::new(TxnManager::new(lm));
            let ids = Arc::new(Mutex::new(Vec::<i64>::new()));

            let mut handles = Vec::new();
            for _ in 0..3 {
                let mgr = Arc::clone(&mgr);
                let ids = Arc::clone(&ids);
                handles.push(shuttle::thread::spawn(move || {
                    let txn = mgr.begin_txn();
                    ids.lock().unwrap().push(txn.id());
                }));
            }
            for h in handles {
                h.join().unwrap();
            }

            // Every id distinct.
            let collected = ids.lock().unwrap();
            let mut seen = HashSet::new();
            for &id in collected.iter() {
                assert!(
                    seen.insert(id),
                    "duplicate txn id {id} allocated (ids={collected:?})"
                );
            }
            // The active map has exactly the three begun txns (none finished).
            assert_eq!(
                mgr.n_active_txns(),
                3,
                "all three begun txns should be active"
            );
        },
        ITERATIONS,
    );
}

/// commit/abort atomicity + all_txns integrity: each of several concurrently
/// begun transactions is finished by exactly one of commit or abort.  After all
/// threads join, the manager's active map must be empty (every entry removed
/// exactly once) and the commit + abort counters must sum to the number of
/// txns (each txn accounted for exactly once — never both, never neither).
///
/// Not vacuous: if `commit_txn` / `abort_txn` did not remove from `all_txns`
/// atomically w.r.t. a concurrent `begin_txn` insert (e.g. a check-then-act on
/// the map instead of a single `write()`), shuttle could interleave a remove
/// against an insert of the same id and leave a lost or leaked entry — the
/// `n_active_txns() == 0` assert would fire.
#[test]
fn interleaved_commit_abort_partition() {
    shuttle::check_random(
        || {
            let lm = Arc::new(LockManager::new());
            let mgr = Arc::new(TxnManager::new(lm));
            // Each thread decides commit vs abort by a fixed parity so the
            // outcome set is deterministic per thread but the *interleaving*
            // of the map mutations is explored by shuttle.
            const N: usize = 4;
            let committed = Arc::new(AtomicUsize::new(0));
            let aborted = Arc::new(AtomicUsize::new(0));

            let mut handles = Vec::new();
            for i in 0..N {
                let mgr = Arc::clone(&mgr);
                let committed = Arc::clone(&committed);
                let aborted = Arc::clone(&aborted);
                handles.push(shuttle::thread::spawn(move || {
                    let txn = mgr.begin_txn();
                    let id = txn.id();
                    if i % 2 == 0 {
                        mgr.commit_txn(id);
                        committed.fetch_add(1, Ordering::SeqCst);
                    } else {
                        mgr.abort_txn(id);
                        aborted.fetch_add(1, Ordering::SeqCst);
                    }
                }));
            }
            for h in handles {
                h.join().unwrap();
            }

            // commit/abort atomicity: every txn is in exactly one bucket.
            let c = committed.load(Ordering::SeqCst);
            let a = aborted.load(Ordering::SeqCst);
            assert_eq!(
                c + a,
                N,
                "every txn must be exactly one of committed/aborted \
                 (committed={c}, aborted={a})"
            );

            // all_txns integrity: no lost or leaked entry.
            assert_eq!(
                mgr.n_active_txns(),
                0,
                "every finished txn must be removed from all_txns"
            );

            // The stats counters agree with the observed outcome.
            let stats = mgr.get_stats();
            assert_eq!(stats.n_begins, N as u64);
            assert_eq!(stats.n_commits, c as u64);
            assert_eq!(stats.n_aborts, a as u64);
            assert_eq!(stats.n_active, 0);
        },
        ITERATIONS,
    );
}

/// Mixed begin-while-finishing: one thread begins a fresh txn while two others
/// finish their own (one commit, one abort).  Stresses concurrent
/// insert-vs-remove on `all_txns`.  The still-open txn must remain in the map
/// (lost-entry check) and the finished ones must be gone (leaked-entry check).
///
/// no lost wakeup: all three threads must complete (the join gate would hang
/// shuttle otherwise).
#[test]
fn concurrent_begin_and_finish_map_integrity() {
    shuttle::check_random(
        || {
            let lm = Arc::new(LockManager::new());
            let mgr = Arc::new(TxnManager::new(lm));

            // Pre-begin two txns that will be finished concurrently.
            let t_commit = mgr.begin_txn();
            let t_abort = mgr.begin_txn();
            let commit_id = t_commit.id();
            let abort_id = t_abort.id();

            // Track the id of the txn begun concurrently (for the lost-entry
            // check) via a shared cell.
            let open_id = Arc::new(Mutex::new(None::<i64>));

            let h_begin = {
                let mgr = Arc::clone(&mgr);
                let open_id = Arc::clone(&open_id);
                shuttle::thread::spawn(move || {
                    let t = mgr.begin_txn();
                    *open_id.lock().unwrap() = Some(t.id());
                })
            };
            let h_commit = {
                let mgr = Arc::clone(&mgr);
                shuttle::thread::spawn(move || {
                    mgr.commit_txn(commit_id);
                })
            };
            let h_abort = {
                let mgr = Arc::clone(&mgr);
                shuttle::thread::spawn(move || {
                    mgr.abort_txn(abort_id);
                })
            };

            h_begin.join().unwrap();
            h_commit.join().unwrap();
            h_abort.join().unwrap();

            // Exactly the concurrently-begun txn remains active.
            assert_eq!(
                mgr.n_active_txns(),
                1,
                "only the still-open txn should remain in all_txns"
            );
            // And the ids are distinct across the three.
            let open = open_id.lock().unwrap().expect("open txn id recorded");
            assert_ne!(open, commit_id);
            assert_ne!(open, abort_id);
            assert_ne!(commit_id, abort_id);
        },
        ITERATIONS,
    );
}
