// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Shuttle concurrency-permutation model of the **consolidation-array Log
//! Write Latch** (`consolidation.rs`, Aether VLDB'10 tech 3 / Silo SOSP'13 /
//! WiredTiger `log_slot.c __wti_log_slot_join`).
//!
//! The whole file compiles to nothing unless built with `--cfg noxu_shuttle`,
//! so `cargo test` and every production build are unaffected.  Under the cfg,
//! `ConsolidationArray`'s `AtomicPtr`/`AtomicBool` resolve (through
//! `noxu_util::dst_sync::atomic`) to shuttle-instrumented atomics, so shuttle's
//! scheduler explores the join / leader-handoff / follower-spin interleavings
//! of the **real** funnel code.
//!
//! # What is modelled
//!
//! N committer threads each run the exact production sequence:
//!   1. `ConsolidationArray::join(&req)`  — single-CAS join (real code).
//!   2. If leader: take a shuttle `Mutex` standing in for `log_write_latch`,
//!      then `run_as_leader(&req, assign)` where `assign` mimics
//!      `LogManager::assign_slot`'s serial LSN work (a monotonic fetch_add of
//!      an "LSN cursor" plus a `prev_offset` = previous cursor).
//!   3. If follower: `wait_as_follower(&req)` — spin on the done flag (real
//!      code); shuttle preempts at the `done` load so every interleaving of
//!      "leader publishes vs. follower observes" is explored.
//!
//! # The safety oracle (proof obligations 1, 2, 4, 5)
//!
//!   * **LSN monotonicity + contiguity + uniqueness (#1)** — every committer
//!     gets a UNIQUE, strictly-increasing LSN; the set of all assigned LSNs is
//!     exactly `{0, 1, .., N-1}` (contiguous, no gaps, no duplicates).  A gap
//!     would break `prev_offset` chaining; a duplicate would mean two entries
//!     stamped the same LSN.
//!   * **prev_offset chain integrity (#4)** — each committer's `prev_offset`
//!     equals the LSN immediately before its own (the entry it chains to), so
//!     the per-file back-chain is intact.
//!   * **No lost / double entries on leader handoff (#2)** — the multiset of
//!     served requests equals the set of committers exactly once each (checked
//!     via the contiguous-LSN oracle: N committers => LSNs 0..N-1, so none
//!     dropped and none processed twice).
//!   * **Durable watermark monotonicity (#5)** — after each committer's LSN is
//!     assigned, a CAS-max durable watermark that only ever advances covers
//!     that committer's LSN and never regresses (models `last_synced_lsn`).
//!
//! # Running
//!
//! ```sh
//! RUSTFLAGS="--cfg noxu_shuttle" cargo test -p noxu-log --test shuttle_consolidation
//! ```
#![cfg(noxu_shuttle)]

use std::sync::atomic::Ordering;

use noxu_log::consolidation::{ConsolidationArray, Join, Request};
use shuttle::sync::Mutex;
use shuttle::sync::atomic::{AtomicU64, AtomicUsize};
use shuttle::sync::Arc;

/// Interleavings shuttle explores per test.
const ITERATIONS: usize = 5_000;

/// The serial LSN state the leader mutates under the (stand-in) log-write
/// latch — mimics `FileManager`'s `next_available_lsn` / `last_used_lsn`.
struct LsnState {
    /// Next LSN to assign (monotonic cursor). 0-based for a compact oracle.
    next: u64,
    /// The LSN of the immediately-prior assigned entry (prev_offset source);
    /// `u64::MAX` sentinel means "no prior entry" (first ever).
    last: u64,
}

/// One committer's assignment result recorded for the oracle.
#[derive(Clone, Copy)]
struct Assigned {
    lsn: u64,
    prev: u64,
}

/// SAFETY ORACLE: N concurrent committers through the REAL consolidation
/// array. Every LSN is unique + contiguous (0..N-1) + strictly increasing in
/// assignment order; every prev_offset chains to the immediately-prior LSN;
/// the durable watermark never regresses and covers every committed LSN.
#[test]
fn consolidation_lsn_monotonic_no_loss() {
    shuttle::check_random(
        || {
            const N: usize = 4;
            let arr: Arc<ConsolidationArray<usize, Assigned>> =
                Arc::new(ConsolidationArray::new());
            // Stands in for `log_write_latch`: the leader holds it for the
            // whole batch, so leaders of successive batches serialise on it.
            let lwl = Arc::new(Mutex::new(LsnState { next: 0, last: u64::MAX }));
            // Records each committer's assignment (indexed by committer id).
            let results: Arc<Vec<Mutex<Option<Assigned>>>> =
                Arc::new((0..N).map(|_| Mutex::new(None)).collect());
            // Single durable watermark (last_synced_lsn), CAS-max advanced.
            let last_synced = Arc::new(AtomicU64::new(0));
            // Counts how many times `assign` ran (must equal N exactly: no
            // lost, no double).
            let assign_calls = Arc::new(AtomicUsize::new(0));

            let handles: Vec<_> = (0..N)
                .map(|id| {
                    let arr = Arc::clone(&arr);
                    let lwl = Arc::clone(&lwl);
                    let results = Arc::clone(&results);
                    let last_synced = Arc::clone(&last_synced);
                    let assign_calls = Arc::clone(&assign_calls);
                    shuttle::thread::spawn(move || {
                        let req = Request::new(id);
                        let assigned = match arr.join(&req) {
                            Join::Leader => {
                                // Leader holds the LWL for the whole batch and
                                // stamps LSNs in arrival order.  The `assign`
                                // closure mutates the LSN state through the
                                // guard captured here (shuttle's Mutex is not
                                // reentrant, so we lock ONCE and mutate the
                                // guard inside the closure).
                                let calls2 = Arc::clone(&assign_calls);
                                let mut st = lwl.lock().unwrap();
                                arr.run_as_leader(&req, |_committer_id| {
                                    calls2.fetch_add(1, Ordering::SeqCst);
                                    // Serial LSN assign — the crux: unique,
                                    // strictly increasing, contiguous, and
                                    // prev_offset = the immediately-prior LSN.
                                    let lsn = st.next;
                                    let prev = st.last;
                                    st.next += 1;
                                    st.last = lsn;
                                    Assigned { lsn, prev }
                                })
                            }
                            Join::Follower => arr.wait_as_follower(&req),
                        };
                        // Record + advance the durable watermark for our LSN.
                        *results[id].lock().unwrap() = Some(assigned);
                        let old = last_synced.load(Ordering::SeqCst);
                        bump_max(&last_synced, assigned.lsn);
                        let newv = last_synced.load(Ordering::SeqCst);
                        assert!(
                            newv >= old,
                            "durable watermark regressed: {old} -> {newv}"
                        );
                        assert!(
                            newv >= assigned.lsn,
                            "durable watermark {newv} does not cover \
                             committed LSN {}",
                            assigned.lsn
                        );
                    })
                })
                .collect();

            for h in handles {
                h.join().unwrap();
            }

            oracle(&results, &assign_calls, N);
        },
        ITERATIONS,
    );
}

/// The shared oracle over all recorded assignments.
fn oracle(
    results: &[Mutex<Option<Assigned>>],
    assign_calls: &AtomicUsize,
    n: usize,
) {
    // No lost / no double: assign ran exactly once per committer.
    assert_eq!(
        assign_calls.load(Ordering::SeqCst),
        n,
        "assign must run exactly N times (no lost, no double entry)"
    );

    let mut all: Vec<Assigned> = results
        .iter()
        .map(|c| c.lock().unwrap().expect("every committer must be served"))
        .collect();

    // Unique + contiguous 0..n-1 (no gaps that break prev_offset chaining;
    // no duplicate LSNs).
    let mut lsns: Vec<u64> = all.iter().map(|a| a.lsn).collect();
    lsns.sort_unstable();
    for (i, &lsn) in lsns.iter().enumerate() {
        assert_eq!(
            lsn, i as u64,
            "LSNs must be contiguous 0..{n}: got {lsns:?}"
        );
    }

    // prev_offset chain: order the assignments by LSN; each entry's prev must
    // equal the LSN before it (sentinel MAX for the first).
    all.sort_unstable_by_key(|a| a.lsn);
    for (i, a) in all.iter().enumerate() {
        let expected_prev =
            if i == 0 { u64::MAX } else { all[i - 1].lsn };
        assert_eq!(
            a.prev, expected_prev,
            "prev_offset chain broken at LSN {}: prev={} expected {}",
            a.lsn, a.prev, expected_prev
        );
    }
}

/// Advance `cell` to at least `v` (lock-free max).
fn bump_max(cell: &AtomicU64, v: u64) {
    let mut cur = cell.load(Ordering::SeqCst);
    while cur < v {
        match cell.compare_exchange(cur, v, Ordering::SeqCst, Ordering::SeqCst)
        {
            Ok(_) => break,
            Err(a) => cur = a,
        }
    }
}

/// NEGATIVE CONTROL (the test has teeth): if the leader does NOT hold the
/// stand-in LWL while assigning — so two leaders of successive batches can run
/// `assign` concurrently — shuttle MUST find an interleaving that violates LSN
/// uniqueness/contiguity.  This proves the safety oracle actually detects the
/// exact race the production `_lwl_guard` in `log_internal` closes.  We assert
/// the buggy variant PANICS under shuttle; if it ever passes, the oracle is
/// toothless and the positive test above is meaningless.
#[test]
#[should_panic]
fn negative_control_no_latch_races() {
    shuttle::check_random(
        || {
            const N: usize = 4;
            let arr: Arc<ConsolidationArray<usize, Assigned>> =
                Arc::new(ConsolidationArray::new());
            // A NON-atomic-safe cursor read/modify/write WITHOUT the LWL: two
            // concurrent leaders can interleave their read+increment and stamp
            // the same LSN.  Modelled with two separate atomic ops (load then
            // store) that shuttle can preempt between.
            let next = Arc::new(AtomicU64::new(0));
            let last = Arc::new(AtomicU64::new(u64::MAX));
            let results: Arc<Vec<Mutex<Option<Assigned>>>> =
                Arc::new((0..N).map(|_| Mutex::new(None)).collect());
            let assign_calls = Arc::new(AtomicUsize::new(0));

            let handles: Vec<_> = (0..N)
                .map(|id| {
                    let arr = Arc::clone(&arr);
                    let next = Arc::clone(&next);
                    let last = Arc::clone(&last);
                    let results = Arc::clone(&results);
                    let assign_calls = Arc::clone(&assign_calls);
                    shuttle::thread::spawn(move || {
                        let req = Request::new(id);
                        let assigned = match arr.join(&req) {
                            Join::Leader => {
                                let next2 = Arc::clone(&next);
                                let last2 = Arc::clone(&last);
                                let calls2 = Arc::clone(&assign_calls);
                                // NO LWL held — the bug.
                                arr.run_as_leader(&req, |_| {
                                    calls2.fetch_add(1, Ordering::SeqCst);
                                    // Racy read-modify-write: load, (preempt),
                                    // store next+1.
                                    let lsn = next2.load(Ordering::SeqCst);
                                    let prev = last2.load(Ordering::SeqCst);
                                    next2.store(lsn + 1, Ordering::SeqCst);
                                    last2.store(lsn, Ordering::SeqCst);
                                    Assigned { lsn, prev }
                                })
                            }
                            Join::Follower => arr.wait_as_follower(&req),
                        };
                        *results[id].lock().unwrap() = Some(assigned);
                    })
                })
                .collect();
            for h in handles {
                h.join().unwrap();
            }
            oracle(&results, &assign_calls, N);
        },
        ITERATIONS,
    );
}
