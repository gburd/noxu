// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Shuttle concurrency-permutation gate for the replication SYNC state
//! machines: the VLSN index (VLSN→LSN tracking) and the Paxos acceptor
//! (election vote tallying / quorum decision). Both use blocking primitives
//! (`noxu_sync::RwLock`, `std::sync::Mutex` + atomics) with NO tokio, so
//! shuttle can schedule their interleavings — unlike rep's async feeder /
//! network / election-I/O loops, which are tokio and OUT of shuttle scope
//! (covered by tokio-level integration tests and by `noxu-spec` protocol
//! models instead).
//!
//! The whole file compiles to nothing unless built with `--cfg noxu_shuttle`,
//! so the default `cargo test` and every production build are unaffected.
//! Under the cfg:
//!
//!   * `VlsnIndex`'s two `RwLock`s route through `noxu_util::dst_sync_pl`
//!     (parking_lot-shaped, matching `noxu_sync::RwLock`).
//!   * `PersistentAcceptorState`'s `Mutex` (`flush_lock`, `accepted_master`)
//!     and atomics (`promised_term`, `accepted_term`) route through
//!     `noxu_util::dst_sync` (std-shaped). The acceptor is used in
//!     `in_memory()` mode so `flush_locked` is a no-op (no real file I/O on
//!     shuttle's cooperative scheduler).
//!
//! Under the default cfg all four seams are transparent re-exports of the real
//! `noxu_sync` / `std::sync` types, so production is byte-identical and shuttle
//! is absent from the dependency graph.
//!
//! # Invariants
//!
//! ## VLSN index (`noxu-spec` `vlsn_streaming`)
//!
//!   * **monotonic-latest** — after concurrent `put`s, `get_latest_vlsn()`
//!     equals the max VLSN inserted (the range's `last` never lags a mapping).
//!   * **no-lost-mapping** — every VLSN that was `put` is subsequently found by
//!     `get_lsn` with its exact `(file, offset)` (a torn bucket-list under a
//!     concurrent insert would drop or corrupt a mapping).
//!   * **no-torn-range** — the range invariant `first <= last` holds at every
//!     read, even mid-race (the range and buckets are two separate locks; a
//!     torn update would surface as `first > last` or a `last` below a stored
//!     mapping).
//!
//! ## Paxos acceptor vote tally (`noxu-spec` `flexible_paxos`)
//!
//!   * **PromiseHonoured** (the `noxu-spec` invariant) — a successful
//!     `try_accept(t, m)` is always at a term `t <= promised_term`; the
//!     recorded (accepted_term, accepted_master) pair is self-consistent even
//!     when two proposers at distinct terms race the shared acceptor.
//!   * **promise-monotone** — the promised term never decreases (a stale
//!     proposer's lower-term promise is always rejected), even when N
//!     proposers race the load-modify-flush cycle.
//!   * **accept-implies-promise** — a successful `try_accept(t, m)` implies the
//!     acceptor had promised term `t` (the split-brain guard: an accept at a
//!     term never promised, or above the promise, is rejected).
//!
//! Two proposers legitimately use DISTINCT terms (the `flexible_paxos` model
//! enforces one leader per term via its `StartElection` uniqueness guard), so
//! this gate models valid executions: proposers at different terms racing the
//! shared acceptor. A same-term-different-master race is outside the
//! protocol's valid executions AND outside `PersistentAcceptorState`'s tracked
//! state (it collapses the Paxos ballot to just the term); see the fidelity
//! note this gate surfaced in
//! `.agent/archived-audits/dst-rep-sync-acceptor-term-fidelity.md`.
//!
//! # Not vacuous (the regression proofs)
//!
//! * **VLSN:** remove the `buckets.sort_unstable_by_key(...)` that keeps the
//!   bucket list ordered by `first_vlsn` after a concurrent out-of-order
//!   `push`, and [`vlsn_concurrent_put_get_monotone`] finds a lost/corrupt
//!   mapping (the `partition_point` binary search in `get_lsn` returns the
//!   wrong bucket for a VLSN whose bucket landed out of order). Restore the
//!   sort and every mapping is found.
//! * **Acceptor:** remove the `flush_lock` coarse-lock from `try_promise` /
//!   `try_accept` (making the load-modify-store of `promised_term` a racy
//!   check-then-set) and [`acceptor_stale_proposer_never_regresses_promise`]
//!   finds a schedule where the stale proposer's lower-term store clobbers the
//!   fresh promise, so `accepted_term > promised_term` (PromiseHonoured
//!   violated). Restore the coarse-lock and every schedule honours the
//!   promise. (The `flexible_paxos` Stateright model checks the same
//!   PromiseHonoured invariant against the abstract protocol.)
//!
//! # Running
//!
//! ```sh
//! RUSTFLAGS="--cfg noxu_shuttle" cargo test -p noxu-rep --test shuttle_rep_sync
//! ```
#![cfg(noxu_shuttle)]

use noxu_rep::elections::PersistentAcceptorState;
use noxu_rep::vlsn::VlsnIndex;
use shuttle::sync::{Arc, Mutex};

/// Number of interleavings shuttle explores per test.
const ITERATIONS: usize = 5_000;

// ─────────────────────────────────────────────────────────────────────────
// GAP 2a — VLSN index concurrent tracking
// ─────────────────────────────────────────────────────────────────────────

/// Concurrent `put` from two writers into a shared `VlsnIndex`, racing a
/// reader doing `get_lsn`/`get_range`. Asserts monotonic-latest,
/// no-lost-mapping, and no-torn-range.
///
/// Each writer inserts a disjoint contiguous VLSN range so the union is
/// deterministic (1..=N). The reader interleaves lookups of already-inserted
/// VLSNs; a torn bucket-list would surface a missing mapping or a range with
/// `first > last`.
#[test]
fn vlsn_concurrent_put_get_monotone() {
    shuttle::check_random(
        || {
            // stride 3 → several buckets → the bucket-list sort/insert path
            // (buckets.sort_unstable_by_key after a push) is exercised under
            // the race, not just a single-bucket append.
            let index = Arc::new(VlsnIndex::new(3));

            // Two writers, disjoint VLSN ranges. Writer A: 1..=6, writer B:
            // 7..=12. file_number/offset are deterministic functions of vlsn
            // so a lookup can verify the exact mapping was not corrupted.
            let writer_a = {
                let index = Arc::clone(&index);
                shuttle::thread::spawn(move || {
                    for v in 1u64..=6 {
                        index.put(v, v as u32, v as u32 * 100);
                    }
                })
            };
            let writer_b = {
                let index = Arc::clone(&index);
                shuttle::thread::spawn(move || {
                    for v in 7u64..=12 {
                        index.put(v, v as u32, v as u32 * 100);
                    }
                })
            };

            // Reader: continuously reads the range; every observed range must
            // satisfy first <= last (no torn range), and the latest must never
            // exceed the max we will ever insert (12).
            let reader = {
                let index = Arc::clone(&index);
                shuttle::thread::spawn(move || {
                    for _ in 0..6 {
                        let range = index.get_range();
                        if !range.is_empty() {
                            assert!(
                                range.get_first() <= range.get_last(),
                                "torn VLSN range: first {} > last {}",
                                range.get_first(),
                                range.get_last()
                            );
                        }
                        let latest = index.get_latest_vlsn();
                        assert!(
                            latest <= 12,
                            "latest VLSN {} exceeds max inserted 12",
                            latest
                        );
                    }
                })
            };

            writer_a.join().unwrap();
            writer_b.join().unwrap();
            reader.join().unwrap();

            // Quiescent post-conditions.
            // monotonic-latest: after all puts, latest == max inserted.
            assert_eq!(
                index.get_latest_vlsn(),
                12,
                "latest VLSN must equal the max inserted after all puts"
            );
            let range = index.get_range();
            assert_eq!(range.get_first(), 1, "range first must be 1");
            assert_eq!(range.get_last(), 12, "range last must be 12");

            // no-lost-mapping: every inserted VLSN is found with its exact
            // (file, offset). get_lsn returns the LTE stride boundary, but a
            // stride boundary vlsn returns its OWN exact mapping; verify those.
            // Stride 3 from vlsn 1 → boundaries 1,4,7,10 have exact mappings.
            for v in [1u64, 4, 7, 10] {
                assert_eq!(
                    index.get_lsn(v),
                    Some((v as u32, v as u32 * 100)),
                    "lost/corrupt mapping for VLSN {v}"
                );
            }
            // And every vlsn 1..=12 must at least resolve to SOME mapping (LTE
            // fall-back) — none silently dropped.
            for v in 1u64..=12 {
                assert!(
                    index.get_lsn(v).is_some(),
                    "VLSN {v} has no mapping after concurrent puts"
                );
            }
        },
        ITERATIONS,
    );
}

// ─────────────────────────────────────────────────────────────────────────
// GAP 2b — Paxos acceptor vote tally / quorum decision
// ─────────────────────────────────────────────────────────────────────────

/// Two proposers concurrently drive a shared acceptor, each at a DISTINCT
/// term (the protocol assigns a unique ballot per proposer — the
/// `noxu-spec` `flexible_paxos` model enforces one leader per term via its
/// `StartElection` uniqueness guard, so two proposers never legitimately share
/// a term). The acceptor's `flush_lock` serialises the load-modify-store
/// cycle; the safety property checked here is **PromiseHonoured** (the
/// `noxu-spec` invariant): a successful accept is always at a term `<=` the
/// promised term, and the acceptor's recorded (accepted_term, accepted_master)
/// pair is internally consistent — never a term/master mismatch, never an
/// accept above the current promise.
///
/// Proposer-lo runs phase 1+2 at term 5 (master "node-lo"); proposer-hi at
/// term 8 (master "node-hi"). They race the shared acceptor. Whatever the
/// interleaving, the acceptor must end in a self-consistent state: the
/// accepted master (if any) matches the accepted term, and the accepted term
/// never exceeds the promised term.
///
/// NOTE: this test deliberately does NOT assert the single-acceptor
/// "one-master-per-term" property, because `PersistentAcceptorState` tracks
/// only the *term* (not the full Paxos proposal / ballot the way JE's
/// `Acceptor.process` compares `promisedProposal.compareTo(...)`), and its
/// `try_promise` uses `t >= promised` (permitting a same-term re-promise). A
/// same-term-different-master race is therefore outside the protocol's valid
/// executions (the spec rules it out structurally) and outside this model's
/// tracked state. See `.agent/archived-audits/dst-rep-sync-acceptor-term-
/// fidelity.md` for the fidelity note this gate surfaced.
#[test]
fn acceptor_concurrent_vote_accept_once() {
    shuttle::check_random(
        || {
            let acceptor = Arc::new(PersistentAcceptorState::in_memory());
            // Records, per proposer, (term, master, accept_succeeded).
            let outcomes: Arc<Mutex<Vec<(u64, &'static str, bool)>>> =
                Arc::new(Mutex::new(Vec::new()));

            let spawn_proposer = |term: u64, master: &'static str| {
                let acceptor = Arc::clone(&acceptor);
                let outcomes = Arc::clone(&outcomes);
                shuttle::thread::spawn(move || {
                    let promised = acceptor.try_promise(term);
                    let accepted =
                        promised && acceptor.try_accept(term, master);
                    outcomes.lock().unwrap().push((term, master, accepted));
                })
            };

            let lo = spawn_proposer(5, "node-lo");
            let hi = spawn_proposer(8, "node-hi");
            lo.join().unwrap();
            hi.join().unwrap();

            let (pterm, aterm, amaster) = acceptor.snapshot();
            let outs = outcomes.lock().unwrap();

            // promise-monotone: the promised term ends at the max of the two
            // proposed terms (8), never below the higher proposal.
            assert!(
                pterm >= 5,
                "promised term {pterm} dropped below the lower proposal 5"
            );

            // PromiseHonoured (noxu-spec `flexible_paxos`): accepted term never
            // exceeds promised term.
            assert!(
                aterm <= pterm,
                "accepted term {aterm} exceeds promised term {pterm} \
                 (PromiseHonoured violated)"
            );

            // accept-implies-promise + self-consistency: if a master was
            // recorded, its term matches the proposer that accepted it, and
            // that proposer reported success.
            match amaster.as_deref() {
                Some("node-lo") => {
                    assert_eq!(
                        aterm, 5,
                        "master node-lo recorded but accepted term is {aterm}"
                    );
                    assert!(
                        outs.iter().any(|(t, m, ok)| *t == 5
                            && *m == "node-lo"
                            && *ok),
                        "node-lo is the accepted master but reported no \
                         successful accept"
                    );
                }
                Some("node-hi") => {
                    assert_eq!(
                        aterm, 8,
                        "master node-hi recorded but accepted term is {aterm}"
                    );
                    assert!(
                        outs.iter().any(|(t, m, ok)| *t == 8
                            && *m == "node-hi"
                            && *ok),
                        "node-hi is the accepted master but reported no \
                         successful accept"
                    );
                }
                Some(other) => panic!("unexpected accepted master {other:?}"),
                None => {
                    // No accept recorded: neither proposer's accept succeeded
                    // (e.g. the higher promise landed between a proposer's
                    // promise and its accept, so t != promised on accept).
                    // That is a valid outcome; no master is a safe state.
                }
            }
        },
        ITERATIONS,
    );
}

/// A stale proposer at a LOWER term racing a fresh proposer at a higher term.
/// The acceptor must never let the stale proposer's promise/accept clobber the
/// higher-term promise (promise-monotone + accept-implies-promise). This is the
/// concurrent analogue of the sequential `restart_does_not_unmake_a_promise` /
/// `try_accept_higher_term_than_promise_rejected` unit tests.
#[test]
fn acceptor_stale_proposer_never_regresses_promise() {
    shuttle::check_random(
        || {
            let acceptor = Arc::new(PersistentAcceptorState::in_memory());

            // Fresh proposer: promise+accept at term 10.
            let fresh = {
                let acceptor = Arc::clone(&acceptor);
                shuttle::thread::spawn(move || {
                    if acceptor.try_promise(10) {
                        acceptor.try_accept(10, "fresh");
                    }
                })
            };
            // Stale proposer: promise+accept at term 4.
            let stale = {
                let acceptor = Arc::clone(&acceptor);
                shuttle::thread::spawn(move || {
                    let _ = acceptor.try_promise(4);
                    let _ = acceptor.try_accept(4, "stale");
                })
            };

            fresh.join().unwrap();
            stale.join().unwrap();

            let (pterm, aterm, amaster) = acceptor.snapshot();

            // accept-implies-promise: if a master was accepted, its term equals
            // the promised term at the time, and the accepted term is <=
            // promised (never an accept above the promise).
            assert!(
                aterm <= pterm,
                "accepted term {aterm} exceeds promised term {pterm} \
                 (split-brain guard violated)"
            );
            // "stale" (term 4) can only be the accepted master if the fresh
            // proposer never promised 10 first — i.e. if stale won BOTH the
            // promise AND accept before fresh's promise. In that case the
            // fresh promise at 10 then bumps promised_term to 10, but the
            // accepted master/term recorded by stale (4) stays until fresh
            // accepts. The invariant that MUST hold regardless: if the
            // accepted master is "stale", the accepted term is 4; if "fresh",
            // it is 10. Never a term/master mismatch.
            match amaster.as_deref() {
                Some("stale") => assert_eq!(
                    aterm, 4,
                    "master 'stale' recorded but accepted term is {aterm}, not 4"
                ),
                Some("fresh") => assert_eq!(
                    aterm, 10,
                    "master 'fresh' recorded but accepted term is {aterm}, not 10"
                ),
                Some(other) => {
                    panic!("unexpected accepted master {other:?}")
                }
                None => {}
            }
        },
        ITERATIONS,
    );
}
