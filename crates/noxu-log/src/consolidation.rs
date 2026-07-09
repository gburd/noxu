// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Consolidation-array Log Write Latch (LWL).
//!
//! This replaces the global `Mutex<LwlScratch>` that serialised **every** WAL
//! append with a lock-free **combining funnel** (Aether VLDB'10 technique 3,
//! Silo SOSP'13, WiredTiger `log_slot.c __wti_log_slot_join`).  It relieves
//! the measured #1 write bottleneck (40/46 threads parking on the LWL mutex;
//! `txn_mix` collapse) WITHOUT sharding the log.
//!
//! # Tenets preserved
//!
//!   * **SINGLE WAL** — one log file, one buffer pool.  The consolidation
//!     array changes only *how the serialisation point is acquired*, not the
//!     log layout.  On-disk format is byte-identical.
//!   * **SINGLE MONOTONIC LSN** — the leader assigns a *contiguous* LSN range
//!     to the whole batch in **arrival order** (the CAS-push determines
//!     arrival order; the leader stamps LSNs so the single monotonic LSN space
//!     is preserved: unique, strictly increasing, no gaps that break the
//!     `prev_offset` chain).
//!   * **No torn writes** — the per-entry serial work (LSN chain, `prev_offset`
//!     patch, buffer-slot reservation) is done by the leader exactly as the
//!     old mutex path did it, one entry at a time in arrival order.  The
//!     `prev_offset` link and CRC finalisation stay correct.
//!
//! # Why a combining funnel (flat combining), not a per-entry CAS ring
//!
//! WT's 128-slot ring parallelises the *buffer copy* under MVCC (each slot is
//! an independent memory region joined by CAS).  Noxu's LSN assignment is
//! inherently sequential (monotonic LSN + `prev_offset` chaining + file-flip
//! coordination — see write-gap-classified §6, "lock-free LSN assign via CAS
//! is NOT adoptable: file-flip + prev_offset coordination breaks").  So the
//! serial work *cannot* be parallelised; what CAN be removed is the **mutex
//! park/wake churn** (measured 26% futex at 512 threads, lwl-round2-design).
//!
//! Aether's consolidation array does exactly that: arriving committers combine
//! into one batch via a single CAS (no park), one becomes the **leader** and
//! performs the whole batch's serial work in arrival order, then publishes
//! each member's assigned `(lsn, prev_offset, buffer segment)`.  Non-leaders
//! spin briefly on their own result cell (a single relaxed load), never
//! parking on a contended mutex.  N committers => 1 combine + 1 batch of
//! serial work + N cheap result reads, instead of N mutex acquire/release
//! cycles each of which pays a futex handoff under contention.
//!
//! This is the WT slot-join mechanism reframed as Aether's single-log
//! consolidation — precisely the "LWL relief without sharding" the design
//! calls for.
//!
//! # The combine protocol (per-committer)
//!
//! 1. **Marshal** the entry into an owned buffer OUTSIDE the funnel (already
//!    done by `log_internal`; CRC is finalised *after* the leader hands back
//!    `prev_offset`, exactly as JE `addPostMarshallingInfo` runs after the
//!    latch).
//! 2. **Join**: publish a `Request` and CAS-push it onto the intrusive stack
//!    (`head`).  The CAS is the single lock-free join (WT `__wti_log_slot_join`).
//!    * If the stack was empty (our push saw `head == null`), WE ARE THE
//!      LEADER for this batch.
//!    * Otherwise we are a FOLLOWER: spin on our `Request::done` flag.
//! 3. **Leader**: atomically take the whole stack (`swap(head, null)`), REVERSE
//!    it to arrival order (the stack is LIFO; the earliest arrival is at the
//!    tail), then for each request in arrival order call the caller-supplied
//!    `assign` closure (LSN assign + prev_offset + buffer reserve — the same
//!    serial work the mutex path did).  Publish each result into the request's
//!    result cell and set `done`.  The leader processes its OWN request as part
//!    of the batch.
//! 4. **Follower**: once `done` is set (Acquire), read the published result.
//!
//! Late joiners that arrive after the leader has swapped the stack simply see
//! an empty `head` and become the leader of a *new* batch — never dropped,
//! never double-processed (proof obligation #2).
//!
//! The `assign` closure runs single-threaded inside the leader.  The CALLER
//! wraps `run_as_leader` in the log-write latch so that leaders of *successive*
//! batches serialise on it — a late joiner that becomes a new leader (after the
//! current leader swapped the stack) still blocks on the latch before touching
//! the shared LSN state, so LSN monotonicity holds ACROSS batches, not just
//! within one.  Crucially the latch is acquired ONCE PER BATCH (by the leader),
//! not once per committer: followers never touch it.  That is the whole win —
//! N committers => 1 latch acquire + 1 batch of serial work + N cheap result
//! reads, dissolving the per-committer futex park/wake convoy (Aether
//! consolidation).

use noxu_util::dst_sync::atomic::{AtomicBool, AtomicPtr, Ordering};

/// A committer's request slot, allocated on the committer's stack and linked
/// into the intrusive combining stack by raw pointer.
///
/// The `result` cell is written by the leader and read by the follower after
/// `done` is observed (Acquire) — the `done` Release/Acquire pair publishes
/// `result`'s bytes, so no separate synchronisation of `result` is needed.
pub struct Request<Rq, Rs> {
    /// The committer's request payload (entry size, flags, etc.).  Read by the
    /// leader while processing the batch.
    pub req: Rq,
    /// The assigned result, written by the leader.  `None` until published.
    result: std::cell::UnsafeCell<Option<Rs>>,
    /// Set (Release) by the leader once `result` is written; observed
    /// (Acquire) by the follower.
    done: AtomicBool,
    /// Intrusive next-pointer for the lock-free stack.
    next: AtomicPtr<Request<Rq, Rs>>,
}

// SAFETY: A `Request` is shared between exactly two threads — the committer
// that owns it (stack-allocated) and the batch leader.  The `done`
// Release/Acquire fence orders all accesses: the leader writes `result` and
// then Release-stores `done=true`; the committer Acquire-loads `done` before
// reading `result`.  No two threads ever touch `result` concurrently (the
// leader writes exactly once before `done`; the owner reads exactly once
// after `done`).  `next` is only touched under the atomic stack protocol.
// The `Rq`/`Rs` payloads must be `Send` for the leader to observe/produce
// them across the thread boundary.
unsafe impl<Rq: Send, Rs: Send> Sync for Request<Rq, Rs> {}

impl<Rq, Rs> Request<Rq, Rs> {
    /// Creates a fresh request slot for the given payload.
    pub fn new(req: Rq) -> Self {
        Request {
            req,
            result: std::cell::UnsafeCell::new(None),
            done: AtomicBool::new(false),
            next: AtomicPtr::new(std::ptr::null_mut()),
        }
    }
}

/// The consolidation array: a lock-free combining funnel over one intrusive
/// stack head.  One instance stands in for the whole log-write latch.
pub struct ConsolidationArray<Rq, Rs> {
    /// Head of the intrusive LIFO stack of pending requests.  `null` means the
    /// batch is open for a new leader.
    head: AtomicPtr<Request<Rq, Rs>>,
}

impl<Rq: Send, Rs: Send> Default for ConsolidationArray<Rq, Rs> {
    fn default() -> Self {
        Self::new()
    }
}

/// Outcome of joining the funnel.
pub enum Join {
    /// This committer won leadership and must drive the batch.
    Leader,
    /// This committer is a follower; wait for the leader to publish its result.
    Follower,
}

impl<Rq: Send, Rs: Send> ConsolidationArray<Rq, Rs> {
    /// Creates an empty consolidation array (no open batch).
    pub fn new() -> Self {
        ConsolidationArray { head: AtomicPtr::new(std::ptr::null_mut()) }
    }

    /// Joins the funnel by CAS-pushing `req` onto the stack.
    ///
    /// Returns [`Join::Leader`] if this push found an empty stack (this
    /// committer must drive the batch), else [`Join::Follower`].
    ///
    /// # Safety contract
    ///
    /// `req` must outlive the batch: the caller MUST NOT drop or move `req`
    /// until it has observed `req.done == true` (leader) — enforced by
    /// [`Self::run_as_leader`] / [`Self::wait_as_follower`], which both block
    /// until the request is resolved.
    pub fn join(&self, req: &Request<Rq, Rs>) -> Join {
        let node = req as *const Request<Rq, Rs> as *mut Request<Rq, Rs>;
        loop {
            let old_head = self.head.load(Ordering::Acquire);
            // Link our node to the current head (arrival LIFO).
            req.next.store(old_head, Ordering::Relaxed);
            // Single-CAS join (WT __wti_log_slot_join).
            match self.head.compare_exchange_weak(
                old_head,
                node,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    // We won the push.  If the stack was empty, WE are the
                    // leader of a fresh batch; otherwise we are a follower.
                    return if old_head.is_null() {
                        Join::Leader
                    } else {
                        Join::Follower
                    };
                }
                Err(_) => continue, // lost the race; retry with fresh head
            }
        }
    }

    /// Drives the batch as the leader.
    ///
    /// Atomically takes the whole stack, reverses it to **arrival order**, and
    /// invokes `assign(&req)` for each request in arrival order.  `assign`
    /// returns the result for that request; it runs single-threaded (the
    /// leader is the only thread processing the batch) so it has the same
    /// exclusivity the old mutex guard gave, for the whole batch.
    ///
    /// Publishes each result and sets `done` (Release).  Followers observe
    /// `done` (Acquire) and read the result.
    ///
    /// Returns the leader's OWN result (the leader's request is the one it
    /// pushed via [`Self::join`], identified by pointer).
    pub fn run_as_leader<F>(
        &self,
        my_req: &Request<Rq, Rs>,
        mut assign: F,
    ) -> Rs
    where
        F: FnMut(&Rq) -> Rs,
    {
        let my_ptr = my_req as *const Request<Rq, Rs>;
        // Atomically detach the whole batch.  Any committer that pushes after
        // this swap sees an empty head and becomes the leader of a NEW batch
        // (proof obligation #2: late joiners are never dropped).
        let taken = self.head.swap(std::ptr::null_mut(), Ordering::AcqRel);

        // The stack is LIFO (latest push at `taken`); walk it collecting raw
        // pointers, then reverse to arrival order so the leader stamps LSNs in
        // the order committers arrived at the funnel (proof obligation #1:
        // LSN order == arrival order).
        let mut chain: Vec<*const Request<Rq, Rs>> = Vec::new();
        let mut cur = taken as *const Request<Rq, Rs>;
        while !cur.is_null() {
            chain.push(cur);
            // SAFETY: `cur` points at a live `Request` still owned by a
            // committer that is blocked in `join`/`wait_as_follower` (or is
            // this leader itself) — none can drop until `done` is set, which
            // happens below.  `next` was published by that committer's
            // Relaxed store before its Release-CAS in `join`, and this
            // leader's `swap` (AcqRel) synchronises-with those CASes, so the
            // `next` chain is fully visible here.
            cur = unsafe { (*cur).next.load(Ordering::Acquire) };
        }
        chain.reverse(); // arrival order

        let mut my_result: Option<Rs> = None;
        for &node in &chain {
            // SAFETY: as above — `node` is a live, pinned `Request`.
            let req_ref: &Request<Rq, Rs> = unsafe { &*node };
            let res = assign(&req_ref.req);
            if node == my_ptr {
                // This is the leader's own request: keep the result to return
                // directly; do not signal `done` on ourselves (we are not
                // waiting on it).
                my_result = Some(res);
            } else {
                // SAFETY: exclusive writer — no follower reads `result` until
                // it observes `done == true`, which we set with Release right
                // after.  The leader is the only writer of this cell.
                unsafe {
                    *req_ref.result.get() = Some(res);
                }
                req_ref.done.store(true, Ordering::Release);
            }
        }

        my_result.expect("leader's own request must be in its own batch")
    }

    /// Waits as a follower for the leader to publish this request's result.
    ///
    /// Spins on `done` (Acquire).  The batch's serial work is bounded (one
    /// pass over the arrived requests, each a few atomics + a buffer reserve),
    /// so the spin is short; no mutex park/wake.  Under `noxu_shuttle` the
    /// scheduler preempts at the `done` load so every interleaving is explored.
    pub fn wait_as_follower(&self, req: &Request<Rq, Rs>) -> Rs {
        // Adaptive backoff (flat-combining discipline): a follower waits only
        // as long as the leader's batch pass, so a short CPU spin usually
        // suffices. But on a fully-subscribed box (N threads on N cores) a
        // *pure* spin starves the leader's core and collapses throughput, so
        // after a bounded spin we yield the core to the leader, escalating to
        // a brief sleep only on a pathologically long wait (leader preempted).
        let mut spins: u32 = 0;
        loop {
            if req.done.load(Ordering::Acquire) {
                // SAFETY: `done == true` (Acquire) synchronises-with the
                // leader's Release store after it wrote `result`; the leader
                // never writes `result` again, and we are the only reader.
                // Take the value out.
                let slot = unsafe { &mut *req.result.get() };
                return slot.take().expect("done implies result present");
            }
            #[cfg(noxu_shuttle)]
            shuttle::thread::yield_now();
            #[cfg(not(noxu_shuttle))]
            {
                if spins < 128 {
                    std::hint::spin_loop();
                } else {
                    // Yield the core to the leader; never sleep on the commit
                    // critical path (sleeping adds latency far larger than a
                    // batch's serial pass and collapses throughput).
                    std::thread::yield_now();
                }
            }
            spins = spins.saturating_add(1);
        }
    }
}

#[cfg(all(test, not(noxu_shuttle)))]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// Single-threaded: a lone committer is always its own leader and gets its
    /// result back directly.
    #[test]
    fn solo_committer_is_leader() {
        let arr: ConsolidationArray<u64, u64> = ConsolidationArray::new();
        let req = Request::new(7u64);
        match arr.join(&req) {
            Join::Leader => {}
            Join::Follower => panic!("solo committer must be leader"),
        }
        let out = arr.run_as_leader(&req, |r| *r * 10);
        assert_eq!(out, 70);
    }

    /// Concurrent committers: EVERY committer's request is processed exactly
    /// once, results are correct, and the leader's `assign` is called under a
    /// caller-held mutex so a monotonic counter never regresses (models the
    /// LSN-assign serialisation).
    #[test]
    fn concurrent_join_all_served_monotonic() {
        use std::sync::Mutex;
        const THREADS: usize = 16;
        const ROUNDS: usize = 200;

        let arr: Arc<ConsolidationArray<u64, u64>> =
            Arc::new(ConsolidationArray::new());
        // Stands in for the log-write latch the leader holds.
        let latch = Arc::new(Mutex::new(()));
        // Monotonic "LSN" counter, only advanced by a leader under the latch.
        let counter = Arc::new(AtomicU64::new(0));
        // Bit set to detect double-processing / lost requests.
        let served = Arc::new(Mutex::new(Vec::<u64>::new()));

        let handles: Vec<_> = (0..THREADS)
            .map(|t| {
                let arr = Arc::clone(&arr);
                let latch = Arc::clone(&latch);
                let counter = Arc::clone(&counter);
                let served = Arc::clone(&served);
                std::thread::spawn(move || {
                    for r in 0..ROUNDS {
                        let tag = (t * ROUNDS + r) as u64;
                        let req = Request::new(tag);
                        let assigned = match arr.join(&req) {
                            Join::Leader => {
                                let _g = latch.lock().unwrap();
                                arr.run_as_leader(&req, |rq| {
                                    // Monotonic assign under the latch: the
                                    // returned value strictly increases in the
                                    // order the leader stamps the batch.
                                    let lsn = counter
                                        .fetch_add(1, Ordering::SeqCst);
                                    served.lock().unwrap().push(*rq);
                                    lsn
                                })
                            }
                            Join::Follower => arr.wait_as_follower(&req),
                        };
                        // Sanity: an assigned LSN is within the total range.
                        assert!(
                            assigned < (THREADS * ROUNDS) as u64,
                            "lsn out of range"
                        );
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }

        // Every request processed EXACTLY once (no lost, no double).
        let mut all = served.lock().unwrap().clone();
        assert_eq!(
            all.len(),
            THREADS * ROUNDS,
            "every committer must be served exactly once"
        );
        all.sort_unstable();
        all.dedup();
        assert_eq!(
            all.len(),
            THREADS * ROUNDS,
            "no request may be processed twice"
        );
        // The counter advanced exactly once per request (monotone, no gaps).
        assert_eq!(counter.load(Ordering::SeqCst), (THREADS * ROUNDS) as u64);
    }
}
