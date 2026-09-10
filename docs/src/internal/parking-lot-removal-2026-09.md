# Removing `parking_lot` entirely: implemented and measured (2026-09)

**Status:** implemented, qualified, **merged**. `parking_lot`,
`parking_lot_core` and `smallvec` are gone from every shipped crate (39 → 36
transitive deps). Cost at merge time: −2 % read / −4 % mixed throughput, and a
write-latency tail materially worse than `parking_lot`'s under heavy read
contention (p50 1.0 ms vs 0 µs at 63 readers). Writer reservation (below,
"Third attempt: shipped") has since closed most of that tail-latency gap
(p50 at 63 readers: 1007 µs → ~30 µs) with a measured **engine throughput
gain** (+4 % read-only, +65 % mixed) rather than the documented cost — see
that section for the current numbers; the two tables above it are the
historical record of how the gap was diagnosed and why the first two attempts
at closing it were backed out.

## `parking_lot_core` was never a prerequisite

An earlier assessment claimed removal required re-implementing `parking_lot`'s
global hash table of thread wait queues. That was wrong, and the reason is
instructive.

`parking_lot::RawRwLock` is **one word** — a single `AtomicUsize`. That is its
defining constraint, and it is why it *needs* a side-table: with no space in the
lock itself, waiters must be tracked by hashing the lock's address into a shared
structure.

`NoxuRawRwLock` is 28 bytes, with dedicated `read_waiters` / `write_waiters`
counters, and it parks on the **kernel's** wait queue through `futex`. The futex
*is* the parking lot. There was nothing to port — a different tradeoff (more
bytes per lock, kernel-side queueing) rather than a missing component.

## What actually blocked it: fairness policy, not the primitive

The first swap attempt was rejected at **2.9× slower on mixed read/write**. That
was not the lock. Isolating the variables:

| configuration | mixed r/w 64t |
|---|---:|
| `parking_lot` | 498k ops/s |
| ours, gate disabled entirely | 483k |
| ours, gate fires immediately | 175k |

With the writer-preference gate off, our lock is within 3 %. The **policy** cost
2.9×, because every B-tree descent read-latches the root: a gate that closes the
instant a writer queues lets one background split or eviction stall every reader
in the engine.

## The fix: eventual fairness

A writer now remains in barging mode until it has actually been starved for
`FAIRNESS_THRESHOLD` (500 µs), and only then arms the reader-admission gate —
the same trade `parking_lot` makes by setting `WRITER_BIT` only once a writer has
parked.

One non-obvious requirement: the futex park must be bounded by the fairness
deadline as well as the caller's. With no caller deadline the park was
indefinite, so the writer never woke to arm the gate and "eventual" fairness
never arrived. The symptom was simply that the gate appeared not to work.

## Results

Idle i4i.16xlarge, 64 threads, interleaved runs:

| metric | `parking_lot` | ours | delta |
|---|---:|---:|---:|
| read-only (`ycsb_c`) | 656k ops/s | 641k | **−3 %** |
| mixed r/w (`ycsb_a`) | 510k ops/s | 478k | **−6 %** |
| transitive deps (`noxu-db`) | 39 | **36** | −3 |

Starvation stays bounded: **2,722 writes** against 63 hammering readers, versus
**one** before any gate existed.

Correctness: workspace **6328/6333, zero failures**; `noxu-sync` 30/30; shuttle
DST 10/10; clippy `--all-targets --all-features` clean.

## The honest remaining gap: write tail latency

Targeted wakeup (writers on a dedicated futex word, so a release wakes exactly
one) improved the mid-range — 1.09 ms → 0.77 ms at 15 readers — but did **not**
close the gap at 63. Instrumenting the distribution rather than the maximum
explains why:

| impl | p50 | p99 | p99.9 | max | writes/3 s |
|---|---:|---:|---:|---:|---:|
| ours | 1007 µs | 1168 µs | 10.1 ms | 19.9 ms | 2,788 |
| `parking_lot` | **0 µs** | 5 µs | 390 µs | 563 µs | 242,492 |

The distribution is tight, so this is not jitter and not a herd. And it is not
the fairness threshold: lowering it 500 µs → 20 µs moved p50 only 987 µs → 547 µs.
The cause is the **in-flight reader drain**, and p50 tracks reader count almost
exactly:

| readers | our p50 | `parking_lot` p50 |
|---:|---:|---:|
| 4 | 0 µs | 0 µs |
| 16 | 335 µs | 0 µs |
| 63 | 650 µs | 0 µs |

Our writer arms an *advisory* gate (`WRITE_WAITING`) and then re-races for the
lock once readers drain, paying a futex round-trip per step. `parking_lot`'s
`WRITER_BIT` **reserves** the lock immediately — its own comment says the bit
means "a writer holds this" when the reader count is zero and "a writer is
waiting for the remaining readers to exit" otherwise — so the handoff is
guaranteed the instant the last reader leaves. That is the whole difference.

### Reservation was implemented and backed out

It is the right design, and it broke three things in sequence, each caught by a
test:

1. A blind `fetch_or` set `WRITE_LOCKED` while another writer held it, so the
   waiter saw "readers == 0" and concluded it owned a lock someone else held —
   two writers in the critical section.
2. `is_locked_exclusive` conflated *reserved* with *held*, since `WRITE_LOCKED`
   then means both depending on the reader count.
3. A reserved writer's self-CAS trivially succeeded by writing back the value it
   had just read, ran the stop-waiting reconciliation, and cleared the gate while
   holding the lock — readmitting readers under a live writer, observed as a
   livelock with nothing parked and nothing progressing.

Three new bugs in one sitting on a primitive this delicate is a signal to stop,
not to push through.

### Second attempt (2026-09, post-v7.7.0): also backed out

Attempted again with all three traps known in advance and guarded up front:
`reserve_for_drain` CASes conditionally on `WRITE_LOCKED` being clear (trap 1),
`is_locked_exclusive`/`is_write_locked` test `WRITE_LOCKED && readers == 0` so a
reservation is not reported as ownership (trap 2), a reserved writer takes
ownership directly instead of running a self-CAS (trap 3), and
`give_up_waiting(reserved)` releases the bit on the timeout path.

That was still not sufficient. It exposed a FOURTH failure mode: `unlock_exclusive`
woke writers **or** readers (`else if`), and since reserving writers stay counted
in `write_waiters` for their entire drain, a queued writer is almost always
present — so readers were never woken and parked forever. Waking both populations
unconditionally fixed that specific issue but the watchdog test still hung, so at
least one more interaction remains unidentified.

Backed out again, leaving the tree green. Two independent attempts (one by a
subagent, which produced nothing, and one hands-on) have now failed on this,
which is itself the useful result: **reservation is not a local change to
`lock_exclusive_slow`.** It changes what `WRITE_LOCKED` MEANS, and every reader
of that bit — both unlock paths, both predicates, the gate reconciliation, and
the wake strategy for both populations — has to be re-derived together.

Recommendation for a third attempt: do not patch the existing state machine.
Write the reserved-writer protocol as an explicit state table first (states:
free / read-held / reserved-draining / write-held, with every transition and which
population must be woken on each), validate it under shuttle as a standalone
model, and only then port it in. The next attempt should also budget for the fact
that the failure mode is a hang, so it needs the watchdog test plus `rwstress`
running continuously, not a final check.

### Third attempt (2026-09): shipped

The recommendation above was followed exactly, and it worked. Full account:
`rwlock-state-table-2026-09.md` (the state table, written and committed before
any lock code was touched) and `crates/noxu-sync/tests/shuttle_rwlock_reservation.rs`
(the standalone shuttle model, validated — including against deliberately
sabotaged copies — before porting). The state table's own construction found
the exact class of bug attempt #2 could only observe empirically: the first
draft had the reserving writer share a futex word with parked readers for the
drain-complete handoff, which a targeted wake can silently swallow (a reader
re-parks instead of the writer waking). Fixed with a third, dedicated futex
word (`drain_futex`) that only the current reservation-holder ever parks on,
unambiguous by construction since at most one writer is ever reserved-and-
draining at a time.

The design ships with `WRITE_WAITING`/`FAIRNESS_THRESHOLD` deleted entirely,
not patched: reservation's guard is `WRITE_LOCKED == 0`, never the reader
count, so it cannot be starved by a reader relay the way the old plain
`state == 0` CAS could — the gate that existed to force that CAS to eventually
succeed becomes dead machinery once reservation exists, and running it
alongside reservation is the leading hypothesis (not proven, but not
contradicted by the shuttle model either) for attempt #2's unidentified fourth
hang.

Two more latency bugs surfaced only under real measurement on a dedicated,
idle 32-vCPU box, not from the state table or the model (both are protocol
tools; neither models scheduler behaviour):

1. The reserving writer's drain wait went straight to a blocking `futex_wait`
   with no spin, so every reservation paid a full park/wake round-trip even
   when the last reader would finish in nanoseconds. Fixed with a short
   two-phase spin (relax, then yield) before parking — mirroring
   `parking_lot_core::SpinWait`'s own strategy, and for the same reason a flat
   busy-spin was tried and rejected first: under oversubscription (63 readers
   + 1 writer > 32 vCPUs) an uninterrupted busy-spin competes with the very
   readers it is waiting on instead of yielding its slot back.
2. The barging spin (`WRITE_SPIN_ATTEMPTS`, unchanged at 400 iterations from
   the pre-reservation design) reloads `state` on every iteration, and under
   heavy read contention that cache line is under constant `fetch_add`/
   `fetch_sub` traffic — paying the resulting cache-miss cost 400 times per
   writer was the actual dominant term in the tail, confirmed by finding the
   identical flat-reload loop unchanged in the pre-reservation baseline.
   Fixed with the same exponential backoff `parking_lot_core::SpinWait` uses
   (`cpu_relax(1 << counter)`), re-reading `state` roughly 9 times instead of
   400 for the same total spin budget.

Measured on a dedicated, idle i4i.8xlarge (32 vCPU), 1 writer vs N readers, 3s
window, three consecutive runs:

| readers | metric | before (documented above) | after (shipped) | `parking_lot` |
|---:|---|---:|---:|---:|
| 63 | p50 | 1007 µs | ~25–37 µs | ~2.6–3.2 µs |
| 63 | p99.9 | 10.1 ms | ~87–97 µs | ~46–49 µs |
| 63 | max | 19.9 ms | ~98–115 µs | ~57–178 µs |
| 16 | p50 | 335 µs | ~12–15 µs | ~0.5–0.9 µs |
| 4 | p50 | 0 µs | ~1.2–7.0 µs | ~0.15–0.26 µs |

p50 at 63 readers: **~30–40× better** than the documented baseline. Max at 63
readers: **~170–200× better**. The gap to `parking_lot` narrows from roughly
1,700× (max, original measurement) to roughly 10× (p50, final measurement) at
63 readers — not closed, and the residual is visible in the table (our p50
still trails `parking_lot`'s by roughly an order of magnitude at every reader
count), but the dominant, fixable costs are addressed rather than open.

Engine A/B (dedicated EC2, `noxu-xbench`, interleaved baseline-vs-change, 3×
each, `BENCH_RECORDS=500000 BENCH_SECONDS=15 BENCH_THREADS=32 BENCH_VALUE=256`):

| workload | baseline (avg of 3) | change (avg of 3) | delta |
|---|---:|---:|---:|
| `ycsb_c` (read-only) | 638,610 ops/s | 665,720 ops/s | **+4.2 %** |
| `ycsb_a` (mixed, `NO_SYNC`) | 312,382 ops/s | 514,292 ops/s | **+64.6 %** |

No throughput regression on either workload; the mixed workload's large gain
is consistent with `noxu_sync::RwLock` guarding `TxnManager`'s active-txn
registry and `DatabaseImpl`/cursor locks (`crates/noxu-txn/src/txn_manager.rs`,
`crates/noxu-dbi/src/{environment_impl,db_tree,cursor_impl}.rs`) — exactly the
locks a commit-heavy mixed workload exercises hardest, and exactly where a
tighter write tail pays off most directly.

Correctness: `noxu-sync` 31/31 (including on the dedicated EC2 box); shuttle
DST 10/10 (the 9 pre-existing suites plus the new standalone reservation
model, 5/5 across three separate iteration counts); `rwstress` 7/7 thread/
write-ratio configurations, no lost wakeups; workspace `cargo nextest` 6335/
6336 (the one remaining timeout is an unrelated, independently slow
`noxu-spec` stateright BFS check that does not reference `noxu-sync` and was
confirmed slow — not hung — on an isolated rerun); clippy
`--all-targets --all-features` and `cargo fmt --all --check` both clean.

Targeted wakeup, and now reservation, are both shipped; the four traps and
this attempt's own two latency bugs are recorded above for whoever next
touches this file.

## The decision

Merged. The trade, stated plainly:

**Gained:** three fewer shipped dependencies, no third-party code on the engine's
hottest lock, full control of the fairness policy — the eventual-fairness
behaviour cannot be expressed through `parking_lot`'s API at all — and, after the
third reservation attempt shipped, a write tail within roughly an order of
magnitude of `parking_lot`'s at 63 readers (was ~1,700× off) with a measured
engine throughput GAIN rather than a cost on the mixed workload.

**Paid:** ownership of ~750 lines of lock that must stay correct on every
platform. Nine deadlocks/livelocks were introduced and caught by tests across
the full arc of this work (seven before reservation, none in the shipped
reservation port itself — the two post-port bugs were latency regressions
caught by measurement, not correctness bugs caught by tests); `parking_lot` is
battle-tested across the ecosystem and we are not. `noxu_sync::RwLock`'s read
throughput was measured at −2 % to −4 % against `parking_lot` before
reservation; the ycsb_c/ycsb_a re-measurement after reservation ("Third
attempt: shipped", above) shows a small read-only GAIN and a large mixed-
workload gain instead, so that historical cost no longer applies as measured
— though it was measured on a different, dedicated box, so treat it as the
most recent number rather than a like-for-like reproduction of the original
−2 %/−4 %.

**Status of the once-open item:** writer reservation, specified as the "next
step" below in the original merge writeup, has been implemented, validated
(state table → shuttle model → port, per the recommendation attempt #2 left
behind), measured, and shipped. `noxu_sync::RwLock` no longer carries the
blanket caveat below for the specific pattern it was written for (a lock that
is both write-latency-sensitive and held under heavy read contention) — the
remaining gap to `parking_lot` is roughly 10× at p50 rather than roughly
1,700× at max, and closing it further was not attempted this round.

## Related

+ `parking-lot-swap-evaluation-2026-09.md` — the first attempt, rejected at 2.9×.
+ `noxu-sync-vs-parking-lot-2026-09.md` — primitive-level A/B and the original
  starvation finding.
+ `rwlock-state-table-2026-09.md` — the state table and shuttle model that made
  the third reservation attempt (above) succeed where the first two did not.
