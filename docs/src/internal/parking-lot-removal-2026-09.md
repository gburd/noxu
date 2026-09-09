# Removing `parking_lot` entirely: implemented and measured (2026-09)

**Status:** implemented, qualified, **merged**. `parking_lot`,
`parking_lot_core` and `smallvec` are gone from every shipped crate (39 → 36
transitive deps). Cost: −2 % read / −4 % mixed throughput, and a write-latency
tail that remains materially worse than `parking_lot`'s under heavy read
contention (p50 1.0 ms vs 0 µs at 63 readers). The tail is understood, its cause
is identified, and the fix is specified below rather than hand-waved.

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
not to push through. Targeted wakeup is independently correct and shipped; the
reservation design and its three traps are recorded here for whoever picks it up.

## The decision

Merged. The trade, stated plainly:

**Gained:** three fewer shipped dependencies, no third-party code on the engine's
hottest lock, and full control of the fairness policy — the eventual-fairness
behaviour cannot be expressed through `parking_lot`'s API at all.

**Paid:** −2 % read / −4 % mixed throughput, a write tail that is still much
worse under heavy read contention, and ownership of ~700 lines of lock that must
stay correct on every platform. Seven deadlocks/livelocks were introduced and
caught by tests across this work; `parking_lot` is battle-tested across the
ecosystem and we are not.

**Next step, specified:** implement writer reservation (above). That is the one
change that closes the p50 gap, and its three traps are already documented. Until
then, avoid `noxu_sync::RwLock` for a lock that is both write-latency-sensitive
and held under heavy read contention. The B-tree node latch is not such a case —
its writes are background splits and evictions, and the measured engine cost is
−2 %/−4 %.

## Related

- `parking-lot-swap-evaluation-2026-09.md` — the first attempt, rejected at 2.9×.
- `noxu-sync-vs-parking-lot-2026-09.md` — primitive-level A/B and the original
  starvation finding.
