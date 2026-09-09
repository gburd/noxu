# Removing `parking_lot` entirely: implemented and measured (2026-09)

**Status:** implemented, qualified, and **awaiting a product decision**. The
branch removes `parking_lot`, `parking_lot_core` and `smallvec` from every
shipped crate (39 → 36 transitive deps) at a cost of −3 % read / −6 % mixed
throughput and a **27× worse worst-case write tail**. Branch:
`refactor/remove-parking-lot`.

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

| readers | ours (max write wait) | `parking_lot` |
|---:|---:|---:|
| 3 | 0.62 ms | 0.03 ms |
| 15 | 1.09 ms | 0.54 ms |
| 63 | **19.94 ms** | **0.73 ms** |

27× worse at 63 readers. **This is not the threshold** — tuning it barely moves
the number (50 µs → 16 ms, 200 µs → 10 ms, 500 µs → 20 ms). The cause is our
**wake strategy**: on every transition where a writer is queued we
`futex_wake(i32::MAX)`, so 63 readers thunder-herd repeatedly, and the unlucky
writer loses several rounds.

`parking_lot` unparks *specific* threads from its queue and never wakes a herd.
Closing this gap needs targeted wakeup — which is the point where a userspace
wait queue genuinely starts to earn its complexity. Note the irony: the
side-table I wrongly cited as a prerequisite is, in fact, what would fix the one
metric still behind.

## The decision

Removal is **viable, not free**:

**For:** three fewer dependencies, no third-party code on the engine's hottest
lock, full control of the fairness policy, and a smaller audit surface.

**Against:** −3 % / −6 % throughput and a 27× worse write tail, in exchange for
owning ~700 lines of lock implementation that must stay correct on every
platform. Four deadlocks were introduced and caught by tests during this work;
`parking_lot` is battle-tested across the ecosystem.

**Recommendation:** do not merge for dependency count alone. Merge if either (a)
controlling fairness policy is itself a goal — the eventual-fairness work is
already done and cannot be expressed through `parking_lot`'s API — or (b) the
tail-latency gap is closed first with targeted wakeup, at which point the trade
becomes ~−4 % throughput for three fewer dependencies and no tail regression.

## Related

- `parking-lot-swap-evaluation-2026-09.md` — the first attempt, rejected at 2.9×.
- `noxu-sync-vs-parking-lot-2026-09.md` — primitive-level A/B and the original
  starvation finding.
