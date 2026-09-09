# Replacing `parking_lot` with `noxu-sync`: measured, NOT adopted (2026-09)

**Status:** the swap was **fully implemented and tested** (workspace builds,
6329/6333 tests pass), then **rejected on measurement**. The custom `RwLock` is
competitive on read-only work (−1.7 %) but **2.9× slower on mixed
read/write** because of its writer-preference gate. `parking_lot` stays.

The work was not wasted: it found and fixed a severe latent throughput cliff in
`noxu_sync::RwLock` that affected existing production users of that lock.

## The question

If the custom `RwLock` is fixed and beats `parking_lot` under contention, why not
use it everywhere and drop the dependency?

## First: two premises that did not survive checking

**"It would remove an external dependency."** `parking_lot` is already an
*unconditional* production dependency of six crates, and the engine's hottest
lock — the B-tree node latch — *is* `parking_lot::RwLock`
(`noxu-tree/src/lib.rs`). Copying its source would duplicate a dependency we
already ship, not remove one.

**"`read_arc` is a structural blocker."** This was my own claim, and it was
wrong. The tree calls `read_arc()` in 39 places and `noxu_sync::RwLock` did not
provide it — but `lock_api` implements `read_arc` for any `R: RawRwLock` behind
its `arc_lock` feature, which our raw lock satisfies. The real obstacle was that
our `RwLock` was a **newtype** (`struct RwLock<T>(lock_api::RwLock<…>)`), and
`read_arc` takes `self: &Arc<Self>`, which a newtype cannot forward without
transmuting `Arc<Newtype>` → `Arc<Inner>`. Converting the newtype to a plain type
alias — exactly `parking_lot`'s shape — fixed it at zero cost, because none of
the four extra methods the newtype added (`is_locked_exclusive`,
`get_n_waiters`, `reader_count`, `raw`) are used anywhere outside `noxu-sync`.

## The swap works

Seven small edits: `arc_lock` on `lock_api`, newtype → alias, two type aliases in
`noxu-tree`, one import in `checkpointer.rs`, and four test-file imports.

- `cargo build --workspace --all-targets`: **0 errors**
- `cargo nextest run --workspace`: **6329/6333 pass, 0 failures**

So this is a decision about performance, not feasibility.

## The measurement that decides it

Idle i4i.16xlarge (64 vCPU), `noxu-xbench`, interleaved runs, 64 threads:

| workload | `parking_lot` | `noxu_sync` | delta |
|---|---:|---:|---:|
| read-only (`ycsb_c`) | 656k ops/s | 645k | **−1.7 %** |
| mixed r/w (`ycsb_a`) | 498k ops/s | 175k | **−65 % (2.9× slower)** |
| mixed r/w, gate disabled | 498k ops/s | 483k | −3 % |

The third row is the diagnosis: with the writer-preference gate removed our lock
is within 3 % of `parking_lot` on the same workload. **The primitive is
competitive; its fairness policy is what costs.**

### Why the gate is so expensive here

Every B-tree descent read-latches every node from the root down, so the root is
read-latched continuously by all threads. Unconditional writer preference means
one background writer — a split, an eviction, a checkpoint — blocks *every*
reader in the engine behind it. Spinning before closing the gate recovers the
read-only case almost entirely (see below) but cannot rescue a workload where
writers arrive constantly.

`parking_lot` avoids this with *eventual* fairness: it barges by default and only
enforces ordering for a waiter that has actually parked.

## What this bought anyway: a latent cliff, found and fixed

The swap was the vehicle that exposed it. With the gate firing the instant a
writer failed its first CAS:

| configuration | read-only 64t |
|---|---:|
| `parking_lot` | 664k |
| ours, gate disabled | 631k |
| ours, gate immediate | **78k** |

An 8.5× collapse. Separating "our lock is slow" from "our *policy* is slow"
mattered — the first framing indicts the primitive, and it should not have.

Fix (shipped, independent of the swap): spin before closing the gate, tuned by
measurement.

| spin attempts | read-only 64t |
|---:|---:|
| 0 | 78k |
| 40 | 139k |
| **400** | **649k** |
| 4000 | 657k |

400 is the knee. Starvation stays fixed there: 6,510 writes against 63 hammering
readers (versus **one** before the gate existed), worst wait 0.58 ms against
`parking_lot`'s 0.60 ms.

**This cliff was latent for existing users of `noxu_sync::RwLock`** —
`noxu-dbi`'s database catalog and `txn_manager` — entirely independent of the
tree. Any writer there stalled all readers of that structure. That fix alone
justifies the exercise.

## Why not adopt the swap

- **A 2.9× regression on mixed workloads is disqualifying.** Mixed r/w with zero
  aborts is one of this engine's measured *strengths* (it beats WiredTiger 2-5×);
  trading it for dependency aesthetics is the wrong trade.
- **Removing `parking_lot` from the graph would require re-implementing
  `parking_lot_core`**, not just the lock: a global hash table of thread wait
  queues with adaptive spinning and eventual fairness. Our futex primitives are
  ~500 lines; that is thousands, and it is where the subtle bugs live.
- **Empirical risk estimate, from this exercise:** adding *one bit* to a lock we
  already own produced **four** deadlocks (three in v7.6.1, one more here), every
  one caught by testing rather than review. Re-implementing a parking lot invites
  that failure class at much larger scale in the engine's hottest path.
- **It does not reduce the dependency count anyway** unless `parking_lot` leaves
  the graph entirely, which the point above makes impractical.

## What would change the answer

- Making the gate *eventual* rather than immediate (arm it only after a writer
  has parked and waited past a threshold, as `parking_lot` does). That is the one
  change that could plausibly close the mixed-workload gap; it is a real design
  task, not a tuning knob.
- A per-lock fairness policy, so the tree's node latches barge while the
  catalog's locks stay writer-preferring. Cheap to state, but it means two lock
  behaviours to reason about.
- A dependency-elimination mandate that outweighs a measured 2.9× regression.

## Reproducing

The swap is preserved on the `spike/noxu-sync-everywhere` branch (not merged).
`docs/src/internal/noxu-sync-vs-parking-lot-2026-09.md` holds the primitive-level
A/B; this document holds the engine-level one.
