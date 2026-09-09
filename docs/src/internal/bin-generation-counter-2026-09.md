# Lock-free BIN generation counter: measured, NOT adopted (2026-09)

**Status:** investigated on request, measured, **recommendation: do not build it
now.** The prize is real but small (2.6 % of a `db_get`); the cost is a
121-site type change across six crates. Revisit if the read path is ever
re-architected for other reasons, or if profiling shows the slot re-validation
latch as a top cost.

## What was asked

The `READ_COMMITTED` dirty-read fix (v7.6.0) re-validates a pre-fetched BIN
slot's LSN under the BIN read latch. That costs one latch acquire/release per
read. A lock-free *generation counter* — bumped on every BIN mutation, read with
a plain atomic load — would let the cursor detect "this slot moved" without
taking the latch at all. Would that be cheaper, and by how much?

## The prize, measured

Isolated microbenchmark (idle i4i.16xlarge, 20 M iterations, `black_box` on both
sides so neither loop is optimised away):

| operation | cost |
|---|---|
| uncontended `parking_lot::RwLock::read()` + read one `u64` | 17.12 ns |
| plain `AtomicU64::load(Acquire)` | 0.31 ns |
| **saving per read** | **16.8 ns** |

Put against the read costs measured in the latch-lite ceiling work:

| baseline | saving | share |
|---|---|---|
| `db_get` ~638 ns | 16.8 ns | **2.6 %** |
| `full_read` ~432 ns | 16.8 ns | **3.9 %** |

This is a consistency check on the earlier accounting, and it passes: the
dirty-read fix was measured to cost ~2.5 % of read throughput, and the mechanism
it added costs 2.6 % of a `db_get` in isolation. The two numbers agree, which
means the fix's overhead is fully explained by the latch and there is no
additional hidden cost to recover.

> An earlier local run of the same microbenchmark reported a 40 ns saving. That
> box was at load average ~15 and the number was noise; the 16.8 ns figure is
> from the idle instance. Recorded because the discrepancy is a useful reminder
> that a 2.4x error is available for free on a loaded machine.

## The cost, measured

A generation counter must be readable **without** the BIN latch, which means it
cannot live inside `TreeNode` — everything in there is behind
`NodeRwLock<TreeNode>` (`parking_lot::RwLock`) by construction. It would have to
sit *beside* the lock:

```rust
// today
pub type ChildArc = Arc<RwLock<TreeNode>>;

// required
struct NodeCell { generation: AtomicU64, node: RwLock<TreeNode> }
pub type ChildArc = Arc<NodeCell>;
```

`Arc` currently points directly at the `RwLock`, so there is no existing wrapper
to extend. Blast radius of introducing one:

| measure | count |
|---|---:|
| `ChildArc` / `Arc<RwLock<TreeNode>>` mentions | 121 |
| `Arc::new(RwLock::new(TreeNode…))` construction sites | 45 |
| crates touching `TreeNode` | 6 (`tree`, `dbi`, `evictor`, `cleaner`, `recovery`, `engine`) |

Every mutation path would additionally have to bump the counter, and *missing one
bump reintroduces the dirty read* — the exact bug this would be optimising the
fix for. That is a poor risk profile: the failure mode is silent, returns
uncommitted data, and would only show up as an intermittent test failure of the
kind that already cost this project a full investigation to identify once.

## Why not now

- **2.6 % is below the bar this project has already set.** The latch-lite descent
  work was rejected at a measured 13-15 % ceiling on the grounds that it was not
  worth the complexity. A 2.6 % win cannot clear a bar that 13-15 % failed.
- **The cost is not the 16.8 ns, it is the invariant.** 121 sites and 45
  constructors is a mechanical change; "every future BIN mutation must remember
  to bump the generation" is a permanent correctness obligation with a silent,
  data-integrity failure mode.
- **It optimises a correctness fix rather than a hot path.** The latch is only
  taken on the slot-revalidation path. Nothing indicates it is a top cost in a
  real profile; it was introduced deliberately to close a dirty read.

## What would change the answer

- A profile showing slot re-validation as a leading cost in a real workload.
- The read path being re-architected for another reason, making `NodeCell` a
  natural part of a change that is happening anyway.
- Optimistic lock coupling (OLC) being adopted for descent — the generation
  counter is the same primitive OLC needs, so the two should be built together,
  not separately. Note that OLC's own check was measured at ~0.8 ns and rejected
  because reads are not latch-bound; that finding applies here too.

## Related

- `latch-lite-descent-ceiling-2026-07.md` — the 13-15 % latch ceiling that set
  the bar, and the ~0.8 ns OLC check measurement.
- `noxu-sync-vs-parking-lot-2026-09.md` — why the engine's node latches are
  `parking_lot::RwLock`.
