# Wave FC — P-2: recovery BIN-restore investigation

**Branch**: `fix/fc-p2-recovery-binrestore`  
**Target**: v3.1.0 (deferred — see outcome below)  
**Outcome**: **REVERTED AND DEFERRED** — escape hatch applied

## Summary

This wave investigated implementing P-2 from the Wave ZC design note: restoring
BINs directly from the checkpoint's dirty-IN map to speed up recovery, rather
than replaying every committed LN into an empty tree.  After a complete
analysis of the recovery system and checkpointing model, a fundamental
correctness blocker was identified that prevents P-2 from being implemented
safely without significant additional infrastructure.  No code was shipped.
The design note in `wave-zc-crash-perf.md` is updated with the detailed
findings.

## W11 baseline numbers (before any P-2 work)

Measured with `timeout 1200 cargo run --bin noxu-bench -- --scales 1000,10000,100000`:

| Scale (N) | w11\_recovery (ms) | Notes |
|---|---|---|
| 1 K | ~1 | trivial |
| 10 K | ~12 | dominated by full log scan from LSN 0 |
| 100 K | ~95 | ~2.9× slower than JE at this scale |

These are approximate, from the design note.  Exact pre-P-2 figures are
captured in `wave-zc-crash-perf.md`.

## Investigation findings

### Current recovery model

Noxu's recovery always scans the log from `Lsn::new(0, 0)` (the beginning),
not from the last checkpoint start.  This is intentional: the
`do_checkpoint()` writes `first_active_lsn = Lsn::new(0, 0)` in the
`CkptEnd` record, telling recovery to replay everything from scratch.

The reason for this conservative choice: `flush_dirty_bins_internal()` only
flushes **dirty** BINs (those modified since the last checkpoint).  BINs that
had no writes since the previous checkpoint are **not** re-logged.  Their
keys exist in an earlier checkpoint's log entries, at LSNs before the current
`checkpoint_start_lsn`.

### The stable-BIN correctness blocker

If recovery started scanning from `checkpoint_start_lsn` instead of from
`Lsn::new(0, 0)`, stable BINs' keys would be irrecoverable:

1. Keys committed before the current `checkpoint_start_lsn` are NOT in
   `redo_entries` (scan starts after them).
2. Stable BINs (not dirty at current checkpoint time) are NOT in `dirty_ins`
   (only current-checkpoint BINs are logged).
3. Result: those keys are silently lost from the recovered tree.

**Example:**

```
LSN  10–90: write keys A, B, C (committed)
LSN 100:    CkptStart₁
LSN 102:    flush BIN(A,B,C) to log
LSN 105:    CkptEnd₁  (checkpoint_start_lsn=100, first_active_lsn=0)

# No writes to A, B, C after here.

LSN 200:    CkptStart₂
LSN 202:    CkptEnd₂  (no dirty BINs for A,B,C — nothing logged)
            (if first_active_lsn changed to CkptStart₂=200)
LSN 250:    CRASH

Recovery with reduced scan (scan from LSN 200):
  dirty_ins:    empty for A,B,C (not logged in [200,250])
  redo_entries: empty for A,B,C (committed at LSN 10–90, before scan start)
  Result:       A, B, C LOST
```

### Why the conservative `first_active_lsn = 0` is correct

With `first_active_lsn = Lsn::new(0, 0)`, the scan covers the entire log
from the very beginning.  ALL committed LNs — pre-checkpoint or
post-checkpoint — are replayed.  The dirty_ins BINs are collected by the
analysis pass and stats are tracked, but the actual LN-redo pass populates
the tree independently and completely.

### Why a "conservative P-2" provides negligible speedup

One might ask: can we pre-populate the tree from dirty_ins BINs AND then
replay all committed LNs (scan from LSN 0)?  Yes, this is correct, but it
provides essentially no speedup:

- The analysis pass still scans the entire log (no change).
- `redo_entries` still contains all N committed LNs (no change).
- The LN-redo loop still iterates all N entries.  An LSN-aware skip at the
  BIN level avoids the final Vec mutation but does not avoid the O(log N)
  tree traversal per entry.
- Net wall-clock saving: ~10–20 % at best (fewer Vec writes, warmer cache).
  This does not reach the 1.5× target and is not worth the complexity.

The entire speedup benefit of P-2 depends on reducing the number of LN-redo
iterations from N to K ≪ N (post-checkpoint LNs only), which requires the
reduced scan range, which requires stable-BIN tracking.

### What JE does differently

JE's checkpoint writes a `DbTree` root entry that acts as a B-tree index of
all BIN versions (stable and dirty alike).  Recovery uses this root to fetch
the current-version BIN for every node, regardless of when it was last
flushed.  Noxu has no equivalent of the `DbTree` / `_jeNameTree` B-tree.
Without it, there is no way to find the current-version BIN for a stable node
at recovery time.

### Other gaps observed

1. **`dirty_in_map` vs `dirty_ins`**: Two separate structures exist —
   `RecoveryManager.dirty_in_map` (metadata only, used for stats/ordering)
   and `AnalysisResult.dirty_ins` (full byte payloads).  Both are populated
   but `dirty_ins` bytes are never used for tree construction.  The `InRecord`
   fields `node_data` and `prev_full_lsn` (in `BinDeltaLogEntry`) are not
   propagated to `InRecord` from the file-backed scanner's BINDelta parsing.

2. **BINDelta chain reconstruction**: `BinDeltaLogEntry` carries
   `prev_full_lsn` (LSN of the last full BIN write) and `prev_delta_lsn`
   (LSN of the previous delta in the chain).  To reconstruct a delta BIN,
   recovery would need to follow this chain: read full BIN at
   `prev_full_lsn`, then apply each delta in LSN order up to the current one.
   The current analysis pass only keeps the LATEST entry per node (not the
   full chain), so delta reconstruction is impossible without reading back
   from the log scanner.

3. **LSN-aware `redo_insert`**: The recovery comments claim "`redo_ln` is
   idempotent (it skips if the tree already holds a newer LSN for the key)"
   but the code does NOT implement this check.  `redo_insert_recursive`
   always overwrites the slot regardless of LSN.  This is harmless in the
   current all-LN-replay path (tree starts empty) but would be a correctness
   issue if BIN pre-population were added.

## What is needed for a correct P-2

For P-2 to be both correct and fast, the following infrastructure must be in
place:

### Prerequisite 1 — DbTree (BIN-version index)

Add a mapping tree that records, for each `(db_id, node_id)`, the LSN of the
current canonical BIN version.  Written at `CkptEnd` time.  At recovery,
this index lets recovery fetch every BIN (stable or dirty) from its most
recent log entry, regardless of when it was last checkpointed.

This is a significant new subsystem (~500–800 lines across `noxu-log`,
`noxu-recovery`, `noxu-dbi`).

### Prerequisite 2 — Correct `first_active_lsn` in `CkptEnd`

Once the DbTree is in place and recovery can reconstruct ALL BINs (stable +
dirty), the checkpoint can safely write `first_active_lsn = CkptStart` (the
actual earliest open transaction LSN, not `Lsn::new(0, 0)`).  Recovery then
scans only post-checkpoint LNs, providing the 3–5× speedup.

### Prerequisite 3 — LSN-aware `redo_insert`

When BINs are pre-loaded before LN redo, `redo_insert` must check the
existing slot LSN and skip the write if the BIN already has the key at the
same or newer LSN (overlap case: LNs committed between `CkptStart` and the
BIN's actual lock time appear in both the BIN and `redo_entries`).

### Prerequisite 4 — BINDelta chain tracking in analysis

`AnalysisResult.dirty_ins` must accumulate the FULL chain per node (last full
BIN + subsequent deltas in LSN order) instead of deduplicating to the latest
entry.  `InRecord` must carry `prev_full_lsn` from `BinDeltaLogEntry`.

### Prerequisite 5 — Tree bulk-load API

A `Tree::build_from_checkpoint_bins(bins: Vec<BinStub>)` API that bulk-loads
pre-materialized BINs into the tree in O(N) time (sorted insertion with
correct IN node construction), bypassing the O(N log N) individual-key
`redo_insert` path.

## Acceptance criteria for a future P-2 wave

When all prerequisites are in place:

1. All existing recovery/crash/power-loss tests pass byte-for-byte.
2. A correctness-equality test recovers the same workload both ways (BIN-restore
   path vs LN-replay path) and asserts identical tree state.
3. W11 benchmark at 1K/10K/100K shows ≥ 1.5× speedup over the current path.
4. `cargo fmt`, `clippy -D warnings`, `doc -D warnings`, `cargo test --workspace`,
   `make docs-check` all pass.

## Status

**Wave FC** investigated and documented the stable-BIN blocker.
**Wave GB** implemented the DbTree foundation (BIN-version index at CkptEnd),
LSN-aware redo_insert, and the equality test harness.  The scan-reduction
remains deferred pending open-txn tracking in the checkpointer.
See `docs/src/internal/wave-gb-dbtree-recovery.md` for the Wave GB STEP-0
findings and the precise description of the remaining gap.
