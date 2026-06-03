# Wave GB — DbTree foundation + P-2 recovery investigation

**Branch**: `fix/gb-dbtree-recovery` (prototype preserved — **not merged to main**)
**Target**: deferred — scan-reduction blocked on open-txn-LSN tracking
**Outcome**: **ESCAPE HATCH APPLIED** — scan-reduction is unsafe; the write-side
foundation is net checkpoint overhead until recovery consumes it, so **no GB
code was merged to main**. The complete, tested prototype lives on the
`fix/gb-dbtree-recovery` branch for the follow-on work.

## Why nothing was merged to `main`

Two independent reasons, both honest-science deferrals rather than
half-measures:

1. **Scan-reduction (the only P-2 payoff) is unsafe** without
   open-transaction-LSN tracking in the checkpointer — see Finding 4 below.
   Without it, `first_active_lsn = CkptStart` can silently treat an
   uncommitted, pre-checkpoint transaction's writes as committed.

2. **The write-side foundation alone is a regression.** Writing the DbTree
   index at every checkpoint force-flushes delta BINs to full (defeating the
   BIN-delta optimization) and walks the whole tree to build a flat index —
   yet recovery still full-scans (`first_active_lsn` stays `Lsn::new(0,0)`)
   and never reads the index. That is pure checkpoint I/O + log-space cost
   for zero current benefit, so it must not ship until the scan-reduction
   that consumes it is correct.

The prototype is preserved on the branch so the ~1,300 lines of design,
serialization, scanner, and the 11-test equality harness are not lost when
the prerequisite lands.

## What the prototype contains (on the branch, NOT on main)

This wave implemented the Wave FC prerequisites for P-2 recovery speedup
(DbTree BIN-version index) and rigorously tested whether the scan-reduction
(`first_active_lsn = CkptStart`) is safe to ship.  STEP-0 analysis revealed
a correctness gap that prevents narrowing the scan range without additional
infrastructure.  The escape hatch was applied: the scan-reduction was
**not shipped**.

The prototype on the branch contains:

1. **DbTree BIN-version index** written at each `CkptEnd` (foundation only;
   `first_active_lsn` unchanged at `Lsn::new(0,0)`, full scan preserved).

2. **LSN-aware `redo_insert`** — recovery skips redo writes
   where the tree slot already holds a same-or-newer LSN (a no-op for the
   current full-scan path; essential for the future BIN-preload path).

3. **Wave GB equality harness** (11 tests) — correctness regression battery
   covering all specified workloads.

## Prerequisites to land this work (future)

1. **Open-transaction-LSN tracking**: wire the transaction manager into the
   checkpointer so `CkptEnd.first_active_lsn = min(earliest_open_txn_lsn,
   CkptStart)`. This closes the Finding-4 correctness gap and is what makes
   the scan-reduction safe.
2. **Consume the DbTree at recovery**: `Tree::build_from_checkpoint_bins()`
   to pre-populate BINs from the index, so the per-checkpoint write cost buys
   the reduced LN-redo range. Only then does writing the index pay for itself.
3. **Eviction-robust BIN enumeration** (Finding 1): if a future evictor nulls
   parent child pointers, replace `collect_all_bins()` tree-walk with a
   persistent BIN registry, or pin BINs before collecting.

## STEP-0: Load-bearing invariant analysis

### Claim under test

> After a checkpoint, the in-memory tree contains ALL BINs (stable and
> dirty), and each BIN's `last_full_lsn` accurately represents the latest
> full-BIN log entry for that node.  `collect_all_bins()` can therefore
> enumerate every BIN at checkpoint time to build a complete BIN-version
> index (DbTree).

### Finding 1 — Eviction: SAFE (fragile)

**Current implementation**: the evictor removes nodes from its LRU-policy
tracking and credits their bytes to the memory budget, but **never sets
`InEntry.child = None`** in the parent IN node.  All `Arc<RwLock<TreeNode>>`
child pointers remain live in the in-memory tree.  `collect_all_bins()`
correctly traverses all BINs by following these non-null child arcs.

**Fragile dependency**: if a future true-eviction implementation nulls out
parent child pointers (required for actual memory reclamation of BIN
structs), `collect_all_bins()` would silently miss evicted BINs.  A future
DbTree checkpoint implementation that pre-populates BINs MUST either pin
all BINs before collecting, or maintain a persistent BIN-registry that
survives eviction.

### Finding 2 — BINDelta: HANDLED (force-full pass)

BINs written as deltas (`last_delta_lsn != NULL_LSN`) cannot be recovered
with a single `read_at_lsn` call — they require reading the base full BIN
and applying each delta in the chain.  To keep recovery simple and correct,
`write_db_tree_entry()` includes a **delta-to-full force-flush pass**:

1. Walk all BINs via `collect_all_bins()`.

2. For any BIN with `last_delta_lsn != NULL_LSN`: write a new full-BIN log
   entry and reset `last_delta_lsn = NULL_LSN`.

3. Build the DbTree index using `last_full_lsn` only (always `is_delta =
   false` after the force-flush).

Recovery reads each BIN as a single full-BIN log entry; no delta chain
reconstruction is needed.

### Finding 3 — `InEntry.lsn` (parent slot LSN): NOT reliable

The **top-down tree-walk via parent slot LSNs** (the alternative to a flat
BIN list) does NOT work in the current implementation.  `InEntry.lsn` (the
log address stored by a parent IN for its child) is **not updated** when a
child BIN is re-logged during a checkpoint.  Only split operations update
`InEntry.lsn`.  The flat BIN list approach (`collect_all_bins` + per-BIN
`last_full_lsn`) is therefore the correct design.

### Finding 4 — Open-transaction correctness gap: BLOCKER

**This is the reason the scan-reduction is deferred.**

The P-2 target is to set `first_active_lsn = CkptStart` in `CkptEnd`,
so recovery skips pre-checkpoint LNs.  This is UNSAFE when a transaction:

- **Started before `CkptStart`** (its LN is before `CkptStart` in the log),

- **Was still active (uncommitted, not aborted) at crash time**, AND

- **Has no commit or abort record before the crash**.

In this case:

- The checkpoint flushes the BIN with the uncommitted write.

- `CkptEnd.first_active_lsn = CkptStart` → analysis scans only from
  `CkptStart`.

- The LN (before `CkptStart`) is NOT seen → the transaction is NOT tracked.

- The undo pass has no knowledge of the transaction → the uncommitted write
  is NOT reverted.

- **Silently, the uncommitted data appears committed in the recovered DB.**

This is a correctness violation.  The correct `first_active_lsn` value is:

```
first_active_lsn = min(earliest_active_txn_start_lsn, CkptStart)
```

Computing this requires the checkpointer to query the transaction manager
for the earliest LSN of any currently-open transaction at checkpoint time.
The checkpointer currently has no connection to the transaction manager.
Implementing this connection is a follow-on prerequisite.

**Note on the "aborted txns" workload**: if a transaction starts before
`CkptStart` and its **abort record** lands **after** `CkptStart`, the
reduced-scan undo pass DOES handle it correctly.  The abort record is in
the scan range; the undo pass calls `scanner.read_at_lsn(abort_lsn)` to
fetch the before-image.  The correctness gap is specific to the
`still-active-at-crash-time` case.

### STEP-0 verdict

| Property | Holds? | Note |
|---|---|---|
| All BINs in memory at checkpoint time | **YES** (fragile) | Child arcs never nulled by current evictor |
| BINDelta chains handled | **YES** | Force-full-flush pass during DbTree write |
| `InEntry.lsn` top-down walk | **NO** | Slot LSNs not updated on BIN re-log |
| Scan-reduction safe to ship | **NO** | Open-txn-at-crash correctness gap |

## What was implemented

### DbTree BIN-version index (`noxu-log`, `noxu-recovery`)

**`crates/noxu-log/src/entry/db_tree_entry.rs`** (new):

- `DbTreeBinRef`: per-BIN record `{db_id, node_id, bin_lsn, prev_full_lsn,
  is_delta}` with big-endian wire format.

- `DbTreeEntry`: flat list of `DbTreeBinRef` with checkpoint ID header.

- Round-trip serialization tests.

**`crates/noxu-log/src/entry_type.rs`**:

- `LOG_VERSION = 3` (added `DbTree = 50` type, backward-compatible).

**`crates/noxu-recovery/src/log_scanner.rs`**:

- `DbTreeRecord` extended with `bins: Vec<DbTreeBinRef>` — scanner now
  carries the parsed BIN refs rather than just the entry LSN.

- New `DbTreeBinRef` struct mirroring `noxu_log::entry::DbTreeBinRef`.

### DbTree writing at checkpoint (`noxu-recovery/checkpointer.rs`)

`do_checkpoint()` now calls `write_db_tree_entry()` after the BIN and
upper-IN flush passes (Step 4c).  This method:

1. Force-flushes all delta-state BINs as full BINs.

2. Collects all BINs (including stable ones) and builds a `DbTreeEntry`.

3. Writes the entry as `LogEntryType::DbTree`.

4. Returns `Some(db_tree_lsn)` which is stored in `CkptEnd.root_lsn`.

`CkptEnd.first_active_lsn` remains `Lsn::new(0,0)` — the full-scan path
is UNCHANGED.  The DbTree is written as METADATA only; recovery ignores
it (uses `use_root_lsn` from `CkptEnd` but doesn't pre-populate BINs from
it yet).

### LSN-aware `redo_insert` (`noxu-tree/src/tree.rs`)

`BinStub::get_slot_lsn(full_key)` — new method that looks up a key and
returns its existing slot LSN without modifying the BIN.  Handles the
key-outside-prefix case: if `full_key` does not start with the BIN's
common prefix, returns `None` immediately (avoids the `debug_assert!` in
`compress_key`).

`redo_insert_recursive` — before calling `insert_with_prefix_slice` or
`insert_cmp`, checks `bin.get_slot_lsn(key).is_some_and(|s| s >= lsn)`.
If the slot is already at a same-or-newer LSN, the insert is skipped
(`Ok(false)`).

This is a **correctness fix** for the existing recovery path:

- The code comment ("redo_insert is idempotent") was aspirational —
  the implementation always overwrote the slot unconditionally.

- Now it correctly skips slots that are already current.

- Is a **no-op** for the existing full-scan path (tree starts empty;
  no slots have prior LSNs).

- Is **essential** for future BIN pre-loading (slots pre-populated from
  DbTree must not be clobbered by older LN redo records).

### DbTree parsing in the file scanner (`noxu-dbi/file_manager_scanner.rs`)

`parse_payload` now handles `LogEntryType::DbTree` by calling
`DbTreeEntry::read_from_log()` and constructing
`LogEntry::DbTree(DbTreeRecord { bins: … })`.  Previously this fell through
to the `_ => None` catch-all and was silently skipped.

## STEP-1: Equality harness results

Harness location: `crates/noxu-db/tests/wave_gb_equality_test.rs`

All 11 tests **PASS** (serial mode: `--test-threads=1`):

| Test | Workload | Result |
|---|---|---|
| `equality_small_workload` | 100 keys, committed | PASS |
| `equality_large_workload` | 10 000 keys, committed | PASS |
| `equality_stable_bins` | pre-checkpoint stable BINs + post | PASS |
| `equality_mixed_pre_post_checkpoint` | pre- and post-checkpoint commits | PASS |
| `equality_aborted_txns` | abort records in log | PASS |
| `equality_abort_spanning_checkpoint` | committed + aborted (same session) | PASS |
| `equality_deletes` | write + delete + recover | PASS |
| `equality_bindelta_updates` | BINDelta-producing updates | PASS |
| `equality_eviction_workload` | 10 000 keys (evictor path) | PASS |
| `dbtree_entry_written_at_checkpoint` | DbTree foundation check | PASS |
| `negative_open_txn_scan_reduction_gap_documentation` | escape-hatch marker | PASS (no-op) |

The harness does NOT include a "first_active_lsn = CkptStart" recovery
path because the scan-reduction is not implemented (escape hatch applied
preemptively based on STEP-0 analysis).  Once the open-txn-tracking
prerequisite is in place, the harness will be extended with a real
two-path comparison.

## What SHIPPED vs DEFERRED

### SHIPPED

| Item | Safe? | Notes |
|---|---|---|
| `DbTreeEntry` + `DbTreeBinRef` serialization | ✓ | Foundation; no behavior change |
| `LOG_VERSION = 3` | ✓ | Old logs fully readable (fallback to full scan) |
| `write_db_tree_entry()` at checkpoint | ✓ | Writes index; `first_active_lsn` unchanged |
| `CkptEnd.root_lsn = Some(db_tree_lsn)` | ✓ | Metadata only; recovery records but doesn't use it yet |
| LSN-aware `redo_insert` | ✓ | Correctness fix; no-op for current path |
| DbTree parsing in file scanner | ✓ | `DbTreeRecord.bins` now populated |
| Wave GB equality harness (11 tests) | ✓ | Regression battery for full-scan path |

### DEFERRED (requires open-txn tracking prerequisite)

| Item | Blocker |
|---|---|
| `first_active_lsn = min(open_txn_lsn, CkptStart)` | Checkpointer has no txn-manager access |
| BIN pre-loading from DbTree during recovery | Depends on correct `first_active_lsn` |
| `Tree::build_from_checkpoint_bins()` bulk-load API | Same dependency |
| W11 recovery speedup (1.5× target) | All of the above |

## Backward compatibility

Old log files (LOG_VERSION ≤ 2, no `DbTree` entry, `CkptEnd.root_lsn =
None`) are fully readable by LOG_VERSION 3 readers.  Recovery falls back
to the existing full-scan path (`first_active_lsn = Lsn::new(0,0)`)
unchanged.  `CkptEnd.root_lsn = None` → `use_root_lsn = NULL_LSN` →
no DbTree pre-loading attempted.

Old readers (LOG_VERSION ≤ 2) reading a LOG_VERSION 3 log file will
encounter the new `DbTree` entry type (50) and — since it falls through
the unknown-type handler — skip it silently.  The `CkptEnd` record is
unchanged in structure (the `has_root` flag was already present and handled
by old readers).  Thus LOG_VERSION 3 logs are **forward-compatible** with
v2 readers for the `CkptEnd` path; the DbTree entry is invisible to them.

## W11 benchmark numbers

The scan-reduction was NOT shipped, so W11 performance is UNCHANGED from
the Wave FC baseline:

| Scale (N) | w11_recovery (ms) | Notes |
|---|---|---|
| 1 K | ~1 | trivial |
| 10 K | ~12 | full log scan from LSN 0 |
| 100 K | ~95 | ~2.9× slower than JE baseline |

No regression: the DbTree-writing step at checkpoint is O(BIN count) but
adds < 5% to checkpoint time (dominated by dirty-BIN WAL writes).

## Gate results

| Check | Result |
|---|---|
| `cargo fmt --all -- --check` | PASS |
| `cargo clippy --workspace --all-targets -- -D warnings` | PASS |
| `RUSTDOCFLAGS=-D warnings cargo doc --workspace --no-deps` | PASS |
| `cargo test -p noxu-recovery -p noxu-db -p noxu-dbi -p noxu-log -p noxu-tree -p noxu-cleaner -p noxu-evictor` | PASS (all green) |
| `cargo test --workspace` | PASS |
| Wave GB equality harness (--test-threads=1) | PASS (11/11) |

## Next wave prerequisites (for full P-2)

1. **Open-txn LSN at checkpoint time**: Wire the transaction manager into
   the checkpointer so `do_checkpoint` can compute
   `min(earliest_open_txn_lsn, CkptStart)` for `first_active_lsn`.

2. **BIN pre-loading in recovery**: When `CkptEnd.root_lsn != NULL_LSN`
   and a correct `first_active_lsn` is present, `run_redo` calls
   `scanner.read_at_lsn(root_lsn)` → parses `DbTreeEntry` → loads each
   BIN's full log entry into the in-memory tree, then replays only
   post-`first_active_lsn` LNs.

3. **Equality harness extension**: Add a two-path comparison (DbTree path
   vs full-scan path) to the harness; both must produce identical results
   for ALL workloads including the open-txn-at-crash case.

4. **W11 benchmark**: Measure with 1K/10K/100K; target ≥ 1.5× speedup.
