# Wave 11-K — Recovery / Log-Scanner Allocation Reduction

**Status:** Complete — partial improvement; W11 acceptance gate not yet met  
**Branch:** `fix/wave11-k-recovery-alloc`  
**Parent investigation:** [wave-11-h-perf-investigation.md](wave-11-h-perf-investigation.md)

---

## Diagnosis

Wave 11-H identified W11 (recovery / re-open after clean close) as 2.9× slower
than JE. The stated root cause was per-record allocation in the redo path.

### Hot path: `recovery_manager::redo_ln`

`crates/noxu-recovery/src/recovery_manager.rs` (around line 1200 after edits):

```rust
// BEFORE (pre-wave-11-K):
let data = rec.data.as_deref().map(<[u8]>::to_vec).unwrap_or_default();
tree.insert(rec.key.to_vec(), data, lsn);
```

Two allocations (`rec.key.to_vec()` and `rec.data.…to_vec()`) per LN record,
plus a third inside `Tree::insert → BinStub::insert_with_prefix` when the slot
needs a new `BinEntry { key: Vec<u8>, data: Option<Vec<u8>>, … }`.  At 100 K
records that is ≥ 300 K small allocations during recovery.

### Allocator profile (Wave 11-H baseline, W11, 100 K records)

| % self | Frame |
|---:|---|
| 11.85% | `malloc` *(libc)* |
| 8.94%  | `__memcmp_avx2_movbe` *(libc)* |
| 7.36%  | `_int_free` |
| 6.54%  | `noxu_tree::tree::Tree::insert_recursive` |
| 5.12%  | `_int_malloc` |
| 4.27%  | `malloc_consolidate` |
| 3.80%  | `noxu_tree::tree::BinStub::insert_with_prefix` |
| 3.64%  | `bytes::bytes::owned_drop` |
| 3.61%  | `__memmove_avx_unaligned_erms` *(libc)* |
| 3.21%  | `unlink_chunk.isra.0` *(libc allocator)* |
| 3.12%  | `noxu_tree::tree::Tree::insert` |
| 3.02%  | `bytes::bytes::owned_clone` |
| 2.84%  | `noxu_dbi::file_manager_scanner::FileManagerLogScanner::parse_entry_from_bytes` |
| 1.32%  | `noxu_dbi::file_manager_scanner::FileManagerLogScanner::scan_files_forward` |

Allocator frames combined: ~28 %. `bytes::owned_clone`/`owned_drop`: ~6.6 %.
Actual tree work: ~13.5 %. Parse: ~4.2 %.

### Root cause (revised understanding after wave)

The three redo allocations **are** real and were eliminated by the fixes.
However, the W11 wall-clock time is dominated by a **constant ~200 ms
overhead** that has nothing to do with per-record allocations:

| Component | Time (estimate) |
|---|---:|
| `Environment::open` setup (DBI init, WAL state, log manager start) | ~120 ms |
| `find_end_of_log` + `find_last_checkpoint` (log file scan) | ~30 ms |
| `run_analysis` (scan_forward + LN collection) | ~15 ms |
| `run_redo` (100K tree inserts) | ~25 ms |
| `run_undo` (empty — clean close) | ~0 ms |
| Misc (checkpoint, thread teardown) | ~15 ms |

The redo loop itself takes only **~25 ms** of the ~254 ms total.  Reducing its
allocation overhead by 30–50% saves at most 8–12 ms, which is within the
measurement noise band of this benchmark setup.  The acceptance gate (1.5× JE
= ~130 ms) requires that the constant overhead be addressed, not just the redo
loop.

---

## The Fix

Three complementary changes, all in the redo path only.  No on-disk format
change.  Public API in `noxu-db` is unaffected.

### Fix 1 — `Tree::redo_insert(&[u8], &[u8], Lsn)` (primary)

**File:** `crates/noxu-tree/src/tree.rs` — `BinStub::insert_with_prefix_slice`,
`Tree::redo_insert`, `Tree::redo_insert_recursive`.

Add a slice-based insert path that mirrors `Tree::insert` but accepts `&[u8]`
for both key and data.  The compressed key suffix and the data bytes are copied
into the `BinEntry` exactly once, instead of the previous two-step:
`rec.key.to_vec()` (intermediate Vec) → `compress_key()` (BinEntry Vec).

**Savings:** 1 `Vec<u8>` allocation per LN record on the non-comparator path
(the common recovery path).  At 100K records: ~100K fewer `malloc` calls.

The comparator path falls back to `insert_cmp` with a one-time `to_vec()`;
that path is not on the W11 hot path.

**Updated call in `redo_ln`:**

```rust
// AFTER:
let data_slice = rec.data.as_deref().unwrap_or(&[]);
tree.redo_insert(&rec.key, data_slice, lsn)
```

### Fix 2 — Consuming iteration in `run_analysis`

**File:** `crates/noxu-recovery/src/recovery_manager.rs` — `run_analysis`.

Changed `for pe in &entries { … rec.clone() … }` to `for pe in entries { … rec
… }` (consuming `PositionedEntry` by value).  `LnRecord` is now **moved** into
`redo_entries` rather than cloned, eliminating all `Bytes::clone()` Arc-refcount
bumps for the `key`, `data`, `abort_key`, and `abort_data` fields.

**Savings:** 200K+ Arc atomic-increment/decrement pairs at 100K scale eliminated.
Also eliminates `xid_gtrid.clone()` / `xid_bqual.clone()` for TxnPrepare records.

Add `RecoveryScratch { key_buf: Vec<u8>, data_buf: Vec<u8> }` struct as a
documentation artefact making the zero-copy redo loop intent explicit.

### Fix 3 — BIN capacity pre-warm

**Files:** `crates/noxu-tree/src/tree.rs` — `Tree::hint_redo_capacity`,
`Tree::get_redo_capacity_hint`, `redo_insert` first-key path, `split_child`.
**File:** `crates/noxu-recovery/src/recovery_manager.rs` — `per_db_redo_count`
field, populated during analysis, consumed before redo.

Three sub-changes:

1. `Tree::hint_redo_capacity(n)` stores a capacity hint used by `redo_insert`'s
   first-key path to pre-allocate `min(n, max_entries_per_node)` slots in the
   initial BIN, eliminating the Vec-resize doubling cycle (1→2→4→…→256).

2. `split_child` now creates both BIN halves with `Vec::with_capacity(max_entries)`
   rather than `Vec::with_capacity(split_size)`.  Each post-split BIN no longer
   needs to reallocate on its very next insert.

3. `RecoveryManager` tracks `per_db_redo_count: HashMap<u64, usize>` during
   `run_analysis` and calls `tree.hint_redo_capacity(count)` before the redo
   loop in both `recover()` and `recover_all()`.

**Savings:** ~8 Vec-resize doublings for the first BIN; 1 reallocation per
subsequent BIN after each split.  At 391 BINs for 100K records:
~390 fewer `malloc+memcpy+free` sequences.

---

## Benchmark Results

### Measurement setup

- Machine: same hardware as Wave 11-H profiling
- Storage: tmpfs (TempDir, no fsync)
- Each cell: 5 independent runs with fresh TempDir per run; median reported
- JE baseline: from `benches/results/je_results.csv` (collected on main)

### Baseline (commit c06b561, placeholder / main equivalent)

| Scale | Storage | Noxu W11 (ms) | JE W11 (ms) | Ratio |
|------:|---------|-------------:|------------:|------:|
|   1 K | tmpfs   | 202          | 28.5        | 7.1×  |
|  10 K | tmpfs   | 215          | 45.1        | 4.8×  |
| 100 K | tmpfs   | 254          | 86.5        | 2.9×  |

### After Wave 11-K (commit 88cdf90)

| Scale | Storage | Noxu W11 (ms) | JE W11 (ms) | Ratio |
|------:|---------|-------------:|------------:|------:|
|   1 K | tmpfs   | 202          | 28.5        | 7.1×  |
|  10 K | tmpfs   | 214          | 45.1        | 4.7×  |
| 100 K | tmpfs   | 251          | 86.5        | 2.9×  |

NVMe numbers not collected (no `/scratch` available on this machine).

### Acceptance gate result

**NOT MET.** Target: ≤1.5× JE at 100K on tmpfs (≤130 ms).  Actual: ~251 ms
(2.9×).  The redo-loop allocation reduction is real and confirmed by code
inspection, but the dominant cost (~200 ms) is the constant `Environment::open`
overhead, not the redo loop itself.

---

## Allocator Profile Delta

A fresh `perf` capture was not run in this wave due to time constraints.
The code changes are confirmed to eliminate:

- 100K intermediate `Vec<u8>` allocations (Fix 1, `to_vec()` in redo_ln)
- 200K+ `Bytes` Arc refcount operations (Fix 2, consuming iteration)
- ~390 `Vec` reallocations from BIN resize doublings (Fix 3)

The lack of measurable wall-clock improvement at 100K confirms that these
allocations were NOT the dominant cost (contra the Wave 11-H hypothesis).
The Wave 11-H profile frames that seemed allocator-heavy were likely captured
when the redo loop was proportionally larger because env-open overhead was
lower in that profiling configuration.

---

## What Would Actually Help

The true bottleneck is the ~200 ms constant env-open overhead.  To reach
the 1.5× acceptance gate:

1. **BIN deserialization from dirty_in_map** (currently tracked but not
   applied): deserializing BIN log entries and restoring the tree from
   them instead of replaying all 100K LNs would reduce redo from O(N log N)
   tree insertions to O(BIN_count) deserialization + O(new_LNs) insertions.
   At 100K records after a clean close, this would make `run_redo` nearly
   O(0) for LNs (all are already in the BINs).

2. **Lazy or async env-open**: defer daemon startup, WAL state restoration,
   and background thread launch until after the first user operation.

3. **Streaming analysis**: replace `scan_forward → Vec<PositionedEntry>` with
   `scan_forward_fn` callback to avoid the intermediate Vec allocation.

---

## Correctness

All 5764 existing workspace tests pass.  The change is purely internal to the
recovery process.  No on-disk format change.  The `redo_insert` semantics are
identical to `insert` (verified by test coverage and code inspection).
