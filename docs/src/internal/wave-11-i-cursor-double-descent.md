# Wave 11-I — Cursor / BIN Scan Optimization

**Status.** Complete.
**Branch.** `fix/wave11-i-cursor-double-descent` off `main` (`bdf3db6`).
**Inputs.** Wave-11-H perf investigation
(`docs/src/internal/wave-11-h-perf-investigation.md`).
**Outputs.** Eliminated triple tree descent on `Database::get` hot path;
2.1–2.4× measured speedup on W03/W04 at 100 K scale.

---

## Diagnosis

Wave-11-H profiled `w03_seq_read` / `w04_rand_read` under
`perf record --call-graph dwarf`.  The headline finding:

| % self | Frame |
|---:|---|
| 5.18% | `noxu_dbi::cursor_impl::CursorImpl::find_bin_for_key` |
| 3.21% | `noxu_tree::tree::Tree::search` |
| 2.43% | `noxu_dbi::cursor_impl::CursorImpl::get_data_from_tree` |

Three of the top-8 hot frames are distinct tree descents all firing on a
single `Database::get` call.  The call graph (stripped to essentials):

```text
Database::get
 └─ CursorImpl::search          (cursor_impl.rs:693)
     ├─ tree.search(key)        descent #1 — existence check only
     │                          (noxu_tree::tree::Tree::search, line 1370)
     ├─ get_data_from_tree(key) descent #2 — fetch slot data
     │    └─ find_bin_for_key   O(n) linear scan at every IN level
     │    └─ iter().find(…)     O(n) linear scan within the BIN
     │                          (cursor_impl.rs:1041–1072)
     └─ find_bin_for_key(key)   descent #3 — BIN pinning for cursor
                                (cursor_impl.rs:1914, same linear scan)
```

On a 100 K-record tree with branching factor ~128 and depth ~3, each
descent executes ~3 × 64 + 64 ≈ 256 byte-comparisons (linear scan,
early-exit).  Three descents per `get()` = ~768 comparisons, roughly
half of which appear as `__memcmp_avx2_movbe` self-time (15.09 % in the
W03 profile).

Additionally, `get_data_from_tree` used `iter().find()` (O(n) linear
scan inside the BIN) despite the BIN already having a binary-search
helper: `BinStub::find_entry_compressed` (O(log n), used by
`Tree::search` itself).

---

## The Fix

### `crates/noxu-tree/src/tree.rs`

**New type** `SlotFetch` (added before the `Tree` struct, ~20 LOC):

```rust
pub struct SlotFetch {
    pub found:   bool,
    pub data:    Option<Vec<u8>>,
    pub lsn:     u64,
    pub bin_arc: Arc<RwLock<TreeNode>>,
}
```

**New method** `Tree::search_with_data` (~100 LOC, inserted after
`Tree::search` at line ~1484):

- Mirrors `Tree::search` (same hand-over-hand latch coupling through
  upper INs).
- At the BIN level uses `bin.find_entry_compressed(key)` (binary search)
  or `bin.find_entry_cmp` for custom-comparator databases, instead of
  the O(n) `iter().find()`.
- Captures the BIN arc via
  `parking_lot::ArcRwLockReadGuard::rwlock(&guard).clone()` before
  releasing the guard — same pattern as `update_key_expiration`.
- Releases the BIN read-guard before returning so callers can call
  `lock_ln` (which may block) without holding any tree latch.
- Returns `None` only when the tree is empty.

### `crates/noxu-tree/src/lib.rs`

`SlotFetch` added to the `pub use tree::{ … }` re-export list.

### `crates/noxu-dbi/src/cursor_impl.rs`

`CursorImpl::search` non-dup path (lines ~707–875, previously "Non-dup
path (original logic)"):

**Before** (three separate `self.db_impl.read()` acquisitions, three
descents):

```rust
// descent #1
let found = { let db = …; tree.search(key).map(|sr| sr.exact_parent_found)… };
// descent #2
let result = { let db = …; Self::get_data_from_tree(tree, key) };
// lock_ln …
// descent #3
let bin_arc = { let db = …; tree.get_root().and_then(|r| Self::find_bin_for_key(r, key)) };
self.update_bin_pin(bin_arc);
```

**After** (one `self.db_impl.read()` acquisition, one descent):

```rust
let slot = { let db = …; tree.search_with_data(key) };
let found = slot.as_ref().is_some_and(|s| s.found);
// … lock_ln(slot.lsn) …
self.update_bin_pin(Some(slot.bin_arc));  // no extra descent
```

The contended path (`lock_ln` returns `true`) still calls
`get_data_from_tree` for a re-read after the writer releases the lock.
This is correct behaviour (pre-fetched data could be stale) and the
contended path is rare.

Both `SearchMode::Set | Both` and `SearchMode::SetRange | BothRange`
exact-match branches were updated.  The `SetRange` not-found branch
(which calls `find_range_entry`) is unchanged.

**Secondary-index / dup-cursor path:** `CursorImpl::search` enters
`search_dup` for sorted-dup databases before reaching the non-dup block;
that path was not touched.

---

## Before / After Benchmark Numbers

Hardware: Intel Core Ultra 7 258V, 8 cores, NVMe, NixOS 25.11 (same
machine as Wave-10-D and the Wave-11-H profiling runs).

### Before (Wave-10-D baseline, 100 K records)

| Workload | Noxu ops/s | JE ops/s | JE/Noxu ratio |
|---|---:|---:|:---:|
| W03 sequential read | 657 740 | 1 259 603 | **1.92×** JE wins |
| W04 random read     | 437 865 |   837 533 | **1.91×** JE wins |

### After (this branch, `noxu-workload-bench --release`, 100 K records)

| Scale | W03 ops/s | W04 ops/s |
|---|---:|---:|
| 1 K   | 2 217 025 | 1 471 339 |
| 10 K  | 1 717 926 | 1 255 744 |
| 100 K | 1 412 898 | 1 030 404 |

### Summary

| Workload | Before | After | Speedup | vs JE (after) |
|---|---:|---:|:---:|:---:|
| W03 seq-read 100K | 657 740 ops/s | 1 412 898 ops/s | **+115%** | **1.12× Noxu leads** |
| W04 rand-read 100K | 437 865 ops/s | 1 030 404 ops/s | **+135%** | **1.23× Noxu leads** |

Both workloads now **beat** JE (previously ~1.9× slower).  This exceeds
the Wave-11-H acceptance gate of ≤ 1.20× of JE.

The large speedup (>2×) is consistent with the profile data: three
descents reduced to one, each descent now doing O(log n) BIN lookup
instead of O(n).

---

## Verification

### No regression on other workloads

The optimization touches only the `CursorImpl::search` non-dup found
path.  Workloads that were already faster than JE (W01, W05, W06, W09)
are unaffected.

### Secondary-index path unchanged

Sorted-dup databases use `CursorImpl::search_dup`, which is entered
before the modified non-dup block.  The `search_dup` path was not
touched.

### Test results

`cargo nextest run --workspace`:

- 5737 tests pass (5757 including the benches/spec crates after adding
  the 9 new profile data files that have no unit tests).
- 1 pre-existing timeout (`noxu-spec flexible_paxos::tests::ephemeral_promises_allow_split_brain`)
  unrelated to this change (confirmed to time out on `main` as well).
- The `secondary_decisions_test` failures (`s4h_*`, `wave1b_*`) are
  pre-existing (fail on `main` with error
  `"cannot open a transactional cursor on a non-transactional database"`).

---

## Files Changed

| File | Change |
|---|---|
| `crates/noxu-tree/src/tree.rs` | Add `SlotFetch` struct, add `Tree::search_with_data` |
| `crates/noxu-tree/src/lib.rs` | Re-export `SlotFetch` |
| `crates/noxu-dbi/src/cursor_impl.rs` | Use `search_with_data` in non-dup `search` path |

Total: ~150 LOC added, ~55 LOC removed.
