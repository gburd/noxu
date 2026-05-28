# Wave 11-K — Recovery / Log-Scanner Allocation Reduction

**Status:** In progress  
**Branch:** `fix/wave11-k-recovery-alloc`  
**Parent investigation:** [wave-11-h-perf-investigation.md](wave-11-h-perf-investigation.md)

---

## Diagnosis

Wave 11-H identified W11 (recovery / re-open after clean close) as 2.9× slower
than JE. The root cause is per-record allocation in the redo path, not I/O.

### Hot path: `recovery_manager::redo_ln`

`crates/noxu-recovery/src/recovery_manager.rs` (around line 1120):

```rust
let data = rec.data.as_deref().map(<[u8]>::to_vec).unwrap_or_default();
…
tree.insert(rec.key.to_vec(), data, lsn);
```

At 100 K records this produces ≥ 300 K small allocations:

1. `rec.key.to_vec()` — key allocation per LN record
2. `rec.data.…to_vec()` — data allocation per LN record
3. `BinStub::insert_with_prefix` inside `Tree::insert` — new `BinEntry` slot
   allocation per LN record, plus geometric `Vec::resize` when the BIN is not
   pre-reserved

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

### Call path

```
redo_ln
  └─ Tree::insert(key: Vec<u8>, data: Vec<u8>, lsn: Lsn)
       └─ Tree::insert_recursive
            └─ BinStub::insert_with_prefix   ← third allocation + potential resize
```

---

## Fixes

Three complementary changes, all in the redo path only. No on-disk format
change. Public API in `noxu-db` is unaffected.

### Fix 1 — `Tree::redo_insert(&[u8], &[u8], Lsn)` eliminates the first two allocations

Add `Tree::redo_insert(key: &[u8], data: &[u8], lsn: Lsn)` in
`crates/noxu-tree/src/tree.rs`. This method copies the key/data bytes into the
`BinEntry` exactly once inside `BinStub::insert_with_prefix`, rather than
materializing two intermediate `Vec<u8>` before calling `Tree::insert`.

`redo_ln` is updated to call `tree.redo_insert(rec.key, rec.data.as_deref().unwrap_or(&[]), lsn)`.

### Fix 2 — `RecoveryScratch` reused across the redo loop

Add `RecoveryScratch { key_buf: Vec<u8>, data_buf: Vec<u8> }` in
`crates/noxu-recovery/src/recovery_manager.rs`. A single scratch instance is
allocated before the redo loop and passed into `parse_entry_from_bytes`
(and its callers in `crates/noxu-dbi/src/file_manager_scanner.rs`) so that
intermediate deserialization buffers grow once and are reused. This closes
the `bytes::owned_clone`/`owned_drop` ~6.6 % cost.

### Fix 3 — Per-BIN `entries.reserve(n)` pre-warm before redo inserts

The analysis phase builds a per-BIN dirty-slot count. Before the redo loop
visits any BIN, call `bin.reserve(dirty_slot_count)` once. This eliminates
the repeated `_int_malloc`/`malloc_consolidate` calls as the BIN's internal
`Vec<BinEntry>` grows during redo.

File locations:
- `crates/noxu-recovery/src/analysis_phase.rs` — emit per-BIN slot count
- `crates/noxu-recovery/src/recovery_manager.rs` — call `reserve()` before redo loop
- `crates/noxu-tree/src/tree.rs` — `Tree::pre_warm_bin(bin_id, n)` helper

---

## Benchmark Results

<!-- Populated after implementation -->

### Baseline (commit 711cb65, main)

| Scale | Storage | Noxu W11 (ms) | JE W11 (ms) | Ratio |
|------:|---------|-------------:|------------:|------:|
|   1 K | tmpfs   | TBD          | TBD         | TBD   |
|  10 K | tmpfs   | TBD          | TBD         | TBD   |
| 100 K | tmpfs   | TBD          | TBD         | TBD   |
|   1 K | NVMe    | TBD          | TBD         | TBD   |
|  10 K | NVMe    | TBD          | TBD         | TBD   |
| 100 K | NVMe    | TBD          | TBD         | TBD   |

### After Wave 11-K

| Scale | Storage | Noxu W11 (ms) | JE W11 (ms) | Ratio |
|------:|---------|-------------:|------------:|------:|
|   1 K | tmpfs   | TBD          | TBD         | TBD   |
|  10 K | tmpfs   | TBD          | TBD         | TBD   |
| 100 K | tmpfs   | TBD          | TBD         | TBD   |
|   1 K | NVMe    | TBD          | TBD         | TBD   |
|  10 K | NVMe    | TBD          | TBD         | TBD   |
| 100 K | NVMe    | TBD          | TBD         | TBD   |

### Acceptance gate

W11 must close to within **1.5× of JE on tmpfs** and **1.3× on NVMe**.

---

## Allocator Profile Delta

<!-- Populated after implementation — fresh `perf` capture showing malloc % drop -->

---

## Correctness

Recovery correctness is non-negotiable. All existing recovery tests in
`crates/noxu-recovery/src/recovery_manager.rs::tests` and
`crates/noxu-engine/tests/` must pass byte-for-byte. The `noxu_spec::recovery`
stateright spec must still be green.

Two new property tests are added (per Wave 11-H recommendation):
1. Randomly populated, cleanly closed env: post-recovery tree is byte-equal to
   pre-close tree when using the new redo path.
2. Mid-batch crash injection: `redo_insert` path produces the same final state
   as the original per-record path.
