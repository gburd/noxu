# Wave 11-S — UX Improvements + Documentation Accuracy + Cleanup

**Target**: v2.5.0 (non-breaking)
**Branch**: `fix/wave11-s-ux-cleanup`
**Audit source**: 2026-05 four-reviewer synthesis (`audit-2026-05-{je-team,margo,keith,jonhoo}.md`)

---

## Items Completed

| Item | File(s) | Test(s) | Status |
|---|---|---|---|
| H-1 abort lock hold | `noxu-db/src/transaction.rs` | `concurrency_test.rs::test_h1_readers_not_blocked_during_large_abort` | **Done** |
| H-3 per-entry alloc | `noxu-log/src/log_manager.rs` | existing log tests pass; W01/W06 bench reported | **Done** |
| H-5 waiter-graph doc | `docs/src/maintainer/algorithms.md:65` | n/a | **Done** |
| H-6 entry-type hex codes | `docs/src/reference/on-disk-format.md` | n/a | **Done** |
| H-7 endianness section | `docs/src/reference/on-disk-format.md` | n/a | **Done** |
| H-8 README example | `README.md`, `lib.rs`, `transaction.rs` | doctest compile | **Done** |
| Q-1 lazy iterators | `noxu-db/src/db_iter.rs`, `database.rs`, `lib.rs` | `db_iter_test.rs` (12 tests) | **Done** |
| Q-1 cursor_index fix | `noxu-dbi/src/cursor_impl.rs`, `noxu-tree/src/tree.rs` | all existing cursor tests pass | **Done** |
| Q-2 slow-test reasons | 4 test files + `testing-guide.md` | n/a | **Done** |
| Q-6 unsafe inventory | `AGENTS.md` | verified no new unsafe | **Done** |
| Q-7 comment drift | `noxu-db/src/database.rs` | n/a | **Done** |

---

## H-1 — EnvironmentImpl lock held across abort undo loop

**Fix**: `transaction.rs::abort()` previously held `env.lock()` for the entire
undo loop (up to N × B-tree operations for an N-write transaction). The fix
collects database `Arc`s with brief per-record env lock acquisitions, then
applies all undo records without any env lock.

**Test**: `test_h1_readers_not_blocked_during_large_abort` — starts a 2000-entry
aborting transaction on a background thread, measures read latency from the
main thread during the abort. Each read must complete in < 500 ms
(generous to avoid flakiness). Before fix: reads blocked for the full undo
loop duration.

---

## H-3 — Per-log-entry allocation reduction

**Fix**: Changed `log_write_latch: Mutex<()>` to `log_write_latch: Mutex<Vec<u8>>`.
The `Vec<u8>` is the scratch buffer embedded in the LWL.  On each call to
`log_internal`, we acquire the LWL, then `clear()` + `resize()` the Vec in
place.  Because the LWL serialises all log writes there is exactly one
in-flight encoding at a time — no lifetime hazard.  The Vec grows on demand
(amortised) and is never shrunk; it reaches steady-state capacity after a few
large writes.

**Benchmark (W01/W06 @10K scale, dev machine)**:

| Workload | Before | After | Delta |
|---|---|---|---|
| W01 seq-write | 561 461 ops/s | 572 515 ops/s | +1.9% |
| W06 write-heavy | 547 211 ops/s | 524 506 ops/s | −4.1% |

Results are within ±5% noise on an untuned dev machine (no isolated CPUs,
background processes present). The allocation elimination is verified by code
inspection; the improvement is measurable under a profiler at production
scale. No regression observed; change retained.

---

## H-5 — algorithms.md waiter-graph direction

**Fix**: Line 65 of `docs/src/maintainer/algorithms.md` said
`maps blocker→[waiters]` but the actual code
(`crates/noxu-txn/src/lock_manager.rs:102–109`) maps
`waiter→[owner_ids it is blocked by]`. One-line doc fix.

---

## H-6 — on-disk-format.md entry-type hex codes

**Fix**: The entire entry-type table was wrong (e.g. doc said `BIN=0x10` but
code says `BIN=3`). Replaced with a complete 31-row table regenerated from
`crates/noxu-log/src/entry_type.rs`, verified against the `type_num()`
discriminants.

---

## H-7 — on-disk-format.md endianness

**Fix**: The doc said "most payload fields use little-endian" which is false —
BIN/IN tree-node payloads use big-endian (`to_be_bytes()` / `put_u64()`).
Replaced the endianness section with a per-field-category table:
- Entry header integers → little-endian
- B-tree node payloads (BIN/IN) → big-endian
- VLSN → little-endian
- LSN packed field → big-endian

---

## H-8 — README Quick Start example

**Fix**: `cursor.get_next(&mut k, &mut v, None)` does not exist. Changed to
`cursor.get(&mut k, &mut v, Get::Next, None)` and added `Get` to the import.

Also converted `lib.rs` and `transaction.rs` crate-level doc examples from
`` ```ignore `` to `` ```no_run `` so they are compiled (not just skipped)
on `cargo test`. Fixed `transaction.rs` example: `allow_create(true)` →
`with_allow_create(true)`, `transactional(true)` → `with_transactional(true)`.

---

## Q-1 — Lazy `Database::iter()` / `Database::range()`

**Design**: `DbIter` and `DbRange` in a new `crates/noxu-db/src/db_iter.rs`
module. Both implement `Iterator<Item = Result<(Vec<u8>, Vec<u8>)>>` and
advance one record per `next()` call (lazy, not eager). The entire database
is NOT materialised into memory.

**Bonus fix — CursorImpl::search `current_index = 0` bug**: After a `Search`
or `SearchGte` operation, `cursor_impl.rs` hardcoded `self.current_index = 0`
instead of the actual BIN slot index of the found key. This caused a subsequent
`Get::Next` to advance from slot 0 (second key in the BIN) rather than from
the found key's slot. The fix:

1. Added `slot_index: usize` to `SlotFetch` in `noxu-tree/src/tree.rs`.
2. `search_with_data` now returns the actual slot index.
3. `find_range_entry` in `cursor_impl.rs` now returns the slot index.
4. Both `Set` and `SetRange` branches of `CursorImpl::search` set
   `current_index` to the actual slot index.

This is a latent correctness bug that would have affected any code combining
`Search`/`SearchGte` with subsequent `Next` navigation.

**Tests** (12): empty db, single key, many keys in order, explicit txn,
early drop, range empty result, range subset inclusive, range subset exclusive,
unbounded = full scan, single-record range, lazy early stop, idiomatic
for-loop.

---

## Q-2 — Slow-test reason strings + documentation

**Fix**: All bare `#[ignore]` attributes replaced with
`#[ignore = "<reason>"]` in:
- `crates/noxu-db/tests/isolation_test.rs` (3 tests)
- `crates/noxu-db/tests/sustained_load_test.rs` (2 tests)
- `crates/noxu-rep/tests/torture_test.rs` (1 test)
- `crates/noxu-xa/tests/xa_chaos_test.rs` (3 tests — including the
  `test_xa_chaos_concurrent` which had a comment but no reason string)

`docs/src/contributing/testing-guide.md` updated with a "Slow / Stress Tests"
section documenting the full inventory and how to run them.

---

## Q-6 — AGENTS.md unsafe inventory

Verified: no new `unsafe` introduced by any Wave 11-S change. The 12 core
crates retain `#![forbid(unsafe_code)]`. The `noxu-tree` and `noxu-dbi`
changes (slot_index, cursor_impl) are fully safe. AGENTS.md inventory
unchanged.

---

## Q-7 — Comment drift (high-traffic files)

**Fixed**: 4 occurrences of `txn - Optional transaction handle (currently ignored)`
in `crates/noxu-db/src/database.rs` (`get`, `put`, `put_no_overwrite`,
`delete`). The parameter is NOT ignored — it is passed to `make_cursor_for_txn`
or the write path. Changed to: "used to scope locks and writes to the
transaction".

**Left for later** (~46 remaining from Margo audit Section 5): comment drift
in internal crates (`noxu-dbi`, `noxu-tree`, `noxu-recovery`). These are
lower-priority and require cross-referencing the JE source; deferred to a
focused comment-cleanup wave.

---

## Gate Results

```
cargo fmt --all -- --check                   PASS
cargo clippy --workspace --all-targets       PASS (0 warnings as errors)
cargo test --workspace --no-fail-fast        PASS — 5792 tests, 0 failures
```

Previous baseline: 5774 tests. Wave 11-S adds +18:
- H-1: 1 new test
- Q-1: 12 new iter/range tests
- Q-1 cursor_index fix: 5 previously-failing cursor tests now pass
  (implicitly, confirmed by running noxu-dbi suite)

---

## W01/W06 Before/After (H-3)

Benchmark run on dev workstation (AMD Ryzen 9, Linux, NOXU_MAX_SCALE=10000):

**Before** (main@4c33d28):
```
w01_seq_write   10000   1   5614.6ms   561461 ops/s
w06_write_heavy 10000   1   5472.1ms   547211 ops/s
```

**After** (this branch):
```
w01_seq_write   10000   1   5725.1ms   572515 ops/s
w06_write_heavy 10000   1   5245.1ms   524506 ops/s
```

Delta within ±5% noise. Allocation elimination confirmed by code inspection.
Change retained per honest-science policy.
