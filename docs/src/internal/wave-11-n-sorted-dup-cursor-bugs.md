# Wave 11-N — sorted-dup cursor bug fixes

This note captures the four `noxu-dbi` sorted-dup cursor bugs that
Wave 11 (v2.3.1 follow-ups) surfaced and that this wave closes.  All
four are now backed by passing regression tests.

The bugs share a common pathology: incomplete multi-primary or
cross-BIN handling in `CursorImpl`'s sorted-dup logic.  Single-primary
sorted-dup use was always correct, which is why
`crates/noxu-db/tests/sorted_dup_test.rs` did not catch them.

| # | Symptom | Root cause | Fix |
|---|---|---|---|
| 1 | `Cursor::count()` over-counts past the first dup of a primary on multi-primary sorted-dup DBs | `count()`'s `backward + 1 + forward` formula double-counted because `backward` left scratch on the first dup, and `forward` then re-traversed every dup including the original position | Drop the `backward` term — total = `forward + 1` |
| 2 | `Get::Search` + `Get::NextDup` returns `NotFound` on every primary except the lexicographically smallest | `search_dup` hard-coded `current_index = 0` after locating the entry, so `retrieve_next` computed the wrong `next_index` | New `Tree::first_entry_at_or_after_with_index` returns the actual BIN slot; `search_dup` stores it and pins the BIN |
| 3 | `SecondaryCursor::get_search_key` + `get_next_dup_full` raises `SecondaryIntegrityException` past the first yield | Same defect as #2 reaching through the secondary layer (the inner cursor stepped onto a foreign primary's data slot, which did not match a real primary record) | Closed by the search_dup fix in #2 |
| 4 | `SecondaryCursor::get_first` + repeated `get_next` revisits primaries or fails to terminate | `apply_dup_filter`'s cross-BIN acceptance paths updated `current_key` / `current_index` but left `current_bin_arc` pointing at the prior BIN, so the next fast-path step read from the stale pin | New `CursorImpl::find_bin_arc_for_key` helper; call `update_bin_pin` at every accept site in `apply_dup_filter` |

## Bug 1 — `Cursor::count()` over-counts

`crates/noxu-dbi/src/cursor_impl.rs` `CursorImpl::count`.

The pre-fix algorithm cloned the cursor at the current position, then:

1. walked backward with `PrevDup` until `NotFound`, counting successes
   into `backward`;
2. walked forward with `NextDup` until `NotFound`, counting successes
   into `forward`;
3. returned `backward + 1 + forward`.

Step 1 leaves scratch parked on the *first* dup of the primary (the
last successful step landed there; the next would have stepped off the
primary).  Step 2's forward walk then traverses positions
`1, 2, …, N−1` — that is, `N−1` successes, where `N` is the dup
count.  Adding `backward` plus the off-by-one `+1` therefore returns
`backward + N`, not `N`.

Empirically, for a 5-dup primary `count()` returned 5 at offset 0, 6 at
offset 1, …, 9 at offset 4 — a textbook `N + i` over-count.

The fix is to use `forward + 1`: the forward walk visits every dup
*after* the first, and we add 1 for the dup scratch is parked on at
the start of the forward walk.

```rust
while let Ok(OperationStatus::Success) =
    scratch.retrieve_next(GetMode::PrevDup) { /* reposition only */ }
let mut forward: i64 = 0;
while let Ok(OperationStatus::Success) =
    scratch.retrieve_next(GetMode::NextDup) { forward += 1; }
return Ok(forward + 1);
```

Regression test: `db_cursor_duplicate_test_duplicate_count`
(`crates/noxu-db/tests/je_db_cursor_test.rs`).  Walks a 6 × 5
multi-primary fixture and asserts `count() == 5` at every position.

## Bug 2 — `Get::Search` + `Get::NextDup` skips non-first primaries

`crates/noxu-dbi/src/cursor_impl.rs` `CursorImpl::search_dup`.

Sorted-dup DBs store `(primary, data)` pairs as a single composite key
`[primary][data][packed_primary_len]`.  After a successful search the
cursor records the raw composite key in `current_key`.  Pre-fix it
*also* hard-coded:

```rust
self.current_index = 0;
```

`current_index` is the slot index inside the BIN, used by
`retrieve_next`'s fast path to compute `next_index = current_index + 1`.
For the lexicographically smallest primary the first dup *does* live
in BIN slot 0, so the bug masks itself; for every other primary, slot
0 holds a different primary, and the next call to `Get::NextDup` reads
that foreign primary's first dup.  `apply_dup_filter` then rejects it
(primary key mismatch) and reports `NotFound`.

The fix adds a sibling of `Tree::first_entry_at_or_after` that returns
the BIN node and the slot index in addition to the entry:

```rust
pub fn first_entry_at_or_after_with_index(&self, key: &[u8])
    -> Option<(Vec<u8>, Vec<u8>, usize, u64,
               Arc<NodeRwLock<TreeNode>>)>;
```

`search_dup` now stores the real `current_index` *and* calls
`update_bin_pin` so subsequent fast-path steps read from the right BIN.
This matches the invariants `get_first` / `get_last` already
maintained.

Regression test: `db_cursor_duplicate_test_get_next_dup`
(`crates/noxu-db/tests/je_db_cursor_test.rs`).  For each of 6 primary
keys it positions with `Get::Search`, walks `Get::NextDup` to
exhaustion, and asserts the dup-set is fully visited in sorted order.

## Bug 3 — `SecondaryCursor::get_search_key` + `get_next_dup_full`

`crates/noxu-db/src/secondary_cursor.rs` `get_search_key`,
`get_next_dup_full` (and shared `step_dup_full`).

`SecondaryCursor::get_search_key` calls `inner.get(Get::Search)`,
which routes through `CursorImpl::search_dup` — so this bug shares
its root cause with Bug 2.  Pre-fix, the very first `get_next_dup_full`
after a `get_search_key` on a non-first secondary key either
(a) reported `NotFound` immediately, or (b) surfaced as a
`SecondaryIntegrityException` because the inner cursor stepped onto a
foreign secondary entry whose stored primary key referenced a record
in the *primary* DB that the user had not inserted.

The Bug 2 fix closes Bug 3 transitively: with `search_dup` storing the
correct BIN slot, `get_next_dup_full` advances inside the requested
secondary key's dup chain instead of jumping into a neighbouring
chain.

Regression test:
`wave11n_bug3_get_search_key_then_next_dup_full_yields_all` in
`crates/noxu-db/tests/wave11n_secondary_dup_test.rs`.  Iterates every
bucket of a 6-bucket sorted-dup secondary (60 primaries) and confirms
each bucket's dup chain is fully and exclusively visited.

## Bug 4 — `SecondaryCursor::get_first` + `get_next` loops

`crates/noxu-dbi/src/cursor_impl.rs` `CursorImpl::apply_dup_filter`.

When `retrieve_next` exhausts the current BIN it crosses to the
adjacent BIN via `Tree::get_next_bin` / `Tree::get_prev_bin`, which
return a detached `Vec<BinEntry>`.  The non-dup path explicitly
re-pins the new BIN before returning.  The dup path, in contrast,
forwarded the new entries to `apply_dup_filter`, which on accept
updated `current_key`, `current_data`, `current_lsn`, and
`current_index` — but **not** `current_bin_arc`.

The next call to `retrieve_next` then took the fast path against the
*stale* BIN pin and read `next_index = current_index + 1` from the BIN
the cursor had already left.  For a small secondary tree (single BIN)
the bug never fired; for the W13 multi-bucket workload (200 records,
16 buckets) the walk would visit ~208 records before reverting to the
old BIN and re-emitting entries until a step cap was hit.

Fix: at every accept site in `apply_dup_filter` (`NextDup` /
`PrevDup`, `NextNoDup` / `PrevNoDup`, `Next` / `Prev`) call
`find_bin_arc_for_key(raw_key)` and `update_bin_pin`.  The new helper
encapsulates the descent:

```rust
fn find_bin_arc_for_key(&self, key: &[u8])
    -> Option<Arc<NodeRwLock<TreeNode>>>
{
    let db = self.db_impl.read();
    let tree = db.get_real_tree()?;
    let root = tree.get_root()?;
    Self::find_bin_for_key(root, key)
}
```

The pin update is a no-op when the BIN has not changed
(`update_bin_pin` already returns early on `Arc::ptr_eq`).

Regression test:
`wave11n_bug4_get_first_get_next_full_walk_terminates` in
`crates/noxu-db/tests/wave11n_secondary_dup_test.rs`.  Walks a
200-record sorted-dup secondary across 16 buckets, with a hard step
cap, and asserts every (sec_key, primary_key) pair is visited
exactly once.

## Files touched

* `crates/noxu-tree/src/tree.rs` — added
  `Tree::first_entry_at_or_after_with_index` (10-line sibling of the
  existing `first_entry_at_or_after`, used by the sorted-dup
  `search_dup` path).
* `crates/noxu-dbi/src/cursor_impl.rs` — `count()`, `search_dup`,
  `apply_dup_filter`, plus new `find_bin_arc_for_key` helper.
* `crates/noxu-db/tests/je_db_cursor_test.rs` — un-`#[ignore]`'d the
  two Wave 11-A regression tests; updated the doc-comment narrative.
* `crates/noxu-db/tests/wave11n_secondary_dup_test.rs` — new
  regression tests for Bug 3 and Bug 4.

## Roadmap status

`docs/src/internal/post-v2.3.0-roadmap.md` carries a new row 11-N for
this wave; the `[Unreleased]` section of `CHANGELOG.md` moves the
four "Known issues" bullets into "Fixed".
