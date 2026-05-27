# Wave 2B — Collections typed API and txn threading

> **Status.** Branch `fix/wave2b-collections-typed-api`, base `main`
> (v1.5.1).  Four commits.  Closes the May 2026 collections / bind
> audit findings #1, #3 / #4, #5, and #11 / #12 — every High finding
> the Sprint 3C scope-down deferred to v1.6.

## 1. Why this wave exists

Sprint 3C
([`docs/src/internal/sprint-3-collections-restriction.md`](sprint-3-collections-restriction.md))
shipped v1.5 with an honest auto-commit-only `&[u8]`-keyed Stored*
surface and explicitly deferred four High audit findings to v1.6:

- **#1** — Stored* operations hard-code `txn = None`.  `Option<&Transaction>`
  was never plumbed through.
- **#3 / #4** — `TransactionRunner` supplies a `&Transaction` no Stored*
  method accepts.  The runner stays useful for raw `Database`/`Cursor`
  sequences but its rustdoc-vs-source mismatch is a known wart.
- **#5** — `StoredList::remove` rustdoc claimed compaction; the v1.5
  body was a single-key delete that left a hole.
- **#11 / #12** — The published mdBook described typed
  `StoredMap<K, V>` / `StoredSet<K>` / `StoredList<V>` shapes; the
  source never matched.

Wave 2B is the implementation of the v1.6 plan that the v1.5 decisions
doc
([`docs/src/internal/v1.5-decisions-2026-05.md`](v1.5-decisions-2026-05.md))
sketched: typed Stored* surface + `Option<&Transaction>` threading +
`TransactionRunner` redesign + `StoredList::remove` compaction, all
landing together as a single SemVer break.

## 2. Findings closed

| Finding | Subject | Disposition |
|---|---|---|
| #1 | Stored* ops hard-code `txn = None` | **CLOSED** by adding `txn: Option<&Transaction>` as the leading argument on every Stored* method.  `None` runs auto-commit; `Some(&t)` participates in `t`. |
| #3 | `TransactionRunner` supplies a `&Transaction` no Stored* method accepts | **CLOSED** as a side-effect of #1.  The runner-supplied `&Transaction` now drives any Stored* method — see `tests/wave2b_tests.rs::runner_txn_drives_storedmap_writes`. |
| #4 | Same as #3 from the Stored* side | **CLOSED** with #3. |
| #5 | `StoredList::remove` does not compact | **CLOSED** by implementing shift-down compaction.  Removing index `i` reads-then-writes every `j > i` down to `j - 1`, deletes the source, and decrements `next_index`.  Whole compaction issued under the supplied txn. |
| #6 | `StoredList::next_index` resets on reopen | Already closed in v1.5 (Sprint 3C added `StoredList::open`).  Re-validated under the typed API. |
| #11 | No typed `StoredMap<K, V>` surface | **CLOSED** by `StoredMap<K, V, KB, VB>` parameterised on `EntryBinding<K>` and `EntryBinding<V>`. |
| #12 | No typed `StoredSet<K>` / `StoredList<V>` surface | **CLOSED** by `StoredKeySet<K, KB>`, `StoredValueSet<V, VB>`, and `StoredList<V, VB>`. |

## 3. Decision matrix — what we picked, why

### 3.1 Step 1 — typed surface shape

| Option | Picked | Rationale |
|---|---|---|
| (a) `StoredMap<K, V>` with associated-type bindings | No | Works for one binding per type at most; Rust's coherence rules make multiple `EntryBinding<T>` impls per `T` (e.g. `IntBinding` vs `PackedIntBinding` for `i32`) painful. |
| (b) `StoredMap<K, V, KB, VB>` parameterised by binding types | **Yes** | Lets users pick the encoding (e.g. `IntBinding` vs `SortedPackedIntBinding`) at construction time.  Bindings are zero-sized in the common case so the cost is just type parameters, not runtime memory. |

### 3.2 Step 2 — txn threading shape

| Option | Picked | Rationale |
|---|---|---|
| (a) `txn: Option<&Transaction>` as the leading arg | **Yes** | Matches BDB-JE's `StoredMap` shape, matches `noxu_db::Database` / `SecondaryDatabase`, allows `None` for auto-commit without loading another method onto every type. |
| (b) Two parallel methods (`get_auto` / `get_with_txn`) | No | API bloat; users have to remember which to call. |
| (c) Separate `TxnView<'t, T>` newtype that wraps `T` plus a txn | No | Pretty but obscures the BDB-JE parallel; would have required a parallel set of constructors. |

### 3.3 Step 3 — `StoredList::remove` compaction strategy

| Option | Picked | Rationale |
|---|---|---|
| (a) Shift-down (read src, write at dst, delete src) | **Yes** | Matches BDB-JE `StoredList.remove(int index)` and `Vec::remove`.  Easy to reason about; works under any txn the user passes. |
| (b) Maintain a "first" index in addition to "next" so head removal is O(1) | No | Would have made head/tail removal asymmetric and required a second persistent counter.  Wave 2B optimises for *correctness and shape parity with BDB-JE*; an O(1) deque is a follow-up. |

### 3.4 Step 4 — TransactionRunner backoff

| Option | Picked | Rationale |
|---|---|---|
| (a) Constant retry sleep (the v1.5 stand-in: `continue` immediately) | No | Hot-loops both lockers under contention. |
| (b) Jittered exponential backoff with caps | **Yes** | Standard pattern; minimal new code; testable in isolation via `RetryConfig::backoff_for(attempt, nanos)` (a pure function). |

Defaults: 10 retries, 10 ms base, 1 s ceiling, ±25% jitter.  All
configurable via builder methods.  Retry triggers on every
`NoxuError::is_retryable()` variant (deadlock, lock conflict, lock
timeout, lock-not-available, transaction timeout, lock preempted).

### 3.5 Step 5 — iterator design

The v1.5 iterator held a `Vec<Vec<u8>>` snapshot of keys taken from
the in-process `BTreeSet` index, then lazily fetched values during
`next()`.  Wave 2B drops the in-process index entirely (every Stored*
view is now stateless), so the iterator design changed:

| Option | Picked | Rationale |
|---|---|---|
| (a) Lazy: hold a live cursor across `next()` calls | No | Needs a cursor lifetime parameter on the iterator, which propagates into every call site (`for entry in map.iter(None)? { ... }` becomes painful). |
| (b) Eager: walk a cursor at iter() construction time, materialise `Vec<(K, V)>`, yield from there | **Yes** | Matches BDB-JE's "snapshot at iter() time" contract; no cursor lifetimes; trivial `Iterator` / `ExactSizeIterator` / `FusedIterator` impls; iterator is `Send` for free. |

Memory cost: O(N) per call to `iter()`.  This is the same trade-off
BDB-JE makes; users iterating a multi-GB database should use a raw
`Cursor` (which streams) instead of `StoredMap::iter`.

### 3.6 Step 6 — out-of-scope `noxu-dbi` cursor bug

While porting the iterators we found a real bug in
`noxu-dbi::cursor_impl::search` for `SearchMode::SetRange`: it sets
`current_index = 0` after a successful match, so a subsequent
`Get::Next` walks from the start of the BIN instead of advancing
from the matched position.  The result is that the cursor revisits
records before the start key.

The bug is real (`Get::SearchGte` then `Get::Next` is broken on any
multi-key BIN) and lives in `noxu-dbi`, which is out of scope for
this wave.  The Wave 2B workaround in `internal::scan_records` is to
walk from the appropriate endpoint (`First` or `Last`) and skip
records that fall outside the requested range; this costs an O(K)
prefix scan instead of landing directly, but is correct under every
cursor mode the engine supports today.  A note has been left in the
helper rustdoc so the workaround can be removed once the underlying
bug is fixed.

## 4. Files touched

```text
crates/noxu-collections/
├── src/
│   ├── lib.rs                  rewrite: v1.6 crate-level rustdoc
│   ├── internal.rs             new: scan_records, encode/decode helpers
│   ├── stored_iterator.rs      rewrite: generic StoredIterator<T>
│   ├── stored_map.rs           rewrite: typed StoredMap<K, V, KB, VB>
│   ├── stored_sorted_map.rs    rewrite: typed StoredSortedMap<K, V, KB, VB>
│   ├── stored_key_set.rs       rewrite: typed StoredKeySet<K, KB>
│   ├── stored_value_set.rs     rewrite: typed StoredValueSet<V, VB>
│   ├── stored_list.rs          rewrite: typed StoredList<V, VB> + compaction
│   └── transaction_runner.rs   rewrite: jittered backoff + drives Stored*
├── tests/
│   ├── collection_tests.rs     port to typed API; drop register_keys cases
│   ├── prop_tests.rs           port to typed API; +1 round-trip property
│   ├── wave2b_tests.rs         new: 13 regression tests for findings closed
│   └── sprint3c_tests.rs       deleted (superseded)
docs/src/
├── collections/
│   ├── README.md               rewrite: v1.6 capability summary
│   ├── stored-map.md           rewrite: typed surface + migration
│   ├── stored-set.md           rewrite: typed surface + migration
│   └── stored-list.md          rewrite: typed surface + compaction
├── getting-started/
│   ├── bindings.md             += "Using bindings with Stored* views"
│   └── migrating.md            += "Wave 2B" section (v1.5 → v1.6)
├── introduction.md             update v1.6 capability matrix rows
└── internal/
    └── wave-2b-collections-typed.md   this document
examples/
└── collections.rs              port to typed StoredMap<String, String>
```

Out of scope (per the wave plan): `noxu-db`, `noxu-persist`, `noxu-xa`,
`noxu-rep`.  None were touched.

## 5. Test inventory

| Suite | Tests | Coverage |
|---|---|---|
| `noxu-collections` lib (`#[cfg(test)] mod tests` in each module) | 63 | Per-type unit tests for typed put/get/remove/iter/clear/read-only/user-txn-commit/user-txn-abort/binding-round-trip |
| `tests/collection_tests.rs` | 51 | BDB-JE TCK ports (`CollectionTest`, `ForeignKeyTest`, `NullValueTest`, `TestSR15721`) under the typed API with `ByteArrayBinding` |
| `tests/prop_tests.rs` | 4 | Proptest invariants: put/get HashMap parity, remove + contains_key, len matches unique key count, iter round-trip |
| `tests/wave2b_tests.rs` | 13 | Regression tests pinning findings #1, #3 / #4, #5, #6, #11 / #12 |
| `noxu-collections` doctests | 3 | Crate-level `#[ignore]` examples (require an environment) |
| **Total for the crate** | **131 + 3 ignored** | |

Workspace-wide `cargo test --workspace --no-fail-fast` and
`cargo clippy --workspace --all-targets -- -D warnings` both pass
clean.

## 6. Breaking changes (for the migrating chapter)

See
[`docs/src/getting-started/migrating.md#wave-2b--collections-typed-api-and-txn-threading-v15--v16`](../getting-started/migrating.md#wave-2b--collections-typed-api-and-txn-threading-v15--v16)
for the full before/after.  Summary:

1. `StoredMap<'db>` → `StoredMap<'db, K, V, KB, VB>` (and same shape
   for every other Stored* type).
2. Every Stored* method takes `txn: Option<&Transaction>` as the
   leading argument.
3. `register_key` / `register_keys` / `known_keys` are deleted; `iter`
   walks the database directly.
4. `StoredMap::len` returns `usize` instead of `u64`.
5. `StoredList::remove` compacts (shift-down) instead of leaving a hole.
6. `TransactionRunner::run`'s closure signature relaxed from `Fn` to
   `FnMut`; backoff retry triggers on every retryable error.
7. Runtime addition: `StoredKeySet::add(txn, &key) -> Result<bool>`
   matching `java.util.Set.add` (`true` when newly inserted).

## 7. Out of scope / follow-ups

- Fix `noxu-dbi::cursor_impl::search` SetRange `current_index` bug so
  `iter_from` can land directly via `SearchGte` instead of walking
  from `Get::First`.
- O(1) head removal via a "first" counter on `StoredList` (alternative
  to shift-down for FIFO workloads).
- Lazy iterator that streams from a live cursor, for users who need
  to iterate a multi-GB Stored* view.
- Schema evolution for `SerdeBinding` (versioned struct shapes).  The
  2-byte version prefix added in Sprint 3C catches inter-format drift
  but does not handle intra-format struct-shape changes.
