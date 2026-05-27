# Wave 7 — v2.0.1-equivalent Polish

**Status:** complete.
**Branch:** `fix/wave7-polish` (off `sprint/v2.1.0-base`, which is in
turn off `main` at the v2.0.0 release).
**Target release:** v2.0.1 (point-release; bug fixes only, no new
public surface).

## 1. Scope

Wave 7 closes out the items that v2.0.0 left as `#[ignore]`'d
regressions plus any remaining low-priority audit items that could be
addressed without crossing into the v2.1 surface-area work. The wave
deliberately stays inside the data-path crates (`noxu-persist`,
`noxu-bind`, `noxu-collections`, `noxu-db`, `noxu-dbi`, `noxu-cleaner`)
and avoids `noxu-rep`, which is being reworked in Wave 8 as part of
the replication test-harness sprint.

## 2. Closed in Wave 7

### 2.1 `noxu-persist` — read-only reopen of an existing entity store

**Audit reference:** Wave 4-C JE TCK port surfaced the deviation as
`tck_persist_read_only_store_reopens_without_allow_create` (was
`#[ignore]`'d).

**Symptom (before):**

```rust
let store = EntityStore::open(
    &env,
    StoreConfig::new("foo").with_read_only(true), // no allow_create
)?;
let pi = store.get_primary_index::<u64, Item>()?;
// → DatabaseError(DatabaseNotFound("Database 'foo_Item' does not
//   exist and allow_create is false"))
```

JE allows this — `setReadOnly(true)` against a path where the entity
DBs already exist on disk is the canonical "open an existing store
read-only" recipe. Noxu rejected it because `Environment::open_database`
cannot resolve an existing entity DB by name without
`allow_create=true`: recovery does not reconstruct a name→db_id map,
so the env's `name_map` is empty after restart and the only way to
materialise an existing DB is to "create" it (which transparently
transplants the recovered tree by db_id).

**Fix:** in `EntityStore::get_primary_index`, when the store is read-
only we transparently force `allow_create=true` on the underlying
`DatabaseConfig`. `read_only=true` on the same config still rejects
every write at the `Database::put` / `Database::delete` boundary via
`check_writable()` → `NoxuError::ReadOnly`, so the change is observably
equivalent to JE for callers but no longer rejects the legitimate
read-only reopen path. The same pattern was already used by
`ClassCatalog::open` for the catalog DB; this change makes the entity
DB consistent.

**Tests now passing (no longer `#[ignore]`):**

- `tck_persist_read_only_store_reopens_without_allow_create`

**Tests added for additional coverage:**

- `tck_persist_read_only_reopen_get_succeeds_after_close` — exercises
  `pi.get(...)` after a read-only reopen and asserts the persisted
  values come back, plus confirms `None` for a key that was never
  written.
- `tck_persist_read_only_reopen_rejects_put` — asserts that
  `pi.put(...)` against a read-only-reopened store surfaces a typed
  read-only error and does not silently succeed (proves the
  `allow_create=true` we pass under the hood does not bypass the per-DB
  read-only flag).

**Files touched:**

- `crates/noxu-persist/src/entity_store.rs` (15 LoC + extensive doc)
- `crates/noxu-persist/tests/tck_persist_operations.rs` (removed
  `#[ignore]`, added two coverage tests)

## 3. Audit sweep — items checked / status

### 3.1 Cursor Medium findings

Wave 1C closed cursor F17–F22 (per-method docs / typed errors / dead
config fields). Wave 2C-4 closed cursor F10–F18 (key-bytes contract,
`SearchBothRange` exposure, `count()` invariant, idempotent close,
inner-cursor close propagation, leaked-cursor warning narrowing).
Cross-checked: no `#[ignore]`'d cursor tests remain in `crates/noxu-db/
tests/` other than the documented stress sweeps and the one
intentional divergence (`cursor_edge_non_txnal_cursor_no_updates`,
which is `#[ignore]`'d with a clear rationale: Noxu accepts non-txn
cursor updates against a txn DB via auto-commit, an intentional API
contract dropped relative to JE).

**Status:** all tractable cursor Medium findings were closed in
Wave 1C / Wave 2C-4. No new Wave 7 work needed.

### 3.2 Persist findings beyond the read-only bug

Wave 1C removed the unused `DatabaseNamer` and `KeySelector` families
plus four unused `PersistError` variants. Wave 2C-1 added the
`#[derive(Entity)]` proc-macro path. Wave 2C-2 wired schema-evolution
through `EntityStore::open`. Wave 7 closes the read-only reopen
deviation above. Cross-checked: no `#[ignore]`'d persist tests remain
in `crates/noxu-persist/tests/` (`tck_persist_operations.rs` reports
`13 passed; 0 failed; 0 ignored`).

**Status:** persist API deviations closed. Remaining gaps
(`RawStore` / `RawObject` / `RawType` raw-access path) are
deliberately omitted — see deferral list.

### 3.3 `noxu-bind` polish

Cross-checked `crates/noxu-bind/`:

- 324 unit tests, 9 doc-tests, 12 integration tests, 12 TCK tests, 51
  property tests — all green, zero `#[ignore]`'d entries.
- No `TODO` / `FIXME` markers in `src/`.
- The `unimplemented!()` references found (in
  `noxu_bind::serial::serde_binding`) are all in tests / doctests
  asserting deserialisation failure on unknown fields.

**Status:** no actionable items.

### 3.4 `noxu-collections` polish

Cross-checked `crates/noxu-collections/`:

- 4 unit tests, 13 integration tests, 12 TCK tests, plus the wave-2b
  typed-collection suite — all green, zero `#[ignore]`'d entries.
- No `TODO` / `FIXME` markers in `src/`.

Wave 2B added typed `StoredMap<K, V>` / `StoredSet<T>` /
`StoredList<T>`; Wave 2C-3 added `DiskOrderedCursor`. The
`StoredSortedKeySet` / `StoredSortedValueSet` / `StoredSortedEntrySet`
classes are deliberately omitted (covered by iterators on
`StoredMap.keys()` / `.values()` / `.entries()`).

**Status:** no actionable items.

### 3.5 `noxu-cleaner` polish

Cross-checked `crates/noxu-cleaner/`:

- 312 unit tests, 34 integration tests — all green, zero `#[ignore]`'d
  entries.
- No `TODO` / `FIXME` markers in `src/`.

The audit's `testCleanInternalNodes` / `testMultiCleaningBug` /
`testEvictionDuringCheckpoint` MEDIUM gap (full-system cleaner-under-
load scenarios) is tracked separately as a v2.2 backlog item — porting
those tests is not a polish-wave task because they exercise behaviour
the existing `noxu-spec::cleaner_safety` and `noxu-spec::cache_vs_cleaner`
Stateright models already cover at the protocol level.

**Status:** no actionable items in scope; integration-test breadth
deferred.

### 3.6 `noxu-db` `#[ignore]` inventory

| Test | Reason | Action |
|---|---|---|
| `power_loss_sweep::power_loss_sweep_1000` | 1000-iter sweep, 30-60 min | Keep `#[ignore]` (sweep test, run with `--ignored`) |
| `concurrent_commits_stress::concurrent_commits_stress` | 70-130 s stress | Keep `#[ignore]` (stress test) |
| `isolation_test::test_64_thread_concurrent_readers` | 64-thread stress | Keep `#[ignore]` (stress) |
| `isolation_test::test_32r32w_concurrent` | 32 R + 32 W contention | Keep `#[ignore]` (stress) |
| `isolation_test::test_200_thread_disjoint_writers` | 200-thread sanity | Keep `#[ignore]` (stress) |
| `sustained_load_test::*` (3 entries) | sustained load, 10s+ | Keep `#[ignore]` (slow profile only) |
| `je_cursor_edge_test::cursor_edge_non_txnal_cursor_no_updates` | intentional API divergence | Keep `#[ignore]` with clear reason annotation |
| `join_cursor.rs:399` doctest | requires v1.6 sorted-dup secondaries | Keep `#[ignore]` (Decision 1B, future work) |

All remaining `#[ignore]`'d entries in scope are either intentional
divergences with clear rationale or cost-driven (slow stress sweeps).

**Status:** no actionable items.

## 4. Deferred to v2.1.x / v2.2 backlog

The following Medium-severity findings from
`docs/src/internal/je-port-audit-2026-05-overview.md` are NOT closed
in Wave 7 and are tracked for future waves:

| # | Finding | Defer to | Reason |
|---|---|---|---|
| 1 | `noxu-rep` Medium/High items (claim audit + 23 missing test classes) | Wave 8 | Wave 8 owns the replication test-harness sprint; Wave 7 explicitly avoids `noxu-rep`. |
| 2 | `je.recovery` SR-numbered regression bugs (16 tests) | v2.1 / v2.2 | Test-port effort, ~1 engineer-month; not a "polish" wave task. |
| 3 | `je.cleaner` full-system-load regression tests (3 tests) | v2.2 | Protocol-level coverage already in `noxu-spec`; test breadth is a separate sprint. |
| 4 | `je.evictor` integration tests (`EvictionThreadPoolTest`, `OffHeapCacheTest`, `SharedCacheTest`) | v2.2 | Same as cleaner — separate test-port sprint. |
| 5 | `Cursor::dup` / `skip_next` / `skip_prev` / `set_range_constraint` | v2.1 | Adds public surface area, not a polish item. |
| 6 | `Cursor::get_database` / `get_config` accessors | v2.1 | Requires `Cursor` to hold an `Arc<Database>` reference, non-trivial refactor. |
| 7 | `Database::get_environment()` accessor | v2.1 | Same as above. |
| 8 | `Database::populate_secondaries` (multi-DB form) | v2.1 | Adds public surface area. |
| 9 | `Database::compare_keys` / `compare_duplicates` (public methods) | v2.1 | Internal-only in Noxu by Wave 1C decision; promoting to public requires API design. |
| 10 | `Environment::sync` / `flush_log` / `evict_memory` / `compress` / `print_startup_info` / `clean_log_file` | v2.1 / v2.2 | Documented as MISSING in the API map; require engine-side plumbing to expose. |
| 11 | `RawStore` / `RawObject` / `RawType` (DPL raw-access path) | v2.x or never | Bytecode-enhancer pattern; the trait-based Noxu DPL replaces this. |
| 12 | `XAEnvironment` integration (XA via `Environment`) | v2.1 | The `noxu-xa` crate is freestanding; surfacing it through `Environment` is a Wave-equivalent task. |
| 13 | `Monitor` / `Arbiter` / `External` rep-node types (operational impl) | Wave 8 / future | Replication scope; Wave 8 territory. |

The deferral list above is exhaustive for the Wave 7 scope. Items 5–10
are the bulk of the v2.1 surface-area work tracked in the v2.1.0 sprint.

## 5. Quality gates

| Gate | Result |
|---|---|
| `cargo fmt --all -- --check` | passes |
| `cargo clippy --workspace --all-targets -- -D warnings` | passes |
| `cargo test --workspace --no-fail-fast` | 146 test-result blocks all OK; 0 failures |
| `make docs-check` | passes |

## 6. Cross-references

- `docs/src/internal/je-port-audit-2026-05-overview.md` — top-level
  audit that Wave 7 sweeps against.
- `docs/src/internal/je-port-audit-2026-05-api-map.md` — per-class API
  map; deferral table in §4 above is keyed against this document.
- `docs/src/internal/wave1c-audit-low-info-cleanup-2026-05.md` — Wave
  1C closure of cursor F17–F22, secondary-database surface, etc.
- `docs/src/internal/wave-2c-2-dpl-evolution.md` — Wave 2C-2 closure of
  the DPL evolution open-path that the read-only reopen fix builds on.
- `docs/src/internal/je-tck-port-2026-05-overview.md` — JE TCK port
  status at v2.0.0 release.

---

*Prepared 2026-05-27 against `fix/wave7-polish` off
`sprint/v2.1.0-base` at `74b739b` (v2.0.0).*
