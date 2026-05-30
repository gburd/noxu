# Secondary / Join public-API audit — 2026-05

**Auditor:** read-only audit by automated agent
**Date:** 2026-05-25
**Branch:** `fix/cursor-search-gte-cross-bin-walk`
**Scope:** Public `SecondaryDatabase`, `SecondaryCursor`, `JoinCursor`,
`SecondaryConfig`, `JoinConfig` and the related key-creator / nullifier
traits in `noxu-db`, cross-referenced against the published mdBook
chapters and the Berkeley DB / BDB-JE reference semantics.

> **Trigger:** the prior cursor audit
> (`api-audit-2026-05-cursor.md`) flagged the secondary surface as
> out-of-scope. This audit picks up that thread and looks for the same
> shape of issue plus the BDB-specific contracts (associate, foreign
> keys, equality joins).

---

## 1. Scope

### Audited

* `crates/noxu-db/src/secondary_config.rs`
  (`SecondaryConfig`, `SecondaryKeyCreator`, `SecondaryMultiKeyCreator`,
  `ForeignKeyDeleteAction`, `ForeignKeyNullifier`,
  `ForeignMultiKeyNullifier`).
* `crates/noxu-db/src/secondary_database.rs`
  (`SecondaryDatabase::{open, get, delete, open_cursor,
  update_secondary, delete_all_for_primary, populate_*,
  start/end/is_incremental_population}`).
* `crates/noxu-db/src/secondary_cursor.rs`
  (`SecondaryCursor::{put, delete, get_current, get_first, get_last,
  get_next, get_prev, get_search_key, get_search_key_range, close,
  is_valid}` plus the `pub(crate)` join helpers
  `get_current_primary_key_only`, `get_current_sec_key_bytes`,
  `count_estimate`, `get_next_dup`, `has_candidate_primary_key`).
* `crates/noxu-db/src/join_config.rs` (`JoinConfig`).
* `crates/noxu-db/src/join_cursor.rs`
  (`JoinCursor::{new, get_next, get_next_key, close, get_database,
  get_config, next_matching_candidate}`).
* `crates/noxu-db/src/database.rs::join` (the public entry point that
  produces `JoinCursor` handles).
* `docs/src/getting-started/secondary-databases.md`.
* `docs/src/transactions/secondary-with-txn.md`.
* `docs/src/transactions/concurrency.md` (the `SecondaryDatabase`
  thread-safety claim).
* The `examples/secondary.rs` example.
* In-module `#[cfg(test)]` blocks plus the integration tests in
  `crates/noxu-db/tests/integration_test.rs` whose names start with
  `sec_`.
* The DPL-layer `noxu-persist::secondary_index` tests
  (`crates/noxu-persist/tests/noxu_persist_tests.rs`) — only as a usage
  consistency check.
* The `ForeignKeyTest`-equivalent tests in
  `crates/noxu-collections/tests/collection_tests.rs:880-1000`.

### Explicitly **not** exercised

* No code was modified, no tests were run, no docs were rebuilt.
* No reference archives (`_/je/`, `_/nosql/`) were consulted; the
  comparison to BDB/BDB-JE semantics is from documented BDB API
  contracts, not from a side-by-side line-level diff against
  `SecondaryDatabase.java`.
* Reproducer sketches are written but not executed.
* `noxu-dbi` has **no** secondary or join modules; the entire
  implementation lives in `noxu-db` on top of the regular `Database`.
  The phrase "noxu-dbi/src/secondary_*.rs / join_*.rs" in the audit
  request does not correspond to anything in the tree
  (`crates/noxu-dbi/src/` listing confirms this).
* `crates/noxu-db/src/foreign_key_delete_action.rs` does not exist;
  `ForeignKeyDeleteAction` lives inside `secondary_config.rs:64`.
* I did not validate transactional behaviour by running a real
  transaction against a secondary; all findings about txn handling are
  by code reading.

---

## 2. Methodology

1. Enumerated every `pub` and `pub(crate)` method on each of the five
   target types and wrote them down in a worksheet.
2. Read each method's source and its rustdoc.
3. Cross-checked the rustdoc claims against the implementation.
4. Cross-checked the mdBook chapters against the actual public API
   surface.
5. For each BDB contract called out in the request — null/empty key
   creator skip, multi-key creator, atomic primary+secondary update,
   foreign-key delete actions, sorted-dup join requirement, dangling
   primary, secondary close ordering, panics on user input — searched
   the tree for the corresponding implementation, then judged whether
   it is honored, partially honored, or missing.
6. For each finding, recorded a specific file:line citation.

---

## 3. Findings table

Severity legend:
**Critical** = data loss, silent corruption, or major contract
violation users are likely to hit.
**High**     = correctness gap that violates documented behavior or
breaks an explicit BDB contract but is detectable.
**Medium**   = API ergonomics, missing surface, or stale docs that will
mislead users.
**Low**      = polish, hardening, or future-proofing.

| # | Severity | Area | Issue (one line) | Citation |
|---|----------|------|------------------|----------|
| F1 | Critical | foreign keys | `ForeignKeyDeleteAction` (Abort/Cascade/Nullify), `foreign_key_database`, `ForeignKeyNullifier`, `ForeignMultiKeyNullifier` are stored on `SecondaryConfig` but never consulted at runtime. No FK constraint is enforced; no Cascade/Nullify/Abort action is performed. | `crates/noxu-db/src/secondary_config.rs:64-110,177-196`; entire absence of any read site outside `secondary_config.rs` confirmed by `rg foreign_key_(database\|delete_action\|nullifier)` |
| F2 | Critical | associate / atomicity | `Database::put` and `Database::delete` are not aware of secondaries. The user must remember to call `secondary.update_secondary(...)` after every primary write or the index silently goes out of sync. The doc comment "On every primary `put` the secondary is updated via `update_secondary`" and the reference to `Database::put_and_update_secondaries` are aspirational — no such hook exists. | `crates/noxu-db/src/secondary_database.rs:15,303`; `crates/noxu-db/src/database.rs:396-489` (`put`) and `586-636` (`delete`) — no secondary callbacks |
| F3 | Critical | sorted-dup secondaries | The inner index is written with `Put::Overwrite`, so a second primary that maps to the *same* secondary key silently displaces the first. Multi-primary→one-secondary-key indexes (the canonical BDB use-case) lose data. The `examples/secondary.rs` example demonstrates this: three "Engineering" employees are inserted, only one is reachable via `secondary.get(b"Engineering")`. | `crates/noxu-db/src/secondary_database.rs:466-485` (`insert_sec_key`); demonstrated in `examples/secondary.rs:108-145,180-200`; the algorithm-doc comment in `join_cursor.rs:11-17` explicitly acknowledges "Noxu's current one-to-one secondary model" |
| F4 | High | transactions | `SecondaryDatabase::open_cursor` accepts `_txn: Option<&Transaction>` and `_config: Option<&CursorConfig>` but discards both — every secondary cursor is auto-commit no matter what the caller passes. `update_secondary` does not even take a `Transaction` parameter. Internal helpers `insert_sec_key`, `delete_sec_key`, `make_inner_cursor`, `populate_from_primary_scan` open auto-commit cursors. | `crates/noxu-db/src/secondary_database.rs:264-273` (`_txn` ignored), `296-376` (`update_secondary` no txn), `462-485` (`insert_sec_key` no txn), `492-525` (`delete_sec_key` no txn), `531-533` (`make_inner_cursor` no txn) |
| F5 | High | transactions | `SecondaryDatabase::delete(txn, key)` only forwards `txn` to `primary.delete(...)` (line 240). The cursor used to walk duplicates and the `delete_all_for_primary` cleanup both run auto-commit. If the txn aborts, secondary cleanup that already happened is **not** rolled back, leaving the index inconsistent with the primary. | `crates/noxu-db/src/secondary_database.rs:206-256`, especially `218` (`open_cursor_internal()` discards txn), `234-236` (`delete_all_for_primary` no txn), `240` (only `primary.delete` is in the txn) |
| F6 | High | docs vs implementation | `docs/src/transactions/secondary-with-txn.md` shows `env.open_secondary(None, "name", None, primary, &sec_config)` and `SecondaryConfig::new().with_transactional(true)`. **Neither exists.** The actual constructor is `SecondaryDatabase::open(primary, sec_db, sec_config)` and `SecondaryConfig` has no `with_transactional` builder. The docs also claim "primary and secondary indices are updated atomically within the same transaction" — this is contradicted by F4. | `docs/src/transactions/secondary-with-txn.md:25-37`; absence of `open_secondary` confirmed by `rg "fn open_secondary"` returning only the test helper at `secondary_database.rs:644`; absence of `with_transactional` confirmed against `secondary_config.rs:237-348` |
| F7 | High | join cursor | `JoinCursor` is documented in rustdoc but has zero coverage in mdBook. `docs/src/SUMMARY.md` does not list it; `JoinConfig` is similarly absent. The crate guide mentions the type name (`docs/src/maintainer/crate-guide.md:146`) and `project-history.md:35` records it as Phase-37 work, but no user-facing chapter exists. | `docs/src/SUMMARY.md` (no entry); `rg "JoinCursor\|join cursor\|equality join" docs/src/` returns only the maintainer/history lines |
| F8 | Medium | optimisation flags inert | `SecondaryConfig::immutable_secondary_key`, `extract_from_primary_key_only`, and the helper `update_may_change_secondary()` are config sinks. `update_may_change_secondary` is defined and tested but never called from `update_secondary` or anywhere else; both flags are accepted by the builder but ignored at runtime — every primary update unconditionally re-runs the key creator and re-reads old data. | `crates/noxu-db/src/secondary_config.rs:191-196,287-301,405-412`; `rg update_may_change_secondary` shows only the definition site and its self-tests |
| F9 | Medium | builder sink | `SecondaryConfig::with_sorted_duplicates(sd)` writes to `self.base.sorted_duplicates` but `base` is *never* used to open the inner database — the user supplies an already-opened `Database`. The flag is silently ignored. There is no `with_transactional` proxy for the same reason. | `crates/noxu-db/src/secondary_config.rs:246-250`; `crates/noxu-db/src/secondary_database.rs:81-104` (`open` ignores `config.base`) |
| F10 | Medium | API gap | `SecondaryDatabase` has no `count`, `exists`, `truncate`, `get_search_both`, no key-only `get_search_key` overload (skipping the primary read), no way to disable the integrity exception in favour of a "skip dangling" mode (BDB's `READ_UNCOMMITTED` semantics). | `crates/noxu-db/src/secondary_database.rs` (full file); BDB `SecondaryDatabase` exposes all of the above |
| F11 | Medium | panic on user input | `SecondaryCursor::new` calls `.expect("Failed to open inner secondary cursor")`. The public `SecondaryDatabase::open_cursor` returns `Result<SecondaryCursor>` but a failure to open the inner cursor will panic instead of surfacing as `Err`. | `crates/noxu-db/src/secondary_cursor.rs:52-58`; called from `secondary_database.rs:267,503` |
| F12 | Medium | join semantics | BDB requires every secondary fed to a join cursor to be sorted-dup so that `get_next_dup` can iterate the candidate set. Because of F3 the inner index is one-to-one, so `SecondaryCursor::get_next_dup` always returns `NotFound` after the first hit (`secondary_cursor.rs:341-364`). Consequently, `JoinCursor` correctly intersects single-element candidate sets and degenerates: it can confirm that one specific primary key is present in N indexes, but cannot enumerate true sorted-dup intersections. The rustdoc admits this (`join_cursor.rs:11-17,109-115`) but the limitation is invisible to users reading the published docs. | `crates/noxu-db/src/secondary_cursor.rs:341-364`; `crates/noxu-db/src/join_cursor.rs:11-17,109-115` |
| F13 | Medium | dangling primary | `SecondaryDatabase::get`, `SecondaryCursor::get_with_mode`, `get_search_key`, `get_search_key_range` all raise `NoxuError::SecondaryIntegrityException` on dangling primary references. There is no per-call lock-mode that suppresses the exception (BDB allows the cursor to skip such records under `READ_UNCOMMITTED`). The cursor is left positioned but the caller cannot continue without surfacing the error. | `crates/noxu-db/src/secondary_database.rs:177-185`; `crates/noxu-db/src/secondary_cursor.rs:233-243,272-280,439-450` |
| F14 | Medium | range scan | `SecondaryCursor::get_search_key_range` performs a `Get::SearchGte`, then *re-reads* the current key with a follow-up `Get::Current` call to update the caller's `search_key` slot (`secondary_cursor.rs:262-267`). Two cursor calls is fragile and depends on `Cursor::get` writing the key back on `Current`; if that contract changes, the caller sees a stale `search_key`. The cursor audit (`api-audit-2026-05-cursor.md`) recently fixed `SearchGte` cross-BIN walk — this method is on the same code path. | `crates/noxu-db/src/secondary_cursor.rs:248-285` |
| F15 | Medium | foreign-key tests are a workaround | The "ForeignKeyTest equivalents" in `crates/noxu-collections/tests/collection_tests.rs:883-995` explicitly state: "The Rust crate has no full SecondaryDatabase foreign-key enforcement in StoredMap (that lives in noxu-db), so we test the logical invariants using two cooperating StoredMap instances and manual constraint checks". This corroborates F1 — there is *no* engine-side enforcement; the tests prove only that the application can simulate the semantics manually. | `crates/noxu-collections/tests/collection_tests.rs:883-895` |
| F16 | Low | unsafe Send | `SecondaryConfig.foreign_key_database` is stored as `Option<*const Database>` with a hand-rolled `unsafe impl Send for SecondaryConfig`. If the user drops the foreign `Database` before the secondary, dereferencing this pointer is undefined behaviour. This is moot today because the field is never read (F1), but should not survive into the FK implementation. | `crates/noxu-db/src/secondary_config.rs:174-202` |
| F17 | Low | API hygiene | All fields on `SecondaryConfig` are `pub` (`key_creator`, `multi_key_creator`, `foreign_*`, `immutable_secondary_key`, `extract_from_primary_key_only`). `validate()` is `pub(crate)` and only runs from `SecondaryDatabase::open`. Users assigning fields directly via struct literal — which the unit tests do (`secondary_config.rs:485-490,690-694`) — bypass validation. Compare to BDB-JE's setter-only API. | `crates/noxu-db/src/secondary_config.rs:160-198,355-405` |
| F18 | Low | `JoinConfig` minimal | `JoinConfig` has only `no_sort`. BDB-JE's `JoinConfig` also exposes `setCacheMode`, and BDB C exposes `DB_JOIN_NOSORT` / `joinAbsolute`. Not strictly required, but the type is documented as "all defaults" without listing which ones are missing. | `crates/noxu-db/src/join_config.rs:14-43` |
| F19 | Low | close ordering not enforced | `Database` has no awareness of associated secondaries, so closing the primary while a `SecondaryDatabase` handle is alive does not raise an error — it relies entirely on `Arc<Mutex<Database>>` reference counting to keep the primary alive. The mdBook docs state "The secondary must be closed before the primary database and before the environment" (`getting-started/secondary-databases.md:106`), but nothing in the code enforces this; an `Arc::clone` of the primary held inside the secondary defers `Database::Drop` until after `secondary.close()` runs, so close-while-secondary-open silently succeeds. | `crates/noxu-db/src/secondary_database.rs:62-64,540-546`; no `track_secondaries` field on `Database` |
| F20 | Low | counter under-reporting | `noxu-dbi::ThroughputStats` defines secondary-specific counters (`throughput_stats.rs:33-48`), but `SecondaryDatabase` never increments them. The throughput dashboard for secondary index ops will read zero. | `crates/noxu-dbi/src/throughput_stats.rs:33-48`; no `n_sec_*` increment in `secondary_database.rs` or `secondary_cursor.rs` |

---

## 4. Detailed findings

### F1 — Foreign-key constraints completely unimplemented (Critical)

`SecondaryConfig` stores everything BDB needs for foreign-key
enforcement:

* `foreign_key_database: Option<*const Database>`
  (`secondary_config.rs:177`)
* `foreign_key_delete_action: ForeignKeyDeleteAction` with `Abort`,
  `Cascade`, `Nullify` variants (`secondary_config.rs:64-77,180`)
* `foreign_key_nullifier: Option<Box<dyn ForeignKeyNullifier>>`
  (`secondary_config.rs:183`)
* `foreign_multi_key_nullifier: Option<Box<dyn
  ForeignMultiKeyNullifier>>` (`secondary_config.rs:186`)

The corresponding `with_*` builder setters exist (lines 310-348) and
`validate()` (lines 355-405) checks consistency between the fields
(e.g. `Nullify` requires a nullifier; the two nullifiers are mutually
exclusive; `ForeignKeyNullifier` cannot pair with a multi-key creator).

**No code outside `secondary_config.rs` ever reads these fields.** A
repo-wide search:

```text
rg 'foreign_key_(database|delete_action|nullifier)|nullify_foreign_key'
```

returns only hits inside `secondary_config.rs` (definitions, builders,
validation, and self-tests). In particular:

* No call to `nullify_foreign_key` exists in `secondary_database.rs`.
* No code reads `foreign_key_database` to resolve constraints.
* `Database::delete` does not consult any registry of secondaries
  pointing at it; nothing implements the FK delete cascade.

The `noxu-collections` test suite acknowledges this gap directly
(`collection_tests.rs:884-892`):

```text
// The Rust crate has no full SecondaryDatabase foreign-key enforcement
// in StoredMap (that lives in noxu-db), so we test the logical
// invariants using two cooperating StoredMap instances and manual
// constraint checks, mirroring the DELETE_ABORT / DELETE_NULLIFY /
// DELETE_CASCADE semantics described in ForeignKeyTest.
```

The note pins the gap on `noxu-db`. The `noxu-db` crate only has
`SecondaryConfig::validate()` (a no-op for behaviour), no enforcement.

**Recommended action:** either (a) document this prominently as
"unimplemented; will be a `NoxuError::Unsupported` on `SecondaryDatabase::open`
when foreign_key_* fields are set" and reject configurations that
specify them, or (b) implement the four runtime hooks: at every primary
insert, validate the foreign key exists in `foreign_key_database`; at
every foreign-DB delete, walk back-references and Abort / Cascade /
Nullify per the configured action.

### F2 — No `associate()` analogue (Critical)

BDB's contract is that `Database::associate(secondary)` registers a
secondary so that every subsequent `put` / `delete` on the primary
automatically maintains the secondary index. Noxu DB has **no such
hook**. `Database::put` (`database.rs:396-489`) and `Database::delete`
(`database.rs:586-636`) write only to the primary's tree. The user is
expected to call `SecondaryDatabase::update_secondary(pri_key, old,
new)` themselves after every primary write.

The rustdoc on `secondary_database.rs:15` says:

```text
//! On every primary `put` the secondary is updated via `update_secondary`.
```

This is not how the code behaves — it is what the integration layer
*would* do if it existed. The same file, `secondary_database.rs:303-305`:

```text
/// Called from `Database::put_and_update_secondaries` (see database.rs
/// integration layer) and from application code that manages secondary
/// index updates manually
```

`Database::put_and_update_secondaries` does not exist (`rg
put_and_update_secondaries` returns only this comment).

The integration tests, the unit tests inside `secondary_database.rs`,
the example (`examples/secondary.rs:108-145`), and the mdBook chapter
(`getting-started/secondary-databases.md:104-130`) all manually call
`update_secondary` after every primary `put`. The chapter is at least
honest about this — it documents the pattern as a user obligation. The
rustdoc and the stale `put_and_update_secondaries` reference are not.

**Recommended action:** either implement an `associate()`-style hook on
`Database` (probably via a `Vec<Weak<SecondaryDatabase>>` plus an
internal callback list invoked from `Database::put` / `delete` in the
same txn) or update the rustdoc on `secondary_database.rs:15` and `:303`
to plainly say "the application is responsible for calling
`update_secondary` after every primary write" and remove the dangling
`put_and_update_secondaries` reference.

### F3 — Sorted-dup secondaries silently lose data (Critical)

The canonical BDB use-case for a secondary index is many-primary→one-
secondary-key, e.g. an "employees by department" index where many
employee records share a department. BDB requires the underlying
secondary database to be `DB_DUPSORT` so that all primary keys for a
given secondary key are stored as duplicates.

`SecondaryDatabase::insert_sec_key` (`secondary_database.rs:466-485`)
writes the secondary entry with `Put::Overwrite`:

```rust
cursor
    .put(sec_key, pri_key, crate::put::Put::Overwrite)
```

This means a second primary that produces the same secondary key
**replaces** the first instead of being added as a duplicate. The
comment immediately above the `put` calls this out as "safe for the
fully-populated path since insert_sec_key is only called when the key
did not previously exist" — but this assumption is wrong whenever two
distinct primaries produce the same secondary key.

The `JoinCursor` rustdoc is honest about the consequence
(`join_cursor.rs:11-17`):

```text
//! In Noxu's current one-to-one secondary model there is at most one
//! candidate per secondary key position.
```

`examples/secondary.rs` demonstrates the bug directly. It inserts
Alice (Engineering), Carol (Engineering), Eve (Engineering), then
queries `secondary.get(b"Engineering", ...)`. Only the last-inserted
Engineering employee is returned. The example printout looks correct
because the program only prints whatever `get` returns — but the cursor
scan shows there is only one entry per department, not three.

`SecondaryConfig::with_sorted_duplicates(true)` exists
(`secondary_config.rs:246`) but is a no-op (F9). The integration test
`sec_multi_key_creator_multiple_keys_per_record`
(`integration_test.rs:2134-2196`) tests the *opposite* direction (one
primary → multiple secondary keys) and so does not reveal F3.

**Recommended action:** either implement true sorted-dup secondary
storage (the inner `Database` must be opened with
`sorted_duplicates=true` and `insert_sec_key` must use a put mode that
appends a new dup, e.g. `NoOverwrite` keyed on `(sec_key, pri_key)`),
or document the one-to-one limitation prominently in
`getting-started/secondary-databases.md` and reject `key_creator`
configurations that can produce collisions (which is undecidable in
general, so the only honest option is the first one).

### F4 — Transaction context is silently dropped (High)

`SecondaryDatabase::open_cursor`
(`secondary_database.rs:264-273`) accepts `_txn: Option<&Transaction>`
and `_config: Option<&CursorConfig>` and discards both. The leading
underscores make this an explicit choice in the source. The returned
`SecondaryCursor` is built by `SecondaryCursor::new`, which calls
`secondary_db.inner_db().open_cursor(None, None)`
(`secondary_cursor.rs:54-57`) — auto-commit, no config.

`SecondaryDatabase::update_secondary`
(`secondary_database.rs:296-376`) takes no `Transaction` parameter.
Internally it calls `insert_sec_key` (line 466), `delete_sec_key` (line
492), and `make_inner_cursor` (line 531), each of which opens a fresh
auto-commit cursor on the inner database with `self.inner.open_cursor(
None, None)`.

The mdBook chapter `transactions/secondary-with-txn.md:11-13` claims:

> When you use transactions to protect writes, primary and secondary
> indices are updated atomically within the same transaction, preventing
> secondary index corruption.

This is not what the code does. A user that follows the documented
pattern will see the primary write inside their txn and the secondary
write commit as a separate auto-commit, which means:

* If the txn aborts, the secondary write stays.
* If the secondary write fails, the primary is already in the txn and
  the user has no way to know they should abort.
* Cursor read isolation is broken — a secondary cursor opened "with a
  transaction" reads outside that transaction.

The existing unit tests never pass a `Transaction` to
`SecondaryDatabase::open_cursor` or to `SecondaryDatabase::delete`
(verified by `rg "secondary\.delete\(Some" crates/`), so this gap is
entirely untested.

**Recommended action:** route the txn through. Specifically:

* Plumb `txn: Option<&Transaction>` through `update_secondary`,
  `insert_sec_key`, `delete_sec_key`, and `populate_from_primary_scan`.
* Have `SecondaryCursor::new` accept the user's txn and pass it to
  `inner_db().open_cursor(txn, config)`.
* Either return an error or document the limitation if F2 is not
  implemented.

### F5 — `SecondaryDatabase::delete(txn, key)` mixes contexts (High)

```rust
pub fn delete(&self, txn: Option<&Transaction>, key: &DatabaseEntry)
    -> Result<OperationStatus> {
    self.check_open()?;
    let mut sec_cursor = self.open_cursor_internal()?;        // auto-commit
    ...
    self.delete_all_for_primary(&pri_key_entry, Some(&old_data))?; // auto-commit
    let primary = self.primary.lock();
    let _ = primary.delete(txn, &pri_key_entry)?;             // in txn
    ...
}
```

`secondary_database.rs:206-256`. `txn` is forwarded only to
`primary.delete`. The cursor used to enumerate duplicates and the
secondary cleanup run auto-commit. If the txn later aborts, the primary
is restored but the secondary entries that were deleted stay deleted —
the index loses entries that the primary still has. This is a
correctness regression caused by F4.

### F6 — Docs reference an API surface that does not exist (High)

`docs/src/transactions/secondary-with-txn.md:14-37` shows:

```rust
let sec_config = SecondaryConfig::new()
    .with_allow_create(true)
    .with_transactional(true)               // ← does not exist on SecondaryConfig
    .with_key_creator(Box::new(my_key_creator));

let sec_db = env.open_secondary(           // ← does not exist on Environment
    None,
    "mySecondaryDatabase",
    None,
    primary,
    &sec_config,
)?;
```

The actual API:

```rust
let sec_config = SecondaryConfig::new()
    .with_allow_create(true)
    .with_key_creator(Box::new(my_key_creator));

// User must open the inner database themselves first:
let sec_db = env.open_database(None, "mySecondaryDatabase",
    &DatabaseConfig::new()
        .with_allow_create(true)
        .with_transactional(true))?;     // transactional belongs on the inner db

let secondary = SecondaryDatabase::open(
    Arc::clone(&primary_arc), sec_db, sec_config)?;
```

This is verified against `secondary_config.rs:237-348` (no
`with_transactional`) and `crates/noxu-db/src/environment.rs` (no
`open_secondary` method — `rg "fn open_secondary"` returns only the
test helper inside `secondary_database.rs:644`).

The same chapter then claims atomic primary+secondary updates — which
is also false per F4.

`getting-started/secondary-databases.md` is **correct** about the open
sequence but contradicts itself on the locking: line 8 says

> Secondary databases are read-only from your application's perspective
> — you do not insert into them directly. Instead, whenever you update
> the primary database, you update the secondary index to reflect the
> change.

— which matches F2 reality. So the two chapters disagree about
ownership of secondary maintenance, and one of them disagrees with the
implementation about transaction propagation.

### F7 — `JoinCursor` not in the published docs (High → Medium for users on rustdoc)

`docs/src/SUMMARY.md` does not list `join_cursor.md` or any chapter on
joins. `rg 'JoinCursor|join cursor|equality join' docs/src/` returns
only `maintainer/crate-guide.md:146` and
`maintainer/project-history.md:35`, both maintainer-facing.

The rustdoc on `JoinCursor` is solid (`join_cursor.rs:1-58,118-125,156-
167`) — the API is reachable for users who read rustdoc — but new users
following the mdBook are unlikely to discover it.

### F8 — Optimisation flags inert (Medium)

`SecondaryConfig::immutable_secondary_key` and
`extract_from_primary_key_only` (`secondary_config.rs:189-196`) are
documented as performance hints:

> When true, the secondary key is immutable and cannot change when the
> primary record is updated. This enables an optimization that skips
> calling the key creator on updates.

The helper `update_may_change_secondary()` (`secondary_config.rs:410-
412`) returns `!immutable && !extract_only`. It is called nowhere
outside its own self-tests:

```text
$ rg update_may_change_secondary
crates/noxu-db/src/secondary_config.rs:410:    pub(crate) fn ...
crates/noxu-db/src/secondary_config.rs:541-551,861-867: tests
```

`secondary_database.rs::update_secondary` re-runs the key creator on
every call regardless of these flags. Setting the flags has zero
effect; users believing they have enabled an optimisation are
mistaken.

### F9 — Builder methods write to fields that are never consulted (Medium)

`SecondaryConfig` carries an inner `base: DatabaseConfig`, but
`SecondaryDatabase::open` takes a *separately opened* `Database`
(`secondary_database.rs:81-104`):

```rust
pub fn open(
    primary: Arc<Mutex<Database>>,
    secondary_db: Database,            // ← already open
    config: SecondaryConfig,
) -> Result<Self>
```

Therefore any builder that touches `self.base` —
`with_sorted_duplicates`, `with_allow_create` (in the inner-DB sense) —
is silently ignored at the secondary layer. It only mirrors what was
*supposed* to be on the inner `Database`.

This is the underlying reason F6 fails: there is no way to pass
`with_transactional(true)` through `SecondaryConfig`, because there is
no path that hands `base` to anything that opens a database.

### F10 — API gaps vs BDB SecondaryDatabase (Medium)

Missing methods (compared to BDB-JE `SecondaryDatabase`):

* `count()` — number of secondary entries.
* `exists()` — equivalent of `existsSearchKey`.
* `truncate()` — clear all secondary entries (and possibly cascade to
  primary depending on FK action).
* `get_search_both` — exact `(secondary_key, primary_key)` lookup.
* No `LockMode` / `ReadOptions` parameter on any read; cannot opt into
  `READ_UNCOMMITTED` to skip dangling entries (F13).
* No "primary-key-only" overload of `get` that avoids the primary
  fetch.
* No `getDatabase()` / `getPrimaryDatabase()` accessors (only
  `primary_db()` which is `pub(crate)`). Users have no documented way
  to recover the primary handle from a secondary.

### F11 — `expect()` inside a public-API constructor (Medium)

`SecondaryCursor::new` (`secondary_cursor.rs:52-58`):

```rust
pub(crate) fn new(secondary_db: &'a SecondaryDatabase) -> Self {
    let inner = secondary_db
        .inner_db()
        .open_cursor(None, None)
        .expect("Failed to open inner secondary cursor");
    Self { inner, secondary_db }
}
```

Called from `SecondaryDatabase::open_cursor` and `open_cursor_internal`,
both of which return `Result<SecondaryCursor>`. A failure to open the
inner cursor — for example because the inner database is closed — will
panic instead of being surfaced. AGENTS.md §Error handling says "new
code should prefer `?` and `.expect("invariant: …")` with a
justification" — this `expect` is on a recoverable error, not an
invariant.

### F12 — `JoinCursor` degenerates because of F3 (Medium)

The join algorithm's contract is:

1. Cursor[0] enumerates a *set* of candidate primaries that share its
   sec key.
2. Cursors[1..n] each confirm membership of each candidate.

Step 1 depends on `SecondaryCursor::get_next_dup` returning more than
one candidate. Because of F3, the inner index is one-to-one, so
`get_next_dup` (`secondary_cursor.rs:341-364`) always returns
`NotFound` after the initial position:

```rust
pub(crate) fn get_next_dup(&mut self) -> Result<OperationStatus> {
    let Some(current_sk) = self.get_current_sec_key_bytes()? else { ... };
    let status = self.inner.get(...Get::Next, ...)?;
    ...
    if new_sk == current_sk { Ok(Success) } else { Ok(NotFound) }
}
```

The inner DB has no duplicates, so stepping `Next` always lands on a
different secondary key → always `NotFound`.

Net effect: a join over N secondaries can confirm "the primary key at
cursor[0]'s position appears in all N indexes" but cannot enumerate
true intersection sets. The unit tests
(`join_cursor.rs:382-440,490-535`) all fit this constraint — each test
inserts at most one record per secondary key.

### F13 — Dangling primary handling (Medium)

When a secondary key points at a primary that has been deleted (BDB
calls this a "secondary integrity violation"), every read raises
`NoxuError::SecondaryIntegrityException` — `secondary_database.rs:181-
185`, `secondary_cursor.rs:233-243,272-280,439-450`. The cursor is left
positioned but the caller cannot continue reading without surfacing
the error.

BDB-JE allows the caller to set `LockMode.READ_UNCOMMITTED` on the
secondary cursor, in which case dangling references are silently
skipped (this exists precisely because secondary writes are not always
in lockstep with primary writes — exactly the situation F4 produces).
Noxu DB exposes no such mode on secondary reads.

### F14 — `get_search_key_range` is fragile (Medium)

`secondary_cursor.rs:248-285`:

```rust
pub fn get_search_key_range(&mut self, search_key: ..., p_key: ..., data: ...) {
    let mut stored_pk = DatabaseEntry::new();
    let status = self.inner.get(search_key, &mut stored_pk,
                                Get::SearchGte, None)?;
    ...
    // Update search_key with the actual key found (GTE may advance it).
    let mut dummy_data = DatabaseEntry::new();
    let _ = self.inner.get(search_key, &mut dummy_data, Get::Current, None);
    ...
}
```

The follow-up `Get::Current` call is needed because the writer (or the
auditor) wasn't sure whether `Get::SearchGte` writes the actual
positioned key back into `search_key`. This is exactly the kind of
brittle two-step that the cursor audit
(`api-audit-2026-05-cursor.md`) flagged on the primary path; the
secondary wrapper inherits the risk. Worse: if the second `get` fails
or returns `NotFound` (e.g. a concurrent delete), the result is silently
ignored (`let _ =`).

### F15 — Foreign-key tests exist only as application-level workarounds (Medium)

`crates/noxu-collections/tests/collection_tests.rs:883-995` contains
four tests:

* `test_foreign_key_delete_abort_pattern`
* `test_foreign_key_delete_nullify_pattern`
* `test_foreign_key_delete_cascade_pattern`
* `test_foreign_key_constraint_insert_invalid_fk`

Each test simulates the FK behaviour with two `StoredMap` instances and
manual checks — `store2.contains_key(b"pk2")` returning `true` is taken
as "the secondary still references the primary, abort". None of them
exercises `SecondaryDatabase::delete`, none of them sets
`ForeignKeyDeleteAction::Cascade`. The opening comment plainly says the
engine has no enforcement (F1).

There is **no** noxu-db test that opens a secondary with a
`foreign_key_database` and verifies that deleting from the foreign
database produces the configured Abort/Cascade/Nullify outcome.

### F16 — Raw pointer ABI (Low)

`secondary_config.rs:174-202`:

```rust
pub foreign_key_database: Option<*const Database>,
...
unsafe impl Send for SecondaryConfig {}
```

The pointer's lifetime is hand-waved by the SAFETY comment ("a raw
pointer to a Database whose lifetime is managed by the application").
If/when F1 is implemented, this is a memory-safety hazard waiting for a
user who drops the foreign DB before the secondary. A safer
representation would be `Option<Arc<Database>>` (matching the way the
primary is held).

### F17 — Public fields bypass validate (Low)

All `SecondaryConfig` fields are `pub`. The unit tests in the same file
construct invalid configs via struct literal:

```rust
let config = SecondaryConfig {
    key_creator: Some(Box::new(SimpleKeyCreator)),
    multi_key_creator: Some(Box::new(MkCreator)),
    ..SecondaryConfig::new()
};
assert!(config.validate(false).is_err());
```

(`secondary_config.rs:485-490`). Real users can construct identical
broken configs and only discover the problem when `SecondaryDatabase::open`
runs `validate(...)`. Setter-only fields plus a plain `Self` would
catch this at compile time.

### F18 — `JoinConfig` minimal (Low)

`join_config.rs:14-43` exposes only `no_sort`. BDB-JE's `JoinConfig`
exposes `setCacheMode`. BDB C exposes `joinAbsolute`. Probably fine for
a 1.x API; worth a TODO comment so future additions are not surprising.

### F19 — Close ordering relies on Arc ref-counts (Low)

`secondary_database.rs:62-64`:

```rust
inner: Database,
primary: Arc<Mutex<Database>>,
config: SecondaryConfig,
```

`Database` does not track its associated secondaries. The mdBook
chapter (`getting-started/secondary-databases.md:106`) asserts:

> The secondary must be closed before the primary database and before
> the environment.

Nothing in the code enforces this; the secondary holds an
`Arc<Mutex<Database>>` clone of the primary, which keeps the primary
`Database` value alive even after the user calls `primary.close()`
(close only marks the handle invalid; Drop fires when the last Arc
goes away). So calling `primary.close()` while a secondary is open
will silently succeed, future operations on that primary handle will
return `DatabaseClosed`, and the secondary's `update_secondary` /
`delete` will fail at the inner `primary.lock().get(...)` step with
`DatabaseClosed`. No explicit error like "primary still has open
secondaries" is raised.

### F20 — Throughput counters under-report secondary ops (Low)

`noxu-dbi::ThroughputStats` defines:

```text
n_sec_search_ok / n_sec_search_fail
n_sec_insert_ok / n_sec_insert_fail
n_sec_update_ok
n_sec_delete_ok / n_sec_delete_fail
n_sec_position
```

(`crates/noxu-dbi/src/throughput_stats.rs:33-48`). None of these is
incremented anywhere — `rg n_sec_` returns only the field
declarations. Operators who watch these counters will see zeros while
`update_secondary` is firing.

---

## 5. Coverage gaps

### 5.1 Behavioural

* **No transactional secondary tests.** Zero tests pass a
  `Transaction` to `SecondaryDatabase::open_cursor`, `update_secondary`,
  or `SecondaryDatabase::delete`. The "atomically within the same
  transaction" claim in `secondary-with-txn.md` is asserted nowhere.
* **No sorted-dup secondary tests.** No test inserts two distinct
  primaries that produce the same secondary key and verifies both are
  retrievable. The integration suite always uses keys derived from
  primary key (so distinct primary key ⇒ distinct sec key) or values
  designed not to collide.
* **No FK-action tests in noxu-db.** F15 above; the only tests of FK
  semantics live in `noxu-collections` and explicitly state they
  simulate the engine behaviour.
* **No JoinCursor sorting test.** `JoinCursor::new` sorts cursors by
  estimated dup count when `no_sort = false`, but no test verifies the
  ordering effect (only `test_join_config_no_sort` checks the flag is
  read; no test exercises a case where sorting changes the candidate
  walk).
* **No SecondaryIntegrityException test.** No test creates a dangling
  secondary entry and verifies the error is raised; no test verifies
  the error type or message.
* **No close-ordering test.** No test closes the primary while a
  secondary is open and asserts a defined behaviour (error or
  graceful).
* **`get_next_dup` and `count_estimate` are pub(crate).** Their tests
  rely on `JoinCursor` exercising them; they are not directly covered.

### 5.2 Documentation

* `JoinCursor` and `JoinConfig` have no mdBook chapter (F7).
* `secondary-databases.md` does not document the one-to-one limitation
  (F3) or that `with_sorted_duplicates` is a no-op (F9).
* `secondary-with-txn.md` references nonexistent APIs and makes
  unfulfilled atomicity claims (F6, F4).
* `secondary-databases.md:106` says "The secondary must be closed
  before the primary database and before the environment" but does not
  warn that violating this is undefined-but-silent.
* `SecondaryConfig::immutable_secondary_key` and
  `extract_from_primary_key_only` are documented as optimisations
  (F8) but their inert status is not noted.
* `ForeignKeyDeleteAction` rustdoc is full of "Cascade … delete all
  primary records" prose (`secondary_config.rs:64-77`) without a
  warning that no path implements it.

### 5.3 Implementation

* Inner secondary index is non-dup; cannot represent the canonical
  index (F3).
* No `associate()`-style hook on `Database` (F2).
* No FK enforcement loop, no Cascade walk, no Nullify call site (F1).
* Secondary cursor and `update_secondary` ignore `Transaction` (F4).
* Throughput counters not wired (F20).

---

## 6. Summary

The secondary / join surface in `noxu-db` is **structurally present
but functionally thin**. The class hierarchy, the trait surface, the
configuration types, and the `JoinCursor` algorithm skeleton are all
ported faithfully. Read paths against the one-to-one model work: the
unit and integration tests exercise the rustdoc'd happy path and
pass. However:

* **Three contracts that BDB users would consider load-bearing are
  missing:** foreign-key constraints (`Abort` / `Cascade` / `Nullify`),
  the `associate()` callback that keeps secondaries in sync with
  primary writes, and sorted-dup support that lets multiple primaries
  share a secondary key (F1, F2, F3 — all Critical).
* **Two transactional contracts are silently violated:**
  `SecondaryDatabase::open_cursor` discards the transaction parameter,
  and `SecondaryDatabase::delete` only forwards the transaction to the
  primary (F4, F5 — High). The mdBook page that promises atomic
  primary+secondary updates inside a transaction is therefore
  documenting fiction (F6 — High).
* **Several builder methods on `SecondaryConfig` are config sinks**
  whose values are never consulted at runtime (F8, F9 — Medium).
* `JoinCursor` is correctly implemented for the one-to-one secondary
  model it is built on, but is not exposed in user-facing docs and
  cannot deliver the equality-join cardinality BDB users expect because
  the underlying secondaries cannot store duplicates (F7, F12 — Medium).
* The remaining findings (F10–F20) are an API-surface and hygiene
  punch-list: missing `count` / `exists` / `truncate`, `expect()` in a
  constructor, a fragile two-step `get_search_key_range`, public config
  fields, an unused FK raw pointer, and dead throughput counters.

The recommended follow-up sequence is, in priority order:

1. Decide and document the secondary model: do we want sorted-dup
   secondaries (BDB-style) or commit to one-to-one and rename the
   feature accordingly (F3). Without that decision the rest of the
   surface is undefined.
2. Implement an `associate()`-equivalent so primary writes maintain
   secondaries inside the user's txn, or remove `txn` parameters from
   `SecondaryDatabase::open_cursor` / `delete` and update the docs to
   match the manual-update reality (F2, F4, F5, F6).
3. Either implement foreign-key enforcement or have
   `SecondaryDatabase::open` reject configurations that set the FK
   fields, with a clear error message (F1, F15).
4. Add tests under transactions, dangling-primary, and sorted-dup
   collisions before adding the corresponding behaviour, so each
   landing PR can prove progress (§5.1).
5. Clean up the inert builders, the docs, and the throughput counters
   together (F8, F9, F18, F19, F20).

No code or docs were changed by this audit.
