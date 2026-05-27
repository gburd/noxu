# Migrating from v1.4.x

This page lists every observable behaviour change between v1.4.x and
v1.5 that is likely to surface in user code. The list is grouped by
sprint so you can correlate each item with its audit finding and
restriction note.

> **v1.5 capability matrix:** see
> [Introduction → v1.5 capability matrix](../introduction.md#v15-capability-matrix)
> for the canonical "what is supported in v1.5 vs planned for v1.6 /
> v2.0" table.

## Wave 2B — Collections typed API and txn threading (v1.5 → v1.6)

Wave 2B closes audit findings #1, #3, #4, #5, #11, and #12 from
the May 2026 collections / bind audit.  This is the largest user-visible
breaking change in the v1.5 → v1.6 transition.  See
[`docs/src/internal/wave-2b-collections-typed.md`](../internal/wave-2b-collections-typed.md)
for the full scope.

### Source-level breaking changes

* **`StoredMap<'db>` is now `StoredMap<'db, K, V, KB, VB>`.**  The map
  is parameterised by `EntryBinding` implementations for keys and
  values.

  ```rust,ignore
  // v1.5
  let map = StoredMap::new(&db, /* read_only = */ false);
  map.put(b"key", b"value")?;

  // v1.6
  use noxu_bind::ByteArrayBinding;
  let map: StoredMap<Vec<u8>, Vec<u8>, _, _> =
      StoredMap::new(&db, ByteArrayBinding, ByteArrayBinding);
  map.put(None, &b"key".to_vec(), &b"value".to_vec())?;
  ```

  Same shape for `StoredSortedMap<K, V, KB, VB>`,
  `StoredKeySet<K, KB>`, `StoredValueSet<V, VB>`, and
  `StoredList<V, VB>`.

* **Every `Stored*` method now takes `txn: Option<&Transaction>` as
  the leading argument.**

  ```rust,ignore
  // v1.5
  map.put(b"k", b"v")?;            // auto-commit
  map.get(b"k")?;
  map.iter()?;

  // v1.6
  map.put(None, &k, &v)?;          // auto-commit
  map.get(None, &k)?;
  map.iter(None)?;
  // ...or pass Some(&txn) to participate in a user txn:
  map.put(Some(&txn), &k, &v)?;
  ```

  This applies to `get`, `put`, `remove`, `contains_key`, `len`,
  `is_empty`, `iter`, `keys`, `values`, `clear`, `iter_from`,
  `iter_reverse`, `first_key`, `last_key`, `first_entry`,
  `last_entry`, `higher_key` (StoredSortedMap), `add`, `contains`,
  `remove` (StoredKeySet), and every `StoredList` method.

* **`StoredMap::len` returns `usize` instead of `u64`.**  The on-disk
  count is bounded by `Database::count() -> u64` but Rust callers
  almost always want `usize` (matching `BTreeMap::len`); the
  collections layer truncates to `usize::MAX` at the boundary.

* **The internal `BTreeSet` key index is removed.**
  `register_key`, `register_keys`, `known_keys` are deleted.  Pre-
  existing data that was visible in v1.5 only after a
  `register_keys` call is now visible automatically because
  `iter` / `keys` / `values` walk the database directly via a
  cursor:

  ```rust,ignore
  // v1.5: iteration only saw keys you'd registered.
  let map = StoredMap::new(&db, true);
  map.register_keys(&[b"a", b"b"]);
  for entry in map.iter()? { /* sees a, b */ }

  // v1.6: iteration sees every record in the database.
  for entry in map.iter(None)? { /* sees every record */ }
  ```

* **`StoredList::remove` now compacts.**  Removing index `i` shifts
  every record at index `j > i` down to `j - 1` and decrements
  `next_index`.  Code that relied on the v1.5 "remove leaves a hole"
  contract will see different `get(idx)` results after `remove`.
  The whole compaction is issued under the supplied txn; pass
  `Some(&txn)` for crash-atomic semantics.

* **`StoredList::new` / `StoredList::open` now take a value
  binding.**

  ```rust,ignore
  // v1.5
  let list = StoredList::new(&db);
  let list = StoredList::open(&db)?;

  // v1.6
  use noxu_bind::ByteArrayBinding;
  let list: StoredList<Vec<u8>, _> =
      StoredList::new(&db, ByteArrayBinding);
  let list: StoredList<Vec<u8>, _> =
      StoredList::open(&db, ByteArrayBinding)?;
  ```

### Behavioural breaking changes

* **`TransactionRunner` now drives `Stored*` methods.**  In v1.5 the
  `&Transaction` it supplied could not be threaded into any `Stored*`
  call (every `Stored*` method ignored its txn argument because there
  *was* no txn argument).  In v1.6 the runner-supplied `&Transaction`
  is the canonical way to make a sequence of `Stored*` writes
  transactional:

  ```rust,ignore
  let runner = TransactionRunner::new(&env);
  runner.run(|txn| {
      map.put(Some(txn), &k1, &v1)?;
      map.put(Some(txn), &k2, &v2)?;
      list.remove(Some(txn), 0)?;          // shift-compaction inside the txn
      Ok(())
  })?;
  ```

* **`TransactionRunner` retries on every retryable error, with
  jittered exponential backoff.**  v1.5 retried only on
  `DeadlockDetected`.  v1.6 retries on every variant returned by
  `NoxuError::is_retryable()` (`LockConflict`, `DeadlockDetected`,
  `LockTimeout`, `LockNotAvailable`, `TransactionTimeout`,
  `LockPreempted`).  Defaults: 10 retries, 10 ms base, 1 s ceiling,
  ±25% jitter.  Configure via:

  ```rust,ignore
  TransactionRunner::new(&env)
      .with_max_retries(20)
      .with_base_backoff(Duration::from_millis(5))
      .with_max_backoff(Duration::from_secs(2))
      .with_jitter(0.1);
  ```

* **`StoredKeySet::add` returns `bool` (newly inserted).**  v1.5 had
  no `add` method; the v1.6 `add` matches `java.util.Set.add`
  semantics (returns `true` on first insert, `false` if already
  present).

* **`TransactionRunner::run`'s closure signature relaxed from `Fn`
  to `FnMut`.**  Closures may now capture mutable state (e.g. retry
  counters).

## Behaviour changes (Sprint 1 — txn wiring)

These are previously-broken paths that the engine now executes
correctly. Code that *depended* on the v1.4.x bug will break.

* **`Database::open_cursor(Some(&txn), …)` now threads `txn` through
  to the cursor.** Cursors opened on a transactional database
  participate in the transaction as documented. v1.4.x silently
  ignored the argument — every cursor was effectively auto-commit.
  The change can surface as new lock conflicts on workloads that were
  accidentally racing against themselves.
* **`SecondaryDatabase::open_cursor(Some(&txn), …)`** — same fix.
* **`Database::count()` on a sorted-dup database** is now correct;
  v1.4.x returned 0.
* **`Database::delete(key)` on a sorted-dup database** now removes
  every duplicate value for `key`. v1.4.x removed only the first
  duplicate.
* **`Environment::close()` after `txn.commit()` succeeds.** v1.4.x's
  active-transactions gate fired even after every txn was already
  committed.
* **`EnvironmentConfig::durability` is honoured.** v1.4.x stored the
  policy on the config but never threaded it into the txn manager.
* **`TransactionConfig::read_uncommitted` is honoured.** Same shape.

## Behaviour changes (Sprint 1 — cursor `Get` variants)

* **`Get::SearchBoth` on a non-duplicates database now validates the
  data argument.** A non-matching data returns `NotFound` instead of
  succeeding on the key alone.
* **`Get::NextDup` / `Get::PrevDup` on a non-duplicates database** now
  return `NotFound` (consistent with the no-dups invariant). v1.4.x's
  behaviour was undefined.
* **`Get::SearchLte`, `Get::FirstDup`, `Get::LastDup`** now return
  `NoxuError::Unsupported`. These variants were never wired in v1.4.x
  (the stub paths returned `NotFound` or panicked depending on the
  db shape); v1.5 surfaces a typed error so callers can match against
  it. Planned for v1.6.

## Behaviour changes (Sprint 3D — v1.5 architectural decisions)

These changes reject configurations the engine cannot honour today.
The breakage radius for each is described in
[`docs/src/internal/sprint-3-decisions-enforced.md`](../internal/sprint-3-decisions-enforced.md);
none of them have non-test callers in the repository.

* **`Environment::begin_transaction(Some(&parent), …)` returns
  `NoxuError::Unsupported` (v1.5) — and the `parent` parameter has been
  removed entirely in v2.0 (Wave 3-1).** Decision 3B. See the v1.5 →
  v2.0 section below for the source-compatibility break.
* **`SecondaryConfig::with_foreign_key_database` /
  `with_foreign_key_delete_action` /
  `with_foreign_key_nullifier` /
  `with_foreign_multi_key_nullifier` are rejected at
  `SecondaryDatabase::open` with `NoxuError::Unsupported`.**
  Decision 2C. The setters are still chainable on
  `SecondaryConfig` so source written against v1.6 keeps compiling
  on v1.5; the rejection fires only when an FK-configured config
  reaches `open`.
* **`SecondaryDatabase` cross-primary collisions return
  `NoxuError::Unsupported`.** Decision 1B. v1.4.x silently overwrote
  the first primary's secondary entry when a second primary produced
  the same secondary key. v1.5 rejects the second insert with a typed
  error and leaves the first primary's mapping intact. Idempotent
  re-inserts of the same `(sec_key, pri_key)` pair remain a no-op so
  v1.4 callers that relied on `update_secondary(pk, None, Some(d))`
  twice for the same primary keep working.

## Behaviour changes (Sprint 3A — XA in-process only)

See [`docs/src/internal/sprint-3-xa-restriction.md`](../internal/sprint-3-xa-restriction.md).

* **`xa_commit(xid)` / `xa_rollback(xid)` on an XID that exists in
  the persistent `_xa_prepared` log but not in the in-memory
  `branches` map return `XaError::CrashDurabilityNotSupported`.**
  v1.4.x returned the misleading `XaError::NotFound` for the same
  case. The XID is still surfaced by `xa_recover` so operators can
  see what is in doubt; clear it with `xa_forget`.
* **`xa_prepare` no longer requires `mark_write`.** v1.5 auto-detects
  writes via `Transaction::has_logged_entries`. `mark_write` is kept
  as a no-op for source compatibility.

## Source-level breaking changes (Sprint 3B — DPL `txn` threading)

This is the only Sprint 3 change with a non-trivial source-level
migration. See
[`docs/src/internal/sprint-3-dpl-restriction.md`](../internal/sprint-3-dpl-restriction.md).

* **`PrimaryIndex::{put, put_no_overwrite, get, delete,
  delete_with_entity, contains, entities, keys}` now take
  `txn: Option<&Transaction>` as the leading argument.**
  Pass `None` for the historical auto-commit semantics.
* **`SecondaryIndex::{get, delete, iter, iter_from}` take
  `txn: Option<&Transaction>` as the leading argument.**
* **DPL secondary index updates remain non-atomic with the user txn
  in v1.5.** A one-shot
  `PersistError::SecondariesNotTransactional` warning logs at the
  first such call against a primary with registered secondaries.
  Suppress in tests with `NOXU_PERSIST_ALLOW_NON_TXN_SECONDARIES=1`.
  Closes alongside Decision 1's sorted-dup work in v1.6.

## On-disk breaking changes (Sprint 3C — collections & bind)

* **`SerdeBinding<T>` payloads now carry a 2-byte
  `[0xCB, 0x01]` magic + version header.** Records written by
  earlier 1.5 release candidates do **not** carry the header and will
  fail to decode under v1.5 with
  `BindError::VersionMismatch`. Migrate by re-`put`-ing the data
  under the v1.5 build, or by staying on the pre-3C build until you
  have a maintenance window. The plain tuple bindings
  (`IntBinding`, `LongBinding`, `StringBinding`,
  `SortedDoubleBinding`) are unaffected.
* **`StoredList::next_index` is now persistent.** Use
  `StoredList::open(&db)` (new) when reopening a database that
  already contains entries; it recovers `next_index` from the
  largest existing 8-byte big-endian key.
  `StoredList::new(&db)` is preserved for empty / fresh databases
  but **does not recover** `next_index`; using it against an existing
  list re-uses slot 0 and overwrites the first record.

## On-disk breaking changes (Wave 2C-2 — DPL entity record envelope)

* **Every entity record stored by `noxu-persist::PrimaryIndex` now
  carries a per-record class-version envelope.**  Pre-Wave-2C-2
  records were the raw output of
  `EntitySerializer::serialize`; v1.6 records prepend

  ```text
  [2-byte class_version BE]
  [1-byte entity_class_tag_len]
  [entity_class_tag bytes]    (UTF-8, length = tag_len)
  [payload bytes]             (your EntitySerializer's output)
  ```

  This is **not backward-compatible** with pre-v1.6 entity stores.
  Reading a pre-v1.6 record under v1.6 fails with
  `PersistError::SerializationError("record too short for entity
  envelope: ...")` or `"entity class tag mismatch: on-disk '…' !=
  expected '…'"`.

  **Migration procedure (one-shot dump and reload):**

  1. While still on v1.5.x, run a dump utility that walks every
     entity database with the user's existing `EntitySerializer`
     and writes the deserialised entities to a sidecar file (any
     format — JSON, ndjson, custom binary; the format is local to
     your migration).
  2. Take the application offline and bump to v1.6.
  3. Open the v1.6 environment with
     `EntityStore::open(&env, StoreConfig::new(...).with_allow_create(true))`,
     iterate the sidecar, and `index.put(None, &ser, &entity)` each
     record.  v1.6's `put` writes the new envelope.
  4. Drop the v1.5 entity database files.

  Stores that opened the entity DBs **only** under v1.6 are
  unaffected — the envelope is universal under v1.6.

* **`Entity` trait gained a default `class_version() -> u16` method.**
  Existing implementations need no change (the default is `0`).
  Bump `class_version()` whenever you change the on-disk shape of an
  entity and supply matching `noxu_persist::evolve::Mutations` via
  `StoreConfig::with_mutations(...)` so the open path can run
  schema evolution for older records.

* **`EntitySerializer` trait gained a default `deserialize_versioned`
  method.**  Existing implementations work as-is.  Override
  `deserialize_versioned` when you want field-level evolution that
  reads old records lazily without rewriting them.  See
  [Schema evolution](../collections/entity-persistence.md#schema-evolution-wave-2c-2).

* **A hidden catalog database
  `__noxu_persist_catalog__<store_name>` is now created in every
  environment that opens an `EntityStore`.**  It records the most
  recent class version observed for each entity name and is
  consulted by the open-path schema-evolution flow.  The catalog is
  opened lazily on the first `get_primary_index<E>()` /
  `evolve()` call, so existing pre-v1.6 environments that have
  already had their data dump-and-reloaded will gain the catalog
  the next time they are opened.

## Documented v1.5 limitations (no source change required)

These are not breakages; they are clarifications. They affect the
shape of patterns we recommend rather than the source-level signature
of any method.

* **`secondary.update_secondary(...)` runs auto-committed** even when
  the surrounding primary write is under a user txn. v1.5 has no
  `associate()` hook; the `update_secondary` call itself does not
  take a transaction. Atomic primary + secondary writes are planned
  for v1.6 alongside Decision 1's sorted-dup + `associate` work. See
  [Secondary Indices with Transactions](../transactions/secondary-with-txn.md).
* **`Stored*` collection methods now thread `Option<&Transaction>`
  through every operation** — v1.5's auto-commit-only restriction is
  closed by Wave 2B (see [Wave 2B — Collections typed API and txn
  threading](#wave-2b--collections-typed-api-and-txn-threading-v15--v16)
  above).  `TransactionRunner` is now the recommended way to drive
  multi-statement `Stored*` sequences.
* **Replication is preview / proof-of-concept.** Ten GA blockers are
  tracked in
  [`docs/src/internal/api-audit-2026-05-rep.md`](../internal/api-audit-2026-05-rep.md).
  Do not deploy `noxu-rep` for production data in v1.5.

## Quick before/after summary

```rust
// v1.4.x (broken)
let cursor = db.open_cursor(Some(&txn), None)?;  // txn was ignored
secondary.update_secondary(&pk, None, Some(&v))?; // silently overwrote
                                                  // existing secondary
                                                  // for cross-primary
                                                  // key collisions
let txn2 = env.begin_transaction(Some(&txn), None)?; // accepted, no-op (v1.4.x)

// v1.5 (correct)
let cursor = db.open_cursor(Some(&txn), None)?;  // honours txn
secondary.update_secondary(&pk, None, Some(&v))?; // returns
                                                  // NoxuError::Unsupported
                                                  // on cross-primary
                                                  // collision
let txn2 = env.begin_transaction(Some(&txn), None)?; // returns
                                                     // NoxuError::Unsupported
                                                     // (v1.5; in v2.0 this
                                                     //  is a compile error
                                                     //  — see the v1.5 →
                                                     //  v2.0 section below)

// DPL (breaking source-level signature change)
// v1.4.x:
//   index.put(&ser, &user)?;
//   let u = index.get(&ser, &id)?;
// v1.5:
index.put(None, &ser, &user)?;          // explicit auto-commit
let u = index.get(None, &ser, &id)?;
// or, to participate in a user txn:
index.put(Some(&txn), &ser, &user)?;
```

---

## v1.5 → v2.0 — nested-transaction parameter removed (Wave 3-1)

Decision 3B in
[`v1.5-decisions-2026-05.md`](../internal/v1.5-decisions-2026-05.md)
staged the deprecation of the nested-transaction `parent` argument in
two phases: v1.5 rejected `Some(_)` at runtime, and v2.0 removes the
parameter from the signature entirely.  Wave 3-1 lands the v2.0 path.

### Breaking signature change

```rust
// v1.4.x and v1.5 / v1.6
fn begin_transaction(
    &self,
    parent: Option<&Transaction>,
    config: Option<&TransactionConfig>,
) -> Result<Transaction>;

// v2.0 (Wave 3-1)
fn begin_transaction(
    &self,
    config: Option<&TransactionConfig>,
) -> Result<Transaction>;
```

### Mechanical migration

```rust
// before
let txn  = env.begin_transaction(None, None)?;
let txn2 = env.begin_transaction(None, Some(&cfg))?;
// (and the v1.5-rejected misuse, which now will not compile)
let bad  = env.begin_transaction(Some(&parent), None)?;

// after
let txn  = env.begin_transaction(None)?;
let txn2 = env.begin_transaction(Some(&cfg))?;
// no v2.0 equivalent for nested txns — they remain unsupported, and the
// type system now enforces it.
```

The blast radius for this change is small in practice because Decision
3B's v1.5 path already rejected the `Some(parent)` form at runtime; in
v2.0 the same misuse is caught at compile time instead.  See
[`wave-3-1-nested-txn-removal.md`](../internal/wave-3-1-nested-txn-removal.md)
for details.

---
