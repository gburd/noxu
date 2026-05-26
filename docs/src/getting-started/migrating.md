# Migrating from v1.4.x

This page lists every observable behaviour change between v1.4.x and
v1.5 that is likely to surface in user code. The list is grouped by
sprint so you can correlate each item with its audit finding and
restriction note.

> **v1.5 capability matrix:** see
> [Introduction → v1.5 capability matrix](../introduction.md#v15-capability-matrix)
> for the canonical "what is supported in v1.5 vs planned for v1.6 /
> v2.0" table.

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
  `NoxuError::Unsupported`.** Decision 3B. The `parent` parameter is
  retained for forward source compatibility and scheduled for removal
  in v2.0.
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
* **`Stored*` collection methods are auto-commit only** in v1.5;
  `TransactionRunner` cannot drive them. Use the runner with the raw
  `Database` / `Cursor` API for now. Threading `Option<&Transaction>`
  is planned for v1.6 alongside the typed-API redesign. See
  [Collections and Persistence — v1.5 collections — what's in
  scope](../collections/README.md#v15-collections--whats-in-scope).
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
let txn2 = env.begin_transaction(Some(&txn), None)?; // accepted, no-op

// v1.5 (correct)
let cursor = db.open_cursor(Some(&txn), None)?;  // honours txn
secondary.update_secondary(&pk, None, Some(&v))?; // returns
                                                  // NoxuError::Unsupported
                                                  // on cross-primary
                                                  // collision
let txn2 = env.begin_transaction(Some(&txn), None)?; // returns
                                                     // NoxuError::Unsupported

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
