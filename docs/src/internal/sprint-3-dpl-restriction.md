# Sprint 3B — DPL `txn` Threading and the In-Memory-Secondary Restriction

> Internal — context for the v1.5 → v1.6 DPL roadmap. Companion to
> [`v1.5-decisions-2026-05.md`](v1.5-decisions-2026-05.md) and the
> persist/xa portion of the May 2026 API audit
> (`api-audit-2026-05-persist-xa.md`).

## What changed in v1.5 (Sprint 3B)

`PrimaryIndex` and `SecondaryIndex` reads and writes now take a leading
`txn: Option<&Transaction>` argument:

| Method (old) | Method (new, v1.5) |
|---|---|
| `PrimaryIndex::get(&ser, &key)` | `PrimaryIndex::get(txn, &ser, &key)` |
| `PrimaryIndex::put(&ser, &entity)` | `PrimaryIndex::put(txn, &ser, &entity)` |
| `PrimaryIndex::put_no_overwrite(&ser, &entity)` | `PrimaryIndex::put_no_overwrite(txn, &ser, &entity)` |
| `PrimaryIndex::delete(&key)` | `PrimaryIndex::delete(txn, &key)` |
| `PrimaryIndex::delete_with_entity(&ser, &key)` | `PrimaryIndex::delete_with_entity(txn, &ser, &key)` |
| `PrimaryIndex::contains(&key)` | `PrimaryIndex::contains(txn, &key)` |
| `PrimaryIndex::entities(&ser)` | `PrimaryIndex::entities(txn, &ser)` |
| `PrimaryIndex::keys()` | `PrimaryIndex::keys(txn)` |
| `SecondaryIndex::get(&ser, &primary, &sk)` | `SecondaryIndex::get(txn, &ser, &primary, &sk)` |
| `SecondaryIndex::delete(&ser, &primary, &sk)` | `SecondaryIndex::delete(txn, &ser, &primary, &sk)` |
| `SecondaryIndex::iter(&ser, &primary)` | `SecondaryIndex::iter(txn, &ser, &primary)` |
| `SecondaryIndex::iter_from(&ser, &primary, &from)` | `SecondaryIndex::iter_from(txn, &ser, &primary, &from)` |

Pass `Some(&txn)` to participate in an explicit transaction; pass
`None` for the historical auto-commit semantics. This is a hard
breaking change at the source level — there is no compatibility shim —
because the historical signature could not participate in a user
transaction. See the migration note at the bottom of this document.

## Audit findings addressed

From `docs/src/internal/api-audit-2026-05-persist-xa.md`:

| ID | Finding | Status in v1.5 |
|---|---|---|
| **C6** | `PrimaryIndex::put` always passes `None` for the txn; entity writes cannot participate in a user txn. | **Closed.** The new `txn` parameter is forwarded to the underlying `Database::{get,put,delete,open_cursor}`. New regression suite at `crates/noxu-persist/tests/txn_threading_tests.rs` proves commit/abort participation. |
| **#10** | DPL secondary indexes are in-memory `BTreeMap`s, not durable. | **Documented as v1.5 limitation; deferred to v1.6.** See restriction discussion below. |
| **#11** | DPL secondary updates are not atomic with the user txn. | **Documented as v1.5 limitation; deferred to v1.6.** Operator-visible signal: `PersistError::SecondariesNotTransactional` (one-shot `log::warn!` per `PrimaryIndex` plus a `debug_assert!` suppressible with `NOXU_PERSIST_ALLOW_NON_TXN_SECONDARIES=1`). |
| **#18** | The audit's "major scope gap": secondary durability and txn-atomicity together. | **Documented and scoped for v1.6** alongside the BDB-JE-shaped `associate()` work in Decision 1 of `v1.5-decisions-2026-05.md`. |

## Why the in-memory-secondary restriction was kept for v1.5

The audit calls in-memory secondaries a *major scope gap*. Closing that
gap inside v1.5 would require:

1. A second underlying `Database` per secondary index, opened with the
   secondary key as the primary key and the entity primary key as the
   value (sorted-dup or non-dup depending on cardinality).
2. An `associate()`-style hook on `Database` so secondary maintenance
   happens inside the same `db_impl.put` write path under the user's
   txn locker.
3. Schema-evolution support to add/remove secondary indexes against an
   existing store without rewriting the primary.
4. `populate_secondary_database` for the catch-up case after opening a
   pre-existing primary that has new secondaries declared.

That is the same body of work `v1.5-decisions-2026-05.md` Decision 1
sized at 1,500–2,500 production + 800–1,500 test lines and recommended
deferring to v1.6 (option B: "ship sorted-dup + associate in v1.6").
Sprint 3B implements only the txn-threading half of that decision —
the half that does not require new on-disk semantics — and leaves the
durability/atomicity half scheduled for v1.6.

A bounded "queue secondary mutations on the txn and apply on commit /
discard on abort" middle ground was considered. It was rejected
because:

* `noxu_db::Transaction` does not expose a public `register_callback`
  API and adding one is explicitly out of scope for Sprint 3B (the
  task limits edits to `crates/noxu-persist/`).
* Without commit/abort callbacks, the queueing logic would have to
  poll the txn state from a side thread, racing with the commit/abort
  itself.
* Even if callbacks were available, the queue would still be a
  process-local data structure — secondaries would still not survive a
  restart, so the "did this secondary mapping exist before the crash?"
  question would remain unanswerable from the on-disk image alone.
  v1.6 has to back secondaries with a real `Database` regardless;
  doing the queueing first as a v1.5 stop-gap is wasted work.

## Operator-visible signal

When `PrimaryIndex::put(Some(&txn), …)` or
`PrimaryIndex::delete_with_entity(Some(&txn), …)` is called against a
primary that has at least one registered secondary index, the
`PrimaryIndex` emits **once per instance**:

```text
WARN noxu_persist: DPL secondary indexes are in-memory only in v1.5;
secondary updates are not atomic with the user transaction (see
docs/src/collections/entity-persistence.md, v1.5 limitations) (entity: <Name>)
```

The message is the `Display` of the new
`PersistError::SecondariesNotTransactional` variant. The error is
constructed but **not returned** — operations continue to succeed —
because the limitation is documented behaviour, not a runtime fault.

In debug builds the same code path also fires a `debug_assert!`. Tests
that legitimately exercise `Some(&txn) + secondary` (for example the
`secondary_index_update_is_not_atomic_with_txn_v1_5` regression test)
opt out via the `NOXU_PERSIST_ALLOW_NON_TXN_SECONDARIES=1` environment
variable.

## What v1.6 will do (planned)

* Open one `Database` per registered secondary, with the same
  transactionality as the primary.
* Move secondary maintenance from `PrimaryIndex` into the same
  `db_impl.put` / `db_impl.delete` write path used by the primary, so
  secondaries are written under the same lock and txn as the primary
  record (closes audit #11).
* Persist the secondary alongside the primary so it survives restart
  (closes audit #10).
* Remove the `PersistError::SecondariesNotTransactional` warning path
  and the `NOXU_PERSIST_ALLOW_NON_TXN_SECONDARIES` env-var escape
  hatch.
* Flip the trailing assertion in
  `tests/txn_threading_tests.rs::secondary_index_update_is_not_atomic_with_txn_v1_5`
  so the test asserts the *fixed* behaviour (secondary rolled back on
  abort).

## Migrating v1.4 callers

The change is mechanical. For every call site:

```rust
// Before (v1.4):
index.put(&ser, &entity)?;
let user = index.get(&ser, &id)?;
index.delete(&id)?;

// After (v1.5):
index.put(None, &ser, &entity)?;
let user = index.get(None, &ser, &id)?;
index.delete(None, &id)?;
```

To opt into transactions, replace `None` with `Some(&txn)` after
calling `env.begin_transaction(None)?`. See
`docs/src/collections/entity-persistence.md` for a full example.

The single workspace example (`examples/persist.rs`) and all
`crates/noxu-persist/` tests have been migrated to the new shape;
external downstream consumers must do the equivalent rewrite.
