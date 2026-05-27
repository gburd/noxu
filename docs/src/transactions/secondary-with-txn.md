# Secondary Indices with Transactions

> **v1.5 capability matrix:** see
> [Introduction → v1.5 capability matrix](../introduction.md#v15-capability-matrix).
>
> **v1.5 limitations:** see
> [Getting Started → Secondary Databases → v1.5 limitations](../getting-started/secondary-databases.md#v15-limitations).
> v1.5 secondaries are one-to-one (Decision 1B); foreign-key constraints
> are rejected at `SecondaryDatabase::open` (Decision 2C); and the
> BDB-JE-style automatic `associate()` hook plus the matching
> `populate_secondaries` helper are **not** implemented in v1.5
> (planned for v1.6).
>
> **Sprint 4½ update:** as of v1.5, `SecondaryDatabase::update_secondary`
> takes an explicit `Option<&Transaction>`. When you thread the *same*
> `txn` through both `Database::put` and
> `SecondaryDatabase::update_secondary`, the primary write and the
> secondary index update are **atomic** — committing or aborting the
> txn commits or rolls back both sides together. The earlier
> partial-atomicity caveat ("`update_secondary` runs auto-committed
> even when the primary write is under a user transaction") no longer
> applies to the manual-update pattern.

## What v1.5 actually does

Earlier drafts of this chapter described a fictional API
(`env.open_secondary(...)`, `SecondaryConfig::with_transactional(true)`,
`Database::put_and_update_secondaries`) that does not exist. The real
v1.5 surface is:

* `SecondaryDatabase::open(primary, inner_secondary_db, sec_config)`
  — opens a secondary view over an already-open secondary `Database`.
  Both the primary and the inner secondary `Database` must be opened
  with `DatabaseConfig::with_transactional(true)` on a transactional
  `Environment`. There is no `with_transactional` flag on
  `SecondaryConfig`.
* `secondary.open_cursor(Some(&txn), config)` — secondary reads
  participate in `txn` correctly in v1.5 (Sprint 1C threaded the
  argument through; pre-1.5 release candidates silently ignored it).
* `secondary.update_secondary(Some(&txn), pri_key, old_data, new_data)`
  — application-driven secondary maintenance. **Takes a transaction**
  as the leading argument as of Sprint 4½; pass the same `txn` you used
  for the primary write to make both sides atomic, or pass `None` to
  run the secondary update auto-committed (the v1.4 / pre-rc shape).
  v1.5 has no `associate()` hook, so the application must still call
  `update_secondary` after every primary `put` / `delete`.

What v1.6 still owes you: an automatic `associate()` hook so
`Database::put` itself drives every attached secondary inside the
caller's transaction. The same shape as the in-memory DPL secondary
gap tracked in
[`docs/src/internal/sprint-3-dpl-restriction.md`](../internal/sprint-3-dpl-restriction.md);
both close together in v1.6 alongside Decision 1's sorted-dup work.

## Opening a secondary on a transactional environment

```rust
use noxu_db::{
    Database, DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig,
    SecondaryConfig, SecondaryDatabase,
};
use parking_lot::Mutex;
use std::path::PathBuf;
use std::sync::Arc;

# struct DepartmentKeyCreator;
# impl noxu_db::SecondaryKeyCreator for DepartmentKeyCreator {
#     fn create_secondary_key(
#         &self, _: &Database, _: &DatabaseEntry, _: &DatabaseEntry,
#         _: &mut DatabaseEntry,
#     ) -> bool { false }
# }
fn open_transactional_secondary(
    env: &Environment,
) -> Result<(Arc<Mutex<Database>>, SecondaryDatabase), Box<dyn std::error::Error>> {
    let db_config = DatabaseConfig::new()
        .with_allow_create(true)
        .with_transactional(true);

    // Both databases use the same transactional DatabaseConfig.
    let primary = env.open_database(None, "employees", &db_config)?;
    let primary = Arc::new(Mutex::new(primary));
    let inner_secondary =
        env.open_database(None, "by_department", &db_config)?;

    let sec_config = SecondaryConfig::new()
        .with_allow_create(true)
        .with_allow_populate(true)
        .with_key_creator(Box::new(DepartmentKeyCreator));

    // SecondaryDatabase::open does not take a transaction. Foreign-key
    // configuration on `sec_config` is rejected here in v1.5 with
    // NoxuError::Unsupported (Decision 2C).
    let secondary =
        SecondaryDatabase::open(Arc::clone(&primary), inner_secondary, sec_config)?;

    Ok((primary, secondary))
}
```

## Read path: cursors honour the transaction

Secondary reads under a user transaction work as expected. Open the
cursor with `Some(&txn)` and close it before committing or aborting:

```rust
use noxu_db::{Get, OperationStatus};

let txn = env.begin_transaction(None)?;
let mut cursor = secondary.open_cursor(Some(&txn), None)?;

let mut sec_key = DatabaseEntry::from_bytes(b"Engineering");
let mut pk = DatabaseEntry::new();
let mut data = DatabaseEntry::new();

let status = cursor.get(&mut sec_key, &mut pk, &mut data, Get::SearchGte, None)?;
if status == OperationStatus::Success {
    // ... use the row ...
}

cursor.close()?;       // close before commit/abort
txn.commit()?;
```

## Write path: atomic primary + secondary in v1.5 (Sprint 4½)

The canonical pattern is to thread the *same* transaction through
both `Database::put` and `SecondaryDatabase::update_secondary`. Both
operations run inside the same lock set and commit (or abort)
together — there is no window where the primary record exists
without its secondary index entry, or vice versa.

```rust
use noxu_db::DatabaseEntry;

let key = DatabaseEntry::from_bytes(b"alice");
let new_value = DatabaseEntry::from_bytes(b"Engineering|Senior Engineer");

// Read the previous primary record so we know the old secondary key.
// (Reads can be auto-commit or under the same txn; either works.)
let old_value = primary.lock().get(None, &key)?;

let txn = env.begin_transaction(None)?;

// Primary write under txn.
primary.lock().put(Some(&txn), &key, &new_value)?;

// Secondary maintenance under the SAME txn. Both ops use `Some(&txn)`,
// so they participate in the same lock set and commit/abort atomically.
secondary.update_secondary(
    Some(&txn),
    &key,
    old_value.as_ref(),
    Some(&new_value),
)?;

// Commit: the primary record AND the secondary index entry persist
// together. If you instead call `txn.abort()`, BOTH are rolled back —
// no dangling secondary entry can survive the abort.
txn.commit()?;
```

Why this is atomic:

* `Database::put(Some(&t), …)` acquires a write lock on the primary
  record's slot under `t`'s locker.
* `SecondaryDatabase::update_secondary(Some(&t), …)` opens an inner
  cursor over the secondary index *with the same `t`*, so its
  `Put::NoOverwrite` insert (or its `Cursor::delete` for a
  sec-key change / primary delete) acquires its locks under `t` as
  well.
* `t.commit()` flushes both the primary log entry and the secondary
  log entry as a unit; `t.abort()` rolls both back.

If `update_secondary` returns `NoxuError::Unsupported`, two distinct
primary records have produced the same secondary key. v1.5 secondaries
are one-to-one (Decision 1B) and reject the second insert at the
secondary layer rather than silently overwriting the first. Because
both writes are under the same txn, **the conflict surfaces before
the primary is committed**: handle the error and either re-key, drop
the new primary, or `txn.abort()` to roll back both sides cleanly.
Sorted-dup secondaries that accept many-to-one mappings are scheduled
for v1.6.

### Auto-commit (v1.4 shape) is still supported

If you do not need cross-database atomicity — e.g. a one-shot
population script or a single-threaded job that tolerates
partial-update windows — pass `None` for the txn argument and each
secondary write is committed individually:

```rust
secondary.update_secondary(None, &key, old_value.as_ref(), Some(&new_value))?;
```

This restores the v1.4 / v1.5.0-rc1 / v1.5.0-rc2 behaviour. It is
intentionally available because not every workload wants to allocate
a transaction for every write, and a one-shot population path has no
caller txn to thread through.

### What's still missing (v1.6)

`Database::put` does **not** automatically drive attached
secondaries. v1.5 has no `associate()` hook, so the application is
still responsible for calling `update_secondary` after every primary
`put` / `delete`. Forgetting to call it leaves the index stale; the
v1.6 automatic-association work removes that hazard by routing every
primary write through every attached secondary inside the same txn.

## Concurrency reminder

> **Note:** Secondary indexes change the lock-ordering shape of your
> workload — a write on the primary takes a lock on the primary, then
> `update_secondary` takes locks on the secondary under the *same*
> txn; a secondary cursor takes a lock on the secondary then chases
> the primary. Concurrent writers can deadlock, and the canonical
> retry loop from
> [Aborting a Transaction](basics.md#aborting-a-transaction) is
> required for the primary write *and* the secondary update — both
> ops are now part of the same transactional unit, so a deadlock on
> either side aborts the whole sequence.

---
