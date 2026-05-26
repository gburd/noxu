# Secondary Indices with Transactions

> **v1.5 capability matrix:** see
> [Introduction → v1.5 capability matrix](../introduction.md#v15-capability-matrix).
>
> **v1.5 limitations:** see
> [Getting Started → Secondary Databases → v1.5 limitations](../getting-started/secondary-databases.md#v15-limitations).
> v1.5 secondaries are one-to-one (Decision 1B); foreign-key constraints
> are rejected at `SecondaryDatabase::open` (Decision 2C); the
> BDB-JE-style `associate()` hook and `populate_secondaries` are **not**
> implemented in v1.5 (planned for v1.6); and `update_secondary` runs
> auto-committed even when the primary write is under a user
> transaction. **You cannot make a primary write and its secondary
> update atomic in v1.5.** This chapter documents the partial-atomicity
> contract honestly so you can decide whether v1.5 secondaries are
> acceptable for your workload.

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
* `secondary.update_secondary(pri_key, old_data, new_data)` —
  application-driven secondary maintenance. **Does not take a
  transaction**: it runs auto-committed, even when the surrounding
  primary write is under an explicit user txn. v1.5 has no
  `associate()` hook, so the application must call
  `update_secondary` after every primary `put` / `delete`.

The atomicity gap is the same shape as the in-memory DPL secondary
limitation tracked in
[`docs/src/internal/sprint-3-dpl-restriction.md`](../internal/sprint-3-dpl-restriction.md);
both close together in v1.6 alongside Decision 1's sorted-dup +
`associate` work.

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

## Read path: cursors do honour the transaction

Secondary reads under a user transaction work as expected. Open the
cursor with `Some(&txn)` and close it before committing or aborting:

```rust
use noxu_db::{Get, OperationStatus};

let txn = env.begin_transaction(None, None)?;
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

## Write path: best-effort atomicity in v1.5

Because `update_secondary` is auto-committed, the recommended pattern
is to commit the primary first under your txn, then call
`update_secondary` from outside the txn:

```rust
use noxu_db::DatabaseEntry;

let key = DatabaseEntry::from_bytes(b"alice");
let new_value = DatabaseEntry::from_bytes(b"Engineering|Senior Engineer");

// Read the previous primary record (auto-commit) so we know the old
// secondary key.
let old_value = primary.lock().get(None, &key)?;

// Primary write under txn.
let txn = env.begin_transaction(None, None)?;
primary.lock().put(Some(&txn), &key, &new_value)?;
txn.commit()?;

// Secondary maintenance runs auto-committed. If this step fails the
// primary is already committed and the secondary will be inconsistent
// until you retry. Plan for crash-mid-update in your application
// recovery.
secondary.update_secondary(&key, old_value.as_ref(), Some(&new_value))?;
```

If `update_secondary` returns `NoxuError::Unsupported`, two distinct
primary records have produced the same secondary key. v1.5 secondaries
are one-to-one (Decision 1B) and reject the second insert at the
secondary layer rather than silently overwriting the first; the primary
is already committed. Your application is responsible for compensating
(re-keying the new primary, deleting it, or surfacing the conflict).
Sorted-dup secondaries that accept many-to-one mappings are scheduled
for v1.6.

If you need stricter atomicity — primary and secondary writes that
commit or abort together — wait for v1.6 or maintain the secondary
yourself in a way that tolerates the gap (e.g. periodic full
re-population from the primary).

## Concurrency reminder

> **Note:** Secondary indexes change the lock-ordering shape of your
> workload — a write on the primary takes a lock on the primary, then
> the auto-committed `update_secondary` takes a lock on the secondary;
> a secondary cursor takes a lock on the secondary then chases the
> primary. Concurrent writers can deadlock, and the canonical
> retry loop from
> [Aborting a Transaction](basics.md#aborting-a-transaction) is
> required for the primary write.

---
