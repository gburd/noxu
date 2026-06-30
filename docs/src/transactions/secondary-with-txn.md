# Secondary Indices with Transactions

> **v1.6 capability matrix:** see
> [Introduction → v1.6 capability matrix](../introduction.md#v15-capability-matrix).
>
> **v1.6 update:** secondaries are now sorted-dup (Decision 1B / audit
> C4), the primary database automatically maintains every registered
> `SecondaryDatabase` under the caller's transaction (audit C3),
> and foreign-key constraints (Abort, Cascade, Nullify) are enforced
> end-to-end (Decision 2C / audit C2). The pre-v1.6 caveats listed
> here in v1.5 are closed.

## What v1.6 actually does

* `SecondaryDatabase::open(primary, inner_secondary_db, sec_config)`
  — opens a secondary view over an already-open secondary `Database`.
  Both the primary and the inner secondary `Database` must be opened
  with `DatabaseConfig::with_transactional(true)` on a transactional
  `Environment`. The inner secondary DB additionally **must** be
  opened with `with_sorted_duplicates(true)` so multiple primaries
  may share a secondary key.
* `secondary.open_cursor(Some(&txn), config)` — secondary reads
  participate in `txn` correctly.  Cursor operations on a secondary
  acquire locks on behalf of the transaction.
* `secondary.update_secondary(Some(&txn), pri_key, old_data, new_data)`
  — manual maintenance for population paths.  Application code that
  goes through `Database::put` / `Database::delete` no longer has to
  call this: every registered secondary is fanned out automatically
  under the caller's txn.

## Read path: cursors honour the transaction

Secondary reads under a user transaction work as expected. Open the
cursor with `Some(&txn)` and close it before committing or aborting:

```rust
use noxu::{Get, OperationStatus};

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

## Write path: automatic primary + secondary atomicity (v1.6)

The simplest pattern is to drive the primary directly: every registered
secondary is updated under the same caller-supplied txn, so commit /
abort is atomic across the primary record and every index entry.

```rust
use noxu::DatabaseEntry;

let key = DatabaseEntry::from_bytes(b"alice");
let new_value = DatabaseEntry::from_bytes(b"Engineering|Senior Engineer");

// Read the previous primary record so we know the old secondary key.
// (Reads can be auto-commit or under the same txn; either works.)
let old_value = primary.lock().get(&key)?;

let txn = env.begin_transaction(None)?;

// Primary put under txn — auto-maintenance walks every registered
// secondary under the same `txn`.
primary.lock().put_in(&txn, &key, &new_value)?;

txn.commit()?;
```

Why this is atomic:

* `Database::put(Some(&t), …)` acquires a write lock on the primary
  record's slot under `t`'s locker.
* The fan-out walks `Database::live_secondaries()` and calls
  `SecondaryHook::maintain(Some(&t), …)`, opening the inner cursor
  over the secondary index *with the same `t`*. Sorted-dup
  `Put::NoDupData` (insert) and `SearchBoth + cursor.delete` (delete)
  acquire their locks under `t`.
* `t.commit()` flushes both the primary log entry and the secondary
  log entries as a unit; `t.abort()` rolls them back together.

The same fan-out covers the **update** case — `Database::put` reads
the pre-put value under the caller's txn before the overwrite, so the
secondary key creator can compute every stale `(sec_key, pri_key)`
pair to delete in addition to inserting the new ones.

## Manual update path (population, external feeds)

When you want to maintain a secondary index by hand — for example,
because you are populating from an offline data feed and there is no
primary write to drive the fan-out — `SecondaryDatabase::update_secondary`
remains available and still honours the caller's txn:

```rust
let txn = env.begin_transaction(None, None)?;
secondary.update_secondary(
    Some(&txn),
    &pri_key,
    old_value.as_ref(),
    Some(&new_value),
)?;
txn.commit()?;
```

The same atomicity contract applies: pass the same `txn` you used for
any primary write to make all sides commit / abort together; pass
`None` to run the secondary write auto-committed.

## Foreign-key constraints under a txn

When a foreign primary `delete` triggers `Cascade` / `Nullify`, every
mutation on the child primary records runs under the foreign delete's
caller-supplied txn. Aborting the foreign delete rolls back the
cascade or the nullification atomically.

```rust
let txn = env.begin_transaction(None, None)?;
match foreign.lock().delete_in(&txn, &fk) {
    Ok(_) => txn.commit()?,
    Err(NoxuError::ForeignConstraintViolation(_)) => {
        // Abort action surfaced — roll back so we are back to the
        // pre-delete state on every child primary too.
        txn.abort()?;
    }
    Err(e) => return Err(e),
}
```

## Concurrency reminder

> **Note:** Secondary indexes change the lock-ordering shape of your
> workload — a write on the primary takes a lock on the primary, then
> the auto-maintenance fan-out takes locks on every registered
> secondary under the *same* txn; a secondary cursor takes a lock on
> the secondary then chases the primary. Concurrent writers can
> deadlock, and the canonical retry loop from
> [Aborting a Transaction](basics.md#aborting-a-transaction) is
> required for the entire `primary.put` call (which now subsumes the
> secondary updates).

---
