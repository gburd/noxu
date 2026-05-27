# StoredList

`StoredList<V, VB>` provides a typed indexed list backed by a Noxu
database.  Indices are 0-based `usize` values encoded as 8-byte
big-endian keys, so iteration order matches insertion order and keys
sort numerically.

## Creating a StoredList

```rust,ignore
use noxu_bind::StringBinding;
use noxu_collections::StoredList;
use noxu_db::{DatabaseConfig, Environment};

let db_config = DatabaseConfig::new().with_allow_create(true);
let db  = env.open_database(None, "events", &db_config)?;

// For a brand-new (or known-empty) database, `new` is fine:
let list: StoredList<String, _> = StoredList::new(&db, StringBinding);

// When reopening a database that may already contain entries,
// use `open` to recover the next-index counter from the database:
let list: StoredList<String, _> =
    StoredList::open(&db, StringBinding)?;
```

`StoredList::open` walks `Get::Last` once to recover `next_index`
from the largest existing 8-byte key.  It returns
`CollectionError::IllegalState` if the largest key is not 8 bytes
(i.e. the database wasn't produced by `StoredList`).

## Operations

```rust,ignore
// Append (returns the assigned index).
let idx: usize = list.push(None, &"event data".to_string())?;

// Get by index.
let value: Option<String> = list.get(None, idx)?;

// Pop the highest index.  Returns the value, or None if empty.
let last: Option<String> = list.pop(None)?;

// Remove a specific index — shift-down compaction.
let removed: Option<String> = list.remove(None, idx)?;

// Length / emptiness.
let n: usize = list.len(None)?;
let empty: bool = list.is_empty(None)?;

// Iterate (in index order).
for v in list.iter(None)? {
    println!("{}", v?);
}

// Clear all elements.
list.clear(None)?;
```

## Compaction (Wave 2B / v1.6)

`list.remove(idx)` performs **shift-down compaction**: every record
at index `i > idx` moves down to `i - 1`, and `next_index` is
decremented by 1.  After the call the list is dense again — there
are no gaps.

The whole shift is issued under the supplied `txn`, so:

- `list.remove(Some(&t), idx)` — every shift is part of `t`.
  Crash / abort rolls back the entire compaction atomically.
- `list.remove(None, idx)` — each shift is its own auto-txn.
  A crash mid-compaction can leave duplicate entries at the
  in-between slots.  Wrap in `TransactionRunner::run` for crash-
  atomic semantics.

Cost: `O(N - idx)` database operations per remove.  This matches
the BDB-JE `StoredList.remove(int index)` contract and
`Vec::remove` semantics.

## Threading a transaction

Every method takes `Option<&Transaction>`:

```rust,ignore
let txn = env.begin_transaction(None)?;
list.push(Some(&txn), &"first".to_string())?;
list.push(Some(&txn), &"second".to_string())?;
let len = list.len(Some(&txn))?;
txn.commit()?;
```

A typical pattern is to wrap a sequence of list operations in a
[`TransactionRunner`](../collections/index.html#v16-collections--whats-in-scope):

```rust,ignore
use noxu_collections::TransactionRunner;
let runner = TransactionRunner::new(&env);
runner.run(|txn| {
    list.push(Some(txn), &"a".to_string())?;
    list.push(Some(txn), &"b".to_string())?;
    list.remove(Some(txn), 0)?;       // shift-compaction inside the txn
    Ok(())
})?;
```

## StoredList vs. StoredMap

`StoredList` auto-assigns sequential 8-byte big-endian integer keys
and tracks the next index internally.  Use it for ordered append-
style logs.  Use `StoredMap` when you control the key space.

## Migrating from v1.5

The v1.5 byte-valued `StoredList<'db>` is gone.  Replace:

```rust,ignore
// v1.5
let list = StoredList::new(&db);
list.push(b"event")?;
let v: Option<Vec<u8>> = list.get(0)?;
list.remove(1)?;            // no compaction; left a hole

// v1.6
use noxu_bind::ByteArrayBinding;
let list: StoredList<Vec<u8>, _> = StoredList::new(&db, ByteArrayBinding);
list.push(None, &b"event".to_vec())?;
let v: Option<Vec<u8>> = list.get(None, 0)?;
list.remove(None, 1)?;      // compacts; index 2..N shift to 1..N-1
```

The compaction change is *behavioural*: code that depended on the
v1.5 "remove leaves a hole" contract will see different `get(idx)`
results after `remove`.
