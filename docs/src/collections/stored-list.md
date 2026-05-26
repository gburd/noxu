# StoredList

`StoredList` provides an indexed list backed by a Noxu database.  Indices
are 0-based `usize` values encoded as 8-byte big-endian keys, so
iteration order matches insertion order.

> **v1.5 surface.**  Earlier drafts of this chapter showed a typed
> `StoredList<V>` configured with a `Sequence` and an
> `EntryBinding<V>`.  That is the v1.6 target shape.  In v1.5 the
> type is `StoredList<'db>` and values are raw bytes; there is no
> `Sequence` involvement and no `EntryBinding`.

## Creating a StoredList

```rust,ignore
use noxu_collections::StoredList;
use noxu_db::{DatabaseConfig, Environment};

let db_config = DatabaseConfig::new().with_allow_create(true);
let db  = env.open_database(None, "events", &db_config)?;

// For a brand-new (or known-empty) database, `new` is fine:
let list = StoredList::new(&db);

// When reopening a database that may already contain entries,
// use `open` to recover the next-index counter from the database:
let list = StoredList::open(&db)?;
```

## Operations

```rust,ignore
// Append (returns the assigned index).
let idx = list.push(b"event data")?;

// Get by index.
let value: Option<Vec<u8>> = list.get(idx)?;

// Pop the highest index.
let last = list.pop()?;

// Remove a specific index (leaves a gap; see v1.5 limitations).
list.remove(idx)?;

// Length / emptiness.
let n = list.len()?;
let empty = list.is_empty()?;
```

## StoredList vs. StoredMap

`StoredList` auto-assigns sequential 8-byte big-endian integer keys
and tracks the next index internally.  Use it for ordered
append-style logs.  Use `StoredMap` when you control the key space.

## v1.5 limitations

These constraints are tracked by the May 2026 collections/bind API
audit and are scheduled for revisit in v1.6.

1. **Auto-commit only.**  Every operation issues the underlying
   `Database` call with `txn = None`.  Transactional semantics
   across `push` / `pop` / `remove` require driving the raw
   `Database` API directly.  (Audit findings #1, #3, #4.)

2. **`new` does not recover the next-index counter.**  `StoredList::new`
   sets `next_index = 0` regardless of what is already in the
   database.  If you call `new` against a database that already
   contains entries and then `push`, the new entries will overwrite
   the existing records at index 0, 1, 2, … silently.  Use
   [`StoredList::open`](#creating-a-storedlist) for the reopen-safe
   path; it scans the largest existing key and recovers
   `next_index` from it.  (Audit finding #6.)

3. **`remove` does not compact.**  `list.remove(idx)` performs a
   single-key delete: the slot at `idx` becomes empty, higher
   indices are *not* shifted down, and `next_index` is unchanged.
   `pop` decrements `next_index` only when it removes the very last
   element; `remove` of an arbitrary middle index does not.  This
   matches the BDB-JE `StoredContainer.removeKey()` shape but
   differs from the rustdoc claim shipped with earlier 1.5 release
   candidates.  (Audit finding #5.)

4. **Backed by a plain `Database`, not a `Sequence`.**  v1.5 uses a
   process-local counter recovered from the database's largest
   key.  Concurrent processes pushing to the same `StoredList`
   will race on the counter; for now use one writer at a time.
   The `Sequence`-backed shape moves with the v1.6 typed-API work.
