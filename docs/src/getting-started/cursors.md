# Cursors

> **v1.5 capability matrix:** see
> [Introduction → v1.5 capability matrix](../introduction.md#v15-capability-matrix).
>
> **v1.5 cursor contract — highlights:**
>
> * `Database::open_cursor(Some(&txn), …)` and
>   `SecondaryDatabase::open_cursor(Some(&txn), …)` correctly thread
>   the supplied transaction through to the underlying cursor in v1.5.
>   Pre-1.5 release candidates silently ignored the
>   transaction; if you are upgrading, see
>   [Migrating from v1.4.x](migrating.md) for the lock-conflict
>   surface that change can expose.
> * `Get::SearchLte`, `Get::FirstDup`, and `Get::LastDup` are implemented.
>   `Get::SearchLte` positions on the largest key `<=` the search key (the
>   floor), returning `NotFound` only when no such key exists.
>   `Get::FirstDup` / `Get::LastDup` position on the first / last duplicate
>   of the current key without leaving the current dup set.
> * `Get::NextDup` and `Get::PrevDup` on a non-duplicates database
>   return `NotFound` (consistent with the no-dups invariant).
> * `Get::SearchBoth` on a non-duplicates database now validates the
>   data argument (a non-matching data returns `NotFound`).

## What is a Cursor?

A cursor is a position marker that can move through a database's records in sorted key order.
Cursors allow you to:

* Iterate forward or backward through all records.
* Seek to a specific key or to the nearest key that is greater-than-or-equal to a target.
* Insert, update, or delete records at the current cursor position.

Cursors are the primary tool for bulk reads, range scans, and operating on databases that allow
duplicate keys.

## Opening and Closing Cursors

```rust
let mut cursor = db.open_cursor(None)?;

// ... use cursor ...

cursor.close()?;
```

The first argument is an optional transaction. The second is an optional `CursorConfig`. Both are
typically `None` for simple use cases.

The transaction argument **is honoured** in v1.5: passing
`Some(&txn)` causes the cursor's reads and writes to acquire locks on
behalf of `txn`, and the cursor must be closed before `txn.commit()` /
`txn.abort()`. Passing `None` opens an auto-commit cursor whose writes
still go through the lock manager but whose locks are
released per-operation.

Cursors must be closed before the database they belong to is closed. Failing to close cursors
before closing a database returns an error.

## Navigating with Get

All cursor navigation is done through a single method with a `Get` enum that specifies the movement:

```rust
use noxu::Get;

let mut key  = DatabaseEntry::new();
let mut data = DatabaseEntry::new();

let status = cursor.get(&mut key, &mut data, Get::First, None)?;
```

The `Get` variants:

| Variant | Behavior | v1.5 |
|---|---|---|
| `Get::First` | Move to the first record (smallest key) | ✅ |
| `Get::Last` | Move to the last record (largest key) | ✅ |
| `Get::Next` | Move to the next record | ✅ |
| `Get::Prev` | Move to the previous record | ✅ |
| `Get::Search` | Move to the record with exactly the given key | ✅ |
| `Get::SearchBoth` | Position to the exact `(key, data)` pair (validates data on non-dup DBs) | ✅ |
| `Get::SearchGte` | Move to the first record with key >= the given key | ✅ |
| `Get::SearchRange` | Alias for `SearchGte` | ✅ |
| `Get::Current` | Re-read the record at the current position | ✅ |
| `Get::NextDup` / `Get::PrevDup` | Next/previous duplicate of the current key | ✅ on sorted-dup DBs; on non-dup DBs they return `NotFound` |
| `Get::SearchLte` | Largest key <= search key (the "floor"); `NotFound` when no key <= search key | ✅ |
| `Get::FirstDup` / `Get::LastDup` | First / last duplicate of the current key (by data order), positioned WITHIN the current dup set | ✅ on sorted-dup DBs; no-op on non-dup DBs |

For `Search`, `SearchGte`, and `SearchRange`, the key to search for must be placed in the key
`DatabaseEntry` before calling `get`. After a successful `Search` the key entry holds the found
key; after `SearchGte` the key entry holds the actual key found (which may be greater than the
search key).

## Forward Iteration

```rust
let mut cursor = db.open_cursor(None)?;
let mut key  = DatabaseEntry::new();
let mut data = DatabaseEntry::new();

let mut status = cursor.get(&mut key, &mut data, Get::First, None)?;
while status == OperationStatus::Success {
    println!(
        "{} = {}",
        std::str::from_utf8(key.data())?,
        std::str::from_utf8(data.data())?
    );
    status = cursor.get(&mut key, &mut data, Get::Next, None)?;
}
cursor.close()?;
```

## Reverse Iteration

```rust
let mut cursor = db.open_cursor(None)?;
let mut key  = DatabaseEntry::new();
let mut data = DatabaseEntry::new();

let mut status = cursor.get(&mut key, &mut data, Get::Last, None)?;
while status == OperationStatus::Success {
    println!("{} = {}", std::str::from_utf8(key.data())?, std::str::from_utf8(data.data())?);
    status = cursor.get(&mut key, &mut data, Get::Prev, None)?;
}
cursor.close()?;
```

## Searching for a Specific Key

```rust
let mut cursor = db.open_cursor(None)?;
let mut search_key = DatabaseEntry::from_bytes(b"carol");
let mut data = DatabaseEntry::new();

let status = cursor.get(&mut search_key, &mut data, Get::Search, None)?;
if status == OperationStatus::Success {
    println!("Found: {}", std::str::from_utf8(data.data())?);
} else {
    println!("Not found");
}
cursor.close()?;
```

## Range Scan (Greater-Than-Or-Equal Search)

`Get::SearchGte` (or its alias `Get::SearchRange`) positions the cursor at the first record with
a key that is greater than or equal to the search key. This is the key primitive for prefix and
range scans:

```rust
let mut cursor = db.open_cursor(None)?;
let mut range_key = DatabaseEntry::from_bytes(b"user:m");  // start of range
let mut data = DatabaseEntry::new();

let mut status = cursor.get(&mut range_key, &mut data, Get::SearchGte, None)?;
while status == OperationStatus::Success {
    let k = std::str::from_utf8(range_key.data())?;
    if !k.starts_with("user:") {
        break; // left the user: namespace
    }
    println!("{} = {}", k, std::str::from_utf8(data.data())?);
    status = cursor.get(&mut range_key, &mut data, Get::Next, None)?;
}
cursor.close()?;
```

## Floor Search (Less-Than-Or-Equal)

`Get::SearchLte` is the mirror of `SearchGte`: it positions the cursor on the
largest key that is less than or equal to the search key (the "floor"). It is
the primitive for "find the most recent entry at or before time T" style
queries. The operation returns `NotFound` only when no key `<=` the search
key exists (every key is larger, or the database is empty).

```rust
// keys {10, 20, 30}
let mut cursor = db.open_cursor(None)?;
let mut key = DatabaseEntry::from_bytes(b"25");
let mut data = DatabaseEntry::new();
let status = cursor.get(&mut key, &mut data, Get::SearchLte, None)?;
// status == Success, key now holds b"20" (the floor of 25)
cursor.close()?;
```

On a sorted-duplicates database, `SearchLte` lands on the *last* duplicate of
the floor key (the greatest record `<=` the search key).

## First / Last Duplicate

For sorted-duplicates databases, `Get::FirstDup` and `Get::LastDup` reposition
the cursor within the duplicate set of the current key, on the first or last
duplicate by data order, without leaving the current key. The cursor must
already be positioned on a record.

```rust
// key "k" has duplicates {a, b, c}
let mut cursor = db.open_cursor(None)?;
let mut key = DatabaseEntry::from_bytes(b"k");
let mut data = DatabaseEntry::new();
cursor.get(&mut key, &mut data, Get::Search, None)?;     // positioned on "k"
cursor.get(&mut key, &mut data, Get::FirstDup, None)?;   // data == b"a"
cursor.get(&mut key, &mut data, Get::LastDup, None)?;    // data == b"c"
cursor.close()?;
```

## Deleting via Cursor

`cursor.delete()` removes the record at the current cursor position. The cursor must have been
successfully positioned (i.e., the most recent `get` returned `Success`) before calling `delete`.

```rust
let mut cursor = db.open_cursor(None)?;
let mut search_key = DatabaseEntry::from_bytes(b"user:bob");
let mut data = DatabaseEntry::new();

if cursor.get(&mut search_key, &mut data, Get::Search, None)? == OperationStatus::Success {
    cursor.delete()?;
}
cursor.close()?;
```

## Writing via Cursor

`cursor.put` inserts or overwrites the record at the current cursor position. Use the `Put` enum
to control overwrite behavior:

```rust
use noxu::Put;

let key  = DatabaseEntry::from_bytes(b"user:dave");
let data = DatabaseEntry::from_bytes(b"Dave Brown, Finance");
cursor.put(&key, &data, Put::Overwrite)?;
```

`Put::Overwrite` replaces any existing record with the given key. `Put::NoOverwrite` returns
`OperationStatus::KeyExists` if the key already exists.

## Replacing Data via Cursor

To update the data for the current cursor position without changing the key:

```rust
// Position cursor on the record to update
let mut search_key = DatabaseEntry::from_bytes(b"user:alice");
let mut old_data = DatabaseEntry::new();
cursor.get(&mut search_key, &mut old_data, Get::Search, None)?;

// Replace the data
let new_data = DatabaseEntry::from_bytes(b"Alice Smith, VP Engineering");
cursor.put(&search_key, &new_data, Put::Overwrite)?;
```

## Important: Always Close Cursors

Cursors hold page locks. Open cursors consume resources and can block other threads. Always close
cursors as soon as you are done with them — preferably in a `defer`-style pattern or at the end of
a lexical scope using Rust's RAII.

---
