# StoredList

`StoredList<V>` provides an indexed list backed by a Noxu database with
integer keys allocated from a `Sequence`.

## Creating a StoredList

```rust
use noxu_collections::StoredList;
use noxu_bind::EntryBinding;

let db  = env.open_database(None, Some("events"), DatabaseConfig::default())?;
let seq = env.open_sequence(None, "events_seq", SequenceConfig::default())?;

let list: StoredList<String> = StoredList::new(db, seq, EntryBinding::<String>::new());
```

## Operations

```rust
// Append (returns the assigned index)
let idx = list.push(txn, &"event data".to_string())?;

// Get by index
let value: Option<String> = list.get(txn, idx)?;

// Remove by index
list.remove(txn, idx)?;

// Iterate (ascending index order)
for (idx, value) in list.iter(txn)? {
    println!("{idx}: {value}");
}
```

## StoredList vs. StoredMap

`StoredList` auto-assigns integer keys via a `Sequence`. Use it when you
need an ordered append-only log or event stream. Use `StoredMap` when you
control the key space.
