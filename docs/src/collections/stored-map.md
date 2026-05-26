# StoredMap

`StoredMap` provides a `BTreeMap`-like view over a Noxu primary database
where keys and values are raw byte slices (`&[u8]`). Use it when you want
familiar collection ergonomics without writing the cursor / `DatabaseEntry`
boilerplate by hand.

> **v1.5 surface.**  The actual `StoredMap` is _not_ generic over typed
> `K` / `V` parameters yet.  Earlier drafts of this chapter showed
> `StoredMap<K, V>` with `TupleBinding<K>` / `EntryBinding<V>` arguments;
> that is the v1.6 target shape.  In v1.5 the type is `StoredMap<'db>`
> and the operations take and return `&[u8]` / `Vec<u8>`.

## Creating a StoredMap

```rust,ignore
use noxu_collections::StoredMap;
use noxu_db::{DatabaseConfig, Environment, EnvironmentConfig};

let env = Environment::open(env_config)?;
let db_config = DatabaseConfig::new().with_allow_create(true);
let db  = env.open_database(None, "users", &db_config)?;

// Second argument is the read-only flag.
let map = StoredMap::new(&db, /* read_only = */ false);
```

## Operations

```rust,ignore
// Insert (returns the previous value, if any).
map.put(b"alice", b"alice@example.com")?;

// Get (returns Option<Vec<u8>>).
let value = map.get(b"alice")?;
assert_eq!(value, Some(b"alice@example.com".to_vec()));

// Remove (returns the previous value, if any).
map.remove(b"alice")?;

// Contains.
assert!(!map.contains_key(b"alice")?);

// Size / emptiness.
let n = map.len()?;            // u64
let empty = map.is_empty()?;   // bool

// Iterate (sorted by key bytes).
for entry in map.iter()? {
    let (k, v) = entry?;
    println!("{:?} -> {:?}", k, v);
}
```

For pre-existing data, populate the internal key index before
iterating (see the `register_key` / `register_keys` methods on
`StoredMap`).

## Sorted semantics

Keys sort by raw byte order.  If you need numeric / signed-integer
sort order, encode keys with `noxu-bind` (`IntBinding`,
`LongBinding`, `SortedDoubleBinding`, …) and pass the resulting
bytes to `put` / `get`.  See [the bindings chapter](../getting-started/bindings.md)
for the exact encodings.

## v1.5 limitations

These constraints are tracked by the May 2026 collections/bind API
audit.  All of them are scheduled for revisit in v1.6.

1. **Auto-commit only.**  Every `StoredMap` operation issues the
   underlying `Database` call with `txn = None`.  There is no way to
   thread an externally-begun `noxu_db::Transaction` into a
   `StoredMap` method.  If you need transactional semantics across
   several writes, drive the raw `Database::put` / `Database::delete`
   API directly with `Some(&txn)`.  (Audit findings #1, #3, #4.)

2. **`StoredMap<K, V>` typed shape is not implemented yet.**  The
   v1.5 type is byte-slice-keyed; the typed shape moves with the
   v1.6 work that also fixes (1).
