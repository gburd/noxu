# StoredSet

Noxu DB v1.5 provides two `&[u8]`-keyed set views over a `Database`:

- **`StoredKeySet`** — set of keys (the database values are unused or
  treated as opaque payloads).
- **`StoredValueSet`** — collection of values stored under tracked keys
  (use this when iteration is by value, not by key).

> **v1.5 surface.**  Earlier drafts of this chapter showed a typed
> `StoredSet<K>` configured with `TupleBinding<K>` and a
> `set_sorted_duplicates(true)` database.  That shape is the v1.6
> target.  In v1.5 the type is byte-slice-keyed, the API takes
> `&[u8]`, and the underlying database is a normal primary index — no
> sorted-duplicate machinery is involved.

## Creating a StoredKeySet

```rust,ignore
use noxu_collections::StoredKeySet;
use noxu_db::{DatabaseConfig, Environment};

let db_config = DatabaseConfig::new().with_allow_create(true);
let db  = env.open_database(None, "tags", &db_config)?;
let set = StoredKeySet::new(&db);
```

## Operations

```rust,ignore
// Add a key (the value stored under it is empty by default).
set.add(b"rust")?;

// Test membership.
assert!(set.contains(b"rust")?);

// Remove.
set.remove(b"rust")?;

// Iterate (sorted by key bytes).
for key in set.iter()? {
    let key = key?;
    println!("{:?}", key);
}
```

For pre-existing data, register the keys you care about with
`register_key` / `register_keys` before iterating.

## v1.5 limitations

1. **Auto-commit only.**  Every operation issues the underlying
   `Database` call with `txn = None`.  Transactional semantics
   require driving the raw `Database` API directly.  (Audit findings
   #1, #3, #4.)

2. **No typed `StoredSet<K>`.**  Use `noxu-bind` to encode typed
   keys to bytes and pass the bytes to `StoredKeySet::add` /
   `contains` / `remove`.

3. **No sorted-duplicate machinery.**  `StoredKeySet` does not open
   the underlying database with `set_sorted_duplicates(true)`; the
   inner database is a plain primary index.  Multi-value-per-key
   storage is part of the v1.6 secondary-index work.
