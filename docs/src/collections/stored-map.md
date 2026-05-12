# StoredMap

`StoredMap<K, V>` provides a `BTreeMap`-like view over a Noxu primary database,
with keys and values automatically serialized via `noxu-bind` bindings.

## Creating a StoredMap

```rust
use noxu_collections::StoredMap;
use noxu_bind::{TupleBinding, EntryBinding};

let env = Environment::open(Path::new("./data"), EnvironmentConfig::default())?;
let db  = env.open_database(None, Some("users"), DatabaseConfig::default())?;

let map: StoredMap<u64, String> = StoredMap::new(
    db,
    TupleBinding::<u64>::new(),
    EntryBinding::<String>::new(),
);
```

## Operations

```rust
// Insert
map.put(txn, &42u64, &"Alice".to_string())?;

// Get
let value: Option<String> = map.get(txn, &42u64)?;

// Remove
map.remove(txn, &42u64)?;

// Iterate (ascending key order)
for (k, v) in map.iter(txn)? {
    println!("{k}: {v}");
}

// Range scan
for (k, v) in map.range(txn, &10u64..&50u64)? {
    println!("{k}: {v}");
}
```

## Sorted Semantics

Keys are sorted by the natural order of the binding's byte representation.
`TupleBinding<u64>` uses big-endian encoding, which preserves numeric sort
order. `TupleBinding<String>` uses UTF-8 byte order (lexicographic).
