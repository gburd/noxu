# StoredSet

`StoredSet<K>` provides set semantics backed by a sorted-duplicate database.
Each key appears at most once; uniqueness is enforced by the underlying
database configuration.

## Creating a StoredSet

```rust
use noxu_collections::StoredSet;
use noxu_bind::TupleBinding;

let mut db_config = DatabaseConfig::default();
db_config.set_sorted_duplicates(true);

let db  = env.open_database(None, Some("tags"), db_config)?;
let set: StoredSet<String> = StoredSet::new(db, TupleBinding::<String>::new());
```

## Operations

```rust
// Insert
set.add(txn, &"rust".to_string())?;

// Contains
let exists = set.contains(txn, &"rust".to_string())?;

// Remove
set.remove(txn, &"rust".to_string())?;

// Iterate (sorted order)
for key in set.iter(txn)? {
    println!("{key}");
}
```
