# StoredSet

Noxu DB provides two typed set views over a `Database`:

- **`StoredKeySet<K, KB>`** — set of typed keys (the database's value
  payload is empty).
- **`StoredValueSet<V, VB>`** — typed collection of database values,
  iterated in cursor walk order.

Both views are stateless: every `contains` / `len` / `iter` call goes
to the database.

## Creating a StoredKeySet

```rust,ignore
use noxu::bind::IntBinding;
use noxu::collections::StoredKeySet;
use noxu::{DatabaseConfig, Environment};

let db_config = DatabaseConfig::new().with_allow_create(true);
let db  = env.open_database(None, "tags", &db_config)?;
let set: StoredKeySet<i32, _> = StoredKeySet::new(&db, IntBinding);
```

## Operations

```rust,ignore
// Add a key.  Returns true if newly inserted, false if already present.
let added: bool = set.add(None, &42)?;

// Test membership.
let present: bool = set.contains(None, &42)?;

// Remove.  Returns whether the key was present.
let removed: bool = set.remove(None, &42)?;

// Iterate (sorted by the key binding's natural order).
for key in set.iter(None)? {
    println!("{}", key?);
}

// Length / emptiness.
let n: usize = set.len(None)?;
let empty: bool = set.is_empty(None)?;

// Clear all.
set.clear(None)?;
```

## Threading a transaction

Every method takes `Option<&Transaction>`:

```rust,ignore
let txn = env.begin_transaction(None)?;
set.add(Some(&txn), &42)?;
assert!(set.contains(Some(&txn), &42)?);
txn.abort()?;                // rolled back
assert!(!set.contains(None, &42)?);
```

## StoredValueSet

`StoredValueSet<V, VB>` exposes a typed view focused on the values
stored in the database.  Iteration walks the cursor and decodes
each value via the supplied binding:

```rust,ignore
use noxu::bind::StringBinding;
use noxu::collections::StoredValueSet;

let vs: StoredValueSet<String, _> = StoredValueSet::new(&db, StringBinding);

for value in vs.iter(None)? {
    println!("{}", value?);
}

// Linear-scan membership check (O(N)).
let has_alpha: bool = vs.contains(None, &"alpha".to_string())?;
```

## Migrating from v1.5

The v1.5 byte-keyed `StoredKeySet<'db>` / `StoredValueSet<'db>` are
gone.  Replace:

```rust,ignore
// v1.5
let ks = StoredKeySet::new(&db);
ks.contains(b"key")?;
ks.register_keys(&[b"a", b"b", b"c"]);   // register_key/known_keys removed

// v1.6
use noxu::bind::ByteArrayBinding;
let ks: StoredKeySet<Vec<u8>, _> = StoredKeySet::new(&db, ByteArrayBinding);
ks.contains(None, &b"key".to_vec())?;
// No registration step — `iter()` walks the database directly.
```
