# StoredMap

`StoredMap<K, V, KB, VB>` provides a `BTreeMap`-like view over a Noxu
primary database, parameterised by [`noxu_bind::EntryBinding`]
implementations for keys and values.  Use it when you want familiar
collection ergonomics with typed Rust keys and values, without writing
the cursor / `DatabaseEntry` boilerplate by hand.

The `StoredMap` is *stateless* — it holds a reference to the database
and the bindings, but no in-process record of "what's in the map".
Every `len()`, `iter()`, `contains_key()` call goes to the database.

## Creating a StoredMap

```rust,ignore
use noxu_bind::{IntBinding, StringBinding};
use noxu_collections::StoredMap;
use noxu_db::{DatabaseConfig, Environment, EnvironmentConfig};

let env = Environment::open(env_config)?;
let db_config = DatabaseConfig::new().with_allow_create(true);
let db  = env.open_database(None, "users", &db_config)?;

let map: StoredMap<i32, String, _, _> =
    StoredMap::new(&db, IntBinding, StringBinding);

// Or read-only:
let ro: StoredMap<i32, String, _, _> =
    StoredMap::new_read_only(&db, IntBinding, StringBinding);
```

The `_` placeholders ask Rust to infer the binding types from the
`IntBinding` / `StringBinding` arguments.

## Operations

```rust,ignore
// Insert (returns the previous value, if any).
let old: Option<String> = map.put(None, &1, &"alice".to_string())?;

// Get (returns Option<V>).
let value: Option<String> = map.get(None, &1)?;
assert_eq!(value, Some("alice".to_string()));

// Remove (returns the previous value, if any).
let removed: Option<String> = map.remove(None, &1)?;

// Contains.
let present: bool = map.contains_key(None, &1)?;

// Size.
let n: usize = map.len(None)?;
let empty: bool = map.is_empty(None)?;

// Iterate.  Returns `StoredIterator<(K, V)>` materialised eagerly
// at the call to `iter()`.
for entry in map.iter(None)? {
    let (k, v) = entry?;
    println!("{} -> {}", k, v);
}

// Keys / values only.
for key in map.keys(None)? { /* ... */ }
for value in map.values(None)? { /* ... */ }

// Clear all records.
map.clear(None)?;
```

## Threading a transaction

Pass `Some(&txn)` to any method to make it participate in `txn`:

```rust,ignore
let txn = env.begin_transaction(None)?;

map.put(Some(&txn), &1, &"alpha".to_string())?;
map.put(Some(&txn), &2, &"beta".to_string())?;
let alpha = map.get(Some(&txn), &1)?;
// ... visible to other reads under txn, invisible to other txns ...

txn.commit()?;
```

Every method accepts `Option<&Transaction>`, including the iterator
constructors (`iter` / `keys` / `values` / `iter_from`).  The
iterator is materialised at call time under `txn`, so concurrent
modifications after the iterator is constructed are *not* reflected.

## Sorted semantics

Iteration order is the natural order of the on-disk byte
representation, which depends on the binding.  Bindings in
`noxu-bind`:

| Binding | Sorts in… |
|---|---|
| `IntBinding`, `LongBinding`, `ShortBinding`, `ByteBinding` | numeric order (signed two's-complement; sign bit flipped on disk) |
| `BoolBinding` | `false < true` |
| `StringBinding` | UTF-8 lexicographic |
| `SortedFloatBinding`, `SortedDoubleBinding` | numeric order |
| `FloatBinding`, `DoubleBinding` | raw IEEE 754 — **not** numeric |
| `ByteArrayBinding` | byte-lex (raw `Vec<u8>`) |
| `RecordNumberBinding` | numeric (big-endian `u64`) |
| `SortedPackedIntBinding`, `SortedPackedLongBinding` | numeric |

If you need a numeric-sort `f64` key, use `SortedDoubleBinding`, not
`DoubleBinding`.  The plain `*Binding` types are length-efficient but
do not sort numerically.

## Migrating from v1.5

The v1.5 `StoredMap<'db>` byte-keyed type is gone.  Replace:

```rust,ignore
// v1.5
let map = StoredMap::new(&db, false);
map.put(b"key", b"value")?;
let v: Option<Vec<u8>> = map.get(b"key")?;

// v1.6
use noxu_bind::ByteArrayBinding;
let map: StoredMap<Vec<u8>, Vec<u8>, _, _> =
    StoredMap::new(&db, ByteArrayBinding, ByteArrayBinding);
map.put(None, &b"key".to_vec(), &b"value".to_vec())?;
let v: Option<Vec<u8>> = map.get(None, &b"key".to_vec())?;
```

`ByteArrayBinding` reproduces the v1.5 byte-slice semantics
verbatim.  See [the migration chapter](../getting-started/migrating.md#wave-2b--collections-typed-api-and-txn-threading)
for the full before/after.
