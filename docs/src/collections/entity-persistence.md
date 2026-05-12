# Entity Persistence (DPL)

The Direct Persistence Layer (`noxu-persist`) allows Rust structs to be
stored directly in Noxu databases without manual key/value encoding.
This is a port of `com.sleepycat.persist` in BDB JE.

## Defining an Entity

```rust
use noxu_persist::{Entity, PrimaryKey, SecondaryKey};

#[derive(Entity, serde::Serialize, serde::Deserialize)]
pub struct User {
    #[primary_key]
    pub id: u64,

    #[secondary_key(name = "by_email")]
    pub email: String,

    pub name: String,
}
```

## EntityStore

```rust
use noxu_persist::EntityStore;

let store = EntityStore::open(&env, "my_store", EntityStoreConfig::default())?;

// Insert
let txn = env.begin_transaction(None)?;
store.put(&txn, &User { id: 1, email: "a@b.com".into(), name: "Alice".into() })?;
txn.commit()?;

// Primary key lookup
let user: Option<User> = store.get_primary(None, &1u64)?;

// Secondary index lookup
let user: Option<User> = store.get_by_index(None, "by_email", &"a@b.com")?;
```

## Key Creation and Serialization

`#[primary_key]` fields are serialized using `TupleBinding` (sort-preserving
by default for standard numeric types). `#[secondary_key]` fields create
automatic secondary database entries that are updated whenever the entity
is written.

## Annotations

| Annotation | Description |
|---|---|
| `#[primary_key]` | Required: defines the unique entity identifier |
| `#[secondary_key(name = "...")]` | Creates a secondary index |
| `#[secondary_key(many_to_one)]` | Secondary index allowing duplicate values |
| `#[transient]` | Field is not persisted |
