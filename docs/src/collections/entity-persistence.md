# Entity Persistence (DPL)

The Direct Persistence Layer (`noxu-persist`) lets you store and retrieve
Rust structs through a typed primary index instead of writing
`DatabaseEntry` byte slices by hand. It is a trait-based layer: you opt
your type in by implementing two traits — `Entity` and an
`EntitySerializer<E>` — and then use `PrimaryIndex<K, E>` and
`SecondaryIndex<SK, PK, E>` for typed reads and writes.

> **Note — no derive macros.** Earlier drafts of this chapter described
> `#[derive(Entity)]`, `#[primary_key]`, `#[secondary_key]`, and
> `#[transient]` annotations. Those macros are **not implemented**. The
> trait-based API documented here is the entire public surface.
> A proc-macro crate that generates the boilerplate may be added in the
> future; until then, all wiring is by trait impl.

## Defining an entity

Implement `Entity` for your struct, and provide an `EntitySerializer`
that turns the struct into bytes and back. The serializer is supplied
explicitly to every read/write call — the persistence layer does not
prescribe a wire format.

```rust
use noxu_persist::{Entity, EntitySerializer, PersistError, Result};

#[derive(Clone, Debug, PartialEq)]
struct User {
    id: u64,
    email: String,
    name: String,
}

impl Entity for User {
    /// Primary-key type; must implement `noxu_persist::PrimaryKey`.
    /// Built-in implementations cover all common scalars (`u32`, `u64`,
    /// `i32`, `i64`, `String`, `Vec<u8>`, …).
    type PrimaryKey = u64;

    fn primary_key(&self) -> &u64 {
        &self.id
    }

    /// Stable name used to derive the underlying database name
    /// (`<entity_name>:primary`) inside an `EntityStore`. Each entity
    /// type should return a unique, never-changing string.
    fn entity_name() -> &'static str {
        "User"
    }
}

/// Choose your own wire format. The example below uses
/// `bincode::serialize`, but any byte-in / byte-out scheme works.
struct UserSerializer;

impl EntitySerializer<User> for UserSerializer {
    fn serialize(&self, u: &User) -> Result<Vec<u8>> {
        bincode::serialize(u)
            .map_err(|e| PersistError::SerializationError(e.to_string()))
    }
    fn deserialize(&self, bytes: &[u8]) -> Result<User> {
        bincode::deserialize(bytes)
            .map_err(|e| PersistError::SerializationError(e.to_string()))
    }
}
```

For one-off projects, `noxu-persist::SimpleSerializer` is a
length-prefixed format that handles common scalar / `String` /
`Vec<u8>` field types via a `FieldEncoder` / `FieldDecoder` builder
pattern; see `crates/noxu-persist/src/simple_serializer.rs`.

## Opening an `EntityStore` and a `PrimaryIndex`

```rust
use noxu_db::{Environment, EnvironmentConfig};
use noxu_persist::{EntityStore, PrimaryIndex, StoreConfig};

let env = Environment::open(
    EnvironmentConfig::new("/var/lib/users".into())
        .with_allow_create(true)
        .with_transactional(true),
)?;

// An EntityStore owns one or more underlying Databases and is the
// factory for typed primary / secondary indexes.
let store_config = StoreConfig::new("user_store").with_allow_create(true);
let mut store = EntityStore::open(&env, store_config)?;

// Open the primary index for the User entity type.
let index: PrimaryIndex<u64, User> = store.get_primary_index()?;
let ser = UserSerializer;
```

## Reading and writing entities

```rust
// Insert / update.
index.put(&ser, &User { id: 1, email: "a@b.com".into(), name: "Alice".into() })?;

// Lookup by primary key.
let user: Option<User> = index.get(&ser, &1u64)?;

// Delete (no secondary indexes touched — see delete_with_entity below).
let removed: bool = index.delete(&1u64)?;

// Iterate in primary-key order.
for user in index.iter(&ser) {
    let u: User = user?;
    println!("{u:?}");
}
```

The `index.put` and `index.delete_with_entity` calls automatically
update every secondary index that has been registered against this
primary index (see below). The plain `index.delete(&pk)` does not
fetch the entity and therefore cannot maintain secondary indexes; use
`delete_with_entity` whenever secondary maintenance is required.

## Secondary indexes

Secondary indexes are opened from the `PrimaryIndex` using a
*key-extractor* closure. The extractor is plain Rust — there is no
derive macro generating it.

```rust
use noxu_persist::SecondaryIndex;

// Open a secondary index keyed by email.
let by_email: SecondaryIndex<String, u64, User> =
    index.open_secondary_index(|u: &User| Some(u.email.clone()));

// Lookup by secondary key. The lookup goes through the registered
// PrimaryIndex to materialise the entity, so the call takes both.
let user: Option<User> = by_email.get(&ser, &index, &"a@b.com".to_string())?;
```

The extractor returns `Option<SK>`: `None` means "this entity has no
secondary key for this index" (think SQL `NULL`), and the entity is
omitted from that index without error.

`SecondaryIndex` supports the same shape of operations as the Java
Edition's `SecondaryDatabase`: `get`, `contains`, `delete`,
`iter`, `iter_from`, `keys_index`, and `sub_index`. Many-to-one is
modelled by having multiple primary keys map to the same secondary
key — the underlying map is `BTreeMap<SK, BTreeSet<PK>>`.

## Schema evolution

The persistence layer does not store a schema with each record — the
serializer you supply is responsible for parsing whatever bytes are
on disk. To migrate an entity type when its layout changes, use the
helpers in `noxu_persist::evolve`:

| Helper | Purpose |
|---|---|
| `Renamer` | Rename a field, leaving its bytes in place. |
| `Deleter` | Delete a field (its bytes are skipped on read). |
| `Converter` | Run a user-supplied closure on the deserialized old form to produce the new form. |
| `Mutations` / `EvolveConfig` | Compose the above into a single migration plan; pass to `EntityStore::open_evolved`. |

See `crates/noxu-persist/src/evolve/` for the concrete API.

## Sequences

For numeric primary keys you don't want to assign by hand,
`noxu_persist::sequence::Sequence` provides a thread-safe counter
that is persisted in the same environment. `MemorySequence` is an
in-memory variant for tests.

## Limitations and roadmap

- No `#[derive(Entity)]` macro yet; each type needs explicit `Entity`
  and `EntitySerializer<E>` impls.
- The serializer is a runtime parameter on every read/write call; it
  is not stored alongside the data. Replacing the serializer for a
  given entity type requires a schema-evolution migration.
- `delete(&pk)` cannot maintain secondary indexes; prefer
  `delete_with_entity`.
- Secondary indexes are in-memory `BTreeMap`s rebuilt by the
  `PrimaryIndex` registration. They are not persisted independently —
  see `crates/noxu-persist/src/secondary_index.rs` for the design
  notes.
