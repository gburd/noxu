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
    /// (`<store_name>_<entity_name>`, e.g. `user_store_User`) inside an
    /// `EntityStore`. Each entity type should return a unique,
    /// never-changing string.
    fn entity_name() -> &'static str {
        "User"
    }
}

/// Choose your own wire format. The example below uses a length-prefixed
/// scheme via `noxu_persist::SimpleSerializer`, which avoids pulling in
/// `serde` for one-off projects. Any byte-in / byte-out scheme is fine —
/// e.g. `bincode`, `serde_json`, manual `byteorder`.
struct UserSerializer;

impl EntitySerializer<User> for UserSerializer {
    fn serialize(&self, u: &User) -> Result<Vec<u8>> {
        use noxu_persist::FieldEncoder;
        let mut enc = FieldEncoder::new();
        enc.write_u64(u.id);
        enc.write_string(&u.email);
        enc.write_string(&u.name);
        Ok(enc.finish())
    }
    fn deserialize(&self, bytes: &[u8]) -> Result<User> {
        use noxu_persist::FieldDecoder;
        let mut dec = FieldDecoder::new(bytes);
        Ok(User {
            id: dec.read_u64()?,
            email: dec.read_string()?,
            name: dec.read_string()?,
        })
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
// `get_primary_index` returns a `&mut`-borrowed `PrimaryIndex`, so bind
// it as `let mut` if you also want to register secondary indexes below.
let mut index: PrimaryIndex<u64, User> = store.get_primary_index()?;
let ser = UserSerializer;
```

## Reading and writing entities

Every `PrimaryIndex` operation takes a leading
`txn: Option<&Transaction>` argument. Pass `Some(&txn)` to participate
in a user transaction (the underlying database write commits or aborts
atomically with the txn). Pass `None` to keep the historical
auto-commit behaviour. This mirrors the
`noxu_db::Database::{get, put, delete}` shape and matches BDB-JE's
`PrimaryIndex` surface.

```rust
// Auto-commit (no surrounding txn).
index.put(
    None,
    &ser,
    &User { id: 1, email: "a@b.com".into(), name: "Alice".into() },
)?;

// Lookup by primary key (auto-commit).
let user: Option<User> = index.get(None, &ser, &1u64)?;

// Delete (auto-commit).  Does not fetch the entity — see
// `delete_with_entity` below.
let removed: bool = index.delete(None, &1u64)?;

// Iterate in primary-key order (auto-commit).
for user in index.entities(None, &ser)? {
    let u: User = user?;
    println!("{u:?}");
}
```

To participate in an explicit transaction:

```rust
use noxu_db::Environment;
# fn doc(env: &Environment) -> noxu_persist::Result<()> {
# use noxu_persist::{EntityStore, PrimaryIndex, StoreConfig, EntitySerializer};
# let mut store = EntityStore::open(env, StoreConfig::new("s").with_allow_create(true))?;
# let index: PrimaryIndex<u64, User> = store.get_primary_index()?;
# struct UserSerializer;
# impl EntitySerializer<User> for UserSerializer {
#     fn serialize(&self, _: &User) -> noxu_persist::Result<Vec<u8>> { Ok(vec![]) }
#     fn deserialize(&self, _: &[u8]) -> noxu_persist::Result<User> { unimplemented!() }
# }
# struct User { id: u64, email: String, name: String }
# impl noxu_persist::Entity for User {
#     type PrimaryKey = u64;
#     fn primary_key(&self) -> &u64 { &self.id }
#     fn entity_name() -> &'static str { "User" }
# }
# let ser = UserSerializer;
let txn = env.begin_transaction(None, None)?;
index.put(
    Some(&txn),
    &ser,
    &User { id: 2, email: "b@c.com".into(), name: "Bob".into() },
)?;
if /* application predicate */ true {
    txn.commit()?;
} else {
    txn.abort()?; // primary write is rolled back
}
# Ok(())
# }
```

The `index.put` and `index.delete_with_entity` calls automatically
update every secondary index that has been registered against this
primary index (see below). The plain `index.delete(txn, &pk)` does not
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
// PrimaryIndex to materialise the entity, so the call takes both, plus
// the optional transaction.
let user: Option<User> =
    by_email.get(None, &ser, &index, &"a@b.com".to_string())?;
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
| `Mutations` / `EvolveConfig` | Compose the above into a single migration plan. |

Migrations are applied via `EntityStore::evolve(&mut self,
&mutations, &config)`, which walks the store's databases and rewrites
each record through the registered `Renamer` / `Deleter` /
`Converter` mutations. See `crates/noxu-persist/src/evolve/` for the
concrete API.

## Sequences

For numeric primary keys you don't want to assign by hand,
`noxu_persist::sequence::Sequence` provides a thread-safe counter
that is persisted in the same environment. `MemorySequence` is an
in-memory variant for tests.

## Limitations and roadmap

### v1.5 limitations

- **Secondary indexes are in-memory only.** Entities with secondary
  keys (registered via `index.open_secondary_index(|e| ...)`) maintain
  the `secondary_key → primary_key` mapping in a process-local
  `BTreeMap<SK, BTreeSet<PK>>`. The mapping is **not** written to the
  underlying log and **does not survive a process restart** — it must
  be rebuilt by re-registering the index and replaying the primaries
  through the extractor. v1.6 will back secondaries with a real
  `Database` so the mapping is durable.
- **Primary-index writes can participate in transactions; secondary
  updates currently cannot.** Calling
  `index.put(Some(&txn), &ser, &entity)` correctly threads the txn
  through to the primary `Database::put`, but the in-memory secondary
  map is updated **immediately** — it is not rolled back if the
  caller later aborts the txn. The first such call against a primary
  with registered secondaries logs a one-shot
  `PersistError::SecondariesNotTransactional` warning so the
  limitation is operator-visible. v1.6 closes this gap together with
  the durability work above.
- See [`docs/src/internal/sprint-3-dpl-restriction.md`](../internal/sprint-3-dpl-restriction.md)
  for the full audit context, the rationale for shipping the
  in-memory secondaries unchanged in v1.5, and the v1.6 plan.

### Other roadmap items

- No `#[derive(Entity)]` macro yet; each type needs explicit `Entity`
  and `EntitySerializer<E>` impls.
- The serializer is a runtime parameter on every read/write call; it
  is not stored alongside the data. Replacing the serializer for a
  given entity type requires a schema-evolution migration.
- `delete(txn, &pk)` cannot maintain secondary indexes; prefer
  `delete_with_entity(txn, &ser, &pk)`.
