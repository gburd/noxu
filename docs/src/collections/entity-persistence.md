# Entity Persistence (DPL)

> **v1.5 capability matrix:** see
> [Introduction → v1.5 capability matrix](../introduction.md#v15-capability-matrix).
>
> **v1.5 limitations** are detailed in the
> ["v1.5 limitations" section below](#v15-limitations) and in
> [`docs/src/internal/sprint-3-dpl-restriction.md`](../internal/sprint-3-dpl-restriction.md).
> Headlines: secondary indexes are in-memory only; secondary updates
> are not atomic with the user txn; primary writes do thread `txn`
> through correctly (Sprint 3B).

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

## Schema evolution (Wave 2C-2)

Noxu DB v1.6 wires schema evolution into the **open path** of
`EntityStore`.  When you call `EntityStore::open` with a non-empty
[`Mutations`](#mutations) attached to the [`StoreConfig`], the first
`get_primary_index<E>()` call for each entity class compares the
user-supplied `E::class_version()` against the persistent class
catalog and **streams** evolution under a single transaction if they
differ.  The streamed path opens a cursor on the entity database,
decodes each record's per-record class-version envelope, applies the
matching mutation, and rewrites the record — all without
materialising the database into RAM.

### On-disk record format (BREAKING)

Starting with v1.6, every entity record is wrapped in a small envelope:

```text
[2-byte class_version BE]
[1-byte entity_class_tag_len]
[entity_class_tag bytes]    (UTF-8, length = tag_len, max 255 bytes)
[payload bytes]             (your EntitySerializer's serialize() output)
```

The payload is the bytes your `EntitySerializer::serialize()` emits.
The persistence layer adds and strips the envelope; user code is
unaffected for the common case.  The envelope is **breaking** vs.
pre-v1.6 entity stores — see [the migration
guide](../getting-started/migrating.md) for the dump-and-reload
procedure.

### Bumping the class version

Add a `class_version()` impl to your `Entity`:

```rust,ignore
impl Entity for User {
    type PrimaryKey = u64;
    fn primary_key(&self) -> &u64 { &self.id }
    fn entity_name() -> &'static str { "User" }
    fn class_version() -> u16 { 1 } // bumped from default 0
}
```

The default is `0`, so existing definitions need no change.  Bump
`class_version()` whenever you change the on-disk shape of the
entity (add / remove / rename fields, or change the way an existing
field is serialized).

### Mutation primitives

| Helper | What it does on the open path |
|---|---|
| `Renamer::for_class("OldName", v, "NewName")` | Records tagged `OldName` are read as `NewName`; the tag is rewritten on the next access (lazy). |
| `Renamer::for_field("Class", v, "oldField", "newField")` | Advisory — your `deserialize_versioned` switches on `class_version` and consults `Mutations` to translate field names. |
| `Deleter::for_class("Class", v)` | Every record of `Class` at version `v` is **deleted** in the streamed evolve. |
| `Deleter::for_field("Class", v, "field")` | Advisory — your `deserialize_versioned` skips the field when reading `v` records. |
| `Converter::for_class("Class", v, fn)` | Every record at version `v` is rewritten with `fn(old_payload) -> new_payload`. |
| `Converter::for_field(...)` | Advisory — your `deserialize_versioned` runs the converter on the field's bytes. |

Class-level Renamer / Deleter / Converter run **eagerly** during
open-path evolution.  Field-level mutations are exposed to your
`EntitySerializer::deserialize_versioned` so you can do **lazy**
field-level evolution on read without rewriting records.

### Mutations

Compose mutations into a `Mutations` set, attach it to the
`StoreConfig`:

```rust,ignore
use noxu_persist::evolve::{Mutations, Converter, Deleter, Renamer};
use noxu_persist::StoreConfig;

let mut mutations = Mutations::new();
// Class-level converter: bump v0 records of "User" to the new shape.
mutations.add_converter(Converter::for_class("User", 0, |old: &[u8]| {
    // Run your migration on the raw payload bytes.
    Some(transform_v0_to_v1(old))
}));
// Class rename: "Person" -> "User" at v0.
mutations.add_renamer(Renamer::for_class("Person", 0, "User"));
// Drop deprecated entity.
mutations.add_deleter(Deleter::for_class("Obsolete", 0));

let cfg = StoreConfig::new("users")
    .with_allow_create(true)
    .with_transactional(true)
    .with_mutations(mutations);
```

### Field-level evolution via `deserialize_versioned`

For lazy field-level evolution (renamers / deleters that don't
require rewriting old records), override
`EntitySerializer::deserialize_versioned`:

```rust,ignore
impl EntitySerializer<UserV1> for UserV1Ser {
    fn serialize(&self, e: &UserV1) -> Result<Vec<u8>> { /* v1 layout */ }
    fn deserialize(&self, b: &[u8]) -> Result<UserV1> { /* v1 layout */ }

    fn deserialize_versioned(
        &self,
        bytes: &[u8],
        class_version: u16,
        mutations: &Mutations,
    ) -> Result<UserV1> {
        match class_version {
            1 => self.deserialize(bytes),
            0 => decode_v0_then_upgrade(bytes, mutations),
            other => Err(PersistError::SerializationError(
                format!("unknown class_version {other}")
            )),
        }
    }
}
```

The `Mutations` reference is the same set you attached to the
`StoreConfig`; consult `mutations.get_renamer(...)` /
`mutations.get_deleter(...)` to drive field-level transforms.

### Idempotence and retries

The open-path evolution is **idempotent**: re-running it after a
successful evolve is a no-op (the catalog records `current_version`,
so the next open finds the catalog at-target and skips the scan).

If the streamed evolve fails midway — e.g. an I/O error, or a
registered `EvolveListener` returns `false` — the wrapping
transaction is aborted and the database is left in its pre-evolve
state.

### Explicit eager evolve

`EntityStore::evolve(&mut self, &mutations, &config)` is still
available for callers that want to drive evolution explicitly
(matches the JE `EntityStore.evolve(EvolveConfig)` shape).  Wave 2C-2
rewrote it to use the same streamed transactional path — it no
longer materialises the database into RAM.  Calling it twice is
harmless: the second call sees no records that match v0 mutations
and returns `EvolveStats { n_read: total, n_converted: 0 }`.

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
