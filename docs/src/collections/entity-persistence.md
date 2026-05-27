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
>
> **v1.6 (Wave 2C-1) update:** the `#[derive(Entity)]`,
> `#[derive(PrimaryKey)]`, and `#[derive(SecondaryKey)]` proc-macros
> are now implemented in the `noxu-persist-derive` crate (re-exported
> from `noxu-persist`). The annotation-style API documented in this
> chapter is therefore live; the manual trait-impl path is preserved
> as the [legacy/no-derive shape](#legacy-manual-trait-impl-path) for
> users that need to opt out (e.g. for crate-graph reasons or to write
> a custom `Entity` impl).

The Direct Persistence Layer (`noxu-persist`) lets you store and retrieve
Rust structs through a typed primary index instead of writing
`DatabaseEntry` byte slices by hand. You opt your type in by deriving
the `Entity` macro (and optionally `SecondaryKey`) and supplying an
`EntitySerializer<E>` impl that turns the struct into bytes and back.

## Defining an entity (with derive macros)

Annotate your struct with `#[derive(Entity)]` and (if it has secondary
indexes) `#[derive(SecondaryKey)]`. Mark the primary-key field with
`#[primary_key]` and each secondary-keyed field with
`#[secondary_key(name = "...", relate = ..., …)]`:

```rust
use noxu_persist::{Entity, EntitySerializer, PersistError, Result, SecondaryKey};

#[derive(Clone, Debug, PartialEq, Entity, SecondaryKey)]
struct User {
    /// Primary key.  Field type must implement `noxu_persist::PrimaryKey`
    /// (built-in for all common scalars + `String` / `Vec<u8>`).
    #[primary_key]
    id: u64,

    /// Unique secondary index — each user has exactly one email and the
    /// email is unique across the store.
    #[secondary_key(name = "by_email", relate = OneToOne)]
    email: String,

    /// Many-to-one foreign-key secondary index.  `Option<u64>` is
    /// auto-unwrapped: the secondary key type is `u64`, and entities
    /// with `dept_id == None` are simply omitted from the index
    /// (think SQL `NULL`).
    #[secondary_key(
        name = "by_dept",
        relate = ManyToOne,
        related_entity = "Department",
        on_related_entity_delete = NULLIFY
    )]
    dept_id: Option<u64>,

    name: String,
}

/// You still write the serializer by hand — serialization format is
/// orthogonal to the entity declaration.  Length-prefixed binary,
/// `bincode`, `serde_json`, etc. are all valid.
struct UserSerializer;

impl EntitySerializer<User> for UserSerializer {
    fn serialize(&self, u: &User) -> Result<Vec<u8>> {
        use noxu_persist::FieldEncoder;
        let mut enc = FieldEncoder::new();
        enc.write_u64(u.id);
        enc.write_string(&u.email);
        match u.dept_id {
            None => enc.write_u8(0),
            Some(d) => { enc.write_u8(1); enc.write_u64(d); }
        }
        enc.write_string(&u.name);
        Ok(enc.finish())
    }
    fn deserialize(&self, bytes: &[u8]) -> Result<User> {
        use noxu_persist::FieldDecoder;
        let mut dec = FieldDecoder::new(bytes);
        let id = dec.read_u64()?;
        let email = dec.read_string()?;
        let has_dept = dec.read_u8()?;
        let dept_id = if has_dept == 0 { None } else { Some(dec.read_u64()?) };
        let name = dec.read_string()?;
        Ok(User { id, email, dept_id, name })
    }
}
```

### Attribute reference

| Attribute | Where | Purpose |
|---|---|---|
| `#[derive(Entity)]` | struct | Implements `noxu_persist::Entity`. Requires exactly one `#[primary_key]` field. |
| `#[entity(name = "...")]` | struct | Overrides the entity-name (default = struct name). Used as part of the underlying database name. |
| `#[primary_key]` | field | Marks the primary-key field. The field's type becomes `Entity::PrimaryKey`. |
| `#[derive(PrimaryKey)]` | struct | Implements `noxu_persist::PrimaryKey` for a custom newtype or composite key struct. |
| `#[derive(SecondaryKey)]` | struct | For each `#[secondary_key(...)]` field, emits a typed `Foo::open_<name>_index` helper plus a `pub const SECONDARY_INDEXES` metadata table. |
| `#[secondary_key(name = "...", relate = ...)]` | field | Declares a secondary index over the field. `relate` is one of `OneToOne`, `ManyToOne`, `OneToMany`, `ManyToMany`. |
| `#[secondary_key(..., related_entity = "Foo")]` | field | Optional foreign-key reference to another entity class name. |
| `#[secondary_key(..., on_related_entity_delete = ...)]` | field | One of `Abort` (default), `Cascade`, `Nullify`. BDB-JE-style `ABORT` / `CASCADE` / `NULLIFY` upper-case spellings are also accepted. |

### Composite primary keys

A composite key is just a struct with `#[derive(PrimaryKey)]` whose
field types each already implement `PrimaryKey`:

```rust
use noxu_persist::PrimaryKey;

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, PrimaryKey)]
struct OrderKey {
    region: String,
    customer_id: u64,
}

#[derive(Clone, Debug, PartialEq, noxu_persist::Entity)]
struct Order {
    #[primary_key]
    key: OrderKey,
    total_cents: u64,
}
```

`PartialOrd + Ord` are required because the `PrimaryIndex` API
constrains the key type to `Ord`. The derive emits a length-prefixed
concatenation of each field's `to_bytes()`; field order in the struct
is the byte-lex sort order of the resulting key.

A **newtype** primary key (`struct UserId(u64);`) delegates directly
to the inner type's `PrimaryKey` impl, so the on-disk bytes are
identical to using `u64` directly — useful when you want type-safety
without a sort-order penalty.

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

// Open the primary index for the User entity type.  Bind as `let mut`
// so we can later register secondary indexes against it.
let mut index: PrimaryIndex<u64, User> = store.get_primary_index()?;
let ser = UserSerializer;

// Open every secondary index declared by `#[derive(SecondaryKey)]`
// in one line each — the helpers carry the typed `SK` parameter.
let by_email = User::open_by_email_index(&mut index);
let by_dept  = User::open_by_dept_index(&mut index);

// Inspect the compile-time metadata if you want to introspect schemas.
for spec in User::SECONDARY_INDEXES {
    println!("{}: relate={:?}, fk={:?}", spec.name, spec.relate, spec.related_entity);
}
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
    &User { id: 1, email: "a@b.com".into(), dept_id: Some(10), name: "Alice".into() },
)?;

// Lookup by primary key (auto-commit).
let user: Option<User> = index.get(None, &ser, &1u64)?;

// Lookup by secondary key.  The lookup goes through the registered
// PrimaryIndex to materialise the entity, so the call takes both, plus
// the optional transaction.
let alice: Option<User> = by_email.get(None, &ser, &index, &"a@b.com".into())?;

// Range scan by secondary key (ManyToOne).
let dept10: Vec<u64> = by_dept.sub_index(&10u64);

// Iterate primaries in primary-key order.
for user in index.entities(None, &ser)? {
    let u: User = user?;
    println!("{u:?}");
}
```

To participate in an explicit transaction, pass `Some(&txn)`:

```rust
let txn = env.begin_transaction(None, None)?;
index.put(
    Some(&txn),
    &ser,
    &User { id: 2, email: "b@c.com".into(), dept_id: None, name: "Bob".into() },
)?;
txn.commit()?;
```

The `index.put` and `index.delete_with_entity` calls automatically
update every secondary index that has been registered against this
primary index. The plain `index.delete(txn, &pk)` does not fetch the
entity and therefore cannot maintain secondary indexes; use
`delete_with_entity` whenever secondary maintenance is required.

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

## v1.5 limitations

- **Secondary indexes are in-memory only.** Entities with secondary
  keys (registered via `User::open_<name>_index(...)` or the manual
  `index.open_secondary_index(|e| ...)` path) maintain the
  `secondary_key → primary_key` mapping in a process-local
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
- **Foreign-key actions are metadata only.** The
  `on_related_entity_delete = ABORT | CASCADE | NULLIFY` attribute is
  recorded in `User::SECONDARY_INDEXES[].on_related_entity_delete`
  but is **not** enforced by the engine in v1.5/v1.6 (the secondary
  layer is in-memory and has no access to a foreign-key constraint
  graph). v2.0 will wire the actions into the cascade path.
- See [`docs/src/internal/sprint-3-dpl-restriction.md`](../internal/sprint-3-dpl-restriction.md)
  for the full audit context, the rationale for shipping the
  in-memory secondaries unchanged in v1.5, and the v1.6 plan.
- See [`docs/src/internal/wave-2c-1-derive-macro.md`](../internal/wave-2c-1-derive-macro.md)
  for the design of the v1.6 derive-macro layer.

## Other roadmap items

- The serializer is a runtime parameter on every read/write call; it
  is not stored alongside the data. Replacing the serializer for a
  given entity type requires a schema-evolution migration.
- `delete(txn, &pk)` cannot maintain secondary indexes; prefer
  `delete_with_entity(txn, &ser, &pk)`.

## Legacy: manual trait-impl path

The derive macros are syntactic sugar over the same traits the engine
exposes. If you cannot use them — for example, if you need to
implement `Entity` for a foreign type via a wrapper, or you want to
keep `noxu-persist-derive` out of your dependency graph — you can
still write the impls by hand. This is the shape every Noxu DB
release before v1.6 supported:

```rust
use noxu_persist::{Entity, PrimaryKey, Result};

#[derive(Clone, Debug, PartialEq)]
struct User { id: u64, email: String, name: String }

impl Entity for User {
    type PrimaryKey = u64;
    fn primary_key(&self) -> &u64 { &self.id }
    fn entity_name() -> &'static str { "User" }
}

// Open a secondary index by hand.
# fn doc(env: &noxu_db::Environment) -> Result<()> {
# use noxu_persist::{EntityStore, PrimaryIndex, SecondaryIndex, StoreConfig};
# let mut store = EntityStore::open(env, StoreConfig::new("s").with_allow_create(true))?;
let mut index: PrimaryIndex<u64, User> = store.get_primary_index()?;
let by_email: SecondaryIndex<String, u64, User> =
    index.open_secondary_index(|u: &User| Some(u.email.clone()));
# Ok(())
# }
```

Internally the derive emits exactly this shape — the only difference
is that the derive does the typing for you and maintains a compile-
time `SECONDARY_INDEXES` metadata table.
