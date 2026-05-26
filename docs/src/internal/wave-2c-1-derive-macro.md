# Wave 2C-1 — DPL derive macros

**Status:** completed in v1.6.

**Closes:** May 2026 JE port audit MEDIUM finding 3 — "The DPL
annotation model (`@Entity`, `@PrimaryKey`, …) is replaced by a manual
trait-implementation path. There is no proc-macro derive yet, so users
cannot ergonomically declare entities."
([`je-port-audit-2026-05-overview.md`](je-port-audit-2026-05-overview.md))

## Goal

Restore ergonomic parity with BDB-JE's `@Entity` / `@PrimaryKey` /
`@SecondaryKey` annotations so users can declare entity classes with a
few `#[derive(...)]`s instead of writing every trait impl by hand.

## Non-goals (deferred)

- **Persistent secondary indexes** — secondaries are still in-memory
  in v1.6; durability is the Wave 2C-2 work.
- **Foreign-key cascade enforcement** — `on_related_entity_delete`
  is recorded as metadata in `SecondarySpec` but the engine does not
  yet act on `Cascade` / `Nullify`. v2.0.
- **Schema-evolution auto-wiring** — Wave 2C-3.
- **Reflection / runtime metadata model** — JE's
  `EntityMetadata` / `FieldMetadata` are deliberately omitted; the
  Rust derive emits a compile-time `pub const SECONDARY_INDEXES:
  &[SecondarySpec]` table instead, which covers the static
  introspection use cases.

## New crate: `noxu-persist-derive`

A new proc-macro crate at `crates/noxu-persist-derive/` provides three
derives. It depends only on `syn`, `quote`, and `proc-macro2` — no
internal `noxu-*` dependencies, because proc-macro crates can never
share a compilation graph with the rest of the workspace.

`noxu-persist` lists `noxu-persist-derive` as a regular dependency and
re-exports the three macros at crate root, mirroring the
`serde` / `serde_derive` pattern. Users only add `noxu-persist` to
their `Cargo.toml`.

## The three derives

### `#[derive(Entity)]`

Implements `noxu_persist::Entity` for the user struct.

- The struct must contain exactly one field annotated `#[primary_key]`.
  That field's type becomes `Entity::PrimaryKey` and `primary_key(&self)`
  returns `&self.<that_field>`.
- An optional struct-level `#[entity(name = "...")]` attribute
  overrides the entity name (default: the struct name).
- Generic structs are supported by passing through
  `impl_generics` / `ty_generics` / `where_clause` from `syn`.

### `#[derive(PrimaryKey)]`

Implements `noxu_persist::PrimaryKey` for a custom key struct.

- **Newtype** (`struct UserId(u64);`) — delegates `to_bytes` /
  `from_bytes` directly to the inner field's `PrimaryKey` impl. Zero
  byte-format overhead vs. using `u64` directly.
- **Composite** (named-field or multi-field tuple) — encodes each
  field with a 4-byte big-endian length prefix followed by the field
  bytes. Field order in the struct equals byte-lex sort order of the
  resulting key.
- **Unit struct** — rejected with a clear compile error.
- The user is responsible for separately deriving
  `Clone + PartialEq + Eq + Hash` (and usually `PartialOrd + Ord`)
  to satisfy `PrimaryKey`'s super-traits and the `PrimaryIndex`
  type bounds.

### `#[derive(SecondaryKey)]`

For each `#[secondary_key(name = "…", relate = …, …)]` field on the
struct, emits:

1. An inherent helper method on the entity:

   ```rust,ignore
   impl User {
       pub fn open_<name>_index<'p>(
           primary: &mut PrimaryIndex<'p, PK, Self>,
       ) -> SecondaryIndex<SK, PK, Self> {
           primary.open_secondary_index(|e: &Self| extractor)
       }
   }
   ```

   where `SK` is the field's type with `Option<T>` unwrapped and the
   extractor is `e.field.clone()` (for `Option<T>`) or
   `Some(e.field.clone())` (for non-`Option`).

2. A `pub const SECONDARY_INDEXES: &'static [SecondarySpec; N]`
   metadata table containing one entry per declared index.

The macro accepts:

| Key | Type | Required | Default |
|---|---|---|---|
| `name` | string literal | yes | — |
| `relate` | identifier — `OneToOne`, `ManyToOne`, `OneToMany`, `ManyToMany` | yes | — |
| `related_entity` | string literal | no | `None` |
| `on_related_entity_delete` | identifier — `Abort`, `Cascade`, `Nullify` (BDB-JE upper-case `ABORT`/`CASCADE`/`NULLIFY` also accepted) | no | `Abort` |

Invalid `relate` / `on_related_entity_delete` values produce
compile-time errors with the full list of allowed identifiers.

## Supporting types in `noxu-persist`

A new module `noxu_persist::secondary_spec` provides the runtime types
the derive references:

- `Relate { OneToOne, ManyToOne, OneToMany, ManyToMany }`
- `DeleteAction { Abort, Cascade, Nullify }`
- `SecondarySpec { name, relate, related_entity, on_related_entity_delete }`

`SecondarySpec` is `Copy + 'static`, has `const fn` constructors, and
can be hand-built by code that bypasses the derive.

## Tests

- `crates/noxu-persist-derive/tests/ui/` — `trybuild` compile-pass +
  compile-fail fixtures:
  - **Pass**: basic Entity, composite/newtype PrimaryKey, full
    SecondaryKey with all options.
  - **Fail**: missing `#[primary_key]`, multiple `#[primary_key]`,
    invalid `relate`, invalid `on_related_entity_delete`, missing
    `name`, unknown attribute keys (in both `#[entity(...)]` and
    `#[secondary_key(...)]`), `derive(SecondaryKey)` on a struct with
    no annotated fields.
- `crates/noxu-persist/tests/derive_tests.rs` — integration tests that
  open an `EntityStore`, exercise the full CRUD + secondary-lookup
  surface against derive-macro entities, and assert that the
  `SECONDARY_INDEXES` metadata table contains the expected entries.
- `examples/persist_derive.rs` — full-app demo that uses only the
  derive path (companion to the manual `examples/persist.rs`).

## Backward compatibility

The manual trait-impl path documented in pre-v1.6 docs continues to
compile and run unchanged. The derive macros are pure additions:

- No existing `Entity` / `PrimaryKey` / `EntitySerializer` trait or
  method signatures were modified.
- The new `SecondarySpec` / `Relate` / `DeleteAction` types live in a
  new module; no existing `noxu_persist::` re-export shadows a prior
  one.
- The `noxu-persist-derive` crate is a transitive dependency only;
  users who do not use the derives pay nothing at runtime.

## Migration guide

Old (still works):

```rust
impl Entity for User {
    type PrimaryKey = u64;
    fn primary_key(&self) -> &u64 { &self.id }
    fn entity_name() -> &'static str { "User" }
}
let by_email: SecondaryIndex<String, u64, User> =
    index.open_secondary_index(|u: &User| Some(u.email.clone()));
```

New:

```rust
#[derive(Clone, Entity, SecondaryKey)]
struct User {
    #[primary_key] id: u64,
    #[secondary_key(name = "by_email", relate = OneToOne)]
    email: String,
}
let by_email = User::open_by_email_index(&mut index);
```

## Out-of-scope follow-ups

- **Wave 2C-2** — back the secondary maps with a `Database` so they
  survive restart and update atomically with the user transaction.
  At that point the `on_related_entity_delete` metadata becomes
  enforceable.
- **Wave 2C-3** — auto-wire schema evolution
  (`Mutations` / `Renamer` / `Deleter` / `Converter`) into the
  `EntityStore::open` path so the derive can declare a `version =
  N` per entity and the engine applies the registered mutations
  without an explicit `evolve()` call.
- **A custom `Serialize` derive** — the
  `EntitySerializer<E>` trait is still hand-written in v1.6. A
  future `#[derive(EntitySerializer)]` could generate a
  length-prefixed binary format from the struct layout. Out of
  scope here — orthogonal to the entity-declaration ergonomics.
