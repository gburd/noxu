# Wave FA — DPL Derive Crate-Path Override (`#[entity(crate = "…")]`)

**Branch**: `fix/fa-derive-crate-attr`
**Target**: v3.1.0
**Status**: Merged

## Problem

`crates/noxu-persist-derive` hard-coded `::noxu::persist::` in every path
emitted by the three DPL derive macros (`Entity`, `PrimaryKey`, `SecondaryKey`).
Users who depend on `noxu-persist` directly — without the `noxu` umbrella
crate — received compile errors like:

```
error[E0433]: failed to resolve: use of undeclared crate or module `noxu`
  --> src/model.rs:3:10
   |
3  | #[derive(Entity)]
   |          ^^^^^^ use of undeclared crate or module `noxu`
```

This is the same problem `serde_derive` solved with `#[serde(crate = "…")]`.
Design Decision 9 (umbrella crate coupling) explicitly deferred this escape
hatch to a future release; Wave FA delivers it.

## Solution

Add `crate = "…"` as a new valid key in the `#[entity(…)]` container
attribute, recognised by **all three** derives.

### Attribute chosen

```text
#[entity(crate = "path")]
```

A single, consistent attribute name across all three derives.  Rationale:

- `entity` is the top-level DPL concept; it is already registered as an
  inert attribute by all three derives (or by new registration for
  `PrimaryKey`).
- A single name avoids users needing to remember three different attribute
  names for one crate-path concept.
- When someone uses `derive(PrimaryKey)` on a composite-key struct and
  adds `#[entity(crate = "noxu_persist")]`, the annotation reads as
  "configure the DPL crate root for this struct" — analogous to how
  `#[serde(crate = "…")]` configures the serde crate root.

### Default behaviour unchanged

When the attribute is absent, generated code still emits
`::noxu::persist::…`.  Umbrella users (`noxu = "3"`) require zero changes.

### Standalone usage

```toml
# Cargo.toml (no noxu umbrella needed)
[dependencies]
noxu-persist = "3"
```

```rust
use noxu_persist::{Entity, PrimaryKey, SecondaryKey};

// Composite key type — override the crate path.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, PrimaryKey)]
#[entity(crate = "noxu_persist")]
struct UserId(u64);

// Entity + secondary indexes — same override, combined with name override.
#[derive(Clone, Debug, Entity, SecondaryKey)]
#[entity(crate = "noxu_persist", name = "MyUser")]
struct User {
    #[primary_key]
    id: UserId,
    #[secondary_key(name = "by_email", relate = OneToOne)]
    email: String,
    #[secondary_key(name = "by_dept", relate = ManyToOne)]
    dept: Option<u64>,
}
```

### Path validation

The `crate` value is a string literal parsed at compile time via
`syn::Path::parse_mod_style`.  A malformed path (e.g., a bare number or
unparsable token) produces a descriptive error at the annotation site:

```
error: `#[entity(crate = "bad path!")]` is not a valid Rust module path;
       expected e.g. `"noxu_persist"` or `"::noxu::persist"`
```

## Files Changed

| File | Change |
|---|---|
| `crates/noxu-persist-derive/src/lib.rs` | Core implementation: `parse_entity_container_attrs`, `parse_krate_from_entity_attr`, `default_krate`; thread `krate: &Path` through all expand functions and composite-key helpers; update `relate_to_tokens` / `delete_action_to_tokens` signatures; register `entity` attribute in `derive(PrimaryKey)` |
| `crates/noxu-persist-derive/Cargo.toml` | Add `noxu-persist` as dev-dep (mirrors serde/serde_derive pattern; enables trybuild fixtures using standalone paths) |
| `crates/noxu-persist-derive/tests/ui.rs` | Register two new pass fixtures |
| `crates/noxu-persist-derive/tests/ui/pass_crate_override_standalone.rs` | New trybuild pass: newtype PK + Entity + SecondaryKey all with `#[entity(crate = "noxu_persist")]` |
| `crates/noxu-persist-derive/tests/ui/pass_crate_override_composite_key.rs` | New trybuild pass: composite PK with `#[entity(crate = "noxu_persist")]` |
| `crates/noxu-persist-derive/tests/ui/fail_unknown_entity_attr.stderr` | Updated golden file (error message now lists `crate` as a valid key alongside `name`) |
| `crates/noxu-persist/src/lib.rs` | Fix self-contradiction: doc notice updated to reflect both umbrella and direct-dep usage patterns; add `#crate-path-escape-hatch` section |
| `crates/noxu-persist/tests/derive_tests.rs` | 4 new standalone tests: newtype PK round-trip, composite PK round-trip + truncated-input error, entity name resolution, secondary-key metadata |
| `docs/src/collections/entity-persistence.md` | Add `#[entity(crate = "…")]` row to attribute table; add §"Crate-path override" subsection with code example |
| `docs/src/maintainer/design-decisions.md` | Decision 9: mark escape hatch implemented (v3.1.0 / Wave FA) |
| `docs/src/internal/wave-fa-derive-crate-attr.md` | This file |
| `CHANGELOG.md` | `[Unreleased]` entry |

## Implementation Details

### `parse_entity_container_attrs` (replaces `entity_name_from_attrs`)

Parses all recognised `#[entity(…)]` keys in a single `parse_nested_meta`
pass:

- `name = "…"` — entity name (unchanged semantics)
- `crate = "…"` — crate-root path override (new)

Unknown keys still produce a compile error; the message now lists both
valid keys.

### `parse_krate_from_entity_attr`

Used by `expand_primary_key` and `expand_secondary_key` when only the
crate root is needed (not the entity name).  Silently skips `name` keys so
that `#[entity(name = "Foo", crate = "noxu_persist")]` on a struct that
uses all three derives compiles without duplicate-key errors.

### `default_krate`

Returns `::noxu::persist` as a `syn::Path`.  Called by both parse functions
when no `crate` key is present.  Uses `syn::parse_str` + `expect` (the
hardcoded string is always valid; this is a programming invariant, not
user input).

### `derive(PrimaryKey)` attribute registration

Before this wave, `proc_macro_derive(PrimaryKey)` registered no inert
attributes, so `#[entity(crate = "…")]` on a key struct produced an
"unused attribute" warning (or error with `deny(unused_attributes)`).
The registration was added:

```rust
#[proc_macro_derive(PrimaryKey, attributes(entity))]
```

### Dev-dep cycle

`noxu-persist-derive` now lists `noxu-persist` in `[dev-dependencies]`.
This is the well-known `serde` / `serde_derive` pattern: `noxu-persist`
depends on `noxu-persist-derive` at compile time (proc-macro), and
`noxu-persist-derive` depends on `noxu-persist` at test time only.
Cargo handles this correctly because dev-dependencies do not participate
in the compilation ordering for the library target.

## Tests Added

| Test | Location | Covers |
|---|---|---|
| `pass_crate_override_standalone` | trybuild | Newtype PK + Entity + SecondaryKey with `#[entity(crate = "noxu_persist")]`; asserts entity name, PK round-trip, SECONDARY_INDEXES length |
| `pass_crate_override_composite_key` | trybuild | Composite PK with override; round-trip encode/decode |
| `standalone_crate_override_newtype_primary_key_round_trip` | derive_tests.rs | Newtype PK encodes to 8 bytes, decodes correctly |
| `standalone_crate_override_composite_primary_key_round_trip` | derive_tests.rs | Named-field composite PK round-trip + truncated-input error |
| `standalone_crate_override_entity_derive_compiles_and_name_resolves` | derive_tests.rs | `entity_name()` and `primary_key()` work with override |
| `standalone_crate_override_secondary_key_metadata` | derive_tests.rs | `SECONDARY_INDEXES` const contains correct `Relate` and `DeleteAction` values |

All pre-existing tests remain green.

## Gate Results

```
cargo fmt --all -- --check          OK
cargo clippy --workspace … -D warnings   OK
RUSTDOCFLAGS=-D warnings cargo doc …    OK
cargo test --workspace --no-fail-fast   OK (all tests pass)
make docs-check                     OK
```
