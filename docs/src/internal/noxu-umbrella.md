# noxu umbrella crate (v3.0.1)

**Branch:** `fix/noxu-umbrella`
**Status:** complete — gate green

---

## Overview

This document describes the `noxu` umbrella crate introduced in v3.0.1.
The umbrella is the single entry point for users: a thin re-export crate
that wires together all 20 component crates behind one name and version.
Users write:

```toml
[dependencies]
noxu = "3"
```

and obtain the full engine, the typed persistence layer (with derive
macros), XA, optional replication, and optional observability.

---

## Problem solved

The `noxu-persist-derive` proc-macro already emitted `::noxu::persist::…`
paths in its generated code (retargeted in the `wip(v3.0.1)` prep commit).
Before this branch the `noxu` crate did not exist, so any crate that
`#[derive(Entity)]` would fail to compile with "cannot find `noxu` in the
list of imported crates".  This branch creates the umbrella and routes all
derive tests through it.

---

## Crate location

`crates/noxu/`

---

## Feature map

| Feature | Default | Dependencies pulled in |
|---|---|---|
| `collections` | **yes** | `noxu-collections` |
| `persist` | **yes** | `noxu-persist`, `noxu-persist-derive` |
| `xa` | **yes** | `noxu-xa` |
| `replication` | no | `noxu-rep` |
| `replication-tls-rustls` | no | `replication` + `noxu-rep/tls-rustls` |
| `replication-tls-native` | no | `replication` + `noxu-rep/tls-native` |
| `observability` | no | `noxu-observe`, `noxu-db/observability` |

Always-on (not feature-gated):

- `noxu-db` — public API: `Environment`, `Database`, `Cursor`, `Transaction`
- `noxu-bind` — serialization bindings (tuple, entry, serial)

---

## Public surface

### At the crate root (`noxu::*`)

Everything from `noxu_db::*` is re-exported at the crate root via
`pub use noxu_db::*;`.  This means `noxu::Environment`,
`noxu::EnvironmentConfig`, `noxu::Database`, `noxu::DatabaseConfig`,
`noxu::Cursor`, `noxu::Transaction`, `noxu::DatabaseEntry`,
`noxu::OperationStatus`, `noxu::NoxuError`, etc. all resolve.

### `noxu::bind`

All of `noxu_bind::*` — tuple encoding, entry views, serial encoding.

### `noxu::collections` (feature `collections`)

All of `noxu_collections::*` — `StoredMap`, `StoredSet`, `StoredList`.

### `noxu::persist` (feature `persist`)

All of `noxu_persist::*` plus the three derive macros re-exported from
`noxu_persist_derive`:

| Symbol | Kind |
|---|---|
| `noxu::persist::Entity` | trait + derive macro |
| `noxu::persist::PrimaryKey` | trait + derive macro |
| `noxu::persist::SecondaryKey` | derive macro |
| `noxu::persist::PrimaryIndex` | struct |
| `noxu::persist::EntityStore` | struct |
| `noxu::persist::StoreConfig` | struct |
| `noxu::persist::EntitySerializer` | trait |
| `noxu::persist::SecondaryIndex` | struct |
| `noxu::persist::SecondarySpec` | struct |
| `noxu::persist::Relate` | enum |
| `noxu::persist::DeleteAction` | enum |
| `noxu::persist::PersistError` | enum |
| `noxu::persist::Result<T>` | type alias |

### `noxu::xa` (feature `xa`)

All of `noxu_xa::*` — `XaEnvironment`, `XaTransaction`, `XaStatus`,
`Xid`, `XaError`.

### `noxu::replication` (feature `replication`)

All of `noxu_rep::*` — replication group, elections, VLSN tracking.

### `noxu::observe` (feature `observability`)

All of `noxu_observe::*` — `tracing` / `metrics` / OpenTelemetry glue.

---

## Derive-path change

`noxu-persist-derive` was retargeted (in the v3.0.1 prep commit) to emit
`::noxu::persist::…` paths instead of `::noxu_persist::…`.  The effect:

- Any crate that uses `#[derive(Entity)]`, `#[derive(PrimaryKey)]`, or
  `#[derive(SecondaryKey)]` must have `noxu` (with `features = ["persist"]`)
  as a dependency — either a direct `[dependency]` or a dev-dependency.
- Users who previously depended on `noxu-persist` directly should switch
  to `noxu = { version = "3", features = ["persist"] }`.

### Crates affected

| Crate | Change |
|---|---|
| `noxu-persist-derive` | Dev-deps: replaced `noxu-persist` + `noxu-db` with `noxu = { …, features = ["persist"] }` |
| `noxu-persist` | Dev-deps: added `noxu = { …, features = ["persist"] }` (for `derive_tests.rs`) |
| Root workspace (`noxu-examples`) | `[dependencies]`: added `noxu = { workspace = true }` |
| `examples/persist_derive.rs` | Changed `use noxu_db::…` / `use noxu_persist::…` to `use noxu::…` / `use noxu::persist::…` |
| `crates/noxu-persist-derive/tests/ui/pass_*.rs` | Changed `use noxu_persist::…` to `use noxu::persist::…` |
| `crates/noxu-persist-derive/tests/ui/fail_*.rs` | Changed `use noxu_persist::…` to `use noxu::persist::…` |

### `.stderr` golden files

All eight trybuild `.stderr` files were regenerated with
`TRYBUILD=overwrite cargo test -p noxu-persist-derive`.  The errors are:

| File | Expected error |
|---|---|
| `fail_missing_primary_key` | "missing `#[primary_key]` field" |
| `fail_two_primary_keys` | "multiple `#[primary_key]` fields are not supported" |
| `fail_invalid_relate` | "invalid `relate = NotARealVariant`" |
| `fail_invalid_on_delete` | "invalid `on_related_entity_delete = Burninate`" |
| `fail_secondary_without_name` | "`#[secondary_key(...)]` requires `name = \"...\"`" |
| `fail_unknown_secondary_attr` | "unrecognised key in `#[secondary_key(...)]`" |
| `fail_unknown_entity_attr` | "unrecognised attribute on `#[entity(...)]`" |
| `fail_secondary_no_fields` | "`#[derive(SecondaryKey)]` requires at least one field" |

All errors are the expected macro-validation errors (not path-resolution
errors), confirming the golden files are correct.

---

## Workspace changes

- `Cargo.toml` `[workspace.members]`: added `"crates/noxu"`.
- `Cargo.toml` `[workspace.dependencies]`: added
  `noxu = { version = "3.0.1", path = "crates/noxu" }`.
- Root `[dependencies]`: added `noxu = { workspace = true }` (needed by
  `examples/persist_derive.rs`).

---

## Smoke tests (`crates/noxu/tests/smoke.rs`)

Four tests prove `noxu = "3"` alone is sufficient:

| Test | What it checks |
|---|---|
| `smoke_core_open_put_get` | `Environment` / `Database` / `DatabaseEntry` API via `noxu::` |
| `smoke_derive_entity_round_trip` | Full put/get via `#[derive(Entity, SecondaryKey)]` and `EntityStore` |
| `smoke_derive_entity_name` | `Entity::entity_name()` from the derive |
| `smoke_primary_key_derive_round_trip` | `#[derive(PrimaryKey)]` composite key round-trip |

All four pass.

---

## Gate results

| Gate | Result |
|---|---|
| `cargo build --workspace` | ✓ clean |
| `cargo fmt --all -- --check` | ✓ clean |
| `cargo clippy --workspace --all-targets -- -D warnings` | ✓ clean |
| `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps` | ✓ clean |
| `cargo nextest run --workspace` | 5801 pass, 1 pre-existing timeout† |
| `make docs-check` | ✓ typos 0, markdownlint 0, mdbook 0 errors |

† `noxu-spec::flexible_paxos::tests::ephemeral_promises_allow_split_brain`
  is a Stateright model-checker test that times out at 120 s on this
  machine.  The same test times out on the `release/v3.0.1` base branch —
  it is pre-existing and unrelated to the umbrella work.  Run with
  `make spec` (which has a longer timeout budget) for the full spec suite.

---

## Dependency list for the published `noxu` crate

Always-on:

- `noxu-db = "3.0.1"`
- `noxu-bind = "3.0.1"`

Optional (pulled in by the correspondingly named feature):

- `noxu-collections = "3.0.1"` (`collections`)
- `noxu-persist = "3.0.1"` (`persist`)
- `noxu-persist-derive = "3.0.1"` (`persist`)
- `noxu-xa = "3.0.1"` (`xa`)
- `noxu-rep = "3.0.1"` (`replication`)
- `noxu-observe = "3.0.1"` (`observability`)

The umbrella itself has no `unsafe` code and no new third-party
dependencies.  All transitive dependencies are already in the workspace.

---

## Publishing order

The orchestrator must publish the component crates before `noxu` itself.
The `noxu` crate must be published last (it has the widest dependency
footprint).  See `docs/src/contributing/publishing.md` for the full order.
