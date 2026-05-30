# Docs: Recommend `noxu` Umbrella Crate (v3.0.2)

**Branch**: `fix/docs-recommend-noxu`
**Status**: complete

This document tracks the v3.0.2 docs-correction release that updates all
user-facing documentation, the README, and examples to recommend the `noxu`
umbrella crate (`noxu = "3"`) instead of the internal `noxu-db` component.

## Motivation

Since v3.0.1 the `noxu` umbrella crate is published on crates.io. It provides:

- `use noxu::{Environment, …}` — all types formerly at `noxu_db::…`
- `noxu::bind::…` — formerly `noxu_bind::…`
- `noxu::collections::…` — formerly `noxu_collections::…` (default feature)
- `noxu::persist::…` — formerly `noxu_persist::…` (default feature)
- `noxu::xa::…` — formerly `noxu_xa::…` (default feature)
- `noxu::replication::…` — formerly `noxu_rep::…` (opt-in feature)
- `noxu::observe::…` — formerly `noxu_observe::…` (opt-in feature)

Applications should add `noxu = "3"` to their `Cargo.toml`, not `noxu-db`.

## Files Changed

### Version bump (3.0.1 → 3.0.2)

- `Cargo.toml` — workspace `version`, all `[workspace.dependencies]` pins,
  `documentation` URL changed to `https://docs.rs/noxu`, root package
  `[dependencies]` simplified to `noxu` + `parking_lot` + `tempfile`

### README.md

- Badges: crates.io and docs.rs now point at `noxu` (not `noxu-db`)
- **Current version** line: `3.0.2`
- Quick Start: `noxu = "3"`, git-tag example uses `v3.0.2`
- Quick Start code block: `use noxu::{…}`, `noxu::Result<()>`
- Added note that the engine is composed of `noxu-*` component crates —
  applications should depend on `noxu`
- Workspace structure table: added `noxu` (umbrella) row
- Security note: version reference updated to v3.0.2
- Build command: `cargo test -p noxu` instead of `cargo test -p noxu-db`
- Capability matrix link updated

### CHANGELOG.md

- Added `## [v3.0.2] — 2026-05-30` section describing this docs-correction
  release

### docs/src/introduction.md

- Quick Start: replaced `noxu-db = { git = … }` with `noxu = "3"` and
  `noxu = { git = … tag = "v3.0.2" }`
- Code block: `use noxu::{…}`, `noxu::Result<()>`

### docs/src/getting-started/installation.md

- Dependency snippet: `noxu = "3"` (was `noxu-db = "0.1"`)
- Feature flags table: added all optional features
- Dev dep example: `noxu = { path = "crates/noxu" }` (was `noxu-db` path dep +
  separate `noxu-bind` dep)
- Removed the stale "none required" feature flag row

### docs/src/getting-started/bindings.md

- Dependency snippet: `noxu = "3"` (was `noxu-bind = { path = … }`)
- Text: `noxu::bind` (was `noxu-bind`, `noxu_bind`)
- `noxu-collections` → `noxu::collections`

### docs/src/collections/README.md

- `noxu-collections` → `noxu::collections` (via the `noxu` umbrella crate)
- `noxu-persist` → `noxu::persist` (via the `noxu` umbrella crate)

### docs/src/collections/entity-persistence.md

- `noxu-persist-derive` / `noxu-persist` → `noxu::persist`

### docs/src/collections/stored-map.md

- ``noxu-bind`:`` → ``noxu::bind`:``

### docs/src/replication/setup.md

- Dependency snippet: `noxu = { version = "3", features = ["replication"] }`
  (was `noxu-rep = { version = "0.1" }`)

### docs/src/contributing/api-stability.md

- Tier table: added `noxu` umbrella row as **Stable (umbrella)**
- Section headers: `noxu-bind`, `noxu-collections`, `noxu-persist`,
  `noxu-xa`, `noxu-rep` → `noxu::bind`, `noxu::collections`, etc.
- Engine re-export note updated to reference `noxu` umbrella root

### docs/src/contributing/semver-policy.md

- Tier table: added `noxu` umbrella as the first Stable row

### docs/src/contributing/publishing.md

- Badge URLs updated: `noxu-db` → `noxu`
- Example docs.rs URL updated

### All use-import examples in user-facing docs (via sed)

The following files had `use noxu_db::`, `use noxu_collections::`,
`use noxu_persist::`, `use noxu_xa::`, `use noxu_rep::`, `use noxu_bind::`
replaced with `use noxu::`, `use noxu::collections::`, `use noxu::persist::`,
`use noxu::xa::`, `use noxu::replication::`, `use noxu::bind::` respectively:

- `docs/src/collections/entity-persistence.md`
- `docs/src/collections/stored-list.md`
- `docs/src/collections/stored-map.md`
- `docs/src/collections/stored-set.md`
- `docs/src/getting-started/cursors.md`
- `docs/src/getting-started/databases.md`
- `docs/src/getting-started/disk-ordered-cursors.md`
- `docs/src/getting-started/environments.md`
- `docs/src/getting-started/reading-writing.md`
- `docs/src/getting-started/records.md`
- `docs/src/getting-started/secondary-databases.md`
- `docs/src/maintainer/reference-source-guide.md`
- `docs/src/operations/monitoring.md`
- `docs/src/replication/in-memory-transport.md`
- `docs/src/transactions/basics.md`
- `docs/src/transactions/concurrency.md`
- `docs/src/transactions/cursors.md`
- `docs/src/transactions/deadlocks.md`
- `docs/src/transactions/isolation.md`
- `docs/src/transactions/secondary-with-txn.md`
- `docs/src/transactions/transaction-config.md`
- `docs/src/transactions/xa-distributed.md`

### examples/ (workspace [[example]] targets)

All examples depend on `noxu` via the workspace root `Cargo.toml`.
The following files were updated (`use noxu_db::` → `use noxu::`, etc.):

| File | Changes |
|---|---|
| `examples/simple.rs` | `use noxu_db::` → `use noxu::` |
| `examples/transactions.rs` | `use noxu_db::` → `use noxu::` |
| `examples/cursor_scan.rs` | `use noxu_db::` → `use noxu::` |
| `examples/sequence.rs` | `use noxu_db::` → `use noxu::` |
| `examples/transaction_config.rs` | `use noxu_db::` → `use noxu::` |
| `examples/scale_validation.rs` | `use noxu_db::` → `use noxu::`; `noxu_db::` → `noxu::` inline |
| `examples/binding.rs` | `use noxu_bind::` → `use noxu::bind::`; `use noxu_db::` → `use noxu::` |
| `examples/collections.rs` | `use noxu_bind::` → `use noxu::bind::`; `use noxu_collections::` → `use noxu::collections::`; `use noxu_db::` → `use noxu::` |
| `examples/getting_started.rs` | `use noxu_bind::` → `use noxu::bind::`; `use noxu_db::` → `use noxu::`; `noxu_bind::Result` → `noxu::bind::Result` |
| `examples/persist.rs` | `use noxu_db::` → `use noxu::`; `use noxu_persist::` → `use noxu::persist::`; `noxu_persist::` → `noxu::persist::` |
| `examples/secondary.rs` | `use noxu_db::` → `use noxu::`; `use noxu_sync::Mutex` → `use parking_lot::Mutex` |
| `examples/xa_distributed.rs` | `use noxu_db::` → `use noxu::`; `use noxu_xa::` → `use noxu::xa::` |

### examples/persist_derive.rs

Already used `use noxu::persist::` and `use noxu::` — no change needed.

### examples/cash/, examples/cask/, examples/ftdb/ (standalone subdir projects)

| File | Change |
|---|---|
| `examples/cash/Cargo.toml` | `noxu-db = { path = … }` → `noxu = { path = … }` |
| `examples/cash/src/store.rs` | `use noxu_db::` → `use noxu::` |
| `examples/cask/Cargo.toml` | `noxu-db = { path = … }` → `noxu = { path = … }` |
| `examples/cask/src/store.rs` | `use noxu_db::` → `use noxu::` |
| `examples/cask/src/server.rs` | `noxu_db::Transaction` → `noxu::Transaction` |
| `examples/ftdb/Cargo.toml` | `noxu-db = { path = … }` → `noxu = { path = … }` |
| `examples/ftdb/src/storage.rs` | `use noxu_db::` → `use noxu::` |
| `examples/ftdb/src/error.rs` | `noxu_db::NoxuError` → `noxu::NoxuError` |

## How Examples are Wired

The workspace root `Cargo.toml` (`[package] name = "noxu-examples"`) owns all
`[[example]]` entries. Its `[dependencies]` section previously listed
`noxu-db`, `noxu-bind`, `noxu-collections`, `noxu-persist`, `noxu-sync` etc.
as separate deps. After this change it depends only on `noxu = { workspace = true }`
(which pulls in all default features: `collections`, `persist`, `xa`) plus
`parking_lot` (for `secondary.rs`) and `tempfile`.

`noxu_sync::Mutex` in `secondary.rs` was replaced with `parking_lot::Mutex`
since `noxu_sync` is an internal crate not re-exported by the umbrella.

The three standalone subdir projects (`cash`, `cask`, `ftdb`) have their own
`Cargo.toml` files and were updated to `noxu = { path = "../../crates/noxu" }`.

## Confirmation

`cargo build --workspace --examples` compiles successfully with all `use noxu::…`
imports after these changes.

## Files NOT changed (per spec)

- `docs/src/internal/` — audit and maintainer docs; component-crate references
  preserved as-is
- Engine/crate source code (`crates/*/src/`)
- Component crate `//!` doc notices
- mTLS code (`noxu-rep/src/auth/`)
- `#![forbid(unsafe_code)]` attributes — unchanged
