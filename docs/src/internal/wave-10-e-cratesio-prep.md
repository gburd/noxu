# Wave 10-E — crates.io publication prep

**Status:** complete (audit + metadata normalization only — no actual publish).
**Branch:** `fix/wave10-e-cratesio-prep` off `sprint/v2.3.0-base`.
**Scope:** Cargo.toml metadata audit, license headers on every crate root,
strategy decision for the future `cargo publish` workstream.

## Strategy chosen: (c) — defer publish, normalize metadata

Three options were considered:

- **(a)** Roll every internal crate into a single fat `noxu-db` crate using
  `cfg`-gated re-exports. Smallest user surface; biggest crate; large
  refactor; loses the per-crate `cargo test -p` ergonomics that contributors
  rely on today.
- **(b)** Publish all 21 crates to crates.io in lockstep. Cleanest user
  story but every internal crate (`noxu-tree`, `noxu-txn`, `noxu-log`, ...)
  becomes a public crates.io artifact whose semver we are forever on the
  hook for, even though they are implementation detail. The
  `[workspace.dependencies]` block also has to grow concrete `version =
  "..."` fields alongside every `path = "..."`, and every release has to
  bump-and-publish the dep graph in topological order.
- **(c)** Keep every crate `publish = false` for now. Users consume Noxu DB
  from git (`noxu-db = { git = "https://codeberg.org/gregburd/noxu", tag =
  "v2.3.0" }`). The metadata is fully populated so flipping `publish =
  false` to `publish = true` later is a one-line change per crate plus the
  workspace dep-graph changes (b) requires.

**Picked: (c)**, because:

1. The workspace dependency graph still uses `path = "..."` everywhere, and
   `cargo publish` rejects path-only deps. Making (b) work requires touching
   every entry of `[workspace.dependencies]` with concrete versions. That
   is the bulk of the (b) workstream and is best done as its own wave once
   the v2.3.0 sprint is closed.
2. Wave 10 is about polishing v2.3.0. A real publish event is a v3.0 (or
   later) release decision — it commits us to a public semver contract on
   currently-internal crates.
3. (c) is non-disruptive: zero behaviour change, zero API change, and
   `cargo build` / `cargo test` all keep working as before.

## What this wave changed

### Workspace `Cargo.toml`

Untouched (other than via per-crate edits below). The
`[workspace.package]` block already exposed `version`, `edition`,
`license`, `repository`, `homepage`, `documentation`, `description`,
`keywords`, `categories`, `readme`, so per-crate manifests can inherit
from it.

### Per-crate `crates/*/Cargo.toml`

All 21 crate manifests now share a uniform metadata shape:

```toml
[package]
name              = "noxu-X"
version.workspace      = true
edition.workspace      = true
license.workspace      = true
repository.workspace   = true
homepage.workspace     = true
keywords.workspace     = true
categories.workspace   = true
readme            = "../../README.md"
description       = "..."
publish           = false  # see docs/src/internal/wave-10-e-cratesio-prep.md
```

#### Public crates (intended to publish at v3.0+)

These already inherited everything except `publish` — only addition was
the explicit `publish = false` line plus the rationale comment:

- `noxu-db`
- `noxu-bind`
- `noxu-collections`
- `noxu-persist`
- `noxu-persist-derive`
- `noxu-rep`
- `noxu-observe`
- `noxu-xa`

#### Internal crates (always `publish = false`)

These already had `publish = false` but were missing
`repository`/`homepage`/`keywords`/`categories`/`readme`. Added all
five inheritance lines so that flipping any one of these to public is a
one-line change later:

- `noxu-cleaner`
- `noxu-config`
- `noxu-dbi`
- `noxu-engine`
- `noxu-evictor`
- `noxu-latch`
- `noxu-log`
- `noxu-recovery`
- `noxu-spec`
- `noxu-sync`
- `noxu-tree`
- `noxu-txn`
- `noxu-util`

#### Outlier — `noxu-xa`

Was the only crate not using workspace inheritance: had hard-coded
`version = "0.1.0"`, `edition = "2021"`, `license = "MIT OR Apache-2.0"`
(license string format also differed), and direct `path = "..."` deps.
Rewrote to match every other crate:

- `version.workspace = true`           (now `2.2.1`, the workspace version)
- `edition = "2021"` retained explicitly. The workspace is on edition
  2024, but bumping `noxu-xa` triggers edition-2024 lints
  (`collapsible_if` for `if let` chains in `prepared_log.rs` and
  `xa_resource.rs`) that are out of scope for a metadata-only wave. A
  follow-up wave should land the `if let` collapses and switch this crate
  to `edition.workspace = true`.
- `license.workspace = true`           (now `Apache-2.0 OR MIT`, canonical form)
- `repository.workspace = true` etc. (added)
- `noxu-db` / `noxu-txn` / `thiserror` / `log` deps now go through `{ workspace = true }`
- `[lints] workspace = true` is **not** added to `noxu-xa` — the workspace
  clippy lints (`redundant_clone`, etc.) flag a couple of patterns in
  `tests/xa_crash_durable_test.rs` that are out of scope for a metadata-only
  wave. Adding `[lints]` should land in the same follow-up wave that fixes
  the edition-2024 lints above.
- `publish = false` added with rationale comment

The version bump from `0.1.0` → `2.2.1` is intentional. `noxu-xa` is part
of the Noxu DB release set; it should track the workspace release version
just like every other crate does.

### License headers

Added a 4-line license header to **22 files** — every `crates/*/src/lib.rs`
plus `crates/noxu-db/src/bin/crash_worker.rs`:

```rust
// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT
```

The SPDX identifier line makes the headers machine-readable for compliance
scanners (e.g. `reuse`, `licensee`). The copyright year `2024-2025` matches
the existing range in `LICENSE-MIT`.

## What this wave did **not** do

- **Did not run `cargo publish --dry-run`.** Every crate is `publish =
  false`, so the registry-side checks are unreachable until strategy (b)
  is adopted.
- **Did not touch `[workspace.dependencies]`.** Adding concrete
  `version = "..."` fields alongside the existing `path = "..."` fields is
  a strategy-(b) prerequisite and will land in the publish wave.
- **Did not split per-crate READMEs.** Every crate currently inherits the
  workspace `README.md`. If/when a crate is published independently and
  warrants its own crates.io landing page (likely candidates: `noxu-db`,
  `noxu-rep`, `noxu-persist`), a dedicated README can be added at that
  point.
- **Did not revisit `keywords` / `categories` per-crate.** The
  workspace-level keywords (`database`, `embedded`, `transactional`,
  `btree`, `wal`) and categories (`database-implementations`,
  `data-structures`, `concurrency`) are accurate for `noxu-db` itself and
  reasonable for the rest of the family, but a future wave that flips
  `publish = true` for individual crates should pick keywords/categories
  per-crate so each crates.io landing page is discoverable on its own
  terms.
- **Did not add `rust-version`.** The toolchain is pinned via
  `rust-toolchain.toml` (`channel = "1.95"`). Adding a `rust-version`
  field to `[workspace.package]` would be useful for crates.io consumers
  but is a publish-wave concern.

## What is left for the actual publish wave

When the project is ready to publish the public-surface crates to
crates.io (likely v3.0):

1. Decide which crates make the cut. Start narrow — most plausibly:
   `noxu-db`, `noxu-bind`, `noxu-collections`, `noxu-persist`,
   `noxu-persist-derive`, `noxu-rep`, `noxu-xa`, `noxu-observe`. If any of
   these still depend on internal crates (they do, transitively), the
   internal crates have to publish too — see step 3.
2. For each public crate, decide its keywords/categories (max 5 keywords,
   crates.io-listed categories) and its dedicated README, if any.
3. Audit `[workspace.dependencies]`: every entry that is consumed by a
   `publish = true` crate must declare both `path` and `version`. Cargo
   will use `path` for in-workspace builds and `version` for the published
   manifest.
4. Decide on `rust-version` (probably the same MSRV as the toolchain
   pin: `1.95`).
5. Topologically sort the crates and run `cargo publish --dry-run -p X`
   for each. Fix any reported issues. Then `cargo publish` in dependency
   order.
6. Add a `RELEASE_PUBLISH.md` checklist (or extend `RELEASE_CHECKLIST.md`)
   so future releases follow the same publish flow.

## Verification

- `cargo check --workspace` — clean.
- `cargo fmt --all -- --check` — clean.
- `cargo clippy --workspace --all-targets -- -D warnings` — clean.
- `make docs-check` — clean.
- License headers present on all `crates/*/src/lib.rs` and
  `crates/noxu-db/src/bin/crash_worker.rs` (22 files total).
- Every `crates/*/Cargo.toml` declares `publish = false` explicitly.
- Every `crates/*/Cargo.toml` inherits `version`, `edition`, `license`,
  `repository`, `homepage`, `keywords`, `categories` from the workspace
  and points `readme` at the root `README.md`.
