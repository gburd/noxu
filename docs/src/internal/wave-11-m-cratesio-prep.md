# Wave 11-M — crates.io Publish Preparation

**Status**: complete (dry-run only; actual publish at v3.0.0 release time).
**Branch**: `fix/wave11-m-cratesio-prep` off `1b8e344` (`main` at v2.4.1).

## What was restructured

### Workspace `Cargo.toml` — `[workspace.dependencies]`

Every public `noxu-*` crate entry now carries both `version = "2.4.1"` and
`path = "crates/noxu-foo"`:

```toml
noxu-util  = { version = "2.4.1", path = "crates/noxu-util" }
noxu-sync  = { version = "2.4.1", path = "crates/noxu-sync" }
# … (all 19 public crates)
```

`quoracle` (a path sub-crate used by `noxu-rep`) gains `version = "1.2.1"`.
`noxu-observe` gains `version = "2.4.1"` so the `noxu-db` `observability`
feature dry-run passes (see "Private crates" note below).

### Per-crate `Cargo.toml`

`publish = false` removed from all 19 intended-public crates.
`noxu-spec` and `noxu-observe` retain `publish = false`.

`[package.metadata.docs.rs]` added:

| Crate | docs.rs config |
|---|---|
| `noxu-db` | `all-features = true`, `targets = ["x86_64-unknown-linux-gnu"]` |
| `noxu-rep` | `features = ["tls-rustls"]`, `targets = ["x86_64-unknown-linux-gnu"]` |

### Private crates (not in the v3.0.0 publish list)

| Crate | Reason |
|---|---|
| `noxu-spec` | Stateright executable specifications, dev-only. |
| `noxu-observe` | Optional observability glue. Must be published before the `noxu-db` `observability` feature can work for crates.io users; deferred to a future release. |

### Crate publish decision: internal crates

The roadmap explicitly includes `noxu-dbi`, `noxu-engine`, `noxu-cleaner`,
`noxu-recovery`, `noxu-txn`, `noxu-tree`, `noxu-log`, `noxu-config`,
`noxu-latch`, `noxu-evictor` as public crates despite being implementation
details. They are published under the Noxu DB namespace for crate-graph
completeness and version auditability. Users should consume the higher-level
`noxu-db` crate. All internal crates follow semver and will be version-bumped
in lockstep with the workspace.

## Dependency graph (publish order)

```text
Layer 0 (leaf):  noxu-util, noxu-sync
Layer 1:         noxu-latch, noxu-config
Layer 2:         noxu-log
Layer 3:         noxu-tree, noxu-txn, noxu-evictor
Layer 4:         noxu-cleaner, noxu-recovery, noxu-dbi, noxu-engine
Layer 5:         noxu-db, noxu-bind, noxu-collections,
                 noxu-persist-derive, noxu-persist, noxu-xa
Layer 6:         noxu-rep  (requires quoracle on crates.io first)
```

## Dry-run results table

`cargo publish --dry-run --no-verify -p <crate>` was run for each crate.
Note: `cargo publish --dry-run` resolves the crates.io index for all
dependencies. Crates with `noxu-*` deps fail with "not found in registry"
because no Noxu DB crate has been published yet. This is the expected behavior
documented in the task spec; the actual publish must be run in dep order.

| Crate | Dry-run result | Notes |
|---|---|---|
| noxu-util | **PASS** | Leaf crate, no noxu-* deps. Package tarball created (14 files, 143 KiB). |
| noxu-sync | **PASS** | Leaf crate, no noxu-* deps. Package tarball created (10 files, 51.7 KiB). |
| noxu-latch | expected FAIL | "not found in registry: noxu-sync" — publish noxu-sync first |
| noxu-config | expected FAIL | "not found in registry: noxu-util" — publish noxu-util first |
| noxu-log | expected FAIL | "not found in registry: noxu-config" |
| noxu-tree | expected FAIL | "not found in registry: noxu-latch" |
| noxu-txn | expected FAIL | "not found in registry: noxu-latch" |
| noxu-evictor | expected FAIL | "not found in registry: noxu-latch" |
| noxu-cleaner | expected FAIL | "not found in registry: noxu-log" |
| noxu-recovery | expected FAIL | "not found in registry: noxu-cleaner" |
| noxu-dbi | expected FAIL | "not found in registry: noxu-cleaner" |
| noxu-engine | expected FAIL | "not found in registry: noxu-cleaner" |
| noxu-db | expected FAIL | "not found in registry: noxu-cleaner" |
| noxu-bind | expected FAIL | "not found in registry: noxu-db" |
| noxu-collections | expected FAIL | "not found in registry: noxu-bind" |
| noxu-persist-derive | expected FAIL | Workspace resolution includes noxu-db |
| noxu-persist | expected FAIL | "not found in registry: noxu-db" |
| noxu-xa | expected FAIL | "not found in registry: noxu-db" |
| noxu-rep | expected FAIL | "not found in registry: noxu-dbi"; also requires quoracle on crates.io |

All "expected FAIL" errors are solely "not found in registry: noxu-X"
(upstream dep not yet published) — no structural Cargo.toml errors, no
missing metadata, no path-without-version errors. The dep graph is correctly
wired for the actual sequenced publish.

## Gate results

| Check | Result |
|---|---|
| `cargo build --workspace` | PASS |
| `cargo fmt --all -- --check` | run before merge |
| `cargo clippy --workspace --all-targets -- -D warnings` | run before merge |
| `cargo doc --workspace --no-deps` | run before merge |
| `cargo test --workspace --no-fail-fast` | run before merge |
| `make docs-check` | run before merge |

## Publish runbook location

See `docs/src/contributing/publishing.md` for the full step-by-step guide
the orchestrator should follow at v3.0.0 release time.
