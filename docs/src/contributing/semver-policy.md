# SemVer Policy

This document defines how Noxu DB versions its public API and what
guarantees users can rely on.

## Canonical reference

The Rust project maintains the authoritative list of what constitutes a
breaking change at:

> <https://doc.rust-lang.org/cargo/reference/semver.html>

Everything in that reference applies to Noxu DB.  The sections below
clarify how Noxu DB applies those rules and handle cases specific to
this project.

---

## Version history and current policy

### Before v3.0.0 (current: v2.x)

Noxu DB is pre-stable.  Breaking changes have been routinely shipped in
minor releases (v2.0, v2.1, v2.2, v2.3, v2.4 …) and have been
documented in `CHANGELOG.md` with a `BREAKING:` prefix.

**There is no API stability guarantee in v2.x.** Pre-v3.0 minor and
patch releases may freely change any public item.

### v3.0.0 and later

Starting with v3.0.0, Noxu DB follows **strict Semantic Versioning**:

- **Patch releases (v3.0.x)**: bug fixes only — no new API, no
  breaking changes.
- **Minor releases (v3.x.0)**: new functionality that is backward
  compatible — new items may be added, existing items must not be
  changed or removed.
- **Major releases (v4.0.0 …)**: may contain breaking changes.
  The `CHANGELOG.md` will list every break under `BREAKING:`.

The stability commitment covers all items listed in
[`api-stability.md`](api-stability.md) under the **Stable** and
**Stable (foundational)** tiers.  Internal-tier crates are explicitly
excluded.

---

## What counts as "breaking"

The following changes are **always breaking** (require a major bump):

| Category | Example |
|----------|---------|
| Removing a public item | Deleting `fn Database::get` |
| Renaming a public item | `NoxuError` → `NoxuDbError` |
| Changing a function signature | Adding a mandatory parameter, changing a return type |
| Changing a public struct's fields (if not `#[non_exhaustive]`) | Adding, removing, or re-typing a field |
| Adding a non-default method to a public trait | `trait EntryBinding` gains a new required `fn` |
| Changing a public enum's variants (if not `#[non_exhaustive]`) | Adding or removing a variant |
| Increasing MSRV | Bumping `rust-toolchain.toml` to a newer stable |
| Changing a public type's `Send`/`Sync` bounds | Making `Environment` non-`Send` |
| Changing sealed-trait behaviour in a way external impls break | — |
| Removing a feature flag | Deleting `features = ["tls-rustls"]` |

The following changes are **not breaking** (allowed in minor releases):

| Category | Example |
|----------|---------|
| Adding a new public item | New `fn Database::scan_prefix` |
| Adding a non-exhaustive enum variant | When the enum is `#[non_exhaustive]` |
| Adding a new optional feature | `features = ["async-rt"]` |
| Adding a default impl to a trait | Existing impls unaffected |
| Adding a `#[deprecated]` attribute | Deprecation is advisory; not a break |
| Fixing a soundness bug | Even if behaviour changes |
| Improving error messages | `Display` and `Debug` output are not stable |
| Changing internal implementation | No observable API surface change |

---

## Behavioural, durability, and on-disk-format changes

SemVer as defined for Rust APIs covers *type signatures*, but a storage
engine has two additional stability surfaces a signature diff does not capture.
Both get explicit rules here, added after the 7.5.4 review flagged a durability
semantics change that shipped as a patch:

### Durability / persistence semantics

A change to the *observable durability or acknowledgement semantics* of a
stable API item — even one that is technically a bug fix and leaves the type
signature unchanged — is **minor-bump-worthy and MUST carry a migration note**.
The canonical example is the 7.5.4 correction of the `Durability` convenience
constants: `COMMIT_SYNC` changed `replicaSync` `Sync → NoSync` and
`COMMIT_NO_SYNC` changed `replicaAck` `None → SimpleMajority`. Those were
correct (they restore JE semantics) but they change the fsync-on-replica and
ack-wait behaviour an existing HA deployment observes. Such a change:

- MUST be called out in `CHANGELOG.md` with the exact before/after values and
  the affected constant names.
- MUST appear in the [migration guide](../getting-started/migrating.md) with
  instructions to reproduce the prior behaviour (e.g. via `Durability::new`).
- SHOULD, going forward, be released as at least a **minor** bump so
  `^`-range resolution does not deliver it silently on `cargo update`.

### On-disk (`.ndb`) format

The on-disk log format is versioned by `noxu_log::file_header::LOG_VERSION`
(currently 3) with a `MIN_LOG_VERSION` floor (2) enforced at open. The stability
rules:

- A change that makes a newer engine write bytes an older supported engine
  **cannot read** is a format break: it MUST bump `LOG_VERSION` and be released
  as at least a minor version, with the cross-version matrix documented in
  [on-disk-format.md](../reference/on-disk-format.md).
- Adding an **optional, flag-gated** field that older readers parse correctly
  (the field is absent unless its presence bit is set, and its presence does
  not change the layout of existing fields) is **not** a format break and needs
  no `LOG_VERSION` bump. Example: per-record TTL expiration in 7.5.4 rides the
  pre-existing `HAVE_EXPIRATION` flag bit in the LN log entry — the entry
  format was byte-identical between 7.5.3 and 7.5.4, so 7.5.3 reads a
  7.5.4-with-TTL file without misinterpreting it (it simply does not act on the
  expiration). Every such addition MUST still be recorded in the on-disk-format
  cross-version compatibility matrix.
- The engine MUST fail loudly (`LogError::VersionMismatch`) on a file whose
  `log_version` is below `MIN_LOG_VERSION` or above the engine's `LOG_VERSION`
  rather than risk misreading.

---

## Compatibility tier table

| Crate | Tier | v3.0+ guarantee |
|-------|------|------------------|
| `noxu` | **Stable (umbrella)** | Full SemVer from v3.0.0. **This is the crate applications should depend on.** |
| `noxu-db` | Stable | Full SemVer from v3.0.0. |
| `noxu-bind` | Stable | Full SemVer from v3.0.0. |
| `noxu-collections` | Stable | Full SemVer from v3.0.0. |
| `noxu-persist` | Stable | Full SemVer from v3.0.0. |
| `noxu-xa` | Stable | Full SemVer from v3.0.0. |
| `noxu-rep` | Stable | Full SemVer from v3.0.0. |
| `noxu-util` | Stable (foundational) | Stable for enumerated items; new items may be added. |
| `noxu-config` | Stable (foundational) | `ConfigManager`, `ConfigParam`, `ParamValue`, `ParamType`, `ConfigError` are stable; the `params` catalogue is stable for names/defaults; new params may be added additively. |
| `noxu-engine` | Internal | May change in any release. |
| `noxu-dbi` | Internal | May change in any release. |
| `noxu-tree` | Internal | May change in any release. |
| `noxu-txn` | Internal | May change in any release. |
| `noxu-evictor` | Internal | May change in any release. |
| `noxu-cleaner` | Internal | May change in any release. |
| `noxu-recovery` | Internal | May change in any release. |
| `noxu-log` | Internal | May change in any release. |
| `noxu-latch` | Internal | May change in any release. |
| `noxu-sync` | Internal | May change in any release. |
| `noxu-observe` | Internal | May change in any release. |
| `noxu-spec` | Internal (tooling) | Not published; spec-only. |
| `noxu-persist-derive` | Internal (proc-macro support) | Accessed only through `noxu-persist`; its standalone API is not stable. |

---

## Deprecation cycle

Before removing or breaking a stable item:

1. Mark the item `#[deprecated(since = "X.Y.Z", note = "use Replacement instead")]`
   in a **minor** release.
2. Announce the removal in `CHANGELOG.md`.
3. Remove in the **next major** release.

> Rust does not support `#[deprecated]` on struct fields (as of Rust
> 1.95).  For deprecated fields, the doc comment is updated to say
> "Deprecated since vX.Y.Z — use … instead" and the setter/builder
> methods (if any) receive `#[deprecated]`.

---

## The CI gate

Every PR that modifies a stable-tier crate runs `cargo semver-checks`
against `main`:

```shell
cargo semver-checks --workspace --baseline-rev main
```

The gate is **advisory** in the first cycle after v3.0.0 lands (it
reports but does not fail CI).  It will be promoted to **blocking**
after one full minor-release cycle has passed with no false positives.

### Handling a `cargo-semver-checks` failure

| Situation | Action |
|-----------|--------|
| True break — remove or change a public item | Revert the break; make an additive change instead; or hold the PR for the next major-version branch. |
| Intentional break — scheduled for next major | Re-title the PR `BREAKING: …` and target the next major-version branch. |
| False positive — tool limitation | Add a `// cargo-semver-checks-ignore: <lints>` comment directly above the changed item and document the justification in the PR description. |

### Pinned version

The workflow pins `cargo-semver-checks` at **v0.47.0**.  When upgrading
the pin, run the full check manually first and review any new lints it
raises.

---

## CHANGELOG conventions

- Items that break the API are prefixed `BREAKING:` in `CHANGELOG.md`.
- Pre-v3.0 breaks appear under the relevant `[v2.x.y]` section.
- Post-v3.0 breaks (only in major releases) appear under `[v4.0.0]` etc.
- `#[deprecated]` additions appear under `### Deprecated` in the release
  they were added.
- Removals (of previously deprecated items) appear under `### Removed`.

---

## References

- [Rust Cargo — SemVer Compatibility](https://doc.rust-lang.org/cargo/reference/semver.html)
- [`api-stability.md`](api-stability.md) — enumeration of the v3.0 stable surface
- [`CHANGELOG.md`](../../../CHANGELOG.md) — per-release change log
