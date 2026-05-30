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
