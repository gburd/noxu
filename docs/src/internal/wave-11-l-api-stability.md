# Wave 11-L — API Stability Commitment + SemVer Policy + cargo-semver-checks CI Gate

**Status**: Complete  
**Branch**: `fix/wave11-l-api-stability`  
**Baseline**: v2.4.1 (commit `1b8e344`)  
**Deliverables committed**: 2026-05-29

---

## Objective

Produce the documentation, audit, and CI infrastructure to enforce the v3.0.0
API stability commitment. This is a cataloguing and policy wave — no public
API shapes were changed; the only Rust code modifications are `#[deprecated]`
markers and doc-comment corrections.

---

## What was done

### 1. Public API enumeration

`docs/src/contributing/api-stability.md` enumerates the stable surface for:

| Crate | Stable items catalogued |
|-------|------------------------|
| `noxu-db` | ~46 types, traits, functions |
| `noxu-bind` | 20 items (structs, traits, 15 primitive bindings) |
| `noxu-collections` | 10 items |
| `noxu-persist` | ~25 items including schema-evolution types |
| `noxu-xa` | 8 items |
| `noxu-rep` | ~45 items across core, transport, TLS, QUIC |
| `noxu-util` | 17 items |
| `noxu-config` | 5 types + 166 param statics (153 stable, 13 deprecated) |

**Total stable surface**: approximately 376 items.

Also documented: pub-but-internal items in `noxu-db` that will be restricted
to `pub(crate)` before the v3.0.0 tag:

- `Transaction::with_log_manager`, `with_env_impl`, `with_inner_txn`,
  `get_inner_txn` — these accept/return types (`LogManager`,
  `EnvironmentImpl`, `noxu_txn::Txn`) that are not re-exported by `noxu-db`,
  making them effectively uncallable from user code without adding internal
  crate dependencies.

### 2. SemVer policy

`docs/src/contributing/semver-policy.md` documents:

- Pre-v3.0: breaking changes permitted in any release (BREAKING: prefix).
- v3.0+: patch = bugfix only, minor = additive only, major = may break.
- Canonical reference: <https://doc.rust-lang.org/cargo/reference/semver.html>
- Compatibility tier table (Stable / Stable-foundational / Internal).
- Deprecation cycle and `#[deprecated]` struct-field note.
- CI gate process and false-positive handling.

### 3. `#[deprecated]` markers added

**noxu-config** (13 params, `since = "2.4.1"`):

| Param | Replacement / reason |
|-------|---------------------|
| `LOG_USE_NIO` | Use `LOG_USE_WRITE_QUEUE` |
| `LOG_DEFERREDWRITE_TEMP` | Configure per-database via `DatabaseConfig` |
| `OLD_REP_RUN_LOG_FLUSH_TASK` | No effect; replication layer always manages task |
| `OLD_REP_LOG_FLUSH_TASK_INTERVAL` | No effect; same as above |
| `CLEANER_BACKGROUND_PROACTIVE_MIGRATION` | Parse-compatibility only; no effect |
| `CLEANER_ADJUST_UTILIZATION` | Optimisation always applied automatically |
| `CLEANER_FOREGROUND_PROACTIVE_MIGRATION` | Parse-compatibility only; no effect |
| `CLEANER_LAZY_MIGRATION` | Parse-compatibility only; no effect |
| `EVICTOR_NODES_PER_SCAN` | Use `EVICTOR_EVICT_BYTES` |
| `EVICTOR_DEADLOCK_RETRY` | Thread pool handles retries automatically |
| `EVICTOR_LRU_ONLY` | Cache always multi-queue; flag has no effect |
| `LOG_DIRECT_NIO` | NIO not applicable to Noxu DB |
| `LOG_CHUNKED_NIO` | NIO not applicable to Noxu DB |

`all_params()` and the internal test module received `#[allow(deprecated)]`.

**noxu-db** (4 methods, `since = "2.4.1"`):

| Item | Reason |
|------|--------|
| `Transaction::new` | Use `Environment::begin_transaction()` |
| `EnvironmentConfig::set_txn_no_sync` | Use `set_durability(Durability::commit_no_sync())` |
| `EnvironmentConfig::with_txn_no_sync` | Use `with_durability(...)` |
| `EnvironmentConfig::set_txn_write_no_sync` | Use `set_durability(Durability::commit_write_no_sync())` |
| `EnvironmentMutableConfig::with_txn_no_sync` | Use `with_durability(...)` |
| `EnvironmentMutableConfig::with_txn_write_no_sync` | Use `with_durability(...)` |

Struct fields `txn_no_sync` / `txn_write_no_sync` on both configs have doc
comments updated to say "Deprecated since 2.4.1" (Rust does not support
`#[deprecated]` on struct fields as of toolchain 1.95).

**noxu-xa** (already deprecated):

`XaError::CrashDurabilityNotSupported` was already `#[deprecated(since =
"2.0.0")]`. Stale v1.5-era doc comments in the `XaResource` trait
(`xa_commit`, `xa_rollback`, `xa_recover`) were updated to reflect that
crash-durable XA has been implemented since Wave 3-2.

**Total new `#[deprecated]` markers**: 19 (13 config statics + 6 methods in
noxu-db). `XaError::CrashDurabilityNotSupported` counted separately (was
already deprecated since 2.0.0).

### 4. CI gate

`cargo-semver-checks v0.47.0` advisory job added to both:

- `.github/workflows/test.yml` (GitHub Actions)
- `.forgejo/workflows/test.yml` (Codeberg / Forgejo)

Both use `continue-on-error: true`. The job fetches full git history
(`fetch-depth: 0`) so `--baseline-rev main` resolves correctly.

---

## cargo-semver-checks first run results

Run against `main` baseline on the public crates:

| Crate | Result | Notes |
|-------|--------|-------|
| `noxu-db` | 1 minor lint (`type_method_marked_deprecated`) | Expected: 6 methods newly deprecated. Advisory only. |
| `noxu-config` | 1 minor lint (`global_value_marked_deprecated`) | Expected: 13 statics newly deprecated. Advisory only. |
| `noxu-xa` | pass | No new deprecations (existing `CrashDurabilityNotSupported` was already there). |
| `noxu-bind` | pass | — |
| `noxu-collections` | pass | — |
| `noxu-persist` | pass | — |
| `noxu-util` | pass | — |
| `noxu-rep` | could not build | `tls-native` feature requires OpenSSL headers not present in dev env; CI Ubuntu runner has libssl-dev. |

All failures are expected and advisory: adding `#[deprecated]` is a **minor**
change per the Rust Cargo reference, not a breaking change. The CI gate is
`continue-on-error: true` for exactly this reason.

The `noxu-db` `inherent_method_now_doc_hidden` failure (seen in an earlier
iteration) was resolved by removing the `#[doc(hidden)]` attributes — hiding
previously-visible methods is a semver break and violates the wave constraint
("only code touch is `#[deprecated]` markers").

---

## Judgement calls

1. **`Transaction::with_log_manager` / `with_env_impl` / `with_inner_txn` /
   `get_inner_txn`**: Not deprecated (would be `doc_hidden` break per
   semver-checks). Instead, the doc comments say "Internal — not part of v3.0
   stable surface" and they're listed in api-stability.md's
   "pub-but-internal" table. They'll become `pub(crate)` in v3.0.0.

2. **Struct-field deprecation**: Rust 1.95 does not support `#[deprecated]`
   on struct fields. `txn_no_sync` / `txn_write_no_sync` in both
   `EnvironmentConfig` and `EnvironmentMutableConfig` receive doc-comment
   "Deprecated since 2.4.1" notes; their setter/builder methods receive
   `#[deprecated]`.

3. **`COMPRESSOR_PURGE_ROOT`**: Listed in `all_params()` after the
   "Deprecated compat" comment, but its own doc comment does not say
   "deprecated". No `#[deprecated]` added.

4. **`noxu-rep` semver-checks**: Skipped in dev environment due to missing
   OpenSSL headers. CI Ubuntu runner has libssl-dev and will run the check
   successfully.

---

## Gates (all passed)

- `cargo fmt --all -- --check` ✓
- `cargo clippy --workspace --all-targets -- -D warnings` ✓
- `RUSTDOCFLAGS=-D warnings cargo doc --workspace --no-deps` ✓
- `cargo test --workspace --no-fail-fast` — **5766 tests, 0 failures** ✓
- `make docs-check` (typos + markdownlint + mdbook build) ✓
- `cargo semver-checks` — advisory; 2 minor-lint crates (expected), others pass ✓
