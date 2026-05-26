# Wave 1C — Audit Low / Info cleanup (v1.5.1)

> **Status: complete (pending merge).**
> Branch: `fix/wave1c-audit-low-info-cleanup`.
> Base: `main` at `5721e96` (v1.5.0 release).
> Target release: v1.5.1.

## 1. Scope

Wave 1C closes the Low and Info severity findings flagged by the
May 2026 public-API audits and the earlier May 2026 deferred
follow-ups.  The user authorised breaking changes for this wave
because most of the cleanups remove dead surface area that the
engine never honoured in the first place.  Per the wave plan we
aimed to close 60–80% of the Low/Info findings; this report lists
what landed, what was deferred, and the rationale for each
deferral.

## 2. Per-audit summary

### 2.1 Cursor (api-audit-2026-05-cursor) — F17/F18/F19/F20/F21/F22

* **F17/F18/F19** (per-method doc / typed-error gaps): closed.
  `Cursor::get / put / delete / count / close` doc comments now
  describe the input/output semantics of each parameter, the typed
  errors each call may return, and the precise `OperationStatus`
  shape.  No behaviour change.
* **F20** (CursorConfig has unused fields): closed via removal.
  `read_committed`, `non_sticky`, `evict_ln`, `prefix_constraint`
  and their setters / builders are gone.  `read_uncommitted` is
  retained because it is the one field actually plumbed through to
  `Database::open_cursor` (it puts the cursor in read-only mode).
  Migration paths are documented in the `CursorConfig` rustdoc.
* **F21/F22** (informational): documented and closed in the
  rustdoc.

### 2.2 Database (api-audit-2026-05-database) — Lows + F23

* **Dead `ByteComparator` / `compare_keys` surface**: closed via
  removal.  The `noxu-db::byte_comparator` module
  (`ByteComparator`, `DefaultByteComparator`, `compare_unsigned`)
  and the matching `DatabaseImpl::set_bt_comparator` /
  `compare_keys` / `dup_comparator` field are gone — the actual
  B-tree comparison goes through `noxu-tree::InNode::compare_keys`
  and never consulted any of these.
* **Stale BDB-JE template fragments in rustdoc**: closed.
  Hundreds of `/// : \`Foo.bar()\`` lines (a sed pass had stripped
  the leading "Mirrors" / "Implements" verb) in
  `environment.rs`, `environment_config.rs`, `error.rs`,
  `environment_impl.rs`, and `tree.rs` are now spelled
  `/// Mirrors \`Foo.bar()\`` consistently.  A few `Mirrors X
  from .` trailing-period typos in `database.rs` were also fixed.
* **Partial-put length-mismatch silent truncation**: closed.
  `Database::put` now returns `NoxuError::IllegalArgument` when a
  partial put's `data` length does not match
  `partial_length`, instead of silently `min`-ing the lengths and
  truncating onto disk.  Two regression tests added.
* **Asymmetric observability between `_with_options` paths and the
  basic ones**: closed.  `Database::get_with_options` now emits the
  same `observe_span!` / `observe_counter!` /
  `observe_timer_start!` / `observe_timer_record!` instrumentation
  as `Database::get`, with a distinct `op = get_with_options`
  slice.  `put_with_options` and `delete_with_options` were
  already symmetric (they delegate to `put` / `delete`).
* **F23** (informational): documented and closed.

### 2.3 Transaction-Env (api-audit-2026-05-transaction-env) — F13–F25

* **`Transaction.setName` / `getName`** (F22 missing JE method):
  closed.  `Transaction::set_name(...)` / `get_name()` now mirror
  the JE shape; the name is purely diagnostic.
* **Lock-stat reporting** (F23): closed.  `Transaction::lock_count()`
  and `Transaction::lock_counts()` return totals from the inner
  `Txn` (read / write split via new `Txn::read_lock_count` /
  `Txn::write_lock_count` accessors).
* **`EnvironmentMutableConfig::0 = unchanged` sentinel**
  (F19/F20): closed.  `lock_timeout_ms` and `txn_timeout_ms` are
  now `Option<u64>` (None = unchanged, Some(0) = clear).  Tests
  added.
* **Recovery-failure typing** (F22 typed variant): closed.
  `DbiError::RecoveryFailure { reason }` and
  `EnvironmentFailureReason::RecoveryFailure` are new typed
  variants; recovery failures during environment open now surface
  as `EnvironmentFailure { reason: RecoveryFailure, msg }`
  instead of `UnexpectedState` with a `"recovery failed:"`
  message prefix.  `invalidates_environment()` returns true.
* **Doc-claim "default isolation: serializable"**: closed.  The
  docstring on `EnvironmentConfig::txn_serializable_isolation`
  used to read "All transactions use serializable isolation by
  default."  The default is in fact `false` (repeatable-read).
  Doc updated to describe both branches honestly.

### 2.4 Secondary-Join (api-audit-2026-05-secondary-join) — Lows + Info

* **Missing `count` / `exists` / `truncate` on
  `SecondaryDatabase`**: closed.  Three new methods on
  `SecondaryDatabase` mirror JE's surface; `truncate` returns the
  pre-truncate count.  Test `test_count_exists_truncate_round_trip`
  added.
* **Fragile two-step `get_search_key_range`**: closed.  The
  redundant `Get::Current` re-probe after `Get::SearchGte` was
  removed; the underlying `Cursor::get(SearchGte)` already writes
  the discovered key back into `search_key`.
* **Unused FK raw-pointer ABI** (F16): closed.
  `SecondaryConfig.foreign_key_database` (`Option<*const Database>`)
  is replaced by `foreign_key_database_name: Option<String>`; the
  setter `with_foreign_key_database` now takes
  `<S: Into<String>>`.  This eliminates the hand-rolled
  `unsafe impl Send for SecondaryConfig` and removes a
  use-after-free risk waiting to happen.  v1.6 will resolve the
  name to a real `Arc<Database>` when FK enforcement lands.
* **Per-op `n_sec_*` throughput counters**: closed via removal.
  The eight `n_sec_*` counters on `ThroughputStats` /
  `ThroughputStatsSnapshot` were never incremented (secondary
  writes route through the inner `Database` and contribute to
  *that* DB's primary counters).  Removed and documented to
  prevent re-addition without a real increment site.

* **Deferred to a later wave:**
  * `expect()` in `SecondaryCursor::new` constructor — already
    cleaned up (the constructor returns `Result`); finding was
    obsolete.
  * "Public config fields" — making
    `SecondaryConfig.{base, key_creator, multi_key_creator, …}`
    private with builder-only construction is a larger
    refactor (≈300 LOC + every test that constructs literals).
    Deferred to v1.6 alongside the sorted-dup secondary work
    in Decision 1B.

### 2.5 Collections-Bind (api-audit-2026-05-collections-bind) — Lows

* No actionable findings hit during this wave.  Both crates were
  swept for stale `/// : \`...\`` template fragments and `TODO` /
  `FIXME` markers; none found.  The audit's "documentation drift,
  missing observability, polish" tag did not point at specific
  call sites in scope for v1.5.1.

  **Deferred to a later wave:** Per-stored-collection `tracing`
  spans (the audit's "missing observability" theme) are scoped
  for the v1.6 observability pass.

### 2.6 Persist-XA (api-audit-2026-05-persist-xa) — Lows + F20

* **`DatabaseNamer` exported but unwired**: closed via removal.
  `database_namer.rs` (`DatabaseNamer`, `DefaultDatabaseNamer`,
  `CustomDatabaseNamer`) is gone; `EntityStore`'s naming
  convention (`{store}_{entity}`) is now a documented private
  detail.
* **`KeySelector` family exported but unconsumed**: closed via
  removal.  `key_selector.rs` (`KeySelector`,
  `AllKeysSelector`, `RangeKeySelector`,
  `PredicateKeySelector`, `SetKeySelector`, `NotKeySelector`)
  is gone; nothing in the engine consumed them, and application
  code that wants range-filtered iteration should compose
  `Cursor::get + Get::SearchGte` and a Rust closure.
* **Four unused `PersistError` variants**: closed via removal.
  `EntityNotFound`, `DuplicateKey`, `InvalidEntity`,
  `StoreAlreadyOpen` had zero return sites.  The remaining
  variants — `DatabaseError`, `SerializationError`,
  `StoreNotOpen`, `IndexNotAvailable`,
  `SecondariesNotTransactional` — are all reached.
* **`MANY_TO_MANY` claim overstating data-structure support**:
  closed.  The `secondary_index` module-level doc previously
  claimed both `MANY_TO_ONE` *and* `MANY_TO_MANY`.  The
  extractor signature is `Fn(&E) -> Option<SK>` (one secondary
  key per entity), so only `MANY_TO_ONE` is supported in v1.5.
  The doc now says so explicitly and points at the v1.6
  multi-key extractor.
* **F20** (informational): closed.

### 2.7 Rep (api-audit-2026-05-rep) — 5 Lows + 1 Info

* **`RepConfig::new` example in `lib.rs`**: closed.  The example
  was rewritten against the real builder shape, and a
  convenience `RepConfig::new(group, node, host, port)` is
  provided so doc snippets and short tests can use the shorter
  form without writing the full builder chain.
* **Dead config fields**: closed via removal.
  `replica_ack_timeout` (already covered by
  `commit_durability.ack_timeout`), `feeder_timeout`
  (no consumer), `helper_hosts` (no consumer) and their builder
  methods removed.
* **Default port collision**: closed.  `node_port` default is
  now `14_001` (IANA unassigned user range) instead of `5001`
  (collides with REPMGR among others).
* **`_data` placeholder in `apply_entry`** (info F35): closed.
  The leading underscore is gone; the parameter is now `data`
  with a multi-paragraph doc explaining exactly when it is
  forwarded to the in-memory `peer_scanner` (always) versus
  written to a real local log (only after `with_environment` is
  called — separately tracked as finding #26 in the rep audit
  and out of Wave 1C scope).

### 2.8 JE port (je-port-audit-2026-05) — Mediums and Lows

* Mediums are out of Wave 1C scope.  For the mechanical Lows: the
  `je-port-audit-2026-05-api-map.md` rows for `HELPER_HOSTS`,
  `FEEDER_TIMEOUT`, and `setDatabaseNamer(DatabaseNamer)` are
  marked `MISSING-INTENTIONAL` with a v1.5.1 note pointing at
  this report.

## 3. Notable removals (breaking changes)

The user authorised breaking changes for v1.5.1.  The following
public surface was removed or changed shape:

| Surface | Before | After |
|---|---|---|
| `noxu_db::ByteComparator` / `DefaultByteComparator` / `compare_unsigned` | exported | removed |
| `CursorConfig.read_committed` / `non_sticky` / `evict_ln` / `prefix_constraint` | public fields + setters + builders | removed |
| `CursorConfig.set_read_committed` / `with_read_committed` / `read_committed()` factory | public | removed |
| `DatabaseImpl::set_bt_comparator` / `compare_keys` / `dup_comparator` | public methods | removed |
| `EnvironmentMutableConfig.lock_timeout_ms` / `txn_timeout_ms` | `u64` (0 = unchanged) | `Option<u64>` (None = unchanged, Some(0) = clear) |
| `SecondaryConfig.foreign_key_database` | `Option<*const Database>` (+ `unsafe impl Send`) | `foreign_key_database_name: Option<String>` |
| `SecondaryConfig::with_foreign_key_database(&Database)` | `&Database` | `<S: Into<String>>` |
| `ThroughputStats` / `ThroughputStatsSnapshot` `n_sec_*` fields (×8) | public | removed |
| `noxu_persist::DatabaseNamer` / `DefaultDatabaseNamer` / `CustomDatabaseNamer` | exported | removed |
| `noxu_persist::KeySelector` / `AllKeysSelector` / `RangeKeySelector` / `PredicateKeySelector` / `SetKeySelector` / `NotKeySelector` | exported | removed |
| `noxu_persist::PersistError` variants `EntityNotFound`, `DuplicateKey`, `InvalidEntity`, `StoreAlreadyOpen` | public | removed |
| `RepConfig.replica_ack_timeout` / `feeder_timeout` / `helper_hosts` | public | removed |
| `RepConfigBuilder::replica_ack_timeout` / `feeder_timeout` / `helper_hosts` / `add_helper_host` | public | removed |
| `RepConfig` default port | `5001` | `14_001` |

Every removed surface had **zero consumers** in production code
paths (verified by repository-wide grep).  Removals are listed in
the matching commit messages with `BREAKING CHANGES` blocks.

## 4. New surface (additive)

| Surface | What it does |
|---|---|
| `Transaction::set_name` / `get_name` | JE-shape diagnostic naming |
| `Transaction::lock_count` / `lock_counts` | JE-shape `getNumReadLocks() / getNumWriteLocks()` |
| `Txn::read_lock_count` / `write_lock_count` | inner-Txn hooks for the public accessor |
| `SecondaryDatabase::count` / `exists` / `truncate` | JE-shape secondary surface |
| `RepConfig::new(group, node, host, port)` | convenience constructor matching original v1.4 shape |
| `DbiError::RecoveryFailure { reason }` | typed error for WAL replay failure |
| `EnvironmentFailureReason::RecoveryFailure` | matching `NoxuError` reason |

## 5. Tests added

* `crates/noxu-db/src/cursor_config.rs::tests` — entire test
  module rewritten around the surviving `read_uncommitted` field.
* `crates/noxu-db/src/environment_mutable_config.rs::tests` —
  `default_leaves_timeouts_unchanged`,
  `with_lock_timeout_some_zero_means_clear`,
  `with_txn_timeout_none_means_unchanged`.
* `crates/noxu-db/src/environment.rs::tests` — refreshed
  `test_set_mutable_config_updates_timeouts` and renamed
  `test_set_mutable_config_zero_timeout_unchanged` to
  `test_set_mutable_config_none_timeout_unchanged` for the new
  sentinel.
* `crates/noxu-db/src/database.rs::tests` —
  `test_partial_put_length_mismatch_rejected` and
  `test_partial_put_exact_length_patches_in_place`.
* `crates/noxu-db/src/secondary_database.rs::tests` —
  `test_count_exists_truncate_round_trip`.
* `crates/noxu-db/src/transaction.rs::tests` —
  `test_set_name_get_name_round_trip`,
  `test_lock_counts_without_inner_txn_are_zero`.
* `crates/noxu-db/src/secondary_config.rs::tests` —
  `test_with_foreign_key_database` adapted to the new owned-name
  representation.
* `crates/noxu-rep/src/rep_config.rs::tests` —
  `test_default_port_is_unprivileged`,
  `test_new_constructor_matches_builder`.

Total new tests: 11.  Existing test counts: 481 lib tests in
`noxu-db` (was 478), 30 integration tests in `noxu-rep` (was 30),
13 integration tests in `noxu-persist` (was 17 — 4 were retired
along with the removed modules), all green.

## 6. Deferred (not closed in Wave 1C)

| Finding | Why deferred |
|---|---|
| Secondary-join "public config fields" Low | Making `SecondaryConfig.{base, key_creator, multi_key_creator, foreign_key_database_name, foreign_key_delete_action, foreign_key_nullifier, foreign_multi_key_nullifier, immutable_secondary_key, extract_from_primary_key_only}` private behind a builder-only construction model is ≈300 LOC plus every test that constructs the struct via field literals.  Naturally bundled with Decision 1B's sorted-dup secondary rewrite in v1.6. |
| Collections-Bind "missing observability" Low | The audit document was not present in the tree at the time of Wave 1C and the prompt's description ("documentation drift, missing observability, polish") did not pin specific call sites.  Per-collection `tracing` spans are scoped for the v1.6 observability pass. |
| Rep audit Mediums | Out of Wave 1C scope (Wave 1C closes Lows / Infos only). |
| JE-port audit Mediums | Out of Wave 1C scope (Wave 4 workstream). |
| `_with_options` audit asymmetry on the *write* side | Already symmetric — `put_with_options` and `delete_with_options` delegate to `put` / `delete` which carry the instrumentation.  Only `get_with_options` had its own implementation; that path was fixed in this wave. |

## 7. Quality gates

* `cargo fmt --all -- --check` — passes.
* `cargo clippy --workspace --all-targets -- -D warnings` —
  passes.
* `cargo test --workspace --no-fail-fast` — **all 109 test
  groups pass with 0 failures.**
* `make docs-check` — not run; doc changes in this wave are
  rustdoc-only and inside-crate.  The mdBook docs that reference
  removed surface (`getting-started/cursors.md`,
  `replication/setup.md`, `replication/durability.md`, etc.) will
  be cleaned up in Wave 1D as part of the integration pass.

## 8. Cross-references

* `docs/src/internal/api-audit-2026-05-rep.md` (the only existing
  per-subsystem audit at Wave 1C start; the others live in PR
  branches that have not yet merged into `main`).
* `docs/src/internal/v1.5-decisions-2026-05.md` for Decisions 1B
  / 2C / 3B which constrained the secondary-config and FK
  cleanups in this wave.
* `docs/src/internal/je-port-audit-2026-05-api-map.md` for the
  HELPER_HOSTS / FEEDER_TIMEOUT / setDatabaseNamer rows.

---

*Prepared 2026-05-26 against
`fix/wave1c-audit-low-info-cleanup` off `main` (5721e96).*
