# Wave 2A — Secondary Database Unification (Decision 1B / audits C2 + C3 + C4)

**Date:** 2026-05-27
**Branch:** `fix/wave2a-redo2-secondary-unification`
**Status:** complete (steps 1-11)

## Scope

Wave 2A closes three coupled audit findings the v1.5 release shipped as
honest gaps:

* **Decision 1B / audit C4** — sorted-dup secondary indexes.  Pre-v1.6 a
  given secondary key could map to at most one primary record; a second
  primary that produced the same secondary key surfaced
  `NoxuError::Unsupported`.
* **Audit C3** — automatic `associate()`-style maintenance.  Pre-v1.6
  applications had to call `SecondaryDatabase::update_secondary` after
  every `Database::put` / `Database::delete`; missing the call left the
  index stale.
* **Decision 2C / audit C2** — foreign-key constraints (Abort / Cascade /
  Nullify).  Pre-v1.6 `SecondaryDatabase::open` rejected any FK config
  with `NoxuError::Unsupported`.

## Step-by-step ledger

| Step | Commit | Summary |
|---|---|---|
| 1 | `2d4e574` | Inner secondary DB requires `with_sorted_duplicates(true)`; `insert_sec_key` switches to `Put::NoDupData`; `delete_sec_key` uses `Get::SearchBoth`.  Test refit: `d1b_secondary_dup_admits_multiple_primaries` replaces the old `d1b_secondary_collision_returns_unsupported`.  Also fixes a latent bug in `cursor_impl::put_dup` so sorted-dup inserts now register with the txn (`lock_write_before_log` + `finalize_write_lock`); without this, abort-undo could leak sorted-dup secondary entries past a rolled-back txn. |
| 2 | `68124f7` | `SecondaryCursor::get_next_dup_full` / `get_prev_dup_full` walk every primary that shares the cursor's current secondary key. |
| 3 | `55b3a5b` | Move `SecondaryDatabase` shared fields into `Arc<SecondaryHookState>`; define `SecondaryHook` trait; `Database` carries `Arc<RwLock<Vec<Weak<dyn SecondaryHook>>>>`; `SecondaryDatabase::open` registers itself.  No call sites yet drive maintenance through the registry. |
| 4 | `a7e1789` | `Database::put` walks `live_secondaries()` and calls `SecondaryHook::maintain(txn, pk, None, Some(new_data))` after the primary write — insert-direction auto-maintenance. |
| 5 | `939337a` | `Database::delete` captures pre-delete values per duplicate iteration and forwards `(Some(old_data), None)` to every secondary under the caller's txn. |
| 6 | `fe955a9` | Update path: `Database::put` reads the pre-put value via `self.get(txn, key, …)` before the overwrite and forwards `(Some(old_data), Some(new_data))` so the state-side `update_secondary` can compute the delete-old + insert-new diff. |
| 7 | `20715e2` | Multi-key creator regression test (`c3_multi_key_creator_auto_maintained_on_put_and_update`).  No code change — `SecondaryHookState::update_secondary` already supports `SecondaryMultiKeyCreator`. |
| 8 | `9fb947c` | FK Abort.  `SecondaryConfig::with_foreign_key_database_handle(Arc<Mutex<Database>>)` (new) records the foreign primary; `SecondaryDatabase::open` registers an `FkReferrer` weak; `Database::delete` walks `live_fk_referrers()` and calls `on_foreign_key_deleted(txn, key)` *before* the delete is applied.  Returns typed `NoxuError::ForeignConstraintViolation` if any child secondary still references the foreign key. |
| 9 | `2ead09e` | FK Cascade with cycle detection.  Thread-local `FK_CASCADE_GUARD: HashSet<(db_id, fk_bytes)>` is acquired before the cascade and released on completion; an already-in-flight frame short-circuits the recursion.  Transitive cascades (root → mid → leaf) work via re-entry into the auto-maintenance plumbing. |
| 10 | `83b734b` | FK Nullify (single-key + multi-key).  Walks the inner index, fetches each child primary's data under the caller's txn, dispatches to `ForeignKeyNullifier` or `ForeignMultiKeyNullifier`, and re-puts the modified record.  Auto-maintenance on the child handles cleaning the now-stale secondary entry. |
| 11 | this doc + style commit `28607ce` | Documentation. |

## API changes (BREAKING source-level)

* **Inner secondary DB must be sorted-dup.**  Every call site that opens
  the inner index must use
  `DatabaseConfig::new().with_allow_create(true).with_sorted_duplicates(true)`.
  `SecondaryDatabase::open` returns `NoxuError::IllegalArgument`
  otherwise.
* **`SecondaryConfig::with_foreign_key_database_handle(Arc<Mutex<Database>>)`**
  is the runtime-enforcing FK setter.  The legacy
  `with_foreign_key_database(name)` setter is retained but is now
  advisory-only; combining `name` without `handle` is rejected with
  `IllegalArgument`.
* **`SecondaryCursor::get_next_dup_full` / `get_prev_dup_full`** are new
  public methods that return the `(sec_key, p_key, data)` triple for the
  next / previous primary sharing the cursor's current secondary key.

## Files touched

* `crates/noxu-db/src/secondary_database.rs` — sorted-dup `insert_sec_key`,
  `delete_sec_key`, `SecondaryHookState` + `Arc` refactor, `SecondaryHook`
  and `FkReferrer` traits + impls, FK Abort/Cascade/Nullify on
  `on_foreign_key_deleted`.
* `crates/noxu-db/src/secondary_cursor.rs` — `get_next_dup_full` /
  `get_prev_dup_full`.
* `crates/noxu-db/src/secondary_config.rs` — `foreign_key_database`
  field, `with_foreign_key_database_handle` setter,
  `has_foreign_key_config` updated.
* `crates/noxu-db/src/database.rs` — registries (`secondaries`,
  `fk_referrers`); `register_secondary` / `live_secondaries` /
  `register_fk_referrer` / `live_fk_referrers` /
  `db_id_for_fk_guard`; auto-maintenance fan-out in `put` and
  `delete`; FK referrer pre-check in `delete`.
* `crates/noxu-dbi/src/cursor_impl.rs` — `put_dup` NoDupData /
  NoOverwrite registers with the txn (the fix that lets sorted-dup
  secondaries roll back atomically with the primary).
* `crates/noxu-db/tests/secondary_decisions_test.rs` — refit and
  expand: 27 tests (was 12).
* Test-helper updates in `crates/noxu-db/src/secondary_cursor.rs`,
  `crates/noxu-db/src/join_cursor.rs`,
  `crates/noxu-db/tests/integration_test.rs`,
  `crates/noxu-db/tests/cursor_test.rs`,
  `crates/noxu-db/src/secondary_database.rs` (in-module tests).

## Audit findings closed

* **C2** — foreign-key constraint enforcement.
* **C3** — `associate()`-style automatic secondary maintenance.
* **C4** — sorted-dup secondary indexes.

## Tests

* `cargo test -p noxu-db --no-fail-fast` → 754 passed, 0 failed.
* `cargo test --workspace --no-fail-fast` → 5300 passed, 0 failed.
* `cargo clippy --workspace --all-targets -- -D warnings` → clean.
* `cargo fmt --all -- --check` → clean.

## Caveats / future work

* `SecondaryDatabase::open` requires the foreign primary handle as
  `Arc<Mutex<Database>>`.  The legacy name-only setter is preserved
  for symmetry with the JE configuration shape but does not by itself
  activate enforcement.
* FK cycle detection uses a thread-local guard.  Multi-threaded
  concurrent foreign-key cascades that touch the same `(db_id,
  fk_value)` frame from different threads will not see each other's
  guard frame; the lock manager still serialises the actual
  mutations, so the worst case is a temporarily-larger cascade fan-out
  rather than an unsoundness.
* The `update_secondary` manual API is preserved for population-style
  workflows (offline import, re-key passes) and for application code
  that wants to short-circuit auto-maintenance for performance reasons
  on bulk rebuilds.
