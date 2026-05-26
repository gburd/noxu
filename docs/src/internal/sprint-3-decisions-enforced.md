# Sprint 3D — v1.5 architectural decisions enforced

> **Branch:** `fix/sprint3-enforce-decisions`
> **Base:** `sprint/v1.5-rc1`
> **Inputs:** [`v1.5-decisions-2026-05.md`](v1.5-decisions-2026-05.md)
> **Output:** three commits, each scoped to a single decision.

## Purpose

Sprints 1 and 2 cleared the cheapest critical findings (cursor txn
threading). Sprint 3D enforces the architectural decisions that the
project owner signed off in `v1.5-decisions-2026-05.md` so users who
reach for unsupported v1.5 features get a clear typed error rather
than silently-broken behaviour. No version bump; the decisions are
purely behavioural and rustdoc/mdBook contract changes.

## Decisions enforced

| ID | Decision | Where the rejection fires | Audit findings closed |
|---|---|---|---|
| **1B** | Secondaries are one-to-one in v1.5; sorted-dup is a v1.6 feature. | `SecondaryDatabase::insert_sec_key` uses `Put::NoOverwrite`; collisions surface as `NoxuError::Unsupported` in `update_secondary` / `populate_if_empty`. Idempotent re-inserts of the same `(sec_key, pri_key)` pair remain a no-op. | C4 |
| **2C** | Foreign-key constraints rejected in v1.5; full FK support in v1.6 alongside Decision 1B's sorted-dup work. | `SecondaryDatabase::open` calls `SecondaryConfig::has_foreign_key_config()` and returns `NoxuError::Unsupported` if any FK field is set. The setters remain chainable for forward source compatibility. | C2, F1, F16 |
| **3B** | Nested transactions rejected in v1.5; `parent` parameter retained until v2.0. | `Environment::begin_transaction` returns `NoxuError::Unsupported` if `parent.is_some()`. The parameter is renamed from `_parent` to `parent` and the rustdoc explicitly documents the rejection. | F11 |

## Surface area touched

```text
crates/noxu-db/src/environment.rs                       (Decision 3B)
crates/noxu-db/src/secondary_config.rs                  (Decision 2C — setter rustdocs + has_foreign_key_config helper)
crates/noxu-db/src/secondary_database.rs                (Decisions 1B + 2C — module/struct/impl rustdocs, open() FK rejection, insert_sec_key one-to-one)
crates/noxu-db/src/join_cursor.rs                       (Decision 1B — mark v1.6-feature test #[ignore])
crates/noxu-db/tests/txn_wiring_test.rs                 (Decision 3B — regression tests f11_*)
crates/noxu-db/tests/secondary_decisions_test.rs (NEW)  (Decisions 1B + 2C — regression tests d1b_* / d2c_*)
docs/src/getting-started/secondary-databases.md         (Decisions 1B + 2C — v1.5 limitations section)
docs/src/transactions/basics.md                         (Decision 3B — nested-txn limitation note)
docs/src/transactions/secondary-with-txn.md             (one-line pointer to v1.5 limitations)
```

Out of scope (handled by parallel sprint agents):
`noxu-collections`, `noxu-persist`, `noxu-xa`. Out of touch list per
the task brief.

## NoxuError::Unsupported

The variant was introduced by Sprint 1A for cursor variants
(`docs/src/internal/api-audit-2026-05-cursor.md` Finding 3) and is
reused unchanged for Sprint 3D. Its display string is
`"operation not yet supported: <message>"`.

## Test changes

Newly added regression tests:

- `crates/noxu-db/tests/txn_wiring_test.rs::f11_nested_transaction_returns_unsupported`
- `crates/noxu-db/tests/txn_wiring_test.rs::f11_nested_transaction_none_still_works`
- `crates/noxu-db/tests/secondary_decisions_test.rs::d1b_secondary_collision_returns_unsupported`
- `crates/noxu-db/tests/secondary_decisions_test.rs::d1b_one_to_one_happy_path`
- `crates/noxu-db/tests/secondary_decisions_test.rs::d1b_same_primary_idempotent_reinsert_ok`
- `crates/noxu-db/tests/secondary_decisions_test.rs::d2c_foreign_key_database_rejected_at_open`
- `crates/noxu-db/tests/secondary_decisions_test.rs::d2c_foreign_key_delete_action_cascade_rejected_at_open`
- `crates/noxu-db/tests/secondary_decisions_test.rs::d2c_foreign_key_nullifier_rejected_at_open`
- `crates/noxu-db/tests/secondary_decisions_test.rs::d2c_no_fk_config_opens_normally`

Existing tests adjusted:

- `crates/noxu-db/src/secondary_database.rs::tests::test_get_by_secondary_key`
  reworded its primary-record fixture so each record uses a distinct
  first byte; the old fixture (`pk1=Apple`, `pk3=Avocado`) collided on
  `'A'` and depended on the silent-overwrite behaviour Decision 1B
  removes.
- `crates/noxu-db/src/join_cursor.rs::tests::test_join_intersection_finds_single_match`
  is gated with `#[ignore = "requires v1.6 sorted-dup secondaries; see
  Decision 1B / audit F7"]`. The test asserts a true dup intersection
  the v1.5 one-to-one model cannot represent; it will be re-enabled
  when sorted-dup secondaries land in v1.6.

## Breaking-change semantics

All three commits are tagged `fix(db)!` — the bang signals an
observable behaviour change that may be visible to users on the
small set of code paths that the audit confirms are dead today:

- **3B / F11:** zero callers in `noxu-*` or `examples/` pass
  `Some(parent)` to `begin_transaction`. Production blast radius is
  zero; only mdBook prose mentioned the parent.
- **2C / C2-F1-F16:** zero call sites in production code consult
  the FK fields, per the audit's repo-wide search. Users who set
  the fields and depended on the silent no-op will now see a typed
  rejection at open.
- **1B / C4:** `SecondaryDatabase::insert_sec_key` switched from
  `Put::Overwrite` to `Put::NoOverwrite`. Two distinct primaries
  that produce the same secondary key now hit the typed
  `Unsupported` error instead of the second silently overwriting
  the first.

The bang is included so consumers of the changelog see the
behaviour change without having to read the bodies.

## Sprint 4½ addendum — partial-atomicity gap closed

Sprint 3D's table above noted that Decision 1B's `Put::NoOverwrite`
semantics rejected cross-primary collisions, but it left the v1.4-era
pattern where `SecondaryDatabase::update_secondary` itself ran
auto-committed under any caller's transaction. The Sprint 4 mdBook
reconciliation explicitly called this out as a real partial-atomicity
gap (audit Theme 2 / finding F5): an aborted primary `put` could
leave the secondary entry behind on disk, because the secondary
update had already been auto-committed.

Sprint 4½ (`fix/sprint4-half-secondary-atomic`) closes the gap for
the **manual-update pattern**:

- `SecondaryDatabase::update_secondary`,
  `SecondaryDatabase::delete_all_for_primary` (crate-internal),
  `insert_sec_key`, `delete_sec_key`, and the `make_inner_cursor`
  helper now take `Option<&Transaction>` as the leading argument and
  forward it to every inner-database operation (`Database::put`,
  `Database::delete`, `Database::open_cursor`).
- When the caller threads the **same** `txn` through
  `Database::put` / `Database::delete` *and*
  `SecondaryDatabase::update_secondary`, the primary write and every
  affected secondary index entry commit or abort together.
- The Decision-1B idempotent-re-insert behaviour is preserved under
  transactional cursors: `Put::NoOverwrite` returns `KeyExists`
  identically on auto-commit and transactional cursors, and the
  existing probe path distinguishes idempotent vs cross-primary
  collision before raising `NoxuError::Unsupported`.

New regression tests in
`crates/noxu-db/tests/secondary_decisions_test.rs`:

- `s4h_abort_rolls_back_primary_and_secondary` — the explicit
  partial-atomicity pin; pre-fix this test fails because the
  secondary entry survives the abort.
- `s4h_commit_persists_primary_and_secondary` — happy-path commit
  variant.
- `s4h_same_primary_idempotent_reinsert_under_same_txn` — carries
  forward `d1b_same_primary_idempotent_reinsert_ok` to the
  transactional cursor.
- `s4h_uncommitted_secondary_write_is_not_visible_to_other_readers`
  — isolation-contract spot-check.

**Out of scope for Sprint 4½:** automatic `associate()`-style
secondary maintenance — where `Database::put` itself drives every
attached secondary inside the caller's transaction. That row of the
capability matrix remains ❌ for v1.5 and is tracked for v1.6
alongside Decision 1's sorted-dup work.

**Breaking change:** `SecondaryDatabase::update_secondary` gains a
leading `txn: Option<&Transaction>` parameter. Callers that adopted
the v1.4 / v1.5.0-rc1 / v1.5.0-rc2 shape must prepend either `None`
(preserves auto-commit semantics) or `Some(&t)` (atomic with
`Database::put(Some(&t), …)`). The fix commit is tagged
`fix(db)!:` accordingly. No version bump (v1.5.0 has not shipped).
