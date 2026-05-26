# Sprint 3C — `noxu-collections` and `noxu-bind` scope-down for v1.5

> **Status.** Branch `fix/sprint3-collections-scope-down`, base
> `sprint/v1.5-rc1`.  Five commits.  All audit findings either
> closed by code/doc change or explicitly DEFERRED to v1.6 with
> rationale.

## 1. Why this sprint exists

The May 2026 collections / bind API audit
(`docs/src/internal/api-audit-2026-05-collections-bind.md`,
referenced by the sprint plan) raised eight High findings against
`noxu-collections` and `noxu-bind`.  The v1.5 architectural
decisions doc
(`docs/src/internal/v1.5-decisions-2026-05.md`) approved a
*smaller-but-honest* v1.5: ship the auto-commit-only Stored*
surface, document the limitations loudly, and move the typed-API

+ transactional redesign to v1.6.

Sprint 3C is the implementation of that decision.

## 2. Findings touched

| Finding | Subject | Disposition | Where |
|---|---|---|---|
| #1 | Stored* ops hard-code `txn=None` | **Documented as v1.5 limitation; threading deferred to v1.6** | crate-level + per-type rustdoc; `docs/src/collections/*.md` |
| #3 | `TransactionRunner` supplies a `&Transaction` no Stored* method accepts | **Documented as v1.5 limitation; runner restricted to raw `Database` API** | `transaction_runner.rs` rustdoc; `docs/src/collections/README.md` |
| #4 | Same as #3 viewed from Stored* side | **Documented; deferred to v1.6** | as above |
| #5 | `StoredList::remove` rustdoc claimed compaction; body just deletes | **CLOSED** by aligning rustdoc with no-compaction behaviour and adding a regression test | `stored_list.rs` |
| #6 | `StoredList::next_index` resets to 0 on reopen; pushes overwrite existing records | **CLOSED** by new `StoredList::open(&db) -> Result<Self>` recovery path; `new` retained as fast/empty-DB constructor with a warning rustdoc | `stored_list.rs`; `tests/sprint3c_tests.rs` |
| #19 | `SerdeBinding` ships no version tag; struct-shape changes silently corrupt | **CLOSED** by 2-byte `[magic, version]` header + new typed `BindError::VersionMismatch` error.  **Breaking on-disk change.** | `crates/noxu-bind/src/error.rs`; `crates/noxu-bind/src/serial/serde_binding.rs` |

The four other High findings in the audit (typed API surface for
`StoredMap<K, V>` / `StoredSet<K>` / `StoredList<V>`, and the
companion of #1 that touches `StoredSortedMap`) are handled by the
documentation pass: the published mdBook examples were rewritten
to match the actual `&[u8]`-keyed surface, and a "v1.5 limitations"
section was added per chapter.  These are not promoted to
audit-closed status because the typed surface is the v1.6 target.

## 3. Decision matrix — what we picked, why

### 3.1 Step 1 — collections contract

| Option | Picked | Rationale |
|---|---|---|
| (a) Thread `Option<&Transaction>` through every Stored* op now | No | Big-but-mechanical API change; breaks every existing v1.4.x caller; would land alongside the typed-API redesign in v1.6 anyway. |
| (b) Document Stored* as auto-commit only in v1.5; revisit in v1.6 | **Yes** | Audit's own recommendation; the decisions-doc preferred shape; smallest risk for v1.5; no public-API surface change. |

### 3.2 Step 2 — `StoredList::remove` doc-vs-body mismatch

| Option | Picked | Rationale |
|---|---|---|
| (a) Implement compaction | No | Compaction is v1.6 work; expensive to land safely without the txn-threading from #1. |
| (b) Update rustdoc to match the no-compaction body | **Yes** | Honest, small, regression-tested. |

The regression test (`test_remove_does_not_reclaim_slot_on_push`)
asserts the actual contract: after `remove(idx)`, the next `push`
lands at `next_index` (the high-water mark), not at the freed slot,
and `next_index` is unchanged.

### 3.3 Step 3 — `StoredList::next_index` non-persistence

| Option | Picked | Rationale |
|---|---|---|
| (a) Implement persistence | **Yes** | Fits in <60 LOC.  Done as a single `Get::Last` cursor read inside a new `StoredList::open` constructor; recovers `next_index` from the largest existing 8-byte big-endian key. |
| (b) Document the limitation and reject reopen | No | Better path was available; option (a) was the spec's preferred branch when the budget allowed. |

`StoredList::new` is preserved with an updated rustdoc that warns
about the reopen hazard.  Two regression tests pin the contract:
one for the recovery path (`open`), one for the hazard documented
on `new` (`stored_list_new_does_not_recover_and_overwrites_on_reopen`).

`open` returns `CollectionError::IllegalState` if the largest key
in the database is not 8 bytes long, so users can't accidentally
clobber an unrelated database that happened to be passed to a
`StoredList`.

### 3.4 Step 4 — `SerdeBinding` schema versioning

| Option | Picked | Rationale |
|---|---|---|
| (a) 2-byte version prefix + typed error on mismatch | **Yes** | Stops silent corruption.  Spec-preferred; small change; clear migration story. |
| (b) Document the limitation only | No | Users would still see silent wrong-shaped values; the audit explicitly asked for a typed error. |

This is a **breaking on-disk change**: pre-3C records do not
carry the header and will fail to decode under v1.5 with
`BindError::VersionMismatch`.  Migration guidance is in
`docs/src/getting-started/bindings.md#serdebinding-version-prefix-v15`
and in commit message of the `feat(bind)!:` commit.  The change is
**not** full schema evolution: changing a struct's shape without
bumping `SERDE_BINDING_VERSION` will still produce wrong-shaped
values silently — the version prefix only catches
inter-binding-format drift, not intra-format struct changes.
Full schema evolution remains v1.6 work.

### 3.5 Step 5 — docs

`docs/src/collections/{stored-map,stored-set,stored-list}.md` were
rewritten:

+ The unimplemented `StoredMap<K, V>` / `StoredSet<K>` /
  `StoredList<V>` typed surface was removed; examples now show the
  actual `&[u8]`-keyed API.
+ Each chapter gained a "v1.5 limitations" section pointing at the
  closed/deferred audit findings.
+ `docs/src/collections/README.md` got a top-level "v1.5
  collections — what's in scope" summary so users see all five
  restrictions in one place.
+ `docs/src/getting-started/bindings.md` got the
  `SerdeBinding` version-prefix format and migration story.

`TransactionRunner` was *not* removed — it remains useful for
sequencing raw `Database` / `Cursor` calls with deadlock retry —
but its rustdoc now states unambiguously that the supplied
`&Transaction` cannot be threaded into Stored* methods in v1.5,
and the runner's example was updated to use the raw `Database`
API.

### 3.6 Step 6 — tests

Added under `crates/noxu-collections/tests/sprint3c_tests.rs`:

+ `stored_map_ops_succeed_without_txn_argument` — auto-commit API
  shape guard for findings #1, #3, #4.  When v1.6 redesigns the
  signatures this test will need to be updated, which is the
  signal we want.
+ `stored_list_ops_succeed_without_txn_argument` — same for
  `StoredList`.
+ `stored_list_open_recovers_next_index_after_reopen` — finding
  #6 fix.  Two-session test: write 3 entries, close, reopen with
  `open`, assert recovered `next_index` and a non-clobbering push.
+ `stored_list_new_does_not_recover_and_overwrites_on_reopen` —
  finding #6 hazard pin.  Documents what `new` actually does so a
  future change to `new`'s contract is a deliberate, visible API
  change.
+ `stored_list_open_on_empty_database_starts_at_zero` —
  empty-database parity with `new`.
+ `stored_list_open_rejects_mixed_use_database` — non-8-byte
  largest key returns `IllegalState`.

Added under `crates/noxu-bind/src/serial/serde_binding.rs`:

+ `test_encoded_payload_starts_with_version_header` — wire-format
  guard.
+ `test_decode_unprefixed_payload_returns_version_mismatch` — the
  audit-asked-for regression test: an old payload decodes to a
  typed error, not a wrong-shaped value.
+ `test_decode_short_payload_returns_version_mismatch` — 0/1-byte
  payloads also surface as `VersionMismatch`.
+ `test_decode_wrong_version_returns_version_mismatch` — right
  magic, wrong version still fails.
+ `test_version_mismatch_display` — formatted error names both
  expected and found bytes.

In-module test fixed in `stored_list.rs`:

+ `test_remove` extended to assert `next_index() == 3` after
  `remove(1)` on a 3-element list (the existing test was silent
  about this).
+ New `test_remove_does_not_reclaim_slot_on_push` documents the
  "remove leaves a hole, push uses next_index" contract.

Tightened `BindError` test surface lives in the new
`serde_binding` tests above; `noxu-bind`'s round-trip tests all
still pass because round-trip exercises the new prefix transparently.

## 4. Deferred to v1.6 (rationale)

| Audit finding | Deferred work | Why |
|---|---|---|
| #1 | Thread `Option<&Transaction>` through every Stored* operation | Lands together with the typed-API redesign; doing it twice would burn a SemVer break twice.  v1.5 ships honest auto-commit. |
| #3 / #4 | Make `TransactionRunner::run`'s `&Transaction` actually drive Stored* methods | Same dependency; resolves naturally once Stored* methods accept `Option<&Transaction>`. |
| (chapter-only) | Typed `StoredMap<K, V>` / `StoredSet<K>` / `StoredList<V>` surface | The mdBook described it; the source never implemented it.  Sprint 3C realigned the docs with reality.  The typed surface is v1.6 work, paired with the txn-threading. |
| (StoredList) | Compaction in `StoredList::remove` | Cheap to do mechanically but pointless without the v1.6 txn-threading; users who need compaction in v1.5 should rewrite via `pop` + `push` under their own discipline. |
| (SerdeBinding) | Full schema evolution | The 2-byte prefix only catches inter-format drift; intra-format struct-shape changes still silently corrupt.  Real schema evolution is a separate workstream (catalog table + per-version decoders) and belongs in v1.6 alongside the typed-API work. |

## 5. Files touched

+ `crates/noxu-collections/src/lib.rs` — crate-level v1.5
  limitations rustdoc.
+ `crates/noxu-collections/src/stored_map.rs` — type-level v1.5
  limitations.
+ `crates/noxu-collections/src/stored_sorted_map.rs` — same.
+ `crates/noxu-collections/src/stored_key_set.rs` — same.
+ `crates/noxu-collections/src/stored_value_set.rs` — same.
+ `crates/noxu-collections/src/stored_list.rs` — type-level
  limitations + `open` constructor + `remove` rustdoc fix +
  in-module tests.
+ `crates/noxu-collections/src/transaction_runner.rs` — runner
  v1.5 limitations + example update.
+ `crates/noxu-collections/tests/sprint3c_tests.rs` — new.
+ `crates/noxu-bind/src/error.rs` — new
  `BindError::VersionMismatch` variant.
+ `crates/noxu-bind/src/serial/serde_binding.rs` — version-prefix
  encode/decode + tests.
+ `docs/src/collections/README.md` — top-level v1.5 summary.
+ `docs/src/collections/stored-map.md` — rewrite to actual API.
+ `docs/src/collections/stored-set.md` — same.
+ `docs/src/collections/stored-list.md` — same + `open` example.
+ `docs/src/getting-started/bindings.md` — `SerdeBinding` version
  prefix format + migration.
+ `docs/src/internal/sprint-3-collections-restriction.md` — this
  document.

Out of scope (per the sprint plan): `noxu-db`, `noxu-persist`,
`noxu-xa`.  None were touched.

## 6. Commits

1. `docs(collections): scope Stored* to auto-commit only for v1.5`
2. `fix(collections): correct StoredList::remove rustdoc to match no-compaction body`
3. `feat(collections): persist StoredList next_index via StoredList::open`
4. `feat(bind)!: version-prefix SerdeBinding payloads (audit #19)`
5. `docs(internal): record sprint 3C scope-down`
