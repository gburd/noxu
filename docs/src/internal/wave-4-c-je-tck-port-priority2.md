# Wave 4-C — JE TCK Port, Priority-2 Packages

This wave ports JE `@Test` invariants from priority-2 packages onto Noxu DB,
focusing on test surfaces that became newly relevant after the v1.5
(Sprint 3C) and v1.6 (Wave 2B/2C-2) work landed:

- `bind/tuple` — TupleInput / TupleOutput round-trips and sort-key
  monotone-byte ordering.
- `bind/serial` — SerialBinding (now `SerdeBinding`), now relevant after
  Sprint 3C added the 2-byte version header.
- `collections` — StoredMap / StoredSortedMap / StoredKeySet /
  StoredValueSet semantics, now relevant after Wave 2B's typed-binding
  redesign.
- `persist` — Sequence / EntityStore / PrimaryIndex non-evolve
  invariants; the schema-evolution surface is already covered by
  Wave 2C-2's `evolve_test.rs`.
- `je.config` — `EnvironmentParamsTest`'s parameter validation,
  mutability, and defaults.

## Tests ported (43 total)

| Package | New file | Tests | Notes |
|---|---|---:|---|
| `bind/tuple` | `crates/noxu-bind/tests/tck_tuple_format.rs` | 12 | Format round-trip + monotone-ordering invariants from `TupleFormatTest` and `TupleOrderingTest` (21 JE rows collapse onto 12 Rust tests). |
| `bind/serial` | `crates/noxu-bind/tests/tck_serial_binding.rs` | 9 | `SerialBindingTest` invariants + 3 new tests for Sprint 3C's version-header guard. |
| `collections` | `crates/noxu-collections/tests/tck_collection_semantics.rs` | 12 | `Map.put` contract, snapshot-iterator semantics, sub-range scans, `Option<T>` null values, txn abort/commit, runner. |
| `persist` | `crates/noxu-persist/tests/tck_persist_operations.rs` | 10 + 1 ignored | Sequence monotonicity / persistence / per-name independence; read-only store; database-name listing; reopen-picks-up-data; idempotent put; count-after-CRUD; put_no_overwrite. |
| `je.config` | `crates/noxu-config/tests/tck_environment_params.rs` | 10 | `testValidation` (split into in-range / below / above / type-mismatch / unknown / mutability / defaults / custom-param). |

## Adaptations vs JE

- `bind/tuple`: Noxu's `write_string` uses a 2-byte terminator and
  `0x00 -> 00 01` escape, so JE's wire-size assertions (`val.length() + 1`)
  are intentionally omitted; the round-trip and ordering invariants are
  preserved.  JE's "null string" marker is JE-only (Noxu's API takes
  `&str`).
- `bind/serial`: Java's `ClassCatalog` / `SerialBinding(catalog, baseClass)`
  pattern collapses onto Noxu's parameterised `SerdeBinding<T>`.  The
  classloader-override invariant is replaced by the magic+version header
  guard (`SERDE_BINDING_MAGIC = 0xCB`, `SERDE_BINDING_VERSION = 0x01`).
- `collections`: Noxu's `Database` is not `Sync`, so the JE
  "two threads share an env" pattern is replaced with same-thread auto-
  commit-visibility checks; cross-thread sharing remains a Wave-5
  follow-up if the engine permits it.
- `persist`: `SequenceTest`'s per-primitive-type matrix collapses onto
  `u64`-only `Sequence` / `MemorySequence`.  Mutations / DPL entity-
  evolution coverage is already in `evolve_test.rs`.
- `je.config`: Java's `IllegalArgumentException` on invalid input maps
  onto a typed `ConfigError::{OutOfRange, TypeMismatch, UnknownParam,
  NotMutable}`.

## Real Noxu deviations surfaced

One genuine Noxu-vs-JE deviation was uncovered and committed as
`#[ignore]`d with a TODO citing the JE-expected behaviour:

- `tck_persist_read_only_store_reopens_without_allow_create`
  (`crates/noxu-persist/tests/tck_persist_operations.rs`) — JE permits
  opening a `StoreConfig::new(...).setReadOnly(true)` against an
  existing on-disk store with no `setAllowCreate(true)`; Noxu rejects
  the reopen with `DatabaseNotFound("...")` because the on-disk DB is
  not surfaced into the env's open-database set unless the open path
  also has `allow_create=true`.  The working test
  `tck_persist_read_only_store_rejects_writes` papers over the
  divergence by passing both flags.

## Per-package TSV updates

Updated rows: 36 newly `PORTED-EQUIVALENT` (status was `NOT-PORTED`)
across:

- `je-tck-port-2026-05-enumeration-bind.tuple.test.tsv` (+21)
- `je-tck-port-2026-05-enumeration-bind.serial.test.tsv` (+7)
- `je-tck-port-2026-05-enumeration-collections.test.tsv` (+2)
- `je-tck-port-2026-05-enumeration-persist.test.tsv` (+3)
- `je-tck-port-2026-05-enumeration-je.config.tsv` (+2)

Aggregate (`je-tck-port-2026-05-overview.md`):

- `PORTED-EQUIVALENT` 147 → **182** (+35 net; 36 status flips minus 1
  duplicate row collapsed onto an existing port).
- `NOT-PORTED` 1796 → **1761**.
- `PORTED-PARTIAL` and `OUT-OF-SCOPE` unchanged at 62 and 63.
