# Wave 9-C — JE TCK Port (additional rows after wave 8)

**Branch**: `fix/wave9-c-je-tck-ports`
**Base**: `sprint/v2.2.0-base`
**Date**: 2026-05-27

## Goal

Continue the JE @Test enumeration port from waves 4-B / 4-C / 6 / 8.
Wave 9-C focuses on rows that were left as `NOT-PORTED` in earlier
waves but are reachable through Noxu's public API — TupleBindingTest /
TupleFormatTest / TupleOrderingTest fillers, CursorEdgeTest and
DatabaseConfigTest extras, RecoveryTest / RecoveryAbortTest extras,
DeadlockTest / LockTest, AtomicPutTest concurrency, and FileManagerTest
log-file-naming.

A test is considered "PORTED-EQUIVALENT" when it would catch the same
class of regression as the JE original.  When Noxu's API is narrower
the row is "PORTED-PARTIAL".  When Noxu cannot express the JE
behaviour (custom byte-comparators, BigInteger / BigDecimal bindings,
JE-internal proxies like `LatchSupport.nBtreeLatchesHeld`, JE-specific
GC / WeakHashMap semantics, or features Noxu has dropped like nested
transactions), the row is "OUT-OF-SCOPE".

## What was ported

### `crates/noxu-bind/tests/tck_tuple_format.rs` — 18 tests appended

`TupleFormatTest` round-trip ports (10):

| JE method | Noxu test |
|---|---|
| `testChars` | `tck_tuple_format_test_chars` |
| `testBytes` | `tck_tuple_format_test_bytes` |
| `testByte` | `tck_tuple_format_test_byte` |
| `testShort` | `tck_tuple_format_test_short` |
| `testFloat` | `tck_tuple_format_test_float` |
| `testDouble` | `tck_tuple_format_test_double` |
| `testSortedFloat` | `tck_tuple_format_test_sorted_float` |
| `testSortedDouble` | `tck_tuple_format_test_sorted_double` |
| `testSortedPackedInt` | `tck_tuple_format_test_sorted_packed_int` |
| `testSortedPackedLong` | `tck_tuple_format_test_sorted_packed_long` |
| `testUnsignedByte` | `tck_tuple_format_test_unsigned_byte` |
| `testUnsignedShort` | `tck_tuple_format_test_unsigned_short` |
| `testUnsignedInt` | `tck_tuple_format_test_unsigned_int` |

`TupleOrderingTest` extras (3):

| JE method | Noxu test |
|---|---|
| `testChars` | `tck_tuple_ordering_test_chars` |
| `testBytes` | `tck_tuple_ordering_test_bytes` |
| `testPackedIntAndLong` | `tck_tuple_ordering_test_packed_int_and_long` |

`TupleBindingTest` (1):

| JE method | Noxu test |
|---|---|
| `testPrimitiveBindings` | `tck_tuple_binding_test_primitive_bindings` |

The `BigInteger`, `BigDecimal`, `SortedBigDecimal`, `FixedString`, and
`NullString` rows were marked **OUT-OF-SCOPE**: Noxu has no
`BigInteger` / `BigDecimal` primitive bindings (no `java.math`
analogue), no fixed-length-string variant of `write_string`, and
no null-marker representation.

### `crates/noxu-db/tests/je_cursor_edge_test.rs` — 2 tests appended

| JE method | Noxu test | Status |
|---|---|---|
| `CursorEdgeTest.testNoWaitLatchRelease` | `cursor_edge_no_wait_latch_release` | PORTED-PARTIAL |
| `CursorEdgeTest.testGetCurrentDuringDupTreeCreation` | `cursor_edge_get_current_during_dup_tree_creation` | PORTED-PARTIAL |

`testNoWaitLatchRelease` is **PARTIAL** because JE additionally checks
`LatchSupport.nBtreeLatchesHeld() == 0` to guard against a latch leak.
Noxu has no equivalent public probe; we assert the user-visible
invariant (no-wait cursor delete fails with a lock error, txn remains
usable).  `testGetCurrentDuringDupTreeCreation` is **PARTIAL** because
the JE original uses `JUnitThread` to overlap T1's dup insert with T2's
fetchCurrent; the Rust port drives the same sequence single-threaded
and asserts that subsequent cursor reads see both dups without panic.

### `crates/noxu-db/tests/je_database_test.rs` — 3 tests appended

| JE method | Noxu test |
|---|---|
| `DatabaseConfigTest.testConfig` | `database_config_snapshot_after_open` |
| `DatabaseConfigTest.testIsTransactional` | `database_config_is_transactional` |
| `DatabaseConfigTest.testOpenReadOnly` | `database_config_open_read_only_rejects_writes` |

`testIsTransactional` was previously mistagged in the TSV as pointing
to a non-existent function (`crates/noxu-db/src/environment.rs::test_is_transactional`);
re-pointed to a real test that asserts `db.get_config().transactional`
under both implicit (auto-commit) and explicit txn handles.

### `crates/noxu-db/tests/je_recovery_test.rs` — 2 tests appended

| JE method | Noxu test |
|---|---|
| `RecoveryAbortTest.testInserts` | `recovery_abort_test_inserts_three_phase_no_dups` |
| `RecoveryTest.testBasicDeleteAll` | `recovery_basic_delete_all_no_resurrect` |

The JE `testInserts` includes an `INCompressorQueueSize` drain step
(force IN-delete replays during recovery) — Noxu has no equivalent
public probe so the port relies on the recovery pipeline doing the
equivalent work without explicit synchronisation.

### `crates/noxu-txn/tests/lock_manager_test.rs` — 3 tests appended

| JE method | Noxu test | Status |
|---|---|---|
| `DeadlockTest.testDeadlockBetweenTwoTxns` | `je_deadlock_between_two_txns` | PORTED-EQUIVALENT |
| `DeadlockTest.testDeadlockProducedByTwoLockersOnOneLock` | `je_deadlock_two_lockers_on_one_lock` | PORTED-EQUIVALENT |
| `LockTest.testLockConflicts` | `je_lock_test_conflicts_matrix` | PORTED-PARTIAL |

`testLockConflicts` is **PARTIAL**: JE's full matrix includes upgrade
mode (`LockType.RANGE_INSERT` etc.) and `DUPLICATE_RANGE` which Noxu's
public `LockType` enum does not expose.  The basic Read/Write
compatibility cells (Read-Read share, Read-Write conflict,
Write-Write conflict, Write-Read conflict) are asserted.

### `crates/noxu-log/tests/je_file_manager_test.rs` (new file, 4 tests)

| JE method | Noxu test |
|---|---|
| `FileManagerTest.testLastFile` | `je_file_manager_last_file_no_files` + `je_file_manager_last_file_skips_decoys` |
| `FileManagerTest.testFileNameFormat` | `je_file_manager_file_name_format_round_trips_via_listing` |
| `FileManagerTest.testFileCreation` | `je_file_manager_list_only_returns_ndb_files` |

Noxu uses `.ndb` instead of JE's `.jdb`, with the same 8-hex-digit
naming.  `testFollowingFile` was marked **OUT-OF-SCOPE**: Noxu's
`FileManager` does not expose `get_following_file_num`.

### `crates/noxu-db/tests/je_atomic_put_test.rs` (new file, 2 tests)

| JE method | Noxu test |
|---|---|
| `AtomicPutTest.testOverwriteNoDuplicates` | `je_atomic_put_overwrite_no_duplicates_concurrent` |
| `AtomicPutTest.testNoOverwriteWithDuplicates` | `je_atomic_put_no_overwrite_with_duplicates_concurrent` |

Both ports drive a 2-thread race over `MAX_KEY = 200` operations,
retrying on `LockConflict`.  The first asserts `put(OVERWRITE)` never
returns a non-Success status; the second asserts that the final
sorted-duplicates database contains no duplicate-of-duplicate
(no two identical (key, data) pairs) under any key.

### Documentation re-tagging (no new tests)

In addition to the substantive ports above, **11 rows** in
`persist.test` and `collections.test` were re-tagged from
`NOT-PORTED` to `PORTED-EQUIVALENT` / `PORTED-PARTIAL` /
`OUT-OF-SCOPE`.  These rows already had Rust analogues that the
earlier name-match heuristic missed:

* `ConvertAndAddTest.testConvertAndAddField` →
  `evolve_test.rs::convert_and_add_test`
* `DevolutionTest.testDevolution` →
  `evolve_test.rs::devolution_revert_schema`
* `EvolveProxyClassTest.test{Delete,Class,Field,Hierarchy}…` (×4) →
  `evolve_test.rs::evolve_proxy_class_test` (PARTIAL)
* `TransactionTest.testRunner{Commit,Abort}` →
  `tck_collection_semantics.rs::tck_collection_transaction_runner_*`
* `TransactionTest.testExplicit{Commit,Abort}` →
  `tck_collection_semantics.rs::tck_collection_*_writes_are_*`
* `TransactionTest.testReadCommitted{Collection,Transaction}` →
  `isolation_test.rs::test_read_committed_releases_lock_*`
* `TransactionTest.testReadUncommitted{Collection,Transaction}` →
  `isolation_test.rs::test_dirty_read_prevented_*`
* `TransactionTest.testNested` → **OUT-OF-SCOPE** (sprint3-1 removed
  nested transactions)
* `TransactionTest.testCurrentTransactionGC`, `TestSR15721.testSR15721Fix`
  → **OUT-OF-SCOPE** (Java GC / WeakHashMap semantics)

## Aggregate counts

| Bucket | Wave 8 end | Wave 9-C end | Δ |
|---|---:|---:|---:|
| PORTED-EQUIVALENT | 205 | ~243 | +38 |
| PORTED-PARTIAL | 89 | ~95 | +6 |
| OUT-OF-SCOPE | 64 | ~76 | +12 |
| NOT-PORTED | 1710 | ~1654 | -56 |

(Δ counts are approximate aggregates over the bind / collections /
je / je.log / je.recovery / je.test / je.txn / persist.test TSVs.
The exact deltas are encoded in those per-package TSVs.)

## Real Noxu bugs surfaced

**None.**  All 34 substantive new tests pass without `#[ignore]`.
The earlier wave-4-B `#[ignore]`d bugs (NOXU-BUG-WAVE4B-1 / -2 / -3)
remain in place; this wave did not touch them.

## Out-of-scope additions in this wave

The following JE tests were marked OUT-OF-SCOPE during wave 9-C:

| JE class | JE method | reason |
|---|---|---|
| `TupleFormatTest` | `testFixedString` | no fixed-length-string API |
| `TupleFormatTest` | `testNullString` | `&str` has no null marker |
| `TupleFormatTest` | `testBigInteger` / `testBigDecimal` / `testSortedBigDecimal` | no java.math primitive bindings |
| `TupleOrderingTest` | `testFixedString` | no fixed-length-string API |
| `TupleOrderingTest` | `testBigInteger` / `testSortedBigDecimal` | no java.math primitive bindings |
| `FileManagerTest` | `testFollowingFile` | no `get_following_file_num` public API |
| `TransactionTest` | `testNested` | nested txns removed in sprint3-1 |
| `TransactionTest` | `testCurrentTransactionGC` | Java WeakHashMap semantics |
| `TestSR15721` | `testSR15721Fix` | Java GC reachability |

## Files added

* `crates/noxu-log/tests/je_file_manager_test.rs` (4 tests)
* `crates/noxu-db/tests/je_atomic_put_test.rs` (2 tests)

## Files updated (test code)

* `crates/noxu-bind/tests/tck_tuple_format.rs` (+18 tests)
* `crates/noxu-db/tests/je_cursor_edge_test.rs` (+2 tests)
* `crates/noxu-db/tests/je_database_test.rs` (+3 tests)
* `crates/noxu-db/tests/je_recovery_test.rs` (+2 tests)
* `crates/noxu-txn/tests/lock_manager_test.rs` (+3 tests)

## Files updated (TSVs)

* `docs/src/internal/je-tck-port-2026-05-enumeration-bind.tuple.test.tsv`
* `docs/src/internal/je-tck-port-2026-05-enumeration-collections.test.tsv`
* `docs/src/internal/je-tck-port-2026-05-enumeration-je.tsv`
* `docs/src/internal/je-tck-port-2026-05-enumeration-je.log.tsv`
* `docs/src/internal/je-tck-port-2026-05-enumeration-je.recovery.tsv`
* `docs/src/internal/je-tck-port-2026-05-enumeration-je.test.tsv`
* `docs/src/internal/je-tck-port-2026-05-enumeration-je.txn.tsv`
* `docs/src/internal/je-tck-port-2026-05-enumeration-persist.test.tsv`

**Total**: 34 substantive new ports (32 PORTED-EQUIVALENT, 4
PORTED-PARTIAL across the new files) + 11 re-tagged rows + 9 newly
OUT-OF-SCOPE rows.

## Methodology

Same recipe as `wave-4-b-je-tck-port-priority1.md` and
`wave-6-je-tck-port-priority-3-4.md`:

1. Open the JE source, identify the invariant the test asserts.
2. Map JE classes/methods to Noxu types/methods.
3. Adapt or skip JE-specific machinery (e.g. `JUnitThread`,
   `LatchSupport`, `INCompressorQueueSize`, `DbInternal`,
   `EnvironmentImpl`); document the adaptation in the test header.
4. Port assertion shape verbatim where possible; weaken to the
   strongest invariant Noxu's API can express and mark
   PORTED-PARTIAL.
5. Run with `timeout 60 cargo test -p <crate> --test <name> --no-fail-fast`.
6. Update the per-package TSV row.
7. Commit per logical batch (5–18 tests).

## Gate status

Run at end of wave; see commit log on `fix/wave9-c-je-tck-ports`:

* `cargo fmt --all -- --check` — pass
* `cargo clippy --workspace --all-targets -- -D warnings` — pass
* per-crate test runs — all 34 new tests pass
* `make docs-check` — pass
