# Wave 4-B — JE TCK Port (Priority 1, Data Correctness)

**Branch**: `fix/wave4-b-je-tck-port-priority1`
**Status**: in-progress (initial pass)
**Date**: 2026-05-27

## Goal

Port a meaningful slice of Berkeley DB Java Edition's `@Test` methods from
the priority-1 packages (`je`, `je.cleaner`, `je.recovery`, `je.tree`,
`je.txn`, `je.dbi`, `je.log`, `je.evictor`) into Noxu's Rust test suite.
Each port asserts the **same invariant** as the JE original — not the same
Java syntax — using Noxu's public API.

A test is considered "PORTED-EQUIVALENT" when it would catch the same class
of regression as the JE original.  When a Noxu API divergence forces a
narrower assertion, the row is marked "PORTED-PARTIAL".  When the JE test
relies on a JE-internal class Noxu does not expose (e.g. `IN`, `Tree`,
`FileSummaryLN`, `LogManager.cleanLog`), or on a feature Noxu does not
support (custom byte comparators, exclusive_create, `env.compress()`), the
row is marked "OUT-OF-SCOPE".

## What was ported

### `crates/noxu-db/tests/je_sr_regression_test.rs` (2 tests)

| JE class | JE method | Status | Notes |
|---|---|---|---|
| `DbCursorDuplicateDeleteTest` | `testSR9900` | PORTED-EQUIVALENT | non-dup variant of `putCurrent` after `delete` |
| `DbCursorDuplicateDeleteTest` | `testSR9992` | PORTED-EQUIVALENT | sorted-dup variant |

JE asserts `putCurrent` returns `KEYEMPTY` after `delete`; Noxu has no
`KeyEmpty` status — `Cursor::put(_, _, Put::Current)` requires an
`Initialized` cursor and `delete()` resets the state to
`NotInitialized`, so the same regression surfaces as a `Cursor::put`
error.

### `crates/noxu-db/tests/je_recovery_sr_test.rs` (4 tests, 3 #[ignore])

| JE class | JE method | Status | Notes |
|---|---|---|---|
| `RecoveryAbortTest` | `testSR9752Part1` | PORTED-EQUIVALENT | passes |
| `RecoveryAbortTest` | `testSR9752Part2` | PORTED-PARTIAL | `#[ignore]` — Noxu bug |
| `RecoveryAbortTest` | `testSR9465Part1` | PORTED-PARTIAL | `#[ignore]` — Noxu bug |
| `RecoveryAbortTest` | `testSR9465Part2` | PORTED-PARTIAL | `#[ignore]` — Noxu bug |

The three `#[ignore]` tests document **real Noxu bugs** surfaced by the
ports (see "Bugs surfaced" below).

### `crates/noxu-db/tests/je_recovery_test.rs` (4 tests)

| JE class | JE method | Status |
|---|---|---|
| `RecoveryTest` | `testBasic` / `testBasicFewerCheckpoints` | PORTED-EQUIVALENT (collapsed) |
| `RecoveryTest` | `testDuplicateOverwrite` | PORTED-EQUIVALENT |
| `RecoveryTest` | `testSR8984Part1` | PORTED-EQUIVALENT (spirit) |
| `RecoveryTest` | `testSR8984Part2` | PORTED-EQUIVALENT (spirit) |

The SR8984 ports use `drop(env)` rather than JE's "no checkpoint at exit"
knob, but assert the equivalent invariant: a deleted record stays deleted
across the close+open cycle.

### `crates/noxu-db/tests/je_cursor_edge_test.rs` (5 tests, 2 #[ignore])

| JE class | JE method | Status |
|---|---|---|
| `CursorEdgeTest` | `testSearchOnDuplicatesWithDeletions` | PORTED-EQUIVALENT |
| `CursorEdgeTest` | `testSearchBothWithOneDuplicate` (JE SR9248) | PORTED-EQUIVALENT |
| `CursorEdgeTest` | `testGetPrevNoDupWithEmptyTree` (JE bug 11700) | PORTED-EQUIVALENT |
| `CursorEdgeTest` | `testReadDeletedUncommitted` | PORTED-PARTIAL `#[ignore]` (Noxu bug) |
| `CursorEdgeTest` | `testNonTxnalCursorNoUpdates` | OUT-OF-SCOPE (skipped) |

### `crates/noxu-db/tests/je_database_test.rs` (9 tests)

| JE class | JE method | Status |
|---|---|---|
| `DatabaseTest` | `testPutExisting` | PORTED-EQUIVALENT |
| `DatabaseTest` | `testZeroLengthData` | PORTED-EQUIVALENT (spirit) |
| `DatabaseTest` | `testDeleteNonDup` | PORTED-EQUIVALENT |
| `DatabaseTest` | `testDeleteDup` | PORTED-EQUIVALENT |
| `DatabaseTest` | `testDeleteAbort` | PORTED-EQUIVALENT |
| `DatabaseTest` | `testPutDuplicate` | PORTED-EQUIVALENT |
| `DatabaseTest` | `testPutNoDupData` | PORTED-EQUIVALENT (via cursor `Put::NoDupData`) |
| `DatabaseTest` | `testPutNoOverwriteInANoDupDb` | PORTED-EQUIVALENT |
| `DatabaseTest` | `testDatabaseCount` | PORTED-EQUIVALENT |

### `crates/noxu-db/tests/je_cursor_delete_test.rs` (4 tests)

| JE class | JE method | Status |
|---|---|---|
| `DbCursorDeleteTest` | `testSimpleDelete` | PORTED-EQUIVALENT |
| `DbCursorDeleteTest` | `testSimpleDeleteAll` | PORTED-EQUIVALENT |
| `DbCursorDeleteTest` | `testSimpleInsertDeleteInsert` | PORTED-EQUIVALENT |
| `DbCursorDeleteTest` | `testSimpleDeletePutCurrent` | PORTED-EQUIVALENT |

### `crates/noxu-txn/tests/lock_manager_test.rs` (6 tests appended)

| JE class | JE method | Status |
|---|---|---|
| `LockManagerTest` | `testSR15926LargeNodeIds` | PORTED-EQUIVALENT |
| `LockManagerTest` | `testNegatives` (3 of N invariants) | PORTED-PARTIAL |
| `LockManagerTest` | `testMultipleReaders` | PORTED-EQUIVALENT |
| `LockManagerTest` | `testUpgradeLock` | PORTED-EQUIVALENT |

(testNegatives is split across three Rust fns:
`je_negatives_repeat_read_returns_existing`,
`je_negatives_release_unrelated_lsn_is_noop`,
`je_negatives_release_by_non_owner_is_noop`.)

## Aggregate counts

Across the priority-1 enumeration TSVs (`je`, `je.dbi`, `je.recovery`,
`je.txn`):

```text
                                                  ported  partial  out-of-scope  not-ported
je-tck-port-...-je.tsv (199 total)                   29     22       1            147
je-tck-port-...-je.dbi.tsv (138 total)                9      0       0            129
je-tck-port-...-je.recovery.tsv (66 total)            9      3       0             54
je-tck-port-...-je.txn.tsv (74 total)                 6     20       0             48
```

Wave-4-B added: **+27 PORTED-EQUIVALENT, +5 PORTED-PARTIAL, +1 OUT-OF-SCOPE**
relative to the wave-1D snapshot.

The other priority-1 TSVs (`je.tree`, `je.cleaner`, `je.log`, `je.evictor`)
are unchanged in this wave because the high-priority remaining items in
those packages all rely on JE-internal classes (e.g. `FileSummaryLN`,
`LogManager.cleanLog`, `IN.findEntry`) that Noxu's public API does not
expose.  Internal-test coverage in those areas already exists in the
respective `crates/noxu-{tree,cleaner,log,evictor}` test directories
(see, e.g., `crates/noxu-tree/tests/bin_in_test.rs`).

## Bugs surfaced

The port surfaced **three real Noxu bugs**, each documented as an
`#[ignore]`-d test that captures the JE invariant.  The tests are
committed so that the regression coverage is in place; lifting the
`#[ignore]` is gated on a follow-up fix.

### NOXU-BUG-WAVE4B-1: aborted dup inserts persist on sorted-duplicates DBs

**Test**: `crates/noxu-db/tests/je_recovery_sr_test.rs::sr9752_part2_abort_after_committed_dups_reverts_with_dups`

**Symptom**: on a sorted-duplicates database, `db.put(Some(&txn), k, x)` for
`x` ∈ {x, y, z} followed by `txn.abort()` leaves `x`, `y`, `z` visible in
the dup chain.  `db.count()` returns 6 instead of 3.  The aborted dup-puts
are not rolled back.

**JE behaviour**: aborted dup inserts are removed; the dup chain reverts.

### NOXU-BUG-WAVE4B-2: aborted delete-then-reinsert corrupts the BIN

**Tests**:

* `crates/noxu-db/tests/je_recovery_sr_test.rs::sr9465_part1_delete_reinsert_abort_restores_no_dups`
* `crates/noxu-db/tests/je_recovery_sr_test.rs::sr9465_part2_delete_reinsert_redelete_abort_restores_no_dups`

**Symptom**: with N=50 committed records, a transaction that deletes them
all and re-inserts them, when aborted, leaves the database with `count()
== 0` while `get()` on individual keys returns a non-deterministic subset
of the originally-committed values.  The minimal repro:

```rust
// pre: 5 records (k=0..4, v=b"orig") committed.
let t = env.begin_transaction(None)?;
for i in 0..5 { db.delete(Some(&t), &k(i))?; }
for i in 0..5 { db.put(Some(&t), &k(i), &b"NEW"[..])?; }
t.abort()?;
// post: count()=0, get(k=0)=NotFound, get(k=1)=Some(b"orig"), ...
```

**JE behaviour**: after the abort, the database has all 5 records with
their original values.

### NOXU-BUG-WAVE4B-3: uncommitted delete is dirty-readable

**Test**: `crates/noxu-db/tests/je_cursor_edge_test.rs::cursor_edge_read_deleted_uncommitted`

**Symptom**: with T1 holding an uncommitted `delete(k)`, a no-wait T2's
`get(k)` returns `Ok(NotFound)` instead of failing with a lock error.  By
contrast, an uncommitted *overwrite* in T1 correctly produces a lock
conflict for T2.  The asymmetry: the write-lock acquired by `delete()` is
not contested on the read path the same way an overwrite's write-lock is.

**JE behaviour**: T2 sees `LockNotAvailableException` until T1 commits;
after commit, T2 sees `NOTFOUND`.

## Out-of-scope

The following classes of JE tests cannot be ported by-spirit using Noxu's
public API and are left as NOT-PORTED in the TSV:

* tests that depend on JE-internal classes (`IN`, `BIN`, `LogManager`,
  `FileSummaryLN`, `Cleaner.cleanLog`, `Tree.dump`, `EnvironmentImpl.compress`);
* tests that depend on custom byte-order comparators (`override_btree_comparator` is inert in Noxu since v1.6.0);
* tests that depend on `exclusive_create` (Noxu's `DatabaseConfig::exclusive` is a single-thread flag, not a creation-mode);
* tests that exercise the `cleanLog`/`evictMemory` public APIs Noxu does not expose;
* the JE replication tests (out-of-scope for wave 4-B; tracked under wave 4-A).

## Files added

* `crates/noxu-db/tests/je_sr_regression_test.rs` (2 tests)
* `crates/noxu-db/tests/je_recovery_sr_test.rs` (4 tests, 3 #[ignore])
* `crates/noxu-db/tests/je_recovery_test.rs` (4 tests)
* `crates/noxu-db/tests/je_cursor_edge_test.rs` (5 tests, 2 #[ignore])
* `crates/noxu-db/tests/je_database_test.rs` (9 tests)
* `crates/noxu-db/tests/je_cursor_delete_test.rs` (4 tests)
* 6 tests appended to `crates/noxu-txn/tests/lock_manager_test.rs`

**Total**: 34 ported tests (28 active + 6 documenting bugs/divergence).

## Files updated

* `docs/src/internal/je-tck-port-2026-05-enumeration-je.tsv`
* `docs/src/internal/je-tck-port-2026-05-enumeration-je.dbi.tsv`
* `docs/src/internal/je-tck-port-2026-05-enumeration-je.recovery.tsv`
* `docs/src/internal/je-tck-port-2026-05-enumeration-je.txn.tsv`

## Follow-up work

* Fix NOXU-BUG-WAVE4B-1 / -2 / -3 (abort-rollback paths and
  uncommitted-delete lock contention).  Each bug has an `#[ignore]`d test
  waiting to be re-enabled.
* Continue porting non-SR tests from the priority-1 TSVs (still ~370 NOT-PORTED rows in those four files combined).
* Port priority-2 packages (`bind`, `collections`, `persist`) under wave 4-C.
