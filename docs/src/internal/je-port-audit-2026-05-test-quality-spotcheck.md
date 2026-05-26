# JE → Noxu Port-Completeness Audit — May 2026 — Test Quality Spotcheck

> Companion to `je-port-audit-2026-05-overview.md`. For six matched
> JE↔Noxu test pairs, this document compares what invariants the
> JE test asserts vs what the Noxu test asserts and flags gaps.
>
> The goal of this spotcheck is to give the project owner a concrete
> "is the port faithful or skin-deep?" answer for question 3 from the
> overview. It is a sample, not a proof. Six pairs out of ~440 mapped
> JE classes is ~1.4 % coverage; if any pair flagged a critical gap
> we would surface it here.

The six pairs were chosen to span the user-visible subsystems:

1. **CursorTest** (JE) ↔ `cursor_test.rs` (Noxu) — cursor lifecycle
   and traversal
2. **TxnTest / LockManagerTest** (JE) ↔ `txn_test.rs` /
   `lock_manager_test.rs` (Noxu) — transactions and locking
3. **CleanerTest / FileSelectionTest** (JE) ↔ `cleaner_test.rs`
   (Noxu) — cleaner / file selector
4. **TupleOrderingTest** (JE) ↔ `prop_tests.rs` /
   `primitive_bindings.rs#[cfg(test)]` (Noxu) — tuple binding
   ordering
5. **CollectionTest** (JE) ↔ `collection_tests.rs` (Noxu) —
   collections views
6. **ElectionsTest** (JE) ↔ `cluster_integration_test.rs` /
   `chaos_test.rs` (Noxu) — replication elections

---

## Spotcheck 1 — CursorTest

| | JE | Noxu |
|---|---|---|
| File | `je/test/com/sleepycat/je/CursorTest.java` | `crates/noxu-db/tests/cursor_test.rs` |
| Test method count | 23 `@Test` | 46 `#[test]` |
| Lines | ~2000 (substantial) | ~750 |

### What JE asserts

`CursorTest` is dominated by 16 phantom-prevention tests
(`testPhantomInsert*GetNext*` × 4, `testPhantomInsertGetPrev*` × 4,
`testPhantomDelete*` × 4, `testPhantomDup*` × 4). Each spins up two
threads sharing a 2-BIN tree, holds a cursor on one BIN, mutates the
adjacent BIN with the second thread, and asserts the resulting key
returned by getNext / getPrev. The remaining 7 tests cover:

- `testGetConfig` — config round-trip on cursor
- `testBasic` / `testMulti` — single-DB and multi-DB insert/scan
  with file size yanked down to ~1KB to force many log files
- `testDbInternalSearch` / `testDbInternalSearchBoth` — internal
  search positioning
- `testInsertionDuringGetNextBinDuringRangeSearch` — insertion
  during a range scan
- `testGetStorageSize` — `Cursor.getStorageSize()` accuracy

### What Noxu asserts

`cursor_test.rs` is structured as 7 numbered groups:

1. **Cursor lifecycle** (5 tests) — initial state, valid before
   positioning, read-write default, state after first get, state
   after close
2. **Empty-database behaviour** (2 tests) — first / last on empty
3. **Cursor get** (5 tests) — first/last with single record,
   next-at-end, prev-at-beginning, forward/backward iteration
4. **Cursor search** (4 tests) — exact, missing, GTE positioning
5. **Cursor put** (3 tests) — overwrite, no-overwrite, current
6. **Cursor delete** (2 tests) — delete current, delete leaves
   neighbours
7. **GTE oracle / brute-force** (~10 tests) — every inter-key gap,
   long-prefix seeds, walk-to-next-bin, brute-force over small
   random data

### Invariant overlap

| Invariant family | JE tests | Noxu tests | Verdict |
|---|---|---|---|
| Cursor lifecycle (open/state/close) | implicit (no dedicated tests) | 5 dedicated tests | **Noxu stronger** |
| Empty-DB behaviour | implicit | 2 dedicated tests | **Noxu stronger** |
| Forward/backward iteration | sampled in `testBasic` | 2 dedicated tests + iteration over 100 records | **Noxu stronger** |
| Search exact / GTE | `testDbInternalSearch*` | 8 dedicated tests + brute-force oracle | **Noxu stronger** |
| Put overwrite / no-overwrite / current | (covered by `Database` tests) | 3 dedicated tests | **Noxu stronger** |
| Delete current | (covered indirectly) | 2 dedicated tests | **Noxu stronger** |
| **Phantom prevention** (16 JE tests) | 16 dedicated tests | NONE in cursor_test.rs | **GAP — HIGH** |
| getStorageSize | 1 test | (none — feature not exposed) | GAP — LOW |
| Multi-DB cursor scan with small files | `testMulti` | (none) | GAP — MEDIUM |

### Spotcheck 1 verdict

The Noxu cursor tests are **stronger than JE on cursor lifecycle and
search positioning**, but **completely lack phantom-prevention
testing under concurrent BIN mutation**. JE devotes 16 of its 23
tests to this; Noxu has zero. There is some phantom coverage in
`isolation_test.rs::test_serializable_prevents_non_repeatable_read`
and `test_serializable_read_lock_blocks_writer_no_wait`, but those
are not the same scenarios — they test single-record locking, not
cursor-on-BIN-edge phantom insertion.

**Severity: HIGH** for the phantom gap. This is a real
data-correctness invariant family with no Noxu coverage.

---

## Spotcheck 2 — TxnTest / LockManagerTest

| | JE TxnTest | JE LockManagerTest | Noxu txn_test.rs | Noxu lock_manager_test.rs |
|---|---|---|---|---|
| Test count | 12 `@Test` | 12 `@Test` | 47 `#[test]` | (separate file) |
| Lines | ~1500 | ~800 | ~600 | ~800 |

### What JE asserts (TxnTest)

- `testBasicLocking` — locker memory accounting; thin-lock overhead
  before/after lock acquire/release
- `testLockMutation` — read→write upgrade and demote
- `testCommit` — txn commit flushes log, updates LSN
- `testAbortNoSplit` — abort restores tree state
- `testTransactionName` — `setName`/`getName` round-trip
- `testSyncCombo` — every combination of (envSync, txnSync) durability
- `testOneLevelDurabilityComboErrors` /
  `testMultiLevelLocalDurabilityComboErrors` — invalid durability
  combos throw
- `testLocalDurabilityCombo` — every Durability constructor variant
- `testNoWaitConfig` — txn with no-wait fails immediately on lock
  conflict
- `testRepeatingOperationFailures` — re-issue on the same operation
- `testPossiblyCommittedState` — txn left in indeterminate state
  after env crash

### What JE asserts (LockManagerTest)

- `testSR15926LargeNodeIds` — lock LSNs with sign bit set
- `testNegatives` — negative tests for `isOwner`, `isLocked`, etc.
- `testMultipleReaders` / `testMultipleReadersSingleWrite{1,2}` —
  reader sharing and writer exclusion
- `testUpgradeLock` — read→write upgrade
- `testNonBlockingLock{1,2}` — no-wait grant matrix
- `testWaitingLock` — blocking acquire wakes when prior holder
  releases
- `testLockConflictInfo` — conflict info attached to the conflict
- `testImportunateTxn{1,2}` — high-priority txn pre-empts holder

### What Noxu asserts (txn_test.rs)

47 `#[test]` covering:

- Txn lifecycle (initial state, commit, abort, double-commit
  rejection, abort idempotence) — 8 tests
- LSN return on commit/abort with/without LogManager — 2 tests
- Write log entry tracking (`note_log_entry`, `has_logged_entries`,
  `last_lsn`) — 4 tests
- Lock acquire / promote / demote / release on commit / release on
  abort — 6 tests
- Isolation flags (`serializable`, `read_committed`, `importunate`)
  — 6 tests
- Hooks (pre-commit, post-commit, fire-on-commit-with-logged-entry,
  no-fire-for-read-only) — 4 tests
- Durability variants (sync / no-sync / write-no-sync without
  LogManager) — 3 tests
- Cursor-open guards (commit-with-open-cursor fails, count tracks
  register/unregister) — 4 tests
- Undo log collection (read-only no undo, write-lock-info populates
  undo on abort) — 2 tests
- Plus 8 misc

### What Noxu asserts (lock_manager_test.rs)

~25 `#[test]` covering:

- Grant types (NEW, EXISTING) — 2
- Read+read sharing — 1
- Read→write promotion — 1
- Non-blocking failure modes — 3
- Release accounting — 4
- Demote — 2
- Multiple-locker conflict matrices — 6
- Total-locks counting — 2
- Plus 4 misc

### Invariant overlap

| Invariant family | JE | Noxu | Verdict |
|---|---|---|---|
| Lifecycle | implicit | 8 dedicated | **Noxu stronger** |
| Commit log durability flush | `testCommit`, `testSyncCombo` | `txn_wiring_test.rs::f3_*_durability_*` | EQUIVALENT |
| Abort rolls back tree | `testAbortNoSplit` | `isolation_test.rs::test_aborted_transaction_full_rollback` | EQUIVALENT |
| Read+read share, read+write conflict | `testMultipleReaders*` | `lock_manager_test.rs` | EQUIVALENT |
| Read→write upgrade | `testUpgradeLock`, `testLockMutation` | dedicated test | EQUIVALENT |
| No-wait lock failure | `testNonBlockingLock*`, `testNoWaitConfig` | `txn_config_test.rs::test_no_wait_causes_immediate_lock_failure`, `lock_manager_test.rs::non_blocking_*` | **Noxu stronger** |
| Importunate / pre-emption | `testImportunateTxn{1,2}` | `txn_test.rs::set_importunate_*` (config-level only, no preemption test) | **GAP — MEDIUM** |
| Locker memory accounting | `testBasicLocking` | (none — Noxu's MemoryBudget tested elsewhere) | GAP — LOW |
| Txn name set/get | `testTransactionName` | (none — `setName`/`getName` not ported) | GAP — LOW |
| Possibly-committed state | `testPossiblyCommittedState` | (none) | GAP — MEDIUM |
| Sign-bit-set LSN locks | `testSR15926LargeNodeIds` | (none — Noxu uses Lsn struct) | GAP — LOW |
| Lock conflict info attached | `testLockConflictInfo` | (partial — error has limited info) | GAP — LOW |

### Spotcheck 2 verdict

The Noxu txn / lock manager tests are **equivalent or stronger** on
the data-path invariants (acquire / release / share / promote /
no-wait). The gaps are in **edge-case behaviour**: importunate-txn
preemption, locker memory accounting, possibly-committed state. These
correspond to features that AGENTS.md acknowledges as deferred or
not-fully-implemented.

**Severity: MEDIUM** for the importunate / possibly-committed gap.

---

## Spotcheck 3 — CleanerTest / FileSelectionTest

| | JE CleanerTest | JE FileSelectionTest | Noxu cleaner_test.rs |
|---|---|---|---|
| Test count | 17 `@Test` | 20 `@Test` | 34 `#[test]` |
| Lines | ~2500 | ~3000 | ~500 |

### What JE asserts (CleanerTest)

The 17 tests build a real environment with a 10 KB file size cap,
write enough data to force multiple log files, then run the cleaner
and verify:

- `testCleanerNoDupes` / `testCleanerWithDupes` — basic cleaning
  reclaims space; dups don't break the cleaner
- `testCleanInternalNodes` — INs (not just LNs) are migrated and
  obsolete entries reclaimed
- `testCleanFileHole` — cleaner tolerates a hole in the file
  numbering
- `testSR13191` — historical regression: cleaner deadlock with
  checkpoint
- `testCleanerStop` — cleaner shuts down promptly when env closes
- `testFileSelectorMemBudget` / `testTrackerMemoryBudget` /
  `testFileSummaryLNMemoryUsage` — memory accounting of
  cleaner-tracking structures
- `testCleanLogReadOnly` — cleaner refuses to run on read-only env
- `testUnexpectedFileDeletion` — cleaner tolerates external rm
- `testMutableConfig` — cleaner config can be changed at runtime
- `testUtilizationDuringCheckpoint` /
  `testEvictionDuringCheckpoint` — concurrent checkpoint+cleaner /
  eviction+cleaner do not corrupt utilization
- `testMultiCleaningBug` — historical regression: two cleaners on
  the same file
- `testOptimizedFileSummaryLNDeletion` — FileSummaryLN is updated
  in place, not appended
- `testCompactBINAfterMigrateLN` — BIN is compacted after the
  cleaner migrates an LN out

### What JE asserts (FileSelectionTest)

20 tests around file-selection-cost-benefit:
`testBaseline*` / `testBasic*` / `testCleaningMode` / `testRetry*` /
`testMinFileUtilization` / `testSteadyStateAutomatic*` /
`testSteadyStateManual*` / `testSteadyStateHighUtilization*` /
`testProtectedFileRange` / `testTruncateDatabase` /
`testRemoveDatabase` / `testForceCleanFiles` / `testLogVersionUpgrade`
/ `testCompressionBug`.

### What Noxu asserts (cleaner_test.rs)

34 unit-level tests grouped:

1. FileSelector empty state (3)
2. FileSelector add/select (4)
3. FileSelector status transitions (5)
4. FileSelector required-utilization (3)
5. FileSelector utilization% calculations (4)
6. FileSelector clear (1)
7. FileSummary basic (4)
8. CleanerThrottle EWMA (6)
9. ProtectedFile range (4)

### Invariant overlap

| Invariant family | JE | Noxu | Verdict |
|---|---|---|---|
| FileSelector lifecycle (add/select/mark/delete) | implicit in CleanerTest | 5 dedicated tests | **Noxu stronger** |
| FileSelector status transitions | implicit | 5 dedicated tests | **Noxu stronger** |
| Utilization % math | `testUtilization*` (mixed) | 4 dedicated unit tests | **Noxu stronger at unit level** |
| Throttle EWMA | (none — Noxu invention) | 6 dedicated tests | NEW (Noxu only) |
| ProtectedFile range | `testProtectedFileRange` | 4 dedicated | EQUIVALENT |
| **Full-system cleaner-on-real-env** | every test | NONE in cleaner_test | **GAP — MEDIUM** |
| Cleaner+checkpoint concurrency | `testUtilizationDuringCheckpoint`, `testEvictionDuringCheckpoint` | `noxu-db/tests/sustained_load_test.rs::test_checkpoint_under_load_30s`, `noxu-spec::cache_vs_cleaner` | PARTIAL |
| Multi-cleaning-on-same-file | `testMultiCleaningBug` | (none) | GAP — MEDIUM |
| Cleaner+memory-budget | 3 tests | (none) | GAP — LOW |
| Cleaner+IN migration | `testCleanInternalNodes`, `testCompactBINAfterMigrateLN` | (none) | GAP — HIGH |
| Read-only cleaner refusal | `testCleanLogReadOnly` | (none) | GAP — LOW |
| External file deletion tolerance | `testUnexpectedFileDeletion` | (none) | GAP — MEDIUM |
| Force-clean / utilization-driven | `testForceCleanFiles`, `testMinFileUtilization` | `cleaner_test.rs::file_selector_check_for_required_util_*` | PARTIAL |

### Spotcheck 3 verdict

The Noxu cleaner tests are **stronger at the unit / data-structure
level** — every part of FileSelector / FileSummary / Throttle has
direct unit-test coverage that JE only exercises implicitly. But
the Noxu tests have **no full-system cleaner-on-real-env coverage**
at the level JE devotes to it. The closest analogue is
`noxu-db/tests/sustained_load_test.rs::test_cleaner_reduces_log_files_under_load`,
which is a single integration test of ~30 seconds.

**Severity: MEDIUM**. The data-structure correctness is well covered,
but the integration-level invariants (cleaner + checkpoint + eviction +
external-fs-events) are not.

---

## Spotcheck 4 — TupleOrderingTest

| | JE | Noxu |
|---|---|---|
| File | `je/test/com/sleepycat/bind/tuple/test/TupleOrderingTest.java` | `crates/noxu-bind/tests/prop_tests.rs` + `crates/noxu-bind/src/tuple/sort_key.rs#[cfg(test)]` + `primitive_bindings.rs#[cfg(test)]` |
| Test method count | 21 `@Test` | 13 `proptest!` blocks + ~70 unit tests |

### What JE asserts

The class has a `check()` helper: every tuple written must be
**strictly less than** (by byte comparison) the previous tuple.
Each `@Test` writes a hand-curated array of values that crosses the
"interesting" boundaries (Byte.MIN, Short.MIN, Integer.MIN, -1, 0, 1,
Byte.MAX, Short.MAX, Integer.MAX, etc.) in increasing order, then
asserts the encoded byte sequences are also in increasing
lexicographic order.

Coverage: String (var-length), FixedString, Chars, Bytes, Boolean,
UnsignedByte, UnsignedShort, UnsignedInt, Byte, Short, Int, Long,
Float, Double, SortedFloat, SortedDouble, PackedInt, PackedLong,
SortedPackedInt, SortedPackedLong, BigInteger, SortedBigDecimal.

### What Noxu asserts

`prop_tests.rs` has property-based tests:

- `prop_int_binding_round_trip(v: i32)` — round-trip
- `prop_long_binding_round_trip(v: i64)` — round-trip
- `prop_string_binding_round_trip(v in "[^\x00]*")` — round-trip
  (excludes embedded null because of historic StringBinding bug,
  per audit-report.md)
- `prop_sorted_float_round_trip(v: f32)` — round-trip
- `prop_sorted_double_round_trip(v: f64)` — round-trip
- `prop_sorted_int_encoding_order(a: i32, b: i32)` —
  **a < b ⇒ encoded(a) <_lex encoded(b)** (ordering invariant)
- `prop_sorted_double_encoding_order(a: f64, b: f64)` — ordering
- `prop_sorted_float_encoding_order(a: f32, b: f32)` — ordering
- `prop_tuple_i32_round_trip` / `prop_tuple_i64_round_trip` —
  tuple-input/tuple-output round-trip
- `prop_packed_int_round_trip` / `prop_packed_long_round_trip` —
  packed-int round-trip

Plus ~70 unit tests in `primitive_bindings.rs` and `sort_key.rs`
covering specific boundary values for each primitive.

### Invariant overlap

| Invariant family | JE | Noxu | Verdict |
|---|---|---|---|
| Round-trip of every primitive | (implicit; not exhaustive) | 13 proptest blocks + 70 unit tests | **Noxu stronger** |
| Sorted-int byte order | `testInt`, `testShort`, `testLong`, `testByte` | `prop_sorted_int_encoding_order` over **all i32 pairs** | **Noxu stronger** (proptest covers full input space) |
| Sorted-float byte order | `testSortedFloat`, `testSortedDouble` | `prop_sorted_float_encoding_order`, `prop_sorted_double_encoding_order` over all valid pairs | **Noxu stronger** |
| Packed-int byte order | `testPackedIntAndLong`, `testSortedPackedInt`, `testSortedPackedLong` | `prop_packed_int_round_trip`, `prop_packed_long_round_trip` (round-trip only, NO ordering proptest) | **GAP — MEDIUM** |
| String byte order | `testString`, `testFixedString` | (none — only round-trip) | **GAP — MEDIUM** |
| Char order | `testChars` | (covered by unit tests) | EQUIVALENT |
| Boolean order | `testBoolean` | unit test | EQUIVALENT |
| Unsigned byte / short / int order | `testUnsigned*` | unit tests | EQUIVALENT |
| Bytes (`writeBytes`) order | `testBytes` | unit tests | EQUIVALENT |
| BigInteger / SortedBigDecimal order | `testBigInteger`, `testSortedBigDecimal` | (none — bindings absent) | **GAP — LOW** (bindings deliberately omitted) |
| Embedded-null in String | not tested by JE | excluded from proptest range `"[^\x00]*"` | **GAP — LOW** (Noxu's encoding handles it; the test just doesn't exercise it) |

### Spotcheck 4 verdict

For the primitives Noxu **does** support, the tests are **stronger
than JE** because they use property-based testing over the full input
space rather than hand-curated boundary arrays. The gaps are:

1. No ordering proptest on packed-int encoding (round-trip only).
2. No ordering proptest on String encoding.
3. No BigInteger/BigDecimal bindings at all.

**Severity: MEDIUM** for items 1 and 2 (a round-trip test does not
prove the byte order matches the natural order of the type). The
existing unit tests on primitive_bindings probably do cover this for
specific values, but the property-based ordering invariant is the
strongest claim and is not asserted for these two cases.

---

## Spotcheck 5 — CollectionTest

| | JE | Noxu |
|---|---|---|
| File | `je/test/com/sleepycat/collections/test/CollectionTest.java` | `crates/noxu-collections/tests/collection_tests.rs` |
| Test method count | 1 parameterized `runTest` | 68 `#[test]` |
| Lines | ~3500 | ~1100 |

### What JE asserts

`CollectionTest` is **a single parameterized test class run with
~30 different parameter combinations** (StoredMap × StoredSortedMap ×
EntityBinding × byte-key × record-number-key × keyAssigner-or-not).
The `runTest()` method calls `testUnindexed()` / `testIndexed()`
which in turn call ~30 helper methods covering:

- Map creation with every binding combination
- `addAll` / `readAll` round-trip
- `iter` / `keys` / `values` / `entrySet`
- `subMap` / `headMap` / `tailMap` (sorted variants)
- `firstKey` / `lastKey`
- `containsKey` / `containsValue`
- `equals` / `hashCode` / `toString`
- `clear`
- `remove` / `replace` / `putIfAbsent`
- StoredList: `add` / `add(int)` / `set(int)` / `remove(int)` /
  `indexOf` / `lastIndexOf`
- Cursor reposition after concurrent put / delete
- StoredEntrySet, StoredKeySet, StoredValueSet bidirectional
  consistency

### What Noxu asserts

68 `#[test]` covering:

- StoredMap put/get/remove round-trip
- StoredMap put-overwrite returns old value
- StoredMap contains_key / len / clear / read-only / iter sorted
  order / values sorted / iter empty / iter after partial remove
- StoredSortedMap first_key / last_key / iter_from / sub_range /
  head_range / iter_reverse / first_entry / last_entry
- StoredList push / get / size / remove by index / pop / index sort
  order / iteration order / add_all / remove_all
- StoredKeySet / StoredValueSet basic ops
- TransactionRunner basic
- Plus 20 misc

### Invariant overlap

| Invariant family | JE | Noxu | Verdict |
|---|---|---|---|
| Map put/get/remove | yes | yes | EQUIVALENT |
| Map iter / keys / values | yes | yes | EQUIVALENT |
| SortedMap firstKey / lastKey | yes | yes | EQUIVALENT |
| SortedMap subMap / headMap / tailMap | yes | range / head / sub_range partial | **GAP — MEDIUM** (Java's NavigableMap has more methods than Noxu's iter_from) |
| Map equals / hashCode | yes | (Rust does not define `PartialEq` on collections views) | DELIBERATELY-OMITTED |
| Map putIfAbsent / replace / compute | yes | (none) | **GAP — MEDIUM** |
| Map putAll | yes | (none) | GAP — LOW |
| StoredList add(int)/set(int)/remove(value)/indexOf/lastIndexOf | yes | only push/pop/get/remove(index) | **GAP — MEDIUM** |
| KeySet/ValueSet bidirectional consistency | yes | partial | GAP — LOW |
| Cursor reposition under concurrent mutation | implicit | (none — `IterDeadlockTest` and `IterRepositionTest` from JE not ported) | **GAP — HIGH** |
| EntityBinding parameter | yes | (no built-in EntityBinding usage in collection_tests) | GAP — LOW (covered in noxu-bind tests) |

### Spotcheck 5 verdict

Noxu's collection tests are **broader than JE on basic operations**
(68 small focused tests vs JE's one test class with many helpers)
but **narrower on Java NavigableMap surface and concurrent-iterator
behaviour**. The most important gap is iterator-vs-mutation
consistency under concurrency — the JE tests `IterDeadlockTest`
and `IterRepositionTest` have no Noxu counterpart.

**Severity: MEDIUM**. The iterator-reposition gap is the most
concerning because Rust iterators are typically built around
single-threaded ownership, so concurrent mutation of the underlying
database while a `StoredIterator` exists may not trigger the safety
properties the JE tests guard. `noxu-collections::stored_map.rs`
maintains a `BTreeSet<Vec<u8>>` of "known keys" for iteration; the
behaviour when keys are concurrently added/removed at the database
level is not exhaustively tested.

---

## Spotcheck 6 — ElectionsTest

| | JE | Noxu |
|---|---|---|
| File | `je/test/com/sleepycat/je/rep/elections/ElectionsTest.java` | `crates/noxu-rep/tests/cluster_integration_test.rs` + `chaos_test.rs` + `quorum_policy_test.rs` |
| Test count | 7 `@Test` | 10 + 15 + 7 = 32 |

### What JE asserts

7 tests, each starts a 3-node group with an Acceptor / Proposer
/ Learner per node:

- `testBasicZeroPrio` — election succeeds with all priority-0 nodes
- `testBasicAllNodes` — election succeeds with all 3 nodes;
  StateChangeListener fires correctly
- `testBasicAllPrioNodes` — election uses priority as tiebreaker
- `testBasicAllButOneNode` — election succeeds with 2/3 nodes
- `testBasicOneNodeCrash` — election succeeds, then re-runs after
  one node crashes
- `testQuorumPolicyAll` — `QuorumPolicy.ALL` requires all 3 to
  vote
- `testNoQuorum` — election with 1/3 nodes fails (quorum not met)

### What Noxu asserts (cluster_integration_test.rs + chaos_test.rs + quorum_policy_test.rs)

`cluster_integration_test.rs` (10 tests):

- `test_election_over_tcp_channels` — Paxos over real TCP, 3-node
  group, highest-VLSN proposer wins
- `test_election_tcp_higher_vlsn_peer_wins` — best-proposal
  selection when the high-VLSN candidate is the acceptor
- `test_replica_applies_1000_entries` — apply_entry at scale
- `test_env_home_registers_restore_service` — restore service
  registration
- `test_three_node_failover` — master crash → replica → Unknown →
  new master
- `test_partition_and_catch_up` — replica falls behind, catches up
- `test_state_change_listener_fires_on_transitions` — listener
  invocation
- `test_fpaxos_5node_election_phase2_2` — Flexible Paxos with
  phase1=4, phase2=2 on 5-node group
- `test_dynamic_peer_add_remove` — add/remove peers at runtime
- `test_update_peer_metadata_while_active` — peer metadata update

`chaos_test.rs` (15 tests):

- `test_no_split_brain_concurrent_elections` — concurrent
  elections do not produce two masters
- `test_election_tolerates_message_drops` — randomly-dropped
  messages do not produce a split master
- `test_vlsn_monotone_under_message_drops` — VLSNs only increase
- `test_partition_minority_cannot_elect` — minority partition
  fails to elect
- `test_quorum_unreachable_election_fails_gracefully` — graceful
  failure
- `test_multi_round_elections_monotone_terms` — terms are
  monotonic
- `test_commit_durability_ack_requirements_all_policies` —
  every ReplicaAckPolicy
- `test_partition_and_recovery_vlsn_delivery` — VLSNs recovered
  after partition heals
- `test_highest_vlsn_wins_election` — VLSN is the primary
  tiebreaker
- `test_channel_close_mid_election_no_panic` — channel close
  during election does not panic
- `test_duplicate_messages_vlsn_nondecreasing` — duplicate
  messages do not decrease VLSN
- `test_partition_matrix_isolation_and_reconnection` —
  partition + reconnect matrix
- `test_large_scale_random_chaos` — large random chaos run
- `test_feeder_runner_ack_tracking_under_drops` — ack tracking
  under drops
- `test_commit_durability_ack_requirements_all_policies` (above)

`quorum_policy_test.rs` (7 tests):

- `test_simple_majority_3_5_7` — majority for 3, 5, 7-node groups
- `test_flexible_5node_phase1_4_phase2_2` — Flexible Paxos
- `test_flexible_invalid_rejected` — invalid Flexible config
- `test_flexible_classic_majority_is_valid` — classic majority
  via Flexible
- `test_quoracle_choose_expression` — quoracle expressions
- `test_quoracle_choose_invalid_rejected` — invalid expressions
- `test_build_majority_expression` — building expressions

Plus `noxu-spec::flexible_paxos` Stateright model.

### Invariant overlap

| Invariant family | JE | Noxu | Verdict |
|---|---|---|---|
| Election with all nodes up | `testBasicAllNodes`, `testBasicZeroPrio` | `test_election_over_tcp_channels`, `chaos_test::test_highest_vlsn_wins_election` | EQUIVALENT |
| Election with N-1 nodes | `testBasicAllButOneNode` | `chaos_test::test_partition_minority_cannot_elect` (inverse) | PARTIAL |
| Re-election after master crash | `testBasicOneNodeCrash` | `cluster_integration_test::test_three_node_failover` | EQUIVALENT |
| QuorumPolicy.ALL | `testQuorumPolicyAll` | `quorum_policy_test::test_simple_majority_*` (covers majority but not all-required) | **GAP — LOW** |
| Insufficient quorum | `testNoQuorum` | `chaos_test::test_quorum_unreachable_election_fails_gracefully` | EQUIVALENT |
| Priority-as-tiebreaker | `testBasicAllPrioNodes` | (none — Noxu uses VLSN as primary tiebreaker, no priority) | **GAP — MEDIUM** |
| StateChangeListener fires | yes (listenerNotifications counter) | `test_state_change_listener_fires_on_transitions` | EQUIVALENT |
| **Concurrent elections / split-brain prevention** | (implicit) | `test_no_split_brain_concurrent_elections` | **Noxu stronger** |
| **Random chaos / message drops** | (none) | `chaos_test.rs` (15 tests) | **Noxu stronger** |
| **Flexible Paxos** | (not in JE) | 7 dedicated tests + Stateright spec | NEW (Noxu only) |

### Spotcheck 6 verdict

Noxu's election tests are **substantially stronger than JE's** in
breadth (10 + 15 + 7 = 32 tests vs JE's 7) and in adversarial
coverage (chaos suite, partition matrix, large-scale random). The
gap is **node-priority as election tiebreaker** — JE's
`testBasicAllPrioNodes` exercises a behaviour that doesn't exist in
Noxu (Noxu uses VLSN as the only primary tiebreaker; node
priority is part of the JE design but is not implemented in
Noxu's `Proposal::is_better_than`).

**Severity: MEDIUM**. Whether priority-as-tiebreaker should be
implemented is a design decision; the absence is currently
undocumented in `omitted-features.md`. The Stateright model
`flexible_paxos` proves the safety property without requiring
priority, so the safety claim still holds.

---

## Cross-cutting observations

### Where Noxu is stronger than JE

1. **Property-based testing**: `noxu-bind`, `noxu-util`,
   `noxu-collections`, `noxu-config`, `noxu-dbi`, `noxu-latch`,
   `noxu-log`, `noxu-rep`, `noxu-tree`, `noxu-txn` all have
   `prop_tests.rs` files (29 `proptest!` blocks total). JE has
   no `proptest`-equivalent.
2. **Stateright models**: `noxu-spec` has 11 executable
   specifications proving abstract correctness of the protocols
   (B+tree latching, Flexible Paxos, WAL group-commit, recovery,
   lock manager + deadlock, VLSN streaming, master transfer,
   network restore, XA 2PC, cleaner safety, cache↔cleaner
   ordering). JE has no equivalent.
3. **Chaos / torture tests**: `noxu-rep::chaos_test.rs` and
   `noxu-rep::torture_test.rs` exercise adversarial scenarios
   (random drops, partition matrices, large-scale random) that JE
   does not.
4. **Concurrency depth**: `noxu-db::isolation_test.rs` runs
   tests at 32, 64, 200 threads; JE's equivalent tops out at
   ~10 threads.

### Where JE is stronger than Noxu

1. **Phantom-prevention scenarios**: 16 dedicated tests in
   `CursorTest` cover phantom insert/delete during get next/prev
   under commit/abort, with cursors positioned at BIN edges. Noxu
   has zero phantom-specific cursor tests.
2. **Regression bug coverage**: 16+ `SR*` named tests guard
   specific historical bugs. None are ported.
3. **Schema evolution**: 16 DPL evolution tests
   (`EvolveTest` family). Noxu has the data structures but no
   open-path tests because the open-path is not wired.
4. **Recovery edge cases**: 22 recovery tests + 8 stepwise
   crash-injection tests. Noxu has `crash_recovery_test.rs` (6
   tests) plus `noxu-spec::recovery_three_phase`.
5. **Cleaner-on-real-env scenarios**: 17 + 20 tests covering
   cleaner-during-checkpoint, cleaner-during-eviction,
   multi-cleaner, IN migration. Noxu has unit-level coverage but
   no equivalent integration tests.
6. **Java NavigableMap collection surface**: subMap / headMap /
   tailMap / putIfAbsent / replace / compute. Noxu has a thinner
   surface.

### Test-quality verdict for question 3 ("are the ported APIs correct?")

For the spot-checked classes:

- **Cursor lifecycle, search, traversal**: the Noxu tests
  meaningfully exercise the same invariants and a few more. **Likely
  correct** modulo the phantom gap.
- **Transactions and locking**: Noxu tests cover the same
  invariant families more thoroughly than JE on the data path.
  **Likely correct** modulo importunate / pre-emption.
- **Cleaner data structures**: well-tested at the unit level.
  **Likely correct** at the data-structure level. Integration
  behaviour under sustained load is **less confident**.
- **Tuple bindings**: property-based tests on byte ordering for
  primitive integer / float / double types are a stronger guarantee
  than JE's hand-curated test arrays. **Likely correct** for
  primitives; packed-int and String byte ordering not asserted.
- **Collections views**: basic operations covered; concurrent
  iterator behaviour and `NavigableMap` surface are **gaps**.
- **Replication elections**: Stronger than JE on most invariants
  (chaos coverage, Flexible Paxos, Stateright spec). Priority gap
  is a **design choice** more than a correctness gap.

Overall: where Noxu has tests, those tests address the JE invariants
and, in many cases, more. Where Noxu does NOT have tests, no claim
can be made about correctness. The class-level test-map.md flagged 12
HIGH-severity test-coverage gaps; the 6-pair spotcheck found
~5 additional MEDIUM gaps within tested classes (phantoms,
importunate, IN migration, packed-int order, iterator-reposition,
priority-tiebreaker).
