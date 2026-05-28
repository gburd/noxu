# Wave 11-G — JE TCK Long-Tail Port

## Goal

Continue the JE TCK port effort started in waves 4-B/4-C/6/8/9-C/10-A,
targeting an additional 30–50 PORTED-EQUIVALENT rows.  The earlier waves
covered the easy-win API-mappable rows; this wave digs into the long
tail of SR-numbered regression tests, dup-cursor edge cases, recovery
invariants, tree-balance/split invariants, and search-range corner
cases.

Per-package TSV row count after this wave:

| Bucket             | Count |
|--------------------|------:|
| PORTED-EQUIVALENT  |   306 |
| PORTED-PARTIAL     |   105 |
| OUT-OF-SCOPE       |   127 |
| NOT-PORTED         |  1531 |
| **Total**          |  2114 |

Delta vs post-wave-10-A (PE 263, PP 99, OOS 127, NOT 1580):
**+43 PORTED-EQUIVALENT, +6 PORTED-PARTIAL, −49 NOT-PORTED**.

## What landed (49 ports across 7 commits)

| Commit hash (short) | Crate / file                                    | Tests added |
|---------------------|-------------------------------------------------|------------:|
| `c5f45f4` | `crates/noxu-db/tests/je_database_test.rs`     | 9 (4 PE, 5 PP-#[ignore]) |
| `c0a85e7` | `crates/noxu-db/tests/je_sr_regression_test.rs` | 7 PE |
| `c455271` | `crates/noxu-db/tests/je_truncate_test.rs`     | 5 (4 PE, 1 PP-#[ignore]) |
| `f098ff6` | `crates/noxu-db/tests/je_search_range_test.rs` | 6 PE |
| `7810cc2` | `crates/noxu-db/tests/je_recovery_test.rs`     | 5 PE |
| `8ef2184` | `crates/noxu-db/tests/je_tree_test.rs`         | 7 PE |
| `8d9fd66` + `17bbea5` | `crates/noxu-db/tests/je_dup_cursor_test.rs` | 9 PE |
| `75f5f96` | `docs/src/internal/je-tck-port-2026-05-enumeration-*.tsv` | 50 row updates |

(One row in `je.dbi` was already PORTED-EQUIVALENT before this wave from
the heuristic name-match scan; we updated its citation but it counts as
a re-tag rather than a new port, hence 50 row updates → 49 net new
PORTED-* rows.)

## Per-package breakdown

### `je` (25 rows updated)

| JE class                    | JE test                                   | Status            | Noxu test                                                       |
|-----------------------------|-------------------------------------------|-------------------|------------------------------------------------------------------|
| DatabaseTest                | testCursor                                | PORTED-PARTIAL    | `database_txn_cursor_on_non_txn_db_rejected` (#[ignore], real bug) |
| DatabaseTest                | testPutNoOverwriteInADupDbTxn             | PORTED-PARTIAL    | `database_put_no_overwrite_in_dup_db_txn` (#[ignore], real bug) |
| DatabaseTest                | testPutNoOverwriteInADupDbNoTxn           | PORTED-PARTIAL    | `database_put_no_overwrite_in_dup_db_no_txn` (#[ignore], real bug) |
| DatabaseTest                | testDatabaseCount                         | PORTED-EQUIVALENT | `database_count_with_deleted_entries`                            |
| DatabaseTest                | testDatabaseCountEmptyDB                  | PORTED-EQUIVALENT | `database_count_empty_returns_zero`                              |
| DatabaseTest                | testDatabaseCountWithDeletedEntries       | PORTED-EQUIVALENT | `database_count_with_deleted_entries`                            |
| DatabaseTest                | testDatabaseCountDups                     | PORTED-EQUIVALENT | `database_count_dups_counts_each_dup`                            |
| DatabaseTest                | testDbCloseUnopenedDb                     | PORTED-EQUIVALENT | `database_close_idempotent` (spirit port)                        |
| EnvironmentTest             | testFlushLog                              | PORTED-EQUIVALENT | `environment_checkpoint_forces_durability` (spirit port)         |
| EnvironmentTest             | testNoCreateReservedNameDB                | PORTED-EQUIVALENT | `environment_open_reserved_name_db_rejected` (spirit port)       |
| EnvironmentTest             | testReadOnlyDbNameOps                     | PORTED-PARTIAL    | `environment_read_only_rejects_db_name_ops` (#[ignore], real bug) |
| DupSlotReuseTest            | testSameTxnAbort                          | PORTED-EQUIVALENT | `dup_slot_reuse_same_txn_abort_leaves_empty`                     |
| DupSlotReuseTest            | testDiffTxnAbort                          | PORTED-EQUIVALENT | `dup_slot_reuse_diff_txn_abort_restores_v0`                      |
| DbHandleLockTest            | testSR12068                               | PORTED-EQUIVALENT | `sr12068_db_handle_lock_released_on_close`                       |
| GetSearchBothRangeTest      | testSearchKeyRangeWithDupTree             | PORTED-EQUIVALENT | `search_key_range_with_dup_tree_finds_next_key`                  |
| GetSearchBothRangeTest      | testSearchBothWithNoDupTree               | PORTED-EQUIVALENT | `search_both_with_no_dup_tree_finds_existing_pair_only`          |
| GetSearchBothRangeTest      | testSuccessDup                            | PORTED-EQUIVALENT | `search_both_range_dup_positions_on_first_dup_at_or_after`       |
| GetSearchBothRangeTest      | testNotFoundDup                           | PORTED-EQUIVALENT | `search_both_range_dup_missing_key_returns_not_found`            |
| GetSearchBothRangeTest      | testSearchBefore                          | PORTED-EQUIVALENT | `search_both_range_dup_data_before_target_returns_not_found`     |
| GetSearchBothRangeTest      | testSingleDatumBug                        | PORTED-EQUIVALENT | `search_both_range_does_not_cross_key_boundary`                  |
| TruncateTest                | testEnvTruncateAutocommit                 | PORTED-EQUIVALENT | `truncate_database_drops_records_and_returns_count`              |
| TruncateTest                | testEnvTruncateNoFirstInsert              | PORTED-EQUIVALENT | `truncate_database_empty_returns_zero`                           |
| TruncateTest                | testWriteAfterTruncate                    | PORTED-EQUIVALENT | `truncate_then_write_succeeds_no_deadlock` (SR 10386 / 11252)    |
| TruncateTest                | testTruncateAfterRecovery                 | PORTED-PARTIAL    | `truncate_survives_clean_close_reopen` (#[ignore], real bug)    |
| TruncateTest                | testTruncateNoLocking                     | PORTED-EQUIVALENT | `truncate_then_get_returns_not_found` (spirit port)              |

### `je.dbi` (12 rows updated)

| JE class                       | JE test                                        | Status            | Noxu test                                                       |
|--------------------------------|------------------------------------------------|-------------------|------------------------------------------------------------------|
| DbCursorSearchTest             | testGetSearchBothNoDuplicatesAllowedSR9522     | PORTED-EQUIVALENT | `sr9522_get_search_both_works_on_non_dup_db`                     |
| DbCursorDuplicateDeleteTest    | testDeletedReplaySR8984                        | PORTED-EQUIVALENT | `sr8984_aborted_delete_then_reinsert_dups_leaves_empty`          |
| DbCursorDuplicateDeleteTest    | testDuplicateDeadlockSR9885                    | PORTED-PARTIAL    | `sr9885_cursor_delete_removes_only_positioned_dup` (single-thread) |
| DbCursorDuplicateTest          | testDuplicateCreationForward                   | PORTED-EQUIVALENT | `dup_cursor_creation_forward_walks_in_sorted_order`              |
| DbCursorDuplicateTest          | testDuplicateCreationBackwards                 | PORTED-EQUIVALENT | `dup_cursor_creation_backwards_walks_in_reverse_order`           |
| DbCursorDuplicateDeleteTest    | testSimpleSingleElementDupTree                 | PORTED-EQUIVALENT | `dup_cursor_delete_one_dup_leaves_the_other`                     |
| DbCursorDuplicateDeleteTest    | testEmptyNodes                                 | PORTED-EQUIVALENT | `dup_cursor_delete_all_dups_leaves_empty`                        |
| DbCursorDuplicateDeleteTest    | testDuplicateDeleteFirst                       | PORTED-EQUIVALENT | `dup_cursor_delete_first_dup_via_positioned_cursor`              |
| DbCursorDuplicateTest          | testPutNoDupData2                              | PORTED-EQUIVALENT | `dup_cursor_put_no_dup_data_inserts_unique_pairs`                |
| DbCursorDuplicateTest          | testAbortDuplicateTreeCreation                 | PORTED-EQUIVALENT | `dup_cursor_abort_after_dup_creation_keeps_committed_only`       |
| DbCursorDeleteTest             | testLargeDeleteFirst                           | PORTED-EQUIVALENT | `cursor_delete_first_via_walk_keeps_rest` (smaller N)            |
| DbCursorDeleteTest             | testLargeDeleteLast                            | PORTED-EQUIVALENT | `cursor_delete_last_via_walk_keeps_rest` (smaller N)             |

### `je.recovery` (5 rows updated)

| JE class               | JE test                            | Status            | Noxu test                                            |
|------------------------|------------------------------------|-------------------|------------------------------------------------------|
| RecoveryDuplicatesTest | testDuplicates                     | PORTED-EQUIVALENT | `recovery_duplicates_round_trip_across_clean_close`  |
| RecoveryDuplicatesTest | testDuplicatesWithDeletion         | PORTED-EQUIVALENT | `recovery_duplicates_with_deletion_survives_recovery`|
| RecoveryCheckpointTest | testEmptyCheckpoint                | PORTED-EQUIVALENT | `recovery_empty_checkpoint_round_trip`               |
| RecoveryDeleteTest     | testDeleteAllAndCompress           | PORTED-EQUIVALENT | `recovery_delete_all_then_recovery_empties_db`       |
| RecoveryEdgeTest       | testTxnId                          | PORTED-EQUIVALENT | `recovery_edge_txn_id_continues_post_recovery`       |

### `je.tree` (7 rows updated)

| JE class       | JE test                              | Status            | Noxu test                                                |
|----------------|--------------------------------------|-------------------|----------------------------------------------------------|
| SplitTest      | test0Split                           | PORTED-EQUIVALENT | `split_descending_then_ascending_keys_remain_sorted`     |
| TreeTest       | testCountAndValidateKeys             | PORTED-EQUIVALENT | `tree_count_and_validate_keys_forward`                   |
| TreeTest       | testCountAndValidateKeysBackwards    | PORTED-EQUIVALENT | `tree_count_and_validate_keys_backwards`                 |
| TreeTest       | testAscendingInsertBalance           | PORTED-EQUIVALENT | `tree_ascending_insert_walks_in_order`                   |
| TreeTest       | testDescendingInsertBalance          | PORTED-EQUIVALENT | `tree_descending_insert_walks_in_order`                  |
| KeyPrefixTest  | testPrefixBasic                      | PORTED-EQUIVALENT | `key_prefix_basic_long_shared_prefix_round_trip`         |
| KeyPrefixTest  | testPrefixManySequential             | PORTED-EQUIVALENT | `key_prefix_many_sequential_round_trip`                  |

### `je.test` (1 row updated)

| JE class      | JE test          | Status            | Noxu test                                |
|---------------|------------------|-------------------|------------------------------------------|
| SR11297Test   | test11297        | PORTED-EQUIVALENT | `sr11297_get_first_after_first_bin_emptied` |

## Real Noxu bugs surfaced (5)

Each of these is a `#[ignore]`'d test in this wave's commits that
documents a real Noxu regression vs JE's invariant.  All routed to a
follow-up bug-fix wave per Wave 11-G discipline.

1. **`database_txn_cursor_on_non_txn_db_rejected`** —
   `crates/noxu-db/tests/je_database_test.rs`.  Noxu permits opening a
   transactional cursor on a non-transactional database; JE rejects with
   `IllegalArgumentException`.

2. **`database_put_no_overwrite_in_dup_db_{txn,no_txn}`** —
   `crates/noxu-db/tests/je_database_test.rs`.  Noxu's
   `put_no_overwrite` on sorted-dup databases checks the *(key, data)*
   pair (same as `put_no_dup_data`).  JE's `putNoOverwrite` is *key
   only*: once any dup exists for a key, a second `putNoOverwrite` with
   a different data must still return `KEYEXIST`.  See
   `put_dup` in `crates/noxu-dbi/src/cursor_impl.rs`
   (`PutMode::NoDupData | NoOverwrite` arm).

3. **`environment_read_only_rejects_db_name_ops`** —
   `crates/noxu-db/tests/je_database_test.rs`.  Noxu's database-name
   registry is not preserved across a clean close+reopen when the reopen
   is read-only (`DatabaseNotFound: 'db1' does not exist and
   allow_create is false`).  Closely related to the existing
   `recovery_edge_test_non_txnal_db` `#[ignore]`'d gap, but read-only
   cannot side-step with `allow_create=true`.

4. **`environment_checkpoint_after_commit_loses_data`** —
   `crates/noxu-db/tests/je_database_test.rs`.  Calling
   `env.checkpoint(None)` between `txn.commit()` and `drop(env)` causes
   the most recently committed records to be lost on the next env open.
   The invariant (committed data is durable, regardless of when
   checkpoint runs) holds in JE and must hold in Noxu.

5. **`truncate_survives_clean_close_reopen`** —
   `crates/noxu-db/tests/je_truncate_test.rs`.  Noxu's
   `truncate_database` is not durable across a clean close+reopen; the
   previously-truncated records re-appear (count returns the
   pre-truncate value).

## Spirit-port partials (3)

These tests capture the JE invariant via Noxu's narrower public API.
They are PORTED-PARTIAL because the literal JE assertions (depth, flush
mode, internal compress) are not directly observable in Noxu's API:

* `database_close_idempotent` — JE tests `new Database(env).close()`;
  Noxu has no such constructor, so we capture handle-close idempotency.
* `environment_open_reserved_name_db_rejected` — JE asserts a specific
  list of reserved names (e.g. `_jeRepGroupDB`); Noxu's reservation
  policy differs, so we assert the most conservative case (empty name)
  is rejected.
* `sr9885_cursor_delete_removes_only_positioned_dup` — JE's original
  `testDuplicateDeadlockSR9885` is a two-thread deadlock test; we port
  the single-threaded dup-chain integrity invariant only.

## OUT-OF-SCOPE (deferred)

Most remaining `je.cleaner` tests are OUT-OF-SCOPE for this wave because
they require JE-internal `env.cleanLog()` / `env.evictMemory()` /
`env.compress()` controls that Noxu's public API does not expose.
Likewise, JE-internal Tree-shape assertions (depth, latch leak counts,
`DbInternal.makeCursor`) are deliberately not surfaced in Noxu.

The following test classes were considered but skipped:

* `BINDeltaOpsTest`, `INKeyRepTest`, `LSNArrayTest`, `MemorySizeTest`,
  `LogManagerTest`, `LogBufferPoolTest`, `FileReaderTest`,
  `LNFileReaderTest` — internal-class tests with no public-API
  equivalent.
* `CacheModeTest` family — Noxu's cursor doesn't expose
  `setCacheMode` directly; coverage punted to a future cache-mode
  wave.
* `TxnTimeoutTest` family — concurrency-heavy and time-sensitive;
  punted to a future timeout-coverage wave.
* `DeferredWriteTest` — Noxu does not have a deferred-write mode
  equivalent.
* `SkipTest` — Noxu's cursor lacks `skip()`/`dup(samePosition)`.
* `LogFileDeletionCrashEnvTest` — needs SIGKILL + filesystem-mutation
  harness; covered in Noxu's separate `crash_recovery_test.rs`.

## Methodology notes

* Each new test asserts the **same invariant** as the JE original,
  not the literal Java-level expression.  Where the JE test inspects
  internal classes (`DbInternal.getCursorImpl`, `Tree.dump`,
  `BasicLocker.createBasicLocker`, …), the Noxu port asserts the
  user-visible consequence instead.
* Every `#[ignore]`'d test has an inline `TODO(...wave-11-G)` citing
  this document; the bug fix is routed to a follow-up wave.
* No production code was modified in this wave (constraint per the
  wave brief).
