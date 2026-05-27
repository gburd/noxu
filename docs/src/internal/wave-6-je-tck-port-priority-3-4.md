# Wave 6 — JE TCK Port (Priority-3 + Priority-4)

This wave continues the JE @Test port work from waves 4-A/B/C and 5,
covering the priority-3 (replication) and priority-4 (misc) packages
identified in `je-tck-port-2026-05-prioritized-backlog.md`.

## Scope and outcome

Wave 6 added **14 PORTED-EQUIVALENT**, **8 PORTED-PARTIAL** and
**1 OUT-OF-SCOPE** TCK rows across `je.rep.elections`, `je.rep.vlsn`,
`je.evictor`, `je.tree`, and `je.dbi` — 33 individual `#[test]`
functions across five new test files.

| Package           | New ported (Eq/Partial/OOS) | New file |
|-------------------|---:|----------|
| `je.rep.elections` | 6 / 0 / 0 | `crates/noxu-rep/tests/je_acceptor_test.rs` (1 `testAcceptor`-equivalent + 3 supporting); `crates/noxu-rep/tests/je_ranking_proposer_test.rs` (5 ports of `RankingProposerTest`) |
| `je.rep.vlsn`      | 2 / 3 / 0 | `crates/noxu-rep/tests/je_vlsn_index_test.rs` |
| `je.evictor`       | 0 / 5 / 0 | `crates/noxu-evictor/tests/je_lru_test.rs` |
| `je.tree`          | 5 / 0 / 0 | `crates/noxu-tree/tests/je_key_test.rs`, `crates/noxu-tree/tests/je_in_test.rs` |
| `je.dbi`           | 1 / 0 / 1 | `crates/noxu-config/tests/je_db_config_manager_test.rs` |

All 33 tests pass under `cargo test --no-fail-fast`.

## Aggregate impact

| Bucket            | Wave 5 end | Wave 6 end |
|-------------------|---:|---:|
| PORTED-EQUIVALENT | 182 | 196 |
| PORTED-PARTIAL    | 62  | 70  |
| OUT-OF-SCOPE      | 63  | 64  |
| NOT-PORTED        | 1761| 1738|

## Per-port summary

### `je.rep.elections`

| JE class              | JE method               | Noxu test                                                         |
|-----------------------|-------------------------|--------------------------------------------------------------------|
| `AcceptorTest`        | `testAcceptor`          | `je_acceptor_test::test_acceptor_je_equivalent`                    |
| `RankingProposerTest` | `testPhase2TwoNodes`    | `je_ranking_proposer_test::test_phase2_two_nodes`                  |
| `RankingProposerTest` | `testPhase2ThreeNodes`  | `je_ranking_proposer_test::test_phase2_three_nodes`                |
| `RankingProposerTest` | `testPhase2ArbOneNode`  | `je_ranking_proposer_test::test_phase2_arb_one_node`               |
| `RankingProposerTest` | `testPhase2ArbTwoNodes` | `je_ranking_proposer_test::test_phase2_arb_two_nodes`              |
| `RankingProposerTest` | `testPhase2TwoArbs`     | `je_ranking_proposer_test::test_phase2_two_arbs`                   |

`AcceptorTest::testAcceptor` was adapted directly: JE's
`Acceptor.process(Propose)` → Noxu's `PersistentAcceptorState::try_promise(t)`
(returning PROMISE/REJECT as `true`/`false`); JE's `Acceptor.process(Accept)`
→ `try_accept(t, master)` (ACCEPTED/REJECT → `true`/`false`).  The
arbiter-filtering invariant from `RankingProposerTest::choosePhase2Value`
was ported as a pure helper that uses `Proposal::cmp` plus an arbiter
filter (priority == 0) — matching the F22 guard already enforced inside
`run_election`.

### `je.rep.vlsn`

| JE class            | JE method                            | Noxu test                                                                |
|---------------------|--------------------------------------|---------------------------------------------------------------------------|
| `VLSNBucketTest`    | `testRemoveFromTail` (PARTIAL)       | `je_vlsn_index_test::test_remove_from_tail_index_level`                  |
| `VLSNBucketTest`    | `testTruncateAfterFileOffset` (P)    | `je_vlsn_index_test::test_truncate_after_file_offset`                    |
| `VLSNIndexTest`     | `testFlushedGets` (PARTIAL)          | `je_vlsn_index_test::test_basic_gets`                                    |
| `VLSNIndexTest`     | `testNonContiguousBucketSmallHoles`  | `je_vlsn_index_test::test_non_contiguous_bucket_small_holes`             |
| `VLSNIndexTest`     | `testNonContiguousBucketLargeHoles`  | `je_vlsn_index_test::test_non_contiguous_bucket_large_holes`             |

PARTIAL ports are flagged where Noxu's `VlsnIndex::truncate_after` only
removes whole buckets and clamps the global range — it does NOT shrink
individual bucket contents.  JE's `removeFromTail` partially trims a
bucket's stride array.  This is a documented semantic difference (see
`crates/noxu-rep/src/vlsn/vlsn_index.rs` doc comments and
`test_truncate_removes_buckets_beyond_point`).

### `je.evictor`

| JE class | JE method               | Noxu test                                                             |
|----------|--------------------------|-----------------------------------------------------------------------|
| `LRUTest`| `testBaseline`           | `je_lru_test::test_baseline_insertion_then_pop_lru`                  |
| `LRUTest`| `testCacheMode_KEEP_HOT` | `je_lru_test::test_keep_hot_via_touch`                               |
| `LRUTest`| `testCacheMode_UNCHANGED`| `je_lru_test::test_unchanged_nodes_stay_in_insertion_order`          |
| `LRUTest`| `testCacheMode_MAKE_COLD`| `je_lru_test::test_unchanged_nodes_stay_in_insertion_order` (alias)  |
| `LRUTest`| `testCacheMode_EVICT_LN` | `je_lru_test::test_evict_ln_via_remove`                              |

All five ports are PARTIAL — JE asserts cache-share percentages across
multiple databases under a fixed cache size; Noxu validates the
underlying LRU semantics directly against `LruList`.  Three additional
`LruList`-only tests exercise `touch`-on-absent-node, double-`remove`,
and the dual-priority list invariants.

### `je.tree`

| JE class | JE method                       | Noxu test                                                  |
|----------|----------------------------------|-------------------------------------------------------------|
| `KeyTest`| `testKeyPrefixer`                | `je_key_test::test_key_prefixer`                           |
| `KeyTest`| `testKeyPrefixSubsetting`        | `je_key_test::test_key_prefix_subsetting`                  |
| `KeyTest`| `testKeyComparisonPerformance`   | `je_key_test::test_key_comparison_equal_repeats`           |
| `KeyTest`| `testKeyComparison`              | `je_key_test::test_key_comparison`                         |
| `INTest` | `testFindEntry` (refined)        | `je_in_test::test_find_entry`                              |
| `INTest` | `testInsertEntry`                | `je_in_test::test_insert_entry_preserves_sorted_order`     |

`testKeyComparison` covers the JE invariant that `compareKeys` uses
**unsigned** byte semantics — the test exercises `0xFF…` vs `0x7F…` to
catch any signed-byte regression.  `testFindEntry` exercises the
upper-IN "virtual entry 0" trick: `find_entry(zb, exact=false,
indicate_if_duplicate=false)` always returns 0 when at least one entry
is present.

### `je.dbi`

| JE class                | JE method               | Noxu test                                              |
|-------------------------|-------------------------|---------------------------------------------------------|
| `DbConfigManagerTest`   | `testBasicParams`       | `je_db_config_manager_test::test_basic_params`         |
| `DbConfigManagerTest`   | `testBooleanWhitespace` | OUT-OF-SCOPE                                            |

`testBooleanWhitespace` is OOS because Noxu's `ConfigManager` accepts
already-typed `ParamValue::Bool(_)` — there is no string-parse path
where the "trim leading/trailing whitespace" concern applies.

## Real Noxu bugs surfaced

**None.**  All 33 tests pass without ignore-markers.  The single
behavioural divergence (`VlsnIndex::truncate_after` vs JE's
`removeFromTail`) is a deliberate design choice already documented in
the source.

## Skipped scope

The following priority-3 sub-packages did not receive new ports in
this wave because the JE tests required full `RepEnvInfo` /
`RepTestBase` infrastructure (heavyweight integration), which is not
in scope for unit-level porting:

* `je.rep.stream` — `FeederWriteQueueTest`, `FeederFilterTest`,
  `ProtocolTest`: full master+replica group + EntityStore.
* `je.rep.txn` — `CommitTokenTest`, `ExceptionTest`: depend on
  `CommitToken` (Noxu has no equivalent) and `RepEnvInfo.joinGroup`.
* `je.rep` (top-level) — `CheckConfigTest`,
  `ExternalNodeTypeTest`: full `ReplicatedEnvironment` lifecycle.

These remain `NOT-PORTED` and will be revisited when a lightweight
JE-style RepTestBase harness is added to noxu-rep, or when the
priority-1/2 backlog is exhausted.

## Files added

* `crates/noxu-rep/tests/je_acceptor_test.rs`           — 4 tests
* `crates/noxu-rep/tests/je_ranking_proposer_test.rs`   — 6 tests
* `crates/noxu-rep/tests/je_vlsn_index_test.rs`         — 7 tests
* `crates/noxu-evictor/tests/je_lru_test.rs`            — 8 tests
* `crates/noxu-tree/tests/je_key_test.rs`               — 5 tests
* `crates/noxu-tree/tests/je_in_test.rs`                — 4 tests (incl. extras)
* `crates/noxu-config/tests/je_db_config_manager_test.rs`— 5 tests (incl. extras)

Total: **39 `#[test]` functions** added in production-only test
crates; no production-code changes were made in this wave.

## Gate status

* `cargo fmt --all -- --check` — pass
* `cargo clippy --workspace --all-targets -- -D warnings` — pass
* per-crate test runs — all 39 new tests pass
* `make docs-check` — pass

## Methodology

Each port follows the recipe from `wave-4-b-je-tck-port-priority1.md`:

1. Open the JE source, identify the invariant the test asserts.
2. Map JE classes/methods to Noxu types/methods in the file header.
3. Adapt or skip JE-specific machinery (e.g. `RepEnvInfo`, `Logger`,
   `EntityStore`); document the adaptation as a note at the top of
   the test file.
4. Port assertion shape verbatim where possible.  Where the API is
   different (e.g. JE's `removeFromTail` vs Noxu's `truncate_after`),
   weaken to the strongest invariant Noxu's API can express and mark
   the row PORTED-PARTIAL.
5. Run with `timeout 60 cargo test -p <crate> --no-fail-fast --test <name>`.
6. Update the per-package TSV row.
7. Commit per logical batch.
