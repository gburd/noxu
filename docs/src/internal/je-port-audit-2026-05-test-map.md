# JE → Noxu Port-Completeness Audit — May 2026 — Test Map

> Companion to `je-port-audit-2026-05-overview.md`. Per-package
> mapping of JE test classes to Noxu test files.

**Status legend**:

- **EQUIVALENT** — JE test class has a Noxu counterpart that exercises
  the same invariant family (cursor, txn, isolation, recovery, etc.).
  May have more or fewer assertions than the JE original; class-level
  coverage only.
- **PARTIAL** — Noxu has a counterpart, but it covers a subset of the
  JE test's scenarios (e.g. only the basic case, not the SR-numbered
  regression).
- **MISSING** — JE has the test class; Noxu has no counterpart and
  the invariants the test guards are NOT covered by `noxu-spec` or
  by another Noxu test.
- **MAPPED-TO-SPEC** — Invariants covered by a `noxu-spec` Stateright
  model, not by a unit/integration test.
- **SKIPPED** — Test package is out of scope for the port (JNI, JMX,
  JCA, Java-serialization compatibility, bytecode-enhancer, dual-rep
  mirroring, log-version-conversion).

**Severity** (from the overview):

- HIGH = data-correctness invariant family without Noxu coverage
- MEDIUM = entire JE test class without Noxu equivalent
- LOW = JE test for an internal-impl edge case Noxu doesn't share
- INFO = deliberately omitted

---

## com.sleepycat.je (24 classes)

| JE test class | Noxu counterpart | Status | Severity |
|---|---|---|---|
| `ApiTest` | (none — JE-specific public-API exposure check) | MISSING | LOW |
| `ClassLoaderTest` | (none — Java classloader specific) | SKIPPED | INFO |
| `ConfigBeanInfoTest` | (none — JavaBeans specific) | SKIPPED | INFO |
| `CursorEdgeTest` | `noxu-db/tests/cursor_test.rs`, `crates/noxu-db/src/cursor.rs#[cfg(test)]` | PARTIAL | LOW |
| `CursorTest` | `noxu-db/tests/cursor_test.rs` (46 tests) | EQUIVALENT | — |
| `DatabaseComparatorsTest` | `noxu-db/src/byte_comparator.rs#[cfg(test)]` | PARTIAL | LOW |
| `DatabaseConfigTest` | `noxu-db/src/database_config.rs#[cfg(test)]` | PARTIAL | LOW |
| `DatabaseEntryTest` | `noxu-db/src/database_entry.rs#[cfg(test)]` | EQUIVALENT | — |
| `DatabaseTest` | `noxu-db/tests/integration_test.rs`, `noxu-db/tests/compat_tests.rs` | EQUIVALENT | — |
| `DbHandleLockTest` | `noxu-txn/src/handle_locker.rs#[cfg(test)]` | PARTIAL | LOW |
| `DbTestProxy` | (test-utility class, not a runnable test) | SKIPPED | INFO |
| `DirtyReadTest` | `noxu-db/tests/isolation_test.rs::test_dirty_read_prevented_under_all_isolation_levels` | EQUIVALENT | — |
| `DupSlotReuseTest` | `noxu-db/tests/sorted_dup_test.rs` | PARTIAL | LOW |
| `EnvironmentConfigTest` | `noxu-config/tests/prop_tests.rs` | PARTIAL | MEDIUM (parameter coverage gaps) |
| `EnvironmentStatTest` | `noxu-engine/src/env_stats.rs#[cfg(test)]` | PARTIAL | LOW |
| `EnvironmentTest` | `noxu-db/tests/integration_test.rs`, `noxu-db/tests/txn_wiring_test.rs`, `noxu-db/tests/compat_tests.rs` | PARTIAL | MEDIUM (db rename/remove/truncate edge cases sampled, not exhaustive) |
| `EnvMultiSubDirTest` | (none — multi-data-dir feature not ported) | MISSING | LOW |
| `GetSearchBothRangeTest` | (none — `getSearchBothRange` not exposed on Database) | MISSING | MEDIUM |
| `InterruptTest` | (none — Rust thread interruption model differs) | SKIPPED | INFO |
| `MultiProcessWriteTest` | `noxu-db/tests/crash_recovery_test.rs` (worker process pattern) | PARTIAL | LOW |
| `ReadCommittedTest` | `noxu-db/tests/isolation_test.rs::test_read_committed_*` | EQUIVALENT | — |
| `RunRecoveryFailureTest` | `noxu-db/tests/crash_recovery_test.rs` | PARTIAL | LOW |
| `StatCaptureTest` | (none — periodic stat capture daemon not ported) | MISSING | LOW |
| `TruncateTest` | `noxu-db/tests/compat_tests.rs::truncate_database_*` | EQUIVALENT | — |

## com.sleepycat.je.cleaner (23 classes)

| JE test class | Noxu counterpart | Status | Severity |
|---|---|---|---|
| `BackgroundIOTest` | `noxu-cleaner/src/throttle.rs#[cfg(test)]` | PARTIAL | LOW |
| `CleanerTest` | `noxu-cleaner/tests/cleaner_test.rs` (34 tests, FileSelector/FileSummary/Throttle level) | PARTIAL | MEDIUM (no full env-under-load tests) |
| `CleanerTestBase` / `CleanerTestUtils` | (test fixtures) | SKIPPED | INFO |
| `DiskLimitTest` | `noxu-db/tests/sustained_load_test.rs::test_cleaner_reduces_log_files_under_load` | PARTIAL | MEDIUM |
| `FileProtectorTest` | `noxu-cleaner/src/file_protector.rs#[cfg(test)]` | PARTIAL | LOW |
| `FileSelectionTest` (20 @Tests) | `noxu-cleaner/tests/cleaner_test.rs` (FileSelector portion) | PARTIAL | MEDIUM |
| `INUtilizationTest` | `noxu-cleaner/src/utilization_tracker.rs#[cfg(test)]`, `in_summary.rs#[cfg(test)]` | PARTIAL | LOW |
| `MakeMigrationLogFiles` | (utility, not a test) | SKIPPED | INFO |
| `OffsetTest` | `noxu-cleaner/src/packed_offsets.rs#[cfg(test)]` | EQUIVALENT | — |
| `ReadOnlyLockingTest` / `ReadOnlyProcess` | (none — read-only env mode not exhaustively tested) | MISSING | LOW |
| `RMWLockingTest` | (none) | MISSING | LOW |
| `SR10553Test` / `SR10597Test` / `SR12885Test` / `SR12978Test` / `SR13061Test` / `SR18567Test` | (none) | MISSING | MEDIUM (regression bugs not guarded) |
| `TruncateAndRemoveTest` | `noxu-db/tests/compat_tests.rs::truncate_*`, `remove_*` | PARTIAL | MEDIUM (cleaner-side processing not tested) |
| `TTLCleaningTest` | `noxu-util/src/ttl.rs#[cfg(test)]` (TTL only, no cleaner integration) | PARTIAL | MEDIUM |
| `UtilizationTest` | `noxu-cleaner/src/utilization_profile.rs#[cfg(test)]` | PARTIAL | LOW |
| `WakeupTest` | (none) | MISSING | LOW |
| **Spec coverage** | `noxu-spec::cleaner_safety`, `noxu-spec::cache_vs_cleaner` | MAPPED-TO-SPEC (partial) | — |

## com.sleepycat.je.config (1 class)

| JE test class | Noxu counterpart | Status | Severity |
|---|---|---|---|
| `EnvironmentParamsTest` | `noxu-config/tests/prop_tests.rs`, `noxu-config/src/param.rs#[cfg(test)]`, `params.rs#[cfg(test)]`, `manager.rs#[cfg(test)]` | EQUIVALENT | — |

## com.sleepycat.je.dbi (26 classes)

| JE test class | Noxu counterpart | Status | Severity |
|---|---|---|---|
| `BINDeltaOperationTest` / `BINDeltaOpsTest` | `noxu-tree/src/delta_info.rs#[cfg(test)]`, `noxu-tree/tests/bin_in_test.rs` | PARTIAL | MEDIUM |
| `CodeCoverageTest` | (Java code-coverage harness) | SKIPPED | INFO |
| `CompressedOopsDetectorTest` | (JVM-specific) | SKIPPED | INFO |
| `DbConfigManagerTest` | `noxu-config/src/manager.rs#[cfg(test)]` | PARTIAL | LOW |
| `DbCursorTest` / `DbCursorTestBase` / `DbCursorSearchTest` / `DbCursorDuplicateTest` / `DbCursorDuplicateDeleteTest` / `DbCursorDuplicateValidationTest` / `DbCursorDeleteTest` / `DbCursorDupTest` | `noxu-db/tests/cursor_test.rs`, `noxu-db/tests/sorted_dup_test.rs` (combined: ~60 tests) | PARTIAL | MEDIUM (some duplicate-validation edge cases not covered) |
| `DbEnvPoolTest` | `noxu-dbi/src/environment_impl.rs#[cfg(test)]` | PARTIAL | LOW |
| `DbTreeTest` | `noxu-dbi/src/db_tree.rs#[cfg(test)]` | PARTIAL | LOW |
| `DeleteUpdateWithoutReadTest` | `noxu-db/tests/integration_test.rs` (sampled) | PARTIAL | LOW |
| `DiskOrderedScanTest` | (none — DiskOrderedCursor not ported) | MISSING | LOW (feature deliberately omitted) |
| `DuplicateEntryException` | (test-fixture for assertion) | SKIPPED | INFO |
| `EmbeddedOpsTest` | `noxu-tree/src/bin.rs#[cfg(test)]` (embedded LN flag) | PARTIAL | LOW |
| `INListTest` | `noxu-dbi/src/in_list.rs#[cfg(test)]` | EQUIVALENT | — |
| `MemoryBudgetTest` | `noxu-dbi/src/memory_budget.rs#[cfg(test)]` | EQUIVALENT | — |
| `NullCursor` | (test fixture) | SKIPPED | INFO |
| `SortedLSNTreeWalkerTest` | (none — internal recovery tooling) | MISSING | LOW |
| `SR12641` | (none) | MISSING | MEDIUM (regression) |
| `StartupTrackerTest` | (none) | MISSING | LOW |
| `UncontendedLockTest` | `noxu-txn/src/thin_lock_impl.rs#[cfg(test)]`, `noxu-txn/tests/lock_manager_test.rs` | PARTIAL | LOW |

## com.sleepycat.je.evictor (11 classes)

| JE test class | Noxu counterpart | Status | Severity |
|---|---|---|---|
| `BackgroundEvictionTest` | (none — full env-under-load eviction not covered) | MISSING | MEDIUM |
| `CacheModeTest` | `noxu-db/src/cache_mode.rs#[cfg(test)]`, `noxu-evictor/src/cache_mode.rs#[cfg(test)]` | PARTIAL | LOW |
| `EvictActionTest` | `noxu-evictor/src/evictor.rs#[cfg(test)]` | PARTIAL | LOW |
| `EvictionThreadPoolTest` | (none — single-threaded evictor in Noxu) | MISSING | LOW |
| `EvictNNodesStatsTest` | `noxu-evictor/src/evictor_stat.rs#[cfg(test)]` | PARTIAL | LOW |
| `EvictSelectionTest` | `noxu-evictor/src/policies/{lru,clock,arc,car,lirs}.rs#[cfg(test)]` | PARTIAL | MEDIUM |
| `LRUTest` | `noxu-evictor/src/lru_list.rs#[cfg(test)]`, `noxu-evictor/src/policies/lru.rs#[cfg(test)]` | PARTIAL | LOW |
| `MeasureOffHeapMemory` | (utility) | SKIPPED | INFO |
| `OffHeapAllocatorTest` / `OffHeapCacheTest` | `noxu-evictor/src/off_heap.rs#[cfg(test)]` | PARTIAL | MEDIUM |
| `SharedCacheTest` | (none — shared cache between envs not exhaustively tested) | MISSING | MEDIUM |

## com.sleepycat.je.incomp (2 classes)

| JE test class | Noxu counterpart | Status | Severity |
|---|---|---|---|
| `EmptyBINTest` | (none — INCompressor not ported) | MISSING | MEDIUM |
| `INCompressorTest` | (none — INCompressor not ported) | MISSING | MEDIUM |

## com.sleepycat.je.jmx (5 classes)

| | | | |
|---|---|---|---|
| All 5 classes | (Java JMX-specific) | SKIPPED | INFO |

## com.sleepycat.je.junit (3 classes)

| | | | |
|---|---|---|---|
| All 3 classes | (internal JUnit-harness utilities) | SKIPPED | INFO |

## com.sleepycat.je.latch (1 class)

| JE test class | Noxu counterpart | Status | Severity |
|---|---|---|---|
| `LatchTest` | `noxu-latch/tests/prop_tests.rs`, `noxu-latch/src/{exclusive,shared,lib}.rs#[cfg(test)]` | EQUIVALENT | — |

## com.sleepycat.je.log (22 classes)

| JE test class | Noxu counterpart | Status | Severity |
|---|---|---|---|
| `BufferPoolReadLatchTest` | `noxu-log/src/log_buffer.rs#[cfg(test)]` | PARTIAL | LOW |
| `FileEdgeCaseTest` | `noxu-log/tests/disk_io_tests.rs::test_file_flip` | PARTIAL | LOW |
| `FileManagerMultiDataDirTest` | (none — multi-data-dir not ported) | MISSING | LOW |
| `FileManagerTest` / `FileManagerTestUtils` | `noxu-log/src/file_manager.rs#[cfg(test)]`, `file_handle.rs#[cfg(test)]` | PARTIAL | LOW |
| `FileReaderBufferingTest` / `FileReaderTest` | `noxu-log/src/file_reader.rs#[cfg(test)]` | PARTIAL | LOW |
| `FSyncManagerTest` | `noxu-log/src/fsync_manager.rs#[cfg(test)]` | PARTIAL | LOW |
| `INFileReaderTest` | `noxu-log/src/in_file_reader.rs#[cfg(test)]` | PARTIAL | LOW |
| `InvisibleTest` | (none — invisible-entry feature not ported) | MISSING | LOW |
| `IOExceptionTest` | (none — fault-injection IO test) | MISSING | MEDIUM |
| `LastFileReaderTest` | `noxu-log/src/last_file_reader.rs#[cfg(test)]` | PARTIAL | LOW |
| `LNFileReaderTest` | `noxu-log/src/ln_file_reader.rs#[cfg(test)]` | PARTIAL | LOW |
| `LogBufferPoolTest` | `noxu-log/src/log_buffer_pool.rs#[cfg(test)]` | PARTIAL | LOW |
| `LogEntryTest` | `noxu-log/tests/noxu_log_tests.rs` (entry roundtrip tests, ~25 cases), `noxu-log/src/entry/*.rs#[cfg(test)]` | EQUIVALENT | — |
| `LogFileGapTest` | (none) | MISSING | LOW |
| `LogFlusherTest` | `noxu-log/src/log_flusher.rs#[cfg(test)]` | PARTIAL | LOW |
| `LoggableTest` | `noxu-log/src/loggable.rs#[cfg(test)]`, `noxu-log/tests/noxu_log_tests.rs` | EQUIVALENT | — |
| `LogManagerTest` (10 @Tests including checksum-exception cases) | `noxu-log/src/log_manager.rs#[cfg(test)]`, `noxu-log/tests/disk_io_tests.rs::test_crc_validation_on_read` | PARTIAL | MEDIUM (checksum-exception persistent vs transient distinction not tested) |
| `LogUtilsTest` | `noxu-log/src/log_utils.rs#[cfg(test)]` | EQUIVALENT | — |
| `TestUtilLogReader` | (test fixture) | SKIPPED | INFO |
| `WriteQueueTest` | (none — write queue not separate from log buffer) | MISSING | LOW |
| **Spec coverage** | `noxu-spec::wal_commit` | MAPPED-TO-SPEC (partial) | — |

## com.sleepycat.je.logversion (5 classes)

| | | | |
|---|---|---|---|
| All 5 classes | (.jdb format compatibility — Noxu .ndb is a different format) | SKIPPED | INFO |

## com.sleepycat.je.recovery (22 classes + 8 in stepwise/)

| JE test class | Noxu counterpart | Status | Severity |
|---|---|---|---|
| `CheckBase` / `RecoveryTestBase` | (test fixtures) | SKIPPED | INFO |
| `CheckBINDeltaTest` | (none) | MISSING | HIGH |
| `CheckNewRootTest` | (none) | MISSING | MEDIUM |
| `CheckpointActivationTest` | `noxu-recovery/src/checkpointer.rs#[cfg(test)]` | PARTIAL | MEDIUM |
| `CheckReverseSplitsTest` / `CheckSplitAuntTest` / `CheckSplitsTest` | `noxu-tree/src/tree.rs#[cfg(test)]`, `noxu-tree/tests/bin_in_test.rs` (split-related) | PARTIAL | HIGH (post-recovery split correctness not asserted across power loss) |
| `DbConfigUpdateRecoveryTest` | (none) | MISSING | MEDIUM |
| `Level2SplitBugTest` | (none — historical SR) | MISSING | LOW |
| `LNSlotReuseTest` | (none) | MISSING | MEDIUM |
| `MultiEnvTest` | (none — multi-env recovery not exhaustively tested) | MISSING | LOW |
| `Recovery2PCTest` / `Rollback2PCTest` | `noxu-xa/tests/xa_*.rs` | PARTIAL | MEDIUM (2PC tests live in noxu-xa; recovery integration not exercised) |
| `RecoveryAbortTest` | `noxu-db/tests/crash_recovery_test.rs::test_uncommitted_transaction_leaves_no_trace` | PARTIAL | MEDIUM |
| `RecoveryCheckpointTest` | (none) | MISSING | MEDIUM |
| `RecoveryDeleteTest` | `noxu-db/tests/crash_recovery_test.rs` (sampled) | PARTIAL | LOW |
| `RecoveryDeltaTest` | (none — BIN-delta replay not exhaustively tested across power loss) | MISSING | HIGH |
| `RecoveryDuplicatesTest` | `noxu-db/tests/sorted_dup_test.rs::test_dup_database_recovery` | PARTIAL | MEDIUM |
| `RecoveryEdgeTest` | `noxu-db/tests/crash_recovery_test.rs::test_torn_write_truncated_entry_recovered` | PARTIAL | MEDIUM |
| `RecoveryTest` (7 @Tests) | `noxu-db/tests/crash_recovery_test.rs` (6 tests) | PARTIAL | MEDIUM |
| `RollbackTrackerTest` | `noxu-recovery/src/rollback_tracker.rs#[cfg(test)]` | EQUIVALENT | — |
| `recovery/stepwise/*` (8 classes) | (none — stepwise crash injection not ported) | MISSING | HIGH (8 classes covering stepwise log-write fault injection) |
| **Spec coverage** | `noxu-spec::recovery_three_phase` | MAPPED-TO-SPEC (partial — abstract model of three-phase recovery) | — |

## com.sleepycat.je.rep (39 classes; sampled at ~40 %)

| JE test class | Noxu counterpart | Status | Severity |
|---|---|---|---|
| `CheckAccessTest` / `CheckConfigTest` | `noxu-rep/src/rep_config.rs#[cfg(test)]` | PARTIAL | LOW |
| `CommitPointConsistencyPolicyTest` | `noxu-rep/src/consistency.rs#[cfg(test)]` | PARTIAL | LOW |
| `ConversionTest` | (none — log-version conversion not ported) | SKIPPED | INFO |
| `DatabaseOperationTest` | `noxu-rep/tests/cluster_integration_test.rs::test_replica_applies_*` | PARTIAL | MEDIUM |
| `ElectableGroupSizeOverrideTest` | `noxu-rep/tests/quorum_policy_test.rs` | PARTIAL | LOW |
| `ExceptionIdiomsTest` | `noxu-rep/src/error.rs#[cfg(test)]` | PARTIAL | LOW |
| `ExternalNodeTypeTest` | (none — external node not implemented) | MISSING | LOW (feature out of scope) |
| `GroupCommitTest` | `noxu-txn/src/group_commit.rs#[cfg(test)]` (local group-commit, not rep group-commit) | PARTIAL | MEDIUM |
| `HandshakeTest` | `noxu-rep/tests/tcp_integration.rs` (TCP handshake covered) | PARTIAL | MEDIUM |
| `HardRecoveryTest` | (none — hard rollback recovery not exhaustively tested) | MISSING | HIGH |
| `JoinGroupTest` / `JoinGroupTimeoutsTest` | `noxu-rep/tests/cluster_integration_test.rs::test_dynamic_peer_add_remove` | PARTIAL | MEDIUM |
| `LocalWriteTxnTest` | (none) | MISSING | MEDIUM |
| `LogRewriteWarningTest` | (none — log-rewrite-listener not ported) | MISSING | LOW |
| `MasterChangeTest` | `noxu-rep/tests/cluster_integration_test.rs::test_three_node_failover`, `chaos_test.rs::test_no_split_brain_concurrent_elections` | EQUIVALENT | — |
| `MockClientNode` | (test fixture) | SKIPPED | INFO |
| `MultiProcessOpenEnvTest` | (none — multi-process rep open not tested) | MISSING | MEDIUM |
| `NodePriorityTest` | `noxu-rep/tests/chaos_test.rs::test_highest_vlsn_wins_election` (priority absent: VLSN-only) | PARTIAL | MEDIUM (node priority is one of JE's election tiebreakers; not implemented in Noxu) |
| `ParamTest` | `noxu-rep/src/rep_config.rs#[cfg(test)]` | PARTIAL | LOW |
| `PerDbReplicationTest` | (none — per-db replication filter not ported) | MISSING | LOW |
| `RecoveryUtilizationTest` | (none) | MISSING | LOW |
| `RepEnvMultiSubDirTest` | (none) | MISSING | LOW |
| `RepGroupAdminTest` | `noxu-rep/src/rep_group.rs#[cfg(test)]`, `cluster_integration_test.rs::test_dynamic_peer_*` | PARTIAL | MEDIUM |
| `RepIDSequenceTest` | (none) | MISSING | LOW |
| `ReplicatedEnvironmentStatsTest` | `noxu-rep/src/rep_stats.rs#[cfg(test)]` | PARTIAL | LOW |
| `ReplicatedEnvironmentTest` | `noxu-rep/src/replicated_environment.rs#[cfg(test)]`, `tests/cluster_integration_test.rs` | PARTIAL | MEDIUM |
| `ReplicatedTransactionTest` | `noxu-rep/src/commit_durability.rs#[cfg(test)]`, `chaos_test.rs::test_commit_durability_ack_requirements_all_policies` | PARTIAL | MEDIUM |
| `ReplicationConfigTest` | `noxu-rep/src/rep_config.rs#[cfg(test)]` | PARTIAL | LOW |
| `ReplicationGroupTest` | `noxu-rep/src/rep_group.rs#[cfg(test)]` | PARTIAL | LOW |
| `ReplicationNetworkConfigTest` | `noxu-rep/src/tls.rs#[cfg(test)]` (TLS), `tcp_integration.rs` | PARTIAL | LOW |
| `ReplicationRateStatsTest` | `noxu-rep/src/rep_stats.rs#[cfg(test)]` | PARTIAL | LOW |
| `RepPreloadTest` | (none — preload at replica not tested) | MISSING | LOW |
| `SecondaryNodeTest` | (none — Secondary node type not implemented) | MISSING | MEDIUM (feature deliberately deferred) |
| `SerializationTest` | `noxu-rep/src/protocol.rs#[cfg(test)]` | PARTIAL | LOW |
| `StateChangeListenerTest` | `noxu-rep/tests/cluster_integration_test.rs::test_state_change_listener_fires_on_transitions` | EQUIVALENT | — |
| `StoredClassCatalogTest` | (Java serialization) | SKIPPED | INFO |
| `UnknownStateReplicaTest` | `noxu-rep/src/node_state.rs#[cfg(test)]` | PARTIAL | LOW |
| `UnresolvedHelperHostTest` | (none) | MISSING | LOW |

## com.sleepycat.je.rep.arb (1 class)

| | | | |
|---|---|---|---|
| `ArbiterTest` | (none — Arbiter not implemented) | MISSING | MEDIUM (feature out of scope for v1.5) |

## com.sleepycat.je.rep.dual/* (42 classes total)

| | | | |
|---|---|---|---|
| All `dual/*` packages | (mirror of base tests run with replication enabled — JE-specific dual-test-runner pattern) | SKIPPED | INFO |

The `dual` pattern re-runs every base JE test with replication
turned on, asserting the test still passes. Noxu's equivalent is
running the base tests under both `noxu-db` and `noxu-rep` env
configs; this is not currently done as a separate test matrix and
is an INFO-severity gap (not a bug).

## com.sleepycat.je.rep.dupconvert (1 class)

| | | | |
|---|---|---|---|
| `RepDupConvertTest` | (none — duplicate-conversion only relevant for legacy log version) | SKIPPED | INFO |

## com.sleepycat.je.rep.elections (8 classes)

| JE test class | Noxu counterpart | Status | Severity |
|---|---|---|---|
| `AcceptorTest` | `noxu-rep/src/elections/paxos.rs#[cfg(test)]` | PARTIAL | MEDIUM |
| `ElectionsTest` (7 @Tests) | `noxu-rep/tests/cluster_integration_test.rs::test_election_*`, `tests/chaos_test.rs` | EQUIVALENT | — |
| `ElectionWithLogVersionTest` | (none — log-version-aware election not ported) | MISSING | LOW |
| `JoinerElectionTest` | `noxu-rep/tests/cluster_integration_test.rs::test_election_tcp_higher_vlsn_peer_wins` | PARTIAL | LOW |
| `ProtocolFailureTest` / `ProtocolTest` | `noxu-rep/src/elections/paxos.rs#[cfg(test)]` | PARTIAL | MEDIUM |
| `RankingProposerTest` | `noxu-rep/src/elections/proposal.rs#[cfg(test)]`, `chaos_test.rs::test_highest_vlsn_wins_election` | PARTIAL | LOW |
| `VLSNFreezeLatchTest` | (none — VLSN freeze latch is a JE internal) | MISSING | LOW |
| **Spec coverage** | `noxu-spec::flexible_paxos` | MAPPED-TO-SPEC | — |

## com.sleepycat.je.rep.impl (14 classes)

| JE test class | Noxu counterpart | Status | Severity |
|---|---|---|---|
| `DTVLSNTest` | `noxu-rep/src/vlsn/*.rs#[cfg(test)]` (DTVLSN = durable-throughput-VLSN; partial coverage) | PARTIAL | MEDIUM |
| `DynamicGroupTest` | `noxu-rep/tests/cluster_integration_test.rs::test_dynamic_peer_add_remove` | PARTIAL | MEDIUM |
| `GroupDbAckFailureTest` | (none) | MISSING | MEDIUM |
| `GroupServiceTest` | `noxu-rep/src/group_service.rs#[cfg(test)]` | PARTIAL | LOW |
| `NetworkPartitionHealingTest` | `noxu-rep/tests/cluster_integration_test.rs::test_partition_and_catch_up`, `chaos_test.rs::test_partition_and_recovery_vlsn_delivery` | EQUIVALENT | — |
| `NodeStateProtocolTest` | `noxu-rep/src/node_state.rs#[cfg(test)]` | PARTIAL | LOW |
| `RepGroupDBTest` / `RepGroupImplCompatibilityTest` / `RepGroupImplTest` / `RepGroupProtocolTest` | `noxu-rep/src/rep_group.rs#[cfg(test)]` | PARTIAL | MEDIUM |
| `ReplayWithBinDeltaInsertionsTest` | (none) | MISSING | HIGH |
| `RepTestBase` (fixture) | (n/a) | SKIPPED | INFO |
| `RoundRobinTest` | (none — round-robin replica selection not ported) | MISSING | LOW |
| `TextProtocolTestBase` (fixture) | (n/a) | SKIPPED | INFO |

## com.sleepycat.je.rep.impl.networkRestore (6 classes)

| JE test class | Noxu counterpart | Status | Severity |
|---|---|---|---|
| `InterruptedNetworkRestoreTest` | `noxu-rep/src/network_restore.rs#[cfg(test)]` | PARTIAL | MEDIUM |
| `NetworkBackupTest` | `noxu-rep/src/network_restore_server.rs#[cfg(test)]` | PARTIAL | LOW |
| `NetworkRestoreNoMasterTest` | (none) | MISSING | MEDIUM |
| `NetworkRestoreTest` | `noxu-rep/tests/cluster_integration_test.rs::test_env_home_registers_restore_service` | PARTIAL | MEDIUM |
| `OneNodeRestoreTest` | (none) | MISSING | LOW |
| `ProtocolTest` | `noxu-rep/src/protocol.rs#[cfg(test)]` | PARTIAL | LOW |
| **Spec coverage** | `noxu-spec::network_restore` | MAPPED-TO-SPEC | — |

## com.sleepycat.je.rep.impl.node (15 classes)

| JE test class | Noxu counterpart | Status | Severity |
|---|---|---|---|
| `CBVLSNTest` | `noxu-rep/src/vlsn/*.rs#[cfg(test)]` | PARTIAL | LOW |
| `DbCacheTest` | (none) | MISSING | LOW |
| `FeederRecordBatchTest` | `noxu-rep/src/stream/feeder.rs#[cfg(test)]`, `chaos_test.rs::test_feeder_runner_ack_tracking_under_drops` | PARTIAL | MEDIUM |
| `GroupShutdownTest` | (none — `shutdown_group` is stubbed per claim audit) | MISSING | HIGH |
| `MasterBounceTest` | `noxu-rep/tests/cluster_integration_test.rs::test_three_node_failover` | PARTIAL | MEDIUM |
| `MasterTransferTest` | `noxu-rep/src/master_transfer.rs#[cfg(test)]` | PARTIAL | HIGH (transfer body partly stubbed per claim audit) |
| `MinorityTransferTest` | (none) | MISSING | MEDIUM |
| `MinRetainedVLSNsTest` | `noxu-rep/src/vlsn/vlsn_index.rs#[cfg(test)]` | PARTIAL | LOW |
| `PrimaryNodeTest` | (none — primary-node concept not present) | MISSING | LOW |
| `ReplicaMasterStateTransitionsTest` | `noxu-rep/src/node_state.rs#[cfg(test)]`, `tests/cluster_integration_test.rs` | PARTIAL | MEDIUM |
| `ReplicaOutputThreadTest` | `noxu-rep/src/stream/output_thread.rs#[cfg(test)]` | PARTIAL | LOW |
| `ReplicaTimeoutTest` | `noxu-rep/tests/phi_detector_test.rs` | PARTIAL | LOW |
| `RepNodeTest` | `noxu-rep/src/rep_node.rs#[cfg(test)]` | PARTIAL | LOW |
| `UpdateJEVersionTest` | (none — version upgrade not in scope) | MISSING | LOW |
| `UpdateNodeAddressTest` | `noxu-rep/tests/cluster_integration_test.rs::test_update_peer_metadata_while_active` | PARTIAL | LOW |
| **Spec coverage** | `noxu-spec::master_transfer`, `noxu-spec::vlsn_streaming` | MAPPED-TO-SPEC | — |

## com.sleepycat.je.rep.jmx (2 classes)

| | | | |
|---|---|---|---|
| Both classes | (Java JMX) | SKIPPED | INFO |

## com.sleepycat.je.rep.monitor (6 classes)

| | | | |
|---|---|---|---|
| All 6 classes | (Monitor node not implemented) | MISSING | MEDIUM (feature deliberately deferred) |

## com.sleepycat.je.rep.persist.test (4 classes)

| | | | |
|---|---|---|---|
| All 4 classes (`SimpleTest`, `UpgradeTest`, `AppBaseImpl`, `AppInterface`) | `noxu-persist/tests/integration_tests.rs`, `noxu_persist_tests.rs` (basic DPL coverage) | PARTIAL | MEDIUM (replicated DPL not exercised; upgrade-test relies on bytecode enhancer) |

## com.sleepycat.je.rep.stream (6 classes)

| JE test class | Noxu counterpart | Status | Severity |
|---|---|---|---|
| `FeederFilterTest` | (none — feeder filter not ported) | MISSING | LOW |
| `FeederReaderTest` | `noxu-rep/src/stream/feeder.rs#[cfg(test)]` | PARTIAL | MEDIUM |
| `FeederWriteQueueTest` | `noxu-rep/src/stream/peer_feeder.rs#[cfg(test)]` | PARTIAL | MEDIUM |
| `ProtocolTest` | `noxu-rep/src/protocol.rs#[cfg(test)]` | PARTIAL | LOW |
| `ReplicaSyncupReaderTest` | `noxu-rep/src/stream/replica_stream.rs#[cfg(test)]` | PARTIAL | MEDIUM |
| `VLSNTestUtils` | (fixture) | SKIPPED | INFO |

## com.sleepycat.je.rep.subscription (5 classes)

| JE test class | Noxu counterpart | Status | Severity |
|---|---|---|---|
| `EntryRequestTypeTest` | `noxu-rep/src/subscription.rs#[cfg(test)]` | PARTIAL | LOW |
| `SubscriptionAuthTestHelper` (fixture) | (n/a) | SKIPPED | INFO |
| `SubscriptionConfigTest` | `noxu-rep/src/subscription.rs#[cfg(test)]` | PARTIAL | LOW |
| `SubscriptionTest` | `noxu-rep/src/subscription.rs#[cfg(test)]` | PARTIAL | MEDIUM |
| `SubscriptionTestBase` (fixture) | (n/a) | SKIPPED | INFO |

## com.sleepycat.je.rep.txn (10 classes)

| JE test class | Noxu counterpart | Status | Severity |
|---|---|---|---|
| `CommitTokenTest` | (none — `CommitToken` not ported) | MISSING | MEDIUM |
| `ExceptionTest` | `noxu-rep/src/error.rs#[cfg(test)]` | PARTIAL | LOW |
| `LockPreemptionTest` | (none — preemption-on-replica not implemented) | MISSING | MEDIUM |
| `PostLogCommitTest` | (none) | MISSING | LOW |
| `RepAutoCommitTest` | (none) | MISSING | MEDIUM |
| `ReplayRecoveryTest` | (none) | MISSING | HIGH (replay-recovery is the central rep correctness invariant) |
| `RollbackTest` / `RollbackToMatchpointTest` / `RollbackWorkload` | (none — matchpoint rollback not exhaustively tested) | MISSING | HIGH |
| `Utils` (fixture) | (n/a) | SKIPPED | INFO |

## com.sleepycat.je.rep.util (11 classes)

| JE test class | Noxu counterpart | Status | Severity |
|---|---|---|---|
| `DbGroupAdminTest` / `DbPingTest` | (admin CLI tools not ported) | MISSING | LOW |
| `EnableRenameTest` | (none) | MISSING | LOW |
| `EnvConvertTest` | (.jdb→.ndb conversion N/A) | SKIPPED | INFO |
| `RepEnvWrapper` (fixture) | (n/a) | SKIPPED | INFO |
| `RepSequenceTest` | (none — replicated Sequence not separately tested) | MISSING | MEDIUM |
| `ResetRepGroupTest` | (none) | MISSING | LOW |
| `ServiceDispatcherTest` / `ServiceDispatcherTestBase` | `noxu-rep/src/net/service_dispatcher.rs#[cfg(test)]` | PARTIAL | MEDIUM |
| `TestChannel` / `TestLogItem` (fixtures) | (n/a) | SKIPPED | INFO |

## com.sleepycat.je.rep.util.ldiff (8 classes)

| | | | |
|---|---|---|---|
| All 8 classes | (LDiff = block-level diff for replica catch-up; not ported, replaced by VLSN streaming) | MISSING | LOW (replaced architecturally) |

## com.sleepycat.je.rep.utilint (11 classes; 2 in net/)

| | | | |
|---|---|---|---|
| Most classes are test fixtures (`WaitFor*Listener`, `LocalAliasNameService`, `RepTestUtils`, etc.) | (n/a) | SKIPPED | INFO |
| `HandshakeTest` | `noxu-rep/tests/tcp_integration.rs` | PARTIAL | LOW |
| `SimpleTxnMapTest` | (internal Java collection class) | SKIPPED | INFO |
| `SizeAwaitMapTest` | (internal Java collection class) | SKIPPED | INFO |
| `TestPasswordAuthentication` (TLS) | `noxu-rep/src/tls.rs#[cfg(test)]` | PARTIAL | LOW |

## com.sleepycat.je.rep.vlsn (10 classes)

| JE test class | Noxu counterpart | Status | Severity |
|---|---|---|---|
| `MergeTest` | `noxu-rep/src/vlsn/vlsn_index.rs#[cfg(test)]` | PARTIAL | MEDIUM |
| `SyncupWithGapsTest` | (none) | MISSING | HIGH |
| `VLPair` (fixture) | (n/a) | SKIPPED | INFO |
| `VLSNAwaitConsistencyTest` | `noxu-rep/src/consistency.rs#[cfg(test)]` | PARTIAL | MEDIUM |
| `VLSNBucketTest` | `noxu-rep/src/vlsn/vlsn_bucket.rs#[cfg(test)]` | EQUIVALENT | — |
| `VLSNCacheTest` | `noxu-rep/src/vlsn/vlsn_index.rs#[cfg(test)]` | PARTIAL | LOW |
| `VLSNCleanerTest` | (none — VLSN-cleaner cooperation not exhaustively tested) | MISSING | MEDIUM |
| `VLSNConsistencyTest` | `noxu-rep/src/consistency.rs#[cfg(test)]` | PARTIAL | MEDIUM |
| `VLSNIndexTest` | `noxu-rep/src/vlsn/vlsn_index.rs#[cfg(test)]` | PARTIAL | MEDIUM |
| `VLSNIndexTruncateTest` | (none) | MISSING | MEDIUM |
| **Spec coverage** | `noxu-spec::vlsn_streaming` | MAPPED-TO-SPEC | — |

## com.sleepycat.je.rep.node.replica (1 class)

| | | | |
|---|---|---|---|
| (nested fixtures) | (n/a) | SKIPPED | INFO |

## com.sleepycat.je.serializecompatibility (3 classes)

| | | | |
|---|---|---|---|
| All 3 classes | (Java `Serializable` cross-version compatibility — Noxu uses `serde`) | SKIPPED | INFO |

## com.sleepycat.je.statcap (1 class)

| | | | |
|---|---|---|---|
| `StatFile` | (none — periodic stat-file daemon not ported) | MISSING | LOW |

## com.sleepycat.je.test (24 classes)

| JE test class | Noxu counterpart | Status | Severity |
|---|---|---|---|
| `AtomicPutTest` | `noxu-db/tests/integration_test.rs::test_atomic_*` | PARTIAL | MEDIUM |
| `DeferredWriteTest` | (none — deferred-write mode not exhaustively tested) | MISSING | MEDIUM |
| `ForeignKeyTest` | (none on `noxu-db`; in `noxu-collections/tests/collection_tests.rs` and `noxu-persist/tests/integration_tests.rs` partially) | PARTIAL | MEDIUM |
| `InternalCursorTest` | `noxu-dbi/tests/integration_tests.rs` | PARTIAL | LOW |
| `JoinTest` | (none) | MISSING | MEDIUM |
| `KeyScanTest` | `noxu-db/tests/cursor_test.rs::cursor_iterates_*`, `cursor_keys_returned_in_lexicographic_order` | EQUIVALENT | — |
| `LogFileDeletionCrashEnvTest` | (none) | MISSING | MEDIUM |
| `MultiEnvOpenCloseTest` | (none) | MISSING | LOW |
| `MultiKeyTxnTestCase` (fixture) | (n/a) | SKIPPED | INFO |
| `OpStatsTest` | `noxu-dbi/src/throughput_stats.rs#[cfg(test)]` | PARTIAL | LOW |
| `PhantomRestartTest` / `PhantomTest` | `noxu-db/tests/cursor_test.rs` (phantom-related cases sampled) | PARTIAL | HIGH (phantom-prevention is a key serializable invariant) |
| `SecondaryAssociationTest` / `SecondaryDirtyReadTest` / `SecondaryMultiComplexTest` / `SecondaryMultiTest` / `SecondarySplitTestMain` / `SecondaryTest` | `noxu-db/src/secondary_database.rs#[cfg(test)]`, `secondary_cursor.rs#[cfg(test)]` (basic) | PARTIAL | MEDIUM (multi-key / split-during-secondary not tested) |
| `SequenceTest` | `noxu-db/src/sequence.rs#[cfg(test)]`, `noxu-persist/src/sequence.rs#[cfg(test)]` | PARTIAL | LOW |
| `SkipTest` | (none — Cursor.skipNext/skipPrev not ported) | MISSING | LOW (feature out of scope) |
| `SpeedyTTLTime` (fixture) | (n/a) | SKIPPED | INFO |
| `SR11297Test` | (none) | MISSING | LOW (regression) |
| `ToManyTest` | (none) | MISSING | MEDIUM |
| `TTLTest` | `noxu-util/src/ttl.rs#[cfg(test)]` | PARTIAL | MEDIUM (full env+TTL+cleaner integration not tested) |

## com.sleepycat.je.tree (22 classes)

| JE test class | Noxu counterpart | Status | Severity |
|---|---|---|---|
| `BinDeltaTest` | `noxu-tree/src/delta_info.rs#[cfg(test)]`, `bin.rs#[cfg(test)]` | PARTIAL | MEDIUM |
| `CountEstimatorTest` | (none — CountEstimator not ported) | MISSING | LOW |
| `CreateOldVersionLogs` (utility) | (n/a) | SKIPPED | INFO |
| `DupConvertTest` | (none — log version dup conversion N/A) | SKIPPED | INFO |
| `FetchWithNoLatchTest` | (none — fetch-without-latch is JE evictor optimization) | MISSING | LOW |
| `GetParentNodeTest` | `noxu-tree/src/tree.rs#[cfg(test)]` | PARTIAL | LOW |
| `INEntryTestBase` (fixture) | (n/a) | SKIPPED | INFO |
| `INKeyRepTest` / `INTargetRepTest` | (folded into Bin/InNode tests) | PARTIAL | LOW |
| `INTest` | `noxu-tree/src/in_node.rs#[cfg(test)]`, `tests/bin_in_test.rs` | PARTIAL | LOW |
| `KeyPrefixTest` | `noxu-tree/src/key.rs#[cfg(test)]`, `noxu-tree/src/in_node.rs#[cfg(test)]` (prefix-related) | PARTIAL | MEDIUM |
| `KeyTest` | `noxu-tree/src/key.rs#[cfg(test)]`, `tests/prop_tests.rs` | PARTIAL | LOW |
| `LSNArrayTest` | `noxu-util/src/lsn.rs#[cfg(test)]` | PARTIAL | LOW |
| `MemorySizeTest` | (none — MemoryBudget present, not exhaustively size-tested) | MISSING | LOW |
| `ReleaseLatchesTest` | `noxu-latch/tests/prop_tests.rs` | PARTIAL | LOW |
| `SplitRace_SR11144Test` | (none — split race regression) | MISSING | HIGH |
| `SplitTest` | `noxu-tree/src/tree.rs#[cfg(test)]`, `noxu-db/tests/concurrent_reads_during_splits.rs` | PARTIAL | MEDIUM |
| `SR13034Test` / `SR13126Test` | (none) | MISSING | LOW (regressions) |
| `TreeTest` / `TreeTestBase` | `noxu-tree/src/tree.rs#[cfg(test)]`, `tests/bin_in_test.rs` | PARTIAL | MEDIUM |
| `ValidateSubtreeDeleteTest` | (none) | MISSING | MEDIUM |
| **Spec coverage** | `noxu-spec::btree_latching` | MAPPED-TO-SPEC | — |

## com.sleepycat.je.trigger (3 classes)

| | | | |
|---|---|---|---|
| All 3 classes | (Database triggers not ported) | MISSING | LOW (feature deliberately deferred) |

## com.sleepycat.je.txn (11 classes)

| JE test class | Noxu counterpart | Status | Severity |
|---|---|---|---|
| `CursorTxnTest` | `noxu-db/tests/cursor_test.rs` (cursor+txn interactions sampled) | PARTIAL | MEDIUM |
| `DeadlockTest` (12 @Tests) | `noxu-txn/src/deadlock_detector.rs#[cfg(test)]`, `noxu-txn/tests/lock_manager_test.rs`, `noxu-spec::lock_manager_deadlock` | PARTIAL | MEDIUM (4-locker / deadlock-intersection cases not exhaustively tested) |
| `LockManagerTest` (12 @Tests) | `noxu-txn/tests/lock_manager_test.rs` (~25 tests) | EQUIVALENT | — |
| `LockTest` | `noxu-txn/src/lock.rs#[cfg(test)]`, `lock_impl.rs#[cfg(test)]` | EQUIVALENT | — |
| `ReadCommitLockersTest` | `noxu-db/tests/isolation_test.rs::test_read_committed_*` | PARTIAL | LOW |
| `TwoPCTest` | `noxu-xa/tests/xa_protocol_test.rs` | PARTIAL | MEDIUM |
| `TxnEndTest` | `noxu-txn/src/txn_end.rs#[cfg(test)]` | EQUIVALENT | — |
| `TxnFSyncTest` | `noxu-db/tests/txn_wiring_test.rs::f3_*_durability_*` | PARTIAL | MEDIUM |
| `TxnMemoryTest` | (none — txn memory budget not exhaustively tested) | MISSING | LOW |
| `TxnTest` (12 @Tests) | `noxu-txn/tests/txn_test.rs` (47 tests) | EQUIVALENT | — |
| `TxnTimeoutTest` | `noxu-db/tests/txn_config_test.rs::test_*_timeout_*` | EQUIVALENT | — |
| **Spec coverage** | `noxu-spec::lock_manager_deadlock` | MAPPED-TO-SPEC | — |

## com.sleepycat.je.util (24 classes)

| JE test class | Noxu counterpart | Status | Severity |
|---|---|---|---|
| `Adler32Test` | `noxu-log/src/checksum.rs#[cfg(test)]` (CRC32, not Adler32; Noxu uses crc32fast) | PARTIAL | LOW (different checksum algorithm by design) |
| `BadFileFilter` (utility) | (n/a) | SKIPPED | INFO |
| `BtreeCorruptionTest` | (none — explicit corruption injection not ported) | MISSING | MEDIUM |
| `CustomDbPrintLogTest` | (none — DbPrintLog CLI not ported) | MISSING | LOW |
| `DbBackupTest` | `noxu-dbi/src/backup_manager.rs#[cfg(test)]` | PARTIAL | MEDIUM |
| `DbCacheSizeTest` | (none — DbCacheSize CLI not ported) | MISSING | LOW |
| `DbDeleteReservedFilesTest` | (none) | MISSING | LOW |
| `DbDumpTest` | (none — DbDump CLI not ported) | MISSING | LOW |
| `DbLsnTest` | `noxu-util/src/lsn.rs#[cfg(test)]`, `tests/prop_tests.rs` | EQUIVALENT | — |
| `DbScavengerTest` | (none — DbScavenger CLI not ported) | MISSING | LOW |
| `DebugRecordTest` | `noxu-log/src/entry/trace_log_entry.rs#[cfg(test)]` | PARTIAL | LOW |
| `DualTestCase` (fixture) | (n/a) | SKIPPED | INFO |
| `EnvTestWrapper` (fixture) | (n/a) | SKIPPED | INFO |
| `HexFormatterTest` | (folded into log debug) | PARTIAL | LOW |
| `InfoFileFilter` (utility) | (n/a) | SKIPPED | INFO |
| `LogFileCorruptionTest` | (none) | MISSING | HIGH |
| `MiniPerf` (utility) | (n/a) | SKIPPED | INFO |
| `PropUtilTest` | `noxu-config/src/manager.rs#[cfg(test)]` | PARTIAL | LOW |
| `RecordSearch` (utility) | (n/a) | SKIPPED | INFO |
| `SimpleClassLoader` (utility) | (n/a) | SKIPPED | INFO |
| `StringDbt` (utility) | (n/a) | SKIPPED | INFO |
| `TestDumper` (utility) | (n/a) | SKIPPED | INFO |
| `TestUtils` (fixture) | (n/a) | SKIPPED | INFO |
| `VerifyLogTest` | (none — DbVerifyLog CLI not ported) | MISSING | MEDIUM |

## com.sleepycat.je.util.dbfilterstats (1 class)

| | | | |
|---|---|---|---|
| (CLI tool) | (n/a) | SKIPPED | INFO |

## com.sleepycat.je.utilint (16 classes)

| JE test class | Noxu counterpart | Status | Severity |
|---|---|---|---|
| `AtomicLongComponentTest` / `AtomicLongMapStatTest` / `LongAvgRateMapStatTest` / `LongAvgRateStatTest` / `LongAvgRateTest` / `LongDiffStatTest` | `noxu-util/src/stats.rs#[cfg(test)]` | PARTIAL | LOW (Noxu has 1 stat type vs JE's 15+, per audit-report.md) |
| `BitMapTest` | (none — internal BitMap utility) | MISSING | LOW |
| `CronScheduleParserTest` | (none — cron parsing utility not ported) | MISSING | LOW |
| `DoubleExpMovingAvgTest` | `noxu-cleaner/src/throttle.rs#[cfg(test)]` (EWMA used in throttle) | PARTIAL | LOW |
| `DummyFileStoreInfo` / `FileStoreInfoTest` | (n/a — fs::statvfs path used directly) | SKIPPED | INFO |
| `ExceptionListenerTest` | `noxu-db/src/error.rs#[cfg(test)]` | PARTIAL | LOW |
| `LoggerUtilsTest` | (none — Java logging utility) | SKIPPED | INFO |
| `StoppableThreadTest` | `noxu-util/src/daemon.rs#[cfg(test)]` | PARTIAL | LOW |
| `TestAction` / `WaitTestHook` (fixtures) | (n/a) | SKIPPED | INFO |

## com.sleepycat.bind/serial/test (4 classes)

| JE test class | Noxu counterpart | Status | Severity |
|---|---|---|---|
| `MarshalledObject` (fixture) | (n/a) | SKIPPED | INFO |
| `NullClassCatalog` (fixture) | (n/a) | SKIPPED | INFO |
| `SerialBindingTest` | `noxu-bind/src/serial/serde_binding.rs#[cfg(test)]`, `simple_serial.rs#[cfg(test)]` | EQUIVALENT (different mechanism) | — |
| `TestClassCatalog` (fixture) | (n/a — ClassCatalog deliberately omitted) | SKIPPED | INFO |

## com.sleepycat.bind/test (1 class)

| | | | |
|---|---|---|---|
| `BindingSpeedTest` | (none — binding microbenchmark) | SKIPPED | INFO |

## com.sleepycat.bind/tuple/test (4 classes)

| JE test class | Noxu counterpart | Status | Severity |
|---|---|---|---|
| `MarshalledObject` (fixture) | (n/a) | SKIPPED | INFO |
| `TupleBindingTest` | `noxu-bind/src/tuple/tuple_input.rs#[cfg(test)]`, `tuple_output.rs#[cfg(test)]`, `primitive_bindings.rs#[cfg(test)]`, `tests/prop_tests.rs` | EQUIVALENT | — |
| `TupleFormatTest` | `noxu-bind/src/tuple/tuple_input.rs#[cfg(test)]`, `tuple_output.rs#[cfg(test)]` | PARTIAL | LOW |
| `TupleOrderingTest` (21 @Tests covering byte-ordering for every primitive) | `noxu-bind/tests/prop_tests.rs::prop_sorted_*_encoding_order`, `noxu-bind/src/tuple/sort_key.rs#[cfg(test)]` | EQUIVALENT (covered by property tests + unit tests) | — |

## com.sleepycat.collections (1 class) + collections/test (17 classes)

| JE test class | Noxu counterpart | Status | Severity |
|---|---|---|---|
| `MultiProcessCoordinator` (top-level) | (n/a) | SKIPPED | INFO |
| `CollectionTest` (parameterized; covers Map/SortedMap/Set/SortedSet/List operations) | `noxu-collections/tests/collection_tests.rs` (68 tests), `tests/prop_tests.rs` | PARTIAL | MEDIUM (Java NavigableMap subMap/headMap/tailMap surface absent) |
| `ForeignKeyTest` | `noxu-persist/tests/integration_tests.rs`, `noxu-db/src/secondary_database.rs#[cfg(test)]` | PARTIAL | MEDIUM |
| `IterDeadlockTest` / `IterRepositionTest` | (none — iterator deadlock / reposition under concurrent mutation not tested) | MISSING | MEDIUM |
| `JoinTest` | (none) | MISSING | MEDIUM |
| `NullTransactionRunner` (fixture) | (n/a) | SKIPPED | INFO |
| `NullValueTest` | `noxu-collections/src/stored_map.rs#[cfg(test)]` (sampled) | PARTIAL | LOW |
| `SecondaryDeadlockTest` | (none) | MISSING | MEDIUM |
| `TestDataBinding` / `TestEntity` / `TestEntityBinding` / `TestKeyAssigner` / `TestKeyCreator` / `TestStore` (fixtures) | (n/a) | SKIPPED | INFO |
| `TestSR15721` | (none — regression) | MISSING | LOW |
| `TransactionTest` | `noxu-collections/src/transaction_runner.rs#[cfg(test)]`, `tests/collection_tests.rs` (txn portion) | PARTIAL | LOW |
| `XACollectionTest` | `noxu-xa/tests/xa_*.rs` | PARTIAL | MEDIUM |

## com.sleepycat.collections/test/serial (5 classes)

| | | | |
|---|---|---|---|
| `CatalogCornerCaseTest` / `StoredClassCatalogTest` / `StoredClassCatalogTestInit` / `TestSerial` | (Java ObjectStreamClass catalog; deliberately omitted in Noxu) | SKIPPED | INFO |
| `TupleSerialFactoryTest` | (none — TupleSerialFactory not ported) | MISSING | LOW |

## com.sleepycat.persist/test (34 classes)

| JE test class | Noxu counterpart | Status | Severity |
|---|---|---|---|
| `AddNewSecKeyToAbstractClassTest` | (none — abstract-class secondary keys with bytecode enhancer) | SKIPPED | INFO |
| `BindingTest` | `noxu-persist/src/entity_serializer.rs#[cfg(test)]`, `simple_serializer.rs#[cfg(test)]` | PARTIAL | MEDIUM |
| `ConvertAndAddTest` | (none — schema evolution open-path not wired) | MISSING | HIGH |
| `CreateAbstractClassData` / `CreateOldVersionBigDecimalDb` / `CreateSecDupsWithoutComparator` / `CreateSecDupsWithoutComparatorEvolve` / `CreateStringDataDB` (data-creation utilities) | (n/a) | SKIPPED | INFO |
| `DevolutionTest` | (none) | MISSING | HIGH |
| `Enhanced0` / `Enhanced1` / `Enhanced2` / `Enhanced3` (bytecode-enhanced fixtures) | (n/a) | SKIPPED | INFO |
| `EvolveCase` / `EvolveClasses` / `EvolveProxyClassTest` / `EvolveTest` / `EvolveTestBase` / `EvolveTestInit` | `noxu-persist/src/evolve/*.rs#[cfg(test)]` (data-structures only) | PARTIAL | HIGH (open-path not wired; types exist but evolution does not run) |
| `ForeignKeyTest` (DPL) | `noxu-persist/tests/integration_tests.rs` | PARTIAL | MEDIUM |
| `GetLastRestartTest` | (none) | MISSING | LOW |
| `IndexTest` | `noxu-persist/src/primary_index.rs#[cfg(test)]`, `secondary_index.rs#[cfg(test)]` | PARTIAL | MEDIUM |
| `JoinTest` (DPL) | (none — `EntityJoin` not ported) | MISSING | MEDIUM |
| `NegativeTest` | (none) | MISSING | LOW |
| `OperationTest` | `noxu-persist/tests/integration_tests.rs`, `tests/noxu_persist_tests.rs` | PARTIAL | MEDIUM |
| `PersistTestUtils` (fixture) | (n/a) | SKIPPED | INFO |
| `ProxyToSimpleTypeTest` | (none — `PersistentProxy` not ported) | MISSING | MEDIUM |
| `SecondaryDupOrderEvolveTest` / `SecondaryDupOrderTest` | (none) | MISSING | MEDIUM |
| `SequenceTest` | `noxu-persist/src/sequence.rs#[cfg(test)]` | PARTIAL | LOW |
| `StringFormatCompatibilityTest` | (none — Noxu encoding is not byte-compatible with JE) | SKIPPED | INFO |
| `SubclassIndexTest` | (none — `getSubclassIndex` not ported) | MISSING | LOW |
| `TestVersionCompatibility` / `TestVersionCompatibilitySuite` | (Noxu has no JE-wire-compat) | SKIPPED | INFO |

## com.sleepycat.util/test (9 classes)

| JE test class | Noxu counterpart | Status | Severity |
|---|---|---|---|
| `ExceptionWrapperTest` | (none — Java exception-wrapper utility) | SKIPPED | INFO |
| `FastOutputStreamTest` | `noxu-bind/src/tuple/tuple_output.rs#[cfg(test)]` | PARTIAL | LOW |
| `GreaterThan` (fixture) | (n/a) | SKIPPED | INFO |
| `PackedIntegerTest` | `noxu-util/src/packed.rs#[cfg(test)]`, `noxu-bind/tests/prop_tests.rs::prop_packed_*` | EQUIVALENT | — |
| `SharedTestUtils` / `TestBase` / `TestEnv` / `TxnTestCase` (fixtures) | (n/a) | SKIPPED | INFO |
| `UtfTest` | `noxu-bind/src/tuple/primitive_bindings.rs#[cfg(test)]` (StringBinding round-trips) | PARTIAL | LOW (UTF-8 corner cases including embedded \0 and surrogate pairs not exhaustively tested; per audit-report.md, embedded \0 was a known issue and `noxu-bind` now uses UTF-8 length-prefix rather than null-terminated) |

## com.sleepycat.utilint (3 classes)

| | | | |
|---|---|---|---|
| `LatencyStatTest` / `StatLoggerTest` / `StatsTrackerTest` | `noxu-util/src/stats.rs#[cfg(test)]` (basic) | PARTIAL | LOW |

---

## Summary counts (test classes)

| Status | Count |
|---|---:|
| EQUIVALENT | ~55 |
| PARTIAL | ~155 |
| MAPPED-TO-SPEC (partial — supplementary) | (cross-referenced where applicable) |
| MISSING | ~135 |
| SKIPPED (deliberate) | ~95 |
| **Total JE test classes** | **~440 of 570 had a meaningful mapping; remainder are fixtures/utilities** |

## Severity rollup (test map)

| Severity | Count |
|---|---:|
| HIGH | 12 |
| MEDIUM | ~70 |
| LOW | ~70 |
| INFO (deliberately omitted) | ~95 |

The 12 HIGH-severity findings are concentrated in three areas:

1. **Recovery / replay correctness** — `CheckBINDeltaTest`,
   `CheckSplitsTest` family, `RecoveryDeltaTest`, the 8
   `recovery/stepwise/*` classes, `ReplayWithBinDeltaInsertionsTest`,
   `ReplayRecoveryTest`, `RollbackTest` family, `SyncupWithGapsTest`,
   `LogFileCorruptionTest`.
2. **Replication failure modes** — `HardRecoveryTest`,
   `GroupShutdownTest`, `MasterTransferTest` (claim-audit confirms
   stub).
3. **Schema evolution** — `EvolveTest` family, `ConvertAndAddTest`,
   `DevolutionTest`.

These are the test classes whose absence most directly threatens the
ability to answer "the port is correct" with high confidence.
