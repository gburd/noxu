# JE Constant / Default / Threshold Audit (2026-07)

## Motivation

An external review found a real semantic-drift bug in transaction
durability: `Durability::COMMIT_NO_SYNC` silently waited for **zero** replica
acknowledgments instead of JE's `SIMPLE_MAJORITY`, and all three durability
convenience constants carried the wrong `replicaSync` / `replicaAck` values.
That drift changed a correctness (durability) guarantee while leaving the
type signatures intact — the kind of bug a signature-level review misses.

The review's meta-point: *"the prior probability that this is the only such
file is low."* This audit is the systematic follow-up: a value-level diff of
every public Noxu constant, default, and threshold that maps to a JE 7.5.11
counterpart, so the sweep becomes a durable, gate-able artifact rather than a
one-off catch.

- JE reference: BDB-JE 7.5.11 (`~/ws/je`).
- Exemplar (already fixed on `origin/main`): `crates/noxu-db/src/durability.rs`
  — `COMMIT_SYNC` / `COMMIT_NO_SYNC` / `COMMIT_WRITE_NO_SYNC` all now use
  `replicaSync = NO_SYNC` + `replicaAck = SIMPLE_MAJORITY`, matching
  `Durability.java:36-64`.

## Method

For each value class below, the Noxu constant was diffed against its JE
counterpart and classified:

- **MATCH** — value equals JE.
- **DRIFT (bug)** — value silently differs from JE in a way that changes
  behavior; fixed to the JE value with a regression test.
- **Intentional deviation** — Noxu's own on-disk (`.ndb`, `LOG_VERSION = 3`)
  or replication-modernization (Flexible Paxos, QUIC) design; must **not** be
  "corrected" to JE values. Confirmed documented.
- **Flagged for human** — a divergence whose intent is unclear, or an
  on-disk-affecting inconsistency that should not be changed silently.

## Result summary

| Value class | Checked | MATCH | Drift (bug) | Intentional | Flagged |
|---|---:|---:|---:|---:|---:|
| Config parameter defaults (`params.rs` ↔ `EnvironmentParams.java`) | 152 mapped | 152 | 0 | — | — |
| Durability convenience constants | 3 | 3 | 0 (fixed prior) | — | — |
| `ReplicaAckPolicy` / `required_acks` semantics | 3 | 3 | 0 | — | — |
| `LockType` conflict matrix (5×5) | 25 | 25 | 0 | — | — |
| `LockType` upgrade matrix (5×5) | 25 | 25 | 0 | — | — |
| `LockType` indices + `causesRestart` | 7 | 7 | 0 | — | — |
| `LockMode` variants + isolation semantics | 4 | 4 | 0 | — | — |
| `CacheMode` variants | 6/7 | 6 | 0 | 1 (`DYNAMIC` unimpl., documented) | — |
| `EntryStates` slot bit values (0x01–0x40) | 6 | 6 | 0 | — | — |
| `EntryStates` extension / transient mask | 3 | — | 0 | 3 (kvmain-fork bits, documented) | — |
| XA flag values (`XaFlags`) | 9 | 9 | 0 | — | — |
| XID size limits | 2 | 2 | 0 | — | — |
| VLSN constants (`LOG_SIZE`, `NULL`, `UNINITIALIZED`) | 3 | 3 | 0 | — | — |
| Log entry-type ordinals (`entry_type.rs`) | 26 | — | 0 | 26 (`.ndb` native format) | — |
| `LOG_VERSION` constants | 3 | — | 0 | 1 (`file_header`=3, authoritative) | 1 (`entry_type::LOG_VERSION`=2, see below) |

**No new correctness drift was found in the constant/default/threshold layer
after the Durability fix.** The durability constants were the anomaly, not the
tip of an iceberg — every mapped config default and every lock/isolation/XA/
VLSN value matches JE 7.5.11 exactly. This audit records that fact so it can
be re-run as a gate.

## 1. Config parameter defaults — 152/152 MATCH

Every `noxu.*` config parameter that maps to a `je.*` parameter carries the
same default (durations normalized to seconds; JE numeric literals with `L`
suffix and `1 << n` / `n * 1024` arithmetic evaluated). Verified against
`~/ws/je/src/com/sleepycat/je/config/EnvironmentParams.java`.

<!-- The full per-param table is generated from the two sources; it is
reproduced verbatim so the audit is self-contained. -->

| Param (suffix, `je.`/`noxu.`) | Noxu default | JE 7.5.11 default | Verdict |
|---|---|---|---|
| `checkpointer.bytesInterval` | `20_000_000` | `20000000L` | MATCH |
| `checkpointer.deadlockRetry` | `3` | `3` | MATCH |
| `checkpointer.highPriority` | `false` | `false` | MATCH |
| `checkpointer.wakeupInterval` | `0s` | `"0"` | MATCH |
| `cleaner.adjustUtilization` | `false` | `false` | MATCH |
| `cleaner.backgroundProactiveMigration` | `false` | `false` | MATCH |
| `cleaner.bytesInterval` | `0` | `0L` | MATCH |
| `cleaner.deadlockRetry` | `3` | `3` | MATCH |
| `cleaner.detailMaxMemoryPercentage` | `2` | `2` | MATCH |
| `cleaner.expunge` | `true` | `true` | MATCH |
| `cleaner.fetchObsoleteSize` | `false` | `false` | MATCH |
| `cleaner.foregroundProactiveMigration` | `false` | `false` | MATCH |
| `cleaner.gradualExpiration` | `true` | `true` | MATCH |
| `cleaner.lazyMigration` | `false` | `false` | MATCH |
| `cleaner.lockTimeout` | `0.5s` | `"500 ms"` | MATCH |
| `cleaner.lookAheadCacheSize` | `8192` | `8192` | MATCH |
| `cleaner.maxBatchFiles` | `0` | `0` | MATCH |
| `cleaner.minAge` | `2` | `2` | MATCH |
| `cleaner.minFileUtilization` | `5` | `5` | MATCH |
| `cleaner.minUtilization` | `50` | `50` | MATCH |
| `cleaner.readSize` | `0` | `0` | MATCH |
| `cleaner.rmwFix` | `true` | `true` | MATCH |
| `cleaner.threads` | `1` | `1` | MATCH |
| `cleaner.trackDetail` | `true` | `true` | MATCH |
| `cleaner.twoPassGap` | `10` | `10` | MATCH |
| `cleaner.twoPassThreshold` | `0` | `0` | MATCH |
| `cleaner.upgradeToLogVersion` | `0` | `0` | MATCH |
| `cleaner.useDeletedDir` | `false` | `false` | MATCH |
| `cleaner.wakeupInterval` | `10s` | `"10 s"` | MATCH |
| `compressor.deadlockRetry` | `3` | `3` | MATCH |
| `compressor.lockTimeout` | `0.5s` | `"500 ms"` | MATCH |
| `compressor.wakeupInterval` | `5s` | `"5 s"` | MATCH |
| `deferredWrite.temp` | `false` | `false` | MATCH |
| `env.backgroundReadLimit` | `0` | `0` | MATCH |
| `env.backgroundSleepInterval` | `0.001s` | `"1 ms"` | MATCH |
| `env.backgroundWriteLimit` | `0` | `0` | MATCH |
| `env.checkLeaks` | `true` | `true` | MATCH |
| `env.comparatorsRequired` | `false` | `false` | MATCH |
| `env.dbCacheClearCount` | `100` | `100` | MATCH |
| `env.dbEviction` | `true` | `true` | MATCH |
| `env.diskOrderedScanLockTimeout` | `10s` | `"10 seconds"` | MATCH |
| `env.dupConvertPreloadAll` | `true` | `true` | MATCH |
| `env.expirationEnabled` | `true` | `true` | MATCH |
| `env.exposeUserData` | `false` | `false` | MATCH |
| `env.fairLatches` | `false` | `false` | MATCH |
| `env.forcedYield` | `false` | `false` | MATCH |
| `env.isLocking` | `true` | `true` | MATCH |
| `env.isReadOnly` | `false` | `false` | MATCH |
| `env.isTransactional` | `false` | `false` | MATCH |
| `env.latchTimeout` | `300s` | `"5 min"` | MATCH |
| `env.logTrace` | `true` | `true` | MATCH |
| `env.recovery` | `true` | `true` | MATCH |
| `env.recoveryForceCheckpoint` | `false` | `false` | MATCH |
| `env.recoveryForceNewFile` | `false` | `false` | MATCH |
| `env.runCheckpointer` | `true` | `true` | MATCH |
| `env.runCleaner` | `true` | `true` | MATCH |
| `env.runEvictor` | `true` | `true` | MATCH |
| `env.runINCompressor` | `true` | `true` | MATCH |
| `env.runOffHeapEvictor` | `true` | `true` | MATCH |
| `env.runVerifier` | `true` | `true` | MATCH |
| `env.setupLogger` | `false` | `false` | MATCH |
| `env.sharedLatches` | `true` | `true` | MATCH |
| `env.startupThreshold` | `300s` | `"5 min"` | MATCH |
| `env.terminateTimeout` | `10s` | `"10 s"` | MATCH |
| `env.ttlClockTolerance` | `7200s` | `"2 h"` | MATCH |
| `env.ttlLnPurgeDelay` | `5s` | `"5 s"` | MATCH |
| `env.ttlMaxTxnTime` | `86400s` | `"24 h"` | MATCH |
| `env.verifyBtree` | `true` | `true` | MATCH |
| `env.verifyBtreeBatchDelay` | `0.01s` | `"10 ms"` | MATCH |
| `env.verifyBtreeBatchSize` | `1000` | `1000` | MATCH |
| `env.verifyDataRecords` | `false` | `false` | MATCH |
| `env.verifyLog` | `true` | `true` | MATCH |
| `env.verifyLogReadDelay` | `0.1s` | `"100 ms"` | MATCH |
| `env.verifyMaxTardiness` | `300s` | `"5 min"` | MATCH |
| `env.verifyObsoleteRecords` | `false` | `false` | MATCH |
| `env.verifySecondaries` | `true` | `true` | MATCH |
| `evictor.allowBinDeltas` | `true` | `true` | MATCH |
| `evictor.coreThreads` | `1` | `1` | MATCH |
| `evictor.criticalPercentage` | `0` | `0` | MATCH |
| `evictor.deadlockRetry` | `3` | `3` | MATCH |
| `evictor.evictBytes` | `524_288` | `524288L` | MATCH |
| `evictor.forcedYield` | `false` | `false` | MATCH |
| `evictor.keepAlive` | `600s` | `"10 min"` | MATCH |
| `evictor.lruOnly` | `true` | `true` | MATCH |
| `evictor.maxThreads` | `10` | `10` | MATCH |
| `evictor.mutateBins` | `true` | `true` | MATCH |
| `evictor.nLRULists` | `4` | `4` | MATCH |
| `evictor.nodesPerScan` | `10` | `10` | MATCH |
| `evictor.useDirtyLRU` | `true` | `true` | MATCH |
| `freeDisk` | `5_368_709_120` | `5368709120L` | MATCH |
| `haltOnCommitAfterChecksumException` | `false` | `false` | MATCH |
| `lock.deadlockDetect` | `true` | `true` | MATCH |
| `lock.deadlockDetectDelay` | `0s` | `"0"` | MATCH |
| `lock.nLockTables` | `1` | `1` | MATCH |
| `lock.oldLockExceptions` | `false` | `false` | MATCH |
| `lock.timeout` | `0.5s` | `"500 ms"` | MATCH |
| `log.bufferSize` | `1 << 20` | `1 << 20` | MATCH |
| `log.checksumRead` | `true` | `true` | MATCH |
| `log.chunkedNIO` | `0` | `0L` | MATCH |
| `log.detectFileDelete` | `true` | `true` | MATCH |
| `log.detectFileDeleteInterval` | `1s` | `"1000 ms"` | MATCH |
| `log.directNIO` | `false` | `false` | MATCH |
| `log.faultReadSize` | `2048` | `2048` | MATCH |
| `log.fileCacheSize` | `100` | `100` | MATCH |
| `log.fileMax` | `10_000_000` | `10000000L` | MATCH |
| `log.fileWarmUpReadSize` | `10_485_760` | `10485760` | MATCH |
| `log.fileWarmUpSize` | `0` | `0` | MATCH |
| `log.flushNoSyncInterval` | `5s` | `"5 s"` | MATCH |
| `log.flushSyncInterval` | `20s` | `"20 s"` | MATCH |
| `log.fsyncTimeLimit` | `5s` | `"5 s"` | MATCH |
| `log.fsyncTimeout` | `0.5s` | `"500 ms"` | MATCH |
| `log.groupCommitInterval` | `0s` | `"0 ns"` | MATCH |
| `log.groupCommitThreshold` | `0` | `0` | MATCH |
| `log.iteratorMaxSize` | `16_777_216` | `16777216` | MATCH |
| `log.iteratorReadSize` | `8192` | `8192` | MATCH |
| `log.memOnly` | `false` | `false` | MATCH |
| `log.nDataDirectories` | `0` | `0` | MATCH |
| `log.numBuffers` | `3` | `NUM_LOG_BUFFERS_DEFAULT (=3)` | MATCH |
| `log.totalBufferBytes` | `0` | `0L` | MATCH |
| `log.useNIO` | `false` | `false` | MATCH |
| `log.useODSYNC` | `false` | `false` | MATCH |
| `log.useWriteQueue` | `true` | `true` | MATCH |
| `log.verifyChecksums` | `false` | `false` | MATCH |
| `log.writeQueueSize` | `1 << 20` | `1 << 20` | MATCH |
| `maxDisk` | `0` | `0L` | MATCH |
| `maxMemory` | `0` | `0L` | MATCH |
| `maxMemoryPercent` | `60` | `60` | MATCH |
| `maxOffHeapMemory` | `0` | `0L` | MATCH |
| `nodeDupTreeMaxEntries` | `128` | `128` | MATCH |
| `nodeMaxEntries` | `128` | `128` | MATCH |
| `offHeap.checksum` | `false` | `false` | MATCH |
| `offHeap.coreThreads` | `1` | `1` | MATCH |
| `offHeap.evictBytes` | `50 * 1024 * 1024` | `50 * 1024 * 1024L` | MATCH |
| `offHeap.keepAlive` | `600s` | `"10 min"` | MATCH |
| `offHeap.maxThreads` | `3` | `3` | MATCH |
| `rep.logFlushTaskInterval` | `300s` | `"5 min"` | MATCH |
| `rep.runLogFlushTask` | `true` | `true` | MATCH |
| `sharedCache` | `false` | `false` | MATCH |
| `stats.collect` | `true` | `true` | MATCH |
| `stats.collect.interval` | `60s` | `"1 min"` | MATCH |
| `stats.file.row.count` | `1440` | `1440` | MATCH |
| `stats.max.files` | `10` | `10` | MATCH |
| `tree.binDelta` | `25` | `25` | MATCH |
| `tree.binDeltaBlindOps` | `true` | `true` | MATCH |
| `tree.binDeltaBlindPuts` | `true` | `true` | MATCH |
| `tree.compactMaxKeyLength` | `16` | `16` | MATCH |
| `tree.maxEmbeddedLN` | `16` | `16` | MATCH |
| `tree.minMemory` | `500 * 1024` | `500L * 1024` | MATCH |
| `txn.deadlockStackTrace` | `false` | `false` | MATCH |
| `txn.dumpLocks` | `false` | `false` | MATCH |
| `txn.serializableIsolation` | `false` | `false` | MATCH |
| `txn.timeout` | `0s` | `"0"` | MATCH |

**Total mapped params compared: 152 — MATCH: 152, DRIFT: 0**

### Config-parameter coverage gaps (not drift — no consuming code)

These JE parameters have no Noxu counterpart. They are internal cleaner
cost-calculation tunables and secondary eviction knobs with no consuming code
in Noxu; they are **not** drift in an existing value. Listed for completeness
so a future implementer knows the JE defaults. **Not** added here (YAGNI —
adding config params with no consumer is dead flexibility).

| JE param | JE default | Purpose |
|---|---|---|
| `je.cleaner.minFilesToDelete` | 5 | cleaner backlog batching |
| `je.cleaner.retries` | 10 | cleaner retry budget |
| `je.cleaner.restartRetries` | 5 | cleaner restart budget |
| `je.cleaner.calc.recentLNSizes` | 10 | utilization estimator window |
| `je.cleaner.calc.minUncountedLNs` | 1000 | utilization estimator |
| `je.cleaner.calc.initialAdjustments` | 5 | utilization estimator |
| `je.cleaner.calc.minProbeSkipFiles` | 5 | utilization probe |
| `je.cleaner.calc.maxProbeSkipFiles` | 20 | utilization probe |
| `je.cleaner.cluster` | false | LN clustering |
| `je.cleaner.clusterAll` | false | LN clustering |
| `je.evictor.wakeupInterval` | 5 s | evictor daemon cadence |
| `je.evictor.useMemoryFloor` | 95 | evictor floor % |
| `je.evictor.nodeScanPercentage` | 10 | evictor scan % |
| `je.evictor.evictionBatchPercentage` | 10 | evictor batch % |
| `je.tree.maxDelta` | 10 | max BIN-deltas before full BIN log |

`je.tree.maxDelta` is the most notable: Noxu has `je.tree.binDelta` (25, the
delta *size* threshold, MATCH) but not the `maxDelta` *count* threshold. This
governs how many BIN-delta records accumulate before a full BIN is logged. It
is a logging-efficiency knob, not a correctness/durability guarantee; flagged
here for the implementer, not fixed.

## 2. Durability / isolation / lock enums — all MATCH

### 2.1 Durability constants (fixed prior on `origin/main`)

`COMMIT_SYNC` = (SYNC, NO_SYNC, SIMPLE_MAJORITY), `COMMIT_NO_SYNC` =
(NO_SYNC, NO_SYNC, SIMPLE_MAJORITY), `COMMIT_WRITE_NO_SYNC` =
(WRITE_NO_SYNC, NO_SYNC, SIMPLE_MAJORITY). Matches `Durability.java:36-64`.

### 2.2 Replica ack semantics — MATCH

`ReplicaAckPolicyKind::required_acks` implements JE's `SIMPLE_MAJORITY`
correctly: `majority = n/2 + 1` nodes; peer acks needed =
`majority - 1 = n/2` (the master's own write is one vote). For n=3 → 1 peer
ack; n=5 → 2 peer acks. `All` → `n - 1`; `None` → 0. This is the coordinator
side of the durability contract that the constant bug bypassed; it is sound —
the bug was purely in the `Durability` constants that fed it.

### 2.3 LockType conflict + upgrade matrices — MATCH

The 5×5 conflict matrix and 5×5 upgrade matrix in
`crates/noxu-txn/src/lock_type.rs` are byte-for-byte identical to
`~/ws/je/src/com/sleepycat/je/txn/LockType.java` (`conflictMatrix` at line 67,
`upgradeMatrix` at line 109). Lock-type indices (READ=0 … RESTART=6) and the
`causesRestart` set (`RANGE_READ`, `RANGE_WRITE`) match. This file already
carries an exhaustive per-entry regression test.

### 2.4 LockMode — MATCH

`DEFAULT`, `READ_UNCOMMITTED`, `READ_COMMITTED`, `RMW` — same four variants and
same isolation semantics as JE `LockMode`.

### 2.5 CacheMode — MATCH (one documented gap)

`DEFAULT`, `KEEP_HOT`, `UNCHANGED`, `MAKE_COLD`, `EVICT_LN`, `EVICT_BIN` all
present. JE's `DYNAMIC` is not implemented; `CacheMode` is already documented
as advisory at the API boundary. No serialized ordinal, so no compat concern.

## 3. Named constants / thresholds — MATCH or intentional

- **VLSN** (`noxu-util/src/vlsn.rs`): `LOG_SIZE = 8`, `NULL_VLSN_SEQUENCE = -1`,
  `UNINITIALIZED_VLSN_SEQUENCE = 0` — match `VLSN.java:24,26,35`.
- **XA flags** (`noxu-xa/src/flags.rs`): `NOFLAGS`, `JOIN`, `RESUME`,
  `TMSUCCESS`, `TMFAIL`, `TMSUSPEND`, `ONEPHASE`, `STARTRSCAN`, `ENDRSCAN` all
  match the X/Open XA (`javax.transaction.xa.XAResource`) standard values that
  JE also uses (`0x00200000`, `0x08000000`, `0x04000000`, `0x20000000`,
  `0x02000000`, `0x40000000`, `0x01000000`, `0x00800000`). `MAXGTRIDSIZE` /
  `MAXBQUALSIZE` = 64 match the XID spec.
- **EntryStates slot bits** (`noxu-tree/src/entry_states.rs`): values
  `0x01`–`0x40` match JE `EntryStates.java:23-36`. Renames (`MIGRATE_BIT` ≈
  `OFFHEAP_DIRTY_BIT`, `UPDATE_KEY_WHEN_LOGGED` ≈ `OFFHEAP_PRI2_BIT`) and the
  `TOMBSTONE_BIT` (0x80) extension are already documented as a deliberate
  kvmain-fork alignment.
- **Group-commit / checkpoint fallbacks** (`noxu-txn/src/group_commit.rs`
  `DEFAULT_MAX_GROUP_COMMIT = 20`, `DEFAULT_GROUP_COMMIT_INTERVAL_MS = 20`;
  `EnvironmentImpl::DEFAULT_CHECKPOINT_INTERVAL_MS = 30_000`): Noxu-native
  heuristic fallbacks used only when the corresponding config is unset. JE's
  group commit is time-driven in `FSyncManager` with no equivalent public
  constant; not drift.

## 4. Intentional deviations (confirmed, do not "fix")

- **`.ndb` log format**: `file_header::LOG_VERSION = 3` and the entire
  `entry_type.rs` ordinal scheme (`FileHeader=1`, `IN=2`, … `ImmutableFile=70`)
  are Noxu's own on-disk numbering, deliberately **not** JE's `LogEntryType`
  numbering. Correcting these to JE values would corrupt the format.
- **Replication modernization**: Flexible Paxos / QUIC constants in
  `noxu-rep` (`MAX_FRAME_PAYLOAD`, peer-scanner limits, etc.) are non-JE by
  design.
- **EntryStates kvmain-fork bits** (§3): documented in-file.
- **`CacheMode::DYNAMIC` omission** (§2.5): documented advisory.

## 5. Flagged for human decision

- **`entry_type::LOG_VERSION: u8 = 2` vs `file_header::LOG_VERSION: u32 = 3`.**
  There are two `LOG_VERSION` constants. The file header (authoritative,
  validated on open, `MIN_LOG_VERSION = 2`) is **3**, matching AGENTS.md and
  the docs. But `entry_header.rs` stamps every per-entry header's `version`
  field from `entry_type::LOG_VERSION`, which is **2**. The per-entry `version`
  is written but never validated on the read path, so this is presently
  cosmetic — yet it means on-disk entry headers carry `version = 2` while the
  file header says `3`. This is a **Noxu-internal inconsistency, not a
  Noxu-vs-JE drift**, and changing the stamped value alters the on-disk bytes
  of every log entry. **Not changed** by this audit (guard: do not silently
  change on-disk layout). Recommend a human decide whether to (a) bump
  `entry_type::LOG_VERSION` to 3 as part of a deliberate format revision, or
  (b) retire the unused per-entry `version` field. (`loggable.rs` also has a
  stale `CURRENT_LOG_VERSION: u8 = 15` comment "LOG_VERSION from LogEntryType"
  — same family of staleness.)

## Re-running this audit (gate)

The parameter-default half is mechanical and should be re-run whenever
`params.rs` or JE's `EnvironmentParams.java` changes:

1. Parse `crates/noxu-config/src/params.rs` for `(name, type, default)`.
2. Parse `EnvironmentParams.java` `new *ConfigParam(...)` blocks for the same.
3. Map on the shared suffix (`noxu.X` ↔ `je.X`), normalize durations to
   seconds and evaluate numeric arithmetic, diff.

The enum/matrix half (lock matrices, durability, XA) is anchored by the
regression tests in `lock_type.rs`, `durability.rs`, and
`noxu-config/tests/je_default_audit.rs` (added by this audit), which assert the
JE-matching values so a future silent edit fails a test.
