# JE → Noxu Port-Completeness Audit — May 2026 — API Map

> Companion to `je-port-audit-2026-05-overview.md`. Per-class
> mapping of the public JE surface to Noxu types and methods.

**Status legend**:

- **EQUIVALENT** — JE class/method has a Noxu counterpart with
  matching shape and behaviour (modulo Rust idiom: snake_case,
  `Result<T, NoxuError>` instead of throws).
- **RENAMED** — Noxu has it under a different name; behaviour matches.
- **PRESENT-WITH-GAPS** — Noxu has the type but not every JE method
  is exposed.
- **MISSING** — JE class or method has no Noxu counterpart.
- **DELIBERATELY-OMITTED** — feature exists in JE but is intentionally
  not ported (Java-only concern: JCA, JMX, BeanInfo, bytecode
  enhancers, Java `Serializable`, `ObjectStreamClass`, etc.).
- **PARTIAL** — Noxu type exists but the implementation is a stub or
  partial (refer to `claim-audit-2026-05.md` for the body-vs-doc gap).

---

## com.sleepycat.je

### `Environment`  →  `noxu_db::environment::Environment`

| JE method | Noxu method | Status | Notes |
|---|---|---|---|
| `close()` | `close()` | EQUIVALENT | per claim-audit, body skips closing `EnvironmentImpl` step |
| `openDatabase(Transaction, String, DatabaseConfig)` | `open_database(...)` | EQUIVALENT | |
| `openSecondaryDatabase(Transaction, String, Database, SecondaryConfig)` | `SecondaryDatabase::open(...)` | RENAMED | constructor on the type rather than a method on Environment |
| `removeDatabase(Transaction, String)` | `remove_database(...)` | EQUIVALENT | |
| `renameDatabase(Transaction, String, String)` | `rename_database(...)` | EQUIVALENT | |
| `truncateDatabase(Transaction, String, boolean)` | `truncate_database(...)` | EQUIVALENT | |
| `getHome()` | `get_home()` | EQUIVALENT | |
| `beginTransaction(Transaction parent, TransactionConfig)` | `begin_transaction(...)` | PRESENT-WITH-GAPS | nested-txn `parent` arg not honoured (Noxu: flat txns only) |
| `checkpoint(CheckpointConfig)` | `checkpoint(Option<&CheckpointConfig>)` | EQUIVALENT | |
| `sync()` | (none on Environment) | MISSING | JE syncs the entire env; Noxu only has `Database::sync` |
| `flushLog(boolean fsync)` | (none) | MISSING | use `Engine::checkpoint`/`fsync` indirectly |
| `cleanLog()` | (delegated to `Engine::clean`) | RENAMED | not on Environment public API |
| `cleanLogFile()` | (none) | MISSING | |
| `evictMemory()` | (delegated to `Engine::evict`) | RENAMED | not on Environment public API |
| `compress()` | (none) | MISSING | INCompressor: bin compression not surfaced |
| `preload(Database[], PreloadConfig)` | `Database::preload` | RENAMED + GAPS | only single-DB form |
| `openDiskOrderedCursor(...)` | (none) | MISSING | DiskOrderedCursor not ported |
| `getConfig()` | `get_config()` | EQUIVALENT | |
| `setMutableConfig(EnvironmentMutableConfig)` | `set_mutable_config(...)` | EQUIVALENT | |
| `getMutableConfig()` | `get_mutable_config()` | EQUIVALENT | |
| `getStats(StatsConfig)` | `get_stats()` | PRESENT-WITH-GAPS | StatsConfig argument absent |
| `getLockStats(StatsConfig)` | (none) | MISSING | |
| `getTransactionStats(StatsConfig)` | (none) | MISSING | |
| `verify(VerifyConfig, PrintStream)` | `verify(...)` | PARTIAL | per claim-audit, body returns empty passing `VerifyResult` without performing verification |
| `getThreadTransaction()` | (none) | MISSING | thread-local txn pattern not idiomatic in Rust |
| `setThreadTransaction(Transaction)` | (none) | MISSING | |
| `isValid()` | `is_valid()` | EQUIVALENT | |
| `isClosed()` | `is_valid()` (negation) | RENAMED | |
| `getInvalidatingException()` | (none) | MISSING | |
| `printStartupInfo(PrintStream)` | (none) | MISSING | |
| `getDatabaseNames()` | `get_database_names()` | EQUIVALENT | |
| (additional Noxu methods) | `is_transactional`, `is_read_only`, `invalidate`, `stat_fsync_count` | EXTRA | |

### `Database`  →  `noxu_db::database::Database`

| JE method | Noxu method | Status | Notes |
|---|---|---|---|
| `close()` | `close()` | EQUIVALENT | |
| `sync()` | `sync()` | EQUIVALENT (with caveat) | per claim-audit, no-ops when LogManager absent |
| `openSequence(Transaction, DatabaseEntry, SequenceConfig)` | `open_sequence(...)` | EQUIVALENT | |
| `removeSequence(Transaction, DatabaseEntry)` | (none) | MISSING | |
| `openCursor(Transaction, CursorConfig)` | `open_cursor(...)` | EQUIVALENT | |
| `openCursor(DiskOrderedCursorConfig)` | (none) | MISSING | DiskOrderedCursor absent |
| `populateSecondaries(Transaction, …)` | (none) | MISSING | |
| `delete(Transaction, DatabaseEntry, WriteOptions)` | `delete(...)` | EQUIVALENT | |
| `get(Transaction, DatabaseEntry, DatabaseEntry, ReadOptions)` | `get(...)` / `get_with_options(...)` | EQUIVALENT | |
| `getSearchBoth(...)` | (cursor-based only) | MISSING on Database | available via `Cursor::get(SearchBoth, …)` |
| `put(...)` | `put(...)` / `put_with_options(...)` | EQUIVALENT | |
| `putNoOverwrite(...)` | `put_no_overwrite(...)` | EQUIVALENT | |
| `putNoDupData(...)` | (use `put_with_options` with NO_DUP_DATA) | RENAMED | |
| `join(Cursor[], JoinConfig)` | `join(...)` | EQUIVALENT | |
| `preload(long maxBytes)` | `preload(PreloadConfig)` | EQUIVALENT | |
| `preload(long maxBytes, long maxMillisecs)` | (single form only) | PRESENT-WITH-GAPS | options bundled into PreloadConfig |
| `count()` | `count()` | EQUIVALENT | |
| `count(long memoryLimit)` | (single form only) | PRESENT-WITH-GAPS | |
| `getStats(StatsConfig)` | `get_stats(...)` | EQUIVALENT | |
| `verify(VerifyConfig)` | `verify(...)` | PARTIAL | stub per claim-audit |
| `getDatabaseName()` | `get_database_name()` | EQUIVALENT | |
| `getConfig()` | `get_config()` | EQUIVALENT | |
| `getEnvironment()` | (none) | MISSING | |
| `compareKeys(DatabaseEntry, DatabaseEntry)` | (internal only) | MISSING on public surface | |
| `compareDuplicates(DatabaseEntry, DatabaseEntry)` | (internal only) | MISSING on public surface | |
| (additional Noxu methods) | `scan_all_kv`, `is_valid`, `state` | EXTRA | |

### `Cursor` / `ForwardCursor`  →  `noxu_db::cursor::Cursor`

| JE method | Noxu method | Status | Notes |
|---|---|---|---|
| `getDatabase()` | (none) | MISSING | |
| `getConfig()` | (none) | MISSING | |
| `getCacheMode()` / `setCacheMode(CacheMode)` | (none) | MISSING | |
| `setRangeConstraint(RangeConstraint)` | (none) | MISSING | useful for prefix-scan limits |
| `close()` | `close()` | EQUIVALENT | |
| `dup(boolean samePosition)` | (none) | MISSING | cursor duplication absent |
| `delete(WriteOptions)` / `delete()` | `delete()` | EQUIVALENT | WriteOptions form absent |
| `put(DatabaseEntry, DatabaseEntry, Put, WriteOptions)` | `put(...)` | EQUIVALENT | |
| `putCurrent(DatabaseEntry)` | `put(Put::Current, …)` | RENAMED | |
| `get(DatabaseEntry, DatabaseEntry, Get, ReadOptions)` | `get(...)` | EQUIVALENT | |
| `getCurrent` / `getFirst` / `getLast` / `getNext` / `getNextDup` / `getNextNoDup` / `getPrev` / `getPrevDup` / `getPrevNoDup` | covered by `get(Get::*)` enum | RENAMED | |
| `getSearchKey` / `getSearchKeyRange` / `getSearchBoth` / `getSearchBothRange` | covered by `get(Get::*)` enum | RENAMED | |
| `skipNext(long maxCount, …)` | (none) | MISSING | |
| `skipPrev(long maxCount, …)` | (none) | MISSING | |
| `count()` | `count()` | EQUIVALENT | |
| `countEstimate()` | (none) | MISSING | |

### `Transaction`  →  `noxu_db::transaction::Transaction`

| JE method | Noxu method | Status | Notes |
|---|---|---|---|
| `abort()` | `abort()` | EQUIVALENT | |
| `getId()` | `get_id()` | EQUIVALENT | |
| `getCommitToken()` | (none) | MISSING | |
| `commit()` | `commit()` | EQUIVALENT | per claim-audit, error doc undercounts surface |
| `commit(Durability)` | `commit_with_durability(...)` | EQUIVALENT | |
| `commitSync()` | (use `commit_with_durability(SYNC)`) | RENAMED | |
| `commitNoSync()` | (use `commit_with_durability(NO_SYNC)`) | RENAMED | |
| `commitWriteNoSync()` | (use `commit_with_durability(WRITE_NO_SYNC)`) | RENAMED | |
| `getPrepared()` | (none on Transaction; lives in `noxu-xa`) | MISSING | |
| `getTxnTimeout(TimeUnit)` / `setTxnTimeout(...)` | `get_txn_timeout()` / `set_txn_timeout(u64)` | RENAMED | TimeUnit collapsed to ms |
| `getLockTimeout(TimeUnit)` / `setLockTimeout(...)` | `get_lock_timeout()` / `set_lock_timeout(u64)` | RENAMED | |
| `setName(String)` / `getName()` | (none) | MISSING | |
| `isValid()` | `is_valid()` | EQUIVALENT | |
| `getState()` | `get_state()` | EQUIVALENT | |
| (additional Noxu methods) | `with_log_manager`, `with_env_impl`, `with_inner_txn`, `get_inner_txn`, `is_read_only`, `elapsed`, `get_durability` | EXTRA | |

### `Sequence`  →  `noxu_db::sequence::Sequence`

| JE method | Noxu method | Status |
|---|---|---|
| `close()` | `close()` | EQUIVALENT |
| `get(Transaction, int delta)` | `get(Option<&Transaction>, i32)` | EQUIVALENT |
| `getDatabase()` | (none) | MISSING |
| `getKey()` | (none) | MISSING |
| `getStats(StatsConfig)` | `get_stats()` | EQUIVALENT |

### `SecondaryDatabase`  →  `noxu_db::secondary_database::SecondaryDatabase`

| JE method | Noxu method | Status |
|---|---|---|
| `close()` | `close()` | EQUIVALENT |
| `startIncrementalPopulation()` | `start_incremental_population()` | EQUIVALENT |
| `endIncrementalPopulation()` | `end_incremental_population()` | EQUIVALENT |
| `isIncrementalPopulationEnabled()` | `is_incremental_population_enabled()` | EQUIVALENT |
| `deleteObsoletePrimaryKeys(...)` | (none) | MISSING |
| `populateSecondaries(...)` | (none) | MISSING |
| `getPrimaryDatabase()` | (none) | MISSING |
| `getSecondaryConfig()` / `getConfig()` | `get_config()` | EQUIVALENT |
| `openSecondaryCursor` / `openCursor` | `open_cursor(...)` | EQUIVALENT |
| `delete(...)` | `delete(...)` | EQUIVALENT |
| `get(...)` / `getSearchBoth(...)` | `get(...)` | PRESENT-WITH-GAPS — getSearchBoth not exposed |
| `put(...)` / `putNoOverwrite` / `putNoDupData` | (only via primary; see `update_secondary`) | RENAMED — secondary writes are routed through the primary |
| `join(Cursor[], JoinConfig)` | (none on secondary; use Database::join) | MISSING on secondary |

### `SecondaryCursor`  →  `noxu_db::secondary_cursor::SecondaryCursor`

| JE method | Noxu method | Status |
|---|---|---|
| `getDatabase()` / `getPrimaryDatabase()` | (none) | MISSING |
| `dup(...)` / `dupSecondary(...)` | (none) | MISSING |
| `delete()` | `delete()` | EQUIVALENT |
| `put` / `putNoOverwrite` / `putNoDupData` / `putCurrent` | `put(...)` | EQUIVALENT (Put-enum form) |
| `getCurrent` / `getFirst` / `getLast` / `getNext` / `getPrev` | each present | EQUIVALENT |
| `getNextDup` / `getNextNoDup` / `getPrevDup` / `getPrevNoDup` | (none) | MISSING |
| `getSearchKey` / `getSearchKeyRange` | each present | EQUIVALENT |
| `getSearchBoth` / `getSearchBothRange` | (none) | MISSING |

### `JoinCursor`  →  `noxu_db::join_cursor::JoinCursor`

| JE method | Noxu method | Status |
|---|---|---|
| `close()` | `close()` (consumes self) | EQUIVALENT |
| `getDatabase()` | `get_database()` | EQUIVALENT |
| `getConfig()` | `get_config()` | EQUIVALENT |
| `get(Get, …)` / `getCurrent` / `getNext` | `get_next(...)` / `get_next_key(...)` | RENAMED — only NEXT semantics exposed |

### `DatabaseEntry`  →  `noxu_db::database_entry::DatabaseEntry`

| JE method | Noxu method | Status |
|---|---|---|
| `setData(byte[])` / `setData(byte[], int, int)` | `set_data` / `set_data_vec` / `set_data_bytes` | EQUIVALENT (richer in Rust) |
| `getData()` | `get_data()` / `data()` | EQUIVALENT |
| `setPartial(int doff, int dlen, boolean)` | `set_partial(...)` | EQUIVALENT |
| `getPartial()` / `getPartialOffset()` / `getPartialLength()` | `is_partial()` / `get_partial_offset()` / `get_partial_length()` | EQUIVALENT |
| `getOffset()` / `setOffset(int)` | `get_offset()` / `set_offset(...)` | EQUIVALENT |
| `getSize()` / `setSize(int)` | `get_size()` / `set_size(...)` | EQUIVALENT |
| `equals(Object)` / `hashCode()` | `Eq` + `Hash` derive | EQUIVALENT |
| `toString()` | `Debug` derive | EQUIVALENT |
| `MAX_DUMP_BYTES` constant | (none) | MISSING (debug-format only) |

### `Durability`  →  `noxu_db::durability::Durability`

| JE constant/method | Noxu equivalent | Status |
|---|---|---|
| `COMMIT_SYNC` / `COMMIT_NO_SYNC` / `COMMIT_WRITE_NO_SYNC` / `READ_ONLY_TXN` | `Durability::COMMIT_SYNC` / `COMMIT_NO_SYNC` / `COMMIT_WRITE_NO_SYNC` (no `READ_ONLY_TXN` constant) | EQUIVALENT (with gap) |
| `Durability(SyncPolicy, SyncPolicy, ReplicaAckPolicy)` | `Durability::new(local, replica, ack)` | EQUIVALENT |
| `parse(String)` | (none) | MISSING |
| `getLocalSync` / `getReplicaSync` / `getReplicaAck` | field-access | EQUIVALENT |
| `enum SyncPolicy { SYNC, NO_SYNC, WRITE_NO_SYNC }` | `SyncPolicy` enum | EQUIVALENT |
| `enum ReplicaAckPolicy { ALL, NONE, SIMPLE_MAJORITY }` | `ReplicaAckPolicy` enum | EQUIVALENT |
| `ReplicaAckPolicy.minAckNodes(int)` | `ReplicaAckPolicy::min_ack_nodes(u32)` | EQUIVALENT |

### Enums and option types

| JE | Noxu | Status |
|---|---|---|
| `LockMode` | `LockMode` | EQUIVALENT |
| `Get` | `Get` | EQUIVALENT |
| `Put` | `Put` | EQUIVALENT |
| `OperationStatus` | `OperationStatus` | EQUIVALENT |
| `OperationResult` | `OperationResult` | EQUIVALENT |
| `ReadOptions` | `ReadOptions` | EQUIVALENT |
| `WriteOptions` | `WriteOptions` | EQUIVALENT |
| `CacheMode` | `CacheMode` | EQUIVALENT |
| `RecoveryProgress` | (none — recovery does not emit progress events) | MISSING |
| `PreloadStatus` | (folded into `PreloadStats`) | RENAMED |
| `ForeignKeyDeleteAction` | `ForeignKeyDeleteAction` | EQUIVALENT |
| `JoinConfig` | `JoinConfig` | EQUIVALENT |

### `EnvironmentConfig` (selected)

JE has 201 `public ` declarations (most are paired
`setX(value)` / `setXVoid(value)` / `getX()` plus 100+ string parameter
constants). Noxu has 160 `pub fn`. The shape matches; the gaps are
~40 less-common parameters not re-exposed as fluent setters.

| JE setter | Noxu setter | Status |
|---|---|---|
| `setAllowCreate` | `with_allow_create` / `set_allow_create` | EQUIVALENT |
| `setTransactional` | `with_transactional` / `set_transactional` | EQUIVALENT |
| `setReadOnly` | `with_read_only` / `set_read_only` | EQUIVALENT |
| `setSharedCache` | `set_shared_cache` | EQUIVALENT (no `with_` form) |
| `setLockTimeout(long, TimeUnit)` | (via TransactionConfig) | RENAMED |
| `setTxnTimeout(long, TimeUnit)` | (via TransactionConfig) | RENAMED |
| `setCachePercent(int)` | `set_cache_percent` | EQUIVALENT |
| `setCacheSize(long)` | `set_cache_size` / `with_cache_size` | EQUIVALENT |
| `setMaxOffHeapMemorySize(long)` | `set_max_off_heap_memory` | RENAMED (shorter) |
| `setMaxDisk(long)` | `set_max_disk` | EQUIVALENT |
| `setFreeDisk(long)` | `set_free_disk` | EQUIVALENT |
| `setRunCheckpointer/Cleaner/Evictor/Verifier(boolean)` | each present | EQUIVALENT |
| `setRunINCompressor(boolean)` | `set_run_in_compressor` | EQUIVALENT |
| `setBackgroundReadLimit(int)` / `setBackgroundWriteLimit(int)` / `setBackgroundSleepInterval(long)` | each present (suffixed `_kb` / `_us`) | EQUIVALENT |
| `setExceptionListener(ExceptionListener)` | (`ExceptionListenerHolder` re-exported) | EQUIVALENT |
| `setLoggingHandler(Handler)` | (none) | MISSING — Java logging-specific |
| `setRecoveryProgressListener(ProgressListener)` | (none) | MISSING |
| `setNodeName(String)` | (rep-specific; in RepConfig) | MISSING on EnvironmentConfig |
| `setLoadPropertyFile(boolean)` / `setConfigParam(String, String)` | (none) | MISSING — string-keyed config not idiomatic in Rust |

(40+ further parameters are ports of internal `EnvironmentParams`
strings that are not re-exposed as `set_*` methods. These can be
considered DELIBERATELY-OMITTED until a user requests them.)

### Stats

| JE class | Noxu equivalent | Status |
|---|---|---|
| `EnvironmentStats` | `noxu_engine::EnvironmentStats` | PRESENT-WITH-GAPS — JE's class has ~80 `getX()` methods; Noxu's has a subset |
| `DatabaseStats` (abstract) | `noxu_db::database_stats::DatabaseStats` | EQUIVALENT |
| `BtreeStats` | `noxu_db::database_stats::BtreeStats` | EQUIVALENT |
| `LockStats` | (none — folded into env stats) | MISSING |
| `TransactionStats` | (none — folded into env stats) | MISSING |
| `PreloadStats` | `PreloadStats` | EQUIVALENT |
| `SequenceStats` | `SequenceStats` | EQUIVALENT |
| `StatsConfig` | `StatsConfig` | EQUIVALENT |

### Exception types (JE → Noxu)

JE defines ~30 exception types under `com.sleepycat.je`. Noxu collapses
them into one `NoxuError` enum with corresponding variants. Behaviour
is equivalent under variant pattern-match; LOW-severity granularity gap.

| JE exception | NoxuError variant | Status |
|---|---|---|
| `DatabaseException` (base) | `NoxuError` (root) | EQUIVALENT |
| `DatabaseExistsException` | `DatabaseAlreadyExists` | EQUIVALENT |
| `DatabaseNotFoundException` | `DatabaseNotFound` | EQUIVALENT |
| `DeadlockException` | `Deadlock` | EQUIVALENT |
| `DiskLimitException` | `DiskLimit` (or `DiskFull`) | EQUIVALENT |
| `DuplicateDataException` | `DuplicateData` | EQUIVALENT |
| `EnvironmentFailureException` | `EnvironmentFailure(EnvironmentFailureReason)` | EQUIVALENT |
| `EnvironmentLockedException` | `EnvironmentLocked` | EQUIVALENT |
| `EnvironmentNotFoundException` | `EnvironmentNotFound` | EQUIVALENT |
| `EnvironmentWedgedException` | (none) | MISSING — wedge state not modelled |
| `LockConflictException` | `LockConflict` | EQUIVALENT |
| `LockNotAvailableException` / `LockNotGrantedException` | `LockNotAvailable` | EQUIVALENT |
| `LockTimeoutException` | `LockTimeout` | EQUIVALENT |
| `LogWriteException` | inside `EnvironmentFailure(LogWrite)` | RENAMED |
| `OperationFailureException` (base) | `OperationFailure` | EQUIVALENT |
| `RunRecoveryException` | `RunRecovery` | EQUIVALENT |
| `ThreadInterruptedException` | (none — Rust threads don't carry this) | DELIBERATELY-OMITTED |
| `TransactionTimeoutException` | `TransactionTimeout` | EQUIVALENT |
| `VersionMismatchException` | `VersionMismatch` | EQUIVALENT |
| `SecondaryConstraintException` | `SecondaryConstraint` | EQUIVALENT |
| `SecondaryIntegrityException` | `SecondaryIntegrity` | EQUIVALENT |
| `SecondaryReferenceException` | `SecondaryReference` | EQUIVALENT |
| `ForeignConstraintException` | `ForeignConstraint` | EQUIVALENT |
| `DeleteConstraintException` | `DeleteConstraint` | EQUIVALENT |
| `UniqueConstraintException` | `UniqueConstraint` | EQUIVALENT |
| `SequenceExistsException` / `SequenceNotFoundException` / `SequenceIntegrityException` / `SequenceOverflowException` | each present as variant | EQUIVALENT |
| `XAFailureException` | (none on `NoxuError`; in `noxu-xa`) | RENAMED |

### `XAEnvironment`

| JE | Noxu | Status |
|---|---|---|
| `XAEnvironment` extends `Environment`, implements `XAResource` | `noxu_xa::Environment` (separate crate, NOT integrated with `noxu-db::Environment`) | PARTIAL — XA implemented standalone but not exposed via `Environment` |
| `start(Xid, int)` / `end(...)` / `prepare(...)` / `commit(...)` / `rollback(...)` | each present in `noxu-xa` | EQUIVALENT |

---

## com.sleepycat.je.rep

### `ReplicatedEnvironment`  →  `noxu_rep::ReplicatedEnvironment`

| JE method | Noxu method | Status |
|---|---|---|
| Constructor `(File envHome, ReplicationConfig, EnvironmentConfig)` | `new(RepConfig)` | RENAMED + GAPS — env home/config plumbing different |
| `getNodeName()` | `get_node_name()` | EQUIVALENT |
| `getState()` | `get_state()` | EQUIVALENT |
| `getGroup()` | `get_rep_group()` | RENAMED |
| `setStateChangeListener(StateChangeListener)` | `set_state_change_listener(...)` | EQUIVALENT |
| `getStateChangeListener()` | (none) | MISSING |
| `setRepMutableConfig(...)` / `getRepMutableConfig()` | (none) | MISSING |
| `getRepConfig()` | `get_config()` | EQUIVALENT |
| `getRepStats(StatsConfig)` | `get_stats()` | RENAMED |
| `printStartupInfo(PrintStream)` | (none) | MISSING |
| `shutdownGroup(long, TimeUnit)` | `shutdown_group(...)` | PARTIAL — per claim-audit, no wait-for-replicas / catch-up / replica-shutdown |
| `registerAppStateMonitor(AppStateMonitor)` | (none) | MISSING |
| `transferMaster(Set<String>, long, TimeUnit)` | `transfer_master(MasterTransferConfig)` | PARTIAL — per claim-audit, body only logs intent |
| (extra Noxu methods) | `bound_addr`, `with_environment`, `is_master`, `is_replica`, `is_active`, `add_peer`, `remove_peer`, `update_peer_metadata`, `get_vlsn_range`, `get_current_vlsn`, `register_vlsn`, `apply_entry`, `record_ack`, `become_master`, `become_replica`, `ensure_unknown_state`, `is_shutdown` | EXTRA |

### `ReplicationConfig`  →  `noxu_rep::RepConfig`

JE exposes ~80 string-keyed parameters with paired setters; Noxu's
`RepConfig` exposes ~24 fluent `with_*` setters. Major gaps:

| JE parameter (string key) | Noxu fluent setter | Status |
|---|---|---|
| `GROUP_NAME` / `NODE_NAME` / `NODE_TYPE` / `HELPER_HOSTS` | builder args / `node_type` / `helper_hosts` | EQUIVALENT |
| `DEFAULT_PORT` / `NODE_HOST_PORT` | `node_port` | EQUIVALENT |
| `BIND_INADDR_ANY` | (none) | MISSING |
| `CONSISTENCY_POLICY` | `consistency_policy` | EQUIVALENT |
| `REP_STREAM_TIMEOUT` / `REPLICA_ACK_TIMEOUT` / `FEEDER_TIMEOUT` | `feeder_timeout` / `replica_ack_timeout` | EQUIVALENT |
| `REPLAY_*` family (cost percent, free disk percent, txn lock timeout, max open db handles, db handle timeout) | (none) | MISSING |
| `ENV_CONSISTENCY_TIMEOUT` | (none) | MISSING |
| `ARBITER_ACK_TIMEOUT` | (none) | MISSING — arbiter not implemented |
| `INSUFFICIENT_REPLICAS_TIMEOUT` | (none) | MISSING |
| `MAX_MESSAGE_SIZE` | (none) | MISSING |
| `MAX_CLOCK_DELTA` | (none) | MISSING |
| (20+ more SSL/network/heartbeat/batch tuning parameters) | partial | PRESENT-WITH-GAPS |

### `ReplicationGroup` / `ReplicationNode`

| JE | Noxu | Status |
|---|---|---|
| `ReplicationGroup.getName()` | `RepGroup::name` | EQUIVALENT |
| `ReplicationGroup.getMember(String)` | `RepGroup::get_node` | RENAMED |
| `ReplicationGroup.getElectableNodes()` / `getMonitorNodes()` / `getDataNodes()` / `getNodes()` | `nodes`, `electable_nodes`, etc. | EQUIVALENT |
| `ReplicationNode.getName()` / `getType()` / `getSocketAddress()` / `getHostName()` / `getPort()` | each present | EQUIVALENT |

### `NetworkRestore` / `NetworkRestoreConfig`

| JE | Noxu | Status |
|---|---|---|
| `execute(InsufficientLogException, NetworkRestoreConfig)` | `execute()` | RENAMED |
| `getBackup()` | (none) | MISSING |
| `getLogProvider()` | (none) | MISSING |
| `getNetworkBackupStats()` | `get_progress()` | RENAMED |
| `setRetainLogFiles` / `setReceiveBufferSize` | (in NetworkRestoreConfig) | EQUIVALENT |

### `StateChangeListener` / `StateChangeEvent`

| JE | Noxu | Status |
|---|---|---|
| `StateChangeListener.stateChange(StateChangeEvent)` | `StateChangeListener::on_state_change(StateChangeEvent)` (trait) | EQUIVALENT |
| `StateChangeEvent.getEventTime()` | `event_time()` | EQUIVALENT |
| `StateChangeEvent.getMasterNodeName()` | `master_name()` | RENAMED |

### `NodeType`

| JE variant | Noxu variant | Status |
|---|---|---|
| `ELECTABLE` | `Electable` | EQUIVALENT |
| `MONITOR` | `Monitor` | PRESENT-WITH-GAPS — variant exists, monitor-node behaviour not implemented |
| `SECONDARY` | `Secondary` | PRESENT-WITH-GAPS — variant exists, secondary-node behaviour not fully implemented |
| `EXTERNAL` | `External` | PRESENT-WITH-GAPS — variant exists, external-node feeder integration partial |
| `ARBITER` | `Arbiter` | PRESENT-WITH-GAPS — variant exists, arbiter behaviour not implemented |

### `QuorumPolicy`

| JE | Noxu | Status |
|---|---|---|
| `ALL` / `SIMPLE_MAJORITY` / `NONE` | each present | EQUIVALENT |
| (extras) `Flexible(phase1, phase2)`, `Custom(quoracle expression)` | extras | EXTRA — Noxu adds Flexible Paxos and quoracle-expression policies absent in JE |

### `CommitPointConsistencyPolicy` / `TimeConsistencyPolicy` / `NoConsistencyRequiredPolicy`

| JE class | Noxu | Status |
|---|---|---|
| three separate classes implementing `ReplicaConsistencyPolicy` | `ConsistencyPolicy` enum with corresponding variants | RENAMED |

### Replication exceptions

| JE | Noxu | Status |
|---|---|---|
| `RollbackException` / `RollbackProhibitedException` | `RepError::Rollback*` variants + `NoxuError::RollbackRequired` | PARTIAL — fewer error subtypes |
| `MasterReplicaTransitionException` | `RepError::MasterReplicaTransition` | EQUIVALENT |
| `MasterStateException` / `ReplicaStateException` | `RepError::Master/ReplicaStateError` | EQUIVALENT |
| `MasterTransferFailureException` | `RepError::MasterTransferFailed` | EQUIVALENT |
| `MemberActiveException` / `MemberNotFoundException` | `RepError::MemberActive/NotFound` | EQUIVALENT |
| `InsufficientAcksException` / `InsufficientReplicasException` | `NoxuError::InsufficientReplicas` | EQUIVALENT |
| `InsufficientLogException` | `RepError::InsufficientLog` | EQUIVALENT |
| `LockPreemptedException` / `DatabasePreemptedException` | (none) | MISSING |
| `LogFileRewriteListener` / `LogOverwriteException` | (none) | MISSING |
| `ReplicaWriteException` | `NoxuError::ReplicaWrite` | EQUIVALENT |
| `ReplicaConsistencyException` | `RepError::ReplicaConsistency` | EQUIVALENT |
| `RestartRequiredException` | `RepError::RestartRequired` | EQUIVALENT |
| `UnknownMasterException` | `RepError::UnknownMaster` | EQUIVALENT |
| `GroupShutdownException` | `RepError::GroupShutdown` | EQUIVALENT |
| `ReplicationSecurityException` | (TLS errors propagate as `RepError::Tls`) | RENAMED |

### Other rep classes

| JE | Noxu | Status |
|---|---|---|
| `Monitor` (separate node type with its own JAR) | (none) | MISSING |
| `Arbiter` (separate node type) | (none) | MISSING |
| `AppStateMonitor` | (none) | MISSING |
| `RepStatManager` | (none) | MISSING |
| `RepInternal` (test-access shim) | (none) | DELIBERATELY-OMITTED |
| `ReplicationBasicConfig` / `ReplicationSSLConfig` / `ReplicationNetworkConfig` | merged into `RepConfig` + `TlsConfig` | RENAMED |
| `RecoveryProgress` enum (rep-specific) | (none) | MISSING |
| `SyncupProgress` enum | (none) | MISSING |

---

## com.sleepycat.bind

| JE class | Noxu | Status |
|---|---|---|
| `EntryBinding<E>` (interface) | `noxu_bind::EntryBinding` (trait) | EQUIVALENT |
| `EntityBinding<E>` (interface) | `noxu_bind::EntityBinding` (trait) | EQUIVALENT |
| `ByteArrayBinding` | `ByteArrayBinding` | EQUIVALENT |
| `RecordNumberBinding` | `RecordNumberBinding` | EQUIVALENT |

## com.sleepycat.bind.tuple

| JE class | Noxu | Status |
|---|---|---|
| `TupleBase` (abstract) | (folded into `TupleOutput`/`TupleInput`) | RENAMED |
| `TupleInput` | `TupleInput` | EQUIVALENT |
| `TupleOutput` | `TupleOutput` | EQUIVALENT |
| `TupleBinding<E>` (abstract) | `TupleBinding<T>` (trait) | EQUIVALENT |
| `TupleInputBinding` | (use `TupleInput::from_*`) | RENAMED |
| `TupleMarshalledBinding` / `TupleTupleBinding` / `TupleTupleMarshalledBinding` / `TupleTupleKeyCreator` / `TupleTupleMarshalledKeyCreator` / `MarshalledTupleEntry` / `MarshalledTupleKeyEntity` | (none) | DELIBERATELY-OMITTED — Java `MarshalledKey` pattern; Rust uses `serde` via `TupleSerdeBinding` |
| Primitive bindings: `BooleanBinding` / `ByteBinding` / `CharacterBinding` / `ShortBinding` / `IntegerBinding` / `LongBinding` / `FloatBinding` / `DoubleBinding` / `StringBinding` | each present (`BoolBinding`, `ByteBinding`, `CharBinding`, `ShortBinding`, `IntBinding`, `LongBinding`, `FloatBinding`, `DoubleBinding`, `StringBinding`) | EQUIVALENT |
| `SortedFloatBinding` / `SortedDoubleBinding` | each present | EQUIVALENT |
| `PackedIntegerBinding` / `PackedLongBinding` | `PackedIntBinding` / `PackedLongBinding` | EQUIVALENT |
| `SortedPackedIntegerBinding` / `SortedPackedLongBinding` | `SortedPackedIntBinding` / `SortedPackedLongBinding` | EQUIVALENT |
| `BigIntegerBinding` / `BigDecimalBinding` / `SortedBigDecimalBinding` | (none) | MISSING — no big-number bindings |

## com.sleepycat.bind.serial

| JE class | Noxu | Status |
|---|---|---|
| `SerialBinding<E>` (abstract; uses `ObjectInput`/`ObjectOutput`) | `SerdeBinding<T>` (uses `serde`) | RENAMED — different serialization mechanism, equivalent role |
| `SerialBase` / `SerialInput` / `SerialOutput` | (Rust uses serde derives directly) | DELIBERATELY-OMITTED |
| `ClassCatalog` (interface) | (none — `serde` carries no per-class metadata) | DELIBERATELY-OMITTED |
| `StoredClassCatalog` | (none) | DELIBERATELY-OMITTED |
| `SerialSerialBinding` / `SerialSerialKeyCreator` / `TupleSerialBinding` / `TupleSerialKeyCreator` / `TupleSerialMarshalledBinding` / `TupleSerialMarshalledKeyCreator` | `TupleSerdeBinding<K, V>` / `TupleSerdeKeyDataBinding<K, V>` | RENAMED — fewer permutations; serde covers them |

---

## com.sleepycat.collections

| JE class | Noxu | Status |
|---|---|---|
| `StoredMap<K, V>` | `StoredMap<'db>` (byte-slice keyed) | EQUIVALENT (with caveat: typed keys not yet exposed at this layer; users go through `EntryBinding` themselves) |
| `StoredSortedMap<K, V>` | `StoredSortedMap<'db>` | PRESENT-WITH-GAPS — `firstKey`/`lastKey`/`first_entry`/`last_entry`/`iter_from`/`iter_reverse` present; missing `headMap`/`tailMap`/`subMap` |
| `StoredKeySet<K>` / `StoredSortedKeySet<K>` | `StoredKeySet` (single type) | PRESENT-WITH-GAPS — sorted variant folded in |
| `StoredValueSet<V>` / `StoredSortedValueSet<V>` | `StoredValueSet` (single type) | PRESENT-WITH-GAPS |
| `StoredEntrySet<K, V>` / `StoredSortedEntrySet<K, V>` | (covered via `StoredMap::iter`) | RENAMED |
| `StoredList<E>` | `StoredList<'db>` | EQUIVALENT (subset of List ops) |
| `StoredCollection` / `StoredContainer` (abstract bases) | (Rust traits/impls inline) | RENAMED |
| `StoredIterator` / `BlockIterator` / `BaseIterator` | `StoredIterator`, `StoredKeyIterator`, `StoredValueIterator` | RENAMED |
| `MyRangeCursor` / `DataCursor` / `DataView` | (internal; subsumed by `Cursor`) | DELIBERATELY-OMITTED |
| `MapEntryParameter` / `StoredMapEntry` | (Rust returns `(Vec<u8>, Vec<u8>)` tuples) | RENAMED |
| `TransactionRunner` | `TransactionRunner` | EQUIVALENT |
| `TransactionWorker` (interface) | closure parameter to `TransactionRunner::run` | RENAMED |
| `CurrentTransaction` | (none — thread-local pattern; not idiomatic in Rust) | DELIBERATELY-OMITTED |
| `PrimaryKeyAssigner` | (none) | MISSING |
| `StoredCollections` (utility class) | (none) | MISSING |
| `TupleSerialFactory` | (none) | MISSING |

### `StoredMap` selected method mapping

| JE method | Noxu method | Status |
|---|---|---|
| `get(Object key)` | `get(&[u8])` | EQUIVALENT |
| `put(K, V)` | `put(&[u8], &[u8])` | EQUIVALENT |
| `append(V)` | (StoredList only) | RENAMED |
| `remove(Object)` | `remove(&[u8])` | EQUIVALENT |
| `putIfAbsent(K, V)` / `replace(...)` / `remove(K, V)` | (none — atomic compare-and-swap not exposed) | MISSING |
| `containsKey` / `containsValue` | `contains_key` / (none) | PRESENT-WITH-GAPS |
| `putAll(Map)` | (none — iterate explicitly) | MISSING |
| `size()` | `len()` | EQUIVALENT |

### `StoredList` selected method mapping

| JE method | Noxu method | Status |
|---|---|---|
| `add(int, E)` / `add(E)` / `append(E)` | `push(value)` | PARTIAL — only append at tail |
| `addAll(int, Collection)` | (none) | MISSING |
| `contains(Object)` | (none) | MISSING |
| `get(int)` | `get(usize)` | EQUIVALENT |
| `indexOf(Object)` / `lastIndexOf(Object)` | (none) | MISSING |
| `remove(int)` / `remove(Object)` | `remove(usize)` (by index only) | PRESENT-WITH-GAPS |
| `set(int, E)` | (none) | MISSING |

---

## com.sleepycat.persist (DPL)

### `EntityStore`

| JE method | Noxu method | Status |
|---|---|---|
| `getEnvironment()` | `get_environment()` | EQUIVALENT |
| `getConfig()` | `get_config()` | EQUIVALENT |
| `getStoreName()` | `get_store_name()` | EQUIVALENT |
| static `getStoreNames(Environment)` | (none) | MISSING |
| `isReplicaUpgradeMode()` | (none) | MISSING |
| `getModel()` | (none — `EntityModel` not ported) | MISSING |
| `getMutations()` | (config-only) | RENAMED |
| `evolve(EvolveConfig)` | `evolve(...)` | PARTIAL — types exist; open-path not yet wired to apply mutations |
| `truncateClass(Class)` / `truncateClass(Transaction, Class)` | (none) | MISSING |
| `sync()` | (none) | MISSING |
| `closeClass(Class)` | (none) | MISSING |
| `close()` | `close()` | EQUIVALENT |
| `getSequence(String)` / `getSequenceConfig(String)` / `setSequenceConfig(...)` | (use `noxu-persist::Sequence` directly) | RENAMED |
| `getPrimaryConfig(Class)` / `setPrimaryConfig(...)` | (config supplied at `get_primary_index` time) | RENAMED |
| `getSecondaryConfig(Class, …)` / `setSecondaryConfig(...)` | (config supplied at `open_secondary_index` time) | RENAMED |
| `getPrimaryIndex(Class<PK>, Class<E>)` | `get_primary_index<K, E>()` (typed) | EQUIVALENT |
| `getSecondaryIndex(PrimaryIndex, Class<SK>, String)` | `open_secondary_index<...>` | EQUIVALENT |
| `getSubclassIndex(...)` | (none) | MISSING |
| `getRawCatalog()` / `getRawStore()` | (none — RawStore not ported) | MISSING |

### `PrimaryIndex<PK, E>`

| JE method | Noxu method | Status |
|---|---|---|
| `put(E)` / `put(Transaction, E)` | `put(...)` | EQUIVALENT |
| `putNoReturn(...)` | (use `put` and ignore return) | RENAMED |
| `putNoOverwrite(...)` | `put_no_overwrite(...)` | EQUIVALENT |
| `put(Transaction, E, ...)` returning `OperationResult` | `put(...)` | EQUIVALENT |
| `get(PK)` / `get(Transaction, PK, LockMode)` | `get(...)` | EQUIVALENT |
| `delete(PK)` / `delete(Transaction, PK)` | `delete(...)` / `delete_with_entity(...)` | EQUIVALENT |
| `contains(PK)` | `contains(...)` | EQUIVALENT |
| `count()` | `count()` | EQUIVALENT |
| `entities(...)` (cursor) | `entities<S>(...)` (Iterator) | RENAMED |
| `keys(...)` | `keys()` | EQUIVALENT |
| `sortedMap()` | (none — go via StoredSortedMap) | MISSING |
| `getDatabase()` | `database()` | EQUIVALENT |

### `SecondaryIndex<SK, PK, E>`

| JE method | Noxu method | Status |
|---|---|---|
| `getDatabase()` / `getKeysDatabase()` | (none — internal) | MISSING |
| `keysIndex()` | `keys_index()` | RENAMED |
| `subIndex(SK)` | `sub_index(&SK)` | EQUIVALENT |
| `get(SK)` | `get(...)` | EQUIVALENT |
| `delete(SK)` | `delete(...)` | EQUIVALENT |
| `contains(SK)` | `contains(...)` | EQUIVALENT |
| `entities(...)` (cursor) | `iter<S>(...)` / `iter_from(...)` | RENAMED |

### `EntityCursor` / `ForwardCursor` (DPL)

| JE | Noxu | Status |
|---|---|---|
| `EntityCursor<E>` interface (next/prev/first/last/dup/delete/update) | `EntityIterator` / `KeyIterator` (next-only Rust Iterator) | RENAMED — only forward iteration |

### `StoreConfig`

| JE setter | Noxu | Status |
|---|---|---|
| `setAllowCreate` / `setExclusiveCreate` / `setReadOnly` / `setTransactional` / `setSecondaryBulkLoad` / `setDeferredWrite` | each present | EQUIVALENT |
| `setMutations(Mutations)` | `with_mutations(...)` | EQUIVALENT |
| `setModel(EntityModel)` | (none) | MISSING |
| `setDatabaseNamer(DatabaseNamer)` | `with_database_namer(...)` | EQUIVALENT |

### Annotations / model

| JE annotation/class | Noxu | Status |
|---|---|---|
| `@Entity` | `Entity` trait (manual impl) | RENAMED — no proc-macro derive yet |
| `@Persistent` | (none — implicit) | DELIBERATELY-OMITTED |
| `@PrimaryKey` | `PrimaryKey` trait | RENAMED |
| `@SecondaryKey` (with `relate`, `relatedEntity`, `onRelatedEntityDelete`) | (manual `KeyCreator` closures) | PARTIAL — declarative form not present |
| `@KeyField` | (none) | DELIBERATELY-OMITTED — composite key encoding handled by `serde`+tuple bindings |
| `@NotPersistent` / `@NotTransient` | (none) | DELIBERATELY-OMITTED |
| `Relationship.{ONE_TO_ONE, ONE_TO_MANY, MANY_TO_ONE, MANY_TO_MANY}` | (modelled via secondary-index multiplicity in user code) | PARTIAL |
| `DeleteAction.{ABORT, CASCADE, NULLIFY}` | `ForeignKeyDeleteAction` enum (in `noxu-db::secondary_config`) | EQUIVALENT |
| `EntityModel` (abstract) / `AnnotationModel` / `BytecodeEnhancer` / `ClassEnhancer` / `ClassEnhancerTask` | (none) | DELIBERATELY-OMITTED — Java bytecode rewriter |
| `PersistentProxy<T>` | (none) | MISSING |
| `ClassMetadata` / `EntityMetadata` / `FieldMetadata` / `PrimaryKeyMetadata` / `SecondaryKeyMetadata` | (none — runtime introspection absent) | DELIBERATELY-OMITTED |

### Raw access

| JE class | Noxu | Status |
|---|---|---|
| `RawStore` | (none) | MISSING |
| `RawObject` | (none) | MISSING |
| `RawType` | (none) | MISSING |
| `RawField` | (none) | MISSING |

The entire raw-access path is absent. Justification: Java's raw access
exists primarily for schema-evolution tooling that walks objects
without compile-time class knowledge; in Rust, schema evolution can
be implemented via per-version `serde` deserialization, removing the
need for a runtime reflection API.

### Schema evolution (`com.sleepycat.persist.evolve`)

| JE class | Noxu type | Status |
|---|---|---|
| `Mutations` | `noxu_persist::evolve::Mutations` | PARTIAL — type exists; not consulted at open-path |
| `Mutation` | `noxu_persist::evolve::MutationKey` | EQUIVALENT |
| `Renamer` | `Renamer` | EQUIVALENT |
| `Deleter` | `Deleter` | EQUIVALENT |
| `Converter` | `Converter` | EQUIVALENT |
| `EntityConverter` | (none) | MISSING |
| `Conversion` (interface) | `ConversionFn` (trait) | RENAMED |
| `EvolveConfig` | `EvolveConfig` | EQUIVALENT |
| `EvolveListener` (interface) | `EvolveListener` (trait) | EQUIVALENT |
| `EvolveEvent` | (folded into listener method args) | RENAMED |
| `EvolveStats` | `EvolveStats` | EQUIVALENT |
| `EvolveInternal` (test-access shim) | (none) | DELIBERATELY-OMITTED |
| `IncompatibleClassException` | (none) | MISSING |
| `DeletedClassException` | (none) | MISSING |

The shape is preserved but the open-path of `EntityStore` does not
yet apply `Mutations` to existing data. Per AGENTS.md and
`audit-report.md`, schema evolution is a known gap.

---

## com.sleepycat.je.tree (internal, included for completeness)

The JE `tree` package is internal-impl, but several classes have
direct Noxu counterparts. They are NOT user-visible API but ARE the
backbone of the data path. **Status legend here is "ported as
internal"**.

| JE class | Noxu | Status |
|---|---|---|
| `IN` (Internal Node) | `noxu_tree::in_node::InNode` | EQUIVALENT (internal) |
| `BIN` (Bottom Internal Node) | `noxu_tree::bin::Bin` | EQUIVALENT (internal) |
| `BINBoundary` | `noxu_tree::bin_boundary::BinBoundary` | EQUIVALENT |
| `BINDeltaBloomFilter` | `noxu_tree::bin_delta_bloom_filter::BinDeltaBloomFilter` | EQUIVALENT |
| `BINReference` | `noxu_tree::bin_reference::BinReference` | EQUIVALENT |
| `ChildReference` | `noxu_tree::child_reference::ChildReference` | EQUIVALENT |
| `LN` | `noxu_tree::ln::Ln` | EQUIVALENT |
| `MapLN` | `noxu_tree::map_ln::MapLn` | EQUIVALENT |
| `NameLN` | `noxu_tree::name_ln::NameLn` | EQUIVALENT |
| `FileSummaryLN` | `noxu_tree::file_summary_ln::FileSummaryLn` | EQUIVALENT |
| `Tree` | `noxu_tree::tree::Tree` | PARTIAL — split/merge present, INCompressor not present |
| `TreeLocation` / `SearchResult` / `TreeUtils` / `TrackingInfo` | each present | EQUIVALENT |
| `Key` | `noxu_tree::key::Key` | EQUIVALENT |
| `INKeyRep` / `INTargetRep` / `INLongRep` / `INArrayRep` | (folded into `InNode` field representations) | RENAMED |
| `OldBINDelta` / `DeltaInfo` | `noxu_tree::delta_info::DeltaInfo` | EQUIVALENT |
| `CountEstimator` | (none — not implemented; matches `Cursor::countEstimate` gap) | MISSING |
| `VersionedLN` | `noxu_tree::versioned_ln::VersionedLn` | EQUIVALENT |
| `WithRootLatched` (interface) | (Rust closure pattern) | RENAMED |
| `EntryStates` | `noxu_tree::entry_states::EntryStates` | EQUIVALENT |
| `StorageSize` | `noxu_tree::storage_size::StorageSize` | EQUIVALENT |
| `Node` (abstract base) | `noxu_tree::node::Node` (trait) | EQUIVALENT |
| `SplitRequiredException` / `CursorsExistException` / `NodeNotEmptyException` | (typed errors in `tree::error`) | RENAMED |
| `TreeWalkerStatsAccumulator` | (none) | MISSING |

---

## Summary counts

| Package | Total JE classes | EQUIVALENT | RENAMED | PRESENT-WITH-GAPS | MISSING | DELIBERATELY-OMITTED | PARTIAL |
|---|---:|---:|---:|---:|---:|---:|---:|
| `com.sleepycat.je` (public) | ~95 | ~28 | ~10 | ~22 | ~18 | ~5 | ~3 |
| `com.sleepycat.je.rep` | ~50 | ~18 | ~8 | ~10 | ~10 | ~2 | ~3 |
| `com.sleepycat.bind*` | ~30 | ~14 | ~4 | 0 | ~3 | ~9 | 0 |
| `com.sleepycat.collections` | ~22 | ~7 | ~5 | ~3 | ~4 | ~3 | 0 |
| `com.sleepycat.persist*` | ~55 | ~12 | ~9 | ~3 | ~14 | ~14 | ~3 |
| `com.sleepycat.je.tree` (internal) | ~30 | ~22 | ~5 | ~1 | ~2 | 0 | ~1 |

(Counts are approximate and include the BeanInfo / Internal accessor
classes which are uniformly DELIBERATELY-OMITTED.)
