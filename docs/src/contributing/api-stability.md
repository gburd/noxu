# API Stability and the v3.0 Public Surface

This document enumerates every public item in each Noxu DB crate that is
committed to **no breaking changes** in minor (e.g. v3.1.0) and patch
(e.g. v3.0.1) releases starting with v3.0.0.

Items are sorted by category: structs, enums, traits, functions/methods,
type aliases, and constants.

See [`semver-policy.md`](semver-policy.md) for what "breaking" means and
how the CI gate enforces the commitment.

---

## Compatibility tiers

| Tier | Crates | Commitment |
|------|--------|-----------|
| **Stable** | `noxu-db`, `noxu-bind`, `noxu-collections`, `noxu-persist`, `noxu-xa`, `noxu-rep` | No breaking change in a minor or patch release from v3.0.0 onward. |
| **Stable (foundational)** | `noxu-util`, `noxu-config` | Stable from v3.0.0 for the items listed; the full `params` catalogue is stable for *names and defaults* only — new params may be added (additive). |
| **Internal** | `noxu-engine`, `noxu-dbi`, `noxu-tree`, `noxu-txn`, `noxu-evictor`, `noxu-cleaner`, `noxu-recovery`, `noxu-log`, `noxu-latch`, `noxu-sync`, `noxu-observe`, `noxu-spec`, `noxu-persist-derive` | These crates are engine internals or tooling. Their public Rust APIs may change in any release. `noxu-persist-derive` is a proc-macro support crate accessed only through `noxu-persist`; its standalone API is internal. |

> **Note — `noxu-engine` items re-exported by `noxu-db`**: Four items from
> `noxu-engine` (`EnvironmentStats`, `VerifyConfig`, `VerifyError`,
> `VerifyResult`) are re-exported at the `noxu-db` crate root.  Because they
> are part of `noxu-db`'s stable surface they are stable; the rest of
> `noxu-engine`'s pub API is not.

---

## `noxu-db` — public API

Primary entry point for all embedded-database users.  All items listed
here are committed to v3.0+ stability.

### Structs

| Item | Description |
|------|-------------|
| `CheckpointConfig` | Parameters for an explicit checkpoint. |
| `Cursor` | B-tree cursor for sequential / positioned access. |
| `CursorConfig` | Options for opening a cursor. |
| `Database` | A named database within an environment. |
| `DatabaseConfig` | Configuration for opening or creating a database. |
| `DatabaseEntry` | A key or data value (wraps `Vec<u8>` or a slice). |
| `BtreeStats` | Per-database B-tree statistics. |
| `DatabaseStats` | Combined statistics for a database. |
| `DiskOrderedCursor` | Cursor that iterates in log-file (disk) order for bulk scans. |
| `DiskOrderedCursorConfig` | Options for `DiskOrderedCursor`. |
| `Durability` | Transaction durability policy (sync/write-no-sync/no-sync). |
| `Environment` | The outermost handle: manages files, cache, and daemons. |
| `EnvironmentConfig` | Full configuration for `Environment::open`. |
| `EnvironmentMutableConfig` | Subset of configuration that can be changed after open. |
| `EnvironmentStats` | Environment-level statistics (cache, log, lock, txn). |
| `ExceptionListenerHolder` | Wrapper for an optional `Arc<dyn ExceptionListener>`. |
| `JoinConfig` | Configuration for a join cursor. |
| `JoinCursor` | Cursor that joins multiple secondary cursors. |
| `OperationResult` | Result of a database operation including the key/data returned. |
| `PreloadConfig` | Options for `Database::preload`. |
| `PreloadStats` | Statistics returned by `Database::preload`. |
| `ReadOptions` | Per-call read options (e.g. lock mode). |
| `SecondaryConfig` | Configuration for a secondary (index) database. |
| `SecondaryCursor` | Cursor on a secondary database. |
| `SecondaryDatabase` | A secondary (index) database. |
| `Sequence` | An integer sequence stored in the database. |
| `SequenceConfig` | Configuration for a sequence. |
| `SequenceStats` | Statistics for a sequence handle. |
| `StatsConfig` | Options controlling which statistics are collected. |
| `Transaction` | A transaction handle. |
| `TransactionConfig` | Options for starting a transaction. |
| `VerifyConfig` | Options for `Environment::verify` / `Database::verify`. |
| `VerifyResult` | Result of a verify operation. |
| `WriteOptions` | Per-call write options. |

### Enums

| Item | Description |
|------|-------------|
| `CacheMode` | Cache priority for a database entry (`EVICT_LN`, `KEEP_HOT`, etc.). |
| `EnvironmentFailureReason` | Reason an environment was invalidated. |
| `ExceptionEvent` | Category of an exception passed to `ExceptionListener`. |
| `ExceptionSource` | Source subsystem that raised an exception. |
| `ExtinctionStatus` | Status returned by `ExtinctionFilter::test`. |
| `LockMode` | Read-lock mode (`DEFAULT`, `RMW`, `DIRTY_READ`, etc.). |
| `NoxuError` | Top-level error type for all `noxu-db` operations. |
| `OperationStatus` | Outcome of a positioned database operation (`SUCCESS`, `NOTFOUND`, etc.). |
| `ReplicaAckPolicy` | Replica acknowledgment level required for a commit. |
| `ScanResult` | Outcome returned by a `ScanFilter`. |
| `SyncPolicy` | Disk-sync level for a durability policy. |
| `TransactionState` | Lifecycle state of a `Transaction` (`Open`, `Prepared`, `Committed`, `Aborted`, `MustAbort`). |
| `VerifyError` | Errors discovered during a verify run. |

### Traits

| Item | Description |
|------|-------------|
| `ExceptionListener` | Callback invoked on non-fatal exceptions. |
| `ExtinctionFilter` | Record-level filter applied during log cleaning. |
| `ForeignKeyNullifier` | Nullifies a foreign key when the primary record is deleted. |
| `ForeignMultiKeyNullifier` | Nullifies multiple foreign keys. |
| `Get` | Trait for types that can serve as a get-mode argument. |
| `Put` | Trait for types that can serve as a put-mode argument. |
| `ScanFilter` | Record predicate evaluated during a disk-ordered scan. |
| `SecondaryKeyCreator` | Derives the secondary key from a primary record. |
| `SecondaryMultiKeyCreator` | Derives multiple secondary keys from a primary record. |

### Functions

| Item | Description |
|------|-------------|
| `open_disk_ordered_cursor_multi` | Opens a `DiskOrderedCursor` that merges multiple databases. |

### Type aliases

| Item | Description |
|------|-------------|
| `Result<T>` | `std::result::Result<T, NoxuError>`. |

### Feature-gated items

| Item | Feature | Description |
|------|---------|-------------|
| `noxu_db::observe_crate` (module) | `observability` | Re-exports `noxu_observe` for subscriber setup. |

### Pub-but-internal items (to be restricted in v3.0)

The following items are technically `pub` in the current codebase but are
**not** part of the v3.0 stable commitment.  They will be restricted to
`pub(crate)` or `pub(super)` before the v3.0.0 tag is applied, or (if the
timing does not allow it) they will carry `#[deprecated]` markers and be
removed in the next major version.

| Item | Reason |
|------|--------|
| `Transaction::new(id, config)` | Construction helper; users obtain transactions via `Environment::begin_transaction()`. Internally-typed overloads expose implementation details. |
| `Transaction::with_log_manager` | Internal wiring step; parameter type `LogManager` is not re-exported. |
| `Transaction::with_env_impl` | Internal wiring step; parameter type `EnvironmentImpl` is not re-exported. |
| `Transaction::with_inner_txn` | Internal wiring step; parameter type `noxu_txn::Txn` is not re-exported. |
| `Transaction::get_inner_txn` | Internal accessor; return type `noxu_txn::Txn` is not re-exported. |
| `ForeignKeyDeleteAction` | Enum variant names may change; treat as internal until v3.0. |

---

## `noxu-bind` — public API

Serialization bindings between Rust types and `DatabaseEntry` byte
representations.

### Structs

| Item | Description |
|------|-------------|
| `ByteArrayBinding` | Pass-through binding for raw byte slices. |
| `RecordNumberBinding` | Big-endian `u64` binding for record-number keys. |
| `SerdeBinding` | Serde-based binding using `bincode` / `postcard`. |
| `TupleInput` | Reader for a sortable tuple byte stream. |
| `TupleOutput` | Writer for a sortable tuple byte stream. |
| `TupleSerdeBinding` | Combined tuple + serde binding for complex value types. |
| `TupleSerdeKeyDataBinding` | Key+data tuple/serde split binding. |

### Traits

| Item | Description |
|------|-------------|
| `EntryBinding<T>` | Converts between `T` and a `DatabaseEntry`. |
| `EntityBinding<E>` | Converts between an entity `E` and two `DatabaseEntry` values (key + data). |
| `TupleBinding` | Reads/writes a value to/from a `TupleInput`/`TupleOutput`. |
| `SortKey` | Provides a sortable byte key for a type. |

### Primitive bindings

`BoolBinding`, `ByteBinding`, `CharBinding`, `DoubleBinding`, `FloatBinding`,
`IntBinding`, `LongBinding`, `PackedIntBinding`, `PackedLongBinding`,
`ShortBinding`, `SortedDoubleBinding`, `SortedFloatBinding`,
`SortedPackedIntBinding`, `SortedPackedLongBinding`, `StringBinding`.

All implement `TupleBinding` and/or `EntryBinding`.

### Enums

| Item | Description |
|------|-------------|
| `BindError` | Error type for binding operations. |

### Type aliases

| Item | Description |
|------|-------------|
| `Result<T>` | `std::result::Result<T, BindError>`. |

---

## `noxu-collections` — public API

Iterator-based typed views over Noxu DB databases.

### Structs

| Item | Description |
|------|-------------|
| `RetryConfig` | Backoff parameters for `TransactionRunner`. |
| `StoredIterator<T>` | Snapshot iterator over typed database values. |
| `StoredKeySet<'db, K, KB>` | Typed set view over database keys. |
| `StoredList<'db, V, VB>` | Indexed list with shift-down compaction on removal. |
| `StoredMap<'db, K, V, KB, VB>` | Typed map view (primary database). |
| `StoredSortedMap<'db, K, V, KB, VB>` | Sorted-map view with navigation (`first_key`, `iter_from`, etc.). |
| `StoredValueSet<'db, V, VB>` | Typed collection view over database values. |
| `TransactionRunner` | Runs a closure under a managed transaction, retrying on deadlock. |

### Enums

| Item | Description |
|------|-------------|
| `CollectionError` | Error type for collection operations. |

### Type aliases

| Item | Description |
|------|-------------|
| `Result<T>` | `std::result::Result<T, CollectionError>`. |

---

## `noxu-persist` — public API

Derive-macro-based entity persistence layer (Direct Persistence Layer).

### Structs

| Item | Description |
|------|-------------|
| `EntityStore<'env>` | Manages one or more entity databases within an environment. |
| `StoreConfig` | Configuration for `EntityStore::open`. |
| `PrimaryIndex<K, E>` | Typed CRUD index on entities by primary key. |
| `SecondaryIndex<SK, E>` | Typed index over a secondary key. |
| `SimpleSerializer` | Field-level binary serializer / deserializer. |
| `MemorySequence` | In-memory integer sequence. |

### Traits

| Item | Description |
|------|-------------|
| `Entity` | Marks a type as database-storable. |
| `PrimaryKey` | Marks a type as a valid primary key. |
| `EntitySerializer<E>` | Custom serialization strategy for an entity. |
| `FieldEncoder` | Encodes a single field to bytes. |
| `FieldDecoder` | Decodes a single field from bytes. |
| `Sequence` | Generic sequence interface. |

### Enums

| Item | Description |
|------|-------------|
| `DeleteAction` | What to do when a referenced primary record is deleted. |
| `PersistError` | Error type for persistence operations. |
| `Relate` | Cardinality of a secondary key relationship. |

### Derive macros

| Item | Description |
|------|-------------|
| `#[derive(Entity)]` | Generates `Entity` impl; recognises `#[primary_key]` and `#[secondary_key]`. |
| `#[derive(PrimaryKey)]` | Generates `PrimaryKey` impl. |
| `#[derive(SecondaryKey)]` | Generates secondary-key extraction code. |

### Schema evolution

| Item | Description |
|------|-------------|
| `ClassCatalog` | Persistent catalog of entity class versions. |
| `ClassMutations` | Set of mutations for one entity class. |
| `CatalogEntry` | A single catalog entry. |
| `Converter` | Converts old entity records to the current version. |
| `ConversionFn` | Type alias for a record-conversion closure. |
| `DecodedRecord` | A deserialized record alongside its class version. |
| `Deleter` | Marks a class version as deleted. |
| `EvolveConfig` | Options for `EntityStore` schema evolution. |
| `EvolveListener` | Callback invoked during evolution. |
| `EvolveStats` | Statistics from an evolution run. |
| `Mutations` | Collection of `ClassMutations` for multiple entities. |
| `MutationKey` | Identifies a specific mutation. |
| `Renamer` | Renames a class or field. |
| `MAX_CLASS_TAG_LEN` | Maximum length of a class-tag string (constant). |
| `catalog_db_name()` | Returns the reserved database name for the class catalog. |

### Type aliases

| Item | Description |
|------|-------------|
| `Result<T>` | `std::result::Result<T, PersistError>`. |

---

## `noxu-xa` — public API

X/Open XA distributed transactions.

### Structs

| Item | Description |
|------|-------------|
| `PreparedLog` | On-disk log of prepared (but not yet committed) XA branches. |
| `XaEnvironment` | `XaResource` implementation backed by a Noxu `Environment`. |
| `XaFlags` | Bitfield of XA flags (`NOFLAGS`, `JOIN`, `RESUME`, `TMSUCCESS`, `TMFAIL`, `TMSUSPEND`, `ONEPHASE`, `TMSTARTRSCAN`, `TMENDRSCAN`, `TMNOFLAGS`). |
| `Xid` | XA transaction identifier (format\_id + global\_txn\_id + branch\_qualifier). |

### Traits

| Item | Description |
|------|-------------|
| `XaResource` | X/Open XA resource manager interface (`xa_start`, `xa_end`, `xa_prepare`, `xa_commit`, `xa_rollback`, `xa_recover`, `xa_forget`). |

### Enums

| Item | Description |
|------|-------------|
| `PrepareResult` | Outcome of `xa_prepare` (`Ok` or `ReadOnly`). |
| `XaError` | XA error type. Note: variant `CrashDurabilityNotSupported` is `#[deprecated(since = "2.0.0")]` and will be removed in v3.0. |
| `XidError` | Error constructing an `Xid`. |

### Type aliases

| Item | Description |
|------|-------------|
| `XaResult<T>` | `std::result::Result<T, XaError>`. |

---

## `noxu-rep` — public API

Master-replica HA replication with elections, VLSN tracking, and subscriptions.

### Core types

| Item | Description |
|------|-------------|
| `ReplicatedEnvironment` | Main entry point — wraps `Environment` and adds replication. |
| `RepConfig` | Configuration for `ReplicatedEnvironment`. |
| `RepTransportKind` | Transport kind selection (TCP, in-memory, QUIC). |
| `RepGroup` | Represents the replication group membership. |
| `RepNode` | A single node within a replication group. |
| `RepStats` | Replication statistics. |
| `NodeType` | Electable vs. secondary node. |
| `NodeState` | Current role of a node (Master, Replica, Unknown, Detached). |
| `NodeStateMachine` | Manages transitions through `NodeState`. |
| `QuorumPolicy` | Quorum definition for elections. |

### Durability and consistency

| Item | Description |
|------|-------------|
| `CommitDurability` | Per-commit durability for a replicated environment. |
| `ReplicaAckPolicy` | Level of replica acknowledgment required before commit returns. |
| `ConsistencyPolicy` | Replica read-consistency guarantee. |

### Master operations

| Item | Description |
|------|-------------|
| `MasterTransfer` | Transfers the master role to another node. |
| `MasterTransferConfig` | Options for a master-transfer operation. |
| `TransferState` | Progress state of a master-transfer. |

### State change notifications

| Item | Description |
|------|-------------|
| `StateChangeEvent` | Event passed to `StateChangeListener`. |
| `StateChangeListener` | Callback invoked when node state changes. |

### Network restore

| Item | Description |
|------|-------------|
| `NetworkRestore` | Restores a node from another group member. |
| `NetworkRestoreConfig` | Options for `NetworkRestore`. |
| `RestoreState` | Progress state of a network restore. |
| `NetworkRestoreServer` | Serves restore requests from other nodes. |
| `RESTORE_SERVICE_NAME` | Service name constant for the restore protocol. |

### Reconnect

| Item | Description |
|------|-------------|
| `ReconnectConfig` | Options for the reconnect-with-retry helper. |
| `ReconnectOutcome` | Outcome of a reconnect attempt. |
| `catch_up_with_retry` | Reconnects and catches up with the master, retrying on failure. |

### Subscription

| Item | Description |
|------|-------------|
| `Subscription` | External consumer of the replication stream. |
| `SubscriptionCallback` | Trait for receiving replication events. |
| `SubscriptionConfig` | Options for a `Subscription`. |
| `SubscriptionState` | State of an active subscription. |

### Failure detection

| Item | Description |
|------|-------------|
| `PhiAccrualDetector` | Phi-accrual heartbeat failure detector. |

### In-memory transport (testing and embedded clusters)

| Item | Description |
|------|-------------|
| `InMemoryTransport` | In-process transport for testing multi-node clusters. |
| `InMemoryEndpoint` | An endpoint in an `InMemoryTransport` group. |
| `InMemoryGroup` | Group of in-memory endpoints. |

### Network transport (default features)

| Item | Feature | Description |
|------|---------|-------------|
| `Channel` (trait) | default | Abstraction over a bidirectional framed channel. |
| `LocalChannel` | default | Loopback channel for single-process testing. |
| `LocalChannelPair` | default | Pair of connected `LocalChannel` instances. |
| `TcpChannel` | default | TCP channel. |
| `TcpChannelListener` | default | TCP channel listener. |
| `TlsTcpChannel` | `tls-rustls` or `tls-native` | TLS-wrapped TCP channel. |
| `TlsTcpChannelListener` | `tls-rustls` or `tls-native` | TLS-wrapped listener. |
| `TlsConfig` | `tls-rustls` or `tls-native` | TLS certificate and key configuration. |
| `DataChannel` | default | Framing wrapper that adds a length-prefix. |
| `MAX_FRAME_PAYLOAD` | default | Maximum frame payload size (64 MiB). |

### QUIC transport (feature-gated)

| Item | Feature | Description |
|------|---------|-------------|
| `QuicChannel` | `quic` | QUIC unidirectional channel. |
| `QuicChannelListener` | `quic` | QUIC listener. |
| `default_server_config` | `quic` | Builds a default QUIC server config. |
| `insecure_client_config` | `quic` | Builds an insecure (no-verify) QUIC client config (testing only). |
| `QuicMultiplexedChannel` | `quic` | Multiplexed QUIC channel. |
| `QuicMultiplexedChannelListener` | `quic` | Multiplexed QUIC listener. |
| `ReconnectToken` | `quic` | Token for resuming a QUIC session. |
| `ReplicationChannel` | `quic` | High-level replication-stream channel over QUIC. |
| `mux_insecure_client_config` | `quic` | Insecure client config for the multiplexed transport. |
| `mux_server_config` | `quic` | Server config for the multiplexed transport. |

### Errors

| Item | Description |
|------|-------------|
| `RepError` | Replication error type. |
| `Result<T>` | `std::result::Result<T, RepError>`. |

---

## `noxu-util` — public API (foundational)

Low-level types used by the engine and its callers.

### Structs

| Item | Description |
|------|-------------|
| `Lsn` | Log sequence number (`(file_number: u32, file_offset: u32)` packed into a `u64`). |
| `Vlsn` | Version log sequence number (monotonic global sequence number for replication). |
| `DaemonThread` | RAII handle for a background daemon thread. |
| `StatDefinition` | Metadata for one statistic counter. |
| `StatGroup` | A named group of `StatDefinition` entries. |

### Enums

| Item | Description |
|------|-------------|
| `StatType` | Category of a statistic (counter, gauge, etc.). |

### Constants

| Item | Description |
|------|-------------|
| `NULL_LSN` | Sentinel LSN indicating "no position" (`u64::MAX`). |
| `NULL_VLSN` | Sentinel VLSN (sequence −1). |
| `FIRST_VLSN` | Smallest valid VLSN (sequence 1). |
| `NULL_VLSN_SEQUENCE` | Raw sequence value of `NULL_VLSN` (−1). |
| `UNINITIALIZED_VLSN_SEQUENCE` | Sentinel for an uninitialized VLSN (0). |
| `VLSN_LOG_SIZE` | Byte size of a serialized VLSN (8). |
| `SECS_PER_HOUR` | Seconds in one hour. |

### Functions

| Item | Description |
|------|-------------|
| `current_time_secs()` | Current Unix timestamp in seconds. |
| `current_time_hours()` | Current Unix timestamp in hours. |
| `is_expired(expiry_secs)` | Returns `true` if the given expiry timestamp has passed. |
| `ttl_secs_to_expiration(ttl)` | Converts a TTL in seconds to an absolute expiry timestamp. |
| `ttl_hours_to_expiration(ttl)` | Converts a TTL in hours to an absolute expiry timestamp. |
| `packed_i32_size(v)` | Byte length of `v` in packed-integer format. |
| `write_packed_i32(w, v)` | Writes `v` in packed-integer format. |
| `read_packed_i32(r)` | Reads a packed `i32`. |
| `packed_i64_size(v)` | Byte length of `v` in packed-long format. |
| `write_packed_i64(w, v)` | Writes `v` in packed-long format. |
| `read_packed_i64(r)` | Reads a packed `i64`. |
| `write_sorted_i32 / read_sorted_i32` | Sorted (sign-extending) `i32` encoding. |
| `write_sorted_i64 / read_sorted_i64` | Sorted `i64` encoding. |
| `write_sorted_f32 / read_sorted_f32` | Sorted `f32` encoding. |
| `write_sorted_f64 / read_sorted_f64` | Sorted `f64` encoding. |

---

## `noxu-config` — public API (foundational)

Configuration parameter system.  Users who extend the engine or build
tooling on top of it interact with this crate.

### Types

| Item | Description |
|------|-------------|
| `ConfigManager` | Holds and validates the live configuration for an environment. |
| `ConfigParam` | Descriptor for one configuration parameter (name, type, default, min/max). |
| `ParamValue` | Tagged union of all valid parameter value types. |
| `ParamType` | Enum of parameter value types. |
| `ConfigError` | Error type for configuration validation. |

### Parameter catalogue (`noxu_config::params`)

166 `pub static ConfigParam` constants are available under
`noxu_config::params::*`.  The following are **deprecated** as of v2.4.1
(see [Deprecated items](#deprecated-items) below):

- `CLEANER_ADJUST_UTILIZATION`
- `CLEANER_FOREGROUND_PROACTIVE_MIGRATION`
- `CLEANER_LAZY_MIGRATION`
- `EVICTOR_NODES_PER_SCAN`
- `EVICTOR_DEADLOCK_RETRY`
- `EVICTOR_LRU_ONLY`
- `LOG_DIRECT_NIO`
- `LOG_CHUNKED_NIO`
- `LOG_USE_NIO`
- `LOG_DEFERREDWRITE_TEMP`
- `OLD_REP_RUN_LOG_FLUSH_TASK`
- `OLD_REP_LOG_FLUSH_TASK_INTERVAL`

All other params are stable.  **New params may be added in minor releases**
(additive); param names and defaults will not change in patch releases.

---

## Deprecated items

The table below lists all items in the stable-tier crates that carry a
`#[deprecated]` attribute as of v2.4.1, together with the replacement.

| Item | Crate | Deprecated since | Planned removal | Replacement |
|------|-------|-----------------|-----------------|-------------|
| `XaError::CrashDurabilityNotSupported` | `noxu-xa` | 2.0.0 | v3.0 | `XaError::NotFound` or `XaError::Protocol` as appropriate. |
| `params::CLEANER_ADJUST_UTILIZATION` | `noxu-config` | 2.4.1 | v3.0 | No replacement; the optimisation is always on. |
| `params::CLEANER_FOREGROUND_PROACTIVE_MIGRATION` | `noxu-config` | 2.4.1 | v3.0 | No replacement; retained only to avoid parse errors in old property files. |
| `params::CLEANER_LAZY_MIGRATION` | `noxu-config` | 2.4.1 | v3.0 | No replacement; same as above. |
| `params::EVICTOR_NODES_PER_SCAN` | `noxu-config` | 2.4.1 | v3.0 | `params::EVICTOR_EVICT_BYTES`. |
| `params::EVICTOR_DEADLOCK_RETRY` | `noxu-config` | 2.4.1 | v3.0 | Use the evictor thread pool; per-pass retry is no longer configurable. |
| `params::EVICTOR_LRU_ONLY` | `noxu-config` | 2.4.1 | v3.0 | Cache eviction policy is now always multi-queue; no equivalent flag. |
| `params::LOG_DIRECT_NIO` | `noxu-config` | 2.4.1 | v3.0 | No replacement; this parameter has no effect. |
| `params::LOG_CHUNKED_NIO` | `noxu-config` | 2.4.1 | v3.0 | No replacement; this parameter has no effect. |
| `params::LOG_USE_NIO` | `noxu-config` | 2.4.1 | v3.0 | `params::LOG_USE_WRITE_QUEUE`. |
| `params::LOG_DEFERREDWRITE_TEMP` | `noxu-config` | 2.4.1 | v3.0 | No replacement; deferred-write is configured per-database. |
| `params::OLD_REP_RUN_LOG_FLUSH_TASK` | `noxu-config` | 2.4.1 | v3.0 | No replacement; the log-flush task is always controlled by the replication layer. |
| `params::OLD_REP_LOG_FLUSH_TASK_INTERVAL` | `noxu-config` | 2.4.1 | v3.0 | No replacement; same as above. |
| `EnvironmentMutableConfig::txn_no_sync` | `noxu-db` | 2.4.1 | v3.0 | `EnvironmentMutableConfig::durability` with `SyncPolicy::NoSync`. |
| `EnvironmentMutableConfig::txn_write_no_sync` | `noxu-db` | 2.4.1 | v3.0 | `EnvironmentMutableConfig::durability` with `SyncPolicy::WriteNoSync`. |
| `EnvironmentConfig::txn_no_sync` | `noxu-db` | 2.4.1 | v3.0 | `EnvironmentConfig::durability`. |
| `EnvironmentConfig::txn_write_no_sync` | `noxu-db` | 2.4.1 | v3.0 | `EnvironmentConfig::durability`. |

> **Note — struct-field deprecation**: Rust does not support
> `#[deprecated]` on struct fields as of the 1.95 toolchain.
> The `EnvironmentConfig` and `EnvironmentMutableConfig` fields are
> documented as deprecated in their rustdoc but do not carry the attribute.
> A `#[deprecated]` note on the owning struct's `set_txn_no_sync` /
> `with_txn_no_sync` accessor methods is added instead.

---

## How to keep this document current

1. Run `cargo semver-checks --workspace --baseline-rev main` before every PR
   that touches a stable-tier crate.  Any detected break must be either
   reverted, labelled `BREAKING:`, or explicitly justified.
2. When adding a new `pub` item to a stable crate, add it to the table
   in the appropriate section.
3. When removing or changing a `pub` item, first mark it `#[deprecated]`
   for at least one minor release cycle.
4. Run `make docs-check` to verify this file builds cleanly.
