//! Environment configuration parameter definitions.
//!
//! This module defines all known configuration parameters with their types,
//! defaults, ranges, and mutability. Parameters are organized by subsystem.

use crate::param::ConfigParam;
use crate::param::ParamValue;
use std::time::Duration;

// =========================================================================
// Memory parameters
// =========================================================================

/// Maximum number of bytes used for the in-memory cache.
/// A value of 0 means use MAX_MEMORY_PERCENT instead.
pub static MAX_MEMORY: ConfigParam = ConfigParam::long_param(
    "noxu.maxMemory",
    None,  // min
    None,  // max
    0,     // default: use percent
    true,  // mutable
    false, // forReplication
);

/// Percentage of JVM memory to use for the cache (1-90).
pub static MAX_MEMORY_PERCENT: ConfigParam = ConfigParam::int_param(
    "noxu.maxMemoryPercent",
    Some(1),  // min
    Some(90), // max
    60,       // default
    true,     // mutable
    false,    // forReplication
);

/// Whether multiple environments share the cache.
pub static SHARED_CACHE: ConfigParam = ConfigParam::bool_param(
    "noxu.sharedCache",
    false, // default
    false, // mutable
    false, // forReplication
);

/// Maximum total disk space used by log files, in bytes. 0 means unlimited.
pub static MAX_DISK: ConfigParam = ConfigParam::long_param(
    "noxu.maxDisk",
    Some(0), // min
    None,    // max
    0,       // default: unlimited
    true,    // mutable
    false,   // forReplication
);

/// Minimum free disk space to reserve, in bytes.
pub static FREE_DISK: ConfigParam = ConfigParam::long_param(
    "noxu.freeDisk",
    Some(0),       // min
    None,          // max
    5_368_709_120, // default: 5 GB
    true,          // mutable
    false,         // forReplication
);

/// Maximum off-heap cache size in bytes. 0 means disabled.
pub static MAX_OFF_HEAP_MEMORY: ConfigParam = ConfigParam::long_param(
    "noxu.maxOffHeapMemory",
    Some(0), // min
    None,    // max
    0,       // default: disabled
    true,    // mutable
    false,   // forReplication
);

// =========================================================================
// Environment parameters
// =========================================================================

/// If true, btree and dup comparators will be instantiated even when recovery
/// is not run (used by utilities such as DbScavenger).
pub static ENV_COMPARATORS_REQUIRED: ConfigParam = ConfigParam::bool_param(
    "noxu.env.comparatorsRequired",
    false, // default
    false, // mutable
    false, // forReplication
);

/// If true, create the environment with recovery enabled.
pub static ENV_RECOVERY: ConfigParam = ConfigParam::bool_param(
    "noxu.env.recovery",
    true,  // default
    false, // mutable
    false, // forReplication
);

/// If true, force a checkpoint during recovery.
pub static ENV_RECOVERY_FORCE_CHECKPOINT: ConfigParam = ConfigParam::bool_param(
    "noxu.env.recoveryForceCheckpoint",
    false, // default
    false, // mutable
    false, // forReplication
);

/// If true, force writing to a new log file during recovery.
pub static ENV_RECOVERY_FORCE_NEW_FILE: ConfigParam = ConfigParam::bool_param(
    "noxu.env.recoveryForceNewFile",
    false, // default
    false, // mutable
    false, // forReplication
);

/// If true, halt the JVM on a commit that follows a checksum exception.
pub static HALT_ON_COMMIT_AFTER_CHECKSUMEXCEPTION: ConfigParam =
    ConfigParam::bool_param(
        "noxu.haltOnCommitAfterChecksumException",
        false, // default
        false, // mutable
        false, // forReplication
    );

/// If true, the environment is transactional.
pub static ENV_IS_TRANSACTIONAL: ConfigParam = ConfigParam::bool_param(
    "noxu.env.isTransactional",
    false, // default
    false, // mutable
    false, // forReplication
);

/// If true, the environment uses locking for concurrent access.
pub static ENV_IS_LOCKING: ConfigParam = ConfigParam::bool_param(
    "noxu.env.isLocking",
    true,  // default
    false, // mutable
    false, // forReplication
);

/// If true, the environment is read-only.
pub static ENV_IS_READ_ONLY: ConfigParam = ConfigParam::bool_param(
    "noxu.env.isReadOnly",
    false, // default
    false, // mutable
    false, // forReplication
);

/// If true, use fair latches (FIFO ordering for waiters).
pub static ENV_FAIR_LATCHES: ConfigParam = ConfigParam::bool_param(
    "noxu.env.fairLatches",
    false, // default
    false, // mutable
    false, // forReplication
);

/// No longer used. Left in place to avoid errors from existing config settings.
pub static ENV_SHARED_LATCHES: ConfigParam = ConfigParam::bool_param(
    "noxu.env.sharedLatches",
    true,  // default
    false, // mutable
    false, // forReplication
);

/// If true, configure the logging framework on environment open.
pub static ENV_SETUP_LOGGER: ConfigParam = ConfigParam::bool_param(
    "noxu.env.setupLogger",
    false, // default
    false, // mutable
    false, // forReplication
);

/// Latch timeout duration.
pub static ENV_LATCH_TIMEOUT: ConfigParam = ConfigParam {
    name: "noxu.env.latchTimeout",
    param_type: crate::param::ParamType::Duration,
    default: ParamValue::Duration(Duration::from_secs(5 * 60)), // 5 min
    min: Some(ParamValue::Duration(Duration::ZERO)),
    max: Some(ParamValue::Duration(Duration::from_secs(75 * 60))),
    mutable: false,
    for_replication: false,
};

/// Clock tolerance used for TTL (time-to-live) expiration checks.
pub static ENV_TTL_CLOCK_TOLERANCE: ConfigParam = ConfigParam {
    name: "noxu.env.ttlClockTolerance",
    param_type: crate::param::ParamType::Duration,
    default: ParamValue::Duration(Duration::from_secs(2 * 3600)), // 2 h
    min: Some(ParamValue::Duration(Duration::ZERO)),
    max: Some(ParamValue::Duration(Duration::from_secs(24 * 3600))),
    mutable: false,
    for_replication: false,
};

/// Maximum assumed lock-hold time for TTL-related decisions.
pub static ENV_TTL_MAX_TXN_TIME: ConfigParam = ConfigParam {
    name: "noxu.env.ttlMaxTxnTime",
    param_type: crate::param::ParamType::Duration,
    default: ParamValue::Duration(Duration::from_secs(24 * 3600)), // 24 h
    min: Some(ParamValue::Duration(Duration::ZERO)),
    max: Some(ParamValue::Duration(Duration::from_secs(30 * 24 * 3600))),
    mutable: false,
    for_replication: false,
};

/// Delay added to record expiration time before purging in the cleaner.
pub static ENV_TTL_LN_PURGE_DELAY: ConfigParam = ConfigParam {
    name: "noxu.env.ttlLnPurgeDelay",
    param_type: crate::param::ParamType::Duration,
    default: ParamValue::Duration(Duration::from_secs(5)),
    min: Some(ParamValue::Duration(Duration::ZERO)),
    max: Some(ParamValue::Duration(Duration::from_secs(24 * 3600))),
    mutable: false,
    for_replication: false,
};

/// If true, include user key/data in exception and log messages.
pub static ENV_EXPOSE_USER_DATA: ConfigParam = ConfigParam::bool_param(
    "noxu.env.exposeUserData",
    false, // default
    true,  // mutable
    false, // forReplication
);

/// If true, enable database-level eviction.
pub static ENV_DB_EVICTION: ConfigParam = ConfigParam::bool_param(
    "noxu.env.dbEviction",
    true,  // default
    false, // mutable
    false, // forReplication
);

/// If true, enable duplicate-db conversion preload of all data.
pub static ENV_DUP_CONVERT_PRELOAD_ALL: ConfigParam = ConfigParam::bool_param(
    "noxu.env.dupConvertPreloadAll",
    true,  // default
    false, // mutable
    false, // forReplication
);

/// Chunk size for Adler32 checksum computation. 0 means compute over whole entry.
pub static ADLER32_CHUNK_SIZE: ConfigParam = ConfigParam::int_param(
    "noxu.adler32.chunkSize",
    Some(0),       // min
    Some(1 << 20), // max: 1 MB
    0,             // default
    true,          // mutable
    false,         // forReplication
);

/// If true, check for resource leaks (cursors, transactions) when closing.
pub static ENV_CHECK_LEAKS: ConfigParam = ConfigParam::bool_param(
    "noxu.env.checkLeaks",
    true,  // default
    false, // mutable
    false, // forReplication
);

/// If true, yield the thread after each latch acquisition (for testing).
pub static ENV_FORCED_YIELD: ConfigParam = ConfigParam::bool_param(
    "noxu.env.forcedYield",
    false, // default
    false, // mutable
    false, // forReplication
);

/// If true, enable TTL (time-to-live) expiration.
pub static ENV_EXPIRATION_ENABLED: ConfigParam = ConfigParam::bool_param(
    "noxu.env.expirationEnabled",
    true,  // default
    true,  // mutable
    false, // forReplication
);

/// Number of processed entries after which the database cache is cleared.
pub static ENV_DB_CACHE_CLEAR_COUNT: ConfigParam = ConfigParam::int_param(
    "noxu.env.dbCacheClearCount",
    Some(1), // min
    None,    // max
    100,     // default
    true,    // mutable
    false,   // forReplication
);

// =========================================================================
// Background daemon parameters
// =========================================================================

/// If true, run the IN compressor daemon thread.
pub static ENV_RUN_IN_COMPRESSOR: ConfigParam = ConfigParam::bool_param(
    "noxu.env.runINCompressor",
    true,  // default
    true,  // mutable
    false, // forReplication
);

/// If true, run the evictor daemon threads.
pub static ENV_RUN_EVICTOR: ConfigParam = ConfigParam::bool_param(
    "noxu.env.runEvictor",
    true,  // default
    true,  // mutable
    false, // forReplication
);

/// If true, run the off-heap evictor daemon threads.
pub static ENV_RUN_OFFHEAP_EVICTOR: ConfigParam = ConfigParam::bool_param(
    "noxu.env.runOffHeapEvictor",
    true,  // default
    true,  // mutable
    false, // forReplication
);

/// If true, run the cleaner daemon threads.
pub static ENV_RUN_CLEANER: ConfigParam = ConfigParam::bool_param(
    "noxu.env.runCleaner",
    true,  // default
    true,  // mutable
    false, // forReplication
);

/// If true, run the checkpointer daemon thread.
pub static ENV_RUN_CHECKPOINTER: ConfigParam = ConfigParam::bool_param(
    "noxu.env.runCheckpointer",
    true,  // default
    true,  // mutable
    false, // forReplication
);

/// If true, run the background verifier daemon.
pub static ENV_RUN_VERIFIER: ConfigParam = ConfigParam::bool_param(
    "noxu.env.runVerifier",
    true,  // default
    true,  // mutable
    false, // forReplication
);

/// Maximum reads per second for background threads (0 = unlimited).
pub static ENV_BACKGROUND_READ_LIMIT: ConfigParam = ConfigParam::int_param(
    "noxu.env.backgroundReadLimit",
    Some(0), // min
    None,    // max
    0,       // default: unlimited
    true,    // mutable
    false,   // forReplication
);

/// Maximum writes per second for background threads (0 = unlimited).
pub static ENV_BACKGROUND_WRITE_LIMIT: ConfigParam = ConfigParam::int_param(
    "noxu.env.backgroundWriteLimit",
    Some(0), // min
    None,    // max
    0,       // default: unlimited
    true,    // mutable
    false,   // forReplication
);

/// Sleep interval between background thread operations (for rate limiting).
pub static ENV_BACKGROUND_SLEEP_INTERVAL: ConfigParam = ConfigParam {
    name: "noxu.env.backgroundSleepInterval",
    param_type: crate::param::ParamType::Duration,
    default: ParamValue::Duration(Duration::from_millis(1)),
    min: Some(ParamValue::Duration(Duration::ZERO)),
    max: Some(ParamValue::Duration(Duration::from_secs(75 * 60))),
    mutable: true,
    for_replication: false,
};

// =========================================================================
// Log parameters
// =========================================================================

/// Total memory for log buffers. 0 means compute from noxu.maxMemory.
/// Minimum value: NUM_LOG_BUFFERS_DEFAULT (3) * MIN_LOG_BUFFER_SIZE (2048) = 6144.
pub static LOG_MEM_SIZE: ConfigParam = ConfigParam::long_param(
    "noxu.log.totalBufferBytes",
    Some(6144), // min: 3 * 2048
    None,       // max
    0,          // default: computed from noxu.maxMemory
    false,      // mutable
    false,      // forReplication
);

/// Number of log buffers.
pub static LOG_NUM_BUFFERS: ConfigParam = ConfigParam::int_param(
    "noxu.log.numBuffers",
    Some(2), // min
    None,    // max
    3,       // default
    false,   // mutable
    false,   // forReplication
);

/// Maximum size of each log buffer in bytes.
pub static LOG_BUFFER_SIZE: ConfigParam = ConfigParam::int_param(
    "noxu.log.bufferSize",
    Some(1 << 10), // min: 1 KB
    None,          // max
    1 << 20,       // default: 1 MB
    false,         // mutable
    false,         // forReplication
);

/// Size of read buffer for log faulting, in bytes.
pub static LOG_FAULT_READ_SIZE: ConfigParam = ConfigParam::int_param(
    "noxu.log.faultReadSize",
    Some(32), // min
    None,     // max
    2048,     // default: 2 KB
    false,    // mutable
    false,    // forReplication
);

/// Size of iterator read buffer for log scanning.
pub static LOG_ITERATOR_READ_SIZE: ConfigParam = ConfigParam::int_param(
    "noxu.log.iteratorReadSize",
    Some(128), // min
    None,      // max
    8192,      // default: 8 KB
    false,     // mutable
    false,     // forReplication
);

/// Maximum size of the iterator buffer for log scanning.
pub static LOG_ITERATOR_MAX_SIZE: ConfigParam = ConfigParam::int_param(
    "noxu.log.iteratorMaxSize",
    Some(128),  // min
    None,       // max
    16_777_216, // default: 16 MB
    false,      // mutable
    false,      // forReplication
);

/// Maximum size of a single log file in bytes.
pub static LOG_FILE_MAX: ConfigParam = ConfigParam::long_param(
    "noxu.log.fileMax",
    Some(1_000_000),     // min: 1 MB
    Some(1_073_741_824), // max: 1 GB
    10_000_000,          // default: 10 MB
    false,               // mutable
    false,               // forReplication
);

/// Number of data directories for log striping (0 = single directory).
pub static LOG_N_DATA_DIRECTORIES: ConfigParam = ConfigParam::int_param(
    "noxu.log.nDataDirectories",
    Some(0),   // min
    Some(256), // max
    0,         // default: no striping
    false,     // mutable
    false,     // forReplication
);

/// If true, verify checksums when reading log entries.
pub static LOG_CHECKSUM_READ: ConfigParam = ConfigParam::bool_param(
    "noxu.log.checksumRead",
    true,  // default
    false, // mutable
    false, // forReplication
);

/// If true, verify all checksums at startup (not just on read).
pub static LOG_VERIFY_CHECKSUMS: ConfigParam = ConfigParam::bool_param(
    "noxu.log.verifyChecksums",
    false, // default
    false, // mutable
    false, // forReplication
);

/// If true, the environment uses an in-memory log (no files).
pub static LOG_MEM_ONLY: ConfigParam = ConfigParam::bool_param(
    "noxu.log.memOnly",
    false, // default
    false, // mutable
    false, // forReplication
);

/// Number of log file handle descriptors to cache.
pub static LOG_FILE_CACHE_SIZE: ConfigParam = ConfigParam::int_param(
    "noxu.log.fileCacheSize",
    Some(3), // min
    None,    // max
    100,     // default
    false,   // mutable
    false,   // forReplication
);

/// Experimental warm-up file read size in bytes (0 = disabled).
pub static LOG_FILE_WARM_UP_SIZE: ConfigParam = ConfigParam::int_param(
    "noxu.log.fileWarmUpSize",
    Some(0), // min
    None,    // max
    0,       // default: disabled
    false,   // mutable
    false,   // forReplication
);

/// Buffer size for warm-up file reads.
pub static LOG_FILE_WARM_UP_BUF_SIZE: ConfigParam = ConfigParam::int_param(
    "noxu.log.fileWarmUpReadSize",
    Some(128),  // min
    None,       // max
    10_485_760, // default: 10 MB
    false,      // mutable
    false,      // forReplication
);

/// If true, detect unexpected log file deletion.
pub static LOG_DETECT_FILE_DELETE: ConfigParam = ConfigParam::bool_param(
    "noxu.log.detectFileDelete",
    true,  // default
    false, // mutable
    false, // forReplication
);

/// Interval at which to check for unexpected file deletions.
pub static LOG_DETECT_FILE_DELETE_INTERVAL: ConfigParam = ConfigParam {
    name: "noxu.log.detectFileDeleteInterval",
    param_type: crate::param::ParamType::Duration,
    default: ParamValue::Duration(Duration::from_millis(1000)),
    min: Some(ParamValue::Duration(Duration::ZERO)),
    max: Some(ParamValue::Duration(Duration::from_secs(75 * 60))),
    mutable: false,
    for_replication: false,
};

/// Timeout for fsync operations.
pub static LOG_FSYNC_TIMEOUT: ConfigParam = ConfigParam {
    name: "noxu.log.fsyncTimeout",
    param_type: crate::param::ParamType::Duration,
    default: ParamValue::Duration(Duration::from_millis(500)),
    min: Some(ParamValue::Duration(Duration::ZERO)),
    max: Some(ParamValue::Duration(Duration::from_secs(75 * 60))),
    mutable: false,
    for_replication: false,
};

/// Time limit for an fsync before logging a warning.
pub static LOG_FSYNC_TIME_LIMIT: ConfigParam = ConfigParam {
    name: "noxu.log.fsyncTimeLimit",
    param_type: crate::param::ParamType::Duration,
    default: ParamValue::Duration(Duration::from_secs(5)),
    min: Some(ParamValue::Duration(Duration::ZERO)),
    max: Some(ParamValue::Duration(Duration::from_secs(30))),
    mutable: false,
    for_replication: false,
};

/// Interval for group commit batching. 0 = no group commit (default).
///
/// JE faithfulness: `EnvironmentParams.LOG_GROUP_COMMIT_INTERVAL` defaults to
/// `0 ns` (`grpWaitOn = false`) and the extended-fork `FSyncManager`
/// (`kvmain/.../log/FSyncManager.java`) removed the leader wait entirely —
/// coalescing is achieved purely by the leader/waiter piggyback during the
/// fsync I/O window, which self-tunes to load (the batch window IS the fsync
/// duration).  A non-zero interval only adds commit latency without improving
/// the batch factor under real concurrency, so the default matches the
/// reference: no wait.
pub static LOG_GROUP_COMMIT_INTERVAL: ConfigParam = ConfigParam {
    name: "noxu.log.groupCommitInterval",
    param_type: crate::param::ParamType::Duration,
    default: ParamValue::Duration(Duration::ZERO),
    min: Some(ParamValue::Duration(Duration::ZERO)),
    max: Some(ParamValue::Duration(Duration::from_secs(75 * 60))),
    mutable: false,
    for_replication: false,
};

/// Minimum number of queued committers before the leader fsyncs immediately
/// (skipping the interval wait).  Default 0 = disabled (JE
/// `LOG_GROUP_COMMIT_THRESHOLD` default), matching the reference pure-piggyback
/// design.  Group-commit waiting is active only when BOTH this and the
/// interval are non-zero (`grpWaitOn`), an opt-in for callers who want to
/// trade latency for a larger forced batch.
pub static LOG_GROUP_COMMIT_THRESHOLD: ConfigParam = ConfigParam::int_param(
    "noxu.log.groupCommitThreshold",
    Some(0), // min
    None,    // max
    0,       // default
    false,   // mutable
    false,   // forReplication
);

/// Maximum number of concurrent `fdatasync`s in flight — the bounded fsync
/// pipeline depth.  Default `1` = the historical single-leader group commit
/// (one `fdatasync` at a time; flat tail latency, throughput capped at the
/// single-file fsync latency).  Values `> 1` let that many `fdatasync`s overlap
/// on the log file, closing much of the write-throughput gap on devices that
/// sustain concurrent same-file syncs (typical NVMe does ~10k/s) at the cost of
/// a slightly higher tail.  The drain (pwrite to the page cache) stays
/// serialized in LSN order regardless of this value, and the durable watermark
/// stays a single monotonic point, so durability is identical at any depth.
/// Conservative default `1` — opt in to `2`/`4`/`8` for write-heavy workloads.
pub static LOG_FSYNC_MAX_LEADERS: ConfigParam = ConfigParam::int_param(
    "noxu.log.fsyncMaxLeaders",
    Some(1),  // min (0 clamps to 1 anyway; expose 1 as the floor)
    Some(64), // max (a sane upper bound; well past any device's useful depth)
    1,        // default: single-leader, no behavior change
    false,    // mutable
    false,    // forReplication
);

/// Concurrency-adaptive fsync batch window: max overlapping `fdatasync`
/// leaders permitted while FEWER than `LOG_FSYNC_ADAPTIVE_TRIGGER` committers
/// are waiting.  Default `1` = disabled (exact JE single-leader piggyback).
///
/// The single-leader group commit (JE `FSyncManager`, `workInProgress`) makes
/// a committer that arrives during an in-flight fsync PARK until that fsync
/// completes.  At high concurrency this coalesces many committers into one
/// fsync (a big win).  At low/mid concurrency the parked committer pays the
/// leader's fsync latency for a batch of only 2-4 — pure added latency.  When
/// this knob is `> 1`, a committer that finds the leader busy AND sees fewer
/// than `LOG_FSYNC_ADAPTIVE_TRIGGER` waiters becomes an additional parallel
/// leader (its own `fdatasync`) instead of parking; once the waiter count
/// reaches the trigger the ceiling clamps back to `LOG_FSYNC_MAX_LEADERS` so
/// committers pile into one big batch.  Recovers low/mid-concurrency write
/// throughput without losing the high-concurrency batching win.  The signal
/// (waiter count) is read under the fsync manager's already-held state lock —
/// no extra atomic, no CAS, no spin on the commit hot path.
pub static LOG_FSYNC_ADAPTIVE_LEADERS: ConfigParam = ConfigParam::int_param(
    "noxu.log.fsyncAdaptiveLeaders",
    Some(1),  // min (1 = disabled)
    Some(64), // max
    1,        // default: disabled, exact JE behaviour
    false,    // mutable
    false,    // forReplication
);

/// Waiter count at/above which the adaptive fsync window
/// ([`LOG_FSYNC_ADAPTIVE_LEADERS`]) clamps the leader ceiling back to
/// [`LOG_FSYNC_MAX_LEADERS`] and forces batching.  Below it, up to
/// `LOG_FSYNC_ADAPTIVE_LEADERS` fsyncs may overlap.  Default `0` (paired with
/// the default `LOG_FSYNC_ADAPTIVE_LEADERS = 1`, the adaptive path is off).
/// A small value (e.g. `4`) means "batch once 4+ committers are contending;
/// below that, let them fsync in parallel".
pub static LOG_FSYNC_ADAPTIVE_TRIGGER: ConfigParam = ConfigParam::int_param(
    "noxu.log.fsyncAdaptiveTrigger",
    Some(0), // min (0 = off)
    Some(1024),
    0,     // default: off
    false, // mutable
    false, // forReplication
);

/// Consolidation-array Log Write Latch (Aether VLDB'10 tech 3 / Silo SOSP'13
/// / WiredTiger `log_slot.c`).  When `true`, concurrent committers combine
/// into one batch via a lock-free CAS-join and a single leader drives the
/// whole batch's LSN-assign + buffer-slot reservation under ONE latch
/// acquisition — dissolving the per-committer futex park/wake convoy on the
/// log-write latch (the #1 measured write bottleneck: 40/46 threads block on
/// it; `txn_mix` collapses).
///
/// The single WAL + single monotonic LSN are preserved (the leader assigns a
/// contiguous LSN range in arrival order); on-disk format is byte-identical.
/// Defaults to `false` (the classic mutex path) — opt in after validating the
/// shuttle model on the target platform.  Immutable at runtime (chosen at
/// environment open).
pub static LOG_CONSOLIDATION_ARRAY: ConfigParam = ConfigParam::bool_param(
    "noxu.log.consolidationArray",
    false, // default: classic mutex LWL, no behaviour change
    false, // mutable
    false, // forReplication
);

/// Interval for periodic log flush with sync durability.
pub static LOG_FLUSH_SYNC_INTERVAL: ConfigParam = ConfigParam {
    name: "noxu.log.flushSyncInterval",
    param_type: crate::param::ParamType::Duration,
    default: ParamValue::Duration(Duration::from_secs(20)),
    min: Some(ParamValue::Duration(Duration::ZERO)),
    max: Some(ParamValue::Duration(Duration::from_secs(75 * 60))),
    mutable: true,
    for_replication: false,
};

/// Interval for periodic log flush without sync (write-no-sync).
pub static LOG_FLUSH_NO_SYNC_INTERVAL: ConfigParam = ConfigParam {
    name: "noxu.log.flushNoSyncInterval",
    param_type: crate::param::ParamType::Duration,
    default: ParamValue::Duration(Duration::from_secs(5)),
    min: Some(ParamValue::Duration(Duration::ZERO)),
    max: Some(ParamValue::Duration(Duration::from_secs(75 * 60))),
    mutable: true,
    for_replication: false,
};

/// If true, use O_DSYNC flag for log writes.
pub static LOG_USE_ODSYNC: ConfigParam = ConfigParam::bool_param(
    "noxu.log.useODSYNC",
    false, // default
    false, // mutable
    false, // forReplication
);

/// If true, use Java NIO for log writes.
///
/// Deprecated — prefer [`LOG_USE_WRITE_QUEUE`] instead.
#[deprecated(
    since = "2.4.1",
    note = "use LOG_USE_WRITE_QUEUE instead; NIO log writes are no longer the preferred path"
)]
pub static LOG_USE_NIO: ConfigParam = ConfigParam::bool_param(
    "noxu.log.useNIO",
    false, // default
    false, // mutable
    false, // forReplication
);

/// If true, use a write queue for asynchronous log I/O.
pub static LOG_USE_WRITE_QUEUE: ConfigParam = ConfigParam::bool_param(
    "noxu.log.useWriteQueue",
    true,  // default
    false, // mutable
    false, // forReplication
);

/// Size of the log write queue in bytes (4 KB – 32 MB).
pub static LOG_WRITE_QUEUE_SIZE: ConfigParam = ConfigParam::int_param(
    "noxu.log.writeQueueSize",
    Some(1 << 12), // min: 4 KB
    Some(1 << 28), // max: 256 MB
    1 << 20,       // default: 1 MB
    false,         // mutable
    false,         // forReplication
);

/// Deferred-write flag for temporary databases.
///
/// Deprecated — per-database deferred-write is configured on the `DatabaseConfig` directly.
#[deprecated(
    since = "2.4.1",
    note = "configure deferred-write per database via DatabaseConfig instead"
)]
pub static LOG_DEFERREDWRITE_TEMP: ConfigParam = ConfigParam::bool_param(
    "noxu.deferredWrite.temp",
    false, // default
    false, // mutable
    false, // forReplication
);

/// Compatibility parameter: whether the replication log-flush task is active.
///
/// Deprecated — the log-flush task is always managed by the replication layer.
#[deprecated(
    since = "2.4.1",
    note = "the replication log-flush task is always controlled by the replication layer; this flag has no effect"
)]
pub static OLD_REP_RUN_LOG_FLUSH_TASK: ConfigParam = ConfigParam::bool_param(
    "noxu.rep.runLogFlushTask",
    true, // default
    true, // mutable
    true, // forReplication
);

/// Compatibility parameter: interval for the replication log-flush task.
///
/// Deprecated — see [`OLD_REP_RUN_LOG_FLUSH_TASK`].
#[deprecated(
    since = "2.4.1",
    note = "the replication log-flush task is always controlled by the replication layer; this interval has no effect"
)]
pub static OLD_REP_LOG_FLUSH_TASK_INTERVAL: ConfigParam = ConfigParam {
    name: "noxu.rep.logFlushTaskInterval",
    param_type: crate::param::ParamType::Duration,
    default: ParamValue::Duration(Duration::from_secs(5 * 60)), // 5 min
    min: Some(ParamValue::Duration(Duration::ZERO)),
    max: Some(ParamValue::Duration(Duration::from_secs(75 * 60))),
    mutable: true,
    for_replication: true,
};

// =========================================================================
// Verification parameters
// =========================================================================

/// Cron-style schedule for the background verifier (e.g., "0 0 * * *").
pub static VERIFY_SCHEDULE: ConfigParam = ConfigParam::string_param(
    "noxu.env.verifySchedule",
    "0 0 * * *", // default
    true,        // mutable
    false,       // forReplication
);

/// Maximum tardiness tolerated before skipping a scheduled verification run.
pub static VERIFY_MAX_TARDINESS: ConfigParam = ConfigParam {
    name: "noxu.env.verifyMaxTardiness",
    param_type: crate::param::ParamType::Duration,
    default: ParamValue::Duration(Duration::from_secs(5 * 60)), // 5 min
    min: Some(ParamValue::Duration(Duration::ZERO)),
    max: Some(ParamValue::Duration(Duration::from_secs(24 * 3600))),
    mutable: true,
    for_replication: false,
};

/// If true, verify B-tree structure during background verification.
pub static VERIFY_BTREE: ConfigParam = ConfigParam::bool_param(
    "noxu.env.verifyBtree",
    true,  // default
    true,  // mutable
    false, // forReplication
);

/// Delay between log entry reads during log verification.
pub static VERIFY_LOG_READ_DELAY: ConfigParam = ConfigParam {
    name: "noxu.env.verifyLogReadDelay",
    param_type: crate::param::ParamType::Duration,
    default: ParamValue::Duration(Duration::from_millis(100)),
    min: Some(ParamValue::Duration(Duration::ZERO)),
    max: Some(ParamValue::Duration(Duration::from_secs(60))),
    mutable: true,
    for_replication: false,
};

/// If true, verify log checksums during background verification.
pub static VERIFY_LOG: ConfigParam = ConfigParam::bool_param(
    "noxu.env.verifyLog",
    true,  // default
    true,  // mutable
    false, // forReplication
);

/// If true, verify secondary database integrity during background verification.
pub static VERIFY_SECONDARIES: ConfigParam = ConfigParam::bool_param(
    "noxu.env.verifySecondaries",
    true,  // default
    true,  // mutable
    false, // forReplication
);

/// If true, verify data record checksums during background verification.
pub static VERIFY_DATA_RECORDS: ConfigParam = ConfigParam::bool_param(
    "noxu.env.verifyDataRecords",
    false, // default
    true,  // mutable
    false, // forReplication
);

/// If true, verify that obsolete records are truly obsolete.
pub static VERIFY_OBSOLETE_RECORDS: ConfigParam = ConfigParam::bool_param(
    "noxu.env.verifyObsoleteRecords",
    false, // default
    true,  // mutable
    false, // forReplication
);

/// Number of B-tree entries verified per batch during background verification.
pub static VERIFY_BTREE_BATCH_SIZE: ConfigParam = ConfigParam::int_param(
    "noxu.env.verifyBtreeBatchSize",
    Some(1),     // min
    Some(10000), // max
    1000,        // default
    true,        // mutable
    false,       // forReplication
);

/// Delay between B-tree verification batches.
pub static VERIFY_BTREE_BATCH_DELAY: ConfigParam = ConfigParam {
    name: "noxu.env.verifyBtreeBatchDelay",
    param_type: crate::param::ParamType::Duration,
    default: ParamValue::Duration(Duration::from_millis(10)),
    min: Some(ParamValue::Duration(Duration::ZERO)),
    max: Some(ParamValue::Duration(Duration::from_secs(60))),
    mutable: true,
    for_replication: false,
};

// =========================================================================
// Tree parameters
// =========================================================================

/// Maximum number of entries in an Internal Node (IN).
pub static NODE_MAX_ENTRIES: ConfigParam = ConfigParam::int_param(
    "noxu.nodeMaxEntries",
    Some(4),     // min
    Some(32767), // max
    128,         // default
    false,       // mutable
    false,       // forReplication
);

/// Maximum number of entries in a duplicate subtree node.
pub static NODE_DUP_TREE_MAX_ENTRIES: ConfigParam = ConfigParam::int_param(
    "noxu.nodeDupTreeMaxEntries",
    Some(4),     // min
    Some(32767), // max
    128,         // default
    false,       // mutable
    false,       // forReplication
);

/// Maximum size for an embedded LN (data stored directly in BIN slot).
pub static TREE_MAX_EMBEDDED_LN: ConfigParam = ConfigParam::int_param(
    "noxu.tree.maxEmbeddedLN",
    Some(0), // min
    None,    // max
    16,      // default: 16 bytes
    false,   // mutable
    false,   // forReplication
);

/// Maximum number of BIN-delta slots as a percentage of total BIN slots.
pub static TREE_BIN_DELTA: ConfigParam = ConfigParam::int_param(
    "noxu.tree.binDelta",
    Some(0),  // min
    Some(75), // max
    25,       // default
    false,    // mutable
    false,    // forReplication
);

/// Whether blind insertions are allowed in BIN-deltas.
pub static BIN_DELTA_BLIND_OPS: ConfigParam = ConfigParam::bool_param(
    "noxu.tree.binDeltaBlindOps",
    true,  // default
    false, // mutable
    false, // forReplication
);

/// Whether blind puts (with bloom filters) are allowed in BIN-deltas.
pub static BIN_DELTA_BLIND_PUTS: ConfigParam = ConfigParam::bool_param(
    "noxu.tree.binDeltaBlindPuts",
    true,  // default
    false, // mutable
    false, // forReplication
);

/// Minimum memory for B-tree nodes, in bytes.
pub static TREE_MIN_MEMORY: ConfigParam = ConfigParam::long_param(
    "noxu.tree.minMemory",
    Some(50 * 1024), // min: 50 KB
    None,            // max
    500 * 1024,      // default: 500 KB
    true,            // mutable
    false,           // forReplication
);

/// Maximum key length for compact (inline) key storage.
pub static TREE_COMPACT_MAX_KEY_LENGTH: ConfigParam = ConfigParam::int_param(
    "noxu.tree.compactMaxKeyLength",
    Some(0),   // min
    Some(255), // max
    16,        // default
    false,     // mutable
    false,     // forReplication
);

// =========================================================================
// IN Compressor parameters
// =========================================================================

/// Wakeup interval for the IN compressor daemon.
pub static COMPRESSOR_WAKEUP_INTERVAL: ConfigParam = ConfigParam {
    name: "noxu.compressor.wakeupInterval",
    param_type: crate::param::ParamType::Duration,
    default: ParamValue::Duration(Duration::from_secs(5)),
    min: Some(ParamValue::Duration(Duration::ZERO)),
    max: Some(ParamValue::Duration(Duration::from_secs(75 * 60))),
    mutable: false,
    for_replication: false,
};

/// Number of times to retry on lock conflict in compressor.
pub static COMPRESSOR_DEADLOCK_RETRY: ConfigParam = ConfigParam::int_param(
    "noxu.compressor.deadlockRetry",
    Some(0), // min
    None,    // max
    3,       // default
    false,   // mutable
    false,   // forReplication
);

/// Lock timeout used during IN compressor operations.
pub static COMPRESSOR_LOCK_TIMEOUT: ConfigParam = ConfigParam {
    name: "noxu.compressor.lockTimeout",
    param_type: crate::param::ParamType::Duration,
    default: ParamValue::Duration(Duration::from_millis(500)),
    min: Some(ParamValue::Duration(Duration::ZERO)),
    max: Some(ParamValue::Duration(Duration::from_secs(75 * 60))),
    mutable: false,
    for_replication: false,
};

// =========================================================================
// Evictor parameters
// =========================================================================

/// Number of evictor core threads.
pub static EVICTOR_CORE_THREADS: ConfigParam = ConfigParam::int_param(
    "noxu.evictor.coreThreads",
    Some(0), // min
    None,    // max
    1,       // default
    true,    // mutable
    false,   // forReplication
);

/// Maximum number of evictor threads.
pub static EVICTOR_MAX_THREADS: ConfigParam = ConfigParam::int_param(
    "noxu.evictor.maxThreads",
    Some(1), // min
    None,    // max
    10,      // default
    true,    // mutable
    false,   // forReplication
);

/// Keep-alive time for idle evictor threads.
pub static EVICTOR_KEEP_ALIVE: ConfigParam = ConfigParam {
    name: "noxu.evictor.keepAlive",
    param_type: crate::param::ParamType::Duration,
    default: ParamValue::Duration(Duration::from_secs(10 * 60)), // 10 min
    min: Some(ParamValue::Duration(Duration::ZERO)),
    max: Some(ParamValue::Duration(Duration::from_secs(24 * 3600))),
    mutable: true,
    for_replication: false,
};

/// Timeout waiting for evictor pool termination at shutdown.
pub static EVICTOR_TERMINATE_TIMEOUT: ConfigParam = ConfigParam {
    name: "noxu.env.terminateTimeout",
    param_type: crate::param::ParamType::Duration,
    default: ParamValue::Duration(Duration::from_secs(10)),
    min: Some(ParamValue::Duration(Duration::ZERO)),
    max: Some(ParamValue::Duration(Duration::from_secs(75 * 60))),
    mutable: true,
    for_replication: false,
};

/// If true, allow BIN-delta eviction.
pub static EVICTOR_ALLOW_BIN_DELTAS: ConfigParam = ConfigParam::bool_param(
    "noxu.evictor.allowBinDeltas",
    true,  // default
    false, // mutable
    false, // forReplication
);

/// If true, mutate BINs to BIN-deltas during eviction (internal/debug only).
pub static EVICTOR_MUTATE_BINS: ConfigParam = ConfigParam::bool_param(
    "noxu.evictor.mutateBins",
    true,  // default
    false, // mutable
    false, // forReplication
);

/// Number of bytes to evict in each eviction pass.
pub static EVICTOR_EVICT_BYTES: ConfigParam = ConfigParam::long_param(
    "noxu.evictor.evictBytes",
    Some(1024), // min
    None,       // max
    524_288,    // default: 512 KB
    false,      // mutable
    false,      // forReplication
);

/// Critical percentage above which eviction is more aggressive.
pub static EVICTOR_CRITICAL_PERCENTAGE: ConfigParam = ConfigParam::int_param(
    "noxu.evictor.criticalPercentage",
    Some(0),    // min
    Some(1000), // max
    0,          // default: disabled
    false,      // mutable
    false,      // forReplication
);

/// If true, use a 2-level LRU: dirty nodes are moved to a second level.
pub static EVICTOR_USE_DIRTY_LRU: ConfigParam = ConfigParam::bool_param(
    "noxu.evictor.useDirtyLRU",
    true,  // default
    false, // mutable
    false, // forReplication
);

/// Number of LRU lists used by the evictor (for concurrency).
pub static EVICTOR_N_LRU_LISTS: ConfigParam = ConfigParam::int_param(
    "noxu.evictor.nLRULists",
    Some(1),  // min
    Some(32), // max
    4,        // default
    false,    // mutable
    false,    // forReplication
);

/// If true, yield the thread after each evictor step (for testing).
pub static EVICTOR_FORCED_YIELD: ConfigParam = ConfigParam::bool_param(
    "noxu.evictor.forcedYield",
    false, // default
    false, // mutable
    false, // forReplication
);

// =========================================================================
// Off-heap cache parameters
// =========================================================================

/// Number of bytes to evict from the off-heap cache per pass.
pub static OFFHEAP_EVICT_BYTES: ConfigParam = ConfigParam::long_param(
    "noxu.offHeap.evictBytes",
    Some(1024),       // min
    None,             // max
    50 * 1024 * 1024, // default: 50 MB
    false,            // mutable
    false,            // forReplication
);

/// If true, store checksums in off-heap cache entries.
pub static OFFHEAP_CHECKSUM: ConfigParam = ConfigParam::bool_param(
    "noxu.offHeap.checksum",
    false, // default
    false, // mutable
    false, // forReplication
);

/// Number of off-heap evictor core threads.
pub static OFFHEAP_CORE_THREADS: ConfigParam = ConfigParam::int_param(
    "noxu.offHeap.coreThreads",
    Some(0), // min
    None,    // max
    1,       // default
    true,    // mutable
    false,   // forReplication
);

/// Maximum number of off-heap evictor threads.
pub static OFFHEAP_MAX_THREADS: ConfigParam = ConfigParam::int_param(
    "noxu.offHeap.maxThreads",
    Some(1), // min
    None,    // max
    3,       // default
    true,    // mutable
    false,   // forReplication
);

/// Keep-alive time for idle off-heap evictor threads.
pub static OFFHEAP_KEEP_ALIVE: ConfigParam = ConfigParam {
    name: "noxu.offHeap.keepAlive",
    param_type: crate::param::ParamType::Duration,
    default: ParamValue::Duration(Duration::from_secs(10 * 60)), // 10 min
    min: Some(ParamValue::Duration(Duration::ZERO)),
    max: Some(ParamValue::Duration(Duration::from_secs(24 * 3600))),
    mutable: true,
    for_replication: false,
};

/// Number of LRU lists used by the off-heap evictor (for concurrency).
/// Note: reuses the same property name as EVICTOR_N_LRU_LISTS.
pub static OFFHEAP_N_LRU_LISTS: ConfigParam = ConfigParam::int_param(
    "noxu.evictor.nLRULists",
    Some(1),  // min
    Some(32), // max
    4,        // default
    false,    // mutable
    false,    // forReplication
);

// =========================================================================
// Checkpointer parameters
// =========================================================================

/// Number of bytes written between checkpoints. 0 means use time-based interval.
pub static CHECKPOINTER_BYTES_INTERVAL: ConfigParam = ConfigParam::long_param(
    "noxu.checkpointer.bytesInterval",
    Some(0),    // min
    None,       // max (Long.MAX_VALUE)
    20_000_000, // default: 20 MB
    false,      // mutable
    false,      // forReplication
);

/// Time between checkpoints. 0 means use bytes-based interval.
pub static CHECKPOINTER_WAKEUP_INTERVAL: ConfigParam = ConfigParam {
    name: "noxu.checkpointer.wakeupInterval",
    param_type: crate::param::ParamType::Duration,
    default: ParamValue::Duration(Duration::ZERO),
    min: Some(ParamValue::Duration(Duration::ZERO)),
    max: Some(ParamValue::Duration(Duration::from_secs(75 * 60))),
    mutable: false,
    for_replication: false,
};

/// Number of times to retry on lock conflict during checkpoint.
pub static CHECKPOINTER_DEADLOCK_RETRY: ConfigParam = ConfigParam::int_param(
    "noxu.checkpointer.deadlockRetry",
    Some(0), // min
    None,    // max
    3,       // default
    false,   // mutable
    false,   // forReplication
);

/// If true, the checkpointer runs at high priority (flushes more aggressively).
pub static CHECKPOINTER_HIGH_PRIORITY: ConfigParam = ConfigParam::bool_param(
    "noxu.checkpointer.highPriority",
    false, // default
    true,  // mutable
    false, // forReplication
);

// =========================================================================
// Cleaner parameters
// =========================================================================

/// Minimum utilization percentage below which log files become candidates for cleaning.
pub static CLEANER_MIN_UTILIZATION: ConfigParam = ConfigParam::int_param(
    "noxu.cleaner.minUtilization",
    Some(0),  // min
    Some(90), // max
    50,       // default
    true,     // mutable
    false,    // forReplication
);

/// Minimum utilization of individual log files; files below this are cleaned.
pub static CLEANER_MIN_FILE_UTILIZATION: ConfigParam = ConfigParam::int_param(
    "noxu.cleaner.minFileUtilization",
    Some(0),  // min
    Some(50), // max
    5,        // default
    true,     // mutable
    false,    // forReplication
);

/// Number of bytes written between cleaner wakeups. 0 means time-based.
pub static CLEANER_BYTES_INTERVAL: ConfigParam = ConfigParam::long_param(
    "noxu.cleaner.bytesInterval",
    Some(0), // min
    None,    // max
    0,       // default: use time-based wakeup
    true,    // mutable
    false,   // forReplication
);

/// Wakeup interval for the cleaner daemon.
pub static CLEANER_WAKEUP_INTERVAL: ConfigParam = ConfigParam {
    name: "noxu.cleaner.wakeupInterval",
    param_type: crate::param::ParamType::Duration,
    default: ParamValue::Duration(Duration::from_secs(10)),
    min: Some(ParamValue::Duration(Duration::ZERO)),
    max: Some(ParamValue::Duration(Duration::from_secs(75 * 60))),
    mutable: true,
    for_replication: false,
};

/// If true, fetch the obsolete LN size before cleaning (for accurate stats).
pub static CLEANER_FETCH_OBSOLETE_SIZE: ConfigParam = ConfigParam::bool_param(
    "noxu.cleaner.fetchObsoleteSize",
    false, // default
    true,  // mutable
    false, // forReplication
);

/// Number of times to retry a cleaner operation on lock conflict.
pub static CLEANER_DEADLOCK_RETRY: ConfigParam = ConfigParam::int_param(
    "noxu.cleaner.deadlockRetry",
    Some(0), // min
    None,    // max
    3,       // default
    true,    // mutable
    false,   // forReplication
);

/// Lock timeout for cleaner lock acquisitions.
pub static CLEANER_LOCK_TIMEOUT: ConfigParam = ConfigParam {
    name: "noxu.cleaner.lockTimeout",
    param_type: crate::param::ParamType::Duration,
    default: ParamValue::Duration(Duration::from_millis(500)),
    min: Some(ParamValue::Duration(Duration::ZERO)),
    max: Some(ParamValue::Duration(Duration::from_secs(75 * 60))),
    mutable: true,
    for_replication: false,
};

/// If true, delete cleaned log files (expunge). If false, rename them.
pub static CLEANER_REMOVE: ConfigParam = ConfigParam::bool_param(
    "noxu.cleaner.expunge",
    true,  // default
    true,  // mutable
    false, // forReplication
);

/// If true, move cleaned files to a "deleted" subdirectory instead of deleting them.
pub static CLEANER_USE_DELETED_DIR: ConfigParam = ConfigParam::bool_param(
    "noxu.cleaner.useDeletedDir",
    false, // default
    true,  // mutable
    false, // forReplication
);

/// Minimum age of a log file in number of files before it can be cleaned.
pub static CLEANER_MIN_AGE: ConfigParam = ConfigParam::int_param(
    "noxu.cleaner.minAge",
    Some(1),    // min
    Some(1000), // max
    2,          // default
    true,       // mutable
    false,      // forReplication
);

/// Maximum number of log files cleaned in one cleaner run. 0 means unlimited.
pub static CLEANER_MAX_BATCH_FILES: ConfigParam = ConfigParam::int_param(
    "noxu.cleaner.maxBatchFiles",
    Some(0),      // min
    Some(100000), // max
    0,            // default: unlimited
    true,         // mutable
    false,        // forReplication
);

/// Read buffer size for the cleaner. 0 means use LOG_ITERATOR_READ_SIZE.
pub static CLEANER_READ_SIZE: ConfigParam = ConfigParam::int_param(
    "noxu.cleaner.readSize",
    Some(128), // min
    None,      // max
    0,         // default: use log iterator size
    true,      // mutable
    false,     // forReplication
);

/// DiskOrderedScan producer queue timeout.
pub static DOS_PRODUCER_QUEUE_TIMEOUT: ConfigParam = ConfigParam {
    name: "noxu.env.diskOrderedScanLockTimeout",
    param_type: crate::param::ParamType::Duration,
    default: ParamValue::Duration(Duration::from_secs(10)),
    min: Some(ParamValue::Duration(Duration::ZERO)),
    max: Some(ParamValue::Duration(Duration::from_secs(75 * 60))),
    mutable: true,
    for_replication: false,
};

/// If true, the cleaner tracks and stores detail info for cheaper cleaning.
pub static CLEANER_TRACK_DETAIL: ConfigParam = ConfigParam::bool_param(
    "noxu.cleaner.trackDetail",
    true,  // default
    false, // mutable
    false, // forReplication
);

/// If true, data expires gradually to prevent cleaning spikes at hour/day boundaries.
pub static CLEANER_GRADUAL_EXPIRATION: ConfigParam = ConfigParam::bool_param(
    "noxu.cleaner.gradualExpiration",
    true,  // default
    true,  // mutable
    false, // forReplication
);

/// Utilization gap between min and max before triggering two-pass cleaning.
pub static CLEANER_TWO_PASS_GAP: ConfigParam = ConfigParam::int_param(
    "noxu.cleaner.twoPassGap",
    Some(1),   // min
    Some(100), // max
    10,        // default
    true,      // mutable
    false,     // forReplication
);

/// Utilization threshold for two-pass cleaning. 0 = CLEANER_MIN_UTILIZATION - 5.
pub static CLEANER_TWO_PASS_THRESHOLD: ConfigParam = ConfigParam::int_param(
    "noxu.cleaner.twoPassThreshold",
    Some(0),   // min
    Some(100), // max
    0,         // default
    true,      // mutable
    false,     // forReplication
);

/// Maximum percentage of cache used for cleaner detail tracking.
pub static CLEANER_DETAIL_MAX_MEMORY_PERCENTAGE: ConfigParam =
    ConfigParam::int_param(
        "noxu.cleaner.detailMaxMemoryPercentage",
        Some(1),  // min
        Some(90), // max
        2,        // default
        true,     // mutable
        false,    // forReplication
    );

/// If true, discard potentially invalid cleaner detail info from old log formats.
pub static CLEANER_RMW_FIX: ConfigParam = ConfigParam::bool_param(
    "noxu.cleaner.rmwFix",
    true,  // default
    false, // mutable
    false, // forReplication
);

/// Comma-separated list of log files to force-clean (by file number range).
pub static CLEANER_FORCE_CLEAN_FILES: ConfigParam = ConfigParam::string_param(
    "noxu.cleaner.forceCleanFiles",
    "",    // default: empty (no forced files)
    true,  // mutable
    false, // forReplication
);

/// Log version to upgrade to during cleaning. 0 = no upgrade. -1 = current.
pub static CLEANER_UPGRADE_TO_LOG_VERSION: ConfigParam = ConfigParam::int_param(
    "noxu.cleaner.upgradeToLogVersion",
    Some(-1), // min
    None,     // max
    0,        // default
    false,    // mutable
    false,    // forReplication
);

/// Number of cleaner threads.
pub static CLEANER_THREADS: ConfigParam = ConfigParam::int_param(
    "noxu.cleaner.threads",
    Some(1), // min
    None,    // max
    1,       // default
    true,    // mutable
    false,   // forReplication
);

/// Number of LookAheadCache entries for the cleaner.
pub static CLEANER_LOOK_AHEAD_CACHE_SIZE: ConfigParam = ConfigParam::int_param(
    "noxu.cleaner.lookAheadCacheSize",
    Some(0), // min
    None,    // max
    8192,    // default: 8 KB
    true,    // mutable
    false,   // forReplication
);

/// Retained to avoid parse errors in old `noxu.properties` files.
///
/// Deprecated — this parameter has no effect.
#[deprecated(
    since = "2.4.1",
    note = "this parameter is retained only for parse compatibility; it has no effect"
)]
pub static CLEANER_BACKGROUND_PROACTIVE_MIGRATION: ConfigParam =
    ConfigParam::bool_param(
        "noxu.cleaner.backgroundProactiveMigration",
        false, // default
        true,  // mutable
        false, // forReplication
    );

// =========================================================================
// Lock/Transaction parameters
// =========================================================================

/// Number of lock tables (for lock striping).
pub static LOCK_N_LOCK_TABLES: ConfigParam = ConfigParam::int_param(
    "noxu.lock.nLockTables",
    Some(1),     // min
    Some(32767), // max
    1,           // default
    false,       // mutable
    false,       // forReplication
);

/// Lock timeout duration. 0 means no timeout.
pub static LOCK_TIMEOUT: ConfigParam = ConfigParam {
    name: "noxu.lock.timeout",
    param_type: crate::param::ParamType::Duration,
    default: ParamValue::Duration(Duration::from_millis(500)),
    min: Some(ParamValue::Duration(Duration::ZERO)),
    max: Some(ParamValue::Duration(Duration::from_secs(75 * 60))),
    mutable: false,
    for_replication: false,
};

/// If true, enable automatic deadlock detection.
pub static LOCK_DEADLOCK_DETECT: ConfigParam = ConfigParam::bool_param(
    "noxu.lock.deadlockDetect",
    true,  // default
    true,  // mutable
    false, // forReplication
);

/// Delay before deadlock detection begins after a lock conflict.
pub static LOCK_DEADLOCK_DETECT_DELAY: ConfigParam = ConfigParam {
    name: "noxu.lock.deadlockDetectDelay",
    param_type: crate::param::ParamType::Duration,
    default: ParamValue::Duration(Duration::ZERO),
    min: Some(ParamValue::Duration(Duration::ZERO)),
    max: Some(ParamValue::Duration(Duration::from_secs(75 * 60))),
    mutable: false,
    for_replication: false,
};

/// If true, throw legacy-style lock exceptions (LockException, etc.).
pub static LOCK_OLD_LOCK_EXCEPTIONS: ConfigParam = ConfigParam::bool_param(
    "noxu.lock.oldLockExceptions",
    false, // default
    false, // mutable
    false, // forReplication
);

/// Transaction timeout duration. 0 means no timeout.
pub static TXN_TIMEOUT: ConfigParam = ConfigParam {
    name: "noxu.txn.timeout",
    param_type: crate::param::ParamType::Duration,
    default: ParamValue::Duration(Duration::ZERO),
    min: Some(ParamValue::Duration(Duration::ZERO)),
    max: Some(ParamValue::Duration(Duration::from_secs(75 * 60))),
    mutable: false,
    for_replication: false,
};

/// If true, all transactions use serializable isolation by default.
pub static TXN_SERIALIZABLE_ISOLATION: ConfigParam = ConfigParam::bool_param(
    "noxu.txn.serializableIsolation",
    false, // default
    false, // mutable
    false, // forReplication
);

/// If true, include a stack trace in deadlock exception messages.
pub static TXN_DEADLOCK_STACK_TRACE: ConfigParam = ConfigParam::bool_param(
    "noxu.txn.deadlockStackTrace",
    false, // default
    true,  // mutable
    false, // forReplication
);

/// If true, dump all lock tables when a deadlock occurs.
pub static TXN_DUMP_LOCKS: ConfigParam = ConfigParam::bool_param(
    "noxu.txn.dumpLocks",
    false, // default
    true,  // mutable
    false, // forReplication
);

/// Default durability policy (as a string). Null means use sync defaults.
pub static TXN_DURABILITY: ConfigParam = ConfigParam::string_param(
    "noxu.txn.durability",
    "",    // default: empty (use sync defaults)
    true,  // mutable
    false, // forReplication
);

// =========================================================================
// Stats / startup parameters
// =========================================================================

/// If environment startup exceeds this duration, startup statistics are logged.
pub static STARTUP_DUMP_THRESHOLD: ConfigParam = ConfigParam {
    name: "noxu.env.startupThreshold",
    param_type: crate::param::ParamType::Duration,
    default: ParamValue::Duration(Duration::from_secs(5 * 60)), // 5 min
    min: Some(ParamValue::Duration(Duration::ZERO)),
    max: Some(ParamValue::Duration(Duration::from_secs(24 * 3600))),
    mutable: false,
    for_replication: false,
};

/// If true, collect statistics in a stats CSV file.
pub static STATS_COLLECT: ConfigParam = ConfigParam::bool_param(
    "noxu.stats.collect",
    true,  // default
    true,  // mutable
    false, // forReplication
);

/// Number of rows per stats file before rotating to a new file.
pub static STATS_FILE_ROW_COUNT: ConfigParam = ConfigParam::int_param(
    "noxu.stats.file.row.count",
    Some(2), // min
    None,    // max
    1440,    // default
    true,    // mutable
    false,   // forReplication
);

/// Maximum number of stats files to retain.
pub static STATS_MAX_FILES: ConfigParam = ConfigParam::int_param(
    "noxu.stats.max.files",
    Some(1), // min
    None,    // max
    10,      // default
    true,    // mutable
    false,   // forReplication
);

/// Interval at which statistics are collected.
pub static STATS_COLLECT_INTERVAL: ConfigParam = ConfigParam {
    name: "noxu.stats.collect.interval",
    param_type: crate::param::ParamType::Duration,
    default: ParamValue::Duration(Duration::from_secs(60)), // 1 min
    min: Some(ParamValue::Duration(Duration::ZERO)),
    max: Some(ParamValue::Duration(Duration::from_secs(24 * 3600))),
    mutable: true,
    for_replication: false,
};

/// Directory for stats files (empty string = environment home directory).
pub static STATS_FILE_DIRECTORY: ConfigParam = ConfigParam::string_param(
    "noxu.stats.file.directory",
    "",    // default: empty (use env home dir)
    false, // mutable
    false, // forReplication
);

// =========================================================================
// Logging parameters
// =========================================================================

/// If true, exceptions and critical events are written to the .jdb log files.
pub static LOGGING_DBLOG: ConfigParam = ConfigParam::bool_param(
    "noxu.env.logTrace",
    true,  // default
    false, // mutable
    false, // forReplication
);

/// Log level for the console (stdout) logging handler.
pub static CONSOLE_HANDLER_LEVEL: ConfigParam = ConfigParam::string_param(
    "noxu.consoleHandler.level",
    "OFF", // default
    true,  // mutable
    false, // forReplication
);

/// Log level for the file logging handler.
pub static FILE_HANDLER_LEVEL: ConfigParam = ConfigParam::string_param(
    "noxu.fileHandler.level",
    "INFO", // default
    true,   // mutable
    false,  // forReplication
);

// =========================================================================
// Deprecated / compat parameters
// Retained to avoid errors when old noxu.properties files are used.
// =========================================================================

/// Deprecated — utilisation adjustments are no longer needed because LN log sizes
/// are stored directly in the B-tree.
#[deprecated(
    since = "2.4.1",
    note = "utilisation adjustments are always applied automatically; this flag has no effect"
)]
pub static CLEANER_ADJUST_UTILIZATION: ConfigParam = ConfigParam::bool_param(
    "noxu.cleaner.adjustUtilization",
    false, // default
    true,  // mutable
    false, // forReplication
);

/// Retained to avoid parse errors in old `noxu.properties` files.
///
/// Deprecated — this parameter has no effect.
#[deprecated(
    since = "2.4.1",
    note = "this parameter is retained only for parse compatibility; it has no effect"
)]
pub static CLEANER_FOREGROUND_PROACTIVE_MIGRATION: ConfigParam =
    ConfigParam::bool_param(
        "noxu.cleaner.foregroundProactiveMigration",
        false, // default
        true,  // mutable
        false, // forReplication
    );

/// Retained to avoid parse errors in old `noxu.properties` files.
///
/// Deprecated — this parameter has no effect.
#[deprecated(
    since = "2.4.1",
    note = "this parameter is retained only for parse compatibility; it has no effect"
)]
pub static CLEANER_LAZY_MIGRATION: ConfigParam = ConfigParam::bool_param(
    "noxu.cleaner.lazyMigration",
    false, // default
    true,  // mutable
    false, // forReplication
);

/// If true, compress (remove empty slots from) the root of the B-tree
/// during compressor runs. Normally only non-root INs are compressed.
pub static COMPRESSOR_PURGE_ROOT: ConfigParam = ConfigParam::bool_param(
    "noxu.compressor.purgeRoot",
    false, // default
    false, // mutable
    false, // forReplication
);

/// Deprecated — replaced by [`EVICTOR_EVICT_BYTES`].
///
/// The number of nodes per evictor scan is now derived from the bytes-to-evict target.
#[deprecated(since = "2.4.1", note = "use EVICTOR_EVICT_BYTES instead")]
pub static EVICTOR_NODES_PER_SCAN: ConfigParam = ConfigParam::int_param(
    "noxu.evictor.nodesPerScan",
    Some(1),    // min
    Some(1000), // max
    10,         // default
    false,      // mutable
    false,      // forReplication
);

/// Cache eviction algorithm: "lru" (default) plus, under the
/// `experimental-eviction-policies` feature, "clock" | "arc" | "car" |
/// "lirs" | "coolhot".
///
/// Defaults to "lru" — the JE-faithful policy (JE's `Evictor` /
/// `LRUEvictor`), which is the well-understood, benchmark-validated default.
/// The scan-resistant alternatives are experimental (gated behind the
/// `experimental-eviction-policies` cargo feature) and, when that feature is
/// off, fall back to LRU with a warning.  Wired through
/// `DbiEnvConfig.evictor_algorithm` to `Evictor::with_algorithm` (both
/// primary and scan policy slots).  See
/// `docs/src/reference/eviction-policies.md`.
pub static EVICTOR_ALGORITHM: ConfigParam = ConfigParam::string_param(
    "noxu.evictor.algorithm",
    "lru", // default (JE-faithful; see policies::lru)
    false, // mutable (policy is fixed at env-open)
    false, // forReplication
);

/// Deprecated — per-pass deadlock retry count is no longer configurable.
///
/// The evictor thread pool handles retry scheduling automatically.
#[deprecated(
    since = "2.4.1",
    note = "the evictor thread pool handles retries automatically; this parameter has no effect"
)]
pub static EVICTOR_DEADLOCK_RETRY: ConfigParam = ConfigParam::int_param(
    "noxu.evictor.deadlockRetry",
    Some(0), // min
    None,    // max
    3,       // default
    false,   // mutable
    false,   // forReplication
);

/// Deprecated — cache eviction policy is always multi-queue; LRU-only mode is not supported.
#[deprecated(
    since = "2.4.1",
    note = "cache eviction is always multi-queue; this flag has no effect"
)]
pub static EVICTOR_LRU_ONLY: ConfigParam = ConfigParam::bool_param(
    "noxu.evictor.lruOnly",
    true,  // default
    false, // mutable
    false, // forReplication
);

/// Deprecated — this parameter has no effect in Noxu DB.
#[deprecated(
    since = "2.4.1",
    note = "Java NIO direct buffers are not applicable to Noxu DB; this parameter has no effect"
)]
pub static LOG_DIRECT_NIO: ConfigParam = ConfigParam::bool_param(
    "noxu.log.directNIO",
    false, // default
    false, // mutable
    false, // forReplication
);

/// Deprecated — this parameter has no effect in Noxu DB.
#[deprecated(
    since = "2.4.1",
    note = "Java NIO chunked writes are not applicable to Noxu DB; this parameter has no effect"
)]
pub static LOG_CHUNKED_NIO: ConfigParam = ConfigParam::long_param(
    "noxu.log.chunkedNIO",
    Some(0),       // min
    Some(1 << 26), // max: 64 MB
    0,             // default: no chunking
    false,         // mutable
    false,         // forReplication
);

// =========================================================================
// Extended-fork parameters (from the kvmain fork archive at _/nosql/)
// =========================================================================

/// Amount of disk space to reserve for internal use, in bytes.
/// When non-zero, this limit is applied in addition to MAX_DISK and FREE_DISK.
pub static RESERVED_DISK: ConfigParam = ConfigParam::long_param(
    "noxu.reservedDisk",
    Some(0), // min
    None,    // max
    0,       // default: disabled
    true,    // mutable
    false,   // forReplication
);

/// If true, operates in test mode — behavior may be modified from normal operation.
pub static TEST_MODE: ConfigParam = ConfigParam::bool_param(
    "noxu.testMode",
    false, // default
    true,  // mutable
    false, // forReplication
);

/// Lock timeout used during network restore operations.
pub static ENV_NETWORK_RESTORE_LOCK_TIMEOUT: ConfigParam = ConfigParam {
    name: "noxu.env.networkRestoreLockTimeout",
    param_type: crate::param::ParamType::Duration,
    default: ParamValue::Duration(Duration::from_secs(2 * 60)), // 2 min
    min: Some(ParamValue::Duration(Duration::ZERO)),
    max: Some(ParamValue::Duration(Duration::from_secs(75 * 60))),
    mutable: false,
    for_replication: false,
};

/// If true, a checksum error in the log is treated as fatal (environment is closed).
pub static LOG_CHECKSUM_FATAL: ConfigParam = ConfigParam::bool_param(
    "noxu.log.checksumFatal",
    true,  // default
    false, // mutable
    false, // forReplication
);

/// If true, secondary index integrity errors are treated as fatal.
pub static TREE_SECONDARY_INTEGRITY_FATAL: ConfigParam =
    ConfigParam::bool_param(
        "noxu.tree.secondaryIntegrityFatal",
        true,  // default
        false, // mutable
        false, // forReplication
    );

/// Returns all defined configuration parameters, including deprecated compatibility entries.
#[allow(deprecated)]
pub fn all_params() -> Vec<&'static ConfigParam> {
    vec![
        // Memory
        &MAX_MEMORY,
        &MAX_MEMORY_PERCENT,
        &SHARED_CACHE,
        &MAX_DISK,
        &FREE_DISK,
        &MAX_OFF_HEAP_MEMORY,
        // Environment
        &ENV_COMPARATORS_REQUIRED,
        &ENV_RECOVERY,
        &ENV_RECOVERY_FORCE_CHECKPOINT,
        &ENV_RECOVERY_FORCE_NEW_FILE,
        &HALT_ON_COMMIT_AFTER_CHECKSUMEXCEPTION,
        &ENV_IS_TRANSACTIONAL,
        &ENV_IS_LOCKING,
        &ENV_IS_READ_ONLY,
        &ENV_FAIR_LATCHES,
        &ENV_SHARED_LATCHES,
        &ENV_SETUP_LOGGER,
        &ENV_LATCH_TIMEOUT,
        &ENV_TTL_CLOCK_TOLERANCE,
        &ENV_TTL_MAX_TXN_TIME,
        &ENV_TTL_LN_PURGE_DELAY,
        &ENV_EXPOSE_USER_DATA,
        &ENV_DB_EVICTION,
        &ENV_DUP_CONVERT_PRELOAD_ALL,
        &ADLER32_CHUNK_SIZE,
        &ENV_CHECK_LEAKS,
        &ENV_FORCED_YIELD,
        &ENV_EXPIRATION_ENABLED,
        &ENV_DB_CACHE_CLEAR_COUNT,
        // Daemons
        &ENV_RUN_IN_COMPRESSOR,
        &ENV_RUN_EVICTOR,
        &ENV_RUN_OFFHEAP_EVICTOR,
        &ENV_RUN_CLEANER,
        &ENV_RUN_CHECKPOINTER,
        &ENV_RUN_VERIFIER,
        // Background rate limiting
        &ENV_BACKGROUND_READ_LIMIT,
        &ENV_BACKGROUND_WRITE_LIMIT,
        &ENV_BACKGROUND_SLEEP_INTERVAL,
        // Log
        &LOG_MEM_SIZE,
        &LOG_NUM_BUFFERS,
        &LOG_BUFFER_SIZE,
        &LOG_FAULT_READ_SIZE,
        &LOG_ITERATOR_READ_SIZE,
        &LOG_ITERATOR_MAX_SIZE,
        &LOG_FILE_MAX,
        &LOG_N_DATA_DIRECTORIES,
        &LOG_CHECKSUM_READ,
        &LOG_VERIFY_CHECKSUMS,
        &LOG_MEM_ONLY,
        &LOG_FILE_CACHE_SIZE,
        &LOG_FILE_WARM_UP_SIZE,
        &LOG_FILE_WARM_UP_BUF_SIZE,
        &LOG_DETECT_FILE_DELETE,
        &LOG_DETECT_FILE_DELETE_INTERVAL,
        &LOG_FSYNC_TIMEOUT,
        &LOG_FSYNC_TIME_LIMIT,
        &LOG_GROUP_COMMIT_INTERVAL,
        &LOG_GROUP_COMMIT_THRESHOLD,
        &LOG_FSYNC_MAX_LEADERS,
        &LOG_FSYNC_ADAPTIVE_LEADERS,
        &LOG_FSYNC_ADAPTIVE_TRIGGER,
        &LOG_CONSOLIDATION_ARRAY,
        &LOG_FLUSH_SYNC_INTERVAL,
        &LOG_FLUSH_NO_SYNC_INTERVAL,
        &LOG_USE_ODSYNC,
        &LOG_USE_NIO,
        &LOG_USE_WRITE_QUEUE,
        &LOG_WRITE_QUEUE_SIZE,
        &LOG_DEFERREDWRITE_TEMP,
        &OLD_REP_RUN_LOG_FLUSH_TASK,
        &OLD_REP_LOG_FLUSH_TASK_INTERVAL,
        // Verification
        &VERIFY_SCHEDULE,
        &VERIFY_MAX_TARDINESS,
        &VERIFY_BTREE,
        &VERIFY_LOG_READ_DELAY,
        &VERIFY_LOG,
        &VERIFY_SECONDARIES,
        &VERIFY_DATA_RECORDS,
        &VERIFY_OBSOLETE_RECORDS,
        &VERIFY_BTREE_BATCH_SIZE,
        &VERIFY_BTREE_BATCH_DELAY,
        // Tree
        &NODE_MAX_ENTRIES,
        &NODE_DUP_TREE_MAX_ENTRIES,
        &TREE_MAX_EMBEDDED_LN,
        &TREE_BIN_DELTA,
        &BIN_DELTA_BLIND_OPS,
        &BIN_DELTA_BLIND_PUTS,
        &TREE_MIN_MEMORY,
        &TREE_COMPACT_MAX_KEY_LENGTH,
        // Compressor
        &COMPRESSOR_WAKEUP_INTERVAL,
        &COMPRESSOR_DEADLOCK_RETRY,
        &COMPRESSOR_LOCK_TIMEOUT,
        // Evictor
        &EVICTOR_CORE_THREADS,
        &EVICTOR_MAX_THREADS,
        &EVICTOR_KEEP_ALIVE,
        &EVICTOR_TERMINATE_TIMEOUT,
        &EVICTOR_ALLOW_BIN_DELTAS,
        &EVICTOR_MUTATE_BINS,
        &EVICTOR_EVICT_BYTES,
        &EVICTOR_CRITICAL_PERCENTAGE,
        &EVICTOR_USE_DIRTY_LRU,
        &EVICTOR_N_LRU_LISTS,
        &EVICTOR_FORCED_YIELD,
        // Off-heap cache
        &OFFHEAP_EVICT_BYTES,
        &OFFHEAP_CHECKSUM,
        &OFFHEAP_CORE_THREADS,
        &OFFHEAP_MAX_THREADS,
        &OFFHEAP_KEEP_ALIVE,
        &OFFHEAP_N_LRU_LISTS,
        // Checkpointer
        &CHECKPOINTER_BYTES_INTERVAL,
        &CHECKPOINTER_WAKEUP_INTERVAL,
        &CHECKPOINTER_DEADLOCK_RETRY,
        &CHECKPOINTER_HIGH_PRIORITY,
        // Cleaner
        &CLEANER_MIN_UTILIZATION,
        &CLEANER_MIN_FILE_UTILIZATION,
        &CLEANER_BYTES_INTERVAL,
        &CLEANER_WAKEUP_INTERVAL,
        &CLEANER_FETCH_OBSOLETE_SIZE,
        &CLEANER_DEADLOCK_RETRY,
        &CLEANER_LOCK_TIMEOUT,
        &CLEANER_REMOVE,
        &CLEANER_USE_DELETED_DIR,
        &CLEANER_MIN_AGE,
        &CLEANER_MAX_BATCH_FILES,
        &CLEANER_READ_SIZE,
        &DOS_PRODUCER_QUEUE_TIMEOUT,
        &CLEANER_TRACK_DETAIL,
        &CLEANER_GRADUAL_EXPIRATION,
        &CLEANER_TWO_PASS_GAP,
        &CLEANER_TWO_PASS_THRESHOLD,
        &CLEANER_DETAIL_MAX_MEMORY_PERCENTAGE,
        &CLEANER_RMW_FIX,
        &CLEANER_FORCE_CLEAN_FILES,
        &CLEANER_UPGRADE_TO_LOG_VERSION,
        &CLEANER_THREADS,
        &CLEANER_LOOK_AHEAD_CACHE_SIZE,
        &CLEANER_BACKGROUND_PROACTIVE_MIGRATION,
        // Deprecated compat cleaner
        &CLEANER_ADJUST_UTILIZATION,
        &CLEANER_FOREGROUND_PROACTIVE_MIGRATION,
        &CLEANER_LAZY_MIGRATION,
        // Compressor
        &COMPRESSOR_PURGE_ROOT,
        &EVICTOR_ALGORITHM,
        // Deprecated compat evictor
        &EVICTOR_NODES_PER_SCAN,
        &EVICTOR_DEADLOCK_RETRY,
        &EVICTOR_LRU_ONLY,
        // Deprecated compat log
        &LOG_DIRECT_NIO,
        &LOG_CHUNKED_NIO,
        // Lock/Transaction
        &LOCK_N_LOCK_TABLES,
        &LOCK_TIMEOUT,
        &LOCK_DEADLOCK_DETECT,
        &LOCK_DEADLOCK_DETECT_DELAY,
        &LOCK_OLD_LOCK_EXCEPTIONS,
        &TXN_TIMEOUT,
        &TXN_SERIALIZABLE_ISOLATION,
        &TXN_DEADLOCK_STACK_TRACE,
        &TXN_DUMP_LOCKS,
        &TXN_DURABILITY,
        // Stats / startup
        &STARTUP_DUMP_THRESHOLD,
        &STATS_COLLECT,
        &STATS_FILE_ROW_COUNT,
        &STATS_MAX_FILES,
        &STATS_COLLECT_INTERVAL,
        &STATS_FILE_DIRECTORY,
        // Logging
        &LOGGING_DBLOG,
        &CONSOLE_HANDLER_LEVEL,
        &FILE_HANDLER_LEVEL,
        // extended-fork-specific
        &RESERVED_DISK,
        &TEST_MODE,
        &ENV_NETWORK_RESTORE_LOCK_TIMEOUT,
        &LOG_CHECKSUM_FATAL,
        &TREE_SECONDARY_INTEGRITY_FATAL,
    ]
}

#[cfg(test)]
#[allow(deprecated)]
mod tests {
    use super::*;
    use std::time::Duration;

    // -----------------------------------------------------------------------
    // Verify that param name strings are correct.
    // These are regression tests — if a name is wrong, lookups in
    // je.properties files will silently fail.
    // -----------------------------------------------------------------------

    #[test]
    fn test_param_names_correct() {
        // Memory
        assert_eq!(MAX_MEMORY.name, "noxu.maxMemory");
        assert_eq!(MAX_MEMORY_PERCENT.name, "noxu.maxMemoryPercent");
        assert_eq!(SHARED_CACHE.name, "noxu.sharedCache");
        assert_eq!(MAX_DISK.name, "noxu.maxDisk");
        assert_eq!(FREE_DISK.name, "noxu.freeDisk");
        assert_eq!(MAX_OFF_HEAP_MEMORY.name, "noxu.maxOffHeapMemory");

        // HALT: uses "noxu.haltOnCommitAfterChecksumException" (no "env." prefix)
        assert_eq!(
            HALT_ON_COMMIT_AFTER_CHECKSUMEXCEPTION.name,
            "noxu.haltOnCommitAfterChecksumException"
        );

        // Log
        assert_eq!(LOG_MEM_SIZE.name, "noxu.log.totalBufferBytes");
        assert_eq!(LOG_FILE_MAX.name, "noxu.log.fileMax");
        assert_eq!(LOG_MEM_ONLY.name, "noxu.log.memOnly");
        assert_eq!(LOG_CHECKSUM_READ.name, "noxu.log.checksumRead");

        // Stats: uses dot-separated sub-keys
        assert_eq!(STATS_COLLECT.name, "noxu.stats.collect");
        assert_eq!(STATS_FILE_ROW_COUNT.name, "noxu.stats.file.row.count");
        assert_eq!(STATS_MAX_FILES.name, "noxu.stats.max.files");
        assert_eq!(STATS_COLLECT_INTERVAL.name, "noxu.stats.collect.interval");
        assert_eq!(STATS_FILE_DIRECTORY.name, "noxu.stats.file.directory");

        // Startup threshold: name is "noxu.env.startupThreshold" (not "Dump")
        assert_eq!(STARTUP_DUMP_THRESHOLD.name, "noxu.env.startupThreshold");

        // Evictor
        assert_eq!(EVICTOR_CORE_THREADS.name, "noxu.evictor.coreThreads");
        assert_eq!(EVICTOR_MAX_THREADS.name, "noxu.evictor.maxThreads");
        assert_eq!(EVICTOR_EVICT_BYTES.name, "noxu.evictor.evictBytes");
        assert_eq!(EVICTOR_N_LRU_LISTS.name, "noxu.evictor.nLRULists");

        // Off-heap: reuses "noxu.evictor.nLRULists" for OFFHEAP_N_LRU_LISTS too
        assert_eq!(OFFHEAP_N_LRU_LISTS.name, "noxu.evictor.nLRULists");

        // Cleaner
        assert_eq!(CLEANER_MIN_UTILIZATION.name, "noxu.cleaner.minUtilization");
        assert_eq!(
            CLEANER_MIN_FILE_UTILIZATION.name,
            "noxu.cleaner.minFileUtilization"
        );
        assert_eq!(CLEANER_THREADS.name, "noxu.cleaner.threads");
        assert_eq!(CLEANER_REMOVE.name, "noxu.cleaner.expunge");

        // Lock / Txn
        assert_eq!(LOCK_N_LOCK_TABLES.name, "noxu.lock.nLockTables");
        assert_eq!(LOCK_TIMEOUT.name, "noxu.lock.timeout");
        assert_eq!(TXN_TIMEOUT.name, "noxu.txn.timeout");

        // Checkpointer
        assert_eq!(
            CHECKPOINTER_BYTES_INTERVAL.name,
            "noxu.checkpointer.bytesInterval"
        );

        // extended-fork-specific
        assert_eq!(RESERVED_DISK.name, "noxu.reservedDisk");
        assert_eq!(TEST_MODE.name, "noxu.testMode");
        assert_eq!(LOG_CHECKSUM_FATAL.name, "noxu.log.checksumFatal");
        assert_eq!(
            TREE_SECONDARY_INTEGRITY_FATAL.name,
            "noxu.tree.secondaryIntegrityFatal"
        );
    }

    // -----------------------------------------------------------------------
    // Verify default values are correct
    // -----------------------------------------------------------------------

    #[test]
    fn test_defaults_match_je() {
        // Memory
        assert_eq!(MAX_MEMORY.default, ParamValue::Long(0));
        assert_eq!(MAX_MEMORY_PERCENT.default, ParamValue::Int(60));
        assert_eq!(MAX_DISK.default, ParamValue::Long(0));
        assert_eq!(FREE_DISK.default, ParamValue::Long(5_368_709_120));

        // Log
        assert_eq!(LOG_FILE_MAX.default, ParamValue::Long(10_000_000));
        assert_eq!(LOG_MEM_ONLY.default, ParamValue::Bool(false));
        assert_eq!(LOG_CHECKSUM_READ.default, ParamValue::Bool(true));
        assert_eq!(LOG_NUM_BUFFERS.default, ParamValue::Int(3));
        assert_eq!(LOG_BUFFER_SIZE.default, ParamValue::Int(1 << 20));
        assert_eq!(LOG_FILE_CACHE_SIZE.default, ParamValue::Int(100));
        assert_eq!(LOG_USE_WRITE_QUEUE.default, ParamValue::Bool(true));

        // Tree
        assert_eq!(NODE_MAX_ENTRIES.default, ParamValue::Int(128));
        assert_eq!(NODE_DUP_TREE_MAX_ENTRIES.default, ParamValue::Int(128));
        assert_eq!(TREE_MAX_EMBEDDED_LN.default, ParamValue::Int(16));
        assert_eq!(TREE_BIN_DELTA.default, ParamValue::Int(25));
        assert_eq!(TREE_MIN_MEMORY.default, ParamValue::Long(500 * 1024));
        assert_eq!(TREE_COMPACT_MAX_KEY_LENGTH.default, ParamValue::Int(16));

        // Evictor
        assert_eq!(EVICTOR_CORE_THREADS.default, ParamValue::Int(1));
        assert_eq!(EVICTOR_MAX_THREADS.default, ParamValue::Int(10));
        assert_eq!(EVICTOR_EVICT_BYTES.default, ParamValue::Long(524_288));
        assert_eq!(EVICTOR_N_LRU_LISTS.default, ParamValue::Int(4));
        assert_eq!(EVICTOR_CRITICAL_PERCENTAGE.default, ParamValue::Int(0));
        assert_eq!(EVICTOR_USE_DIRTY_LRU.default, ParamValue::Bool(true));
        assert_eq!(EVICTOR_ALLOW_BIN_DELTAS.default, ParamValue::Bool(true));
        assert_eq!(EVICTOR_MUTATE_BINS.default, ParamValue::Bool(true));

        // Off-heap
        assert_eq!(
            OFFHEAP_EVICT_BYTES.default,
            ParamValue::Long(50 * 1024 * 1024)
        );
        assert_eq!(OFFHEAP_CORE_THREADS.default, ParamValue::Int(1));
        assert_eq!(OFFHEAP_MAX_THREADS.default, ParamValue::Int(3));

        // Checkpointer
        assert_eq!(
            CHECKPOINTER_BYTES_INTERVAL.default,
            ParamValue::Long(20_000_000)
        );
        assert_eq!(
            CHECKPOINTER_WAKEUP_INTERVAL.default,
            ParamValue::Duration(Duration::ZERO)
        );
        assert_eq!(CHECKPOINTER_DEADLOCK_RETRY.default, ParamValue::Int(3));
        assert_eq!(CHECKPOINTER_HIGH_PRIORITY.default, ParamValue::Bool(false));

        // Cleaner
        assert_eq!(CLEANER_MIN_UTILIZATION.default, ParamValue::Int(50));
        assert_eq!(CLEANER_MIN_FILE_UTILIZATION.default, ParamValue::Int(5));
        assert_eq!(CLEANER_MIN_AGE.default, ParamValue::Int(2));
        assert_eq!(CLEANER_THREADS.default, ParamValue::Int(1));
        assert_eq!(
            CLEANER_LOOK_AHEAD_CACHE_SIZE.default,
            ParamValue::Int(8192)
        );
        assert_eq!(CLEANER_REMOVE.default, ParamValue::Bool(true));
        assert_eq!(CLEANER_TRACK_DETAIL.default, ParamValue::Bool(true));
        assert_eq!(CLEANER_GRADUAL_EXPIRATION.default, ParamValue::Bool(true));
        assert_eq!(
            CLEANER_DETAIL_MAX_MEMORY_PERCENTAGE.default,
            ParamValue::Int(2)
        );

        // Lock / Txn
        assert_eq!(LOCK_N_LOCK_TABLES.default, ParamValue::Int(1));
        assert_eq!(
            LOCK_TIMEOUT.default,
            ParamValue::Duration(Duration::from_millis(500))
        );
        assert_eq!(TXN_TIMEOUT.default, ParamValue::Duration(Duration::ZERO));
        assert_eq!(TXN_SERIALIZABLE_ISOLATION.default, ParamValue::Bool(false));
        assert_eq!(LOCK_DEADLOCK_DETECT.default, ParamValue::Bool(true));
        assert_eq!(LOCK_OLD_LOCK_EXCEPTIONS.default, ParamValue::Bool(false));

        // Env daemon flags
        assert_eq!(ENV_RUN_EVICTOR.default, ParamValue::Bool(true));
        assert_eq!(ENV_RUN_CLEANER.default, ParamValue::Bool(true));
        assert_eq!(ENV_RUN_CHECKPOINTER.default, ParamValue::Bool(true));
        assert_eq!(ENV_RUN_IN_COMPRESSOR.default, ParamValue::Bool(true));
        assert_eq!(ENV_RUN_VERIFIER.default, ParamValue::Bool(true));
        assert_eq!(ENV_IS_LOCKING.default, ParamValue::Bool(true));
        assert_eq!(ENV_IS_TRANSACTIONAL.default, ParamValue::Bool(false));
        assert_eq!(ENV_IS_READ_ONLY.default, ParamValue::Bool(false));

        // Stats
        assert_eq!(STATS_COLLECT.default, ParamValue::Bool(true));
        assert_eq!(STATS_FILE_ROW_COUNT.default, ParamValue::Int(1440));
        assert_eq!(STATS_MAX_FILES.default, ParamValue::Int(10));

        // Startup threshold ("noxu.env.startupThreshold", default 5 min)
        assert_eq!(
            STARTUP_DUMP_THRESHOLD.default,
            ParamValue::Duration(Duration::from_secs(5 * 60))
        );

        // extended-fork-specific defaults
        assert_eq!(RESERVED_DISK.default, ParamValue::Long(0));
        assert_eq!(TEST_MODE.default, ParamValue::Bool(false));
        assert_eq!(LOG_CHECKSUM_FATAL.default, ParamValue::Bool(true));
        assert_eq!(
            TREE_SECONDARY_INTEGRITY_FATAL.default,
            ParamValue::Bool(true)
        );
    }

    #[test]
    fn test_all_params_no_duplicates() {
        // Verify there are no duplicate param names in all_params() (except
        // the intentional OFFHEAP_N_LRU_LISTS / EVICTOR_N_LRU_LISTS alias).
        let params = all_params();
        let mut seen = std::collections::HashMap::new();
        for p in &params {
            let count = seen.entry(p.name).or_insert(0usize);
            *count += 1;
        }
        for (name, count) in &seen {
            // The only known intentional duplicate is "noxu.evictor.nLRULists"
            // (shared by EVICTOR_N_LRU_LISTS and OFFHEAP_N_LRU_LISTS).
            if *name == "noxu.evictor.nLRULists" {
                assert_eq!(
                    *count, 2,
                    "noxu.evictor.nLRULists should appear exactly twice"
                );
            } else {
                assert_eq!(*count, 1, "param '{}' is duplicated", name);
            }
        }
    }

    #[test]
    fn test_deprecated_compat_params_registered() {
        let params = all_params();
        let names: Vec<&str> = params.iter().map(|p| p.name).collect();
        assert!(
            names.contains(&"noxu.cleaner.adjustUtilization"),
            "CLEANER_ADJUST_UTILIZATION missing"
        );
        assert!(
            names.contains(&"noxu.cleaner.foregroundProactiveMigration"),
            "CLEANER_FOREGROUND_PROACTIVE_MIGRATION missing"
        );
        assert!(
            names.contains(&"noxu.cleaner.lazyMigration"),
            "CLEANER_LAZY_MIGRATION missing"
        );
        assert!(
            names.contains(&"noxu.compressor.purgeRoot"),
            "COMPRESSOR_PURGE_ROOT missing"
        );
        assert!(
            names.contains(&"noxu.evictor.nodesPerScan"),
            "EVICTOR_NODES_PER_SCAN missing"
        );
        assert!(
            names.contains(&"noxu.evictor.algorithm"),
            "EVICTOR_ALGORITHM missing"
        );
        assert!(
            names.contains(&"noxu.evictor.deadlockRetry"),
            "EVICTOR_DEADLOCK_RETRY missing"
        );
        assert!(
            names.contains(&"noxu.evictor.lruOnly"),
            "EVICTOR_LRU_ONLY missing"
        );
        assert!(
            names.contains(&"noxu.log.directNIO"),
            "LOG_DIRECT_NIO missing"
        );
        assert!(
            names.contains(&"noxu.log.chunkedNIO"),
            "LOG_CHUNKED_NIO missing"
        );
    }

    #[test]
    fn test_param_min_max_bounds() {
        // MAX_MEMORY_PERCENT: min=1, max=90
        assert_eq!(MAX_MEMORY_PERCENT.min, Some(ParamValue::Int(1)));
        assert_eq!(MAX_MEMORY_PERCENT.max, Some(ParamValue::Int(90)));

        // LOG_FILE_MAX: min=1_000_000, max=1_073_741_824
        assert_eq!(LOG_FILE_MAX.min, Some(ParamValue::Long(1_000_000)));
        assert_eq!(LOG_FILE_MAX.max, Some(ParamValue::Long(1_073_741_824)));

        // CLEANER_MIN_UTILIZATION: min=0, max=90
        assert_eq!(CLEANER_MIN_UTILIZATION.min, Some(ParamValue::Int(0)));
        assert_eq!(CLEANER_MIN_UTILIZATION.max, Some(ParamValue::Int(90)));

        // CLEANER_MIN_FILE_UTILIZATION: min=0, max=50
        assert_eq!(CLEANER_MIN_FILE_UTILIZATION.min, Some(ParamValue::Int(0)));
        assert_eq!(CLEANER_MIN_FILE_UTILIZATION.max, Some(ParamValue::Int(50)));

        // EVICTOR_N_LRU_LISTS: min=1, max=32
        assert_eq!(EVICTOR_N_LRU_LISTS.min, Some(ParamValue::Int(1)));
        assert_eq!(EVICTOR_N_LRU_LISTS.max, Some(ParamValue::Int(32)));

        // LOCK_N_LOCK_TABLES: min=1, max=32767
        assert_eq!(LOCK_N_LOCK_TABLES.min, Some(ParamValue::Int(1)));
        assert_eq!(LOCK_N_LOCK_TABLES.max, Some(ParamValue::Int(32767)));

        // NODE_MAX_ENTRIES: min=4, max=32767
        assert_eq!(NODE_MAX_ENTRIES.min, Some(ParamValue::Int(4)));
        assert_eq!(NODE_MAX_ENTRIES.max, Some(ParamValue::Int(32767)));

        // TREE_MIN_MEMORY: min=50*1024
        assert_eq!(TREE_MIN_MEMORY.min, Some(ParamValue::Long(50 * 1024)));
    }

    #[test]
    fn test_mutability_flags_match_je() {
        // Mutable params
        assert!(MAX_MEMORY.mutable);
        assert!(MAX_MEMORY_PERCENT.mutable);
        assert!(MAX_DISK.mutable);
        assert!(FREE_DISK.mutable);
        assert!(ENV_RUN_EVICTOR.mutable);
        assert!(ENV_RUN_CLEANER.mutable);
        assert!(ENV_RUN_CHECKPOINTER.mutable);
        assert!(ENV_RUN_IN_COMPRESSOR.mutable);
        assert!(CLEANER_MIN_UTILIZATION.mutable);
        assert!(CLEANER_THREADS.mutable);
        assert!(CLEANER_DEADLOCK_RETRY.mutable);
        assert!(LOCK_DEADLOCK_DETECT.mutable);
        assert!(TXN_DEADLOCK_STACK_TRACE.mutable);
        assert!(TXN_DUMP_LOCKS.mutable);
        assert!(STATS_COLLECT.mutable);
        assert!(ENV_EXPOSE_USER_DATA.mutable);
        assert!(ENV_EXPIRATION_ENABLED.mutable);
        assert!(CHECKPOINTER_HIGH_PRIORITY.mutable);
        assert!(TREE_MIN_MEMORY.mutable);

        // Immutable params
        assert!(!LOG_FILE_MAX.mutable);
        assert!(!LOG_MEM_ONLY.mutable);
        assert!(!LOG_CHECKSUM_READ.mutable);
        assert!(!NODE_MAX_ENTRIES.mutable);
        assert!(!TREE_MAX_EMBEDDED_LN.mutable);
        assert!(!EVICTOR_N_LRU_LISTS.mutable);
        assert!(!EVICTOR_EVICT_BYTES.mutable);
        assert!(!CHECKPOINTER_BYTES_INTERVAL.mutable);
        assert!(!LOCK_N_LOCK_TABLES.mutable);
        assert!(!TXN_SERIALIZABLE_ISOLATION.mutable);
        assert!(!STATS_FILE_DIRECTORY.mutable);
    }
}

#[cfg(test)]
mod c1_all_defaults_in_range_test {
    use super::all_params;

    #[test]
    fn c1_every_param_default_validates_against_its_own_bounds() {
        // A parameter's own default MUST satisfy its min/max bounds — otherwise
        // opening an environment with defaults would fail validation. This guards
        // the C-1 duration-bounds work (and all other bounded params).
        //
        // Exception (JE-faithful): a default of `0` is JE's documented "auto"
        // sentinel for size params computed from `maxMemory` (e.g.
        // `LOG_TOTAL_BUFFER_BYTES` / JE `LOG_MEM_SIZE`, whose min is
        // `LOG_MEM_SIZE_MIN` but whose default 0 means "compute from je.maxMemory").
        // JE itself does not validate that 0 against the min — neither do we.
        for p in all_params() {
            let is_auto_sentinel = matches!(
                p.default,
                crate::param::ParamValue::Int(0)
                    | crate::param::ParamValue::Long(0)
            ) && p.min.is_some();
            if is_auto_sentinel {
                continue;
            }
            assert!(
                p.validate(&p.default).is_ok(),
                "param {} default {:?} violates its own bounds (min={:?} max={:?})",
                p.name,
                p.default,
                p.min,
                p.max
            );
        }
    }
}
