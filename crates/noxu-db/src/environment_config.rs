//! Environment configuration.
//!
//! Mirrors `EnvironmentConfig` / `EnvironmentMutableConfig` from7.5.11.
//! Every parameter from 's `EnvironmentConfig.java` is represented here.
//! Java-specific parameters (NIO, JCA/RA) are included with documentation
//! noting the accepted deviation for a native Rust implementation.
//!
//! Parameters are grouped by subsystem to match the layout.

use crate::durability::Durability;
use crate::error::ExceptionListener;
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;

/// Wrapper around an optional `ExceptionListener` that implements `Debug` and
/// `Clone` so that `EnvironmentConfig` can keep those derives.
///
/// Mirrors `EnvironmentConfig.setExceptionListener(ExceptionListener)`.
#[derive(Clone, Default)]
pub struct ExceptionListenerHolder(pub Option<Arc<dyn ExceptionListener>>);

impl fmt::Debug for ExceptionListenerHolder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.0 {
            None => f.write_str("None"),
            Some(_) => f.write_str("Some(<ExceptionListener>)"),
        }
    }
}

/// Configuration for opening a Noxu DB environment.
///
/// Configuration for a Noxu DB environment. Provides 150+ typed parameters for tuning all subsystems.
/// Use the builder pattern (`set_*` / `with_*`) to configure individual
/// parameters; all fields have -identical defaults unless noted.
#[derive(Debug, Clone)]
#[must_use]
pub struct EnvironmentConfig {
    // -----------------------------------------------------------------------
    // Core / environment lifecycle
    // -----------------------------------------------------------------------
    /// Home directory for the environment.
    pub home: PathBuf,

    /// Allow creation of a new environment if it does not exist.
    /// Mirrors `EnvironmentConfig.setAllowCreate()` / default false.
    pub allow_create: bool,

    /// Open the environment for transactional use.
    /// Mirrors `ENV_IS_TRANSACTIONAL` / default false.
    pub transactional: bool,

    /// Open the environment in read-only mode.
    /// Mirrors `ENV_READ_ONLY` / default false.
    pub read_only: bool,

    /// Enable locking.  When false the environment runs without a lock
    /// manager (equivalent to-transactional, non-locking mode).
    /// Mirrors `ENV_IS_LOCKING` / default true.
    pub env_is_locking: bool,

    /// Share the B-tree cache across multiple environments in the same JVM.
    /// In Noxu, this is a configuration hint; shared-cache pooling is
    /// accepted as a future work item.
    /// Mirrors `SHARED_CACHE` / default false.
    pub shared_cache: bool,

    /// Force a checkpoint after recovery completes.
    /// Mirrors `ENV_RECOVERY_FORCE_CHECKPOINT` / default false.
    pub env_recovery_force_checkpoint: bool,

    /// Force a new log file to be started after recovery.
    /// Mirrors `ENV_RECOVERY_FORCE_NEW_FILE` / default false.
    pub env_recovery_force_new_file: bool,

    /// Halt the environment on commit after a `ChecksumException`.
    /// Mirrors `HALT_ON_COMMIT_AFTER_CHECKSUMEXCEPTION` / default false.
    pub halt_on_commit_after_checksum_exception: bool,

    // -----------------------------------------------------------------------
    // Memory / cache
    // -----------------------------------------------------------------------
    /// Maximum bytes for the B-tree cache.
    /// Mirrors `MAX_MEMORY` / `EnvironmentConfig.setCacheSize()` / default 0
    /// ( auto-sizes to 60% of heap).  Noxu default: 64 MiB.
    pub cache_size: u64,

    /// Cache size as a percentage of system memory (0 = use `cache_size`).
    /// Mirrors `MAX_MEMORY_PERCENT` / `EnvironmentConfig.setCachePercent()` /
    /// default 60.  When non-zero, overrides `cache_size`.
    pub cache_percent: u32,

    /// Off-heap cache size in bytes.  0 = disabled.
    /// Mirrors `MAX_OFF_HEAP_MEMORY` / default 0.
    pub max_off_heap_memory: u64,

    /// Maximum disk space the environment may use in bytes.  0 = unlimited.
    /// Mirrors `MAX_DISK` / default 0.
    pub max_disk: u64,

    /// Minimum free disk space in bytes; triggers `DiskLimitExceeded` if the
    /// available space on the file-system falls below this threshold.
    /// Mirrors `FREE_DISK` / default 5 GiB.
    pub free_disk: u64,

    /// Extra bytes reserved on top of `free_disk`: writes fail when available
    /// free space drops below `free_disk + reserved_disk`.  Mirrors JE
    /// `RESERVED_DISK` (`EnvironmentParams.RESERVED_DISK`) / default 0.
    pub reserved_disk: u64,

    // -----------------------------------------------------------------------
    // Background daemons — run flags
    // -----------------------------------------------------------------------
    /// Run the background INCompressor daemon.
    /// Mirrors `ENV_RUN_IN_COMPRESSOR` / default true.
    pub run_in_compressor: bool,

    /// Run the background Checkpointer daemon.
    /// Mirrors `ENV_RUN_CHECKPOINTER` / default true.
    pub run_checkpointer: bool,

    /// Run the background Cleaner daemon.
    /// Mirrors `ENV_RUN_CLEANER` / default true.
    pub run_cleaner: bool,

    /// Run the background Evictor daemon.
    /// Mirrors `ENV_RUN_EVICTOR` / default true.
    pub run_evictor: bool,

    /// Run the background off-heap Evictor daemon.
    /// Mirrors `ENV_RUN_OFFHEAP_EVICTOR` / default true (when off-heap configured).
    pub run_offheap_evictor: bool,

    /// Run the background data-integrity Verifier daemon.
    /// Mirrors `ENV_RUN_VERIFIER` / default false.
    pub run_verifier: bool,

    // -----------------------------------------------------------------------
    // Background daemons — rate limits & sleep
    // -----------------------------------------------------------------------
    /// Maximum read throughput for background daemons in KB/s.  0 = unlimited.
    /// Mirrors `ENV_BACKGROUND_READ_LIMIT` / default 0.
    pub env_background_read_limit_kb: u32,

    /// Maximum write throughput for background daemons in KB/s.  0 = unlimited.
    /// Mirrors `ENV_BACKGROUND_WRITE_LIMIT` / default 0.
    pub env_background_write_limit_kb: u32,

    /// Sleep interval for background daemons between work units in
    /// microseconds.  0 = no enforced sleep.
    /// Mirrors `ENV_BACKGROUND_SLEEP_INTERVAL` / default 0.
    pub env_background_sleep_interval_us: u64,

    // -----------------------------------------------------------------------
    // Environment behaviour flags
    // -----------------------------------------------------------------------
    /// Check for lock leaks when databases are closed.
    ///
    /// **Reserved / not yet implemented as of v3.1.** Setting a non-default
    /// value (false) has no effect and emits a `WARN` log at
    /// `Environment::open` time.
    ///
    /// Mirrors `ENV_CHECK_LEAKS` / default true.
    pub env_check_leaks: bool,

    /// Force thread yields at latch acquire/release points (a test-only
    /// fairness-stress knob that helps shake out latch-ordering races).
    ///
    /// **Implemented (7.1).** When `true`, `noxu-latch` injects
    /// `std::thread::yield_now()` after each latch grant and on each latch
    /// release; when `false` (the default) the injection point is a single
    /// relaxed atomic load, so it is effectively free.  Wired via
    /// `noxu_latch::configure` at `Environment::open`.
    ///
    /// Mirrors JE `ENV_FORCED_YIELD` / default false.
    pub env_forced_yield: bool,

    /// Use fair (FIFO-ordered) latches.  May reduce throughput under low
    /// contention but prevents starvation.
    ///
    /// **Reserved / not implemented.** Setting a non-default value (true) has
    /// no effect and emits a `WARN` log at `Environment::open` time.  Noxu's
    /// latches are backed by `noxu-sync`'s futex primitives, which are
    /// fundamentally non-fair and have no FIFO wait queue to toggle; a
    /// faithful fair-latch mode is a dedicated latch rewrite (a ticket/FIFO
    /// queue in `noxu-sync`), tracked separately.
    ///
    /// Mirrors JE `ENV_FAIR_LATCHES` (`setFairLatches`) / default false.
    pub env_fair_latches: bool,

    /// Latch acquisition timeout in milliseconds.  0 = no timeout (block until
    /// acquired).  A non-zero timeout turns a latch deadlock — which would
    /// otherwise hang — into a diagnosable `LatchTimeout` error.
    ///
    /// **Implemented (7.1).** Wired into `noxu-latch`'s exclusive and shared
    /// acquire paths via `noxu_latch::configure` at `Environment::open`.  The
    /// default `300_000` (5 min) is treated as "unset" — leaving it unchanged
    /// preserves the historical latch behaviour byte-for-byte; setting any
    /// other value opts in to that timeout (and `0` means block until
    /// acquired).
    ///
    /// Mirrors JE `ENV_LATCH_TIMEOUT` / default 300_000 ms (5 min).
    pub env_latch_timeout_ms: u64,

    /// TTL clock tolerance — records within this many milliseconds of their
    /// expiration time are treated as expired.
    ///
    /// **Reserved / not yet implemented as of v3.1.** Setting a non-default
    /// value (non-zero) has no effect and emits a `WARN` log at
    /// `Environment::open` time.
    ///
    /// Mirrors `ENV_TTL_CLOCK_TOLERANCE` / default 0.
    pub env_ttl_clock_tolerance_ms: u64,

    /// Enable TTL-based record expiration at the environment level.
    ///
    /// **Reserved / not yet implemented as of v3.1.** Setting a non-default
    /// value (true) has no effect and emits a `WARN` log at
    /// `Environment::open` time.
    ///
    /// Mirrors `ENV_EXPIRATION_ENABLED` / default false.
    pub env_expiration_enabled: bool,

    /// Enable per-database node eviction.
    ///
    /// **Reserved / not yet implemented as of v3.1.** Setting a non-default
    /// value (true) has no effect and emits a `WARN` log at
    /// `Environment::open` time.
    ///
    /// Mirrors `ENV_DB_EVICTION` / default false.
    pub env_db_eviction: bool,

    // -----------------------------------------------------------------------
    // Log / I-O
    // -----------------------------------------------------------------------
    /// Maximum size of a single log file in bytes.
    /// Mirrors `LOG_FILE_MAX` / default 10 MiB.
    pub log_file_max_bytes: u64,

    /// Number of cached open file handles (LRU-evicted when full).
    /// Mirrors `LOG_FILE_CACHE_SIZE` / default 100.
    pub log_file_cache_size: usize,

    /// Validate entry checksums on every log read.
    /// Mirrors `LOG_CHECKSUM_READ` / default true.
    pub log_checksum_read: bool,

    /// Verify all checksums during log scans (more thorough than
    /// `log_checksum_read`; used by background verifier).
    /// Mirrors `LOG_VERIFY_CHECKSUMS` / default false.
    pub log_verify_checksums: bool,

    /// Timeout for a single `fdatasync` call in milliseconds.
    /// Mirrors `LOG_FSYNC_TIMEOUT` / default 500_000 ms.
    pub log_fsync_timeout_ms: u64,

    /// Soft limit on fsync duration in milliseconds; logs a warning when
    /// exceeded.  0 = disabled.
    /// Mirrors `LOG_FSYNC_TIME_LIMIT` / default 0.
    pub log_fsync_time_limit_ms: u64,

    /// Number of write buffers in the log buffer pool.
    /// Mirrors `LOG_NUM_BUFFERS` / default 3.
    pub log_num_buffers: usize,

    /// Total bytes across all log write buffers.
    /// Mirrors `LOG_TOTAL_BUFFER_BYTES` / default 7 MiB.
    pub log_total_buffer_bytes: u64,

    /// Per-buffer size override in bytes.  0 = derive from
    /// `log_total_buffer_bytes / log_num_buffers`.
    /// Mirrors `LOG_BUFFER_SIZE` / default 0.
    pub log_buffer_size: usize,

    /// Size of the fault-in read buffer for random BIN fetches.
    /// Mirrors `LOG_FAULT_READ_SIZE` / default 2 KiB.
    pub log_fault_read_size: usize,

    /// Log iterator read buffer in bytes.
    /// Mirrors `LOG_ITERATOR_READ_SIZE` / default 8 KiB.
    pub log_iterator_read_size: usize,

    /// Log iterator maximum buffer size in bytes (grows up to this limit).
    /// Mirrors `LOG_ITERATOR_MAX_SIZE` / default 16 MiB.
    pub log_iterator_max_size: usize,

    /// Number of data directories for log file striping.  0 = single dir.
    /// Mirrors `LOG_N_DATA_DIRECTORIES` / default 0.
    pub log_n_data_directories: u32,

    /// Run in in-memory-only mode (no log files written).
    /// Mirrors `LOG_MEM_ONLY` / default false.
    pub log_mem_only: bool,

    /// Detect external deletion of log files and respond gracefully.
    /// Mirrors `LOG_DETECT_FILE_DELETE` / default false.
    pub log_detect_file_delete: bool,

    /// Interval between log-file deletion detection polls in milliseconds.
    /// Mirrors `LOG_DETECT_FILE_DELETE_INTERVAL` / default 3_000 ms.
    pub log_detect_file_delete_interval_ms: u64,

    /// Interval between periodic flush-and-sync operations in milliseconds.
    /// 0 = disabled.  : `LOG_FLUSH_SYNC_INTERVAL` / default 0.
    pub log_flush_sync_interval_ms: u64,

    /// Interval between periodic flush-without-sync operations in
    /// milliseconds.  0 = disabled.
    /// Mirrors `LOG_FLUSH_NO_SYNC_INTERVAL` / default 0.
    pub log_flush_no_sync_interval_ms: u64,

    /// Use `O_DSYNC` when opening log files.  Accepted deviation: on Linux
    /// Noxu passes `O_DSYNC` to `OpenOptions`; semantics are equivalent.
    /// Mirrors `LOG_USE_ODSYNC` / default false.
    pub log_use_odsync: bool,

    /// Use an asynchronous write queue between the log manager and the OS.
    /// Mirrors `LOG_USE_WRITE_QUEUE` / default false.
    pub log_use_write_queue: bool,

    /// Size of the asynchronous write queue in bytes.
    /// Mirrors `LOG_WRITE_QUEUE_SIZE` / default 1 MiB.
    pub log_write_queue_size: usize,

    /// Group-commit waiter threshold.  0 = disabled.
    /// Mirrors `LOG_GROUP_COMMIT_THRESHOLD` / default 0.
    pub log_group_commit_threshold: usize,

    /// Group-commit interval in milliseconds.  0 = disabled.
    /// Mirrors `LOG_GROUP_COMMIT_INTERVAL` / default 0.
    pub log_group_commit_interval_ms: u64,

    /// Bounded fsync pipeline depth — the maximum number of concurrent
    /// `fdatasync`s in flight on the log file.  `1` (default) = the
    /// single-leader group commit (flat tail latency); `> 1` overlaps that
    /// many `fdatasync`s to raise write throughput on devices that sustain
    /// concurrent same-file syncs, at a slightly higher tail.  Durability is
    /// identical at any depth (the drain stays LSN-ordered; the durable
    /// watermark stays monotonic).  Mirrors `LOG_FSYNC_MAX_LEADERS` / default 1.
    pub log_fsync_max_leaders: usize,

    /// Enable the consolidation-array Log Write Latch (Aether/Silo/WT).
    ///
    /// When `true`, concurrent committers combine into one batch via a
    /// lock-free CAS-join and a single leader drives the whole batch under one
    /// latch acquisition, dissolving the per-committer futex convoy on the LWL.
    /// Single WAL + single monotonic LSN preserved; on-disk format identical.
    /// Defaults to `false` (classic mutex LWL).  Mirrors
    /// `LOG_CONSOLIDATION_ARRAY`.
    pub log_consolidation_array: bool,

    // -----------------------------------------------------------------------
    // B-tree
    // -----------------------------------------------------------------------
    /// Maximum number of entries per Internal Node (IN).
    /// Mirrors `NODE_MAX_ENTRIES` / default 128.
    pub node_max_entries: u32,

    /// Maximum number of entries per duplicate-tree node.
    /// Mirrors `NODE_DUP_TREE_MAX_ENTRIES` / default 128.
    pub node_dup_tree_max_entries: u32,

    /// Maximum value size in bytes for inline (embedded) LNs stored directly
    /// in the BIN slot.  Records larger than this are stored as separate LNs.
    /// Mirrors `TREE_MAX_EMBEDDED_LN` / default 16.
    pub tree_max_embedded_ln: u32,

    /// Maximum percentage of BIN entries that may be in a delta before a
    /// full BIN is written (0–100).
    /// Mirrors `TREE_MAX_DELTA` / default 25.
    pub tree_max_delta: u8,

    /// Write BIN-delta log entries (partial BIN updates).
    /// Mirrors `TREE_BIN_DELTA` / default true.
    pub tree_bin_delta: bool,

    /// BIN-delta percent threshold (0–75): a BIN is logged as a delta only
    /// when its delta-slot count is `<= nEntries * percent / 100`.
    /// Mirrors JE `TREE_BIN_DELTA` / `BIN_DELTA_PERCENT` (int, default 25).
    pub tree_bin_delta_percent: u8,

    /// Minimum memory per B-tree node in bytes.  0 = no minimum.
    /// Mirrors `TREE_MIN_MEMORY` / default 0.
    pub tree_min_memory: u64,

    /// Maximum key length for compact (prefix-compressed) key storage.
    /// Mirrors `TREE_COMPACT_MAX_KEY_LENGTH` / default 16.
    pub tree_compact_max_key_length: u32,

    // -----------------------------------------------------------------------
    // INCompressor
    // -----------------------------------------------------------------------
    /// INCompressor wakeup interval in milliseconds.
    /// Mirrors `COMPRESSOR_WAKEUP_INTERVAL` / default 5_000 ms.
    pub in_compressor_wakeup_interval_ms: u64,

    /// Number of deadlock retries per INCompressor pass.
    /// Mirrors `COMPRESSOR_DEADLOCK_RETRY` / default 3.
    pub compressor_deadlock_retry: u32,

    /// Lock timeout for INCompressor operations in milliseconds.
    /// Mirrors `COMPRESSOR_LOCK_TIMEOUT` / default 500 ms.
    pub compressor_lock_timeout_ms: u64,

    /// Purge the root IN when it becomes empty after compression.
    /// Mirrors `COMPRESSOR_PURGE_ROOT` / default false.
    pub compressor_purge_root: bool,

    // -----------------------------------------------------------------------
    // Cleaner
    // -----------------------------------------------------------------------
    /// Minimum log utilization percentage; cleaning triggers when below this.
    /// Mirrors `CLEANER_MIN_UTILIZATION` / default 50.
    pub cleaner_min_utilization: u8,
    /// JE CLEANER_TWO_PASS_GAP (default 10).
    pub cleaner_two_pass_gap: i32,
    /// JE CLEANER_TWO_PASS_THRESHOLD (default 0 => minUtilization - 5).
    pub cleaner_two_pass_threshold: i32,

    /// Minimum per-file utilization; files below this are always candidates.
    /// Mirrors `CLEANER_MIN_FILE_UTILIZATION` / default 5.
    pub cleaner_min_file_utilization: u8,

    /// Number of background cleaner threads.
    /// Mirrors `CLEANER_THREADS` / default 1.
    pub cleaner_threads: u32,

    /// Minimum number of log files that must exist before cleaning begins.
    /// Mirrors `CLEANER_MIN_FILES_TO_CLEAN` / default 2.
    pub cleaner_min_file_count: u32,

    /// Minimum age of a log file (in checkpoints) before it becomes a
    /// candidate.  : `CLEANER_MIN_AGE` / default 2.
    pub cleaner_min_age: u32,

    /// Bytes written between cleaner wakeups (byte-based trigger).
    /// 0 = disabled.  : `CLEANER_BYTES_INTERVAL` / default 0.
    pub cleaner_bytes_interval: u64,

    /// Time between cleaner wakeups in milliseconds (time-based trigger).
    /// 0 = disabled.  : `CLEANER_WAKEUP_INTERVAL` / default 0.
    pub cleaner_wakeup_interval_ms: u64,

    /// Fetch the sizes of obsolete records when calculating utilization.
    /// Mirrors `CLEANER_FETCH_OBSOLETE_SIZE` / default false.
    pub cleaner_fetch_obsolete_size: bool,

    /// Adjust utilization accounting for uncommitted transactions.
    /// Mirrors `CLEANER_ADJUST_UTILIZATION` / default false.
    pub cleaner_adjust_utilization: bool,

    /// Number of deadlock retries per cleaner migration pass.
    /// Mirrors `CLEANER_DEADLOCK_RETRY` / default 3.
    pub cleaner_deadlock_retry: u32,

    /// Lock timeout for cleaner migration operations in milliseconds.
    /// Mirrors `CLEANER_LOCK_TIMEOUT` / default 500 ms.
    pub cleaner_lock_timeout_ms: u64,

    /// Expunge (delete) cleaned log files immediately rather than keeping them
    /// in a `deleted/` sub-directory.
    /// Mirrors `CLEANER_EXPUNGE` / default true.
    pub cleaner_expunge: bool,

    /// Move cleaned log files to a `deleted/` sub-directory instead of
    /// deleting them in place.
    /// Mirrors `CLEANER_USE_DELETED_DIR` / default false.
    pub cleaner_use_deleted_dir: bool,

    /// Maximum number of log files processed per cleaner batch.
    /// 0 = unlimited.  : `CLEANER_MAX_BATCH_FILES` / default 0.
    pub cleaner_max_batch_files: u32,

    /// Bytes read per cleaner file scan pass.
    /// Mirrors `CLEANER_READ_SIZE` / default 8 KiB.
    pub cleaner_read_size: usize,

    /// Maximum percentage of the cache to use for cleaner utilization detail.
    /// Mirrors `CLEANER_DETAIL_MAX_MEMORY_PERCENTAGE` / default 2.
    pub cleaner_detail_max_memory_percentage: u32,

    /// Number of LN records to look ahead during file cleaning.
    /// Mirrors `CLEANER_LOOK_AHEAD_CACHE_SIZE` / default 32.
    pub cleaner_look_ahead_cache_size: usize,

    /// Migrate live records proactively in the foreground (user threads).
    /// Mirrors `CLEANER_FOREGROUND_PROACTIVE_MIGRATION` / default false.
    pub cleaner_foreground_proactive_migration: bool,

    /// Migrate live records proactively in the background cleaner thread.
    /// Mirrors `CLEANER_BACKGROUND_PROACTIVE_MIGRATION` / default false.
    pub cleaner_background_proactive_migration: bool,

    /// Lazy migration: defer LN migration until the slot is next accessed.
    /// Mirrors `CLEANER_LAZY_MIGRATION` / default false.
    pub cleaner_lazy_migration: bool,

    /// Enable TTL-based record expiration tracking in the cleaner.
    /// Mirrors `CLEANER_EXPIRATION_ENABLED` / default false.
    pub cleaner_expiration_enabled: bool,

    // -----------------------------------------------------------------------
    // Checkpointer
    // -----------------------------------------------------------------------
    /// Number of bytes written between automatic checkpoints.
    /// Mirrors `CHECKPOINTER_BYTES_INTERVAL` / default 20 MiB.
    pub checkpointer_bytes_interval: u64,

    /// Time between automatic checkpoints in milliseconds.
    /// 0 = disabled.  : `CHECKPOINTER_WAKEUP_INTERVAL` / default 30_000 ms.
    pub checkpointer_wakeup_interval_ms: u64,

    /// Minimum time between automatic checkpoints in seconds (0 = disabled).
    /// : relates to `CHECKPOINTER_HIGH_PRIORITY`.
    pub checkpointer_min_interval_secs: u64,

    /// Number of deadlock retries per checkpoint.
    /// Mirrors `CHECKPOINTER_DEADLOCK_RETRY` / default 3.
    pub checkpointer_deadlock_retry: u32,

    /// Run checkpoints at high priority (flush more aggressively).
    /// Mirrors `CHECKPOINTER_HIGH_PRIORITY` / default false.
    pub checkpointer_high_priority: bool,

    // -----------------------------------------------------------------------
    // Evictor
    // -----------------------------------------------------------------------
    /// Number of tree nodes examined per evictor pass.
    /// Mirrors `EVICTOR_NODES_PER_SCAN` / default 10.
    pub evictor_nodes_per_scan: usize,

    /// Cache eviction algorithm: "lru" | "clock" | "arc" | "car" | "lirs".
    /// Mirrors `EVICTOR_ALGORITHM` / default "lru" (JE-faithful).
    pub evictor_algorithm: String,

    /// Bytes to evict from the cache per evictor pass.
    /// Mirrors `EVICTOR_EVICT_BYTES` / default 512 KiB.
    pub evictor_evict_bytes: u64,

    /// Percentage above the cache target at which critical eviction kicks in.
    /// Mirrors `EVICTOR_CRITICAL_PERCENTAGE` / default 5.
    pub evictor_critical_percentage: u32,

    /// Use LRU-only eviction (no priority-1 / priority-2 split).
    /// Mirrors `EVICTOR_LRU_ONLY` / default false.
    pub evictor_lru_only: bool,
    /// JE EVICTOR_USE_DIRTY_LRU (default true). Forced false when off-heap is enabled.
    pub evictor_use_dirty_lru: bool,

    /// JE `EVICTOR_MUTATE_BINS` (default true): when true, the evictor may
    /// strip obsolete LNs out of a BIN during eviction (PartialEvict) rather
    /// than leaving the BIN untouched.  Mirrors `EnvironmentParams`
    /// `EVICTOR_MUTATE_BINS`.
    pub evictor_mutate_bins: bool,

    /// Number of LRU lists (increases parallelism under contention).
    /// Mirrors `EVICTOR_N_LRU_LISTS` / default 4.
    pub evictor_n_lru_lists: u32,

    /// Number of deadlock retries per evictor pass.
    /// Mirrors `EVICTOR_DEADLOCK_RETRY` / default 3.
    pub evictor_deadlock_retry: u32,

    /// Minimum number of background evictor threads always kept alive.
    /// Mirrors `EVICTOR_CORE_THREADS` / default 1.
    pub evictor_core_threads: usize,

    /// Maximum number of background evictor threads.
    /// Mirrors `EVICTOR_MAX_THREADS` / default 10.
    pub evictor_max_threads: usize,

    /// Keep-alive time for idle evictor threads in milliseconds.
    /// Mirrors `EVICTOR_KEEP_ALIVE` / default 60_000 ms.
    pub evictor_keep_alive_ms: u64,

    /// Allow the evictor to write BIN-delta entries rather than full BINs.
    /// Mirrors `EVICTOR_ALLOW_BIN_DELTAS` / default true.
    pub evictor_allow_bin_deltas: bool,

    // -----------------------------------------------------------------------
    // Off-heap evictor
    // -----------------------------------------------------------------------
    /// Bytes to evict from the off-heap cache per pass.
    /// Mirrors `OFFHEAP_EVICT_BYTES` / default 512 KiB.
    pub offheap_evict_bytes: u64,

    /// Number of LRU lists for the off-heap cache.
    /// Mirrors `OFFHEAP_N_LRU_LISTS` / default 4.
    pub offheap_n_lru_lists: u32,

    /// Checksum off-heap cache entries on write and verify on read.
    /// Mirrors `OFFHEAP_CHECKSUM` / default false.
    pub offheap_checksum: bool,

    /// Minimum number of off-heap evictor threads always kept alive.
    /// Mirrors `OFFHEAP_CORE_THREADS` / default 1.
    pub offheap_core_threads: usize,

    /// Maximum number of off-heap evictor threads.
    /// Mirrors `OFFHEAP_MAX_THREADS` / default 10.
    pub offheap_max_threads: usize,

    /// Keep-alive time for idle off-heap evictor threads in milliseconds.
    /// Mirrors `OFFHEAP_KEEP_ALIVE` / default 60_000 ms.
    pub offheap_keep_alive_ms: u64,

    // -----------------------------------------------------------------------
    // Locking
    // -----------------------------------------------------------------------
    /// Lock timeout in milliseconds.
    /// Mirrors `LOCK_TIMEOUT` / default 500 ms.
    pub lock_timeout_ms: u64,

    /// Number of lock table shards.
    /// Mirrors `LOCK_N_LOCK_TABLES` / default 1.  Noxu default: 16.
    pub lock_n_lock_tables: u32,

    /// Run the deadlock detector on lock waits.
    /// Mirrors `LOCK_DEADLOCK_DETECT` / default true.
    pub lock_deadlock_detect: bool,

    /// Delay before deadlock detection runs (milliseconds).
    /// 0 = detect immediately on every wait.
    /// Mirrors `LOCK_DEADLOCK_DETECT_DELAY` / default 0.
    pub lock_deadlock_detect_delay_ms: u64,

    // -----------------------------------------------------------------------
    // Transactions
    // -----------------------------------------------------------------------
    /// Transaction timeout in milliseconds.  0 = no timeout.
    /// Mirrors `TXN_TIMEOUT` / default 0.
    pub txn_timeout_ms: u64,

    /// Default durability policy for transactions.
    /// Mirrors `TXN_DURABILITY`.
    pub durability: Durability,

    /// Commits do not wait for the log to reach disk.
    /// Mirrors `TXN_NO_SYNC` / default false.
    ///
    /// **Deprecated since 2.4.1** — use the [`durability`][Self::durability]
    /// field with `Durability::commit_no_sync()` instead.
    pub txn_no_sync: bool,

    /// Commits write the log to the OS buffer but skip `fdatasync`.
    /// Mirrors `TXN_WRITE_NO_SYNC` / default false.
    ///
    /// **Deprecated since 2.4.1** — use the [`durability`][Self::durability]
    /// field with `Durability::commit_write_no_sync()` instead.
    pub txn_write_no_sync: bool,

    /// When `true`, all transactions default to **serializable**
    /// (degree-3) isolation: read locks are retained through commit.
    /// When `false` (the default), transactions use
    /// **repeatable-read** isolation — read locks are still held for
    /// the lifetime of the txn but predicate / phantom protection is
    /// not provided.  Per-transaction overrides are available via
    /// [`crate::transaction_config::TransactionConfig::set_serializable_isolation`]
    /// and [`crate::transaction_config::TransactionConfig::set_read_committed`].
    /// Mirrors `TXN_SERIALIZABLE_ISOLATION` / default false.
    pub txn_serializable_isolation: bool,

    /// Capture a stack trace at deadlock detection time (expensive).
    /// Mirrors `TXN_DEADLOCK_STACK_TRACE` / default false.
    pub txn_deadlock_stack_trace: bool,

    /// Dump all lock state on deadlock detection (diagnostic, expensive).
    /// Mirrors `TXN_DUMP_LOCKS` / default false.
    pub txn_dump_locks: bool,

    // -----------------------------------------------------------------------
    // Verifier daemon
    // -----------------------------------------------------------------------
    /// Cron-style schedule string for the background verifier.
    /// Empty string = run continuously when `run_verifier = true`.
    /// Mirrors `VERIFY_SCHEDULE` / default `""`.
    pub verify_schedule: String,

    /// Verify log-file checksums in the background.
    /// Mirrors `VERIFY_LOG` / default false.
    pub verify_log: bool,

    /// Delay between log verification read operations in milliseconds.
    /// Mirrors `VERIFY_LOG_READ_DELAY` / default 0.
    pub verify_log_read_delay_ms: u64,

    /// Verify the B-tree structure in the background.
    /// Mirrors `VERIFY_BTREE` / default false.
    pub verify_btree: bool,

    /// Verify secondary index consistency in the background.
    /// Mirrors `VERIFY_SECONDARIES` / default true.
    pub verify_secondaries: bool,

    /// Verify data records (values) in the background.
    /// Mirrors `VERIFY_DATA_RECORDS` / default false.
    pub verify_data_records: bool,

    /// Verify obsolete records have correct LSNs in the background.
    /// Mirrors `VERIFY_OBSOLETE_RECORDS` / default false.
    pub verify_obsolete_records: bool,

    /// Number of B-tree nodes verified per verifier batch.
    /// Mirrors `VERIFY_BTREE_BATCH_SIZE` / default 1_000.
    pub verify_btree_batch_size: u32,

    /// Delay between B-tree verification batches in milliseconds.
    /// Mirrors `VERIFY_BTREE_BATCH_DELAY` / default 10 ms.
    pub verify_btree_batch_delay_ms: u64,

    // -----------------------------------------------------------------------
    // Disk-ordered cursor
    // -----------------------------------------------------------------------
    /// Timeout for the disk-ordered cursor producer queue in milliseconds.
    /// Mirrors `DOS_PRODUCER_QUEUE_TIMEOUT` / default 10_000 ms.
    pub dos_producer_queue_timeout_ms: u64,

    // -----------------------------------------------------------------------
    // Recovery
    // -----------------------------------------------------------------------
    /// Force a checkpoint after recovery completes (alias; see
    /// `env_recovery_force_checkpoint` above).
    // (No duplicate field; already covered above.)

    // -----------------------------------------------------------------------
    // Background stats collection
    // -----------------------------------------------------------------------

    /// Collect environment statistics in the background.
    /// Mirrors `STATS_COLLECT` / default false.
    pub stats_collect: bool,

    /// Interval between background stats collection passes in seconds.
    /// Mirrors `STATS_COLLECT_INTERVAL` / default 300 s.
    pub stats_collect_interval_secs: u64,

    /// Maximum number of stats CSV files to retain.
    /// Mirrors `STATS_MAX_FILES` / default 100.
    pub stats_max_files: u32,

    /// Rows per stats CSV file before rotation.
    /// Mirrors `STATS_FILE_ROW_COUNT` / default 1_000.
    pub stats_file_row_count: u32,

    /// Directory for stats CSV files.  `None` = use the environment home.
    /// Mirrors `STATS_FILE_DIRECTORY` / default `None`.
    pub stats_file_directory: Option<PathBuf>,

    /// Startup statistics dump threshold in milliseconds.  Dump stats if
    /// startup takes longer than this.  0 = disabled.
    /// Mirrors `STARTUP_DUMP_THRESHOLD` / default 0.
    pub startup_dump_threshold_ms: u64,

    // -----------------------------------------------------------------------
    // Callbacks
    // -----------------------------------------------------------------------
    /// Optional callback invoked when a background daemon thread encounters
    /// an exception.  Set this to receive notifications from the Checkpointer,
    /// Cleaner, Evictor, INCompressor, and Verifier daemons.
    ///
    /// Mirrors `EnvironmentConfig.setExceptionListener(ExceptionListener)`.
    pub exception_listener: ExceptionListenerHolder,
}

impl EnvironmentConfig {
    /// Creates a new `EnvironmentConfig` with the given home directory and
    /// -identical defaults for all parameters.
    pub fn new(home: PathBuf) -> Self {
        Self {
            home,
            // Core
            allow_create: false,
            transactional: false,
            read_only: false,
            env_is_locking: true,
            shared_cache: false,
            env_recovery_force_checkpoint: false,
            env_recovery_force_new_file: false,
            halt_on_commit_after_checksum_exception: false,
            // Memory
            cache_size: 64 * 1024 * 1024, // Noxu default: 64 MiB
            cache_percent: 0,
            max_off_heap_memory: 0,
            max_disk: 0,
            free_disk: 5 * 1024 * 1024 * 1024, //: 5 GiB
            reserved_disk: 0,
            // Daemon run flags
            run_in_compressor: true,
            run_checkpointer: true,
            run_cleaner: true,
            run_evictor: true,
            run_offheap_evictor: false,
            run_verifier: false,
            // Background daemon rate limits
            env_background_read_limit_kb: 0,
            env_background_write_limit_kb: 0,
            env_background_sleep_interval_us: 0,
            // Environment behaviour
            env_check_leaks: true,
            env_forced_yield: false,
            env_fair_latches: false,
            env_latch_timeout_ms: 300_000,
            env_ttl_clock_tolerance_ms: 0,
            env_expiration_enabled: false,
            env_db_eviction: false,
            // Log
            log_file_max_bytes: 10 * 1024 * 1024,
            log_file_cache_size: 100,
            log_checksum_read: true,
            log_verify_checksums: false,
            log_fsync_timeout_ms: 500_000,
            log_fsync_time_limit_ms: 0,
            log_num_buffers: 3,
            log_total_buffer_bytes: 7 * 1024 * 1024,
            log_buffer_size: 0,
            log_fault_read_size: 2048,
            log_iterator_read_size: 8192,
            log_iterator_max_size: 16 * 1024 * 1024,
            log_n_data_directories: 0,
            log_mem_only: false,
            log_detect_file_delete: false,
            log_detect_file_delete_interval_ms: 3_000,
            log_flush_sync_interval_ms: 0,
            log_flush_no_sync_interval_ms: 0,
            log_use_odsync: false,
            log_use_write_queue: false,
            log_write_queue_size: 1024 * 1024,
            log_group_commit_threshold: 0,
            log_group_commit_interval_ms: 0,
            log_fsync_max_leaders: 1,
            log_consolidation_array: false,
            // B-tree
            node_max_entries: 128,
            node_dup_tree_max_entries: 128,
            tree_max_embedded_ln: 16,
            tree_max_delta: 25,
            tree_bin_delta: true,
            tree_bin_delta_percent: 25,
            tree_min_memory: 0,
            tree_compact_max_key_length: 16,
            // INCompressor
            in_compressor_wakeup_interval_ms: 5_000,
            compressor_deadlock_retry: 3,
            compressor_lock_timeout_ms: 500,
            compressor_purge_root: false,
            // Cleaner
            cleaner_min_utilization: 50,
            cleaner_two_pass_gap: 10,
            cleaner_two_pass_threshold: 0,
            cleaner_min_file_utilization: 5,
            cleaner_threads: 1,
            cleaner_min_file_count: 2,
            cleaner_min_age: 2,
            cleaner_bytes_interval: 0,
            cleaner_wakeup_interval_ms: 0,
            cleaner_fetch_obsolete_size: false,
            cleaner_adjust_utilization: false,
            cleaner_deadlock_retry: 3,
            cleaner_lock_timeout_ms: 500,
            cleaner_expunge: true,
            cleaner_use_deleted_dir: false,
            cleaner_max_batch_files: 0,
            cleaner_read_size: 8192,
            cleaner_detail_max_memory_percentage: 2,
            cleaner_look_ahead_cache_size: 32,
            cleaner_foreground_proactive_migration: false,
            cleaner_background_proactive_migration: false,
            cleaner_lazy_migration: false,
            cleaner_expiration_enabled: false,
            // Checkpointer
            checkpointer_bytes_interval: 20_000_000,
            checkpointer_wakeup_interval_ms: 30_000,
            checkpointer_min_interval_secs: 0,
            checkpointer_deadlock_retry: 3,
            checkpointer_high_priority: false,
            // Evictor
            evictor_nodes_per_scan: 10,
            evictor_algorithm: "lru".to_string(),
            evictor_evict_bytes: 512 * 1024,
            evictor_critical_percentage: 5,
            evictor_lru_only: false,
            evictor_use_dirty_lru: true,
            evictor_mutate_bins: true,
            evictor_n_lru_lists: 4,
            evictor_deadlock_retry: 3,
            evictor_core_threads: 1,
            evictor_max_threads: 10,
            evictor_keep_alive_ms: 60_000,
            evictor_allow_bin_deltas: true,
            // Off-heap evictor
            offheap_evict_bytes: 512 * 1024,
            offheap_n_lru_lists: 4,
            offheap_checksum: false,
            offheap_core_threads: 1,
            offheap_max_threads: 10,
            offheap_keep_alive_ms: 60_000,
            // Locking
            lock_timeout_ms: 500,
            lock_n_lock_tables: 64, // Noxu default (documented deviation from JE default 1)
            lock_deadlock_detect: true,
            lock_deadlock_detect_delay_ms: 0,
            // Transactions
            txn_timeout_ms: 0,
            durability: Durability::default(),
            txn_no_sync: false,
            txn_write_no_sync: false,
            txn_serializable_isolation: false,
            txn_deadlock_stack_trace: false,
            txn_dump_locks: false,
            // Verifier
            verify_schedule: String::new(),
            verify_log: false,
            verify_log_read_delay_ms: 0,
            verify_btree: false,
            verify_secondaries: true,
            verify_data_records: false,
            verify_obsolete_records: false,
            verify_btree_batch_size: 1_000,
            verify_btree_batch_delay_ms: 10,
            // Disk-ordered cursor
            dos_producer_queue_timeout_ms: 10_000,
            // Stats
            stats_collect: false,
            stats_collect_interval_secs: 300,
            stats_max_files: 100,
            stats_file_row_count: 1_000,
            stats_file_directory: None,
            startup_dump_threshold_ms: 0,
            exception_listener: ExceptionListenerHolder(None),
        }
    }

    // -----------------------------------------------------------------------
    // Core setters
    // -----------------------------------------------------------------------

    pub fn set_allow_create(&mut self, v: bool) -> &mut Self {
        self.allow_create = v;
        self
    }
    pub fn with_allow_create(mut self, v: bool) -> Self {
        self.allow_create = v;
        self
    }

    pub fn set_transactional(&mut self, v: bool) -> &mut Self {
        self.transactional = v;
        self
    }
    pub fn with_transactional(mut self, v: bool) -> Self {
        self.transactional = v;
        self
    }

    pub fn set_read_only(&mut self, v: bool) -> &mut Self {
        self.read_only = v;
        self
    }
    pub fn with_read_only(mut self, v: bool) -> Self {
        self.read_only = v;
        self
    }

    pub fn set_env_is_locking(&mut self, v: bool) -> &mut Self {
        self.env_is_locking = v;
        self
    }

    pub fn set_shared_cache(&mut self, v: bool) -> &mut Self {
        self.shared_cache = v;
        self
    }

    pub fn set_env_recovery_force_checkpoint(&mut self, v: bool) -> &mut Self {
        self.env_recovery_force_checkpoint = v;
        self
    }

    pub fn set_env_recovery_force_new_file(&mut self, v: bool) -> &mut Self {
        self.env_recovery_force_new_file = v;
        self
    }

    pub fn set_halt_on_commit_after_checksum_exception(
        &mut self,
        v: bool,
    ) -> &mut Self {
        self.halt_on_commit_after_checksum_exception = v;
        self
    }

    // -----------------------------------------------------------------------
    // Memory setters
    // -----------------------------------------------------------------------

    pub fn set_cache_size(&mut self, bytes: u64) -> &mut Self {
        self.cache_size = bytes;
        self
    }
    pub fn with_cache_size(mut self, bytes: u64) -> Self {
        self.cache_size = bytes;
        self
    }

    pub fn set_cache_percent(&mut self, pct: u32) -> &mut Self {
        self.cache_percent = pct;
        self
    }

    pub fn set_max_off_heap_memory(&mut self, bytes: u64) -> &mut Self {
        self.max_off_heap_memory = bytes;
        self
    }

    pub fn set_max_disk(&mut self, bytes: u64) -> &mut Self {
        self.max_disk = bytes;
        self
    }

    pub fn set_free_disk(&mut self, bytes: u64) -> &mut Self {
        self.free_disk = bytes;
        self
    }

    /// Setter for [`Self::reserved_disk`] (`RESERVED_DISK`): extra bytes
    /// reserved on top of `free_disk`. 0 (default) reserves nothing.
    pub fn set_reserved_disk(&mut self, bytes: u64) -> &mut Self {
        self.reserved_disk = bytes;
        self
    }

    /// Builder form of [`Self::set_max_disk`] (`MAX_DISK`): absolute cap on
    /// total log size in bytes. 0 (default) disables the cap.
    pub fn with_max_disk(mut self, bytes: u64) -> Self {
        self.max_disk = bytes;
        self
    }

    /// Builder form of [`Self::set_free_disk`] (`FREE_DISK`): keep-this-much
    /// free on the filesystem, in bytes. 0 disables the free-space reserve.
    pub fn with_free_disk(mut self, bytes: u64) -> Self {
        self.free_disk = bytes;
        self
    }

    /// Builder form of [`Self::set_reserved_disk`] (`RESERVED_DISK`): reserve
    /// this many bytes on top of `free_disk`; writes fail when free space
    /// drops below `free_disk + reserved_disk`. 0 (default) reserves nothing.
    /// Mirrors JE `EnvironmentParams.RESERVED_DISK`.
    pub fn with_reserved_disk(mut self, bytes: u64) -> Self {
        self.reserved_disk = bytes;
        self
    }

    // -----------------------------------------------------------------------
    // Daemon run-flag setters
    // -----------------------------------------------------------------------

    pub fn set_run_in_compressor(&mut self, v: bool) -> &mut Self {
        self.run_in_compressor = v;
        self
    }
    pub fn set_run_checkpointer(&mut self, v: bool) -> &mut Self {
        self.run_checkpointer = v;
        self
    }
    pub fn set_run_cleaner(&mut self, v: bool) -> &mut Self {
        self.run_cleaner = v;
        self
    }
    pub fn set_run_evictor(&mut self, v: bool) -> &mut Self {
        self.run_evictor = v;
        self
    }
    pub fn set_run_offheap_evictor(&mut self, v: bool) -> &mut Self {
        self.run_offheap_evictor = v;
        self
    }
    pub fn set_run_verifier(&mut self, v: bool) -> &mut Self {
        self.run_verifier = v;
        self
    }

    // -----------------------------------------------------------------------
    // Background daemon rate / sleep
    // -----------------------------------------------------------------------

    pub fn set_env_background_read_limit_kb(&mut self, kb: u32) -> &mut Self {
        self.env_background_read_limit_kb = kb;
        self
    }
    pub fn set_env_background_write_limit_kb(&mut self, kb: u32) -> &mut Self {
        self.env_background_write_limit_kb = kb;
        self
    }
    pub fn set_env_background_sleep_interval_us(
        &mut self,
        us: u64,
    ) -> &mut Self {
        self.env_background_sleep_interval_us = us;
        self
    }

    // -----------------------------------------------------------------------
    // Environment behaviour setters
    // -----------------------------------------------------------------------

    pub fn set_env_check_leaks(&mut self, v: bool) -> &mut Self {
        self.env_check_leaks = v;
        self
    }
    pub fn set_env_forced_yield(&mut self, v: bool) -> &mut Self {
        self.env_forced_yield = v;
        self
    }
    pub fn set_env_fair_latches(&mut self, v: bool) -> &mut Self {
        self.env_fair_latches = v;
        self
    }
    pub fn set_env_latch_timeout_ms(&mut self, ms: u64) -> &mut Self {
        self.env_latch_timeout_ms = ms;
        self
    }
    pub fn set_env_ttl_clock_tolerance_ms(&mut self, ms: u64) -> &mut Self {
        self.env_ttl_clock_tolerance_ms = ms;
        self
    }
    pub fn set_env_expiration_enabled(&mut self, v: bool) -> &mut Self {
        self.env_expiration_enabled = v;
        self
    }
    pub fn set_env_db_eviction(&mut self, v: bool) -> &mut Self {
        self.env_db_eviction = v;
        self
    }

    // -----------------------------------------------------------------------
    // Log setters
    // -----------------------------------------------------------------------

    pub fn set_log_file_max_bytes(&mut self, bytes: u64) -> &mut Self {
        self.log_file_max_bytes = bytes;
        self
    }
    pub fn with_log_file_max_bytes(mut self, bytes: u64) -> Self {
        self.log_file_max_bytes = bytes;
        self
    }
    pub fn set_log_file_cache_size(&mut self, n: usize) -> &mut Self {
        self.log_file_cache_size = n;
        self
    }
    pub fn set_log_checksum_read(&mut self, v: bool) -> &mut Self {
        self.log_checksum_read = v;
        self
    }
    pub fn set_log_verify_checksums(&mut self, v: bool) -> &mut Self {
        self.log_verify_checksums = v;
        self
    }
    pub fn set_log_fsync_timeout_ms(&mut self, ms: u64) -> &mut Self {
        self.log_fsync_timeout_ms = ms;
        self
    }
    pub fn set_log_fsync_time_limit_ms(&mut self, ms: u64) -> &mut Self {
        self.log_fsync_time_limit_ms = ms;
        self
    }
    pub fn set_log_num_buffers(&mut self, n: usize) -> &mut Self {
        self.log_num_buffers = n;
        self
    }
    pub fn set_log_total_buffer_bytes(&mut self, bytes: u64) -> &mut Self {
        self.log_total_buffer_bytes = bytes;
        self
    }
    pub fn set_log_buffer_size(&mut self, bytes: usize) -> &mut Self {
        self.log_buffer_size = bytes;
        self
    }
    pub fn set_log_fault_read_size(&mut self, bytes: usize) -> &mut Self {
        self.log_fault_read_size = bytes;
        self
    }
    pub fn set_log_iterator_read_size(&mut self, bytes: usize) -> &mut Self {
        self.log_iterator_read_size = bytes;
        self
    }
    pub fn set_log_iterator_max_size(&mut self, bytes: usize) -> &mut Self {
        self.log_iterator_max_size = bytes;
        self
    }
    pub fn set_log_n_data_directories(&mut self, n: u32) -> &mut Self {
        self.log_n_data_directories = n;
        self
    }
    pub fn set_log_mem_only(&mut self, v: bool) -> &mut Self {
        self.log_mem_only = v;
        self
    }
    pub fn set_log_detect_file_delete(&mut self, v: bool) -> &mut Self {
        self.log_detect_file_delete = v;
        self
    }
    pub fn set_log_detect_file_delete_interval_ms(
        &mut self,
        ms: u64,
    ) -> &mut Self {
        self.log_detect_file_delete_interval_ms = ms;
        self
    }
    pub fn set_log_flush_sync_interval_ms(&mut self, ms: u64) -> &mut Self {
        self.log_flush_sync_interval_ms = ms;
        self
    }
    pub fn set_log_flush_no_sync_interval_ms(&mut self, ms: u64) -> &mut Self {
        self.log_flush_no_sync_interval_ms = ms;
        self
    }
    pub fn set_log_use_odsync(&mut self, v: bool) -> &mut Self {
        self.log_use_odsync = v;
        self
    }
    pub fn set_log_use_write_queue(&mut self, v: bool) -> &mut Self {
        self.log_use_write_queue = v;
        self
    }
    pub fn set_log_write_queue_size(&mut self, bytes: usize) -> &mut Self {
        self.log_write_queue_size = bytes;
        self
    }
    pub fn set_log_group_commit_threshold(&mut self, n: usize) -> &mut Self {
        self.log_group_commit_threshold = n;
        self
    }
    pub fn set_log_group_commit_interval_ms(&mut self, ms: u64) -> &mut Self {
        self.log_group_commit_interval_ms = ms;
        self
    }
    /// Set the bounded fsync pipeline depth (max concurrent `fdatasync`s).
    /// `1` = single-leader (default); `2`/`4`/`8` trade a slightly higher tail
    /// for write throughput.  Clamped to `>= 1` by the `FsyncManager`.
    pub fn set_log_fsync_max_leaders(&mut self, n: usize) -> &mut Self {
        self.log_fsync_max_leaders = n;
        self
    }

    /// Enable the consolidation-array Log Write Latch (Aether/Silo/WT).
    ///
    /// Off by default (classic mutex LWL).  When enabled, concurrent
    /// committers combine into one batch via a lock-free CAS-join and a single
    /// leader drives the batch under one latch acquisition, relieving the LWL
    /// contention that serialises writes.  Single WAL + single monotonic LSN
    /// preserved; on-disk format identical.  Mirrors `LOG_CONSOLIDATION_ARRAY`.
    pub fn set_log_consolidation_array(&mut self, enabled: bool) -> &mut Self {
        self.log_consolidation_array = enabled;
        self
    }
    pub fn with_log_group_commit(
        mut self,
        threshold: usize,
        interval_ms: u64,
    ) -> Self {
        self.log_group_commit_threshold = threshold;
        self.log_group_commit_interval_ms = interval_ms;
        self
    }

    // -----------------------------------------------------------------------
    // B-tree setters
    // -----------------------------------------------------------------------

    pub fn set_node_max_entries(&mut self, n: u32) -> &mut Self {
        self.node_max_entries = n;
        self
    }
    pub fn set_node_dup_tree_max_entries(&mut self, n: u32) -> &mut Self {
        self.node_dup_tree_max_entries = n;
        self
    }
    pub fn set_tree_max_embedded_ln(&mut self, bytes: u32) -> &mut Self {
        self.tree_max_embedded_ln = bytes;
        self
    }
    pub fn set_tree_max_delta(&mut self, pct: u8) -> &mut Self {
        self.tree_max_delta = pct;
        self
    }
    pub fn set_tree_bin_delta(&mut self, v: bool) -> &mut Self {
        self.tree_bin_delta = v;
        self
    }

    /// Set the BIN-delta percent threshold (0–75, JE `BIN_DELTA_PERCENT`).
    pub fn set_tree_bin_delta_percent(&mut self, pct: u8) -> &mut Self {
        self.tree_bin_delta_percent = pct;
        self
    }
    pub fn set_tree_min_memory(&mut self, bytes: u64) -> &mut Self {
        self.tree_min_memory = bytes;
        self
    }
    pub fn set_tree_compact_max_key_length(&mut self, bytes: u32) -> &mut Self {
        self.tree_compact_max_key_length = bytes;
        self
    }

    // -----------------------------------------------------------------------
    // INCompressor setters
    // -----------------------------------------------------------------------

    pub fn set_in_compressor_wakeup_interval_ms(
        &mut self,
        ms: u64,
    ) -> &mut Self {
        self.in_compressor_wakeup_interval_ms = ms;
        self
    }
    pub fn set_compressor_deadlock_retry(&mut self, n: u32) -> &mut Self {
        self.compressor_deadlock_retry = n;
        self
    }
    pub fn set_compressor_lock_timeout_ms(&mut self, ms: u64) -> &mut Self {
        self.compressor_lock_timeout_ms = ms;
        self
    }
    pub fn set_compressor_purge_root(&mut self, v: bool) -> &mut Self {
        self.compressor_purge_root = v;
        self
    }

    // -----------------------------------------------------------------------
    // Cleaner setters
    // -----------------------------------------------------------------------

    pub fn set_cleaner_two_pass_gap(&mut self, gap: i32) -> &mut Self {
        self.cleaner_two_pass_gap = gap;
        self
    }

    pub fn set_cleaner_two_pass_threshold(&mut self, t: i32) -> &mut Self {
        self.cleaner_two_pass_threshold = t;
        self
    }

    pub fn set_cleaner_min_utilization(&mut self, pct: u8) -> &mut Self {
        self.cleaner_min_utilization = pct;
        self
    }
    pub fn with_cleaner_min_utilization(mut self, pct: u8) -> Self {
        self.cleaner_min_utilization = pct;
        self
    }
    pub fn set_cleaner_min_file_utilization(&mut self, pct: u8) -> &mut Self {
        self.cleaner_min_file_utilization = pct;
        self
    }
    pub fn set_cleaner_threads(&mut self, n: u32) -> &mut Self {
        self.cleaner_threads = n;
        self
    }
    pub fn set_cleaner_min_file_count(&mut self, n: u32) -> &mut Self {
        self.cleaner_min_file_count = n;
        self
    }
    pub fn set_cleaner_min_age(&mut self, checkpoints: u32) -> &mut Self {
        self.cleaner_min_age = checkpoints;
        self
    }
    pub fn set_cleaner_bytes_interval(&mut self, bytes: u64) -> &mut Self {
        self.cleaner_bytes_interval = bytes;
        self
    }
    pub fn set_cleaner_wakeup_interval_ms(&mut self, ms: u64) -> &mut Self {
        self.cleaner_wakeup_interval_ms = ms;
        self
    }
    pub fn set_cleaner_fetch_obsolete_size(&mut self, v: bool) -> &mut Self {
        self.cleaner_fetch_obsolete_size = v;
        self
    }
    pub fn set_cleaner_adjust_utilization(&mut self, v: bool) -> &mut Self {
        self.cleaner_adjust_utilization = v;
        self
    }
    pub fn set_cleaner_deadlock_retry(&mut self, n: u32) -> &mut Self {
        self.cleaner_deadlock_retry = n;
        self
    }
    pub fn set_cleaner_lock_timeout_ms(&mut self, ms: u64) -> &mut Self {
        self.cleaner_lock_timeout_ms = ms;
        self
    }
    pub fn set_cleaner_expunge(&mut self, v: bool) -> &mut Self {
        self.cleaner_expunge = v;
        self
    }
    pub fn set_cleaner_use_deleted_dir(&mut self, v: bool) -> &mut Self {
        self.cleaner_use_deleted_dir = v;
        self
    }
    pub fn set_cleaner_max_batch_files(&mut self, n: u32) -> &mut Self {
        self.cleaner_max_batch_files = n;
        self
    }
    pub fn set_cleaner_read_size(&mut self, bytes: usize) -> &mut Self {
        self.cleaner_read_size = bytes;
        self
    }
    pub fn set_cleaner_detail_max_memory_percentage(
        &mut self,
        pct: u32,
    ) -> &mut Self {
        self.cleaner_detail_max_memory_percentage = pct;
        self
    }
    pub fn set_cleaner_look_ahead_cache_size(&mut self, n: usize) -> &mut Self {
        self.cleaner_look_ahead_cache_size = n;
        self
    }
    pub fn set_cleaner_foreground_proactive_migration(
        &mut self,
        v: bool,
    ) -> &mut Self {
        self.cleaner_foreground_proactive_migration = v;
        self
    }
    pub fn set_cleaner_background_proactive_migration(
        &mut self,
        v: bool,
    ) -> &mut Self {
        self.cleaner_background_proactive_migration = v;
        self
    }
    pub fn set_cleaner_lazy_migration(&mut self, v: bool) -> &mut Self {
        self.cleaner_lazy_migration = v;
        self
    }
    pub fn set_cleaner_expiration_enabled(&mut self, v: bool) -> &mut Self {
        self.cleaner_expiration_enabled = v;
        self
    }

    // -----------------------------------------------------------------------
    // Checkpointer setters
    // -----------------------------------------------------------------------

    pub fn set_checkpointer_bytes_interval(&mut self, bytes: u64) -> &mut Self {
        self.checkpointer_bytes_interval = bytes;
        self
    }
    pub fn with_checkpointer_bytes_interval(mut self, bytes: u64) -> Self {
        self.checkpointer_bytes_interval = bytes;
        self
    }
    pub fn set_checkpointer_wakeup_interval_ms(
        &mut self,
        ms: u64,
    ) -> &mut Self {
        self.checkpointer_wakeup_interval_ms = ms;
        self
    }
    pub fn set_checkpointer_min_interval_secs(
        &mut self,
        secs: u64,
    ) -> &mut Self {
        self.checkpointer_min_interval_secs = secs;
        self
    }
    pub fn set_checkpointer_deadlock_retry(&mut self, n: u32) -> &mut Self {
        self.checkpointer_deadlock_retry = n;
        self
    }
    pub fn set_checkpointer_high_priority(&mut self, v: bool) -> &mut Self {
        self.checkpointer_high_priority = v;
        self
    }

    // -----------------------------------------------------------------------
    // Evictor setters
    // -----------------------------------------------------------------------

    pub fn set_evictor_nodes_per_scan(&mut self, n: usize) -> &mut Self {
        self.evictor_nodes_per_scan = n;
        self
    }
    pub fn with_evictor_nodes_per_scan(mut self, n: usize) -> Self {
        self.evictor_nodes_per_scan = n;
        self
    }
    /// Set the cache eviction algorithm ("lru"|"clock"|"arc"|"car"|"lirs").
    pub fn set_evictor_algorithm(
        &mut self,
        algo: impl Into<String>,
    ) -> &mut Self {
        self.evictor_algorithm = algo.into();
        self
    }
    /// Builder form of [`set_evictor_algorithm`](Self::set_evictor_algorithm).
    pub fn with_evictor_algorithm(mut self, algo: impl Into<String>) -> Self {
        self.evictor_algorithm = algo.into();
        self
    }
    pub fn set_evictor_evict_bytes(&mut self, bytes: u64) -> &mut Self {
        self.evictor_evict_bytes = bytes;
        self
    }
    pub fn set_evictor_critical_percentage(&mut self, pct: u32) -> &mut Self {
        self.evictor_critical_percentage = pct;
        self
    }
    pub fn set_evictor_use_dirty_lru(&mut self, v: bool) -> &mut Self {
        self.evictor_use_dirty_lru = v;
        self
    }

    /// Setter for [`Self::evictor_mutate_bins`] (`EVICTOR_MUTATE_BINS`).
    pub fn set_evictor_mutate_bins(&mut self, v: bool) -> &mut Self {
        self.evictor_mutate_bins = v;
        self
    }

    pub fn set_evictor_lru_only(&mut self, v: bool) -> &mut Self {
        self.evictor_lru_only = v;
        self
    }
    pub fn set_evictor_n_lru_lists(&mut self, n: u32) -> &mut Self {
        self.evictor_n_lru_lists = n;
        self
    }
    pub fn set_evictor_deadlock_retry(&mut self, n: u32) -> &mut Self {
        self.evictor_deadlock_retry = n;
        self
    }
    pub fn set_evictor_core_threads(&mut self, n: usize) -> &mut Self {
        self.evictor_core_threads = n;
        self
    }
    pub fn set_evictor_max_threads(&mut self, n: usize) -> &mut Self {
        self.evictor_max_threads = n;
        self
    }
    pub fn set_evictor_keep_alive_ms(&mut self, ms: u64) -> &mut Self {
        self.evictor_keep_alive_ms = ms;
        self
    }
    pub fn set_evictor_allow_bin_deltas(&mut self, v: bool) -> &mut Self {
        self.evictor_allow_bin_deltas = v;
        self
    }

    // -----------------------------------------------------------------------
    // Off-heap evictor setters
    // -----------------------------------------------------------------------

    pub fn set_offheap_evict_bytes(&mut self, bytes: u64) -> &mut Self {
        self.offheap_evict_bytes = bytes;
        self
    }
    pub fn set_offheap_n_lru_lists(&mut self, n: u32) -> &mut Self {
        self.offheap_n_lru_lists = n;
        self
    }
    pub fn set_offheap_checksum(&mut self, v: bool) -> &mut Self {
        self.offheap_checksum = v;
        self
    }
    pub fn set_offheap_core_threads(&mut self, n: usize) -> &mut Self {
        self.offheap_core_threads = n;
        self
    }
    pub fn set_offheap_max_threads(&mut self, n: usize) -> &mut Self {
        self.offheap_max_threads = n;
        self
    }
    pub fn set_offheap_keep_alive_ms(&mut self, ms: u64) -> &mut Self {
        self.offheap_keep_alive_ms = ms;
        self
    }

    // -----------------------------------------------------------------------
    // Locking setters
    // -----------------------------------------------------------------------

    pub fn set_lock_timeout(&mut self, ms: u64) -> &mut Self {
        self.lock_timeout_ms = ms;
        self
    }
    pub fn set_lock_n_lock_tables(&mut self, n: u32) -> &mut Self {
        self.lock_n_lock_tables = n;
        self
    }
    pub fn set_lock_deadlock_detect(&mut self, v: bool) -> &mut Self {
        self.lock_deadlock_detect = v;
        self
    }
    pub fn set_lock_deadlock_detect_delay_ms(&mut self, ms: u64) -> &mut Self {
        self.lock_deadlock_detect_delay_ms = ms;
        self
    }

    // -----------------------------------------------------------------------
    // Transaction setters
    // -----------------------------------------------------------------------

    pub fn set_txn_timeout(&mut self, ms: u64) -> &mut Self {
        self.txn_timeout_ms = ms;
        self
    }
    pub fn set_durability(&mut self, d: Durability) -> &mut Self {
        self.durability = d;
        self
    }
    pub fn with_durability(mut self, d: Durability) -> Self {
        self.durability = d;
        self
    }
    /// Sets `txn_no_sync`.
    ///
    /// **Deprecated** — use [`set_durability`][Self::set_durability] /
    /// [`with_durability`][Self::with_durability] instead.
    #[deprecated(
        since = "2.4.1",
        note = "use set_durability(Durability::commit_no_sync()) instead"
    )]
    pub fn set_txn_no_sync(&mut self, v: bool) -> &mut Self {
        self.txn_no_sync = v;
        self
    }
    /// Builder-style `txn_no_sync`.
    ///
    /// **Deprecated** — use [`with_durability`][Self::with_durability] instead.
    #[deprecated(
        since = "2.4.1",
        note = "use with_durability(Durability::commit_no_sync()) instead"
    )]
    pub fn with_txn_no_sync(mut self, v: bool) -> Self {
        self.txn_no_sync = v;
        self
    }
    /// Sets `txn_write_no_sync`.
    ///
    /// **Deprecated** — use [`set_durability`][Self::set_durability] instead.
    #[deprecated(
        since = "2.4.1",
        note = "use set_durability(Durability::commit_write_no_sync()) instead"
    )]
    pub fn set_txn_write_no_sync(&mut self, v: bool) -> &mut Self {
        self.txn_write_no_sync = v;
        self
    }
    pub fn set_txn_serializable_isolation(&mut self, v: bool) -> &mut Self {
        self.txn_serializable_isolation = v;
        self
    }
    pub fn set_txn_deadlock_stack_trace(&mut self, v: bool) -> &mut Self {
        self.txn_deadlock_stack_trace = v;
        self
    }
    pub fn set_txn_dump_locks(&mut self, v: bool) -> &mut Self {
        self.txn_dump_locks = v;
        self
    }

    // -----------------------------------------------------------------------
    // Verifier setters
    // -----------------------------------------------------------------------

    pub fn set_verify_schedule(&mut self, s: String) -> &mut Self {
        self.verify_schedule = s;
        self
    }
    pub fn set_verify_log(&mut self, v: bool) -> &mut Self {
        self.verify_log = v;
        self
    }
    pub fn set_verify_log_read_delay_ms(&mut self, ms: u64) -> &mut Self {
        self.verify_log_read_delay_ms = ms;
        self
    }
    pub fn set_verify_btree(&mut self, v: bool) -> &mut Self {
        self.verify_btree = v;
        self
    }
    pub fn set_verify_secondaries(&mut self, v: bool) -> &mut Self {
        self.verify_secondaries = v;
        self
    }
    pub fn set_verify_data_records(&mut self, v: bool) -> &mut Self {
        self.verify_data_records = v;
        self
    }
    pub fn set_verify_obsolete_records(&mut self, v: bool) -> &mut Self {
        self.verify_obsolete_records = v;
        self
    }
    pub fn set_verify_btree_batch_size(&mut self, n: u32) -> &mut Self {
        self.verify_btree_batch_size = n;
        self
    }
    pub fn set_verify_btree_batch_delay_ms(&mut self, ms: u64) -> &mut Self {
        self.verify_btree_batch_delay_ms = ms;
        self
    }

    // -----------------------------------------------------------------------
    // Disk-ordered cursor setters
    // -----------------------------------------------------------------------

    pub fn set_dos_producer_queue_timeout_ms(&mut self, ms: u64) -> &mut Self {
        self.dos_producer_queue_timeout_ms = ms;
        self
    }

    // -----------------------------------------------------------------------
    // Stats setters
    // -----------------------------------------------------------------------

    pub fn set_stats_collect(&mut self, v: bool) -> &mut Self {
        self.stats_collect = v;
        self
    }
    pub fn set_stats_collect_interval_secs(&mut self, secs: u64) -> &mut Self {
        self.stats_collect_interval_secs = secs;
        self
    }
    pub fn set_stats_max_files(&mut self, n: u32) -> &mut Self {
        self.stats_max_files = n;
        self
    }
    pub fn set_stats_file_row_count(&mut self, n: u32) -> &mut Self {
        self.stats_file_row_count = n;
        self
    }
    pub fn set_stats_file_directory(&mut self, dir: PathBuf) -> &mut Self {
        self.stats_file_directory = Some(dir);
        self
    }

    pub fn set_startup_dump_threshold_ms(&mut self, ms: u64) -> &mut Self {
        self.startup_dump_threshold_ms = ms;
        self
    }

    // -----------------------------------------------------------------------
    // Callback setters
    // -----------------------------------------------------------------------

    /// Registers a callback to be invoked when a background daemon thread
    /// encounters an unhandled exception.
    ///
    /// Mirrors `EnvironmentConfig.setExceptionListener(ExceptionListener)`.
    pub fn set_exception_listener(
        &mut self,
        listener: Arc<dyn ExceptionListener>,
    ) -> &mut Self {
        self.exception_listener = ExceptionListenerHolder(Some(listener));
        self
    }

    /// Returns the registered `ExceptionListener`, if any.
    pub fn exception_listener(&self) -> Option<Arc<dyn ExceptionListener>> {
        self.exception_listener.0.clone()
    }

    // ── Consuming (chainable) `with_*` builders (review P1-8) ──────────
    // Generated for every (non-deprecated) `set_*` parameter so the
    // chained-builder form works uniformly for all parameters. Each
    // delegates to its `set_*` counterpart, reusing any validation.
    // The struct-level `#[must_use]` already covers the returned `Self`.

    /// Builder-style (consuming) `env_is_locking` setter. See [`Self::set_env_is_locking`].
    pub fn with_env_is_locking(mut self, v: bool) -> Self {
        self.set_env_is_locking(v);
        self
    }

    /// Builder-style (consuming) `shared_cache` setter. See [`Self::set_shared_cache`].
    pub fn with_shared_cache(mut self, v: bool) -> Self {
        self.set_shared_cache(v);
        self
    }

    /// Builder-style (consuming) `env_recovery_force_checkpoint` setter. See [`Self::set_env_recovery_force_checkpoint`].
    pub fn with_env_recovery_force_checkpoint(mut self, v: bool) -> Self {
        self.set_env_recovery_force_checkpoint(v);
        self
    }

    /// Builder-style (consuming) `env_recovery_force_new_file` setter. See [`Self::set_env_recovery_force_new_file`].
    pub fn with_env_recovery_force_new_file(mut self, v: bool) -> Self {
        self.set_env_recovery_force_new_file(v);
        self
    }

    /// Builder-style (consuming) `halt_on_commit_after_checksum_exception` setter. See [`Self::set_halt_on_commit_after_checksum_exception`].
    pub fn with_halt_on_commit_after_checksum_exception(
        mut self,
        v: bool,
    ) -> Self {
        self.set_halt_on_commit_after_checksum_exception(v);
        self
    }

    /// Builder-style (consuming) `cache_percent` setter. See [`Self::set_cache_percent`].
    pub fn with_cache_percent(mut self, pct: u32) -> Self {
        self.set_cache_percent(pct);
        self
    }

    /// Builder-style (consuming) `max_off_heap_memory` setter. See [`Self::set_max_off_heap_memory`].
    pub fn with_max_off_heap_memory(mut self, bytes: u64) -> Self {
        self.set_max_off_heap_memory(bytes);
        self
    }

    /// Builder-style (consuming) `run_in_compressor` setter. See [`Self::set_run_in_compressor`].
    pub fn with_run_in_compressor(mut self, v: bool) -> Self {
        self.set_run_in_compressor(v);
        self
    }

    /// Builder-style (consuming) `run_checkpointer` setter. See [`Self::set_run_checkpointer`].
    pub fn with_run_checkpointer(mut self, v: bool) -> Self {
        self.set_run_checkpointer(v);
        self
    }

    /// Builder-style (consuming) `run_cleaner` setter. See [`Self::set_run_cleaner`].
    pub fn with_run_cleaner(mut self, v: bool) -> Self {
        self.set_run_cleaner(v);
        self
    }

    /// Builder-style (consuming) `run_evictor` setter. See [`Self::set_run_evictor`].
    pub fn with_run_evictor(mut self, v: bool) -> Self {
        self.set_run_evictor(v);
        self
    }

    /// Builder-style (consuming) `run_offheap_evictor` setter. See [`Self::set_run_offheap_evictor`].
    pub fn with_run_offheap_evictor(mut self, v: bool) -> Self {
        self.set_run_offheap_evictor(v);
        self
    }

    /// Builder-style (consuming) `run_verifier` setter. See [`Self::set_run_verifier`].
    pub fn with_run_verifier(mut self, v: bool) -> Self {
        self.set_run_verifier(v);
        self
    }

    /// Builder-style (consuming) `env_background_read_limit_kb` setter. See [`Self::set_env_background_read_limit_kb`].
    pub fn with_env_background_read_limit_kb(mut self, kb: u32) -> Self {
        self.set_env_background_read_limit_kb(kb);
        self
    }

    /// Builder-style (consuming) `env_background_write_limit_kb` setter. See [`Self::set_env_background_write_limit_kb`].
    pub fn with_env_background_write_limit_kb(mut self, kb: u32) -> Self {
        self.set_env_background_write_limit_kb(kb);
        self
    }

    /// Builder-style (consuming) `env_background_sleep_interval_us` setter. See [`Self::set_env_background_sleep_interval_us`].
    pub fn with_env_background_sleep_interval_us(mut self, us: u64) -> Self {
        self.set_env_background_sleep_interval_us(us);
        self
    }

    /// Builder-style (consuming) `env_check_leaks` setter. See [`Self::set_env_check_leaks`].
    pub fn with_env_check_leaks(mut self, v: bool) -> Self {
        self.set_env_check_leaks(v);
        self
    }

    /// Builder-style (consuming) `env_forced_yield` setter. See [`Self::set_env_forced_yield`].
    pub fn with_env_forced_yield(mut self, v: bool) -> Self {
        self.set_env_forced_yield(v);
        self
    }

    /// Builder-style (consuming) `env_fair_latches` setter. See [`Self::set_env_fair_latches`].
    pub fn with_env_fair_latches(mut self, v: bool) -> Self {
        self.set_env_fair_latches(v);
        self
    }

    /// Builder-style (consuming) `env_latch_timeout_ms` setter. See [`Self::set_env_latch_timeout_ms`].
    pub fn with_env_latch_timeout_ms(mut self, ms: u64) -> Self {
        self.set_env_latch_timeout_ms(ms);
        self
    }

    /// Builder-style (consuming) `env_ttl_clock_tolerance_ms` setter. See [`Self::set_env_ttl_clock_tolerance_ms`].
    pub fn with_env_ttl_clock_tolerance_ms(mut self, ms: u64) -> Self {
        self.set_env_ttl_clock_tolerance_ms(ms);
        self
    }

    /// Builder-style (consuming) `env_expiration_enabled` setter. See [`Self::set_env_expiration_enabled`].
    pub fn with_env_expiration_enabled(mut self, v: bool) -> Self {
        self.set_env_expiration_enabled(v);
        self
    }

    /// Builder-style (consuming) `env_db_eviction` setter. See [`Self::set_env_db_eviction`].
    pub fn with_env_db_eviction(mut self, v: bool) -> Self {
        self.set_env_db_eviction(v);
        self
    }

    /// Builder-style (consuming) `log_file_cache_size` setter. See [`Self::set_log_file_cache_size`].
    pub fn with_log_file_cache_size(mut self, n: usize) -> Self {
        self.set_log_file_cache_size(n);
        self
    }

    /// Builder-style (consuming) `log_checksum_read` setter. See [`Self::set_log_checksum_read`].
    pub fn with_log_checksum_read(mut self, v: bool) -> Self {
        self.set_log_checksum_read(v);
        self
    }

    /// Builder-style (consuming) `log_verify_checksums` setter. See [`Self::set_log_verify_checksums`].
    pub fn with_log_verify_checksums(mut self, v: bool) -> Self {
        self.set_log_verify_checksums(v);
        self
    }

    /// Builder-style (consuming) `log_fsync_timeout_ms` setter. See [`Self::set_log_fsync_timeout_ms`].
    pub fn with_log_fsync_timeout_ms(mut self, ms: u64) -> Self {
        self.set_log_fsync_timeout_ms(ms);
        self
    }

    /// Builder-style (consuming) `log_fsync_time_limit_ms` setter. See [`Self::set_log_fsync_time_limit_ms`].
    pub fn with_log_fsync_time_limit_ms(mut self, ms: u64) -> Self {
        self.set_log_fsync_time_limit_ms(ms);
        self
    }

    /// Builder-style (consuming) `log_num_buffers` setter. See [`Self::set_log_num_buffers`].
    pub fn with_log_num_buffers(mut self, n: usize) -> Self {
        self.set_log_num_buffers(n);
        self
    }

    /// Builder-style (consuming) `log_total_buffer_bytes` setter. See [`Self::set_log_total_buffer_bytes`].
    pub fn with_log_total_buffer_bytes(mut self, bytes: u64) -> Self {
        self.set_log_total_buffer_bytes(bytes);
        self
    }

    /// Builder-style (consuming) `log_buffer_size` setter. See [`Self::set_log_buffer_size`].
    pub fn with_log_buffer_size(mut self, bytes: usize) -> Self {
        self.set_log_buffer_size(bytes);
        self
    }

    /// Builder-style (consuming) `log_fault_read_size` setter. See [`Self::set_log_fault_read_size`].
    pub fn with_log_fault_read_size(mut self, bytes: usize) -> Self {
        self.set_log_fault_read_size(bytes);
        self
    }

    /// Builder-style (consuming) `log_iterator_read_size` setter. See [`Self::set_log_iterator_read_size`].
    pub fn with_log_iterator_read_size(mut self, bytes: usize) -> Self {
        self.set_log_iterator_read_size(bytes);
        self
    }

    /// Builder-style (consuming) `log_iterator_max_size` setter. See [`Self::set_log_iterator_max_size`].
    pub fn with_log_iterator_max_size(mut self, bytes: usize) -> Self {
        self.set_log_iterator_max_size(bytes);
        self
    }

    /// Builder-style (consuming) `log_n_data_directories` setter. See [`Self::set_log_n_data_directories`].
    pub fn with_log_n_data_directories(mut self, n: u32) -> Self {
        self.set_log_n_data_directories(n);
        self
    }

    /// Builder-style (consuming) `log_mem_only` setter. See [`Self::set_log_mem_only`].
    pub fn with_log_mem_only(mut self, v: bool) -> Self {
        self.set_log_mem_only(v);
        self
    }

    /// Builder-style (consuming) `log_detect_file_delete` setter. See [`Self::set_log_detect_file_delete`].
    pub fn with_log_detect_file_delete(mut self, v: bool) -> Self {
        self.set_log_detect_file_delete(v);
        self
    }

    /// Builder-style (consuming) `log_detect_file_delete_interval_ms` setter. See [`Self::set_log_detect_file_delete_interval_ms`].
    pub fn with_log_detect_file_delete_interval_ms(mut self, ms: u64) -> Self {
        self.set_log_detect_file_delete_interval_ms(ms);
        self
    }

    /// Builder-style (consuming) `log_flush_sync_interval_ms` setter. See [`Self::set_log_flush_sync_interval_ms`].
    pub fn with_log_flush_sync_interval_ms(mut self, ms: u64) -> Self {
        self.set_log_flush_sync_interval_ms(ms);
        self
    }

    /// Builder-style (consuming) `log_flush_no_sync_interval_ms` setter. See [`Self::set_log_flush_no_sync_interval_ms`].
    pub fn with_log_flush_no_sync_interval_ms(mut self, ms: u64) -> Self {
        self.set_log_flush_no_sync_interval_ms(ms);
        self
    }

    /// Builder-style (consuming) `log_use_odsync` setter. See [`Self::set_log_use_odsync`].
    pub fn with_log_use_odsync(mut self, v: bool) -> Self {
        self.set_log_use_odsync(v);
        self
    }

    /// Builder-style (consuming) `log_use_write_queue` setter. See [`Self::set_log_use_write_queue`].
    pub fn with_log_use_write_queue(mut self, v: bool) -> Self {
        self.set_log_use_write_queue(v);
        self
    }

    /// Builder-style (consuming) `log_write_queue_size` setter. See [`Self::set_log_write_queue_size`].
    pub fn with_log_write_queue_size(mut self, bytes: usize) -> Self {
        self.set_log_write_queue_size(bytes);
        self
    }

    /// Builder-style (consuming) `log_group_commit_threshold` setter. See [`Self::set_log_group_commit_threshold`].
    pub fn with_log_group_commit_threshold(mut self, n: usize) -> Self {
        self.set_log_group_commit_threshold(n);
        self
    }

    /// Builder-style (consuming) `log_group_commit_interval_ms` setter. See [`Self::set_log_group_commit_interval_ms`].
    pub fn with_log_group_commit_interval_ms(mut self, ms: u64) -> Self {
        self.set_log_group_commit_interval_ms(ms);
        self
    }

    /// Builder-style (consuming) `node_max_entries` setter. See [`Self::set_node_max_entries`].
    pub fn with_node_max_entries(mut self, n: u32) -> Self {
        self.set_node_max_entries(n);
        self
    }

    /// Builder-style (consuming) `node_dup_tree_max_entries` setter. See [`Self::set_node_dup_tree_max_entries`].
    pub fn with_node_dup_tree_max_entries(mut self, n: u32) -> Self {
        self.set_node_dup_tree_max_entries(n);
        self
    }

    /// Builder-style (consuming) `tree_max_embedded_ln` setter. See [`Self::set_tree_max_embedded_ln`].
    pub fn with_tree_max_embedded_ln(mut self, bytes: u32) -> Self {
        self.set_tree_max_embedded_ln(bytes);
        self
    }

    /// Builder-style (consuming) `tree_max_delta` setter. See [`Self::set_tree_max_delta`].
    pub fn with_tree_max_delta(mut self, pct: u8) -> Self {
        self.set_tree_max_delta(pct);
        self
    }

    /// Builder-style (consuming) `tree_bin_delta` setter. See [`Self::set_tree_bin_delta`].
    pub fn with_tree_bin_delta(mut self, v: bool) -> Self {
        self.set_tree_bin_delta(v);
        self
    }

    /// Builder-style (consuming) `tree_bin_delta_percent` setter. See [`Self::set_tree_bin_delta_percent`].
    pub fn with_tree_bin_delta_percent(mut self, pct: u8) -> Self {
        self.set_tree_bin_delta_percent(pct);
        self
    }

    /// Builder-style (consuming) `tree_min_memory` setter. See [`Self::set_tree_min_memory`].
    pub fn with_tree_min_memory(mut self, bytes: u64) -> Self {
        self.set_tree_min_memory(bytes);
        self
    }

    /// Builder-style (consuming) `tree_compact_max_key_length` setter. See [`Self::set_tree_compact_max_key_length`].
    pub fn with_tree_compact_max_key_length(mut self, bytes: u32) -> Self {
        self.set_tree_compact_max_key_length(bytes);
        self
    }

    /// Builder-style (consuming) `in_compressor_wakeup_interval_ms` setter. See [`Self::set_in_compressor_wakeup_interval_ms`].
    pub fn with_in_compressor_wakeup_interval_ms(mut self, ms: u64) -> Self {
        self.set_in_compressor_wakeup_interval_ms(ms);
        self
    }

    /// Builder-style (consuming) `compressor_deadlock_retry` setter. See [`Self::set_compressor_deadlock_retry`].
    pub fn with_compressor_deadlock_retry(mut self, n: u32) -> Self {
        self.set_compressor_deadlock_retry(n);
        self
    }

    /// Builder-style (consuming) `compressor_lock_timeout_ms` setter. See [`Self::set_compressor_lock_timeout_ms`].
    pub fn with_compressor_lock_timeout_ms(mut self, ms: u64) -> Self {
        self.set_compressor_lock_timeout_ms(ms);
        self
    }

    /// Builder-style (consuming) `compressor_purge_root` setter. See [`Self::set_compressor_purge_root`].
    pub fn with_compressor_purge_root(mut self, v: bool) -> Self {
        self.set_compressor_purge_root(v);
        self
    }

    /// Builder-style (consuming) `cleaner_two_pass_gap` setter. See [`Self::set_cleaner_two_pass_gap`].
    pub fn with_cleaner_two_pass_gap(mut self, gap: i32) -> Self {
        self.set_cleaner_two_pass_gap(gap);
        self
    }

    /// Builder-style (consuming) `cleaner_two_pass_threshold` setter. See [`Self::set_cleaner_two_pass_threshold`].
    pub fn with_cleaner_two_pass_threshold(mut self, t: i32) -> Self {
        self.set_cleaner_two_pass_threshold(t);
        self
    }

    /// Builder-style (consuming) `cleaner_min_file_utilization` setter. See [`Self::set_cleaner_min_file_utilization`].
    pub fn with_cleaner_min_file_utilization(mut self, pct: u8) -> Self {
        self.set_cleaner_min_file_utilization(pct);
        self
    }

    /// Builder-style (consuming) `cleaner_threads` setter. See [`Self::set_cleaner_threads`].
    pub fn with_cleaner_threads(mut self, n: u32) -> Self {
        self.set_cleaner_threads(n);
        self
    }

    /// Builder-style (consuming) `cleaner_min_file_count` setter. See [`Self::set_cleaner_min_file_count`].
    pub fn with_cleaner_min_file_count(mut self, n: u32) -> Self {
        self.set_cleaner_min_file_count(n);
        self
    }

    /// Builder-style (consuming) `cleaner_min_age` setter. See [`Self::set_cleaner_min_age`].
    pub fn with_cleaner_min_age(mut self, checkpoints: u32) -> Self {
        self.set_cleaner_min_age(checkpoints);
        self
    }

    /// Builder-style (consuming) `cleaner_bytes_interval` setter. See [`Self::set_cleaner_bytes_interval`].
    pub fn with_cleaner_bytes_interval(mut self, bytes: u64) -> Self {
        self.set_cleaner_bytes_interval(bytes);
        self
    }

    /// Builder-style (consuming) `cleaner_wakeup_interval_ms` setter. See [`Self::set_cleaner_wakeup_interval_ms`].
    pub fn with_cleaner_wakeup_interval_ms(mut self, ms: u64) -> Self {
        self.set_cleaner_wakeup_interval_ms(ms);
        self
    }

    /// Builder-style (consuming) `cleaner_fetch_obsolete_size` setter. See [`Self::set_cleaner_fetch_obsolete_size`].
    pub fn with_cleaner_fetch_obsolete_size(mut self, v: bool) -> Self {
        self.set_cleaner_fetch_obsolete_size(v);
        self
    }

    /// Builder-style (consuming) `cleaner_adjust_utilization` setter. See [`Self::set_cleaner_adjust_utilization`].
    pub fn with_cleaner_adjust_utilization(mut self, v: bool) -> Self {
        self.set_cleaner_adjust_utilization(v);
        self
    }

    /// Builder-style (consuming) `cleaner_deadlock_retry` setter. See [`Self::set_cleaner_deadlock_retry`].
    pub fn with_cleaner_deadlock_retry(mut self, n: u32) -> Self {
        self.set_cleaner_deadlock_retry(n);
        self
    }

    /// Builder-style (consuming) `cleaner_lock_timeout_ms` setter. See [`Self::set_cleaner_lock_timeout_ms`].
    pub fn with_cleaner_lock_timeout_ms(mut self, ms: u64) -> Self {
        self.set_cleaner_lock_timeout_ms(ms);
        self
    }

    /// Builder-style (consuming) `cleaner_expunge` setter. See [`Self::set_cleaner_expunge`].
    pub fn with_cleaner_expunge(mut self, v: bool) -> Self {
        self.set_cleaner_expunge(v);
        self
    }

    /// Builder-style (consuming) `cleaner_use_deleted_dir` setter. See [`Self::set_cleaner_use_deleted_dir`].
    pub fn with_cleaner_use_deleted_dir(mut self, v: bool) -> Self {
        self.set_cleaner_use_deleted_dir(v);
        self
    }

    /// Builder-style (consuming) `cleaner_max_batch_files` setter. See [`Self::set_cleaner_max_batch_files`].
    pub fn with_cleaner_max_batch_files(mut self, n: u32) -> Self {
        self.set_cleaner_max_batch_files(n);
        self
    }

    /// Builder-style (consuming) `cleaner_read_size` setter. See [`Self::set_cleaner_read_size`].
    pub fn with_cleaner_read_size(mut self, bytes: usize) -> Self {
        self.set_cleaner_read_size(bytes);
        self
    }

    /// Builder-style (consuming) `cleaner_detail_max_memory_percentage` setter. See [`Self::set_cleaner_detail_max_memory_percentage`].
    pub fn with_cleaner_detail_max_memory_percentage(
        mut self,
        pct: u32,
    ) -> Self {
        self.set_cleaner_detail_max_memory_percentage(pct);
        self
    }

    /// Builder-style (consuming) `cleaner_look_ahead_cache_size` setter. See [`Self::set_cleaner_look_ahead_cache_size`].
    pub fn with_cleaner_look_ahead_cache_size(mut self, n: usize) -> Self {
        self.set_cleaner_look_ahead_cache_size(n);
        self
    }

    /// Builder-style (consuming) `cleaner_foreground_proactive_migration` setter. See [`Self::set_cleaner_foreground_proactive_migration`].
    pub fn with_cleaner_foreground_proactive_migration(
        mut self,
        v: bool,
    ) -> Self {
        self.set_cleaner_foreground_proactive_migration(v);
        self
    }

    /// Builder-style (consuming) `cleaner_background_proactive_migration` setter. See [`Self::set_cleaner_background_proactive_migration`].
    pub fn with_cleaner_background_proactive_migration(
        mut self,
        v: bool,
    ) -> Self {
        self.set_cleaner_background_proactive_migration(v);
        self
    }

    /// Builder-style (consuming) `cleaner_lazy_migration` setter. See [`Self::set_cleaner_lazy_migration`].
    pub fn with_cleaner_lazy_migration(mut self, v: bool) -> Self {
        self.set_cleaner_lazy_migration(v);
        self
    }

    /// Builder-style (consuming) `cleaner_expiration_enabled` setter. See [`Self::set_cleaner_expiration_enabled`].
    pub fn with_cleaner_expiration_enabled(mut self, v: bool) -> Self {
        self.set_cleaner_expiration_enabled(v);
        self
    }

    /// Builder-style (consuming) `checkpointer_wakeup_interval_ms` setter. See [`Self::set_checkpointer_wakeup_interval_ms`].
    pub fn with_checkpointer_wakeup_interval_ms(mut self, ms: u64) -> Self {
        self.set_checkpointer_wakeup_interval_ms(ms);
        self
    }

    /// Builder-style (consuming) `checkpointer_min_interval_secs` setter. See [`Self::set_checkpointer_min_interval_secs`].
    pub fn with_checkpointer_min_interval_secs(mut self, secs: u64) -> Self {
        self.set_checkpointer_min_interval_secs(secs);
        self
    }

    /// Builder-style (consuming) `checkpointer_deadlock_retry` setter. See [`Self::set_checkpointer_deadlock_retry`].
    pub fn with_checkpointer_deadlock_retry(mut self, n: u32) -> Self {
        self.set_checkpointer_deadlock_retry(n);
        self
    }

    /// Builder-style (consuming) `checkpointer_high_priority` setter. See [`Self::set_checkpointer_high_priority`].
    pub fn with_checkpointer_high_priority(mut self, v: bool) -> Self {
        self.set_checkpointer_high_priority(v);
        self
    }

    /// Builder-style (consuming) `evictor_evict_bytes` setter. See [`Self::set_evictor_evict_bytes`].
    pub fn with_evictor_evict_bytes(mut self, bytes: u64) -> Self {
        self.set_evictor_evict_bytes(bytes);
        self
    }

    /// Builder-style (consuming) `evictor_critical_percentage` setter. See [`Self::set_evictor_critical_percentage`].
    pub fn with_evictor_critical_percentage(mut self, pct: u32) -> Self {
        self.set_evictor_critical_percentage(pct);
        self
    }

    /// Builder-style (consuming) `evictor_use_dirty_lru` setter. See [`Self::set_evictor_use_dirty_lru`].
    pub fn with_evictor_use_dirty_lru(mut self, v: bool) -> Self {
        self.set_evictor_use_dirty_lru(v);
        self
    }

    /// Builder-style `evictor_mutate_bins` setter (`EVICTOR_MUTATE_BINS`,
    /// default true): when true the evictor may strip obsolete LNs out of a
    /// BIN during eviction. See [`Self::set_evictor_mutate_bins`].
    pub fn with_evictor_mutate_bins(mut self, v: bool) -> Self {
        self.set_evictor_mutate_bins(v);
        self
    }

    /// Builder-style (consuming) `evictor_lru_only` setter. See [`Self::set_evictor_lru_only`].
    pub fn with_evictor_lru_only(mut self, v: bool) -> Self {
        self.set_evictor_lru_only(v);
        self
    }

    /// Builder-style (consuming) `evictor_n_lru_lists` setter. See [`Self::set_evictor_n_lru_lists`].
    pub fn with_evictor_n_lru_lists(mut self, n: u32) -> Self {
        self.set_evictor_n_lru_lists(n);
        self
    }

    /// Builder-style (consuming) `evictor_deadlock_retry` setter. See [`Self::set_evictor_deadlock_retry`].
    pub fn with_evictor_deadlock_retry(mut self, n: u32) -> Self {
        self.set_evictor_deadlock_retry(n);
        self
    }

    /// Builder-style (consuming) `evictor_core_threads` setter. See [`Self::set_evictor_core_threads`].
    pub fn with_evictor_core_threads(mut self, n: usize) -> Self {
        self.set_evictor_core_threads(n);
        self
    }

    /// Builder-style (consuming) `evictor_max_threads` setter. See [`Self::set_evictor_max_threads`].
    pub fn with_evictor_max_threads(mut self, n: usize) -> Self {
        self.set_evictor_max_threads(n);
        self
    }

    /// Builder-style (consuming) `evictor_keep_alive_ms` setter. See [`Self::set_evictor_keep_alive_ms`].
    pub fn with_evictor_keep_alive_ms(mut self, ms: u64) -> Self {
        self.set_evictor_keep_alive_ms(ms);
        self
    }

    /// Builder-style (consuming) `evictor_allow_bin_deltas` setter. See [`Self::set_evictor_allow_bin_deltas`].
    pub fn with_evictor_allow_bin_deltas(mut self, v: bool) -> Self {
        self.set_evictor_allow_bin_deltas(v);
        self
    }

    /// Builder-style (consuming) `offheap_evict_bytes` setter. See [`Self::set_offheap_evict_bytes`].
    pub fn with_offheap_evict_bytes(mut self, bytes: u64) -> Self {
        self.set_offheap_evict_bytes(bytes);
        self
    }

    /// Builder-style (consuming) `offheap_n_lru_lists` setter. See [`Self::set_offheap_n_lru_lists`].
    pub fn with_offheap_n_lru_lists(mut self, n: u32) -> Self {
        self.set_offheap_n_lru_lists(n);
        self
    }

    /// Builder-style (consuming) `offheap_checksum` setter. See [`Self::set_offheap_checksum`].
    pub fn with_offheap_checksum(mut self, v: bool) -> Self {
        self.set_offheap_checksum(v);
        self
    }

    /// Builder-style (consuming) `offheap_core_threads` setter. See [`Self::set_offheap_core_threads`].
    pub fn with_offheap_core_threads(mut self, n: usize) -> Self {
        self.set_offheap_core_threads(n);
        self
    }

    /// Builder-style (consuming) `offheap_max_threads` setter. See [`Self::set_offheap_max_threads`].
    pub fn with_offheap_max_threads(mut self, n: usize) -> Self {
        self.set_offheap_max_threads(n);
        self
    }

    /// Builder-style (consuming) `offheap_keep_alive_ms` setter. See [`Self::set_offheap_keep_alive_ms`].
    pub fn with_offheap_keep_alive_ms(mut self, ms: u64) -> Self {
        self.set_offheap_keep_alive_ms(ms);
        self
    }

    /// Builder-style (consuming) `lock_timeout` setter. See [`Self::set_lock_timeout`].
    pub fn with_lock_timeout(mut self, ms: u64) -> Self {
        self.set_lock_timeout(ms);
        self
    }

    /// Builder-style (consuming) `lock_n_lock_tables` setter. See [`Self::set_lock_n_lock_tables`].
    pub fn with_lock_n_lock_tables(mut self, n: u32) -> Self {
        self.set_lock_n_lock_tables(n);
        self
    }

    /// Builder-style (consuming) `lock_deadlock_detect` setter. See [`Self::set_lock_deadlock_detect`].
    pub fn with_lock_deadlock_detect(mut self, v: bool) -> Self {
        self.set_lock_deadlock_detect(v);
        self
    }

    /// Builder-style (consuming) `lock_deadlock_detect_delay_ms` setter. See [`Self::set_lock_deadlock_detect_delay_ms`].
    pub fn with_lock_deadlock_detect_delay_ms(mut self, ms: u64) -> Self {
        self.set_lock_deadlock_detect_delay_ms(ms);
        self
    }

    /// Builder-style (consuming) `txn_timeout` setter. See [`Self::set_txn_timeout`].
    pub fn with_txn_timeout(mut self, ms: u64) -> Self {
        self.set_txn_timeout(ms);
        self
    }

    /// Builder-style (consuming) `txn_serializable_isolation` setter. See [`Self::set_txn_serializable_isolation`].
    pub fn with_txn_serializable_isolation(mut self, v: bool) -> Self {
        self.set_txn_serializable_isolation(v);
        self
    }

    /// Builder-style (consuming) `txn_deadlock_stack_trace` setter. See [`Self::set_txn_deadlock_stack_trace`].
    pub fn with_txn_deadlock_stack_trace(mut self, v: bool) -> Self {
        self.set_txn_deadlock_stack_trace(v);
        self
    }

    /// Builder-style (consuming) `txn_dump_locks` setter. See [`Self::set_txn_dump_locks`].
    pub fn with_txn_dump_locks(mut self, v: bool) -> Self {
        self.set_txn_dump_locks(v);
        self
    }

    /// Builder-style (consuming) `verify_schedule` setter. See [`Self::set_verify_schedule`].
    pub fn with_verify_schedule(mut self, s: String) -> Self {
        self.set_verify_schedule(s);
        self
    }

    /// Builder-style (consuming) `verify_log` setter. See [`Self::set_verify_log`].
    pub fn with_verify_log(mut self, v: bool) -> Self {
        self.set_verify_log(v);
        self
    }

    /// Builder-style (consuming) `verify_log_read_delay_ms` setter. See [`Self::set_verify_log_read_delay_ms`].
    pub fn with_verify_log_read_delay_ms(mut self, ms: u64) -> Self {
        self.set_verify_log_read_delay_ms(ms);
        self
    }

    /// Builder-style (consuming) `verify_btree` setter. See [`Self::set_verify_btree`].
    pub fn with_verify_btree(mut self, v: bool) -> Self {
        self.set_verify_btree(v);
        self
    }

    /// Builder-style (consuming) `verify_secondaries` setter. See [`Self::set_verify_secondaries`].
    pub fn with_verify_secondaries(mut self, v: bool) -> Self {
        self.set_verify_secondaries(v);
        self
    }

    /// Builder-style (consuming) `verify_data_records` setter. See [`Self::set_verify_data_records`].
    pub fn with_verify_data_records(mut self, v: bool) -> Self {
        self.set_verify_data_records(v);
        self
    }

    /// Builder-style (consuming) `verify_obsolete_records` setter. See [`Self::set_verify_obsolete_records`].
    pub fn with_verify_obsolete_records(mut self, v: bool) -> Self {
        self.set_verify_obsolete_records(v);
        self
    }

    /// Builder-style (consuming) `verify_btree_batch_size` setter. See [`Self::set_verify_btree_batch_size`].
    pub fn with_verify_btree_batch_size(mut self, n: u32) -> Self {
        self.set_verify_btree_batch_size(n);
        self
    }

    /// Builder-style (consuming) `verify_btree_batch_delay_ms` setter. See [`Self::set_verify_btree_batch_delay_ms`].
    pub fn with_verify_btree_batch_delay_ms(mut self, ms: u64) -> Self {
        self.set_verify_btree_batch_delay_ms(ms);
        self
    }

    /// Builder-style (consuming) `dos_producer_queue_timeout_ms` setter. See [`Self::set_dos_producer_queue_timeout_ms`].
    pub fn with_dos_producer_queue_timeout_ms(mut self, ms: u64) -> Self {
        self.set_dos_producer_queue_timeout_ms(ms);
        self
    }

    /// Builder-style (consuming) `stats_collect` setter. See [`Self::set_stats_collect`].
    pub fn with_stats_collect(mut self, v: bool) -> Self {
        self.set_stats_collect(v);
        self
    }

    /// Builder-style (consuming) `stats_collect_interval_secs` setter. See [`Self::set_stats_collect_interval_secs`].
    pub fn with_stats_collect_interval_secs(mut self, secs: u64) -> Self {
        self.set_stats_collect_interval_secs(secs);
        self
    }

    /// Builder-style (consuming) `stats_max_files` setter. See [`Self::set_stats_max_files`].
    pub fn with_stats_max_files(mut self, n: u32) -> Self {
        self.set_stats_max_files(n);
        self
    }

    /// Builder-style (consuming) `stats_file_row_count` setter. See [`Self::set_stats_file_row_count`].
    pub fn with_stats_file_row_count(mut self, n: u32) -> Self {
        self.set_stats_file_row_count(n);
        self
    }

    /// Builder-style (consuming) `stats_file_directory` setter. See [`Self::set_stats_file_directory`].
    pub fn with_stats_file_directory(mut self, dir: PathBuf) -> Self {
        self.set_stats_file_directory(dir);
        self
    }

    /// Builder-style (consuming) `startup_dump_threshold_ms` setter. See [`Self::set_startup_dump_threshold_ms`].
    pub fn with_startup_dump_threshold_ms(mut self, ms: u64) -> Self {
        self.set_startup_dump_threshold_ms(ms);
        self
    }

    /// Builder-style (consuming) `exception_listener` setter. See [`Self::set_exception_listener`].
    pub fn with_exception_listener(
        mut self,
        listener: Arc<dyn ExceptionListener>,
    ) -> Self {
        self.set_exception_listener(listener);
        self
    }
}

impl Default for EnvironmentConfig {
    fn default() -> Self {
        Self::new(PathBuf::from("."))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_defaults_core() {
        let c = EnvironmentConfig::default();
        assert_eq!(c.home, PathBuf::from("."));
        assert!(!c.allow_create);
        assert!(!c.transactional);
        assert!(!c.read_only);
        assert!(c.env_is_locking);
        assert!(!c.shared_cache);
        assert!(!c.env_recovery_force_checkpoint);
        assert!(!c.env_recovery_force_new_file);
        assert!(!c.halt_on_commit_after_checksum_exception);
    }

    #[test]
    fn test_with_builders_are_uniform_and_chainable() {
        // review P1-8: every parameter now has a consuming `with_*` builder,
        // including ones that previously only had `set_*`. The whole config
        // can be built in one chained expression.
        let c = EnvironmentConfig::new(PathBuf::from("/tmp/x"))
            .with_allow_create(true)
            .with_transactional(true)
            .with_shared_cache(true) // previously set_*-only
            .with_run_cleaner(false) // previously set_*-only
            .with_log_num_buffers(7) // previously set_*-only
            .with_cache_percent(42); // previously set_*-only
        assert!(c.allow_create);
        assert!(c.transactional);
        assert!(c.shared_cache);
        assert!(!c.run_cleaner);
        assert_eq!(c.log_num_buffers, 7);
        assert_eq!(c.cache_percent, 42);
    }

    #[test]
    fn test_defaults_memory() {
        let c = EnvironmentConfig::default();
        assert_eq!(c.cache_size, 64 * 1024 * 1024);
        assert_eq!(c.cache_percent, 0);
        assert_eq!(c.max_off_heap_memory, 0);
        assert_eq!(c.max_disk, 0);
        assert_eq!(c.free_disk, 5 * 1024 * 1024 * 1024);
    }

    #[test]
    fn test_defaults_daemons() {
        let c = EnvironmentConfig::default();
        assert!(c.run_in_compressor);
        assert!(c.run_checkpointer);
        assert!(c.run_cleaner);
        assert!(c.run_evictor);
        assert!(!c.run_offheap_evictor);
        assert!(!c.run_verifier);
    }

    #[test]
    fn test_defaults_log() {
        let c = EnvironmentConfig::default();
        assert_eq!(c.log_file_max_bytes, 10 * 1024 * 1024);
        assert_eq!(c.log_file_cache_size, 100);
        assert!(c.log_checksum_read);
        assert!(!c.log_verify_checksums);
        assert_eq!(c.log_fsync_timeout_ms, 500_000);
        assert_eq!(c.log_fsync_time_limit_ms, 0);
        assert_eq!(c.log_num_buffers, 3);
        assert_eq!(c.log_total_buffer_bytes, 7 * 1024 * 1024);
        assert_eq!(c.log_buffer_size, 0);
        assert_eq!(c.log_fault_read_size, 2048);
        assert_eq!(c.log_iterator_read_size, 8192);
        assert_eq!(c.log_iterator_max_size, 16 * 1024 * 1024);
        assert_eq!(c.log_n_data_directories, 0);
        assert!(!c.log_mem_only);
        assert!(!c.log_detect_file_delete);
        assert_eq!(c.log_detect_file_delete_interval_ms, 3_000);
        assert_eq!(c.log_flush_sync_interval_ms, 0);
        assert_eq!(c.log_flush_no_sync_interval_ms, 0);
        assert!(!c.log_use_odsync);
        assert!(!c.log_use_write_queue);
        assert_eq!(c.log_write_queue_size, 1024 * 1024);
        assert_eq!(c.log_group_commit_threshold, 0);
        assert_eq!(c.log_group_commit_interval_ms, 0);
    }

    #[test]
    fn test_defaults_btree() {
        let c = EnvironmentConfig::default();
        assert_eq!(c.node_max_entries, 128);
        assert_eq!(c.node_dup_tree_max_entries, 128);
        assert_eq!(c.tree_max_embedded_ln, 16);
        assert_eq!(c.tree_max_delta, 25);
        assert!(c.tree_bin_delta);
        assert_eq!(c.tree_min_memory, 0);
        assert_eq!(c.tree_compact_max_key_length, 16);
    }

    #[test]
    fn test_defaults_cleaner() {
        let c = EnvironmentConfig::default();
        assert_eq!(c.cleaner_min_utilization, 50);
        assert_eq!(c.cleaner_min_file_utilization, 5);
        assert_eq!(c.cleaner_threads, 1);
        assert_eq!(c.cleaner_min_file_count, 2);
        assert_eq!(c.cleaner_min_age, 2);
        assert_eq!(c.cleaner_bytes_interval, 0);
        assert_eq!(c.cleaner_wakeup_interval_ms, 0);
        assert!(!c.cleaner_fetch_obsolete_size);
        assert!(!c.cleaner_adjust_utilization);
        assert_eq!(c.cleaner_deadlock_retry, 3);
        assert_eq!(c.cleaner_lock_timeout_ms, 500);
        assert!(c.cleaner_expunge);
        assert!(!c.cleaner_use_deleted_dir);
        assert_eq!(c.cleaner_max_batch_files, 0);
        assert_eq!(c.cleaner_read_size, 8192);
        assert_eq!(c.cleaner_detail_max_memory_percentage, 2);
        assert_eq!(c.cleaner_look_ahead_cache_size, 32);
        assert!(!c.cleaner_foreground_proactive_migration);
        assert!(!c.cleaner_background_proactive_migration);
        assert!(!c.cleaner_lazy_migration);
        assert!(!c.cleaner_expiration_enabled);
    }

    #[test]
    fn test_defaults_checkpointer() {
        let c = EnvironmentConfig::default();
        assert_eq!(c.checkpointer_bytes_interval, 20_000_000);
        assert_eq!(c.checkpointer_wakeup_interval_ms, 30_000);
        assert_eq!(c.checkpointer_min_interval_secs, 0);
        assert_eq!(c.checkpointer_deadlock_retry, 3);
        assert!(!c.checkpointer_high_priority);
    }

    #[test]
    fn test_defaults_evictor() {
        let c = EnvironmentConfig::default();
        assert_eq!(c.evictor_nodes_per_scan, 10);
        assert_eq!(c.evictor_evict_bytes, 512 * 1024);
        assert_eq!(c.evictor_critical_percentage, 5);
        assert!(!c.evictor_lru_only);
        assert_eq!(c.evictor_n_lru_lists, 4);
        assert_eq!(c.evictor_deadlock_retry, 3);
        assert_eq!(c.evictor_core_threads, 1);
        assert_eq!(c.evictor_max_threads, 10);
        assert_eq!(c.evictor_keep_alive_ms, 60_000);
        assert!(c.evictor_allow_bin_deltas);
    }

    #[test]
    fn test_defaults_offheap() {
        let c = EnvironmentConfig::default();
        assert_eq!(c.offheap_evict_bytes, 512 * 1024);
        assert_eq!(c.offheap_n_lru_lists, 4);
        assert!(!c.offheap_checksum);
        assert_eq!(c.offheap_core_threads, 1);
        assert_eq!(c.offheap_max_threads, 10);
        assert_eq!(c.offheap_keep_alive_ms, 60_000);
    }

    #[test]
    fn test_defaults_locking() {
        let c = EnvironmentConfig::default();
        assert_eq!(c.lock_timeout_ms, 500);
        assert_eq!(c.lock_n_lock_tables, 64);
        assert!(c.lock_deadlock_detect);
        assert_eq!(c.lock_deadlock_detect_delay_ms, 0);
    }

    #[test]
    fn test_defaults_txn() {
        let c = EnvironmentConfig::default();
        assert_eq!(c.txn_timeout_ms, 0);
        assert!(!c.txn_no_sync);
        assert!(!c.txn_write_no_sync);
        assert!(!c.txn_serializable_isolation);
        assert!(!c.txn_deadlock_stack_trace);
        assert!(!c.txn_dump_locks);
    }

    #[test]
    fn test_defaults_verifier() {
        let c = EnvironmentConfig::default();
        assert_eq!(c.verify_schedule, "");
        assert!(!c.verify_log);
        assert_eq!(c.verify_log_read_delay_ms, 0);
        assert!(!c.verify_btree);
        assert!(c.verify_secondaries);
        assert!(!c.verify_data_records);
        assert!(!c.verify_obsolete_records);
        assert_eq!(c.verify_btree_batch_size, 1_000);
        assert_eq!(c.verify_btree_batch_delay_ms, 10);
    }

    #[test]
    fn test_defaults_stats() {
        let c = EnvironmentConfig::default();
        assert!(!c.stats_collect);
        assert_eq!(c.stats_collect_interval_secs, 300);
        assert_eq!(c.stats_max_files, 100);
        assert_eq!(c.stats_file_row_count, 1_000);
        assert!(c.stats_file_directory.is_none());
    }

    #[test]
    fn test_set_allow_create() {
        let mut c = EnvironmentConfig::default();
        c.set_allow_create(true);
        assert!(c.allow_create);
    }

    #[test]
    fn test_set_cache_size() {
        let mut c = EnvironmentConfig::default();
        c.set_cache_size(128 * 1024 * 1024);
        assert_eq!(c.cache_size, 128 * 1024 * 1024);
    }

    #[test]
    fn test_set_free_disk() {
        let mut c = EnvironmentConfig::default();
        c.set_free_disk(1024 * 1024 * 1024);
        assert_eq!(c.free_disk, 1024 * 1024 * 1024);
    }

    #[test]
    fn test_set_log_params() {
        let mut c = EnvironmentConfig::default();
        c.set_log_file_max_bytes(20 * 1024 * 1024);
        c.set_log_num_buffers(5);
        c.set_log_total_buffer_bytes(5 * 1024 * 1024);
        c.set_log_iterator_read_size(16384);
        c.set_log_iterator_max_size(32 * 1024 * 1024);
        c.set_log_n_data_directories(2);
        c.set_log_mem_only(true);
        c.set_log_use_odsync(true);
        c.set_log_detect_file_delete(true);
        c.set_log_detect_file_delete_interval_ms(5000);
        c.set_log_flush_sync_interval_ms(1000);
        c.set_log_flush_no_sync_interval_ms(500);
        c.set_log_fsync_time_limit_ms(200);
        c.set_log_use_write_queue(true);
        c.set_log_write_queue_size(2 * 1024 * 1024);
        c.set_log_verify_checksums(true);
        assert_eq!(c.log_file_max_bytes, 20 * 1024 * 1024);
        assert_eq!(c.log_num_buffers, 5);
        assert_eq!(c.log_total_buffer_bytes, 5 * 1024 * 1024);
        assert_eq!(c.log_iterator_read_size, 16384);
        assert_eq!(c.log_iterator_max_size, 32 * 1024 * 1024);
        assert_eq!(c.log_n_data_directories, 2);
        assert!(c.log_mem_only);
        assert!(c.log_use_odsync);
        assert!(c.log_detect_file_delete);
        assert_eq!(c.log_detect_file_delete_interval_ms, 5000);
        assert_eq!(c.log_flush_sync_interval_ms, 1000);
        assert_eq!(c.log_flush_no_sync_interval_ms, 500);
        assert_eq!(c.log_fsync_time_limit_ms, 200);
        assert!(c.log_use_write_queue);
        assert_eq!(c.log_write_queue_size, 2 * 1024 * 1024);
        assert!(c.log_verify_checksums);
    }

    #[test]
    fn test_set_btree_params() {
        let mut c = EnvironmentConfig::default();
        c.set_node_max_entries(256);
        c.set_node_dup_tree_max_entries(64);
        c.set_tree_max_embedded_ln(32);
        c.set_tree_max_delta(30);
        c.set_tree_bin_delta(false);
        c.set_tree_min_memory(1024);
        c.set_tree_compact_max_key_length(32);
        assert_eq!(c.node_max_entries, 256);
        assert_eq!(c.node_dup_tree_max_entries, 64);
        assert_eq!(c.tree_max_embedded_ln, 32);
        assert_eq!(c.tree_max_delta, 30);
        assert!(!c.tree_bin_delta);
        assert_eq!(c.tree_min_memory, 1024);
        assert_eq!(c.tree_compact_max_key_length, 32);
    }

    #[test]
    fn test_set_cleaner_params() {
        let mut c = EnvironmentConfig::default();
        c.set_cleaner_threads(4);
        c.set_cleaner_min_file_count(5);
        c.set_cleaner_min_age(3);
        c.set_cleaner_expiration_enabled(true);
        c.set_cleaner_bytes_interval(5_000_000);
        c.set_cleaner_wakeup_interval_ms(10_000);
        c.set_cleaner_fetch_obsolete_size(true);
        c.set_cleaner_adjust_utilization(true);
        c.set_cleaner_deadlock_retry(5);
        c.set_cleaner_lock_timeout_ms(1000);
        c.set_cleaner_expunge(false);
        c.set_cleaner_use_deleted_dir(true);
        c.set_cleaner_max_batch_files(10);
        c.set_cleaner_detail_max_memory_percentage(5);
        c.set_cleaner_foreground_proactive_migration(true);
        c.set_cleaner_background_proactive_migration(true);
        c.set_cleaner_lazy_migration(true);
        assert_eq!(c.cleaner_threads, 4);
        assert_eq!(c.cleaner_min_file_count, 5);
        assert_eq!(c.cleaner_min_age, 3);
        assert!(c.cleaner_expiration_enabled);
        assert_eq!(c.cleaner_bytes_interval, 5_000_000);
        assert_eq!(c.cleaner_wakeup_interval_ms, 10_000);
        assert!(c.cleaner_fetch_obsolete_size);
        assert!(c.cleaner_adjust_utilization);
        assert_eq!(c.cleaner_deadlock_retry, 5);
        assert_eq!(c.cleaner_lock_timeout_ms, 1000);
        assert!(!c.cleaner_expunge);
        assert!(c.cleaner_use_deleted_dir);
        assert_eq!(c.cleaner_max_batch_files, 10);
        assert_eq!(c.cleaner_detail_max_memory_percentage, 5);
        assert!(c.cleaner_foreground_proactive_migration);
        assert!(c.cleaner_background_proactive_migration);
        assert!(c.cleaner_lazy_migration);
    }

    #[test]
    fn test_set_checkpointer_params() {
        let mut c = EnvironmentConfig::default();
        c.set_checkpointer_wakeup_interval_ms(60_000);
        c.set_checkpointer_deadlock_retry(5);
        c.set_checkpointer_high_priority(true);
        assert_eq!(c.checkpointer_wakeup_interval_ms, 60_000);
        assert_eq!(c.checkpointer_deadlock_retry, 5);
        assert!(c.checkpointer_high_priority);
    }

    #[test]
    fn test_set_evictor_params() {
        let mut c = EnvironmentConfig::default();
        c.set_evictor_evict_bytes(1024 * 1024);
        c.set_evictor_critical_percentage(10);
        c.set_evictor_n_lru_lists(8);
        c.set_evictor_deadlock_retry(5);
        c.set_evictor_keep_alive_ms(30_000);
        c.set_evictor_allow_bin_deltas(false);
        assert_eq!(c.evictor_evict_bytes, 1024 * 1024);
        assert_eq!(c.evictor_critical_percentage, 10);
        assert_eq!(c.evictor_n_lru_lists, 8);
        assert_eq!(c.evictor_deadlock_retry, 5);
        assert_eq!(c.evictor_keep_alive_ms, 30_000);
        assert!(!c.evictor_allow_bin_deltas);
    }

    #[test]
    fn test_set_offheap_params() {
        let mut c = EnvironmentConfig::default();
        c.set_offheap_evict_bytes(1024 * 1024);
        c.set_offheap_n_lru_lists(8);
        c.set_offheap_checksum(true);
        c.set_offheap_core_threads(2);
        c.set_offheap_max_threads(4);
        c.set_offheap_keep_alive_ms(30_000);
        assert_eq!(c.offheap_evict_bytes, 1024 * 1024);
        assert_eq!(c.offheap_n_lru_lists, 8);
        assert!(c.offheap_checksum);
        assert_eq!(c.offheap_core_threads, 2);
        assert_eq!(c.offheap_max_threads, 4);
        assert_eq!(c.offheap_keep_alive_ms, 30_000);
    }

    #[test]
    fn test_set_locking_params() {
        let mut c = EnvironmentConfig::default();
        c.set_lock_timeout(1000);
        c.set_lock_n_lock_tables(32);
        c.set_lock_deadlock_detect(false);
        c.set_lock_deadlock_detect_delay_ms(100);
        assert_eq!(c.lock_timeout_ms, 1000);
        assert_eq!(c.lock_n_lock_tables, 32);
        assert!(!c.lock_deadlock_detect);
        assert_eq!(c.lock_deadlock_detect_delay_ms, 100);
    }

    #[test]
    #[allow(deprecated)] // exercises the deprecated txn_no_sync / txn_write_no_sync setters
    fn test_set_txn_params() {
        let mut c = EnvironmentConfig::default();
        c.set_txn_timeout(5000);
        c.set_txn_no_sync(true);
        c.set_txn_write_no_sync(true);
        c.set_txn_serializable_isolation(true);
        c.set_txn_deadlock_stack_trace(true);
        c.set_txn_dump_locks(true);
        assert_eq!(c.txn_timeout_ms, 5000);
        assert!(c.txn_no_sync);
        assert!(c.txn_write_no_sync);
        assert!(c.txn_serializable_isolation);
        assert!(c.txn_deadlock_stack_trace);
        assert!(c.txn_dump_locks);
    }

    #[test]
    fn test_set_verifier_params() {
        let mut c = EnvironmentConfig::default();
        c.set_run_verifier(true);
        c.set_verify_schedule("0 2 * * *".to_string());
        c.set_verify_log(true);
        c.set_verify_log_read_delay_ms(50);
        c.set_verify_btree(true);
        c.set_verify_secondaries(false);
        c.set_verify_data_records(true);
        c.set_verify_obsolete_records(true);
        c.set_verify_btree_batch_size(500);
        c.set_verify_btree_batch_delay_ms(20);
        assert!(c.run_verifier);
        assert_eq!(c.verify_schedule, "0 2 * * *");
        assert!(c.verify_log);
        assert_eq!(c.verify_log_read_delay_ms, 50);
        assert!(c.verify_btree);
        assert!(!c.verify_secondaries);
        assert!(c.verify_data_records);
        assert!(c.verify_obsolete_records);
        assert_eq!(c.verify_btree_batch_size, 500);
        assert_eq!(c.verify_btree_batch_delay_ms, 20);
    }

    #[test]
    fn test_set_stats_params() {
        let mut c = EnvironmentConfig::default();
        c.set_stats_collect(true);
        c.set_stats_collect_interval_secs(60);
        c.set_stats_max_files(50);
        c.set_stats_file_row_count(2000);
        c.set_stats_file_directory(PathBuf::from("/var/log/noxu"));
        assert!(c.stats_collect);
        assert_eq!(c.stats_collect_interval_secs, 60);
        assert_eq!(c.stats_max_files, 50);
        assert_eq!(c.stats_file_row_count, 2000);
        assert_eq!(
            c.stats_file_directory,
            Some(PathBuf::from("/var/log/noxu"))
        );
    }

    #[test]
    #[allow(deprecated)] // exercises deprecated with_txn_no_sync in builder chain
    fn test_builder_chain() {
        let c = EnvironmentConfig::new(PathBuf::from("/data"))
            .with_allow_create(true)
            .with_transactional(true)
            .with_cache_size(512 * 1024 * 1024)
            .with_log_file_max_bytes(5 * 1024 * 1024)
            .with_cleaner_min_utilization(40)
            .with_checkpointer_bytes_interval(10_000_000)
            .with_evictor_nodes_per_scan(20)
            .with_log_group_commit(5, 10)
            .with_txn_no_sync(false)
            .with_durability(Durability::COMMIT_SYNC);
        assert_eq!(c.home, PathBuf::from("/data"));
        assert!(c.allow_create);
        assert!(c.transactional);
        assert_eq!(c.cache_size, 512 * 1024 * 1024);
        assert_eq!(c.log_file_max_bytes, 5 * 1024 * 1024);
        assert_eq!(c.cleaner_min_utilization, 40);
        assert_eq!(c.checkpointer_bytes_interval, 10_000_000);
        assert_eq!(c.evictor_nodes_per_scan, 20);
        assert_eq!(c.log_group_commit_threshold, 5);
        assert_eq!(c.log_group_commit_interval_ms, 10);
        assert!(!c.txn_no_sync);
        assert_eq!(c.durability, Durability::COMMIT_SYNC);
    }

    #[test]
    fn test_env_behaviour_params() {
        let mut c = EnvironmentConfig::default();
        c.set_env_check_leaks(false);
        c.set_env_forced_yield(true);
        c.set_env_fair_latches(true);
        c.set_env_latch_timeout_ms(60_000);
        c.set_env_ttl_clock_tolerance_ms(100);
        c.set_env_expiration_enabled(true);
        c.set_env_db_eviction(true);
        c.set_env_background_read_limit_kb(10_000);
        c.set_env_background_write_limit_kb(5_000);
        c.set_env_background_sleep_interval_us(100);
        assert!(!c.env_check_leaks);
        assert!(c.env_forced_yield);
        assert!(c.env_fair_latches);
        assert_eq!(c.env_latch_timeout_ms, 60_000);
        assert_eq!(c.env_ttl_clock_tolerance_ms, 100);
        assert!(c.env_expiration_enabled);
        assert!(c.env_db_eviction);
        assert_eq!(c.env_background_read_limit_kb, 10_000);
        assert_eq!(c.env_background_write_limit_kb, 5_000);
        assert_eq!(c.env_background_sleep_interval_us, 100);
    }

    #[test]
    fn test_compressor_params() {
        let mut c = EnvironmentConfig::default();
        c.set_in_compressor_wakeup_interval_ms(1000);
        c.set_compressor_deadlock_retry(5);
        c.set_compressor_lock_timeout_ms(1000);
        c.set_compressor_purge_root(true);
        assert_eq!(c.in_compressor_wakeup_interval_ms, 1000);
        assert_eq!(c.compressor_deadlock_retry, 5);
        assert_eq!(c.compressor_lock_timeout_ms, 1000);
        assert!(c.compressor_purge_root);
    }

    #[test]
    fn test_dos_cursor_params() {
        let mut c = EnvironmentConfig::default();
        c.set_dos_producer_queue_timeout_ms(5000);
        assert_eq!(c.dos_producer_queue_timeout_ms, 5000);
    }

    #[test]
    fn test_clone() {
        let c1 = EnvironmentConfig::default()
            .with_allow_create(true)
            .with_cache_size(256 * 1024 * 1024);
        let c2 = c1.clone();
        assert_eq!(c1.allow_create, c2.allow_create);
        assert_eq!(c1.cache_size, c2.cache_size);
        assert_eq!(c1.free_disk, c2.free_disk);
    }
}
