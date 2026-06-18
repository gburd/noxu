//! Construction-time configuration for `EnvironmentImpl`.
//!
//! This struct is populated from `noxu_db::EnvironmentConfig` in the
//! `noxu-db` crate (which depends on `noxu-dbi`).  Having a separate
//! struct here avoids a circular dependency between the two crates.

/// All construction-time parameters for `EnvironmentImpl`.
///
/// Mirrors the subset of `EnvironmentConfig` that must be known at the
/// time the environment is constructed (most values are passed directly
/// to sub-system constructors and cannot be changed afterwards without
/// rebuilding the environment).
#[derive(Debug, Clone)]
pub struct DbiEnvConfig {
    // -----------------------------------------------------------------------
    // Core
    // -----------------------------------------------------------------------
    pub read_only: bool,
    pub transactional: bool,
    pub env_is_locking: bool,
    pub env_recovery_force_checkpoint: bool,
    pub env_recovery_force_new_file: bool,
    pub halt_on_commit_after_checksum_exception: bool,
    /// Reserved / not yet implemented as of v3.1.
    /// Stored for future use; not read by any subsystem.
    pub env_check_leaks: bool,
    /// Reserved / not yet implemented as of v3.1.
    /// Stored for future use; not read by any subsystem.
    pub env_forced_yield: bool,
    /// Reserved / not yet implemented as of v3.1.
    /// Stored for future use; not read by any subsystem.
    pub env_fair_latches: bool,
    /// Latch acquisition timeout in milliseconds.  0 = block forever.
    /// Reserved / not yet implemented as of v3.1.
    /// Stored for future use; not read by the latch layer.
    pub env_latch_timeout_ms: u64,
    /// Reserved / not yet implemented as of v3.1.
    /// Stored for future use; not read by any subsystem.
    pub env_ttl_clock_tolerance_ms: u64,
    /// Reserved / not yet implemented as of v3.1.
    /// Stored for future use; not read by any subsystem.
    pub env_expiration_enabled: bool,
    /// Reserved / not yet implemented as of v3.1.
    /// Stored for future use; not read by the evictor.
    pub env_db_eviction: bool,

    // -----------------------------------------------------------------------
    // Memory
    // -----------------------------------------------------------------------
    /// Maximum B-tree cache size in bytes.
    pub cache_size: u64,
    pub cache_percent: u32,
    pub max_off_heap_memory: u64,
    pub max_disk: u64,
    pub free_disk: u64,

    // -----------------------------------------------------------------------
    // Log
    // -----------------------------------------------------------------------
    pub log_file_max_bytes: u64,
    pub log_file_cache_size: usize,
    pub log_checksum_read: bool,
    pub log_verify_checksums: bool,
    pub log_fsync_timeout_ms: u64,
    pub log_fsync_time_limit_ms: u64,
    pub log_num_buffers: usize,
    /// Per-buffer size in bytes (= `log_total_buffer_bytes / log_num_buffers`
    /// unless `log_buffer_size` is set explicitly).
    pub log_buffer_size: usize,
    pub log_fault_read_size: usize,
    pub log_iterator_read_size: usize,
    pub log_iterator_max_size: usize,
    pub log_n_data_directories: u32,
    pub log_mem_only: bool,
    pub log_detect_file_delete: bool,
    pub log_detect_file_delete_interval_ms: u64,
    pub log_flush_sync_interval_ms: u64,
    pub log_flush_no_sync_interval_ms: u64,
    pub log_use_odsync: bool,
    pub log_use_write_queue: bool,
    pub log_write_queue_size: usize,
    pub log_group_commit_threshold: usize,
    pub log_group_commit_interval_ms: u64,

    // -----------------------------------------------------------------------
    // B-tree
    // -----------------------------------------------------------------------
    pub node_max_entries: u32,
    pub node_dup_tree_max_entries: u32,
    pub tree_max_embedded_ln: u32,
    pub tree_max_delta: u8,
    pub tree_bin_delta: bool,
    pub tree_min_memory: u64,
    pub tree_compact_max_key_length: u32,

    // -----------------------------------------------------------------------
    // INCompressor
    // -----------------------------------------------------------------------
    pub run_in_compressor: bool,
    pub in_compressor_wakeup_interval_ms: u64,
    pub compressor_deadlock_retry: u32,
    pub compressor_lock_timeout_ms: u64,
    pub compressor_purge_root: bool,

    // -----------------------------------------------------------------------
    // Cleaner
    // -----------------------------------------------------------------------
    pub run_cleaner: bool,
    pub cleaner_min_utilization: u8,
    pub cleaner_min_file_utilization: u8,
    pub cleaner_threads: u32,
    pub cleaner_min_file_count: u32,
    pub cleaner_min_age: u32,
    pub cleaner_bytes_interval: u64,
    pub cleaner_wakeup_interval_ms: u64,
    pub cleaner_fetch_obsolete_size: bool,
    pub cleaner_adjust_utilization: bool,
    pub cleaner_deadlock_retry: u32,
    pub cleaner_lock_timeout_ms: u64,
    pub cleaner_expunge: bool,
    pub cleaner_use_deleted_dir: bool,
    pub cleaner_max_batch_files: u32,
    pub cleaner_read_size: usize,
    pub cleaner_detail_max_memory_percentage: u32,
    pub cleaner_look_ahead_cache_size: usize,
    pub cleaner_foreground_proactive_migration: bool,
    pub cleaner_background_proactive_migration: bool,
    pub cleaner_lazy_migration: bool,
    pub cleaner_expiration_enabled: bool,

    // -----------------------------------------------------------------------
    // Checkpointer
    // -----------------------------------------------------------------------
    pub run_checkpointer: bool,
    pub checkpointer_bytes_interval: u64,
    pub checkpointer_wakeup_interval_ms: u64,
    pub checkpointer_deadlock_retry: u32,
    pub checkpointer_high_priority: bool,

    // -----------------------------------------------------------------------
    // Evictor
    // -----------------------------------------------------------------------
    pub run_evictor: bool,
    pub evictor_nodes_per_scan: usize,
    pub evictor_evict_bytes: u64,
    pub evictor_critical_percentage: u32,
    pub evictor_lru_only: bool,
    /// JE EVICTOR_USE_DIRTY_LRU (default true). Forced false when off-heap is
    /// enabled.
    pub evictor_use_dirty_lru: bool,
    pub evictor_n_lru_lists: u32,
    pub evictor_deadlock_retry: u32,
    pub evictor_core_threads: usize,
    pub evictor_max_threads: usize,
    pub evictor_keep_alive_ms: u64,
    pub evictor_allow_bin_deltas: bool,

    // -----------------------------------------------------------------------
    // Off-heap evictor
    // -----------------------------------------------------------------------
    pub run_offheap_evictor: bool,
    pub offheap_evict_bytes: u64,
    pub offheap_n_lru_lists: u32,
    pub offheap_checksum: bool,
    pub offheap_core_threads: usize,
    pub offheap_max_threads: usize,
    pub offheap_keep_alive_ms: u64,

    // -----------------------------------------------------------------------
    // Locking
    // -----------------------------------------------------------------------
    pub lock_timeout_ms: u64,
    pub lock_deadlock_detect: bool,
    pub lock_deadlock_detect_delay_ms: u64,
    /// Number of lock-table shards (JE `LOCK_N_LOCK_TABLES`). Noxu defaults to
    /// 64 (a documented deviation from JE's default of 1) for write
    /// concurrency. Clamped to >= 1 by the LockManager.
    pub n_lock_tables: usize,

    // -----------------------------------------------------------------------
    // Transactions
    // -----------------------------------------------------------------------
    pub txn_timeout_ms: u64,
    pub txn_serializable_isolation: bool,
    pub txn_deadlock_stack_trace: bool,
    pub txn_dump_locks: bool,

    // -----------------------------------------------------------------------
    // Recovery
    // -----------------------------------------------------------------------
    pub env_recovery_force_checkpoint_field: bool,

    // -----------------------------------------------------------------------
    // Verifier
    // -----------------------------------------------------------------------
    pub run_verifier: bool,
    pub verify_log: bool,
    pub verify_log_read_delay_ms: u64,
    pub verify_btree: bool,
    pub verify_secondaries: bool,
    pub verify_data_records: bool,
    pub verify_obsolete_records: bool,
    pub verify_btree_batch_size: u32,
    pub verify_btree_batch_delay_ms: u64,

    // -----------------------------------------------------------------------
    // Stats
    // -----------------------------------------------------------------------
    pub stats_collect: bool,
    pub stats_collect_interval_secs: u64,

    // -----------------------------------------------------------------------
    // Background rate limits
    // -----------------------------------------------------------------------
    pub env_background_read_limit_kb: u32,
    pub env_background_write_limit_kb: u32,
    pub env_background_sleep_interval_us: u64,
}

impl Default for DbiEnvConfig {
    fn default() -> Self {
        Self {
            // Core
            read_only: false,
            transactional: false,
            env_is_locking: true,
            env_recovery_force_checkpoint: false,
            env_recovery_force_new_file: false,
            halt_on_commit_after_checksum_exception: false,
            env_check_leaks: true,
            env_forced_yield: false,
            env_fair_latches: false,
            env_latch_timeout_ms: 300_000,
            env_ttl_clock_tolerance_ms: 0,
            env_expiration_enabled: false,
            env_db_eviction: false,
            // Memory
            cache_size: 64 * 1024 * 1024,
            cache_percent: 0,
            max_off_heap_memory: 0,
            max_disk: 0,
            free_disk: 5 * 1024 * 1024 * 1024,
            // Log
            log_file_max_bytes: 10 * 1024 * 1024,
            log_file_cache_size: 100,
            log_checksum_read: true,
            log_verify_checksums: false,
            log_fsync_timeout_ms: 500_000,
            log_fsync_time_limit_ms: 0,
            log_num_buffers: 3,
            log_buffer_size: 1024 * 1024,
            log_fault_read_size: 65536,
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
            log_group_commit_threshold: 4,
            log_group_commit_interval_ms: 1,
            // B-tree
            node_max_entries: 128,
            node_dup_tree_max_entries: 128,
            tree_max_embedded_ln: 16,
            tree_max_delta: 25,
            tree_bin_delta: true,
            tree_min_memory: 0,
            tree_compact_max_key_length: 16,
            // INCompressor
            run_in_compressor: true,
            in_compressor_wakeup_interval_ms: 5_000,
            compressor_deadlock_retry: 3,
            compressor_lock_timeout_ms: 500,
            compressor_purge_root: false,
            // Cleaner
            run_cleaner: true,
            cleaner_min_utilization: 50,
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
            run_checkpointer: true,
            checkpointer_bytes_interval: 20_000_000,
            checkpointer_wakeup_interval_ms: 30_000,
            checkpointer_deadlock_retry: 3,
            checkpointer_high_priority: false,
            // Evictor
            run_evictor: true,
            evictor_nodes_per_scan: 10,
            evictor_evict_bytes: 512 * 1024,
            evictor_critical_percentage: 5,
            evictor_lru_only: false,
            evictor_use_dirty_lru: true,
            evictor_n_lru_lists: 4,
            evictor_deadlock_retry: 3,
            evictor_core_threads: 1,
            evictor_max_threads: 10,
            evictor_keep_alive_ms: 60_000,
            evictor_allow_bin_deltas: true,
            // Off-heap evictor
            run_offheap_evictor: false,
            offheap_evict_bytes: 512 * 1024,
            offheap_n_lru_lists: 4,
            offheap_checksum: false,
            offheap_core_threads: 1,
            offheap_max_threads: 10,
            offheap_keep_alive_ms: 60_000,
            // Locking
            lock_timeout_ms: 500,
            n_lock_tables: 64,
            lock_deadlock_detect: true,
            lock_deadlock_detect_delay_ms: 0,
            // Transactions
            txn_timeout_ms: 0,
            txn_serializable_isolation: false,
            txn_deadlock_stack_trace: false,
            txn_dump_locks: false,
            // Recovery
            env_recovery_force_checkpoint_field: false,
            // Verifier
            run_verifier: false,
            verify_log: false,
            verify_log_read_delay_ms: 0,
            verify_btree: false,
            verify_secondaries: true,
            verify_data_records: false,
            verify_obsolete_records: false,
            verify_btree_batch_size: 1_000,
            verify_btree_batch_delay_ms: 10,
            // Stats
            stats_collect: false,
            stats_collect_interval_secs: 300,
            // Background rate limits
            env_background_read_limit_kb: 0,
            env_background_write_limit_kb: 0,
            env_background_sleep_interval_us: 0,
        }
    }
}
