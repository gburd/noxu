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
    // Core
    pub read_only: bool,
    pub transactional: bool,

    // Memory
    /// Maximum B-tree cache size in bytes.
    pub cache_size: u64,

    // Log
    pub log_file_max_bytes: u64,
    pub log_num_buffers: usize,
    /// Per-buffer size in bytes (= log_total_buffer_bytes / log_num_buffers).
    pub log_buffer_size: usize,
    pub log_fault_read_size: usize,
    pub log_group_commit_threshold: usize,
    pub log_group_commit_interval_ms: u64,

    // Cleaner
    pub run_cleaner: bool,
    pub cleaner_min_utilization: u8,
    pub cleaner_min_file_count: u32,
    pub cleaner_min_age: u32,

    // Checkpointer
    pub run_checkpointer: bool,
    pub checkpointer_bytes_interval: u64,
    /// Checkpointer daemon sleep interval in milliseconds.
    pub checkpointer_interval_ms: u64,

    // Evictor
    pub run_evictor: bool,
    pub evictor_nodes_per_scan: usize,
    pub evictor_lru_only: bool,

    // Locking
    pub lock_timeout_ms: u64,

    // Transactions
    pub txn_timeout_ms: u64,
}

impl Default for DbiEnvConfig {
    fn default() -> Self {
        Self {
            read_only: false,
            transactional: false,
            cache_size: 64 * 1024 * 1024,
            log_file_max_bytes: 10 * 1024 * 1024,
            log_num_buffers: 3,
            log_buffer_size: 1024 * 1024, // 1 MiB per buffer
            log_fault_read_size: 65536,
            log_group_commit_threshold: 0,
            log_group_commit_interval_ms: 0,
            run_cleaner: true,
            cleaner_min_utilization: 50,
            cleaner_min_file_count: 2,
            cleaner_min_age: 0,
            run_checkpointer: true,
            checkpointer_bytes_interval: 20_000_000,
            checkpointer_interval_ms: 30_000,
            run_evictor: true,
            evictor_nodes_per_scan: 10,
            evictor_lru_only: false,
            lock_timeout_ms: 500,
            txn_timeout_ms: 0,
        }
    }
}
