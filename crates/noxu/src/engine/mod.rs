// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT
#![allow(dead_code, clippy::type_complexity, clippy::too_many_arguments)]
//! Engine orchestration for Noxu DB.
//!
//! Wires together all internal subsystems, manages daemon thread lifecycle,
//! and coordinates environment open/close.
//!
//! # Overview
//!
//! The `noxu-engine` crate is the internal orchestration layer for Noxu DB.
//! It brings together all the subsystems built in earlier phases:
//!
//! - **noxu-dbi**: EnvironmentImpl, DatabaseImpl, CursorImpl
//! - **noxu-txn**: Transaction and lock management
//! - **noxu-evictor**: Cache eviction
//! - **noxu-cleaner**: Log garbage collection
//! - **noxu-recovery**: Checkpointing and recovery
//!
//! # Architecture
//!
//! The [`Engine`] struct is the central coordination point. It:
//! 1. Creates and owns all subsystems
//! 2. Runs recovery on environment open
//! 3. Starts background daemon threads (evictor, cleaner, checkpointer)
//! 4. Coordinates orderly shutdown
//! 5. Provides unified access to all subsystems
//!
//! # Usage
//!
//! ```rust,ignore
//! use crate::engine::{Engine, EngineConfig};
//!
//! // Open an environment
//! let config = EngineConfig::new("/data/mydb")
//!     .cache_size(128 * 1024 * 1024)
//!     .transactional(true);
//! let engine = Engine::open(config)?;
//!
//! // Use the engine...
//! let stats = engine.get_stats();
//! println!("Cache usage: {:.1}%", stats.cache_utilization_percent());
//!
//! // Explicitly close (or drop will close)
//! engine.close()?;
//! ```
//!
//! # Configuration
//!
//! Environment behavior is controlled via [`EngineConfig`]:
//!
//! - Cache size and eviction settings
//! - Transaction and lock timeouts
//! - Daemon thread intervals
//! - Read-only mode
//! - Checkpoint and cleaning thresholds
//!
//! # Background Daemons
//!
//! Three daemon threads run in the background:
//!
//! 1. **Evictor**: Evicts nodes from cache when memory budget is exceeded
//! 2. **Cleaner**: Garbage collects log files when utilization is low
//! 3. **Checkpointer**: Flushes dirty nodes to bound recovery time
//!
//! All daemons can be individually enabled/disabled via configuration.
//!
//! # Statistics
//!
//! [`EnvironmentStats`] provides a unified view of all subsystem statistics:
//!
//! - Cache usage and eviction metrics
//! - Cleaning progress and file statistics
//! - Checkpoint frequency and flush counts
//! - Database and transaction counts

pub mod daemon_manager;
pub mod engine;
pub mod engine_config;
pub mod env_stats;
pub mod error;
pub mod verify;

// Re-export main types at crate root
pub use daemon_manager::DaemonManager;
pub use engine::Engine;
pub use engine_config::EngineConfig;
pub use env_stats::EnvironmentStats;
pub use error::{EngineError, Result};
pub use verify::{
    VerifyConfig, VerifyError, VerifyResult, verify_database,
    verify_database_impl, verify_environment, verify_tree,
};

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_crate_public_api() {
        // Verify all main types are re-exported
        let _: EngineConfig;
        let _: Engine;
        let _: EnvironmentStats;
        let _: EngineError;
        let _: Result<()>;
    }

    #[test]
    fn test_basic_engine_lifecycle() {
        let dir = TempDir::new().unwrap();
        let config = EngineConfig::new(dir.path())
            .cache_size(10 * 1024 * 1024)
            .evictor_wakeup_interval_ms(100)
            .cleaner_wakeup_interval_ms(100)
            .checkpointer_wakeup_interval_ms(100);

        let engine = Engine::open(config).unwrap();
        assert!(engine.is_open());

        let stats = engine.get_stats();
        assert_eq!(stats.cache_size, 10 * 1024 * 1024);

        engine.close().unwrap();
        assert!(!engine.is_open());
    }

    #[test]
    fn test_config_validation() {
        let dir = TempDir::new().unwrap();

        // Valid config
        let config = EngineConfig::new(dir.path());
        assert!(config.validate().is_ok());

        // Invalid: cache too small
        let config = EngineConfig::new(dir.path()).cache_size(1024);
        assert!(config.validate().is_err());

        // Invalid: zero lock tables
        let config = EngineConfig::new(dir.path()).lock_table_count(0);
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_readonly_mode() {
        let dir = TempDir::new().unwrap();
        let config = EngineConfig::new(dir.path())
            .read_only(true)
            .cleaner_enabled(false)
            .checkpointer_enabled(false);

        let engine = Engine::open(config).unwrap();
        assert!(engine.get_config().read_only);

        // Write operations should fail
        assert!(engine.checkpoint("test").is_err());
        assert!(engine.clean(5).is_err());
    }

    #[test]
    fn test_subsystem_access() {
        let dir = TempDir::new().unwrap();
        let config = EngineConfig::new(dir.path()).cache_size(10 * 1024 * 1024);

        let engine = Engine::open(config).unwrap();

        // Verify subsystem access
        let env_impl = engine.get_env_impl();
        assert!(env_impl.lock().is_open());

        let evictor = engine.get_evictor();
        let _ = evictor.get_stats();

        let cleaner = engine.get_cleaner();
        let _ = cleaner.get_stats();

        let checkpointer = engine.get_checkpointer();
        let _ = checkpointer.get_stats();
    }
}
