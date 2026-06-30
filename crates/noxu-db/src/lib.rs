// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT

#![allow(
    dead_code,
    unused_macros,
    unused_imports,
    clippy::type_complexity,
    clippy::too_many_arguments
)]
//! > **Internal component of the [`noxu`](https://crates.io/crates/noxu) database.**
//! >
//! > This crate is published only so the `noxu` umbrella crate can depend on it.
//! > Use `noxu` (`noxu = "3"`) in applications; depend on this crate directly only
//! > if you are extending the engine internals. Its API may change without a major
//! > version bump.
//!
//! Noxu DB - An embedded transactional database engine.
//!
//! Public API : Environment, Database,
//! Cursor, Transaction, DatabaseEntry, SecondaryDatabase, Sequence, etc.
//!
//! This crate provides the public API for Noxu DB.
//! It is designed to be familiar to embedded database users while being
//! idiomatic Rust.
//!
//! # Example
//!
//! ```no_run
//! use noxu_db::{EnvironmentConfig, DatabaseConfig};
//! use std::path::PathBuf;
//!
//! let env_config = EnvironmentConfig::new(PathBuf::from("/tmp/mydb"))
//!     .with_allow_create(true)
//!     .with_transactional(true);
//!
//! let db_config = DatabaseConfig::new()
//!     .with_allow_create(true)
//!     .with_transactional(true);
//! ```

#[macro_use]
mod observe;
pub mod unimplemented_params;

/// Re-export of the observability crate when the `observability` feature is enabled.
/// Users can access `noxu_db::observe_crate::{metrics, tracing}` for their own
/// recorder/subscriber setup.
#[cfg(feature = "observability")]
pub use noxu_observe as observe_crate;

/// Periodic metrics exporter (samples `stats()` to the `metrics` facade).
/// Only available with the `observability` feature.
#[cfg(feature = "observability")]
pub mod metrics_export;

/// Re-export of the synchronization primitive that appears in this crate's
/// public API.  `SecondaryDatabase::open` takes the primary database wrapped
/// in `Arc<Mutex<Database>>`; this re-export lets callers name that `Mutex`
/// (and its guard) without depending on the internal `noxu-sync` crate
/// directly — reachable as `noxu::Mutex` through the umbrella.
pub use noxu_sync::{Mutex, MutexGuard};

/// Re-exports of `noxu-dbi` types that appear in public API signatures.
///
/// `Environment::set_replica_coordinator` takes a
/// `SharedReplicaAckCoordinator`; users implementing a custom
/// `ReplicaAckCoordinator` for testing need `ReplicaAckCoordinator` and
/// `AckWaitError`/`AckWaitErrorKind` without depending on the internal
/// `noxu-dbi` crate directly.  All are reachable as `noxu::Type` through
/// the umbrella crate.
///
/// Closes re-audit JE F-6 and the partial fix noted in reaudit-jonhoo #3.
pub use noxu_dbi::{
    AckWaitError, AckWaitErrorKind, ReplicaAckCoordinator,
    ReplicaAckPolicyKind, SharedReplicaAckCoordinator,
};

/// Re-exports of `noxu-recovery` types that appear in public API signatures.
///
/// `Environment::recovered_prepared_txns` returns `Vec<PreparedTxnInfo>`;
/// `Environment::take_recovered_prepared_lns` returns
/// `Vec<PreparedLnReplay>`, and `apply_recovered_prepared_lns` takes
/// `&[PreparedLnReplay]`.  XA users need these types to name return values
/// without depending on the internal `noxu-recovery` crate directly.
///
/// Closes reaudit-jonhoo #3.
pub use noxu_recovery::{
    PreparedLnOperation, PreparedLnReplay, PreparedTxnInfo,
};

pub mod cache_mode;
pub mod checkpoint_config;
pub mod cursor;
pub mod cursor_config;
pub mod database;
pub mod database_config;
pub mod database_entry;
pub mod database_stats;
pub mod db_iter;
pub mod disk_ordered_cursor;
pub mod durability;
pub mod environment;
pub mod environment_config;
pub mod environment_mutable_config;
pub mod error;
pub mod extinction_filter;
pub mod get;
pub mod join_config;
pub mod join_cursor;
pub mod lock_mode;
pub mod operation_result;
pub mod operation_status;
pub mod preload;
pub mod put;
pub mod read_options;
pub mod scan_filter;
pub mod secondary_config;
pub mod secondary_cursor;
pub mod secondary_database;
pub mod sequence;
pub mod sequence_config;
pub mod sequence_stats;
pub mod stats_config;
pub mod transaction;
pub mod transaction_config;
pub mod write_options;

// Re-export commonly used types
pub use cache_mode::CacheMode;
pub use checkpoint_config::CheckpointConfig;
pub use cursor::Cursor;
pub use cursor_config::CursorConfig;
pub use database::Database;
pub use database_config::{Comparator, DatabaseConfig};
pub use database_entry::DatabaseEntry;
pub use database_stats::{BtreeStats, DatabaseStats};
pub use db_iter::{DbIter, DbRange};
pub use disk_ordered_cursor::{
    DiskOrderedCursor, DiskOrderedCursorConfig, open_disk_ordered_cursor_multi,
};
pub use durability::{Durability, ReplicaAckPolicy, SyncPolicy};
pub use environment::Environment;
pub use environment_config::{EnvironmentConfig, ExceptionListenerHolder};
pub use environment_mutable_config::EnvironmentMutableConfig;
pub use error::{
    EnvironmentFailureReason, ExceptionEvent, ExceptionListener,
    ExceptionSource, NoxuError, Result,
};
pub use extinction_filter::{ExtinctionFilter, ExtinctionStatus};
pub use get::Get;
pub use join_config::JoinConfig;
pub use join_cursor::JoinCursor;
pub use lock_mode::LockMode;
pub use noxu_dbi::Trigger;
pub use noxu_engine::{
    EnvironmentStats, VerifyConfig, VerifyError, VerifyResult,
};
pub use operation_result::OperationResult;
pub use operation_status::OperationStatus;
pub use preload::{PreloadConfig, PreloadStats};
pub use put::Put;
pub use read_options::ReadOptions;
pub use scan_filter::{ScanFilter, ScanResult};
pub use secondary_config::{
    ForeignKeyDeleteAction, ForeignKeyNullifier, ForeignMultiKeyNullifier,
    SecondaryConfig, SecondaryKeyCreator, SecondaryMultiKeyCreator,
};
pub use secondary_cursor::SecondaryCursor;
pub use secondary_database::SecondaryDatabase;
pub use sequence::Sequence;
pub use sequence_config::SequenceConfig;
pub use sequence_stats::SequenceStats;
pub use stats_config::StatsConfig;
pub use transaction::Transaction;
pub use transaction_config::TransactionConfig;
pub use write_options::WriteOptions;
