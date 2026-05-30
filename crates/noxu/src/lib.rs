// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Noxu DB — An embedded transactional key-value database engine in Rust.
//!
//! # Overview
//!
//! This crate bundles all Noxu DB components into a single publishable
//! library. The internal organisation mirrors the former multi-crate
//! workspace; each subsystem is accessible via its own module path:
//!
//! | Module | Former crate | Contents |
//! |---|---|---|
//! | `noxu::util` | `noxu-util` | LSN, VLSN, byte encoding, stats |
//! | `noxu::sync` | `noxu-sync` | Futex-based Mutex/RwLock/Condvar |
//! | `noxu::latch` | `noxu-latch` | B-tree node latching |
//! | `noxu::config` | `noxu-config` | 400+ configuration parameters |
//! | `noxu::log` | `noxu-log` | Write-ahead log, FileManager, LogManager |
//! | `noxu::tree` | `noxu-tree` | B+tree node types (IN, BIN, LN) |
//! | `noxu::txn` | `noxu-txn` | Transactions, locking, deadlock detection |
//! | `noxu::evictor` | `noxu-evictor` | Cache eviction policies |
//! | `noxu::cleaner` | `noxu-cleaner` | Log file garbage collection |
//! | `noxu::recovery` | `noxu-recovery` | Checkpoint-based crash recovery |
//! | `noxu::dbi` | `noxu-dbi` | EnvironmentImpl, DatabaseImpl, CursorImpl |
//! | `noxu::engine` | `noxu-engine` | Engine orchestration, daemon lifecycle |
//! | `noxu::db` | `noxu-db` | Public API facade (also re-exported at root) |
//! | `noxu::bind` | `noxu-bind` | Tuple/serial/serde serialization bindings |
//! | `noxu::collections` | `noxu-collections` | StoredMap, StoredSet, StoredList |
//! | `noxu::persist` | `noxu-persist` | Trait-based entity persistence (DPL) |
//! | `noxu::xa` | `noxu-xa` | XA distributed transactions |
//! | `noxu::rep` | `noxu-rep` | Master-replica HA, elections, VLSN |
//! | `noxu::observe` | `noxu-observe` | Metrics / tracing / OpenTelemetry glue |
//!
//! # Quick Start
//!
//! ```no_run
//! use noxu::{EnvironmentConfig, DatabaseConfig};
//! use std::path::PathBuf;
//!
//! let env_config = EnvironmentConfig::new(PathBuf::from("/tmp/mydb"))
//!     .with_allow_create(true)
//!     .with_transactional(true);
//! ```

#![allow(
    dead_code,
    unused_macros,
    unused_imports,
    clippy::type_complexity,
    clippy::too_many_arguments
)]

// ── Foundation ──────────────────────────────────────────────────────────────
pub mod util;
pub mod sync;
pub mod latch;
pub mod config;

// ── Storage layer ───────────────────────────────────────────────────────────
pub mod log;
pub mod tree;
pub mod txn;
pub mod evictor;
pub mod cleaner;
pub mod recovery;

// ── Database internals ──────────────────────────────────────────────────────
pub mod dbi;
pub mod engine;

// ── Public API ──────────────────────────────────────────────────────────────
pub mod db;

// ── Higher-level APIs ───────────────────────────────────────────────────────
pub mod bind;
pub mod collections;
pub mod persist;
pub mod xa;

// ── Distributed ─────────────────────────────────────────────────────────────
pub mod rep;

// ── Observability (always compiled; backend deps optional) ──────────────────
pub mod observe;

// ── Re-export the entire db public surface at the crate root ─────────────────
// This lets existing code use `noxu::Environment` etc. directly.
pub use db::cache_mode::CacheMode;
pub use db::checkpoint_config::CheckpointConfig;
pub use db::cursor::Cursor;
pub use db::cursor_config::CursorConfig;
pub use db::database::Database;
pub use db::database_config::DatabaseConfig;
pub use db::database_entry::DatabaseEntry;
pub use db::database_stats::{BtreeStats, DatabaseStats};
pub use db::db_iter::{DbIter, DbRange};
pub use db::disk_ordered_cursor::{
    DiskOrderedCursor, DiskOrderedCursorConfig, open_disk_ordered_cursor_multi,
};
pub use db::durability::{Durability, ReplicaAckPolicy, SyncPolicy};
pub use db::environment::Environment;
pub use db::environment_config::{EnvironmentConfig, ExceptionListenerHolder};
pub use db::environment_mutable_config::EnvironmentMutableConfig;
pub use db::error::{
    EnvironmentFailureReason, ExceptionEvent, ExceptionListener,
    ExceptionSource, NoxuError, Result,
};
pub use db::extinction_filter::{ExtinctionFilter, ExtinctionStatus};
pub use db::get::Get;
pub use db::join_config::JoinConfig;
pub use db::join_cursor::JoinCursor;
pub use db::lock_mode::LockMode;
pub use db::operation_result::OperationResult;
pub use db::operation_status::OperationStatus;
pub use db::preload::{PreloadConfig, PreloadStats};
pub use db::put::Put;
pub use db::read_options::ReadOptions;
pub use db::scan_filter::{ScanFilter, ScanResult};
pub use db::secondary_config::{
    ForeignKeyDeleteAction, ForeignKeyNullifier, ForeignMultiKeyNullifier,
    SecondaryConfig, SecondaryKeyCreator, SecondaryMultiKeyCreator,
};
pub use db::secondary_cursor::SecondaryCursor;
pub use db::secondary_database::SecondaryDatabase;
pub use db::sequence::Sequence;
pub use db::sequence_config::SequenceConfig;
pub use db::sequence_stats::SequenceStats;
pub use db::stats_config::StatsConfig;
pub use db::transaction::Transaction;
pub use db::transaction_config::TransactionConfig;
pub use db::write_options::WriteOptions;
pub use engine::{EnvironmentStats, VerifyConfig, VerifyError, VerifyResult};

// ── Proc-macro re-exports (always available via persist module) ──────────────
// These are also accessible via noxu::persist::{Entity, PrimaryKey, SecondaryKey}
pub use noxu_persist_derive::{Entity, PrimaryKey, SecondaryKey};
