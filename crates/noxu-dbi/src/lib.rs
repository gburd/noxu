#![forbid(unsafe_code)]
// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT
#![allow(dead_code, clippy::type_complexity, clippy::too_many_arguments)]
//! > **Internal component of the [`noxu`](https://crates.io/crates/noxu) database.**
//! >
//! > This crate is published only so the `noxu` umbrella crate can depend on it.
//! > Use `noxu` (`noxu = "7"`) in applications; depend on this crate directly only
//! > if you are extending the engine internals. Its API may change without a major
//! > version bump.
//!
//! Database internals for Noxu DB.
//!
//! internal implementations including
//! EnvironmentImpl, DatabaseImpl, CursorImpl, DbTree, MemoryBudget.

pub mod backup_manager;
pub mod cursor_impl;
mod database_config;
mod database_id;
mod database_impl;
mod db_tree;
mod db_type;
pub mod dbi_config;
pub mod disk_limit;
pub mod disk_ordered_cursor_impl;
pub mod dup_key_data;
mod env_failure_reason;
mod env_state;
mod environment_impl;
mod error;
mod file_manager_scanner;
mod get_mode;
mod memory_budget;
pub mod name_ln_codec;
mod node_sequence;
mod operation;
mod operation_status;
mod put_mode;
pub mod replica_ack;
pub mod replica_replay;
mod search_mode;
pub mod throughput_stats;
pub mod trigger;
mod truncate_result;

pub use backup_manager::{BackupDestination, BackupManager};
pub use cursor_impl::CursorImpl;
#[cfg(any(test, feature = "testing"))]
pub use cursor_impl::{clear_cursor_fail_flag, set_cursor_fail_after};
pub use database_config::{ConfigComparator, DatabaseConfig};
pub use database_id::DatabaseId;
pub use database_impl::{DatabaseImpl, DatabaseTree};
pub use db_tree::DbTree;
pub use db_type::DbType;
pub use dbi_config::DbiEnvConfig;
pub use disk_ordered_cursor_impl::{
    DiskOrderedCursorImpl, DiskOrderedCursorOptions,
};
pub use env_failure_reason::EnvironmentFailureReason;
pub use env_state::EnvState;
pub use environment_impl::EnvironmentImpl;
pub use replica_replay::ReplicaReplay;
pub use trigger::Trigger;
// EV-15: re-export the evictor so noxu-db can cache an Arc<Evictor> for
// per-write critical eviction without taking a direct noxu-evictor dependency.
pub use error::{DbiError, Result};
pub use get_mode::GetMode;
pub use memory_budget::{MemoryBudget, MemoryBudgetStats, MemoryOverhead};
pub use node_sequence::NodeSequence;
pub use noxu_evictor::Evictor;
pub use operation::Operation;
pub use operation_status::OperationStatus;
pub use put_mode::PutMode;
pub use replica_ack::{
    AckWaitError, AckWaitErrorKind, ReplicaAckCoordinator,
    ReplicaAckPolicyKind, SharedReplicaAckCoordinator,
};
pub use search_mode::SearchMode;
pub use throughput_stats::{ThroughputStats, ThroughputStatsSnapshot};
pub use truncate_result::TruncateResult;
