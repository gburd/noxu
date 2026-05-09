#![allow(dead_code, clippy::type_complexity, clippy::too_many_arguments)]
//! Database internals for Noxu DB.
//!
//! internal implementations including
//! EnvironmentImpl, DatabaseImpl, CursorImpl, DbTree, MemoryBudget, INList.

pub mod backup_manager;
pub mod cursor_impl;
mod database_config;
pub mod dbi_config;
pub mod dup_key_data;
pub mod throughput_stats;
mod file_manager_scanner;
mod database_id;
mod database_impl;
mod db_tree;
mod db_type;
mod env_failure_reason;
mod env_state;
mod environment_impl;
mod error;
mod get_mode;
mod in_list;
mod memory_budget;
mod node_sequence;
mod operation;
mod operation_status;
mod put_mode;
mod search_mode;
mod truncate_result;

pub use backup_manager::{BackupDestination, BackupManager};
pub use dbi_config::DbiEnvConfig;
pub use cursor_impl::CursorImpl;
#[cfg(any(test, feature = "testing"))]
pub use cursor_impl::{clear_cursor_fail_flag, set_cursor_fail_after};
pub use database_config::DatabaseConfig;
pub use database_id::DatabaseId;
pub use database_impl::{DatabaseImpl, DatabaseTree};
pub use throughput_stats::{ThroughputStats, ThroughputStatsSnapshot};
pub use db_tree::DbTree;
pub use db_type::DbType;
pub use env_failure_reason::EnvironmentFailureReason;
pub use env_state::EnvState;
pub use environment_impl::EnvironmentImpl;
pub use error::{DbiError, Result};
pub use get_mode::GetMode;
pub use in_list::INList;
pub use memory_budget::{MemoryBudget, MemoryBudgetStats, MemoryOverhead};
pub use node_sequence::NodeSequence;
pub use operation::Operation;
pub use operation_status::OperationStatus;
pub use put_mode::PutMode;
pub use search_mode::SearchMode;
pub use truncate_result::TruncateResult;
