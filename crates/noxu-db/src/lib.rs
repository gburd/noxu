#![allow(dead_code, clippy::type_complexity, clippy::too_many_arguments)]
//! Noxu DB - An embedded transactional database engine.
//!
//! Public API : Environment, Database,
//! Cursor, Transaction, DatabaseEntry, SecondaryDatabase, Sequence, etc.
//!
//! This crate provides the public API for Noxu DB.
//! Java Edition. It is designed to be familiar to BDB users while being
//! idiomatic Rust.
//!
//! # Example
//!
//! ```ignore
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

pub mod byte_comparator;
pub mod cache_mode;
pub mod cursor;
pub mod cursor_config;
pub mod database;
pub mod database_config;
pub mod database_entry;
pub mod durability;
pub mod environment;
pub mod environment_config;
pub mod error;
pub mod extinction_filter;
pub mod get;
pub mod lock_mode;
pub mod operation_result;
pub mod operation_status;
pub mod put;
pub mod read_options;
pub mod scan_filter;
pub mod secondary_config;
pub mod secondary_cursor;
pub mod secondary_database;
pub mod sequence;
pub mod sequence_config;
pub mod sequence_stats;
pub mod transaction;
pub mod transaction_config;
pub mod write_options;

// Re-export commonly used types
pub use byte_comparator::{ByteComparator, DefaultByteComparator, compare_unsigned};
pub use cache_mode::CacheMode;
pub use cursor::Cursor;
pub use cursor_config::CursorConfig;
pub use database::Database;
pub use database_config::DatabaseConfig;
pub use database_entry::DatabaseEntry;
pub use durability::{Durability, ReplicaAckPolicy, SyncPolicy};
pub use environment::Environment;
pub use environment_config::{EnvironmentConfig, ExceptionListenerHolder};
pub use noxu_engine::EnvironmentStats;
pub use error::{
    EnvironmentFailureReason, ExceptionEvent, ExceptionListener, ExceptionSource, NoxuError, Result,
};
pub use extinction_filter::{ExtinctionFilter, ExtinctionStatus};
pub use get::Get;
pub use lock_mode::LockMode;
pub use operation_result::OperationResult;
pub use operation_status::OperationStatus;
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
pub use transaction::Transaction;
pub use transaction_config::TransactionConfig;
pub use write_options::WriteOptions;
