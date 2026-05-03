//! Log entry types and traits.
//!
//! Port of `com.sleepycat.je.log.entry` package.
//!
//! Each log entry in the WAL consists of a header (managed by the log manager)
//! followed by entry-specific data. This module defines the types for various
//! log entry payloads.

pub mod bin_delta_log_entry;
pub mod commit_abort_entry;
pub mod db_operation_type;
pub mod empty_log_entry;
pub mod file_header_entry;
pub mod in_log_entry;
pub mod ln_log_entry;
pub mod name_ln_log_entry;
pub mod replication_context;
pub mod restore_required;
pub mod trace_log_entry;

pub use bin_delta_log_entry::BinDeltaLogEntry;
pub use commit_abort_entry::TxnEndEntry;
pub use db_operation_type::DbOperationType;
pub use empty_log_entry::EmptyLogEntry;
pub use file_header_entry::{FileHeader, FileHeaderEntry};
pub use in_log_entry::InLogEntry;
pub use ln_log_entry::LnLogEntry;
pub use name_ln_log_entry::NameLnLogEntry;
pub use replication_context::ReplicationContext;
pub use restore_required::RestoreRequired;
pub use trace_log_entry::TraceLogEntry;
