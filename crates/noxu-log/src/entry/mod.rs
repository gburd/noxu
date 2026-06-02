//! Log entry types and traits.
//!
//!
//! Each log entry in the WAL consists of a header (managed by the log manager)
//! followed by entry-specific data. This module defines the types for various
//! log entry payloads.

pub mod bin_delta_log_entry;
pub mod db_tree_entry;
pub mod commit_abort_entry;
pub mod db_operation_type;
pub mod del_dup_ln_entry;
pub mod dup_count_ln_entry;
pub mod empty_log_entry;
pub mod file_header_entry;
pub mod file_summary_ln_entry;
pub mod immutable_file_entry;
pub mod in_delete_info_entry;
pub mod in_dupdelete_info_entry;
pub mod in_log_entry;
pub mod ln_log_entry;
pub mod matchpoint_entry;
pub mod name_ln_log_entry;
pub mod old_bin_delta_entry;
pub mod old_ln_entry;
pub mod replication_context;
pub mod restore_required;
pub mod rollback_end_entry;
pub mod rollback_start_entry;
pub mod trace_log_entry;
pub mod txn_prepare_entry;

pub use bin_delta_log_entry::BinDeltaLogEntry;
pub use db_tree_entry::{DbTreeBinRef, DbTreeEntry, DbTreeEntryError};
pub use commit_abort_entry::TxnEndEntry;
pub use db_operation_type::DbOperationType;
pub use del_dup_ln_entry::DelDupLnEntry;
pub use dup_count_ln_entry::DupCountLnEntry;
pub use empty_log_entry::EmptyLogEntry;
pub use file_header_entry::{FileHeader, FileHeaderEntry};
pub use file_summary_ln_entry::FileSummaryLnEntry;
pub use immutable_file_entry::ImmutableFileEntry;
pub use in_delete_info_entry::InDeleteInfoEntry;
pub use in_dupdelete_info_entry::InDupDeleteInfoEntry;
pub use in_log_entry::InLogEntry;
pub use ln_log_entry::{LnEntryRef, LnLogEntry};
pub use matchpoint_entry::MatchpointEntry;
pub use name_ln_log_entry::NameLnLogEntry;
pub use old_bin_delta_entry::OldBinDeltaEntry;
pub use old_ln_entry::OldLnEntry;
pub use replication_context::ReplicationContext;
pub use restore_required::RestoreRequired;
pub use rollback_end_entry::RollbackEndEntry;
pub use rollback_start_entry::RollbackStartEntry;
pub use trace_log_entry::TraceLogEntry;
pub use txn_prepare_entry::{TxnPrepareEntry, TxnPrepareError};
