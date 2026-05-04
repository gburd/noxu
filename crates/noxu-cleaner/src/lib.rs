#![allow(dead_code, clippy::type_complexity, clippy::too_many_arguments)]
//! Log file garbage collection for Noxu DB.
//!
//! Port of `com.sleepycat.je.cleaner` - tracks per-file space utilization
//! and reclaims space from deleted/obsolete records.

pub mod cleaner;
pub mod cleaner_stat;
pub mod db_file_summary;
pub mod error;
pub mod expiration_tracker;
pub mod file_processor;
pub mod file_protector;
pub mod file_selector;
pub mod file_summary;
pub mod in_summary;
pub mod ln_info;
pub mod packed_offsets;
pub mod tracked_file_summary;
pub mod utilization_profile;
pub mod utilization_tracker;

// Re-exports
pub use cleaner::{CleanResult, Cleaner};
pub use cleaner_stat::{CleanerStats, CleanerStatsSnapshot};
pub use db_file_summary::DbFileSummary;
pub use error::{CleanerError, Result};
pub use expiration_tracker::ExpirationTracker;
pub use file_processor::{
    BinLookupResult, FileProcessResult, FileProcessor, InLookupResult,
    MigrationOutcome, RealTreeLookup, SharedTreeLookup, TreeLookup,
};
pub use file_protector::FileProtector;
pub use file_selector::{
    CheckpointStartCleanerState, FileSelector, FileStatus,
};
pub use file_summary::FileSummary;
pub use in_summary::InSummary;
pub use ln_info::LnInfo;
pub use packed_offsets::PackedOffsets;
pub use tracked_file_summary::TrackedFileSummary;
pub use utilization_profile::UtilizationProfile;
pub use utilization_tracker::UtilizationTracker;
