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
//! Log file garbage collection for Noxu DB.
//!
//! tracks per-file space utilization
//! and reclaims space from deleted/obsolete records.

pub mod cleaner;
pub mod cleaner_stat;
pub mod data_eraser;
pub mod db_file_summary;
pub mod error;
pub mod expiration_profile;
pub mod expiration_tracker;
pub mod extinction_scanner;
pub mod file_processor;
pub mod file_protector;
pub mod file_selector;
pub mod file_summary;
pub mod in_summary;
pub mod ln_info;
pub mod packed_offsets;
pub mod throttle;
pub mod tracked_file_summary;
pub mod utilization_profile;
pub mod utilization_tracker;
pub mod utilization_tracker_observer;
pub mod verify_utils;

// Re-exports
pub use cleaner::{CleanResult, Cleaner};
pub use cleaner_stat::{CleanerStats, CleanerStatsSnapshot};
pub use data_eraser::{DataEraser, EraseRequest};
pub use db_file_summary::DbFileSummary;
pub use error::{CleanerError, Result};
pub use expiration_profile::{ExpirationProfile, ExpirationProfileStore};
pub use expiration_tracker::ExpirationTracker;
pub use extinction_scanner::{ExtinctionScanner, ExtinctionTask};
pub use file_processor::{
    BinLookupResult, FileProcessResult, FileProcessor, InLookupResult,
    MigrationOutcome, PROCESS_PENDING_EVERY_N_LNS_PUB, RealTreeLookup,
    SharedTreeLookup, TreeLookup,
};
pub use file_protector::FileProtector;
pub use file_selector::{
    CheckpointStartCleanerState, FileSelector, FileStatus,
};
pub use file_summary::FileSummary;
pub use in_summary::InSummary;
pub use ln_info::LnInfo;
pub use packed_offsets::PackedOffsets;
pub use throttle::CleanerThrottle;
pub use tracked_file_summary::TrackedFileSummary;
pub use utilization_profile::UtilizationProfile;
pub use utilization_tracker::{ObsoleteKind, UtilizationTracker};
pub use utilization_tracker_observer::UtilizationTrackerObserver;
pub use verify_utils::{CheckLsnsResult, check_lsns, obsolete_lsn_set};
