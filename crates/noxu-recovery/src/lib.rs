#![forbid(unsafe_code)]
// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT

#![allow(dead_code, clippy::type_complexity, clippy::too_many_arguments)]
//! Checkpoint-based crash recovery for Noxu DB.
//!
//! performs recovery during Environment
//! open and periodic checkpointing to bound recovery time.

pub mod analysis_result;
pub mod checkpoint_end;
pub mod checkpoint_start;
pub mod checkpoint_stat;
pub mod checkpointer;
pub mod dirty_in_map;
pub mod error;
pub mod log_scanner;
pub mod recovery_info;
pub mod recovery_manager;
pub mod rollback_tracker;

pub use analysis_result::{
    AnalysisResult, DirtyInEntry, DirtyInKey, PreparedLnOperation,
    PreparedLnReplay, PreparedTxnInfo,
};
pub use checkpoint_end::CheckpointEnd;
pub use checkpoint_start::CheckpointStart;
pub use checkpoint_stat::{CheckpointStats, CheckpointStatsSnapshot};
pub use checkpointer::{CheckpointConfig, CheckpointResult, Checkpointer};
pub use dirty_in_map::{CheckpointReference, CkptState, DirtyINMap};
pub use error::{RecoveryError, Result};
pub use log_scanner::{
    CkptEndRecord, CkptStartRecord, DbTreeRecord, InMemoryLogScanner, InRecord,
    LnOperation, LnRecord, LogEntry, LogScanner, NameLnRecord, PositionedEntry,
    RollbackEndRecord, RollbackStartRecord, TxnAbortRecord, TxnCommitRecord,
    TxnPrepareRecord,
};
pub use recovery_info::RecoveryInfo;
pub use recovery_manager::{
    RecoveryManager, RecoveryProgress, RecoveryStats, RedoAction, UndoAction,
};
pub use rollback_tracker::{RollbackPeriod, RollbackScanner, RollbackTracker};
