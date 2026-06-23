//! Recovery processing information.
//!

use crate::analysis_result::{PreparedLnReplay, PreparedTxnInfo};
use crate::checkpoint_end::CheckpointEnd;
use noxu_util::{Lsn, NULL_LSN};
use std::fmt;

/// Keeps information about recovery processing.
///
/// This structure is populated during recovery and contains the LSNs and IDs
/// needed to restore the database to a consistent state. It tracks checkpoint
/// locations, transaction state, and the last allocated IDs for various
/// database objects.
///
///
#[derive(Debug, Clone)]
pub struct RecoveryInfo {
    /// Location of last entry processed during recovery.
    pub last_used_lsn: Lsn,

    /// First unused spot in the log (end of file).
    pub next_available_lsn: Lsn,

    /// LSN of the first active transaction at checkpoint time.
    pub first_active_lsn: Lsn,

    /// LSN of the checkpoint start entry used for recovery.
    pub checkpoint_start_lsn: Lsn,

    /// LSN of the checkpoint end entry used for recovery.
    pub checkpoint_end_lsn: Lsn,

    /// Root LSN to use for the database tree.
    pub use_root_lsn: Lsn,

    /// First CkptStart following the CkptEnd (indicates partial checkpoint).
    pub partial_checkpoint_start_lsn: Lsn,

    /// The checkpoint end record itself.
    pub checkpoint_end: Option<CheckpointEnd>,

    /// REC-H: the ID of the recovered checkpoint (`CkptEnd.id`), used to
    /// continue the checkpoint-ID sequence after recovery instead of
    /// restarting at 1.  `None` when the log had no prior checkpoint.  The ID
    /// is a debug/log tag, not a correctness key, but it should not regress or
    /// collide across restarts.  JE: `Checkpointer.setCheckpointId`.
    pub recovered_checkpoint_id: Option<u64>,

    /// ID sequence values recovered from checkpoint.
    pub use_max_node_id: u64,
    pub use_min_replicated_node_id: i64,
    pub use_max_db_id: u64,
    pub use_min_replicated_db_id: i64,
    pub use_max_txn_id: u64,
    pub use_min_replicated_txn_id: i64,

    /// Transactions that completed XA phase 1 (`TxnPrepare` written) but
    /// were not committed or rolled back before the crash.  Surfaced to
    /// the XA layer for `xa_recover()` so the transaction manager can
    /// resolve them.  Empty when the WAL contained no in-doubt prepares.
    ///
    /// Wave 3-2 of the v1.5+ remediation plan.
    pub recovered_prepared_txns: Vec<PreparedTxnInfo>,

    /// LN entries (in WAL order) belonging to each prepared transaction
    /// that has not yet been resolved.  Keyed by txn_id.
    ///
    /// `xa_commit(xid)` looks up the entry, replays each LN into the
    /// in-memory tree, and writes a `TxnCommit` frame.
    /// `xa_rollback(xid)` writes a `TxnAbort` frame; nothing has to be
    /// undone in the tree because the prepared writes were never
    /// applied during the redo phase.
    ///
    /// Stored as raw byte vectors instead of `LnRecord` so this struct
    /// stays self-contained (`LnRecord` borrows lifetime-bound `Bytes`).
    pub prepared_txn_lns: hashbrown::HashMap<u64, Vec<PreparedLnReplay>>,

    /// Database name → database ID mappings recovered from `NameLN` / `NameLNTxn`
    /// WAL entries.  Populated by the analysis pass and consumed by
    /// `EnvironmentImpl::new_with_config_inner` to rebuild `name_map` before
    /// any `open_database` call, enabling read-only reopens with
    /// `allow_create = false` to succeed.
    ///
    /// A `None` value means the name was removed (NameLN with is_deleted=true).
    pub recovered_db_names: hashbrown::HashMap<String, u64>,

    /// Database name → persisted comparator identities `(btree, dup)` (DBI-14).
    ///
    /// Mirrors `AnalysisResult::recovered_db_comparators`; consumed by
    /// `EnvironmentImpl::open_database` to enforce comparator mismatch
    /// semantics on open (JE `DatabaseImpl.ComparatorReader`).
    pub recovered_db_comparators:
        hashbrown::HashMap<String, (Option<String>, Option<String>)>,

    /// VLSN→LSN pairs replayed during the redo phase.
    ///
    /// X-14 fix: populated from every LN record that carries a non-zero
    /// VLSN during the redo pass.  Used by `ReplicatedEnvironment::with_environment`
    /// to rebuild the in-memory VLSN index after crash recovery.
    ///
    /// Pairs are (vlsn, lsn.as_u64()); sorted by vlsn ascending.
    pub recovered_vlsns: Vec<(u64, u64)>,

    /// Minimum matchpoint LSN across all completed rollback periods.
    ///
    /// X-1 fix: after recovery, the VLSN index must be truncated to the
    /// VLSN corresponding to this LSN so it is consistent with the
    /// recovered B-tree state.  `None` if no rollbacks were detected.
    pub rollback_matchpoint_lsn: Option<u64>,

    /// CLN-4: per-file `FileSummary` rebuilt from persisted `FileSummaryLN`
    /// records (latest per file) plus obsolete counting for log entries
    /// written after each file's last FileSummaryLN LSN.  Consumed by
    /// `EnvironmentImpl` to seed the cleaner's `UtilizationProfile` so the
    /// cleaner sees real utilization immediately after restart.
    ///
    /// Empty when the WAL had no FileSummaryLN records (a fresh env or one
    /// that never checkpointed).  Keyed by file number; the value carries the
    /// 11-field `FileSummary` breakdown as `(field tuple)` via
    /// `RebuiltFileSummary`.
    ///
    /// JE: `UtilizationProfile.populateCache` builds `fileSummaryMap`;
    /// `RecoveryUtilizationTracker.transferToUtilizationTracker` adds the
    /// recovery-counted obsolete deltas.
    pub rebuilt_file_summaries: hashbrown::HashMap<u32, RebuiltFileSummary>,
}

/// CLN-4: a per-file utilization summary rebuilt during recovery.
///
/// Mirrors the `FileSummary` field layout that the cleaner uses, but lives in
/// `noxu-recovery` to avoid forcing a `noxu-cleaner` dependency on every
/// `RecoveryInfo` consumer.  `EnvironmentImpl` converts it into the cleaner's
/// `FileSummary` when seeding the profile.
#[derive(Debug, Clone, Default)]
pub struct RebuiltFileSummary {
    pub total_count: i32,
    pub total_size: i32,
    pub total_in_count: i32,
    pub total_in_size: i32,
    pub total_ln_count: i32,
    pub total_ln_size: i32,
    pub max_ln_size: i32,
    pub obsolete_in_count: i32,
    pub obsolete_ln_count: i32,
    pub obsolete_ln_size: i32,
    pub obsolete_ln_size_counted: i32,
}

impl RecoveryInfo {
    /// Creates a new RecoveryInfo with all fields initialized to null/zero values.
    pub fn new() -> Self {
        Self {
            last_used_lsn: NULL_LSN,
            next_available_lsn: NULL_LSN,
            first_active_lsn: NULL_LSN,
            checkpoint_start_lsn: NULL_LSN,
            checkpoint_end_lsn: NULL_LSN,
            use_root_lsn: NULL_LSN,
            partial_checkpoint_start_lsn: NULL_LSN,
            checkpoint_end: None,
            recovered_checkpoint_id: None,
            use_max_node_id: 0,
            use_min_replicated_node_id: 0,
            use_max_db_id: 0,
            use_min_replicated_db_id: 0,
            use_max_txn_id: 0,
            use_min_replicated_txn_id: 0,
            recovered_prepared_txns: Vec::new(),
            prepared_txn_lns: hashbrown::HashMap::new(),
            recovered_db_names: hashbrown::HashMap::new(),
            recovered_db_comparators: hashbrown::HashMap::new(),
            recovered_vlsns: Vec::new(),
            rollback_matchpoint_lsn: None,
            rebuilt_file_summaries: hashbrown::HashMap::new(),
        }
    }
}

impl Default for RecoveryInfo {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for RecoveryInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "RecoveryInfo {{\n\
             \x20 last_used_lsn: {},\n\
             \x20 next_available_lsn: {},\n\
             \x20 first_active_lsn: {},\n\
             \x20 checkpoint_start_lsn: {},\n\
             \x20 checkpoint_end_lsn: {},\n\
             \x20 use_root_lsn: {},\n\
             \x20 partial_checkpoint_start_lsn: {},\n\
             \x20 use_max_node_id: {},\n\
             \x20 use_min_replicated_node_id: {},\n\
             \x20 use_max_db_id: {},\n\
             \x20 use_min_replicated_db_id: {},\n\
             \x20 use_max_txn_id: {},\n\
             \x20 use_min_replicated_txn_id: {}\n\
             }}",
            self.last_used_lsn.as_u64(),
            self.next_available_lsn.as_u64(),
            self.first_active_lsn.as_u64(),
            self.checkpoint_start_lsn.as_u64(),
            self.checkpoint_end_lsn.as_u64(),
            self.use_root_lsn.as_u64(),
            self.partial_checkpoint_start_lsn.as_u64(),
            self.use_max_node_id,
            self.use_min_replicated_node_id,
            self.use_max_db_id,
            self.use_min_replicated_db_id,
            self.use_max_txn_id,
            self.use_min_replicated_txn_id,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let info = RecoveryInfo::new();
        assert_eq!(info.last_used_lsn, NULL_LSN);
        assert_eq!(info.next_available_lsn, NULL_LSN);
        assert_eq!(info.first_active_lsn, NULL_LSN);
        assert_eq!(info.checkpoint_start_lsn, NULL_LSN);
        assert_eq!(info.checkpoint_end_lsn, NULL_LSN);
        assert_eq!(info.use_root_lsn, NULL_LSN);
        assert_eq!(info.partial_checkpoint_start_lsn, NULL_LSN);
        assert!(info.checkpoint_end.is_none());
        assert_eq!(info.use_max_node_id, 0);
        assert_eq!(info.use_min_replicated_node_id, 0);
        assert_eq!(info.use_max_db_id, 0);
        assert_eq!(info.use_min_replicated_db_id, 0);
        assert_eq!(info.use_max_txn_id, 0);
        assert_eq!(info.use_min_replicated_txn_id, 0);
    }

    #[test]
    fn test_default() {
        let info = RecoveryInfo::default();
        assert_eq!(info.last_used_lsn, NULL_LSN);
        assert_eq!(info.use_max_node_id, 0);
    }

    #[test]
    fn test_field_assignment() {
        let mut info = RecoveryInfo::new();

        info.last_used_lsn = Lsn::new(1, 100);
        info.next_available_lsn = Lsn::new(2, 200);
        info.first_active_lsn = Lsn::new(3, 300);
        info.checkpoint_start_lsn = Lsn::new(4, 400);
        info.checkpoint_end_lsn = Lsn::new(5, 500);
        info.use_root_lsn = Lsn::new(6, 600);
        info.partial_checkpoint_start_lsn = Lsn::new(7, 700);

        info.use_max_node_id = 1000;
        info.use_min_replicated_node_id = -1000;
        info.use_max_db_id = 2000;
        info.use_min_replicated_db_id = -2000;
        info.use_max_txn_id = 3000;
        info.use_min_replicated_txn_id = -3000;

        assert_eq!(info.last_used_lsn, Lsn::new(1, 100));
        assert_eq!(info.next_available_lsn, Lsn::new(2, 200));
        assert_eq!(info.first_active_lsn, Lsn::new(3, 300));
        assert_eq!(info.checkpoint_start_lsn, Lsn::new(4, 400));
        assert_eq!(info.checkpoint_end_lsn, Lsn::new(5, 500));
        assert_eq!(info.use_root_lsn, Lsn::new(6, 600));
        assert_eq!(info.partial_checkpoint_start_lsn, Lsn::new(7, 700));

        assert_eq!(info.use_max_node_id, 1000);
        assert_eq!(info.use_min_replicated_node_id, -1000);
        assert_eq!(info.use_max_db_id, 2000);
        assert_eq!(info.use_min_replicated_db_id, -2000);
        assert_eq!(info.use_max_txn_id, 3000);
        assert_eq!(info.use_min_replicated_txn_id, -3000);
    }

    #[test]
    fn test_to_string() {
        let mut info = RecoveryInfo::new();
        info.last_used_lsn = Lsn::new(1, 100);
        info.use_max_node_id = 1000;

        let s = info.to_string();
        assert!(s.contains("RecoveryInfo"));
        assert!(s.contains("last_used_lsn"));
        assert!(s.contains("1000")); // use_max_node_id value
    }

    #[test]
    fn test_display_impl() {
        let info = RecoveryInfo::new();
        let s = format!("{}", info);
        assert!(s.contains("RecoveryInfo"));
    }

    #[test]
    fn test_checkpoint_end_field() {
        let mut info = RecoveryInfo::new();
        assert!(info.checkpoint_end.is_none());

        let ckpt_end = CheckpointEnd::new(
            123,
            "test",
            Lsn::new(1, 0),
            None,
            NULL_LSN,
            0,
            0,
            0,
            0,
            0,
            0,
            false,
        );

        info.checkpoint_end = Some(ckpt_end);
        assert!(info.checkpoint_end.is_some());
        assert_eq!(info.checkpoint_end.as_ref().unwrap().get_id(), 123);
    }

    #[test]
    fn test_negative_replicated_ids() {
        let mut info = RecoveryInfo::new();
        info.use_min_replicated_node_id = i64::MIN;
        info.use_min_replicated_db_id = i64::MIN;
        info.use_min_replicated_txn_id = i64::MIN;

        assert_eq!(info.use_min_replicated_node_id, i64::MIN);
        assert_eq!(info.use_min_replicated_db_id, i64::MIN);
        assert_eq!(info.use_min_replicated_txn_id, i64::MIN);
    }

    #[test]
    fn test_max_values() {
        let mut info = RecoveryInfo::new();
        info.use_max_node_id = u64::MAX;
        info.use_max_db_id = u64::MAX;
        info.use_max_txn_id = u64::MAX;

        assert_eq!(info.use_max_node_id, u64::MAX);
        assert_eq!(info.use_max_db_id, u64::MAX);
        assert_eq!(info.use_max_txn_id, u64::MAX);
    }
}
