//! Recovery processing information.
//!

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

    /// ID sequence values recovered from checkpoint.
    pub use_max_node_id: u64,
    pub use_min_replicated_node_id: i64,
    pub use_max_db_id: u64,
    pub use_min_replicated_db_id: i64,
    pub use_max_txn_id: u64,
    pub use_min_replicated_txn_id: i64,
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
            use_max_node_id: 0,
            use_min_replicated_node_id: 0,
            use_max_db_id: 0,
            use_min_replicated_db_id: 0,
            use_max_txn_id: 0,
            use_min_replicated_txn_id: 0,
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
