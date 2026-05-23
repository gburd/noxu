//! Log entry type definitions.
//!
//!
//! LogEntryType is an enumeration of all log entry types. Each type has
//! associated metadata: type number, version, transactional/replication flags,
//! and marshalling behavior.

use std::fmt;

/// Current log version for Noxu DB.
///
/// This is a NEW Rust-native log format (NOT binary-compatible with ).
/// We start at version 1 for the Noxu format.
pub const LOG_VERSION: u8 = 1;

/// First valid log version.
pub const FIRST_LOG_VERSION: u8 = 1;

/// Maximum number of entry types supported.
const MAX_TYPE_NUM: usize = 128;

/// Log entry type enumeration.
///
/// Each variant represents a distinct type of log entry with specific
/// serialization and recovery semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum LogEntryType {
    // File metadata
    FileHeader = 1,

    // Tree nodes - upper IN levels
    IN = 2,
    BIN = 3,
    BINDelta = 4,

    // User data LNs
    InsertLN = 10,
    UpdateLN = 11,
    DeleteLN = 12,
    InsertLNTxn = 13,
    UpdateLNTxn = 14,
    DeleteLNTxn = 15,

    // Internal database LNs
    MapLN = 20,
    NameLN = 21,
    NameLNTxn = 22,
    FileSummaryLN = 23,

    // Transaction records
    TxnCommit = 30,
    TxnAbort = 31,
    TxnPrepare = 32,

    // Checkpoint records
    CkptStart = 40,
    CkptEnd = 41,

    // Database tree root
    DbTree = 50,

    // Utility/debugging
    Trace = 60,

    // Replication
    Matchpoint = 61,

    // HA rollback markers
    RollbackStart = 62,
    RollbackEnd = 63,

    // Tree compression
    INDeleteInfo = 64,
    INDupDeleteInfo = 65,

    // Legacy / old-format entries (for recovery compatibility)
    OldBINDelta = 66,
    OldLN = 67,
    DelDupLN = 68,
    DupCountLN = 69,

    // File lifecycle
    ImmutableFile = 70,
}

impl LogEntryType {
    /// Returns the type number (persistent identifier) for this entry type.
    #[inline]
    pub fn type_num(self) -> u8 {
        self as u8
    }

    /// Looks up a log entry type by its type number.
    pub fn from_type_num(type_num: u8) -> Option<Self> {
        match type_num {
            1 => Some(LogEntryType::FileHeader),
            2 => Some(LogEntryType::IN),
            3 => Some(LogEntryType::BIN),
            4 => Some(LogEntryType::BINDelta),
            10 => Some(LogEntryType::InsertLN),
            11 => Some(LogEntryType::UpdateLN),
            12 => Some(LogEntryType::DeleteLN),
            13 => Some(LogEntryType::InsertLNTxn),
            14 => Some(LogEntryType::UpdateLNTxn),
            15 => Some(LogEntryType::DeleteLNTxn),
            20 => Some(LogEntryType::MapLN),
            21 => Some(LogEntryType::NameLN),
            22 => Some(LogEntryType::NameLNTxn),
            23 => Some(LogEntryType::FileSummaryLN),
            30 => Some(LogEntryType::TxnCommit),
            31 => Some(LogEntryType::TxnAbort),
            32 => Some(LogEntryType::TxnPrepare),
            40 => Some(LogEntryType::CkptStart),
            41 => Some(LogEntryType::CkptEnd),
            50 => Some(LogEntryType::DbTree),
            60 => Some(LogEntryType::Trace),
            61 => Some(LogEntryType::Matchpoint),
            62 => Some(LogEntryType::RollbackStart),
            63 => Some(LogEntryType::RollbackEnd),
            64 => Some(LogEntryType::INDeleteInfo),
            65 => Some(LogEntryType::INDupDeleteInfo),
            66 => Some(LogEntryType::OldBINDelta),
            67 => Some(LogEntryType::OldLN),
            68 => Some(LogEntryType::DelDupLN),
            69 => Some(LogEntryType::DupCountLN),
            70 => Some(LogEntryType::ImmutableFile),
            _ => None,
        }
    }

    /// Returns true if this log entry type is valid.
    #[inline]
    pub fn is_valid(type_num: u8) -> bool {
        Self::from_type_num(type_num).is_some()
    }

    /// Returns true if this log entry holds transactional information.
    pub fn is_transactional(self) -> bool {
        matches!(
            self,
            LogEntryType::InsertLNTxn
                | LogEntryType::UpdateLNTxn
                | LogEntryType::DeleteLNTxn
                | LogEntryType::NameLNTxn
                | LogEntryType::TxnCommit
                | LogEntryType::TxnAbort
                | LogEntryType::TxnPrepare
        )
    }

    /// Returns true if this log entry type can be replicated.
    pub fn is_replication_possible(self) -> bool {
        matches!(
            self,
            LogEntryType::InsertLN
                | LogEntryType::UpdateLN
                | LogEntryType::DeleteLN
                | LogEntryType::InsertLNTxn
                | LogEntryType::UpdateLNTxn
                | LogEntryType::DeleteLNTxn
                | LogEntryType::NameLN
                | LogEntryType::NameLNTxn
                | LogEntryType::TxnCommit
                | LogEntryType::TxnAbort
                | LogEntryType::Matchpoint
                | LogEntryType::Trace // For testing only
        )
    }

    /// Returns true if this entry type can serve as a replication sync point.
    ///
    /// Sync points contain a replication node ID and can be used to
    /// synchronize the replication stream.
    pub fn is_sync_point(self) -> bool {
        matches!(
            self,
            LogEntryType::TxnCommit
                | LogEntryType::TxnAbort
                | LogEntryType::Matchpoint
        )
    }

    /// Returns true if this entry type must be marshalled inside the log
    /// write latch.
    ///
    /// Most entries can be serialized outside the latch. Exceptions include
    /// entries that update shared metadata (MapLN, FileSummaryLN) or must be
    /// synchronized with VLSN assignment (Commit, Abort).
    pub fn marshall_inside_latch(self) -> bool {
        matches!(
            self,
            LogEntryType::MapLN
                | LogEntryType::FileSummaryLN
                | LogEntryType::TxnCommit
                | LogEntryType::TxnAbort
                | LogEntryType::DbTree
        )
    }

    /// Returns true if this is a Btree node type (IN, BIN, BINDelta).
    pub fn is_node_type(self) -> bool {
        matches!(
            self,
            LogEntryType::IN
                | LogEntryType::BIN
                | LogEntryType::BINDelta
                | LogEntryType::InsertLN
                | LogEntryType::UpdateLN
                | LogEntryType::DeleteLN
                | LogEntryType::InsertLNTxn
                | LogEntryType::UpdateLNTxn
                | LogEntryType::DeleteLNTxn
                | LogEntryType::MapLN
                | LogEntryType::NameLN
                | LogEntryType::NameLNTxn
                | LogEntryType::FileSummaryLN
        )
    }

    /// Returns true if this is a user LN type.
    pub fn is_user_ln_type(self) -> bool {
        matches!(
            self,
            LogEntryType::InsertLN
                | LogEntryType::UpdateLN
                | LogEntryType::DeleteLN
                | LogEntryType::InsertLNTxn
                | LogEntryType::UpdateLNTxn
                | LogEntryType::DeleteLNTxn
        )
    }

    /// Returns true if this is an LN type (any leaf node, user or internal).
    ///
    ///
    pub fn is_ln_type(self) -> bool {
        matches!(
            self,
            LogEntryType::InsertLN
                | LogEntryType::UpdateLN
                | LogEntryType::DeleteLN
                | LogEntryType::InsertLNTxn
                | LogEntryType::UpdateLNTxn
                | LogEntryType::DeleteLNTxn
                | LogEntryType::MapLN
                | LogEntryType::NameLN
                | LogEntryType::NameLNTxn
                | LogEntryType::FileSummaryLN
                | LogEntryType::OldLN
                | LogEntryType::DelDupLN
                | LogEntryType::DupCountLN
        )
    }

    /// Returns true if this is an internal node type (IN levels).
    pub fn is_in_type(self) -> bool {
        matches!(
            self,
            LogEntryType::IN | LogEntryType::BIN | LogEntryType::BINDelta
        )
    }

    /// Returns the display name for this entry type.
    pub fn display_name(self) -> &'static str {
        match self {
            LogEntryType::FileHeader => "FileHeader",
            LogEntryType::IN => "IN",
            LogEntryType::BIN => "BIN",
            LogEntryType::BINDelta => "BINDelta",
            LogEntryType::InsertLN => "INS_LN",
            LogEntryType::UpdateLN => "UPD_LN",
            LogEntryType::DeleteLN => "DEL_LN",
            LogEntryType::InsertLNTxn => "INS_LN_TX",
            LogEntryType::UpdateLNTxn => "UPD_LN_TX",
            LogEntryType::DeleteLNTxn => "DEL_LN_TX",
            LogEntryType::MapLN => "MapLN",
            LogEntryType::NameLN => "NameLN",
            LogEntryType::NameLNTxn => "NameLN_TX",
            LogEntryType::FileSummaryLN => "FileSummaryLN",
            LogEntryType::TxnCommit => "Commit",
            LogEntryType::TxnAbort => "Abort",
            LogEntryType::TxnPrepare => "Prepare",
            LogEntryType::CkptStart => "CkptStart",
            LogEntryType::CkptEnd => "CkptEnd",
            LogEntryType::DbTree => "DbTree",
            LogEntryType::Trace => "Trace",
            LogEntryType::Matchpoint => "Matchpoint",
            LogEntryType::RollbackStart => "RollbackStart",
            LogEntryType::RollbackEnd => "RollbackEnd",
            LogEntryType::INDeleteInfo => "INDeleteInfo",
            LogEntryType::INDupDeleteInfo => "INDupDeleteInfo",
            LogEntryType::OldBINDelta => "OldBINDelta",
            LogEntryType::OldLN => "OldLN",
            LogEntryType::DelDupLN => "DelDupLN",
            LogEntryType::DupCountLN => "DupCountLN",
            LogEntryType::ImmutableFile => "ImmutableFile",
        }
    }
}

impl fmt::Display for LogEntryType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.display_name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_type_num_roundtrip() {
        for type_num in 1u8..128 {
            if let Some(entry_type) = LogEntryType::from_type_num(type_num) {
                assert_eq!(entry_type.type_num(), type_num);
            }
        }
    }

    #[test]
    fn test_is_valid() {
        assert!(LogEntryType::is_valid(1)); // FileHeader
        assert!(LogEntryType::is_valid(30)); // TxnCommit
        assert!(!LogEntryType::is_valid(100));
        assert!(!LogEntryType::is_valid(0));
    }

    #[test]
    fn test_transactional() {
        assert!(LogEntryType::TxnCommit.is_transactional());
        assert!(LogEntryType::InsertLNTxn.is_transactional());
        assert!(!LogEntryType::InsertLN.is_transactional());
        assert!(!LogEntryType::IN.is_transactional());
    }

    #[test]
    fn test_replication() {
        assert!(LogEntryType::InsertLNTxn.is_replication_possible());
        assert!(LogEntryType::TxnCommit.is_replication_possible());
        assert!(!LogEntryType::IN.is_replication_possible());
        assert!(!LogEntryType::BIN.is_replication_possible());
    }

    #[test]
    fn test_sync_point() {
        assert!(LogEntryType::TxnCommit.is_sync_point());
        assert!(LogEntryType::Matchpoint.is_sync_point());
        assert!(!LogEntryType::InsertLNTxn.is_sync_point());
    }

    #[test]
    fn test_node_types() {
        assert!(LogEntryType::BIN.is_in_type());
        assert!(LogEntryType::IN.is_in_type());
        assert!(LogEntryType::InsertLN.is_user_ln_type());
        assert!(!LogEntryType::MapLN.is_user_ln_type());
    }

    #[test]
    fn test_marshall_inside_latch() {
        // These types must be marshalled under the log write latch.
        assert!(LogEntryType::MapLN.marshall_inside_latch());
        assert!(LogEntryType::FileSummaryLN.marshall_inside_latch());
        assert!(LogEntryType::TxnCommit.marshall_inside_latch());
        assert!(LogEntryType::TxnAbort.marshall_inside_latch());
        assert!(LogEntryType::DbTree.marshall_inside_latch());

        // These are outside-latch types.
        assert!(!LogEntryType::BIN.marshall_inside_latch());
        assert!(!LogEntryType::IN.marshall_inside_latch());
        assert!(!LogEntryType::InsertLNTxn.marshall_inside_latch());
        assert!(!LogEntryType::Trace.marshall_inside_latch());
    }

    #[test]
    fn test_display_name_and_display_trait() {
        for type_num in 1u8..128 {
            if let Some(entry_type) = LogEntryType::from_type_num(type_num) {
                let name = entry_type.display_name();
                assert!(!name.is_empty(), "display_name should not be empty");
                // Display trait should produce the same string.
                assert_eq!(format!("{}", entry_type), name);
            }
        }
    }

    #[test]
    fn test_is_node_type_coverage() {
        // All tree-node types.
        for t in [
            LogEntryType::IN,
            LogEntryType::BIN,
            LogEntryType::BINDelta,
            LogEntryType::InsertLN,
            LogEntryType::UpdateLN,
            LogEntryType::DeleteLN,
            LogEntryType::InsertLNTxn,
            LogEntryType::UpdateLNTxn,
            LogEntryType::DeleteLNTxn,
            LogEntryType::MapLN,
            LogEntryType::NameLN,
            LogEntryType::NameLNTxn,
            LogEntryType::FileSummaryLN,
        ] {
            assert!(t.is_node_type(), "{} should be a node type", t);
        }

        // Non-node types.
        for t in [
            LogEntryType::FileHeader,
            LogEntryType::TxnCommit,
            LogEntryType::TxnAbort,
            LogEntryType::TxnPrepare,
            LogEntryType::CkptStart,
            LogEntryType::CkptEnd,
            LogEntryType::DbTree,
            LogEntryType::Trace,
            LogEntryType::Matchpoint,
            LogEntryType::RollbackStart,
            LogEntryType::RollbackEnd,
            LogEntryType::INDeleteInfo,
            LogEntryType::INDupDeleteInfo,
            LogEntryType::OldBINDelta,
            LogEntryType::OldLN,
            LogEntryType::DelDupLN,
            LogEntryType::DupCountLN,
            LogEntryType::ImmutableFile,
        ] {
            assert!(!t.is_node_type(), "{} should not be a node type", t);
        }
    }

    #[test]
    fn test_from_type_num_exhaustive() {
        // Every number 0..=255 either maps to a known type or returns None.
        let mut found = 0usize;
        for n in 0u8..=255 {
            match LogEntryType::from_type_num(n) {
                Some(t) => {
                    // Round-trip must hold.
                    assert_eq!(t.type_num(), n);
                    found += 1;
                }
                None => {
                    assert!(!LogEntryType::is_valid(n));
                }
            }
        }
        // We expect exactly the number of variants defined in the enum.
        assert_eq!(found, 31);
    }
}
