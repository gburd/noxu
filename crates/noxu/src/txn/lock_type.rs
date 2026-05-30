//! Lock type definitions and conflict/upgrade matrices.
//!

use crate::txn::{LockConflict, LockUpgrade};

/// Lock types used in the transaction system.
///
/// Noxu DB uses hierarchical locking with five primary lock types.
/// The conflict and upgrade matrices define how locks interact.
///
///
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum LockType {
    /// Basic read lock on a record.
    ///
    /// Allows multiple concurrent readers but blocks writers.
    Read,

    /// Write lock on a record (exclusive).
    ///
    /// Blocks all other lockers from reading or writing this record.
    Write,

    /// Range read lock  -  locks a range to prevent phantom inserts.
    ///
    /// Similar to Read but also prevents inserts into the range.
    RangeRead,

    /// Range write lock  -  combines range and write locking.
    ///
    /// Blocks all access and prevents inserts into the range.
    RangeWrite,

    /// Range insert lock  -  used when inserting into a range.
    ///
    /// Conflicts with range locks to trigger restart on phantom detection.
    RangeInsert,

    /// No lock requested (dirty read).
    ///
    /// Special type indicating no locking should be performed.
    None,

    /// Restart marker  -  not a real lock type.
    ///
    /// Used internally to indicate a range restart is needed.
    Restart,
}

impl LockType {
    /// Returns true if this is a write lock that modifies data.
    ///
    /// Note: Per implementation, RangeInsert is NOT considered a write lock
    /// for the purposes of transaction commit/abort. Only Write and RangeWrite
    /// require undo information.
    #[inline]
    pub fn is_write_lock(self) -> bool {
        matches!(self, LockType::Write | LockType::RangeWrite)
    }

    /// Returns true if this lock type causes a restart when upgraded.
    ///
    /// RangeRead and RangeWrite cause restarts in certain upgrade scenarios.
    #[inline]
    pub fn causes_restart(self) -> bool {
        matches!(self, LockType::RangeRead | LockType::RangeWrite)
    }

    /// Returns the conflict status between a held lock and a requested lock.
    ///
    /// This implements the 5x5 conflict matrix from the:
    /// ```text
    ///              READ    WRITE   RANGE_R  RANGE_W  RANGE_I
    /// READ        ALLOW   BLOCK   ALLOW    BLOCK    ALLOW
    /// WRITE       BLOCK   BLOCK   BLOCK    BLOCK    ALLOW
    /// RANGE_READ  ALLOW   BLOCK   ALLOW    BLOCK    BLOCK
    /// RANGE_WRITE BLOCK   BLOCK   BLOCK    BLOCK    BLOCK
    /// RANGE_INS   ALLOW   ALLOW   RESTART  RESTART  ALLOW
    /// ```
    pub fn get_conflict(self, requested: LockType) -> LockConflict {
        use LockConflict::*;
        use LockType::*;

        match (self, requested) {
            // READ row
            (Read, Read) => Allow,
            (Read, Write) => Block,
            (Read, RangeRead) => Allow,
            (Read, RangeWrite) => Block,
            (Read, RangeInsert) => Allow,

            // WRITE row
            (Write, Read) => Block,
            (Write, Write) => Block,
            (Write, RangeRead) => Block,
            (Write, RangeWrite) => Block,
            (Write, RangeInsert) => Allow,

            // RANGE_READ row
            (RangeRead, Read) => Allow,
            (RangeRead, Write) => Block,
            (RangeRead, RangeRead) => Allow,
            (RangeRead, RangeWrite) => Block,
            (RangeRead, RangeInsert) => Block,

            // RANGE_WRITE row
            (RangeWrite, Read) => Block,
            (RangeWrite, Write) => Block,
            (RangeWrite, RangeRead) => Block,
            (RangeWrite, RangeWrite) => Block,
            (RangeWrite, RangeInsert) => Block,

            // RANGE_INSERT row
            (RangeInsert, Read) => Allow,
            (RangeInsert, Write) => Allow,
            (RangeInsert, RangeRead) => LockConflict::Restart,
            (RangeInsert, RangeWrite) => LockConflict::Restart,
            (RangeInsert, RangeInsert) => Allow,

            // None and Restart are not used in conflict checking
            _ => Allow,
        }
    }

    /// Returns the upgrade result when a locker holds this lock and requests another.
    ///
    /// This implements the 5x5 upgrade matrix from the:
    /// ```text
    ///              READ           WRITE              RANGE_R            RANGE_W              RANGE_I
    /// READ        EXISTING       WRITE_PROMOTE      RANGE_READ_IMMED   RANGE_WRITE_PROMOTE  ILLEGAL
    /// WRITE       EXISTING       EXISTING           RANGE_WRITE_IMMED  RANGE_WRITE_IMMED    ILLEGAL
    /// RANGE_READ  EXISTING       RANGE_WRITE_PROM   EXISTING           RANGE_WRITE_PROMOTE  ILLEGAL
    /// RANGE_WRITE EXISTING       EXISTING           EXISTING           EXISTING             ILLEGAL
    /// RANGE_INS   ILLEGAL        ILLEGAL            ILLEGAL            ILLEGAL              EXISTING
    /// ```
    pub fn get_upgrade(self, requested: LockType) -> LockUpgrade {
        use LockType::*;
        use LockUpgrade::*;

        match (self, requested) {
            // READ row
            (Read, Read) => Existing,
            (Read, Write) => WritePromote,
            (Read, RangeRead) => RangeReadImmed,
            (Read, RangeWrite) => RangeWritePromote,
            (Read, RangeInsert) => Illegal,

            // WRITE row
            (Write, Read) => Existing,
            (Write, Write) => Existing,
            (Write, RangeRead) => RangeWriteImmed,
            (Write, RangeWrite) => RangeWriteImmed,
            (Write, RangeInsert) => Illegal,

            // RANGE_READ row
            (RangeRead, Read) => Existing,
            (RangeRead, Write) => RangeWritePromote,
            (RangeRead, RangeRead) => Existing,
            (RangeRead, RangeWrite) => RangeWritePromote,
            (RangeRead, RangeInsert) => Illegal,

            // RANGE_WRITE row
            (RangeWrite, Read) => Existing,
            (RangeWrite, Write) => Existing,
            (RangeWrite, RangeRead) => Existing,
            (RangeWrite, RangeWrite) => Existing,
            (RangeWrite, RangeInsert) => Illegal,

            // RANGE_INSERT row
            (RangeInsert, Read) => Illegal,
            (RangeInsert, Write) => Illegal,
            (RangeInsert, RangeRead) => Illegal,
            (RangeInsert, RangeWrite) => Illegal,
            (RangeInsert, RangeInsert) => Existing,

            // None and Restart upgrades
            (None, _) => Illegal,
            (_, None) => Existing,
            (Restart, _) => Illegal,
            (_, Restart) => Illegal,
        }
    }

    /// Returns the array index for this lock type (0-4 for the 5 main types).
    ///
    /// Used for indexing into statistics arrays.
    #[inline]
    pub fn index(self) -> usize {
        match self {
            LockType::Read => 0,
            LockType::Write => 1,
            LockType::RangeRead => 2,
            LockType::RangeWrite => 3,
            LockType::RangeInsert => 4,
            LockType::None => 5,
            LockType::Restart => 6,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_write_lock() {
        assert!(!LockType::Read.is_write_lock());
        assert!(LockType::Write.is_write_lock());
        assert!(!LockType::RangeRead.is_write_lock());
        assert!(LockType::RangeWrite.is_write_lock());
        assert!(!LockType::RangeInsert.is_write_lock()); // Important: NOT a write lock per 
        assert!(!LockType::None.is_write_lock());
        assert!(!LockType::Restart.is_write_lock());
    }

    #[test]
    fn test_causes_restart() {
        assert!(!LockType::Read.causes_restart());
        assert!(!LockType::Write.causes_restart());
        assert!(LockType::RangeRead.causes_restart());
        assert!(LockType::RangeWrite.causes_restart());
        assert!(!LockType::RangeInsert.causes_restart());
        assert!(!LockType::None.causes_restart());
        assert!(!LockType::Restart.causes_restart());
    }

    #[test]
    fn test_index() {
        assert_eq!(LockType::Read.index(), 0);
        assert_eq!(LockType::Write.index(), 1);
        assert_eq!(LockType::RangeRead.index(), 2);
        assert_eq!(LockType::RangeWrite.index(), 3);
        assert_eq!(LockType::RangeInsert.index(), 4);
        assert_eq!(LockType::None.index(), 5);
        assert_eq!(LockType::Restart.index(), 6);
    }

    // Test all 25 entries of the conflict matrix
    #[test]
    fn test_conflict_matrix_read_row() {
        assert_eq!(
            LockType::Read.get_conflict(LockType::Read),
            LockConflict::Allow
        );
        assert_eq!(
            LockType::Read.get_conflict(LockType::Write),
            LockConflict::Block
        );
        assert_eq!(
            LockType::Read.get_conflict(LockType::RangeRead),
            LockConflict::Allow
        );
        assert_eq!(
            LockType::Read.get_conflict(LockType::RangeWrite),
            LockConflict::Block
        );
        assert_eq!(
            LockType::Read.get_conflict(LockType::RangeInsert),
            LockConflict::Allow
        );
    }

    #[test]
    fn test_conflict_matrix_write_row() {
        assert_eq!(
            LockType::Write.get_conflict(LockType::Read),
            LockConflict::Block
        );
        assert_eq!(
            LockType::Write.get_conflict(LockType::Write),
            LockConflict::Block
        );
        assert_eq!(
            LockType::Write.get_conflict(LockType::RangeRead),
            LockConflict::Block
        );
        assert_eq!(
            LockType::Write.get_conflict(LockType::RangeWrite),
            LockConflict::Block
        );
        assert_eq!(
            LockType::Write.get_conflict(LockType::RangeInsert),
            LockConflict::Allow
        );
    }

    #[test]
    fn test_conflict_matrix_range_read_row() {
        assert_eq!(
            LockType::RangeRead.get_conflict(LockType::Read),
            LockConflict::Allow
        );
        assert_eq!(
            LockType::RangeRead.get_conflict(LockType::Write),
            LockConflict::Block
        );
        assert_eq!(
            LockType::RangeRead.get_conflict(LockType::RangeRead),
            LockConflict::Allow
        );
        assert_eq!(
            LockType::RangeRead.get_conflict(LockType::RangeWrite),
            LockConflict::Block
        );
        assert_eq!(
            LockType::RangeRead.get_conflict(LockType::RangeInsert),
            LockConflict::Block
        );
    }

    #[test]
    fn test_conflict_matrix_range_write_row() {
        assert_eq!(
            LockType::RangeWrite.get_conflict(LockType::Read),
            LockConflict::Block
        );
        assert_eq!(
            LockType::RangeWrite.get_conflict(LockType::Write),
            LockConflict::Block
        );
        assert_eq!(
            LockType::RangeWrite.get_conflict(LockType::RangeRead),
            LockConflict::Block
        );
        assert_eq!(
            LockType::RangeWrite.get_conflict(LockType::RangeWrite),
            LockConflict::Block
        );
        assert_eq!(
            LockType::RangeWrite.get_conflict(LockType::RangeInsert),
            LockConflict::Block
        );
    }

    #[test]
    fn test_conflict_matrix_range_insert_row() {
        assert_eq!(
            LockType::RangeInsert.get_conflict(LockType::Read),
            LockConflict::Allow
        );
        assert_eq!(
            LockType::RangeInsert.get_conflict(LockType::Write),
            LockConflict::Allow
        );
        assert_eq!(
            LockType::RangeInsert.get_conflict(LockType::RangeRead),
            LockConflict::Restart
        );
        assert_eq!(
            LockType::RangeInsert.get_conflict(LockType::RangeWrite),
            LockConflict::Restart
        );
        assert_eq!(
            LockType::RangeInsert.get_conflict(LockType::RangeInsert),
            LockConflict::Allow
        );
    }

    // Test all 25 entries of the upgrade matrix
    #[test]
    fn test_upgrade_matrix_read_row() {
        assert_eq!(
            LockType::Read.get_upgrade(LockType::Read),
            LockUpgrade::Existing
        );
        assert_eq!(
            LockType::Read.get_upgrade(LockType::Write),
            LockUpgrade::WritePromote
        );
        assert_eq!(
            LockType::Read.get_upgrade(LockType::RangeRead),
            LockUpgrade::RangeReadImmed
        );
        assert_eq!(
            LockType::Read.get_upgrade(LockType::RangeWrite),
            LockUpgrade::RangeWritePromote
        );
        assert_eq!(
            LockType::Read.get_upgrade(LockType::RangeInsert),
            LockUpgrade::Illegal
        );
    }

    #[test]
    fn test_upgrade_matrix_write_row() {
        assert_eq!(
            LockType::Write.get_upgrade(LockType::Read),
            LockUpgrade::Existing
        );
        assert_eq!(
            LockType::Write.get_upgrade(LockType::Write),
            LockUpgrade::Existing
        );
        assert_eq!(
            LockType::Write.get_upgrade(LockType::RangeRead),
            LockUpgrade::RangeWriteImmed
        );
        assert_eq!(
            LockType::Write.get_upgrade(LockType::RangeWrite),
            LockUpgrade::RangeWriteImmed
        );
        assert_eq!(
            LockType::Write.get_upgrade(LockType::RangeInsert),
            LockUpgrade::Illegal
        );
    }

    #[test]
    fn test_upgrade_matrix_range_read_row() {
        assert_eq!(
            LockType::RangeRead.get_upgrade(LockType::Read),
            LockUpgrade::Existing
        );
        assert_eq!(
            LockType::RangeRead.get_upgrade(LockType::Write),
            LockUpgrade::RangeWritePromote
        );
        assert_eq!(
            LockType::RangeRead.get_upgrade(LockType::RangeRead),
            LockUpgrade::Existing
        );
        assert_eq!(
            LockType::RangeRead.get_upgrade(LockType::RangeWrite),
            LockUpgrade::RangeWritePromote
        );
        assert_eq!(
            LockType::RangeRead.get_upgrade(LockType::RangeInsert),
            LockUpgrade::Illegal
        );
    }

    #[test]
    fn test_upgrade_matrix_range_write_row() {
        assert_eq!(
            LockType::RangeWrite.get_upgrade(LockType::Read),
            LockUpgrade::Existing
        );
        assert_eq!(
            LockType::RangeWrite.get_upgrade(LockType::Write),
            LockUpgrade::Existing
        );
        assert_eq!(
            LockType::RangeWrite.get_upgrade(LockType::RangeRead),
            LockUpgrade::Existing
        );
        assert_eq!(
            LockType::RangeWrite.get_upgrade(LockType::RangeWrite),
            LockUpgrade::Existing
        );
        assert_eq!(
            LockType::RangeWrite.get_upgrade(LockType::RangeInsert),
            LockUpgrade::Illegal
        );
    }

    #[test]
    fn test_upgrade_matrix_range_insert_row() {
        assert_eq!(
            LockType::RangeInsert.get_upgrade(LockType::Read),
            LockUpgrade::Illegal
        );
        assert_eq!(
            LockType::RangeInsert.get_upgrade(LockType::Write),
            LockUpgrade::Illegal
        );
        assert_eq!(
            LockType::RangeInsert.get_upgrade(LockType::RangeRead),
            LockUpgrade::Illegal
        );
        assert_eq!(
            LockType::RangeInsert.get_upgrade(LockType::RangeWrite),
            LockUpgrade::Illegal
        );
        assert_eq!(
            LockType::RangeInsert.get_upgrade(LockType::RangeInsert),
            LockUpgrade::Existing
        );
    }

    #[test]
    fn test_upgrade_matrix_none() {
        // None as held lock is illegal
        assert_eq!(
            LockType::None.get_upgrade(LockType::Read),
            LockUpgrade::Illegal
        );
        assert_eq!(
            LockType::None.get_upgrade(LockType::Write),
            LockUpgrade::Illegal
        );

        // None as requested lock is always existing (no upgrade needed)
        assert_eq!(
            LockType::Read.get_upgrade(LockType::None),
            LockUpgrade::Existing
        );
        assert_eq!(
            LockType::Write.get_upgrade(LockType::None),
            LockUpgrade::Existing
        );
    }
}
