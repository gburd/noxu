//! Undo information for write locks.
//!

/// Information needed to undo write operations if a transaction aborts.
///
/// Stores the "abort version"  -  the state of a record before this txn modified it.
/// This allows the transaction to restore the previous state on abort.
///
///
#[derive(Debug, Clone)]
pub struct WriteLockInfo {
    /// LSN of the record's abort version.
    pub abort_lsn: u64,

    /// Whether the abort version is a known-deleted record.
    pub abort_known_deleted: bool,

    /// Key of the abort version (if key updates allowed).
    pub abort_key: Option<Vec<u8>>,

    /// Data of the abort version (if embedded in BIN).
    pub abort_data: Option<Vec<u8>>,

    /// VLSN of the abort version.
    pub abort_vlsn: i64,

    /// On-disk size of the abort version.
    pub abort_log_size: i32,

    /// Expiration time of the abort version.
    pub abort_expiration: i32,

    /// Whether expiration is in hours (true) or days (false).
    pub abort_expiration_in_hours: bool,

    /// True if the LSN has never been locked before by this Txn.
    ///
    /// Per the: "True if this locker has never had this LSN locked, is false otherwise.
    /// This is used to determine if the locker must add undo information for a write lock."
    pub never_locked: bool,

    /// Database ID of the database that was modified.
    ///
    /// Stored so that `Txn::abort()` can route each `UndoRecord` to the
    /// correct database's B-tree.
    pub database_id: u64,
}

impl WriteLockInfo {
    /// Creates a new WriteLockInfo with default abort version values.
    pub fn new() -> Self {
        Self {
            abort_lsn: noxu_util::NULL_LSN.as_u64(),
            abort_known_deleted: false,
            abort_key: None,
            abort_data: None,
            abort_vlsn: -1,
            abort_log_size: 0,
            abort_expiration: 0,
            abort_expiration_in_hours: false,
            never_locked: true,
            database_id: 0,
        }
    }

    /// Copies all abort information from another WriteLockInfo.
    ///
    ///
    pub fn copy_all_info(&mut self, from: &WriteLockInfo) {
        self.abort_lsn = from.abort_lsn;
        self.abort_known_deleted = from.abort_known_deleted;
        self.abort_key = from.abort_key.clone();
        self.abort_data = from.abort_data.clone();
        self.abort_vlsn = from.abort_vlsn;
        self.abort_log_size = from.abort_log_size;
        self.abort_expiration = from.abort_expiration;
        self.abort_expiration_in_hours = from.abort_expiration_in_hours;
        self.never_locked = from.never_locked;
        self.database_id = from.database_id;
    }

    /// Sets the abort information from a log entry.
    ///
    ///
    pub fn set_abort_info(
        &mut self,
        abort_lsn: u64,
        abort_key: Option<Vec<u8>>,
        abort_data: Option<Vec<u8>>,
        abort_vlsn: i64,
        abort_log_size: i32,
        abort_known_deleted: bool,
        abort_expiration: i32,
        abort_expiration_in_hours: bool,
    ) {
        self.abort_lsn = abort_lsn;
        self.abort_key = abort_key;
        self.abort_data = abort_data;
        self.abort_vlsn = abort_vlsn;
        self.abort_log_size = abort_log_size;
        self.abort_known_deleted = abort_known_deleted;
        self.abort_expiration = abort_expiration;
        self.abort_expiration_in_hours = abort_expiration_in_hours;
    }

    /// Returns true if this represents a NULL abort LSN.
    pub fn is_null_abort_lsn(&self) -> bool {
        self.abort_lsn == noxu_util::NULL_LSN.as_u64()
    }
}

impl Default for WriteLockInfo {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let info = WriteLockInfo::new();
        assert_eq!(info.abort_lsn, noxu_util::NULL_LSN.as_u64());
        assert!(!info.abort_known_deleted);
        assert!(info.abort_key.is_none());
        assert!(info.abort_data.is_none());
        assert_eq!(info.abort_vlsn, -1);
        assert_eq!(info.abort_log_size, 0);
        assert_eq!(info.abort_expiration, 0);
        assert!(!info.abort_expiration_in_hours);
        assert!(info.never_locked);
    }

    #[test]
    fn test_copy_all_info() {
        let mut source = WriteLockInfo::new();
        source.abort_lsn = 12345;
        source.abort_known_deleted = true;
        source.abort_key = Some(vec![1, 2, 3]);
        source.abort_data = Some(vec![4, 5, 6]);
        source.abort_vlsn = 100;
        source.abort_log_size = 42;
        source.abort_expiration = 86400;
        source.abort_expiration_in_hours = true;
        source.never_locked = false;

        let mut dest = WriteLockInfo::new();
        dest.copy_all_info(&source);

        assert_eq!(dest.abort_lsn, 12345);
        assert!(dest.abort_known_deleted);
        assert_eq!(dest.abort_key, Some(vec![1, 2, 3]));
        assert_eq!(dest.abort_data, Some(vec![4, 5, 6]));
        assert_eq!(dest.abort_vlsn, 100);
        assert_eq!(dest.abort_log_size, 42);
        assert_eq!(dest.abort_expiration, 86400);
        assert!(dest.abort_expiration_in_hours);
        assert!(!dest.never_locked);
    }

    #[test]
    fn test_set_abort_info() {
        let mut info = WriteLockInfo::new();
        info.set_abort_info(
            99999,
            Some(vec![10, 20]),
            Some(vec![30, 40]),
            200,
            128,
            true,
            3600,
            false,
        );

        assert_eq!(info.abort_lsn, 99999);
        assert_eq!(info.abort_key, Some(vec![10, 20]));
        assert_eq!(info.abort_data, Some(vec![30, 40]));
        assert_eq!(info.abort_vlsn, 200);
        assert_eq!(info.abort_log_size, 128);
        assert!(info.abort_known_deleted);
        assert_eq!(info.abort_expiration, 3600);
        assert!(!info.abort_expiration_in_hours);
    }

    #[test]
    fn test_is_null_abort_lsn() {
        let info = WriteLockInfo::new();
        assert!(info.is_null_abort_lsn());

        let mut info2 = WriteLockInfo::new();
        info2.abort_lsn = 12345;
        assert!(!info2.is_null_abort_lsn());
    }
}
