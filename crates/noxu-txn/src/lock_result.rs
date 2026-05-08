//! Result of a Locker.lock() call.
//!

use crate::{LockGrantType, WriteLockInfo};

/// Result of a Locker.lock() call.
///
/// Encapsulates the grant type and optional write lock info (for write locks).
///
/// 
#[derive(Debug)]
pub struct LockResult {
    /// The type of lock grant that occurred.
    pub grant: LockGrantType,

    /// Write lock undo information (only for write locks).
    pub write_lock_info: Option<WriteLockInfo>,
}

impl LockResult {
    /// Creates a new LockResult.
    pub fn new(
        grant: LockGrantType,
        write_lock_info: Option<WriteLockInfo>,
    ) -> Self {
        Self { grant, write_lock_info }
    }

    /// Creates a LockResult with no write lock info.
    pub fn simple(grant: LockGrantType) -> Self {
        Self { grant, write_lock_info: None }
    }

    /// Sets the abort info in the write lock info, if present.
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
        if let Some(ref mut info) = self.write_lock_info {
            info.set_abort_info(
                abort_lsn,
                abort_key,
                abort_data,
                abort_vlsn,
                abort_log_size,
                abort_known_deleted,
                abort_expiration,
                abort_expiration_in_hours,
            );
        }
    }

    /// Copies write lock info from another WriteLockInfo.
    ///
    /// 
    pub fn copy_write_lock_info(&mut self, from: &WriteLockInfo) {
        if let Some(ref mut info) = self.write_lock_info {
            info.copy_all_info(from);
        }
    }

    /// Returns true if this result contains write lock info.
    pub fn has_write_lock_info(&self) -> bool {
        self.write_lock_info.is_some()
    }

    /// Returns true if the lock was granted (NEW, PROMOTION, or EXISTING).
    ///
    /// 
    pub fn is_granted(&self) -> bool {
        matches!(
            self.grant,
            LockGrantType::New
                | LockGrantType::Promotion
                | LockGrantType::Existing
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let info = WriteLockInfo::new();
        let result = LockResult::new(LockGrantType::New, Some(info));
        assert_eq!(result.grant, LockGrantType::New);
        assert!(result.has_write_lock_info());
    }

    #[test]
    fn test_simple() {
        let result = LockResult::simple(LockGrantType::Existing);
        assert_eq!(result.grant, LockGrantType::Existing);
        assert!(!result.has_write_lock_info());
    }

    #[test]
    fn test_set_abort_info() {
        let mut result = LockResult::new(
            LockGrantType::Promotion,
            Some(WriteLockInfo::new()),
        );
        result.set_abort_info(
            12345,
            Some(vec![1, 2, 3]),
            Some(vec![4, 5, 6]),
            100,
            42,
            true,
            3600,
            false,
        );

        let info = result.write_lock_info.as_ref().unwrap();
        assert_eq!(info.abort_lsn, 12345);
        assert_eq!(info.abort_key, Some(vec![1, 2, 3]));
        assert_eq!(info.abort_data, Some(vec![4, 5, 6]));
        assert_eq!(info.abort_vlsn, 100);
        assert_eq!(info.abort_log_size, 42);
        assert!(info.abort_known_deleted);
        assert_eq!(info.abort_expiration, 3600);
        assert!(!info.abort_expiration_in_hours);
    }

    #[test]
    fn test_set_abort_info_none() {
        let mut result = LockResult::simple(LockGrantType::New);
        // Should not panic
        result.set_abort_info(12345, None, None, 100, 42, false, 0, false);
    }

    #[test]
    fn test_copy_write_lock_info() {
        let mut source = WriteLockInfo::new();
        source.abort_lsn = 99999;
        source.abort_known_deleted = true;
        source.abort_vlsn = 200;

        let mut result =
            LockResult::new(LockGrantType::New, Some(WriteLockInfo::new()));
        result.copy_write_lock_info(&source);

        let info = result.write_lock_info.as_ref().unwrap();
        assert_eq!(info.abort_lsn, 99999);
        assert!(info.abort_known_deleted);
        assert_eq!(info.abort_vlsn, 200);
    }

    #[test]
    fn test_copy_write_lock_info_none() {
        let source = WriteLockInfo::new();
        let mut result = LockResult::simple(LockGrantType::New);
        // Should not panic
        result.copy_write_lock_info(&source);
    }
}
