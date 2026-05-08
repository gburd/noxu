//! Lock modes for read operations.
//!

/// Lock mode for read operations.
///
/// Specifies the locking behavior for a read operation. Controls isolation
/// level and whether locks are acquired.
///
/// 
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum LockMode {
    /// Use the default isolation level for the cursor or transaction.
    ///
    /// For transactional operations, this is typically read-committed.
    /// For non-transactional operations, this is read-uncommitted.
    #[default]
    Default,

    /// Read without acquiring locks (dirty reads).
    ///
    /// Reads may return data that is currently being modified by another
    /// transaction and may be rolled back. Provides maximum concurrency
    /// but minimal isolation.
    ReadUncommitted,

    /// Read with read-committed isolation.
    ///
    /// A read lock is acquired but released when the cursor moves or
    /// the read operation completes. Prevents dirty reads but allows
    /// non-repeatable reads.
    ReadCommitted,

    /// Read-modify-write: acquire write lock on read.
    ///
    /// Acquires a write lock immediately, even though the operation is a read.
    /// Use this when you intend to modify the record after reading it, to
    /// avoid deadlocks caused by lock upgrades.
    Rmw,
}

impl LockMode {
    /// Returns whether this mode allows dirty reads.
    pub fn allows_dirty_reads(&self) -> bool {
        matches!(self, LockMode::ReadUncommitted)
    }

    /// Returns whether this mode acquires a write lock.
    pub fn acquires_write_lock(&self) -> bool {
        matches!(self, LockMode::Rmw)
    }

    /// Returns whether this mode provides read-committed isolation.
    pub fn is_read_committed(&self) -> bool {
        matches!(self, LockMode::ReadCommitted | LockMode::Default)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default() {
        assert_eq!(LockMode::default(), LockMode::Default);
    }

    #[test]
    fn test_allows_dirty_reads() {
        assert!(LockMode::ReadUncommitted.allows_dirty_reads());
        assert!(!LockMode::Default.allows_dirty_reads());
        assert!(!LockMode::ReadCommitted.allows_dirty_reads());
        assert!(!LockMode::Rmw.allows_dirty_reads());
    }

    #[test]
    fn test_acquires_write_lock() {
        assert!(LockMode::Rmw.acquires_write_lock());
        assert!(!LockMode::Default.acquires_write_lock());
        assert!(!LockMode::ReadUncommitted.acquires_write_lock());
        assert!(!LockMode::ReadCommitted.acquires_write_lock());
    }

    #[test]
    fn test_is_read_committed() {
        assert!(LockMode::Default.is_read_committed());
        assert!(LockMode::ReadCommitted.is_read_committed());
        assert!(!LockMode::ReadUncommitted.is_read_committed());
        assert!(!LockMode::Rmw.is_read_committed());
    }

    #[test]
    fn test_equality() {
        assert_eq!(LockMode::Default, LockMode::Default);
        assert_ne!(LockMode::Default, LockMode::Rmw);
    }

    #[test]
    fn test_clone() {
        let mode1 = LockMode::Rmw;
        let mode2 = mode1;
        assert_eq!(mode1, mode2);
    }

    #[test]
    fn test_copy() {
        let mode1 = LockMode::ReadCommitted;
        let mode2 = mode1;
        assert_eq!(mode1, mode2);
    }

    #[test]
    fn test_debug() {
        let mode = LockMode::Rmw;
        let debug = format!("{:?}", mode);
        assert_eq!(debug, "Rmw");
    }
}
