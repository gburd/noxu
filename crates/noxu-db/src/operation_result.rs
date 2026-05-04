//! Result of database operations.
//!
//! Port of `com.sleepycat.je.OperationResult`.

/// Result of a successful database operation.
///
/// Returned by Database and Cursor methods to provide information about
/// the operation that was performed. Note that not all operations return
/// an OperationResult - some return None on success.
///
/// Port of `com.sleepycat.je.OperationResult`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationResult {
    /// Whether the operation modified the database.
    ///
    /// True if the operation resulted in an insertion, update, or deletion.
    /// False if the operation only read data or had no effect.
    pub is_update: bool,

    /// Expiration time in milliseconds since epoch.
    ///
    /// Zero indicates the record never expires. For records with time-to-live
    /// (TTL) expiration, this is the absolute time when the record will expire.
    pub expiration_time: u64,
}

impl OperationResult {
    /// Creates a new OperationResult.
    pub fn new(is_update: bool, expiration_time: u64) -> Self {
        Self { is_update, expiration_time }
    }

    /// Creates an OperationResult for a read operation (no update).
    pub fn read() -> Self {
        Self { is_update: false, expiration_time: 0 }
    }

    /// Creates an OperationResult for an update operation.
    pub fn update() -> Self {
        Self { is_update: true, expiration_time: 0 }
    }

    /// Creates an OperationResult for a read with expiration time.
    pub fn read_with_expiration(expiration_time: u64) -> Self {
        Self { is_update: false, expiration_time }
    }

    /// Creates an OperationResult for an update with expiration time.
    pub fn update_with_expiration(expiration_time: u64) -> Self {
        Self { is_update: true, expiration_time }
    }

    /// Returns whether the record has an expiration time set.
    pub fn has_expiration(&self) -> bool {
        self.expiration_time > 0
    }

    /// Returns whether the record is expired at the given time.
    pub fn is_expired(&self, current_time: u64) -> bool {
        self.has_expiration() && current_time >= self.expiration_time
    }
}

impl Default for OperationResult {
    fn default() -> Self {
        Self::read()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let result = OperationResult::new(true, 12345);
        assert!(result.is_update);
        assert_eq!(result.expiration_time, 12345);
    }

    #[test]
    fn test_read() {
        let result = OperationResult::read();
        assert!(!result.is_update);
        assert_eq!(result.expiration_time, 0);
    }

    #[test]
    fn test_update() {
        let result = OperationResult::update();
        assert!(result.is_update);
        assert_eq!(result.expiration_time, 0);
    }

    #[test]
    fn test_read_with_expiration() {
        let result = OperationResult::read_with_expiration(1000);
        assert!(!result.is_update);
        assert_eq!(result.expiration_time, 1000);
    }

    #[test]
    fn test_update_with_expiration() {
        let result = OperationResult::update_with_expiration(2000);
        assert!(result.is_update);
        assert_eq!(result.expiration_time, 2000);
    }

    #[test]
    fn test_has_expiration() {
        let no_exp = OperationResult::read();
        let with_exp = OperationResult::read_with_expiration(100);
        assert!(!no_exp.has_expiration());
        assert!(with_exp.has_expiration());
    }

    #[test]
    fn test_is_expired() {
        let result = OperationResult::read_with_expiration(1000);
        assert!(!result.is_expired(500));
        assert!(result.is_expired(1000));
        assert!(result.is_expired(1500));
    }

    #[test]
    fn test_is_expired_no_expiration() {
        let result = OperationResult::read();
        assert!(!result.is_expired(u64::MAX));
    }

    #[test]
    fn test_default() {
        let result = OperationResult::default();
        assert!(!result.is_update);
        assert_eq!(result.expiration_time, 0);
    }

    #[test]
    fn test_clone() {
        let result1 = OperationResult::update_with_expiration(500);
        let result2 = result1.clone();
        assert_eq!(result1, result2);
    }

    #[test]
    fn test_equality() {
        let r1 = OperationResult::read();
        let r2 = OperationResult::read();
        let r3 = OperationResult::update();
        assert_eq!(r1, r2);
        assert_ne!(r1, r3);
    }

    #[test]
    fn test_debug() {
        let result = OperationResult::update_with_expiration(123);
        let debug = format!("{:?}", result);
        assert!(debug.contains("is_update"));
        assert!(debug.contains("expiration_time"));
    }
}
