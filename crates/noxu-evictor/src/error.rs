//! Error types for the evictor.
//!
//! Port of error handling from `com.sleepycat.je.evictor`.

use thiserror::Error;

/// Errors that can occur during eviction operations.
///
/// Port of exception handling from JE's evictor package.
#[derive(Debug, Error)]
pub enum EvictorError {
    /// Eviction operation failed.
    #[error("eviction failed: {0}")]
    EvictionFailed(String),

    /// Cache usage exceeds the configured budget.
    #[error("cache overflow: usage {usage} > budget {budget}")]
    CacheOverflow {
        /// Current cache usage in bytes.
        usage: i64,
        /// Maximum allowed cache budget in bytes.
        budget: i64,
    },

    /// Node cannot be evicted because it is pinned or has dependencies.
    #[error("cannot evict node {node_id}: {reason}")]
    CannotEvict {
        /// ID of the node that cannot be evicted.
        node_id: u64,
        /// Reason why the node cannot be evicted.
        reason: String,
    },

    /// Invalid eviction state.
    #[error("invalid eviction state: {0}")]
    InvalidState(String),
}

/// Result type for eviction operations.
pub type Result<T> = std::result::Result<T, EvictorError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_eviction_failed_error() {
        let err = EvictorError::EvictionFailed("test reason".to_string());
        assert_eq!(err.to_string(), "eviction failed: test reason");
    }

    #[test]
    fn test_cache_overflow_error() {
        let err = EvictorError::CacheOverflow { usage: 1000, budget: 800 };
        assert_eq!(err.to_string(), "cache overflow: usage 1000 > budget 800");
    }

    #[test]
    fn test_cannot_evict_error() {
        let err = EvictorError::CannotEvict {
            node_id: 42,
            reason: "node is dirty".to_string(),
        };
        assert_eq!(err.to_string(), "cannot evict node 42: node is dirty");
    }

    #[test]
    fn test_invalid_state_error() {
        let err =
            EvictorError::InvalidState("unexpected condition".to_string());
        assert_eq!(
            err.to_string(),
            "invalid eviction state: unexpected condition"
        );
    }

    #[test]
    fn test_result_type() {
        let ok_result: Result<i32> = Ok(42);
        assert_eq!(ok_result.unwrap(), 42);

        let err_result: Result<i32> =
            Err(EvictorError::EvictionFailed("test".to_string()));
        assert!(err_result.is_err());
    }
}
