//! Error types for the collections crate.
//!

use thiserror::Error;

/// Errors that can occur when using collection views.
///
#[derive(Debug, Error)]
pub enum CollectionError {
    /// An underlying database error occurred.
    #[error("database error: {0}")]
    DatabaseError(#[from] crate::db::NoxuError),

    /// A binding conversion error occurred.
    #[error("binding error: {0}")]
    BindingError(String),

    /// The iterator has been exhausted.
    #[error("iterator exhausted")]
    IteratorExhausted,

    /// The collection is read-only and a write was attempted.
    #[error("collection is read-only")]
    ReadOnly,

    /// A concurrent modification was detected.
    #[error("concurrent modification detected")]
    ConcurrentModification,

    /// An illegal state was encountered.
    #[error("illegal state: {0}")]
    IllegalState(String),
}

/// Result type for collection operations.
pub type Result<T> = std::result::Result<T, CollectionError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = CollectionError::ReadOnly;
        assert_eq!(err.to_string(), "collection is read-only");
    }

    #[test]
    fn test_database_error_conversion() {
        let db_err = crate::db::NoxuError::DatabaseClosed;
        let err: CollectionError = db_err.into();
        assert!(matches!(err, CollectionError::DatabaseError(_)));
        assert!(err.to_string().contains("database"));
    }

    #[test]
    fn test_binding_error() {
        let err = CollectionError::BindingError("bad conversion".to_string());
        assert!(err.to_string().contains("binding error"));
    }

    #[test]
    fn test_iterator_exhausted() {
        let err = CollectionError::IteratorExhausted;
        assert_eq!(err.to_string(), "iterator exhausted");
    }

    #[test]
    fn test_illegal_state() {
        let err =
            CollectionError::IllegalState("cursor not positioned".to_string());
        assert!(err.to_string().contains("illegal state"));
    }
}
