//! Error types for the persistence layer.
//!

use thiserror::Error;

/// Errors that can occur in the persistence layer.
///
#[derive(Debug, Error)]
pub enum PersistError {
    /// An error from the underlying database layer.
    #[error("database error: {0}")]
    DatabaseError(#[from] noxu_db::NoxuError),

    /// The requested entity was not found.
    #[error("entity not found")]
    EntityNotFound,

    /// A duplicate primary key was detected during insert.
    #[error("duplicate primary key")]
    DuplicateKey,

    /// An error occurred during serialization or deserialization.
    #[error("serialization error: {0}")]
    SerializationError(String),

    /// The entity store is not open.
    #[error("store not open")]
    StoreNotOpen,

    /// The entity store is already open.
    #[error("store already open")]
    StoreAlreadyOpen,

    /// An entity failed validation.
    #[error("invalid entity: {0}")]
    InvalidEntity(String),

    /// The requested index is not available.
    #[error("index not available: {0}")]
    IndexNotAvailable(String),

    /// A primary write was performed inside a user transaction while the
    /// `PrimaryIndex` had registered (in-memory) secondary indexes.
    ///
    /// In v1.5 DPL secondary indexes are in-memory only and their updates
    /// are NOT atomic with the surrounding transaction: secondary mutations
    /// are applied immediately on the primary `put` / `delete_with_entity`
    /// call regardless of whether the user later commits or aborts the
    /// transaction.  This variant is **not** returned from those methods —
    /// they continue to succeed — but it is constructed and emitted as a
    /// `log::warn!` (one-shot per `PrimaryIndex`) so operators have a
    /// machine-greppable signal that the limitation applies to them.
    ///
    /// Tracking issue: persistent secondaries are scoped for v1.6.
    #[error(
        "DPL secondary indexes are in-memory only in v1.5; secondary \
         updates are not atomic with the user transaction (see \
         docs/src/collections/entity-persistence.md, v1.5 limitations)"
    )]
    SecondariesNotTransactional,
}

/// Result type for persistence layer operations.
pub type Result<T> = std::result::Result<T, PersistError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_entity_not_found_display() {
        let err = PersistError::EntityNotFound;
        assert_eq!(err.to_string(), "entity not found");
    }

    #[test]
    fn test_duplicate_key_display() {
        let err = PersistError::DuplicateKey;
        assert_eq!(err.to_string(), "duplicate primary key");
    }

    #[test]
    fn test_serialization_error_display() {
        let err = PersistError::SerializationError("bad format".to_string());
        assert_eq!(err.to_string(), "serialization error: bad format");
    }

    #[test]
    fn test_store_not_open_display() {
        let err = PersistError::StoreNotOpen;
        assert_eq!(err.to_string(), "store not open");
    }

    #[test]
    fn test_store_already_open_display() {
        let err = PersistError::StoreAlreadyOpen;
        assert_eq!(err.to_string(), "store already open");
    }

    #[test]
    fn test_invalid_entity_display() {
        let err = PersistError::InvalidEntity("missing key".to_string());
        assert_eq!(err.to_string(), "invalid entity: missing key");
    }

    #[test]
    fn test_index_not_available_display() {
        let err = PersistError::IndexNotAvailable("email_idx".to_string());
        assert_eq!(err.to_string(), "index not available: email_idx");
    }

    #[test]
    fn test_secondaries_not_transactional_display() {
        let err = PersistError::SecondariesNotTransactional;
        let msg = err.to_string();
        assert!(msg.contains("secondary indexes"));
        assert!(msg.contains("in-memory"));
    }

    #[test]
    fn test_database_error_from() {
        let db_err = noxu_db::NoxuError::DatabaseClosed;
        let err: PersistError = db_err.into();
        assert!(matches!(err, PersistError::DatabaseError(_)));
        assert!(err.to_string().contains("database error"));
    }
}
