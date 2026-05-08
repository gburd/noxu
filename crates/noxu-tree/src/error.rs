//! Error types for the noxu-tree crate.
//!

use thiserror::Error;

/// Errors that can occur during tree operations.
#[derive(Error, Debug)]
pub enum TreeError {
    /// Attempted to delete/remove a non-empty node.
    ///
    /// .
    #[error("Node is not empty and cannot be deleted")]
    NodeNotEmpty,

    /// Attempted to remove a node that still has cursors positioned on it.
    ///
    /// .
    #[error("Cursors exist on node and it cannot be removed")]
    CursorsExist,

    /// Attempted an operation that requires a node split.
    ///
    /// .
    #[error("Node split is required")]
    SplitRequired,

    /// Key was not found in the tree.
    #[error("Key not found")]
    KeyNotFound,

    /// Invalid tree level specified.
    #[error("Invalid tree level: {level}")]
    InvalidLevel { level: i32 },

    /// I/O error occurred.
    #[error("I/O error: {0}")]
    Io(String),

    /// Log error occurred.
    #[error("Log error: {0}")]
    LogError(String),
}

/// Type alias for tree operation results.
pub type TreeResult<T> = Result<T, TreeError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = TreeError::NodeNotEmpty;
        assert_eq!(err.to_string(), "Node is not empty and cannot be deleted");

        let err = TreeError::CursorsExist;
        assert_eq!(
            err.to_string(),
            "Cursors exist on node and it cannot be removed"
        );

        let err = TreeError::InvalidLevel { level: -5 };
        assert_eq!(err.to_string(), "Invalid tree level: -5");
    }

    #[test]
    fn test_error_format() {
        let err = TreeError::KeyNotFound;
        assert_eq!(err.to_string(), "Key not found");
    }

    #[test]
    fn test_error_variants() {
        let errors = vec![
            TreeError::NodeNotEmpty,
            TreeError::CursorsExist,
            TreeError::SplitRequired,
            TreeError::KeyNotFound,
            TreeError::InvalidLevel { level: 42 },
        ];

        // All should be displayable
        for err in errors {
            assert!(!err.to_string().is_empty());
        }
    }

    #[test]
    fn test_io_error_conversion() {
        let tree_err = TreeError::Io("file not found".to_string());
        assert!(tree_err.to_string().contains("file not found"));
    }
}
