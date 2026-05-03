//! Error types for the cleaner module.

use thiserror::Error;

/// Errors that can occur during log file cleaning operations.
#[derive(Debug, Error)]
pub enum CleanerError {
    /// A cleaning operation failed.
    #[error("cleaning failed: {0}")]
    CleaningFailed(String),

    /// A log file was not found.
    #[error("file not found: {file_number}")]
    FileNotFound {
        /// The missing file number.
        file_number: u32,
    },

    /// An error occurred during utilization tracking.
    #[error("utilization tracking error: {0}")]
    UtilizationError(String),

    /// An I/O error occurred.
    #[error("io error: {0}")]
    IoError(#[from] std::io::Error),
}

/// Result type for cleaner operations.
pub type Result<T> = std::result::Result<T, CleanerError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = CleanerError::CleaningFailed("test error".to_string());
        assert_eq!(err.to_string(), "cleaning failed: test error");
    }

    #[test]
    fn test_file_not_found() {
        let err = CleanerError::FileNotFound { file_number: 42 };
        assert_eq!(err.to_string(), "file not found: 42");
    }

    #[test]
    fn test_utilization_error() {
        let err = CleanerError::UtilizationError("tracking failed".to_string());
        assert_eq!(
            err.to_string(),
            "utilization tracking error: tracking failed"
        );
    }

    #[test]
    fn test_io_error_conversion() {
        let io_err =
            std::io::Error::new(std::io::ErrorKind::NotFound, "file missing");
        let err: CleanerError = io_err.into();
        assert!(matches!(err, CleanerError::IoError(_)));
    }
}
