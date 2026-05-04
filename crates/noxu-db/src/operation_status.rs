//! Operation status for database operations.
//!
//! Shared between database and cursor modules.

/// Operation status returned by database and cursor operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationStatus {
    /// Operation succeeded.
    Success,
    /// Record not found.
    NotFound,
    /// Key already exists (for NoOverwrite operations).
    KeyExists,
}
