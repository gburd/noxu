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
    /// The record at the cursor position was deleted by another operation
    /// while the cursor was positioned on it.  JE: `OperationStatus.KEYEMPTY`.
    /// Returned by `putCurrent` / `delete` when the current slot is defunct.
    KeyEmpty,
}
