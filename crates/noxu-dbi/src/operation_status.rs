//! Operation result status.
//!

/// Status returned by cursor and database operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationStatus {
    /// Operation succeeded.
    Success,
    /// Key/data pair not found.
    NotFound,
    /// Key already exists (for no-overwrite operations).
    KeyExist,
    /// Record at cursor position was deleted.
    KeyEmpty,
}

impl OperationStatus {
    /// Returns true if the operation succeeded.
    pub fn is_success(&self) -> bool {
        *self == OperationStatus::Success
    }
}

impl std::fmt::Display for OperationStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let msg = match self {
            OperationStatus::Success => "SUCCESS",
            OperationStatus::NotFound => "NOTFOUND",
            OperationStatus::KeyExist => "KEYEXIST",
            OperationStatus::KeyEmpty => "KEYEMPTY",
        };
        write!(f, "{}", msg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_equality() {
        assert_eq!(OperationStatus::Success, OperationStatus::Success);
        assert_ne!(OperationStatus::Success, OperationStatus::NotFound);
    }

    #[test]
    fn test_is_success() {
        assert!(OperationStatus::Success.is_success());
        assert!(!OperationStatus::NotFound.is_success());
        assert!(!OperationStatus::KeyExist.is_success());
        assert!(!OperationStatus::KeyEmpty.is_success());
    }

    #[test]
    fn test_display() {
        assert_eq!(OperationStatus::Success.to_string(), "SUCCESS");
        assert_eq!(OperationStatus::NotFound.to_string(), "NOTFOUND");
        assert_eq!(OperationStatus::KeyExist.to_string(), "KEYEXIST");
        assert_eq!(OperationStatus::KeyEmpty.to_string(), "KEYEMPTY");
    }

    #[test]
    fn test_all_variants() {
        let statuses = [
            OperationStatus::Success,
            OperationStatus::NotFound,
            OperationStatus::KeyExist,
            OperationStatus::KeyEmpty,
        ];

        assert_eq!(statuses.len(), 4);
    }
}
