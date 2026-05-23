//! Transaction state enum.
//!

/// Transaction states.
///
///
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnState {
    /// Transaction is active and accepting operations.
    Open,
    /// Transaction must be aborted (due to a failure).
    MustAbort,
    /// Transaction has been committed.
    Committed,
    /// Transaction has been aborted.
    Aborted,
}

impl TxnState {
    /// Returns true if the transaction is open.
    pub fn is_open(&self) -> bool {
        *self == TxnState::Open
    }

    /// Returns true if the transaction is in a valid state for operations.
    pub fn is_valid(&self) -> bool {
        matches!(self, TxnState::Open | TxnState::MustAbort)
    }

    /// Returns true if the transaction has ended (committed or aborted).
    pub fn is_ended(&self) -> bool {
        matches!(self, TxnState::Committed | TxnState::Aborted)
    }
}

impl std::fmt::Display for TxnState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TxnState::Open => write!(f, "OPEN"),
            TxnState::MustAbort => write!(f, "MUST_ABORT"),
            TxnState::Committed => write!(f, "COMMITTED"),
            TxnState::Aborted => write!(f, "ABORTED"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_state_predicates() {
        assert!(TxnState::Open.is_open());
        assert!(!TxnState::MustAbort.is_open());
        assert!(!TxnState::Committed.is_open());
        assert!(!TxnState::Aborted.is_open());

        assert!(TxnState::Open.is_valid());
        assert!(TxnState::MustAbort.is_valid());
        assert!(!TxnState::Committed.is_valid());
        assert!(!TxnState::Aborted.is_valid());

        assert!(!TxnState::Open.is_ended());
        assert!(!TxnState::MustAbort.is_ended());
        assert!(TxnState::Committed.is_ended());
        assert!(TxnState::Aborted.is_ended());
    }

    #[test]
    fn test_display() {
        assert_eq!(TxnState::Open.to_string(), "OPEN");
        assert_eq!(TxnState::MustAbort.to_string(), "MUST_ABORT");
        assert_eq!(TxnState::Committed.to_string(), "COMMITTED");
        assert_eq!(TxnState::Aborted.to_string(), "ABORTED");
    }
}
