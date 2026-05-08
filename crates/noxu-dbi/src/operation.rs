//! Replication operation types.
//!

/// Types of operations for replication.
///
/// 
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operation {
    /// Standard put operation.
    Put,
    /// Put only if key doesn't exist.
    NoOverwrite,
    /// No-op / filler operation.
    Placeholder,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_equality() {
        assert_eq!(Operation::Put, Operation::Put);
        assert_ne!(Operation::Put, Operation::NoOverwrite);
    }

    #[test]
    fn test_all_variants() {
        let ops = [Operation::Put,
            Operation::NoOverwrite,
            Operation::Placeholder];

        assert_eq!(ops.len(), 3);
    }
}
