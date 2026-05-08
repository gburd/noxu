//! Cursor get modes.
//!

/// Distinguishes which variety of get operation a cursor should use.
///
/// 
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GetMode {
    /// Get the next key/data pair.
    Next,
    /// Get the previous key/data pair.
    Prev,
    /// Get the next duplicate key/data pair.
    NextDup,
    /// Get the previous duplicate key/data pair.
    PrevDup,
    /// Get the next non-duplicate key/data pair.
    NextNoDup,
    /// Get the previous non-duplicate key/data pair.
    PrevNoDup,
}

impl GetMode {
    /// Returns true if this is a forward-moving cursor operation.
    pub fn is_forward(&self) -> bool {
        matches!(self, GetMode::Next | GetMode::NextDup | GetMode::NextNoDup)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_forward() {
        assert!(GetMode::Next.is_forward());
        assert!(GetMode::NextDup.is_forward());
        assert!(GetMode::NextNoDup.is_forward());

        assert!(!GetMode::Prev.is_forward());
        assert!(!GetMode::PrevDup.is_forward());
        assert!(!GetMode::PrevNoDup.is_forward());
    }

    #[test]
    fn test_equality() {
        assert_eq!(GetMode::Next, GetMode::Next);
        assert_ne!(GetMode::Next, GetMode::Prev);
    }

    #[test]
    fn test_all_variants() {
        // Ensure all variants are tested
        let modes = [GetMode::Next,
            GetMode::Prev,
            GetMode::NextDup,
            GetMode::PrevDup,
            GetMode::NextNoDup,
            GetMode::PrevNoDup];

        assert_eq!(modes.len(), 6);

        // Verify is_forward for all
        assert!(GetMode::Next.is_forward());
        assert!(!GetMode::Prev.is_forward());
        assert!(GetMode::NextDup.is_forward());
        assert!(!GetMode::PrevDup.is_forward());
        assert!(GetMode::NextNoDup.is_forward());
        assert!(!GetMode::PrevNoDup.is_forward());
    }
}
