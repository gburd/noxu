//! Cursor search modes.
//!

/// Distinguishes cursor search operations.
///
///
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchMode {
    /// Search for an exact key match.
    Set,
    /// Search for an exact key and data match.
    Both,
    /// Search for key greater than or equal to search key.
    SetRange,
    /// Search for key/data greater than or equal to search key/data.
    BothRange,
}

impl SearchMode {
    /// Returns true if this is an exact search (not a range search).
    pub fn is_exact_search(&self) -> bool {
        matches!(self, SearchMode::Set | SearchMode::Both)
    }

    /// Returns true if this search includes data matching.
    pub fn is_data_search(&self) -> bool {
        matches!(self, SearchMode::Both | SearchMode::BothRange)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_exact_search() {
        assert!(SearchMode::Set.is_exact_search());
        assert!(SearchMode::Both.is_exact_search());
        assert!(!SearchMode::SetRange.is_exact_search());
        assert!(!SearchMode::BothRange.is_exact_search());
    }

    #[test]
    fn test_is_data_search() {
        assert!(!SearchMode::Set.is_data_search());
        assert!(SearchMode::Both.is_data_search());
        assert!(!SearchMode::SetRange.is_data_search());
        assert!(SearchMode::BothRange.is_data_search());
    }

    #[test]
    fn test_equality() {
        assert_eq!(SearchMode::Set, SearchMode::Set);
        assert_ne!(SearchMode::Set, SearchMode::SetRange);
    }

    #[test]
    fn test_all_variants() {
        let modes = [
            SearchMode::Set,
            SearchMode::Both,
            SearchMode::SetRange,
            SearchMode::BothRange,
        ];

        assert_eq!(modes.len(), 4);

        // Verify is_exact_search for all
        assert!(SearchMode::Set.is_exact_search());
        assert!(SearchMode::Both.is_exact_search());
        assert!(!SearchMode::SetRange.is_exact_search());
        assert!(!SearchMode::BothRange.is_exact_search());

        // Verify is_data_search for all
        assert!(!SearchMode::Set.is_data_search());
        assert!(SearchMode::Both.is_data_search());
        assert!(!SearchMode::SetRange.is_data_search());
        assert!(SearchMode::BothRange.is_data_search());
    }
}
