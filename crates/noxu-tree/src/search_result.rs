//! Tree search result structure.
//!
//! Port of `com.sleepycat.je.tree.SearchResult` from JE.
//!
//! Returned by tree search operations to indicate the result of a search
//! and the location where a key was found (or should be inserted).

/// Result of a tree search operation.
///
/// Contains information about whether an exact parent match was found,
/// the index within that parent, and whether the child is resident in memory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SearchResult {
    /// True if an exact match for the parent was found.
    pub exact_parent_found: bool,

    /// Index within the parent where the key was found (or should be inserted).
    ///
    /// This is the slot index in the parent IN/BIN. For insertions, this is
    /// where the new entry should go. For exact matches, this is where the
    /// matching entry is located.
    pub index: i32,

    /// True if the child node is not currently resident in memory.
    ///
    /// When true, the child needs to be fetched from disk before access.
    /// The LSN in the parent's slot indicates where to read the child from.
    pub child_not_resident: bool,
}

impl SearchResult {
    /// Creates a new SearchResult with all fields set to default values.
    ///
    /// Default values:
    /// - `exact_parent_found`: false
    /// - `index`: -1
    /// - `child_not_resident`: false
    pub fn new() -> Self {
        SearchResult {
            exact_parent_found: false,
            index: -1,
            child_not_resident: false,
        }
    }

    /// Creates a SearchResult with specific values.
    ///
    /// # Arguments
    /// * `exact_parent_found` - Whether an exact parent match was found
    /// * `index` - The slot index in the parent
    /// * `child_not_resident` - Whether the child needs to be fetched from disk
    pub fn with_values(
        exact_parent_found: bool,
        index: i32,
        child_not_resident: bool,
    ) -> Self {
        SearchResult { exact_parent_found, index, child_not_resident }
    }

    /// Resets all fields to their default values.
    ///
    /// This allows reusing a SearchResult instance across multiple searches.
    pub fn reset(&mut self) {
        self.exact_parent_found = false;
        self.index = -1;
        self.child_not_resident = false;
    }

    /// Returns true if this search result indicates a successful exact match.
    ///
    /// An exact match means the parent was found and contains the key at the
    /// specified index.
    #[inline]
    pub fn is_exact_match(&self) -> bool {
        self.exact_parent_found && self.index >= 0
    }

    /// Returns true if the index is valid (>= 0).
    #[inline]
    pub fn has_valid_index(&self) -> bool {
        self.index >= 0
    }
}

impl Default for SearchResult {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let result = SearchResult::new();

        assert!(!result.exact_parent_found);
        assert_eq!(result.index, -1);
        assert!(!result.child_not_resident);
    }

    #[test]
    fn test_default() {
        let result = SearchResult::default();

        assert!(!result.exact_parent_found);
        assert_eq!(result.index, -1);
        assert!(!result.child_not_resident);
    }

    #[test]
    fn test_with_values() {
        let result = SearchResult::with_values(true, 5, true);

        assert!(result.exact_parent_found);
        assert_eq!(result.index, 5);
        assert!(result.child_not_resident);
    }

    #[test]
    fn test_reset() {
        let mut result = SearchResult::with_values(true, 10, true);

        result.reset();

        assert!(!result.exact_parent_found);
        assert_eq!(result.index, -1);
        assert!(!result.child_not_resident);
    }

    #[test]
    fn test_is_exact_match() {
        let mut result = SearchResult::new();
        assert!(!result.is_exact_match());

        result.exact_parent_found = true;
        result.index = 5;
        assert!(result.is_exact_match());

        result.exact_parent_found = false;
        assert!(!result.is_exact_match());

        result.exact_parent_found = true;
        result.index = -1;
        assert!(!result.is_exact_match());
    }

    #[test]
    fn test_has_valid_index() {
        let mut result = SearchResult::new();
        assert!(!result.has_valid_index());

        result.index = 0;
        assert!(result.has_valid_index());

        result.index = 100;
        assert!(result.has_valid_index());

        result.index = -1;
        assert!(!result.has_valid_index());

        result.index = -5;
        assert!(!result.has_valid_index());
    }

    #[test]
    fn test_clone() {
        let result1 = SearchResult::with_values(true, 42, true);
        let result2 = result1.clone();

        assert_eq!(result1, result2);
        assert_eq!(result2.exact_parent_found, true);
        assert_eq!(result2.index, 42);
        assert_eq!(result2.child_not_resident, true);
    }

    #[test]
    fn test_equality() {
        let result1 = SearchResult::with_values(true, 5, false);
        let result2 = SearchResult::with_values(true, 5, false);
        let result3 = SearchResult::with_values(false, 5, false);

        assert_eq!(result1, result2);
        assert_ne!(result1, result3);
    }
}
