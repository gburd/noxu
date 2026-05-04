//! Get operation types.
//!
//! Port of get operation types from `com.sleepycat.je`.

/// Type of get operation for cursors and databases.
///
/// Specifies how to position the cursor or which record to retrieve.
///
/// Port of get operation types from Berkeley DB Java Edition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Get {
    /// Get the record matching the key.
    ///
    /// Searches for a record with the specified key. For duplicate databases,
    /// returns the first duplicate.
    Search,

    /// Get the record matching both key and data.
    ///
    /// For duplicate databases, searches for a record matching both the key
    /// and the data. Returns an error if not found.
    SearchBoth,

    /// Get the record with the smallest key.
    ///
    /// Positions the cursor at the first record in the database.
    First,

    /// Get the record with the largest key.
    ///
    /// Positions the cursor at the last record in the database.
    Last,

    /// Get the next record.
    ///
    /// Moves the cursor to the next record in key order. For duplicate
    /// databases, moves to the next duplicate of the current key, or the
    /// first duplicate of the next key if at the last duplicate.
    Next,

    /// Get the previous record.
    ///
    /// Moves the cursor to the previous record in key order. For duplicate
    /// databases, moves to the previous duplicate of the current key, or the
    /// last duplicate of the previous key if at the first duplicate.
    Prev,

    /// Get the next record with a different key.
    ///
    /// Skips all duplicates of the current key and moves to the first
    /// duplicate of the next key.
    NextNoDup,

    /// Get the previous record with a different key.
    ///
    /// Skips all duplicates of the current key and moves to the last
    /// duplicate of the previous key.
    PrevNoDup,

    /// Get the current record.
    ///
    /// Returns the record at the current cursor position. Useful after
    /// positioning the cursor to re-read the record.
    Current,

    /// Get the record with the smallest key greater than or equal to the specified key.
    ///
    /// Positions the cursor at the first record with a key greater than or
    /// equal to the search key.
    ///
    /// Also known as `SearchRange` (JE: `Cursor.getSearchKeyRange`).
    SearchGte,

    /// Alias for `SearchGte`.  Matches the JE `SEARCH_RANGE`/`getSearchKeyRange` name.
    SearchRange,

    /// Get the record with the largest key less than or equal to the specified key.
    ///
    /// Positions the cursor at the last record with a key less than or
    /// equal to the search key.
    SearchLte,

    /// Get the first duplicate of the current key.
    ///
    /// For duplicate databases, positions at the first duplicate of the
    /// current key. Has no effect if not positioned on a key.
    FirstDup,

    /// Get the last duplicate of the current key.
    ///
    /// For duplicate databases, positions at the last duplicate of the
    /// current key. Has no effect if not positioned on a key.
    LastDup,

    /// Get the next duplicate of the current key.
    ///
    /// For duplicate databases, moves to the next duplicate of the current
    /// key. Returns an error if at the last duplicate.
    NextDup,

    /// Get the previous duplicate of the current key.
    ///
    /// For duplicate databases, moves to the previous duplicate of the current
    /// key. Returns an error if at the first duplicate.
    PrevDup,
}

impl Get {
    /// Returns whether this operation requires a key parameter.
    pub fn requires_key(&self) -> bool {
        matches!(
            self,
            Get::Search | Get::SearchBoth | Get::SearchGte | Get::SearchLte | Get::SearchRange
        )
    }

    /// Returns whether this operation requires a data parameter.
    pub fn requires_data(&self) -> bool {
        matches!(self, Get::SearchBoth)
    }

    /// Returns whether this operation is valid only for duplicate databases.
    pub fn requires_duplicates(&self) -> bool {
        matches!(
            self,
            Get::SearchBoth
                | Get::FirstDup
                | Get::LastDup
                | Get::NextDup
                | Get::PrevDup
        )
    }

    /// Returns whether this operation moves the cursor position.
    pub fn moves_cursor(&self) -> bool {
        !matches!(self, Get::Current)
    }

    /// Returns whether this operation moves forward in key order.
    pub fn moves_forward(&self) -> bool {
        matches!(self, Get::Next | Get::NextDup | Get::NextNoDup)
    }

    /// Returns whether this operation moves backward in key order.
    pub fn moves_backward(&self) -> bool {
        matches!(self, Get::Prev | Get::PrevDup | Get::PrevNoDup)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_requires_key() {
        assert!(Get::Search.requires_key());
        assert!(Get::SearchBoth.requires_key());
        assert!(Get::SearchGte.requires_key());
        assert!(Get::SearchLte.requires_key());
        assert!(!Get::First.requires_key());
        assert!(!Get::Next.requires_key());
    }

    #[test]
    fn test_requires_data() {
        assert!(Get::SearchBoth.requires_data());
        assert!(!Get::Search.requires_data());
        assert!(!Get::First.requires_data());
    }

    #[test]
    fn test_requires_duplicates() {
        assert!(Get::SearchBoth.requires_duplicates());
        assert!(Get::FirstDup.requires_duplicates());
        assert!(Get::LastDup.requires_duplicates());
        assert!(Get::NextDup.requires_duplicates());
        assert!(Get::PrevDup.requires_duplicates());
        assert!(!Get::First.requires_duplicates());
        assert!(!Get::Next.requires_duplicates());
    }

    #[test]
    fn test_moves_cursor() {
        assert!(Get::First.moves_cursor());
        assert!(Get::Next.moves_cursor());
        assert!(!Get::Current.moves_cursor());
    }

    #[test]
    fn test_moves_forward() {
        assert!(Get::Next.moves_forward());
        assert!(Get::NextDup.moves_forward());
        assert!(Get::NextNoDup.moves_forward());
        assert!(!Get::Prev.moves_forward());
        assert!(!Get::First.moves_forward());
    }

    #[test]
    fn test_moves_backward() {
        assert!(Get::Prev.moves_backward());
        assert!(Get::PrevDup.moves_backward());
        assert!(Get::PrevNoDup.moves_backward());
        assert!(!Get::Next.moves_backward());
        assert!(!Get::Last.moves_backward());
    }

    #[test]
    fn test_equality() {
        assert_eq!(Get::First, Get::First);
        assert_ne!(Get::First, Get::Last);
    }

    #[test]
    fn test_clone() {
        let get1 = Get::Search;
        let get2 = get1.clone();
        assert_eq!(get1, get2);
    }

    #[test]
    fn test_copy() {
        let get1 = Get::Next;
        let get2 = get1;
        assert_eq!(get1, get2);
    }

    #[test]
    fn test_debug() {
        let get = Get::SearchBoth;
        let debug = format!("{:?}", get);
        assert_eq!(debug, "SearchBoth");
    }
}
