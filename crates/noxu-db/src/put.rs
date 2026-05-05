//! Put operation types.
//!
//! Port of put operation types from `com.sleepycat.je`.

/// Type of put operation for cursors and databases.
///
/// Specifies how to insert or update records.
///
/// Port of put operation types from Berkeley DB Java Edition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Put {
    /// Insert if the key doesn't exist, else return error.
    ///
    /// For non-duplicate databases, returns an error if the key exists.
    /// For duplicate databases, returns an error if the key/data pair exists.
    NoOverwrite,

    /// Insert or update (default behavior).
    ///
    /// For non-duplicate databases, inserts if the key doesn't exist, or
    /// replaces the data if the key exists. For duplicate databases, inserts
    /// a new duplicate.
    Overwrite,

    /// Insert if key doesn't exist, else do nothing.
    ///
    /// Similar to NoOverwrite, but returns success (with no update) if the
    /// key already exists, rather than an error. For duplicate databases,
    /// returns success if the key/data pair exists.
    NoDupData,

    /// Update the record at the current cursor position.
    ///
    /// Replaces the data at the current cursor position. The key cannot be
    /// changed. Returns an error if the cursor is not positioned.
    Current,
}

impl Put {
    /// Returns whether this operation allows overwriting existing records.
    pub fn allows_overwrite(&self) -> bool {
        matches!(self, Put::Overwrite | Put::Current)
    }

    /// Returns whether this operation returns an error if the record exists.
    pub fn errors_if_exists(&self) -> bool {
        matches!(self, Put::NoOverwrite)
    }

    /// Returns whether this operation requires the cursor to be positioned.
    pub fn requires_positioned_cursor(&self) -> bool {
        matches!(self, Put::Current)
    }

    /// Returns whether this operation prevents duplicate data.
    pub fn prevents_duplicates(&self) -> bool {
        matches!(self, Put::NoDupData)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_allows_overwrite() {
        assert!(Put::Overwrite.allows_overwrite());
        assert!(Put::Current.allows_overwrite());
        assert!(!Put::NoOverwrite.allows_overwrite());
        assert!(!Put::NoDupData.allows_overwrite());
    }

    #[test]
    fn test_errors_if_exists() {
        assert!(Put::NoOverwrite.errors_if_exists());
        assert!(!Put::Overwrite.errors_if_exists());
        assert!(!Put::NoDupData.errors_if_exists());
        assert!(!Put::Current.errors_if_exists());
    }

    #[test]
    fn test_requires_positioned_cursor() {
        assert!(Put::Current.requires_positioned_cursor());
        assert!(!Put::Overwrite.requires_positioned_cursor());
        assert!(!Put::NoOverwrite.requires_positioned_cursor());
        assert!(!Put::NoDupData.requires_positioned_cursor());
    }

    #[test]
    fn test_prevents_duplicates() {
        assert!(Put::NoDupData.prevents_duplicates());
        assert!(!Put::Overwrite.prevents_duplicates());
        assert!(!Put::NoOverwrite.prevents_duplicates());
        assert!(!Put::Current.prevents_duplicates());
    }

    #[test]
    fn test_equality() {
        assert_eq!(Put::NoOverwrite, Put::NoOverwrite);
        assert_ne!(Put::NoOverwrite, Put::Overwrite);
    }

    #[test]
    fn test_clone() {
        let put1 = Put::Current;
        let put2 = put1;
        assert_eq!(put1, put2);
    }

    #[test]
    fn test_copy() {
        let put1 = Put::Overwrite;
        let put2 = put1;
        assert_eq!(put1, put2);
    }

    #[test]
    fn test_debug() {
        let put = Put::NoDupData;
        let debug = format!("{:?}", put);
        assert_eq!(debug, "NoDupData");
    }

    #[test]
    fn test_all_variants() {
        let variants =
            [Put::NoOverwrite, Put::Overwrite, Put::NoDupData, Put::Current];
        // Just ensure all variants are created successfully
        assert_eq!(variants.len(), 4);
    }
}
