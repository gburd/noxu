//! Cursor put modes.
//!
//! Port of `com.sleepycat.je.dbi.PutMode`.

/// Distinguishes cursor put operations.
///
/// Port of `com.sleepycat.je.dbi.PutMode`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PutMode {
    /// Replace data at current position.
    Current,
    /// Insert key/data if not exists (duplicates DB).
    NoDupData,
    /// Insert if key doesn't exist.
    NoOverwrite,
    /// Insert or overwrite.
    Overwrite,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_equality() {
        assert_eq!(PutMode::Current, PutMode::Current);
        assert_ne!(PutMode::Current, PutMode::Overwrite);
    }

    #[test]
    fn test_all_variants() {
        let modes = [PutMode::Current,
            PutMode::NoDupData,
            PutMode::NoOverwrite,
            PutMode::Overwrite];

        assert_eq!(modes.len(), 4);
    }
}
