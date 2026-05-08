//! Recovery cursor-like position tracker.
//!
//!
//! Tracks a location in a tree during recovery operations. This is similar
//! to a cursor but is used specifically for recovery processing.

use noxu_util::{Lsn, NULL_LSN};

/// Cursor-like object tracking a location in a tree.
///
/// Used during recovery to track the position being processed and maintain
/// context about the current entry's attributes.
#[derive(Debug, Clone)]
pub struct TreeLocation {
    /// Index within the current BIN.
    pub index: i32,

    /// The key at this location (if known).
    pub ln_key: Option<Vec<u8>>,

    /// LSN of the child at this location.
    pub child_lsn: Lsn,

    /// Logged size of the child entry.
    pub child_logged_size: i32,

    /// True if this entry is known deleted.
    pub is_kd: bool,

    /// True if this is an embedded LN.
    pub is_embedded: bool,
}

impl TreeLocation {
    /// Creates a new TreeLocation with default values.
    pub fn new() -> Self {
        TreeLocation {
            index: -1,
            ln_key: None,
            child_lsn: NULL_LSN,
            child_logged_size: 0,
            is_kd: false,
            is_embedded: false,
        }
    }

    /// Creates a TreeLocation with specific index and key.
    ///
    /// # Arguments
    /// * `index` - The slot index in the BIN
    /// * `ln_key` - The key at this location
    pub fn with_index_and_key(index: i32, ln_key: Vec<u8>) -> Self {
        TreeLocation {
            index,
            ln_key: Some(ln_key),
            child_lsn: NULL_LSN,
            child_logged_size: 0,
            is_kd: false,
            is_embedded: false,
        }
    }

    /// Resets all fields to their default values.
    ///
    /// This allows reusing a TreeLocation instance.
    pub fn reset(&mut self) {
        self.index = -1;
        self.ln_key = None;
        self.child_lsn = NULL_LSN;
        self.child_logged_size = 0;
        self.is_kd = false;
        self.is_embedded = false;
    }

    /// Returns true if this location has a valid index.
    #[inline]
    pub fn has_valid_index(&self) -> bool {
        self.index >= 0
    }

    /// Returns true if a key is present at this location.
    #[inline]
    pub fn has_key(&self) -> bool {
        self.ln_key.is_some()
    }

    /// Returns the key as a slice, if present.
    #[inline]
    pub fn key_as_slice(&self) -> Option<&[u8]> {
        self.ln_key.as_deref()
    }

    /// Sets the key at this location.
    pub fn set_key(&mut self, key: Vec<u8>) {
        self.ln_key = Some(key);
    }

    /// Clears the key at this location.
    pub fn clear_key(&mut self) {
        self.ln_key = None;
    }
}

impl Default for TreeLocation {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use noxu_util::Lsn;

    #[test]
    fn test_new() {
        let loc = TreeLocation::new();

        assert_eq!(loc.index, -1);
        assert!(loc.ln_key.is_none());
        assert_eq!(loc.child_lsn, NULL_LSN);
        assert_eq!(loc.child_logged_size, 0);
        assert!(!loc.is_kd);
        assert!(!loc.is_embedded);
    }

    #[test]
    fn test_default() {
        let loc = TreeLocation::default();

        assert_eq!(loc.index, -1);
        assert!(loc.ln_key.is_none());
    }

    #[test]
    fn test_with_index_and_key() {
        let key = b"test_key".to_vec();
        let loc = TreeLocation::with_index_and_key(5, key.clone());

        assert_eq!(loc.index, 5);
        assert_eq!(loc.ln_key, Some(key));
        assert_eq!(loc.child_lsn, NULL_LSN);
    }

    #[test]
    fn test_reset() {
        let mut loc = TreeLocation::with_index_and_key(10, b"key".to_vec());
        loc.child_lsn = Lsn::new(1, 1000);
        loc.child_logged_size = 100;
        loc.is_kd = true;
        loc.is_embedded = true;

        loc.reset();

        assert_eq!(loc.index, -1);
        assert!(loc.ln_key.is_none());
        assert_eq!(loc.child_lsn, NULL_LSN);
        assert_eq!(loc.child_logged_size, 0);
        assert!(!loc.is_kd);
        assert!(!loc.is_embedded);
    }

    #[test]
    fn test_has_valid_index() {
        let mut loc = TreeLocation::new();
        assert!(!loc.has_valid_index());

        loc.index = 0;
        assert!(loc.has_valid_index());

        loc.index = 42;
        assert!(loc.has_valid_index());

        loc.index = -1;
        assert!(!loc.has_valid_index());
    }

    #[test]
    fn test_has_key() {
        let mut loc = TreeLocation::new();
        assert!(!loc.has_key());

        loc.set_key(b"key".to_vec());
        assert!(loc.has_key());

        loc.clear_key();
        assert!(!loc.has_key());
    }

    #[test]
    fn test_key_as_slice() {
        let mut loc = TreeLocation::new();
        assert!(loc.key_as_slice().is_none());

        let key = b"test_key".to_vec();
        loc.set_key(key.clone());

        assert_eq!(loc.key_as_slice(), Some(key.as_slice()));
    }

    #[test]
    fn test_set_and_clear_key() {
        let mut loc = TreeLocation::new();

        let key1 = b"key1".to_vec();
        loc.set_key(key1.clone());
        assert_eq!(loc.ln_key, Some(key1));

        let key2 = b"key2".to_vec();
        loc.set_key(key2.clone());
        assert_eq!(loc.ln_key, Some(key2));

        loc.clear_key();
        assert!(loc.ln_key.is_none());
    }

    #[test]
    fn test_clone() {
        let mut loc1 = TreeLocation::with_index_and_key(5, b"key".to_vec());
        loc1.child_lsn = Lsn::new(2, 2000);
        loc1.is_kd = true;

        let loc2 = loc1.clone();

        assert_eq!(loc2.index, loc1.index);
        assert_eq!(loc2.ln_key, loc1.ln_key);
        assert_eq!(loc2.child_lsn, loc1.child_lsn);
        assert_eq!(loc2.is_kd, loc1.is_kd);
    }

    #[test]
    fn test_mutable_fields() {
        let mut loc = TreeLocation::new();

        loc.index = 10;
        loc.child_lsn = Lsn::new(5, 5000);
        loc.child_logged_size = 256;
        loc.is_kd = true;
        loc.is_embedded = true;

        assert_eq!(loc.index, 10);
        assert_eq!(loc.child_lsn, Lsn::new(5, 5000));
        assert_eq!(loc.child_logged_size, 256);
        assert!(loc.is_kd);
        assert!(loc.is_embedded);
    }
}
