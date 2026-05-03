//! BIN boundary key tracking.
//!
//! Port of `com.sleepycat.je.tree.BINBoundary` from JE.
//!
//! Identifies a BIN boundary by tracking the key(s) that define the boundary
//! between adjacent BINs in the tree.

/// Identifies a BIN boundary.
///
/// A BIN boundary is defined by one or more keys that separate adjacent BINs
/// in the tree. This is used during operations like splits and compaction to
/// track boundaries between BINs.
#[derive(Debug, Clone, Default)]
pub struct BinBoundary {
    /// True if this is the last BIN in its parent's key range.
    pub is_last_bin: bool,

    /// The keys that define this boundary.
    ///
    /// For a boundary between two BINs, this contains the key(s) that
    /// separate them. The exact semantics depend on whether this is the
    /// last BIN in the range.
    pub keys: Vec<Vec<u8>>,
}

impl BinBoundary {
    /// Creates a new BIN boundary.
    ///
    /// # Arguments
    /// * `is_last_bin` - True if this is the last BIN in its parent's key range
    /// * `keys` - The keys that define this boundary
    pub fn new(is_last_bin: bool, keys: Vec<Vec<u8>>) -> Self {
        BinBoundary { is_last_bin, keys }
    }

    /// Creates a BIN boundary with a single key.
    ///
    /// # Arguments
    /// * `is_last_bin` - True if this is the last BIN in its parent's key range
    /// * `key` - The key that defines this boundary
    pub fn with_single_key(is_last_bin: bool, key: Vec<u8>) -> Self {
        BinBoundary { is_last_bin, keys: vec![key] }
    }

    /// Creates a BIN boundary for the last BIN (with no keys).
    pub fn last_bin() -> Self {
        BinBoundary { is_last_bin: true, keys: Vec::new() }
    }

    /// Returns true if this boundary has any keys.
    #[inline]
    pub fn has_keys(&self) -> bool {
        !self.keys.is_empty()
    }

    /// Returns the number of keys in this boundary.
    #[inline]
    pub fn key_count(&self) -> usize {
        self.keys.len()
    }

    /// Returns the first key, if any.
    #[inline]
    pub fn first_key(&self) -> Option<&[u8]> {
        self.keys.first().map(|k| k.as_slice())
    }

    /// Returns the last key, if any.
    #[inline]
    pub fn last_key(&self) -> Option<&[u8]> {
        self.keys.last().map(|k| k.as_slice())
    }

    /// Adds a key to this boundary.
    pub fn add_key(&mut self, key: Vec<u8>) {
        self.keys.push(key);
    }

    /// Clears all keys from this boundary.
    pub fn clear_keys(&mut self) {
        self.keys.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let keys = vec![b"key1".to_vec(), b"key2".to_vec()];
        let boundary = BinBoundary::new(false, keys.clone());

        assert!(!boundary.is_last_bin);
        assert_eq!(boundary.keys.len(), 2);
        assert_eq!(boundary.keys, keys);
    }

    #[test]
    fn test_with_single_key() {
        let key = b"single_key".to_vec();
        let boundary = BinBoundary::with_single_key(false, key.clone());

        assert!(!boundary.is_last_bin);
        assert_eq!(boundary.keys.len(), 1);
        assert_eq!(boundary.keys[0], key);
    }

    #[test]
    fn test_last_bin() {
        let boundary = BinBoundary::last_bin();

        assert!(boundary.is_last_bin);
        assert!(boundary.keys.is_empty());
    }

    #[test]
    fn test_default() {
        let boundary = BinBoundary::default();

        assert!(!boundary.is_last_bin);
        assert!(boundary.keys.is_empty());
    }

    #[test]
    fn test_has_keys() {
        let mut boundary = BinBoundary::default();
        assert!(!boundary.has_keys());

        boundary.add_key(b"key".to_vec());
        assert!(boundary.has_keys());

        boundary.clear_keys();
        assert!(!boundary.has_keys());
    }

    #[test]
    fn test_key_count() {
        let mut boundary = BinBoundary::default();
        assert_eq!(boundary.key_count(), 0);

        boundary.add_key(b"key1".to_vec());
        assert_eq!(boundary.key_count(), 1);

        boundary.add_key(b"key2".to_vec());
        assert_eq!(boundary.key_count(), 2);
    }

    #[test]
    fn test_first_key() {
        let mut boundary = BinBoundary::default();
        assert!(boundary.first_key().is_none());

        boundary.add_key(b"first".to_vec());
        boundary.add_key(b"second".to_vec());

        assert_eq!(boundary.first_key(), Some(b"first".as_slice()));
    }

    #[test]
    fn test_last_key() {
        let mut boundary = BinBoundary::default();
        assert!(boundary.last_key().is_none());

        boundary.add_key(b"first".to_vec());
        boundary.add_key(b"second".to_vec());

        assert_eq!(boundary.last_key(), Some(b"second".as_slice()));
    }

    #[test]
    fn test_add_key() {
        let mut boundary = BinBoundary::default();

        boundary.add_key(b"key1".to_vec());
        assert_eq!(boundary.key_count(), 1);

        boundary.add_key(b"key2".to_vec());
        assert_eq!(boundary.key_count(), 2);

        assert_eq!(boundary.keys[0], b"key1");
        assert_eq!(boundary.keys[1], b"key2");
    }

    #[test]
    fn test_clear_keys() {
        let mut boundary = BinBoundary::with_single_key(false, b"key".to_vec());

        assert!(boundary.has_keys());

        boundary.clear_keys();

        assert!(!boundary.has_keys());
        assert_eq!(boundary.key_count(), 0);
    }

    #[test]
    fn test_clone() {
        let keys = vec![b"key1".to_vec(), b"key2".to_vec()];
        let boundary1 = BinBoundary::new(true, keys);

        let boundary2 = boundary1.clone();

        assert_eq!(boundary2.is_last_bin, boundary1.is_last_bin);
        assert_eq!(boundary2.keys, boundary1.keys);
    }

    #[test]
    fn test_is_last_bin_flag() {
        let boundary1 = BinBoundary::new(true, vec![]);
        assert!(boundary1.is_last_bin);

        let boundary2 = BinBoundary::new(false, vec![]);
        assert!(!boundary2.is_last_bin);
    }
}
