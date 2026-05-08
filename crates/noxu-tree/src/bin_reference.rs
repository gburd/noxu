//! BIN reference for the INCompressor.
//!
//!
//! Identifies a BIN and a set of deleted keys for the INCompressor daemon.
//! The INCompressor processes these references to remove empty slots and
//! compress the tree structure.

/// Identifies a BIN and a set of deleted keys for the INCompressor.
///
/// When keys are deleted from the database, the BIN slots are marked as
/// deleted but not immediately removed. The INCompressor daemon processes
/// BINReferences to compact BINs by removing deleted entries.
#[derive(Debug, Clone)]
pub struct BinReference {
    /// The unique node ID of the BIN.
    pub node_id: i64,

    /// The database ID this BIN belongs to.
    pub db_id: u64,

    /// Keys that have been deleted and can be removed from the BIN.
    pub deleted_keys: Vec<Vec<u8>>,
}

impl BinReference {
    /// Creates a new BIN reference.
    ///
    /// # Arguments
    /// * `node_id` - The unique node ID of the BIN
    /// * `db_id` - The database ID this BIN belongs to
    /// * `deleted_keys` - Keys that have been deleted from this BIN
    pub fn new(node_id: i64, db_id: u64, deleted_keys: Vec<Vec<u8>>) -> Self {
        BinReference { node_id, db_id, deleted_keys }
    }

    /// Creates a new BIN reference with a single deleted key.
    ///
    /// # Arguments
    /// * `node_id` - The unique node ID of the BIN
    /// * `db_id` - The database ID this BIN belongs to
    /// * `deleted_key` - The deleted key
    pub fn with_single_key(
        node_id: i64,
        db_id: u64,
        deleted_key: Vec<u8>,
    ) -> Self {
        BinReference { node_id, db_id, deleted_keys: vec![deleted_key] }
    }

    /// Creates a new BIN reference with no deleted keys.
    ///
    /// Keys can be added later using `add_deleted_key()`.
    ///
    /// # Arguments
    /// * `node_id` - The unique node ID of the BIN
    /// * `db_id` - The database ID this BIN belongs to
    pub fn new_empty(node_id: i64, db_id: u64) -> Self {
        BinReference { node_id, db_id, deleted_keys: Vec::new() }
    }

    /// Returns the number of deleted keys in this reference.
    #[inline]
    pub fn deleted_key_count(&self) -> usize {
        self.deleted_keys.len()
    }

    /// Returns true if this reference has any deleted keys.
    #[inline]
    pub fn has_deleted_keys(&self) -> bool {
        !self.deleted_keys.is_empty()
    }

    /// Adds a deleted key to this reference.
    ///
    /// # Arguments
    /// * `key` - The deleted key to add
    pub fn add_deleted_key(&mut self, key: Vec<u8>) {
        self.deleted_keys.push(key);
    }

    /// Clears all deleted keys from this reference.
    pub fn clear_deleted_keys(&mut self) {
        self.deleted_keys.clear();
    }

    /// Returns an iterator over the deleted keys.
    pub fn deleted_keys_iter(&self) -> impl Iterator<Item = &[u8]> {
        self.deleted_keys.iter().map(|k| k.as_slice())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let keys = vec![b"key1".to_vec(), b"key2".to_vec()];
        let bin_ref = BinReference::new(42, 100, keys.clone());

        assert_eq!(bin_ref.node_id, 42);
        assert_eq!(bin_ref.db_id, 100);
        assert_eq!(bin_ref.deleted_keys.len(), 2);
        assert_eq!(bin_ref.deleted_keys, keys);
    }

    #[test]
    fn test_with_single_key() {
        let key = b"deleted_key".to_vec();
        let bin_ref = BinReference::with_single_key(123, 456, key.clone());

        assert_eq!(bin_ref.node_id, 123);
        assert_eq!(bin_ref.db_id, 456);
        assert_eq!(bin_ref.deleted_keys.len(), 1);
        assert_eq!(bin_ref.deleted_keys[0], key);
    }

    #[test]
    fn test_new_empty() {
        let bin_ref = BinReference::new_empty(999, 888);

        assert_eq!(bin_ref.node_id, 999);
        assert_eq!(bin_ref.db_id, 888);
        assert!(bin_ref.deleted_keys.is_empty());
    }

    #[test]
    fn test_deleted_key_count() {
        let mut bin_ref = BinReference::new_empty(1, 2);
        assert_eq!(bin_ref.deleted_key_count(), 0);

        bin_ref.add_deleted_key(b"key1".to_vec());
        assert_eq!(bin_ref.deleted_key_count(), 1);

        bin_ref.add_deleted_key(b"key2".to_vec());
        assert_eq!(bin_ref.deleted_key_count(), 2);
    }

    #[test]
    fn test_has_deleted_keys() {
        let mut bin_ref = BinReference::new_empty(1, 2);
        assert!(!bin_ref.has_deleted_keys());

        bin_ref.add_deleted_key(b"key".to_vec());
        assert!(bin_ref.has_deleted_keys());

        bin_ref.clear_deleted_keys();
        assert!(!bin_ref.has_deleted_keys());
    }

    #[test]
    fn test_add_deleted_key() {
        let mut bin_ref = BinReference::new_empty(10, 20);

        bin_ref.add_deleted_key(b"first".to_vec());
        assert_eq!(bin_ref.deleted_key_count(), 1);
        assert_eq!(bin_ref.deleted_keys[0], b"first");

        bin_ref.add_deleted_key(b"second".to_vec());
        assert_eq!(bin_ref.deleted_key_count(), 2);
        assert_eq!(bin_ref.deleted_keys[1], b"second");
    }

    #[test]
    fn test_clear_deleted_keys() {
        let keys = vec![b"key1".to_vec(), b"key2".to_vec()];
        let mut bin_ref = BinReference::new(1, 2, keys);

        assert!(bin_ref.has_deleted_keys());

        bin_ref.clear_deleted_keys();

        assert!(!bin_ref.has_deleted_keys());
        assert_eq!(bin_ref.deleted_key_count(), 0);
    }

    #[test]
    fn test_deleted_keys_iter() {
        let keys = vec![b"key1".to_vec(), b"key2".to_vec(), b"key3".to_vec()];
        let bin_ref = BinReference::new(1, 2, keys);

        let collected: Vec<&[u8]> = bin_ref.deleted_keys_iter().collect();

        assert_eq!(collected.len(), 3);
        assert_eq!(collected[0], b"key1");
        assert_eq!(collected[1], b"key2");
        assert_eq!(collected[2], b"key3");
    }

    #[test]
    fn test_clone() {
        let keys = vec![b"key1".to_vec(), b"key2".to_vec()];
        let bin_ref1 = BinReference::new(42, 100, keys);

        let bin_ref2 = bin_ref1.clone();

        assert_eq!(bin_ref2.node_id, bin_ref1.node_id);
        assert_eq!(bin_ref2.db_id, bin_ref1.db_id);
        assert_eq!(bin_ref2.deleted_keys, bin_ref1.deleted_keys);
    }

    #[test]
    fn test_empty_iter() {
        let bin_ref = BinReference::new_empty(1, 2);
        let collected: Vec<&[u8]> = bin_ref.deleted_keys_iter().collect();
        assert!(collected.is_empty());
    }

    #[test]
    fn test_node_and_db_ids() {
        let bin_ref = BinReference::new_empty(12345, 67890);
        assert_eq!(bin_ref.node_id, 12345);
        assert_eq!(bin_ref.db_id, 67890);
    }
}
