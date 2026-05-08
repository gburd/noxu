//! Reference from parent to child in the B-tree.
//!
//!
//! A ChildReference contains the key, LSN, and state for a reference from
//! a parent IN to a child node. This is primarily used for the tree root.

use crate::entry_states::SlotState;
use noxu_util::Lsn;

/// A reference in the tree from parent to child.
///
/// Contains a key, LSN (on-disk location), and state byte for tracking
/// deletion and dirty status. This structure is used primarily for the
/// tree root reference, though in it's also used within IN slot arrays.
///
/// In the Noxu DB Rust port, individual IN/BIN slot storage is separate from
/// ChildReference, but the concepts are similar.
#[derive(Debug, Clone)]
pub struct ChildReference {
    /// The key identifying this child in the parent's key range.
    pub key: Vec<u8>,

    /// The LSN of the child node on disk, or NULL_LSN if not yet logged.
    pub lsn: Lsn,

    /// State flags for this reference (dirty, known-deleted, etc.).
    pub state: SlotState,
}

impl ChildReference {
    /// Creates a new ChildReference.
    ///
    /// # Arguments
    /// * `key` - The key identifying this child
    /// * `lsn` - The LSN of the child node on disk
    /// * `state` - Initial state flags
    pub fn new(key: Vec<u8>, lsn: Lsn, state: SlotState) -> Self {
        ChildReference { key, lsn, state }
    }

    /// Creates a new ChildReference with empty state.
    ///
    /// # Arguments
    /// * `key` - The key identifying this child
    /// * `lsn` - The LSN of the child node on disk
    pub fn new_with_key_and_lsn(key: Vec<u8>, lsn: Lsn) -> Self {
        ChildReference { key, lsn, state: SlotState::new() }
    }

    /// Returns true if the known-deleted flag is set.
    #[inline]
    pub fn is_known_deleted(&self) -> bool {
        self.state.is_known_deleted()
    }

    /// Sets the known-deleted flag.
    #[inline]
    pub fn set_known_deleted(&mut self) {
        self.state.set_known_deleted();
    }

    /// Clears the known-deleted flag.
    #[inline]
    pub fn clear_known_deleted(&mut self) {
        self.state.clear_known_deleted();
    }

    /// Returns true if the pending-deleted flag is set.
    #[inline]
    pub fn is_pending_deleted(&self) -> bool {
        self.state.is_pending_deleted()
    }

    /// Sets the pending-deleted flag.
    #[inline]
    pub fn set_pending_deleted(&mut self) {
        self.state.set_pending_deleted();
    }

    /// Clears the pending-deleted flag.
    #[inline]
    pub fn clear_pending_deleted(&mut self) {
        self.state.clear_pending_deleted();
    }

    /// Returns true if the dirty flag is set.
    #[inline]
    pub fn is_dirty(&self) -> bool {
        self.state.is_dirty()
    }

    /// Sets the dirty flag.
    #[inline]
    pub fn set_dirty(&mut self) {
        self.state.set_dirty();
    }

    /// Clears the dirty flag.
    #[inline]
    pub fn clear_dirty(&mut self) {
        self.state.clear_dirty();
    }

    /// Returns true if the embedded-LN flag is set.
    #[inline]
    pub fn is_embedded_ln(&self) -> bool {
        self.state.is_embedded_ln()
    }

    /// Sets the embedded-LN flag.
    #[inline]
    pub fn set_embedded_ln(&mut self) {
        self.state.set_embedded_ln();
    }

    /// Clears all transient state bits (not persisted to disk).
    #[inline]
    pub fn clear_transient_bits(&mut self) {
        self.state.clear_transient_bits();
    }
}

impl Default for ChildReference {
    fn default() -> Self {
        ChildReference {
            key: Vec::new(),
            lsn: noxu_util::NULL_LSN,
            state: SlotState::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use noxu_util::{Lsn, NULL_LSN};

    #[test]
    fn test_new() {
        let key = b"test_key".to_vec();
        let lsn = Lsn::new(1, 1000);
        let state = SlotState::new();

        let child_ref = ChildReference::new(key.clone(), lsn, state);

        assert_eq!(child_ref.key, key);
        assert_eq!(child_ref.lsn, lsn);
        assert!(!child_ref.is_dirty());
    }

    #[test]
    fn test_new_with_key_and_lsn() {
        let key = b"key".to_vec();
        let lsn = Lsn::new(5, 5000);

        let child_ref = ChildReference::new_with_key_and_lsn(key.clone(), lsn);

        assert_eq!(child_ref.key, key);
        assert_eq!(child_ref.lsn, lsn);
        assert!(!child_ref.is_dirty());
        assert!(!child_ref.is_known_deleted());
    }

    #[test]
    fn test_default() {
        let child_ref = ChildReference::default();

        assert!(child_ref.key.is_empty());
        assert_eq!(child_ref.lsn, NULL_LSN);
        assert!(!child_ref.is_dirty());
    }

    #[test]
    fn test_dirty_flag() {
        let mut child_ref = ChildReference::default();

        assert!(!child_ref.is_dirty());

        child_ref.set_dirty();
        assert!(child_ref.is_dirty());

        child_ref.clear_dirty();
        assert!(!child_ref.is_dirty());
    }

    #[test]
    fn test_known_deleted_flag() {
        let mut child_ref = ChildReference::default();

        assert!(!child_ref.is_known_deleted());

        child_ref.set_known_deleted();
        assert!(child_ref.is_known_deleted());

        child_ref.clear_known_deleted();
        assert!(!child_ref.is_known_deleted());
    }

    #[test]
    fn test_pending_deleted_flag() {
        let mut child_ref = ChildReference::default();

        assert!(!child_ref.is_pending_deleted());

        child_ref.set_pending_deleted();
        assert!(child_ref.is_pending_deleted());

        child_ref.clear_pending_deleted();
        assert!(!child_ref.is_pending_deleted());
    }

    #[test]
    fn test_embedded_ln_flag() {
        let mut child_ref = ChildReference::default();

        assert!(!child_ref.is_embedded_ln());

        child_ref.set_embedded_ln();
        assert!(child_ref.is_embedded_ln());
    }

    #[test]
    fn test_multiple_flags() {
        let mut child_ref = ChildReference::default();

        child_ref.set_dirty();
        child_ref.set_pending_deleted();

        assert!(child_ref.is_dirty());
        assert!(child_ref.is_pending_deleted());
        assert!(!child_ref.is_known_deleted());

        child_ref.clear_dirty();
        assert!(!child_ref.is_dirty());
        assert!(child_ref.is_pending_deleted());
    }

    #[test]
    fn test_clear_transient_bits() {
        let mut child_ref = ChildReference::default();

        child_ref.state.set_dirty();
        child_ref.state.set_dirty();

        assert!(child_ref.state.is_dirty());
        assert!(child_ref.is_dirty());

        child_ref.clear_transient_bits();

        assert!(child_ref.state.is_dirty()); // dirty is not a transient bit
        assert!(child_ref.is_dirty()); // Non-transient bit remains
    }

    #[test]
    fn test_clone() {
        let key = b"clone_key".to_vec();
        let lsn = Lsn::new(10, 10000);
        let mut child_ref =
            ChildReference::new_with_key_and_lsn(key, lsn);
        child_ref.set_dirty();

        let cloned = child_ref.clone();

        assert_eq!(cloned.key, child_ref.key);
        assert_eq!(cloned.lsn, child_ref.lsn);
        assert_eq!(cloned.is_dirty(), child_ref.is_dirty());
    }
}
