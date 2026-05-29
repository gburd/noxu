//! Typed iterators for `noxu-collections` Stored* views.
//!
//! The pre-1.6 iterator was a snapshot-of-keys
//! type that lazily fetched values, parameterised over `&[u8]` keys.
//! In v1.6 the Stored* surface is fully typed (parameterised by
//! `EntryBinding<K>` / `EntryBinding<V>`), so the iterator is now
//! generic over the item type `T` it yields.
//!
//! Implementation strategy: at iter() construction time the calling
//! Stored* view opens a cursor under the supplied `Option<&Transaction>`,
//! walks every record (or every record from a starting key), decodes
//! each via the bindings, and pushes the decoded items into a `Vec<T>`.
//! The iterator then yields from the `Vec`.  This matches BDB-JE's
//! "snapshot at iter() time" contract and avoids holding a live cursor
//! across the iteration's lifetime — the latter would force every
//! call site to thread three or four extra lifetime parameters.

use crate::error::Result;

/// Generic snapshot-based iterator over Stored* views.
///
/// `T` is the item type, which is `(K, V)` for `iter()`, `K` for
/// `keys()`, and `V` for `values()`.
///
/// # Snapshot semantics
///
/// The iterator is materialised eagerly at the call to `iter()` /
/// `keys()` / `values()`.  Concurrent modifications made *after* the
/// iterator has been constructed are not reflected in the iteration.
/// If you need transactional semantics, pass `Some(&txn)` to the
/// `iter()` call so the snapshot scan participates in your txn and
/// holds the appropriate locks.
pub struct StoredIterator<T> {
    /// Items materialised at iter() construction time.
    items: std::vec::IntoIter<T>,
    /// Total number of items at construction (for `len()` / size_hint).
    total: usize,
    /// Number consumed so far.
    consumed: usize,
}

impl<T> StoredIterator<T> {
    /// Constructs a new iterator from a pre-materialised vector of items.
    ///
    /// Called by Stored* views after they have completed the cursor scan.
    pub(crate) fn from_vec(items: Vec<T>) -> Self {
        let total = items.len();
        StoredIterator { items: items.into_iter(), total, consumed: 0 }
    }

    /// Returns the total number of items the iterator was constructed with.
    pub fn total(&self) -> usize {
        self.total
    }

    /// Returns the number of items already produced.
    pub fn consumed(&self) -> usize {
        self.consumed
    }

    /// Returns the number of items remaining.
    pub fn remaining(&self) -> usize {
        self.total.saturating_sub(self.consumed)
    }

    /// Returns whether the iterator has been exhausted.
    pub fn is_exhausted(&self) -> bool {
        self.remaining() == 0
    }
}

impl<T> Iterator for StoredIterator<T> {
    type Item = Result<T>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.items.next() {
            Some(item) => {
                self.consumed += 1;
                Some(Ok(item))
            }
            None => None,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.remaining();
        (remaining, Some(remaining))
    }
}

impl<T> ExactSizeIterator for StoredIterator<T> {
    fn len(&self) -> usize {
        self.remaining()
    }
}

impl<T> std::iter::FusedIterator for StoredIterator<T> {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_iterator_is_exhausted() {
        let mut iter: StoredIterator<i32> = StoredIterator::from_vec(vec![]);
        assert_eq!(iter.total(), 0);
        assert_eq!(iter.remaining(), 0);
        assert!(iter.is_exhausted());
        assert!(iter.next().is_none());
    }

    #[test]
    fn iterator_yields_in_order_and_tracks_progress() {
        let mut iter = StoredIterator::from_vec(vec![1, 2, 3]);
        assert_eq!(iter.total(), 3);
        assert_eq!(iter.size_hint(), (3, Some(3)));

        assert_eq!(iter.next().unwrap().unwrap(), 1);
        assert_eq!(iter.consumed(), 1);
        assert_eq!(iter.remaining(), 2);

        assert_eq!(iter.next().unwrap().unwrap(), 2);
        assert_eq!(iter.next().unwrap().unwrap(), 3);
        assert!(iter.next().is_none());
        assert!(iter.is_exhausted());
    }

    #[test]
    fn iterator_is_fused() {
        let mut iter = StoredIterator::from_vec(vec![10]);
        assert_eq!(iter.next().unwrap().unwrap(), 10);
        assert!(iter.next().is_none());
        assert!(iter.next().is_none());
        assert!(iter.next().is_none());
    }

    #[test]
    fn exact_size_iterator() {
        let iter = StoredIterator::from_vec(vec!["a", "b", "c"]);
        assert_eq!(iter.len(), 3);
    }
}
