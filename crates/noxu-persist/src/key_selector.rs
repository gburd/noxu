//! Key filtering for entity iteration.
//!
//! Port of key selection/filtering concepts from `com.sleepycat.persist`.
//! Provides traits and implementations for filtering entities by their
//! primary key during iteration.

use crate::entity::PrimaryKey;

/// Filter for selecting entities during iteration.
///
/// A `KeySelector` is applied during iteration to include or exclude
/// entities based on their primary key. This avoids deserializing
/// entity data for records that will be skipped.
///
/// Port of key-range filtering from `com.sleepycat.persist.EntityCursor`.
pub trait KeySelector<K: PrimaryKey> {
    /// Returns `true` if the entity with this key should be included.
    fn select(&self, key: &K) -> bool;
}

/// A selector that accepts all keys.
///
/// This is the default selector used when no filtering is needed.
#[derive(Debug, Clone, Copy, Default)]
pub struct AllKeysSelector;

impl<K: PrimaryKey> KeySelector<K> for AllKeysSelector {
    fn select(&self, _key: &K) -> bool {
        true
    }
}

/// A selector that accepts keys in a range.
///
/// Supports open or closed boundaries on either end. When a boundary
/// is `None`, that side of the range is unbounded.
///
/// Requires `K: Ord` so that key comparisons are possible.
#[derive(Debug, Clone)]
pub struct RangeKeySelector<K: PrimaryKey + Ord> {
    /// Minimum bound (inclusive or exclusive depending on `min_inclusive`).
    min: Option<K>,
    /// Maximum bound (inclusive or exclusive depending on `max_inclusive`).
    max: Option<K>,
    /// Whether the minimum bound is inclusive.
    min_inclusive: bool,
    /// Whether the maximum bound is inclusive.
    max_inclusive: bool,
}

impl<K: PrimaryKey + Ord> RangeKeySelector<K> {
    /// Creates a range selector with no bounds (accepts all keys).
    pub fn unbounded() -> Self {
        Self { min: None, max: None, min_inclusive: true, max_inclusive: true }
    }

    /// Creates a closed range selector `[min, max]`.
    pub fn closed(min: K, max: K) -> Self {
        Self {
            min: Some(min),
            max: Some(max),
            min_inclusive: true,
            max_inclusive: true,
        }
    }

    /// Creates an open range selector `(min, max)`.
    pub fn open(min: K, max: K) -> Self {
        Self {
            min: Some(min),
            max: Some(max),
            min_inclusive: false,
            max_inclusive: false,
        }
    }

    /// Creates a half-open range selector `[min, max)`.
    pub fn half_open(min: K, max: K) -> Self {
        Self {
            min: Some(min),
            max: Some(max),
            min_inclusive: true,
            max_inclusive: false,
        }
    }

    /// Creates a selector with only a lower bound `[min, ...)` or `(min, ...)`.
    pub fn from(min: K, inclusive: bool) -> Self {
        Self {
            min: Some(min),
            max: None,
            min_inclusive: inclusive,
            max_inclusive: true,
        }
    }

    /// Creates a selector with only an upper bound `(..., max]` or `(..., max)`.
    pub fn to(max: K, inclusive: bool) -> Self {
        Self {
            min: None,
            max: Some(max),
            min_inclusive: true,
            max_inclusive: inclusive,
        }
    }

    /// Builder-style method to set the minimum bound.
    pub fn with_min(mut self, min: K, inclusive: bool) -> Self {
        self.min = Some(min);
        self.min_inclusive = inclusive;
        self
    }

    /// Builder-style method to set the maximum bound.
    pub fn with_max(mut self, max: K, inclusive: bool) -> Self {
        self.max = Some(max);
        self.max_inclusive = inclusive;
        self
    }
}

impl<K: PrimaryKey + Ord> KeySelector<K> for RangeKeySelector<K> {
    fn select(&self, key: &K) -> bool {
        // Check lower bound
        if let Some(ref min) = self.min {
            if self.min_inclusive {
                if key < min {
                    return false;
                }
            } else if key <= min {
                return false;
            }
        }

        // Check upper bound
        if let Some(ref max) = self.max {
            if self.max_inclusive {
                if key > max {
                    return false;
                }
            } else if key >= max {
                return false;
            }
        }

        true
    }
}

/// A selector based on a predicate closure.
///
/// Useful for ad-hoc filtering without defining a new type.
pub struct PredicateKeySelector<K: PrimaryKey> {
    predicate: Box<dyn Fn(&K) -> bool + Send + Sync>,
}

impl<K: PrimaryKey> PredicateKeySelector<K> {
    /// Creates a new predicate-based selector.
    pub fn new<F>(predicate: F) -> Self
    where
        F: Fn(&K) -> bool + Send + Sync + 'static,
    {
        Self { predicate: Box::new(predicate) }
    }
}

impl<K: PrimaryKey> KeySelector<K> for PredicateKeySelector<K> {
    fn select(&self, key: &K) -> bool {
        (self.predicate)(key)
    }
}

/// A selector that accepts keys in an explicit set.
///
/// Useful when you know exactly which keys you want.
pub struct SetKeySelector<K: PrimaryKey> {
    keys: std::collections::HashSet<K>,
}

impl<K: PrimaryKey> SetKeySelector<K> {
    /// Creates a new set selector from an iterator of keys.
    pub fn new(keys: impl IntoIterator<Item = K>) -> Self {
        Self { keys: keys.into_iter().collect() }
    }
}

impl<K: PrimaryKey> KeySelector<K> for SetKeySelector<K> {
    fn select(&self, key: &K) -> bool {
        self.keys.contains(key)
    }
}

/// A selector that inverts another selector.
pub struct NotKeySelector<K: PrimaryKey, S: KeySelector<K>> {
    inner: S,
    _marker: std::marker::PhantomData<K>,
}

impl<K: PrimaryKey, S: KeySelector<K>> NotKeySelector<K, S> {
    /// Creates a new negating selector wrapping `inner`.
    pub fn new(inner: S) -> Self {
        Self { inner, _marker: std::marker::PhantomData }
    }
}

impl<K: PrimaryKey, S: KeySelector<K>> KeySelector<K> for NotKeySelector<K, S> {
    fn select(&self, key: &K) -> bool {
        !self.inner.select(key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // u64 implements PrimaryKey + Ord, so we use it directly.

    #[test]
    fn test_all_keys_selector() {
        let sel = AllKeysSelector;
        assert!(sel.select(&0u64));
        assert!(sel.select(&u64::MAX));
    }

    #[test]
    fn test_range_closed() {
        let sel = RangeKeySelector::closed(10u64, 20);
        assert!(!sel.select(&9));
        assert!(sel.select(&10));
        assert!(sel.select(&15));
        assert!(sel.select(&20));
        assert!(!sel.select(&21));
    }

    #[test]
    fn test_range_open() {
        let sel = RangeKeySelector::open(10u64, 20);
        assert!(!sel.select(&10));
        assert!(sel.select(&11));
        assert!(sel.select(&19));
        assert!(!sel.select(&20));
    }

    #[test]
    fn test_range_half_open() {
        let sel = RangeKeySelector::half_open(10u64, 20);
        assert!(sel.select(&10));
        assert!(sel.select(&19));
        assert!(!sel.select(&20));
    }

    #[test]
    fn test_range_unbounded() {
        let sel = RangeKeySelector::<u64>::unbounded();
        assert!(sel.select(&0));
        assert!(sel.select(&u64::MAX));
    }

    #[test]
    fn test_range_from_inclusive() {
        let sel = RangeKeySelector::from(10u64, true);
        assert!(!sel.select(&9));
        assert!(sel.select(&10));
        assert!(sel.select(&100));
    }

    #[test]
    fn test_range_from_exclusive() {
        let sel = RangeKeySelector::from(10u64, false);
        assert!(!sel.select(&10));
        assert!(sel.select(&11));
    }

    #[test]
    fn test_range_to_inclusive() {
        let sel = RangeKeySelector::to(20u64, true);
        assert!(sel.select(&0));
        assert!(sel.select(&20));
        assert!(!sel.select(&21));
    }

    #[test]
    fn test_range_to_exclusive() {
        let sel = RangeKeySelector::to(20u64, false);
        assert!(sel.select(&19));
        assert!(!sel.select(&20));
    }

    #[test]
    fn test_range_builder() {
        let sel = RangeKeySelector::<u64>::unbounded()
            .with_min(5, true)
            .with_max(15, false);
        assert!(!sel.select(&4));
        assert!(sel.select(&5));
        assert!(sel.select(&14));
        assert!(!sel.select(&15));
    }

    #[test]
    fn test_predicate_selector() {
        let sel = PredicateKeySelector::new(|k: &u64| k.is_multiple_of(2));
        assert!(sel.select(&0));
        assert!(!sel.select(&1));
        assert!(sel.select(&42));
        assert!(!sel.select(&43));
    }

    #[test]
    fn test_set_selector() {
        let sel = SetKeySelector::new(vec![1u64, 3, 5, 7]);
        assert!(sel.select(&1));
        assert!(!sel.select(&2));
        assert!(sel.select(&3));
        assert!(!sel.select(&4));
        assert!(sel.select(&5));
    }

    #[test]
    fn test_set_selector_empty() {
        let sel = SetKeySelector::<u64>::new(vec![]);
        assert!(!sel.select(&0));
        assert!(!sel.select(&1));
    }

    #[test]
    fn test_not_selector() {
        let inner = RangeKeySelector::closed(10u64, 20);
        let sel = NotKeySelector::new(inner);
        assert!(sel.select(&9));
        assert!(!sel.select(&10));
        assert!(!sel.select(&15));
        assert!(!sel.select(&20));
        assert!(sel.select(&21));
    }

    #[test]
    fn test_not_all_keys_is_none() {
        let sel = NotKeySelector::new(AllKeysSelector);
        assert!(!sel.select(&0u64));
        assert!(!sel.select(&u64::MAX));
    }

    #[test]
    fn test_range_with_i32_keys() {
        let sel = RangeKeySelector::closed(-10i32, 10);
        assert!(!sel.select(&-11));
        assert!(sel.select(&-10));
        assert!(sel.select(&0));
        assert!(sel.select(&10));
        assert!(!sel.select(&11));
    }

    #[test]
    fn test_range_single_point() {
        let sel = RangeKeySelector::closed(5u64, 5);
        assert!(!sel.select(&4));
        assert!(sel.select(&5));
        assert!(!sel.select(&6));
    }

    #[test]
    fn test_range_open_single_point_is_empty() {
        let sel = RangeKeySelector::open(5u64, 5);
        // Open range (5,5) should match nothing
        assert!(!sel.select(&4));
        assert!(!sel.select(&5));
        assert!(!sel.select(&6));
    }
}
