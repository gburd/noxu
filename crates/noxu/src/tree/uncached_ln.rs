//! UncachedLN  -  LN variant that avoids data copy when not cached.
//!
//!
//! This is one of the 10 extended-fork enhancements. An UncachedLN is used
//! when the LN data is not needed in cache  -  the data is read directly
//! from the log without copying into the BIN cache. This optimization
//! reduces memory usage for workloads where data is read once and
//! discarded (e.g., sequential scans).
//!
//! UncachedLN is a subclass of LN. In Noxu DB, we use a factory
//! function to create LNs with appropriate hints for the evictor.

use crate::tree::ln::Ln;

/// Marker flag bit for LNs that should not be cached.
///
/// This is an internal flag used by the evictor to identify LNs that
/// should be evicted more aggressively.
pub const UNCACHED_FLAG: u32 = 0x20000000;

/// Creates an uncached LN  -  one whose data should not be retained in cache.
///
/// This is used for workloads where data is read once and then discarded,
/// such as sequential table scans. The evictor will prioritize evicting
/// uncached LNs to keep frequently-accessed data in cache.
///
/// # Arguments
///
/// * `data` - The record data, or None for a deleted record
///
/// # Returns
///
/// An LN marked for aggressive eviction
pub fn make_uncached_ln(data: Option<Vec<u8>>) -> Ln {
    let mut ln = Ln::new(data);
    // Mark as fetched cold  -  this is the closest equivalent to "uncached"
    // in the current implementation. The evictor will prioritize evicting
    // cold LNs.
    ln.set_fetched_cold();
    ln
}

/// Creates an uncached LN from a byte slice.
///
/// Convenience wrapper around `make_uncached_ln` that copies the data.
pub fn make_uncached_ln_from_bytes(data: &[u8]) -> Ln {
    make_uncached_ln(Some(data.to_vec()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::Vlsn;

    #[test]
    fn test_make_uncached_ln() {
        let data = b"uncached data".to_vec();
        let ln = make_uncached_ln(Some(data.clone()));

        assert_eq!(ln.get_data(), Some(data.as_slice()));
        assert!(!ln.is_deleted());
        assert!(ln.is_dirty());
        // Uncached LNs are marked as fetched_cold
        assert!(ln.is_fetched_cold());
    }

    #[test]
    fn test_make_uncached_ln_deleted() {
        let ln = make_uncached_ln(None);

        assert!(ln.is_deleted());
        assert!(ln.is_fetched_cold());
    }

    #[test]
    fn test_make_uncached_ln_from_bytes() {
        let data = b"test data";
        let ln = make_uncached_ln_from_bytes(data);

        assert_eq!(ln.get_data(), Some(data.as_slice()));
        assert!(ln.is_fetched_cold());
    }

    #[test]
    fn test_uncached_ln_serialization() {
        let data = b"serialize me".to_vec();
        let mut ln = make_uncached_ln(Some(data.clone()));
        ln.set_vlsn(Vlsn::new(42));

        let mut buf = Vec::new();
        ln.write_to_log(&mut buf);

        let ln2 = Ln::read_from_log(&buf).unwrap();

        // After deserialization, the data is intact
        assert_eq!(ln2.get_data(), Some(data.as_slice()));
        assert_eq!(ln2.get_vlsn().sequence(), 42);
        // But the transient fetched_cold flag is not persisted
        assert!(!ln2.is_fetched_cold());
    }

    #[test]
    fn test_uncached_ln_eviction_hint() {
        let regular_ln = Ln::new(Some(b"regular".to_vec()));
        let uncached_ln = make_uncached_ln(Some(b"uncached".to_vec()));

        // Regular LN is not marked for aggressive eviction
        assert!(!regular_ln.is_fetched_cold());

        // Uncached LN is marked for aggressive eviction
        assert!(uncached_ln.is_fetched_cold());
    }

    #[test]
    fn test_uncached_ln_memory_tracking() {
        let data = vec![0u8; 1000];
        let ln = make_uncached_ln(Some(data));

        // Memory tracking works the same as regular LNs
        assert!(ln.get_memory_size() > 1000);
        assert_eq!(
            ln.get_memory_size(),
            ln.get_memory_size_included_by_parent()
        );
    }
}
