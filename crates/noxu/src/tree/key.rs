//! Key comparison and prefix utilities.
//!
//!
//! Provides utilities for comparing keys and computing key prefixes for
//! key compression in the B-tree.

use std::cmp::Ordering;

/// Empty key constant.
pub const EMPTY_KEY: &[u8] = &[];

/// Trait for custom key comparison.
///
/// Allows databases to use application-specific ordering rather than
/// the default unsigned byte-by-byte comparison.
pub trait KeyComparator: Send + Sync {
    /// Compares two keys and returns their ordering.
    ///
    /// # Arguments
    /// * `key1` - First key to compare
    /// * `key2` - Second key to compare
    ///
    /// # Returns
    /// `Ordering::Less` if key1 < key2, `Ordering::Equal` if equal,
    /// `Ordering::Greater` if key1 > key2
    fn compare(&self, key1: &[u8], key2: &[u8]) -> Ordering;
}

/// Compares two keys using unsigned byte-by-byte comparison.
///
/// This is the default comparison used when no custom comparator is specified.
/// Each byte is treated as an unsigned value (0-255).
///
/// # Arguments
/// * `key1` - First key
/// * `key2` - Second key
///
/// # Returns
/// `Ordering::Less` if key1 < key2, `Ordering::Equal` if equal,
/// `Ordering::Greater` if key1 > key2
pub fn compare_unsigned_bytes(key1: &[u8], key2: &[u8]) -> Ordering {
    // Rust's slice comparison already does unsigned byte-by-byte comparison
    key1.cmp(key2)
}

/// Compares two keys using either a custom comparator or default unsigned comparison.
///
/// # Arguments
/// * `key1` - First key
/// * `key2` - Second key
/// * `comparator` - Optional custom comparator. If None, uses unsigned byte comparison.
///
/// # Returns
/// `Ordering::Less` if key1 < key2, `Ordering::Equal` if equal,
/// `Ordering::Greater` if key1 > key2
pub fn compare_keys(
    key1: &[u8],
    key2: &[u8],
    comparator: Option<&dyn KeyComparator>,
) -> Ordering {
    match comparator {
        Some(cmp) => cmp.compare(key1, key2),
        None => compare_unsigned_bytes(key1, key2),
    }
}

/// Returns the length of the common prefix between two keys.
///
/// Used for key prefix compression in the B-tree. The returned length
/// is the number of leading bytes that are identical in both keys.
///
/// # Arguments
/// * `key1` - First key
/// * `key2` - Second key
///
/// # Returns
/// The number of bytes in the common prefix (0 if no common prefix)
pub fn get_key_prefix_length(key1: &[u8], key2: &[u8]) -> usize {
    let min_len = key1.len().min(key2.len());
    let mut prefix_len = 0;

    for i in 0..min_len {
        if key1[i] == key2[i] {
            prefix_len += 1;
        } else {
            break;
        }
    }

    prefix_len
}

/// Creates a key prefix from the common leading bytes of two keys.
///
/// Returns `None` if the keys have no common prefix (differ at first byte).
/// Returns `Some(prefix)` if there is at least one common leading byte.
///
/// # Arguments
/// * `key1` - First key
/// * `key2` - Second key
///
/// # Returns
/// `Some(Vec<u8>)` containing the common prefix, or `None` if no common prefix
pub fn create_key_prefix(key1: &[u8], key2: &[u8]) -> Option<Vec<u8>> {
    let prefix_len = get_key_prefix_length(key1, key2);

    if prefix_len == 0 { None } else { Some(key1[..prefix_len].to_vec()) }
}

/// Default key comparator that uses unsigned byte comparison.
#[derive(Debug, Clone, Copy)]
pub struct DefaultComparator;

impl KeyComparator for DefaultComparator {
    fn compare(&self, key1: &[u8], key2: &[u8]) -> Ordering {
        compare_unsigned_bytes(key1, key2)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compare_unsigned_bytes() {
        let k1 = b"abc";
        let k2 = b"abd";
        let k3 = b"abc";

        assert_eq!(compare_unsigned_bytes(k1, k2), Ordering::Less);
        assert_eq!(compare_unsigned_bytes(k2, k1), Ordering::Greater);
        assert_eq!(compare_unsigned_bytes(k1, k3), Ordering::Equal);
    }

    #[test]
    fn test_compare_different_lengths() {
        let k1 = b"abc";
        let k2 = b"abcd";

        assert_eq!(compare_unsigned_bytes(k1, k2), Ordering::Less);
        assert_eq!(compare_unsigned_bytes(k2, k1), Ordering::Greater);
    }

    #[test]
    fn test_compare_empty_keys() {
        let k1 = EMPTY_KEY;
        let k2 = b"x";

        assert_eq!(compare_unsigned_bytes(k1, k2), Ordering::Less);
        assert_eq!(compare_unsigned_bytes(k1, k1), Ordering::Equal);
    }

    #[test]
    fn test_compare_with_unsigned_bytes() {
        // Test that bytes are treated as unsigned (0-255)
        let k1 = &[0xFE_u8];
        let k2 = &[0x01_u8];

        // 0xFE (254) > 0x01 (1) when treated as unsigned
        assert_eq!(compare_unsigned_bytes(k1, k2), Ordering::Greater);
    }

    #[test]
    fn test_get_key_prefix_length() {
        let k1 = b"abcdef";
        let k2 = b"abcxyz";

        assert_eq!(get_key_prefix_length(k1, k2), 3); // "abc"

        let k3 = b"xyz";
        assert_eq!(get_key_prefix_length(k1, k3), 0);

        let k4 = b"abcdef";
        assert_eq!(get_key_prefix_length(k1, k4), 6); // Full match
    }

    #[test]
    fn test_get_key_prefix_length_different_lengths() {
        let k1 = b"abc";
        let k2 = b"abcdef";

        assert_eq!(get_key_prefix_length(k1, k2), 3);
        assert_eq!(get_key_prefix_length(k2, k1), 3);
    }

    #[test]
    fn test_create_key_prefix() {
        let k1 = b"abcdef";
        let k2 = b"abcxyz";

        let prefix = create_key_prefix(k1, k2);
        assert_eq!(prefix, Some(b"abc".to_vec()));

        let k3 = b"xyz";
        let no_prefix = create_key_prefix(k1, k3);
        assert_eq!(no_prefix, None);
    }

    #[test]
    fn test_create_key_prefix_empty() {
        let k1 = EMPTY_KEY;
        let k2 = b"abc";

        assert_eq!(create_key_prefix(k1, k2), None);
        assert_eq!(create_key_prefix(k2, k1), None);
    }

    #[test]
    fn test_compare_keys_with_default() {
        let k1 = b"abc";
        let k2 = b"abd";

        assert_eq!(compare_keys(k1, k2, None), Ordering::Less);
    }

    #[test]
    fn test_compare_keys_with_custom_comparator() {
        struct ReverseComparator;

        impl KeyComparator for ReverseComparator {
            fn compare(&self, key1: &[u8], key2: &[u8]) -> Ordering {
                key2.cmp(key1) // Reverse order
            }
        }

        let k1 = b"abc";
        let k2 = b"abd";
        let cmp = ReverseComparator;

        // With reverse comparator, "abc" > "abd"
        assert_eq!(compare_keys(k1, k2, Some(&cmp)), Ordering::Greater);
    }

    #[test]
    fn test_default_comparator() {
        let cmp = DefaultComparator;
        let k1 = b"abc";
        let k2 = b"abd";

        assert_eq!(cmp.compare(k1, k2), Ordering::Less);
        assert_eq!(compare_keys(k1, k2, Some(&cmp)), Ordering::Less);
    }
}
