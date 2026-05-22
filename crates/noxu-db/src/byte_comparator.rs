//! Byte-slice comparator interface for zero-allocation key comparisons.
//!
//!
//! In Java, a `Comparator<byte[]>` requires allocating a new byte array for
//! every comparison. `ByteComparator` avoids this by accepting offset + length
//! parameters into an existing array, matching how BIN slot keys are
//! stored.  In Rust the equivalent GC cost does not arise, but the interface
//! is preserved for structural fidelity.

use std::cmp::Ordering;

/// Key and duplicate-data comparator that avoids unnecessary allocations.
///
/// 
///
/// Implement this trait instead of a simple `Fn(&[u8], &[u8]) -> Ordering`
/// when the comparator needs to operate on sub-ranges of larger buffers
/// without constructing intermediate slices — for example when comparing
/// the BIN identifier key stored as `key[key_offset..key_offset+key_len]`.
///
/// # Default implementation
///
/// `ByteComparator::DEFAULT` provides unsigned lexicographic comparison,
/// equivalent to `Key::compareUnsignedBytes`.
///
/// # Example
///
/// ```
/// use noxu_db::ByteComparator;
///
/// struct ReverseComparator;
///
/// impl ByteComparator for ReverseComparator {
///     fn compare(
///         &self,
///         key1: &[u8], key1_offset: usize, key1_len: usize,
///         key2: &[u8], key2_offset: usize, key2_len: usize,
///     ) -> std::cmp::Ordering {
///         let s1 = &key1[key1_offset..key1_offset + key1_len];
///         let s2 = &key2[key2_offset..key2_offset + key2_len];
///         s2.cmp(s1)
///     }
/// }
/// ```
pub trait ByteComparator: Send + Sync {
    /// Compare two byte regions and return their ordering.
    ///
    /// `key1[key1_offset..key1_offset + key1_len]` is compared against
    /// `key2[key2_offset..key2_offset + key2_len]`.
    fn compare(
        &self,
        key1: &[u8],
        key1_offset: usize,
        key1_len: usize,
        key2: &[u8],
        key2_offset: usize,
        key2_len: usize,
    ) -> Ordering;
}

/// Default unsigned-lexicographic byte comparator.
///
/// 
pub struct DefaultByteComparator;

impl ByteComparator for DefaultByteComparator {
    fn compare(
        &self,
        key1: &[u8],
        key1_offset: usize,
        key1_len: usize,
        key2: &[u8],
        key2_offset: usize,
        key2_len: usize,
    ) -> Ordering {
        let s1 = &key1[key1_offset..key1_offset + key1_len];
        let s2 = &key2[key2_offset..key2_offset + key2_len];
        s1.cmp(s2)
    }
}

/// Convenience function: compare full byte slices using unsigned-byte order.
///
/// 
pub fn compare_unsigned(key1: &[u8], key2: &[u8]) -> Ordering {
    key1.cmp(key2)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_comparator() {
        let cmp = DefaultByteComparator;
        let k1 = b"abc";
        let k2 = b"abd";
        assert_eq!(
            cmp.compare(k1, 0, 3, k2, 0, 3),
            Ordering::Less
        );
        assert_eq!(
            cmp.compare(k2, 0, 3, k1, 0, 3),
            Ordering::Greater
        );
        assert_eq!(
            cmp.compare(k1, 0, 3, k1, 0, 3),
            Ordering::Equal
        );
    }

    #[test]
    fn test_subrange_compare() {
        let cmp = DefaultByteComparator;
        // "xabc" compared to "yabd", subrange [1..4]
        let buf1 = b"xabc";
        let buf2 = b"yabd";
        assert_eq!(
            cmp.compare(buf1, 1, 3, buf2, 1, 3),
            Ordering::Less
        );
    }

    #[test]
    fn test_unsigned_byte_order() {
        // Unsigned order: 0x80 > 0x7F
        let k1 = &[0x7Fu8];
        let k2 = &[0x80u8];
        assert_eq!(compare_unsigned(k1, k2), Ordering::Less);
    }
}
