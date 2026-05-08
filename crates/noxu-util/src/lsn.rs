//! Log Sequence Number (LSN) utilities.
//!
//! Log Sequence Number (LSN) — a u64 combining file number and byte offset.
//!
//! An LSN is a u64 comprised of a file number (upper 32 bits) and offset
//! within that file (lower 32 bits) which references a unique record in
//! the database environment log.

use std::fmt;

/// A Log Sequence Number referencing a position in the write-ahead log.
///
/// Encoded as: `file_number (32 bits) | file_offset (32 bits)`
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Lsn(u64);

/// Sentinel value representing an uninitialized or invalid LSN.
pub const NULL_LSN: Lsn = Lsn(u64::MAX);

/// File number used for transient (non-persistent) LSNs.
const MAX_FILE_NUM: u32 = u32::MAX;

impl Lsn {
    /// Creates a new LSN from a file number and offset.
    #[inline]
    pub fn new(file_number: u32, file_offset: u32) -> Self {
        Lsn((file_number as u64) << 32 | (file_offset as u64))
    }

    /// Creates a transient (non-persistent) LSN with the given offset.
    ///
    /// Transient LSNs use MAX_FILE_NUM and an ascending sequence of offsets.
    /// They are used for in-memory-only entries that have not been logged.
    #[inline]
    pub fn transient_lsn(offset: u32) -> Self {
        Self::new(MAX_FILE_NUM, offset)
    }

    /// Returns the file number component.
    #[inline]
    pub fn file_number(self) -> u32 {
        (self.0 >> 32) as u32
    }

    /// Returns the file offset component.
    #[inline]
    pub fn file_offset(self) -> u32 {
        self.0 as u32
    }

    /// Returns the raw u64 representation.
    #[inline]
    pub fn as_u64(self) -> u64 {
        self.0
    }

    /// Creates an LSN from a raw u64 value.
    #[inline]
    pub fn from_u64(val: u64) -> Self {
        Lsn(val)
    }

    /// Returns true if this is the null (uninitialized) LSN.
    #[inline]
    pub fn is_null(self) -> bool {
        self == NULL_LSN
    }

    /// Returns true if this is a transient (non-persistent) LSN.
    ///
    /// A transient LSN has a file number of MAX_FILE_NUM.
    #[inline]
    pub fn is_transient(self) -> bool {
        self.file_number() == MAX_FILE_NUM
    }

    /// Returns true if this LSN is either null or transient.
    #[inline]
    pub fn is_transient_or_null(self) -> bool {
        self.is_null() || self.is_transient()
    }

    /// Returns the approximate byte distance between two LSNs, assuming
    /// no log files have been cleaned (deleted).
    ///
    /// This is an approximation; the actual log may be slightly more or less.
    pub fn no_cleaning_distance(self, other: Lsn, log_file_size: u64) -> u64 {
        if self.is_null() {
            return 0;
        }

        let my_file = self.file_number() as u64;
        let other_file =
            if other.is_null() { 0 } else { other.file_number() as u64 };
        let other_offset =
            if other.is_null() { 0 } else { other.file_offset() as u64 };

        if my_file == other_file {
            let my_offset = self.file_offset() as u64;
            my_offset.abs_diff(other_offset)
        } else if my_file > other_file {
            Self::calc_diff(
                my_file - other_file,
                log_file_size,
                self.file_offset() as u64,
                other_offset,
            )
        } else {
            Self::calc_diff(
                other_file - my_file,
                log_file_size,
                other_offset,
                self.file_offset() as u64,
            )
        }
    }

    fn calc_diff(
        file_distance: u64,
        log_file_size: u64,
        later_offset: u64,
        earlier_offset: u64,
    ) -> u64 {
        file_distance * log_file_size + later_offset - earlier_offset
    }

    /// Returns the absolute difference between two LSNs in raw u64 space.
    ///
    /// This is the simple arithmetic difference of the underlying u64
    /// representation. Both LSNs must be non-null.
    ///
    /// For a distance that accounts for log file sizes, use
    /// `no_cleaning_distance`.
    pub fn distance(self, other: Lsn) -> u64 {
        self.0.abs_diff(other.0)
    }
}

impl Ord for Lsn {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // NULL_LSN is not valid for comparison — comparing against NULL_LSN panics.
        assert!(
            !self.is_null(),
            "Lsn::cmp: self is NULL_LSN -- invalid comparison"
        );
        assert!(
            !other.is_null(),
            "Lsn::cmp: other is NULL_LSN -- invalid comparison"
        );
        // Compare by file number first, then offset within file.
        let file_cmp = self.file_number().cmp(&other.file_number());
        if file_cmp != std::cmp::Ordering::Equal {
            return file_cmp;
        }
        self.file_offset().cmp(&other.file_offset())
    }
}

impl PartialOrd for Lsn {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl fmt::Debug for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_null() {
            write!(f, "Lsn(NULL)")
        } else {
            write!(
                f,
                "Lsn(0x{:x}/0x{:x})",
                self.file_number(),
                self.file_offset()
            )
        }
    }
}

impl fmt::Display for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_null() {
            write!(f, "NULL_LSN")
        } else {
            write!(f, "0x{:x}/0x{:x}", self.file_number(), self.file_offset())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_and_accessors() {
        let lsn = Lsn::new(0x1A, 0x2B00);
        assert_eq!(lsn.file_number(), 0x1A);
        assert_eq!(lsn.file_offset(), 0x2B00);
    }

    #[test]
    fn test_null_lsn() {
        assert!(NULL_LSN.is_null());
        assert!(!Lsn::new(0, 0).is_null());
    }

    #[test]
    fn test_transient_lsn() {
        let t = Lsn::transient_lsn(42);
        assert!(t.is_transient());
        assert!(!t.is_null());
        assert!(t.is_transient_or_null());
        assert_eq!(t.file_offset(), 42);
    }

    #[test]
    fn test_ordering() {
        let a = Lsn::new(1, 100);
        let b = Lsn::new(1, 200);
        let c = Lsn::new(2, 50);
        assert!(a < b);
        assert!(b < c);
        assert!(a < c);
    }

    #[test]
    fn test_roundtrip_u64() {
        let lsn = Lsn::new(0xDEAD, 0xBEEF);
        let raw = lsn.as_u64();
        assert_eq!(Lsn::from_u64(raw), lsn);
    }

    #[test]
    fn test_max_values() {
        let lsn = Lsn::new(u32::MAX - 1, u32::MAX);
        assert_eq!(lsn.file_number(), u32::MAX - 1);
        assert_eq!(lsn.file_offset(), u32::MAX);
        assert!(!lsn.is_transient());
    }

    #[test]
    #[should_panic(expected = "NULL_LSN")]
    fn test_cmp_null_lsn_self_panics() {
        let _ = NULL_LSN.cmp(&Lsn::new(1, 0));
    }

    #[test]
    #[should_panic(expected = "NULL_LSN")]
    fn test_cmp_null_lsn_other_panics() {
        let _ = Lsn::new(1, 0).cmp(&NULL_LSN);
    }

    #[test]
    #[should_panic(expected = "NULL_LSN")]
    fn test_partial_cmp_null_lsn_panics() {
        let a = Lsn::new(1, 0);
        let _ = a.partial_cmp(&NULL_LSN);
    }

    #[test]
    fn test_distance() {
        let a = Lsn::new(0, 100);
        let b = Lsn::new(0, 300);
        assert_eq!(a.no_cleaning_distance(b, 10_000_000), 200);

        let c = Lsn::new(2, 500);
        let d = Lsn::new(0, 200);
        // 2 files * 10MB + 500 - 200 = 20_000_300
        assert_eq!(c.no_cleaning_distance(d, 10_000_000), 20_000_300);
    }

    #[test]
    fn test_no_cleaning_distance_je_port() {
        let a = Lsn::new(1, 10);
        let b = Lsn::new(3, 40);
        // (3-1)*100 + 40 - 10 = 230
        assert_eq!(b.no_cleaning_distance(a, 100), 230);
        assert_eq!(a.no_cleaning_distance(b, 100), 230);

        let c = Lsn::new(1, 50);
        // same file: |50 - 10| = 40
        assert_eq!(a.no_cleaning_distance(c, 100), 40);
        assert_eq!(c.no_cleaning_distance(a, 100), 40);
    }

    #[test]
    fn test_no_cleaning_distance_null_other() {
        // When other is NULL_LSN, it is treated as file 0, offset 0
        let a = Lsn::new(2, 500);
        // (2-0)*10_000_000 + 500 - 0
        assert_eq!(a.no_cleaning_distance(NULL_LSN, 10_000_000), 20_000_500);
    }

    #[test]
    fn test_lsn_distance_simple() {
        let a = Lsn::new(0, 100);
        let b = Lsn::new(0, 200);
        // raw u64 difference
        assert_eq!(a.distance(b), 100);
        assert_eq!(b.distance(a), 100);
    }

    #[test]
    fn test_lsn_distance_cross_file() {
        let a = Lsn::new(1, 0);
        let b = Lsn::new(2, 0);
        // file 2 offset 0 raw = 2<<32; file 1 offset 0 raw = 1<<32
        assert_eq!(a.distance(b), 1u64 << 32);
    }

    #[test]
    fn test_lsn_distance_self() {
        let a = Lsn::new(5, 100);
        assert_eq!(a.distance(a), 0);
    }

    #[test]
    fn test_from_u64_null_lsn() {
        assert_eq!(Lsn::from_u64(u64::MAX), NULL_LSN);
        assert!(Lsn::from_u64(u64::MAX).is_null());
    }

    // Verify file_number and file_offset roundtrip for large values including 0xFFFFFFFF.
    #[test]
    fn test_je_large_values() {
        let values: &[u32] = &[0xFF, 0xFFFF, 0xFFFFFF, 0x7FFFFFFF];
        for &v in values {
            let lsn = Lsn::new(v, v);
            assert_eq!(lsn.file_number(), v, "file_number mismatch for 0x{:x}", v);
            assert_eq!(lsn.file_offset(), v, "file_offset mismatch for 0x{:x}", v);
        }
    }

    // Verify that higher file number produces a greater LSN.
    #[test]
    fn test_comparable_inequality_file_number() {
        let values: &[u32] = &[0xFF, 0xFFFF, 0xFFFFFF, 0x7FFFFFFF];
        for &v in values {
            let lsn1 = Lsn::new(v, v);
            let lsn2 = Lsn::new(0, v);
            assert!(lsn1 > lsn2, "expected lsn1 > lsn2 for 0x{:x}", v);
            assert!(lsn2 < lsn1);
        }
    }

    // Verify that higher file offset produces a greater LSN within the same file.
    #[test]
    fn test_comparable_inequality_file_offset() {
        let values: &[u32] = &[0xFF, 0xFFFF, 0xFFFFFF, 0x7FFFFFFF];
        for &v in values {
            let lsn1 = Lsn::new(v, v);
            let lsn2 = Lsn::new(v, 0);
            assert!(lsn1 > lsn2, "expected lsn1 > lsn2 for offset 0x{:x}", v);
            assert!(lsn2 < lsn1);
        }
    }

    #[test]
    fn test_display_and_debug_formats() {
        let lsn = Lsn::new(0xAB, 0xCD);
        assert!(format!("{}", lsn).contains("ab"));
        assert!(format!("{}", lsn).contains("cd"));
        assert!(format!("{:?}", lsn).contains("ab"));
        assert_eq!(format!("{}", NULL_LSN), "NULL_LSN");
        assert_eq!(format!("{:?}", NULL_LSN), "Lsn(NULL)");
    }

    #[test]
    fn test_transient_lsn_is_transient_or_null() {
        assert!(NULL_LSN.is_transient_or_null());
        assert!(Lsn::transient_lsn(0).is_transient_or_null());
        assert!(!Lsn::new(0, 0).is_transient_or_null());
    }

    // -----------------------------------------------------------------------
    // Verify NULL_LSN sentinel, and that Lsn::new(file, offset) stores and
    // returns both components correctly.
    // Test values: {0xFF, 0xFFFF, 0xFFFFFF, 0x7FFFFFFF, 0xFFFFFFFFL};
    // the last entry (0xFFFFFFFF for both file and offset)
    // produces NULL_LSN (u64::MAX) and is tested separately.
    // -----------------------------------------------------------------------

    #[test]
    fn test_make_entry_null_lsn_sentinel() {
        // NULL_LSN must equal Lsn::new(u32::MAX, u32::MAX) == u64::MAX
        assert_eq!(NULL_LSN.as_u64(), u64::MAX);
        assert!(NULL_LSN.is_null());
        // Lsn(0,0) must NOT be null
        assert!(!Lsn::new(0, 0).is_null());
    }

    #[test]
    fn test_make_entry_roundtrip() {
        // Test values (excluding 0xFFFFFFFF which creates NULL_LSN)
        let values: &[u32] = &[0xFF, 0xFFFF, 0xFFFFFF, 0x7FFF_FFFF];
        for &v in values {
            let lsn = Lsn::new(v, v);
            assert_eq!(
                lsn.file_number(),
                v,
                "file_number mismatch for value 0x{v:x}"
            );
            assert_eq!(
                lsn.file_offset(),
                v,
                "file_offset mismatch for value 0x{v:x}"
            );
        }
    }

    #[test]
    fn test_make_entry_null_lsn_from_max_components() {
        // 0xFFFFFFFF for both file and offset produces NULL_LSN
        let lsn = Lsn::new(u32::MAX, u32::MAX);
        assert!(lsn.is_null(), "Lsn::new(u32::MAX, u32::MAX) must be NULL_LSN");
        assert_eq!(lsn, NULL_LSN);
    }

    // -----------------------------------------------------------------------
    // compare(NULL, NULL), compare(LSN, NULL), compare(NULL, LSN) all panic.
    // compare(smaller, larger) and same-file different-offset comparisons.
    // -----------------------------------------------------------------------

    #[test]
    #[should_panic(expected = "NULL_LSN")]
    fn test_compare_null_null_panics() {
        let _ = NULL_LSN.cmp(&NULL_LSN);
    }

    #[test]
    #[should_panic(expected = "NULL_LSN")]
    fn test_compare_lsn_null_panics() {
        let lsn = Lsn::new(0, 0);
        let _ = lsn.cmp(&NULL_LSN);
    }

    #[test]
    #[should_panic(expected = "NULL_LSN")]
    fn test_compare_null_lsn_panics() {
        let lsn = Lsn::new(0, 0);
        let _ = NULL_LSN.cmp(&lsn);
    }

    #[test]
    fn test_compare_smaller_larger() {
        let smaller = Lsn::new(1, 0);
        let larger = Lsn::new(2, 0);
        assert!(smaller < larger);
        assert!(larger > smaller);
        assert_eq!(smaller.cmp(&smaller), std::cmp::Ordering::Equal);
    }

    #[test]
    fn test_compare_same_file_different_offset() {
        let lsn_lo = Lsn::new(5, 100);
        let lsn_hi = Lsn::new(5, 200);
        assert!(lsn_lo < lsn_hi);
        assert!(lsn_hi > lsn_lo);
        assert_eq!(lsn_lo.cmp(&lsn_lo), std::cmp::Ordering::Equal);
    }

    // -----------------------------------------------------------------------
    // The maximum valid file offset is u32::MAX - 1 (0xFFFFFFFE); offset
    // 0xFFFFFFFF is reserved (together with file 0xFFFFFFFF) for NULL_LSN.
    // -----------------------------------------------------------------------

    #[test]
    fn test_max_file_offset() {
        // Maximum file offset that can be stored without producing NULL_LSN
        // when combined with file_number = 0.
        let max_offset = u32::MAX - 1; // 0xFFFFFFFE
        let lsn = Lsn::new(0, max_offset);
        assert_eq!(lsn.file_number(), 0);
        assert_eq!(lsn.file_offset(), max_offset);
        assert!(!lsn.is_null());

        // Using u32::MAX as offset with file 0 is NOT null (only both == MAX is null)
        let lsn2 = Lsn::new(0, u32::MAX);
        assert!(!lsn2.is_null(), "only Lsn(MAX,MAX) is NULL_LSN");
        assert_eq!(lsn2.file_offset(), u32::MAX);
    }

    // -----------------------------------------------------------------------
    // Large file numbers must not corrupt the offset bits, and large offsets
    // must not corrupt the file number bits.
    // -----------------------------------------------------------------------

    #[test]
    fn test_overflow_large_file_number() {
        // A large file number must not bleed into the offset field.
        let large_file = 0x0FFF_FFFF_u32; // near the top but not MAX
        let offset = 0x0000_0042_u32;
        let lsn = Lsn::new(large_file, offset);
        assert_eq!(lsn.file_number(), large_file, "file_number corrupted");
        assert_eq!(lsn.file_offset(), offset, "offset corrupted by large file number");
    }

    #[test]
    fn test_overflow_large_offset() {
        // A large offset must not bleed into the file number field.
        let file = 0x0000_0001_u32;
        let large_offset = 0x7FFF_FFFF_u32;
        let lsn = Lsn::new(file, large_offset);
        assert_eq!(lsn.file_number(), file, "file_number corrupted by large offset");
        assert_eq!(lsn.file_offset(), large_offset, "offset corrupted");
    }

    #[test]
    fn test_overflow_independent_components() {
        // Verify file number and offset are stored in strictly separate bit fields.
        // file_number occupies bits [63:32], file_offset occupies bits [31:0].
        let lsn = Lsn::new(0xABCD_EF01, 0x1234_5678);
        assert_eq!(lsn.file_number(), 0xABCD_EF01);
        assert_eq!(lsn.file_offset(), 0x1234_5678);
        // Confirm raw encoding
        assert_eq!(
            lsn.as_u64(),
            0xABCD_EF01_1234_5678_u64,
            "raw u64 encoding is wrong"
        );
    }

    // -----------------------------------------------------------------------
    // Two LSNs constructed with the same file/offset must compare equal.
    // -----------------------------------------------------------------------

    #[test]
    fn test_comparable_equality() {
        let values: &[u32] = &[0xFF, 0xFFFF, 0xFFFFFF, 0x7FFF_FFFF];
        for &v in values {
            let lsn1 = Lsn::new(v, v);
            let lsn2 = Lsn::new(v, v);
            assert_eq!(lsn1, lsn2, "equality failed for 0x{v:x}");
            assert_eq!(
                lsn1.cmp(&lsn2),
                std::cmp::Ordering::Equal,
                "cmp equality failed for 0x{v:x}"
            );
        }
    }
}
