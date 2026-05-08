//! Version Log Sequence Number (VLSN).
//!
//! Version Log Sequence Number (VLSN).
//!
//! A VLSN identifies a replicated log entry by a monotonically increasing
//! sequence number. Used by the replication subsystem to track replication
//! progress and consistency.

use std::fmt;
use std::io::{self, Read, Write};

/// Serialized size of a VLSN in the log (8 bytes, stored as little-endian i64).
pub const VLSN_LOG_SIZE: usize = 8;

/// Sequence value representing a null (uninitialized) VLSN.
pub const NULL_VLSN_SEQUENCE: i64 = -1;

/// Sequence value representing an uninitialized VLSN field in log entries
/// (because the field did not exist in that version of the log or in a
/// non-HA commit/abort variant of a log entry).
pub const UNINITIALIZED_VLSN_SEQUENCE: i64 = 0;

/// A Version Log Sequence Number used for replication tracking.
///
/// Each replicated log entry is assigned a monotonically increasing VLSN
/// by the master node. Replicas use VLSNs to track their position in the
/// replication stream.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Vlsn {
    sequence: i64,
}

/// The null VLSN, representing an unset or invalid value.
pub const NULL_VLSN: Vlsn = Vlsn { sequence: NULL_VLSN_SEQUENCE };

/// The first valid VLSN sequence.
pub const FIRST_VLSN: Vlsn = Vlsn { sequence: 1 };

impl Vlsn {
    /// Creates a new VLSN with the given sequence number.
    #[inline]
    pub fn new(sequence: i64) -> Self {
        Vlsn { sequence }
    }

    /// Returns the sequence number.
    #[inline]
    pub fn sequence(self) -> i64 {
        self.sequence
    }

    /// Returns true if this is the null VLSN.
    #[inline]
    pub fn is_null(self) -> bool {
        self.sequence == NULL_VLSN_SEQUENCE
    }

    /// Returns true if the given raw sequence value represents a null VLSN.
    ///
    #[inline]
    pub fn is_null_seq(seq: i64) -> bool {
        seq == NULL_VLSN_SEQUENCE
    }

    /// Returns the VLSN that would follow this one.
    ///
    /// If this is NULL_VLSN, returns FIRST_VLSN.
    #[inline]
    pub fn next(self) -> Vlsn {
        if self.is_null() { FIRST_VLSN } else { Vlsn::new(self.sequence + 1) }
    }

    /// Returns the VLSN that would precede this one.
    ///
    /// If this is NULL_VLSN or FIRST_VLSN, returns NULL_VLSN.
    #[inline]
    pub fn prev(self) -> Vlsn {
        if self.is_null() || self.sequence == 1 {
            NULL_VLSN
        } else {
            Vlsn::new(self.sequence - 1)
        }
    }

    /// Returns true if this VLSN directly follows `other`.
    ///
    /// Handles the case where `other` is NULL_VLSN (this must be FIRST_VLSN).
    pub fn follows(self, other: Vlsn) -> bool {
        (other.is_null() && self.sequence == 1)
            || (!other.is_null() && other.sequence == self.sequence - 1)
    }

    /// Returns the smaller of two VLSNs, ignoring NULL_VLSN values if one
    /// value is not NULL_VLSN.
    pub fn min(a: Vlsn, b: Vlsn) -> Vlsn {
        if a.is_null() {
            return b;
        }
        if b.is_null() {
            return a;
        }
        if a <= b { a } else { b }
    }

    /// Writes this VLSN to the log as a little-endian i64 (8 bytes).
    pub fn write_to_log(self, w: &mut impl Write) -> io::Result<()> {
        w.write_all(&self.sequence.to_le_bytes())
    }

    /// Reads a VLSN from the log (little-endian i64, 8 bytes).
    pub fn read_from_log(r: &mut impl Read) -> io::Result<Vlsn> {
        let mut buf = [0u8; 8];
        r.read_exact(&mut buf)?;
        Ok(Vlsn::new(i64::from_le_bytes(buf)))
    }
}

impl Ord for Vlsn {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Both null => equal
        if self.sequence == NULL_VLSN_SEQUENCE
            && other.sequence == NULL_VLSN_SEQUENCE
        {
            return std::cmp::Ordering::Equal;
        }
        // Null is always less than any non-null
        if self.sequence == NULL_VLSN_SEQUENCE {
            return std::cmp::Ordering::Less;
        }
        if other.sequence == NULL_VLSN_SEQUENCE {
            return std::cmp::Ordering::Greater;
        }
        self.sequence.cmp(&other.sequence)
    }
}

impl PartialOrd for Vlsn {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl fmt::Debug for Vlsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_null() {
            write!(f, "Vlsn(NULL)")
        } else {
            write!(f, "Vlsn({})", self.sequence)
        }
    }
}

impl fmt::Display for Vlsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.sequence)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_null_vlsn() {
        assert!(NULL_VLSN.is_null());
        assert!(!FIRST_VLSN.is_null());
        assert_eq!(NULL_VLSN.sequence(), NULL_VLSN_SEQUENCE);
    }

    #[test]
    fn test_first_vlsn() {
        assert_eq!(FIRST_VLSN.sequence(), 1);
        assert!(!FIRST_VLSN.is_null());
    }

    #[test]
    fn test_is_null_seq_static() {
        assert!(Vlsn::is_null_seq(NULL_VLSN_SEQUENCE));
        assert!(Vlsn::is_null_seq(-1));
        assert!(!Vlsn::is_null_seq(0));
        assert!(!Vlsn::is_null_seq(1));
        assert!(!Vlsn::is_null_seq(i64::MAX));
    }

    #[test]
    fn test_next_prev() {
        assert_eq!(NULL_VLSN.next(), FIRST_VLSN);
        assert_eq!(FIRST_VLSN.next(), Vlsn::new(2));
        assert_eq!(Vlsn::new(100).next(), Vlsn::new(101));
        assert_eq!(FIRST_VLSN.prev(), NULL_VLSN);
        assert_eq!(Vlsn::new(5).prev(), Vlsn::new(4));
        assert_eq!(Vlsn::new(2).prev(), FIRST_VLSN);
        assert_eq!(NULL_VLSN.prev(), NULL_VLSN);
    }

    #[test]
    fn test_follows() {
        assert!(FIRST_VLSN.follows(NULL_VLSN));
        assert!(Vlsn::new(2).follows(FIRST_VLSN));
        assert!(Vlsn::new(100).follows(Vlsn::new(99)));
        assert!(!Vlsn::new(3).follows(FIRST_VLSN));
        assert!(!NULL_VLSN.follows(NULL_VLSN));
        assert!(!FIRST_VLSN.follows(FIRST_VLSN));
    }

    #[test]
    fn test_ordering() {
        assert!(NULL_VLSN < FIRST_VLSN);
        assert!(FIRST_VLSN < Vlsn::new(2));
        assert!(Vlsn::new(99) < Vlsn::new(100));
        assert_eq!(NULL_VLSN.cmp(&NULL_VLSN), std::cmp::Ordering::Equal);
        assert_eq!(Vlsn::new(5).cmp(&Vlsn::new(5)), std::cmp::Ordering::Equal);
        assert!(Vlsn::new(5) > NULL_VLSN);
    }

    #[test]
    fn test_ordering_strict_large_sequences() {
        // Large sequences sort correctly
        let a = Vlsn::new(1000);
        let b = Vlsn::new(1001);
        assert!(a < b);
        assert!(b > a);
        // NULL_VLSN is less than all positive sequences
        assert!(NULL_VLSN < Vlsn::new(1000));
        // NULL_VLSN is less than UNINITIALIZED (0)
        assert!(NULL_VLSN < Vlsn::new(UNINITIALIZED_VLSN_SEQUENCE));
    }

    #[test]
    fn test_min() {
        assert_eq!(Vlsn::min(NULL_VLSN, FIRST_VLSN), FIRST_VLSN);
        assert_eq!(Vlsn::min(FIRST_VLSN, NULL_VLSN), FIRST_VLSN);
        assert_eq!(Vlsn::min(Vlsn::new(3), Vlsn::new(5)), Vlsn::new(3));
        assert_eq!(Vlsn::min(Vlsn::new(5), Vlsn::new(3)), Vlsn::new(3));
        assert_eq!(Vlsn::min(NULL_VLSN, NULL_VLSN), NULL_VLSN);
        assert_eq!(Vlsn::min(Vlsn::new(10), Vlsn::new(10)), Vlsn::new(10));
    }

    #[test]
    fn test_write_and_read_from_log_roundtrip() {
        let vlsns = [NULL_VLSN, FIRST_VLSN, Vlsn::new(42), Vlsn::new(i64::MAX)];
        for &v in &vlsns {
            let mut buf = Vec::new();
            v.write_to_log(&mut buf).unwrap();
            assert_eq!(buf.len(), VLSN_LOG_SIZE);
            let decoded = Vlsn::read_from_log(&mut Cursor::new(&buf)).unwrap();
            assert_eq!(v, decoded, "roundtrip failed for {:?}", v);
        }
    }

    #[test]
    fn test_write_to_log_little_endian() {
        // NULL_VLSN sequence = -1, as i64 little-endian = FF FF FF FF FF FF FF FF
        let mut buf = Vec::new();
        NULL_VLSN.write_to_log(&mut buf).unwrap();
        assert_eq!(buf, vec![0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]);

        // FIRST_VLSN sequence = 1, as i64 little-endian = 01 00 00 00 00 00 00 00
        let mut buf = Vec::new();
        FIRST_VLSN.write_to_log(&mut buf).unwrap();
        assert_eq!(buf, vec![0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
    }

    #[test]
    fn test_read_from_log_null_vlsn() {
        // -1 as little-endian i64
        let buf = vec![0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF];
        let v = Vlsn::read_from_log(&mut Cursor::new(&buf)).unwrap();
        assert_eq!(v, NULL_VLSN);
        assert!(v.is_null());
    }

    #[test]
    fn test_display_and_debug() {
        assert_eq!(format!("{}", NULL_VLSN), "-1");
        assert_eq!(format!("{:?}", NULL_VLSN), "Vlsn(NULL)");
        assert_eq!(format!("{}", FIRST_VLSN), "1");
        assert_eq!(format!("{:?}", FIRST_VLSN), "Vlsn(1)");
    }

    #[test]
    fn test_vlsn_roundtrip() {
        // Basic new/sequence roundtrip
        let v = Vlsn::new(12345);
        assert_eq!(v.sequence(), 12345);
        assert!(!v.is_null());
    }
}
