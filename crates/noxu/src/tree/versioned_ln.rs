//! Versioned LN  -  LN that preserves VLSN information.
//!
//!
//! VersionedLN is a subclass of LN that preserves VLSN information
//! for replication purposes. In Noxu DB, all LNs can hold VLSNs, so this
//! module provides a convenience constructor for creating versioned LNs.

use crate::tree::ln::Ln;
use crate::util::Vlsn;

/// Creates a versioned LN (one that preserves its VLSN).
///
/// VersionedLN is a subclass. In Noxu, all LNs can hold VLSNs,
/// so this is just a convenience constructor.
///
/// # Arguments
///
/// * `data` - The record data, or None for a deleted record
/// * `vlsn` - The VLSN to assign to this record version
pub fn make_versioned_ln(data: Option<Vec<u8>>, vlsn: Vlsn) -> Ln {
    let mut ln = Ln::new(data);
    ln.set_vlsn(vlsn);
    ln
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_make_versioned_ln() {
        let data = b"versioned data".to_vec();
        let vlsn = Vlsn::new(100);

        let ln = make_versioned_ln(Some(data.clone()), vlsn);

        assert_eq!(ln.get_data(), Some(data.as_slice()));
        assert_eq!(ln.get_vlsn().sequence(), 100);
        assert!(!ln.is_deleted());
    }

    #[test]
    fn test_make_versioned_ln_deleted() {
        let vlsn = Vlsn::new(200);

        let ln = make_versioned_ln(None, vlsn);

        assert!(ln.is_deleted());
        assert_eq!(ln.get_vlsn().sequence(), 200);
    }

    #[test]
    fn test_versioned_ln_serialization() {
        let data = b"test".to_vec();
        let vlsn = Vlsn::new(12345);

        let ln = make_versioned_ln(Some(data.clone()), vlsn);

        let mut buf = Vec::new();
        ln.write_to_log(&mut buf);

        let ln2 = Ln::read_from_log(&buf).unwrap();

        assert_eq!(ln2.get_data(), Some(data.as_slice()));
        assert_eq!(ln2.get_vlsn().sequence(), 12345);
    }
}
