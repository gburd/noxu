//! NameLN data-field codec for the DBI-14 comparator identities.
//!
//! The NameLN record's `data` field maps a database name to its `db_id`.
//! Historically that field was exactly the 8-byte little-endian db_id.  DBI-14
//! appends the persisted comparator identities after it, faithfully to JE's
//! `DatabaseImpl.btreeComparatorBytes` / `duplicateComparatorBytes` (which
//! store the serialized comparator *class name*).  A Rust `Fn` has no portable
//! name, so Noxu persists the application-supplied identity string instead and
//! re-checks it at open (see `docs/src/maintainer/design-decisions.md`).
//!
//! Layout (all integers little-endian):
//!
//! ```text
//!   [db_id: u64]
//!   [btree_id_len: u16][btree_id_bytes...]      (len 0 = no btree comparator)
//!   [dup_id_len:   u16][dup_id_bytes...]        (len 0 = no dup comparator)
//! ```
//!
//! The trailing comparator block is optional: a record that ends right after
//! the db_id (the pre-DBI-14 format) decodes to `(None, None)`, so old WAL
//! files remain readable.

/// Encodes the optional comparator identities into the bytes that follow the
/// 8-byte db_id in a NameLN data field.  Returns an empty `Vec` when both
/// identities are absent, preserving the pre-DBI-14 wire format byte-for-byte.
pub fn encode_comparator_ids(
    btree_id: Option<&str>,
    dup_id: Option<&str>,
) -> Vec<u8> {
    if btree_id.is_none() && dup_id.is_none() {
        return Vec::new();
    }
    let mut out = Vec::new();
    for id in [btree_id, dup_id] {
        let s = id.unwrap_or("");
        out.extend_from_slice(&(s.len() as u16).to_le_bytes());
        out.extend_from_slice(s.as_bytes());
    }
    out
}

/// Decodes the comparator identities from the bytes following the db_id.
/// An empty slice (pre-DBI-14 format) decodes to `(None, None)`.  A malformed
/// trailer decodes conservatively to whatever could be read, then `None`.
pub fn decode_comparator_ids(
    trailer: &[u8],
) -> (Option<String>, Option<String>) {
    if trailer.is_empty() {
        return (None, None);
    }
    let mut off = 0usize;
    let mut read_one = || -> Option<String> {
        if off + 2 > trailer.len() {
            return None;
        }
        let len = u16::from_le_bytes([trailer[off], trailer[off + 1]]) as usize;
        off += 2;
        if len == 0 {
            return Some(String::new());
        }
        if off + len > trailer.len() {
            return None;
        }
        let s = String::from_utf8(trailer[off..off + len].to_vec()).ok();
        off += len;
        s
    };
    // A zero-length identity means "explicitly no comparator of this kind"
    // (still distinct from the absent trailer), so map empty string -> None.
    let btree = read_one().filter(|s| !s.is_empty());
    let dup = read_one().filter(|s| !s.is_empty());
    (btree, dup)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_both() {
        let enc = encode_comparator_ids(Some("rev"), Some("le_u32"));
        assert_eq!(
            decode_comparator_ids(&enc),
            (Some("rev".to_string()), Some("le_u32".to_string()))
        );
    }

    #[test]
    fn round_trip_btree_only() {
        let enc = encode_comparator_ids(Some("rev"), None);
        assert_eq!(
            decode_comparator_ids(&enc),
            (Some("rev".to_string()), None)
        );
    }

    #[test]
    fn round_trip_dup_only() {
        let enc = encode_comparator_ids(None, Some("d"));
        assert_eq!(decode_comparator_ids(&enc), (None, Some("d".to_string())));
    }

    #[test]
    fn pre_dbi14_format_is_none_none() {
        // Empty trailer (old 8-byte db_id-only record).
        assert_eq!(decode_comparator_ids(&[]), (None, None));
        // No identities -> empty encoding.
        assert!(encode_comparator_ids(None, None).is_empty());
    }

    #[test]
    fn malformed_trailer_is_safe() {
        // Length claims 5 bytes but only 1 present.
        let bad = vec![5u8, 0, b'x'];
        let _ = decode_comparator_ids(&bad); // must not panic
    }
}
