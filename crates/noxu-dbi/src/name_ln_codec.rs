//! NameLN data-field codec for the persisted per-database metadata that
//! follows the 8-byte db_id: the DBI-14 comparator identities and the NEW-5
//! per-DB fanout (`maxTreeEntriesPerNode`).
//!
//! The NameLN record's `data` field maps a database name to its `db_id`.
//! Historically that field was exactly the 8-byte little-endian db_id.  DBI-14
//! appends the persisted comparator identities after it, faithfully to JE's
//! `DatabaseImpl.btreeComparatorBytes` / `duplicateComparatorBytes` (which
//! store the serialized comparator *class name*).  A Rust `Fn` has no portable
//! name, so Noxu persists the application-supplied identity string instead and
//! re-checks it at open (see `docs/src/maintainer/design-decisions.md`).
//!
//! NEW-5 appends the per-DB fanout after the comparator block, mirroring JE's
//! `DatabaseImpl.writeToLog` which serializes `maxTreeEntriesPerNode`
//! (DatabaseImpl.java:2134) and `readFromLog` which reads it back
//! (DatabaseImpl.java:2203).  JE only falls back to the environment-level
//! `NODE_MAX` default when the persisted field is zero (DatabaseImpl.java:420);
//! Noxu preserves that fallback for records that predate NEW-5 (no fanout in
//! the trailer) by decoding them to `None`.
//!
//! Layout (all integers little-endian):
//!
//! ```text
//!   [db_id: u64]                                (written by the caller)
//!   --- trailer (this module) ---
//!   [btree_id_len: u16][btree_id_bytes...]      (len 0 = no btree comparator)
//!   [dup_id_len:   u16][dup_id_bytes...]        (len 0 = no dup comparator)
//!   [FANOUT_TAG: u8 = 0xF0][fanout: i32]        (NEW-5, optional)
//! ```
//!
//! Backward compatibility of the trailer, from the shortest form up:
//!
//! * An EMPTY trailer (the pre-DBI-14 8-byte db_id-only record) decodes to
//!   `(None, None, None)`.
//! * A trailer ending right after the comparator block (a DBI-14 record with
//!   no NEW-5 fanout) decodes the comparators and leaves the fanout `None`.
//! * A trailer with the `FANOUT_TAG` sentinel after the comparator block
//!   (a NEW-5 record) additionally decodes the fanout.
//!
//! Because the fanout is guarded by a one-byte sentinel that never appears at
//! that position in the older formats (the older formats simply have no bytes
//! there), old WAL files remain readable and new WAL files add the fanout
//! without a global LOG_VERSION bump.

/// Sentinel byte marking the presence of a NEW-5 per-DB fanout field after
/// the comparator block.  Chosen to be distinct from any comparator-block
/// continuation: in the pre-NEW-5 formats there are simply no bytes at this
/// position, so a reader that sees this sentinel knows it is a NEW-5 record.
const FANOUT_TAG: u8 = 0xF0;

/// Encodes the optional comparator identities into the bytes that follow the
/// 8-byte db_id in a NameLN data field.  Returns an empty `Vec` when both
/// identities are absent, preserving the pre-DBI-14 wire format byte-for-byte.
///
/// Retained for callers that only need the comparator block (and for the
/// DBI-15 replication round-trip test).  New writers should prefer
/// [`encode_name_ln_trailer`], which also carries the NEW-5 fanout.
pub fn encode_comparator_ids(
    btree_id: Option<&str>,
    dup_id: Option<&str>,
) -> Vec<u8> {
    if btree_id.is_none() && dup_id.is_none() {
        return Vec::new();
    }
    encode_comparator_block(btree_id, dup_id)
}

/// Always emits the two length-prefixed comparator identities (each empty
/// string when its `Option` is `None`).  Used when a later field (the NEW-5
/// fanout) must follow, so the comparator block cannot be elided.
fn encode_comparator_block(
    btree_id: Option<&str>,
    dup_id: Option<&str>,
) -> Vec<u8> {
    let mut out = Vec::new();
    for id in [btree_id, dup_id] {
        let s = id.unwrap_or("");
        out.extend_from_slice(&(s.len() as u16).to_le_bytes());
        out.extend_from_slice(s.as_bytes());
    }
    out
}

/// Encodes the full NameLN trailer: the DBI-14 comparator identities and,
/// when `fanout` is `Some`, the NEW-5 per-DB fanout.
///
/// * `fanout == None` and both comparators absent → empty `Vec` (byte-for-byte
///   the pre-DBI-14 format).
/// * `fanout == None` with a comparator present → the DBI-14 comparator block
///   only (byte-for-byte the DBI-14 format).
/// * `fanout == Some(n)` → the comparator block (always emitted so the fanout
///   has a fixed starting offset) followed by `[FANOUT_TAG][n as i32]`.
///
/// A `Some(0)` fanout is treated as "no explicit fanout" and elided, matching
/// JE's zero-means-env-default convention (DatabaseImpl.java:420) so a database
/// that never overrode the fanout writes the historical format.
pub fn encode_name_ln_trailer(
    btree_id: Option<&str>,
    dup_id: Option<&str>,
    fanout: Option<i32>,
) -> Vec<u8> {
    let fanout = fanout.filter(|&n| n > 0);
    match fanout {
        None => encode_comparator_ids(btree_id, dup_id),
        Some(n) => {
            let mut out = encode_comparator_block(btree_id, dup_id);
            out.push(FANOUT_TAG);
            out.extend_from_slice(&n.to_le_bytes());
            out
        }
    }
}

/// Reads the two length-prefixed comparator identities from `trailer`,
/// returning them plus the offset of the first byte AFTER the comparator
/// block.  On a malformed trailer the offset is clamped to the trailer length
/// so no fanout is subsequently read.
fn read_comparator_block(
    trailer: &[u8],
) -> (Option<String>, Option<String>, usize) {
    if trailer.is_empty() {
        return (None, None, 0);
    }
    let mut off = 0usize;
    let mut read_one = || -> Option<String> {
        if off + 2 > trailer.len() {
            off = trailer.len();
            return None;
        }
        let len = u16::from_le_bytes([trailer[off], trailer[off + 1]]) as usize;
        off += 2;
        if len == 0 {
            return Some(String::new());
        }
        if off + len > trailer.len() {
            off = trailer.len();
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
    (btree, dup, off)
}

/// Decodes the comparator identities from the bytes following the db_id.
/// An empty slice (pre-DBI-14 format) decodes to `(None, None)`.  A malformed
/// trailer decodes conservatively to whatever could be read, then `None`.
///
/// Ignores any trailing NEW-5 fanout field; use [`decode_name_ln_trailer`] to
/// recover the fanout as well.
pub fn decode_comparator_ids(
    trailer: &[u8],
) -> (Option<String>, Option<String>) {
    let (btree, dup, _) = read_comparator_block(trailer);
    (btree, dup)
}

/// Decodes the full NameLN trailer: the DBI-14 comparator identities and the
/// optional NEW-5 per-DB fanout.
///
/// The fanout is `None` for pre-NEW-5 records (the comparator block ends the
/// trailer, or the trailer is empty), preserving today's env-default fallback
/// behaviour; it is `Some(n)` when the `FANOUT_TAG` sentinel and a valid i32
/// follow the comparator block.  A malformed / truncated fanout field decodes
/// to `None` (conservative fallback to the env default).
pub fn decode_name_ln_trailer(
    trailer: &[u8],
) -> (Option<String>, Option<String>, Option<i32>) {
    let (btree, dup, off) = read_comparator_block(trailer);
    let fanout = if off < trailer.len() && trailer[off] == FANOUT_TAG {
        let start = off + 1;
        if start + 4 <= trailer.len() {
            let n = i32::from_le_bytes([
                trailer[start],
                trailer[start + 1],
                trailer[start + 2],
                trailer[start + 3],
            ]);
            if n > 0 { Some(n) } else { None }
        } else {
            None
        }
    } else {
        None
    };
    (btree, dup, fanout)
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
        let _ = decode_name_ln_trailer(&bad); // must not panic
    }

    // ── NEW-5: fanout round-trips and backward compatibility ──────────────

    #[test]
    fn new5_fanout_only_round_trips() {
        // No comparators, explicit fanout.  The comparator block is emitted as
        // two zero-length strings so the fanout has a fixed offset.
        let enc = encode_name_ln_trailer(None, None, Some(4));
        assert_eq!(decode_name_ln_trailer(&enc), (None, None, Some(4)));
        // The comparator-only decoder still sees no comparators.
        assert_eq!(decode_comparator_ids(&enc), (None, None));
    }

    #[test]
    fn new5_fanout_with_comparators_round_trips() {
        let enc = encode_name_ln_trailer(Some("rev"), Some("le_u32"), Some(37));
        assert_eq!(
            decode_name_ln_trailer(&enc),
            (Some("rev".to_string()), Some("le_u32".to_string()), Some(37))
        );
        // Comparator-only decoder ignores the fanout tail.
        assert_eq!(
            decode_comparator_ids(&enc),
            (Some("rev".to_string()), Some("le_u32".to_string()))
        );
    }

    #[test]
    fn new5_zero_fanout_is_elided() {
        // A zero (env-default) fanout writes the historical format and decodes
        // back to None, so a DB that never overrode the fanout is byte-for-byte
        // the pre-NEW-5 record.
        assert!(encode_name_ln_trailer(None, None, Some(0)).is_empty());
        assert!(encode_name_ln_trailer(None, None, None).is_empty());
        assert_eq!(
            encode_name_ln_trailer(Some("rev"), None, Some(0)),
            encode_comparator_ids(Some("rev"), None)
        );
    }

    #[test]
    fn new5_pre_new5_records_decode_to_no_fanout() {
        // Empty trailer (pre-DBI-14): everything None.
        assert_eq!(decode_name_ln_trailer(&[]), (None, None, None));
        // DBI-14 comparator-only trailer (no fanout tag): fanout None.
        let dbi14 = encode_comparator_ids(Some("rev"), Some("dup"));
        assert_eq!(
            decode_name_ln_trailer(&dbi14),
            (Some("rev".to_string()), Some("dup".to_string()), None)
        );
    }

    #[test]
    fn new5_truncated_fanout_is_none() {
        // A trailer that has the fanout tag but a short (truncated) i32 must
        // decode to fanout None rather than panic or read garbage.
        let mut bad = encode_comparator_block(None, None);
        bad.push(FANOUT_TAG);
        bad.extend_from_slice(&[1u8, 2]); // only 2 of 4 bytes
        assert_eq!(decode_name_ln_trailer(&bad), (None, None, None));
    }

    // DBI-15: the comparator identity rides in the NameLN data field, which
    // the master sends verbatim and the replica writes verbatim to its own
    // WAL, then re-decodes through the same recovery scanner.  This proves
    // the full master-data-layout -> replica-decode round-trip end to end
    // (the same bytes both sides see on the wire).
    #[test]
    fn dbi15_master_nameln_data_layout_round_trips_to_replica_decode() {
        // Build the NameLN data EXACTLY as EnvironmentImpl::log_name_ln does.
        let db_id: u64 = 42;
        let mut data = db_id.to_le_bytes().to_vec();
        data.extend_from_slice(&encode_name_ln_trailer(
            Some("reverse"),
            Some("le_u32"),
            Some(64),
        ));

        // Replica recovery (file_manager_scanner) reads the first 8 bytes as
        // the db_id and decodes the trailer as the comparator identities +
        // fanout.
        assert!(data.len() >= 8);
        let decoded_id = u64::from_le_bytes(data[..8].try_into().unwrap());
        assert_eq!(decoded_id, db_id);
        let (btree, dup, fanout) = decode_name_ln_trailer(&data[8..]);
        assert_eq!(btree.as_deref(), Some("reverse"));
        assert_eq!(dup.as_deref(), Some("le_u32"));
        assert_eq!(fanout, Some(64));
    }
}
