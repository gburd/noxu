//! Sorted-duplicate two-part key encoding.
//!
//! For databases with `sortedDuplicates=true`, each (key, data) pair is stored
//! as a single BIN slot using a composite two-part key:
//!
//!   `[key_bytes][data_bytes][reverse_packed_key_len]`
//!
//! The key length is stored at the END in "reverse-packed" format so that key
//! prefix compression applies to the primary-key prefix of the combined key.
//!
//! The custom comparator `cmp_two_part_keys` must be used for BIN searches:
//! it splits both keys and compares the primary part first, then the data part.
//!

/// Returns the number of bytes needed to encode `value` (non-negative) in
/// reverse-packed format.  Mirrors `PackedInteger.getWriteIntLength(value)`.
fn packed_int_len(value: usize) -> usize {
    if value <= 119 {
        1
    } else if value - 119 <= 0xFF {
        2
    } else if value - 119 <= 0xFFFF {
        3
    } else if value - 119 <= 0xFFFFFF {
        4
    } else {
        5
    }
}

/// Appends a reverse-packed non-negative integer to `buf` starting at
/// `start_off`.  Layout: big-endian value bytes then a marker byte.
///
///
fn write_packed_int_at(buf: &mut Vec<u8>, start_off: usize, value: usize) {
    let len = packed_int_len(value);
    if buf.len() < start_off + len {
        buf.resize(start_off + len, 0);
    }
    if value <= 119 {
        buf[start_off] = value as u8;
        return;
    }
    let abs_val = (value - 119) as u64;
    let marker_off = start_off + len - 1;
    match len {
        2 => {
            buf[start_off] = abs_val as u8;
            buf[marker_off] = 120;
        }
        3 => {
            buf[start_off] = (abs_val >> 8) as u8;
            buf[start_off + 1] = abs_val as u8;
            buf[marker_off] = 121;
        }
        4 => {
            buf[start_off] = (abs_val >> 16) as u8;
            buf[start_off + 1] = (abs_val >> 8) as u8;
            buf[start_off + 2] = abs_val as u8;
            buf[marker_off] = 122;
        }
        5 => {
            buf[start_off] = (abs_val >> 24) as u8;
            buf[start_off + 1] = (abs_val >> 16) as u8;
            buf[start_off + 2] = (abs_val >> 8) as u8;
            buf[start_off + 3] = abs_val as u8;
            buf[marker_off] = 123;
        }
        _ => unreachable!(),
    }
}

/// Reads the packed key length from the end of a two-part key buffer.
///
/// Returns `(key_size, packed_len_bytes)` where `packed_len_bytes` is the
/// number of bytes consumed by the encoding at the end of `buf`.
///
///
fn read_packed_int_from_end(buf: &[u8]) -> Option<(usize, usize)> {
    if buf.is_empty() {
        return None;
    }
    let marker = buf[buf.len() - 1];
    let marker_i = marker as i8;
    if (0..=119).contains(&marker_i) {
        return Some((marker as usize, 1));
    }
    // Positive multi-byte: marker is 120–123.
    if !(120..=123).contains(&marker) {
        return None; // negative or out of range — invalid for key sizes
    }
    let byte_len = (marker - 119) as usize; // number of value bytes before marker
    let total_len = byte_len + 1;
    if buf.len() < total_len {
        return None;
    }
    let val_start = buf.len() - total_len;
    let mut abs_val: u64 = 0;
    for i in 0..byte_len {
        abs_val = (abs_val << 8) | (buf[val_start + i] as u64);
    }
    let key_size = abs_val as usize + 119;
    Some((key_size, total_len))
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Combines primary key and data into a two-part key for sorted-dup storage.
///
/// Format: `[key_bytes][data_bytes][packed_key_len]`
///
///
pub fn combine(key: &[u8], data: &[u8]) -> Vec<u8> {
    let size_len = packed_int_len(key.len());
    let total = key.len() + data.len() + size_len;
    let mut buf = Vec::with_capacity(total);
    buf.extend_from_slice(key);
    buf.extend_from_slice(data);
    buf.resize(total, 0);
    write_packed_int_at(&mut buf, key.len() + data.len(), key.len());
    buf
}

/// Splits a two-part key into `(primary_key, data)`.
///
/// Returns `None` if the buffer is malformed.
///
///
pub fn split(two_part_key: &[u8]) -> Option<(Vec<u8>, Vec<u8>)> {
    let (key_size, size_len) = read_packed_int_from_end(two_part_key)?;
    let data_end = two_part_key.len().checked_sub(size_len)?;
    if key_size > data_end {
        return None;
    }
    let key = two_part_key[..key_size].to_vec();
    let data = two_part_key[key_size..data_end].to_vec();
    Some((key, data))
}

/// Returns the primary key portion of a two-part key (no allocation of data).
pub fn get_key(two_part_key: &[u8]) -> Option<Vec<u8>> {
    let (key_size, size_len) = read_packed_int_from_end(two_part_key)?;
    let data_end = two_part_key.len().checked_sub(size_len)?;
    if key_size > data_end {
        return None;
    }
    Some(two_part_key[..key_size].to_vec())
}

/// Returns `combine(key, b"")` — the smallest two-part key for the given
/// primary key.  Used as a lower-bound search key to position the cursor at
/// the first duplicate of `key`.
pub fn lower_bound(key: &[u8]) -> Vec<u8> {
    combine(key, b"")
}

/// Returns true if `two_part_key` belongs to `primary_key`.
pub fn matches_key(two_part_key: &[u8], primary_key: &[u8]) -> bool {
    get_key(two_part_key).map(|k| k == primary_key).unwrap_or(false)
}

/// Compares two two-part keys using separate primary-key and data comparators.
///
/// 1. Extract and compare primary-key parts.
/// 2. If equal, compare data parts.
///
///
pub fn cmp_two_part_keys<K, D>(
    a: &[u8],
    b: &[u8],
    key_cmp: K,
    data_cmp: D,
) -> std::cmp::Ordering
where
    K: Fn(&[u8], &[u8]) -> std::cmp::Ordering,
    D: Fn(&[u8], &[u8]) -> std::cmp::Ordering,
{
    let (a_key, a_data) = match split(a) {
        Some(kd) => kd,
        None => return std::cmp::Ordering::Equal,
    };
    let (b_key, b_data) = match split(b) {
        Some(kd) => kd,
        None => return std::cmp::Ordering::Equal,
    };
    match key_cmp(&a_key, &b_key) {
        std::cmp::Ordering::Equal => data_cmp(&a_data, &b_data),
        ord => ord,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_combine_split_round_trip() {
        let key = b"hello";
        let data = b"world";
        let combined = combine(key, data);
        let (k, d) = split(&combined).unwrap();
        assert_eq!(k, key);
        assert_eq!(d, data);
    }

    #[test]
    fn test_combine_empty_data() {
        let key = b"abc";
        let combined = combine(key, b"");
        let (k, d) = split(&combined).unwrap();
        assert_eq!(k, key);
        assert_eq!(d, b"");
    }

    #[test]
    fn test_combine_empty_key() {
        let combined = combine(b"", b"data");
        let (k, d) = split(&combined).unwrap();
        assert_eq!(k, b"");
        assert_eq!(d, b"data");
    }

    #[test]
    fn test_lower_bound() {
        let lb = lower_bound(b"abc");
        let (k, d) = split(&lb).unwrap();
        assert_eq!(k, b"abc");
        assert_eq!(d, b"");
    }

    #[test]
    fn test_matches_key() {
        let two_part = combine(b"abc", b"xyz");
        assert!(matches_key(&two_part, b"abc"));
        assert!(!matches_key(&two_part, b"ab"));
        assert!(!matches_key(&two_part, b"abcd"));
    }

    #[test]
    fn test_cmp_two_part_keys_different_keys() {
        let a = combine(b"aaa", b"xyz");
        let b = combine(b"bbb", b"abc");
        let cmp = cmp_two_part_keys(&a, &b, |x, y| x.cmp(y), |x, y| x.cmp(y));
        assert_eq!(cmp, std::cmp::Ordering::Less);
    }

    #[test]
    fn test_cmp_two_part_keys_same_key_diff_data() {
        let a = combine(b"key", b"aaa");
        let b = combine(b"key", b"bbb");
        let cmp = cmp_two_part_keys(&a, &b, |x, y| x.cmp(y), |x, y| x.cmp(y));
        assert_eq!(cmp, std::cmp::Ordering::Less);
    }

    #[test]
    fn test_cmp_two_part_keys_equal() {
        let a = combine(b"key", b"data");
        let b = combine(b"key", b"data");
        let cmp = cmp_two_part_keys(&a, &b, |x, y| x.cmp(y), |x, y| x.cmp(y));
        assert_eq!(cmp, std::cmp::Ordering::Equal);
    }

    #[test]
    fn test_cmp_correctness_prefix_ambiguity() {
        // Key "a" with data "bc" vs key "ab" with data "c".
        // Correct order: "a" < "ab", so first < second.
        let a = combine(b"a", b"bc");
        let b = combine(b"ab", b"c");
        let cmp = cmp_two_part_keys(&a, &b, |x, y| x.cmp(y), |x, y| x.cmp(y));
        assert_eq!(cmp, std::cmp::Ordering::Less);
        // Note: lexicographic comparison of raw bytes would give the wrong answer here.
    }

    #[test]
    fn test_large_key_round_trip() {
        // Key length > 119 requires multi-byte packed int.
        let key = vec![b'k'; 200];
        let data = b"data";
        let combined = combine(&key, data);
        let (k, d) = split(&combined).unwrap();
        assert_eq!(k, key);
        assert_eq!(d, data);
    }

    // ── packed-integer encoding: every width and every boundary ──────────
    //
    // The reverse-packed length prefix has five widths selected by hard-coded
    // thresholds (119, 0xFF, 0xFFFF, 0xFFFFFF). Only the 1-byte width was
    // exercised, because real primary keys are short. But `combine` writes this
    // prefix on EVERY two-part key, so a wrong threshold or a wrong marker byte
    // silently corrupts the split of any key long enough to cross into the next
    // width -- and the corruption surfaces as a dup cursor returning the wrong
    // primary key, not as a decode error.

    /// The round-trip property, stated once and checked at every boundary:
    /// `split(combine(k, d)) == (k, d)` regardless of how long `k` is.
    fn assert_round_trips(key_len: usize) {
        let key: Vec<u8> = (0..key_len).map(|i| (i % 251) as u8).collect();
        let data = b"payload".to_vec();
        let combined = combine(&key, &data);

        let (k2, d2) = split(&combined)
            .unwrap_or_else(|| panic!("split failed for a {key_len}-byte key"));
        assert_eq!(k2, key, "key corrupted at length {key_len}");
        assert_eq!(d2, data, "data corrupted at length {key_len}");

        // The no-allocation accessor must agree with the full split.
        assert_eq!(
            get_key(&combined).unwrap(),
            key,
            "get_key disagrees with split at length {key_len}"
        );
        assert!(matches_key(&combined, &key));
        assert!(!matches_key(&combined, b"definitely-not-this-key"));
    }

    #[test]
    fn packed_length_round_trips_across_every_encoding_width() {
        // 1-byte width, and its last value.
        for n in [0, 1, 2, 119] {
            assert_round_trips(n);
        }
        // Each threshold, one below and one above, so an off-by-one in a
        // boundary shows up as a specific failing length.
        for n in [120, 121, 119 + 0xFF, 119 + 0x100, 119 + 0xFFFF] {
            assert_round_trips(n);
        }
    }

    /// The encoded length must be exactly what `packed_int_len` promises at
    /// every width. `combine` sizes its buffer from that function, so a
    /// disagreement between the predicted length and the bytes actually
    /// written would either truncate the key or leave a zero gap in it.
    #[test]
    fn the_predicted_and_actual_encoded_lengths_agree_at_every_width() {
        for (value, want_len) in [
            (0usize, 1usize),
            (119, 1),
            (120, 2),
            (119 + 0xFF, 2),
            (119 + 0x100, 3),
            (119 + 0xFFFF, 3),
            (119 + 0x1_0000, 4),
            (119 + 0xFF_FFFF, 4),
            (119 + 0x100_0000, 5),
        ] {
            assert_eq!(
                packed_int_len(value),
                want_len,
                "packed_int_len({value}) picked the wrong width"
            );

            let mut buf = Vec::new();
            write_packed_int_at(&mut buf, 0, value);
            assert_eq!(
                buf.len(),
                want_len,
                "write_packed_int_at({value}) wrote {} bytes but \
                 packed_int_len promised {want_len}",
                buf.len()
            );
            assert_eq!(
                read_packed_int_from_end(&buf),
                Some((value, want_len)),
                "the {want_len}-byte encoding of {value} did not decode back"
            );
        }
    }

    /// `write_packed_int_at` must honour a non-zero offset and must extend the
    /// buffer only as far as it needs. `combine` relies on writing the prefix
    /// after the key and data, so an offset bug would overwrite the payload.
    #[test]
    fn write_packed_int_at_respects_a_non_zero_offset() {
        let mut buf = b"KEYDATA".to_vec();
        let at = buf.len();
        write_packed_int_at(&mut buf, at, 3);
        assert_eq!(
            &buf[..at],
            b"KEYDATA",
            "writing the length prefix must not disturb the payload"
        );
        assert_eq!(read_packed_int_from_end(&buf), Some((3, 1)));
    }

    // ── malformed input: split must decline, not panic or lie ────────────

    /// A dup BIN slot is read straight off disk, so `split` is a parser on
    /// untrusted bytes. It must return `None` for every malformed shape rather
    /// than panicking (which would abort a cursor scan) or returning a
    /// plausible-looking wrong key (which would silently hand the caller
    /// another record's data).
    #[test]
    fn split_declines_malformed_buffers_instead_of_panicking_or_lying() {
        // Empty: nothing to read the marker from.
        assert_eq!(split(b""), None);
        assert_eq!(get_key(b""), None);

        // A marker claiming a key longer than the buffer holds.
        //  buf = [0xFF] means key_size = 255 with a 1-byte prefix, but there
        //  are zero bytes of key+data available.
        assert_eq!(
            split(&[0xFFu8]),
            None,
            "a key size larger than the buffer must be rejected"
        );

        // Negative / out-of-range marker (>=124 reads as a negative i8 or an
        // unsupported width): not a valid key size.
        for marker in [124u8, 200, 255] {
            let buf = vec![0u8, 0u8, marker];
            assert_eq!(
                read_packed_int_from_end(&buf),
                None,
                "marker {marker} is not a valid key-size encoding"
            );
            assert_eq!(split(&buf), None);
        }

        // A multi-byte marker whose value bytes are truncated.
        assert_eq!(
            read_packed_int_from_end(&[123u8]),
            None,
            "a 5-byte encoding needs 4 value bytes before its marker"
        );
        assert_eq!(read_packed_int_from_end(&[0u8, 122u8]), None);

        // A well-formed prefix whose key_size overruns the payload.
        let mut buf = b"ab".to_vec();
        write_packed_int_at(&mut buf, 2, 99); // claims a 99-byte key
        assert_eq!(
            split(&buf),
            None,
            "key_size must be validated against the available payload"
        );
        assert_eq!(get_key(&buf), None, "get_key must apply the same check");
    }

    /// The empty-data case is the `lower_bound` seek key, so it must round-trip
    /// exactly -- it is what positions a cursor on the first duplicate of a key.
    #[test]
    fn lower_bound_is_the_smallest_two_part_key_for_its_primary() {
        let key = b"pkey";
        let lb = lower_bound(key);
        assert_eq!(split(&lb).unwrap(), (key.to_vec(), Vec::new()));
        assert!(matches_key(&lb, key));

        // And it must sort at or before every real duplicate of that key.
        for data in [&b"a"[..], &b"zzz"[..], &[0u8][..]] {
            let full = combine(key, data);
            assert!(
                cmp_two_part_keys(
                    &lb,
                    &full,
                    |x: &[u8], y: &[u8]| x.cmp(y),
                    |x: &[u8], y: &[u8]| x.cmp(y),
                ) != std::cmp::Ordering::Greater,
                "lower_bound must not sort after a real duplicate"
            );
        }
    }

    /// An empty primary key is legal (key_size 0) and must not be confused with
    /// a malformed buffer -- `None` and `Some(empty)` are different answers.
    #[test]
    fn an_empty_primary_key_round_trips_rather_than_reading_as_malformed() {
        let combined = combine(b"", b"data");
        assert_eq!(split(&combined).unwrap(), (Vec::new(), b"data".to_vec()));
        assert_eq!(get_key(&combined), Some(Vec::new()));
        assert!(matches_key(&combined, b""));
        assert!(!matches_key(&combined, b"x"));
    }
}
