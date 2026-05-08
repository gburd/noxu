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
    get_key(two_part_key)
        .map(|k| k == primary_key)
        .unwrap_or(false)
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
}
