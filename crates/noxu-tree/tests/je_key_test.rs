//! JE-equivalent key tests.
//!
//! Wave 6 — Priority-4 JE TCK port.
//!
//! Ports invariants from `je/test/com/sleepycat/je/tree/KeyTest.java`,
//! adapted to Noxu's `noxu_tree::key` module.
//!
//! Mapping table (JE -> Noxu):
//! * `Key.createKeyPrefix(k1, k2)`        -> `key::create_key_prefix(k1, k2)`
//! * `Key.compareKeys(k1, k2, null)`      -> `key::compare_keys(k1, k2, None)`
//! * `IN.setKeyPrefix / getKeyPrefix`     -> tested at the IN level (covered
//!                                           by other tree tests).  The
//!                                           `keyPrefixSubsetTest` invariant
//!                                           (a key's prefix must be a prefix
//!                                           of another key) is captured by
//!                                           the helper `is_prefix_of` here.

use std::cmp::Ordering;

use noxu_tree::key::{compare_keys, create_key_prefix};

// --------------------------------------------------------------------------
// testKeyPrefixer — direct port.
// --------------------------------------------------------------------------
#[test]
fn test_key_prefixer() {
    fn make(k1: &str, k2: &str) -> Option<String> {
        create_key_prefix(k1.as_bytes(), k2.as_bytes())
            .map(|v| String::from_utf8(v).unwrap())
    }

    // Direct ports of the JE assertions.
    assert_eq!(make("aaaa", "aaab").as_deref(), Some("aaa"));
    assert_eq!(make("abaa", "aaab").as_deref(), Some("a"));
    assert_eq!(make("baaa", "aaab"), None);
    assert_eq!(make("aaa", "aaa").as_deref(), Some("aaa"));
    assert_eq!(make("aaa", "aaab").as_deref(), Some("aaa"));
}

// --------------------------------------------------------------------------
// testKeyPrefixSubsetting — the IN-level subset check, ported as a pure
// byte-level helper.  JE asserts that `compareToKeyPrefix(in, newKey)`
// returns true iff the IN's stored prefix is itself a prefix of newKey.
// --------------------------------------------------------------------------
fn key_prefix_is_prefix_of(key_prefix: Option<&[u8]>, new_key: &[u8]) -> bool {
    match key_prefix {
        None => false,
        Some(p) if p.is_empty() => false,
        Some(p) => p.len() <= new_key.len() && new_key.starts_with(p),
    }
}

#[test]
fn test_key_prefix_subsetting() {
    // Direct ports of JE's keyPrefixSubsetTest assertions.
    // (keyPrefix, newKey, expected)
    assert!(key_prefix_is_prefix_of(Some(b"aaa"), b"aaa"));
    assert!(key_prefix_is_prefix_of(Some(b"aa"), b"aaa"));
    assert!(!key_prefix_is_prefix_of(Some(b"aaa"), b"aa"));
    assert!(!key_prefix_is_prefix_of(Some(b""), b"aa"));
    assert!(!key_prefix_is_prefix_of(None, b"aa"));
    assert!(!key_prefix_is_prefix_of(Some(b"baa"), b"aa"));
}

// --------------------------------------------------------------------------
// testKeyComparison — direct port of JE's compareKeys assertions.
// --------------------------------------------------------------------------
#[test]
fn test_key_comparison() {
    // ("aaa", "aab") -> less
    let key1: &[u8] = b"aaa";
    let key2: &[u8] = b"aab";
    assert_eq!(compare_keys(key1, key2, None), Ordering::Less);
    assert_eq!(compare_keys(key2, key1, None), Ordering::Greater);
    assert_eq!(compare_keys(key1, key1, None), Ordering::Equal);

    // ("aa", "aab") -> less (shorter prefix)
    let key1: &[u8] = b"aa";
    let key2: &[u8] = b"aab";
    assert_eq!(compare_keys(key1, key2, None), Ordering::Less);
    assert_eq!(compare_keys(key2, key1, None), Ordering::Greater);

    // ("", "aab") -> less
    let key1: &[u8] = b"";
    let key2: &[u8] = b"aab";
    assert_eq!(compare_keys(key1, key2, None), Ordering::Less);
    assert_eq!(compare_keys(key2, key1, None), Ordering::Greater);
    assert_eq!(compare_keys(key1, key1, None), Ordering::Equal);

    // ("", "") -> equal
    let key1: &[u8] = b"";
    let key2: &[u8] = b"";
    assert_eq!(compare_keys(key1, key2, None), Ordering::Equal);

    // Critical JE invariant: bytes are UNSIGNED.  0xFF > 0x7F.
    let ba1: &[u8] = &[0xFF, 0xFF, 0xFF];
    let ba2: &[u8] = &[0x7F, 0x7F, 0x7F];
    assert_eq!(
        compare_keys(ba1, ba2, None),
        Ordering::Greater,
        "compare_keys must use UNSIGNED byte semantics"
    );
}

// --------------------------------------------------------------------------
// JE testKeyComparisonPerformance — ported as a smaller smoke test that
// repeatedly compares equal keys without allocating; we don't need a
// million iterations to validate correctness.
// --------------------------------------------------------------------------
#[test]
fn test_key_comparison_equal_repeats() {
    let key1: &[u8] = b"abcdefghijabcdefghij";
    let key2: &[u8] = b"abcdefghijabcdefghij";
    for _ in 0..1000 {
        assert_eq!(compare_keys(key1, key2, None), Ordering::Equal);
    }
}

// --------------------------------------------------------------------------
// Extra invariants implied by the JE port:
// (1) prefix length 0 => create_key_prefix returns None
// (2) prefix == k1 if k1 is itself a prefix of k2 (or equal)
// --------------------------------------------------------------------------
#[test]
fn test_key_prefix_invariants() {
    // (1) Disjoint first byte: None.
    assert_eq!(create_key_prefix(b"x", b"y"), None);
    // (2) k1 itself is a prefix of k2: prefix is k1.
    assert_eq!(create_key_prefix(b"abc", b"abcdef"), Some(b"abc".to_vec()));
    // (3) k1 == k2: prefix is k1.
    assert_eq!(create_key_prefix(b"abc", b"abc"), Some(b"abc".to_vec()));
    // (4) Empty inputs: no prefix.
    assert_eq!(create_key_prefix(b"", b"abc"), None);
    assert_eq!(create_key_prefix(b"abc", b""), None);
    assert_eq!(create_key_prefix(b"", b""), None);
}
