#![no_main]

//! Round-trip fuzz test for DatabaseEntry serialization.
//!
//! Feeds random bytes into DatabaseEntry and verifies:
//! - No panics on construction from arbitrary data.
//! - Round-trip: from_bytes(data).data_opt() == data.
//! - Offset/size manipulation never panics and stays within bounds.
//! - Partial entry configuration never panics.
//! - from_vec, from_data, From<&[u8]>, From<String>, From<&str> all work.

use libfuzzer_sys::fuzz_target;

use arbitrary::Arbitrary;
use noxu_db::DatabaseEntry;

#[derive(Debug, Arbitrary)]
struct FuzzInput {
    /// Raw data bytes.
    data: Vec<u8>,
    /// Offset to set (may be out of bounds -- that is fine).
    offset: usize,
    /// Size to set (may be out of bounds -- that is fine).
    size: usize,
    /// Whether to enable partial mode.
    partial: bool,
    /// Partial offset.
    partial_offset: usize,
    /// Partial length.
    partial_length: usize,
}

fuzz_target!(|input: FuzzInput| {
    // -- Basic round-trip --
    let entry = DatabaseEntry::from_bytes(&input.data);
    if let Some(got) = entry.data_opt() {
        assert_eq!(got, input.data.as_slice(), "from_bytes round-trip failed");
    }

    // -- from_vec round-trip --
    let entry_vec = DatabaseEntry::from_vec(input.data.clone());
    assert_eq!(
        entry_vec.data_opt().map(|d| d.to_vec()),
        entry.data_opt().map(|d| d.to_vec()),
        "from_vec != from_bytes"
    );

    // -- from_data alias --
    let entry_data = DatabaseEntry::from_data(&input.data);
    assert_eq!(
        entry_data.data_opt().map(|d| d.to_vec()),
        entry.data_opt().map(|d| d.to_vec()),
        "from_data != from_bytes"
    );

    // -- From trait impls --
    let entry_from_slice: DatabaseEntry = input.data.as_slice().into();
    assert_eq!(
        entry_from_slice.data_opt().map(|d| d.to_vec()),
        entry.data_opt().map(|d| d.to_vec()),
        "From<&[u8]> mismatch"
    );

    let entry_from_vec: DatabaseEntry = input.data.clone().into();
    assert_eq!(
        entry_from_vec.data_opt().map(|d| d.to_vec()),
        entry.data_opt().map(|d| d.to_vec()),
        "From<Vec<u8>> mismatch"
    );

    // -- Offset/size manipulation (should never panic) --
    let mut entry_mut = entry.clone();
    entry_mut.set_offset(input.offset);
    entry_mut.set_size(input.size);
    // get_data should not panic regardless of offset/size
    let _ = entry_mut.data_opt();
    let _ = entry_mut.data();

    // -- Partial configuration (should never panic) --
    let mut entry_partial = entry.clone();
    entry_partial.set_partial(
        input.partial_offset,
        input.partial_length,
        input.partial,
    );
    assert_eq!(entry_partial.is_partial(), input.partial);
    assert_eq!(entry_partial.partial_offset(), input.partial_offset);
    assert_eq!(entry_partial.partial_length(), input.partial_length);

    // -- set_data round-trip --
    let mut entry_set = DatabaseEntry::new();
    assert!(entry_set.is_empty());
    entry_set.set_data(&input.data);
    if let Some(got) = entry_set.data_opt() {
        assert_eq!(got, input.data.as_slice(), "set_data round-trip failed");
    }

    // -- set_data_vec round-trip --
    let mut entry_set_vec = DatabaseEntry::new();
    entry_set_vec.set_data_vec(input.data.clone());
    if let Some(got) = entry_set_vec.data_opt() {
        assert_eq!(
            got,
            input.data.as_slice(),
            "set_data_vec round-trip failed"
        );
    }

    // -- clear --
    let mut entry_clear = DatabaseEntry::from_bytes(&input.data);
    entry_clear.clear();
    assert!(entry_clear.is_empty());
    assert_eq!(entry_clear.data_opt(), None);

    // -- Clone + Eq --
    let cloned = entry.clone();
    assert_eq!(entry, cloned);

    // -- From<String> --
    if let Ok(s) = std::str::from_utf8(&input.data) {
        let entry_from_str: DatabaseEntry = s.into();
        assert_eq!(
            entry_from_str.data_opt().unwrap(),
            input.data.as_slice(),
            "From<&str> mismatch"
        );
        let entry_from_string: DatabaseEntry = s.to_string().into();
        assert_eq!(
            entry_from_string.data_opt().unwrap(),
            input.data.as_slice(),
            "From<String> mismatch"
        );
    }
});
