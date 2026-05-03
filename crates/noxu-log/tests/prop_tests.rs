//! Property-based tests for noxu-log types.

use noxu_log::checksum::ChecksumValidator;
use noxu_log::entry_header::LogEntryHeader;
use noxu_log::entry_type::LogEntryType;
use noxu_log::log_utils;
use noxu_log::provisional::Provisional;
use noxu_util::{Lsn, Vlsn};
use proptest::prelude::*;
use std::io::Cursor;

/// Strategy to generate a valid LogEntryType.
fn arb_entry_type() -> impl Strategy<Value = LogEntryType> {
    prop_oneof![
        Just(LogEntryType::FileHeader),
        Just(LogEntryType::IN),
        Just(LogEntryType::BIN),
        Just(LogEntryType::BINDelta),
        Just(LogEntryType::InsertLN),
        Just(LogEntryType::UpdateLN),
        Just(LogEntryType::DeleteLN),
        Just(LogEntryType::InsertLNTxn),
        Just(LogEntryType::UpdateLNTxn),
        Just(LogEntryType::DeleteLNTxn),
        Just(LogEntryType::MapLN),
        Just(LogEntryType::NameLN),
        Just(LogEntryType::NameLNTxn),
        Just(LogEntryType::FileSummaryLN),
        Just(LogEntryType::TxnCommit),
        Just(LogEntryType::TxnAbort),
        Just(LogEntryType::TxnPrepare),
        Just(LogEntryType::CkptStart),
        Just(LogEntryType::CkptEnd),
        Just(LogEntryType::DbTree),
        Just(LogEntryType::Trace),
        Just(LogEntryType::Matchpoint),
    ]
}

/// Strategy to generate a Provisional value.
fn arb_provisional() -> impl Strategy<Value = Provisional> {
    prop_oneof![
        Just(Provisional::No),
        Just(Provisional::Yes),
        Just(Provisional::BeforeCkptEnd),
    ]
}

// =============================================================================
// Log entry header property tests
// =============================================================================

proptest! {
    /// Header write/read round-trip preserves entry_type, item_size,
    /// provisional, and replicated fields (without VLSN).
    #[test]
    fn header_roundtrip_no_vlsn(
        entry_type in arb_entry_type(),
        item_size in 0u32..100_000_000u32,
        provisional in arb_provisional(),
    ) {
        let header = LogEntryHeader::new(
            entry_type,
            item_size,
            provisional,
            false, // not replicated
            None,  // no VLSN
        );

        let mut buf = Vec::new();
        header.write_to_log(&mut buf).unwrap();

        let lsn = Lsn::new(0, 0);
        let decoded = LogEntryHeader::read_from_log(&buf, lsn).unwrap();

        prop_assert_eq!(header.entry_type(), decoded.entry_type());
        prop_assert_eq!(header.item_size(), decoded.item_size());
        prop_assert_eq!(header.provisional(), decoded.provisional());
        prop_assert_eq!(header.replicated(), decoded.replicated());
    }

    /// Header round-trip with VLSN preserves all fields including the VLSN.
    #[test]
    fn header_roundtrip_with_vlsn(
        entry_type in arb_entry_type(),
        item_size in 0u32..100_000_000u32,
        provisional in arb_provisional(),
        vlsn_seq in 1i64..i64::MAX,
    ) {
        let vlsn = Some(Vlsn::new(vlsn_seq));
        let header = LogEntryHeader::new(
            entry_type,
            item_size,
            provisional,
            true, // replicated
            vlsn,
        );

        let mut buf = Vec::new();
        header.write_to_log(&mut buf).unwrap();

        let lsn = Lsn::new(0, 0);
        let decoded = LogEntryHeader::read_from_log(&buf, lsn).unwrap();

        prop_assert_eq!(header.entry_type(), decoded.entry_type());
        prop_assert_eq!(header.item_size(), decoded.item_size());
        prop_assert_eq!(header.provisional(), decoded.provisional());
        prop_assert!(decoded.replicated());
        prop_assert!(decoded.vlsn_present());
        prop_assert_eq!(header.vlsn(), decoded.vlsn());
    }

    /// Header size is either MIN_HEADER_SIZE or MAX_HEADER_SIZE.
    #[test]
    fn header_size_consistent(
        entry_type in arb_entry_type(),
        item_size in 0u32..100_000_000u32,
        replicated: bool,
    ) {
        let vlsn = if replicated { Some(Vlsn::new(1)) } else { None };
        let header = LogEntryHeader::new(
            entry_type, item_size, Provisional::No, replicated, vlsn,
        );

        let mut buf = Vec::new();
        header.write_to_log(&mut buf).unwrap();

        // replicated or vlsn present => MAX_HEADER_SIZE, else MIN_HEADER_SIZE
        if replicated {
            prop_assert_eq!(buf.len(), noxu_log::entry_header::MAX_HEADER_SIZE);
        } else {
            prop_assert_eq!(buf.len(), noxu_log::entry_header::MIN_HEADER_SIZE);
        }
    }
}

// =============================================================================
// Log utils integer encoding property tests
// =============================================================================

proptest! {
    /// write_i32/read_i32 round-trip.
    #[test]
    fn log_utils_i32_roundtrip(val: i32) {
        let mut buf = Vec::new();
        log_utils::write_i32(&mut buf, val).unwrap();
        prop_assert_eq!(buf.len(), log_utils::INT_BYTES);
        let result = log_utils::read_i32(&mut Cursor::new(&buf)).unwrap();
        prop_assert_eq!(result, val);
    }

    /// write_i64/read_i64 round-trip.
    #[test]
    fn log_utils_i64_roundtrip(val: i64) {
        let mut buf = Vec::new();
        log_utils::write_i64(&mut buf, val).unwrap();
        prop_assert_eq!(buf.len(), log_utils::LONG_BYTES);
        let result = log_utils::read_i64(&mut Cursor::new(&buf)).unwrap();
        prop_assert_eq!(result, val);
    }

    /// write_u32/read_u32 round-trip.
    #[test]
    fn log_utils_u32_roundtrip(val: u32) {
        let mut buf = Vec::new();
        log_utils::write_u32(&mut buf, val).unwrap();
        let result = log_utils::read_u32(&mut Cursor::new(&buf)).unwrap();
        prop_assert_eq!(result, val);
    }

    /// write_i16/read_i16 round-trip.
    #[test]
    fn log_utils_i16_roundtrip(val: i16) {
        let mut buf = Vec::new();
        log_utils::write_i16(&mut buf, val).unwrap();
        let result = log_utils::read_i16(&mut Cursor::new(&buf)).unwrap();
        prop_assert_eq!(result, val);
    }

    /// Byte array round-trip with arbitrary data.
    #[test]
    fn log_utils_byte_array_roundtrip(data in prop::collection::vec(any::<u8>(), 0..256)) {
        let mut buf = Vec::new();
        log_utils::write_byte_array(&mut buf, Some(&data)).unwrap();
        let result = log_utils::read_byte_array(&mut Cursor::new(&buf)).unwrap();
        prop_assert_eq!(result, Some(data));
    }

    /// String round-trip with arbitrary UTF-8 strings.
    #[test]
    fn log_utils_string_roundtrip(s in ".*") {
        let mut buf = Vec::new();
        log_utils::write_string(&mut buf, Some(&s)).unwrap();
        let result = log_utils::read_string(&mut Cursor::new(&buf)).unwrap();
        prop_assert_eq!(result, Some(s));
    }

    /// Bool round-trip.
    #[test]
    fn log_utils_bool_roundtrip(val: bool) {
        let mut buf = Vec::new();
        log_utils::write_bool(&mut buf, val).unwrap();
        let result = log_utils::read_bool(&mut Cursor::new(&buf)).unwrap();
        prop_assert_eq!(result, val);
    }
}

// =============================================================================
// CRC32 checksum property tests
// =============================================================================

proptest! {
    /// Same data always produces the same checksum (determinism).
    #[test]
    fn checksum_deterministic(data in prop::collection::vec(any::<u8>(), 0..1024)) {
        let c1 = ChecksumValidator::compute(&data);
        let c2 = ChecksumValidator::compute(&data);
        prop_assert_eq!(c1, c2);
    }

    /// Incremental checksum matches one-shot checksum.
    #[test]
    fn checksum_incremental_matches_oneshot(
        part1 in prop::collection::vec(any::<u8>(), 0..512),
        part2 in prop::collection::vec(any::<u8>(), 0..512),
    ) {
        let mut full = part1.clone();
        full.extend_from_slice(&part2);

        let oneshot = ChecksumValidator::compute(&full);

        let mut validator = ChecksumValidator::new();
        validator.update_all(&part1);
        validator.update_all(&part2);
        let incremental = validator.value();

        prop_assert_eq!(oneshot, incremental);
    }

    /// Checksum of different data is (very likely) different.
    /// We test this by flipping a byte -- not guaranteed but extremely likely.
    #[test]
    fn checksum_differs_on_modification(
        data in prop::collection::vec(any::<u8>(), 1..256),
        flip_idx in 0usize..256,
    ) {
        let idx = flip_idx % data.len();
        let original_checksum = ChecksumValidator::compute(&data);

        let mut modified = data.clone();
        modified[idx] ^= 0xFF; // flip all bits of one byte
        let modified_checksum = ChecksumValidator::compute(&modified);

        // If the byte was already 0xFF and we flip to 0x00 or vice versa,
        // the checksums should still differ in practice (CRC32 is good at this).
        // But this is probabilistic, not guaranteed for every input.
        if data[idx] != modified[idx] {
            prop_assert_ne!(original_checksum, modified_checksum,
                "CRC32 collision: modifying byte {} from {:#x} to {:#x}",
                idx, data[idx], modified[idx]);
        }
    }
}
