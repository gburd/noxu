//! Property-based tests for noxu-log types (Hegel / hegeltest).

use hegel::generators;
use noxu_log::checksum::ChecksumValidator;
use noxu_log::entry_header::LogEntryHeader;
use noxu_log::entry_type::LogEntryType;
use noxu_log::log_utils;
use noxu_log::provisional::Provisional;
use noxu_util::{Lsn, Vlsn};
use std::io::Cursor;

/// Generator producing a valid LogEntryType.
#[hegel::composite]
fn arb_entry_type(tc: hegel::TestCase) -> LogEntryType {
    tc.draw(generators::sampled_from(vec![
        LogEntryType::FileHeader,
        LogEntryType::IN,
        LogEntryType::BIN,
        LogEntryType::BINDelta,
        LogEntryType::InsertLN,
        LogEntryType::UpdateLN,
        LogEntryType::DeleteLN,
        LogEntryType::InsertLNTxn,
        LogEntryType::UpdateLNTxn,
        LogEntryType::DeleteLNTxn,
        LogEntryType::MapLN,
        LogEntryType::NameLN,
        LogEntryType::NameLNTxn,
        LogEntryType::FileSummaryLN,
        LogEntryType::TxnCommit,
        LogEntryType::TxnAbort,
        LogEntryType::TxnPrepare,
        LogEntryType::CkptStart,
        LogEntryType::CkptEnd,
        LogEntryType::DbTree,
        LogEntryType::Trace,
        LogEntryType::Matchpoint,
    ]))
}

/// Generator producing a Provisional value.
#[hegel::composite]
fn arb_provisional(tc: hegel::TestCase) -> Provisional {
    tc.draw(generators::sampled_from(vec![
        Provisional::No,
        Provisional::Yes,
        Provisional::BeforeCkptEnd,
    ]))
}

// =============================================================================
// Log entry header property tests
// =============================================================================

/// Header write/read round-trip preserves entry_type, item_size,
/// provisional, and replicated fields (without VLSN).
#[hegel::test]
fn header_roundtrip_no_vlsn(tc: hegel::TestCase) {
    let entry_type = tc.draw(arb_entry_type());
    let item_size =
        tc.draw(generators::integers::<u32>().max_value(100_000_000 - 1));
    let provisional = tc.draw(arb_provisional());

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

    assert_eq!(header.entry_type(), decoded.entry_type());
    assert_eq!(header.item_size(), decoded.item_size());
    assert_eq!(header.provisional(), decoded.provisional());
    assert_eq!(header.replicated(), decoded.replicated());
}

/// Header round-trip with VLSN preserves all fields including the VLSN.
#[hegel::test]
fn header_roundtrip_with_vlsn(tc: hegel::TestCase) {
    let entry_type = tc.draw(arb_entry_type());
    let item_size =
        tc.draw(generators::integers::<u32>().max_value(100_000_000 - 1));
    let provisional = tc.draw(arb_provisional());
    let vlsn_seq = tc.draw(
        generators::integers::<i64>().min_value(1).max_value(i64::MAX - 1),
    );

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

    assert_eq!(header.entry_type(), decoded.entry_type());
    assert_eq!(header.item_size(), decoded.item_size());
    assert_eq!(header.provisional(), decoded.provisional());
    assert!(decoded.replicated());
    assert!(decoded.vlsn_present());
    assert_eq!(header.vlsn(), decoded.vlsn());
}

/// Header size is either MIN_HEADER_SIZE or MAX_HEADER_SIZE.
#[hegel::test]
fn header_size_consistent(tc: hegel::TestCase) {
    let entry_type = tc.draw(arb_entry_type());
    let item_size =
        tc.draw(generators::integers::<u32>().max_value(100_000_000 - 1));
    let replicated = tc.draw(generators::booleans());

    let vlsn = if replicated { Some(Vlsn::new(1)) } else { None };
    let header = LogEntryHeader::new(
        entry_type,
        item_size,
        Provisional::No,
        replicated,
        vlsn,
    );

    let mut buf = Vec::new();
    header.write_to_log(&mut buf).unwrap();

    // replicated or vlsn present => MAX_HEADER_SIZE, else MIN_HEADER_SIZE
    if replicated {
        assert_eq!(buf.len(), noxu_log::entry_header::MAX_HEADER_SIZE);
    } else {
        assert_eq!(buf.len(), noxu_log::entry_header::MIN_HEADER_SIZE);
    }
}

// =============================================================================
// Log utils integer encoding property tests
// =============================================================================

/// write_i32/read_i32 round-trip.
#[hegel::test]
fn log_utils_i32_roundtrip(tc: hegel::TestCase) {
    let val = tc.draw(generators::integers::<i32>());
    let mut buf = Vec::new();
    log_utils::write_i32(&mut buf, val).unwrap();
    assert_eq!(buf.len(), log_utils::INT_BYTES);
    let result = log_utils::read_i32(&mut Cursor::new(&buf)).unwrap();
    assert_eq!(result, val);
}

/// write_i64/read_i64 round-trip.
#[hegel::test]
fn log_utils_i64_roundtrip(tc: hegel::TestCase) {
    let val = tc.draw(generators::integers::<i64>());
    let mut buf = Vec::new();
    log_utils::write_i64(&mut buf, val).unwrap();
    assert_eq!(buf.len(), log_utils::LONG_BYTES);
    let result = log_utils::read_i64(&mut Cursor::new(&buf)).unwrap();
    assert_eq!(result, val);
}

/// write_u32/read_u32 round-trip.
#[hegel::test]
fn log_utils_u32_roundtrip(tc: hegel::TestCase) {
    let val = tc.draw(generators::integers::<u32>());
    let mut buf = Vec::new();
    log_utils::write_u32(&mut buf, val).unwrap();
    let result = log_utils::read_u32(&mut Cursor::new(&buf)).unwrap();
    assert_eq!(result, val);
}

/// write_i16/read_i16 round-trip.
#[hegel::test]
fn log_utils_i16_roundtrip(tc: hegel::TestCase) {
    let val = tc.draw(generators::integers::<i16>());
    let mut buf = Vec::new();
    log_utils::write_i16(&mut buf, val).unwrap();
    let result = log_utils::read_i16(&mut Cursor::new(&buf)).unwrap();
    assert_eq!(result, val);
}

/// Byte array round-trip with arbitrary data.
#[hegel::test]
fn log_utils_byte_array_roundtrip(tc: hegel::TestCase) {
    let data = tc.draw(generators::binary().max_size(255));
    let mut buf = Vec::new();
    log_utils::write_byte_array(&mut buf, Some(&data)).unwrap();
    let result = log_utils::read_byte_array(&mut Cursor::new(&buf)).unwrap();
    assert_eq!(result, Some(data));
}

/// String round-trip with arbitrary UTF-8 strings.
#[hegel::test]
fn log_utils_string_roundtrip(tc: hegel::TestCase) {
    let s = tc.draw(generators::text());
    let mut buf = Vec::new();
    log_utils::write_string(&mut buf, Some(&s)).unwrap();
    let result = log_utils::read_string(&mut Cursor::new(&buf)).unwrap();
    assert_eq!(result, Some(s));
}

/// Bool round-trip.
#[hegel::test]
fn log_utils_bool_roundtrip(tc: hegel::TestCase) {
    let val = tc.draw(generators::booleans());
    let mut buf = Vec::new();
    log_utils::write_bool(&mut buf, val).unwrap();
    let result = log_utils::read_bool(&mut Cursor::new(&buf)).unwrap();
    assert_eq!(result, val);
}

// =============================================================================
// CRC32 checksum property tests
// =============================================================================

/// Same data always produces the same checksum (determinism).
#[hegel::test]
fn checksum_deterministic(tc: hegel::TestCase) {
    let data = tc.draw(generators::binary().max_size(1023));
    let c1 = ChecksumValidator::compute(&data);
    let c2 = ChecksumValidator::compute(&data);
    assert_eq!(c1, c2);
}

/// Incremental checksum matches one-shot checksum.
#[hegel::test]
fn checksum_incremental_matches_oneshot(tc: hegel::TestCase) {
    let part1 = tc.draw(generators::binary().max_size(511));
    let part2 = tc.draw(generators::binary().max_size(511));

    let mut full = part1.clone();
    full.extend_from_slice(&part2);

    let oneshot = ChecksumValidator::compute(&full);

    let mut validator = ChecksumValidator::new();
    validator.update_all(&part1);
    validator.update_all(&part2);
    let incremental = validator.value();

    assert_eq!(oneshot, incremental);
}

/// Checksum of different data is (very likely) different.
/// We test this by flipping a byte -- not guaranteed but extremely likely.
#[hegel::test]
fn checksum_differs_on_modification(tc: hegel::TestCase) {
    let data = tc.draw(generators::binary().min_size(1).max_size(255));
    let flip_idx = tc.draw(generators::integers::<usize>().max_value(255));
    let idx = flip_idx % data.len();
    let original_checksum = ChecksumValidator::compute(&data);

    let mut modified = data.clone();
    modified[idx] ^= 0xFF; // flip all bits of one byte
    let modified_checksum = ChecksumValidator::compute(&modified);

    // If the byte was already 0xFF and we flip to 0x00 or vice versa,
    // the checksums should still differ in practice (CRC32 is good at this).
    // But this is probabilistic, not guaranteed for every input.
    if data[idx] != modified[idx] {
        assert_ne!(
            original_checksum, modified_checksum,
            "CRC32 collision: modifying byte {} from {:#x} to {:#x}",
            idx, data[idx], modified[idx]
        );
    }
}
