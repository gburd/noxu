//! Benchmarks for noxu-log: entry header serialization, checksum, LSN, packed int.

#![allow(clippy::unit_arg)]

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use std::io::Cursor;

use noxu_log::{ChecksumValidator, LogEntryHeader, LogEntryType, Provisional};
use noxu_util::packed::{
    read_packed_i32, read_packed_i64, write_packed_i32, write_packed_i64,
};
use noxu_util::{Lsn, Vlsn};

// ---------------------------------------------------------------------------
// LogEntryHeader serialization benchmarks
// ---------------------------------------------------------------------------

fn bench_entry_header_serialize_no_vlsn(c: &mut Criterion) {
    let header = LogEntryHeader::new(
        LogEntryType::BIN,
        1024,
        Provisional::No,
        false,
        None,
    );

    c.bench_function("entry_header_serialize_no_vlsn", |b| {
        b.iter(|| {
            let mut buf = Vec::with_capacity(32);
            black_box(header.write_to_log(&mut buf).unwrap());
            black_box(buf.len());
        })
    });
}

fn bench_entry_header_serialize_with_vlsn(c: &mut Criterion) {
    let header = LogEntryHeader::new(
        LogEntryType::InsertLNTxn,
        512,
        Provisional::Yes,
        true,
        Some(Vlsn::new(42)),
    );

    c.bench_function("entry_header_serialize_with_vlsn", |b| {
        b.iter(|| {
            let mut buf = Vec::with_capacity(32);
            black_box(header.write_to_log(&mut buf).unwrap());
            black_box(buf.len());
        })
    });
}

// ---------------------------------------------------------------------------
// LogEntryHeader deserialization benchmarks
// ---------------------------------------------------------------------------

fn bench_entry_header_deserialize_no_vlsn(c: &mut Criterion) {
    let header = LogEntryHeader::new(
        LogEntryType::BIN,
        1024,
        Provisional::No,
        false,
        None,
    );
    let mut buf = Vec::new();
    header.write_to_log(&mut buf).unwrap();
    let lsn = Lsn::new(1, 100);

    c.bench_function("entry_header_deserialize_no_vlsn", |b| {
        b.iter(|| {
            black_box(
                LogEntryHeader::read_from_log(black_box(&buf), lsn).unwrap(),
            );
        })
    });
}

fn bench_entry_header_deserialize_with_vlsn(c: &mut Criterion) {
    let header = LogEntryHeader::new(
        LogEntryType::InsertLNTxn,
        512,
        Provisional::Yes,
        true,
        Some(Vlsn::new(99)),
    );
    let mut buf = Vec::new();
    header.write_to_log(&mut buf).unwrap();
    let lsn = Lsn::new(2, 200);

    c.bench_function("entry_header_deserialize_with_vlsn", |b| {
        b.iter(|| {
            black_box(
                LogEntryHeader::read_from_log(black_box(&buf), lsn).unwrap(),
            );
        })
    });
}

// ---------------------------------------------------------------------------
// Checksum benchmarks
// ---------------------------------------------------------------------------

fn bench_checksum_64b(c: &mut Criterion) {
    let data = vec![0xABu8; 64];
    c.bench_function("checksum_compute_64B", |b| {
        b.iter(|| black_box(ChecksumValidator::compute(black_box(&data))))
    });
}

fn bench_checksum_1kb(c: &mut Criterion) {
    let data = vec![0xCDu8; 1024];
    c.bench_function("checksum_compute_1KB", |b| {
        b.iter(|| black_box(ChecksumValidator::compute(black_box(&data))))
    });
}

fn bench_checksum_16kb(c: &mut Criterion) {
    let data = vec![0xEFu8; 16 * 1024];
    c.bench_function("checksum_compute_16KB", |b| {
        b.iter(|| black_box(ChecksumValidator::compute(black_box(&data))))
    });
}

// ---------------------------------------------------------------------------
// LSN pack/unpack benchmarks
// ---------------------------------------------------------------------------

fn bench_lsn_pack_unpack(c: &mut Criterion) {
    c.bench_function("lsn_pack_unpack", |b| {
        b.iter(|| {
            let lsn = Lsn::new(black_box(0xDEAD_u32), black_box(0xBEEF_u32));
            let raw = lsn.as_u64();
            let restored = Lsn::from_u64(raw);
            black_box(restored.file_number());
            black_box(restored.file_offset());
        })
    });
}

fn bench_lsn_new(c: &mut Criterion) {
    c.bench_function("lsn_new", |b| {
        b.iter(|| black_box(Lsn::new(black_box(42_u32), black_box(1024_u32))))
    });
}

fn bench_lsn_file_number(c: &mut Criterion) {
    let lsn = Lsn::new(42, 1024);
    c.bench_function("lsn_file_number", |b| {
        b.iter(|| black_box(lsn.file_number()))
    });
}

fn bench_lsn_file_offset(c: &mut Criterion) {
    let lsn = Lsn::new(42, 1024);
    c.bench_function("lsn_file_offset", |b| {
        b.iter(|| black_box(lsn.file_offset()))
    });
}

// ---------------------------------------------------------------------------
// Packed integer encode/decode benchmarks
// ---------------------------------------------------------------------------

fn bench_packed_i32_tiny(c: &mut Criterion) {
    // 1-byte tier: values in [-119, 119]
    c.bench_function("packed_i32_encode_decode_tiny (42)", |b| {
        let mut buf = Vec::with_capacity(8);
        b.iter(|| {
            buf.clear();
            write_packed_i32(&mut buf, black_box(42)).unwrap();
            let mut cur = Cursor::new(&buf);
            black_box(read_packed_i32(&mut cur).unwrap());
        })
    });
}

fn bench_packed_i32_medium(c: &mut Criterion) {
    // 2-byte tier
    c.bench_function("packed_i32_encode_decode_medium (10000)", |b| {
        let mut buf = Vec::with_capacity(8);
        b.iter(|| {
            buf.clear();
            write_packed_i32(&mut buf, black_box(10_000)).unwrap();
            let mut cur = Cursor::new(&buf);
            black_box(read_packed_i32(&mut cur).unwrap());
        })
    });
}

fn bench_packed_i32_large(c: &mut Criterion) {
    // 5-byte tier: i32::MAX
    c.bench_function("packed_i32_encode_decode_large (i32::MAX)", |b| {
        let mut buf = Vec::with_capacity(8);
        b.iter(|| {
            buf.clear();
            write_packed_i32(&mut buf, black_box(i32::MAX)).unwrap();
            let mut cur = Cursor::new(&buf);
            black_box(read_packed_i32(&mut cur).unwrap());
        })
    });
}

fn bench_packed_i64_tiny(c: &mut Criterion) {
    c.bench_function("packed_i64_encode_decode_tiny (42)", |b| {
        let mut buf = Vec::with_capacity(16);
        b.iter(|| {
            buf.clear();
            write_packed_i64(&mut buf, black_box(42i64)).unwrap();
            let mut cur = Cursor::new(&buf);
            black_box(read_packed_i64(&mut cur).unwrap());
        })
    });
}

fn bench_packed_i64_large(c: &mut Criterion) {
    c.bench_function("packed_i64_encode_decode_large (i64::MAX)", |b| {
        let mut buf = Vec::with_capacity(16);
        b.iter(|| {
            buf.clear();
            write_packed_i64(&mut buf, black_box(i64::MAX)).unwrap();
            let mut cur = Cursor::new(&buf);
            black_box(read_packed_i64(&mut cur).unwrap());
        })
    });
}

// ---------------------------------------------------------------------------
// Groups
// ---------------------------------------------------------------------------

criterion_group!(
    header_benches,
    bench_entry_header_serialize_no_vlsn,
    bench_entry_header_serialize_with_vlsn,
    bench_entry_header_deserialize_no_vlsn,
    bench_entry_header_deserialize_with_vlsn,
);

criterion_group!(
    checksum_benches,
    bench_checksum_64b,
    bench_checksum_1kb,
    bench_checksum_16kb,
);

criterion_group!(
    lsn_benches,
    bench_lsn_pack_unpack,
    bench_lsn_new,
    bench_lsn_file_number,
    bench_lsn_file_offset,
);

criterion_group!(
    packed_int_benches,
    bench_packed_i32_tiny,
    bench_packed_i32_medium,
    bench_packed_i32_large,
    bench_packed_i64_tiny,
    bench_packed_i64_large,
);

criterion_main!(
    header_benches,
    checksum_benches,
    lsn_benches,
    packed_int_benches
);
