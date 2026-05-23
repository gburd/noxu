//! Benchmarks for noxu-util: LSN, VLSN, packed integer encoding, CRC32 vs CRC32C.

#![allow(clippy::unit_arg)]

use criterion::{
    BenchmarkId, Criterion, Throughput, black_box, criterion_group,
    criterion_main,
};
use std::io::Cursor;

use noxu_util::lsn::Lsn;
use noxu_util::packed::{
    read_packed_i32, read_packed_i64, read_sorted_i32, read_sorted_i64,
    write_packed_i32, write_packed_i64, write_sorted_i32, write_sorted_i64,
};
use noxu_util::vlsn::Vlsn;

// ---------------------------------------------------------------------------
// LSN benchmarks
// ---------------------------------------------------------------------------

fn bench_lsn_new(c: &mut Criterion) {
    c.bench_function("lsn_new", |b| {
        b.iter(|| black_box(Lsn::new(black_box(42), black_box(1024))))
    });
}

fn bench_lsn_from_u64(c: &mut Criterion) {
    c.bench_function("lsn_from_u64", |b| {
        b.iter(|| black_box(Lsn::from_u64(black_box(0x0000_002A_0000_0400))))
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

fn bench_lsn_roundtrip(c: &mut Criterion) {
    c.bench_function("lsn_roundtrip", |b| {
        b.iter(|| {
            let lsn = Lsn::new(black_box(0xDEAD), black_box(0xBEEF));
            let raw = lsn.as_u64();
            let restored = Lsn::from_u64(raw);
            black_box(restored.file_number());
            black_box(restored.file_offset());
        })
    });
}

fn bench_lsn_comparison(c: &mut Criterion) {
    let a = Lsn::new(1, 100);
    let b = Lsn::new(2, 50);
    c.bench_function("lsn_comparison", |b_iter| {
        b_iter.iter(|| black_box(a < b))
    });
}

fn bench_lsn_no_cleaning_distance(c: &mut Criterion) {
    let a = Lsn::new(2, 500);
    let b = Lsn::new(0, 200);
    c.bench_function("lsn_no_cleaning_distance", |bench| {
        bench.iter(|| black_box(a.no_cleaning_distance(b, 10_000_000)))
    });
}

// ---------------------------------------------------------------------------
// VLSN benchmarks
// ---------------------------------------------------------------------------

fn bench_vlsn_new(c: &mut Criterion) {
    c.bench_function("vlsn_new", |b| {
        b.iter(|| black_box(Vlsn::new(black_box(42))))
    });
}

fn bench_vlsn_next(c: &mut Criterion) {
    let v = Vlsn::new(100);
    c.bench_function("vlsn_next", |b| b.iter(|| black_box(v.next())));
}

fn bench_vlsn_prev(c: &mut Criterion) {
    let v = Vlsn::new(100);
    c.bench_function("vlsn_prev", |b| b.iter(|| black_box(v.prev())));
}

fn bench_vlsn_comparison(c: &mut Criterion) {
    let a = Vlsn::new(10);
    let b = Vlsn::new(20);
    c.bench_function("vlsn_comparison", |bench| {
        bench.iter(|| black_box(a < b))
    });
}

fn bench_vlsn_min(c: &mut Criterion) {
    let a = Vlsn::new(10);
    let b = Vlsn::new(20);
    c.bench_function("vlsn_min", |bench| {
        bench.iter(|| black_box(Vlsn::min(a, b)))
    });
}

// ---------------------------------------------------------------------------
// Packed integer benchmarks
// ---------------------------------------------------------------------------

fn bench_write_packed_i32_small(c: &mut Criterion) {
    c.bench_function("write_packed_i32_small (42)", |b| {
        let mut buf = Vec::with_capacity(16);
        b.iter(|| {
            buf.clear();
            black_box(write_packed_i32(&mut buf, black_box(42)).unwrap());
        })
    });
}

fn bench_write_packed_i32_medium(c: &mut Criterion) {
    c.bench_function("write_packed_i32_medium (10000)", |b| {
        let mut buf = Vec::with_capacity(16);
        b.iter(|| {
            buf.clear();
            black_box(write_packed_i32(&mut buf, black_box(10_000)).unwrap());
        })
    });
}

fn bench_write_packed_i32_large(c: &mut Criterion) {
    c.bench_function("write_packed_i32_large (i32::MAX)", |b| {
        let mut buf = Vec::with_capacity(16);
        b.iter(|| {
            buf.clear();
            black_box(write_packed_i32(&mut buf, black_box(i32::MAX)).unwrap());
        })
    });
}

fn bench_read_packed_i32_small(c: &mut Criterion) {
    let mut buf = Vec::new();
    write_packed_i32(&mut buf, 42).unwrap();
    c.bench_function("read_packed_i32_small (42)", |b| {
        b.iter(|| {
            let mut cursor = Cursor::new(&buf);
            black_box(read_packed_i32(&mut cursor).unwrap());
        })
    });
}

fn bench_read_packed_i32_large(c: &mut Criterion) {
    let mut buf = Vec::new();
    write_packed_i32(&mut buf, i32::MAX).unwrap();
    c.bench_function("read_packed_i32_large (i32::MAX)", |b| {
        b.iter(|| {
            let mut cursor = Cursor::new(&buf);
            black_box(read_packed_i32(&mut cursor).unwrap());
        })
    });
}

fn bench_write_packed_i64_small(c: &mut Criterion) {
    c.bench_function("write_packed_i64_small (42)", |b| {
        let mut buf = Vec::with_capacity(16);
        b.iter(|| {
            buf.clear();
            black_box(write_packed_i64(&mut buf, black_box(42)).unwrap());
        })
    });
}

fn bench_write_packed_i64_large(c: &mut Criterion) {
    c.bench_function("write_packed_i64_large (i64::MAX)", |b| {
        let mut buf = Vec::with_capacity(16);
        b.iter(|| {
            buf.clear();
            black_box(write_packed_i64(&mut buf, black_box(i64::MAX)).unwrap());
        })
    });
}

fn bench_read_packed_i64_small(c: &mut Criterion) {
    let mut buf = Vec::new();
    write_packed_i64(&mut buf, 42).unwrap();
    c.bench_function("read_packed_i64_small (42)", |b| {
        b.iter(|| {
            let mut cursor = Cursor::new(&buf);
            black_box(read_packed_i64(&mut cursor).unwrap());
        })
    });
}

fn bench_read_packed_i64_large(c: &mut Criterion) {
    let mut buf = Vec::new();
    write_packed_i64(&mut buf, i64::MAX).unwrap();
    c.bench_function("read_packed_i64_large (i64::MAX)", |b| {
        b.iter(|| {
            let mut cursor = Cursor::new(&buf);
            black_box(read_packed_i64(&mut cursor).unwrap());
        })
    });
}

fn bench_write_sorted_i32(c: &mut Criterion) {
    c.bench_function("write_sorted_i32", |b| {
        let mut buf = Vec::with_capacity(16);
        b.iter(|| {
            buf.clear();
            black_box(write_sorted_i32(&mut buf, black_box(42)).unwrap());
        })
    });
}

fn bench_read_sorted_i32(c: &mut Criterion) {
    let mut buf = Vec::new();
    write_sorted_i32(&mut buf, 42).unwrap();
    c.bench_function("read_sorted_i32", |b| {
        b.iter(|| {
            let mut cursor = Cursor::new(&buf);
            black_box(read_sorted_i32(&mut cursor).unwrap());
        })
    });
}

fn bench_write_sorted_i64(c: &mut Criterion) {
    c.bench_function("write_sorted_i64", |b| {
        let mut buf = Vec::with_capacity(16);
        b.iter(|| {
            buf.clear();
            black_box(write_sorted_i64(&mut buf, black_box(42)).unwrap());
        })
    });
}

fn bench_read_sorted_i64(c: &mut Criterion) {
    let mut buf = Vec::new();
    write_sorted_i64(&mut buf, 42).unwrap();
    c.bench_function("read_sorted_i64", |b| {
        b.iter(|| {
            let mut cursor = Cursor::new(&buf);
            black_box(read_sorted_i64(&mut cursor).unwrap());
        })
    });
}

// ---------------------------------------------------------------------------
// CRC32 (Ethernet / zlib polynomial) vs CRC32C (Castagnoli / iSCSI / NVMe)
//
// CRC32C has hardware acceleration (SSE4.2 on x86-64, crc32 instruction on
// ARMv8.1) reaching ~20 GB/s; the software fallback is ~500 MB/s.
// CRC32 (crc32fast) uses SIMD acceleration where available (~3–5 GB/s).
//
// Payload sizes mirror typical replication feeder frames:
//   64 B  — heartbeat / CBVLSN datagram
//   256 B — small key-value LN
//   1 KB  — typical BIN-delta
//   4 KB  — full BIN / log page
//  64 KB  — large restore chunk
// ---------------------------------------------------------------------------

const CHECKSUM_SIZES: &[usize] = &[64, 256, 1024, 4096, 65536];

fn bench_checksums(c: &mut Criterion) {
    let mut group = c.benchmark_group("checksum");

    for &size in CHECKSUM_SIZES {
        let data: Vec<u8> = (0..size).map(|i| (i & 0xFF) as u8).collect();
        group.throughput(Throughput::Bytes(size as u64));

        group.bench_with_input(
            BenchmarkId::new("crc32_ethernet", size),
            &data,
            |b, d| {
                b.iter(|| {
                    let mut h = crc32fast::Hasher::new();
                    h.update(black_box(d));
                    black_box(h.finalize())
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new("crc32c_castagnoli", size),
            &data,
            |b, d| b.iter(|| black_box(crc32c::crc32c(black_box(d)))),
        );
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Key comparison benchmarks
// ---------------------------------------------------------------------------

fn bench_byte_slice_comparison_short(c: &mut Criterion) {
    let k1 = b"key_0001";
    let k2 = b"key_0002";
    c.bench_function("byte_slice_cmp_short (8B)", |b| {
        b.iter(|| black_box(k1.as_slice().cmp(k2.as_slice())))
    });
}

fn bench_byte_slice_comparison_long(c: &mut Criterion) {
    let k1: Vec<u8> = (0..256).map(|i| (i % 256) as u8).collect();
    let mut k2 = k1.clone();
    k2[255] = 0xFF;
    c.bench_function("byte_slice_cmp_long (256B)", |b| {
        b.iter(|| black_box(k1.as_slice().cmp(k2.as_slice())))
    });
}

// ---------------------------------------------------------------------------
// Groups
// ---------------------------------------------------------------------------

criterion_group!(
    lsn_benches,
    bench_lsn_new,
    bench_lsn_from_u64,
    bench_lsn_file_number,
    bench_lsn_file_offset,
    bench_lsn_roundtrip,
    bench_lsn_comparison,
    bench_lsn_no_cleaning_distance,
);

criterion_group!(
    vlsn_benches,
    bench_vlsn_new,
    bench_vlsn_next,
    bench_vlsn_prev,
    bench_vlsn_comparison,
    bench_vlsn_min,
);

criterion_group!(
    packed_int_benches,
    bench_write_packed_i32_small,
    bench_write_packed_i32_medium,
    bench_write_packed_i32_large,
    bench_read_packed_i32_small,
    bench_read_packed_i32_large,
    bench_write_packed_i64_small,
    bench_write_packed_i64_large,
    bench_read_packed_i64_small,
    bench_read_packed_i64_large,
    bench_write_sorted_i32,
    bench_read_sorted_i32,
    bench_write_sorted_i64,
    bench_read_sorted_i64,
);

criterion_group!(checksum_benches, bench_checksums);

criterion_group!(
    key_cmp_benches,
    bench_byte_slice_comparison_short,
    bench_byte_slice_comparison_long,
);

criterion_main!(
    lsn_benches,
    vlsn_benches,
    packed_int_benches,
    checksum_benches,
    key_cmp_benches,
);
