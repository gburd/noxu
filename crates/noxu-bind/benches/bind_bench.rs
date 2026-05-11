//! Benchmarks for noxu-bind: TupleOutput write, TupleInput read, round-trips.

#![allow(clippy::approx_constant)]

use criterion::{Criterion, black_box, criterion_group, criterion_main};

use noxu_bind::tuple::{TupleInput, TupleOutput};

// ---------------------------------------------------------------------------
// TupleOutput write benchmarks
// ---------------------------------------------------------------------------

fn bench_tuple_write_i32(c: &mut Criterion) {
    c.bench_function("tuple_write_i32", |b| {
        let mut out = TupleOutput::new();
        b.iter(|| {
            out.reset();
            out.write_i32(black_box(42));
            black_box(out.len());
        })
    });
}

fn bench_tuple_write_i64(c: &mut Criterion) {
    c.bench_function("tuple_write_i64", |b| {
        let mut out = TupleOutput::new();
        b.iter(|| {
            out.reset();
            out.write_i64(black_box(123_456_789));
            black_box(out.len());
        })
    });
}

fn bench_tuple_write_string_short(c: &mut Criterion) {
    c.bench_function("tuple_write_string_short (5B)", |b| {
        let mut out = TupleOutput::new();
        b.iter(|| {
            out.reset();
            out.write_string(black_box("hello"));
            black_box(out.len());
        })
    });
}

fn bench_tuple_write_string_medium(c: &mut Criterion) {
    let s = "a]".repeat(50); // 100 bytes
    c.bench_function("tuple_write_string_medium (100B)", |b| {
        let mut out = TupleOutput::new();
        b.iter(|| {
            out.reset();
            out.write_string(black_box(&s));
            black_box(out.len());
        })
    });
}

fn bench_tuple_write_packed_int_small(c: &mut Criterion) {
    c.bench_function("tuple_write_packed_int_small (42)", |b| {
        let mut out = TupleOutput::new();
        b.iter(|| {
            out.reset();
            out.write_packed_int(black_box(42));
            black_box(out.len());
        })
    });
}

fn bench_tuple_write_packed_int_large(c: &mut Criterion) {
    c.bench_function("tuple_write_packed_int_large (i32::MAX)", |b| {
        let mut out = TupleOutput::new();
        b.iter(|| {
            out.reset();
            out.write_packed_int(black_box(i32::MAX));
            black_box(out.len());
        })
    });
}

fn bench_tuple_write_packed_long_small(c: &mut Criterion) {
    c.bench_function("tuple_write_packed_long_small (42)", |b| {
        let mut out = TupleOutput::new();
        b.iter(|| {
            out.reset();
            out.write_packed_long(black_box(42));
            black_box(out.len());
        })
    });
}

fn bench_tuple_write_sorted_float(c: &mut Criterion) {
    c.bench_function("tuple_write_sorted_float", |b| {
        let mut out = TupleOutput::new();
        b.iter(|| {
            out.reset();
            out.write_sorted_float(black_box(3.14));
            black_box(out.len());
        })
    });
}

fn bench_tuple_write_sorted_double(c: &mut Criterion) {
    c.bench_function("tuple_write_sorted_double", |b| {
        let mut out = TupleOutput::new();
        b.iter(|| {
            out.reset();
            out.write_sorted_double(black_box(3.14159265));
            black_box(out.len());
        })
    });
}

fn bench_tuple_write_bool(c: &mut Criterion) {
    c.bench_function("tuple_write_bool", |b| {
        let mut out = TupleOutput::new();
        b.iter(|| {
            out.reset();
            out.write_bool(black_box(true));
            black_box(out.len());
        })
    });
}

// ---------------------------------------------------------------------------
// TupleInput read benchmarks
// ---------------------------------------------------------------------------

fn bench_tuple_read_i32(c: &mut Criterion) {
    let mut out = TupleOutput::new();
    out.write_i32(42);
    let buf = out.to_vec();
    c.bench_function("tuple_read_i32", |b| {
        b.iter(|| {
            let mut input = TupleInput::new(black_box(&buf));
            black_box(input.read_i32().unwrap());
        })
    });
}

fn bench_tuple_read_i64(c: &mut Criterion) {
    let mut out = TupleOutput::new();
    out.write_i64(123_456_789);
    let buf = out.to_vec();
    c.bench_function("tuple_read_i64", |b| {
        b.iter(|| {
            let mut input = TupleInput::new(black_box(&buf));
            black_box(input.read_i64().unwrap());
        })
    });
}

fn bench_tuple_read_string_short(c: &mut Criterion) {
    let mut out = TupleOutput::new();
    out.write_string("hello");
    let buf = out.to_vec();
    c.bench_function("tuple_read_string_short (5B)", |b| {
        b.iter(|| {
            let mut input = TupleInput::new(black_box(&buf));
            black_box(input.read_string().unwrap());
        })
    });
}

fn bench_tuple_read_packed_int_small(c: &mut Criterion) {
    let mut out = TupleOutput::new();
    out.write_packed_int(42);
    let buf = out.to_vec();
    c.bench_function("tuple_read_packed_int_small (42)", |b| {
        b.iter(|| {
            let mut input = TupleInput::new(black_box(&buf));
            black_box(input.read_packed_int().unwrap());
        })
    });
}

fn bench_tuple_read_packed_int_large(c: &mut Criterion) {
    let mut out = TupleOutput::new();
    out.write_packed_int(i32::MAX);
    let buf = out.to_vec();
    c.bench_function("tuple_read_packed_int_large (i32::MAX)", |b| {
        b.iter(|| {
            let mut input = TupleInput::new(black_box(&buf));
            black_box(input.read_packed_int().unwrap());
        })
    });
}

fn bench_tuple_read_sorted_float(c: &mut Criterion) {
    let mut out = TupleOutput::new();
    out.write_sorted_float(3.14);
    let buf = out.to_vec();
    c.bench_function("tuple_read_sorted_float", |b| {
        b.iter(|| {
            let mut input = TupleInput::new(black_box(&buf));
            black_box(input.read_sorted_float().unwrap());
        })
    });
}

fn bench_tuple_read_sorted_double(c: &mut Criterion) {
    let mut out = TupleOutput::new();
    out.write_sorted_double(3.14159265);
    let buf = out.to_vec();
    c.bench_function("tuple_read_sorted_double", |b| {
        b.iter(|| {
            let mut input = TupleInput::new(black_box(&buf));
            black_box(input.read_sorted_double().unwrap());
        })
    });
}

// ---------------------------------------------------------------------------
// Round-trip benchmarks (write then read)
// ---------------------------------------------------------------------------

fn bench_tuple_roundtrip_mixed(c: &mut Criterion) {
    c.bench_function("tuple_roundtrip_mixed (i32+i64+string)", |b| {
        b.iter(|| {
            let mut out = TupleOutput::new();
            out.write_i32(black_box(42));
            out.write_i64(black_box(123_456_789));
            out.write_string(black_box("hello"));
            let buf = out.to_vec();

            let mut input = TupleInput::new(&buf);
            black_box(input.read_i32().unwrap());
            black_box(input.read_i64().unwrap());
            black_box(input.read_string().unwrap());
        })
    });
}

fn bench_tuple_to_database_entry(c: &mut Criterion) {
    let mut out = TupleOutput::new();
    out.write_i32(42);
    out.write_i64(123_456_789);
    out.write_string("benchmark");
    c.bench_function("tuple_to_database_entry", |b| {
        b.iter(|| {
            black_box(out.to_database_entry());
        })
    });
}

// ---------------------------------------------------------------------------
// Groups
// ---------------------------------------------------------------------------

criterion_group!(
    write_benches,
    bench_tuple_write_i32,
    bench_tuple_write_i64,
    bench_tuple_write_string_short,
    bench_tuple_write_string_medium,
    bench_tuple_write_packed_int_small,
    bench_tuple_write_packed_int_large,
    bench_tuple_write_packed_long_small,
    bench_tuple_write_sorted_float,
    bench_tuple_write_sorted_double,
    bench_tuple_write_bool,
);

criterion_group!(
    read_benches,
    bench_tuple_read_i32,
    bench_tuple_read_i64,
    bench_tuple_read_string_short,
    bench_tuple_read_packed_int_small,
    bench_tuple_read_packed_int_large,
    bench_tuple_read_sorted_float,
    bench_tuple_read_sorted_double,
);

criterion_group!(
    roundtrip_benches,
    bench_tuple_roundtrip_mixed,
    bench_tuple_to_database_entry,
);

criterion_main!(write_benches, read_benches, roundtrip_benches);
