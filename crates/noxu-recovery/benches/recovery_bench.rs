//! Benchmarks for noxu-recovery: checkpoint serialization, DirtyINMap, RollbackTracker.

#![allow(clippy::unit_arg)]

use criterion::{Criterion, black_box, criterion_group, criterion_main};

use noxu_recovery::{
    CheckpointReference, CheckpointStart, DirtyINMap, RollbackTracker,
};
use noxu_util::Lsn;

// ---------------------------------------------------------------------------
// CheckpointStart serialization benchmarks
// ---------------------------------------------------------------------------

fn bench_checkpoint_start_serialize_short(c: &mut Criterion) {
    let ckpt = CheckpointStart::new(1, "daemon");

    c.bench_function("checkpoint_start_serialize_short (invoker=daemon)", |b| {
        b.iter(|| {
            let mut buf = Vec::with_capacity(32);
            black_box(ckpt.write_to_log(&mut buf).unwrap());
            black_box(buf.len());
        })
    });
}

fn bench_checkpoint_start_serialize_long(c: &mut Criterion) {
    let ckpt = CheckpointStart::new(999, "api_triggered_by_application_code");

    c.bench_function(
        "checkpoint_start_serialize_long (invoker=34B string)",
        |b| {
            b.iter(|| {
                let mut buf = Vec::with_capacity(64);
                black_box(ckpt.write_to_log(&mut buf).unwrap());
                black_box(buf.len());
            })
        },
    );
}

fn bench_checkpoint_start_roundtrip(c: &mut Criterion) {
    let ckpt = CheckpointStart::new(42, "recovery");
    let mut serialized = Vec::new();
    ckpt.write_to_log(&mut serialized).unwrap();

    c.bench_function("checkpoint_start_roundtrip", |b| {
        b.iter(|| {
            let decoded =
                CheckpointStart::read_from_log(black_box(&serialized))
                    .unwrap();
            black_box(decoded.get_id());
        })
    });
}

// ---------------------------------------------------------------------------
// DirtyINMap insert benchmarks
// ---------------------------------------------------------------------------

fn bench_dirty_in_map_insert_10(c: &mut Criterion) {
    c.bench_function("dirty_in_map_insert_10", |b| {
        b.iter(|| {
            let mut map = DirtyINMap::new();
            for i in 0u64..10 {
                let r = CheckpointReference::new(
                    black_box(i),
                    black_box(1),
                    black_box(false),
                    black_box(1),
                );
                map.add_dirty_in(r);
            }
            black_box(map.get_num_entries());
        })
    });
}

fn bench_dirty_in_map_insert_100(c: &mut Criterion) {
    c.bench_function("dirty_in_map_insert_100", |b| {
        b.iter(|| {
            let mut map = DirtyINMap::new();
            for i in 0u64..100 {
                // Mix of BINs (level 1) and upper INs (level 2).
                let level = if i % 3 == 0 { 2 } else { 1 };
                let is_delta = i % 5 == 0;
                let r = CheckpointReference::new(
                    black_box(i),
                    black_box(1),
                    black_box(is_delta),
                    black_box(level),
                );
                map.add_dirty_in(r);
            }
            black_box(map.get_num_entries());
        })
    });
}

fn bench_dirty_in_map_multi_level(c: &mut Criterion) {
    // Simulate a tree with 4 levels: BINs at 1, INs at 2, 3, 4.
    c.bench_function("dirty_in_map_insert_multi_level (4 levels, 50 nodes)", |b| {
        b.iter(|| {
            let mut map = DirtyINMap::new();
            for level in 1i32..=4 {
                for i in 0u64..50 {
                    let r = CheckpointReference::new(
                        i + (level as u64 * 1000),
                        1,
                        false,
                        level,
                    );
                    map.add_dirty_in(r);
                }
            }
            black_box(map.get_num_entries());
        })
    });
}

// ---------------------------------------------------------------------------
// RollbackTracker period tracking benchmarks
// ---------------------------------------------------------------------------

fn bench_rollback_tracker_register_start(c: &mut Criterion) {
    c.bench_function("rollback_tracker_register_start", |b| {
        b.iter(|| {
            let mut tracker = RollbackTracker::new();
            let matchpoint = Lsn::new(1, 100);
            let start_lsn = Lsn::new(1, 200);
            tracker.register_rollback_start(
                black_box(matchpoint),
                black_box(start_lsn),
            );
            black_box(tracker.has_incomplete_rollbacks());
        })
    });
}

fn bench_rollback_tracker_complete_period(c: &mut Criterion) {
    c.bench_function("rollback_tracker_complete_period", |b| {
        b.iter(|| {
            let mut tracker = RollbackTracker::new();
            let matchpoint = Lsn::new(1, 100);
            let start_lsn = Lsn::new(1, 200);
            let end_lsn = Lsn::new(0, 50);

            tracker.register_rollback_start(matchpoint, start_lsn);
            tracker.register_rollback_end(
                black_box(matchpoint),
                black_box(end_lsn),
            );
            black_box(tracker.period_count());
        })
    });
}

fn bench_rollback_tracker_is_in_period_10(c: &mut Criterion) {
    // Pre-build a tracker with 10 completed rollback periods, then benchmark
    // the membership query.
    let mut tracker = RollbackTracker::new();
    for i in 0u32..10 {
        let matchpoint = Lsn::new(0, i * 1000);
        let start_lsn = Lsn::new(0, i * 1000 + 500);
        let end_lsn = Lsn::new(0, i * 1000 + 10);
        tracker.register_rollback_start(matchpoint, start_lsn);
        tracker.register_rollback_end(matchpoint, end_lsn);
    }

    let query_lsn = Lsn::new(0, 5250); // Inside one of the periods.

    c.bench_function("rollback_tracker_is_in_period_10", |b| {
        b.iter(|| {
            black_box(tracker.is_in_rollback_period(black_box(query_lsn)));
        })
    });
}

fn bench_rollback_tracker_is_not_in_period(c: &mut Criterion) {
    let mut tracker = RollbackTracker::new();
    for i in 0u32..10 {
        let matchpoint = Lsn::new(0, i * 1000);
        let start_lsn = Lsn::new(0, i * 1000 + 500);
        let end_lsn = Lsn::new(0, i * 1000 + 10);
        tracker.register_rollback_start(matchpoint, start_lsn);
        tracker.register_rollback_end(matchpoint, end_lsn);
    }

    let query_lsn = Lsn::new(5, 0); // Way beyond all periods.

    c.bench_function("rollback_tracker_is_not_in_period_10", |b| {
        b.iter(|| {
            black_box(tracker.is_in_rollback_period(black_box(query_lsn)));
        })
    });
}

// ---------------------------------------------------------------------------
// Groups
// ---------------------------------------------------------------------------

criterion_group!(
    checkpoint_benches,
    bench_checkpoint_start_serialize_short,
    bench_checkpoint_start_serialize_long,
    bench_checkpoint_start_roundtrip,
);

criterion_group!(
    dirty_in_map_benches,
    bench_dirty_in_map_insert_10,
    bench_dirty_in_map_insert_100,
    bench_dirty_in_map_multi_level,
);

criterion_group!(
    rollback_benches,
    bench_rollback_tracker_register_start,
    bench_rollback_tracker_complete_period,
    bench_rollback_tracker_is_in_period_10,
    bench_rollback_tracker_is_not_in_period,
);

criterion_main!(checkpoint_benches, dirty_in_map_benches, rollback_benches);
