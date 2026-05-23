//! Benchmarks for noxu-txn: lock conflict matrix, lock manager, deadlock detection.

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use hashbrown::{HashMap, HashSet};

use noxu_txn::{DeadlockDetector, LockManager, LockType};

// ---------------------------------------------------------------------------
// Lock conflict matrix benchmarks
// ---------------------------------------------------------------------------

fn bench_lock_conflict_allow(c: &mut Criterion) {
    c.bench_function("lock_conflict_allow (Read vs Read)", |b| {
        b.iter(|| black_box(LockType::Read.get_conflict(LockType::Read)))
    });
}

fn bench_lock_conflict_block(c: &mut Criterion) {
    c.bench_function("lock_conflict_block (Read vs Write)", |b| {
        b.iter(|| black_box(LockType::Read.get_conflict(LockType::Write)))
    });
}

fn bench_lock_conflict_all_25(c: &mut Criterion) {
    let types = [
        LockType::Read,
        LockType::Write,
        LockType::RangeRead,
        LockType::RangeWrite,
        LockType::RangeInsert,
    ];

    c.bench_function("lock_conflict_all_25_pairs", |b| {
        b.iter(|| {
            for &held in &types {
                for &requested in &types {
                    black_box(held.get_conflict(requested));
                }
            }
        })
    });
}

fn bench_lock_upgrade_all_25(c: &mut Criterion) {
    let types = [
        LockType::Read,
        LockType::Write,
        LockType::RangeRead,
        LockType::RangeWrite,
        LockType::RangeInsert,
    ];

    c.bench_function("lock_upgrade_all_25_pairs", |b| {
        b.iter(|| {
            for &held in &types {
                for &requested in &types {
                    black_box(held.get_upgrade(requested));
                }
            }
        })
    });
}

fn bench_lock_is_write(c: &mut Criterion) {
    c.bench_function("lock_is_write_lock", |b| {
        b.iter(|| {
            black_box(LockType::Read.is_write_lock());
            black_box(LockType::Write.is_write_lock());
            black_box(LockType::RangeRead.is_write_lock());
            black_box(LockType::RangeWrite.is_write_lock());
            black_box(LockType::RangeInsert.is_write_lock());
        })
    });
}

// ---------------------------------------------------------------------------
// Lock manager benchmarks
// ---------------------------------------------------------------------------

fn bench_lock_acquire_single_no_contention(c: &mut Criterion) {
    let mgr = LockManager::new();

    c.bench_function("lock_acquire_single_no_contention", |b| {
        b.iter(|| {
            black_box(
                mgr.lock(
                    black_box(1000),
                    black_box(1),
                    LockType::Read,
                    false,
                    false,
                )
                .unwrap(),
            );
        })
    });
}

fn bench_lock_acquire_release_new_lsn(c: &mut Criterion) {
    let mgr = LockManager::new();
    let mut lsn_counter = 0u64;

    c.bench_function("lock_acquire_unique_lsn", |b| {
        b.iter(|| {
            lsn_counter += 1;
            black_box(
                mgr.lock(
                    black_box(lsn_counter),
                    black_box(1),
                    LockType::Write,
                    false,
                    false,
                )
                .unwrap(),
            );
        })
    });
}

fn bench_lock_acquire_write(c: &mut Criterion) {
    let mgr = LockManager::new();

    c.bench_function("lock_acquire_write_same_locker", |b| {
        b.iter(|| {
            black_box(
                mgr.lock(
                    black_box(2000),
                    black_box(1),
                    LockType::Write,
                    false,
                    false,
                )
                .unwrap(),
            );
        })
    });
}

// ---------------------------------------------------------------------------
// Deadlock detector benchmarks
// ---------------------------------------------------------------------------

fn bench_deadlock_no_cycle_chain_5(c: &mut Criterion) {
    // Linear chain: 1->2, 2->3, 3->4, 4->5 (no cycle).
    let mut waits_for = HashMap::new();
    waits_for.insert(1i64, HashSet::from([2i64]));
    waits_for.insert(2, HashSet::from([3]));
    waits_for.insert(3, HashSet::from([4]));
    waits_for.insert(4, HashSet::from([5]));

    c.bench_function("deadlock_no_cycle_chain_5", |b| {
        b.iter(|| {
            let result = DeadlockDetector::detect(6, &[1], &waits_for);
            black_box(result);
        })
    });
}

fn bench_deadlock_no_cycle_chain_20(c: &mut Criterion) {
    // Linear chain of 20 nodes (no cycle).
    let mut waits_for = HashMap::new();
    for i in 1i64..=20 {
        waits_for.insert(i, HashSet::from([i + 1]));
    }

    c.bench_function("deadlock_no_cycle_chain_20", |b| {
        b.iter(|| {
            let result = DeadlockDetector::detect(22, &[1], &waits_for);
            black_box(result);
        })
    });
}

fn bench_deadlock_with_cycle_3(c: &mut Criterion) {
    // T1->T2, T2->T3. T3 requests lock held by T1 => cycle.
    let mut waits_for = HashMap::new();
    waits_for.insert(1i64, HashSet::from([2i64]));
    waits_for.insert(2, HashSet::from([3]));

    c.bench_function("deadlock_with_cycle_3", |b| {
        b.iter(|| {
            let result = DeadlockDetector::detect(3, &[1], &waits_for);
            black_box(result);
        })
    });
}

fn bench_deadlock_with_cycle_10(c: &mut Criterion) {
    // Chain of 10: 1->2, 2->3, ..., 9->10. T10 requests lock held by T1 => cycle of 10.
    let mut waits_for = HashMap::new();
    for i in 1i64..=9 {
        waits_for.insert(i, HashSet::from([i + 1]));
    }

    c.bench_function("deadlock_with_cycle_10", |b| {
        b.iter(|| {
            let result = DeadlockDetector::detect(10, &[1], &waits_for);
            black_box(result);
        })
    });
}

fn bench_deadlock_diamond_no_cycle(c: &mut Criterion) {
    // Diamond: T1->{T2,T3}, T2->T4, T3->T4. No cycle.
    let mut waits_for = HashMap::new();
    waits_for.insert(1i64, HashSet::from([2i64, 3]));
    waits_for.insert(2, HashSet::from([4]));
    waits_for.insert(3, HashSet::from([4]));

    c.bench_function("deadlock_diamond_no_cycle", |b| {
        b.iter(|| {
            let result = DeadlockDetector::detect(5, &[1], &waits_for);
            black_box(result);
        })
    });
}

// ---------------------------------------------------------------------------
// Groups
// ---------------------------------------------------------------------------

criterion_group!(
    conflict_benches,
    bench_lock_conflict_allow,
    bench_lock_conflict_block,
    bench_lock_conflict_all_25,
    bench_lock_upgrade_all_25,
    bench_lock_is_write,
);

criterion_group!(
    lock_mgr_benches,
    bench_lock_acquire_single_no_contention,
    bench_lock_acquire_release_new_lsn,
    bench_lock_acquire_write,
);

criterion_group!(
    deadlock_benches,
    bench_deadlock_no_cycle_chain_5,
    bench_deadlock_no_cycle_chain_20,
    bench_deadlock_with_cycle_3,
    bench_deadlock_with_cycle_10,
    bench_deadlock_diamond_no_cycle,
);

criterion_main!(conflict_benches, lock_mgr_benches, deadlock_benches);
