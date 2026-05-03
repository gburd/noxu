//! Benchmarks for noxu-tree: BIN search, IN node insert, flag ops, key compare.

use criterion::{Criterion, black_box, criterion_group, criterion_main};

use noxu_tree::{
    BIN_LEVEL, DEFAULT_MAX_ENTRIES, InNode, MAIN_LEVEL,
    entry_states::DIRTY_BIT,
};
use noxu_util::{Lsn, NULL_LSN};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build an InNode (BIN-level) pre-populated with `count` sorted keys.
fn make_in_node_with_entries(count: usize) -> InNode {
    let max = count.max(DEFAULT_MAX_ENTRIES);
    let mut node = InNode::new(1, BIN_LEVEL, max);
    for i in 0..count {
        let key = format!("key_{:06}", i).into_bytes();
        let lsn = Lsn::new(0, i as u32);
        node.insert_entry(key, lsn, DIRTY_BIT).unwrap();
    }
    node
}

// ---------------------------------------------------------------------------
// BIN search benchmarks
// ---------------------------------------------------------------------------

fn bench_bin_search_16(c: &mut Criterion) {
    let node = make_in_node_with_entries(16);
    // Search for a key in the middle of the node.
    let target = b"key_000008".as_slice();

    c.bench_function("bin_search_16", |b| {
        b.iter(|| {
            black_box(node.find_entry(black_box(target), true, false));
        })
    });
}

fn bench_bin_search_128(c: &mut Criterion) {
    let node = make_in_node_with_entries(128);
    let target = b"key_000064".as_slice();

    c.bench_function("bin_search_128", |b| {
        b.iter(|| {
            black_box(node.find_entry(black_box(target), true, false));
        })
    });
}

fn bench_bin_search_16_miss(c: &mut Criterion) {
    let node = make_in_node_with_entries(16);
    // Key that does not exist.
    let target = b"zzz_missing".as_slice();

    c.bench_function("bin_search_16_miss", |b| {
        b.iter(|| {
            black_box(node.find_entry(black_box(target), false, false));
        })
    });
}

fn bench_bin_search_128_miss(c: &mut Criterion) {
    let node = make_in_node_with_entries(128);
    let target = b"zzz_missing".as_slice();

    c.bench_function("bin_search_128_miss", |b| {
        b.iter(|| {
            black_box(node.find_entry(black_box(target), false, false));
        })
    });
}

// ---------------------------------------------------------------------------
// InNode insert benchmarks
// ---------------------------------------------------------------------------

fn bench_in_node_insert(c: &mut Criterion) {
    // Insert unique keys one at a time into a fresh node.
    c.bench_function("in_node_insert", |b| {
        let mut counter = 0u64;
        b.iter(|| {
            // Use a node large enough that it never fills during the bench run.
            let mut node = InNode::new(1, BIN_LEVEL, 16 * 1024);
            for i in 0..16usize {
                counter += 1;
                let key =
                    format!("k_{:016}", counter + i as u64).into_bytes();
                black_box(
                    node.insert_entry(key, NULL_LSN, DIRTY_BIT).unwrap(),
                );
            }
        })
    });
}

fn bench_in_node_insert_sorted(c: &mut Criterion) {
    // Insert 128 already-sorted keys into a fresh BIN node.
    c.bench_function("in_node_insert_128_sorted", |b| {
        b.iter(|| {
            let mut node = InNode::new(1, BIN_LEVEL, 256);
            for i in 0..128usize {
                let key = format!("key_{:06}", i).into_bytes();
                black_box(
                    node.insert_entry(
                        black_box(key),
                        NULL_LSN,
                        DIRTY_BIT,
                    )
                    .unwrap(),
                );
            }
        })
    });
}

// ---------------------------------------------------------------------------
// InNode flag operation benchmarks
// ---------------------------------------------------------------------------

fn bench_in_node_flag_ops(c: &mut Criterion) {
    let mut node = make_in_node_with_entries(16);

    c.bench_function("in_node_flag_ops (dirty/KD/PD set+get)", |b| {
        b.iter(|| {
            // dirty flag
            node.set_dirty(black_box(true));
            black_box(node.is_dirty());
            node.set_dirty(false);

            // entry dirty bit
            node.set_entry_dirty(0);
            black_box(node.is_entry_dirty(0));
            node.clear_entry_dirty(0);

            // known-deleted bit
            node.set_known_deleted(1);
            black_box(node.is_entry_known_deleted(1));
            node.clear_known_deleted(1);

            // pending-deleted bit
            node.set_pending_deleted(2);
            black_box(node.is_entry_pending_deleted(2));
            node.clear_pending_deleted(2);

            // bin-delta flag
            node.set_bin_delta(true);
            black_box(node.is_bin_delta());
            node.set_bin_delta(false);
        })
    });
}

fn bench_in_node_set_lsn(c: &mut Criterion) {
    let mut node = make_in_node_with_entries(16);
    let new_lsn = Lsn::new(5, 4096);

    c.bench_function("in_node_set_lsn_16_slots", |b| {
        b.iter(|| {
            for i in 0..node.n_entries() {
                node.set_lsn(i, black_box(new_lsn));
            }
            black_box(node.get_lsn(0));
        })
    });
}

// ---------------------------------------------------------------------------
// Key comparison benchmarks
// ---------------------------------------------------------------------------

fn bench_key_compare_equal(c: &mut Criterion) {
    let k1 = b"key_000042";
    let k2 = b"key_000042";
    c.bench_function("key_compare_equal (10B)", |b| {
        b.iter(|| black_box(k1.as_slice().cmp(k2.as_slice())))
    });
}

fn bench_key_compare_less(c: &mut Criterion) {
    let k1 = b"key_000001";
    let k2 = b"key_000099";
    c.bench_function("key_compare_less (10B)", |b| {
        b.iter(|| black_box(k1.as_slice().cmp(k2.as_slice())))
    });
}

fn bench_key_compare_long(c: &mut Criterion) {
    let k1: Vec<u8> = (0u8..128).collect();
    let mut k2 = k1.clone();
    k2[127] = 0xFF;
    c.bench_function("key_compare_long (128B)", |b| {
        b.iter(|| black_box(k1.as_slice().cmp(k2.as_slice())))
    });
}

fn bench_upper_in_search(c: &mut Criterion) {
    // Upper IN node (level 2) has virtual key-0 behavior.
    let level2 = MAIN_LEVEL | 2;
    let mut node = InNode::new(1, level2, DEFAULT_MAX_ENTRIES);
    for i in 0..64usize {
        let key = format!("key_{:06}", i).into_bytes();
        let _ = node.insert_entry(key, NULL_LSN, 0);
    }
    let target = b"key_000032".as_slice();

    c.bench_function("upper_in_search_64", |b| {
        b.iter(|| {
            black_box(node.find_entry(black_box(target), true, false));
        })
    });
}

// ---------------------------------------------------------------------------
// Groups
// ---------------------------------------------------------------------------

criterion_group!(
    bin_search_benches,
    bench_bin_search_16,
    bench_bin_search_128,
    bench_bin_search_16_miss,
    bench_bin_search_128_miss,
    bench_upper_in_search,
);

criterion_group!(
    in_insert_benches,
    bench_in_node_insert,
    bench_in_node_insert_sorted,
);

criterion_group!(
    in_flag_benches,
    bench_in_node_flag_ops,
    bench_in_node_set_lsn,
);

criterion_group!(
    key_compare_benches,
    bench_key_compare_equal,
    bench_key_compare_less,
    bench_key_compare_long,
);

criterion_main!(
    bin_search_benches,
    in_insert_benches,
    in_flag_benches,
    key_compare_benches,
);
