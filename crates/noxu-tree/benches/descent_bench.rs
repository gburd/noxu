//! STEP 1 ceiling microbench for latch-lite / optimistic tree descent
//! (MVCC proposal §6c option 3).
//!
//! Go/no-go question: **what fraction of a warm point-read is the
//! hand-over-hand shared-latch acquire/release?** Optimistic latch-coupling
//! can only ever remove that fraction; it cannot touch binary-search /
//! key-compare / data-clone / tree-walk cost, nor the record lock (`lock_ln`,
//! a layer above the tree, not benched here). If the latch fraction is small
//! (<~10-15 %), a perfect latch-lite descent yields at most that, and the
//! honest conclusion is the read gap is traversal/LN-bound, not latch-bound.
//!
//! Rather than model a whole alternate descent (fragile under a loaded host),
//! this decomposes the read into its primitives and times each in a tight
//! loop, so the *ratios within one run* are robust even when the box is busy:
//!
//!   `full_read`   — the real `Tree::search_with_data` hot path.
//!   `latch_pair`  — one `read()` acquire + `drop` on a resident node (the
//!                   per-level primitive latch-lite removes). The real descent
//!                   uses `read_arc()` (owned guard) which is this plus one
//!                   `Arc` refcount bump; `latch_pair_arc` adds that bump.
//!   `latch_pair_arc` — `read()`+`drop` plus an `Arc::clone`/drop, modelling
//!                   the `read_arc()` owned-guard cost per level.
//!   `bin_search`  — one `find_entry_compressed` (BIN binary search).
//!   `data_clone`  — one `Bytes::clone` of the value (the refcount bump the
//!                   read returns).
//!
//! Ceiling = depth * latch_pair_arc / full_read. Reported per config.
//!
//! Run: `cargo bench -p noxu-tree --bench descent_bench`

use criterion::{Criterion, criterion_group, criterion_main};
use noxu_tree::tree::{Tree, TreeNode};
use noxu_util::Lsn;
use std::hint::black_box;

fn build_tree(n: usize, fanout: usize) -> (Tree, Vec<Vec<u8>>) {
    let tree = Tree::new(1, fanout);
    let mut keys = Vec::with_capacity(n);
    for i in 0..n {
        let key = (i as u64).to_be_bytes().to_vec();
        let data = vec![0u8; 100]; // 100-byte values, YCSB-ish
        tree.insert(key.clone(), data, Lsn::from_u64(i as u64 + 1))
            .expect("insert");
        keys.push(key);
    }
    (tree, keys)
}

fn tree_depth(tree: &Tree) -> usize {
    let mut depth = 1;
    let mut node = tree.get_root().expect("non-empty");
    loop {
        let child = {
            let g = node.read();
            if g.is_bin() {
                break;
            }
            match &*g {
                TreeNode::Internal(n) => n.get_child(0),
                TreeNode::Bottom(_) => None,
            }
        };
        match child {
            Some(c) => {
                node = c;
                depth += 1;
            }
            None => break,
        }
    }
    depth
}

/// Return the leftmost BIN arc so the primitive benches touch a real
/// leaf node (representative slot count / cache line footprint).
fn leftmost_bin(tree: &Tree) -> noxu_tree::tree::ChildArc {
    let mut node = tree.get_root().expect("non-empty");
    loop {
        let child = {
            let g = node.read();
            if g.is_bin() {
                return node.clone();
            }
            match &*g {
                TreeNode::Internal(n) => n.get_child(0),
                TreeNode::Bottom(_) => None,
            }
        };
        match child {
            Some(c) => node = c,
            None => return node,
        }
    }
}

fn bench_descent(c: &mut Criterion) {
    for &(n, fanout, label) in &[
        (200_000usize, 128usize, "n200k_f128"),
        (1_000_000usize, 128usize, "n1M_f128"),
    ] {
        let (tree, keys) = build_tree(n, fanout);
        let depth = tree_depth(&tree);
        eprintln!(
            "[ceiling] config={label} keys={n} fanout={fanout} tree_depth={depth} \
             (=> {depth} latch acquire/release pairs per point read)"
        );

        let key = keys[n / 2].clone();
        let bin = leftmost_bin(&tree);

        let mut group = c.benchmark_group(format!("descent/{label}"));

        group.bench_function("full_read", |b| {
            b.iter(|| black_box(tree.search_with_data(black_box(&key))))
        });

        // One resident node read()+drop — the primitive latch-lite removes.
        group.bench_function("latch_pair", |b| {
            b.iter(|| {
                let g = bin.read();
                black_box(g.is_bin());
                drop(g);
            })
        });

        // read()+drop plus an Arc clone/drop == the read_arc() owned-guard
        // cost the real descent pays per level.
        group.bench_function("latch_pair_arc", |b| {
            b.iter(|| {
                let owned = bin.clone();
                let g = owned.read();
                black_box(g.is_bin());
                drop(g);
                drop(owned);
            })
        });

        // One BIN binary search (find_entry_compressed).
        group.bench_function("bin_search", |b| {
            b.iter(|| {
                let g = bin.read();
                if let TreeNode::Bottom(b) = &*g {
                    black_box(b.find_entry_compressed(black_box(&key)));
                }
            })
        });

        // Model of what an OLC descent does PER LEVEL instead of a latch
        // pair: two relaxed atomic version loads (read-before, re-read-after)
        // plus an Arc-pointer read. This is the cost latch-lite *keeps*, so
        // the achievable per-level win is (latch_pair_arc - olc_version_check).
        let ver = std::sync::atomic::AtomicU64::new(0);
        group.bench_function("olc_version_check", |b| {
            b.iter(|| {
                use std::sync::atomic::Ordering::Acquire;
                let v1 = ver.load(Acquire);
                let child = black_box(&bin); // model child-pointer read
                let v2 = ver.load(Acquire);
                black_box((v1, v2, child));
            })
        });

        group.finish();
    }
}

criterion_group!(benches, bench_descent);
criterion_main!(benches);
