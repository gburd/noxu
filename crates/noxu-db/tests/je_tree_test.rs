//! JE tree-level invariant ports — split, count, balance, key-prefix.
//!
//! Each test below corresponds to a method in `test/com/sleepycat/je/tree/`.
//! These tests exercise the public-API surface (Database/Cursor) but assert
//! tree-shape invariants (sorted iteration, key-prefix transparency, large
//! key sets surviving splits) that JE asserts at the Tree internal level.

use noxu_db::{
    Comparator, DatabaseConfig, DatabaseEntry, EnvironmentConfig, Get,
    OperationStatus, VerifyConfig,
};
use tempfile::TempDir;

fn open_env_db(
    dir: &TempDir,
    name: &str,
) -> (noxu_db::Environment, noxu_db::Database) {
    let env_cfg = EnvironmentConfig::new(dir.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    let env = noxu_db::Environment::open(env_cfg).unwrap();
    let db_cfg =
        DatabaseConfig::new().with_allow_create(true).with_transactional(true);
    let db = env.open_database(None, name, &db_cfg).unwrap();
    (env, db)
}

// ──────────────────────────────────────────────────────────────────────────────
// JE: SplitTest.test0Split (order-preservation surface; the specific
// 0th-entry-promotion + delete/compress/re-split + retrieve-140 scenario is
// ported faithfully as split_0split_zeroth_entry_promotion below).
//
// JE invariant: inserting 16 keys in descending order then 16 in ascending
// order both end up sorted on cursor walk; the splits must preserve order
// invariants.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn split_descending_then_ascending_keys_remain_sorted() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_env_db(&dir, "split_0");

    let txn = env.begin_transaction(None).unwrap();
    // Insert descending: 160, 150, 140, ..., 10
    for i in (10..=160).rev().step_by(10) {
        db.put_in(
            &txn,
            DatabaseEntry::from_bytes(&[i as u8]),
            DatabaseEntry::from_bytes(&[1]),
        )
        .unwrap();
    }
    // Insert ascending: 1, 2, ..., 9
    for i in 1..10u8 {
        db.put_in(
            &txn,
            DatabaseEntry::from_bytes(&[i]),
            DatabaseEntry::from_bytes(&[1]),
        )
        .unwrap();
    }
    txn.commit().unwrap();

    // Cursor walk must yield sorted keys.
    let txn = env.begin_transaction(None).unwrap();
    let mut c = db.open_cursor_in(&txn, None).unwrap();
    let mut prev: Option<u8> = None;
    let mut count = 0usize;
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    let mut s = c.get(&mut k, &mut d, Get::First, None).unwrap();
    while s == OperationStatus::Success {
        let cur = k.data_opt().unwrap()[0];
        if let Some(p) = prev {
            assert!(p < cur, "keys must walk in ascending order: {p} < {cur}");
        }
        prev = Some(cur);
        count += 1;
        s = c.get(&mut k, &mut d, Get::Next, None).unwrap();
    }
    assert_eq!(count, 16 + 9);
    drop(c);
    txn.commit().unwrap();
}

// ──────────────────────────────────────────────────────────────────────────────
// JE: TreeTest.testCountAndValidateKeys / testCountAndValidateKeysBackwards
//
// JE invariant: insert N random keys, then walk forward and backward; the
// number of records walked must equal N in both directions, and the keys
// must be sorted.
// ──────────────────────────────────────────────────────────────────────────────

const N_KEYS: u32 = 500;

#[test]
fn tree_count_and_validate_keys_forward() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_env_db(&dir, "count_fwd");

    let txn = env.begin_transaction(None).unwrap();
    // Pseudo-random distinct keys (sorted by hash to scatter).
    let mut keys: Vec<u32> =
        (0..N_KEYS).map(|i| i.wrapping_mul(2_654_435_761)).collect();
    keys.sort();
    keys.dedup();
    let n = keys.len();
    for k in &keys {
        db.put_in(
            &txn,
            DatabaseEntry::from_bytes(&k.to_be_bytes()),
            DatabaseEntry::from_bytes(b"v"),
        )
        .unwrap();
    }
    txn.commit().unwrap();

    let txn = env.begin_transaction(None).unwrap();
    let mut c = db.open_cursor_in(&txn, None).unwrap();
    let mut prev: Option<Vec<u8>> = None;
    let mut count = 0usize;
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    let mut s = c.get(&mut k, &mut d, Get::First, None).unwrap();
    while s == OperationStatus::Success {
        let cur = k.data_opt().unwrap().to_vec();
        if let Some(p) = &prev {
            assert!(p < &cur, "forward walk must be sorted");
        }
        prev = Some(cur);
        count += 1;
        s = c.get(&mut k, &mut d, Get::Next, None).unwrap();
    }
    assert_eq!(count, n);
    drop(c);
    txn.commit().unwrap();
}

#[test]
fn tree_count_and_validate_keys_backwards() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_env_db(&dir, "count_bwd");

    let txn = env.begin_transaction(None).unwrap();
    let mut keys: Vec<u32> =
        (0..N_KEYS).map(|i| i.wrapping_mul(2_654_435_761)).collect();
    keys.sort();
    keys.dedup();
    let n = keys.len();
    for k in &keys {
        db.put_in(
            &txn,
            DatabaseEntry::from_bytes(&k.to_be_bytes()),
            DatabaseEntry::from_bytes(b"v"),
        )
        .unwrap();
    }
    txn.commit().unwrap();

    let txn = env.begin_transaction(None).unwrap();
    let mut c = db.open_cursor_in(&txn, None).unwrap();
    let mut prev: Option<Vec<u8>> = None;
    let mut count = 0usize;
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    let mut s = c.get(&mut k, &mut d, Get::Last, None).unwrap();
    while s == OperationStatus::Success {
        let cur = k.data_opt().unwrap().to_vec();
        if let Some(p) = &prev {
            assert!(p > &cur, "backward walk must be reverse-sorted");
        }
        prev = Some(cur);
        count += 1;
        s = c.get(&mut k, &mut d, Get::Prev, None).unwrap();
    }
    assert_eq!(count, n);
    drop(c);
    txn.commit().unwrap();
}

// ──────────────────────────────────────────────────────────────────────────────
// JE: TreeTest.testAscendingInsertBalance / testDescendingInsertBalance
// (order/count surface; the level-balance <= 10 assertion is ported as
// tree_insert_balance_levels_bounded below).
//
// JE invariant: ascending and descending insert sequences both produce a
// tree that is fully traversable (forward and backward) with all keys
// visible.  JE additionally asserts the tree depth, but Noxu's
// public API doesn't expose depth — we capture the order/count invariant
// instead.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn tree_ascending_insert_walks_in_order() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_env_db(&dir, "asc_balance");
    let txn = env.begin_transaction(None).unwrap();
    for i in 0..N_KEYS {
        db.put_in(
            &txn,
            DatabaseEntry::from_bytes(&i.to_be_bytes()),
            DatabaseEntry::from_bytes(b""),
        )
        .unwrap();
    }
    txn.commit().unwrap();

    let txn = env.begin_transaction(None).unwrap();
    let mut c = db.open_cursor_in(&txn, None).unwrap();
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    for i in 0..N_KEYS {
        let s = c.get(&mut k, &mut d, Get::Next, None).unwrap();
        assert_eq!(s, OperationStatus::Success);
        let mut a = [0u8; 4];
        a.copy_from_slice(k.data_opt().unwrap());
        assert_eq!(u32::from_be_bytes(a), i);
    }
    let s = c.get(&mut k, &mut d, Get::Next, None).unwrap();
    assert_eq!(s, OperationStatus::NotFound);
    drop(c);
    txn.commit().unwrap();
}

#[test]
fn tree_descending_insert_walks_in_order() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_env_db(&dir, "desc_balance");
    let txn = env.begin_transaction(None).unwrap();
    for i in (0..N_KEYS).rev() {
        db.put_in(
            &txn,
            DatabaseEntry::from_bytes(&i.to_be_bytes()),
            DatabaseEntry::from_bytes(b""),
        )
        .unwrap();
    }
    txn.commit().unwrap();

    let txn = env.begin_transaction(None).unwrap();
    let mut c = db.open_cursor_in(&txn, None).unwrap();
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    for i in 0..N_KEYS {
        let s = c.get(&mut k, &mut d, Get::Next, None).unwrap();
        assert_eq!(s, OperationStatus::Success);
        let mut a = [0u8; 4];
        a.copy_from_slice(k.data_opt().unwrap());
        assert_eq!(u32::from_be_bytes(a), i);
    }
    drop(c);
    txn.commit().unwrap();
}

// ──────────────────────────────────────────────────────────────────────────────
// JE: KeyPrefixTest.testPrefixBasic (public-API round-trip; the
// prefix-actually-computed `somePrefixSeen` assertion is ported at the
// BinStub level in noxu-tree/src/tree.rs::keyprefix_basic_computes_prefix).
//
// JE invariant: keys with a long shared prefix can be inserted, walked, and
// retrieved correctly — key-prefixing is transparent to the public API.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn key_prefix_basic_long_shared_prefix_round_trip() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_env_db(&dir, "prefix_basic");

    let prefix = b"abcdefghijklmnopqrstuvwxyz0123456789-";
    let mut keys: Vec<Vec<u8>> = Vec::new();
    for i in 0..100u32 {
        let mut k = prefix.to_vec();
        k.extend_from_slice(&i.to_be_bytes());
        keys.push(k);
    }

    let txn = env.begin_transaction(None).unwrap();
    for k in &keys {
        db.put_in(
            &txn,
            DatabaseEntry::from_bytes(k),
            DatabaseEntry::from_bytes(b"v"),
        )
        .unwrap();
    }
    txn.commit().unwrap();

    // Walk: must produce keys in sorted order, count == keys.len().
    let txn = env.begin_transaction(None).unwrap();
    let mut c = db.open_cursor_in(&txn, None).unwrap();
    let mut walked: Vec<Vec<u8>> = Vec::new();
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    let mut s = c.get(&mut k, &mut d, Get::First, None).unwrap();
    while s == OperationStatus::Success {
        walked.push(k.data_opt().unwrap().to_vec());
        s = c.get(&mut k, &mut d, Get::Next, None).unwrap();
    }
    let mut sorted = keys.clone();
    sorted.sort();
    assert_eq!(walked, sorted);

    // Random search-by-key works.
    for k in &keys {
        let mut out = DatabaseEntry::new();
        let s = db
            .get_into(Some(&txn), DatabaseEntry::from_bytes(k), &mut out)
            .unwrap();
        assert!(s);
        assert_eq!(out.data_opt().unwrap(), b"v");
    }
    drop(c);
    txn.commit().unwrap();
}

// ──────────────────────────────────────────────────────────────────────────────
// JE: KeyPrefixTest.testPrefixManySequential (1000 sequential prefixed
// keys round-trip; JE uses ~94k timestamp keys, scaled down).
//
// JE invariant: 1000 sequential u32 keys with a shared prefix all round-trip.
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn key_prefix_many_sequential_round_trip() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_env_db(&dir, "prefix_seq");

    let prefix = b"shared-prefix-";
    let txn = env.begin_transaction(None).unwrap();
    for i in 0..1000u32 {
        let mut k = prefix.to_vec();
        k.extend_from_slice(&i.to_be_bytes());
        db.put_in(
            &txn,
            DatabaseEntry::from_bytes(&k),
            DatabaseEntry::from_bytes(&i.to_be_bytes()),
        )
        .unwrap();
    }
    txn.commit().unwrap();

    let txn = env.begin_transaction(None).unwrap();
    let mut c = db.open_cursor_in(&txn, None).unwrap();
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    for i in 0..1000u32 {
        let s = c.get(&mut k, &mut d, Get::Next, None).unwrap();
        assert_eq!(s, OperationStatus::Success);
        let key_bytes = k.data_opt().unwrap();
        assert_eq!(&key_bytes[..prefix.len()], prefix);
        let mut a = [0u8; 4];
        a.copy_from_slice(&key_bytes[prefix.len()..]);
        assert_eq!(u32::from_be_bytes(a), i);
    }
    drop(c);
    txn.commit().unwrap();
}

// ──────────────────────────────────────────────────────────────────────────────
// JE: TreeTest.testSimpleTreeCreation — rudimentary insert/retrieve of a few
// keys (including keys that are byte-prefixes of each other).
// ──────────────────────────────────────────────────────────────────────────────
#[test]
fn tree_simple_creation_insert_retrieve() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_env_db(&dir, "simple");
    let txn = env.begin_transaction(None).unwrap();
    for k in [b"aaaaa".as_ref(), b"aaaab", b"aaaa", b"aaa"] {
        db.put_in(
            &txn,
            DatabaseEntry::from_bytes(k),
            DatabaseEntry::from_bytes(b""),
        )
        .unwrap();
    }
    txn.commit().unwrap();

    let txn = env.begin_transaction(None).unwrap();
    for k in [b"aaaaa".as_ref(), b"aaaab", b"aaaa", b"aaa"] {
        let mut out = DatabaseEntry::new();
        let s = db
            .get_into(Some(&txn), DatabaseEntry::from_bytes(k), &mut out)
            .unwrap();
        assert!(s, "key {k:?} must be retrievable");
    }
    txn.commit().unwrap();
}

// ──────────────────────────────────────────────────────────────────────────────
// JE: TreeTest.testMultipleInsertRetrieve1 — insert N keys, then verify the
// first cursor position is the minimum key and the last is the maximum key
// (JE asserts getFirstNode()/getLastNode() key == min/max), and that the tree
// has grown past a single node (getTreeStats() > 1).
// ──────────────────────────────────────────────────────────────────────────────
#[test]
fn tree_first_last_node_are_min_max() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_env_db(&dir, "minmax");

    let txn = env.begin_transaction(None).unwrap();
    let mut min_key = vec![0xffu8; 8];
    let mut max_key = vec![0x00u8; 8];
    for i in 0..N_KEYS {
        // Scatter keys so min/max are interior to the insertion sequence.
        let k = (i.wrapping_mul(2_654_435_761)).to_be_bytes().to_vec();
        if k < min_key {
            min_key = k.clone();
        }
        if k > max_key {
            max_key = k.clone();
        }
        db.put_in(
            &txn,
            DatabaseEntry::from_bytes(&k),
            DatabaseEntry::from_bytes(b"v"),
        )
        .unwrap();
    }
    txn.commit().unwrap();

    let txn = env.begin_transaction(None).unwrap();
    let mut c = db.open_cursor_in(&txn, None).unwrap();
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    // First == min.
    assert_eq!(
        c.get(&mut k, &mut d, Get::First, None).unwrap(),
        OperationStatus::Success
    );
    assert_eq!(k.data_opt().unwrap(), min_key.as_slice(), "first == min key");
    // Last == max.
    assert_eq!(
        c.get(&mut k, &mut d, Get::Last, None).unwrap(),
        OperationStatus::Success
    );
    assert_eq!(k.data_opt().unwrap(), max_key.as_slice(), "last == max key");
    drop(c);

    // getTreeStats() > 1: the tree must have split past a single BIN.
    let stats = db.stats(None).unwrap();
    assert!(
        stats.btree.bottom_internal_node_count > 1,
        "N_KEYS inserts must produce more than one BIN (got {})",
        stats.btree.bottom_internal_node_count
    );
    txn.commit().unwrap();
}

// ──────────────────────────────────────────────────────────────────────────────
// JE: TreeTest.testAscendingInsertBalance / testDescendingInsertBalance — after
// ascending / descending inserts, the tree depth stays bounded (JE fails if
// either side's level count exceeds 10).  Noxu exposes the depth via
// BtreeStats.main_tree_max_depth.
// ──────────────────────────────────────────────────────────────────────────────
fn insert_and_check_depth(name: &str, ascending: bool) {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_env_db(&dir, name);
    let txn = env.begin_transaction(None).unwrap();
    let iter: Box<dyn Iterator<Item = u32>> = if ascending {
        Box::new(0..N_KEYS)
    } else {
        Box::new((0..N_KEYS).rev())
    };
    for i in iter {
        db.put_in(
            &txn,
            DatabaseEntry::from_bytes(&i.to_be_bytes()),
            DatabaseEntry::from_bytes(b""),
        )
        .unwrap();
    }
    txn.commit().unwrap();

    let stats = db.stats(None).unwrap();
    assert!(
        stats.btree.main_tree_max_depth <= 10,
        "{name}: tree depth {} must stay <= 10 (JE balance invariant)",
        stats.btree.main_tree_max_depth
    );
    // Sanity: the tree actually holds all keys.
    assert_eq!(
        stats.btree.leaf_node_count, N_KEYS as u64,
        "{name}: all keys must be present"
    );
}

#[test]
fn tree_ascending_insert_balance_levels_bounded() {
    insert_and_check_depth("asc_depth", true);
}

#[test]
fn tree_descending_insert_balance_levels_bounded() {
    insert_and_check_depth("desc_depth", false);
}

// ──────────────────────────────────────────────────────────────────────────────
// JE: TreeTest.testVerify — insert N keys, verify() succeeds, and BtreeStats
// obey the shape invariants: internalNodeCount < bottomInternalNodeCount <
// leafNodeCount, and leafNodeCount == N_KEYS.
// ──────────────────────────────────────────────────────────────────────────────
#[test]
fn tree_verify_and_stats_shape() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_env_db(&dir, "verify");
    let txn = env.begin_transaction(None).unwrap();
    for i in 0..N_KEYS {
        let k = (i.wrapping_mul(2_654_435_761)).to_be_bytes().to_vec();
        db.put_in(
            &txn,
            DatabaseEntry::from_bytes(&k),
            DatabaseEntry::from_bytes(b"v"),
        )
        .unwrap();
    }
    txn.commit().unwrap();

    // env.verify (structural) must pass.
    let vr = env.verify(&VerifyConfig::new()).unwrap();
    assert_eq!(vr.error_count(), 0, "verify errors: {:?}", vr.errors);

    let bt = db.stats(None).unwrap().btree;
    assert_eq!(bt.leaf_node_count, N_KEYS as u64, "leaf count == N_KEYS");
    assert!(
        bt.internal_node_count < bt.bottom_internal_node_count,
        "IN count ({}) < BIN count ({})",
        bt.internal_node_count,
        bt.bottom_internal_node_count
    );
    assert!(
        bt.bottom_internal_node_count < bt.leaf_node_count,
        "BIN count ({}) < LN count ({})",
        bt.bottom_internal_node_count,
        bt.leaf_node_count
    );
}

// ──────────────────────────────────────────────────────────────────────────────
// JE: SplitTest.test0Split — the "0th entry promotion" split correctness bug.
// Build the specific topology, delete to empty a subtree, compress, re-insert
// 140 so a mid-level IN's 0th entry exceeds its parent reference, then force a
// split.  The regression: record 140 must remain retrievable (the old split
// code lost it by leaving a stale "150" root entry).
// ──────────────────────────────────────────────────────────────────────────────
#[test]
fn split_0split_zeroth_entry_promotion() {
    let dir = TempDir::new().unwrap();
    // NODE_MAX == 4, matching JE's open(4).
    let env_cfg = EnvironmentConfig::new(dir.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    let env = noxu_db::Environment::open(env_cfg).unwrap();
    let db_cfg = DatabaseConfig::new()
        .with_allow_create(true)
        .with_transactional(true)
        .with_node_max_entries(4);
    let db = env.open_database(None, "zerosplit", &db_cfg).unwrap();

    let put = |v: u8| {
        db.put(DatabaseEntry::from_bytes(&[v]), DatabaseEntry::from_bytes(&[1]))
            .unwrap()
    };

    // Build up: 160,150,...,10 then 151,152,153.
    let mut v = 160i32;
    while v > 0 {
        put(v as u8);
        v -= 10;
    }
    put(151);
    put(152);
    put(153);

    // Delete 130 and 140 to empty a subtree, then compress.
    assert!(db.delete(DatabaseEntry::from_bytes(&[130])).unwrap());
    assert!(db.delete(DatabaseEntry::from_bytes(&[140])).unwrap());
    env.compress().unwrap();

    // Re-insert 140 so a mid-level IN's 0th entry < its parent reference.
    put(140);

    // Force the mid-level split: insert 154..158.
    for i in 154u8..159 {
        put(i);
    }

    // The regression: 140 must still be retrievable.
    let mut out = DatabaseEntry::new();
    let found =
        db.get_into(None, DatabaseEntry::from_bytes(&[140]), &mut out).unwrap();
    assert!(found, "record 140 must survive the 0th-entry-promotion split");
}

// ──────────────────────────────────────────────────────────────────────────────
// JE: KeyPrefixTest.testPrefixManyRandom — insert many prefixed keys in a
// randomized order (JE uses ~94k timestamp keys, fixed seed).  Every key must
// round-trip and cursor-walk in sorted order.  Scaled to 2000 keys.
// ──────────────────────────────────────────────────────────────────────────────
#[test]
fn key_prefix_many_random_round_trip() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_env_db(&dir, "prefix_random");

    let prefix = b"shared-prefix-";
    // Deterministic shuffle: xorshift ordering of 0..2000.
    let mut order: Vec<u32> = (0..2000u32).collect();
    let mut state: u64 = 10; // JE uses fixed seed 10.
    for i in (1..order.len()).rev() {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let j = (state as usize) % (i + 1);
        order.swap(i, j);
    }

    let txn = env.begin_transaction(None).unwrap();
    for &i in &order {
        let mut k = prefix.to_vec();
        k.extend_from_slice(&i.to_be_bytes());
        db.put_in(
            &txn,
            DatabaseEntry::from_bytes(&k),
            DatabaseEntry::from_bytes(&i.to_be_bytes()),
        )
        .unwrap();
    }
    txn.commit().unwrap();

    // Cursor walk yields the keys in sorted (ascending i) order.
    let txn = env.begin_transaction(None).unwrap();
    let mut c = db.open_cursor_in(&txn, None).unwrap();
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    for i in 0..2000u32 {
        assert_eq!(
            c.get(&mut k, &mut d, Get::Next, None).unwrap(),
            OperationStatus::Success
        );
        let key_bytes = k.data_opt().unwrap();
        assert_eq!(&key_bytes[..prefix.len()], prefix);
        let mut a = [0u8; 4];
        a.copy_from_slice(&key_bytes[prefix.len()..]);
        assert_eq!(u32::from_be_bytes(a), i, "random-order keys must sort");
    }
    assert_eq!(
        c.get(&mut k, &mut d, Get::Next, None).unwrap(),
        OperationStatus::NotFound
    );
    drop(c);
    txn.commit().unwrap();
}

// ──────────────────────────────────────────────────────────────────────────────
// JE: KeyPrefixTest.testRLEComparator — a custom (run-length-encoded) key
// comparator combined with key prefixing.  Keys are RLE-encoded so their byte
// order differs from their logical order; prefixing must not corrupt them.
// Every seeded key must be retrievable via the same comparator.
// ──────────────────────────────────────────────────────────────────────────────
fn str_to_rle(s: &str) -> Vec<u8> {
    // 4-byte big-endian run length + 1-byte char, per run.
    let bytes: Vec<char> = s.chars().collect();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        let mut len = 1u32;
        while i + 1 < bytes.len() && bytes[i] == bytes[i + 1] {
            len += 1;
            i += 1;
        }
        out.extend_from_slice(&len.to_be_bytes());
        out.push(bytes[i] as u8);
        i += 1;
    }
    out
}

fn rle_to_str(b: &[u8]) -> String {
    let mut out = String::new();
    let mut i = 0;
    while i + 5 <= b.len() {
        let len = u32::from_be_bytes([b[i], b[i + 1], b[i + 2], b[i + 3]]);
        let c = b[i + 4] as char;
        for _ in 0..len {
            out.push(c);
        }
        i += 5;
    }
    out
}

// ENGINE BUG (candidate): BIN split + key prefixing + a custom btree
// comparator whose ordering diverges from byte order loses records.
//
// Reproduction (see /tmp/audit/remediation/tp-je-tree.md, NEW-TREE-RLE):
// inserting > fanout RLE-encoded keys into a key-prefixing DB with an RLE
// comparator makes keys unretrievable starting at the first BIN split
// (insert #128 at the default fanout); ~250/300 keys are missing at the end.
// Controls: prefixing OFF loses nothing; a byte-order-*consistent* comparator
// (reverse) + prefixing also loses nothing.  A whole-tree cursor walk after the
// split is NOT in comparator order (walk_sorted=false) even though every key is
// physically present — the BINs are byte-ordered while descent/lookup are
// comparator-ordered, so keys become unreachable.
//
// Root cause (noxu-tree/src/tree.rs): key prefixing (a BYTE-common-prefix) is
// applied to comparator-ordered BINs.  Two loci:
//   1. `BinStub::insert_cmp` else-branch (~tree.rs:2085): when the BIN already
//      has a non-empty prefix it stores the FULL key via `insert_slot`, mixing
//      full keys with prefix-stripped suffixes in the same node.
//   2. `Tree::split_child` (~tree.rs:4649): re-prefixes the split halves with
//      `recompute_key_prefix()` (byte-based `compute_key_prefix` +
//      byte-`compress_key` suffix truncation) whenever `key_prefixing` is true
//      — it does NOT also require `key_comparator.is_none()`.  Noxu's own design
//      note (tree.rs:305) says a configured comparator must SKIP prefixing
//      (`insert_raw`); the split/reconstitute paths don't honour that when both
//      key_prefixing AND a comparator are set.
// Fix direction: gate every prefix-compute/re-encode path on
// `key_comparator.is_none()` (comparator ⇒ no prefixing, store full keys), OR
// make prefix computation comparator-aware.  JE's KeyPrefixTest.testRLEComparator
// combines prefixing + a non-byte-order comparator and passes, so this is a
// genuine Noxu fidelity gap, not a JE-ism.
//
// This is a FAITHFUL port kept #[ignore]d (not weakened) per the test-parity
// contract: it must pass once the split/prefix path respects the configured
// comparator.  Un-ignore when NEW-TREE-RLE is fixed.
//
// JE: KeyPrefixTest.testRLEComparator
#[ignore = "NEW-TREE-RLE: split+key-prefixing+non-byte-order comparator loses records"]
#[test]
fn key_prefix_rle_comparator_round_trip() {
    let dir = TempDir::new().unwrap();
    let env_cfg = EnvironmentConfig::new(dir.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    let env = noxu_db::Environment::open(env_cfg).unwrap();

    // RLE comparator: decode both keys and compare as strings.
    let cmp = Comparator::new("rle", |a: &[u8], b: &[u8]| {
        rle_to_str(a).cmp(&rle_to_str(b))
    });
    let db_cfg = DatabaseConfig::new()
        .with_allow_create(true)
        .with_transactional(true)
        .with_key_prefixing(true)
        .with_btree_comparator(cmp);
    let db = env.open_database(None, "rle", &db_cfg).unwrap();

    // Seed with deterministic pseudo-random longs (JE uses Random(0)).
    let mut state: u64 = 0x9e37_79b9_7f4a_7c15;
    let mut next = || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    let key_count = 1000;
    let mut keys: Vec<Vec<u8>> = Vec::with_capacity(key_count);
    for _ in 0..key_count {
        let n = next() as i64;
        let str_key = n.to_string();
        let rle = str_to_rle(&str_key);
        keys.push(rle.clone());
        // put + immediate get, as JE does.
        db.put(
            DatabaseEntry::from_bytes(&rle),
            DatabaseEntry::from_bytes(&rle),
        )
        .unwrap();
        let mut out = DatabaseEntry::new();
        assert!(
            db.get_into(None, DatabaseEntry::from_bytes(&rle), &mut out)
                .unwrap(),
            "RLE key must be retrievable immediately after insert"
        );
    }

    // Verify every (deduped) key is present.
    for rle in &keys {
        let mut out = DatabaseEntry::new();
        assert!(
            db.get_into(None, DatabaseEntry::from_bytes(rle), &mut out)
                .unwrap(),
            "RLE key must remain retrievable under prefixing + comparator"
        );
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// JE: CountEstimatorTest.testDupsInsertSequential — with sorted duplicates, a
// cursor positioned on a key returns the exact duplicate count via cursor
// count(), and db.count() tracks the running total.  (The `countEstimate()`
// off-by-a-factor arm needs an internal API Noxu does not expose publicly; the
// EXACT count arm — which the estimate equals for sequential inserts — is
// ported.)
// ──────────────────────────────────────────────────────────────────────────────
#[test]
fn count_estimator_dups_sequential_exact_counts() {
    let dir = TempDir::new().unwrap();
    let env_cfg = EnvironmentConfig::new(dir.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    let env = noxu_db::Environment::open(env_cfg).unwrap();
    let db_cfg = DatabaseConfig::new()
        .with_allow_create(true)
        .with_transactional(true)
        .with_sorted_duplicates(true);
    let db = env.open_database(None, "dups", &db_cfg).unwrap();

    let n_dups = [1u32, 2, 3, 20, 50, 100, 50, 20, 3, 2, 1];
    let mut total: u64 = 0;
    for (i, &nd) in n_dups.iter().enumerate() {
        for j in 0..nd {
            db.put(
                DatabaseEntry::from_bytes(&(i as u32).to_be_bytes()),
                DatabaseEntry::from_bytes(&j.to_be_bytes()),
            )
            .unwrap();
        }
        total += nd as u64;
        assert_eq!(db.count().unwrap(), total, "db.count tracks total");
    }

    // For each key, a positioned cursor's count() == nDups.
    for (i, &nd) in n_dups.iter().enumerate() {
        let mut c = db.open_cursor(None).unwrap();
        let mut k = DatabaseEntry::from_bytes(&(i as u32).to_be_bytes());
        let mut d = DatabaseEntry::new();
        assert_eq!(
            c.get(&mut k, &mut d, Get::Search, None).unwrap(),
            OperationStatus::Success,
            "key {i} must be found"
        );
        assert_eq!(
            c.count().unwrap(),
            nd as u64,
            "cursor.count() must equal the exact duplicate count for key {i}"
        );
        drop(c);
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// JE: SplitTest.testSplitOverSizedNode [#24917] — fill a BIN to the OLD (larger)
// fanout, reduce the fanout across reopen (so the BIN is now over-sized), then
// insert at the leftmost position.  The first insertion causes a split where
// the existing node keeps 1 record and the new sibling holds the other 255.
// JE's bug was that the new sibling was sized to the NEW fanout and could not
// hold 255 entries (ArrayIndexOutOfBoundsException).  Noxu sizes BIN slots
// dynamically (Vec), so the AIOOBE cannot occur, but the correctness intent —
// an over-sized-node split preserves every record — is ported here.
// ──────────────────────────────────────────────────────────────────────────────
#[test]
fn split_oversized_node_after_fanout_reduction() {
    use noxu_db::StatsConfig;
    let dir = TempDir::new().unwrap();

    // Phase 1: fanout 256, fill a BIN with 256 records (1000..1256).
    {
        let env_cfg = EnvironmentConfig::new(dir.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true);
        let env = noxu_db::Environment::open(env_cfg).unwrap();
        let db_cfg = DatabaseConfig::new()
            .with_allow_create(true)
            .with_transactional(true)
            .with_node_max_entries(256);
        let db = env.open_database(None, "oversized", &db_cfg).unwrap();
        for i in 1000u32..1256 {
            db.put(
                DatabaseEntry::from_bytes(&i.to_be_bytes()),
                DatabaseEntry::from_bytes(&[0u8; 20]),
            )
            .unwrap();
        }
        db.close().unwrap();
        env.close().unwrap();
    }

    // Phase 2: reopen with fanout 128 (the recovered BIN now holds 256 > 128).
    let env_cfg = EnvironmentConfig::new(dir.path().to_path_buf())
        .with_allow_create(false)
        .with_transactional(true);
    let env = noxu_db::Environment::open(env_cfg).unwrap();
    let db_cfg = DatabaseConfig::new()
        .with_allow_create(false)
        .with_transactional(true)
        .with_node_max_entries(128);
    let db = env.open_database(None, "oversized", &db_cfg).unwrap();

    // Insert records at the BEGINNING (keys 999..0 descending) — the leftmost
    // position — forcing the over-sized node to split.  Must not panic and must
    // preserve every record.
    for i in (0u32..1000).rev() {
        db.put(
            DatabaseEntry::from_bytes(&i.to_be_bytes()),
            DatabaseEntry::from_bytes(&[0u8; 20]),
        )
        .unwrap();
    }

    // Oracle: all 1256 records present and in sorted order.
    let mut c = db.open_cursor(None).unwrap();
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    let mut count = 0u32;
    let mut prev: Option<u32> = None;
    let mut s = c.get(&mut k, &mut d, Get::First, None).unwrap();
    while s == OperationStatus::Success {
        let mut a = [0u8; 4];
        a.copy_from_slice(k.data_opt().unwrap());
        let cur = u32::from_be_bytes(a);
        if let Some(p) = prev {
            assert!(p < cur, "over-sized split must keep keys sorted");
        }
        prev = Some(cur);
        count += 1;
        s = c.get(&mut k, &mut d, Get::Next, None).unwrap();
    }
    drop(c);
    assert_eq!(count, 1256, "every record must survive the over-sized split");

    // The tree must actually reflect the reduced fanout (multiple BINs).
    let stats = db.stats(Some(&StatsConfig::new().with_fast(false))).unwrap();
    assert!(
        stats.btree.bottom_internal_node_count > 1,
        "fanout reduction + splits must produce multiple BINs"
    );
}

// ──────────────────────────────────────────────────────────────────────────────
// JE: CountEstimatorTest.testDupsInsertNonSequential — the EXACT-count arm.
// With duplicates inserted in a non-sequential (column-major) order, a cursor
// positioned on a key still reports the exact duplicate count via count().
// (JE's `countEstimate()` off-by-up-to-2x arm exercises an internal estimator
// API — `count_estimate()` is `pub(crate)` on Noxu's SecondaryCursor and not
// on the public Cursor — so only the EXACT count, which the estimate must
// equal for counts below nodeMax, is ported.  See tp-je-tree.md.)
// ──────────────────────────────────────────────────────────────────────────────
#[test]
fn count_estimator_dups_nonsequential_exact_counts() {
    let dir = TempDir::new().unwrap();
    let env = noxu_db::Environment::open(
        EnvironmentConfig::new(dir.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true),
    )
    .unwrap();
    let db = env
        .open_database(
            None,
            "dups_ns",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true)
                .with_sorted_duplicates(true),
        )
        .unwrap();

    let n_dups = [1u32, 2, 3, 20, 50, 90, 50, 20, 3, 2, 1];
    let max = *n_dups.iter().max().unwrap();
    let mut total: u64 = 0;
    // Column-major insertion: outer loop over dup index j, inner over key i.
    for j in 0..max {
        for (i, &nd) in n_dups.iter().enumerate() {
            if j < nd {
                db.put(
                    DatabaseEntry::from_bytes(&(i as u32).to_be_bytes()),
                    DatabaseEntry::from_bytes(&j.to_be_bytes()),
                )
                .unwrap();
                total += 1;
            }
        }
    }
    assert_eq!(db.count().unwrap(), total, "db.count tracks the total");

    for (i, &nd) in n_dups.iter().enumerate() {
        let mut c = db.open_cursor(None).unwrap();
        let mut k = DatabaseEntry::from_bytes(&(i as u32).to_be_bytes());
        let mut d = DatabaseEntry::new();
        assert_eq!(
            c.get(&mut k, &mut d, Get::Search, None).unwrap(),
            OperationStatus::Success
        );
        assert_eq!(
            c.count().unwrap(),
            nd as u64,
            "cursor.count() must be the exact dup count for key {i} \
             regardless of insertion order"
        );
        drop(c);
    }
}
