//! The 10 benchmark workloads for Noxu DB.
//!
//! Each function returns the number of logical operations performed so that
//! the caller can compute ns/op and ops/sec.

use noxu_db::{Database, DatabaseEntry, Environment, Get, OperationStatus};
use rand::rngs::SmallRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};

/// 64-byte benchmark value used across all workloads.
const VALUE: &[u8] = b"noxu-workload-bench-value-XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX";

/// Build a 10-digit zero-padded decimal key for integer `i`.
///
/// Keys produced this way sort in numeric order under lexicographic comparison
/// because they are all the same width.
#[inline]
fn make_key(i: usize) -> Vec<u8> {
    format!("{:010}", i).into_bytes()
}

// ─────────────────────────────────────────────────────────────────────────────
// W01 – Sequential write
// ─────────────────────────────────────────────────────────────────────────────

/// Insert `n` records with sequential keys 0..n.
pub fn w01_seq_write(db: &Database, n: usize) -> usize {
    for i in 0..n {
        let k = DatabaseEntry::from_vec(make_key(i));
        let v = DatabaseEntry::from_bytes(VALUE);
        db.put(None, &k, &v).unwrap();
    }
    n
}

// ─────────────────────────────────────────────────────────────────────────────
// W02 – Random write
// ─────────────────────────────────────────────────────────────────────────────

/// Insert `n` records with keys 0..n shuffled into random order.
pub fn w02_rand_write(db: &Database, n: usize) -> usize {
    let mut rng = SmallRng::seed_from_u64(42);
    let mut keys: Vec<usize> = (0..n).collect();
    keys.shuffle(&mut rng);

    for i in keys {
        let k = DatabaseEntry::from_vec(make_key(i));
        let v = DatabaseEntry::from_bytes(VALUE);
        db.put(None, &k, &v).unwrap();
    }
    n
}

// ─────────────────────────────────────────────────────────────────────────────
// W03 – Sequential read
// ─────────────────────────────────────────────────────────────────────────────

/// Read all `n` sequential keys in sorted order (key-by-key get).
///
/// Assumes the database has been pre-populated with keys 0..n.
pub fn w03_seq_read(db: &Database, n: usize) -> usize {
    let mut data = DatabaseEntry::new();
    for i in 0..n {
        let k = DatabaseEntry::from_vec(make_key(i));
        let _ = db.get(None, &k, &mut data).unwrap();
    }
    n
}

// ─────────────────────────────────────────────────────────────────────────────
// W04 – Random read
// ─────────────────────────────────────────────────────────────────────────────

/// Perform `n` random `get()` calls with keys sampled uniformly from 0..n.
///
/// Assumes the database has been pre-populated with keys 0..n.
pub fn w04_rand_read(db: &Database, n: usize) -> usize {
    let mut rng = SmallRng::seed_from_u64(99);
    let mut data = DatabaseEntry::new();
    for _ in 0..n {
        let idx: usize = rng.gen_range(0..n);
        let k = DatabaseEntry::from_vec(make_key(idx));
        let _ = db.get(None, &k, &mut data).unwrap();
    }
    n
}

// ─────────────────────────────────────────────────────────────────────────────
// W05 – Range scan
// ─────────────────────────────────────────────────────────────────────────────

/// Perform 100 range scans.
///
/// Each scan starts at a different 1%-boundary of the key space and reads
/// n/100 consecutive records via cursor (SearchGte then Next).
///
/// Assumes the database has been pre-populated with keys 0..n.
/// Returns the total number of records read.
pub fn w05_range_scan(db: &Database, n: usize) -> usize {
    let scan_len = n / 100;
    let mut total_ops = 0usize;

    for scan in 0..100 {
        let start_idx = scan * scan_len;
        let start_bytes = make_key(start_idx);

        let mut cursor = db.open_cursor(None, None).unwrap();
        let mut start_key = DatabaseEntry::from_vec(start_bytes);
        let mut data_e = DatabaseEntry::new();

        // Position cursor at or after start_idx.
        let mut status = cursor
            .get(&mut start_key, &mut data_e, Get::SearchGte, None)
            .unwrap();

        let mut read = 0usize;
        while status == OperationStatus::Success && read < scan_len {
            total_ops += 1;
            read += 1;
            status = cursor
                .get(&mut start_key, &mut data_e, Get::Next, None)
                .unwrap();
        }

        cursor.close().unwrap();
    }

    total_ops
}

// ─────────────────────────────────────────────────────────────────────────────
// W06 – Write-heavy mixed (90% put / 10% get)
// ─────────────────────────────────────────────────────────────────────────────

/// `n` operations: 9 puts then 1 get, cycling through key space.
///
/// Assumes the database has been pre-populated with keys 0..n so that
/// the get operations can find records.
pub fn w06_write_heavy(db: &Database, n: usize) -> usize {
    let mut data = DatabaseEntry::new();
    let v = DatabaseEntry::from_bytes(VALUE);
    for i in 0..n {
        let k = DatabaseEntry::from_vec(make_key(i % n));
        if i % 10 == 9 {
            // 10th operation is a read
            let _ = db.get(None, &k, &mut data).unwrap();
        } else {
            // first 9 are writes
            db.put(None, &k, &v).unwrap();
        }
    }
    n
}

// ─────────────────────────────────────────────────────────────────────────────
// W07 – Read-heavy mixed (90% get / 10% put)
// ─────────────────────────────────────────────────────────────────────────────

/// `n` operations: 9 gets then 1 put, cycling through key space.
///
/// Assumes the database has been pre-populated with keys 0..n.
pub fn w07_read_heavy(db: &Database, n: usize) -> usize {
    let mut data = DatabaseEntry::new();
    let v = DatabaseEntry::from_bytes(VALUE);
    for i in 0..n {
        let k = DatabaseEntry::from_vec(make_key(i % n));
        if i % 10 == 9 {
            // 10th operation is a write
            db.put(None, &k, &v).unwrap();
        } else {
            // first 9 are reads
            let _ = db.get(None, &k, &mut data).unwrap();
        }
    }
    n
}

// ─────────────────────────────────────────────────────────────────────────────
// W08 – Delete + insert
// ─────────────────────────────────────────────────────────────────────────────

/// For each key 0..n: delete then re-insert the record.
///
/// Assumes the database has been pre-populated with keys 0..n.
/// Returns 2*n (one delete + one insert per key).
pub fn w08_delete_insert(db: &Database, n: usize) -> usize {
    let v = DatabaseEntry::from_bytes(VALUE);
    for i in 0..n {
        let k = DatabaseEntry::from_vec(make_key(i));
        let _ = db.delete(None, &k).unwrap();
        db.put(None, &k, &v).unwrap();
    }
    2 * n
}

// ─────────────────────────────────────────────────────────────────────────────
// W09 – Multi-operation transaction
// ─────────────────────────────────────────────────────────────────────────────

/// For each iteration 0..n: begin a transaction, get 3 existing keys, put 2
/// keys, then commit.
///
/// Assumes the database has been pre-populated with keys 0..n.
/// Returns 5*n (3 gets + 2 puts per transaction).
pub fn w09_txn_multi(env: &Environment, db: &Database, n: usize) -> usize {
    let v = DatabaseEntry::from_bytes(VALUE);
    for i in 0..n {
        let txn = env.begin_transaction(None, None).unwrap();

        // 3 gets at key i, i+1, i+2 (wrap around)
        for delta in 0..3usize {
            let k = DatabaseEntry::from_vec(make_key((i + delta) % n));
            let mut data = DatabaseEntry::new();
            let _ = db.get(Some(&txn), &k, &mut data).unwrap();
        }

        // 2 puts at key i, i+1 (wrap around)
        for delta in 0..2usize {
            let k = DatabaseEntry::from_vec(make_key((i + delta) % n));
            db.put(Some(&txn), &k, &v).unwrap();
        }

        txn.commit().unwrap();
    }
    5 * n
}
