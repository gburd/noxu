//! The 10 benchmark workloads for Noxu DB.
//!
//! Each function returns the number of logical operations performed so that
//! the caller can compute ns/op and ops/sec.

use noxu_db::{Database, DatabaseEntry, Environment, Get, OperationStatus};
use rand::rngs::SmallRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};

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
pub fn w01_seq_write(db: &Database, n: usize, value: &[u8]) -> usize {
    for i in 0..n {
        let k = DatabaseEntry::from_vec(make_key(i));
        let v = DatabaseEntry::from_bytes(value);
        db.put( &k, &v).unwrap();
    }
    n
}

// ─────────────────────────────────────────────────────────────────────────────
// W02 – Random write
// ─────────────────────────────────────────────────────────────────────────────

/// Insert `n` records with keys 0..n shuffled into random order.
pub fn w02_rand_write(db: &Database, n: usize, value: &[u8]) -> usize {
    let mut rng = SmallRng::seed_from_u64(42);
    let mut keys: Vec<usize> = (0..n).collect();
    keys.shuffle(&mut rng);

    for i in keys {
        let k = DatabaseEntry::from_vec(make_key(i));
        let v = DatabaseEntry::from_bytes(value);
        db.put( &k, &v).unwrap();
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
        let _ = db.get_into(None, &k, &mut data).unwrap();
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
        let _ = db.get_into(None, &k, &mut data).unwrap();
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

        let mut cursor = db.open_cursor(None).unwrap();
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
pub fn w06_write_heavy(db: &Database, n: usize, value: &[u8]) -> usize {
    let mut data = DatabaseEntry::new();
    let v = DatabaseEntry::from_bytes(value);
    for i in 0..n {
        let k = DatabaseEntry::from_vec(make_key(i % n));
        if i % 10 == 9 {
            // 10th operation is a read
            let _ = db.get_into(None, &k, &mut data).unwrap();
        } else {
            // first 9 are writes
            db.put( &k, &v).unwrap();
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
pub fn w07_read_heavy(db: &Database, n: usize, value: &[u8]) -> usize {
    let mut data = DatabaseEntry::new();
    let v = DatabaseEntry::from_bytes(value);
    for i in 0..n {
        let k = DatabaseEntry::from_vec(make_key(i % n));
        if i % 10 == 9 {
            // 10th operation is a write
            db.put( &k, &v).unwrap();
        } else {
            // first 9 are reads
            let _ = db.get_into(None, &k, &mut data).unwrap();
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
pub fn w08_delete_insert(db: &Database, n: usize, value: &[u8]) -> usize {
    let v = DatabaseEntry::from_bytes(value);
    for i in 0..n {
        let k = DatabaseEntry::from_vec(make_key(i));
        let _ = db.delete( &k).unwrap();
        db.put( &k, &v).unwrap();
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
pub fn w09_txn_multi(
    env: &Environment,
    db: &Database,
    n: usize,
    value: &[u8],
) -> usize {
    let v = DatabaseEntry::from_bytes(value);
    for i in 0..n {
        let txn = env.begin_transaction(None).unwrap();

        // 3 gets at key i, i+1, i+2 (wrap around)
        for delta in 0..3usize {
            let k = DatabaseEntry::from_vec(make_key((i + delta) % n));
            let mut data = DatabaseEntry::new();
            let _ = db.get_into(Some(&txn), &k, &mut data).unwrap();
        }

        // 2 puts at key i, i+1 (wrap around)
        for delta in 0..2usize {
            let k = DatabaseEntry::from_vec(make_key((i + delta) % n));
            db.put_in(&txn, &k, &v).unwrap();
        }

        txn.commit().unwrap();
    }
    5 * n
}

// ─────────────────────────────────────────────────────────────────────────────
// W12 – XA two-phase commit
// ─────────────────────────────────────────────────────────────────────────────

/// Full XA 2PC cycle: xa_start → put → xa_end → xa_prepare → xa_commit.
///
/// Each iteration is one complete distributed transaction branch.
/// Returns `n` (one 2PC round trip per iteration).
pub fn w12_xa_2pc(
    xa: &noxu_xa::XaEnvironment,
    db: &Database,
    n: usize,
    value: &[u8],
) -> usize {
    use noxu_xa::{XaFlags, XaResource, Xid};

    let v = DatabaseEntry::from_bytes(value);
    for i in 0..n {
        let gtrid = format!("bench_{:010}", i);
        let xid = Xid::new(1, gtrid.as_bytes(), b"w12").unwrap();

        xa.xa_start(&xid, XaFlags::NOFLAGS).unwrap();

        {
            let txn = xa.get_transaction(&xid).unwrap();
            let k = DatabaseEntry::from_vec(make_key(i % n.max(1)));
            db.put_in(&*txn, &k, &v).unwrap();
            xa.mark_write(&xid).unwrap();
        }

        xa.xa_end(&xid, XaFlags::TMSUCCESS).unwrap();
        xa.xa_prepare(&xid, XaFlags::NOFLAGS).unwrap();
        xa.xa_commit(&xid, XaFlags::NOFLAGS).unwrap();
    }
    n
}

/// Single-phase XA commit (ONEPHASE optimization — skip prepare).
///
/// xa_start → put → xa_end → xa_commit(ONEPHASE).
/// Returns `n`.
pub fn w12_xa_1pc(
    xa: &noxu_xa::XaEnvironment,
    db: &Database,
    n: usize,
    value: &[u8],
) -> usize {
    use noxu_xa::{XaFlags, XaResource, Xid};

    let v = DatabaseEntry::from_bytes(value);
    for i in 0..n {
        let gtrid = format!("bench1p_{:010}", i);
        let xid = Xid::new(1, gtrid.as_bytes(), b"w12_1p").unwrap();

        xa.xa_start(&xid, XaFlags::NOFLAGS).unwrap();

        {
            let txn = xa.get_transaction(&xid).unwrap();
            let k = DatabaseEntry::from_vec(make_key(i % n.max(1)));
            db.put_in(&*txn, &k, &v).unwrap();
            xa.mark_write(&xid).unwrap();
        }

        xa.xa_end(&xid, XaFlags::TMSUCCESS).unwrap();
        xa.xa_commit(&xid, XaFlags::ONEPHASE).unwrap();
    }
    n
}
// ─────────────────────────────────────────────────────────────────────────────
// W13 – Sorted-dup secondary index walk (Wave 11-B)
// ─────────────────────────────────────────────────────────────────────────────
//
// Wave 10-D flagged that no benchmark exercises the sorted-dup secondary
// index path that landed in Wave 2A.  W13 closes that gap.
//
// Scenario: populate a primary DB with N records.  A secondary key
// creator buckets primaries to share secondary keys
// (`bucket = primary_key as u32 % BUCKETS`), so each secondary key
// owns ~N/BUCKETS primaries — exactly the many-primaries-to-one-
// secondary-key shape that sorted-dup secondaries are built for.
//
// Read phase: walk the secondary cursor with `get_first` then repeated
// `get_next` for up to `2 * n` steps.  Operations counted = number of
// (sec_key, primary_key, data) triples actually observed.
//
// Bugs surfaced during Wave 11-B authoring (DO NOT FIX HERE — routed to
// a follow-up bug-fix wave per Wave 11-B / Wave 11-A discipline):
//
//   1. `SecondaryCursor::get_search_key` followed by `get_next_dup_full`
//      triggers the same multi-primary boundary-check bug as
//      `db_cursor_duplicate_test_get_next_dup` in the noxu-db TCK suite,
//      surfaced here as `SecondaryIntegrityException`.
//   2. Once the dup chain under one secondary key spans more than a
//      handful of records, plain `get_first` + repeated `get_next`
//      walks revisit primaries and either yield wrong primary keys
//      (triggering `SecondaryIntegrityException`) or fail to terminate.
//
// Both are real noxu sorted-dup cursor bugs.  W13 therefore caps the
// walk at `2 * n` and treats both natural termination and cap-hit as
// valid completions; the harness reports the actual yield count, which
// tells us whether the walk got farther on noxu or on JE.  As the bugs
// are fixed in subsequent waves, the assertion in the smoke test (and
// the docs/src/operations/benchmarks.md interpretation) will tighten.
//
// JE counterpart: `benches/je-bench/.../w13SecondaryDupWalk` opens a
// SecondaryDatabase with a SecondaryKeyCreator that buckets the primary
// key the same way and walks via `Cursor.getFirst` + `Cursor.getNext`.

use noxu_db::{
    DatabaseConfig, EnvironmentConfig, SecondaryConfig, SecondaryDatabase,
    SecondaryKeyCreator,
};
use noxu_sync::Mutex;
use std::path::Path;
use std::sync::Arc;

/// Number of secondary-key buckets.  100 buckets × 1K..10K primaries
/// gives 10..100 dups per secondary key — the multi-primary regime
/// sorted-dup secondaries are designed for, and the regime that
/// surfaces the noxu cursor bugs documented above.
const W13_BUCKETS: u32 = 100;

/// Buckets primary keys by `key_as_u32 % W13_BUCKETS`.  The primary key
/// is the 10-digit zero-padded decimal produced by `make_key(i)`; we
/// parse it back as a `u32` to derive the bucket id, then encode the
/// bucket id as a 4-byte big-endian secondary key.  Big-endian keeps
/// secondary-key sort order matching numeric order.
struct W13BucketKeyCreator;

impl SecondaryKeyCreator for W13BucketKeyCreator {
    fn create_secondary_key(
        &self,
        _secondary_db: &Database,
        key: &DatabaseEntry,
        _data: &DatabaseEntry,
        result: &mut DatabaseEntry,
    ) -> bool {
        let bytes = key.get_data().unwrap_or(&[]);
        let s = std::str::from_utf8(bytes).unwrap_or("0");
        let n: u32 = s.parse::<u64>().unwrap_or(0) as u32;
        let bucket = n % W13_BUCKETS;
        result.set_data(&bucket.to_be_bytes());
        true
    }
}

/// Open a fresh primary + sorted-dup secondary on the given directory,
/// populate `n` primary records, then open the secondary with
/// `allow_populate=true` so the index is built in one pass.
pub fn w13_setup(
    dir: &Path,
    n: usize,
    value: &[u8],
) -> (Environment, Arc<Mutex<Database>>, SecondaryDatabase) {
    let env_cfg = EnvironmentConfig::new(dir.to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    let env = Environment::open(env_cfg).unwrap();

    let pri_cfg =
        DatabaseConfig::new().with_allow_create(true).with_transactional(true);
    let primary = Arc::new(Mutex::new(
        env.open_database(None, "w13_primary", &pri_cfg).unwrap(),
    ));

    {
        let v = DatabaseEntry::from_bytes(value);
        let pri = primary.lock();
        for i in 0..n {
            let k = DatabaseEntry::from_vec(make_key(i));
            pri.put( &k, &v).unwrap();
        }
    }

    let sec_db_cfg = DatabaseConfig::new()
        .with_allow_create(true)
        .with_sorted_duplicates(true);
    let sec_db = env.open_database(None, "w13_secondary", &sec_db_cfg).unwrap();
    let sec_cfg = SecondaryConfig::new()
        .with_allow_create(true)
        .with_allow_populate(true)
        .with_key_creator(Box::new(W13BucketKeyCreator));
    let secondary =
        SecondaryDatabase::open(Arc::clone(&primary), sec_db, sec_cfg).unwrap();

    (env, primary, secondary)
}

/// W13 read phase: walk the sorted-dup secondary from `get_first` via
/// `get_next`, capped at `2 * n` steps.  The cap defends against the
/// unbounded-loop bug surfaced during Wave 11-B authoring (see module
/// comment above).  Both natural termination and `Err`-from-engine are
/// treated as valid completions; the harness reports the actual yield
/// count.
pub fn w13_secondary_dup_walk(
    secondary: &SecondaryDatabase,
    n: usize,
) -> usize {
    let mut cursor = secondary.open_cursor(None).unwrap();
    let mut sec_key = DatabaseEntry::new();
    let mut p_key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();

    let cap = n.saturating_mul(2).max(1);
    let mut total = 0usize;
    let mut s = match cursor.get_first(&mut sec_key, &mut p_key, &mut data) {
        Ok(s) => s,
        Err(_) => return 0,
    };
    while s == OperationStatus::Success && total < cap {
        total += 1;
        s = match cursor.get_next(&mut sec_key, &mut p_key, &mut data) {
            Ok(s) => s,
            Err(_) => break,
        };
    }
    total
}

/// Single-shot workload entry-point: setup + bounded read walk.
/// Returns the number of (sec_key, primary_key, data) triples observed.
pub fn w13_secondary_dup(env_dir: &Path, n: usize, value: &[u8]) -> usize {
    let (_env, _primary, secondary) = w13_setup(env_dir, n, value);
    w13_secondary_dup_walk(&secondary, n)
}

#[cfg(test)]
mod w13_tests {
    use super::*;
    use tempfile::TempDir;

    /// Smoke test: walk completes within the safety cap and yields
    /// at least one record.  Tightening to `walked == n` is gated on
    /// fixing the noxu sorted-dup cursor bugs documented in the W13
    /// module comment.
    #[test]
    fn w13_smoke_1000() {
        let dir = TempDir::new().unwrap();
        let value = vec![0x58u8; 64];
        let n = 1_000;
        let walked = w13_secondary_dup(dir.path(), n, &value);
        assert!(walked > 0, "W13: walk yielded zero records");
        assert!(
            walked <= 2 * n,
            "W13: walk exceeded the safety cap (got {})",
            walked
        );
    }
}
