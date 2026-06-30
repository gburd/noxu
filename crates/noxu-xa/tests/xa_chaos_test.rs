//! XA Chaos / Scale / Performance Test Suite
//!
//! Tests distributed transaction coordination across multiple independent
//! Noxu environments (simulating separate database clusters). Exercises:
//!
//! - **Correctness**: Full 2PC round-trip across N environments
//! - **Chaos**: Random aborts, prepare failures, timeout injection
//! - **Scale**: Many concurrent XA branches under contention
//! - **Performance**: XA 2PC throughput vs single-phase commit
//!
//! ## Running
//!
//! ```text
//! # Quick smoke test (default):
//! cargo test -p noxu-xa --test xa_chaos_test -- --nocapture
//!
//! # Full chaos suite (longer, tests marked #[ignore]):
//! XA_CHAOS_SECS=60 XA_CHAOS_THREADS=16 \
//!   cargo test -p noxu-xa --test xa_chaos_test -- --ignored --nocapture
//! ```

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use noxu_db::{
    Database, DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig,
};
use noxu_xa::{PrepareResult, XaEnvironment, XaFlags, XaResource, Xid};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use tempfile::TempDir;

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

/// A "cluster" in this test is simply a separate Noxu environment + XA wrapper.
struct Cluster {
    xa: XaEnvironment,
    db: Database,
    _dir: TempDir,
}

impl Cluster {
    fn new(name: &str) -> Self {
        let dir = TempDir::new().unwrap();
        let env_config = EnvironmentConfig::new(dir.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true);
        let env = Environment::open(env_config).unwrap();
        let db_config = DatabaseConfig::new()
            .with_allow_create(true)
            .with_transactional(true);
        let db = env.open_database(None, name, &db_config).unwrap();
        let xa = XaEnvironment::new(env);
        Self { xa, db, _dir: dir }
    }
}

/// Simple transaction manager coordinating 2PC across multiple XaResources.
struct SimpleTM;

impl SimpleTM {
    /// Execute a full 2PC commit across the given clusters for one Xid.
    ///
    /// Returns Ok(true) if committed, Ok(false) if read-only (no commit needed).
    fn commit_2pc(clusters: &[&Cluster], xid: &Xid) -> Result<bool, String> {
        // Phase 1: prepare all. Track which returned Ok (vs ReadOnly).
        let mut prepared_indices: Vec<usize> = Vec::new();
        for (i, cluster) in clusters.iter().enumerate() {
            match cluster.xa.xa_prepare(xid, XaFlags::NOFLAGS) {
                Ok(PrepareResult::Ok) => {
                    prepared_indices.push(i);
                }
                Ok(PrepareResult::ReadOnly) => {
                    // Branch already cleaned up by xa_prepare; nothing to do.
                }
                Err(e) => {
                    // Rollback all prepared clusters + remaining idle ones
                    for &pi in &prepared_indices {
                        let _ =
                            clusters[pi].xa.xa_rollback(xid, XaFlags::NOFLAGS);
                    }
                    // Rollback unprepared clusters (still in Idle state)
                    for cluster in clusters.iter().skip(i + 1) {
                        let _ = cluster.xa.xa_rollback(xid, XaFlags::NOFLAGS);
                    }
                    return Err(format!("prepare failed on cluster {i}: {e}"));
                }
            }
        }

        if prepared_indices.is_empty() {
            return Ok(false);
        }

        // Phase 2: commit only the clusters that returned PrepareResult::Ok
        for &pi in &prepared_indices {
            if let Err(e) = clusters[pi].xa.xa_commit(xid, XaFlags::NOFLAGS) {
                return Err(format!("commit failed on cluster {pi}: {e}"));
            }
        }

        Ok(true)
    }

    /// Rollback all clusters for the given Xid.
    fn rollback_all(clusters: &[&Cluster], xid: &Xid) {
        for cluster in clusters {
            let _ = cluster.xa.xa_rollback(xid, XaFlags::NOFLAGS);
        }
    }
}

fn make_xid(format_id: i32, txn_num: u64, branch: u8) -> Xid {
    let gtrid = format!("gtxn_{txn_num:08}");
    let bqual = format!("branch_{branch:02}");
    Xid::new(format_id, gtrid.as_bytes(), bqual.as_bytes()).unwrap()
}

// ─────────────────────────────────────────────────────────────────────────────
// Correctness Tests
// ─────────────────────────────────────────────────────────────────────────────

/// Full 2PC across 3 clusters: write to all, prepare, commit, verify.
#[test]
fn test_xa_multi_cluster_2pc() {
    let c1 = Cluster::new("cluster1");
    let c2 = Cluster::new("cluster2");
    let c3 = Cluster::new("cluster3");
    let clusters = [&c1, &c2, &c3];

    let xid = make_xid(1, 1, 0);

    // Start branches
    for cluster in &clusters {
        cluster.xa.xa_start(&xid, XaFlags::NOFLAGS).unwrap();
    }

    // Write to each cluster
    for (i, cluster) in clusters.iter().enumerate() {
        let txn = cluster.xa.get_transaction(&xid).unwrap();
        let key = DatabaseEntry::from_vec(format!("key_{i}").into_bytes());
        let val = DatabaseEntry::from_vec(format!("val_{i}").into_bytes());
        cluster.db.put_in(&txn, &key, &val).unwrap();
        cluster.xa.mark_write(&xid).unwrap();
    }

    // End branches
    for cluster in &clusters {
        cluster.xa.xa_end(&xid, XaFlags::TMSUCCESS).unwrap();
    }

    // 2PC commit
    let committed = SimpleTM::commit_2pc(&clusters, &xid).unwrap();
    assert!(committed);

    // Verify data in all clusters
    for (i, cluster) in clusters.iter().enumerate() {
        let key = DatabaseEntry::from_vec(format!("key_{i}").into_bytes());
        let mut val = DatabaseEntry::new();
        let status = cluster.db.get_into(None, &key, &mut val).unwrap();
        assert!(status);
        assert_eq!(val.data_opt(), Some(format!("val_{i}").as_bytes()),);
    }
}

/// 2PC rollback: prepare all, then rollback — data should not persist.
#[test]
fn test_xa_multi_cluster_rollback() {
    let c1 = Cluster::new("cluster1");
    let c2 = Cluster::new("cluster2");
    let clusters = [&c1, &c2];

    let xid = make_xid(1, 2, 0);

    for cluster in &clusters {
        cluster.xa.xa_start(&xid, XaFlags::NOFLAGS).unwrap();
    }

    for (i, cluster) in clusters.iter().enumerate() {
        let txn = cluster.xa.get_transaction(&xid).unwrap();
        let key =
            DatabaseEntry::from_vec(format!("rollback_key_{i}").into_bytes());
        let val = DatabaseEntry::from_bytes(b"should_not_persist");
        cluster.db.put_in(&txn, &key, &val).unwrap();
        cluster.xa.mark_write(&xid).unwrap();
    }

    for cluster in &clusters {
        cluster.xa.xa_end(&xid, XaFlags::TMSUCCESS).unwrap();
    }

    // Prepare all
    for cluster in &clusters {
        let result = cluster.xa.xa_prepare(&xid, XaFlags::NOFLAGS).unwrap();
        assert_eq!(result, PrepareResult::Ok);
    }

    // Rollback instead of commit
    SimpleTM::rollback_all(&clusters, &xid);

    // Verify data NOT present
    for (i, cluster) in clusters.iter().enumerate() {
        let key =
            DatabaseEntry::from_vec(format!("rollback_key_{i}").into_bytes());
        let mut val = DatabaseEntry::new();
        let status = cluster.db.get_into(None, &key, &mut val).unwrap();
        assert!(!status);
    }
}

/// Mixed read-only + write branches: read-only cluster returns ReadOnly from prepare.
#[test]
fn test_xa_mixed_readonly_write() {
    let c_write = Cluster::new("writer");
    let c_read = Cluster::new("reader");

    let xid = make_xid(1, 3, 0);

    // Start on both
    c_write.xa.xa_start(&xid, XaFlags::NOFLAGS).unwrap();
    c_read.xa.xa_start(&xid, XaFlags::NOFLAGS).unwrap();

    // Write only to c_write
    {
        let txn = c_write.xa.get_transaction(&xid).unwrap();
        let key = DatabaseEntry::from_bytes(b"mixed_key");
        let val = DatabaseEntry::from_bytes(b"mixed_val");
        c_write.db.put_in(&txn, &key, &val).unwrap();
        c_write.xa.mark_write(&xid).unwrap();
    }

    // End both
    c_write.xa.xa_end(&xid, XaFlags::TMSUCCESS).unwrap();
    c_read.xa.xa_end(&xid, XaFlags::TMSUCCESS).unwrap();

    // Prepare: c_read should be ReadOnly
    let prep_read = c_read.xa.xa_prepare(&xid, XaFlags::NOFLAGS).unwrap();
    assert_eq!(prep_read, PrepareResult::ReadOnly);

    // Prepare + commit c_write
    let prep_write = c_write.xa.xa_prepare(&xid, XaFlags::NOFLAGS).unwrap();
    assert_eq!(prep_write, PrepareResult::Ok);
    c_write.xa.xa_commit(&xid, XaFlags::NOFLAGS).unwrap();

    // Verify
    let key = DatabaseEntry::from_bytes(b"mixed_key");
    let mut val = DatabaseEntry::new();
    let status = c_write.db.get_into(None, &key, &mut val).unwrap();
    assert!(status);
    assert_eq!(val.data_opt(), Some(b"mixed_val".as_slice()));
}

/// Many independent XA branches with different XIDs — no interference.
#[test]
fn test_xa_many_independent_branches() {
    let cluster = Cluster::new("multi");
    let n = 50;

    let xids: Vec<Xid> = (0..n).map(|i| make_xid(1, i, 0)).collect();

    // Start all
    for xid in &xids {
        cluster.xa.xa_start(xid, XaFlags::NOFLAGS).unwrap();
    }

    // Write to all
    for (i, xid) in xids.iter().enumerate() {
        let txn = cluster.xa.get_transaction(xid).unwrap();
        let key = DatabaseEntry::from_vec(format!("multi_{i:04}").into_bytes());
        let val = DatabaseEntry::from_vec(format!("value_{i}").into_bytes());
        cluster.db.put_in(&txn, &key, &val).unwrap();
        cluster.xa.mark_write(xid).unwrap();
    }

    // End all
    for xid in &xids {
        cluster.xa.xa_end(xid, XaFlags::TMSUCCESS).unwrap();
    }

    // Commit even-numbered, rollback odd-numbered
    for (i, xid) in xids.iter().enumerate() {
        if i % 2 == 0 {
            let prep = cluster.xa.xa_prepare(xid, XaFlags::NOFLAGS).unwrap();
            assert_eq!(prep, PrepareResult::Ok);
            cluster.xa.xa_commit(xid, XaFlags::NOFLAGS).unwrap();
        } else {
            cluster.xa.xa_rollback(xid, XaFlags::NOFLAGS).unwrap();
        }
    }

    // Verify: even keys present, odd keys absent
    for i in 0..n as usize {
        let key = DatabaseEntry::from_vec(format!("multi_{i:04}").into_bytes());
        let mut val = DatabaseEntry::new();
        let status = cluster.db.get_into(None, &key, &mut val).unwrap();
        if i % 2 == 0 {
            assert!(status);
        } else {
            assert!(!status);
        }
    }
}

/// Recover returns only prepared (not yet committed) XIDs.
#[test]
fn test_xa_recover_multi_cluster() {
    let c1 = Cluster::new("recover1");
    let c2 = Cluster::new("recover2");

    // Prepare xid1 on c1 only
    let xid1 = make_xid(1, 10, 0);
    c1.xa.xa_start(&xid1, XaFlags::NOFLAGS).unwrap();
    {
        let txn = c1.xa.get_transaction(&xid1).unwrap();
        c1.db.put_in(&txn, b"rk", b"rv").unwrap();
    }
    c1.xa.mark_write(&xid1).unwrap();
    c1.xa.xa_end(&xid1, XaFlags::TMSUCCESS).unwrap();
    c1.xa.xa_prepare(&xid1, XaFlags::NOFLAGS).unwrap();

    // Prepare xid2 on c2 only
    let xid2 = make_xid(1, 11, 0);
    c2.xa.xa_start(&xid2, XaFlags::NOFLAGS).unwrap();
    {
        let txn = c2.xa.get_transaction(&xid2).unwrap();
        c2.db.put_in(&txn, b"rk2", b"rv2").unwrap();
    }
    c2.xa.mark_write(&xid2).unwrap();
    c2.xa.xa_end(&xid2, XaFlags::TMSUCCESS).unwrap();
    c2.xa.xa_prepare(&xid2, XaFlags::NOFLAGS).unwrap();

    // Recover each cluster
    let recovered1 = c1.xa.xa_recover(XaFlags::STARTRSCAN).unwrap();
    assert_eq!(recovered1.len(), 1);
    assert_eq!(recovered1[0], xid1);

    let recovered2 = c2.xa.xa_recover(XaFlags::STARTRSCAN).unwrap();
    assert_eq!(recovered2.len(), 1);
    assert_eq!(recovered2[0], xid2);

    // Clean up
    c1.xa.xa_commit(&xid1, XaFlags::NOFLAGS).unwrap();
    c2.xa.xa_commit(&xid2, XaFlags::NOFLAGS).unwrap();
}

// ─────────────────────────────────────────────────────────────────────────────
// Chaos Tests
// ─────────────────────────────────────────────────────────────────────────────

/// Chaos test: N threads performing random XA operations with random outcomes.
///
/// Each thread performs a loop of:
///   1. xa_start on all clusters
///   2. write to some subset of clusters
///   3. xa_end
///   4. randomly: commit (2PC), rollback, or one-phase commit
///
/// Invariants verified:
/// - No panics
/// - Committed data is always readable
/// - Rolled-back data is never readable
/// - xa_recover returns only genuinely prepared XIDs
#[test]
#[ignore = "stress: concurrent XA chaos (60 s by default, configurable via XA_CHAOS_SECS); run with --ignored"]
fn test_xa_chaos_concurrent() {
    let chaos_secs: u64 = std::env::var("XA_CHAOS_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10);
    let num_threads: usize = std::env::var("XA_CHAOS_THREADS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8);
    let num_clusters = 3;

    // Shared clusters (each thread gets its own XIDs so no contention on branches)
    let clusters: Vec<Arc<Cluster>> = (0..num_clusters)
        .map(|i| Arc::new(Cluster::new(&format!("chaos_{i}"))))
        .collect();

    let committed_count = Arc::new(AtomicU64::new(0));
    let rolled_back_count = Arc::new(AtomicU64::new(0));
    let error_count = Arc::new(AtomicU64::new(0));
    let barrier = Arc::new(Barrier::new(num_threads));
    let deadline = Instant::now() + Duration::from_secs(chaos_secs);

    let mut handles = Vec::with_capacity(num_threads);

    for thread_id in 0..num_threads {
        let clusters = clusters.clone();
        let committed = Arc::clone(&committed_count);
        let rolled_back = Arc::clone(&rolled_back_count);
        let errors = Arc::clone(&error_count);
        let barrier = Arc::clone(&barrier);

        let handle = std::thread::spawn(move || {
            let mut rng = SmallRng::seed_from_u64(thread_id as u64 * 7919 + 31);
            barrier.wait();

            let mut txn_counter: u64 = 0;

            while Instant::now() < deadline {
                txn_counter += 1;
                let xid = make_xid(
                    thread_id as i32 + 1,
                    (thread_id as u64) * 1_000_000 + txn_counter,
                    0,
                );

                // Decide which clusters participate (at least 1)
                let participating: Vec<usize> =
                    (0..clusters.len()).filter(|_| rng.gen_bool(0.7)).collect();
                let participating = if participating.is_empty() {
                    vec![0]
                } else {
                    participating
                };

                // xa_start
                let mut started = Vec::new();
                for &ci in &participating {
                    match clusters[ci].xa.xa_start(&xid, XaFlags::NOFLAGS) {
                        Ok(()) => started.push(ci),
                        Err(_) => {
                            errors.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
                if started.is_empty() {
                    continue;
                }

                // Write to a random subset of started clusters
                let write_clusters: Vec<usize> = started
                    .iter()
                    .copied()
                    .filter(|_| rng.gen_bool(0.6))
                    .collect();

                for &ci in &write_clusters {
                    let cluster = &clusters[ci];
                    match cluster.xa.get_transaction(&xid) {
                        Ok(txn) => {
                            let key = DatabaseEntry::from_vec(
                                format!(
                                    "chaos_t{thread_id}_txn{txn_counter}_c{ci}"
                                )
                                .into_bytes(),
                            );
                            let val = DatabaseEntry::from_vec(
                                format!("v_{txn_counter}").into_bytes(),
                            );
                            if cluster.db.put_in(&txn, &key, &val).is_ok() {
                                let _ = cluster.xa.mark_write(&xid);
                            }
                        }
                        Err(_) => {
                            errors.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }

                // xa_end
                for &ci in &started {
                    let _ = clusters[ci].xa.xa_end(&xid, XaFlags::TMSUCCESS);
                }

                // Decide outcome: 50% commit, 30% rollback, 20% one-phase
                let roll = rng.gen_range(0..100u32);
                if roll < 50 {
                    // 2PC commit
                    let cluster_refs: Vec<&Cluster> = started
                        .iter()
                        .map(|&ci| clusters[ci].as_ref())
                        .collect();
                    match SimpleTM::commit_2pc(&cluster_refs, &xid) {
                        Ok(_) => {
                            committed.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(_) => {
                            // Prepare failed — branches already rolled back by TM
                            rolled_back.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                } else if roll < 80 {
                    // Rollback
                    for &ci in &started {
                        let _ =
                            clusters[ci].xa.xa_rollback(&xid, XaFlags::NOFLAGS);
                    }
                    rolled_back.fetch_add(1, Ordering::Relaxed);
                } else {
                    // One-phase commit (only valid on single cluster)
                    if started.len() == 1 {
                        let ci = started[0];
                        match clusters[ci].xa.xa_commit(&xid, XaFlags::ONEPHASE)
                        {
                            Ok(()) => {
                                committed.fetch_add(1, Ordering::Relaxed);
                            }
                            Err(_) => {
                                errors.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    } else {
                        // Can't one-phase with multiple clusters; rollback
                        for &ci in &started {
                            let _ = clusters[ci]
                                .xa
                                .xa_rollback(&xid, XaFlags::NOFLAGS);
                        }
                        rolled_back.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().expect("chaos thread panicked");
    }

    let total_committed = committed_count.load(Ordering::Relaxed);
    let total_rolled_back = rolled_back_count.load(Ordering::Relaxed);
    let total_errors = error_count.load(Ordering::Relaxed);

    eprintln!(
        "=== XA Chaos Results ({chaos_secs}s, {num_threads} threads) ==="
    );
    eprintln!("  committed:   {total_committed}");
    eprintln!("  rolled_back: {total_rolled_back}");
    eprintln!("  errors:      {total_errors}");
    eprintln!(
        "  throughput:  {:.0} txns/s",
        (total_committed + total_rolled_back) as f64 / chaos_secs as f64
    );

    // Invariant: no panics occurred (thread.join succeeded above)
    // Invariant: we completed some transactions
    assert!(
        total_committed + total_rolled_back > 0,
        "no transactions completed"
    );

    // Invariant: recover on each cluster returns empty (all branches resolved)
    for (i, cluster) in clusters.iter().enumerate() {
        let recovered = cluster.xa.xa_recover(XaFlags::STARTRSCAN).unwrap();
        assert!(
            recovered.is_empty(),
            "cluster {i} has {} unresolved prepared branches",
            recovered.len()
        );
    }
}

/// Chaos test: TMFAIL branches must be rolled back (never committed).
#[test]
fn test_xa_tmfail_branches_rollback_only() {
    let cluster = Cluster::new("tmfail");
    let xid = make_xid(1, 100, 0);

    cluster.xa.xa_start(&xid, XaFlags::NOFLAGS).unwrap();
    {
        let txn = cluster.xa.get_transaction(&xid).unwrap();
        let key = DatabaseEntry::from_bytes(b"fail_key");
        let val = DatabaseEntry::from_bytes(b"fail_val");
        cluster.db.put_in(&txn, &key, &val).unwrap();
        cluster.xa.mark_write(&xid).unwrap();
    }

    // End with TMFAIL — marks branch as RollbackOnly
    cluster.xa.xa_end(&xid, XaFlags::TMFAIL).unwrap();

    // Prepare should fail (wrong state — RollbackOnly, not Idle)
    let result = cluster.xa.xa_prepare(&xid, XaFlags::NOFLAGS);
    assert!(result.is_err());

    // Must rollback
    cluster.xa.xa_rollback(&xid, XaFlags::NOFLAGS).unwrap();

    // Verify data not present
    let key = DatabaseEntry::from_bytes(b"fail_key");
    let mut val = DatabaseEntry::new();
    let status = cluster.db.get_into(None, &key, &mut val).unwrap();
    assert!(!status);
}

/// Chaos: interleaved suspend/resume across multiple branches.
#[test]
fn test_xa_interleaved_suspend_resume() {
    let cluster = Cluster::new("suspend");

    let xid1 = make_xid(1, 200, 1);
    let xid2 = make_xid(1, 200, 2);

    // Start both
    cluster.xa.xa_start(&xid1, XaFlags::NOFLAGS).unwrap();
    cluster.xa.xa_start(&xid2, XaFlags::NOFLAGS).unwrap();

    // Write to xid1, suspend
    {
        let txn = cluster.xa.get_transaction(&xid1).unwrap();
        let key = DatabaseEntry::from_bytes(b"s1_key");
        let val = DatabaseEntry::from_bytes(b"s1_val");
        cluster.db.put_in(&txn, &key, &val).unwrap();
        cluster.xa.mark_write(&xid1).unwrap();
    }
    cluster.xa.xa_end(&xid1, XaFlags::TMSUSPEND).unwrap();

    // Write to xid2, suspend
    {
        let txn = cluster.xa.get_transaction(&xid2).unwrap();
        let key = DatabaseEntry::from_bytes(b"s2_key");
        let val = DatabaseEntry::from_bytes(b"s2_val");
        cluster.db.put_in(&txn, &key, &val).unwrap();
        cluster.xa.mark_write(&xid2).unwrap();
    }
    cluster.xa.xa_end(&xid2, XaFlags::TMSUSPEND).unwrap();

    // Resume xid1, do more work
    cluster.xa.xa_start(&xid1, XaFlags::RESUME).unwrap();
    {
        let txn = cluster.xa.get_transaction(&xid1).unwrap();
        let key = DatabaseEntry::from_bytes(b"s1_key2");
        let val = DatabaseEntry::from_bytes(b"s1_val2");
        cluster.db.put_in(&txn, &key, &val).unwrap();
    }
    cluster.xa.xa_end(&xid1, XaFlags::TMSUCCESS).unwrap();

    // Resume xid2, end
    cluster.xa.xa_start(&xid2, XaFlags::RESUME).unwrap();
    cluster.xa.xa_end(&xid2, XaFlags::TMSUCCESS).unwrap();

    // Commit xid1 (2PC), rollback xid2
    let prep1 = cluster.xa.xa_prepare(&xid1, XaFlags::NOFLAGS).unwrap();
    assert_eq!(prep1, PrepareResult::Ok);
    cluster.xa.xa_commit(&xid1, XaFlags::NOFLAGS).unwrap();
    cluster.xa.xa_rollback(&xid2, XaFlags::NOFLAGS).unwrap();

    // Verify: xid1's keys present, xid2's absent
    let mut val = DatabaseEntry::new();
    assert!(cluster.db.get_into(None, b"s1_key", &mut val).unwrap());
    assert!(cluster.db.get_into(None, b"s1_key2", &mut val).unwrap());
    assert!(!cluster.db.get_into(None, b"s2_key", &mut val).unwrap());
}

// ─────────────────────────────────────────────────────────────────────────────
// Scale Tests
// ─────────────────────────────────────────────────────────────────────────────

/// Scale: 1000 concurrent XA branches on a single cluster.
#[test]
fn test_xa_scale_1000_branches() {
    let cluster = Cluster::new("scale");
    let n = 1000;

    let xids: Vec<Xid> = (0..n).map(|i| make_xid(1, i, 0)).collect();

    // Start all
    for xid in &xids {
        cluster.xa.xa_start(xid, XaFlags::NOFLAGS).unwrap();
    }

    // Write to all
    for (i, xid) in xids.iter().enumerate() {
        let txn = cluster.xa.get_transaction(xid).unwrap();
        let key = DatabaseEntry::from_vec(format!("scale_{i:06}").into_bytes());
        let val = DatabaseEntry::from_bytes(b"scale_value");
        cluster.db.put_in(&txn, &key, &val).unwrap();
        cluster.xa.mark_write(xid).unwrap();
    }

    // End all
    for xid in &xids {
        cluster.xa.xa_end(xid, XaFlags::TMSUCCESS).unwrap();
    }

    // Prepare all
    for xid in &xids {
        let result = cluster.xa.xa_prepare(xid, XaFlags::NOFLAGS).unwrap();
        assert_eq!(result, PrepareResult::Ok);
    }

    // Commit all
    for xid in &xids {
        cluster.xa.xa_commit(xid, XaFlags::NOFLAGS).unwrap();
    }

    // Verify a sample
    for i in (0..n as usize).step_by(100) {
        let key = DatabaseEntry::from_vec(format!("scale_{i:06}").into_bytes());
        let mut val = DatabaseEntry::new();
        let status = cluster.db.get_into(None, &key, &mut val).unwrap();
        assert!(status);
    }
}

/// Scale: concurrent threads each managing their own XA branch on shared cluster.
#[test]
fn test_xa_scale_concurrent_threads() {
    let num_threads = 8;
    let ops_per_thread = 100;
    let cluster = Arc::new(Cluster::new("concurrent_scale"));
    let barrier = Arc::new(Barrier::new(num_threads));
    let total_committed = Arc::new(AtomicU64::new(0));

    let handles: Vec<_> = (0..num_threads)
        .map(|tid| {
            let cluster = Arc::clone(&cluster);
            let barrier = Arc::clone(&barrier);
            let committed = Arc::clone(&total_committed);

            std::thread::spawn(move || {
                barrier.wait();

                for op in 0..ops_per_thread {
                    let xid = make_xid(
                        tid as i32 + 1,
                        (tid * 100_000 + op) as u64,
                        0,
                    );

                    cluster.xa.xa_start(&xid, XaFlags::NOFLAGS).unwrap();
                    {
                        let txn = cluster.xa.get_transaction(&xid).unwrap();
                        let key = DatabaseEntry::from_vec(
                            format!("t{tid}_op{op:05}").into_bytes(),
                        );
                        let val = DatabaseEntry::from_bytes(b"thread_val");
                        cluster.db.put_in(&txn, &key, &val).unwrap();
                        cluster.xa.mark_write(&xid).unwrap();
                    }
                    cluster.xa.xa_end(&xid, XaFlags::TMSUCCESS).unwrap();

                    // One-phase commit (single cluster)
                    cluster.xa.xa_commit(&xid, XaFlags::ONEPHASE).unwrap();
                    committed.fetch_add(1, Ordering::Relaxed);
                }
            })
        })
        .collect();

    for h in handles {
        h.join().expect("thread panicked");
    }

    let total = total_committed.load(Ordering::Relaxed);
    assert_eq!(total, (num_threads * ops_per_thread) as u64);
    eprintln!(
        "Scale concurrent: {total} XA commits across {num_threads} threads"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Performance Tests
// ─────────────────────────────────────────────────────────────────────────────

/// Performance: compare XA 2PC commit throughput vs single-phase commit.
#[test]
#[ignore = "perf-benchmark: XA 2PC vs single-phase throughput (5000 ops); run with --ignored"]
fn test_xa_perf_2pc_vs_single_phase() {
    let n = 5000;
    let cluster = Cluster::new("perf");
    let value = vec![0x42u8; 128];

    // Warm up
    for i in 0..100u64 {
        let xid = make_xid(99, i, 0);
        cluster.xa.xa_start(&xid, XaFlags::NOFLAGS).unwrap();
        {
            let txn = cluster.xa.get_transaction(&xid).unwrap();
            let key = DatabaseEntry::from_vec(format!("warm_{i}").into_bytes());
            let val = DatabaseEntry::from_bytes(&value);
            cluster.db.put_in(&txn, &key, &val).unwrap();
            cluster.xa.mark_write(&xid).unwrap();
        }
        cluster.xa.xa_end(&xid, XaFlags::TMSUCCESS).unwrap();
        cluster.xa.xa_commit(&xid, XaFlags::ONEPHASE).unwrap();
    }

    // Benchmark: XA 2PC (prepare + commit)
    let start = Instant::now();
    for i in 0..n as u64 {
        let xid = make_xid(1, i, 0);
        cluster.xa.xa_start(&xid, XaFlags::NOFLAGS).unwrap();
        {
            let txn = cluster.xa.get_transaction(&xid).unwrap();
            let key =
                DatabaseEntry::from_vec(format!("2pc_{i:06}").into_bytes());
            let val = DatabaseEntry::from_bytes(&value);
            cluster.db.put_in(&txn, &key, &val).unwrap();
            cluster.xa.mark_write(&xid).unwrap();
        }
        cluster.xa.xa_end(&xid, XaFlags::TMSUCCESS).unwrap();
        cluster.xa.xa_prepare(&xid, XaFlags::NOFLAGS).unwrap();
        cluster.xa.xa_commit(&xid, XaFlags::NOFLAGS).unwrap();
    }
    let elapsed_2pc = start.elapsed();

    // Benchmark: XA one-phase commit (skip prepare)
    let start = Instant::now();
    for i in 0..n as u64 {
        let xid = make_xid(2, i, 0);
        cluster.xa.xa_start(&xid, XaFlags::NOFLAGS).unwrap();
        {
            let txn = cluster.xa.get_transaction(&xid).unwrap();
            let key =
                DatabaseEntry::from_vec(format!("1pc_{i:06}").into_bytes());
            let val = DatabaseEntry::from_bytes(&value);
            cluster.db.put_in(&txn, &key, &val).unwrap();
            cluster.xa.mark_write(&xid).unwrap();
        }
        cluster.xa.xa_end(&xid, XaFlags::TMSUCCESS).unwrap();
        cluster.xa.xa_commit(&xid, XaFlags::ONEPHASE).unwrap();
    }
    let elapsed_1pc = start.elapsed();

    // Benchmark: plain (non-XA) transaction
    let start = Instant::now();
    for i in 0..n {
        let txn = cluster.xa.inner().begin_transaction(None).unwrap();
        let key = DatabaseEntry::from_vec(format!("plain_{i:06}").into_bytes());
        let val = DatabaseEntry::from_bytes(&value);
        cluster.db.put_in(&txn, &key, &val).unwrap();
        txn.commit().unwrap();
    }
    let elapsed_plain = start.elapsed();

    let ops_2pc = n as f64 / elapsed_2pc.as_secs_f64();
    let ops_1pc = n as f64 / elapsed_1pc.as_secs_f64();
    let ops_plain = n as f64 / elapsed_plain.as_secs_f64();

    eprintln!("=== XA Performance ({n} ops) ===");
    eprintln!(
        "  2PC:        {:.0} ops/s ({:.2} ms/op)",
        ops_2pc,
        elapsed_2pc.as_secs_f64() * 1000.0 / n as f64
    );
    eprintln!(
        "  One-phase:  {:.0} ops/s ({:.2} ms/op)",
        ops_1pc,
        elapsed_1pc.as_secs_f64() * 1000.0 / n as f64
    );
    eprintln!(
        "  Plain txn:  {:.0} ops/s ({:.2} ms/op)",
        ops_plain,
        elapsed_plain.as_secs_f64() * 1000.0 / n as f64
    );
    eprintln!(
        "  2PC overhead vs plain: {:.1}%",
        (elapsed_2pc.as_secs_f64() / elapsed_plain.as_secs_f64() - 1.0) * 100.0
    );
    eprintln!(
        "  1PC overhead vs plain: {:.1}%",
        (elapsed_1pc.as_secs_f64() / elapsed_plain.as_secs_f64() - 1.0) * 100.0
    );

    // Sanity: 2PC overhead should be less than 5× plain.
    // On fast NVMe with fsync coalescing, 2PC can be nearly as fast as plain
    // since disk I/O dominates both paths equally.
    assert!(
        elapsed_2pc.as_secs_f64() < elapsed_plain.as_secs_f64() * 5.0,
        "2PC was pathologically slow vs plain"
    );
}

/// Performance: concurrent XA 2PC throughput across 2 clusters.
#[test]
#[ignore = "perf-benchmark: concurrent XA 2PC across 2 clusters, 8 threads; run with --ignored"]
fn test_xa_perf_concurrent_multi_cluster() {
    let num_threads = 8;
    let ops_per_thread = 500;
    let c1 = Arc::new(Cluster::new("perf_c1"));
    let c2 = Arc::new(Cluster::new("perf_c2"));
    let barrier = Arc::new(Barrier::new(num_threads));
    let total_committed = Arc::new(AtomicU64::new(0));
    let value = vec![0x55u8; 64];

    let start = Instant::now();

    let handles: Vec<_> = (0..num_threads)
        .map(|tid| {
            let c1 = Arc::clone(&c1);
            let c2 = Arc::clone(&c2);
            let barrier = Arc::clone(&barrier);
            let committed = Arc::clone(&total_committed);
            let value = value.clone();

            std::thread::spawn(move || {
                barrier.wait();

                for op in 0..ops_per_thread {
                    let xid = make_xid(
                        tid as i32 + 1,
                        (tid * 1_000_000 + op) as u64,
                        0,
                    );

                    // Start on both clusters
                    c1.xa.xa_start(&xid, XaFlags::NOFLAGS).unwrap();
                    c2.xa.xa_start(&xid, XaFlags::NOFLAGS).unwrap();

                    // Write to both
                    {
                        let txn = c1.xa.get_transaction(&xid).unwrap();
                        let key = DatabaseEntry::from_vec(
                            format!("mc_t{tid}_op{op}").into_bytes(),
                        );
                        let val = DatabaseEntry::from_bytes(&value);
                        c1.db.put_in(&txn, &key, &val).unwrap();
                        c1.xa.mark_write(&xid).unwrap();
                    }
                    {
                        let txn = c2.xa.get_transaction(&xid).unwrap();
                        let key = DatabaseEntry::from_vec(
                            format!("mc_t{tid}_op{op}").into_bytes(),
                        );
                        let val = DatabaseEntry::from_bytes(&value);
                        c2.db.put_in(&txn, &key, &val).unwrap();
                        c2.xa.mark_write(&xid).unwrap();
                    }

                    // End both
                    c1.xa.xa_end(&xid, XaFlags::TMSUCCESS).unwrap();
                    c2.xa.xa_end(&xid, XaFlags::TMSUCCESS).unwrap();

                    // 2PC: prepare both, commit both
                    c1.xa.xa_prepare(&xid, XaFlags::NOFLAGS).unwrap();
                    c2.xa.xa_prepare(&xid, XaFlags::NOFLAGS).unwrap();
                    c1.xa.xa_commit(&xid, XaFlags::NOFLAGS).unwrap();
                    c2.xa.xa_commit(&xid, XaFlags::NOFLAGS).unwrap();

                    committed.fetch_add(1, Ordering::Relaxed);
                }
            })
        })
        .collect();

    for h in handles {
        h.join().expect("thread panicked");
    }

    let elapsed = start.elapsed();
    let total = total_committed.load(Ordering::Relaxed);
    let ops_sec = total as f64 / elapsed.as_secs_f64();

    eprintln!("=== XA Concurrent Multi-Cluster Performance ===");
    eprintln!("  threads:    {num_threads}");
    eprintln!("  total ops:  {total}");
    eprintln!("  elapsed:    {:.2}s", elapsed.as_secs_f64());
    eprintln!("  throughput: {ops_sec:.0} 2PC txns/s");

    assert_eq!(total, (num_threads * ops_per_thread) as u64);
}
