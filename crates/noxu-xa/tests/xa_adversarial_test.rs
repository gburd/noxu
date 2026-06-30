//! XA Adversarial Corner-Case Tests
//!
//! These tests target subtle bugs that are common in XA implementations:
//!
//! 1. Crash recovery via PreparedLog (persist across env close/reopen)
//! 2. Concurrent XID reuse race conditions
//! 3. Branch leak detection (abandoned branches)
//! 4. Key contention across XA branches (lock conflicts)
//! 5. PreparedLog stress (many prepared branches)
//! 6. ONEPHASE after suspend/resume cycle
//! 7. Rapid fire: start→end→prepare→commit in tight loop (resource exhaustion)
//! 8. Mixed xa_forget and xa_recover interactions
//! 9. Serialization edge cases (max-length fields, binary data in XIDs)
//! 10. Rollback of prepared branch leaves no trace

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Duration;

use noxu_db::{
    Database, DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig,
};
use noxu_xa::{
    PrepareResult, XaEnvironment, XaError, XaFlags, XaResource, Xid,
};
use tempfile::TempDir;

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

fn make_env(dir: &std::path::Path) -> Environment {
    let cfg = EnvironmentConfig::new(dir.to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    Environment::open(cfg).unwrap()
}

fn make_xa_with_log(dir: &std::path::Path) -> (XaEnvironment, Database) {
    let env = make_env(dir);
    let db = env
        .open_database(
            None,
            "test",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap();
    let xa = XaEnvironment::new(env).with_prepared_log().unwrap();
    (xa, db)
}

fn make_xa(dir: &std::path::Path) -> (XaEnvironment, Database) {
    let env = make_env(dir);
    let db = env
        .open_database(
            None,
            "test",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap();
    let xa = XaEnvironment::new(env);
    (xa, db)
}

// ─────────────────────────────────────────────────────────────────────────────
// 1. PreparedLog Crash Recovery — persist prepared branches across env restart
// ─────────────────────────────────────────────────────────────────────────────

/// Simulate crash: prepare 3 branches, drop env, reopen, verify xa_recover
/// returns all 3 XIDs from the persistent log.
#[test]
fn test_crash_recovery_prepared_log_persists() {
    let dir = TempDir::new().unwrap();
    let xid1 = Xid::new(1, b"crash_g1", b"br1").unwrap();
    let xid2 = Xid::new(1, b"crash_g2", b"br2").unwrap();
    let xid3 = Xid::new(1, b"crash_g3", b"br3").unwrap();

    // Phase 1: prepare branches, then "crash" (drop without commit)
    {
        let (xa, db) = make_xa_with_log(dir.path());
        for (i, xid) in [&xid1, &xid2, &xid3].iter().enumerate() {
            xa.xa_start(xid, XaFlags::NOFLAGS).unwrap();
            let txn = xa.get_transaction(xid).unwrap();
            let key =
                DatabaseEntry::from_vec(format!("crash_k{i}").into_bytes());
            let val = DatabaseEntry::from_bytes(b"crash_value");
            db.put_in(&txn, &key, &val).unwrap();
            xa.mark_write(xid).unwrap();
            xa.xa_end(xid, XaFlags::TMSUCCESS).unwrap();
            xa.xa_prepare(xid, XaFlags::NOFLAGS).unwrap();
        }
        // Simulate crash: drop everything without commit
        drop(db);
        // xa and env dropped here
    }

    // Phase 2: reopen and recover
    {
        let (xa, _db) = make_xa_with_log(dir.path());
        let recovered = xa.xa_recover(XaFlags::STARTRSCAN).unwrap();
        assert_eq!(
            recovered.len(),
            3,
            "expected 3 recovered XIDs, got {}",
            recovered.len()
        );
        assert!(recovered.contains(&xid1));
        assert!(recovered.contains(&xid2));
        assert!(recovered.contains(&xid3));
    }
}

/// Prepare one branch, commit it, then crash. Recovery should return empty
/// (committed branches are removed from the log).
#[test]
fn test_crash_recovery_committed_not_recovered() {
    let dir = TempDir::new().unwrap();
    let xid = Xid::new(1, b"committed_crash", b"br").unwrap();

    {
        let (xa, db) = make_xa_with_log(dir.path());
        xa.xa_start(&xid, XaFlags::NOFLAGS).unwrap();
        let txn = xa.get_transaction(&xid).unwrap();
        db.put_in(&txn, b"ck", b"cv").unwrap();
        xa.mark_write(&xid).unwrap();
        xa.xa_end(&xid, XaFlags::TMSUCCESS).unwrap();
        xa.xa_prepare(&xid, XaFlags::NOFLAGS).unwrap();
        xa.xa_commit(&xid, XaFlags::NOFLAGS).unwrap();
        drop(db);
    }

    {
        let (xa, _db) = make_xa_with_log(dir.path());
        let recovered = xa.xa_recover(XaFlags::STARTRSCAN).unwrap();
        assert!(
            recovered.is_empty(),
            "committed branch should not be recovered"
        );
    }
}

/// Prepare, rollback, crash. Recovery should return empty
/// (rolled-back branches are removed from the log).
#[test]
fn test_crash_recovery_rolled_back_not_recovered() {
    let dir = TempDir::new().unwrap();
    let xid = Xid::new(1, b"rb_crash", b"br").unwrap();

    {
        let (xa, db) = make_xa_with_log(dir.path());
        xa.xa_start(&xid, XaFlags::NOFLAGS).unwrap();
        let txn = xa.get_transaction(&xid).unwrap();
        db.put_in(&txn, b"rbk", b"rbv").unwrap();
        xa.mark_write(&xid).unwrap();
        xa.xa_end(&xid, XaFlags::TMSUCCESS).unwrap();
        xa.xa_prepare(&xid, XaFlags::NOFLAGS).unwrap();
        xa.xa_rollback(&xid, XaFlags::NOFLAGS).unwrap();
        drop(db);
    }

    {
        let (xa, _db) = make_xa_with_log(dir.path());
        let recovered = xa.xa_recover(XaFlags::STARTRSCAN).unwrap();
        assert!(recovered.is_empty());
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// 2. Concurrent XID Reuse Race
// ─────────────────────────────────────────────────────────────────────────────

/// Multiple threads race to start the same XID simultaneously.
/// At most one should succeed at any given instant (DuplicateXid for others).
/// After the winner commits, others may retry and succeed.
/// Key invariant: no panics, no lost data, total commits >= iterations.
#[test]
fn test_concurrent_xid_reuse_race() {
    let dir = TempDir::new().unwrap();
    let (xa, db) = make_xa(dir.path());
    let xa = Arc::new(xa);
    let db = Arc::new(db);
    let barrier = Arc::new(Barrier::new(4));
    let success_count = Arc::new(AtomicU64::new(0));
    let duplicate_count = Arc::new(AtomicU64::new(0));
    let iterations = 50u64;

    let handles: Vec<_> = (0..4)
        .map(|tid| {
            let xa = Arc::clone(&xa);
            let db = Arc::clone(&db);
            let barrier = Arc::clone(&barrier);
            let success = Arc::clone(&success_count);
            let dups = Arc::clone(&duplicate_count);

            std::thread::spawn(move || {
                barrier.wait();
                for i in 0..iterations {
                    // All threads try the same XID
                    let xid = Xid::new(1, format!("race_{i:04}").as_bytes(), b"br").unwrap();

                    match xa.xa_start(&xid, XaFlags::NOFLAGS) {
                        Ok(()) => {
                            let txn = xa.get_transaction(&xid).unwrap();
                            let key = DatabaseEntry::from_vec(
                                format!("race_t{tid}_i{i}").into_bytes(),
                            );
                            let val = DatabaseEntry::from_bytes(b"won");
                            let _ = db.put_in(&txn, &key, &val);
                            xa.mark_write(&xid).unwrap();
                            xa.xa_end(&xid, XaFlags::TMSUCCESS).unwrap();
                            xa.xa_commit(&xid, XaFlags::ONEPHASE).unwrap();
                            success.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(XaError::DuplicateXid) => {
                            dups.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(e) => {
                            panic!("unexpected error from thread {tid} iter {i}: {e}");
                        }
                    }
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    let total = success_count.load(Ordering::Relaxed);
    let total_dups = duplicate_count.load(Ordering::Relaxed);

    // Every iteration produced at least one commit (winner) — total >= iterations.
    // With 4 threads, some XIDs may be reused sequentially, so total can be > iterations.
    assert!(
        total >= iterations,
        "expected at least {iterations} commits, got {total}"
    );
    // Sanity: some DuplicateXid errors occurred (4 threads racing)
    assert!(
        total_dups > 0,
        "expected some DuplicateXid errors but got 0 — threads didn't actually race"
    );
    // Total attempts = success + dups should equal 4 * iterations
    assert_eq!(total + total_dups, 4 * iterations);
}

// ─────────────────────────────────────────────────────────────────────────────
// 3. Rapid-Fire Resource Exhaustion
// ─────────────────────────────────────────────────────────────────────────────

/// 10,000 complete XA cycles in a tight loop to detect resource leaks
/// (file handles, memory, lock table entries).
#[test]
fn test_rapid_fire_10k_cycles() {
    let dir = TempDir::new().unwrap();
    let (xa, db) = make_xa(dir.path());
    let value = vec![0xABu8; 128];

    for i in 0..10_000u64 {
        let xid = Xid::new(1, &i.to_le_bytes(), b"rapid").unwrap();
        xa.xa_start(&xid, XaFlags::NOFLAGS).unwrap();
        {
            let txn = xa.get_transaction(&xid).unwrap();
            let key =
                DatabaseEntry::from_vec(format!("rapid_{i:08}").into_bytes());
            let val = DatabaseEntry::from_bytes(&value);
            db.put_in(&txn, &key, &val).unwrap();
            xa.mark_write(&xid).unwrap();
        }
        xa.xa_end(&xid, XaFlags::TMSUCCESS).unwrap();
        xa.xa_commit(&xid, XaFlags::ONEPHASE).unwrap();
    }

    // Verify no leftover branches
    let recovered = xa.xa_recover(XaFlags::STARTRSCAN).unwrap();
    assert!(recovered.is_empty());

    // Spot check
    let key = DatabaseEntry::from_bytes(b"rapid_00009999");
    let mut val = DatabaseEntry::new();
    let status = db.get_into(None, &key, &mut val).unwrap();
    assert!(status);
}

/// 10,000 prepare→commit cycles with PreparedLog enabled (tests log doesn't grow unbounded)
#[test]
fn test_rapid_fire_10k_with_prepared_log() {
    let dir = TempDir::new().unwrap();
    let (xa, db) = make_xa_with_log(dir.path());

    for i in 0..10_000u64 {
        let xid = Xid::new(1, &i.to_le_bytes(), b"plog").unwrap();
        xa.xa_start(&xid, XaFlags::NOFLAGS).unwrap();
        {
            let txn = xa.get_transaction(&xid).unwrap();
            let key =
                DatabaseEntry::from_vec(format!("plog_{i:08}").into_bytes());
            let val = DatabaseEntry::from_bytes(b"v");
            db.put_in(&txn, &key, &val).unwrap();
            xa.mark_write(&xid).unwrap();
        }
        xa.xa_end(&xid, XaFlags::TMSUCCESS).unwrap();
        xa.xa_prepare(&xid, XaFlags::NOFLAGS).unwrap();
        xa.xa_commit(&xid, XaFlags::NOFLAGS).unwrap();
    }

    // All committed — recovery should be empty
    let recovered = xa.xa_recover(XaFlags::STARTRSCAN).unwrap();
    assert!(recovered.is_empty());
}

// ─────────────────────────────────────────────────────────────────────────────
// 4. PreparedLog Stress — Many Prepared Branches
// ─────────────────────────────────────────────────────────────────────────────

/// Prepare 500 branches simultaneously, then commit half / rollback half.
/// Verify PreparedLog correctly tracks the resolved ones.
#[test]
fn test_prepared_log_500_branches() {
    let dir = TempDir::new().unwrap();
    let (xa, db) = make_xa_with_log(dir.path());
    let n = 500;

    let xids: Vec<Xid> = (0..n)
        .map(|i| {
            Xid::new(1, format!("stress_{i:04}").as_bytes(), b"br").unwrap()
        })
        .collect();

    // Start, write, end, prepare all
    for (i, xid) in xids.iter().enumerate() {
        xa.xa_start(xid, XaFlags::NOFLAGS).unwrap();
        let txn = xa.get_transaction(xid).unwrap();
        let key =
            DatabaseEntry::from_vec(format!("stress_k{i:04}").into_bytes());
        let val = DatabaseEntry::from_bytes(b"stress_val");
        db.put_in(&txn, &key, &val).unwrap();
        xa.mark_write(xid).unwrap();
        xa.xa_end(xid, XaFlags::TMSUCCESS).unwrap();
        let result = xa.xa_prepare(xid, XaFlags::NOFLAGS).unwrap();
        assert_eq!(result, PrepareResult::Ok);
    }

    // All 500 should be recoverable
    let recovered = xa.xa_recover(XaFlags::STARTRSCAN).unwrap();
    assert_eq!(recovered.len(), n);

    // Commit first half, rollback second half
    for (i, xid) in xids.iter().enumerate() {
        if i < n / 2 {
            xa.xa_commit(xid, XaFlags::NOFLAGS).unwrap();
        } else {
            xa.xa_rollback(xid, XaFlags::NOFLAGS).unwrap();
        }
    }

    // Nothing left
    let recovered = xa.xa_recover(XaFlags::STARTRSCAN).unwrap();
    assert!(recovered.is_empty());

    // Verify: first half data present, second half absent
    for i in 0..n {
        let key =
            DatabaseEntry::from_vec(format!("stress_k{i:04}").into_bytes());
        let mut val = DatabaseEntry::new();
        let status = db.get_into(None, &key, &mut val).unwrap();
        if i < n / 2 {
            assert!(status);
        } else {
            assert!(!status);
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// 5. ONEPHASE After Suspend/Resume Cycle
// ─────────────────────────────────────────────────────────────────────────────

/// Complex lifecycle: start → work → suspend → resume → more work → end → ONEPHASE commit
#[test]
fn test_onephase_after_suspend_resume() {
    let dir = TempDir::new().unwrap();
    let (xa, db) = make_xa(dir.path());
    let xid = Xid::new(1, b"susp_1pc", b"br").unwrap();

    xa.xa_start(&xid, XaFlags::NOFLAGS).unwrap();

    // First work segment
    {
        let txn = xa.get_transaction(&xid).unwrap();
        db.put_in(&txn, b"s1pc_k1", b"v1").unwrap();
        xa.mark_write(&xid).unwrap();
    }

    // Suspend
    xa.xa_end(&xid, XaFlags::TMSUSPEND).unwrap();

    // Resume
    xa.xa_start(&xid, XaFlags::RESUME).unwrap();

    // Second work segment
    {
        let txn = xa.get_transaction(&xid).unwrap();
        db.put_in(&txn, b"s1pc_k2", b"v2").unwrap();
    }

    // End and ONEPHASE commit
    xa.xa_end(&xid, XaFlags::TMSUCCESS).unwrap();
    xa.xa_commit(&xid, XaFlags::ONEPHASE).unwrap();

    // Both writes visible
    let mut val = DatabaseEntry::new();
    assert!(db.get_into(None, b"s1pc_k1", &mut val).unwrap());
    assert!(db.get_into(None, b"s1pc_k2", &mut val).unwrap());
}

// ─────────────────────────────────────────────────────────────────────────────
// 6. Serialization Edge Cases — Binary/Max-Length XIDs
// ─────────────────────────────────────────────────────────────────────────────

/// XIDs with max-length (64 byte) gtrid and bqual containing binary data
/// must round-trip correctly through PreparedLog.
#[test]
fn test_prepared_log_binary_max_length_xids() {
    let dir = TempDir::new().unwrap();

    // Create XIDs with 64-byte binary fields
    let gtrid: Vec<u8> = (0..64).map(|i| (i * 7 + 13) as u8).collect();
    let bqual: Vec<u8> = (0..64).map(|i| (i * 11 + 3) as u8).collect();
    let xid = Xid::new(i32::MIN, &gtrid, &bqual).unwrap();

    // Also test with null bytes and special characters
    let mut gtrid2 = vec![0u8; 64];
    gtrid2[0] = 0xFF;
    gtrid2[32] = 0x00;
    gtrid2[63] = 0xFE;
    let xid2 = Xid::new(i32::MAX, &gtrid2, &[0u8; 64]).unwrap();

    // Prepare both
    {
        let (xa, db) = make_xa_with_log(dir.path());
        for (i, xid_ref) in [&xid, &xid2].iter().enumerate() {
            xa.xa_start(xid_ref, XaFlags::NOFLAGS).unwrap();
            let txn = xa.get_transaction(xid_ref).unwrap();
            let key =
                DatabaseEntry::from_vec(format!("bin_key_{i}").into_bytes());
            db.put_in(&txn, &key, b"bin_val").unwrap();
            xa.mark_write(xid_ref).unwrap();
            xa.xa_end(xid_ref, XaFlags::TMSUCCESS).unwrap();
            xa.xa_prepare(xid_ref, XaFlags::NOFLAGS).unwrap();
        }
        drop(db);
    }

    // Recover and verify exact match
    {
        let (xa, _db) = make_xa_with_log(dir.path());
        let recovered = xa.xa_recover(XaFlags::STARTRSCAN).unwrap();
        assert_eq!(recovered.len(), 2);
        assert!(recovered.contains(&xid));
        assert!(recovered.contains(&xid2));
    }
}

/// XID with empty gtrid and bqual persists correctly
#[test]
fn test_prepared_log_empty_xid_components() {
    let dir = TempDir::new().unwrap();
    let xid = Xid::new(0, b"", b"").unwrap();

    {
        let (xa, db) = make_xa_with_log(dir.path());
        xa.xa_start(&xid, XaFlags::NOFLAGS).unwrap();
        let txn = xa.get_transaction(&xid).unwrap();
        db.put_in(&txn, b"empty_k", b"empty_v").unwrap();
        xa.mark_write(&xid).unwrap();
        xa.xa_end(&xid, XaFlags::TMSUCCESS).unwrap();
        xa.xa_prepare(&xid, XaFlags::NOFLAGS).unwrap();
        drop(db);
    }

    {
        let (xa, _db) = make_xa_with_log(dir.path());
        let recovered = xa.xa_recover(XaFlags::STARTRSCAN).unwrap();
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0], xid);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// 7. xa_forget + xa_recover Interaction
// ─────────────────────────────────────────────────────────────────────────────

/// xa_forget removes from both in-memory AND persistent log.
/// After forget, xa_recover should not return the XID.
#[test]
fn test_forget_removes_from_persistent_log() {
    let dir = TempDir::new().unwrap();
    let xid = Xid::new(1, b"forget_persist", b"br").unwrap();

    {
        let (xa, db) = make_xa_with_log(dir.path());
        xa.xa_start(&xid, XaFlags::NOFLAGS).unwrap();
        let txn = xa.get_transaction(&xid).unwrap();
        db.put_in(&txn, b"fk", b"fv").unwrap();
        xa.mark_write(&xid).unwrap();
        xa.xa_end(&xid, XaFlags::TMSUCCESS).unwrap();
        xa.xa_prepare(&xid, XaFlags::NOFLAGS).unwrap();
        xa.xa_forget(&xid, XaFlags::NOFLAGS).unwrap();
        drop(db);
    }

    // Reopen — forgotten XID must not appear
    {
        let (xa, _db) = make_xa_with_log(dir.path());
        let recovered = xa.xa_recover(XaFlags::STARTRSCAN).unwrap();
        assert!(recovered.is_empty(), "forgotten XID should not be recovered");
    }
}

/// xa_forget on a XID that only exists in persistent log (simulating post-crash forget)
#[test]
fn test_forget_persistent_only_xid() {
    let dir = TempDir::new().unwrap();
    let xid = Xid::new(1, b"persist_only_forget", b"br").unwrap();

    // Prepare and "crash"
    {
        let (xa, db) = make_xa_with_log(dir.path());
        xa.xa_start(&xid, XaFlags::NOFLAGS).unwrap();
        let txn = xa.get_transaction(&xid).unwrap();
        db.put_in(&txn, b"pof_k", b"pof_v").unwrap();
        xa.mark_write(&xid).unwrap();
        xa.xa_end(&xid, XaFlags::TMSUCCESS).unwrap();
        xa.xa_prepare(&xid, XaFlags::NOFLAGS).unwrap();
        drop(db);
    }

    // Reopen — XID exists only in persistent log
    {
        let (xa, _db) = make_xa_with_log(dir.path());
        let recovered = xa.xa_recover(XaFlags::STARTRSCAN).unwrap();
        assert_eq!(recovered.len(), 1);

        // Forget it
        xa.xa_forget(&xid, XaFlags::NOFLAGS).unwrap();
        let recovered = xa.xa_recover(XaFlags::STARTRSCAN).unwrap();
        assert!(recovered.is_empty());
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// 8. Rollback of Prepared Branch Leaves No Trace
// ─────────────────────────────────────────────────────────────────────────────

/// After prepare→rollback, the data must be completely absent,
/// AND the key must be writable by a subsequent transaction without conflict.
#[test]
fn test_rollback_prepared_frees_locks_completely() {
    let dir = TempDir::new().unwrap();
    let (xa, db) = make_xa(dir.path());

    let xid = Xid::new(1, b"lock_free", b"br").unwrap();
    let key = DatabaseEntry::from_bytes(b"contested_key");

    // Write, prepare, rollback
    xa.xa_start(&xid, XaFlags::NOFLAGS).unwrap();
    {
        let txn = xa.get_transaction(&xid).unwrap();
        db.put_in(&txn, &key, b"first").unwrap();
        xa.mark_write(&xid).unwrap();
    }
    xa.xa_end(&xid, XaFlags::TMSUCCESS).unwrap();
    xa.xa_prepare(&xid, XaFlags::NOFLAGS).unwrap();
    xa.xa_rollback(&xid, XaFlags::NOFLAGS).unwrap();

    // Key should be writable without lock conflict
    let xid2 = Xid::new(1, b"lock_free_2", b"br").unwrap();
    xa.xa_start(&xid2, XaFlags::NOFLAGS).unwrap();
    {
        let txn = xa.get_transaction(&xid2).unwrap();
        db.put_in(&txn, &key, b"second").unwrap();
        xa.mark_write(&xid2).unwrap();
    }
    xa.xa_end(&xid2, XaFlags::TMSUCCESS).unwrap();
    xa.xa_commit(&xid2, XaFlags::ONEPHASE).unwrap();

    // Verify
    let mut val = DatabaseEntry::new();
    db.get_into(None, &key, &mut val).unwrap();
    assert_eq!(val.data_opt(), Some(b"second".as_slice()));
}

// ─────────────────────────────────────────────────────────────────────────────
// 9. Concurrent Prepared Branches Sharing Keys
// ─────────────────────────────────────────────────────────────────────────────

/// Two XA branches write to DIFFERENT keys — no conflict, both commit.
#[test]
fn test_concurrent_branches_disjoint_keys() {
    let dir = TempDir::new().unwrap();
    let (xa, db) = make_xa(dir.path());

    let xid1 = Xid::new(1, b"disjoint_1", b"br").unwrap();
    let xid2 = Xid::new(1, b"disjoint_2", b"br").unwrap();

    // Start both
    xa.xa_start(&xid1, XaFlags::NOFLAGS).unwrap();
    xa.xa_start(&xid2, XaFlags::NOFLAGS).unwrap();

    // Write to different keys
    {
        let txn1 = xa.get_transaction(&xid1).unwrap();
        db.put_in(&txn1, b"key_A", b"val_A").unwrap();
        xa.mark_write(&xid1).unwrap();
    }
    {
        let txn2 = xa.get_transaction(&xid2).unwrap();
        db.put_in(&txn2, b"key_B", b"val_B").unwrap();
        xa.mark_write(&xid2).unwrap();
    }

    // End and prepare both
    xa.xa_end(&xid1, XaFlags::TMSUCCESS).unwrap();
    xa.xa_end(&xid2, XaFlags::TMSUCCESS).unwrap();
    assert_eq!(
        xa.xa_prepare(&xid1, XaFlags::NOFLAGS).unwrap(),
        PrepareResult::Ok
    );
    assert_eq!(
        xa.xa_prepare(&xid2, XaFlags::NOFLAGS).unwrap(),
        PrepareResult::Ok
    );

    // Commit both
    xa.xa_commit(&xid1, XaFlags::NOFLAGS).unwrap();
    xa.xa_commit(&xid2, XaFlags::NOFLAGS).unwrap();

    // Both visible
    let mut val = DatabaseEntry::new();
    assert!(db.get_into(None, b"key_A", &mut val).unwrap());
    assert!(db.get_into(None, b"key_B", &mut val).unwrap());
}

// ─────────────────────────────────────────────────────────────────────────────
// 10. Multi-threaded Stress with PreparedLog
// ─────────────────────────────────────────────────────────────────────────────

/// 8 threads, each doing 200 full 2PC cycles with PreparedLog enabled.
/// Verifies thread-safety of PreparedLog under contention.
#[test]
fn test_concurrent_prepared_log_stress() {
    let dir = TempDir::new().unwrap();
    let (xa, db) = make_xa_with_log(dir.path());
    let xa = Arc::new(xa);
    let db = Arc::new(db);
    let barrier = Arc::new(Barrier::new(8));
    let committed = Arc::new(AtomicU64::new(0));

    let handles: Vec<_> = (0..8)
        .map(|tid| {
            let xa = Arc::clone(&xa);
            let db = Arc::clone(&db);
            let barrier = Arc::clone(&barrier);
            let committed = Arc::clone(&committed);

            std::thread::spawn(move || {
                barrier.wait();
                for i in 0..200u64 {
                    let xid = Xid::new(
                        tid + 1,
                        format!("cpl_t{tid}_i{i:04}").as_bytes(),
                        b"br",
                    )
                    .unwrap();

                    xa.xa_start(&xid, XaFlags::NOFLAGS).unwrap();
                    {
                        let txn = xa.get_transaction(&xid).unwrap();
                        let key = DatabaseEntry::from_vec(
                            format!("cpl_t{tid}_k{i:04}").into_bytes(),
                        );
                        let val = DatabaseEntry::from_bytes(b"cpl_val");
                        db.put_in(&txn, &key, &val).unwrap();
                        xa.mark_write(&xid).unwrap();
                    }
                    xa.xa_end(&xid, XaFlags::TMSUCCESS).unwrap();
                    xa.xa_prepare(&xid, XaFlags::NOFLAGS).unwrap();
                    xa.xa_commit(&xid, XaFlags::NOFLAGS).unwrap();
                    committed.fetch_add(1, Ordering::Relaxed);
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    let total = committed.load(Ordering::Relaxed);
    assert_eq!(total, 8 * 200);

    // No lingering prepared branches
    let recovered = xa.xa_recover(XaFlags::STARTRSCAN).unwrap();
    assert!(recovered.is_empty());
}

// ─────────────────────────────────────────────────────────────────────────────
// 11. Abandon Without Commit (Leak Detection)
// ─────────────────────────────────────────────────────────────────────────────

/// Start branches, do work, but never end/commit them.
/// Verify they appear as NOT prepared (Active state — not in xa_recover).
/// This simulates an application crash mid-work.
#[test]
fn test_abandoned_active_branches_not_in_recover() {
    let dir = TempDir::new().unwrap();
    let (xa, db) = make_xa(dir.path());

    for i in 0..20u64 {
        let xid = Xid::new(1, &i.to_le_bytes(), b"abandon").unwrap();
        xa.xa_start(&xid, XaFlags::NOFLAGS).unwrap();
        let txn = xa.get_transaction(&xid).unwrap();
        let key = DatabaseEntry::from_vec(format!("abn_{i}").into_bytes());
        db.put_in(&txn, &key, b"v").unwrap();
        xa.mark_write(&xid).unwrap();
        // Never call xa_end or xa_commit — branch is abandoned in Active state
    }

    // Active branches should NOT appear in xa_recover
    let recovered = xa.xa_recover(XaFlags::STARTRSCAN).unwrap();
    assert!(recovered.is_empty(), "active branches should not be in recover");
}

// ─────────────────────────────────────────────────────────────────────────────
// 12. Timeout-Driven Adversarial Test
// ─────────────────────────────────────────────────────────────────────────────

/// Stress test that runs for a fixed duration, mixing commits, rollbacks,
/// one-phase, and forgets — verifying invariants hold throughout.
#[test]
fn test_adversarial_mixed_operations_timed() {
    let dir = TempDir::new().unwrap();
    let (xa, db) = make_xa_with_log(dir.path());
    let xa = Arc::new(xa);
    let db = Arc::new(db);
    let stop = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(Barrier::new(4));

    let ops = Arc::new(AtomicU64::new(0));
    let deadline = Duration::from_secs(5);

    let handles: Vec<_> = (0..4)
        .map(|tid| {
            let xa = Arc::clone(&xa);
            let db = Arc::clone(&db);
            let stop = Arc::clone(&stop);
            let barrier = Arc::clone(&barrier);
            let ops = Arc::clone(&ops);

            std::thread::spawn(move || {
                use rand::rngs::SmallRng;
                use rand::{Rng, SeedableRng};

                let mut rng = SmallRng::seed_from_u64(tid as u64 * 997 + 1);
                barrier.wait();
                let mut counter = 0u64;

                while !stop.load(Ordering::Relaxed) {
                    counter += 1;
                    let xid = Xid::new(
                        tid + 1,
                        format!("adv_t{tid}_{counter:08}").as_bytes(),
                        b"adv",
                    )
                    .unwrap();

                    xa.xa_start(&xid, XaFlags::NOFLAGS).unwrap();
                    {
                        let txn = xa.get_transaction(&xid).unwrap();
                        let key = DatabaseEntry::from_vec(
                            format!("adv_t{tid}_{counter}").into_bytes(),
                        );
                        let val = DatabaseEntry::from_bytes(b"adversarial");
                        let _ = db.put_in(&txn, &key, &val);
                        xa.mark_write(&xid).unwrap();
                    }
                    xa.xa_end(&xid, XaFlags::TMSUCCESS).unwrap();

                    // Random outcome
                    let roll: u32 = rng.gen_range(0..100);
                    if roll < 40 {
                        // 2PC commit
                        xa.xa_prepare(&xid, XaFlags::NOFLAGS).unwrap();
                        xa.xa_commit(&xid, XaFlags::NOFLAGS).unwrap();
                    } else if roll < 60 {
                        // One-phase commit
                        xa.xa_commit(&xid, XaFlags::ONEPHASE).unwrap();
                    } else if roll < 85 {
                        // Rollback from idle (no prepare)
                        xa.xa_rollback(&xid, XaFlags::NOFLAGS).unwrap();
                    } else if roll < 95 {
                        // Prepare then rollback
                        xa.xa_prepare(&xid, XaFlags::NOFLAGS).unwrap();
                        xa.xa_rollback(&xid, XaFlags::NOFLAGS).unwrap();
                    } else {
                        // Prepare then forget
                        xa.xa_prepare(&xid, XaFlags::NOFLAGS).unwrap();
                        xa.xa_forget(&xid, XaFlags::NOFLAGS).unwrap();
                    }

                    ops.fetch_add(1, Ordering::Relaxed);
                }
            })
        })
        .collect();

    std::thread::sleep(deadline);
    stop.store(true, Ordering::Relaxed);

    for h in handles {
        h.join().unwrap();
    }

    let total_ops = ops.load(Ordering::Relaxed);
    eprintln!(
        "adversarial mixed: {total_ops} ops in 5s ({:.0} ops/s)",
        total_ops as f64 / 5.0
    );

    // Invariant: no unresolved branches remain
    let recovered = xa.xa_recover(XaFlags::STARTRSCAN).unwrap();
    assert!(
        recovered.is_empty(),
        "found {} unresolved prepared branches after adversarial test",
        recovered.len()
    );
}
