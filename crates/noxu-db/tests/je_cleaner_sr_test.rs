//! C5 — cleaner SR-numbered regression tests ported from JE.
//!
//! Each test guards a specific historical JE cleaner data-loss/corruption bug.
//! The SR numbers are JE's internal bug identifiers. These are end-to-end
//! environment tests (the scenarios exercise the cleaner + tree + fetch path),
//! so they live alongside the other end-to-end suites in `noxu-db/tests/`
//! rather than the unit-level `noxu-cleaner/tests/`.
//!
//! JE source: `com/sleepycat/je/cleaner/SR10553Test`, `SR12885Test`.
//!
//! SR13061 (`FileSummaryLN.hasStringKey`) is intentionally NOT ported: it
//! guards a JE backward-compat bug where an old-style STRING file-summary key
//! survived a log-version bump and was misread as an 8-byte integer key. Noxu
//! has a single binary `.ndb` log format (LOG_VERSION 3) with no legacy
//! string-key path (`FileSummaryLnEntry` uses a fixed binary layout and there
//! is no `has_string_key` heuristic), so the bug class cannot exist here. It
//! is a JE-format-migration artefact, not a Noxu fidelity gap.

use noxu_db::{
    DatabaseConfig, DatabaseEntry, EnvironmentConfig, OperationStatus,
};
use std::path::Path;
use tempfile::TempDir;

/// JE `SR10553Test.openEnv` / `SR12885Test.openEnv`: daemons off, small log
/// files so cleaning is frequent.
fn open_env(dir: &Path) -> noxu_db::Environment {
    let mut cfg = EnvironmentConfig::new(dir.to_path_buf())
        .with_allow_create(true)
        .with_transactional(true)
        // Small log file size makes cleaning more frequent (JE LOG_FILE_MAX).
        .with_log_file_max_bytes(1024 * 8);
    cfg.set_run_cleaner(false);
    cfg.set_run_evictor(false);
    cfg.set_run_checkpointer(false);
    cfg.set_run_in_compressor(false);
    noxu_db::Environment::open(cfg).unwrap()
}

/// JE `TestUtils.getTestArray(i)`: a deterministic 4-byte big-endian key/value.
fn test_array(i: u32) -> Vec<u8> {
    i.to_be_bytes().to_vec()
}

// ---------------------------------------------------------------------------
// SR10553 — cleaner must set knownDeleted for deleted records
// ---------------------------------------------------------------------------

/// JE `SR10553Test.testSR10553`.
///
/// Put a key with many duplicate values (enough to fill a log file), delete
/// the key (do not compress), checkpoint, clean the log, evict, then scan all
/// values.
///
/// Before the SR10553 fix, scanning over deleted records would throw a
/// `LogFileNotFoundException` when faulting in a deleted record whose log file
/// had been cleaned — because the cleaner was not setting `knownDeleted` for
/// deleted records. The faithful invariant: after delete + clean + evict, a
/// full scan must complete WITHOUT error (and find no surviving values).
#[test]
fn sr10553_clean_then_scan_deleted_does_not_fail() {
    let dir = TempDir::new().unwrap();
    let env = open_env(dir.path());
    let db = env
        .open_database(
            None,
            "foo",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true)
                .with_sorted_duplicates(true),
        )
        .unwrap();

    // Put some duplicates, enough to fill a log file.
    const COUNT: u32 = 10;
    let key = test_array(0);
    for i in 0..COUNT {
        db.put(
            &DatabaseEntry::from_bytes(&key),
            &DatabaseEntry::from_bytes(&test_array(i)))
        .unwrap();
    }

    // Confirm the duplicate count.
    {
        let mut cursor = db.open_cursor( None).unwrap();
        let mut k = DatabaseEntry::from_bytes(&key);
        let mut d = DatabaseEntry::new();
        let s = cursor.get(&mut k, &mut d, noxu_db::Get::Search, None).unwrap();
        assert_eq!(s, OperationStatus::Success);
        assert_eq!(cursor.count().unwrap(), COUNT as u64);
        cursor.close().unwrap();
    }

    // Delete everything. Do not compress.
    assert!(db.delete( &DatabaseEntry::from_bytes(&key)).unwrap());

    // Checkpoint and clean.
    env.checkpoint(Some(&noxu_db::CheckpointConfig::new().with_force(true)))
        .unwrap();
    let cleaned = env.clean_log().unwrap();
    assert!(cleaned > 0, "expected the log to be cleaned, cleaned={cleaned}");

    // Force eviction.
    let _ = env.evict_memory().unwrap();

    // Scan all values — must complete without a LogFileNotFound-style error.
    // (Before the SR10553 fix this threw when faulting a deleted record whose
    // file had been cleaned.)
    {
        let mut cursor = db.open_cursor( None).unwrap();
        let mut k = DatabaseEntry::new();
        let mut d = DatabaseEntry::new();
        let mut status =
            cursor.get(&mut k, &mut d, noxu_db::Get::First, None).unwrap();
        let mut seen = 0;
        while status == OperationStatus::Success {
            seen += 1;
            assert!(seen < 1000, "runaway scan");
            status =
                cursor.get(&mut k, &mut d, noxu_db::Get::Next, None).unwrap();
        }
        cursor.close().unwrap();
        // Everything was deleted; the scan must find nothing AND not error.
        assert_eq!(seen, 0, "all values were deleted; scan must be empty");
    }

    db.close().unwrap();
    env.close().unwrap();
}

// ---------------------------------------------------------------------------
// SR12885 — pending-LN migration vs. slot-reuse + abort must not lose data
// ---------------------------------------------------------------------------

/// JE `SR12885Test.testSR12885`.
///
/// The original SR12885 bug involved the cleaner putting the wrong node ID on
/// the pending-LN list when a slot was reused by an active transaction that
/// later aborted, eventually causing a `LogFileNotFoundException` (or a
/// spurious NOTFOUND) on a fetch of the surviving key.
///
/// JE's own comment notes the *specific* node-ID bug is no longer applicable
/// in engines that lock the LSN rather than the node ID — which is exactly
/// Noxu's model (lock-based, per-record LSN locking; see AGENTS.md "Isolation
/// model: Lock-based, NOT MVCC"). Noxu LNs have no node IDs. We therefore port
/// the *still-applicable invariant*: a key that survives cleaner LN-migration
/// interleaved with txn slot-reuse + abort must still fetch SUCCESS (data must
/// not be lost to a cleaned file). We drive the same operation sequence JE
/// uses to provoke the pending-LN/migration path.
#[test]
fn sr12885_pending_ln_migration_with_slot_reuse_abort_keeps_data() {
    let dir = TempDir::new().unwrap();
    let env = open_env(dir.path());
    let db = env
        .open_database(
            None,
            "foo",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap();

    const COUNT: u32 = 10;
    let data = test_array(0);

    // Add some records, enough to fill a log file (no-overwrite inserts).
    for i in 0..COUNT {
        let s = db
            .put_no_overwrite(
                &DatabaseEntry::from_bytes(&test_array(i)),
                &DatabaseEntry::from_bytes(&data))
            .unwrap();
        assert!(s);
    }

    // Delete all but key 0, so the first file can be cleaned but key 0 will
    // need to be migrated.
    for i in 1..COUNT {
        let s = db
            .delete( &DatabaseEntry::from_bytes(&test_array(i)))
            .unwrap();
        assert!(s);
    }

    // Checkpoint and clean to set the migrate flag for key 0 (done while key 0
    // is unlocked, so it is not yet put on the pending list).
    env.checkpoint(Some(&noxu_db::CheckpointConfig::new().with_force(true)))
        .unwrap();
    let cleaned = env.clean_log().unwrap();
    assert!(cleaned > 0, "expected the log to be cleaned, cleaned={cleaned}");

    let key0 = test_array(0);

    // Using a transaction, delete then re-insert key 0, reusing the slot
    // (a new LSN). Do not abort until after the cleaner migration step.
    let txn = env.begin_transaction(None).unwrap();
    assert!(db.delete_in(&txn, &DatabaseEntry::from_bytes(&key0)).unwrap());
    assert!(db.put_no_overwrite_in(&txn,
            &DatabaseEntry::from_bytes(&key0),
            &DatabaseEntry::from_bytes(&data))
        .unwrap());

    // Checkpoint again to perform LN migration: key 0 is locked, so it goes on
    // the pending list with the newly-inserted (reused-slot) version.
    env.checkpoint(Some(&noxu_db::CheckpointConfig::new().with_force(true)))
        .unwrap();

    // Abort to revert to the original key-0 LSN, then delete with a new txn so
    // the current LN for key 0 is deleted.
    txn.abort().unwrap();
    let txn2 = env.begin_transaction(None).unwrap();
    assert!(db.delete_in(&txn2, &DatabaseEntry::from_bytes(&key0)).unwrap());

    // Checkpoint to process pending LNs and delete the cleaned file, then
    // abort the delete so the BIN reverts to the node we needed to migrate.
    env.checkpoint(Some(&noxu_db::CheckpointConfig::new().with_force(true)))
        .unwrap();
    txn2.abort().unwrap();

    // A fetch of key 0 must succeed (before the SR12885 fix this raised a
    // LogFileNotFoundException / spurious NOTFOUND because the surviving LN's
    // file had been deleted without migration).
    let mut val = DatabaseEntry::new();
    let status =
        db.get_into(None, &DatabaseEntry::from_bytes(&key0), &mut val).unwrap();
    assert!(status,
        "SR12885: surviving key 0 must fetch SUCCESS after cleaner migration \
         + slot-reuse + abort (data must not be lost to a cleaned file)"
    );
    assert_eq!(val.get_data(), Some(data.as_slice()));

    db.close().unwrap();
    env.close().unwrap();
}
