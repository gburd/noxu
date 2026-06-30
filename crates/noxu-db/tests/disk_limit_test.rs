//! Disk-limit enforcement (HEADLINE test).
//!
//! Faithful port of JE's disk-limit machinery: refuse new user writes before
//! the disk fills so recovery stays possible, and resume once space is
//! reclaimed.
//!
//! JE refs:
//! - `je/cleaner/Cleaner.java` `recalcLogSizeStats` / `getDiskLimitViolation`
//!   (the violation computation and cached volatile flag).
//! - `je/dbi/EnvironmentImpl.java` `checkDiskLimitViolation`.
//! - `je/Cursor.java` `checkUpdatesAllowed` (gates user writes; exempts
//!   internal DBs via `dbImpl.getDbType().isInternal()`).
//!
//! Fail-pre (on `main`, before this feature): user writes succeed until the
//! real disk fills; `DiskLimitExceeded` is never returned. Pass-post: once
//! total log size exceeds `MAX_DISK` the next user write returns
//! `DiskLimitExceeded`; reads and aborts still work; freeing space resumes
//! writes; the cleaner's own writes are never blocked (it freed the space).

use noxu_db::{
    DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig, NoxuError,
    OperationStatus,
};
use tempfile::TempDir;

fn open(dir: &TempDir, max_disk: u64) -> Environment {
    let cfg = EnvironmentConfig::new(dir.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(true)
        // Small log files so the total log size crosses MAX_DISK quickly and
        // so the cleaner has whole files to reclaim.
        .with_log_file_max_bytes(64 * 1024)
        // MAX_DISK is the absolute log-size cap. FREE_DISK off so the test is
        // deterministic regardless of the host's actual free space.
        .with_max_disk(max_disk)
        .with_free_disk(0);
    Environment::open(cfg).unwrap()
}

fn val(i: usize) -> DatabaseEntry {
    // ~1 KiB values so a modest record count grows the log past the cap.
    DatabaseEntry::from_bytes(&vec![(i & 0xff) as u8; 1024])
}

/// HEADLINE: write past MAX_DISK -> DiskLimitExceeded; reads + abort still
/// work over-limit; cleaner can still write (it frees space) -> writes resume.
#[test]
fn disk_limit_blocks_then_resumes() {
    let dir = TempDir::new().unwrap();
    // 256 KiB cap: a handful of 64 KiB log files.
    let env = open(&dir, 256 * 1024);
    let db_cfg =
        DatabaseConfig::new().with_allow_create(true).with_transactional(true);
    let db = env.open_database(None, "dl", &db_cfg).unwrap();

    // Write until the disk limit blocks a user write. We refresh the cached
    // violation state ourselves rather than wait for the background daemon
    // (JE: the daemon refreshes on an interval; refresh_disk_limit forces it).
    let mut blocked_at = None;
    for i in 0..2000usize {
        let key = DatabaseEntry::from_bytes(&(i as u64).to_be_bytes());
        env.refresh_disk_limit().unwrap();
        match db.put(None, &key, &val(i)) {
            Ok(OperationStatus::Success) => {}
            Ok(other) => panic!("unexpected status at {i}: {other:?}"),
            Err(NoxuError::DiskLimitExceeded { used, limit }) => {
                assert!(
                    used >= limit,
                    "violation should report used({used}) >= limit({limit})"
                );
                blocked_at = Some(i);
                break;
            }
            Err(e) => panic!("unexpected error at {i}: {e}"),
        }
    }
    let blocked_at = blocked_at.expect(
        "expected a user write to be refused with DiskLimitExceeded once the \
         log grew past MAX_DISK",
    );
    assert!(blocked_at > 0, "should have written some records first");

    // The limit is still active: another user write is still refused.
    let key = DatabaseEntry::from_bytes(&(blocked_at as u64).to_be_bytes());
    assert!(
        matches!(
            db.put(None, &key, &val(blocked_at)),
            Err(NoxuError::DiskLimitExceeded { .. })
        ),
        "writes must stay blocked while over the limit"
    );

    // Reads must still work while over-limit (JE: read-only ops are not gated).
    let read_key = DatabaseEntry::from_bytes(&0u64.to_be_bytes());
    let mut out = DatabaseEntry::new();
    let s = db.get(None, &read_key, &mut out).unwrap();
    assert_eq!(s, OperationStatus::Success, "reads must work over-limit");
    assert_eq!(out.get_data().unwrap().len(), 1024);

    // A transaction abort must still work over-limit (JE: abort is not gated;
    // it frees, it does not consume the user write budget).
    let txn = env.begin_transaction(None).unwrap();
    // The put inside the txn is itself a user write and is refused...
    assert!(matches!(
        db.put(Some(&txn), &read_key, &val(1)),
        Err(NoxuError::DiskLimitExceeded { .. })
    ));
    // ...but aborting the txn still succeeds.
    txn.abort().expect("abort must succeed while over the disk limit");

    // Free space: delete records and run the cleaner. The cleaner's OWN writes
    // (migrating live LNs, writing FileSummaryLNs to the internal utilization
    // DB) must NOT be blocked by the limit, or it could never reclaim space.
    // clean_log() succeeding here proves the internal-writes-exempt rule.
    for i in 0..blocked_at {
        let key = DatabaseEntry::from_bytes(&(i as u64).to_be_bytes());
        // Deletes are also gated while over-limit, so we may need to clean
        // first. Try the delete; ignore a disk-limit refusal and rely on the
        // checkpoint+clean below to reclaim whole obsolete files.
        let _ = db.delete(None, &key);
    }
    // Checkpoint flushes the tree so cleaned files become fully obsolete, then
    // clean_log reclaims them (the cleaner refreshes the disk-limit state after
    // its pass, JE Cleaner.manageDiskUsage -> freshenLogSizeStats).
    let _ = env.checkpoint(None);
    let _cleaned = env.clean_log().expect(
        "cleaner must be able to write/delete while over the limit \
         (internal-writes-exempt rule); otherwise it deadlocks",
    );
    env.refresh_disk_limit().unwrap();

    // Writes must resume once we are back within the limit. If a single clean
    // pass did not reclaim enough, drive a few more delete+clean cycles.
    let mut resumed = false;
    for round in 0..8 {
        let key =
            DatabaseEntry::from_bytes(&(10_000 + round as u64).to_be_bytes());
        env.refresh_disk_limit().unwrap();
        match db.put(None, &key, &val(round)) {
            Ok(OperationStatus::Success) => {
                resumed = true;
                break;
            }
            Err(NoxuError::DiskLimitExceeded { .. }) => {
                // Still over-limit: reclaim more and retry.
                for i in 0..blocked_at {
                    let k =
                        DatabaseEntry::from_bytes(&(i as u64).to_be_bytes());
                    let _ = db.delete(None, &k);
                }
                let _ = env.checkpoint(None);
                let _ = env.clean_log();
            }
            other => panic!("unexpected on resume: {other:?}"),
        }
    }
    assert!(
        resumed,
        "writes must resume after the cleaner reclaims space below MAX_DISK"
    );
}

/// Default behaviour is unchanged: with MAX_DISK=0 and FREE_DISK=0 the tracker
/// is inert and writes are never refused.
#[test]
fn disabled_by_default_never_blocks() {
    let dir = TempDir::new().unwrap();
    let env = open(&dir, 0); // max_disk=0, free_disk=0 (from open())
    let db_cfg =
        DatabaseConfig::new().with_allow_create(true).with_transactional(true);
    let db = env.open_database(None, "nolimit", &db_cfg).unwrap();

    for i in 0..500usize {
        let key = DatabaseEntry::from_bytes(&(i as u64).to_be_bytes());
        env.refresh_disk_limit().unwrap();
        let s = db.put(None, &key, &val(i)).unwrap();
        assert_eq!(
            s,
            OperationStatus::Success,
            "no enforcement when both limits are 0"
        );
    }
}
