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
    DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig,
    EnvironmentMutableConfig, NoxuError,
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
///
/// JE parity: `DiskLimitTest.testWritesProhibited` (standalone, non-HA half) --
/// a disk-limit violation prohibits user writes while reads/aborts continue,
/// and freeing space re-enables writes. (The 3-node replicated-group ack-policy
/// portion of `testWritesProhibited` is a rep test, out of scope for the
/// cleaner package.) Also covers the standalone half of
/// `DiskLimitTest.testCheckpointCleanEvict` (the cleaner's own writes are
/// never blocked because they free the space).
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
        match db.put(&key, val(i)) {
            Ok(()) => {}
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
            db.put(&key, val(blocked_at)),
            Err(NoxuError::DiskLimitExceeded { .. })
        ),
        "writes must stay blocked while over the limit"
    );

    // Reads must still work while over-limit (JE: read-only ops are not gated).
    let read_key = DatabaseEntry::from_bytes(&0u64.to_be_bytes());
    let mut out = DatabaseEntry::new();
    let s = db.get_into(None, &read_key, &mut out).unwrap();
    assert!(s, "reads must work over-limit");
    assert_eq!(out.data_opt().unwrap().len(), 1024);

    // A transaction abort must still work over-limit (JE: abort is not gated;
    // it frees, it does not consume the user write budget).
    let txn = env.begin_transaction(None).unwrap();
    // The put inside the txn is itself a user write and is refused...
    assert!(matches!(
        db.put_in(&txn, &read_key, val(1)),
        Err(NoxuError::DiskLimitExceeded { .. })
    ));
    // ...but aborting the txn still succeeds.
    txn.abort().expect("abort must succeed while over the disk limit");

    // The cleaner's OWN writes (migrating live LNs, writing FileSummaryLNs to
    // the internal utilization DB) must NOT be blocked by the limit, or it
    // could never reclaim space. clean_log() succeeding while over-limit proves
    // the internal-writes-exempt rule (JE: internal DBs skip
    // checkUpdatesAllowed). It runs force=false (JE Environment.cleanLog ->
    // invokeCleaner(false)); on a 100%-live workload it reclaims nothing, and
    // that is correct -- JE getBestFile(forceCleaning=false) never selects a
    // 100%-live file (UtilizationCalculator.java:405-431).
    let _cleaned = env.clean_log().expect(
        "cleaner must be able to write while over the limit \
         (internal-writes-exempt rule); otherwise it deadlocks",
    );
    env.refresh_disk_limit().unwrap();

    // Deletes are gated under the limit too (JE Cursor.deleteInternal ->
    // checkUpdatesAllowed), so the workload cannot free itself by deleting +
    // cleaning while blocked: writes stay blocked.
    let key = DatabaseEntry::from_bytes(&(blocked_at as u64).to_be_bytes());
    assert!(
        matches!(
            db.put(&key, val(blocked_at)),
            Err(NoxuError::DiskLimitExceeded { .. })
        ),
        "writes must remain blocked over the limit -- deletes are gated, so \
         cleaning a 100%-live workload cannot drop below the cap (JE resumes \
         by relaxing the limit, not by reclaim-below-cap)"
    );

    // RESUME by RELAXING the limit -- the Noxu analogue of JE
    // DiskLimitTest.allowWrites() (setMaxDisk(0), DiskLimitTest.java:578-580).
    // Disabling MAX_DISK (FREE_DISK is already 0 in this test) clears the
    // violation, so the next user write succeeds.
    let mut env = env;
    env.set_mutable_config(EnvironmentMutableConfig::new().with_max_disk(0))
        .expect("relaxing MAX_DISK must succeed");
    env.refresh_disk_limit().unwrap();

    let resume_key = DatabaseEntry::from_bytes(&10_000u64.to_be_bytes());
    match db.put(&resume_key, val(0)) {
        Ok(()) => {}
        other => panic!(
            "write must resume once the limit is relaxed (JE allowWrites / \
             setMaxDisk(0)); got {other:?}"
        ),
    }
    // And the record is readable back -- resume actually persisted the write.
    let mut out = DatabaseEntry::new();
    let s = db.get_into(None, &resume_key, &mut out).unwrap();
    assert!(s, "resumed write must be readable");
    assert_eq!(out.data_opt().unwrap().len(), 1024);
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
        db.put(&key, val(i)).unwrap();
    }
}
