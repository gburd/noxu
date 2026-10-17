//! FID-REC-1 (S3) witness: the background checkpointer's write-volume trigger
//! must fire in production.
//!
//! JE `Checkpointer.isRunnable` (Checkpointer.java:639-646) measures growth
//! since the last checkpoint from the LIVE next-LSN:
//!   `getNoCleaningDistance(getFileManager().getNextLsn(), lastCheckpointStart,
//!    logFileMax) >= logSizeBytesInterval`.
//!
//! On 81b2f048, `is_runnable`'s byte branch read `bytes_since_checkpoint`, an
//! accumulator whose only writer (`wakeup_after_write`) is called from tests
//! only.  With a non-zero byte interval the daemon therefore saw `0 >=
//! interval` = false on every wake, and the background checkpointer ran only
//! when the cleaner reported pending files -- never on write volume.  A
//! write-heavy, no-cleaning workload never truncated its recovery log.
//!
//! This test disables the cleaner, sets a small byte interval and a short
//! wakeup, writes well past the interval across many transactions, and asserts
//! the background checkpointer runs (a non-zero `checkpoint.checkpoints` stat).
//! RED on 81b2f048 (never checkpoints on write volume); GREEN after FID-REC-1
//! makes `is_runnable` measure the live-LSN distance like JE.

use noxu_db::{DatabaseConfig, Environment, EnvironmentConfig};
use std::time::{Duration, Instant};
use tempfile::TempDir;

#[test]
fn fid_rec_1_background_checkpoint_fires_on_write_volume() {
    let td = TempDir::new().unwrap();

    // Small byte interval (256 KiB) so a modest write workload crosses it;
    // cleaner OFF so the ONLY possible trigger is write volume (not
    // needs_checkpoint_for_cleaned_files); short wakeup so the daemon
    // evaluates is_runnable promptly.
    let env = Environment::open(
        EnvironmentConfig::new(td.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true)
            .with_run_checkpointer(true)
            .with_run_cleaner(false)
            .with_checkpointer_bytes_interval(256 * 1024)
            .with_checkpointer_wakeup_interval_ms(100),
    )
    .expect("open env");

    let db = env
        .open_database(
            None,
            "rec1",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .expect("open db");

    // Write ~2 MiB of committed data (many transactions, each ~1 KiB value),
    // comfortably past the 256 KiB interval.
    let value = vec![0xABu8; 1024];
    for i in 0u32..2048 {
        let txn = env.begin_transaction(None).expect("begin txn");
        db.put_in(&txn, i.to_be_bytes(), &value).expect("put");
        txn.commit().expect("commit");
    }

    // Wait up to 10s for the background checkpointer to run on write volume.
    // NO explicit env.checkpoint() call -- the whole point is the DAEMON
    // trigger.
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut runs = 0u64;
    while Instant::now() < deadline {
        runs = env.stats().expect("stats").checkpoint.checkpoints;
        if runs > 0 {
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    assert!(
        runs > 0,
        "FID-REC-1: background checkpointer never ran on write volume \
         (checkpoint.checkpoints == 0) -- the byte-interval trigger is dead; \
         wrote ~2 MiB with a 256 KiB interval and the cleaner off"
    );
}
