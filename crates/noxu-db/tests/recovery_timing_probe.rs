//! Recovery startup timing probe (ignored by default — run explicitly).
//!
//! Populates ~100k records with automatic checkpointing effectively disabled
//! (huge checkpointer byte interval) so that the entire log tail must be
//! replayed at the next open. Then times `Environment::open()` (which runs
//! recovery: find-last-checkpoint → analysis scan → redo → undo).
//!
//! Run:
//!   cargo test -p noxu-db --test recovery_timing_probe -- --ignored --nocapture
//!
//! This is the BEFORE/AFTER measurement for the streaming analysis change
//! (route `run_analysis` through `scan_forward_fn` instead of materialising
//! the bounded range into a `Vec<PositionedEntry>`).
//!
//! ponytail: a probe, not a regression gate — #[ignore]'d so CI never runs it.

use noxu_db::{DatabaseConfig, DatabaseEntry, EnvironmentConfig};
use std::path::Path;
use std::time::Instant;
use tempfile::TempDir;

const N: u32 = 100_000;

fn open_env(dir: &Path) -> noxu_db::Environment {
    noxu_db::Environment::open(
        EnvironmentConfig::new(dir.to_path_buf())
            .with_allow_create(true)
            .with_transactional(true)
            // Suppress automatic checkpoints so the whole log tail is the
            // recovery analysis/redo range (the case the benchmark hits).
            .with_checkpointer_bytes_interval(1 << 40),
    )
    .unwrap()
}

fn open_db(env: &noxu_db::Environment) -> noxu_db::Database {
    env.open_database(
        None,
        "probe",
        &DatabaseConfig::new().with_allow_create(true).with_transactional(true),
    )
    .unwrap()
}

#[test]
#[ignore = "timing probe; run explicitly with --ignored --nocapture"]
fn recovery_open_timing_100k() {
    let dir = TempDir::new().unwrap();

    // Phase 1: populate. Many small committed txns so there is a large LN
    // redo range after the (suppressed) checkpoint.
    {
        let env = open_env(dir.path());
        let db = open_db(&env);
        let t0 = Instant::now();
        // Batch into transactions of 1000 to generate plenty of TxnCommit
        // records alongside the LNs.
        let mut i = 0u32;
        while i < N {
            let txn = env.begin_transaction(None).unwrap();
            let end = (i + 1000).min(N);
            for k in i..end {
                let key = format!("key_{k:08}");
                let val = format!("val_{k:08}");
                db.put(
                    Some(&txn),
                    &DatabaseEntry::from_bytes(key.as_bytes()),
                    &DatabaseEntry::from_bytes(val.as_bytes()),
                )
                .unwrap();
            }
            txn.commit().unwrap();
            i = end;
        }
        eprintln!("populate {N} records: {:?}", t0.elapsed());
        // Simulate a crash (no clean close): each txn commit was durable
        // (default sync), so the WAL is on disk, but we must NOT run the
        // close-time forced checkpoint (EnvironmentImpl::close ->
        // do_checkpoint("close")) or the recovery analysis range collapses to
        // near-empty and the streaming path is never exercised. `mem::forget`
        // skips Drop -> close -> checkpoint, leaving the full post-checkpoint
        // tail for recovery to replay (the scenario the benchmark hits).
        //
        // ponytail: forget leaks the handles for the rest of the test process;
        // fine for a one-shot ignored probe. The forgotten env still holds the
        // env file lock, so recovery runs against a COPY of the log files in a
        // second dir (a crash leaves the lock unheld on disk anyway).
        std::mem::forget(db);
        std::mem::forget(env);
    }

    // Copy the populated log files into a fresh dir so the forgotten env's
    // still-held file lock does not block the recovery open.
    let rec_dir = TempDir::new().unwrap();
    for entry in std::fs::read_dir(dir.path()).unwrap() {
        let p = entry.unwrap().path();
        if p.extension().is_some_and(|x| x == "ndb") {
            std::fs::copy(&p, rec_dir.path().join(p.file_name().unwrap()))
                .unwrap();
        }
    }

    // Phase 2: time recovery (open).
    let t1 = Instant::now();
    let env = open_env(rec_dir.path());
    let elapsed = t1.elapsed();
    eprintln!("RECOVERY open() of {N} records: {elapsed:?}");

    // Sanity: data is present.
    let db = open_db(&env);
    let mut v = DatabaseEntry::new();
    let st = db
        .get(None, &DatabaseEntry::from_bytes(b"key_00050000"), &mut v)
        .unwrap();
    assert_eq!(
        st,
        noxu_db::OperationStatus::Success,
        "expected key_00050000 to survive recovery"
    );
    drop(db);
    drop(env);
}
