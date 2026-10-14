//! D1 / F-3: a cleaner LN fetch that fails with an env-invalidating read
//! error must invalidate the environment and stop cleaning.  It must not be
//! retried forever as `Locked`, and no data file may be deleted.
//!
//! JE: a checksum failure while cleaning throws
//! `EnvironmentFailureException(LOG_CHECKSUM)` (FileProcessor.java:
//! 1062-1064); an LN fetch failure throws an EFE with the specific reason
//! (IN.java:2826-2843).  Both invalidate the env
//! (EnvironmentFailureReason.java:61-66, 76-77).  `processPending` stops
//! once the env is invalid (Cleaner.java:1250-1252), and `manageDiskUsage`
//! fails before deleting anything (Cleaner.java:758-759).
//!
//! Schedule (deterministic): a foreign WRITE lock on the victim's slot LSN
//! makes pass 1 report the victim as Locked (pending; control: lock denial
//! is retried and the env stays valid).  The lock is released, the
//! victim's LN bytes are corrupted on disk, and pass 2 retries the pending
//! LN, whose fetch now fails.
use super::*;
use crate::{CheckpointConfig, DatabaseConfig};
use noxu_txn::LockType;
use noxu_util::Lsn;

const VICTIM: &[u8] = b"cleaner-victim";
const MARKER: &[u8] = b"CLEANER-VICTIM-VALUE-";
const FOREIGN: i64 = -7_777_777;

fn value(tag: &[u8]) -> Vec<u8> {
    let mut v = tag.to_vec();
    v.resize(64, b'q');
    v
}

fn log_files(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut v: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "ndb"))
        .collect();
    v.sort();
    v
}

fn cfg(dir: &std::path::Path) -> EnvironmentConfig {
    let mut cfg = EnvironmentConfig::new(dir.to_path_buf())
        .with_allow_create(true)
        .with_transactional(true)
        .with_log_file_max_bytes(4096);
    cfg.set_run_cleaner(false);
    cfg.set_run_checkpointer(false);
    cfg.set_run_evictor(false);
    cfg.set_run_in_compressor(false);
    cfg.set_run_verifier(false);
    cfg
}

fn run(corrupt: bool) {
    let temp = tempfile::TempDir::new().unwrap();
    let dbc =
        DatabaseConfig::new().with_allow_create(true).with_transactional(true);
    {
        let env = Environment::open(cfg(temp.path())).unwrap();
        let db = env.open_database(None, "cln", &dbc).unwrap();
        db.put(VICTIM, value(MARKER)).unwrap();
        for round in 0..4u32 {
            for i in 0..40u32 {
                db.put(
                    format!("f{i:03}"),
                    value(format!("r{round}").as_bytes()),
                )
                .unwrap();
            }
        }
        env.checkpoint(Some(&CheckpointConfig::new().with_force(true)))
            .unwrap();
        db.close().unwrap();
        env.close().unwrap();
    }
    let victim_file = log_files(temp.path())
        .into_iter()
        .find(|f| {
            std::fs::read(f).unwrap().windows(MARKER.len()).any(|w| w == MARKER)
        })
        .expect("fixture: victim LN on disk");
    let env = Environment::open(cfg(temp.path())).unwrap();
    let db = env.open_database(None, "cln", &dbc).unwrap();
    let (tree, locks) = {
        let inner = env.env_impl.lock();
        let dbi = inner
            .get_all_database_impls()
            .into_iter()
            .find(|db| db.read().get_name() == "cln")
            .unwrap();
        let dbi = dbi.read();
        (dbi.get_real_tree_arc().unwrap(), Arc::clone(inner.get_lock_manager()))
    };
    let slot = tree.read().unwrap().search_with_data(VICTIM).unwrap();
    assert!(slot.found && slot.data.is_none(), "fixture: victim resident");
    let l0 = Lsn::from_u64(slot.lsn);
    // Pass 1 (control): lock denial -> Locked/pending, env valid, file kept.
    locks.lock(l0.as_u64(), FOREIGN, LockType::Write, false, false).unwrap();
    let p1 = env.clean_log_forced();
    assert!(p1.is_ok(), "corrupt={corrupt}: pass 1 (lock denial) {p1:?}");
    assert!(env.is_valid(), "corrupt={corrupt}: lock denial invalidated env");
    assert!(victim_file.exists(), "corrupt={corrupt}: pass 1 deleted the file");
    assert_eq!(
        tree.read().unwrap().search_with_data(VICTIM).unwrap().lsn,
        l0.as_u64(),
        "corrupt={corrupt}: locked victim was migrated"
    );
    locks.release(l0.as_u64(), FOREIGN).unwrap();
    if !corrupt {
        // Control: the pending LN is retried and migrated; data intact.
        let p2 = env.clean_log_forced();
        assert!(p2.is_ok(), "control pass 2 {p2:?}");
        assert!(env.is_valid());
        assert_ne!(
            tree.read().unwrap().search_with_data(VICTIM).unwrap().lsn,
            l0.as_u64(),
            "control: pending victim never migrated"
        );
        assert_eq!(
            db.get(VICTIM).unwrap().unwrap().as_ref(),
            &value(MARKER)[..]
        );
        db.close().unwrap();
        env.close().unwrap();
        return;
    }
    let mut bytes = std::fs::read(&victim_file).unwrap();
    let off = bytes.windows(MARKER.len()).position(|w| w == MARKER).unwrap();
    bytes[off + 3] ^= 0xFF;
    std::fs::write(&victim_file, bytes).unwrap();
    let files_before = log_files(temp.path());
    let p2 = env.clean_log_forced();
    let p3 = env.clean_log_forced();
    let valid = env.is_valid();
    let files_after = log_files(temp.path());
    let deleted =
        files_before.iter().filter(|f| !files_after.contains(f)).count();
    let bad: Vec<String> = [
        (p2.is_ok())
            .then(|| format!("pass 2 (pending LN, fetch fails) {p2:?}")),
        (p3.is_ok()).then(|| format!("pass 3 after failure {p3:?}")),
        valid.then(|| "env still valid after LOG_CHECKSUM".to_string()),
        (deleted != 0)
            .then(|| format!("{deleted} log files deleted after failure")),
        (!victim_file.exists()).then(|| "victim file deleted".to_string()),
    ]
    .into_iter()
    .flatten()
    .collect();
    assert!(bad.is_empty(), "cleaner fetch failure not fail-closed: {bad:?}");
    drop(db);
    drop(env);
}

#[test]
fn cleaner_pending_ln_fetch_failure_invalidates_and_stops() {
    run(true);
}

#[test]
fn cleaner_lock_denial_is_retried_control() {
    run(false);
}
