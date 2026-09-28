//! NEW-CLEANER-DBOBSOLETE: remove_database / truncate_database must count the
//! removed database's log footprint (LNs + INs) obsolete so the ORDINARY
//! (force=false) cleaner reclaims the space.
//!
//! Before the fix, only `clean_log_forced()` (force=true) reclaimed a
//! removed/truncated DB's files: `remove_database` and the auto-commit
//! `truncate_database` never called the `UtilizationTracker.countObsoleteDb`
//! equivalent, so the tracker still saw those files at ~100% utilization and
//! the force=false daemon never selected them.  JE
//! `DatabaseImpl.finishDeleteProcessing` -> `LogManager.countObsoleteDb`
//! (`DatabaseImpl.java:1624`) counts the whole DB obsolete at remove/truncate
//! commit; this port wires `remove_database` / auto-commit `truncate_database`
//! to `UtilizationTracker::count_obsolete_db`.
//!
//! The transactional truncate path is DELIBERATELY NOT whole-DB-counted: it
//! drains eagerly per-record under the txn (NEW-TRUNCATE-1), so every deleted
//! LN is already counted obsolete at commit via the cursor delete path.
//! Whole-DB counting on top would double-count.  See
//! `txn_truncate_reclaims_without_double_counting`.

use noxu_db::{DatabaseConfig, Environment, EnvironmentConfig};
use tempfile::TempDir;

/// Small files so a modest fill spans several of them; low min_utilization so
/// only genuinely-obsolete files are selected by the force=false cleaner.
fn open_env(path: std::path::PathBuf) -> Environment {
    let mut cfg = EnvironmentConfig::new(path)
        .with_allow_create(true)
        .with_transactional(true)
        .with_log_file_max_bytes(4 * 1024)
        .with_cache_size(16 * 1024 * 1024)
        .with_cleaner_min_utilization(50);
    cfg.set_run_cleaner(false);
    cfg.set_run_checkpointer(false);
    Environment::open(cfg).unwrap()
}

fn fill(env: &Environment, name: &str, n: u32) {
    let db = env
        .open_database(None, name, &DatabaseConfig::new().with_allow_create(true))
        .unwrap();
    let value = vec![0x5Au8; 256];
    for i in 0..n {
        db.put(&i.to_be_bytes(), &value).unwrap();
    }
    db.close().unwrap();
}

/// Auto-commit remove_database: after remove + a checkpoint, the ordinary
/// (force=false) cleaner must reclaim the removed DB's files.
#[test]
fn autocommit_remove_reclaims_under_force_false_cleaner() {
    let tmp = TempDir::new().unwrap();
    let env = open_env(tmp.path().to_path_buf());

    // Two databases: one we remove, one that stays 100% live.  The live DB
    // guarantees the reclaim signal is about the REMOVED db, not global churn.
    fill(&env, "keep", 50);
    fill(&env, "gone", 200);
    env.checkpoint(None).unwrap();

    // Remove the DB (auto-commit).  Then checkpoint so the tracker's bumped
    // obsolete counts land in FileSummaryLN.
    env.remove_database(None, "gone").unwrap();
    env.checkpoint(None).unwrap();

    // ORDINARY cleaner, force=false.  Must reclaim the removed DB's files.
    let cleaned = env.clean_log().unwrap();
    assert!(
        cleaned > 0,
        "force=false cleaner reclaimed {cleaned} files after remove_database; \
         the removed DB's footprint was never counted obsolete \
         (NEW-CLEANER-DBOBSOLETE gap: countObsoleteDb not wired)"
    );

    env.close().unwrap();
}

/// Auto-commit truncate_database: same contract as remove.
#[test]
fn autocommit_truncate_reclaims_under_force_false_cleaner() {
    let tmp = TempDir::new().unwrap();
    let env = open_env(tmp.path().to_path_buf());

    fill(&env, "keep", 50);
    fill(&env, "trunc", 200);
    env.checkpoint(None).unwrap();

    let n = env.truncate_database(None, "trunc").unwrap();
    assert_eq!(n, 200, "truncate count");
    env.checkpoint(None).unwrap();

    let cleaned = env.clean_log().unwrap();
    assert!(
        cleaned > 0,
        "force=false cleaner reclaimed {cleaned} files after auto-commit \
         truncate_database; the truncated DB's footprint was never counted \
         obsolete (NEW-CLEANER-DBOBSOLETE gap)"
    );

    env.close().unwrap();
}
