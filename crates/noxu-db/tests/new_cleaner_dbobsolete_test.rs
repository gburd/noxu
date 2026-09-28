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
        .open_database(
            None,
            name,
            &DatabaseConfig::new().with_allow_create(true),
        )
        .unwrap();
    let value = vec![0x5Au8; 256];
    for i in 0..n {
        db.put(i.to_be_bytes(), &value).unwrap();
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

/// ABORT: a transactional remove that is rolled back must NOT count the DB's
/// footprint obsolete (the DB survives, its space stays live).  Non-vacuous:
/// a naive fix that counted obsolete at operation time (not commit) would
/// corrupt the tracker here.
#[test]
fn aborted_remove_does_not_count_obsolete_or_reclaim() {
    let tmp = TempDir::new().unwrap();
    let env = open_env(tmp.path().to_path_buf());

    fill(&env, "survivor", 200);
    env.checkpoint(None).unwrap();

    let txn = env.begin_transaction(None).unwrap();
    env.remove_database(Some(&txn), "survivor").unwrap();
    txn.abort().unwrap();

    // DB must still exist and be fully readable.
    assert!(
        env.database_names().unwrap().contains(&"survivor".to_string()),
        "aborted remove: database must survive"
    );
    let db =
        env.open_database(None, "survivor", &DatabaseConfig::new()).unwrap();
    for i in 0..200u32 {
        assert!(
            db.get(i.to_be_bytes()).unwrap().is_some(),
            "aborted remove: record {i} must survive"
        );
    }
    db.close().unwrap();
    env.checkpoint(None).unwrap();

    // No file should be reclaimable: the DB is 100% live, nothing obsolete.
    let cleaned = env.clean_log().unwrap();
    assert_eq!(
        cleaned, 0,
        "aborted remove: force=false cleaner reclaimed {cleaned} files, but \
         the rolled-back DB is fully live — its space must NOT be counted \
         obsolete"
    );
    // Tracker invariant: no over-count.
    for (f, s) in env.cleaner_diagnostics().unwrap().file_summaries {
        assert!(
            s.obsolete_ln_count <= s.total_ln_count
                && s.obsolete_in_count <= s.total_in_count,
            "file {f}: obsolete exceeds total (LN {}/{}, IN {}/{}) — the \
             aborted remove wrongly counted obsolete",
            s.obsolete_ln_count,
            s.total_ln_count,
            s.obsolete_in_count,
            s.total_in_count
        );
    }

    let vr = env.verify(&noxu_db::VerifyConfig::new()).unwrap();
    assert_eq!(vr.error_count(), 0, "verify after aborted remove");

    env.close().unwrap();
}

/// COMMIT + REOPEN: after a committed remove and a checkpoint, utilization
/// must survive recovery (the reclaim is durable), and env.verify() == 0.
#[test]
fn committed_remove_survives_reopen_and_reclaims() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().to_path_buf();

    {
        let env = open_env(path.clone());
        fill(&env, "keep", 50);
        fill(&env, "gone", 200);
        env.checkpoint(None).unwrap();

        let txn = env.begin_transaction(None).unwrap();
        env.remove_database(Some(&txn), "gone").unwrap();
        txn.commit().unwrap();
        // Checkpoint so the bumped obsolete counts land in FileSummaryLN.
        env.checkpoint(None).unwrap();
        env.close().unwrap();
    }

    // Reopen: recovery must restore the utilization (from the persisted
    // FileSummaryLN) so the force=false cleaner still reclaims.
    let env = open_env(path);
    assert!(
        !env.database_names().unwrap().contains(&"gone".to_string()),
        "committed remove: database must stay gone after reopen"
    );
    let cleaned = env.clean_log().unwrap();
    assert!(
        cleaned > 0,
        "committed remove + reopen: force=false cleaner reclaimed {cleaned} \
         files — the obsolete accounting did not survive recovery"
    );
    let vr = env.verify(&noxu_db::VerifyConfig::new()).unwrap();
    assert_eq!(vr.error_count(), 0, "verify after committed-remove reopen");
    env.close().unwrap();
}

/// NO DOUBLE-COUNT: a transactional truncate drains eagerly (NEW-TRUNCATE-1),
/// counting each deleted LN obsolete at commit.  It must NOT also whole-DB
/// count them.  Guard: obsolete counts never exceed totals, verify() == 0, and
/// the force=false cleaner still reclaims (the eager drain already made the
/// space obsolete — no need for clean_log_forced).
#[test]
fn txn_truncate_reclaims_without_double_counting() {
    let tmp = TempDir::new().unwrap();
    let env = open_env(tmp.path().to_path_buf());

    fill(&env, "keep", 50);
    fill(&env, "trunc", 200);
    env.checkpoint(None).unwrap();

    let txn = env.begin_transaction(None).unwrap();
    let n = env.truncate_database(Some(&txn), "trunc").unwrap();
    assert_eq!(n, 200, "txn truncate count");
    txn.commit().unwrap();
    env.checkpoint(None).unwrap();

    // No over-count: obsolete must never exceed total for any file.  A
    // double-count (eager drain + whole-DB) would push obsolete past total.
    for (f, s) in env.cleaner_diagnostics().unwrap().file_summaries {
        assert!(
            s.obsolete_ln_count <= s.total_ln_count
                && s.obsolete_ln_size <= s.total_ln_size
                && s.obsolete_in_count <= s.total_in_count,
            "file {f}: DOUBLE-COUNT — obsolete exceeds total (LN {}/{}, \
             size {}/{}, IN {}/{})",
            s.obsolete_ln_count,
            s.total_ln_count,
            s.obsolete_ln_size,
            s.total_ln_size,
            s.obsolete_in_count,
            s.total_in_count
        );
    }

    let cleaned = env.clean_log().unwrap();
    assert!(
        cleaned > 0,
        "txn truncate: force=false cleaner reclaimed {cleaned} files — the \
         eager drain's obsolete accounting must let the ordinary cleaner \
         reclaim without clean_log_forced"
    );

    let vr = env.verify(&noxu_db::VerifyConfig::new()).unwrap();
    assert_eq!(vr.error_count(), 0, "verify after txn truncate");
    env.close().unwrap();
}
