//! R-FIRSTLSN in-process clamp probe for DDL-in-open-txn (create/remove/rename).
//!
//! Isolates the first-active-txn-file clamp effect from recovery/relog
//! semantics: it asserts the LOG FILE holding an OPEN txn's transactional
//! NameLN entry is NOT deleted by a forced cleaning pass while the txn is open.
//! On rc2 (buggy) the clamp under-reports (NULL) and the file is deleted; with
//! the fix the clamp == that file and it is protected.

use noxu_db::{
    CheckpointConfig, DatabaseConfig, DatabaseEntry, Environment,
    EnvironmentConfig,
};
use std::path::Path;

fn ndb_files(dir: &Path) -> Vec<u32> {
    let mut v: Vec<u32> = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| {
            let p = e.ok()?.path();
            let n = p.file_name()?.to_str()?.to_string();
            n.strip_suffix(".ndb").and_then(|s| u32::from_str_radix(s, 16).ok())
        })
        .collect();
    v.sort_unstable();
    v
}

fn env_cfg(dir: &Path) -> EnvironmentConfig {
    let mut cfg = EnvironmentConfig::new(dir.to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    cfg.set_run_cleaner(false);
    cfg.set_run_checkpointer(false);
    cfg.set_log_file_max_bytes(1_000_000);
    cfg.set_cleaner_min_age(1);
    cfg.set_cleaner_min_utilization(90);
    cfg
}

fn dbcfg() -> DatabaseConfig {
    DatabaseConfig::new().with_allow_create(true).with_transactional(true)
}

fn roll(db: &noxu_db::Database, env: &Environment, gens: u32) {
    let value = vec![0x5Au8; 4096];
    for g in 0..gens {
        let txn = env.begin_transaction(None).unwrap();
        for k in 0..40u32 {
            db.put_in(
                &txn,
                DatabaseEntry::from_bytes(&(g * 40 + k).to_be_bytes()),
                DatabaseEntry::from_bytes(&value),
            )
            .unwrap();
        }
        txn.commit().unwrap();
    }
}

fn barrier(env: &Environment) {
    let force = CheckpointConfig::new().with_force(true);
    for _ in 0..6 {
        env.clean_log_forced().unwrap();
        env.checkpoint(Some(&force)).unwrap();
        env.clean_log_forced().unwrap();
        env.checkpoint(Some(&force)).unwrap();
    }
}

/// create-in-txn: the file holding the open txn's provisional NameLNTxn must
/// survive a forced clean while the txn is open.
#[test]
fn create_in_txn_nameln_file_protected() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let env = Environment::open(env_cfg(&path)).unwrap();
    let filler = env.open_database(None, "filler", &dbcfg()).unwrap();
    roll(&filler, &env, 40);

    let txn = env.begin_transaction(None).unwrap();
    let victim = env.open_database(Some(&txn), "victim", &dbcfg()).unwrap();
    let nameln_file = *ndb_files(&path).last().unwrap();

    roll(&filler, &env, 60);
    barrier(&env);

    let survived = ndb_files(&path).contains(&nameln_file);
    victim.close().unwrap();
    txn.commit().unwrap();
    filler.close().unwrap();
    env.close().unwrap();

    assert!(
        survived,
        "the file (#{nameln_file}) holding the OPEN create-in-txn NameLNTxn \
         was deleted by a forced clean while the txn was open — the \
         first-active-txn-file clamp under-reported (R-FIRSTLSN)"
    );
}

/// remove-in-txn: the file holding the open txn's NameLN-delete tombstone must
/// survive a forced clean while the txn is open.
#[test]
fn remove_in_txn_tombstone_file_protected() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let env = Environment::open(env_cfg(&path)).unwrap();
    let filler = env.open_database(None, "filler", &dbcfg()).unwrap();

    // Create + commit victim.
    {
        let vt = env.begin_transaction(None).unwrap();
        let v = env.open_database(Some(&vt), "victim", &dbcfg()).unwrap();
        v.put_in(
            &vt,
            DatabaseEntry::from_bytes(b"k"),
            DatabaseEntry::from_bytes(b"v"),
        )
        .unwrap();
        v.close().unwrap();
        vt.commit().unwrap();
    }
    roll(&filler, &env, 40);

    // Open txn T: remove victim (NameLN-delete tombstone is T's first entry).
    let txn = env.begin_transaction(None).unwrap();
    env.remove_database(Some(&txn), "victim").unwrap();
    let tombstone_file = *ndb_files(&path).last().unwrap();

    roll(&filler, &env, 60);
    barrier(&env);

    let survived = ndb_files(&path).contains(&tombstone_file);
    txn.commit().unwrap();
    filler.close().unwrap();
    env.close().unwrap();

    assert!(
        survived,
        "the file (#{tombstone_file}) holding the OPEN remove-in-txn NameLN \
         tombstone was deleted by a forced clean while the txn was open \
         (R-FIRSTLSN)"
    );
}

/// rename-in-txn: the file holding the open txn's rename NameLN entries must
/// survive a forced clean while the txn is open.
#[test]
fn rename_in_txn_nameln_file_protected() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let env = Environment::open(env_cfg(&path)).unwrap();
    let filler = env.open_database(None, "filler", &dbcfg()).unwrap();

    {
        let vt = env.begin_transaction(None).unwrap();
        let v = env.open_database(Some(&vt), "victim", &dbcfg()).unwrap();
        v.put_in(
            &vt,
            DatabaseEntry::from_bytes(b"k"),
            DatabaseEntry::from_bytes(b"v"),
        )
        .unwrap();
        v.close().unwrap();
        vt.commit().unwrap();
    }
    roll(&filler, &env, 40);

    let txn = env.begin_transaction(None).unwrap();
    env.rename_database(Some(&txn), "victim", "victim2").unwrap();
    let nameln_file = *ndb_files(&path).last().unwrap();

    roll(&filler, &env, 60);
    barrier(&env);

    let survived = ndb_files(&path).contains(&nameln_file);
    txn.commit().unwrap();
    filler.close().unwrap();
    env.close().unwrap();

    assert!(
        survived,
        "the file (#{nameln_file}) holding the OPEN rename-in-txn NameLN \
         entries was deleted by a forced clean while the txn was open \
         (R-FIRSTLSN)"
    );
}
