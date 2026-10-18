//! Witness for R-UNOPENED-RECLAIM — the cleaner must not reclaim the log file
//! of a database that was recovered from the log but has NOT been opened since
//! the restart.
//!
//! Root cause (rc2 2662fad4): the cleaner's `SharedTreeLookup::resolve_tree`
//! (`crates/noxu-cleaner/src/file_processor.rs:843-844`) does
//! `self.extra_trees.get(&db_id).unwrap_or(&self.tree)`, where `self.tree` is
//! the `db_id = 1` PLACEHOLDER primary tree (`Tree::new(1, 256)`,
//! `environment_impl.rs:1236`).  The `extra_trees` registry is populated ONLY
//! at `open_database_inner` time (`environment_impl.rs:2524`, `2932`, `3420`):
//! a recovered tree sits in `self.recovered_trees` and is transplanted into
//! the registry only when the application OPENS that database.  So for a
//! database that exists (recovered from the WAL) but that nobody has opened
//! since the restart, its tree is absent from `extra_trees`; the cleaner
//! checks its LNs against the empty id=1 placeholder, every LN looks NotFound
//! -> Dead -> the file holding its records (and/or its root) is reclaimed ->
//! PERMANENT DATA LOSS.
//!
//! JE: `FileProcessor.processLN`/`processFile` resolve the db via
//! `envImpl.getDbTree().getDb(dbId, ...)` (FileProcessor.java:856, 1114),
//! which FAULTS IN the `DatabaseImpl` from the id-database catalog
//! (`DbTree.getDb(DatabaseId, long, String)`, DbTree.java:1609-1670 — a
//! CursorImpl.searchExact on the id DB) even when no user handle is open.  An
//! LN is declared obsolete only when `db == null || db.isDeleted()`
//! (FileProcessor.java:864, 1117) — i.e. only when the catalog says the id is
//! absent or deleted.  An unopened-but-existing db's LNs are therefore checked
//! against its REAL recovered tree, never a placeholder.
//!
//! Each variant: write records into a db, close the env, REOPEN WITHOUT
//! opening that db, force-clean, then reopen+open+read.  RED on rc2: records
//! are gone.  GREEN with the fix: all records survive.

use noxu_db::{
    CheckpointConfig, Comparator, Database, DatabaseConfig, DatabaseEntry,
    Environment, EnvironmentConfig, Mutex, OperationStatus, SecondaryConfig,
    SecondaryDatabase, SecondaryKeyCreator,
};
use std::path::Path;
use std::sync::Arc;

const N: u32 = 400;
const VLEN: usize = 400;

fn open_env(dir: &Path, create: bool) -> Environment {
    let mut cfg = EnvironmentConfig::new(dir.to_path_buf())
        .with_allow_create(create)
        .with_transactional(true);
    // Deterministic: no background daemons; we drive the cleaner explicitly.
    cfg.set_run_cleaner(false);
    cfg.set_run_checkpointer(false);
    cfg.set_run_evictor(false);
    cfg.set_run_in_compressor(false);
    // Small files so each db's records sit in their own non-current file that
    // becomes age-eligible once we roll the write position past them.  minAge
    // 1 (JE minimum) so a file one older than the current one is cleanable.
    cfg.set_log_file_max_bytes(64 * 1024);
    cfg.set_cleaner_min_age(1);
    Environment::open(cfg).expect("open env")
}

fn db_config() -> DatabaseConfig {
    DatabaseConfig::new().with_allow_create(true).with_transactional(true)
}

fn ro_config() -> DatabaseConfig {
    DatabaseConfig::new().with_transactional(true)
}

fn fill(db: &Database, n: u32, tag: u8) {
    let v = vec![tag; VLEN];
    for k in 0..n {
        db.put(DatabaseEntry::from_bytes(&k.to_be_bytes()), &v).unwrap();
    }
}

/// Churn through an always-open db by OVERWRITING the same 64 keys many times.
/// This (a) rolls the log forward so the target db's earlier file(s) become
/// age-eligible, and (b) leaves the churn db's own earlier files mostly
/// obsolete, so the cleaner has reclaimable churn files to delete even while it
/// RETAINS the unopened target's files (proving no starvation — requirement 3).
fn roll_log(db: &Database, generations: u32) {
    let v = vec![0xEEu8; VLEN];
    for _g in 0..generations {
        for k in 0..64u32 {
            db.put(DatabaseEntry::from_bytes(&k.to_be_bytes()), &v).unwrap();
        }
    }
}

/// Drive the cleaner's two-checkpoint deletion barrier so cleaned files are
/// actually DELETED (not merely marked cleaned).  Returns the number of files
/// the cleaner deleted in this env session.  A clean alone only
/// `mark_file_cleaned`s; the file is removed only after it passes
/// cleaned -> checkpointed -> safe_to_delete, driven by checkpoints.
fn force_reclaim(env: &Environment) -> u64 {
    let force = CheckpointConfig::new().with_force(true);
    for _ in 0..3 {
        env.clean_log_forced().unwrap();
        env.checkpoint(Some(&force)).unwrap();
    }
    // One more clean so delete_safe_files() runs after the barrier cleared.
    env.clean_log_forced().unwrap();
    env.stats().unwrap().cleaner.deletions
}

/// Count how many of the target db's N keys survive a reopen+open+read.
fn survivors_default(path: &Path, name: &str, tag: u8) -> u32 {
    let env = open_env(path, false);
    let db = env
        .open_database(None, name, &ro_config())
        .expect("target db must still exist after clean+reopen");
    let mut n = 0u32;
    let want = vec![tag; VLEN];
    for k in 0..N {
        let mut v = DatabaseEntry::new();
        if db
            .get_into(None, k.to_be_bytes(), &mut v)
            .expect("get must not hit a dangling slot into a reclaimed file")
            && v.data_opt() == Some(&want[..])
        {
            n += 1;
        }
    }
    db.close().unwrap();
    env.close().unwrap();
    n
}

// ───────────────────────── variant (a): default comparator, id > 1 ─────────

#[test]
fn unopened_default_db_survives_force_clean() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    {
        let env = open_env(&path, true);
        // db "churn" gets id=1 (first user db); db "target" gets id=2.
        let churn = env.open_database(None, "churn", &db_config()).unwrap();
        let target = env.open_database(None, "target", &db_config()).unwrap();
        fill(&target, N, 0xA1);
        roll_log(&churn, 60);
        target.close().unwrap();
        churn.close().unwrap();
        env.close().unwrap();
    }
    // Reopen WITHOUT opening "target"; keep "churn" open (so id=1 exists) and
    // force a clean.  The target's recovered tree is absent from extra_trees.
    {
        let env = open_env(&path, false);
        let churn = env.open_database(None, "churn", &ro_config()).unwrap();
        let deleted = force_reclaim(&env);
        eprintln!(
            "[a] cleaner deletions={deleted} (target files RETAINED; \
            churn files may be reclaimed)"
        );
        churn.close().unwrap();
        env.close().unwrap();
    }
    let n = survivors_default(&path, "target", 0xA1);
    assert_eq!(
        n,
        N,
        "unopened default-comparator db lost {} / {N} records",
        N - n
    );
}

// ───────────────────────── variant (b): custom comparator ──────────────────

fn reverse_cmp() -> Comparator {
    Comparator::new("reverse_be_u32", |a: &[u8], b: &[u8]| b.cmp(a))
}

#[test]
fn unopened_custom_comparator_db_survives_force_clean() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    {
        let env = open_env(&path, true);
        let churn = env.open_database(None, "churn", &db_config()).unwrap();
        let target = env
            .open_database(
                None,
                "cmp_target",
                &db_config().with_btree_comparator(reverse_cmp()),
            )
            .unwrap();
        fill(&target, N, 0xB2);
        roll_log(&churn, 60);
        target.close().unwrap();
        churn.close().unwrap();
        env.close().unwrap();
    }
    {
        let env = open_env(&path, false);
        let churn = env.open_database(None, "churn", &ro_config()).unwrap();
        let deleted = force_reclaim(&env);
        eprintln!(
            "[b] cleaner deletions={deleted} (target files RETAINED; \
            churn files may be reclaimed)"
        );
        churn.close().unwrap();
        env.close().unwrap();
    }
    // Reopen with the matching comparator (required for a comparator db).
    let env = open_env(&path, false);
    let db = env
        .open_database(
            None,
            "cmp_target",
            &ro_config().with_btree_comparator(reverse_cmp()),
        )
        .expect("custom-comparator db must still exist after clean+reopen");
    let mut n = 0u32;
    let want = vec![0xB2u8; VLEN];
    for k in 0..N {
        let mut v = DatabaseEntry::new();
        if db.get_into(None, k.to_be_bytes(), &mut v).expect("get must not Err")
            && v.data_opt() == Some(&want[..])
        {
            n += 1;
        }
    }
    db.close().unwrap();
    env.close().unwrap();
    assert_eq!(
        n,
        N,
        "unopened custom-comparator db lost {} / {N} records",
        N - n
    );
}

// ───────────────── variant (c): secondary whose primary is open ────────────

struct FirstByteKeyCreator;
impl SecondaryKeyCreator for FirstByteKeyCreator {
    fn create_secondary_key(
        &self,
        _db: &Database,
        _key: &DatabaseEntry,
        data: &DatabaseEntry,
        result: &mut DatabaseEntry,
    ) -> bool {
        if let Some(d) = data.data_opt()
            && !d.is_empty()
        {
            result.set_data(&[d[0]]);
            return true;
        }
        false
    }
}

fn secondary_count(sec: &SecondaryDatabase) -> u64 {
    let mut c = sec.open_cursor(None).unwrap();
    let mut sk = DatabaseEntry::new();
    let mut pk = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    let mut n = 0u64;
    let mut st = c.get_first(&mut sk, &mut pk, &mut d).unwrap();
    while st == OperationStatus::Success {
        n += 1;
        st = c.get_next(&mut sk, &mut pk, &mut d).unwrap();
    }
    n
}

#[test]
fn unopened_secondary_survives_while_primary_open() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    {
        let env = open_env(&path, true);
        let churn = env.open_database(None, "churn", &db_config()).unwrap();
        let primary = Arc::new(Mutex::new(
            env.open_database(None, "prim", &db_config()).unwrap(),
        ));
        let inner = env
            .open_database(
                None,
                "sec",
                &db_config().with_sorted_duplicates(true),
            )
            .unwrap();
        let sec = SecondaryDatabase::open(
            Arc::clone(&primary),
            inner,
            SecondaryConfig::new()
                .with_allow_create(true)
                .with_key_creator(Box::new(FirstByteKeyCreator)),
        )
        .unwrap();
        // Each primary record's data[0] is a bucketed secondary key.
        for k in 0..N {
            let data = vec![(k % 251) as u8; VLEN];
            primary
                .lock()
                .put(DatabaseEntry::from_bytes(&k.to_be_bytes()), &data)
                .unwrap();
        }
        assert_eq!(
            secondary_count(&sec),
            N as u64,
            "preflight: secondary populated"
        );
        roll_log(&churn, 60);
        sec.close().unwrap();
        primary.lock().close().unwrap();
        drop(primary);
        churn.close().unwrap();
        env.close().unwrap();
    }
    // Reopen, open the PRIMARY (its tree is registered) but NOT the inner
    // secondary index db, then force a clean.  The inner db's recovered tree
    // is absent from extra_trees -> its LNs resolve against the placeholder.
    {
        let env = open_env(&path, false);
        let churn = env.open_database(None, "churn", &ro_config()).unwrap();
        let primary = env.open_database(None, "prim", &ro_config()).unwrap();
        let deleted = force_reclaim(&env);
        eprintln!(
            "[c] cleaner deletions={deleted} (target files RETAINED; \
            churn files may be reclaimed)"
        );
        primary.close().unwrap();
        churn.close().unwrap();
        env.close().unwrap();
    }
    // Reopen everything and verify the secondary index still resolves.
    let env = open_env(&path, false);
    let primary = Arc::new(Mutex::new(
        env.open_database(None, "prim", &ro_config()).unwrap(),
    ));
    let inner = env
        .open_database(None, "sec", &ro_config().with_sorted_duplicates(true))
        .unwrap();
    let sec = SecondaryDatabase::open(
        Arc::clone(&primary),
        inner,
        SecondaryConfig::new().with_key_creator(Box::new(FirstByteKeyCreator)),
    )
    .expect("secondary db must still exist after clean+reopen");
    let n = secondary_count(&sec);
    sec.close().unwrap();
    // Close the primary handle before closing the env (the Arc<Mutex<Database>>
    // still holds an open handle otherwise).
    primary.lock().close().unwrap();
    drop(primary);
    env.close().unwrap();
    assert_eq!(
        n,
        N as u64,
        "unopened secondary lost {} / {N} index entries after clean+reopen",
        N as u64 - n
    );
}

// ───────────────── variant (d): the id==1 collision direction ──────────────

#[test]
fn unopened_id1_db_survives_force_clean() {
    // Here the TARGET is the first user db -> db_id = 1, the placeholder's id.
    // On reopen the placeholder primary_tree is EMPTY (the recovered id=1 tree
    // is transplanted only on open), so an unopened id=1 db still resolves to
    // an empty tree -> NotFound -> reclaimed.  The other collision direction.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    {
        let env = open_env(&path, true);
        let target = env.open_database(None, "target", &db_config()).unwrap(); // id=1
        let churn = env.open_database(None, "churn", &db_config()).unwrap(); // id=2
        fill(&target, N, 0xD4);
        roll_log(&churn, 60);
        target.close().unwrap();
        churn.close().unwrap();
        env.close().unwrap();
    }
    {
        let env = open_env(&path, false);
        // Open ONLY churn (id=2); leave the id=1 target unopened.
        let churn = env.open_database(None, "churn", &ro_config()).unwrap();
        let deleted = force_reclaim(&env);
        eprintln!(
            "[d] cleaner deletions={deleted} (target files RETAINED; \
            churn files may be reclaimed)"
        );
        churn.close().unwrap();
        env.close().unwrap();
    }
    let n = survivors_default(&path, "target", 0xD4);
    assert_eq!(n, N, "unopened id=1 db lost {} / {N} records", N - n);
}

// ───────────── requirement (3): no starvation — other files still cleaned ───

#[test]
fn unopened_db_does_not_starve_cleaning_of_other_files() {
    // An unopened db pins its files (retained), but the cleaner must still
    // reclaim an OPEN db's obsolete files.  JE: a file that can't be fully
    // processed is put back with updated utilization; it never blocks other
    // files' selection.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    {
        let env = open_env(&path, true);
        let churn = env.open_database(None, "churn", &db_config()).unwrap();
        let target = env.open_database(None, "target", &db_config()).unwrap();
        fill(&target, N, 0xC3);
        // Heavy overwrite churn → many obsolete churn files to reclaim.
        roll_log(&churn, 120);
        target.close().unwrap();
        churn.close().unwrap();
        env.close().unwrap();
    }
    let deleted;
    {
        let env = open_env(&path, false);
        // target stays UNOPENED; churn open.
        let churn = env.open_database(None, "churn", &ro_config()).unwrap();
        deleted = force_reclaim(&env);
        churn.close().unwrap();
        env.close().unwrap();
    }
    eprintln!("[starvation] deletions={deleted}");
    assert!(
        deleted > 0,
        "cleaner reclaimed NO files — the unopened db starved all cleaning \
         (requirement 3: retained files must not block other files)"
    );
    // And the unopened target still survives.
    let n = survivors_default(&path, "target", 0xC3);
    assert_eq!(
        n,
        N,
        "target lost {} / {N} records during no-starvation run",
        N - n
    );
}

// ───────────── requirement (4): once opened, later pass reclaims ────────────

#[test]
fn opened_db_file_reclaims_normally_after_registration() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    {
        let env = open_env(&path, true);
        let churn = env.open_database(None, "churn", &db_config()).unwrap();
        let target = env.open_database(None, "target", &db_config()).unwrap();
        fill(&target, N, 0xE5);
        roll_log(&churn, 60);
        target.close().unwrap();
        churn.close().unwrap();
        env.close().unwrap();
    }
    // Reopen and OPEN the target (registers its tree), then overwrite all its
    // keys so its old file becomes obsolete, then clean: the old file must be
    // reclaimed normally and all records remain readable at their new value.
    {
        let env = open_env(&path, false);
        let target = env.open_database(None, "target", &ro_config()).unwrap();
        let churn = env.open_database(None, "churn", &ro_config()).unwrap();
        let newv = vec![0x5Eu8; VLEN];
        for k in 0..N {
            target
                .put(DatabaseEntry::from_bytes(&k.to_be_bytes()), &newv)
                .unwrap();
        }
        let deleted = force_reclaim(&env);
        eprintln!("[reopened] deletions={deleted}");
        target.close().unwrap();
        churn.close().unwrap();
        env.close().unwrap();
    }
    // Every record survives at its NEW value.
    let n = survivors_default(&path, "target", 0x5E);
    assert_eq!(n, N, "opened+reclaimed db lost {} / {N} records", N - n);
}

// ───────────── requirement (5): a truly removed db's file IS reclaimed ──────

#[test]
fn removed_db_file_is_reclaimed() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    {
        let env = open_env(&path, true);
        let churn = env.open_database(None, "churn", &db_config()).unwrap();
        let doomed = env.open_database(None, "doomed", &db_config()).unwrap();
        fill(&doomed, N, 0xF6);
        roll_log(&churn, 40);
        doomed.close().unwrap();
        // Remove the database durably: its LNs become genuinely obsolete.
        env.remove_database(None, "doomed").unwrap();
        let deleted = force_reclaim(&env);
        eprintln!("[removed] deletions={deleted}");
        assert!(
            deleted > 0,
            "a removed db's obsolete files must be reclaimable (requirement 5)"
        );
        churn.close().unwrap();
        env.close().unwrap();
    }
    // The removed db must be gone on reopen.
    let env = open_env(&path, false);
    let res = env.open_database(None, "doomed", &ro_config());
    assert!(res.is_err(), "removed db 'doomed' must not exist after reopen");
    env.close().unwrap();
}
