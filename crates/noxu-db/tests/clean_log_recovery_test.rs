//! Regression tests for the `Environment::clean_log()` recovery-corruption
//! data-safety bug (fix/clean-log-recovery-corruption).
//!
//! # The bug
//!
//! Noxu's database catalog (name -> id mapping) is an in-memory `HashMap`
//! rebuilt from `NameLN` WAL entries during recovery (REC-B) — NOT a
//! checkpointed mapping tree the way JE stores it.  The log cleaner does not
//! recognise `NameLN` / `NameLNTxn` entries (they fall into the `Other`
//! bucket in `Cleaner::decode_ln_entries_from_file`) and so never migrates
//! them forward the way JE's cleaner migrates naming/mapping-tree LNs via
//! `FileProcessor.processLN`.
//!
//! A single `clean_log()` + reopen was fine (the file holding the `NameLN`
//! had not been selected for reclamation yet), but *repeated* force-clean +
//! checkpoint cycles eventually reclaimed the file that held a database's
//! only `NameLN`.  Recovery then could not find the database and
//! `open_database` failed with `DatabaseNotFound` — losing the database (and
//! all its records) entirely.
//!
//! # The fix
//!
//! The checkpointer re-logs the live catalog (one fresh `NameLN` per open
//! database) at the START of every checkpoint.  Because the cleaner only
//! deletes a file after it passes the two-checkpoint deletion barrier, a
//! fresh `NameLN` for every live database always lands in a file newer than
//! any file the barrier can make deletable — so recovery's full-log scan
//! always finds it.  This is Noxu's analog of JE flushing the mapping-tree
//! root at checkpoint (`Checkpointer.flushRoot`) so the catalog is durable at
//! the checkpoint fence, restoring JE's "do not delete a cleaned file until a
//! checkpoint reflects its (migrated) entries" invariant for the HashMap
//! catalog.
//!
//! These tests FAIL on the pre-fix base (the repeated-cycle case gets
//! `DatabaseNotFound` on reopen) and PASS after the fix.

use noxu_db::{DatabaseConfig, Environment, EnvironmentConfig};
use tempfile::TempDir;

/// JE-faithful reclamation (REG-CLEANER-TEST-NONJE reconciliation).
///
/// The cleaner must reclaim files that contain OBSOLETE space (superseded LN
/// versions) WITHOUT losing any live record — never-rewritten records that
/// happen to reside in old files must survive both physical reclamation and a
/// subsequent recovery.  This is the real JE property
/// (`UtilizationCalculator.getBestFile`, UtilizationCalculator.java:405-431:
/// a file is selected for cleaning only when its utilization is below the
/// threshold — i.e. it has obsolete space — never merely because it is old).
///
/// The ORIGINAL form of this test (added by the regressing merge 1cc6b013)
/// asserted that `clean_log()` physically deletes the 100%-LIVE, never-
/// rewritten old files (`reclaimed > 0` over `old_files`).  That is NOT JE
/// behavior: `getBestFile` under `forceCleaning=false` (which `clean_log()`
/// now maps to, matching `Environment.cleanLog` -> `invokeCleaner`) NEVER
/// selects a 100%-live file — the `forceCleaning` branch that would is
/// explicitly labelled "forced for testing".  Forcing it back reintroduces
/// the migration treadmill (REG-CLEANER-DISKLIMIT).  Reconciled here to assert
/// the JE-faithful property: obsolete-space files ARE reclaimed, live records
/// are NOT lost.
fn old_live_records_survive_reclamation(clean: bool) {
    use noxu_db::CheckpointConfig;
    use std::collections::BTreeSet;

    let tmp = TempDir::new().unwrap();
    let files = || -> BTreeSet<_> {
        std::fs::read_dir(tmp.path())
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| p.extension().is_some_and(|e| e == "ndb"))
            .collect()
    };
    let config = || {
        let mut cfg = EnvironmentConfig::new(tmp.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true)
            .with_log_file_max_bytes(64 * 1024)
            .with_cache_size(32 * 1024 * 1024);
        cfg.set_run_cleaner(false);
        cfg.set_run_checkpointer(false);
        cfg.set_run_evictor(false);
        cfg.set_run_in_compressor(false);
        cfg
    };
    let value = |k: u32, generation: u8| {
        let mut v = vec![generation; 512];
        v[..4].copy_from_slice(&k.to_be_bytes());
        v
    };
    let env = Environment::open(config()).unwrap();
    let db_cfg =
        DatabaseConfig::new().with_allow_create(true).with_transactional(true);
    let db = env.open_database(None, "old-live", &db_cfg).unwrap();
    for k in 0u32..2000 {
        db.put(k.to_be_bytes(), value(k, 0)).unwrap();
    }
    db.sync().unwrap();
    let old_files = files();
    assert!(old_files.len() > 2, "old records must span closed files");

    // The old keys are NEVER rewritten. Only newer keys generate obsolete
    // versions, so a clean-close checkpoint cannot repair lost old LN images.
    for generation in 1..=4 {
        for k in 2000u32..3000 {
            db.put(k.to_be_bytes(), value(k, generation)).unwrap();
        }
    }
    let checkpoint = CheckpointConfig::new().with_force(true);
    env.checkpoint(Some(&checkpoint)).unwrap();
    // Peak file count: after loading + churning + a first checkpoint, before
    // any reclamation.  A successful clean of the obsolete-churn files brings
    // the on-disk count below this.
    let peak_file_count = files().len();
    let mut cleaned = 0;
    for _ in 0..3 {
        if clean {
            cleaned += env.clean_log().unwrap();
        }
        env.checkpoint(Some(&checkpoint)).unwrap();
    }
    let remaining = files();
    // Total on-disk file count BEFORE the churn+clean cycle vs AFTER: the
    // churn (keys 2000..3000 rewritten 4x) creates whole files of superseded
    // (obsolete) LN versions, which the cleaner reclaims.  We assert the
    // JE-faithful outcome: files with obsolete space are physically reclaimed
    // (the total file count drops relative to the peak), while the 100%-live
    // never-rewritten old files (keys 0..2000) are NOT selected — matching JE
    // `getBestFile(forceCleaning=false)`.
    let old_live_files_still_present =
        old_files.intersection(&remaining).count();
    eprintln!(
        "clean={clean}: cleaned={cleaned}, live-old files still present={}/{}, total files now={}",
        old_live_files_still_present,
        old_files.len(),
        remaining.len(),
    );
    if clean {
        // JE-faithful reclaim of OBSOLETE-space files happened (the churn's
        // superseded versions were migrated/reclaimed).
        assert!(cleaned > 0, "must activate real cleaner processing");
        // Physical reclamation occurred: with a genuinely-obsolete churn set,
        // at least one whole file of superseded versions is deleted, so the
        // on-disk file count is strictly below the count of files that existed
        // at the churn peak.  (We compare against the peak captured below.)
        assert!(
            remaining.len() < peak_file_count,
            "cleaner must physically delete at least one obsolete-space file              (peak={peak_file_count}, now={})",
            remaining.len()
        );
        // JE-faithful: the 100%-live never-rewritten old files are NOT
        // force-reclaimed (forceCleaning=false never selects them). They must
        // still be present — their live records have not been thrown away.
        assert_eq!(
            old_live_files_still_present,
            old_files.len(),
            "100%-live never-rewritten old files must NOT be reclaimed under              clean_log() (JE forceCleaning=false); their live records must              stay on disk"
        );
    } else {
        // Control: without ever calling clean_log(), no file is reclaimed.
        assert_eq!(
            old_files.intersection(&remaining).count(),
            old_files.len(),
            "control must not reclaim files"
        );
    }
    db.close().unwrap();
    env.close().unwrap();
    drop(db);
    drop(env);

    let env = Environment::open(config()).unwrap();
    let db = env
        .open_database(
            None,
            "old-live",
            &DatabaseConfig::new().with_transactional(true),
        )
        .unwrap();
    let mut missing_old = 0;
    let mut mismatches = Vec::new();
    for k in 0u32..3000 {
        let actual = db.get(k.to_be_bytes()).unwrap();
        if k < 2000 && actual.is_none() {
            missing_old += 1;
        }
        if actual.as_deref()
            != Some(value(k, if k < 2000 { 0 } else { 4 }).as_slice())
        {
            mismatches.push(k);
        }
    }
    eprintln!(
        "clean={clean}: missing old records={missing_old}/2000, mismatched key/value pairs={}",
        mismatches.len()
    );
    assert!(
        mismatches.is_empty(),
        "reopen lost/changed {} records; first keys: {:?}",
        mismatches.len(),
        &mismatches[..mismatches.len().min(20)]
    );
    db.close().unwrap();
    env.close().unwrap();
}

#[test]
fn cleaner_reclaims_old_files_without_losing_live_records() {
    old_live_records_survive_reclamation(true);
}

#[test]
fn old_live_records_without_cleaning_control() {
    old_live_records_survive_reclamation(false);
}

/// Single clean_log() + reopen preserves all records.  (Passed even before
/// the fix — kept as a lower-bound guard.)
#[test]
fn clean_log_then_reopen_preserves_all_records() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path();
    let value = vec![0xCDu8; 512];
    let n = 500u32;

    // Phase 1: load + update-churn (create obsolete versions), force-clean, close.
    {
        let mut cfg = EnvironmentConfig::new(dir.to_path_buf())
            .with_allow_create(true)
            .with_transactional(true)
            .with_log_file_max_bytes(64 * 1024)
            .with_cache_size(16 * 1024 * 1024);
        cfg.set_run_cleaner(false); // no daemon; we force explicitly
        cfg.set_run_checkpointer(false);
        let env = Environment::open(cfg).unwrap();
        let db = env
            .open_database(
                None,
                "t",
                &DatabaseConfig::new()
                    .with_allow_create(true)
                    .with_transactional(true),
            )
            .unwrap();
        // churn: write each key 5x so prior versions become obsolete
        for _ in 0..5 {
            for k in 0..n {
                db.put(k.to_be_bytes(), &value).unwrap();
            }
        }
        db.sync().unwrap();
        let reclaimed = env.clean_log().unwrap();
        eprintln!("clean_log reclaimed {reclaimed} files");
        // checkpoint so cleaned state is durable, then close cleanly
        env.checkpoint(None).ok();
        db.close().unwrap();
        env.close().unwrap();
    }

    // Phase 2: reopen (runs recovery) and count.
    {
        let mut cfg = EnvironmentConfig::new(dir.to_path_buf())
            .with_transactional(true)
            .with_cache_size(16 * 1024 * 1024);
        cfg.set_run_cleaner(false);
        cfg.set_run_checkpointer(false);
        let env = Environment::open(cfg).unwrap();
        let db = env
            .open_database(
                None,
                "t",
                &DatabaseConfig::new().with_transactional(true),
            )
            .unwrap();
        let mut found = 0u32;
        for k in 0..n {
            if db.get(k.to_be_bytes()).unwrap().is_some() {
                found += 1;
            }
        }
        eprintln!("after clean_log + reopen: {found}/{n} records survived");
        assert_eq!(found, n, "clean_log + reopen LOST records: {found}/{n}");
    }
}

/// clean_log() with the background cleaner + checkpointer daemons enabled,
/// then a clean close + reopen preserves all records.
#[test]
fn clean_log_with_daemons_then_reopen_preserves_all_records() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path();
    let value = vec![0xCDu8; 512];
    let n = 500u32;
    {
        let mut cfg = EnvironmentConfig::new(dir.to_path_buf())
            .with_allow_create(true)
            .with_transactional(true)
            .with_log_file_max_bytes(64 * 1024)
            .with_cache_size(16 * 1024 * 1024);
        cfg.set_run_cleaner(true); // daemons ON
        cfg.set_run_checkpointer(true);
        cfg.set_cleaner_min_utilization(50);
        let env = Environment::open(cfg).unwrap();
        let db = env
            .open_database(
                None,
                "t",
                &DatabaseConfig::new()
                    .with_allow_create(true)
                    .with_transactional(true),
            )
            .unwrap();
        for _ in 0..5 {
            for k in 0..n {
                db.put(k.to_be_bytes(), &value).unwrap();
            }
        }
        db.sync().unwrap();
        let reclaimed = env.clean_log().unwrap();
        eprintln!("[daemons] clean_log reclaimed {reclaimed} files");
        // NO explicit checkpoint — close cleanly and see if recovery is intact
        db.close().unwrap();
        env.close().unwrap();
    }
    {
        let mut cfg = EnvironmentConfig::new(dir.to_path_buf())
            .with_transactional(true)
            .with_cache_size(16 * 1024 * 1024);
        cfg.set_run_cleaner(false);
        cfg.set_run_checkpointer(false);
        let env = Environment::open(cfg).unwrap();
        let db = env
            .open_database(
                None,
                "t",
                &DatabaseConfig::new().with_transactional(true),
            )
            .unwrap();
        let mut found = 0u32;
        for k in 0..n {
            if db.get(k.to_be_bytes()).unwrap().is_some() {
                found += 1;
            }
        }
        eprintln!("[daemons] after clean_log + reopen: {found}/{n} survived");
        assert_eq!(
            found, n,
            "[daemons] clean_log + reopen LOST records: {found}/{n}"
        );
    }
}

/// THE REGRESSION GUARD: repeated force-clean + checkpoint cycles must not
/// lose the database or its records.  FAILS on the pre-fix base with
/// `DatabaseNotFound` on reopen (the file holding the database's only NameLN
/// was reclaimed); PASSES after the fix.
#[test]
fn repeated_clean_log_checkpoint_cycles_then_reopen() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path();
    let value = vec![0xEEu8; 512];
    let n = 300u32;
    {
        let mut cfg = EnvironmentConfig::new(dir.to_path_buf())
            .with_allow_create(true)
            .with_transactional(true)
            .with_log_file_max_bytes(64 * 1024)
            .with_cache_size(16 * 1024 * 1024);
        cfg.set_run_cleaner(false);
        cfg.set_run_checkpointer(false);
        let env = Environment::open(cfg).unwrap();
        let db = env
            .open_database(
                None,
                "t",
                &DatabaseConfig::new()
                    .with_allow_create(true)
                    .with_transactional(true),
            )
            .unwrap();
        // several rounds of churn + clean + checkpoint (multi-checkpoint clean)
        for round in 0..4 {
            for _ in 0..3 {
                for k in 0..n {
                    db.put(k.to_be_bytes(), &value).unwrap();
                }
            }
            db.sync().unwrap();
            let r = env.clean_log().unwrap();
            env.checkpoint(None).ok();
            eprintln!("round {round}: clean_log reclaimed {r}");
        }
        db.close().unwrap();
        env.close().unwrap();
    }
    {
        let mut cfg = EnvironmentConfig::new(dir.to_path_buf())
            .with_transactional(true)
            .with_cache_size(16 * 1024 * 1024);
        cfg.set_run_cleaner(false);
        cfg.set_run_checkpointer(false);
        let env = Environment::open(cfg).unwrap();
        let db = env
            .open_database(
                None,
                "t",
                &DatabaseConfig::new().with_transactional(true),
            )
            .expect(
                "database must survive repeated clean_log + checkpoint cycles",
            );
        let mut found = 0u32;
        for k in 0..n {
            if db.get(k.to_be_bytes()).unwrap().is_some() {
                found += 1;
            }
        }
        eprintln!("multi-checkpoint clean reopen: {found}/{n} survived");
        assert_eq!(
            found, n,
            "multi-checkpoint clean_log LOST records: {found}/{n}"
        );
    }
}

/// Multi-database variant of the regression guard: several databases must ALL
/// survive repeated force-clean + checkpoint cycles (each database's `NameLN`
/// must be preserved).
#[test]
fn repeated_clean_log_multiple_databases_all_survive() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path();
    let value = vec![0x5Au8; 512];
    let n = 150u32;
    let db_names = ["alpha", "beta", "gamma"];
    {
        let mut cfg = EnvironmentConfig::new(dir.to_path_buf())
            .with_allow_create(true)
            .with_transactional(true)
            .with_log_file_max_bytes(64 * 1024)
            .with_cache_size(16 * 1024 * 1024);
        cfg.set_run_cleaner(false);
        cfg.set_run_checkpointer(false);
        let env = Environment::open(cfg).unwrap();
        let dbs: Vec<_> = db_names
            .iter()
            .map(|name| {
                env.open_database(
                    None,
                    name,
                    &DatabaseConfig::new()
                        .with_allow_create(true)
                        .with_transactional(true),
                )
                .unwrap()
            })
            .collect();
        for round in 0..4 {
            for _ in 0..3 {
                for db in &dbs {
                    for k in 0..n {
                        db.put(k.to_be_bytes(), &value).unwrap();
                    }
                }
            }
            for db in &dbs {
                db.sync().unwrap();
            }
            let r = env.clean_log().unwrap();
            env.checkpoint(None).ok();
            eprintln!("[multi-db] round {round}: clean_log reclaimed {r}");
        }
        for db in dbs {
            db.close().unwrap();
        }
        env.close().unwrap();
    }
    {
        let mut cfg = EnvironmentConfig::new(dir.to_path_buf())
            .with_transactional(true)
            .with_cache_size(16 * 1024 * 1024);
        cfg.set_run_cleaner(false);
        cfg.set_run_checkpointer(false);
        let env = Environment::open(cfg).unwrap();
        for name in db_names {
            let db = env
                .open_database(
                    None,
                    name,
                    &DatabaseConfig::new().with_transactional(true),
                )
                .unwrap_or_else(|e| {
                    panic!("database '{name}' must survive: {e:?}")
                });
            let mut found = 0u32;
            for k in 0..n {
                if db.get(k.to_be_bytes()).unwrap().is_some() {
                    found += 1;
                }
            }
            assert_eq!(
                found, n,
                "[multi-db] '{name}' LOST records: {found}/{n}"
            );
        }
        eprintln!("[multi-db] all {} databases survived", db_names.len());
    }
}

/// CONTROL: identical rounds but with NO clean_log — isolates the bug to
/// `clean_log()`, not checkpointing.  Passes before and after the fix.
#[test]
fn repeated_checkpoint_no_clean_then_reopen() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path();
    let value = vec![0xEEu8; 512];
    let n = 300u32;
    {
        let mut cfg = EnvironmentConfig::new(dir.to_path_buf())
            .with_allow_create(true)
            .with_transactional(true)
            .with_log_file_max_bytes(64 * 1024)
            .with_cache_size(16 * 1024 * 1024);
        cfg.set_run_cleaner(false);
        cfg.set_run_checkpointer(false);
        let env = Environment::open(cfg).unwrap();
        let db = env
            .open_database(
                None,
                "t",
                &DatabaseConfig::new()
                    .with_allow_create(true)
                    .with_transactional(true),
            )
            .unwrap();
        for _round in 0..4 {
            for _ in 0..3 {
                for k in 0..n {
                    db.put(k.to_be_bytes(), &value).unwrap();
                }
            }
            db.sync().unwrap();
            env.checkpoint(None).ok(); // NO clean_log
        }
        db.close().unwrap();
        env.close().unwrap();
    }
    {
        let mut cfg = EnvironmentConfig::new(dir.to_path_buf())
            .with_transactional(true)
            .with_cache_size(16 * 1024 * 1024);
        cfg.set_run_cleaner(false);
        cfg.set_run_checkpointer(false);
        let env = Environment::open(cfg).unwrap();
        let db = env
            .open_database(
                None,
                "t",
                &DatabaseConfig::new().with_transactional(true),
            )
            .unwrap();
        let mut found = 0u32;
        for k in 0..n {
            if db.get(k.to_be_bytes()).unwrap().is_some() {
                found += 1;
            }
        }
        eprintln!("CONTROL (checkpoint, no clean): {found}/{n} survived");
        assert_eq!(found, n);
    }
}
