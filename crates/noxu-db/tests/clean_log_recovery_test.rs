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
