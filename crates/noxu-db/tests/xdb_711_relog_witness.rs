//! XDB-711 — deterministic cross-database resurrection witness (S1).
//!
//! Root cause (`xdb-711.md` §4): the per-checkpoint catalog relog
//! (`relog_live_catalog_impl`) snapshots `name_map` as `(name, id)` pairs and
//! then logs one **non-transactional LIVE** NameLN per pair.  A snapshot taken
//! BEFORE a later committed DDL on that name (rename / remove / recreate) can
//! be WRITTEN AFTER that DDL's NameLN.  7.11.x recovery is a full
//! latest-NameLN-per-name replay, so a stale `(name, old_id)` landing at a
//! higher LSN than the committed rebind binds `name` to the stale old id — one
//! database's records surface under another name and the real database's
//! records are lost, SILENTLY (recovery returns Ok; `verify` passes).
//!
//! This witness forces the exact hazard interleaving deterministically via the
//! `#[doc(hidden)]` test seam
//! [`Environment::relog_live_catalog_with_snapshot_hook_for_test`]: the relog
//! takes its `name_map` snapshot, then the hook COMMITS a remove+recreate (new
//! id) / rename on the snapshotted name, then the relog appends.  Daemons are
//! OFF so the ONLY relog is the one the test drives — the result is
//! deterministic (no seeds).
//!
//! Oracle: after the forced relog + a reopen (full-scan recovery), the current
//! name must bind to its CURRENT committed id and records, never the stale id's
//! records.  RED on the pre-fix engine (relog re-logs the stale binding →
//! recovery resurrects the old id's tree); GREEN with the fix (the relog's
//! non-blocking READ name lock + re-read of `name_map` rejects the stale copy;
//! the DDL-resolution side publishes `name_map` before clearing the pending
//! mark, so the happens-before chain holds).
//!
//! The fix is the write-side JE-faithful mechanism: JE writes the NameLN +
//! MapLN for a DDL as one op under one locker and never re-logs a free-floating
//! live NameLN; its cleaner skips a NameLN write-locked by a pending DDL txn
//! (`FileProcessor.processFoundLN` nonBlockingLock DENIED → addPendingLN,
//! `FileProcessor.java:1353-1370`).

#![allow(clippy::unwrap_used)]

use noxu_db::{
    CheckpointConfig, DatabaseConfig, Environment, EnvironmentConfig,
};
use std::path::Path;
use tempfile::TempDir;

fn db_cfg() -> DatabaseConfig {
    DatabaseConfig::new().with_allow_create(true).with_transactional(true)
}

/// Daemons OFF so the test is the only driver of the catalog relog.
fn open_env(dir: &Path, create: bool) -> Environment {
    Environment::open(
        EnvironmentConfig::new(dir.to_path_buf())
            .with_allow_create(create)
            .with_transactional(true)
            .with_run_checkpointer(false)
            .with_run_cleaner(false)
            .with_run_evictor(false)
            .with_run_in_compressor(false)
            .with_run_verifier(false),
    )
    .unwrap()
}

/// Return the sorted record values of `name` as owned `Vec<u8>`s (empty if the
/// db is absent or empty).
fn records(env: &Environment, name: &str) -> Vec<Vec<u8>> {
    let cfg = DatabaseConfig::new().with_transactional(true);
    let Ok(db) = env.open_database(None, name, &cfg) else {
        return Vec::new();
    };
    let mut vals = Vec::new();
    if let Ok(mut cur) = db.open_cursor(None) {
        while let Ok(Some((_k, v))) = cur.next() {
            vals.push(v.as_ref().to_vec());
        }
    }
    db.close().ok();
    vals.sort();
    vals
}

/// XDB-711 core witness (removed-db resurrection): a transactional REMOVE of
/// `charlie` COMMITS inside the relog's snapshot→append window.  The relog
/// snapshot captured `charlie -> A`; a stale relog re-asserts that LIVE NameLN
/// at a higher LSN than the committed remove tombstone, so full-scan recovery
/// resurrects the removed database -- and because id A's tree holds charlie's
/// old records, they reappear (the XDB-711 §4 resurrection shape).  The removed
/// name must stay removed after recovery.
#[test]
fn relog_cannot_resurrect_a_removed_name_committed_in_the_window() {
    let dir = TempDir::new().unwrap();
    {
        let env = open_env(dir.path(), true);

        // Seed `charlie` (id A) with distinctive records + a second db so the
        // catalog has >1 entry to relog.
        let charlie = env.open_database(None, "charlie", &db_cfg()).unwrap();
        for r in 0u32..3 {
            let t = env.begin_transaction(None).unwrap();
            charlie
                .put_in(
                    &t,
                    r.to_be_bytes(),
                    format!("charlie-rec-{r}").as_bytes(),
                )
                .unwrap();
            t.commit().unwrap();
        }
        charlie.close().ok();

        let keep = env.open_database(None, "keep", &db_cfg()).unwrap();
        let t = env.begin_transaction(None).unwrap();
        keep.put_in(&t, 0u32.to_be_bytes(), b"keep-0").unwrap();
        t.commit().unwrap();
        keep.close().ok();

        env.checkpoint(Some(&CheckpointConfig::new().with_force(true))).ok();

        // Drive one catalog relog.  Snapshot sees `charlie -> A`; the hook then
        // REMOVES charlie (transactional, committed).  A buggy relog appends
        // the stale LIVE `charlie -> A` AFTER the committed remove tombstone,
        // so recovery's latest-NameLN-per-name rule resurrects the removed
        // database.
        let env_ref = &env;
        env.relog_live_catalog_with_snapshot_hook_for_test(&|| {
            let t = env_ref.begin_transaction(None).unwrap();
            env_ref.remove_database(Some(&t), "charlie").unwrap();
            t.commit().unwrap();
        });

        // A final checkpoint captures the current catalog durably.
        env.checkpoint(Some(&CheckpointConfig::new().with_force(true))).ok();
        env.close().ok();
    }

    // Reopen (full-scan recovery).
    let env = open_env(dir.path(), false);
    let names = env.database_names().unwrap();
    assert!(
        !names.contains(&"charlie".to_string()),
        "XDB-711: committed-removed 'charlie' resurrected after recovery; \
         names={names:?}"
    );
    // charlie's old records must NOT surface under any surviving name.
    for name in &names {
        for v in records(&env, name) {
            assert!(
                !v.starts_with(b"charlie-rec"),
                "XDB-711: removed db's records resurfaced under {name:?}: {:?}",
                String::from_utf8_lossy(&v)
            );
        }
    }
    assert_eq!(records(&env, "keep"), vec![b"keep-0".to_vec()]);
    env.close().ok();
}

/// XDB-711 rename variant: in the relog window, RENAME charlie→charlie2
/// (committed), so the snapshot's `charlie -> A` is a now-removed name and
/// `charlie2 -> A` is the live one.  A buggy relog re-asserts the removed
/// `charlie -> A` live, resurrecting the old name after recovery.
#[test]
fn relog_cannot_resurrect_a_renamed_away_old_name() {
    let dir = TempDir::new().unwrap();
    {
        let env = open_env(dir.path(), true);
        let charlie = env.open_database(None, "charlie", &db_cfg()).unwrap();
        for r in 0u32..3 {
            let t = env.begin_transaction(None).unwrap();
            charlie
                .put_in(&t, r.to_be_bytes(), format!("data-{r}").as_bytes())
                .unwrap();
            t.commit().unwrap();
        }
        charlie.close().ok();
        env.checkpoint(Some(&CheckpointConfig::new().with_force(true))).ok();

        let env_ref = &env;
        env.relog_live_catalog_with_snapshot_hook_for_test(&|| {
            let t = env_ref.begin_transaction(None).unwrap();
            env_ref.rename_database(Some(&t), "charlie", "charlie2").unwrap();
            t.commit().unwrap();
        });

        env.checkpoint(Some(&CheckpointConfig::new().with_force(true))).ok();
        env.close().ok();
    }

    let env = open_env(dir.path(), false);
    let names = env.database_names().unwrap();
    assert!(
        !names.contains(&"charlie".to_string()),
        "XDB-711: renamed-away 'charlie' resurrected after recovery; names={names:?}"
    );
    assert!(
        names.contains(&"charlie2".to_string()),
        "XDB-711: rename target 'charlie2' missing after recovery; names={names:?}"
    );
    // charlie2 keeps the renamed db's records.
    let got = records(&env, "charlie2");
    assert_eq!(
        got,
        vec![b"data-0".to_vec(), b"data-1".to_vec(), b"data-2".to_vec()],
        "XDB-711: renamed db lost its records; got={:?}",
        strs(&got)
    );
    env.close().ok();
}

fn strs(vs: &[Vec<u8>]) -> Vec<String> {
    vs.iter().map(|v| String::from_utf8_lossy(v).into_owned()).collect()
}
