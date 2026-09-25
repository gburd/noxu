//! NEW-2 — a database's configured `NODE_MAX_ENTRIES` (fanout) must survive
//! a close/reopen cycle.
//!
//! Before the fix, recovery seeded and transplanted a hard-coded
//! `Tree::new(db_id, 256)` (environment_impl.rs), so the reopened tree always
//! had fanout 256 regardless of the configured `NODE_MAX_ENTRIES`.  A database
//! opened with a small fanout (e.g. 4) would behave with fanout 256 after
//! restart: wrong structure, wrong split geometry.  This also made
//! `forced_split_recovery_test` vacuous (a NODE_MAX=4 tree recovered at 256
//! and never split with 40 keys) and blocked a GAP B crash-recovery test that
//! needs a genuinely multi-level tree.
//!
//! JE reference: `DatabaseImpl` serializes `maxTreeEntriesPerNode` in its log
//! record (`DatabaseImpl.writeToLog`, DatabaseImpl.java:2134; read back at
//! DatabaseImpl.java:2203) and, for a newly created database whose field is 0,
//! falls back to the environment-level `NODE_MAX` default
//! (DatabaseImpl.java:420).  On recovery the `DatabaseImpl` is reconstituted
//! with its fanout, so the tree's split geometry is preserved across restart.
//!
//! In Noxu the per-DB fanout is carried by `DatabaseImpl` (derived from the
//! `DatabaseConfig` supplied at open, falling back to the env-level
//! `NODE_MAX_ENTRIES`, exactly like JE).  The recovered tree must inherit that
//! fanout instead of the hard-coded 256.
//!
//! Each test inserts enough ascending keys to force many BIN splits at
//! fanout 4 (a multi-level tree with `bottom_internal_node_count >= 2`), then
//! closes and reopens, and asserts the reopened tree STILL splits at fanout 4
//! — i.e. its structure reflects the small fanout, not 256.

use noxu_db::{
    DatabaseConfig, DatabaseEntry, EnvironmentConfig, StatsConfig,
};
use std::path::Path;
use tempfile::TempDir;

const NODE_MAX: u32 = 4;
// 40 keys at fanout 4 must produce many BINs; at fanout 256 it produces one.
const N: u32 = 40;

fn open_env(dir: &Path, env_node_max: u32) -> noxu_db::Environment {
    let mut cfg = EnvironmentConfig::new(dir.to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    cfg.set_run_cleaner(false);
    cfg.set_run_checkpointer(false);
    cfg.set_run_evictor(false);
    cfg.set_run_in_compressor(false);
    cfg.set_node_max_entries(env_node_max);
    noxu_db::Environment::open(cfg).unwrap()
}

fn ikey(i: u32) -> String {
    format!("k{i:08}")
}

fn bin_and_in_count(db: &noxu_db::Database) -> (u64, u64) {
    let s = db.stats(Some(&StatsConfig::new().with_fast(false))).unwrap();
    (
        s.btree.bottom_internal_node_count,
        s.btree.internal_node_count,
    )
}

/// Per-DB `DatabaseConfig::node_max_entries` must survive reopen.
#[test]
fn db_config_node_max_survives_reopen() {
    let dir = TempDir::new().unwrap();
    let db_cfg = || {
        DatabaseConfig::new()
            .with_allow_create(true)
            .with_node_max_entries(NODE_MAX)
    };

    // Build a genuinely multi-level tree at fanout 4.
    {
        let env = open_env(dir.path(), 256); // env default large; DB overrides
        let db = env.open_database(None, "d", &db_cfg()).unwrap();
        for i in 0..N {
            let k = ikey(i);
            db.put(
                DatabaseEntry::from_bytes(k.as_bytes()),
                DatabaseEntry::from_bytes(k.as_bytes()),
            )
            .unwrap();
        }
        let (bins, ins) = bin_and_in_count(&db);
        assert!(
            bins >= 2,
            "pre-reopen: fanout-{NODE_MAX} tree with {N} keys must have \
             multiple BINs, got bin_count={bins} in_count={ins} \
             (would be 1 at fanout 256)"
        );
        db.close().unwrap();
        env.close().unwrap();
    }

    // Reopen (recover) and assert the tree STILL reflects fanout 4.
    let env = open_env(dir.path(), 256);
    let db = env.open_database(None, "d", &db_cfg()).unwrap();
    let (bins, ins) = bin_and_in_count(&db);
    assert!(
        bins >= 2,
        "post-reopen: fanout-{NODE_MAX} tree must STILL split at {NODE_MAX} \
         (bin_count>=2), got bin_count={bins} in_count={ins}; recovery \
         reconstructed the tree at the wrong fanout (256), losing the \
         configured NODE_MAX_ENTRIES"
    );

    // Also confirm inserts still split at 4, not 256: adding a handful more
    // keys must add more BINs.
    let (bins_before, _) = bin_and_in_count(&db);
    for i in N..(N + 8) {
        let k = ikey(i);
        db.put(
            DatabaseEntry::from_bytes(k.as_bytes()),
            DatabaseEntry::from_bytes(k.as_bytes()),
        )
        .unwrap();
    }
    let (bins_after, _) = bin_and_in_count(&db);
    assert!(
        bins_after > bins_before,
        "post-reopen inserts must keep splitting at fanout {NODE_MAX} \
         (bins {bins_before} -> {bins_after}); a fanout-256 tree would not \
         split with so few keys"
    );
}

/// Env-level `NODE_MAX_ENTRIES` (used when the DB does not override it) must
/// survive reopen — mirrors JE's env-default fallback (DatabaseImpl.java:420).
#[test]
fn env_config_node_max_survives_reopen() {
    let dir = TempDir::new().unwrap();
    let db_cfg = || DatabaseConfig::new().with_allow_create(true);

    {
        let env = open_env(dir.path(), NODE_MAX); // env-level fanout 4
        let db = env.open_database(None, "d", &db_cfg()).unwrap();
        for i in 0..N {
            let k = ikey(i);
            db.put(
                DatabaseEntry::from_bytes(k.as_bytes()),
                DatabaseEntry::from_bytes(k.as_bytes()),
            )
            .unwrap();
        }
        let (bins, ins) = bin_and_in_count(&db);
        assert!(
            bins >= 2,
            "pre-reopen: env fanout-{NODE_MAX} tree with {N} keys must have \
             multiple BINs, got bin_count={bins} in_count={ins}"
        );
        db.close().unwrap();
        env.close().unwrap();
    }

    let env = open_env(dir.path(), NODE_MAX);
    let db = env.open_database(None, "d", &db_cfg()).unwrap();
    let (bins, ins) = bin_and_in_count(&db);
    assert!(
        bins >= 2,
        "post-reopen: env fanout-{NODE_MAX} tree must STILL split at \
         {NODE_MAX} (bin_count>=2), got bin_count={bins} in_count={ins}"
    );
}
