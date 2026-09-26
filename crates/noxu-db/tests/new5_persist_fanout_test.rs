//! NEW-5 — a database's per-DB fanout (`NODE_MAX_ENTRIES` /
//! `maxTreeEntriesPerNode`) must be PERSISTED in the on-disk database record
//! and restored on reopen even when the caller does NOT re-supply a
//! `DatabaseConfig` carrying that fanout.
//!
//! NEW-2 (merged) fixed the case where the caller re-supplies the
//! `DatabaseConfig` at open: the recovered tree inherits the configured
//! fanout instead of a hard-coded 256.  The remaining gap NEW-5 closes: on a
//! DEFAULT-config reopen (caller passes a `DatabaseConfig` with
//! `node_max_entries == 0`, so the effective fanout falls back to the
//! ENV-level `NODE_MAX_ENTRIES`), the originally-configured per-DB fanout was
//! lost — the DB reverted to the env default.
//!
//! JE reference: `DatabaseImpl.writeToLog` serializes `maxTreeEntriesPerNode`
//! (DatabaseImpl.java:2134); `readFromLog` reads it back (DatabaseImpl.java:2203)
//! and only falls back to the env-level `NODE_MAX` default when the persisted
//! field is zero (DatabaseImpl.java:420).  Noxu persists the fanout in the
//! NameLN data trailer (alongside the DBI-14 comparator identities) and, on
//! reopen, applies it as the effective fanout when the caller did not supply a
//! non-default per-DB fanout.
//!
//! The tests create a DB with a small NON-default fanout while the env's
//! NODE_MAX is large, insert enough keys to force many BIN splits at the small
//! fanout, close, then reopen with a DEFAULT `DatabaseConfig` (no
//! `with_node_max_entries`) — and assert the reopened tree STILL splits at the
//! configured small fanout, not the env default.

use noxu_db::{DatabaseConfig, DatabaseEntry, EnvironmentConfig, StatsConfig};
use std::path::Path;
use tempfile::TempDir;

const DB_NODE_MAX: u32 = 4;
// Env default fanout, deliberately large so a fanout-256 tree of N keys is a
// single BIN.
const ENV_NODE_MAX: u32 = 256;
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

fn bin_count(db: &noxu_db::Database) -> u64 {
    let s = db.stats(Some(&StatsConfig::new().with_fast(false))).unwrap();
    s.btree.bottom_internal_node_count
}

/// Per-DB fanout must survive a DEFAULT-config reopen (NEW-5).
///
/// Create at fanout 4 with the env default at 256, close, then reopen passing
/// a DEFAULT `DatabaseConfig` (node_max_entries == 0).  Under NEW-2 alone the
/// effective fanout would resolve to the env-level 256 and the tree would
/// collapse to a single BIN on the recovered path; NEW-5 restores the
/// persisted fanout 4.
#[test]
fn per_db_fanout_persisted_across_default_reopen() {
    let dir = TempDir::new().unwrap();

    // Create with an EXPLICIT small per-DB fanout while the env default is
    // large, and force a genuinely multi-level tree.
    {
        let env = open_env(dir.path(), ENV_NODE_MAX);
        let db_cfg = DatabaseConfig::new()
            .with_allow_create(true)
            .with_node_max_entries(DB_NODE_MAX);
        let db = env.open_database(None, "d", &db_cfg).unwrap();
        for i in 0..N {
            let k = ikey(i);
            db.put(
                DatabaseEntry::from_bytes(k.as_bytes()),
                DatabaseEntry::from_bytes(k.as_bytes()),
            )
            .unwrap();
        }
        let bins = bin_count(&db);
        assert!(
            bins >= 2,
            "pre-reopen: fanout-{DB_NODE_MAX} tree with {N} keys must have \
             multiple BINs, got bin_count={bins} (would be 1 at fanout 256)"
        );
        db.close().unwrap();
        env.close().unwrap();
    }

    // Reopen WITHOUT re-supplying the per-DB fanout: a DEFAULT DatabaseConfig
    // (node_max_entries == 0).  The env default is still 256.  If the fanout
    // were not persisted, the effective fanout would be 256.
    let env = open_env(dir.path(), ENV_NODE_MAX);
    let db_cfg = DatabaseConfig::new().with_allow_create(true);
    // Sanity: this config carries NO explicit fanout.
    assert_eq!(db_cfg.node_max_entries, 0, "test setup: default config");
    let db = env.open_database(None, "d", &db_cfg).unwrap();

    let bins = bin_count(&db);
    assert!(
        bins >= 2,
        "post-reopen (DEFAULT config): fanout-{DB_NODE_MAX} tree must STILL \
         split at {DB_NODE_MAX} (bin_count>=2), got bin_count={bins}; the \
         persisted per-DB NODE_MAX_ENTRIES was lost and the DB reverted to \
         the env default ({ENV_NODE_MAX})"
    );

    // Inserting a handful more keys must keep splitting at fanout 4, not 256.
    let before = bin_count(&db);
    for i in N..(N + 8) {
        let k = ikey(i);
        db.put(
            DatabaseEntry::from_bytes(k.as_bytes()),
            DatabaseEntry::from_bytes(k.as_bytes()),
        )
        .unwrap();
    }
    let after = bin_count(&db);
    assert!(
        after > before,
        "post-reopen inserts must keep splitting at fanout {DB_NODE_MAX} \
         (bins {before} -> {after}); a fanout-{ENV_NODE_MAX} tree would not \
         split with so few keys"
    );

    db.close().unwrap();
    env.close().unwrap();
}
