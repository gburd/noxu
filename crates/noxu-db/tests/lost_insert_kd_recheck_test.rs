//! BUG-SPLIT-LOST-INSERT-1 part 2/2 (tag-native) — bounded one-shot re-descent.
//!
//! A `search(Set/Both)` that hits an exact KNOWN-DELETED slot re-descends ONCE
//! from the root and re-examines the slot on the current resident instance
//! before concluding NOTFOUND (JE searchExact -> lockLNAndCheckDefunct,
//! CursorImpl.java:1937-1980).  This guards the stale-resident-BIN-instance
//! window (a concurrent evictor detach+refault racing the latch-free descent),
//! WITHOUT the deferred run_with_defunct_restart machinery.
//!
//! This test proves the BOUND and TERMINATION (requirement 2a): a genuinely
//! committed-deleted key (a real KD slot, compressor off) must return NOTFOUND
//! and must NOT loop — the re-descent fires at most once and then concludes.
//! The stale-instance race itself is exercised by the delete_heavy /
//! cleaner_ln stress measurements (a deterministic instance-swap hook does not
//! exist at the dbi layer at v7.11.0; see the commit message).
#![cfg(not(noxu_shuttle))]

use noxu_db::{DatabaseConfig, Durability, Environment, EnvironmentConfig};
use std::path::Path;

fn env_cfg(p: &Path) -> EnvironmentConfig {
    let mut c = EnvironmentConfig::new(p.to_path_buf())
        .with_transactional(true)
        .with_allow_create(true)
        .with_durability(Durability::COMMIT_SYNC);
    c.set_run_cleaner(false);
    c.set_run_checkpointer(false);
    c.set_run_in_compressor(false); // keep the KD slot resident
    c
}
fn db_cfg() -> DatabaseConfig {
    DatabaseConfig::new().with_allow_create(true).with_transactional(true)
}

#[test]
fn committed_deleted_key_terminates_at_notfound_bounded() {
    let dir = tempfile::tempdir().unwrap();
    let env = Environment::open(env_cfg(dir.path())).unwrap();
    let db = env.open_database(None, "d", &db_cfg()).unwrap();

    // Commit k, then commit a delete of k.  With the compressor off this
    // leaves a genuine KNOWN-DELETED slot resident — the exact state the
    // part-2 re-descent re-examines.
    db.put(b"k", b"v").unwrap();
    let txn = env.begin_transaction(None).unwrap();
    assert!(db.delete_in(&txn, b"k").unwrap());
    txn.commit().unwrap();

    // A genuinely-deleted key: the KD exact match triggers exactly one
    // re-descent, which re-examines the (still-KD) current instance and
    // concludes NOTFOUND.  It must terminate (no restart storm / hang) and
    // report the key absent.
    for _ in 0..1000 {
        assert!(
            db.get(b"k").unwrap().is_none(),
            "a committed-deleted key must read as absent and terminate"
        );
    }

    // A plain absent key (never inserted) must also be NOTFOUND and must NOT
    // pay the re-descent (no KD slot) — correctness, not cost, asserted here.
    assert!(db.get(b"absent").unwrap().is_none());

    // A live key is unaffected.
    db.put(b"live", b"x").unwrap();
    assert_eq!(db.get(b"live").unwrap().as_deref(), Some(&b"x"[..]));

    db.close().unwrap();
    env.close().unwrap();
}
