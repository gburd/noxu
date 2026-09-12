//! Overwriting a record through the real API must make its prior version
//! visible to the cleaner as obsolete.
//!
//! `Txn::count_obsolete_abort_lsns` used to skip counting whenever
//! `abort_data.is_some()`, copying JE's `maybeCountObsoleteLSN` gate
//! (`Txn.java:1096`). In JE that test is a valid proxy for "the LN was embedded
//! in its parent BIN, so it was already counted obsolete at logging time",
//! because JE assigns `abortData` only inside `if (bin.isEmbeddedLN(idx))`
//! (`CursorImpl.java:3328`).
//!
//! Noxu populates `abort_data` on EVERY overwrite — the in-memory undo path
//! needs the before-image unconditionally — so the proxy was always true and
//! obsolete-counting was suppressed for every transactional overwrite. The
//! cleaner's `UtilizationTracker` therefore never learned about any real garbage:
//! every file looked ~100 % utilized, and the daemon (`force=false`) never
//! selected a file no matter how `min_utilization` was set.
//!
//! These tests drive `Database::put` — the path every real caller takes — rather
//! than `CursorImpl::with_log_manager`. The existing obsolete-counting tests use
//! the latter and bypass the `Txn`-attached path entirely, which is why this went
//! unnoticed.
//!
//! Counting is now on by DEFAULT
//! (`docs/src/internal/space-amplification-2026-09.md` Phase 4): the
//! `NOXU_COUNT_AUTOCOMMIT_OBSOLETE` / `NOXU_COUNT_TXN_OBSOLETE` gates these
//! tests used to set are gone, so they now exercise the shipped default path
//! directly.

use noxu_db::{DatabaseConfig, Environment, EnvironmentConfig};
use tempfile::TempDir;

/// Repeatedly overwriting one key must drive utilization DOWN.
#[test]
fn overwrites_through_the_public_api_are_counted_obsolete() {
    let tmp = TempDir::new().unwrap();
    let value = vec![0x5Au8; 512];

    let mut cfg = EnvironmentConfig::new(tmp.path().to_path_buf())
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
            "util",
            &DatabaseConfig::new().with_allow_create(true),
        )
        .unwrap();

    // One live record, overwritten many times. Every overwrite but the last
    // leaves a dead LN version, so the log is overwhelmingly garbage.
    let key = b"the-one-key";
    for _ in 0..600 {
        db.put(key, &value).unwrap();
    }
    env.checkpoint(None).unwrap();
    // Cleaning refreshes the utilization stats from the tracker.
    env.clean_log().unwrap();

    let stats = env.stats().unwrap();
    let util = stats.cleaner.max_utilization;

    // With ~600 versions of one 512-byte record and only one live, utilization
    // must be far below 100%. Before the fix the tracker counted no LN obsolete
    // at all and this reported ~100.
    assert!(
        util > 0,
        "utilization must be published at all (0 means the stat is unwired)"
    );
    assert!(
        util < 60,
        "max_utilization is {util}%, but only 1 of ~600 logged versions of this \
         record is live -- the prior versions are not being counted obsolete. \
         This is the abort_data-as-embedded-proxy bug: every transactional \
         overwrite's prior LN is skipped in count_obsolete_abort_lsns, so the \
         cleaner cannot see the garbage and its daemon never selects a file."
    );

    drop(db);
    env.close().unwrap();
}

/// A single write of each key must NOT look obsolete — guards over-counting.
#[test]
fn distinct_keys_written_once_are_not_counted_obsolete() {
    let tmp = TempDir::new().unwrap();
    let value = vec![0x33u8; 512];

    let mut cfg = EnvironmentConfig::new(tmp.path().to_path_buf())
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
            "util2",
            &DatabaseConfig::new().with_allow_create(true),
        )
        .unwrap();

    // Every record written exactly once: all data is live, nothing obsolete.
    for i in 0..600u32 {
        db.put(format!("k{i:06}").as_bytes(), &value).unwrap();
    }
    env.checkpoint(None).unwrap();
    env.clean_log().unwrap();

    let stats = env.stats().unwrap();
    let util = stats.cleaner.min_utilization;
    assert!(
        util > 50,
        "min_utilization is {util}% but every record was written exactly once, \
         so almost nothing is obsolete -- a low value here means the fix \
         OVER-counts, marking live versions obsolete (which would let the \
         cleaner discard live data)"
    );

    drop(db);
    env.close().unwrap();
}
