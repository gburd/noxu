//! Integration tests for JE 7.5 TTL / record expiration, exercised through
//! the public `noxu-db` API.
//!
//! Coverage:
//!   1. put-with-TTL round-trip: a record written with a future TTL is
//!      readable (found) before it expires.
//!   2. no-TTL put is unaffected (regression guard for the threaded
//!      expiration path).
//!   3. day-granularity TTL (JE `WriteOptions.setTTL(ttl, DAYS)`).
//!   4. TTL survives close+reopen (the expiration is carried in the LN log
//!      entry and replayed by recovery — audit finding F8, now fixed).
//!
//! The "read-after-expiry returns NOTFOUND", master-switch, clock-tolerance,
//! and recovery-preserves-the-expiration-value behaviors are unit-tested at
//! the layers that can control the packed expiration directly
//! (`noxu-tree::tree` slot filtering, `noxu-recovery::recovery_manager` redo),
//! since the public TTL API produces future-only expirations and a fast test
//! cannot wait an hour for a record to expire.
//!
//! References:
//!   - JE `WriteOptions.setTTL` / `ExpirationInfo` (put path).
//!   - JE `LNLogEntry.getExpiration` / `RecoveryManager.redo` (recovery).

#![allow(clippy::unwrap_used, clippy::expect_used)]

use noxu_db::{DatabaseConfig, EnvironmentConfig, TtlUnit, WriteOptions};
use tempfile::TempDir;

fn open(dir: &TempDir) -> (noxu_db::Environment, noxu_db::Database) {
    let env_cfg = EnvironmentConfig::new(dir.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    let env = noxu_db::Environment::open(env_cfg).unwrap();
    let db_cfg = DatabaseConfig::new().with_allow_create(true);
    let db = env.open_database(None, "ttl", &db_cfg).unwrap();
    (env, db)
}

#[test]
fn put_with_ttl_is_found_before_expiry() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open(&dir);

    // 1000-hour TTL: far in the future, so the record is live.
    let opts = WriteOptions::with_expiration(1_000);
    db.put_with_options(None, b"k", b"v", &opts).unwrap();

    let got = db.get(b"k").unwrap();
    assert_eq!(
        got.as_deref(),
        Some(&b"v"[..]),
        "TTL record must be found before expiry"
    );
}

#[test]
fn no_ttl_put_still_works() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open(&dir);

    // A plain put (no options) and an options-put with ttl=0 both store a
    // never-expiring record.
    db.put(b"a", b"1").unwrap();
    db.put_with_options(None, b"b", b"2", &WriteOptions::new()).unwrap();

    assert_eq!(db.get(b"a").unwrap().as_deref(), Some(&b"1"[..]));
    assert_eq!(db.get(b"b").unwrap().as_deref(), Some(&b"2"[..]));
}

#[test]
fn day_granularity_ttl_round_trips() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open(&dir);

    // JE recommends DAYS to minimize per-slot storage.  A 30-day TTL is live.
    let opts = WriteOptions::new().with_ttl_unit(30, TtlUnit::Days);
    db.put_with_options(None, b"day", b"val", &opts).unwrap();
    assert_eq!(db.get(b"day").unwrap().as_deref(), Some(&b"val"[..]));
}

#[test]
fn ttl_record_survives_close_and_reopen() {
    let dir = TempDir::new().unwrap();

    // Phase 1: write TTL records, then close (exit checkpoint + WAL fsync).
    {
        let (env, db) = open(&dir);
        let opts = WriteOptions::with_expiration(5_000);
        for i in 0u32..50 {
            let key = i.to_be_bytes();
            db.put_with_options(None, key, b"payload", &opts).unwrap();
        }
        drop(db);
        drop(env);
    }

    // Phase 2: reopen and confirm every record survived recovery.  The LN
    // log entries carried the expiration (5000 h, still in the future), so
    // the records are both durable AND non-expired after replay.
    {
        let (_env, db) = open(&dir);
        for i in 0u32..50 {
            let key = i.to_be_bytes();
            let got = db.get(key).unwrap();
            assert_eq!(
                got.as_deref(),
                Some(&b"payload"[..]),
                "TTL record {i} lost after close+reopen (recovery)"
            );
        }
    }
}
