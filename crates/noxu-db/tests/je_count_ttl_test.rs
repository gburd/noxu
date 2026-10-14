//! Witness for A19 — `Database::count()` must exclude TTL-expired records.
//!
//! Port of the observable core of JE `Database.count` under TTL (its BIN scan
//! sees only live slots).  Before the fix, Noxu `count()` returned the O(1)
//! `entry_count` atomic, which has no decrement event for a time-based expiry,
//! so an expired record was still counted.  After the fix `count()` walks the
//! tree filtering TTL-expired slots via the shared `slot_is_live` predicate.
//!
//! Uses the TTL test-time hook (`noxu_util::ttl::set_test_time_ms`).

#![allow(clippy::unwrap_used, clippy::expect_used)]

use noxu_db::{DatabaseConfig, EnvironmentConfig, TtlUnit, WriteOptions};
use noxu_util::ttl::set_test_time_ms;
use tempfile::TempDir;

const BASE_MS: u64 = 1_454_472_306_000;
const MILLIS_PER_HOUR: u64 = 3600 * 1000;

fn open_env(dir: &TempDir) -> noxu_db::Environment {
    let env_cfg = EnvironmentConfig::new(dir.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(true)
        .with_run_evictor(false)
        .with_run_cleaner(false)
        .with_run_checkpointer(false)
        .with_run_in_compressor(false);
    noxu_db::Environment::open(env_cfg).unwrap()
}

fn int_key(k: u32) -> [u8; 4] {
    k.to_be_bytes()
}

/// Count must drop as records expire: 10 records with a 1-hour TTL plus 5 with
/// no TTL => count 15 while live; after advancing past the TTL => count 5.
#[test]
fn count_excludes_ttl_expired() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let db = env
        .open_database(
            None,
            "foo",
            &DatabaseConfig::new().with_allow_create(true),
        )
        .unwrap();

    set_test_time_ms(BASE_MS);

    let ttl = WriteOptions::new()
        .with_ttl_unit(1, TtlUnit::Hours)
        .with_update_ttl(true);

    // 10 records expiring in 1 hour.
    for i in 0..10u32 {
        db.put_with_options(None, int_key(i), [0u8; 1], &ttl).unwrap();
    }
    // 5 records with no TTL (keys 100..105).
    for i in 100..105u32 {
        db.put(int_key(i), [0u8; 1]).unwrap();
    }

    assert_eq!(
        db.count().unwrap(),
        15,
        "all 15 records are live immediately after insert"
    );

    // Advance past the 1-hour TTL.
    set_test_time_ms(BASE_MS + 3 * MILLIS_PER_HOUR);

    assert_eq!(
        db.count().unwrap(),
        5,
        "count() must exclude the 10 TTL-expired records, leaving the 5 \
         never-expiring records (JE Database.count sees only live slots)"
    );

    set_test_time_ms(0);
}
