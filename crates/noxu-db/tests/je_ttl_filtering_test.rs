//! Port of JE `com.sleepycat.je.test.TTLTest` methods that need a controllable
//! clock to age records past their expiration mid-test.
//!
//! Enabled by the TTL test-time hook (`noxu_util::ttl::set_test_time_ms` /
//! `advance_test_time_ms`), the faithful analog of JE's static
//! `TTL.setTimeTestHook` / `fixedSystemTime` (TTL.java:124-162 /
//! TTLTest.java:1858 `setFixedTimeHook`).  JE routes every expiry decision
//! through `TTL.currentSystemTime`; Noxu routes them through
//! `noxu_util::ttl::current_system_time_ms`, which honors the hook.
//!
//! These tests mutate a process-global clock, so they run serially within this
//! file (one `#[test]` each, each installs then clears the hook) — the hook is
//! cleared at the end of every test, matching JE's tearDown
//! (`TTL.setTimeTestHook(null); fixedSystemTime = 0`).

#![allow(clippy::unwrap_used, clippy::expect_used)]

use noxu_db::{DatabaseConfig, EnvironmentConfig, TtlUnit, WriteOptions};
use noxu_util::ttl::{MILLIS_PER_DAY, advance_test_time_ms, set_test_time_ms};
use tempfile::TempDir;

/// JE `TTLTest.BASE_MS`: 2016-02-03 04:05:06 UTC, in ms since the epoch.
/// A fixed, far-from-epoch time so the clock-cleared sentinel (0) is safe.
const BASE_MS: u64 = 1_454_472_306_000;

fn open_env(dir: &TempDir) -> noxu_db::Environment {
    // JE TTLTest turns off all daemons so filtering (not compression/cleaning)
    // is what makes expired records disappear.
    let env_cfg = EnvironmentConfig::new(dir.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(true)
        .with_run_evictor(false)
        .with_run_cleaner(false)
        .with_run_checkpointer(false)
        .with_run_in_compressor(false);
    noxu_db::Environment::open(env_cfg).unwrap()
}

fn open_db(env: &noxu_db::Environment) -> noxu_db::Database {
    let db_cfg = DatabaseConfig::new().with_allow_create(true);
    env.open_database(None, "foo", &db_cfg).unwrap()
}

/// 4-byte big-endian key, matching JE `IntegerBinding.intToEntry` ordering so
/// a disk-ordered / range scan sees keys in numeric order.
fn int_key(k: i32) -> [u8; 4] {
    (k as u32).to_be_bytes()
}

/// JE: `write(key, ttl, DAYS)` with `updateTTL(true)`; data = the ttl value.
/// Returns nothing (JE returns the stored expiration time; we only assert
/// read-visibility, which is the observable contract).
fn write_ttl(db: &noxu_db::Database, key: i32, ttl_days: u64) {
    let opts = WriteOptions::new()
        .with_ttl_unit(ttl_days, TtlUnit::Days)
        .with_update_ttl(true);
    db.put_with_options(None, int_key(key), [ttl_days as u8], &opts).unwrap();
}

/// JE `readExpectFound` / `readExpectNotFound`: a plain `db.get` either finds
/// the record or sees it filtered as expired.
fn expect_found(db: &noxu_db::Database, key: i32) {
    assert!(
        db.get(int_key(key)).unwrap().is_some(),
        "key {key} must be found (live)"
    );
}

fn expect_not_found(db: &noxu_db::Database, key: i32) {
    assert!(
        db.get(int_key(key)).unwrap().is_none(),
        "key {key} must read NOTFOUND (expired/filtered)"
    );
}

/// JE `TTLTest.testFiltering` (TTLTest.java:611).
///
/// Insert 11 records with a day-TTL equal to their key (record 0 has no TTL).
/// Advancing the clock one day at a time expires one slot at a time, starting
/// from record 1.  No compression runs, but expired records are filtered out
/// of queries.  JE additionally checks `BIN.getNEntries() == 11` (no
/// compression), `preload` LN counts and a `DiskOrderedScan`; those use
/// `DbInternal`/BIN internals.  The portable, observable core is: after each
/// one-day advance, every expired key reads NOTFOUND and every live key reads
/// SUCCESS, and a full forward scan returns exactly the live set.
#[test]
fn ttl_test_test_filtering() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let db = open_db(&env);

    set_test_time_ms(BASE_MS);

    // Insert 11 records; record i expires i days from now (0 = never).
    for i in 0..=10 {
        write_ttl(&db, i, i as u64);
    }

    // Everything is live immediately after insert.
    for j in 0..=10 {
        expect_found(&db, j);
    }

    // Advance one day at a time. After the advance for i, record i has expired
    // (except record 0, which never expires).
    let mut live: Vec<i32> = (0..=10).collect();
    for i in 0..=10 {
        advance_test_time_ms(MILLIS_PER_DAY);

        if i != 0 {
            live.retain(|&k| k != i);
        }

        for j in 0..=10 {
            if live.contains(&j) {
                expect_found(&db, j);
            } else {
                expect_not_found(&db, j);
            }
        }

        // A full forward scan returns exactly the live set (JE diskOrderedScan
        // equivalent via a plain cursor, which also filters expired slots).
        let mut scanned: Vec<i32> = Vec::new();
        let mut cursor = db.open_cursor(None).unwrap();
        while let Some((k, _v)) = cursor.next().unwrap() {
            let mut arr = [0u8; 4];
            arr.copy_from_slice(&k[..4]);
            scanned.push(u32::from_be_bytes(arr) as i32);
        }
        drop(cursor);
        scanned.sort_unstable();
        let mut expected = live.clone();
        expected.sort_unstable();
        assert_eq!(
            scanned, expected,
            "after advancing to day {i}, scan must return exactly the live set"
        );
    }

    // At the end only record 0 (no TTL) survives.
    assert_eq!(live, vec![0]);

    // Clear the hook (JE tearDown: TTL.setTimeTestHook(null)).
    set_test_time_ms(0);
}
