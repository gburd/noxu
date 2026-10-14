//! Port of JE `com.sleepycat.je.test.TTLTest.testRepeatableRead` /
//! `testSecondaryRepeatableRead` (TTLTest.java:1062, :1211) — observable core.
//!
//! JE rule (CursorImpl.lockLN, CursorImpl.java:3573-3587): when a locker
//! `ownsOrSharesLock(lsn)` AND the record `expiresWithin(TTL_MAX_TXN_TIME)`,
//! the record is treated as "locked and expires soon" and its expiry is
//! suppressed (`defunct = isDeleted`, not `isDefunct`).  So a txn that already
//! read (locked) a record keeps seeing it even after it expires mid-txn
//! (repeatable read); a different txn / autocommit read after expiry must get
//! NOTFOUND.
//!
//! Uses the TTL test-time hook (`noxu_util::ttl::set_test_time_ms`), the
//! faithful analog of JE `TTL.setTimeTestHook` / `fixedSystemTime`.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use noxu_db::{DatabaseConfig, EnvironmentConfig, TtlUnit, WriteOptions};
use noxu_util::ttl::set_test_time_ms;
use tempfile::TempDir;

/// JE `TTLTest.BASE_MS`: 2016-02-03 04:05:06 UTC.
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

fn open_db(env: &noxu_db::Environment) -> noxu_db::Database {
    let db_cfg =
        DatabaseConfig::new().with_allow_create(true).with_transactional(true);
    env.open_database(None, "foo", &db_cfg).unwrap()
}

/// JE `TTLTest.testRepeatableRead` first part (TTLTest.java:1062-1111):
/// a transaction that reads (locks) a record keeps seeing it after it expires
/// mid-txn, but a *different* transaction sees it as NOTFOUND.
#[test]
fn ttl_test_test_repeatable_read() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let db = open_db(&env);

    set_test_time_ms(BASE_MS);
    let key = (123u32).to_be_bytes();

    // Write a record expiring in 1 hour and READ it within a txn (locks it).
    let txn = env.begin_transaction(None).unwrap();
    db.put_with_options(
        Some(&txn),
        key,
        [0u8; 1],
        &WriteOptions::new()
            .with_ttl_unit(1, TtlUnit::Hours)
            .with_update_ttl(true),
    )
    .unwrap();
    assert!(
        db.get_in(&txn, key).unwrap().is_some(),
        "record must be found when first read (live)"
    );

    // Advance time well past the 1-hour expiry (still within TTL_MAX_TXN_TIME,
    // JE default 24h). JE: `fixedSystemTime = result.getExpirationTime() + 1`.
    set_test_time_ms(BASE_MS + 3 * MILLIS_PER_HOUR);

    // Repeatable read: the SAME txn, which already locked the record, must
    // still see it even though it is now expired (JE lockLN
    // wasLockedAndExpiresSoon => expiry suppressed for the lock owner).
    assert!(
        db.get_in(&txn, key).unwrap().is_some(),
        "repeatable read: a txn that already locked the record must still \
         see it after it expires mid-txn (JE CursorImpl.lockLN:3573-3587)"
    );
    txn.commit().unwrap();

    // A DIFFERENT transaction does NOT own the lock, so the expired record is
    // filtered: NOTFOUND (JE: `assertNull(db.get(txn, ...))` with a fresh txn).
    let txn2 = env.begin_transaction(None).unwrap();
    assert!(
        db.get_in(&txn2, key).unwrap().is_none(),
        "a different txn (not the lock owner) must see the expired record as \
         NOTFOUND — expiry must NOT be suppressed for a non-owner"
    );
    txn2.commit().unwrap();

    // An autocommit read (no prior lock) must also see NOTFOUND.
    assert!(
        db.get(key).unwrap().is_none(),
        "autocommit read must see the expired record as NOTFOUND"
    );

    set_test_time_ms(0);
}
