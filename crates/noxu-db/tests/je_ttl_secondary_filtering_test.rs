//! Secondary TTL propagation witness (BUG — primary expiration must flow onto
//! the secondary index entry).  Port of the observable core of JE
//! `com.sleepycat.je.test.TTLTest.testSecondaryExpirationTimeStorage`
//! (TTLTest.java:889): the secondary index entry carries the primary record's
//! expiration, so the secondary slot expires together with the primary.
//!
//! Before the fix, Noxu created secondary index entries with no expiration, so
//! after the primary expired a read via the secondary found a live index entry
//! pointing at a filtered (expired) primary and raised
//! `SecondaryIntegrityException` instead of a clean NOTFOUND.
//!
//! Uses the TTL test-time hook (`noxu_util::ttl::set_test_time_ms`).

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use noxu_db::secondary_config::{SecondaryConfig, SecondaryKeyCreator};
use noxu_db::{
    Database, DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig,
    ReadOptions, SecondaryDatabase, TtlUnit, WriteOptions,
};
use noxu_sync::Mutex;
use noxu_util::ttl::set_test_time_ms;
use tempfile::TempDir;

const BASE_MS: u64 = 1_454_472_306_000;
const MILLIS_PER_HOUR: u64 = 3600 * 1000;

/// JE keyCreator: secondary key = first byte of primary data.
struct FirstByteKeyCreator;
impl SecondaryKeyCreator for FirstByteKeyCreator {
    fn create_secondary_key(
        &self,
        _db: &Database,
        _key: &DatabaseEntry,
        data: &DatabaseEntry,
        result: &mut DatabaseEntry,
    ) -> bool {
        if let Some(d) = data.data_opt()
            && !d.is_empty()
        {
            result.set_data(&[d[0]]);
            return true;
        }
        false
    }
}

fn open_primary(env: &Environment, name: &str) -> Arc<Mutex<Database>> {
    let db = env
        .open_database(
            None,
            name,
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap();
    Arc::new(Mutex::new(db))
}

/// A primary record written with a TTL, read back via its secondary index:
/// while live it is found; after expiry a secondary read returns a clean
/// NOTFOUND (the secondary entry expired with the primary) — NOT a
/// SecondaryIntegrityException.
#[test]
fn ttl_secondary_entry_expires_with_primary() {
    let dir = TempDir::new().unwrap();
    let env_cfg = EnvironmentConfig::new(dir.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(true)
        .with_run_evictor(false)
        .with_run_cleaner(false)
        .with_run_checkpointer(false)
        .with_run_in_compressor(false);
    let env = Environment::open(env_cfg).unwrap();

    let primary = open_primary(&env, "primary");
    let inner = env
        .open_database(
            None,
            "sec",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true)
                .with_sorted_duplicates(true),
        )
        .unwrap();
    let sec = SecondaryDatabase::open(
        Arc::clone(&primary),
        inner,
        SecondaryConfig::new()
            .with_allow_create(true)
            .with_sorted_duplicates(true)
            .with_key_creator(Box::new(FirstByteKeyCreator)),
    )
    .unwrap();

    set_test_time_ms(BASE_MS);

    let pri_key = (123u32).to_be_bytes();
    let sec_key = [4u8];
    let data = [4u8];

    // Write a primary record expiring in 1 hour (sec key = 4).
    primary
        .lock()
        .put_with_options(
            None,
            pri_key,
            data,
            &WriteOptions::new()
                .with_ttl_unit(1, TtlUnit::Hours)
                .with_update_ttl(true),
        )
        .unwrap();

    // While live: an autocommit secondary read finds it.
    let mut pk = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    assert!(
        sec.get_into(None, sec_key, &mut pk, &mut d).unwrap(),
        "live secondary read must find the record"
    );

    // Advance past the 1-hour expiry.
    set_test_time_ms(BASE_MS + 3 * MILLIS_PER_HOUR);

    // The secondary index entry must have expired together with the primary:
    // a locking autocommit read returns a clean NOTFOUND, NOT a
    // SecondaryIntegrityException (which is what a never-expiring index entry
    // pointing at a filtered primary would have raised).
    let mut pk2 = DatabaseEntry::new();
    let mut d2 = DatabaseEntry::new();
    let found = sec.get_into(None, sec_key, &mut pk2, &mut d2).expect(
        "expired secondary read must NOT raise SecondaryIntegrityException",
    );
    assert!(!found, "expired secondary read must return NOTFOUND");

    // And a dirty read agrees.
    let mut pk3 = DatabaseEntry::new();
    let mut d3 = DatabaseEntry::new();
    let found_dirty = sec
        .get_into_with_options(
            None,
            sec_key,
            &mut pk3,
            &mut d3,
            &ReadOptions::read_uncommitted(),
        )
        .expect("expired dirty secondary read must NOT raise");
    assert!(!found_dirty, "expired dirty secondary read must return NOTFOUND");

    set_test_time_ms(0);
}
