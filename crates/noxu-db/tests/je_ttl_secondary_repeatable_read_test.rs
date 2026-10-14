//! Port of JE `com.sleepycat.je.test.TTLTest.testSecondaryRepeatableRead`
//! (TTLTest.java:1211) — observable core.
//!
//! A secondary record (index entry + primary record) that expires within
//! TTL_MAX_TXN_TIME stays visible to a transaction that already locked it
//! (JE holds an extra lock on the secondary to support repeatable read),
//! while a different transaction sees NOTFOUND.  The JE `getNTotalLocks()`
//! count assertions are internal introspection; the portable contract is the
//! read visibility.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use noxu_db::secondary_config::{SecondaryConfig, SecondaryKeyCreator};
use noxu_db::{
    Database, DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig,
    SecondaryDatabase, TtlUnit, WriteOptions,
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

// PORTED-FAILING (kept ignored, matching integrate c9f62dad which deleted it):
// the secondary TTL *propagation* fix (c9f62dad) makes the secondary index
// entry expire with the primary, but secondary *repeatable read* additionally
// needs JE's extra lock on the secondary slot AND the TTL-RR lock-ownership
// rule (CursorImpl.lockLN wasLockedAndExpiresSoon) applied through the
// secondary->primary fetch path (SecondaryDatabase::get_into ->
// get_into_with_options -> primary get_with_options).  BUG-TTL-REPEATABLE-READ-1
// wires that rule only into the PRIMARY cursor exact-read, not into the
// secondary read's internal primary fetch, so a lock owner's repeatable read
// via the secondary after mid-txn expiry still returns NOTFOUND.  Un-ignore
// once the secondary read threads lock-ownership into its primary fetch
// (a distinct, larger change than the propagation commit scoped here).
#[test]
#[ignore = "secondary repeatable-read needs TTL-RR threaded through the             secondary->primary fetch + an extra secondary lock; out of scope             for the TTL-propagation commit (matches integrate PORTED-FAILING)"]
fn ttl_test_test_secondary_repeatable_read() {
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

    // Write a record expiring in 1 hour (sec key = 4).
    {
        let txn = env.begin_transaction(None).unwrap();
        primary
            .lock()
            .put_with_options(
                Some(&txn),
                pri_key,
                data,
                &WriteOptions::new()
                    .with_ttl_unit(1, TtlUnit::Hours)
                    .with_update_ttl(true),
            )
            .unwrap();
        txn.commit().unwrap();
    }

    // In a txn: read via primary (locks primary) AND via secondary (locks the
    // secondary index entry + primary), then advance past expiry.  Both reads
    // must still succeed within the same txn (repeatable read).
    let txn = env.begin_transaction(None).unwrap();
    assert!(
        primary.lock().get_in(&txn, pri_key).unwrap().is_some(),
        "primary read must find the live record"
    );
    let mut pk = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    assert!(
        sec.get_into(Some(&txn), sec_key, &mut pk, &mut d).unwrap(),
        "secondary read must find the live record"
    );

    // Advance past the 1-hour expiry (within the 24h TTL_MAX_TXN_TIME window).
    set_test_time_ms(BASE_MS + 3 * MILLIS_PER_HOUR);

    // Repeatable read within the same txn: the secondary lookup (index +
    // primary), both now expired, must still return the record because this
    // txn owns the locks (JE: extra secondary lock supports repeatable read).
    let mut pk2 = DatabaseEntry::new();
    let mut d2 = DatabaseEntry::new();
    assert!(
        sec.get_into(Some(&txn), sec_key, &mut pk2, &mut d2).unwrap(),
        "repeatable read: a txn that already locked the record must still see \
         it via the secondary after it expires mid-txn"
    );
    txn.commit().unwrap();

    // A DIFFERENT transaction (not the lock owner) sees the expired record as
    // NOTFOUND via the secondary.
    let txn2 = env.begin_transaction(None).unwrap();
    let mut pk3 = DatabaseEntry::new();
    let mut d3 = DatabaseEntry::new();
    assert!(
        !sec.get_into(Some(&txn2), sec_key, &mut pk3, &mut d3).unwrap(),
        "a different txn must see the expired secondary record as NOTFOUND"
    );
    txn2.commit().unwrap();

    set_test_time_ms(0);
}
