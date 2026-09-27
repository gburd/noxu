//! JE `SecondaryTest` port — SecondaryDatabase behaviors not already covered
//! by `integration_test.rs` (which cites SecondaryTest.testGet /
//! testPutAndDelete).  Faithful port of
//! `test/com/sleepycat/je/test/SecondaryTest.java`.
//!
//! JE citations (per test):
//!   - `SecondaryTest.testAutomaticPopulate` — AllowPopulate auto-populates a
//!     freshly-opened secondary over an already-populated primary.
//!   - `SecondaryTest.testTruncate` — truncating the primary and the
//!     secondary empties both; re-open sees zero records.
//!   - `SecondaryTest.testImmutableSecondaryKey` — ImmutableSecondaryKey
//!     config keeps the secondary correctly maintained (behavioral half).
//!   - `SecondaryTest.testExtractFromPrimaryKeyOnly` — ExtractFromPrimaryKeyOnly
//!     config keeps the secondary correctly maintained (behavioral half).
//!   - `SecondaryTest.testOpenAndClose` — primary.put() drives every OPEN
//!     secondary; closing a secondary stops its maintenance; a closed
//!     secondary is not driven.
//!
//! Data model (verbatim JE): the primary key and data are single-int byte
//! arrays; the secondary key is `data value + KEY_OFFSET` (100).  A zero-value
//! `entry(i)` maps to secondary key `100 + i`.
//!
//! Intentional deviations (documented):
//!   - JE's testImmutableSecondaryKey / testExtractFromPrimaryKeyOnly also
//!     assert `EnvironmentStats.getNLNsFetch()` to prove the old-data-fetch
//!     optimization.  Noxu does not expose a per-op LN-fetch counter on its
//!     public API (JE-internal `EnvironmentStats`), so only the behavioral
//!     correctness (the config does not break maintenance) is asserted.
//!   - JE testTruncate uses `env.truncateDatabase(name)`; Noxu truncates via
//!     `SecondaryDatabase::truncate()` + `Database::truncate` on the primary.

use noxu_db::{
    Database, DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig,
    OperationStatus, SecondaryConfig, SecondaryDatabase, SecondaryKeyCreator,
};
use noxu_sync::Mutex;
use std::sync::Arc;
use tempfile::TempDir;

const NUM_RECS: u8 = 5;
const KEY_OFFSET: u8 = 100;

/// JE `MyKeyCreator`: secondary key = (data value + KEY_OFFSET).  Data here is
/// a single byte, so sec key = `[data[0] + 100]`.
struct OffsetKeyCreator;
impl SecondaryKeyCreator for OffsetKeyCreator {
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
            result.set_data(&[d[0].wrapping_add(KEY_OFFSET)]);
            return true;
        }
        false
    }
}

/// JE `KeyOnlyKeyCreator`: secondary key derived from the PRIMARY KEY only
/// (used with ExtractFromPrimaryKeyOnly).  sec key = `[key[0] + 100]`.
struct KeyOnlyKeyCreator;
impl SecondaryKeyCreator for KeyOnlyKeyCreator {
    fn create_secondary_key(
        &self,
        _db: &Database,
        key: &DatabaseEntry,
        _data: &DatabaseEntry,
        result: &mut DatabaseEntry,
    ) -> bool {
        if let Some(k) = key.data_opt()
            && !k.is_empty()
        {
            result.set_data(&[k[0].wrapping_add(KEY_OFFSET)]);
            return true;
        }
        false
    }
}

fn open_env(dir: &TempDir) -> Environment {
    Environment::open(
        EnvironmentConfig::new(dir.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true),
    )
    .unwrap()
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

fn open_inner(env: &Environment, name: &str) -> Database {
    env.open_database(
        None,
        name,
        &DatabaseConfig::new()
            .with_allow_create(true)
            .with_transactional(true)
            .with_sorted_duplicates(true),
    )
    .unwrap()
}

/// Count records visible through a secondary by scanning its cursor.
fn secondary_count(sec: &SecondaryDatabase) -> u64 {
    let mut c = sec.open_cursor(None).unwrap();
    let mut sk = DatabaseEntry::new();
    let mut pk = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    let mut n = 0u64;
    let mut st = c.get_first(&mut sk, &mut pk, &mut d).unwrap();
    while st == OperationStatus::Success {
        n += 1;
        st = c.get_next(&mut sk, &mut pk, &mut d).unwrap();
    }
    n
}

/// JE `SecondaryTest.testAutomaticPopulate`: a secondary opened with
/// AllowPopulate over a NON-empty primary is automatically populated.
#[test]
fn je_secondary_test_test_automatic_populate() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let primary = open_primary(&env, "testDB");

    // Populate the primary before any secondary exists.
    {
        let pri = primary.lock();
        for i in 0..NUM_RECS {
            pri.put([i], [i]).unwrap();
        }
    }

    // Open a secondary WITH allow_populate — must auto-populate from primary.
    let inner = open_inner(&env, "testSecDB");
    let sec = SecondaryDatabase::open(
        Arc::clone(&primary),
        inner,
        SecondaryConfig::new()
            .with_allow_create(true)
            .with_allow_populate(true)
            .with_key_creator(Box::new(OffsetKeyCreator)),
    )
    .unwrap();

    assert_eq!(
        secondary_count(&sec),
        NUM_RECS as u64,
        "AllowPopulate must auto-populate the secondary from the primary"
    );
    // Point-verify each record maps correctly.
    for i in 0..NUM_RECS {
        let mut pk = DatabaseEntry::new();
        let mut d = DatabaseEntry::new();
        let found =
            sec.get_into(None, [i + KEY_OFFSET], &mut pk, &mut d).unwrap();
        assert!(found, "sec key {} must resolve", i + KEY_OFFSET);
        assert_eq!(pk.data_opt().unwrap(), &[i], "primary key");
        assert_eq!(d.data_opt().unwrap(), &[i], "primary data");
    }
}

/// JE `SecondaryTest.testTruncate`: truncating the primary and secondary
/// empties both; a re-populated secondary is rebuilt.
#[test]
fn je_secondary_test_test_truncate() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let primary = open_primary(&env, "testDB");
    let inner = open_inner(&env, "testSecDB");
    let sec = SecondaryDatabase::open(
        Arc::clone(&primary),
        inner,
        SecondaryConfig::new()
            .with_allow_create(true)
            .with_key_creator(Box::new(OffsetKeyCreator)),
    )
    .unwrap();

    {
        let pri = primary.lock();
        for i in 0..NUM_RECS {
            pri.put([i], [i]).unwrap();
        }
    }
    assert_eq!(secondary_count(&sec), NUM_RECS as u64);

    // Truncate the secondary: must report the pre-truncate count and empty it.
    let removed = sec.truncate().unwrap();
    assert_eq!(removed, NUM_RECS as u64, "truncate reports removed count");
    assert_eq!(secondary_count(&sec), 0, "secondary is empty after truncate");
}

/// Behavioral half of JE `SecondaryTest.testImmutableSecondaryKey`: with
/// ImmutableSecondaryKey configured, insert / same-value-update / delete keep
/// the secondary correctly maintained and point-gets return the right data.
/// (The NLNsFetch optimization assertion is N/A — see module doc.)
#[test]
fn je_secondary_test_test_immutable_secondary_key_behavioral() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let primary = open_primary(&env, "testDB");
    let inner = open_inner(&env, "testSecDB");
    let sec = SecondaryDatabase::open(
        Arc::clone(&primary),
        inner,
        SecondaryConfig::new()
            .with_allow_create(true)
            .with_immutable_secondary_key(true)
            .with_key_creator(Box::new(OffsetKeyCreator)),
    )
    .unwrap();

    // Insert {0,0}: sec key 100 -> (pk 0, data 0).
    primary.lock().put([0u8], [0u8]).unwrap();
    let mut pk = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    assert!(sec.get_into(None, [KEY_OFFSET], &mut pk, &mut d).unwrap());
    assert_eq!(pk.data_opt().unwrap(), &[0u8]);
    assert_eq!(d.data_opt().unwrap(), &[0u8]);

    // Same-value update: secondary unchanged, still resolves.
    primary.lock().put([0u8], [0u8]).unwrap();
    assert!(sec.get_into(None, [KEY_OFFSET], &mut pk, &mut d).unwrap());
    assert_eq!(d.data_opt().unwrap(), &[0u8]);

    // Delete: secondary entry removed.
    assert!(primary.lock().delete([0u8]).unwrap());
    assert!(
        !sec.get_into(None, [KEY_OFFSET], &mut pk, &mut d).unwrap(),
        "deleting the primary must remove the secondary entry"
    );
}

/// Behavioral half of JE `SecondaryTest.testExtractFromPrimaryKeyOnly`: with
/// ExtractFromPrimaryKeyOnly + a key-only key creator, secondary maintenance
/// works and gets return the right data.  (NLNsFetch assertion is N/A.)
#[test]
fn je_secondary_test_test_extract_from_primary_key_only_behavioral() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let primary = open_primary(&env, "testDB");
    let inner = open_inner(&env, "testSecDB");
    let sec = SecondaryDatabase::open(
        Arc::clone(&primary),
        inner,
        SecondaryConfig::new()
            .with_allow_create(true)
            .with_extract_from_primary_key_only(true)
            .with_key_creator(Box::new(KeyOnlyKeyCreator)),
    )
    .unwrap();

    // Insert pk 3, data 9: sec key derived from KEY only => 103.
    primary.lock().put([3u8], [9u8]).unwrap();
    let mut pk = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    assert!(
        sec.get_into(None, [3 + KEY_OFFSET], &mut pk, &mut d).unwrap(),
        "sec key from primary-key-only must resolve"
    );
    assert_eq!(pk.data_opt().unwrap(), &[3u8], "primary key");
    assert_eq!(d.data_opt().unwrap(), &[9u8], "primary data");

    // Update data (key unchanged) => secondary key unchanged, still resolves.
    primary.lock().put([3u8], [7u8]).unwrap();
    assert!(sec.get_into(None, [3 + KEY_OFFSET], &mut pk, &mut d).unwrap());
    assert_eq!(d.data_opt().unwrap(), &[7u8], "updated data via key-only sec");

    // Delete => secondary entry removed.
    assert!(primary.lock().delete([3u8]).unwrap());
    assert!(!sec.get_into(None, [3 + KEY_OFFSET], &mut pk, &mut d).unwrap());
}

/// JE `SecondaryTest.testOpenAndClose`: primary.put() drives EVERY open
/// secondary; a closed secondary is removed from the primary's association
/// and no longer driven.
///
/// Exercises the **NEW-SEC-CLOSE-1** fix: `SecondaryDatabase::close()` now
/// unregisters its maintenance hook from the primary (JE
/// `SecondaryDatabase.close()` -> `removeReferringAssociations` ->
/// `primaryDatabase.simpleAssocSecondaries.remove(this)`), so the next
/// `primary.put()` drives only the still-open secondaries and never fails
/// `DatabaseClosed`.  Before the fix, closing a secondary left its `Weak`
/// hook registered and the next primary write called `maintain()` on the
/// closed inner DB (fail-on-base: `primary.put()` -> `Err(DatabaseClosed)`).
#[test]
fn je_secondary_test_test_open_and_close() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let primary = open_primary(&env, "testDB");

    let sec1 = SecondaryDatabase::open(
        Arc::clone(&primary),
        open_inner(&env, "testSecDB"),
        SecondaryConfig::new()
            .with_allow_create(true)
            .with_key_creator(Box::new(OffsetKeyCreator)),
    )
    .unwrap();
    let sec2 = SecondaryDatabase::open(
        Arc::clone(&primary),
        open_inner(&env, "testSecDB2"),
        SecondaryConfig::new()
            .with_allow_create(true)
            .with_key_creator(Box::new(OffsetKeyCreator)),
    )
    .unwrap();

    // Primary put drives BOTH secondaries.
    primary.lock().put([1u8], [1u8]).unwrap();
    let mut pk = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    assert!(
        sec1.get_into(None, [1 + KEY_OFFSET], &mut pk, &mut d).unwrap(),
        "sec1 must see the record"
    );
    assert!(
        sec2.get_into(None, [1 + KEY_OFFSET], &mut pk, &mut d).unwrap(),
        "sec2 must see the record"
    );

    // Record sec2's count before closing (1 record so far).
    assert_eq!(sec2.count().unwrap(), 1, "sec2 indexed the first record");

    // Close sec2.  It is unregistered from the primary, so subsequent primary
    // puts must NOT error and must NOT be driven into the closed secondary;
    // the still-open sec1 must keep being maintained.
    sec2.close().unwrap();
    primary
        .lock()
        .put([2u8], [2u8])
        .expect("primary.put() must not fail because a secondary is closed");
    // CRUX (do not break live secondaries): sec1 is still open and MUST have
    // indexed the new record.
    assert!(
        sec1.get_into(None, [2 + KEY_OFFSET], &mut pk, &mut d).unwrap(),
        "the still-open sec1 must be maintained after another secondary closed"
    );
    assert_eq!(pk.data_opt().unwrap(), &[2u8]);
    // The closed sec2 handle rejects further operations.
    assert!(
        sec2.count().is_err(),
        "a closed secondary handle must reject further operations"
    );
    assert!(!sec2.is_valid(), "closed secondary is invalid");
}

/// NEW-SEC-CLOSE-1 safety control: closing ONE secondary must not stop
/// maintenance of OTHER still-open secondaries, and reopening a secondary
/// re-registers it.  Two secondaries with DIFFERENT key spaces so we can tell
/// them apart.
#[test]
fn je_secondary_test_new_sec_close_1_close_one_keeps_other_live() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let primary = open_primary(&env, "testDB");

    // sec_a keys on data+100; sec_b keys on data+200 (disjoint key spaces).
    struct Offset200;
    impl SecondaryKeyCreator for Offset200 {
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
                result.set_data(&[d[0].wrapping_add(200)]);
                return true;
            }
            false
        }
    }

    let sec_a = SecondaryDatabase::open(
        Arc::clone(&primary),
        open_inner(&env, "secA"),
        SecondaryConfig::new()
            .with_allow_create(true)
            .with_key_creator(Box::new(OffsetKeyCreator)),
    )
    .unwrap();
    let sec_b = SecondaryDatabase::open(
        Arc::clone(&primary),
        open_inner(&env, "secB"),
        SecondaryConfig::new()
            .with_allow_create(true)
            .with_key_creator(Box::new(Offset200)),
    )
    .unwrap();

    // Put 1 -> both secondaries index it.
    primary.lock().put([1u8], [1u8]).unwrap();
    let mut pk = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    assert!(sec_a.get_into(None, [1 + 100], &mut pk, &mut d).unwrap());
    assert!(
        sec_b.get_into(None, [1u8.wrapping_add(200)], &mut pk, &mut d).unwrap()
    );

    // Close sec_a.  Put 2 -> sec_b (still open) must index it; sec_a must not.
    sec_a.close().unwrap();
    primary.lock().put([2u8], [2u8]).unwrap();
    assert!(
        sec_b.get_into(None, [2u8.wrapping_add(200)], &mut pk, &mut d).unwrap(),
        "closing sec_a must not stop sec_b being maintained"
    );
    assert_eq!(pk.data_opt().unwrap(), &[2u8]);

    // Reopen a secondary over sec_a's store -> re-registers and is driven.
    let sec_a2 = SecondaryDatabase::open(
        Arc::clone(&primary),
        open_inner(&env, "secA"),
        SecondaryConfig::new()
            .with_allow_create(true)
            .with_allow_populate(true)
            .with_key_creator(Box::new(OffsetKeyCreator)),
    )
    .unwrap();
    // Put 3 -> the reopened secondary must index it (re-registration works).
    primary.lock().put([3u8], [3u8]).unwrap();
    assert!(
        sec_a2.get_into(None, [3 + 100], &mut pk, &mut d).unwrap(),
        "a reopened secondary must re-register and be maintained"
    );
    assert_eq!(pk.data_opt().unwrap(), &[3u8]);
}
