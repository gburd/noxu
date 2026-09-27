//! JE `ToManyTest` port — to-many secondary indexes via a
//! `SecondaryMultiKeyCreator` (each byte of the primary data is a secondary
//! key).  Faithful port of `test/com/sleepycat/je/test/ToManyTest.java`.
//!
//! JE citations:
//!   - `ToManyTest.testManyToMany` — sorted-dup secondary; a secondary key
//!     may map to many primaries and a primary produces many secondary keys.
//!   - `ToManyTest.testOneToMany` — N/A (Decision 1B): Noxu implements only
//!     sorted-dup secondaries, so the non-dup / unique-constraint one-to-many
//!     config the JE test needs does not exist.  We instead assert the
//!     documented rejection of a non-dup secondary (non-vacuous).
//!
//! Verification (verbatim JE `verify()`): after each write we rebuild the
//! primary→secondary maps two ways — by scanning the primary DB and by
//! scanning the secondary DB — and assert both agree with the expected
//! model maps we maintain in lockstep.
//!
//! Intentional deviation (documented):
//!   - JE also asserts `DbInternal.getCursorImpl(c).getNSecondaryWrites()`
//!     equals the number of secondary map changes.  Noxu does not expose a
//!     per-cursor secondary-write counter on its public API (JE-internal
//!     `DbInternal` reflection), so that per-write count is not checked.
//!     The substantive invariant — that the secondary index exactly mirrors
//!     the primary data's byte-set after every mutation — is fully asserted.

use noxu_db::{
    Database, DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig,
    OperationStatus, SecondaryConfig, SecondaryDatabase,
};
use noxu_db::secondary_config::SecondaryMultiKeyCreator;
use noxu_sync::Mutex;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use tempfile::TempDir;

/// JE `MyKeyCreator`: each byte of the primary data is a distinct secondary
/// key.  (JE adds `new DatabaseEntry(data, i, 1)`; a zero byte is a valid
/// secondary key here, unlike JoinTest.)
struct EachByteMultiKeyCreator;
impl SecondaryMultiKeyCreator for EachByteMultiKeyCreator {
    fn create_secondary_keys(
        &self,
        _db: &Database,
        _key: &DatabaseEntry,
        data: &DatabaseEntry,
        results: &mut Vec<DatabaseEntry>,
    ) {
        if let Some(d) = data.data_opt() {
            for b in d {
                results.push(DatabaseEntry::from_bytes(&[*b]));
            }
        }
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

fn open_primary(env: &Environment) -> Arc<Mutex<Database>> {
    let db = env
        .open_database(
            None,
            "pri",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap();
    Arc::new(Mutex::new(db))
}

fn open_secondary(
    env: &Environment,
    primary: &Arc<Mutex<Database>>,
    dups: bool,
) -> SecondaryDatabase {
    let inner = env
        .open_database(
            None,
            "sec",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true)
                .with_sorted_duplicates(dups),
        )
        .unwrap();
    SecondaryDatabase::open(
        Arc::clone(primary),
        inner,
        SecondaryConfig::new()
            .with_allow_create(true)
            .with_sorted_duplicates(dups)
            .with_multi_key_creator(Box::new(EachByteMultiKeyCreator)),
    )
    .unwrap()
}

/// The model: pri_map[priKey] = set of secondary-key bytes (from priData).
type Model = BTreeMap<u8, BTreeSet<u8>>;

fn bytes_to_set(data: Option<&[u8]>) -> Option<BTreeSet<u8>> {
    data.map(|b| b.iter().copied().collect())
}

struct Harness {
    _dir: TempDir,
    _env: Environment,
    primary: Arc<Mutex<Database>>,
    secondary: SecondaryDatabase,
    /// Expected primary map (JE priMap0).
    pri_model: Model,
}

impl Harness {
    fn new(dups: bool) -> Self {
        let dir = TempDir::new().unwrap();
        let env = open_env(&dir);
        let primary = open_primary(&env);
        let secondary = open_secondary(&env, &primary, dups);
        Self { _dir: dir, _env: env, primary, secondary, pri_model: Model::new() }
    }

    /// JE `write`: put (priData != null) or delete (priData == null) a single
    /// primary record.  Returns Ok on success, Err if the write was rejected
    /// (unique-constraint violation in the one-to-many case).
    fn write(&self, pri_key: u8, pri_data: Option<&[u8]>) -> Result<(), String> {
        let pri = self.primary.lock();
        match pri_data {
            Some(d) => pri
                .put([pri_key], d)
                .map_err(|e| e.to_string()),
            None => {
                // JE: get then delete; delete of a missing key is fine here
                // because writeAndVerify only deletes existing records.
                pri.delete([pri_key]).map(|_| ()).map_err(|e| e.to_string())
            }
        }
    }

    /// JE `updateMaps`: mutate the expected model to reflect the write.
    fn update_model(&mut self, pri_key: u8, new_pri_data: Option<BTreeSet<u8>>) {
        match new_pri_data {
            Some(set) => {
                self.pri_model.insert(pri_key, set);
            }
            None => {
                self.pri_model.remove(&pri_key);
            }
        }
    }

    /// JE `writeAndVerify`.
    fn write_and_verify(&mut self, pri_key: u8, pri_data: Option<&[u8]>) {
        self.write(pri_key, pri_data)
            .expect("write must succeed in writeAndVerify");
        self.update_model(pri_key, bytes_to_set(pri_data));
        self.verify();
    }

    /// JE `verify`: rebuild the maps from the primary DB and the secondary DB
    /// and assert they agree with the model.
    fn verify(&self) {
        // Expected secondary model derived from pri_model:
        // sec_model[secKey] = { priKey : secKey in pri_model[priKey] }.
        let mut sec_model: BTreeMap<u8, BTreeSet<u8>> = BTreeMap::new();
        for (pk, secset) in &self.pri_model {
            for sk in secset {
                sec_model.entry(*sk).or_default().insert(*pk);
            }
        }

        // Build pri_map1 / sec_map1 from a primary-DB scan.
        let mut pri_map1: Model = Model::new();
        let mut sec_map1: BTreeMap<u8, BTreeSet<u8>> = BTreeMap::new();
        {
            let pri = self.primary.lock();
            let mut c = pri.open_cursor(None).unwrap();
            while let Some((k, v)) = c.next().unwrap() {
                let pk = k[0];
                let set: BTreeSet<u8> = v.iter().copied().collect();
                for sk in &set {
                    sec_map1.entry(*sk).or_default().insert(pk);
                }
                pri_map1.insert(pk, set);
            }
        }

        // Build pri_map2 / sec_map2 from a secondary-DB scan.
        // Empty-data primaries can't be reached via the secondary, so seed
        // them from pri_map1 (JE does the same via priData.isEmpty()).
        let mut pri_map2: Model = Model::new();
        let mut sec_map2: BTreeMap<u8, BTreeSet<u8>> = BTreeMap::new();
        for (pk, set) in &pri_map1 {
            if set.is_empty() {
                pri_map2.insert(*pk, BTreeSet::new());
            }
        }
        {
            let mut sc = self.secondary.open_cursor(None).unwrap();
            let mut sk_e = DatabaseEntry::new();
            let mut pk_e = DatabaseEntry::new();
            let mut d_e = DatabaseEntry::new();
            let mut st = sc.get_first(&mut sk_e, &mut pk_e, &mut d_e).unwrap();
            while st == OperationStatus::Success {
                let sk = sk_e.data_opt().unwrap()[0];
                let pk = pk_e.data_opt().unwrap()[0];
                pri_map2.entry(pk).or_default().insert(sk);
                sec_map2.entry(sk).or_default().insert(pk);
                st = sc.get_next(&mut sk_e, &mut pk_e, &mut d_e).unwrap();
            }
        }

        // JE's four assertions.
        assert_eq!(self.pri_model, pri_map1, "priMap0 == priMap1 (primary scan)");
        assert_eq!(pri_map1, pri_map2, "priMap1 == priMap2 (secondary scan)");
        assert_eq!(sec_model, sec_map1, "secMap0 == secMap1");
        assert_eq!(sec_map1, sec_map2, "secMap1 == secMap2");
    }
}

/// JE `ToManyTest.testManyToMany`: sorted-dup secondary.
#[test]
fn je_to_many_test_test_many_to_many() {
    let mut h = Harness::new(true /* dups */);

    h.write_and_verify(0, Some(&[]));
    h.write_and_verify(0, None);
    h.write_and_verify(0, Some(&[0, 1, 2]));
    h.write_and_verify(0, None);
    h.write_and_verify(0, Some(&[]));
    h.write_and_verify(0, Some(&[0]));
    h.write_and_verify(0, Some(&[0, 1]));
    h.write_and_verify(0, Some(&[0, 1, 2]));
    h.write_and_verify(0, Some(&[1, 2]));
    h.write_and_verify(0, Some(&[2]));
    h.write_and_verify(0, Some(&[]));
    h.write_and_verify(0, None);

    h.write_and_verify(0, Some(&[0, 1, 2]));
    h.write_and_verify(1, Some(&[1, 2, 3]));
    h.write_and_verify(0, None);
    h.write_and_verify(1, None);
    h.write_and_verify(0, Some(&[0, 1, 2]));
    h.write_and_verify(1, Some(&[1, 2, 3]));
    h.write_and_verify(0, Some(&[0]));
    h.write_and_verify(1, Some(&[3]));
    h.write_and_verify(0, None);
    h.write_and_verify(1, None);
}

/// JE `ToManyTest.testOneToMany`: **N/A (documented deviation, Decision 1B).**
///
/// JE's one-to-many test uses a NON-dup secondary and relies on the
/// UniqueConstraintException raised when a secondary key already maps to a
/// different primary.  Noxu v1.6 secondaries are always sorted-dup by design
/// (Decision 1B / audit C4): the inner index DB MUST be opened with
/// `with_sorted_duplicates(true)`, and `SecondaryDatabase::open` rejects a
/// non-dup inner index outright.  There is therefore no unique-constraint /
/// one-to-many secondary to exercise — the whole premise of the JE test is a
/// configuration Noxu does not support.
///
/// Rather than leave this vacuous, we assert the documented limitation
/// itself: opening a secondary over a non-dup inner index is rejected with
/// the Decision-1B error.  If Noxu ever gains unique secondaries this test
/// will start failing and must be replaced by the full JE port.
#[test]
fn je_to_many_test_test_one_to_many_na_non_dup_secondary_rejected() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let primary = open_primary(&env);

    // A non-dup inner index (JE's one-to-many config).
    let inner = env
        .open_database(
            None,
            "sec",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true)
                .with_sorted_duplicates(false),
        )
        .unwrap();
    let r = SecondaryDatabase::open(
        Arc::clone(&primary),
        inner,
        SecondaryConfig::new()
            .with_allow_create(true)
            .with_sorted_duplicates(false)
            .with_multi_key_creator(Box::new(EachByteMultiKeyCreator)),
    );
    assert!(
        r.is_err(),
        "Decision 1B: a non-dup (unique / one-to-many) secondary must be \
         rejected — Noxu implements only sorted-dup secondaries"
    );
    let msg = r.err().unwrap().to_string();
    assert!(
        msg.contains("sorted_duplicates") || msg.contains("Decision 1B"),
        "rejection must cite the Decision-1B sorted-dup requirement, got: {msg}"
    );
}
