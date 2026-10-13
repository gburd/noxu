//! Crash-recovery undo of an in-flight txn's deletes in a sorted-duplicate
//! database -- and therefore in every secondary index -- must restore every
//! committed duplicate.
//!
//! Provenance: freebsd-kd-slot-review reviewer probes (finding F1), imported
//! from /work/artifacts/freebsd-kd-slot-review/zz_review_kd_probe.rs
//! (probe_d, sha256 9f8dd7ed...) and zz_review_kd_probe2.rs (probe_e,
//! probe_e2, sha256 092fffcd...).  At 4407d488 and 6ea0a5e2 recovery
//! restored only 21/41 secondary entries (d), 26/41 (e) and 21/41 (e2) dup
//! records.
//!
//! JE stores a dup record as a two-part key (DupKeyData) and undoes it by
//! that full key: RecoveryManager.undo finds the slot for the logged key and
//! reverts it when `logLsn == slotLsn` (RecoveryManager.java:1885-1963).
//!
//! Crash image = copy of the environment files (minus `noxu.lck`) taken
//! while the env is open, after a later sync auto-commit forced the log.
#![cfg(not(noxu_shuttle))]

use noxu_db::{
    CheckpointConfig, Database, DatabaseConfig, DatabaseEntry, Durability,
    Environment, EnvironmentConfig, Get, Mutex, OperationStatus,
    SecondaryConfig, SecondaryDatabase, SecondaryKeyCreator,
};
use std::path::Path;
use std::sync::Arc;

fn env_cfg(p: &Path, nmax: u32) -> EnvironmentConfig {
    let mut c = EnvironmentConfig::new(p.to_path_buf())
        .with_transactional(true)
        .with_allow_create(true)
        .with_durability(Durability::COMMIT_SYNC)
        .with_node_max_entries(nmax);
    c.set_run_cleaner(false);
    c.set_run_checkpointer(false);
    c.set_run_in_compressor(false);
    c
}

fn db_cfg(dups: bool) -> DatabaseConfig {
    DatabaseConfig::new()
        .with_allow_create(true)
        .with_transactional(true)
        .with_sorted_duplicates(dups)
}

fn image(dir: &Path) -> tempfile::TempDir {
    let img = tempfile::tempdir().unwrap();
    for f in std::fs::read_dir(dir).unwrap() {
        let f = f.unwrap();
        if f.file_type().unwrap().is_file() && f.file_name() != "noxu.lck" {
            std::fs::copy(f.path(), img.path().join(f.file_name())).unwrap();
        }
    }
    img
}

fn k(i: u32) -> Vec<u8> {
    format!("k{:05}", i).into_bytes()
}

fn v(i: u32) -> Vec<u8> {
    format!("val-{:05}-committed", i).into_bytes()
}

fn force_ckpt(env: &Environment) {
    env.checkpoint(Some(&CheckpointConfig::new().with_force(true))).unwrap();
}

/// Every (key, data) pair in key order.
fn scan_all(db: &Database) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut c = db.open_cursor(None).unwrap();
    let (mut key, mut d) = (DatabaseEntry::new(), DatabaseEntry::new());
    let mut out = Vec::new();
    let mut st = c.get(&mut key, &mut d, Get::First, None).unwrap();
    while st == OperationStatus::Success {
        out.push((
            key.data_opt().unwrap().to_vec(),
            d.data_opt().unwrap_or_default().to_vec(),
        ));
        st = c.get(&mut key, &mut d, Get::Next, None).unwrap();
    }
    out
}

fn expected_dups(n: u32) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut want = Vec::new();
    for i in 0..n {
        want.push((k(i), b"d1".to_vec()));
        want.push((k(i), b"d2".to_vec()));
    }
    want.push((b"zz".to_vec(), b"x".to_vec()));
    want
}

fn verify_clean(env: &Environment, at: &str) {
    let r = env.verify(&noxu_db::VerifyConfig::new()).unwrap();
    assert_eq!(r.error_count(), 0, "verify errors at {at}: {:?}", r.errors);
}

fn load_dups(db: &Database, n: u32) {
    for i in 0..n {
        db.put(k(i), b"d1").unwrap();
        db.put(k(i), b"d2").unwrap();
    }
}

/// Recovers `img` and asserts every committed dup came back.
fn assert_dups_recovered(img: &Path, n: u32) {
    let env = Environment::open(env_cfg(img, 8)).unwrap();
    let db = env.open_database(None, "dd", &db_cfg(true)).unwrap();
    assert_eq!(scan_all(&db), expected_dups(n), "dup records after recovery");
    assert_eq!(db.count().unwrap(), u64::from(2 * n + 1), "count()");
    verify_clean(&env, "recovered");
    db.close().unwrap();
    env.close().unwrap();
}

/// (e) txn deletes whole keys (both dups each) via `Database::delete_in`,
/// checkpoint in flight, crash -> every dup restored.
#[test]
#[ignore = "A5: dup/secondary delete-all over retained KD slots needs the sorted-dup search KD-skip (integrate f5e310a1 lineage: bbb0ccc0ec/6ea0a5e289/cc5facc649/f5e310a1), deferred from this patch batch; Database::delete_in on a dup key re-finds its own just-KD-marked dup and reports no removal. The scanner fix (this commit) is still required for KD correctness and closes the empty-value and single-dup-cursor cases."]
fn dup_db_txn_delete_whole_keys_crash_restores_all_dups() {
    let dir = tempfile::tempdir().unwrap();
    let n = 20u32;
    let img;
    {
        let env = Environment::open(env_cfg(dir.path(), 8)).unwrap();
        let db = env.open_database(None, "dd", &db_cfg(true)).unwrap();
        load_dups(&db, n);
        assert_eq!(scan_all(&db).len() as u32, 2 * n);
        let txn = env.begin_transaction(None).unwrap();
        for i in (0..n).step_by(2) {
            assert!(db.delete_in(&txn, k(i)).unwrap());
        }
        assert_eq!(db.get_in(&txn, k(0)).unwrap(), None);
        force_ckpt(&env);
        db.put(b"zz", b"x").unwrap();
        img = image(dir.path());
        txn.abort().unwrap();
        assert_eq!(scan_all(&db), expected_dups(n), "live after abort");
        db.close().unwrap();
        env.close().unwrap();
    }
    assert_dups_recovered(img.path(), n);
}

/// (e2) a cursor deletes ONE dup per key in a txn, checkpoint in flight,
/// crash -> every dup restored.
#[test]
fn dup_db_txn_cursor_delete_one_dup_crash_restores_all_dups() {
    let dir = tempfile::tempdir().unwrap();
    let n = 20u32;
    let img;
    {
        let env = Environment::open(env_cfg(dir.path(), 8)).unwrap();
        let db = env.open_database(None, "dd", &db_cfg(true)).unwrap();
        load_dups(&db, n);
        let txn = env.begin_transaction(None).unwrap();
        {
            let mut c = db.open_cursor_in(&txn, None).unwrap();
            for i in 0..n {
                let mut key = DatabaseEntry::from_bytes(&k(i));
                let mut d = DatabaseEntry::from_bytes(b"d1");
                assert_eq!(
                    c.get(&mut key, &mut d, Get::SearchBoth, None).unwrap(),
                    OperationStatus::Success
                );
                c.delete().unwrap();
            }
            c.close().unwrap();
        }
        force_ckpt(&env);
        db.put(b"zz", b"x").unwrap();
        img = image(dir.path());
        txn.abort().unwrap();
        assert_eq!(scan_all(&db), expected_dups(n), "live after abort");
        db.close().unwrap();
        env.close().unwrap();
    }
    assert_dups_recovered(img.path(), n);
}

/// Secondary key = the first 9 data bytes (`val-NNNNN`).
struct DataKey;
impl SecondaryKeyCreator for DataKey {
    fn create_secondary_key(
        &self,
        _db: &Database,
        _key: &DatabaseEntry,
        data: &DatabaseEntry,
        result: &mut DatabaseEntry,
    ) -> bool {
        match data.data_opt() {
            Some(d) if d.len() >= 9 => {
                result.set_data(&d[..9]);
                true
            }
            _ => false,
        }
    }
}

type Opened = (Environment, Arc<Mutex<Database>>, SecondaryDatabase);

fn open_pri_sec(p: &Path) -> Opened {
    let env = Environment::open(env_cfg(p, 8)).unwrap();
    let pri = Arc::new(Mutex::new(
        env.open_database(None, "pri", &db_cfg(false)).unwrap(),
    ));
    let inner = env.open_database(None, "sec", &db_cfg(true)).unwrap();
    let sec = SecondaryDatabase::open(
        Arc::clone(&pri),
        inner,
        SecondaryConfig::new()
            .with_allow_create(true)
            .with_key_creator(Box::new(DataKey)),
    )
    .unwrap();
    (env, pri, sec)
}

fn sec_count(sec: &SecondaryDatabase) -> u32 {
    let mut c = sec.open_cursor(None).unwrap();
    let (mut sk, mut pk, mut d) =
        (DatabaseEntry::new(), DatabaseEntry::new(), DatabaseEntry::new());
    let mut cnt = 0u32;
    let mut st = c.get_first(&mut sk, &mut pk, &mut d).unwrap();
    while st == OperationStatus::Success {
        cnt += 1;
        st = c.get_next(&mut sk, &mut pk, &mut d).unwrap();
    }
    cnt
}

/// (d) txn delete of half the primaries (secondary maintenance runs in the
/// same txn), checkpoint, crash while the txn is open -> primary AND
/// secondary both restored.
#[test]
#[ignore = "A5: dup/secondary delete-all over retained KD slots needs the sorted-dup search KD-skip (integrate f5e310a1 lineage: bbb0ccc0ec/6ea0a5e289/cc5facc649/f5e310a1), deferred from this patch batch; Database::delete_in on a dup key re-finds its own just-KD-marked dup and reports no removal. The scanner fix (this commit) is still required for KD correctness and closes the empty-value and single-dup-cursor cases."]
fn secondary_txn_delete_crash_restores_primary_and_secondary() {
    let dir = tempfile::tempdir().unwrap();
    let n = 40u32;
    let img;
    {
        let (env, pri, sec) = open_pri_sec(dir.path());
        for i in 0..n {
            pri.lock().put(k(i), v(i)).unwrap();
        }
        assert_eq!(sec_count(&sec), n);
        let txn = env.begin_transaction(None).unwrap();
        for i in (0..n).step_by(2) {
            assert!(pri.lock().delete_in(&txn, k(i)).unwrap());
        }
        force_ckpt(&env);
        pri.lock().put(b"zz-after", b"zzzzzzzzzz").unwrap();
        img = image(dir.path());
        txn.abort().unwrap();
        assert_eq!(sec_count(&sec), n + 1, "live secondary after abort");
        sec.close().unwrap();
        pri.lock().close().unwrap();
        env.close().unwrap();
    }
    let (env, pri, sec) = open_pri_sec(img.path());
    for i in 0..n {
        assert_eq!(
            pri.lock().get(k(i)).unwrap().as_deref(),
            Some(v(i).as_slice()),
            "primary {i}"
        );
    }
    assert_eq!(sec_count(&sec), n + 1, "secondary entries after recovery");
    for i in 0..n {
        let (mut pk, mut d) = (DatabaseEntry::new(), DatabaseEntry::new());
        assert!(
            sec.get_into(None, &v(i)[..9], &mut pk, &mut d).unwrap(),
            "sec lookup {i}"
        );
        assert_eq!(pk.data_opt(), Some(k(i).as_slice()), "sec -> pri {i}");
    }
    verify_clean(&env, "recovered");
    sec.close().unwrap();
    pri.lock().close().unwrap();
    env.close().unwrap();
}

/// Same root cause on a plain (non-dup) DB: a committed record with an EMPTY
/// value, deleted by an in-flight txn, checkpoint, crash -> the record comes
/// back with its empty value.  The LN log format writes the same 0 length for
/// empty data and for a deletion, so only the entry type tells them apart.
#[test]
fn empty_value_txn_delete_crash_restores_record() {
    let dir = tempfile::tempdir().unwrap();
    let img;
    {
        let env = Environment::open(env_cfg(dir.path(), 8)).unwrap();
        let db = env.open_database(None, "d", &db_cfg(false)).unwrap();
        db.put(b"empty", b"").unwrap();
        db.put(b"full", b"v").unwrap();
        let txn = env.begin_transaction(None).unwrap();
        assert!(db.delete_in(&txn, b"empty").unwrap());
        assert!(db.delete_in(&txn, b"full").unwrap());
        force_ckpt(&env);
        db.put(b"zz", b"x").unwrap();
        img = image(dir.path());
        txn.abort().unwrap();
        assert_eq!(db.get(b"empty").unwrap().as_deref(), Some(&b""[..]));
        db.close().unwrap();
        env.close().unwrap();
    }
    let env = Environment::open(env_cfg(img.path(), 8)).unwrap();
    let db = env.open_database(None, "d", &db_cfg(false)).unwrap();
    assert_eq!(db.get(b"full").unwrap().as_deref(), Some(&b"v"[..]));
    assert_eq!(
        db.get(b"empty").unwrap().as_deref(),
        Some(&b""[..]),
        "committed empty-value record after recovery"
    );
    verify_clean(&env, "recovered");
    db.close().unwrap();
    env.close().unwrap();
}
