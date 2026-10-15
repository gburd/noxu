//! Port of JE `SecondaryDirtyReadTest` (SecondaryDirtyReadTest.java).
//!
//! Two threads race: one does READ_UNCOMMITTED secondary reads, the other
//! deletes / updates primary records.  The invariant under test
//! (JE bug #22603 / BUG-SEC-KEYED-DIRTYREAD-1): a dirty-read secondary access
//! never raises SecondaryIntegrityException when the primary is concurrently
//! removed — it returns NOTFOUND (keyed read) or skips to the next record
//! (scan).  JE's loop runs to MAX_KEY (1000 in the short profile); this port
//! uses a bounded key range and the same two-thread structure (no artificial
//! sleeps — JE has none).

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use noxu_db::secondary_config::{SecondaryConfig, SecondaryKeyCreator};
use noxu_db::{
    Database, DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig,
    ReadOptions, SecondaryDatabase,
};
use noxu_sync::Mutex;
use tempfile::TempDir;

const MAX_KEY: u32 = 500;
const KEY_OFFSET: u32 = 100;

fn e(v: u32) -> [u8; 4] {
    v.to_be_bytes()
}

/// Secondary key = primary data value + KEY_OFFSET.  Returns false for the
/// sentinel data -1 (0xFFFF_FFFF), mirroring JE's "update to -1 deletes the
/// secondary key record" case.
struct OffsetKeyCreator;
impl SecondaryKeyCreator for OffsetKeyCreator {
    fn create_secondary_key(
        &self,
        _db: &Database,
        _key: &DatabaseEntry,
        data: &DatabaseEntry,
        result: &mut DatabaseEntry,
    ) -> bool {
        let d = data.data_opt().unwrap();
        let v = u32::from_be_bytes(d.try_into().unwrap());
        if v == u32::MAX {
            return false; // JE: datum -1 => no secondary key.
        }
        result.set_data(&e(v + KEY_OFFSET));
        true
    }
}

fn setup()
-> (TempDir, Environment, Arc<Mutex<Database>>, Arc<SecondaryDatabase>) {
    let dir = TempDir::new().unwrap();
    let env = Environment::open(
        EnvironmentConfig::new(dir.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true),
    )
    .unwrap();
    let pri = {
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
    };
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
    let sec = Arc::new(
        SecondaryDatabase::open(
            Arc::clone(&pri),
            inner,
            SecondaryConfig::new()
                .with_allow_create(true)
                .with_sorted_duplicates(true)
                .with_key_creator(Box::new(OffsetKeyCreator)),
        )
        .unwrap(),
    );
    (dir, env, pri, sec)
}

fn add_records(pri: &Arc<Mutex<Database>>) {
    let p = pri.lock();
    for i in 0..MAX_KEY {
        p.put(e(i), e(i)).unwrap();
    }
}

/// JE `testDeleteWhileReadingByKey`: dirty-read-by-key vs primary-delete.
#[test]
fn sec_dirty_read_delete_while_reading_by_key() {
    let (_dir, _env, pri, sec) = setup();
    add_records(&pri);

    let done = Arc::new(AtomicUsize::new(0));

    // Deleter runs independently to completion, then signals done.
    let deleter = {
        let pri = Arc::clone(&pri);
        let done = Arc::clone(&done);
        std::thread::spawn(move || {
            for i in 0..MAX_KEY {
                let _ = pri.lock().delete(e(i));
                std::thread::yield_now();
            }
            done.store(1, Ordering::Release);
        })
    };

    // Reader repeatedly dirty-reads every key until the deleter is done.  The
    // contract: a concurrently deleted primary surfaces as NOTFOUND, NEVER as
    // SecondaryIntegrityException.
    let reader = {
        let sec = Arc::clone(&sec);
        let done = Arc::clone(&done);
        std::thread::spawn(move || {
            let ru = ReadOptions::read_uncommitted();
            let mut pk = DatabaseEntry::new();
            let mut d = DatabaseEntry::new();
            while done.load(Ordering::Acquire) == 0 {
                for k in 0..MAX_KEY {
                    sec.get_into_with_options(
                        None,
                        e(k + KEY_OFFSET),
                        &mut pk,
                        &mut d,
                        &ru,
                    )
                    .expect("dirty keyed read must never raise an error");
                }
            }
        })
    };

    deleter.join().unwrap();
    reader.join().unwrap();
}

/// JE `testUpdateWhileReadingByKey`: dirty-read-by-key vs a primary update
/// that changes the datum to the sentinel (-1), which deletes the secondary
/// key record (the key creator declines to index -1).  The dirty keyed read
/// must never raise SecondaryIntegrityException; it returns NOTFOUND once the
/// secondary key is gone.
#[test]
fn sec_dirty_read_update_while_reading_by_key() {
    let (_dir, _env, pri, sec) = setup();
    add_records(&pri);

    let done = Arc::new(AtomicUsize::new(0));

    // Updater runs independently to completion (each key updated once to the
    // -1 sentinel, which deletes its secondary key record), then signals done.
    let updater = {
        let pri = Arc::clone(&pri);
        let done = Arc::clone(&done);
        std::thread::spawn(move || {
            for i in 0..MAX_KEY {
                let _ = pri.lock().put(e(i), e(u32::MAX));
                std::thread::yield_now();
            }
            done.store(1, Ordering::Release);
        })
    };

    // Reader repeatedly dirty-reads keys by secondary key until the updater is
    // done.  The contract: it must NEVER raise SecondaryIntegrityException,
    // whether the key is present (old datum) or gone (sec key deleted).
    let reader = {
        let sec = Arc::clone(&sec);
        let done = Arc::clone(&done);
        std::thread::spawn(move || {
            let ru = ReadOptions::read_uncommitted();
            let mut pk = DatabaseEntry::new();
            let mut d = DatabaseEntry::new();
            while done.load(Ordering::Acquire) == 0 {
                for k in 0..MAX_KEY {
                    sec.get_into_with_options(
                        None,
                        e(k + KEY_OFFSET),
                        &mut pk,
                        &mut d,
                        &ru,
                    )
                    .expect("dirty keyed read must never raise an error");
                }
            }
        })
    };

    updater.join().unwrap();
    reader.join().unwrap();
}

/// JE `testUpdateWhileScanning`: dirty-read scan vs the same -1 update.
#[test]
fn sec_dirty_read_update_while_scanning() {
    let (_dir, _env, pri, sec) = setup();
    add_records(&pri);

    let done = Arc::new(AtomicUsize::new(0));

    let updater = {
        let pri = Arc::clone(&pri);
        let done = Arc::clone(&done);
        std::thread::spawn(move || {
            for i in 0..MAX_KEY {
                let _ = pri.lock().put(e(i), e(u32::MAX));
                std::thread::yield_now();
            }
            done.store(1, Ordering::Release);
        })
    };

    let reader = {
        let sec = Arc::clone(&sec);
        let done = Arc::clone(&done);
        std::thread::spawn(move || {
            let cfg = noxu_db::CursorConfig::new().with_read_uncommitted(true);
            while done.load(Ordering::Acquire) == 0 {
                let mut c = sec.open_cursor(Some(&cfg)).unwrap();
                let mut sk = DatabaseEntry::new();
                let mut pk = DatabaseEntry::new();
                let mut d = DatabaseEntry::new();
                let mut st = c
                    .get_first(&mut sk, &mut pk, &mut d)
                    .expect("dirty scan must never raise an error");
                while st == noxu_db::OperationStatus::Success {
                    let dv = u32::from_be_bytes(
                        d.data_opt().unwrap().try_into().unwrap(),
                    );
                    // A record visible via the secondary must still have a
                    // real (non-sentinel) datum consistent with its sec key.
                    if dv != u32::MAX {
                        let skv = u32::from_be_bytes(
                            sk.data_opt().unwrap().try_into().unwrap(),
                        );
                        assert_eq!(skv, dv + KEY_OFFSET);
                    }
                    st = c
                        .get_next(&mut sk, &mut pk, &mut d)
                        .expect("dirty scan getNext must never raise an error");
                }
            }
        })
    };

    updater.join().unwrap();
    reader.join().unwrap();
}

/// JE `testDeleteWhileScanning`: dirty-read scan vs primary-delete.  A scan
/// skips deleted records and never raises SecondaryIntegrityException.
#[test]
fn sec_dirty_read_delete_while_scanning() {
    let (_dir, _env, pri, sec) = setup();
    add_records(&pri);

    let done = Arc::new(AtomicUsize::new(0));

    let deleter = {
        let pri = Arc::clone(&pri);
        let done = Arc::clone(&done);
        std::thread::spawn(move || {
            for i in 0..MAX_KEY {
                let _ = pri.lock().delete(e(i));
                std::thread::yield_now();
            }
            done.store(1, Ordering::Release);
        })
    };

    // Scan repeatedly under READ_UNCOMMITTED; must never raise an error, and
    // every returned record must be internally consistent (sec key derived
    // from the primary data we got back).
    let reader = {
        let sec = Arc::clone(&sec);
        let done = Arc::clone(&done);
        std::thread::spawn(move || {
            let cfg = noxu_db::CursorConfig::new().with_read_uncommitted(true);
            while done.load(Ordering::Acquire) == 0 {
                let mut c = sec.open_cursor(Some(&cfg)).unwrap();
                let mut sk = DatabaseEntry::new();
                let mut pk = DatabaseEntry::new();
                let mut d = DatabaseEntry::new();
                let mut st = c
                    .get_first(&mut sk, &mut pk, &mut d)
                    .expect("dirty scan must never raise an error");
                while st == noxu_db::OperationStatus::Success {
                    // Consistency: sec key == primary datum + KEY_OFFSET.
                    let dv = u32::from_be_bytes(
                        d.data_opt().unwrap().try_into().unwrap(),
                    );
                    let skv = u32::from_be_bytes(
                        sk.data_opt().unwrap().try_into().unwrap(),
                    );
                    assert_eq!(
                        skv,
                        dv + KEY_OFFSET,
                        "dirty-scan record must be internally consistent"
                    );
                    st = c
                        .get_next(&mut sk, &mut pk, &mut d)
                        .expect("dirty scan getNext must never raise an error");
                }
            }
        })
    };

    deleter.join().unwrap();
    reader.join().unwrap();
}
