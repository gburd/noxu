//! NEW-CURSOR-SEC-MAINT — a public `Cursor::put` / `Cursor::delete` on a
//! primary that has registered secondaries must maintain those secondaries,
//! exactly as `Database::put` / `Database::delete` do.
//!
//! Before the fix, `Cursor::put` and `Cursor::delete` carried no
//! secondary-hook fan-out: writing through a cursor left every secondary
//! index stale (a lookup by the secondary key did not find the record; a
//! REPLACE through a cursor left the OLD secondary key still pointing at the
//! primary key).  JE `Cursor.putInternal` runs the same secondary
//! maintenance for cursor puts that `Database.put` runs, via
//! `SecondaryTrigger`/`putNotify`.
//!
//! Data model (mirrors `je_secondary_test.rs`): the primary key and data are
//! single-byte arrays; the secondary key is `data value + KEY_OFFSET` (100).

use noxu_db::{
    Database, DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig,
    Put, SecondaryConfig, SecondaryDatabase, SecondaryKeyCreator,
};
use noxu_sync::Mutex;
use std::sync::Arc;
use tempfile::TempDir;

const KEY_OFFSET: u8 = 100;

/// secondary key = (data value + KEY_OFFSET).  Data is a single byte, so
/// sec key = `[data[0] + 100]`.
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

fn open_secondary(
    primary: Arc<Mutex<Database>>,
    env: &Environment,
    name: &str,
) -> SecondaryDatabase {
    let inner = env
        .open_database(
            None,
            name,
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true)
                .with_sorted_duplicates(true),
        )
        .unwrap();
    SecondaryDatabase::open(
        primary,
        inner,
        SecondaryConfig::new()
            .with_allow_create(true)
            .with_key_creator(Box::new(OffsetKeyCreator)),
    )
    .unwrap()
}

/// Resolve `sec_key` through the secondary, returning the primary data if the
/// index still maps it.
fn lookup(sec: &SecondaryDatabase, sec_key: u8) -> Option<Vec<u8>> {
    let mut pk = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    if sec.get_into(None, [sec_key], &mut pk, &mut d).unwrap() {
        Some(d.data_opt().unwrap_or(&[]).to_vec())
    } else {
        None
    }
}

/// Cursor put (fresh insert) must maintain the secondary.
#[test]
fn cursor_put_insert_maintains_secondary() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let primary = open_primary(&env, "p");
    let sec = open_secondary(Arc::clone(&primary), &env, "s");

    {
        let pri = primary.lock();
        let mut cur = pri.open_cursor(None).unwrap();
        cur.put(
            &DatabaseEntry::from_bytes(b"k"),
            &DatabaseEntry::from_bytes(&[5]),
            Put::Overwrite,
        )
        .unwrap();
        cur.close().unwrap();
    }

    // Secondary key = 5 + 100 = 105 must now resolve to the primary data.
    assert_eq!(
        lookup(&sec, 105),
        Some(vec![5]),
        "a cursor put(insert) must maintain the secondary index"
    );
}

/// Cursor put_no_overwrite (NoOverwrite insert) must maintain the secondary.
#[test]
fn cursor_put_no_overwrite_maintains_secondary() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let primary = open_primary(&env, "p");
    let sec = open_secondary(Arc::clone(&primary), &env, "s");

    {
        let pri = primary.lock();
        let mut cur = pri.open_cursor(None).unwrap();
        cur.put(
            &DatabaseEntry::from_bytes(b"k"),
            &DatabaseEntry::from_bytes(&[7]),
            Put::NoOverwrite,
        )
        .unwrap();
        cur.close().unwrap();
    }

    assert_eq!(
        lookup(&sec, 107),
        Some(vec![7]),
        "a cursor put_no_overwrite(insert) must maintain the secondary index"
    );
}

/// Cursor REPLACE (put over an existing key) must MOVE the secondary key
/// old->new: the old secondary key must no longer resolve, the new one must.
#[test]
fn cursor_put_replace_moves_secondary_key() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let primary = open_primary(&env, "p");
    let sec = open_secondary(Arc::clone(&primary), &env, "s");

    // Seed via Database::put (known-good) so we isolate the REPLACE path.
    {
        let pri = primary.lock();
        pri.put(b"k", [5]).unwrap();
    }
    assert_eq!(lookup(&sec, 105), Some(vec![5]), "seed maps 105->5");

    // REPLACE the data via a cursor put: 5 -> 9, so sec key 105 -> 109.
    {
        let pri = primary.lock();
        let mut cur = pri.open_cursor(None).unwrap();
        cur.put(
            &DatabaseEntry::from_bytes(b"k"),
            &DatabaseEntry::from_bytes(&[9]),
            Put::Overwrite,
        )
        .unwrap();
        cur.close().unwrap();
    }

    assert_eq!(
        lookup(&sec, 105),
        None,
        "cursor REPLACE must delete the OLD secondary key entry"
    );
    assert_eq!(
        lookup(&sec, 109),
        Some(vec![9]),
        "cursor REPLACE must insert the NEW secondary key entry"
    );
}

/// Cursor put_current (replace the data at the current position) must
/// maintain the secondary (move old->new).
#[test]
fn cursor_put_current_maintains_secondary() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let primary = open_primary(&env, "p");
    let sec = open_secondary(Arc::clone(&primary), &env, "s");

    {
        let pri = primary.lock();
        pri.put(b"k", [5]).unwrap();
    }
    assert_eq!(lookup(&sec, 105), Some(vec![5]), "seed maps 105->5");

    {
        let pri = primary.lock();
        let mut cur = pri.open_cursor(None).unwrap();
        // Position on the key, then replace its data via put_current.
        let mut k = DatabaseEntry::from_bytes(b"k");
        let mut v = DatabaseEntry::new();
        let st = cur
            .get(&mut k, &mut v, noxu_db::Get::Search, None)
            .unwrap();
        assert_eq!(st, noxu_db::OperationStatus::Success);
        cur.put(
            &DatabaseEntry::from_bytes(b"k"),
            &DatabaseEntry::from_bytes(&[9]),
            Put::Current,
        )
        .unwrap();
        cur.close().unwrap();
    }

    assert_eq!(
        lookup(&sec, 105),
        None,
        "cursor put_current must delete the OLD secondary key entry"
    );
    assert_eq!(
        lookup(&sec, 109),
        Some(vec![9]),
        "cursor put_current must insert the NEW secondary key entry"
    );
}

/// Cursor delete must remove the secondary entry (JE
/// `Cursor.deleteInternal` -> secondary trigger delete fan-out).
#[test]
fn cursor_delete_maintains_secondary() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let primary = open_primary(&env, "p");
    let sec = open_secondary(Arc::clone(&primary), &env, "s");

    {
        let pri = primary.lock();
        pri.put(b"k", [5]).unwrap();
    }
    assert_eq!(lookup(&sec, 105), Some(vec![5]), "seed maps 105->5");

    {
        let pri = primary.lock();
        let mut cur = pri.open_cursor(None).unwrap();
        let mut k = DatabaseEntry::from_bytes(b"k");
        let mut v = DatabaseEntry::new();
        let st = cur
            .get(&mut k, &mut v, noxu_db::Get::Search, None)
            .unwrap();
        assert_eq!(st, noxu_db::OperationStatus::Success);
        cur.delete().unwrap();
        cur.close().unwrap();
    }

    assert_eq!(
        lookup(&sec, 105),
        None,
        "a cursor delete must remove the dangling secondary index entry"
    );
}

/// Under an explicit transaction, a cursor put must maintain the secondary
/// atomically (abort rolls back BOTH the primary and the secondary).
#[test]
fn cursor_put_under_txn_maintains_secondary_and_aborts_together() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let primary = open_primary(&env, "p");
    let sec = open_secondary(Arc::clone(&primary), &env, "s");

    // Commit path: secondary is maintained.
    {
        let pri = primary.lock();
        let txn = env.begin_transaction(None).unwrap();
        let mut cur = pri.open_cursor_in(&txn, None).unwrap();
        cur.put(
            &DatabaseEntry::from_bytes(b"c"),
            &DatabaseEntry::from_bytes(&[5]),
            Put::Overwrite,
        )
        .unwrap();
        cur.close().unwrap();
        txn.commit().unwrap();
    }
    assert_eq!(lookup(&sec, 105), Some(vec![5]), "committed cursor put maps 105->5");

    // Abort path: neither primary nor secondary sees the write.
    {
        let pri = primary.lock();
        let txn = env.begin_transaction(None).unwrap();
        let mut cur = pri.open_cursor_in(&txn, None).unwrap();
        cur.put(
            &DatabaseEntry::from_bytes(b"a"),
            &DatabaseEntry::from_bytes(&[8]),
            Put::Overwrite,
        )
        .unwrap();
        cur.close().unwrap();
        txn.abort().unwrap();
    }
    assert_eq!(
        lookup(&sec, 108),
        None,
        "an aborted cursor put must leave the secondary index untouched"
    );
}
