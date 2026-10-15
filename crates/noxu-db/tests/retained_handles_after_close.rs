//! R1 (re-review of fix/ro-envlock): operations on a cursor the caller kept
//! after `Database::close` / `Environment::close` must fail, never return
//! wrong data or write.  JE: every Cursor operation runs `checkOpen` ->
//! `checkEnv` (EnvironmentImpl.checkIfInvalid) + `dbHandle.checkOpen()`
//! (Cursor.java:5381-5403), and `Database.checkOpen` throws
//! IllegalStateException("Database was closed.") (Database.java:2367-2372).
use noxu_db::{
    CheckpointConfig, DatabaseConfig, DatabaseEntry, Environment,
    EnvironmentConfig, NoxuError, OperationStatus, Put, SecondaryConfig,
    SecondaryDatabase, SecondaryKeyCreator,
};
use noxu_sync::Mutex;
use std::{collections::BTreeMap, fs, path::Path, sync::Arc};

fn writer(dir: &Path) -> Environment {
    let mut cfg = EnvironmentConfig::new(dir.to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    cfg.set_run_cleaner(false);
    cfg.set_run_checkpointer(false);
    cfg.set_run_in_compressor(false);
    cfg.set_stats_collect(false);
    Environment::open(cfg).unwrap()
}

fn dbcfg() -> DatabaseConfig {
    DatabaseConfig::new().with_allow_create(true).with_transactional(true)
}

fn val(i: u32) -> Vec<u8> {
    let mut v = vec![5u8; 300];
    v[..4].copy_from_slice(&i.to_be_bytes());
    v
}

/// Writes 50 non-embedded records, checkpoints and closes, so a reopened
/// env must fetch LN data from the log.
fn seed(dir: &Path) {
    let w = writer(dir);
    let d = w.open_database(None, "foo", &dbcfg()).unwrap();
    for i in 0..50u32 {
        d.put(i.to_be_bytes(), val(i)).unwrap();
    }
    d.close().unwrap();
    w.checkpoint(Some(&CheckpointConfig::new().with_force(true))).unwrap();
    w.close().unwrap();
}

fn snap(dir: &Path) -> BTreeMap<String, Vec<u8>> {
    fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "ndb"))
        .map(|p| {
            (
                p.file_name().unwrap().to_string_lossy().into_owned(),
                fs::read(&p).unwrap(),
            )
        })
        .collect()
}

#[test]
fn rw_retained_cursor_read_after_close_errors() {
    let dir = tempfile::tempdir().unwrap();
    seed(dir.path());
    let env = writer(dir.path());
    let db = env.open_database(None, "foo", &dbcfg()).unwrap();
    let mut c = db.open_cursor(None).unwrap();
    // Control: normal cursor reads return full values.
    let (_, v0) = c.next().unwrap().unwrap();
    assert_eq!(&v0[..], &val(0)[..]);
    db.close().unwrap();
    // Closed database, env still open: JE Database.checkOpen -> ISE.
    let r = c.next();
    assert!(
        r.is_err(),
        "cursor read after Database::close: {:?}",
        r.map(|o| o.map(|(k, v)| (k.len(), v.len())))
    );
    env.close().unwrap();
    for _ in 0..2 {
        let r = c.next();
        assert!(
            r.is_err(),
            "cursor read after Environment::close: {:?}",
            r.map(|o| o.map(|(k, v)| (k.len(), v.len())))
        );
    }
    assert!(c.seek(3u32.to_be_bytes()).is_err());
    assert!(c.count().is_err());
    assert!(db.get(1u32.to_be_bytes()).is_err());
    // Close after env close is allowed (JE Cursor.close checks env only) and
    // is idempotent.
    c.close().unwrap();
    c.close().unwrap();
    drop(c);
    drop(db);
    drop(env);
    // Lock released: a fresh RW open succeeds and the data is intact.
    let env = writer(dir.path());
    let db = env.open_database(None, "foo", &dbcfg()).unwrap();
    assert_eq!(
        db.get(7u32.to_be_bytes()).unwrap().as_deref(),
        Some(&val(7)[..])
    );
    db.close().unwrap();
    env.close().unwrap();
}

#[test]
fn rw_retained_cursor_write_after_close_errors_and_writes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    seed(dir.path());
    let env = writer(dir.path());
    let db = env.open_database(None, "foo", &dbcfg()).unwrap();
    let mut c = db.open_cursor(None).unwrap();
    assert!(c.next().unwrap().is_some());
    db.close().unwrap();
    env.close().unwrap();
    let before = snap(dir.path());
    let k = DatabaseEntry::from_bytes(&999u32.to_be_bytes());
    let d = DatabaseEntry::from_bytes(&val(999));
    for p in [Put::Overwrite, Put::NoOverwrite, Put::Current] {
        let r = c.put(&k, &d, p);
        assert!(r.is_err(), "cursor put {p:?} after close: {r:?}");
    }
    let r = c.delete();
    assert!(r.is_err(), "cursor delete after close: {r:?}");
    drop(c);
    drop(db);
    drop(env);
    assert_eq!(snap(dir.path()), before, "retained cursor wrote to the log");
    let env = writer(dir.path());
    let db = env.open_database(None, "foo", &dbcfg()).unwrap();
    assert_eq!(db.get(999u32.to_be_bytes()).unwrap(), None);
    assert_eq!(
        db.get(0u32.to_be_bytes()).unwrap().as_deref(),
        Some(&val(0)[..])
    );
    db.close().unwrap();
    env.close().unwrap();
}

struct FirstByte;
impl SecondaryKeyCreator for FirstByte {
    fn create_secondary_key(
        &self,
        _db: &noxu_db::Database,
        _key: &DatabaseEntry,
        data: &DatabaseEntry,
        result: &mut DatabaseEntry,
    ) -> bool {
        match data.data_opt() {
            Some(d) if d.len() > 4 => {
                result.set_data(&d[..4]);
                true
            }
            _ => false,
        }
    }
}

#[test]
fn retained_secondary_cursor_after_close_errors() {
    let dir = tempfile::tempdir().unwrap();
    let env = writer(dir.path());
    let primary =
        Arc::new(Mutex::new(env.open_database(None, "p", &dbcfg()).unwrap()));
    let sdb = env
        .open_database(None, "s", &dbcfg().with_sorted_duplicates(true))
        .unwrap();
    let sec = SecondaryDatabase::open(
        Arc::clone(&primary),
        sdb,
        SecondaryConfig::new()
            .with_allow_create(true)
            .with_key_creator(Box::new(FirstByte)),
    )
    .unwrap();
    for i in 0..20u32 {
        primary.lock().put(i.to_be_bytes(), val(i)).unwrap();
    }
    let mut sc = sec.open_cursor(None).unwrap();
    let (mut k, mut pk, mut d) =
        (DatabaseEntry::new(), DatabaseEntry::new(), DatabaseEntry::new());
    assert_eq!(
        sc.get_next(&mut k, &mut pk, &mut d).unwrap(),
        OperationStatus::Success
    );
    assert_eq!(d.data(), &val(0)[..]);
    sec.close().unwrap();
    primary.lock().close().unwrap();
    env.close().unwrap();
    let r = sc.get_next(&mut k, &mut pk, &mut d);
    assert!(r.is_err(), "secondary cursor after close: {r:?}");
    let _ = sc.close();
}

/// Requirement 2: the FileManager-close added for the RO env-lock fix must
/// not change the normal close path.  A clean `Environment::close` still
/// returns Ok; the public handle still refuses a *second* close with
/// `EnvironmentClosed` (JE `Environment.close` throws on double-close — this
/// is pre-existing, unchanged behaviour); and close-then-drop must not panic
/// or double-free the FileManager (the inner `EnvironmentImpl` close is
/// guarded by its env-state flag, so the FileManager is closed exactly once).
#[test]
fn environment_close_normal_path_unchanged_and_drop_is_safe() {
    let dir = tempfile::tempdir().unwrap();
    seed(dir.path());
    let env = writer(dir.path());
    {
        let db = env.open_database(None, "foo", &dbcfg()).unwrap();
        assert_eq!(
            db.get(0u32.to_be_bytes()).unwrap().as_deref(),
            Some(&val(0)[..])
        );
        db.close().unwrap();
    }
    // First close: Ok (and closes the FileManager).
    env.close().unwrap();
    // Second close on the public handle: refused with EnvironmentClosed
    // (unchanged tag behaviour), never a panic or double-free.
    assert!(matches!(env.close(), Err(NoxuError::EnvironmentClosed)));
    // close-then-drop: Drop must not panic or double-free the FileManager.
    drop(env);
    // The env lock is released: a fresh writer opens and the data is intact.
    let env = writer(dir.path());
    let db = env.open_database(None, "foo", &dbcfg()).unwrap();
    assert_eq!(
        db.get(3u32.to_be_bytes()).unwrap().as_deref(),
        Some(&val(3)[..])
    );
    db.close().unwrap();
    env.close().unwrap();
}
