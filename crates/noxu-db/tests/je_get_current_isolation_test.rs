//! `Get::Current` must re-read the current slot under the BIN latch and lock
//! it per the cursor's isolation, mirroring JE `CursorImpl.lockAndGetCurrent`
//! / `getCurrent` (CursorImpl.java:1983-2095, getCurrent :2230) reached from
//! `Cursor.getCurrentNoDups` (Cursor.java:3062-3092). The divergence this file
//! pins (freebsd-lock-error-map-rereview2.md COORDINATOR ADDENDUM item 3,
//! probe rr2_probe `cur_upd_*`): the old code returned the cursor's *cached*
//! key/data without re-reading the slot or locking, so:
//!
//!   * a DEFAULT/READ_COMMITTED reader did NOT block on another txn's
//!     uncommitted write-locked slot (JE takes LockType.READ, which conflicts
//!     with the writer's WRITE lock and blocks/times out — getLockType,
//!     Cursor.java:4260-4276);
//!   * a READ_UNCOMMITTED cursor did NOT see the uncommitted new value (JE
//!     uses LockType.NONE but still re-reads the live slot, so it returns the
//!     dirty value — lockLNAndCheckDefunct case 3, CursorImpl.java:2062-2095).
//!
//! All scenarios here are deterministic (no sleeps/threads): the writer's
//! update/delete is applied before Get::Current runs, so the outcome is fixed.

use noxu_db::{
    CursorConfig, Database, DatabaseConfig, DatabaseEntry, Environment,
    EnvironmentConfig, Get, NoxuError, OperationStatus, Transaction,
    TransactionConfig,
};
use tempfile::TempDir;

const K: &[u8] = b"k";
const V_OLD: &[u8] = b"v";
const V_NEW: &[u8] = b"u";

fn open_env(dir: &TempDir) -> Environment {
    Environment::open(
        EnvironmentConfig::new(dir.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true)
            .with_lock_timeout(50),
    )
    .unwrap()
}

fn open_db(env: &Environment, name: &str, dups: bool) -> Database {
    env.open_database(
        None,
        name,
        &DatabaseConfig::new()
            .with_allow_create(true)
            .with_transactional(true)
            .with_sorted_duplicates(dups),
    )
    .unwrap()
}

fn writer_txn(env: &Environment) -> Transaction {
    env.begin_transaction(Some(
        &TransactionConfig::new().with_lock_timeout_ms(50),
    ))
    .unwrap()
}

#[track_caller]
fn assert_lock_timeout<T: std::fmt::Debug>(
    r: std::result::Result<T, NoxuError>,
    what: &str,
) {
    match r {
        Err(e) if e.is_lock_timeout() && e.is_retryable() => {}
        other => panic!(
            "{what}: expected typed LockTimeout (JE LockTimeoutException), \
             got {other:?}"
        ),
    }
}

#[track_caller]
fn current(
    c: &mut noxu_db::Cursor,
) -> Result<(OperationStatus, Option<Vec<u8>>)> {
    let (mut k, mut v) = (DatabaseEntry::new(), DatabaseEntry::new());
    let s = c.get(&mut k, &mut v, Get::Current, None)?;
    Ok((s, v.data_opt().map(|b| b.to_vec())))
}

type Result<T> = std::result::Result<T, NoxuError>;

// ─────────────────────────────────────────────────────────────────────────
// UPDATE (the primary divergence, probe cur_upd_*)
// ─────────────────────────────────────────────────────────────────────────

/// DEFAULT (repeatable-read) reader, foreign uncommitted UPDATE: Get::Current
/// takes LockType.READ, conflicts with the writer's WRITE lock, and times out.
/// (A repeatable-read reader that positioned with `seek` would ALSO already
/// hold a read lock that blocks the writer; to isolate the Get::Current lock,
/// the reader uses read-committed so it drops the read lock after `seek`, and
/// Get::Current must re-acquire+block — same WRITE/READ conflict JE sees.)
#[test]
fn get_current_default_over_foreign_uncommitted_update_blocks() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let db = open_db(&env, "db", false);
    db.put(K, V_OLD).unwrap();
    let reader = env
        .begin_transaction(Some(
            &TransactionConfig::new()
                .with_read_committed(true)
                .with_lock_timeout_ms(50),
        ))
        .unwrap();
    let mut c = db.open_cursor_in(&reader, None).unwrap();
    assert!(c.seek(K).unwrap().is_some());
    let w = writer_txn(&env);
    assert_ne!(reader.id(), w.id(), "fixture: distinct lockers");
    db.put_in(&w, K, V_NEW).unwrap();
    let r = current(&mut c);
    c.close().unwrap();
    reader.abort().unwrap();
    w.abort().unwrap();
    assert_lock_timeout(
        r,
        "Get::Current (default reader) over foreign uncommitted update",
    );
}

/// READ_UNCOMMITTED cursor, foreign uncommitted UPDATE: no lock (LockType.NONE)
/// but Get::Current RE-READS the live slot, so it returns the uncommitted new
/// value — not the cached old value. This is the exact bug: pre-fix the cached
/// `V_OLD` was returned.
#[test]
fn get_current_ru_over_foreign_uncommitted_update_sees_new_value() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let db = open_db(&env, "db", false);
    db.put(K, V_OLD).unwrap();
    let reader = writer_txn(&env);
    let cc = CursorConfig::read_uncommitted();
    let mut c = db.open_cursor_in(&reader, Some(&cc)).unwrap();
    assert_eq!(c.seek(K).unwrap().as_deref(), Some(V_OLD));
    let w = writer_txn(&env);
    assert_ne!(reader.id(), w.id());
    db.put_in(&w, K, V_NEW).unwrap();
    let (s, v) = current(&mut c).unwrap();
    c.close().unwrap();
    reader.abort().unwrap();
    w.abort().unwrap();
    assert_eq!(s, OperationStatus::Success);
    assert_eq!(
        v.as_deref(),
        Some(V_NEW),
        "READ_UNCOMMITTED Get::Current must re-read and see the \
         uncommitted new value (JE lockLNAndCheckDefunct case 3)"
    );
}

/// After the foreign writer COMMITS its update, a fresh Get::Current re-read
/// returns the new committed value (reader is read-committed so it is not
/// repeatable-read-pinned to the old value).
#[test]
fn get_current_over_committed_update_sees_new_value() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let db = open_db(&env, "db", false);
    db.put(K, V_OLD).unwrap();
    let reader = env
        .begin_transaction(Some(
            &TransactionConfig::new().with_read_committed(true),
        ))
        .unwrap();
    let mut c = db.open_cursor_in(&reader, None).unwrap();
    assert_eq!(c.seek(K).unwrap().as_deref(), Some(V_OLD));
    let w = writer_txn(&env);
    db.put_in(&w, K, V_NEW).unwrap();
    w.commit().unwrap();
    let (s, v) = current(&mut c).unwrap();
    c.close().unwrap();
    reader.abort().unwrap();
    assert_eq!(s, OperationStatus::Success);
    assert_eq!(v.as_deref(), Some(V_NEW));
}

/// After the foreign writer ABORTS its update, Get::Current re-read returns
/// the restored old value.
#[test]
fn get_current_after_foreign_update_abort_sees_old_value() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let db = open_db(&env, "db", false);
    db.put(K, V_OLD).unwrap();
    let reader = env
        .begin_transaction(Some(
            &TransactionConfig::new().with_read_committed(true),
        ))
        .unwrap();
    let mut c = db.open_cursor_in(&reader, None).unwrap();
    assert_eq!(c.seek(K).unwrap().as_deref(), Some(V_OLD));
    let w = writer_txn(&env);
    db.put_in(&w, K, V_NEW).unwrap();
    w.abort().unwrap();
    let (s, v) = current(&mut c).unwrap();
    c.close().unwrap();
    reader.abort().unwrap();
    assert_eq!(s, OperationStatus::Success);
    assert_eq!(v.as_deref(), Some(V_OLD));
}

/// Own-txn UPDATE then Get::Current: the cursor's own txn holds the WRITE
/// lock, so no self-block; Get::Current re-reads and returns the new value.
#[test]
fn get_current_after_own_update_sees_new_value() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let db = open_db(&env, "db", false);
    db.put(K, V_OLD).unwrap();
    let t = env.begin_transaction(None).unwrap();
    let mut c = db.open_cursor_in(&t, None).unwrap();
    assert_eq!(c.seek(K).unwrap().as_deref(), Some(V_OLD));
    db.put_in(&t, K, V_NEW).unwrap();
    let (s, v) = current(&mut c).unwrap();
    c.close().unwrap();
    t.abort().unwrap();
    assert_eq!(s, OperationStatus::Success);
    assert_eq!(v.as_deref(), Some(V_NEW));
}

/// SERIALIZABLE txn, own UPDATE then Get::Current: exercises the
/// `lock_ln` RANGE_READ branch inside the re-read. The serializable txn
/// seeks K (taking RangeRead), updates K to V_NEW (own WRITE lock), then
/// Get::Current re-locks RangeRead (granted, own txn) and re-reads the slot,
/// returning V_NEW. (A cross-txn serializable block is not deterministically
/// testable at this layer without a thread: a serializable reader that
/// positions on K already holds RangeRead on K, so the foreign writer would
/// block on the READER, not the other way round; and a READ_UNCOMMITTED
/// cursor correctly takes NONE even under a serializable txn, per JE
/// getLockType. The DEFAULT/READ_COMMITTED blocking case above already
/// covers the READ-vs-WRITE conflict class.)
#[test]
fn get_current_serializable_own_update_sees_new_value() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let db = open_db(&env, "db", false);
    db.put(K, V_OLD).unwrap();
    let t = env
        .begin_transaction(Some(
            &TransactionConfig::new()
                .with_serializable_isolation(true)
                .with_lock_timeout_ms(50),
        ))
        .unwrap();
    let mut c = db.open_cursor_in(&t, None).unwrap();
    assert_eq!(c.seek(K).unwrap().as_deref(), Some(V_OLD));
    db.put_in(&t, K, V_NEW).unwrap();
    let (s, v) = current(&mut c).unwrap();
    c.close().unwrap();
    t.abort().unwrap();
    assert_eq!(s, OperationStatus::Success);
    assert_eq!(v.as_deref(), Some(V_NEW));
}

// ─────────────────────────────────────────────────────────────────────────
// DELETE / KNOWN-DELETED slot (re-read must observe the deletion)
// ─────────────────────────────────────────────────────────────────────────

/// RU cursor, foreign uncommitted DELETE: READ_UNCOMMITTED treats a
/// provisionally-defunct slot as non-existent (lockLNAndCheckDefunct case 3,
/// CursorImpl.java:2082-2089), so Get::Current returns NotFound — it must NOT
/// return the cached old value.
#[test]
fn get_current_ru_over_foreign_uncommitted_delete_is_notfound() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let db = open_db(&env, "db", false);
    db.put(K, V_OLD).unwrap();
    let reader = writer_txn(&env);
    let cc = CursorConfig::read_uncommitted();
    let mut c = db.open_cursor_in(&reader, Some(&cc)).unwrap();
    assert_eq!(c.seek(K).unwrap().as_deref(), Some(V_OLD));
    let w = writer_txn(&env);
    assert!(db.delete_in(&w, K).unwrap());
    let (s, _v) = current(&mut c).unwrap();
    c.close().unwrap();
    reader.abort().unwrap();
    w.abort().unwrap();
    assert_eq!(
        s,
        OperationStatus::NotFound,
        "READ_UNCOMMITTED Get::Current over a foreign uncommitted delete \
         skips the provisionally-defunct slot (not the cached old value)"
    );
}

/// After the foreign writer's DELETE commits, Get::Current re-read reports the
/// slot gone (NotFound) — reader is read-committed so it is not pinned to the
/// old value.
#[test]
fn get_current_over_committed_delete_is_notfound() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let db = open_db(&env, "db", false);
    db.put(K, V_OLD).unwrap();
    let reader = env
        .begin_transaction(Some(
            &TransactionConfig::new().with_read_committed(true),
        ))
        .unwrap();
    let mut c = db.open_cursor_in(&reader, None).unwrap();
    assert_eq!(c.seek(K).unwrap().as_deref(), Some(V_OLD));
    let w = writer_txn(&env);
    assert!(db.delete_in(&w, K).unwrap());
    w.commit().unwrap();
    let (s, _v) = current(&mut c).unwrap();
    c.close().unwrap();
    reader.abort().unwrap();
    assert_eq!(s, OperationStatus::NotFound);
}

/// After the foreign writer's DELETE aborts, Get::Current re-read sees the
/// restored old value (the record is back).
#[test]
fn get_current_after_foreign_delete_abort_sees_old_value() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let db = open_db(&env, "db", false);
    db.put(K, V_OLD).unwrap();
    let reader = env
        .begin_transaction(Some(
            &TransactionConfig::new().with_read_committed(true),
        ))
        .unwrap();
    let mut c = db.open_cursor_in(&reader, None).unwrap();
    assert_eq!(c.seek(K).unwrap().as_deref(), Some(V_OLD));
    let w = writer_txn(&env);
    assert!(db.delete_in(&w, K).unwrap());
    w.abort().unwrap();
    let (s, v) = current(&mut c).unwrap();
    c.close().unwrap();
    reader.abort().unwrap();
    assert_eq!(s, OperationStatus::Success);
    assert_eq!(v.as_deref(), Some(V_OLD));
}

// ─────────────────────────────────────────────────────────────────────────
// Sorted-duplicate database (two-part composite key)
// ─────────────────────────────────────────────────────────────────────────

/// Sorted-dup, RU cursor over a foreign uncommitted DELETE of the positioned
/// dup: Get::Current returns NotFound (the dup slot is provisionally defunct).
#[test]
fn dup_get_current_ru_over_foreign_uncommitted_delete_is_notfound() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let db = open_db(&env, "db", true);
    db.put(K, b"a").unwrap();
    db.put(K, b"b").unwrap();
    let reader = writer_txn(&env);
    let cc = CursorConfig::read_uncommitted();
    let mut c = db.open_cursor_in(&reader, Some(&cc)).unwrap();
    // Position on the (K, "a") dup via SearchBoth.
    let (mut k, mut d) =
        (DatabaseEntry::from_bytes(K), DatabaseEntry::from_bytes(b"a"));
    assert_eq!(
        c.get(&mut k, &mut d, Get::SearchBoth, None).unwrap(),
        OperationStatus::Success
    );
    // Foreign txn deletes the (K,"a") dup via a cursor positioned on it.
    let w = writer_txn(&env);
    {
        let mut wc = db.open_cursor_in(&w, None).unwrap();
        let (mut wk, mut wd) =
            (DatabaseEntry::from_bytes(K), DatabaseEntry::from_bytes(b"a"));
        assert_eq!(
            wc.get(&mut wk, &mut wd, Get::SearchBoth, None).unwrap(),
            OperationStatus::Success
        );
        wc.delete().unwrap();
        wc.close().unwrap();
    }
    let (s, _v) = current(&mut c).unwrap();
    c.close().unwrap();
    reader.abort().unwrap();
    w.abort().unwrap();
    assert_eq!(
        s,
        OperationStatus::NotFound,
        "sorted-dup RU Get::Current over a foreign uncommitted dup delete \
         must observe the deletion"
    );
}

/// Sorted-dup, DEFAULT reader over a foreign uncommitted DELETE of the
/// positioned dup: Get::Current takes a READ lock on the dup slot and blocks
/// on the deleter's WRITE lock.
#[test]
fn dup_get_current_default_over_foreign_uncommitted_delete_blocks() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let db = open_db(&env, "db", true);
    db.put(K, b"a").unwrap();
    db.put(K, b"b").unwrap();
    let reader = env
        .begin_transaction(Some(
            &TransactionConfig::new()
                .with_read_committed(true)
                .with_lock_timeout_ms(50),
        ))
        .unwrap();
    let mut c = db.open_cursor_in(&reader, None).unwrap();
    let (mut k, mut d) =
        (DatabaseEntry::from_bytes(K), DatabaseEntry::from_bytes(b"a"));
    assert_eq!(
        c.get(&mut k, &mut d, Get::SearchBoth, None).unwrap(),
        OperationStatus::Success
    );
    let w = writer_txn(&env);
    {
        let mut wc = db.open_cursor_in(&w, None).unwrap();
        let (mut wk, mut wd) =
            (DatabaseEntry::from_bytes(K), DatabaseEntry::from_bytes(b"a"));
        assert_eq!(
            wc.get(&mut wk, &mut wd, Get::SearchBoth, None).unwrap(),
            OperationStatus::Success
        );
        wc.delete().unwrap();
        wc.close().unwrap();
    }
    let r = current(&mut c);
    c.close().unwrap();
    reader.abort().unwrap();
    w.abort().unwrap();
    assert_lock_timeout(
        r,
        "sorted-dup Get::Current (default) over foreign uncommitted dup delete",
    );
}

/// Control: foreign writer touches a DIFFERENT key; Get::Current over the
/// unaffected positioned record returns the still-current value with no
/// block, under every isolation. Guards against over-locking.
#[test]
fn get_current_unaffected_by_foreign_write_to_other_key() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let db = open_db(&env, "db", false);
    db.put(K, V_OLD).unwrap();
    db.put(b"other", b"o").unwrap();
    let reader = env
        .begin_transaction(Some(
            &TransactionConfig::new().with_read_committed(true),
        ))
        .unwrap();
    let mut c = db.open_cursor_in(&reader, None).unwrap();
    assert_eq!(c.seek(K).unwrap().as_deref(), Some(V_OLD));
    let w = writer_txn(&env);
    db.put_in(&w, b"other", b"O").unwrap();
    let (s, v) = current(&mut c).unwrap();
    c.close().unwrap();
    reader.abort().unwrap();
    w.abort().unwrap();
    assert_eq!(s, OperationStatus::Success);
    assert_eq!(v.as_deref(), Some(V_OLD));
}
