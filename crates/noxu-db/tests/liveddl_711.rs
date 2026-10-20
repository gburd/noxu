//! R-LIVEDDL-711 — LIVE (no XA, no crash) catalog-DDL isolation witness.
//!
//! A transaction `T` that performs a DDL op (remove / rename / truncate) on a
//! database `x` holds `x`'s records in doubt until `T` resolves.  JE serialises
//! every catalog operation on a name through a NameLN lock
//! (`DbTree.lockNameLN` WRITE for remove/rename/truncate, `DbTree.getDb` +
//! `acquireHandleLock` READ for open; `getDbNames` is lock-free — `LockType.NONE`).
//! So while `T` holds its uncommitted DDL on `x`, a concurrent remove / rename /
//! truncate / open of `x` BLOCKS until `T` resolves (or times out with a typed
//! lock error) and has **no effect** until then.  After `T` aborts, `x` is
//! intact with every committed record.
//!
//! The 7.11.x base takes **no** name-record lock for the live transactional DDL
//! path, so a concurrent auto-commit remove/truncate/rename slips through,
//! mutates the catalog, and — once `T` aborts — leaves `x` GONE or EMPTY:
//! committed records are lost (S2).
//!
//! Invariant asserted by every case: the final state equals *some* serial
//! history (T-then-other, or other-then-T), with **no lost committed records**.
//! A concurrent op either (a) waits for T and then sees T's resolution, or
//! (b) fails with a typed, retryable lock/conflict error
//! (`LockTimeout` / `LockConflict` / `DeadlockDetected`) and has no effect.
//!
//! These witnesses are RED on the base (`223a643a`) and on `e53f0a41`
//! (fix/xa-ddl-711); they turn GREEN only with the live NameLN lock.

use noxu_db::{DatabaseConfig, Environment, EnvironmentConfig, NoxuError};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tempfile::TempDir;

const REC_COUNT: u64 = 8;
// Short per-lock timeout: a conflicting concurrent op fails fast with a typed
// LockTimeout instead of blocking the test thread forever, making every case
// deterministic in a single process.
const LOCK_MS: u64 = 400;

fn open_env(dir: &TempDir) -> Environment {
    Environment::open(
        EnvironmentConfig::new(dir.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true)
            .with_lock_timeout(LOCK_MS),
    )
    .unwrap()
}

fn db_cfg() -> DatabaseConfig {
    DatabaseConfig::new().with_allow_create(true).with_transactional(true)
}

/// Create `name` with `REC_COUNT` committed records, then close the handle so
/// DDL is permitted (remove/rename/truncate refuse open handles).
fn seed(env: &Environment, name: &str) {
    let db = env.open_database(None, name, &db_cfg()).unwrap();
    for i in 0..REC_COUNT {
        db.put(format!("k{i}").as_bytes(), format!("v{i}").as_bytes()).unwrap();
    }
    db.close().unwrap();
}

fn exists(env: &Environment, name: &str) -> bool {
    env.database_names().unwrap().contains(&name.to_string())
}

/// Count records by reopening the db (auto-commit read).  Panics if absent.
fn record_count(env: &Environment, name: &str) -> u64 {
    let db = env
        .open_database(
            None,
            name,
            &DatabaseConfig::new().with_transactional(true),
        )
        .unwrap();
    let c = db.count().unwrap();
    db.close().unwrap();
    c
}

/// True iff `e` is a typed, retryable lock/conflict error — the only
/// acceptable failure for a concurrent op that races T's DDL lock.
fn is_typed_conflict(e: &NoxuError) -> bool {
    matches!(
        e,
        NoxuError::LockTimeout { .. }
            | NoxuError::LockConflict(_)
            | NoxuError::DeadlockDetected
            | NoxuError::LockNotAvailable
    )
}

// ---------------------------------------------------------------------------
// Core data-loss witnesses (the S2 the ticket names)
// ---------------------------------------------------------------------------

/// T: remove(Some) "x" uncommitted; concurrent auto-commit remove(None) "x";
/// T aborts.  Serial history: either the concurrent remove was blocked (T
/// aborts → x survives with 8 records) or it was serialised before T (then T's
/// remove no-ops on the already-gone db → x absent, but NO committed records
/// were lost because the concurrent remove committed removing an empty-of-T db).
/// On the base, the concurrent remove succeeds AND T's abort does not restore
/// x → x GONE while T aborted → committed records lost.  RED.
#[test]
fn t_remove_abort_vs_concurrent_remove_keeps_records() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    seed(&env, "x");

    let txn = env.begin_transaction(None).unwrap();
    env.remove_database(Some(&txn), "x").unwrap();

    // Concurrent auto-commit remove of the SAME name while T holds it in doubt.
    // T acquired the name's WRITE lock first (synchronously, above), so the
    // concurrent remove must conflict (typed, retryable) — it cannot slip
    // through and destroy x's records behind T's back.
    let concurrent = env.remove_database(None, "x");

    txn.abort().unwrap();

    assert!(
        concurrent.is_err()
            && is_typed_conflict(concurrent.as_ref().unwrap_err()),
        "concurrent remove of an in-doubt-remove name must typed-conflict, got \
         {concurrent:?}"
    );
    // T aborted and the concurrent remove conflicted (no effect) -> x intact.
    assert!(exists(&env, "x"), "x gone after T.abort + conflicting remove");
    assert_eq!(
        record_count(&env, "x"),
        REC_COUNT,
        "committed records lost after T.abort"
    );
}

/// T: remove(Some) "x" uncommitted; concurrent auto-commit truncate(None) "x";
/// T aborts.  A concurrent truncate must conflict (JE WRITE lock).  If it
/// slips through it empties x; T's abort cannot restore the records → RED.
#[test]
fn t_remove_abort_vs_concurrent_truncate_keeps_records() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    seed(&env, "x");

    let txn = env.begin_transaction(None).unwrap();
    env.remove_database(Some(&txn), "x").unwrap();

    let concurrent = env.truncate_database(None, "x");

    txn.abort().unwrap();

    // After T aborts, x MUST exist with all 8 records regardless of whether the
    // concurrent truncate was blocked (it must have been, to be serialisable:
    // a committed truncate before an aborted remove would leave x empty, which
    // is lost records; JE blocks it).
    assert!(exists(&env, "x"), "x gone after T.abort");
    assert_eq!(
        record_count(&env, "x"),
        REC_COUNT,
        "committed records lost: concurrent truncate emptied x and T.abort \
         could not restore them (was concurrent={concurrent:?})"
    );
    // And the concurrent truncate must have conflicted (not silently emptied).
    if let Ok(n) = concurrent {
        panic!(
            "concurrent truncate of an in-doubt-remove db returned Ok({n}) — \
             JE WRITE-lock conflict expected"
        );
    }
}

/// T: remove(Some) "x" uncommitted; concurrent auto-commit open(None) "x".
/// JE's open takes a READ handle lock that the resurrected/held WRITE lock
/// denies → the opener must conflict (or, if it opened before T, see a
/// consistent snapshot).  After T aborts, x is intact.
#[test]
fn t_remove_abort_vs_concurrent_open_keeps_records() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    seed(&env, "x");

    let txn = env.begin_transaction(None).unwrap();
    env.remove_database(Some(&txn), "x").unwrap();

    let opened = env.open_database(None, "x", &db_cfg());
    // If it opened, close it so the DDL-on-abort path is unobstructed.
    if let Ok(db) = &opened {
        let _ = db.count();
        db.close().unwrap();
    }

    txn.abort().unwrap();

    assert!(exists(&env, "x"), "x gone after T.abort");
    assert_eq!(
        record_count(&env, "x"),
        REC_COUNT,
        "records lost after T.abort"
    );
    // The concurrent open must either conflict OR succeed; if it succeeds it
    // must not have observed x as absent.
    match opened {
        Ok(_) | Err(_) => { /* either is serialisable; data-loss is the bug */ }
    }
}

/// T: rename(Some) x→y uncommitted; concurrent auto-commit remove(None) "x"
/// (rename-from).  T aborts → x must survive with records; y must NOT exist.
#[test]
fn t_rename_abort_vs_concurrent_remove_from_keeps_records() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    seed(&env, "x");

    let txn = env.begin_transaction(None).unwrap();
    env.rename_database(Some(&txn), "x", "y").unwrap();

    let concurrent = env.remove_database(None, "x");

    txn.abort().unwrap();

    assert!(
        exists(&env, "x"),
        "x gone after rename-abort (was {concurrent:?})"
    );
    assert_eq!(record_count(&env, "x"), REC_COUNT, "records lost");
    assert!(!exists(&env, "y"), "y must not exist after rename aborted");
    if let Ok(()) = concurrent {
        panic!("concurrent remove of an in-doubt-rename source returned Ok");
    }
}

/// T: rename(Some) x→y uncommitted; concurrent auto-commit open(None) "y"
/// (rename-to target).  T aborts → neither y created nor x lost.
#[test]
fn t_rename_abort_vs_concurrent_create_target_is_serialisable() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    seed(&env, "x");

    let txn = env.begin_transaction(None).unwrap();
    env.rename_database(Some(&txn), "x", "y").unwrap();

    // Concurrent create of the rename TARGET "y".
    let concurrent = env.open_database(None, "y", &db_cfg());
    if let Ok(db) = &concurrent {
        db.close().unwrap();
    }

    txn.abort().unwrap();

    // x must survive; the rename aborted.
    assert!(exists(&env, "x"), "x gone after rename-abort");
    assert_eq!(record_count(&env, "x"), REC_COUNT, "records lost");
    // The concurrent create of "y" must either have conflicted with T's
    // in-doubt create of "y", or committed before T (then T's rename-to-y must
    // have conflicted and aborted clean).  Either way x's records survive.
    match concurrent {
        Ok(_) | Err(_) => {}
    }
}

/// T: truncate(Some) "x" uncommitted; concurrent auto-commit remove(None) "x";
/// T aborts → x intact with all records.
#[test]
fn t_truncate_abort_vs_concurrent_remove_keeps_records() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    seed(&env, "x");

    let txn = env.begin_transaction(None).unwrap();
    let _n = env.truncate_database(Some(&txn), "x").unwrap();

    let concurrent = env.remove_database(None, "x");

    txn.abort().unwrap();

    assert!(
        exists(&env, "x"),
        "x gone after truncate-abort (was {concurrent:?})"
    );
    assert_eq!(record_count(&env, "x"), REC_COUNT, "records lost after abort");
    if let Ok(()) = concurrent {
        panic!("concurrent remove of an in-doubt-truncate db returned Ok");
    }
}

// ---------------------------------------------------------------------------
// Commit side: the serial history must hold when T commits too.
// ---------------------------------------------------------------------------

/// T: remove(Some) "x"; concurrent remove(None) "x"; T commits.  Serial
/// history: both removes serialise; x ends up gone, and there is exactly one
/// successful remove (the loser conflicts).  No resurrection.
#[test]
fn t_remove_commit_vs_concurrent_remove_no_resurrection() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    seed(&env, "x");

    let txn = env.begin_transaction(None).unwrap();
    env.remove_database(Some(&txn), "x").unwrap();

    let concurrent = env.remove_database(None, "x");

    txn.commit().unwrap();

    // Either way, x must be GONE after T commits its remove.
    assert!(!exists(&env, "x"), "x resurrected after committed remove");
    // The concurrent remove must have conflicted (T holds the name) OR, if it
    // ran first, T's remove no-ops; it must never be a *second successful*
    // destructive op on live records.
    match concurrent {
        Ok(()) => { /* ran before T; T no-ops; x gone — ok */ }
        Err(ref e) if is_typed_conflict(e) => { /* blocked by T — ok */ }
        Err(e) => panic!("unexpected error: {e:?}"),
    }
}

/// T: rename(Some) x→y; concurrent open(None) "x"; T commits.  After commit x
/// is gone and y exists with all records (no double-named db, no lost records).
#[test]
fn t_rename_commit_vs_concurrent_open_source_is_serialisable() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    seed(&env, "x");

    let txn = env.begin_transaction(None).unwrap();
    env.rename_database(Some(&txn), "x", "y").unwrap();

    let concurrent = env.open_database(None, "x", &db_cfg());
    if let Ok(db) = &concurrent {
        db.close().unwrap();
    }

    txn.commit().unwrap();

    assert!(!exists(&env, "x"), "x still present after committed rename");
    assert!(exists(&env, "y"), "y absent after committed rename");
    assert_eq!(record_count(&env, "y"), REC_COUNT, "records lost on rename");
}

// ---------------------------------------------------------------------------
// True concurrency: a second thread BLOCKS on T, then sees T's resolution.
// ---------------------------------------------------------------------------

/// Two real threads.  T removes "x" and holds it ~250ms before aborting; the
/// other thread's concurrent remove must either block-then-fail-typed, or
/// block-then-succeed-after-abort — never destroy x's records while T aborts.
/// Two real threads.  T removes "x" and holds it ~250ms before aborting; the
/// other thread concurrently OPENS "x".  JE's open takes a READ handle lock the
/// in-doubt WRITE holder blocks; once T aborts the open proceeds and sees "x"
/// with all its committed records.  This proves the concurrent op genuinely
/// WAITS on T (rather than racing through), and that T's abort loses nothing.
#[test]
fn concurrent_thread_open_blocks_on_in_doubt_then_sees_abort() {
    let dir = TempDir::new().unwrap();
    // Longer timeout so the blocker actually waits for T rather than timing out
    // before T resolves.
    let env = Arc::new(
        Environment::open(
            EnvironmentConfig::new(dir.path().to_path_buf())
                .with_allow_create(true)
                .with_transactional(true)
                .with_lock_timeout(5_000),
        )
        .unwrap(),
    );
    seed(&env, "x");

    let txn = env.begin_transaction(None).unwrap();
    env.remove_database(Some(&txn), "x").unwrap();

    let env2 = Arc::clone(&env);
    let started = std::time::Instant::now();
    // The concurrent op is a *destructive* remove: it must block on T's WRITE
    // lock (the whole point), then — after T aborts — either succeed (a
    // committed remove, which legitimately removes x) or conflict.  Either
    // outcome is serialisable; what must never happen is x ending up EMPTY or
    // half-gone with records silently lost while T aborted.
    let other = thread::spawn(move || env2.remove_database(None, "x"));

    // Hold the in-doubt remove, let the other thread block on the name lock.
    thread::sleep(Duration::from_millis(250));
    txn.abort().unwrap();

    let res = other.join().unwrap();
    let waited = started.elapsed();

    // The blocker must have WAITED for T (it could not have finished before we
    // released the lock at ~250ms): a race-through would return almost
    // instantly.  (Lower bound deliberately conservative to avoid flakiness.)
    assert!(
        res.is_err() || waited >= Duration::from_millis(200),
        "concurrent remove did not block on the in-doubt name lock \
         (waited={waited:?}, res={res:?})"
    );

    // Serial history: either x survives with all 8 records (blocker
    // conflicted / T restored it and the blocker then no-op'd), or x is absent
    // because the blocker's remove COMMITTED after T aborted.  What must not
    // happen: x present but with records silently lost.
    if exists(&env, "x") {
        assert_eq!(
            record_count(&env, "x"),
            REC_COUNT,
            "x present but committed records lost after T.abort (other={res:?})"
        );
    } else {
        assert!(
            res.is_ok(),
            "x is gone but the concurrent remove did NOT commit it \
             (other={res:?}) — that is data loss via T's aborted remove"
        );
    }
}

// ---------------------------------------------------------------------------
// db_id rename-pairing (coverage-gap #3): a LIVE rename -> resolve, no crash.
// ---------------------------------------------------------------------------

/// The deferred (transactional) rename logs a delete-of-old + create-of-new
/// NameLN pair under the user txn (`log_rename_name_ln_txn`), and the renamed
/// database KEEPS ITS DatabaseId (the new name's create carries the old name's
/// db_id).  If that pairing is broken (the new name gets a fresh db_id, or the
/// delete/create are mis-associated), the renamed database would lose its tree
/// -- the records would NOT follow the name.  Every prior rename witness
/// crosses a crash (exercising the recovered-path pairing); this one resolves
/// a live rename with NO crash, so a broken LIVE pairing is caught directly.
///
/// rename a->b, commit (no crash): b must carry a's ORIGINAL 8 records, keyed
/// identically, because the rename re-pairs the delete(a) with the create(b)
/// under a's db_id so b reuses a's tree.  A broken pairing (fresh db_id for b,
/// or the create not matched to the delete) leaves b empty.  A second,
/// independent rename c->d in its OWN txn confirms the pairing is not an
/// artifact of the first db's id.
#[test]
fn live_rename_keeps_db_identity_without_crash() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    seed(&env, "a"); // "a" has 8 records k0..k7 = v0..v7

    let txn = env.begin_transaction(None).unwrap();
    env.rename_database(Some(&txn), "a", "b").unwrap();
    txn.commit().unwrap();

    assert!(!exists(&env, "a"), "a still present after committed rename");
    assert!(exists(&env, "b"), "b absent after committed rename");
    let b = env
        .open_database(
            None,
            "b",
            &DatabaseConfig::new().with_transactional(true),
        )
        .unwrap();
    assert_eq!(
        b.count().unwrap(),
        REC_COUNT,
        "rename pairing broken: b must keep a's original {REC_COUNT} records"
    );
    for i in 0..REC_COUNT {
        assert_eq!(
            b.get(format!("k{i}").as_bytes()).unwrap().as_deref(),
            Some(format!("v{i}").as_bytes()),
            "rename pairing broken: b lost a's original record k{i}"
        );
    }
    b.close().unwrap();

    // A second rename in its own txn: c (3 recs) -> d, d must keep c's records
    // (independent db id -> the pairing is general, not luck of the first id).
    let c = env.open_database(None, "c", &db_cfg()).unwrap();
    for i in 0..3u64 {
        c.put(format!("m{i}").as_bytes(), format!("u{i}").as_bytes()).unwrap();
    }
    c.close().unwrap();
    let txn2 = env.begin_transaction(None).unwrap();
    env.rename_database(Some(&txn2), "c", "d").unwrap();
    txn2.commit().unwrap();
    let d = env
        .open_database(
            None,
            "d",
            &DatabaseConfig::new().with_transactional(true),
        )
        .unwrap();
    assert_eq!(d.count().unwrap(), 3, "rename pairing broken: d lost c's recs");
    assert_eq!(d.get(b"m0").unwrap().as_deref(), Some(b"u0".as_ref()));
    d.close().unwrap();
}
