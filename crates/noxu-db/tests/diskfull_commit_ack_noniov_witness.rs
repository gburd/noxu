//! ENG-DISKFULL-ACK-1 prep witness: `commit()` / `abort()` must fail closed on
//! an environment invalidated by a NON-I/O failure, not only by a log I/O
//! error.
//!
//! The original fix's `Transaction::check_env()` consulted ONLY
//! `LogManager::is_io_invalid()`.  But `EnvironmentImpl::is_valid()` (and JE's
//! `checkIfInvalid()`, which keys on `envState == INVALID` from ANY source)
//! also fails on the general `is_invalid` flag, which `EnvironmentImpl::
//! invalidate` sets on any `EnvironmentFailure` — a daemon-fatal panic / wedge
//! (`Environment::invalidate`, reason `UnexpectedStateFatal`), a recovery
//! log-checksum failure, or a replication rollback — WITHOUT touching
//! `io_invalid`.  So before this prep an empty-txn `commit()` on such an env
//! still took the empty-txn fast path and returned a misleading Ok — the exact
//! acknowledged-but-not-durable bug via a different invalidation source.
//!
//! RED on the io_invalid-only `check_env` (f5a40c13 / 5bf3e5fc): `c=Ok`.
//! GREEN with the prep (`check_env` mirrors `is_valid()`): `c=Err`.
use noxu_db::{DatabaseConfig, Durability, Environment, EnvironmentConfig};

fn cfg(p: &std::path::Path) -> EnvironmentConfig {
    EnvironmentConfig::new(p.to_path_buf())
        .with_allow_create(true)
        .with_transactional(true)
}
fn dbcfg() -> DatabaseConfig {
    DatabaseConfig::new().with_allow_create(true).with_transactional(true)
}

/// A commit on a NON-I/O-invalidated env (daemon-fatal) must return Err —
/// even for an empty transaction that logged nothing.
#[test]
fn commit_on_non_io_invalidated_env_returns_err() {
    let dir = tempfile::tempdir().unwrap();
    let env = Environment::open(cfg(dir.path())).unwrap();
    let db = env.open_database(None, "a", &dbcfg()).unwrap();
    db.put(b"seed", b"v").unwrap();

    // Begin an empty txn BEFORE the invalidation (the field window).
    let t = env.begin_transaction(None).unwrap();

    // Invalidate the environment via the production daemon-fatal path
    // (sets is_invalid / state=Invalid, NOT io_invalid). No I/O error.
    env.invalidate();
    assert!(!env.is_valid(), "env must be invalid after invalidate()");

    // The empty txn logged nothing; without reading is_invalid, commit() would
    // take the empty-txn fast path and return Ok.
    let c = t.commit_with_durability(Durability::COMMIT_SYNC);
    assert!(
        c.is_err(),
        "commit() on a NON-I/O-invalidated env returned Ok \
         (acknowledged-but-not-durable) — JE Transaction.checkEnv throws here. \
         c={c:?}"
    );
    drop(db);
    let _ = env.close();
}

/// JE `Transaction.abort` calls `checkEnv()` after the closed-txn check and
/// throws on an invalid env.  Noxu must (a) return Err, and (b) STILL leave the
/// txn unregistered so `Environment::close()` is not wedged (JE relies on GC;
/// Noxu refuses close while a txn is registered).
#[test]
fn abort_on_invalidated_env_returns_err_and_does_not_wedge_close() {
    let dir = tempfile::tempdir().unwrap();
    let env = Environment::open(cfg(dir.path())).unwrap();
    let db = env.open_database(None, "a", &dbcfg()).unwrap();
    db.put(b"seed", b"v").unwrap();

    let t = env.begin_transaction(None).unwrap();
    env.invalidate();
    assert!(!env.is_valid());

    // abort() on an OPEN txn with an invalid env must return Err (JE checkEnv
    // throws), matching commit's fail-closed behaviour.
    let a = t.abort();
    assert!(
        a.is_err(),
        "abort() on an invalidated env must return Err (JE Transaction.abort \
         -> checkEnv throws). a={a:?}"
    );

    // ...but the txn must be unregistered so close() is not blocked by a
    // leaked active transaction (the operational recover-by-reopen path).
    drop(db);
    let closed = env.close();
    assert!(
        closed.is_ok(),
        "Environment::close() must succeed after an abort on an invalidated \
         env (no leaked active txn). closed={closed:?}"
    );
}
