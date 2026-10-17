//! rep_user_write_paths_probe — R-REPUSER fail-closed across EVERY public
//! user-write path.
//!
//! Companion to `rep_user_write_probe.rs` (which covers the explicit-txn
//! commit path). This file closes the coordinator-review GAP: the refusal must
//! fire on the MOST COMMON write — a plain auto-commit `db.put(k, v)` — and on
//! every other public write path, not only on an explicit transaction's
//! commit. Each test wires a real `ReplicatedEnvironment` as the data env's
//! replica-ack coordinator (the public silent-failure surface) and asserts the
//! write is refused with `NoxuError::ReplicationNotSupported`, BEFORE anything
//! is logged (nothing durable after reopen). A standalone-env control proves
//! each path is unaffected without a coordinator.
//!
//! On the base (coordinator not refusing) every one of these auto-commit /
//! cursor / non-txnal / DDL paths returns `Ok` and silently drops the write
//! (never shipped) — that is the S1 defect this file guards against.

use std::path::PathBuf;
use std::sync::Arc;

use noxu_db::durability::{Durability, ReplicaAckPolicy, SyncPolicy};
use noxu_db::error::NoxuError;
use noxu_db::{DatabaseConfig, Environment, EnvironmentConfig, Put};
use noxu_rep::{NodeType, RepConfig, ReplicatedEnvironment};
use tempfile::TempDir;

fn master_env(node_name: &str) -> Arc<ReplicatedEnvironment> {
    let cfg = RepConfig::builder("paths_group", node_name, "127.0.0.1")
        .node_port(0)
        .node_type(NodeType::Electable)
        .build();
    let env = Arc::new(ReplicatedEnvironment::new(cfg).unwrap());
    env.become_master(1).unwrap();
    env
}

/// Open a data env + database, then install a refusing `ReplicatedEnvironment`
/// coordinator. Databases are opened BEFORE wiring (as a real app does), so
/// the pre-existing DB stays usable for the refusal tests on data writes.
fn env_db_with_coordinator(
    tmp: &TempDir,
) -> (Environment, noxu_db::Database, Arc<ReplicatedEnvironment>) {
    let env_cfg = EnvironmentConfig::new(PathBuf::from(tmp.path()))
        .with_allow_create(true)
        .with_transactional(true);
    let env = Environment::open(env_cfg).unwrap();
    let db = env
        .open_database(
            None,
            "d",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap();
    let rep = master_env("paths_master");
    env.set_replica_coordinator(rep.clone());
    (env, db, rep)
}

fn assert_refused<T: std::fmt::Debug>(res: Result<T, NoxuError>, path: &str) {
    match res {
        Err(NoxuError::ReplicationNotSupported { reason }) => {
            assert!(!reason.is_empty(), "{path}: refusal reason empty");
        }
        Ok(v) => panic!(
            "{path}: S1 REGRESSION — write returned Ok({v:?}); it was silently \
             accepted and will never be streamed (or replication was genuinely \
             fixed — update this guard deliberately for v8.0.0)."
        ),
        Err(other) => {
            panic!("{path}: expected ReplicationNotSupported, got {other:?}")
        }
    }
}

/// Reopen the env and assert key `k` is absent — the refused write never
/// reached a durable commit.
fn assert_absent_after_reopen(tmp: &TempDir, k: &[u8]) {
    let cfg = EnvironmentConfig::new(PathBuf::from(tmp.path()))
        .with_transactional(true);
    let env = Environment::open(cfg).unwrap();
    let db = env
        .open_database(
            None,
            "d",
            &DatabaseConfig::new().with_transactional(true),
        )
        .unwrap();
    assert!(
        db.get(k).unwrap().is_none(),
        "refused write must not be durable; key {k:?} present after reopen"
    );
    let _ = db.close();
    let _ = env.close();
}

// ── (1) auto-commit db.put — THE most common write, the review's headline ──
#[test]
fn auto_commit_put_is_refused() {
    let tmp = TempDir::new().unwrap();
    let (env, db, rep) = env_db_with_coordinator(&tmp);
    assert_refused(db.put(b"k", b"v"), "auto-commit db.put");
    let _ = db.close();
    let _ = env.close();
    let _ = rep.close();
    assert_absent_after_reopen(&tmp, b"k");
}

// ── (2) auto-commit db.delete ──
#[test]
fn auto_commit_delete_is_refused() {
    let tmp = TempDir::new().unwrap();
    let (env, db, rep) = env_db_with_coordinator(&tmp);
    assert_refused(db.delete(b"k"), "auto-commit db.delete");
    let _ = db.close();
    let _ = env.close();
    let _ = rep.close();
}

// ── (2b) auto-commit put_no_overwrite ──
#[test]
fn auto_commit_put_no_overwrite_is_refused() {
    let tmp = TempDir::new().unwrap();
    let (env, db, rep) = env_db_with_coordinator(&tmp);
    assert_refused(
        db.put_no_overwrite(b"k", b"v"),
        "auto-commit db.put_no_overwrite",
    );
    let _ = db.close();
    let _ = env.close();
    let _ = rep.close();
}

// ── (3) cursor write under auto-commit (no explicit txn) ──
#[test]
fn auto_commit_cursor_put_is_refused() {
    let tmp = TempDir::new().unwrap();
    let (env, db, rep) = env_db_with_coordinator(&tmp);
    let mut cur = db.open_cursor(None).unwrap();
    let key = noxu_db::DatabaseEntry::from_bytes(b"ck");
    let val = noxu_db::DatabaseEntry::from_bytes(b"cv");
    assert_refused(
        cur.put(&key, &val, Put::Overwrite),
        "auto-commit cursor.put",
    );
    drop(cur);
    let _ = db.close();
    let _ = env.close();
    let _ = rep.close();
    assert_absent_after_reopen(&tmp, b"ck");
}

// ── (4) non-transactional DB write (ThreadLocker). JE refuses a user write to
//        a replicated DB that is not under a (master) txn; here the fail-closed
//        env refuses the non-txnal auto-commit write just like the txnal one. ─
#[test]
fn non_transactional_db_write_is_refused() {
    let tmp = TempDir::new().unwrap();
    let env_cfg = EnvironmentConfig::new(PathBuf::from(tmp.path()))
        .with_allow_create(true)
        .with_transactional(true);
    let env = Environment::open(env_cfg).unwrap();
    // A NON-transactional database.
    let db = env
        .open_database(
            None,
            "nt",
            &DatabaseConfig::new().with_allow_create(true),
        )
        .unwrap();
    let rep = master_env("paths_master_nt");
    env.set_replica_coordinator(rep.clone());
    assert_refused(db.put(b"k", b"v"), "non-transactional db.put");
    let _ = db.close();
    let _ = env.close();
    let _ = rep.close();
}

// ── (5) DDL create (open_database with allow_create) ──
#[test]
fn ddl_create_is_refused() {
    let tmp = TempDir::new().unwrap();
    let (env, db, rep) = env_db_with_coordinator(&tmp);
    // Creating a NEW database after the coordinator is wired must be refused
    // (NameLN is never shipped — DDL replay is missing).
    let res = env.open_database(
        None,
        "newdb",
        &DatabaseConfig::new().with_allow_create(true).with_transactional(true),
    );
    assert_refused(res.map(|_| "db"), "DDL open_database create");
    let _ = db.close();
    let _ = env.close();
    let _ = rep.close();
}

// ── (6) DDL remove / rename / truncate ──
#[test]
fn ddl_remove_rename_truncate_are_refused() {
    let tmp = TempDir::new().unwrap();
    let (env, db, rep) = env_db_with_coordinator(&tmp);
    assert_refused(
        env.remove_database(None, "d").map(|_| ()),
        "DDL remove_database",
    );
    assert_refused(
        env.rename_database(None, "d", "d2").map(|_| ()),
        "DDL rename_database",
    );
    assert_refused(
        env.truncate_database(None, "d").map(|_| 0u64),
        "DDL truncate_database",
    );
    let _ = db.close();
    let _ = env.close();
    let _ = rep.close();
}

// ── (7) explicit-txn put + commit (the v1 surface — still refused) ──
#[test]
fn explicit_txn_put_commit_is_refused() {
    let tmp = TempDir::new().unwrap();
    let (env, db, rep) = env_db_with_coordinator(&tmp);
    let txn = env.begin_transaction(None).unwrap();
    // The write op itself is refused now (before the LN), not just commit.
    let res = db.put_in(&txn, b"k", b"v");
    assert_refused(res, "explicit-txn db.put_in");
    let _ = txn.abort();
    let _ = db.close();
    let _ = env.close();
    let _ = rep.close();
    assert_absent_after_reopen(&tmp, b"k");
}

// ── control: standalone env (no coordinator) — every path unaffected ──
#[test]
fn standalone_env_all_paths_unaffected() {
    let tmp = TempDir::new().unwrap();
    let env_cfg = EnvironmentConfig::new(PathBuf::from(tmp.path()))
        .with_allow_create(true)
        .with_transactional(true);
    let env = Environment::open(env_cfg).unwrap();
    let db = env
        .open_database(
            None,
            "d",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap();
    // auto-commit put/delete
    db.put(b"k", b"v").expect("standalone auto-commit put must work");
    assert_eq!(db.get(b"k").unwrap().as_deref(), Some(&b"v"[..]));
    assert!(db.delete(b"k").unwrap(), "standalone delete must work");
    // cursor put
    let mut cur = db.open_cursor(None).unwrap();
    let key = noxu_db::DatabaseEntry::from_bytes(b"ck");
    let val = noxu_db::DatabaseEntry::from_bytes(b"cv");
    cur.put(&key, &val, Put::Overwrite)
        .expect("standalone cursor put must work");
    drop(cur);
    assert_eq!(db.get(b"ck").unwrap().as_deref(), Some(&b"cv"[..]));
    // explicit txn commit
    let txn = env.begin_transaction(None).unwrap();
    db.put_in(&txn, b"tk", b"tv").unwrap();
    let dur = Durability::new(
        SyncPolicy::Sync,
        SyncPolicy::Sync,
        ReplicaAckPolicy::SimpleMajority,
    );
    txn.commit_with_durability(dur)
        .expect("standalone explicit-txn commit must work");
    assert_eq!(db.get(b"tk").unwrap().as_deref(), Some(&b"tv"[..]));
    // DDL create
    let db2 = env
        .open_database(
            None,
            "d2",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .expect("standalone DDL create must work");
    let _ = db2.close();
    let _ = db.close();
    let _ = env.close();
}
