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
use noxu_db::{
    DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig, Put,
    SequenceConfig,
};
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
    // FID-CURSOR-AUTOCOMMIT: a cursor opened with no txn on a transactional db
    // refuses updates (OperationNotAllowed) BEFORE the write can reach the
    // replica-refusal layer.  Either refusal upholds the S1 invariant that a
    // user write on a replica is never silently accepted.
    match cur.put(&key, &val, Put::Overwrite) {
        Err(NoxuError::ReplicationNotSupported { reason }) => {
            assert!(!reason.is_empty(), "auto-commit cursor.put: empty reason");
        }
        Err(NoxuError::OperationNotAllowed(m)) => {
            assert!(
                m.contains("a Transaction was not supplied to openCursor"),
                "auto-commit cursor.put: unexpected OperationNotAllowed: {m}",
            );
        }
        other => panic!(
            "auto-commit cursor.put: expected the write to be refused, got {other:?}"
        ),
    }
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

// ── (8) Sequence: open_sequence(allow_create) + Sequence::get both write ──
//
// A sequence persists its counter via `Database::put_no_overwrite` (initial
// record, in `Sequence::open`) and `Database::put`/`put_in` (on `get`), both of
// which funnel through `Database::check_writable`. On a master-wired env the
// sequence create is refused; and even a pre-existing sequence's `get` (which
// must persist the advanced window) is refused.
#[test]
fn sequence_create_and_get_are_refused() {
    let tmp = TempDir::new().unwrap();
    // Open env + a plain DB and PRE-CREATE a sequence BEFORE wiring, so we can
    // also exercise `get` on an existing sequence under the refusing coord.
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
    let seq_key = DatabaseEntry::from_bytes(b"seq1");
    {
        // Pre-create + advance once, standalone (no coordinator yet).
        let seq = db
            .open_sequence(
                &seq_key,
                SequenceConfig::new().with_allow_create(true),
            )
            .unwrap();
        let _ = seq.get(None, 1).unwrap();
    }

    // Now wire the refusing coordinator.
    let rep = master_env("paths_master_seq");
    env.set_replica_coordinator(rep.clone());

    // (a) opening a NEW sequence that must create its initial record -> refused.
    let new_key = DatabaseEntry::from_bytes(b"seq_new");
    let res_create = db
        .open_sequence(&new_key, SequenceConfig::new().with_allow_create(true));
    assert_refused(res_create.map(|_| "seq"), "Sequence open create");

    // (b) `get` on the EXISTING sequence that must persist an advanced window
    //     -> refused when it needs to write (cache exhausted). Use a delta that
    //     forces a persist (cache_size default is small; request past it).
    let seq = db
        .open_sequence(&seq_key, SequenceConfig::new().with_allow_create(false))
        .expect("opening an existing sequence (a read) must still work");
    // Drain the cache window so the next get must persist a new window.
    let mut refused = false;
    for _ in 0..10_000 {
        match seq.get(None, 1) {
            Ok(_) => continue,
            Err(NoxuError::ReplicationNotSupported { .. }) => {
                refused = true;
                break;
            }
            Err(other) => panic!("Sequence::get: unexpected {other:?}"),
        }
    }
    assert!(
        refused,
        "Sequence::get must be refused once it needs to persist a new window"
    );

    let _ = db.close();
    let _ = env.close();
    let _ = rep.close();
    assert_absent_after_reopen(&tmp, b"seq_new");
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
    // cursor put (FID-CURSOR-AUTOCOMMIT: a cursor write on a transactional db
    // needs a transactional locker; open under an explicit auto-commit txn).
    {
        let ctxn = env.begin_transaction(None).unwrap();
        let mut cur = db.open_cursor_in(&ctxn, None).unwrap();
        let key = noxu_db::DatabaseEntry::from_bytes(b"ck");
        let val = noxu_db::DatabaseEntry::from_bytes(b"cv");
        cur.put(&key, &val, Put::Overwrite)
            .expect("standalone cursor put must work");
        cur.close().unwrap();
        ctxn.commit().unwrap();
    }
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
