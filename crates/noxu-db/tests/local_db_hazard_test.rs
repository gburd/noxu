//! Hazard analysis of GAP-REPLAY-3 local-DB screening.
//!
//! On the earlier tip 4adbd0d4 the JE-exact predicate ENABLED a replica/local
//! write & create to a NON-replicated database (to match JE's
//! `allowReplicaWrite`).  That exposed three hazards from JE machinery Noxu
//! does not yet have.  These witnesses first established the empirical truth
//! on 4adbd0d4, and now (on the SAFE tip) assert the hazards are PREVENTED:
//! a replica refuses all local-origin user writes & creates (the 7.11.x
//! safety gate — a replicated env treats every user DB as replicated for
//! screening).
//!
//! Findings on 4adbd0d4:
//!   H1 (VLSN collision): REFUTED as described — the user data-path commit
//!      writes a NULL_VLSN entry and never drives the replication VLSN counter
//!      (`EnvironmentImpl::log_txn_commit`, the only consumer, has NO
//!      production caller).  A local write consumed 0 VLSNs.
//!   H3 (master feeds a local DB): REFUTED via the feed — a non-replicated
//!      write is not VLSN-tagged, so the WAL scanner never ships it.
//!   H2 (db-id collision): REAL structurally — Noxu has ONE positive
//!      `next_db_id`, not JE's separate negative local space — but it cannot
//!      manifest through replication today (NameLN DB-creation is neither
//!      VLSN-tagged/fed nor applied by replica replay).  It WOULD collide once
//!      DB-create replication is wired, which is why local DBs on a replica
//!      are refused until the full port lands.
//!
//! On the SAFE tip these witnesses assert the writes/creates are REFUSED.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use noxu_db::{
    DatabaseConfig, Environment, EnvironmentConfig, TransactionConfig,
};
use tempfile::TempDir;

/// A replicated `Environment` with a VLSN counter installed (as
/// `ReplicatedEnvironment::with_environment` does).
fn replicated_env_with_counter() -> (TempDir, Environment, Arc<AtomicU64>) {
    let tmp = TempDir::new().unwrap();
    let cfg = EnvironmentConfig::new(tmp.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    let env = Environment::open(cfg).unwrap();
    let counter = Arc::new(AtomicU64::new(0));
    {
        let ei = env.env_impl_arc_for_test();
        let g = ei.lock();
        g.set_replicated(true);
        g.set_replication_vlsn_counter(Arc::clone(&counter));
    }
    (tmp, env, counter)
}

/// H1 (prevented): on a REPLICA, a local write to a would-be-local DB is now
/// REFUSED with `ReplicaWrite` (7.11.x safety gate), so it can never consume a
/// VLSN from the replica's counter.  The counter stays put (which was already
/// true on 4adbd0d4 — the data path never drives it — but now the write is
/// refused outright, closing the hazard at the source).
#[test]
#[allow(deprecated)] // with_replicated: 7.11.x safety gate
fn h1_local_write_on_replica_is_refused_and_consumes_no_vlsn() {
    let (_tmp, env, counter) = replicated_env_with_counter();
    let local_cfg = DatabaseConfig::new()
        .with_allow_create(true)
        .with_transactional(true)
        .with_replicated(false);
    let localdb = env.open_database(None, "localdb", &local_cfg).unwrap();

    env.env_impl_arc_for_test().lock().set_replica_write_screen(true);

    let before = counter.load(Ordering::SeqCst);

    // A default-txn write to the would-be-local DB is refused (safety gate).
    let w = localdb.put(b"k", b"v");
    assert!(
        matches!(w, Err(noxu_db::NoxuError::ReplicaWrite)),
        "H1 prevented: a local write on a replica must be refused with \
         ReplicaWrite, got {w:?}"
    );
    // And a local-write txn write is refused as a config mismatch or replica
    // write (either way NOT accepted): the key safety property is it is not Ok.
    let txn = env
        .begin_transaction(Some(
            &TransactionConfig::new().with_local_write(true),
        ))
        .unwrap();
    let w2 = localdb.put_in(&txn, b"k2", b"v2");
    assert!(
        w2.is_err(),
        "H1 prevented: a local-write-txn write on a replica must not be \
         accepted, got {w2:?}"
    );
    let _ = txn.abort();

    let after = counter.load(Ordering::SeqCst);
    assert_eq!(
        after, before,
        "H1: the replica's VLSN counter must not advance ({before} -> {after})"
    );

    drop(localdb);
    let _ = env.close();
}

/// H2 (prevented): a non-replicated DB CREATE on a REPLICA is now refused, so
/// it can never take an id from the single `next_db_id` counter.  The witness
/// still documents the structural single-id-space fact (the counter does NOT
/// advance because the create is refused before allocation).
#[test]
#[allow(deprecated)]
fn h2_local_db_create_on_replica_is_refused_no_id_consumed() {
    let (_tmp, env, _counter) = replicated_env_with_counter();

    let next_before = env.env_impl_arc_for_test().lock().peek_next_db_id();

    env.env_impl_arc_for_test().lock().set_replica_write_screen(true);
    let local_cfg = DatabaseConfig::new()
        .with_allow_create(true)
        .with_transactional(true)
        .with_replicated(false);
    let created = env.open_database(None, "local_on_replica", &local_cfg);
    assert!(
        matches!(created, Err(noxu_db::NoxuError::ReplicaWrite)),
        "H2 prevented: creating a would-be-local DB on a replica must be \
         refused with ReplicaWrite, got {:?}",
        created.map(|_| "Ok(db)")
    );

    let next_after = env.env_impl_arc_for_test().lock().peek_next_db_id();
    assert_eq!(
        next_after, next_before,
        "H2: a refused create must NOT consume a db-id from the single shared \
         counter ({next_before} -> {next_after}); Noxu has one positive id \
         space (no separate negative local space), so allowing the create \
         would risk an id collision once DB-create replication is wired — \
         hence it is refused until the full local-DB port (local-db-design.md)"
    );

    let _ = env.close();
}

/// H3 (documented): on a MASTER, a non-replicated DB write consumes no VLSN
/// (the data path writes NULL_VLSN; the WAL scanner ships only VLSN-tagged
/// entries, so a local DB is never fed).  This is the pre-existing (v7.11.0)
/// side; the master IS allowed to write local DBs (screen off), and the point
/// is that doing so does not VLSN-tag/feed them.
#[test]
#[allow(deprecated)]
fn h3_master_local_db_write_consumes_no_vlsn() {
    let (_tmp, env, counter) = replicated_env_with_counter();
    let local_cfg = DatabaseConfig::new()
        .with_allow_create(true)
        .with_transactional(true)
        .with_replicated(false);
    // On a master the would-be-local DB is still forced replicated by the
    // 7.11.x gate, but the screen is OFF so writes succeed; the point is the
    // VLSN counter is not driven by the user data path.
    let localdb = env.open_database(None, "localdb", &local_cfg).unwrap();

    let before = counter.load(Ordering::SeqCst);
    localdb.put(b"k", b"v").unwrap();
    let after = counter.load(Ordering::SeqCst);

    assert_eq!(
        after, before,
        "H3: a user data-path write must not drive the VLSN counter \
         ({before} -> {after}); only log_txn_commit does, and it has no \
         production caller"
    );

    drop(localdb);
    let _ = env.close();
}
