//! rep_user_write_probe — the original R-REPUSER reproducer, adapted to the
//! v7.11.1 fail-closed behaviour.
//!
//! It uses ONLY the public API, exactly as `docs/src/replication/setup.md` and
//! `replica_ack_policy_test.rs` show an application wiring a replicated master:
//! `Environment::open` + `open_database`, a `ReplicatedEnvironment` that
//! `become_master`s, `env.set_replica_coordinator(rep_env)`, then a real
//! `db.put_in` + `commit_with_durability`.
//!
//! HISTORY — the defect this probe was first written to catch (R-REPUSER /
//! `BUG-REP-USER-WRITES-NOT-REPLICATED`, S1): a user `put`+commit through the
//! public data path was NEVER streamed to any replica, because
//! `noxu-db::Transaction::write_txn_end` logs `TxnCommit` with `NULL_VLSN` and
//! the feeder ships only VLSN-tagged entries. Under ack policy `None` the
//! commit "succeeded" locally and silently dropped the write; under
//! `SimpleMajority`/`All` it blocked the full ack timeout then returned a
//! misleading `InsufficientReplicas`. `get_current_vlsn()`/`commit_token()`
//! never advanced.
//!
//! FAIL-CLOSED (v7.11.1): the complete JE-faithful fix is v8.0.0; 7.11.x fails
//! closed at the public API. A user write on a data env wired to a
//! `ReplicatedEnvironment` coordinator is refused with
//! `NoxuError::ReplicationNotSupported` **at the write op, before any LN is
//! logged** (not merely at commit), so nothing durable is produced and no
//! application is silently misled. (See `rep_user_write_paths_probe.rs` for
//! the full per-path coverage: auto-commit, cursor, non-txnal, DDL.)
//!
//! WHAT THIS PROBE PINS (must NOT weaken): the write is refused (NOT a silent
//! `Ok`, NOT a misleading `InsufficientReplicas`), the master's VLSN
//! high-water and commit token never advance, and the write is not durable
//! after reopen.

use std::path::PathBuf;
use std::sync::Arc;

use noxu_rep::{NodeType, RepConfig, ReplicatedEnvironment, rep_node::RepNode};

fn build_master_env(node_name: &str) -> Arc<ReplicatedEnvironment> {
    let cfg = RepConfig::builder("probe_group", node_name, "127.0.0.1")
        .node_port(0)
        .node_type(NodeType::Electable)
        .build();
    Arc::new(ReplicatedEnvironment::new(cfg).unwrap())
}

fn add_peers(env: &ReplicatedEnvironment, n: u32) {
    for i in 1..=n {
        let peer = RepNode::new(
            format!("peer{}", i),
            NodeType::Electable,
            "127.0.0.1".into(),
            7_000 + i as u16,
            10 + i,
        );
        env.add_peer(peer).unwrap();
    }
}

use noxu_db::durability::{Durability, ReplicaAckPolicy, SyncPolicy};
use noxu_db::error::NoxuError;
use noxu_db::{DatabaseConfig, Environment, EnvironmentConfig};
use tempfile::TempDir;

fn run_refusal_probe(ack_policy: ReplicaAckPolicy, label: &str) {
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

    let rep_env = build_master_env("probe_master");
    rep_env.become_master(1).unwrap();
    add_peers(&rep_env, 1);

    let vlsn_before = rep_env.get_current_vlsn();
    let token_before = rep_env.commit_token();
    eprintln!(
        "[probe:{label}] before: get_current_vlsn={vlsn_before} \
         commit_token={:?}",
        token_before.as_ref().map(|t| t.vlsn())
    );

    env.set_replica_coordinator(rep_env.clone());
    // A long timeout: if the refusal were NOT at the write op / before the ack
    // wait, this would blow the timeout — the elapsed-time proxy is the
    // immediacy of the Err return below.
    env.set_replica_ack_timeout(std::time::Duration::from_secs(30));

    // A REAL user write through the public data path. The refusal now fires at
    // the write op (`put_in` -> `check_writable`), before any LN is logged.
    let txn = env.begin_transaction(None).unwrap();
    let started = std::time::Instant::now();
    let put_res = db.put_in(&txn, b"the-key", b"the-value");
    // If the write op did not refuse (older behaviour), fall through to commit,
    // which must then refuse — we accept either as "refused", but never a
    // silent Ok reaching a durable commit.
    let outcome: Result<(), NoxuError> = match put_res {
        Err(e) => Err(e),
        Ok(()) => {
            let dur =
                Durability::new(SyncPolicy::Sync, SyncPolicy::Sync, ack_policy);
            txn.commit_with_durability(dur)
        }
    };
    let elapsed = started.elapsed();

    let vlsn_after = rep_env.get_current_vlsn();
    let token_after = rep_env.commit_token();
    eprintln!("[probe:{label}] outcome = {:?} (after {:?})", outcome, elapsed);
    eprintln!(
        "[probe:{label}] after:  get_current_vlsn={vlsn_after} \
         commit_token={:?}",
        token_after.as_ref().map(|t| t.vlsn())
    );

    match outcome {
        Err(NoxuError::ReplicationNotSupported { reason }) => {
            assert!(!reason.is_empty(), "refusal must carry a reason");
            eprintln!(
                "[probe:{label}] VERDICT: fail-closed — refused with \
                 ReplicationNotSupported"
            );
        }
        Ok(()) => panic!(
            "[probe:{label}] S1 REGRESSION OR FIX: user write reached a durable \
             commit (Ok) — if replication now genuinely ships the write, update \
             this guard deliberately (v8.0.0); otherwise it was silently \
             dropped."
        ),
        other => panic!(
            "[probe:{label}] UNEXPECTED: {:?} after {:?} — expected \
             fail-closed ReplicationNotSupported.",
            other, elapsed
        ),
    }

    // Immediate — never reached the 30s ack timeout.
    assert!(
        elapsed < std::time::Duration::from_secs(5),
        "[probe:{label}] refusal must be immediate; waited {:?}",
        elapsed
    );
    // No silent tag-and-drop.
    assert_eq!(
        vlsn_after, vlsn_before,
        "[probe:{label}] VLSN advanced for a refused user write"
    );
    assert_eq!(
        token_after.map(|t| t.vlsn()).unwrap_or(0),
        0,
        "[probe:{label}] a non-zero commit token appeared for a refused write"
    );

    let _ = txn.abort();
    let _ = db.close();
    let _ = env.close();
    let _ = rep_env.close();

    // Not durable: reopen and confirm the key is absent.
    let reopen = EnvironmentConfig::new(PathBuf::from(tmp.path()))
        .with_transactional(true);
    let env2 = Environment::open(reopen).unwrap();
    let db2 = env2
        .open_database(
            None,
            "d",
            &DatabaseConfig::new().with_transactional(true),
        )
        .unwrap();
    assert!(
        db2.get(b"the-key").unwrap().is_none(),
        "[probe:{label}] refused write must not be durable after reopen"
    );
    let _ = db2.close();
    let _ = env2.close();
}

#[test]
fn user_put_commit_is_refused_simple_majority() {
    run_refusal_probe(ReplicaAckPolicy::SimpleMajority, "simple_majority");
}

#[test]
fn user_put_commit_is_refused_ack_none() {
    run_refusal_probe(ReplicaAckPolicy::None, "ack_none");
}

/// Non-vacuity control: the same put+commit on a standalone env (no
/// coordinator) commits Ok and is durable.
#[test]
fn user_put_commit_without_coordinator_is_ok_and_durable() {
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
    let txn = env.begin_transaction(None).unwrap();
    db.put_in(&txn, b"the-key", b"the-value").unwrap();
    let dur = Durability::new(
        SyncPolicy::Sync,
        SyncPolicy::Sync,
        ReplicaAckPolicy::SimpleMajority,
    );
    txn.commit_with_durability(dur)
        .expect("standalone env must commit a user write Ok");
    assert_eq!(db.get(b"the-key").unwrap().as_deref(), Some(&b"the-value"[..]));
    let _ = db.close();
    let _ = env.close();
}
