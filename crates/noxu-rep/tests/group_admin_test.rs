//! Integration tests for F7 (transfer_master) and F8 (shutdown_group).
//!
//! Closes findings F7 and F8 of `docs/src/internal/api-audit-2026-05-rep.md`.

use noxu_rep::{
    master_transfer::MasterTransferConfig, NodeType, RepConfig, RepNode,
    ReplicatedEnvironment,
};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

fn config(name: &str, env_home: &std::path::Path) -> RepConfig {
    RepConfig::builder("g1", name, "127.0.0.1")
        .node_port(0)
        .env_home(env_home)
        .build()
}

/// Build a `ReplicatedEnvironment` with the ADMIN service registered
/// but WITHOUT the election driver (which would otherwise spontaneously
/// elect a 1-node group when run via `open`).
fn admin_env(name: &str, env_home: &std::path::Path) -> Arc<ReplicatedEnvironment> {
    let env = Arc::new(
        ReplicatedEnvironment::new(config(name, env_home)).unwrap(),
    );
    env.register_admin_service();
    env
}

#[test]
fn transfer_master_demotes_old_and_promotes_new() {
    let dir1 = TempDir::new().unwrap();
    let dir2 = TempDir::new().unwrap();

    let master_env = admin_env("master", dir1.path());
    let master_addr =
        master_env.bound_addr().expect("master must bind");

    let target_env = admin_env("target", dir2.path());
    let target_addr =
        target_env.bound_addr().expect("target must bind");

    // Have master become master and register the target as a peer.
    master_env.become_master(1).unwrap();
    master_env
        .add_peer(RepNode::new(
            "target".to_string(),
            NodeType::Electable,
            "127.0.0.1".to_string(),
            target_addr.port(),
            2,
        ))
        .unwrap();

    // Target needs to know master's address too (for the demoted master to
    // become its replica).
    target_env
        .add_peer(RepNode::new(
            "master".to_string(),
            NodeType::Electable,
            "127.0.0.1".to_string(),
            master_addr.port(),
            1,
        ))
        .unwrap();

    assert!(master_env.is_master());
    assert!(!target_env.is_master());

    // Initiate transfer.
    let cfg = MasterTransferConfig::new(
        "target".to_string(),
        Duration::from_secs(5),
    );
    Arc::clone(&master_env).transfer_master(cfg).expect("transfer must succeed");

    // Old master is now a replica of target.
    assert!(
        master_env.is_replica(),
        "old master must be replica after transfer (got state {:?})",
        master_env.get_state()
    );
    assert_eq!(master_env.get_master_name(), Some("target".to_string()));

    // Target became master at term 2 (master's previous term was 1).
    // Allow a short grace window for the ADMIN handler to apply the
    // command on the target side.
    let mut target_is_master = target_env.is_master();
    for _ in 0..50 {
        if target_is_master {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
        target_is_master = target_env.is_master();
    }
    assert!(
        target_is_master,
        "target must be master after transfer (got state {:?})",
        target_env.get_state()
    );

    Arc::clone(&master_env).close().unwrap();
    Arc::clone(&target_env).close().unwrap();
}

#[test]
fn transfer_master_rejects_unknown_target() {
    let dir = TempDir::new().unwrap();
    let env = admin_env("master", dir.path());
    env.become_master(1).unwrap();

    let cfg = MasterTransferConfig::new(
        "ghost".to_string(),
        Duration::from_secs(1),
    );
    let res = Arc::clone(&env).transfer_master(cfg);
    assert!(res.is_err(), "transfer to unknown peer must fail");
    Arc::clone(&env).close().unwrap();
}

#[test]
fn shutdown_group_closes_master_and_signals_replicas() {
    let dir1 = TempDir::new().unwrap();
    let dir2 = TempDir::new().unwrap();

    let master_env = admin_env("master", dir1.path());
    let replica_env = admin_env("replica", dir2.path());
    let replica_addr =
        replica_env.bound_addr().expect("replica must bind");

    master_env.become_master(1).unwrap();
    master_env
        .add_peer(RepNode::new(
            "replica".to_string(),
            NodeType::Electable,
            "127.0.0.1".to_string(),
            replica_addr.port(),
            2,
        ))
        .unwrap();

    // Master initiates shutdown_group with a 5 s replica timeout.
    Arc::clone(&master_env).shutdown_group(5_000).expect(
        "shutdown_group must succeed",
    );

    // Master is closed.
    assert!(master_env.is_shutdown());

    // Replica also closed (the ADMIN handler called close()).
    let mut replica_closed = replica_env.is_shutdown();
    for _ in 0..50 {
        if replica_closed {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
        replica_closed = replica_env.is_shutdown();
    }
    assert!(
        replica_closed,
        "replica must be closed after master shutdown_group"
    );
}

#[test]
fn shutdown_group_only_runs_on_master() {
    let dir = TempDir::new().unwrap();
    let env = admin_env("replica", dir.path());
    env.become_replica("phantom").unwrap();
    let res = Arc::clone(&env).shutdown_group(1_000);
    assert!(res.is_err(), "shutdown_group on a replica must fail");
    Arc::clone(&env).close().unwrap();
}
