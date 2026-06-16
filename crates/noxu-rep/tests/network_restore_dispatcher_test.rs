//! Integration tests for F2 / F4: NetworkRestore wired through the
//! dispatcher path.
//!
//! Closes findings F2 and F4 of the 2026 review.
//!
//! The standalone `NetworkRestoreServer::start` listener works, but the
//! production `ReplicatedEnvironment` registers the RESTORE handler on the
//! `TcpServiceDispatcher`.  Before this wave, the only client
//! (`NetworkRestore::execute`) spoke the raw-TCP magic protocol, which was
//! incompatible with the dispatcher's service-name handshake.  A new
//! replica that joined a group could therefore not bootstrap.
//!
//! The fix: `NetworkRestore::execute_via_dispatcher` speaks the dispatcher
//! protocol and is exposed to operators via
//! `ReplicatedEnvironment::bootstrap_via_dispatcher(peer_name)`.

use noxu_rep::{NodeType, RepConfig, RepNode, ReplicatedEnvironment};
use std::fs;
use tempfile::TempDir;

#[test]
fn bootstrap_via_dispatcher_copies_ndb_files_from_peer() {
    // Source node: env_home contains a few `.ndb` files.
    let src_dir = TempDir::new().unwrap();
    let src_home = src_dir.path().to_path_buf();
    fs::write(src_home.join("00000001.ndb"), b"file 1 contents").unwrap();
    fs::write(src_home.join("00000002.ndb"), b"file 2 contents -- longer")
        .unwrap();
    fs::write(src_home.join("README.md"), b"not an ndb file").unwrap();

    let src_config = RepConfig::builder("g1", "src", "127.0.0.1")
        .node_port(0)
        .env_home(&src_home)
        .build();
    let src_env = ReplicatedEnvironment::new(src_config).unwrap();
    let src_addr = src_env.bound_addr().expect("source must bind");

    // Destination node: env_home is empty.  We register the source as a
    // peer and call bootstrap_via_dispatcher.
    let dst_dir = TempDir::new().unwrap();
    let dst_home = dst_dir.path().to_path_buf();
    let dst_config = RepConfig::builder("g1", "dst", "127.0.0.1")
        .node_port(0)
        .env_home(&dst_home)
        .build();
    let dst_env = ReplicatedEnvironment::new(dst_config).unwrap();

    dst_env
        .add_peer(RepNode::new(
            "src".to_string(),
            NodeType::Electable,
            "127.0.0.1".to_string(),
            src_addr.port(),
            2,
        ))
        .unwrap();

    // Bootstrap.
    dst_env.bootstrap_via_dispatcher("src").expect("bootstrap must succeed");

    // The two `.ndb` files must have been copied.
    let f1 = fs::read(dst_home.join("00000001.ndb")).unwrap();
    assert_eq!(f1, b"file 1 contents");
    let f2 = fs::read(dst_home.join("00000002.ndb")).unwrap();
    assert_eq!(f2, b"file 2 contents -- longer");
    // Non-`.ndb` files are NOT copied.
    assert!(!dst_home.join("README.md").exists());

    src_env.close().unwrap();
    dst_env.close().unwrap();
}

#[test]
fn bootstrap_via_dispatcher_rejects_unknown_peer() {
    let dst_dir = TempDir::new().unwrap();
    let config = RepConfig::builder("g1", "n1", "127.0.0.1")
        .node_port(0)
        .env_home(dst_dir.path())
        .build();
    let env = ReplicatedEnvironment::new(config).unwrap();

    let err = env.bootstrap_via_dispatcher("ghost");
    assert!(err.is_err(), "unknown peer must error");
    env.close().unwrap();
}

#[test]
fn bootstrap_via_dispatcher_requires_env_home() {
    let config =
        RepConfig::builder("g1", "n1", "127.0.0.1").node_port(0).build();
    let env = ReplicatedEnvironment::new(config).unwrap();

    let err = env.bootstrap_via_dispatcher("anyone");
    assert!(err.is_err(), "env without env_home must error");
    env.close().unwrap();
}

#[test]
fn dispatcher_path_round_trips_a_falling_behind_replica() {
    // Higher-level scenario: master writes a bunch of entries; a fresh
    // replica node opens with an empty env_home; replica calls
    // bootstrap_via_dispatcher to copy the master's log files; replica
    // can then resume.
    let master_dir = TempDir::new().unwrap();
    let master_home = master_dir.path().to_path_buf();

    // Pre-create some "log" files representing committed master state.
    for n in 1u32..=5 {
        let name = format!("{:08x}.ndb", n);
        let body = format!("master log file {}", n).into_bytes();
        fs::write(master_home.join(&name), &body).unwrap();
    }

    let master_cfg = RepConfig::builder("g1", "master", "127.0.0.1")
        .node_port(0)
        .env_home(&master_home)
        .build();
    let master_env = ReplicatedEnvironment::new(master_cfg).unwrap();
    let master_addr = master_env.bound_addr().expect("master must bind");

    // Fresh replica with empty env_home.
    let replica_dir = TempDir::new().unwrap();
    let replica_home = replica_dir.path().to_path_buf();
    let replica_cfg = RepConfig::builder("g1", "replica", "127.0.0.1")
        .node_port(0)
        .env_home(&replica_home)
        .build();
    let replica_env = ReplicatedEnvironment::new(replica_cfg).unwrap();
    replica_env
        .add_peer(RepNode::new(
            "master".to_string(),
            NodeType::Electable,
            "127.0.0.1".to_string(),
            master_addr.port(),
            1,
        ))
        .unwrap();

    // Pre-restore: empty.
    let entries: Vec<_> =
        fs::read_dir(&replica_home).unwrap().collect::<Result<_, _>>().unwrap();
    let pre_count = entries
        .iter()
        .filter(|e| {
            e.path()
                .extension()
                .and_then(|s| s.to_str())
                .is_some_and(|s| s == "ndb")
        })
        .count();
    assert_eq!(pre_count, 0);

    // Restore.
    replica_env
        .bootstrap_via_dispatcher("master")
        .expect("dispatcher restore must succeed");

    // Post-restore: 5 ndb files.
    let entries: Vec<_> =
        fs::read_dir(&replica_home).unwrap().collect::<Result<_, _>>().unwrap();
    let post_count = entries
        .iter()
        .filter(|e| {
            e.path()
                .extension()
                .and_then(|s| s.to_str())
                .is_some_and(|s| s == "ndb")
        })
        .count();
    assert_eq!(post_count, 5, "all 5 .ndb files must be present");

    // Spot-check content.
    let body = fs::read(replica_home.join("00000003.ndb")).unwrap();
    assert_eq!(body, b"master log file 3");

    master_env.close().unwrap();
    replica_env.close().unwrap();
}
