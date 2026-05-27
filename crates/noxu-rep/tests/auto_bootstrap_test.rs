//! Wave 9-A fix 2 regression test: replica I/O thread auto-bootstraps via
//! the dispatcher when the master signals `NeedsRestore`.
//!
//! Prior behaviour (Wave 4-A): the replica thread observed
//! `NeedsRestore` (`Ok(false)` from `catch_up_from_peer`) and logged a
//! warning, expecting an operator to call `bootstrap_via_dispatcher`
//! manually.  Wave 9-A plumbs a `Weak<ReplicatedEnvironment>` into the
//! spawned thread so the thread can upgrade and call
//! `bootstrap_via_dispatcher` itself.
//!
//! This test:
//!   1. Builds a master `ReplicatedEnvironment` with a few pre-existing
//!      `.ndb` files in its `env_home`.  The master's `PeerLogScanner`
//!      is empty so any catch-up request from a replica with
//!      `start_vlsn=0` will be answered with `NEEDS_RESTORE`.
//!   2. Builds a replica with an empty `env_home` and a wired
//!      `EnvironmentImpl` so the replica I/O thread will spawn.
//!   3. Adds master as a peer.
//!   4. Calls `become_replica`.  The replica I/O thread should
//!      open a `PEER_FEEDER` channel to the master, receive
//!      `NEEDS_RESTORE`, upgrade the `Weak<Self>`, and call
//!      `bootstrap_via_dispatcher("master")` which copies the master's
//!      `.ndb` files into the replica's `env_home`.
//!   5. Verifies (within a bounded poll loop) that the master's `.ndb`
//!      files now exist in the replica's `env_home`.

use noxu_dbi::EnvironmentImpl;
use noxu_rep::{NodeType, RepConfig, RepNode, ReplicatedEnvironment};
use std::fs;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;

#[test]
fn replica_auto_bootstraps_on_needs_restore() {
    // --- master setup ----------------------------------------------------
    let master_dir = TempDir::new().unwrap();
    let master_home = master_dir.path().to_path_buf();

    // Pre-seed the master with `.ndb` files representing committed log
    // state.  The replica should end up with byte-identical copies.
    fs::write(master_home.join("00000001.ndb"), b"master log file 1").unwrap();
    fs::write(master_home.join("00000002.ndb"), b"master log file 2 longer")
        .unwrap();
    fs::write(master_home.join("00000003.ndb"), b"master log 3").unwrap();

    let master_cfg = RepConfig::builder("g_auto", "master", "127.0.0.1")
        .node_port(0)
        .env_home(&master_home)
        .build();
    let master_env = ReplicatedEnvironment::new(master_cfg).unwrap();
    let master_addr = master_env.bound_addr().expect("master must bind");

    // --- replica setup ---------------------------------------------------
    // The replica's env_home is a separate temp dir and the wired
    // `EnvironmentImpl` is opened there.  After `become_replica`, the
    // background I/O thread will receive `NEEDS_RESTORE` and trigger
    // `bootstrap_via_dispatcher` automatically.
    let replica_dir = TempDir::new().unwrap();
    let replica_home = replica_dir.path().to_path_buf();

    let replica_cfg = RepConfig::builder("g_auto", "replica", "127.0.0.1")
        .node_port(0)
        .env_home(&replica_home)
        .build();
    let replica_env =
        Arc::new(ReplicatedEnvironment::new(replica_cfg).unwrap());
    // Wave 9-A fix 2: register the self-weak so the I/O thread can
    // auto-bootstrap.  In production this is done by `open()`; for
    // tests that drive transitions manually we wire it explicitly.
    replica_env.init_self_weak();

    // Wire a real `EnvironmentImpl` so `become_replica` actually spawns
    // the I/O thread (the spawn is gated on `env_impl` being set AND
    // `get_log_manager()` returning `Some`).  Use a separate dir for
    // the live env so `EnvironmentImpl::new` does not collide with the
    // pre-seeded `.ndb` files we are testing the restore copy of.
    let live_env_dir = TempDir::new().unwrap();
    let env_impl = Arc::new(
        EnvironmentImpl::new(live_env_dir.path(), false, false).unwrap(),
    );
    replica_env.with_environment(env_impl);

    // Register the master as a peer so the replica thread can resolve
    // its address.
    replica_env
        .add_peer(RepNode::new(
            "master".to_string(),
            NodeType::Electable,
            "127.0.0.1".to_string(),
            master_addr.port(),
            1,
        ))
        .unwrap();

    // Sanity: replica_home is empty before transition.
    let pre: Vec<_> = fs::read_dir(&replica_home)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path()
                .extension()
                .and_then(|s| s.to_str())
                .is_some_and(|s| s == "ndb")
        })
        .collect();
    assert_eq!(pre.len(), 0, "replica env_home must start empty");

    // --- transition ------------------------------------------------------
    replica_env.become_replica("master").unwrap();

    // --- assertion: poll until the auto-bootstrap copies the .ndb files --
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut copied: usize = 0;
    while Instant::now() < deadline {
        copied = fs::read_dir(&replica_home)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path()
                    .extension()
                    .and_then(|s| s.to_str())
                    .is_some_and(|s| s == "ndb")
            })
            .count();
        if copied >= 3 {
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    assert!(
        copied >= 3,
        "replica I/O thread should auto-bootstrap and copy 3 .ndb files; \
         found {} after 10s",
        copied,
    );

    // Spot-check content.
    let body1 = fs::read(replica_home.join("00000001.ndb")).unwrap();
    assert_eq!(body1, b"master log file 1");
    let body2 = fs::read(replica_home.join("00000002.ndb")).unwrap();
    assert_eq!(body2, b"master log file 2 longer");
    let body3 = fs::read(replica_home.join("00000003.ndb")).unwrap();
    assert_eq!(body3, b"master log 3");

    // Cleanup.  Closing the replica signals the I/O thread to exit and
    // joins it.
    replica_env.close().unwrap();
    master_env.close().unwrap();
}
