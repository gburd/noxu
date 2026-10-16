//! GAP-REPLAY-3 (end-to-end): `ReplicatedEnvironment::become_replica` arms the
//! replica-write screen on the shared `EnvironmentImpl`, and `become_master`
//! clears it.  Combined with `noxu-db`'s `check_writable` + the cursor-level
//! `check_local_write_allowed` screen, a replica refuses local user writes
//! with `ReplicaWriteException` (JE `ReadonlyTxn.disallowReplicaWrite`), and a
//! master allows them.
//!
//! RED at 81b2f048: `EnvironmentImpl` had no replica-write screen at all, so a
//! node that became a replica still accepted local writes.

use std::sync::Arc;
use std::time::Duration;

use noxu_dbi::EnvironmentImpl;
use noxu_rep::{RepConfig, ReplicatedEnvironment};

fn cfg(group: &str, name: &str, env_home: &std::path::Path) -> RepConfig {
    RepConfig::builder(group, name, "127.0.0.1")
        .node_port(0)
        .env_home(env_home.to_path_buf())
        .heartbeat_interval(Duration::from_millis(200))
        .build()
}

#[test]
fn become_replica_arms_write_screen_become_master_clears_it() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let env_home = tmp.path().to_path_buf();

    let env_impl = Arc::new(
        EnvironmentImpl::new(&env_home, false, true)
            .expect("EnvironmentImpl::new"),
    );
    let rep_env = ReplicatedEnvironment::open(cfg("g_gap3", "n1", &env_home))
        .expect("open");
    rep_env.with_environment(Arc::clone(&env_impl));

    // Fresh node: screen off (writes allowed).
    assert!(
        !env_impl.is_replica_write_screened(),
        "a fresh env must not screen writes"
    );

    // Become a replica -> screen armed.
    rep_env.become_replica("some_master").expect("become_replica");
    assert!(
        env_impl.is_replica_write_screened(),
        "become_replica must arm the replica-write screen (GAP-REPLAY-3)"
    );

    // Become the master -> screen cleared.
    rep_env.become_master(1).expect("become_master");
    assert!(
        !env_impl.is_replica_write_screened(),
        "become_master must clear the replica-write screen"
    );

    let _ = rep_env.close();
}
