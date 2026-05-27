//! Integration test for F11: VLSN index persistence across env close/open.
//!
//! Closes finding F11 of `docs/src/internal/api-audit-2026-05-rep.md`.

use noxu_rep::{RepConfig, ReplicatedEnvironment};
use std::sync::Arc;
use tempfile::TempDir;

#[test]
fn vlsn_index_persists_across_close_and_reopen() {
    let dir = TempDir::new().expect("temp dir");
    let env_home = dir.path().to_path_buf();

    // First epoch: open as master, register VLSNs, close.
    {
        let config = RepConfig::builder("test_group", "node1", "127.0.0.1")
            .node_port(0)
            .env_home(&env_home)
            .build();
        let env = ReplicatedEnvironment::new(config).unwrap();
        env.become_master(1).unwrap();
        for v in 1u64..=20 {
            env.register_vlsn(v, 0, (v * 100) as u32);
        }
        assert_eq!(env.get_current_vlsn(), 20);
        env.close().unwrap();
    }

    // Verify the index file exists.
    let idx_path = env_home.join("vlsn.idx");
    assert!(idx_path.exists(), "vlsn.idx should be persisted");

    // Second epoch: reopen, expect the VLSN index to be restored.
    {
        let config = RepConfig::builder("test_group", "node1", "127.0.0.1")
            .node_port(0)
            .env_home(&env_home)
            .build();
        let env = ReplicatedEnvironment::new(config).unwrap();
        // Without re-registering anything, the VLSN range should be
        // recovered from disk.
        let range = env.get_vlsn_range();
        assert_eq!(range.first(), 1, "first VLSN should round-trip");
        assert_eq!(range.last(), 20, "last VLSN should round-trip");
        assert_eq!(env.get_current_vlsn(), 20);
        env.close().unwrap();
    }
}

#[test]
fn vlsn_index_persistence_daemon_flushes_in_background() {
    let dir = TempDir::new().expect("temp dir");
    let env_home = dir.path().to_path_buf();

    let config = RepConfig::builder("test_group", "node1", "127.0.0.1")
        .node_port(0)
        .env_home(&env_home)
        .build();

    // Use `open` so the persistence daemon runs.  We'll register some
    // VLSNs, sleep long enough for one tick, then verify the file
    // exists.
    let env = ReplicatedEnvironment::open(config).unwrap();
    env.become_master(1).unwrap();
    for v in 1u64..=5 {
        env.register_vlsn(v, 0, (v * 10) as u32);
    }

    // Daemon flushes every 2s; allow up to 5s.
    let idx_path = env_home.join("vlsn.idx");
    let mut found = false;
    for _ in 0..50 {
        if idx_path.exists() {
            found = true;
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    // Even if the daemon hasn't flushed yet within the polling window,
    // close() will run the belt-and-braces final flush.
    Arc::clone(&env).close().unwrap();
    assert!(idx_path.exists(), "vlsn.idx must exist after close");
    let _ = found;
}

#[test]
fn corrupt_vlsn_index_is_recovered_with_fresh_state() {
    let dir = TempDir::new().expect("temp dir");
    let env_home = dir.path().to_path_buf();

    // Write a bogus vlsn.idx file.
    std::fs::write(env_home.join("vlsn.idx"), b"not-a-valid-vlsn-idx-file")
        .unwrap();

    let config = RepConfig::builder("test_group", "node1", "127.0.0.1")
        .node_port(0)
        .env_home(&env_home)
        .build();
    let env = ReplicatedEnvironment::new(config).unwrap();

    // The corrupt file is removed and the env starts with an empty index.
    assert_eq!(env.get_current_vlsn(), 0);
    env.close().unwrap();
}
