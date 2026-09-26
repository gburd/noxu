//! B6 / V15 / F2 (HIGH): the log cleaner must not delete a log file a lagging
//! replica still needs.
//!
//! JE pins every log file at or after the file containing the global CBVLSN
//! (the Cleaner Barrier VLSN — the minimum VLSN any still-attached electable
//! replica has acknowledged) via the `FileProtector`'s replication-protected
//! range, so the cleaner never reclaims a file a replica must still read
//! (`GlobalCBVLSN` / `LocalCBVLSNUpdater` + `FileProtector`).
//!
//! Noxu built both halves of this mechanism — `GroupService`'s CBVLSN
//! (`min(known_vlsn)` over active electable nodes) and
//! `noxu-cleaner::FileProtector` (whose own doc names "replication feeders" as
//! a consumer) — but **never wired them together**. Nothing in `noxu-rep` ever
//! protected a file for replication, so under sustained writes on the master a
//! lagging replica's needed files could be reclaimed by the cleaner, turning a
//! routine lag into an unavoidable full network restore.
//!
//! ## Fail-before / pass-after
//!
//! On base `b4cd7f7d`:
//! - `ReplicatedEnvironment` has no way to translate the CBVLSN into a
//!   file-protection bound, so the file protector protects nothing for
//!   replication. `replication_protected_file_floor()` does not exist / is
//!   `None`, and the cleaner would delete the lagging replica's files.
//!
//! On this branch:
//! - After each ack the master recomputes the CBVLSN, maps it to the log file
//!   that contains it via the `VlsnIndex`, and sets a replication-protected
//!   **file floor** on the shared `FileProtector`: every file at or after that
//!   floor is protected. Files fully below the CBVLSN can still be cleaned.
//! - As the lagging replica catches up, the CBVLSN advances, the floor rises,
//!   and the previously-protected older files become cleanable again.
//! - A non-replicated environment is unaffected (no floor is ever set).

use std::sync::Arc;

use noxu_cleaner::FileProtector;
use noxu_rep::net::channel::LocalChannelPair;
use noxu_rep::{NodeType, RepConfig, RepNode, ReplicatedEnvironment};

/// Deterministic VLSN→file layout used by these tests: `FILE_STRIDE` VLSNs per
/// log file, so VLSN `v` lives in file `(v - 1) / FILE_STRIDE`.
const FILE_STRIDE: u64 = 10;

fn file_for_vlsn(v: u64) -> u32 {
    ((v - 1) / FILE_STRIDE) as u32
}

fn master_cfg(group: &str, name: &str) -> RepConfig {
    RepConfig::builder(group, name, "127.0.0.1")
        .node_port(0)
        .node_type(NodeType::Electable)
        .build()
}

/// Register a deterministic VLSN→(file, offset) mapping in the master's VLSN
/// index so `CBVLSN → containing file` is exactly predictable.
fn seed_vlsn_index(env: &ReplicatedEnvironment, up_to: u64) {
    let idx = env.vlsn_index_arc();
    for v in 1..=up_to {
        idx.register(v, file_for_vlsn(v), ((v - 1) % FILE_STRIDE) as u32 * 16);
    }
}

/// Build a master with two electable replica peers and a directly-injected
/// `FileProtector` (the deterministic stand-in for the one the cleaner
/// consults — in production `with_environment` wires the real one from
/// `EnvironmentImpl::get_file_protector`).
fn master_with_two_replicas(
    group: &str,
) -> (Arc<ReplicatedEnvironment>, Arc<FileProtector>) {
    let env = Arc::new(ReplicatedEnvironment::new(master_cfg(group, "master")).unwrap());
    for (i, name) in ["replicaA", "replicaB"].iter().enumerate() {
        env.add_peer(RepNode::new(
            name.to_string(),
            NodeType::Electable,
            "127.0.0.1".to_string(),
            6_500 + i as u16,
            10 + i as u32,
        ))
        .unwrap();
    }
    // Register feeder objects so record_ack can advance per-replica acks.
    let pair_a = LocalChannelPair::new();
    let pair_b = LocalChannelPair::new();
    env.init_self_weak();
    env.register_feeder_channel("replicaA".to_string(), Arc::new(pair_a.channel_a));
    env.register_feeder_channel("replicaB".to_string(), Arc::new(pair_b.channel_a));

    let protector = Arc::new(FileProtector::new());
    env.set_replication_file_protector(Arc::clone(&protector));

    env.become_master(1).unwrap();
    (env, protector)
}

/// PRIMARY TEST: with a lagging replica, the file containing the CBVLSN and
/// every later file must be protected from the cleaner, while files fully
/// below the CBVLSN are cleanable.
#[test]
fn cbvlsn_protects_lagging_replica_files_from_cleaner() {
    let (env, protector) = master_with_two_replicas("b6_protect");

    // Master has rolled several log files: VLSN 1..=40 (files 0..=3).
    seed_vlsn_index(&env, 40);

    // replicaB has caught up to VLSN 40 (file 3); replicaA is lagging at
    // VLSN 15 (file 1). CBVLSN = min(15, 40) = 15 → containing file = 1.
    env.record_ack(40, "replicaB");
    env.record_ack(15, "replicaA");

    let floor = env
        .replication_protected_file_floor()
        .expect("replication is active: a protected file floor must be set");
    assert_eq!(
        floor,
        file_for_vlsn(15),
        "CBVLSN=15 lives in file {}, so the floor must pin file {} and above",
        file_for_vlsn(15),
        file_for_vlsn(15),
    );

    // File 0 (VLSN 1..=10) is fully below the CBVLSN → cleanable.
    assert!(
        !protector.is_protected(0),
        "file 0 is fully below the CBVLSN (15) and must be cleanable",
    );
    // File 1 contains the CBVLSN and files 2,3 are above it → all protected.
    for f in [1u32, 2, 3] {
        assert!(
            protector.is_protected(f),
            "file {f} is at/after the CBVLSN's file and must be protected \
             (FAIL-BEFORE: nothing wires the CBVLSN to the cleaner, so the \
             lagging replica's files get deleted)",
        );
    }

    env.close().unwrap();
}

/// As the lagging replica catches up, the CBVLSN advances and the
/// previously-protected older files become cleanable (protection released).
#[test]
fn protection_is_released_as_replica_catches_up() {
    let (env, protector) = master_with_two_replicas("b6_release");
    seed_vlsn_index(&env, 40);

    // Start: replicaA lagging at 15 (file 1), replicaB at 40.
    env.record_ack(40, "replicaB");
    env.record_ack(15, "replicaA");
    assert_eq!(env.replication_protected_file_floor(), Some(1));
    assert!(!protector.is_protected(0));
    assert!(protector.is_protected(1));

    // replicaA catches up to VLSN 25 (file 2). CBVLSN = min(25, 40) = 25 →
    // file 2. Files 0 and 1 are now fully below the CBVLSN and cleanable.
    env.record_ack(25, "replicaA");
    assert_eq!(
        env.replication_protected_file_floor(),
        Some(2),
        "floor must rise to the file containing the advanced CBVLSN (25)",
    );
    assert!(
        !protector.is_protected(1),
        "file 1 is now below the advanced CBVLSN (25) and must be released",
    );
    assert!(!protector.is_protected(0), "file 0 stays cleanable");
    assert!(protector.is_protected(2), "file 2 (contains CBVLSN 25) still protected");
    assert!(protector.is_protected(3), "file 3 (above CBVLSN) still protected");

    // replicaA fully catches up to 40. CBVLSN = 40 → file 3. Only file 3 pinned.
    env.record_ack(40, "replicaA");
    assert_eq!(env.replication_protected_file_floor(), Some(3));
    assert!(!protector.is_protected(2), "file 2 released once CBVLSN reaches 40");
    assert!(protector.is_protected(3));

    env.close().unwrap();
}

/// A non-replicated environment (no feeders / no protector wired) is
/// unaffected: no replication-protected floor is ever set.
#[test]
fn non_replicated_env_sets_no_protection_floor() {
    let env =
        Arc::new(ReplicatedEnvironment::new(master_cfg("b6_none", "solo")).unwrap());
    env.become_master(1).unwrap();
    // No peers, no protector injected, no acks.
    assert_eq!(
        env.replication_protected_file_floor(),
        None,
        "with no replicas there is nothing to protect for replication",
    );
    env.close().unwrap();
}
