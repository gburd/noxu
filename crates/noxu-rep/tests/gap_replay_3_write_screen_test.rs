//! GAP-REPLAY-3 (end-to-end): `ReplicatedEnvironment::become_replica` arms the
//! replica-write screen on the shared `EnvironmentImpl`, and `become_master`
//! clears it.  Combined with `noxu-db`'s `check_writable` + the cursor-level
//! `check_local_write_allowed` screen, a replica refuses local user writes
//! with `ReplicaWriteException` (JE `ReadonlyTxn.disallowReplicaWrite`), and a
//! master allows them.
//!
//! RED at 81b2f048: `EnvironmentImpl` had no replica-write screen at all, so a
//! node that became a replica still accepted local writes.
//!
//! BUG-REP-SCREEN-RACE: the screen must track the role ATOMICALLY.  The first
//! case asserts the role->screen INVARIANT (read atomically) rather than a
//! bare "screen armed" after `become_replica`, because a single-node group's
//! own election driver legitimately promotes the lone node to Master (quorum
//! of 1), concurrently with the test -- so a bare post-`become_replica`
//! "screen armed" assertion races the driver and flaked (the node had already
//! become its own Master and cleared the screen).  What must ALWAYS hold is
//! the invariant: Replica <=> armed, Master <=> cleared.  The second case
//! stress-contends the two transitions from two threads to witness that the
//! pair never tears (the S2 hole).

use std::sync::Arc;
use std::time::Duration;

use noxu_dbi::EnvironmentImpl;
use noxu_rep::{NodeState, RepConfig, ReplicatedEnvironment};

fn cfg(group: &str, name: &str, env_home: &std::path::Path) -> RepConfig {
    RepConfig::builder(group, name, "127.0.0.1")
        .node_port(0)
        .env_home(env_home.to_path_buf())
        .heartbeat_interval(Duration::from_millis(200))
        .build()
}

/// Assert the role->screen invariant, reading the pair ATOMICALLY under the
/// state lock so a concurrent transition cannot make the two reads disagree.
///
/// Invariant (JE `RepImpl.checkIfMaster`: only MASTER may originate a write;
/// REPLICA/UNKNOWN/DETACHED throw): the replica-write screen is armed iff the
/// node is NOT the Master.  A non-Master-with-screen-cleared sample is the S2
/// silent-divergence hole (a non-master accepting a local user write).
fn assert_role_screen_invariant(rep_env: &ReplicatedEnvironment, ctx: &str) {
    let (role, screen) =
        rep_env.node_state_machine_for_test().role_and_screen();
    if let Some(armed) = screen {
        if matches!(role, NodeState::Master) {
            assert!(
                !armed,
                "{ctx}: node reports Master but the replica-write screen is \
                 ARMED -- a legitimate master write would be spuriously refused"
            );
        } else {
            assert!(
                armed,
                "{ctx}: node reports {role:?} (not Master) but the \
                 replica-write screen is CLEARED -- a local user write would \
                 be accepted on a non-master (S2 silent divergence); JE \
                 RepImpl.checkIfMaster throws ReplicaWriteException / \
                 UnknownMasterException for every non-Master role"
            );
        }
    }
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

    // A wired replicated env that is NOT yet the Master (Detached / Unknown)
    // screens writes -- JE `RepImpl.checkIfMaster` throws
    // `UnknownMasterException` for UNKNOWN/DETACHED (RepImpl.java:1087-1091),
    // so a node that has not become the master originates no user write.  We
    // assert the role->screen INVARIANT (read atomically), not a fixed value,
    // because this single-node group's own election driver may already have
    // promoted the lone node to Master (quorum of 1).
    assert_role_screen_invariant(&rep_env, "fresh wired env");

    // Become a replica.  The screen is armed ATOMICALLY with the Replica
    // transition, so the invariant (not Master => armed) holds the instant
    // `become_replica` returns.  (We assert the INVARIANT, not a bare "armed",
    // because the election driver may already have promoted the lone node to
    // its own Master -- a consistent Master-with-screen-cleared, not a torn
    // pair.  The former bare assertion raced that driver and flaked.)
    rep_env.become_replica("some_master").expect("become_replica");
    assert_role_screen_invariant(&rep_env, "after become_replica");

    // Become the master -> screen cleared, atomically.
    rep_env.become_master(1).expect("become_master");
    assert_role_screen_invariant(&rep_env, "after become_master");
    // Once we are the undisputed Master (no further role change here), the
    // screen is cleared.
    assert!(
        rep_env.get_state() != NodeState::Replica,
        "after become_master the node must not be a Replica"
    );
    assert!(
        !env_impl.is_replica_write_screened(),
        "become_master must clear the replica-write screen"
    );

    let _ = rep_env.close();
}

/// BUG-REP-SCREEN-RACE (the `become_replica_arms_write_screen_...` flake root
/// cause): the replica-write screen and the node role must update ATOMICALLY,
/// so no observer ever sees `role == Replica` with the screen CLEARED (the S2
/// silent-divergence hole: a replica that would accept a local user write) or
/// `role == Master` with the screen ARMED.
///
/// Before the fix, `become_replica` / `become_master` stored the screen
/// OUTSIDE the state write lock, in opposite orders, so a concurrent
/// transition (the election-driver daemon, a master-transfer handler, and an
/// application thread are all real callers) could leave the pair inconsistent.
/// This witness drives the two transitions from two threads and samples the
/// `(role, screen)` pair atomically under the state read lock
/// (`NodeStateMachine::role_and_screen`); it panics on any inconsistent pair.
///
/// RED before the fix (fails within a few hundred iterations on x86, almost
/// immediately on aarch64); GREEN after (the pair is flipped inside the one
/// state write lock).  Deterministic in the sense that it does not rely on the
/// election driver's timing -- it forces the contended transitions directly.
#[test]
fn become_replica_become_master_keep_role_and_screen_consistent() {
    use std::sync::atomic::{AtomicBool, Ordering};

    let tmp = tempfile::TempDir::new().expect("tempdir");
    let env_home = tmp.path().to_path_buf();

    let env_impl = Arc::new(
        EnvironmentImpl::new(&env_home, false, true)
            .expect("EnvironmentImpl::new"),
    );
    let rep_env =
        ReplicatedEnvironment::open(cfg("g_gap3race", "n1", &env_home))
            .expect("open");
    rep_env.with_environment(Arc::clone(&env_impl));

    let stop = Arc::new(AtomicBool::new(false));
    let rounds = 20_000usize;

    // Thread A: hammer become_replica.
    let a_env = Arc::clone(&rep_env);
    let a_stop = Arc::clone(&stop);
    let ta = std::thread::spawn(move || {
        while !a_stop.load(Ordering::Relaxed) {
            let _ = a_env.become_replica("some_master");
            std::thread::yield_now();
        }
    });
    // Thread B: hammer become_master.
    let b_env = Arc::clone(&rep_env);
    let b_stop = Arc::clone(&stop);
    let tb = std::thread::spawn(move || {
        let mut term = 1u64;
        while !b_stop.load(Ordering::Relaxed) {
            let _ = b_env.become_master(term);
            term += 1;
            std::thread::yield_now();
        }
    });

    // Sampler: read the (role, screen) pair atomically and check the
    // invariant.  The screen is the sole S2 gate on the write path
    // (cursor_impl `check_local_write_allowed` / environment `check_writable`
    // read ONLY the screen, never the role), so a non-Master-with-screen-
    // cleared sample is a live write-acceptance window.  JE `checkIfMaster`
    // admits only MASTER, so the invariant is: armed iff NOT Master.
    let sm = rep_env.node_state_machine_for_test();
    for _ in 0..rounds {
        let (role, screen) = sm.role_and_screen();
        if let Some(armed) = screen {
            if matches!(role, NodeState::Master) {
                assert!(
                    !armed,
                    "node reports Master but the replica-write screen is \
                     ARMED -- a legitimate master write would be spuriously \
                     refused. (role, screen) = ({role:?}, {armed})"
                );
            } else {
                assert!(
                    armed,
                    "S2 HOLE: node reports {role:?} (not Master) but the \
                     replica-write screen is CLEARED -- a local user write \
                     would be accepted on a non-master (silent divergence). \
                     (role, screen) = ({role:?}, {armed})"
                );
            }
        }
        std::thread::yield_now();
    }

    stop.store(true, Ordering::Relaxed);
    let _ = ta.join();
    let _ = tb.join();
    let _ = rep_env.close();
}
