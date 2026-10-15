//! Deterministic witness for **BUG-REP-STATE-TOCTOU** (v7.11.1 item
//! **R-REPSTATE**): the check-then-act race in
//! `ReplicatedEnvironment::become_master` / `become_replica`.
//!
//! ## The bug
//!
//! At stack-v1, `become_master` was a two-step:
//!
//! ```text
//!     self.ensure_unknown_state()?;              // takes+releases state lock
//!     let old = self.node_state.get_state();     // takes+releases state lock
//!     self.node_state.transition_to(Master)?;    // takes+releases state lock
//! ```
//!
//! Each `NodeStateMachine::transition_to` takes the state write lock in a
//! *separate* critical section, so the compound change is check-then-act
//! across lock boundaries. A second actor — in production the election-driver
//! daemon (`open` -> `start_election_driver` -> `run_election_loop` ->
//! `become_master`), here a second application thread — can interleave in the
//! window between the `ensure_unknown_state` step and the final
//! `transition_to(Master)` step, drive the node to `Master` first, and leave
//! the first thread to attempt `MASTER -> MASTER`, which the state machine
//! rejects with `InvalidStateTransition` (also seen as `UNKNOWN -> UNKNOWN`).
//!
//! ## The witness
//!
//! A test-only mid-transition hook (gated `cfg(any(test, feature =
//! "test-harness"))`, absent from released builds) fires *at the exact
//! mid-point of the compound transition* — the window a second actor would
//! have interleaved into. Thread A begins `become_master`; its hook releases
//! thread B, which runs its own `become_master`, then A pauses briefly to
//! give B time to reach the state lock.
//!
//! * **GREEN (fixed code):** `transition_via_unknown` holds the state write
//!   lock across the *entire* compound transition, so B must block on the
//!   lock until A completes; B then observes the node already `Master` and
//!   returns an idempotent no-op success. Both calls return `Ok`.
//!
//! * **RED (stack-v1 code):** with the two-step released between hops, B
//!   interleaves, sets `Master`, and A's trailing `transition_to(Master)`
//!   fails `MASTER -> MASTER`. (Reproduced on a stack-v1 worktree with an
//!   equivalent hook spliced into the two-step; see the fix report.)
//!
//! The env is built with `ReplicatedEnvironment::new` (NOT `open`) so no
//! election-driver thread runs: the only contenders are this test's two
//! threads, making the witness fully deterministic.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use noxu_rep::node_state::NodeState;
use noxu_rep::{RepConfig, ReplicatedEnvironment};

fn single_node_cfg(dir: &std::path::Path) -> RepConfig {
    RepConfig::builder("toctou_grp", "n1", "127.0.0.1")
        .node_port(0)
        .env_home(dir.to_path_buf())
        .build()
}

/// Two threads race `become_master` with a deterministic interleave forced at
/// the mid-point of the compound transition. The fix must make BOTH succeed
/// (one real transition, one idempotent no-op) with zero
/// `InvalidStateTransition`.
#[test]
fn become_master_contended_mid_transition_is_atomic() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    // `new` (not `open`): no election-driver thread, so the only state
    // contenders are the two threads below.
    let env = Arc::new(
        ReplicatedEnvironment::new(single_node_cfg(tmp.path()))
            .expect("ReplicatedEnvironment::new"),
    );

    // Count InvalidStateTransition (or any) failures across both threads.
    let errors = Arc::new(AtomicUsize::new(0));

    // Hand-off: thread A's mid-transition hook releases thread B, which runs
    // its own become_master, then A waits briefly so B is guaranteed to reach
    // (and, under the fix, block on) the state lock while A still holds it.
    let b_release = Arc::new(std::sync::Barrier::new(2));

    {
        let env2 = Arc::clone(&env);
        let b_release_hook = Arc::clone(&b_release);
        // Install the hook on the SAME state machine both threads share.
        env.node_state_machine_for_test().set_mid_transition_hook(Some(
            Arc::new(move || {
                // Fired by thread A while it holds the state write lock, at
                // the compound-transition mid-point. Release B, then dwell so
                // B races into the lock during the window the old two-step
                // left open.
                b_release_hook.wait();
                std::thread::sleep(Duration::from_millis(50));
                // One-shot: a reentrant/second fire (e.g. B's own transition,
                // were it ever to run the Unknown->Master arm) must not
                // re-block. Clearing here keeps the witness to a single forced
                // interleave.
                env2.node_state_machine_for_test()
                    .set_mid_transition_hook(None);
            }),
        ));
    }

    let thread_a = {
        let env = Arc::clone(&env);
        let errors = Arc::clone(&errors);
        std::thread::spawn(move || {
            if env.become_master(1).is_err() {
                errors.fetch_add(1, Ordering::SeqCst);
            }
        })
    };

    let thread_b = {
        let env = Arc::clone(&env);
        let errors = Arc::clone(&errors);
        let b_release = Arc::clone(&b_release);
        std::thread::spawn(move || {
            // Wait until A is mid-transition, then contend.
            b_release.wait();
            if env.become_master(1).is_err() {
                errors.fetch_add(1, Ordering::SeqCst);
            }
        })
    };

    thread_a.join().expect("thread A");
    thread_b.join().expect("thread B");

    assert_eq!(
        errors.load(Ordering::SeqCst),
        0,
        "BUG-REP-STATE-TOCTOU: contended become_master must not raise \
         InvalidStateTransition; the compound transition must be atomic \
         under a single state write lock"
    );
    assert!(
        env.is_master(),
        "node must be Master after both contended become_master calls"
    );
    assert_eq!(env.get_state(), NodeState::Master);
}

/// Same contention, but the loser's intent is `become_replica` — proving the
/// atomic compound transition is coherent when the two contenders request
/// *different* active roles. Exactly one role wins; neither call errors.
#[test]
fn become_master_vs_become_replica_contended_is_atomic() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let env = Arc::new(
        ReplicatedEnvironment::new(single_node_cfg(tmp.path()))
            .expect("ReplicatedEnvironment::new"),
    );

    let errors = Arc::new(AtomicUsize::new(0));
    let b_release = Arc::new(std::sync::Barrier::new(2));

    {
        let env2 = Arc::clone(&env);
        let b_release_hook = Arc::clone(&b_release);
        env.node_state_machine_for_test().set_mid_transition_hook(Some(
            Arc::new(move || {
                b_release_hook.wait();
                std::thread::sleep(Duration::from_millis(50));
                env2.node_state_machine_for_test()
                    .set_mid_transition_hook(None);
            }),
        ));
    }

    let thread_a = {
        let env = Arc::clone(&env);
        let errors = Arc::clone(&errors);
        std::thread::spawn(move || {
            if env.become_master(1).is_err() {
                errors.fetch_add(1, Ordering::SeqCst);
            }
        })
    };

    let thread_b = {
        let env = Arc::clone(&env);
        let errors = Arc::clone(&errors);
        let b_release = Arc::clone(&b_release);
        std::thread::spawn(move || {
            b_release.wait();
            // B contends with become_replica. With the fix B blocks on the
            // state lock until A finishes, then runs its own atomic
            // Master->Unknown->Replica compound transition. Neither errors.
            if env.become_replica("n1").is_err() {
                errors.fetch_add(1, Ordering::SeqCst);
            }
        })
    };

    thread_a.join().expect("thread A");
    thread_b.join().expect("thread B");

    assert_eq!(
        errors.load(Ordering::SeqCst),
        0,
        "contended become_master vs become_replica must not raise \
         InvalidStateTransition"
    );
    // The exact final role depends on scheduling (B may land after A), but it
    // must be a well-defined active role, never a wedged/invalid state.
    let final_state = env.get_state();
    assert!(
        matches!(final_state, NodeState::Master | NodeState::Replica),
        "final state must be an active role, got {final_state}"
    );
}
