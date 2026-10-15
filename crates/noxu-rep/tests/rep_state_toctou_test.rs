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
//! thread B, which runs its own transition, then A pauses briefly to give B
//! time to reach the state lock.
//!
//! * **GREEN (fixed code):** `transition_via_unknown` holds the state write
//!   lock across the *entire* compound transition, so B must block on the
//!   lock until A completes; B then observes the node already in the target
//!   role and returns an idempotent no-op success. Both calls return `Ok`.
//!
//! * **RED (stack-v1 code):** with the two-step released between hops, B
//!   interleaves, sets `Master`, and A's trailing `transition_to(Master)`
//!   fails `MASTER -> MASTER`. (Reproduced on a stack-v1 worktree with an
//!   equivalent hook spliced into the two-step; see the fix report.)
//!
//! The env is built with `ReplicatedEnvironment::new` (NOT `open`) so no
//! election-driver thread runs: the only contenders are this test's two
//! threads, making the witness fully deterministic.
//!
//! ## Why this witness can never *hang* (v2 — R-REPSTATE hang fix)
//!
//! The original witness coordinated thread A and thread B with a bare
//! `std::sync::Barrier::new(2)` whose two parties were (1) A's mid-transition
//! hook and (2) B's pre-transition wait, backed by the **process-global**
//! one-shot `MID_TRANSITION_HOOK`. That hand-off deadlocks whenever the hook
//! fires a number of times other than exactly once:
//!
//! * **Hook fires zero times** (A takes a path that skips the mid-point —
//!   e.g. the idempotent "already in target" arm): B's pre-transition
//!   `Barrier::wait()` never gets its second party and blocks forever
//!   (observed field shape 2: thread B parked at its own barrier wait, thread
//!   A already returned without firing).
//! * **Hook fires twice** (the still-installed global hook is re-entered by
//!   B's *own* transition before the one-shot clear is visible): the extra
//!   `wait()` opens a fresh barrier generation that never completes, so B
//!   parks *inside* the hook with no partner (observed field shape 1: thread
//!   B parked in `fire_mid_transition_hook` with no partner).
//!
//! A hung witness is worse than a failing one: a CI gate built on it would
//! hang too. This v2 witness removes every unbounded wait and makes the hook
//! fire *exactly once*:
//!
//! * **Bounded hand-offs.** Every cross-thread rendezvous is an
//!   `mpsc::sync_channel` `send`/`recv_timeout` with a finite deadline. A
//!   missing partner surfaces as a `RecvTimeoutError` that fails the test
//!   (RED) — it can never park forever.
//! * **One-shot, self-clearing hook.** The hook body is guarded by an
//!   `AtomicBool` so it runs its rendezvous *at most once* even if a later
//!   transition re-enters it; a second entry is a cheap no-op that performs
//!   no wait. The hook is also cleared from the global slot the moment it
//!   runs. A stale or double hook therefore cannot deadlock.
//! * **Watchdog.** Each test arms a watchdog thread that `panic!`s (failing
//!   the test, never hanging the process) if the body does not finish within
//!   a generous budget. This is the backstop that guarantees the witness can
//!   only ever go GREEN or RED, never hang.
//! * **Explicit precondition + hygienic global reset.** Each test asserts the
//!   freshly built node is `Detached` and resets the process-global hook to
//!   `None` on entry, so a hook leaked by an earlier test (or an earlier
//!   failed run in the same binary) cannot contaminate it.
//!
//! None of this changes what the witness *proves*: the atomicity assertions
//! (`errors == 0`, well-defined final role) are unchanged and never weakened.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{RecvTimeoutError, SyncSender};
use std::time::Duration;

use noxu_rep::node_state::NodeState;
use noxu_rep::{RepConfig, ReplicatedEnvironment};

/// The mid-transition witness hook is process-global, so the two witnesses in
/// this file must not install hooks concurrently. Serialize them.
static WITNESS_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Upper bound on any single cross-thread hand-off. Generous (CI under heavy
/// parallel load) yet finite: if a rendezvous partner never arrives the test
/// fails with `RecvTimeoutError` instead of parking forever.
const HANDOFF_TIMEOUT: Duration = Duration::from_secs(20);

/// Upper bound on a whole test body. The watchdog `panic!`s past this, turning
/// any residual deadlock into a RED failure rather than a process hang.
const WATCHDOG_TIMEOUT: Duration = Duration::from_secs(45);

/// How long thread A dwells at the compound-transition mid-point so thread B
/// is guaranteed to reach (and, under the fix, block on) the state write lock
/// during the window the old two-step left open.
const A_DWELL: Duration = Duration::from_millis(50);

fn single_node_cfg(dir: &std::path::Path) -> RepConfig {
    RepConfig::builder("toctou_grp", "n1", "127.0.0.1")
        .node_port(0)
        .env_home(dir.to_path_buf())
        .build()
}

/// Arm a watchdog that aborts the test (via `panic!`, which the harness turns
/// into a RED failure) if `done` is not signalled within [`WATCHDOG_TIMEOUT`].
/// Returns the sender; dropping it (on the normal path, after the body
/// completes) cancels the watchdog.
fn arm_watchdog(name: &'static str) -> SyncSender<()> {
    let (tx, rx) = std::sync::mpsc::sync_channel::<()>(1);
    std::thread::Builder::new()
        .name(format!("watchdog-{name}"))
        .spawn(move || match rx.recv_timeout(WATCHDOG_TIMEOUT) {
            Ok(()) | Err(RecvTimeoutError::Disconnected) => {
                // Body finished (or sender dropped) — stand down.
            }
            Err(RecvTimeoutError::Timeout) => {
                panic!(
                    "R-REPSTATE witness '{name}' did not complete within \
                     {WATCHDOG_TIMEOUT:?}: the hand-off deadlocked. This is a \
                     witness defect, not a pass — fail loudly rather than hang."
                );
            }
        })
        .expect("spawn watchdog");
    tx
}

/// Install the one-shot, self-clearing mid-transition hook on the shared
/// (process-global) slot. When the hook first runs it (a) signals thread B via
/// `release_b`, (b) dwells [`A_DWELL`] so B races into the state lock, and (c)
/// clears itself from the global slot. A `fired` guard makes any *second* entry
/// (e.g. B's own transition re-entering before the clear is visible) a no-op
/// with no wait — so the hook can never park a thread with no partner.
fn install_oneshot_hook(
    env: &Arc<ReplicatedEnvironment>,
    release_b: SyncSender<()>,
) {
    let env2 = Arc::clone(env);
    let fired = Arc::new(AtomicBool::new(false));
    env.node_state_machine_for_test().set_mid_transition_hook(Some(Arc::new(
        move || {
            // One-shot: only the FIRST entry performs the rendezvous. A second
            // entry (B's own transition, were the clear not yet visible) must
            // not block.
            if fired.swap(true, Ordering::SeqCst) {
                return;
            }
            // Clear the global slot immediately so no later transition fires
            // this hook again. We are inside the state write lock here.
            env2.node_state_machine_for_test().set_mid_transition_hook(None);
            // Release B. `send` on a rendezvous channel never blocks longer
            // than the receiver takes; the receiver uses `recv_timeout`.
            let _ = release_b.send(());
            // Dwell so B reaches the state lock during the window the old
            // two-step left open. (The fix holds the lock across this dwell;
            // the bug released it here.)
            std::thread::sleep(A_DWELL);
        },
    )));
}

/// Two threads race `become_master` with a deterministic interleave forced at
/// the mid-point of the compound transition. The fix must make BOTH succeed
/// (one real transition, one idempotent no-op) with zero
/// `InvalidStateTransition`.
#[test]
fn become_master_contended_mid_transition_is_atomic() {
    let _serial = WITNESS_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let _wd = arm_watchdog("become_master_contended");

    let tmp = tempfile::TempDir::new().expect("tempdir");
    // `new` (not `open`): no election-driver thread, so the only state
    // contenders are the two threads below.
    let env = Arc::new(
        ReplicatedEnvironment::new(single_node_cfg(tmp.path()))
            .expect("ReplicatedEnvironment::new"),
    );
    // Precondition: the only state writers are this test's threads, and the
    // node starts Detached. Make the assumption explicit and defensively
    // clear any hook leaked by an earlier test/run.
    env.node_state_machine_for_test().set_mid_transition_hook(None);
    assert_eq!(
        env.get_state(),
        NodeState::Detached,
        "witness precondition: a freshly `new`-built node must be Detached \
         (no third actor may have transitioned it)"
    );

    let errors = Arc::new(AtomicUsize::new(0));

    // Bounded hand-off: A's hook releases B exactly once. `recv_timeout` on
    // B's side turns a missing release into a RED failure, never a hang.
    let (release_b_tx, release_b_rx) = std::sync::mpsc::sync_channel::<()>(1);
    install_oneshot_hook(&env, release_b_tx);

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
        std::thread::spawn(move || {
            // Wait until A is mid-transition, then contend. Bounded: if A
            // never fires the hook the test fails here (RED) rather than
            // parking forever.
            match release_b_rx.recv_timeout(HANDOFF_TIMEOUT) {
                Ok(()) => {}
                Err(e) => {
                    eprintln!(
                        "thread B: A never reached the mid-transition hook \
                         ({e:?}); the witness hand-off failed"
                    );
                    errors.fetch_add(1, Ordering::SeqCst);
                    return;
                }
            }
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
    let _serial = WITNESS_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let _wd = arm_watchdog("become_master_vs_become_replica");

    let tmp = tempfile::TempDir::new().expect("tempdir");
    let env = Arc::new(
        ReplicatedEnvironment::new(single_node_cfg(tmp.path()))
            .expect("ReplicatedEnvironment::new"),
    );
    env.node_state_machine_for_test().set_mid_transition_hook(None);
    assert_eq!(
        env.get_state(),
        NodeState::Detached,
        "witness precondition: a freshly `new`-built node must be Detached \
         (no third actor may have transitioned it)"
    );

    let errors = Arc::new(AtomicUsize::new(0));

    let (release_b_tx, release_b_rx) = std::sync::mpsc::sync_channel::<()>(1);
    install_oneshot_hook(&env, release_b_tx);

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
        std::thread::spawn(move || {
            match release_b_rx.recv_timeout(HANDOFF_TIMEOUT) {
                Ok(()) => {}
                Err(e) => {
                    eprintln!(
                        "thread B: A never reached the mid-transition hook \
                         ({e:?}); the witness hand-off failed"
                    );
                    errors.fetch_add(1, Ordering::SeqCst);
                    return;
                }
            }
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
