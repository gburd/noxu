# Gap B implementation notes (checkpoint @ ~29 min)

## 1. How `ReplicaReceiver::run_until` is structured

`crates/noxu-rep/src/stream/replica_stream.rs::ReplicaReceiver::run_until`
(around line 347):

```rust
pub fn run_until(&self, log_writer: &mut dyn LogWriter,
                  shutdown: Option<&AtomicBool>) -> Result<()> {
    let recv_timeout = if shutdown.is_some() { 1s } else { 30s };
    loop {
        if let Some(flag) = shutdown && flag.load(SeqCst) { return Ok(()); }
        let frame = match self.channel.receive(recv_timeout) { ... };
        // parse frame, verify CRC, enforce VLSN ordering (LOG-7),
        // validate entry type (LOG-10), write_entry, send ack.
    }
}
```

It already takes `Option<&AtomicBool>` and re-checks it every loop
iteration, with the receive timeout shortened to 1s specifically so an
external flag-flip is observed promptly. This is the *exact* idiom
`ReplicatedEnvironment::close()` already uses to interrupt a blocked
receiver thread (`io_shutdown` flip + `handle.join()`). **It is cleanly
interruptible** — no refactor needed to add a second stop condition, as
long as that condition can be expressed as "check a flag/predicate every
≤1s", which is exactly what a master-change signal needs.

Answer to judgement question 1: **not a large refactor.** The loop is
already interruptible via the established flag-polling pattern. The
missing piece is not "make the loop interruptible" (it already is) — it's
"give the *caller* (the become_replica-spawned closure) an outer loop that
resolves the master fresh, re-syncs, and re-enters streaming with a stop
condition that also trips on master change," which is a moderate, well-
scoped addition, not a rewrite of the receive loop itself.

## 2. Where master identity is observable from the replica thread

- `self.master_tracker: MasterTracker` (`elections/master_tracker.rs`) —
  `set_master(name, term)` / `get_master()` / `get_term()`. Currently has
  **no generation/version counter** and no notification mechanism — it's
  passive storage. `become_replica` calls `master_tracker.set_master(name, 0)`
  unconditionally (note: term is hardcoded `0` here, not the election term —
  pre-existing, separate from Gap B).
- `become_replica`'s spawned closure (`replicated_environment.rs` ~2671-3025)
  captures `master_addr_opt` (resolved ONCE at spawn from `group_service`)
  and `master: String` (captured once). It never re-reads `master_tracker`
  after spawn.
- The election driver (`run_election_loop`, ~1150-1330) is the thing that
  calls `become_replica(&winner_node.name)` again when this node loses a
  *new* election — i.e. `become_replica` can be called more than once over
  a node's lifetime, once per election it loses.

## 3. Architectural blocker found: `become_replica` is not idempotent w.r.t. thread spawn

This is the important finding, and it changes the shape of the fix from
"add a check inside the existing thread's loop" to "also make
`become_replica` idempotent."

`become_replica` (~2671) does, on every call where an `EnvironmentImpl` is
wired:
```rust
let handle = std::thread::Builder::new()
    .name(format!("noxu-replica-{}", node_name))
    .spawn(move || { /* one-shot: syncup once, then catch_up_from_peer_until
                         in a small retry loop, then return */ })
    .expect(...);
self.io_threads.lock().unwrap().push(handle);
```

There is **no check** for "is a receive thread already running for this
node" before spawning, and no signal sent to a previously-spawned thread to
stop. `NodeState` transitions force `Replica -> Unknown -> Replica` on a
second `become_replica` call (`node_state.rs`: `Replica.can_transition_to`
includes `Unknown`/`Master`/`Shutdown` but not `Replica` itself), so
`ensure_unknown_state()` + `transition_to(Replica)` both succeed silently —
nothing in the state machine catches the double-spawn.

Consequence: if the election driver calls `become_replica(new_master)` while
the *previous* master is still alive and its receive thread's channel has
not yet errored/closed (typical mid-stream failover: old master still up,
just lost an election to a higher-term/higher-VLSN peer), **two
`noxu-replica-*` threads run concurrently**, each with its own
`EnvironmentLogWriter`/`ReplicaReplay`, both driving `log_with_vlsn` and
`replay.apply_entry` against the *same* live `EnvironmentImpl`. The LOG-7
VLSN-ordering check would likely make one of them error out once VLSNs
diverge, but that's a race outcome, not a designed teardown — it is exactly
the kind of "polling hack that races" the task told me not to build, except
this one is latent in the *existing* code, not something I'd be adding.

This means a correct Gap B fix needs three pieces, not one:

1. **`MasterTracker`: add a generation counter**, bumped whenever
   `set_master`/`update_master` changes the `(name, term)` pair. This is the
   "new atomic generation counter ... checked between frames" the prior
   session's notes already proposed — small, additive, no new concurrency
   primitive (same `RwLock<u64>` shape as `master_term`).

2. **`become_replica`: idempotent-spawn guard.** Mirror the existing daemon
   pattern (`start_vlsn_persistence_daemon` / `start_election_driver`):
   check whether a `noxu-replica-<node>` thread is already alive (a
   dedicated `AtomicBool "replica_thread_running"` is simpler and less
   fragile than name-sniffing `io_threads`, since `io_threads` mixes
   multiple daemon kinds with no tagging) before spawning. If one is
   already running, `become_replica` just updates `master_tracker` (already
   does this) and returns — the *running* thread's own outer loop (piece 3)
   picks up the change via the generation counter. Clear the flag when the
   thread naturally exits (channel closed for good / diverged-refused /
   shutdown) — mirrors the existing `io_shutdown` + join lifecycle in
   `close()`, just scoped to this one thread instead of the whole node.

3. **Outer `'reconnect: loop` in the spawned closure**, replacing today's
   one-shot syncup-then-stream body:
   ```
   'reconnect: loop {
       if io_shutdown or !replica_thread_should_continue { return }
       let (addr, epoch) = resolve current master fresh from group_service
                            + master_tracker.generation();
       run syncup_with_feeder_at(addr) — on DivergedRefused/NeedsRestore:
           return (do NOT loop back into syncup against a node that just
           told us it can't reconcile our tail — matches today's early
           `return` exactly, just now reachable on re-entry too, not only
           at spawn).
       stream via run_until(writer, Some(&stop_flag)) where stop_flag is
       polled the SAME way io_shutdown already is, extended with ONE more
       check alongside it: `master_tracker.generation() != epoch` — no new
       thread, no new synchronization primitive, just one more condition
       checked at the same poll point `run_until` already re-checks every
       ≤1s. This requires either widening `run_until`'s `Option<&AtomicBool>`
       parameter to a small predicate type, or adding a sibling method; I
       lean toward changing the shutdown parameter's type from
       `Option<&AtomicBool>` to `Option<&dyn Fn() -> bool>` (or a tiny
       enum/struct) so callers can compose "io_shutdown OR generation
       changed" without a helper thread. This does touch `run_until`'s
       signature, which several existing tests + `catch_up_from_peer_until`
       call directly — needs care, not a rewrite.
       On stream end (channel closed / VLSN error) or generation mismatch:
       continue 'reconnect (re-resolve, re-syncup, re-stream).
   }
   ```

None of this requires touching `noxu-tree`/`noxu-txn` internals or the
syncup safety gate itself — it's confined to `master_tracker.rs`,
`replicated_environment.rs` (the `become_replica` spawn block), and
`replica_stream.rs`'s `run_until` signature. **Judgement: this is a
moderate, well-scoped refactor, not a "large refactor" that should trigger
a STOP** — but it is bigger than "watch a flag inside the existing loop"
because of the latent double-spawn issue in (2), which I did not expect
before reading `become_replica` closely. Proceeding to implement all three
pieces.

## 4. Interaction with the default-deny safety gate (judgement question 2)

Confirmed and important: **re-syncup on master change is exactly the
scenario most likely to hit `DivergedRefused`,** because:

- The *old* one-shot design streamed from whatever master it was pointed at
  for the node's entire lifetime, with syncup run only once at spawn. A
  mid-stream failover today means the replica silently keeps consuming
  frames from the stale channel (per the gap description) until that
  channel breaks on its own (old master crash) — it never re-evaluates
  divergence against the new master at all.
- After the fix, every master change triggers a **fresh full syncup**
  against the new master, which re-runs `classify_tail`/`verify_rollback`.
  A replica that was mid-stream from the old master and has applied entries
  the new master never saw (classic failover — old master accepted writes
  the new master, elected from a different point, does not have) is now
  correctly detected as diverged. If that divergent tail contains a
  committed txn-end or a non-transactional LN above the matchpoint, the gate
  now **correctly refuses** (`DivergedRefused`) where the pre-fix code would
  have silently kept streaming garbage on top of stale state.
- This is a **behavior change, and an improvement**, not a regression: the
  replica now either (a) rolls back a safely-truncatable provisional tail
  and converges, or (b) refuses and stops, surfacing the need for a network
  restore — instead of (pre-fix) silently corrupting its view by streaming
  the new master's history on top of undetected stale/divergent state.
- This MUST be covered by an explicit test, not just implied: I will add
  one scenario where the divergent tail is *safely truncatable* (rollback +
  resume) and one where it is *not* (commit above matchpoint on the old
  master's tail → `DivergedRefused` after failover, replica stops instead of
  continuing against the stale master or corrupting itself against the new
  one). The `NeedsRestore`/`DivergedRefused` early-`return` in the spawned
  closure already matches this; the outer-loop version must preserve the
  exact same early-return semantics on re-entry (see piece 3 above) rather
  than retrying the syncup in a tight loop.

## Next steps

1. Implement `MasterTracker` generation counter + tests.
2. Implement `become_replica` idempotent-spawn guard.
3. Change `run_until`'s shutdown parameter to a composable predicate (or add
   a sibling entry point) and update the ~10 existing call sites/tests.
4. Wrap the spawned closure's body in the outer reconnect loop.
5. Write the fails-on-base test: two-master mid-stream handoff using real
   TCP (`RepConfig` + `with_environment`, following
   `rep1_step5_live_syncup_test.rs` / `auto_bootstrap_test.rs` patterns) —
   one variant asserting rollback+converge, one asserting
   `DivergedRefused`-and-stop.
