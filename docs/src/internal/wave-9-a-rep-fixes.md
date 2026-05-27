# Wave 9-A — Three small noxu-rep fixes (v2.1.1 / v2.2.0)

Wave 9-A closes three follow-up items left open after the v2.1.0 release:
the `become_master` invariant on non-electable node types (Wave 8 follow-up),
the replica auto-bootstrap path on `NeedsRestore` (Wave 4-A follow-up), and
a long-standing flake in the phi-detector integration test (pre-existing
since v1.5.0).

All three are small, surgical changes confined to `crates/noxu-rep/`.  No
changes to public types other than one new method
(`ReplicatedEnvironment::init_self_weak`), which is additive and
backward-compatible.

## Fix 1 — `become_master` rejects non-electable node types

JE invariant: only `Electable` nodes can become master.  `Secondary`,
`Monitor`, and `Arbiter` must be rejected at the API layer
(`je.rep.txn.ExceptionTest`).  Noxu's previous implementation accepted
`Secondary` nodes and transitioned them to `NodeState::Master`, which
silently violated the invariant — a Secondary that ran an election could
end up "master" of a group that should not have elected it.

### Change

A 6-line guard at the top of `become_master`:

```rust
if !self.config.node_type.can_be_master() {
    return Err(RepError::InvalidStateTransition(format!(
        "node '{}' has type {} which is not electable as master",
        self.config.node_name.as_str(),
        self.config.node_type,
    )));
}
```

`NodeType::can_be_master()` already encodes the JE invariant
(`Electable` only).  The guard runs after the `is_shutdown()` check and
before the state-machine transition, so an attempted transition leaves
all internal state (master_tracker, feeders, listeners) untouched.

### Test invariants

- `crates/noxu-rep/tests/je_rep_txn_tck.rs::secondary_node_become_master_should_fail`
  no longer carries `#[ignore]`.  It now passes deterministically.
- All existing `become_master` tests continue to pass; they construct the
  env with the default `NodeType::Electable` and are unaffected.

## Fix 2 — Replica I/O thread auto-bootstraps on `NeedsRestore`

Before this fix, the I/O thread spawned by `become_replica` observed
`Ok(false)` (NEEDS_RESTORE) from `catch_up_from_peer`, logged a warning,
and exited.  An operator was expected to call
`bootstrap_via_dispatcher(peer_name)` manually.  This blocked fully
automated replica recovery for the "fresh restart" / "fall behind"
scenarios that operators most commonly hit in production.

### Change

Plumb a `Weak<ReplicatedEnvironment>` into the spawned thread:

- New field `self_weak: OnceLock<Weak<Self>>` on `ReplicatedEnvironment`.
- New method `init_self_weak(self: &Arc<Self>)`, idempotent
  (`OnceLock::set` returns `Err` on re-entry), called automatically by
  `ReplicatedEnvironment::open()` and by the test harness's
  `RepEnvInfo::open_env`.
- The `become_replica` I/O thread captures the `Weak` clone and, on
  `NeedsRestore`, upgrades it and calls `bootstrap_via_dispatcher`.
  After a successful bootstrap, it loops back into `catch_up_from_peer`.
  The retry budget is capped at `MAX_AUTO_BOOTSTRAP_ATTEMPTS = 2` so a
  misbehaving master cannot loop forever.
- If the upgrade fails (the env was dropped) or the env is in shutdown,
  the thread exits cleanly with no panic.

The `OnceLock<Weak<Self>>` approach was chosen over changing
`become_replica`'s receiver to `self: &Arc<Self>` because the latter
would have been a breaking API change for ~70 test sites that build the
env via raw `Arc::new(ReplicatedEnvironment::new(...))`.  Callers that
never invoke `init_self_weak` get the original operator-driven
behaviour (the I/O thread sees `None` and falls through to the warning
path), preserving the behaviour of all pre-existing tests.

### Test invariants

`crates/noxu-rep/tests/auto_bootstrap_test.rs::replica_auto_bootstraps_on_needs_restore`:

- Master env with three pre-seeded `.ndb` files in its `env_home` and an
  empty `PeerLogScanner` (so `negotiate_syncup` returns `NeedsRestore`
  for any catch-up at vlsn=0).
- Replica env with empty `env_home` and a wired `EnvironmentImpl`.
  `init_self_weak` is called explicitly so the auto-bootstrap path is
  exercised.
- After `become_replica("master")`, polls the replica's `env_home` for
  up to 10 s.  Asserts that all three `.ndb` files appear with
  byte-identical contents.

### Caveats

- The auto-bootstrap loop only triggers when `env_impl` has been wired
  in via `with_environment` AND `get_log_manager()` returns `Some`.
  Read-only environments (no log manager) still need operator
  intervention; this is the existing contract.
- After bootstrap copies files into `env_home`, the live
  `EnvironmentImpl` is **not** automatically reopened to pick up the
  new `.ndb` files.  Operators must close and reopen the underlying
  env after a network restore.  Wave 9-A does not change this.

## Fix 3 — De-flake `test_master_tracker_phi_mode`

The phi-detector integration test asserted that phi exceeds threshold=1.0
after a single 200 ms silence following 20 heartbeats at ~10 ms intervals.
Under workspace test load the inter-arrival sample window could pick up
a GC-pause-style outlier (e.g. a 100 ms scheduling stall) that inflated
stddev to the point where `z = (elapsed − mean) / stddev` stayed near
1.5, putting phi just under threshold.  Observed flake rate: ~20% on
contended runners.

### Change (option (b) from the Wave 9-A plan)

Two surgical changes to the test only — no phi-detector API changes:

1. Prime with **30** heartbeats instead of 20 so a single outlier is
   diluted in the variance computation.
2. Replace the single check after a fixed 200 ms silence with a
   bounded poll loop (up to 3 s, 50 ms granularity) that breaks as soon
   as `is_master_alive()` returns `false`.  Phi is monotonically
   non-decreasing as `elapsed` grows, so any sufficient elapsed time
   crosses the threshold deterministically; the test only needs to wait
   long enough for it to happen.

Validation: ran the test five consecutive times under workspace load
without a single failure.  Previously the same workload reproduced
flakes within ~5 runs.

### Why not option (a)?

Option (a) — inject a synthetic clock via a `Clock` trait — was scoped
at >100 LOC across `phi_detector.rs` and `master_tracker.rs` (both use
`Instant::now()` directly in `record_heartbeat`, `phi`,
`is_master_alive`, `time_since_heartbeat`, and `update_master`).  The
trait would also have to thread through every constructor and every
test-construction site in `noxu-rep`, plus the `paxos.rs` code path
that consumes `&PhiAccrualDetector`.  Captured for v3.0+ as a clean
follow-up; tracked alongside the other "future test seam" work in the
internal design notes.

## Capability matrix impact

None of these fixes change the publicly advertised replication
capabilities (master election, VLSN streaming, network restore, master
transfer, XA 2PC).  No changes to `docs/src/introduction.md` are
required.

## Risk analysis

- Fix 1 narrows the contract of `become_master` — callers that were
  silently relying on `Secondary` nodes succeeding now get an error.
  The only known caller in production is the election driver itself,
  which only invokes `become_master` after a Paxos round in which a
  `Secondary` could not have been a candidate (Secondary nodes do not
  run acceptors), so the practical impact is zero.  All in-tree tests
  that construct Secondary nodes go through `become_replica`.
- Fix 2 is additive: paths that do not call `init_self_weak` retain
  the prior operator-driven behaviour exactly.
- Fix 3 only touches the test body — no production code changes.

## Files touched

```text
crates/noxu-rep/src/replicated_environment.rs   (+ field, + method, become_master guard, become_replica auto-bootstrap)
crates/noxu-rep/src/test_harness.rs              (+ init_self_weak call in open_env)
crates/noxu-rep/tests/je_rep_txn_tck.rs          (- #[ignore] on secondary_node_become_master_should_fail)
crates/noxu-rep/tests/auto_bootstrap_test.rs     (NEW; regression test for fix 2)
crates/noxu-rep/tests/phi_detector_test.rs       (de-flake)
docs/src/internal/wave-9-a-rep-fixes.md          (this file)
```
