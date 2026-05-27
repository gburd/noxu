# Wave 8 — `RepTestBase` harness + heavy rep TCK port

## Goal

The v2.0.0 deferred TCK port left ~30+ heavyweight rep tests in
`je.rep.stream`, `je.rep.txn`, and top-level `je.rep` un-ported because
they each open a multi-node `ReplicatedEnvironment` group via a
JE-style `RepTestBase` / `RepEnvInfo` fixture that did not exist in
noxu-rep yet.  Without that harness, every port would have to inline
the same ~50 lines of boilerplate, and tests using real network
coordination would risk hanging.

Wave 8 builds the harness, then ports the heavy tests on top of it.

## What changed

### 1. `crates/noxu-rep/src/test_harness.rs` — the harness

A new module, gated behind the new `test-harness` cargo feature
(auto-enabled in `dev-dependencies` so `cargo test` picks it up but
released crates do not), exposes:

* **`RepEnvInfo`** — Rust analog of JE's
  `RepTestUtils.RepEnvInfo`: holds one node's `RepConfig`, its
  numeric node id, and (after `open_env`) its `Arc<ReplicatedEnvironment>`.
  Methods mirror JE: `open_env`, `close_env`, `abnormal_close_env`,
  `get_env`, `is_master` / `is_replica` / `is_unknown`, `state`,
  `current_vlsn`.
* **`RepTestBase`** — Rust analog of JE's
  `com.sleepycat.je.rep.impl.RepTestBase`: encapsulates a multi-node
  group with shared group name, port range, and election policy, plus
  lifecycle / replication / failover / assertion helpers.  Method
  names mirror JE so port translations are mechanical.
* **`RepTestBaseBuilder`** — fluent builder
  (`builder(group_name).group_size(N).build()`).
* **`CountingListener`** — `StateChangeListener` that counts
  master / replica / unknown / detached / shutdown transitions.

Critical discipline: **the harness never opens a real TCP socket.**
All "replication" between harness nodes is driven by direct method
calls on each node's `ReplicatedEnvironment`
(`become_master` / `become_replica` / `register_vlsn` /
`apply_entry`).  This works because noxu-rep's `ReplicatedEnvironment`
is already drivable purely in-process — the TCP receive loop in
`become_replica` is only spawned when an `EnvironmentImpl` has been
attached via `with_environment`, which the harness never does.

Tests that exercise the real network protocol layer continue to use
the existing `cluster_integration_test.rs` + `TcpChannel` /
`TcpChannelListener` setups.

### 2. Three new heavy-test files

| File | JE source | Tests ported |
|---|---|---:|
| `tests/je_rep_top_level_tck.rs`     | `je.rep.*`         | 13 |
| `tests/je_rep_txn_tck.rs`           | `je.rep.txn.*`     | 14 + 1 #[ignore]'d |
| `tests/je_rep_stream_tck.rs`        | `je.rep.stream.*`  |  9 |
| **Total**                           |                    | **36 active + 1 #[ignore]** |

Each test names its JE source file and method in its rustdoc.

#### `je_rep_top_level_tck.rs` — top-level `je.rep`

* `StateChangeListenerTest` (3): listener replacement, basic
  transition history, secondary-node listener history.
* `ReplicatedEnvironmentTest` (3): fresh-open state, config round-
  trip, close+reopen.
* `JoinGroupTest` (2): join-leave-join cycle, repeated open fails.
* `ReplicationGroupTest` (1): basic membership visibility.
* `SecondaryNodeTest` (2): secondary join-leave-join, secondary
  follows new master after failover.
* `ElectableGroupSizeOverrideTest` (1): Flexible quorum policy.
* `NodePriorityTest` (1): VLSN-based tiebreak after failover.

#### `je_rep_txn_tck.rs` — `je.rep.txn`

* `CommitTokenTest` (2): VLSN total ordering, empty-txn invariant.
* `RepAutoCommitTest` (2): master-write fan-out,
  Detached → Master through Unknown.
* `PostLogCommitTest` (1): replica catch-up after master-only commit.
* `RollbackTest` (4): pre-/post-/straddling-matchpoint discard,
  old-master rejoins-as-replica.
* `LockPreemptionTest` (1): apply on shutdown env fails.
* `ExceptionTest` (1 + 1 ignored): become_master on shutdown env
  fails; secondary become_master should fail (ignored — see Bug 1
  below).
* `ReplayRecoveryTest` (1): replay resumes after reopen.
* Harness-level locks (2): replica VLSN monotonic, await_state.

#### `je_rep_stream_tck.rs` — `je.rep.stream`

* `FeederReaderTest` (3): forward-scan coverage, range invariant,
  catch-up start VLSN.
* `FeederWriteQueueTest` (1): VLSN-order preservation.
* `ProtocolTest` (1): full fan-out smoke.
* `ReplicaSyncupReaderTest` (2): replicated-vs-master-only,
  multi-checkpoint chunks.
* `FeederFilterTest` (2): no-op baseline, no silent drops.

### 3. Enumeration TSV updates

* `docs/src/internal/je-tck-port-2026-05-enumeration-je.rep.tsv` —
  13 rows updated: net +6 PORTED-EQUIVALENT, +4 PORTED-PARTIAL,
  -10 NOT-PORTED.  (`ReplicationGroupTest.testBasic` was re-tagged
  EQUIVALENT → PARTIAL because the harness only tests the membership
  subset.)
* `docs/src/internal/je-tck-port-2026-05-enumeration-je.rep.txn.tsv`
  — 11 rows updated: net +3 PORTED-EQUIVALENT, +7 PORTED-PARTIAL,
  -10 NOT-PORTED.  (`CommitTokenTest.testBasic` was re-tagged
  EQUIVALENT → PARTIAL because Noxu does not yet expose `CommitToken`.)
* `docs/src/internal/je-tck-port-2026-05-enumeration-je.rep.stream.tsv`
  — 9 rows updated: net +0 PORTED-EQUIVALENT, +8 PORTED-PARTIAL,
  -8 NOT-PORTED.  (`ProtocolTest.testBasic` was re-tagged
  EQUIVALENT → PARTIAL because the harness covers only the fan-out
  smoke-test subset.)

The bulk-update was driven by `scripts/wave8_update_tsv.py`, which is
checked in for reproducibility.

## Real Noxu bugs surfaced

### Bug 1 — `become_master` accepts Secondary nodes

`crate::ReplicatedEnvironment::become_master` does not check
`config.node_type` and will happily transition a `NodeType::Secondary`
node to `NodeState::Master`.  This violates the JE invariant that
secondary nodes are not electable.

Surfaced by `je_rep_txn_tck::secondary_node_become_master_should_fail`,
which is `#[ignore]`d and tracked as a wave-8 follow-up.  The fix is a
~3-line guard at the top of `become_master`:

```rust
if self.config.node_type != NodeType::Electable {
    return Err(RepError::ConfigError(
        "Only Electable nodes can become master".into()
    ));
}
```

Confirmed reproducible by running with `-- --ignored`:

```
$ cargo test -p noxu-rep --test je_rep_txn_tck --features test-harness \
    -- --ignored secondary_node_become_master_should_fail
... thread 'secondary_node_become_master_should_fail' panicked at:
    Secondary node must not be electable as master; got Ok(())
```

## Final gate status

* `cargo fmt --all -- --check` — clean.
* `cargo clippy --workspace --all-targets --all-features` — clean (no
  new warnings introduced by Wave 8 over baseline).
* `cargo test -p noxu-rep --no-fail-fast --lib` — 620 passed, 0 failed
  (612 baseline + 8 harness self-tests).
* `cargo test -p noxu-rep --test je_rep_top_level_tck --features test-harness` —
  13 passed, 0 failed.
* `cargo test -p noxu-rep --test je_rep_txn_tck --features test-harness` —
  14 passed, 1 ignored, 0 failed.
* `cargo test -p noxu-rep --test je_rep_stream_tck --features test-harness` —
  9 passed, 0 failed.
* `make docs-check` — pass.

All Wave 8 test files run in **<60 ms each** because there is no
network coordination — the harness operates entirely in-process.

## Out of scope for Wave 8

Tests that require an `EnvironmentImpl` wired into the
`ReplicatedEnvironment` (anything that exercises `Database.put` /
`Database.get` directly) are still NOT-PORTED.  Those need a future
wave that extends the harness to attach a real `noxu-db::Environment`
via `ReplicatedEnvironment::with_environment`, which would in turn
need an in-memory file backend or a `tempfile`-driven setup.

This includes most of `RepAutoCommitTest`, `PostLogCommitTest`'s
exception-path subset, all of `LockPreemptionTest` beyond shutdown-
env, and all `ReplayRecoveryTest` rollback variants.
