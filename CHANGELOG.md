# Changelog

All notable changes to Noxu DB are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added

- **REP-7 live read replicas.** A streaming replica now applies the master's
  committed log entries to a LIVE in-memory B-tree as they stream in, so a
  replica can serve reads of replicated data WITHOUT a restart/recovery cycle.
  Port of `com.sleepycat.je.rep.impl.node.Replay`:
  - `noxu_dbi::ReplicaReplay` applies each streamed entry to the live tree
    (`Replay.replayEntry` / `applyLN`). Transactional LNs are applied
    provisionally and resolved at the replica-side commit/abort entry
    (`Replay.activeTxns` / `ReplayTxn`); non-transactional LNs apply
    immediately.
  - The tree mutation reuses `noxu_recovery::apply_redo_ln` — the SAME function
    the crash-recovery redo pass uses — so the live-apply and a subsequent
    crash-recovery produce the SAME tree (the WAL stays the source of truth).
  - `EnvironmentLogWriter::with_replay` wires the live-apply into the replica
    receive path after the WAL write + VLSN-index update; `become_replica`
    installs it.
  - Reads on the replica read the same `Arc<RwLock<Tree>>` opened cursors
    traverse, so live data is visible immediately.
  - `ReplicaReplay::last_applied_vlsn` is the seam REP-10 (consistency
    policies) gates reads on.

- **REP-10 replica read-consistency policies enforced.** A read transaction
  that begins on a *replica* now BLOCKS until the replica's applied state
  satisfies the configured `ReplicaConsistencyPolicy`, or the policy timeout
  expires (a clean error, never a hang). Port of
  `com.sleepycat.je.rep.{NoConsistencyRequiredPolicy,TimeConsistencyPolicy,
  CommitPointConsistencyPolicy}` +
  `Replica.ConsistencyTracker.awaitVLSN` / `lagAwait`:
  - `noxu_rep::ConsistencyTracker` is the consistency-wait. It REUSES the
    REP-7 `last_applied_vlsn` handle (`ReplicaReplay::last_applied_vlsn_handle`)
    as the wait predicate — not a parallel tracker — and parks the reader
    until `last_applied_vlsn` reaches the required VLSN.
  - `CommitPointConsistency` waits until `last_applied_vlsn >= token VLSN`
    (JE `awaitVLSN` vs `lastReplayedTxnVLSN`); `TimeConsistency` waits until
    the estimated lag behind the master is within `max_lag` (JE `lagAwait`);
    `NoConsistency` returns immediately (JE `NoConsistencyRequiredPolicy`).
  - `noxu_rep::CommitToken` (`{ group, vlsn }`) is the Rust port of
    `com.sleepycat.je.CommitToken`. The master mints one for its latest commit
    via `ReplicatedEnvironment::commit_token` (port of
    `MasterTxn.getCommitToken`); a client passes it to a replica read via
    `ConsistencyPolicy::commit_point(&token, timeout)` (port of
    `new CommitPointConsistencyPolicy(...)`).
  - `ReplicatedEnvironment::begin_read_consistency(policy_override)` is the
    read-gate (port of `ReplicaConsistencyPolicy.ensureConsistency` from a
    replica `beginTransaction` / `RepImpl.checkConsistency`). It consults the
    per-transaction override else the node default
    (`RepConfig::consistency_policy`) and runs the wait before the read
    proceeds.
  - On timeout it returns `RepError::ConsistencyTimeout` /
    `ReplicaLagExceeded` — the equivalent of JE
    `ReplicaConsistencyException`.
  - **Default is `NoConsistency`**, so existing behaviour is unchanged unless
    a policy is set.

### Changed

- The REP-7 known limitation is corrected: replicas now serve live reads, not
  warm-standby-only.
- The REP-10 known limitation is corrected: replica read-consistency policies
  (`NoConsistency` / `TimeConsistency` / `CommitPointConsistency`) are now
  ENFORCED on the replica read path; a `CommitPointConsistency` read blocks
  until the replica has replayed past the `CommitToken`'s VLSN.
