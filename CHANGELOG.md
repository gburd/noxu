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
  - `ReplicaReplay::last_applied_vlsn` is the seam a future REP-10
    (consistency policies) will gate reads on.

### Changed

- The REP-7 known limitation is corrected: replicas now serve live reads, not
  warm-standby-only. (REP-10 read-consistency-policy enforcement remains a
  separate follow-up.)
