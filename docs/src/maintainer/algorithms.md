# Algorithms

Every named algorithm in Noxu DB, with source locations and academic references.

## B+tree — Latch-Coupling Search and Update

**Source**: `crates/noxu-tree/src/tree.rs`, `bin.rs`, `in_node.rs`

Standard B+tree with latch coupling traversal:
1. Acquire root with shared latch
2. Find correct child; acquire child latch; release parent latch
3. Repeat until BIN reached
4. Upgrade BIN latch to write for mutations

**Key prefix compression**: Common prefix stored once in BIN header; slots
store suffix only. `recompute_key_prefix()` rebuilds on deserialization.

**BIN-delta**: Changed slots only are logged, reducing write amplification.
Base BIN reconstructed by reading `last_full_lsn` then applying deltas.

**Reference**: Noxu DB Architecture Notes; Ramakrishnan & Gehrke, *Database
Management Systems*, Chapter 14.

## Write-Ahead Log — Group Commit

**Source**: `crates/noxu-log/src/log_manager.rs`

Write latch is released **before** `fdatasync`. Multiple concurrent
`commit_with_durability()` callers may share a single `fsync` call.

`GroupCommit` (in `noxu-txn`) adds explicit batching: commits accumulate for
up to 1ms then are flushed together. Measured improvement: 40% throughput
increase on concurrent write workloads.

**Reference**: Gray & Reuter, *Transaction Processing: Concepts and Techniques*,
Chapter 9.

## Three-Phase Recovery

**Source**: `crates/noxu-recovery/src/recovery_manager.rs`

1. **Find end of log**: scan backward, validate CRC32
2. **Build tree from checkpoint**: read `CheckpointEnd → root_lsn`, reconstruct INs/BINs
3. **Redo committed / undo uncommitted**: scan from `first_active_lsn`

**Reference**: Noxu DB Architecture Notes; Ramakrishnan & Gehrke, Chapter 18.

## Log Cleaning — FIFO-Ish File Deletion

**Source**: `crates/noxu-cleaner/src/`

Utilization profile maintained incrementally. File with lowest utilization
selected. Live entries migrated (re-logged). File deleted after next checkpoint.

Adaptive back-pressure via `CleanerThrottle` prevents cleaner starvation.

**Reference**: Noxu DB Cleaner Design Notes; O'Neil et al., "The Log-Structured
Merge-Tree (LSM-Tree)", *Acta Informatica* 1996.

## Deadlock Detection — Waiter Graph Cycle Search

**Source**: `crates/noxu-txn/src/lock_manager.rs`

`waiter_graph: Mutex<HashMap<i64, Vec<i64>>>` maps blocker→[waiters].
Maintained incrementally (O(1) per lock acquisition/release).

Cycle detection: DFS from each node. Youngest transaction (by txn_id) in the
cycle is selected as victim. `NoxuError::LockDeadlock` returned to victim.

**Reference**: Bernstein, Hadzilacos & Goodman, *Concurrency Control and
Recovery in Database Systems*, Chapter 2.

## FPaxos — Flexible Paxos Leader Election

**Source**: `crates/noxu-rep/src/elections/paxos.rs`

Classic Paxos relaxed to use different quorum sizes for Phase 1 and Phase 2.
Safety theorem: ∀ Q1 ∈ QS1, ∀ Q2 ∈ QS2: Q1 ∩ Q2 ≠ ∅.

For n=5: phase1=4, phase2=2 satisfies 4+2>5.
- Phase 1 uses `phase1_quorum` promises
- Phase 2 uses `phase2_quorum` accepts (broadcasted only to Phase 1 promisers)

**References**:
- Howard, H. (2019). *Distributed Consensus Revised*. UCAM-CL-TR-935.
- Howard, H. et al. (2016). *Flexible Paxos*. PaPoC 2016.
- Lamport, L. (1998). *Paxos Made Simple*. ACM SIGACT News.

## Phi Accrual Failure Detector

**Source**: `crates/noxu-rep/src/elections/phi_detector.rs`

Inter-arrival time distribution maintained as a sliding window of size N
(default 1000 samples). Mean μ and variance σ² computed from the window.

```
φ(t) = -log10(1 - Φ((t - μ) / σ))
```

where Φ is the Normal CDF approximated by Abramowitz & Stegun 26.2.17.

`is_available(threshold)`: `φ < threshold`. Default threshold 8.0 means
failure is declared when there is less than 1-in-10^8 probability of a
legitimate inter-arrival gap.

Adaptive phase timeout: `μ + k·σ` (k=3.0 → 99.7% confidence), floor 50ms,
ceiling 5s.

**Reference**: Hayashibara, N. et al. (2004). *The φ Accrual Failure Detector*.
SRDS 2004.

## quoracle — LP-Optimal Quorum Systems

**Source**: `crates/noxu-rep/quoracle/` (git submodule)

`QuorumSystem<Node>` built from a logical expression over nodes. For
`QuorumPolicy::Expression`, the optimal quorum is selected by solving a
linear program over node capacity/latency hints.

Naor & Wool's load and availability metrics computed for each candidate
quorum to guide LP objective.

**Reference**: Naor, M. & Wool, A. (1998). *The Load, Capacity, and
Availability of Quorum Systems*. SIAM J. Computing.

## CBVLSN — Committed Barrier VLSN

**Source**: `crates/noxu-rep/src/stream/peer_feeder.rs`

The Committed Barrier VLSN is the minimum VLSN across all nodes that have
committed up to that point. It marks the earliest VLSN that can be cleaned
from the master's log without losing data needed by any replica.

The master broadcasts CBVLSN to replicas via unreliable QUIC datagrams so
replicas can safely advance their own cleaning.

**Reference**: Noxu DB HA Design Notes; Lamport, L. (1978). *Time, Clocks, and
the Ordering of Events in a Distributed System*. CACM.
