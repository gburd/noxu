# Noxu DB Safety Model

This document applies
[STPA](http://psas.scripts.mit.edu/home/get_file.php?name=STPA_handbook.pdf)-style
hazard analysis to the Noxu DB embedded transactional database engine for the
purpose of guiding design and testing efforts to prevent unacceptable losses.

## Losses

We wish to prevent the following undesirable situations:

- **Data loss**  -  committed data disappears or becomes unreadable
- **Data corruption**  -  B-tree or log enters inconsistent state
- **Inconsistent reads**  -  queries return stale or phantom results
- **Process crash**  -  panic or resource exhaustion kills the host process
- **Deadlock**  -  transactions permanently block, preventing forward progress

## System Boundary

**Inside the boundary** (things we control):

- Codebase: safe control actions that prevent losses
- Documentation: guidance on safe usage, recommended configurations
- Test suite: unit, integration, property-based, and fuzz tests

**Outside the boundary** (things we cannot control):

- Hardware failures (disk corruption, power loss mid-write)
- Kernel bugs (filesystem, scheduler)
- User code (application logic, configuration choices)

## Hazards

### H1: Data Loss

Data may be lost if:

- WAL flush ordering is violated (modification applied before log write)
- `fsync` fails silently or is not called at commit time
- Checkpoint references data that has been cleaned (log file deleted)
- Recovery fails to replay all committed operations after crash

**Mitigations:**
- Write-ahead logging: all modifications logged before B-tree update
- CRC32 checksums on every log entry, validated during recovery
- Cleaner coordinates with checkpoint to avoid deleting active data
- 3-phase recovery: find checkpoint -> redo -> undo uncommitted

### H2: Data Corruption

The B-tree may become inconsistent if:

- Latch coupling protocol is violated during tree traversal
- Split/merge operations leave orphaned or duplicate entries
- Key prefix compression produces incorrect key reconstruction
- BIN-delta application produces incorrect slot state

**Mitigations:**
- Strict top-down latch coupling (parent before child)
- `Verify` subsystem validates B-tree structural invariants
- Serialization round-trip tests for all node types
- Property-based tests for key ordering invariants

### H3: Deadlock

Transactions may deadlock if:

- Two transactions hold locks that the other needs
- Latch ordering is violated (child before parent)

**Mitigations:**
- `DeadlockDetector` runs DFS cycle detection on wait graph
- Victim transaction aborted with `LockConflict` error
- 16-shard lock table reduces contention
- Strict latch ordering protocol documented and enforced

### H4: Resource Exhaustion

The process may crash if:

- Cache grows beyond configured memory budget
- Log files accumulate without cleaning
- File handles are leaked

**Mitigations:**
- `MemoryBudget` explicitly tracks all allocations
- LRU evictor enforces cache size limits
- Cleaner daemon reclaims space from obsolete log entries
- RAII patterns for file handles and latch guards

## Memory Safety

Noxu DB targets **zero `unsafe` code** in core crates:

- All concurrency through `parking_lot::Mutex/RwLock`
- Tree nodes use `Arc<RwLock<IN>>` for shared ownership
- Atomic operations use `std::sync::atomic` with correct orderings
- Exceptions limited to: `memmap2` (memory-mapped files), potential off-heap cache

## Replication Safety

- VLSN (Version Log Sequence Number) provides total ordering across replicas
- Election protocol requires quorum (majority of electable nodes)
- Master transfer uses coordinated handoff with VLSN synchronization
- Network restore detects and repairs VLSN gaps

## Summary

| Hazard | Primary Mitigation | Secondary Mitigation |
|--------|-------------------|---------------------|
| Data loss | WAL + fsync | 3-phase recovery |
| Corruption | Latch coupling + CRC32 | B-tree verification |
| Deadlock | DFS cycle detection | Latch ordering protocol |
| Resource exhaustion | MemoryBudget + LRU | Cleaner daemon |
