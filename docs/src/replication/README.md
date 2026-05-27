# High Availability

> **v2.0 status — GA.** All ten noxu-rep GA blockers identified in
> the May 2026 API audit
> ([`docs/src/internal/api-audit-2026-05-rep.md`](../internal/api-audit-2026-05-rep.md))
> are closed in v2.0.  Wave 3-3 closed F1 (`ReplicaAckPolicy`
> honoured on commit), F3 (dispatcher service-name length bound), F6
> (election driver wired in `open()`), F10 (peer scanner bounded),
> and F22 (Arbiters cannot win Paxos elections).  Wave 4-A closed
> F2/F4 (NetworkRestore via dispatcher), F5/F31 (acceptor promises
> persisted across restart), F7/F8 (`transfer_master` and
> `shutdown_group`), F9 (Feeder per replica on `become_master`), and
> F11 (VLSN index persisted across restart).  See the
> [Wave 4-A report](../internal/wave-4-a-rep-ga-finish.md) for
> per-finding resolution notes.

This chapter describes Noxu DB's replication subsystem (`noxu-rep`), which
provides active/passive multi-node high availability using the **Flexible
Paxos** consensus protocol.

The architecture corresponds to the **Noxu DB High Availability (HA) Guide**,
with significant extensions: the phi accrual failure detector replaces binary
heartbeat timeouts; `quoracle` provides LP-optimal quorum selection; and both
TCP and QUIC transports are supported.

## In This Chapter

1. [Concepts](concepts.md) — master/replica model, VLSN ordering, single-master invariant
2. [Setup and Configuration](setup.md) — `ReplicatedEnvironment`, `RepConfig`, group topology
3. [Leader Elections](elections.md) — FPaxos phases, phi accrual, adaptive timeouts
4. [Consistency Policies](consistency.md) — replica read freshness guarantees
5. [Durability Policies](durability.md) — `ReplicaAckPolicy`, async acks, group commit
6. [Network Restore](network-restore.md) — VLSN gap recovery, full environment restore
7. [Master Transfer](master-transfer.md) — graceful leader handoff
8. [Dynamic Membership](dynamic-membership.md) — `add_peer`/`remove_peer`, capacity hints
9. [Transport Layer](transport.md) — TCP channel, QUIC multiplexed streams, reconnect
