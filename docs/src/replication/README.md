# High Availability

> **v2.0 status — GA.** The replication subsystem reached GA in v2.0
> with all ten pre-v2.0 blockers closed.  See
> the 2026 review
> for the per-finding details.

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
