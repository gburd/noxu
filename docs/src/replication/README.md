# High Availability

> **v1.5 status — PREVIEW / proof-of-concept.** The replication
> subsystem (`noxu-rep`) is **not** recommended for production in
> v1.5. The May 2026 noxu-rep API audit
> ([`docs/src/internal/api-audit-2026-05-rep.md`](../internal/api-audit-2026-05-rep.md))
> identified **10 GA blockers**, none of which were addressed in
> Sprints 1–3. Headlines:
>
> * `ReplicaAckPolicy` is not honoured on commit — the master
>   returns success after local fsync regardless of how many replicas
>   have acknowledged. The single most-marketed durability promise
>   of the subsystem is silently a no-op.
> * The election driver is not wired into
>   `ReplicatedEnvironment::new`; a freshly-constructed node sits in
>   `Detached` until `become_master()` is called manually.
> * The dispatcher path of `NetworkRestore::execute()` is broken on
>   arrival (4-byte magic is misinterpreted as a length prefix); new
>   replicas cannot bootstrap through the documented path.
> * The acceptor's promise state is not persisted across restart, so
>   the Stateright safety proof does not apply to the production
>   binary; two masters per term can be elected.
> * `transfer_master` and `shutdown_group` are silent no-ops.
>
> The full list and remediation plan are in the audit's
> [§7 — cross-reference: GA blockers for v1.x / v2.0](../internal/api-audit-2026-05-rep.md#7-cross-reference-blockers-for-v1x--v20).
> The chapters below describe the **intended** contract of the
> replication subsystem and remain useful for design review and for
> driving the v1.x → v2.0 GA work, but the documented behaviour does
> not match the production binary in v1.5. Treat each example as a
> design sketch until the GA-blocker list is closed.

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
