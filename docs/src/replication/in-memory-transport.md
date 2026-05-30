# In-Memory Transport

> **v2.4 — GA.**  The in-memory transport is a first-class production
> transport alongside [TCP, TLS, and QUIC](transport.md).

`noxu::replication::net::InMemoryTransport` lets you compose multi-node
replication clusters inside a single process.  It is a real
implementation of the same `Channel` trait that the wire transports
use, so every higher layer (elections, feeder, replica stream,
network restore) works identically over it without any changes.

## When to use it

| Scenario | Reason |
|---|---|
| Embedded multi-node clusters | A single process can host an `N`-node group with no socket setup, no port reservation, no firewall rules. |
| Integration tests | Real `ReplicatedEnvironment` lifecycle with no risk of hung-socket flakes. |
| Stateright drivers | Property-test harnesses that need actual replication code paths but no real network. |
| Small / single-tenant deployments | Replica caching tier inside the same process as the master. |

## Topologies

Two factory shapes are exposed:

| Constructor | Shape | Use case |
|---|---|---|
| `InMemoryTransport::new_pair() -> (InMemoryEndpoint, InMemoryEndpoint)` | back-to-back endpoints | 2-node master/replica pair |
| `InMemoryTransport::new_group(n: usize) -> InMemoryGroup` | `n`-node fully connected | any election quorum |

The mesh maintains exactly `N · (N - 1)` directional channels — one
per ordered `(from, to)` pair — and routes each `send` to the
corresponding peer's receive queue, mirroring a real point-to-point
socket cluster.

## Crash injection

Production cluster tests need to exercise crash recovery without
tearing down the entire process.

```rust
use noxu::replication::net::InMemoryTransport;

let mesh = InMemoryTransport::new_group(3);
mesh.simulate_crash(0);          // node 0 down: every channel touching it is closed
assert!(mesh.try_channel(0, 1).is_none());
mesh.reconnect(0);               // node 0 restarted: row rewired against live peers
mesh.channel(0, 1).send(b"reborn").unwrap();
```

After `simulate_crash(node)`, every previously-cloned `Arc<dyn
Channel>` for that node returns
`RepError::ChannelClosed` on `send` / `receive`, exactly as a real
socket disconnect would.

## End-to-end example

```rust
use noxu::replication::net::{Channel, InMemoryTransport};
use noxu::replication::test_harness::RepTestBase;
use std::time::Duration;

// 1. Build a 3-node fully-connected mesh.
let mesh = InMemoryTransport::new_group(3);
assert_eq!(mesh.size(), 3);

// 2. Build a 3-node replicated group (master + 2 replicas).
let mut group = RepTestBase::builder("demo").group_size(3).build();
group.create_group(/* term */ 1).unwrap();

// 3. Master writes 50 entries; both replicas converge.
group.populate_db(1, 50).unwrap();
group.assert_all_at_vlsn(50);

// 4. Crash the master, fail over to node 1.
let _ = group.close_master().unwrap();
group.failover_to(1).unwrap();
assert!(group.nodes()[1].is_master());

// 5. Wire layer is independent — direct send/receive still works.
mesh.channel(0, 1).send(b"hello").unwrap();
let got = mesh.channel(1, 0).receive(Duration::from_millis(50)).unwrap();
assert_eq!(got, Some(b"hello".to_vec()));

// 6. Clean shutdown.
group.shutdown_all();
```

## RepConfig integration

`RepConfig` exposes `transport_kind: RepTransportKind` so callers can
declare their intent.  The default is `Tcp` to preserve backward
compatibility.

```rust
use noxu::replication::{RepConfig, RepTransportKind};

let cfg = RepConfig::builder("g", "n", "127.0.0.1")
    .transport_kind(RepTransportKind::InMemory)
    .build();
assert_eq!(cfg.transport_kind, RepTransportKind::InMemory);
```

The field is advisory: noxu-rep's channel construction is performed
by the user (or by the `RepTestBase` harness) directly through the
relevant transport factory.  `transport_kind` lets observability /
chaos / harness layers introspect the transport choice without
inspecting individual channel types.

## Public API surface

| Symbol | Module | Notes |
|---|---|---|
| `InMemoryTransport` | `noxu::replication::net::inmem` | factory ZST |
| `InMemoryEndpoint`  | `noxu::replication::net::inmem` | implements `Channel` |
| `InMemoryGroup`     | `noxu::replication::net::inmem` | `n`-node mesh; `simulate_crash`, `reconnect` |
| `RepTransportKind`  | `noxu::replication::rep_config` | enum: `Tcp`, `Tls`, `Quic`, `InMemory` |
| `RepConfig::transport_kind` | `noxu::replication::rep_config` | declarative selector |
| `RepConfigBuilder::transport_kind` | `noxu::replication::rep_config` | builder method |

The pre-existing `noxu::replication::test_harness::RepTestBase` /
`RepEnvInfo` / `CountingListener` types are also lifted out of the
`cfg(test) / feature = "test-harness"` gate.
The `test-harness` feature flag is retained as a no-op for backward
compatibility with downstream `Cargo.toml` entries.

## Design invariants

* **Wire compatibility.**  `InMemoryEndpoint` implements the same
  `Channel` trait as `TcpChannel` / `TlsTcpChannel` /
  `QuicMultiplexedChannel`, so all higher layers (`FeederRunner`,
  `ReplicaStream`, `run_election`, `run_acceptor`,
  `NetworkRestoreServer`) work identically over it.
* **No real I/O.**  `InMemoryGroup` performs zero file-descriptor or
  socket work.  It can be used in tests without `tcpdump`-style
  flakes, port-collision races, or kernel-level netem chaos.
* **`Arc<dyn Channel>` handles outlive the mesh.**  Pre-cloned
  handles remain usable until both endpoints have been dropped; once
  the mesh itself drops, sends to a dropped peer surface
  `ChannelClosed` instead of panicking.
* **Crash + reconnect deterministic.**  `simulate_crash(n)` is
  idempotent.  `reconnect(n)` rewires only fully-down peer pairs,
  leaving half-open links untouched so harnesses can model
  asymmetric partitions.

## Tests

The in-memory transport ships with:

* 11 unit tests in `crates/noxu-rep/src/net/inmem.rs` covering
  pair / group / crash / reconnect / handle-lifetime invariants.
* 7 integration tests in
  `crates/noxu-rep/tests/inmem_transport_test.rs`:
  3-node election + replication, master-crash failover,
  network-restore catch-up of a 100-entry partition, mesh
  round-trip, crash + reconnect cycle, `RepConfig` round-trip,
  end-to-end smoke with clean shutdown.
* All test invocations complete in under 60 seconds.
