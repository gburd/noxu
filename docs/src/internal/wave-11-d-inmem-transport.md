# Wave 11-D — In-memory transport for production use

**Target release:** v2.4.0
**Branch:** `fix/wave11-d-inmem-transport`
**Status:** merged (post-v2.3.0 roadmap row 11-D)

## Goal

Wave 8 added an in-memory wire-level transport (`LocalChannel` /
`LocalChannelPair`) and a `RepTestBase` / `RepEnvInfo` cluster harness
behind `cfg(any(test, feature = "test-harness"))`.  Wave 11-D
promotes both into first-class production transports alongside TCP,
TLS, and QUIC so users can compose multi-node clusters in-process for
embedded deployments, integration tests, and Stateright-driven
property tests.

## Public API surface added

| Symbol | Module | Notes |
|---|---|---|
| `noxu_rep::net::InMemoryTransport` | `crates/noxu-rep/src/net/inmem.rs` | factory ZST; `new_pair`, `new_group(n)` |
| `noxu_rep::net::InMemoryEndpoint`  | same | implements `Channel` over `LocalChannel` |
| `noxu_rep::net::InMemoryGroup`     | same | n-node mesh; `simulate_crash`, `reconnect`, `is_node_live`, `try_channel`, `channel`, `size` |
| `noxu_rep::RepTransportKind`       | `crates/noxu-rep/src/rep_config.rs` | enum: `Tcp`, `Tls`, `Quic`, `InMemory` (default `Tcp`) |
| `RepConfig::transport_kind`        | same | declarative selector field |
| `RepConfigBuilder::transport_kind` | same | builder method |
| `noxu_rep::test_harness`           | `crates/noxu-rep/src/test_harness.rs` | lifted out of `cfg(test)` / `feature = "test-harness"` gate; module is now always public.  The feature flag is retained as a no-op for backward compatibility. |

Crate-root re-exports added:

* `noxu_rep::InMemoryTransport`
* `noxu_rep::InMemoryEndpoint`
* `noxu_rep::InMemoryGroup`
* `noxu_rep::RepTransportKind`

## Design invariants

1. **Channel-trait compatibility.**  `InMemoryEndpoint` implements
   the same `noxu_rep::net::channel::Channel` trait as `TcpChannel`,
   `TlsTcpChannel`, and `QuicMultiplexedChannel`.  Higher layers
   (feeder, replica stream, elections, network restore) consume `dyn
   Channel` and work identically over the in-memory transport.
2. **Mesh shape.**  `InMemoryGroup::new(n)` builds an n-node fully
   connected mesh with `n · (n - 1)` directional channels.
   `endpoints[i][j]` is node `i`'s view of its socket to `j`; sends
   on it are observed by `endpoints[j][i]`.  Diagonal slots
   (`endpoints[i][i]`) stay empty so the caller can index by
   `(from, to)` without arithmetic.
3. **Crash idempotence.**  `simulate_crash(node)` closes every
   channel touching `node` and is a no-op on already-crashed nodes.
   Pre-cloned `Arc<dyn Channel>` handles surface
   `RepError::ChannelClosed` on `send` / `receive` after the crash.
4. **Half-open partitions allowed.**  `reconnect(node)` only rewires
   peer pairs where *both* directions are currently empty, so a
   harness can model asymmetric partitions by reconnecting one
   direction at a time.
5. **Lock ordering.**  `reconnect` always locks `(lo, hi)` with
   `lo < hi` to keep a global lock order across the matrix; the
   implementation is deadlock-free under arbitrary concurrent
   `reconnect` / `simulate_crash` calls.
6. **Handle lifetime.**  `InMemoryEndpoint` holds its inner
   `LocalChannel` in an `Arc`, so cloned `Arc<dyn Channel>` handles
   stay valid even after the owning `InMemoryGroup` is dropped (they
   simply transition to closed once the peer side has gone).
7. **Backward compatibility.**  The old `LocalChannel` and
   `LocalChannelPair` types remain unchanged and continue to work
   for unit tests that already import them; the new
   `InMemoryTransport` / `InMemoryEndpoint` types are an additive
   higher-level API.

## RepConfig integration

`RepConfig::transport_kind` is advisory: noxu-rep's channel
construction is performed by the user (or by `RepTestBase`) directly
through the relevant transport factory.  The field documents intent
and lets observability / chaos / harness layers introspect the
transport choice without inspecting individual channel types.

Default is `RepTransportKind::Tcp` to preserve pre-Wave-11-D
behaviour; existing callers see no observable change.

## Tests added

### Unit tests (`crates/noxu-rep/src/net/inmem.rs`, 11 tests)

* `pair_round_trip` — `new_pair()` bidirectional smoke.
* `group_3node_mesh_is_fully_connected` — every directed pair has a
  channel, sends route to the matching peer endpoint.
* `group_independent_pairs_do_not_cross_talk` — `(0, 1)` traffic is
  invisible to `(0, 2)` and `(0, 3)` queues.
* `simulate_crash_closes_all_channels_for_node` — pre-cloned handles
  surface `ChannelClosed`; surviving pair still works; crashed
  slots return `None` from `try_channel`.
* `simulate_crash_is_idempotent` — second crash is a no-op.
* `reconnect_after_crash_restores_traffic` — `reconnect(0)` rewires
  all of node 0's row; new sends round-trip end-to-end.
* `channel_out_of_range_panics`, `channel_self_loop_panics`,
  `empty_group_panics` — input-validation panics.
* `one_node_group_has_no_channels` — degenerate `n=1` mesh is
  considered "live" with zero channels.
* `channel_handle_outlives_borrow_of_group` — `Arc<dyn Channel>`
  remains valid after the group drops.

### Integration tests (`crates/noxu-rep/tests/inmem_transport_test.rs`, 7 tests)

* `three_node_group_elects_master_and_replicates` — 3-node group,
  master writes 50 records, both replicas converge.
* `failover_after_master_crash_promotes_replica` — master + 2
  replicas, master closes, replica 1 promoted, surviving replica
  re-pointed, post-failover writes propagate.
* `network_restore_catches_up_lagging_replica` — 100-record
  partition, then sequential `apply_entry` catch-up, both nodes
  converge at VLSN 110.
* `inmem_group_round_trips_between_every_pair` — wire-level mesh
  round-trip across every ordered pair.
* `inmem_simulate_crash_then_reconnect_cycles_correctly` — wire-level
  crash + reconnect cycle.
* `rep_config_transport_kind_round_trips` — builder field round-trip;
  `ReplicatedEnvironment::new` accepts an `InMemory` config.
* `three_node_inmem_full_smoke_with_clean_shutdown` — end-to-end
  smoke covering mesh + harness + clean shutdown.

All integration tests run in well under the wave-gate `timeout 60`.

## Files touched

* `crates/noxu-rep/src/net/inmem.rs` (new, 615 lines incl. tests)
* `crates/noxu-rep/src/net/mod.rs` (re-export wiring)
* `crates/noxu-rep/src/lib.rs` (test_harness gate lifted; crate-root re-exports)
* `crates/noxu-rep/src/rep_config.rs` (`RepTransportKind`, `transport_kind` field + builder)
* `crates/noxu-rep/tests/inmem_transport_test.rs` (new, 292 lines)
* `docs/src/replication/in-memory-transport.md` (new chapter)
* `docs/src/replication/transport.md` (lists in-memory transport, table updated)
* `docs/src/SUMMARY.md` (mdBook ToC entry)
* `docs/src/introduction.md` (capability matrix row)
* `docs/src/internal/wave-11-d-inmem-transport.md` (this note)
* `docs/src/internal/post-v2.3.0-roadmap.md` (11-D row → merged)
* `CHANGELOG.md` (`[Unreleased]` entry)

## Acceptance gate

Per `docs/src/internal/post-v2.3.0-roadmap.md` Wave 11-D:

* `noxu-rep` exposes `InMemoryTransport` (or similar) alongside
  TCP/TLS/QUIC. ✅ — `noxu_rep::net::InMemoryTransport`.
* The same `Channel` trait is implemented; `RepConfig` accepts the
  transport choice. ✅ — `RepTransportKind::InMemory` +
  `RepConfig::transport_kind`.
* New chapter `docs/src/replication/in-memory-transport.md` with
  usage example. ✅
* Tests cover 3-node group, replication flows, election, network
  restore. ✅ — 7 integration tests + 11 unit tests.
