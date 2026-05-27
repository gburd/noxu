# Wave 4-A — `noxu-rep` GA Finish

**Status**: complete.  All 10 noxu-rep v2.0 GA blockers from
[`api-audit-2026-05-rep.md`](api-audit-2026-05-rep.md) §7 are closed.

Wave 3-3 closed F1, F3, F6, F10, F22.  Wave 4-A closes the remaining five
blocker groups (F2/F4, F5/F31, F7/F8, F9, F11).

## Per-finding resolution

### F11 — VLSN index lost on restart

**Resolution**: persist the in-memory `VlsnIndex` to
`<env_home>/vlsn.idx` on a 2-second tick and on shutdown; load on
`ReplicatedEnvironment::new` when `env_home` is configured.

* New module `noxu_rep::vlsn::persist` — atomic write/load with
  CRC32-protected, versioned binary format (magic `b"VIDX"`,
  version 1, header + entry array + crc).
* New `VlsnIndex::snapshot_entries`, `VlsnBucket::append_entries`
  let the persistence layer walk the in-memory state under a
  read lock without copying buckets.
* `ReplicatedEnvironment::start_vlsn_persistence_daemon` —
  spawned by `open()`.  Joined cleanly by `close()`; final flush
  on the way out so a clean close is recoverable.
* Belt-and-braces final flush in `close()` for non-`open()` callers.
* Corrupt files are logged and removed; the next persist cycle
  writes a clean one.

**Tests**: 7 unit tests in `vlsn::persist::tests`; 3 integration
tests in `tests/vlsn_persistence_test.rs`.

### F9 — `become_master` doesn't spawn feeders

**Resolution**: `become_master` now iterates the group's known
electable/secondary peers and inserts a `Feeder` tracker for each.
`add_peer` while self is master immediately dispatches a `Feeder`
for the new peer.  Arbiters are excluded — they don't receive log
entries.

The replication architecture stays pull-based: replicas connect to
the master's `PEER_FEEDER` service via `catch_up_from_peer`.  The
new behaviour ensures the master's tracker state and in-memory
cache (`peer_scanner`) are populated when mastership starts,
rather than waiting for the first replica connection.

* New `ReplicatedEnvironment::feeder_replica_names()` accessor.
* New `ReplicatedEnvironment::replicate_entry(vlsn, file, offset,
  type, data)` — combines `register_vlsn` with a push into
  `peer_scanner` so downstream replicas pulling from `PEER_FEEDER`
  receive entries without a disk round-trip.

**Tests**: 4 integration tests in `tests/feeder_spawn_test.rs`.

### F5 / F31 — Acceptor promise state non-persistent

**Resolution**: every Paxos promise/accept is fsynced before the
response leaves the acceptor.

* New module `noxu_rep::elections::acceptor_state` —
  `PersistentAcceptorState` durably stores
  `(promised_term, accepted_term, accepted_master)` as
  `<env_home>/acceptor.state`.  Format: magic `b"PXST"`, version 1,
  header + master-name + crc; atomic via tmp+rename.
* New `paxos::run_acceptor_with_state` — variant of `run_acceptor`
  that delegates to `try_promise(t)` and `try_accept(t, master)`.
* `ElectionAcceptorState::with_env_home` constructor.  When `env_home`
  is set in `RepConfig`, `ReplicatedEnvironment::new` wires the
  ELECTION service to the persistent backend; otherwise it falls
  back to in-memory mode (test/legacy).
* `ElectionService::handle` now calls `run_acceptor_with_state`.

The Paxos safety invariant — "an acceptor never accepts a proposal
at a term lower than its highest promise" — now survives process
restarts.  An old proposer at a stale term is rejected even after a
crash.

**Tests**: 6 unit tests in `acceptor_state::tests`; 3 integration
tests in `tests/acceptor_persistence_test.rs` (including a
restart-rejects-stale-term scenario).

### F2 / F4 — Network restore broken on the dispatcher path

**Resolution**: a dispatcher-aware client speaks the
service-name handshake protocol the dispatcher requires.

* `NetworkRestore::execute_via_dispatcher` — speaks the dispatcher
  protocol.  Connects via `connect_to_service(RESTORE)`, sends
  `RESTORE_MAGIC` over the channel framing (not raw TCP bytes),
  receives one framed payload `[count][file_records...]`, and
  decodes it into `local_log_dir`.  Bound-checks every offset
  against `payload.len()`.
* `ReplicatedEnvironment::bootstrap_via_dispatcher(peer_name)` —
  high-level operator API.  Looks up the peer in `GroupService`,
  builds a `NetworkRestoreConfig`, and runs
  `execute_via_dispatcher`.

`become_replica`'s noxu-replica I/O thread now logs prescriptively
when the master signals NeedsRestore — operators call
`bootstrap_via_dispatcher` and re-attach.  Auto-bootstrap from the
receive thread requires a back-reference to `Arc<Self>` and is a
small follow-up; the operator-driven path is GA-correct.

**Tests**: 4 integration tests in `tests/network_restore_dispatcher_test.rs`.

### F7 / F8 — `transfer_master` and `shutdown_group` no-ops

**Resolution**: a new `ADMIN` service handles group-level commands.

* New module `noxu_rep::group_admin` — registered as service
  `"ADMIN"` on the `TcpServiceDispatcher`.  Wire format: a single
  command frame `[cmd:u8][term:u64?][name:utf8?]` with a single
  ack byte (0x00 OK / 0x01 REJECTED).  Three commands:
  * `TRANSFER_MASTER` — recipient becomes master at the new
    term (when self is the named target) or becomes replica of
    the new master (when self is a peer).
  * `SHUTDOWN_GROUP` — recipient calls `close()` on its env.
  * `STEP_DOWN` — caller-driven self-demote helper.
* `register_admin_service(dispatcher, Weak<env>)` — handler holds
  a Weak so it does not extend the env's lifetime.  The
  `ReplicatedEnvironment::open` lifecycle invokes this after the
  election driver and persistence daemon.

* `transfer_master`:
  1. Resolve target's address from `GroupService`.
  2. Compute new term = current_term + 1.
  3. Send TRANSFER_MASTER to the target (must ack OK).
  4. Best-effort propagate to other peers.
  5. Demote self to replica of the target.
* `shutdown_group`:
  1. Iterate peers; send SHUTDOWN_GROUP to each (best-effort,
     deadline-bounded).
  2. Close self last.

**Breaking change**: the lib test
`test_transfer_master_as_master` (which asserted a no-op success)
was renamed to `test_transfer_master_requires_registered_target`
with the opposite expectation.  This is captured by `feat(rep)!`
in the commit subject.

**Tests**: 4 integration tests in `tests/group_admin_test.rs`.

## Tests added

| File | Tests |
|---|---|
| `tests/vlsn_persistence_test.rs` | 3 |
| `tests/feeder_spawn_test.rs` | 4 |
| `tests/acceptor_persistence_test.rs` | 3 |
| `tests/network_restore_dispatcher_test.rs` | 4 |
| `tests/group_admin_test.rs` | 4 |
| `vlsn::persist::tests` (unit) | 7 |
| `elections::acceptor_state::tests` (unit) | 6 |
| **Total** | **31 new tests** |

All 612 lib tests pass.  All 18 new integration tests pass.  All
existing integration tests (cluster, tcp, election driver, scaling,
phi, quorum-policy, replica-ack-policy, dispatcher bounds,
peer-scanner bounds, arbiter-election, prop) pass without
modification.

## Audit findings closed

| # | Status | Note |
|---|---|---|
| F1 | closed (Wave 3-3) | `ReplicaAckPolicy` honoured on commit |
| F2 / F4 | **closed (Wave 4-A)** | NetworkRestore via dispatcher |
| F3 | closed (Wave 3-3) | dispatcher service-name length bound |
| F5 / F31 | **closed (Wave 4-A)** | acceptor promises persistent |
| F6 | closed (Wave 3-3) | election driver wired into `open()` |
| F7 / F8 | **closed (Wave 4-A)** | `transfer_master`, `shutdown_group` |
| F9 | **closed (Wave 4-A)** | Feeder per replica on `become_master` |
| F10 | closed (Wave 3-3) | peer scanner bounded |
| F11 | **closed (Wave 4-A)** | VLSN index persisted across restart |
| F22 | closed (Wave 3-3) | Arbiters cannot win Paxos elections |

The remaining medium and low audit findings (doc drift, decorative
stats, dual quorum/electable views, etc.) are not GA blockers and
will be tracked by the post-GA cleanup wave.
