# API Audit — May 2026 — `noxu-rep` (Replication Subsystem)

> **Read-only audit.** Companion to the May 2026 public-API audit. This
> document enumerates the public surface of the replication subsystem
> (`crates/noxu-rep/`) and its associated mdBook chapters
> (`docs/src/replication/`), compares each item against the documented
> contract, the well-known BDB-JE HA semantics it claims to port, and the
> Stateright specifications in `crates/noxu-spec/`. No code, configuration,
> or test was modified in the course of this audit.
>
> The companion claim audit (`docs/src/internal/claim-audit-2026-05.md`,
> 13 noxu-rep findings, 4 high) is referenced where it overlaps; this audit
> goes broader (transport, durability plumbing, restore wire compatibility,
> persisted state, election protocol soundness, doc-API drift) and uses the
> same six themes as the original API audit:
>
> 1. Config-not-plumbed
> 2. Transaction-not-threaded
> 3. Sorted-dup / duplicate semantics (not applicable to noxu-rep)
> 4. Fictional APIs in docs
> 5. Cross-restart durability of replication state
> 6. BDB-JE divergence not documented

## 1. Scope

### Audited

* **Source**: `crates/noxu-rep/src/**/*.rs` (22,452 LOC across 41 files,
  including TLS, QUIC, TCP, election, VLSN, peer-feeder, network-restore
  client + server, and the `ReplicatedEnvironment` API).
* **Documentation**: every chapter under `docs/src/replication/`
  (README, concepts, setup, elections, consistency, durability,
  dynamic-membership, master-transfer, network-restore, transport).
* **Public surface enumerated** from `crates/noxu-rep/src/lib.rs`
  re-exports plus everything reachable via `pub` on those types.
* **Cross-references** examined: `noxu-db::durability::ReplicaAckPolicy`,
  `noxu-db::error::NoxuError::{ReplicaWrite, InsufficientReplicas,
  RollbackRequired}`, `noxu-spec::{flexible_paxos, master_transfer,
  network_restore, vlsn_streaming}` and `crates/noxu-rep/tests/`
  (cluster, chaos, torture, tcp, prop, replica-scale, phi-detector,
  quorum-policy).

### Excluded

* No multi-process cluster was started. Behaviour under partition,
  fault injection, or real network conditions was inferred from code
  paths and existing chaos tests.
* `noxu-rep/quoracle/` (vendored submodule) is treated as a black-box
  dependency; only its surface as consumed by `QuorumPolicy` was checked.
* The `tls.rs` module (1402 LOC) was not deeply audited; only
  cross-referenced for `unwrap`/`panic` on network-derived data, of which
  there are none on the production path (only in tests).

## 2. Methodology

For each public type / method:

1. **Enumerate the public surface**: `lib.rs` re-exports plus reachable
   `pub fn` / `pub struct` from each module.
2. **Read the rustdoc** and the matching mdBook chapter that documents it.
3. **Read the implementation**: every numbered finding has a file:line
   citation pointing at the actual code.
4. **Cross-reference** the code against:
   * the HA semantics it claims to port (`ReplicatedEnvironment`,
     `ReplicationConfig`, `Durability.ReplicaAckPolicy`,
     `StateChangeListener`, `NetworkRestore`, master transfer),
   * the abstract Stateright specs in `noxu-spec/`,
   * the existing `claim-audit-2026-05.md` findings (so we extend rather
     than re-state).
5. **Severity** uses the same scale as the May 2026 API audit:
   * **critical** — silently violates a documented safety/durability
     contract or is a remote DoS / unsafe deserialization on the
     public network surface;
   * **high** — feature is documented as working but the production
     path is unwired, broken-on-arrival, or returns success while
     doing nothing observable;
   * **medium** — config-not-plumbed, missing error mode, unsound
     ordering or counting that would surface in larger clusters;
   * **low** — doc-string drift, naming, or minor wording issues;
   * **info** — observation, not a defect.

### Audit limits (be honest)

* No live exercising of multi-node clusters with real packet loss,
  asymmetric partitions, or master-host crashes was performed.
* No fault injection was run against the QUIC/TCP transports.
* `cargo test -p noxu-rep` was not re-run; existing test results were
  read but not reproduced.
* Several large modules (`net/quic_mux.rs`, `tls.rs`, `vlsn_index.rs`)
  were skimmed rather than read line-by-line; findings about them are
  conservative.

## 3. Findings table

| Sev | Subject | Doc claim | Actual | Recommendation |
|---|---|---|---|---|
| critical | `Transaction::commit` durability under replication | `Durability.ReplicaAckPolicy` controls how many replicas must ack before commit returns | `noxu-rep::CommitDurability::required_acks` is never called outside its own tests; commit path in `noxu-db` returns immediately after local fsync regardless of policy | Wire the master commit path through `AckTracker::register/wait_with_timeout` keyed on the configured `ReplicaAckPolicy`; surface `NoxuError::InsufficientReplicas` |
| critical | `NetworkRestore::execute()` against a `ReplicatedEnvironment` | docs/setup imply restore "just works" via the dispatcher | Client connects via raw `TcpStream::connect` and writes 4-byte magic `NRST`; the dispatcher's `handle_incoming` reads those 4 bytes as a `u32 LE` service-name length (`0x4E52_5354` = 1.31 GiB) → tries to allocate 1.3 GiB and read that many bytes for a name. Restore can never succeed against a `ReplicatedEnvironment` | Make `NetworkRestore::execute()` use `connect_to_service(addr, "RESTORE")`, OR have the dispatcher detect the `NRST` magic. Bound the service-name length |
| critical | `TcpServiceDispatcher` framing | `"service name [len: u32 LE][utf8 bytes]"` | `handle_incoming` reads `name_len: u32` then `vec![0u8; name_len]` — no bound. 4 bytes from any peer cause up to 4 GiB allocation; 100 such connections OOM the dispatcher | Cap service-name length (≤ 256 bytes is more than enough); reject larger lengths before allocation |
| critical | `NetworkRestoreServer::handle` (dispatcher path) | "stream each file's name + size + bytes" | The ServiceHandler implementation buffers every `.ndb` file into a single `Vec<u8>` `payload` then calls `channel.send(&payload)`. `MAX_FRAME_PAYLOAD = 64 MiB` (`channel.rs:40`); restore of any DB > 64 MiB is rejected by the receiver, smaller DBs allocate the full size in RAM on the master | Stream chunks as separate framed messages, or extend the channel API with a "stream-of-frames" mode. The standalone `serve_raw` path is correct; only the dispatcher path is broken |
| critical | `run_acceptor` Paxos safety (`elections/paxos.rs:248`) | "rejects Phase 1 only if a higher-numbered proposal was already promised" | `promised_term: Option<u64>` is a function-local `let` that resets to `None` on every call. Two concurrent acceptor sessions on the same node have independent promises; there is no persistent state, so a node can promise term *T* on connection A and also on connection B with term *T-1*. Across restart there is no record at all | Promise state must be in `ReplicatedEnvironment` (or a sibling `Acceptor` struct) shared across all acceptor calls; promised_term + accepted value must be persisted (e.g., in a small dedicated WAL record) before the Promise is sent |
| high | Election driver wiring | docs/concepts § 1, lib.rs `new()` doc: "creation will trigger an election" | `run_election` / `run_acceptor` / `Election` are library code. Nothing in `ReplicatedEnvironment` calls them. State transitions are externally driven by `become_master(term)` / `become_replica(name)`; the only way to actually run elections is for the application to call them by hand. (Confirmed by the closely related `claim-audit-2026-05.md` findings on `new()` and `become_master`.) | Either (a) add a daemon thread inside `ReplicatedEnvironment` that runs the election loop and applies its outcome via the state machine, or (b) explicitly retract the "creation will trigger an election" promise in docs and rename `become_master` → `force_master` |
| high | `transfer_master` (`replicated_environment.rs:788`) | "ensures all changes at this node are available at the new master upon conclusion of the operation" | Body validates `is_master`, logs the intent, returns `Ok(())`. No drain, no sync, no abdicate. (Re-flagged from `claim-audit-2026-05.md` for completeness — still unfixed at audit time.) | Either implement the four-step protocol (drain → sync → abdicate → reconnect) using `MasterTransfer` and a real ABDICATE wire message, or remove the API and document the absence |
| high | `shutdown_group` (`replicated_environment.rs:959`) | "Master waits for all active Replicas to catch up… and then shuts them down" | Validates `is_master`, logs, calls `self.close()`. No catch-up wait. No replicas notified. | Send `Shutdown { reason }` to every active replica, await ack with timeout, then close. Until then, document the limitation |
| high | `become_master` feeder spawn | "FeederRunner + EnvironmentLogScanner background thread is spawned for each currently-registered replica" | Iterates `feeders_snap`, only `log::info!`s "served by PeerFeederService on the TCP dispatcher". No FeederRunner is ever spawned. The peer-feeder service handles connections only when an external replica initiates one — there is no per-replica feeder loop pushing entries out | Spawn a `FeederRunner` per replica (or change the doc to say "feeders are pull-based; replicas connect to PEER_FEEDER and stream") |
| high | `apply_entry` unbounded memory growth | "Applies a log entry received from the master" | `peer_scanner.push(vlsn, entry_type, _data)` (replicated_environment.rs:830) appends every applied entry to an in-memory `VecDeque` with no eviction. A long-running replica accumulates one VecDeque entry per replicated record forever. The "_data" leading underscore signals that this entry is stored only for re-broadcast, never applied to the local log | Cap the queue, drop entries below CBVLSN, or back the scanner with the on-disk log (`EnvironmentLogScanner`) once `with_environment` has been called |
| high | VLSN index cross-restart durability | "VlsnIndex maps VLSNs to log file positions" (lib.rs) | `VlsnIndex` is built fresh in `ReplicatedEnvironment::new` and lives entirely in memory (`vlsn/vlsn_index.rs:55` `put`/`register`). Nothing reads it from disk on restart; nothing writes it to disk on commit. After a crash, a replica has no record of which VLSNs it had applied; the only safe action is a full network restore, which itself is broken (see above) | Persist VLSN→LSN map alongside the log (typical option: rebuild from the log during recovery) and load it before resuming replication |
| high | `RepStats` is decorative | docs/concepts § 6 implies counters reflect production activity; `consistency.md` shows `stats.replica_lag_ms()`, `stats.known_master_vlsn()`, `stats.local_vlsn()` | Not a single one of `elections_held`, `elections_won`, `elections_lost`, `feeders_created`, `feeders_shutdown`, `acks_received`, `ack_timeouts`, `entries_replicated`, `entries_applied`, `bytes_replicated`, `max_replica_lag_ms` is incremented anywhere outside `rep_stats.rs` itself. Production code never updates them | Either remove `RepStats` from the public surface or wire the increments into the corresponding code paths |
| high | Doc-API drift in `setup.md` | full Rust example with `RepConfig::builder().node_name(…).node_address(…).group_name(…).initial_peers(vec![…])` and `ReplicatedEnvironment::new(env, rep_config)` | None of those builder methods exist. `RepConfig::builder` is `(group_name, node_name, node_host) -> RepConfigBuilder`; address is a separate `node_port`; initial peers are added one at a time via `add_initial_peer`; `ReplicatedEnvironment::new` takes a single `config` argument and does NOT wrap an existing `Environment`. The example will not compile | Replace the example with a copy-paste-clean working snippet; the unit tests in `replicated_environment.rs:1015+` show the real shape |
| high | Doc-API drift in `dynamic-membership.md` | `RepNode::new("node-4", "192.168.1.14:5001")` followed by `.with_read_capacity_pct(70).with_write_capacity_pct(50).with_latency_hint_ms(3)` | Real `RepNode::new(name: String, node_type: NodeType, host: String, port: u16, node_id: u32)` (5 args). Real builders are `with_read_capacity(f64)`, `with_write_capacity(f64)`, `with_latency_hint(Duration)`. The percent-suffix builders do not exist | Rewrite the dynamic-membership chapter using real signatures |
| high | Doc-API drift in `durability.md` | `RepConfig::builder().replica_ack_policy(SimpleMajority).replica_ack_timeout_ms(5000)` | No such builder methods. The real path is `commit_durability(CommitDurability::new(policy, timeout))`. `replica_ack_timeout` exists as a separate field but with no documented relationship to `commit_durability.ack_timeout` | Document the actual builder; clarify which timeout governs commit-side ack waiting |
| high | Doc-API drift in `consistency.md` | `ConsistencyPolicy::NoConsistencyRequired`, `Time { permissible_lag, timeout }`, `CommitPoint { vlsn, timeout }`; `db.get_with_consistency(txn, key, policy)`; `rep_env.get_rep_stats()`; `stats.replica_lag_ms() / known_master_vlsn() / local_vlsn()` | Real variants are `NoConsistency`, `TimeConsistency { max_lag, timeout }`, `CommitPointConsistency { vlsn: i64, timeout }`. None of `db.get_with_consistency`, `rep_env.get_rep_stats`, `stats.replica_lag_ms()`, `known_master_vlsn()`, `local_vlsn()` exist. The actual stats are `pub AtomicU64` fields, not methods | Rewrite using real variant names; expose proper getters or document the public-field convention |
| high | Doc-API drift in `master-transfer.md` | `rep_env.initiate_master_transfer("node-2", Duration::from_secs(30))?` | Method does not exist. Real entry point is `transfer_master(MasterTransferConfig::new(target_node, timeout))` and it currently does nothing (see finding above) | Fix signature; once implemented, document the actual return semantics |
| high | Doc-API drift in `network-restore.md` | "master's `NetworkRestoreProvider` (wired into `TcpServiceDispatcher`)" | The struct is `NetworkRestoreServer`; no such thing as `NetworkRestoreProvider` | Rename or recompile-test the doc |
| high | `ConsistencyPolicy::TimeConsistency` "1 VLSN ≈ 1 ms" approximation | docs/consistency `TimeConsistency` is documented as wall-clock lag bound | Implementation `consistency.rs:67-78` uses `master_vlsn − current_vlsn` directly as milliseconds (`let lag_ms = lag_vlsns as u64`), with an inline comment "in a real implementation this would use timestamps from heartbeat messages". The error returned (`ReplicaLagExceeded { lag_ms }`) conveys VLSN counts, not milliseconds | Either thread heartbeat timestamps through and compute real lag, or rename the policy to `VlsnLagConsistency` |
| medium | Acceptor counter-proposal handling | `paxos.rs:151` Phase 1 accepts a counter-proposal's `ElectionProposal` as a "Promise" | When a peer sends back its own `ElectionProposal` (with `term: peer_term`), the proposer treats that as a Promise (`promises.push(...)`) regardless of whether `peer_term ≥ proposer_term`. A peer that promised a strictly higher term to a competing proposer can still register a Promise here, breaking Paxos invariants | The acceptor's response should be either `ElectionVote { granted: true, term: my_promised_term }` or `Reject`. If a counter-proposal is sent, it must include the promised term so the proposer can reject when it is higher |
| medium | `GroupService::quorum_size` ignores configured `QuorumPolicy` | docs/concepts § 3 says quorum is `RepGroup::quorum_size` driven by `QuorumPolicy` | `GroupService::quorum_size()` (`group_service.rs:331`) computes `(electable_count / 2) + 1` directly. `RepGroup::quorum_size()` (`rep_group.rs:144`) consults the policy. Two different views of "quorum" depending on which API the caller takes | Make `GroupService::quorum_size` delegate to a shared `QuorumPolicy`, or remove it |
| medium | `electable_count` divergence | docs imply Arbiters participate in elections | `RepGroup::electable_count` (`rep_group.rs:106`) filters by `is_electable()` (= `Electable \| Arbiter`);`GroupService::electable_count`(`group_service.rs:345`) filters by`node_type == Electable`. So an Arbiter is in/out of the quorum count depending on which struct the caller asks.`CommitDurability::required_acks` takes a u32 and is shared; the answer differs based on caller | Pick one definition and use it everywhere. BDB-JE counts arbiters in elections but not in data-replica acks; the Rust port needs separate "elector count" and "data-replica count" |
| medium | Arbiters can win elections | `node_type.rs::can_be_master() == false` for `Arbiter` | `paxos::run_election` resolves the winner by name lookup but never checks `can_be_master`. If an Arbiter happens to share a high VLSN (e.g., 0 in a fresh group) and majority votes flow back, the Arbiter is returned as the winner. There is no guard | Filter out non-`can_be_master` nodes from `best_proposal` and resolve ties to data nodes |
| medium | `NetworkRestore::execute` is non-atomic | docs/network-restore step 4: "replica replaces its local log with the received files" | Each `.ndb` is written directly to its destination path; on a partial transfer the destination is left as a truncated file. `retain_log_files` only renames the existing file; the in-progress file is the target itself, not a `.tmp` | Write each file to `dest_path.with_extension("partial")` and `fs::rename` after the last byte; `fsync` the directory afterwards |
| medium | RESTORE service authentication | docs/concepts § 5 "replication peers are authenticated at the Paxos layer" | RESTORE service is reachable on the same TCP dispatcher and has only the 4-byte `NRST` magic gate. Any caller that can reach the port can copy the entire log set. The QUIC variant uses TLS+rcgen but with `SkipCertVerification` — trust on first connect only | Bind RESTORE to a separately authenticated channel, or check that the requesting peer's address matches a `GroupService` member |
| medium | Replication wire protocol has no version | `protocol.rs:6` "simple tag+length+value" | No protocol-version handshake. Adding a tag breaks all older replicas; older replicas decoding `tag = 13` get `ProtocolError("unknown tag")` and disconnect. Same for the network-restore wire format | Reserve `Handshake.version: u16` (or wrap protocol in a `[version: u16][message]`); document compatibility window |
| medium | `apply_entry` ignores entry payload | `apply_entry(vlsn, entry_type, _data: Vec<u8>)` silently discards `_data` (note the leading underscore) when no environment is wired in | The replica's local log is only written if the receive-loop in `become_replica` is wired (`replicated_environment.rs:684`). Without `with_environment`, every received entry is registered in the in-memory VLSN index with file=0 offset=0 (`vlsn_index.register(vlsn, 0, 0)`) and the data is dropped. Tests `test_apply_entry_registers_vlsn` confirm this no-op behaviour passes | Either require `with_environment` before `become_replica`, or document that data is lost without it |
| medium | NodeState regex contradicts code | lib.rs and `replicated_environment.rs` doc: `[ MASTER \| REPLICA \| UNKNOWN ]+ DETACHED` | `NodeState::Shutdown` is the terminal state in the actual machine; transitioning from `Shutdown` to `Detached` is rejected. The visible state at the end of life is `SHUTDOWN`, not `DETACHED`. The state machine has 5 states; only 4 are documented in the regex | Either add `Shutdown` to the documented regex or rename `Shutdown` → `Detached` to match doc and BDB-JE |
| medium | `NoxuError::ReplicaWrite` and `NoxuError::InsufficientReplicas` are unreached | `noxu-db::error` (line 502, 505) "A write was attempted on a replica node"; "Insufficient replicas acknowledged the commit" | grep over the entire workspace shows neither variant is constructed outside `error.rs` and a single test that builds the value directly. Nothing in `noxu-db` knows whether the local environment is replicated, so writes on a replica are *not* rejected by `noxu-db` and commits do not block on acks | Either thread a `replica_state: Option<NodeState>` into `EnvironmentImpl`/`Transaction`, or move the variants to `noxu-rep::RepError` and remove from `noxu-db` |
| medium | `MasterTransfer::start/complete/fail` are an isolated state machine | implies it tracks an actual transfer | The `MasterTransfer` struct in `master_transfer.rs` is a Mutex around an enum and a timer. Nothing in `ReplicatedEnvironment::transfer_master` instantiates it; it's a publicly exported state-tracking struct that no production code drives | Remove from public API or wire it up |
| medium | `set_state_change_listener` "one listener per node" claim | "there is one listener per replication node, not one per handle. Invoking this method adds to the set of listeners." | `replicated_environment.rs:885` actually pushes into a `Vec<Arc<dyn StateChangeListener>>` — it's a multi-listener API. The "one per node" doc is contradicted by the next sentence in the same docstring; keep the multi semantics, fix the first sentence | Tighten doc to "any number of listeners may be registered; each is notified for every transition" |
| medium | `Stateright` specs vs. production divergence | `noxu-spec/src/lib.rs:17` lists `flexible_paxos` as a model of `noxu-rep::elections::paxos::run_election` / `run_acceptor` | The Stateright `flexible_paxos.rs` checks an *abstract* `ElectionSafety` invariant (at most one leader per term) over an idealised model that *assumes* persistent promised-term state per acceptor. The production `run_acceptor` does not have persistent promised-term state (see critical finding above), so the spec proves a property the implementation does not guarantee | Either persist the promise state and re-prove the spec models the implementation, or make the spec explicitly reflect "promise state lost on every connection" and rerun the model checker |
| medium | `Stateright` spec references missing module | `noxu-spec/src/lib.rs:55` mentions `master_transfer::NodeRole`; `master_transfer.rs:10` lists `crates/noxu-rep/src/elections/master_term.rs` as the production analogue | `crates/noxu-rep/src/elections/master_term.rs` does not exist (`ls elections/` shows only `election_config, election, master_tracker, mod, paxos, phi_detector, proposal`). The spec is referencing a module that was never written | Delete the reference or write the corresponding module |
| medium | `phi_detector` first-call sample is dropped | `phi_detector.rs::record_heartbeat` doc: "Updates the inter-arrival sample window." | First call has no previous heartbeat so no inter-arrival sample is added (`claim-audit-2026-05.md` low). Re-flagged here because the production `MasterTracker.with_phi` has no warm-up wait — `is_master_alive()` returns `phi.is_available()` which is `true` when fewer than 2 samples exist, but `suggested_phase_timeout` falls back to `fallback` when there is no σ. Behaviour is correct; doc is imprecise | Note in doc that the first heartbeat seeds `last_timestamp` but produces no sample |
| medium | `update_peer_metadata` can violate FPaxos quorum invariant | docs/dynamic-membership: "It is safe to call while replication streams are active" | `update_peer_metadata` adjusts capacity/latency without validating `phase1 + phase2 > n` for `Flexible` policies. If a node's `read_capacity_pct=0` would push it out of the LP-optimal phase-1 quorum under `Expression`, no rebuild check fires (the rebuild goes through `set_quorum_policy` only) | Re-validate `quorum_policy.validate(electable_count)` after every metadata update; reject if it fails |
| low | `lib.rs` example uses old constructor | "let config = RepConfig::new(\"my_group\".to_string(), \"node1\".to_string(), \"localhost\".to_string(), 5001);" | `RepConfig::new` does not exist; only the builder. Example is `ignore`d so doctest does not catch it | Replace with the builder form |
| low | RepConfig `replica_ack_timeout` has no consumer | docstring says "How long to wait for replica acknowledgments" | grep shows the field is set in builder, copied in `build()`, never read again. The actual ack wait, if it existed, would consult `commit_durability.ack_timeout` instead | Remove `replica_ack_timeout` (it's dead config) or wire it as a fallback when `commit_durability.ack_timeout` is unset |
| low | RepConfig `feeder_timeout` has no consumer | "How long the master waits for a replica feeder response" | Set in builder, never consulted on the feeder/peer-feeder/replica-stream paths | Either remove or wire into `Feeder::run` |
| low | RepConfig `helper_hosts` has no consumer | docstring "Helper hosts for joining the group" | Set, stored, never read by any non-test code. New nodes joining never reach out to helpers; the only way to populate `GroupService` is `add_peer` calls or `initial_peers` | Either implement helper-host bootstrap or remove the field |
| low | Default `node_port = 5001` is shared with PostgreSQL | `rep_config.rs:23` | 5001 is a registered port for various services; collision is likely. The constant matters only as a default; users override it | Pick an unprivileged ephemeral default, e.g., 14_000 |
| info | `apply_entry`'s third argument | `apply_entry(vlsn, entry_type, _data)` | The `_data` underscore means the parameter is intentionally unused except for forwarding to `peer_scanner.push`. This is a deliberate placeholder until the env-impl-backed path is wired | Track via a single GitHub issue |

## 4. Detailed findings

The findings below expand the table above with file:line citations,
reproducer sketches, and BDB-JE / Stateright references. They are
numbered to match the order in the table for cross-reference.

### 1. `Transaction::commit` does not honour `ReplicaAckPolicy` (CRITICAL)

* **File**: cross-crate. `crates/noxu-rep/src/commit_durability.rs:34`
  defines `ReplicaAckPolicy::required_acks`. The `noxu-db` commit path is
  in `crates/noxu-db/src/transaction.rs:172` (`commit`) and `:190`
  (`commit_with_durability`).
* **Doc claim**: docs/replication/durability.md:11–18 describes the
  enum and explicitly says "Master waits for majority of replicas".
  `noxu-db` re-exports its own `ReplicaAckPolicy`
  (`noxu-db::durability::ReplicaAckPolicy`) and `Durability` carries
  the `replica_ack` field.
* **Actual**: a workspace-wide grep for `commit_durability`,
  `ReplicaAckPolicy`, or `required_acks` shows zero callers outside
  `noxu-rep` itself or its tests. `noxu-db::Transaction::commit` does
  not consult any replication state. `noxu-db` has no reference to
  `noxu-rep::AckTracker`, no `Arc<ReplicatedEnvironment>` field, and no
  way to know whether the env is replicated. `NoxuError::ReplicaWrite`
  and `NoxuError::InsufficientReplicas` are defined
  (`crates/noxu-db/src/error.rs:502, 505`) but never returned.
* **Expected**: BDB-JE `Txn.commit(Durability)` waits for
  `Durability.ReplicaAckPolicy` acks via the master's ack tracker or
  fails with `InsufficientAcksException`. Stateright spec
  `vlsn_streaming` models a corresponding ack flow.
* **Reproducer**: open a `ReplicatedEnvironment`, call
  `become_master(1)`, `register_vlsn(1, 0, 100)`, then commit a real
  transaction with `Durability { replica_ack: All, … }`. The commit
  returns `Ok(())` even though no replica is connected and no ack was
  recorded.
* **Recommendation**: thread `Arc<ReplicatedEnvironment>` (or just
  `Arc<AckTracker>`) into `noxu-db::Transaction::commit`; on master,
  call `ack_tracker.register(commit_vlsn, required_acks(electable))`,
  block on `ack_tracker.wait(commit_vlsn, ack_timeout)`, surface
  `NoxuError::InsufficientReplicas`. On replica, return
  `NoxuError::ReplicaWrite` on any `commit` of a non-empty write set.

### 2. NetworkRestore client/server protocol mismatch (CRITICAL)

* **File**: client `crates/noxu-rep/src/network_restore.rs:209` opens
  a raw `TcpStream::connect(addr)` and writes the 4-byte magic
  `RESTORE_MAGIC = 0x4E52_5354`. Server side: registered on the
  dispatcher in `replicated_environment.rs:228` via
  `dispatcher.register(RESTORE_SERVICE_NAME, …)`.
* **Doc claim**: docs/replication/network-restore.md describes a
  unified flow that "uses a dedicated TCP service
  (`NetworkRestoreServer`) registered on the `TcpServiceDispatcher`".
* **Actual**: the dispatcher's wire protocol begins with
  `[name_len: u32 LE][utf8 bytes]` (`net/service_dispatcher.rs:233`).
  When the client writes the 4 magic bytes first, the dispatcher
  parses them as a name-length, gets `0x4E52_5354` ≈ 1.31 GiB, and
  tries to allocate that much RAM for the name. Restore therefore
  cannot succeed against a `ReplicatedEnvironment` whose dispatcher is
  the only listener.
* **Tests**: `tests/cluster_integration_test.rs:251–339`
  `test_env_home_registers_restore_service` exercises a *standalone*
  `NetworkRestoreServer` (not the dispatcher path) — that is why this
  bug has gone unnoticed.
* **Expected per BDB-JE**: BDB-JE has a dedicated network-restore TCP
  port and a `NetworkRestore` client that speaks a stream protocol
  matching the server it connects to. Stateright spec
  `network_restore` models a resumable file copy that doesn't depend
  on Java details, but it does assume the wire protocol on both sides
  is identical.
* **Reproducer**: start a `ReplicatedEnvironment` with
  `env_home(some_path)`. From a second process, run
  `NetworkRestore::new(cfg).execute()` against the dispatcher's port.
  The dispatcher panics or hangs trying to allocate ~1.3 GiB.
* **Recommendation**: change `NetworkRestore::execute()` to call
  `service_dispatcher::connect_to_service(addr, "RESTORE")` first,
  then send the magic over the framed channel. Update
  `NetworkRestoreServer::handle` to read its 4-byte magic from the
  framed channel (which it already does for the service-handler path,
  but the rest of the body is broken — see finding 4). Until then,
  document that network restore only works against a *standalone*
  `NetworkRestoreServer::start(addr)`.

### 3. `TcpServiceDispatcher` unbounded service-name allocation (CRITICAL DoS)

* **File**: `crates/noxu-rep/src/net/service_dispatcher.rs:241`
  `let name_len = u32::from_le_bytes(len_buf) as usize; let mut
  name_buf = vec![0u8; name_len];`
* **Doc claim**: transport.md and concepts.md describe service
  negotiation as `[name_len: u32 LE][service_name: utf8 bytes]`.
* **Actual**: `name_len` is unchecked. A malicious or accidental peer
  sending a 4-byte u32 max value causes a 4 GiB allocation (or OOM if
  smaller). 1000 such connections × 4 GiB = guaranteed OOM. Even on
  64-bit hosts the allocation can take long enough to amplify the
  attack.
* **Expected**: every length prefix should be bounded by an explicit
  ceiling (`MAX_SERVICE_NAME_LEN = 256` bytes is more than enough — the
  longest defined service name is `"PEER_FEEDER"`).
* **Reproducer**: `nc <addr> <port>` and send `\xFF\xFF\xFF\xFF`. The
  dispatcher thread panics or OOMs.
* **Recommendation**: add `if name_len > MAX_SERVICE_NAME_LEN { return; }`
  before the `vec![0u8; name_len]`. The same bound check applies to
  every length-prefixed `String` decoded in `protocol.rs::decode_string`
  (`u32 LE` length); audit there too — though the channel layer's
  `MAX_FRAME_PAYLOAD = 64 MiB` provides a partial cap.

### 4. NetworkRestore dispatcher path buffers entire DB into RAM (CRITICAL)

* **File**: `crates/noxu-rep/src/network_restore_server.rs:316–362`
  in the `ServiceHandler::handle` impl. A single `Vec<u8> payload` is
  built (`payload.extend_from_slice(&chunk[..n])`) for every byte of
  every `.ndb` file, then `channel.send(&payload)?`.
* **Doc claim**: docs/network-restore.md "stream each file's name +
  size + bytes" — implies streaming, not buffering.
* **Actual**: `channel.send` enforces `payload_len ≤ MAX_FRAME_PAYLOAD
  = 64 MiB` (`net/channel.rs:40, 745`). Any DB larger than 64 MiB is
  rejected by the receiver. DBs smaller than 64 MiB but several GiB
  smaller than free RAM still allocate the full DB before sending it,
  doubling the master's working set.
* **Expected**: the standalone `serve_raw` path (`:128`) streams
  chunk-by-chunk. The dispatcher path should do the same, possibly by
  extending `Channel` with `send_streaming(&[&[u8]])` or a helper that
  emits multiple framed messages.
* **Reproducer**: create a 100 MiB log file, register
  `NetworkRestoreServer` via `with_environment`, attempt restore via
  the dispatcher path. Receiver returns frame-too-large.
* **Recommendation**: send each file as its own frame, or extend the
  channel framing with chunked frames (length 0xFFFF_FFFF = "more
  follows"). Until then, never use the dispatcher for restore on
  databases larger than ~50 MiB.

### 5. `run_acceptor` loses promise state across messages (CRITICAL)

* **File**: `crates/noxu-rep/src/elections/paxos.rs:248–326`. The
  function signature is `fn run_acceptor(channel, name, own_vlsn,
  own_priority, own_term)`; it receives ONE Phase 1 message and ONE
  Phase 2 message and returns. The local `let mut promised_term:
  Option<u64> = None;` is therefore reset every time the function is
  called.
* **Doc claim**: docs/concepts.md § 1 describes Paxos where "an
  acceptor that has already promised a higher-termed proposal rejects
  with ElectionVote { granted: false }".
* **Actual**: there is no shared promise state across acceptor
  invocations. Two concurrent Paxos rounds against the same node use
  two different `run_acceptor` invocations with two independent
  `promised_term` values — the second invocation will gladly promise
  a *lower* term than the one already promised in the first
  invocation. Across crash there is also no record of what was
  promised.
* **Expected per Paxos**: a Paxos acceptor maintains *persisted*
  `(promised_ballot, accepted_ballot, accepted_value)` and consults
  them on every Phase 1 / Phase 2 message; promised values must
  survive crash.
* **Stateright spec divergence**: `noxu-spec/src/flexible_paxos.rs`
  models acceptors with persistent promise state (an array indexed by
  acceptor) — its `ElectionSafety` proof relies on that persistence.
  The production code does not match the model; the spec proves a
  property the implementation does not guarantee.
* **Reproducer**: spin two proposers in separate threads, both
  targeting the same acceptor. Both will receive `Promise` for the
  same term. Both will think they have quorum if their other peers
  also agree. Two masters per term is possible.
* **Recommendation**: hoist `promised_term` (and accepted-value) onto
  a shared `Acceptor` struct held by `ReplicatedEnvironment`,
  protected by a mutex; persist on every change before sending the
  Promise/Accept response.

### 6. Election driver is unwired (HIGH)

* **File**: `replicated_environment.rs:181`. The `new()` function
  starts the TCP dispatcher but never starts an election or master
  watchdog.
* **Doc claim**: lib.rs and concepts.md state that creating a
  `ReplicatedEnvironment` "starts participating in the replication
  group" and that "creation will trigger an election".
* **Actual**: `run_election`, `run_acceptor`, `Election`, and
  `MasterTracker` are present but uncalled from production code paths.
  Tests call `become_master(term)` directly; in deployment the
  application has to do the same. The PEER_FEEDER service is
  registered but it only listens; nothing ever proposes.
* **Reproducer**: open three `ReplicatedEnvironment`s with each other
  in `helper_hosts`. None becomes master. None becomes replica. They
  all sit in `Detached`.
* **Recommendation**: add a daemon thread inside the env that drives
  elections via `run_election` against peers in `GroupService`,
  applies outcomes through the state machine. See
  `cluster_integration_test.rs:94`
  `test_election_over_tcp_channels` for what the wiring would look
  like.

### 7–8. `transfer_master`, `shutdown_group` are stubs (HIGH)

Already covered in `claim-audit-2026-05.md`. Re-flagged for the
production-readiness summary; status unchanged at audit time.

### 9. `become_master` does not spawn feeder threads (HIGH)

Already covered in `claim-audit-2026-05.md`. The peer-feeder service
*is* registered; replicas can pull, but the master never pushes. In
combination with finding 6 (no election driver) this means a
production deployment built on this code never actually replicates.

### 10. `apply_entry` unbounded peer_scanner growth (HIGH)

* **File**: `replicated_environment.rs:830` plus
  `stream/peer_feeder.rs:87` (`PeerLogScanner::push`).
* **Actual**: every replicated entry is pushed into a
  `Mutex<VecDeque<(u64, u8, Vec<u8>)>>` for downstream re-broadcast.
  No cap, no eviction below CBVLSN, no bound on payload size.
* **Reproducer**: become a replica, receive 10 million 1-KiB entries
  → ~10 GiB RSS even though the local log already holds the same
  data on disk.
* **Recommendation**: cap the queue, drop entries below CBVLSN
  (`GroupService::get_cbvlsn`), or back the scanner with the on-disk
  log via `EnvironmentLogScanner` once `with_environment` has been
  called.

### 11. VLSN index has no on-disk persistence (HIGH)

* **File**: `vlsn/vlsn_index.rs`, `vlsn/vlsn_bucket.rs`,
  `vlsn/vlsn_range.rs`. None of them open or write a file.
* **Doc claim**: lib.rs "Maps VLSNs to log file positions" — implies
  durable mapping. The cleaner relies on CBVLSN to know which log
  files are safe to reclaim.
* **Actual**: the VLSN index is built fresh every time
  `ReplicatedEnvironment::new` runs. After a crash there is no record
  of which VLSNs the local log already contains; on restart the
  replica claims `VlsnRange::default()` (range 0..0) and asks the
  master to start from 0, which fails or triggers a full
  network-restore (and that path is broken — see finding 2).
* **Recommendation**: persist VLSN→LSN map alongside the log
  (rebuild from the log during recovery is also acceptable; either
  way, code must be added to `noxu-recovery` to populate the index
  before replication resumes).

### 12. `RepStats` counters are decorative (HIGH)

* **File**: `crates/noxu-rep/src/rep_stats.rs`. None of `acks_received`,
  `entries_replicated`, `entries_applied`, `bytes_replicated`,
  `feeders_created`, `feeders_shutdown`, `elections_held`,
  `elections_won`, `elections_lost`, `ack_timeouts`,
  `max_replica_lag_ms` is incremented anywhere outside this file.
* **Doc claim**: docs/consistency.md exposes them via fictional
  `stats.replica_lag_ms()` etc.
* **Actual**: `ReplicatedEnvironment::get_stats() -> &RepStats` always
  returns zeros.
* **Recommendation**: either wire the counters into the corresponding
  paths (especially the master commit and the replica apply paths),
  or remove the type from the public surface.

### 13–17. Doc-API drift (HIGH × 5)

The five most egregious cases are listed in the table; in each case the
example will not compile. The full list of fictional symbols found:

* `RepConfig::builder()` (no args), `node_name()`, `node_address()`,
  `group_name()`, `replica_ack_policy()`, `replica_ack_timeout_ms()`,
  `initial_peers(Vec<RepNode>)`, `durability_sync_write`.
* `RepNode::new(name, address)` (2-arg form),
  `with_read_capacity_pct`, `with_write_capacity_pct`,
  `with_latency_hint_ms`.
* `ReplicatedEnvironment::new(env, rep_config)` (2-arg form),
  `initiate_master_transfer`, `get_rep_stats`.
* `ConsistencyPolicy::NoConsistencyRequired`,
  `ConsistencyPolicy::Time { permissible_lag, timeout }`,
  `ConsistencyPolicy::CommitPoint { vlsn, timeout }`.
* `db.get_with_consistency(txn, key, policy)`.
* `RepStats::replica_lag_ms()`, `known_master_vlsn()`,
  `local_vlsn()` (these are `AtomicU64` fields, not methods).
* `NetworkRestoreProvider`, `RepError::InsufficientAcks { needed,
  received }` — wait, that one *does* exist in `RepError`
  (`error.rs:41`), but the doc says it lives somewhere else (the doc
  language is "RepError::InsufficientAcks"; the implementation
  language is "InsufficientAcks"). OK — that one is consistent.

The setup.md, dynamic-membership.md, durability.md, consistency.md,
and master-transfer.md chapters all need rewriting against the real
public surface. The unit tests in `replicated_environment.rs:1015+`
show the canonical, working invocations.

### 18. `TimeConsistency` uses VLSN counts as ms (HIGH/medium)

* **File**: `consistency.rs:67–78`. Inline comment admits "Approximate:
  each VLSN is roughly 1ms of lag. In a real implementation this would
  use timestamps from heartbeat messages." The error returned is
  `ReplicaLagExceeded { lag_ms, limit_ms }` but `lag_ms = master_vlsn -
  current_vlsn`, i.e., a count, not a duration.
* **Recommendation**: either thread heartbeat timestamps through and
  compute real wall-clock lag, or rename `TimeConsistency` to
  `VlsnLagConsistency` to match its semantics.

### 19. Acceptor counter-proposal is unconditionally counted (MEDIUM)

* **File**: `paxos.rs:155–169`. When a peer responds with an
  `ElectionProposal` (its own counter-suggestion) the proposer pushes
  it onto `promises` regardless of any term comparison. A peer that
  already promised a strictly higher term to a competing proposer can
  still register a "Promise" here, breaking Paxos invariants.

### 20. `GroupService::quorum_size` ignores `QuorumPolicy` (MEDIUM)

* **File**: `group_service.rs:331`. Always returns
  `(electable_count / 2) + 1`. `RepGroup::quorum_size` does consult
  policy. The two structs hold the same membership but report
  different quorum sizes when `QuorumPolicy != SimpleMajority`.

### 21. `electable_count` divergence between `RepGroup` and `GroupService` (MEDIUM)

* **File**: `rep_group.rs:106` includes Arbiters; `group_service.rs:345`
  excludes them. `CommitDurability::required_acks` is computed in
  callers from one or the other depending on path; in production it
  is computed nowhere because it has no caller (finding 1).

### 22. Arbiters can be elected master (MEDIUM)

* **File**: `paxos.rs:202` resolves `winner_id` by name lookup
  without checking `node_type.can_be_master()`. An Arbiter at a tied
  VLSN (e.g., 0) can win; once it claims master it cannot serve
  reads (`is_data_node() == false`) and cannot generate VLSNs. Cluster
  is wedged until next election.

### 23. NetworkRestore non-atomic file write (MEDIUM)

* **File**: `network_restore.rs:268–308`. Each `.ndb` is written
  directly to its destination path; on a partial transfer the
  destination is left as a truncated file. `retain_log_files` only
  saves the *previous* file; the in-progress write target is the
  same path that future restarts will load.
* **Recommendation**: write to `dest_path.with_extension("partial")`,
  `fsync`, atomic-rename, `fsync(dir)`.

### 24. RESTORE service has no source authentication (MEDIUM)

The 4-byte magic is the entire gate. Anyone reachable on the
replication port can clone the entire `.ndb` directory. The QUIC
variant uses TLS but with `SkipCertVerification`.

### 25. Replication wire protocol has no version (MEDIUM)

`protocol.rs::decode` rejects unknown tags with
`ProtocolError("unknown tag")`. There is no version handshake;
adding a tag breaks every old replica. Same for the `RESTORE_MAGIC`
restore protocol.

### 26. `apply_entry`'s data is silently discarded without `with_environment` (MEDIUM)

`replicated_environment.rs:817` `apply_entry(vlsn, entry_type, _data)`.
The `vlsn_index.register(vlsn, 0, 0)` call records position
`(file=0, offset=0)` regardless of where the entry actually lives in
the log; the `_data` is fed to `peer_scanner` only. Without
`with_environment` having been called, the entry is never appended
to a real log and is lost on restart.

### 27. Documented state regex omits Shutdown (MEDIUM)

lib.rs and `replicated_environment.rs` doc claim `[ MASTER | REPLICA |
UNKNOWN ]+ DETACHED`. The implementation’s terminal state is
`Shutdown`, and `Shutdown → Detached` is rejected. Users observing
`get_state()` after `close()` see `SHUTDOWN`, not `DETACHED`.

### 28. `NoxuError::ReplicaWrite` / `InsufficientReplicas` are unreached (MEDIUM)

`crates/noxu-db/src/error.rs:502, 505`. Defined and tested for
formatting only. `noxu-db` has no awareness of replica state.

### 29. `MasterTransfer` is an isolated state machine (MEDIUM)

`master_transfer.rs::MasterTransfer::start/complete/fail/elapsed/is_timed_out`
are all real methods, but no production code instantiates a
`MasterTransfer`. `ReplicatedEnvironment::transfer_master` does not
even read the `target_node` of the supplied `MasterTransferConfig`
into a `MasterTransfer` struct.

### 30. `set_state_change_listener` "one listener per node" doc is wrong (MEDIUM)

`replicated_environment.rs:866` is "one listener per replication node,
not one per handle. Invoking this method adds to the set of
listeners." The two halves contradict each other; the implementation
matches the second half. This is a small wording fix.

### 31. Stateright `flexible_paxos` proves what production does not provide (MEDIUM)

The Stateright model in `noxu-spec/src/flexible_paxos.rs` assumes
acceptors maintain persistent promise state. Production does not.
The model proves `ElectionSafety` (≤1 leader/term) but the
implementation does not enforce the precondition the model relies on.
This is the most important divergence between code and spec.

### 32. Stateright spec references `master_term.rs` (MEDIUM)

`noxu-spec/src/lib.rs:55` and
`noxu-spec/src/master_transfer.rs:10` reference
`crates/noxu-rep/src/elections/master_term.rs`. The file does not
exist (the directory contains `election_config, election,
master_tracker, mod, paxos, phi_detector, proposal`). The spec is
referencing a never-written module.

### 33. `phi_detector` first-call sample dropped (MEDIUM/LOW)

Already covered in claim-audit-2026-05. Re-flagged to align scoring.

### 34. `update_peer_metadata` skips quorum revalidation (MEDIUM)

`replicated_environment.rs:464`. After updating capacity/latency
hints, the LP-optimal `Expression` quorum may no longer satisfy
intersection. There is no `quorum_policy.validate(electable_count)`
call after the update.

### Low / info findings 35–40

Covered in the table. These are doc-string churn items.

## 5. Coverage gaps in tests

The existing test suite is sizable (5,773 LOC across eight integration
test files) but it does **not** exercise the production paths flagged
above:

1. **Master commit blocked on acks**: no integration test commits a
   real transaction on a `ReplicatedEnvironment` master and verifies
   it blocks until N replicas ack. The closest test
   (`tcp_integration.rs:932`
   `test_ack_tracker_quorum_satisfies_durability`) only manipulates
   the `AckTracker` directly; it does not call `Transaction::commit`.
2. **NetworkRestore against a `ReplicatedEnvironment`**:
   `cluster_integration_test.rs:251` deliberately uses a *standalone*
   `NetworkRestoreServer`, bypassing the dispatcher path that
   production code wires. The dispatcher-integrated path has zero
   integration coverage (and would fail if it had any — see finding 2,
   4).
3. **Cross-restart durability**: no test creates a
   `ReplicatedEnvironment`, replicates entries, drops the env, and
   verifies the replica resumes with the correct VLSN. The VLSN index
   has no persistence at all (finding 11).
4. **Election driver from `new()`**: `cluster_integration_test.rs:94`
   `test_election_over_tcp_channels` calls `run_election` directly,
   not via `ReplicatedEnvironment::new`. There is no test that opens
   three `ReplicatedEnvironment`s and verifies one becomes master.
5. **DoS resistance of dispatcher framing**: no fuzzing or boundary
   test sends a 4-byte `0xFFFFFFFF` length to
   `TcpServiceDispatcher::handle_incoming` (finding 3).
6. **Unbounded peer_scanner**: no test verifies that long-running
   replicas drop entries below CBVLSN (finding 10). Existing tests
   push a few thousand entries; nothing watches RSS.
7. **Acceptor persistent promise**: there is no test that runs two
   concurrent proposers against the same acceptor. The Stateright
   model proves safety in the abstract; the production impl does not
   match (finding 5/31).
8. **Configured-but-unused fields**: `replica_ack_timeout`,
   `feeder_timeout`, `helper_hosts` are never asserted to *do*
   anything; only that they round-trip through the builder.

A useful addition would be a single integration test asserting, for
every `RepConfig` field, that mutating it changes some observable
behaviour. Today, that test would fail for at least four fields.

## 6. Summary by severity

| Severity | Count | Themes |
|---|---|---|
| critical | 5 | Config-not-plumbed (acks), broken-on-arrival network restore (×2 sub-issues), unbounded dispatcher allocation, election protocol unsafety |
| high | 13 | Election driver unwired, transfer/shutdown stubs, feeder spawn missing, peer-scanner unbounded, VLSN cross-restart durability, decorative stats, doc-API drift (×5), TimeConsistency-as-VLSN |
| medium | 16 | Counter-proposal Paxos handling, dual quorum/electable views, Arbiters elected master, non-atomic restore, no auth, no protocol version, apply_entry data discard without env, Shutdown vs Detached, unreached `noxu-db` error variants, isolated `MasterTransfer`, listener-count doc, Stateright spec divergence (×2), phi first sample, missing `update_peer_metadata` revalidation |
| low | 5 | `RepConfig::new` example, dead config fields (`replica_ack_timeout`, `feeder_timeout`, `helper_hosts`), default port collision |
| info | 1 | `_data` placeholder in `apply_entry` |
| **Total** | **40** | |

## 7. Cross-reference: blockers for v1.x / v2.0 GA

The following findings would prevent recommending the noxu-rep public
surface for production replication in a v2.0 GA release. They are
ordered by user-visible impact.

| # | Finding | Why it blocks GA |
|---|---|---|
| 1 | `ReplicaAckPolicy` not honoured on commit | The single most-marketed durability promise of the subsystem is silently a no-op. A user who configures `All` and runs without replicas connected sees commits succeed; they have no replica copy and don't know it |
| 5 / 31 | Acceptor promise state non-persistent | Two masters per term can be elected. Split-brain. Data loss if both write. The Stateright spec proves the wrong thing |
| 6 | Election driver unwired | A `ReplicatedEnvironment` constructed per the docs *never holds an election* — it just sits in `Detached`. The product does not work at all without externally-driven `become_master` |
| 2 / 4 | Network restore broken on the dispatcher path | New replicas cannot bootstrap; old replicas that fall behind cannot recover. The standalone path that does work is not the one wired into `ReplicatedEnvironment` |
| 3 | Dispatcher unbounded allocation | Any peer (or attacker on a private network with a typo) can OOM the master. No bound check on a 4-byte length prefix |
| 9 | `become_master` doesn't spawn feeders | Even if elections happened, the master would never push log entries — replicas would have to pull, and there is no pull driver either |
| 10 | `apply_entry` unbounded growth | A long-running replica OOMs in steady state |
| 11 | VLSN index lost on restart | A clean replica restart cannot resume; only path is full network restore (which is broken) |
| 7 / 8 | `transfer_master`, `shutdown_group` no-ops | Two operator-facing APIs that silently do nothing |
| 22 | Arbiters can win elections | Wedges the cluster |

## Resolution (Wave 3-3 / Wave 4-A)

| # | Status | Wave | Closing commit |
|---|---|---|---|
| 1 | **closed** | 3-3 | `feat(rep)!: honour ReplicaAckPolicy on Transaction::commit (F1)` |
| 2 / 4 | **closed** | **4-A** | `feat(rep): wire NetworkRestore through the dispatcher path (F2/F4)` |
| 3 | **closed** | 3-3 | `fix(rep)!: bound service-name length on TCP dispatcher (F3)` |
| 5 / 31 | **closed** | **4-A** | `feat(rep): persist Paxos acceptor promises across restarts (F5/F31)` |
| 6 | **closed** | 3-3 | `feat(rep): wire election driver into ReplicatedEnvironment::open (F6)` |
| 7 / 8 | **closed** | **4-A** | `feat(rep)!: implement transfer_master and shutdown_group (F7/F8)` |
| 9 | **closed** | **4-A** | `feat(rep)!: spawn feeder per known replica on become_master (F9)` |
| 10 | **closed** | 3-3 | `fix(rep): bound PeerLogScanner memory under sustained apply_entry (F10)` |
| 11 | **closed** | **4-A** | `feat(rep): persist VLSN index across restarts (F11)` |
| 22 | **closed** | 3-3 | `fix(rep): arbiters cannot win Paxos elections (F22)` |

All ten v2.0 GA blockers identified by this audit are closed.  See
[`wave-4-a-rep-ga-finish.md`](wave-4-a-rep-ga-finish.md) for the
resolution narrative and pointers to the new tests.

The remaining doc-drift, fictional-API, and decorative-stats findings
are **not** GA blockers — they are marketing/UX issues that should be
fixed before publication but do not by themselves invalidate
correctness.

The cumulative picture as of the original audit (May 2026) was that
the replication subsystem was at the **preview / proof-of-concept**
stage: the algorithms had been sketched, the wire formats and channels
worked for unit tests, the quorum library and phi detector were
excellent contributions, but **none of the algorithm modules were
wired into the public `ReplicatedEnvironment` API in a way that
delivered the documented behaviour end-to-end**.

This was remediated across Waves 3-3 and 4-A.  As of v2.0 the
`ReplicatedEnvironment` public API delivers the documented behaviour
end-to-end and all ten GA-blocker findings (1, 2, 3, 4, 5, 6, 7, 8, 9,
10, 11, 22, 31) are closed.  Subsequent waves will revisit the
Stateright specs against the production binary and address the
remaining medium/low audit findings.

— end of audit —
