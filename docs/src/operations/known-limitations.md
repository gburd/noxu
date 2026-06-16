# 7. Known Limitations

## Replication security — **deploy only on a trusted network**

The May-2026 security review (see
the 2026 review) identified
six blocker-class security gaps in the replication wire
protocol. Until those are closed, **the replication
subsystem must not be deployed across an untrusted network
boundary**:

- The replication wire protocol has no authentication. A peer
  identity (`group_name`, `node_name`) is self-claimed
  plaintext and not verified.
- **`peer_allowlist` (mTLS Phase 2 — v3.1.0)**: `RepConfig::peer_allowlist`
  is now enforced at the TLS handshake layer via
  `TlsTcpChannelListener::bind_with_tls_and_allowlist`.  Peers whose
  certificate Subject CN or DNS SAN does not match the configured list
  are rejected before any application data is exchanged.  Requires
  `RepTransportKind::Tls`; see the replication security setup guide.
- `NetworkRestoreServer` streams the entire on-disk
  environment to anyone who connects to its port.
- `PeerFeederService` streams the WAL to anyone who connects.
- Election proposals and votes are unsigned and unauthenticated.
  An on-path attacker can flip the cluster master.
- `NetworkRestore` (client) trusts server-supplied filenames —
  a malicious peer can write attacker-controlled bytes to any
  filesystem path the noxu process can write
  (path traversal).
- TCP frame `payload_len` (32-bit) is unbounded — a single
  attacker frame can trigger a 4 GiB allocation.
- The QUIC channel's ergonomic constructor (`QuicChannel::connect`)
  installs a no-op `ServerCertVerifier`. The user must
  explicitly opt OUT of skip-verification by using
  `connect_with_config` to get authenticated TLS.

Recommended deployment until these are remediated:

- Replicate only across a host-firewalled or
  VPC-segmented network where every peer IP is statically
  known and the firewall blocks all other inbound traffic.
- Do not expose the replication port on any
  internet-reachable interface.
- Treat the replication network as trusted: if a peer is
  compromised, the entire replication group is
  compromised.

## Other limitations

| Limitation | Status | Workaround |
|-----------|--------|------------|
| **Concurrent write throughput gap** | Known — Noxu LockManager uses 64 shards; alternative implementations using per-slot lock designs scale better at 16+ concurrent writers | Keep writer concurrency ≤ 8 threads per environment for optimal throughput; use disjoint key ranges when possible |
| **TiB-scale validation not automated** | `examples/scale_validation.rs` is a manual pre-production check; not run in CI | Run manually: `cargo run --example scale_validation -- --records 10000000 --threads 8` |
| **Sustained slow-test suite not in default CI** | P4/P5 tests marked `#[ignore]` to avoid CI timeouts | Run explicitly: `cargo nextest run -p noxu-db --profile slow --run-ignored all` |
| **`TupleSerdeBinding` sort order** | Uses `serde` binary encoding, not sort-preserving tuple encoding | Use raw `DatabaseEntry` with manually constructed sort-preserving keys for range scans on tuples |
| **Property-based tests timeout in fast nextest runs** | `noxu-db::prop_tests` and `noxu-collections::prop_tests` may timeout under default 60 s limit | Run with `--profile slow` or increase timeout in `.config/nextest.toml` |
| **Replication: server-side network restore** | TCP file transfer implemented; client-side `NetworkRestore::execute()` complete | Full production hardening of restore protocol is recommended before use in HA deployments |
| **`Engine::close` does not close `EnvironmentImpl`** | Identified by May-2026 claim audit. The doc lists "3. Close EnvironmentImpl" but the body skips that step with an inline TODO comment. | Drop the `Engine` and rely on the `Environment`'s own RAII close; or call `env.close()` directly. |
| **`verify_environment` / `verify_database` are stubs** | Identified by May-2026 claim audit. Both functions return an empty passing `VerifyResult{}` without performing verification work. | Treat their `Ok` result as "no errors detected by this stub" — not as a guarantee of consistency. |
| **`ReplicatedEnvironment::new` does not start the replication group** | Identified by May-2026 claim audit. The doc claims `new()` initiates an election and contacts peers; the body only constructs state and starts the TCP service dispatcher. | The replication subsystem requires further wiring (election trigger, peer contact) before it provides the documented behaviour. |
| **`become_master` active feeder threads + WAL-scanner auto-feed** | **Implemented** (v3.2.0 push-feeder threads, v4.0.0 WAL-scanner auto-feed, C-C2+C-C2b). `become_master` spawns `FeederRunner` threads per registered channel. When `with_environment(env_impl)` has been called the `FeederRunner` uses `EnvironmentLogScanner` to auto-discover VLSN-tagged WAL entries written by real `EnvironmentImpl` commits — no `replicate_entry` call needed. Without a wired env the fallback in-memory queue (populated by `replicate_entry`) still works. Convergence tested end-to-end with real `EnvironmentImpl` commits. | Call `with_environment(env_impl)` then `register_feeder_channel` then `become_master`. The WAL-scanner auto-feed path is active; `replicate_entry` is still supported for backwards compatibility and for callers that do not use `EnvironmentImpl`. |
| **`transfer_master`** | Substantially implemented (v2.0+): sends network messages and demotes self to replica. Some edge cases (election quorum enforcement during transfer) are not covered. | Use `transfer_master` for graceful topology changes; monitor via `StateChangeListener` for the role transition. |
| **`shutdown_group` catch-up wait** | **Implemented** (v3.2.0, M-4): when `FeederRunner` threads are active, `shutdown_group` waits up to half the timeout for each replica to ack the master's VLSN before sending `SHUTDOWN_GROUP`. Pull-path replicas (no registered channel) receive `SHUTDOWN_GROUP` without a VLSN-level wait. | Use `shutdown_group` with a generous timeout (≥ 10 s) to give replicas time to catch up. |
| **No built-in metrics export** | `env.get_stats()` returns a snapshot; there is no Prometheus/OpenTelemetry integration | Wrap `get_stats()` in your own scraper thread |
| **`JoinCursor` over sorted-dup secondaries** | `test_join_intersection_finds_single_match` is `#[ignore]`; `JoinCursor` requires sorted-dup secondary indexes which are a v1.6 feature (Decision 1B). | Planned for a dedicated follow-up wave. Use multiple single-key cursors and intersect results in application code. |
| **`Get::SearchLte`, `Get::FirstDup`, `Get::LastDup`** | These `Get` enum variants return `NoxuError::Unsupported` at runtime (Wave 11-R audit finding 3-D). | Use `Get::SearchBothRange` + manual iteration for LTE, or position with `Get::Search` and step backward. |
| **`Environment::get_lock_stats()` / `get_transaction_stats()`** | JE exposes separate lock-table and transaction-subsystem stat surfaces. Noxu has only `get_stats()` (Wave 11-R audit finding 3-C). | Use `get_stats()` for aggregate numbers; per-lock-table granularity is not available. |
| **`LogFlushTask` — no public type** | Background log-flush daemon (`LOG_FLUSH_NO_SYNC_INTERVAL`) runs internally but is not exposed as a public API (Wave 11-R audit finding 3-F). | Set `with_log_group_commit` + `with_durability(CommitNoSync)` and rely on the daemon; no manual flush handle is available. |
| **`env_check_leaks` (reserved, v3.1)** | Stored but never read; lock-leak detection at database close is not implemented. Setting to `false` emits a `WARN` log. | No action needed for correctness; lock leaks are not currently detected. |
| **`env_forced_yield` (reserved, v3.1)** | Stored but never read; yield-point injection in critical sections is not implemented. Setting to `true` emits a `WARN` log. | Has no effect; intended for testing fairness in a future release. |
| **`env_fair_latches` (reserved, v3.1)** | Stored but never read; FIFO-ordered latch construction is not implemented. Setting to `true` emits a `WARN` log. | Has no effect; latches do not guarantee FIFO order. |
| **`env_latch_timeout_ms` (reserved, v3.1)** | Stored but never read by the latch layer. Setting to a value other than 300,000 emits a `WARN` log. | Has no effect; latches block indefinitely. |
| **`env_ttl_clock_tolerance_ms` (reserved, v3.1)** | Stored but never read; TTL expiration is not implemented. Setting to non-zero emits a `WARN` log. | Has no effect; records are never expired by the engine. |
| **`env_expiration_enabled` (reserved, v3.1)** | Stored but never read; TTL-based record expiration is not implemented. Setting to `true` emits a `WARN` log. | Has no effect; records are never expired by the engine. |
| **Per-record TTL is hours-granularity only** | `WriteOptions::with_ttl(hours)` and `with_expiration(hours)` store expiration as hours since the Unix epoch. Seconds-granularity TTL is **not supported**: `ttl_secs_to_expiration` exists as a utility function but the engine's read path (`is_expired`) consults `expiration_in_hours = true` on every BIN, so passing a seconds-based expiry would be compared against the wrong clock and expire records immediately or never expire them. Use only the hours-based API. (St-H6, fixed: prior versions had a BIN-split bug where right-half records were silently expired — this is now corrected.) | Use only `WriteOptions::with_ttl(hours)` / `with_expiration(hours)`. |
| **`env_db_eviction` (reserved, v3.1)** | Stored but never read; per-database node eviction is not implemented. Setting to `true` emits a `WARN` log. | Has no effect; eviction does not differentiate by database. |
| **Chained / replica-to-replica log feeding** | The master is the only ongoing log-feed source. BDB-JE supports a replica feeding another replica (cascading feeders); Noxu does not. (Replica-to-replica *file-level* copy exists via network restore.) | All replicas stream from the master; size the master's outbound capacity accordingly. |
| **Database/transaction triggers** | Not implemented. BDB-JE exposes `DatabaseTrigger` / transaction triggers for change notification; Noxu has no equivalent. | Implement change hooks in application code around `put`/`delete`/`commit`. |
| **Admin tooling (dump / load / print-log)** | No `DbDump`/`DbLoad`/`DbPrintLog`-equivalent CLI utilities. | Use the public API for export/import; there is no offline log inspector. |
| **Code coverage not tracked in CI** | A `make coverage` target (`cargo-llvm-cov`) exists but coverage is not measured or gated in CI; there is no committed coverage baseline. | Run `make coverage` locally to inspect coverage of changed code. |
| **Spec models are protocol models, not conformance proofs** | The `noxu-spec` Stateright specs model-check the *protocol design*'s safety/liveness; they are abstract models kept in sync with the Rust by review convention (two anchor to production types), not a mechanical refinement proof of the implementation. | A green spec means the protocol is safe; rely on the unit/integration suites for implementation conformance. |
| **Cleaner: `FilesToMigrate` (`forceCleanFiles`) not implemented (CLN-8)** | JE's `cleaner.forceCleanFiles` config parameter and `FilesToMigrate` operator feature (force-clean a specific set of files) are not implemented. The third selection tier in `UtilizationCalculator.getBestFile` is absent. | Not needed for correctness; only relevant when an operator needs to compact specific log files. Implement if a migration / compaction use-case requires it: add a `force_clean_files: Vec<u32>` field to `Cleaner` and a third tier in `select_file_for_cleaning_with_policy`. |
| **Cleaner: `UtilizationProfile` not persisted across crashes (CLN-11)** | `UtilizationProfile` is in-memory only. JE persists file summaries to a dedicated `FileSummaryDB` internal BTree database and restores them via `populateCache` at recovery. After a crash, Noxu loses utilization detail and the skip-known-obsolete optimization until the next full scan. **What is needed to implement**: a new internal database (a `noxu_dbi::DatabaseImpl` keyed by file number), integration with `noxu-recovery`'s checkpoint/restore path to write and read back file summaries, and a `populate_cache` call at environment open. This is deferred because it requires significant cross-crate changes to `noxu-dbi` and `noxu-recovery`. | After a crash Noxu will re-scan log files to rebuild utilization data; this is safe but slower. |
| **Cleaner: `ExpirationProfileStore` is in-memory only (CLN-9 partial)** | The per-file `ExpirationProfileStore` (histogram of expiration times per file) is implemented in memory. JE persists this data in `ExpirationProfile.java` backed by the same `FileSummaryDB`. In-memory data is lost on crash; the TTL-adjusted file selection will recompute from scratch. **What is needed to implement persistence**: same as CLN-11 above (a FileSummaryDB). | Records with TTL still expire correctly; only the cleaner's prediction accuracy suffers after a crash until the profile is rebuilt. |
| **Cleaner: `wakeupAfterNoWrites` not wired to Checkpointer daemon (CLN-14 partial)** | `Cleaner::with_checkpoint_wakeup_fn` is implemented and the callback is invoked after each successful cleaning pass. However, noxu-engine (which owns the Checkpointer daemon loop) has not yet been updated to register the callback. Until the engine wires `Checkpointer::wakeup_after_write` into the cleaner, cleaned files may not be promptly deleted when write activity stops. **To complete**: in `noxu-engine`'s environment-init path, register the wakeup callback on the cleaner. | Cleaned files are deleted at the next scheduled checkpoint interval (default 60 s). No data loss risk. |

---

## Quick-reference: `EnvironmentConfig` production defaults

```rust
EnvironmentConfig::new(path)
    .with_allow_create(true)
    .with_transactional(true)
    // Cache: 30% of available RAM, e.g. 8 GiB on a 32 GiB machine
    .with_cache_size(8 * 1024 * 1024 * 1024)
    // Log files: 64 MiB each (larger = less cleaner overhead)
    .with_log_file_max_bytes(64 * 1024 * 1024)
    // Checkpoint every 128 MiB written
    .with_checkpointer_bytes_interval(128 * 1024 * 1024)
    // Start cleaning files that are < 60% live (default 50%)
    .with_cleaner_min_utilization(60)
    // Group commit: batch up to 32 writers, flush every 2 ms
    .with_log_group_commit(32, 2)
    // Lock / txn timeouts to detect deadlocks quickly
    // (set via EnvironmentMutableConfig after open)
```
