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
| **`TupleSerdeBinding` value encoding is serde (not order-preserving)** | `TupleSerdeBinding` encodes KEYS with the sort-preserving tuple encoder (range scans on tuple keys work correctly) and encodes the DATA/value with serde binary. Only the value encoding is non-order-preserving — which is correct, since the B-tree compares keys, not values. (The earlier wording wrongly implied the keys were not sortable.) | None for keys — tuple-keyed range scans are correct. Do not rely on value-byte ordering (values are not a sort dimension). |
| **Property-based tests timeout in fast nextest runs** | `noxu-db::prop_tests` and `noxu-collections::prop_tests` may timeout under default 60 s limit | Run with `--profile slow` or increase timeout in `.config/nextest.toml` |
| **Replication: server-side network restore** | TCP file transfer implemented; client-side `NetworkRestore::execute()` complete | Full production hardening of restore protocol is recommended before use in HA deployments |
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
| **Compressor: BIN slot removal does not consult the lock manager (IC-3)** | `Tree::compress_bin` (noxu-tree) removes `known_deleted` slots from a BIN without checking whether the slot is write-locked by an in-flight transaction. JE's `BIN.compress` consults the cursor/lock state. The lock manager lives in a **different crate** (`noxu-txn`) that `noxu-tree` does not depend on, so a cross-crate write-lock check is out of scope for the tree layer. **Why this is currently safe**: the compressor daemon (`environment_impl.rs`: `collect_bins_with_known_deleted` → `compress_bin`) only ever sees *committed* defunct slots. The dbi write path (`cursor_impl.rs::delete`) physically removes a slot via `tree.delete()` while holding the txn write lock — it never leaves a write-locked `known_deleted` tombstone in a `BinStub`. The only writer of `BinStub.entries[].known_deleted = true` is BIN-delta / recovery replay, which replays only already-committed deletes. So no uncommitted, write-locked slot can reach `compress_bin`. JE ref: `BIN.compress` (BIN.java) and `INCompressor.compress` (INCompressor.java ~line 465-466, 587). **What would be needed if a future write path ever leaves an uncommitted write-locked tombstone in a `BinStub`**: pass a lock-state query callback (or move the defunct-slot scan behind a `noxu-txn`-aware predicate) into `compress_bin` so it can skip slots that are still write-locked, mirroring JE's `BIN.compress` lock check. | No action needed for correctness in the current design. If a new code path is added that tombstones a slot before its deleting transaction commits, it must NOT enqueue that BIN to the compressor until commit. |
| **Some cleaner/evictor tuning config parameters are not yet wired** | A few JE config parameters control features whose underlying model is not fully ported, so the parameters are accepted but ignored: `BIN_DELTA_BLIND_OPS` / `BIN_DELTA_BLIND_PUTS` (blind-put-on-BIN-delta infra exists but production never enables it); `EVICTOR_MUTATE_BINS`, `EVICTOR_FORCED_YIELD`, `CLEANER_RMW_FIX`, `CLEANER_GRADUAL_EXPIRATION` (features not ported); `RESERVED_DISK` (disk-full reservation not implemented). | None needed for correctness — these are tuning knobs. `CLEANER_TWO_PASS_GAP`/`CLEANER_TWO_PASS_THRESHOLD`, `EVICTOR_USE_DIRTY_LRU`, `EVICTOR_EVICT_BYTES`/`EVICTOR_CRITICAL_PERCENTAGE`, and `LOCK_N_LOCK_TABLES` were previously in this set and are now wired. |
| **Disk-limit enforcement (`MAX_DISK` / `FREE_DISK`) is not enforced** | The `noxu.maxDisk` / `noxu.freeDisk` config parameters and the `NoxuError::DiskLimitExceeded` error type / `EnvironmentFailureReason::DiskLimit` are defined, but **no write-path check enforces them** — `DiskLimitExceeded` is never returned in production. JE's `DiskLimit` machinery refuses new writes before the disk fills (so recovery stays possible) and resumes once the cleaner/checkpointer free space. Noxu currently does not refuse writes on a near-full disk. JE ref: `DiskLimits` / `Environment` write-prohibition. | **Monitor free disk space externally** and stop the application before the volume fills; do not rely on `MAX_DISK`/`FREE_DISK` to protect against disk-full. Enforcement is scoped for a future release. |
| **Shared cache across environments (`SHARED_CACHE`) is not implemented** | The `noxu.sharedCache` parameter is accepted but multiple `Environment`s in one process each get their own cache and memory budget; there is no shared evictor/budget balancing across environments. JE's `SharedEvictor` balances one cache budget across all environments that set `setSharedCache(true)`. | Size each environment's `cache_size` independently. Do not rely on `SHARED_CACHE` to bound total multi-environment memory. |
| **Recovery verification: LSN↔utilization-profile overlap check not implemented** | JE's recovery tests run `env.verify()` AND `VerifyUtils.checkLsns()`. Noxu's `Environment::verify` performs the live-tree structural walk (child accessibility, key-range containment, non-deleted-slot LSN validity) and is run after every recovery in the test suite, but the `checkLsns` half — asserting the set of live tree LSNs is disjoint from the obsolete LSNs in the utilization profile — is not implemented (it needs the UtilizationProfile threaded into the verifier across the noxu-engine/noxu-cleaner boundary). A recovery producing a correct tree but a utilization profile that mislabels a live LSN as obsolete would pass Noxu's verify but fail JE's. | The structural tree walk catches tree corruption; cleaner/utilization correctness is exercised separately by the cleaner SR-regression and utilization-profile unit tests. Implement the LSN↔UP overlap check when the verifier is given UP access. |
| **DPL secondary indexes are in-memory and not transactional** | In the `noxu-persist` DPL layer, registered secondary indexes are maintained in memory and their updates are applied immediately on the primary `put` / `delete_with_entity` call — they are NOT rolled back if the surrounding user transaction aborts. The crate emits a one-shot `log::warn!` when a primary write occurs inside a user transaction with registered secondaries. (The lower-level `noxu-db` `SecondaryDatabase` API, by contrast, threads the same transaction through and IS atomic.) | Use the `noxu-db` `SecondaryDatabase` API when secondary-index updates must be atomic with the transaction; or avoid aborting transactions that mutated DPL-indexed entities. Persistent transactional DPL secondaries are scoped for a future release. |
| **Collections iterators are snapshots, not live cursors** | `noxu-collections` `StoredIterator` captures a snapshot of keys at `iter()` time (participating in the caller's transaction), whereas JE's `StoredIterator` holds a live cursor that re-reads as it advances. This is a deliberate design choice (avoids holding a cursor open across the iteration) but means modifications made after `iter()` is called are not observed by an in-flight iteration. | Re-create the iterator to observe concurrent modifications; or use a `noxu-db` `Cursor` directly for live-cursor semantics. |
| **DPL composite primary-key on-disk format changed in v4.x (PERSIST-COMP-1)** | `#[derive(PrimaryKey)]` for a multi-field key struct previously length-prefixed each field (`[4-byte BE len][bytes]`), which sorted keys by field lengths instead of logical tuple order — corrupting ordered iteration / range scans. The encoding is now order-preserving and self-delimiting with no length prefix (fixed-width numerics by width; `String`/`Vec<u8>` as a `0x00`-terminated escaped byte string, mirroring JE `TupleOutput.writeString`). This is a **breaking on-disk change**. | Rebuild (dump + reload) any DPL store whose entities use a multi-field `#[derive(PrimaryKey)]`. Single-field newtype keys are unaffected. No production users on v4.x, so no in-place converter is provided. |
| **Tuple string encoding is not wire-compatible with JE** | `noxu-bind` tuple string encoding uses a double-terminator + escaped UTF-8, whereas JE uses a single `0x00` terminator + modified-UTF-8. Noxu's encoding round-trips correctly within Noxu but is NOT byte-compatible with JE-encoded tuples. (Noxu uses a Rust-native on-disk format and does not target JE interop — see the `.ndb` format decision.) | None needed for Noxu-only use. Do not attempt to read JE-encoded tuple data with Noxu or vice versa. |
| **Replication HA protocol is incomplete (election ranking, syncup, master transfer)** | Some JE replication protocol components are still partial. **Implemented since this row was written**: the DTVLSN substrate and tracking (`get`/`set`/`update_dtvlsn`, master-side `update_dtvlsn_from_feeders`), DTVLSN-based election ranking (`Proposal.dtvlsn` is the major ranking key, wired into production `run_election_with_phi_dtvlsn`), and `is_authoritative_master` partition/quorum detection. **Still partial**: the `CommitFreezeLatch` exists as a primitive but is not yet wired into the replica replay / acceptor paths (VLSN can still advance mid-election); the bilateral syncup matchpoint protocol does not roll a *diverged* replica back to a verified matchpoint (`negotiate_syncup` is a range-availability check only — risk of divergent/lost writes on a diverged tail); and there is no periodic `DTVLSNFlusher` daemon. (`transfer_master` itself does change master — see its own row.) | **Do not rely on automatic failover / master election for correctness in production.** Replication is suitable for read-scaling and warm-standby with operator-supervised failover only. See the replication chapter; full HA election/syncup fidelity is in progress. |

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
