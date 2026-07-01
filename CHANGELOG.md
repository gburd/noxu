# Changelog

All notable changes to Noxu DB are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and Noxu DB adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
starting with v2.0.0.  Pre-v2.0 releases were the audit-driven remediation
phase and contain breaking changes between minor versions; the
[migration guide](docs/src/getting-started/migrating.md) calls out every
breaking change with a code-level recipe.

For dense per-release context (sprint and wave attribution, audit
finding IDs, full test-gate counts), see the annotated git tags
(`git tag -l vX.Y.Z --format='%(contents)'`) and the per-wave reports
listed in [References](#references).
## [Unreleased]

A small, focused cleanup release: it removes the moot config knobs that were
`#[deprecated]` in 7.1 (a breaking public-API removal — treated as a
major-semantics 7.2 release since there are no external users) and truths-up
stale `known-limitations.md` docs that claimed already-done work was deferred.

### Removed

- **BREAKING: moot `EnvironmentConfig` knobs deleted outright (`noxu-db`).**
  The config knobs `#[deprecated]` in 7.1 ("will be removed in 8.0") are now
  removed — fields, `set_*` setters, `with_*` builders, and their `Default`
  values. They were stored-but-never-read (nothing in `DbiEnvConfig` /
  `EnvironmentImpl` consumed them). No deprecated stubs are left (no external
  users → a clean delete is correct). Removed:
  - `adler32_chunk_size` — Noxu uses CRC32 (crc32fast, CLMUL-accelerated) for
    on-disk integrity, never Adler32.
  - The JE-style logging/tracing knobs — `logging_level`,
    `console_logging_level`, `file_logging_level`, `trace_console`, `trace_db`,
    `trace_file`, `trace_level`, `trace_file_count`, `trace_file_limit_bytes`,
    and the per-subsystem `trace_level_lock_manager` / `_recovery` /
    `_evictor` / `_cleaner`. Diagnostics route through the Rust `log` crate /
    `noxu-observe` / `RUST_LOG`.
  - `env_dup_convert_preload_all` — configures the JE 4→5 duplicate-DB
    on-disk conversion, N/A to Noxu's native `.ndb` format.

  Reserved-not-yet-implemented knobs (`env_fair_latches`,
  `env_expiration_enabled`, `env_ttl_clock_tolerance_ms`, `env_db_eviction`,
  `BIN_DELTA_BLIND_*`, ...) are **kept** — they emit a `WARN` and track real
  deferred features.

### Docs

- **Truthed-up `docs/src/operations/known-limitations.md`.** Corrected stale
  rows that claimed completed work was deferred/inert, verified against the
  code:
  - IN cached-node heap-footprint compactions: T-3 (`LsnRep::Compact`) and
    T-2/T-5 (`KeyRep::Compact`) are **implemented** as in-memory cached-node
    compactions (the on-disk `.ndb` format is unaffected — `serialize_full`
    writes full keys via `get_full_key()` and full 8-byte LSNs via
    `as_u64()`); `TREE_COMPACT_MAX_KEY_LENGTH` (default 16) is **wired and
    active**. The only remaining T-3 item is optional variable-width LSN
    packing. Moved out of the "deferred/inert" framing.
  - `EVICTOR_MUTATE_BINS` and `TREE_COMPACT_MAX_KEY_LENGTH` corrected from
    "accepted-but-inert" (EV-11/T-5) to wired-and-active.
  - Removed stale comments claiming non-transactional DB names don't survive
    recovery (`je_recovery_test.rs`, `je_database_test.rs`): they describe a
    bygone v2.2.1 limitation; `recovery_edge_test_non_txnal_db` passes.
  - Updated the DBI-14 knobs row and `reference/configuration.md` to say the
    moot knobs are removed in 7.2 (not "deprecated no-ops / removed in 8.0").

## [7.1.0] - 2026-07-01

### Added

- **`EVICTOR_MUTATE_BINS` LN-stripping gate (`noxu-evictor` + `noxu-dbi` +
  `noxu-db`).** The evictor's PartialEvict LN-stripping path is now gated on
  `EnvironmentConfig::with_evictor_mutate_bins` (`noxu.evictor.mutateBins`,
  default **true** — JE-faithful). With `false` the evictor no longer mutates
  a BIN by stripping its LNs (`strip_lns_from_node` returns `Some(0)`); only
  whole-node eviction / put-back applies. Threaded `EnvironmentConfig` ->
  `DbiEnvConfig` -> `Evictor::with_mutate_bins`. Default `true` is
  byte-identical to prior behaviour. JE ref:
  `EnvironmentParams.EVICTOR_MUTATE_BINS`, `Evictor` `mutateBins`.
- **`dos_producer_queue_timeout_ms` DiskOrderedScan producer timeout
  (`noxu-dbi` + `noxu-db`).** The DiskOrderedScan producer thread now honours
  `EnvironmentConfig::with_dos_producer_queue_timeout_ms` (`noxu.dos.producer
  QueueTimeout`, default 10 s): when a lagging consumer keeps the bounded
  producer queue full past the timeout, the producer fails the scan with an
  `OperationFailed` error instead of blocking forever. Implemented via a
  polling `try_send` offer loop (`offer_with_timeout`) that also observes
  cancellation promptly. Threaded `EnvironmentConfig` -> `DbiEnvConfig` ->
  `EnvironmentImpl::get_dos_producer_queue_timeout_ms` ->
  `DiskOrderedCursorOptions`. Removed from the `unimplemented_params` WARN
  registry. Default 10 s and a draining consumer are byte-identical to prior
  behaviour. JE ref: `DiskOrderedScanner` / `BlockingQueue.offer(item,
  timeout)`, `EnvironmentParams.DOS_PRODUCER_QUEUE_TIMEOUT`.
- **`RESERVED_DISK` disk-space reservation (`noxu-dbi` + `noxu-db`).** Beyond
  `FREE_DISK`, the new `EnvironmentConfig::with_reserved_disk(bytes)`
  (`noxu.reservedDisk`, default 0) reserves N extra bytes: a user write is
  refused with `DiskLimitExceeded` once filesystem free space drops below
  `FREE_DISK + RESERVED_DISK`. Wired into the existing `DiskLimitTracker`
  gate (`crates/noxu-dbi/src/disk_limit.rs`); the reservation is subtracted
  from available free space in the same direction as `FREE_DISK`. Default 0 is
  byte-identical to prior behaviour (no extra reservation). JE ref:
  `EnvironmentParams.RESERVED_DISK`, `Cleaner.recalcLogSizeStats`.

- **Latch fairness knobs `env_latch_timeout_ms` + `env_forced_yield` wired
  (`noxu-latch` + `noxu-dbi`).** Two previously accepted-but-inert JE latch
  knobs are now real features, wired non-breaking:
  - **`env_latch_timeout_ms`** (JE `EnvironmentParams.ENV_LATCH_TIMEOUT`) —
    the exclusive and shared latch acquire paths now fail with a
    `LatchError::Timeout` (surfaced as `NoxuError::LatchTimeout`) if a latch
    cannot be acquired within the configured timeout, turning a latch deadlock
    (previously a hang) into a diagnosable error. `0` = no timeout. The default
    `300_000` (5 min) is the "unset" sentinel that preserves the historical
    latch behaviour byte-for-byte.
  - **`env_forced_yield`** (JE `EnvironmentParams.ENV_FORCED_YIELD`) — a
    test-only fairness-stress knob that injects `std::thread::yield_now()` at
    latch acquire/release points to shake out latch-ordering races; a single
    relaxed atomic load (effectively free) when off, which is the default.

  Both are installed process-globally at `Environment::open` via a new
  `noxu_latch::configure`; an environment that leaves both at their defaults
  sees exactly the pre-7.1 latch behaviour (zero production change). Removed
  both from the `unimplemented_params` WARN registry. **`env_fair_latches`**
  (JE `setFairLatches` / `ENV_FAIR_LATCHES`) remains **reserved and
  deliberately not faked**: Noxu's futex-based `noxu-sync` latches are
  fundamentally non-fair with no FIFO wait queue to toggle, so a faithful
  fair-latch mode is a dedicated latch rewrite tracked separately.

- **CLN-2 / `VerifyUtils.checkLsns()` — LSN↔utilization-profile overlap check
  (`noxu-cleaner` + `noxu-engine` + `noxu-dbi` + `noxu-db`).** `Environment::verify`
  now runs BOTH halves of JE's recovery verification. In addition to the
  existing live-tree structural walk it performs the `checkLsns` overlap check:
  the set of live tree LSNs must be DISJOINT from the obsolete LSNs recorded in
  the `UtilizationTracker`. The engine gathers the live LN LSNs from each live
  (non-known-deleted) BIN slot (`noxu_engine::gather_tree_lsns`, JE `GatherLSNs`
  driven by a `SortedLSNTreeWalker`); the cleaner supplies the obsolete set at
  per-LSN OFFSET granularity by rebuilding `Lsn::new(file_num, offset)` from each
  `TrackedFileSummary`'s obsolete-offset detail (`noxu_cleaner::check_lsns` /
  `obsolete_lsn_set`, JE `UtilizationProfile.getObsoleteDetailPacked` +
  `DbLsn.makeLsn`); `check_lsns_against_tracker` bridges the two and reports any
  live LSN found in the obsolete set as a `DataInconsistency` verify error (JE
  "Obsolete LSN set contains valid LSN" → `LOG_INTEGRITY`
  `EnvironmentFailureException`). `NULL_LSN` is ignored on both sides (JE
  `GatherLSNs.processLSN` skips `DbLsn.NULL_LSN`). The `UtilizationTracker` is
  threaded into the verifier via `EnvironmentImpl::get_utilization_tracker`,
  locked once per `verify()` and held read-only across all databases. This is
  ADDITIVE and non-breaking: `verify` is a diagnostic path and `VerifyResult`
  already carries `errors`. The recovery suites (`recovery_correctness_test`,
  `crash_recovery_test`) now assert LSN↔profile disjointness after every
  recovery, so a recovery producing a correct tree but a utilization profile
  that mislabels a live LSN as obsolete now FAILS verification. Positive and
  negative unit tests (`noxu_cleaner::verify_utils`:
  `test_check_lsns_healthy_passes`, `test_check_lsns_detects_live_in_obsolete`)
  prove the check is not vacuous. JE ref:
  `com.sleepycat.je.util.VerifyUtils.checkLsns` / `verifyUtilizationInfo`.

- **`exception_listener` daemon-error callback (`noxu-config` + `noxu-dbi` +
  `noxu-db`).** A faithful analogue of JE `ExceptionListener`: register a
  callback on `EnvironmentConfig::with_exception_listener`, and when a
  background daemon (checkpointer / cleaner / log-flusher) hits a recoverable
  error — previously silently swallowed — the listener's
  `exception_event(&ExceptionEvent)` fires with the daemon source, the error
  message, and the OS thread name. Wired through a new
  `noxu_config::ExceptionDispatcher` shared into each daemon at spawn and
  installed by `Environment::open` before any daemon does work; a no-op (zero
  cost) when no listener is registered. JE ref:
  `com.sleepycat.je.ExceptionListener`, `EnvironmentImpl` daemon catch blocks.
- **`env_check_leaks` lock-leak detection at close (`noxu-txn` + `noxu-db`).**
  At `Environment::close`, when `env_check_leaks` is `true` (the default),
  Noxu walks the active lock table (new `LockManager::report_leaked_locks`)
  and logs a `warn!` for any lock still held with an owner — an application
  leak (a dropped `Transaction`, a cursor held open). Diagnostic only: it
  reports the leaked `(lsn, owner_locker_ids)`, it does not force-release or
  fail the close. Removed `env_check_leaks` from the `unimplemented_params`
  WARN registry. JE ref: `EnvironmentImpl` leak checking.
- **Stats-file dump (`STATS_FILE_*`, `noxu-db`).** When `stats_collect` is
  enabled, a `noxu-stats-file` background daemon (faithful analogue of JE
  `StatCapture`) samples the same snapshot `Environment::stats()` returns and
  appends a CSV row to a rotating stats file (`noxu.stat.<N>.csv`) in
  `stats_file_directory` (default: env home) every
  `stats_collect_interval_secs`. After `stats_file_row_count` rows it rotates;
  at most `stats_max_files` files are retained (oldest pruned). The CSV is
  self-contained (no external recorder needed). New `noxu_db::stats_file`
  module. JE ref: `EnvironmentParams.STATS_FILE_*`, `StatCapture`.
- **`startup_dump_threshold_ms` startup performance summary (`noxu-db`).**
  When `Environment::open` takes at least the configured threshold (startup is
  dominated by the crash-recovery analysis/redo/undo passes), Noxu now logs a
  `warn!` startup summary with the elapsed open time and a `get_stats()`
  snapshot so operators can see why a slow start happened. Threshold `0` (the
  default) disables it. Removed from the `unimplemented_params` WARN registry.
  JE ref: `EnvironmentParams.STARTUP_DUMP_THRESHOLD`.
- **L-3 debug-build latch-ordering assertion (`noxu-latch`).** A faithful
  analogue of BDB-JE's debug-only latch-ordering enforcement
  (`LatchSupport` / per-thread `LatchTable`). `LatchContext` gains an optional
  ordering `rank`; a per-thread stack of held ranked latches asserts that
  latches are acquired in strictly-increasing rank order, panicking on a
  lock-ordering bug. Like JE's, the check is compiled out entirely in release
  builds (`#[cfg(debug_assertions)]`) — zero release-build cost. Rank `0` (the
  default) opts out, so existing unranked B-tree node latches are unaffected.
  New public `noxu_latch::latch_order` module and `LatchContext::with_rank`.

### Deprecated

- **Moot `EnvironmentConfig` knobs deprecated (7.1, non-breaking).** A set of
  config knobs that configure features Noxu deliberately does not have are now
  `#[deprecated]` on their public setters (they still compile; they will be
  removed in 8.0):
  - `adler32_chunk_size` — Noxu uses CRC32 (crc32fast, CLMUL-accelerated) for
    on-disk integrity, never Adler32 (weak on short messages). This knob
    configures a checksum Noxu does not use.
    See `docs/src/internal/checksum-selection.md`.
  - The JE-style logging/tracing knobs — `logging_level`,
    `console_logging_level`, `file_logging_level`, `trace_console`, `trace_db`,
    `trace_file`, `trace_level`, `trace_file_count`, `trace_file_limit_bytes`,
    and the per-subsystem `trace_level_lock_manager` / `_recovery` / `_evictor`
    / `_cleaner`. Noxu routes ALL diagnostics through the Rust `log` crate /
    `noxu-observe` / `RUST_LOG`; a second logging system would be redundant.
    Configure logging via `RUST_LOG` or the `log` facade.
  - `env_dup_convert_preload_all` — configures the JE 4→5 duplicate-DB on-disk
    conversion, N/A to Noxu's native `.ndb` format (no legacy dup format to
    convert). Marked deprecated-moot in its rustdoc (no setter to attribute).

  These knobs were also removed from the `unimplemented_params` WARN registry:
  a deprecated-moot knob announces itself at compile time via `#[deprecated]`
  rather than pretending to be a real-but-unimplemented parameter that emits a
  runtime `warn!`.

### Added (7.1 cleaner completions)

- **CLN-14: cleaner → checkpointer `wakeupAfterNoWrites` wiring
  (feat(noxu-dbi, noxu-recovery)).** The cleaner's `with_checkpoint_wakeup_fn`
  callback (invoked after each successful cleaning pass) is now wired by the
  engine to a new `Checkpointer::wakeup_after_no_writes`, which notifies the
  checkpointer daemon's sleep condvar (without setting shutdown) so the daemon
  wakes early and re-evaluates `is_runnable` — which already returns `true` via
  `needs_checkpoint_for_cleaned_files()`. Previously the callback existed but
  `noxu-dbi` never registered it, so on an idle environment cleaned files were
  only deleted at the next scheduled checkpointer wakeup interval (default
  60 s). Now they are reclaimed promptly. Faithful to JE
  `FileProcessor.doClean` → `envImpl.getCheckpointer().wakeupAfterNoWrites()`.
  Non-breaking: additive `Checkpointer::wakeup_after_no_writes` and
  `Cleaner::set_checkpoint_wakeup_fn` / `Cleaner::has_checkpoint_wakeup_fn`; no
  API removal, no on-disk format change.

- **CLN-8: force-clean files / `FilesToMigrate` (feat(noxu-cleaner)).** Added a
  force-clean set to the cleaner (JE `Cleaner.forceCleanFiles` /
  `FilesToMigrate`): `Cleaner::set_force_clean_files` / `add_force_clean_file`
  / `clear_force_clean_files` / `get_force_clean_files`, backed by a
  `BTreeSet<u32>` on the `FileSelector`. A new third selection tier in
  `FileSelector::select_file_for_cleaning_with_policy` — the forceCleaning /
  `filesToMigrate` tier of JE `UtilizationCalculator.getBestFile` — prefers a
  **safe-to-clean** file from the set (age-eligible, not in-progress, not
  inside the oldest open transaction's log window per the CLN-4 clamp) over
  the utilization-selected candidate, bypassing the utilization gate and the
  two-pass dry-run, and drains it from the set once selected. An unsafe forced
  file stays in the set and is skipped. Reachable via
  `EnvironmentImpl::get_cleaner()`; a public `noxu-db` / `noxu-admin` control
  path is deferred (smaller diff). Non-breaking: additive methods and an
  additive selection tier; no API removal, no on-disk format change.

### Fixed

- **`FsyncManager` group-commit leader-hand-off lost-wakeup (fix(noxu-log)).**
  The leader designated the next cohort's leader with a bare
  `Condvar::notify_one` (`FSyncGroup::wakeup_one`) that set no state under the
  group mutex. A `notify_one` that landed after the leader captured the cohort
  but before the next waiter reached its `wait` was lost (a notify with no
  waiter is a no-op), orphaning the next leader until `LOG_FSYNC_TIMEOUT`
  (default 500 ms) recovered it via its own timeout fsync. In production this
  was a commit/shutdown *stall* up to the timeout; the commit was never lost
  (the `DurableImpliesLogged` invariant always held), so this is a liveness
  fix, not a durability fix. The fix is the same predicate-before-wait class as
  the DST M2 `DaemonManager` `WakeHandle` pre-check: `wakeup_one` now arms a
  `leader_notified` flag under the group mutex *before* `notify_one`, and
  `wait_for_event` consumes it *before* blocking, so a designation is never
  lost and the hand-off is timeout-independent. The documented "orphaned
  `DoLeaderFsync` cohort" was a consequence of this single lost designation (a
  fresh leader that captures the cohort covers it via `wakeup_all`; only a lost
  `wakeup_one` with no fresh leader stalled the cohort), so this one
  root-cause fix closes both documented symptoms. Durability preserved: all
  `fsync_manager` unit tests (incl. fsync-before-commit + leader-failure-
  fails-all-waiters) and the crash-recovery gates stay green. Default build is
  byte-identical.

### Added (SHARED_CACHE cross-environment cache balancing — 7.1)

- **`SHARED_CACHE` is now wired — cross-environment cache-budget balancing
  (`feat(evictor,dbi,db)`).** The `noxu.sharedCache` parameter
  (`EnvironmentConfig::with_shared_cache(true)`) was previously accepted but
  inert: every `Environment` in a process got its own cache + memory budget.
  Multiple environments opened with `shared_cache = true` now join a
  **process-global shared evictor** — a faithful port of JE
  `com.sleepycat.je.evictor.SharedEvictor` + the shared `MemoryBudget`
  (`EnvironmentConfig.setSharedCache`). All sharing envs share ONE
  `Arc<Evictor>`, ONE memory budget (sized from the **first** joining env's
  `cache_size`, JE-faithful), and ONE global LRU spanning every registered
  env's B-trees; eviction picks victims across **all** sharing envs, so total
  resident memory stays bounded by the ONE shared budget instead of the sum of
  the per-env budgets. Implemented on top of the existing EVICTOR-RECLAIM-1
  multi-tree infrastructure: the shared evictor already walks every tree in a
  shared `db_trees_registry` and enforces one budget via the `Arbiter` reading
  one `cache_usage` counter, so a shared cache is just all sharing envs
  pointing at the same three shared `Arc`s. On `Environment::close`/`Drop` the
  env **deregisters** its trees from the shared LRU **before** they drop (no
  dangling trees / use-after-close), and the shared evictor + its single
  daemon tear down when the last member leaves (resettable, with a
  `SharedEvictorHandle::reset_for_test` hook to bound process-global
  test-isolation leakage). **`shared_cache = false` (the default) is entirely
  unchanged**: a private per-env evictor + arbiter + budget counter + daemon,
  exactly as before — the existing `eviction_pressure_test` and
  `evictor_reclaim_multitree_test` stay green. New process-global singleton
  lives in `crates/noxu-evictor/src/shared.rs` (`SharedEvictorHandle`,
  `SharedCacheParams`). Headline test
  `crates/noxu-db/tests/shared_cache_test.rs` opens two shared-cache envs,
  loads ~2x the ONE budget across both, and proves total resident stays ~=
  one budget (not the sum), the first joiner's budget wins, eviction spans
  both envs, both envs' data re-fetches, and after closing one env the
  survivor keeps reading + writing + evicting. DST shuttle coverage of the
  register/deregister/scan interleavings (no use-after-close, no lost
  deregistration): `crates/noxu-evictor/tests/shuttle_shared_cache.rs`
  (`--cfg noxu_shuttle`, 5000 interleavings each). JE ref:
  `evictor/SharedEvictor.java`, `dbi/MemoryBudget.java` (shared),
  `EnvironmentConfig.setSharedCache`.

### Added (DST wave 2 — shuttle safety oracle + lock_manager coverage)

- **`FsyncManager` shuttle safety oracle is now a green gate** (was
  `#[ignore]`'d in M2 because the hand-off's liveness depended on
  `LOG_FSYNC_TIMEOUT`, which shuttle cannot model). With the lost-wakeup fix
  above the hand-off is timeout-independent, so
  `crates/noxu-log/tests/shuttle_fsync_manager.rs` now runs three oracle tests
  (5000 interleavings each): `fsync_coalescing_and_coverage_hold` (the safety
  oracle — `DurableImpliesLogged`, `FsyncedNeverDecreases`, coalescing
  `1..=N`), `fsync_failure_fails_all_waiters` (a failed leader fsync fails
  every waiter), and `group_commit_wait_holds_under_sim_clock` (drives the
  group-commit timed wait via the `SimClock` `advance_and_fire` from M1.1).
  Routes `FsyncManager`'s `Mutex`/`Condvar` through `noxu_util::dst_sync_pl`;
  default build re-exports the real `noxu-sync` types (zero production change).
  Reverting the lost-wakeup fix makes the oracle deadlock (verified), so the
  gate is not blind.
- **`lock_manager` shuttle coverage.**
  `crates/noxu-txn/tests/shuttle_lock_manager.rs` (gated `--cfg noxu_shuttle`,
  2000 interleavings each) routes the lock_manager's shard-table /
  waiter-graph `Mutex` and per-waiter grant `Condvar` through
  `noxu_util::dst_sync_pl` and exercises: a two-lock deadlock cycle aborts
  exactly one victim and grants the other (no-deadlock-undetected +
  victim-consistency, mapped to `noxu-spec` `lock_manager_deadlock`), and a
  blocked waiter is always granted on release with no lost wakeup
  (`WriteLocksExclusive`). The 50 ms deadlock re-detection slice is driven
  deterministically by a `SimClock` via `advance_and_fire`
  (`LockManager::with_config_clock`, M1.1). Default build re-exports the real
  `noxu-sync` types (zero production change). `log_buffer` shuttle coverage
  remains deferred (its segment latch is a `lock_api::RawMutex`, which shuttle
  0.9 does not expose).

### Added (DST Milestone 1.1 — clock thread-through + parking_lot-over-shuttle)

- **Injectable `Clock` threaded through the remaining control-flow time sites.**
  Extends DST M1 (which added the `Clock` trait + `RealClock`/`SimClock` to
  `noxu-util`) so a `SimClock` can drive *all* timeout-relevant time:
  - `FsyncManager` (`noxu-log`): the group-commit wait (`grpc_interval_ms`) and
    the `LOG_FSYNC_TIMEOUT` recovery now read time through an injectable
    `Clock` instead of `std::time::Instant`. New `FsyncManager::with_clock`
    builder; `new()` still defaults to `RealClock`.
  - `LockManager` (`noxu-txn`): the lock-wait loop's timeout math and 50 ms
    deadlock re-detection slice read time through an injectable `Clock`. New
    `LockManager::with_config_clock` builder; `with_config()` /
    `with_lock_timeout()` / `new()` still default to `RealClock`.
  - `DaemonManager` (`noxu-engine`): documented as intentionally *not* clock-
    threaded — its wakeup interval is a config `Duration` and its shutdown path
    is notify-driven (already proven shuttle-clean in M2), so a `SimClock`
    would add nothing.

  All injection is **additive and non-breaking**: every existing constructor is
  unchanged and keeps defaulting to `RealClock`, so the default build has zero
  production behavior change.
- **`noxu_util::dst_sync_pl`: a parking_lot-over-shuttle wrapper.** Removes the
  M2 blocker that `noxu-sync`-based modules (e.g. `lock_manager`) could not be
  shuttle-swapped because `noxu-sync` is `parking_lot`-shaped while
  `shuttle::sync` is `std::sync`-shaped. The wrapper presents the
  `parking_lot` API (`lock() -> guard`, `wait_for(&mut guard, dur)`):
  - Default build (`#[cfg(not(noxu_shuttle))]`): a transparent re-export of the
    real `noxu-sync` primitives — zero production change; shuttle stays out of
    the default dependency graph.
  - `#[cfg(noxu_shuttle)]`: thin, fully-safe wrappers over `shuttle::sync`
    (an `Option`-backed guard newtype bridges shuttle's by-value `wait`, so
    `noxu-util` keeps `#![forbid(unsafe_code)]`).
  - **Clock-driven timed waits under shuttle.** shuttle 0.9's `wait_timeout`
    never times out; the wrapper's `wait_for` registers a `SimClock` deadline
    and the harness's `advance_and_fire(clock, dur)` advances sim-time and
    notifies due waiters so a timed wait fires *deterministically* when the
    harness advances the clock past the deadline. A shuttle self-test
    (`noxu-util/tests/shuttle_dst_sync_pl.rs`) proves the wrapped `Mutex` is
    schedulable and the clock-driven timeout fires under every interleaving.

### Documentation

- **P2-1 — doc version drift.** Updated the remaining `noxu = "3"` (and
  a few stray `version = "3"` / `version = "6"`) quick-start snippets in
  crate `lib.rs` docs and `docs/src/` to `noxu = "7"`, matching the 7.0
  workspace version. User-copied install snippets now show the correct
  version. (The `docs/src/internal/noxu-umbrella.md` historical record keeps
  its point-in-time `3.0.1` references.)

### Changed

- **P2-2 — removed crate-wide `#![allow(dead_code)]` from public crates.**
  Dropped the blanket `dead_code` allow from `noxu-bind`, `noxu-collections`,
  `noxu-persist`, and `noxu-rep` so genuinely-unused items surface in CI
  (`clippy::type_complexity` / `clippy::too_many_arguments` allows kept).
  Removed the resulting dead items (`ScanShape`, `Phantom` in
  `noxu-collections`; a dead `make_expected` test helper in `noxu-rep`) and
  annotated two API-symmetry wrapper methods (`AnyServiceDispatcher::{is_running,
  addr}`) with a scoped `#[allow(dead_code)]`. `noxu`, `noxu-observe`,
  `noxu-xa`, `noxu-persist-derive` had no crate-wide `dead_code` allow.

- **P2-3 — async usage guide in the user docs.** Added a
  "Using Noxu from Async Code" page to the mdBook getting-started section
  (`docs/src/getting-started/async.md`), mirroring the umbrella crate's
  rustdoc note: Noxu is blocking by design, wrap work in
  `tokio::task::spawn_blocking`, and never hold a `Transaction`/`Cursor`
  across an `.await`.

- **P2-4 — advisory cache-mode knobs documented explicitly.** The
  user-settable `cache_mode` hints (`ReadOptions`, `WriteOptions`,
  `DatabaseConfig`) and `update_ttl` were already `#[deprecated]` as inert in
  7.0; this makes the advisory status explicit in the docs so the knobs don't
  read as silently lying. Added an "Advisory status" note to the `CacheMode`
  rustdoc (it is a live type used by the env-level evictor policy, but the
  per-op / per-DB hints are not honored), tightened the `get_with_options` /
  `put_with_options` doc comments to say "accepted but not yet honored," and
  added an advisory note to the `DatabaseConfig` table in
  `docs/src/reference/configuration.md` with a tracking note.

- **P2-5 — documented the 22-crate-split rationale.** Added a "Why 22 crates
  instead of one crate with features?" section to
  `docs/src/maintainer/crate-guide.md` explaining the layered architecture,
  faithful-to-JE module boundaries, and independent versioning that motivate
  the split, plus the user contract to depend on the `noxu` umbrella (not the
  component crates, whose APIs may change without a major bump). Closes the
  review finding as a documented deliberate decision; no crates were
  restructured.

### Fixed

- **w11_recovery benchmark measurement artifact.** The `w11_recovery`
  workload in `benches/noxu-bench/src/main.rs` timed the re-opened
  environment's teardown (close-time checkpoint, daemon shutdown, final flush)
  along with the actual `Environment::open()` log-replay recovery, inflating
  the number and making JE look ~3.8x faster than a clean recovery
  measurement. The harness now stashes the re-opened handle and drops it
  *after* the timer stops, so w11 measures recovery only. Updated the
  benchmark docs (`docs/src/operations/benchmarks.md`,
  `docs/src/maintainer/benchmarking.md`) to flag the historical number as a
  pre-fix artifact. Benchmark harness only — no engine change.

## [7.0.0] - 2026-07-01

### Changed (BREAKING — 7.0 core API reshape)

- **Idiomatic-Rust public API for `noxu-db`.** The core read/write/cursor
  surface was reshaped so the common path reads as ordinary Rust; the
  historical out-param + `OperationStatus` + `DatabaseEntry`-everywhere shape
  is gone from the point-operation surface. This is a source-breaking change
  for every caller of `Database` / `Cursor` / `SecondaryDatabase`.
  - **Reads return `Result<Option<Bytes>>`** (review P0-3). `Database::get(key)`
    auto-commits; `Database::get_in(&txn, key)` reads under an explicit
    transaction. The lower-level buffer-reuse / partial-read escape hatch is
    `get_into(txn, key, &mut DatabaseEntry) -> Result<bool>`.
    `get_with_options(txn, key, opts) -> Result<Option<Bytes>>` (dropped its
    `&mut out` parameter). `SecondaryDatabase::get` was renamed to
    `get_into(txn, key, &mut p_key, &mut data) -> Result<bool>`.
  - **Writes are named auto-commit vs transactional, not a bare `Option`**
    (review P0-2). `put(key, data) -> Result<()>` / `put_in(&txn, key, data)`;
    `delete(key) -> Result<bool>` / `delete_in(&txn, key)`;
    `put_no_overwrite(key, data) -> Result<bool>` / `put_no_overwrite_in(...)`.
    `put_with_options` / `put_partial` keep an `Option<&Transaction>`.
  - **Cursors borrow their transaction** (review P0-1).
    `open_cursor(config) -> Cursor<'static>` for auto-commit;
    `open_cursor_in(&txn, config) -> Cursor<'txn>`. The borrow checker now
    rejects committing or dropping a transaction while a cursor on it is alive
    — the old "close the cursor before commit" prose invariant is a compile
    error. `Cursor::next`/`prev`/`seek` return `Result<Option<...>>`; the
    lower-level `Cursor::get`/`put`/`delete` keep `OperationStatus`.
  - **Keys and values accept `impl AsRef<[u8]>`** (review P1-3) — `b"k"`,
    `&str`, `Vec<u8>`, `Bytes`, `DatabaseEntry`, etc.; no `DatabaseEntry`
    wrapper required at the call site. `DatabaseEntry` remains for the
    buffer-reuse / partial-read escape hatches. Consequence: the historical
    "None key" (a `DatabaseEntry` with no data set, distinct from an empty
    `b""`) can no longer be expressed, so the write path no longer rejects it
    — an empty key is accepted (the three `*_with_none_key_returns_illegal_
    argument` unit tests were removed; `test_put_with_explicit_empty_key_
    accepted` is the canonical behaviour).
- **Consumer-crate cascade.** `noxu-collections`, `noxu-persist`, `noxu-xa`,
  the `noxu` umbrella, every example (`simple`/quickstart, `getting_started`,
  `binding`, `cursor_scan`, `sequence`, `transactions`, `transaction_config`,
  `secondary`, `xa_distributed`, `scale_validation`, and the `cask`/`cash`/
  `ftdb` example crates) and every benchmark (`api_bench`, the comparison and
  workload benches) were updated to the new signatures. The collections
  (`StoredMap`/`StoredSet`/`StoredList`) and persist (`PrimaryIndex`/
  `SecondaryIndex`/`EntityStore`) public surfaces keep their
  `Option<&Transaction>` parameters and idiomatic return types — only their
  internal wiring onto `noxu-db` changed; the DPL transactional-secondary
  fan-out is preserved. The user guide (getting-started + transactions
  chapters) was updated to demonstrate the new API.

### Changed (BREAKING — 7.0 API cleanups: getters, errors, builders, iterators)

The mechanical P1/P2 cleanups that layer on the core reshape above:

- **C-GETTER naming** (review P1-1). `get_x()` field getters were renamed to
  `x()` across the public surface (`get_` is retained only where a key lookup
  happens, e.g. `Database::get`/`get_in`, cursor `get_next`/`get_first`):
  - `DatabaseEntry`: `get_data` → `data_opt` (the `Option<&[u8]>` accessor;
    `data()` still returns `&[u8]`), `get_size` → `len`, `get_offset` →
    `offset`, `get_partial_offset`/`get_partial_length` →
    `partial_offset`/`partial_length`.
  - `Database`: `get_database_name` → `name`, `get_config` → `config`,
    `get_sorted_duplicates` → `sorted_duplicates`, `get_stats` → `stats`.
  - `SecondaryDatabase`: `get_database_name` → `name`, `get_config` → `config`.
  - `Transaction`: `get_id` → `id`, `get_name` → `name`, `get_state` → `state`,
    `get_durability` → `durability`, `get_lock_timeout` → `lock_timeout`,
    `get_txn_timeout` → `txn_timeout`.
  - `Environment`: `get_database_names` → `database_names`, `get_home` →
    `home`, `get_config` → `config`, `get_mutable_config` → `mutable_config`,
    `get_stats` → `stats`, `get_replica_ack_timeout` → `replica_ack_timeout`.
  - `Cursor`: `get_state` → `state`; `JoinCursor`: `get_database` → `database`,
    `get_config` → `config`; `ScanResult`: `get_include` → `included`,
    `get_stop` → `stops`; `Sequence`: `get_stats` → `stats`; `WriteOptions`:
    `get_expiration_time` → `expiration_time`; `EnvironmentConfig`:
    `get_exception_listener` → `exception_listener`. The redundant
    `DatabaseStats`/`BtreeStats`/`JoinConfig` getters over `pub` fields were
    removed.
- **`NoxuError` error chains** (review P1-2). `NoxuError` and
  `EnvironmentFailureReason` are now `#[non_exhaustive]`. A new
  `NoxuError::OperationFailed { msg, #[source] source }` variant carries the
  originating sub-crate error (log/B-tree/comparator/DBI) so
  `std::error::Error::source()` chains — the previously-lossy
  `From<DbiError>` / `From<TxnError>` / `cursor::map_cursor_err` flattening to
  a string is gone. Display text and retryable/fatal classification are
  unchanged.
- **Internal wiring hidden** (review P1-6). `Transaction::with_log_manager` /
  `with_env_impl` / `with_inner_txn` are now `pub(crate)`; `Transaction::new`
  is `pub(crate)` (and no longer `#[deprecated]`); `Transaction::get_inner_txn`
  is `#[doc(hidden)]`. These exposed engine-internal types
  (`LogManager`/`EnvironmentImpl`/`Txn`) that `noxu-db` does not re-export.
- **Lazy Stored\* iterators** (review P1-7). `StoredMap`/`StoredSortedMap`
  `iter`/`keys`/`values` (and `StoredKeySet`/`StoredValueSet`/`StoredList`
  `iter`, `StoredSortedMap` `iter_from`/`iter_reverse`) are now lazy,
  cursor-backed iterators (`impl Iterator<Item = Result<…>>`) that are O(1) to
  create and do not materialise the whole keyspace. The previous eager
  behaviour is preserved under explicitly-named
  `snapshot()`/`keys_snapshot()`/`values_snapshot()`.
- **Uniform consuming `with_*` builders** (review P1-8). Every non-deprecated
  `EnvironmentConfig` / `DatabaseConfig` parameter now has a consuming `with_*`
  builder (returning `Self`) so the chained-builder form works for every
  parameter, not a hand-picked subset. The `&mut`-style `set_*` setters are
  retained.
- **Inert config knobs deprecated** (review P1-9 / P2-4). The silently-inert
  `DatabaseConfig` setters (`exclusive`, `replicated`, `cache_mode`,
  `bin_delta`, `use_existing_config`) and per-op advisory setters
  (`WriteOptions::with_cache_mode`/`with_update_ttl`/`evict_after_write`,
  `ReadOptions::with_cache_mode`/`evict_after_read`) are now
  `#[deprecated(note = "not yet implemented …")]` so a settable knob no longer
  silently lies. `WriteOptions::with_ttl` is unaffected (TTL is honoured).
  (The reserved `EnvironmentConfig` params already WARN at
  `Environment::open`.)
- **Polish** (review P2-1/P2-2/P2-3). Quick-start dependency doc strings now
  say `noxu = "7"`; the crate-wide `#![allow(dead_code, unused_imports,
  unused_macros)]` was removed from `noxu-db` and the underlying warnings
  fixed; the umbrella crate docs gained a "Using Noxu from async code"
  section (blocking by design; use `spawn_blocking`; do not hold a
  `Transaction` across `.await`).

### Added

- **Deterministic Simulation Testing (DST) Milestone 1 — seed-reproducible
  storage-fault crash gate.** A Noxu-native DST harness (JE has no analogue)
  that makes crash/recovery a pure function of `(seed, workload)`:
  - `noxu-util`: an injectable `Clock` trait (`now_unix_ms` / `now_nanos` /
    `sleep`) with `RealClock` (the production default — delegates to stdlib,
    zero behavior change) and `SimClock` (atomic tick + `advance`, time only
    moves when the harness drives it); a seeded `Prng` (`xorshift64*`) that the
    harness draws every fault decision from; and `ttl::is_expired_with(clock,
    ...)` for clock-aware TTL expiry. DST is strictly opt-in.
  - `noxu-log`: a `faultdisk` fault layer over the positioned-I/O chokepoint
    (`posio`'s four functions) plus the fsync path, injecting per-seed **torn
    writes** (write a prefix then power-cut so the tail + later writes never
    reach disk), **fsync drop** (ack durability without flushing, then
    power-cut), **disk-full** (`ENOSPC`), and **corruption** (bit-rot). Gated
    behind one process-global `AtomicBool` never set by production code —
    inactive = one relaxed atomic load, then the real path.
  - `noxu-db`: `tests/dst_crash_sweep.rs` — a fast subset (~120 seeds, &lt;60s)
    for local dev / PR CI and a `#[ignore]` `long_sweep` (10k seeds) release
    gate, asserting no-lost-committed-txn (strict prefix) + no-uncommitted-leak
    + total-recovery on every seed. The `crash_worker` reads `NOXU_DST_SEED`
    and installs the fault disk; a failing seed reproduces byte-for-byte
    (`NOXU_DST_SEED=<n>` is printed). This closes the in-process
    kernel-buffer-drop power-loss gap the SIGKILL `power_loss_sweep` cannot
    reach. See `docs/src/contributing/testing-guide.md`.

- **Deterministic Simulation Testing (DST) Milestone 2 — shuttle concurrency
  gate.** A [`shuttle`](https://docs.rs/shuttle) concurrency-permutation gate
  that explores thread interleavings of the **real** engine code under a seed
  and shrinks failing schedules — complementing M1 (storage faults) and
  `noxu-spec` (abstract protocol models):
  - `noxu-util`: a cfg-gated `dst_sync` seam that re-exports `std::sync` +
    `std::thread` by default and `shuttle::sync` + `shuttle::thread` under
    `--cfg noxu_shuttle`. shuttle is a `[target.'cfg(noxu_shuttle)']`
    dependency, so it is **not in the default dependency graph** — zero
    production change. Plus `dst_invariants`, the shared DST oracle reusing the
    `noxu-spec` `wal_commit` properties (`LsnMonotone`,
    `FsyncedNeverDecreases`, `DurableImpliesLogged`) as runnable asserts.
  - `noxu-engine`: `tests/shuttle_daemon_shutdown.rs` — a **green** shuttle gate
    (5000 interleavings) proving the `DaemonManager` shutdown/wakeup path is
    deadlock-free (no lost wakeup, no use-after-shutdown, correct join order).
  - `noxu-log`: `tests/shuttle_fsync_manager.rs` — routes the `FsyncManager`
    group-commit protocol through the seam; a passing test proves shuttle
    *detects* the leader hand-off's timeout-masked orphan, with the full safety
    oracle `#[ignore]`d pending a timeout-independent hand-off (shuttle cannot
    model the `LOG_FSYNC_TIMEOUT` recovery). `lock_manager` is not yet covered
    (parking_lot-shaped locks + `Instant`-based deadlock re-detection).
    See `docs/src/contributing/testing-guide.md`.

- **Database / transaction triggers (DB-TRIG).** A new public
  [`Trigger`](noxu_db::Trigger) trait (`crates/noxu-dbi/src/trigger.rs`,
  re-exported from `noxu-db`) is a faithful port of BDB-JE
  `com.sleepycat.je.trigger.Trigger` + `TransactionTrigger`, fired by the
  engine on data changes and transaction resolution. Register one or more on a
  `DatabaseConfig` via `with_trigger(Arc<dyn Trigger>)` / `add_trigger(...)`
  (JE `DatabaseConfig.setTriggers`); multiple triggers fire in **registration
  order**.
  - `put(txn_id, key, old_data, new_data)` fires after a successful put
    within the transaction (`old_data = None` on insert, `Some(prev)` on
    update); `delete(txn_id, key, old_data)` after a successful delete
    (JE `Trigger.put` / `Trigger.delete`, fired by
    `TriggerManager.runPutTriggers` / `runDeleteTriggers` after the actual
    tree mutation). The trigger sees the change **before commit** and can make
    accompanying changes under the same transaction; on abort those changes
    roll back with the transaction.
  - `commit(txn_id)` / `abort(txn_id)` (default no-op, mirroring JE's
    `instanceof TransactionTrigger` check) fire on the transaction's
    resolution, once per modified database, in registration order — JE
    `TriggerManager.runCommitTriggers` / `runAbortTriggers` over
    `Txn.getTriggerDbs()` (the modified-database set populated by
    `noteTriggerDb`).
  - **Persistence / replication adaptation (diverges from JE, by design):**
    JE's `PersistentTrigger` serializes the trigger's *class name* into the
    database record and re-instantiates it by name on open. A Rust closure /
    trait object has no reconstructable name, so — exactly as the DBI-14
    comparator API — Noxu triggers are **runtime-registered only: not
    persisted, not replicated.** Applications must re-register triggers on
    every `DatabaseConfig` open. This matches JE's own current state
    (`Trigger.java`: "Only transient triggers are currently supported";
    triggers "must be configured on each node in a rep group separately").
  - The no-trigger write path pays a single `is_empty()` check
    (JE `DatabaseImpl.hasUserTriggers()`); existing behaviour is unchanged.

- **Admin tooling: `dump` / `load` / `print-log` CLI (`noxu-admin`).** A new
  binary (`crates/noxu-db/src/bin/noxu_admin.rs`, built as `noxu-admin`)
  provides three read-mostly utilities, faithful ports of BDB-JE
  `com.sleepycat.je.util.DbDump` / `DbLoad` / `DbPrintLog` (+ `CmdUtil`).
  **`dump`** opens the environment read-only, walks a database cursor, and
  writes the classic `db_dump` text format (`VERSION=3` header,
  `format=print`/`format=bytevalue`, `type=btree`, `dupsort=0/1`,
  `HEADER=END`, then alternating space-prefixed key/data lines terminated by
  `DATA=END`). Byte encoding is byte-for-byte JE `CmdUtil.formatEntry`:
  printable ASCII (33..126) literal with backslash doubled, non-printable as
  `\HH`, or all-hex in `bytevalue` mode — so the format is **binary-safe**
  (round-trips non-UTF-8 keys/values losslessly). **`load`** is the inverse
  (`DbLoad`): it parses the header and puts each key/data pair in a single
  transaction into the (auto-created) target database; `-n` selects
  no-overwrite mode. **`print-log`** walks the WAL via a read-only
  `FileManager` + `LogFileReader` (no recovery; works on a closed env),
  printing `lsn=… type=… size=…` per entry plus decoded txn id and key/data
  sizes for LN and Txn-end entries, with `-S` for a per-type summary.
  Argument parsing is a small hand-rolled JE-style flag parser (no new
  dependency — the core engine keeps its dependency set minimal).
  Headline test (`crates/noxu-db/tests/admin_cli_test.rs`): `dump | load`
  round-trips an all-256-byte-values record, newline/backslash/NUL bytes, and
  duplicate keys in both `print` and `bytevalue` formats; `print-log` emits
  the TxnCommit + insert-LN entries for known writes. Also adds
  `Database::get_sorted_duplicates()` (reads the opened `DatabaseImpl`'s real
  dup-sort flag, mirroring JE `getConfig().getSortedDuplicates()` after
  `DbInternal.setUseExistingConfig`). **Dup-sort caveat**: Noxu does not
  persist the sorted-duplicates flag across a reopen, so `dump` cannot
  auto-detect it — pass `-D` to dump a duplicates database (symmetric to JE
  `DbLoad -c dupsort=true`). See `docs/src/operations/admin-tooling.md`.

- **Disk-limit enforcement (`MAX_DISK` / `FREE_DISK`).** The `noxu.maxDisk` /
  `noxu.freeDisk` config parameters are now enforced on the user-write path,
  a faithful port of BDB-JE's disk-limit machinery
  (`cleaner/Cleaner.java` `recalcLogSizeStats`/`getDiskLimitViolation`,
  `dbi/EnvironmentImpl.java` `checkDiskLimitViolation`, `Cursor.java`
  `checkUpdatesAllowed`). A new `DiskLimitTracker` (in `noxu-dbi`) caches a
  volatile violation flag computed from total log size (sum of `.ndb` file
  lengths) plus filesystem free space (`fs2::available_space` / statvfs)
  against `MAX_DISK` (absolute log-size cap) and `FREE_DISK` (keep-this-much
  -free reserve): `availBytes = (maxDisk>0) ? min(diskFree-freeDisk,
  maxDisk-totalLog) : diskFree-freeDisk`; a write is prohibited when
  `availBytes <= 0`. `Cursor::put`/`delete` read the cached flag with a single
  atomic load (no per-write statvfs) and return `NoxuError::DiskLimitExceeded`
  BEFORE logging or mutating the tree. **Internal databases are exempt** (JE
  `dbImpl.getDbType().isInternal()`) so the cleaner/checkpointer/recovery
  writes — which free space — are never blocked and the env never deadlocks at
  the limit. The flag is refreshed periodically by the checkpointer daemon and
  after every cleaner pass (JE `Cleaner.manageDiskUsage`), and at env-open;
  once space is reclaimed writes resume automatically. New builders
  `EnvironmentConfig::with_max_disk` / `with_free_disk` and
  `Environment::refresh_disk_limit()`. **Default behaviour is unchanged**:
  `MAX_DISK` defaults to 0 (disabled); `FREE_DISK` defaults to 5 GiB (JE
  default) and only trips below 5 GiB free; when both are 0 the tracker is
  inert (the check is one branch, no statvfs). Headline test:
  `crates/noxu-db/tests/disk_limit_test.rs`.

- **Cursor `Get::SearchLte`, `Get::FirstDup`, `Get::LastDup`.** The three
  remaining cursor positioning modes are now implemented, faithful to
  BDB-JE / BDB `Cursor` semantics:
  - `Get::SearchLte` (floor): positions on the largest key `<=` the search
    key, composed from the BDB `DB_SET_RANGE`-then-step-back floor lookup
    (the LTE mirror of `Cursor.getSearchKeyRange` / `Get.SEARCH_GTE`).
    Returns `NotFound` only when no key `<=` the search key exists. On
    sorted-dup DBs it lands on the last duplicate of the floor key.
  - `Get::FirstDup` / `Get::LastDup`: position WITHIN the current duplicate
    set on the first/last duplicate by data order
    (`Cursor.getFirstDup` / `Cursor.getLastDup`), over Noxu's composite
    two-part-key dup model.
  Pre-fix these returned `NoxuError::Unsupported`.
- **`JoinCursor` over sorted-dup secondaries.** The natural-join cursor now
  works over sorted-dup secondary indexes with multiple primary keys per
  secondary key (the common case): cursor[0] walks its duplicate set for
  candidate primary keys and cursors[1..] probe each candidate with an
  exact `(secKey, primaryKey)` `SearchMode::BOTH` lookup, returning the
  intersection of primary keys present under all secondary keys
  (faithful to JE `JoinCursor.retrieveNext`). The join algorithm was
  already a faithful port; this lands the headline multi-primary
  intersection test and removes the stale gating.

- **Built-in metrics export (`observability` feature).** Noxu can now publish
  its statistics continuously to the [`metrics`](https://docs.rs/metrics)
  facade, the Rust-ecosystem analogue of BDB-JE's read-only JMX MBean export.
  - `noxu_observe::export::{describe_export_metrics, emit}` map each
    operationally relevant field of `EnvironmentStats` onto a recorder-agnostic
    gauge/counter, citing the JE `StatGroup` it derives from (`EVICTOR_*`,
    `LOGMGR_*`/`FILEMGR_*`/`FSYNCMGR_*`, `LOCK_*`, `Txn`, `Cleaner`,
    `Checkpointer`, `THROUGHPUT_PRI_*`).
  - `noxu_db::metrics_export::MetricsExporter` spawns a daemon that samples
    `Environment::get_stats()` on an interval and emits to the facade — so any
    installed recorder (Prometheus, StatsD, OpenTelemetry, …) collects the full
    stat set `get_stats()` exposes, with no hot-path changes.
  - Optional `noxu_observe::prometheus::install()` convenience (behind the
    `prometheus` feature, `metrics-exporter-prometheus` with default features
    off) returns a handle that renders the text exposition for a `/metrics`
    scrape endpoint.
  - **Default-off / zero-cost.** With `observability` disabled, `cargo build`
    pulls no `metrics`/`tracing`/`prometheus`/`noxu-observe` crates (verified by
    `cargo tree`) and the `observe_*` macros compile to nothing.
  - `Environment::get_stats().cache_usage` is now wired to the live tree-memory
    counter (was previously hardcoded to 0).
  - Note: the exported `noxu_db_pri_*` throughput metrics are surfaced but read
    0 — the engine's per-database `ThroughputStats` counters are defined yet
    never incremented (a pre-existing gap, documented in
    `docs/src/operations/monitoring.md`).

- **Chained / replica-to-replica log feeding (`cascade_feeding`).** A replica
  can now feed a downstream replica (master → R1 → R2), using the IDENTICAL
  feeder mechanism the master uses: `FeederRunner` (JE `Feeder`) reading the
  VLSN-tagged stream from the node's own WAL via `EnvironmentLogScanner`
  (JE `MasterFeederSource`/`FeederReader` over the VLSN index + WAL), served
  through `PeerFeederService` (JE `FeederManager`). A replica persists+flushes
  each received VLSN-tagged entry to its own WAL so its file length advances and
  its downstream feeder can serve it. Gated by `cascade_feeding` (default off =
  master-direct, unchanged). The in-memory `replicate_entry` queue is retained
  only as the env-less convenience source (tests / non-`EnvironmentImpl`
  callers); it is NOT a second feeder mechanism and never on a production
  durability path — it feeds through the same `FeederRunner` loop. A
  `wal_feeds_served` counter proves the cascade rides the WAL feeder path.
  Ack/durability bound: the master remains the ack/quorum authority; a
  downstream replica's ack is seen by its immediate upstream but not propagated
  transitively to the master's commit quorum (documented). Faithful to JE
  `Feeder`/`FeederManager`/`FeederSource`/`MasterFeederSource`/`FeederReader`.

- **DPL secondary indexes are now transactional and persistent (correctness
  fix).** `noxu-persist` secondary indexes were a process-local in-memory map
  updated eagerly on the primary `put`/`delete`, OUTSIDE transaction control —
  so an aborted txn left the secondary pointing at rolled-back state (a real
  correctness hole). They are now real `noxu_db::SecondaryDatabase`s maintained
  within the same transaction as the primary write (the fan-out fires under the
  user txn), so they commit/abort atomically with the primary and persist
  across restart (no longer rebuilt from an in-memory side map). The side map,
  the maintainer list, the one-shot `log::warn!`, and
  `PersistError::SecondariesNotTransactional` are removed (net −208 lines).
  BREAKING DPL API: `EntityStore::open_secondary_index(&mut primary, name,
  serializer, extractor)` (env-aware, needs a DB name) and the
  `#[derive(SecondaryKey)]` `open_<name>_index(&mut store, &mut primary, ...)`
  signature; a secondary key type must impl `PrimaryKey` (byte encoding).
  Faithful to JE `Store.openSecondaryDatabase` / `PersistKeyCreator`.

### Fixed

- **`DaemonManager` shutdown lost-wakeup (surfaced by the DST M2 shuttle
  gate).** `WakeHandle::wait_timeout` blocked on its condvar without first
  checking the already-set notify flag. A `notify()` (from `shutdown()`) that
  landed between a daemon's loop iteration and its next `wait_timeout` was lost
  (a condvar notify with no waiter is a no-op), so the daemon slept for the
  full wakeup interval before observing shutdown — a shutdown *stall* up to the
  configured interval (default 5 s). Fixed with a predicate-before-wait guard;
  shutdown now wakes daemons promptly regardless of the notify/wait race.

## [6.4.2] - 2026-06-29

### Fixed

- **Compressor now consults the lock manager before removing a `known_deleted`
  slot (IC-3, defended).** `Tree::compress_bin` previously removed every
  `known_deleted` slot from a BIN without checking whether the slot was still
  write-locked by an in-flight transaction — safe today only by an
  undocumented-in-code invariant ("no write path ever leaves an uncommitted,
  write-locked tombstone in a `BinStub`"), a latent landmine that a future write
  path could trip into tree corruption (the compressor physically removing a
  slot a live txn still references). The new
  `Tree::compress_bin_with_lock_check(bin, is_locked: Option<&dyn Fn(u64)->bool>)`
  takes a caller-supplied lock-state predicate and SKIPS any `known_deleted`
  slot the predicate reports as locked, mirroring JE `BIN.compress`
  (`BIN.java:1141-1172`), which calls `lockManager.isLockUncontended(lsn)` and
  does `anyLocked = true; continue;` on a contended slot. The dbi layer
  (`environment_impl.rs`: the INCompressor daemon and `compress_all`) supplies
  `move |lsn| lock_manager.get_lock_info(lsn) != (0, 0)` — the inverse of JE's
  `isLockUncontended` (`nWaiters == 0 && nOwners == 0`). `noxu-tree` gains **no**
  `noxu-txn` dependency: the predicate is a `dyn Fn`, the lock knowledge lives
  in the closure. A `NULL_LSN` slot is discarded without consulting the
  predicate (JE: "Can discard a NULL_LSN entry without locking"). When no
  predicate is supplied (recovery, BIN-delta replay, lock-manager-less tests)
  behavior is unchanged — all `known_deleted` slots are removed. **Lock
  ordering:** the predicate runs while `compress_bin` holds the BIN write
  latch; `get_lock_info` takes a lock-table shard mutex for one short,
  non-blocking critical section and releases it before returning, and the
  LockManager never latches a BIN, so the only edge is BIN-latch ->
  shard-mutex (acyclic) — no deadlock. Headline test
  (`test_ic3_compress_skips_write_locked_slot`): a write-locked tombstone is
  KEPT while a committed/unlocked tombstone in the same BIN is removed;
  end-to-end (`ic3_compress_predicate_consults_real_lock_manager`): the
  predicate the compressor builds consults the env's real `LockManager`.

- **Critical: adjacent-key transactions could abort the host process
  (dynomite/dyniak report).** Two compounding defects turned a transaction that
  touches adjacent keys into a hard `process::abort()`:
  1. **Illegal `RangeInsert -> Write` / `RangeInsert -> Read` lock upgrade.** A
     new-key insert takes a `RangeInsert` next-key lock on its successor's real
     LSN (phantom prevention). When the SAME transaction then writes or reads
     that successor (an existing key locked by its real LSN), it requested a
     `Write`/`Read` on the LSN it already held as `RangeInsert` — ILLEGAL in JE's
     upgrade matrix, which formerly `panic!`ed. (JE never reaches this: its
     next-key lock and the later access resolve to one uniform LSN locus and
     the inserter never accesses the successor it locked; Noxu's split lock
     locus — synthetic id for new keys, real LSN for existing keys — can reach
     it. See design-decisions "Lock *locus*" / TXN-LOCUS.) Fixed at the source:
     the write path releases the txn's own `RangeInsert` before requesting
     `Write` (`Txn::release_range_insert_for_write`); the read path skips the
     `Read`/`RangeRead` when the txn already holds `RangeInsert`
     (`Txn::holds_range_insert`). The lock matrix is unchanged (verified
     identical to JE `LockType.upgradeMatrix`). A defensive audit of all eight
     cursor lock-acquisition sites confirmed the remaining seam directions are
     covered by the existing `owns_any_lock` guards.
  2. **Panic-in-`Drop` escalated to a process abort.** The panic in (1)
     poisoned the transaction lock; `Transaction::Drop` → `abort()` and
     `EnvironmentImpl::Drop` then did `lock().unwrap()` on the poisoned mutex,
     double-panicking inside a destructor — which Rust escalates to
     `process::abort()`. All lock acquisitions on the abort/Drop paths now
     recover the guard via `unwrap_or_else(|p| p.into_inner())` for a
     best-effort cleanup instead of crashing the process.
  Also: the illegal upgrade itself now returns `TxnError::IllegalUpgrade` ->
  `NoxuError::TransactionAborted` (defense in depth) rather than `panic!`,
  faithful to JE treating the equivalent as a catchable
  `EnvironmentFailureException(UNEXPECTED_STATE)` rather than a JVM abort.
  Regression tests in `range_insert_upgrade_test` (verified failing pre-fix,
  passing post-fix); serializable phantom protection preserved.

- **Survivable-panics audit: WAL buffer-pool exhaustion no longer aborts the
  process.** `LogBufferPool::bump_and_write_dirty` previously called
  `panic!("No free log buffers after flushing dirty buffers")` on an internal
  "should not happen" state, crashing the whole process from a function that
  already returns `Result`. It now returns `LogError::Internal`, faithful to JE
  `LogBufferPool.bumpAndWriteDirty` (LogBufferPool.java:363), which throws
  `EnvironmentFailureException.unexpectedState` rather than aborting the JVM.
  (The full audit of 33 Drop impls + all production `panic!`/`unreachable!` +
  the decode/network/recovery surface found the codebase otherwise already
  panic-safe.)

## [6.4.1] - 2026-06-25

### Performance

- **Read fast-path: uncontended auto-commit / read-committed reads skip the
  per-read lock acquire+release round-trip.** A read formerly acquired a `Read`
  lock and released it immediately (two shard-mutex round-trips) solely to
  detect a concurrent writer. `LockManager::probe_read_uncontended` now confirms
  "no foreign write owner, no waiters" with a single shard access and skips the
  registration when the slot is unlocked (the common case) — behaviour-identical
  since these isolation levels release immediately anyway; a write owner or
  waiter falls back to the full path. Measured: single-threaded reads +29-86%,
  concurrent read throughput improved.
- **Thread-id hash cached in a thread-local.** `noxu_sync::thread_id()` built a
  fresh `DefaultHasher` and hashed the thread id on every mutex/rwlock
  lock+unlock across the whole engine (~2.3% of write-path CPU); now computed
  once per thread.
- **Interruptible daemon shutdown.** The in-compressor, cleaner, and log-flush
  daemons polled their shutdown flag in 100 ms `thread::sleep` chunks, adding up
  to ~200 ms of latency to `close()` / `drop()`. They now use a condvar-based
  `DaemonSignal` so `shutdown()` wakes them immediately (mirrors the
  checkpointer). Measured: env re-open 208 ms → 7.4 ms (the W11 recovery
  benchmark was measuring teardown stall, not recovery — actual recovery was
  always fast and scales cleanly with replay size).

### Fixed

- **WAL group-commit fsync coalescing now matches JE `FSyncManager.flushAndSync`
  ordering (perf/group-commit-coalesce).** `LogManager::flush_sync` previously
  drained the shared log buffer (`fill_flush_pending`, advancing the buffer
  watermark) and `pwrite`-ed the captured ranges BEFORE entering the
  fsync-manager leader/waiter decision. A concurrent committer that did not skip
  at `flush_sync_if_needed`'s fast path would find an empty pending buffer (a
  prior leader already drained it) yet still enter the fsync manager and —
  because the prior leader's fsync window opened late — slip in between that
  leader's `pwrite` and its `fdatasync`, becoming its own leader for a
  *redundant* fsync. Noxu issued ~1.7-2.5× more `fdatasync` calls than JE under
  concurrent commits as a result. The fix restructures the path to match JE
  `flushAndSync` exactly: the leader/waiter decision
  (`FsyncManager::flush_and_sync`, JE `mgrMutex`) is made FIRST, and ONLY the
  leader (or a timed-out thread) performs the drain + `pwrite` (JE
  `flushBeforeSync`) followed by the single `fdatasync` (JE `executeFSync`).
  Waiters piggyback and do no I/O; on wake they return the leader's durable
  result LSN so a subsequent `flush_sync_if_needed` still observes
  `last_synced_lsn >= its lsn`. Durability is preserved exactly: a thread
  arriving as a waiter after the leader started joins the FRESH next-waiters
  group (never `wakeup_all`-ed by the current leader), so it becomes the next
  leader and drains + fsyncs its own bytes — it can never piggyback on an fsync
  that did not cover its writes. The "release LWL before I/O" invariant is kept
  (the leader drains under the LWL briefly, then `pwrite` + `fdatasync` outside
  it). An `fdatasync` failure still sets `io_invalid` and propagates the error
  to every piggybacking committer. Measured fsyncs-per-commit on /scratch
  (btrfs-on-dm-crypt, CommitSync): 8 threads × 500 commits 0.42 → 0.31
  (~26% fewer fsyncs); 16 threads × 500 commits 0.26 → 0.18 (~31% fewer), with
  lower wall-clock too. New coverage: `crash_worker` mode
  `concurrent_commit_sync` + `crash_recovery_test::test_concurrent_commit_sync_survives_sigkill`
  (N-concurrent CommitSync → SIGKILL → recover, all committed txns present),
  `fsync_manager::test_leader_fsync_failure_fails_all_piggybacking_waiters`
  (a failed leader fsync fails every coalesced waiter), and the
  `group_commit_coalesce_bench` real-disk benchmark (fsyncs-per-commit gate).

### Added

- **`EVICTOR_ALGORITHM` config parameter (`noxu.evictor.algorithm`).** The cache
  eviction policy is now selectable per-environment
  (`"lru"|"clock"|"arc"|"car"|"lirs"`, default `"lru"`), wired from
  `EnvironmentConfig` → `DbiEnvConfig` → `Evictor::with_algorithm` for both the
  primary and scan policy slots (previously env-open hardcoded LRU). Parsed via
  `EvictionAlgorithm::from_name`. New accessors `Environment::evictor_algorithm_name`
  (verify the selected policy at runtime) and `Environment::cache_usage_bytes`
  (the live arbiter-tracked budget; `get_stats().cache_usage` remains a
  placeholder). The default stays LRU — JE-faithful.
- **Eviction-policy cache-pressure benchmark**
  (`benches/noxu-bench/src/bin/evictor_policy_bench.rs`): random / scan / mixed
  workloads over a working set larger than the cache, all 5 policies, median of
  3, on real disk. Results in `benches/results/evictor-policy-pressure.md`.

### Fixed

- **Eviction now reclaims to budget across all database trees
  (EVICTOR-RECLAIM-1, fixed).** Under sustained cache pressure the evictor
  previously reclaimed almost nothing — resident `cache_usage_bytes()` stayed
  ~1.45× the configured budget (measured: 16 MiB cache, ~21 MB working set,
  ~23 MB resident; `stripped~1`, `freed~0`). Two distinct defects combined:
  1. **Split-created BINs/INs were never registered with the evictor LRU.**
     Only the first-key root+BIN and re-fetched nodes ever called
     `note_added`; the proactive-split path (`split_child` / `splitRoot`)
     created new siblings/roots without it, so after a tree grew past its
     first BIN every subsequent BIN was invisible to the evictor. The policy
     lists held ~2 node_ids for a 158-BIN tree, so `evict_batch` had almost no
     candidates. JE `IN.splitInternal` calls `inList.add(newSibling)`; the
     `InListListener` is now threaded through `insert_recursive` /
     `split_child` so a freshly-split node is registered the instant it
     becomes resident.
  2. **The evictor searched only a single primary tree slot.** Its
     `strip_lns_from_node` / `flush_dirty_node_to_log` / `evict_root` / the
     `do_evict` detach closure looked up candidates in one tree, so a second
     database's BINs (`db_id` ≠ the primary slot) were targeted via the
     `InListListener` but could never be found/stripped. JE walks ONE env-wide
     `INList` covering all DBs and resolves each target IN's owning DB via
     `target.getDatabase()` (`Evictor.processTarget`, Evictor.java:2374); the
     evictor now consults the shared `db_trees_registry` (the same registry
     the checkpointer and cleaner use) to find the owning tree, and operates
     on it (correct `db_id` for logging; detach re-wires the parent in the
     same tree). Lock-ordering safe: `candidate_trees()` snapshots the
     registry and releases its mutex before any per-tree lock is taken.

  Measured after the fix (16 MiB cache, ~21 MB working set across two user
  DBs, `/scratch` real disk): `stripped 790`, `freed ~16 MB`, resident
  ~0.53× budget, all records re-fetch correctly. Headline regression guard:
  `crates/noxu-db/tests/evictor_reclaim_multitree_test.rs`. **Default eviction
  policy stays LRU** (JE-faithful). See
  `docs/src/operations/known-limitations.md`.


## [6.4.0] - 2026-06-24

### Added (JE-fidelity backlog completion — REP / tree / cleaner / evictor / dbi)

This cycle closes the remaining deferred JE-fidelity findings from the
2026-06-19 census. Each is a faithful transliteration with the JE source method
cited at the implementation site; see the per-merge commit messages and the
annotated history for full test-gate counts.

- **REP-1 STEP 5 — live networked diverged-tail syncup driver.** A diverged
  replica now auto-reconciles via live syncup rollback instead of a full
  network restore. `ReplicaSyncupReader` (backward log walk yielding per-VLSN
  LSN + fingerprint + numPassedCommits), the `EntryRequest` /
  `EntryNotFound` / `AlternateMatchpoint` matchpoint-negotiation wire protocol,
  and a live `Replay.rollback` that truncates the replica's log + tree to the
  agreed matchpoint (reusing the STEPS 1-4 durable machinery), then resumes
  streaming. A divergent tail (`RollbackToMatchpoint`) runs the live rollback;
  `NetworkRestore` (no common matchpoint) and `HardRecovery` (commit crossed)
  stay the JE-faithful fallbacks. JE `ReplicaFeederSyncup` / `ReplicaSyncupReader`
  / `BaseProtocol` / `Replay.rollback`.
- **REP-7 — live read replicas.** A streaming replica applies the master's
  committed operations to its live in-memory B-tree as they stream
  (`noxu_dbi::ReplicaReplay`), so it serves fresh reads without a restart
  (no longer warm-standby-only). Tree mutation goes through one shared
  `noxu_recovery::apply_redo_ln` that crash-recovery redo also uses, so
  live-apply and recovery-redo are a single code path (no divergence).
  Uncommitted master txns apply provisionally and resolve at commit. JE
  `Replay.replayEntry`.
- **REP-10 — replica read-consistency policies.** `ReplicaConsistencyPolicy`
  (NoConsistency / TimeConsistency / CommitPointConsistency) is now enforced on
  the replica read path, gating on REP-7's `last_applied_vlsn` hook. A
  `CommitPointConsistency` read blocks until the replica replays past the
  `CommitToken`'s VLSN; `TimeConsistency` blocks until within the lag; timeout
  yields a clean `ConsistencyTimeout` (never a hang). Default stays
  NoConsistency. JE `Replica.ConsistencyTracker.awaitVLSN` / `CommitToken`.
- **T-2 / T-3 / T-5 — IN-array heap compaction (completes T-4 from v6.3.0).**
  T-3: per-slot LSNs become a node-level `LsnRep` (`Empty` 0-byte / `Compact`
  4-byte/slot base-file-relative / `Long` fallback) — the `NULL_LSN == u64::MAX`
  blocker is solved exactly as JE (the 3-byte `0xff_ffff` sentinel + base-file
  relative encoding, `INLongRep.entryLsnByteArray`). T-2: per-slot keys become a
  node-level `KeyRep` (`Compact` fixed-width buffer for all-small-key nodes /
  `Default` fallback), `INKeyRep.MaxKeySize`. T-5 wires
  `TREE_COMPACT_MAX_KEY_LENGTH`. On-disk format unchanged.
- **DBI-14 / DBI-15 — user-supplied comparators + inert-flag sweep.**
  `DatabaseConfig::with_btree_comparator` / `with_duplicate_comparator` take a
  `Comparator` (an `Arc<dyn Fn(&[u8],&[u8])->Ordering>` + a stable identity
  string); the comparator threads through the tree's key-comparison hot path.
  Persistence + mismatch: the comparator identity is persisted in the DB record;
  a reopen without a matching comparator fails with `ComparatorMismatch` (never
  silently mis-orders). The inert-flag sweep catalogues accepted-but-inert
  `EnvironmentConfig` knobs (logging/tracing routed through `log` / `noxu-observe`,
  stats-file dump, exception_listener, etc.). JE `DatabaseConfig.setBtreeComparator`
  / `ComparatorReader`.
- **EV-14 — evictRoot + a latent EV-13 corruption fix.** The evictor can evict
  an idle user-DB root IN (logging it first + updating `root_log_lsn`);
  `root_nodes_evicted` is no longer always 0. Building the re-fetch-on-access
  path surfaced and fixed a latent EV-13 gap: the tree descent returned `None`
  for a non-resident child instead of re-fetching it from the log
  (`ChildReference.fetchTarget`); `detach_node_by_id` now stamps the child's
  `last_full_lsn` into the parent slot so re-fetch reads the current on-disk
  version. JE `Evictor.evictRoot` / `Tree.getRootIN`.
- **CLN-4 / C7 / REC-Z / L-5-delta — persisted utilization; the cleaner relies
  on it.** C7 persists the full `FileSummary` breakdown + `PackedOffsets` in
  `FileSummaryLN`. CLN-4 rebuilds the per-file `UtilizationProfile` from those
  records inline during the recovery analysis pass and seeds the cleaner, so the
  cleaner sees real utilization immediately after restart (no re-warm lag).
  L-5-delta counts the superseded prior BIN-delta obsolete; REC-Z counts
  rolled-back LN versions obsolete during recovery. JE
  `UtilizationProfile.populateCache` / `RecoveryUtilizationTracker` /
  `RollbackTracker.countObsolete` / `IN.java` auxOldLsn.
- **DBI-24 — UtilizationTracker detail-memory budget cap.** The tracker caps its
  per-LSN obsolete-offset detail at `CLEANER_DETAIL_MAX_MEMORY_PERCENTAGE`
  (default 2% of cache), flushing detail when exceeded while preserving the
  aggregate counts (so util% / file selection is unaffected). JE
  `UtilizationTracker.getCacheMemoryUsage` / `Cleaner` DETAIL_MAX_MEMORY_PERCENTAGE.
- **CLN-26 / CLN-24 — TTL day-band proration + persisted expiration histogram.**
  CLN-26: the gradual-expiration band prorates over the whole day for day-TTL
  data (`ExpirationProfile.getExpiredBytes`). CLN-24: the per-file expiration
  histogram is serialized into `FileSummaryLN` and restored at recovery via the
  CLN-4 seam, so the cleaner's TTL-adjusted prediction survives restart. JE
  `ExpirationTracker.serialize` / `ExpirationProfile`.

### Changed

- **TXN-11 — lock-storage locus documented as identical to JE (no code change).**
  Investigation found the premise (JE embeds a `ThinLockImpl` in the BIN slot)
  factually wrong: JE `LockManager` keeps all per-record locks, thin included, in
  a side `Map<Long,Lock>` keyed by LSN (`TOTAL_THINLOCKIMPL_OVERHEAD` charges a
  `HASHMAP_ENTRY_OVERHEAD`; `BIN.java` / `IN.java` carry no lock field). Noxu's
  `lock_tables: Vec<Mutex<HashMap<u64, Lock>>>` is a 1:1 structural match. The
  earlier "authorized deviation" note in design-decisions overstated a
  non-existent difference and is corrected.

## [6.3.0] - 2026-06-22

### Added

- **REC-T/U/Y + REP-1 STEPS 1-4 + matchpoint decision core — diverged-tail
  syncup rollback machinery.** `RollbackStart` / `RollbackEnd` entries carry
  the matchpoint VLSN + active txn ids + matchpoint LSN; `RollbackPeriod.containsLN`
  gates on the active txn ids; a faithful `TxnChain` reverts each in-window LN to
  its previous version; recovery re-marks invisible + fsyncs rolled-back entries
  (the checksum cloaks the invisible bit so the flip is a single pwrite); the
  `ReplicaFeederSyncup` matchpoint decision core (`find_matchpoint` +
  `verify_rollback`) is in place. The live networked syncup driver was deferred
  (shipped in the following cycle as REP-1 STEP 5). JE `Replay.rollback` /
  `TxnChain` / `RollbackTracker`.
- **T-4 — INTargetRep heap compaction.** The resident-child pointer becomes a
  node-level `INTargetRep` (`None` / `Sparse` / `Default`); an upper IN with no
  resident children costs 0 child-pointer bytes. JE `INTargetRep`.

### Note on on-disk format

The `RollbackStart` / `RollbackEnd` field additions are a format change for HA
rollback entries only. No released non-HA build ever wrote these entries (they
were produced only by the later live syncup driver), so this is a clean MINOR
bump — `LOG_VERSION` is unchanged.

## [6.2.0] - 2026-06-19

### Added / Fixed

- **JE fidelity census fix campaign — all HIGH + MED + LOW findings addressed.**
  A function-by-function fidelity census across all ten subsystems (0 CRITICAL,
  8 HIGH, ~45 MED, ~25 LOW) drove this release. Highlights:
  - **Data-integrity**: L-14 (recovery halts on mid-file corruption via
    `findCommittedTxn`), CLN-7 (DOS producer file-protection guard), REP-9
    (ack-durability wired to commit-wait keyed by commit VLSN), REC-C/S
    (id-sequence recovery — db/txn/node ids no longer restart at 1 after
    recovery).
  - **Obsolete-accounting** (CLN-9/10/11): JE's three obsolete-counting methods
    (`countObsoleteNode` / `Inexact` / `DupsAllowed`) + a per-DB axis, fixing a
    structural under-count; validated ~5-6% footprint reduction under sustained
    churn.
  - **Evictor**: EV-13 (detach the evicted node from its parent so the heap is
    actually freed — was a phantom-free), EV-6/7 (now load-bearing
    NON_EVICTABLE guards for cached-children + root INs), EV-15 (synchronous
    critical eviction in writer threads).
  - **Recovery**: REC-D/F/G/H/AA/P checkpoint-breadth fixes (REC-AA: the
    dirty-upper-IN level computation + JE's +1 flush-level adjustment).
  - **Memory budget** (DBI-20-23): real per-category accounting.
  - **BIN-delta** (T-17): count-based delta decision with the configurable
    percent + prohibit-next-delta chain guard.
  - **Config** (C-1): all 30 Duration params bounded + validated; the inert
    two-pass gap/threshold and several evictor knobs wired.
  - **Dead-code removal** (~9.5k LOC): a `BinStub` conformance drift-guard
    confirmed no drift, then the shelved faithful `Bin`/`InNode`, the dead
    `txn_chain`, `INList`, and a duplicate `LruList` were deleted.
  No breaking public API.

## [6.1.0] - 2026-06-19

### Fixed (evictor — CLN-F2 regression)

- **CLN-F2 regression: dirty strip-0 BINs no longer cycle in pri2 forever
  under LRU-only, so eviction reclaims memory again.**  Commit 29119ca
  ("fall through to full BIN eviction when strip frees 0 bytes") changed the
  `evict_batch` strip-returns-0 path so that a *dirty* BIN was always routed
  to the priority-2 dirty-LRU (`pri2.add_front`).  But under
  `EVICTOR_LRU_ONLY` the `evict_batch` phase machine returns at phase 1 and
  **never drains pri2**, so a dirty strip-0 BIN was parked there forever and
  its memory was never reclaimed — the shared `cache_usage` counter never
  dropped and the engine could not get back under budget
  (`evictor_f1_f2_eviction_reduces_cache_usage` regressed:
  `usage_before == usage_after`).  The fix splits the two strip-0 cases
  faithfully to JE `Evictor.processTarget` (`Evictor.java` ~2755-2795):
  a **clean** strip-0 BIN falls through to `evict(target, parent, index)` and
  is fully evicted with its node bytes credited (the original CLN-F2 goal,
  preserved); a **dirty** strip-0 BIN gets the JE `moveToPri2LRU` one-time
  second chance only when the dirty-LRU set is actually in use
  (`use_dirty_lru && !lru_only`, mirroring JE's `useDirtyLRUSet` guard at
  ~2758-2766), and otherwise reverts to the pre-CLN-F2 put-back so a later
  pass can strip its now-clean slots once a checkpoint has logged+cleaned
  them — which is where a dirty BIN's reclaimable memory (the LN value heap)
  actually lives.  The CLN-F2 unit test (`test_evict_batch_partial_evict_
  path`, clean BINs -> `nodes_evicted == 3`) and the integration test (real
  tree, dirty-then-checkpointed BINs -> `cache_usage` drops) both pass.

### Fixed (recovery durability — REC-F1/REC-F2)

- **REC-F1: checkpoint `CkptEnd` is now fsync'd on every path.**
  `Checkpointer::do_checkpoint` logged the `CkptEnd` entry with
  `flush_required=true, fsync_required=false`, which only reaches the OS page
  cache (no `fdatasync`).  `do_checkpoint` then advances the cleaner's
  safe-to-delete file barrier via `cleaner.after_checkpoint(...)` — off a
  non-durable checkpoint.  Only `EnvironmentImpl::close` followed with an
  explicit `flush_sync()`; the daemon checkpoint and the bytes-triggered
  `wakeup_after_write` checkpoint did not, so a crash after an auto/daemon
  checkpoint could reference cleaned files and lose committed/migrated data.
  `CkptEnd` is now logged with `fsync_required=true`, mirroring JE
  `Checkpointer.doCheckpoint` (~line 895):
  `logManager.logForceFlush(endEntry, true /*fsyncRequired*/, ...)` — "We
  must flush and fsync to ensure that cleaned files are not referenced. This
  also ensures that this checkpoint is not wasted if we crash."  The fsync
  precedes the cleaner barrier advance on all three callers (close, daemon,
  bytes-triggered).  This adds one `fdatasync` per checkpoint; JE pays the
  same cost deliberately for durability.

- **REC-F2: LN-redo apply now enforces JE's redo currency guard.**
  The LN-redo path (`Tree::redo_insert` →
  `redo_insert_recursive_inner`) unconditionally overwrote
  `entries[idx].lsn`/`.data` with no comparison to the existing slot LSN.
  Combined with IN-redo installing a BIN slot at the BIN's logged LSN X, a
  later LN-redo of an older committed LN at LSN Y < X for the same key could
  overwrite the slot with the older value and reset the slot LSN backward —
  reverting committed data.  The apply now skips the overwrite when the
  existing slot LSN is greater-than-or-equal to the logged LSN (replace only
  when `log_lsn > slot_lsn`), matching JE `RecoveryManager.redo()`
  (~line 2512/2544): `lsnCmp = compareTo(logrecLsn, treeLsn); if (lsnCmp > 0)
  replace`.  This makes `redo_ln`/`redo_insert` genuinely idempotent
  regardless of redo/undo phase order.  Noxu keeps its redo-then-undo
  ordering (JE undoes before redo, RecoveryManager.buildTree ~line 1967);
  the currency guard removes the hazard so reordering is not required.  The
  false "idempotent" doc comments were corrected to match the implemented
  guard.
### Fixed (tree/cursor — runtime read/scan now honors `known_deleted`, TREE-F1)

- **Exact lookups and cursor scans no longer surface `known_deleted` BIN
  slots.** A `known_deleted` slot legitimately exists in a live BIN during
  BIN-delta reconstitution (`mutate_to_full_bin` applies delta KD slots) until
  the compressor reclaims it. The runtime `*Stub` read/scan paths checked TTL
  expiry but ignored `known_deleted`, so a `get` or cursor scan in that window
  could return a deleted record — a wrong-results bug.
  - `Tree::search` / `Tree::search_with_data` now report a `known_deleted`
    slot as ABSENT on an exact match, mirroring the tail of JE
    `IN.findEntry` (`IN.java:3197`): `if (ret >= 0 && exact &&
    isEntryKnownDeleted(ret & 0xffff)) return -1;`.
  - `Tree::first_entry_at_or_after(_with_index)`, `Tree::get_first_node`,
    `Tree::get_last_node`, and the `CursorImpl` within-BIN / cross-BIN advance
    (`get_first`, `get_last`, `retrieve_next`) now skip non-live slots,
    mirroring JE `CursorImpl.lockAndGetCurrent` (`CursorImpl.java:2062-2064`),
    which returns `null` for `isEntryKnownDeleted(index)` so the `getNext`
    loop steps past it — including crossing entirely-KD edge BINs.
  - A single shared liveness predicate `BinStub::slot_is_live` (KD + TTL) is
    used at every user-facing read/scan site. The compressor / recovery KD
    iteration paths (`collect_bins_with_known_deleted`, `prune_empty_bin`,
    recovery undo) are unchanged and still observe KD slots on purpose.
### Fixed (txn — locker lock-sharing on the acquisition path, TXN-F2)

- **`LockManager::lock` / `lock_with_timeout` now consult the lock-sharing
  registry on every acquisition.** Previously the production acquisition path
  (used by every locker: `ThreadLocker`, `HandleLocker`, `BasicLocker`,
  `Txn`) called `LockImpl::lock`, which hard-wired sharing off
  (`try_lock_with_sharing(..., &|_| false)`). The `lock_with_sharing*` family
  that *did* honor the registry was only ever reached from its own unit test.
  `ThreadLocker::new` and `HandleLocker::with_buddy` faithfully populate the
  registry, but acquisition never read it — so two `ThreadLocker`s on the same
  thread (e.g. two cursors under auto-commit) or a `HandleLocker` + its buddy
  txn requesting conflicting locks on the same LSN would self-deadlock or
  spuriously `LockTimeout`, which JE never does. JE `LockImpl.tryLock` checks
  `!locker.sharesLocksWith(ownerLocker) && !ownerLocker.sharesLocksWith(locker)`
  on **every** acquisition (LockImpl.java:647-648). The production path now
  builds the `sharesLocksWith` predicate from the registry and routes through
  `LockImpl::lock_with_sharing`; the `lock_with_sharing` /
  `lock_with_sharing_and_timeout` methods are now thin deprecated forwarders.
  This also corrects a doc-bug (TXN-F6) that claimed the plain `lock()` path
  already used the registry — now true.

### Fixed (txn — restart-conflict scan honors lock sharing, TXN-F1)

- **`LockImpl`'s restart-conflict waiter scan now skips a waiter the
  requestor shares locks with.** JE `LockImpl.lock` checks `waiterType !=
  RESTART && locker != waiterLocker && !locker.sharesLocksWith(waiterLocker)`
  (LockImpl.java:395) in the waiter scan that runs when a restart-causing
  request (RANGE_READ / RANGE_WRITE) has to wait. The Rust
  `lock_with_sharing` received the `shares_fn` for `try_lock` but did not
  thread it into the restart loop, so a requestor sharing locks with a
  RANGE_INSERT waiter would spuriously restart instead of waiting normally.
  Added the `!shares_fn(w.locker_id)` clause; `LockImpl::lock` now delegates
  to `lock_with_sharing(..., &|_| false)` so a single implementation carries
  the restart scan (mirroring how `try_lock` delegates to
  `try_lock_with_sharing`).

### Fixed (txn — importunate lock steal in the wait path, TXN-F3, rep-only)

- **Importunate (HA `ReplayTxn`) lock requests now steal a held conflicting
  lock instead of being conflated with `jumpAheadOfWaiters`.** `Txn::lock`
  passed `self.importunate` into the `jump_ahead_of_waiters` slot of
  `lock_with_timeout`, but jumping ahead of *waiters* never removes a
  conflicting *owner* — so an importunate replay would block / time out
  against a non-importunate owner. JE: normal `Locker.lock` always passes
  `jumpAheadOfWaiters=false` (Locker.java:503); importunate is handled inside
  `LockManager.waitForLock` by `if (isImportunate) { result =
  stealLock(...) }` (LockManager.java:552), letting HA replay preempt a
  preemptable owner. `Txn::lock` now passes `false` for jump-ahead and routes
  importunate requests through a new `lock_importunate_with_timeout`, which
  steals from preemptable owners (mirroring `stealLockInternal`,
  LockManager.java:1599) and re-attempts. A non-preemptable owner (another
  importunate locker, tracked in a new non-preemptable registry) blocks the
  steal, falling back to a normal wait (JE's `continue`, LockManager.java:556).
  `LockImpl::steal_lock` gained a `steal_lock_preemptable` variant honoring
  `getPreemptable()` (LockImpl.java:543).
### Fixed (cleaner — faithful multi-tier file selection, CLN-F1)

- **File selection now applies the utilization threshold as JE's AGGREGATE
  multi-tier gate instead of a per-file exclusion** (`noxu-cleaner`
  `file_selector.rs`). The production `select_file_for_cleaning` previously
  collapsed `UtilizationCalculator.getBestFile`'s decision into a per-file
  filter (`avg_util >= min_utilization -> skip`), and dropped
  `cleaner_min_file_utilization` on the floor. This caused both under-cleaning
  (the aggregate was below threshold but the best file's own util was above it,
  so it was skipped and the log grew) and over-cleaning (any sub-threshold file
  was cleaned even when the aggregate said cleaning was not warranted). The
  candidate loop now tracks the lowest-avg `bestFile` and lowest-max-gradual
  `bestGradualFile` over ALL eligible files with no per-file exclusion, and the
  decision is JE-faithful: tier 1 `predictedMinUtil < minUtilization ->
  bestFile`; tier 2 `bestGradualFileMaxUtil < minFileUtilization ->
  bestGradualFile`; tier 4 forced -> bestFile (UtilizationCalculator.java
  ~344-425). `compute_predicted_min_util` now returns the true AGGREGATE
  utilization (summed obsolete / summed total, honouring in-progress files)
  rather than the per-file minimum (FileSummary.java:292). `cleaner_min_file_
  utilization` is wired end-to-end (config -> `Cleaner::with_min_file_
  utilization` -> selection second tier). Reproduction tests:
  `test_clnf1_aggregate_below_threshold_selects_high_util_best_file` (was
  skipped pre-fix) and `test_clnf1_aggregate_above_threshold_cleans_nothing`
  (over-cleaned pre-fix).

### Fixed (evictor — BIN nodes can now be fully evicted, CLN-F2)

- **A clean, unpinned, cursor-free BIN whose LN strip frees 0 bytes now falls
  through to full eviction** (`noxu-evictor` `evictor.rs`). The `PartialEvict`
  decision was terminal: `evict_batch` always put the BIN back, so a BIN node's
  heap could never be reclaimed and the tree's structural footprint could not
  shrink under pressure. The arm now mirrors `Evictor.processTarget`
  (Evictor.java ~2712-2795): if partial eviction frees bytes -> strippedPutBack;
  if it frees 0 bytes -> give a dirty BIN a second chance in the pri2 dirty-LRU,
  otherwise FULLY evict it (credit `node_size_fn` + `nodes_evicted`). The
  existing pin/cursor/dirty guards are preserved (`strip_lns_from_node` returns
  `None` for a pinned or cursor-referenced BIN -> put back). Reproduction:
  `test_evict_batch_partial_evict_path` now asserts full eviction (was always
  put-back) plus `test_evict_batch_partial_evict_dirty_bin_moves_to_pri2`.

### Fixed (cleaner — obsolete-LN size guard, CLN-F3)

- **`UtilizationTracker::track_obsolete` only accumulates the LN size and
  counted tally when `size > 0`** (`noxu-cleaner` `utilization_tracker.rs`),
  matching `BaseUtilizationTracker.countObsoleteNode` (~184-189: "the size is
  optional when tracking obsolete LNs"). Previously both incremented
  unconditionally, which would corrupt the average-LN-size estimator if ever
  called with `size <= 0` (latent: the sole production caller passes a real
  size). Test: `test_track_obsolete_ln_size_zero_does_not_count_size`.

### Removed (cleaner — dead reinvented heuristic, CLN-F6)

- **Deleted `FileSelector::check_for_required_util`** and its test-only callers
  (`noxu-cleaner`). It was a previously-flagged reinvented "required-util
  shortfall" heuristic (`new_req = actual + (actual - target)`) with no
  production caller — the faithful two-pass path is `Cleaner::two_pass_check` +
  `FileSelector::remove_file_from_cleaning`. Removed to prevent future
  mis-wiring.

## [6.0.0] - 2026-06-19

### Changed (BREAKING) (engine — remove fake-passing verify stubs)

- **Removed `noxu_engine::verify_environment(&VerifyConfig)` and
  `noxu_engine::verify_database(&str, &VerifyConfig)`** (and their `lib.rs`
  re-exports). Both were stubs that logged a warning and returned an empty
  *passing* `VerifyResult` without performing any integrity check — a caller
  received `passed = true` for a corrupt database. They could not do real work:
  structural verification requires a live `EnvironmentImpl` / `DatabaseImpl`
  handle, which these signatures (a bare `&str` / no env handle) do not provide.
  The real, already-wired entry points are unchanged: `Environment::verify` and
  `Database::verify` (noxu-db), which route through
  `noxu_engine::verify_database_impl` → `verify_tree` and perform a genuine
  live-tree structural walk (child accessibility, key-range containment,
  non-deleted-slot LSN validity). This mirrors JE `DbVerify` /
  `Environment.verify`, which always operate on an opened environment. Callers
  of the removed functions (none existed outside their own stub tests) should
  use `Environment::verify` / `Database::verify`. Added
  `test_verify_tree_detects_null_lsn` proving the verifier detects a real
  structural fault (a non-deleted BIN slot carrying a NULL LSN) rather than
  silently passing.

### Fixed (engine — `Engine::close` now closes `EnvironmentImpl`)

- **`Engine::close` now calls `EnvironmentImpl::close()`** (`noxu-engine`),
  completing step 3 of its documented shutdown sequence. Previously the body
  carried a TODO ("EnvironmentImpl doesn't have explicit close yet - would be
  added in full implementation") and skipped the step, so the dbi-layer daemons
  (evictor / checkpointer / INCompressor / cleaner / log-flush) and the final
  forced checkpoint + WAL fsync owned by `EnvironmentImpl` only ran later via
  `Drop`. `EnvironmentImpl::close()` is idempotent (early-returns when already
  closed), so the explicit call and the `Drop` backstop do not conflict. The
  close-path doc comment was corrected to describe the real behaviour. Test
  `test_engine_open_and_close` now asserts `get_env_impl().lock().is_open()` is
  false after `Engine::close`.

### Fixed (cleaner — two-pass gate keys on the utilization uncertainty band, CFG-TWOPASS-1)

- **`CLEANER_TWO_PASS_GAP` / `CLEANER_TWO_PASS_THRESHOLD` are now wired and gate
  on the per-file (min, max) utilization uncertainty band** (`noxu-cleaner` /
  `noxu-dbi` / `noxu-db`), faithfully porting JE
  `UtilizationCalculator.getBestFile`. Added
  `ExpirationTracker::get_expired_bytes_band` returning the (lower, gradual-upper)
  expired-bytes pair (JE `ExpirationProfile.getExpiredBytes`): lower = bytes
  whose expiration interval fully passed; gradual-upper = + a prorated fraction
  of bytes expiring within the current interval. `scan_file_summary` populates
  both bounds on the `FileSummary` (new `obsolete_expired_gradual_size`);
  `FileSelector` computes `min_utilization_pct` / `max_utilization_pct` from the
  band and requests a two-pass dry-run (`required_util = twoPassThreshold`,
  threshold 0 → `minUtilization − 5`) exactly when `maxUtil > twoPassThreshold
  && (maxUtil − minUtil) >= twoPassGap`. Wired end-to-end from
  `EnvironmentConfig.cleaner_two_pass_gap/threshold`. Tests
  `test_expired_bytes_band_uncertainty`, `test_two_pass_gate_fires_on_uncertainty_band`.


### Testing (Margo JE test-accuracy review — txn/bind/rep/XA)

- Verdict: the transaction, binding, collections, persist, XA, and
  implemented-replication tests port nearly identically to JE — the
  lock-manager / deadlock / isolation / phantom suites are faithful or STRONGER
  (full 25×25 conflict+upgrade matrices, T-F2 next-key phantom suite). C7
  (RMW), C8 (4-locker/intersecting deadlock), F1/F3, COL-KEYSET-1,
  PERSIST-COMP-1 all verified faithful and passing. No UNJUSTIFIED divergences.
  Two WEAKENED items corrected:
- Tightened `read_uncommitted_sees_dirty_write`: JE `DirtyReadTest` asserts the
  READ_UNCOMMITTED reader sees the SPECIFIC uncommitted value; the impl makes
  it deterministic (synchronous in-memory put before commit, gated by a write
  barrier), so the assertion is now `assert_eq!(data, "dirty")` rather than the
  over-loose `"dirty" || "baseline"` disjunction.
- Documented `je_ranking_proposer_test::test_phase2_arb_one_node`: JE also
  asserts the lone-arbiter-higher-DTVLSN → no-master cases; those require
  DTVLSN-based election ranking (an authorized deferral) and are now explicitly
  noted as skipped-pending-DTVLSN, with a note that the test exercises a
  test-local arb-exclusion helper (production `run_election` enforces the same
  via its F22 guard).


### Testing (Keith JE test-accuracy review — W1/W2/D1/M1)

- Verdict: the storage-engine core is faithfully ported at ~100% on the
  consistency-critical paths (recovery equality, stepwise torn-write sweep,
  forced split topologies, BIN-delta/known-deleted, cleaner SR regressions,
  log-corruption detection, post-recovery structural verify) — zero outright
  WEAKENED ports. The corrections:
- **W1**: raised the dup-cursor test scale (~5 keys → ~300 keys / 2000 inserts,
  2-byte keys) so the duplicate walk crosses BIN boundaries (multi-BIN dup
  traversal + BIN-split-under-dups), which the prior 5-key fixture could not
  exercise.
- **W2**: restored the "large delete" cursor tests to multi-BIN scale
  (N 100 → 300, above one BIN at fanout 128) so delete-then-walk spans BINs.
- **D1**: replaced the stale `je_rmw_locking_test.rs` header (it still claimed
  RMW was unimplemented / tests `#[ignore]`d — false since the C7 fix) with
  accurate prose.
- **M1**: corrected recovery-test comments that overclaimed `VerifyUtils.checkLsns`;
  documented the LSN↔utilization-profile-overlap check as a tracked residue
  (env.verify()'s structural tree walk IS run after every recovery; the
  LSN↔UP half needs the UP threaded into the verifier).


## [5.0.0] - 2026-06-18

### Fixed (isolation — LockMode::Rmw takes a write lock, C7)

- **`LockMode::Rmw` now acquires a WRITE lock on read** (`noxu-db` / `noxu-dbi`):
  found by the JE-fidelity test port (C7) — Noxu accepted `LockMode::Rmw` but
  the cursor/get read paths ignored it, so an RMW read behaved like a plain read
  and did NOT block a concurrent writer (JE `Cursor.java:5281` maps RMW → WRITE
  lock so a later same-txn update cannot deadlock and a concurrent writer blocks
  at read time). Added `CursorImpl::upgrade_current_to_write_lock` and wired it
  into both `Cursor::get` (on `LockMode::Rmw`) and
  `Database::get_with_options` (on `ReadOptions::read_modify_write`). The
  faithful `je_rmw_locking_test.rs` tests are now un-ignored and pass
  (RMW read blocks a no_wait writer and a concurrent writer until commit).


### Testing (JE test-fidelity — C8: deadlock 4-locker + intersecting cycles)

- **Ports JE `DeadlockTest` 4-locker and intersecting-cycle cases** beyond the
  existing 2/3-locker coverage:
  - `noxu-txn/tests/integration_tests.rs` (graph-level, deterministic via
    `DeadlockDetector::detect`): `deadlock_four_locker_cycle_detected`
    (T1→T2→T3→T4→T1 ring, JE `testDeadlockAmongFourTxns`) and
    `deadlock_intersection_one_common_locker_detected` (two cycles sharing a
    common locker, JE `testDeadlockIntersectionWithOneCommonLocker`).
  - `noxu-txn/tests/lock_manager_test.rs` (end-to-end threaded via
    `LockManager::lock`): `je_deadlock_among_four_txns` (4-thread ring) and
    `je_deadlock_intersection_one_common_locker` (3-thread intersecting cycle
    with a shared read lock). Each asserts the cycle is broken — at least one
    waiter surfaces `TxnError::Deadlock` and no thread hangs (all join).

### Testing (JE test-fidelity — C7: RMW locking core invariant) — FINDING

- **New `je_rmw_locking_test.rs`** ports the core `LockMode.RMW` contract from
  JE (`RMWLockingTest` / `Cursor.get(..., LockMode.RMW)`): a read with
  `LockMode::Rmw` must take a WRITE lock and block a concurrent writer.
- **FINDING (real Noxu divergence):** `LockMode::Rmw` is *defined* but its
  write-lock-on-read semantics are NOT implemented. `Cursor::get`'s
  `lock_mode` parameter is `_lock_mode` (ignored); `get_with_options` routes
  `Rmw` through the same plain-read `cursor.search` path as `Default`; and
  `noxu-dbi`'s `CursorImpl::search` / `get_current` never acquire a write lock
  for a read. An RMW read therefore behaves like a plain read and does NOT
  block a concurrent writer.
- The two faithful RMW tests
  (`rmw_read_holds_write_lock_no_wait_writer_conflicts`,
  `rmw_read_blocks_concurrent_writer_until_commit`) are `#[ignore]`d (NOT
  weakened) to document the gap; they pass once RMW write-locking is wired.
  The control test `plain_read_committed_releases_lock_writer_succeeds` runs
  in the default suite and validates the harness. Run the ignored tests with
  `cargo test -p noxu-db --test je_rmw_locking_test -- --ignored`.

### Testing (JE test-fidelity — C6: log-file corruption detection)

- **New `log_corruption_test.rs`** — faithful in spirit to JE
  `com.sleepycat.je.util.LogFileCorruptionTest.testDataCorruptWithVerifier`
  (which flips a byte at `fileLength/2` and expects
  `EnvironmentFailureException`):
  - `byte_flip_in_committed_entry_is_detected`: write a committed workload
    spanning several log files, flip one byte (all 8 bits) at the midpoint of
    a non-final committed `.ndb` file, reopen, and assert the corruption is
    DETECTED — the recovered set is a strict prefix of the committed set (the
    corrupt entry + tail are dropped at the CRC/torn boundary) and NO
    garbage/wrong value is ever returned. Proves the per-entry CRC32 catches a
    flipped committed entry rather than silently returning it.
  - `mid_entry_truncation_torn_tail_not_returned`: truncate the last file
    mid-entry; the torn tail must be treated as end-of-log and never surfaced
    as data (recovered set is a subset of the committed set, no garbage).

### Added (API parity — `Environment::clean_log`)

- **`Environment::clean_log()`** — public synchronous log-cleaning trigger
  mirroring JE `Environment.cleanLog()`. Forwards to the cleaner and returns
  the number of files cleaned. Needed for deterministic cleaner regression
  tests (C5) and for applications that reclaim space on demand rather than
  relying on the background daemon. (Previously only the read-only-rejection
  variant was covered; the working manual-clean path was unexposed.)

### Testing (JE test-fidelity — C5: cleaner SR regressions)

- **New `je_cleaner_sr_test.rs` ports two high-signal JE cleaner SR
  regressions** (`com/sleepycat/je/cleaner/SR10553Test`, `SR12885Test`):
  - `sr10553_clean_then_scan_deleted_does_not_fail`: put duplicates, delete
    all, checkpoint, `clean_log()`, evict, scan — the scan must complete
    without a LogFileNotFound-style error (JE: cleaner must set knownDeleted
    for deleted records). Asserts `cleaned > 0`.
  - `sr12885_pending_ln_migration_with_slot_reuse_abort_keeps_data`: drive the
    cleaner LN-migration + txn slot-reuse + abort sequence; the surviving key
    must still fetch SUCCESS (data not lost to a cleaned file).
  Adaptation note: JE's specific SR12885 node-ID bug is, per JE's own comment,
  not applicable to LSN-locking engines — Noxu locks LSNs and LNs have no node
  IDs (AGENTS.md "Lock-based, NOT MVCC"), so the still-applicable data-safety
  invariant is ported.
- **SR13061 (`FileSummaryLN.hasStringKey`) SKIPPED** (documented in the test
  module): it guards a JE log-version-migration bug where an old STRING
  file-summary key was misread as an 8-byte integer key. Noxu has a single
  binary `.ndb` format with no legacy string-key path, so the bug class cannot
  exist — not a fidelity gap.

### Testing (JE test-fidelity — C4: RecoveryDeltaTest testCompress + testKnownDeleted)

- **`recovery_correctness_test.rs` now ports JE
  `com.sleepycat.je.recovery.RecoveryDeltaTest`** (`testCompress`,
  `testKnownDeleted`):
  - `delta_test_compress_recovers_surviving_set`: insert, delete every other,
    `env.compress()`, force checkpoint, recover, assert the recovered set ==
    the surviving committed set (+ structural `env.verify()`). Authorized
    deviation: the JE `NDeltaINFlush == 0` ("compress forces a full BIN")
    invariant tests JE's deferred-compression mechanic; Noxu deletes
    PHYSICALLY (IC-3, `tree.rs::compress_bin`), so `env.compress()` is a no-op
    for committed deletes and the stat invariant does not apply — the
    data-correctness half is ported faithfully.
  - `delta_test_known_deleted_replays`: drive a checkpoint that writes
    BIN-deltas whose base BINs carry known-deleted tombstone slots (from
    aborted inserts), then recover and assert every committed key is present
    and no tombstone key leaks (BIN-delta reconstitution clears stale KD).
    Asserts `checkpoint.delta_in_flush > 0` (JE `getNDeltaINFlush() > 0`).
    Authorized deviation: the Noxu checkpointer hardcodes the BIN-delta dirty
    threshold at 25% (`checkpointer.rs` const `TREE_BIN_DELTA`) and does not
    read the config param, so JE's `BIN_DELTA_PERCENT = 75` cannot be set; the
    KD churn / committed mutation are applied to small per-BIN subsets to stay
    under 25% while still producing KD-bearing deltas. Asserted property
    (KD-delta replay correctness) is preserved.

### Testing (JE test-fidelity — C3: forced split-recovery topologies)

- **New `forced_split_recovery_test.rs` ports three JE recovery topology
  suites** — each deliberately drives a specific B-tree topology, then
  recovers and asserts BOTH data equality AND structural integrity
  (`env.verify()` zero errors, per JE `CheckBase.recoverAndLoadData`):
  - `new_root_via_split_recovers` / `change_and_evict_root_recovers`
    (JE `CheckNewRootTest.testWrittenBySplit` / `testChangeAndEvictRoot`):
    new-root creation via ascending right-splits + checkpoint, and root
    survival across eviction + checkpoint.
  - `split_aunt_recovers` (JE `CheckSplitAuntTest.testSplitAunt`): deep tree,
    dirty the left branch, checkpoint to level 2 leaving an ancestor dirty,
    then split the right branch ("split-aunt"), close w/out checkpoint,
    recover.
  - `reverse_split_recovers` / `complete_removal_recovers`
    (JE `CheckReverseSplitsTest.testReverseSplit` / `testCompleteRemoval`):
    empty the leftmost BIN, checkpoint, compress out the empty BIN (reverse
    split / subtree removal), then split/insert and recover; complete-removal
    additionally asserts a single surviving BIN after compress.
  Adaptation: ASCII keys instead of JE `IntegerBinding`, `env.evict_memory()`
  instead of JE's evictor `TestHook`, `env.checkpoint(force)` for JE
  `env.sync()`; split/merge geometry preserved via matching NODE_MAX and
  insert/delete counts.

### Testing (JE test-fidelity — C2: deterministic stepwise truncation sweep)

- **New `stepwise_truncation_test.rs` ports JE `CheckBase.stepwiseLoop`**
  (driven by `CheckSplitsTest.testBasicInsert` and the
  `recovery/stepwise` support classes `EntryTrackerReader` / `LogEntryInfo` /
  `TestData`). Where `power_loss_sweep.rs` only sampled RANDOM kill points,
  this is JE's deterministic EXHAUSTIVE torn-write boundary sweep: write a
  known 21-key ascending autocommit workload with `NODE_MAX = 4` (forcing BIN
  splits), walk every log-entry boundary in every `.ndb` file with the
  production header/LN parsers (`noxu_log::LogEntryHeader`,
  `LnLogEntry::parse_from_slice` — the analogue of JE's `EntryTrackerReader`),
  truncate at each boundary, recover, and assert the recovered set equals the
  EXACT surviving subset (independently computed by replaying the surviving
  log prefix, mirroring JE's `updateExpectedSet`). Same exact-set assertion
  strength as JE `CheckBase.validate`; `env.verify()` runs after each
  recovery (C1). Adaptation: ASCII `key_NNNN` keys instead of JE
  `IntegerBinding` 4-byte keys; scenario and assertion strength preserved.


### Fixed (evictor config — EVICTOR_USE_DIRTY_LRU wired; dead config documented)

- **`EVICTOR_USE_DIRTY_LRU` is now read from config** (`noxu-evictor` /
  `noxu-dbi` / `noxu-db`): the evictor derived dirty-LRU staging from
  `!lru_only` and ignored the `EVICTOR_USE_DIRTY_LRU` parameter (default true).
  Now wired end-to-end (`EnvironmentConfig.evictor_use_dirty_lru` →
  `DbiEnvConfig` → `Evictor::with_use_dirty_lru`), and forced false when an
  *enabled* off-heap cache is present (JE Evictor.java:1705). Test
  `test_use_dirty_lru_config_and_offheap_override`.
- Documented the remaining not-yet-wired cleaner/evictor tuning parameters
  (`CLEANER_TWO_PASS_GAP/THRESHOLD`, `BIN_DELTA_BLIND_OPS/PUTS`,
  `EVICTOR_MUTATE_BINS/FORCED_YIELD`, `CLEANER_RMW_FIX/GRADUAL_EXPIRATION`,
  `RESERVED_DISK`) in known-limitations: their underlying features/models are
  not fully ported, so the params are accepted but ignored (tuning knobs, no
  correctness impact). The two-pass case uses a functional-but-different
  `required_util` heuristic pending the min/max-utilization uncertainty band.

### Changed (BREAKING — persist composite-key on-disk format, PERSIST-COMP-1)

- **Composite (multi-field) primary-key on-disk encoding changed; existing
  composite-key DPL databases must be rebuilt.** `#[derive(PrimaryKey)]` for a
  multi-field key struct previously encoded each field as
  `[4-byte BE length][field bytes]`. The length prefix made the on-disk key
  sort by `(len(field0), field0, len(field1), …)` instead of the logical tuple
  order `(field0, field1, …)`, so ordered iteration and `PrimaryIndex` range
  scans over any multi-field primary key returned records in the WRONG order
  (silent ordering corruption). The encoding is now order-preserving and
  self-delimiting with NO length prefix, matching JE's tuple key format
  (`com.sleepycat.bind.tuple.TupleOutput`): fixed-width numerics keep their
  big-endian / sign-flipped big-endian bytes and decode by width; `String` and
  `Vec<u8>` are written as a `0x00`-terminated, escaped byte string (data
  `0x00` → `0x00 0x01`, terminator `0x00 0x00`) — the same idea as JE
  `TupleOutput.writeString`'s null-terminated UTF-8. Byte-lexicographic order
  of the concatenation now equals logical tuple order.
  - **Migration**: dump and reload any DPL store whose entities use a
    multi-field `#[derive(PrimaryKey)]`. Single-field newtype keys (e.g.
    `struct UserId(u64);`) are byte-compatible and need no action.
  - There are no known production users on v4.x, so no in-place converter is
    provided.

### Fixed (secondary / join — JE-fidelity F1/F3)

- **Foreign-key constraint now enforced on secondary INSERT** (`noxu-db`, F3):
  JE `SecondaryDatabase.insertKey` rejects (`ForeignConstraintException`) a
  secondary insert whose key is absent from the configured foreign-key
  database. Noxu enforced this only on the foreign-DELETE side (Abort/Cascade/
  Nullify); the INSERT side silently accepted dangling references. Added the
  per-key foreign-DB existence check in `insert_sec_key`, skipped inside an FK
  cascade/nullify (the thread-local guard) so the nullify-rewrite isn't
  re-checked and the foreign DB isn't re-locked (deadlock). Regression test
  `fk_insert_rejects_secondary_key_absent_from_foreign_db`; corrected
  `fk_nullify_multi_key_nullifier_path` to populate all referenced foreign keys
  (JE applies the FK check per generated multi-key, so the prior fixture was
  JE-invalid).
- **JoinCursor probe now uses SearchBoth, not the cursor's current position**
  (`noxu-db`, F1): JE `JoinCursor.retrieveNext` probes each secondary with
  `search(secKey, candidatePK, SearchMode.BOTH)` — an exact lookup that scans
  the whole duplicate set. Noxu read only the single primary key the cursor was
  parked on (`Get::Current`), silently dropping join matches whenever a
  secondary key maps to more than one primary. Now captures the join secondary
  key once and `SearchBoth`-probes against it. (Fully exercised only with
  sorted-dup secondaries, a v1.6 deferred feature; correct for the current
  one-to-one model and faithful for when sorted-dup lands.)


### Fixed (collections — atomic StoredKeySet.add, JE-fidelity COL-KEYSET-1)

- **`StoredKeySet::add` is now an atomic `putNoOverwrite`** (`noxu-collections`):
  it did a non-atomic get-then-put (a TOCTOU where two concurrent adds could
  both observe "absent" and both report the key as newly-added). JE
  `StoredKeySet.add` uses a single `putNoOverwrite` that atomically reports
  whether the key was new. Now matches JE. (The prior put could not actually
  clobber user data — a key-set's value is always empty — so this is a
  race-correctness fix, not data-loss.)


### Testing (JE test-fidelity — C1: structural post-recovery verification)

- **Recovery tests now assert STRUCTURAL integrity, not just data equality**
  (JE `CheckBase.recoverAndLoadData` runs `env.verify()` + `checkLsns()` after
  every recovery). The Noxu recovery suites
  (`recovery_correctness_test.rs::recover_and_collect`,
  `crash_recovery_test.rs::reopen_db`) asserted only `BTreeMap` data equality;
  they now also run `Environment::verify` and require zero structural errors
  after every clean-recover and crash-recover scenario. All 15 correctness +
  11 crash tests pass with the stronger check (Noxu's recovery produces
  structurally-sound trees, not merely correct data).


### Security / Rust-quality (jonhoo review + cargo-deny)

- **Bumped `lru` 0.12 → 0.16** (`noxu-log`, `noxu-evictor`): resolves
  RUSTSEC-2026-0002 (an `IterMut` Stacked-Borrows unsoundness in `lru` ≤ 0.16.2).
  Noxu never calls the affected `iter_mut` path, but the dependency is upgraded
  to the patched version regardless. API-compatible; all tests green.
- **`cargo deny` is now a CI gate** (GitHub workflow) and a `make deny` target:
  the `deny.toml` existed but was wired into nothing. Modernised its schema to
  the current cargo-deny format; supply-chain + license checks now pass and run
  on every push.
- **`#[must_use]` on the public config types** (`EnvironmentConfig`,
  `DatabaseConfig`, `TransactionConfig`, `CursorConfig`): the owned-`self`
  `with_*` builders silently no-op'd when used as a statement; the attribute
  makes that a warning.
- Removed the tracked empty `CHANGELOG.md.tmp` (repo hygiene).


## [4.1.0] - 2026-06-18

### Performance (recovery — streaming analysis scan, JE-fidelity)

- **Recovery analysis no longer materialises the bounded log range into an
  intermediate `Vec`** (`noxu-recovery` / `noxu-dbi`). `RecoveryManager::run_analysis`
  previously called `scanner.scan_forward(start, end)`, which parsed every
  entry in the post-checkpoint range into a `Vec<PositionedEntry>` (each LN
  entry cloning its key/data `Bytes`) only to iterate it once. It now drives a
  single forward pass through the new `LogScanner::scan_forward_fn(start, end,
  cb)` streaming callback, which the file-backed `FileManagerLogScanner`
  overrides to invoke the per-entry closure inline from the mmap'd/read file
  bytes — eliminating the O(N) intermediate allocation. This mirrors JE's
  `LNFileReader` / `INFileReader` read loop (`FileReader.readNextEntry`), which
  pulls one entry at a time rather than building the whole range. The redo-LN,
  IN-redo, and undo passes are unchanged (they iterate in-memory state or read
  backward, matching JE's multi-pass structure — only the single-forward-scan
  analysis pass was streamed). Measured recovery `Environment::open()` of a
  100k-record crash log: ~273 ms → ~264 ms (~3%, interleaved 8-round mean) —
  the intermediate `Vec` was a real but minor cost; the redo/tree-splice/fsync
  path dominates recovery time at this scale. Semantics are byte-for-byte
  identical; all recovery, crash-recovery, and JE-recovery suites stay green.

### Fixed (cache evictor — keystone wiring, JE-fidelity)

- **The cache evictor is no longer inert in production** (`noxu-tree` /
  `noxu-evictor` / `noxu-dbi`, evictor F1+F2). Two confirmed Critical gaps:
  - **F1 — LRU policy lists were never populated.** The evictor's
    `note_ins_added` / `note_ins_accessed` / `note_ins_removed` had zero
    callers outside the crate's own tests, so `evict_batch`'s phase quotas
    (`policy.len()`) were always 0 and the evictor selected nothing. Added an
    `InListListener` trait in `noxu-tree` (the tree's analogue of JE's `INList`
    feeding the evictor's `LRUList`s) which `Evictor` implements. The tree now
    notifies the listener on the production paths: BIN/root creation in
    `Tree::insert` (JE `IN.fetchTarget`/initial build → `Evictor.addBack`),
    every BIN reached during `Tree::search` descent (JE access →
    `Evictor.moveBack`, add-if-absent so freshly split BINs register on first
    touch), and BIN prune in `Tree::prune_empty_bin` (JE node removal →
    `Evictor.remove`). `EnvironmentImpl::open_database` installs the `Evictor`
    as each database tree's listener and points the evictor's eviction walk at
    that tree.
  - **F2 — eviction never decremented the shared budget counter.** The
    evictor shares `cache_usage: Arc<AtomicI64>` with `Tree::memory_counter`;
    inserts `fetch_add` to it but eviction only *accounted* `bytes_evicted`
    and never subtracted, so the engine could never get back under budget by
    evicting. Added `Arbiter::release_memory` (clamped at `>= 0`) and call it
    from `do_evict_with_callbacks` after each batch — JE
    `IN.updateMemorySize(-bytes)` →
    `MemoryBudget.updateTreeMemoryUsage(-bytes)`.
  - Reproduce-first regression tests (`noxu-dbi`
    `evictor_f1_lru_lists_populated_by_production_inserts`,
    `evictor_f1_f2_eviction_reduces_cache_usage`): open a small-cache env,
    insert past the budget, evict, and assert the LRU lists grow, the evictor
    evicts/strips > 0 nodes, and `cache_usage` drops. Both FAIL against the
    pre-fix code (lists empty, 0 evicted, counter unchanged) and pass after.
  - Deferred to follow-on waves (F4): multi-database round-robin eviction —
    the evictor currently walks the last database tree installed; the
    single-database case is fully covered.

### Fixed (recovery — physical log truncation, JE-fidelity log audit)

- **Torn trailing log entry is now physically truncated at recovery**
  (`noxu-log` / `noxu-dbi`, log-audit F-1): `find_end_of_log` detected the last
  valid entry and repositioned the write cursor after it, but left the torn /
  half-written trailing bytes (and any higher-numbered orphan files) on disk —
  relying on overwrite-on-next-write. JE `RecoveryManager.setEndOfFile` →
  `FileManager.truncateLog` physically `ftruncate`s the file to the recovery
  point and deletes higher orphan files (descending, to avoid a log gap, SR
  [#19463]). Added `FileManager::truncate_single_file` / `truncate_log` and
  call them from `find_end_of_log` (read-write only). Regression test
  `test_find_end_of_log_physically_truncates_torn_tail` (fail-pre/pass-post).

### Fixed (lock-table config plumbing — follow-up to the DRIFT-2 fix)

- **`lock_n_lock_tables` now flows from the public API to the LockManager**
  (`noxu-db`): the prior DRIFT-2 commit added `DbiEnvConfig.n_lock_tables` but a
  `DbiEnvConfig` struct literal in `noxu-db` did not set it. Wired
  `EnvironmentConfig.lock_n_lock_tables` → `DbiEnvConfig.n_lock_tables` →
  `LockManager::with_config`, and aligned the public default to 64 (was a third
  inconsistent value, 16). The shard count is now consistent end-to-end.

### Fixed (lock manager — JE-fidelity, deep audit)

- **`rangeInsertConflict` now honors `sharesLocksWith`** (`noxu-txn`): JE
  `LockImpl.rangeInsertConflict` skips a RANGE_INSERT owner that shares locks
  with the waiter (`!ownerLocker.sharesLocksWith(waiterLocker)`); Noxu's
  `range_insert_conflict` dropped that clause, so a RESTART waiter could be
  spuriously kept blocked one extra cycle when a same-sharing-group locker held
  a RANGE_INSERT. Added `range_insert_conflict_with_sharing` /
  `release_with_sharing` and wired the production `LockManager::release` /
  `release_all_for_locker` to pass the share-group predicate. No correctness or
  isolation impact (transient blocking only). Test
  `test_range_insert_conflict_honors_sharing`.
- **`LOCK_N_LOCK_TABLES` config now wired** (`noxu-txn` / `noxu-dbi` /
  `noxu-engine`): the lock-table shard count was a hardcoded constant (64); the
  `LOCK_N_LOCK_TABLES` config parameter was defined but never read, and the
  engine reported a third inconsistent value (16) in its stats. The shard count
  is now an instance field set via `LockManager::with_config`, populated from
  `DbiEnvConfig.n_lock_tables` (default 64 — a documented deviation from JE's
  default of 1, for write concurrency); the engine stat reports the LIVE shard
  count. Tuning/observability fidelity only — lock semantics are identical for
  any fixed shard count. Test `test_with_config_shard_count_honored`.

### Added (replication — commit freeze latch primitive, D3)

- **`CommitFreezeLatch`** (`noxu-rep`, JE `CommitFreezeLatch`): a freeze
  primitive that holds VLSN advancement on a node for the duration of an
  election round so the VLSN/DTVLSN reported in a Paxos Promise does not move
  mid-election (`freeze` / `vlsn_event` / `await_thaw` / `clear_latch`, condvar
  -based, with the JE timeout and the older-proposal-ignored and
  older-event-does-not-thaw rules). The primitive is complete and unit-tested;
  wiring it into the replica replay path (`await_thaw` before VLSN advance) and
  the acceptor/learner (`freeze` on promise, `vlsn_event` on result) is a
  follow-on — until then VLSN can still advance mid-election (JE itself notes
  the latch is a "good faith effort", not a hard guarantee). Tests cover
  thaw-on-event, timeout, and the proposal-ordering guards.

### Fixed (replication — election ranking, D2)

- **Elections now rank by DTVLSN, not raw VLSN** (`noxu-rep`, D2): the election
  proposal ordering was `(vlsn, priority, term, name)`. JE ranks by
  `Ranking(major=DTVLSN, minor=VLSN)` (`MasterSuggestionGenerator.getRanking`)
  so the most *durable* node (highest VLSN replicated to a majority) wins over a
  node with a higher raw VLSN but an uncommitted tail — preventing a
  data-laggard or speculative-tail node from being elected and then losing
  those writes on a subsequent failover. `Proposal` gained a `dtvlsn` major key
  (0 = UNINITIALIZED → falls back to VLSN, JE's pre-DTVLSN behavior); the
  `ElectionProposal` wire message now carries `dtvlsn`; the election driver and
  acceptor thread the node's live DTVLSN (`get_dtvlsn`) through
  `run_election_with_phi_dtvlsn` / `run_acceptor_with_state`. Builds on the
  DTVLSN substrate (D7) and authoritative-master detection (D4). Tests
  `test_higher_dtvlsn_wins_over_higher_vlsn`,
  `test_dtvlsn_tie_falls_back_to_vlsn`, and the ElectionProposal wire
  round-trip.

### Added (replication — authoritative-master detection, D4)

- **`is_authoritative_master`** (`noxu-rep`, JE
  `ElectionQuorum.isAuthoritativeMaster`): returns true only when this node is
  the group master AND is still connected to enough electable replicas that,
  including itself, a SIMPLE_MAJORITY quorum is present
  (`(active_electable_replicas + 1) >= electable_total / 2 + 1`). A master on
  the minority side of a partition is non-authoritative — the building block
  for suppressing its `MASTER_RANKING` so the majority side can elect a fresh
  master without split-brain. Pure quorum logic extracted as
  `authoritative_quorum_met` for testing. Tests
  `test_authoritative_quorum_met`,
  `test_is_authoritative_master_requires_master_role`.

### Added (replication — DTVLSN substrate, D7 part 1)

- **In-memory Durable Transaction VLSN tracking** (`noxu-rep`): added the
  DTVLSN to `ReplicatedEnvironment` (JE `RepNode.dtvlsn`) — the highest VLSN
  known replicated to a majority of electable replicas. `get_dtvlsn`,
  advance-only `update_dtvlsn` (`AtomicLongMax.updateMax`), `set_dtvlsn`
  (replica path), and `update_dtvlsn_from_feeders` implementing JE
  `FeederManager.updateDTVLSN` (min across qualifying feeders, advance once a
  SIMPLE_MAJORITY ack-count exceeds the current value). Recomputed on every
  ack. This is the substrate the election ranking (D2) and authoritative-master
  detection (D4) require. The `TxnEndEntry` on-disk format already carries a
  `dtvlsn` field; populating it from the master's DTVLSN on commit and reading
  it back on the replica (so a restarted replica recovers its DTVLSN) is a
  follow-on cross-crate wave (noxu-dbi commit path ↔ noxu-rep), as is the
  null-txn `DTVLSNFlusher`. Tests `test_dtvlsn_update_max_advances_only`,
  `test_dtvlsn_majority_min_across_feeders`.

### Documented (known limitations surfaced to users)

- Added user-facing `known-limitations.md` rows for limitations already noted
  in code: DPL secondary indexes are in-memory and not transactional (DPL-1;
  the lower-level `noxu-db` `SecondaryDatabase` is atomic), collections
  iterators are snapshots not live cursors (COL-1), tuple string encoding is
  not wire-compatible with JE (TB-1, deliberate — Noxu uses a Rust-native
  format), and the replication HA protocol is incomplete (election ranking,
  authoritative-master partition detection, syncup matchpoint, DTVLSN,
  master-transfer — D2/D3/D4/D5/D7/D9): do not rely on automatic failover for
  correctness; operator-supervised failover only.

### Fixed (replication — network restore integrity)

- **Network restore had no per-file integrity check** (`noxu-rep`, D10): a
  truncated or bit-flipped log file transferred during a network restore was
  written to the replica's disk and accepted as valid, surfacing only later as
  a recovery-level CRC failure. The restore protocol now appends a CRC32
  trailer per file (JE `NetworkBackup` sends a `MessageDigest` with `FileEnd`;
  Noxu uses the project-wide `crc32fast`); the client recomputes the CRC while
  receiving and rejects (and removes) a file on mismatch. Applied to BOTH
  transfer paths — the raw-TCP `send_files_to`/`execute` and the dispatcher
  `payload`/`execute_via_dispatcher`. Regression test
  `test_restore_digest_detects_corruption`; the auto-bootstrap and dispatcher
  integration tests exercise the symmetric round-trip.

### Changed (replication — ack-quorum)

- **Durable-commit ack wait no longer spin-polls** (`noxu-rep`, D6): the master
  previously waited for replica acks with a sleep-poll loop (up to 20 ms added
  latency per durable commit, CPU spin). `AckTracker` now carries a `Condvar`;
  committers block in `wait_until_satisfied` and are woken the instant an ack
  lands (JE `FeederTxns.TxnInfo` uses a per-transaction `CountDownLatch.await`).
- **Non-electable acks no longer count toward durability quorum** (`noxu-rep`,
  D6): `record_ack` now drops acks from Monitor / Secondary / unknown nodes
  (JE `DurabilityQuorum.replicaAcksQualify` — only electable replicas qualify).
  Regression tests `wait_until_satisfied_wakes_on_ack`,
  `wait_until_satisfied_times_out_without_enough_acks`,
  `test_record_ack_from_non_electable_does_not_qualify`.

### Fixed (replication — VLSN range semantics)

- **`lastSync` / `lastTxnEnd` doc-comment inversion** (`noxu-rep`, D8): the
  `VlsnRange` field comments described `commit_vlsn` as the "sync matchpoint"
  and `sync_vlsn` as the "transaction end" — transposed from JE. JE
  `VLSNRange` keeps two distinct concepts: `lastSync` (highest sync-point VLSN,
  the matchpoint candidate) and `lastTxnEnd` (highest commit/abort VLSN, the
  rollback boundary). Corrected the field/getter semantics, added JE-faithful
  aliases `get_last_sync` / `get_last_txn_end`, and added
  `update_for_new_mapping` mirroring `VLSNRange.getUpdateForNewMapping`
  (entry-type dispatch so a Matchpoint advances `lastSync` ahead of
  `lastTxnEnd`). The syncup matchpoint protocol that consumes these fields
  remains a tracked parity gap (D5).
### Fixed (tree — compressor TOCTOU / production panic)

- **IC-1 — empty-BIN prune could remove a LIVE entry** (`noxu-tree`):
  `Tree::compress_bin`'s prune step read `now_empty` under a FRESH read lock
  taken *after* the compression write lock was dropped, then called
  `self.delete(&id_key)`, which re-descends by key. Between the `now_empty`
  read and the delete, a concurrent insert could repopulate the BIN, and
  `self.delete(&id_key)` then removed whatever LIVE entry matched `id_key` —
  tree corruption / lost write. Replaced with a new `Tree::prune_empty_bin`
  that re-descends to the specific empty BIN and, **under the parent IN write
  latch**, re-validates `n_entries == 0`, not-a-delta, and `cursor_count == 0`
  before removing the BIN's parent slot; if any check fails it removes NOTHING.
  This is the faithful port of JE `Tree.delete(idKey)` /
  `Tree.searchDeletableSubTree` (Tree.java ~line 755-800,
  `NodeNotEmptyException` / `CursorsExistException`) as called by
  `INCompressor.pruneBIN` (INCompressor.java ~line 502-510). Regression tests
  `test_ic1_prune_empty_bin_aborts_when_repopulated`,
  `test_ic1_prune_empty_bin_aborts_with_cursor`,
  `test_ic1_prune_empty_bin_succeeds_when_truly_empty` (fail-pre/pass-post).
- **IC-2 — `BIN::compress` aborted the process on a live cursor** (`noxu-tree`):
  `Bin::compress` had `assert!(self.n_cursors() == 0, "compress called with
  active cursors")`, which panics (aborts) in production. JE never panics here
  — `INCompressor.compress`/`pruneBIN` (INCompressor.java ~line 465-466, 587)
  checks `bin.nCursors() > 0` and REQUEUES the BIN for a later pass. Now
  `compress` returns `false` ("nothing compressed, try later") and leaves the
  BIN untouched when cursors are present. Regression test
  `test_ic2_compress_with_cursor_is_noop_not_panic` (fail-pre/pass-post).

### Documented (tree)

- **IC-3 — compressor BIN slot removal does not consult the lock manager**
  (`noxu-tree`): documented as a known limitation
  (`docs/src/operations/known-limitations.md`). The lock manager lives in a
  different crate (`noxu-txn`); the tree layer has no access to it. This is
  safe in the current design because the compressor only ever sees committed
  defunct slots (the dbi write path physically removes slots under the txn
  write lock; the only writer of `BinStub.known_deleted = true` is
  BIN-delta/recovery replay of committed deletes). A `ponytail:` code comment
  in `compress_bin` records the ceiling and upgrade path.

### Fixed (replication — split-brain)

- **Paxos Phase-2 acceptor admitted an unpromised higher term** (`noxu-rep`,
  D1): the election acceptor accepted a phase-2 `Accept` whenever its term was
  `>= promised` (and the phase-2 guard used `term >= phase1_term`). JE
  `Acceptor.process(Accept)` (Acceptor.java:210-211) rejects unless the
  Accept's proposal EQUALS the promised proposal
  (`promisedProposal.compareTo(accept.getProposal()) != 0` → Reject) — there is
  no implicit promise-bump on accept. The `>=` admitted a proposer that got a
  phase-1 promise at term T1 then sent a phase-2 Accept at T2 > T1 without a
  fresh phase 1, letting two proposers reach phase-2 quorum at different terms
  (classic split-brain). Now `try_accept` and the phase-2 guard require exact
  equality with the promised term. Regression tests
  `try_accept_higher_term_than_promise_rejected_split_brain_guard`,
  `test_acceptor_rejects_accept_at_unpromised_term`, and the
  `prop_acceptor_accept_contract` property model (corrected to JE semantics).

### Fixed (production-wiring gaps found by fix-verification audit)

- **key_prefixing lost on recovery** (`noxu-dbi`): `DatabaseImpl::set_recovered_tree`
  (the crash-reopen path) replaced the tree without re-applying the key_prefixing
  flag, so a `key_prefixing=true` database silently disabled prefix compression
  for all inserts after any reopen. Now re-applies the flag (JE
  DatabaseImpl.getKeyPrefixing survives recovery via persistent DB metadata).
  Regression test `test_set_recovered_tree_preserves_key_prefixing` (fail-pre/pass-post).
- **CLN-4 cleaner txn-window clamp was inert** (`noxu-dbi`): `EnvironmentImpl`
  wired the cleaner's tree-registry and utilization-tracker but NOT its
  `TxnManager`, so `do_clean`'s first-active-transaction clamp
  (`first_active_txn_file`) was always `None` — the cleaner could select files
  whose log entries an open transaction still needed (JE
  `UtilizationCalculator.getBestFile` clamps to `min(newestFile,
  firstActiveTxnFile)`). Now wires `with_txn_manager` onto the production cleaner.
  Regression test `gap8_production_cleaner_has_txn_manager_wired` (fail-pre/pass-post).
- Corrected stale `log_manager.rs` doc comments that still described the
  pre-fix "LWL covers pwrite64" design; the LWL is released before pwrite
  (DRIFT-1, already fixed) and the comments now describe the JE-faithful state.

### Fixed

- **B-tree DRIFT-1 — splitSpecial heuristic** (`noxu-tree`): Sequential-append
  and sequential-prepend workloads now use JE's `IN.splitSpecial` split-index
  selection. When all routing decisions during the top-down descent are
  leftmost (`AllLeft`, prepend) or rightmost (`AllRight`, append), the split
  index is forced to `1` or `n-1` respectively instead of `n/2`. The left BIN
  stays near-full after each split, cutting BIN count and write amplification
  roughly in half for sequential workloads while leaving random-insert balance
  unchanged.  New descent-tracking booleans `all_left_so_far` /
  `all_right_so_far` thread through `insert_recursive_inner` and
  `redo_insert_recursive_inner`.  Acceptance tests:
  `test_split_special_ascending_fewer_bins_than_midpoint`,
  `test_split_special_descending_fewer_bins_than_midpoint`,
  `test_split_special_random_inserts_stay_balanced`.
  Ref: `IN.java splitSpecial` ~line 4129, `Tree.java forceSplit` ~line 1907.

- **B-tree DRIFT-2 — idKeyIndex comment** (`noxu-tree`): The `split_child`
  rustdoc previously claimed `idKeyIndex` determines which half keeps the
  identifier key; the code always keeps the left half. The comment now
  accurately documents that left-only is a correct safe simplification under
  preemptive-split discipline, with a reference to `IN.java splitInternal`
  ~line 4172 for the full JE logic.

- **B-tree DRIFT-3 — key_prefixing flag** (`noxu-tree`): Noxu was always
  applying BIN key-prefix compression, ignoring the `DatabaseConfig.
  setKeyPrefixing` flag. Fixed: `Tree` now has a `key_prefixing: bool` field
  (default `false`, matching JE `KEY_PREFIXING_DEFAULT`). When `false`,
  `BinStub::insert_raw` stores full keys without any prefix; `split_child`
  skips `recompute_key_prefix` on both halves. Custom-comparator (sorted-dup)
  databases are unaffected. A `Tree::set_key_prefixing()` setter is provided;
  wiring from `DatabaseImpl` to `Tree` is a follow-up in `noxu-dbi`.  New
  method `BinStub::insert_raw`. Acceptance tests:
  `test_key_prefixing_false_stores_full_keys`,
  `test_key_prefixing_true_compresses_keys`,
  `test_key_prefixing_custom_comparator_no_prefix`.
  Ref: `IN.java computeKeyPrefix` ~line 2456.

- **B-tree DRIFT-4 — BIN-delta threshold (noxu-tree side)** (`noxu-tree`):
  `Bin::should_log_delta` was hardcoded to `dirty <= total / 4` (always 25%).
  JE uses the configurable integer formula
  `deltaLimit = (nEntries * binDeltaPercent) / 100`.  New method
  `Bin::should_log_delta_pct(bin_delta_percent: u8)` implements the JE
  formula exactly; `should_log_delta()` is kept as a backward-compatible
  no-arg wrapper calling `should_log_delta_pct(25)`.  **Note:** the
  `noxu-recovery::checkpointer` has a separate hardcoded
  `const TREE_BIN_DELTA: f64 = 0.25` — unifying that with the config
  parameter is a follow-up task (out of scope for this PR; noxu-recovery
  is off-limits).  Acceptance tests:
  `test_should_log_delta_pct_default_25`,
  `test_should_log_delta_pct_50`,
  `test_should_log_delta_pct_integer_rounding`,
  `test_should_log_delta_pct_vs_old_formula_at_pct30`.
  Ref: `BIN.java shouldLogDelta` ~line 1892.

- **B-tree DRIFT-5 — reconstituteBIN pre-compression + resize** (`noxu-tree`):
  `Bin::mutate_to_full_bin` now matches JE `BIN.reconstituteBIN` ~line 2383:
  (1) compress non-dirty deleted slots on the full BIN before applying the
  delta (handles slots compressed away after the last full write but before
  the delta); (2) count new insertions and resize the full BIN if
  `n_insertions + n_entries > max_entries`, preventing spurious
  `SplitRequired` errors and oversized BINs. New method `Bin::resize(new_max)`.
  Acceptance tests:
  `test_mutate_to_full_bin_resize_for_new_insertion`,
  `test_mutate_to_full_bin_resize_enlarges_bin`.
  Ref: `BIN.java reconstituteBIN` ~line 2383, `mutateToFullBIN` ~line 2195.

### Changed

- **TOMBSTONE_BIT (0x80) — documented as intentional Noxu extension**
  (`noxu-tree`, DRIFT-7): `TOMBSTONE_BIT` is NOT in JE `EntryStates.java`.
  Noxu uses it for blind-deletion tombstones (`ExtinctionScanner`). It is
  intentionally persisted (NOT in `TRANSIENT_BITS`) so tombstones survive
  checkpoints and can be reclaimed by the cleaner. A JE-format reader
  encountering 0x80 set will ignore it safely (JE processes state bits
  independently by masking). Expanded rustdoc on `TOMBSTONE_BIT` and
  `TRANSIENT_BITS` to record this analysis.

- **Cursor D1/D5 — delete cursor position + adjustCursorsForInsert** (`noxu-dbi`,
  `noxu-db`): After `cursor.delete()`, subsequent `Next`/`Prev` now returns
  the successor/predecessor rather than `NotFound`.  A new `PendingDeleted`
  cursor state retains the gap index (= former successor slot) after physical
  removal, matching JE `CursorImpl.deleteCurrentRecord()` PD-flag semantics.
  Also, `Get::Current` on a cursor whose slot was shifted by a concurrent
  insert now re-anchors correctly instead of returning `NotFound`/wrong key
  (CC-1 re-anchor extended to detect key mismatch at `current_index`).
  Acceptance tests: `d1_delete_then_next_returns_successor`,
  `d1_iterate_and_delete_all_records`, `d5_insert_before_positioned_cursor`.
  Ref: `CursorImpl.java adjustCursorsForInsert` ~line 997,
  `deleteCurrentRecord()` PD-flag, `getNext()` PD-check.

- **Cursor D2 — BOTH_RANGE on non-dup DB** (`noxu-dbi`): On a non-duplicate
  database, `SearchMode::BothRange` is now converted to `SearchMode::Both`
  (exact key+data match), matching JE `Cursor.java search()` conversion.
  Previously did a range search ignoring the `data` argument.
  Acceptance tests: `d2_both_range_non_dup_non_matching_data_returns_not_found`.
  Ref: `Cursor.java search()` BOTH_RANGE → BOTH conversion.

- **Cursor D3/D4 — KEYEMPTY for defunct slots** (`noxu-dbi`, `noxu-db`):
  `cursor.delete()` and `cursor.put(Put::Current)` on a slot already deleted
  by a concurrent operation now return `OperationStatus::KeyEmpty` instead of
  silently succeeding.  New `OperationStatus::KeyEmpty` variant added to the
  public API.  Acceptance tests: `d3_delete_on_defunct_slot_returns_key_empty`,
  `d4_put_current_on_defunct_slot_returns_key_empty`.
  Ref: `CursorImpl.java deleteCurrentRecord()`, `Cursor.java putCurrent()`
  KEYEMPTY paths.

- **Cursor D10 — SearchGte writes back found key** (`noxu-db`): Already
  implemented; added explicit acceptance test
  `d10_search_gte_writes_back_found_key` confirming the behavior.
  Ref: `Cursor.java getSearchKeyRange()` key input/output param.

- **Cursor D11 — putNoDupData on non-dup DB is an error** (`noxu-dbi`,
  `noxu-db`): `Put::NoDupData` on a non-duplicate database now returns
  `Err(OperationNotAllowed)` with a clear message, matching JE's
  `UnsupportedOperationException` from `Cursor.putNoDupData()`.
  Acceptance test: `d11_put_no_dup_data_on_non_dup_db_errors`.
  Ref: `Cursor.java putNoDupData()` non-dup guard.

- **Secondary D6/D7 — integrity errors on corrupt secondary index**
  (`noxu-db`): `insert_sec_key()` now raises `SecondaryIntegrityException`
  when a duplicate `(sec_key, pri_key)` pair is detected in a fully-populated
  index.  `delete_sec_key()` raises it when the `(sec_key, pri_key)` pair is
  missing.  Matches JE `SecondaryDatabase.java insertSecKey()`/`deleteSecKey()`
  integrity checks.  Acceptance tests: `d6_duplicate_sec_key_insert_raises_integrity_error`,
  `d7_missing_sec_entry_on_delete_raises_integrity_error`.

- **Secondary D8 — dirty-read missing primary skip** (`noxu-db`): Secondary
  cursors opened with `CursorConfig::read_uncommitted()` now return `NotFound`
  (skip the record) instead of raising `SecondaryIntegrityException` when the
  primary record is missing.  Matches JE `SecondaryCursor.java`
  `getWithPrimaryData()` dirty-read skip.  Acceptance test:
  `d8_dirty_read_missing_primary_skips_record`.

- **Secondary D9 — auto-maintenance removes old secondary key on overwrite**
  (`noxu-db`): Already implemented via `Database::put` fetching `old_data`
  before the write.  Acceptance test `d9_overwrite_changing_sec_key_removes_old_entry`
  added to confirm.

- **Secondary cascade delete double-delete fix** (`noxu-db`):
  `SecondaryDatabase::delete()` and `SecondaryCursor::delete()` no longer
  call `delete_all_for_primary` before `primary.delete()`.  The auto-hook
  registered with the primary handles secondary cleanup; the prior double-call
  triggered D7 errors on every cascade delete.

- **Part 5 — D12 dupsPutNoOverwrite concurrent lock**: Documented as a known
  gap.  JE's `BuddyLocker` next-key lock for concurrent `NoDupData` inserts
  is approximated by the existing synthetic-key lock + B-tree latch
  serialization.  Full BuddyLocker wiring deferred; see
  `docs/d12-dupsPutNoOverwrite-gap.md`.

 (`noxu-recovery`, `noxu-tree`,
  `noxu-dbi`): Previously the recovery redo pass discarded the dirty-IN map
  after building it, rebuilding user trees purely from committed LN replay.
  This diverged from JE's algorithm (`RecoveryManager.buildINs`/`recoverIN`/
  `recoverChildIN`). Three stages shipped:
  - **Stage 1** (DRIFT-1): Deserialise `InRecord.node_data` bytes and splice
    each IN/BIN into the in-memory tree using the JE three-case LSN currency
    check (`recoverChildIN`, `RecoveryManager.java` ~line 1412): slot LSN ==
    log LSN → noop; slot older → replace; slot newer → skip.
    Root INs use `recoverRootIN` semantics (insert if absent, replace if older).
    New `Tree::recover_in_redo`, `Tree::recover_root_bin`,
    `Tree::recover_child_bin`, `Tree::deserialize_upper_in`,
    `Tree::deserialize_bin`; new `InRedoResult` enum.
  - **Stage 2** (DRIFT-3/4): Sort dirty INs by level descending (root INs
    first) mirroring JE's `readRootINs`/`readNonRootINs` two-pass ordering.
    Filter provisional INs (`Provisional::Yes` always skipped;
    `Provisional::BeforeCkptEnd` replayed only when `CkptEnd.lsn > entry.lsn`;
    JE `INFileReader.isProvisional()`). Added `InRecord.is_provisional` field
    populated from entry-header flags 0x80/0x40.
  - **Stage 3** (DRIFT-10): BIN-delta reconstitution during IN-redo.
    `Tree::reconstitute_bin_delta(base_bytes, delta_bytes)` merges a delta
    onto its base full BIN and recomputes key prefix, implementing JE
    `BINDelta.reconstituteBIN`. Graceful degradation when the base is not
    in the scan range.
  - **Stage 4** (DRIFT-2 / T-F3): Re-enabling the `afterCheckpointStart` gate
    deferred. The gate requires loading baseline BINs from the checkpoint
    snapshot (JE loads user-DB BINs from the mapping tree); until that path
    exists the full LN scan range is kept for correctness.
  New crash tests: `in_redo_bin_flushed_by_checkpoint_survives_crash`,
  `in_redo_bin_delta_reconstituted_survives_crash`.
- **WAL Tier-1B Part 1 — LogBufferPool::write_dirty implemented (DRIFT-2)**
  (`noxu-log`): `LogBufferPool::write_dirty` was a no-op stub that reset
  `dirty_start`/`dirty_end` without writing any bytes.  Under buffer pressure
  `bump_and_write_dirty` would panic with "No free log buffers after flushing
  dirty buffers".  Now calls `FileManager::write_buffer_to_file` for each
  dirty buffer in the chain, matching JE `LogBufferPool.writeDirty` →
  `writeBufferToFile` → `fileManager.writeLogBuffer`.  `FileManager` is now
  wired into `LogBufferPool` at construction time (JE holds the same
  reference).  Acceptance test: `test_write_dirty_drains_ring_no_panic`.

- **WAL Tier-1B Part 3 — fsync closing file under LWL on file flip (DRIFT-3/7)**
  (`noxu-log`): On a file flip, the closing file was not fsynced before the
  new file received writes.  `get_write_buffer(flipped=true)` now calls
  `FileManager::sync_log_end_and_finish_file()` (fsync + LRU cache eviction)
  after `bumpAndWriteDirty` and before `advanceLsn` advances
  `current_file_num`, restoring JE's invariant (`FileManager.
  syncLogEndAndFinishFile`, line 2077).  Also fixes the LSN-advance ordering
  inversion: `set_last_position` is now called AFTER `get_write_buffer`
  returns (JE serialLogWork step 4 after step 3).  Crash test:
  `test_file_flip_fsync_ordering_crash_recovery`.

- **WAL Tier-1B Part 2 — LWL released before disk I/O (DRIFT-1)**
  (`noxu-log`): `log_internal` held the LWL through `segment.put` (bytes
  copy) and `flush_sync` held it through `pwrite64`, serialising all
  concurrent committers on the syscall.  The LWL now covers only: LSN
  assignment, `shouldFlipFile`/`calculateNextLsn`, `getWriteBuffer`,
  `advanceLsn`, buffer `allocate` + `registerLsn` — then releases.  Bytes
  copy (`segment.put`) and all I/O (pwrite, fdatasync) happen outside the
  LWL, matching JE `LogManager.serialLogWork` (logWriteMutex released before
  `LogBufferSegment.put`).  Fixes the false "correct logWriteMutex design"
  comment.  Added `FileManager::write_buffer_to_file(file_num, ...)` for
  correct file targeting when dirty buffers are written after a flip.
  Acceptance test: `test_concurrent_log_internal_latch_released_before_put`.

  JE references (all three parts): `LogManager.serialLogWork`,
  `LogBufferPool.writeDirty/getWriteBuffer`, `FileManager.
  syncLogEndAndFinishFile`.

- **CC-4 residual — per-tree provisional-flag coordination** (`noxu-recovery`,
  `noxu-evictor`): The prior CC-4 fix introduced a single `AtomicI32`
  `checkpoint_max_flush_level` holding the **global** maximum dirty upper-IN
  level across all trees.  In a multi-database environment where tree A has no
  dirty upper INs and tree B does, a dirty BIN evicted from tree A was logged
  `Provisional::Yes` (because `node_level < global_max_level` from tree B).
  However, the checkpoint writes no non-provisional ancestor for tree A, so
  recovery discards the provisional BIN → if a crash occurs before the next
  checkpoint re-logs that BIN, tree A's mutation is **silently lost**.

  Root cause: JE's `DirtyINMap` holds a `Map<DatabaseImpl, Integer>`
  (`highestFlushLevels`) keyed per-`DatabaseImpl`; `getHighestFlushLevel(db)`
  returns `IN.MIN_LEVEL` (0) for databases absent from the map, making the
  comparison false → `Provisional.NO`.  Noxu collapsed this to one global
  value, breaking the per-tree guarantee.

  Fix (option A — faithful): replace `checkpoint_max_flush_level: AtomicI32`
  with `checkpoint_flush_levels: Mutex<HashMap<u64, i32>>`.  Only trees that
  have dirty upper INs get an entry.  `get_eviction_provisional(db_id,
  node_level)` looks up the tree's level; absent entry → 0 → `Provisional::No`.
  `CheckpointGuard::drop` clears the map before clearing `in_progress`.
  Evictor passes `self.db_id` to `get_eviction_provisional`.

  JE ref: `DirtyINMap.coordinateEvictionWithCheckpoint` /
  `DirtyINMap.getHighestFlushLevel` (per-`DatabaseImpl` lookup).

  Acceptance test (fail-pre/pass-post):
  `test_cc4_residual_tree_a_no_upper_ins_yields_provisional_no` — two trees,
  tree A absent from flush-levels map, tree B present; asserts tree A's BIN
  gets `Provisional::No`, tree B's BIN gets `Provisional::Yes`.
  Updated existing tests: `test_cc4_below_max_flush_level_yields_provisional_yes`,
  `test_cc4_at_or_above_max_flush_level_yields_provisional_no`,
  `test_cc4_guard_resets_max_flush_level`, `test_checkpoint_guard`.
- **R3 — comparator-aware BIN navigation in `get_next_bin` / `get_prev_bin`** (`noxu-tree`):
  `get_adjacent_bin_attempt` was a `static fn` without comparator access, so
  the IN-level descent used raw byte `<=` instead of the configured custom
  comparator.  For sorted-dup / secondary-index databases where comparator order
  ≠ byte order this produced wrong adjacent-BIN lookups and incorrect cursor
  iteration across BIN boundaries.  Fixed by converting to `&self` methods and
  routing through `upper_in_floor_index` (comparator-aware, St-H4 binary search).
  JE: `Tree.getNextIN` / `Tree.getPrevIN` use comparator-aware `IN.findEntry`.

- **R4 — comparator-aware descent in `cursor_impl::find_bin_for_key`** (`noxu-dbi`):
  The cursor's own IN-routing helper used raw byte `<=` in its linear floor scan.
  All seven call-sites now receive `tree.get_comparator()` and the comparison
  honours the custom comparator.  Exposed `Tree::get_comparator(&self)` for this.
  JE: `CursorImpl` descent helpers delegate to `IN.findEntry` (comparator-aware).

- **TXN-1 — unconditional deadlock re-check in `lock_with_sharing_and_timeout`** (`noxu-txn`):
  The sharing-path wait loop only re-ran deadlock detection on `timed_out.timed_out()`
  (every 50 ms slice) and used stale owner IDs captured at Phase 1.  The plain
  `lock_with_timeout` path already re-checked after every wakeup with fresh owner IDs;
  now `lock_with_sharing_and_timeout` mirrors it exactly.
  JE: `LockManager.waitForLock` checks deadlock every loop iteration unconditionally.

- **TXN-4 — `lock_ln` validates txn state even for read-uncommitted** (`noxu-dbi`):
  `CursorImpl::lock_ln` early-returned for read-uncommitted cursors without calling
  `guard.lock()`, so an `Aborted` or `MustAbort` txn doing a dirty read was not
  caught and silently returned stale data.  Now calls `guard.lock(lsn,
  LockType::None, false)` before returning; `LockType::None` runs `check_state`
  inside `Txn::lock` and returns `NoneNeeded` immediately (no real lock acquired).
  Also added `NoneNeeded` early-return guard in `Txn::lock` to prevent phantom
  `read_locks` tracking entries.
  JE: `CursorImpl.lockLN` calls `locker.lock(lsn, LockType.NONE, ...)` even for
  dirty reads so `checkState`/`checkPreempted` runs.

- **TXN-5 — `HandleLocker` shares locks with non-transactional buddy** (`noxu-txn`):
  `HandleLocker::with_buddy` previously set `share_with_txn_id = None` when the
  buddy was non-transactional (dropping the buddy entirely), so
  `shares_locks_with` always returned `false` for non-txn buddies.  Added
  `share_with_non_txn_id` field; `with_buddy` now stores the buddy ID in the
  correct field; `shares_locks_with` checks both.
  JE: `HandleLocker.sharesLocksWith` checks `shareWithNonTxnlLocker` by identity.

- **TXN-6 — documented `select_victim` vs JE anti-livelock rationale** (`noxu-txn`):
  Added rustdoc to `DeadlockDetector::select_victim` explaining the Noxu
  deterministic "fewest locks then youngest" criterion and the JE
  `DeadlockChecker.chooseTargetedLocker` pseudo-random choice (anti-livelock
  on repeated identical deadlocks).  No code change; both strategies are correct.
- **CLN-FAITHFUL — restore JE `selectFileForCleaning` structure; cleaner is no longer inert** (`noxu-cleaner`, `noxu-dbi`):
  The live `do_clean` path previously called the FIFO-only `select_file_for_cleaning()`
  (queue drain) and never reached the utilization-scoring (getBestFile) path.
  The cleaner was inert in production: it only cleaned files if they were
  manually enqueued via `add_file_to_clean`.

  This fix faithfully re-ports four JE components:

  - **`FileSelector::select_file_for_cleaning` unified** (Part 1):
    New method matching JE `FileSelector.selectFileForCleaning`
    (FileSelector.java ~line 170): drains TO_BE_CLEANED queue first
    (JE ~line 175), then falls through to `select_file_for_cleaning_with_policy`
    (= `UtilizationCalculator.getBestFile`, JE ~line 184).
    Old FIFO-only variant renamed to `select_from_queue` (public helper).
    Added `remove_file_from_cleaning` (CLN NEW-3, JE FileSelector.removeFile
    ~line 325): removes a file after a two-pass skip so it is not rescanned.

  - **`UtilizationProfile::get_file_summary_map`** (Part 2):
    Faithful port of JE `UtilizationProfile.getFileSummaryMap(bool)`
    (UtilizationProfile.java ~line 210): merges the in-memory cached
    `FileSummary` entries with live `UtilizationTracker.TrackedFileSummary`s
    when `include_tracked=true`, including tracker-only files not yet in
    the profile map.
    `Cleaner` now holds `utilization_profile` + `utilization_tracker`;
    wired in `environment_impl.rs` symmetric to `LockManager`.

  - **`Cleaner::do_clean` matches JE `FileProcessor.doClean`** (Part 3):
    Rewritten to reproduce JE FileProcessor.doClean (FileProcessor.java
    ~line 317):
    1. Build `fileSummaryMap = profile.getFileSummaryMap(true, tracker)` before loop.
    2. Loop: `processPending()` → refresh map on iterations > 0 (CLN-13) →
       unified `select_file_for_cleaning` (autonomous, no manual enqueue needed) →
       two-pass check (CLN-5, now uses `remove_file_from_cleaning`) →
       `processFile` → `markFileCleaned`.
    CLN-1/2/3/4/5/13/14, X-5 checkpoint barrier all preserved.

  - **CLN NEW-4 — real expiration_time in `decode_ln_entries_from_file`** (Part 4):
    InsertLN/UpdateLN/InsertLNTxn/UpdateLNTxn entries now carry
    `expiration_time: ln.expiration as u64` (hours since epoch, CLN-10)
    instead of the hardcoded `0`.
    JE: `FileProcessor.processFile` reads `lnEntry.getExpiration()` (~line 1004).
    The two-pass TTL-adjusted utilization now sees real expired bytes.

  Acceptance tests added: `autonomous_selection_from_profile_without_manual_enqueue`
  (FAIL-PRE / PASS-POST), `fifo_queue_drained_before_profile_scoring`,
  `get_file_summary_map_merges_tracker_data`, `remove_file_from_cleaning_does_not_reenqueue`.

- **CLN-4 (wiring) — first-active-transaction file clamping now live** (`noxu-cleaner`):
  `Cleaner::do_clean` now reads `TxnManager::get_first_active_lsn()` and skips
  files whose `file_number >= first_active_txn_file`, preventing the cleaner
  from processing files still inside an open transaction's log window.
  Added `with_txn_manager(Arc<TxnManager>)` builder.  The clamping logic
  existed in `select_file_for_cleaning_with_profile_and_txn` but was dead
  in the production path; now wired.
  JE: `UtilizationCalculator.getBestFile` first-active clamp.

- **CLN-5 — two-pass cleaning correctly skips over-utilized files** (`noxu-cleaner`):
  When `required_util >= 0`, `do_clean` calls `two_pass_check` which
  scans the file, computes `recalcUtil = (obsolete + expired) / total`,
  and skips cleaning if `recalcUtil > required_util`.  Previously
  `force_cleaning = true` was set instead, causing over-cleaning.
  JE: `FileProcessor.doClean` revisalRun two-pass block (~line 420–465).

- **CLN-10 — `LnInfo.expiration_time` unit corrected to hours** (`noxu-cleaner`):
  The field was documented as "milliseconds since epoch" but the correct
  unit (matching `ExpirationTracker`, the log format, and St-H6's
  hours-only TTL invariant) is **hours since epoch**.  No live runtime
  mismatch existed (`expiration_time` is always 0 in the current live path),
  but the wrong doc would have caused 3600× errors if the field were
  populated.  Both `LnInfo` and `ExpirationTracker` now explicitly document
  the hours unit.

- **CLN-12 — periodic `process_pending` now runs during file processing** (`noxu-cleaner`):
  The periodic hook in `FileProcessor::process_file` previously drained
  the look-ahead cache instead of calling `process_pending`.  It now
  invokes a `process_pending_fn` callback (set by `Cleaner::process_single_file`
  via `ProcessPendingCtx`) every `PROCESS_PENDING_EVERY_N_LNS` entries,
  matching JE's `FileProcessor.processFile` behavior (~line 1004–1005).
  Cache drain is now correctly triggered only on cache-full or end-of-file.

### Added

- **CLN-6 — three-tier file selection policy** (`noxu-cleaner`):
  `FileSelector::select_file_for_cleaning_with_policy` adds:
  1. Global gate: `predicted_total_threshold` — if `predictedMinUtil >= threshold`,
     no file is selected.
  2. Per-file primary threshold: `min_utilization_pct` (existing).
  3. Per-file second tier: `min_file_utilization_pct` (JE `minFileUtilization`);
     effective threshold is `min(primary, second_tier)` in normal mode.
  `force_cleaning` bypasses all tiers.  Added `compute_predicted_min_util`
  helper.
  JE: `UtilizationCalculator.getBestFile` ~lines 174–425.

- **CLN-9 (partial) — per-file `ExpirationProfileStore`** (`noxu-cleaner`):
  `ExpirationProfileStore` (a `HashMap<u32, ExpirationTracker>`) is now
  implemented and wired into `two_pass_check`.  The store accumulates
  per-file expiration data from two-pass dry runs, improving future
  TTL-adjusted utilization scoring.  In-memory only; persistence across
  crashes is deferred (see CLN-11 in known-limitations.md).
  JE: `ExpirationProfile.putFile` / `removeFile` / `getExpiredBytes`.

- **CLN-13 — select-one/process-one loop** (`noxu-cleaner`):
  `do_clean` now selects and processes one file at a time (instead of
  batch-selecting then processing).  This ensures the file summary map
  is re-evaluated after each cleaned file, matching JE semantics.
  JE: `FileProcessor.doClean` loop (~line 386).

- **CLN-14 (partial) — `wakeupAfterNoWrites` callback** (`noxu-cleaner`):
  Added `Cleaner::with_checkpoint_wakeup_fn(Arc<dyn Fn()>)`.  When set,
  the callback is invoked after each successful cleaning pass, allowing
  the engine to trigger a prompt checkpoint so cleaned files are deleted
  quickly.  The noxu-engine wiring is deferred (see known-limitations.md).
  JE: `FileProcessor.doClean` ~line 290.

- **Known limitations documented** (`docs/src/operations/known-limitations.md`):
  Added rows for CLN-8 (`FilesToMigrate`/`forceCleanFiles` not implemented),
  CLN-11 (`UtilizationProfile` not persisted), CLN-9 partial persistence
  deferral, and CLN-14 engine wiring deferral.

- **TXN-2 — serializable-active counter now wired** (`noxu-txn`, `noxu-db`):
  `TxnManager::register_serializable()` is now called from
  `Environment::begin_transaction()` whenever the transaction config
  requests serializable isolation, and `unregister_serializable()` is
  called from `Transaction::unregister_inner_txn()` on every terminal path
  (commit, abort, `resolved_commit_after_prepare`,
  `resolved_abort_after_prepare`). Mirrors JE `TxnManager.registerTxn` /
  `unRegisterTxn` `nActiveSerializable` logic. Pre-fix,
  `are_other_serializable_transactions_active()` always returned false
  regardless of how many serializable transactions were live.
  Acceptance tests: `txn2_serializable_counter_commit`,
  `txn2_serializable_counter_abort`, `txn2_non_serializable_counter_unaffected`,
  `txn2_mixed_serializable_and_plain` (fail-pre: counter always 0;
  pass-post: counter tracks live serializable txns exactly).
  `TxnStats` / `TxnStatsSnapshot` gain `n_active_serializable` field.

- **TXN-3 — explicit txns unregister from TxnManager (T-F5 verification)**:
  T-F5 (`fix/checkpoint-user-bins`) already wired `unregister_inner_txn` at
  all four terminal paths in `Transaction`. Confirmed: `all_txns` drains to
  zero and `n_commits`/`n_aborts` are accurate. Test
  `txn3_all_txns_drains_to_zero_commit_and_abort` (fail-pre: `all_txns` grew
  without bound; pass-post: 0 after all explicit txns finish).

- **CLN-1 — pending LN gating prevents data-loss file deletion** (`noxu-cleaner`):
  `FileSelector` now tracks LNs that could not be migrated because their BIN slot
  was locked by a concurrent writer (`pending_lns: HashMap<Lsn, LnInfo>`,
  `pending_dbs: HashSet<DbId>`, `any_pending_during_checkpoint: bool`), faithful
  to JE `FileSelector.java` lines 133–522.  When `process_found_ln` returns
  `Locked`, `FileProcessResult::locked_lns` captures the entry and the cleaner
  registers it via `add_pending_ln`.  The checkpoint barrier respects
  `any_pending_during_checkpoint`: if pending items existed during the checkpoint
  window, CLEANED files advance only to CHECKPOINTED (requiring another
  checkpoint) rather than directly to FullyProcessed.  `update_processed_files`
  promotes CHECKPOINTED → FullyProcessed the moment the pending set drains.
  `Cleaner::process_pending` retries locked LNs at the start of each cleaning
  pass (JE `Cleaner.processPending`).  Without this fix, a file whose live LN
  could not be migrated would eventually be deleted, leaving a dangling BIN slot
  after a crash (silent data loss).
  Acceptance tests: `cln1_pending_ln_gates_file_deletion`,
  `cln1_no_pending_lns_fast_path_one_checkpoint`,
  `cln1_pending_ln_added_mid_checkpoint_keeps_file_blocked`,
  `test_process_checkpoint_end_with_pending_needs_two_checkpoints`.

- **CLN-3 — `put_back_file_for_cleaning` / finally-equivalent** (`noxu-cleaner`):
  If `process_single_file` errors or is interrupted (non-completed result), the
  file is now returned to `TO_BE_CLEANED` via `FileSelector::put_back_file_for_cleaning`
  instead of remaining stuck in `BEING_CLEANED` forever.  Matches JE
  `FileProcessor.java` doClean() `finally` block (~lines 591–593).
  Acceptance tests: `cln3_failed_processing_puts_file_back_for_retry`,
  `cln3_put_back_noop_if_not_being_cleaned`.

- **CLN-2 — `fully_processed_files` snapshot in checkpoint state** (`noxu-cleaner`):
  `CheckpointStartCleanerState` now captures both CLEANED and FULLY_PROCESSED
  file sets (JE `FileSelector.getFilesAtCheckpointStart` snapshots both).
  `Cleaner::get_checkpoint_start_state()` calls `process_pending()` before taking
  the snapshot so avoidably-pending LNs are drained first (CLN-7 addressed
  alongside CLN-2).  The checkpointer uses `get_checkpoint_start_state()` instead
  of calling `get_checkpoint_state` directly.  When no pending items exist during
  a checkpoint, CLEANED files advance to FullyProcessed in a single checkpoint
  (JE fast-path: `else { makeReservedFiles(cleanedFiles) }`).  The two tests that
  encoded the old incorrect two-checkpoint-always behavior were updated.
  Acceptance tests: `cln2_checkpoint_state_captures_fully_processed_files`,
  `cln2_fully_processed_files_always_safe_to_delete`,
  `cln2_two_checkpoint_barrier_only_needed_when_pending`.

- **CLN-4 — first-active-txn file clamping in file selection** (`noxu-cleaner`):
  `FileSelector::select_file_for_cleaning_with_profile_and_txn` clamps the file
  selection window to `effective_newest = min(newest_file, first_active_txn_file)`
  before computing `last_file_to_clean`, so files within an open transaction’s
  log window are not selected for cleaning.  Matches JE
  `UtilizationCalculator.getBestFile`’s `firstActiveFile` clamping.
  The existing `select_file_for_cleaning_with_profile` is now a convenience
  wrapper passing `first_active_txn_file = None`.
  Acceptance tests: `cln4_long_running_txn_prevents_cleaning_within_active_window`,
  `cln4_txn_window_excludes_best_candidate`.
- **CC-5 — Per-latch read-hold counter** (`noxu-latch`): the global
  `READ_HOLD_COUNT` thread-local was shared across all `SharedLatch`
  instances, so holding a read guard on latch L1 and acquiring a read guard
  on a different latch L2 on the same thread triggered a false-fatal
  "already held in shared mode" panic.  Fixed by replacing the global
  `Cell<u32>` with a `HashMap<latch_address, u32>` so only same-latch
  reentrancy is blocked — matching JE `ReentrantReadWriteLock.getReadHoldCount()`
  per-lock semantics (`SharedLatchImpl`).  The read-to-write upgrade deadlock
  check is also now per-latch.  Tests: `test_two_independent_shared_latches_no_panic`
  (fail-pre: panic; pass-post: ok), `test_same_latch_shared_reacquire_still_panics`,
  `test_same_latch_read_to_write_still_panics`, `test_read_l1_write_l2_no_panic`.

- **CC-2 — Coupled descent in `first_entry_at_or_after_with_index`**
  (`noxu-tree`): the method did `arc.read().is_bin()` (lock acquired and
  released) then a second `arc.read()` on the next line — a window in which a
  concurrent split could promote the node (BIN→upper IN) or move the sought
  key to a new sibling, yielding a false "not found".  Fixed by using the
  same `read_arc()` hand-over-hand pattern as every other descent method
  (`search`, `first_entry_at_or_after`, `get_first_node`, `get_last_node`,
  `get_adjacent_bin_attempt`).  JE reference: `Tree.searchSubTree` /
  `Tree.search` in `com/sleepycat/je/tree/Tree.java`.  Tests:
  `test_split_boundary_key_found`, `test_key_at_exact_split_point_found`,
  `test_returned_index_matches_slot`, `test_stress_concurrent_splits`.

- **CC-3 — JE-correct daemon shutdown order** (`noxu-engine`): the previous
  shutdown join order was evictor → cleaner → checkpointer.  JE
  `EnvironmentImpl.shutdownDaemons` requires cleaner → checkpointer → evictor
  ("Cleaner has to be shutdown before checkpointer because former calls the
  latter"; the evictor must remain available to flush dirty nodes until the
  final checkpoint completes).  Fixed by reordering the joins to match JE
  exactly.  Tests: `test_cc3_shutdown_order_cleaner_checkpointer_evictor`
  (uses blocking barriers to make a wrong order deadlock-deterministic),
  `test_cc3_shutdown_no_deadlock_bounded_time`.

- **Checkpointer now flushes all open user-database BINs** (`noxu-recovery`),
  not just the internal `primary_tree`. Previously a checkpoint walked only
  the primary tree, so dirty BINs in user databases were never written at
  checkpoint time — the checkpoint did not capture committed user data, which
  is why recovery had to full-scan the log and why bounded recovery (T-F3) was
  unsafe. The checkpointer now enumerates every open user-database tree from
  the shared db-trees registry and flushes each tree's dirty BINs + upper INs
  (faithful to JE's `Checkpointer.processINList` walking the env-wide INList).
  Regression test `stage1_checkpoint_stats_show_user_db_bins_flushed`
  (FAIL-PRE: 0 user BINs flushed on the old code / PASS-POST) plus
  `stage1_user_db_data_survives_checkpoint_and_recovery` and the
  multiple-database variant.
- **T-F4 — `TxnManager::update_first_lsn` is now wired** from the cursor
  write path, so `get_first_active_lsn()` returns the real oldest-active
  transaction LSN (JE `Txn.firstLoggedLsn`). The value is recorded but the
  recovery-scan consumer (T-F3) remains deferred: bounding the scan at a
  non-zero `first_active_lsn` requires recovery to pre-load checkpointed BINs
  before redo (P-2), which is not yet implemented. `CkptEnd.first_active_lsn`
  therefore still records `Lsn::new(0,0)` (full scan) — correct and safe.
  Test `stage2_txn_manager_records_first_active_lsn`; the open-txn-spanning-
  checkpoint crash test continues to pass.

- **CC-1 / D-2 — cursor correctness on BIN split**: a cursor positioned in the
  upper half of a BIN (index ≥ split_index) that split under it would silently
  skip all records in the new sibling that follow the cursor's slot.
  `retrieve_next` now detects a split-induced stale position
  (`current_index ≥ bin.entries.len()`) and re-anchors the cursor to the
  correct BIN via a tree search before advancing.  This is functionally
  equivalent to JE's eager `BIN.adjustCursors` (BIN.java:883, called from
  IN.java:4259) and produces the same final state without requiring
  `noxu-tree` to hold live cursor references.
  Regression tests `test_cc1_cursor_repositioned_after_bin_split_upper_half`
  and `test_cc1_cursor_stays_in_old_bin_after_split` cover both cursor-position
  cases and demonstrate fail-pre / pass-post behaviour.

- **CC-6 — evictor non-blocking latch + cursor-pin recheck** (`noxu-evictor`):
  `flush_dirty_node_to_log` and `strip_lns_from_node` previously called
  `node_arc.write()` (blocking write latch) after taking a metadata snapshot
  without holding the lock, stalling the evictor thread under cursor read
  pressure and allowing the memory budget to grow unbounded. Additionally,
  no cursor-count re-validation was performed under the lock, so a cursor
  that pinned a BIN between the pre-lock snapshot and the write-latch
  acquisition could cause a pinned BIN to be evicted or stripped.
  Fix: a new `find_node_arc_nonblocking` helper uses `try_read()` at every
  tree level; `flush_dirty_node_to_log` and `strip_lns_from_node` now use
  `try_write()` (non-blocking, JE `latchNoWait`-style) and re-check
  `cursor_count > 0` under the lock before proceeding. If the latch is
  contested or the node is pinned, the node is put back into the eviction
  list rather than blocking.
  JE ref: `Evictor.java` `isPinned()` + `latchNoWait`.
  Acceptance tests: `test_cc6_flush_nonblocking_when_write_held`,
  `test_cc6_strip_nonblocking_when_write_held`,
  `test_cc6_cursor_pin_recheck_under_lock_strip`,
  `test_cc6_cursor_pin_recheck_under_lock_flush`.

- **CC-4 — evictor provisional-flag coordination** (`noxu-evictor`,
  `noxu-recovery`): `flush_dirty_node_to_log` logged every evicted BIN as
  `Provisional::No`, even during a checkpoint. If the checkpoint crashed
  before writing `CkptEnd`, recovery treated the evictor's non-provisional
  BIN entry as authoritative even though the checkpoint did not complete.
  Fix: `Checkpointer` gains a new `AtomicI32` field
  `checkpoint_max_flush_level` (published by `flush_upper_ins_internal`
  before logging; reset to 0 by `CheckpointGuard::drop`). The new
  `Checkpointer::get_eviction_provisional(node_level)` returns
  `Provisional::Yes` when a checkpoint is in progress and the node is below
  the max flush level, `Provisional::No` otherwise. `Evictor` accepts an
  optional `Arc<Checkpointer>` via `with_checkpointer()`; when wired,
  `flush_dirty_node_to_log` calls `get_eviction_provisional` instead of the
  hardcoded `Provisional::No`.
  JE ref: `Checkpointer.coordinateEvictionWithCheckpoint` /
  `DirtyINMap.coordinateEvictionWithCheckpoint`.
  Acceptance tests: `test_cc4_no_checkpoint_in_progress_yields_provisional_no`,
  `test_cc4_below_max_flush_level_yields_provisional_yes`,
  `test_cc4_at_or_above_max_flush_level_yields_provisional_no`,
  `test_cc4_guard_resets_max_flush_level`, `test_cc4_evictor_wires_checkpointer`.
## [v4.0.0] — 2026-06-04

Major release. It completes the production-readiness review remediation
(every Critical and High blocker fixed or honestly resolved) and the
WAL-scanner replication auto-feed (C-C2b). The version is **4.0.0** rather
than 3.3.0 because, under the project's strict-SemVer-at-v3.0+ policy, one
breaking public-API change (R-F04) landed since v3.2.0 and mandates a major
bump.

### Breaking Changes

- **`noxu-xa`: `XaEnvironment::get_transaction()` now returns
  `Arc<Transaction>` instead of `&Transaction`** (R-F04 soundness fix —
  see the *Fixed (soundness)* section below). The previous `&Transaction`
  borrowed into the XA branch map and could dangle if a protocol-violating
  `xa_rollback`/`xa_commit` freed the transaction concurrently. Returning an
  `Arc<Transaction>` keeps the transaction alive independently of the map,
  removes the only `unsafe` in the crate (`noxu-xa` now carries
  `#![forbid(unsafe_code)]`), and is the sole source-incompatible change in
  this release. **Migration:** call sites that passed the result as
  `Option<&Transaction>` now write `Some(&*txn)`. See
  `docs/src/getting-started/migrating.md`.

The on-disk log format adds an optional VLSN-tagged entry header for
replicated commits (C-C2b) and the v3 file-header CRC32 (St-C3); both are
backward compatible — standalone, non-replicated environments write
byte-unchanged 14-byte entry headers, and legacy v2 files remain readable.
No data migration is required.

### Documentation (review-item honesty: T-F3, T-F4, St-H1, St-H3)

- **T-F3 / T-F4** reclassified from OPEN to **won't-fix / documented**.
  Recovery already uses `CkptEnd.first_active_lsn` as its scan boundary
  (hard-coded to `0,0` = full scan = correct but unbounded). Bounding it at a
  real `first_active_lsn` is **unsafe** under the current checkpointer, which
  flushes only the internal `primary_tree` and never user-database BINs:
  committed LNs before `first_active_lsn` would be silently dropped on
  recovery (the St-H6 Site 2 data-loss class). `TxnManager::update_first_lsn`
  and `get_first_active_lsn` rustdoc now state the machinery is intentionally
  unwired and why; `get_first_active_lsn()` always returns `NULL_LSN` today.
  No behavioural change — full-scan recovery is the correct, safe default.
- **St-H1 / St-H3** (mixed on-disk endianness) confirmed **documented**:
  `file_header.rs` now scopes the `byte_order = 0x00` marker to the
  file-header fields only (entry headers are little-endian, some payloads
  big-endian) and cross-references `docs/src/reference/on-disk-format.md`,
  whose "Endianness" table already specifies each layer.

### Performance

- **St-H2 — Evictor O(tree) node-size search eliminated** (`noxu-evictor`):
  `do_evict` previously performed two independent root-down O(tree) searches
  per eviction candidate — one for `NodeEvictionInfo` and a second for the
  in-memory byte size — making eviction O(n·batch) for a tree with n nodes.
  The new `find_node_full` helper does a **single** root-down walk that
  extracts eviction metadata, the in-memory byte count, and the node `Arc`
  together.  `do_evict` now caches the size in a `RefCell<HashMap>` during
  the info walk and drains it O(1) when `node_size_fn` is called, eliminating
  the second tree walk entirely.  The three prior separate recursive helpers
  (`find_node_info_recursive`, `find_node_size_recursive`,
  `find_node_arc_recursive`) have been removed.
  Size formula, eviction policy, and memory-budget accounting are unchanged.
  See the 2026 review for details.

### Fixed (data-loss correctness — St-H6, two sites)

- **St-H6 Site 1 — Silent data-loss on BIN split when records have TTL** (`noxu-tree`):
  `Tree::split_child` hardcoded `expiration_in_hours: false` on the new
  right-half sibling BIN instead of inheriting the flag from the splitting
  BIN.  Because every public TTL write path (`WriteOptions::with_ttl` /
  `with_expiration`) stores `expiration_time` as **hours** since the Unix
  epoch, the right-sibling entries' hours-granularity values (~495 000 in
  2026) were compared against `current_time_secs()` (~1.78 billion) by
  `is_expired(…, false)` and treated as if they had expired in January 1970.
  Any key that landed in the right half of a split returned `NotFound` for
  the remainder of the environment's lifetime — **128 out of 256 TTL records
  were silently lost in the benchmark scenario**.

  Fix: capture `b.expiration_in_hours` from the splitting BIN before
  `drop(child_guard)` and pass it to the sibling constructor.  Also corrected
  the three other hardcoded-`false` sites (initial-BIN constructors in
  `insert` / `redo_insert`, and a test-only BIN in `checkpointer.rs`) to
  `true`, matching `tree.rs:980` and the `deserialize_full` default.
  Added a `debug_assert!` at the split site to guard against future
  flag divergence.

  JE reference: `BIN.java::split()` propagates `expirationInHours` via
  `setExpirationInHours(hours)` on the new sibling.

  Regression tests:
  - `noxu-db/tests/ttl_bin_split_regression_test.rs` — three integration
    tests, two of which are FAIL-PRE/PASS-POST:
    `test_ttl_records_survive_bin_split_right_sibling_256` (128 keys lost
    pre-fix, 0 post-fix) and `test_ttl_and_no_ttl_keys_both_survive_bin_split`
    (64 keys lost pre-fix, 0 post-fix).
  - `noxu-tree/src/tree.rs` — two unit tests:
    `test_split_child_sibling_inherits_expiration_in_hours` and
    `test_hours_value_is_expired_only_with_false_flag`.

- **St-H6 Site 2 — Records vanish after close+reopen if background
  checkpoint ran during writes** (`noxu-recovery`):
  `RecoveryManager::eligible_for_redo` applied a `after_ckpt_start` guard
  to **non-transactional** LNs (those written by the `with_auto_txn` auto-
  commit path, where `locker_id = 0`).  When the background checkpointer
  thread (default 30-second interval) wrote a `CkptStart` record between
  two batches of inserts, LN records written before that `CkptStart` were
  skipped during recovery — **a variable number of records (observed
  33–194 out of 256) silently vanished after close+reopen**.

  Root cause: JE's checkpoint captures full BIN state so pre-checkpoint
  non-transactional LNs are safely skipped.  Noxu's checkpointer only
  flushes the internal `primary_tree` (not the open user-database trees),
  so the checkpoint does NOT capture the pre-checkpoint records.  The fix
  mirrors the existing logic for committed transactional LNs: non-
  transactional LNs are now always replayed regardless of checkpoint start
  position.  `redo_ln` / `redo_insert` is idempotent (skips if the tree
  already has a newer LSN for the key).

  Regression test: `test_ttl_records_survive_close_and_reopen` — FAIL-PRE
  (intermittent: 33–194/256 records missing when background checkpointer
  fires during the test), PASS-POST (stable 0 missing across 15+ runs).

### Added (C-C2b — WAL-scanner auto-feed)

- **`LogManager::log_with_vlsn`** (`noxu-log`): new write path that produces
  a 22-byte WAL header with `REPLICATED_MASK | VLSN_PRESENT_MASK` flags and
  the 8-byte VLSN value at offset 14. The existing `log()` path is
  byte-unchanged (14-byte header, no VLSN field).
- **`EnvironmentImpl::set_replication_vlsn_counter`** (`noxu-dbi`): installs
  a shared `Arc<AtomicU64>` VLSN counter. When set, `log_txn_commit`
  increments the counter and calls `log_with_vlsn`, writing VLSN-tagged WAL
  entries. Standalone envs are unaffected.
- **`ReplicatedEnvironment::with_environment` now wires the VLSN counter**
  (`noxu-rep`): calling `with_environment(env_impl)` installs the shared
  VLSN counter on the env so every subsequent `log_txn_commit` on the master
  is auto-tagged.
- **`spawn_feeder_runner` WAL-scanner path** (`noxu-rep`): when an
  `EnvironmentImpl` is wired, the `FeederRunner` background thread uses
  `EnvironmentLogScanner` as its source instead of the in-memory
  `PeerLogScanner` queue. Real commits are auto-fed to replicas without any
  `replicate_entry` call.
- **New convergence test** `test_wal_scanner_autofeed_convergence`: performs
  real `EnvironmentImpl::log_txn_commit` calls and asserts that
  all committed entries are received by the replica via WAL-scanner auto-feed.
  This test **fails on `origin/main`** (scanner finds no VLSN-tagged entries)
  and **passes with this change**. Closes the C-C2b qualification gap.
- **Format regression test** `test_standalone_env_writes_no_vlsn_header`:
  proves standalone envs still write 14-byte headers with no VLSN bits set.
- **Header format test** `test_log_with_vlsn_header_format`:
  asserts the 22-byte header layout, flags, and VLSN value on disk.

### Fixed (test robustness + stats accuracy)

- **`LockManager::get_stats()` now reports real `n_waiters` / `n_owners`** by
  summing across lock tables; previously `n_waiters` was hardcoded to `0` and
  `n_owners` was the lock count. The aggregate waiter/owner counts are now
  truthful.
- **`f12_explicit_txn_read_blocks_auto_commit_write`** made deterministic: it
  now uses a generous lock timeout (so the blocked write waits rather than
  timing out under load) and synchronizes on the live lock-waiter count
  instead of a fixed sleep. Robust under heavy CPU contention (20/20).
- **`test_x10_secondary_abort_read_committed_no_torn_state`** made
  deterministic and corrected: the reader now uses an explicit READ_COMMITTED
  transaction and asserts on the secondary cursor's atomically-resolved
  primary data (Wave 1B), instead of a separate auto-commit `get` that
  introduced a time-of-check/time-of-use window at a different isolation
  level. Robust under load (15/15) and now exercises the real READ_COMMITTED
  secondary-cursor atomicity guarantee.

### Added (on-disk format — St-C3, LOG_VERSION 2→3)

- The log file header now carries a CRC32 (v3 header = 36 bytes) so a torn
  header write is detected at open time (`LogError::HeaderChecksumMismatch`).
  Backward-compatible: legacy v2 files (32-byte header, no CRC) remain fully
  readable — each file's first-entry offset is resolved from its own version
  via `FileHeader::on_disk_size` (v2→32, v3→36), with no data migration.
  New files are written as v3. Version-aware offset handling threads through
  `file_manager`, `file_manager_scanner`, `cleaner`, and the recovery parser.

### Documentation (Q&A-surfaced gaps)

- Clarified that `noxu-spec` Stateright specs are **abstract protocol models**
  (they model-check the protocol design's safety/liveness and are kept in sync
  with the code by review convention; two anchor to production types) — NOT a
  mechanical refinement/conformance proof of the Rust implementation. Updated
  `AGENTS.md` and `docs/src/maintainer/crate-guide.md`.
- Added known-limitations entries for genuine BDB-JE-parity gaps: chained
  (replica-to-replica) log feeding, database/transaction triggers, admin
  dump/load/print-log tooling, code coverage not tracked in CI, and the
  spec-vs-implementation distinction.

### Fixed (isolation correctness — T-F2)

- **SERIALIZABLE isolation now prevents phantom reads via next-key range
  locking** (JE `Cursor.getLockType(rangeLock)` protocol).
  - `cursor_impl::lock_ln` acquires `LockType::RangeRead` instead of `Read`
    when `txn.is_serializable_isolation()`. `RangeRead` conflicts with
    concurrent `RangeInsert` on the same key slot, blocking phantom inserts or
    triggering a cursor restart.
  - New `lock_range_insert`: all new-key txn inserts acquire `RangeInsert` on
    the would-be successor key’s LSN. If a SERIALIZABLE scanner holds
    `RangeRead` on that slot, the insert is blocked until the scanner commits.
  - New `lock_eof_for_scan`: SERIALIZABLE forward scans that reach EOF acquire
    `RangeRead` on a per-database EOF sentinel (`Lsn::eof_lock_lsn`), blocking
    concurrent appends past the last scanned key.
  - `lock_manager.rs`: `WaitRestart` wakeup now correctly returns
    `Err(RangeRestart)` — the lock was never owned, and the scanner must
    restart. Previously it incorrectly returned `Ok(New)`, silently granting a
    lock the manager never added to the owner set.
  - `Locker::owns_any_lock` guards the same-transaction scan+insert case
    against an illegal `RangeRead`→`RangeInsert` upgrade.
  - `Database::put`/`put_no_overwrite` now use `NoxuError::from(e)` so lock
    errors surface as `LockNotAvailable`/`LockConflict` instead of
    `OperationNotAllowed`. `NoxuError::LockTimeout` gains a `detail` field
    preserving the owner/requester diagnostic.
  - Five new isolation tests prove phantom prevention and non-interference
    with lower isolation levels.
### Added (C-C2 — active push feeder)

- `ReplicatedEnvironment::register_feeder_channel(replica_name, channel)`: new
  method that registers a `Channel` for active-push log delivery to a specific
  replica. When `become_master` is called (or if already master), a
  `FeederRunner` background thread is spawned for each registered channel. The
  thread reads from a dedicated in-memory queue populated by
  `replicate_entry` / `apply_entry` fan-out and streams framed log entries to
  the replica. Previously `become_master` only created in-memory `Feeder`
  tracker structs without spawning any threads (C-C2 gap).
- `ReplicatedEnvironment::active_feeder_runner_acked_vlsn(replica_name)`: new
  method to inspect the last VLSN acknowledged by a replica's `FeederRunner`.
- Integration tests `crates/noxu-rep/tests/cc2_feeder_integration_test.rs`
  demonstrating convergence (6 tests including multi-entry, ack tracking,
  shutdown catch-up, late-registration, and apply_entry fan-out).

### Fixed (M-4 — `shutdown_group` replica catch-up wait)

- `ReplicatedEnvironment::shutdown_group` now waits up to half the configured
  timeout for `FeederRunner` replicas to acknowledge the master's current VLSN
  before sending `SHUTDOWN_GROUP`. Replicas on the pull path (no registered
  channel) are still sent `SHUTDOWN_GROUP` without a VLSN wait. Previously
  `shutdown_group` never checked replica catch-up status (M-4 gap).

### Fixed (review St-H5)

- `TreeNode::find_entry` now returns the FLOOR child slot (largest entry ≤ key)
  for non-exact lookups on Internal nodes, instead of the insertion point
  (which routes one child too far right). Consistent with the descent helper
  `upper_in_floor_index` and JE `IN.findEntry`. Previously latent (the live
  descent path does not use this arm); fixed to remove the landmine. Test
  `test_find_entry_internal_nonexact_returns_floor`.

### Fixed (memory safety — review R-F01)

- `LogBufferSegment` no longer stores raw pointers into the owning
  `LogBuffer`'s inline fields. The latch + pin-count are now a shared
  `Arc<LogBufferControl>` cloned into each segment, so moving the `LogBuffer`
  value no longer dangles a live segment's references (previously undefined
  behaviour if a buffer were moved while a segment was outstanding). Only the
  heap-backed `data_ptr` remains (it survives moves); `LogBufferSegment::put`
  no longer needs raw-pointer dereferences. Move-safety regression test
  `test_segment_survives_buffer_move`. noxu-log unsafe inventory 8 → 7.

### Changed (performance + correctness — review St-H4)

- Internal-node (upper-IN) tree descent now uses a binary floor-search
  (`Tree::upper_in_floor_index`) instead of an O(n) linear scan, applied
  uniformly across all eight descent sites. This also fixes a latent bug where
  `search_with_coupling` used a raw byte comparison and ignored a configured
  custom key comparator on that path. Verified by a property test comparing
  the binary search to a reference linear floor scan (incl. before/after/
  between/exact probes) and the full tree/db/dbi suites.

### Documentation (review follow-up)

- `file_header.rs`: corrected the byte-order documentation (the `byte_order`
  marker describes the 32-byte file header only; entry headers are
  little-endian and some payloads big-endian) and documented the missing
  header-checksum gap (review St-H1/St-H3/St-C3).
- Added `docs/src/internal/deferred-blocker-designs-2026-06.md`: concrete
  implementation designs + qualification plans for the dedicated-effort
  blockers (St-C3 on-disk format v3, St-H4/St-H5 unified IN floor-search,
  T-F2 SERIALIZABLE next-key locking, C-C2 become_master feeder threads) and
  the reaffirmed latent deferrals (R-F01, St-H6, T-F3/T-F4).

### Fixed (resource leak / stats — review T-F5)

- Explicit transactions now unregister from the `TxnManager` on commit/abort
  (and on the XA resolved-commit/resolved-abort paths). Previously only
  auto-commit transactions called `commit_txn`/`abort_txn`, so
  `TxnManager::all_txns` and the lock manager's locker-label map grew without
  bound for the process lifetime, `n_active_txns()` climbed monotonically, and
  `n_commits`/`n_aborts` undercounted. Regression test:
  `f5_explicit_txns_unregister_from_txn_manager`.

### Fixed (memory safety — from the v3.x production-readiness review)

- **noxu-xa (R-F04, use-after-free):** `XaEnvironment::get_transaction` returned
  a `&Transaction` borrowed from a `Mutex`-guarded map after releasing the
  guard; a concurrent (protocol-violating) `xa_rollback`/`xa_commit` could free
  the boxed transaction, dangling the reference. It now returns an
  `Arc<Transaction>` clone that keeps the transaction alive independently of
  the branch map. The `unsafe` pointer dereference is removed and `noxu-xa` now
  carries `#![forbid(unsafe_code)]` (zero unsafe). **Breaking:**
  `get_transaction` returns `Arc<Transaction>` instead of `&Transaction`;
  call sites that passed the result as `Option<&Transaction>` now write
  `Some(&*txn)`.
- **noxu-log (R-F03, undefined behaviour):** `FileManager::mmap_file` now
  refuses to memory-map the current write file. That file can be appended
  concurrently by the log writer while a disk-ordered cursor reads it, which
  violates `memmap2`'s no-concurrent-modification contract (UB). The log
  scanner already falls back to positioned `pread` reads, which are safe under
  concurrent appends. Sealed files are still mapped.

### Changed (recovery — defensive correctness, review T-F1)

- The recovery undo pass now enforces the JE `BIN.recoverRecord` currency
  check: an undo (delete or revert-to-before-image) is applied to a tree slot
  only when the slot still holds the exact version logged by the record being
  undone (`slotLsn == logLsn`). Previously the undo applied unconditionally
  and a code comment falsely claimed the check was "delegated to the tree
  layer". This closes the theoretical hole where an aborted transaction's
  before-image could overwrite a later committed write of the same key during
  recovery. NOTE: the specific interleaving could not be reproduced as a live
  failure on `main` (it is masked by runtime-abort reversion, the
  redo-only-committed model, and the no-active-txns fast path), so this is a
  defensive alignment with the reference algorithm rather than a fix for a
  demonstrated live corruption. Added a recovery-correctness regression test
  (`aborted_then_committed_same_key_recovers_committed_value`).

### Fixed (correctness + honesty — from the v3.x production-readiness review)

- **noxu-latch**: `thread_id()` now sets `| 1` so a thread whose hash is 0 no
  longer collides with the "unowned" sentinel and false-panics "latch already
  held" on first acquisition (review R-F05).
- **noxu-log**: documented the load-bearing struct-field drop-order invariant
  behind the `FileLogSource` lifetime `transmute` (review R-F02).
- **noxu-tree**: corrected the `BinStub::apply_delta` docstring — it is dead
  code that corrupts prefix-compressed keys and must not be used to
  reconstitute a BIN (removed the misleading `reconstituteBIN` claim; review
  St-C2/St-M3).
- **Docs honesty**: SERIALIZABLE isolation docs no longer claim range locks /
  phantom prevention — the cursor layer acquires plain read locks, so the
  delivered guarantee is repeatable-read (phantoms not yet prevented; review
  T-F2/T-F8). Corrected the config-parameter count (400+ → ~165), the crate
  count (19/21 → 22), the CRC32 throughput claim (x86-64-only, with the
  AArch64 software-fallback caveat), the README `unsafe` table (removed a
  `noxu-db` block that no longer exists), and the AGENTS.md `noxu-log` unsafe
  inventory (6 → 8).

### Added (documentation)

- the 2026 review — synthesis of a
  four-domain, seven-persona production-readiness review, with the prioritized
  blocker list, plus the four detailed source reports. The review found
  Critical correctness/soundness issues that remain open (recovery undo
  currency check, range-lock phantom prevention, two noxu-log `unsafe`
  soundness defects, XA use-after-free, file-header checksum); these gate a
  production major release and are tracked there.

### Fixed (durability — Critical)

- **WAL fsync fast-path could skip the fdatasync for a SYNC commit, silently
  losing committed data on power failure.** `flush_no_sync()` (used by
  `WRITE_NO_SYNC` auto-commits and the optional background no-sync flush
  daemon) advanced the same `last_flush_lsn` watermark that
  `flush_sync_if_needed()` consults to coalesce/skip fsyncs. A mixed-durability
  workload — a `WRITE_NO_SYNC` write to the page cache followed by a `SYNC`
  commit at a lower LSN — would see `last_flush_lsn` already past the SYNC
  commit and skip its `fdatasync`, leaving the commit in the OS page cache
  only. Added a separate durable watermark `last_synced_lsn` that is advanced
  *only* after a successful `fdatasync`; `flush_sync_if_needed` now keys its
  skip decision off it. Regression test:
  `test_flush_no_sync_does_not_satisfy_sync_durability`.

### Changed (safety — defensive)

- `BinStub::apply_delta` (noxu-tree) docstring corrected: it is dead code that
  writes uncompressed keys into prefix-compressed slots and must not be used to
  reconstitute a BIN (the live path is `mutate_to_full_bin`). Removed the
  misleading `BIN.reconstituteBIN()` claim that invited misuse.

### Added (recovery correctness tests)

- `open_txn_spanning_checkpoint_recovers_correctly` (crash/SIGKILL test):
  proves an open transaction whose writes precede a checkpoint does not leak
  uncommitted data through crash recovery. Locks in the isolation/recovery
  invariant against any future recovery scan-range optimization.
- `recovery_correctness_test.rs`: a workload suite (stable BINs, eviction,
  BINDelta chains, aborts spanning checkpoints, deletes, mixed pre/post
  checkpoint commits) validating full-scan recovery reconstructs committed
  state exactly.

### Documentation

- Recorded the true root cause blocking the P-2 recovery-scan optimization:
  the checkpointer flushes only `primary_tree`, not per-database user trees,
  so recovery is inherently a full scan. P-2 is a future optimization (needs a
  checkpoint redesign), not a correctness blocker; current full-scan recovery
  is correct. The full prototype is preserved on `fix/gb-proper-p2`. See
  `docs/src/internal/wave-gb-dbtree-recovery.md`.

### Documentation

- Wave GB (DbTree / P-2 recovery): documented the STEP-0 correctness analysis.
  The scan-reduction speedup is deferred — narrowing the recovery scan to
  `CkptStart` is unsafe while a transaction can span the checkpoint without a
  commit/abort record (it would surface uncommitted data as committed). The
  full tested prototype (DbTree index, LSN-aware redo_insert, 11-test equality
  harness) is preserved on the `fix/gb-dbtree-recovery` branch; nothing was
  merged to main because the write-side alone is net checkpoint overhead until
  recovery consumes the index. See
  `docs/src/internal/wave-gb-dbtree-recovery.md`.

## [v3.2.0] — 2026-06-02

### Added (replication — mTLS Phase 3)

- **End-to-end mTLS for the replication service and QUIC.** Phase 3 extends
  the Phase 2 peer-allowlist enforcement to the two paths that were still
  unauthenticated:
  - `TlsTcpServiceDispatcher` — the replication service dispatcher now binds
    via `bind_with_tls_and_allowlist`, so a node with `transport_kind = Tls`
    enforces mTLS end-to-end (was plain TCP).
  - QUIC — `QuicChannelListener::bind_with_tls_and_allowlist` /
    `TlsConfig::to_quinn_server_config_with_allowlist` wire the same
    `PeerAllowlistVerifier`, requiring and validating client certs against the
    CA + allowlist before any stream data (was `with_no_client_auth`).
  - The empty-allowlist **fail-closed** policy is now consistent across the
    TLS listener, dispatcher, and QUIC; a TLS node with an empty allowlist is
    a `ConfigError` rather than a silent plain-TCP downgrade.
  - Enforcement remains `tls-rustls`-only (`tls-native` has no client-cert
    verification API). See the 2026 review.

### Fixed (portability — RISC-V 64 + Windows on ARM64)

- **Windows (aarch64-pc-windows-msvc) support.** Validated the full workspace
  builds and all tests pass on Windows on ARM64, with three fixes:
  - `noxu-log`: a cross-platform positioned-I/O shim (`posio`) — Windows'
    `FileExt` exposes `seek_read`/`seek_write` (no `*_exact`/`*_all`), so the
    Unix `read_at`/`read_exact_at`/`write_all_at` calls didn't compile.
  - `noxu-log`: cross-platform directory fsync (`posio::sync_dir`) — the C-1
    parent-directory fsync opened the directory as a file, which fails on
    Windows without `FILE_FLAG_BACKUP_SEMANTICS`; now real dir-fsync on Unix,
    best-effort on Windows (NTFS journals the entry).
  - `noxu-rep`: the unbindable-address test now uses a non-local IP
    (RFC 5737 TEST-NET-1) instead of the privileged port 1 (Windows lets
    unprivileged users bind low ports).
- **RISC-V 64 (riscv64gc-unknown-linux-gnu)** validated: full workspace builds,
  all 170 test-suites pass, no code changes required.
- See `docs/src/internal/portability-rv-windows.md`.

## [v3.1.0] — 2026-05-31

Feature + remediation release on the umbrella line. Adds enforced mTLS
peer-authentication for replication, the DPL derive crate-path escape hatch,
and the full 2026-05 re-audit remediation (config completeness, umbrella API
gaps, crash-safety, the LogFlushTask latch regression, doc/spec accuracy).
No breaking change to the engine's on-disk format. Builds on v3.0.2.

### Security (Wave FB — mTLS Phase 2)

- **`peer_allowlist` enforcement** (`noxu-rep`): `RepConfig::peer_allowlist`
  is now enforced at the TLS handshake layer.
  `TlsTcpChannelListener::bind_with_tls_and_allowlist` installs a
  `PeerAllowlistVerifier` (`rustls::server::danger::ClientCertVerifier`)
  that rejects peers whose certificate Subject CN or DNS SAN is not in the
  configured list.  This closes the "peer_allowlist is inert" re-audit trap
  (mTLS Phase 1 honesty check removed).
- **Client-cert presentation**: `TlsConfig::to_rustls_client_config` now
  presents the client certificate for `PemFiles`/`PemBytes` identities,
  enabling server-side verification without API changes.
- Empty `peer_allowlist` is a `ConfigError` at construction (fail-closed).
- New public API: `TlsTcpChannelListener::bind_with_tls_and_allowlist`,
  `PeerAllowlist`, `TlsIdentity`, `TrustedCerts` re-exported from
  `noxu_rep`.

### Fixed (Wave ZC — crash-safety + perf, v3.1.0 candidate)

- **R-2 (regression)**: the `LogFlushTask` background daemon (added for
  `log_flush_no_sync_interval_ms`, X-11) held the log-write-latch across
  `pwrite64`, stalling all foreground commits during each background flush.
  `flush_no_sync` now snapshots state under the LWL, releases it, then does
  the write I/O — no more periodic commit-latency spikes.
- **R-7 (crash-safety)**: the log cleaner no longer silently falls back to a
  stale LSN when a migration WAL write fails; it aborts that slot's migration
  and retains the source file, preventing recovery data loss.
- **R-3 (crash-safety)**: recovered XA `TxnCommit` records now carry a real
  VLSN in replicated mode, and the recovery VLSN rebuild includes
  `TxnCommit`-derived VLSNs, so an XA-resolved commit is not lost to
  replication after a subsequent crash.
- **R-5**: documented and tested the non-transactional `NameLN` invariant
  (a non-transactional `open_database` create is durably committed at write
  time; recovery correctly treats it as committed).
- **R-1 (perf, partial)**: `collect_dirty_buffers` reuses the outer buffer
  collection across `flush_sync` calls instead of reallocating it each time.
  The inner per-buffer `to_vec()` copy remains — it is unavoidable while the
  LWL is released before I/O for R-2 (the bytes must be owned snapshots once
  the latch is dropped). Net: one fewer allocation per flush; the per-buffer
  copy is retained by design.
- **P-1 (perf)**: `FSyncGroup` gained an `AtomicBool` fast-path that
  eliminates the group-commit thundering-herd re-lock.
- **P-2**: W11 recovery throughput gap (~2.9× JE) scoped as a design note
  for a dedicated follow-up wave (BIN restore from the dirty-IN map). See
  the 2026 review.

### Added (v3.1.0 candidate)

- **Wave FA: `#[entity(crate = "…")]` escape hatch for direct `noxu-persist`
  users** — the three DPL derive macros (`Entity`, `PrimaryKey`,
  `SecondaryKey`) now accept `#[entity(crate = "noxu_persist")]` on each
  annotated struct to redirect generated code from `::noxu::persist::…` to
  `::noxu_persist::…`.  Users who depend on `noxu-persist` directly (without
  the `noxu` umbrella) can now use the derive macros without requiring the
  umbrella crate in their dependency graph.  Default behaviour (umbrella
  path) is unchanged; existing code requires no modifications.  Follows the
  `serde` / `#[serde(crate = "…")]` pattern.  Design Decision 9 escape-hatch
  deferral is now resolved.
- **Wave ZB: Re-audit reports archived** — four independent re-audit reports
  (`reaudit-2026-05-{je,margo,keith,jonhoo}.md`) copied into
  `docs/src/internal/` with a synthesis index.
- **mTLS Phase 1 (design + foundation)** for replication: a `peer_allowlist`
  config field and an `auth` module are plumbed through `noxu-rep`. This is
  foundation only — the dispatcher does not yet enforce mTLS; enforcement is
  planned for a later release. See `docs/src/internal/auth-mtls-design-2026-05.md`
  and the 2026 review.
- **Public API audit (May 2026)** documented across seven internal reports
  (overview, database, cursor, transaction/environment, secondary/join,
  collections/bind, persist/xa) under `docs/src/internal/`.
- **`noxu::Mutex` / `noxu::MutexGuard` re-export** — `noxu-db` now re-exports
  the `noxu_sync::Mutex` type that appears in its public API

### Changed (Wave ZB, v3.1.0 candidate)

- **Umbrella Quick-start example fixed** (`crates/noxu/src/lib.rs`): corrected
  `open_database` third arg (`bool` -> `&DatabaseConfig`) and `db.put` arg
  types; changed `\`\`\`ignore` to `\`\`\`no_run` so examples are compile-checked.
- **README `db.get` call fixed** (`README.md`): removed spurious fourth arg;
  `Database::get` takes 3 args.
- **`noxu-persist` doc examples corrected**: use `noxu::persist::` import paths;
  added derive-macro umbrella-dependency notice.
- **`verify_environment` / `verify_database` stubs now honest**: emit a
  `log::warn!` at call time and carry rustdoc noting they are stubs.
- **Stale `TODO(bug)` comments updated** in 5 `noxu-db` test files: now say
  "regression guard" (bugs fixed in commits 90918c5-b947b34).
- **C-6 TODO comments updated** in `recovery_manager.rs`: stale wave-11-r link
  updated to wave-11-y; write-path txn_id completion acknowledged; MapLN
  B-tree undo documented as known gap.
- **`recover()` / `recover_all()` docs updated**: documents the intentional
  asymmetry (single-DB has no catalog entries; multi-DB runs the C-6
  mapping-tree undo pass).
- **`recovery.md` updated**: added Phase 2b (Mapping-Tree Undo Pass, C-6).
- **`crate-guide.md` updated**: crate count 19 -> 22; added `noxu-persist-derive`,
  `noxu` (umbrella), `noxu-spec` sections; removed false "no derive macros" claim.
- **`algorithms.md` updated**: victim selection documents H-4 fix (fewest locks
  primary; youngest tiebreaker); recovery section updated with mapping-tree undo.
- **`design-decisions.md` updated**: fixed "Noxu and Noxu" in Decision 3;
  removed stale `off_heap.rs` unsafe row; added Decisions 9 (umbrella + derive
  coupling), 10 (`cache_size` total budget), 11 (mTLS Phase 2 not yet wired).
- **Stateright spec stamps updated**: all 7 v2.4.0-stamped specs re-stamped to
  v3.1.0 with per-spec notes; file citations in `recovery_three_phase.rs` and
  `vlsn_streaming.rs` corrected.
- **Workspace MSRV declared**: `rust-version = "1.85"` in `[workspace.package]`.
- **Workspace lints strengthened**: `unsafe_op_in_unsafe_fn = "deny"`;
  `clippy::undocumented_unsafe_blocks = "warn"`.
- **Wave-reference comments cleaned** in `recovery_manager.rs`
  (`SecondaryDatabase::open` takes `Arc<Mutex<Database>>`). Callers can now
  name it as `noxu::Mutex` and no longer need a direct dependency on the
  internal `noxu-sync` crate. The `secondary` example was updated to
  `use noxu::Mutex;` and the `noxu-sync` dev-dependency was dropped from the
  examples package.
- **Wave ZA** (fix/za-config-api): Config API gaps and silent-ignore elimination.
  - `noxu::PreparedTxnInfo`, `noxu::PreparedLnReplay`, `noxu::PreparedLnOperation`
    re-exported from `noxu-db` (closes jonhoo #3, JE F-6).
  - `noxu::SharedReplicaAckCoordinator`, `noxu::ReplicaAckCoordinator`,
    `noxu::AckWaitError`, `noxu::AckWaitErrorKind`, `noxu::ReplicaAckPolicyKind`
    re-exported from `noxu-db` (closes JE F-6).
  - `unimplemented_params` registry: 7 config parameters (`env_latch_timeout_ms`,
    `env_expiration_enabled`, `env_db_eviction`, `env_fair_latches`,
    `env_check_leaks`, `env_forced_yield`, `env_ttl_clock_tolerance_ms`) now
    emit `WARN`-level log at `Environment::open` when set to non-default values.
  - `RepConfig::peer_allowlist` emits `WARN` at `ReplicatedEnvironment::new`
    when non-empty (mTLS Phase 2 not yet implemented).

### Fixed (v3.1.0 candidate)

- **Wave ZA** (fix/za-config-api):
  - `DbIter` / `DbRange` now carry a `'txn` lifetime parameter, making
    use-after-commit a compile-time error (closes jonhoo #4).
  - `commit_pending_database` TOCTOU: `pending_names` changed from
    `HashSet<String>` to `HashMap<String, DatabaseId>`; the pending→committed
    transition is now atomic under the `pending_names` write lock; O(N) db_map
    linear scan eliminated; concurrent `open_database` for a pending name
    returns `DatabaseAlreadyExists` instead of silently creating a duplicate
    (closes keith R-4).

### Changed (v3.1.0 candidate)

- **Wave ZA** (fix/za-config-api):
  - Rustdoc for 7 unimplemented `EnvironmentConfig` fields updated to state
    "Reserved / not yet implemented as of v3.1" with explicit warning note.
  - `RepConfig::peer_allowlist` and `RepConfigBuilder::peer_allowlist` rustdoc
    rewritten to state the allowlist has no effect until Phase 2.
  - `known-limitations.md` updated with `peer_allowlist` and all 7 reserved
    config params explicitly listed.
  - `migrating.md` updated with Wave ZA breaking changes (`DbIter` lifetime,
    `pending_names` internal API change, new re-exports).

## [v3.0.2] — 2026-05-30

Docs-correction release. No engine code or public API change.

### Changed

- **Documentation**: all user-facing docs, the README, and examples now
  recommend the `noxu` umbrella crate (`noxu = "3"`, `use noxu::…`) instead
  of the internal `noxu-db` component crate. The umbrella was introduced in
  v3.0.1; this release corrects the misdirection.
- **Version bump**: workspace version `3.0.1` → `3.0.2`.
- **README**: crates.io / docs.rs badges now point at `noxu` (not `noxu-db`);
  Quick Start uses `noxu = "3"` and `use noxu::…`.
- **Examples**: all workspace example `[[example]]` targets and standalone
  projects (`cash`, `cask`, `ftdb`) use `noxu = …` as their dependency and
  `use noxu::…` imports.
- **`docs/src/getting-started/installation.md`**: dependency instructions
  updated to `noxu = "3"` with feature-flag table.
- **`docs/src/introduction.md`**: Quick Start updated.
- All `use noxu_db::`, `use noxu_collections::`, `use noxu_persist::`,
  `use noxu_xa::`, `use noxu_rep::`, `use noxu_bind::` import examples in
  docs/src/ rewritten to `use noxu::…` equivalents.

## [v3.0.0] — 2026-05-29

First crates.io release. This is the first major version to commit to the
SemVer stability policy (`docs/src/contributing/semver-policy.md`): from v3.0
onward, no breaking public-API change ships in a minor or patch release.

v3.0.0 lands the full remediation of the 2026-05 audit (first per-subsystem
pass and second cross-feature pass) plus the API-stability, crates.io, and
voice-cleanup work. See the per-wave reports under `docs/src/internal/`.

### Breaking changes

- **`Environment::open_database` is transactional** (C-4). When a transaction
  is supplied, database creation participates in the transaction: it rolls
  back on `txn.abort()` and is invisible to `get_database_names()` until the
  transaction commits. Database-creation now logs a provisional `NameLNTxn`
  inside the creating transaction (C-6); recovery undoes the NameLN for
  aborted or crash-before-commit creations. Old logs (commit-time `NameLN`,
  no txn_id) still recover unchanged.
- **`cache_size` is the total memory budget** (X-12). Previously it bounded
  only the BIN-tree Arbiter; log write buffers and the off-heap cache were
  separate pools. The Arbiter now receives
  `cache_size − log_buf_total − off_heap_reserved` (floored at 1 MiB). To
  preserve a prior BIN-tree allocation, increase `cache_size` by the log-buffer
  and off-heap sizes. See the migration guide.
- **`log_flush_no_sync_interval_ms` is now active** (X-11). Previously stored
  but never consumed; a non-zero value now starts the `noxu-log-flusher`
  background daemon that flushes `CommitNoSync` data on the configured interval.
- **Deprecated items scheduled for removal** (Wave 11-L): `Transaction::new`
  (use `Environment::begin_transaction()`), `EnvironmentConfig::set_txn_no_sync`
  / `with_txn_no_sync` / `set_txn_write_no_sync` and the
  `EnvironmentMutableConfig` equivalents (use `set_durability`/`with_durability`),
  `XaError::CrashDurabilityNotSupported`, and 13 obsolete `noxu-config::params`
  statics. These carry `#[deprecated(since = "2.4.1")]`.

See `docs/src/getting-started/migrating.md` for code-level migration recipes
for each breaking change.

### Highlights

- Full 2026-05 audit remediation across Waves 11-Q through 11-Y: WAL/recovery
  crash-safety (parent-dir fsync, fsync-failure env invalidation, recovery
  CRC32, log-buffer memory ordering), lock-manager ordering and victim
  selection, evictor `PartialEvict` actually freeing memory, cursor/database
  lazy `iter()`/`range()`, on-disk-format documentation accuracy, and the
  cross-feature criticals (recovered-XA-commit VLSN, cleaner×checkpoint
  deletion barrier, open-ended rollback intervals).
- `#![forbid(unsafe_code)]` on the 12 zero-unsafe core crates.
- API-stability surface enumerated; advisory `cargo-semver-checks` CI gate.
- All 19 public crates restructured for crates.io publication.

### Detailed changes

### Fixed (v3.0.0 — Wave 11-U recovery/checkpoint/cleaner/VLSN cluster)

- **X-8 — Checkpointer no longer writes redundant empty BINDelta after evictor
  flushes a BIN**: the dirty-BIN snapshot taken under the tree read lock could
  contain BINs that the evictor cleared before the per-node write-lock was
  acquired.  The previous guard only skipped empty-AND-clean nodes; the fix
  adds `if !b.dirty && dirty == 0 { continue; }` which correctly skips any
  already-clean BIN regardless of entry count.  (Wave 11-U X-8)

- **X-2 — VLSN index persistence now capped at the last checkpoint boundary**:
  `vlsn.idx` was flushed periodically with no coordination with the
  checkpointer.  After a crash the B-tree could recover to VLSN N while
  `vlsn.idx` claimed M > N, causing a feedgap mismatch.  The VLSN flush
  daemon now calls `flush_to_disk_capped(cap_lsn)` where `cap_lsn` is the
  last durable checkpoint end LSN; entries beyond that position are filtered
  out before writing.  (Wave 11-U X-2)

- **X-7 — Cleaner now dispatches secondary-LN liveness checks to the correct
  tree**: `SharedTreeLookup` previously ignored `db_id` and always looked up
  keys in the primary tree.  Secondary keys not found in the primary tree were
  misclassified as `Obsolete` and silently dropped during cleaning.
  `DatabaseImpl.real_tree` is now `Arc<RwLock<Tree>>` (shared), and the
  environment wires a live `db_trees_registry` to the cleaner so
  `lookup_parent_bin`/`migrate_ln_slot` dispatch to the correct tree per
  db_id.  (Wave 11-U X-7; **breaking**: `DatabaseImpl::get_real_tree()`
  return type changed to `Option<RwLockReadGuard<'_, Tree>>`)

- **C-6 (partial) — `NameLnRecord` carries `txn_id`; mapping-tree undo pass
  is functional**: `NameLnRecord` gains a `txn_id: Option<u64>` field
  populated from `LnLogEntry.txn_id` during recovery scanning.  The analysis
  pass now builds `recovered_db_txn_ids` alongside `recovered_db_names`.
  `run_mapping_tree_undo_pass` removes NameLN entries whose txn_id is in the
  aborted-transactions set.  Completed end-to-end in Wave 11-Y below.
  (Wave 11-U C-6)

### Fixed (v3.0.0 — Wave 11-Y C-6 end-to-end)

- **C-6 (complete) — `NameLNTxn` now written inside the creating transaction**:
  `EnvironmentImpl::open_database_transactional` now accepts a `txn_id: u64`
  parameter and calls the new `log_name_ln_txn` helper to write a
  `LogEntryType::NameLNTxn` entry (`Provisional::Yes`) **inside** the creating
  transaction.  `commit_pending_database` no longer writes a second `NameLN`;
  the `TxnCommit` record from the normal commit path serves as the durability
  marker.  The mapping-tree undo predicate was also strengthened to remove
  crash-before-commit entries (txn_id absent from `committed_txns`, not just
  present in `aborted_txns`).  Old WAL files (NameLN with txn_id=None) are
  treated as committed and always survive recovery.  The previously `#[ignore]`d
  end-to-end test `test_c6_aborted_db_creation_not_recovered` is now live.
  (Wave 11-Y C-6)

### Fixed (v3.0.0 — Wave 11-X XA/config/cache-budget fixes)

- **X-11 — `log_flush_no_sync_interval_ms` now wired to `LogFlushTask` daemon**:
  setting `log_flush_no_sync_interval_ms` previously had no effect; data
  committed with `CommitNoSync` stayed in write buffers indefinitely.
  `EnvironmentImpl` now starts a `noxu-log-flusher` background thread that
  calls `LogManager::flush_no_sync()` on the configured interval. (Wave 11-X X-11)

- **X-4 — Recovered XA branch TOCTOU window closed**:
  a concurrent `xa_start(JOIN, xid)` during `xa_commit`/`xa_rollback` I/O on a
  recovered branch received `XaError::NotFound` instead of `XaError::Protocol`.
  `XaEnvironment` now maintains a `resolving_xids` sentinel set; `xa_start(JOIN)`
  checks it and returns `Protocol` (retryable) during the resolution window.
  (Wave 11-X X-4)

- **X-10 — Secondary index abort torn-state verified safe under READ_COMMITTED**:
  the audit claimed a torn-state window during secondary+primary abort undo.
  Investigation confirmed that the existing per-slot write locks prevent this
  under READ_COMMITTED (the default): write locks are held across the entire
  undo pass and released only after all before-images are restored. Under
  READ_UNCOMMITTED the torn state is observable but is expected behaviour for
  that isolation level.  Regression test added. (Wave 11-X X-10)

### Changed (v3.0.0 — Wave 11-X — **BREAKING**)

- **X-12 — `cache_size` is now the total memory budget**:
  previously `cache_size` bounded only the BIN tree Arbiter; log write buffers
  (`log_num_buffers × log_buffer_size`) and off-heap cache (`max_off_heap_memory`)
  were independent pools, so actual memory could exceed `cache_size` significantly.
  The Arbiter is now initialised with
  `cache_size − log_buf_total − off_heap_reserved` (floored at 1 MiB).
  Users who set `cache_size` to bound the BIN tree pool must add the log-buffer
  and off-heap sizes to maintain the same allocation. (Wave 11-X X-12)
  See [migration guide](docs/src/getting-started/migrating.md).

### Fixed (v3.0.0 — Wave 11-T cross-feature criticals)

- **X-13 — `Database::check_open` and `CursorImpl::check_state` now verify env
  validity**: after a C-2 fsync failure (`io_invalid = true`) or explicit
  `EnvironmentImpl::invalidate()`, reads and cursor operations now return
  `EnvironmentFailure` instead of silently succeeding on stale data.
  `EnvironmentImpl::is_invalid` changed from `AtomicBool` to
  `Arc<AtomicBool>` so callers cache the flag without locking.
  `map_cursor_err()` added to `cursor.rs` to propagate env-failure errors
  correctly. (Wave 11-T X-13)

- **X-15 — Open-ended rollback interval now detected during recovery**:
  `RollbackTracker::is_in_rollback_period()` previously ignored
  `pending_rollback_starts` (incomplete rollback periods), allowing
  entries in an open-ended window to be re-applied during redo after a
  crash mid-rollback.  Now both completed and incomplete periods are
  consulted. (Wave 11-T X-15)

- **X-5 — Cleaner checkpoint barrier wired end-to-end (critical data-loss fix)**:
  the three-state deletion barrier (`cleaned → checkpointed → safe_to_delete`)
  was fully implemented in `FileSelector` but never called from outside the
  cleaner.  Files were deleted in the same cleaning pass before any checkpoint,
  making before-image undo reads fail silently (slot deleted instead of
  restored).  `Checkpointer` now holds an optional `Arc<Cleaner>` and calls
  `cleaner.after_checkpoint(&state)` after each successful checkpoint, activating
  the two-checkpoint deletion barrier. (Wave 11-T X-5)

- **X-6 — Cleaner migration writes real WAL LN entry**: `migrate_ln_slot` now
  writes a non-transactional `UpdateLN` WAL entry via `write_migration_ln()`
  and uses the returned LSN for the tree slot, ensuring recovery can find
  migrated data after a crash before the next checkpoint. (Wave 11-T X-6)

- **X-3 — Recovered XA commit allocates real VLSN in replicated env**:
  `write_txn_commit_for_recovered` now calls
  `coordinator.alloc_vlsn_for_recovered_commit(commit_lsn)` after writing
  the `TxnCommit` WAL frame.  `ReplicatedEnvironment` increments the VLSN
  counter and registers the commit in the VLSN index so replicas learn about
  the recovered XA transaction. (Wave 11-T X-3)

- **X-1 + X-14 — VLSN index rebuilt and truncated after recovery**:
  `RecoveryManager::run_redo_all` now collects `(vlsn, lsn)` pairs from all
  replayed LN entries (`RecoveryInfo::recovered_vlsns`).  After recovery,
  `ReplicatedEnvironment::with_environment()` re-registers these pairs into
  the VLSN index (X-14) and then calls `truncate_after(safe_vlsn)` based on
  the rollback matchpoint (X-1), ensuring the index is consistent with the
  recovered B-tree state. (Wave 11-T X-1, X-14)

### Breaking Changes (v3.0.0 — Wave 11-T)

- `CleanResult::files_deleted` now reflects the two-checkpoint barrier:
  files are only counted when they are actually removed after passing the
  barrier, not in the same cleaning pass.  Tests expecting immediate deletion
  must be updated (see `noxu-cleaner/src/cleaner.rs` for examples).
- `ReplicaAckCoordinator` has a new default method
  `alloc_vlsn_for_recovered_commit`; no action needed for existing impls.

### Added (v2.5.0 — Wave 11-S)

- **`Database::iter(txn)` + `Database::range(txn, range)`**: lazy forward
  iterators that implement `Iterator<Item = Result<(Vec<u8>, Vec<u8>)>>`.
  Records are fetched one at a time; the entire database is NOT eagerly
  materialised (addresses the 2026 review findings 2.1 / 2.3).
  See the 2026 review. (Wave 11-S Q-1)

### Fixed (v2.5.0 — Wave 11-S)

- **`Transaction::abort` env-lock hold** (H-1): the abort undo loop no longer
  holds the `EnvironmentImpl` mutex for the full undo duration. Each database
  handle is looked up with a brief per-record env lock acquisition; all undo
  application happens lock-free. Eliminates reader-starvation latency spikes
  during large-transaction aborts.
  (Wave 11-S H-1, the 2026 review F-2.2)

- **`CursorImpl::search` `current_index = 0` bug**: after a `Search` or
  `SearchGte` operation the cursor's `current_index` was always reset to 0,
  causing the subsequent `Get::Next` to advance from the second key in the
  BIN rather than from the found position. Fixed by propagating the actual
  BIN slot index from `search_with_data` and `find_range_entry`.
  (Wave 11-S Q-1 bonus, affects any code combining Search with Next)

- **`log_manager.rs` per-call `Vec` allocation** (H-3): the scratch buffer for
  log-entry encoding is now embedded in the LWL mutex (reused across calls).
  Eliminates a heap allocation on every log write.
  (Wave 11-S H-3, the 2026 review F-1.1)

### Documentation (v2.5.0 — Wave 11-S)

- **`docs/src/reference/on-disk-format.md`**: complete entry-type table
  regenerated from `crates/noxu-log/src/entry_type.rs` (H-6); endianness
  section rewritten per-field-category to accurately reflect that BIN/IN
  payloads are big-endian while entry headers are little-endian (H-7).

- **`docs/src/maintainer/algorithms.md`**: corrected `waiter_graph` direction
  (was "blocker->[waiters]", is "waiter->[owner_ids it is blocked by]") (H-5).

- **README.md Quick Start**: fixed `cursor.get_next` (non-existent) to
  `cursor.get(..., Get::Next, ...)` (H-8).

- **`lib.rs` / `transaction.rs` doc examples**: converted from ignore to no_run
  so they are compiled by `cargo test`. Fixed stale builder method names in
  `transaction.rs` example (H-8).

- **`docs/src/contributing/testing-guide.md`**: added "Slow / Stress Tests"
  section documenting the ignore inventory and how to run them (Q-2).

- All bare `#[ignore]` attributes in slow/stress/perf tests replaced with
  `#[ignore = "<reason>"]` (Q-2).

### Added (v3.0.0 candidate)

### Added (v3.0.0 candidate — Wave 11-R)

- **`Environment::compress()`** — synchronous BIN-compression trigger,
  mirroring JE `Environment.compress()`.  Drains the INCompressor queue in
  one pass; returns the count of BINs compressed.  Useful in tests and for
  applications that want deterministic memory reclamation after bulk deletes.
  (Q-3)

- **`Environment::evict_memory()`** — explicit evictor trigger, mirroring
  JE `Environment.evictMemory()`.  Requests the cache evictor to free pages
  toward the configured cache size; returns bytes freed.  (Q-3)

### Fixed (v3.0.0 candidate — Wave 11-R)

- **C-4 `open_database` transactional semantics**: the `txn` parameter is
  now honoured.  When a transaction is supplied and `allow_create = true`,
  the database creation is rolled back on `txn.abort()` and is invisible to
  `get_database_names()` until the transaction commits.  (Breaking: `_txn`
  renamed to `txn`; see `docs/src/getting-started/migrating.md`.)

- **C-5 `BIN::should_log_delta()` guard clauses**: three predicates from
  JE `BIN.shouldLogDelta()` were missing and are now added: (1) already-delta
  BINs always re-log as deltas; (2) `prohibit_next_delta` set by `compress()`
  forces a full BIN; (3) `last_full_version == NULL_LSN` forces a full BIN.
  Checkpoint output may differ in compress-then-checkpoint scenarios; recovery
  is strictly safer.

- **C-6 recovery two-pass structural scaffolding**: `RecoveryManager` now
  has an explicit `run_mapping_tree_undo_pass()` phase called after analysis
  and before data-LN redo, mirroring JE `buildTree()` phases B/D.  The
  aborted-NameLN removal loop is structurally correct; full JE parity
  (storing `txn_id` in NameLN WAL entries) is a follow-up.

- **C-8 SR9465/SR9752 TSV resolution**: four `PORTED-PARTIAL` entries in
  `je-tck-port-2026-05-enumeration-je.recovery.tsv` updated to
  `PORTED-EQUIVALENT`.  The underlying bugs (aborted delete+reinsert corrupts
  BIN; aborted dup inserts persist) were fixed in Wave 5; this wave audited
  and confirmed the fixes.

- **Q-4 recovery test fidelity**: `recovery_abort_test_inserts_three_phase_no_dups`
  now calls `env.compress()` after the abort phase, matching JE's
  `RecoveryAbortTest.testInserts`.  Previously the compressor-drain step was
  omitted due to the absence of a synchronous compress API.



- **API stability commitment**: `docs/src/contributing/api-stability.md` enumerates
  the v3.0 stable public surface for `noxu-db`, `noxu-bind`, `noxu-collections`,
  `noxu-persist`, `noxu-xa`, `noxu-rep`, `noxu-util`, and `noxu-config`.
  (Wave 11-L)

- **SemVer policy**: `docs/src/contributing/semver-policy.md` documents the
  pre-v3.0 (breaking-permitted) and v3.0+ (strict SemVer) policies, the
  definition of "breaking" per the Rust Cargo reference, the compatibility
  tier table, and the deprecation cycle.
  (Wave 11-L)

- **`cargo-semver-checks` CI gate**: advisory `semver-checks` job added to
  both `.github/workflows/test.yml` and `.forgejo/workflows/test.yml`, pinned
  at `cargo-semver-checks v0.47.0`.  Currently `continue-on-error: true`;
  will be promoted to blocking after one clean minor-release cycle post-v3.0.0.
  (Wave 11-L)

### Changed

- **crates.io publish preparation** (Wave 11-M): the workspace dependency
  graph has been restructured so every public `noxu-*` crate now carries
  `version = "2.4.1"` alongside its `path` entry in
  `[workspace.dependencies]`. The 19 crates intended for crates.io
  (see list below) have had `publish = false` removed. `noxu-spec` and
  `noxu-observe` remain private for now.

  v3.0.0 will be the **first crates.io release**. The full publish runbook
  (dep order, 60-second wait between publishes, docs.rs verification,
  badge updates, yank procedure) is documented at
  `docs/src/contributing/publishing.md`.

  Public crates in publish order:
  `noxu-util` → `noxu-sync` → `noxu-latch` → `noxu-config` → `noxu-log`
  → `noxu-tree` → `noxu-txn` → `noxu-evictor` → `noxu-cleaner`
  → `noxu-recovery` → `noxu-dbi` → `noxu-engine` → `noxu-db`
  → `noxu-bind` → `noxu-collections` → `noxu-persist-derive`
  → `noxu-persist` → `noxu-xa` → `noxu-rep`.

### Deprecated (v2.4.1)

The following items are marked `#[deprecated(since = "2.4.1")]` and will be
removed in v3.0.0.  Each has a `note` pointing to the replacement.

- **`noxu-db`**: `Transaction::new` (use `Environment::begin_transaction()`),
  `EnvironmentConfig::set_txn_no_sync` / `with_txn_no_sync` /
  `set_txn_write_no_sync` (use `set_durability` / `with_durability`),
  `EnvironmentMutableConfig::with_txn_no_sync` / `with_txn_write_no_sync`
  (use `with_durability`).
- **`noxu-xa`**: `XaError::CrashDurabilityNotSupported` (already deprecated
  since 2.0.0; removal confirmed for v3.0).
- **`noxu-config::params`**: `CLEANER_ADJUST_UTILIZATION`,
  `CLEANER_FOREGROUND_PROACTIVE_MIGRATION`, `CLEANER_LAZY_MIGRATION`,
  `CLEANER_BACKGROUND_PROACTIVE_MIGRATION`, `EVICTOR_NODES_PER_SCAN`,
  `EVICTOR_DEADLOCK_RETRY`, `EVICTOR_LRU_ONLY`, `LOG_DIRECT_NIO`,
  `LOG_CHUNKED_NIO`, `LOG_USE_NIO`, `LOG_DEFERREDWRITE_TEMP`,
  `OLD_REP_RUN_LOG_FLUSH_TASK`, `OLD_REP_LOG_FLUSH_TASK_INTERVAL`.

## [v2.4.2] — 2026-05-29

### Fixed

- **C-1** — fsync the parent directory after creating a new log file
  (`noxu-log/src/file_manager.rs`).  POSIX requires the parent directory
  fsync after `creat`/`rename` for the directory entry to be durable;
  without it a power loss between file creation and the next directory
  write loses the file from the directory entirely, taking all data
  written to it with it. Cross-confirmed by the JE-team and Keith
  audits.

- **C-2** — fsync error permanently invalidates the environment.
  `LogManager` now carries an `Arc<AtomicBool> io_invalid` checked at
  every `log()` entry; on any `fdatasync` error the flag is set and all
  subsequent commits fail fast.  Closes the fsyncgate-class window where
  the engine would continue accepting writes after a kernel I/O error.

- **C-3** — verify CRC32 in the recovery log scanner
  (`noxu-dbi/src/file_manager_scanner.rs`).  The scanner previously
  parsed entries without checking the stored CRC; bit-flip corruption
  silently injected garbage into the recovered B-tree.  CRC mismatches
  now cause the scanner to treat the entry as end-of-valid-log (the
  conservative recovery posture).

- **C-7** — `Release`/`Acquire` ordering on log-buffer pin-count
  (`noxu-log/src/log_buffer.rs`).  The `pin_count.fetch_sub` was
  `Relaxed`; under the C++/Rust memory model, a thread observing
  `pin_count == 0` could be reordered before the writer's segment
  writes, losing data.  Now `Release` on the decrement, `Acquire` on
  the zero-check.

- **H-2** — establish shard-before-waiter-graph lock ordering in
  `noxu-txn/src/lock_manager.rs`.  Documented the canonical order;
  added `flush_and_clear_waiter()` helper used by all six victim-cleanup
  paths so the ordering is mechanically enforced.

- **H-4** — deadlock victim selection now populates `lock_counts`
  (`noxu-txn/src/lock_manager.rs::compute_lock_counts`).  Previously
  `select_victim` always received an empty `HashMap`, falling through
  to the youngest-tiebreaker; the documented primary criterion (fewest
  locks held) was dead code.  The shard scan only runs on the rare
  cycle path; no cost on the common no-cycle path.

- **H-9** — `PartialEvict` now actually frees slot data.  Added
  `BinStub::strip_lns` (clears `data: Option<Vec<u8>>` on non-dirty
  slots, returns bytes freed) and `Evictor::strip_lns_from_node`
  (locates and strips the BIN).  Previously the evictor incremented
  stats and credited bytes against the budget without freeing any
  heap; the budget tracker drifted below reality and the evictor
  under-fired under pressure.

### Changed

- **C-9** — reorganized the `unsafe` inventory in `AGENTS.md` as a
  per-crate table.  Added the `std::mem::transmute` in
  `noxu-log/log_source.rs:61` (sound: `Arc<FileHandle>` outlives the
  guard) and the `unsafe impl Send for LogBufferSegment`.  Removed three
  stale `unsafe impl Send + Sync` blocks in
  `noxu-rep::elections::{election, master_tracker, phi_detector}` whose
  fields auto-derive the bounds.

- **Q-5** — added `#![forbid(unsafe_code)]` to the 12 zero-unsafe
  crates: `noxu-tree`, `noxu-txn`, `noxu-evictor`, `noxu-cleaner`,
  `noxu-recovery`, `noxu-dbi`, `noxu-engine`, `noxu-bind`,
  `noxu-collections`, `noxu-persist`, `noxu-config`, `noxu-util`.  The
  zero-unsafe claim is now machine-enforced.

- **Voice cleanup.** Removed agent-process artifacts (wave/sprint labels,
  boastful adjectives, false provenance claims) from all user-facing
  documentation and public-crate rustdocs.  No API or behaviour change.
  `README.md`, `docs/src/introduction.md`, `docs/src/getting-started/`,
  `docs/src/transactions/`, `docs/src/replication/`, `docs/src/collections/`,
  `docs/src/operations/benchmarks.md`, `docs/src/reference/architecture.md`,
  `docs/src/contributing/porting-guidelines.md`,
  `docs/src/maintainer/project-history.md`, and public `///` rustdocs in
  `noxu-db`, `noxu-bind`, `noxu-collections`, `noxu-persist`, `noxu-rep`,
  `noxu-xa`.

### Deferred

- **H-3** (per-log-entry allocation reduction), **H-1** (abort lock-hold),
  **H-5–H-8** (documentation accuracy fixes), **Q-1–Q-4, Q-6, Q-7**
  (UX + cleanup) — wave 11-S.
- **C-4, C-5, C-6, C-8** (breaking semantic fixes) — wave 11-R / v3.0.0.

See the 2026 review
for the full per-fix details.

## [v2.4.1] — 2026-05-29

### Fixed

- `noxu-rep::phi_detector_test::test_master_tracker_phi_mode` is no longer
  `#[ignore]`'d.  Wave 9-A's de-flake reduced but did not eliminate a
  ~20 % miss rate on dev machines under workspace test load.  The miss
  was traced to the test's first assertion ("master must be alive right
  after heartbeats"), which is fundamentally racy: phi is computed from
  `last_heartbeat.elapsed()`, so any scheduler delay between the final
  `record_heartbeat()` and the `is_master_alive()` check briefly inflates
  phi above the 1.0 threshold even when no master failure occurred.  The
  fix removes that racy assertion (the deterministic alive-after-heartbeats
  invariant is already covered by unit tests in `master_tracker.rs` and
  `phi_accrual.rs` with controlled clocks) and keeps only the
  monotonic, timing-robust failure-detection assertions.  Verified with
  8 consecutive successful runs.

## [v2.4.0] — 2026-05-28

### Known issues

- `noxu-rep::phi_detector_test::test_master_tracker_phi_mode` is `#[ignore]`'d
  with a fresh TODO. Wave 9-A's de-flake reduced the miss rate but a ~20 %
  failure remains under workspace test load on dev machines (the first
  assertion `master must be alive right after heartbeats` trips when
  scheduler delay between the last `record_heartbeat()` and the
  `is_master_alive()` call pushes phi briefly above the 1.0 threshold). The
  proper fix is deterministic phi-clock injection or restructuring the
  test; tracked for a follow-up wave.  *(Closed in v2.4.1.)*

## [v2.3.2] — 2026-05-28

### Fixed (v2.3.2)

- **`AnalysisResult::record_active_txn` precondition gap** (`noxu-recovery`).
  Calling `record_active_txn` after `record_commit` / `record_abort` for the
  same txn id re-inserted the txn into `active_txn_ids`, causing
  `has_active_txns()` to return a phantom `true`.  Added an early-return guard.
  (Wave 11-E regression)

- **Transactional cursor on non-transactional database now rejected**
  (`noxu-db`).  `Database::open_cursor(Some(&txn), None)` now returns
  `IllegalArgument` when the database is non-transactional, matching JE.
  (Wave 11-G regression)

- **`put_no_overwrite` on sorted-dup DB now checks key only** (`noxu-dbi`).
  `CursorImpl::put_dup` was checking the `(key, data)` pair for both
  `NoDupData` and `NoOverwrite`; per JE semantics `NoOverwrite` must check
  the key only.
  (Wave 11-G regression)

- **Database name registry now persisted across clean close+reopen**
  (`noxu-dbi`, `noxu-recovery`).  Writes a `NameLN` WAL entry on database
  creation; recovery re-populates `name_map` from these entries.  Read-only
  reopens and non-transactional databases both survive the cycle.
  (Wave 11-G and Wave 10-A regression)

- **Explicit checkpoint no longer loses committed data** (`noxu-recovery`).
  `Checkpointer::do_checkpoint()` was writing `NULL_LSN` as `first_active_lsn`
  in `CkptEnd`, causing recovery to skip committed LN entries before the
  checkpoint start.  Fixed by writing `Lsn::new(0, 0)` and always replaying
  committed LNs in `eligible_for_redo`.
  (Wave 11-G regression)

- **`truncate_database` is now durable across clean close+reopen**
  (`noxu-dbi`).  Before replacing the in-memory tree, write non-transactional
  `DeleteLN` entries for every key; recovery replays them after the original
  inserts, leaving an empty tree.
  (Wave 11-G regression)

<!-- ============================================================== -->
<!-- Note: the Added (v2.4.0 — Wave 11-D) and subsequent v2.4.0      -->
<!-- entries below are LOGICALLY part of the [v2.4.0] section above. -->
<!-- They were authored under [Unreleased] before the v2.3.2 patch   -->
<!-- release was inserted in front of v2.4.0; rather than re-order   -->
<!-- the entire file (which would lose `git blame` history) we leave -->
<!-- them in place and rely on the per-entry section headers         -->
<!-- ("Wave 11-D", "Wave 11-E", …) to identify which release each    -->
<!-- belongs to.                                                     -->
<!-- ============================================================== -->

### Added (v2.4.0 — Wave 11-D)

- **First-class in-memory replication transport.** Wave 11-D promotes
  the in-memory transport from a `cfg(test)` / `feature = "test-harness"`
  test fixture into a production transport alongside TCP, TLS, and QUIC.
  See [`docs/src/replication/in-memory-transport.md`](docs/src/replication/in-memory-transport.md)
  and the wave note at
  the 2026 review.
  - New: `noxu_rep::net::InMemoryTransport` (factory) with
    `new_pair()` and `new_group(n)`.
  - New: `noxu_rep::net::InMemoryEndpoint` (implements the same
    `Channel` trait as `TcpChannel` / `TlsTcpChannel` /
    `QuicMultiplexedChannel`).
  - New: `noxu_rep::net::InMemoryGroup` (n-node fully-connected mesh)
    with `simulate_crash(node)`, `reconnect(node)`,
    `is_node_live(node)`, and `try_channel(from, to)` for crash
    recovery, partition, and asymmetric-link tests.
  - New: `noxu_rep::RepTransportKind` enum (`Tcp`, `Tls`, `Quic`,
    `InMemory`; default `Tcp`) and `RepConfig::transport_kind` /
    `RepConfigBuilder::transport_kind` so callers declare their
    transport choice declaratively.
  - The pre-existing `noxu_rep::test_harness::RepTestBase` /
    `RepEnvInfo` / `CountingListener` types are lifted out of the
    `cfg(test)` / `feature = "test-harness"` gate and are now
    always part of the public API surface; the `test-harness`
    feature flag is retained as a no-op for backward compatibility.
  - 11 new unit tests in `crates/noxu-rep/src/net/inmem.rs`; 7 new
    integration tests in
    `crates/noxu-rep/tests/inmem_transport_test.rs`.

### Fixed (v2.3.1 — Wave 11-N)

Four noxu sorted-dup cursor bugs surfaced during Wave 11 and routed to
this follow-up wave (Wave 11-N) are now closed.  All four shared a
common root-cause area: incomplete multi-primary / cross-BIN handling
in `noxu-dbi::CursorImpl`'s sorted-dup logic.  None affected
single-primary sorted-dup use, which has been covered by
`crates/noxu-db/tests/sorted_dup_test.rs` throughout.

1. **`Cursor::count()` over-counted past the first dup of a primary**
   on multi-primary sorted-dup DBs.  The previous formula
   `backward + 1 + forward` double-counted because the backward walk
   already repositioned scratch on the first dup, and the forward
   walk then re-traversed every dup including the original
   position.  Fix in `noxu-dbi::CursorImpl::count`: drop the
   `backward` term, return `forward + 1`.  Regression test
   `db_cursor_duplicate_test_duplicate_count` (no longer `#[ignore]`).
2. **`Get::Search` + `Get::NextDup` returned NotFound on every primary
   except the lexicographically smallest**, on multi-primary
   sorted-dup DBs.  Root cause: `search_dup` hard-coded
   `current_index = 0` after locating the entry, so the subsequent
   `retrieve_next` computed `next_index = 1` in the BIN's slot
   space.  Fix: new `Tree::first_entry_at_or_after_with_index`
   returns the BIN node and the slot index; `search_dup` now stores
   the real index and pins the BIN, mirroring the invariant
   `get_first` / `get_last` already maintain.  Regression test
   `db_cursor_duplicate_test_get_next_dup` (no longer `#[ignore]`).
3. **`SecondaryCursor::get_search_key` + `get_next_dup_full`**
   triggered `SecondaryIntegrityException` past the first yield.
   This is the same `Search`-then-step boundary defect as #2 reaching
   through the secondary layer; closed by the same `search_dup` fix.
   Regression test `wave11n_bug3_get_search_key_then_next_dup_full_yields_all`
   in `crates/noxu-db/tests/wave11n_secondary_dup_test.rs`.
4. **`SecondaryCursor::get_first` + repeated `get_next` revisited
   primaries or failed to terminate** once the secondary tree spanned
   more than one BIN.  Root cause: `apply_dup_filter`'s cross-BIN
   acceptance paths updated `current_key` / `current_index` but left
   `current_bin_arc` pointing at the prior BIN, so the next
   `retrieve_next` fast-path read `next_index = current_index + 1`
   from the stale BIN — effectively re-emitting old entries.  Fix:
   new `CursorImpl::find_bin_arc_for_key` helper plus an
   `update_bin_pin` call at every accept site in `apply_dup_filter`.
   Regression test `wave11n_bug4_get_first_get_next_full_walk_terminates`.

See the 2026 review for the
full per-bug analysis.

### Tests

* **TCK ports (Wave 11-A).**  6 dup-cursor methods from JE's
  `com.sleepycat.je.dbi.DbCursorDuplicateTest` ported to
  `crates/noxu-db/tests/je_db_cursor_test.rs`
  (`testDuplicateCreationForward` / `Backwards`, `testGetNextNoDup`,
  `testPutNoDupData2`, `testDuplicateReplacement`,
  `testDuplicateDuplicates`).  Master TSV bumped from NOT-PORTED to
  PORTED-EQUIVALENT.

### Benchmarks

* **W13 sorted-dup secondary index walk (Wave 11-B).**  New workload
  in `benches/noxu-bench/` plus a matching JE counterpart in
  `benches/je-bench/`.  Closes Wave 10-D gap #1.
* **Real-storage W10 / W11 re-run (Wave 11-C).**  W10 (concurrent)
  and W11 (recovery) re-run on real NVMe at N=10 000;
  FsyncManager group-commit coalescing now visible (~6–30×
  coalescing factor depending on writer count).  Numbers tabled in
  `docs/src/operations/benchmarks.md`.

### Documentation

* the 2026 review: narrative summary
  of Waves 11-A / 11-B / 11-C, including the four sorted-dup cursor
  bugs surfaced (all closed in Wave 11-N — see `### Fixed` above).
* the 2026 review: per-bug
  analysis for the four sorted-dup cursor bugs closed in Wave 11-N.
* `docs/src/operations/benchmarks.md`: new W13 and "Real-storage
W10 / W11 re-run" sections.

### Changed

- **Stateright spec coverage (Wave 11-F)** — every protocol modelled
  in `noxu-spec` is now stamped with an explicit `VALIDATED-AS-OF`
  version in its module preamble.  Five models were also
  strengthened with new or upgraded invariants:
  * `wal_commit::FsyncedNeverDecreases` is now a true 2-state
    monotonicity invariant (was a coarse termination check).
  * `recovery_three_phase::IdempotentReplay` is now a true 2-state
    idempotency invariant (snapshot the materialisation after the
    first redo; assert subsequent redos yield the same vector).
  * `cleaner_safety::LiveCheckHonoured` (new) — every deleted file
    must have its `cleared_for_delete` bit cleared at the moment
    of deletion.
  * `cache_vs_cleaner::MigratedReflectsDisk` (new) — every committed
    migration must equal the cleaner's pre-migration snapshot.
  * `xa_two_phase_commit::RecoveryConsistent` (new) — closes the
    original module-preamble TODO with a 2-state pre-crash /
    post-recovery decision-consistency predicate.

  All 11 specs continue to pass under `make spec` in ~31 seconds.

### Added (v2.4.0 — Wave 11-E)

- **Wave 11-E — Property test expansion**: +39 new `proptest` blocks
  across `noxu-tree` (BIN-delta and DeltaInfo round-trips, 7), `noxu-bind`
  (`SortKey` reverse and ordering properties, 6), `noxu-cleaner`
  (utilization tracker oracle and `FileSummary` arithmetic, 10),
  `noxu-recovery` (rollback periods and `AnalysisResult` txn state
  machine, 9), and `noxu-rep` (Paxos acceptor and VLSN streaming, 7).
  See the 2026 review.
  Adds `proptest` as a dev-dependency for `noxu-cleaner` and
  `noxu-recovery`.  No production-code changes.

### Notes (Wave 11-E)

- Wave 11-E surfaced one behaviour gap in `noxu-recovery::AnalysisResult`
  (`record_active_txn` does not defensively check the committed/aborted
  sets), committed as an `#[ignore]`'d test
  `prop_active_txn_after_terminal_resurrects_phantom_active`.  Bug fix
  routed to a post-v2.4.0 wave per the property-test discipline.

### Added (v2.4.0 — Wave 11-G)

- **Wave 11-G — JE TCK long-tail port (49 new tests).**  Across
  `crates/noxu-db/tests/`: 9 DatabaseTest/EnvironmentTest invariants,
  7 SR-numbered + DupSlotReuse regression tests, 5 TruncateTest
  invariants, 6 GetSearchBothRangeTest range-query corner cases, 5
  recovery invariants (RecoveryDuplicates / Checkpoint / Delete /
  EdgeTxnId), 7 tree-level invariants (Split / TreeBalance /
  KeyPrefix), and 9 dup cursor invariants
  (DbCursorDuplicate{,Delete}Test).  TSV row totals went from PE 263 /
  PP 99 / NOT 1580 to PE 306 / PP 105 / NOT 1531 (+43 PE, +6 PP, −49
  NOT).  See
  the 2026 review.

### Tracked Noxu bugs surfaced (Wave 11-G; 5 total)

Each of these is a `#[ignore]`'d test in this wave's commits that
documents a real Noxu regression vs JE's invariant.  All routed to a
follow-up bug-fix wave (no production code changed in Wave 11-G).

- `database_txn_cursor_on_non_txn_db_rejected` — Noxu permits opening
  a transactional cursor on a non-transactional database; JE rejects.
- `database_put_no_overwrite_in_dup_db_{txn,no_txn}` — Noxu's
  `put_no_overwrite` on sorted-dup databases checks the *(key, data)*
  pair instead of the key alone.
- `environment_read_only_rejects_db_name_ops` — Noxu's database-name
  registry is not preserved across a clean close+read-only reopen.
- `environment_checkpoint_after_commit_loses_data` — Calling
  `env.checkpoint(None)` between `txn.commit()` and `drop(env)` causes
  the most recently committed records to be lost on the next env open.
- `truncate_survives_clean_close_reopen` — Noxu's `truncate_database`
  is not durable across a clean close+reopen.

### Added (v2.4.0 — Wave 11-H)

- Wave 11-H: per-workload `perf` profile captures (W03/W04/W10/W11)
  and a single-workload profiler harness under `benches/profiles/`.
  See the 2026 review for the
  per-workload root-cause analysis and the ROI ordering of waves
  11-I (cursor/BIN), 11-K (recovery), and 11-J (fsync).

### Performance (v2.4.0 — Wave 11-I)

- `Database::get` hot path: eliminated triple tree descent (Wave-11-I).
  `Tree::search_with_data` folds the previous three separate descents
  (existence check, data fetch, BIN pinning) into one, and replaces the
  O(n) `iter().find()` BIN slot lookup with the existing binary-search
  helper `find_entry_compressed`.
  - W03 sequential read (100 K): 657 K → 1 413 K ops/s (+115%)
  - W04 random read (100 K):     438 K → 1 030 K ops/s (+135%)
  - Both workloads now exceed JE on the same hardware.
  - Secondary-index / sorted-dup path unchanged.
  - See the 2026 review.

### Performance (v2.4.0 — Wave 11-J)

- `FsyncManager` crash-safety property test added
  (`test_fsync_before_commit_invariant`): verifies that every committed
  transaction's LSN is fdatasync’d before `txn.commit()` returns, using
  8 concurrent committers and 200 ops each.  The test is not `#[ignore]`
  and runs in `cargo test -p noxu-log`.
- Performance investigation: a Treiber-stack + per-waiter condvar rewrite
  was prototyped but reverted after back-to-back benchmarks showed 10–46 %
  regressions attributable to per-call `Arc` allocation overhead and
  coalescing-window changes.  See
  the 2026 review for the full diagnosis
  and recommended next steps.

### Performance (v2.4.0 — Wave 11-K)

- Recovery redo path: reduced per-record allocations (Wave-11-K).
  Three complementary changes in `noxu-tree` and `noxu-recovery`:
  - `Tree::redo_insert(&[u8], &[u8], Lsn)` + `BinStub::insert_with_prefix_slice`:
    eliminates one intermediate `Vec<u8>` per LN record by passing `Bytes`-backed
    `&[u8]` slices directly to the BIN insertion code (Fix 1).
  - Consuming iteration in `run_analysis`: moves `LnRecord` into `redo_entries`
    without `Bytes::clone()` Arc-refcount bumps (Fix 2 — eliminates 200K+
    atomic increment/decrement pairs at 100K-record scale).
  - `Tree::hint_redo_capacity` + pre-allocated BIN split halves in `split_child`:
    eliminates Vec-resize doublings in the initial BIN and in each new BIN
    created during redo (Fix 3).
  - Add `RecoveryScratch` struct documenting the zero-copy redo loop intent.
  - All 5764 tests pass; gate: fmt + clippy + doc all clean.
  - W11 wall-clock improvement is within measurement noise at 100K on this
    machine (≈251ms vs ≈254ms baseline, ratio 2.9× JE).  Root-cause analysis
    in the 2026 review explains why the gap
    remains: the dominant ≈200ms cost is env-open overhead outside the redo loop,
    not allocator pressure in the redo path itself.  A follow-up (BIN
    deserialization from dirty_in_map, or lazy env-open) would be needed to
    reach the 1.5× acceptance gate.

## [2.2.1] - 2026-05-27

CI-green release.  Unblocks GitHub Pages and Codeberg Pages publishing.

### Fixed

- 17 `cargo doc -D warnings` broken intra-doc links across `noxu-txn`,
  `noxu-dbi`, `noxu-db`, `noxu-rep`, and `noxu-xa`.  Private-item and
  out-of-scope references are now plain backticked code instead of
  resolvable links.
- 74 lychee link-check errors in the rendered mdBook.  Chapter-intro
  cross-references that pointed at `foo/README.md` (which mdBook
  renders as `foo/index.html`, not `foo/README.html`) were corrected
  in seven chapters; eight unlisted internal docs were added under
  *Internal Documents* in `SUMMARY.md`; one stale
  `je-fidelity-review.md` link was removed.
- `.github/workflows/docs.yml` now builds the book twice — once with
  an empty `MDBOOK_OUTPUT__HTML__SITE_URL` for lychee (so `404.html`'s
  `<base href>` is empty), then again with the real `/noxu/` prefix
  for upload — eliminating false-positive 404s from lychee.

### Compatibility

No source-code changes outside doc-comment text and `SUMMARY.md`.
Fully backwards compatible with v2.2.0.

## [2.2.0] - 2026-05-27

`noxu-rep` correctness fixes, Stateright spec re-validation, and 38
additional JE TCK ports.  Wave 9 finishes everything Wave 8 surfaced.

### Fixed

- `noxu-rep`: `become_master` now rejects non-electable node types.
  Closes the `secondary_node_become_master_should_fail` regression
  that Wave 8 surfaced and pinned with `#[ignore]` — secondary nodes
  could previously transition incorrectly to master.
- `noxu-rep`: the replica I/O thread auto-bootstraps via the
  dispatcher when the master signals `NeedsRestore`.  Holds a
  `Weak<Self>` back-reference and falls through cleanly if the
  environment was dropped.  Closes a Wave 4-A follow-up.
- `noxu-rep`: de-flaked `test_master_tracker_phi_mode`.  The
  pre-existing ~20 % flake under workspace test load is now
  deterministic, so CI test runs are stable.

### Changed

- Stateright executable specs in `noxu-spec` updated to model the
  v2.0.0 persistence changes:
  - `flexible_paxos` models persistent acceptor promises across
    restart (closes F5 / F31, no-two-masters-per-term holds).
  - `vlsn_streaming` models persistent `vlsn.idx` across restart
    (closes F11, replicas resume without full network restore).
  - `master_transfer` drives F9 feeder spawning on master transition.
  - Dispatcher-mediated network restore (F2 / F4) is now in the spec.
  - All five updated specs pass with no counterexamples; the
    production code matches the abstract protocol.

### Added

- 38 new JE TCK ports (PORTED-EQUIVALENT), 7 PORTED-PARTIAL, 13
  OUT-OF-SCOPE classifications, across `bind/tuple` (18, including
  `TupleFormatTest` round-trips and `TupleOrderingTest`),
  `je.cursor` / `je.config` (5), `je.recovery` (2), `je.txn`
  deadlock + lock tests (3), `je.log` `FileManagerTest` (4), and
  `je.test.AtomicPutTest` (2).  Aggregate JE TCK status:
  PORTED-EQUIVALENT 205 → 243, NOT-PORTED 1 710 → 1 653.

### Compatibility

No on-disk format changes vs v2.1.0.  No public API changes; the
`become_master` guard returns a typed error for what was previously
accepted-but-broken.

## [2.1.0] - 2026-05-27

Polish release: the v2.0.0 read-only-reopen bug is fixed, the
heavy `noxu-rep` test harness lands, and stale references to the
old `lamdb` repository name are scrubbed so external clones over
HTTPS work end-to-end.

### Added

- `noxu-rep` ships a `RepTestBase` / `RepEnvInfo` test harness
  gated behind a new `test-harness` cargo feature.  The harness
  uses in-memory channels — it never opens a real TCP socket —
  and exposes `create_group`, `find_master`, `await_state`,
  `await_vlsn_at_least`, `replicate_one`, `populate_db`,
  `catch_up_replica`, `failover_to`, `assert_all_at_vlsn`, and
  auto-cleanup on `Drop`.  Release builds are unaffected.
- 36 ports of heavy `je.rep` TCK tests on top of the new harness,
  each running in under 50 ms: 13 from the top-level rep TCK
  (lifecycle + group membership), 14 from `je_rep_txn_tck`
  (replicated commit / abort interleavings), and 9 from
  `je_rep_stream_tck` (stream integrity, durability, gaps).

### Fixed

- `noxu-persist`: read-only reopen of an existing entity store no
  longer requires `allow_create=true`, matching JE behaviour.  The
  previously-`#[ignore]`'d regression
  `tck_persist_read_only_store_reopens_without_allow_create` now
  passes.  Discovered during the JE TCK port (Wave 4-C).
- Documentation and submodule pointers no longer reference the old
  `lamdb` GitHub org — `.gitmodules` uses HTTPS instead of SSH (so
  external `git submodule update --init` works without a registered
  Codeberg SSH key), GitHub Actions deploys to `/noxu/` instead of
  `/lamdb/`, and mdBook internal docs use `$JE_HOME` / `$NOSQL_HOME`
  instead of hard-coded developer paths.

### Known Issues

- Wave 8 surfaced one regression — `noxu-rep` `become_master` did
  not check `NodeType::Secondary` — that is committed as an
  `#[ignore]`'d test.  Fixed in v2.2.0.

### Compatibility

No on-disk format change vs v2.0.0.  The `test-harness` feature is
opt-in; release builds are unaffected.

## [2.0.0] - 2026-05-27

First semver-stable release.  `noxu-rep` is GA-ready, the JE TCK
port is well underway, and three correctness bugs surfaced by the
TCK port have been fixed at root.  See the
[migration guide](docs/src/getting-started/migrating.md) for the
v1.x → v2.0.0 upgrade path.

### Added

- **Replication GA.**  All ten v2.0 GA blockers from
  the 2026 review §7 are closed:
  - `ReplicaAckPolicy` honoured on commit (F1).
  - Dispatcher service-name length bounded (F3).
  - `NetworkRestore` wired through the dispatcher path (F2 / F4).
  - Paxos acceptor promises persistent across restart (F5 / F31) —
    split-brain prevention.
  - Election driver wired into `ReplicatedEnvironment::open` (F6).
  - `transfer_master` and `shutdown_group` implemented end-to-end
    (F7 / F8).
  - `become_master` spawns feeders per known replica (F9).
  - `PeerLogScanner` memory bounded (F10).
  - `VLSN` index persistent across restart (F11).
  - Arbiters cannot win Paxos elections (F22).
- 126 JE TCK tests ported across three priority bands
  (data-correctness, high-level APIs, replication + miscellaneous).
  Aggregate: PORTED-EQUIVALENT 147 → 196, PORTED-PARTIAL 62 → 70,
  NOT-PORTED 1 796 → 1 738.
- Wave 6 added the priority-3 (replication-light) and priority-4
  (miscellaneous) bands on top of the v2.0.0-rc1 ports.

### Fixed

Three real Noxu correctness bugs surfaced and fixed at root by
Wave 4-B's JE TCK port and Wave 5's follow-up.  Their regression
tests are now `#[test]` (no longer `#[ignore]`'d):

- **SR9465** — aborted delete-then-reinsert no longer corrupts BIN.
  `Transaction::abort`, `resolved_abort_after_prepare`, and
  `Database::apply_auto_txn_undo` now sort undo records by
  `current_lsn` descending; the entry counter is restored on undo
  of deletes.  Discovered during JE TCK port (Wave 4-B).
- **SR9752 part 2** — aborted dup inserts no longer persist on
  sorted-duplicates DBs.  `put_dup` `PutMode::Overwrite` now
  records undo info like the other branches.  Discovered during
  JE TCK port (Wave 4-B).
- **`testReadDeletedUncommitted`** — uncommitted deletes now
  properly conflict with reads.  The deleter holds an additional
  synthetic-key write lock; readers contest it on `NotFound`, with
  an `owns_write_lock` short-circuit to avoid `read_locks`
  pollution.  Discovered during JE TCK port (Wave 4-B).

### Compatibility

- **Synthetic-key lock IDs** added to the lock-manager protocol for
  missing-key reads (Bug 3 fix above).  Internal protocol change.
- Acceptor and VLSN persistence add small on-disk files in the
  environment directory (`noxu-rep` only).
- Otherwise no user-visible breaking changes vs v1.6.0.

### Known Issues

- JE TCK heavy integration tests (top-level `je.rep`, `je.rep.txn`,
  `je.rep.stream`) require a JE-style `RepTestBase` / `RepEnvInfo`
  harness that did not yet exist in `noxu-rep`.  These remain
  `NOT-PORTED` and were addressed in v2.1.0.
- `noxu-persist` rejects read-only reopen with `allow_create=false`
  (committed as `#[ignore]`'d regression).  Fixed in v2.1.0.

## [2.0.0-rc1] - 2026-05-27

Release candidate for v2.0.0.  All ten `noxu-rep` GA blockers
closed plus 87 JE TCK ports and three Noxu correctness fixes; see
v2.0.0 above for the consolidated changelog.  Wave 4-A finished
the rep GA, Wave 4-B / 4-C ported the priority-1 + priority-2 TCK
bands, and Wave 5 fixed the three correctness bugs Wave 4-B
surfaced.  Test gate: 5 501 tests, all passing.

## [1.6.0] - 2026-05-27

Major architectural release: foreign-key constraints, automatic
secondary maintenance, sorted-dup secondaries, crash-durable XA,
DPL schema evolution, derive macros, `DiskOrderedCursor`.

### Added

- **Foreign-key constraints** (Abort / Cascade / Nullify) implemented
  end-to-end with cycle detection.  Closes audit C2.
- **Automatic secondary maintenance** — `Database::put` and
  `Database::delete` drive registered secondaries inside the user's
  txn.  Manual `update_secondary` still works for compatibility but
  is no longer required.  Closes audit C3.
- **Sorted-dup secondary indexes** — many primaries can share a
  secondary key.  Closes audit C4.
- **Crash-durable XA** — `TxnPrepare` WAL frame plus recovery
  integration.  `xa_recover` / `xa_commit` / `xa_rollback` work
  end-to-end across process restart.  Closes audit C5.
- **DPL schema evolution** wired into the open path; per-record
  class-version envelope; `Mutations` / `Renamer` / `Deleter` /
  `Converter` support.
- **`@Entity` / `@PrimaryKey` / `@SecondaryKey` proc-macros** in a
  new `noxu-persist-derive` crate.
- **`DiskOrderedCursor`** — multi-DB high-throughput unordered scan.
- Partial replication GA (5 of 10 blockers): F1, F3, F6, F10, F22.

### Changed

- Typed collections: `StoredMap<K, V, KB, VB>`, `StoredSet`,
  `StoredList` are now parameterised by `EntryBindings`.  All
  `Stored*` methods take `txn: Option<&Transaction>` as the leading
  argument; `TransactionRunner` threads its txn.  Closes
  collections-bind audit findings #1 / #3 / #4 / #11 / #12.
- `StoredList::remove` now compacts.  Closes #5.

### Removed

- **Nested transactions.**  `Environment::begin_transaction` no
  longer accepts a `parent: Option<&Transaction>` argument.  This
  is a compile-time error rather than a runtime error for nested
  callers.

### Compatibility — BREAKING

- WAL log version bumped 1 → 2 (`TxnPrepare` frame added).  Not
  forward-compatible: a v1.5.x reader cannot replay a v1.6.0 WAL.
- `SerdeBinding` payloads carry a 2-byte version header
  (BREAKING on-disk vs pre-Sprint-3 payloads).
- DPL primary-index entries carry a per-record class-version
  envelope (BREAKING on-disk vs pre-v1.6 DPL stores).
- `Database::put` / `Database::delete` now auto-maintain
  registered secondaries — observable behaviour change on the
  user's txn.
- `Stored*` collection method signatures changed (txn argument,
  type parameters).
- `Environment::begin_transaction` parent argument removed.

See the [migration guide](docs/src/getting-started/migrating.md)
for code-level recipes.

### Deferred to v2.0

- Rep GA blockers F2 / F4 / F5 / F7 / F8 / F9 / F11 / F31.
- JE TCK port: ~2 069 `@Test` methods enumerated; priority backlog
  in `docs/src/internal/je-tck-port-2026-05-prioritized-backlog.md`.

## [1.5.1] - 2026-05-26

Polish release closing v1.5.0 deferred items.

### Added

- `Transaction::set_name` / `get_name` (previously stubbed).
- By-txn lock-stat reporting (audit txn-env F14).
- Synthetic auto-commit transactions: every `db.put(None, …)` /
  `db.delete(None, …)` now wraps the operation in a transient `Txn`
  allocated from `TxnManager::begin_auto_txn()`.  Auto-commit and
  explicit-txn lockers share the same id space.
- `LockManager::register_locker_label` / `format_locker` API; deadlock
  messages now use typed locker labels (`auto-txn:42` / `txn:17`).
- `SecondaryDatabase::count` / `exists` / `truncate` (missing in v1.5.0).

### Fixed

- `SecondaryCursor::delete` now cascades to BOTH the secondary entry
  AND the corresponding primary record under the same txn — both
  commit together or abort together.  Closes the F5 sub-item flagged
  in Sprint 4.5.
- Pre-existing TOCTOU bug in `CursorImpl::put` for `PutMode::NoOverwrite`
  / `NoDupData`: the post-lock re-check fired only on `NULL_LSN`
  paths.  Now fires unconditionally.
- NULL-LSN insert races between concurrent auto-commit inserts of the
  same brand-new key now serialise through the lock manager via
  `Lsn::synthetic_key_lock_id(db_id, key)` rather than relying on
  tree latching.
- Recovery-failure typing: now a typed `RecoveryFailure` variant
  rather than a `String`.
- `get_search_key_range` no longer relies on a fragile two-step
  protocol.
- `Database` partial-put length mismatch now returns a typed error
  instead of silently truncating.
- Several previously-decorative `n_sec_*` throughput counters now
  increment.

### Removed — BREAKING

Audit Low/Info dead-code cleanup.  None of these were exercised by
any consumer in the workspace, but external users depending on them
must migrate:

- Types: `ByteComparator`, `DatabaseNamer`, `KeySelector` (and its
  variants), four `PersistError` variants the implementation never
  returned, the unused FK raw-pointer ABI.
- Methods: `Database::compare_keys`, `Sequence::current`,
  `Sequence::get_database`, `Sequence::get_key` (and other unused
  accessors flagged by audits).
- Config fields: `RepConfig::replica_ack_timeout`, `feeder_timeout`,
  `helper_hosts`.

### Compatibility

No on-disk WAL format change.  Auto-commit still writes
`InsertLN` / `DeleteLN` with `txn_id = 0` (no synthetic
`TxnCommit` / `TxnAbort` frames).  Backwards compatible with
v1.4.x / v1.5.0 environments.  Source-level breaking changes are
the dead-code removals above.

## [1.5.0] - 2026-05-26

Public-API audit remediation release.  Closes 6 of 6 critical and 27
of 34 high-severity findings from the May 2026 public API audit, plus
a substantive partial-atomicity gap surfaced during Sprint 4.

### Added

- **Typed errors** for previously-silent failures:
  - `NoxuError::Unsupported` (cursor `SearchLte` / `FirstDup` /
    `LastDup`, nested txn, FK config, secondary collisions).
  - `XaError::CrashDurabilityNotSupported` (XA across restart).
  - `PersistError::SecondariesNotTransactional` (DPL warning).
  - `BindError::VersionMismatch` (`SerdeBinding` decode).
- 2-byte version header on every `SerdeBinding` payload.

### Fixed

- **C1**: `Database::open_cursor(Some(&txn))` no longer silently
  drops the txn — now routes through `make_cursor_for_txn()`.
- **C4**: `insert_sec_key` no longer uses `Put::Overwrite` (which
  lost many-primary-to-one-secondary records).  Now
  `Put::NoOverwrite` plus a typed collision error.  Sorted-dup
  secondaries arrived in v1.6.
- **C6**: DPL `PrimaryIndex` writes no longer always pass `txn=None`;
  all `PrimaryIndex` / `SecondaryIndex` methods now take
  `txn: Option<&Transaction>` as the leading argument.
- F1 active-txns leak; F2 `read_uncommitted` no longer silently
  dropped; F3 durability config no longer ignored; F12 auto-commit
  isolation correct; two latent recovery bugs unmasked by F1.
- Cursor F4: `NextDup` / `PrevDup` on a non-dup database now return
  `NotFound` instead of misbehaving.
- Cursor F5: `SearchBoth` validates the data argument.
- `Database::count()` / `Database::delete(key)` correct on sorted-dup
  databases (delete now removes all dups).
- Sprint 4.5: `SecondaryDatabase::update_secondary` now atomic with
  the user's txn (manual-update pattern), closing F5.
- Secondary F4: `open_cursor` threads its txn.
- XA F1: `mark_write` footgun — fixed via auto-detect.
- Collections F5: `StoredList::remove` rustdoc-vs-body mismatch.
- Collections F6: `next_index` persistence via `StoredList::open`.
- Collections F19: `SerdeBinding` 2-byte version header (above).
- Txn-env F11: nested txn rejected with typed error (parameter
  removed in v2.0).
- Txn-env F16: one-to-one secondary collision rejected with typed
  error.

### Restricted scope (typed errors at the API surface)

- **C2**: `ForeignKeyDeleteAction` Abort / Cascade / Nullify now
  rejected at `SecondaryDatabase::open` with typed
  `NoxuError::Unsupported`.  Full FK arrived in v1.6.
- **C3**: `associate()`-style hook on `Database::put` / `delete`
  documented as a v1.5 limitation; the manual `update_secondary`
  pattern is the workaround.  Auto-association arrived in v1.6.
- **C5**: `xa_prepare` is restricted to in-process with typed
  `XaError::CrashDurabilityNotSupported`.  Crash-durable XA arrived
  in v2.0.

### Compatibility — BREAKING

- DPL `PrimaryIndex`: every method now takes
  `txn: Option<&Transaction>` as the leading argument.
- `SecondaryDatabase::update_secondary`: now takes
  `txn: Option<&Transaction>` as the leading argument.
- `SerdeBinding` adds a 2-byte version header (BREAKING on-disk for
  existing `SerdeBinding` data).
- Several methods that silently no-op'd in v1.4.x now thread their
  arguments correctly — pre-existing lock conflicts in user code
  may surface (this is the bug fix being shipped).

No on-disk format changes for primary KV data.  Backwards compatible
with v1.4.x environments at the storage layer.

### Deferred

- v1.6: collections #1 / #3 / #4 (`Stored*` txn threading and typed
  `StoredMap<K, V>`); persist #10 / #11 / #18 (DPL secondaries
  durable + atomic); automatic `associate()`-style maintenance.
- v2.0: nested-txn parameter removal; crash-durable XA;
  `noxu-rep` GA (10 GA blockers).

Test gate: 5 339 tests, 0 failed.

## Pre-v1.5 (audit baseline)

Pre-v1.5 releases were the audit-driven remediation phase that turned
internal documentation, code comments, and test claims into
verified-against-code facts.  They are summarised here for
historical context; consult the annotated tags
(`git tag -l v1.4.0 --format='%(contents)'`, etc.) for the dense
release notes.

- **v1.4.3** (2026-05-25) — Fixed: `Cursor::get(SearchGte)` returned
  spurious `NotFound` when the seed fell between two BINs and the
  chosen BIN's largest key was less than the seed; the fix walks to
  the next BIN once.  New deterministic and brute-force-oracle
  property tests landed alongside.  No on-disk or API changes.
- **v1.4.2** (2026-05-25) — Fixed: `Cursor::get(SearchGte)` panicked
  in `noxu_tree::tree::compress_key` when the seed was shorter than a
  BIN's learned key prefix (affected prefix-bounded scans over tagged
  keyspaces).  Defensive guard added to `tree::delete_recursive` at
  the matching call site.  No on-disk or API changes.
- **v1.4.1** (2026-05-25) — Closed 26 of 43 audit items from the 2026-05
  claim audit and security review: all 16
  medium / low claim-audit items, 2 of 6 security blockers
  (LOG-2 4 GiB allocation bound, LOG-4 path-traversal closure in
  `NetworkRestore`), and 7 of 10 security important items (TLS-2/3/4
  silent / warn behaviour now `Err`, LOG-3 centralised
  `MAX_ITEM_SIZE`, LOG-5 unknown-entry-type error logging, LOG-6
  VLSN ordering verified during recovery, LOG-7 replicas reject
  non-monotonic VLSN frames).
- **v1.4.0** (2026-05-24) — Added: 1 000-iteration torn-write power-loss
  test sweep, qemu whole-VM kill procedure (Layer 2 of the power-loss
  tests), `noxu-sustained-baseline` 24 h baseline binary emitting
  per-window CSV metrics, and operational runbooks for recovery loops,
  cleaner backlog, election thrash, and slow checkpoints.  No code
  behaviour changes.

## References

### Migration

- [Migration guide](docs/src/getting-started/migrating.md) — code-level
  recipes for every breaking change v1.4 → v2.x.

### Audit reports

The May 2026 public-API audit drove the v1.5.x and v1.6.x sprints.
The original audit reports recorded in this branch:

- the 2026 review —
  noxu-rep audit, 40 findings.
- the 2026 review — aggregate.
- the 2026 review —
  doc-vs-code claim audit (43 items, drove v1.4.1).
- the 2026 review
  — JE port-completeness audit overview (links to api-map / test-map /
  test-quality-spotcheck).

### Decisions

- the 2026 review —
  architectural decisions (1B / 2C / 3B) signed off by the project
  owner; enforced via Sprint 3D.
- the 2026 review
  — typed `Unsupported` errors for restricted surfaces.

### Wave reports

Each sprint and wave landed an internal note documenting motivation,
scope, and test gate.  In commit order:

- Wave 1C — audit Low/Info cleanup
- Wave 2A — secondary database unification
- Wave 2B — collections typed API and txn threading
- Wave 2C-1 — DPL derive macros
- Wave 2C-2 — DPL schema evolution
- Wave 2C-3 — DiskOrderedCursor
- Wave 3-1 — nested-transaction parameter removed
- Wave 3-2 — crash-durable XA
- Wave 4-A — noxu-rep GA finish
- Wave 4-B — JE TCK port (priority 1)
- Wave 4-C — JE TCK port (priority 2)
- Wave 5 — Noxu correctness fixes (TCK regressions)
- Wave 6 — JE TCK port (priority 3 + 4)
- Wave 7 — v2.0.1 polish
- Wave 8 — RepTestBase harness + heavy rep TCK port
- Wave 9-A — noxu-rep fixes (v2.1.1 / v2.2.0)
- Wave 9-B — Stateright spec re-validation
- Wave 9-C — JE TCK port (additional rows)

### How this file is maintained

See the 2026 review
for the format convention, the relationship to git tag annotations,
and the workflow for updating this file on each future release.
