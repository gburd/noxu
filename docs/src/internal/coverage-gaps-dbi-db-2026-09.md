# Coverage gap detail — noxu-dbi / noxu-db (2026-09)

Follow-on to [the coverage baseline](coverage-baseline-2026-09.md), which established that these two
crates are the only ones below the >85% mandate. This file records **which**
functions are uncovered, so the test-writing pass targets real gaps rather than
padding a number.

Measurement: `cargo llvm-cov -p noxu-dbi --json` on `test/coverage-dbi-db`
(worktree `/tmp/w-covdbi`), then de-duplicated across codegen units.

## Measurement artifact: llvm-cov over-counts noxu-dbi's function total

`llvm-cov`'s summary counts **one function record per codegen unit / test
binary**, so a `pub fn` reachable from four integration-test binaries appears
four times. Its 722-function denominator collapses to **817 → 817** distinct
symbols once `Cs<disambiguator>_` is normalised away, and its `78.25%` becomes
**75.4%** on distinct symbols (worse, not better — the duplicates that *were*
covered inflated the ratio).

Second artifact, this one in our favour: **20** of the 201 distinct uncovered
symbols in `environment_impl.rs` are v0 *placeholder-generic* records
(`...pE`) — the uninstantiated shell llvm emits for
`EnvironmentImpl::new/new_with_config/new_with_config_inner/from_dbi_config`,
which are generic over `impl AsRef<Path>`. Every real monomorphisation IS
covered. These 20 are structurally uncoverable and must be treated as an
accepted gap.

So the honest distinct-symbol picture is 616 covered / 817 total, of which
**181 are genuinely uncovered** and 20 are artifacts.

## noxu-dbi uncovered, by file (distinct symbols)

| file | top-level fns missed | closures missed | note |
|---|---:|---:|---|
| `environment_impl.rs` | 33 (13 real + 20 placeholder) | 79 | biggest gap |
| `cursor_impl.rs` | 14 | 23 | read/write hot path |
| `database_impl.rs` | 7 | 9 | |
| `disk_ordered_cursor_impl.rs` | 2 | 6 | DOS producer thread |
| `replica_ack.rs` | 4 | 0 | recovered-commit VLSN path |
| `trigger.rs` | 4 | 0 | **all four are trait default methods** |
| `disk_limit.rs` | 3 | 0 | |
| `database_config.rs` | 3 | 0 | 2 are `Debug` impls |
| `backup_manager.rs` | 2 | 0 | |
| `memory_budget.rs` | 2 | 0 | |
| `file_manager_scanner.rs` | 0 | 4 | `parse_payload` error arms |
| `dup_key_data.rs` | 0 | 2 | |
| `replica_replay.rs` | 1 | 0 | |
| `node_sequence.rs`, `db_tree.rs` | 1 each | 0/1 | `Default` impls |

### Classification

**(a) Real untested logic — write a test**

* `environment_impl.rs`: `truncate_database`, `compress_all`, `run_cleaner`,
  `relog_live_catalog`, `discard_extinct_records`, `refresh_disk_limit`,
  `get_throughput_snapshot`, the recovered-prepared-txn trio
  (`recovered_prepared_txns` / `take_recovered_prepared_lns` /
  `forget_recovered_prepared_txn`), `n_lns_extinct` / `enqueue_erase` /
  `is_record_extinction_active`, `log_delete_ln`,
  `set_exception_sink` + `exception_dispatcher`.
* `cursor_impl.rs`: `search_lte`, `get_first_dup`, `get_last_dup`,
  `put_with_expiration`, `upgrade_current_to_write_lock`, `attach_txn`,
  `get_current_lsn`.
* `database_impl.rs`: `collect_btree_stats`, `update_key_expiration`,
  `entry_count`, `triggers` / `has_user_triggers`.
* `disk_limit.rs`: `effective_limit`, `violation_message`,
  `last_total_log_size`.
* `replica_ack.rs`: the three recovered-commit VLSN entry points.
* `trigger.rs`: the four default methods (`commit`/`abort`/`add_trigger`/
  `remove_trigger`) — a real test registers a trigger that overrides *nothing*
  and asserts the defaults are no-ops rather than panics, which is the actual
  documented contract ("a trigger that does not implement `TransactionTrigger`
  simply has no commit/abort behaviour").

**(b) Dead / unreachable — note for a follow-up removal pass, do not delete now**

* `DbiError::DatabaseExists` — a "compatibility alias" for
  `DatabaseAlreadyExists` with an identical `#[error]` string. No constructor
  anywhere in the workspace. Candidate for removal.
* `DbiEnvConfig` fields explicitly documented "Reserved / not yet implemented.
  Stored for future use; not read by any subsystem": `env_check_leaks`,
  `env_forced_yield`, `env_fair_latches`, `env_latch_timeout_ms`,
  `env_ttl_clock_tolerance_ms`, `env_expiration_enabled`, `env_db_eviction`.
  These are config surface with no consumer; they inflate the line count of
  `Default::default` (which IS covered) but nothing reads them.

**(c) Hard-to-trigger error paths — fault-injection / edge test**

* `cursor_impl::set_cursor_fail_after` / `clear_cursor_fail_flag` — these are
  the crate's own fault-injection hooks, `#[cfg(any(test, feature =
  "testing"))]`, and are used from `noxu-db`'s tests but never from
  `noxu-dbi`'s. Exercising them from a `noxu-dbi` test both covers them and
  covers the `log_ln_write` failure arm they gate.
* `file_manager_scanner::parse_payload` malformed-entry arms (4 closures).
* `disk_ordered_cursor_impl::produce` / `clone_dbi_err` — producer-thread
  error propagation.

**(d) Trivial boilerplate — accepted, no test**

* The 20 placeholder-generic `environment_impl` records (structurally
  uncoverable).
* `Debug` impls: `DatabaseImpl`, `DatabaseConfig`, `ConfigComparator`.
* `Default` impls: `DbTree`, `NodeSequence`, `DatabaseTree`, `BackupManager`,
  `DiskOrderedCursorOptions`.
* Pure one-line accessors with no branch: `get_memory_budget`,
  `get_node_sequence`, `get_disk_limit`, `get_creation_time`,
  `is_invalid_flag`, `get_dos_producer_queue_timeout_ms`,
  `evictor_algorithm_name`, `MemoryBudget::max_memory`,
  `tree_memory_counter`, `BackupManager::last_backup_ms`,
  `ReplicaReplay::last_applied_vlsn_handle`, the `CursorImpl::with_*`
  builder setters. Testing a `&self.field` return asserts nothing about
  behaviour — writing such tests is exactly the tautology anti-pattern the
  48 deleted `test_copy`/`test_clone` tests were removed for. Several get
  covered incidentally by the (a) tests above anyway.
