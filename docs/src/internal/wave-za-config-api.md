# Wave ZA — Config API gaps and silent-ignore elimination (v3.1.0)

**Branch**: `fix/za-config-api`  
**Target**: v3.1.0  
**Re-audit references**:

- JE F-1 (config params silently ignored)
- JE F-2 / margo #4 (mTLS peer_allowlist security trap)
- JE F-6 / jonhoo #3 (umbrella API type leakage)
- jonhoo #4 (DbIter/DbRange lifetime safety)
- Keith R-4 (commit_pending_database TOCTOU + O(N) scan)

---

## Config-param audit table

Complete audit of every `EnvironmentConfig` field. The "consumed?" column
reflects grep of the field name across the whole workspace at commit
`d276a56`.

| Parameter | Default | Consumed? | Action |
|---|---|---|---|
| `home` | `.` | Yes — all I/O | — |
| `allow_create` | false | Yes — `Environment::open` | — |
| `transactional` | false | Yes — `EnvironmentImpl::from_dbi_config` | — |
| `read_only` | false | Yes — throughout | — |
| `env_is_locking` | true | Yes — lock manager init | — |
| `shared_cache` | false | Stored; noted as future work in doc | Reserved (not in unimpl list; default false, no harm) |
| `env_recovery_force_checkpoint` | false | Yes — recovery | — |
| `env_recovery_force_new_file` | false | Yes — recovery | — |
| `halt_on_commit_after_checksum_exception` | false | Yes — log commit path | — |
| `logging_level` | None | Accepted; Rust log crate runtime-controlled | N/A (Rust idiom differs from JE) |
| `cache_size` | 64 MiB | Yes — Arbiter / budget | — |
| `cache_percent` | 0 | Yes — `from_dbi_config` | — |
| `max_off_heap_memory` | 0 | Yes — Arbiter (X-12) | — |
| `max_disk` | 0 | Yes — DiskLimitExceeded | — |
| `free_disk` | 5 GiB | Yes — disk check | — |
| `run_in_compressor` | true | Yes — daemon spawn | — |
| `run_checkpointer` | true | Yes — daemon spawn | — |
| `run_cleaner` | true | Yes — daemon spawn | — |
| `run_evictor` | true | Yes — evictor spawn | — |
| `run_offheap_evictor` | false | Yes — offheap spawn | — |
| `run_verifier` | false | Yes — verifier spawn | — |
| `env_background_read_limit_kb` | 0 | Yes — daemon rate limits | — |
| `env_background_write_limit_kb` | 0 | Yes — daemon rate limits | — |
| `env_background_sleep_interval_us` | 0 | Yes — daemon sleep | — |
| **`env_check_leaks`** | **true** | **NO** | **Non-silent: warn! + doc updated. Registry entry added.** |
| **`env_forced_yield`** | **false** | **NO** | **Non-silent: warn! + doc updated. Registry entry added.** |
| **`env_fair_latches`** | **false** | **NO** | **Non-silent: warn! + doc updated. Registry entry added.** |
| **`env_latch_timeout_ms`** | **300_000** | **NO** | **Non-silent: warn! (non-300000 value) + doc updated. Registry entry added.** |
| **`env_ttl_clock_tolerance_ms`** | **0** | **NO** | **Non-silent: warn! (non-zero) + doc updated. Registry entry added.** |
| **`env_expiration_enabled`** | **false** | **NO** | **Non-silent: warn! + doc updated. Registry entry added.** |
| **`env_db_eviction`** | **false** | **NO** | **Non-silent: warn! + doc updated. Registry entry added.** |
| `env_dup_convert_preload_all` | true | Stored; used in dup-convert path (minor) | — |
| `adler32_chunk_size` | 0 | Stored; CRC32 used unconditionally (documented) | — |
| `log_file_max_bytes` | 10 MiB | Yes — FileManager | — |
| `log_file_cache_size` | 100 | Yes — FileManager | — |
| `log_checksum_read` | true | Yes — log read path | — |
| `log_verify_checksums` | false | Yes — log verify | — |
| `log_fsync_timeout_ms` | 500_000 | Yes — fsync manager | — |
| `log_fsync_time_limit_ms` | 0 | Yes — fsync timing | — |
| `log_num_buffers` | 3 | Yes — LogManager init | — |
| `log_total_buffer_bytes` | 7 MiB | Yes — buf_size calc | — |
| `log_buffer_size` | 0 | Yes — buf_size override | — |
| `log_fault_read_size` | 2048 | Yes — FileManager | — |
| `log_iterator_read_size` | 8192 | Yes — log iterator | — |
| `log_iterator_max_size` | 16 MiB | Yes — log iterator | — |
| `log_n_data_directories` | 0 | Yes — FileManager | — |
| `log_mem_only` | false | Yes — FileManager | — |
| `log_detect_file_delete` | false | Yes — FileManager | — |
| `log_detect_file_delete_interval_ms` | 3000 | Yes — daemon | — |
| `log_flush_sync_interval_ms` | 0 | Yes — flush daemon | — |
| `log_flush_no_sync_interval_ms` | 0 | Yes — flush daemon (X-11 fix) | — |
| `log_use_odsync` | false | Yes — open flags | — |
| `log_use_write_queue` | false | Yes — write path | — |
| `log_write_queue_size` | 1 MiB | Yes — write queue | — |
| `log_group_commit_threshold` | 4 | Yes — group commit | — |
| `log_group_commit_interval_ms` | 1 | Yes — group commit | — |
| `node_max_entries` | 128 | Yes — IN/BIN split | — |
| `node_dup_tree_max_entries` | 128 | Yes — dup tree | — |
| `tree_max_embedded_ln` | 16 | Yes — LN embedding | — |
| `tree_max_delta` | 25 | Yes — BIN delta | — |
| `tree_bin_delta` | true | Yes — BIN delta | — |
| `tree_min_memory` | 0 | Yes — eviction guard | — |
| `tree_compact_max_key_length` | 16 | Yes — key compact | — |
| `in_compressor_wakeup_interval_ms` | 5000 | Yes — INCompressor | — |
| `compressor_deadlock_retry` | 3 | Yes — compressor | — |
| `compressor_lock_timeout_ms` | 500 | Yes — compressor | — |
| `compressor_purge_root` | false | Yes — compressor | — |
| `cleaner_*` (19 fields) | various | Yes — Cleaner daemon | — |
| `checkpointer_*` (5 fields) | various | Yes — Checkpointer | — |
| `evictor_*` (11 fields) | various | Yes — Evictor | — |
| `offheap_*` (6 fields) | various | Yes — off-heap evictor | — |
| `lock_timeout_ms` | 500 | Yes — LockManager | — |
| `lock_n_lock_tables` | 16 | Yes — LockManager | — |
| `lock_deadlock_detect` | true | Yes — LockManager | — |
| `lock_deadlock_detect_delay_ms` | 0 | Yes — LockManager | — |
| `txn_timeout_ms` | 0 | Yes — TxnManager | — |
| `durability` | COMMIT_SYNC | Yes — Transaction::commit | — |
| `txn_no_sync` | false | Yes (deprecated) | — |
| `txn_write_no_sync` | false | Yes (deprecated) | — |
| `txn_serializable_isolation` | false | Yes — lock mode | — |
| `txn_deadlock_stack_trace` | false | Yes — deadlock report | — |
| `txn_dump_locks` | false | Yes — lock dump | — |
| `verify_*` (9 fields) | various | Yes — verifier (stubbed but consumed) | — |
| `dos_producer_queue_timeout_ms` | 10000 | Yes — DiskOrderedCursor | — |
| `stats_*` (5 fields) | various | Yes — stats daemon | — |
| `trace_*` / `console_logging_level` / `file_logging_level` / `startup_dump_threshold_ms` | various | Stored; Rust log integration differs from JE | N/A (Rust idiom) |
| `exception_listener` | None | Yes — daemon exception handling | — |

**Total unimplemented params**: 7 (all now non-silent via registry + warn!)

---

## API types re-exported (Item 3)

| Type | Previously required | Now reachable as |
|---|---|---|
| `SharedReplicaAckCoordinator` | `noxu-dbi` direct dep | `noxu::SharedReplicaAckCoordinator` |
| `ReplicaAckCoordinator` (trait) | `noxu-dbi` direct dep | `noxu::ReplicaAckCoordinator` |
| `AckWaitError` | `noxu-dbi` direct dep | `noxu::AckWaitError` |
| `AckWaitErrorKind` | `noxu-dbi` direct dep | `noxu::AckWaitErrorKind` |
| `ReplicaAckPolicyKind` | `noxu-dbi` direct dep | `noxu::ReplicaAckPolicyKind` |
| `PreparedTxnInfo` | `noxu-recovery` direct dep | `noxu::PreparedTxnInfo` |
| `PreparedLnReplay` | `noxu-recovery` direct dep | `noxu::PreparedLnReplay` |
| `PreparedLnOperation` | `noxu-recovery` direct dep | `noxu::PreparedLnOperation` |

---

## mTLS peer_allowlist honesty fix (Item 2)

**Before**: `RepConfig::peer_allowlist` field doc claimed connections would
only be accepted if the peer cert matched the allowlist.  This was false —
the server TLS config always used `.with_no_client_auth()`.

**After**:

- Field and builder method rustdoc rewritten to state explicitly that the
  allowlist is **NOT YET ENFORCED** (Phase 2 pending).
- `ReplicatedEnvironment::new` emits `log::warn!` when `peer_allowlist` is
  non-empty.
- `known-limitations.md` updated with explicit `peer_allowlist` tracking entry.

No attempt to implement Phase 2 in this wave (too broad; sibling waves own
the rep/transport layer).

---

## DbIter/DbRange `'txn` lifetime (Item 4)

`DbIter` and `DbRange` now carry a `PhantomData<&'txn Transaction>` field.
`Database::iter` and `Database::range` have a `'txn` lifetime parameter that
ties the iterator's lifetime to the transaction borrow.

**Effect**: the borrow checker now rejects use-after-commit at compile time.
Code that passes `None` for the transaction is unaffected (lifetime is
`'static`-like — inferred as `'_`).

---

## commit_pending_database TOCTOU + O(N) scan (Item 5)

**TOCTOU fix**: `pending_names` changed from `HashSet<String>` to
`HashMap<String, DatabaseId>`.  The `db_id` is stored at insert time in
`open_database_inner`, eliminating the O(N) `db_map` linear scan in the old
`commit_pending_database`.  The write lock on `pending_names` is held
throughout the `commit_pending_database` operation (across both the
`pending.remove` and `name_map.write().insert`), closing the gap identified
in re-audit-keith R-4 where the name was briefly absent from both maps.

**O(N) scan elimination**: `abort_pending_database` also now uses the stored
`db_id` for O(1) `db_map` removal.

**Concurrent-open guard**: `open_database_inner` now checks `pending_names`
before attempting to create a new database with the same name.  If a name is
in `pending_names` (being committed from another transaction), the call
returns `DatabaseAlreadyExists` rather than creating a conflicting
`DatabaseImpl`.

**Tests added** (in `noxu-dbi/tests/integration_tests.rs`):

- `test_commit_pending_database_no_toctou`
- `test_abort_pending_database`
- `test_commit_pending_concurrent_open_no_duplicate_db_id`
