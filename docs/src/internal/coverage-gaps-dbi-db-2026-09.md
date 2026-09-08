# Coverage gap detail — noxu-dbi / noxu-db (2026-09)

Follow-on to [the coverage baseline](coverage-baseline-2026-09.md), which
established that these two crates are the only ones below the >85% mandate. This
file records **which** functions and branches are uncovered, so the
test-writing pass targets real gaps rather than padding a number.

Measurement: `cargo llvm-cov -p <crate> [--lib] [--branch] --json` on
`test/coverage-dbi-db`, then de-duplicated across codegen units.

## Result

| crate | axis | baseline | after | target |
|---|---|---:|---:|---|
| noxu-dbi | region | 81.28% | **87.81%** | PASS |
| noxu-dbi | function | 78.25% | **88.04%** | PASS |
| noxu-dbi | line | 78.95% | **86.14%** | PASS |
| noxu-dbi | branch | 54.30% | **61.43%** | FAIL — see ceiling below |
| noxu-db (`--lib`) | region | 81.10% | **91.31%** | PASS |
| noxu-db (`--lib`) | function | 73.60% | **89.38%** | PASS |
| noxu-db (`--lib`) | line | 77.20% | **89.18%** | PASS |
| noxu-db (`--lib`) | branch | 59.80% | **68.06%** | FAIL — see ceiling below |

Region, function and line coverage are over target on both crates. **Branch is
not**, and the rest of this file is the honest accounting of why.

## Measurement artifacts worth knowing

**1. `llvm-cov` over-counts the function total.** Its summary emits one
function record per codegen unit / test binary, so a `pub fn` reachable from
four integration-test binaries appears four times. On the baseline measurement
`noxu-dbi`'s 722-function denominator collapsed to 817 *distinct* symbols and
its reported `78.25%` became **75.4%** — worse, not better, because the
duplicated records that *were* covered inflated the ratio.

**2. Some uncovered `environment_impl` symbols are structurally
uncoverable.** 20 of the baseline's 201 uncovered `environment_impl.rs` symbols
were v0 *placeholder-generic* records (`…pE`): the uninstantiated shell llvm
emits for `EnvironmentImpl::new` / `new_with_config` / `new_with_config_inner` /
`from_dbi_config`, which are generic over `impl AsRef<Path>`. Every real
monomorphisation is covered. These must be treated as an accepted gap.

**3. Branch coverage counts a `#[cfg(test)]` module's own branches.** In
`noxu-dbi` the in-module test code contributes 40 branch arms, of which **0 are
covered** — they are the failure arms of `assert!` / `matches!`, which by
construction are never taken while the tests pass. That drags the reported
number down by roughly 2 points and cannot be fixed by writing tests. It can
only get worse as more tests are added, which is one reason branch coverage
moved slower than function coverage during this pass.

## The branch ceiling, and why it is where it is

The remaining branch gap is concentrated in six files:

| file | missed arms | branch |
|---|---:|---:|
| `noxu-dbi/cursor_impl.rs` | 193 / 446 | 56.7% |
| `noxu-dbi/environment_impl.rs` | 103 / 276 | 62.7% |
| `noxu-db/environment.rs` | 53 / 176 | 69.9% |
| `noxu-db/secondary_database.rs` | 53 / 116 | 54.3% |
| `noxu-db/database.rs` | 38 / 116 | 67.2% |
| `noxu-db/transaction.rs` | 37 / 126 | 70.6% |

Inspecting the uncovered arms line by line, they fall into four groups. Only the
first is worth more tests.

### (a) Multi-subsystem interaction states — reachable, expensive

Arms gated on a *combination* of wired subsystems, e.g. in
`commit_with_durability`:

```rust
if !self.read_only && logged_data && durability.replica_ack
    && let Some(coord) = &self.replica_coordinator
```

Reaching the replica-ack arm needs an installed `ReplicaAckCoordinator`, i.e. a
replication harness. Similarly, `environment_impl`'s recovery arms
(`make_invisible` on a corrupt-entry rollback, `force` failing on a specific
file set) need a crash-injection harness. These are legitimately reachable but
belong in `noxu-rep` / crash-recovery integration tests, not in a coverage pass
on these two crates.

### (b) Defensive arms on already-validated invariants

`cursor_impl.rs` has ~30 uncovered arms of the shape:

```rust
if let Some(tree) = db.get_real_tree() { … }
```

The `None` arm fires only if a `DatabaseImpl` has no tree, which
`DatabaseImpl::new` makes impossible (it always calls `build_tree`). Same
pattern for `if let Ok(mut guard) = arc.write()` — the `Err` arm is lock
poisoning, which this codebase treats as fatal. Forcing these would mean adding
test-only mutators to break invariants the type system otherwise maintains, and
the resulting test would assert nothing about real behaviour.

### (c) Error-propagation arms behind `?`

Each `?` on a fallible subsystem call is a branch. Covering them one by one
needs per-call-site fault injection at the log/tree/lock layer. `noxu-dbi` has
its own hook for exactly this (`set_cursor_fail_after`, now tested), but it gates
only the two cursor state checks. A general facility would be a real
engineering project with its own design questions, not a test-writing task.

### (d) The `#[cfg(test)]` assertion arms from artifact 3 above

Structurally uncoverable.

### Realistic ceiling

For `noxu-dbi`, production-code branch coverage (excluding the test-module arms)
is **49.0%** of *arms*; llvm-cov's own file summaries put the file-level figure
at 61.4%. Closing group (a) — the replication and crash-injection harnesses —
would plausibly reach the mid-70s. Groups (b) and (c) put a hard ceiling well
below 100%, and **>85% branch is not reachable for these two crates without
either a fault-injection framework or tests that assert nothing**.

The honest recommendation is to hold `noxu-dbi` and `noxu-db` to the
region/function/line mandate (now met) and track branch coverage as a
diagnostic, revisiting it if and when a fault-injection facility lands.
`noxu-log` sits in the same position at 72.1% branch, so this is not specific to
these two crates.

## Dead code found, for a follow-up removal pass

Noted rather than deleted, per the scope of this pass.

* `DbiError::DatabaseExists` — a "compatibility alias" for
  `DatabaseAlreadyExists` with a byte-identical `#[error]` string and no
  constructor anywhere in the workspace.
* `DbiEnvConfig` fields documented "Reserved / not yet implemented. Stored for
  future use; not read by any subsystem": `env_check_leaks`, `env_forced_yield`,
  `env_fair_latches`, `env_latch_timeout_ms`, `env_ttl_clock_tolerance_ms`,
  `env_expiration_enabled`, `env_db_eviction`. Config surface with no consumer.
  They are threaded all the way from `EnvironmentConfig` through
  `Environment::open` into `DbiEnvConfig` and then read by nothing.
* `SecondaryDatabase::delete_all_for_primary` and `delete_sec_key` /
  `make_inner_cursor` carry `#[allow(dead_code)]` with a note that the
  FK-cascade cleanup is "not yet wired into delete".

## Bugs found while writing these tests

Three, all filed in `CHANGELOG.md`:

1. **`TreeStats::n_entries` counted upper-IN routing slots as records** —
   `Database::stats(fast = false)` and `Database::preload` over-reported the
   record count, growing with tree height. Fixed (field renamed
   `n_leaf_entries`); the two pre-existing `collect_stats` unit tests had been
   written *around* the defect, one asserting only `n_entries >= 1` and the other
   computing `n_entries - n_ins` and calling it a "rough check".
2. **`Environment::invalidate()` did not invalidate open `Database`
   handles** — two separate flags; the one `Database` caches for its lock-free
   `check_open` fast path was not being set, so `env.is_valid()` reported
   `false` while open handles kept serving reads and accepting writes. Fixed.
3. **`TransactionConfig::read_only` does not prevent writes** — NOT fixed;
   it changes behaviour that currently "works" for callers who set the flag and
   got writes anyway, so it needs its own commit. Current behaviour is pinned by
   `transaction.rs::read_only_transactions_do_not_yet_reject_writes`, which
   names it as a gap and will fail when the guard is added.

## Accepted gaps (no test written, deliberately)

Per this repo's anti-tautology culture — 48 tautological `test_copy` /
`test_clone` tests were deleted after an external review named that
anti-pattern — the following were left uncovered rather than padded:

* `Debug` impls: `DatabaseImpl`, `DatabaseConfig`, `ConfigComparator`,
  `Triggers`, `Comparator`.
* `Default` impls that forward to `new()`: `DbTree`, `NodeSequence`,
  `DatabaseTree`, `BackupManager`, `DiskOrderedCursorOptions`.
* Branch-free one-line accessors returning `&self.field`:
  `get_memory_budget`, `get_node_sequence`, `get_disk_limit`,
  `get_creation_time`, `BackupManager::last_backup_ms`,
  `ReplicaReplay::last_applied_vlsn_handle`, `MemoryBudget::tree_memory_counter`.
  Several are covered incidentally by the behavioural tests anyway.
* The 20 placeholder-generic `environment_impl` records.
