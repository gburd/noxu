# Testing Guide

## Test Categories

### Unit Tests

Unit tests live inside the source file they test, in a `#[cfg(test)]` module at
the bottom of the file. They test a single function or struct in isolation.

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lsn_round_trip() {
        let lsn = Lsn::new(3, 1024);
        assert_eq!(Lsn::from_u64(lsn.as_u64()), lsn);
    }
}
```

### Integration Tests

Integration tests live in `tests/` inside each crate (e.g.,
`crates/noxu-db/tests/`). They test the public API from the perspective of an
external caller. Each test file is a separate compilation unit.

All integration tests that touch the filesystem must create a temporary
directory using the `TempDir` isolation pattern:

```rust
use tempfile::TempDir;

fn open_test_env() -> (TempDir, Environment) {
    let dir = TempDir::new().unwrap();
    let env = Environment::open(dir.path(), EnvironmentConfig::default()).unwrap();
    (dir, env)  // caller must hold TempDir alive for the test's duration
}
```

Never use a fixed path like `/tmp/noxu-test` — tests run in parallel and will
collide.

### Property-Based Tests

Property tests use the `proptest` crate. They live in `#[cfg(test)]` modules
with a `proptest!` macro block. See `crates/noxu-log/tests/` for examples.

```rust
use proptest::prelude::*;

proptest! {
    #[test]
    fn round_trip_packed_int(v in 0u64..=u64::MAX) {
        let encoded = PackedInt::encode(v);
        assert_eq!(PackedInt::decode(&encoded).unwrap(), v);
    }
}
```

### Fuzz Tests

Fuzz targets live in `tests/fuzz/`. They use `cargo-fuzz` and require nightly:

```bash
cargo +nightly fuzz list
cargo +nightly fuzz run fuzz_log_entry -- -max_total_time=3600
```

The six fuzz targets are: `fuzz_log_entry`, `fuzz_bin_entry`, `fuzz_lsn`,
`fuzz_packed_int`, `fuzz_recovery`, `fuzz_replication`.

## Test Runner

Use `cargo nextest` for all test runs. It is faster than `cargo test`, shows
cleaner output, and respects per-test timeouts from `.config/nextest.toml`:

```bash
cargo nextest run --workspace              # all tests
cargo nextest run -p noxu-txn             # one crate
cargo nextest run -p noxu-rep --no-fail-fast  # keep going past first failure
```

## Naming Conventions

- Unit test functions: `test_<what>_<condition>` (e.g., `test_lsn_overflow_returns_error`)
- Integration test files: `<subsystem>_test.rs` (e.g., `concurrency_test.rs`)
- Property test functions: `round_trip_*`, `invariant_*`, or describe the property

## Test Isolation

Key rules:

1. **No shared state** — each test creates its own `TempDir` and opens a fresh
   `Environment`.
2. **No fixed ports** — replication tests bind to port `0` (OS assigns an
   ephemeral port).
3. **No `sleep`** — use channels, condvars, or retry loops with a deadline.
4. **Always close before asserting** — WAL is flushed on `env.close()` (or when
   the `Environment` is dropped). Do not assert file contents while the env is
   still open.
5. **Drop order matters** — drop `Database` handles before dropping the
   `Environment`. The environment's WAL flush happens on its drop.

## Running the Full Test Suite

```bash
# Matches the CI command exactly
cargo nextest run --workspace --all-features
```

For replication tests (noxu-rep), the suite takes approximately 90 seconds on a
modern workstation due to the election timeout and chaos test durations.

## Debugging a Failing Test

```bash
# Show all stdout/stderr from the test
cargo test -p noxu-rep -- test_name --nocapture

# Enable debug logging
RUST_LOG=noxu_rep=debug cargo test -p noxu-rep -- test_name --nocapture

# Enable full backtraces
RUST_BACKTRACE=full cargo test -p noxu-rep -- test_name --nocapture

# Run the test in isolation (nextest runs each test in its own process)
cargo nextest run -p noxu-rep -E 'test(test_name)'
```

## Adding Tests for a Noxu Feature

When porting a Noxu feature, locate its Java tests in `_/je/` under the same
package path. Port
each `@Test` method to a Rust `#[test]`. Preserve the test names (translated to
snake_case) and the intent of each assertion.

## Slow / Stress Tests

Several tests are marked `#[ignore]` because they are too slow for routine
CI runs (stress tests, torture tests, performance benchmarks).  Each ignored
test includes a reason string documenting why it is ignored:

```rust
#[test]
#[ignore = "stress: 200 threads × disjoint writers, up to 120 s wall time; run with --ignored"]
fn test_200_thread_disjoint_writers() { ... }
```

To run all ignored tests in a crate:

```bash
cargo test -p noxu-db -- --ignored
cargo test -p noxu-rep -- --ignored
cargo test -p noxu-xa -- --ignored
```

To run a specific ignored test:

```bash
cargo test -p noxu-db --test isolation_test test_64_thread_concurrent_readers -- --ignored
```

The slow test inventory (as of Wave 11-S):

| Crate | Test | Reason |
|---|---|---|
| `noxu-db` | `test_64_thread_concurrent_readers` | stress: 64 readers × 1000 keys × 1000 txns |
| `noxu-db` | `test_32r32w_concurrent` | stress: 32r/32w × 5000 ops |
| `noxu-db` | `test_200_thread_disjoint_writers` | stress: 200 threads, up to 120 s |
| `noxu-db` | `test_sustained_8r8w_60s` | stress: sustained load 60 s |
| `noxu-db` | `test_checkpoint_under_load_30s` | stress: checkpoint under load 30 s |
| `noxu-rep` | `torture_replication` | torture: multi-node election/failover loop |
| `noxu-xa` | `test_xa_chaos_concurrent` | stress: concurrent XA chaos (≥ 60 s) |
| `noxu-xa` | `test_xa_perf_2pc_vs_single_phase` | perf-benchmark |
| `noxu-xa` | `test_xa_perf_concurrent_multi_cluster` | perf-benchmark |

These tests are intended to run in a nightly CI job via `cargo test --workspace -- --ignored`.

## Deterministic Simulation Testing (DST)

DST makes crash/recovery a pure function of `(seed, workload)` so that a
failure reproduces *exactly* from a single seed. Milestone 1 is a
seed-reproducible **storage-fault crash gate** (`crates/noxu-db/tests/
dst_crash_sweep.rs`).

Unlike `power_loss_sweep.rs` — which SIGKILLs the crash worker at a random
wall-clock millisecond and therefore *cannot* drop in-flight kernel buffers —
the DST sweep drives the engine through the **FaultDisk** layer
(`noxu_log::faultdisk`). For each seed the worker subprocess installs a fault
controller (via the `NOXU_DST_SEED` env var) and, at a *seed-chosen write*,
injects one of:

| Fault | What happens |
|---|---|
| **Torn write** | Write only a prefix of the buffer, then `process::exit` (no `Drop`) — the dropped tail and every later write never reach disk. Byte-precise, reproducible power loss in-process. |
| **Fsync drop** | Acknowledge `fsync` without flushing, then power-cut — writes the engine believed durable vanish. |
| **Disk full** | Return `StorageFull` (`ENOSPC`) from the write. |
| **Corruption** | Flip bytes in the just-written region (bit-rot). |

The parent then reopens the env and asserts the durability invariants:
no-lost-committed-txn (the recovered committed set is a strict prefix),
no-uncommitted-leak, total recovery (reopen never panics), and LSN-monotone
(no later commit kept while an earlier one is dropped).

**Zero production change.** The whole fault layer is gated behind one
process-global `AtomicBool` that production code never sets. Inactive, every
posio call does one relaxed atomic load and takes the real path. DST is
strictly opt-in (it is `RealClock` + real disk everywhere by default).

### Running the fast subset (local dev / PR CI)

```bash
cargo test -p noxu-db --test dst_crash_sweep
```

Runs ~120 seeds in well under 60 s, exercising all four fault kinds plus
no-fault controls, the determinism proof, and the oracle self-check.

### Running the release gate

The **full DST gate is required before a release**; the fast subset is enough
for local work and PRs:

```bash
cargo test -p noxu-db --test dst_crash_sweep --release \
    -- --ignored long_sweep --nocapture
```

Runs 10 000 seeds.

### Reproducing a failing seed

On any failure the sweep prints the exact seed:

```text
FAILURE: NOXU_DST_SEED=4193 -> non-prefix recovery: present keys [0,1,3] have a gap
```

Re-run that one seed against the crash worker directly — the run is
byte-for-byte deterministic, so the same seed reproduces the same crash and the
same recovered state:

```bash
NOXU_DST_SEED=4193 \
NOXU_CRASH_DIR=/tmp/dst-repro \
NOXU_CRASH_MODE=committed_then_uncommitted \
    cargo run -p noxu-db --bin crash_worker
```

The determinism property is verified by `dst_same_seed_reproduces_exactly`,
which runs one torn-write seed twice and asserts byte-identical recovered
state.

### DST Milestone 2 — shuttle concurrency gate

Milestone 1 (above) makes *storage faults* deterministic. Milestone 2 makes
*thread interleavings* deterministic, using
[`shuttle`](https://docs.rs/shuttle): a concurrency-permutation tester that
replaces the `std::sync` synchronisation primitives and `std::thread` with
instrumented look-alike wrappers, explores thread schedules under a seed, and *shrinks*
any failing schedule. It finds concurrency bugs — races, deadlocks, lost
wakeups — in the **real** engine code, complementing M1 (storage faults) and
`noxu-spec` (abstract protocol models).

#### The swap: `noxu_util::dst_sync`

The concurrency-critical modules import their `Mutex` / `Condvar` / thread
spawn from `noxu_util::dst_sync` instead of `std::sync` / `std::thread`. That
module is a **cfg-gated re-export**:

- **default / every production and released build:** `dst_sync` is a
  transparent re-export of `std::sync` + `std::thread` — the compiler sees the
  identical `std` types it always did. `shuttle` is a
  `[target.'cfg(noxu_shuttle)'.dependencies]` dependency, so it is **not even
  in the dependency graph** unless the cfg is set. **Zero production change.**
- **under `--cfg noxu_shuttle` (dev/test only):** the same names resolve to
  `shuttle::sync` + `shuttle::thread`, and the modules' locks become
  schedulable by shuttle.

The shuttle tests live in `crates/*/tests/shuttle_*.rs`, each guarded by
`#![cfg(noxu_shuttle)]`, so under the default cfg they compile to an empty test
binary.

#### Which protocols are covered

| Protocol | Status | Why |
|---|---|---|
| `DaemonManager` shutdown / wakeup (`shuttle_daemon_shutdown.rs`) | **Green gate** | The shutdown wakeup is an explicit `notify()`, so liveness does not rely on a timeout — shuttle can prove deadlock-freedom. |
| `FsyncManager` group-commit (`shuttle_fsync_manager.rs`) | **Green gate** (DST wave 2) | The leader hand-off previously recovered a lost `wakeup_one` via `LOG_FSYNC_TIMEOUT`, which shuttle cannot model. DST wave 2 **fixed** the lost-wakeup (a `leader_notified` predicate-before-wait flag, the same class as the `WakeHandle` pre-check), so the hand-off is timeout-independent and the full safety oracle (`DurableImpliesLogged`, `FsyncedNeverDecreases`, coalescing, failure fan-out) now runs green over 5000 interleavings. Reverting the fix makes the oracle deadlock, so the gate is not blind. |
| `lock_manager` deadlock detection (`shuttle_lock_manager.rs`) | **Green gate** (DST wave 2) | Routes the shard-table / waiter-graph `Mutex` and per-waiter grant `Condvar` through `noxu_util::dst_sync_pl`; drives the 50 ms re-detection slice via a `SimClock` (`LockManager::with_config_clock`). Asserts no-deadlock-undetected + victim-consistency (a two-lock cycle aborts exactly one victim) and no lost wakeup on grant (`WriteLocksExclusive`), mapped to `noxu-spec` `lock_manager_deadlock`. |
| `log_buffer` segment pin/release | **Deferred** | The segment latch is a `noxu_sync::RawMutex` (`lock_api::RawMutex` shape); shuttle 0.9 exposes no `lock_api::RawMutex`, and the `RawMutex::INIT` const requirement blocks a clean wrapper. The segment's other concurrency is raw-pointer `unsafe` shuttle would not schedule. Deferred until a raw-lock-over-shuttle shim is scheduled. |
| `SHARED_CACHE` evictor register / deregister / evict (`shuttle_shared_cache.rs`) | **Green gate** | The cross-environment shared-cache registry interleavings are scheduled through the seam. |
| **B-tree `split_child` / `compress_node` (`shuttle_bin_split.rs`)** | **Green gate** (DST tree coverage) | Routes the tree-node latch through `noxu_util::dst_sync_pl` under the cfg (production stays byte-identical `parking_lot::RwLock`); the hand-over-hand `read_arc()` descent is backed under the cfg by `noxu_latch::dst_arc_guard` (an Arc-owning read guard the `#![forbid(unsafe_code)]` tree/util crates cannot host), so the whole tree is schedulable. shuttle races `split_child` against an INCompressor-style merge-clear (and two concurrent splitters) on ONE shared child — the drop→reacquire check-then-act window that let the BIN-split bug (`bug-bin-split-concurrency.md`) escape into a 96-thread benchmark instead of DST. Asserts no-panic + split-atomicity + key-order, mapped to `noxu-spec` `btree_latching` (`AtMostOneSplit` / `NoLostWrites`). **Not vacuous:** reverting the v7.2.2 re-check makes shuttle find the `SplitEntries::get_key` out-of-bounds panic — the identical benchmark symptom. |
| **`TxnManager` begin/commit/abort (`shuttle_txn_commit.rs`)** | **Green gate** (DST txn/cursor coverage) | Routes `TxnManager.all_txns` through `noxu_util::dst_sync_pl`, `group_commit` + the lock-manager label registry through `noxu_util::dst_sync`, and `next_txn_id` through `noxu_util::dst_sync::atomic` (default build byte-identical). Explores concurrent begin/commit/abort; asserts **txn-id uniqueness** (no two txns share an id), **commit/abort atomicity** (exactly one of committed/aborted, never both/neither), and **`all_txns` integrity** (no lost or leaked map entry). Invariants map to `noxu-spec` `wal_commit` (monotonic allocator, 2-state committed). **Not vacuous:** replacing `next_txn_id.fetch_add` with a racy load+store makes shuttle report `duplicate txn id 1 allocated (ids=[1, 2, 1])`. |
| **`CursorImpl` reposition vs BIN split (`shuttle_cursor.rs`)** | **Green gate** (DST txn/cursor coverage) | Routes the cursor's `db_impl` RwLock through `noxu_util::dst_sync_pl` (default build byte-identical; `dst_sync_pl::RwLock` *is* `noxu_sync::RwLock` under the default cfg, so noxu-db/env callers interoperate unchanged). The tree node latch is already seamed. shuttle races a cursor stepping/repositioning (the CC-1 split-adjustment re-anchor in `retrieve_next`) against a concurrent insert that splits the BIN under it — the concurrent analogue of the sequential CC-1 regression tests. Asserts **no-panic**, **position-valid**, and **no-skip / no-double-return** (a full forward scan across the split visits the live tail exactly once), mapped to `noxu-spec` `btree_latching` (`NoLostWrites`). **Not vacuous:** forcing the CC-1 re-anchor off makes shuttle find a schedule where the scan skips the split-migrated key `"04"` (visited `{"03"}`, expected `{"03","04"}`). |
| **Checkpoint vs mutation — lost-dirty-node (`shuttle_checkpoint_mutation.rs`)** | **Green gate** (DST recovery coverage) | Races the checkpointer's dirty-BIN flush pass — modelled by `Tree::shuttle_checkpoint_flush_bins`, a faithful copy of `noxu_recovery::Checkpointer::flush_one_tree_bins`'s full-BIN path (`collect_dirty_bins` snapshot under read, then per-BIN write-lock + JE X-8 early-exit guard + capture-keys + `clear_dirty_after_full_log`), sans the `LogManager` WAL write — against concurrent inserts that dirty/split the same BINs. Flush and insert serialise on the per-BIN node write lock (JE per-IN latch ordering). Asserts the **lost-dirty-node** invariant (every concurrently-inserted key is captured OR still dirty — never silently clean-but-unflushed), **no-panic** (no half-flushed split), per-BIN key order, and (2nd test) **checkpoint convergence** (a key dirtied during checkpoint N is captured by N+1). Mapped to `noxu-spec` recovery/checkpoint durability. **Not vacuous:** clearing dirty WITHOUT capturing makes shuttle find a clean-but-unflushed key (seed `4701725966304036809`, key `k0000a`). |

> shuttle surfaced two real, latent lost-wakeups masked in production by
> timeouts, both now **fixed**. The `DaemonManager` one (a missing
> predicate-before-wait in `WakeHandle::wait_timeout`, a shutdown stall up to
> the wakeup interval) was fixed in M2. The `FsyncManager` one (a lost
> leader-designation `wakeup_one`, recovered by the fsync timeout — a
> commit/shutdown stall up to `LOG_FSYNC_TIMEOUT`) was fixed in DST wave 2 with
> a `leader_notified` predicate-before-wait flag, which also un-blocked the
> `FsyncManager` safety oracle above.

#### Running the shuttle gate

The shuttle gate is part of the **release** DST gate (like M1's long sweep), not
required for local dev. It needs the `noxu_shuttle` cfg via `RUSTFLAGS`:

```bash
# All shuttle targets:
RUSTFLAGS="--cfg noxu_shuttle" cargo test -p noxu-engine --test shuttle_daemon_shutdown
RUSTFLAGS="--cfg noxu_shuttle" cargo test -p noxu-log    --test shuttle_fsync_manager
RUSTFLAGS="--cfg noxu_shuttle" cargo test -p noxu-txn    --test shuttle_lock_manager
RUSTFLAGS="--cfg noxu_shuttle" cargo test -p noxu-evictor --test shuttle_shared_cache
RUSTFLAGS="--cfg noxu_shuttle" cargo test -p noxu-tree   --test shuttle_bin_split
RUSTFLAGS="--cfg noxu_shuttle" cargo test -p noxu-txn    --test shuttle_txn_commit
RUSTFLAGS="--cfg noxu_shuttle" cargo test -p noxu-dbi    --test shuttle_cursor
RUSTFLAGS="--cfg noxu_shuttle" cargo test -p noxu-tree   --test shuttle_checkpoint_mutation

# The M1.1 parking_lot-over-shuttle wrapper self-test:
RUSTFLAGS="--cfg noxu_shuttle" cargo test -p noxu-util --test shuttle_dst_sync_pl

# Reproduce a specific shuttle schedule (shuttle prints a failing seed and a
# replayable schedule string on failure):
SHUTTLE_RANDOM_SEED=12345 RUSTFLAGS="--cfg noxu_shuttle" \
    cargo test -p noxu-engine --test shuttle_daemon_shutdown
```

The shared invariant asserts the shuttle tests check
(`noxu_util::dst_invariants`) are the same safety properties the `noxu-spec`
`wal_commit` model checks against the abstract protocol — `LsnMonotone`,
`FsyncedNeverDecreases`, `DurableImpliesLogged` — now checked against the real
code at every explored interleaving. This is the "specs become the DST
oracle" synergy: write each invariant once, check it two ways.

#### Coverage scope and the road to "100%"

"100% DST coverage" is defined as **all `noxu-sync` / `std::sync` /
`parking_lot`-based concurrency protocols in the core engine** — it explicitly
excludes the `tokio`-async replication networking in `noxu-rep`, which shuttle
cannot model (shuttle instruments `shuttle::sync` + `shuttle::thread`, not the
tokio runtime). Rep's async loops are covered by tokio-level tests and by
`noxu-spec` protocol models instead.

Against that scope: **8 protocols are gated** (the table above), **1 is
hard-blocked by shuttle 0.9** (`log_buffer`'s `lock_api::RawMutex` segment
latch has no safe shuttle 0.9 shape), and **2 are sequenced follow-ups**
(recovery-vs-mutation, rep sync state machines), both now tractable because the
tree, txn, and cursor seams are landed. The one gap that mattered most — the
BIN-split check-then-act race that a benchmark had to catch instead of DST — is
closed by `shuttle_bin_split.rs`, and the two adjacent races that could have
hidden the same way (a torn commit/abort `all_txns` map; a cursor skipping a
split-migrated key) are now closed by `shuttle_txn_commit.rs` /
`shuttle_cursor.rs`. Maintainers with the local
`.agent/archived-audits/dst-coverage-map.md` (gitignored) have the full
per-subsystem breakdown.

### DST Milestone 1.1 — clock thread-through + parking_lot-over-shuttle

M1 added the injectable `Clock` (`noxu_util::{Clock, RealClock, SimClock}`) but
left two seams for M1.1, both now done:

1. **Clock threaded through the remaining control-flow time sites.** A
   `SimClock` can now drive every timeout-relevant clock read:
   - `FsyncManager::with_clock` — the group-commit wait and `LOG_FSYNC_TIMEOUT`
     recovery.
   - `LockManager::with_config_clock` — the lock-wait timeout and 50 ms deadlock
     re-detection slice.
   - `DaemonManager` is intentionally *not* clock-threaded (config `Duration` +
     notify-driven shutdown; nothing to virtualise).

   Every existing constructor keeps defaulting to `RealClock`, so the default
   build has **zero** production behavior change.

2. **`noxu_util::dst_sync_pl`: a `parking_lot`-over-shuttle wrapper.** M2's
   `dst_sync` only swaps `std::sync`, so only `std`-shaped modules
   (`FsyncManager`, `DaemonManager`) could be shuttle-tested. `dst_sync_pl`
   presents the `parking_lot` shape (`lock() -> guard`,
   `wait_for(&mut guard, dur)`) that `noxu-sync`-based modules use:
   - Default build: a transparent re-export of the real `noxu-sync` types —
     zero cost, shuttle absent from the graph.
   - `#[cfg(noxu_shuttle)]`: fully-safe wrappers over `shuttle::sync`.

   **The timed-wait crux.** shuttle 0.9's `Condvar::wait_timeout` never times
   out. The wrapper's `wait_for` registers a `SimClock` deadline; the harness
   calls `advance_and_fire(clock, dur)` to advance simulated time and notify
   waiters whose deadline elapsed, which then observe `timed_out() == true`. A
   level-triggered fired-flag plus re-notification of pending fires closes the
   notify-before-block gap, so a **clock-driven timed wait fires
   deterministically** under shuttle+`SimClock`. This is what unblocks a
   `lock_manager` / `FsyncManager` oracle whose liveness depends on a timeout.
   The self-test `noxu-util/tests/shuttle_dst_sync_pl.rs` proves the wrapped
   `Mutex` is schedulable and the clock-driven timeout fires on every explored
   interleaving.
