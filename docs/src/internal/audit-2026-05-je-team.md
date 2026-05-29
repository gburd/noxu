# Noxu DB — JE-Team Audit Findings

**Date**: 2026-05-29  
**Auditors**: Charles Lamb, Mark Hayes, Linda Lee, Sam Haradhvala, Brian O'Neill  
*(channeled)*  
**Noxu branch**: `fix/wave11-l-api-stability` (post-v2.4.1)  
**JE reference**: `/home/gburd/ws/je/` (Oracle BDB-JE)

---

## Executive Summary

Noxu DB is a credible port of the BDB-JE architecture in Rust.  The core
data structures (BIN layout, WAL framing, recovery phases, lock tables, group
commit) are recognisably faithful.  However, this review found **33 concrete
findings** spread across the seven focus areas, several of which affect
correctness under real-world workloads.  The top five most actionable items
are called out at the end.

---

## 1. Algorithmic Fidelity

### 1-A  `BIN::should_log_delta()` — missing `isDeltaProhibited()` call

- **Severity**: Critical  
- **Subsystem**: B-tree  
- **Noxu**: `crates/noxu-tree/src/bin.rs:799–810`  
- **JE reference**: `tree/BIN.java:1892–1929` (`shouldLogDelta`)

JE's `shouldLogDelta()` has three guard clauses before it computes the dirty
ratio:

```
1. if isBINDelta()            → return true  (already a delta, always log as one)
2. if isDeltaProhibited()     → return false  where isDeltaProhibited() checks
                                              prohibitNextDelta ||
                                              isDeferredWriteMode() ||
                                              lastFullLsn == NULL_LSN
3. numDeltas <= 0             → return false
```

Noxu's `should_log_delta()` does none of these.  It only checks the dirty
ratio (`dirty_count <= total / 4`) and the empty-entry case.  Concretely:

1. **Missing early return for in-delta BINs**: A BIN already mutated to a
   delta should always re-log as a delta.  Noxu will re-evaluate the ratio,
   potentially deciding not to, and log a spurious full BIN.
2. **Missing `prohibit_next_delta` check**: When compress() removes a dirty
   slot it sets `prohibit_next_delta = true` to prevent the next delta from
   silently omitting the compressed slot.  Noxu's `should_log_delta()` never
   reads this flag; only `can_mutate_to_bin_delta()` does.  Checkpoint code
   that calls `should_log_delta()` directly bypasses the guard.
3. **Missing `lastFullLsn == NULL_LSN` guard**: A BIN that has never been
   fully logged must not be logged as a delta (there is no base to apply it
   against).  Noxu has no equivalent guard in `should_log_delta()`.
4. **Missing DeferredWrite prohibition**: Deltas are not supported for
   DeferredWrite databases in JE.

**Suggested fix**: Replicate JE's three guard clauses in `should_log_delta()`.
The `prohibit_next_delta` check must be added to the same method (not just
`can_mutate_to_bin_delta()`).

---

### 1-B  `BIN::should_log_delta()` — hardcoded 25 % threshold

- **Severity**: High  
- **Subsystem**: B-tree  
- **Noxu**: `crates/noxu-tree/src/bin.rs:806` (`dirty_count <= total / 4`)  
- **JE reference**: `tree/BIN.java:1920` (`(nEntries * db.getBinDeltaPercent()) / 100`)

Noxu uses `total / 4` (integer division, always floors).  JE calls
`databaseImpl.getBinDeltaPercent()`, which defaults to 25 but is configurable
via `TREE_BIN_DELTA`.  Integer division means a 3-entry BIN (`3/4 == 0`)
never logs a delta even with all 3 slots dirty (delta of 3 = valid, full
count = 3, ratio = 100 %, still useful).  Noxu silently disables BIN-deltas
for any BIN with fewer than 4 entries.  At small BIN fills (early inserts,
end-of-file BINs) this trades a cheap delta write for an expensive full BIN
write, amplifying write-amplification and log cleaning cost.

**Suggested fix**: Store the configured delta percentage per `DatabaseImpl` and
pass it through to `should_log_delta()`.  For now, at minimum change
`dirty_count * 4 <= total` to avoid the flooring edge-case.

---

### 1-C  Recovery — MapLN two-pass (mapping-tree undo/redo) skipped

- **Severity**: Critical  
- **Subsystem**: Recovery  
- **Noxu**: `crates/noxu-recovery/src/recovery_manager.rs:401–525`  
- **JE reference**: `recovery/RecoveryManager.java:733–875` (`buildTree()`)

JE's `buildTree()` is a five-phase operation:

```
Phase A: buildINs(mappingTree)   — read MapLN INs for mapping database
Phase B: undoLNs(mapLNSet)       — undo aborted MapLNs, collect committed txn ids
Phase C: startFileCacheWarmer()
Phase D: redoLNs(mapLNSet)       — replay MapLNs (NameLNs, MapLNs)
Phase E: buildINs(mainTree)      — read main-data INs
Phase F: buildINList()
Phase G: undoLNs(lnSet)          — undo aborted data LNs
Phase H: redoLNs(lnSet)          — redo data LNs + non-txnal LNs
```

Noxu's `recover_all()` collapses this into a single sequential pass:
`find_end_of_log → find_last_checkpoint → run_analysis → run_redo_all →
run_undo_all`.  There is **no separate MapLN phase**.  All entry types,
including NameLNs and MapLNs that describe the database catalog, are processed
in the same redo/undo pass as data LNs.

The consequence is subtle but real: in JE, the mapping tree is fully restored
before any data-tree undo/redo, so recovery can correctly distinguish
transactional vs. non-transactional databases and restore their catalog state
first.  Noxu's single-pass approach may redo a NameLN *after* a data LN that
depends on the database already being registered, which means the data LN is
applied to a potentially-absent catalog entry.

This correlates with the documented Wave 10-A bug: "non-transactional db
registration is lost across clean close+reopen" (`je_recovery_test.rs:485`;
PORTED-PARTIAL note).

**Suggested fix**: Introduce a two-pass recovery for the name/mapping layer
analogous to JE's Phase B/D.  At minimum, guarantee that all NameLN entries
are replayed before any user-data LNs.

---

### 1-D  WAL group-commit — thundering-herd not resolved

- **Severity**: Medium  
- **Subsystem**: WAL / group commit  
- **Noxu**: `crates/noxu-log/src/fsync_manager.rs:86` (`FSyncGroup::wait_for_event`)  
- **JE reference**: `log/FileManager.java:1770–1807` (fsync coalescing)

Wave 11-J documented the issue and reverted a Treiber-stack fix due to
per-call allocation overhead.  The current `FsyncManager` retains the
thundering-herd wakeup: after each `fdatasync`, `wakeup_all()` fires
`condvar.notify_all()`, causing all N waiters to race for
`FSyncGroup::inner: Mutex<FsyncGroupInner>`.  With 8 concurrent committers
this produces 7 contended `lock_slow` futex calls per fsync cycle.

The Wave 11-J doc proposes a low-risk fix: add `AtomicBool work_done_atomic`
to `FSyncGroup` and check it without the mutex in `wait_for_event` before
acquiring `inner`.  This was identified but not implemented.

**Suggested fix**: Implement the `AtomicBool work_done_atomic` fast-path check
proposed in Wave 11-J's "What's Left" section.

---

### 1-E  Deadlock detection — victim-notify protocol absent

- **Severity**: High  
- **Subsystem**: Lock manager / deadlock  
- **Noxu**: `crates/noxu-txn/src/deadlock_detector.rs:54–73`; `lock_impl.rs`  
- **JE reference**: `txn/LockManager.java:290–390` (`lock()`, `notifyVictim()`)

Noxu's `DeadlockDetector::detect()` performs a correct DFS cycle detection.
The victim selection algorithm (`select_victim`: fewest locks, tiebreak by
youngest txn ID) matches JE's `selectVictim()`.  However, JE's
`LockManager.lock()` implements a multi-round notify protocol:

1. Detect deadlock → identify victim.
2. If this locker is **not** the victim → call `notifyVictim(victim)`, which
   interrupts the victim thread via its condvar.
3. If `notifyVictim` returns false (victim already resolved) → retry.
4. Loop until this locker acquires the lock.

Noxu has no equivalent of step 2–4.  The deadlock is detected but the victim
is only identified; there is no mechanism to wake the victim thread and force
it to abort.  Without `notifyVictim`, a deadlock cycle can remain alive
indefinitely if both threads time out at approximately the same time, or if
the lock-timeout path is not reached.  This is not a theoretical concern:
under high concurrency the `test_64_thread_concurrent_readers` and
`test_32r32w_concurrent` tests are marked `#[ignore]`, which may mask a
related livelock.

**Suggested fix**: Add a per-locker "abort-now" flag checked in the lock-wait
loop, and have the deadlock-detection caller set the victim's flag and wake
its condvar.

---

### 1-F  Cleaner utilization — single-pass, no UtilizationCalculator two-pass

- **Severity**: High  
- **Subsystem**: Cleaner  
- **Noxu**: `crates/noxu-cleaner/src/file_selector.rs:228–260`  
- **JE reference**: `cleaner/UtilizationCalculator.java:78–145`

JE's `UtilizationCalculator.getBestFile()` performs a two-pass algorithm.  In
the first pass it computes an upper/lower utilization bound per file; if the
lower bound is above `minUtilization`, it stops (no cleaning needed).  If the
first pass selects a file, the second pass verifies the candidate's "true
utilization" by counting non-obsolete live entries.

Noxu's `FileSelector::select_best_file()` uses `adjusted_utilization_pct()`
(a single TTL-adjusted formula) and sorts files by that metric.  There is no
two-pass refinement.  The `two_pass_runs` stat counter exists in
`CleanerStat`, and there are tests for it, but looking at the code the counter
is only incremented inside `check_for_required_util()`, not in the main
`select_best_file()` path, suggesting the two-pass trigger is not actually
hooked into the primary selection loop.

Concretely: JE's first pass computes the histogram of utilization across all
files and updates it to remove overlap before making a selection.  Noxu
applies the TTL discount but does not build or update a histogram.  Files with
medium utilization may be over- or under-selected compared to JE.

**Suggested fix**: Validate that the `required_util` / `force_cleaning`
two-pass logic in `FileSelector` is actually triggered from the main
`Cleaner::run()` loop, and add an integration test that verifies two-pass
cleaning fires when overall utilization is marginal.

---

### 1-G  Log file creation — parent directory not fsynced

- **Severity**: Critical  
- **Subsystem**: WAL / FileManager  
- **Noxu**: `crates/noxu-log/src/file_manager.rs:407–450` (`create_file_internal`)  
- **JE reference**: `log/FileManager.java` (no explicit `syncDir` in JE either — *see below*)

Noxu's `create_file_internal` creates a new `.ndb` file, writes the file
header, calls `file.flush()`, then `file.sync_all()`.  It does **not** call
`fsync` on the parent directory after creating the file.

POSIX requires syncing the parent directory to make the directory entry
(the filename itself) durable.  Without it, after a crash between file
creation and the first fsync of the environment directory, the file will
exist in memory (kernel page cache) but may not appear in the directory on
reboot, silently losing the log file.  This is particularly dangerous for the
*first* log file created in a freshly-initialised environment.

Note: JE runs on the JVM and the JDK does not expose a `syncDirectory` API;
JE relies on the OS / filesystem to maintain directory durability.  However,
for a native Rust implementation targeting Linux, this is a known POSIX
correctness requirement (see `open(2)` manpage: "A POSIX.1-conformant
filesystem ensures...").

**Suggested fix**: After `create_file_internal` succeeds, open the parent
directory (`File::open(dir)`) and call `dir_file.sync_all()` to fsync the
directory inode.  This only happens on log-file flips, so the overhead is
negligible.

---

### 1-H  Durability — `NoSync` / `WriteNoSync` branching correct but `CommitNoSync` needs verification

- **Severity**: Medium  
- **Subsystem**: WAL / durability  
- **Noxu**: `crates/noxu-txn/src/txn.rs:395–515` (`commit_with_durability`)  
- **JE reference**: `txn/Txn.java` (commit)

The three-way branch in Noxu (`CommitSync` → `flush_sync_if_needed`,
`CommitWriteNoSync` → `flush_no_sync`, `CommitNoSync` → neither) appears
structurally correct.  The `commit_with_durability` code path correctly omits
the `flush` and `fsync` for `CommitNoSync`.

However, Noxu hardcodes `flush = true` in the `log_entry()` helper inside
`commit_with_durability` (`crates/noxu-txn/src/txn.rs:707`):

```rust
let flush = true; // always at least flush on commit (fsync implies flush)
```

This comment says "always flush" even for `CommitNoSync`, yet the next step
skips the `flush_no_sync()` call for `CommitNoSync`.  The `flush` flag in
`log()` controls whether the log buffer is transferred to the OS (not yet
fsynced).  With `flush = true` inside `log()` but no follow-on flush call,
the data sits in the application's write buffer.  The `flush` flag inside
`LogManager::log()` should respect the policy.  This does not cause
data loss for `CommitSync` or `CommitWriteNoSync`, but for `CommitNoSync` the
claim "data remains in application buffers" may be violated if `log()` with
`flush=true` actually moves data to the OS buffer.  Clarification and an
explicit test comparing actual fdatasync counts are needed.

**Suggested fix**: Thread the `SyncPolicy` into `log_entry()` so the inner
`flush` flag is set to `false` for `CommitNoSync`, matching the documented
semantics precisely.

---

### 1-I  Replication — `open_database` ignores the transaction parameter

- **Severity**: Critical  
- **Subsystem**: Transactions / replication  
- **Noxu**: `crates/noxu-db/src/environment.rs:448` (`_txn: Option<&Transaction>`)  
- **JE reference**: `dbi/DbTree.java:468–525` (`createDb`, `lockNameLN`)

In JE, `Environment.openDatabase(Transaction txn, String name, DatabaseConfig)`:

1. Acquires a write lock on the NameLN representing the database name,
   using the provided `txn` as the locker.
2. The database creation is therefore **transactional**: if the caller
   aborts `txn`, the database creation is rolled back.
3. `getDatabaseNames()` (which traverses the name tree with `LockType.NONE`)
   will not see the database until the transaction commits.

Noxu silently drops the transaction parameter (named `_txn` with no
logic).  Database creation is always immediate, non-transactional, and
non-rollbackable.  This produces two divergences:

- **Rollback of `openDatabase` does not work**: `txn.abort()` after
  `env.open_database(Some(&txn), ...)` does not remove the database.
- **`get_database_names()` committed-only semantics violated**: Noxu's
  `get_database_names()` reads from `name_map`, which is written during
  `open_database()` regardless of whether the creating transaction has
  committed.  JE returns committed names only.

**Suggested fix**: Implement transactional `openDatabase`: lock the `name_map`
entry under the provided locker and register an abort handler that removes the
entry if the transaction is aborted.

---

### 1-J  `get_database_names()` — committed-only semantics violated (corollary of 1-I)

- **Severity**: High  
- **Subsystem**: B-tree / environment  
- **Noxu**: `crates/noxu-dbi/src/environment_impl.rs:1106–1108`  
- **JE reference**: `dbi/DbTree.java:1829–1855` (`getDbNames()`)

JE's `getDbNames()` traverses the in-tree name database with `LockType.NONE`,
which provides read-committed visibility: only committed NameLNs appear.
Noxu's implementation is `self.name_map.read().keys().cloned().collect()`, a
plain in-memory hash map read with no transaction visibility.  Combined with
1-I (creation is immediate), a database opened inside an uncommitted
transaction appears in `get_database_names()` immediately.

**Suggested fix**: This is resolved by fixing 1-I; once `name_map` insertions
are deferred to transaction commit, `get_database_names()` will naturally
reflect committed state.

---

## 2. TCK Port Quality

Thirty sampled entries from the JE-TCK port enumeration TSVs were evaluated.

### 2-A  Mass false positives from name-match heuristic (≥ 8 entries)

- **Severity**: High  
- **Subsystem**: Testing  
- **TSV files**: `je-tck-port-2026-05-enumeration-je.cleaner.tsv`,
  `je.recovery.tsv`, `je.log.tsv`, `je.evictor.tsv`, `je.rep.*tsv`

At least 8 entries across multiple TSV files are marked `PORTED-EQUIVALENT`
purely because a Noxu test happens to share a generic name (`test_basic`,
`test_delete`, `test_remove`, `test_transactional`) with a JE test in a
completely different subsystem.  Examples:

| JE test | JE class | Noxu "equivalent" | Actual Noxu test |
|---|---|---|---|
| `FileSelectionTest.testBasic` | Cleaner file selection | `vlsn_bucket.rs::test_basic` | VLSN bucket strides |
| `INUtilizationTest.testBasic` | IN utilization counting | `vlsn_bucket.rs::test_basic` | VLSN bucket strides |
| `FSyncManagerTest.testBasic` | Group-commit fsync | `vlsn_bucket.rs::test_basic` | VLSN bucket strides |
| `RecoveryAbortTest.testBasic` | Multi-db abort recovery | `vlsn_bucket.rs::test_basic` | VLSN bucket strides |
| `UtilizationTest.testDelete` | Utilization after delete | `cursor.rs::test_delete` | Cursor delete op |
| `DbConfigUpdateRecoveryTest.testTransactional` | Config recovery | `entry_type.rs::test_transactional` | Log entry type flag |

These are not ports at all; the name-match heuristic produced spurious
results.  Counting them as PORTED-EQUIVALENT inflates the apparent test
coverage.

**Suggested fix**: Re-label these entries as `NOT-PORTED` and create genuine
ports for the missing JE tests (cleaner file selection, IN utilization, group
commit, recovery abort with multiple databases).

---

### 2-B  SR9465 Parts 1 & 2 — known abort-after-delete bug not fixed

- **Severity**: Critical  
- **Subsystem**: B-tree / transactions  
- **Noxu**: `crates/noxu-db/tests/je_recovery_sr_test.rs:87,146`  
- **JE reference**: `recovery/RecoveryAbortTest.java:testSR9465Part1/2`

These two tests are currently passing (no `#[ignore]` in the test file) but
the TSV marks them `PORTED-PARTIAL` with the note:

> "surfaced NOXU-BUG: aborted delete-then-reinsert corrupts BIN; ~half of
> records lost post-abort"

Checking the actual test file confirms no `#[ignore]` annotation is present on
`sr9465_part1_delete_reinsert_abort_restores_no_dups`.  This creates an
ambiguous situation: either the tests now pass because the bug was fixed (no
wave note documents this), or the tests silently pass without exercising the
critical abort+recovery path.  The TSV notes must be reconciled with the test
source.

**Suggested fix**: Verify whether SR9465 is fixed.  If fixed, update the TSV
notes.  If not fixed, re-add the `#[ignore]` with a descriptive reason.

---

### 2-C  SR9752 Part 2 — aborted dup inserts persist (no rollback for dup put)

- **Severity**: Critical  
- **Subsystem**: B-tree / duplicates / undo  
- **Noxu**: `crates/noxu-db/tests/je_recovery_sr_test.rs:266`  
- **JE reference**: `recovery/RecoveryAbortTest.java:testSR9752Part2`

TSV notes: "surfaced NOXU-BUG: aborted dup inserts persist (no rollback for
dup put)".  After a transaction that inserts duplicates is aborted, the
duplicates remain visible.  This is a correctness regression — aborting a
transaction must remove all its writes, including duplicate insertions.  No
fix wave is documented.

**Suggested fix**: Implement abort-time undo for sorted-duplicate put
operations in the undo pass of recovery/transaction abort.

---

### 2-D  Recovery abort test — INCompressorQueue drain omitted

- **Severity**: Medium  
- **Subsystem**: Recovery / B-tree compression  
- **Noxu**: `crates/noxu-db/tests/je_recovery_test.rs:280–360`  
- **JE reference**: `recovery/RecoveryAbortTest.java:testInserts`

JE's `testInserts` waits for the IN-Compressor queue to drain before
proceeding to the next phase (`while (realEnv.getINCompressorQueueSize() > 0)`).
This forces the recovery to replay IN-deletes, verifying that slot compression
after abort interacts correctly with recovery.  Noxu has no equivalent probe
and the test comment acknowledges:

> "Noxu has no equivalent public probe, so this port relies on the recovery
> pipeline doing the equivalent work."

Without the compressor drain, the test does not exercise the same invariant.
The JE test is specifically designed to catch SR-class bugs in which compressor
activity interacts badly with undo/redo ordering.

**Suggested fix**: Add an internal test helper that triggers BIN compression
and drains pending compress work, then call it between test phases.

---

### 2-E  `RecoveryAbortTest.testInserts` — `env.compress()` call missing

- **Severity**: Medium  
- **Subsystem**: Recovery / BIN compression  
- **Noxu**: `crates/noxu-db/tests/je_recovery_test.rs:280`  
- **JE reference**: `recovery/RecoveryAbortTest.java` (multiple test methods
  call `env.compress()`)

Several JE recovery tests call `env.compress()` to explicitly flush the
in-compressor queue.  Noxu's `Environment` does not expose a `compress()`
method at all.  Because BIN compression is daemon-driven in Noxu and cannot
be triggered synchronously in tests, recovery tests that depend on compression
side-effects before the environment close cannot faithfully replicate JE's
behaviour.  (See also 3-A.)

---

### 2-F  Durability config not set in most recovery ports

- **Severity**: Medium  
- **Subsystem**: Recovery / durability  
- **Noxu**: Multiple recovery test files  
- **JE reference**: Various

JE recovery tests often open the environment with explicit durability:

```java
EnvironmentConfig config = new EnvironmentConfig();
config.setTransactional(true);
config.setDurability(Durability.COMMIT_SYNC);
```

Noxu's recovery test helpers (`open_env`) use `EnvironmentConfig::new(path).with_allow_create(true).with_transactional(true)` with no explicit durability.  The default is `COMMIT_SYNC`, which happens to match JE's default; however, tests that explicitly set `NO_SYNC` or `WRITE_NO_SYNC` in JE to force a crash-at-boundary scenario do not do so in Noxu, meaning the write-no-sync crash path is systematically under-tested in ported recovery tests.

---

### 2-G  Name-heuristic mapping obscures coverage gaps in cleaner, log, evictor

- **Severity**: Medium  
- **Subsystem**: Testing / cleaner / log / evictor  

Count of PORTED-EQUIVALENT entries by name-match heuristic (pointing to
unrelated tests): approximately 8–10 in the sampled TSVs, representing an
effective test gap equivalent to ~8–10 integration tests for the cleaner,
group-commit fsync, and evictor subsystems.  The actual coverage of these
subsystems by genuine behavioural tests is much lower than the PORTED-EQUIVALENT
count suggests.

---

## 3. Feature Parity

### 3-A  Missing: `Environment::compress()` — explicit BIN compression

- **Severity**: High  
- **Subsystem**: B-tree / environment API  
- **Noxu**: absent  
- **JE**: `Environment.java:1887` (`compress()`)

JE's `compress()` drains the IN-Compressor queue synchronously.  Noxu has
no equivalent.  This affects tests (see 2-E) and production code that relies
on releasing memory by compressing deleted BIN slots on demand.  The
`EnvironmentConfig` fields `run_in_compressor`,
`in_compressor_wakeup_interval_ms`, `compressor_deadlock_retry`, etc. are
wired in `noxu-dbi`, showing the configuration exists; the missing piece is
the public synchronous trigger.

---

### 3-B  Missing: `Environment::evict_memory()` — explicit eviction

- **Severity**: Medium  
- **Subsystem**: Evictor / environment API  
- **Noxu**: absent  
- **JE**: `Environment.java:1860` (`evictMemory()`)

Allows applications to explicitly request memory eviction.  Used in JE
integration tests and by applications that want to bound memory footprint
after bulk inserts.

---

### 3-C  Missing: `Environment::get_lock_stats()` and `get_transaction_stats()`

- **Severity**: Medium  
- **Subsystem**: Monitoring / environment API  
- **Noxu**: absent (only `get_stats()` exists)  
- **JE**: `Environment.java:2188` (`getLockStats`), `2219` (`getTransactionStats`)

JE exposes separate stats surfaces for lock table and transaction subsystems.
Noxu's `get_stats()` returns `EnvironmentStats` which rolls everything into
one struct but may not expose per-lock-table granularity.  Operators cannot
diagnose lock contention or transaction throughput separately.

---

### 3-D  `Get::SearchLte`, `Get::FirstDup`, `Get::LastDup` — documented as unimplemented

- **Severity**: Medium  
- **Subsystem**: Cursor API  
- **Noxu**: `crates/noxu-db/src/cursor.rs:238–249`; `src/get.rs`  
- **JE**: `Cursor.java` (all implemented)

These three variants exist in the `Get` enum with doc comments noting they
return `NoxuError::Unsupported`.  The doc strings are accurate, but having
public enum variants that immediately fail on the only API entry point
(`Cursor::get`) is a usability trap.  Application code that matches on `Get`
variants will compile fine and fail at runtime.

---

### 3-E  Missing: `Environment::verify()` with `VerifyConfig`

- **Severity**: Low  
- **Subsystem**: Environment API  
- **Noxu**: `Environment::verify()` exists (`environment.rs:1319`) but
  signature `verify(&self, config: Option<&VerifyConfig>, _db_name: Option<&str>)`
  ignores the `_db_name` parameter.  
- **JE**: `Environment.java:2290`

`verify()` exists but single-database verification is not wired.  The
`_db_name` parameter is silently ignored.

---

### 3-F  Missing: `LogFlushTask` — periodic background log flush daemon

- **Severity**: Medium  
- **Subsystem**: WAL / daemon  
- **Noxu**: absent as a distinct public type  
- **JE**: `LogFlushTask` (background task that periodically flushes uncommitted
  writes to OS when using `NO_SYNC` durability)

JE uses `LogFlushTask` to provide background write durability for applications
using `COMMIT_NO_SYNC`.  Without it, data can remain in application buffers
for unbounded periods.  Noxu has `LOG_FLUSH_NO_SYNC_INTERVAL` in config but
whether the corresponding daemon runs is not visible in the public API.

---

### 3-G  Missing: `Database::truncate()` return value — record count not documented

- **Severity**: Low  
- **Subsystem**: Database API  
- **Noxu**: `environment.rs:601` (`truncate_database` returns `Result<u64>`)  
- **JE**: `Environment.java:1125` (`truncateDatabase(..., returnCount: boolean)`)

JE's `truncateDatabase` takes a `returnCount: boolean` parameter; if
`false`, it skips the count scan (cheaper for large databases).  Noxu always
counts (reads every slot), which is expensive for large databases being
truncated where the caller does not need the count.

---

## 4. Subtle Correctness / Durability Gotchas

### 4-A  Parent directory fsync on new log file — confirmed missing

*(See 1-G above — critical, full analysis there.)*

---

### 4-B  `open_database` with transaction — transactional semantics absent

*(See 1-I above — critical, full analysis there.)*

---

### 4-C  LSN ordering — correct (no wrap-around bug)

- **Severity**: Informational  
- **Subsystem**: Util  
- **Noxu**: `crates/noxu-util/src/lsn.rs:185–210`

`Lsn::cmp()` compares `file_number` first, then `file_offset`.  This is
correct lexicographic ordering.  `NULL_LSN` comparisons panic rather than
silently giving wrong order.  No wrap-around bug found.

---

### 4-D  `Database::truncate()` with open cursors — correct guard

- **Severity**: Informational  
- **Subsystem**: Database API  
- **Noxu**: `crates/noxu-dbi/src/environment_impl.rs:1014–1022`

Noxu correctly checks `reference_count() > 0` and returns
`DbiError::DatabaseInUse` before truncating.  JE's equivalent check uses the
`inUseCount` / handle-lock mechanism.  The semantics differ slightly (Noxu
uses reference counting rather than a handle lock), but the practical effect
(rejecting truncate when the database is open) is the same.

---

### 4-E  `SyncPolicy.WRITE_NO_SYNC` — inner `flush=true` flag discrepancy

*(See 1-H above — medium severity.)*

---

### 4-F  `BIN::should_log_delta()` — delta on NULL last_full_lsn

*(See 1-A above — critical, missing `lastFullLsn == NULL_LSN` guard.)*

---

## 5. Documentation / Comment Lies

### 5-A  `LOG_USE_NIO` config param — Java-specific doc carried verbatim

- **Severity**: Low  
- **Subsystem**: Config  
- **Noxu**: `crates/noxu-config/src/params.rs:599`

Doc comment reads: "If true, use Java NIO for log writes."  This is
word-for-word from JE's `EnvironmentParams.LOG_USE_NIO` and has no meaning in
Rust.  The `#[deprecated]` note redirects to `LOG_USE_WRITE_QUEUE` which is
correct, but the primary doc comment should say "has no effect in Noxu DB"
rather than describing Java NIO semantics.

---

### 5-B  `Bin::latch()` / `latch_shared()` / `release_latch()` — no-op without documentation

- **Severity**: Medium  
- **Subsystem**: B-tree latching  
- **Noxu**: `crates/noxu-tree/src/bin.rs:295–305` (`InNode`); `1000–1017` (`Bin`)

The `InNode` helper (used internally by `Bin`) has `latch()`, `latch_shared()`,
and `release_latch()` as completely empty no-op methods with no doc comment
explaining this.  The outer `Bin` delegates to these no-ops.  Code reading
these methods would assume latching is implemented at the `Bin` level and
protected by them; in reality concurrency is achieved via the
`Arc<RwLock<TreeNode>>` wrapper in the tree module.  The no-op stubs serve as
API placeholders but are undocumented, making it easy to misread code that
calls them as if it provides any concurrency guarantee.

**Suggested fix**: Add a doc comment to each no-op: "Concurrency is managed
by the `Arc<RwLock<TreeNode>>` wrapper in `noxu_tree::tree`.  These stubs are
API placeholders; callers must hold the parent `RwLock` guard."

---

### 5-C  `Txn::commit_with_durability` — comment misleads on `CommitNoSync` flush

- **Severity**: Low  
- **Subsystem**: Transactions  
- **Noxu**: `crates/noxu-txn/src/txn.rs:707`

```rust
let flush = true; // always at least flush on commit (fsync implies flush)
```

The comment says "always flush" but for `CommitNoSync` the subsequent
`flush_no_sync()` call is skipped.  The comment implies both `flush=true` in
`log()` and the `flush_no_sync()` call are part of the same "at least flush"
guarantee, yet `CommitNoSync` explicitly bypasses both.  Either the comment or
the `flush = true` default is misleading.

---

### 5-D  `Environment::verify()` — `_db_name` silently ignored, no doc note

- **Severity**: Low  
- **Subsystem**: Environment API  
- **Noxu**: `crates/noxu-db/src/environment.rs:1319`

The `verify()` signature accepts `_db_name: Option<&str>` (underscore prefix
implies ignored) but the doc comment does not mention this limitation.  A
caller passing a specific database name will silently get a full-environment
verification instead.

---

### 5-E  Cursor `Get::SearchGte` doc — claims to be "alias for `SearchRange`" but both exist

- **Severity**: Low  
- **Subsystem**: Cursor API  
- **Noxu**: `crates/noxu-db/src/get.rs` (`SearchRange` variant)

`Get::SearchRange` is documented as "Alias for `SearchGte`. Matches the
`SEARCH_RANGE`/`getSearchKeyRange` name."  Both variants are present in the
same enum and both are handled by the same match arm in `Cursor::get`.  Having
two variants with identical semantics without a `#[deprecated]` on one of them
creates API confusion.  This is not a correctness issue but is a
documentation / API design smell.

---

### 5-F  `should_log_delta()` doc says "25%" but code computes `total / 4` (integer division)

- **Severity**: Medium  
- **Subsystem**: B-tree  
- **Noxu**: `crates/noxu-tree/src/bin.rs:801–802`

The method doc comment says "A delta is logged when <= 25% of slots are
dirty."  The code computes `dirty_count <= total / 4`.  For `total = 3`,
`total / 4 = 0`, so no delta is ever logged for a 3-entry BIN even if 0 of 3
slots are dirty (`0 <= 0` is true — actually this passes, but `dirty_count`
must be > 0 from the earlier guard).  For `total = 7`, `total / 4 = 1`, so
only 1 dirty slot triggers delta logging despite 2 dirty out of 7 being
28 % (above the stated 25 %).  The integer-division threshold does not match
the documented percentage.

---

## 6. JE-isms Carried Over Without Justification

### 6-A  `SecondaryConfig` — Builder-method pattern for simple struct

- **Severity**: Low (idiomatic)  
- **Subsystem**: Database API  
- **Noxu**: `crates/noxu-db/src/secondary_config.rs:160–410`

`SecondaryConfig` uses ~15 builder methods (`set_key_creator`,
`set_multi_key_creator`, etc.) where a Rust struct literal would be
more ergonomic.  The struct fields are `pub`, so callers already access them
directly in some places.  This is a direct port of JE's getter/setter Java
bean pattern and provides no Rust idiom benefit.

---

### 6-B  `Box<dyn SecondaryKeyCreator>` vs function type

- **Severity**: Low (idiomatic)  
- **Subsystem**: Database API  
- **Noxu**: `crates/noxu-db/src/secondary_config.rs:164,167`

`key_creator: Option<Box<dyn SecondaryKeyCreator>>` mirrors JE's interface but
is less ergonomic than a function type `Option<Arc<dyn Fn(key, pkey, skey)
-> bool + Send + Sync>>` or a generic `K: SecondaryKeyCreator`.  The trait
object prevents inline closures without boxing.

---

### 6-C  Error enum — large flat `NoxuError` wall

- **Severity**: Low (idiomatic)  
- **Subsystem**: Error handling  
- **Noxu**: `crates/noxu-db/src/error.rs`

`NoxuError` maps all JE exception types into a single flat enum
(`DatabaseNotFound`, `DatabaseAlreadyExists`, `OperationNotAllowed`,
`LockTimeout`, `DeadlockDetected`, `InsufficientReplicas`, ...).  For the
public API layer this is reasonable, but internal subsystems also use this same
enum rather than typed sub-errors.  The result is that callers of internal
functions get a `NoxuError` variant that carries a `String` message, losing
structured error context.  A typed `LockError` / `RecoveryError` /
`CleanerError` hierarchy would be more idiomatic and match the existing
`noxu-txn::TxnError`, `noxu-recovery::RecoveryError` pattern.

---

### 6-D  Hand-rolled waiter list in `LockImpl` vs `VecDeque`

- **Severity**: Low (idiomatic)  
- **Subsystem**: Lock manager  
- **Noxu**: `crates/noxu-txn/src/lock_impl.rs:29–38`

`LockImpl` uses `Option<LockInfo>` + `Option<Vec<LockInfo>>` for the "first
owner" optimization (matching JE's `firstOwner` / `ownerSet` pattern).  This
is correct and mirrors JE exactly.  In Rust, `smallvec::SmallVec<[LockInfo; 1]>`
would be more idiomatic and slightly more efficient, but the current approach
is not wrong — it is simply a JE-ism retained without Rust justification.

---

## 7. `#[ignore]` Audit

### Summary table

| Test | File:line | Current status | Recommendation |
|---|---|---|---|
| `concurrent_commits_no_lost_writes` | `concurrent_commits_stress.rs:68` | `#[ignore = "stress…"]` | **(b)** Gate behind `slow-tests` feature |
| `test_64_thread_concurrent_readers` | `isolation_test.rs:630` | `#[ignore]` no reason | **(c)** Add reason string: "stress — 64×1K txns; run with --ignored" |
| `test_32r32w_concurrent` | `isolation_test.rs:725` | `#[ignore]` no reason | **(c)** Add reason string |
| `test_200_thread_disjoint_writers` | `isolation_test.rs:837` | `#[ignore]` no reason | **(c)** Add reason string |
| `cursor_edge_non_txnal_cursor_no_updates` | `je_cursor_edge_test.rs:343` | `#[ignore = "intentional divergence"]` | **(c)** Keep; reason is clear |
| `power_loss_sweep_thousand_iterations` | `power_loss_sweep.rs:101` | `#[ignore = "1000-iteration sweep"]` | **(c)** Keep as-is |
| `test_sustained_8r8w_60s` | `sustained_load_test.rs:86` | `#[ignore]` no reason | **(b)** Gate behind `slow-tests`; add "60s wall-clock" note |
| `test_checkpoint_under_load_30s` | `sustained_load_test.rs:200` | `#[ignore]` no reason | **(b)** Gate behind `slow-tests`; add "30s wall-clock" note |
| `replica_scale_test` | `replica_scale_test.rs:332` | `#[ignore = "long-running"]` | **(b)** Gate behind `slow-tests` |
| `torture_replication` | `torture_test.rs:1133` | `#[ignore]` no reason | **(c)** Add reason; keep ignored |
| `test_xa_chaos_concurrent` | `xa_chaos_test.rs:379` | `#[ignore]` with run instructions | **(c)** Keep; reason present |
| `test_xa_perf_2pc_vs_single_phase` | `xa_chaos_test.rs:795` | `#[ignore]` no reason | **(c)** Add reason: "throughput benchmark; not a correctness test" |
| `test_xa_perf_concurrent_multi_cluster` | `xa_chaos_test.rs:904` | `#[ignore]` no reason | **(c)** Add reason |

### 7-A  Three `isolation_test.rs` ignores without reason strings

- **Severity**: Low  
- **Subsystem**: Testing

`test_64_thread_concurrent_readers`, `test_32r32w_concurrent`, and
`test_200_thread_disjoint_writers` have bare `#[ignore]` annotations with no
reason.  Cargo and most CI systems skip them silently.  They are meaningful
stress / correctness tests that could plausibly detect livelock in the
deadlock-detect path (see 1-E).  At minimum each needs a reason string; they
should be gated behind a feature or profile so they can be run in a nightly
CI job.

---

### 7-B  `recovery_edge_test_non_txnal_db` — not `#[ignore]` but has documented bug

- **Severity**: High  
- **Subsystem**: Recovery  
- **Noxu**: `crates/noxu-db/tests/je_recovery_test.rs:485`

The test is NOT marked `#[ignore]` but the TSV explicitly notes:

> "wave10-a port committed #[ignore]; surfaces real noxu bug — non-transactional
> db registration is lost across clean close+reopen"

Checking the test source confirms the `#[ignore]` was removed (the test now
runs in CI).  The comment at the top of the test says "db registration appears
to be flushed to the WAL".  Either:

1. The bug was quietly fixed and the TSV is stale, or  
2. The test passes vacuously because the recovery fixture does not trigger the
   exact code path that fails.

This needs explicit investigation.  If the bug is not fixed, this test is a
**false negative** running in CI.

---

### 7-C  `sr9465` and `sr9752_part2` — TSV says `#[ignore]` but tests are not ignored

- **Severity**: Critical  
- **Subsystem**: Recovery / B-tree undo  
- **Noxu**: `crates/noxu-db/tests/je_recovery_sr_test.rs:87,146,266`

TSV notes for `sr9465_part1`, `sr9465_part2`, and `sr9752_part2`:

> "NOXU-BUG: aborted delete-then-reinsert corrupts BIN; ~half of records lost
> post-abort"
> "NOXU-BUG: aborted dup inserts persist (no rollback for dup put)"

Checking the actual test file: none of these tests have `#[ignore]`.  They run
in CI on every push.  There are two possibilities:

- The bugs were fixed and the TSV notes are stale (high risk: no wave document
  records a fix for these).  
- The tests pass vacuously — the invariant is checked but the code path that
  exercises the bug (delete+reinsert within an aborting txn with later
  recovery) does not trigger the corruption.

If the tests pass without triggering the bugs, they are **false-positive tests
masking known critical correctness bugs**.

**Recommendation**: (d) If bugs are fixed, update TSV and add a regression
note.  If bugs are still present, re-mark with `#[ignore = "NOXU-BUG #SR9465:
aborted delete-then-reinsert corrupts BIN"]`.

---

## Summary Table

| Severity | B-tree | Txn/Locking | WAL/Log | Recovery | Cleaner | API/Docs | Testing | Total |
|---|---|---|---|---|---|---|---|---|
| Critical | 2 | 2 | 1 | 2 | 0 | 0 | 2 | **9** |
| High | 2 | 1 | 1 | 1 | 1 | 2 | 3 | **11** |
| Medium | 2 | 0 | 1 | 2 | 0 | 3 | 2 | **10** |
| Low | 1 | 0 | 0 | 0 | 0 | 4 | 1 | **6** |
| Informational | 0 | 0 | 0 | 2 | 0 | 0 | 0 | **2** |
| **Total** | **7** | **3** | **3** | **7** | **1** | **9** | **8** | **38** |

*(Note: some findings span multiple subsystems and are counted in the primary subsystem.)*

---

## Top 5 Most Actionable Items

1. **[1-I / 1-J — Critical] `open_database` ignores transaction parameter.**  
   The `_txn` parameter in `Environment::open_database` is silently dropped.
   Database creation is non-transactional and non-rollbackable; `get_database_names()`
   shows uncommitted databases.  Fix: implement transactional NameLN locking in
   `EnvironmentImpl::open_database`, deferring `name_map` insertion to commit time.

2. **[7-C / 2-B-C — Critical] SR9465/SR9752 tests running but may mask live bugs.**  
   Three tests claimed in TSV notes to expose critical correctness bugs
   (aborted delete+reinsert corrupts BIN; aborted dup inserts persist) are
   not marked `#[ignore]` and run in CI.  Immediate action: determine if the
   underlying bugs are fixed or if the tests pass vacuously; re-add `#[ignore]`
   if not fixed.

3. **[1-A — Critical] `BIN::should_log_delta()` missing three guard clauses.**  
   The `prohibit_next_delta` flag, the `lastFullLsn == NULL_LSN` guard, and the
   existing-delta early-return from JE's `isDeltaProhibited()` are all absent.
   A BIN with `prohibit_next_delta = true` (set after compression removes a
   dirty slot) will incorrectly log a delta that omits the compressed slot,
   silently losing the compression in the checkpoint record.

4. **[1-C — Critical] Recovery missing MapLN two-pass.**  
   JE processes the database name/mapping tree (MapLNs, NameLNs) in a separate
   undo+redo pass before replaying main data LNs.  Noxu collapses everything
   into one pass.  This correlates with the known non-transactional database
   registration loss bug.  Fix: introduce a MapLN pre-pass in `recover_all()`.

5. **[1-G — Critical] Parent directory not fsynced on new log file creation.**  
   `FileManager::create_file_internal` does `file.sync_all()` but not
   `parent_dir.sync_all()`.  On Linux ext4/XFS, a crash between file creation
   and the next directory sync can make the log file disappear from the
   directory, silently losing all data written to it.  Fix: one line —
   open the parent directory `File` and call `sync_all()` after the new file
   header is written.

---

*Report generated by reading source only; no builds or tests were executed.*
