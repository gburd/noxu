# Noxu DB — BDB-JE Team Re-audit (2026-05-30)

**Auditor persona**: Sleepycat / BDB-JE original team  
**Repo state**: `origin/main` @ `8f63f6e` (v3.0.2 + umbrella crate docs)  
**Worktree**: `/tmp/reaudit-je`  
**Prior audits read**: `audit-2026-05-synthesis.md`, `wave-11-q-correctness.md`,
`wave-11-r-semantic.md`, `wave-11-s-ux-cleanup.md`, `wave-11-t-crossfeature.md`,
`wave-11-u-recovery-cluster.md`, `wave-11-x-config-xa-cluster.md`,
`wave-11-y-c6-endtoend.md`  
**Items NOT re-reported (already fixed)**: C-1..C-9, H-1..H-10, X-1..X-15  

---

## Findings

---

### F-1. Multiple config parameters stored but silently ignored — no production effect

**Severity**: High  
**Subsystem**: Config / Core  
**Files**:
- `crates/noxu-db/src/environment_config.rs` (public API)
- `crates/noxu-dbi/src/dbi_config.rs` (transfer struct)

**Description**: Seven config parameters are accepted, validated, and
transferred into `DbiEnvConfig` but are never read in any production code
path. The doc comments claim real behaviour; none of it is implemented:

| Parameter | Documented behaviour | Actually happens |
|---|---|---|
| `env_latch_timeout_ms` | "A timeout causes `EnvironmentFailure`" | `LatchContext::new` is always used (no timeout); field never passed to `noxu-latch` |
| `env_expiration_enabled` | "Enable TTL-based record expiration at the env level" | Never checked in cursor/read path |
| `env_db_eviction` | "Enable per-database node eviction" | Never consulted by evictor |
| `env_fair_latches` | "FIFO-ordered latches — prevents starvation" | `SharedLatch`/`ExclusiveLatch` always constructed with `LatchContext::new` |
| `env_check_leaks` | "Check for lock leaks when databases are closed" | Never consulted at close time |
| `env_forced_yield` | "Force thread yields in critical sections (useful for testing fairness)" | No yield-point reads this flag |
| `env_ttl_clock_tolerance_ms` | "TTL clock tolerance for expiration" | Never read in cursor/expiration logic |

Verification: `grep -rn "\.env_latch_timeout_ms\|\.env_expiration_enabled\|\.env_db_eviction\|\.env_fair_latches\|\.env_check_leaks\|\.env_forced_yield" crates/` finds only assignment sites, config-transfer sites, and test assertions — zero production reads in `environment_impl.rs` or any subsystem. Prior audit X-11 fixed `log_flush_no_sync_interval_ms`; these seven were not picked up at the same time.

**JE reference**: `EnvironmentImpl.java` constructor reads every config field passed to it; `LatchContext` is wired to `ENV_LATCH_TIMEOUT` at latch construction time; `ENV_EXPIRATION_ENABLED` gates the expiry logic in `CursorImpl.java`.

**Suggested action**: Either implement each parameter (preferred for `env_latch_timeout_ms` and `env_expiration_enabled` which are security/correctness relevant), or add a prominent `/// **Not yet implemented — reserved for a future release. Setting this has no effect.**` doc comment on each, and list all seven in `docs/src/operations/known-limitations.md`. Do not leave present-tense "causes EnvironmentFailure" claims in the doc of a no-op setter.

---

### F-2. mTLS Phase 1: `peer_allowlist` config field is a security trap

**Severity**: High  
**Subsystem**: Replication / Security  
**Files**:
- `crates/noxu-rep/src/rep_config.rs:150` (`peer_allowlist: Vec<String>`)
- `crates/noxu-rep/src/auth.rs` (Phase 1 — allowlist matching logic only)
- `docs/src/internal/security-review-2026-05.md:380-390` (NA-1..NA-6 still open)

**Description**: `RepConfig::peer_allowlist` is a public config field with
a `peer_allowlist()` builder method. Its doc comment says "incoming
connections are accepted only if the peer's leaf certificate carries a
Subject Common Name … that matches one of these strings." This is false:
the `PeerAllowlist` type exists only in `auth.rs` with unit tests;
it is **not wired to the dispatcher, the TLS ServerConfig, or any
handshake path**. `grep -rn "PeerAllowlist" crates/noxu-rep/src/` returns
only `auth.rs`. `replicated_environment.rs`, `quic_mux.rs`, `quic_channel.rs`,
and `channel.rs` contain zero references to it.

A user reading the API docs would believe they can secure their cluster
by calling `.peer_allowlist(vec!["node-1.example", "node-2.example"])`.
The config is silently accepted and ignored. Any peer can connect.

The CHANGELOG entry says this is "foundation only — the dispatcher does
not yet enforce mTLS" but that disclaimer is buried. The method-level
doc (`rep_config.rs:325-334`) says "not enforced; the dispatcher does not
yet require mTLS" — but a user reading `peer_allowlist` in rustdoc (which
formats the field-level doc, not the method-level doc) sees the false claim.
`known-limitations.md` says "no authentication" generically but does NOT
flag `peer_allowlist` as a noop.

**JE reference**: There is no direct JE analog; Noxu is introducing a feature JE did not have. The issue is that the half-implemented feature surface looks functional.

**Suggested action (immediate)**: Add `#[deprecated = "peer_allowlist is not yet enforced. Phase 2 dispatcher integration is planned for v3.1. Until then this field is accepted and ignored."]` to the field and setter OR add a one-sentence `/// **Phase 1 only: accepted but not enforced. Phase 2 dispatcher wiring is planned for v3.1.**` to the field doc comment. Add a row to `known-limitations.md` specifically for `peer_allowlist`. **Do not let users ship this thinking they have mTLS.**

---

### F-3. Stale `TODO(bug)` comments describe fixed bugs as currently active

**Severity**: Medium  
**Subsystem**: Tests / Documentation accuracy  
**Files**:
- `crates/noxu-db/tests/je_database_test.rs:601-605, 637-643, 804-810, 921-923`
- `crates/noxu-db/tests/je_truncate_test.rs:124-126`

**Description**: Five tests carry `TODO(bug)` comment blocks that describe
bugs as **currently present** (e.g. "Noxu currently permits this combination,
returning Ok(cursor) instead of Err", "Noxu's truncate_database is not durable
— after a clean close+reopen the previously-truncated records re-appear").
These bugs were all fixed in commits that post-date the wave-11-g comments:

| Test | Bug described | Fixing commit |
|---|---|---|
| `database_txn_cursor_on_non_txn_db_rejected` | Noxu returns Ok on txn cursor / non-txn DB | `90918c5` |
| `database_put_no_overwrite_in_dup_db_txn` | put_no_overwrite uses (key,data) not key-only | `e21effb` |
| `environment_read_only_rejects_db_name_ops` | DB registry lost on read-only reopen | `d9bc4c1` |
| `environment_checkpoint_after_commit_loses_data` | checkpoint loses recent commits | `81c1f42` |
| `truncate_survives_clean_close_reopen` | truncate not durable across reopen | `b947b34` |

Commit `87799d1` normalized the TODO label format but preserved the stale
"currently" language. All five tests currently pass with no `#[ignore]`
(CI reports 0 failures), confirming the bugs are gone. The comments create
false impressions of known live bugs in CI-passing code.

**Suggested action**: Remove the "currently permits / not durable" language
from each comment, or change to "WAS a bug; fixed in <commit>. Retained as
regression guard." Mark the TSV status as `PORTED-EQUIVALENT` for the five
entries still showing `PORTED-PARTIAL` in wave-11-g's reasoning.

---

### F-4. C-6 "Complete" claim vs. residual TODO comments describing unfinished MapLN undo

**Severity**: Medium  
**Subsystem**: Recovery  
**Files**:
- `crates/noxu-recovery/src/recovery_manager.rs:246-259` (`mapping_tree_db_names` doc)
- `crates/noxu-recovery/src/recovery_manager.rs:591-598` (`run_mapping_tree_undo_pass` doc)

**Description**: Wave-11-y marks C-6 as "Complete ✓". The wave-11-y doc
correctly describes what was implemented: writing `NameLNTxn` inside the
creating transaction, and un-ignoring the end-to-end test. However, the
`recovery_manager.rs` source still carries two prominent TODO comments
reading "# TODO (C-6 full implementation)" and "# TODO (C-6 full JE parity)"
that say a **full MapLN B-tree undo pass** is not implemented:

```
/// # TODO (C-6 full JE parity)
/// - Store NameLN txn_id in the WAL entry … (done in wave-11-y)
/// - Implement a full MapLN B-tree undo (requires a dedicated mapping-tree
///   database, tracked as a follow-up wave).
```

The second bullet is genuine: JE stores catalog entries in a separate
on-disk mapping B-tree (`_jeNameTree`). Noxu uses a `HashMap`; there is
no JE-equivalent MapLN undo. The wave-11-y doc claims "Complete" for the
HashMap-level fix but does not address the MapLN gap. Both TODOs also
reference `wave-11-r-semantic.md § C-6` (outdated since wave-11-y exists).

Additionally the doc comment on `mapping_tree_db_names` references
`docs/src/internal/wave-11-r-semantic.md` as the tracking doc, not
wave-11-y; this link is stale.

**JE reference**: `RecoveryManager.java::buildTree()` phases A–D walk the
separate `_jeNameTree` and `MapLN` B-tree; the Rust port replaces this with
a HashMap, losing the structural undo pass for complex scenarios involving
multiple checkpoint boundaries and partial MapLN flushes.

**Suggested action**: Either (a) acknowledge the MapLN B-tree undo gap in
`known-limitations.md` and remove the "C-6 full implementation/parity" TODO
markers from the code (replacing them with a comment about the architectural
difference), or (b) add a new tracking item for the MapLN follow-up. Update
the doc link from wave-11-r to wave-11-y. The wave-11-y "Complete" claim is
misleading until the MapLN gap is either closed or explicitly acknowledged
as out-of-scope.

---

### F-5. 1,526 JE-TCK test methods NOT-PORTED; 105 PARTIAL — high-value gaps in critical subsystems

**Severity**: Medium  
**Subsystem**: Test coverage / JE parity  
**Files**: `docs/src/internal/je-tck-port-2026-05-enumeration-*.tsv`

**Description**: Across all 46 TSV files, 1,526 JE test methods are
NOT-PORTED and 105 are PARTIAL. The largest unported clusters are in the
most critical subsystems:

| TSV | NOT-PORTED | PARTIAL | Total |
|---|---|---|---|
| `je.rep.tsv` (replication) | 178 | 4 | 198 |
| `je.test.tsv` (main JE test suite) | 152 | 1 | 164 |
| `je.cleaner.tsv` | 131 | 17 | 159 |
| `je.dbi.tsv` | 103 | 2 | 139 |
| `je.log.tsv` | 79 | 0 | 95 |
| `je.txn.tsv` | 40 | 21 | 75 |
| `je.tree.tsv` | 53 | 0 | 74 |
| `persist.test.tsv` | 85 | 4 | 98 |

Specific high-value missing tests (from `je.test.tsv`):

- **`DeferredWriteTest`** (14 NOT-PORTED): JE deferred-write mode tests;
  `deferred_write` is a supported `DatabaseConfig` field in Noxu but the JE
  regression tests for crash/eviction/cleaning in deferred-write mode are
  entirely absent.
- **`JoinTest`** (2): `testJoin` and `testWriteDuringJoin` — `JoinCursor`
  exists but the core `testJoin` test is not ported (the only join test is
  `#[ignore]`'d due to the sorted-dup gap).
- **`ForeignKeyTest`** (4): cascade delete / nullify / illegal-nullifier tests.
- **`je.txn.tsv`**: `CursorTxnTest::testNullTxnLockRelease` and
  `DeadlockTest::testDeadlockBetweenTwoLockers` are both listed as
  `priority: critical` / NOT-PORTED.

The `je.txn.tsv` PARTIAL cluster (21) represents class-level-only coverage
of `LockManagerTest` — Noxu has lock manager tests but lacks method-level
twins for `testMultipleReadersSingleWrite`, `testNonBlockingLock`,
`testWaitingLock`, `testLockConflictInfo`, and `testImportunateTxn`.

**Suggested action**: Triage the `je.txn.tsv` / `je.test.tsv` NOT-PORTED
tests by the `priority: critical` label in the TSV. Port `DeadlockTest::testDeadlockBetweenTwoLockers`, `CursorTxnTest::testNullTxnLockRelease`, and the `LockManagerTest` method-level twins in a follow-up wave. The `DeferredWriteTest` gap is lower priority given deferred-write is a minor mode, but `testCloseOpenNoSync` (crash without checkpoint leaves data behind) is a valuable regression scenario.

---

### F-6. `Environment::set_replica_coordinator` takes `noxu_dbi::SharedReplicaAckCoordinator` — type not re-exported by umbrella

**Severity**: Medium  
**Subsystem**: Umbrella crate / Public API surface  
**Files**:
- `crates/noxu-db/src/environment.rs:912-915` (public `set_replica_coordinator`)
- `crates/noxu-dbi/src/lib.rs:71-72` (`SharedReplicaAckCoordinator` exported from noxu-dbi)
- `crates/noxu-db/src/lib.rs` (does NOT re-export `SharedReplicaAckCoordinator` or `ReplicaAckCoordinator`)
- `crates/noxu/src/lib.rs` (umbrella — does NOT expose these)

**Description**: `Environment::set_replica_coordinator` is a `pub` method whose
parameter type is `noxu_dbi::SharedReplicaAckCoordinator`
(= `Arc<dyn ReplicaAckCoordinator>`). Neither `noxu-db` nor the `noxu` umbrella
re-exports this type alias or the `ReplicaAckCoordinator` trait. A user who adds
only `noxu = "3"` to their `Cargo.toml` and wants to call
`env.set_replica_coordinator(coord)` cannot name the parameter type without
also adding `noxu-dbi` as a direct dependency.

In practice the only current implementation is `noxu_rep::ReplicatedEnvironment`,
so users of the replication feature depend on `noxu-rep` which in turn depends
on `noxu-dbi` — the type is transitively available. But a user who wants to
implement a custom `ReplicaAckCoordinator` for testing cannot do so without
depending on an internal crate.

**JE reference**: JE's `ReplicaAckPolicy` is a clean public enum; this is
a Rust layering issue specific to the Noxu crate structure.

**Suggested action**: Add to `crates/noxu-db/src/lib.rs`:
```rust
pub use noxu_dbi::{ReplicaAckCoordinator, SharedReplicaAckCoordinator,
                   AckWaitError, AckWaitErrorKind};
```
Then the `noxu` umbrella inherits them via `pub use noxu_db::*`. This is a
small non-breaking addition; `SharedReplicaAckCoordinator` is already part
of the public API by virtue of being the parameter type of a `pub fn`.

---

### F-7. `AnalysisResult::record_active_txn` bug comment says "Bug" but the bug is fixed — confusing prop test

**Severity**: Low  
**Subsystem**: Recovery / Tests  
**Files**:
- `crates/noxu-recovery/tests/prop_tests.rs:352-395`
- `crates/noxu-recovery/src/analysis_result.rs:282-299` (defensive guard)

**Description**: The prop test `prop_active_txn_after_terminal_resurrects_phantom_active`
is prefaced with a block comment beginning "Bug observation surfaced by Wave 11-E"
and describing a phantom-active-txn bug where calling `record_active_txn` after
`record_commit` would leave the txn in `active_txn_ids`.

The **bug is fixed**: `record_active_txn` (line 295-297 of `analysis_result.rs`)
carries an explicit defensive guard:
```rust
if self.committed_txns.contains_key(&txn_id)
    || self.aborted_txns.contains(&txn_id)
{
    return;
}
```
The prop test is therefore a passing **regression guard**, not a
live-bug documentation. The "Bug observation" framing leads readers to believe
the bug is still open. The test has no `#[ignore]` and passes in CI.

The todo comment at line 376 ("TODO: decide whether `record_active_txn` should be
hardened…") is also stale — the hardening was applied.

**Suggested action**: Retitle the test comment from "Bug observation" to
"Regression test: was Wave 11-E bug, fixed by defensive guard in
`record_active_txn`." Remove the open-question TODO — the decision has been made.

---

### F-8. `verify_environment` / `verify_database` return empty pass result — stubs in production code

**Severity**: Low (documented in `known-limitations.md`)  
**Subsystem**: Engine / Verify  
**Files**:
- `crates/noxu-engine/src/verify.rs:453-467` (`verify_environment`)
- `crates/noxu-engine/src/verify.rs:487-499` (`verify_database`)

**Description**: `verify_environment` and `verify_database` return
`VerifyResult { errors: vec![], warnings: vec![], passed: true, … }` without
performing any verification work. `VerifyConfig` has four fields
(`verify_btree`, `verify_log`, `verify_data_checksums`, `repair`) that
control nothing. A production operator who calls `env.verify_environment(config)?`
and checks `result.passed` will always get `true` regardless of data integrity.

`known-limitations.md` documents this ("`verify_environment` / `verify_database`
are stubs"). The `Engine::close` stub (also documented) similarly skips the
`EnvironmentImpl::close` step. The `ReplicatedEnvironment::new` stub, `become_master`,
`transfer_master`, and `shutdown_group` stubs are also documented.

The `VerifyConfig` parameter's inner boolean fields (`verify_btree` etc.) suggest
granular verification is on offer — an operator reading only the rustdoc would
not know the whole thing is a no-op.

**Suggested action**: Add `#[doc = "**Stub** — returns a passing `VerifyResult` without performing any verification. See `docs/src/operations/known-limitations.md`."]` to the function signatures, or at minimum add an inline `log::warn!("verify_environment called but verification is not yet implemented")` so operators see the stub in logs. The existing `known-limitations.md` coverage is necessary but not sufficient for production safety.

---

### F-9. Wave-11-J fsync coalescing deferred with no follow-up wave scheduled

**Severity**: Low  
**Subsystem**: WAL / Performance  
**Files**:
- `docs/src/internal/wave-11-j-fsync-coalescing.md` ("full rewrite deferred pending allocator investigation")
- `crates/noxu-log/src/fsync_manager.rs` (still has thundering-herd wakeup)

**Description**: Wave-11-J investigated replacing `FsyncManager`'s group-condvar
wakeup with a Treiber-stack per-waiter queue. The rewrite was implemented,
tested correct, but showed consistent 10-46% regressions across all W10
workloads and was reverted. The deliverable was a property test and a
diagnosis document. The underlying thundering-herd pattern remains.

No follow-up wave is listed in `post-v2.3.0-roadmap.md` for this item.
The `FsyncManager` benchmarks showed it as the binding constraint on concurrent
write workloads. The "pending allocator investigation" referenced in wave-11-j
has not visibly proceeded.

**Suggested action**: Either add a roadmap entry for wave-11-J2 (or a
performance-only wave) to resolve the allocator/scheduler diagnosis, or document
in `docs/src/operations/sizing.md` that concurrent write throughput is bounded
by the FsyncManager condvar wakeup and recommend the group-commit config
(`with_log_group_commit`) as the primary mitigation.

---

### F-10. JE-TCK wave-9-C / wave-10-A / wave-11-G test quality: faithful overall, minor gaps

**Severity**: Low (test quality audit)  
**Subsystem**: Tests  
**Files**: Multiple `crates/noxu-db/tests/je_*_test.rs`

**Description**: Spot-checking 15 PORTED-EQUIVALENT tests from later waves:

| Test | Faithful? | Gap |
|---|---|---|
| `recovery_abort_test_inserts_three_phase_no_dups` | ✓ Faithful | Calls `env.compress()` after abort per Q-4 fix |
| `sr9465_part1_delete_reinsert_abort_restores_no_dups` | ✓ Faithful | Correct abort-then-recover sequence |
| `sr9752_part2_abort_after_committed_dups_reverts_with_dups` | ✓ Faithful | Checks both pre- and post-recovery |
| `cursor_edge_no_wait_latch_release` | ✓ Faithful | Matches JE `LatchSupport.nBtreeLatchesHeld==0` spirit |
| `dup_cursor_abort_after_dup_creation_keeps_committed_only` | ✓ Faithful | in-txn abort verified by cursor walk |
| `dup_cursor_delete_first_dup_via_positioned_cursor` | ✓ Faithful | Walks full dup chain after delete |
| `search_key_range_with_dup_tree_finds_next_key` | ✓ Faithful | Correct `SearchKeyRange` semantics |
| `environment_checkpoint_forces_durability` | ✓ Faithful | Reopen verification present |
| `test_c6_aborted_db_creation_not_recovered` | ✓ Faithful | In-memory log scanner, correct undo predicate |
| `recovery_basic_insert_delete_modify_round_trip` | ✓ Faithful | Pre- and post-recovery key walks |
| `je_atomic_put_no_overwrite_with_duplicates_concurrent` | ✓ Faithful | 2-thread concurrent; dup invariant checked |
| `cursor_edge_read_deleted_uncommitted` | ✓ Faithful | READ_UNCOMMITTED vs READ_COMMITTED divergence tested |
| `recovery_edge_test_no_log_files` | ✓ Faithful | Empty env recovery path |
| `dup_cursor_put_no_dup_data_inserts_unique_pairs` | ✓ Faithful | 9/10 unique pairs verified |
| `db_cursor_duplicate_test_duplicate_count` | ✓ Faithful | Wave-11-N count fix verified |

The sampled tests are generally faithful. The one structural gap is that
~10 recovery tests in `je_recovery_test.rs` use clean `drop(env)` (which
triggers Noxu's shutdown checkpoint) instead of JE's explicit abrupt-close
pattern. This was acknowledged in `wave-11-g` and partially addressed by
Q-4 adding `env.compress()` before reopen in one test. The remaining tests
that test recovery from "no final checkpoint" cannot be fully ported without
a Noxu API to suppress the shutdown checkpoint.

**Suggested action**: None urgently needed. Document in the TSV notes that
clean-drop-based recovery tests are equivalent in Noxu because the shutdown
checkpoint is always forced. Add a `/// # Noxu adaptation` comment to any
test where the recovery setup differs from JE to explain the reasoning.

---

## Summary Table

| # | Severity | Subsystem | Title |
|---|---|---|---|
| F-1 | **High** | Config | 7 config params stored but never consumed (env_latch_timeout_ms, env_expiration_enabled, env_db_eviction, env_fair_latches, env_check_leaks, env_forced_yield, env_ttl_clock_tolerance_ms) |
| F-2 | **High** | Replication/Security | mTLS `peer_allowlist` looks enforced but is a no-op — security trap |
| F-3 | Medium | Tests | 5 stale `TODO(bug)` comments describe bugs as active that were fixed in waves 9-11 |
| F-4 | Medium | Recovery | C-6 "Complete" claim vs. residual TODO comments on MapLN B-tree undo gap |
| F-5 | Medium | Test coverage | 1,526 JE-TCK test methods NOT-PORTED; 105 PARTIAL; critical gaps in je.txn, je.cleaner |
| F-6 | Medium | Umbrella API | `set_replica_coordinator` exposes `noxu_dbi::SharedReplicaAckCoordinator` not re-exported by umbrella |
| F-7 | Low | Recovery/Tests | `prop_active_txn` "Bug observation" comment is stale; bug was fixed by defensive guard |
| F-8 | Low | Engine | `verify_environment`/`verify_database` stubs (documented but rustdoc misleads) |
| F-9 | Low | WAL/Perf | Wave-11-J fsync coalescing deferred with no scheduled follow-up wave |
| F-10 | Low | Tests | Clean-drop vs. abrupt-close in recovery tests (structural; partially addressed) |

**Counts by severity**:
- Critical: 0
- High: 2 (F-1, F-2)
- Medium: 4 (F-3, F-4, F-5, F-6)
- Low: 4 (F-7, F-8, F-9, F-10)

---

## Top 8 Genuinely-Lingering Items

Ranked by combination of impact and ease of fix:

1. **F-2 (High)** — `peer_allowlist` is a security trap. A one-line deprecation or doc correction prevents a production operator from shipping a cluster they believe is authenticated. Fix time: 30 minutes.

2. **F-1 (High)** — Seven config params are no-ops with misleading doc comments. `env_latch_timeout_ms` is the worst: its doc claims `EnvironmentFailure` on timeout, which is the kind of safety guarantee operators rely on. Fix: add "not yet implemented" to each doc or wire them. Medium effort.

3. **F-3 (Medium)** — Five test files have "Noxu currently permits…" / "not durable" language that is false. This creates a cargo-cult problem: future contributors reading the TODO believe they are dealing with known live bugs in CI-passing code. Fix time: 1 hour to clean up all five.

4. **F-6 (Medium)** — `SharedReplicaAckCoordinator`/`ReplicaAckCoordinator` are not re-exported by `noxu-db` or the umbrella. Any user who wants to implement a custom coordinator hits a compile error pointing to an internal crate. Four-line fix in `noxu-db/src/lib.rs`.

5. **F-4 (Medium)** — C-6 "Complete" vs. residual MapLN B-tree undo TODOs. The discrepancy creates confusion about whether C-6 is actually done. Either delete the TODOs and add a known-limitations entry, or create a new tracking item. Fix time: 20 minutes documentation + decision.

6. **F-5 (Medium)** — The `je.txn.tsv` gap is the highest-risk: `DeadlockTest::testDeadlockBetweenTwoLockers` (priority: critical, NOT-PORTED) tests a scenario where two lockers deadlock. Noxu's H-2 fix (wave-11-Q) addressed the internal lock-manager deadlock; but there is no ported test for the user-visible two-transaction deadlock scenario the JE SR was about. Porting this single test is medium effort and high value.

7. **F-8 (Low)** — `verify_environment` stubs. The fix to add a `log::warn!` call costs 5 minutes and prevents silent "all clear" reports from unimplemented code in production.

8. **F-9 (Low)** — The FsyncManager thundering-herd is the primary throughput ceiling on concurrent write workloads. Scheduling a follow-up investigation (even as a note in the roadmap) keeps the issue visible.

---

## Items Cross-Referenced Against Prior Audit IDs

| Finding | Relationship to Prior Audit |
|---|---|
| F-1 (`env_latch_timeout_ms` etc.) | New — wave-11-X fixed X-11 (`log_flush_no_sync_interval_ms`) but did not sweep other config no-ops |
| F-2 (mTLS peer_allowlist) | New — introduced by `chore/auth-mtls-by-default` branch after the audits |
| F-3 (stale TODO(bug) comments) | New — bugs were fixed in wave-11-G follow-up commits; the stale comments postdate all prior audits |
| F-4 (C-6 MapLN TODO) | [prior: C-6] partial — the NameLNTxn end-to-end was fixed in wave-11-Y; the MapLN structural gap is the residual |
| F-5 (JE-TCK NOT-PORTED) | [prior: Q-3, Q-4] partial — acknowledged gap; wave-11-G addressed ~50 tests; 1,526 remain |
| F-6 (SharedReplicaAckCoordinator) | New — introduced with the API audit (`api-audit-2026-05-rep.md`); the fix (`set_replica_coordinator`) was added but the type re-export was not |
| F-7 (prop_active_txn comment) | New — bug was present in wave-11-E; defensive guard added later without updating comment |
| F-8 (verify stubs) | [prior: known-limitations.md] documented but action not taken |
| F-9 (fsync coalescing) | New — wave-11-J explicitly deferred; no subsequent wave |
| F-10 (clean-drop recovery) | [prior: Q-4] partially addressed in wave-11-R |

---

*Report generated: 2026-05-30. Read-only audit of `origin/main` @ `8f63f6e`.*  
*Worktree used: `/tmp/reaudit-je` (git worktree of `origin/main`).*  
*Prior findings C-1..C-9, H-1..H-10, X-1..X-15 confirmed fixed and not re-reported.*
