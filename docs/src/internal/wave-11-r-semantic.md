# Wave 11-R — Semantic Correctness Fixes (v3.0.0)

**Branch**: `fix/wave11-r-semantic`
**Target**: v3.0.0 (breaking changes allowed — pre-3.0 SemVer)
**Audit source**: `audit-2026-05-{je-team,margo,keith,jonhoo}.md`,
`audit-2026-05-synthesis.md`

Wave 11-Q (v2.4.2) closed the non-breaking critical findings.
Wave 11-R closes the breaking ones for v3.0.0.

---

## Items

### C-4 — `open_database` ignores the transaction parameter

**Severity**: Critical
**Files**: `crates/noxu-dbi/src/environment_impl.rs`,
`crates/noxu-db/src/environment.rs`, `crates/noxu-db/src/transaction.rs`
**Cite**: audit-2026-05-je-team.md 1-I / 1-J, audit-2026-05-margo.md 5.9

**What was wrong**: The `txn: Option<&Transaction>` parameter on
`Environment::open_database` was named `_txn` and silently ignored.
Database creation was always immediate and non-rollbackable.
`get_database_names()` returned databases from uncommitted transactions.

**Fix implemented**:

- `EnvironmentImpl` gains `pending_names: RwLock<HashSet<String>>` to
  track names registered in uncommitted transactions.
- `open_database_transactional()` adds the name to `pending_names` (not
  `name_map`) and defers the NameLN WAL write.
- `commit_pending_database()` / `abort_pending_database()` move or remove
  the entry when the transaction resolves.
- `get_database_names()` reads from `name_map` only (committed names).
- `Transaction` gains `abort_callbacks` / `commit_callbacks` vecs.
  `register_abort_callback()` / `register_commit_callback()` are wired by
  `Environment::open_database()` when `txn` is `Some`.
  Callbacks fire from `abort()`, `commit_with_durability()`, and the XA
  resolved-* paths.

**Tests**: `test_transactional_open_database_abort_removes_db` (abort +
reopen), `test_get_database_names_excludes_uncommitted` (concurrent
visibility).

**Breaking**: `_txn` renamed to `txn`; parameter is now functional.
See `docs/src/getting-started/migrating.md` for the before/after recipe.

---

### C-5 — BIN delta logging missing JE guard clauses

**Severity**: Critical
**File**: `crates/noxu-tree/src/bin.rs::should_log_delta`
**Cite**: audit-2026-05-je-team.md 1-A

**What was wrong**: `BIN::should_log_delta()` omitted three predicates
that JE checks in `BIN.shouldLogDelta()`:

1. Already-delta BINs should always re-log as a delta.
2. `prohibit_next_delta` flag (set by `compress()`) must prevent a delta.
3. `last_full_version == NULL_LSN` must prevent a delta (no base to apply
   against).

**Fix implemented**: All three guards added, with detailed comments citing
the JE source lines.  Existing `test_should_log_delta` updated to set
`last_full_version` before the dirty-ratio path.  Three new focused unit
tests added: `test_should_log_delta_guard_{already_delta,
prohibit_next_delta, no_full_bin_yet}`.

**Breaking**: Checkpoint output changes in compress-then-checkpoint
scenarios (more full BINs, fewer deltas).  On-disk format unchanged;
recovery is strictly safer.  Potentially backportable to v2.x.

---

### C-6 — Recovery missing dedicated MapLN two-pass

**Severity**: Critical (partial fix)
**File**: `crates/noxu-recovery/src/recovery_manager.rs`
**Cite**: audit-2026-05-je-team.md 1-C

**What was wrong**: JE's `buildTree()` runs a dedicated undo+redo pass
over NameLNs/MapLNs (the mapping tree) BEFORE replaying main data LNs.
Noxu collapsed everything into one pass with no structural separation.

**Fix implemented** (partial):

- `RecoveryManager::mapping_tree_db_names: HashMap<String, u64>` added to
  snapshot the committed catalog after the mapping-tree pass.
- `run_mapping_tree_undo_pass()` introduced as an explicit phase called
  from `recover_all()` AFTER analysis but BEFORE `run_redo_all()`.
  It removes aborted NameLN entries from `recovered_db_names` and
  populates `mapping_tree_db_names`.
- `recover_all()` gains an explicit structural comment labelling the
  two-pass boundary.

**What is NOT yet done (TODOs)**:

- NameLNs currently carry `txn_id = None` (non-transactional WAL format).
  The aborted-name removal loop is structurally correct but a no-op on
  current WAL files.  Full JE parity requires storing `txn_id` in the
  NameLN WAL entry.
- A full MapLN B-tree undo (JE has a dedicated on-disk mapping database;
  Noxu uses a `HashMap`).

**Safety**: All existing recovery tests pass.  The structural change is
additive.

---

### C-8 — SR9465/SR9752 tests investigation

**Severity**: Critical (if confirmed)
**File**: `crates/noxu-db/tests/je_recovery_sr_test.rs`
**Cite**: audit-2026-05-je-team.md 2-B / 2-C / 7-C

**Outcome**: Category (3) — bugs are **already fixed**. All four tests
pass genuinely.

| Test | TSV status | Finding | Action |
|---|---|---|---|
| SR9465 Part 1 | PORTED-PARTIAL → **PORTED-EQUIVALENT** | Bug fixed in Wave 5: abort sorts undo by LSN descending | TSV updated |
| SR9465 Part 2 | PORTED-PARTIAL → **PORTED-EQUIVALENT** | Same fix | TSV updated |
| SR9752 Part 1 | PORTED-EQUIVALENT | Always passed | No change |
| SR9752 Part 2 | PORTED-PARTIAL → **PORTED-EQUIVALENT** | Bug fixed in Wave 5: sorted-dup writes registered with lock manager | TSV updated |

---

### Q-3 — Missing JE API surface

**Severity**: Medium each; high collectively
**Cite**: audit-2026-05-je-team.md 3-A, 3-B, 3-E

**Implemented**:

- `EnvironmentImpl::compress_all()` / `Environment::compress()` —
  synchronous INCompressor drain (mirrors `Environment.compress()` JE 1887).
- `EnvironmentImpl::evict_memory()` / `Environment::evict_memory()` —
  explicit evictor trigger (mirrors `Environment.evictMemory()` JE 1860).

**Not implemented in this wave** (see `known-limitations.md`):

- `JoinCursor` over sorted-dup secondaries (3-G / v1.6 Decision 1B —
  large follow-up wave).
- `Environment::get_lock_stats()` / `get_transaction_stats()` (3-C —
  monitoring gap, tracked separately).
- `Get::SearchLte`, `Get::FirstDup`, `Get::LastDup` (3-D — runtime
  unsupported variants, already documented in cursor.rs).
- `LogFlushTask` public type (3-F — daemon exists, public type missing).

---

### Q-4 — Recovery test fidelity

**Severity**: High
**Cite**: audit-2026-05-je-team.md 2-D / 2-E

`recovery_abort_test_inserts_three_phase_no_dups` now calls
`env.compress()` after the abort phase to drain the IN-compressor queue
before the recovery reopen, matching JE's `RecoveryAbortTest.testInserts`
behaviour.  Previously this step was omitted ("no equivalent public probe").

---

## Gate results

- `cargo fmt --all -- --check`: PASS
- `cargo clippy --all-targets -- -D warnings` (19 core crates): PASS
- `RUSTDOCFLAGS=-D warnings cargo doc --no-deps` (19 core crates): PASS
- `cargo test -p noxu-{util,config,latch,log,tree,txn,evictor,cleaner,recovery,dbi,engine,db,bind,collections,persist,xa}`: PASS (0 failures)
- `cargo test -p noxu-recovery -p noxu-db --no-fail-fast`: PASS (recovery correctness maintained)

The `heed` recursion-limit error in `benches/comparison/` is a pre-existing
upstream issue unrelated to this wave.

---

## Breaking changes (v3.0.0 migration notes)

See `docs/src/getting-started/migrating.md` § "v2.x → v3.0" for
before/after recipes for C-4 and C-5.
