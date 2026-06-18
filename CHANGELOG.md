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
