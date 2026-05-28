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

### Known issues (v2.4.0 prep)

- `noxu-rep::phi_detector_test::test_master_tracker_phi_mode` is `#[ignore]`'d
  with a fresh TODO. Wave 9-A's de-flake reduced the miss rate but a ~20 %
  failure remains under workspace test load on dev machines (the first
  assertion `master must be alive right after heartbeats` trips when
  scheduler delay between the last `record_heartbeat()` and the
  `is_master_alive()` call pushes phi briefly above the 1.0 threshold). The
  proper fix is deterministic phi-clock injection or restructuring the
  test; tracked for a follow-up wave.

## [v2.3.2] — 2026-05-28

### Fixed (v2.3.2)

- **`AnalysisResult::record_active_txn` precondition gap** (`noxu-recovery`).
  Calling `record_active_txn` after `record_commit` / `record_abort` for the
  same txn id re-inserted the txn into `active_txn_ids`, causing
  `has_active_txns()` to return a phantom `true`.  Added an early-return guard.
  ([Wave 11-E regression](docs/src/internal/wave-11-e-property-tests.md))

- **Transactional cursor on non-transactional database now rejected**
  (`noxu-db`).  `Database::open_cursor(Some(&txn), None)` now returns
  `IllegalArgument` when the database is non-transactional, matching JE.
  ([Wave 11-G regression](docs/src/internal/wave-11-g-je-tck-longtail.md))

- **`put_no_overwrite` on sorted-dup DB now checks key only** (`noxu-dbi`).
  `CursorImpl::put_dup` was checking the `(key, data)` pair for both
  `NoDupData` and `NoOverwrite`; per JE semantics `NoOverwrite` must check
  the key only.
  ([Wave 11-G regression](docs/src/internal/wave-11-g-je-tck-longtail.md))

- **Database name registry now persisted across clean close+reopen**
  (`noxu-dbi`, `noxu-recovery`).  Writes a `NameLN` WAL entry on database
  creation; recovery re-populates `name_map` from these entries.  Read-only
  reopens and non-transactional databases both survive the cycle.
  ([Wave 11-G and Wave 10-A regression](docs/src/internal/wave-11-g-je-tck-longtail.md))

- **Explicit checkpoint no longer loses committed data** (`noxu-recovery`).
  `Checkpointer::do_checkpoint()` was writing `NULL_LSN` as `first_active_lsn`
  in `CkptEnd`, causing recovery to skip committed LN entries before the
  checkpoint start.  Fixed by writing `Lsn::new(0, 0)` and always replaying
  committed LNs in `eligible_for_redo`.
  ([Wave 11-G regression](docs/src/internal/wave-11-g-je-tck-longtail.md))

- **`truncate_database` is now durable across clean close+reopen**
  (`noxu-dbi`).  Before replacing the in-memory tree, write non-transactional
  `DeleteLN` entries for every key; recovery replays them after the original
  inserts, leaving an empty tree.
  ([Wave 11-G regression](docs/src/internal/wave-11-g-je-tck-longtail.md))

### Added (v2.4.0 — Wave 11-D)

- **First-class in-memory replication transport.** Wave 11-D promotes
  the in-memory transport from a `cfg(test)` / `feature = "test-harness"`
  test fixture into a production transport alongside TCP, TLS, and QUIC.
  See [`docs/src/replication/in-memory-transport.md`](docs/src/replication/in-memory-transport.md)
  and the wave note at
  [`docs/src/internal/wave-11-d-inmem-transport.md`](docs/src/internal/wave-11-d-inmem-transport.md).
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

See `docs/src/internal/wave-11-n-sorted-dup-cursor-bugs.md` for the
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

* `docs/src/internal/wave-11-v231-followups.md`: narrative summary
  of Waves 11-A / 11-B / 11-C, including the four sorted-dup cursor
  bugs surfaced (all closed in Wave 11-N — see `### Fixed` above).
* `docs/src/internal/wave-11-n-sorted-dup-cursor-bugs.md`: per-bug
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
  See [`docs/src/internal/wave-11-e-property-tests.md`](docs/src/internal/wave-11-e-property-tests.md).
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
  [`docs/src/internal/wave-11-g-je-tck-longtail.md`](docs/src/internal/wave-11-g-je-tck-longtail.md).

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
  See `docs/src/internal/wave-11-h-perf-investigation.md` for the
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
  - See `docs/src/internal/wave-11-i-cursor-double-descent.md`.

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
  `api-audit-2026-05-rep.md` §7 are closed:
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
- **v1.4.1** (2026-05-25) — Closed 26 of 43 audit items from
  `claim-audit-2026-05` and `security-review-2026-05`: all 16
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

- [`api-audit-2026-05-rep.md`](docs/src/internal/api-audit-2026-05-rep.md) —
  noxu-rep audit, 40 findings.
- [`audit-report.md`](docs/src/internal/audit-report.md) — aggregate.
- [`claim-audit-2026-05.md`](docs/src/internal/claim-audit-2026-05.md) —
  doc-vs-code claim audit (43 items, drove v1.4.1).
- [`je-port-audit-2026-05-overview.md`](docs/src/internal/je-port-audit-2026-05-overview.md)
  — JE port-completeness audit overview (links to api-map / test-map /
  test-quality-spotcheck).

### Decisions

- [`v1.5-decisions-2026-05.md`](docs/src/internal/v1.5-decisions-2026-05.md) —
  architectural decisions (1B / 2C / 3B) signed off by the project
  owner; enforced via Sprint 3D.
- [`sprint-3-decisions-enforced.md`](docs/src/internal/sprint-3-decisions-enforced.md)
  — typed `Unsupported` errors for restricted surfaces.

### Wave reports

Each sprint and wave landed an internal note documenting motivation,
scope, and test gate.  In commit order:

- [Wave 1C — audit Low/Info cleanup](docs/src/internal/wave1c-audit-low-info-cleanup-2026-05.md)
- [Wave 2A — secondary database unification](docs/src/internal/wave-2a-secondary-unification.md)
- [Wave 2B — collections typed API and txn threading](docs/src/internal/wave-2b-collections-typed.md)
- [Wave 2C-1 — DPL derive macros](docs/src/internal/wave-2c-1-derive-macro.md)
- [Wave 2C-2 — DPL schema evolution](docs/src/internal/wave-2c-2-dpl-evolution.md)
- [Wave 2C-3 — DiskOrderedCursor](docs/src/internal/wave-2c-3-disk-ordered-cursor.md)
- [Wave 3-1 — nested-transaction parameter removed](docs/src/internal/wave-3-1-nested-txn-removal.md)
- [Wave 3-2 — crash-durable XA](docs/src/internal/wave-3-2-crash-durable-xa.md)
- [Wave 4-A — noxu-rep GA finish](docs/src/internal/wave-4-a-rep-ga-finish.md)
- [Wave 4-B — JE TCK port (priority 1)](docs/src/internal/wave-4-b-je-tck-port-priority1.md)
- [Wave 4-C — JE TCK port (priority 2)](docs/src/internal/wave-4-c-je-tck-port-priority2.md)
- [Wave 5 — Noxu correctness fixes (TCK regressions)](docs/src/internal/wave-5-noxu-correctness-fixes.md)
- [Wave 6 — JE TCK port (priority 3 + 4)](docs/src/internal/wave-6-je-tck-port-priority-3-4.md)
- [Wave 7 — v2.0.1 polish](docs/src/internal/wave-7-polish.md)
- [Wave 8 — RepTestBase harness + heavy rep TCK port](docs/src/internal/wave-8-rep-testbase.md)
- [Wave 9-A — noxu-rep fixes (v2.1.1 / v2.2.0)](docs/src/internal/wave-9-a-rep-fixes.md)
- [Wave 9-B — Stateright spec re-validation](docs/src/internal/wave-9-b-stateright-revalidation.md)
- [Wave 9-C — JE TCK port (additional rows)](docs/src/internal/wave-9-c-je-tck-ports.md)

### How this file is maintained

See [`docs/src/internal/wave-10-b-changelog.md`](docs/src/internal/wave-10-b-changelog.md)
for the format convention, the relationship to git tag annotations,
and the workflow for updating this file on each future release.
