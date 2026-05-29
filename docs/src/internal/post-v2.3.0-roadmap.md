# Post-v2.3.0 Roadmap (Wave 11 onward)

This document tracks the work outstanding after the May 2026
audit-driven sprint cycle reached its natural stopping point at
v2.3.0.  Every item has an owner-tag (the wave that's working on
it or that will), a target release, a status, and a concrete
acceptance gate.

The roadmap is structured around the project owner's stated
priorities (post-v2.3.0):

1. **Always**: docs / tests / comments / man pages stay in sync
   with source.  CI green on GitHub + Codeberg.
2. **Follow-ups** — small, scoped items that close out gaps the
   existing waves explicitly flagged.
3. **Naturally-bounded extensions** — work with a clear endpoint
   (JE TCK long tail, performance optimizations on the JE-wins
   workloads, crates.io publishing).
4. **Open-ended directions** — in-memory transport, more property
   tests, API stability/SemVer commitment, Stateright spec coverage
   expansion.
5. **Performance** — JE gap-closing and general wins are
   high-priority across the whole roadmap.

## Universal constraints

Every wave ships:

* All affected docs (rustdoc, mdBook chapters, internal notes,
  per-feature man-page-equivalents, README, CHANGELOG) updated to
  match the new code.
* All affected tests updated; new code lands with new tests.
* All affected comments accurate.
* `cargo fmt`, `cargo clippy --workspace --all-targets -- -D warnings`,
  `cargo test --workspace --no-fail-fast`, `cargo doc --workspace
  --no-deps -D warnings`, `make docs-check` all green.
* CI passing on both GitHub Actions and Codeberg's Forgejo
  workflow.

## Tracker

| Wave | Target | Scope | Status |
|---|---|---|---|
| 11-A | v2.3.1 | Wave 10-A continuation (more dup-cursor JE TCK ports) | merged — 6 PORTED + 2 #[ignore] on real noxu bugs (see `wave-11-v231-followups.md`) |
| 11-B | v2.3.1 | Sorted-dup secondary index benchmark workload | merged — W13 in noxu-bench + je-bench, 2 more sorted-dup cursor bugs surfaced |
| 11-C | v2.3.1 | Real-storage (NVMe) benchmark re-run | merged — W10/W11 re-run on /scratch NVMe; FsyncManager coalescing now visible (~6–30×) |
| 11-D | v2.4.0 | In-memory-only transport for noxu-rep | merged — `InMemoryTransport` + `RepTransportKind::InMemory` (`docs/src/internal/wave-11-d-inmem-transport.md`) |
| 11-E | v2.4.0 | Property test expansion (target: +20 properties) | merged: +39 proptest blocks across noxu-tree/bind/cleaner/recovery/rep (+1 #[ignore]'d behavior); see [wave-11-e-property-tests.md](./wave-11-e-property-tests.md) |
| 11-F | v2.4.0 | Stateright coverage of remaining 6 protocols | merged — [all 11 specs stamped VALIDATED-AS-OF v2.0.0/v2.4.0; 5 strengthened](wave-11-f-stateright-coverage.md) |
| 11-G | v2.4.0 | Continue JE TCK long-tail port (~30-50 more tests) | merged: +49 PORTED-EQUIVALENT/PARTIAL ([wave-11-g-je-tck-longtail.md](wave-11-g-je-tck-longtail.md)) |
| 11-H | v2.4.0 | Performance investigation on JE-wins workloads (W03/W04/W10/W11) | merged: per-workload analysis + ROI plan in [wave-11-h-perf-investigation.md](wave-11-h-perf-investigation.md) |
| 11-I | v2.4.0 | Optimize cursor descent / BIN scan (closes W03/W04 gap) | **merged**: W03 +115%, W04 +135%; both now beat JE ([wave-11-i-cursor-double-descent.md](wave-11-i-cursor-double-descent.md)) |
| 11-J | v2.4.0 | Optimize fsync coalescing (closes W10 gap) | **investigation complete**: Treiber-stack rewrite prototyped and reverted (10–46 % regression); property test added; see [wave-11-j-fsync-coalescing.md](wave-11-j-fsync-coalescing.md) |
| 11-K | v2.4.0 | Optimize log scanner (closes W11 gap) | **landed (partial)**: 3 alloc reductions in redo path (Tree::redo_insert + zero-copy LnRecord + BIN capacity hint); ~1 % wall-clock improvement on W11 (env-open dominates, not redo loop); follow-up needed for full gap closure — see [wave-11-k-recovery-alloc.md](wave-11-k-recovery-alloc.md) |
| 11-N | v2.3.1 | Sorted-dup cursor bug fixes (4 bugs Wave 11-A/B surfaced) | merged — see `wave-11-n-sorted-dup-cursor-bugs.md`; the 4 #[ignore]'d / safety-cap regression tests are now passing live tests |
| 11-BF | v2.3.2 | Bug-fix wave: 6 regressions from Wave 11-E/G | **merged** — all 6 `#[ignore]`'d tests fixed and promoted; see [wave-11-bugfix-v232.md](wave-11-bugfix-v232.md): record_active_txn guard, txn-cursor-on-non-txn-db, NoOverwrite dup-DB semantics, db-name registry WAL persistence, checkpoint data-loss, truncate durability |
| 11-L | v3.0.0 | API stability commitment + SemVer policy + deprecation cycle | queued |
| 11-M | v3.0.0 | Path-dep restructuring + actual crates.io publish | queued |

## Acceptance gates per wave

### 11-A — Wave 10-A continuation

Goal: close out the dup-cursor JE TCK test ports the original
Wave 10-A agent didn't finish, plus any other scoped JE TCK
candidates that were one-line-from-done.

Acceptance:

* PORTED-EQUIVALENT count in master TSV grows by ≥5.
* No new `#[ignore]`'d regressions surfaced (or, if any, they're
  routed into a follow-up bug-fix wave).

### 11-B — Sorted-dup secondary index benchmark

Wave 10-D (`docs/src/internal/wave-10-d-benchmarks.md`) flagged
this as gap #1: no benchmark workload exercises the sorted-dup
secondary index path that landed in Wave 2A.

Acceptance:

* New workload W13 (or similar) defined in `benches/noxu-bench/`
  that exercises many-primary-to-one-secondary-key reads.
* Equivalent JE workload in `benches/je-bench/`.
* `benches/results/` and `docs/src/operations/benchmarks.md`
  updated with W13 numbers.

### 11-C — Real-storage benchmark re-run

Wave 10-D ran on tmpfs, where fsync coalescing is meaningless.
Real NVMe numbers may tell a different story for W10
(concurrent) and W11 (recovery).

Acceptance:

* The `run_comparison.sh` harness invoked with
  `NOXU_BENCH_DIR=/path/to/nvme` for both engines.
* New section in `docs/src/operations/benchmarks.md` with the
  NVMe numbers.  Honest about what changes vs the tmpfs run.

### 11-D — In-memory transport for noxu-rep

Wave 8 added a test-harness in-memory transport that's
test-only.  This wave makes the in-memory transport a first-class
production transport so users can compose multi-node clusters
in-process (useful for testing, small deployments, embedded use
cases).

Acceptance:

* `noxu-rep` exposes an `InMemoryTransport` (or similarly named)
  alongside the existing TCP/TLS/QUIC transports.
* The same `Transport` trait is implemented; `RepConfig` accepts
  it as a transport choice.
* New chapter `docs/src/replication/in-memory-transport.md`
  with usage example.
* Tests cover: 3-node group via in-memory transport, replication
  flows, election, network restore.

### 11-E — Property test expansion

Acceptance:

* Net new ≥20 `proptest!` blocks across crates that have light
  property-test coverage today.  Bias toward:
  * `noxu-tree` BIN-delta encoding/decoding properties
  * `noxu-recovery` ARIES-style replay invariants
  * `noxu-cleaner` utilization-tracking properties
  * `noxu-rep` Paxos / VLSN streaming properties
  * `noxu-bind` tuple format reverse properties
* Any property test that surfaces a real bug commits the failing
  test as `#[ignore]` with a TODO; bug fixes routed to a separate
  wave.

### 11-F — Stateright spec coverage

Wave 9-B updated 5 of 11 specs to reflect Wave 4-A persistence
changes.  The remaining 6 protocols (B+tree latching, WAL
group-commit, recovery, lock manager + deadlock, cleaner safety,
cache↔cleaner ordering, XA 2PC, plus polish on existing) need:
either an updated model that reflects v2.x reality, or
documentation that the existing model still holds.

Acceptance:

* Every protocol listed in `crates/noxu-spec` has either an
  explicit "validated unchanged" annotation in its module doc, or
  an updated model with passing tests.
* Counterexamples (if any) commit failing models with TODOs;
  production fixes routed to separate waves.

### 11-G — JE TCK long-tail port

Acceptance:

* PORTED-EQUIVALENT count grows by ≥30 in this wave.
* Master TSV updated.
* Any newly-surfaced Noxu bugs `#[ignore]`'d with TODOs; routed
  to bug-fix waves.

### 11-H — Performance investigation

Acceptance:

* Per-workload (W03, W04, W10, W11) flame-graph or equivalent
  profile data captured.
* Concrete optimization hypotheses listed in
  `docs/src/internal/wave-11-h-perf-investigation.md`.
* Each hypothesis estimates an LOC range and a confidence level
  for closing the gap.
* This wave does NOT actually optimize — it sets up 11-I/J/K.

### 11-I — Cursor descent / BIN scan optimization (W03/W04)

Acceptance:

* W03 and W04 numbers improve to within 1.2× of JE on the
  identical hardware/workload as the v2.3.0 benchmark report.
* No regression on Noxu-wins workloads (W01/W05/W06/W09).
* All correctness tests still pass.

### 11-J — fsync coalescing investigation (W10)

**Status**: investigation complete; full rewrite deferred.

A Treiber-stack + per-waiter condvar replacement for `Mutex<FsyncState>` was
implemented, tested correct (all 5765 workspace tests pass + 1 new property
test), but showed 10–46 % performance regressions due to per-call `Arc`
allocation overhead.  The rewrite was reverted.  Acceptance gate not met.

Deliverable: `test_fsync_before_commit_invariant` added to `noxu-log` — a new
`#[test]` that verifies every committed LSN is fsync’d before `commit()`
returns.  See [wave-11-j-fsync-coalescing.md](wave-11-j-fsync-coalescing.md)
for the full diagnosis and recommended next steps.

### 11-K — Log scanner / recovery allocation reduction (W11)

**Status:** landed on `fix/wave11-k-recovery-alloc`.

Three allocation-reduction changes merged:

* Fix 1: `Tree::redo_insert(&[u8], &[u8], Lsn)` — eliminates one intermediate
  `Vec<u8>` per LN record during redo (previously `rec.key.to_vec()` before
  calling `Tree::insert`).
* Fix 2: consuming iteration in `run_analysis` — moves `LnRecord` into
  `redo_entries` without `Bytes::clone()`, eliminating 200K+ Arc
  refcount bumps at 100K scale.
* Fix 3: `Tree::hint_redo_capacity` + pre-allocated BIN split halves —
  eliminates Vec-resize doublings in the initial BIN and in each BIN
  created by `split_child`.

Measured improvement (tmpfs, this machine):

| Scale | Baseline | After 11-K | JE | Ratio |
|------:|---------:|-----------:|----:|------:|
| 1 K   | ~202ms   | ~202ms     | 28ms | 7.2× |
| 10 K  | ~214ms   | ~214ms     | 45ms | 4.7× |
| 100 K | ~254ms   | ~251ms     | 87ms | 2.9× |

The allocator-path changes are confirmed by the refactoring (fewer
calls to `to_vec()`, no `Bytes::clone()` in the analysis hot loop).
The measured W11 wall-clock improvement at 100K is within the
benchmark noise band (~1%) — see the wave doc for root-cause analysis.

Acceptance gate (1.5× of JE on tmpfs) is NOT yet met.  The wave doc
explains why: the dominant remaining cost is the ~200ms env-open
overhead that is NOT in the recovery path, not the LN redo loop itself.
A follow-up (e.g., lazy env-open optimisation or BIN deserialization
from the dirty_in_map) would be needed to close the gap.

### 11-N — Sorted-dup cursor bug fixes

Merged on `fix/wave11-n-sorted-dup-cursor-bugs` (off
`fix/wave11-v231-followups`).  Closes the four sorted-dup cursor bugs
that Wave 11-A and Wave 11-B surfaced and routed to a follow-up
bug-fix wave.

Acceptance (all met):

* Bug 1 — `Cursor::count()` correct on multi-primary sorted-dup DBs.
* Bug 2 — `Get::Search` + `Get::NextDup` correct on every primary
  (not just the lexicographically smallest).
* Bug 3 — `SecondaryCursor::get_search_key` + `get_next_dup_full`
  no longer raises `SecondaryIntegrityException` past the first
  yield.
* Bug 4 — `SecondaryCursor::get_first` + repeated `get_next` walks
  every (sec_key, primary_key) pair exactly once and terminates.
* Two `#[ignore]`'d Wave 11-A regression tests promoted to live
  tests; two new regression tests added under
  `crates/noxu-db/tests/wave11n_secondary_dup_test.rs`.
* Per-bug analysis in
  `docs/src/internal/wave-11-n-sorted-dup-cursor-bugs.md`.

### 11-L — API stability commitment

Acceptance:

* Public API in `noxu-db`, `noxu-bind`, `noxu-collections`,
  `noxu-persist`, `noxu-xa`, `noxu-rep` is enumerated in
  `docs/src/contributing/api-stability.md`.
* SemVer policy documented: pre-v3.0 has free breaking changes;
  v3.0+ commits to no breaking public-API change in a minor or
  patch release.
* `#[deprecated]` markers added to any pre-3.0 surface that's
  going to disappear.
* CI gate (cargo-semver-checks or similar) added to flag
  breaking changes in PRs.

### 11-M — Crates.io publishing

Acceptance:

* Workspace dep graph restructured: each public crate's
  `noxu-*` dependencies use `version = "..."` (not `path = ...`).
* `cargo publish --dry-run -p <crate>` succeeds for every
  intended-public crate.
* Actual `cargo publish` executes for noxu-util, noxu-sync,
  noxu-latch, noxu-config, noxu-log, noxu-bind, noxu-tree,
  noxu-txn, noxu-evictor, noxu-cleaner, noxu-recovery, noxu-dbi,
  noxu-engine, noxu-collections, noxu-persist, noxu-persist-derive,
  noxu-xa, noxu-rep, noxu-db (in dep order).
* docs.rs builds successfully.
* README and CHANGELOG updated to point at crates.io.

## Cross-cutting commitments

Every wave's deliverables include:

* Updated rustdoc on every modified pub item.
* Updated mdBook chapter if the user-facing surface changed.
* Updated `CHANGELOG.md` (Unreleased section) until the wave is
  released, then moved into the released section.
* Updated `docs/src/introduction.md` capability matrix if a
  capability shipped or shifted.
* CI green on both GitHub and Codeberg before merge.

## How to update this doc

When dispatching a new wave, edit the Tracker table to update its
status (`queued` → `dispatched` → `merged` → `released`).  Add new
rows to the bottom for late additions.

When a wave merges, write a brief one-line outcome in the Status
column linking to the per-wave internal note.
