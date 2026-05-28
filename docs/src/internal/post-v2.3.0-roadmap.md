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
| 11-A | v2.3.1 | Wave 10-A continuation (more dup-cursor JE TCK ports) | dispatched |
| 11-B | v2.3.1 | Sorted-dup secondary index benchmark workload | dispatched |
| 11-C | v2.3.1 | Real-storage (NVMe) benchmark re-run | dispatched |
| 11-D | v2.4.0 | In-memory-only transport for noxu-rep | dispatched |
| 11-E | v2.4.0 | Property test expansion (target: +20 properties) | queued |
| 11-F | v2.4.0 | Stateright coverage of remaining 6 protocols | merged — [all 11 specs stamped VALIDATED-AS-OF v2.0.0/v2.4.0; 5 strengthened](wave-11-f-stateright-coverage.md) |
| 11-G | v2.4.0 | Continue JE TCK long-tail port (~30-50 more tests) | queued |
| 11-H | v2.4.0 | Performance investigation on JE-wins workloads (W03/W04/W10/W11) | queued |
| 11-I | v2.5.0 | Optimize cursor descent / BIN scan (closes W03/W04 gap) | gated on 11-H findings |
| 11-J | v2.5.0 | Optimize fsync coalescing (closes W10 gap) | gated on 11-H findings |
| 11-K | v2.5.0 | Optimize log scanner (closes W11 gap) | gated on 11-H findings |
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

### 11-J — fsync coalescing (W10)

Acceptance:

* W10 closes to within 1.3× of JE on real NVMe (per the 11-C
  numbers).
* No regression on durability tests.

### 11-K — Log scanner optimization (W11)

Acceptance:

* W11 closes to within 1.5× of JE.
* Recovery correctness tests still pass.

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
