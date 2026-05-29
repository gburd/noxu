# Summary

[Introduction](introduction.md)
[Acknowledgements](acknowledgements.md)

---

# User Guide

- [Getting Started](getting-started/README.md)
  - [Installation](getting-started/installation.md)
  - [Environments](getting-started/environments.md)
  - [Databases](getting-started/databases.md)
  - [Records](getting-started/records.md)
  - [Reading and Writing](getting-started/reading-writing.md)
  - [Cursors](getting-started/cursors.md)
  - [Disk-Ordered Cursors](getting-started/disk-ordered-cursors.md)
  - [Secondary Databases](getting-started/secondary-databases.md)
  - [Serialization Bindings](getting-started/bindings.md)
  - [Migrating from v1.4.x](getting-started/migrating.md)

- [Transaction Processing](transactions/README.md)
  - [Transaction Basics](transactions/basics.md)
  - [Transaction Configuration](transactions/transaction-config.md)
  - [Cursors and Transactions](transactions/cursors.md)
  - [Secondary Indexes with Transactions](transactions/secondary-with-txn.md)
  - [Concurrency](transactions/concurrency.md)
  - [Isolation Levels](transactions/isolation.md)
  - [Deadlock Handling](transactions/deadlocks.md)
  - [Durability Policies](transactions/durability.md)
  - [Backup and Recovery](transactions/backup-recovery.md)
  - [XA Distributed Transactions](transactions/xa-distributed.md)

- [High Availability](replication/README.md)
  - [Concepts](replication/concepts.md)
  - [Setup and Configuration](replication/setup.md)
  - [Leader Elections](replication/elections.md)
  - [Consistency Policies](replication/consistency.md)
  - [Durability Policies](replication/durability.md)
  - [Network Restore](replication/network-restore.md)
  - [Master Transfer](replication/master-transfer.md)
  - [Dynamic Membership](replication/dynamic-membership.md)
  - [Transport Layer](replication/transport.md)
  - [In-Memory Transport](replication/in-memory-transport.md)

- [Collections and Persistence](collections/README.md)
  - [StoredMap](collections/stored-map.md)
  - [StoredSet](collections/stored-set.md)
  - [StoredList](collections/stored-list.md)
  - [Entity Persistence (DPL)](collections/entity-persistence.md)

---

# Programmer's Reference

- [Reference Overview](reference/README.md)
  - [Architecture](reference/architecture.md)
  - [Write-Ahead Log Format](reference/log-format.md)
  - [B-tree Internals](reference/btree.md)
  - [Concurrency Model](reference/concurrency-model.md)
  - [Recovery Protocol](reference/recovery.md)
  - [Cache Eviction](reference/cache-eviction.md)
  - [Log Cleaning](reference/log-cleaning.md)
  - [Configuration Reference](reference/configuration.md)
  - [On-Disk Format](reference/on-disk-format.md)

---

# Operations

- [Operations Guide](operations/README.md)
  - [Sizing](operations/sizing.md)
  - [Monitoring](operations/monitoring.md)
  - [Performance Tuning](operations/tuning.md)
  - [Backup](operations/backup.md)
  - [Recovery Procedures](operations/recovery-ops.md)
  - [Operational Runbooks](operations/runbooks.md)
  - [Power-Loss Testing](operations/power-loss.md)
  - [Numerical Baseline](operations/numerical-baseline.md)
  - [Performance Benchmarks](operations/benchmarks.md)
  - [Known Limitations](operations/known-limitations.md)

---

# Contributing

- [Contributing](contributing/README.md)
  - [Build and Test](contributing/build-and-test.md)
  - [Porting Guidelines](contributing/porting-guidelines.md)
  - [Testing Guide](contributing/testing-guide.md)
  - [PR Process](contributing/pr-process.md)
  - [Release Process](contributing/release.md)
  - [API Stability](contributing/api-stability.md)
  - [SemVer Policy](contributing/semver-policy.md)
  - [Publishing to crates.io](contributing/publishing.md)

---

# Maintainer's Guide

- [For Future Maintainers](maintainer/README.md)
  - [Project History](maintainer/project-history.md)
  - [Crate Guide](maintainer/crate-guide.md)
  - [Algorithms](maintainer/algorithms.md)
  - [Design Decisions](maintainer/design-decisions.md)
  - [Reference Source Navigation](maintainer/reference-source-guide.md)
  - [Development Workflow](maintainer/development.md)
  - [Testing](maintainer/testing.md)
  - [Chaos and Soak Testing](maintainer/chaos-soak-testing.md)
  - [Benchmarking](maintainer/benchmarking.md)

---

# Internal Documents

- [Internal Overview](internal/README.md)
  - [Design Review](internal/design-review.md)
  - [Audit Report](internal/audit-report.md)
  - [Rust Code Review](internal/rust-review.md)
  - [Serialization Research](internal/serialization-research.md)
  - [Checksum Selection](internal/checksum-selection.md)
  - [Wave 1C — audit Low/Info cleanup](internal/wave1c-audit-low-info-cleanup-2026-05.md)
  - [Wave 2A — Secondary database unification](internal/wave-2a-secondary-unification.md)
  - [Wave 2B — Collections typed API and txn threading](internal/wave-2b-collections-typed.md)
  - [Wave 2C-1 — DPL derive macros](internal/wave-2c-1-derive-macro.md)
  - [Wave 2C-2 — DPL schema evolution](internal/wave-2c-2-dpl-evolution.md)
  - [Wave 2C-3 — DiskOrderedCursor](internal/wave-2c-3-disk-ordered-cursor.md)
  - [Wave 3-1 — nested-transaction parameter removed](internal/wave-3-1-nested-txn-removal.md)
  - [Wave 3-2 — Crash-durable XA](internal/wave-3-2-crash-durable-xa.md)
  - [Wave 4-A — noxu-rep GA finish](internal/wave-4-a-rep-ga-finish.md)
  - [JE TCK port (2026-05) — Overview](internal/je-tck-port-2026-05-overview.md)
  - [Wave 4-B — JE TCK port (priority 1)](internal/wave-4-b-je-tck-port-priority1.md)
  - [Wave 5 — Noxu correctness fixes (JE TCK regressions)](internal/wave-5-noxu-correctness-fixes.md)
  - [Wave 4-C — JE TCK port (priority 2)](internal/wave-4-c-je-tck-port-priority2.md)
  - [Wave 6 — JE TCK port (priority 3 + 4)](internal/wave-6-je-tck-port-priority-3-4.md)
  - [Wave 7 — v2.0.1 polish](internal/wave-7-polish.md)
  - [Wave 8 — RepTestBase harness + heavy rep TCK port](internal/wave-8-rep-testbase.md)
  - [Wave 9-A — noxu-rep fixes (v2.1.1 / v2.2.0)](internal/wave-9-a-rep-fixes.md)
  - [Wave 9-B — Stateright spec re-validation](internal/wave-9-b-stateright-revalidation.md)
  - [Wave 9-C — JE TCK port (additional rows)](internal/wave-9-c-je-tck-ports.md)
  - [Wave 10-B — `CHANGELOG.md` generation](internal/wave-10-b-changelog.md)
  - [Wave 10-C — README + capability matrix refresh](internal/wave-10-c-readme-matrix.md)
  - [Wave 10-D — Performance benchmarks vs JE](internal/wave-10-d-benchmarks.md)
  - [v1.5 architectural decisions (2026-05)](internal/v1.5-decisions-2026-05.md)
  - [Sprint 1 follow-up — F12 residuals](internal/sprint-1-followup-f12.md)
  - [Sprint 3 — architectural decisions enforced](internal/sprint-3-decisions-enforced.md)
  - [Sprint 3 — collections scope restriction](internal/sprint-3-collections-restriction.md)
  - [Sprint 3 — DPL scope restriction](internal/sprint-3-dpl-restriction.md)
  - [Sprint 3 — XA scope restriction](internal/sprint-3-xa-restriction.md)
  - [API audit (2026-05) — noxu-rep](internal/api-audit-2026-05-rep.md)
  - [JE port audit (2026-05) — Overview](internal/je-port-audit-2026-05-overview.md)
  - [Wave 10-E — crates.io publication prep](internal/wave-10-e-cratesio-prep.md)
  - [Wave 10-F — CI matrix expansion](internal/wave-10-f-ci-matrix.md)
  - [Post-v2.3.0 Roadmap (Wave 11 onward)](internal/post-v2.3.0-roadmap.md)
  - [Wave 11 — v2.3.1 follow-ups](internal/wave-11-v231-followups.md)
  - [Wave 11-N — sorted-dup cursor bug fixes](internal/wave-11-n-sorted-dup-cursor-bugs.md)
  - [Wave 11-F — Stateright spec coverage expansion](internal/wave-11-f-stateright-coverage.md)
  - [Wave 11-E — Property test expansion](internal/wave-11-e-property-tests.md)
  - [Wave 11-G — JE TCK long-tail port](internal/wave-11-g-je-tck-longtail.md)
  - [Wave 11-H — Performance investigation on JE-wins workloads](internal/wave-11-h-perf-investigation.md)
  - [Wave 11 Bug-Fix Wave — v2.3.2](internal/wave-11-bugfix-v232.md)
  - [Wave 11-I — Cursor / BIN scan optimization](internal/wave-11-i-cursor-double-descent.md)
  - [Wave 11-J — fsync coalescing investigation](internal/wave-11-j-fsync-coalescing.md)
  - [Wave 11-K — Recovery / log-scanner allocation reduction](internal/wave-11-k-recovery-alloc.md)
  - [Audit (2026-05) — synthesis](internal/audit-2026-05-synthesis.md)
  - [Audit (2026-05) — JE-team](internal/audit-2026-05-je-team.md)
  - [Audit (2026-05) — Margo (algorithms + docs)](internal/audit-2026-05-margo.md)
  - [Audit (2026-05) — Keith (perf + correctness)](internal/audit-2026-05-keith.md)
  - [Audit (2026-05) — Jonhoo (Rust idiom + soundness)](internal/audit-2026-05-jonhoo.md)
  - [Wave 11-Q — correctness fixes from 2026-05 audit](internal/wave-11-q-correctness.md)
  - [Wave 11-R — semantic correctness fixes (v3.0.0)](internal/wave-11-r-semantic.md)
  - [Wave 11-S — UX improvements + docs accuracy + cleanup](internal/wave-11-s-ux-cleanup.md)
  - [Wave 11-L — API stability commitment + SemVer policy](internal/wave-11-l-api-stability.md)
  - [Wave 11-V — Voice cleanup (agent-cruft + provenance)](internal/wave-11-v-cleanup.md)
  - [Audit (2026-05) — 2nd-pass cross-feature](internal/audit-2026-05-2ndpass-crossfeature.md)
  - [Wave 11-T — cross-feature critical correctness fixes](internal/wave-11-t-crossfeature.md)
