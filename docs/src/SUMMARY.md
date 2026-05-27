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
  - [Known Limitations](operations/known-limitations.md)

---

# Contributing

- [Contributing](contributing/README.md)
  - [Build and Test](contributing/build-and-test.md)
  - [Porting Guidelines](contributing/porting-guidelines.md)
  - [Testing Guide](contributing/testing-guide.md)
  - [PR Process](contributing/pr-process.md)
  - [Release Process](contributing/release.md)

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
