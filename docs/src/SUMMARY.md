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
  - [Using Noxu from Async Code](getting-started/async.md)
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
  - [Admin Tooling (dump / load / print-log)](operations/admin-tooling.md)
  - [Operational Runbooks](operations/runbooks.md)
  - [Power-Loss Testing](operations/power-loss.md)
  - [Numerical Baseline](operations/numerical-baseline.md)
  - [Performance Benchmarks](operations/benchmarks.md)
  - ["Where Noxu Leads" Benchmarks](operations/lead-benchmarks.md)
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
  - [Serialization Research](internal/serialization-research.md)
  - [Checksum Selection](internal/checksum-selection.md)
  - [mTLS-by-default design (2026-05)](internal/auth-mtls-design-2026-05.md)
  - [noxu umbrella crate (v3.0.1)](internal/noxu-umbrella.md)
  - [Portability validation — RISC-V 64 + Windows on ARM64](internal/portability-rv-windows.md)
  - [Wave GB — DbTree foundation + P-2 recovery investigation](internal/wave-gb-dbtree-recovery.md)
    - [Deferred-blocker implementation designs](internal/deferred-blocker-designs-2026-06.md)
  - [Write ceiling: the cleaner throttle (2026-07)](internal/write-ceiling-cleaner-throttle-2026-07.md)
  - [fsync group-commit: batch factor (2026-07)](internal/fsync-group-commit-batch-factor-2026-07.md)
  - [JE constant/default/threshold audit (2026-07)](internal/je-constant-audit-2026-07.md)
