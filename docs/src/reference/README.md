# Programmer's Reference

This chapter documents Noxu DB's internal architecture at the level needed to
understand, debug, or extend the system. It corresponds to the **Noxu DB
Programmer's Reference Guide**, adapted for the Rust implementation.

The canonical high-level overview is also available in [`ARCHITECTURE.md`](https://codeberg.org/gregburd/noxu/blob/main/ARCHITECTURE.md)
at the project root.

## In This Chapter

1. [Architecture](architecture.md) — crate structure, data flow, subsystem interactions
2. [Write-Ahead Log Format](log-format.md) — `.ndb` files, entry header, entry types, checksums
3. [B-tree Internals](btree.md) — IN, BIN, LN nodes; key prefix compression; BIN-delta
4. [Concurrency Model](concurrency-model.md) — latch hierarchy, lock table sharding, compatibility matrix
5. [Recovery Protocol](recovery.md) — 3-phase crash recovery, checkpoint protocol
6. [Cache Eviction](cache-eviction.md) — LRU eviction, priority queues, `CacheMode`, `MemoryBudget`
7. [Log Cleaning](log-cleaning.md) — utilization tracking, file selector, LN migration
8. [Configuration Reference](configuration.md) — all 400+ parameters with defaults and valid ranges
9. [On-Disk Format](on-disk-format.md) — directory layout, file header, endianness
