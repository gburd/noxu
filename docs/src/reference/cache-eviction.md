# Cache Eviction

The evictor (`noxu-evictor`) keeps the in-memory B+tree cache within its
configured memory budget using a dual-priority LRU system.

## Memory Budget

`MemoryBudget` (in `noxu-dbi`) explicitly tracks memory of every tree node,
lock, and buffer. Noxu DB does not rely on the allocator for memory accounting
— this is a direct port of JE's `MemoryBudget`.

```rust
EnvironmentConfig::new(path)
    .with_cache_size(4 * 1024 * 1024 * 1024)        // 4 GiB on-heap
    .with_max_off_heap_memory(2 * 1024 * 1024 * 1024) // 2 GiB off-heap
```

## Dual-Priority LRU

| Priority | Contents | Eviction order |
|---|---|---|
| **Priority 1 (mixed)** | Clean and dirty nodes | Evicted first |
| **Priority 2 (dirty)** | Dirty nodes only | Evicted last |

When a node is dirtied it moves to Priority 2. Eviction from Priority 2
writes the node to the log first.

## Eviction Triggers

1. **Daemon eviction** — background threads maintain headroom
2. **Inline eviction** — application threads that exceed the budget
3. **Critical eviction** — emergency when approaching hard limits

## CacheMode

| Mode | Behaviour |
|---|---|
| `Default` | Normal LRU |
| `Unchanged` | Do not change LRU position |
| `EvictLn` | Evict the LN immediately after the operation |
| `EvictBin` | Evict the BIN immediately after the operation |
| `KeepHot` | Move to most-recently-used end |
| `MakeEvictable` | Move toward least-recently-used end |

## Monitoring

```rust
let stats = env.get_stats()?;
stats.cache_utilization_percent()   // fraction of budget in use
stats.num_cache_miss()              // cache misses (log reads)
stats.num_not_resident_nodes()      // currently evicted nodes
```
