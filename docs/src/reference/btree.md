# B-tree Internals

Noxu DB stores all records in a B+tree maintained by `noxu-tree`. The tree
is a direct port of Noxu's in-memory tree with three node types, key prefix
compression, BIN-delta incremental logging, and latch-coupling traversal.

## Node Types

### IN — Internal Node

Upper-level tree nodes containing keys and child pointers. Each slot holds a
key (the minimum key of that child's subtree) and an `Arc<RwLock<Node>>`
reference to a child IN or BIN. INs live in memory and are written to the log
during checkpoint.

### BIN — Bottom Internal Node

The leaf-level internal nodes. Each slot holds a key and either:

- A pointer to a separate `LN` node (for larger values), or
- An **embedded LN** (small values stored directly in the BIN slot)

BINs carry per-slot metadata:

- `dirty: bool` — set on insert/update, cleared after checkpoint
- `modification_times: Vec<u64>` — TTL write timestamps
- `creation_times: Vec<u64>` — TTL insert timestamps

The `BinStub` (evicted form) retains `last_full_lsn`, `last_delta_lsn`, and
`cursor_count` for the evictor.

### LN — Leaf Node

The actual data records (full key-value pairs). Small LNs are embedded
directly in their parent BIN slot.

## Key Prefix Compression

When BIN keys share a common prefix, the prefix is stored once in the BIN
header; individual slots store only the suffix. `recompute_key_prefix()`
rebuilds the prefix when a BIN is deserialized. This reduces memory for
structured keys (e.g., UUIDs, hierarchical paths).

## BIN-delta — Incremental Logging

Rather than logging a full BIN on every update, Noxu DB logs only the changed
slots as a **BIN-delta**. A delta references the base BIN by `last_full_lsn`.

During recovery, `mutate_to_full_bin_from_log()` reads the base BIN then
applies deltas in sequence. `last_delta_lsn` is reset to `NULL_LSN` after
each full BIN write.

## Latch-Coupling Traversal

Tree descents use **latch coupling** (crabbing): acquire the child's latch
before releasing the parent's latch. Standard descents hold a **shared
latch** on each IN, then a **write latch** on the target BIN for mutations.

## Dirty Node Tracking

- `Tree::collect_dirty_bins(db_id)` — returns BINs needing checkpoint flush
- `Tree::collect_dirty_upper_ins(db_id)` — returns INs needing checkpoint flush
- `Tree::collect_bins_with_known_deleted()` — for `INCompressor` daemon

## Per-BIN Interior Mutability

Each BIN is wrapped in `Arc<RwLock<Bin>>`, allowing concurrent reads to
different BINs without contending on a tree-level lock.
