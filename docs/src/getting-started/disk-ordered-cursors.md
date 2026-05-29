# Disk-Ordered Cursors

A `DiskOrderedCursor` returns user records in approximate **on-disk order** —
the order in which their leaf-node (LN) log entries were physically written
to the write-ahead log — rather than in B-tree key order.  This trades
ordering and isolation for raw scan throughput, which is exactly what
applications like full-database export, replication catch-up, and
analytical scans need.

This API was added in Noxu DB v1.6.  It provides high-throughput
unordered scans with an optional `dedup_keys` facility.

## When to use a disk-ordered cursor

Use a `DiskOrderedCursor` when:

- You need to read every record in one or more databases at maximum
  throughput.
- You do **not** care about key order.
- You are willing to tolerate `READ_UNCOMMITTED` semantics (or stronger
  if you arrange a quiesce barrier yourself).

Use a regular [`Cursor`](./cursors.md) when:

- You need records in key order.
- You need transactional isolation (read-committed, serializable,
  etc.).
- You need to look up specific keys efficiently.

## Trade-offs at a glance

| Property | `Cursor` | `DiskOrderedCursor` |
|---|---|---|
| Order of returned keys | Key order (B-tree) | Disk order (log append order) |
| Lock acquisition       | Yes (per record)   | **No** |
| Isolation              | Per-txn isolation  | At best `READ_UNCOMMITTED` |
| Throughput             | Limited by random I/O on B-tree pages | Sequential log read |
| Deduplication of stale versions | Yes — only the latest committed value is returned | **No by default** — the same key may appear multiple times if it was updated |

## Basic usage

```rust,ignore
use noxu_db::{
    DatabaseEntry, DiskOrderedCursorConfig, Environment, EnvironmentConfig,
    OperationStatus,
};

# fn main() -> noxu_db::Result<()> {
let env = Environment::open(
    EnvironmentConfig::new("/path/to/env".into())
        .with_allow_create(true)
        .with_transactional(true),
)?;
let db = env.open_database(
    None,
    "users",
    &noxu_db::DatabaseConfig::new().with_allow_create(true),
)?;

// ... populate db ...

let mut cursor = db.open_disk_ordered_cursor(
    DiskOrderedCursorConfig::new().with_queue_size(64),
)?;

let mut key = DatabaseEntry::new();
let mut data = DatabaseEntry::new();
while cursor.next(&mut key, &mut data)? == OperationStatus::Success {
    // Process the record.  Note: `key` is *not* in sorted order.
    process(key.data(), data.data());
}
cursor.close()?;
# Ok(())
# }
# fn process(_k: &[u8], _v: &[u8]) {}
```

## Multi-database scans

Pass a slice of database references to scan multiple databases as one
unified stream:

```rust,ignore
use noxu_db::{open_disk_ordered_cursor_multi, DiskOrderedCursorConfig};

# fn example(db_a: &noxu_db::Database, db_b: &noxu_db::Database)
#     -> noxu_db::Result<()> {
let dbs = [db_a, db_b];
let mut cursor =
    open_disk_ordered_cursor_multi(&dbs, DiskOrderedCursorConfig::new())?;
// next() returns records from either database in disk order.
# Ok(())
# }
```

All databases must belong to the same `Environment`.  The cursor's
lifetime is tied to the borrow of the slice, so a database cannot be
closed while the cursor is still scanning.

## Configuration knobs

```rust,ignore
use noxu_db::DiskOrderedCursorConfig;

let cfg = DiskOrderedCursorConfig::new()
    // Producer thread blocks when this many entries are queued.
    .with_queue_size(1000)
    // Soft cap on cumulative key+data bytes buffered.
    .with_internal_memory_limit(64 * 1024 * 1024)
    // Skip data; only emit keys (slightly faster).
    .with_keys_only(true)
    // Noxu extension: filter out stale versions of repeated keys.
    .with_dedup_keys(true);
```

| Field | JE field | Default | Purpose |
|---|---|---|---|
| `queue_size`            | `setQueueSize`            | `1000`     | Bound on the producer→consumer channel. |
| `internal_memory_limit` | `setInternalMemoryLimit`  | `usize::MAX` | Soft cap on bytes buffered. |
| `lsn_batch_size`        | `setLSNBatchSize`         | `usize::MAX` | Advisory; honoured as a producer cancel-check interval. |
| `keys_only`             | `setKeysOnly`             | `false`   | Emit empty data. |
| `bins_only`             | `setBINsOnly`             | `false`   | Alias for `keys_only` in Noxu. |
| `count_only`            | `setCountOnly` (`@hidden`) | `false`   | Alias for `keys_only`. |
| `dedup_keys`            | *(Noxu extension)*        | `false`   | Filter repeated keys client-side. |

## Consistency caveats

The records returned by a `DiskOrderedCursor` correspond to the state of
the database at the moment each LN was written to the log, which may
include **uncommitted** writes (it sees the log directly, with no lock
acquisition).  Concurrent inserts/updates/deletes performed *during*
the scan are not required to be visible.

Applications that need a transactionally-consistent snapshot should
drain in-flight writers before opening the cursor — for example, by
holding a quiesce barrier or scheduling the export when the database
is otherwise idle.

### Stale versions

By default, every LN that survives in the log and belongs to one of the
targeted databases is yielded — *even if a newer version of the same
key follows later in the scan*.  This matches BDB JE's behaviour and is
the right answer for bulk-export workflows that want to observe every
mutation.

If you only want each key once, set
`DiskOrderedCursorConfig::with_dedup_keys(true)`.  Note that `dedup_keys`
returns the **first** version of a key encountered (the **oldest** still
in the log), not the latest.  For latest-only semantics use a regular
B-tree cursor.

### Deletes

The producer skips LN log entries whose operation is `Delete`.  However,
because the log is append-only, the *original* insert of a since-deleted
key may still appear in the scan unless `dedup_keys = true` (in which
case the dedup set absorbs it).  Treat the absence of "the most recent
version of key K" as the only safe signal of liveness; use a regular
cursor if you need a strict liveness check.

## Producer-thread lifecycle

Opening a cursor spawns one background producer thread named
`noxu-disk-ordered-cursor`.  It walks the log files in ascending order
and pushes decoded records onto a bounded channel.

The thread is joined when the cursor is dropped *or* `close()` is
called explicitly:

```rust,ignore
let cursor = db.open_disk_ordered_cursor(cfg)?;
// Use cursor...
cursor.close()?;     // explicit join
// or: drop(cursor); // implicit join via Drop
```

## Thread safety

`DiskOrderedCursor` is not `Send` and not `Sync`.  A single cursor must
be advanced from a single thread.  If you need parallel scans, open one
cursor per worker — each gets its own producer thread and operates
independently.

## Observability

The producer thread is named `noxu-disk-ordered-cursor` and its panics
(should they occur) are logged via the `log` crate at WARN level under
the `noxu-disk-ordered-cursor` target.  No additional metrics are
emitted in v1.6 — see the
[`noxu-observe`](../reference/configuration.md) integration if you need
to wire structured tracing.
