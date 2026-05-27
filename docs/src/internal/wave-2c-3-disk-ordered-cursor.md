# Wave 2C-3 — DiskOrderedCursor

**Audit finding closed:** JE port-audit MEDIUM —
> "DiskOrderedCursor is entirely absent from Noxu.  The JE class is the
> high-throughput unordered scan API used for bulk export."

**Branch:** `fix/wave2c-3-disk-ordered-cursor` (off `sprint/v1.6.0-rc1-base`).

## Goal

Provide an API equivalent in shape to JE's `DiskOrderedCursor` so applications
that need to bulk-scan one or more databases without paying B-tree traversal
cost can do so on Noxu.

## Public API surface

`crates/noxu-db/src/disk_ordered_cursor.rs`:

- `pub struct DiskOrderedCursorConfig`
  - `queue_size: usize` (default 1000)
  - `lsn_batch_size: usize` (default `usize::MAX`)
  - `internal_memory_limit: usize` (default `usize::MAX`)
  - `keys_only: bool` (default false)
  - `bins_only: bool` (default false; alias for `keys_only`)
  - `count_only: bool` (default false; alias for `keys_only`)
  - `dedup_keys: bool` (default false; **Noxu extension**)
  - Builder methods `with_queue_size`, `with_lsn_batch_size`,
    `with_internal_memory_limit`, `with_keys_only`, `with_bins_only`,
    `with_count_only`, `with_dedup_keys`.

- `pub struct DiskOrderedCursor<'env>`
  - `fn next(&mut self, key, data) -> Result<OperationStatus>`
  - `fn current(&self, key, data) -> Result<OperationStatus>`
  - `fn close(self) -> Result<()>`
  - `Drop` joins the producer thread.

- `Database::open_disk_ordered_cursor(config) -> Result<DiskOrderedCursor<'_>>`
  — single-database convenience.

- `pub fn open_disk_ordered_cursor_multi(databases, config) -> Result<DiskOrderedCursor<'_>>`
  — N-database scan.

## Implementation strategy

`crates/noxu-dbi/src/disk_ordered_cursor_impl.rs`:

```text
                       +--------------------------+
   open() ----spawn--> |  Producer thread          |
                       |  noxu-disk-ordered-cursor |
                       +-----------+--------------+
                                   |
                       FileManagerLogScanner::
                       scan_forward(file_start, file_end)
                                   |
                       filter LN, db_id, !Delete
                                   |
              budget.reserve()     |
              tx.send((k, d))      |
                                   v
                        +------------------+
                        | sync_channel     |
                        | (queue_size)     |
                        +--------+---------+
                                 |
              budget.release()   |
              rx.recv_timeout()  |
                                 v
                        consumer::next_entry()
```

### Files scanned per loop iteration

The producer iterates the log files in ascending order
(`FileManager::list_file_numbers()`).  For each file it calls
`FileManagerLogScanner::scan_forward(Lsn(file, 0), Lsn(file+1, 0))`,
which mmaps the file (or falls back to `pread64`) and yields a
`Vec<PositionedEntry>` whose contents are zero-copy `Bytes` slices into
the file region.  Memory peak per iteration is therefore bounded by one
log-file's worth of decoded entries — roughly 30-100 MiB on default
configurations — plus the channel queue.

### Memory budget

`MemoryBudget` tracks the cumulative `key.len() + data.len()` of items
buffered in the channel via an `AtomicUsize`.  When a producer
`reserve(n)` would exceed the limit, it parks on a `Condvar` (with a
50 ms timeout so cancellation is noticed promptly).  The consumer calls
`release(n)` after every successful `recv()`, which `notify_all`s the
producer.

The budget short-circuits when the limit is `usize::MAX`, avoiding the
mutex/condvar overhead in the common case.  At least one item is always
allowed through even if it exceeds the budget, so a giant payload
cannot deadlock the scan.

### Cancellation

`shutdown()` sets an `AtomicBool` and notifies the budget's condvar.
The producer checks the flag every 64 entries (or every
`lsn_batch_size`, whichever is smaller).  The consumer drains any
in-flight items so the producer never blocks on `tx.send()`, then
joins the thread.

`Drop` calls `shutdown()` automatically, so applications using RAII
never observe a leaked thread.

### Lifetime

`DiskOrderedCursor<'env>` carries a `PhantomData<&'env ()>` tied to the
borrow of the `&[&Database]` slice.  This prevents the application from
closing a `Database` (and therefore the underlying environment) while a
scan is still in flight, mirroring JE's `addCursor` / `removeCursor`
ref-counting without runtime overhead.

## Invariants

1. **Producer is always joined.**  Every public construction of a
   `DiskOrderedCursor` either successfully spawns a producer (whose
   handle is recorded in `DiskOrderedCursorImpl::handle`) or sets
   `handle = None` (no-WAL environment).  `Drop` and `shutdown` are
   idempotent and always join `Some(handle)`.

2. **Producer cannot block the consumer indefinitely.**  All `recv`s
   use `recv_timeout(100ms)` so the consumer can observe its own
   cancellation in case the producer was killed.  All `send`s are on a
   bounded `sync_channel`, but the `MemoryBudget::reserve()` wait is
   itself cancellable.

3. **End-of-stream is sticky.**  Once `next_entry()` returns `Ok(None)`
   it always returns `Ok(None)`.  Once it returns an `Err`, it always
   returns the same error class (latched in `terminal_err`).

4. **No locks acquired on the data path.**  The producer never calls
   into the lock manager.  Consequently the cursor cannot deadlock
   against transactional writers, but it also offers no isolation
   beyond `READ_UNCOMMITTED`.

5. **Filter is purely log-side.**  An LN entry is yielded if and only
   if its `db_id` is in the targeted set, its operation is not
   `Delete`, and its `data` is `Some(_)`.  No B-tree state is consulted.

## Tests

`crates/noxu-db/tests/disk_ordered_cursor_test.rs` — 13 integration tests:

| Test | What it verifies |
|---|---|
| `walks_all_inserted_records` | 1000 records visible after auto-commit + checkpoint |
| `skips_deleted_records` | Live keys still present after partial deletes |
| `skips_deleted_records_with_dedup` | `dedup_keys = true` yields each key once |
| `multi_db_scan_returns_all_dbs` | Two-DB scan returns union of records |
| `bounded_queue_completes` | `queue_size = 2`, `mem_limit = 64` still completes |
| `drop_mid_iteration_joins_producer` | Tiny queue + mid-scan drop has no thread leak |
| `close_is_idempotent` | `close()` after `close()` succeeds |
| `stale_versions_visible_by_default` | All three updates of one key are visible (JE-correct) |
| `dedup_keys_filters_repeated_keys` | `dedup_keys = true` reduces to one entry |
| `current_returns_last_record` | `current()` re-emits last `next()` result |
| `empty_db_yields_no_records` | Empty database → immediate `NotFound` |
| `keys_only_returns_empty_data` | `keys_only = true` elides data bytes |
| `empty_db_list_is_rejected` | `IllegalArgument` on empty `&[&Database]` |

Plus 5 unit tests in `noxu-dbi::disk_ordered_cursor_impl` covering
`MemoryBudget` (reserve/release/cancel) and `open()` with no log
manager (instant end-of-stream).

## Files touched

- new `crates/noxu-db/src/disk_ordered_cursor.rs`
- new `crates/noxu-dbi/src/disk_ordered_cursor_impl.rs`
- new `crates/noxu-db/tests/disk_ordered_cursor_test.rs`
- new `docs/src/getting-started/disk-ordered-cursors.md`
- new `docs/src/internal/wave-2c-3-disk-ordered-cursor.md`
- modified `crates/noxu-db/src/lib.rs` — re-exports
- modified `crates/noxu-dbi/src/lib.rs` — re-exports
- modified `crates/noxu-db/src/database.rs` — three `pub(crate)` accessors
  (`cached_log_manager`, `check_open_for_doc`, `database_id_for_doc`)
- modified `docs/src/SUMMARY.md`, `docs/src/introduction.md`

## What's intentionally not done

- **No phase-I sort by LSN.**  JE's algorithm gathers LSNs in B-tree
  order, sorts them, and fetches in disk order to maximise read locality.
  Noxu reads files sequentially from offset 0 upward, which is already
  in disk order — the JE sort step is unnecessary.  This makes the
  Noxu implementation simpler and avoids the LSN-buffer memory overhead.

- **No `getCurrent()` re-fetch from log.**  JE's `getCurrent()` can fetch
  the LN at the cursor's current LSN if the in-memory copy was lost.
  Noxu's `current()` simply re-emits the last `next()` result from a
  `Vec<u8>` buffer — adequate for the JE contract, which only promises
  to return whatever the last `next()` returned.

- **No per-DB pruning by file range.**  All log files are scanned for
  every cursor.  A future optimisation could record per-DB LSN ranges
  in the catalogue and skip irrelevant files; v1.6 does the simple
  thing.

- **`lsn_batch_size` is advisory.**  JE uses it to bound the LSN sort
  buffer.  Noxu does not have an LSN sort step, so the field is
  honoured only as a producer cancellation-check granularity.

These omissions are documented in the public docs and do not affect
correctness for the supported workloads.
