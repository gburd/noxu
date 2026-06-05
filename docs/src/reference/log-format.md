# Write-Ahead Log Format

The write-ahead log (WAL) is the foundation of Noxu DB's durability. Every
mutation is written to the log before being applied. The log is a sequence of
numbered `.ndb` files in the environment directory, managed by `noxu-log`.

## File Management

`FileManager` presents the abstraction of one contiguous logical log built
from physical files. It handles:

- File creation and rotation when a file exceeds `log_file_max_bytes` (default 10 MiB)
- File handle caching with LRU eviction (capacity: 10 open handles)
- LSN allocation — each file contributes a contiguous range of LSNs

Files are named with 8-digit lowercase hex numbers and the `.ndb` extension:

```text
00000000.ndb    # log file 0
00000001.ndb    # log file 1
0000002a.ndb    # log file 42
```

## Log Manager

`LogManager` is the central coordinator for log writes and reads. It:

- Manages a pool of reusable write buffers (`LogBufferPool`)
- Serializes concurrent writers through a write latch
- Computes CRC32 checksums on each entry
- Coordinates `fdatasync` for durability
- Routes reads to the buffer pool (for recent entries) or disk (for older ones)

The write latch is released *before* `fdatasync`. This enables **group commit**:
multiple concurrent `commit_with_durability()` callers share a single `fsync`.

## LSN — Log Sequence Number

An `Lsn` uniquely identifies any log entry as a `(file_number: u32, offset: u32)`
pair packed into a `u64`:

```text
bits 63..32  →  file_number (u32)
bits 31..0   →  byte offset within the file (u32)
```

`NULL_LSN` (all zeros) is the sentinel representing "no LSN".

## Entry Header Format

Every log entry begins with a header:

```text
Offset  Size  Field
------  ----  -----
0       4     CRC32 checksum (covers bytes 4..end, little-endian)
4       1     Entry type
5       1     Flags (bitfield)
6       4     Previous entry offset (little-endian)
10      4     Payload size in bytes (little-endian)
[14     8     VLSN (present when VLSN_PRESENT flag is set, little-endian)]
```

Base header: **14 bytes**. With VLSN: **22 bytes**.

### Flag Bits

| Bit | Name | Meaning |
|-----|------|---------|
| 0 | `PROVISIONAL` | Part of an uncommitted transaction subtree |
| 1 | `REPLICATED` | Part of the replication stream |
| 2 | `INVISIBLE` | Hidden (aborted transaction cleanup) |
| 3 | `VLSN_PRESENT` | 8-byte VLSN field follows the base header |

## Entry Types

| Category | Types |
|---|---|
| Tree nodes | `IN`, `BIN`, `LN`, `DEL_LN`, `BIN_DELTA` |
| Transaction | `COMMIT`, `ABORT`, `PREPARE` (XA) |
| Administrative | `CHECKPOINT_START`, `CHECKPOINT_END`, `FILE_HEADER`, `TRACE` |
| Database mgmt | `MAP_LN` (name→id), `DELETED_DUPLICATE_LN` |
| Replication | `MATCHPOINT`, `COMMIT` with VLSN |
| Extended-fork entry types | `EXTINCT_LN` (TTL expiry) |

## VLSN — Virtual Log Sequence Number

The `Vlsn` (in `noxu-util`) is a monotonically increasing `i64` assigned to
each replicated log entry. It survives log cleaning because it is independent
of physical LSN layout. The `VlsnIndex` in `noxu-rep` maps `Vlsn → Lsn`.

### How VLSN tagging works (C-C2b)

When `ReplicatedEnvironment::with_environment(env_impl)` is called, it installs
a shared `AtomicU64` VLSN counter on the `EnvironmentImpl`. Every subsequent
call to `EnvironmentImpl::log_txn_commit` increments the counter atomically
and calls `LogManager::log_with_vlsn`, writing the 22-byte header form with
`REPLICATED_MASK | VLSN_PRESENT_MASK` flags. Standalone (non-replicated)
environments never call `log_with_vlsn`; their commit entries always use the
14-byte header with no VLSN field — the on-disk format is byte-unchanged.

The master's `EnvironmentLogScanner` then scans WAL files, recognises the
`VLSN_PRESENT` flag, and auto-feeds the entries to each registered replica
without any `replicate_entry` call from the application.
