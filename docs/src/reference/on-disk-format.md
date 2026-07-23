# On-Disk Format

Noxu DB uses a Rust-native on-disk format. It is **not** binary-compatible
with Noxu DB (`.jdb` files).

## Directory Layout

```text
/path/to/environment/
    noxu.lck            Environment lock file
    00000000.ndb        Log file 0
    00000001.ndb        Log file 1
    0000002a.ndb        Log file 42
    ...
```

Files are named with 8-digit lowercase hex file numbers and `.ndb` extension.
Gaps indicate cleaned (deleted) files.

## Log File Structure

Each `.ndb` file:

1. **File header** (version-aware size): magic (`NOXUDB\0\0`), log format
   version (`u32`), byte-order marker, timestamp, file number (`u32`),
   previous-file last-entry offset, and — in `LOG_VERSION` 3 — a trailing CRC32
   over the header. A v3 header is **36 bytes**; a legacy v2 header is
   **32 bytes** (no CRC). The first log entry begins immediately after the
   header, so the first-entry offset is resolved per file from its own
   version via `FileHeader::on_disk_size(version)` (v2 → 32, v3 → 36). v2 files
   remain fully readable; a torn/corrupt v3 header is detected by the CRC at
   open time (`LogError::HeaderChecksumMismatch`).
2. **Log entries** (variable length, packed with no alignment padding)

## Entry Header

```text
Offset  Size  Field
------  ----  -----
0       4     CRC32 checksum (little-endian, covers bytes 4..end)
4       1     Entry type
5       1     Flags (bitfield)
6       4     Previous entry offset (little-endian)
10      4     Payload size in bytes (little-endian)
[14     8     VLSN (little-endian, present when VLSN_PRESENT flag set)]
```

Base header: **14 bytes**. With VLSN: **22 bytes**.

## LSN Encoding

```text
bits 63..32  →  file_number (u32)
bits 31..0   →  byte offset within the file (u32)
NULL_LSN = 0x0000_0000_0000_0000
```

## VLSN Encoding

A `Vlsn` is a signed `i64`, little-endian. `NULL_VLSN = i64::MIN`.

## Endianness

Endianness varies by field category:

| Field category | Encoding | Source |
|---|---|---|
| Entry header integers (CRC32, prev\_offset, payload\_size, VLSN) | **little-endian** | `to_le_bytes()` / `get_u32_le()` in `log_manager.rs` |
| BIN / IN node payload integers (`u32`, `u64` fields such as entry counts and child LSNs) | **big-endian** | `BytesMut::put_u64()` / `to_be_bytes()` in `noxu-tree` serializers |
| LSN packed field (`u64` stored as `file_num:32 ++ file_offset:32`) | **big-endian** | `Lsn::as_u64()` bit layout |
| VLSN (signed `i64` in the header extension) | **little-endian** | `get_i64_le()` |

Summary: **headers are little-endian; tree-node payloads (BIN/IN) are
big-endian**. Big-endian hosts are not currently supported (the engine is
designed for x86-64 / aarch64 little-endian hosts, but the B-tree payloads
are intentionally big-endian so that byte-wise key comparison preserves
numeric sort order without extra transformation).

## Entry Type Codes

The following table is generated from `crates/noxu-log/src/entry_type.rs`.
Each `Code` is the decimal discriminant of the `LogEntryType` enum; the
hex equivalent is shown for readability.

| Code | Hex | Name | Description |
|---|---|---|---|
| 1 | 0x01 | `FileHeader` | Log file header |
| 2 | 0x02 | `IN` | Upper internal node (full) |
| 3 | 0x03 | `BIN` | Bottom internal node (full) |
| 4 | 0x04 | `BINDelta` | Incremental BIN update |
| 10 | 0x0a | `InsertLN` | Non-txn insert leaf node |
| 11 | 0x0b | `UpdateLN` | Non-txn update leaf node |
| 12 | 0x0c | `DeleteLN` | Non-txn delete leaf node tombstone |
| 13 | 0x0d | `InsertLNTxn` | Transactional insert leaf node |
| 14 | 0x0e | `UpdateLNTxn` | Transactional update leaf node |
| 15 | 0x0f | `DeleteLNTxn` | Transactional delete leaf node |
| 20 | 0x14 | `MapLN` | Database id→root mapping |
| 21 | 0x15 | `NameLN` | Database name→id mapping |
| 22 | 0x16 | `NameLNTxn` | Transactional name→id mapping |
| 23 | 0x17 | `FileSummaryLN` | Per-file utilization summary |
| 30 | 0x1e | `TxnCommit` | Transaction commit record |
| 31 | 0x1f | `TxnAbort` | Transaction abort record |
| 32 | 0x20 | `TxnPrepare` | XA two-phase commit prepare (v2+) |
| 40 | 0x28 | `CkptStart` | Begin checkpoint |
| 41 | 0x29 | `CkptEnd` | End checkpoint |
| 50 | 0x32 | `DbTree` | Database tree root record |
| 60 | 0x3c | `Trace` | Debug trace entry |
| 61 | 0x3d | `Matchpoint` | Replication sync point |
| 62 | 0x3e | `RollbackStart` | HA rollback start marker |
| 63 | 0x3f | `RollbackEnd` | HA rollback end marker |
| 64 | 0x40 | `INDeleteInfo` | Tree compression delete info |
| 65 | 0x41 | `INDupDeleteInfo` | Tree compression dup-delete info |
| 66 | 0x42 | `OldBINDelta` | Legacy BIN-delta (recovery compat) |
| 67 | 0x43 | `OldLN` | Legacy LN format (recovery compat) |
| 68 | 0x44 | `DelDupLN` | Legacy dup-delete LN |
| 69 | 0x45 | `DupCountLN` | Legacy dup-count LN |
| 70 | 0x46 | `ImmutableFile` | Immutable file lifecycle marker |

> **Not binary compatible with other database formats.**
> Noxu uses different serialization and different type codes;
> `.ndb` files are not readable by any other database engine.

## LN (Leaf Node) Payload

An LN entry (`InsertLN` / `UpdateLN` / `DeleteLN` and their `*Txn` variants)
begins its payload with a one-byte flag bitfield, followed by the database ID
and the optional/variable-length fields whose presence the flags indicate
(transactional abort info, keys, data). Two of the flag bits carry the TTL
record-expiration feature:

| Bit | Mask | Meaning |
|---|---|---|
| 0 | 0x01 | Abort version was known-deleted |
| 1 | 0x02 | Record is embedded in the BIN after this operation |
| 2 | 0x04 | Abort key present |
| 3 | 0x08 | Abort data present |
| 4 | 0x10 | Abort VLSN present |
| 5 | 0x20 | Abort LSN present |
| 6 | 0x40 | Abort-version expiration present (4-byte `i32`) |
| 7 | 0x80 | Record expiration present (4-byte `i32`) |

When bit 7 (`HAVE_EXPIRATION`) is set, a 4-byte big-endian `i32` expiration
time (packed hours since the Unix epoch, JE `LNLogEntry.getExpiration`) is
written in the payload; when clear, the record has no expiration. The
expiration fields are **optional and flag-gated**, so this is not a format
version change: an LN entry written without a TTL is byte-identical to a
pre-TTL entry, and an older log (or any entry with the flag clear) reads back
as never-expiring (expiration = 0). Recovery replays the expiration into the
B-tree slot so a record's TTL survives a crash.

## `CkptEnd` Body

The `CkptEnd` (type `0x29`) body records the metadata recovery needs to
rebuild from a checkpoint. Its fixed leading fields are (all big-endian):

| Field | Bytes | Notes |
|---|---|---|
| checkpoint id | 8 (u64) | matches the `CkptStart` |
| invoker len + string | 2 + N | UTF-8 invoker tag |
| checkpoint start LSN | 8 | |
| flags | 1 | bit 0 = has mapping-tree root LSN; bit 1 = cleaned-files-to-delete |
| root LSN | 8 (if flag set) | mapping-tree root (always absent in Noxu — the catalog is an in-memory map, not an on-disk mapping tree) |
| first active LSN | 8 | |
| last-{local,replicated}-{node,db,txn} ids | 8 × 6 | ID sequence maxima |
| timestamp | 8 + 4 | seconds (i64) + nanos (u32) |

### v2 per-database roots trailer (optional)

After the timestamp, a checkpoint MAY append a **per-database tree-roots
trailer**. It is written only when at least one open user database has a
materialisable tree root, so a checkpoint with no seedable roots is
**byte-identical to the pre-v2 `CkptEnd`** (full backward compatibility):

| Field | Bytes | Notes |
|---|---|---|
| marker | 1 | `0x01` introduces the trailer; absent in a v1 entry |
| count | 4 (u32) | number of `(db_id, root_lsn)` pairs |
| pairs | 16 × count | each: db_id (u64) + tree root LSN (u64) |

Each `root_lsn` is the LSN the database's tree root IN/BIN was last logged at
as of this checkpoint. Recovery seeds each reconstructed tree from it and
lazily fetches pre-checkpoint BINs on demand instead of replaying every
pre-checkpoint LN (see [Recovery Protocol](recovery.md)).

**Reading rule:** a reader that finds no trailing marker byte (an old v1
entry) yields an empty per-DB-roots set; recovery then seeds no tree and
falls back to full LN redo. Any malformed/truncated trailer degrades to the
same empty set — a valid checkpoint is never rejected because of a torn
trailer.
