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

1. **File header** (fixed size): log format version (`u32`), file number (`u32`)
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

All multi-byte integers in entry headers are **little-endian**. Most payload
fields also use little-endian. Big-endian hosts are not currently supported.

## Entry Type Codes (selection)

| Code | Name | Description |
|---|---|---|
| 0x01 | `LN` | Leaf node (key/value record) |
| 0x02 | `DEL_LN` | Deleted leaf node tombstone |
| 0x10 | `BIN` | Bottom internal node (full) |
| 0x11 | `BIN_DELTA` | Incremental BIN update |
| 0x12 | `IN` | Upper internal node |
| 0x20 | `COMMIT` | Transaction commit |
| 0x21 | `ABORT` | Transaction abort |
| 0x30 | `CHECKPOINT_START` | Begin checkpoint |
| 0x31 | `CHECKPOINT_END` | End checkpoint |
| 0x40 | `MAP_LN` | Database name→id mapping |
| 0x50 | `MATCHPOINT` | Replication sync point |
| 0x60 | `EXTINCT_LN` | TTL-expired record (Noxu) |

> **Not binary compatible with other database formats.**
> serialization and different type codes; Noxu `.ndb` files are not readable
