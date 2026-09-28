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

## BIN Node Payload — embedded LN storage

A full `BIN` (`0x03`) serialises `node_id` (`u64`, BE) and the slot count
(`u32`, BE), then one record per slot; a `BINDelta` (`0x04`) is the same but
prefixed with the dirty-slot index and includes only dirty slots. Each slot
is:

```text
key_len (u32, BE) | key | lsn (u64, BE) | has_data (u8) [| data_len (u32, BE) | data] | known_deleted (u8)
```

The **`has_data` byte** decides whether the LN's value is *embedded* in the
slot:

- `has_data = 1` — the slot carries the value inline (`data_len` + `data`
  follow). Read directly.
- `has_data = 0` — the slot carries **an LSN pointer only**; the value lives
  in the LN log entry at `lsn` and is materialised from the log on read (the
  same path an evictor-stripped LN takes). `known_deleted` still follows.

**Embedding rule (1A, `TREE_MAX_EMBEDDED_LN` / `noxu.tree.maxEmbeddedLN`,
default 16):** on serialisation a slot embeds its value (`has_data = 1`) only
when the value length is `<= max_embedded_ln` (and the DB is not a
sorted-duplicate or internal DB — those never embed). A larger value is
written as an LSN pointer only (`has_data = 0`). This mirrors JE
`CursorImpl.shouldEmbedLN` (`data.length <= env.getMaxEmbeddedLN()`) and
`BIN.updateRecord(.., newEmbeddedLN ? data : null, ..)`. It keeps the BIN
image small for large records so a checkpoint no longer amplifies the log by
re-serialising the full value into every BIN it writes (REG-CLEANER-DISKLIMIT).

**Backward compatibility — no `LOG_VERSION` bump.** The `has_data` byte has
always been part of the slot format, so the change is purely *which value* the
serialiser writes for a large record, not a new field:

- **Old-format BINs** (written before 1A, when Noxu embedded every value
  regardless of size) have `has_data = 1` for large records. The decoder reads
  the embedded bytes exactly as before — an existing database opens and reads
  with no migration.
- **New-format BINs** write `has_data = 0` for a value `> max_embedded_ln`;
  the decoder yields `data = None` for that slot and the read path fetches the
  value from `lsn` — the identical code path used for an evictor-stripped slot,
  so recovery and normal reads both materialise the value.
- A slot with no value (a deletion tombstone, or an already-stripped resident
  LN) is likewise written `has_data = 0`; `known_deleted` disambiguates a
  deleted slot from a live-but-non-embedded one.

Because the distinction is carried by the existing `has_data` byte and both
shapes decode correctly, an old log reads under new code and a new log reads
under any reader that already understood the slot format — no format-version
field and no `LOG_VERSION` change.

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

## `NameLN` / `NameLNTxn` data field

A `NameLN` (type `0x15`) or `NameLNTxn` (type `0x16`) maps a database *name*
(the LN key) to its persistent per-database metadata (the LN data). The data
field is a fixed 8-byte database ID followed by an **optional, self-describing
trailer**:

| Field | Bytes | Notes |
|---|---|---|
| db_id | 8 (u64, LE) | database ID |
| btree_id_len + bytes | 2 (u16, LE) + N | B-tree comparator identity (DBI-14); len 0 = none |
| dup_id_len + bytes | 2 (u16, LE) + N | duplicate comparator identity (DBI-14); len 0 = none |
| fanout marker | 1 | `0xF0` introduces the fanout field (NEW-5); absent otherwise |
| fanout | 4 (i32, LE, if marker present) | persisted `NODE_MAX_ENTRIES` / `maxTreeEntriesPerNode` |

The trailer is layered so that each historical shape reads correctly, shortest
first:

- **Pre-DBI-14** records are exactly the 8-byte db_id (no trailer) and decode
  to no comparators and no fanout.
- **DBI-14** records append the two length-prefixed comparator identities and
  decode with no fanout.
- **NEW-5** records additionally append `[0xF0][fanout i32]` after the
  comparator block, so the fanout has a fixed starting offset.

A fanout of `0` (JE's "use the environment `NODE_MAX_ENTRIES` default",
`DatabaseImpl.java:420`) is **elided** — but this does NOT occur in a normal environment. The
**resolved** effective fanout is persisted, and the env default `NODE_MAX_ENTRIES`
is `128` (not `0`), so a database that never overrode its fanout still writes a
`[0xF0][128]` trailer, NOT the pre-NEW-5 shape. This mirrors JE, which stores the
resolved default and
serializes `maxTreeEntriesPerNode` in the `DatabaseImpl` record
(`DatabaseImpl.writeToLog`, `DatabaseImpl.java:2134`) and reads it back
(`readFromLog`, `:2203`).

**Reading rule:** a reader that finds no fanout marker (a pre-NEW-5 record, or
a NEW-5 record for a default-fanout database) recovers no fanout, and the
database falls back to the environment-level `NODE_MAX_ENTRIES` on reopen —
exactly the prior behaviour. An older reader that understands only the DBI-14
comparator block parses the two identities and ignores the fanout tail. Any
malformed/truncated trailer degrades to no fanout (env-default fallback); a
valid catalog entry is never rejected because of a torn trailer. Because the
addition is an optional, marker-gated field that older readers parse
correctly, it does **not** bump `LOG_VERSION`.

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

## Format version stability & cross-version compatibility

The on-disk log format is versioned by `noxu_log::file_header::LOG_VERSION`
(currently **3**), with a `MIN_LOG_VERSION` floor (**2**) enforced at open. A
file whose `log_version` is below the floor or above the engine's `LOG_VERSION`
is rejected with `LogError::VersionMismatch` — the engine fails loudly rather
than risk misreading.

### Policy

- **A format-breaking change** (newer engine writes bytes an older supported
  engine cannot read) bumps `LOG_VERSION` and ships as at least a **minor**
  release, documented in this matrix and the
  [SemVer policy](../contributing/semver-policy.md).
- **An optional, flag-gated field addition** that older readers parse correctly
  (absent unless a presence bit is set; existing field layout unchanged) is
  **not** a break and does not bump `LOG_VERSION`, but is still recorded here.

### Cross-version compatibility matrix

| Writer → Reader | Result |
|---|---|
| v2 file → v3 engine | ✅ read (v3 resolves the 32-byte v2 header via `on_disk_size`) |
| v3 file → v3 engine | ✅ read |
| v3 file → v2 engine | ❌ `VersionMismatch` (v2 predates the v3 header CRC) |
| below `MIN_LOG_VERSION` | ❌ `VersionMismatch` (fail loud) |

### Per-release entry-format notes

- **7.5.4 (TTL / record expiration)**: **no format change.** Per-record
  expiration is carried by the pre-existing `HAVE_EXPIRATION` / `HAVE_ABORT_
  EXPIRATION` flag bits in the LN log entry (see [Entry Header](#entry-header)),
  which have been part of the JE-faithful entry layout since before 7.5.3. A
  record written without a TTL sets no flag and writes no expiration bytes, so
  its encoding is unchanged; a record written *with* a TTL sets the flag and
  appends the expiration field. The `ln_log_entry` serialization is
  byte-identical between 7.5.3 and 7.5.4. **Consequence:** a 7.5.3 engine can
  open and read a 7.5.4 environment, including files that contain TTL records —
  it parses the expiration field identically but does not act on it (records do
  not expire under 7.5.3). `LOG_VERSION` remains 3 across 7.5.3 ↔ 7.5.4.

- **NEW-5 (persisted per-DB fanout)**: **no format change.** The database's
  resolved `NODE_MAX_ENTRIES` (`maxTreeEntriesPerNode`) is persisted in the
  `NameLN` / `NameLNTxn` data field as an optional, `0xF0`-marker-gated `i32`
  after the DBI-14 comparator block (see
  [`NameLN` data field](#nameln--namelntxn-data-field)). New databases persist
  their **resolved** fanout, so a normal database writes a `[0xF0][fanout]`
  trailer (the env default is `128`, not `0`, so the trailer is present, not
  elided). Backward compatibility is by READ TOLERANCE, not byte-identical
  writes: older readers parse the comparator identities and skip the fanout
  tail; records written before NEW-5 carry no fanout and reopen with the
  environment-level `NODE_MAX_ENTRIES` fallback (the prior behaviour). New
  databases get the persisted fanout restored on reopen even without a
  re-supplied `DatabaseConfig`; old databases keep relying on the env
  `NODE_MAX_ENTRIES` until the catalog entry is rewritten (a fresh `NameLN` is
  emitted at the next checkpoint's catalog relog, or on the next
  create/rename). `LOG_VERSION` remains 3.
