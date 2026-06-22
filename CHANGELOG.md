# Changelog

All notable changes to Noxu DB are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Changed

- **T-3 (IN-array LSN compaction):** the per-slot `lsn: Lsn` field (8 bytes)
  on `BinEntry`/`InEntry` was hoisted to a node-level packed `LsnRep`
  (`IN.entryLsnByteArray` / `IN.entryLsnLongArray`, IN.java:251-289).  LSNs are
  stored `base_file_number`-relative at 4 bytes/slot (1 byte file-number
  offset + 3 byte file offset) — half the raw `u64` cost — falling back to a
  `u64`-per-slot `Long` rep only when a node's file-number spread exceeds 127
  or a file offset exceeds `0xff_fffe`.  An all-NULL node uses the 0-byte
  `Empty` rep.  `NULL_LSN` is encoded via the `0xff_ffff` file-offset
  sentinel (NOT the raw `u64::MAX`), so nodes with NULL slots still pack
  compactly.  Access is via `BinStub`/`InNodeStub` `get_lsn(slot)` /
  `set_lsn(slot, lsn)`.  The on-disk `serialize_full`/`serialize_delta` bytes
  are unchanged (this is an in-memory heap optimization).
