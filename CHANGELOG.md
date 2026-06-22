# Changelog

All notable changes to Noxu DB are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Changed

- **T-4 (heap): `INTargetRep` None/Sparse/Default compaction of the
  resident-child-pointer array.** The cached child pointer was moved off each
  per-slot `InEntry` (an always-present `Option<Arc>`, 8 bytes/slot even when
  no child was resident) into a node-level `TargetRep` on `InNodeStub`,
  faithful to JE `INTargetRep` (`None`/`Sparse`/`Default`). An upper IN with no
  resident children — the common case — now uses the `None` rep (0
  child-pointer bytes) instead of `N * size_of::<Option<Arc>>()`; a few cached
  children use `Sparse` (cap 4, `INTargetRep.Sparse.MAX_ENTRIES`), inflating to
  the full `Default` array only when many children are resident. Children
  travel with their slots through splits/merges and shift on insert/remove
  (`INArrayRep.copy`). Purely an in-memory layout change: the on-disk
  serialization is unchanged (child pointers were never serialized). Saves 8
  bytes/slot plus the eliminated per-node child array for non-resident upper
  INs.

### Deferred

- **T-3 (`INLongRep` per-slot LSN byte-width packing) — deferred.** A faithful
  port is blocked on `NULL_LSN == u64::MAX`: JE `INLongRep` requires
  non-negative values and would force 8-byte width for any node holding a
  single NULL slot (common for not-yet-logged slots), defeating the win. The
  actual scope to port is JE's `baseFileNumber`-relative `entryLsnByteArray`
  plus a reserved transient-offset encoding.
- **T-2 (`INKeyRep.MaxKeySize` compact key array) and T-5
  (`TREE_COMPACT_MAX_KEY_LENGTH` wiring) — deferred.** Most invasive of the
  three (touches every key access: find_entry / prefix / split / serialize).
  Per-slot keys remain `Vec<u8>`; `TREE_COMPACT_MAX_KEY_LENGTH` stays
  accepted-but-inert. See `docs/src/operations/known-limitations.md`.
